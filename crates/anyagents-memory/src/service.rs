//! `anyagents memory-service`: a separate process that embeds raw history in
//! the background and answers dense search and rerank requests over its
//! socket. It reads the kernel's host index and the raw sources read-only, and
//! writes only its own dense index.
use crate::{
    dense::DenseIndex,
    embed::{HostCatalog, Stripe, embed_documents},
    model::{self, ModelPin},
    protocol::{self, Reply, Request, Status},
    runtime::{self, Encoder, Error},
    text::readable,
};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// The kernel's host index, as `anyagents-core` names it.
pub const HOST_INDEX: &str = anyagents_core::host_retrieval::INDEX_FILE;

#[derive(Clone, Debug)]
pub struct Config {
    pub data_dir: PathBuf,
    /// Where pinned models and the runtime live; `<data>/memory-service/models`
    /// unless the operator chooses another directory.
    pub models: PathBuf,
    /// Background embedding workers, one core each. Query work is separate.
    pub index_threads: usize,
    /// Cores for one query's embedding or rerank.
    pub query_threads: usize,
    /// Nice value of the background embedder (Linux): it yields to every
    /// normal-priority process.
    pub nice: i32,
    /// Embed nothing in the background; only answer queries.
    pub no_index: bool,
    /// The pinned pair to run (`pins::EMBEDDERS`, `pins::RERANKERS`).
    pub embedder: ModelPin,
    pub reranker: ModelPin,
}

impl Config {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_owned(),
            models: data_dir.join("memory-service").join("models"),
            index_threads: 1,
            query_threads: 2,
            nice: 19,
            no_index: false,
            embedder: crate::pins::EMBEDDER,
            reranker: crate::pins::RERANKER,
        }
    }
    pub fn dense_path(&self) -> PathBuf {
        self.data_dir.join("memory-service").join("dense-v1.db")
    }
    /// The pair this configuration embeds and reranks with.
    pub fn pins(&self) -> [ModelPin; 2] {
        [self.embedder, self.reranker]
    }
}

/// Lower the calling thread's priority. Threads ONNX Runtime starts from it
/// inherit the value.
#[cfg(target_os = "linux")]
fn nice_this_thread(nice: i32) {
    // SAFETY: plain syscalls on the calling thread.
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        libc::setpriority(libc::PRIO_PROCESS, tid, nice);
    }
}
#[cfg(not(target_os = "linux"))]
fn nice_this_thread(nice: i32) {
    // SAFETY: lowers this process's priority; other platforms have no
    // per-thread nice value.
    unsafe {
        libc::setpriority(libc::PRIO_PROCESS, 0, nice);
    }
}

struct Shared {
    config: Config,
    embedder: Mutex<Encoder>,
    reranker: Mutex<Encoder>,
    stop: AtomicBool,
}

impl Shared {
    fn status(&self) -> Result<Status, Error> {
        let dense = DenseIndex::open(
            &self.config.dense_path(),
            self.config.embedder.name,
            self.config.embedder.dims,
        )?;
        let (embedded, vectors) = dense.counts()?;
        let host = self.config.data_dir.join(HOST_INDEX);
        let total = if host.exists() {
            HostCatalog::open(&host)?.count()?
        } else {
            0
        };
        Ok(Status {
            embedder: self.config.embedder.name.into(),
            reranker: self.config.reranker.name.into(),
            embedded_documents: embedded,
            pending_documents: total.saturating_sub(embedded),
            vectors,
        })
    }

    fn handle(&self, request: Request) -> Result<Reply, Error> {
        match request {
            Request::Status => Ok(Reply::Status {
                status: self.status()?,
            }),
            Request::Search {
                query,
                owners,
                source,
                since_ms,
                until_ms,
                limit,
            } => {
                let vector = {
                    let mut embedder = self.embedder.lock().map_err(|_| "embedder poisoned")?;
                    embedder.embed_queries(&[query.as_str()])?.remove(0)
                };
                let dense = DenseIndex::open(
                    &self.config.dense_path(),
                    self.config.embedder.name,
                    self.config.embedder.dims,
                )?;
                let hits = dense.search(
                    &vector,
                    &owners,
                    source.as_deref(),
                    since_ms,
                    until_ms,
                    limit.min(200),
                )?;
                Ok(Reply::Search {
                    hits,
                    status: self.status()?,
                })
            }
            Request::Rerank { query, passages } => {
                let texts: Vec<String> = passages.iter().map(|p| readable(p.as_bytes())).collect();
                let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
                let mut reranker = self.reranker.lock().map_err(|_| "reranker poisoned")?;
                let mut scores = Vec::with_capacity(refs.len());
                for batch in refs.chunks(8) {
                    scores.extend(reranker.score(&query, batch)?);
                }
                Ok(Reply::Rerank { scores })
            }
        }
    }
}

fn serve_connection(shared: &Shared, stream: UnixStream) {
    let mut reader = BufReader::new(&stream);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let reply = protocol::decode_request(line.trim())
        .and_then(|request| shared.handle(request).map_err(|e| e.to_string()));
    let _ = (&stream).write_all(protocol::encode_reply(&reply).as_bytes());
}

/// Bind the service socket, owner-only. A stale socket left by a dead
/// service is replaced; a live one is refused.
fn bind(socket: &Path) -> std::io::Result<UnixListener> {
    if socket.exists() {
        if UnixStream::connect(socket).is_ok() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("a memory service already answers on {}", socket.display()),
            ));
        }
        std::fs::remove_file(socket)?;
    }
    // Created owner-only: no window in which the data directory's group
    // could connect. Bound before any other thread starts, so the umask
    // change is this thread's alone.
    // SAFETY: plain syscalls; the previous mask is restored at once.
    let previous = unsafe { libc::umask(0o177) };
    let bound = UnixListener::bind(socket);
    unsafe { libc::umask(previous) };
    let listener = bound?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Run until the process is stopped.
pub fn serve(config: Config) -> Result<(), Error> {
    let pins = config.pins();
    model::check(&config.models, &pins)?;
    // Before ONNX Runtime starts its threads: see `bind`.
    let listener = bind(&protocol::socket_path(&config.data_dir))?;
    let runtime = model::runtime_pin().ok_or("no pinned ONNX Runtime for this platform")?;
    let environment = runtime::init(&config.models, &runtime)?;
    let shared = Arc::new(Shared {
        embedder: Mutex::new(Encoder::load(
            environment,
            &config.models,
            &pins[0],
            config.query_threads,
        )?),
        reranker: Mutex::new(Encoder::load(
            environment,
            &config.models,
            &pins[1],
            config.query_threads,
        )?),
        config: config.clone(),
        stop: AtomicBool::new(false),
    });
    if !config.no_index {
        let workers = config.index_threads.max(1);
        for index in 0..workers {
            let indexer = Arc::clone(&shared);
            let stripe = Stripe { index, of: workers };
            std::thread::Builder::new()
                .name(format!("memory-indexer-{index}"))
                .spawn(move || {
                    nice_this_thread(indexer.config.nice);
                    if let Err(error) = index_loop(&indexer, environment, stripe) {
                        eprintln!("memory-service: background embedding stopped: {error}");
                    }
                })?;
        }
    }
    eprintln!(
        "memory-service: {} + {} on {}",
        pins[0].name,
        pins[1].name,
        protocol::socket_path(&config.data_dir).display()
    );
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || serve_connection(&shared, stream));
    }
    shared.stop.store(true, Ordering::Relaxed);
    Ok(())
}

/// How often an idle embedder looks for new documents, and how often it
/// sweeps documents the host index dropped.
const IDLE_POLL: Duration = Duration::from_secs(30);
const SWEEP_EVERY: Duration = Duration::from_secs(600);
const BATCH: usize = 64;

/// Embed new documents first (above the tail mark, oldest first), then the
/// backlog newest first (below the backfill mark). Both marks commit with
/// each document, so a restart resumes where it stopped. Each worker owns one
/// stripe of rowids and its own marks, on a single-threaded session: small
/// encoders use cores best one batch per core. The first worker also sweeps.
fn index_loop(
    shared: &Shared,
    environment: &ort::environment::Environment,
    stripe: Stripe,
) -> Result<(), Error> {
    let config = &shared.config;
    let mut encoder = Encoder::load(environment, &config.models, &config.embedder, 1)?;
    let mut dense = DenseIndex::open(
        &config.dense_path(),
        config.embedder.name,
        config.embedder.dims,
    )?;
    let host_path = config.data_dir.join(HOST_INDEX);
    let mut last_sweep = Instant::now();
    while !shared.stop.load(Ordering::Relaxed) {
        if !host_path.exists() {
            std::thread::sleep(IDLE_POLL);
            continue;
        }
        let host = HostCatalog::open(&host_path)?;
        let did = index_step(&host, &mut encoder, &mut dense, stripe)?;
        if stripe.index == 0 && last_sweep.elapsed() >= SWEEP_EVERY {
            sweep(&host, &dense)?;
            last_sweep = Instant::now();
        }
        if did == 0 {
            std::thread::sleep(IDLE_POLL);
        }
    }
    Ok(())
}

/// One bounded step of background work for one stripe; returns documents
/// examined. Marks are named by stripe (`tail/<i>/<n>`), so a restart with
/// another worker count starts fresh marks and skips what is already
/// embedded by its digest.
pub fn index_step(
    host: &HostCatalog,
    encoder: &mut Encoder,
    dense: &mut DenseIndex,
    stripe: Stripe,
) -> Result<usize, Error> {
    let tail_key = format!("tail/{}/{}", stripe.index, stripe.of);
    let backfill_key = format!("backfill/{}/{}", stripe.index, stripe.of);
    let tail = match dense.progress(&tail_key)? {
        Some(tail) => tail,
        None => {
            // First run: everything already published is backlog.
            let top = host.max_rowid()?;
            dense.set_progress(&tail_key, top)?;
            dense.set_progress(&backfill_key, top + 1)?;
            top
        }
    };
    let fresh = host.after(tail, BATCH, stripe)?;
    if !fresh.is_empty() {
        embed_documents(encoder, dense, &fresh, &tail_key)?;
        return Ok(fresh.len());
    }
    let backfill = dense.progress(&backfill_key)?.unwrap_or(0);
    let older = host.before(backfill, BATCH, stripe)?;
    embed_documents(encoder, dense, &older, &backfill_key)?;
    if older.is_empty() && backfill > 0 {
        dense.set_progress(&backfill_key, 0)?;
    }
    Ok(older.len())
}

/// Drop vectors of documents the host index no longer holds at the same
/// digest: removed, redacted, or republished with other bytes.
pub fn sweep(host: &HostCatalog, dense: &DenseIndex) -> Result<usize, Error> {
    let mut stale = Vec::new();
    for (id, sha) in dense.documents()? {
        if !host.contains(&id, &sha)? {
            stale.push(id);
        }
    }
    Ok(dense.forget(&stale)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_socket_is_owner_only_and_a_live_service_is_never_taken_over() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory-service.sock");
        let live = bind(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(
            bind(&path).unwrap_err().kind(),
            std::io::ErrorKind::AddrInUse
        );
        // A socket whose service is gone is replaced.
        drop(live);
        drop(bind(&path).unwrap());
    }
}
