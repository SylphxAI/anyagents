//! `anyagents memory-service`: a separate process that embeds raw history in
//! the background and answers dense search and rerank requests over its
//! socket. It reads the kernel's host index and the raw sources read-only, and
//! writes only its own dense index.
use crate::{
    dense::DenseIndex,
    embed::{HostCatalog, Stripe, embed_documents},
    model::{self, ModelPin},
    protocol::{self, Rebuild, Reply, Request, Status},
    providers::{
        DEFAULT_MAX_REBUILD_USD, Embedder, EmbedderConfig, Local, ProviderConfig, Reranker, Stamp,
        local_embedder_pin, local_reranker_pin,
    },
    runtime::{self, Error},
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
    /// Which embedder and reranker run (`<data>/memory-service/config.json`;
    /// the pinned local pair when absent).
    pub providers: ProviderConfig,
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
            providers: ProviderConfig::default(),
        }
    }
    /// The dense index of one vector space. Each space has its own file, so
    /// changing the embedding model never mixes vectors and switching back
    /// finds the old index while it is kept.
    pub fn dense_path(&self, stamp: &Stamp) -> PathBuf {
        self.data_dir
            .join("memory-service")
            .join(format!("dense-{}.db", stamp.digest()))
    }
    /// The pinned local models this configuration runs, to fetch and verify.
    pub fn local_pins(&self) -> Result<Vec<ModelPin>, Error> {
        Ok(local_embedder_pin(&self.providers.embedder)?
            .into_iter()
            .chain(local_reranker_pin(&self.providers.reranker)?)
            .collect())
    }
    pub fn max_rebuild_usd(&self) -> f64 {
        self.providers
            .max_rebuild_usd
            .unwrap_or(DEFAULT_MAX_REBUILD_USD)
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

/// The runtime a local model needs, started only when one is configured.
pub fn local_runtime(
    config: &Config,
) -> Result<Option<&'static ort::environment::Environment>, Error> {
    let pins = config.local_pins()?;
    if pins.is_empty() {
        return Ok(None);
    }
    model::check(&config.models, &pins)?;
    let runtime = model::runtime_pin().ok_or("no pinned ONNX Runtime for this platform")?;
    Ok(Some(runtime::init(&config.models, &runtime)?))
}

/// Progress key of the US$ (in millionths) a rebuild has spent.
const SPENT: &str = "spent_microusd";
/// Documents read to estimate a rebuild.
const ESTIMATE_SAMPLE: usize = 256;

/// Estimate what embedding everything the host index holds costs with
/// `embedder`, from a uniform sample of its documents read and cut exactly as
/// the embedder will. Identical text embedded once makes the real figure
/// lower.
pub fn estimate(host: &HostCatalog, embedder: &Embedder, max_usd: f64) -> Result<Rebuild, Error> {
    let documents = host.count()?;
    let sample = host.sample(ESTIMATE_SAMPLE)?;
    let mut tokens = 0u64;
    let mut read = 0u64;
    for document in &sample {
        let Ok(raw) = anyagents_core::raw_segment::read_raw(&document.raw) else {
            continue;
        };
        read += 1;
        tokens += embedder
            .chunks_of(&raw)?
            .iter()
            .map(|c| c.tokens as u64)
            .sum::<u64>();
    }
    let estimated_tokens = if read == 0 {
        0
    } else {
        (tokens as f64 / read as f64 * documents as f64).round() as u64
    };
    Ok(Rebuild {
        documents,
        sampled_documents: read,
        estimated_tokens,
        estimated_usd: embedder
            .price()
            .map(|per_million| per_million * estimated_tokens as f64 / 1e6),
        max_usd,
        state: String::new(),
        spent_usd: 0.0,
    })
}

/// May the backlog of a new space be embedded? A local model always may; a
/// remote one only within the cap, and only with a known price.
fn rebuild_state(estimate: &Rebuild, remote: bool) -> String {
    match estimate.estimated_usd {
        _ if !remote => "running".into(),
        Some(usd) if usd <= estimate.max_usd => "running".into(),
        Some(usd) => format!(
            "waiting: estimated US${usd:.2} exceeds max_rebuild_usd US${:.2}",
            estimate.max_usd
        ),
        None => "waiting: the model's price is unknown; set usd_per_million_tokens".into(),
    }
}

struct Shared {
    config: Config,
    stamp: Stamp,
    embedder_name: String,
    embedder: Mutex<Embedder>,
    reranker: Mutex<Reranker>,
    rebuild: Mutex<Option<Rebuild>>,
    /// US$ spent by background workers since the service started.
    spent_microusd: std::sync::atomic::AtomicU64,
    stop: AtomicBool,
}

impl Shared {
    fn dense(&self) -> Result<DenseIndex, Error> {
        Ok(DenseIndex::open(
            &self.config.dense_path(&self.stamp),
            &self.stamp.key(),
            self.stamp.dimensions,
        )?)
    }

    fn status(&self) -> Result<Status, Error> {
        let dense = self.dense()?;
        let (embedded, vectors) = dense.counts()?;
        let host = self.config.data_dir.join(HOST_INDEX);
        let total = if host.exists() {
            HostCatalog::open(&host)?.count()?
        } else {
            0
        };
        let query_usd = self.embedder.lock().map_or(0.0, |e| e.usage().usd)
            + self.reranker.lock().map_or(0.0, |r| r.usage().usd);
        let mut rebuild = self.rebuild.lock().map_err(|_| "poisoned")?.clone();
        if let Some(rebuild) = &mut rebuild {
            rebuild.spent_usd = dense.progress(SPENT)?.unwrap_or(0) as f64 / 1e6;
            if rebuild.state == "running" && backlog_done(&dense)? {
                rebuild.state = "done".into();
            }
        }
        Ok(Status {
            embedder: self.embedder_name.clone(),
            reranker: self
                .reranker
                .lock()
                .map_or_else(|_| String::new(), |r| r.name()),
            embedded_documents: embedded,
            pending_documents: total.saturating_sub(embedded),
            vectors,
            dense_space: self.stamp.key(),
            rebuild,
            spent_usd: query_usd + self.spent_microusd.load(Ordering::Relaxed) as f64 / 1e6,
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
                let hits = self.dense()?.search(
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
                Ok(Reply::Rerank {
                    scores: reranker.score(&query, &refs)?,
                })
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
    // Before ONNX Runtime starts its threads: see `bind`.
    let listener = bind(&protocol::socket_path(&config.data_dir))?;
    let environment = local_runtime(&config)?;
    let local = environment.map(|environment| Local {
        environment,
        models: &config.models,
        threads: config.query_threads,
    });
    let embedder = Embedder::load(&config.providers.embedder, &config.data_dir, local.as_ref())?;
    let reranker = Reranker::load(&config.providers.reranker, &config.data_dir, local.as_ref())?;
    let stamp = embedder.stamp();
    let embedder_name = match &config.providers.embedder {
        EmbedderConfig::Local { .. } => stamp.model.clone(),
        EmbedderConfig::OpenAi(remote) => format!("{} ({})", remote.model, remote.profile),
    };
    let shared = Arc::new(Shared {
        embedder_name,
        stamp: stamp.clone(),
        embedder: Mutex::new(embedder),
        reranker: Mutex::new(reranker),
        rebuild: Mutex::new(None),
        spent_microusd: std::sync::atomic::AtomicU64::new(0),
        config: config.clone(),
        stop: AtomicBool::new(false),
    });
    if !config.no_index {
        // A space whose backlog is not embedded yet is a rebuild: say what
        // it will read and cost before any more of it runs.
        let host_path = config.data_dir.join(HOST_INDEX);
        let pending = !backlog_done(&shared.dense()?)?;
        if pending && host_path.exists() {
            let host = HostCatalog::open(&host_path)?;
            let embedder = shared.embedder.lock().map_err(|_| "embedder poisoned")?;
            let mut rebuild = estimate(&host, &embedder, config.max_rebuild_usd())?;
            rebuild.state = rebuild_state(&rebuild, embedder.is_remote());
            drop(embedder);
            eprintln!(
                "memory-service: dense space {} to build: {} documents, about {} tokens, {}; {}",
                stamp.key(),
                rebuild.documents,
                rebuild.estimated_tokens,
                rebuild
                    .estimated_usd
                    .map_or("price unknown".to_owned(), |usd| format!(
                        "about US${usd:.2}"
                    )),
                rebuild.state
            );
            let path = config.dense_path(&stamp).with_extension("estimate.json");
            std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))?;
            std::fs::write(&path, serde_json::to_vec_pretty(&rebuild)?)?;
            *shared.rebuild.lock().map_err(|_| "poisoned")? = Some(rebuild);
        }
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
        shared.embedder_name,
        shared
            .reranker
            .lock()
            .map_or_else(|_| String::new(), |r| r.name()),
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
/// encoders use cores best one batch per core. The first worker also sweeps,
/// and drops other spaces' indexes once this one has no backlog.
fn index_loop(
    shared: &Shared,
    environment: Option<&'static ort::environment::Environment>,
    stripe: Stripe,
) -> Result<(), Error> {
    let config = &shared.config;
    let local = environment.map(|environment| Local {
        environment,
        models: &config.models,
        threads: 1,
    });
    let mut encoder = Embedder::load(&config.providers.embedder, &config.data_dir, local.as_ref())?;
    if encoder.stamp() != shared.stamp {
        return Err("the embedder changed its space while loading".into());
    }
    let mut dense = shared.dense()?;
    let host_path = config.data_dir.join(HOST_INDEX);
    let mut last_sweep = Instant::now();
    while !shared.stop.load(Ordering::Relaxed) {
        if !host_path.exists() {
            std::thread::sleep(IDLE_POLL);
            continue;
        }
        // A space waiting for the operator embeds nothing; a remote rebuild
        // stops taking backlog at its cap, whatever the estimate said.
        let (embedding, backlog) = {
            let rebuild = shared.rebuild.lock().map_err(|_| "poisoned")?;
            let spent = dense.progress(SPENT)?.unwrap_or(0) as f64 / 1e6;
            match rebuild.as_ref() {
                Some(r) if r.state != "running" => (false, false),
                _ => (
                    true,
                    !encoder.is_remote() || spent < config.max_rebuild_usd(),
                ),
            }
        };
        if !embedding {
            std::thread::sleep(IDLE_POLL);
            continue;
        }
        let host = HostCatalog::open(&host_path)?;
        let before = encoder.usage().usd;
        let did = index_step(&host, &mut encoder, &mut dense, stripe, backlog)?;
        let spent = encoder.usage().usd - before;
        if spent > 0.0 {
            let micro = (spent * 1e6).round() as i64;
            shared
                .spent_microusd
                .fetch_add(micro as u64, Ordering::Relaxed);
            let total = dense.progress(SPENT)?.unwrap_or(0) + micro;
            dense.set_progress(SPENT, total)?;
        }
        if stripe.index == 0 && last_sweep.elapsed() >= SWEEP_EVERY {
            sweep(&host, &dense)?;
            if backlog_done(&dense)? {
                prune_other_spaces(&config.dense_path(&shared.stamp))?;
            }
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
/// embedded by its digest. With `backlog` false only new documents are
/// embedded.
pub fn index_step(
    host: &HostCatalog,
    encoder: &mut Embedder,
    dense: &mut DenseIndex,
    stripe: Stripe,
    backlog: bool,
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
    if !backlog {
        return Ok(0);
    }
    let backfill = dense.progress(&backfill_key)?.unwrap_or(0);
    let older = host.before(backfill, BATCH, stripe)?;
    embed_documents(encoder, dense, &older, &backfill_key)?;
    if older.is_empty() && backfill > 0 {
        dense.set_progress(&backfill_key, 0)?;
    }
    Ok(older.len())
}

/// Every stripe that started has embedded its whole backlog.
fn backlog_done(dense: &DenseIndex) -> Result<bool, Error> {
    let marks = dense.progress_with_prefix("backfill/")?;
    Ok(!marks.is_empty() && marks.iter().all(|(_, v)| *v == 0))
}

/// Remove the indexes of other vector spaces beside `keep`: they are
/// rebuildable projections of the host index, and this one now covers it.
pub fn prune_other_spaces(keep: &Path) -> Result<usize, Error> {
    let Some(dir) = keep.parent() else {
        return Ok(0);
    };
    let keep_name = keep
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    let mut removed = 0;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let base = name
            .strip_suffix("-wal")
            .or_else(|| name.strip_suffix("-shm"))
            .unwrap_or(name);
        let estimate = base
            .strip_suffix(".estimate.json")
            .map(|b| format!("{b}.db"));
        let base = estimate.as_deref().unwrap_or(base);
        if base.starts_with("dense-") && base.ends_with(".db") && base != keep_name {
            std::fs::remove_file(&path)?;
            removed += 1;
        }
    }
    Ok(removed)
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
    use crate::providers::{Endpoint, Remote};
    use serde_json::{Value, json};

    fn host_with(dir: &Path, lines: usize) -> PathBuf {
        use anyagents_core::{
            host_index::{HostIndex, IndexMetadata, MemoryScope},
            raw_segment::retained_jsonl_records,
        };
        let file = dir.join("history.jsonl");
        let text: String = (0..lines)
            .map(|i| format!("{{\"content\":\"message number {i} about deploys\"}}\n"))
            .collect();
        std::fs::write(&file, text).unwrap();
        let path = dir.join(HOST_INDEX);
        let mut index = HostIndex::open(&path).unwrap();
        for (i, raw) in retained_jsonl_records(&file).unwrap().iter().enumerate() {
            let metadata = IndexMetadata {
                agent_id: "agt".into(),
                source: MemoryScope::Transcripts,
                timestamp_ms: Some(i as i64),
                direction: "in".into(),
                sender: "s".into(),
                destination: "d".into(),
                tool_name: None,
                tool_names: Vec::new(),
                role: None,
            };
            index.index(&format!("doc{i}"), &metadata, raw).unwrap();
        }
        path
    }

    fn remote(price: Option<f64>, model: &str) -> Remote {
        Remote {
            profile: "openrouter/default".into(),
            model: model.into(),
            dimensions: Some(2),
            usd_per_million_tokens: price,
            ..Remote::default()
        }
    }

    /// A new embedding model is a new space in its own file. Before its
    /// backlog runs, the rebuild is estimated from the history it will read;
    /// a remote model above the cap, or with no known price, waits. New
    /// history is still embedded only when the space runs, and the old
    /// space's files go once the new one has no backlog.
    #[test]
    fn a_new_embedding_space_is_estimated_capped_built_apart_and_replaces_the_old() {
        let dir = tempfile::tempdir().unwrap();
        let host_path = host_with(dir.path(), 12);
        let host = HostCatalog::open(&host_path).unwrap();
        let (url, seen) = crate::providers::tests::serve(|_, body: &Value| {
            let n = body["input"].as_array().unwrap().len();
            let data: Vec<Value> = (0..n)
                .map(|i| json!({"index": i, "embedding": [1.0, i as f64]}))
                .collect();
            (
                200,
                json!({"data": data, "usage": {"prompt_tokens": 5 * n}}),
            )
        });
        let endpoint = || Endpoint::for_test(&format!("{url}/v1"), "k");
        let pricey = Embedder::remote(remote(Some(1e6), "a/pricey"), endpoint()).unwrap();
        let estimate_pricey = estimate(&host, &pricey, 1.0).unwrap();
        assert_eq!(estimate_pricey.documents, 12);
        assert_eq!(estimate_pricey.sampled_documents, 12);
        assert!(estimate_pricey.estimated_tokens >= 12 * 4);
        assert!(estimate_pricey.estimated_usd.unwrap() > 1.0);
        assert!(rebuild_state(&estimate_pricey, true).starts_with("waiting: estimated"));
        let unpriced = Embedder::remote(remote(None, "a/unpriced"), endpoint()).unwrap();
        let estimate_unpriced = estimate(&host, &unpriced, 1.0).unwrap();
        assert_eq!(estimate_unpriced.estimated_usd, None);
        assert!(rebuild_state(&estimate_unpriced, true).contains("price is unknown"));
        assert_eq!(rebuild_state(&estimate_unpriced, false), "running");
        assert!(seen.lock().unwrap().is_empty(), "estimating calls no model");

        let mut cheap = Embedder::remote(remote(Some(0.01), "a/cheap"), endpoint()).unwrap();
        let estimate_cheap = estimate(&host, &cheap, 1.0).unwrap();
        assert_eq!(rebuild_state(&estimate_cheap, true), "running");

        let config = Config::new(dir.path());
        let old_path = config.dense_path(&pricey.stamp());
        let new_path = config.dense_path(&cheap.stamp());
        assert_ne!(old_path, new_path, "each space has its own file");
        let stamp = cheap.stamp();
        drop(DenseIndex::open(&old_path, &pricey.stamp().key(), 2).unwrap());
        std::fs::write(old_path.with_extension("estimate.json"), "{}").unwrap();
        let mut dense = DenseIndex::open(&new_path, &stamp.key(), 2).unwrap();
        // First step: everything published is backlog, and none of it is
        // embedded while the backlog is held back.
        assert_eq!(
            index_step(&host, &mut cheap, &mut dense, Stripe::ALL, false).unwrap(),
            0
        );
        assert_eq!(dense.counts().unwrap().0, 0);
        assert!(!backlog_done(&dense).unwrap());
        while index_step(&host, &mut cheap, &mut dense, Stripe::ALL, true).unwrap() > 0 {}
        assert_eq!(dense.counts().unwrap().0, 12);
        assert!(backlog_done(&dense).unwrap());
        assert!(cheap.usage().usd > 0.0);
        assert_eq!(prune_other_spaces(&new_path).unwrap(), 2);
        assert!(!old_path.exists() && new_path.exists());
    }

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
