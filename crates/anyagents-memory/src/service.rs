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

/// Progress key of the US$ (in millionths) a rebuild has spent. Only spend
/// while the space is building counts.
const SPENT: &str = "spent_microusd";
/// Progress key set once every stripe has embedded its backlog: the space is
/// built, and new history is embedded as it comes. A stamp change empties
/// the index and this mark with it.
const BUILT: &str = "built_ms";
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
pub fn rebuild_state(estimate: &Rebuild, remote: bool) -> String {
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

/// Is this space built? The first time every stripe's backlog is found
/// embedded, that is recorded, so a later restart with another worker count
/// (whose new marks start a cheap re-walk) is not a rebuild.
fn built(dense: &DenseIndex) -> Result<bool, Error> {
    if dense.progress(BUILT)?.is_some() {
        return Ok(true);
    }
    if !backlog_done(dense)? {
        return Ok(false);
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |d| d.as_millis() as i64);
    dense.set_progress(BUILT, now)?;
    Ok(true)
}

/// Before a space's backlog runs: what building it reads and costs, and
/// whether it may start. `None` when the space is built or there is no
/// history to estimate from yet.
pub fn plan_rebuild(
    host_path: &Path,
    dense: &DenseIndex,
    embedder: &Embedder,
    max_usd: f64,
) -> Result<Option<Rebuild>, Error> {
    if built(dense)? || !host_path.exists() {
        return Ok(None);
    }
    let host = HostCatalog::open(host_path)?;
    let mut rebuild = estimate(&host, embedder, max_usd)?;
    rebuild.spent_usd = dense.progress(SPENT)?.unwrap_or(0) as f64 / 1e6;
    rebuild.state = rebuild_state(&rebuild, embedder.is_remote());
    Ok(Some(rebuild))
}

/// May background workers embed now? A local model always may. A remote one
/// embeds nothing while its rebuild waits for the operator, and while its
/// space is building it stops, new history included, once the spend the
/// provider reports reaches `max_usd`: everything embedded before the space
/// is built is part of the rebuild's cost. A built space embeds new history
/// as it comes.
pub fn may_embed(
    rebuild: Option<&Rebuild>,
    remote: bool,
    building: bool,
    spent_usd: f64,
    max_usd: f64,
) -> bool {
    if !remote {
        return true;
    }
    if rebuild.is_some_and(|r| r.state != "running") {
        return false;
    }
    !building || spent_usd < max_usd
}

/// A rebuild's state as status reports it.
fn reported_state(rebuild: &Rebuild, built: bool, remote: bool) -> String {
    if rebuild.state != "running" {
        return rebuild.state.clone();
    }
    if built {
        return "done".into();
    }
    if remote && rebuild.spent_usd >= rebuild.max_usd {
        return format!(
            "stopped: reported spend US${:.2} reached max_rebuild_usd US${:.2}",
            rebuild.spent_usd, rebuild.max_usd
        );
    }
    "running".into()
}

/// What `anyagents memory-service estimate` reports.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Estimate {
    pub embedder: String,
    /// The vector space, when the embedder's width is known without calling
    /// it (`dimensions` set, or a local model).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub space: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dense_index: Option<PathBuf>,
    /// The space's index exists and holds its whole backlog: nothing to
    /// rebuild.
    pub built: bool,
    pub rebuild: Rebuild,
}

/// What building the configured embedder's space over the host index as it
/// stands would read and cost, and whether the service would start it: the
/// same estimate the service makes before a rebuild, made without calling a
/// model, reading a profile's key or writing anything.
pub fn estimate_configured(config: &Config) -> Result<Estimate, Error> {
    let host_path = config.data_dir.join(HOST_INDEX);
    if !host_path.exists() {
        return Err(format!("no host index at {}", host_path.display()).into());
    }
    let (embedder, name, width_known) = match &config.providers.embedder {
        EmbedderConfig::OpenAi(remote) => {
            let mut offline = (**remote).clone();
            let known = offline.dimensions.is_some();
            offline.dimensions = Some(offline.dimensions.unwrap_or(1));
            (
                Embedder::remote(offline, crate::providers::Endpoint::offline())?,
                format!("{} ({})", remote.model, remote.profile),
                known,
            )
        }
        local => {
            let environment = local_runtime(config)?;
            let runtime = environment.map(|environment| Local {
                environment,
                models: &config.models,
                threads: 1,
            });
            let embedder = Embedder::load(local, &config.data_dir, runtime.as_ref())?;
            let name = embedder.stamp().model;
            (embedder, name, true)
        }
    };
    let stamp = embedder.stamp();
    let dense_path = config.dense_path(&stamp);
    let built = width_known
        && dense_path.exists()
        && DenseIndex::open_read_only(&dense_path, &stamp.key(), stamp.dimensions)?
            .map(|dense| -> Result<bool, Error> {
                Ok(dense.progress(BUILT)?.is_some() || backlog_done(&dense)?)
            })
            .transpose()?
            .unwrap_or(false);
    let host = HostCatalog::open(&host_path)?;
    let mut rebuild = estimate(&host, &embedder, config.max_rebuild_usd())?;
    rebuild.state = if built {
        "done".into()
    } else {
        rebuild_state(&rebuild, embedder.is_remote())
    };
    Ok(Estimate {
        embedder: name,
        space: width_known.then(|| stamp.key()),
        dense_index: width_known.then_some(dense_path),
        built,
        rebuild,
    })
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
        let (query_usd, remote) = self
            .embedder
            .lock()
            .map_or((0.0, false), |e| (e.usage().usd, e.is_remote()));
        let query_usd = query_usd + self.reranker.lock().map_or(0.0, |r| r.usage().usd);
        let mut rebuild = self.rebuild.lock().map_err(|_| "poisoned")?.clone();
        if let Some(rebuild) = &mut rebuild {
            rebuild.spent_usd = dense.progress(SPENT)?.unwrap_or(0) as f64 / 1e6;
            rebuild.state = reported_state(rebuild, built(&dense)?, remote);
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
    run(listener, config)
}

/// Serve on a socket already bound, and embed in the background unless
/// `no_index`.
pub fn run(listener: UnixListener, config: Config) -> Result<(), Error> {
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
        let planned = {
            let embedder = shared.embedder.lock().map_err(|_| "embedder poisoned")?;
            plan_rebuild(
                &host_path,
                &shared.dense()?,
                &embedder,
                config.max_rebuild_usd(),
            )?
        };
        if let Some(rebuild) = planned {
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
        // stops at its cap, whatever the estimate said.
        let building = !built(&dense)?;
        let allowed = {
            let rebuild = shared.rebuild.lock().map_err(|_| "poisoned")?;
            let spent = dense.progress(SPENT)?.unwrap_or(0) as f64 / 1e6;
            may_embed(
                rebuild.as_ref(),
                encoder.is_remote(),
                building,
                spent,
                config.max_rebuild_usd(),
            )
        };
        if !allowed {
            std::thread::sleep(IDLE_POLL);
            continue;
        }
        let host = HostCatalog::open(&host_path)?;
        let before = encoder.usage().usd;
        let did = index_step(&host, &mut encoder, &mut dense, stripe, true)?;
        let spent = encoder.usage().usd - before;
        if spent > 0.0 {
            let micro = (spent * 1e6).round() as i64;
            shared
                .spent_microusd
                .fetch_add(micro as u64, Ordering::Relaxed);
            if building {
                dense.add_progress(SPENT, micro)?;
            }
        }
        if stripe.index == 0 && last_sweep.elapsed() >= SWEEP_EVERY {
            sweep(&host, &dense)?;
            if built(&dense)? {
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
        let dir = crate::test_tempdir();
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

    /// A host index holding `docs` (owner, text), one document each, named
    /// `<owner>-<i>`.
    fn host_docs(dir: &Path, docs: &[(&str, &str)]) -> PathBuf {
        use anyagents_core::{
            host_index::{HostIndex, IndexMetadata, MemoryScope},
            raw_segment::retained_jsonl_records,
        };
        let file = dir.join("docs.jsonl");
        let text: String = docs
            .iter()
            .map(|(_, text)| json!({"content": text}).to_string() + "\n")
            .collect();
        std::fs::write(&file, text).unwrap();
        let path = dir.join(HOST_INDEX);
        let mut index = HostIndex::open(&path).unwrap();
        for (i, raw) in retained_jsonl_records(&file).unwrap().iter().enumerate() {
            let metadata = IndexMetadata {
                agent_id: docs[i].0.into(),
                source: MemoryScope::Transcripts,
                timestamp_ms: Some(i as i64),
                direction: "in".into(),
                sender: "s".into(),
                destination: "d".into(),
                tool_name: None,
                tool_names: Vec::new(),
                role: None,
            };
            index
                .index(&format!("{}-{i}", docs[i].0), &metadata, raw)
                .unwrap();
        }
        path
    }

    /// `openrouter/default` holding a key where the profile pool keeps it.
    fn openrouter_profile(dir: &Path) {
        let key = dir.join("profiles/openrouter/default/.config/opencode/openrouter.key");
        std::fs::create_dir_all(key.parent().unwrap()).unwrap();
        std::fs::write(key, "sk-or-test\n").unwrap();
    }

    /// The desk's pair, qwen3 embeddings and rerank-3 through
    /// `openrouter/default`, sent to a stub at `url`.
    fn desk_pair(url: &str, usd_per_million_tokens: Option<f64>, max: f64) -> ProviderConfig {
        use crate::providers::RerankerConfig;
        ProviderConfig {
            embedder: EmbedderConfig::OpenAi(Box::new(Remote {
                profile: "openrouter/default".into(),
                model: "qwen/qwen3-embedding-8b".into(),
                base_url: Some(format!("{url}/v1")),
                dimensions: Some(2),
                usd_per_million_tokens,
                query_prefix: "Q: ".into(),
                ..Remote::default()
            })),
            reranker: RerankerConfig::Rerank(Box::new(Remote {
                profile: "openrouter/default".into(),
                model: "mongodb/rerank-3".into(),
                base_url: Some(format!("{url}/v1")),
                usd_per_million_tokens: Some(0.05),
                ..Remote::default()
            })),
            max_rebuild_usd: Some(max),
        }
    }

    /// Run the service over `config` on its socket, as `serve` does.
    fn start(config: Config) -> protocol::Client {
        let socket = protocol::socket_path(&config.data_dir);
        let listener = UnixListener::bind(&socket).unwrap();
        std::thread::spawn(move || run(listener, config));
        protocol::Client::new(socket, Duration::from_secs(30))
    }

    fn status_of(client: &protocol::Client) -> Status {
        match client.call(&Request::Status).unwrap() {
            Reply::Status { status } => status,
            other => panic!("{other:?}"),
        }
    }

    fn wait_for(client: &protocol::Client, done: impl Fn(&Status) -> bool) -> Status {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let status = status_of(client);
            if done(&status) {
                return status;
            }
            assert!(Instant::now() < deadline, "timed out: {status:?}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// The owner's rule: a rebuild is estimated before it runs and refused
    /// above `max_rebuild_usd`. The admin's preview and the service make the
    /// same estimate; neither calls a model, and a refused rebuild embeds
    /// nothing.
    #[test]
    fn a_rebuild_above_its_cap_is_estimated_and_refused_before_any_model_call() {
        let dir = crate::test_tempdir();
        host_with(dir.path(), 12);
        openrouter_profile(dir.path());
        let (url, seen) = crate::providers::tests::serve(|_, _| (500, json!({})));
        let mut config = Config::new(dir.path());
        // US$1 per token: any build is far above a US$1 cap.
        config.providers = desk_pair(&url, Some(1e6), 1.0);

        let preview = estimate_configured(&config).unwrap();
        assert!(!preview.built);
        assert_eq!(preview.rebuild.documents, 12);
        assert_eq!(preview.rebuild.sampled_documents, 12);
        assert!(preview.rebuild.estimated_tokens >= 12 * 4);
        assert!(preview.rebuild.estimated_usd.unwrap() > 1.0);
        assert!(
            preview.rebuild.state.starts_with("waiting: estimated US$"),
            "{}",
            preview.rebuild.state
        );
        let dense_path = preview.dense_index.clone().unwrap();
        assert!(!dense_path.exists(), "the preview writes nothing");

        let client = start(config.clone());
        let status = wait_for(&client, |s| s.rebuild.is_some());
        let rebuild = status.rebuild.unwrap();
        assert_eq!(rebuild.documents, 12);
        assert!(rebuild.state.starts_with("waiting: estimated US$"));
        assert!(status.dense_space.contains("qwen/qwen3-embedding-8b"));
        let written: Rebuild = serde_json::from_slice(
            &std::fs::read(dense_path.with_extension("estimate.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(written.estimated_tokens, rebuild.estimated_tokens);
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(status_of(&client).embedded_documents, 0);
        assert!(seen.lock().unwrap().is_empty(), "no model was called");

        // With no known price a remote rebuild waits too.
        config.providers = desk_pair(&url, None, 1.0);
        let unpriced = estimate_configured(&config).unwrap();
        assert_eq!(unpriced.rebuild.estimated_usd, None);
        assert!(unpriced.rebuild.state.contains("price is unknown"));
    }

    /// A rebuild within its estimate still stops when the spend the provider
    /// reports reaches the cap, and history published while it builds counts
    /// toward it.
    #[test]
    fn a_building_space_stops_when_reported_spend_reaches_the_cap() {
        let dir = crate::test_tempdir();
        // More than two background steps of documents.
        host_with(dir.path(), 2 * BATCH + 2);
        openrouter_profile(dir.path());
        let (url, seen) = crate::providers::tests::serve(|_, body: &Value| {
            let n = body["input"].as_array().unwrap().len();
            let data: Vec<Value> = (0..n)
                .map(|i| json!({"index": i, "embedding": [1.0, i as f64]}))
                .collect();
            // Each request reports US$1.
            (
                200,
                json!({"data": data, "usage": {"prompt_tokens": n, "cost": 1.0}}),
            )
        });
        let mut config = Config::new(dir.path());
        config.providers = desk_pair(&url, Some(0.01), 1.5);
        let client = start(config);
        let status = wait_for(&client, |s| {
            s.rebuild
                .as_ref()
                .is_some_and(|r| r.state.starts_with("stopped"))
        });
        let rebuild = status.rebuild.unwrap();
        assert!(
            rebuild.spent_usd >= 1.5 && rebuild.spent_usd < 3.0,
            "{rebuild:?}"
        );
        assert!(
            rebuild.state.contains("max_rebuild_usd US$1.50"),
            "{}",
            rebuild.state
        );
        let requests = seen.lock().unwrap().len();
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(
            seen.lock().unwrap().len(),
            requests,
            "nothing more is embedded"
        );
        let status = status_of(&client);
        assert!(status.pending_documents > 0);
        assert!(status.embedded_documents < (2 * BATCH + 2) as u64);
    }

    /// What may run, by the rebuild's state, its spend and whether the space
    /// is built.
    #[test]
    fn the_rebuild_gate_follows_state_spend_and_completion() {
        let rebuild = |state: &str, spent_usd: f64| Rebuild {
            state: state.into(),
            spent_usd,
            max_usd: 10.0,
            ..Rebuild::default()
        };
        let running = rebuild("running", 0.0);
        let waiting = rebuild(
            "waiting: estimated US$12.00 exceeds max_rebuild_usd US$10.00",
            0.0,
        );
        // A local model is never held.
        assert!(may_embed(Some(&waiting), false, true, 99.0, 10.0));
        // A remote model waiting for the operator embeds nothing at all.
        assert!(!may_embed(Some(&waiting), true, true, 0.0, 10.0));
        assert!(!may_embed(Some(&waiting), true, false, 0.0, 10.0));
        // While building, the cap stops everything, new history included.
        assert!(may_embed(Some(&running), true, true, 9.99, 10.0));
        assert!(!may_embed(Some(&running), true, true, 10.0, 10.0));
        assert!(!may_embed(None, true, true, 10.0, 10.0));
        // A built space embeds new history as it comes.
        assert!(may_embed(Some(&running), true, false, 10.0, 10.0));
        assert!(may_embed(None, true, false, 50.0, 10.0));

        assert_eq!(reported_state(&running, false, true), "running");
        assert_eq!(reported_state(&running, true, true), "done");
        assert!(reported_state(&rebuild("running", 10.0), false, true).starts_with("stopped"));
        assert_eq!(
            reported_state(&rebuild("running", 10.0), false, false),
            "running"
        );
        assert_eq!(reported_state(&waiting, false, true), waiting.state);
    }

    /// The vector space is the stamp: another model, width, cut or passage
    /// prefix is another space, rebuilt in its own file while the old one
    /// keeps serving; a query prefix or a new reranker is not, and a built
    /// space is never rebuilt. A file holding vectors of another space is
    /// emptied and rebuilt.
    #[test]
    fn a_stamp_mismatch_triggers_a_rebuild_and_nothing_else_does() {
        let dir = crate::test_tempdir();
        let host_path = host_with(dir.path(), 12);
        let host = HostCatalog::open(&host_path).unwrap();
        let (url, seen) = crate::providers::tests::serve(|_, body: &Value| {
            let n = body["input"].as_array().unwrap().len();
            let data: Vec<Value> = (0..n)
                .map(|i| json!({"index": i, "embedding": [1.0, i as f64]}))
                .collect();
            (200, json!({"data": data, "usage": {"prompt_tokens": n}}))
        });
        let endpoint = || Endpoint::for_test(&format!("{url}/v1"), "k");
        let config = Config::new(dir.path());
        let mut built_model = Embedder::remote(remote(Some(0.01), "a/one"), endpoint()).unwrap();
        let stamp = built_model.stamp();
        let old_path = config.dense_path(&stamp);
        let mut old = DenseIndex::open(&old_path, &stamp.key(), 2).unwrap();
        let plan = plan_rebuild(&host_path, &old, &built_model, 10.0)
            .unwrap()
            .unwrap();
        assert_eq!((plan.documents, plan.state.as_str()), (12, "running"));
        while index_step(&host, &mut built_model, &mut old, Stripe::ALL, true).unwrap() > 0 {}
        assert!(
            plan_rebuild(&host_path, &old, &built_model, 10.0)
                .unwrap()
                .is_none(),
            "a built space is not rebuilt"
        );
        // A restart with more workers starts new marks: still built.
        old.set_progress("backfill/1/2", 99).unwrap();
        assert!(
            plan_rebuild(&host_path, &old, &built_model, 10.0)
                .unwrap()
                .is_none()
        );

        // Same space: a query instruction only shapes queries.
        let mut same = remote(Some(0.01), "a/one");
        same.query_prefix = "Instruct: find\nQuery: ".into();
        assert_eq!(Embedder::remote(same, endpoint()).unwrap().stamp(), stamp);

        let mut changes = Vec::new();
        changes.push(remote(Some(0.01), "a/two"));
        let mut cut = remote(Some(0.01), "a/one");
        cut.piece_tokens = Some(128);
        changes.push(cut);
        let mut prefixed = remote(Some(0.01), "a/one");
        prefixed.passage_prefix = "passage: ".into();
        changes.push(prefixed);
        let mut wider = remote(Some(0.01), "a/one");
        wider.dimensions = Some(4);
        changes.push(wider);
        for change in changes {
            let model = change.model.clone();
            let new = Embedder::remote(change, endpoint()).unwrap();
            assert_ne!(new.stamp(), stamp, "{model}");
            let path = config.dense_path(&new.stamp());
            assert_ne!(path, old_path, "{model}");
            let dense = DenseIndex::open(&path, &new.stamp().key(), new.dims()).unwrap();
            let plan = plan_rebuild(&host_path, &dense, &new, 10.0)
                .unwrap()
                .unwrap();
            assert_eq!(
                (plan.documents, plan.state.as_str()),
                (12, "running"),
                "{model}"
            );
            drop(dense);
            std::fs::remove_file(&path).unwrap();
        }

        // The old file opened under another space is emptied and rebuilt.
        drop(old);
        let mut other = Embedder::remote(remote(Some(0.01), "a/two"), endpoint()).unwrap();
        let reused = DenseIndex::open(&old_path, &other.stamp().key(), 2).unwrap();
        assert_eq!(reused.counts().unwrap(), (0, 0));
        assert!(
            plan_rebuild(&host_path, &reused, &other, 10.0)
                .unwrap()
                .is_some()
        );
        drop(reused);

        // A new space builds beside the old one, which goes once it is built.
        let mut old = DenseIndex::open(&old_path, &stamp.key(), 2).unwrap();
        while index_step(&host, &mut built_model, &mut old, Stripe::ALL, true).unwrap() > 0 {}
        let new_path = config.dense_path(&other.stamp());
        let mut new = DenseIndex::open(&new_path, &other.stamp().key(), 2).unwrap();
        while index_step(&host, &mut other, &mut new, Stripe::ALL, true).unwrap() > 0 {}
        assert_eq!(
            old.counts().unwrap().0,
            12,
            "the old space serves until then"
        );
        assert!(built(&new).unwrap());
        assert!(prune_other_spaces(&new_path).unwrap() >= 1);
        assert!(!old_path.exists() && new_path.exists());
        assert!(!seen.lock().unwrap().is_empty());
    }

    /// Hybrid retrieval end to end with stub providers: the service embeds
    /// the host index through the embeddings stub, answers dense search only
    /// for the owners asked, the kernel's fusion merges it with BM25, and the
    /// rerank stub's scores decide the final order. Every request carries
    /// OpenRouter's data policy.
    #[test]
    fn hybrid_retrieval_fuses_dense_with_bm25_and_the_reranker_orders_the_result() {
        use anyagents_core::{
            HistorySearchQuery,
            host_index::{HostIndex, MemoryScope},
        };
        let dir = crate::test_tempdir();
        let texts = [
            ("agt", "deploy failed on staging"),
            ("agt", "部署失敗 要 rollback"),
            ("agt", "lunch plans"),
            ("agt", "deploy notes for friday"),
            ("peer", "部署 peer secret"),
        ];
        let host_path = host_docs(dir.path(), &texts);
        openrouter_profile(dir.path());
        let (url, seen) = crate::providers::tests::serve(|path, body: &Value| {
            if path == "/v1/rerank" {
                let scores: Vec<Value> = body["documents"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(i, d)| {
                        let d = d.as_str().unwrap();
                        let score = if d.contains("friday") {
                            3.0
                        } else if d.contains("部署") {
                            2.0
                        } else if d.contains("staging") {
                            1.0
                        } else {
                            0.0
                        };
                        json!({"index": i, "relevance_score": score})
                    })
                    .collect();
                return (200, json!({"results": scores}));
            }
            // Queries and Cantonese text share a direction; English deploy
            // text is near it; the rest is orthogonal.
            let data: Vec<Value> = body["input"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, text)| {
                    let text = text.as_str().unwrap();
                    let v = if text.starts_with("Q: ") || text.contains("部署") {
                        [1.0, 0.0]
                    } else if text.contains("deploy") {
                        [0.6, 0.8]
                    } else {
                        [0.0, 1.0]
                    };
                    json!({"index": i, "embedding": v})
                })
                .collect();
            (200, json!({"data": data, "usage": {"prompt_tokens": 4}}))
        });
        let mut config = Config::new(dir.path());
        config.providers = desk_pair(&url, Some(0.01), 10.0);
        let client = start(config);
        wait_for(&client, |s| {
            s.embedded_documents == 5 && s.pending_documents == 0
        });

        let owners = vec!["agt".to_owned()];
        let lexical: Vec<String> = HostIndex::open(&host_path)
            .unwrap()
            .search(
                &owners,
                MemoryScope::Transcripts,
                &HistorySearchQuery {
                    query: Some("deploy".into()),
                    ..Default::default()
                },
            )
            .unwrap()
            .into_iter()
            .map(|hit| hit.id)
            .collect();
        let mut sorted = lexical.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec!["agt-0", "agt-3"],
            "BM25 finds the English text only"
        );
        let (hits, _) = client
            .search("deploy", &owners, None, None, None, 10)
            .unwrap();
        let dense: Vec<String> = hits.iter().map(|h| h.document_id.clone()).collect();
        assert_eq!(
            dense[0], "agt-1",
            "the Cantonese answer leads dense retrieval"
        );
        assert_eq!(dense.len(), 4, "only the owners asked: {dense:?}");
        assert!(hits.iter().all(|h| h.agent_id == "agt"));

        let fused: Vec<String> =
            crate::fusion::reciprocal_rank(&[lexical.clone(), dense], crate::fusion::K)
                .into_iter()
                .map(|(id, _)| id)
                .collect();
        let mut agreed = fused[..2].to_vec();
        agreed.sort();
        assert_eq!(agreed, vec!["agt-0", "agt-3"], "found by both ranks first");
        assert_eq!(&fused[2..], ["agt-1", "agt-2"]);

        let text_of = |id: &str| {
            let i: usize = id.rsplit('-').next().unwrap().parse().unwrap();
            texts[i].1.to_owned()
        };
        let passages: Vec<String> = fused.iter().map(|id| text_of(id)).collect();
        let scores = client.rerank("deploy", &passages).unwrap();
        let mut order: Vec<(f32, &String)> = scores.into_iter().zip(&fused).collect();
        order.sort_by(|a, b| b.0.total_cmp(&a.0));
        let reranked: Vec<&str> = order.iter().map(|(_, id)| id.as_str()).collect();
        assert_eq!(reranked, ["agt-3", "agt-1", "agt-0", "agt-2"]);

        let requests = seen.lock().unwrap();
        assert!(requests.iter().any(
            |(path, _, body)| path == "/v1/embeddings" && body["input"] == json!(["Q: deploy"])
        ));
        let rerank = requests
            .iter()
            .find(|(path, ..)| path == "/v1/rerank")
            .unwrap();
        assert_eq!(rerank.2["model"], json!("mongodb/rerank-3"));
        for (path, auth, body) in requests.iter() {
            assert_eq!(auth, "Bearer sk-or-test", "{path}");
            assert_eq!(body["provider"]["data_collection"], json!("deny"), "{path}");
        }
    }

    #[test]
    fn the_socket_is_owner_only_and_a_live_service_is_never_taken_over() {
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::test_tempdir();
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
