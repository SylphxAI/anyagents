//! `anyagents memory-service`: a separate process that embeds raw history in
//! the background and answers dense search and rerank requests over its
//! socket. It reads the kernel's host index and the raw sources read-only, and
//! writes only its own dense index.
use crate::{
    dense::DenseIndex,
    embed::{HostCatalog, Stripe, embed_documents, host_index_available},
    model::{self, ModelPin},
    protocol::{self, Rebuild, Reply, Request, Status},
    providers::{
        DEFAULT_MAX_DAILY_USD, DEFAULT_MAX_REBUILD_USD, Embedder, EmbedderConfig, Local,
        ProviderConfig, Reranker, Stamp, local_embedder_pin, local_reranker_pin,
    },
    runtime::{self, Error},
    text::readable,
};
use rusqlite::{Connection, OptionalExtension, params};
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
    /// How long an idle or held background worker waits before it looks
    /// again.
    pub poll: Duration,
    /// A failed background step is retried after this, doubling to
    /// [`RETRY_MAX`].
    pub retry: Duration,
}

/// The longest wait between two retries of a failing background worker.
pub const RETRY_MAX: Duration = Duration::from_secs(600);

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
            poll: Duration::from_secs(30),
            retry: Duration::from_secs(30),
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
    pub fn max_daily_usd(&self) -> f64 {
        self.providers
            .max_daily_usd
            .unwrap_or(DEFAULT_MAX_DAILY_USD)
    }
    /// The day ledger of spend outside rebuilds.
    pub fn ledger_path(&self) -> PathBuf {
        self.data_dir.join("memory-service").join("spend.db")
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

/// The progress marks of one worker's stripe. They are named by the host
/// index's generation and the worker count, so marks another worker count
/// left, or marks of a host index whose rowids have since started over, are
/// never read: a new set walks the catalog again, and documents already
/// embedded at the same digest are only marked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Marks {
    pub tail: String,
    pub backfill: String,
}

impl Marks {
    pub fn of(generation: &str, stripe: Stripe) -> Self {
        Self {
            tail: format!("tail/{generation}/{}/{}", stripe.of, stripe.index),
            backfill: format!("backfill/{generation}/{}/{}", stripe.of, stripe.index),
        }
    }
}

/// Has every stripe of `workers` embedded the backlog of this host index
/// generation? Marks of another worker count or generation do not count.
fn backlog_done(dense: &DenseIndex, generation: &str, workers: usize) -> Result<bool, Error> {
    let of = workers.max(1);
    for index in 0..of {
        let marks = Marks::of(generation, Stripe { index, of });
        if dense.progress(&marks.backfill)? != Some(0) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Is this space built? The first time every stripe's backlog is found
/// embedded, that is recorded, so a later restart with another worker count,
/// or a rebuilt host index (whose new marks start a re-walk that pays only
/// for history not yet embedded), is not a rebuild.
fn built(dense: &DenseIndex, generation: &str, workers: usize) -> Result<bool, Error> {
    if dense.progress(BUILT)?.is_some() {
        return Ok(true);
    }
    if !backlog_done(dense, generation, workers)? {
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
    workers: usize,
) -> Result<Option<Rebuild>, Error> {
    if !host_index_available(host_path) {
        return Ok(None);
    }
    let host = HostCatalog::open(host_path)?;
    if built(dense, &host.generation()?, workers)? {
        return Ok(None);
    }
    let mut rebuild = estimate(&host, embedder, max_usd)?;
    rebuild.spent_usd = dense.progress(SPENT)?.unwrap_or(0) as f64 / 1e6;
    rebuild.state = rebuild_state(&rebuild, embedder.is_remote());
    Ok(Some(rebuild))
}

/// What remote models have spent, against their caps.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Budget {
    /// Reported spend of the rebuild of this space, and its cap.
    pub rebuild_usd: f64,
    pub max_rebuild_usd: f64,
    /// Spend outside the rebuild today (UTC), and its cap.
    pub today_usd: f64,
    pub max_daily_usd: f64,
}

/// May background workers embed now? `Err` says why they wait. A local model
/// always may. A remote one waits while its rebuild waits for the operator.
/// While the space is building, everything it embeds, new history included,
/// is the rebuild's cost and stops at `max_rebuild_usd`. Once it is built,
/// new history is embedded until today's spend outside rebuilds reaches
/// `max_daily_usd`, and waits for the next UTC day.
pub fn gate(
    rebuild: Option<&Rebuild>,
    remote: bool,
    building: bool,
    budget: Budget,
) -> Result<(), String> {
    if !remote {
        return Ok(());
    }
    if let Some(rebuild) = rebuild.filter(|r| r.state != "running") {
        return Err(rebuild.state.clone());
    }
    if building && budget.rebuild_usd >= budget.max_rebuild_usd {
        return Err(format!(
            "stopped: reported spend US${:.2} reached max_rebuild_usd US${:.2}",
            budget.rebuild_usd, budget.max_rebuild_usd
        ));
    }
    if !building && budget.today_usd >= budget.max_daily_usd {
        return Err(daily_cap_reached(budget.today_usd, budget.max_daily_usd));
    }
    Ok(())
}

fn daily_cap_reached(today_usd: f64, max_daily_usd: f64) -> String {
    format!(
        "paused: US${today_usd:.2} spent today reached max_daily_usd US${max_daily_usd:.2}; resumes at 00:00 UTC"
    )
}

/// A rebuild's state as status reports it.
fn reported_state(rebuild: &Rebuild, built: bool, remote: bool, error: Option<&str>) -> String {
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
    if let Some(error) = error {
        return format!("retrying: {error}");
    }
    "running".into()
}

/// Spend outside rebuilds, per UTC day, in its own small database so a
/// restart does not reset the day's cap.
pub struct Ledger {
    conn: Mutex<Connection>,
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Self, Error> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(10))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS daily(day INTEGER PRIMARY KEY, usd REAL NOT NULL);",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Days since the Unix epoch, UTC.
    pub fn day_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| (d.as_secs() / 86_400) as i64)
    }

    pub fn spent_on(&self, day: i64) -> Result<f64, Error> {
        let conn = self.conn.lock().map_err(|_| "ledger poisoned")?;
        Ok(conn
            .query_row("SELECT usd FROM daily WHERE day=?1", [day], |r| r.get(0))
            .optional()?
            .unwrap_or(0.0))
    }

    /// Add `usd` to `day` and return the day's total.
    pub fn charge_on(&self, day: i64, usd: f64) -> Result<f64, Error> {
        if usd <= 0.0 {
            return self.spent_on(day);
        }
        let conn = self.conn.lock().map_err(|_| "ledger poisoned")?;
        Ok(conn.query_row(
            "INSERT INTO daily VALUES(?1,?2) ON CONFLICT(day) DO UPDATE SET usd=usd+excluded.usd
             RETURNING usd",
            params![day, usd],
            |r| r.get(0),
        )?)
    }

    pub fn today(&self) -> Result<f64, Error> {
        self.spent_on(Self::day_now())
    }

    pub fn charge(&self, usd: f64) -> Result<f64, Error> {
        self.charge_on(Self::day_now(), usd)
    }
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

/// The configured embedder, made only to count and stamp: a remote one is
/// never called and its profile's key is not read. Also its name, and
/// whether its width (so its space) is known without calling it.
fn configured_embedder(config: &Config) -> Result<(Embedder, String, bool), Error> {
    match &config.providers.embedder {
        EmbedderConfig::OpenAi(remote) => {
            let mut offline = (**remote).clone();
            let known = offline.dimensions.is_some();
            offline.dimensions = Some(offline.dimensions.unwrap_or(1));
            Ok((
                Embedder::remote(offline, crate::providers::Endpoint::offline())?,
                format!("{} ({})", remote.model, remote.profile),
                known,
            ))
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
            Ok((embedder, name, true))
        }
    }
}

/// Has the service recorded this space as built? Read-only.
fn recorded_built(path: &Path, stamp: &Stamp) -> Result<bool, Error> {
    if !path.exists() {
        return Ok(false);
    }
    Ok(
        match DenseIndex::open_read_only(path, &stamp.key(), stamp.dimensions)? {
            Some(dense) => dense.progress(BUILT)?.is_some(),
            None => false,
        },
    )
}

/// What building the configured embedder's space over the host index as it
/// stands would read and cost, and whether the service would start it: the
/// same estimate the service makes before a rebuild, made without calling a
/// model, reading a profile's key or writing anything.
pub fn estimate_configured(config: &Config) -> Result<Estimate, Error> {
    let host_path = config.data_dir.join(HOST_INDEX);
    if !host_index_available(&host_path) {
        return Err(format!("no host index at {}", host_path.display()).into());
    }
    let (embedder, name, width_known) = configured_embedder(config)?;
    let stamp = embedder.stamp();
    let dense_path = config.dense_path(&stamp);
    let built = width_known && recorded_built(&dense_path, &stamp)?;
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

/// `anyagents memory-service prune`: remove the dense indexes of every other
/// vector space, once the configured one is recorded as built. Nothing
/// removes a space on its own: a paid index is deleted only by this
/// command.
pub fn prune(config: &Config) -> Result<Vec<PathBuf>, Error> {
    let (embedder, name, width_known) = configured_embedder(config)?;
    if !width_known {
        return Err(format!(
            "the space of {name} is learned from the model; set dimensions to prune"
        )
        .into());
    }
    let stamp = embedder.stamp();
    let keep = config.dense_path(&stamp);
    if !recorded_built(&keep, &stamp)? {
        return Err(format!(
            "{name}'s space ({}) is not built yet; nothing removed",
            keep.display()
        )
        .into());
    }
    prune_other_spaces(&keep)
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
    /// Spend outside rebuilds, per UTC day.
    ledger: Ledger,
    /// Per background worker: its last error while it retries, and why it
    /// waits.
    workers: Mutex<Vec<WorkerState>>,
    stop: AtomicBool,
}

#[derive(Clone, Debug, Default)]
struct WorkerState {
    error: Option<String>,
    waiting: Option<String>,
}

impl Shared {
    fn dense(&self) -> Result<DenseIndex, Error> {
        Ok(DenseIndex::open(
            &self.config.dense_path(&self.stamp),
            &self.stamp.key(),
            self.stamp.dimensions,
        )?)
    }

    fn set_worker(&self, index: usize, update: impl FnOnce(&mut WorkerState)) {
        if let Ok(mut workers) = self.workers.lock()
            && let Some(state) = workers.get_mut(index)
        {
            update(state);
        }
    }

    fn status(&self) -> Result<Status, Error> {
        let dense = self.dense()?;
        let (embedded, vectors) = dense.counts()?;
        let host = self.config.data_dir.join(HOST_INDEX);
        let (total, generation) = if host_index_available(&host) {
            let host = HostCatalog::open(&host)?;
            (host.count()?, host.generation()?)
        } else {
            (0, "legacy".to_owned())
        };
        let (query_usd, remote) = self
            .embedder
            .lock()
            .map_or((0.0, false), |e| (e.usage().usd, e.is_remote()));
        let query_usd = query_usd + self.reranker.lock().map_or(0.0, |r| r.usage().usd);
        let workers = self.workers.lock().map_err(|_| "poisoned")?.clone();
        let errors: Vec<String> = workers
            .iter()
            .enumerate()
            .filter_map(|(i, w)| w.error.as_ref().map(|e| format!("worker {i}: {e}")))
            .collect();
        let indexing = if self.config.no_index {
            "off".to_owned()
        } else if !errors.is_empty() {
            format!("retrying: {}", errors[0])
        } else if let Some(waiting) = workers.iter().find_map(|w| w.waiting.clone()) {
            waiting
        } else if total > embedded {
            "running".to_owned()
        } else {
            "idle".to_owned()
        };
        let built = built(&dense, &generation, self.config.index_threads.max(1))?;
        let mut rebuild = self.rebuild.lock().map_err(|_| "poisoned")?.clone();
        if let Some(rebuild) = &mut rebuild {
            rebuild.spent_usd = dense.progress(SPENT)?.unwrap_or(0) as f64 / 1e6;
            rebuild.state =
                reported_state(rebuild, built, remote, errors.first().map(String::as_str));
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
            indexing,
            errors,
            today_usd: self.ledger.today()?,
            max_daily_usd: self.config.max_daily_usd(),
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
                // Past the daily cap a remote query is not embedded: the
                // server answers from BM25 and says why dense did not run.
                let vector = {
                    let mut embedder = self.embedder.lock().map_err(|_| "embedder poisoned")?;
                    if embedder.is_remote() {
                        let today = self.ledger.today()?;
                        let cap = self.config.max_daily_usd();
                        if today >= cap {
                            return Err(daily_cap_reached(today, cap).into());
                        }
                    }
                    let before = embedder.usage().usd;
                    let embedded = embedder.embed_queries(&[query.as_str()]);
                    self.ledger.charge(embedder.usage().usd - before)?;
                    embedded?.remove(0)
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
                if matches!(*reranker, Reranker::Remote(_)) {
                    let today = self.ledger.today()?;
                    let cap = self.config.max_daily_usd();
                    if today >= cap {
                        return Err(daily_cap_reached(today, cap).into());
                    }
                }
                let before = reranker.usage().usd;
                let scores = reranker.score(&query, &refs);
                self.ledger.charge(reranker.usage().usd - before)?;
                Ok(Reply::Rerank { scores: scores? })
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
    config.providers.require_prices()?;
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
        ledger: Ledger::open(&config.ledger_path())?,
        workers: Mutex::new(vec![WorkerState::default(); config.index_threads.max(1)]),
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
                config.index_threads.max(1),
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
                    index_loop(&indexer, environment, stripe);
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

/// How often worker 0 sweeps documents the host index dropped, and the
/// documents one background step reads.
const SWEEP_EVERY: Duration = Duration::from_secs(600);
const BATCH: usize = 64;

/// One background worker, for as long as the service runs. A step that
/// fails (a model, the network, a database) is retried after
/// `Config::retry`, doubling to [`RETRY_MAX`]; the worker never stops, and
/// status reports its last error until a step succeeds.
fn index_loop(
    shared: &Shared,
    environment: Option<&'static ort::environment::Environment>,
    stripe: Stripe,
) {
    let mut retry = shared.config.retry;
    let mut worker = None;
    let mut last_sweep = Instant::now();
    while !shared.stop.load(Ordering::Relaxed) {
        match index_once(shared, environment, stripe, &mut worker, &mut last_sweep) {
            Ok(wait) => {
                shared.set_worker(stripe.index, |w| w.error = None);
                retry = shared.config.retry;
                if !wait.is_zero() {
                    std::thread::sleep(wait);
                }
            }
            Err(error) => {
                let message = format!("{error} (next try in {} s)", retry.as_secs_f64().ceil());
                eprintln!("memory-service: worker {}: {message}", stripe.index);
                shared.set_worker(stripe.index, |w| w.error = Some(message));
                std::thread::sleep(retry);
                retry = (retry * 2).min(RETRY_MAX);
            }
        }
    }
}

/// One pass of one worker: embed new documents first (above the tail mark,
/// oldest first), then the backlog newest first (below the backfill mark),
/// if the caps allow. Both marks commit with each document, so a restart
/// resumes where it stopped. Each worker owns one stripe of rowids on a
/// single-threaded session. Spend is counted whether or not the step
/// succeeds: to the rebuild while the space builds, to the day after. The
/// first worker also sweeps. Returns how long to wait before the next pass.
fn index_once(
    shared: &Shared,
    environment: Option<&'static ort::environment::Environment>,
    stripe: Stripe,
    worker: &mut Option<(Embedder, DenseIndex)>,
    last_sweep: &mut Instant,
) -> Result<Duration, Error> {
    let config = &shared.config;
    let host_path = config.data_dir.join(HOST_INDEX);
    if !host_index_available(&host_path) {
        return Ok(config.poll);
    }
    if worker.is_none() {
        let local = environment.map(|environment| Local {
            environment,
            models: &config.models,
            threads: 1,
        });
        let encoder = Embedder::load(&config.providers.embedder, &config.data_dir, local.as_ref())?;
        if encoder.stamp() != shared.stamp {
            return Err("the embedder changed its space while loading".into());
        }
        *worker = Some((encoder, shared.dense()?));
    }
    let Some((encoder, dense)) = worker.as_mut() else {
        return Ok(config.poll);
    };
    let host = HostCatalog::open(&host_path)?;
    let building = !built(dense, &host.generation()?, stripe.of)?;
    let budget = Budget {
        rebuild_usd: dense.progress(SPENT)?.unwrap_or(0) as f64 / 1e6,
        max_rebuild_usd: config.max_rebuild_usd(),
        today_usd: shared.ledger.today()?,
        max_daily_usd: config.max_daily_usd(),
    };
    let allowed = {
        let rebuild = shared.rebuild.lock().map_err(|_| "poisoned")?;
        gate(rebuild.as_ref(), encoder.is_remote(), building, budget)
    };
    if let Err(reason) = allowed {
        shared.set_worker(stripe.index, |w| w.waiting = Some(reason));
        return Ok(config.poll);
    }
    shared.set_worker(stripe.index, |w| w.waiting = None);
    let before = encoder.usage().usd;
    let step = index_step(&host, encoder, dense, stripe, true);
    let spent = encoder.usage().usd - before;
    if spent > 0.0 {
        let micro = (spent * 1e6).round() as i64;
        shared
            .spent_microusd
            .fetch_add(micro as u64, Ordering::Relaxed);
        if building {
            dense.add_progress(SPENT, micro)?;
        } else {
            shared.ledger.charge(spent)?;
        }
    }
    let did = step?;
    if stripe.index == 0 && last_sweep.elapsed() >= SWEEP_EVERY {
        sweep(&host, dense)?;
        *last_sweep = Instant::now();
    }
    Ok(if did == 0 {
        config.poll
    } else {
        Duration::ZERO
    })
}

/// One bounded step of background work for one stripe; returns documents
/// examined. Marks are named by the host index's generation and the stripe
/// ([`Marks`]): a restart with another worker count, or a host index whose
/// rowids started over, starts fresh marks and skips what is already
/// embedded by its digest. With `backlog` false only new documents are
/// embedded.
pub fn index_step(
    host: &HostCatalog,
    encoder: &mut Embedder,
    dense: &mut DenseIndex,
    stripe: Stripe,
    backlog: bool,
) -> Result<usize, Error> {
    let marks = Marks::of(&host.generation()?, stripe);
    let tail = match dense.progress(&marks.tail)? {
        Some(tail) => tail,
        None => {
            // First run: everything already published is backlog.
            let top = host.max_rowid()?;
            dense.set_progress(&marks.tail, top)?;
            dense.set_progress(&marks.backfill, top + 1)?;
            top
        }
    };
    let fresh = host.after(tail, BATCH, stripe)?;
    if !fresh.is_empty() {
        embed_documents(encoder, dense, &fresh, &marks.tail)?;
        return Ok(fresh.len());
    }
    if !backlog {
        return Ok(0);
    }
    let backfill = dense.progress(&marks.backfill)?.unwrap_or(0);
    let older = host.before(backfill, BATCH, stripe)?;
    embed_documents(encoder, dense, &older, &marks.backfill)?;
    if older.is_empty() && backfill > 0 {
        dense.set_progress(&marks.backfill, 0)?;
    }
    Ok(older.len())
}

/// Remove the indexes of other vector spaces beside `keep`, with their
/// estimates; returns what was removed. Only `memory-service prune` calls
/// it.
pub fn prune_other_spaces(keep: &Path) -> Result<Vec<PathBuf>, Error> {
    let Some(dir) = keep.parent() else {
        return Ok(Vec::new());
    };
    let keep_name = keep
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    let mut removed = Vec::new();
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
            removed.push(path);
        }
    }
    removed.sort();
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
        let generation = host.generation().unwrap();
        assert!(!backlog_done(&dense, &generation, 1).unwrap());
        while index_step(&host, &mut cheap, &mut dense, Stripe::ALL, true).unwrap() > 0 {}
        assert_eq!(dense.counts().unwrap().0, 12);
        assert!(backlog_done(&dense, &generation, 1).unwrap());
        assert!(cheap.usage().usd > 0.0);
        assert_eq!(prune_other_spaces(&new_path).unwrap().len(), 2);
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
            max_daily_usd: None,
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

    /// What may run, by the rebuild's state, its spend, the day's spend and
    /// whether the space is built.
    #[test]
    fn the_gate_follows_rebuild_state_both_caps_and_completion() {
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
        let budget = |rebuild_usd: f64, today_usd: f64| Budget {
            rebuild_usd,
            max_rebuild_usd: 10.0,
            today_usd,
            max_daily_usd: 2.0,
        };
        // A local model is never held.
        assert!(gate(Some(&waiting), false, true, budget(99.0, 99.0)).is_ok());
        // A remote model waiting for the operator embeds nothing at all.
        assert_eq!(
            gate(Some(&waiting), true, true, budget(0.0, 0.0)),
            Err(waiting.state.clone())
        );
        assert!(gate(Some(&waiting), true, false, budget(0.0, 0.0)).is_err());
        // While building, the rebuild cap stops everything, new history
        // included; the day's spend does not count.
        assert!(gate(Some(&running), true, true, budget(9.99, 50.0)).is_ok());
        assert!(
            gate(Some(&running), true, true, budget(10.0, 0.0))
                .unwrap_err()
                .starts_with("stopped")
        );
        assert!(gate(None, true, true, budget(10.0, 0.0)).is_err());
        // A built space embeds new history until the day's cap.
        assert!(gate(Some(&running), true, false, budget(50.0, 1.99)).is_ok());
        assert!(
            gate(None, true, false, budget(0.0, 2.0))
                .unwrap_err()
                .contains("max_daily_usd US$2.00")
        );

        assert_eq!(reported_state(&running, false, true, None), "running");
        assert_eq!(reported_state(&running, true, true, None), "done");
        assert!(
            reported_state(&rebuild("running", 10.0), false, true, None).starts_with("stopped")
        );
        assert_eq!(
            reported_state(&rebuild("running", 10.0), false, false, None),
            "running"
        );
        assert_eq!(reported_state(&waiting, false, true, None), waiting.state);
        assert_eq!(
            reported_state(&running, false, true, Some("worker 0: HTTP 400")),
            "retrying: worker 0: HTTP 400"
        );
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
        let plan = plan_rebuild(&host_path, &old, &built_model, 10.0, 1)
            .unwrap()
            .unwrap();
        assert_eq!((plan.documents, plan.state.as_str()), (12, "running"));
        while index_step(&host, &mut built_model, &mut old, Stripe::ALL, true).unwrap() > 0 {}
        assert!(
            plan_rebuild(&host_path, &old, &built_model, 10.0, 1)
                .unwrap()
                .is_none(),
            "a built space is not rebuilt"
        );
        // A restart with more workers starts new marks: still built.
        assert!(
            plan_rebuild(&host_path, &old, &built_model, 10.0, 2)
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
            let plan = plan_rebuild(&host_path, &dense, &new, 10.0, 1)
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
            plan_rebuild(&host_path, &reused, &other, 10.0, 1)
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
        assert!(built(&new, &host.generation().unwrap(), 1).unwrap());
        assert!(!prune_other_spaces(&new_path).unwrap().is_empty());
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
        drop(requests);
        assert!(
            status_of(&client).today_usd > 0.0,
            "search spend counts to the day"
        );
    }

    /// An embeddings stub: each request answers with vectors and reports
    /// `cost`, except the requests `fail` names (by order, from 1), which
    /// are refused with HTTP 400. Rerank requests score every passage 0.5.
    #[allow(clippy::type_complexity)]
    fn stub_models(
        cost: f64,
        fail: &'static [usize],
    ) -> (String, std::sync::Arc<Mutex<Vec<(String, String, Value)>>>) {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::providers::tests::serve(move |path, body: &Value| {
            if path == "/v1/rerank" {
                let n = body["documents"].as_array().unwrap().len();
                let results: Vec<Value> = (0..n)
                    .map(|i| json!({"index": i, "relevance_score": 0.5}))
                    .collect();
                return (200, json!({"results": results, "usage": {"cost": cost}}));
            }
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if fail.contains(&call) {
                return (400, json!({"error": "refused"}));
            }
            let n = body["input"].as_array().unwrap().len();
            let data: Vec<Value> = (0..n)
                .map(|i| json!({"index": i, "embedding": [1.0, i as f64]}))
                .collect();
            (
                200,
                json!({"data": data, "usage": {"prompt_tokens": n, "cost": cost}}),
            )
        })
    }

    /// The desk pair's embedder, called at `url`, as the service loads it.
    fn desk_embedder(url: &str) -> Embedder {
        let EmbedderConfig::OpenAi(remote) = desk_pair(url, Some(0.01), 10.0).embedder else {
            unreachable!()
        };
        Embedder::remote(*remote, Endpoint::for_test(&format!("{url}/v1"), "k")).unwrap()
    }

    /// Build the configured space over the host index, as one worker would.
    fn build_space(config: &Config, url: &str) -> PathBuf {
        let mut embedder = desk_embedder(url);
        let path = config.dense_path(&embedder.stamp());
        let mut dense = DenseIndex::open(&path, &embedder.stamp().key(), 2).unwrap();
        let host = HostCatalog::open(&config.data_dir.join(HOST_INDEX)).unwrap();
        while index_step(&host, &mut embedder, &mut dense, Stripe::ALL, true).unwrap() > 0 {}
        assert!(built(&dense, &host.generation().unwrap(), 1).unwrap());
        path
    }

    /// Defect: with another `--index-threads` mid-build, marks of the old
    /// worker count never reached 0 and the space was never built. Only the
    /// current count's marks decide, and the re-walk pays only for what is
    /// not embedded yet.
    #[test]
    fn a_changed_worker_count_mid_build_still_builds_the_space() {
        let dir = crate::test_tempdir();
        let host_path = host_with(dir.path(), 12);
        let host = HostCatalog::open(&host_path).unwrap();
        let (url, seen) = stub_models(0.0, &[]);
        let mut embedder = desk_embedder(&url);
        let mut dense =
            DenseIndex::open(&dir.path().join("dense.db"), &embedder.stamp().key(), 2).unwrap();
        let generation = host.generation().unwrap();
        // Two workers: only the first runs one step, then the service
        // restarts with one worker.
        let first = Stripe { index: 0, of: 2 };
        assert!(index_step(&host, &mut embedder, &mut dense, first, true).unwrap() > 0);
        assert!(!backlog_done(&dense, &generation, 2).unwrap());
        while index_step(&host, &mut embedder, &mut dense, Stripe::ALL, true).unwrap() > 0 {}
        assert!(built(&dense, &generation, 1).unwrap());
        assert_eq!(dense.counts().unwrap().0, 12);
        let embedded: usize = seen
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, body)| body["input"].as_array().unwrap().len())
            .sum();
        assert_eq!(embedded, 12, "nothing is embedded twice");
    }

    /// Defect: a host index rebuilt from scratch restarts its rowids, and
    /// marks kept from the old one hid every new document. A new generation
    /// starts new marks.
    #[test]
    fn a_rebuilt_host_index_is_walked_again_from_its_new_rowids() {
        let dir = crate::test_tempdir();
        let host_path = host_with(dir.path(), 12);
        let (url, _) = stub_models(0.0, &[]);
        let mut embedder = desk_embedder(&url);
        let mut dense =
            DenseIndex::open(&dir.path().join("dense.db"), &embedder.stamp().key(), 2).unwrap();
        {
            let host = HostCatalog::open(&host_path).unwrap();
            while index_step(&host, &mut embedder, &mut dense, Stripe::ALL, true).unwrap() > 0 {}
        }
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", host_path.display()));
        }
        // Fewer documents than the old tail mark, all new.
        host_docs(
            dir.path(),
            &[
                ("agt", "one"),
                ("agt", "two"),
                ("agt", "three"),
                ("agt", "four"),
                ("agt", "five"),
            ],
        );
        let host = HostCatalog::open(&host_path).unwrap();
        while index_step(&host, &mut embedder, &mut dense, Stripe::ALL, true).unwrap() > 0 {}
        for i in 0..5 {
            assert!(
                dense.embedded_sha(&format!("agt-{i}")).unwrap().is_some(),
                "agt-{i}"
            );
        }
    }

    /// Defect: one failed request ended a worker for good while status said
    /// `running`. It now retries with backoff, status names the error, and
    /// it finishes the space.
    #[test]
    fn a_failing_worker_retries_reports_its_error_and_finishes() {
        let dir = crate::test_tempdir();
        host_with(dir.path(), 12);
        openrouter_profile(dir.path());
        let (url, _) = stub_models(0.01, &[1, 2, 3]);
        let mut config = Config::new(dir.path());
        config.providers = desk_pair(&url, Some(0.01), 10.0);
        config.retry = Duration::from_millis(150);
        config.poll = Duration::from_millis(50);
        let client = start(config);
        let failing = wait_for(&client, |s| !s.errors.is_empty());
        assert!(
            failing.errors[0].contains("HTTP 400"),
            "{:?}",
            failing.errors
        );
        assert!(
            failing.indexing.starts_with("retrying"),
            "{}",
            failing.indexing
        );
        let state = failing.rebuild.unwrap().state;
        assert!(state.starts_with("retrying: worker 0:"), "{state}");
        let done = wait_for(&client, |s| {
            s.errors.is_empty() && s.rebuild.as_ref().is_some_and(|r| r.state == "done")
        });
        assert_eq!(done.embedded_documents, 12);
    }

    /// Defect: a batch's model requests are all made before anything is
    /// written, so a failure in a later request threw away vectors already
    /// paid for. They are kept, their spend counted, and the retry pays only
    /// for the rest.
    #[test]
    fn a_failed_request_keeps_the_vectors_already_paid_for() {
        let dir = crate::test_tempdir();
        let host_path = host_with(dir.path(), 100);
        let host = HostCatalog::open(&host_path).unwrap();
        let (url, seen) = stub_models(1.0, &[2]);
        let mut embedder = desk_embedder(&url);
        let mut dense =
            DenseIndex::open(&dir.path().join("dense.db"), &embedder.stamp().key(), 2).unwrap();
        let documents = host.after(0, 100, Stripe::ALL).unwrap();
        let error = embed_documents(&mut embedder, &mut dense, &documents, "m").unwrap_err();
        assert!(error.to_string().contains("HTTP 400"), "{error}");
        assert_eq!(embedder.usage().requests, 1);
        assert!(
            (embedder.usage().usd - 1.0).abs() < 1e-9,
            "the paid request counts"
        );
        assert_eq!(dense.counts().unwrap().0, 64, "the paid batch is stored");
        embed_documents(&mut embedder, &mut dense, &documents, "m").unwrap();
        assert_eq!(dense.counts().unwrap().0, 100);
        let requests = seen.lock().unwrap();
        let last = &requests.last().unwrap().2;
        assert_eq!(last["input"].as_array().unwrap().len(), 36, "only the rest");
    }

    /// The day's cap on spend outside rebuilds is persisted and, once
    /// reached, holds query embedding, rerank and new-history embedding: no
    /// remote request is made until the next UTC day, which starts at zero.
    #[test]
    fn the_daily_cap_holds_queries_rerank_and_new_history() {
        let dir = crate::test_tempdir();
        let host_path = host_with(dir.path(), 12);
        openrouter_profile(dir.path());
        let (url, seen) = stub_models(0.5, &[]);
        let mut config = Config::new(dir.path());
        config.providers = desk_pair(&url, Some(0.01), 10.0);
        config.providers.max_daily_usd = Some(1.0);
        config.poll = Duration::from_millis(50);
        build_space(&config, &url);
        // New history after the space was built.
        {
            use anyagents_core::{
                host_index::{HostIndex, IndexMetadata, MemoryScope},
                raw_segment::retained_jsonl_records,
            };
            let file = dir.path().join("later.jsonl");
            std::fs::write(
                &file,
                "{\"content\":\"later one\"}\n{\"content\":\"later two\"}\n",
            )
            .unwrap();
            let mut index = HostIndex::open(&host_path).unwrap();
            for (i, raw) in retained_jsonl_records(&file).unwrap().iter().enumerate() {
                let metadata = IndexMetadata {
                    agent_id: "agt".into(),
                    source: MemoryScope::Transcripts,
                    timestamp_ms: Some(100 + i as i64),
                    direction: "in".into(),
                    sender: "s".into(),
                    destination: "d".into(),
                    tool_name: None,
                    tool_names: Vec::new(),
                    role: None,
                };
                index.index(&format!("later{i}"), &metadata, raw).unwrap();
            }
        }
        // Spent earlier today, by a run before this restart.
        let day = Ledger::day_now();
        {
            let ledger = Ledger::open(&config.ledger_path()).unwrap();
            assert_eq!(ledger.charge_on(day, 1.0).unwrap(), 1.0);
        }
        let ledger = Ledger::open(&config.ledger_path()).unwrap();
        assert_eq!(ledger.spent_on(day).unwrap(), 1.0, "persisted");
        assert_eq!(ledger.spent_on(day + 1).unwrap(), 0.0, "a new day");
        let before = seen.lock().unwrap().len();

        let client = start(config);
        let status = wait_for(&client, |s| s.indexing.starts_with("paused"));
        assert!(
            status.indexing.contains("max_daily_usd US$1.00"),
            "{}",
            status.indexing
        );
        assert_eq!(status.pending_documents, 2);
        assert_eq!((status.today_usd, status.max_daily_usd), (1.0, 1.0));
        // The server answers these from BM25 and lists why in degraded.
        let query = client
            .search("deploys", &["agt".to_owned()], None, None, None, 5)
            .unwrap_err();
        assert!(query.contains("max_daily_usd"), "{query}");
        let refused = client
            .rerank("deploys", &["a".into(), "b".into()])
            .unwrap_err();
        assert!(refused.contains("max_daily_usd"), "{refused}");
        assert_eq!(seen.lock().unwrap().len(), before, "no remote request");
        assert_eq!(status_of(&client).today_usd, 1.0);
    }

    /// A remote model with no configured price would count as free, so
    /// neither cap could hold: the service refuses to start with one.
    #[test]
    fn a_remote_model_without_a_price_is_refused_at_start() {
        let dir = crate::test_tempdir();
        openrouter_profile(dir.path());
        let (url, seen) = stub_models(0.0, &[]);
        let unpriced = |config: ProviderConfig| {
            let mut service = Config::new(dir.path());
            service.providers = config;
            let socket = dir.path().join("unpriced.sock");
            let _ = std::fs::remove_file(&socket);
            run(UnixListener::bind(&socket).unwrap(), service)
                .unwrap_err()
                .to_string()
        };
        let error = unpriced(desk_pair(&url, None, 10.0));
        assert!(error.contains("usd_per_million_tokens"), "{error}");
        let mut config = desk_pair(&url, Some(0.01), 10.0);
        if let crate::providers::RerankerConfig::Rerank(reranker) = &mut config.reranker {
            reranker.usd_per_million_tokens = None;
        }
        let error = unpriced(config.clone());
        assert!(error.contains("usd_per_search"), "{error}");
        // Either price makes a reranker countable.
        if let crate::providers::RerankerConfig::Rerank(reranker) = &mut config.reranker {
            reranker.usd_per_search = Some(0.002);
        }
        assert!(config.require_prices().is_ok());
        assert!(desk_pair(&url, Some(0.01), 10.0).require_prices().is_ok());
        assert!(
            ProviderConfig::default().require_prices().is_ok(),
            "local is free"
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    /// Nothing removes a vector space on its own; `prune` does, and only
    /// once the configured space is built.
    #[test]
    fn prune_runs_only_when_asked_and_only_beside_a_built_space() {
        let dir = crate::test_tempdir();
        host_with(dir.path(), 12);
        let (url, _) = stub_models(0.0, &[]);
        let mut config = Config::new(dir.path());
        config.providers = desk_pair(&url, Some(0.01), 10.0);
        let other = dir.path().join("memory-service/dense-0123456789abcdef.db");
        std::fs::create_dir_all(other.parent().unwrap()).unwrap();
        std::fs::write(&other, b"paid vectors").unwrap();
        let refused = prune(&config).unwrap_err();
        assert!(refused.to_string().contains("not built"), "{refused}");
        assert!(other.exists());
        let kept = build_space(&config, &url);
        assert_eq!(prune(&config).unwrap(), vec![other.clone()]);
        assert!(!other.exists() && kept.exists());
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

    #[test]
    fn estimate_configured_supports_postgres_pointer_without_sqlite_file() {
        let dir = crate::test_tempdir();
        let host_path = dir.path().join(HOST_INDEX);
        assert!(!host_index_path_missing_ok(&host_path));

        let (url, _) = stub_models(0.0, &[]);
        let mut config = Config::new(dir.path());
        config.providers = desk_pair(&url, Some(0.01), 10.0);

        // Without SQLite file or PG URL, estimate_configured reports missing host index.
        let missing_err = estimate_configured(&config).unwrap_err().to_string();
        assert!(missing_err.contains("no host index"), "{missing_err}");

        // With `database-url` pointing to PostgreSQL (and NO SQLite file on disk),
        // `estimate_configured` skips the file-existence check and routes to PostgreSQL.
        let pg_url = std::env::var("ANYAGENTS_TEST_POSTGRES_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if let Some(live_pg) = pg_url {
            std::fs::write(dir.path().join("database-url"), format!("{live_pg}\n")).unwrap();
            assert!(!host_path.exists());
            let est = estimate_configured(&config).unwrap();
            assert_eq!(est.embedder, "desk");
            assert!(!host_path.exists());
        } else {
            std::fs::write(
                dir.path().join("database-url"),
                "postgres://user:pass@127.0.0.1:1/nonexistent_db\n",
            )
            .unwrap();
            assert!(host_index_path_missing_ok(&host_path));
            let pg_err = estimate_configured(&config).unwrap_err().to_string();
            assert!(
                !pg_err.contains("no host index"),
                "PG-configured estimate_configured must not bail out on missing SQLite file: {pg_err}"
            );
            assert!(!host_path.exists());
        }
    }

    fn host_index_path_missing_ok(host_path: &Path) -> bool {
        host_index_available(host_path)
    }
}
