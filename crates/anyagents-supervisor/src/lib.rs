//! One process per running agent. The mailbox wakes it; a dead process waits out
//! a capped backoff; a quota answer waits until its reset time.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixDatagram;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyagents_agent::{DeliveryLoop, RunOutcome};
use anyagents_core::{
    Agent, AgentId, BillingMode, NewUsage, Store, Wake, agent_config_dir, agent_work_dir, utc_day,
};
use anyagents_harness::{DeliveryMode, Harness, SteerSupport, TurnUsage};

/// Time source. Production uses the wall clock. Tests advance a manual clock.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
    /// Block until `woken` is set, `deadline` is due, or [`Clock::poke`].
    ///
    /// `None` waits until a poke. A deadline in the past returns at once.
    fn wait(&self, deadline: Option<i64>, woken: &AtomicBool);
    fn poke(&self);
}

/// Wall clock. `wait` uses the operating system's timeout.
pub struct SystemClock {
    lock: Mutex<()>,
    wake: Condvar,
}

impl SystemClock {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            wake: Condvar::new(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        anyagents_core::now_ms()
    }

    fn wait(&self, deadline: Option<i64>, woken: &AtomicBool) {
        let mut guard = lock(&self.lock);
        loop {
            if woken.swap(false, Ordering::SeqCst) {
                return;
            }
            let now = self.now_ms();
            if let Some(deadline) = deadline
                && now >= deadline
            {
                return;
            }
            guard = park_system(&self.wake, guard, deadline, now);
        }
    }

    fn poke(&self) {
        let _guard = lock(&self.lock);
        self.wake.notify_all();
    }
}

fn park_system<'a>(
    wake: &Condvar,
    guard: MutexGuard<'a, ()>,
    deadline: Option<i64>,
    now: i64,
) -> MutexGuard<'a, ()> {
    match deadline {
        Some(deadline) => {
            let remain = deadline.saturating_sub(now).max(0) as u64;
            let (guard, _) = wake
                .wait_timeout(guard, Duration::from_millis(remain))
                .unwrap_or_else(|err| err.into_inner());
            guard
        }
        None => wake.wait(guard).unwrap_or_else(|err| err.into_inner()),
    }
}

struct ManualState {
    now_ms: Mutex<i64>,
    wake: Condvar,
}

/// Frozen clock. [`ManualClock::set_ms`] is the only way time moves.
#[derive(Clone)]
pub struct ManualClock {
    state: Arc<ManualState>,
}

impl ManualClock {
    pub fn new(now_ms: i64) -> Self {
        Self {
            state: Arc::new(ManualState {
                now_ms: Mutex::new(now_ms),
                wake: Condvar::new(),
            }),
        }
    }

    pub fn set_ms(&self, now_ms: i64) {
        *lock(&self.state.now_ms) = now_ms;
        self.state.wake.notify_all();
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        *lock(&self.state.now_ms)
    }

    fn wait(&self, deadline: Option<i64>, woken: &AtomicBool) {
        let mut guard = lock(&self.state.now_ms);
        loop {
            if woken.swap(false, Ordering::SeqCst) {
                return;
            }
            if let Some(deadline) = deadline
                && *guard >= deadline
            {
                return;
            }
            guard = self
                .state
                .wake
                .wait(guard)
                .unwrap_or_else(|err| err.into_inner());
        }
    }

    fn poke(&self) {
        let _guard = lock(&self.state.now_ms);
        self.state.wake.notify_all();
    }
}

/// Delay before attempt `n` (1-based). About 250 ms, doubling, capped at 60 s,
/// then spread by about ±20% from `salt`.
pub fn restart_delay_ms(attempt: u32, salt: u64) -> i64 {
    let shift = attempt.saturating_sub(1).min(16);
    let base = 250i64.saturating_mul(1_i64 << shift).min(60_000);
    let percent = (salt % 40) as i64 - 20;
    let scaled = base.saturating_mul(100 + percent) / 100;
    scaled.clamp(1, 60_000)
}

fn salt_for(agent_id: &AgentId, attempt: u32) -> u64 {
    fnv1a(agent_id.as_str()).wrapping_add(u64::from(attempt))
}

fn fnv1a(text: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// What the vendor factory needs. The config directory is the harness home.
#[derive(Clone, Debug)]
pub struct LaunchRequest {
    pub agent: Agent,
    pub config_dir: PathBuf,
    pub work_dir: PathBuf,
    pub extra_env: Vec<(String, String)>,
}

pub type Launch<H> = Arc<dyn Fn(LaunchRequest) -> Result<H, String> + Send + Sync>;

/// How long a turn must stand still before a harness that never closes it is
/// counted as done, and how long to watch for that.
const ANSWER_QUIET: Duration = Duration::from_millis(500);
const ANSWER_PATIENCE: Duration = Duration::from_millis(120_000);

/// Supervisor view of one agent. Ages stay on the mailbox; this is the process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveState {
    pub process_alive: bool,
    pub delivery_mode: DeliveryMode,
    pub steer_support: SteerSupport,
    pub quota_reset_at_ms: Option<i64>,
    pub restart_at_ms: Option<i64>,
    pub pid: Option<u32>,
}

/// Latest [`LiveState`] for each agent the supervisor has touched.
#[derive(Clone, Default)]
pub struct HealthBoard {
    inner: Arc<Mutex<HashMap<AgentId, LiveState>>>,
}

impl HealthBoard {
    pub fn publish(&self, id: &AgentId, state: LiveState) {
        lock(&self.inner).insert(id.clone(), state);
    }

    pub fn get(&self, id: &AgentId) -> Option<LiveState> {
        lock(&self.inner).get(id).cloned()
    }
}

/// Catalog delivery when no process is up. Codex steers; other harnesses follow up.
pub fn declared(harness: &str) -> (DeliveryMode, SteerSupport) {
    if harness == "codex" {
        (DeliveryMode::Steer, SteerSupport::Yes)
    } else {
        (DeliveryMode::Followup, SteerSupport::Unproven)
    }
}

pub fn mode_name(mode: DeliveryMode) -> &'static str {
    match mode {
        DeliveryMode::Steer => "steer",
        DeliveryMode::Followup => "followup",
        DeliveryMode::Queue => "queue",
    }
}

pub fn steer_name(steer: SteerSupport) -> &'static str {
    match steer {
        SteerSupport::Yes => "yes",
        SteerSupport::No => "no",
        SteerSupport::Unproven => "unproven",
    }
}

struct Slot {
    stop: Arc<AtomicBool>,
    woken: Arc<AtomicBool>,
    busy: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

struct Inner<H> {
    store: Arc<Store>,
    clock: Arc<dyn Clock>,
    launch: Launch<H>,
    extra_env: Vec<(String, String)>,
    board: HealthBoard,
    stop: AtomicBool,
    workers: Mutex<HashMap<AgentId, Slot>>,
}

/// Keeps one harness process alive for every agent in state `running`.
pub struct Supervisor<H: Harness + 'static> {
    inner: Arc<Inner<H>>,
    wake_path: Option<PathBuf>,
    listener: Option<JoinHandle<()>>,
    router: Option<JoinHandle<()>>,
}

impl<H: Harness + 'static> Supervisor<H> {
    pub fn start(
        store: Arc<Store>,
        clock: Arc<dyn Clock>,
        launch: Launch<H>,
        extra_env: Vec<(String, String)>,
    ) -> Result<Self, String> {
        let inner = Arc::new(Inner {
            store: Arc::clone(&store),
            clock,
            launch,
            extra_env,
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            workers: Mutex::new(HashMap::new()),
        });
        let (wake_path, listener) = bind_wake(&inner)?;
        let events = store.subscribe();
        let router_inner = Arc::clone(&inner);
        let router = thread::Builder::new()
            .name("anyagents-supervisor".into())
            .spawn(move || route(router_inner, events))
            .map_err(|err| err.to_string())?;
        Ok(Self {
            inner,
            wake_path,
            listener,
            router: Some(router),
        })
    }

    pub fn live(&self, id: &AgentId) -> Option<LiveState> {
        self.inner.board.get(id)
    }

    pub fn board(&self) -> HealthBoard {
        self.inner.board.clone()
    }
}

impl<H: Harness + 'static> Drop for Supervisor<H> {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::SeqCst);
        let threads = {
            let mut workers = lock(&self.inner.workers);
            for slot in workers.values() {
                slot.stop.store(true, Ordering::SeqCst);
                slot.woken.store(true, Ordering::SeqCst);
            }
            workers
                .values_mut()
                .filter_map(|slot| slot.thread.take())
                .collect::<Vec<_>>()
        };
        self.inner.clock.poke();
        self.inner.store.close_wakes();
        if let Some(path) = &self.wake_path {
            let _ = UnixDatagram::unbound().and_then(|sock| sock.send_to(b"x", path));
        }
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
        if let Some(router) = self.router.take() {
            let _ = router.join();
        }
        for thread in threads {
            let _ = thread.join();
        }
    }
}

fn bind_wake<H>(inner: &Arc<Inner<H>>) -> Result<(Option<PathBuf>, Option<JoinHandle<()>>), String>
where
    H: Harness + 'static,
{
    let path = inner.store.data_dir().join("wake.sock");
    if path.as_os_str().len() > 107 {
        return Ok((None, None));
    }
    match fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_socket() => {
            fs::remove_file(&path).map_err(|err| err.to_string())?;
        }
        Ok(_) => {
            return Err(format!(
                "wake socket path exists and is not a socket: {}",
                path.display()
            ));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.to_string()),
    }
    let socket = UnixDatagram::bind(&path).map_err(|err| err.to_string())?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).map_err(|err| err.to_string())?;
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|err| err.to_string())?;
    let listen_inner = Arc::clone(inner);
    let listener = thread::Builder::new()
        .name("anyagents-wake".into())
        .spawn(move || listen_wake(socket, listen_inner))
        .map_err(|err| err.to_string())?;
    Ok((Some(path), Some(listener)))
}

fn listen_wake<H: Harness>(socket: UnixDatagram, inner: Arc<Inner<H>>) {
    let mut buf = [0u8; 256];
    while !inner.stop.load(Ordering::SeqCst) {
        let count = match socket.recv(&mut buf) {
            Ok(count) => count,
            Err(err)
                if err.kind() == std::io::ErrorKind::TimedOut
                    || err.kind() == std::io::ErrorKind::WouldBlock =>
            {
                continue;
            }
            Err(_) => break,
        };
        if count == 0 {
            continue;
        }
        match buf[0] {
            b'x' => break,
            b'm' | b'a' => {
                let Ok(text) = std::str::from_utf8(&buf[1..count]) else {
                    continue;
                };
                let Ok(agent_id) = AgentId::parse(text) else {
                    continue;
                };
                let wake = if buf[0] == b'm' {
                    Wake::Mail { agent_id }
                } else {
                    Wake::Agent { agent_id }
                };
                inner.store.poke(wake);
            }
            _ => {}
        }
    }
}

fn route<H: Harness + 'static>(inner: Arc<Inner<H>>, events: Receiver<Wake>) {
    if let Ok(agents) = inner.store.list_agents() {
        for agent in agents {
            if agent.state == "running" {
                ensure(&inner, agent.id);
            }
        }
    }
    while let Ok(wake) = events.recv() {
        if inner.stop.load(Ordering::SeqCst) {
            break;
        }
        match wake {
            Wake::Mail { agent_id } => {
                if let Some(slot) = lock(&inner.workers).get(&agent_id) {
                    slot.woken.store(true, Ordering::SeqCst);
                    inner.clock.poke();
                }
                if is_running(&inner.store, &agent_id) {
                    ensure(&inner, agent_id);
                }
            }
            Wake::Agent { agent_id } => {
                if is_running(&inner.store, &agent_id) {
                    ensure(&inner, agent_id);
                }
            }
        }
    }
}

fn is_running(store: &Store, id: &AgentId) -> bool {
    store.agent(id).is_ok_and(|agent| agent.state == "running")
}

fn ensure<H: Harness + 'static>(inner: &Arc<Inner<H>>, id: AgentId) {
    let mut workers = lock(&inner.workers);
    if let Some(slot) = workers.get_mut(&id) {
        let finished = slot.done.load(Ordering::SeqCst);
        let busy = slot.busy.load(Ordering::SeqCst);
        if !finished || busy {
            return;
        }
        if let Some(thread) = slot.thread.take() {
            let _ = thread.join();
        }
        workers.remove(&id);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let woken = Arc::new(AtomicBool::new(false));
    let busy = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let run_inner = Arc::clone(inner);
    let run_id = id.clone();
    let run_stop = Arc::clone(&stop);
    let run_woken = Arc::clone(&woken);
    let run_busy = Arc::clone(&busy);
    let run_done = Arc::clone(&done);
    let thread = thread::Builder::new()
        .name("anyagents-agent".into())
        .spawn(move || {
            if run_agent(run_inner, run_id, run_stop, run_woken) {
                run_busy.store(true, Ordering::SeqCst);
            }
            run_done.store(true, Ordering::SeqCst);
        });
    let Ok(thread) = thread else {
        return;
    };
    workers.insert(
        id,
        Slot {
            stop,
            woken,
            busy,
            done,
            thread: Some(thread),
        },
    );
}

enum Boot<H: Harness> {
    Ready(Box<DeliveryLoop<H>>),
    Busy,
    Retry,
}

fn run_agent<H: Harness + 'static>(
    inner: Arc<Inner<H>>,
    id: AgentId,
    stop: Arc<AtomicBool>,
    woken: Arc<AtomicBool>,
) -> bool {
    let Ok(agent) = inner.store.agent(&id) else {
        return false;
    };
    let paths = AgentPaths::from_store(&inner.store, &agent);
    let notify = activity_notify(Arc::clone(&inner.clock), Arc::clone(&woken));
    let mut session = Session {
        inner,
        id,
        agent,
        paths,
        notify,
        stop,
        woken,
        attempt: 0,
        restart_at: None,
        delivery: None,
        recorded: HashSet::new(),
        settled: 0,
    };
    session.run()
}

struct AgentPaths {
    config_dir: PathBuf,
    managed_work: PathBuf,
    work_dir: PathBuf,
}

impl AgentPaths {
    fn from_store(store: &Store, agent: &Agent) -> Self {
        let data = store.data_dir();
        let managed_work = agent_work_dir(data, &agent.id);
        let work_dir = agent
            .workdir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| managed_work.clone());
        Self {
            config_dir: agent_config_dir(data, &agent.id),
            managed_work,
            work_dir,
        }
    }
}

fn activity_notify(clock: Arc<dyn Clock>, woken: Arc<AtomicBool>) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
        woken.store(true, Ordering::SeqCst);
        clock.poke();
    })
}

struct Session<H: Harness> {
    inner: Arc<Inner<H>>,
    id: AgentId,
    agent: Agent,
    paths: AgentPaths,
    notify: Arc<dyn Fn() + Send + Sync>,
    stop: Arc<AtomicBool>,
    woken: Arc<AtomicBool>,
    attempt: u32,
    restart_at: Option<i64>,
    delivery: Option<DeliveryLoop<H>>,
    recorded: HashSet<String>,
    settled: i64,
}

impl<H: Harness + 'static> Session<H> {
    fn run(&mut self) -> bool {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return false;
            }
            if self.delivery.is_none() {
                match self.boot() {
                    Boot::Ready(delivery) => {
                        delivery.harness().watch_activity(Arc::clone(&self.notify));
                        self.restart_at = None;
                        self.delivery = Some(*delivery);
                    }
                    Boot::Busy => return true,
                    Boot::Retry => {
                        if !self.wait_backoff() {
                            return false;
                        }
                    }
                }
                continue;
            }
            self.publish();
            if !self.process_alive() {
                if !self.wait_backoff() {
                    return false;
                }
                self.replace_process();
                continue;
            }
            self.settle();
            self.settle_quiet();
            if self.quota_waiting() {
                continue;
            }
            if self.deliver_once() {
                return true;
            }
        }
    }

    fn boot(&self) -> Boot<H> {
        let harness = match self.launch_harness() {
            Ok(harness) => harness,
            Err(_) => return Boot::Retry,
        };
        let store = match Store::open(self.inner.store.data_dir().join("state.db")) {
            Ok(store) => store,
            Err(_) => return Boot::Retry,
        };
        match DeliveryLoop::start(store, self.id.clone(), harness) {
            Ok(delivery) => Boot::Ready(Box::new(delivery)),
            Err(err) if err.agent_busy() => Boot::Busy,
            Err(_) => Boot::Retry,
        }
    }

    fn launch_harness(&self) -> Result<H, String> {
        let _ = fs::create_dir_all(&self.paths.config_dir);
        let _ = fs::create_dir_all(&self.paths.managed_work);
        let _ = fs::create_dir_all(&self.paths.work_dir);
        (self.inner.launch)(LaunchRequest {
            agent: self.agent.clone(),
            config_dir: self.paths.config_dir.clone(),
            work_dir: self.paths.work_dir.clone(),
            extra_env: self.inner.extra_env.clone(),
        })
    }

    fn process_alive(&self) -> bool {
        self.delivery
            .as_ref()
            .is_some_and(|delivery| delivery.harness().health().process_alive)
    }

    fn publish(&self) {
        let state = match self.delivery.as_ref() {
            Some(delivery) => {
                let health = delivery.harness().health();
                LiveState {
                    process_alive: health.process_alive,
                    delivery_mode: health.delivery_mode,
                    steer_support: health.steer_support,
                    quota_reset_at_ms: health.quota_reset_at_ms,
                    restart_at_ms: if health.process_alive {
                        None
                    } else {
                        self.restart_at
                    },
                    pid: delivery.harness().process_id(),
                }
            }
            None => {
                let (mode, steer) = declared(&self.agent.harness);
                LiveState {
                    process_alive: false,
                    delivery_mode: mode,
                    steer_support: steer,
                    quota_reset_at_ms: None,
                    restart_at_ms: self.restart_at,
                    pid: None,
                }
            }
        };
        self.inner.board.publish(&self.id, state);
    }

    fn wait_backoff(&mut self) -> bool {
        if self.restart_at.is_none() {
            self.attempt = self.attempt.saturating_add(1);
            let delay = restart_delay_ms(self.attempt, salt_for(&self.id, self.attempt));
            self.restart_at = Some(self.inner.clock.now_ms().saturating_add(delay));
        }
        self.publish();
        let Some(deadline) = self.restart_at else {
            return !self.stop.load(Ordering::SeqCst);
        };
        while self.inner.clock.now_ms() < deadline {
            if self.stop.load(Ordering::SeqCst) {
                return false;
            }
            self.inner.clock.wait(Some(deadline), &self.woken);
        }
        self.restart_at = None;
        !self.stop.load(Ordering::SeqCst)
    }

    fn replace_process(&mut self) {
        if self.delivery.is_none() {
            self.restart_at = None;
            return;
        }
        match self.launch_harness() {
            Ok(harness) => {
                harness.watch_activity(Arc::clone(&self.notify));
                if let Some(delivery) = self.delivery.as_mut() {
                    delivery.replace_harness(harness);
                }
                self.restart_at = None;
            }
            Err(_) => self.restart_at = None,
        }
    }

    fn quota_waiting(&mut self) -> bool {
        let reset_at = self
            .delivery
            .as_ref()
            .and_then(|delivery| delivery.harness().health().quota_reset_at_ms);
        let Some(reset_at) = reset_at else {
            return false;
        };
        if self.inner.clock.now_ms() >= reset_at {
            if let Some(delivery) = self.delivery.as_mut() {
                delivery.clear_quota();
            }
            return false;
        }
        self.publish();
        self.inner.clock.wait(Some(reset_at), &self.woken);
        true
    }

    /// `true` when a second loop already holds the agent.
    fn deliver_once(&mut self) -> bool {
        let outcome = {
            let Some(delivery) = self.delivery.as_mut() else {
                return false;
            };
            delivery.run_once()
        };
        match outcome {
            Ok(RunOutcome::Quota { .. }) => false,
            Ok(RunOutcome::Idle | RunOutcome::Delivered { .. } | RunOutcome::Deferred { .. }) => {
                self.settle();
                if self.quota_is_ahead() {
                    return false;
                }
                self.inner.clock.wait(None, &self.woken);
                false
            }
            Err(err) if err.agent_busy() => true,
            Err(_) => {
                if self.process_alive() && !self.wait_backoff() {
                    return false;
                }
                if self.process_alive() {
                    self.replace_process();
                }
                false
            }
        }
    }

    fn quota_is_ahead(&self) -> bool {
        let Some(delivery) = self.delivery.as_ref() else {
            return false;
        };
        match delivery.harness().health().quota_reset_at_ms {
            Some(reset_at) => self.inner.clock.now_ms() < reset_at,
            None => false,
        }
    }

    fn settle(&mut self) {
        let pending = self.finished_turns();
        let agent = self.agent.clone();
        let clock = Arc::clone(&self.inner.clock);
        for (seq, id, usage) in pending {
            if !self.recorded.contains(&id) {
                let recorded = self
                    .delivery
                    .as_ref()
                    .is_some_and(|delivery| record_turn(delivery, &agent, &clock, &usage));
                if !recorded {
                    break;
                }
                self.recorded.insert(id);
            }
            if !self.mark_processed(seq) {
                break;
            }
            self.settled = seq;
        }
    }

    /// A harness that never closes its turn records a finished message with no
    /// token counts once its turn stands still. The answer is already on the
    /// harness, so the message counts as done.
    fn settle_quiet(&mut self) {
        let Some(delivery) = self.delivery.as_ref() else {
            return;
        };
        if delivery.harness().ends_on_quiet().is_none() {
            return;
        }
        let Ok(accepted) = delivery.store().accepted_cursor(&self.id) else {
            return;
        };
        let Ok(processed) = delivery.store().processed_cursor(&self.id) else {
            return;
        };
        if accepted <= processed {
            return;
        }
        let Ok(messages) = delivery.store().inbox(&self.id, Some(processed)) else {
            return;
        };
        // An answer is not asked of the harness again, so it never holds the turn.
        let Some(open) = messages
            .into_iter()
            .find(|message| message.seq > processed && message.reply_to.is_none())
        else {
            return;
        };
        if !delivery.harness().quiet_for(
            open.id.as_str(),
            ANSWER_QUIET,
            ANSWER_PATIENCE,
        ) {
            return;
        }
        // The answer is the reply; then the message itself is done.
        let agent = self.agent.clone();
        if let Some(delivery) = self.delivery.as_ref() {
            let _ = delivery.record_answer(&agent);
        }
        // A second loop in the same store may already have settled it.
        let Ok(processed) = delivery.store().processed_cursor(&self.id) else {
            return;
        };
        if open.seq > processed {
            self.settled = open.seq;
            self.mark_processed(open.seq);
        }
    }

    fn finished_turns(&self) -> Vec<(i64, String, TurnUsage)> {
        let Some(delivery) = self.delivery.as_ref() else {
            return Vec::new();
        };
        let Ok(accepted) = delivery.store().accepted_cursor(&self.id) else {
            return Vec::new();
        };
        let Ok(messages) = delivery.store().inbox(&self.id, Some(self.settled)) else {
            return Vec::new();
        };
        let mut pending = Vec::new();
        for message in messages {
            if message.seq > accepted {
                break;
            }
            let id = message.id.as_str().to_string();
            let Some(usage) = delivery.harness().usage_for(&id) else {
                break;
            };
            pending.push((message.seq, id, usage));
        }
        pending
    }

    fn mark_processed(&self, seq: i64) -> bool {
        let Some(delivery) = self.delivery.as_ref() else {
            return false;
        };
        let Ok(processed) = delivery.store().processed_cursor(&self.id) else {
            return false;
        };
        if seq <= processed {
            return true;
        }
        delivery
            .store()
            .advance_processed_through(&self.id, seq)
            .is_ok()
    }
}

fn record_turn<H: Harness>(
    delivery: &DeliveryLoop<H>,
    agent: &Agent,
    clock: &Arc<dyn Clock>,
    usage: &TurnUsage,
) -> bool {
    delivery
        .store()
        .record_usage(NewUsage {
            agent_id: agent.id.clone(),
            harness: agent.harness.clone(),
            model: agent.model.clone(),
            input_tokens: tokens(usage.input_tokens),
            output_tokens: tokens(usage.output_tokens),
            cache_read_tokens: tokens(usage.cache_read_tokens),
            cache_write_tokens: tokens(usage.cache_write_tokens),
            reported_cost_micros: usage.reported_cost_micros,
            estimated_cost_micros: None,
            billing_mode: BillingMode::Subscription,
            quota_state: None,
            day: utc_day(clock.now_ms()),
        })
        .is_ok()
}

fn tokens(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_delay_stays_in_range() {
        assert_eq!(restart_delay_ms(1, 0), 200);
        assert_eq!(restart_delay_ms(1, 39), 297);
        for salt in 0..100 {
            let delay = restart_delay_ms(1, salt);
            assert!((200..=299).contains(&delay), "{salt} -> {delay}");
        }
        assert!(restart_delay_ms(30, 0) <= 60_000);
        assert!(restart_delay_ms(30, 39) <= 60_000);
        assert!(restart_delay_ms(30, 39) >= 1);
    }
}
