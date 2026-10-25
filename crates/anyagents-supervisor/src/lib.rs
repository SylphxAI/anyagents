//! One process per running agent. The mailbox wakes it; a dead process waits out
//! a capped backoff; a quota answer waits until its reset time.

use std::collections::HashMap;
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
use anyagents_harness::{DeliverOutcome, DeliveryMode, Harness, SteerSupport, TurnState, TurnUsage};

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
    /// The AnyAgents data directory: where installed harnesses live.
    pub data_dir: std::path::PathBuf,
    pub config_dir: PathBuf,
    pub work_dir: PathBuf,
    pub extra_env: Vec<(String, String)>,
}

pub type Launch<H> = Arc<dyn Fn(LaunchRequest) -> Result<H, String> + Send + Sync>;

/// How often a worker with an empty mailbox looks again. One store read, so
/// `flush` and a restarted process never leave a line waiting for an arrival.
const IDLE_REVIEW_MS: i64 = 250;

/// Supervisor view of one agent. Ages stay on the mailbox; this is the process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveState {
    pub process_alive: bool,
    pub turn_state: TurnState,
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

/// Catalog delivery when no process is up. It matches what each adapter reports
/// while it runs.
///
/// Steer: Codex takes its `turn/start` while a turn is open and steers into it;
/// Claude Code writes a new user message into the running stream-json child
/// (acknowledged by its replay echo); OpenCode takes a prompt with `delivery: "steer"`,
/// and its own session inbox decides whether that joins the running turn or
/// starts the next one. Muse's app-server takes `ifBusy: "steer"` on
/// `turn/start`.
///
/// Followup: the contract double (`ANYAGENTS_HARNESS_BIN`) reads one line per
/// turn and answers with a `result`, so a line written mid-turn waits for that
/// turn to end. AGY's stream input also starts a turn per line; its hook helpers
/// have no wired, confirmed consumption acknowledgement.
pub fn declared(harness: &str) -> (DeliveryMode, SteerSupport) {
    match harness {
        "codex" | "claude-code" | "opencode" | "muse" | "agy" | "omp" | "dsh" | "grok" => {
            (DeliveryMode::Steer, SteerSupport::Yes)
        }
        _ => (DeliveryMode::Followup, SteerSupport::Unproven),
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
                } else {
                    stop_slot(&inner, &agent_id);
                }
            }
        }
    }
}

fn is_running(store: &Store, id: &AgentId) -> bool {
    store.agent(id).is_ok_and(|agent| agent.state == "running")
}

fn stop_slot<H: Harness + 'static>(inner: &Arc<Inner<H>>, id: &AgentId) {
    let done = {
        let mut workers = lock(&inner.workers);
        if let Some(slot) = workers.get_mut(id) {
            slot.stop.store(true, Ordering::SeqCst);
            slot.woken.store(true, Ordering::SeqCst);
            Some(Arc::clone(&slot.done))
        } else {
            None
        }
    };
    inner.clock.poke();
    if let Ok(agent) = inner.store.agent(id) {
        let (mode, steer) = declared(&agent.harness);
        inner.board.publish(
            id,
            LiveState {
                process_alive: false,
                turn_state: TurnState::Idle,
                delivery_mode: mode,
                steer_support: steer,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
            },
        );
    }
    if let Some(done) = done {
        let deadline = std::time::Instant::now() + Duration::from_millis(250);
        while !done.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        let finished_thread = {
            let mut workers = lock(&inner.workers);
            if let Some(slot) = workers.get_mut(id)
                && slot.done.load(Ordering::SeqCst)
            {
                let thread = slot.thread.take();
                workers.remove(id);
                thread
            } else {
                None
            }
        };
        if let Some(thread) = finished_thread {
            let _ = thread.join();
        }
    }
}

fn ensure<H: Harness + 'static>(inner: &Arc<Inner<H>>, id: AgentId) {
    let stopping_thread = {
        let mut workers = lock(&inner.workers);
        if let Some(slot) = workers.get_mut(&id) {
            let finished = slot.done.load(Ordering::SeqCst);
            let stopping = slot.stop.load(Ordering::SeqCst);
            let busy = slot.busy.load(Ordering::SeqCst);
            if (!finished && !stopping) || busy {
                return;
            }
            let thread = slot.thread.take();
            workers.remove(&id);
            thread
        } else {
            None
        }
    };
    if let Some(thread) = stopping_thread {
        let _ = thread.join();
    }
    let mut workers = lock(&inner.workers);
    if workers.contains_key(&id) {
        return;
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
        served: None,
        written: 0,
        turns_recorded: 0,
        pending_recovery: false,
        completed_turn_in_session: false,
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
    /// What the harness had served when the last row was recorded. The next
    /// row carries the difference, so a burst is charged once and an idle loop
    /// adds nothing.
    served: Option<TurnUsage>,
    /// Lines this loop wrote since the last usage row.
    written: u32,
    /// Turns already recorded from the current harness process.
    turns_recorded: u32,
    /// Set when a fresh harness process starts (`boot` or `replace_process`)
    /// so an open turn interrupted by the previous process's exit is resumed
    /// with a `[Recovery]` prompt if no new mailbox message wakes it first.
    pending_recovery: bool,
    /// Set once at least one turn has completed during this session, enabling
    /// automatic scale-to-zero (`archived`) for sub-agents when idle.
    completed_turn_in_session: bool,
}

impl<H: Harness + 'static> Session<H> {
    fn run(&mut self) -> bool {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                self.delivery = None;
                self.publish();
                return false;
            }
            if self.delivery.is_none() {
                match self.boot() {
                    Boot::Ready(delivery) => {
                        delivery.harness().watch_activity(Arc::clone(&self.notify));
                        self.restart_at = None;
                        self.pending_recovery = delivery
                            .store()
                            .open_turn(&self.id)
                            .ok()
                            .flatten()
                            .is_some();
                        self.delivery = Some(*delivery);
                    }
                    Boot::Busy => {
                        self.delivery = None;
                        self.restart_at = None;
                        self.publish();
                        return true;
                    }
                    Boot::Retry => {
                        if !self.wait_backoff() {
                            self.delivery = None;
                            self.restart_at = None;
                            self.publish();
                            return false;
                        }
                    }
                }
                continue;
            }
            self.publish();
            if self.quota_waiting() {
                continue;
            }
            if !self.process_alive() {
                if !self.wait_backoff() {
                    self.delivery = None;
                    self.restart_at = None;
                    self.publish();
                    return false;
                }
                self.replace_process();
                continue;
            }
            if self.deliver_once() {
                self.delivery = None;
                self.restart_at = None;
                self.publish();
                return true;
            }
        }
    }

    fn boot(&self) -> Boot<H> {
        let store = match Store::open(self.inner.store.data_dir().join("state.db")) {
            Ok(store) => store,
            Err(_) => return Boot::Retry,
        };
        let harness = match self.launch_harness() {
            Ok(harness) => harness,
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
            data_dir: self.inner.store.data_dir().to_path_buf(),
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
        let state = match self
            .delivery
            .as_ref()
            .filter(|_| !self.stop.load(Ordering::SeqCst))
        {
            Some(delivery) => {
                let health = delivery.harness().health();
                LiveState {
                    process_alive: health.process_alive,
                    turn_state: if health.process_alive {
                        health.turn_state
                    } else {
                        TurnState::Idle
                    },
                    delivery_mode: health.delivery_mode,
                    steer_support: health.steer_support,
                    quota_reset_at_ms: health.quota_reset_at_ms,
                    restart_at_ms: if health.process_alive {
                        None
                    } else {
                        self.restart_at
                    },
                    pid: if health.process_alive {
                        delivery.harness().process_id()
                    } else {
                        None
                    },
                }
            }
            None => {
                let (mode, steer) = declared(&self.agent.harness);
                LiveState {
                    process_alive: false,
                    turn_state: TurnState::Idle,
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
        // Flush any usage the exited process finished serving before swapping it.
        self.record_usage();
        match self.launch_harness() {
            Ok(mut harness) => {
                harness.watch_activity(Arc::clone(&self.notify));
                // A quota wait belonged to the process that is gone.
                harness.clear_quota();
                if let Some(delivery) = self.delivery.as_mut() {
                    self.pending_recovery = delivery
                        .store()
                        .open_turn(&self.id)
                        .ok()
                        .flatten()
                        .is_some();
                    delivery.replace_harness(harness);
                }
                self.served = None;
                self.turns_recorded = 0;
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

    /// One pass over the mailbox. Every line is handed to the harness's own
    /// input or steer API, in sequence order; the cursor moves behind each one
    /// it takes. Nothing here waits for a turn to end.
    ///
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
            Ok(RunOutcome::Delivered { delivered }) => {
                // New mailbox messages just woke the session directly; any open
                // turn from before a process restart is now superseded by them.
                self.pending_recovery = false;
                self.written = self
                    .written
                    .saturating_add(delivered.clamp(0, u64::from(u32::MAX)) as u32);
                self.wake_unless_more();
                false
            }
            Ok(RunOutcome::Idle) => {
                // The mailbox has drained, so whatever the vendor reported for
                // this burst is read once and recorded.
                self.record_usage();
                if self.pending_recovery && self.recover_once() {
                    return false;
                }
                if self.try_auto_archive_subagent() {
                    return false;
                }
                self.wake_unless_more();
                false
            }
            Err(anyagents_agent::Error::HarnessDown) => {
                if !self.wait_backoff() {
                    return false;
                }
                self.replace_process();
                false
            }
            Err(anyagents_agent::Error::HarnessFailed(_)) => {
                if !self.wait_backoff() {
                    return false;
                }
                self.replace_process();
                false
            }
            Err(err) if err.agent_busy() => true,
            Err(_) => {
                self.wake_unless_more();
                false
            }
        }
    }

    /// Record the usage the harness served since the last row. It is read when
    /// the mailbox has drained, never holds a line back, and adds nothing while
    /// the harness has served nothing new.
    fn record_usage(&mut self) {
        let Some(delivery) = self.delivery.as_ref() else {
            return;
        };
        let served = delivery.harness().last_usage();
        let before = self.served.clone().unwrap_or_default();
        let turns_served = delivery.harness().turns_served();

        if served == before {
            if turns_served > self.turns_recorded
                && delivery.harness().health().turn_state == TurnState::Idle
            {
                self.turns_recorded = turns_served;
                let _ = delivery.store().note_turn_completed(&self.id);
                self.completed_turn_in_session = true;
            }
            return;
        }
        // One row for each unit the vendor charged: its turns, or as many as
        // the lines that were written here.
        let turns = turns_served
            .saturating_sub(self.turns_recorded)
            .max(self.written.max(1));
        let unit = owned_share(served.difference(before), turns);
        let mut recorded = true;
        for _ in 0..turns {
            if unit.input_tokens == 0 && unit.output_tokens == 0 {
                break;
            }
            recorded &= record_turn(delivery, &self.agent, &self.inner.clock, &unit);
        }
        if recorded {
            self.served = Some(served);
            self.written = 0;
            self.turns_recorded = turns_served;
            if delivery.harness().health().turn_state == TurnState::Idle {
                let _ = delivery.store().note_turn_completed(&self.id);
                self.completed_turn_in_session = true;
            }
        }
    }

    /// Automatically scale a sub-agent (`parent_id.is_some()`) to zero (`state = "archived"`)
    /// once it has finished its turn and its mailbox is drained. Sending any future
    /// message to the sub-agent automatically wakes and resumes it.
    fn try_auto_archive_subagent(&mut self) -> bool {
        if self.agent.parent_id.is_none() || !self.completed_turn_in_session {
            return false;
        }
        let archived = {
            let Some(delivery) = self.delivery.as_ref() else {
                return false;
            };
            if delivery.harness().health().turn_state != TurnState::Idle {
                return false;
            }
            delivery
                .store()
                .complete_idle_subagent(&self.id)
                .unwrap_or(false)
        };
        if !archived {
            return false;
        }
        self.stop.store(true, Ordering::SeqCst);
        self.delivery = None;
        self.restart_at = None;
        reap_ephemeral_dirs(&self.paths.config_dir, &self.paths.managed_work);
        self.publish();
        true
    }

    /// Deliver one synthetic `[Recovery]` wake when a newly started harness
    /// process has an unfinished turn (`opened_seq > completed_seq`) and an
    /// empty mailbox. Returns `true` when the caller should re-enter the main
    /// loop immediately (e.g. after process replacement on failure).
    fn recover_once(&mut self) -> bool {
        let outcome = {
            let Some(delivery) = self.delivery.as_mut() else {
                self.pending_recovery = false;
                return false;
            };
            let open = match delivery.store().open_turn(&self.id) {
                Ok(Some(open)) => open,
                Ok(None) | Err(_) => {
                    self.pending_recovery = false;
                    return false;
                }
            };
            delivery.recover_open_turn(&open)
        };
        match outcome {
            Ok(DeliverOutcome::Accepted { .. }) => {
                self.pending_recovery = false;
                self.written = self.written.saturating_add(1);
                false
            }
            Ok(DeliverOutcome::Deferred) | Ok(DeliverOutcome::Quota { .. }) => false,
            Err(anyagents_agent::Error::HarnessDown)
            | Err(anyagents_agent::Error::HarnessFailed(_)) => {
                if !self.wait_backoff() {
                    return true;
                }
                self.replace_process();
                true
            }
            Err(_) => {
                self.pending_recovery = false;
                false
            }
        }
    }

    /// Sleep until a new line arrives, unless one arrived while this pass ran:
    /// then the next pass runs at once, so two lines in flight are both written.
    fn wake_unless_more(&mut self) {
        let more = self.delivery.as_ref().is_some_and(|delivery| {
            let has_work = self.pending_recovery
                || delivery
                    .store()
                    .next_undelivered(&self.id)
                    .is_ok_and(|next| next.is_some());
            has_work && delivery.harness().input_ready()
        });
        if more {
            return;
        }
        // With no line to hand over, a short timer is the backstop: a line read
        // by `flush` while this worker was restarting is picked up from the next
        // process without waiting for another arrival.
        self.inner.clock.wait(
            Some(self.inner.clock.now_ms() + IDLE_REVIEW_MS),
            &self.woken,
        );
    }
}

fn reap_ephemeral_dirs(config_dir: &std::path::Path, managed_work: &std::path::Path) {
    let seat_ids = detect_seat_ids(config_dir);
    for rel in [
        "cache-home",
        "state-home",
        "tmp",
        ".rustup",
        ".cargo",
        "data-home/opencode",
        "opencode-session-id",
        "claude-session-id",
        ".claude/projects",
        ".codex/sessions",
    ] {
        let target = config_dir.join(rel);
        remove_path_with_seat(&target, seat_ids);
    }
    remove_path_with_seat(managed_work, seat_ids);
}

#[cfg(unix)]
fn detect_seat_ids(config_dir: &std::path::Path) -> Option<(u32, u32, u32)> {
    use std::os::unix::fs::MetadataExt as _;
    let (me, my_gid) = fs::metadata("/proc/self")
        .map(|m| (m.uid(), m.gid()))
        .unwrap_or((0, 0));
    let cfg_meta = fs::metadata(config_dir).ok()?;
    let gid = cfg_meta.gid();
    let sock_gid = config_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| fs::metadata(p).ok())
        .map(|m| m.gid())
        .unwrap_or(gid);
    if cfg_meta.uid() != me && cfg_meta.uid() != 0 {
        return Some((cfg_meta.uid(), gid, sock_gid));
    }
    if let Ok(entries) = fs::read_dir(config_dir) {
        for entry in entries.flatten() {
            if let Ok(m) = entry.metadata() {
                if m.uid() != me && m.uid() != 0 {
                    return Some((m.uid(), gid, sock_gid));
                }
                if m.is_dir()
                    && let Ok(sub) = fs::read_dir(entry.path())
                {
                    for child in sub.flatten() {
                        if let Ok(cm) = child.metadata()
                            && cm.uid() != me
                            && cm.uid() != 0
                        {
                            return Some((cm.uid(), gid, sock_gid));
                        }
                    }
                }
            }
        }
    }
    if gid != my_gid && gid != 0 {
        return Some((gid, gid, sock_gid));
    }
    None
}

#[cfg(not(unix))]
fn detect_seat_ids(_config_dir: &std::path::Path) -> Option<(u32, u32, u32)> {
    None
}

fn remove_path_with_seat(target: &std::path::Path, seat_ids: Option<(u32, u32, u32)>) {
    let Ok(meta) = fs::symlink_metadata(target) else {
        return;
    };
    if meta.file_type().is_symlink() || meta.is_file() {
        if fs::remove_file(target).is_ok() {
            return;
        }
    } else if meta.is_dir() && fs::remove_dir_all(target).is_ok() {
        return;
    }
    if let Some((uid, gid, sock_gid)) = seat_ids
        && std::path::Path::new("/usr/local/bin/anyagents-setpriv").is_file()
    {
        let uid_s = uid.to_string();
        let gid_s = gid.to_string();
        let groups_s = if sock_gid != gid && sock_gid != 0 {
            format!("{gid},{sock_gid}")
        } else {
            gid_s.clone()
        };
        let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
            .args([
                "--reuid",
                &uid_s,
                "--regid",
                &gid_s,
                "--groups",
                &groups_s,
                "--no-new-privs",
                "--",
                "/bin/rm",
                "-rf",
            ])
            .arg(target)
            .status();
        let _ = fs::remove_dir_all(target);
        let _ = fs::remove_file(target);
    }
}

fn record_turn<H: Harness>(
    delivery: &DeliveryLoop<H>,
    agent: &Agent,
    clock: &Arc<dyn Clock>,
    usage: &TurnUsage,
) -> bool {
    let result = delivery.store().record_usage(NewUsage {
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
    });
    result.is_ok()
}

/// One unit's share of a difference, spread over `turns` units so the sum of
/// the rows is the total the vendor reported.
fn owned_share(difference: TurnUsage, turns: u32) -> TurnUsage {
    let turns = u64::from(turns.max(1));
    let share = |value: u64| value / turns;
    TurnUsage {
        input_tokens: share(difference.input_tokens),
        output_tokens: share(difference.output_tokens),
        cache_read_tokens: share(difference.cache_read_tokens),
        cache_write_tokens: share(difference.cache_write_tokens),
        reported_cost_micros: difference
            .reported_cost_micros
            .map(|cost| cost / turns as i64),
    }
}

fn tokens(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

#[cfg(test)]
mod tests {
    #[test]
    fn declared_catalog_matches_native_steer_adapters() {
        for harness in [
            "codex",
            "claude-code",
            "opencode",
            "muse",
            "agy",
            "omp",
            "dsh",
            "grok",
        ] {
            assert_eq!(
                super::declared(harness),
                (super::DeliveryMode::Steer, super::SteerSupport::Yes),
                "{harness}"
            );
        }
        assert_eq!(
            super::declared("contract"),
            (super::DeliveryMode::Followup, super::SteerSupport::Unproven)
        );
    }

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
