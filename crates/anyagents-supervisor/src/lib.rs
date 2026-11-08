//! One process per running agent. The mailbox wakes it; a dead process waits out
//! a capped backoff; a quota answer waits until its reset time.

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixDatagram;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub use anyagents_agent::recovery_outbound;
use anyagents_agent::{DeliveryLoop, RunOutcome};
use anyagents_core::{
    Agent, AgentId, BillingMode, NewUsage, Store, Wake, agent_config_dir, agent_work_dir, utc_day,
};
use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, SteerSupport, TurnState, TurnUsage,
};

/// Maximum drain window (ms) to let an active turn finish naturally after a
/// harness or model switch is requested. While draining, new mailbox
/// messages are held back in SQLite (never steered into the retiring process)
/// so continuous incoming messages cannot extend the old turn indefinitely.
/// Once the turn finishes or this drain window expires, the process is replaced.
/// This is the only reason mail ever waits behind a live process. A credential
/// or profile change holds nothing: mail keeps flowing to the running process,
/// which moves to the new profile at its next natural boundary.
const PENDING_SWITCH_DRAIN_MS: i64 = 15_000;

/// After a replacement fails to launch, the old process keeps serving mail for
/// this long before the next attempt.
const SWITCH_RETRY_MS: i64 = 5_000;

/// Time source. Production uses the wall clock. Tests advance a manual clock.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
    /// Block until `woken` is set, `deadline` is due, or [`Clock::poke`] finds
    /// `woken` set. Consumes the flag; `true` when it was the flag that ended
    /// the wait, `false` when the deadline did.
    ///
    /// `None` waits for the flag. A deadline in the past returns at once.
    fn wait(&self, deadline: Option<i64>, woken: &Arc<AtomicBool>) -> bool;
    /// Wake every waiter whose flag is set. Set the flag first.
    fn poke(&self);
}

/// Wall clock. Each waiter parks its own thread, and [`Clock::poke`] unparks
/// only the waiters whose flag is set: one agent's mail or vendor activity
/// never wakes the rest of the fleet.
pub struct SystemClock {
    waiters: Mutex<Vec<Waiter>>,
    next_waiter: AtomicU64,
}

struct Waiter {
    id: u64,
    woken: Arc<AtomicBool>,
    thread: thread::Thread,
}

impl SystemClock {
    pub fn new() -> Self {
        Self {
            waiters: Mutex::new(Vec::new()),
            next_waiter: AtomicU64::new(0),
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

    fn wait(&self, deadline: Option<i64>, woken: &Arc<AtomicBool>) -> bool {
        if woken.swap(false, Ordering::SeqCst) {
            return true;
        }
        let id = self.next_waiter.fetch_add(1, Ordering::Relaxed);
        lock(&self.waiters).push(Waiter {
            id,
            woken: Arc::clone(woken),
            thread: thread::current(),
        });
        // Registered before the flag is read again: a poke that ran before
        // registration left the flag set, one after it unparks this thread
        // (an unpark before park is kept, so it is never lost).
        let flagged = loop {
            if woken.swap(false, Ordering::SeqCst) {
                break true;
            }
            let now = self.now_ms();
            match deadline {
                Some(deadline) if now >= deadline => break false,
                Some(deadline) => thread::park_timeout(Duration::from_millis(
                    deadline.saturating_sub(now).max(0) as u64,
                )),
                None => thread::park(),
            }
        };
        let mut waiters = lock(&self.waiters);
        if let Some(at) = waiters.iter().position(|waiter| waiter.id == id) {
            waiters.swap_remove(at);
        }
        flagged
    }

    fn poke(&self) {
        for waiter in lock(&self.waiters).iter() {
            if waiter.woken.load(Ordering::SeqCst) {
                waiter.thread.unpark();
            }
        }
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

    fn wait(&self, deadline: Option<i64>, woken: &Arc<AtomicBool>) -> bool {
        let mut guard = lock(&self.state.now_ms);
        loop {
            if woken.swap(false, Ordering::SeqCst) {
                return true;
            }
            if let Some(deadline) = deadline
                && *guard >= deadline
            {
                return false;
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

/// How soon a worker with an empty mailbox looks again after it last did
/// work or was woken. One store read, so `flush` and a restarted process never
/// leave a line waiting for an arrival.
const IDLE_REVIEW_MS: i64 = 250;
/// Ceiling of the idle review interval. Each review that finds nothing to do
/// doubles the interval up to this bound; mail, vendor activity and agent
/// wakes still end the wait at once and reset it. What only a review notices
/// (a background command exiting before scale-to-zero, a profile rotation
/// file) is picked up within this bound.
const IDLE_REVIEW_MAX_MS: i64 = 15_000;

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
    pub live_harness: Option<String>,
    pub live_model: Option<String>,
    pub process_started_at_ms: Option<i64>,
    pub last_error: Option<String>,
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
    match anyagents_harness::normalize_harness_name(harness) {
        "codex" | "claude-code" | "opencode" | "muse" | "agy" | "omp" | "dsh" | "grok"
        | "spiron" => (DeliveryMode::Steer, SteerSupport::Yes),
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
    done: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

#[cfg(test)]
type IdleReleaseHook = Arc<dyn Fn(&AgentId) + Send + Sync>;

struct Inner<H> {
    store: Arc<Store>,
    clock: Arc<dyn Clock>,
    launch: Launch<H>,
    extra_env: Vec<(String, String)>,
    board: HealthBoard,
    stop: AtomicBool,
    workers: Mutex<HashMap<AgentId, Slot>>,
    #[cfg(test)]
    idle_release_hook: Option<IdleReleaseHook>,
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
            #[cfg(test)]
            idle_release_hook: None,
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
                if inner.store.complete_idle_agent(&agent.id).unwrap_or(false) {
                    continue;
                }
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
                if is_running(&inner.store, &agent_id) {
                    if let Some(slot) = lock(&inner.workers).get(&agent_id) {
                        slot.woken.store(true, Ordering::SeqCst);
                        inner.clock.poke();
                    }
                    ensure(&inner, agent_id);
                } else {
                    stop_slot(&inner, &agent_id);
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
                live_harness: None,
                live_model: None,
                process_started_at_ms: None,
                last_error: None,
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
            if !finished && !stopping {
                slot.woken.store(true, Ordering::SeqCst);
                drop(workers);
                inner.clock.poke();
                return;
            }
            let thread = slot.thread.take();
            workers.remove(&id);
            thread.map(|t| (t, finished))
        } else {
            None
        }
    };
    if let Some((thread, finished)) = stopping_thread {
        if finished {
            let _ = thread.join();
        } else {
            let _ = thread::Builder::new()
                .name("anyagents-reap".into())
                .spawn(move || {
                    let _ = thread.join();
                });
        }
    }
    let mut workers = lock(&inner.workers);
    if workers.contains_key(&id) {
        return;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let woken = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let run_inner = Arc::clone(inner);
    let run_id = id.clone();
    let run_stop = Arc::clone(&stop);
    let run_woken = Arc::clone(&woken);
    let run_done = Arc::clone(&done);
    let thread = thread::Builder::new()
        .name("anyagents-agent".into())
        .spawn(move || {
            let _ = run_agent(run_inner, run_id, run_stop, run_woken);
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
            done,
            thread: Some(thread),
        },
    );
}

enum Boot<H: Harness> {
    Ready(Box<DeliveryLoop<H>>),
    Busy,
    Stopped,
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
        quota_started_at: None,
        pending_switch_started_at: None,
        switch_retry_at: 0,
        process_started_at: None,
        process_profile_rotated_ms: 0,
        last_error: None,
        consecutive_turn_errors: 0,
        idle_review_ms: IDLE_REVIEW_MS,
        process_scan_logged: false,
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
    /// automatic scale-to-zero (`idle`) for any agent after a completed turn.
    completed_turn_in_session: bool,
    /// Clock timestamp (`now_ms`) when the current quota wait began, so an
    /// operator profile refresh/upgrade (`.anyagents-profile-rotated-ms`)
    /// can clear the quota hold before `reset_at`.
    quota_started_at: Option<i64>,
    pending_switch_started_at: Option<i64>,
    /// No replacement is attempted before this clock time (see `SWITCH_RETRY_MS`).
    switch_retry_at: i64,
    process_started_at: Option<i64>,
    process_profile_rotated_ms: i64,
    last_error: Option<String>,
    consecutive_turn_errors: u32,
    /// Current idle review interval, between `IDLE_REVIEW_MS` and
    /// `IDLE_REVIEW_MAX_MS`.
    idle_review_ms: i64,
    /// Whether the whole-process-table fallback of the in-flight check was
    /// already reported for this agent.
    process_scan_logged: bool,
}

impl<H: Harness + 'static> Session<H> {
    fn run(&mut self) -> bool {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                self.record_usage();
                self.delivery = None;
                self.publish();
                return false;
            }
            if self.delivery.is_none() {
                match self.boot() {
                    Boot::Ready(delivery) => {
                        delivery.harness().watch_activity(Arc::clone(&self.notify));
                        self.restart_at = None;
                        // Unknown until recover_once reads durable state successfully.
                        self.pending_recovery = true;
                        self.delivery = Some(*delivery);
                    }
                    Boot::Busy => {
                        self.delivery = None;
                        self.restart_at = None;
                        self.publish();
                        return true;
                    }
                    Boot::Stopped => {
                        self.delivery = None;
                        self.restart_at = None;
                        self.publish();
                        return false;
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
            // A changed configuration supersedes the old process's fault,
            // quota hold or restart delay before any further dispatch.
            if self.try_apply_pending_switch() {
                continue;
            }
            if self.quota_waiting() {
                continue;
            }
            if !self.process_alive() {
                self.record_usage();
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

    fn boot(&mut self) -> Boot<H> {
        if self.stop.load(Ordering::SeqCst) {
            return Boot::Stopped;
        }
        let store = match Store::open(self.inner.store.data_dir().join("state.db")) {
            Ok(store) => store,
            Err(_) => return Boot::Retry,
        };
        // Acquire the per-agent process lock BEFORE launching the child or
        // creating workspace/scratch directories so concurrent archive/delete
        // reaping under this lock cannot race an in-flight spawn.
        let lock = match store.try_lock_agent(&self.id) {
            Ok(lock) => lock,
            Err(anyagents_core::Error::AgentBusy) => return Boot::Busy,
            Err(_) => return Boot::Retry,
        };
        match store.agent(&self.id) {
            Ok(fresh) if fresh.state != "running" => return Boot::Stopped,
            Ok(_) => {}
            Err(anyagents_core::Error::AgentNotFound(_)) => return Boot::Stopped,
            Err(_) => return Boot::Retry,
        }
        if self.stop.load(Ordering::SeqCst) {
            return Boot::Stopped;
        }
        let harness = match self.launch_harness() {
            Ok(harness) => harness,
            Err(_) if self.stop.load(Ordering::SeqCst) => return Boot::Stopped,
            Err(_) => return Boot::Retry,
        };
        match DeliveryLoop::start_with_lock(store, lock, self.id.clone(), harness) {
            Ok(delivery) => Boot::Ready(Box::new(delivery)),
            Err(err) if err.agent_busy() => Boot::Busy,
            Err(_) => Boot::Retry,
        }
    }

    fn launch_harness(&mut self) -> Result<H, String> {
        if self.stop.load(Ordering::SeqCst) {
            return Err("session stopping".into());
        }
        // The running process keeps this identity unless a replacement starts.
        let serving = self.agent.clone();
        if let Ok(fresh) = self.inner.store.agent(&self.id) {
            if matches!(
                fresh.state.as_str(),
                "archived" | "paused" | "stopped" | "idle"
            ) {
                self.stop.store(true, Ordering::SeqCst);
                return Err(format!("agent is {}", fresh.state));
            }
            self.paths = AgentPaths::from_store(&self.inner.store, &fresh);
            self.agent = fresh;
        }
        let _ = fs::create_dir_all(&self.paths.config_dir);
        let _ = fs::create_dir_all(&self.paths.managed_work);
        let _ = fs::create_dir_all(&self.paths.work_dir);
        let rotated_ms =
            fs::read_to_string(self.paths.config_dir.join(".anyagents-profile-rotated-ms"))
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .unwrap_or(0);
        let res = (self.inner.launch)(LaunchRequest {
            agent: self.agent.clone(),
            data_dir: self.inner.store.data_dir().to_path_buf(),
            config_dir: self.paths.config_dir.clone(),
            work_dir: self.paths.work_dir.clone(),
            extra_env: self.inner.extra_env.clone(),
        });
        match &res {
            Ok(_) => {
                let rotated_advanced = rotated_ms > self.process_profile_rotated_ms;
                self.pending_switch_started_at = None;
                self.process_started_at = Some(self.inner.clock.now_ms());
                self.process_profile_rotated_ms = rotated_ms;
                if rotated_advanced || self.consecutive_turn_errors == 0 {
                    self.last_error = None;
                    if rotated_advanced {
                        self.consecutive_turn_errors = 0;
                    }
                }
            }
            Err(err) => {
                self.last_error = Some(err.clone());
                self.agent = serving;
            }
        }
        res
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
                    live_harness: health.process_alive.then(|| self.agent.harness.clone()),
                    live_model: if health.process_alive {
                        self.agent.model.clone()
                    } else {
                        None
                    },
                    process_started_at_ms: if health.process_alive {
                        self.process_started_at
                    } else {
                        None
                    },
                    last_error: health
                        .sign_in_fault
                        .or_else(|| delivery.harness().exit_diagnostic())
                        .or_else(|| self.last_error.clone()),
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
                    live_harness: None,
                    live_model: None,
                    process_started_at_ms: None,
                    last_error: self.last_error.clone(),
                }
            }
        };
        self.inner.board.publish(&self.id, state);
    }

    fn wait_backoff(&mut self) -> bool {
        let started_at = self.inner.clock.now_ms();
        if self.restart_at.is_none() {
            self.attempt = self.attempt.saturating_add(1);
            let delay = restart_delay_ms(self.attempt, salt_for(&self.id, self.attempt));
            self.restart_at = Some(started_at.saturating_add(delay));
        }
        self.publish();
        let Some(deadline) = self.restart_at else {
            return !self.stop.load(Ordering::SeqCst);
        };
        while self.inner.clock.now_ms() < deadline {
            if self.stop.load(Ordering::SeqCst) {
                return false;
            }
            let rotated =
                fs::read_to_string(self.paths.config_dir.join(".anyagents-profile-rotated-ms"))
                    .ok()
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .is_some_and(|ms| ms >= started_at);
            let cleared =
                fs::read_to_string(self.paths.config_dir.join(".anyagents-quota-cleared-ms"))
                    .ok()
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .is_some_and(|ms| ms >= started_at);
            let config_switched = self.inner.store.agent(&self.id).is_ok_and(|fresh| {
                fresh.harness != self.agent.harness || fresh.model != self.agent.model
            });
            if rotated || cleared || config_switched {
                self.attempt = 0;
                break;
            }
            self.inner.clock.wait(Some(deadline), &self.woken);
        }
        self.restart_at = None;
        !self.stop.load(Ordering::SeqCst)
    }

    fn profile_rotation_pending(&self) -> bool {
        fs::read_to_string(self.paths.config_dir.join(".anyagents-profile-rotated-ms"))
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .is_some_and(|ms| ms > self.process_profile_rotated_ms)
    }

    fn profile_rotation_waits_for_turn(&self) -> bool {
        self.profile_rotation_pending()
            && self.delivery.as_ref().is_some_and(|delivery| {
                let health = delivery.harness().health();
                health.process_alive && health.turn_state == TurnState::InTurn
            })
    }

    /// The harness or model this agent is configured for differs from the one
    /// its process runs.
    fn runtime_switch_pending(&self) -> bool {
        self.inner.store.agent(&self.id).is_ok_and(|fresh| {
            fresh.harness != self.agent.harness || fresh.model != self.agent.model
        })
    }

    /// Returns whether a new process took over.
    fn replace_process(&mut self) -> bool {
        // Every replacement path (including quota recovery) must preserve an
        // active child when only a profile refresh is pending. A dead child can
        // still be recovered immediately, and an explicit harness or model
        // switch has already had its bounded drain.
        if self.profile_rotation_waits_for_turn() && !self.runtime_switch_pending() {
            return false;
        }
        if self.delivery.is_none() {
            self.restart_at = None;
            return false;
        }
        // Flush any usage the exited process finished serving before swapping it.
        if !self.record_usage() {
            return false;
        }
        match self.launch_harness() {
            Ok(mut harness) => {
                harness.watch_activity(Arc::clone(&self.notify));
                // A quota wait belonged to the process that is gone.
                harness.clear_quota();
                if let Some(delivery) = self.delivery.as_mut() {
                    self.pending_recovery = true;
                    delivery.replace_harness(harness);
                }
                self.served = None;
                self.turns_recorded = 0;
                self.restart_at = None;
                true
            }
            Err(_) => {
                self.restart_at = None;
                false
            }
        }
    }

    fn quota_waiting(&mut self) -> bool {
        let reset_at = self
            .delivery
            .as_ref()
            .and_then(|delivery| delivery.harness().health().quota_reset_at_ms);
        let Some(reset_at) = reset_at else {
            self.quota_started_at = None;
            return false;
        };
        let now = self.inner.clock.now_ms();
        let started_at = *self.quota_started_at.get_or_insert(now);
        let rotated_after_start =
            fs::read_to_string(self.paths.config_dir.join(".anyagents-profile-rotated-ms"))
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .is_some_and(|ms| ms >= started_at);
        let cleared_after_start =
            fs::read_to_string(self.paths.config_dir.join(".anyagents-quota-cleared-ms"))
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .is_some_and(|ms| ms >= started_at);
        let config_switched = self.inner.store.agent(&self.id).is_ok_and(|fresh| {
            fresh.harness != self.agent.harness || fresh.model != self.agent.model
        });
        if rotated_after_start || cleared_after_start || config_switched || now >= reset_at {
            self.quota_started_at = None;
            if let Some(delivery) = self.delivery.as_mut() {
                delivery.clear_quota();
            }
            if rotated_after_start || config_switched {
                self.replace_process();
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
        if self.try_apply_pending_switch() {
            return false;
        }
        if let Some(switch_started_at) = self.pending_switch_started_at {
            // An explicit harness or model switch is pending while the old
            // process is mid-turn: hold new mailbox messages in SQLite for the
            // replacement process instead of steering them into the old turn,
            // and cut over at the turn boundary or at the drain deadline.
            let deadline = switch_started_at.saturating_add(PENDING_SWITCH_DRAIN_MS);
            let now = self.inner.clock.now_ms();
            // Never busy-loop on a wait whose deadline is already past.
            let wait_until = if deadline > now {
                deadline.min(now + 1_000)
            } else {
                now + 1_000
            };
            self.inner.clock.wait(Some(wait_until), &self.woken);
            return false;
        }
        if self.pending_recovery
            && self
                .delivery
                .as_ref()
                .is_some_and(|d| d.harness().has_unconfirmed_hold())
        {
            if self.recover_once() {
                return false;
            }
            if !self.pending_recovery {
                self.wake_unless_more();
                return false;
            }
        }
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
                self.last_error = None;
                self.written = self
                    .written
                    .saturating_add(delivered.clamp(0, u64::from(u32::MAX)) as u32);
                self.attempt = 0;
                self.idle_review_ms = IDLE_REVIEW_MS;
                self.wake_unless_more();
                false
            }
            Ok(RunOutcome::Idle) => {
                // No message was accepted on this pass (the mailbox may still
                // contain deferred mail). Record whatever the vendor reported.
                self.attempt = 0;
                if !self.record_usage() {
                    self.publish();
                    self.inner.clock.wait(
                        Some(self.inner.clock.now_ms() + IDLE_REVIEW_MS),
                        &self.woken,
                    );
                    return false;
                }
                if self.try_apply_pending_switch() {
                    return false;
                }
                if self.delivery.as_ref().is_some_and(|d| {
                    !d.harness().health().process_alive
                        && (d.store().has_open_turn(&self.id).unwrap_or(false)
                            || d.harness().has_unconfirmed_hold())
                }) {
                    if !self.wait_backoff() {
                        return false;
                    }
                    self.replace_process();
                    return false;
                }
                if self.pending_recovery && self.recover_once() {
                    return false;
                }
                if let Some(err) = self.recoverable_mid_turn_fault() {
                    self.last_error = Some(err);
                    if self.consecutive_turn_errors < 3 {
                        self.consecutive_turn_errors =
                            self.consecutive_turn_errors.saturating_add(1);
                        self.publish();
                        if !self.wait_backoff() {
                            return false;
                        }
                        self.replace_process();
                        return false;
                    }
                    self.pending_recovery = false;
                }
                if self.try_release_idle_agent() {
                    return false;
                }
                self.idle_wait();
                false
            }
            Err(anyagents_agent::Error::HarnessDown) => {
                if !self.wait_backoff() {
                    return false;
                }
                self.replace_process();
                false
            }
            Err(anyagents_agent::Error::HarnessFailed(err)) => {
                self.consecutive_turn_errors = self.consecutive_turn_errors.saturating_add(1);
                let lower = err.to_ascii_lowercase();
                let is_auth_401 = lower.contains("(401)")
                    || lower.contains(" 401 ")
                    || lower.contains("status 401")
                    || lower.contains("http 401")
                    || lower.contains("unauthorized")
                    || lower.contains("invalid_api_key");
                self.last_error = Some(err);
                if is_auth_401 || self.consecutive_turn_errors >= 3 {
                    self.pending_recovery = false;
                }
                self.publish();
                if !self.wait_backoff() {
                    return false;
                }
                self.replace_process();
                false
            }
            Err(err) if err.agent_busy() => true,
            Err(err) => {
                // Store errors (locked/malformed) retry with backoff, not the
                // fixed 250ms idle poll: a fleet of agents hitting the same
                // locked store at 4Hz pins the daemon at ~100% per agent.
                // If the child also died before or during this store failure,
                // replace it once the backoff clears instead of entering a
                // second consecutive wait_backoff() on the outer !process_alive
                // check.
                self.last_error = Some(format!("store: {err}"));
                self.publish();
                if !self.wait_backoff() {
                    return false;
                }
                if !self.process_alive() {
                    self.replace_process();
                }
                false
            }
        }
    }

    /// When a running agent's configured harness or model changed, hold new
    /// incoming mailbox messages at the gate so they cannot extend an active
    /// turn on the retiring process, and replace the live process immediately
    /// when idle or faulted, or once `PENDING_SWITCH_DRAIN_MS` expires.
    ///
    /// A profile change holds no mail and never preempts a live `InTurn` child:
    /// the process keeps working and is replaced when it is next idle or faulted.
    /// A replacement that fails to launch leaves the old process serving mail.
    fn try_apply_pending_switch(&mut self) -> bool {
        let replaceable = {
            let Some(delivery) = self.delivery.as_ref() else {
                self.pending_switch_started_at = None;
                return false;
            };
            let health = delivery.harness().health();
            health.turn_state == TurnState::Idle
                || !health.process_alive
                || health.sign_in_fault.is_some()
                || self.last_error.is_some()
        };
        let Ok(fresh) = self.inner.store.agent(&self.id) else {
            return false;
        };
        let now = self.inner.clock.now_ms();
        if now < self.switch_retry_at {
            self.pending_switch_started_at = None;
            return false;
        }
        let runtime_changed =
            fresh.harness != self.agent.harness || fresh.model != self.agent.model;
        if !runtime_changed {
            self.pending_switch_started_at = None;
            if !replaceable
                || !self.profile_rotation_pending()
                || self.profile_rotation_waits_for_turn()
            {
                return false;
            }
            return self.finish_switch(now);
        }
        let started_at = *self.pending_switch_started_at.get_or_insert(now);
        let drain_expired = now.saturating_sub(started_at) >= PENDING_SWITCH_DRAIN_MS;
        if !replaceable && !drain_expired {
            return false;
        }
        self.pending_switch_started_at = None;
        self.finish_switch(now)
    }

    fn finish_switch(&mut self, now: i64) -> bool {
        if self.replace_process() {
            return true;
        }
        self.switch_retry_at = now.saturating_add(SWITCH_RETRY_MS);
        false
    }

    /// Record the usage the harness served since the last row. It is read when
    /// the mailbox has drained, never holds a line back, and adds nothing while
    /// the harness has served nothing new.
    fn record_usage(&mut self) -> bool {
        let Some(delivery) = self.delivery.as_ref() else {
            return true;
        };
        let served = delivery.harness().last_usage();
        let before = self.served.clone().unwrap_or_default();
        let turns_served = delivery.harness().turns_served();
        let health = delivery.harness().health();
        let completed = (turns_served > self.turns_recorded || served != before)
            && health.turn_state == TurnState::Idle
            && health.sign_in_fault.is_none();
        // Persist completion before publishing progress or resetting the usage
        // baseline. On failure the same turn remains eligible for another pass.
        if completed && let Err(err) = delivery.store().note_turn_completed(&self.id) {
            eprintln!(
                "anyagents-supervisor: {}: persist turn completion failed: {err}",
                self.id
            );
            self.last_error = Some(format!("persist turn completion: {err}"));
            return false;
        }
        if served != before {
            let turns = turns_served
                .saturating_sub(self.turns_recorded)
                .max(self.written.max(1));
            let unit = owned_share(served.difference(before), turns);
            for _ in 0..turns {
                if unit.input_tokens == 0 && unit.output_tokens == 0 {
                    break;
                }
                if let Err(err) = record_turn(delivery, &self.agent, &self.inner.clock, &unit) {
                    eprintln!(
                        "anyagents-supervisor: {}: persist turn usage failed: {err}",
                        self.id
                    );
                    self.last_error = Some(format!("persist turn usage failed: {err}"));
                    return false;
                }
            }
            self.served = Some(served);
            self.written = 0;
        }
        if completed {
            self.turns_recorded = turns_served;
            self.completed_turn_in_session = true;
            self.consecutive_turn_errors = 0;
            self.last_error = None;
        }
        true
    }

    /// Automatically scale any agent to zero (`state = "idle"`)
    /// once it has finished its turn, its mailbox is drained, it has no active
    /// child sub-agents, and its harness process has no in-flight background commands.
    /// Its config directory and session are preserved so sending any future message
    /// to the agent automatically wakes and resumes it.
    fn try_release_idle_agent(&mut self) -> bool {
        if self.last_error.is_some() {
            return false;
        }
        let scaled = {
            let Some(delivery) = self.delivery.as_ref() else {
                return false;
            };
            let health = delivery.harness().health();
            if health.turn_state != TurnState::Idle || health.sign_in_fault.is_some() {
                return false;
            }
            let mut scanned = false;
            let inflight =
                harness_has_inflight_processes(delivery.harness().process_id(), &mut scanned);
            if scanned && !self.process_scan_logged {
                self.process_scan_logged = true;
                eprintln!(
                    "anyagents-supervisor: {}: /proc/<pid>/task/<tid>/children is unavailable; \
                     idle release reads the whole process table on each review",
                    self.id
                );
            }
            if inflight {
                return false;
            }
            delivery
                .store()
                .complete_idle_agent(&self.id)
                .unwrap_or(false)
        };
        if !scaled {
            return false;
        }
        #[cfg(test)]
        if let Some(hook) = &self.inner.idle_release_hook {
            hook(&self.id);
        }
        // Mail may have committed after the idle transition, with its wake
        // already consumed by ensure() while this slot still looked active.
        // Serialize the fresh check and stop publication with ensure(): abort
        // for existing work; a later arrival sees a stopping slot and replaces
        // it. Keep the delivery lock until teardown finishes under this guard,
        // so the replacement cannot boot into AgentBusy and lose that wake.
        let inner = Arc::clone(&self.inner);
        let _workers = lock(&inner.workers);
        if !inner
            .store
            .agent(&self.id)
            .is_ok_and(|agent| agent.state == "idle")
            || !matches!(inner.store.next_undelivered(&self.id), Ok(None))
        {
            self.woken.store(true, Ordering::SeqCst);
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
    fn recoverable_mid_turn_fault(&self) -> Option<String> {
        let delivery = self.delivery.as_ref()?;
        let health = delivery.harness().health();
        let fault = health.sign_in_fault?;
        if delivery.harness().has_unconfirmed_idle_hold() {
            return Some(fault);
        }
        if health.turn_state != TurnState::Idle
            || !delivery.store().has_open_turn(&self.id).unwrap_or(false)
        {
            return None;
        }
        is_transient_turn_fault(&fault).then_some(fault)
    }

    fn recover_once(&mut self) -> bool {
        let outcome = {
            let Some(delivery) = self.delivery.as_mut() else {
                self.pending_recovery = false;
                return false;
            };
            let has_hold = delivery.harness().has_unconfirmed_hold();
            if has_hold {
                let held_ids = delivery.harness().peek_unconfirmed_request_ids();
                if !held_ids.is_empty()
                    && let Err(err) = delivery
                        .store()
                        .promote_messages_to_open_turn(&self.id, &held_ids)
                {
                    self.last_error = Some(format!("read open turn for recovery: {err}"));
                    return false;
                }
            } else if !matches!(delivery.store().next_undelivered(&self.id), Ok(None)) {
                // Idle also covers deferred or passive mail. Recovery must not
                // overtake any queued message, and a failed read is not emptiness.
                return false;
            }
            let open = match delivery.store().open_turn(&self.id) {
                Ok(open) => open,
                Err(err) => {
                    self.last_error = Some(format!("read open turn for recovery: {err}"));
                    return false;
                }
            };
            let reconciled = if has_hold {
                match delivery.harness_mut().reconcile_unconfirmed_for_recovery() {
                    Ok(rec) => Ok(rec),
                    Err(anyagents_harness::HarnessError::Dead) => {
                        Err(anyagents_agent::Error::HarnessDown)
                    }
                    Err(anyagents_harness::HarnessError::Failed(reason)) => {
                        Err(anyagents_agent::Error::HarnessFailed(reason))
                    }
                }
            } else {
                Ok(None)
            };
            match (reconciled, open) {
                (Err(err), _) => Err(err),
                (Ok(_), None) => {
                    self.pending_recovery = false;
                    if self
                        .last_error
                        .as_deref()
                        .is_some_and(|error| error.starts_with("read open turn for recovery:"))
                    {
                        self.last_error = None;
                    }
                    if has_hold {
                        self.last_error = None;
                    }
                    return false;
                }
                (Ok(rec), Some(open)) => {
                    delivery.recover_open_turn_with_reconciliation(&open, rec.as_ref())
                }
            }
        };
        match outcome {
            Ok(DeliverOutcome::Accepted { .. }) => {
                self.pending_recovery = false;
                self.last_error = None;
                self.written = self.written.saturating_add(1);
                false
            }
            Ok(DeliverOutcome::Deferred) | Ok(DeliverOutcome::Quota { .. }) => false,
            Err(anyagents_agent::Error::HarnessDown) => {
                if !self.wait_backoff() {
                    return true;
                }
                self.replace_process();
                true
            }
            Err(anyagents_agent::Error::HarnessFailed(err)) => {
                self.consecutive_turn_errors = self.consecutive_turn_errors.saturating_add(1);
                let lower = err.to_ascii_lowercase();
                let is_auth_401 = lower.contains("(401)")
                    || lower.contains(" 401 ")
                    || lower.contains("status 401")
                    || lower.contains("http 401")
                    || lower.contains("unauthorized")
                    || lower.contains("invalid_api_key");
                self.last_error = Some(err);
                if is_auth_401 || self.consecutive_turn_errors >= 3 {
                    self.pending_recovery = false;
                    self.publish();
                    return false;
                }
                if !self.wait_backoff() {
                    return true;
                }
                self.replace_process();
                true
            }
            Err(err) => {
                self.last_error = Some(format!("persist turn recovery: {err}"));
                false
            }
        }
    }

    /// Sleep through one idle review interval. A wake (mail, vendor activity,
    /// agent change) ends it at once and resets the interval; a review that
    /// ran out its interval with nothing to do doubles the next one, so an
    /// idle agent's store reads and process checks fall to one per
    /// `IDLE_REVIEW_MAX_MS` instead of four a second.
    fn idle_wait(&mut self) {
        let deadline = self.inner.clock.now_ms() + self.idle_review_ms;
        self.idle_review_ms = if self.inner.clock.wait(Some(deadline), &self.woken) {
            IDLE_REVIEW_MS
        } else {
            self.idle_review_ms
                .saturating_mul(2)
                .min(IDLE_REVIEW_MAX_MS)
        };
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

fn is_transient_turn_fault(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    if lower.contains("(400)")
        || lower.contains("http 400")
        || lower.contains("status 400")
        || lower.contains("(401)")
        || lower.contains(" 401 ")
        || lower.contains("status 401")
        || lower.contains("http 401")
        || lower.contains("unauthorized")
        || lower.contains("invalid_api_key")
        || lower.contains("(403)")
        || lower.contains("http 403")
        || lower.contains("context limit")
        || lower.contains("not signed in")
    {
        return false;
    }
    [
        "server_error",
        "internal_error",
        "overloaded",
        "http 500",
        "http 502",
        "http 503",
        "http 504",
        "status 500",
        "status 502",
        "status 503",
        "status 504",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
        "connection reset",
        "broken pipe",
        "unexpected eof",
        "stream closed",
        "stream ended",
        "timed out",
        "temporarily unavailable",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// Return `true` when `root_pid` has any non-zombie descendant process in `/proc`
/// other than an idle persistent sidecar (`anyagents ... mcp`, `codex-code-mode-host`,
/// or `language_server_linux`) that itself has no child processes.
/// `true` when the harness process has a command running under it (any
/// child other than an idle persistent sidecar). Sets `scanned` when the
/// kernel offers no per-task `children` file and the whole process table had
/// to be read instead.
fn harness_has_inflight_processes(root_pid: Option<u32>, scanned: &mut bool) -> bool {
    let Some(root_pid) = root_pid.filter(|&p| p > 0) else {
        return false;
    };
    #[cfg(unix)]
    {
        // Read the harness's own subtree: a whole-/proc scan per idle review
        // reads every process's status file on the host, per agent.
        let children = match proc_children(root_pid) {
            Some(children) => children,
            None => {
                *scanned = true;
                return harness_has_inflight_processes_by_scan(root_pid);
            }
        };
        for cpid in children {
            if !proc_is_live(cpid) {
                continue;
            }
            if is_persistent_sidecar(cpid) {
                if proc_children(cpid)
                    .unwrap_or_default()
                    .into_iter()
                    .any(proc_is_live)
                {
                    return true;
                }
            } else {
                return true;
            }
        }
        false
    }
    #[cfg(not(unix))]
    {
        let _ = (root_pid, scanned);
        false
    }
}

/// Direct children of `pid` from `/proc/<pid>/task/<tid>/children`. A
/// process that is gone has none. `None` only when the process's tasks are
/// readable but no task offers a `children` file (a kernel built without
/// `CONFIG_PROC_CHILDREN`).
#[cfg(unix)]
fn proc_children(pid: u32) -> Option<Vec<u32>> {
    let Ok(tasks) = fs::read_dir(format!("/proc/{pid}/task")) else {
        return Some(Vec::new());
    };
    let mut exposed = false;
    let mut children = Vec::new();
    for task in tasks.flatten() {
        if let Ok(list) = fs::read_to_string(task.path().join("children")) {
            exposed = true;
            children.extend(
                list.split_whitespace()
                    .filter_map(|p| p.parse::<u32>().ok()),
            );
        }
    }
    exposed.then_some(children)
}

/// `true` unless the process is gone, a zombie, or dead.
#[cfg(unix)]
fn proc_is_live(pid: u32) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    let state = stat
        .rfind(')')
        .and_then(|end| stat[end + 1..].trim_start().chars().next())
        .unwrap_or(' ');
    !matches!(state, 'Z' | 'X' | 'x')
}

#[cfg(unix)]
fn is_persistent_sidecar(pid: u32) -> bool {
    let cmdline = fs::read(format!("/proc/{pid}/cmdline"))
        .unwrap_or_default()
        .into_iter()
        .map(|b| if b == 0 { b' ' } else { b })
        .collect::<Vec<u8>>();
    let cmd = String::from_utf8_lossy(&cmdline);
    let cmd_trimmed = cmd.trim();
    cmd_trimmed.contains(" mcp ")
        || cmd_trimmed.ends_with(" mcp")
        || cmd_trimmed.contains("codex-code-mode-host")
        || cmd_trimmed.contains("language_server_linux")
}

/// Fallback for kernels without `/proc/<pid>/task/<tid>/children`.
#[cfg(unix)]
fn harness_has_inflight_processes_by_scan(root_pid: u32) -> bool {
    let Ok(entries) = fs::read_dir("/proc") else {
        return false;
    };
    let mut children_of: HashMap<u32, Vec<u32>> = HashMap::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(pid_str) = name.to_str() else {
            continue;
        };
        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };
        let Ok(status) = fs::read_to_string(entry.path().join("status")) else {
            continue;
        };
        let mut ppid = 0u32;
        let mut state_ch = ' ';
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("PPid:") {
                ppid = rest.trim().parse::<u32>().unwrap_or(0);
            } else if let Some(rest) = line.strip_prefix("State:") {
                state_ch = rest.trim().chars().next().unwrap_or(' ');
            }
        }
        if ppid > 0 && !matches!(state_ch, 'Z' | 'X') {
            children_of.entry(ppid).or_default().push(pid);
        }
    }
    let Some(direct_children) = children_of.get(&root_pid) else {
        return false;
    };
    for &cpid in direct_children {
        if is_persistent_sidecar(cpid) {
            if children_of
                .get(&cpid)
                .is_some_and(|grandkids| !grandkids.is_empty())
            {
                return true;
            }
        } else {
            return true;
        }
    }
    false
}

fn reap_ephemeral_dirs(config_dir: &std::path::Path, managed_work: &std::path::Path) {
    let seat_ids = detect_seat_ids(config_dir);
    for rel in [
        "cache-home",
        "tmp",
        ".tmp",
        ".rustup",
        ".cargo",
        ".gemini/tmp",
        ".cache",
        "cache",
    ] {
        let target = config_dir.join(rel);
        remove_path_with_seat(&target, seat_ids);
    }
    const BUILD_DIRS: &[&str] = &["target", "node_modules", ".next", ".turbo", "dist"];
    for rel in BUILD_DIRS {
        remove_path_with_seat(&managed_work.join(rel), seat_ids);
    }
    for container in ["apps", "packages", "crates", "services"] {
        let Ok(entries) = fs::read_dir(managed_work.join(container)) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                for rel in BUILD_DIRS {
                    remove_path_with_seat(&path.join(rel), seat_ids);
                }
            }
        }
    }
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
            .stderr(std::process::Stdio::null())
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
) -> Result<(), anyagents_core::Error> {
    delivery.store().record_usage(NewUsage {
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
    })?;
    Ok(())
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
            "spiron",
            "sylphx",
            "sylphx-agent",
            "sylphx_agent",
            "sylphx/default",
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
    fn legacy_admin_files_do_not_disable_spiron_supervision() {
        let dir = tempfile::tempdir().unwrap();
        for legacy in ["admin.sock", "admin.lock"] {
            fs::write(dir.path().join(legacy), b"stale").unwrap();
        }
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let mut new = anyagents_core::NewAgent::new("cloud-worker", "spiron");
        new.role = Some("Worker".into());
        let agent = store.create_agent(new).unwrap();
        store.set_agent_state(&agent.id, "running").unwrap();
        store
            .receive(&agent.id, anyagents_core::Incoming::new("first"))
            .unwrap();
        let (launched, launches) = std::sync::mpsc::channel();
        let supervisor = Supervisor::start(
            Arc::clone(&store),
            Arc::new(SystemClock::new()),
            Arc::new(move |_| {
                launched.send(()).unwrap();
                Ok(anyagents_harness::FakeHarness::new(DeliveryMode::Steer))
            }),
            Vec::new(),
        )
        .unwrap();
        launches.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(is_running(&store, &agent.id));
        store
            .receive(&agent.id, anyagents_core::Incoming::new("second"))
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while store.delivered_cursor(&agent.id).unwrap() != 2 {
            assert!(std::time::Instant::now() < deadline, "mail was stranded");
            thread::sleep(Duration::from_millis(5));
        }
        drop(supervisor);
    }

    struct FinishFirstTurn(anyagents_harness::FakeHarness);

    impl Harness for FinishFirstTurn {
        fn deliver(
            &mut self,
            message: &anyagents_harness::OutboundMessage,
        ) -> Result<anyagents_harness::DeliverOutcome, anyagents_harness::HarnessError> {
            self.0.set_auto_finish(message.seq == 1);
            self.0.deliver(message)
        }

        fn background(&mut self) -> Result<(), anyagents_harness::HarnessError> {
            self.0.background()
        }

        fn resume(&mut self, session: &str) -> Result<(), anyagents_harness::HarnessError> {
            self.0.resume(session)
        }

        fn cancel(&mut self) -> Result<(), anyagents_harness::HarnessError> {
            self.0.cancel()
        }

        fn health(&self) -> anyagents_harness::HarnessHealth {
            self.0.health()
        }

        fn turns_served(&self) -> u32 {
            self.0.turns_served()
        }
    }

    #[test]
    fn mail_during_idle_release_keeps_a_roleless_child_live() {
        assert_mail_during_idle_release_is_delivered(false, None);
    }

    #[test]
    fn mail_during_idle_release_keeps_a_role_bearing_child_live() {
        assert_mail_during_idle_release_is_delivered(false, Some("Researcher"));
    }

    #[test]
    fn mail_during_idle_release_keeps_a_role_bearing_root_live() {
        assert_mail_during_idle_release_is_delivered(true, Some("Researcher"));
    }

    fn assert_mail_during_idle_release_is_delivered(is_root: bool, role: Option<&str>) {
        use std::sync::mpsc;
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let mut new = anyagents_core::NewAgent::new("worker", "contract");
        if !is_root {
            let parent = store
                .create_agent(anyagents_core::NewAgent::new("parent", "contract"))
                .unwrap();
            new.parent_id = Some(parent.id);
        }
        new.role = role.map(str::to_string);
        let agent = store.create_agent(new).unwrap();
        let work = dir.path().join("work");
        fs::create_dir_all(&work).unwrap();
        store
            .set_agent_running(&agent.id, work.to_str().unwrap())
            .unwrap();
        store
            .receive(&agent.id, anyagents_core::Incoming::new("first"))
            .unwrap();

        let (at_idle_tx, at_idle_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let resume_rx = Mutex::new(resume_rx);
        let launches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted_launches = Arc::clone(&launches);
        let inner = Arc::new(Inner {
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock::new()),
            launch: Arc::new(move |_| {
                counted_launches.fetch_add(1, Ordering::SeqCst);
                Ok(FinishFirstTurn(anyagents_harness::FakeHarness::new(
                    DeliveryMode::Steer,
                )))
            }),
            extra_env: Vec::new(),
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            workers: Mutex::new(HashMap::new()),
            idle_release_hook: Some(Arc::new(move |_| {
                at_idle_tx.send(()).unwrap();
                lock(&resume_rx)
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap();
            })),
        });
        // Run the real worker and ensure() path without an asynchronous router:
        // consume every arrival wake explicitly at the exact blocked boundary.
        let supervisor = Supervisor {
            inner: Arc::clone(&inner),
            wake_path: None,
            listener: None,
            router: None,
        };
        ensure(&inner, agent.id.clone());
        at_idle_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(store.agent(&agent.id).unwrap().state, "idle");
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 1);
        assert!(store.open_turn(&agent.id).unwrap().is_none());

        // Commit through another Store after idle committed but before stop.
        // ensure() consumes the wake while the old slot is still active.
        let other = Store::open(dir.path().join("state.db")).unwrap();
        other
            .receive(&agent.id, anyagents_core::Incoming::new("second"))
            .unwrap();
        ensure(&inner, agent.id.clone());
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert!(other.next_undelivered(&agent.id).unwrap().is_some());
        resume_tx.send(()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let delivered = other.delivered_cursor(&agent.id).unwrap() == 2;
            let live = supervisor
                .live(&agent.id)
                .is_some_and(|live| live.process_alive && live.turn_state == TurnState::InTurn);
            if delivered && live {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "committed mail was stranded after idle release"
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(other.agent(&agent.id).unwrap().state, "running");
        assert!(other.next_undelivered(&agent.id).unwrap().is_none());
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        let workers = lock(&inner.workers);
        let slot = workers.get(&agent.id).unwrap();
        assert!(!slot.stop.load(Ordering::SeqCst));
        assert!(!slot.done.load(Ordering::SeqCst));
        drop(workers);
        drop(supervisor);
    }

    #[test]
    fn vendor_activity_on_one_agent_wakes_only_that_agent() {
        use std::sync::atomic::AtomicUsize;
        let clock = Arc::new(SystemClock::new());
        let flags: Vec<Arc<AtomicBool>> =
            (0..8).map(|_| Arc::new(AtomicBool::new(false))).collect();
        let returned = Arc::new(AtomicUsize::new(0));
        let waiters: Vec<_> = flags
            .iter()
            .map(|flag| {
                let clock = Arc::clone(&clock);
                let flag = Arc::clone(flag);
                let returned = Arc::clone(&returned);
                thread::spawn(move || {
                    let flagged = clock.wait(None, &flag);
                    returned.fetch_add(1, Ordering::SeqCst);
                    flagged
                })
            })
            .collect();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while lock(&clock.waiters).len() < flags.len() && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(lock(&clock.waiters).len(), flags.len());
        // The callback a harness's activity watcher calls for agent 0.
        let activity = activity_notify(Arc::clone(&clock) as Arc<dyn Clock>, Arc::clone(&flags[0]));
        activity();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while returned.load(Ordering::SeqCst) < 1 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            returned.load(Ordering::SeqCst),
            1,
            "a poke for one agent must leave every other waiter parked"
        );
        assert_eq!(lock(&clock.waiters).len(), flags.len() - 1);
        for flag in &flags {
            flag.store(true, Ordering::SeqCst);
        }
        clock.poke();
        for waiter in waiters {
            assert!(waiter.join().unwrap(), "each wait ends on its own flag");
        }
        assert!(lock(&clock.waiters).is_empty());
        // A deadline ends a wait without a flag, and says so.
        let flag = Arc::new(AtomicBool::new(false));
        assert!(!clock.wait(Some(clock.now_ms() + 20), &flag));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn inflight_check_reads_the_harness_subtree() {
        use std::process::Command;
        let mut bare = Command::new("sleep").arg("30").spawn().unwrap();
        let mut parent = Command::new("sh")
            .args(["-c", "sleep 30 & wait"])
            .spawn()
            .unwrap();
        assert!(
            proc_children(bare.id()).is_some(),
            "this kernel exposes /proc/<pid>/task/<tid>/children"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !harness_has_inflight_processes(Some(parent.id()), &mut false)
            && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        let mut scanned = false;
        assert!(
            harness_has_inflight_processes(Some(parent.id()), &mut scanned),
            "a running command under the harness is in flight"
        );
        assert!(
            !harness_has_inflight_processes(Some(bare.id()), &mut scanned),
            "a harness with no children has nothing in flight"
        );
        assert!(!harness_has_inflight_processes(None, &mut scanned));
        assert!(
            !scanned,
            "the subtree read must not fall back to a full scan"
        );
        assert_eq!(
            proc_children(u32::MAX - 1),
            Some(Vec::new()),
            "a process that is gone has no children and needs no scan"
        );
        let _ = Command::new("pkill")
            .args(["-P", &parent.id().to_string()])
            .status();
        let _ = parent.kill();
        let _ = bare.kill();
        let _ = parent.wait();
        let _ = bare.wait();
    }

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

#[cfg(test)]
mod persistence_tests {
    use super::*;
    use anyagents_harness::{FakeHarness, HarnessError, OutboundMessage};
    use rusqlite::Connection;

    struct UsageHarness {
        inner: FakeHarness,
        usage: TurnUsage,
    }

    impl Harness for UsageHarness {
        fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
            self.inner.deliver(message)
        }
        fn background(&mut self) -> Result<(), HarnessError> {
            self.inner.background()
        }
        fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
            self.inner.resume(session)
        }
        fn cancel(&mut self) -> Result<(), HarnessError> {
            self.inner.cancel()
        }
        fn health(&self) -> anyagents_harness::HarnessHealth {
            self.inner.health()
        }
        fn turns_served(&self) -> u32 {
            self.inner.turns_served()
        }
        fn served_usage(&self) -> TurnUsage {
            if self.turns_served() > 0 {
                self.usage.clone()
            } else {
                TurnUsage::default()
            }
        }
    }

    fn fixture(usage: TurnUsage) -> (tempfile::TempDir, Session<UsageHarness>, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("worker", "contract"))
            .unwrap();
        let paths = AgentPaths::from_store(&store, &agent);
        let make_harness = move || UsageHarness {
            inner: FakeHarness::new(DeliveryMode::Steer),
            usage: usage.clone(),
        };
        let harness = make_harness();
        let inner = Arc::new(Inner {
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock::new()),
            launch: Arc::new(move |_| Ok(make_harness())),
            extra_env: Vec::new(),
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            workers: Mutex::new(HashMap::new()),
            idle_release_hook: None,
        });
        let delivery = DeliveryLoop::start(
            Store::open(dir.path().join("state.db")).unwrap(),
            agent.id.clone(),
            harness,
        )
        .unwrap();
        let mut session = Session {
            inner,
            id: agent.id.clone(),
            agent,
            paths,
            notify: Arc::new(|| {}),
            stop: Arc::new(AtomicBool::new(false)),
            woken: Arc::new(AtomicBool::new(false)),
            attempt: 0,
            restart_at: None,
            delivery: Some(delivery),
            served: None,
            written: 0,
            turns_recorded: 0,
            pending_recovery: true,
            completed_turn_in_session: false,
            quota_started_at: None,
            pending_switch_started_at: None,
            switch_retry_at: 0,
            process_started_at: None,
            process_profile_rotated_ms: 0,
            last_error: None,
            consecutive_turn_errors: 0,
            idle_review_ms: IDLE_REVIEW_MS,
            process_scan_logged: false,
        };
        store
            .receive(
                &session.id,
                anyagents_core::Incoming::new("finish this work"),
            )
            .unwrap();
        assert!(matches!(
            session.delivery.as_mut().unwrap().run_once().unwrap(),
            RunOutcome::Delivered { .. }
        ));
        assert!(store.open_turn(&session.id).unwrap().is_some());
        let connection = Connection::open(dir.path().join("state.db")).unwrap();
        (dir, session, connection)
    }

    #[test]
    fn failed_recovery_read_remains_pending_and_recovers_after_storage_is_repaired() {
        let (_dir, mut session, sql) = fixture(TurnUsage::default());
        sql.execute_batch(
            "ALTER TABLE turn_recovery RENAME TO saved_recovery;
            CREATE VIEW turn_recovery AS SELECT * FROM unavailable_recovery;",
        )
        .unwrap();
        session.pending_recovery = false;
        session.replace_process();
        assert!(
            session.pending_recovery,
            "replacement must not mistake unknown for no open turn"
        );
        assert!(!session.recover_once());
        assert!(session.pending_recovery);
        assert!(
            session
                .last_error
                .as_deref()
                .unwrap()
                .contains("read open turn for recovery")
        );
        assert!(
            session
                .delivery
                .as_ref()
                .unwrap()
                .harness()
                .inner
                .written_ids()
                .is_empty()
        );
        sql.execute_batch(
            "DROP VIEW turn_recovery; ALTER TABLE saved_recovery RENAME TO turn_recovery;",
        )
        .unwrap();
        assert!(!session.recover_once());
        assert!(!session.pending_recovery);
        assert_eq!(
            session
                .delivery
                .as_ref()
                .unwrap()
                .harness()
                .inner
                .written_ids()
                .len(),
            1
        );
        assert_eq!(
            session
                .inner
                .store
                .open_turn(&session.id)
                .unwrap()
                .unwrap()
                .recovery_count,
            1
        );
    }

    #[test]
    fn failed_recovery_persistence_retries_the_same_delivery_without_duplicate_vendor_work() {
        let (_dir, mut session, sql) = fixture(TurnUsage::default());
        session.replace_process();
        sql.execute_batch(
            "CREATE TRIGGER fail_recovery BEFORE UPDATE OF recovery_count ON turn_recovery
            BEGIN SELECT RAISE(FAIL, 'recovery injected'); END;",
        )
        .unwrap();
        assert!(!session.recover_once());
        assert!(session.pending_recovery);
        assert!(
            session
                .last_error
                .as_deref()
                .unwrap()
                .contains("recovery injected")
        );
        let written = session
            .delivery
            .as_ref()
            .unwrap()
            .harness()
            .inner
            .written_ids()
            .to_vec();
        assert_eq!(written.len(), 1);
        assert_eq!(
            session
                .inner
                .store
                .open_turn(&session.id)
                .unwrap()
                .unwrap()
                .recovery_count,
            0
        );
        sql.execute_batch("DROP TRIGGER fail_recovery;").unwrap();
        assert!(!session.recover_once());
        assert!(!session.pending_recovery);
        assert!(session.last_error.is_none());
        assert_eq!(
            session
                .delivery
                .as_ref()
                .unwrap()
                .harness()
                .inner
                .written_ids(),
            written
        );
        assert_eq!(
            session
                .inner
                .store
                .open_turn(&session.id)
                .unwrap()
                .unwrap()
                .recovery_count,
            1
        );
    }

    #[test]
    fn store_error_in_delivery_backs_off_instead_of_repolling_at_idle_interval() {
        let (_dir, mut session, sql) = fixture(TurnUsage::default());
        // Store errors (locked/malformed) must take the backoff branch:
        // attempt advances (restart_delay_ms >= 200ms) instead of an
        // immediate retry at the fleet-wide 250ms idle poll.
        sql.execute_batch(
            "ALTER TABLE messages RENAME TO saved_messages;
            CREATE VIEW messages AS SELECT * FROM unavailable_messages;",
        )
        .unwrap();
        session.attempt = 0;
        session.deliver_once();
        assert_eq!(
            session.attempt, 1,
            "store error must use wait_backoff, not the idle poll"
        );
        assert!(
            session
                .last_error
                .as_deref()
                .is_some_and(|e| e.starts_with("store:")),
            "store error must be recorded"
        );
    }

    #[test]
    fn store_error_with_dead_process_replaces_child_after_single_backoff() {
        let (_dir, mut session, sql) = fixture(TurnUsage::default());
        sql.execute_batch(
            "ALTER TABLE messages RENAME TO saved_messages;
            CREATE VIEW messages AS SELECT * FROM unavailable_messages;",
        )
        .unwrap();
        session
            .delivery
            .as_mut()
            .unwrap()
            .harness_mut()
            .inner
            .kill();
        assert!(!session.process_alive());
        session.attempt = 0;
        session.deliver_once();
        assert_eq!(
            session.attempt, 1,
            "dead process + store error must back off once before replacement"
        );
        assert!(
            session.process_alive(),
            "dead process must be replaced right after wait_backoff clears"
        );
        assert!(
            session.restart_at.is_none(),
            "replacement must clear pending restart_at"
        );
    }

    #[test]
    fn authoritative_absence_of_an_open_turn_clears_pending_recovery() {
        let (_dir, mut session, _sql) = fixture(TurnUsage::default());
        session
            .inner
            .store
            .note_turn_completed(&session.id)
            .unwrap();
        session.replace_process();
        assert!(session.pending_recovery);
        assert!(!session.recover_once());
        assert!(!session.pending_recovery);
        assert!(
            session
                .delivery
                .as_ref()
                .unwrap()
                .harness()
                .inner
                .written_ids()
                .is_empty()
        );
    }

    #[test]
    fn completion_failure_retains_zero_usage_turn_until_durable_retry_succeeds() {
        assert_completion_retry(TurnUsage::default());
    }

    #[test]
    fn completion_failure_retains_usage_baseline_until_durable_retry_succeeds() {
        assert_completion_retry(TurnUsage {
            input_tokens: 11,
            output_tokens: 7,
            ..TurnUsage::default()
        });
    }

    fn assert_completion_retry(usage: TurnUsage) {
        let (_dir, mut session, sql) = fixture(usage.clone());
        session
            .delivery
            .as_mut()
            .unwrap()
            .harness_mut()
            .inner
            .complete_turn()
            .unwrap();
        sql.execute_batch(
            "CREATE TRIGGER fail_completion BEFORE UPDATE OF completed_seq ON turn_recovery
            BEGIN SELECT RAISE(FAIL, 'completion injected'); END;",
        )
        .unwrap();
        assert!(!session.record_usage());
        assert_eq!(session.turns_recorded, 0);
        assert!(session.served.is_none());
        assert!(!session.completed_turn_in_session);
        assert!(
            session
                .last_error
                .as_deref()
                .unwrap()
                .contains("completion injected")
        );
        assert!(
            session
                .inner
                .store
                .open_turn(&session.id)
                .unwrap()
                .is_some()
        );
        assert!(
            session
                .inner
                .store
                .list_usage(Some(&session.id))
                .unwrap()
                .is_empty()
        );
        session.replace_process();
        assert_eq!(
            session.delivery.as_ref().unwrap().harness().turns_served(),
            1,
            "failed persistence must not discard the old harness baseline"
        );
        sql.execute_batch("DROP TRIGGER fail_completion;").unwrap();
        assert!(session.record_usage());
        assert_eq!(session.turns_recorded, 1);
        assert!(session.completed_turn_in_session);
        assert!(session.last_error.is_none());
        assert!(
            session
                .inner
                .store
                .open_turn(&session.id)
                .unwrap()
                .is_none()
        );
        assert!(session.record_usage());
        let rows = session.inner.store.list_usage(Some(&session.id)).unwrap();
        assert_eq!(rows.len(), usize::from(usage.input_tokens > 0));
        if let Some(row) = rows.first() {
            assert_eq!(row.input_tokens, 11);
            assert_eq!(row.output_tokens, 7);
        }
    }

    #[test]
    fn boot_refuses_to_spawn_or_recreate_dirs_when_agent_is_archived_or_locked() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("worker", "opencode"))
            .unwrap();
        let agent = store.set_agent_state(&agent.id, "running").unwrap();
        let launches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let launches_counter = Arc::clone(&launches);
        let inner = Arc::new(Inner::<FakeHarness> {
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock::new()),
            launch: Arc::new(move |_| {
                launches_counter.fetch_add(1, Ordering::SeqCst);
                Err("unexpected launch".into())
            }),
            extra_env: Vec::new(),
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            workers: Mutex::new(HashMap::new()),
            idle_release_hook: None,
        });
        let paths = AgentPaths::from_store(&store, &agent);
        let mut session = Session {
            inner,
            id: agent.id.clone(),
            agent,
            paths,
            notify: Arc::new(|| {}),
            stop: Arc::new(AtomicBool::new(false)),
            woken: Arc::new(AtomicBool::new(false)),
            attempt: 0,
            restart_at: None,
            delivery: None,
            served: None,
            written: 0,
            turns_recorded: 0,
            pending_recovery: false,
            completed_turn_in_session: false,
            quota_started_at: None,
            pending_switch_started_at: None,
            process_started_at: None,
            process_profile_rotated_ms: 0,
            last_error: None,
            consecutive_turn_errors: 0,
            idle_review_ms: IDLE_REVIEW_MS,
            process_scan_logged: false,
        };

        let held = store.try_lock_agent(&session.id).unwrap();
        assert!(matches!(session.boot(), Boot::Busy));
        assert_eq!(launches.load(Ordering::SeqCst), 0);
        drop(held);

        store.set_agent_state(&session.id, "archived").unwrap();
        let _ = fs::remove_dir_all(&session.paths.work_dir);
        assert!(matches!(session.boot(), Boot::Stopped));
        assert_eq!(launches.load(Ordering::SeqCst), 0);
        assert!(!session.paths.work_dir.exists());
    }
}
