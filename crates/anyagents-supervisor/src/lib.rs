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
    Agent, AgentId, AgentLock, BillingMode, NewUsage, Store, Wake, agent_config_dir,
    agent_work_dir, utc_day,
};
use anyagents_harness::{DeliverOutcome, Delivery, Harness, HarnessHealth, TurnState, TurnUsage};

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

/// Operator markers in an agent's config directory: the time its provider
/// profile was replaced, and the time its quota hold was cleared.
const PROFILE_ROTATED: &str = ".anyagents-profile-rotated-ms";
const QUOTA_CLEARED: &str = ".anyagents-quota-cleared-ms";

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
    /// Set `woken` and wake the waiter that waits on it. The default sets the
    /// flag and calls [`Clock::poke`].
    fn wake(&self, woken: &Arc<AtomicBool>) {
        woken.store(true, Ordering::SeqCst);
        self.poke();
    }
}

/// Wall clock. Each waiter parks its own thread, and [`Clock::wake`] unparks
/// only the waiter on that flag: one agent's mail or vendor activity never
/// wakes, or even visits, the rest of the fleet.
pub struct SystemClock {
    /// Parked threads by the address of the flag they wait on.
    waiters: Mutex<HashMap<usize, Vec<Waiter>>>,
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
            waiters: Mutex::new(HashMap::new()),
            next_waiter: AtomicU64::new(0),
        }
    }
}

fn flag_key(woken: &Arc<AtomicBool>) -> usize {
    Arc::as_ptr(woken) as usize
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
        let key = flag_key(woken);
        lock(&self.waiters).entry(key).or_default().push(Waiter {
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
        if let Some(parked) = waiters.get_mut(&key) {
            parked.retain(|waiter| waiter.id != id);
            if parked.is_empty() {
                waiters.remove(&key);
            }
        }
        flagged
    }

    fn poke(&self) {
        for waiter in lock(&self.waiters).values().flatten() {
            if waiter.woken.load(Ordering::SeqCst) {
                waiter.thread.unpark();
            }
        }
    }

    fn wake(&self, woken: &Arc<AtomicBool>) {
        woken.store(true, Ordering::SeqCst);
        // A waiter registers before it reads the flag again, so a wake that
        // finds nobody here is seen by the flag read.
        if let Some(parked) = lock(&self.waiters).get(&flag_key(woken)) {
            for waiter in parked {
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
    /// A copy of the agent's process lock for a process host that outlives
    /// this kernel (a keeper) to hold for the life of the process. `None`
    /// when processes are hosted in the kernel.
    pub lock: Option<Arc<AgentLock>>,
}

pub type Launch<H> = Arc<dyn Fn(LaunchRequest) -> Result<H, String> + Send + Sync>;

/// Who holds harness processes, and whether they outlive this kernel.
///
/// With keepers (`docs/adr/0007-process-architecture.md`) a harness process
/// and the adapter that speaks to it live in their own process. A kernel that
/// stops leaves them running ([`Supervisor::retain_processes`]); the next one
/// takes each over ([`Custody::attach`]) and resumes delivery from the
/// mailbox cursor, with the adapter's memory of what it was given intact.
pub trait Custody<H>: Send + Sync {
    /// Launched processes outlive this kernel: a launch hands the host a copy
    /// of the agent's process lock, and a stop may leave them running.
    fn outlives_kernel(&self) -> bool;
    /// Agents whose processes a previous kernel left running.
    fn retained(&self) -> Vec<AgentId>;
    /// Take over the process a previous kernel left for `agent`.
    fn attach(&self, agent: &AgentId) -> Result<Option<Attached<H>>, String>;
    /// End the process a previous kernel left for `agent` without taking it
    /// over: the agent is no longer running.
    fn release(&self, agent: &AgentId);
    /// Leave `harness` running for the next kernel.
    fn retain(&self, harness: H);
    /// Keep `note` with the process for the next kernel that attaches.
    fn note(&self, harness: &H, note: &str);
    /// Another kernel took the process over: this one must stand down for
    /// the agent, neither ending the process nor starting another.
    fn superseded(&self, _harness: &H) -> bool {
        false
    }
    /// What `harness` answers now comes from its adapter: one in this
    /// process always does; a kept one only while its keeper's link is open
    /// and answering (a closed or wedged link answers with what it last
    /// knew). Asked after the answers it vouches for.
    fn answers_live(&self, _harness: &H) -> bool {
        true
    }
}

/// A process a previous kernel left, taken over.
pub struct Attached<H> {
    pub harness: H,
    /// The copy of the agent's process lock the host held.
    pub lock: AgentLock,
    /// What the process was launched for, which the agent's configuration
    /// may no longer name: a difference is a switch, made at its boundary.
    pub harness_name: String,
    pub model: Option<String>,
    pub started_at_ms: Option<i64>,
    /// What the last kernel kept with the process ([`Custody::note`]).
    pub note: Option<String>,
    /// Older processes of the same agent left beside it (a kernel that died
    /// in the middle of a replacement): retired once what they proved is
    /// committed.
    pub older: Vec<H>,
}

/// Harness processes hosted in the kernel: they end with it.
pub struct InProcess;

impl<H> Custody<H> for InProcess {
    fn outlives_kernel(&self) -> bool {
        false
    }

    fn retained(&self) -> Vec<AgentId> {
        Vec::new()
    }

    fn attach(&self, _agent: &AgentId) -> Result<Option<Attached<H>>, String> {
        Ok(None)
    }

    fn release(&self, _agent: &AgentId) {}

    fn retain(&self, harness: H) {
        drop(harness);
    }

    fn note(&self, _harness: &H, _note: &str) {}
}

/// What a launch answers when the kernel stops while the harness is still
/// starting: the process holding it (a keeper) goes on starting, holding a
/// copy of the agent's lock, for the next kernel. The worker leaves the lock
/// locked: unlocking any copy would unlock the keeper's.
pub const LAUNCH_ABANDONED: &str =
    "the kernel is stopping; the harness's start goes on for the next kernel";

/// What a kernel keeps with a process for the kernel that takes it over:
/// the usage it already recorded from it, so none is recorded twice, the
/// operator profile marker it was launched under, and a `[Recovery]` note it
/// still owes the process's agent.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct ProcessNote {
    served: Option<TurnUsage>,
    turns_recorded: u32,
    written: u32,
    profile_rotated_ms: i64,
    completed_turn: bool,
    #[serde(default)]
    pending_recovery: bool,
}

/// Whether served totals grew past `before`, field by field: a token count
/// above its last reading, or a cost above the last one (a positive cost
/// where there was none). Equal totals are no news, whatever they carry.
fn usage_advanced(served: &TurnUsage, before: &TurnUsage) -> bool {
    let cost = match (served.reported_cost_micros, before.reported_cost_micros) {
        (Some(now), Some(then)) => now > then,
        (Some(now), None) => now > 0,
        _ => false,
    };
    served.input_tokens > before.input_tokens
        || served.output_tokens > before.output_tokens
        || served.cache_read_tokens > before.cache_read_tokens
        || served.cache_write_tokens > before.cache_write_tokens
        || cost
}

/// How soon a worker looks again after it handed lines over, or while a turn
/// is open, after it last did work or was woken. One store read, so `flush`
/// and a restarted process never leave a line waiting for an arrival.
const IDLE_REVIEW_MS: i64 = 250;
/// Ceiling of the review interval while a turn is open. Each review that
/// finds nothing to do doubles the interval up to this bound; mail, harness
/// transitions and agent wakes end the wait at once and reset it. It covers
/// an adapter whose turn end is only learned by asking (OpenCode's usage).
const IDLE_REVIEW_MAX_MS: i64 = 15_000;
/// How long a worker whose harness is idle sleeps without any event. Mail,
/// harness transitions, agent changes and the exit of a command that held
/// the agent awake are events; this only bounds what a lost wake hint from
/// another process, or an operator marker written without a wake, can delay.
const IDLE_SAFETY_NET_MS: i64 = 60_000;

/// Supervisor view of one agent. Ages stay on the mailbox; this is the process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveState {
    pub process_alive: bool,
    pub turn_state: TurnState,
    /// What the agent's harness process guarantees for mail, as it announced
    /// itself. `None` while no process has been started.
    pub delivery: Option<Delivery>,
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

struct Slot {
    stop: Arc<AtomicBool>,
    woken: Arc<AtomicBool>,
    /// Set before `woken` when the agent row changed: the worker reads the
    /// row again instead of on every pass.
    agent_changed: Arc<AtomicBool>,
    done: Arc<Finished>,
    thread: Option<JoinHandle<()>>,
}

/// Set once when a worker thread has returned; a stopper waits on it instead
/// of spinning.
#[derive(Default)]
struct Finished {
    flag: Mutex<bool>,
    changed: Condvar,
}

impl Finished {
    fn set(&self) {
        *lock(&self.flag) = true;
        self.changed.notify_all();
    }

    fn is_set(&self) -> bool {
        *lock(&self.flag)
    }

    /// Wait up to `timeout` for the worker to return.
    fn wait(&self, timeout: Duration) -> bool {
        let guard = lock(&self.flag);
        let (guard, _) = self
            .changed
            .wait_timeout_while(guard, timeout, |done| !*done)
            .unwrap_or_else(|err| err.into_inner());
        *guard
    }
}

#[cfg(test)]
type IdleReleaseHook = Arc<dyn Fn(&AgentId) + Send + Sync>;

struct Inner<H> {
    store: Arc<Store>,
    clock: Arc<dyn Clock>,
    launch: Launch<H>,
    custody: Arc<dyn Custody<H>>,
    extra_env: Vec<(String, String)>,
    board: HealthBoard,
    stop: AtomicBool,
    /// Set by [`Supervisor::retain_processes`]: stopping workers leave their
    /// processes to the next kernel instead of ending them.
    retain: AtomicBool,
    workers: Mutex<HashMap<AgentId, Slot>>,
    #[cfg(test)]
    idle_release_hook: Option<IdleReleaseHook>,
}

/// Keeps one harness process alive for every agent in state `running`.
pub struct Supervisor<H: Harness + 'static> {
    inner: Arc<Inner<H>>,
    wake_path: Option<PathBuf>,
    listener: Option<WakeListener>,
    router: Option<JoinHandle<()>>,
}

/// The thread that turns other processes' wake datagrams into store wakes,
/// and a handle on its socket so stopping it needs no receive timeout.
struct WakeListener {
    socket: UnixDatagram,
    thread: JoinHandle<()>,
}

impl<H: Harness + 'static> Supervisor<H> {
    /// Start with harness processes hosted in this kernel.
    pub fn start(
        store: Arc<Store>,
        clock: Arc<dyn Clock>,
        launch: Launch<H>,
        extra_env: Vec<(String, String)>,
    ) -> Result<Self, String> {
        Self::start_with_custody(store, clock, launch, extra_env, Arc::new(InProcess))
    }

    /// Start with `custody` holding harness processes. Processes a previous
    /// kernel left are taken over by the workers of running agents and ended
    /// for every other agent.
    pub fn start_with_custody(
        store: Arc<Store>,
        clock: Arc<dyn Clock>,
        launch: Launch<H>,
        extra_env: Vec<(String, String)>,
        custody: Arc<dyn Custody<H>>,
    ) -> Result<Self, String> {
        let inner = Arc::new(Inner {
            store: Arc::clone(&store),
            clock,
            launch,
            custody,
            extra_env,
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            retain: AtomicBool::new(false),
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

    /// Stop, leaving every harness process to the next kernel when the
    /// custody lets processes outlive this one. A worker in the middle of a
    /// call finishes it first; nothing is ended.
    pub fn retain_processes(self) {
        self.inner.retain.store(true, Ordering::SeqCst);
        drop(self);
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
        if let Some(listener) = self.listener.take() {
            // Either ends the blocking receive: the shutdown, or the byte if
            // the socket still takes datagrams.
            let _ = listener.socket.shutdown(std::net::Shutdown::Read);
            if let Some(path) = &self.wake_path {
                let _ = UnixDatagram::unbound().and_then(|sock| sock.send_to(b"x", path));
            }
            let _ = listener.thread.join();
        }
        if let Some(router) = self.router.take() {
            let _ = router.join();
        }
        for thread in threads {
            let _ = thread.join();
        }
    }
}

fn bind_wake<H>(inner: &Arc<Inner<H>>) -> Result<(Option<PathBuf>, Option<WakeListener>), String>
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
    let handle = socket.try_clone().map_err(|err| err.to_string())?;
    let listen_inner = Arc::clone(inner);
    let thread = thread::Builder::new()
        .name("anyagents-wake".into())
        .spawn(move || listen_wake(socket, listen_inner))
        .map_err(|err| err.to_string())?;
    Ok((
        Some(path),
        Some(WakeListener {
            socket: handle,
            thread,
        }),
    ))
}

/// A blocking receive: the thread sleeps until a datagram arrives or the
/// supervisor shuts the socket down.
fn listen_wake<H: Harness>(socket: UnixDatagram, inner: Arc<Inner<H>>) {
    let mut buf = [0u8; 256];
    while !inner.stop.load(Ordering::SeqCst) {
        let count = match socket.recv(&mut buf) {
            Ok(count) => count,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
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
    // A process a previous kernel left is taken over by its running agent's
    // worker, which releases it to idle through the same checks as any live
    // process (its turn, its commands still running). Every other one is
    // ended: its agent no longer runs.
    let retained: std::collections::HashSet<AgentId> =
        inner.custody.retained().into_iter().collect();
    if let Ok(agents) = inner.store.list_live_agents() {
        let mut running = std::collections::HashSet::new();
        for agent in agents {
            if agent.state == "running" {
                running.insert(agent.id.clone());
                if !retained.contains(&agent.id)
                    && inner.store.complete_idle_agent(&agent.id).unwrap_or(false)
                {
                    continue;
                }
                ensure(&inner, agent.id);
            }
        }
        for id in retained.difference(&running) {
            inner.custody.release(id);
        }
    }
    while let Ok(wake) = events.recv() {
        if inner.stop.load(Ordering::SeqCst) {
            break;
        }
        match wake {
            Wake::Mail { agent_id } => {
                // Mail never changes the agent row: a live worker only needs
                // waking, and reads its own mailbox. A row change that stops
                // it arrives as `Wake::Agent`.
                if wake_live_worker(&inner, &agent_id, false) {
                    continue;
                }
                if is_running(&inner.store, &agent_id) {
                    ensure(&inner, agent_id);
                } else {
                    stop_slot(&inner, &agent_id);
                }
            }
            Wake::Agent { agent_id } => {
                let row = inner.store.agent(&agent_id).ok();
                // A parent is released only once its children are settled, so a
                // child's row change is the event that can unblock that release.
                let parent = row.as_ref().and_then(|agent| agent.parent_id.clone());
                if row.is_some_and(|agent| agent.state == "running") {
                    if !wake_live_worker(&inner, &agent_id, true) {
                        ensure(&inner, agent_id);
                    }
                } else {
                    stop_slot(&inner, &agent_id);
                }
                if let Some(parent) = parent {
                    wake_live_worker(&inner, &parent, false);
                }
            }
        }
    }
}

/// Wake the agent's worker when one is running and not stopping, marking the
/// agent row changed when `agent_changed`. `false` when there is none.
fn wake_live_worker<H: Harness + 'static>(
    inner: &Arc<Inner<H>>,
    id: &AgentId,
    agent_changed: bool,
) -> bool {
    let workers = lock(&inner.workers);
    let Some(slot) = workers
        .get(id)
        .filter(|slot| !slot.done.is_set() && !slot.stop.load(Ordering::SeqCst))
    else {
        return false;
    };
    if agent_changed {
        slot.agent_changed.store(true, Ordering::SeqCst);
    }
    inner.clock.wake(&slot.woken);
    true
}

fn is_running(store: &Store, id: &AgentId) -> bool {
    store.agent(id).is_ok_and(|agent| agent.state == "running")
}

fn stop_slot<H: Harness + 'static>(inner: &Arc<Inner<H>>, id: &AgentId) {
    let done = {
        let mut workers = lock(&inner.workers);
        if let Some(slot) = workers.get_mut(id) {
            slot.stop.store(true, Ordering::SeqCst);
            inner.clock.wake(&slot.woken);
            Some(Arc::clone(&slot.done))
        } else {
            None
        }
    };
    if inner.store.agent(id).is_ok() {
        inner.board.publish(
            id,
            LiveState {
                process_alive: false,
                turn_state: TurnState::Idle,
                delivery: None,
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
        done.wait(Duration::from_millis(250));
        let finished_thread = {
            let mut workers = lock(&inner.workers);
            if let Some(slot) = workers.get_mut(id)
                && slot.done.is_set()
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
            let finished = slot.done.is_set();
            let stopping = slot.stop.load(Ordering::SeqCst);
            if !finished && !stopping {
                inner.clock.wake(&slot.woken);
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
    let agent_changed = Arc::new(AtomicBool::new(true));
    let done = Arc::new(Finished::default());
    let run_inner = Arc::clone(inner);
    let run_id = id.clone();
    let signals = Signals {
        stop: Arc::clone(&stop),
        woken: Arc::clone(&woken),
        agent_changed: Arc::clone(&agent_changed),
    };
    let run_done = Arc::clone(&done);
    let thread = thread::Builder::new()
        .name("anyagents-agent".into())
        .spawn(move || {
            let _ = run_agent(run_inner, run_id, signals);
            run_done.set();
        });
    let Ok(thread) = thread else {
        return;
    };
    workers.insert(
        id,
        Slot {
            stop,
            woken,
            agent_changed,
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

/// The flags a worker shares with the router.
struct Signals {
    stop: Arc<AtomicBool>,
    woken: Arc<AtomicBool>,
    agent_changed: Arc<AtomicBool>,
}

impl Signals {
    #[cfg(test)]
    fn detached() -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            woken: Arc::new(AtomicBool::new(false)),
            agent_changed: Arc::new(AtomicBool::new(true)),
        }
    }
}

fn run_agent<H: Harness + 'static>(inner: Arc<Inner<H>>, id: AgentId, signals: Signals) -> bool {
    let Ok(agent) = inner.store.agent(&id) else {
        return false;
    };
    let notify = activity_notify(Arc::clone(&inner.clock), Arc::clone(&signals.woken));
    Session::new(inner, agent, notify, signals).run()
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
    Arc::new(move || clock.wake(&woken))
}

struct Session<H: Harness> {
    inner: Arc<Inner<H>>,
    id: AgentId,
    agent: Agent,
    paths: AgentPaths,
    notify: Arc<dyn Fn() + Send + Sync>,
    stop: Arc<AtomicBool>,
    woken: Arc<AtomicBool>,
    /// Set by the router when the agent row changed.
    agent_changed: Arc<AtomicBool>,
    /// The agent row as last read: what the agent is configured to run.
    configured: Option<Agent>,
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
    /// with a `[Recovery]` note, ahead of any new mail.
    pending_recovery: bool,
    /// The recovery note is not yet written to the vendor: new mail, which is
    /// younger than the interrupted turn's, waits behind it.
    recovery_blocks_mail: bool,
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
    /// Exit watch over the commands that kept an idle harness from release.
    exit_watch: Option<ExitWatch>,
    /// Those commands cannot be watched (no pidfd): review them on the
    /// backoff timer instead.
    review_children: bool,
    /// A copy of the process lock for each process host this worker starts,
    /// when hosts outlive the kernel.
    host_lock: Option<Arc<AgentLock>>,
    /// The process came from a previous kernel, with whether a `[Recovery]`
    /// note is owed: one that kernel owed, or a turn left open in the store
    /// while the process sits idle.
    adopted: Option<bool>,
    /// The note last kept with the process.
    kept_note: Option<ProcessNote>,
}

impl<H: Harness + 'static> Session<H> {
    fn new(
        inner: Arc<Inner<H>>,
        agent: Agent,
        notify: Arc<dyn Fn() + Send + Sync>,
        signals: Signals,
    ) -> Self {
        let paths = AgentPaths::from_store(&inner.store, &agent);
        Self {
            inner,
            id: agent.id.clone(),
            agent,
            paths,
            notify,
            stop: signals.stop,
            woken: signals.woken,
            agent_changed: signals.agent_changed,
            configured: None,
            attempt: 0,
            restart_at: None,
            delivery: None,
            served: None,
            written: 0,
            turns_recorded: 0,
            pending_recovery: false,
            recovery_blocks_mail: false,
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
            exit_watch: None,
            review_children: false,
            host_lock: None,
            adopted: None,
            kept_note: None,
        }
    }

    /// One read of the harness's state. A pass decides on one such snapshot,
    /// and reads again only after it changed the harness.
    fn harness_health(&self) -> Option<HarnessHealth> {
        self.delivery
            .as_ref()
            .map(|delivery| delivery.harness().health())
    }

    fn run(&mut self) -> bool {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                self.record_usage();
                self.end_delivery();
                self.publish();
                return false;
            }
            if self.delivery.is_none() {
                match self.boot() {
                    Boot::Ready(delivery) => {
                        delivery.harness().watch_activity(Arc::clone(&self.notify));
                        self.exit_watch = None;
                        self.restart_at = None;
                        // A fresh process may follow an interrupted turn:
                        // unknown until recover_once reads durable state. A
                        // process taken over from a previous kernel never
                        // stopped: its turn goes on, and it is owed a
                        // recovery note only as decided when it was adopted.
                        self.pending_recovery = self.adopted.take().unwrap_or(true);
                        self.recovery_blocks_mail = false;
                        self.kept_note = None;
                        self.delivery = Some(*delivery);
                        self.sync_note();
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
            let Some(health) = self.harness_health() else {
                continue;
            };
            if self
                .delivery
                .as_ref()
                .is_some_and(|delivery| self.inner.custody.superseded(delivery.harness()))
            {
                self.stand_down();
                return false;
            }
            self.sync_note();
            // A changed configuration supersedes the old process's fault,
            // quota hold or restart delay before any further dispatch.
            if self.try_apply_pending_switch(&health) {
                continue;
            }
            if self.quota_waiting(&health) {
                continue;
            }
            if !health.process_alive {
                self.commit_proven();
                self.record_usage_with(&health);
                if !self.wait_backoff() {
                    self.end_delivery();
                    self.restart_at = None;
                    self.publish();
                    return false;
                }
                self.replace_process();
                continue;
            }
            if self.deliver_once() {
                self.stop_delivery();
                self.restart_at = None;
                self.publish();
                return true;
            }
        }
    }

    /// The worker stops: its process ends with it, or, while the kernel
    /// stops with [`Supervisor::retain_processes`], is left to the next one.
    fn end_delivery(&mut self) {
        if !self.inner.retain.load(Ordering::SeqCst) {
            self.stop_delivery();
            return;
        }
        let host_lock = self.host_lock.take();
        if let Some(delivery) = self.delivery.take() {
            // The host keeps holding the agent's lock after this process.
            if let Some(lock) = &host_lock {
                lock.retain();
            }
            self.inner.custody.retain(delivery.into_harness());
        }
    }

    /// End the process and release the agent's lock.
    fn stop_delivery(&mut self) {
        self.delivery = None;
        self.host_lock = None;
    }

    /// Another kernel took the process over. Let go of it without ending it,
    /// and without releasing the lock it and that kernel hold, and stop: no
    /// second process is started beside it.
    fn stand_down(&mut self) {
        let host_lock = self.host_lock.take();
        if let Some(delivery) = self.delivery.take() {
            if let Some(lock) = &host_lock {
                lock.retain();
            }
            self.inner.custody.retain(delivery.into_harness());
        }
        self.restart_at = None;
        self.last_error = Some("another server took this agent's harness over".into());
        self.publish();
    }

    /// Keep this worker's accounting of the process with it, for a kernel
    /// that takes the process over after this one, whenever it changed.
    fn sync_note(&mut self) {
        if !self.inner.custody.outlives_kernel() {
            return;
        }
        let Some(delivery) = self.delivery.as_ref() else {
            return;
        };
        let note = ProcessNote {
            served: self.served.clone(),
            turns_recorded: self.turns_recorded,
            written: self.written,
            profile_rotated_ms: self.process_profile_rotated_ms,
            completed_turn: self.completed_turn_in_session,
            pending_recovery: self.pending_recovery,
        };
        if self.kept_note.as_ref() == Some(&note) {
            return;
        }
        if let Ok(text) = serde_json::to_string(&note) {
            self.inner.custody.note(delivery.harness(), &text);
            self.kept_note = Some(note);
        }
    }

    /// Take over a process a previous kernel left, under the agent lock copy
    /// its host held, with that kernel's accounting of it.
    fn adopt(&mut self, store: Store, attached: Attached<H>) -> Boot<H> {
        let Attached {
            harness,
            lock,
            harness_name,
            model,
            started_at_ms,
            note,
            older,
        } = attached;
        let fresh = match store.agent(&self.id) {
            Ok(fresh) if fresh.state == "running" => fresh,
            Ok(_) | Err(anyagents_core::Error::AgentNotFound(_)) => {
                // The processes end before their lock is released: never the
                // other way round, which would leave one running unlocked.
                drop(harness);
                drop(older);
                drop(lock);
                return Boot::Stopped;
            }
            Err(_) => {
                // Not this kernel's to end on a read failure: leave them.
                lock.retain();
                self.inner.custody.retain(harness);
                for other in older {
                    self.inner.custody.retain(other);
                }
                return Boot::Retry;
            }
        };
        let host_lock = match lock.share() {
            Ok(copy) => Arc::new(copy),
            Err(_) => {
                lock.retain();
                self.inner.custody.retain(harness);
                for other in older {
                    self.inner.custody.retain(other);
                }
                return Boot::Retry;
            }
        };
        self.paths = AgentPaths::from_store(&self.inner.store, &fresh);
        self.configured = Some(fresh.clone());
        // The process serves what it was launched for; a configuration
        // changed while no kernel ran is a switch at its next boundary.
        self.agent = Agent {
            harness: harness_name,
            model,
            ..fresh
        };
        let note: ProcessNote = note
            .as_deref()
            .and_then(|text| serde_json::from_str(text).ok())
            .unwrap_or_else(|| ProcessNote {
                profile_rotated_ms: self.marker_ms(PROFILE_ROTATED).unwrap_or(0),
                ..ProcessNote::default()
            });
        let mut delivery = DeliveryLoop::adopted(store, lock, self.id.clone(), harness);
        // An older process an interrupted replacement left is retired first,
        // as a replacement retires one: what it proved is committed. Its
        // turn was interrupted, and the newest process never saw that mail:
        // a recovery is owed, as after any replacement.
        let replaced = !older.is_empty();
        for other in older {
            if let Err(err) = delivery.retire_other(other) {
                self.last_error = Some(format!("commit proven mail: {err}"));
            }
        }
        // A recovery the last kernel owed, or a turn the store holds open
        // while the process sits idle and served nothing since (it was lost
        // with no kernel to see it), is owed still. A turn the process
        // finished while no kernel ran (it served a turn, or usage, past what
        // the note recorded) is completed now instead, so a routine restart
        // sends no `[Recovery]` note; its usage is recorded on the next pass.
        // Only live answers say a turn finished: a served turn the adapter
        // itself answered (its keeper's link open), or usage that grew while
        // the process lives. A keeper that is gone, before or after they are
        // read, reads as idle with nothing new, and is owed its recovery.
        let harness = delivery.harness();
        let health = harness.health();
        let turns = harness.turns_served();
        let usage = harness.last_usage();
        let answered = self.inner.custody.answers_live(harness);
        let live = health.process_alive && harness.health().process_alive;
        let idle = health.turn_state == TurnState::Idle && health.sign_in_fault.is_none();
        let finished_detached = !replaced
            && !note.pending_recovery
            && idle
            && ((answered && turns > note.turns_recorded)
                || (live && usage_advanced(&usage, &note.served.clone().unwrap_or_default())));
        let store = delivery.store();
        let completed = finished_detached && store.note_turn_completed(&self.id).is_ok();
        let idle_with_open_turn = health.turn_state == TurnState::Idle
            && !completed
            && store.has_open_turn(&self.id).unwrap_or(false);
        self.served = note.served;
        self.turns_recorded = note.turns_recorded;
        self.written = note.written;
        self.process_profile_rotated_ms = note.profile_rotated_ms;
        self.completed_turn_in_session = note.completed_turn;
        self.process_started_at = Some(started_at_ms.unwrap_or_else(|| self.inner.clock.now_ms()));
        self.host_lock = Some(host_lock);
        self.adopted = Some(replaced || note.pending_recovery || idle_with_open_turn);
        Boot::Ready(Box::new(delivery))
    }

    fn boot(&mut self) -> Boot<H> {
        if self.stop.load(Ordering::SeqCst) {
            return Boot::Stopped;
        }
        let store = match Store::open(self.inner.store.data_dir().join("state.db")) {
            Ok(store) => store,
            Err(_) => return Boot::Retry,
        };
        // A process a previous kernel left holds the agent's lock: take it
        // over instead of starting another.
        match self.inner.custody.attach(&self.id) {
            Ok(Some(attached)) => return self.adopt(store, attached),
            Ok(None) => {}
            Err(_) => return Boot::Retry,
        }
        // Acquire the per-agent process lock BEFORE launching the child or
        // creating workspace/scratch directories so concurrent archive/delete
        // reaping under this lock cannot race an in-flight spawn.
        let lock = match store.try_lock_agent(&self.id) {
            Ok(lock) => lock,
            Err(anyagents_core::Error::AgentBusy) => return Boot::Busy,
            Err(_) => return Boot::Retry,
        };
        self.host_lock = if self.inner.custody.outlives_kernel() {
            match lock.share() {
                Ok(copy) => Some(Arc::new(copy)),
                Err(_) => return Boot::Retry,
            }
        } else {
            None
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
            Err(error) if error == LAUNCH_ABANDONED => {
                // The keeper still starting holds this lock on: never unlock.
                lock.retain();
                return Boot::Stopped;
            }
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
            self.configured = Some(fresh.clone());
            self.agent = fresh;
        }
        let _ = fs::create_dir_all(&self.paths.config_dir);
        let _ = fs::create_dir_all(&self.paths.managed_work);
        let _ = fs::create_dir_all(&self.paths.work_dir);
        let rotated_ms = self.marker_ms(PROFILE_ROTATED).unwrap_or(0);
        let res = (self.inner.launch)(LaunchRequest {
            agent: self.agent.clone(),
            data_dir: self.inner.store.data_dir().to_path_buf(),
            config_dir: self.paths.config_dir.clone(),
            work_dir: self.paths.work_dir.clone(),
            extra_env: self.inner.extra_env.clone(),
            lock: self.host_lock.clone(),
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
                if err != LAUNCH_ABANDONED {
                    self.last_error = Some(err.clone());
                }
                self.agent = serving;
            }
        }
        res
    }

    fn process_alive(&self) -> bool {
        self.harness_health()
            .is_some_and(|health| health.process_alive)
    }

    /// Publish the state this worker sleeps in, then wait for `deadline` or a
    /// wake. `true` when a wake ended the wait.
    fn sleep(&mut self, deadline: Option<i64>) -> bool {
        self.sync_note();
        self.publish();
        self.inner.clock.wait(deadline, &self.woken)
    }

    /// The time an operator marker in the agent's config directory records.
    fn marker_ms(&self, name: &str) -> Option<i64> {
        fs::read_to_string(self.paths.config_dir.join(name))
            .ok()
            .and_then(|text| text.trim().parse::<i64>().ok())
    }

    /// The agent row: what the agent is configured to run. It is read again
    /// only after the router marked it changed (`Wake::Agent`), at a process
    /// launch, or when the idle safety net passed; `None` while it cannot be
    /// read.
    fn configured(&mut self) -> Option<&Agent> {
        if self.agent_changed.swap(false, Ordering::SeqCst) || self.configured.is_none() {
            self.configured = self.inner.store.agent(&self.id).ok();
            if self.configured.is_none() {
                self.agent_changed.store(true, Ordering::SeqCst);
            }
        }
        self.configured.as_ref()
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
                    delivery: Some(health.delivery),
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
            None => LiveState {
                process_alive: false,
                turn_state: TurnState::Idle,
                delivery: None,
                quota_reset_at_ms: None,
                restart_at_ms: self.restart_at,
                pid: None,
                live_harness: None,
                live_model: None,
                process_started_at_ms: None,
                last_error: self.last_error.clone(),
            },
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
            let rotated = self
                .marker_ms(PROFILE_ROTATED)
                .is_some_and(|ms| ms >= started_at);
            let cleared = self
                .marker_ms(QUOTA_CLEARED)
                .is_some_and(|ms| ms >= started_at);
            if rotated || cleared || self.runtime_switch_pending() == Some(true) {
                self.attempt = 0;
                break;
            }
            self.sleep(Some(deadline));
        }
        self.restart_at = None;
        !self.stop.load(Ordering::SeqCst)
    }

    fn profile_rotation_pending(&self) -> bool {
        self.marker_ms(PROFILE_ROTATED)
            .is_some_and(|ms| ms > self.process_profile_rotated_ms)
    }

    fn profile_rotation_waits_for_turn(&self, health: &HarnessHealth) -> bool {
        health.process_alive
            && health.turn_state == TurnState::InTurn
            && self.profile_rotation_pending()
    }

    /// The harness or model this agent is configured for differs from the one
    /// its process runs. `None` while the agent row cannot be read.
    fn runtime_switch_pending(&mut self) -> Option<bool> {
        let (harness, model) = (self.agent.harness.clone(), self.agent.model.clone());
        let configured = self.configured()?;
        Some(configured.harness != harness || configured.model != model)
    }

    /// Returns whether a new process took over.
    fn replace_process(&mut self) -> bool {
        // Every replacement path (including quota recovery) must preserve an
        // active child when only a profile refresh is pending. A dead child can
        // still be recovered immediately, and an explicit harness or model
        // switch has already had its bounded drain.
        if self
            .harness_health()
            .is_some_and(|health| self.profile_rotation_waits_for_turn(&health))
            && self.runtime_switch_pending() != Some(true)
        {
            return false;
        }
        if self.delivery.is_none() {
            self.restart_at = None;
            return false;
        }
        self.commit_proven();
        // Flush any usage the exited process finished serving before swapping it.
        if !self.record_usage() {
            return false;
        }
        // A replacement on the same harness resumes the same vendor session,
        // so the old process is stopped and read to the end first: what it
        // proved in its last moments is committed, its transcript or journal
        // is no longer written while the new one reads it, and it releases
        // the session for the new one to take. A launch that then fails
        // leaves a dead process, which the dead-process backoff replaces. A
        // switch to another harness starts another session: the old process
        // keeps serving until the new one is up.
        let running = self.agent.harness.clone();
        let same_session = self
            .configured()
            .is_none_or(|configured| configured.harness == running);
        if same_session && let Some(delivery) = self.delivery.as_mut() {
            if let Err(err) = delivery.retire_harness() {
                self.last_error = Some(format!("commit proven mail: {err}"));
            }
            self.publish();
        }
        match self.launch_harness() {
            Ok(mut harness) => {
                harness.watch_activity(Arc::clone(&self.notify));
                // A quota wait belonged to the process that is gone.
                harness.clear_quota();
                if let Some(delivery) = self.delivery.as_mut() {
                    if !same_session && let Err(err) = delivery.retire_harness() {
                        self.last_error = Some(format!("commit proven mail: {err}"));
                    }
                    self.pending_recovery = true;
                    self.recovery_blocks_mail = false;
                    delivery.replace_harness(harness);
                }
                self.exit_watch = None;
                self.served = None;
                self.turns_recorded = 0;
                self.restart_at = None;
                self.sync_note();
                true
            }
            Err(_) => {
                self.restart_at = None;
                false
            }
        }
    }

    fn quota_waiting(&mut self, health: &HarnessHealth) -> bool {
        let Some(reset_at) = health.quota_reset_at_ms else {
            self.quota_started_at = None;
            return false;
        };
        let now = self.inner.clock.now_ms();
        let started_at = *self.quota_started_at.get_or_insert(now);
        let rotated_after_start = self
            .marker_ms(PROFILE_ROTATED)
            .is_some_and(|ms| ms >= started_at);
        let cleared_after_start = self
            .marker_ms(QUOTA_CLEARED)
            .is_some_and(|ms| ms >= started_at);
        let config_switched = self.runtime_switch_pending() == Some(true);
        if rotated_after_start || cleared_after_start || config_switched || now >= reset_at {
            self.quota_started_at = None;
            if let Some(delivery) = self.delivery.as_mut() {
                delivery.clear_quota();
            }
            // A new process makes this pass's snapshot stale: start over.
            return (rotated_after_start || config_switched) && self.replace_process();
        }
        self.commit_proven();
        self.sleep(Some(reset_at));
        true
    }

    /// Commit what the harness already proved it took, before anything that
    /// skips the delivery pass: a quota wait, a switch drain, or replacing
    /// the process, alive or dead. A proof outlives the process that gave it
    /// but not the adapter that holds it, so an uncommitted one would hand
    /// the message to the next process again. Nothing is written.
    fn commit_proven(&mut self) {
        let Some(delivery) = self.delivery.as_mut() else {
            return;
        };
        match delivery.commit_proven() {
            Ok(committed) => {
                self.written = self
                    .written
                    .saturating_add(committed.clamp(0, u64::from(u32::MAX)) as u32);
            }
            Err(err) => self.last_error = Some(format!("commit proven mail: {err}")),
        }
    }

    /// The last message the harness refused for good in this pass, as the
    /// agent's error, so a parked message is seen.
    fn take_rejection_note(&mut self) -> Option<String> {
        let delivery = self.delivery.as_mut()?;
        delivery.take_rejections().pop().map(|rejection| {
            format!(
                "message {} (seq {}) rejected by the harness: {}",
                rejection.message_id, rejection.seq, rejection.reason
            )
        })
    }

    /// One pass over the mailbox. Every line is handed to the harness's own
    /// input or steer API, in sequence order; the cursor moves behind each one
    /// it takes. Nothing here waits for a turn to end.
    ///
    /// `true` when a second loop already holds the agent.
    fn deliver_once(&mut self) -> bool {
        if let Some(switch_started_at) = self.pending_switch_started_at {
            // An explicit harness or model switch is pending while the old
            // process is mid-turn: hold new mailbox messages in SQLite for the
            // replacement process instead of steering them into the old turn,
            // and cut over at the turn boundary (a harness transition wakes
            // this worker) or at the drain deadline.
            // What the old process proves while it drains is committed now.
            self.commit_proven();
            let deadline = switch_started_at.saturating_add(PENDING_SWITCH_DRAIN_MS);
            let now = self.inner.clock.now_ms();
            // Never busy-loop on a wait whose deadline is already past.
            let wait_until = if deadline > now {
                deadline
            } else {
                now + 1_000
            };
            self.sleep(Some(wait_until));
            return false;
        }
        // An interrupted turn's messages are older than any mail behind
        // them: its recovery note goes first, and new mail follows it only
        // once it is written.
        if self.pending_recovery {
            if self.recover_once() {
                return false;
            }
            if self.pending_recovery && self.recovery_blocks_mail {
                if let Some(health) = self.harness_health() {
                    self.idle_wait(&health);
                }
                return false;
            }
        }
        let outcome = {
            let Some(delivery) = self.delivery.as_mut() else {
                return false;
            };
            delivery.run_once()
        };
        let rejected = self.take_rejection_note();
        match outcome {
            Ok(RunOutcome::Quota { .. }) => {
                if rejected.is_some() {
                    self.last_error = rejected;
                    self.publish();
                }
                false
            }
            Ok(RunOutcome::Delivered { delivered }) => {
                // The turn this opens may start new commands: the next idle
                // point looks at the process tree again.
                self.exit_watch = None;
                self.last_error = rejected;
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
                if rejected.is_some() {
                    self.last_error = rejected;
                    self.publish();
                }
                let Some(mut health) = self.harness_health() else {
                    return false;
                };
                if !self.record_usage_with(&health) {
                    let retry_at = self.inner.clock.now_ms() + IDLE_REVIEW_MS;
                    self.sleep(Some(retry_at));
                    return false;
                }
                if self.try_apply_pending_switch(&health) {
                    return false;
                }
                if !health.process_alive
                    && self
                        .delivery
                        .as_ref()
                        .is_some_and(|d| d.store().has_open_turn(&self.id).unwrap_or(false))
                {
                    if !self.wait_backoff() {
                        return false;
                    }
                    self.replace_process();
                    return false;
                }
                if self.pending_recovery {
                    if self.recover_once() {
                        return false;
                    }
                    // A recovery note may have opened a turn.
                    let Some(fresh) = self.harness_health() else {
                        return false;
                    };
                    health = fresh;
                }
                if let Some(err) = self.recoverable_mid_turn_fault(&health) {
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
                if self.try_release_idle_agent(&health) {
                    return false;
                }
                self.idle_wait(&health);
                false
            }
            Err(anyagents_agent::Error::HarnessDown) => {
                // The process went while mail was offered: a turn its adapter
                // saw end before then is completed, not recovered.
                if let Some(health) = self.harness_health() {
                    self.record_usage_with(&health);
                }
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
    fn try_apply_pending_switch(&mut self, health: &HarnessHealth) -> bool {
        if self.delivery.is_none() {
            self.pending_switch_started_at = None;
            return false;
        }
        let replaceable = health.turn_state == TurnState::Idle
            || !health.process_alive
            || health.sign_in_fault.is_some()
            || self.last_error.is_some();
        let Some(runtime_changed) = self.runtime_switch_pending() else {
            return false;
        };
        let now = self.inner.clock.now_ms();
        if now < self.switch_retry_at {
            self.pending_switch_started_at = None;
            return false;
        }
        if !runtime_changed {
            self.pending_switch_started_at = None;
            if !replaceable
                || !self.profile_rotation_pending()
                || self.profile_rotation_waits_for_turn(health)
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
        match self.harness_health() {
            Some(health) => self.record_usage_with(&health),
            None => true,
        }
    }

    /// [`Session::record_usage`] against a snapshot this pass already read.
    fn record_usage_with(&mut self, health: &HarnessHealth) -> bool {
        let Some(delivery) = self.delivery.as_ref() else {
            return true;
        };
        let served = delivery.harness().last_usage();
        let before = self.served.clone().unwrap_or_default();
        let turns_served = delivery.harness().turns_served();
        // A turn the adapter saw end did end, even if its process exited
        // since: a served turn counts whenever the adapter itself answered
        // (it is in this process, or its keeper's link is open). Usage alone
        // says a turn ended only while the process lives: one that is gone
        // (or a keeper that no longer answers) reads idle with its last
        // totals, which proves nothing; its turn is recovered by the next
        // process. Served totals only grow: a lower or equal reading is no
        // news.
        let served_turn = turns_served > self.turns_recorded
            && self.inner.custody.answers_live(delivery.harness());
        let advanced = usage_advanced(&served, &before);
        let completed = (served_turn || (advanced && health.process_alive))
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
        let served_changed = advanced;
        if served_changed {
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
        if completed || served_changed {
            self.sync_note();
        }
        true
    }

    /// Automatically scale any agent to zero (`state = "idle"`)
    /// once it has finished its turn, its mailbox is drained, it has no active
    /// child sub-agents, and its harness process has no in-flight background commands.
    /// Its config directory and session are preserved so sending any future message
    /// to the agent automatically wakes and resumes it.
    fn try_release_idle_agent(&mut self, health: &HarnessHealth) -> bool {
        self.review_children = false;
        if self.last_error.is_some() {
            return false;
        }
        if health.turn_state != TurnState::Idle || health.sign_in_fault.is_some() {
            self.exit_watch = None;
            return false;
        }
        if self.commands_in_flight() {
            return false;
        }
        let scaled = self.delivery.as_ref().is_some_and(|delivery| {
            delivery
                .store()
                .complete_idle_agent(&self.id)
                .unwrap_or(false)
        });
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
        self.stop_delivery();
        self.restart_at = None;
        reap_ephemeral_dirs(&self.paths.config_dir, &self.paths.managed_work);
        self.publish();
        true
    }

    /// Whether a command still runs under the idle harness. The process tree
    /// is read once per idle point; while the commands found then still run,
    /// it is not read again, and the exit of any of them wakes this worker.
    fn commands_in_flight(&mut self) -> bool {
        if self.exit_watch.as_ref().is_some_and(|watch| !watch.fired()) {
            return true;
        }
        self.exit_watch = None;
        let Some(delivery) = self.delivery.as_ref() else {
            return false;
        };
        let mut scanned = false;
        let running = harness_inflight_processes(delivery.harness().process_id(), &mut scanned);
        if scanned && !self.process_scan_logged {
            self.process_scan_logged = true;
            eprintln!(
                "anyagents-supervisor: {}: /proc/<pid>/task/<tid>/children is unavailable; \
                 idle release reads the whole process table at each idle point",
                self.id
            );
        }
        if running.is_empty() {
            return false;
        }
        match ExitWatch::start(&running, Arc::clone(&self.notify)) {
            Ok(watch) => self.exit_watch = Some(watch),
            // One already exited: look again on the next pass.
            Err(WatchError::Gone) => self.woken.store(true, Ordering::SeqCst),
            Err(WatchError::Unsupported) => self.review_children = true,
        }
        true
    }

    /// Deliver one synthetic `[Recovery]` wake when a newly started harness
    /// process has an unfinished turn (`opened_seq > completed_seq`) and an
    /// empty mailbox. Returns `true` when the caller should re-enter the main
    /// loop immediately (e.g. after process replacement on failure).
    fn recoverable_mid_turn_fault(&self, health: &HarnessHealth) -> Option<String> {
        let delivery = self.delivery.as_ref()?;
        let fault = health.sign_in_fault.clone()?;
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
            let open = match delivery.store().open_turn(&self.id) {
                Ok(open) => open,
                Err(err) => {
                    self.last_error = Some(format!("read open turn for recovery: {err}"));
                    return false;
                }
            };
            match open {
                None => {
                    self.pending_recovery = false;
                    self.recovery_blocks_mail = false;
                    if self
                        .last_error
                        .as_deref()
                        .is_some_and(|error| error.starts_with("read open turn for recovery:"))
                    {
                        self.last_error = None;
                    }
                    return false;
                }
                Some(open) => delivery.recover_open_turn(&open),
            }
        };
        match outcome {
            Ok(DeliverOutcome::Accepted { .. } | DeliverOutcome::Duplicate) => {
                self.pending_recovery = false;
                self.recovery_blocks_mail = false;
                self.last_error = None;
                self.written = self.written.saturating_add(1);
                // The store counted this recovery: the note kept with the
                // process says it is no longer owed before anything else, so
                // a kernel that dies now leaves no second note to send.
                self.sync_note();
                false
            }
            Ok(DeliverOutcome::Rejected { reason, .. }) => {
                self.pending_recovery = false;
                self.recovery_blocks_mail = false;
                self.last_error = Some(format!("recovery note rejected by the harness: {reason}"));
                self.sync_note();
                self.publish();
                false
            }
            // Written and not yet proved: mail may follow it.
            Ok(DeliverOutcome::InFlight) => {
                self.recovery_blocks_mail = false;
                false
            }
            // Not written: mail waits behind it.
            Ok(DeliverOutcome::Deferred | DeliverOutcome::Quota { .. }) => {
                self.recovery_blocks_mail = true;
                false
            }
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

    /// Sleep until something can change what this worker would do.
    ///
    /// An idle harness waits for an event: mail, a harness transition, an
    /// agent change, or the exit of a command that kept it awake, bounded only
    /// by `IDLE_SAFETY_NET_MS`. While a turn is open (or such commands cannot
    /// be watched) it also reviews on a backoff from `IDLE_REVIEW_MS` doubling
    /// to `IDLE_REVIEW_MAX_MS`, for an adapter that learns its turn end only by
    /// asking; a wake resets the backoff.
    fn idle_wait(&mut self, health: &HarnessHealth) {
        let timed = health.turn_state == TurnState::InTurn || self.review_children;
        let interval = if timed {
            self.idle_review_ms
        } else {
            IDLE_SAFETY_NET_MS
        };
        let deadline = self.inner.clock.now_ms() + interval;
        if self.sleep(Some(deadline)) {
            self.idle_review_ms = IDLE_REVIEW_MS;
        } else if timed {
            self.idle_review_ms = self
                .idle_review_ms
                .saturating_mul(2)
                .min(IDLE_REVIEW_MAX_MS);
        } else {
            // A whole safety net without an event: read the agent row again.
            self.agent_changed.store(true, Ordering::SeqCst);
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
        let deadline = self.inner.clock.now_ms() + IDLE_REVIEW_MS;
        self.sleep(Some(deadline));
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

/// The commands running under the harness process: every live child other
/// than an idle persistent sidecar (`anyagents ... mcp`,
/// `codex-code-mode-host`, `language_server_linux`), and every live child of
/// a sidecar. Sets `scanned` when the kernel offers no per-task `children`
/// file and the whole process table had to be read instead.
fn harness_inflight_processes(root_pid: Option<u32>, scanned: &mut bool) -> Vec<u32> {
    let Some(root_pid) = root_pid.filter(|&p| p > 0) else {
        return Vec::new();
    };
    #[cfg(unix)]
    {
        // Read the harness's own subtree: a whole-/proc scan reads every
        // process's status file on the host.
        let Some(children) = proc_children(root_pid) else {
            *scanned = true;
            return harness_inflight_processes_by_scan(root_pid);
        };
        let mut running = Vec::new();
        for cpid in children.into_iter().filter(|&pid| proc_is_live(pid)) {
            if is_persistent_sidecar(cpid) {
                running.extend(
                    proc_children(cpid)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|&pid| proc_is_live(pid)),
                );
            } else {
                running.push(cpid);
            }
        }
        running
    }
    #[cfg(not(unix))]
    {
        let _ = (root_pid, scanned);
        Vec::new()
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
fn harness_inflight_processes_by_scan(root_pid: u32) -> Vec<u32> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
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
    let mut running = Vec::new();
    for &cpid in children_of.get(&root_pid).into_iter().flatten() {
        if is_persistent_sidecar(cpid) {
            running.extend(children_of.get(&cpid).into_iter().flatten().copied());
        } else {
            running.push(cpid);
        }
    }
    running
}

/// Why an exit watch was not armed.
#[derive(Debug, PartialEq, Eq)]
enum WatchError {
    /// One of the processes had already exited. Only pidfds report it.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Gone,
    /// The kernel offers no pidfd for them.
    Unsupported,
}

/// Waits on pidfds for any of a set of processes to exit, then calls
/// `notify` once. Dropping the watch ends its thread without a call.
struct ExitWatch {
    fired: Arc<AtomicBool>,
    /// Write end of the pipe the thread also polls: closing it on drop wakes
    /// the thread with a hang-up.
    _cancel: std::os::fd::OwnedFd,
}

impl ExitWatch {
    #[cfg(target_os = "linux")]
    fn start(pids: &[u32], notify: Arc<dyn Fn() + Send + Sync>) -> Result<Self, WatchError> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        let mut pidfds = Vec::with_capacity(pids.len());
        for &pid in pids {
            let Ok(pid) = libc::pid_t::try_from(pid) else {
                return Err(WatchError::Unsupported);
            };
            // SAFETY: pidfd_open takes a pid and flags and returns a new
            // descriptor or -1.
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            if fd < 0 {
                return Err(match std::io::Error::last_os_error().raw_os_error() {
                    Some(libc::ESRCH) => WatchError::Gone,
                    _ => WatchError::Unsupported,
                });
            }
            // SAFETY: the syscall returned a descriptor this process now owns.
            pidfds.push(unsafe { OwnedFd::from_raw_fd(fd as i32) });
        }
        let mut ends = [0; 2];
        // SAFETY: pipe2 fills both descriptors on success.
        if unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(WatchError::Unsupported);
        }
        // SAFETY: pipe2 returned two descriptors this process now owns.
        let (cancel_rx, cancel_tx) =
            unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
        let fired = Arc::new(AtomicBool::new(false));
        let thread_fired = Arc::clone(&fired);
        thread::Builder::new()
            .name("anyagents-exits".into())
            .spawn(move || {
                let mut polled: Vec<libc::pollfd> = pidfds
                    .iter()
                    .chain(std::iter::once(&cancel_rx))
                    .map(|fd| libc::pollfd {
                        fd: fd.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    })
                    .collect();
                loop {
                    // SAFETY: `polled` holds valid descriptors owned by this
                    // thread for the whole call.
                    let ready = unsafe {
                        libc::poll(polled.as_mut_ptr(), polled.len() as libc::nfds_t, -1)
                    };
                    if ready < 0
                        && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
                    {
                        continue;
                    }
                    break;
                }
                let (cancel, exits) = polled.split_last().expect("cancel end polled");
                if cancel.revents == 0 && exits.iter().any(|fd| fd.revents != 0) {
                    thread_fired.store(true, Ordering::SeqCst);
                    notify();
                }
            })
            .map_err(|_| WatchError::Unsupported)?;
        Ok(Self {
            fired,
            _cancel: cancel_tx,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn start(_pids: &[u32], _notify: Arc<dyn Fn() + Send + Sync>) -> Result<Self, WatchError> {
        Err(WatchError::Unsupported)
    }

    /// One of the watched processes has exited.
    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
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
    use super::*;
    use anyagents_harness::DeliveryMode;

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
            custody: Arc::new(InProcess),
            extra_env: Vec::new(),
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            retain: AtomicBool::new(false),
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
        assert!(!slot.done.is_set());
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
        while harness_inflight_processes(Some(parent.id()), &mut false).is_empty()
            && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        let mut scanned = false;
        assert_eq!(
            harness_inflight_processes(Some(parent.id()), &mut scanned).len(),
            1,
            "a running command under the harness is in flight"
        );
        assert!(
            harness_inflight_processes(Some(bare.id()), &mut scanned).is_empty(),
            "a harness with no children has nothing in flight"
        );
        assert!(harness_inflight_processes(None, &mut scanned).is_empty());
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

    #[cfg(target_os = "linux")]
    #[test]
    fn an_exit_watch_wakes_once_when_a_command_exits_and_never_after_drop() {
        use std::process::Command;
        use std::sync::mpsc;
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let notify: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = lock(&tx).send(());
        });
        let mut short = Command::new("sleep").arg("0.3").spawn().unwrap();
        let mut long = Command::new("sleep").arg("30").spawn().unwrap();
        let watch = match ExitWatch::start(&[short.id(), long.id()], Arc::clone(&notify)) {
            Ok(watch) => watch,
            // A kernel without pidfd falls back to the timed review.
            Err(WatchError::Unsupported) => {
                let _ = long.kill();
                let _ = long.wait();
                let _ = short.wait();
                return;
            }
            Err(WatchError::Gone) => panic!("both commands still run"),
        };
        assert!(!watch.fired());
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the exit of one watched command wakes the worker");
        assert!(watch.fired());
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        let _ = short.wait();

        let cancelled = ExitWatch::start(&[long.id()], Arc::clone(&notify)).unwrap();
        drop(cancelled);
        let _ = long.kill();
        let _ = long.wait();
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "a dropped watch never wakes the worker"
        );
        assert_eq!(
            ExitWatch::start(&[long.id()], notify).err(),
            Some(WatchError::Gone),
            "a command that already exited is reported, not watched"
        );
    }

    /// Records each wait's deadline and returns at once, as a deadline would.
    struct RecordingClock {
        now_ms: i64,
        deadlines: Mutex<Vec<Option<i64>>>,
    }

    impl Clock for RecordingClock {
        fn now_ms(&self) -> i64 {
            self.now_ms
        }
        fn wait(&self, deadline: Option<i64>, woken: &Arc<AtomicBool>) -> bool {
            lock(&self.deadlines).push(deadline);
            woken.swap(false, Ordering::SeqCst)
        }
        fn poke(&self) {}
    }

    #[test]
    fn an_idle_worker_waits_for_events_and_only_an_open_turn_reviews_on_a_timer() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("worker", "contract"))
            .unwrap();
        let clock = Arc::new(RecordingClock {
            now_ms: 1_700_000_000_000,
            deadlines: Mutex::new(Vec::new()),
        });
        let inner = Arc::new(Inner::<anyagents_harness::FakeHarness> {
            store: Arc::clone(&store),
            clock: Arc::clone(&clock) as Arc<dyn Clock>,
            launch: Arc::new(|_| Err("no launch".into())),
            custody: Arc::new(InProcess),
            extra_env: Vec::new(),
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            retain: AtomicBool::new(false),
            workers: Mutex::new(HashMap::new()),
            idle_release_hook: None,
        });
        let mut session = Session::new(inner, agent.clone(), Arc::new(|| {}), Signals::detached());
        session.delivery = Some(
            DeliveryLoop::start(
                Store::open(dir.path().join("state.db")).unwrap(),
                agent.id.clone(),
                anyagents_harness::FakeHarness::new(DeliveryMode::Steer),
            )
            .unwrap(),
        );
        let last_deadline = || *lock(&clock.deadlines).last().unwrap();
        let now = clock.now_ms;

        // A faulted idle agent is not reviewed every few seconds forever.
        session.last_error = Some("the last turn failed".into());
        assert!(!session.deliver_once());
        assert_eq!(last_deadline(), Some(now + IDLE_SAFETY_NET_MS));
        session.last_error = None;

        // Mail opens a turn; while it is open the worker reviews on a backoff.
        store
            .receive(&agent.id, anyagents_core::Incoming::new("work"))
            .unwrap();
        assert!(!session.deliver_once());
        assert_eq!(last_deadline(), Some(now + IDLE_REVIEW_MS));
        assert!(!session.deliver_once());
        assert_eq!(last_deadline(), Some(now + IDLE_REVIEW_MS));
        assert!(!session.deliver_once());
        assert_eq!(last_deadline(), Some(now + 2 * IDLE_REVIEW_MS));

        // The turn ends: an idle worker with nothing to release waits for an
        // event again.
        session
            .delivery
            .as_mut()
            .unwrap()
            .harness_mut()
            .complete_turn()
            .unwrap();
        assert!(!session.deliver_once());
        assert_eq!(last_deadline(), Some(now + IDLE_SAFETY_NET_MS));
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
    use anyagents_harness::{DeliveryMode, FakeHarness, HarnessError, OutboundMessage};
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
        let make_harness = move || UsageHarness {
            inner: FakeHarness::new(DeliveryMode::Steer),
            usage: usage.clone(),
        };
        let harness = make_harness();
        let inner = Arc::new(Inner {
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock::new()),
            launch: Arc::new(move |_| Ok(make_harness())),
            custody: Arc::new(InProcess),
            extra_env: Vec::new(),
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            retain: AtomicBool::new(false),
            workers: Mutex::new(HashMap::new()),
            idle_release_hook: None,
        });
        let delivery = DeliveryLoop::start(
            Store::open(dir.path().join("state.db")).unwrap(),
            agent.id.clone(),
            harness,
        )
        .unwrap();
        let mut session = Session::new(inner, agent, Arc::new(|| {}), Signals::detached());
        session.delivery = Some(delivery);
        session.pending_recovery = true;
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
            custody: Arc::new(InProcess),
            extra_env: Vec::new(),
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            retain: AtomicBool::new(false),
            workers: Mutex::new(HashMap::new()),
            idle_release_hook: None,
        });
        let mut session = Session::new(inner, agent, Arc::new(|| {}), Signals::detached());

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

#[cfg(test)]
mod custody_tests {
    use super::*;

    /// Equal totals are no news, whatever cost they carry; any total above
    /// its last reading is.
    #[test]
    fn usage_advances_only_when_a_total_grows() {
        let usage = |tokens: u64, cost: Option<i64>| TurnUsage {
            input_tokens: tokens,
            output_tokens: tokens,
            reported_cost_micros: cost,
            ..TurnUsage::default()
        };
        assert!(!usage_advanced(&usage(5, Some(7)), &usage(5, Some(7))));
        assert!(!usage_advanced(&usage(5, None), &usage(5, Some(7))));
        assert!(!usage_advanced(&usage(4, Some(6)), &usage(5, Some(7))));
        assert!(!usage_advanced(&usage(5, Some(0)), &usage(5, None)));
        assert!(usage_advanced(&usage(5, Some(8)), &usage(5, Some(7))));
        assert!(usage_advanced(&usage(5, Some(1)), &usage(5, None)));
        assert!(usage_advanced(&usage(6, Some(7)), &usage(5, Some(7))));
        assert_eq!(
            usage(4, Some(6))
                .difference(usage(5, Some(7)))
                .reported_cost_micros,
            Some(0)
        );
    }
    use anyagents_core::NewAgent;
    use anyagents_harness::{HarnessError, OutboundMessage};

    /// A process whose end looks at the agent lock: whether it was still held
    /// when the process ended.
    struct Probe {
        lock_path: PathBuf,
        held_when_ended: Arc<Mutex<Option<bool>>>,
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            use std::os::fd::AsRawFd as _;
            let file = fs::File::open(&self.lock_path).unwrap();
            // SAFETY: flock on a descriptor this probe owns; dropping it
            // releases what it took.
            let free = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 };
            *lock(&self.held_when_ended) = Some(!free);
        }
    }

    impl Harness for Probe {
        fn deliver(&mut self, _message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
            Ok(DeliverOutcome::Deferred)
        }
        fn background(&mut self) -> Result<(), HarnessError> {
            Ok(())
        }
        fn resume(&mut self, _session: &str) -> Result<(), HarnessError> {
            Ok(())
        }
        fn cancel(&mut self) -> Result<(), HarnessError> {
            Ok(())
        }
        fn health(&self) -> HarnessHealth {
            HarnessHealth {
                process_alive: true,
                turn_state: TurnState::Idle,
                delivery: anyagents_harness::Delivery {
                    level: anyagents_harness::DeliveryLevel::TurnEnd,
                    idempotent: false,
                    proof: "a probe proves nothing".into(),
                    peer: None,
                },
                quota_reset_at_ms: None,
                sign_in_fault: None,
            }
        }
    }

    struct OneLeft(Mutex<Option<Attached<Probe>>>);

    impl Custody<Probe> for OneLeft {
        fn outlives_kernel(&self) -> bool {
            true
        }
        fn retained(&self) -> Vec<AgentId> {
            Vec::new()
        }
        fn attach(&self, _agent: &AgentId) -> Result<Option<Attached<Probe>>, String> {
            Ok(lock(&self.0).take())
        }
        fn release(&self, _agent: &AgentId) {}
        fn retain(&self, harness: Probe) {
            std::mem::forget(harness);
        }
        fn note(&self, _harness: &Probe, _note: &str) {}
    }

    /// A process left for an agent that stopped running meanwhile is ended
    /// before the agent lock it was taken over with is released: never left
    /// running without its lock, even for a moment.
    #[test]
    fn a_process_taken_over_for_a_stopped_agent_ends_before_its_lock_is_released() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("alpha", "claude-code"))
            .unwrap();
        store.set_agent_state(&agent.id, "paused").unwrap();
        let taken = store.try_lock_agent(&agent.id).unwrap();
        let keeper_copy = taken.share().unwrap();
        taken.retain();
        drop(taken);
        let adopted = AgentLock::adopt(
            std::os::fd::AsFd::as_fd(&keeper_copy)
                .try_clone_to_owned()
                .unwrap(),
        );
        // The keeper is gone: only the adopted copy holds the lock now.
        drop(keeper_copy);
        let held_when_ended = Arc::new(Mutex::new(None));
        let probe = Probe {
            lock_path: anyagents_core::agent_lock_path(store.data_dir(), &agent.id),
            held_when_ended: Arc::clone(&held_when_ended),
        };
        let inner = Arc::new(Inner::<Probe> {
            store: Arc::clone(&store),
            clock: Arc::new(SystemClock::new()),
            launch: Arc::new(|_| Err("no launch".into())),
            custody: Arc::new(OneLeft(Mutex::new(Some(Attached {
                harness: probe,
                lock: adopted,
                harness_name: "claude-code".into(),
                model: None,
                started_at_ms: None,
                note: None,
                older: Vec::new(),
            })))),
            extra_env: Vec::new(),
            board: HealthBoard::default(),
            stop: AtomicBool::new(false),
            retain: AtomicBool::new(false),
            workers: Mutex::new(HashMap::new()),
            idle_release_hook: None,
        });
        let mut session = Session::new(inner, agent.clone(), Arc::new(|| {}), Signals::detached());
        assert!(matches!(session.boot(), Boot::Stopped));
        assert_eq!(
            *lock(&held_when_ended),
            Some(true),
            "the lock was released before the process ended"
        );
        store.try_lock_agent(&agent.id).unwrap();
    }
}
