//! One long-lived vendor child that speaks one JSON object per line.
//!
//! The adapter decides what a line means. This crate owns the process: spawning
//! it, writing a line, reading lines, remembering which mailbox ids were written,
//! and finishing the open turn when the adapter says a line is a result.
//!
//! An id already written, or found in any text the child sends back, is not
//! written again. A message written while a turn is open waits, and the next
//! result promotes it. That is followup.

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

mod rpc;

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, HarnessError, HarnessHealth, OutboundMessage, SteerSupport,
    TurnState, TurnUsage, message_id_in_text,
};
use serde_json::Value;

pub use rpc::{NotificationEffect, RpcChild, RpcFail, RpcLaunch};

/// What an adapter does with one line the child printed.
pub struct LineEffect {
    /// This line closes the open turn.
    pub finishes_turn: bool,
    /// Session id the vendor reported, when this line carries one.
    pub session_id: Option<String>,
    /// Token totals for the turn this line finished.
    pub usage: Option<TurnUsage>,
    /// A rate limit with a reset time, in epoch milliseconds.
    pub quota_reset_at_ms: Option<i64>,
    /// Answer text for the open turn, when this line carries some.
    pub text: Option<String>,
}

/// How to start one vendor child.
pub struct Launch {
    pub name: String,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub work_dir: Option<PathBuf>,
    pub session_id: Option<String>,
    pub mode: DeliveryMode,
    pub steer: SteerSupport,
    pub on_line: Arc<dyn Fn(&Value) -> LineEffect + Send + Sync>,
}

/// One vendor child.
pub struct StreamHarness {
    name: String,
    program: PathBuf,
    args: Vec<String>,
    environment: Vec<(String, String)>,
    work_dir: Option<PathBuf>,
    mode: DeliveryMode,
    steer: SteerSupport,
    shared: Arc<Mutex<State>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    on_line: Arc<dyn Fn(&Value) -> LineEffect + Send + Sync>,
}

/// Shared by every harness over one child: the owner and any reader.
struct State {
    child: Option<Child>,
    alive: bool,
    epoch: u64,
    seen: HashSet<String>,
    /// The same ids, in the order they were first seen.
    seen_order: Vec<String>,
    written: Vec<String>,
    open: Vec<String>,
    followups: Vec<String>,
    finished: HashSet<String>,
    turns_started: usize,
    quota_reset_at_ms: Option<i64>,
    session_id: Option<String>,
    usages: Vec<(String, TurnUsage)>,
    /// The text lines of the open turn so far, and the id they were read under.
    text: Option<(String, Vec<String>)>,
    /// The last open turn ended on quiet rather than on a result line.
    finished_quiet: HashSet<String>,
    /// Per finished id, the closing line the turn ended on.
    results: Vec<(String, Value)>,
}

impl StreamHarness {
    /// Spawn the child described by `launch` and start reading its stdout.
    ///
    /// `on_line` runs on the reader thread for every JSON line. It must not call
    /// back into this harness.
    pub fn launch(launch: Launch) -> Result<Self, HarnessError> {
        let mut harness = Self {
            name: launch.name,
            program: launch.program,
            args: launch.args,
            environment: launch.environment,
            work_dir: launch.work_dir,
            mode: launch.mode,
            steer: launch.steer,
            shared: Arc::new(Mutex::new(State::new(launch.session_id.as_deref()))),
            stdin: Arc::new(Mutex::new(None)),
            wake: Arc::new(Condvar::new()),
            reader: None,
            on_line: launch.on_line,
        };
        harness.spawn()?;
        Ok(harness)
    }

    /// Add one environment variable for every child this harness starts from
    /// now on, including a restart after a resume.
    pub fn set_env(&mut self, key: &str, value: &str) {
        self.environment.retain(|(item, _)| item != key);
        self.environment.push((key.to_string(), value.to_string()));
    }

    pub fn program(&self) -> &Path {
        &self.program
    }

    pub fn written_ids(&self) -> Vec<String> {
        lock(&self.shared).written.clone()
    }

    pub fn turns_started(&self) -> usize {
        lock(&self.shared).turns_started
    }

    pub fn session_id(&self) -> Option<String> {
        lock(&self.shared).session_id.clone()
    }

    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        let state = lock(&self.shared);
        state
            .usages
            .iter()
            .rev()
            .find(|(item, _)| item == id)
            .map(|(_, usage)| usage.clone())
    }

    pub fn finished(&self, id: &str) -> bool {
        lock(&self.shared).finished.contains(id)
    }

    /// The last line the adapter closed a turn with while `id` was open, for an
    /// adapter that reads how the turn ended from that line.
    pub fn result_for(&self, id: &str) -> Option<Value> {
        lock(&self.shared)
            .results
            .iter()
            .rev()
            .find(|(item, _)| item == id)
            .map(|(_, value)| value.clone())
    }

    /// The last open turn for `id` ended on quiet, not on a result line.
    pub fn finished_quiet(&self, id: &str) -> bool {
        lock(&self.shared).finished_quiet.contains(id)
    }

    pub fn wait_finished(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.finished.contains(id))
    }

    pub fn seen(&self, id: &str) -> bool {
        lock(&self.shared).seen.contains(id)
    }

    /// Every id written or echoed back, in the order it was first seen.
    pub fn seen_ids(&self) -> Vec<String> {
        lock(&self.shared).seen_order.clone()
    }

    /// The mailbox id seen last. A channel that hands over what comes after it
    /// cannot repeat a line the host has already written.
    pub fn highest_id(&self) -> Option<String> {
        self.seen_ids().into_iter().next_back()
    }

    pub fn wait_seen(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.seen.contains(id))
    }

    /// No line has arrived for `quiet` and the process is up. Gives up after
    /// `patience` and when the child exits.
    pub fn quiet(&self, quiet: Duration, patience: Duration) -> bool {
        let deadline = Instant::now() + patience;
        loop {
            let epoch = lock(&self.shared).epoch;
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let state = lock(&self.shared);
            let (state, _) = self
                .wake
                .wait_timeout_while(state, quiet.min(deadline - now), |state| {
                    state.epoch == epoch
                })
                .unwrap_or_else(|err| err.into_inner());
            if state.alive && state.epoch == epoch {
                let mut state = state;
                finish_open_quiet(&mut state);
                self.wake.notify_all();
                return true;
            }
            if !state.alive {
                return false;
            }
        }
    }

    pub fn wait_quota(&self, timeout: Duration) -> Option<i64> {
        let mut found = None;
        self.wait_pred(timeout, |state| {
            found = state.quota_reset_at_ms;
            found.is_some()
        });
        found
    }

    /// The text lines the adapter attached to `id`, joined. The turn may still
    /// be open, so the answer may still grow.
    pub fn text_for(&self, id: &str) -> Option<String> {
        let state = lock(&self.shared);
        state
            .text
            .as_ref()
            .filter(|(owner, _)| owner == id)
            .map(|(_, lines)| lines.join("\n"))
    }

    /// Write one line for `message`, unless that id was already written or echoed.
    pub fn deliver(
        &mut self,
        message: &OutboundMessage,
        line: &Value,
    ) -> Result<DeliverOutcome, HarnessError> {
        let mut state = lock(&self.shared);
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        if state.seen.contains(&message.id) {
            return Ok(DeliverOutcome::Accepted {
                turn_finished: state.finished.contains(&message.id),
            });
        }
        if let Some(reset_at_ms) = state.quota_reset_at_ms {
            return Ok(DeliverOutcome::Quota { reset_at_ms });
        }
        if write_stdin(&self.stdin, line).is_err() {
            state.alive = false;
            self.wake.notify_all();
            return Err(HarnessError::Dead);
        }
        if state.open.is_empty() {
            state.turns_started += 1;
            state.open.push(message.id.clone());
        } else {
            state.followups.push(message.id.clone());
        }
        note_seen(&mut state, &message.id);
        state.written.push(message.id.clone());
        Ok(DeliverOutcome::Accepted {
            turn_finished: false,
        })
    }

    pub fn background(&self) -> Result<(), HarnessError> {
        if lock(&self.shared).alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    /// Restart the child with `command` when the session changed or it has exited.
    pub fn resume(&mut self, session: &str, mut command: Command) -> Result<(), HarnessError> {
        let same = {
            let state = lock(&self.shared);
            state.alive && state.session_id.as_deref() == Some(session)
        };
        if same {
            return Ok(());
        }
        self.stop_child();
        self.args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        for (key, value) in &self.environment {
            command.env(key, value);
        }
        lock(&self.shared).session_id = Some(session.to_string());
        self.spawn_command(command)
    }

    /// Write `line` and drop the open turn. The vendor decides what the line means.
    pub fn cancel(&mut self, line: &Value) -> Result<(), HarnessError> {
        let mut state = lock(&self.shared);
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        if write_stdin(&self.stdin, line).is_err() {
            state.alive = false;
            self.wake.notify_all();
            return Err(HarnessError::Dead);
        }
        state.open.clear();
        state.followups.clear();
        Ok(())
    }

    pub fn health(&self) -> HarnessHealth {
        let state = lock(&self.shared);
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.open.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery_mode: self.mode,
            steer_support: self.steer,
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }

    pub fn clear_quota(&self) {
        lock(&self.shared).quota_reset_at_ms = None;
    }

    pub fn process_id(&self) -> Option<u32> {
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    pub fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let _ = thread::Builder::new()
            .name(format!("{}-watch", self.name))
            .spawn(move || {
                let mut state = lock(&shared);
                let mut seen = 0u64;
                loop {
                    if !state.alive {
                        drop(state);
                        notify();
                        break;
                    }
                    if state.epoch != seen {
                        seen = state.epoch;
                        drop(state);
                        notify();
                        state = lock(&shared);
                        continue;
                    }
                    state = wake.wait(state).unwrap_or_else(|err| err.into_inner());
                }
            });
    }

    fn spawn(&mut self) -> Result<(), HarnessError> {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        self.spawn_command(command)
    }

    fn spawn_command(&mut self, mut command: Command) -> Result<(), HarnessError> {
        close_inherited_files(&mut command);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        for (key, value) in &self.environment {
            command.env(key, value);
        }
        let mut child = command
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let Some(child_stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdin missing".into()));
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdout missing".into()));
        };
        {
            let mut state = lock(&self.shared);
            state.child = Some(child);
            state.alive = true;
            state.open.clear();
            state.followups.clear();
            state.text = None;
        }
        *lock(&self.stdin) = Some(child_stdin);
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let on_line = Arc::clone(&self.on_line);
        match thread::Builder::new()
            .name(self.name.clone())
            .spawn(move || read_stdout(stdout, shared, wake, on_line))
        {
            Ok(handle) => self.reader = Some(handle),
            Err(err) => {
                self.stop_child();
                return Err(HarnessError::Failed(err.to_string()));
            }
        }
        Ok(())
    }

    fn stop_child(&mut self) {
        *lock(&self.stdin) = None;
        {
            let mut state = lock(&self.shared);
            state.alive = false;
            if let Some(child) = state.child.as_mut() {
                let _ = child.kill();
            }
        }
        self.wake.notify_all();
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        let mut state = lock(&self.shared);
        if let Some(mut child) = state.child.take() {
            let _ = child.wait();
        }
        state.open.clear();
        state.followups.clear();
        state.text = None;
        state.alive = false;
    }

    fn wait_pred(&self, timeout: Duration, mut pred: impl FnMut(&State) -> bool) -> bool {
        let mut state = lock(&self.shared);
        let deadline = Instant::now() + timeout;
        loop {
            if pred(&state) {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, wait) = self
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|err| err.into_inner());
            state = guard;
            if wait.timed_out() && !pred(&state) {
                return false;
            }
        }
    }
}

impl Drop for StreamHarness {
    fn drop(&mut self) {
        self.stop_child();
    }
}

/// A vendor child inherits this process's open files by default, including the
/// server socket. Then the server never sees the child exit: EOF waits for a
/// socket only the server itself closes, and a dead vendor looks alive and holds
/// its mailbox forever. Every child starts without them.
#[cfg(target_os = "linux")]
fn close_inherited_files(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    // `pre_exec` answers the command it was given: the call must land on it, so
    // the closure runs in the child just before `exec`. One `close_range` call
    // closes every descriptor above stderr; nothing else is touched. The server
    // runs no other program, so nothing it keeps is lost.
    let linked = unsafe {
        command.pre_exec(|| {
            let closed = libc::close_range(3, u32::MAX, 0);
            if closed != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        })
    };
    let _ = linked;
}

#[cfg(not(target_os = "linux"))]
fn close_inherited_files(_command: &mut Command) {}

/// The same rule, for a test that starts something else.
#[cfg(target_os = "linux")]
pub fn close_inherited_for_test(command: &mut Command) {
    close_inherited_files(command)
}

impl State {
    fn new(session_id: Option<&str>) -> Self {
        Self {
            child: None,
            alive: false,
            epoch: 0,
            seen: HashSet::new(),
            seen_order: Vec::new(),
            written: Vec::new(),
            open: Vec::new(),
            followups: Vec::new(),
            finished: HashSet::new(),
            turns_started: 0,
            quota_reset_at_ms: None,
            session_id: session_id.map(str::to_string),
            usages: Vec::new(),
            text: None,
            finished_quiet: HashSet::new(),
            results: Vec::new(),
        }
    }
}

fn read_stdout(
    stdout: ChildStdout,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    on_line: Arc<dyn Fn(&Value) -> LineEffect + Send + Sync>,
) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let effect = on_line(&value);
        {
            let mut state = lock(&shared);
            note_markers(&mut state, &value);
            if let Some(reset_at_ms) = effect.quota_reset_at_ms {
                state.quota_reset_at_ms = Some(reset_at_ms);
            }
            if let Some(session_id) = effect.session_id
                && !session_id.is_empty()
            {
                state.session_id = Some(session_id);
            }
            if let Some(text) = effect.text
                && let Some(id) = state.open.first().cloned()
            {
                match &mut state.text {
                    Some((owner, lines)) if *owner == id => lines.push(text),
                    other => *other = Some((id, vec![text])),
                }
            }
            if effect.finishes_turn {
                finish_open(&mut state, effect.usage.unwrap_or_default(), &value);
            }
            state.epoch = state.epoch.wrapping_add(1);
        }
        wake.notify_all();
    }
    {
        let mut state = lock(&shared);
        state.alive = false;
        state.epoch = state.epoch.wrapping_add(1);
    }
    wake.notify_all();
}

/// A harness that streams without a closing result line ends its turn here:
/// the open message is finished with the text seen so far and no usage.
fn finish_open_quiet(state: &mut State) {
    let Some((id, _)) = state.text.clone() else {
        return;
    };
    // A turn that steers in a followup is not over until the vendor says so.
    if !state.open.first().is_some_and(|owner| owner == &id) || !state.followups.is_empty() {
        return;
    }
    state.finished_quiet.insert(id.clone());
    id_for_quiet(state, &id);
}

/// Finish the open message with no usage of its own, keeping its last text.
fn id_for_quiet(state: &mut State, id: &str) {
    let text = state.text.clone();
    state.open.clear();
    state.followups.clear();
    state.finished.insert(id.to_string());
    state.text = text;
}

fn finish_open(state: &mut State, usage: TurnUsage, line: &Value) {
    if state.open.is_empty() {
        return;
    }
    for id in state.open.drain(..) {
        state.finished.insert(id.clone());
        state.usages.push((id.clone(), usage.clone()));
        state.results.push((id, line.clone()));
    }
    if !state.followups.is_empty() {
        state.turns_started += 1;
        state.open.append(&mut state.followups);
    }
}

/// Remember a mailbox id the child sent back, once, in the order it arrived.
fn note_seen(state: &mut State, id: &str) {
    if !id.is_empty() && state.seen.insert(id.to_string()) {
        state.seen_order.push(id.to_string());
    }
}

fn note_markers(state: &mut State, value: &Value) {
    match value {
        Value::String(text) => {
            if let Some(id) = message_id_in_text(text) {
                note_seen(state, id);
            }
        }
        Value::Array(items) => {
            for item in items {
                note_markers(state, item);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                note_markers(state, item);
            }
        }
        _ => {}
    }
}

fn write_stdin(slot: &Mutex<Option<ChildStdin>>, value: &Value) -> std::io::Result<()> {
    let mut guard = lock(slot);
    let Some(stdin) = guard.as_mut() else {
        return Err(std::io::Error::other("stdin closed"));
    };
    writeln!(stdin, "{value}")?;
    stdin.flush()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}
