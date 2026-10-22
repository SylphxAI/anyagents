//! One long-lived child from a catalog entry, and the line protocol it speaks.
//!
//! A stream harness is data: its program name, the arguments that resume or
//! start it, the line that carries a message, and what a printed line means.
//! This module holds that data in [`StreamSpec`] and the process in
//! [`StreamHarness`], so a new followup harness is a catalog entry rather than a
//! copied adapter. The mailbox id is the first line of the user text
//! (`anyagents-message:<id>`); an id already written, or echoed back, is not
//! written again.

mod rpc;

pub use rpc::{NotificationEffect, RequestHandler, RpcChild, RpcFail, RpcLaunch};

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, HarnessError, HarnessHealth, OutboundMessage, SteerSupport,
    TurnState, TurnUsage, compose_message, message_id_in_text, spawn_command,
};
use serde_json::{Value, json};

/// What one printed line means to the turn it belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// What a vendor child prints, and how it is started again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineProtocol {
    /// `result` closes the turn and reports usage but no cost.
    Result,
    /// `done` closes the turn and reports usage but no cost.
    Done,
    /// `result` closes the turn with usage and an optional `cost_micros`; a
    /// `text` line is answer text.
    ResultWithCost,
}

/// One harness's catalog entry, as the launch needs it.
#[derive(Clone, Debug)]
pub struct StreamSpec {
    /// Thread and process name, unique per harness.
    pub name: String,
    /// Arguments before the resume flag. `None` means the program takes no
    /// arguments until it resumes.
    pub args: Vec<String>,
    /// The flag that carries a session id, and the line that starts a turn.
    pub resume_flag: &'static str,
    pub start_line: &'static str,
    pub cancel: &'static str,
    pub protocol: LineProtocol,
}

impl StreamSpec {
    /// The catalog entry for `program`, a followup harness whose fields a real
    /// session still has to confirm.
    pub fn for_program(program: &str) -> Self {
        match program {
            "agy" => Self {
                name: "anyagents-agy".to_string(),
                args: vec!["--output-format".to_string(), "stream-json".to_string()],
                resume_flag: "--resume",
                start_line: "user",
                cancel: "interrupt",
                protocol: LineProtocol::Result,
            },
            "opencode" => Self {
                name: "anyagents-opencode".to_string(),
                args: vec!["serve".to_string()],
                resume_flag: "--session",
                start_line: "prompt",
                cancel: "cancel",
                protocol: LineProtocol::ResultWithCost,
            },
            _ => Self {
                name: format!("anyagents-{program}"),
                args: Vec::new(),
                resume_flag: "--session",
                start_line: "user",
                cancel: "cancel",
                protocol: LineProtocol::Done,
            },
        }
    }

    fn resume_args(&self, session: Option<&str>) -> Vec<String> {
        let mut args = self.args.clone();
        if let Some(id) = session {
            args.push(self.resume_flag.to_string());
            args.push(id.to_string());
        }
        args
    }

    fn effect(&self, value: &Value) -> LineEffect {
        let kind = value.get("type").and_then(Value::as_str);
        let finishes = match self.protocol {
            LineProtocol::Result | LineProtocol::ResultWithCost => kind == Some("result"),
            LineProtocol::Done => kind == Some("done"),
        };
        LineEffect {
            finishes_turn: finishes,
            session_id: value
                .get("session_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            usage: finishes.then(|| self.usage_of(value)),
            quota_reset_at_ms: quota_of(value),
            text: self.text_of(value, finishes),
        }
    }

    fn usage_of(&self, value: &Value) -> TurnUsage {
        let usage = value
            .get("usage")
            .or_else(|| value.pointer("/result/usage"));
        TurnUsage {
            input_tokens: number(usage, "input_tokens"),
            output_tokens: number(usage, "output_tokens"),
            cache_read_tokens: number(usage, "cache_read_tokens"),
            cache_write_tokens: number(usage, "cache_write_tokens"),
            reported_cost_micros: match self.protocol {
                LineProtocol::ResultWithCost => value.get("cost_micros").and_then(Value::as_i64),
                _ => None,
            },
        }
    }

    /// Only a vendor that streams an answer without a closing result line has
    /// answer text: a `text` line, or the text on the closing line itself.
    fn text_of(&self, value: &Value, finishes: bool) -> Option<String> {
        if self.protocol != LineProtocol::ResultWithCost {
            return None;
        }
        let kind = value.get("type").and_then(Value::as_str);
        if !(finishes || kind == Some("text")) {
            return None;
        }
        value
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    }
}

/// How to start one vendor child.
pub struct Launch {
    pub spec: StreamSpec,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub work_dir: Option<PathBuf>,
    pub session_id: Option<String>,
    pub mode: DeliveryMode,
    pub steer: SteerSupport,
}

/// One vendor child.
pub struct StreamHarness {
    spec: StreamSpec,
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
    pending: Option<PendingWrite>,
    #[cfg(target_os = "linux")]
    readiness: Option<JoinHandle<()>>,
}

// Only the delivery owner advances this buffer. A partial line remains in
// mailbox custody (Deferred) until every byte reaches this process's pipe.
struct PendingWrite {
    id: String,
    bytes: Vec<u8>,
    offset: usize,
    generation: u64,
}

/// Shared by every harness over one child: the owner and any reader.
struct State {
    child: Option<Child>,
    alive: bool,
    epoch: u64,
    generation: u64,
    input_blocked: bool,
    seen: HashSet<String>,
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
}

impl StreamHarness {
    /// Spawn the child described by `launch` and start reading its stdout.
    ///
    /// The spec's protocol runs on the reader thread for every JSON line.
    pub fn launch(launch: Launch) -> Result<Self, HarnessError> {
        let mut harness = Self {
            spec: launch.spec,
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
            pending: None,
            #[cfg(target_os = "linux")]
            readiness: None,
        };
        harness.spawn()?;
        Ok(harness)
    }

    /// Start a child for `program` from the catalog, reading its answers under
    /// `spec`.
    pub fn open(
        spec: StreamSpec,
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: Vec<(String, String)>,
    ) -> Result<Self, HarnessError> {
        let args = spec.resume_args(session_id);
        Self::launch(Launch {
            spec,
            program: program.into(),
            args,
            environment,
            work_dir: None,
            session_id: session_id.map(str::to_string),
            mode: DeliveryMode::Followup,
            steer: SteerSupport::Unproven,
        })
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
    pub fn background(&self) -> Result<(), HarnessError> {
        if lock(&self.shared).alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    /// Restart the child when the session changed or it has exited.
    pub fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let same = {
            let state = lock(&self.shared);
            state.alive && state.session_id.as_deref() == Some(session)
        };
        if same {
            return Ok(());
        }
        self.stop_child();
        self.args = self.spec.resume_args(Some(session));
        lock(&self.shared).session_id = Some(session.to_string());
        self.spawn()
    }

    /// Write `line` and drop the open turn. The vendor decides what the line means.
    pub fn cancel(&mut self, line: &Value) -> Result<(), HarnessError> {
        if !lock(&self.shared).alive {
            return Err(HarnessError::Dead);
        }
        // A cancel must never split an unfinished JSON line. Keep the message
        // pending for a retry rather than claiming cancellation or delivery.
        if self.pending.is_some() {
            return Err(HarnessError::Failed("stdin delivery pending".into()));
        }
        let result = write_stdin(&self.stdin, line);
        let mut state = lock(&self.shared);
        if let Err(err) = result {
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Err(HarnessError::Failed("stdin backpressure".into()));
            }
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

    /// Write one line into the child and record it as taken.
    pub fn deliver(
        &mut self,
        message: &OutboundMessage,
        line: &Value,
    ) -> Result<DeliverOutcome, HarnessError> {
        let bytes = self
            .pending
            .is_none()
            .then(|| format!("{line}\n").into_bytes());
        {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            if let Some(pending) = &self.pending {
                if pending.id != message.id {
                    return Ok(DeliverOutcome::Deferred);
                }
            } else {
                if state.seen.contains(&message.id) {
                    return Ok(DeliverOutcome::Accepted {
                        turn_finished: state.finished.contains(&message.id),
                    });
                }
                if let Some(reset_at_ms) = state.quota_reset_at_ms {
                    return Ok(DeliverOutcome::Quota { reset_at_ms });
                }
                // Associate even an immediate result with its owner before the
                // first byte is visible to the child. This is not acceptance.
                if state.open.is_empty() {
                    state.turns_started += 1;
                    state.open.push(message.id.clone());
                } else {
                    state.followups.push(message.id.clone());
                }
                self.pending = Some(PendingWrite {
                    id: message.id.clone(),
                    bytes: bytes.expect("new write"),
                    offset: 0,
                    generation: state.generation,
                });
            }
        }
        // Never hold shared state across pipe I/O. On Linux stdin is
        // nonblocking: backpressure returns custody to the mailbox immediately.
        let pending = self.pending.as_mut().expect("reserved above");
        let result = write_pending(&self.stdin, pending);
        let mut state = lock(&self.shared);
        if state.generation != pending.generation || !state.alive {
            return Err(HarnessError::Dead);
        }
        match result {
            Ok(false) => {
                state.input_blocked = true;
                self.wake.notify_all();
                Ok(DeliverOutcome::Deferred)
            }
            Ok(true) => {
                state.input_blocked = false;
                let pending = self.pending.take().expect("completed write");
                state.seen.insert(pending.id.clone());
                state.written.push(pending.id);
                Ok(DeliverOutcome::Accepted {
                    turn_finished: state.finished.contains(&message.id),
                })
            }
            Err(_) => {
                state.alive = false;
                self.wake.notify_all();
                Err(HarnessError::Dead)
            }
        }
    }

    pub fn input_ready(&self) -> bool {
        !lock(&self.shared).input_blocked
    }

    pub fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let _ = thread::Builder::new()
            .name(format!("{}-watch", self.spec.name))
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
        // The seat the server configured for this agent: the child starts as
        // the agent's own uid, and no server-only key reaches it.
        let mut command = spawn_command(&self.program, &self.environment);
        command.args(&self.args);
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
        if let Err(err) = nonblocking_stdin(&child_stdin) {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
            return Err(HarnessError::Failed(err.to_string()));
        }
        let generation = {
            let mut state = lock(&self.shared);
            state.generation = state.generation.wrapping_add(1);
            // Pipe acceptance belongs to this process, never its replacement.
            state.input_blocked = false;
            state.seen.clear();
            state.written.clear();
            state.finished.clear();
            state.finished_quiet.clear();
            state.usages.clear();
            state.quota_reset_at_ms = None;
            state.child = Some(child);
            state.alive = true;
            state.open.clear();
            state.followups.clear();
            state.text = None;
            state.generation
        };
        self.pending = None;
        #[cfg(target_os = "linux")]
        {
            // A dup keeps poll's descriptor valid even while stop closes the
            // owner's copy. Killing our child wakes poll with POLLERR.
            use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
            let fd = unsafe { libc::fcntl(child_stdin.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
            if fd < 0 {
                self.stop_child();
                return Err(HarnessError::Failed(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let shared = Arc::clone(&self.shared);
            let wake = Arc::clone(&self.wake);
            self.readiness = match thread::Builder::new()
                .name(format!("{}-stdin", self.spec.name))
                .spawn(move || watch_writable(fd, shared, wake, generation))
            {
                Ok(handle) => Some(handle),
                Err(err) => {
                    self.stop_child();
                    return Err(HarnessError::Failed(err.to_string()));
                }
            };
        }
        *lock(&self.stdin) = Some(child_stdin);
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let spec = self.spec.clone();
        match thread::Builder::new()
            .name(spec.name.clone())
            .spawn(move || read_stdout(stdout, shared, wake, spec, generation))
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
        self.pending = None;
        *lock(&self.stdin) = None;
        let mut child = {
            let mut state = lock(&self.shared);
            state.alive = false;
            state.child.take()
        };
        self.wake.notify_all();
        if let Some(child) = child.as_mut() {
            anyagents_harness::kill_child(child, &self.environment);
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        #[cfg(target_os = "linux")]
        if let Some(handle) = self.readiness.take() {
            let _ = handle.join();
        }
        if let Some(mut child) = child {
            let _ = child.wait();
        }
        let mut state = lock(&self.shared);
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

impl State {
    fn new(session_id: Option<&str>) -> Self {
        Self {
            child: None,
            alive: false,
            epoch: 0,
            generation: 0,
            input_blocked: false,
            seen: HashSet::new(),
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
        }
    }
}

fn read_stdout(
    stdout: ChildStdout,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    spec: StreamSpec,
    generation: u64,
) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let effect = spec.effect(&value);
        {
            let mut state = lock(&shared);
            if state.generation != generation {
                return;
            }
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
                finish_open(&mut state, effect.usage.unwrap_or_default());
            }
            state.epoch = state.epoch.wrapping_add(1);
        }
        wake.notify_all();
    }
    {
        let mut state = lock(&shared);
        if state.generation != generation {
            return;
        }
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

fn finish_open(state: &mut State, usage: TurnUsage) {
    if state.open.is_empty() {
        return;
    }
    for id in state.open.drain(..) {
        state.finished.insert(id.clone());
        state.usages.push((id, usage.clone()));
    }
    if !state.followups.is_empty() {
        state.turns_started += 1;
        state.open.append(&mut state.followups);
    }
}

fn note_markers(state: &mut State, value: &Value) {
    match value {
        Value::String(text) => {
            if let Some(id) = message_id_in_text(text) {
                state.seen.insert(id.to_string());
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

// Event-driven pipe readiness, not a mailbox poller or delivery queue.
#[cfg(target_os = "linux")]
fn watch_writable(
    fd: std::os::fd::OwnedFd,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    generation: u64,
) {
    use std::os::fd::AsRawFd;
    loop {
        let state = lock(&shared);
        let state = wake
            .wait_while(state, |state| {
                state.alive && state.generation == generation && !state.input_blocked
            })
            .unwrap_or_else(|err| err.into_inner());
        if !state.alive || state.generation != generation {
            return;
        }
        drop(state);
        let mut poll = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut poll, 1, -1) };
        if result < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        let mut state = lock(&shared);
        if !state.alive || state.generation != generation {
            return;
        }
        state.input_blocked = false;
        if result < 0 || poll.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            state.alive = false;
        }
        state.epoch = state.epoch.wrapping_add(1);
        drop(state);
        wake.notify_all();
    }
}

#[cfg(target_os = "linux")]
fn nonblocking_stdin(stdin: &ChildStdin) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = stdin.as_raw_fd();
    // This descriptor is owned here, not inherited by the child as its stdin.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn nonblocking_stdin(_stdin: &ChildStdin) -> std::io::Result<()> {
    Ok(())
}

fn write_pending(
    slot: &Mutex<Option<ChildStdin>>,
    pending: &mut PendingWrite,
) -> std::io::Result<bool> {
    let mut guard = lock(slot);
    let stdin = guard
        .as_mut()
        .ok_or_else(|| std::io::Error::other("stdin closed"))?;
    while pending.offset < pending.bytes.len() {
        match stdin.write(&pending.bytes[pending.offset..]) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(count) => pending.offset += count,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(err) => return Err(err),
        }
    }
    stdin.flush()?;
    Ok(true)
}

fn write_stdin(slot: &Mutex<Option<ChildStdin>>, value: &Value) -> std::io::Result<()> {
    let mut guard = lock(slot);
    let Some(stdin) = guard.as_mut() else {
        return Err(std::io::Error::other("stdin closed"));
    };
    // One small control line must be one pipe write: write_fmt can split a
    // JSON value and leave a corrupt prefix behind on WouldBlock.
    let bytes = format!("{value}\n").into_bytes();
    #[cfg(target_os = "linux")]
    if bytes.len() > libc::PIPE_BUF {
        return Err(std::io::Error::other(
            "control line exceeds atomic pipe write",
        ));
    }
    stdin.write_all(&bytes)?;
    stdin.flush()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

fn number(value: Option<&Value>, key: &str) -> u64 {
    value
        .and_then(|item| item.get(key))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn quota_of(value: &Value) -> Option<i64> {
    if value.get("type").and_then(Value::as_str) != Some("rate_limit") {
        return None;
    }
    value.get("resets_at_ms").and_then(Value::as_i64)
}

/// The line that puts one message into the child.
pub fn message_line(spec: &StreamSpec, message: &OutboundMessage) -> Value {
    // The trusted header the server wrote, then the body: one composition
    // for every harness. Carries both top-level `type`/`text` and the
    // `event`/`message` envelope that `agy --input-format stream-json` decodes.
    let text = compose_message(&message.id, &message.context, &message.text);
    json!({
        "type": spec.start_line,
        "event": spec.start_line,
        "text": text,
        "message": {
            "role": "user",
            "content": text
        }
    })
}

/// The line that stops the current turn.
pub fn cancel_line(spec: &StreamSpec) -> Value {
    json!({
        "type": spec.cancel,
        "event": "control_request",
        "request": { "subtype": spec.cancel }
    })
}

#[cfg(test)]
mod tests;

/// Close every descriptor above stderr in the child just before it starts, so a
/// vendor never inherits the server's open files.
#[cfg(target_os = "linux")]
fn close_inherited_files(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    // `pre_exec` answers the command it was given: the call must land on it, so
    // the closure runs in the child just before `exec`. One `close_range` call
    // closes every descriptor above stderr; nothing else is touched.
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
