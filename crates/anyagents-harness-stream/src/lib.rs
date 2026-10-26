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
        let kind = value
            .get("type")
            .or_else(|| value.get("event"))
            .and_then(Value::as_str);
        let is_error = value
            .pointer("/result/status")
            .and_then(Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("ERROR"));
        let finishes = !is_error
            && match self.protocol {
                LineProtocol::Result | LineProtocol::ResultWithCost => kind == Some("result"),
                LineProtocol::Done => {
                    kind == Some("done")
                        || kind == Some("session_settled")
                        || (kind == Some("prompt_result")
                            && value.get("sessionSettled").and_then(Value::as_bool) != Some(false))
                }
            };
        LineEffect {
            finishes_turn: finishes,
            session_id: value
                .get("session_id")
                .or_else(|| value.get("sessionId"))
                .or_else(|| value.pointer("/data/sessionId"))
                .or_else(|| value.get("conversation_id"))
                .or_else(|| value.pointer("/result/conversation_id"))
                .or_else(|| value.pointer("/step_update/conversation_id"))
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
            .or_else(|| value.pointer("/result/usage"))
            .or_else(|| value.pointer("/message/usage"));
        TurnUsage {
            input_tokens: number(usage, "input_tokens").max(number(usage, "input")),
            output_tokens: number(usage, "output_tokens").max(number(usage, "output")),
            cache_read_tokens: number(usage, "cache_read_tokens").max(number(usage, "cacheRead")),
            cache_write_tokens: number(usage, "cache_write_tokens")
                .max(number(usage, "cacheWrite")),
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

/// Native `LanguageServerService` ConnectRPC configuration for Antigravity (`agy`).
#[derive(Clone, Debug)]
pub struct AgyRpcConfig {
    pub config_dir: PathBuf,
    pub csrf_token: String,
    pub port_override: Option<u16>,
}

/// One vendor child.
pub struct StreamHarness {
    spec: StreamSpec,
    program: PathBuf,
    args: Vec<String>,
    environment: Vec<(String, String)>,
    work_dir: Option<PathBuf>,
    agy_rpc: Option<AgyRpcConfig>,
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
    turn_user_inputs: usize,
    idle_since: Option<Instant>,
    planner_config: Option<Value>,
    http_port: Option<u16>,
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
        Self::launch_with_agy_rpc(launch, None)
    }

    /// Spawn the child with an optional Antigravity `LanguageServerService` RPC endpoint.
    pub fn launch_with_agy_rpc(
        launch: Launch,
        agy_rpc: Option<AgyRpcConfig>,
    ) -> Result<Self, HarnessError> {
        let mut harness = Self {
            spec: launch.spec,
            program: launch.program,
            args: launch.args,
            environment: launch.environment,
            work_dir: launch.work_dir,
            agy_rpc,
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

    pub fn turns_served(&self) -> u32 {
        lock(&self.shared).finished.len() as u32
    }

    pub fn served_usage(&self) -> TurnUsage {
        let state = lock(&self.shared);
        let mut total = TurnUsage::default();
        for (_, usage) in &state.usages {
            total.input_tokens += usage.input_tokens;
            total.output_tokens += usage.output_tokens;
            total.cache_read_tokens += usage.cache_read_tokens;
            total.cache_write_tokens += usage.cache_write_tokens;
            if let Some(cost) = usage.reported_cost_micros {
                *total.reported_cost_micros.get_or_insert(0) += cost;
            }
        }
        total
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
        if let Some(ref rpc) = self.agy_rpc {
            let (session_id, child_pid, cached_port) = {
                let state = lock(&self.shared);
                (
                    state.session_id.clone(),
                    state.child.as_ref().map(Child::id),
                    state.http_port,
                )
            };
            if let Some(session_id) = session_id
                && let Some(port) = resolve_agy_http_port(rpc, cached_port, child_pid)
            {
                lock(&self.shared).http_port = Some(port);
                agy_rpc_cancel(port, &rpc.csrf_token, &session_id).map_err(HarnessError::Failed)?;
                let mut state = lock(&self.shared);
                state.open.clear();
                state.followups.clear();
                state.turn_user_inputs = 0;
                state.idle_since = None;
                return Ok(());
            }
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
        state.turn_user_inputs = 0;
        state.idle_since = None;
        Ok(())
    }

    pub fn health(&self) -> HarnessHealth {
        let mut state = lock(&self.shared);
        if !state.alive
            && let Some(child) = state.child.as_mut()
            && matches!(child.try_wait(), Ok(Some(_)))
        {
            state.child = None;
        }
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
        let mut state = lock(&self.shared);
        if !state.alive
            && let Some(child) = state.child.as_mut()
            && matches!(child.try_wait(), Ok(Some(_)))
        {
            state.child = None;
        }
        state.child.as_ref().map(Child::id)
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
        let mut agy_steer_ctx = None;
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
                if let Some(reset_at_ms) = state.quota_reset_at_ms
                    && !state.finished.contains(&message.id)
                {
                    return Ok(DeliverOutcome::Quota { reset_at_ms });
                }
                if state.seen.contains(&message.id) {
                    return Ok(DeliverOutcome::Accepted {
                        turn_finished: state.finished.contains(&message.id),
                    });
                }
                if let Some(reset_at_ms) = state.quota_reset_at_ms {
                    return Ok(DeliverOutcome::Quota { reset_at_ms });
                }
                if !state.open.is_empty()
                    && let Some(ref rpc) = self.agy_rpc
                {
                    if state
                        .idle_since
                        .is_some_and(|t| t.elapsed() < AGY_TURN_CLOSE_GRACE)
                        || state.turn_user_inputs == 0
                        || state.session_id.is_none()
                    {
                        return Ok(DeliverOutcome::Deferred);
                    }
                    let session_id = state.session_id.clone().expect("checked above");
                    let child_pid = state.child.as_ref().map(Child::id);
                    let cached_port = state.http_port;
                    let cached_planner = state.planner_config.clone();
                    let generation = state.generation;
                    agy_steer_ctx = Some((
                        rpc.clone(),
                        session_id,
                        child_pid,
                        cached_port,
                        cached_planner,
                        generation,
                    ));
                } else {
                    // Associate even an immediate result with its owner before the
                    // first byte is visible to the child. This is not acceptance.
                    if state.open.is_empty() {
                        state.turns_started += 1;
                        state.open.push(message.id.clone());
                        state.turn_user_inputs = 0;
                        state.idle_since = None;
                    } else if self.mode == DeliveryMode::Steer {
                        if !state.open.contains(&message.id) {
                            state.open.push(message.id.clone());
                        }
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
        }
        if let Some((rpc, session_id, child_pid, cached_port, mut planner_config, generation)) =
            agy_steer_ctx
        {
            let Some(port) = resolve_agy_http_port(&rpc, cached_port, child_pid) else {
                let mut state = lock(&self.shared);
                if state.generation == generation && !state.open.is_empty() {
                    state.idle_since = Some(Instant::now());
                }
                return Ok(DeliverOutcome::Deferred);
            };
            {
                let mut state = lock(&self.shared);
                if state.generation != generation || !state.alive {
                    return Err(HarnessError::Dead);
                }
                state.http_port = Some(port);
            }
            let running = agy_rpc_is_running(port, &rpc.csrf_token, &session_id)
                .map_err(HarnessError::Failed)?;
            if running {
                let mut state = lock(&self.shared);
                if state.generation != generation || !state.alive {
                    return Err(HarnessError::Dead);
                }
                state.idle_since = None;
            } else {
                let mut state = lock(&self.shared);
                if state.generation != generation || !state.alive {
                    return Err(HarnessError::Dead);
                }
                if state.open.is_empty() {
                    state.idle_since = None;
                    return Ok(DeliverOutcome::Deferred);
                }
                let now = Instant::now();
                let first_idle = state.idle_since.is_none();
                let idle_since = *state.idle_since.get_or_insert(now);
                if now.duration_since(idle_since) < AGY_TURN_CLOSE_GRACE {
                    let remain =
                        AGY_TURN_CLOSE_GRACE.saturating_sub(now.duration_since(idle_since));
                    drop(state);
                    if first_idle {
                        let shared = Arc::clone(&self.shared);
                        let wake = Arc::clone(&self.wake);
                        let _ = thread::spawn(move || {
                            thread::sleep(remain);
                            let mut st = lock(&shared);
                            if st.generation == generation && st.alive {
                                st.epoch = st.epoch.wrapping_add(1);
                            }
                            drop(st);
                            wake.notify_all();
                        });
                    }
                    return Ok(DeliverOutcome::Deferred);
                }
                drop(state);
                if planner_config.is_none() {
                    planner_config =
                        agy_rpc_fetch_planner_config(port, &rpc.csrf_token, &session_id)
                            .map_err(HarnessError::Failed)?;
                    let mut state = lock(&self.shared);
                    if state.generation != generation || !state.alive {
                        return Err(HarnessError::Dead);
                    }
                    if planner_config.is_some() {
                        state.planner_config = planner_config.clone();
                    }
                }
            }
            let composed = compose_message(&message.id, &message.context, &message.text);
            {
                let mut state = lock(&self.shared);
                if state.generation != generation || !state.alive {
                    return Err(HarnessError::Dead);
                }
                if state.open.is_empty() {
                    return Ok(DeliverOutcome::Deferred);
                }
                if !running {
                    // The previous turn in `agy` already finished (`CASCADE_RUN_STATUS_IDLE`)
                    // while held open by a background task. Close those open messages now
                    // so `turn_recovery.completed_seq` advances and never replays stale
                    // completed turns on restart.
                    finish_open(&mut state, TurnUsage::default());
                    state.turns_started += 1;
                }
                if !state.open.contains(&message.id) {
                    state.open.push(message.id.clone());
                }
            }
            if let Err(err) = agy_rpc_send_steer(
                port,
                &rpc.csrf_token,
                &session_id,
                &composed,
                planner_config.as_ref(),
            ) {
                let mut state = lock(&self.shared);
                state.open.retain(|id| id != &message.id);
                if err.contains("plan model not specified") {
                    if !state.open.is_empty() {
                        state.idle_since = Some(Instant::now());
                    }
                    return Ok(DeliverOutcome::Deferred);
                }
                return Err(HarnessError::Failed(err));
            }
            let mut state = lock(&self.shared);
            if state.generation != generation || !state.alive {
                return Err(HarnessError::Dead);
            }
            state.idle_since = None;
            state.turn_user_inputs = state.turn_user_inputs.saturating_add(1);
            state.seen.insert(message.id.clone());
            state.written.push(message.id.clone());
            return Ok(DeliverOutcome::Accepted {
                turn_finished: !running || state.finished.contains(&message.id),
            });
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
        let state = lock(&self.shared);
        if state.input_blocked {
            return false;
        }
        if self.agy_rpc.is_some()
            && !state.open.is_empty()
            && (state
                .idle_since
                .is_some_and(|t| t.elapsed() < AGY_TURN_CLOSE_GRACE)
                || state.turn_user_inputs == 0
                || state.session_id.is_none())
        {
            return false;
        }
        true
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
            state.turn_user_inputs = 0;
            state.idle_since = None;
            state.planner_config = None;
            state.http_port = self.agy_rpc.as_ref().and_then(|r| r.port_override);
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
        let child = {
            let mut state = lock(&self.shared);
            state.alive = false;
            state.child.take()
        };
        self.wake.notify_all();
        if let Some(mut child) = child {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        #[cfg(target_os = "linux")]
        if let Some(handle) = self.readiness.take() {
            let _ = handle.join();
        }
        let mut state = lock(&self.shared);
        state.open.clear();
        state.followups.clear();
        state.turn_user_inputs = 0;
        state.idle_since = None;
        state.planner_config = None;
        state.http_port = None;
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
            turn_user_inputs: 0,
            idle_since: None,
            planner_config: None,
            http_port: None,
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
            state.idle_since = None;
            note_markers(&mut state, &value);
            if value
                .pointer("/step_update/step_type")
                .and_then(Value::as_str)
                == Some("user_input")
            {
                state.turn_user_inputs = state.turn_user_inputs.saturating_add(1);
            }
            if let Some(reset_at_ms) = effect.quota_reset_at_ms {
                state.quota_reset_at_ms = Some(reset_at_ms);
                let rejected_ids = std::mem::take(&mut state.open);
                for id in rejected_ids {
                    state.seen.remove(&id);
                }
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
                let usage = effect.usage.unwrap_or_default();
                finish_open(&mut state, usage);
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
        if let Some(child) = state.child.as_mut()
            && matches!(child.try_wait(), Ok(Some(_)))
        {
            state.child = None;
        }
        state.epoch = state.epoch.wrapping_add(1);
    }
    wake.notify_all();
    for _ in 0..10 {
        {
            let mut state = lock(&shared);
            if state.generation != generation || state.child.is_none() {
                break;
            }
            if let Some(child) = state.child.as_mut()
                && matches!(child.try_wait(), Ok(Some(_)))
            {
                state.child = None;
                break;
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
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
    state.turn_user_inputs = 0;
    state.idle_since = None;
    state.finished.insert(id.to_string());
    state.text = text;
}

fn finish_open(state: &mut State, usage: TurnUsage) {
    state.turn_user_inputs = 0;
    state.idle_since = None;
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
    let bytes = format!("{value}\n").into_bytes();
    stdin.write_all(&bytes)?;
    stdin.flush()
}

fn resolve_agy_http_port(
    rpc: &AgyRpcConfig,
    cached: Option<u16>,
    child_pid: Option<u32>,
) -> Option<u16> {
    if let Some(port) = rpc.port_override.or(cached) {
        return Some(port);
    }
    let log_path = rpc.config_dir.join(".gemini/antigravity-cli/cli.log");
    let text = std::fs::read_to_string(&log_path).ok()?;
    let pid_needle =
        child_pid.map(|pid| format!("Starting language server process with pid {pid}"));
    let mut pid_matched = pid_needle.is_none();
    let mut found_port = None;
    for line in text.lines() {
        if let Some(ref needle) = pid_needle
            && line.contains(needle)
        {
            pid_matched = true;
            found_port = None;
            continue;
        }
        if pid_matched
            && let Some((_, after)) =
                line.split_once("Language server listening on random port at ")
            && let Some(port_str) = after.strip_suffix(" for HTTP")
            && let Ok(port) = port_str.trim().parse::<u16>()
        {
            found_port = Some(port);
        }
    }
    found_port
}

fn agy_rpc_post(
    port: u16,
    csrf_token: &str,
    method: &str,
    payload: &Value,
) -> Result<Value, String> {
    use std::io::Read as _;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};

    let body = serde_json::to_vec(payload).map_err(|err| err.to_string())?;
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        .map_err(|err| format!("connect 127.0.0.1:{port}: {err}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut request = format!(
        "POST /exa.language_server_pb.LanguageServerService/{method} HTTP/1.0\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\n\
         x-codeium-csrf-token: {csrf_token}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    stream
        .write_all(&request)
        .and_then(|_| stream.flush())
        .map_err(|err| format!("write {method}: {err}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| format!("read {method}: {err}"))?;
    let (head, resp_body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("malformed HTTP response from {method}"))?;
    let status_line = head.lines().next().unwrap_or_default();
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status_code) {
        return Err(format!("{method} HTTP {status_code}: {}", resp_body.trim()));
    }
    if resp_body.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str::<Value>(resp_body.trim()).map_err(|err| format!("decode {method}: {err}"))
}

/// Grace window when an `agy` cascade transitions to `CASCADE_RUN_STATUS_IDLE`.
/// Normal turns emit `{"event":"result"}` on stdout within ~100ms (`pollResponse`
/// ticker). Cascades held open by an active background task (e.g. `schedule`
/// timer via `isRootIdleWithTasks`) never emit `result` while idle and instead
/// accept a new invocation via `SendUserCascadeMessage` with `plannerConfig`.
const AGY_TURN_CLOSE_GRACE: Duration = Duration::from_millis(200);

fn agy_rpc_is_running(port: u16, csrf_token: &str, session_id: &str) -> Result<bool, String> {
    let resp = agy_rpc_post(port, csrf_token, "GetAllCascadeTrajectories", &json!({}))?;
    let status = resp
        .get("trajectorySummaries")
        .and_then(|m| m.get(session_id))
        .and_then(|s| s.get("status"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    Ok(status == "CASCADE_RUN_STATUS_RUNNING")
}

fn agy_rpc_fetch_planner_config(
    port: u16,
    csrf_token: &str,
    session_id: &str,
) -> Result<Option<Value>, String> {
    let resp = agy_rpc_post(
        port,
        csrf_token,
        "GetCascadeTrajectoryGeneratorMetadata",
        &json!({ "cascadeId": session_id }),
    )?;
    Ok(resp
        .get("generatorMetadata")
        .and_then(Value::as_array)
        .and_then(|items| items.last())
        .and_then(|meta| meta.get("plannerConfig"))
        .cloned())
}

fn agy_rpc_send_steer(
    port: u16,
    csrf_token: &str,
    session_id: &str,
    text: &str,
    planner_config: Option<&Value>,
) -> Result<(), String> {
    let cascade_config = match planner_config {
        Some(cfg) => json!({ "plannerConfig": cfg }),
        None => json!({}),
    };
    let payload = json!({
        "cascadeId": session_id,
        "items": [{"text": text}],
        "cascadeConfig": cascade_config,
        "blocking": false,
        "deliveryStrategy": "MESSAGE_DELIVERY_STRATEGY_NEXT_INVOCATION"
    });
    agy_rpc_post(port, csrf_token, "SendUserCascadeMessage", &payload).map(|_| ())
}

fn agy_rpc_cancel(port: u16, csrf_token: &str, session_id: &str) -> Result<(), String> {
    let payload = json!({
        "cascadeId": session_id
    });
    agy_rpc_post(port, csrf_token, "CancelCascadeInvocation", &payload).map(|_| ())
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
    if value.get("type").and_then(Value::as_str) == Some("rate_limit") {
        return value.get("resets_at_ms").and_then(Value::as_i64);
    }
    if let Some(err_text) = value
        .pointer("/result/error")
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        && let Some(delay_ms) = parse_resets_in_ms(err_text)
    {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        return Some(now_ms.saturating_add(delay_ms));
    }
    None
}

fn parse_resets_in_ms(text: &str) -> Option<i64> {
    let lower = text.to_ascii_lowercase();
    let idx = lower.find("resets in ")?;
    let after = &lower[idx + "resets in ".len()..];
    let token = after
        .split_whitespace()
        .next()?
        .trim_end_matches(|c: char| !c.is_ascii_alphanumeric());
    if token.is_empty() {
        return None;
    }
    let mut total_ms: i64 = 0;
    let mut num: i64 = 0;
    let mut has_digits = false;
    let mut matched_unit = false;
    for ch in token.chars() {
        if let Some(d) = ch.to_digit(10) {
            num = num.saturating_mul(10).saturating_add(i64::from(d));
            has_digits = true;
        } else if has_digits {
            let factor = match ch {
                'd' => 86_400_000,
                'h' => 3_600_000,
                'm' => 60_000,
                's' => 1_000,
                _ => return None,
            };
            total_ms = total_ms.saturating_add(num.saturating_mul(factor));
            num = 0;
            has_digits = false;
            matched_unit = true;
        } else {
            return None;
        }
    }
    (matched_unit && !has_digits && total_ms > 0).then_some(total_ms)
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
