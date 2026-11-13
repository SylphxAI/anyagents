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

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, DeliveryMode, FileStamp, HarnessError, HarnessHealth,
    OutboundMessage, Proof, TurnState, TurnUsage, compose_message, message_id_in_text,
    spawn_command,
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
    /// A persistent controller acknowledges each correlated input before the
    /// mailbox releases custody. `result` is the only terminal frame.
    AcknowledgedResult,
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
            // The adapter contract (`docs/architecture.md`): one JSON line on
            // stdin per message, a `result` line when its turn ends. The
            // contract double and an `adapter.json` program speak it.
            _ => Self {
                name: format!("anyagents-{program}"),
                args: Vec::new(),
                resume_flag: "--session",
                start_line: "user",
                cancel: "cancel",
                protocol: LineProtocol::Result,
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
        let has_error_payload = matches!(kind, Some("done") | Some("result"))
            && value
                .pointer("/result/error")
                .or_else(|| value.get("error"))
                .and_then(|v| {
                    v.as_str().or_else(|| {
                        v.get("message")
                            .and_then(Value::as_str)
                            .or_else(|| v.get("code").and_then(Value::as_str))
                    })
                })
                .is_some_and(|s| !s.trim().is_empty());
        let is_error = value
            .pointer("/result/status")
            .and_then(Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("ERROR"))
            || has_error_payload;
        let quota_reset_at_ms = quota_of(value);
        let finishes = quota_reset_at_ms.is_none()
            && match self.protocol {
                LineProtocol::AcknowledgedResult => kind == Some("result"),
                LineProtocol::Result => {
                    kind == Some("result") && (!is_error || self.name == "anyagents-agy")
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
            quota_reset_at_ms,
            text: self.text_of(value),
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
            cache_read_tokens: number(usage, "cache_read_tokens")
                .max(number(usage, "cacheRead"))
                .max(number(usage, "cache_read_input_tokens")),
            cache_write_tokens: number(usage, "cache_write_tokens")
                .max(number(usage, "cacheWrite"))
                .max(number(usage, "cache_creation_input_tokens")),
            reported_cost_micros: None,
        }
    }

    /// Answer text: only Spiron streams it, as `text_delta` lines.
    fn text_of(&self, value: &Value) -> Option<String> {
        if self.protocol != LineProtocol::AcknowledgedResult {
            return None;
        }
        (value.get("type").and_then(Value::as_str) == Some("text_delta"))
            .then(|| value.get("text").or_else(|| value.get("delta")))
            .flatten()
            .and_then(Value::as_str)
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
    /// What this child guarantees once it announced itself.
    pub delivery: Delivery,
}

/// A child that reads one line per turn: mail for a busy agent waits for the
/// turn to end.
pub const LINE_PER_TURN: Delivery = Delivery {
    level: DeliveryLevel::TurnEnd,
    idempotent: false,
    proof: std::borrow::Cow::Borrowed("one input line per turn"),
    peer: None,
};

/// A child that takes every line into the running turn; the written line is
/// the only proof.
pub const LINE_INTO_TURN: Delivery = Delivery {
    level: DeliveryLevel::MidTurn,
    idempotent: false,
    proof: std::borrow::Cow::Borrowed("input line written"),
    peer: None,
};

/// A Spiron peer whose `ready` names no consumption proof: it is given one
/// input per turn, never one while a turn runs, and `accepted` commits it.
/// Gated on the capability for peers built before it; it goes when no such
/// Spiron is installed.
const OLDER_SPIRON: Delivery = Delivery {
    level: DeliveryLevel::TurnEnd,
    idempotent: false,
    proof: std::borrow::Cow::Borrowed("accepted (one input per turn)"),
    peer: None,
};

/// Spiron's headless peer when its `ready` frame names `steer_consumed` and
/// `idempotent_input`.
pub const SPIRON: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed("accepted/steer_consumed receipts; request_id journal"),
    peer: None,
};

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
    delivery: Delivery,
    shared: Arc<Mutex<State>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
    pending: Option<PendingWrite>,
    #[cfg(test)]
    after_write: Option<Box<dyn FnOnce() + Send>>,
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
    stderr: BoundedStderr,
    stderr_done: bool,
    exit_status: Option<ExitStatus>,
    exit_fault: Option<String>,
    state_dir: Option<PathBuf>,
    alive: bool,
    /// Bumped by every stdout line: what [`StreamHarness::quiet`] measures.
    epoch: u64,
    /// Bumped only when something the host reads changes: what the activity
    /// watcher waits on.
    changes: u64,
    /// When this child last printed a line.
    last_line_at: Option<Instant>,
    generation: u64,
    input_blocked: bool,
    turn_user_inputs: usize,
    last_turn_step_type: Option<String>,
    idle_since: Option<Instant>,
    last_rpc_health_poll: Option<Instant>,
    last_steer_probe: Option<Instant>,
    planner_config: Option<Value>,
    http_port: Option<u16>,
    /// The one Antigravity steer under way, and once the language server
    /// settled it, whether it queued the message.
    agy_steer: Option<AgySteer>,
    /// The Antigravity turn (its `turns_started`) whose cascade refused a
    /// steer: it is given no more, and the next input waits for its end.
    agy_refused_turn: Option<usize>,
    /// Messages a cascade's durable mailbox holds that the cascade has not
    /// shown taking: the language server queued them for this process, or a
    /// resumed cascade still held them from an earlier one. Its answer to
    /// `SendUserCascadeMessage` is no proof: a cascade whose run fails never
    /// takes what waits in its mailbox.
    agy_queued: Vec<AgyQueued>,
    /// Messages a cascade took: a `USER_INPUT` row of its transcript, or its
    /// mailbox's `read.json`, names them, or a turn this process wrote on
    /// stdin showed its first step or closed. A proof about the session, so
    /// a new process keeps it.
    agy_taken: HashSet<String>,
    /// The session's cascade was retired (its record is poisoned): no later
    /// line names it again.
    session_retired: bool,
    /// stdin broke: the process takes no more lines, and its stdout's end,
    /// read to the last proof, reports it gone.
    stdin_broken: bool,
    seen: HashSet<String>,
    acknowledged: HashSet<String>,
    input_turns: HashMap<String, String>,
    rejected: HashMap<String, String>,
    written: Vec<String>,
    open: Vec<String>,
    followups: Vec<String>,
    finished: HashSet<String>,
    turns_started: usize,
    quota_reset_at_ms: Option<i64>,
    sign_in_fault: Option<String>,
    session_id: Option<String>,
    usages: Vec<(String, TurnUsage)>,
    /// The text lines of the open turn so far, and the id they were read under.
    text: Option<(String, Vec<String>)>,
    /// The last open turn ended on quiet rather than on a result line.
    finished_quiet: HashSet<String>,
    /// What this process's `ready` frame announced: `None` until it is read,
    /// then whether the peer proves consumption and deduplicates input.
    ready: Option<bool>,
    /// The protocol and capabilities that frame named.
    peer: Option<String>,
    /// Inputs written whole into a peer that proves consumption, which it has
    /// neither proved consumed nor refused yet.
    in_flight: HashSet<String>,
    /// Inputs that peer proved are in its session journal.
    consumed: HashSet<String>,
    /// Consumed inputs it answered as journaled by an earlier process.
    duplicates: HashSet<String>,
    /// Inputs the session's own journal shows an earlier process applied.
    /// They belong to the session, so a new process keeps them.
    applied_before: HashSet<String>,
    /// The open turn refused a steer as closed: it takes no more, and the
    /// next input waits for its result to start the next turn.
    turn_closing: bool,
    /// Turns in a row the peer could not start (`turn_not_started`), and
    /// when the next one may be offered.
    refused_turns: u32,
    turn_retry_at: Option<Instant>,
    /// When the first of those refusals came, and the reason the last one
    /// gave.
    turn_refused_since: Option<Instant>,
    turn_refusal: Option<String>,
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
            delivery: launch.delivery,
            shared: Arc::new(Mutex::new(State::new(launch.session_id.as_deref()))),
            stdin: Arc::new(Mutex::new(None)),
            wake: Arc::new(Condvar::new()),
            reader: None,
            stderr_reader: None,
            pending: None,
            #[cfg(test)]
            after_write: None,
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
            delivery: LINE_PER_TURN,
        })
    }

    /// Persist the child's exit fault into `dir` for the server to show.
    pub fn set_state_dir(&self, dir: &Path) {
        let mut state = lock(&self.shared);
        state.state_dir = Some(dir.to_path_buf());
        state.refresh_exit_fault();
        state.persist_failure();
    }

    pub fn program(&self) -> &Path {
        &self.program
    }

    pub fn written_ids(&self) -> Vec<String> {
        lock(&self.shared).written.clone()
    }

    pub fn state_dir(&self) -> Option<PathBuf> {
        lock(&self.shared).state_dir.clone()
    }

    /// Inputs the session's journal shows an earlier process applied: each
    /// is answered `Duplicate` and never written again.
    pub fn recognise_applied(&self, ids: impl IntoIterator<Item = String>) {
        lock(&self.shared).applied_before.extend(ids);
    }

    pub fn seed_session_id(&self, session_id: &str) {
        let trimmed = session_id.trim();
        if !trimmed.is_empty() {
            let mut state = lock(&self.shared);
            if state.session_id.is_none() {
                state.session_id = Some(trimmed.to_string());
            }
        }
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
                let before = state.observed();
                finish_open_quiet(&mut state);
                if state.observed() != before {
                    state.changes = state.changes.wrapping_add(1);
                }
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
        if self.spec.protocol == LineProtocol::AcknowledgedResult && !state.finished.contains(id) {
            return None;
        }
        state
            .text
            .as_ref()
            .filter(|(owner, _)| owner == id)
            .map(|(_, lines)| {
                let joined =
                    lines.join(if self.spec.protocol == LineProtocol::AcknowledgedResult {
                        ""
                    } else {
                        "\n"
                    });
                anyagents_harness::masking::mask_known_secrets(&joined).into_owned()
            })
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
                state.last_turn_step_type = None;
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
            // The child closed its input: it is going, and its stdout's end,
            // read to the last line, reports it gone.
            state.stdin_broken = true;
            state.touch();
            self.wake.notify_all();
            return Err(HarnessError::Dead);
        }
        if self.spec.protocol != LineProtocol::AcknowledgedResult {
            state.open.clear();
            state.followups.clear();
        }
        state.turn_user_inputs = 0;
        state.last_turn_step_type = None;
        state.idle_since = None;
        Ok(())
    }

    /// What this harness already proved about `id`, whether or not its
    /// process still runs, without writing anything (`Harness::proven`). A
    /// read of the state the stdout reader and the cascade watcher keep: it
    /// does no I/O.
    pub fn proven(&self, id: &str) -> Option<Proof> {
        let state = lock(&self.shared);
        state.proof_of(self.spec.protocol, self.agy_rpc.is_some(), id)
    }

    /// A read of the child's state. It never does I/O: an Antigravity
    /// cascade that stays open without a closing line is watched by its own
    /// turn timer ([`watch_agy_cascade`]), not by how often the host asks.
    pub fn health(&self) -> HarnessHealth {
        let mut state = lock(&self.shared);
        if !state.alive {
            state.reap_child();
        }
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.open.is_empty() && state.in_flight.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery: self.delivery_now(&state),
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: state.failure(),
        }
    }

    /// What this child guarantees as it announced itself. A Spiron peer whose
    /// `ready` names no consumption proof keeps the older custody rules.
    fn delivery_now(&self, state: &State) -> Delivery {
        let delivery = if self.spec.protocol == LineProtocol::AcknowledgedResult
            && state.ready == Some(false)
        {
            OLDER_SPIRON
        } else {
            self.delivery.clone()
        };
        delivery.with_peer(state.peer.clone())
    }

    pub fn clear_quota(&self) {
        let mut state = lock(&self.shared);
        state.quota_reset_at_ms = None;
        state.sign_in_fault = None;
    }

    pub fn process_id(&self) -> Option<u32> {
        let mut state = lock(&self.shared);
        if !state.alive {
            state.reap_child();
        }
        state.child.as_ref().map(Child::id)
    }

    /// Write one line into the child and record it as taken.
    ///
    /// What the vendor already proved about the message answers first,
    /// before liveness or a quota: a proof outlives the process that gave
    /// it. A refusal for good is answered again for the same id.
    pub fn deliver(
        &mut self,
        message: &OutboundMessage,
        line: &Value,
    ) -> Result<DeliverOutcome, HarnessError> {
        let id = message.id.as_str();
        if let Some(proof) = self.proven(id) {
            return Ok(proof_outcome(proof));
        }
        if let Some(reason) = lock(&self.shared).rejected.get(id) {
            return Ok(DeliverOutcome::Rejected {
                reason: reason.clone(),
            });
        }
        if self.spec.protocol == LineProtocol::AcknowledgedResult {
            let state = lock(&self.shared);
            // The peer says what it can do before it is given anything: its
            // `ready` decides which custody rules every input follows.
            if state.ready.is_none() && state.alive {
                return Ok(DeliverOutcome::Deferred);
            }
            if state.ready == Some(true) {
                drop(state);
                return self.deliver_consumed(message, line);
            }
            // An older peer proves nothing about input it takes mid-turn: it
            // is given the next input only once the running turn has ended.
            let unwritten = self.pending.as_ref().is_none_or(|p| p.id != id);
            if unwritten && !state.seen.contains(id) && !state.open.is_empty() {
                return Ok(DeliverOutcome::Deferred);
            }
        }
        if self.agy_rpc.is_some() {
            let mut state = lock(&self.shared);
            // A settled steer is judged by what the cascade holds or took
            // (queued), or afresh (returned).
            if state
                .agy_steer
                .as_ref()
                .is_some_and(|steer| steer.outcome.is_some())
            {
                state.agy_steer = None;
            }
            // A steer under way is in flight until the language server
            // settles it; one its cascade holds, until the cascade takes it.
            if state.agy_steer.as_ref().is_some_and(|steer| steer.id == id) {
                return Ok(DeliverOutcome::InFlight);
            }
            if state.agy_queued.iter().any(|queued| queued.id == id) {
                return if state.alive {
                    Ok(DeliverOutcome::InFlight)
                } else {
                    Err(HarnessError::Dead)
                };
            }
        }
        let bytes = self
            .pending
            .is_none()
            .then(|| format!("{line}\n").into_bytes());
        let mut agy_steer = None;
        {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            if let Some(pending) = &self.pending {
                if pending.id != id {
                    return Ok(DeliverOutcome::Deferred);
                }
            } else {
                if state.stdin_broken {
                    return Ok(DeliverOutcome::Deferred);
                }
                if let Some(reset_at_ms) = state.quota_reset_at_ms {
                    return Ok(DeliverOutcome::Quota { reset_at_ms });
                }
                // Written and not proven yet: an Antigravity prompt until its
                // cascade shows the turn, an older Spiron's until `accepted`.
                if state.seen.contains(id) {
                    return Ok(if self.agy_rpc.is_some() {
                        DeliverOutcome::InFlight
                    } else if self.spec.protocol == LineProtocol::AcknowledgedResult {
                        DeliverOutcome::Deferred
                    } else {
                        DeliverOutcome::Accepted {
                            turn_finished: state.finished.contains(id),
                        }
                    });
                }
                // One Antigravity steer at a time, and nothing written past
                // it until it is settled: mail reaches the cascade in order.
                if state.agy_steer.is_some() {
                    return Ok(DeliverOutcome::Deferred);
                }
                if let Some(rpc) = &self.agy_rpc
                    && agy_steers(&state)
                {
                    if let Some(reopens_in) = agy_input_gate(&state) {
                        // A gate that opens with time, not with a line, wakes
                        // the host when it does.
                        if let Some(remain) = reopens_in {
                            nudge_after(&self.shared, &self.wake, state.generation, remain);
                        }
                        return Ok(DeliverOutcome::Deferred);
                    }
                    state.agy_steer = Some(AgySteer {
                        id: id.to_string(),
                        outcome: None,
                        sent: None,
                    });
                    agy_steer = Some(AgySteerJob {
                        rpc: rpc.clone(),
                        id: id.to_string(),
                        text: compose_message(id, &message.context, &message.text),
                        session_id: state.session_id.clone().unwrap_or_default(),
                        child_pid: state.child.as_ref().map(Child::id),
                        cached_port: state.http_port,
                        planner_config: state.planner_config.clone(),
                        generation: state.generation,
                    });
                } else {
                    // Associate even an immediate result with its owner before the
                    // first byte is visible to the child. This is not acceptance.
                    if state.open.is_empty() {
                        state.turns_started += 1;
                        state.open.push(id.to_string());
                        state.turn_user_inputs = 0;
                        state.idle_since = None;
                        state.sign_in_fault = None;
                    } else if self.mode == DeliveryMode::Steer {
                        if !state.open.iter().any(|open| open == id) {
                            state.open.push(id.to_string());
                        }
                    } else {
                        state.followups.push(id.to_string());
                    }
                    self.pending = Some(PendingWrite {
                        id: id.to_string(),
                        bytes: bytes.expect("new write"),
                        offset: 0,
                        generation: state.generation,
                    });
                }
            }
        }
        if let Some(job) = agy_steer {
            return Ok(agy_start_steer(
                &self.shared,
                &self.wake,
                &self.spec.name,
                job,
            ));
        }
        // Never hold shared state across pipe I/O. On Linux stdin is
        // nonblocking: backpressure returns custody to the mailbox immediately.
        let pending = self.pending.as_mut().expect("reserved above");
        let result = write_pending(&self.stdin, pending);
        // Tests can hold the writer here until the real stdout reader has
        // processed a receipt, making the earliest-receipt interleaving exact.
        #[cfg(test)]
        if let Some(after_write) = self.after_write.take() {
            after_write();
        }
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
                drop(state);
                self.deliver(message, line)
            }
            // The line never reached the child whole, so it was not written:
            // it stays in the mailbox. The child is going; its stdout's end
            // says so once every line it wrote before is read.
            Err(_) => {
                let pending = self.pending.take().expect("failed write");
                state.open.retain(|open| *open != pending.id);
                state.followups.retain(|open| *open != pending.id);
                state.stdin_broken = true;
                state.touch();
                self.wake.notify_all();
                Ok(DeliverOutcome::Deferred)
            }
        }
    }

    /// Delivery to a peer that proves consumption (`steer_consumed`) and
    /// deduplicates input by its request id (`idempotent_input`).
    ///
    /// Every input is written at once: a `prompt` when no turn is open, a
    /// `steer` into the open one, so the peer takes all pending input
    /// together, in order, at its next step. The answer is `InFlight` until
    /// the peer proves the input is in its journal (the turn's `accepted`, a
    /// `steer_consumed`), then `Accepted`, or `Duplicate` when an earlier
    /// process had already journaled it. An input the peer refuses or ends
    /// its turn without consuming returns to the mailbox and is written again;
    /// it was certainly not journaled, and a second copy would be dropped by
    /// its request id anyway. Nothing waits for a turn to end.
    fn deliver_consumed(
        &mut self,
        message: &OutboundMessage,
        line: &Value,
    ) -> Result<DeliverOutcome, HarnessError> {
        let id = message.id.as_str();
        {
            let mut state = lock(&self.shared);
            // A consumption the peer proved stays proved after it exits, and
            // a refusal for good stays one.
            if let Some(Ok(
                settled @ (DeliverOutcome::Accepted { .. }
                | DeliverOutcome::Duplicate
                | DeliverOutcome::Rejected { .. }),
            )) = state.consumption_outcome(id)
            {
                return Ok(settled);
            }
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            // The pipe finishes the line it started before any other input
            // is judged: a half-written line blocks every line behind it.
            if self.pending.is_none() {
                if let Some(outcome) = state.consumption_outcome(id) {
                    return outcome;
                }
                if let Some(reset_at_ms) = state.quota_reset_at_ms {
                    return Ok(DeliverOutcome::Quota { reset_at_ms });
                }
                let starts_turn = state.open.is_empty();
                // A turn starts only once every earlier input has its answer,
                // so a line still on its way to a closed turn cannot land
                // behind the next one.
                if starts_turn && !state.in_flight.is_empty() {
                    return Ok(DeliverOutcome::Deferred);
                }
                // A turn that refused a steer as closed takes no more; its
                // result, already on its way, opens the way to the next turn.
                if !starts_turn && state.turn_closing {
                    return Ok(DeliverOutcome::Deferred);
                }
                // A peer that could not start the last turn is asked again
                // when its backoff ends; the reader wakes the host then.
                if starts_turn
                    && state
                        .turn_retry_at
                        .is_some_and(|retry_at| Instant::now() < retry_at)
                {
                    return Ok(DeliverOutcome::Deferred);
                }
                // A session lease a dead process left lapses within
                // `SPIRON_LEASE`; turns that still cannot start after it are
                // a fault of this process, not a wait.
                if starts_turn
                    && let Some(since) = state.turn_refused_since
                    && since.elapsed() >= SPIRON_LEASE
                {
                    return Err(HarnessError::Failed(format!(
                        "spiron could not start a turn for {}s: {}",
                        since.elapsed().as_secs(),
                        state.turn_refusal.as_deref().unwrap_or("turn_not_started")
                    )));
                }
                if state.stdin_broken {
                    return Ok(DeliverOutcome::Deferred);
                }
                let mut line = line.clone();
                line["type"] = json!(if starts_turn { "prompt" } else { "steer" });
                line["request_id"] = json!(id);
                if starts_turn {
                    state.turns_started += 1;
                    state.turn_user_inputs = 0;
                    state.idle_since = None;
                    state.sign_in_fault = None;
                    state.turn_closing = false;
                }
                // The owner before the first byte is visible: an immediate
                // receipt finds it. This is not consumption.
                state.open.push(id.to_string());
                state.in_flight.insert(id.to_string());
                self.pending = Some(PendingWrite {
                    id: id.to_string(),
                    bytes: format!("{line}\n").into_bytes(),
                    offset: 0,
                    generation: state.generation,
                });
            }
        }
        let pending = self.pending.as_mut().expect("reserved above");
        let result = write_pending(&self.stdin, pending);
        #[cfg(test)]
        if let Some(after_write) = self.after_write.take() {
            after_write();
        }
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
                let written = self.pending.take().expect("completed write").id;
                state.seen.insert(written.clone());
                state.written.push(written.clone());
                if written != id {
                    // An earlier line finished; this one is judged afresh.
                    drop(state);
                    return self.deliver_consumed(message, line);
                }
                // Refused before this lock: it goes back to the mailbox.
                state
                    .consumption_outcome(id)
                    .unwrap_or(Ok(DeliverOutcome::Deferred))
            }
            // The line never reached the peer whole, so it was not written:
            // it goes back to the mailbox. The peer is going; its stdout's
            // end says so once every receipt it wrote before is read.
            Err(_) => {
                let failed = self.pending.take().expect("failed write").id;
                release_input(&mut state, &failed);
                state.stdin_broken = true;
                state.touch();
                self.wake.notify_all();
                Ok(DeliverOutcome::Deferred)
            }
        }
    }

    /// Wait until the peer's `ready` frame is read or the process exits.
    /// A peer that says nothing within `patience` is refused.
    pub fn wait_ready(&self, patience: Duration) -> Result<(), HarnessError> {
        if self.wait_pred(patience, |state| state.ready.is_some() || !state.alive) {
            return Ok(());
        }
        Err(HarnessError::Failed(format!(
            "{} sent no ready frame within {}s",
            self.spec.name,
            patience.as_secs()
        )))
    }

    /// The peer proves consumption and deduplicates input, so every input is
    /// steered at once and replayed freely.
    pub fn proves_consumption(&self) -> bool {
        lock(&self.shared).ready == Some(true)
    }

    pub fn input_ready(&self) -> bool {
        let state = lock(&self.shared);
        if state.input_blocked || state.stdin_broken {
            return false;
        }
        if self.spec.protocol == LineProtocol::AcknowledgedResult
            && (state.ready.is_none()
                || (state.open.is_empty()
                    && state
                        .turn_retry_at
                        .is_some_and(|retry_at| Instant::now() < retry_at)))
        {
            return false;
        }
        // An Antigravity prompt its cascade has not shown taking, a steer
        // under way, or a steer gate is not input the host can give: the
        // turn's first step, the steer's settling, the gate's line or its
        // nudge wakes it.
        if self.agy_rpc.is_some()
            && (agy_steer_running(&state)
                || state
                    .open
                    .first()
                    .is_some_and(|opener| agy_prompt_untaken(&state, opener))
                || (agy_steers(&state) && agy_input_gate(&state).is_some()))
        {
            return false;
        }
        true
    }

    /// Call `notify` when something the host reads through this harness
    /// changes (a turn opens or closes, input is acknowledged or unblocked, a
    /// quota, fault or session id appears, the child exits), and once at
    /// start. A streamed step or text delta changes none of it and wakes
    /// nobody.
    pub fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let _ = thread::Builder::new()
            .name(format!("{}-watch", self.spec.name))
            .spawn(move || {
                let mut state = lock(&shared);
                let mut seen = None;
                loop {
                    if !state.alive {
                        drop(state);
                        notify();
                        break;
                    }
                    if seen != Some(state.changes) {
                        seen = Some(state.changes);
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
        // A resumed cascade's own record, read before its process starts:
        // what it took is not written again, and what its mailbox still
        // holds is its to take, so it is not written again either.
        let resumed = self.agy_rpc.as_ref().and_then(|rpc| {
            let cascade = lock(&self.shared).session_id.clone()?;
            Some(agy_scan(rpc, &cascade))
        });
        // The seat the server configured for this agent: the child starts as
        // the agent's own uid, and no server-only key reaches it.
        let mut command = spawn_command(&self.program, &self.environment)?;
        command.args(&self.args);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
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
        let Some(stderr) = child.stderr.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(HarnessError::Failed("child stderr missing".into()));
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
            state.acknowledged.clear();
            state.input_turns.clear();
            // A refusal for good stays one: `rejected` outlives the process.
            state.written.clear();
            state.finished.clear();
            state.finished_quiet.clear();
            state.usages.clear();
            // What the last process announced and consumed is its own.
            state.ready = None;
            state.peer = None;
            state.in_flight.clear();
            state.consumed.clear();
            state.duplicates.clear();
            state.turn_closing = false;
            state.refused_turns = 0;
            state.turn_retry_at = None;
            state.turn_refused_since = None;
            state.turn_refusal = None;
            state.stdin_broken = false;
            state.session_retired = false;
            state.agy_queued.clear();
            if let Some((taken, held)) = resumed {
                state.applied_before.extend(taken);
                state.agy_queued = held;
            }
            state.quota_reset_at_ms = None;
            state.sign_in_fault = None;
            state.stderr = BoundedStderr::default();
            state.stderr_done = false;
            state.exit_status = None;
            state.exit_fault = None;
            state.child = Some(child);
            state.alive = true;
            state.open.clear();
            state.followups.clear();
            state.turn_user_inputs = 0;
            state.last_turn_step_type = None;
            state.idle_since = None;
            state.last_rpc_health_poll = None;
            state.last_steer_probe = None;
            state.planner_config = None;
            state.http_port = self.agy_rpc.as_ref().and_then(|r| r.port_override);
            state.agy_steer = None;
            state.agy_refused_turn = None;
            state.text = None;
            state.last_line_at = None;
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
        self.stderr_reader = match thread::Builder::new()
            .name(format!("{}-stderr", self.spec.name))
            .spawn(move || {
                let mut stderr = stderr;
                let mut bytes = [0; 4096];
                loop {
                    match stderr.read(&mut bytes) {
                        Ok(0) => break,
                        Ok(count) => {
                            let mut state = lock(&shared);
                            if state.generation != generation {
                                return;
                            }
                            state.stderr.push(&bytes[..count]);
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(_) => break,
                    }
                }
                let mut state = lock(&shared);
                if state.generation == generation {
                    state.stderr_done = true;
                    state.refresh_exit_fault();
                    state.touch();
                }
                wake.notify_all();
            }) {
            Ok(handle) => Some(handle),
            Err(err) => {
                self.stop_child();
                return Err(HarnessError::Failed(err.to_string()));
            }
        };
        if let Some(rpc) = self.agy_rpc.clone() {
            let shared = Arc::clone(&self.shared);
            let wake = Arc::clone(&self.wake);
            // Detached: it may be inside a bounded request when the child
            // stops, and it exits on the generation change once it returns.
            if let Err(err) = thread::Builder::new()
                .name(format!("{}-cascade", self.spec.name))
                .spawn(move || watch_agy_cascade(shared, wake, rpc, generation))
            {
                self.stop_child();
                return Err(HarnessError::Failed(err.to_string()));
            }
        }
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let spec = self.spec.clone();
        let agy_rpc = self.agy_rpc.clone();
        match thread::Builder::new()
            .name(spec.name.clone())
            .spawn(move || read_stdout(stdout, shared, wake, spec, agy_rpc, generation))
        {
            Ok(handle) => self.reader = Some(handle),
            Err(err) => {
                self.stop_child();
                return Err(HarnessError::Failed(err.to_string()));
            }
        }
        Ok(())
    }

    /// Stop the child and read what it wrote to the end: the stdout reader
    /// reaches its end before this returns, so every proof the child gave is
    /// known (`proven`) and kept until the next process starts.
    pub fn stop(&mut self) {
        self.stop_child();
    }

    fn stop_child(&mut self) {
        self.pending = None;
        *lock(&self.stdin) = None;
        let child = lock(&self.shared).child.take();
        if let Some(mut child) = child {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
        }
        // The reader marks the child gone once it read the last line.
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        lock(&self.shared).alive = false;
        self.wake.notify_all();
        if let Some(handle) = self.stderr_reader.take() {
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
        state.last_turn_step_type = None;
        state.idle_since = None;
        state.last_rpc_health_poll = None;
        state.last_steer_probe = None;
        state.planner_config = None;
        state.http_port = None;
        state.text = None;
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

// Retain at most 16 KiB while draining the entire pipe. If retention cuts a
// line, withhold that partial line rather than publishing a credential prefix.
const STDERR_LIMIT: usize = 16 * 1024;

#[derive(Default)]
struct BoundedStderr {
    bytes: Vec<u8>,
    truncated: bool,
}

impl BoundedStderr {
    fn push(&mut self, bytes: &[u8]) {
        let retain = bytes.len().min(STDERR_LIMIT - self.bytes.len());
        self.bytes.extend_from_slice(&bytes[..retain]);
        self.truncated |= retain < bytes.len();
    }

    fn masked(&self, complete: bool) -> String {
        let bytes = if self.truncated || !complete {
            let end = self
                .bytes
                .iter()
                .rposition(|b| *b == b'\n')
                .map_or(0, |pos| pos + 1);
            &self.bytes[..end]
        } else {
            &self.bytes[..]
        };
        let text = String::from_utf8_lossy(bytes);
        let mut masked = anyagents_harness::masking::mask_known_secrets(&text).into_owned();
        if self.truncated {
            masked.push_str("\n[stderr truncated; incomplete line withheld]");
        }
        masked
    }
}

/// Everything the host reads through the harness API: liveness, turn state,
/// receipts that decide a deferred delivery, quota, faults, input readiness
/// and the vendor session id. A line that leaves all of it unchanged is not a
/// transition and wakes nobody.
#[derive(PartialEq, Eq)]
struct Observed {
    alive: bool,
    input_blocked: bool,
    awaiting_first_step: bool,
    agy_idle: bool,
    open: usize,
    followups: usize,
    seen: usize,
    finished: usize,
    acknowledged: usize,
    rejected: usize,
    ready: Option<bool>,
    in_flight: usize,
    consumed: usize,
    quota_reset_at_ms: Option<i64>,
    failure: Option<String>,
    session_id: Option<String>,
}

impl State {
    fn observed(&self) -> Observed {
        Observed {
            alive: self.alive,
            input_blocked: self.input_blocked,
            awaiting_first_step: self.turn_user_inputs == 0,
            agy_idle: self.idle_since.is_some(),
            open: self.open.len(),
            followups: self.followups.len(),
            seen: self.seen.len(),
            finished: self.finished.len(),
            acknowledged: self.acknowledged.len(),
            rejected: self.rejected.len(),
            ready: self.ready,
            in_flight: self.in_flight.len(),
            consumed: self.consumed.len(),
            quota_reset_at_ms: self.quota_reset_at_ms,
            failure: self.failure(),
            session_id: self.session_id.clone(),
        }
    }

    /// Record a transition the host must see.
    fn touch(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        self.changes = self.changes.wrapping_add(1);
    }

    /// What the mailbox may record for an input written to a peer that
    /// proves consumption, or `None` when it is not in this process's hands
    /// (never written, or returned for writing again).
    fn consumption_outcome(&self, id: &str) -> Option<Result<DeliverOutcome, HarnessError>> {
        if let Some(reason) = self.rejected.get(id) {
            return Some(Ok(DeliverOutcome::Rejected {
                reason: reason.clone(),
            }));
        }
        if self.consumed.contains(id) {
            return Some(Ok(if self.duplicates.contains(id) {
                DeliverOutcome::Duplicate
            } else {
                DeliverOutcome::Accepted {
                    turn_finished: self.finished.contains(id),
                }
            }));
        }
        self.in_flight
            .contains(id)
            .then_some(Ok(DeliverOutcome::InFlight))
    }

    /// What the vendor proved about `id`: the evidence each protocol's
    /// level rests on, kept after the process exits.
    ///
    /// - Every protocol: the session's own record shows an earlier process
    ///   applied it (`applied_before`).
    /// - Spiron that proves consumption: `accepted` or `steer_consumed`.
    /// - An older Spiron: `accepted`.
    /// - Antigravity with its language server: the cascade took it (its
    ///   transcript or mailbox says so, or its turn showed a step or closed).
    /// - A line-per-turn child: the whole line was written.
    fn proof_of(&self, protocol: LineProtocol, agy: bool, id: &str) -> Option<Proof> {
        if self.applied_before.contains(id) {
            return Some(Proof::Duplicate);
        }
        let taken = || Proof::Taken {
            turn_finished: self.finished.contains(id),
        };
        match protocol {
            LineProtocol::AcknowledgedResult => match self.ready {
                Some(true) if self.consumed.contains(id) => Some(if self.duplicates.contains(id) {
                    Proof::Duplicate
                } else {
                    taken()
                }),
                Some(false) if self.acknowledged.contains(id) => Some(taken()),
                _ => None,
            },
            _ if agy => self.agy_taken.contains(id).then(taken),
            _ => self.seen.contains(id).then(taken),
        }
    }

    fn failure(&self) -> Option<String> {
        self.exit_fault
            .clone()
            .or_else(|| self.sign_in_fault.clone())
    }

    fn persist_failure(&self) {
        if let (Some(dir), Some(error)) = (&self.state_dir, self.failure()) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |time| time.as_millis());
            let path = dir.join(".anyagents-last-error.json");
            let payload = json!({
                "error": error,
                "timestamp_ms": now,
                "quota_reset_at_ms": self.quota_reset_at_ms,
            })
            .to_string();
            if let Err(err) =
                anyagents_harness::atomic_write_in_existing_dir(&path, payload.as_bytes(), 0o664)
            {
                eprintln!("cannot persist {}: {err}", path.display());
            }
        }
    }

    fn reap_child(&mut self) {
        if let Some(child) = self.child.as_mut()
            && let Ok(Some(status)) = child.try_wait()
        {
            self.child = None;
            self.exit_status = Some(status);
            self.refresh_exit_fault();
        }
    }

    fn refresh_exit_fault(&mut self) {
        if let Some(status) = self.exit_status
            && !status.success()
        {
            let stderr = self.stderr.masked(self.stderr_done);
            self.exit_fault = Some(format!(
                "harness exited with {status}{}{}{}{}",
                self.sign_in_fault.as_ref().map_or("", |_| ": "),
                self.sign_in_fault.as_deref().unwrap_or(""),
                if stderr.trim().is_empty() {
                    ""
                } else {
                    "; stderr: "
                },
                stderr.trim()
            ));
            self.persist_failure();
        }
    }

    fn new(session_id: Option<&str>) -> Self {
        Self {
            child: None,
            stderr: BoundedStderr::default(),
            stderr_done: false,
            exit_status: None,
            exit_fault: None,
            state_dir: None,
            alive: false,
            epoch: 0,
            changes: 0,
            last_line_at: None,
            generation: 0,
            input_blocked: false,
            seen: HashSet::new(),
            acknowledged: HashSet::new(),
            input_turns: HashMap::new(),
            rejected: HashMap::new(),
            written: Vec::new(),
            open: Vec::new(),
            followups: Vec::new(),
            turn_user_inputs: 0,
            last_turn_step_type: None,
            idle_since: None,
            last_rpc_health_poll: None,
            last_steer_probe: None,
            planner_config: None,
            http_port: None,
            agy_steer: None,
            agy_refused_turn: None,
            agy_queued: Vec::new(),
            agy_taken: HashSet::new(),
            session_retired: false,
            stdin_broken: false,
            finished: HashSet::new(),
            turns_started: 0,
            quota_reset_at_ms: None,
            sign_in_fault: None,
            session_id: session_id.map(str::to_string),
            usages: Vec::new(),
            text: None,
            finished_quiet: HashSet::new(),
            ready: None,
            peer: None,
            in_flight: HashSet::new(),
            consumed: HashSet::new(),
            duplicates: HashSet::new(),
            applied_before: HashSet::new(),
            turn_closing: false,
            refused_turns: 0,
            turn_retry_at: None,
            turn_refused_since: None,
            turn_refusal: None,
        }
    }
}

fn read_stdout(
    stdout: ChildStdout,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    spec: StreamSpec,
    agy_rpc: Option<AgyRpcConfig>,
    generation: u64,
) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(mut value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        // Answer text is read before masking.
        let raw_text = spec.text_of(&value);
        anyagents_harness::masking::mask_json_strings(&mut value);
        let mut effect = spec.effect(&value);
        if raw_text.is_some() {
            effect.text = raw_text;
        }
        let kind = value
            .get("type")
            .or_else(|| value.get("event"))
            .and_then(Value::as_str);
        if spec.name == "anyagents-agy" && effect.quota_reset_at_ms.is_some() {
            let (last_step_type, session_id, child_pid, cached_port) = {
                let state = lock(&shared);
                (
                    state.last_turn_step_type.clone(),
                    effect
                        .session_id
                        .clone()
                        .or_else(|| state.session_id.clone()),
                    state.child.as_ref().map(Child::id),
                    state.http_port,
                )
            };
            let mut is_stale_quota = last_step_type.as_deref().is_some_and(|st| {
                st != "error_message" && st != "user_input" && st != "system_message"
            });
            if !is_stale_quota
                && let (Some(rpc), Some(sid)) = (agy_rpc.as_ref(), session_id.as_deref())
                && let Some(port) = resolve_agy_http_port(rpc, cached_port, child_pid)
            {
                lock(&shared).http_port = Some(port);
                if matches!(
                    agy_rpc_is_latest_step_quota_error(port, &rpc.csrf_token, sid),
                    Ok(false)
                ) {
                    is_stale_quota = true;
                }
            }
            if is_stale_quota {
                effect.quota_reset_at_ms = None;
                if kind == Some("result") {
                    effect.finishes_turn = true;
                    effect.usage = Some(spec.usage_of(&value));
                }
            }
        }
        let error = value
            .pointer("/result/error")
            .or_else(|| value.get("error"))
            .and_then(|v| {
                v.as_str().or_else(|| {
                    v.get("message")
                        .and_then(Value::as_str)
                        .or_else(|| v.get("code").and_then(Value::as_str))
                })
            })
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let poisoned_agy = spec.name == "anyagents-agy"
            && matches!(kind, Some("done") | Some("result"))
            && error.is_some_and(|error| {
                error
                    .to_ascii_lowercase()
                    .contains("invalid_argument (code 400)")
            });
        let synthetic_401 = synthetic_auth_error(&value);
        if poisoned_agy || synthetic_401.is_some() {
            effect.finishes_turn = false;
        }
        if let Some(ref rpc) = agy_rpc {
            if poisoned_agy {
                let path = rpc.config_dir.join(".anyagents-last-error.json");
                let payload = json!({
                    "error": error,
                    "timestamp_ms": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0),
                })
                .to_string();
                if let Err(err) = anyagents_harness::atomic_write_in_existing_dir(
                    &path,
                    payload.as_bytes(),
                    0o664,
                ) {
                    eprintln!("cannot persist {}: {err}", path.display());
                }
            } else if let Some(reset_at_ms) = effect.quota_reset_at_ms {
                let err_msg = value
                    .pointer("/result/error")
                    .or_else(|| value.get("error"))
                    .and_then(Value::as_str)
                    .unwrap_or("RESOURCE_EXHAUSTED: quota reached");
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                let path = rpc.config_dir.join(".anyagents-last-error.json");
                let payload = serde_json::json!({
                    "error": err_msg,
                    "quota_reset_at_ms": reset_at_ms,
                    "timestamp_ms": now_ms,
                })
                .to_string();
                if let Err(err) = anyagents_harness::atomic_write_in_existing_dir(
                    &path,
                    payload.as_bytes(),
                    0o664,
                ) {
                    eprintln!("cannot persist {}: {err}", path.display());
                }
            } else if effect.finishes_turn {
                let _ = std::fs::remove_file(rpc.config_dir.join(".anyagents-last-error.json"));
            }
        }
        let mut retry_after = None;
        {
            let mut state = lock(&shared);
            if state.generation != generation {
                return;
            }
            let before = state.observed();
            state.idle_since = None;
            note_markers(&mut state, &value);
            if spec.protocol == LineProtocol::AcknowledgedResult
                && kind == Some("ready")
                && state.ready.is_none()
            {
                state.ready = Some(announces_consumption(&value));
                state.peer = Some(announced_peer(&value));
            }
            // A peer that proves consumption is read by its own rules; the
            // receipts below are the older peer's.
            let consumption =
                spec.protocol == LineProtocol::AcknowledgedResult && state.ready == Some(true);
            if consumption {
                retry_after = note_consumption_receipt(&mut state, kind, &value);
            } else if spec.protocol == LineProtocol::AcknowledgedResult
                && let Some(id) = value.get("request_id").and_then(Value::as_str)
                && state
                    .open
                    .iter()
                    .chain(&state.followups)
                    .any(|open| open == id)
            {
                match kind {
                    Some("accepted") => {
                        state.acknowledged.insert(id.to_string());
                        state.input_turns.insert(id.to_string(), id.to_string());
                    }
                    Some("rejected") => match spiron_refusal(&value) {
                        // Never taken: it is offered again once the turn
                        // that refused it is gone.
                        SpironRefusal::NotStarted | SpironRefusal::TurnClosed => {
                            let id = id.to_string();
                            state.open.retain(|open| *open != id);
                            state.followups.retain(|open| *open != id);
                            state.seen.remove(&id);
                        }
                        SpironRefusal::ForGood(reason) => {
                            let id = id.to_string();
                            state.open.retain(|open| *open != id);
                            state.followups.retain(|open| *open != id);
                            state.rejected.insert(id, reason);
                        }
                    },
                    _ => {}
                }
            }
            if let Some(step_type) = value
                .pointer("/step_update/step_type")
                .and_then(Value::as_str)
            {
                state.last_turn_step_type = Some(step_type.to_string());
                if step_type == "user_input" || state.turn_user_inputs == 0 {
                    state.turn_user_inputs = state.turn_user_inputs.saturating_add(1);
                }
                agy_note_opener_taken(&mut state);
            }
            if let Some(reset_at_ms) = effect.quota_reset_at_ms {
                state.quota_reset_at_ms = Some(reset_at_ms);
                // Such a peer still closes its turn with a result that names
                // what it consumed; nothing is dropped before that.
                if !consumption {
                    let rejected_ids = std::mem::take(&mut state.open);
                    for id in rejected_ids {
                        state.seen.remove(&id);
                    }
                }
            }
            if let Some(session_id) = effect.session_id
                && !session_id.is_empty()
                && !state.session_retired
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
            let correlated = spec.protocol != LineProtocol::AcknowledgedResult
                || value
                    .get("request_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| {
                        state.input_turns.values().any(|owner| owner == id)
                            || (consumption && state.in_flight.contains(id))
                    });
            // Its result closes the turn even when it reports a quota: what
            // the turn did not consume goes back to the mailbox.
            let finishes = if consumption {
                kind == Some("result") && synthetic_401.is_none()
            } else {
                effect.finishes_turn
            };
            if finishes && correlated {
                if spec.protocol == LineProtocol::AcknowledgedResult
                    && let Some((_, lines)) = &mut state.text
                {
                    *lines = vec![
                        anyagents_harness::masking::mask_known_secrets(&lines.concat())
                            .into_owned(),
                    ];
                }
                state.sign_in_fault = None;
                if spec.protocol == LineProtocol::AcknowledgedResult {
                    let error = value
                        .get("error")
                        .and_then(|error| {
                            error
                                .as_str()
                                .or_else(|| error.get("message").and_then(Value::as_str))
                        })
                        .map(str::trim)
                        .filter(|error| !error.is_empty());
                    if effect.quota_reset_at_ms.is_none()
                        && (value.get("is_error").and_then(Value::as_bool) == Some(true)
                            || value.get("status").and_then(Value::as_str) == Some("failed")
                            || error.is_some())
                    {
                        state.sign_in_fault =
                            Some(error.unwrap_or("harness turn failed").to_string());
                    }
                }
                let usage = effect.usage.unwrap_or_else(|| spec.usage_of(&value));
                if consumption {
                    finish_consumed(
                        &mut state,
                        value["request_id"].as_str().unwrap(),
                        usage,
                        &value,
                    );
                } else if spec.protocol == LineProtocol::AcknowledgedResult {
                    finish_correlated(&mut state, value["request_id"].as_str().unwrap(), usage);
                } else {
                    finish_open(&mut state, usage);
                }
            } else if let Some(ref synth_err) = synthetic_401 {
                let mut rejected_ids = std::mem::take(&mut state.open);
                rejected_ids.append(&mut state.followups);
                for id in rejected_ids {
                    state.seen.remove(&id);
                    state.acknowledged.remove(&id);
                    state.finished.remove(&id);
                }
                state.sign_in_fault = Some(synth_err.clone());
                // The child is stopped; what it wrote before is still read,
                // and its stdout's end reports it gone.
                if let Some(child) = state.child.as_mut() {
                    let _ = child.kill();
                }
            } else if correlated
                && (poisoned_agy || effect.quota_reset_at_ms.is_none())
                && matches!(kind, Some("done") | Some("result"))
                && let Some(err_str) = error
            {
                if poisoned_agy {
                    if let Some(ref rpc) = agy_rpc {
                        let _ = std::fs::remove_file(rpc.config_dir.join("agy-session-id"));
                    }
                    // Do not let sync_session recreate the retired pointer.
                    state.session_id = None;
                    state.session_retired = true;
                    // The cascade takes nothing more: the child is stopped,
                    // and its stdout's end reports it gone.
                    if let Some(child) = state.child.as_mut() {
                        let _ = child.kill();
                    }
                }
                state.sign_in_fault = Some(err_str.to_string());
                state.open.clear();
                state.followups.clear();
            }
            if finishes
                && correlated
                && state.failure().is_none()
                && let Some(dir) = &state.state_dir
            {
                let _ = std::fs::remove_file(dir.join(".anyagents-last-error.json"));
            }
            let after = state.observed();
            if after.failure != before.failure {
                state.persist_failure();
            }
            if after != before {
                state.changes = state.changes.wrapping_add(1);
            }
            state.last_line_at = Some(Instant::now());
            state.epoch = state.epoch.wrapping_add(1);
        }
        wake.notify_all();
        // A turn the peer could not start opens again with time alone.
        if let Some(wait) = retry_after {
            nudge_after(&shared, &wake, generation, wait);
        }
        // A step or a turn's end is when a cascade takes what its mailbox
        // holds: its record says which.
        if let Some(rpc) = &agy_rpc
            && (value.get("step_update").is_some() || kind == Some("result"))
        {
            agy_settle(&shared, &wake, rpc, generation);
        }
    }
    // Every line is read: what the cascade took before the child went is in
    // its record, and is looked at once more before the child counts as gone.
    if let Some(rpc) = &agy_rpc {
        agy_settle(&shared, &wake, rpc, generation);
    }
    // EOF can race waitpid and the independent stderr drainer. Collect both
    // before publishing a normal exit, with the old bounded reap grace period.
    let deadline = Instant::now() + Duration::from_millis(250);
    let mut state = lock(&shared);
    loop {
        if state.generation != generation {
            return;
        }
        state.reap_child();
        if (state.child.is_none() && state.stderr_done) || Instant::now() >= deadline {
            break;
        }
        let (guard, _) = wake
            .wait_timeout(state, Duration::from_millis(10))
            .unwrap_or_else(|err| err.into_inner());
        state = guard;
    }
    state.alive = false;
    state.refresh_exit_fault();
    state.touch();
    wake.notify_all();
}

fn is_oauth_auth_error_text(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    text.contains("OAuth session expired")
        || text.contains("OAuth access token has expired")
        || text.contains("Failed to authenticate")
        || text.contains("Not logged in")
        || text.contains("authentication_failed")
        || text.contains("authentication_error")
        || (lower.contains("401")
            && (lower.contains("oauth")
                || lower.contains("authenticate")
                || lower.contains("authentication")
                || lower.contains("access token")))
}

fn collect_text_fields(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) if !s.trim().is_empty() => out.push(s.clone()),
        Value::Array(items) => {
            for item in items {
                collect_text_fields(item, out);
            }
        }
        Value::Object(map) => {
            if let Some(Value::String(text)) = map.get("text") {
                if !text.trim().is_empty() {
                    out.push(text.clone());
                }
            } else {
                for (k, v) in map {
                    if matches!(
                        k.as_str(),
                        "content" | "message" | "error" | "result" | "delta"
                    ) {
                        collect_text_fields(v, out);
                    }
                }
            }
        }
        _ => {}
    }
}

pub fn synthetic_auth_error(value: &Value) -> Option<String> {
    let kind = value
        .get("type")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str);
    let model = value
        .pointer("/message/model")
        .or_else(|| value.get("model"))
        .and_then(Value::as_str);
    if model == Some("<synthetic>") {
        let mut texts = Vec::new();
        collect_text_fields(
            value.pointer("/message/content").unwrap_or(value),
            &mut texts,
        );
        let joined = texts.join("\n");
        if is_oauth_auth_error_text(&joined) {
            return Some(joined.trim().to_string());
        }
    }
    if kind == Some("result")
        && let Some(text) = value.get("result").and_then(Value::as_str)
    {
        let trimmed = text.trim();
        let is_synthetic_prefix = trimmed.starts_with("Failed to authenticate")
            || trimmed.starts_with("API Error: 401")
            || trimmed.starts_with("OAuth access token has expired")
            || trimmed.starts_with("OAuth session expired")
            || trimmed.starts_with("Not logged in");
        if is_synthetic_prefix && is_oauth_auth_error_text(trimmed) {
            return Some(trimmed.to_string());
        }
    }
    None
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
    state.last_turn_step_type = None;
    state.idle_since = None;
    state.finished.insert(id.to_string());
    state.text = text;
}

/// The turn ended: its inputs are finished, the first carrying its usage. An
/// Antigravity turn that closed took the line that opened it; a message its
/// cascade's mailbox still holds was not taken by it and finishes nothing.
fn finish_open(state: &mut State, usage: TurnUsage) {
    state.turn_user_inputs = 0;
    state.last_turn_step_type = None;
    state.idle_since = None;
    if state.open.is_empty() {
        return;
    }
    let mut usage = Some(usage);
    for id in std::mem::take(&mut state.open) {
        if state.agy_queued.iter().any(|queued| queued.id == id) {
            continue;
        }
        if state.seen.contains(&id) {
            state.agy_taken.insert(id.clone());
        }
        state.finished.insert(id.clone());
        let item_usage = usage.take().unwrap_or_default();
        state.usages.push((id, item_usage));
    }
    if !state.followups.is_empty() {
        state.turns_started += 1;
        state.open.append(&mut state.followups);
    }
}

/// The result that closes an older Spiron's turn: its one input is finished.
fn finish_correlated(state: &mut State, turn: &str, usage: TurnUsage) {
    if !state.open.iter().any(|id| id == turn) {
        return;
    }
    state.open.retain(|id| id != turn);
    state.finished.insert(turn.to_string());
    state.usages.push((turn.to_string(), usage));
    state.turn_user_inputs = 0;
    state.last_turn_step_type = None;
    state.idle_since = None;
}

/// The `ready` frame names both capabilities the pipelined path relies on:
/// a per-input consumption signal and request-id deduplication.
fn announces_consumption(value: &Value) -> bool {
    let named = |capability: &str| {
        value
            .get("capabilities")
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(capability)))
    };
    named("steer_consumed") && named("idempotent_input")
}

/// What a `ready` frame says about the peer: its protocol and capabilities.
fn announced_peer(value: &Value) -> String {
    let protocol = value
        .get("protocol")
        .and_then(Value::as_str)
        .unwrap_or("unnamed protocol");
    let capabilities: Vec<&str> = value
        .get("capabilities")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if capabilities.is_empty() {
        protocol.to_string()
    } else {
        format!("{protocol} [{}]", capabilities.join(", "))
    }
}

/// One receipt from a peer that proves consumption. Only an input this
/// process wrote and has not settled is moved; a consumption proof is kept
/// whatever else is known, since it states what the journal holds.
///
/// Returns how long to wait before the host is woken to offer a turn the
/// peer could not start again.
fn note_consumption_receipt(
    state: &mut State,
    kind: Option<&str>,
    value: &Value,
) -> Option<Duration> {
    let request = value.get("request_id").and_then(Value::as_str);
    match kind {
        // The turn-starting prompt is consumed when its turn is accepted,
        // which the peer says only after journaling it. A steer may be
        // acknowledged before that; it is consumed after it, in order.
        Some("accepted") => {
            let id = request
                .filter(|id| state.in_flight.contains(*id))?
                .to_string();
            if value.get("mode").and_then(Value::as_str) == Some("duplicate") {
                state.duplicates.insert(id.clone());
            }
            settle_consumed(state, &id, &id);
            state.refused_turns = 0;
            state.turn_retry_at = None;
            state.turn_refused_since = None;
            state.turn_refusal = None;
        }
        Some("steer_ack") if value.get("accepted").and_then(Value::as_bool) == Some(true) => {
            if let (Some(id), Some(turn)) = (
                request.filter(|id| state.in_flight.contains(*id)),
                value.get("turn_id").and_then(Value::as_str),
            ) {
                state.acknowledged.insert(id.to_string());
                state.input_turns.insert(id.to_string(), turn.to_string());
            }
        }
        Some("steer_consumed") => {
            let id = value.get("steer_request_id").and_then(Value::as_str)?;
            // A steer an earlier process journaled is consumed all the same.
            if value.get("duplicate").and_then(Value::as_bool) == Some(true) {
                state.duplicates.insert(id.to_string());
            }
            let turn = value
                .get("turn_id")
                .and_then(Value::as_str)
                .or_else(|| state.input_turns.get(id).map(String::as_str))
                .unwrap_or(id)
                .to_string();
            let id = id.to_string();
            settle_consumed(state, &id, &turn);
        }
        Some("rejected") => {
            let id = request
                .filter(|id| state.in_flight.contains(*id))?
                .to_string();
            match spiron_refusal(value) {
                // The turn could not start: nothing given to it was
                // journaled. Spiron starts a turn only once it holds the
                // session's lease, which a process that died keeps for up to
                // `SPIRON_LEASE`, so the same input is offered again after a
                // backoff; the reader wakes the host then. Refusals past
                // that lease are a fault (`deliver_consumed`).
                SpironRefusal::NotStarted => {
                    let opened = state.open.first().is_some_and(|owner| *owner == id);
                    release_input(state, &id);
                    state.turn_closing |= !state.open.is_empty();
                    if opened {
                        state.refused_turns = state.refused_turns.saturating_add(1);
                        state.turn_refused_since.get_or_insert_with(Instant::now);
                        state.turn_refusal = value
                            .get("error")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        let wait = TURN_RETRY_FIRST
                            .saturating_mul(1 << (state.refused_turns - 1).min(5))
                            .min(TURN_RETRY_MAX);
                        state.turn_retry_at = Some(Instant::now() + wait);
                        return Some(wait);
                    }
                }
                // The turn it was written to closed first: never journaled.
                // The next turn takes it; that turn's `result` (already on
                // its way) or this refusal wakes the host.
                SpironRefusal::TurnClosed => {
                    release_input(state, &id);
                    state.turn_closing |= !state.open.is_empty();
                }
                SpironRefusal::ForGood(reason) => {
                    state.in_flight.remove(&id);
                    state.open.retain(|open| open != &id);
                    state.rejected.insert(id, reason);
                }
            }
        }
        _ => {}
    }
    None
}

/// The first wait before a turn the peer could not start is offered again;
/// each refusal in a row doubles it up to [`TURN_RETRY_MAX`].
const TURN_RETRY_FIRST: Duration = Duration::from_secs(1);
const TURN_RETRY_MAX: Duration = Duration::from_secs(30);

/// How long Spiron's session lease outlives the process that last renewed
/// it (`LEASE_MS` in `spiron-agent`'s `local_admission.rs`): no turn can
/// start until a dead holder's lease lapses.
const SPIRON_LEASE: Duration = Duration::from_secs(180);

/// What a Spiron `rejected` frame means for the input it names, by its code
/// (`spiron-cli`'s `headless.rs`).
#[derive(Debug, PartialEq, Eq)]
enum SpironRefusal {
    /// `turn_not_started`: the turn could not take its lease or journal its
    /// input. A later turn can.
    NotStarted,
    /// `turn_closed` (the turn sealed before the steer reached it) or
    /// `no_active_turn` (a steer with no turn): the next turn takes it.
    TurnClosed,
    /// Every other code, and a code this adapter does not know: no offer
    /// makes Spiron take this input as it is written (`empty_prompt`,
    /// `wrong_session`, `unknown_command`, `upgrade_retired`).
    ForGood(String),
}

fn spiron_refusal(value: &Value) -> SpironRefusal {
    match value.get("code").and_then(Value::as_str) {
        Some("turn_not_started") => SpironRefusal::NotStarted,
        Some("turn_closed" | "no_active_turn") => SpironRefusal::TurnClosed,
        code => SpironRefusal::ForGood(format!(
            "spiron rejected the input: {}",
            code.unwrap_or("no code")
        )),
    }
}

fn settle_consumed(state: &mut State, id: &str, turn: &str) {
    state.in_flight.remove(id);
    state.consumed.insert(id.to_string());
    state.acknowledged.insert(id.to_string());
    state.input_turns.insert(id.to_string(), turn.to_string());
}

/// Give an input back to the mailbox: the peer refused it, or ended the turn
/// it was steered into without consuming it, so it is not in the journal and
/// the next pass writes it again, in order. A consumed input never goes back.
fn release_input(state: &mut State, id: &str) {
    if state.consumed.contains(id) {
        return;
    }
    state.in_flight.remove(id);
    state.open.retain(|open| open != id);
    state.acknowledged.remove(id);
    state.input_turns.remove(id);
    state.seen.remove(id);
}

/// The result that closes `turn` for a peer that proves consumption. Its
/// consumed inputs are finished, the turn's usage on its opener; inputs it
/// acknowledged but never consumed, and every id it lists undelivered, go
/// back to the mailbox. An input with no answer yet stays in flight until the
/// peer refuses it. A duplicate turn ran nothing, so it finishes nothing.
fn finish_consumed(state: &mut State, turn: &str, usage: TurnUsage, value: &Value) {
    if value.get("duplicate").and_then(Value::as_bool) == Some(true)
        && state.in_flight.contains(turn)
    {
        state.duplicates.insert(turn.to_string());
        settle_consumed(state, turn, turn);
    }
    // A turn closed on an input it never accepted did not journal it.
    release_input(state, turn);
    let undelivered: Vec<String> = value
        .get("undelivered_steer_ids")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    for id in &undelivered {
        release_input(state, id);
    }
    let mut members: Vec<String> = state
        .input_turns
        .iter()
        .filter(|(_, owner)| owner.as_str() == turn)
        .map(|(id, _)| id.clone())
        .collect();
    // The opener first, so it carries the turn's usage.
    members.sort_by_key(|id| id != turn);
    for id in members {
        if !state.consumed.contains(&id) {
            release_input(state, &id);
        } else if !state.duplicates.contains(&id) && state.finished.insert(id.clone()) {
            let item_usage = if id == turn {
                usage.clone()
            } else {
                TurnUsage::default()
            };
            state.usages.push((id, item_usage));
        }
    }
    if state.open.first().is_some_and(|owner| owner == turn) {
        state.open.clear();
    } else {
        state
            .open
            .retain(|id| id != turn && state.input_turns.get(id).is_none_or(|owner| owner != turn));
    }
    state.turn_closing &= !state.open.is_empty();
    state.turn_user_inputs = 0;
    state.last_turn_step_type = None;
    state.idle_since = None;
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
        // A pipe the child closed takes no more lines. The child is going,
        // and the stdout reader reports it gone once it read the last line.
        let broken =
            result < 0 || poll.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0;
        state.stdin_broken |= broken;
        state.touch();
        drop(state);
        wake.notify_all();
        if broken {
            return;
        }
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
    if let Ok(text) = std::fs::read_to_string(&log_path) {
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
        if found_port.is_some() {
            return found_port;
        }
    }
    #[cfg(target_os = "linux")]
    if let Some(pid) = child_pid {
        return resolve_agy_http_port_from_proc(pid, &rpc.csrf_token);
    }
    None
}

#[cfg(target_os = "linux")]
fn resolve_agy_http_port_from_proc(pid: u32, csrf_token: &str) -> Option<u16> {
    let fd_dir = format!("/proc/{pid}/fd");
    let mut inodes = HashSet::new();
    if let Ok(entries) = std::fs::read_dir(&fd_dir) {
        for entry in entries.flatten() {
            if let Ok(target) = std::fs::read_link(entry.path())
                && let Some(s) = target.to_str()
                && let Some(rest) = s.strip_prefix("socket:[")
                && let Some(inode) = rest.strip_suffix(']')
            {
                inodes.insert(inode.to_string());
            }
        }
    }
    let process_uid = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|st| {
            st.lines()
                .find(|l| l.starts_with("Uid:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .map(str::to_string)
        });
    if inodes.is_empty() && process_uid.is_none() {
        return None;
    }
    let tcp_text = std::fs::read_to_string(format!("/proc/{pid}/net/tcp")).ok()?;
    let mut candidate_ports = Vec::new();
    for line in tcp_text.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        let matched = if !inodes.is_empty() {
            parts.len() > 9 && parts[3] == "0A" && inodes.contains(parts[9])
        } else if let Some(ref uid) = process_uid {
            parts.len() > 9 && parts[3] == "0A" && parts[7] == uid
        } else {
            false
        };
        if matched
            && let Some((ip_hex, port_hex)) = parts[1].split_once(':')
            && ip_hex == "0100007F"
            && let Ok(port) = u16::from_str_radix(port_hex, 16)
        {
            candidate_ports.push(port);
        }
    }
    for port in candidate_ports {
        if agy_rpc_post(port, csrf_token, "GetAllCascadeTrajectories", &json!({})).is_ok() {
            return Some(port);
        }
    }
    None
}

/// How long a probe of the language server may take. A probe only reads: one
/// that times out is asked again later. A send has no such limit.
const AGY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// One read-only request to the language server, decoded.
fn agy_rpc_post(
    port: u16,
    csrf_token: &str,
    method: &str,
    payload: &Value,
) -> Result<Value, String> {
    let body = agy_rpc_exchange(port, csrf_token, method, payload, Some(AGY_PROBE_TIMEOUT))
        .map_err(|err| err.to_string())?;
    if body.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str::<Value>(body.trim()).map_err(|err| format!("decode {method}: {err}"))
}

/// Why a language server request left no answer to use, by what the server
/// can have done with it.
#[derive(Debug)]
enum AgyRpcError {
    /// It never reached the server whole (no connection, or cut off while
    /// written): the server did nothing with it.
    Unreached(String),
    /// The server answered with an error (a Connect error, a non-2xx status).
    Refused(String),
    /// It reached the server, and no complete answer came back: the server
    /// may have acted on it.
    Unanswered(String),
}

impl std::fmt::Display for AgyRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreached(why) | Self::Refused(why) | Self::Unanswered(why) => f.write_str(why),
        }
    }
}

/// One Connect request (`POST /exa.language_server_pb.LanguageServerService/
/// <method>`, JSON) and the body of its 2xx answer. `read_timeout` bounds
/// the wait for the answer; `None` waits until the server answers or closes.
fn agy_rpc_exchange(
    port: u16,
    csrf_token: &str,
    method: &str,
    payload: &Value,
    read_timeout: Option<Duration>,
) -> Result<String, AgyRpcError> {
    use std::io::Read as _;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};

    let body =
        serde_json::to_vec(payload).map_err(|err| AgyRpcError::Unreached(err.to_string()))?;
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        .map_err(|err| AgyRpcError::Unreached(format!("connect 127.0.0.1:{port}: {err}")))?;
    let _ = stream.set_read_timeout(read_timeout);
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
        .map_err(|err| AgyRpcError::Unreached(format!("write {method}: {err}")))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|err| AgyRpcError::Unanswered(format!("read {method}: {err}")))?;
    let response = String::from_utf8_lossy(&response);
    let malformed = || AgyRpcError::Unanswered(format!("malformed HTTP response from {method}"));
    let (head, resp_body) = response.split_once("\r\n\r\n").ok_or_else(malformed)?;
    let status_code = head
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(malformed)?;
    if !(200..300).contains(&status_code) {
        return Err(AgyRpcError::Refused(format!(
            "{method} HTTP {status_code}: {}",
            resp_body.trim()
        )));
    }
    Ok(resp_body.to_string())
}

/// Grace window when an `agy` cascade transitions to `CASCADE_RUN_STATUS_IDLE`.
/// Normal turns emit `{"event":"result"}` on stdout within ~100ms (`pollResponse`
/// ticker). Cascades held open by an active background task (e.g. `schedule`
/// timer via `isRootIdleWithTasks`) never emit `result` while idle and instead
/// accept a new invocation via `SendUserCascadeMessage` with `plannerConfig`.
const AGY_TURN_CLOSE_GRACE: Duration = Duration::from_millis(200);

/// The pause after a steer probe found no cascade to steer into, or could
/// not ask, and after a steer that never reached the language server.
const AGY_STEER_PROBE_GAP: Duration = Duration::from_millis(400);

/// Whether a steer must wait: `Some(None)` until an event wakes the host,
/// `Some(Some(d))` for `d`.
///
/// - A steer under way: its settling wakes the host.
/// - A turn whose cascade refused a steer takes no more: its end does.
/// - A steer goes through the language server, while the turn it joins was
///   started on stdin, and nothing orders the two. A known cascade that has
///   not shown the turn's first step may still be idle; a steer sent now
///   would start its own invocation ahead of the turn's own message. The
///   first step line ends this wait.
/// - A cascade that just went idle may be ending its turn, whose `result`
///   names no invocation: a steer sent as a new invocation now would have that
///   late `result` close it. [`AGY_TURN_CLOSE_GRACE`] is the vendor's
///   `pollResponse` bound on that line.
/// - A probe that found nothing to steer into, or failed, is not repeated
///   for [`AGY_STEER_PROBE_GAP`].
///
/// A fresh session has no cascade to steer into until the language server
/// shows one, and the steer path asks it at once rather than after a delay.
/// A cascade whose mailbox holds mail and that runs no turn of this process
/// is steered at once: the message joins the mail and starts the invocation
/// that takes all of it, in order.
fn agy_input_gate(state: &State) -> Option<Option<Duration>> {
    if agy_steer_running(state) || (agy_held(state) && !state.open.is_empty()) {
        return Some(None);
    }
    if let Some(idle) = state.idle_since
        && idle.elapsed() < AGY_TURN_CLOSE_GRACE
    {
        return Some(Some(AGY_TURN_CLOSE_GRACE - idle.elapsed()));
    }
    if let Some(probe) = state.last_steer_probe
        && probe.elapsed() < AGY_STEER_PROBE_GAP
    {
        return Some(Some(AGY_STEER_PROBE_GAP - probe.elapsed()));
    }
    (state.turn_user_inputs == 0 && state.session_id.is_some() && !state.open.is_empty())
        .then_some(None)
}

/// Whether the next input goes into the cascade through the language server
/// rather than to stdin as a turn of its own: a turn is open, the cascade is
/// held open past its turn, or its mailbox holds mail it has not taken. A
/// cascade held open that refused a steer gets the next input on stdin, but
/// never while its mailbox holds mail: a turn opened on stdin may run before
/// the cascade takes what waits, so the next input joins the mailbox behind
/// it instead.
fn agy_steers(state: &State) -> bool {
    !state.open.is_empty()
        || (state.turn_user_inputs > 0 && !agy_held(state))
        || !state.agy_queued.is_empty()
}

/// `id` opened the running turn on stdin and its cascade has not shown taking
/// it: no step of the turn yet, the language server has not reported the
/// cascade running, and the turn has not closed. Until then the prompt is in
/// flight, not committed: a line the process never read is offered again as
/// input of its own.
fn agy_prompt_untaken(state: &State, id: &str) -> bool {
    state.seen.contains(id)
        && state.open.first().is_some_and(|opener| opener == id)
        && !state.agy_taken.contains(id)
}

/// The cascade showed the turn opened on stdin running (its first step, or
/// the language server's run status): it took its opening line.
fn agy_note_opener_taken(state: &mut State) {
    if state.turn_user_inputs > 0
        && let Some(opener) = state.open.first()
        && state.seen.contains(opener)
        && !state.agy_queued.iter().any(|queued| queued.id == *opener)
        && !state.agy_taken.contains(opener)
    {
        let opener = opener.clone();
        state.agy_taken.insert(opener);
    }
}

/// The running turn's cascade refused a steer.
fn agy_held(state: &State) -> bool {
    state.agy_refused_turn == Some(state.turns_started)
}

fn agy_steer_running(state: &State) -> bool {
    state
        .agy_steer
        .as_ref()
        .is_some_and(|steer| steer.outcome.is_none())
}

/// One message on its way into an Antigravity cascade.
struct AgySteer {
    id: String,
    /// `None` while the request runs.
    outcome: Option<AgySteered>,
    /// Where to look for it in the cascade's record once it is sent: a
    /// cascade can take it before the language server answers.
    sent: Option<AgyQueued>,
}

/// How a steer settled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AgySteered {
    /// The language server queued the message in the cascade's durable
    /// mailbox: it answered `{}`, or, after an answer that never came, the
    /// mailbox or the transcript holds the message. That is no proof the
    /// cascade takes it: it waits in [`State::agy_queued`] until the
    /// cascade's record shows it did.
    Queued,
    /// Not sent, or certainly not queued: it goes back to the mailbox.
    Returned,
}

/// A message a cascade's durable mailbox holds, and how to find whether the
/// cascade took it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AgyQueued {
    id: String,
    /// The cascade whose mailbox holds it.
    cascade: String,
    /// Its mailbox file (`messages/<mail>.json`), once found.
    mail: Option<String>,
    /// When its send began: an older mailbox file is not its.
    since: SystemTime,
    /// How much of the cascade's transcript is known not to name it.
    transcript_from: u64,
}

/// What one steer needs, taken from the state when it starts.
struct AgySteerJob {
    rpc: AgyRpcConfig,
    id: String,
    text: String,
    session_id: String,
    child_pid: Option<u32>,
    cached_port: Option<u16>,
    planner_config: Option<Value>,
    generation: u64,
}

/// Run `job` off the delivery path; its message is in flight until the steer
/// settles, which wakes the host.
fn agy_start_steer(
    shared: &Arc<Mutex<State>>,
    wake: &Arc<Condvar>,
    name: &str,
    job: AgySteerJob,
) -> DeliverOutcome {
    let generation = job.generation;
    let (thread_shared, thread_wake) = (Arc::clone(shared), Arc::clone(wake));
    // Detached: the send waits for its answer without a timeout, and ends
    // with the language server's process at the latest. A steer of a
    // process that was replaced settles nothing.
    let started = thread::Builder::new()
        .name(format!("{name}-steer"))
        .spawn(move || agy_steer(thread_shared, thread_wake, job));
    if started.is_ok() {
        return DeliverOutcome::InFlight;
    }
    lock(shared).agy_steer = None;
    agy_back_off(shared, wake, generation);
    DeliverOutcome::Deferred
}

fn agy_steer(shared: Arc<Mutex<State>>, wake: Arc<Condvar>, job: AgySteerJob) {
    let (id, generation) = (job.id.clone(), job.generation);
    let outcome = agy_steer_attempt(&shared, &wake, job);
    let mut state = lock(&shared);
    if state.generation == generation
        && let Some(steer) = state.agy_steer.as_mut()
        && steer.id == id
    {
        steer.outcome = Some(outcome);
        state.touch();
    }
    drop(state);
    wake.notify_all();
}

/// The message goes back to the mailbox and the next steer waits
/// [`AGY_STEER_PROBE_GAP`].
fn agy_back_off(shared: &Arc<Mutex<State>>, wake: &Arc<Condvar>, generation: u64) -> AgySteered {
    {
        let mut state = lock(shared);
        if state.generation == generation {
            state.last_steer_probe = Some(Instant::now());
        }
    }
    nudge_after(shared, wake, generation, AGY_STEER_PROBE_GAP);
    AgySteered::Returned
}

/// The refusals the language server answers before it queues anything.
const AGY_REFUSALS: [&str; 3] = [
    "plan model not specified",
    "cascade not found",
    "there is an ongoing battle",
];

/// One steer: ask the language server whether the cascade runs (and for its
/// planner config when it does not), then `SendUserCascadeMessage` with
/// `NEXT_INVOCATION`, which queues the message in the cascade's durable
/// mailbox and answers `{}`. The answer is waited for without a timeout. A
/// probe that fails, or a refusal, returns the message; an answer that
/// never came is settled from the mailbox, never by sending again blindly.
/// A queued message is held ([`State::agy_queued`]) until the cascade's
/// record shows it took it, which may come before the answer does.
fn agy_steer_attempt(
    shared: &Arc<Mutex<State>>,
    wake: &Arc<Condvar>,
    job: AgySteerJob,
) -> AgySteered {
    let AgySteerJob {
        rpc,
        id,
        text,
        mut session_id,
        child_pid,
        cached_port,
        mut planner_config,
        generation,
    } = job;
    let Some(port) = agy_port(shared, &rpc, cached_port, child_pid, generation) else {
        return agy_back_off(shared, wake, generation);
    };
    let Ok((resolved_sid, running)) = agy_rpc_is_running(port, &rpc.csrf_token, &session_id) else {
        agy_forget_port(shared, &rpc, generation);
        return agy_back_off(shared, wake, generation);
    };
    if resolved_sid.is_empty() {
        return agy_back_off(shared, wake, generation);
    }
    {
        let mut state = lock(shared);
        if state.generation != generation || !state.alive {
            return AgySteered::Returned;
        }
        if resolved_sid != session_id {
            session_id = resolved_sid;
            state.session_id = Some(session_id.clone());
        }
        if running {
            state.idle_since = None;
            state.last_steer_probe = None;
            state.turn_user_inputs = state.turn_user_inputs.max(1);
            agy_note_opener_taken(&mut state);
        } else {
            // A cascade that ran nothing yet is steered only to start the
            // invocation that takes the mail its mailbox holds.
            if state.turn_user_inputs == 0 && state.agy_queued.is_empty() {
                state.idle_since = None;
                drop(state);
                return agy_back_off(shared, wake, generation);
            }
            if !state.open.is_empty() {
                let now = Instant::now();
                let first_idle = state.idle_since.is_none();
                let idle_for = now.duration_since(*state.idle_since.get_or_insert(now));
                if idle_for < AGY_TURN_CLOSE_GRACE {
                    drop(state);
                    if first_idle {
                        nudge_after(shared, wake, generation, AGY_TURN_CLOSE_GRACE - idle_for);
                    }
                    return AgySteered::Returned;
                }
            }
        }
    }
    if !running && planner_config.is_none() {
        let Ok(found) = agy_rpc_fetch_planner_config(port, &rpc.csrf_token, &session_id) else {
            return agy_back_off(shared, wake, generation);
        };
        planner_config = found;
        let mut state = lock(shared);
        if state.generation == generation && planner_config.is_some() {
            state.planner_config = planner_config.clone();
        }
    }
    {
        let mut state = lock(shared);
        if state.generation != generation || !state.alive {
            return AgySteered::Returned;
        }
        // The turn ended while it was asked: the message opens the next one
        // on stdin, unless the mailbox holds mail it must follow.
        if state.open.is_empty() && state.turn_user_inputs == 0 && state.agy_queued.is_empty() {
            return AgySteered::Returned;
        }
        let had_open = !state.open.is_empty();
        if !running && had_open {
            // The previous turn in `agy` already finished (`CASCADE_RUN_STATUS_IDLE`)
            // while held open by a background task. Close those open messages now
            // so `turn_recovery.completed_seq` advances and never replays stale
            // completed turns on restart.
            finish_open(&mut state, TurnUsage::default());
            state.turns_started += 1;
        } else if !had_open {
            state.turns_started += 1;
        }
        if !state.open.contains(&id) {
            state.open.push(id.clone());
        }
    }
    // What the server writes for this message is written after this.
    let since = SystemTime::now();
    let transcript_from = agy_conversation_dir(&rpc, &session_id)
        .and_then(|dir| std::fs::metadata(dir.join("logs/transcript_full.jsonl")).ok())
        .map_or(0, |meta| meta.len());
    let sending = AgyQueued {
        id: id.clone(),
        cascade: session_id.clone(),
        mail: None,
        since,
        transcript_from,
    };
    {
        let mut state = lock(shared);
        if state.generation == generation
            && let Some(steer) = state.agy_steer.as_mut().filter(|steer| steer.id == id)
        {
            steer.sent = Some(sending.clone());
        }
    }
    let sent = agy_rpc_send_steer(
        port,
        &rpc.csrf_token,
        &session_id,
        &text,
        planner_config.as_ref(),
    );
    let queued = match &sent {
        Ok(()) => true,
        Err(AgyRpcError::Unreached(_)) => false,
        Err(AgyRpcError::Refused(reason))
            if AGY_REFUSALS.iter().any(|known| reason.contains(known)) =>
        {
            false
        }
        // No answer, or one that does not say what happened: the cascade's
        // own record does.
        Err(_) => agy_took(&rpc, &session_id, &id, since, transcript_from),
    };
    // Queued is not taken: the cascade's record decides, now and later.
    let held = queued.then(|| {
        let mut held = sending;
        let taken = agy_check(&rpc, &mut held);
        (held, taken)
    });
    let mut state = lock(shared);
    if state.generation != generation {
        return AgySteered::Returned;
    }
    if let Some((held, taken)) = held {
        state.idle_since = None;
        state.turn_user_inputs = state.turn_user_inputs.saturating_add(1);
        state.written.push(id.clone());
        if taken {
            state.agy_taken.insert(id);
        } else if !state.agy_taken.contains(&id) {
            state.agy_queued.push(held);
        }
        return AgySteered::Queued;
    }
    state.open.retain(|open| open != &id);
    match sent {
        // The cascade went idle after it was asked, and a new invocation
        // needs the planner config: the next steer waits out the turn's
        // close and sends it.
        Err(AgyRpcError::Refused(reason))
            if reason.contains("plan model not specified")
                && running
                && planner_config.is_none() =>
        {
            if !state.open.is_empty() {
                state.idle_since = Some(Instant::now());
            }
            drop(state);
            nudge_after(shared, wake, generation, AGY_TURN_CLOSE_GRACE);
            AgySteered::Returned
        }
        // The cascade refused it while no turn of this process runs and its
        // mailbox holds mail: no turn's end will reopen the way, so it is
        // asked again after a pause.
        Err(AgyRpcError::Refused(_)) if state.open.is_empty() && !state.agy_queued.is_empty() => {
            drop(state);
            agy_back_off(shared, wake, generation)
        }
        // The cascade refused it: the turn takes no more steers, and the
        // message waits for its end.
        Err(AgyRpcError::Refused(_)) => {
            state.agy_refused_turn = Some(state.turns_started);
            AgySteered::Returned
        }
        Err(AgyRpcError::Unreached(_)) => {
            drop(state);
            agy_forget_port(shared, &rpc, generation);
            agy_back_off(shared, wake, generation)
        }
        _ => {
            drop(state);
            agy_back_off(shared, wake, generation)
        }
    }
}

/// Look at the cascade's record for every message its mailbox holds, and
/// move each it took to [`State::agy_taken`], waking the host. Reads files
/// only, never under the state lock; a process replaced since settles
/// nothing.
fn agy_settle(shared: &Arc<Mutex<State>>, wake: &Condvar, rpc: &AgyRpcConfig, generation: u64) {
    let mut held = {
        let state = lock(shared);
        if state.generation != generation {
            return;
        }
        let mut held = state.agy_queued.clone();
        // A steer whose answer has not come yet is looked for too.
        held.extend(
            state
                .agy_steer
                .as_ref()
                .filter(|steer| steer.outcome.is_none())
                .and_then(|steer| steer.sent.clone()),
        );
        held
    };
    if held.is_empty() {
        return;
    }
    let taken: Vec<bool> = held.iter_mut().map(|held| agy_check(rpc, held)).collect();
    let mut state = lock(shared);
    if state.generation != generation {
        return;
    }
    let mut moved = false;
    for (looked, taken) in held.into_iter().zip(taken) {
        let at = state.agy_queued.iter().position(|q| q.id == looked.id);
        if taken {
            if let Some(at) = at {
                state.agy_queued.remove(at);
            }
            moved |= state.agy_taken.insert(looked.id);
        } else if let Some(at) = at {
            state.agy_queued[at] = looked;
        } else if let Some(steer) = state.agy_steer.as_mut().filter(|s| s.id == looked.id) {
            steer.sent = Some(looked);
        }
    }
    if moved {
        state.touch();
        drop(state);
        wake.notify_all();
    }
}

/// A language server that could not be reached at the port this process
/// kept is looked for again by the next steer.
fn agy_forget_port(shared: &Arc<Mutex<State>>, rpc: &AgyRpcConfig, generation: u64) {
    let mut state = lock(shared);
    if state.generation == generation {
        state.http_port = rpc.port_override;
    }
}

/// The language server's HTTP port, kept for this process, and its version
/// as the peer, read once from the head of `cli.log`.
fn agy_port(
    shared: &Arc<Mutex<State>>,
    rpc: &AgyRpcConfig,
    cached_port: Option<u16>,
    child_pid: Option<u32>,
    generation: u64,
) -> Option<u16> {
    let port = resolve_agy_http_port(rpc, cached_port, child_pid)?;
    let version = lock(shared)
        .peer
        .is_none()
        .then(|| agy_server_version(rpc, child_pid))
        .flatten();
    let mut state = lock(shared);
    if state.generation == generation && state.alive {
        state.http_port = Some(port);
        if let Some(version) = version {
            state.peer.get_or_insert(format!("agy {version}"));
        }
    }
    Some(port)
}

/// How much of `cli.log` the version is looked for in: it is the second line
/// of the language server's start.
const AGY_LOG_HEAD: u64 = 64 * 1024;

/// `Language server version: 1.5.8`, from the block of `cli.log` that starts
/// `child_pid`'s language server, or else from the first block (a child
/// started through a wrapper names another pid).
fn agy_server_version(rpc: &AgyRpcConfig, child_pid: Option<u32>) -> Option<String> {
    const START: &str = "Starting language server process with pid ";
    let log = std::fs::File::open(rpc.config_dir.join(".gemini/antigravity-cli/cli.log")).ok()?;
    let mut ours = child_pid.is_none();
    let mut first = None;
    for line in BufReader::new(log.take(AGY_LOG_HEAD)).lines() {
        let Ok(line) = line else { break };
        if let Some((_, pid)) = line.split_once(START) {
            ours = child_pid.is_none_or(|child| pid.trim().parse::<u32>() == Ok(child));
            continue;
        }
        if let Some((_, version)) = line.split_once("Language server version: ") {
            let version = version.trim().to_string();
            if ours {
                return Some(version);
            }
            first.get_or_insert(version);
        }
    }
    first
}

/// `brain/<cascade>/.system_generated` under the agent's config directory,
/// for a cascade id that names one directory.
fn agy_conversation_dir(rpc: &AgyRpcConfig, cascade: &str) -> Option<PathBuf> {
    let plain = !cascade.is_empty()
        && cascade
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    plain.then(|| {
        rpc.config_dir
            .join(".gemini/antigravity-cli/brain")
            .join(cascade)
            .join(".system_generated")
    })
}

/// Whether `cascade` holds the message `id` that a send which began at
/// `since` may have queued: a mailbox file written since then carries its
/// marker line (in its base64 `CortexStep`), or a `USER_INPUT` row of the
/// transcript past `transcript_from` does, where the cascade records a
/// message it already took. Each mailbox file outlives its delivery
/// (`read.json` marks it read).
fn agy_took(
    rpc: &AgyRpcConfig,
    cascade: &str,
    id: &str,
    since: SystemTime,
    transcript_from: u64,
) -> bool {
    let Some(dir) = agy_conversation_dir(rpc, cascade) else {
        return false;
    };
    agy_find_mail(&dir.join("messages"), id, since).is_some()
        || agy_transcript_takes(&dir.join("logs/transcript_full.jsonl"), transcript_from, id).0
}

/// Whether the cascade took the message `queued` holds: its mailbox file is
/// marked read, or a `USER_INPUT` row of the transcript names it. Finds the
/// mailbox file on the way, and moves past the transcript it read, so the
/// next look reads only what the cascade wrote since.
fn agy_check(rpc: &AgyRpcConfig, queued: &mut AgyQueued) -> bool {
    let Some(dir) = agy_conversation_dir(rpc, &queued.cascade) else {
        return false;
    };
    let messages = dir.join("messages");
    if queued.mail.is_none() {
        queued.mail = agy_find_mail(&messages, &queued.id, queued.since);
    }
    if queued
        .mail
        .as_ref()
        .is_some_and(|mail| agy_read_set(&messages).contains(mail))
    {
        return true;
    }
    let (taken, read_to) = agy_transcript_takes(
        &dir.join("logs/transcript_full.jsonl"),
        queued.transcript_from,
        &queued.id,
    );
    queued.transcript_from = read_to;
    taken
}

/// What a cascade's own record says about the messages it was given, read
/// before a process resumes it: every message a `USER_INPUT` row of its
/// transcript names was taken (an earlier process applied it), and every
/// message whose mailbox file is still marked undelivered
/// (`messages/undelivered/<mail>`) is the cascade's to take at its next
/// invocation, so it is held, in send order, rather than written again.
fn agy_scan(rpc: &AgyRpcConfig, cascade: &str) -> (HashSet<String>, Vec<AgyQueued>) {
    let mut taken = HashSet::new();
    let Some(dir) = agy_conversation_dir(rpc, cascade) else {
        return (taken, Vec::new());
    };
    let transcript_from = agy_transcript_rows(&dir.join("logs/transcript_full.jsonl"), 0, |id| {
        taken.insert(id);
    });
    let messages = dir.join("messages");
    let read = agy_read_set(&messages);
    let mut waiting = Vec::new();
    for entry in std::fs::read_dir(messages.join("undelivered"))
        .into_iter()
        .flatten()
        .flatten()
    {
        let Some(mail) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if read.contains(&mail) {
            continue;
        }
        let Ok(raw) = std::fs::read(messages.join(format!("{mail}.json"))) else {
            continue;
        };
        let Ok(record) = serde_json::from_slice::<Value>(&raw) else {
            continue;
        };
        let Some(id) = record
            .get("stepPayload")
            .and_then(Value::as_str)
            .and_then(agy_base64)
            .and_then(|step| agy_marker_id(&step))
        else {
            continue;
        };
        if taken.contains(&id) {
            continue;
        }
        let sent = record
            .get("timestamp")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        waiting.push((
            sent,
            AgyQueued {
                id,
                cascade: cascade.to_string(),
                mail: Some(mail),
                since: std::time::UNIX_EPOCH,
                transcript_from,
            },
        ));
    }
    waiting.sort_by(|a, b| a.0.cmp(&b.0));
    (taken, waiting.into_iter().map(|(_, held)| held).collect())
}

/// Allowance for file times against the clock read before a send.
const AGY_MAILBOX_SLACK: Duration = Duration::from_secs(2);

/// The mailbox file (`messages/<mail>.json`) written since `since` that
/// carries the marker line of `id`, raw or inside its base64 `CortexStep`.
fn agy_find_mail(messages: &Path, id: &str, since: SystemTime) -> Option<String> {
    let earliest = since
        .checked_sub(AGY_MAILBOX_SLACK)
        .unwrap_or(std::time::UNIX_EPOCH);
    for entry in std::fs::read_dir(messages).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "json")
            || path.file_name().is_some_and(|name| name == "read.json")
            || entry
                .metadata()
                .and_then(|meta| meta.modified())
                .is_ok_and(|written| written < earliest)
        {
            continue;
        }
        let Ok(raw) = std::fs::read(&path) else {
            continue;
        };
        let payload = serde_json::from_slice::<Value>(&raw)
            .ok()
            .and_then(|mail| mail.get("stepPayload")?.as_str().and_then(agy_base64));
        if agy_holds_marker(&raw, id) || payload.is_some_and(|step| agy_holds_marker(&step, id)) {
            return path.file_stem()?.to_str().map(str::to_string);
        }
    }
    None
}

/// The mailbox ids `messages/read.json` marks read.
fn agy_read_set(messages: &Path) -> HashSet<String> {
    std::fs::read(messages.join("read.json"))
        .ok()
        .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
        .and_then(|read| {
            read.as_object().map(|marks| {
                marks
                    .iter()
                    .filter(|(_, mark)| mark.as_bool() == Some(true))
                    .map(|(mail, _)| mail.clone())
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// Whether a `USER_INPUT` row of `transcript` past `from` names `id`, and
/// where the last whole row read ends.
fn agy_transcript_takes(transcript: &Path, from: u64, id: &str) -> (bool, u64) {
    let mut taken = false;
    let read_to = agy_transcript_rows(transcript, from, |row| taken |= row == id);
    (taken, read_to)
}

/// Call `row` with the message of every `USER_INPUT` row of `transcript`
/// past `from`, and answer where the last whole row ends: a row still being
/// written is read again next time.
fn agy_transcript_rows(transcript: &Path, from: u64, mut row: impl FnMut(String)) -> u64 {
    use std::io::{Seek as _, SeekFrom};
    let Ok(mut file) = std::fs::File::open(transcript) else {
        return from;
    };
    if file.seek(SeekFrom::Start(from)).is_err() {
        return from;
    }
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut read_to = from;
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) if line.last() != Some(&b'\n') => break,
            Ok(count) => {
                read_to += count as u64;
                if let Some(id) = agy_row_marker(&line) {
                    row(id);
                }
            }
        }
    }
    read_to
}

/// The message a transcript row shows the cascade took: a `USER_INPUT` row
/// whose user request opens with the marker line (`<USER_REQUEST>`, a
/// newline, `anyagents-message:<id>`, a newline, JSON-escaped). A marker
/// quoted further in, as a recovery note quotes the turn it recovers, is
/// not one.
fn agy_row_marker(line: &[u8]) -> Option<String> {
    const ROW: &str = "\"type\":\"USER_INPUT\"";
    const REQUEST: &str = "<USER_REQUEST>\\nanyagents-message:";
    let line = String::from_utf8_lossy(line);
    if !line.contains(ROW) {
        return None;
    }
    let rest = &line[line.find(REQUEST)? + REQUEST.len()..];
    agy_plain_id(&rest[..rest.find("\\n")?])
}

/// The message whose marker line opens the user input of a mailbox
/// `CortexStep`.
fn agy_marker_id(step: &[u8]) -> Option<String> {
    let marker = anyagents_harness::MESSAGE_MARKER_PREFIX.as_bytes();
    let at = step
        .windows(marker.len())
        .position(|window| window == marker)?
        + marker.len();
    let rest = &step[at..];
    let end = rest.iter().position(|byte| *byte == b'\n')?;
    agy_plain_id(std::str::from_utf8(&rest[..end]).ok()?)
}

/// `id` when it is a mailbox id: letters, digits, `_` and `-`.
fn agy_plain_id(id: &str) -> Option<String> {
    (!id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'))
    .then(|| id.to_string())
}

/// Whether `bytes` carry the marker line of `id`: the marker, then the end of
/// its line, raw or as JSON escapes it.
fn agy_holds_marker(bytes: &[u8], id: &str) -> bool {
    let marker = format!("{}{id}", anyagents_harness::MESSAGE_MARKER_PREFIX);
    let marker = marker.as_bytes();
    bytes.windows(marker.len()).enumerate().any(|(at, window)| {
        let rest = &bytes[at + marker.len()..];
        window == marker && (rest.starts_with(b"\n") || rest.starts_with(b"\\n"))
    })
}

/// Base64, standard or URL-safe, padded or not: protobuf JSON's `bytes`.
fn agy_base64(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let (mut acc, mut bits) = (0u32, 0u32);
    for byte in text.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' => break,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

/// How long an open cascade's stdout stays quiet before its run status is
/// asked, and the spacing between two asks.
const AGY_CASCADE_POLL: Duration = Duration::from_secs(1);

/// Longest wait between two attempts to find the language server's port.
const AGY_PORT_RETRY_MAX: Duration = Duration::from_secs(60);

/// The turn timer of one Antigravity child. A cascade held open by a
/// background task never prints its closing `result`, so while a turn is open
/// and stdout has been quiet for [`AGY_CASCADE_POLL`], ask the language server
/// once per interval whether the cascade still runs; one that stays idle past
/// [`AGY_TURN_CLOSE_GRACE`] closes the turn. Streaming lines prove the cascade
/// runs, so an active turn costs no requests, and an idle child costs none.
///
/// While the cascade's mailbox holds mail it has not taken, its record is
/// read once per [`AGY_CASCADE_POLL`] as well: a cascade can take it without
/// a line on stdout (an invocation it resumed on its own), and stdout's steps
/// only make the look come sooner.
fn watch_agy_cascade(
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    rpc: AgyRpcConfig,
    generation: u64,
) {
    let mut port_retry = AGY_CASCADE_POLL;
    let mut port_after: Option<Instant> = None;
    let mut last_settle: Option<Instant> = None;
    loop {
        let (session_id, child_pid, cached_port) = {
            let mut state = lock(&shared);
            loop {
                if !state.alive || state.generation != generation {
                    return;
                }
                let now = Instant::now();
                let settle_due = (!state.agy_queued.is_empty()
                    || state
                        .agy_steer
                        .as_ref()
                        .is_some_and(|steer| steer.outcome.is_none() && steer.sent.is_some()))
                .then(|| last_settle.map_or(now, |last| last + AGY_CASCADE_POLL));
                let status_due =
                    (!state.open.is_empty() && state.turn_user_inputs > 0).then(|| {
                        [state.last_rpc_health_poll, state.last_line_at]
                            .into_iter()
                            .flatten()
                            .max()
                            .map_or(now, |last| last + AGY_CASCADE_POLL)
                            .max(port_after.unwrap_or(now))
                    });
                let Some(due) = settle_due.into_iter().chain(status_due).min() else {
                    state = wake.wait(state).unwrap_or_else(|err| err.into_inner());
                    continue;
                };
                if now < due {
                    state = wake
                        .wait_timeout(state, due - now)
                        .unwrap_or_else(|err| err.into_inner())
                        .0;
                    continue;
                }
                if settle_due.is_some_and(|due| due <= now) {
                    last_settle = Some(now);
                    drop(state);
                    agy_settle(&shared, &wake, &rpc, generation);
                    state = lock(&shared);
                    continue;
                }
                state.last_rpc_health_poll = Some(now);
                if let Some(session_id) = state.session_id.clone() {
                    break (
                        session_id,
                        state.child.as_ref().map(Child::id),
                        state.http_port,
                    );
                }
            }
        };
        let Some(port) = resolve_agy_http_port(&rpc, cached_port, child_pid) else {
            port_after = Some(Instant::now() + port_retry);
            port_retry = (port_retry * 2).min(AGY_PORT_RETRY_MAX);
            continue;
        };
        port_after = None;
        port_retry = AGY_CASCADE_POLL;
        {
            let mut state = lock(&shared);
            if state.generation == generation && state.alive {
                state.http_port = Some(port);
            }
        }
        let Ok((resolved_sid, running)) = agy_rpc_is_running(port, &rpc.csrf_token, &session_id)
        else {
            continue;
        };
        let mut state = lock(&shared);
        if state.generation != generation || !state.alive || state.open.is_empty() {
            continue;
        }
        if resolved_sid != session_id {
            state.session_id = Some(resolved_sid);
            state.touch();
        }
        if running {
            state.idle_since = None;
            continue;
        }
        let now = Instant::now();
        let first_idle = state.idle_since.is_none();
        let idle_since = *state.idle_since.get_or_insert(now);
        if now.duration_since(idle_since) >= AGY_TURN_CLOSE_GRACE {
            finish_open(&mut state, TurnUsage::default());
            state.turn_user_inputs = 1;
            state.touch();
            drop(state);
            wake.notify_all();
        } else if first_idle {
            drop(state);
            nudge_after(
                &shared,
                &wake,
                generation,
                AGY_TURN_CLOSE_GRACE.saturating_sub(now.duration_since(idle_since)),
            );
        }
    }
}

/// Wake the host once `remain` has passed, when an input held back by a
/// time gate (the cascade-close grace, a recent steer probe, a turn the peer
/// could not start) may be offered again.
fn nudge_after(shared: &Arc<Mutex<State>>, wake: &Arc<Condvar>, generation: u64, remain: Duration) {
    let shared = Arc::clone(shared);
    let wake = Arc::clone(wake);
    let _ = thread::spawn(move || {
        thread::sleep(remain);
        let mut state = lock(&shared);
        if state.generation == generation && state.alive {
            state.touch();
        }
        drop(state);
        wake.notify_all();
    });
}

fn agy_rpc_is_running(
    port: u16,
    csrf_token: &str,
    session_id: &str,
) -> Result<(String, bool), String> {
    let resp = agy_rpc_post(port, csrf_token, "GetAllCascadeTrajectories", &json!({}))?;
    agy_running_status(&resp, session_id)
}

fn agy_running_status(resp: &Value, session_id: &str) -> Result<(String, bool), String> {
    let summaries = resp
        .get("trajectorySummaries")
        .and_then(Value::as_object)
        .ok_or("GetAllCascadeTrajectories missing trajectorySummaries")?;
    // Validate every candidate before inferring that no cascade is running.
    let statuses: Result<Vec<_>, String> = summaries
        .iter()
        .map(|(cid, entry)| {
            let running = match entry.get("status").and_then(Value::as_str) {
                Some("CASCADE_RUN_STATUS_RUNNING") => true,
                Some("CASCADE_RUN_STATUS_IDLE") => false,
                _ => return Err(format!("invalid cascade run status for {cid}")),
            };
            Ok((cid.clone(), running))
        })
        .collect();
    let statuses = statuses?;
    if let Some(entry) = statuses.iter().find(|(cid, _)| cid == session_id) {
        return Ok(entry.clone());
    }
    if let Some(entry) = statuses.iter().find(|(_, running)| *running) {
        return Ok(entry.clone());
    }
    if let [entry] = statuses.as_slice() {
        return Ok(entry.clone());
    }
    Ok((session_id.to_string(), false))
}

fn agy_rpc_is_latest_step_quota_error(
    port: u16,
    csrf_token: &str,
    session_id: &str,
) -> Result<bool, String> {
    let resp = agy_rpc_post(port, csrf_token, "GetAllCascadeTrajectories", &json!({}))?;
    let Some(step_count) = resp
        .get("trajectorySummaries")
        .and_then(|m| m.get(session_id))
        .and_then(|s| s.get("stepCount"))
        .and_then(Value::as_u64)
    else {
        return Ok(true);
    };
    if step_count == 0 {
        return Ok(false);
    }
    let offset = step_count.saturating_sub(2);
    let steps_resp = agy_rpc_post(
        port,
        csrf_token,
        "GetCascadeTrajectorySteps",
        &json!({ "cascadeId": session_id, "stepOffset": offset }),
    )?;
    let Some(last_step) = steps_resp
        .get("steps")
        .and_then(Value::as_array)
        .and_then(|arr| arr.last())
    else {
        return Ok(true);
    };
    agy_step_quota_error(last_step)
}

fn agy_step_quota_error(last_step: &Value) -> Result<bool, String> {
    let step_type = last_step
        .get("type")
        .and_then(Value::as_str)
        .filter(|kind| {
            kind.starts_with("CORTEX_STEP_TYPE_") && kind.len() > "CORTEX_STEP_TYPE_".len()
        })
        .ok_or("latest cascade step missing valid type")?;
    if step_type != "CORTEX_STEP_TYPE_ERROR_MESSAGE" {
        return Ok(false);
    }
    let status = last_step
        .get("status")
        .and_then(Value::as_str)
        .ok_or("latest cascade error step missing status")?;
    if status == "CORTEX_STEP_STATUS_CLEARED" {
        return Ok(false);
    }
    let raw = last_step.to_string().to_ascii_lowercase();
    Ok(raw.contains("resets in ") || raw.contains("resource_exhausted"))
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

/// `SendUserCascadeMessage`, waiting for its answer however long it takes:
/// an answer that is late is not a reason to send again.
fn agy_rpc_send_steer(
    port: u16,
    csrf_token: &str,
    session_id: &str,
    text: &str,
    planner_config: Option<&Value>,
) -> Result<(), AgyRpcError> {
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
    agy_rpc_exchange(port, csrf_token, "SendUserCascadeMessage", &payload, None).map(|_| ())
}

fn agy_rpc_cancel(port: u16, csrf_token: &str, session_id: &str) -> Result<(), String> {
    let payload = json!({
        "cascadeId": session_id
    });
    agy_rpc_post(port, csrf_token, "CancelCascadeInvocation", &payload).map(|_| ())
}

/// The delivery answer for what a harness already proved.
fn proof_outcome(proof: Proof) -> DeliverOutcome {
    match proof {
        Proof::Taken { turn_finished } => DeliverOutcome::Accepted { turn_finished },
        Proof::Duplicate => DeliverOutcome::Duplicate,
    }
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
    {
        if let Some(reset_ms) = parse_resets_at_ms(err_text) {
            return Some(reset_ms);
        }
        if let Some(delay_ms) = parse_resets_in_ms(err_text) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            return Some(now_ms.saturating_add(delay_ms));
        }
    }
    None
}

fn parse_resets_at_ms(text: &str) -> Option<i64> {
    let lower = text.to_ascii_lowercase();
    let idx = lower.find("resets at ")?;
    let after = text[idx + "resets at ".len()..].trim_start();
    let token = after
        .split_whitespace()
        .next()?
        .trim_end_matches(['.', ',', ';', ')', ']', '"']);
    parse_rfc3339_utc_ms(token)
}

fn parse_rfc3339_utc_ms(ts: &str) -> Option<i64> {
    let (date_part, rest) = ts.split_once(['T', 't'])?;
    let mut date_it = date_part.split('-');
    let year: i64 = date_it.next()?.parse().ok()?;
    let month: u32 = date_it.next()?.parse().ok()?;
    let day: u32 = date_it.next()?.parse().ok()?;
    if date_it.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let (time_core, offset_secs): (&str, i64) = if let Some(core) = rest.strip_suffix(['Z', 'z']) {
        (core, 0)
    } else {
        let idx = rest.rfind(['+', '-'])?;
        let (core, off) = rest.split_at(idx);
        let sign = if off.starts_with('-') { -1 } else { 1 };
        let mut off_it = off[1..].split(':');
        let oh: i64 = off_it.next()?.parse().ok()?;
        let om: i64 = off_it.next()?.parse().ok()?;
        if off_it.next().is_some() || !(0..=23).contains(&oh) || !(0..=59).contains(&om) {
            return None;
        }
        (core, sign * (oh * 3600 + om * 60))
    };

    let (hms, frac_ms) = match time_core.split_once('.') {
        Some((hms, frac)) => {
            if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let padded = format!("{frac:0<3}");
            let ms: i64 = padded[..3].parse().ok()?;
            (hms, ms)
        }
        None => (time_core, 0),
    };
    let mut hms_it = hms.split(':');
    let hour: i64 = hms_it.next()?.parse().ok()?;
    let min: i64 = hms_it.next()?.parse().ok()?;
    let sec: i64 = hms_it.next()?.parse().ok()?;
    if hms_it.next().is_some()
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&min)
        || !(0..=59).contains(&sec)
    {
        return None;
    }

    // Howard Hinnant days-from-civil (1970-01-01 = 0).
    let y = year - if month <= 2 { 1 } else { 0 };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m = i64::from(month);
    let d = i64::from(day);
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;

    let secs = days
        .checked_mul(86_400)?
        .checked_add(hour * 3600 + min * 60 + sec)?
        .checked_sub(offset_secs)?;
    secs.checked_mul(1_000)?.checked_add(frac_ms)
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

/// Inspect `<config_dir>/.anyagents-last-error.json` and the latest Antigravity
/// CLI log under `<config_dir>/.gemini/antigravity-cli/log/cli-*.log` to detect
/// an active quota reset timestamp (`> now_ms`) and the latest sanitized harness
/// delivery error after any profile/model rotation.
pub fn detect_agy_session_quota_and_error(
    config_dir: &Path,
    now_ms: i64,
) -> (Option<i64>, Option<String>) {
    static CACHE: std::sync::LazyLock<Mutex<HashMap<PathBuf, AgyEvidence>>> =
        std::sync::LazyLock::new(Mutex::default);
    let logs = lock(&CACHE)
        .get(config_dir)
        .map(|entry| entry.inputs.log_paths());
    if let Some(logs) = logs {
        let inputs = AgyInputs::stamp(config_dir, logs);
        if let Some(entry) = lock(&CACHE).get(config_dir)
            && entry.inputs == inputs
        {
            return entry.finding.clone().at(now_ms);
        }
    }
    let (inputs, finding) = AgyInputs::read(config_dir);
    lock(&CACHE).insert(
        config_dir.to_path_buf(),
        AgyEvidence {
            inputs,
            finding: finding.clone(),
        },
    );
    finding.at(now_ms)
}

/// What an Antigravity config directory says about its last turn before the
/// current time is applied. It depends only on files, so it is kept until
/// one of them changes: the scheduler asks every second, and the logs it
/// reads can be large.
#[derive(Clone)]
enum AgyFinding {
    Nothing,
    Quota {
        reset_at_ms: i64,
        error: Option<String>,
    },
    Error(Option<String>),
}

impl AgyFinding {
    fn at(self, now_ms: i64) -> (Option<i64>, Option<String>) {
        match self {
            Self::Nothing => (None, None),
            Self::Quota { reset_at_ms, error } if reset_at_ms > now_ms => {
                (Some(reset_at_ms), error)
            }
            Self::Quota { .. } => (None, None),
            Self::Error(error) => (None, error),
        }
    }
}

struct AgyEvidence {
    inputs: AgyInputs,
    finding: AgyFinding,
}

/// Stamps of every file the finding reads: the operator markers, the
/// persisted error, the log directory (a log created or removed) and its
/// three newest logs, whatever their age, since a log older than a marker
/// becomes relevant again when it is appended to.
#[derive(PartialEq, Eq)]
struct AgyInputs {
    rotated: Option<FileStamp>,
    cleared: Option<FileStamp>,
    last_error: Option<FileStamp>,
    log_dir: Option<FileStamp>,
    newest_logs: Vec<(PathBuf, Option<FileStamp>)>,
}

impl AgyInputs {
    fn markers(config_dir: &Path, newest_logs: Vec<(PathBuf, Option<FileStamp>)>) -> Self {
        Self {
            rotated: FileStamp::of(&config_dir.join(".anyagents-profile-rotated-ms")),
            cleared: FileStamp::of(&config_dir.join(".anyagents-quota-cleared-ms")),
            last_error: FileStamp::of(&config_dir.join(".anyagents-last-error.json")),
            log_dir: FileStamp::of(&config_dir.join(".gemini/antigravity-cli/log")),
            newest_logs,
        }
    }

    fn log_paths(&self) -> Vec<PathBuf> {
        self.newest_logs
            .iter()
            .map(|(path, _)| path.clone())
            .collect()
    }

    /// The stamps a cached finding is checked against, for the logs the last
    /// read found newest: a few `stat` calls, no listing and no file read.
    fn stamp(config_dir: &Path, logs: Vec<PathBuf>) -> Self {
        Self::markers(
            config_dir,
            logs.into_iter()
                .map(|path| {
                    let stamp = FileStamp::of(&path);
                    (path, stamp)
                })
                .collect(),
        )
    }

    /// Read every input and derive the finding, with the stamps taken
    /// before the reads so a change during them is seen next time.
    fn read(config_dir: &Path) -> (Self, AgyFinding) {
        let log_dir = config_dir.join(".gemini/antigravity-cli/log");
        let log_dir_stamp = FileStamp::of(&log_dir);
        let mut logs: Vec<(SystemTime, PathBuf, FileStamp)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&log_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !name.starts_with("cli-") || !name.ends_with(".log") {
                    continue;
                }
                if let Ok(meta) = entry.metadata()
                    && let Ok(mtime) = meta.modified()
                {
                    logs.push((mtime, path, FileStamp::from_metadata(&meta)));
                }
            }
        }
        logs.sort_by_key(|(mtime, _, _)| std::cmp::Reverse(*mtime));
        let mut inputs = Self::markers(
            config_dir,
            logs.iter()
                .take(3)
                .map(|(_, path, stamp)| (path.clone(), Some(*stamp)))
                .collect(),
        );
        // The listing above is as new as this stamp, not as the later one.
        inputs.log_dir = log_dir_stamp;
        let finding = agy_finding(config_dir, &logs);
        (inputs, finding)
    }
}

fn agy_finding(config_dir: &Path, logs: &[(SystemTime, PathBuf, FileStamp)]) -> AgyFinding {
    let rotated_ms = std::fs::read_to_string(config_dir.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let cleared_ms = std::fs::read_to_string(config_dir.join(".anyagents-quota-cleared-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let reset_floor_ms = rotated_ms.max(cleared_ms);
    if let Ok(raw) = std::fs::read_to_string(config_dir.join(".anyagents-last-error.json"))
        && let Ok(val) = serde_json::from_str::<Value>(&raw)
    {
        let ts = val.get("timestamp_ms").and_then(Value::as_i64).unwrap_or(0);
        if ts > reset_floor_ms {
            let err = val.get("error").and_then(Value::as_str).map(str::to_string);
            if let Some(raw_reset) = val.get("quota_reset_at_ms").and_then(Value::as_i64) {
                return AgyFinding::Quota {
                    reset_at_ms: raw_reset,
                    error: err,
                };
            }
            if err.is_some() {
                return AgyFinding::Error(err);
            }
        }
    }
    let recent = logs.iter().filter_map(|(mtime, path, _)| {
        let mtime_ms = mtime
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_millis() as i64;
        (mtime_ms > reset_floor_ms).then_some((mtime_ms, path))
    });
    for (mtime_ms, path) in recent.take(3) {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        for line in text.lines().rev() {
            if let Some((_, after)) = line.split_once("run ended with error and no response: ") {
                let err_str = after.trim().to_string();
                if let Some(dur_ms) = parse_resets_in_ms(&err_str) {
                    return AgyFinding::Quota {
                        reset_at_ms: mtime_ms.saturating_add(dur_ms),
                        error: Some(err_str),
                    };
                }
                return AgyFinding::Error(Some(err_str));
            }
            if line.contains("Sending user message to conversation")
                && !text.contains("run ended with error and no response:")
            {
                return AgyFinding::Nothing;
            }
        }
    }
    AgyFinding::Nothing
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
mod consumption_tests;
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
