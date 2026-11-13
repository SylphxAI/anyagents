//! Muse adapter.
//!
//! One child runs `muse serve`, which speaks JSON-RPC on its stdin and stdout
//! and owns the session. Startup sends `initialize` (its `serverInfo` names the
//! peer), `initialized`, then `session/start` (or `session/resume` when a
//! session id is already known) and `session/setApprovalMode` with `allowAll`,
//! since the host is the only client and nothing else answers the tool
//! approvals. An `approval/request` from the vendor is answered `{}`, the host
//! holds the approval; cancel sends `turn/cancel`.
//!
//! Delivery is next-step and idempotent (`docs/delivery.md`):
//! - Every message is one `turn/start` with `ifBusy: "steer"`: it opens a turn
//!   when none runs (the turn takes the command's id) and is written durably
//!   into the running turn's inbox otherwise. The request is sent without
//!   waiting; its answer arrives as `$rpc/response`. The mailbox id is the
//!   first line of the text (`anyagents-message:<id>`).
//! - The `commandId` is derived from the mailbox id alone, the same in every
//!   process and for every payload. Muse keeps every command's decision for
//!   the session and replays it for the same id, so writing a command again
//!   applies nothing.
//! - The answer only says the input is admitted, and it reads the same for a
//!   fresh admission and for the replay of one an earlier process wrote (its
//!   `queuedAtUs` is absent on `started`, on replays after a restart and on
//!   connections without `experimentalApi`, so it proves nothing). The proof
//!   that the model saw an input is the `userMessage` item carrying its
//!   `commandId` and the turn it joined. Resuming a session reads the items
//!   its record already holds (the resume's history, or `view/page` when the
//!   history did not fit Muse's budget): a message found there is a duplicate
//!   and is never written again, and every other admitted input is consumed
//!   while this process watches, so its item reaches it. That item's turn's
//!   `turn/completed` finishes it. A steer the turn never consumed is carried
//!   by the vendor into a successor turn named after its command, and stays in
//!   flight until that turn's proof.
//! - `-32030` (`commandRejected`) is a decision Muse records against the
//!   command id. With reason `command_id_conflict` the id is already held by
//!   an earlier write of this message (another payload): it was admitted, and
//!   waits for its proof like any other admitted input. With any other reason,
//!   and for `-32002` (input too large) and `-32602` (invalid params), the
//!   vendor refused the message for good: no write under its id can succeed,
//!   so it is rejected. An admission refusal (queue full, overloaded,
//!   draining, starting up) or an unknown outcome is deferred and written
//!   again under the same id once the vendor has moved on (a turn boundary,
//!   or the next pass when no turn runs); every answer wakes the host. A
//!   session fault (not found, not loaded, held elsewhere) fails delivery,
//!   and the supervisor restarts the process.
//! - A message is not written while an earlier one's `turn/start` is
//!   unanswered, so a refusal never lets a later message overtake it.
//! - The process is dead only once its stdout reached end of file, so every
//!   proof it printed is read first. A write that fails leaves the message
//!   for the same command id later.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState, TurnUsage, message_id_in_text,
};
use anyagents_harness_stream::{NotificationEffect, RpcChild, RpcFail, RpcLaunch};
use serde_json::{Value, json};

/// How long a startup or cancel request may take to come back. Delivery
/// never waits on a request.
const REQUEST_WAIT: Duration = Duration::from_secs(60);
/// A cold resume can take much longer than a request.
const RESUME_WAIT: Duration = Duration::from_secs(180);

/// `commandRejected`: a decision Muse records against the command id, with
/// `data.reason`; the same id answers it again.
const COMMAND_REJECTED: i64 = -32030;
/// The `commandRejected` reason for an id an earlier write holds with
/// another payload.
const COMMAND_ID_CONFLICT: [&str; 2] = ["command_id_conflict", "commandIdConflict"];
/// The frame or params are over the vendor's limit (`inputTooLarge`).
const INPUT_TOO_LARGE: i64 = -32002;
/// Malformed params (`invalidParams`).
const INVALID_PARAMS: i64 = -32602;
/// Session faults: no write into this process can succeed.
const SESSION_NOT_FOUND: i64 = -32020;
const SESSION_NOT_LOADED: i64 = -32024;
const WRITER_LEASE_HELD: i64 = -32021;
/// The most events one `view/page` serves.
const PAGE_LIMIT: u64 = 1_000;

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed("userMessage item per turn/start commandId"),
    peer: None,
};

/// One long-lived Muse `serve` child.
pub struct MuseHarness {
    rpc: RpcChild,
    shared: Arc<Mutex<State>>,
    /// Counts the lines the child printed, for [`MuseHarness::wait_until`].
    lines: Arc<(Mutex<u64>, Condvar)>,
}

/// Where one mailbox message stands with the vendor.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Stage {
    /// Its `turn/start` is out and unanswered.
    Sending,
    /// Queued in `turn`'s inbox, or the prompt of the turn it started. `None`:
    /// that turn ended first, and the vendor carries it into a successor.
    Admitted { turn: Option<String> },
    /// The model saw it in `turn`, which still runs.
    Consumed { turn: String },
    /// The turn that consumed it has completed.
    Finished,
    /// An earlier process of the session wrote it; the vendor holds it.
    Duplicate,
    /// Not applied, or not known to be: write it again once the vendor has
    /// passed `boundary`, or when no turn runs.
    Retry { boundary: u64 },
    /// The vendor refused it for good: no write under its id can succeed.
    Rejected(String),
    /// The session cannot take input in this process.
    Fault(String),
}

struct Input {
    seq: i64,
    command: String,
    stage: Stage,
}

#[derive(Default)]
struct State {
    /// The session this host drives.
    session: String,
    /// Session the server asked for, kept when the vendor did not name one yet.
    wanted_session: Option<String>,
    /// Working directory (`workspaceRoot`) for `session/start`.
    work_dir: Option<PathBuf>,
    /// What the process said about itself in `initialize`.
    peer: Option<String>,
    /// The inputs the session's record held when this process resumed it,
    /// by mailbox id and by command id: written by an earlier process.
    held: HashSet<String>,
    held_commands: HashSet<String>,
    /// The reader reached the end of the vendor's stdout: every line the
    /// process printed has been read, and nothing more will come.
    reader_done: bool,
    /// Told when the reader is done.
    watchers: Vec<Arc<dyn Fn() + Send + Sync>>,
    /// The turn that runs.
    running: Option<String>,
    /// Turns this process saw run, or that inputs joined.
    known_turns: HashSet<String>,
    completed_turns: HashSet<String>,
    /// Turns `turn/started` opened while this process ran.
    started_turns: HashSet<String>,
    /// Every message this process handled, by mailbox id.
    inputs: HashMap<String, Input>,
    /// Mailbox id by command id, for each command this process wrote.
    commands: HashMap<String, String>,
    /// Mailbox id by the request id of its unanswered `turn/start`.
    requests: HashMap<i64, String>,
    /// Answers that arrived before their request id was recorded.
    early: HashMap<i64, (Value, Value)>,
    /// Bumped by every turn boundary: a turn starting or completing, an
    /// input entering the conversation.
    boundary: u64,
    /// Bumped by every change the host reads through the adapter.
    version: u64,
    /// Ids this process wrote, in write order.
    written: Vec<String>,
    /// Usage of each turn this process saw complete.
    served: Vec<(String, TurnUsage)>,
    last_usage: Option<TurnUsage>,
    quota_reset_at_ms: Option<i64>,
}

impl State {
    /// The answer for a message the vendor has settled, whether or not the
    /// process still runs: a proof, the session's record, or a refusal for
    /// good.
    fn settled(&self, id: &str) -> Option<DeliverOutcome> {
        match self.inputs.get(id).map(|input| &input.stage) {
            Some(Stage::Consumed { .. }) => Some(DeliverOutcome::Accepted {
                turn_finished: false,
            }),
            Some(Stage::Finished) => Some(DeliverOutcome::Accepted {
                turn_finished: true,
            }),
            Some(Stage::Duplicate) => Some(DeliverOutcome::Duplicate),
            Some(Stage::Rejected(reason)) => Some(DeliverOutcome::Rejected {
                reason: reason.clone(),
            }),
            Some(_) => None,
            None => self.holds(id).then_some(DeliverOutcome::Duplicate),
        }
    }

    /// Whether the session's record held this message when this process
    /// resumed it.
    fn holds(&self, id: &str) -> bool {
        self.held.contains(id) || self.held_commands.contains(&command_id(id))
    }

    /// Whether the vendor has work of the host's in hand: a running turn, or
    /// a message written and not yet settled.
    fn busy(&self) -> bool {
        self.running.is_some()
            || self.inputs.values().any(|input| {
                matches!(
                    input.stage,
                    Stage::Sending | Stage::Admitted { .. } | Stage::Retry { .. }
                )
            })
    }

    fn changed(&mut self) {
        self.version = self.version.wrapping_add(1);
    }

    fn passed_boundary(&mut self) {
        self.boundary = self.boundary.wrapping_add(1);
        self.changed();
    }
}

impl MuseHarness {
    /// Start one child and complete the handshake.
    ///
    /// `program` is the `muse` executable, or a test double; it is invoked with
    /// `serve`. `session_id`, when set, is resumed instead of started.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        Self::start_in_with_args(program, &[], session_id, environment, None)
    }

    /// Start one child with extra CLI arguments after `serve`, its working
    /// directory set to `work_dir`, which a new session also takes as its
    /// workspace root.
    pub fn start_in_with_args(
        program: impl Into<PathBuf>,
        extra_args: &[String],
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let shared = Arc::new(Mutex::new(State {
            wanted_session: session_id.map(str::to_string),
            work_dir: work_dir.map(Path::to_path_buf),
            ..State::default()
        }));
        let on_shared = Arc::clone(&shared);
        let lines = Arc::new((Mutex::new(0u64), Condvar::new()));
        let on_lines = Arc::clone(&lines);
        // Dropped with the reader's line handler, once it read the last line.
        let reader_end = ReaderEnd {
            shared: Arc::clone(&shared),
            lines: Arc::clone(&lines),
        };
        let mut args = vec!["serve".to_string()];
        args.extend(extra_args.iter().cloned());
        let rpc = RpcChild::launch(
            RpcLaunch {
                name: "anyagents-muse".to_string(),
                program: program.into(),
                args,
                environment: environment
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
                work_dir: work_dir.map(Path::to_path_buf),
            },
            Arc::new(move |line| {
                let _reader = &reader_end;
                let unobserved = {
                    let mut state = lock(&on_shared);
                    let before = state.version;
                    on_notification(&mut state, line);
                    state.version == before
                };
                let (count, wake) = &*on_lines;
                *lock(count) += 1;
                wake.notify_all();
                NotificationEffect {
                    quota_reset_at_ms: None,
                    unobserved,
                }
            }),
            Arc::new(on_request),
        )?;
        let harness = Self { rpc, shared, lines };
        match harness.handshake() {
            Ok(()) => Ok(harness),
            // A process that went away during the handshake is a dead
            // harness, which its reader reports once it read every line.
            Err(err) if harness.rpc.alive() => Err(err),
            Err(_) => Ok(harness),
        }
    }

    /// Ids this process wrote, in write order (a message written again after
    /// a refusal appears again).
    pub fn written_ids(&self) -> Vec<String> {
        lock(&self.shared).written.clone()
    }

    /// Turns opened while this process ran.
    pub fn turns_started(&self) -> usize {
        lock(&self.shared).started_turns.len()
    }

    /// Whether a turn runs.
    pub fn in_turn(&self) -> bool {
        lock(&self.shared).running.is_some()
    }

    /// The session this host drives.
    pub fn session_id(&self) -> Option<String> {
        let state = lock(&self.shared);
        if !state.session.is_empty() {
            return Some(state.session.clone());
        }
        state.wanted_session.clone()
    }

    /// What the process said about itself in `initialize`.
    pub fn peer(&self) -> Option<String> {
        lock(&self.shared).peer.clone()
    }

    /// Wait until `done` holds for this harness or the deadline passes. It is
    /// checked again whenever the child prints a line.
    pub fn wait_until(&self, timeout: Duration, mut done: impl FnMut(&Self) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        let (count, wake) = &*self.lines;
        loop {
            let seen = *lock(count);
            if done(self) {
                return true;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            let guard = lock(count);
            if *guard == seen {
                let _ = wake.wait_timeout(guard, left);
            }
        }
    }

    fn handshake(&self) -> Result<(), HarnessError> {
        let init = self.call(
            "initialize",
            json!({
                "clientInfo": {"name": "anyagents", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {
                    "requestedCapabilities": ["userShell", "sessionMcp"],
                    "userInputDialogs": false
                }
            }),
            REQUEST_WAIT,
        )?;
        lock(&self.shared).peer = peer_of(&init);
        self.rpc.notify("initialized", json!({})).map_err(fail)?;
        let (wanted, work_dir) = {
            let state = lock(&self.shared);
            (state.wanted_session.clone(), state.work_dir.clone())
        };
        let start_params = || {
            let mut params = json!({
                "commandId": fresh_command_id(),
                "permissionProfile": ":unrestricted",
            });
            if let Some(dir) = &work_dir {
                params["workspaceRoot"] = json!(dir.display().to_string());
            }
            params
        };
        let resumed = match &wanted {
            Some(session) => match self.resume_session(session, true) {
                Ok(result) => Some(result),
                Err(err) if is_session_missing(&err) => None,
                Err(err) => return Err(err),
            },
            None => None,
        };
        // A resumed session without a workspace root is started afresh in
        // the working directory.
        let resumed = resumed.filter(|result| {
            work_dir.is_none()
                || result
                    .pointer("/session/workspaceRoot")
                    .and_then(Value::as_str)
                    .is_some()
        });
        let (result, resumed) = match resumed {
            Some(result) => (result, true),
            None => (
                self.call("session/start", start_params(), RESUME_WAIT)?,
                false,
            ),
        };
        let session = result
            .pointer("/session/sessionId")
            .and_then(Value::as_str)
            .or_else(|| result.get("sessionId").and_then(Value::as_str))
            .map(str::to_string)
            .or(wanted)
            .ok_or_else(|| HarnessError::Failed("session id missing".into()))?;
        let record = if resumed {
            self.read_record(&session, &result)?
        } else {
            Record::default()
        };
        {
            let mut state = lock(&self.shared);
            state.session = session.clone();
            state.held = record.ids;
            state.held_commands = record.commands;
            if let Some(turn) = active_turn(&result) {
                state.known_turns.insert(turn.clone());
                state.running = Some(turn);
            }
            state.changed();
        }
        self.call(
            "session/setApprovalMode",
            json!({
                "commandId": fresh_command_id(),
                "mode": "allowAll",
                "sessionId": session
            }),
            REQUEST_WAIT,
        )?;
        Ok(())
    }

    /// The inputs `session`'s record holds, from the history its resume
    /// served. When the history did not fit Muse's budget, the view is paged
    /// forward from its latest compaction boundary (from its start when it
    /// has none): an input an earlier process wrote and the host has not
    /// committed is newer than that boundary, since the process consumed it
    /// right before it went away. A view that cannot be read leaves what was
    /// read.
    fn read_record(&self, session: &str, resumed: &Value) -> Result<Record, HarnessError> {
        let mut record = Record::default();
        let history = resumed.get("history").unwrap_or(&Value::Null);
        let served = history.get("items").and_then(Value::as_array).or_else(|| {
            history
                .pointer("/snapshot/state/items")
                .and_then(Value::as_array)
        });
        if let Some(items) = served {
            for item in items {
                record.take(item);
            }
            return Ok(record);
        }
        let page = |cursor: Option<&str>, anchored: bool| {
            let mut params = json!({
                "sessionId": session,
                "direction": "forward",
                "limit": PAGE_LIMIT,
            });
            if let Some(cursor) = cursor {
                params["cursor"] = json!(cursor);
            } else if anchored {
                params["anchor"] = json!("latestCompaction");
            }
            self.call("view/page", params, REQUEST_WAIT)
        };
        let mut result = match page(None, true) {
            Err(HarnessError::Dead) => return Err(HarnessError::Dead),
            Err(_) => page(None, false),
            ok => ok,
        };
        loop {
            let page_result = match result {
                Ok(page_result) => page_result,
                Err(HarnessError::Dead) => return Err(HarnessError::Dead),
                Err(_) => return Ok(record),
            };
            for event in page_result
                .get("events")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(item) = event.pointer("/params/item") {
                    record.take(item);
                }
            }
            match page_result.get("nextCursor").and_then(Value::as_str) {
                Some(cursor) => result = page(Some(cursor), false),
                None => return Ok(record),
            }
        }
    }

    fn call(&self, method: &str, params: Value, wait: Duration) -> Result<Value, HarnessError> {
        self.rpc.request(method, params, wait).map_err(fail)
    }

    fn resume_session(
        &self,
        session: &str,
        include_permission_profile: bool,
    ) -> Result<Value, HarnessError> {
        const LOCK_RETRY_WAITS_MS: &[u64] = &[200, 400, 800, 1200, 1500];
        for wait_ms in LOCK_RETRY_WAITS_MS
            .iter()
            .copied()
            .map(Some)
            .chain(std::iter::once(None))
        {
            // Without `excludeItems`, the result serves the history the
            // record is read from.
            let mut params = json!({
                "commandId": fresh_command_id(),
                "sessionId": session,
            });
            if include_permission_profile {
                params["permissionProfile"] = json!(":unrestricted");
            }
            match self.call("session/resume", params, RESUME_WAIT) {
                Ok(result) => return Ok(result),
                Err(err) if is_session_lock_conflict(&err) => {
                    let Some(delay_ms) = wait_ms else {
                        return Err(err);
                    };
                    if !self.rpc.alive() {
                        return Err(HarnessError::Dead);
                    }
                    std::thread::sleep(Duration::from_millis(delay_ms));
                }
                Err(err) => return Err(err),
            }
        }
        unreachable!("retry loop always returns on the final attempt")
    }
}

impl Harness for MuseHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let params = {
            let mut state = lock(&self.shared);
            // What the vendor settled stands after the process died.
            if let Some(answer) = state.settled(&message.id) {
                return Ok(answer);
            }
            if state.reader_done {
                return Err(HarnessError::Dead);
            }
            let stage = state.inputs.get(&message.id).map(|input| &input.stage);
            match stage {
                Some(Stage::Fault(reason)) => {
                    let reason = reason.clone();
                    state.inputs.remove(&message.id);
                    return Err(HarnessError::Failed(reason));
                }
                Some(Stage::Sending | Stage::Admitted { .. }) => {
                    return Ok(DeliverOutcome::InFlight);
                }
                _ => {}
            }
            if let Some(reset_at_ms) = state.quota_reset_at_ms {
                return Ok(DeliverOutcome::Quota { reset_at_ms });
            }
            if let Some(Stage::Retry { boundary }) = stage
                && state.running.is_some()
                && state.boundary == *boundary
            {
                return Ok(DeliverOutcome::Deferred);
            }
            if state
                .inputs
                .values()
                .any(|input| input.stage == Stage::Sending && input.seq < message.seq)
            {
                return Ok(DeliverOutcome::Deferred);
            }
            // The process stopped taking input but has not printed its last
            // line yet; its end of file reports it dead and wakes the host.
            if !self.rpc.alive() {
                return Ok(DeliverOutcome::Deferred);
            }
            if state.session.is_empty() {
                return Err(HarnessError::Failed("no session".into()));
            }
            let command = command_id(&message.id);
            state.commands.insert(command.clone(), message.id.clone());
            state.inputs.insert(
                message.id.clone(),
                Input {
                    seq: message.seq,
                    command: command.clone(),
                    stage: Stage::Sending,
                },
            );
            state.written.push(message.id.clone());
            state.changed();
            // The same bytes on every write of this command: the vendor
            // replays a command only when its payload matches.
            json!({
                "commandId": command,
                "sessionId": state.session,
                "input": [{"type": "text", "text": compose_text(message)}],
                "ifBusy": "steer",
                "reasoningEffort": "max",
            })
        };
        let sent = self.rpc.send_request("turn/start", params);
        let mut state = lock(&self.shared);
        match sent {
            Ok(request) => {
                match state.early.remove(&request) {
                    Some((result, error)) => on_answer(&mut state, &message.id, &result, &error),
                    None => {
                        state.requests.insert(request, message.id.clone());
                    }
                }
                Ok(state
                    .settled(&message.id)
                    .unwrap_or(DeliverOutcome::InFlight))
            }
            Err(_) => {
                // The line was not written, or only in part, which the vendor
                // cannot parse. The message goes again under the same command,
                // which the vendor replays if it took it.
                let boundary = state.boundary;
                if let Some(input) = state.inputs.get_mut(&message.id)
                    && input.stage == Stage::Sending
                {
                    input.stage = Stage::Retry { boundary };
                }
                state.changed();
                Ok(DeliverOutcome::Deferred)
            }
        }
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        match lock(&self.shared).settled(message_id)? {
            DeliverOutcome::Accepted { turn_finished } => Some(Proof::Taken { turn_finished }),
            DeliverOutcome::Duplicate => Some(Proof::Duplicate),
            _ => None,
        }
    }

    /// Stop the child and read what it printed up to end of file; what it
    /// proved stays answerable.
    fn stop(&mut self) {
        self.rpc.stop_child();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if lock(&self.shared).reader_done {
            Err(HarnessError::Dead)
        } else {
            Ok(())
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        if lock(&self.shared).reader_done {
            return Err(HarnessError::Dead);
        }
        let result = self.resume_session(session, false)?;
        let resumed = result
            .pointer("/session/sessionId")
            .and_then(Value::as_str)
            .unwrap_or(session)
            .to_string();
        let record = self.read_record(&resumed, &result)?;
        let mut state = lock(&self.shared);
        if resumed != state.session {
            // Writes into another session mean nothing to this one.
            state.inputs.retain(|_, input| state_settled(&input.stage));
            state.held.clear();
            state.held_commands.clear();
        }
        state.held.extend(record.ids);
        state.held_commands.extend(record.commands);
        state.session = resumed;
        state.running = active_turn(&result);
        if let Some(turn) = state.running.clone() {
            state.known_turns.insert(turn);
        }
        state.changed();
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let (session, turn) = {
            let state = lock(&self.shared);
            (state.session.clone(), state.running.clone())
        };
        if let Some(turn) = turn
            && !session.is_empty()
        {
            self.call(
                "turn/cancel",
                json!({
                    "commandId": fresh_command_id(),
                    "sessionId": session,
                    "turnId": turn
                }),
                REQUEST_WAIT,
            )?;
        }
        let mut state = lock(&self.shared);
        state.running = None;
        state.changed();
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let state = lock(&self.shared);
        HarnessHealth {
            process_alive: !state.reader_done,
            turn_state: if state.busy() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: DELIVERY.with_peer(state.peer.clone()),
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }

    fn turns_served(&self) -> u32 {
        u32::try_from(lock(&self.shared).served.len()).unwrap_or(u32::MAX)
    }

    /// The total of every turn this process has finished, read when the
    /// mailbox has drained.
    fn served_usage(&self) -> TurnUsage {
        let state = lock(&self.shared);
        let mut total = TurnUsage::default();
        for (_, usage) in &state.served {
            total.input_tokens += usage.input_tokens;
            total.output_tokens += usage.output_tokens;
            total.cache_read_tokens += usage.cache_read_tokens;
            total.cache_write_tokens += usage.cache_write_tokens;
            total.reported_cost_micros =
                match (total.reported_cost_micros, usage.reported_cost_micros) {
                    (Some(total), Some(one)) => Some(total + one),
                    (total, one) => total.or(one),
                };
        }
        total
    }

    fn clear_quota(&mut self) {
        lock(&self.shared).quota_reset_at_ms = None;
    }

    fn process_id(&self) -> Option<u32> {
        self.rpc.process_id()
    }

    /// Lines the host reads wake `notify`, and so does the reader reaching
    /// the end of the vendor's stdout, which is when the process is dead.
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        lock(&self.shared).watchers.push(Arc::clone(&notify));
        self.rpc.watch_activity(notify)
    }
}

/// Owned by the reader's line handler, which the reader drops once it read
/// the vendor's last line: then, and only then, the process is dead.
struct ReaderEnd {
    shared: Arc<Mutex<State>>,
    lines: Arc<(Mutex<u64>, Condvar)>,
}

impl Drop for ReaderEnd {
    fn drop(&mut self) {
        let watchers = {
            let mut state = lock(&self.shared);
            state.reader_done = true;
            state.changed();
            std::mem::take(&mut state.watchers)
        };
        let (count, wake) = &*self.lines;
        *lock(count) += 1;
        wake.notify_all();
        for notify in watchers {
            notify();
        }
    }
}

/// The inputs a session's record holds: by mailbox id (the first line of
/// their text) and by command id.
#[derive(Default)]
struct Record {
    ids: HashSet<String>,
    commands: HashSet<String>,
}

impl Record {
    /// Note one served item, when it is an input that entered the
    /// conversation.
    fn take(&mut self, item: &Value) {
        if item.get("kind").and_then(Value::as_str) != Some("userMessage")
            || item.get("retracted").and_then(Value::as_bool) == Some(true)
        {
            return;
        }
        if let Some(command) = item.get("commandId").and_then(Value::as_str) {
            self.commands.insert(command.to_string());
        }
        if let Some(id) = item
            .get("text")
            .and_then(Value::as_str)
            .and_then(message_id_in_text)
        {
            self.ids.insert(id.to_string());
        }
    }
}

/// What stays answerable for a message after the session changes.
fn state_settled(stage: &Stage) -> bool {
    matches!(
        stage,
        Stage::Consumed { .. } | Stage::Finished | Stage::Duplicate | Stage::Rejected(_)
    )
}

/// The command id of one mailbox message: a UUIDv7, the shape Muse requires,
/// the same in every process and for every payload. Its time is the mailbox
/// id's own (a TypeID carries a UUIDv7), and the rest is a hash of the id.
pub fn command_id(message_id: &str) -> String {
    // Earlier adapters hashed a write number into the key; every id is the
    // one they gave a message's first write, so a message they wrote is
    // replayed, not written anew.
    const FIRST_WRITE: u32 = 0;
    let digest =
        fnv1a_128(format!("anyagents/muse/turn-start\0{message_id}\0{FIRST_WRITE}").as_bytes());
    let millis = typeid_millis(message_id).unwrap_or((digest >> 88) as u64 & 0xFF_FFFF_FFFF);
    let value = (u128::from(millis & 0xFFFF_FFFF_FFFF) << 80)
        | (0x7 << 76)
        | (((digest >> 64) & 0x0FFF) << 64)
        | (0b10 << 62)
        | (digest & 0x3FFF_FFFF_FFFF_FFFF);
    uuid_text(value)
}

/// The millisecond time of the first TypeID in `id` (`msg_…`, or a recovery
/// note's `recovery_msg_…_<n>`).
fn typeid_millis(id: &str) -> Option<u64> {
    const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
    id.split('_').find_map(|part| {
        let bytes = part.as_bytes();
        if bytes.len() != 26 || bytes[0] > b'7' {
            return None;
        }
        let mut value: u128 = 0;
        for byte in bytes {
            let digit = ALPHABET.iter().position(|c| c == byte)?;
            value = (value << 5) | digit as u128;
        }
        Some((value >> 80) as u64)
    })
}

fn fnv1a_128(bytes: &[u8]) -> u128 {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u128::from(*byte)).wrapping_mul(PRIME)
    })
}

/// A new UUIDv7 for a command that is not delivery (start, resume, approval
/// mode, cancel): each is sent once and waited on.
fn fresh_command_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let ms = (nanos / 1_000_000) & 0xFFFF_FFFF_FFFF;
    let rand_a = nanos & 0x0FFF;
    let rand_b = (nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 4) & 0x3FFF_FFFF_FFFF_FFFF;
    uuid_text((ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b)
}

fn uuid_text(value: u128) -> String {
    let hex = format!("{value:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// `muse <version>` from the `initialize` result.
fn peer_of(init: &Value) -> Option<String> {
    let version = match init.get("serverInfo") {
        Some(Value::String(version)) => Some(version.trim().to_string()),
        Some(info) => info
            .get("version")
            .and_then(Value::as_str)
            .map(|version| version.trim().to_string()),
        None => None,
    }
    .filter(|version| !version.is_empty());
    match version {
        Some(version) if version.to_ascii_lowercase().starts_with("muse") => Some(version),
        Some(version) => Some(format!("muse {version}")),
        None => init
            .get("userAgent")
            .and_then(Value::as_str)
            .filter(|agent| !agent.trim().is_empty())
            .map(str::to_string),
    }
}

fn active_turn(result: &Value) -> Option<String> {
    result
        .pointer("/session/activeTurnId")
        .or_else(|| result.get("activeTurnId"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn is_session_lock_conflict(err: &HarnessError) -> bool {
    let text = err.to_string().to_ascii_lowercase();
    text.contains("already in use") || text.contains("-32021")
}

fn is_session_missing(err: &HarnessError) -> bool {
    let text = err.to_string();
    text.contains("not found") || text.contains("sessionNotFound") || text.contains("-32020")
}

fn fail(fail: RpcFail) -> HarnessError {
    match fail {
        RpcFail::Dead => HarnessError::Dead,
        other => HarnessError::Failed(other.reason()),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

/// The answer to a request the vendor sends. An approval is the host's: the
/// empty result lets the vendor carry on, and its own next step is the answer.
fn on_request(method: &str, _params: &Value) -> Option<Value> {
    match method {
        "approval/request" | "ping" => Some(json!({})),
        _ => None,
    }
}

fn on_notification(state: &mut State, value: &Value) {
    let method = value.get("method").and_then(Value::as_str).unwrap_or("");
    let params = value.get("params").unwrap_or(&Value::Null);
    match method {
        "$rpc/response" => {
            let null = Value::Null;
            let result = params.get("result").unwrap_or(&null);
            let error = params.get("error").unwrap_or(&null);
            let Some(request) = params.get("id").and_then(Value::as_i64) else {
                return;
            };
            match state.requests.remove(&request) {
                Some(id) => on_answer(state, &id, result, error),
                None => {
                    state.early.insert(request, (result.clone(), error.clone()));
                }
            }
        }
        "turn/started" => on_turn_started(state, params),
        "item/completed" => on_item(state, params),
        "turn/completed" => on_turn_completed(state, params),
        "usage/updated" | "turn/usage" => state.last_usage = Some(usage_of(params)),
        "rate_limits/updated" | "rate_limit" => {
            if let Some(reset_at_ms) = quota_of(params) {
                state.quota_reset_at_ms = Some(reset_at_ms);
                state.changed();
            }
        }
        _ => {
            if let Some(reset_at_ms) = quota_in_text(params) {
                state.quota_reset_at_ms = Some(reset_at_ms);
                state.changed();
            }
        }
    }
}

/// The vendor answered the `turn/start` that wrote `id`. Every answer wakes
/// the host: an admitted input waits for its proof, a refusal for its next
/// write.
fn on_answer(state: &mut State, id: &str, result: &Value, error: &Value) {
    let held = state.holds(id);
    let State {
        inputs,
        completed_turns,
        known_turns,
        boundary,
        quota_reset_at_ms,
        ..
    } = state;
    let Some(input) = inputs.get_mut(id) else {
        return;
    };
    // A proof that arrived first already settled it.
    if input.stage != Stage::Sending {
        return;
    }
    if error.is_null() {
        // A fresh admission and the replay of one an earlier process wrote
        // answer alike. Either way the vendor holds the input unconsumed: one
        // the session had consumed was in its record, and is never written.
        // Its `userMessage` item proves it.
        let turn = result.get("turnId").and_then(Value::as_str);
        if let Some(turn) = turn {
            known_turns.insert(turn.to_string());
        }
        input.stage = Stage::Admitted {
            turn: turn
                .filter(|turn| !completed_turns.contains(*turn))
                .map(str::to_string),
        };
        state.changed();
        return;
    }
    let reason = RpcFail::Failed(error.clone()).reason();
    input.stage = if let Some(reset_at_ms) = quota_from_error(error) {
        *quota_reset_at_ms = Some(reset_at_ms);
        Stage::Retry {
            boundary: *boundary,
        }
    } else {
        match error.get("code").and_then(Value::as_i64) {
            // An earlier write of this message holds the id: it was admitted.
            Some(COMMAND_REJECTED) if is_id_conflict(error) => {
                if held {
                    Stage::Duplicate
                } else {
                    Stage::Admitted { turn: None }
                }
            }
            // Recorded against the id, or the message itself is malformed or
            // too large: no write of it can succeed.
            Some(COMMAND_REJECTED | INPUT_TOO_LARGE | INVALID_PARAMS) => Stage::Rejected(reason),
            Some(SESSION_NOT_FOUND | SESSION_NOT_LOADED | WRITER_LEASE_HELD) => {
                Stage::Fault(reason)
            }
            // Refused at admission (queue full, overloaded, draining, starting
            // up), or the outcome is unknown (interrupted, cancelled, a
            // malformed answer): the same command again either applies it or
            // replays what the vendor did with it, at the next boundary, or
            // on the next pass when no turn runs.
            _ => Stage::Retry {
                boundary: *boundary,
            },
        }
    };
    state.changed();
}

/// A `commandRejected` whose reason is that the id is already held.
fn is_id_conflict(error: &Value) -> bool {
    error
        .pointer("/data/reason")
        .and_then(Value::as_str)
        .is_some_and(|reason| COMMAND_ID_CONFLICT.contains(&reason))
}

fn on_turn_started(state: &mut State, params: &Value) {
    if let Some(turn) = params.get("turnId").and_then(Value::as_str) {
        state.known_turns.insert(turn.to_string());
        state.started_turns.insert(turn.to_string());
        if !state.completed_turns.contains(turn) {
            state.running = Some(turn.to_string());
        }
        // A turn named after a command is that command's own: a fresh start,
        // or the successor that carries a steer its turn never consumed.
        for input in state.inputs.values_mut() {
            if input.command == turn
                && matches!(input.stage, Stage::Sending | Stage::Admitted { turn: None })
            {
                input.stage = Stage::Admitted {
                    turn: Some(turn.to_string()),
                };
            }
        }
    }
    state.passed_boundary();
}

/// A `userMessage` item: the input entered the conversation.
fn on_item(state: &mut State, params: &Value) {
    let Some(item) = params.get("item") else {
        return;
    };
    if item.get("kind").and_then(Value::as_str) != Some("userMessage")
        || item.get("retracted").and_then(Value::as_bool) == Some(true)
    {
        return;
    }
    let Some(turn) = item.get("turnId").and_then(Value::as_str) else {
        return;
    };
    let command = item.get("commandId").and_then(Value::as_str);
    let Some(id) = command
        .and_then(|command| state.commands.get(command).cloned())
        .or_else(|| {
            item.get("text")
                .and_then(Value::as_str)
                .and_then(message_id_in_text)
                .map(str::to_string)
        })
    else {
        return;
    };
    let stage = if state.completed_turns.contains(turn) {
        Stage::Finished
    } else {
        Stage::Consumed {
            turn: turn.to_string(),
        }
    };
    state.known_turns.insert(turn.to_string());
    match state.inputs.get_mut(&id) {
        Some(input) => {
            if !matches!(
                input.stage,
                Stage::Finished | Stage::Duplicate | Stage::Rejected(_)
            ) {
                input.stage = stage;
            }
        }
        // Written by an earlier process, carried into a turn of this one.
        None => {
            state.inputs.insert(
                id,
                Input {
                    seq: 0,
                    command: command.unwrap_or_default().to_string(),
                    stage,
                },
            );
        }
    }
    state.passed_boundary();
}

/// A turn ended: it finishes the inputs it consumed; a steer it never
/// consumed waits for the successor the vendor starts for it.
fn on_turn_completed(state: &mut State, params: &Value) {
    let hit_quota = quota_of(params);
    if let Some(reset_at_ms) = hit_quota {
        state.quota_reset_at_ms = Some(reset_at_ms);
    }
    let Some(turn) = params
        .get("turnId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| state.running.clone())
    else {
        state.passed_boundary();
        return;
    };
    if state.running.as_deref() == Some(turn.as_str()) {
        state.running = None;
    }
    if !state.completed_turns.insert(turn.clone()) {
        state.passed_boundary();
        return;
    }
    let mut joined = state.known_turns.contains(&turn);
    for input in state.inputs.values_mut() {
        match &input.stage {
            Stage::Consumed { turn: consumed } if *consumed == turn => {
                joined = true;
                // A turn the quota cut short did not answer its input: the
                // input stays consumed and the turn unfinished.
                if hit_quota.is_none() {
                    input.stage = Stage::Finished;
                }
            }
            Stage::Admitted {
                turn: Some(admitted),
            } if *admitted == turn => {
                joined = true;
                input.stage = Stage::Admitted { turn: None };
            }
            _ => {}
        }
    }
    if joined && hit_quota.is_none() {
        let usage = params
            .get("usage")
            .map(usage_of)
            .or_else(|| state.last_usage.clone())
            .unwrap_or_default();
        state.served.push((turn, usage));
    }
    state.passed_boundary();
}

fn usage_of(value: &Value) -> TurnUsage {
    let usage = value.get("usage").unwrap_or(value);
    TurnUsage {
        input_tokens: u64_field(usage, "inputTokens")
            .max(u64_field(usage, "input_tokens"))
            .max(u64_field(usage, "prompt_tokens")),
        output_tokens: u64_field(usage, "outputTokens")
            .max(u64_field(usage, "output_tokens"))
            .max(u64_field(usage, "completion_tokens")),
        cache_read_tokens: u64_field(usage, "cachedInputTokens")
            .max(u64_field(usage, "cache_read_input_tokens")),
        cache_write_tokens: u64_field(usage, "cacheWriteInputTokens")
            .max(u64_field(usage, "cache_creation_input_tokens")),
        reported_cost_micros: usage
            .get("costMicros")
            .or_else(|| usage.get("cost_micros"))
            .and_then(Value::as_i64),
    }
}

fn u64_field(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn quota_of(value: &Value) -> Option<i64> {
    for pointer in ["/resetsAt", "/resets_at_ms", "/rateLimit/resetsAt"] {
        if let Some(n) = value.pointer(pointer).and_then(Value::as_i64) {
            return Some(epoch_ms(n));
        }
    }
    quota_in_text(value)
}

fn quota_in_text(value: &Value) -> Option<i64> {
    let raw = value.to_string();
    let lower = raw.to_ascii_lowercase();
    if !lower.contains("quota") && !lower.contains("rate_limit") && !lower.contains("429") {
        return None;
    }
    let idx = lower.find("resets at ")?;
    let after = &raw[idx + "resets at ".len()..];
    let token = after.split_whitespace().next()?;
    parse_iso_utc_ms(token)
}

fn parse_iso_utc_ms(s: &str) -> Option<i64> {
    let s = s.trim_matches(|c: char| !c.is_ascii_alphanumeric());
    let s = s.strip_suffix('Z').or_else(|| s.strip_suffix('z'))?;
    let (date, time) = s.split_once('T').or_else(|| s.split_once(' '))?;
    let mut d = date.split('-');
    let y: i64 = d.next()?.parse().ok()?;
    let m: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&day) {
        return None;
    }
    let time_main = time.split('.').next()?;
    let mut t = time_main.split(':');
    let hh: i64 = t.next()?.parse().ok()?;
    let mm: i64 = t.next()?.parse().ok()?;
    let ss: i64 = t.next().unwrap_or("0").parse().ok()?;
    if hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    let y_adj = if m <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let secs = days * 86_400 + hh * 3_600 + mm * 60 + ss;
    Some(secs.saturating_mul(1_000))
}

/// A 429 answer, or one whose text names a quota, is a wait until the reset
/// time it carries. Without a reset time the vendor did not say when to try
/// again, so it is an ordinary refusal.
fn quota_from_error(error: &Value) -> Option<i64> {
    if error.get("code").and_then(Value::as_i64) != Some(429)
        && !error.to_string().to_lowercase().contains("quota")
    {
        return None;
    }
    quota_of(error.get("data").unwrap_or(error))
}

fn epoch_ms(n: i64) -> i64 {
    if n >= 1_000_000_000_000 {
        n
    } else {
        n.saturating_mul(1_000)
    }
}

/// The user step one mailbox message becomes.
fn compose_text(message: &OutboundMessage) -> String {
    let context = &message.context;
    let known = context.sender.is_some()
        || context.chat_id.is_some()
        || context.message_id.is_some()
        || context.destination.is_some();
    if !known {
        return anyagents_harness::marked_message(&message.id, &message.text);
    }
    anyagents_harness::compose_message(&message.id, context, &message.text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_iso_utc_reset_in_muse_rate_limit_error() {
        let params = json!({
            "status": "failed",
            "error": {
                "message": "API error 429 [request_id=5f55e49a-bec1-4c8f-9aee-f57fd86f5168]: Subscription quota exhausted. Your usage window resets at 2026-10-26T00:00:00Z. (rate_limit_error)"
            }
        });
        assert_eq!(quota_of(&params), Some(1_792_972_800_000));
    }

    #[test]
    fn a_command_id_is_a_uuid7_fixed_by_the_message_alone() {
        let id = "msg_01m78v9pswehq8ns457b0vdz42";
        let first = command_id(id);
        assert_eq!(first, command_id(id), "the same in every process");
        // The id an earlier adapter gave this message's first write, so a
        // message it wrote is replayed rather than written anew.
        assert_eq!(first, "01a1d1b4-db3c-7408-97fe-f55acf1aaa64");
        assert_ne!(first, command_id("msg_01m78vwxx4eh69a0g459dnsd32"));
        let hex: String = first.chars().filter(|c| *c != '-').collect();
        assert_eq!(first.len(), 36);
        assert_eq!(&hex[12..13], "7", "version 7: {first}");
        assert!(
            matches!(&hex[16..17], "8" | "9" | "a" | "b"),
            "RFC variant: {first}"
        );
        // Its time is the message's own: the TypeID's first 48 bits.
        let millis = u64::from_str_radix(&hex[0..12], 16).unwrap();
        assert_eq!(Some(millis), typeid_millis(id));
        assert!(millis > 1_790_000_000_000 && millis < 1_800_000_000_000);
        // A recovery note for that message shares its time, not its id.
        let note = command_id(&format!("recovery_{id}_1"));
        assert_eq!(&note.replace('-', "")[0..12], &hex[0..12]);
        assert_ne!(note, first);
    }

    #[test]
    fn the_peer_is_the_server_version_from_the_handshake() {
        assert_eq!(
            peer_of(&json!({"serverInfo": "1.4.18-R8587.1", "userAgent": "x"})).as_deref(),
            Some("muse 1.4.18-R8587.1")
        );
        assert_eq!(
            peer_of(&json!({"serverInfo": {"name": "muse", "version": "1.4.18"}})).as_deref(),
            Some("muse 1.4.18")
        );
        assert_eq!(
            peer_of(&json!({"userAgent": "muse/1.4.18"})).as_deref(),
            Some("muse/1.4.18")
        );
        assert_eq!(peer_of(&json!({})), None);
    }

    fn sending(state: &mut State, id: &str, seq: i64) {
        let command = command_id(id);
        state.commands.insert(command.clone(), id.to_string());
        state.inputs.insert(
            id.to_string(),
            Input {
                seq,
                command,
                stage: Stage::Sending,
            },
        );
    }

    fn stage(state: &State, id: &str) -> Stage {
        state.inputs[id].stage.clone()
    }

    fn rejection(code: i64, reason: Option<&str>) -> Value {
        let mut error = json!({"code": code, "message": "refused"});
        if let Some(reason) = reason {
            error["data"] = json!({"commandId": "c", "reason": reason});
        }
        error
    }

    fn user_message(id: &str, turn: &str) -> Value {
        json!({"item": {"kind": "userMessage", "turnId": turn, "commandId": command_id(id),
            "text": format!("anyagents-message:{id}\nhello")}})
    }

    #[test]
    fn a_replayed_ack_without_queued_at_waits_for_its_item_and_settles() {
        // A second process resumed a session whose turn `t1` an earlier
        // process ran: `steer` was admitted into it and never consumed.
        let mut state = State {
            running: Some("t2".into()),
            known_turns: HashSet::from(["t2".to_string()]),
            ..State::default()
        };
        for id in ["started", "steer"] {
            sending(&mut state, id, 1);
        }
        // Replays after a restart carry no `queuedAtUs`, and `started` acks
        // never do: neither tells a replay from a fresh admission.
        let replay = |disposition: &str, turn: &str| {
            json!({"commandId": "c", "status": "accepted", "disposition": disposition,
                "turnId": turn, "startedNewTurn": disposition == "started"})
        };
        on_answer(&mut state, "steer", &replay("steered", "t1"), &Value::Null);
        on_answer(&mut state, "started", &replay("started", "c"), &Value::Null);
        assert_eq!(
            stage(&state, "steer"),
            Stage::Admitted {
                turn: Some("t1".into())
            }
        );
        assert_eq!(state.settled("steer"), None, "admitted is not proved");
        // The vendor carries the steer into a successor named after its
        // command, whose item settles it.
        let successor = command_id("steer");
        on_turn_started(&mut state, &json!({"turnId": successor}));
        on_item(&mut state, &user_message("steer", &successor));
        assert_eq!(
            state.settled("steer"),
            Some(DeliverOutcome::Accepted {
                turn_finished: false
            })
        );
        on_turn_completed(&mut state, &json!({"turnId": successor}));
        assert_eq!(
            state.settled("steer"),
            Some(DeliverOutcome::Accepted {
                turn_finished: true
            })
        );
    }

    #[test]
    fn the_session_record_settles_what_an_earlier_process_wrote() {
        let mut record = Record::default();
        let note = |id: &str, kind: &str| {
            json!({"kind": kind, "turnId": "t1", "commandId": command_id(id),
                "text": format!("anyagents-message:{id}\nhello")})
        };
        record.take(&note("consumed", "userMessage"));
        record.take(&note("answer", "agentMessage"));
        record.take(&json!({"kind": "userMessage", "turnId": "t1",
            "commandId": "01a1d1b4-0000-7000-8000-000000000000",
            "text": "anyagents-message:other-command\nhello"}));
        let mut retracted = note("retracted", "userMessage");
        retracted["retracted"] = json!(true);
        record.take(&retracted);
        let state = State {
            held: record.ids,
            held_commands: record.commands,
            ..State::default()
        };
        assert_eq!(state.settled("consumed"), Some(DeliverOutcome::Duplicate));
        // Written under another command id, found by its first line.
        assert_eq!(
            state.settled("other-command"),
            Some(DeliverOutcome::Duplicate)
        );
        assert_eq!(state.settled("answer"), None);
        assert_eq!(state.settled("retracted"), None);
        assert_eq!(state.settled("fresh"), None);
    }

    #[test]
    fn each_refusal_is_settled_by_what_it_means() {
        let mut state = State {
            running: Some("t1".into()),
            boundary: 7,
            held: HashSet::from(["held-conflict".to_string()]),
            ..State::default()
        };
        let cases = [
            (
                "conflict",
                rejection(COMMAND_REJECTED, Some("command_id_conflict")),
            ),
            (
                "held-conflict",
                rejection(COMMAND_REJECTED, Some("command_id_conflict")),
            ),
            (
                "recorded",
                rejection(COMMAND_REJECTED, Some("runtime_busy")),
            ),
            ("bare", rejection(COMMAND_REJECTED, None)),
            ("too-large", rejection(INPUT_TOO_LARGE, None)),
            ("invalid", rejection(INVALID_PARAMS, None)),
            ("queue-full", rejection(-32031, None)),
            ("overloaded", rejection(-32001, None)),
            ("interrupted", rejection(-32013, None)),
            ("unloaded", rejection(SESSION_NOT_LOADED, None)),
        ];
        for (seq, (id, error)) in cases.iter().enumerate() {
            sending(&mut state, id, seq as i64);
            let before = state.version;
            on_answer(&mut state, id, &Value::Null, error);
            assert_ne!(state.version, before, "{id}: every answer wakes the host");
        }
        // The id is held by an earlier write of the message: admitted, and
        // waiting for its proof, or the record already holds it.
        assert_eq!(stage(&state, "conflict"), Stage::Admitted { turn: None });
        assert_eq!(state.settled("conflict"), None);
        assert_eq!(stage(&state, "held-conflict"), Stage::Duplicate);
        for id in ["recorded", "bare", "too-large", "invalid"] {
            assert!(
                matches!(state.settled(id), Some(DeliverOutcome::Rejected { .. })),
                "{id}: {:?}",
                stage(&state, id)
            );
        }
        assert_eq!(
            state.settled("too-large"),
            Some(DeliverOutcome::Rejected {
                reason: "-32002 refused".into()
            })
        );
        for id in ["queue-full", "overloaded", "interrupted"] {
            assert_eq!(stage(&state, id), Stage::Retry { boundary: 7 }, "{id}");
        }
        assert!(matches!(stage(&state, "unloaded"), Stage::Fault(_)));
        // A proof never overturns a refusal for good.
        on_item(&mut state, &user_message("recorded", "t1"));
        assert!(matches!(stage(&state, "recorded"), Stage::Rejected(_)));
    }

    #[test]
    fn a_turn_finishes_what_it_consumed_and_leaves_its_unconsumed_steer_in_flight() {
        let mut state = State::default();
        for (id, seq) in [("prompt", 1), ("steer", 2), ("next", 3)] {
            sending(&mut state, id, seq);
        }
        let started = |command: &str| json!({"turnId": command, "commandId": command});
        let prompt = command_id("prompt");
        on_turn_started(&mut state, &started(&prompt));
        on_item(
            &mut state,
            &json!({"item": {"kind": "userMessage", "turnId": prompt, "commandId": prompt}}),
        );
        on_answer(
            &mut state,
            "steer",
            &json!({"disposition": "steered", "turnId": prompt}),
            &Value::Null,
        );
        on_turn_completed(
            &mut state,
            &json!({"turnId": prompt, "usage": {"inputTokens": 4}}),
        );
        assert_eq!(stage(&state, "prompt"), Stage::Finished);
        assert_eq!(stage(&state, "steer"), Stage::Admitted { turn: None });
        assert_eq!(stage(&state, "next"), Stage::Sending);
        assert!(state.busy(), "the carried steer is still the vendor's");
        assert_eq!(state.served.len(), 1);
        // The successor named after the steer's command proves it.
        let successor = command_id("steer");
        on_turn_started(&mut state, &started(&successor));
        on_item(
            &mut state,
            &json!({"item": {"kind": "userMessage", "turnId": successor,
                "commandId": successor, "steered": true}}),
        );
        assert_eq!(
            stage(&state, "steer"),
            Stage::Consumed {
                turn: successor.clone()
            }
        );
        // A refusal at admission waits for the next boundary while a turn
        // runs, under the same command id.
        on_answer(&mut state, "next", &Value::Null, &rejection(-32031, None));
        assert_eq!(
            stage(&state, "next"),
            Stage::Retry {
                boundary: state.boundary
            }
        );
        assert_eq!(state.inputs["next"].command, command_id("next"));
    }
}
