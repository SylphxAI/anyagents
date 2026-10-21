//! Muse adapter.
//!
//! One child runs `muse serve`, which speaks JSON-RPC on its stdin and stdout
//! and owns the session. Startup sends `initialize`, `initialized`, then
//! `session/start` (or `session/resume` when a session id is already known) and
//! `session/setApprovalMode` with `allowAll`, since the host is the only client
//! and nothing else answers the tool approvals.
//!
//! The mailbox id is the first line of the user text (`anyagents-message:<id>`).
//! An id already written, or echoed back, is not written again.
//!
//! Declared delivery mode is steer: `deliver` sends `turn/start` when no turn is
//! open and, while one is open, `turn/start` with `ifBusy: "steer"`, which puts
//! the input into the running turn. `turn/started` opens a turn,
//! `turn/completed` closes it and carries its usage. A turn that ends with an
//! error leaves its messages accepted. An `approval/request` from the vendor is
//! answered `{}`, the host holds the approval; cancel sends `turn/cancel`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnReport, TurnState, TurnUsage, marked_message, message_id_in_text,
};
use anyagents_harness_stream::{NotificationEffect, RpcChild, RpcFail, RpcLaunch};
use serde_json::{Value, json};

/// How long one request may take to come back.
const RPC_WAIT: Duration = Duration::from_secs(5);
/// A cold resume can take much longer than a request.
const RESUME_WAIT: Duration = Duration::from_secs(180);

/// One long-lived Muse `serve` child.
pub struct MuseHarness {
    rpc: RpcChild,
    shared: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    /// The session this host drives.
    session: String,
    /// Session the server asked for, kept when the vendor did not name one yet.
    wanted_session: Option<String>,
    /// The open turn.
    turn: Option<String>,
    /// Messages an accepted `turn/start` took while no turn was open.
    taken: Vec<String>,
    /// Messages the open turn carries.
    open: Vec<String>,
    seen: HashSet<String>,
    written: Vec<String>,
    finished: HashSet<String>,
    turns_started: usize,
    usages: Vec<(String, TurnUsage)>,
    reports: Vec<(String, TurnReport)>,
    quota_reset_at_ms: Option<i64>,
    last_usage: Option<TurnUsage>,
}

/// The session id `result` names, for `session/start` or `session/resume`, or
/// `wanted` when the vendor did not name one.
fn session_from(result: &Value, wanted: Option<String>) -> Option<String> {
    result
        .pointer("/session/sessionId")
        .and_then(Value::as_str)
        .or_else(|| result.get("sessionId").and_then(Value::as_str))
        .map(str::to_string)
        .or(wanted)
}

/// `true` when the vendor said this session is unknown rather than that it
/// could not be reached. Only that answer starts a new session.
fn is_refusal(session: &str, error: &str) -> bool {
    let error = error.to_lowercase();
    let named = !session.is_empty() && error.contains(&session.to_lowercase());
    error.contains("not found")
        || error.contains("notfound")
        || error.contains("unknown session")
        || error.contains("no such session")
        || error.contains("does not exist")
        || error.contains("404")
        || named && error.contains("not ")
}

/// `true` when the vendor's answer to `session/resume` says the session is
/// gone.
fn session_refused(result: &Value) -> bool {
    match result.get("error") {
        Some(error) => is_refusal("", &error.to_string()),
        None => false,
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
        let harness = Self::start_in(program, session_id, environment, None)?;
        harness.handshake()?;
        Ok(harness)
    }

    /// Start one child with its working directory set to `work_dir`.
    pub fn start_in(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let shared = Arc::new(Mutex::new(State::default()));
        let on_shared = Arc::clone(&shared);
        let rpc = RpcChild::launch(
            RpcLaunch {
                name: "anyagents-muse".to_string(),
                program: program.into(),
                args: vec!["serve".to_string()],
                environment: environment
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
                work_dir: work_dir.map(Path::to_path_buf),
            },
            Arc::new(move |line| on_notification(&mut lock(&on_shared), line)),
            Arc::new(on_request),
        )?;
        lock(&shared).wanted_session = session_id.map(str::to_string);
        Ok(Self { rpc, shared })
    }

    /// Start one child and resume `session_id` when the vendor still knows it.
    ///
    /// A vendor that answers "no such session" starts a new session instead,
    /// and stderr says so: a conversation that had to be left behind is never
    /// dropped silently. Any other failure is reported, never read as gone.
    pub fn start_after_restart(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let program: PathBuf = program.into();
        // The new server's step: the stored session is handed to the child at
        // start, as `session/resume`. A vendor that says it is gone starts a
        // new conversation instead, on the same child, and stderr says why.
        let mut harness = Self::start_in(program, session_id, environment, work_dir)?;
        if let Err(refusal) = harness.handshake() {
            let said = format!("anyagents-muse: {refusal}; starting a new one");
            harness.resume_after_refusal(&said)?;
            eprintln!("{said}");
        }
        Ok(harness)
    }

    /// Ids this process actually wrote, in write order.
    pub fn written_ids(&self) -> Vec<String> {
        lock(&self.shared).written.clone()
    }

    /// Turns opened with `turn/start`.
    pub fn turns_started(&self) -> usize {
        lock(&self.shared).turns_started
    }

    /// The session this host drives.
    pub fn session_id(&self) -> Option<String> {
        let state = lock(&self.shared);
        if !state.session.is_empty() {
            return Some(state.session.clone());
        }
        state.wanted_session.clone()
    }

    /// The arguments the current child was started with.
    pub fn args(&self) -> Vec<String> {
        self.rpc.args_record().to_vec()
    }

    /// Start a new child with `session`: the same one when it is already the
    /// session this child runs, else a child that resumes it.
    ///
    /// A vendor that answers "no such session" starts a new session instead,
    /// and the reason goes to `said`: a conversation that had to be left
    /// behind is never dropped silently. Any other answer stands.
    pub fn resume_or_start(
        &mut self,
        session: Option<&str>,
        restart: bool,
        said: &mut Vec<String>,
    ) -> Result<(), HarnessError> {
        let Some(session) = session else {
            return Ok(());
        };
        if self.session_id().as_deref() == Some(session) {
            return Ok(());
        }
        if let Err(err) = self.resume_vendor(session) {
            let message = match &err {
                RpcFail::Dead => HarnessError::Dead.to_string(),
                other => other.reason(),
            };
            if !(restart && is_refusal(session, &message)) || !self.rpc.alive() {
                return Err(HarnessError::Failed(message));
            }
            let said_text =
                format!("session {session} was not resumed ({message}); starting a new one");
            said.push(format!("anyagents-muse: {said_text}"));
            // The vendor is up and holding no session: it gets a new one on the
            // same child, with the same fresh state.
            return self.resume_after_refusal(&said_text);
        }
        Ok(())
    }

    /// The vendor has ended without speaking once since it started: the
    /// session it was asked to resume was not taken.
    pub fn silent_end(&self) -> bool {
        !self.rpc.alive() && self.rpc.lines() == 0
    }

    /// Attach to `session` on the running child.
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.resume_vendor(session)
            .map(|_| ())
            .map_err(|err| match err {
                RpcFail::Dead => HarnessError::Dead,
                other => HarnessError::Failed(other.reason()),
            })
    }

    /// Ask the vendor for `session`. Its own error comes back unresolved, so a
    /// caller can tell "no such session" from a request that failed.
    fn resume_vendor(&self, session: &str) -> Result<Value, RpcFail> {
        let result = self.rpc.request(
            "session/resume",
            json!({
                "commandId": command_id(),
                "sessionId": session,
                "excludeItems": true
            }),
            RESUME_WAIT,
        )?;
        let mut state = lock(&self.shared);
        state.session =
            session_from(&result, Some(session.to_string())).unwrap_or_else(|| session.to_string());
        state.turn = result
            .pointer("/session/activeTurnId")
            .and_then(Value::as_str)
            .map(str::to_string);
        state.open.clear();
        Ok(result)
    }

    /// Take up a new session on the running child after a resume it refused:
    /// the resume's own state is dropped, and `session/start` names the new one.
    fn resume_after_refusal(&mut self, said: &str) -> Result<(), HarnessError> {
        *lock(&self.shared) = State::default();
        self.handshake()
            .map_err(|err| HarnessError::Failed(format!("{said} ({err})")))
    }

    /// Wait until a finished turn closed `id`, or the deadline passes.
    pub fn wait_finished(&self, id: &str, timeout: Duration) -> bool {
        let shared = Arc::clone(&self.shared);
        let id = id.to_string();
        self.rpc
            .wait_until(timeout, move || lock(&shared).finished.contains(&id))
    }

    /// The usage recorded for `id`.
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        let state = lock(&self.shared);
        state
            .usages
            .iter()
            .rev()
            .find(|(item, _)| item == id)
            .map(|(_, usage)| usage.clone())
    }

    /// Wait until the vendor opened a turn, or the deadline passes.
    pub fn wait_in_turn(&self, timeout: Duration) -> bool {
        let shared = Arc::clone(&self.shared);
        self.rpc
            .wait_until(timeout, move || lock(&shared).turn.is_some())
    }

    /// Wait until the child has exited, or the deadline passes.
    pub fn wait_dead(&self, timeout: Duration) -> bool {
        self.rpc.wait_until(timeout, || !self.rpc.alive())
    }

    fn handshake(&self) -> Result<(), HarnessError> {
        self.call(
            "initialize",
            json!({"clientInfo": {"name": "anyagents", "version": "0.1.0"}}),
            RPC_WAIT,
        )?;
        let wanted = lock(&self.shared).wanted_session.clone();
        let (method, params) = match &wanted {
            Some(session) => (
                "session/resume",
                json!({
                    "commandId": command_id(),
                    "sessionId": session,
                    "excludeItems": true
                }),
            ),
            None => ("session/start", json!({"commandId": command_id()})),
        };
        let result = self.call(method, params, RESUME_WAIT)?;
        // The vendor's own answer decides: a resume it refused is never
        // recorded as the session this host runs.
        let session = session_from(&result, wanted);
        let Some(session) =
            session.filter(|_| method == "session/start" || !session_refused(&result))
        else {
            let reason = result
                .get("error")
                .map(ToString::to_string)
                .unwrap_or_else(|| "the vendor named no session".into());
            return Err(HarnessError::Failed(format!(
                "session resume refused: {reason}"
            )));
        };
        {
            let mut state = lock(&self.shared);
            state.session = session.clone();
            state.turn = result
                .pointer("/session/activeTurnId")
                .or_else(|| result.get("activeTurnId"))
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        self.call(
            "session/setApprovalMode",
            json!({
                "commandId": command_id(),
                "mode": "allowAll",
                "sessionId": session
            }),
            RPC_WAIT,
        )?;
        Ok(())
    }

    fn call(&self, method: &str, params: Value, wait: Duration) -> Result<Value, HarnessError> {
        self.rpc.request(method, params, wait).map_err(fail)
    }
}

impl Harness for MuseHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let (session, in_turn) = {
            let mut state = lock(&self.shared);
            if !self.rpc.alive() {
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
            if state.session.is_empty() {
                return Err(HarnessError::Failed("no session".into()));
            }
            let in_turn = state.turn.is_some();
            if !in_turn {
                state.taken.push(message.id.clone());
            }
            (state.session.clone(), in_turn)
        };
        let mut params = json!({
            "commandId": command_id(),
            "sessionId": session,
            "input": [{"type": "text", "text": marked_message(&message.id, &message.text)}],
            "reasoningEffort": "max",
        });
        if in_turn {
            params["ifBusy"] = json!("steer");
        }
        // A message steered into the open turn is carried by that turn, so its
        // `turn/completed` settles this one too. It is recorded before the call:
        // the vendor may close the turn before the reply to this request arrives.
        if in_turn {
            let mut state = lock(&self.shared);
            if !state.open.contains(&message.id) {
                state.open.push(message.id.clone());
            }
        }
        match self.rpc.request("turn/start", params, RPC_WAIT) {
            Ok(result) => {
                let mut state = lock(&self.shared);
                state.written.push(message.id.clone());
                state.seen.insert(message.id.clone());
                if let Some(turn_id) = result.get("turnId").and_then(Value::as_str) {
                    state.turn = Some(turn_id.to_string());
                }
                Ok(DeliverOutcome::Accepted {
                    turn_finished: state.finished.contains(&message.id),
                })
            }
            Err(RpcFail::Dead) => {
                let mut state = lock(&self.shared);
                state.taken.retain(|id| id != &message.id);
                state.open.retain(|id| id != &message.id);
                Err(HarnessError::Dead)
            }
            Err(fail) => {
                let mut state = lock(&self.shared);
                state.taken.retain(|id| id != &message.id);
                state.open.retain(|id| id != &message.id);
                if let Some(reset_at_ms) = quota_from_error(&fail) {
                    state.quota_reset_at_ms = Some(reset_at_ms);
                    return Ok(DeliverOutcome::Quota { reset_at_ms });
                }
                Err(HarnessError::Failed(fail.reason()))
            }
        }
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if self.rpc.alive() {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        MuseHarness::resume(self, session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let (session, turn) = {
            let state = lock(&self.shared);
            (state.session.clone(), state.turn.clone())
        };
        if let (session, Some(turn)) = (session, turn)
            && !session.is_empty()
        {
            self.call(
                "turn/cancel",
                json!({
                    "commandId": command_id(),
                    "sessionId": session,
                    "turnId": turn
                }),
                RPC_WAIT,
            )?;
        }
        let mut state = lock(&self.shared);
        state.turn = None;
        state.open.clear();
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let state = lock(&self.shared);
        HarnessHealth {
            process_alive: self.rpc.alive(),
            turn_state: if state.turn.is_none() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }

    fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        MuseHarness::usage_for(self, id)
    }

    fn turn_report_for(&self, id: &str) -> Option<TurnReport> {
        let state = lock(&self.shared);
        state
            .reports
            .iter()
            .rev()
            .find(|(item, _)| item == id)
            .map(|(_, report)| report.clone())
    }

    fn clear_quota(&mut self) {
        lock(&self.shared).quota_reset_at_ms = None;
    }

    fn process_id(&self) -> Option<u32> {
        self.rpc.process_id()
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.rpc.watch_activity(notify)
    }
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

fn on_notification(state: &mut State, value: &Value) -> NotificationEffect {
    let method = value.get("method").and_then(Value::as_str).unwrap_or("");
    let params = value.get("params").cloned().unwrap_or(Value::Null);
    note_ids(state, &params);
    match method {
        "turn/started" => {
            let turn = params
                .get("turnId")
                .and_then(Value::as_str)
                .map(str::to_string);
            // A steer joins the open turn; anything else opens a new one.
            let same = turn.is_some() && turn == state.turn;
            if !same {
                state.turns_started += 1;
                state.turn = turn;
            }
            if state.open.is_empty() {
                state.open.append(&mut state.taken);
            }
        }
        "turn/completed" => {
            let turn = params.get("turnId").and_then(Value::as_str);
            if turn.is_none() || turn == state.turn.as_deref() {
                finish_turn(state, &params);
            }
        }
        "usage/updated" | "turn/usage" => state.last_usage = Some(usage_of(&params)),
        "rate_limits/updated" | "rate_limit" => {
            if let Some(reset_at_ms) = quota_of(&params) {
                state.quota_reset_at_ms = Some(reset_at_ms);
            }
        }
        _ => {}
    }
    NotificationEffect::default()
}

fn finish_turn(state: &mut State, params: &Value) {
    let usage = params
        .get("usage")
        .map(usage_of)
        .or_else(|| state.last_usage.clone())
        .unwrap_or_default();
    let failed = params
        .get("error")
        .filter(|error| !error.is_null())
        .map(|error| {
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("turn failed")
                .to_string()
        })
        .or_else(|| {
            params
                .get("status")
                .and_then(Value::as_str)
                .filter(|status| *status == "failed" || *status == "error")
                .map(|_| "turn failed".to_string())
        });
    let carried: Vec<String> = if state.open.is_empty() {
        std::mem::take(&mut state.taken)
    } else {
        std::mem::take(&mut state.open)
    };
    for id in carried {
        state.finished.insert(id.clone());
        state.usages.push((id.clone(), usage.clone()));
        let report = match &failed {
            Some(reason) => TurnReport::Failed(reason.clone()),
            None => TurnReport::Succeeded(usage.clone()),
        };
        state.reports.push((id, report));
    }
    state.turn = None;
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
    None
}

/// A 429 answer, or one whose text names a quota, is a wait until the reset
/// time it carries. Without a reset time the vendor did not say when to try
/// again, so it is an ordinary failure.
fn quota_from_error(fail: &RpcFail) -> Option<i64> {
    let RpcFail::Failed(error) = fail else {
        return None;
    };
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

fn note_ids(state: &mut State, value: &Value) {
    match value {
        Value::String(text) => {
            if let Some(id) = message_id_in_text(text) {
                state.seen.insert(id.to_string());
            }
        }
        Value::Array(items) => {
            for item in items {
                note_ids(state, item);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                note_ids(state, item);
            }
        }
        _ => {}
    }
}

/// A uuid7 command id, the shape the vendor's own commands use.
fn command_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let ms = (nanos / 1_000_000) & 0xFFFF_FFFF_FFFF;
    let rand_a = nanos & 0x0FFF;
    let rand_b = (nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 4) & 0x3FFF_FFFF_FFFF_FFFF;
    let value = (ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b;
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
