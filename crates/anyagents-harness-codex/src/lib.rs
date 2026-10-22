//! Codex adapter.
//!
//! One child runs `codex app-server`. Messages are one JSON object per line.
//! Startup sends `initialize`, `initialized`, then `thread/start` (or
//! `thread/resume` when a session id is already known).
//!
//! The mailbox id is the first line of the user text (`anyagents-message:<id>`)
//! and the `clientUserMessageId` field. An id already written, or echoed back,
//! is not written again.
//!
//! Declared delivery mode is steer. `deliver` sends `turn/start` when no turn
//! is open and `turn/steer` during one. `cancel` sends `turn/interrupt`.
//! `turn_finished` becomes true when `turn/completed` reports status `completed`.
//! Token counts come from `thread/tokenUsage/updated`. This vendor reports no
//! currency amount, so the cost stays empty.
//!
//! A rate-limit notice is a wait only when it includes a reset time. Values of
//! `resetsAt` at or above `10^12` are epoch milliseconds; smaller values are
//! epoch seconds.

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnState, TurnUsage, compose_message, message_id_in_text,
};
use serde_json::{Value, json};

const RPC_WAIT: Duration = Duration::from_secs(3);

/// One long-lived Codex app-server child.
pub struct CodexHarness {
    shared: Arc<Mutex<State>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct State {
    child: Option<Child>,
    alive: bool,
    epoch: u64,
    seen: HashSet<String>,
    written: Vec<String>,
    open: Vec<String>,
    finished: HashSet<String>,
    turns_started: usize,
    quota_reset_at_ms: Option<i64>,
    thread_id: Option<String>,
    turn_id: Option<String>,
    last_usage: Option<TurnUsage>,
    usages: Vec<(String, TurnUsage)>,
    next_rpc: i64,
    pending: std::collections::HashMap<i64, Pending>,
}

enum Pending {
    Waiting,
    Done(Value),
    Err(Value),
}

enum CallFail {
    Dead,
    Quota(i64),
    Failed(String),
}

enum Ready {
    Done(Value),
    Err(Value),
}

impl CodexHarness {
    /// Start one child and complete the handshake.
    ///
    /// `program` is the `codex` executable, or a test double; it is invoked with
    /// `app-server`. `session_id`, when set, is passed to `thread/resume` instead
    /// of `thread/start`. `environment` overrides those variables in the child;
    /// pass an empty slice for a normal launch.
    ///
    /// If the child exits during startup, the returned adapter is already dead
    /// so the caller can observe that. A failure to spawn is an error.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        Self::start_in(program, session_id, environment, None)
    }

    /// Start one child with its working directory set to `work_dir`.
    pub fn start_in(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let harness = Self::launch(program.into(), environment, work_dir)?;
        match harness.handshake(session_id) {
            Ok(()) => Ok(harness),
            Err(err) => {
                if harness.health().process_alive {
                    Err(err)
                } else {
                    Ok(harness)
                }
            }
        }
    }

    /// Ids this process actually wrote, in write order.
    pub fn written_ids(&self) -> Vec<String> {
        lock(&self.shared).written.clone()
    }

    /// Turns opened with `turn/start`.
    pub fn turns_started(&self) -> usize {
        lock(&self.shared).turns_started
    }

    /// Thread id from `thread/start` or `thread/resume`.
    pub fn session_id(&self) -> Option<String> {
        lock(&self.shared).thread_id.clone()
    }

    /// Usage copied onto `id` when its turn completed.
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        let state = lock(&self.shared);
        state
            .usages
            .iter()
            .rev()
            .find(|(item, _)| item == id)
            .map(|(_, usage)| usage.clone())
    }

    /// Whether a completed turn finished `id`.
    pub fn is_finished(&self, id: &str) -> bool {
        lock(&self.shared).finished.contains(id)
    }

    /// Wait until a completed turn finishes `id`, or the deadline passes.
    pub fn wait_finished(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.finished.contains(id))
    }

    /// Wait until a rate-limit notice with a reset time has been seen.
    pub fn wait_quota(&self, timeout: Duration) -> Option<i64> {
        let mut found = None;
        self.wait_pred(timeout, |state| {
            found = state.quota_reset_at_ms;
            found.is_some()
        });
        found
    }

    /// Wait until the child has exited or its stdout has closed.
    pub fn wait_dead(&self, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| !state.alive)
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

    fn launch(
        program: PathBuf,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let harness = Self {
            shared: Arc::new(Mutex::new(State::default())),
            stdin: Arc::new(Mutex::new(None)),
            wake: Arc::new(Condvar::new()),
            reader: None,
        };
        let env_pairs: Vec<(String, String)> = environment
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let mut cmd = anyagents_harness::spawn_command(&program, &env_pairs);
        cmd.arg("app-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = work_dir {
            cmd.current_dir(dir);
        }
        for (key, value) in environment {
            cmd.env(key, value);
        }
        let mut child = cmd
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
            let mut state = lock(&harness.shared);
            state.child = Some(child);
            state.alive = true;
        }
        *lock(&harness.stdin) = Some(child_stdin);
        let shared = Arc::clone(&harness.shared);
        let wake = Arc::clone(&harness.wake);
        let handle = thread::Builder::new()
            .name("anyagents-codex".into())
            .spawn(move || read_stdout(stdout, shared, wake))
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        // The reader handle has to live on the struct. Rebuild is awkward because
        // `harness` was not mut and spawn failure must still kill the child.
        let harness = attach_reader(harness, handle);
        Ok(harness)
    }

    fn handshake(&self, session: Option<&str>) -> Result<(), HarnessError> {
        self.rpc(
            "initialize",
            json!({"clientInfo": {"name": "anyagents", "version": "0.1.0"}}),
        )?;
        self.notify("initialized", json!({}))?;
        let (method, params) = if let Some(session) = session {
            ("thread/resume", json!({"threadId": session}))
        } else {
            ("thread/start", json!({}))
        };
        let result = self.rpc(method, params)?;
        let thread_id = result
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| session.map(ToOwned::to_owned));
        let Some(thread_id) = thread_id else {
            return Err(HarnessError::Failed("thread id missing".into()));
        };
        lock(&self.shared).thread_id = Some(thread_id);
        Ok(())
    }

    fn rpc(&self, method: &str, params: Value) -> Result<Value, HarnessError> {
        match self.call(method, params) {
            Ok(value) => Ok(value),
            Err(CallFail::Dead) => Err(HarnessError::Dead),
            Err(CallFail::Quota(_)) => Err(HarnessError::Failed("quota during startup".into())),
            Err(CallFail::Failed(reason)) => Err(HarnessError::Failed(reason)),
        }
    }

    fn notify(&self, method: &str, params: Value) -> Result<(), HarnessError> {
        let line = json!({"jsonrpc": "2.0", "method": method, "params": params});
        if write_stdin(&self.stdin, &line).is_err() {
            self.mark_dead();
            Err(HarnessError::Dead)
        } else {
            Ok(())
        }
    }

    fn call(&self, method: &str, params: Value) -> Result<Value, CallFail> {
        let id = {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(CallFail::Dead);
            }
            state.next_rpc += 1;
            let id = state.next_rpc;
            state.pending.insert(id, Pending::Waiting);
            id
        };
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        if write_stdin(&self.stdin, &line).is_err() {
            lock(&self.shared).pending.remove(&id);
            self.mark_dead();
            return Err(CallFail::Dead);
        }
        let deadline = Instant::now() + RPC_WAIT;
        let mut state = lock(&self.shared);
        loop {
            if let Some(ready) = take_ready(&mut state, id) {
                return match ready {
                    Ready::Done(value) => Ok(value),
                    Ready::Err(error) => {
                        if let Some(reset_at_ms) = quota_from_rpc_error(&error) {
                            state.quota_reset_at_ms = Some(reset_at_ms);
                            Err(CallFail::Quota(reset_at_ms))
                        } else {
                            Err(CallFail::Failed(rpc_failure(&error)))
                        }
                    }
                };
            }
            if !state.alive {
                state.pending.remove(&id);
                return Err(CallFail::Dead);
            }
            let now = Instant::now();
            if now >= deadline {
                state.pending.remove(&id);
                return Err(CallFail::Failed("timed out".into()));
            }
            let (guard, _) = self
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|err| err.into_inner());
            state = guard;
        }
    }

    fn mark_dead(&self) {
        lock(&self.shared).alive = false;
        self.wake.notify_all();
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
        state.turn_id = None;
        state.alive = false;
    }
}

fn attach_reader(mut harness: CodexHarness, handle: JoinHandle<()>) -> CodexHarness {
    harness.reader = Some(handle);
    harness
}

impl Drop for CodexHarness {
    fn drop(&mut self) {
        self.stop_child();
    }
}

impl Harness for CodexHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let (thread_id, steering) = {
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
            let Some(thread_id) = state.thread_id.clone() else {
                return Err(HarnessError::Failed("no thread".into()));
            };
            let steering = state.turn_id.clone();
            state.open.push(message.id.clone());
            (thread_id, steering)
        };
        let input = json!([{
            "type": "text",
            "text": compose_message(&message.id, &message.context, &message.text)
        }]);
        let (method, params) = if let Some(turn_id) = &steering {
            (
                "turn/steer",
                json!({
                    "threadId": thread_id,
                    "expectedTurnId": turn_id,
                    "input": input,
                    "clientUserMessageId": message.id,
                }),
            )
        } else {
            (
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": input,
                    "clientUserMessageId": message.id,
                }),
            )
        };
        let rpc = self.call(method, params);
        let mut state = lock(&self.shared);
        match rpc {
            Ok(result) => {
                commit_delivery(&mut state, &message.id, steering.is_none(), method, &result);
                Ok(DeliverOutcome::Accepted {
                    turn_finished: state.finished.contains(&message.id),
                })
            }
            Err(CallFail::Dead) => {
                unstage(&mut state, &message.id);
                Err(HarnessError::Dead)
            }
            Err(CallFail::Quota(reset_at_ms)) => {
                unstage(&mut state, &message.id);
                state.quota_reset_at_ms = Some(reset_at_ms);
                Ok(DeliverOutcome::Quota { reset_at_ms })
            }
            Err(CallFail::Failed(reason)) => {
                unstage(&mut state, &message.id);
                Err(HarnessError::Failed(reason))
            }
        }
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if lock(&self.shared).alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        if !lock(&self.shared).alive {
            return Err(HarnessError::Dead);
        }
        let result = self.rpc("thread/resume", json!({"threadId": session}))?;
        let thread_id = result
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .unwrap_or(session)
            .to_string();
        let mut state = lock(&self.shared);
        state.thread_id = Some(thread_id);
        state.turn_id = None;
        state.open.clear();
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let (thread_id, turn_id) = {
            let state = lock(&self.shared);
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            (state.thread_id.clone(), state.turn_id.clone())
        };
        if let (Some(thread_id), Some(turn_id)) = (thread_id, turn_id) {
            self.rpc(
                "turn/interrupt",
                json!({"threadId": thread_id, "turnId": turn_id}),
            )?;
        }
        let mut state = lock(&self.shared);
        state.open.clear();
        state.turn_id = None;
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let state = lock(&self.shared);
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.open.is_empty() {
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

    fn turns_served(&self) -> u32 {
        lock(&self.shared).usages.len() as u32
    }

    /// The total of every turn this process has finished, read when the
    /// mailbox has drained.
    fn served_usage(&self) -> TurnUsage {
        let state = lock(&self.shared);
        let mut total = TurnUsage::default();
        for (_, usage) in &state.usages {
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
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        watch_child(Arc::clone(&self.shared), Arc::clone(&self.wake), notify);
    }
}

fn commit_delivery(state: &mut State, id: &str, started_turn: bool, method: &str, result: &Value) {
    if !state.written.iter().any(|item| item == id) {
        state.written.push(id.to_string());
        state.seen.insert(id.to_string());
        if started_turn {
            state.turns_started += 1;
        }
    }
    if state.finished.contains(id) {
        return;
    }
    let turn_id = if method == "turn/steer" {
        result.get("turnId").and_then(Value::as_str)
    } else {
        result.pointer("/turn/id").and_then(Value::as_str)
    };
    if let Some(turn_id) = turn_id {
        state.turn_id = Some(turn_id.to_string());
    }
}

fn unstage(state: &mut State, id: &str) {
    if state.finished.contains(id) {
        return;
    }
    state.open.retain(|item| item != id);
}

fn finish_open(state: &mut State) {
    let usage = state.last_usage.clone().unwrap_or_default();
    for id in state.open.drain(..) {
        state.finished.insert(id.clone());
        state.usages.push((id, usage.clone()));
    }
    state.turn_id = None;
}

fn take_ready(state: &mut State, id: i64) -> Option<Ready> {
    match state.pending.remove(&id) {
        Some(Pending::Done(value)) => Some(Ready::Done(value)),
        Some(Pending::Err(value)) => Some(Ready::Err(value)),
        Some(Pending::Waiting) => {
            state.pending.insert(id, Pending::Waiting);
            None
        }
        None => None,
    }
}

fn read_stdout(stdout: ChildStdout, shared: Arc<Mutex<State>>, wake: Arc<Condvar>) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        {
            let mut state = lock(&shared);
            apply_line(&mut state, &value);
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

fn watch_child(shared: Arc<Mutex<State>>, wake: Arc<Condvar>, notify: Arc<dyn Fn() + Send + Sync>) {
    let _ = thread::Builder::new()
        .name("anyagents-codex-watch".into())
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

fn apply_line(state: &mut State, value: &Value) {
    note_ids(state, value);
    if value.get("method").is_some()
        && value.get("result").is_none()
        && value.get("error").is_none()
    {
        let method = value.get("method").and_then(Value::as_str).unwrap_or("");
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        on_notification(state, method, &params);
        return;
    }
    let Some(id) = value.get("id").and_then(Value::as_i64) else {
        return;
    };
    if !matches!(state.pending.get(&id), Some(Pending::Waiting)) {
        return;
    }
    if let Some(error) = value.get("error") {
        state.pending.insert(id, Pending::Err(error.clone()));
        return;
    }
    if let Some(result) = value.get("result") {
        state.pending.insert(id, Pending::Done(result.clone()));
    }
}

fn on_notification(state: &mut State, method: &str, params: &Value) {
    match method {
        "thread/tokenUsage/updated" => state.last_usage = Some(usage_from_codex(params)),
        "turn/completed" => {
            let status = params
                .pointer("/turn/status")
                .and_then(Value::as_str)
                .unwrap_or("");
            if status == "completed" {
                finish_open(state);
            } else {
                state.open.clear();
                state.turn_id = None;
            }
        }
        "account/rateLimits/updated" => {
            if let Some(reset_at_ms) = quota_from_limits(params) {
                state.quota_reset_at_ms = Some(reset_at_ms);
            }
        }
        _ => {}
    }
}

fn usage_from_codex(params: &Value) -> TurnUsage {
    let last = params
        .pointer("/tokenUsage/last")
        .or_else(|| params.pointer("/tokenUsage/total"));
    TurnUsage {
        input_tokens: u64_field(last, "inputTokens"),
        output_tokens: u64_field(last, "outputTokens"),
        cache_read_tokens: u64_field(last, "cachedInputTokens"),
        cache_write_tokens: u64_field(last, "cacheWriteInputTokens"),
        reported_cost_micros: None,
    }
}

fn u64_field(value: Option<&Value>, key: &str) -> u64 {
    value
        .and_then(|item| item.get(key))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn note_ids(state: &mut State, value: &Value) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                if key == "clientUserMessageId"
                    && let Some(id) = item.as_str()
                    && !id.is_empty()
                {
                    state.seen.insert(id.to_string());
                }
                note_ids(state, item);
            }
        }
        Value::Array(items) => {
            for item in items {
                note_ids(state, item);
            }
        }
        Value::String(text) => {
            if let Some(id) = message_id_in_text(text) {
                state.seen.insert(id.to_string());
            }
        }
        _ => {}
    }
}

fn quota_from_limits(params: &Value) -> Option<i64> {
    let limits = params.get("rateLimits").unwrap_or(params);
    let reached = limits
        .get("rateLimitReachedType")
        .and_then(Value::as_str)
        .is_some_and(|text| !text.is_empty());
    if !reached && !any_window_full(limits) {
        return None;
    }
    for pointer in [
        "/primary/resetsAt",
        "/secondary/resetsAt",
        "/individualLimit/resetsAt",
    ] {
        if let Some(n) = limits.pointer(pointer).and_then(Value::as_i64) {
            return Some(epoch_ms(n));
        }
    }
    None
}

fn any_window_full(limits: &Value) -> bool {
    [
        "/primary/usedPercent",
        "/secondary/usedPercent",
        "/individualLimit/usedPercent",
    ]
    .iter()
    .any(|pointer| limits.pointer(pointer).is_some_and(percent_full))
}

fn percent_full(value: &Value) -> bool {
    value.as_f64().is_some_and(|number| number >= 100.0)
}

fn quota_from_rpc_error(error: &Value) -> Option<i64> {
    if !strings_contain_limit(error) {
        return None;
    }
    find_resets_at(error).map(epoch_ms)
}

fn strings_contain_limit(value: &Value) -> bool {
    match value {
        Value::String(text) => {
            text.contains("usageLimitExceeded") || text.contains("rateLimitExceeded")
        }
        Value::Array(items) => items.iter().any(strings_contain_limit),
        Value::Object(map) => map.values().any(strings_contain_limit),
        _ => false,
    }
}

fn find_resets_at(value: &Value) -> Option<i64> {
    match value {
        Value::Object(map) => {
            if let Some(n) = map.get("resetsAt").and_then(Value::as_i64) {
                return Some(n);
            }
            map.values().find_map(find_resets_at)
        }
        Value::Array(items) => items.iter().find_map(find_resets_at),
        _ => None,
    }
}

fn epoch_ms(n: i64) -> i64 {
    if n >= 1_000_000_000_000 {
        n
    } else {
        n.saturating_mul(1_000)
    }
}

fn rpc_failure(error: &Value) -> String {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("request failed");
    match error.get("code").and_then(Value::as_i64) {
        Some(code) => format!("{code} {message}"),
        None => message.to_string(),
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
