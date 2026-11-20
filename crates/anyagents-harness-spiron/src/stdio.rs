//! Direct stdio JSON-lines backend for `spiron --headless --json` (zero `--listen`).
//!
//! Supports both `spiron.headless/v2` (native mid-turn `steer` with
//! `accepted`/`steer_consumed` consumption proofs) and legacy
//! `spiron.headless/v1` (honestly downgraded to `DeliveryLevel::TurnEnd` unless
//! `ready.capabilities` explicitly declares `steer_consumed`).

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ExitStatus, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, HarnessError, HarnessHealth, OutboundMessage, Proof,
    TurnState, TurnUsage, compose_message,
};
use serde_json::{Value, json};

use crate::{STOP_GRACE, SpironLaunch, control_id};

const MAX_STDERR_BYTES: usize = 4096;

struct Shared {
    ready: bool,
    alive: bool,
    supports_steer: bool,
    idempotent: bool,
    turn_tool_context: bool,
    peer: Option<String>,
    session_id: Option<String>,
    turn_state: TurnState,
    turn_closing: bool,
    active_id: Option<String>,
    turns_served: u32,
    served_usage: TurnUsage,
    written: Vec<String>,
    open: Vec<String>,
    in_flight: HashSet<String>,
    acknowledged: HashSet<String>,
    input_turns: HashMap<String, String>,
    turn_members: HashMap<String, Vec<String>>,
    proofs: HashMap<String, Proof>,
    rejected: HashMap<String, (String, bool)>,
    texts: HashMap<String, String>,
    usages: HashMap<String, TurnUsage>,
    failures: HashMap<String, String>,
    recognised: HashSet<String>,
    stderr: String,
    exit_status: Option<ExitStatus>,
    watchers: Vec<Arc<dyn Fn() + Send + Sync>>,
}

impl Shared {
    fn new(session_id: Option<String>) -> Self {
        Self {
            ready: false,
            alive: true,
            supports_steer: false,
            idempotent: false,
            turn_tool_context: false,
            peer: None,
            session_id,
            turn_state: TurnState::Idle,
            turn_closing: false,
            active_id: None,
            turns_served: 0,
            served_usage: TurnUsage::default(),
            written: Vec::new(),
            open: Vec::new(),
            in_flight: HashSet::new(),
            acknowledged: HashSet::new(),
            input_turns: HashMap::new(),
            turn_members: HashMap::new(),
            proofs: HashMap::new(),
            rejected: HashMap::new(),
            texts: HashMap::new(),
            usages: HashMap::new(),
            failures: HashMap::new(),
            recognised: HashSet::new(),
            stderr: String::new(),
            exit_status: None,
            watchers: Vec::new(),
        }
    }

    fn delivery(&self) -> Delivery {
        if self.supports_steer {
            Delivery {
                level: DeliveryLevel::NextStep,
                idempotent: self.idempotent,
                proof: Cow::Borrowed("accepted/steer_consumed receipts; request_id journal"),
                peer: self.peer.clone(),
            }
        } else {
            Delivery {
                level: DeliveryLevel::TurnEnd,
                idempotent: self.idempotent,
                proof: Cow::Borrowed("accepted receipt per turn"),
                peer: self.peer.clone(),
            }
        }
    }

    fn release_input(&mut self, id: &str) {
        if self.proofs.contains_key(id) {
            return;
        }
        self.in_flight.remove(id);
        self.open.retain(|item| item != id);
        self.acknowledged.remove(id);
        self.input_turns.remove(id);
        if self.active_id.as_deref() == Some(id) && self.open.is_empty() {
            self.active_id = None;
            self.turn_state = TurnState::Idle;
            self.turn_closing = false;
        }
    }

    fn settled_outcome(&self, id: &str) -> Option<DeliverOutcome> {
        if self.recognised.contains(id) {
            return Some(DeliverOutcome::Duplicate);
        }
        if let Some(proof) = self.proofs.get(id) {
            return Some(match proof {
                Proof::Taken { turn_finished } => DeliverOutcome::Accepted {
                    turn_finished: *turn_finished,
                },
                Proof::Duplicate => DeliverOutcome::Duplicate,
            });
        }
        if let Some((reason, message_specific)) = self.rejected.get(id) {
            return Some(DeliverOutcome::rejected(reason.clone(), *message_specific));
        }
        None
    }

    fn notify(&self) {
        for cb in &self.watchers {
            cb();
        }
    }
}

pub(crate) struct StdioSpiron {
    program: PathBuf,
    environment: Vec<(String, String)>,
    launch: SpironLaunch,
    state: Arc<(Mutex<Shared>, Condvar)>,
    stdin: Mutex<Option<ChildStdin>>,
    child: Mutex<Option<Child>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl StdioSpiron {
    pub(crate) fn launch(launch: SpironLaunch, patience: Duration) -> Result<Self, HarnessError> {
        let state = Arc::new((
            Mutex::new(Shared::new(launch.session.clone())),
            Condvar::new(),
        ));
        let mut h = Self {
            program: launch.program.clone(),
            environment: launch.environment.clone(),
            launch,
            state,
            stdin: Mutex::new(None),
            child: Mutex::new(None),
            threads: Mutex::new(Vec::new()),
        };
        h.spawn_process(patience)?;
        Ok(h)
    }

    fn st(&self) -> MutexGuard<'_, Shared> {
        self.state.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn spawn_process(&mut self, patience: Duration) -> Result<(), HarnessError> {
        let args = self.launch.build_stdio_launch_args();
        let mut cmd = anyagents_harness::spawn_command(&self.launch.program, &self.environment)?;
        cmd.args(&args)
            .current_dir(&self.launch.work_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = anyagents_harness::spawn_child(&mut cmd)
            .map_err(|e| HarnessError::Failed(e.to_string()))?;
        let (stdin, stdout, stderr) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take());
        *self.stdin.lock().unwrap_or_else(|e| e.into_inner()) = stdin;
        *self.child.lock().unwrap_or_else(|e| e.into_inner()) = Some(child);

        let mut handles = Vec::new();
        if let Some(err_pipe) = stderr {
            let st = Arc::clone(&self.state);
            if let Ok(t) = std::thread::Builder::new()
                .name("spiron-stderr".into())
                .spawn(move || {
                    let mut r = BufReader::new(err_pipe);
                    let mut line = String::new();
                    while r.read_line(&mut line).unwrap_or(0) > 0 {
                        let mut g = st.0.lock().unwrap_or_else(|e| e.into_inner());
                        if g.stderr.len() < MAX_STDERR_BYTES {
                            let mut end = (MAX_STDERR_BYTES - g.stderr.len()).min(line.len());
                            while end > 0 && !line.is_char_boundary(end) {
                                end -= 1;
                            }
                            g.stderr.push_str(&line[..end]);
                        }
                        line.clear();
                    }
                })
            {
                handles.push(t);
            }
        }
        if let Some(out_pipe) = stdout {
            let st = Arc::clone(&self.state);
            if let Ok(t) = std::thread::Builder::new()
                .name("spiron-stdout".into())
                .spawn(move || {
                    let mut r = BufReader::new(out_pipe);
                    let mut line = String::new();
                    while r.read_line(&mut line).unwrap_or(0) > 0 {
                        if let Ok(val) = serde_json::from_str::<Value>(line.trim()) {
                            handle_frame(&st, &val);
                        }
                        line.clear();
                    }
                    let mut g = st.0.lock().unwrap_or_else(|e| e.into_inner());
                    g.alive = false;
                    g.turn_state = TurnState::Idle;
                    g.turn_closing = false;
                    g.open.clear();
                    g.in_flight.clear();
                    st.1.notify_all();
                    g.notify();
                })
            {
                handles.push(t);
            }
        }
        *self.threads.lock().unwrap_or_else(|e| e.into_inner()) = handles;

        let deadline = Instant::now() + patience;
        loop {
            let g = self.st();
            if g.ready {
                return Ok(());
            }
            if !g.alive {
                drop(g);
                let diag = self
                    .exit_diagnostic()
                    .unwrap_or_else(|| "spiron exited before ready".into());
                return Err(HarnessError::Failed(diag));
            }
            let now = Instant::now();
            if now >= deadline {
                drop(g);
                self.stop();
                return Err(HarnessError::Failed(format!(
                    "spiron did not emit ready within {}s",
                    patience.as_secs()
                )));
            }
            let _ = self
                .state
                .1
                .wait_timeout(g, (deadline - now).min(Duration::from_millis(50)));
        }
    }

    fn reap(&self) {
        if let Some(c) = self
            .child
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
            && let Ok(Some(st)) = c.try_wait()
        {
            self.st().exit_status = Some(st);
        }
    }

    fn write_json(&self, frame: &Value) -> Result<(), HarnessError> {
        let mut guard = self.stdin.lock().unwrap_or_else(|e| e.into_inner());
        let stdin = guard.as_mut().ok_or(HarnessError::Dead)?;
        writeln!(stdin, "{frame}").map_err(|_| HarnessError::Dead)?;
        stdin.flush().map_err(|_| HarnessError::Dead)
    }

    pub(crate) fn deliver_with_turn_context(
        &mut self,
        msg: &OutboundMessage,
        ctx: Option<&str>,
    ) -> Result<DeliverOutcome, HarnessError> {
        let (frame, starts_turn) = {
            let mut g = self.st();
            if let Some(settled) = g.settled_outcome(&msg.id) {
                return Ok(settled);
            }
            if !g.alive {
                return Err(HarnessError::Dead);
            }
            if g.in_flight.contains(&msg.id) {
                return Ok(DeliverOutcome::InFlight);
            }
            let starts_turn = g.turn_state == TurnState::Idle && g.open.is_empty();
            if starts_turn {
                if !g.in_flight.is_empty() {
                    return Ok(DeliverOutcome::Deferred);
                }
            } else if !g.supports_steer || g.turn_closing {
                return Ok(DeliverOutcome::Deferred);
            }
            let mut frame = json!({
                "type": if starts_turn { "prompt" } else { "steer" },
                "request_id": msg.id,
                "text": compose_message(&msg.id, &msg.context, &msg.text),
            });
            if starts_turn && let Some(c) = ctx {
                frame["tool_context"] = json!(c);
            }
            // Reserve custody before writing so an immediate vendor receipt
            // correlates cleanly. This is never a consumption proof.
            g.in_flight.insert(msg.id.clone());
            g.open.push(msg.id.clone());
            if starts_turn {
                g.active_id = Some(msg.id.clone());
                g.turn_state = TurnState::InTurn;
                g.turn_closing = false;
            }
            g.written.push(msg.id.clone());
            (frame, starts_turn)
        };

        if let Err(err) = self.write_json(&frame) {
            let mut g = self.st();
            g.release_input(&msg.id);
            if let Some(pos) = g.written.iter().rposition(|item| item == &msg.id) {
                g.written.remove(pos);
            }
            if starts_turn && g.open.is_empty() {
                g.active_id = None;
                g.turn_state = TurnState::Idle;
                g.turn_closing = false;
            }
            drop(g);
            self.reap();
            return Err(err);
        }

        let g = self.st();
        g.notify();
        if let Some(settled) = g.settled_outcome(&msg.id) {
            return Ok(settled);
        }
        if g.in_flight.contains(&msg.id) {
            return Ok(DeliverOutcome::InFlight);
        }
        if !g.alive {
            return Err(HarnessError::Dead);
        }
        Ok(DeliverOutcome::Deferred)
    }

    pub(crate) fn stop(&mut self) {
        let _ = self.write_json(&json!({"type": "stop", "request_id": control_id("stop")}));
        let _ = self.stdin.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut c) = self.child.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let st = crate::wait_exited(&mut c, STOP_GRACE).or_else(|| {
                anyagents_harness::kill_child(&mut c, &self.environment);
                c.wait().ok()
            });
            if let Some(s) = st {
                self.st().exit_status = Some(s);
            }
        }
        for h in self
            .threads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            let _ = h.join();
        }
    }

    pub(crate) fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.stop();
        self.launch.session = Some(session.to_string());
        *self.st() = Shared::new(Some(session.to_string()));
        self.spawn_process(Duration::from_secs(60))
    }

    pub(crate) fn cancel(&mut self) -> Result<(), HarnessError> {
        if !self.st().alive {
            return Err(HarnessError::Dead);
        }
        self.write_json(&json!({"type": "cancel", "request_id": control_id("cancel")}))
    }
    pub(crate) fn recognise_applied(&self, ids: impl IntoIterator<Item = String>) {
        self.st().recognised.extend(ids);
    }
    pub(crate) fn proves_consumption(&self) -> bool {
        self.st().supports_steer
    }
    pub(crate) fn supports_turn_tool_context(&self) -> bool {
        let g = self.st();
        g.turn_tool_context || g.ready
    }
    pub(crate) fn session_id(&self) -> Option<String> {
        self.st().session_id.clone()
    }
    pub(crate) fn written_ids(&self) -> Vec<String> {
        self.st().written.clone()
    }
    pub(crate) fn proven(&self, id: &str) -> Option<Proof> {
        let g = self.st();
        if g.recognised.contains(id) {
            return Some(Proof::Duplicate);
        }
        g.proofs.get(id).copied()
    }
    pub(crate) fn text_for(&self, id: &str) -> Option<String> {
        self.st().texts.get(id).cloned()
    }
    pub(crate) fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        self.st().usages.get(id).cloned()
    }
    pub(crate) fn failure_for(&self, id: &str) -> Option<String> {
        self.st().failures.get(id).cloned()
    }
    pub(crate) fn task_turn_result(&self, id: &str) -> Option<(String, String)> {
        let g = self.st();
        let opener = g
            .input_turns
            .get(id)
            .cloned()
            .or_else(|| g.session_id.clone())
            .unwrap_or_default();
        Some((opener, g.texts.get(id)?.clone()))
    }
    pub(crate) fn health(&self) -> HarnessHealth {
        self.reap();
        let g = self.st();
        HarnessHealth {
            process_alive: g.alive,
            turn_state: g.turn_state,
            delivery: g.delivery(),
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }
    pub(crate) fn background(&self) -> Result<(), HarnessError> {
        if self.st().alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }
    pub(crate) fn process_id(&self) -> Option<u32> {
        self.child
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(Child::id)
    }
    pub(crate) fn pending_input_owner(&self) -> Option<String> {
        self.st().active_id.clone()
    }
    pub(crate) fn input_ready(&self) -> bool {
        let g = self.st();
        g.alive
            && g.ready
            && (g.turn_state == TurnState::Idle || (g.supports_steer && !g.turn_closing))
    }
    pub(crate) fn served_usage(&self) -> TurnUsage {
        self.st().served_usage.clone()
    }
    pub(crate) fn turns_served(&self) -> u32 {
        self.st().turns_served
    }
    pub(crate) fn exit_diagnostic(&self) -> Option<String> {
        self.reap();
        let g = self.st();
        let err = g.stderr.trim();
        match (g.exit_status, err.is_empty()) {
            (Some(st), false) => Some(format!("spiron exited with {st}: {err}")),
            (Some(st), true) => Some(format!("spiron exited with {st}")),
            (None, false) if !g.alive => Some(format!("spiron exited: {err}")),
            _ => None,
        }
    }
    pub(crate) fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.st().watchers.push(notify);
    }
    pub(crate) fn program(&self) -> &Path {
        &self.program
    }
}

impl Drop for StdioSpiron {
    fn drop(&mut self) {
        self.stop();
    }
}

fn format_peer(val: &Value) -> String {
    let protocol = val
        .get("protocol")
        .and_then(Value::as_str)
        .unwrap_or("spiron.headless/v1");
    let capabilities: Vec<&str> = val
        .get("capabilities")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut peer = if capabilities.is_empty() {
        protocol.to_string()
    } else {
        format!("{protocol} [{}]", capabilities.join(", "))
    };
    if let Some(version) = val.get("version").and_then(Value::as_str) {
        peer.push_str(&format!("; spiron {version}"));
    }
    peer
}

fn handle_frame(state: &(Mutex<Shared>, Condvar), val: &Value) {
    let mut g = state.0.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(sid) = val
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
    {
        g.session_id = Some(sid.to_string());
    }
    let req_id = val
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| g.active_id.clone());
    match val.get("type").and_then(Value::as_str).unwrap_or("") {
        "ready" => {
            let protocol = val.get("protocol").and_then(Value::as_str).unwrap_or("");
            let is_v2 = protocol == anyagents_harness_stream::SPIRON_V2;
            let has_cap = |cap: &str| {
                val.get("capabilities")
                    .and_then(Value::as_array)
                    .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(cap)))
            };
            g.supports_steer = is_v2 || has_cap("steer_consumed");
            g.idempotent = is_v2 || has_cap("idempotent_input");
            g.turn_tool_context =
                val.get("features")
                    .and_then(Value::as_array)
                    .is_some_and(|items| {
                        items
                            .iter()
                            .any(|item| item.as_str() == Some("turn-tool-context-v1"))
                    });
            g.peer = Some(format_peer(val));
            g.ready = true;
        }
        "accepted" => {
            if let Some(id) = req_id.filter(|id| g.in_flight.contains(id) || g.open.contains(id)) {
                let duplicate = val.get("mode").and_then(Value::as_str) == Some("duplicate")
                    || val.get("duplicate").and_then(Value::as_bool) == Some(true);
                g.in_flight.remove(&id);
                g.acknowledged.insert(id.clone());
                g.input_turns.insert(id.clone(), id.clone());
                if duplicate {
                    g.proofs.insert(id.clone(), Proof::Duplicate);
                    g.open.retain(|item| item != &id);
                    if g.open.is_empty() {
                        g.active_id = None;
                        g.turn_state = TurnState::Idle;
                    }
                } else {
                    g.proofs.insert(
                        id.clone(),
                        Proof::Taken {
                            turn_finished: false,
                        },
                    );
                    let members = g.turn_members.entry(id.clone()).or_default();
                    if !members.contains(&id) {
                        members.push(id.clone());
                    }
                    g.active_id = Some(id);
                    g.turn_state = TurnState::InTurn;
                }
            }
        }
        "steer_ack" => {
            if let Some(id) = val.get("request_id").and_then(Value::as_str) {
                if val.get("accepted").and_then(Value::as_bool) == Some(true) {
                    if g.in_flight.contains(id) {
                        g.acknowledged.insert(id.to_string());
                        if let Some(turn) = val
                            .get("turn_id")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .or_else(|| g.active_id.clone())
                        {
                            g.input_turns.insert(id.to_string(), turn);
                        }
                    }
                } else {
                    g.release_input(id);
                    if !g.open.is_empty() {
                        g.turn_closing = true;
                    }
                }
            }
        }
        "steer_consumed" => {
            if let Some(id) = val
                .get("steer_request_id")
                .or_else(|| val.get("request_id"))
                .and_then(Value::as_str)
                .map(str::to_string)
            {
                let turn = val
                    .get("turn_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| g.input_turns.get(&id).cloned())
                    .or_else(|| g.active_id.clone())
                    .unwrap_or_else(|| id.clone());
                let duplicate = val.get("duplicate").and_then(Value::as_bool) == Some(true);
                g.in_flight.remove(&id);
                g.acknowledged.insert(id.clone());
                g.input_turns.insert(id.clone(), turn.clone());
                if duplicate {
                    g.proofs.insert(id, Proof::Duplicate);
                } else {
                    g.proofs.insert(
                        id.clone(),
                        Proof::Taken {
                            turn_finished: false,
                        },
                    );
                    let members = g.turn_members.entry(turn).or_default();
                    if !members.contains(&id) {
                        members.push(id);
                    }
                }
            }
        }
        "rejected" => {
            if let Some(id) = val.get("request_id").and_then(Value::as_str) {
                match val.get("code").and_then(Value::as_str) {
                    Some(
                        code @ ("empty_prompt" | "wrong_session" | "unknown_command"
                        | "upgrade_retired" | "missing_request_id"),
                    ) => {
                        g.release_input(id);
                        g.rejected.insert(
                            id.to_string(),
                            (
                                format!("spiron rejected the input: {code}"),
                                code == "empty_prompt",
                            ),
                        );
                    }
                    Some("turn_closed" | "no_active_turn") => {
                        g.release_input(id);
                        if !g.open.is_empty() {
                            g.turn_closing = true;
                        }
                    }
                    _ => {
                        g.release_input(id);
                    }
                }
            }
        }
        "text_delta" | "delta" => {
            if let Some(id) = g.active_id.clone().or(req_id)
                && let Some(d) = val
                    .get("delta")
                    .or_else(|| val.get("text"))
                    .and_then(Value::as_str)
            {
                g.texts.entry(id).or_default().push_str(d);
            }
        }
        "text" | "assistant" => {
            if let Some(id) = g.active_id.clone().or(req_id)
                && let Some(t) = val
                    .get("text")
                    .or_else(|| val.get("content"))
                    .and_then(Value::as_str)
                && !t.is_empty()
            {
                let slot = g.texts.entry(id).or_default();
                if slot.is_empty() || t.len() >= slot.len() {
                    *slot = t.to_string();
                }
            }
        }
        "usage" => {
            if let Some(u) = parse_usage(val.get("usage").unwrap_or(val)) {
                g.served_usage = add_usage(&g.served_usage, &u);
                if let Some(id) = g.active_id.clone().or(req_id) {
                    let prev = g.usages.remove(&id).unwrap_or_default();
                    g.usages.insert(id, add_usage(&prev, &u));
                }
            }
        }
        "result" => {
            let turn_id = val
                .get("request_id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| g.active_id.clone());
            if let Some(undelivered) = val.get("undelivered_steer_ids").and_then(Value::as_array) {
                for id in undelivered.iter().filter_map(Value::as_str) {
                    g.release_input(id);
                }
            }
            if let Some(ref turn) = turn_id {
                if val.get("duplicate").and_then(Value::as_bool) == Some(true)
                    && g.in_flight.contains(turn)
                {
                    g.in_flight.remove(turn);
                    g.proofs.insert(turn.clone(), Proof::Duplicate);
                }
                if let Some(u) = parse_result_usage(val) {
                    g.served_usage = add_usage(&g.served_usage, &u);
                    g.usages.insert(turn.clone(), u);
                }
                if let Some(t) = val
                    .get("final_text")
                    .or_else(|| val.get("text"))
                    .or_else(|| val.get("result"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    g.texts.insert(turn.clone(), t.to_string());
                }
                let final_text = g.texts.get(turn).cloned();
                let turn_err = if val.get("is_error").and_then(Value::as_bool) == Some(true)
                    || val.get("status").and_then(Value::as_str) == Some("failed")
                    || val
                        .get("error")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.trim().is_empty())
                {
                    Some(
                        val.get("error")
                            .or_else(|| val.get("message"))
                            .or_else(|| val.get("result"))
                            .and_then(Value::as_str)
                            .unwrap_or("spiron turn failed")
                            .to_string(),
                    )
                } else {
                    None
                };
                let mut members = g.turn_members.remove(turn).unwrap_or_default();
                if matches!(g.proofs.get(turn), Some(Proof::Taken { .. }))
                    && !members.contains(turn)
                {
                    members.insert(0, turn.clone());
                }
                for member in &members {
                    if let Some(ref text) = final_text {
                        g.texts.insert(member.clone(), text.clone());
                    }
                    if let Some(ref err) = turn_err {
                        g.failures.insert(member.clone(), err.clone());
                    }
                    if matches!(g.proofs.get(member), Some(Proof::Taken { .. })) {
                        g.proofs.insert(
                            member.clone(),
                            Proof::Taken {
                                turn_finished: true,
                            },
                        );
                    }
                }
            }
            // Any input still in flight when the turn closes was never consumed.
            let unconsumed: Vec<String> = g.in_flight.iter().cloned().collect();
            for id in unconsumed {
                g.release_input(&id);
            }
            g.open.clear();
            g.turns_served = g.turns_served.saturating_add(1);
            g.turn_state = TurnState::Idle;
            g.turn_closing = false;
            g.active_id = None;
        }
        "error" => {
            let msg = val
                .get("message")
                .or_else(|| val.get("error"))
                .and_then(Value::as_str)
                .unwrap_or("spiron error");
            if let Some(id) = req_id {
                g.failures.insert(id, msg.to_string());
            }
            let unconsumed: Vec<String> = g.in_flight.iter().cloned().collect();
            for id in unconsumed {
                g.release_input(&id);
            }
            g.open.clear();
            g.turn_state = TurnState::Idle;
            g.turn_closing = false;
            g.active_id = None;
        }
        _ => {}
    }
    state.1.notify_all();
    g.notify();
}

fn add_usage(a: &TurnUsage, b: &TurnUsage) -> TurnUsage {
    TurnUsage {
        input_tokens: a.input_tokens.saturating_add(b.input_tokens),
        output_tokens: a.output_tokens.saturating_add(b.output_tokens),
        cache_read_tokens: a.cache_read_tokens.saturating_add(b.cache_read_tokens),
        cache_write_tokens: a.cache_write_tokens.saturating_add(b.cache_write_tokens),
        reported_cost_micros: match (a.reported_cost_micros, b.reported_cost_micros) {
            (Some(x), Some(y)) => Some(x.saturating_add(y)),
            (x, y) => x.or(y),
        },
    }
}

fn parse_result_usage(val: &Value) -> Option<TurnUsage> {
    let mut u = val.get("usage").and_then(parse_usage).unwrap_or_default();
    if u.reported_cost_micros.is_none()
        && let Some(micros) = val.get("cost_micros").and_then(Value::as_i64)
    {
        u.reported_cost_micros = Some(micros);
    }
    (u.input_tokens > 0
        || u.output_tokens > 0
        || u.cache_read_tokens > 0
        || u.cache_write_tokens > 0
        || u.reported_cost_micros.is_some())
    .then_some(u)
}

fn parse_usage(v: &Value) -> Option<TurnUsage> {
    let o = v.as_object()?;
    let u = TurnUsage {
        input_tokens: o.get("input_tokens").and_then(Value::as_u64).unwrap_or(0),
        output_tokens: o.get("output_tokens").and_then(Value::as_u64).unwrap_or(0),
        cache_read_tokens: o
            .get("cache_read_input_tokens")
            .or_else(|| o.get("cache_read_tokens"))
            .or_else(|| o.get("cached_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_write_tokens: o
            .get("cache_creation_input_tokens")
            .or_else(|| o.get("cache_write_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        reported_cost_micros: o
            .get("reported_cost_micros")
            .or_else(|| o.get("cost_micros"))
            .and_then(Value::as_i64)
            .or_else(|| {
                o.get("total_cost_usd")
                    .or_else(|| o.get("cost_usd"))
                    .and_then(Value::as_f64)
                    .map(|usd| (usd * 1_000_000.0).round().max(0.0) as i64)
            }),
    };
    (u.input_tokens > 0
        || u.output_tokens > 0
        || u.cache_read_tokens > 0
        || u.cache_write_tokens > 0
        || u.reported_cost_micros.is_some())
    .then_some(u)
}
