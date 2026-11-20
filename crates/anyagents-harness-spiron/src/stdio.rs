//! Direct stdio JSON-lines backend for `spiron --headless --json` (zero `--listen`).
//! Supports both `spiron.headless/v1` and `spiron.headless/v2` over child stdin/stdout.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ExitStatus, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, HarnessError, HarnessHealth, OutboundMessage, Proof, TurnState, TurnUsage,
    compose_message,
};
use serde_json::{Value, json};

use crate::{STOP_GRACE, SpironLaunch, control_id};

const MAX_STDERR_BYTES: usize = 4096;

struct Shared {
    ready: bool,
    alive: bool,
    session_id: Option<String>,
    turn_state: TurnState,
    active_id: Option<String>,
    turns_served: u32,
    served_usage: TurnUsage,
    written: Vec<String>,
    proofs: HashMap<String, Proof>,
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
            session_id,
            turn_state: TurnState::Idle,
            active_id: None,
            turns_served: 0,
            served_usage: TurnUsage::default(),
            written: Vec::new(),
            proofs: HashMap::new(),
            texts: HashMap::new(),
            usages: HashMap::new(),
            failures: HashMap::new(),
            recognised: HashSet::new(),
            stderr: String::new(),
            exit_status: None,
            watchers: Vec::new(),
        }
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
        {
            let g = self.st();
            if g.recognised.contains(&msg.id) {
                return Ok(DeliverOutcome::Duplicate);
            }
            if let Some(Proof::Taken { turn_finished }) = g.proofs.get(&msg.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: *turn_finished,
                });
            }
            if !g.alive {
                return Err(HarnessError::Dead);
            }
            if g.turn_state == TurnState::InTurn {
                return Ok(DeliverOutcome::Deferred);
            }
        }
        let mut frame = json!({ "type": "prompt", "request_id": msg.id, "text": compose_message(&msg.id, &msg.context, &msg.text) });
        if let Some(c) = ctx {
            frame["tool_context"] = json!(c);
        }
        self.write_json(&frame)?;
        let mut g = self.st();
        g.active_id = Some(msg.id.clone());
        g.turn_state = TurnState::InTurn;
        g.written.push(msg.id.clone());
        g.proofs.entry(msg.id.clone()).or_insert(Proof::Taken {
            turn_finished: false,
        });
        g.notify();
        Ok(DeliverOutcome::InFlight)
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
        self.write_json(&json!({"type": "cancel", "request_id": control_id("cancel")}))
    }
    pub(crate) fn recognise_applied(&self, ids: impl IntoIterator<Item = String>) {
        self.st().recognised.extend(ids);
    }
    pub(crate) fn session_id(&self) -> Option<String> {
        self.st().session_id.clone()
    }
    pub(crate) fn written_ids(&self) -> Vec<String> {
        self.st().written.clone()
    }
    pub(crate) fn proven(&self, id: &str) -> Option<Proof> {
        self.st().proofs.get(id).copied()
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
        Some((
            g.session_id.clone().unwrap_or_default(),
            g.texts.get(id)?.clone(),
        ))
    }
    pub(crate) fn health(&self) -> HarnessHealth {
        self.reap();
        let g = self.st();
        HarnessHealth {
            process_alive: g.alive,
            turn_state: g.turn_state,
            delivery: crate::DELIVERY,
            quota_reset_at_ms: None,
            sign_in_fault: None,
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
        g.alive && g.ready && g.turn_state == TurnState::Idle
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
        "ready" => g.ready = true,
        "accepted" | "steer_consumed" => {
            if let Some(id) = req_id {
                g.active_id = Some(id.clone());
                g.proofs.insert(
                    id,
                    Proof::Taken {
                        turn_finished: false,
                    },
                );
            }
            g.turn_state = TurnState::InTurn;
        }
        "text_delta" | "delta" => {
            if let Some(id) = req_id
                && let Some(d) = val
                    .get("delta")
                    .or_else(|| val.get("text"))
                    .and_then(Value::as_str)
            {
                g.texts.entry(id).or_default().push_str(d);
            }
        }
        "text" | "assistant" => {
            if let Some(id) = req_id
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
                if let Some(id) = req_id {
                    let prev = g.usages.remove(&id).unwrap_or_default();
                    g.usages.insert(id, add_usage(&prev, &u));
                }
            }
        }
        "result" => {
            if let Some(u) = val.get("usage").and_then(parse_usage) {
                g.served_usage = add_usage(&g.served_usage, &u);
                if let Some(ref id) = req_id {
                    g.usages.insert(id.clone(), u);
                }
            }
            if let Some(ref id) = req_id {
                if let Some(t) = val
                    .get("text")
                    .or_else(|| val.get("result"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    g.texts.insert(id.clone(), t.to_string());
                }
                if val.get("is_error").and_then(Value::as_bool) == Some(true)
                    || val
                        .get("error")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.trim().is_empty())
                {
                    let msg = val
                        .get("error")
                        .or_else(|| val.get("message"))
                        .or_else(|| val.get("result"))
                        .and_then(Value::as_str)
                        .unwrap_or("spiron turn failed");
                    g.failures.insert(id.clone(), msg.to_string());
                }
                g.proofs.insert(
                    id.clone(),
                    Proof::Taken {
                        turn_finished: true,
                    },
                );
            }
            g.turns_served = g.turns_served.saturating_add(1);
            g.turn_state = TurnState::Idle;
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
            g.turn_state = TurnState::Idle;
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

fn parse_usage(v: &Value) -> Option<TurnUsage> {
    let o = v.as_object()?;
    let u = TurnUsage {
        input_tokens: o.get("input_tokens").and_then(Value::as_u64).unwrap_or(0),
        output_tokens: o.get("output_tokens").and_then(Value::as_u64).unwrap_or(0),
        cache_read_tokens: o
            .get("cache_read_input_tokens")
            .or_else(|| o.get("cache_read_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_write_tokens: o
            .get("cache_creation_input_tokens")
            .or_else(|| o.get("cache_write_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        reported_cost_micros: o
            .get("reported_cost_micros")
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
