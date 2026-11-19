//! Embedded guest adapters for `dsh` (Gateway HTTP + WebSocket follow) and
//! `grok` (ACP stdio JSON-RPC via [`RpcChild`]).

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState, marked_message, message_id_in_text, spawn_command,
};
use anyagents_harness_stream::{NotificationEffect, RpcChild, RpcFail, RpcLaunch};
use serde_json::{Value, json};

const CALL_TIMEOUT: Duration = Duration::from_secs(30);
const START_TIMEOUT: Duration = Duration::from_secs(30);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// ============================================================================
// Grok Guest Adapter (ACP JSON-RPC over stdio)
// ============================================================================

#[derive(Default)]
struct GrokState {
    session: String,
    wanted_session: Option<String>,
    peer: Option<String>,
    requests: HashMap<i64, String>,
    admitted: HashMap<String, bool>,
    finished: HashMap<String, bool>,
    rejected: HashMap<String, String>,
    answers: HashMap<String, String>,
    current_text: String,
    unanswered: usize,
    turns_served: u32,
}

pub(crate) struct GrokGuestHarness {
    rpc: RpcChild,
    shared: Arc<Mutex<GrokState>>,
    work_dir: PathBuf,
    mcp_servers: Value,
}

impl GrokGuestHarness {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        program: impl Into<PathBuf>,
        model: Option<&str>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: &Path,
        guest_exe: &Path,
        mcp_url: Option<&str>,
        mcp_token_file: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let shared = Arc::new(Mutex::new(GrokState {
            wanted_session: session_id
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            ..GrokState::default()
        }));
        let on_shared = Arc::clone(&shared);
        let mut args = vec![
            "agent".to_string(),
            "--no-leader".to_string(),
            "--always-approve".to_string(),
        ];
        if let Some(m) = model.map(str::trim).filter(|s| !s.is_empty()) {
            args.push("-m".to_string());
            args.push(m.to_string());
        }
        args.push("stdio".to_string());

        let rpc = RpcChild::launch(
            RpcLaunch {
                name: "anyagents-guest-grok".to_string(),
                program: program.into(),
                args,
                environment: environment
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
                work_dir: Some(work_dir.to_path_buf()),
            },
            Arc::new(move |line| on_grok_notification(&mut lock(&on_shared), line)),
            Arc::new(|method, _params| {
                if method == "session/request_permission" {
                    Some(json!({ "outcome": { "outcome": "selected", "optionId": "allow_once" } }))
                } else {
                    None
                }
            }),
        )?;

        let mcp_servers = if let Some(url) = mcp_url {
            let mut proxy_args = vec![
                "mcp-proxy".to_string(),
                "--url".to_string(),
                url.to_string(),
            ];
            if let Some(tf) = mcp_token_file {
                proxy_args.push("--token-file".to_string());
                proxy_args.push(tf.display().to_string());
            }
            json!([{
                "name": "anyagents",
                "command": guest_exe.display().to_string(),
                "args": proxy_args,
                "env": []
            }])
        } else {
            json!([])
        };

        let harness = Self {
            rpc,
            shared,
            work_dir: work_dir.to_path_buf(),
            mcp_servers,
        };
        harness.handshake()?;
        Ok(harness)
    }

    pub(crate) fn session_id(&self) -> Option<String> {
        let st = lock(&self.shared);
        if !st.session.is_empty() {
            Some(st.session.clone())
        } else {
            st.wanted_session.clone()
        }
    }

    pub(crate) fn answer(&self, id: &str) -> Option<String> {
        lock(&self.shared).answers.get(id).cloned()
    }

    fn call(&self, method: &str, params: Value) -> Result<Value, HarnessError> {
        self.rpc
            .request(method, params, CALL_TIMEOUT)
            .map_err(map_rpc_err)
    }

    fn handshake(&self) -> Result<(), HarnessError> {
        let initialized = self.call(
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": { "readTextFile": true, "writeTextFile": true },
                    "terminal": true
                }
            }),
        )?;
        let peer = initialized.get("agentInfo").and_then(|info| {
            let name = info.get("name")?.as_str()?;
            Some(match info.get("version").and_then(Value::as_str) {
                Some(v) => format!("{name} {v}"),
                None => name.to_string(),
            })
        });
        let wanted = {
            let mut st = lock(&self.shared);
            st.peer = peer;
            st.wanted_session.clone()
        };
        let cwd = self.work_dir.display().to_string();
        let res = match &wanted {
            Some(sid) => self
                .call(
                    "session/load",
                    json!({
                        "sessionId": sid,
                        "cwd": cwd,
                        "mcpServers": self.mcp_servers,
                        "_meta": { "yoloMode": true }
                    }),
                )
                .or_else(|_| {
                    self.call(
                        "session/new",
                        json!({
                            "cwd": cwd,
                            "mcpServers": self.mcp_servers,
                            "_meta": { "yoloMode": true }
                        }),
                    )
                })?,
            None => self.call(
                "session/new",
                json!({
                    "cwd": cwd,
                    "mcpServers": self.mcp_servers,
                    "_meta": { "yoloMode": true }
                }),
            )?,
        };
        let sid = res
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(wanted)
            .ok_or_else(|| HarnessError::Failed("grok session id missing".into()))?;
        lock(&self.shared).session = sid;
        Ok(())
    }
}

fn on_grok_notification(st: &mut GrokState, value: &Value) -> NotificationEffect {
    let method = value.get("method").and_then(Value::as_str).unwrap_or("");
    let params = value.get("params").unwrap_or(&Value::Null);
    match method {
        "$rpc/response" => {
            if let Some(req_id) = params.get("id").and_then(Value::as_i64)
                && let Some(msg_id) = st.requests.remove(&req_id)
            {
                let stop_reason = params
                    .pointer("/result/stopReason")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let error = params.get("error").filter(|v| !v.is_null());
                if let Some(err) = error.filter(|_| !st.admitted.contains_key(&msg_id)) {
                    let msg = err
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("grok request failed");
                    st.rejected.insert(msg_id, msg.to_string());
                } else {
                    if stop_reason == "cancelled" && !st.admitted.contains_key(&msg_id) {
                        return NotificationEffect::default();
                    }
                    st.finished.insert(msg_id.clone(), true);
                    st.turns_served = st.turns_served.saturating_add(1);
                    if !st.current_text.is_empty() {
                        let ans = std::mem::take(&mut st.current_text);
                        st.answers.insert(msg_id, ans);
                    }
                }
                st.unanswered = st.unanswered.saturating_sub(1);
            }
        }
        "session/update" => {
            let kind = params
                .pointer("/update/sessionUpdate")
                .and_then(Value::as_str)
                .unwrap_or("");
            if kind == "agent_message_chunk"
                && let Some(chunk) = params
                    .pointer("/update/content/text")
                    .and_then(Value::as_str)
            {
                st.current_text.push_str(chunk);
                return NotificationEffect {
                    unobserved: true,
                    ..NotificationEffect::default()
                };
            }
            if kind == "user_message_chunk" {
                let raw = params.to_string();
                if let Some(id) = st
                    .requests
                    .values()
                    .find(|id| raw.contains(id.as_str()))
                    .cloned()
                {
                    st.admitted.entry(id).or_insert(false);
                }
            }
        }
        "x.ai/queue/changed" => {
            let raw = params.to_string();
            let known: Vec<String> = st.requests.values().cloned().collect();
            for id in known {
                if raw.contains(&id) {
                    st.admitted.entry(id).or_insert(false);
                }
            }
        }
        _ => {}
    }
    NotificationEffect::default()
}

impl Harness for GrokGuestHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let id = message.id.as_str();
        let session = {
            let mut st = lock(&self.shared);
            if let Some(reason) = st.rejected.get(id) {
                return Ok(DeliverOutcome::rejected(reason.clone(), false));
            }
            if st.finished.contains_key(id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: true,
                });
            }
            if let Some(&dup) = st.admitted.get(id) {
                return Ok(if dup {
                    DeliverOutcome::Duplicate
                } else {
                    DeliverOutcome::Accepted {
                        turn_finished: false,
                    }
                });
            }
            if !self.rpc.alive() {
                return Err(HarnessError::Dead);
            }
            if st.requests.values().any(|v| v == id) {
                return Ok(DeliverOutcome::InFlight);
            }
            if st.session.is_empty() {
                return Err(HarnessError::Failed("no grok session".into()));
            }
            st.unanswered = st.unanswered.saturating_add(1);
            st.session.clone()
        };
        let body = marked_message(&message.id, &message.text);
        let req_id = self
            .rpc
            .send_request(
                "session/prompt",
                json!({
                    "sessionId": session,
                    "prompt": [{"type": "text", "text": body}],
                }),
            )
            .map_err(map_rpc_err)?;
        let mut st = lock(&self.shared);
        st.requests.insert(req_id, message.id.clone());
        Ok(DeliverOutcome::InFlight)
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        let st = lock(&self.shared);
        if st.finished.contains_key(message_id) {
            return Some(Proof::Taken {
                turn_finished: true,
            });
        }
        st.admitted.get(message_id).map(|&dup| {
            if dup {
                Proof::Duplicate
            } else {
                Proof::Taken {
                    turn_finished: false,
                }
            }
        })
    }

    fn stop(&mut self) {
        self.rpc.stop_child();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if self.rpc.alive() {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        if !self.rpc.alive() {
            return Err(HarnessError::Dead);
        }
        let cwd = self.work_dir.display().to_string();
        let _ = self.call(
            "session/load",
            json!({
                "sessionId": session,
                "cwd": cwd,
                "mcpServers": self.mcp_servers,
                "_meta": { "yoloMode": true }
            }),
        );
        lock(&self.shared).session = session.to_string();
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let session = lock(&self.shared).session.clone();
        if !session.is_empty() {
            self.rpc
                .notify("session/cancel", json!({ "sessionId": session }))
                .map_err(map_rpc_err)?;
        }
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let st = lock(&self.shared);
        let alive = self.rpc.alive();
        HarnessHealth {
            process_alive: alive,
            turn_state: if alive && st.unanswered > 0 {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: Delivery {
                level: DeliveryLevel::MidTurn,
                idempotent: false,
                proof: std::borrow::Cow::Borrowed("session/update user_message_chunk"),
                peer: st.peer.clone(),
            },
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    fn process_id(&self) -> Option<u32> {
        self.rpc.process_id()
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).turns_served
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.rpc.watch_activity(notify)
    }
}

fn map_rpc_err(err: RpcFail) -> HarnessError {
    match err {
        RpcFail::Dead => HarnessError::Dead,
        other => HarnessError::Failed(other.reason()),
    }
}

// ============================================================================
// Dsh Guest Adapter (Gateway HTTP + WebSocket follow)
// ============================================================================

struct DshState {
    child: Option<Child>,
    alive: bool,
    port: Option<u16>,
    launch_token: Option<String>,
    cookie: Option<String>,
    session: String,
    admitted: HashMap<String, bool>,
    finished: HashMap<String, bool>,
    rejected: HashMap<String, String>,
    answers: HashMap<String, String>,
    open_ids: VecDeque<String>,
    in_turn: bool,
    turns_served: u32,
    current_text: String,
    follow_sock: Option<TcpStream>,
    watchers: Vec<Arc<dyn Fn() + Send + Sync>>,
}

impl DshState {
    fn notify(&self) {
        for cb in &self.watchers {
            cb();
        }
    }
}

pub(crate) struct DshGuestHarness {
    environment: Vec<(String, String)>,
    work_dir: PathBuf,
    shared: Arc<Mutex<DshState>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    follower: Option<JoinHandle<()>>,
}

impl DshGuestHarness {
    pub(crate) fn start(
        program: impl Into<PathBuf>,
        patch_file: Option<&Path>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: &Path,
    ) -> Result<Self, HarnessError> {
        let program = program.into();
        let env_vec: Vec<(String, String)> = environment
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let session = session_id
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(pseudo_uuid);

        let mut args = vec![
            "--profile".to_string(),
            "web".to_string(),
            "--no-open".to_string(),
        ];
        if let Some(patch) = patch_file {
            args.push("--patch".to_string());
            args.push(patch.display().to_string());
        }
        args.push("--port".to_string());
        args.push("0".to_string());

        let mut cmd = spawn_command(&program, &env_vec)
            .map_err(|e| HarnessError::Failed(format!("spawn dsh: {e}")))?;
        cmd.args(&args)
            .current_dir(work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = cmd
            .spawn()
            .map_err(|e| HarnessError::Failed(format!("spawn dsh: {e}")))?;
        let Some(stdout) = child.stdout.take() else {
            anyagents_harness::kill_child(&mut child, &env_vec);
            let _ = child.wait();
            return Err(HarnessError::Failed("dsh stdout missing".into()));
        };

        let shared = Arc::new(Mutex::new(DshState {
            child: Some(child),
            alive: true,
            port: None,
            launch_token: None,
            cookie: None,
            session: session.clone(),
            admitted: HashMap::new(),
            finished: HashMap::new(),
            rejected: HashMap::new(),
            answers: HashMap::new(),
            open_ids: VecDeque::new(),
            in_turn: false,
            turns_served: 0,
            current_text: String::new(),
            follow_sock: None,
            watchers: Vec::new(),
        }));
        let wake = Arc::new(Condvar::new());
        let r_shared = Arc::clone(&shared);
        let r_wake = Arc::clone(&wake);
        let reader = thread::Builder::new()
            .name("anyagents-guest-dsh-stdout".into())
            .spawn(move || {
                let mut buf = BufReader::new(stdout);
                let mut line = String::new();
                while let Ok(n) = buf.read_line(&mut line) {
                    if n == 0 {
                        break;
                    }
                    if let Some((port, token)) = parse_dsh_banner(&line) {
                        let mut st = lock(&r_shared);
                        st.port = Some(port);
                        st.launch_token = Some(token);
                        r_wake.notify_all();
                        st.notify();
                    }
                    line.clear();
                }
                let mut st = lock(&r_shared);
                st.alive = false;
                r_wake.notify_all();
                st.notify();
            })
            .map_err(|e| HarnessError::Failed(e.to_string()))?;

        let mut harness = Self {
            environment: env_vec,
            work_dir: work_dir.to_path_buf(),
            shared,
            wake,
            reader: Some(reader),
            follower: None,
        };
        harness.connect()?;
        Ok(harness)
    }

    pub(crate) fn session_id(&self) -> Option<String> {
        let st = lock(&self.shared);
        (!st.session.is_empty()).then(|| st.session.clone())
    }

    pub(crate) fn answer(&self, id: &str) -> Option<String> {
        lock(&self.shared).answers.get(id).cloned()
    }

    fn connect(&mut self) -> Result<(), HarnessError> {
        let deadline = Instant::now() + START_TIMEOUT;
        let (port, token, session) = {
            let mut st = lock(&self.shared);
            while st.alive && (st.port.is_none() || st.launch_token.is_none()) {
                let now = Instant::now();
                if now >= deadline {
                    return Err(HarnessError::Failed(
                        "dsh did not print web address in time".into(),
                    ));
                }
                st = self
                    .wake
                    .wait_timeout(st, deadline - now)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            if !st.alive {
                return Err(HarnessError::Dead);
            }
            (
                st.port.expect("checked"),
                st.launch_token.clone().expect("checked"),
                st.session.clone(),
            )
        };
        let cookie = dsh_exchange_token(port, &token).map_err(HarnessError::Failed)?;
        dsh_rpc_call(
            port,
            &cookie,
            "session/create",
            "init",
            &json!({ "request": { "sessionId": session, "cwd": self.work_dir.display().to_string() } }),
        )
        .map_err(HarnessError::Failed)?;
        {
            let mut st = lock(&self.shared);
            st.cookie = Some(cookie.clone());
        }
        self.start_follow(port, &cookie, &session)
    }

    fn start_follow(&mut self, port: u16, cookie: &str, session: &str) -> Result<(), HarnessError> {
        let mut ws = ws_connect(port, cookie).map_err(HarnessError::Failed)?;
        ws_send_text(
            &mut ws,
            &json!({
                "type": "open",
                "streamId": "follow",
                "endpoint": "session/follow",
                "payload": {"args": {"request": {
                    "address": {"kind": "session", "sessionId": session},
                    "maxMessages": 1,
                }}},
            })
            .to_string(),
        )
        .map_err(HarnessError::Failed)?;
        let handle = ws.try_clone().ok();
        {
            let mut st = lock(&self.shared);
            if let Some(old) = st.follow_sock.take() {
                let _ = old.shutdown(Shutdown::Both);
            }
            st.follow_sock = handle;
        }
        let f_shared = Arc::clone(&self.shared);
        let f_wake = Arc::clone(&self.wake);
        let join = thread::Builder::new()
            .name("anyagents-guest-dsh-follow".into())
            .spawn(move || {
                while let Ok(Some(text)) = ws_read_text(&mut ws) {
                    if let Ok(frame) = serde_json::from_str::<Value>(&text) {
                        let ev = frame.get("value").unwrap_or(&frame);
                        let ty = ev.get("type").and_then(Value::as_str).unwrap_or("");
                        let mut st = lock(&f_shared);
                        let raw = ev.to_string();
                        if let Some(mid) = message_id_in_any_string(&raw) {
                            st.admitted.entry(mid).or_insert(false);
                        }
                        match ty {
                            "turn/start" => {
                                st.in_turn = true;
                                st.current_text.clear();
                            }
                            "assistant/delta" | "agent/delta" | "message/delta" => {
                                if let Some(delta) = ev
                                    .pointer("/data/delta")
                                    .or_else(|| ev.pointer("/data/text"))
                                    .and_then(Value::as_str)
                                {
                                    st.current_text.push_str(delta);
                                }
                            }
                            "turn/end" => {
                                st.in_turn = false;
                                st.turns_served = st.turns_served.saturating_add(1);
                                let ans = std::mem::take(&mut st.current_text);
                                let closed: Vec<String> = st.open_ids.drain(..).collect();
                                for oid in closed {
                                    st.finished.insert(oid.clone(), true);
                                    if !ans.is_empty() {
                                        st.answers.insert(oid, ans.clone());
                                    }
                                }
                            }
                            _ => {}
                        }
                        f_wake.notify_all();
                        st.notify();
                    }
                }
            })
            .map_err(|e| HarnessError::Failed(e.to_string()))?;
        self.follower = Some(join);
        Ok(())
    }
}

impl Harness for DshGuestHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let (port, cookie, session) = {
            let mut st = lock(&self.shared);
            if let Some(reason) = st.rejected.get(&message.id) {
                return Ok(DeliverOutcome::rejected(reason.clone(), false));
            }
            if st.finished.contains_key(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: true,
                });
            }
            if let Some(&dup) = st.admitted.get(&message.id) {
                return Ok(if dup {
                    DeliverOutcome::Duplicate
                } else {
                    DeliverOutcome::Accepted {
                        turn_finished: false,
                    }
                });
            }
            if !st.alive {
                return Err(HarnessError::Dead);
            }
            let Some(port) = st.port else {
                return Err(HarnessError::Failed("dsh port unavailable".into()));
            };
            let Some(cookie) = st.cookie.clone() else {
                return Err(HarnessError::Failed("dsh auth cookie unavailable".into()));
            };
            st.open_ids.push_back(message.id.clone());
            st.in_turn = true;
            (port, cookie, st.session.clone())
        };
        let body = marked_message(&message.id, &message.text);
        dsh_rpc_call(
            port,
            &cookie,
            "session/prompt",
            &message.id,
            &json!({
                "request": {
                    "sessionId": session,
                    "requestId": message.id,
                    "content": [{ "type": "text", "text": body }],
                    "mode": "steer",
                }
            }),
        )
        .map_err(HarnessError::Failed)?;
        let mut st = lock(&self.shared);
        st.admitted.entry(message.id.clone()).or_insert(false);
        Ok(DeliverOutcome::Accepted {
            turn_finished: false,
        })
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        let st = lock(&self.shared);
        if st.finished.contains_key(message_id) {
            return Some(Proof::Taken {
                turn_finished: true,
            });
        }
        st.admitted.get(message_id).map(|&dup| {
            if dup {
                Proof::Duplicate
            } else {
                Proof::Taken {
                    turn_finished: false,
                }
            }
        })
    }

    fn stop(&mut self) {
        let mut child = {
            let mut st = lock(&self.shared);
            st.alive = false;
            if let Some(s) = st.follow_sock.take() {
                let _ = s.shutdown(Shutdown::Both);
            }
            st.child.take()
        };
        if let Some(ref mut c) = child {
            anyagents_harness::kill_child(c, &self.environment);
            let _ = c.wait();
        }
        if let Some(h) = self.reader.take() {
            let _ = h.join();
        }
        if let Some(h) = self.follower.take() {
            let _ = h.join();
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
        let (port, cookie) = {
            let st = lock(&self.shared);
            (
                st.port
                    .ok_or_else(|| HarnessError::Failed("no dsh port".into()))?,
                st.cookie
                    .clone()
                    .ok_or_else(|| HarnessError::Failed("no dsh cookie".into()))?,
            )
        };
        dsh_rpc_call(
            port,
            &cookie,
            "session/create",
            "init",
            &json!({ "request": { "sessionId": session, "cwd": self.work_dir.display().to_string() } }),
        )
        .map_err(HarnessError::Failed)?;
        lock(&self.shared).session = session.to_string();
        self.start_follow(port, &cookie, session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let (port, cookie, session) = {
            let st = lock(&self.shared);
            (
                st.port
                    .ok_or_else(|| HarnessError::Failed("no dsh port".into()))?,
                st.cookie
                    .clone()
                    .ok_or_else(|| HarnessError::Failed("no dsh cookie".into()))?,
                st.session.clone(),
            )
        };
        dsh_rpc_call(
            port,
            &cookie,
            "session/cancel",
            "cancel",
            &json!({ "request": { "sessionId": session } }),
        )
        .map(|_| ())
        .map_err(HarnessError::Failed)
    }

    fn health(&self) -> HarnessHealth {
        let st = lock(&self.shared);
        HarnessHealth {
            process_alive: st.alive,
            turn_state: if st.alive && st.in_turn {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: Delivery {
                level: DeliveryLevel::MidTurn,
                idempotent: true,
                proof: std::borrow::Cow::Borrowed("session/follow stream"),
                peer: None,
            },
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    fn process_id(&self) -> Option<u32> {
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).turns_served
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        lock(&self.shared).watchers.push(notify);
    }
}

fn parse_dsh_banner(line: &str) -> Option<(u16, String)> {
    let idx = line.find("127.0.0.1:")?;
    let after = &line[idx + "127.0.0.1:".len()..];
    let port_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    let port = port_str.parse::<u16>().ok()?;
    let tok_idx = after.find("token=")?;
    let tok: String = after[tok_idx + "token=".len()..]
        .chars()
        .take_while(|c| !c.is_whitespace() && *c != '"' && *c != '\'')
        .collect();
    (!tok.is_empty()).then_some((port, tok))
}

fn dsh_exchange_token(port: u16, token: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    let _ = stream.set_read_timeout(Some(CALL_TIMEOUT));
    let req = format!(
        "GET /?token={token} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .map_err(|e| e.to_string())?;
    for line in resp.lines() {
        if line.to_ascii_lowercase().starts_with("set-cookie:")
            && let Some((_, v)) = line.split_once(':')
            && let Some(pair) = v.trim().split(';').next()
            && pair.trim().starts_with("dsh-auth-")
        {
            return Ok(pair.trim().to_string());
        }
    }
    Err("dsh token exchange did not return dsh-auth cookie".into())
}

fn dsh_rpc_call(
    port: u16,
    cookie: &str,
    method: &str,
    rpc_id: &str,
    args: &Value,
) -> Result<Value, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    let _ = stream.set_read_timeout(Some(CALL_TIMEOUT));
    let payload = json!({
        "type": "client-request",
        "rpcId": rpc_id,
        "method": method,
        "payload": { "args": args },
    });
    let body = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    let hdr = format!(
        "POST /api/{method} HTTP/1.0\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Origin: http://127.0.0.1:{port}\r\n\
         Cookie: {cookie}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(hdr.as_bytes())
        .map_err(|e| e.to_string())?;
    stream.write_all(&body).map_err(|e| e.to_string())?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).map_err(|e| e.to_string())?;
    let (_, resp_body) = raw.split_once("\r\n\r\n").unwrap_or(("", &raw));
    serde_json::from_str::<Value>(resp_body.trim()).or_else(|_| Ok(json!({})))
}

fn ws_connect(port: u16, cookie: &str) -> Result<TcpStream, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    let _ = stream.set_read_timeout(Some(CALL_TIMEOUT));
    let key_hdr = ["Sec-WebSocket", "Key"].join("-");
    let req = format!(
        "GET /api/remote.mux HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Origin: http://127.0.0.1:{port}\r\n\
         Cookie: {cookie}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         {key_hdr}: AAAAAAAAAAAAAAAAAAAAAA==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    loop {
        let mut b = [0u8; 1];
        if stream.read_exact(&mut b).is_err() {
            return Err("dsh websocket handshake truncated".into());
        }
        buf.push(b[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let _ = stream.set_read_timeout(None);
    Ok(stream)
}

fn ws_send_text(stream: &mut TcpStream, text: &str) -> Result<(), String> {
    let data = text.as_bytes();
    let mut frame = vec![0x81u8];
    if data.len() < 126 {
        frame.push(0x80 | (data.len() as u8));
    } else if data.len() <= u16::MAX as usize {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(data.len() as u16).to_be_bytes());
    } else {
        frame.push(0x80 | 127);
        frame.extend_from_slice(&(data.len() as u64).to_be_bytes());
    }
    let mask = [0x11u8, 0x22, 0x33, 0x44];
    frame.extend_from_slice(&mask);
    for (i, &b) in data.iter().enumerate() {
        frame.push(b ^ mask[i % 4]);
    }
    stream.write_all(&frame).map_err(|e| e.to_string())
}

fn ws_read_text(stream: &mut TcpStream) -> Result<Option<String>, String> {
    loop {
        let mut h = [0u8; 2];
        match stream.read_exact(&mut h) {
            Ok(()) => {}
            Err(_) => return Ok(None),
        }
        let opcode = h[0] & 0x0f;
        let masked = (h[1] & 0x80) != 0;
        let mut len = (h[1] & 0x7f) as u64;
        if len == 126 {
            let mut b = [0u8; 2];
            stream.read_exact(&mut b).map_err(|e| e.to_string())?;
            len = u16::from_be_bytes(b) as u64;
        } else if len == 127 {
            let mut b = [0u8; 8];
            stream.read_exact(&mut b).map_err(|e| e.to_string())?;
            len = u64::from_be_bytes(b);
        }
        let mut mask = [0u8; 4];
        if masked {
            stream.read_exact(&mut mask).map_err(|e| e.to_string())?;
        }
        let mut payload = vec![0u8; len as usize];
        stream.read_exact(&mut payload).map_err(|e| e.to_string())?;
        if masked {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= mask[i % 4];
            }
        }
        if opcode == 0x8 {
            return Ok(None);
        }
        if opcode == 0x9 {
            // ping -> continue
            continue;
        }
        if opcode == 0x1 || opcode == 0x0 {
            return Ok(Some(String::from_utf8_lossy(&payload).into_owned()));
        }
    }
}

fn message_id_in_any_string(raw: &str) -> Option<String> {
    for line in raw.lines() {
        if let Some(id) = message_id_in_text(line.trim()) {
            return Some(id.to_string());
        }
    }
    None
}

fn pseudo_uuid() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("dsh-{:016x}-{}", nanos as u64, std::process::id())
}
