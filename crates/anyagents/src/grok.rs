//! Grok CLI (`grok`) native ACP JSON-RPC 2.0 daemon adapter
//! (`grok agent --no-leader --always-approve stdio`).
//!
//! Speaks ACP over stdio via [`RpcChild`], configures `[ui] follow_up_behavior = "steer"`
//! in `<config_dir>/.grok/config.toml` plus `[mcp_servers.anyagents]`, and sends
//! `session/prompt` asynchronously (`send_request`) so mid-turn messages steer
//! into the active turn without blocking the delivery loop.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnState, TurnUsage, compose_message, marked_message, message_id_in_text,
};
use anyagents_harness_stream::{NotificationEffect, RpcChild, RpcFail, RpcLaunch};
use serde_json::{Value, json};

fn rpc_wait() -> Duration {
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
        Duration::from_secs(5)
    } else {
        Duration::from_secs(60)
    }
}

pub struct GrokHarness {
    rpc: RpcChild,
    shared: Arc<Mutex<State>>,
    work_dir: PathBuf,
    mcp_servers: Value,
}

#[derive(Default)]
struct State {
    session: String,
    wanted_session: Option<String>,
    open: Vec<String>,
    inflight_reqs: HashSet<i64>,
    seen: HashSet<String>,
    written: Vec<String>,
    finished: HashSet<String>,
    turns_started: usize,
    usages: Vec<(String, TurnUsage)>,
    quota_reset_at_ms: Option<i64>,
}

impl GrokHarness {
    pub fn start(
        program: impl Into<PathBuf>,
        model: Option<&str>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: &Path,
        mcp_command: &Path,
        mcp_args: &[String],
    ) -> Result<Self, HarnessError> {
        let shared = Arc::new(Mutex::new(State::default()));
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
                name: "anyagents-grok".to_string(),
                program: program.into(),
                args,
                environment: environment
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
                work_dir: Some(work_dir.to_path_buf()),
            },
            Arc::new(move |line| on_notification(&mut lock(&on_shared), line)),
            Arc::new(on_request),
        )?;
        lock(&shared).wanted_session = session_id.map(str::to_string);
        let mcp_servers = json!([{
            "name": "anyagents",
            "command": mcp_command.display().to_string(),
            "args": mcp_args,
            "env": []
        }]);
        let harness = Self {
            rpc,
            shared,
            work_dir: work_dir.to_path_buf(),
            mcp_servers,
        };
        match harness.handshake() {
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

    pub fn session_id(&self) -> Option<String> {
        let st = lock(&self.shared);
        if !st.session.is_empty() {
            return Some(st.session.clone());
        }
        st.wanted_session.clone()
    }

    fn handshake(&self) -> Result<(), HarnessError> {
        self.call(
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": { "readTextFile": true, "writeTextFile": true },
                    "terminal": true
                }
            }),
            rpc_wait(),
        )?;
        let wanted = lock(&self.shared).wanted_session.clone();
        let cwd = self.work_dir.display().to_string();
        let result = match &wanted {
            Some(sid) => {
                let load_params = json!({
                    "sessionId": sid,
                    "cwd": cwd,
                    "mcpServers": self.mcp_servers,
                    "_meta": { "yoloMode": true }
                });
                match self.call("session/load", load_params, rpc_wait()) {
                    Ok(res) => res,
                    Err(_) => self.call(
                        "session/new",
                        json!({
                            "cwd": cwd,
                            "mcpServers": self.mcp_servers,
                            "_meta": { "yoloMode": true }
                        }),
                        rpc_wait(),
                    )?,
                }
            }
            None => self.call(
                "session/new",
                json!({
                    "cwd": cwd,
                    "mcpServers": self.mcp_servers,
                    "_meta": { "yoloMode": true }
                }),
                rpc_wait(),
            )?,
        };
        let session = result
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(wanted)
            .ok_or_else(|| HarnessError::Failed("grok session id missing".into()))?;
        lock(&self.shared).session = session;
        Ok(())
    }

    fn call(&self, method: &str, params: Value, wait: Duration) -> Result<Value, HarnessError> {
        self.rpc.request(method, params, wait).map_err(fail)
    }
}

impl Harness for GrokHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let session = {
            let st = lock(&self.shared);
            if !self.rpc.alive() {
                return Err(HarnessError::Dead);
            }
            if st.seen.contains(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: st.finished.contains(&message.id),
                });
            }
            if let Some(reset_at_ms) = st.quota_reset_at_ms {
                return Ok(DeliverOutcome::Quota { reset_at_ms });
            }
            if st.session.is_empty() {
                return Err(HarnessError::Failed("no grok session".into()));
            }
            st.session.clone()
        };
        let params = json!({
            "sessionId": session,
            "prompt": [{"type": "text", "text": compose_text(message)}],
        });
        {
            let mut st = lock(&self.shared);
            if st.open.is_empty() {
                st.turns_started += 1;
            }
            if !st.open.contains(&message.id) {
                st.open.push(message.id.clone());
            }
        }
        match self.rpc.send_request("session/prompt", params) {
            Ok(req_id) => {
                let mut st = lock(&self.shared);
                st.inflight_reqs.insert(req_id);
                st.written.push(message.id.clone());
                st.seen.insert(message.id.clone());
                Ok(DeliverOutcome::Accepted {
                    turn_finished: st.finished.contains(&message.id),
                })
            }
            Err(err) => {
                let mut st = lock(&self.shared);
                st.open.retain(|id| id != &message.id);
                Err(fail(err))
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
            rpc_wait(),
        );
        let mut st = lock(&self.shared);
        st.session = session.to_string();
        st.open.clear();
        st.inflight_reqs.clear();
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let session = lock(&self.shared).session.clone();
        if !session.is_empty() {
            let _ = self
                .rpc
                .notify("session/cancel", json!({ "sessionId": session }));
        }
        let mut st = lock(&self.shared);
        st.open.clear();
        st.inflight_reqs.clear();
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let st = lock(&self.shared);
        HarnessHealth {
            process_alive: self.rpc.alive(),
            turn_state: if st.open.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: st.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }

    fn clear_quota(&mut self) {
        lock(&self.shared).quota_reset_at_ms = None;
    }

    fn process_id(&self) -> Option<u32> {
        self.rpc.process_id()
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).finished.len() as u32
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.rpc.watch_activity(notify)
    }
}

pub fn write_grok_config(
    config_dir: &Path,
    model: Option<&str>,
    command: &Path,
    args: &[String],
) -> Result<(), anyagents_core::Error> {
    let grok_dir = config_dir.join(".grok");
    std::fs::create_dir_all(&grok_dir)?;
    let path = grok_dir.join("config.toml");
    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let mut lines: Vec<String> = Vec::new();
    let mut in_mcp_anyagents = false;
    for line in existing.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            in_mcp_anyagents = t == "[mcp_servers.anyagents]";
        }
        if in_mcp_anyagents
            || t.starts_with("follow_up_behavior")
            || t.starts_with("default_model")
        {
            continue;
        }
        lines.push(line.to_string());
    }
    let mut out = lines.join("\n").trim().to_string();
    if !out.contains("[ui]") {
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str("\n[ui]\nfollow_up_behavior = \"steer\"\n");
    } else {
        out = out.replacen("[ui]", "[ui]\nfollow_up_behavior = \"steer\"", 1);
    }
    if let Some(m) = model.map(str::trim).filter(|s| !s.is_empty()) {
        out = format!("default_model = \"{}\"\n{out}", m.replace('"', "\\\""));
    }
    let escaped_args: Vec<String> = args
        .iter()
        .map(|a| format!("\"{}\"", a.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&format!(
        "\n[mcp_servers.anyagents]\ncommand = \"{}\"\nargs = [{}]\nenabled = true\n",
        command
            .display()
            .to_string()
            .replace('\\', "\\\\")
            .replace('"', "\\\""),
        escaped_args.join(", ")
    ));
    let tmp = path.with_extension("toml.new");
    std::fs::write(&tmp, out)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o664));
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

pub fn write_omp_mcp_config(
    config_dir: &Path,
    command: &Path,
    args: &[String],
) -> Result<(), anyagents_core::Error> {
    let omp_dir = config_dir.join(".omp").join("agent");
    std::fs::create_dir_all(&omp_dir)?;
    crate::mcp_config::write_claude(&omp_dir.join("mcp.json"), command, args)
}

fn on_request(method: &str, params: &Value) -> Option<Value> {
    match method {
        "session/request_permission" => {
            let option_id = params
                .pointer("/params/options/0/optionId")
                .or_else(|| params.pointer("/options/0/optionId"))
                .and_then(Value::as_str)
                .unwrap_or("allow");
            Some(json!({
                "outcome": {
                    "outcome": "selected",
                    "optionId": option_id
                }
            }))
        }
        "ping" => Some(json!({})),
        _ => None,
    }
}

fn on_notification(state: &mut State, value: &Value) -> NotificationEffect {
    let method = value.get("method").and_then(Value::as_str).unwrap_or("");
    let params = value.get("params").cloned().unwrap_or(Value::Null);
    note_ids(state, &params);
    if method == "$rpc/response" {
        if let Some(id) = params.get("id").and_then(Value::as_i64) {
            state.inflight_reqs.remove(&id);
        }
        if state.inflight_reqs.is_empty() && !state.open.is_empty() {
            for msg_id in state.open.drain(..) {
                state.finished.insert(msg_id.clone());
                state.usages.push((msg_id, TurnUsage::default()));
            }
        }
    }
    NotificationEffect::default()
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

fn compose_text(message: &OutboundMessage) -> String {
    let context = &message.context;
    let known = context.sender.is_some()
        || context.chat_id.is_some()
        || context.message_id.is_some()
        || context.destination.is_some();
    if !known {
        return marked_message(&message.id, &message.text);
    }
    compose_message(&message.id, context, &message.text)
}

fn fail(fail: RpcFail) -> HarnessError {
    match fail {
        RpcFail::Dead => HarnessError::Dead,
        other => HarnessError::Failed(other.reason()),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
