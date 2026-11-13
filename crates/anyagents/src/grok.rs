//! Grok CLI (`grok`) native ACP JSON-RPC 2.0 adapter
//! (`grok agent --no-leader --always-approve stdio`).
//!
//! Speaks ACP over stdio via [`RpcChild`] and configures
//! `[ui] follow_up_behavior = "steer"` in `<config_dir>/.grok/config.toml`
//! plus `[mcp_servers.anyagents]`. Every mailbox message is one
//! `session/prompt`, sent without waiting for its answer: an idle session runs
//! it as a turn; a busy one appends it to its `pending_inputs` queue and
//! promotes it into the running turn at the next safe gap.
//!
//! Delivery is `mid-turn` (`docs/delivery.md`). A message is accepted once
//! Grok shows it admitted: its marker line in an `x.ai/queue/changed` report
//! (the running prompt or a pending entry), in a `user_message_chunk` or an
//! `x.ai/session/interjection`, or the answer to its request. Grok names none
//! of these by an id AnyAgents sends, so the marker line is the link, and it
//! deduplicates nothing. A JSON-RPC error answer, before Grok showed the
//! prompt admitted, that carries one of Grok's refusal texts is a refusal:
//! the message goes back to the mailbox. Any other error leaves it unknown
//! whether Grok applied the prompt, so it counts as taken and is never
//! written again. A message is written only once every earlier one is
//! admitted, so a refused message is never overtaken by a later one.
//!
//! These wire facts come from the 1.0.67 binary's strings, not from a live
//! session; `tests/invariants/fakes/proto_grok.py` marks which are inferred.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth,
    MESSAGE_MARKER_PREFIX, OutboundMessage, TurnState, compose_message, marked_message,
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

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::MidTurn,
    idempotent: false,
    proof: std::borrow::Cow::Borrowed("marker in x.ai/queue/changed or user_message_chunk; session/prompt answer"),
    peer: None,
};

/// What Grok answers a `session/prompt` it turns away before queueing it:
/// "prompt blocked: ..." (no model; a `UserPromptSubmit` hook's "Prompt
/// blocked by hook ..."), "prompt refused: session folder removed", "The
/// session is not ready yet. Send again in a moment.".
const REFUSALS: &[&str] = &[
    "prompt blocked",
    "prompt refused",
    "session is not ready yet",
];

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
    /// What the `initialize` answer's `agentInfo` named.
    peer: Option<String>,
    /// Every message this process was given, by mailbox id.
    prompts: HashMap<String, Prompt>,
    /// The mailbox id behind each `session/prompt` request.
    requests: HashMap<i64, String>,
    /// Answers that arrived before their request was recorded: the error
    /// message, or `None` for a result.
    early: HashMap<i64, Option<String>>,
    /// Written, and Grok has shown nothing about it yet.
    unconfirmed: HashSet<String>,
    /// Written and not yet answered.
    unanswered: usize,
    /// A prompt was taken since the session was last idle.
    serving: bool,
    turns_served: u32,
}

/// One `session/prompt` this process wrote.
struct Prompt {
    /// `None` while the line is on its way.
    request: Option<i64>,
    /// Grok showed it queued or in its turn.
    admitted: bool,
    answer: Option<Answer>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// Its turn ended, or an error left it unknown whether it was applied.
    Taken,
    /// Grok turned it away before queueing it.
    Refused,
}

impl Prompt {
    /// What the mailbox may record for it, or `None` while Grok has shown
    /// nothing.
    fn outcome(&self) -> Option<DeliverOutcome> {
        match self.answer {
            Some(Answer::Refused) => Some(DeliverOutcome::Deferred),
            Some(Answer::Taken) => Some(DeliverOutcome::Accepted {
                turn_finished: true,
            }),
            None => self.admitted.then_some(DeliverOutcome::Accepted {
                turn_finished: false,
            }),
        }
    }
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
        let allow_native_fs = std::env::var("ANYAGENTS_ALLOW_NATIVE_FILE_TOOLS")
            .ok()
            .is_some_and(|v| v.trim() == "1");
        let initialized = self.call(
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": { "readTextFile": allow_native_fs, "writeTextFile": allow_native_fs },
                    "terminal": true
                }
            }),
            rpc_wait(),
        )?;
        lock(&self.shared).peer = announced_peer(&initialized);
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
        let id = message.id.as_str();
        let session = {
            let mut st = lock(&self.shared);
            // What Grok showed about a message outlives its process.
            if let Some(known) = written_outcome(&mut st, id) {
                return Ok(known);
            }
            if !self.rpc.alive() {
                return Err(HarnessError::Dead);
            }
            if st.prompts.contains_key(id) {
                return Ok(DeliverOutcome::InFlight);
            }
            // Behind a message Grok has not admitted yet nothing is written:
            // if Grok turned that one away, a later one would overtake it.
            if !st.unconfirmed.is_empty() {
                return Ok(DeliverOutcome::Deferred);
            }
            if st.session.is_empty() {
                return Err(HarnessError::Failed("no grok session".into()));
            }
            st.prompts.insert(
                id.to_string(),
                Prompt {
                    request: None,
                    admitted: false,
                    answer: None,
                },
            );
            st.unconfirmed.insert(id.to_string());
            st.unanswered += 1;
            st.session.clone()
        };
        let params = json!({
            "sessionId": session,
            "prompt": [{"type": "text", "text": compose_text(message)}],
        });
        // Never hold the state across the pipe: the reader needs it to drain
        // the child's output.
        let sent = self.rpc.send_request("session/prompt", params);
        let mut st = lock(&self.shared);
        match sent {
            Ok(request) => {
                st.requests.insert(request, id.to_string());
                if let Some(prompt) = st.prompts.get_mut(id) {
                    prompt.request = Some(request);
                }
                if let Some(error) = st.early.remove(&request) {
                    answer(&mut st, id, request, error);
                }
                Ok(written_outcome(&mut st, id).unwrap_or(DeliverOutcome::InFlight))
            }
            Err(err) => {
                st.prompts.remove(id);
                st.unconfirmed.remove(id);
                st.unanswered -= 1;
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
        lock(&self.shared).session = session.to_string();
        Ok(())
    }

    /// Ask Grok to stop the running turn. Its prompts settle when Grok
    /// answers them; nothing is forgotten here.
    fn cancel(&mut self) -> Result<(), HarnessError> {
        let session = lock(&self.shared).session.clone();
        if !session.is_empty() {
            self.rpc
                .notify("session/cancel", json!({ "sessionId": session }))
                .map_err(fail)?;
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
            delivery: DELIVERY.with_peer(st.peer.clone()),
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

/// What the mailbox may record for a message this process wrote, once Grok
/// has shown something about it. A refusal forgets the write, so the next
/// offer writes the message again.
fn written_outcome(state: &mut State, id: &str) -> Option<DeliverOutcome> {
    let outcome = state.prompts.get(id)?.outcome()?;
    if outcome == DeliverOutcome::Deferred {
        state.prompts.remove(id);
    }
    Some(outcome)
}

/// `name version` from the `initialize` answer's `agentInfo`.
fn announced_peer(initialized: &Value) -> Option<String> {
    let info = initialized.get("agentInfo")?;
    let name = info.get("name").and_then(Value::as_str)?;
    Some(match info.get("version").and_then(Value::as_str) {
        Some(version) => format!("{name} {version}"),
        None => name.to_string(),
    })
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
    let existing = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(err) => return Err(err.into()),
    };
    let mut lines: Vec<String> = Vec::new();
    let mut in_mcp_anyagents = false;
    for line in existing.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            in_mcp_anyagents = t == "[mcp_servers.anyagents]";
        }
        if in_mcp_anyagents || t.starts_with("follow_up_behavior") || t.starts_with("default_model")
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
    anyagents_harness::atomic_write_nonempty_file(&path, out.as_bytes(), 0o664)?;
    Ok(())
}

pub const OMP_NATIVE_TOOL_POLICY_YAML: &str = "\
mcp:
  enableProjectConfig: false
web_search:
  enabled: false
task:
  maxRecursionDepth: 0
memory:
  backend: \"off\"
hindsight:
  autoRecall: false
  mentalModelAutoSeed: false
  mentalModelsEnabled: false
mnemopi:
  autoRecall: false
  autoRetain: false
ask:
  enabled: false
";

pub fn ensure_omp_policy(omp_dir: &Path) -> Result<std::path::PathBuf, anyagents_core::Error> {
    std::fs::create_dir_all(omp_dir)?;
    let policy_path = omp_dir.join("anyagents-policy.yml");
    anyagents_harness::atomic_write_nonempty_file(
        &policy_path,
        OMP_NATIVE_TOOL_POLICY_YAML.as_bytes(),
        0o664,
    )?;
    Ok(policy_path)
}

pub fn write_omp_mcp_config(
    config_dir: &Path,
    command: &Path,
    args: &[String],
) -> Result<(), anyagents_core::Error> {
    let omp_dir = config_dir.join(".omp").join("agent");
    std::fs::create_dir_all(&omp_dir)?;
    if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
        ensure_omp_policy(&omp_dir)?;
    }
    crate::mcp_config::write_claude(&omp_dir.join("mcp.json"), command, args)
}

fn on_request(method: &str, params: &Value) -> Option<Value> {
    match method {
        "session/request_permission" => {
            let options = params
                .pointer("/params/options")
                .or_else(|| params.get("options"))?
                .as_array()?;
            // Permission policy is explicit. Malformed/unsupported requests cannot invent a grant.
            let policy = std::env::var("ANYAGENTS_GROK_PERMISSION_KIND")
                .unwrap_or_else(|_| "reject_once".into());
            let option_id = options
                .iter()
                .find(|o| o.get("kind").and_then(Value::as_str) == Some(policy.as_str()))?
                .get("optionId")?
                .as_str()
                .filter(|id| !id.is_empty())?;
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
    let params = value.get("params").unwrap_or(&Value::Null);
    match method {
        "$rpc/response" => {
            if let Some(request) = params.get("id").and_then(Value::as_i64) {
                let error = params
                    .get("error")
                    .filter(|error| !error.is_null())
                    .map(|error| {
                        error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    });
                match state.requests.get(&request).cloned() {
                    Some(id) => answer(state, &id, request, error),
                    None => {
                        state.early.insert(request, error);
                    }
                }
            }
        }
        "x.ai/queue/changed" | "x.ai/session/interjection" => admit_named(state, params),
        "session/update" => {
            if params
                .pointer("/update/sessionUpdate")
                .and_then(Value::as_str)
                != Some("user_message_chunk")
            {
                // Streamed output: nothing the host reads changed.
                return NotificationEffect {
                    unobserved: true,
                    ..NotificationEffect::default()
                };
            }
            admit_named(state, params);
        }
        _ => {}
    }
    NotificationEffect::default()
}

/// Grok answered `request`, which carried `id`: `error` is its error
/// message, `None` for a result.
fn answer(state: &mut State, id: &str, request: i64, error: Option<String>) {
    // A request is answered once.
    state.requests.remove(&request);
    let Some(prompt) = state.prompts.get_mut(id) else {
        return;
    };
    if prompt.request != Some(request) || prompt.answer.is_some() {
        return;
    }
    let refused = error.is_some_and(|message| !prompt.admitted && is_refusal(&message));
    prompt.answer = Some(if refused {
        Answer::Refused
    } else {
        Answer::Taken
    });
    state.unconfirmed.remove(id);
    state.unanswered = state.unanswered.saturating_sub(1);
    state.serving |= !refused;
    if state.serving && state.unanswered == 0 {
        state.serving = false;
        state.turns_served += 1;
    }
}

fn is_refusal(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    REFUSALS.iter().any(|refusal| message.contains(refusal))
}

/// Grok showed a queue or a turn: every unconfirmed message whose marker line
/// it carries is admitted.
fn admit_named(state: &mut State, params: &Value) {
    if state.unconfirmed.is_empty() {
        return;
    }
    let text = params.to_string();
    let named: Vec<String> = state
        .unconfirmed
        .iter()
        .filter(|id| names_marker(&text, id))
        .cloned()
        .collect();
    for id in named {
        state.unconfirmed.remove(&id);
        if let Some(prompt) = state.prompts.get_mut(&id) {
            prompt.admitted = true;
        }
    }
}

/// Whether `text` carries the marker line of `id`, and not that of a longer
/// id it is a prefix of.
fn names_marker(text: &str, id: &str) -> bool {
    let marker = format!("{MESSAGE_MARKER_PREFIX}{id}");
    text.match_indices(&marker).any(|(at, _)| {
        text[at + marker.len()..]
            .chars()
            .next()
            .is_none_or(|next| !(next.is_alphanumeric() || next == '_' || next == '-'))
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unreadable_config_is_preserved_and_absent_config_can_initialize() {
        let dir = tempfile::tempdir().unwrap();
        write_grok_config(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).unwrap();
        let path = dir.path().join(".grok/config.toml");
        std::fs::write(&path, [0xff, 0xfe]).unwrap();
        assert!(write_grok_config(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).is_err());
        assert_eq!(std::fs::read(path).unwrap(), [0xff, 0xfe]);
    }

    #[test]
    fn permission_selection_requires_an_explicit_valid_configured_option() {
        let _lock = crate::server::tests::env_lock();
        let old = std::env::var_os("ANYAGENTS_GROK_PERMISSION_KIND");
        unsafe {
            std::env::remove_var("ANYAGENTS_GROK_PERMISSION_KIND");
        }
        for malformed in [
            json!({}),
            json!({"options":[]}),
            json!({"options":[{"kind":"allow_once","optionId":"allow"}]}),
            json!({"options":[{"kind":"reject_once"}]}),
            json!({"options":[{"kind":"reject_once","optionId":17}]}),
        ] {
            assert!(on_request("session/request_permission", &malformed).is_none());
        }
        let options = json!({"options":[{"kind":"reject_once","optionId":"deny"},{"kind":"allow_once","optionId":"grant"}]});
        assert_eq!(
            on_request("session/request_permission", &options).unwrap()["outcome"]["optionId"],
            "deny"
        );
        unsafe {
            std::env::set_var("ANYAGENTS_GROK_PERMISSION_KIND", "allow_once");
        }
        assert_eq!(
            on_request("session/request_permission", &options).unwrap()["outcome"]["optionId"],
            "grant"
        );
        unsafe {
            match old {
                Some(value) => std::env::set_var("ANYAGENTS_GROK_PERMISSION_KIND", value),
                None => std::env::remove_var("ANYAGENTS_GROK_PERMISSION_KIND"),
            }
        }
    }

    #[test]
    fn write_omp_mcp_config_writes_native_tool_policy_and_mcp_json() {
        let dir = tempfile::tempdir().unwrap();
        write_omp_mcp_config(
            dir.path(),
            Path::new("/opt/bin/anyagents"),
            &["mcp".to_string()],
        )
        .unwrap();
        let mcp_path = dir.path().join(".omp/agent/mcp.json");
        assert!(mcp_path.is_file());
        if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
            let policy =
                std::fs::read_to_string(dir.path().join(".omp/agent/anyagents-policy.yml"))
                    .unwrap();
            assert!(policy.contains("enableProjectConfig: false"), "{policy}");
            assert!(policy.contains("web_search:\n  enabled: false"), "{policy}");
            assert!(policy.contains("maxRecursionDepth: 0"), "{policy}");
            assert!(policy.contains("backend: \"off\""), "{policy}");
        }
    }

    /// One written prompt, as `deliver` records it before the line leaves.
    fn written(state: &mut State, id: &str, request: i64) {
        state.prompts.insert(
            id.to_string(),
            Prompt {
                request: Some(request),
                admitted: false,
                answer: None,
            },
        );
        state.requests.insert(request, id.to_string());
        state.unconfirmed.insert(id.to_string());
        state.unanswered += 1;
    }

    fn answered(request: i64, error: Option<&str>) -> Value {
        json!({"method": "$rpc/response", "params": {
            "id": request,
            "result": if error.is_none() { json!({"stopReason": "end_turn"}) } else { Value::Null },
            "error": error.map_or(Value::Null, |message| json!({"code": -32603, "message": message})),
        }})
    }

    #[test]
    fn a_marker_names_its_own_id_and_not_a_longer_one() {
        let text = json!({"runningText": "anyagents-message:recovery_a_10\nbody"}).to_string();
        assert!(names_marker(&text, "recovery_a_10"));
        assert!(!names_marker(&text, "recovery_a_1"));
        assert!(names_marker("anyagents-message:m1", "m1"));
    }

    #[test]
    fn a_queue_report_admits_only_the_prompts_it_names() {
        let mut state = State::default();
        written(&mut state, "m1", 1);
        written(&mut state, "m2", 2);
        on_notification(
            &mut state,
            &json!({"method": "x.ai/queue/changed", "params": {
                "entries": [{"id": "q-1", "text": "anyagents-message:m1\nhello"}],
                "runningText": null
            }}),
        );
        assert_eq!(
            state.prompts["m1"].outcome(),
            Some(DeliverOutcome::Accepted {
                turn_finished: false
            })
        );
        assert_eq!(state.prompts["m2"].outcome(), None);
        assert_eq!(state.unconfirmed.len(), 1);
    }

    #[test]
    fn a_refusal_before_admission_hands_the_message_back_and_any_other_error_keeps_it() {
        let mut state = State::default();
        written(&mut state, "refused", 1);
        written(&mut state, "unknown", 2);
        written(&mut state, "admitted", 3);
        on_notification(
            &mut state,
            &answered(
                1,
                Some("The session is not ready yet. Send again in a moment."),
            ),
        );
        on_notification(&mut state, &answered(2, Some("internal error")));
        on_notification(
            &mut state,
            &json!({"method": "session/update", "params": {"update": {
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "text", "text":
                    "The user sent a message while you were working:\n<user_query>\nanyagents-message:admitted\nhi\n</user_query>"}
            }}}),
        );
        on_notification(
            &mut state,
            &answered(3, Some("prompt blocked: model unavailable")),
        );
        assert_eq!(
            written_outcome(&mut state, "refused"),
            Some(DeliverOutcome::Deferred)
        );
        assert!(
            !state.prompts.contains_key("refused"),
            "the next offer writes it again"
        );
        for taken in ["unknown", "admitted"] {
            assert_eq!(
                written_outcome(&mut state, taken),
                Some(DeliverOutcome::Accepted {
                    turn_finished: true
                }),
                "{taken}"
            );
        }
        assert_eq!(state.unanswered, 0);
        assert_eq!(state.turns_served, 1);
    }

    #[test]
    fn an_answer_that_beats_its_request_id_is_kept_for_it() {
        let mut state = State::default();
        on_notification(&mut state, &answered(7, None));
        assert!(state.early.contains_key(&7));
        on_notification(&mut state, &answered(8, None));
        written(&mut state, "m1", 7);
        let error = state.early.remove(&7).unwrap();
        answer(&mut state, "m1", 7, error);
        assert_eq!(
            written_outcome(&mut state, "m1"),
            Some(DeliverOutcome::Accepted {
                turn_finished: true
            })
        );
    }

    #[test]
    fn the_peer_is_what_initialize_names() {
        assert_eq!(
            announced_peer(&json!({"agentInfo": {"name": "grok-shell", "version": "1.0.67"}})),
            Some("grok-shell 1.0.67".to_string())
        );
        assert_eq!(announced_peer(&json!({})), None);
    }
}

#[cfg(all(test, unix))]
mod conformance {
    //! The delivery conformance suite (`anyagents_invariants::conformance`)
    //! against this adapter, over a scripted Grok (`fakes/proto_grok.py`).
    use std::path::Path;

    use anyagents_invariants::conformance::{Fixture, Subject};

    use super::GrokHarness;

    pub struct Grok;

    impl Subject for Grok {
        type Harness = GrokHarness;
        const HARNESS: &'static str = "grok";
        const PROTOCOL: &'static str = "grok";

        fn launch(fixture: &Fixture, session: Option<&str>) -> GrokHarness {
            GrokHarness::start(
                fixture.program(),
                None,
                session,
                &[],
                fixture.dir(),
                Path::new("/opt/bin/anyagents"),
                &["mcp".to_string()],
            )
            .expect("grok starts")
        }

        fn session(harness: &GrokHarness) -> Option<String> {
            harness.session_id()
        }
    }

    anyagents_invariants::conformance_suite!(Grok);
}
