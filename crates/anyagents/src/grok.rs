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
//! deduplicates nothing, so nothing is ever written to Grok twice.
//!
//! An error answer to a prompt Grok already showed admitted ends that turn:
//! the message was taken. An error answer before Grok showed it admitted is
//! parked as rejected and its sender told; it is never written again. For a
//! request the JSON-RPC layer could not take (`-32700`, `-32600`, `-32601`,
//! `-32602`) that is certain: Grok never ran it, and it would refuse the same
//! request again. For any other error it is unknown whether Grok applied it,
//! and since Grok deduplicates nothing a second write could apply it twice,
//! while counting it as taken could lose it without a word. Grok 1.0.67 has
//! no refusal a later offer clears: with `follow_up_behavior = "steer"` it
//! appends every prompt for its session to `pending_inputs`, and a prompt a
//! hook blocks is held at the front of that queue, not answered. A message
//! is written only once every earlier one is admitted or answered, so mail
//! keeps its order.
//!
//! These wire facts come from the 1.0.67 binary's strings, not from a live
//! session; `tests/invariants/fakes/proto_grok.py` marks which are inferred.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth,
    MESSAGE_MARKER_PREFIX, OutboundMessage, Proof, TurnState, compose_message, marked_message,
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
    proof: std::borrow::Cow::Borrowed(
        "marker in x.ai/queue/changed or user_message_chunk; session/prompt answer",
    ),
    peer: None,
};

/// The JSON-RPC errors raised for a request the server could not read
/// (parse error, invalid request, method not found, invalid params): the
/// handler never ran, and the same request is refused again.
const UNREAD_REQUEST: &[i64] = &[-32700, -32600, -32601, -32602];

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
    /// object, or `None` for a result.
    early: HashMap<i64, Option<Value>>,
    /// Written, and Grok has shown nothing about it yet.
    unconfirmed: HashSet<String>,
    /// Written and not yet answered.
    unanswered: usize,
    /// Answered with an error before Grok showed it admitted, and why: never
    /// written again, by this process or a later one of this adapter.
    rejected: HashMap<String, String>,
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
    /// Grok answered its request after admitting it: the turn that took it
    /// ended, with a result or an error.
    answered: bool,
}

impl Prompt {
    /// What Grok proved about it, or `None` while Grok has shown nothing.
    fn proof(&self) -> Option<Proof> {
        (self.admitted || self.answered).then_some(Proof::Taken {
            turn_finished: self.answered,
        })
    }
}

impl State {
    /// What Grok proved about `id`. A proof outlives the process.
    fn proven(&self, id: &str) -> Option<Proof> {
        self.prompts.get(id)?.proof()
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
            if let Some(known) = known_outcome(&st, id) {
                return Ok(known);
            }
            if !self.rpc.alive() {
                return Err(HarnessError::Dead);
            }
            if st.prompts.contains_key(id) {
                return Ok(DeliverOutcome::InFlight);
            }
            // Behind a message Grok has neither admitted nor answered nothing
            // is written, so mail reaches Grok in mailbox order. Its answer
            // or admission wakes the host to offer the next one.
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
                    answered: false,
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
                Ok(known_outcome(&st, id).unwrap_or(DeliverOutcome::InFlight))
            }
            // The line was not written: Grok has nothing to apply. The
            // process is judged by its output, which ends at EOF.
            Err(_) => {
                st.prompts.remove(id);
                st.unconfirmed.remove(id);
                st.unanswered -= 1;
                Ok(DeliverOutcome::Deferred)
            }
        }
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        lock(&self.shared).proven(message_id)
    }

    /// Stop the child and read what it already printed, to its end: every
    /// proof it gave is known to `proven` afterwards.
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

/// What the mailbox may record for a message this adapter wrote, once Grok
/// has shown something about it: a rejection, or a proof. Both stand after
/// the process exits.
fn known_outcome(state: &State, id: &str) -> Option<DeliverOutcome> {
    if let Some(reason) = state.rejected.get(id) {
        return Some(DeliverOutcome::Rejected {
            reason: reason.clone(),
        });
    }
    match state.proven(id)? {
        Proof::Taken { turn_finished } => Some(DeliverOutcome::Accepted { turn_finished }),
        Proof::Duplicate => Some(DeliverOutcome::Duplicate),
    }
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
                    .cloned();
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

/// Grok answered `request`, which carried `id`: `error` is its error object,
/// `None` for a result.
fn answer(state: &mut State, id: &str, request: i64, error: Option<Value>) {
    // A request is answered once.
    state.requests.remove(&request);
    let Some(prompt) = state.prompts.get_mut(id) else {
        return;
    };
    if prompt.request != Some(request) || prompt.answered {
        return;
    }
    let rejected = match error {
        Some(error) if !prompt.admitted => Some(rejection(&error)),
        // A result, or an error that ended a turn Grok had taken it into.
        _ => None,
    };
    match rejected {
        Some(reason) => {
            state.prompts.remove(id);
            state.rejected.insert(id.to_string(), reason);
        }
        None => {
            prompt.answered = true;
            state.serving = true;
        }
    }
    state.unconfirmed.remove(id);
    state.unanswered = state.unanswered.saturating_sub(1);
    if state.serving && state.unanswered == 0 {
        state.serving = false;
        state.turns_served += 1;
    }
}

/// Why a prompt Grok answered with `error` before showing it admitted is
/// parked. It is never written again: for a request the JSON-RPC layer
/// could not read, Grok never ran it and would refuse it again; for any
/// other error Grok may have applied it, and it deduplicates nothing.
fn rejection(error: &Value) -> String {
    let code = error.get("code").and_then(Value::as_i64);
    let mut what = match code {
        Some(code) => format!("{code} "),
        None => String::new(),
    };
    what.push_str(
        error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("error"),
    );
    match error.get("data") {
        None | Some(Value::Null) => {}
        Some(Value::String(data)) => what.push_str(&format!(" ({data})")),
        Some(data) => what.push_str(&format!(" ({data})")),
    }
    if code.is_some_and(|code| UNREAD_REQUEST.contains(&code)) {
        format!("grok refused the prompt before running it: {what}")
    } else {
        format!(
            "grok answered the prompt with {what} before showing it queued; it may or may \
             not have applied it and deduplicates nothing, so it is not written again"
        )
    }
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
                answered: false,
            },
        );
        state.requests.insert(request, id.to_string());
        state.unconfirmed.insert(id.to_string());
        state.unanswered += 1;
    }

    /// The answer to `request`: a result, or the error `{code, message}`.
    fn answered(request: i64, error: Option<(i64, &str)>) -> Value {
        json!({"method": "$rpc/response", "params": {
            "id": request,
            "result": if error.is_none() { json!({"stopReason": "end_turn"}) } else { Value::Null },
            "error": error.map_or(Value::Null, |(code, message)| json!({"code": code, "message": message})),
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
            state.proven("m1"),
            Some(Proof::Taken {
                turn_finished: false
            })
        );
        assert_eq!(state.proven("m2"), None);
        assert_eq!(state.unconfirmed.len(), 1);
    }

    #[test]
    fn an_error_before_admission_is_parked_and_one_after_it_ends_the_turn() {
        let mut state = State::default();
        written(&mut state, "unread", 1);
        written(&mut state, "unknown", 2);
        written(&mut state, "admitted", 3);
        // The texts this adapter once took for refusals are Grok's log and
        // TUI lines: an error carrying one is no different from any other.
        on_notification(&mut state, &answered(1, Some((-32602, "Invalid params"))));
        on_notification(
            &mut state,
            &answered(
                2,
                Some((
                    -32603,
                    "The session is not ready yet. Send again in a moment.",
                )),
            ),
        );
        on_notification(
            &mut state,
            &json!({"method": "session/update", "params": {"update": {
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "text", "text":
                    "The user sent a message while you were working:\n<user_query>\nanyagents-message:admitted\nhi\n</user_query>"}
            }}}),
        );
        on_notification(&mut state, &answered(3, Some((-32603, "Internal error"))));
        let reason = |id: &str| match known_outcome(&state, id) {
            Some(DeliverOutcome::Rejected { reason }) => reason,
            other => panic!("{id}: {other:?}"),
        };
        assert!(reason("unread").contains("before running it: -32602 Invalid params"));
        assert!(reason("unknown").contains("may or may not have applied it"));
        for parked in ["unread", "unknown"] {
            // Not a proof, and nothing left to write.
            assert_eq!(state.proven(parked), None, "{parked}");
            assert!(!state.prompts.contains_key(parked), "{parked}");
        }
        assert_eq!(
            known_outcome(&state, "admitted"),
            Some(DeliverOutcome::Accepted {
                turn_finished: true
            })
        );
        assert!(state.unconfirmed.is_empty());
        assert_eq!(state.unanswered, 0);
        assert_eq!(state.turns_served, 1);
    }

    #[test]
    fn a_rejection_names_the_error_and_its_data() {
        assert_eq!(
            rejection(
                &json!({"code": -32602, "message": "Invalid params", "data": "missing field `prompt`"})
            ),
            "grok refused the prompt before running it: -32602 Invalid params (missing field `prompt`)"
        );
        assert!(
            rejection(&json!({"code": -32000, "message": "Authentication required"}))
                .starts_with("grok answered the prompt with -32000 Authentication required before")
        );
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
            known_outcome(&state, "m1"),
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

#[cfg(all(test, unix))]
mod process {
    //! The adapter over a real child: the scripted Grok of the conformance
    //! suite (`fakes/proto_grok.py`), and a shell script where the order of
    //! what the child prints must be exact.
    use std::path::Path;
    use std::time::{Duration, Instant};

    use anyagents_harness::{
        Context, DeliverOutcome, Harness, HarnessError, OutboundMessage, Proof,
    };
    use anyagents_invariants::conformance::{World, ids};

    use super::GrokHarness;
    use super::conformance::Grok;

    fn outbound(id: &str, seq: i64) -> OutboundMessage {
        OutboundMessage {
            id: id.to_string(),
            seq,
            text: "hello".to_string(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        }
    }

    /// The scripted Grok answers the next prompt with `error` (a file the
    /// fake reads) while a turn runs: the message is parked, written once,
    /// never offered to Grok again, and the process is not restarted.
    fn a_prompt_answered_with(error: &str, expect: &str) {
        let mut world = World::<Grok>::new();
        let first = world.prompt();
        std::fs::write(world.fixture.dir().join(error), "").unwrap();
        let poison = world.receive(1).remove(0);
        world.pump_until("the prompt is parked", |w| w.cursor() == poison.seq);
        let rejections = world.delivery.take_rejections();
        assert_eq!(rejections.len(), 1, "{rejections:?}");
        assert_eq!(rejections[0].message_id, poison.id.as_str());
        assert!(
            rejections[0].reason.contains(expect),
            "{}",
            rejections[0].reason
        );
        // Offered again, it is answered the same, and nothing is written.
        let again = outbound(poison.id.as_str(), poison.seq);
        for _ in 0..3 {
            assert!(matches!(
                world.delivery.harness_mut().deliver(&again),
                Ok(DeliverOutcome::Rejected { .. })
            ));
        }
        world.pump_for(Duration::from_millis(200));
        // Mail behind it still flows.
        let next = world.receive(1).remove(0);
        world.drain(next.seq);
        let wire = world.fixture.wire();
        assert_eq!(
            wire.iter().filter(|id| **id == poison.id.as_str()).count(),
            1,
            "written again: {wire:?}"
        );
        let journal = ids(&world.fixture.journal());
        assert!(!journal.contains(&poison.id.to_string()), "{journal:?}");
        assert!(journal.contains(&first.id.to_string()), "{journal:?}");
        assert_eq!(
            journal.iter().filter(|id| **id == next.id.as_str()).count(),
            1,
            "{journal:?}"
        );
        assert!(world.delivery.take_rejections().is_empty());
        assert_eq!(world.fixture.launches(), 1, "restarted");
    }

    #[test]
    fn a_request_grok_cannot_read_is_parked_and_never_written_again() {
        a_prompt_answered_with("invalid-params", "before running it: -32602 Invalid params");
    }

    #[test]
    fn an_unclassified_error_before_admission_is_parked_not_resent_nor_restarted() {
        a_prompt_answered_with("internal-error", "may or may not have applied it");
    }

    /// A Grok that admits `m1` and prints nothing more until it is stopped.
    /// The script is the shell's input file, so nothing executes a file
    /// written by this test.
    const ADMITS_THEN_WAITS: &str = r#"IFS= read -r line
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentInfo":{"name":"grok-shell","version":"1.0.67"}}}'
IFS= read -r line
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"s1"}}'
IFS= read -r line
printf '%s\n' '{"jsonrpc":"2.0","method":"x.ai/queue/changed","params":{"sessionId":"s1","entries":[],"runningText":"anyagents-message:m1\nhello"}}'
: > printed
exec sleep 30
"#;

    #[test]
    fn a_proof_printed_just_before_stop_is_proven_after_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("agent"), ADMITS_THEN_WAITS).unwrap();
        // `sh agent --no-leader --always-approve stdio`, in the directory.
        let mut harness = GrokHarness::start(
            "/bin/sh",
            None,
            None,
            &[],
            dir.path(),
            Path::new("/opt/bin/anyagents"),
            &[],
        )
        .expect("the script starts");
        let m1 = outbound("m1", 1);
        assert!(matches!(
            harness.deliver(&m1),
            Ok(DeliverOutcome::InFlight | DeliverOutcome::Accepted { .. })
        ));
        // The proof is in the pipe; whether the reader took it yet is open.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !dir.path().join("printed").exists() {
            assert!(Instant::now() < deadline, "the script never admitted m1");
            std::thread::sleep(Duration::from_millis(5));
        }
        harness.stop();
        // Stopped means read to the end of its output.
        assert!(!harness.health().process_alive);
        assert_eq!(
            harness.proven("m1"),
            Some(Proof::Taken {
                turn_finished: false
            })
        );
        // A proof is answered before liveness; anything new finds it gone.
        assert_eq!(
            harness.deliver(&m1),
            Ok(DeliverOutcome::Accepted {
                turn_finished: false
            })
        );
        assert_eq!(harness.deliver(&outbound("m2", 2)), Err(HarnessError::Dead));
        assert_eq!(harness.proven("m2"), None);
    }
}
