//! Antigravity (agy) adapter.
//!
//! One child runs `agy -p --input-format stream-json --output-format
//! stream-json`. The CLI reads one NDJSON message per line on stdin and runs a
//! turn for each, so a message written while a turn is open waits for that
//! turn's `result` line and is then read as the next turn.
//!
//! Declared delivery mode is followup. Steer support is yes through the
//! vendor's own before-step hook: agy runs a configured `PreInvocation` hook
//! before every model call and takes `{"injectSteps": [{"userMessage": ...}]}`
//! as input for that call. [`AgyHarness::with_hook`] gives this agent one; it
//! serves a [`SteerQueue`] in a fresh directory each time the child starts, and
//! the process that starts the agent points the hook at it with
//! [`HOOK_DIR_ENV`] (the hook program is `anyagents-steer-hook`). A `result`
//! event closes the open turn and keeps the token counts it carries; the hook
//! takes each line once the child has read it.
//!
//! With no hook (no steered delivery), a message written while a turn is open
//! is held for the next turn.
//!
//! Resume starts the child again with `--conversation <id>`. Cancel writes
//! `{"type":"interrupt"}`.
//!
//! The hook program `anyagents-steer-hook` answers `{}` for anything that is
//! not this agent's hook directory, so it never disturbs another agent.

use std::collections::HashSet;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, OutboundMessage, SteerItem, SteerQueue,
    SteerSupport, marked_message, message_id_in_text,
};
use anyagents_harness_stream::{Launch, LineEffect, StreamHarness};
use serde_json::{Value, json};

/// Environment variable that names the hook directory this agent's child reads.
pub const HOOK_DIR_ENV: &str = "ANYAGENTS_AGY_HOOK_DIR";

/// One long-lived Antigravity child.
pub struct AgyHarness {
    inner: StreamHarness,
    /// The config directory whose `hooks/pre_invocation` runs before every
    /// model call.
    root: PathBuf,
    hook: Option<PathBuf>,
    steer: Option<SteerQueue>,
}

impl AgyHarness {
    /// Start one child.
    ///
    /// `program` is the `agy` executable, or a test double. `session_id`, when
    /// set, is passed as `--conversation`. `environment` overrides those
    /// variables in the child; pass an empty slice for a normal launch.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, anyagents_harness::HarnessError> {
        let args = launch_args(session_id);
        let environment = owned_env(environment);
        let program = program.into();
        Ok(Self {
            inner: StreamHarness::launch(Launch {
                name: "anyagents-agy".to_string(),
                program,
                args,
                environment,
                work_dir: None,
                session_id: session_id.map(str::to_string),
                mode: DeliveryMode::Followup,
                steer: SteerSupport::Unproven,
                on_line: Arc::new(read_line),
            })?,
            root: PathBuf::new(),
            hook: None,
            steer: None,
        })
    }

    /// Take input between model steps through an `agy` `PreInvocation` hook
    /// installed under `root`/hook. `root` is the agent's config directory; each
    /// start gets a fresh hook directory, so a line is never injected twice.
    pub fn with_hook(mut self, root: PathBuf) -> Result<Self, anyagents_harness::HarnessError> {
        let hook = fresh_dir(&root, "agy-steer");
        let steer = SteerQueue::new(hook.join("steer.jsonl"), HOOK_BATCH);
        write_settings(&root, &hook)?;
        self.root = root;
        self.hook = Some(hook);
        self.steer = Some(steer);
        Ok(self)
    }

    /// Name this agent's hook directory to the child and to every process it
    /// launches, so its before-step hook finds the queue.
    fn point_hook(&mut self) {
        if let Some(hook) = self.hook.as_ref().map(|hook| hook.display().to_string()) {
            self.inner.set_env(HOOK_DIR_ENV, &hook);
        }
    }

    /// The hook directory the running child reads, when steering is on.
    pub fn hook_dir(&self) -> Option<&Path> {
        self.hook.as_deref()
    }

    /// The queue a before-step hook fills.
    pub fn steer_queue(&self) -> Option<&SteerQueue> {
        self.steer.as_ref()
    }

    pub fn written_ids(&self) -> Vec<String> {
        self.inner.written_ids()
    }
    pub fn turns_started(&self) -> usize {
        self.inner.turns_started()
    }
    pub fn session_id(&self) -> Option<String> {
        self.inner.session_id()
    }
    pub fn wait_finished(&self, id: &str, timeout: std::time::Duration) -> bool {
        self.inner.wait_finished(id, timeout)
    }
    pub fn wait_quota(&self, timeout: std::time::Duration) -> Option<i64> {
        self.inner.wait_quota(timeout)
    }
    pub fn seen(&self, id: &str) -> bool {
        self.inner.seen(id)
    }
    pub fn wait_seen(&self, id: &str, timeout: std::time::Duration) -> bool {
        self.inner.wait_seen(id, timeout)
    }
    pub fn usage_for(&self, id: &str) -> Option<anyagents_harness::TurnUsage> {
        self.inner.usage_for(id)
    }
    /// The highest mailbox id this child has written or echoed back. The hook
    /// hands over what comes after it.
    pub fn newest_id(&self) -> Option<String> {
        self.inner.highest_id()
    }
}

impl Harness for AgyHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        // The child reads a whole turn of input between model calls. With the
        // hook on, the line waits for the next step instead of opening a turn
        // the vendor cannot take while it works.
        if self.steer.is_some() && self.inner.health().turn_state == anyagents_harness::TurnState::InTurn {
            return Ok(DeliverOutcome::Deferred);
        }
        // The hook runs during the turn the next write starts, so the child's
        // hook directory is named before that write.
        self.point_hook();
        let line = json!({
            "event": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": marked_message(&message.id, &message.text)}]
            }
        });
        self.inner.deliver(message, &line)
    }

    fn poll_steer(&mut self) -> Result<Option<SteerItem>, HarnessError> {
        if self.steer.is_none() {
            return Ok(None);
        }
        if !self.inner.health().process_alive {
            return Err(HarnessError::Dead);
        }
        self.point_hook();
        let Some(queue) = &self.steer else {
            return Ok(None);
        };
        // A line the child already read is acknowledged by the hook and gone
        // from the queue; one whose turn finished here is not injected again.
        while let Some(line) = queue.lines().into_iter().next() {
            if !self.inner.seen(&line.id) || self.inner.wait_finished(&line.id, std::time::Duration::ZERO) {
                break;
            }
            queue.consume(&line.id);
        }
        Ok(queue.take())
    }

    fn inject(&mut self, message: &OutboundMessage) -> Result<bool, HarnessError> {
        if !self.inner.health().process_alive {
            return Err(HarnessError::Dead);
        }
        if self.inner.seen(&message.id) {
            return Ok(false);
        }
        if self.steer.is_none() {
            return Ok(false);
        }
        self.point_hook();
        let Some(queue) = &self.steer else {
            return Ok(false);
        };
        // The host writes the line for the next step; the hook reads it in order.
        debug_log(&format!("queue lines {:?}", queue.lines()));
        let mut lines = queue.lines();
        if lines.iter().any(|line| line.id == message.id) {
            return Ok(true);
        }
        lines.push(anyagents_harness::SteerLine {
            id: message.id.clone(),
            seq: message.seq,
            message: message.text.clone(),
        });
        let pushed = lines
            .into_iter()
            .filter_map(|line| serde_json::to_string(&line).ok())
            .map(|line| line + "\n")
            .collect::<String>();
        std::fs::write(queue.path(), pushed)
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        debug_log(&format!("inject {} into {}", message.id, queue.path().display()));
        Ok(true)
    }

    fn release_steer(&mut self, keep: &str) {
        if let Some(queue) = &self.steer {
            queue.release_for(keep);
        }
    }

    fn answer_for(&self, _id: &str) -> Option<String> {
        None
    }

    fn background(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, session: &str) -> Result<(), anyagents_harness::HarnessError> {
        self.point_hook();
        let mut command = Command::new(self.inner.program());
        command.args(launch_args(Some(session)));
        self.inner.resume(session, command)
    }

    fn cancel(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        self.inner.cancel(&json!({"type": "interrupt"}))
    }

    fn health(&self) -> anyagents_harness::HarnessHealth {
        let mut health = self.inner.health();
        if self.steer.is_some() {
            health.steer_support = SteerSupport::Yes;
        }
        health
    }

    fn usage_for(&self, id: &str) -> Option<anyagents_harness::TurnUsage> {
        self.inner.usage_for(id)
    }

    fn clear_quota(&mut self) {
        self.inner.clear_quota();
    }

    fn process_id(&self) -> Option<u32> {
        self.inner.process_id()
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.inner.watch_activity(notify)
    }

    /// A `result` that reports an error, or an `is_error` flag, is a failed
    /// turn: the message stays accepted and its reason is the vendor's own text.
    fn turn_report_for(&self, id: &str) -> Option<anyagents_harness::TurnReport> {
        if !self.inner.finished(id) {
            return None;
        }
        match failed_result(&self.inner.result_for(id).unwrap_or(Value::Null)) {
            Some(reason) => Some(anyagents_harness::TurnReport::Failed(reason)),
            None => self
                .inner
                .usage_for(id)
                .map(anyagents_harness::TurnReport::Succeeded),
        }
    }
}

/// The reason a `result` line reports a failure, when it does.
/// The vendor's own reason for a failed turn, from the result frame's body.
pub fn failed_result(value: &Value) -> Option<String> {
    let body = value.get("result").filter(|body| body.is_object());
    let body = body.unwrap_or(value);
    let status = body.get("status").and_then(Value::as_str).unwrap_or("");
    if status.is_empty() || status == "SUCCESS" {
        return None;
    }
    let reason = body.get("error").and_then(Value::as_str).unwrap_or("");
    if reason.is_empty() {
        return Some(status.to_string());
    }
    Some(reason.to_string())
}

/// The CLI's print mode, reading and writing stream-json.
///
/// `--print` is left out on purpose: the CLI takes the next word after
/// `--print` as its prompt, so `--print --input-format stream-json` is read as
/// a prompt of `--input-format` (read from the live CLI on 2026-10-20). Stream
/// input mode is print mode, so the two flags are not needed together.
fn launch_args(session_id: Option<&str>) -> Vec<String> {
    let mut args = vec![
        "--input-format".to_string(),
        "stream-json".to_string(),
        "--output-format".to_string(),
        "stream-json".to_string(),
    ];
    if let Some(id) = session_id {
        args.push("--conversation".to_string());
        args.push(id.to_string());
    }
    args
}

/// Append a diagnostic line when `ANYAGENTS_AGY_DEBUG` names a file.
fn debug_log(line: &str) {
    let Some(path) = std::env::var_os("ANYAGENTS_AGY_DEBUG") else {
        return;
    };
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        use std::io::Write;
        let _ = writeln!(file, "{line}");
    }
}

/// Name the hook in the agent's own config directory, as `agy` reads it:
/// `.agents/hooks.json` in the project, `~/.gemini/config/hooks.json` for the
/// user. An existing `PreInvocation` command for this agent is kept; the hook
/// is appended, and one this adapter wrote before is replaced.
pub fn write_settings(root: &Path, hook: &Path) -> Result<(), HarnessError> {
    // `hook` is the script this agent's client wrote, and it runs this client
    // for this agent. A client that wrote none falls back to the sibling
    // program, so a hook is always registered.
    let program = if hook.is_file() {
        hook.to_path_buf()
    } else {
        std::env::current_exe()
            .ok()
            .and_then(|exe| {
                let sibling = exe.with_file_name("anyagents-steer-hook");
                sibling.exists().then_some(sibling)
            })
            .unwrap_or_else(|| PathBuf::from("anyagents-steer-hook"))
    };
    let command = json!({
        "command": program.display().to_string(),
        "timeoutMs": 15000,
        "env": {HOOK_DIR_ENV: hook.display().to_string()}
    });
    for file in [root.join(".agents").join("hooks.json"), root.join("hooks.json")] {
        let mut settings: Value = std::fs::read(&file)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_else(|| json!({}));
        let Some(root_object) = settings.as_object_mut() else {
            settings = json!({});
            continue;
        };
        let hooks = root_object
            .entry("hooks")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| HarnessError::Failed("hooks is not an object".into()))?;
        let list = hooks
            .entry("PreInvocation")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| HarnessError::Failed("PreInvocation is not a list".into()))?;
        list.retain(|entry| !is_our_hook(entry));
        list.push(json!({"hooks": [command]}));
        if let Some(parent) = file.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let bytes = serde_json::to_vec_pretty(&settings)
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        std::fs::write(&file, bytes).map_err(|err| HarnessError::Failed(err.to_string()))?;
    }
    Ok(())
}

/// A `PreInvocation` entry this adapter wrote.
fn is_our_hook(entry: &Value) -> bool {
    let hooks = match entry.get("hooks").and_then(Value::as_array) {
        Some(hooks) => hooks,
        None => return false,
    };
    // Both names are this client's: the sibling program, and the per-agent
    // script that runs it.
    hooks.iter().any(|hook| {
        hook.get("command").and_then(Value::as_str).is_some_and(|command| {
            command.ends_with("anyagents-steer-hook") || command.ends_with("agy-steer-hook")
        })
    })
}

/// How many pushed lines the hook adds to one model call.
const HOOK_BATCH: usize = 40;

/// The line handed to the hook, with its mailbox id first so the child's echo
/// names it.
pub fn hook_user_text(lines: &[anyagents_harness::SteerLine]) -> String {
    let mut text = String::new();
    for line in lines {
        text.push_str(&marked_message(&line.id, &line.message));
        text.push_str("\n\n");
    }
    text.trim_end().to_string()
}

/// Mailbox ids a hook answer added, in order.
pub fn injected_ids(answer: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    let Some(steps) = answer.get("injectSteps").and_then(Value::as_array) else {
        return ids;
    };
    for step in steps {
        if let Some(text) = step.get("userMessage").and_then(Value::as_str) {
            for chunk in text.split("\n\n") {
                if let Some(id) = message_id_in_text(chunk) {
                    ids.push(id.to_string());
                }
            }
        }
    }
    ids
}

/// A new, empty directory under `root`, so no line is injected by two children.
fn fresh_dir(root: &Path, name: &str) -> PathBuf {
    let base = root.join(name);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let dir = base.join(format!("{}-{stamp}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn owned_env(environment: &[(&str, &str)]) -> Vec<(String, String)> {
    // The hook variable is this adapter's own and is set only for an agent
    // that has a hook.
    environment
        .iter()
        .filter(|(key, _)| *key != HOOK_DIR_ENV)
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

/// One line of the CLI's stream. It announces each frame with `event`; the
/// frame's own body is under the same name, so `{"event":"result","result":{}}`.
fn read_line(value: &Value) -> LineEffect {
    let event = value.get("event").and_then(Value::as_str);
    let body = event
        .and_then(|name| value.get(name))
        .filter(|body| body.is_object())
        .unwrap_or(value);
    let is_result = event == Some("result");
    LineEffect {
        finishes_turn: is_result,
        session_id: body
            .get("conversation_id")
            .or_else(|| value.get("conversation_id"))
            .and_then(Value::as_str)
            .map(str::to_string),
        usage: is_result.then(|| usage_of(body)),
        // Checked on the whole line: the event name lives at the top.
        quota_reset_at_ms: quota_of(value),
        // A step's streamed text is the answer so far; the result line repeats
        // the whole answer when it closes.
        text: answer_of(value, body, is_result),
    }
}

/// The answer so far: a step's streamed text, or the result's whole response.
fn answer_of(value: &Value, body: &Value, is_result: bool) -> Option<String> {
    // A step streams its text as it goes, whether or not it has finished.
    if value.get("event").and_then(Value::as_str) == Some("step_update") {
        return body
            .get("text_delta")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string);
    }
    if !is_result {
        return value
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string);
    }
    body.get("response")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn usage_of(value: &Value) -> anyagents_harness::TurnUsage {
    let usage = value.get("usage");
    anyagents_harness::TurnUsage {
        input_tokens: number(usage, "input_tokens"),
        output_tokens: number(usage, "output_tokens"),
        cache_read_tokens: number(usage, "cache_read_tokens"),
        cache_write_tokens: number(usage, "cache_write_tokens"),
        reported_cost_micros: None,
    }
}

fn number(value: Option<&Value>, key: &str) -> u64 {
    value
        .and_then(|item| item.get(key))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn quota_of(value: &Value) -> Option<i64> {
    if value.get("event").and_then(Value::as_str) != Some("rate_limit") {
        return None;
    }
    let body = value.get("rate_limit").filter(|body| body.is_object());
    body.unwrap_or(value).get("resets_at_ms").and_then(Value::as_i64)
}

/// One line of the hook's input file: the mailbox message, as the server sent it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PendingLine {
    pub id: String,
    pub seq: i64,
    pub text: String,
}

/// The hook's body: the unread mailbox lines after `since`, in sequence order,
/// unfiltered, answered as the `injectSteps` that the CLI adds before its next
/// model call.
///
/// `since` is the sequence of the newest message the adapter has already
/// written or seen, or `None` for a process that has seen nothing: every line
/// after it entered the CLI through the hook, and is injected here. A line whose
/// id is in `delivered` was written by the server, which is exploring past the
/// floor while a message it wrote is not accepted yet; it is never injected.
/// Lines are taken oldest first and the rest follows on the next call.
pub fn hook_answer(pending: &[PendingLine], since: Option<i64>, delivered: &HashSet<String>, limit: usize) -> Value {
    let taken: Vec<&PendingLine> = pending
        .iter()
        .filter(|line| since.is_none_or(|floor| line.seq > floor))
        .filter(|line| !delivered.contains(&line.id))
        .take(limit)
        .collect();
    if taken.is_empty() {
        return json!({});
    }
    let mut used = String::from("New messages arrived while you were working (delivered mid-turn):");
    for line in taken {
        used.push_str("\n\n");
        used.push_str(&marked_message(&line.id, &line.text));
    }
    json!({"injectSteps": [{"userMessage": used}]})
}

/// Read the hook's event on stdin and answer on stdout. `session` is the
/// conversation this agent runs, empty for no check. The event's `conversationId`
/// must match it, and the turn's own first call already carries its prompt.
pub fn run_hook<R: BufRead, W: Write>(
    input: R,
    out: &mut W,
    session: &str,
    pending: impl FnOnce() -> Vec<PendingLine>,
    since: Option<i64>,
    delivered: &HashSet<String>,
    limit: usize,
) {
    let mut event = String::new();
    let mut reader = input;
    let _ = reader.read_to_string(&mut event);
    let event: Value = serde_json::from_str(&event).unwrap_or(Value::Null);
    if event.get("invocationNum").and_then(Value::as_i64).unwrap_or(0) == 0 {
        let _ = writeln!(out, "{{}}");
        return;
    }
    if !session.is_empty()
        && event
            .get("conversationId")
            .and_then(Value::as_str)
            .is_some_and(|id| id != session)
    {
        let _ = writeln!(out, "{{}}");
        return;
    }
    let answer = hook_answer(&pending(), since, delivered, limit);
    let _ = writeln!(out, "{answer}");
    let _ = out.flush();
}


