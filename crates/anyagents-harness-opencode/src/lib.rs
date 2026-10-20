//! OpenCode adapter.
//!
//! One child runs `opencode serve`. Messages are one JSON object per line. The
//! mailbox id is the first line of the user text (`anyagents-message:<id>`). An
//! id already written, or echoed back, is not written again.
//!
//! Declared delivery mode is followup: a message written while a turn is open
//! waits for the next `result`. Steer support is yes through the vendor's own
//! value on a prompt: `{"type":"prompt","text":...,"delivery":"steer"}` enters
//! the running turn instead of queueing behind it. Token counts and a reported
//! cost, when the vendor sends one, are kept on the finished ids.
//!
//! Resume starts the child again with `--session` when the session id changed or
//! the process has exited. Cancel writes `{"type":"cancel"}`.
//!
//! The answer a turn produced is the last `text` line after the prompt, up to
//! and including the `result` line. It is read with [`OpenCodeHarness::answer`]
//! and does not finish the turn.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, OutboundMessage, SteerItem, SteerQueue,
    SteerSupport, marked_message,
};
use anyagents_harness_stream::{Launch, LineEffect, StreamHarness};
use serde_json::{Value, json};

/// One long-lived OpenCode `serve` child.
pub struct OpenCodeHarness {
    inner: StreamHarness,
    steer: Option<SteerQueue>,
}

impl OpenCodeHarness {
    /// Start one child.
    ///
    /// `program` is the `opencode` executable, or a test double. `session_id`,
    /// when set, is passed as `--session`.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, anyagents_harness::HarnessError> {
        Self::start_in(program, session_id, environment, None)
    }

    /// Start one child with its working directory set to `work_dir`.
    pub fn start_in(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, anyagents_harness::HarnessError> {
        Self::start_double(program, None, session_id, environment, work_dir)
    }

    /// Start one child. `subcommand` replaces `serve` for a test double that
    /// takes its orders from a subcommand of the same executable.
    pub fn start_double(
        program: impl Into<PathBuf>,
        subcommand: Option<&str>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, anyagents_harness::HarnessError> {
        let mut args = vec![subcommand.unwrap_or("serve").to_string()];
        if let Some(id) = session_id {
            args.push("--session".to_string());
            args.push(id.to_string());
        }
        Ok(Self {
            inner: StreamHarness::launch(Launch {
                name: "anyagents-opencode".to_string(),
                program: program.into(),
                args,
                environment: owned_env(environment),
                work_dir: work_dir.map(Path::to_path_buf),
                session_id: session_id.map(str::to_string),
                mode: DeliveryMode::Followup,
                steer: SteerSupport::Unproven,
                on_line: Arc::new(read_line),
            })?,
            steer: None,
        })
    }

    /// Also watch `queue` for lines a channel pushed between model steps, and
    /// take them in through a steered prompt.
    pub fn with_steer(mut self, queue: SteerQueue) -> Self {
        self.steer = Some(queue);
        self
    }

    /// A prompt that enters the running turn.
    pub fn steer_prompt(message: &OutboundMessage) -> Value {
        json!({
            "type": "prompt",
            "text": marked_message(&message.id, &message.text),
            "delivery": "steer"
        })
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
    pub fn usage_for(&self, id: &str) -> Option<anyagents_harness::TurnUsage> {
        self.inner.usage_for(id)
    }
    pub fn seen(&self, id: &str) -> bool {
        self.inner.seen(id)
    }
    pub fn wait_seen(&self, id: &str, timeout: std::time::Duration) -> bool {
        self.inner.wait_seen(id, timeout)
    }
    /// Wait `quiet` with no line from the child. A quiet turn the child never
    /// closed is finished when the answer stands still.
    pub fn wait_quiet(
        &self,
        quiet: std::time::Duration,
        patience: std::time::Duration,
    ) -> bool {
        self.inner.quiet(quiet, patience)
    }
    /// The turn for `id` ended on quiet, not on a result line.
    pub fn closed_on_quiet(&self, id: &str) -> bool {
        self.inner.finished_quiet(id)
    }
    /// The answer text the vendor sent for `id`, when it has sent one.
    pub fn answer(&self, id: &str) -> Option<String> {
        self.inner.text_for(id)
    }
}

impl Harness for OpenCodeHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let line = json!({
            "type": "prompt",
            "text": marked_message(&message.id, &message.text)
        });
        self.inner.deliver(message, &line)
    }

    fn poll_steer(&mut self) -> Result<Option<SteerItem>, HarnessError> {
        let Some(queue) = &self.steer else {
            return Ok(None);
        };
        if !self.inner.health().process_alive {
            return Err(HarnessError::Dead);
        }
        // A line the child already echoes is not written a second time.
        while let Some(line) = queue.lines().into_iter().next() {
            if !self.inner.seen(&line.id) {
                break;
            }
            queue.consume(&line.id);
        }
        Ok(queue.take())
    }

    fn inject(&mut self, message: &OutboundMessage) -> Result<bool, HarnessError> {
        if self.steer.is_none() {
            return Ok(false);
        }
        if !self.inner.health().process_alive {
            return Err(HarnessError::Dead);
        }
        if self.inner.seen(&message.id) {
            return Ok(false);
        }
        // A prompt written while the turn is open is taken by that turn; the
        // queue line is the copy that was pushed, not a turn of its own.
        let outcome = self.inner.deliver(message, &Self::steer_prompt(message))?;
        Ok(matches!(outcome, DeliverOutcome::Accepted { .. }))
    }

    fn release_steer(&mut self, keep: &str) {
        if let Some(queue) = &self.steer {
            queue.release_for(keep);
        }
    }

    fn background(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, session: &str) -> Result<(), anyagents_harness::HarnessError> {
        let mut command = Command::new(self.inner.program());
        command.arg("serve").arg("--session").arg(session);
        self.inner.resume(session, command)
    }

    fn cancel(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        self.inner.cancel(&json!({"type": "cancel"}))
    }

    fn answer_for(&self, id: &str) -> Option<String> {
        self.answer(id)
    }

    fn ends_on_quiet(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_millis(500))
    }

    fn quiet_for(&self, _id: &str, quiet: std::time::Duration, patience: std::time::Duration) -> bool {
        self.wait_quiet(quiet, patience)
    }

    fn health(&self) -> anyagents_harness::HarnessHealth {
        let mut health = self.inner.health();
        if self.steer.is_some() {
            health.steer_support = SteerSupport::Yes;
        }
        health
    }
}

fn owned_env(environment: &[(&str, &str)]) -> Vec<(String, String)> {
    environment
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn read_line(value: &Value) -> LineEffect {
    let is_result = value.get("type").and_then(Value::as_str) == Some("result");
    LineEffect {
        finishes_turn: is_result,
        session_id: value
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        usage: is_result.then(|| usage_of(value)),
        quota_reset_at_ms: quota_of(value),
        text: answer_of(value, is_result),
    }
}

/// A text line, or the text on the `result` line itself, is the answer so far.
fn answer_of(value: &Value, is_result: bool) -> Option<String> {
    if !(is_result || value.get("type").and_then(Value::as_str) == Some("text")) {
        return None;
    }
    value
        .get("text")
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
        reported_cost_micros: value.get("cost_micros").and_then(Value::as_i64),
    }
}

fn number(value: Option<&Value>, key: &str) -> u64 {
    value
        .and_then(|item| item.get(key))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn quota_of(value: &Value) -> Option<i64> {
    if value.get("type").and_then(Value::as_str) != Some("rate_limit") {
        return None;
    }
    value.get("resets_at_ms").and_then(Value::as_i64)
}
