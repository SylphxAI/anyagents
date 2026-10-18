//! OpenCode adapter.
//!
//! One child runs `opencode serve`. Messages are one JSON object per line. The
//! mailbox id is the first line of the user text (`anyagents-message:<id>`). An
//! id already written, or echoed back, is not written again.
//!
//! Declared delivery mode is followup: a message written while a turn is open
//! waits for the next `result`. Steer support is unproven. Token counts and a
//! reported cost, when the vendor sends one, are kept on the finished ids.
//!
//! Resume starts the child again with `--session` when the session id changed or
//! the process has exited. Cancel writes `{"type":"cancel"}`.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use anyagents_harness::{DeliveryMode, SteerSupport, marked_message};
use anyagents_harness_stream::{Launch, LineEffect, StreamHarness};
use serde_json::{Value, json};

/// One long-lived OpenCode `serve` child.
pub struct OpenCodeHarness {
    inner: StreamHarness,
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
        let mut args = vec!["serve".to_string()];
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
                work_dir: None,
                session_id: session_id.map(str::to_string),
                mode: DeliveryMode::Followup,
                steer: SteerSupport::Unproven,
                on_line: Arc::new(read_line),
            })?,
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
}

impl anyagents_harness::Harness for OpenCodeHarness {
    fn deliver(
        &mut self,
        message: &anyagents_harness::OutboundMessage,
    ) -> Result<anyagents_harness::DeliverOutcome, anyagents_harness::HarnessError> {
        let line = json!({
            "type": "prompt",
            "text": marked_message(&message.id, &message.text)
        });
        self.inner.deliver(message, &line)
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

    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.inner.health()
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
    }
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
