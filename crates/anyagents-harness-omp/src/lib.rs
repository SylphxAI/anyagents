//! OMP adapter.
//!
//! One child runs `omp` and speaks one JSON object per line. The mailbox id is
//! the first line of the user text (`anyagents-message:<id>`). An id already
//! written, or echoed back, is not written again.
//!
//! The vendor's delivery mode has not been verified against a real OMP session.
//! This adapter declares followup and marks steer support unverified, which is
//! the honest reading until that session is run.
//!
//! The first real session has to confirm these field names before the adapter
//! is called working:
//! - a finished turn is a line whose `type` is `done`;
//! - that line's `session_id` is the session, and `usage.input_tokens`,
//!   `usage.output_tokens`, `usage.cache_read_tokens` and
//!   `usage.cache_write_tokens` are the token counts;
//! - a wait is a line whose `type` is `rate_limit` with `resets_at_ms` in epoch
//!   milliseconds.
//!
//! Resume starts the child again with `--session`. Cancel writes
//! `{"type":"cancel"}`.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use anyagents_harness::{DeliveryMode, SteerSupport, marked_message};
use anyagents_harness_stream::{Launch, LineEffect, StreamHarness};
use serde_json::{Value, json};

/// One long-lived OMP child. Its delivery mode is unverified.
pub struct OmpHarness {
    inner: StreamHarness,
}

impl OmpHarness {
    /// Start one child.
    ///
    /// `program` is the `omp` executable, or a test double. `session_id`, when
    /// set, is passed as `--session`.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, anyagents_harness::HarnessError> {
        let mut args = Vec::new();
        if let Some(id) = session_id {
            args.push("--session".to_string());
            args.push(id.to_string());
        }
        Ok(Self {
            inner: StreamHarness::launch(Launch {
                name: "anyagents-omp".to_string(),
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
}

impl anyagents_harness::Harness for OmpHarness {
    fn deliver(
        &mut self,
        message: &anyagents_harness::OutboundMessage,
    ) -> Result<anyagents_harness::DeliverOutcome, anyagents_harness::HarnessError> {
        let line = json!({
            "type": "user",
            "text": marked_message(&message.id, &message.text)
        });
        self.inner.deliver(message, &line)
    }

    fn background(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, session: &str) -> Result<(), anyagents_harness::HarnessError> {
        let mut command = Command::new(self.inner.program());
        command.arg("--session").arg(session);
        self.inner.resume(session, command)
    }

    fn cancel(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        self.inner.cancel(&json!({"type": "cancel"}))
    }

    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.inner.health()
    }
}

impl OmpHarness {
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

/// `done` closes the turn. The field names are the ones this adapter reads;
/// a real session has to confirm them.
fn owned_env(environment: &[(&str, &str)]) -> Vec<(String, String)> {
    environment
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn read_line(value: &Value) -> LineEffect {
    let is_done = value.get("type").and_then(Value::as_str) == Some("done");
    LineEffect {
        finishes_turn: is_done,
        session_id: value
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        usage: is_done.then(|| usage_of(value)),
        quota_reset_at_ms: quota_of(value),
        text: None,
    }
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
    if value.get("type").and_then(Value::as_str) != Some("rate_limit") {
        return None;
    }
    value.get("resets_at_ms").and_then(Value::as_i64)
}
