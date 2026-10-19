//! Adapter contract for a vendor harness, plus a scripted fake used by tests.
//!
//! A harness is admitted with whatever delivery mode it can actually do. The
//! mailbox and the delivery loop do not change between modes. Adapters carry
//! the mailbox id on the first line of the user text as `anyagents-message:<id>`.

mod fake;

use std::time::Duration;

pub use fake::FakeHarness;

/// How a harness takes input that arrives during a turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryMode {
    /// Input enters the active turn, including while a tool runs.
    Steer,
    /// The tool takes the input now and acts on it when the current turn ends.
    Followup,
    /// The tool cannot take input during a turn. Deliver it when the turn ends.
    Queue,
}

/// Whether this harness has shown it can steer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SteerSupport {
    Yes,
    No,
    Unproven,
}

/// Whether a turn is open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnState {
    Idle,
    InTurn,
}

/// What `health` reports. Quota and sign-in trouble are states, not crashes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessHealth {
    pub process_alive: bool,
    pub turn_state: TurnState,
    pub delivery_mode: DeliveryMode,
    pub steer_support: SteerSupport,
    pub quota_reset_at_ms: Option<i64>,
    pub sign_in_fault: Option<String>,
}

/// One mailbox message handed to a harness. The id is what a replay is deduped on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundMessage {
    pub id: String,
    pub seq: i64,
    pub text: String,
    pub sender: Option<String>,
    pub reply_to: Option<String>,
    pub source_time_ms: Option<i64>,
}

/// Result of one `deliver` call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliverOutcome {
    /// The harness has the message. `turn_finished` means the turn that
    /// contained it has already completed.
    Accepted { turn_finished: bool },
    /// Queue mode: a turn is open, so the message stays in the mailbox.
    Deferred,
    /// The vendor asked us to wait. Not a failure and not a restart.
    Quota { reset_at_ms: i64 },
}

/// The harness process could not take the message.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HarnessError {
    #[error("harness process is not running")]
    Dead,
    #[error("{0}")]
    Failed(String),
}

/// Calls every harness adapter implements. The last four have defaults so a
/// scripted fake can stay a delivery double without a process.
pub trait Harness: Send {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError>;
    fn background(&mut self) -> Result<(), HarnessError>;
    fn resume(&mut self, session: &str) -> Result<(), HarnessError>;
    fn cancel(&mut self) -> Result<(), HarnessError>;
    fn health(&self) -> HarnessHealth;

    /// Answer text the vendor sent for `id`, when it sends text before a result.
    fn answer_for(&self, _id: &str) -> Option<String> {
        None
    }

    /// How long a turn must stay silent before its answer stands still, for a
    /// vendor that streams text and never closes a turn with a result line.
    /// `None` means the vendor always closes its turn.
    fn ends_on_quiet(&self) -> Option<Duration> {
        None
    }

    /// Wait until the turn is silent long enough to stand still, giving up
    /// after `patience`.
    fn quiet_for(&self, _id: &str, _quiet: Duration, _patience: Duration) -> bool {
        false
    }

    /// Usage recorded for a finished turn, if this adapter kept it.
    fn usage_for(&self, _id: &str) -> Option<TurnUsage> {
        None
    }

    /// Which finished turn this is for `id`, counting from 1. A message that
    /// fails and is handed over again reports 2, so the host can tell the retry
    /// from the failure it already recorded. `None` means the adapter does not
    /// count, and every report is treated as the same turn.
    fn turn_version(&self, _id: &str) -> u64 {
        0
    }

    /// How the finished turn that included `id` ended. `None` means the adapter
    /// cannot tell, and the turn counts as a success. An adapter that reports a
    /// failure keeps the message accepted, never processed.
    fn turn_report_for(&self, id: &str) -> Option<TurnReport> {
        self.usage_for(id).map(TurnReport::Succeeded)
    }

    /// Forget a quota wait so delivery can resume after the reset time.
    fn clear_quota(&mut self) {}

    /// Operating system id of the vendor process, when it has one.
    fn process_id(&self) -> Option<u32> {
        None
    }

    /// Call `notify` after the vendor process reports activity or exits.
    ///
    /// The callback runs on a background thread. It must not call back into
    /// this harness.
    fn watch_activity(&self, _notify: std::sync::Arc<dyn Fn() + Send + Sync>) {}
}

/// Token and cost totals one vendor turn reported.
///
/// Cost is in millionths of the vendor's currency unit when the vendor reports
/// a cost. A vendor that reports only tokens leaves the cost empty.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub reported_cost_micros: Option<i64>,
}

/// How a finished turn ended, as the vendor reported it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnReport {
    /// The turn completed. Its usage is recorded.
    Succeeded(TurnUsage),
    /// The turn ended in an error. The message is not processed and is offered
    /// to the next turn. `reason` comes from the vendor's own error text.
    Failed(String),
}

/// First line of user text. The rest of the text is the mailbox body.
pub const MESSAGE_MARKER_PREFIX: &str = "anyagents-message:";

/// User text whose first line is the mailbox id.
pub fn marked_message(id: &str, text: &str) -> String {
    format!("{MESSAGE_MARKER_PREFIX}{id}\n{text}")
}

/// Mailbox id when `text` starts with the marker line.
pub fn message_id_in_text(text: &str) -> Option<&str> {
    let id = text.lines().next()?.strip_prefix(MESSAGE_MARKER_PREFIX)?;
    if id.is_empty() { None } else { Some(id) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_is_only_the_first_line() {
        let text = marked_message("msg_1", "hello\nanyagents-message:other");
        assert_eq!(message_id_in_text(&text), Some("msg_1"));
        assert_eq!(message_id_in_text("hello"), None);
        assert_eq!(message_id_in_text("anyagents-message:"), None);
    }
}
