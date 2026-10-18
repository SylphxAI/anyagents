//! Adapter contract for a vendor harness, plus a scripted fake used by tests.
//!
//! A harness is admitted with whatever delivery mode it can actually do. The
//! mailbox and the delivery loop do not change between modes.

mod fake;

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

/// Five calls every harness adapter implements.
pub trait Harness: Send {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError>;
    fn background(&mut self) -> Result<(), HarnessError>;
    fn resume(&mut self, session: &str) -> Result<(), HarnessError>;
    fn cancel(&mut self) -> Result<(), HarnessError>;
    fn health(&self) -> HarnessHealth;
}
