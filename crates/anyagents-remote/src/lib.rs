//! The remote-session executor (ADR 0010 D6, D7): an agent that runs as a
//! vendor's remote session has no local process. The kernel holds one async
//! connection per such agent, on one shared runtime, and drives it through one
//! vendor-neutral contract, [`Adapter`]:
//!
//! * `create` a session, or reconnect to the one the store names;
//! * `deliver` mail into it under its mailbox id (`request_id`), which steers
//!   a running turn when the vendor takes input mid-turn;
//! * `attach` to its events from a cursor: every event after the cursor is
//!   replayed, then [`EventKind::CaughtUp`], then live events. Spiron's
//!   `spiron.headless/v2` resumes by `seq` (`attach {after_seq}`, ADR 0011
//!   D4); Claude Managed Agents by reopening its SSE stream, reading the
//!   session's history and deduplicating by event id;
//! * `interrupt` the running turn, `stop` the session, set its `budget`;
//! * answer host tool calls that arrive as events (ADR 0011 D7), and hand a
//!   session that calls back over MCP a fresh `grant` when its token rotates.
//!
//! Each adapter declares what it can do as [`Capabilities`], the way a local
//! harness declares its delivery level: the delivery level and its proof,
//! whether a turn can be interrupted, whether the vendor or the kernel holds
//! the spend cap, and how the session reaches the coordination tools.
//!
//! [`Executor`] is the kernel side. It implements the harness contract the
//! supervisor and delivery loop already drive, so a remote agent is the third
//! executor kind beside a local process and a keeper. Its lifecycle, cursor
//! and accounting live in the `remote_executions` row, so it survives a server
//! restart by reattaching from the cursor instead of creating a session.

use std::future::Future;
use std::pin::Pin;

use anyagents_harness::{Delivery, TurnUsage};
use futures::Stream;
use serde_json::Value;

pub mod claude_managed;
mod executor;
#[cfg(feature = "mock")]
pub mod mock;
pub mod pacing;
mod sse;

pub use executor::{
    Config, Executor, HostTools, Settled, attached, block_on, change_budget, end_before_delete,
    settle, spawn_blocking, take_over,
};

/// What an adapter call returns.
pub type Fut<'a, T> = Pin<Box<dyn Future<Output = Result<T, AdapterError>> + Send + 'a>>;
/// One attachment's events: the replay after the cursor, then
/// [`EventKind::CaughtUp`], then live events, in the vendor's order.
pub type Events = Pin<Box<dyn Stream<Item = Result<Event, AdapterError>> + Send>>;

/// What one adapter can do, declared once, like a local harness's delivery
/// level (`docs/delivery.md`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capabilities {
    /// The adapter's name, as `agents list` shows it (`claude-managed`).
    pub adapter: &'static str,
    /// The level mail reaches a running session at, and what proves it:
    /// `mid-turn` adapters answer [`Admission::Accepted`]; `next-step`
    /// adapters answer [`Admission::InFlight`] and later prove each input
    /// with [`EventKind::Consumed`].
    pub delivery: Delivery,
    /// The vendor stops a running turn without ending the session.
    pub interrupt: bool,
    /// Who holds the spend cap.
    pub budget: Budgeting,
    /// The vendor bills per use: such an agent never runs without a cap.
    pub metered: bool,
    /// How the session reaches the AnyAgents coordination tools.
    pub coordination: Coordination,
}

/// Who holds a remote agent to its spend cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Budgeting {
    /// The vendor enforces a hard cap per session and pauses the session at
    /// it ([`Stop::BudgetReached`]); raising the cap resumes it.
    Vendor,
    /// The vendor reports cost ([`EventKind::Usage`], [`EventKind::CostTotal`])
    /// and the kernel holds the cap: at it, the executor interrupts the
    /// running turn and holds the agent's mail (ADR 0011 D8).
    Kernel,
}

/// How a remote session reaches the coordination tools.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coordination {
    /// It calls back to this server's public MCP endpoint (`public_url`) with
    /// a short-lived remote session token ([`Grant`]).
    Mcp,
    /// Its tool calls arrive as [`EventKind::HostToolCall`] on the connection
    /// the kernel already holds, and are answered with
    /// [`Adapter::tool_result`] (ADR 0011 D7).
    HostTools,
}

/// The coordination MCP grant a remote session calls back with: the public
/// MCP URL and a short-lived remote session token. The token is never logged.
#[derive(Clone, PartialEq, Eq)]
pub struct Grant {
    pub mcp_url: String,
    pub token: String,
}

impl std::fmt::Debug for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grant")
            .field("mcp_url", &self.mcp_url)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// A tool the host serves over the connection (ADR 0011 D7).
#[derive(Clone, Debug, PartialEq)]
pub struct HostTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// What a new session is created with.
#[derive(Clone, Debug)]
pub struct Spec {
    pub agent_id: String,
    pub agent_name: String,
    pub model: Option<String>,
    pub system: Option<String>,
    /// For [`Coordination::Mcp`].
    pub grant: Option<Grant>,
    /// For [`Coordination::HostTools`].
    pub host_tools: Vec<HostTool>,
    /// The spend cap, in US cents.
    pub budget_cents: Option<i64>,
}

/// One vendor session: its id and the adapter's own handle on it (vendor
/// agent, environment, vault ids, an attach URL). Kept in the store; never a
/// secret.
#[derive(Clone, Debug, PartialEq)]
pub struct Handle {
    pub session: String,
    pub state: Value,
}

/// Why a turn ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stop {
    /// It finished, or was interrupted.
    EndTurn,
    /// The session waits for a decision only a client can give.
    RequiresAction,
    /// The session paused at its spend cap; the turn resumes when it rises.
    BudgetReached,
    /// It ended in an error.
    Failed(String),
}

/// The vendor's own state of a session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VendorStatus {
    Running,
    Idle,
    Rescheduling,
    Terminated,
}

/// A session as the vendor reports it now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub status: VendorStatus,
    /// What the session has cost so far, in millionths of a US dollar.
    pub cost_micros: Option<i64>,
    pub budget_cents: Option<i64>,
}

/// One session event.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    /// Where this event sits: what a later `attach` resumes after (a `seq`,
    /// an event id). Empty for an event the vendor gave no position.
    pub cursor: String,
    pub at_ms: Option<i64>,
    pub kind: EventKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EventKind {
    /// The replay is over; what follows is live.
    CaughtUp,
    /// An input the session holds, under the mailbox id it was delivered
    /// with when the vendor keeps one; `consumed` once a turn took it.
    Input {
        request_id: Option<String>,
        text: String,
        consumed: bool,
    },
    /// Proof that a turn consumed an input (`next-step`).
    Consumed {
        request_id: String,
        duplicate: bool,
    },
    /// Inputs written and never consumed: offer them again.
    Returned {
        request_ids: Vec<String>,
        reason: String,
    },
    Text(String),
    ToolUse {
        name: String,
        input: Value,
    },
    ToolResult {
        text: String,
        is_error: bool,
    },
    /// The session asks the host to run a tool and waits for its result.
    HostToolCall {
        call_id: String,
        name: String,
        input: Value,
    },
    /// Usage since the last report: tokens, and cost in
    /// `reported_cost_micros` when the vendor priced it.
    Usage(TurnUsage),
    /// The session's whole cost so far, as the vendor tracks it.
    CostTotal {
        micros: i64,
    },
    TurnStarted,
    TurnEnded(Stop),
    /// The session is over and can never run again.
    Terminated(Option<String>),
    /// An error the vendor reported without ending the turn.
    Error(String),
    /// Anything else, by the vendor's type.
    Other(String),
}

/// How the vendor answered a delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// The session's durable record holds the message (`mid-turn` proof).
    Accepted { event_id: Option<String> },
    /// Written; [`EventKind::Consumed`] will prove it (`next-step`).
    InFlight,
    /// The session already held this request id; nothing new started.
    Duplicate,
    /// Refused for good, without applying it.
    Refused {
        reason: String,
        message_specific: bool,
    },
    /// Not taken now, and not applied; a later offer can succeed, no sooner
    /// than `retry_after_ms` when the vendor said.
    Wait {
        reason: String,
        retry_after_ms: Option<u64>,
    },
    /// The answer was lost: [`Adapter::holds`] settles it before anything is
    /// written again.
    Unknown(String),
}

/// A failed adapter call.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AdapterError {
    /// A network or vendor fault a retry can clear.
    #[error("{0}")]
    Transient(String),
    /// The vendor refused the credential.
    #[error("{0}")]
    Auth(String),
    /// The session no longer exists, or can never run again.
    #[error("{0}")]
    Gone(String),
    /// The adapter does not offer this call.
    #[error("{0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(String),
}

/// One provider's remote sessions. Every call is a request to the vendor; an
/// adapter keeps no state of its own beyond its client.
pub trait Adapter: Send + Sync + 'static {
    fn capabilities(&self) -> Capabilities;

    /// Create a session. `previous` is the adapter state of the agent's last
    /// session, so a vendor-side agent definition is reused, not recreated.
    /// `checkpoint` is handed the adapter state as soon as each vendor-side
    /// object exists, so a create that fails halfway leaves nothing a retry
    /// would make again.
    fn create<'a>(
        &'a self,
        spec: &'a Spec,
        previous: &'a Value,
        checkpoint: &'a (dyn Fn(&Value) + Send + Sync),
    ) -> Fut<'a, Handle>;

    /// Attach to the session's events: every event after `after` (all of
    /// them when `None`; none when `after` is past every event the vendor
    /// still holds), then [`EventKind::CaughtUp`], then live events, each
    /// once. A dropped stream is followed by a new `attach` from the last
    /// cursor applied. On `spiron.headless/v2` the cursor is a `seq` and
    /// `CaughtUp` is the live `ready`, the one echoing this attach's
    /// `request_id`, not a `ready` inside the replay (ADR 0011 D4).
    fn attach<'a>(&'a self, handle: &'a Handle, after: Option<&'a str>) -> Fut<'a, Events>;

    /// Hand the session one message under its mailbox id. A running session
    /// takes it at the level the adapter declares.
    fn deliver<'a>(
        &'a self,
        handle: &'a Handle,
        request_id: &'a str,
        text: &'a str,
    ) -> Fut<'a, Admission>;

    /// Whether the session already holds the input delivered as
    /// `request_id`: how a lost answer is settled before a second write.
    /// Only events after the cursor `after` need to be read: the executor
    /// already applied everything before it.
    fn holds<'a>(
        &'a self,
        handle: &'a Handle,
        request_id: &'a str,
        after: Option<&'a str>,
    ) -> Fut<'a, bool>;

    fn snapshot<'a>(&'a self, handle: &'a Handle) -> Fut<'a, Snapshot>;

    /// Stop the running turn; the session stays.
    fn interrupt<'a>(&'a self, handle: &'a Handle) -> Fut<'a, ()>;

    /// End the session for good and withdraw what it was granted.
    fn stop<'a>(&'a self, handle: &'a Handle) -> Fut<'a, ()>;

    /// Change the vendor's spend cap, in US cents ([`Budgeting::Vendor`]).
    fn set_budget<'a>(&'a self, handle: &'a Handle, cents: i64) -> Fut<'a, ()>;

    /// Replace the coordination MCP grant ([`Coordination::Mcp`]).
    fn grant<'a>(&'a self, handle: &'a Handle, grant: &'a Grant) -> Fut<'a, ()>;

    /// Answer a host tool call ([`EventKind::HostToolCall`]).
    fn tool_result<'a>(
        &'a self,
        handle: &'a Handle,
        call_id: &'a str,
        output: &'a str,
        is_error: bool,
    ) -> Fut<'a, ()>;
}
