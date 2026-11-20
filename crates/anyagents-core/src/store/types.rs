//! Domain data types persisted and returned by [`Store`](super::Store).

use std::collections::HashMap;

use super::TurnErrors;
use crate::error::Error;
use crate::id::{AgentId, ChannelId, MessageId, UsageId};
use crate::quote::Quote;

/// A line's provider coordinates: the chat, the forum topic and the message id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Origin {
    pub chat_id: Option<i64>,
    pub thread_id: Option<i64>,
    pub message_id: Option<i64>,
}

/// Batched roster metadata. Archived agents never need runtime diagnostics.
#[derive(Default)]
pub struct AgentQueryMetadata {
    pub aliases: HashMap<String, Vec<String>>,
    pub oldest_undelivered: HashMap<String, i64>,
    pub open_turns: HashMap<String, i64>,
    pub schedules: HashMap<String, Vec<AgentSchedule>>,
    /// Agents with a run of errored turns in progress, or parked for one.
    pub turn_errors: HashMap<String, TurnErrors>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnOwnershipSnapshot {
    pub last_delivered_seq: Option<i64>,
    pub last_delivered_at_ms: Option<i64>,
    pub last_delivered_sender: Option<String>,
    pub last_delivered_source_key: Option<String>,
    pub last_outbound_at_ms: Option<i64>,
}

/// One agent row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Agent {
    pub is_admin: bool,
    pub id: AgentId,
    pub name: String,
    pub role: Option<String>,
    /// Public capability/charter summary shared in team and group rosters (Layer 1).
    pub description: Option<String>,
    pub harness: String,
    pub model: Option<String>,
    pub workdir: Option<String>,
    /// Private operating instructions injected only into this agent's system prompt (Layer 2).
    pub instructions: Option<String>,
    pub state: String,
    pub created_at_ms: i64,
    /// The gateway origin and this agent's own gateway token, set at create.
    /// Never a provider key.
    pub launch_env: Vec<(String, String)>,
    /// The parent agent that spawned this agent, when it is a child/sub-agent.
    pub parent_id: Option<AgentId>,
}

impl Agent {
    /// Returns the agent's public capability/charter summary (`description`) for team and group
    /// rosters without exposing multi-paragraph private `instructions`.
    pub fn public_description(&self) -> Option<String> {
        if let Some(desc) = self
            .description
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(desc.to_string());
        }
        let raw = self
            .instructions
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())?;
        let first_line = raw.lines().next().unwrap_or(raw).trim();
        if first_line.is_empty() {
            return None;
        }
        let truncated: String = first_line.chars().take(300).collect();
        Some(truncated)
    }

    /// Internal communication and discovery scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`):
    /// - `"isolated"`: strictly confined to direct `parent`, own `children`, and bound external channels;
    ///   cannot message or read siblings or other roots.
    /// - `"private"` (default for all sub-agents): only direct `parent`,
    ///   own `children`, or shared `group:<name>` can message it; can message `public` agents.
    /// - `"siblings"` (explicit opt-in):
    ///   reachable by `parent`, `children`, and sibling sub-agents under the same `parent` with
    ///   `"siblings"` or `"public"` scope.
    /// - `"public"` (default for Root Agents, or sub-agents with `public: true`): reachable across the workspace.
    pub fn scope(&self) -> &'static str {
        self.scope_or_default(self.default_scope())
    }

    pub(super) fn scope_or_default(&self, default: &'static str) -> &'static str {
        if let Some((_, v)) = self.launch_env.iter().find(|(k, _)| k == "ANYAGENTS_SCOPE") {
            match v.trim().to_ascii_lowercase().as_str() {
                "isolated" | "none" => return "isolated",
                "private" | "false" | "0" => return "private",
                "siblings" | "sibling" | "team" => return "siblings",
                "public" | "org" | "all" | "true" | "1" => return "public",
                _ => {}
            }
        }
        if let Some((_, v)) = self
            .launch_env
            .iter()
            .find(|(k, _)| k == "ANYAGENTS_PUBLIC")
        {
            match v.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "public" => return "public",
                "siblings" | "sibling" | "team" => return "siblings",
                "isolated" => return "isolated",
                "0" | "false" | "no" | "private" => return "private",
                _ => {}
            }
        }
        default
    }

    pub(super) fn default_scope(&self) -> &'static str {
        if self.parent_id.is_none() {
            "public"
        } else {
            "private"
        }
    }

    pub fn is_isolated(&self) -> bool {
        self.scope() == "isolated"
    }

    pub fn allows_sibling_messages(&self) -> bool {
        matches!(self.scope(), "siblings" | "public")
    }

    /// Whether this sub-agent has been explicitly opted in (`public: true` / `ANYAGENTS_PUBLIC=1`)
    /// by its Parent Agent to accept external messages and group invites across the workspace.
    pub fn is_public_subagent(&self) -> bool {
        self.parent_id.is_some() && self.scope() == "public"
    }

    /// Whether this agent accepts direct messages and group invites from agents
    /// outside its direct parent/child lineage. Root agents are always public (`true`);
    /// sub-agents default to `false` (`private`) unless their Parent sets `public: true`.
    pub fn is_public(&self) -> bool {
        self.parent_id.is_none() || self.scope() == "public"
    }

    /// Lifecycle mode (`"standing"` | `"task"`):
    /// - `"standing"` (default for Root Agents, or sub-agents hired as long-lived team members):
    ///   remains on standby (`idle`) between assignments without turn-ownership archive reminders.
    /// - `"task"` (default for Sub-Agents): hired for a bounded mission; must report back (`send`)
    ///   before exiting a parent-initiated turn, and once reported (`waiting_parent`), awaits
    ///   either a follow-up `send` or `archive` from its parent.
    pub fn lifecycle(&self) -> &'static str {
        if let Some((_, v)) = self
            .launch_env
            .iter()
            .find(|(k, _)| k == "ANYAGENTS_LIFECYCLE")
        {
            match v.trim().to_ascii_lowercase().as_str() {
                "standing" | "permanent" | "daemon" | "resident" => return "standing",
                "task" | "ephemeral" | "freelance" | "oneshot" => return "task",
                _ => {}
            }
        }
        self.default_lifecycle()
    }

    fn default_lifecycle(&self) -> &'static str {
        if self.parent_id.is_none() {
            "standing"
        } else {
            "task"
        }
    }

    pub fn is_task_subagent(&self) -> bool {
        self.parent_id.is_some() && self.lifecycle() == "task"
    }
}

/// An agent's identity, lifecycle state and place in the tree, without its
/// instructions or launch environment: what rosters, trees and sweeps over
/// many agents need per row. Policy derived from the launch environment
/// (scope, lifecycle mode) needs the full [`Agent`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentSummary {
    pub id: AgentId,
    pub name: String,
    pub harness: String,
    pub model: Option<String>,
    pub state: String,
    pub parent_id: Option<AgentId>,
    pub role: Option<String>,
    pub is_admin: bool,
    pub created_at_ms: i64,
}

/// Fields for `agents.create`. The harness value is a catalog key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewAgent {
    pub name: String,
    pub role: Option<String>,
    pub description: Option<String>,
    pub harness: String,
    pub model: Option<String>,
    pub workdir: Option<String>,
    pub instructions: Option<String>,
    pub parent_id: Option<AgentId>,
}

impl NewAgent {
    pub fn new(name: impl Into<String>, harness: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            role: None,
            description: None,
            harness: harness.into(),
            model: None,
            workdir: None,
            instructions: None,
            parent_id: None,
        }
    }
}

/// One self-scheduled wake (`at`, `every`) or event watch (`watch`) owned by an agent.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentSchedule {
    pub id: String,
    pub agent_id: AgentId,
    pub kind: String,
    pub spec: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fired_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_baseline: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed: Option<String>,
    pub state: String,
    pub created_at_ms: i64,
}

/// Input for [`Store::create_schedule`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewSchedule {
    pub agent_id: AgentId,
    pub kind: String,
    pub spec: String,
    pub prompt: String,
    pub interval_ms: Option<i64>,
    pub next_fire_at_ms: Option<i64>,
    pub watch_baseline: Option<String>,
}

/// A channel adapter and the bot or account it speaks as.
///
/// `credential_ref` names a secret. The secret itself is never in this row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Channel {
    pub id: ChannelId,
    pub kind: String,
    pub identity: Option<String>,
    /// Adapter settings, as JSON. Never a secret.
    pub config: String,
    pub credential_ref: Option<String>,
    /// Adapter memory, as JSON: a poll offset, the chats an agent answers in.
    /// Never a secret.
    pub state: String,
    pub created_at_ms: i64,
    /// Agents attached to this channel, in attach order.
    pub agents: Vec<AgentId>,
}

/// Fields for creating a channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewChannel {
    pub kind: String,
    pub identity: Option<String>,
    pub config: String,
    pub credential_ref: Option<String>,
}

/// One append to a mailbox. An empty source key is not a key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incoming {
    pub text: String,
    pub sender: Option<String>,
    pub source_time_ms: Option<i64>,
    pub source_key: Option<String>,
    pub reply_to: Option<String>,
    /// The message this one quotes, when the provider sent one. It is rendered
    /// into the stored body; an agent's text cannot forge it.
    pub quote: Option<Quote>,
    /// For a line from a channel: the provider chat and forum topic it is in,
    /// and the provider id of the message itself. These are what a bare answer
    /// is resolved against.
    pub origin: Option<Origin>,
    /// Overrides the receive clock so tests can age a message without sleeping.
    pub received_at_ms: Option<i64>,
}

impl Incoming {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            sender: None,
            source_time_ms: None,
            source_key: None,
            reply_to: None,
            quote: None,
            origin: None,
            received_at_ms: None,
        }
    }

    /// The body as it is stored, or handed to a harness: the text, then any
    /// trusted quote.
    pub fn body(&self) -> String {
        crate::quote::compose(&self.text, self.quote.as_ref())
    }
}

/// How far a message has moved: committed, or taken by the harness's own input
/// or steer API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Ack {
    Received,
    Delivered,
}

impl std::fmt::Display for Ack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Received => "received",
            Self::Delivered => "delivered",
        })
    }
}

/// A mailbox message the harness refused for good, parked past the cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectedMessage {
    pub message_id: String,
    pub agent_id: AgentId,
    pub seq: i64,
    pub sender: Option<String>,
    pub reason: String,
    pub rejected_at_ms: i64,
    /// When its sender was told, if it was.
    pub notified_at_ms: Option<i64>,
}

/// One mailbox entry. `ack` is derived from the cursors, not stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub id: MessageId,
    pub agent_id: AgentId,
    pub seq: i64,
    pub text: String,
    pub sender: Option<String>,
    pub source_time_ms: Option<i64>,
    pub source_key: Option<String>,
    pub reply_to: Option<String>,
    pub received_at_ms: i64,
    pub ack: Ack,
    /// When the message was handed to the vendor process.
    pub delivered_at_ms: Option<i64>,
    /// The provider coordinates the channel stored with it.
    pub origin: Option<Origin>,
    /// The trusted header a harness shows its model: the authenticated author,
    /// the channel, the chat and topic, the reply a bare answer names, and the
    /// destination that answer goes to.
    pub context: anyagents_harness::Context,
}

/// A new insert, or the row already stored for this source key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiveOutcome {
    New(Message),
    Duplicate(Message),
}

/// One turn in an agent's bidirectional conversation timeline (`in` or `out`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConversationTurn {
    #[serde(default)]
    pub seq: i64,
    pub id: String,
    /// `"in"` when received by this agent; `"out"` when sent by this agent.
    pub direction: String,
    pub from: String,
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub text: String,
    pub timestamp_ms: i64,
}

/// Ordering for [`HistorySearchQuery`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HistorySort {
    #[default]
    Relevance,
    Newest,
    Oldest,
}

impl HistorySort {
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("newest" | "desc" | "time_desc" | "recent" | "latest") => Self::Newest,
            Some("oldest" | "asc" | "time_asc" | "chronological") => Self::Oldest,
            _ => Self::Relevance,
        }
    }
}

/// Structured + lexical search request over agent conversations and tool history (`Raw-RAG`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HistorySearchQuery {
    /// Primary search query string (optional when browsing/filtering by range, tool, peer, or time).
    pub query: Option<String>,
    /// Additional search queries (OR-combined with `query` for multi-keyword / synonym recall).
    pub queries: Vec<String>,
    /// Negative terms — any turn containing any of these substrings (case-insensitive) is excluded.
    pub exclude: Vec<String>,
    /// Agent name (`"assistant"`, `"all"`) or `agt_...` ID.
    pub agent: Option<String>,
    /// Optional peer filter (user, channel chat, internal group, or other agent).
    pub with_peer: Option<String>,
    /// Direction filter: `"in"`, `"out"`, or `None` / `"all"`.
    pub direction: Option<String>,
    /// Optional sender substring/exact filter.
    pub sender: Option<String>,
    /// Whether to include tool/system history (`true` by default). Set `false` to search only messages.
    pub include_tools: bool,
    /// When `true`, searches ONLY tool/system history (`kind = 'tool'`).
    pub only_tools: bool,
    /// Optional tool name filter (e.g. `"browser"`, `"agents"`, `"schedule"`, `"send"`, `"bash"`).
    pub tool_name: Option<String>,
    /// Lower timestamp bound (`timestamp_ms > since_ms`).
    pub since_ms: Option<i64>,
    /// Upper timestamp bound (`timestamp_ms <= until_ms`).
    pub until_ms: Option<i64>,
    /// Optional sequence range lower bound (`seq >= from_seq`).
    pub from_seq: Option<i64>,
    /// Optional sequence range upper bound (`seq <= to_seq`).
    pub to_seq: Option<i64>,
    /// Result ordering (`Relevance`, `Newest`, `Oldest`).
    pub sort: HistorySort,
    /// Contiguous neighbor turns (`0..=10`) to include around each matched hit.
    pub radius: usize,
    /// Maximum turns to return (`1..=200`).
    pub limit: usize,
    /// Caller-authorized immutable agent IDs. `None` is the unrestricted operator view;
    /// `Some` is the visibility boundary resolved by the API before this query
    /// reaches the Raw-RAG reader. This is deliberately separate from `agent`,
    /// which is a caller-supplied narrowing selector.
    #[serde(default)]
    pub visible_agent_ids: Option<Vec<String>>,
}

impl Default for HistorySearchQuery {
    fn default() -> Self {
        Self {
            query: None,
            queries: Vec::new(),
            exclude: Vec::new(),
            agent: None,
            with_peer: None,
            direction: None,
            sender: None,
            include_tools: true,
            only_tools: false,
            tool_name: None,
            since_ms: None,
            until_ms: None,
            from_seq: None,
            to_seq: None,
            sort: HistorySort::Relevance,
            radius: 0,
            limit: 20,
            visible_agent_ids: None,
        }
    }
}

/// Age of the oldest message the harness has not taken. `None` means the agent
/// has caught up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lag {
    pub oldest_undelivered_age_ms: Option<i64>,
}

/// How this turn was paid for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BillingMode {
    Subscription,
    ApiKey,
}

impl BillingMode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Subscription => "subscription",
            Self::ApiKey => "api-key",
        }
    }

    pub(super) fn parse(raw: &str) -> Result<Self, Error> {
        match raw {
            "subscription" => Ok(Self::Subscription),
            "api-key" => Ok(Self::ApiKey),
            other => Err(Error::Message(format!("unknown billing mode: {other}"))),
        }
    }
}

impl std::fmt::Display for BillingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One turn's usage row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageRecord {
    pub id: UsageId,
    pub agent_id: AgentId,
    pub harness: String,
    pub model: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reported_cost_micros: Option<i64>,
    pub estimated_cost_micros: Option<i64>,
    pub billing_mode: BillingMode,
    pub quota_state: Option<String>,
    pub day: String,
    pub recorded_at_ms: i64,
}

/// Fields for one usage insert. The id and record time are assigned here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewUsage {
    pub agent_id: AgentId,
    pub harness: String,
    pub model: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reported_cost_micros: Option<i64>,
    pub estimated_cost_micros: Option<i64>,
    pub billing_mode: BillingMode,
    pub quota_state: Option<String>,
    pub day: String,
}

/// Maximum number of automatic `[Recovery]` wake attempts for a single open turn.
pub const MAX_RECOVERY_ATTEMPTS: i64 = 3;

/// One outbound message already committed by the agent during an interrupted turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundEffect {
    pub id: String,
    pub destination: String,
    pub reply_to: Option<String>,
    pub body: String,
    pub sent_at_ms: i64,
}

/// An open turn that a harness accepted (`opened_seq > completed_seq`) and had
/// not yet completed when its process stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenTurn {
    pub agent_id: AgentId,
    pub opened_seq: i64,
    pub completed_seq: i64,
    pub recovery_count: i64,
    pub opened_at_ms: i64,
    pub messages: Vec<Message>,
    pub completed_outbounds: Vec<OutboundEffect>,
}

/// A mailbox or agent change a waiter can block on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wake {
    /// A new message was inserted for this agent.
    Mail { agent_id: AgentId },
    /// This agent was marked running.
    Agent { agent_id: AgentId },
}

/// Outcome of one [`Store::checkpoint_wal`] pass, in WAL frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalCheckpoint {
    /// Frames in the WAL when the passive pass ran.
    pub log_frames: i64,
    /// Frames copied back into the database by the passive pass.
    pub checkpointed_frames: i64,
    /// `true` when a reader held an older snapshot and stopped the backfill.
    pub passive_busy: bool,
    /// `true` when the WAL was fully backfilled and truncated to zero bytes.
    pub truncated: bool,
}
