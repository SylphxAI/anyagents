//! SQLite mailbox. A message is received when its insert commits, and that
//! commit assigns the next per-agent sequence. Source time is stored and never
//! used to order delivery. The delivery cursor moves only over a contiguous
//! prefix.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use anyagents_harness::normalize_harness_name;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::error::Error;
use crate::id::{AgentId, ChannelId, MessageId, UsageId};
use crate::quote::Quote;

#[cfg(test)]
#[path = "store/audit_tests.rs"]
mod audit_tests;
mod group_policy;
pub use group_policy::{Group, GroupAction, GroupActor};

/// A line's provider coordinates: the chat, the forum topic and the message id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Origin {
    pub chat_id: Option<i64>,
    pub thread_id: Option<i64>,
    pub message_id: Option<i64>,
}
use crate::migrate::{self, now_ms as clock_now_ms};

/// Milliseconds since the Unix epoch. A clock that is before the epoch reads as 0.
pub fn now_ms() -> i64 {
    clock_now_ms()
}

const MESSAGE_COLS: &str = "id, agent_id, seq, source_time_ms, source_key, sender, body, \
     reply_to, received_at_ms, delivered_at_ms, origin, context";

const AGENT_COLS: &str = "id, name, harness, model, workdir, instructions, state, \
     created_at_ms, launch_env, parent_id, is_admin, role, description";

fn is_scope_env_key(key: &str) -> bool {
    matches!(key, "ANYAGENTS_SCOPE" | "ANYAGENTS_PUBLIC")
}

fn is_policy_env_key(key: &str) -> bool {
    is_scope_env_key(key) || key == "ANYAGENTS_LIFECYCLE"
}

/// Validate and normalize an agent lifecycle (`"task"` or `"standing"`) without persisting state.
pub fn parse_agent_lifecycle(lifecycle: &str) -> Result<&'static str, Error> {
    match lifecycle.trim().to_ascii_lowercase().as_str() {
        "standing" | "permanent" | "daemon" | "resident" => Ok("standing"),
        "task" | "ephemeral" | "freelance" | "oneshot" => Ok("task"),
        other => Err(Error::Message(format!(
            "invalid lifecycle `{other}`; expected `task` or `standing`"
        ))),
    }
}

fn lifecycle_env(agent: &Agent, lifecycle: &str) -> Vec<(String, String)> {
    let mut env = agent.launch_env.clone();
    env.retain(|(key, _)| key != "ANYAGENTS_LIFECYCLE");
    env.push(("ANYAGENTS_LIFECYCLE".to_string(), lifecycle.to_string()));
    env
}

/// Validate and normalize an agent scope without persisting any state.
pub fn parse_agent_scope(scope: &str) -> Result<&'static str, Error> {
    match scope.trim().to_ascii_lowercase().as_str() {
        "isolated" | "none" => Ok("isolated"),
        "private" | "false" | "0" => Ok("private"),
        "siblings" | "sibling" | "team" => Ok("siblings"),
        "public" | "org" | "all" | "true" | "1" => Ok("public"),
        other => Err(Error::Message(format!(
            "invalid scope `{other}`; expected `isolated`, `private`, `siblings`, or `public`"
        ))),
    }
}

fn scope_env(agent: &Agent, scope: &str) -> Vec<(String, String)> {
    let mut env = agent.launch_env.clone();
    env.retain(|(key, _)| !is_scope_env_key(key));
    env.push(("ANYAGENTS_SCOPE".to_string(), scope.to_string()));
    if scope == "public" {
        env.push(("ANYAGENTS_PUBLIC".to_string(), "1".to_string()));
    }
    env
}

/// Batched roster metadata. Archived agents never need runtime diagnostics.
#[derive(Default)]
pub struct AgentQueryMetadata {
    pub aliases: HashMap<String, Vec<String>>,
    pub oldest_undelivered: HashMap<String, i64>,
    pub open_turns: HashMap<String, i64>,
    pub schedules: HashMap<String, Vec<AgentSchedule>>,
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

    fn scope_or_default(&self, default: &'static str) -> &'static str {
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

    fn default_scope(&self) -> &'static str {
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
    fn as_str(self) -> &'static str {
        match self {
            Self::Subscription => "subscription",
            Self::ApiKey => "api-key",
        }
    }

    fn parse(raw: &str) -> Result<Self, Error> {
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

/// Held for one vendor loop or lifecycle mutation. Drop explicitly releases the
/// flock even while a fork-inherited descriptor remains open.
#[derive(Debug)]
pub struct AgentLock {
    _file: File,
}

impl Drop for AgentLock {
    fn drop(&mut self) {
        // Closing only our descriptor leaves flock held by a concurrent fork's
        // inherited copy until exec. Release the open-file-description lock
        // explicitly so the next turn and busy probe see the intended lifetime.
        let _ = unsafe { libc::flock(self._file.as_raw_fd(), libc::LOCK_UN) };
    }
}

/// A mailbox or agent change a waiter can block on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wake {
    /// A new message was inserted for this agent.
    Mail { agent_id: AgentId },
    /// This agent was marked running.
    Agent { agent_id: AgentId },
}

/// Open database. Methods take a shared reference; writes serialize on the mutex.
pub struct Store {
    conn: Mutex<Connection>,
    db_path: PathBuf,
    data_dir: PathBuf,
    wakes: Mutex<Vec<Sender<Wake>>>,
    capabilities: Mutex<Option<String>>,
}

enum ReadConn<'a> {
    Owned(Connection),
    Guard(MutexGuard<'a, Connection>),
}

impl std::ops::Deref for ReadConn<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        match self {
            Self::Owned(c) => c,
            Self::Guard(g) => g,
        }
    }
}

/// A capability is 32 random bytes, written in hex.
const CAPABILITY_BYTES: usize = 32;

/// Freeze the old effective policy before removing implicit role/name defaults.
/// New rows always have an explicit scope. This bounded, transactional upgrade
/// touches only legacy environment metadata, never identities or work state.
fn preserve_legacy_scopes(conn: &mut Connection) -> Result<(), Error> {
    let missing_scope_sql = "SELECT EXISTS(SELECT 1 FROM agents WHERE NOT EXISTS (
                SELECT 1 FROM json_each(CASE WHEN json_valid(launch_env) THEN launch_env ELSE '[]' END) entry
                WHERE entry.key % 2 = 0 AND entry.value = 'ANYAGENTS_SCOPE'
                  AND lower(trim(json_extract(launch_env, '$[' || (entry.key + 1) || ']'))) IN (
                    'isolated','none','private','false','0','siblings','sibling','team',
                    'public','org','all','true','1')))";
    if !conn.query_row(missing_scope_sql, [], |r| r.get::<_, bool>(0))? {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let ids: Vec<String> = {
        let mut stmt = tx.prepare(
            "SELECT id FROM agents WHERE NOT EXISTS (
                SELECT 1 FROM json_each(CASE WHEN json_valid(launch_env) THEN launch_env ELSE '[]' END) entry
                WHERE entry.key % 2 = 0 AND entry.value = 'ANYAGENTS_SCOPE'
                  AND lower(trim(json_extract(launch_env, '$[' || (entry.key + 1) || ']'))) IN (
                    'isolated','none','private','false','0','siblings','sibling','team',
                    'public','org','all','true','1'))",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?
    };
    for id in ids {
        let agent = agent_by_id(&tx, &AgentId::parse(&id)?)?;
        // Compatibility only: role and name never select policy for new agents.
        let legacy_default = if agent.parent_id.is_none() {
            "public"
        } else if agent.name.ends_with("-assistant") {
            "isolated"
        } else if agent.role.as_deref().is_some_and(|r| !r.trim().is_empty()) {
            "siblings"
        } else {
            "private"
        };
        let scope = agent.scope_or_default(legacy_default);
        tx.execute(
            "UPDATE agents SET launch_env = ?1 WHERE id = ?2",
            params![encode_env(&scope_env(&agent, scope)), id],
        )?;
    }
    tx.commit()?;
    Ok(())
}

impl Store {
    /// Open or create the database, turn on WAL, and apply pending migrations.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        // A 30s wait: ~78 agents finishing at once queue here instead of failing
        // with `database is locked`. BEGIN IMMEDIATE (not the DEFERRED default)
        // is what makes the wait apply to read-then-write paths.
        conn.busy_timeout(Duration::from_secs(30))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let mut mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            mode = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        }
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Message(format!(
                "sqlite journal mode is {mode}, not wal"
            )));
        }
        let mut conn = conn;
        let schema_version = migrate::current_version(&conn)?;
        migrate::migrate(&mut conn)?;
        preserve_legacy_scopes(&mut conn)?;
        if migrate::current_version(&conn)? < 1 {
            return Err(Error::Message("migrations did not apply".into()));
        }
        // Read first so ordinary opens of current stores do not take the writer
        // lock. The UPDATE is idempotent and changes only the harness selector.
        let has_legacy_harness: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE harness IN ('sylphx', 'sylphx-agent', 'sylphx_agent', 'sylphx/default'))",
            [],
            |row| row.get(0),
        )?;
        if has_legacy_harness {
            conn.execute(
                "UPDATE agents SET harness = 'spiron' WHERE harness IN ('sylphx', 'sylphx-agent', 'sylphx_agent', 'sylphx/default')",
                [],
            )?;
        }
        let has_legacy_state: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE state NOT IN ('created', 'running', 'idle', 'paused', 'archived'))",
            [],
            |row| row.get(0),
        )?;
        if has_legacy_state {
            conn.execute(
                "UPDATE agents SET state = 'archived' WHERE state NOT IN ('created', 'running', 'idle', 'paused', 'archived')",
                [],
            )?;
        }
        let has_aux_schema: bool = conn.query_row(
            "SELECT (SELECT count(*) FROM sqlite_master WHERE name IN ('message_versions', 'agent_topology_state', 'idx_rag_chunks_agent_id_time')) = 3",
            [],
            |row| row.get(0),
        )?;
        if !has_aux_schema {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS message_versions (
                    agent_id TEXT NOT NULL,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    body TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (agent_id, chat_id, message_id)
                );
                CREATE TABLE IF NOT EXISTS agent_topology_state (
                    agent_id TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_id_time
                    ON rag_chunks(agent_id, timestamp_ms DESC, chunk_id);",
            )?;
        }
        // UNIQUE(agent_id, seq) already supports reverse mailbox sequence scans.
        // Avoid even idempotent DDL on ordinary opens: it needs the writer lock.
        for (name, sql) in [
            (
                "idx_schedules_active_agent",
                "CREATE INDEX IF NOT EXISTS idx_schedules_active_agent ON agent_schedules(agent_id, created_at_ms, id) WHERE state = 'active'",
            ),
            (
                "idx_messages_agent_time",
                "CREATE INDEX IF NOT EXISTS idx_messages_agent_time ON messages(agent_id, received_at_ms DESC, seq DESC)",
            ),
            (
                "idx_outbound_agent_time",
                "CREATE INDEX IF NOT EXISTS idx_outbound_agent_time ON outbound_messages(agent_id, sent_at_ms DESC, id DESC)",
            ),
        ] {
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1)",
                [name],
                |row| row.get(0),
            )?;
            if !exists {
                conn.execute_batch(sql)?;
            }
        }
        // Older databases may already have this column from an earlier runtime.
        // Presence, not an arbitrary failed ALTER, is the idempotency condition.
        let has_public: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('groups') WHERE name = 'public')",
            [],
            |row| row.get(0),
        )?;
        if !has_public {
            conn.execute(
                "ALTER TABLE groups ADD COLUMN public INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        // Current schemas maintain FTS transactionally on every history write.
        // Only legacy upgrades need the full derivative-index validation; doing
        // it on every open scans history and competes with the server writer.
        if schema_version < 23 {
            ensure_lexical_fts_backfilled(&mut conn)?;
        }
        Ok(Self {
            conn: Mutex::new(conn),
            db_path: path.to_path_buf(),
            data_dir: data_dir_for(path),
            wakes: Mutex::new(Vec::new()),
            capabilities: Mutex::new(None),
        })
    }

    /// Hold this process's mailbox capability key, so it can mint and check
    /// capabilities. The server sets it at start; the key never leaves memory.
    /// A process naming its own key makes every capability it minted useless to
    /// any other process, which is why only the one server should.
    pub fn hold_capability_key(&self, key: &str) -> Result<(), Error> {
        if key.len() < 32 {
            return Err(Error::Message("the capability key is too short".into()));
        }
        *self
            .capabilities
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(key.to_string());
        Ok(())
    }

    /// Whether this process can decide capability validity authoritatively.
    /// Helpers without the key may ask the daemon; the daemon must never ask
    /// its own socket again after rejecting a capability.
    pub fn holds_capability_key(&self) -> bool {
        self.capabilities
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .is_some()
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.db_path
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// A mailbox capability for one agent instance: an opaque value, stored as
    /// a hash, bound to the agent's immutable id and to the generation of its
    /// current session. Only the server that minted it reads it back.
    pub fn issue_capability(&self, agent_id: &AgentId) -> Result<String, Error> {
        let key = self.capability_key()?;
        let mut bytes = [0u8; CAPABILITY_BYTES];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        let token = hex(&bytes);
        let now = now_ms();
        {
            let mut conn = self.conn();
            agent_by_id(&conn, agent_id)?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // A new instance replaces the old one: the generation moves, and
            // every capability minted before it stops authenticating.
            let generation: i64 = tx.query_row(
                "SELECT COALESCE(MAX(generation), 0) + 1 FROM agent_capabilities WHERE agent_id = ?1",
                [agent_id.as_str()],
                |row| row.get(0),
            )?;
            tx.execute(
                "INSERT INTO agent_capabilities (token_hash, agent_id, generation, created_at_ms, revoked_at_ms)
                 VALUES (?1, ?2, ?3, ?4, NULL)",
                params![hash(&key, &token), agent_id.as_str(), generation, now],
            )?;
            tx.execute(
                "UPDATE agent_capabilities SET revoked_at_ms = ?1
                 WHERE agent_id = ?2 AND generation < ?3 AND revoked_at_ms IS NULL",
                params![now, agent_id.as_str(), generation],
            )?;
            tx.commit()?;
        }
        Ok(token)
    }

    /// The agent a presented capability authenticates, when the server minted
    /// it and it was not replaced. The comparison is on the stored hash.
    pub fn authenticate_capability(&self, token: &str) -> Result<AgentId, Error> {
        let key = self.capability_key()?;
        let digest = hash(&key, token);
        let conn = self.read_conn();
        let found = conn
            .query_row(
                "SELECT agent_id FROM agent_capabilities
                 WHERE token_hash = ?1 AND revoked_at_ms IS NULL",
                [digest.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        match found {
            Some(id) => AgentId::parse(&id),
            None => Err(Error::Message("the session is not authenticated".into())),
        }
    }

    /// Resolves the full [`Agent`] record authenticated by a presented capability token.
    pub fn authenticate_agent(&self, token: &str) -> Result<Agent, Error> {
        let agent_id = self.authenticate_capability(token)?;
        self.agent(&agent_id)
    }

    /// A receipt one named send already produced, or `None`.
    pub fn send_receipt(&self, id: &str) -> Result<Option<String>, Error> {
        let conn = self.conn();
        Ok(conn
            .query_row("SELECT receipt FROM sends WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .optional()?)
    }

    /// A committed mailbox receipt for this source, including when the server
    /// stopped before recording its whole-send receipt. Call under the lifecycle
    /// lock before preparing an archived recipient for an idempotent retry.
    pub fn message_by_source_key(
        &self,
        agent_id: &AgentId,
        key: &str,
    ) -> Result<Option<Message>, Error> {
        message_by_source(&self.conn(), agent_id, key)
    }

    /// Keep one named send's receipt after it was delivered.
    pub fn record_send_receipt(&self, id: &str, receipt: &str) -> Result<(), Error> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO sends (id, receipt, created_at_ms) VALUES (?1, ?2, ?3)
             ON CONFLICT (id) DO NOTHING",
            params![id, receipt, now_ms()],
        )?;
        Ok(())
    }

    /// This process's capability key. A command that starts before the server
    /// reads the key from `ANYAGENTS_CAPABILITY_KEY` (the test and repair path),
    /// else from the stored key the server wrote, else its own.
    fn capability_key(&self) -> Result<String, Error> {
        if let Some(key) = self
            .capabilities
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
        {
            return Ok(key);
        }
        Err(Error::Message(
            "this process holds no mailbox capability key".into(),
        ))
    }

    /// Whether a capability was ever minted on this deployment. Once one has
    /// been, the deployment has an identity boundary.
    pub fn ever_authenticated(&self) -> bool {
        let conn = self.conn();
        conn.query_row("SELECT COUNT(*) FROM agent_capabilities", [], |row| {
            row.get::<_, i64>(0)
        })
        .map(|count| count > 0)
        .unwrap_or(false)
    }

    /// Replace an agent's instance: its generation moves, so every capability
    /// minted for the last instance stops authenticating at once.
    pub fn retire_capabilities(&self, agent_id: &AgentId) -> Result<(), Error> {
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        conn.execute(
            "UPDATE agent_capabilities SET revoked_at_ms = ?1
             WHERE agent_id = ?2 AND revoked_at_ms IS NULL",
            params![now_ms(), agent_id.as_str()],
        )?;
        Ok(())
    }

    /// A channel for later mailbox inserts and running transitions.
    ///
    /// Events already committed are not replayed. Scan current state after
    /// subscribing.
    pub fn subscribe(&self) -> Receiver<Wake> {
        let (sender, receiver) = mpsc::channel();
        self.wakes
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(sender);
        receiver
    }

    /// Drop every subscriber so a blocked `recv` returns.
    pub fn close_wakes(&self) {
        self.wakes
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clear();
    }

    /// Fan a wake to in-process subscribers. Does not touch the datagram socket.
    pub fn poke(&self, wake: Wake) {
        self.fanout(&wake);
    }

    fn notify(&self, wake: Wake) {
        self.send_datagram(&wake);
        self.fanout(&wake);
    }

    fn fanout(&self, wake: &Wake) {
        let mut wakes = self.wakes.lock().unwrap_or_else(|err| err.into_inner());
        wakes.retain(|sender| sender.send(wake.clone()).is_ok());
    }

    fn send_datagram(&self, wake: &Wake) {
        let (tag, id) = match wake {
            Wake::Mail { agent_id } => (b'm', agent_id.as_str()),
            Wake::Agent { agent_id } => (b'a', agent_id.as_str()),
        };
        let path = self.data_dir.join("wake.sock");
        if path.as_os_str().len() > 107 {
            return;
        }
        let mut payload = Vec::with_capacity(1 + id.len());
        payload.push(tag);
        payload.extend_from_slice(id.as_bytes());
        if let Ok(socket) = UnixDatagram::unbound() {
            let _ = socket.send_to(&payload, &path);
        }
    }

    /// Exclusive lock for this agent's vendor process. A second caller gets [`Error::AgentBusy`].
    pub fn try_lock_agent(&self, agent_id: &AgentId) -> Result<AgentLock, Error> {
        let dir = self.data_dir.join("locks");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.lock", agent_id.as_str()));
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        // flock, not a POSIX record lock: two descriptors in one process must exclude
        // each other, which is what a second loop in the same test process needs.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == ErrorKind::WouldBlock || err.raw_os_error() == Some(libc::EAGAIN) {
                return Err(Error::AgentBusy);
            }
            return Err(Error::Io(err));
        }
        Ok(AgentLock { _file: file })
    }

    /// Serialize lifecycle state and archive-marker changes across API/server
    /// processes. This is separate from the vendor-process lock: an archived
    /// or live process must never prevent its lifecycle from being changed.
    /// Keep the returned guard alive through preparation and the store commit.
    pub fn lock_agent_lifecycle(&self, agent_id: &AgentId) -> Result<AgentLock, Error> {
        self.agent_lifecycle_lock(agent_id, false)
    }

    /// Maintenance must skip a busy lifecycle rather than delay unrelated mail.
    pub fn try_lock_agent_lifecycle(&self, agent_id: &AgentId) -> Result<AgentLock, Error> {
        self.agent_lifecycle_lock(agent_id, true)
    }

    fn agent_lifecycle_lock(
        &self,
        agent_id: &AgentId,
        nonblocking: bool,
    ) -> Result<AgentLock, Error> {
        let dir = self.data_dir.join("locks");
        std::fs::create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(format!("{}.lifecycle", agent_id.as_str())))?;
        let operation = libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 };
        loop {
            let rc = unsafe { libc::flock(file.as_raw_fd(), operation) };
            if rc == 0 {
                return Ok(AgentLock { _file: file });
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == ErrorKind::WouldBlock {
                return Err(Error::AgentBusy);
            }
            if error.kind() != ErrorKind::Interrupted {
                return Err(Error::Io(error));
            }
        }
    }

    pub fn create_agent(&self, new: NewAgent) -> Result<Agent, Error> {
        validate_token(&new.name, "agent name")?;
        let harness = normalize_harness_name(&new.harness);
        if harness.is_empty() {
            return Err(Error::Message("harness is empty".into()));
        }
        let clean_role = new
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let clean_description = new
            .description
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                new.instructions
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .and_then(|raw| raw.lines().next())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|line| line.chars().take(300).collect())
            });
        let mut agent = Agent {
            is_admin: false,
            id: AgentId::generate(),
            name: new.name,
            role: clean_role,
            description: clean_description,
            harness: harness.to_string(),
            model: new.model,
            workdir: new.workdir,
            instructions: new.instructions,
            state: "created".into(),
            created_at_ms: now_ms(),
            launch_env: Vec::new(),
            parent_id: new.parent_id,
        };
        agent.launch_env = scope_env(&agent, agent.default_scope());
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let taken: i64 = tx.query_row(
            "SELECT COUNT(*) FROM agents WHERE name = ?1",
            [&agent.name],
            |row| row.get(0),
        )?;
        if taken != 0 {
            return Err(Error::AgentExists(agent.name));
        }
        tx.execute(
            "INSERT INTO agents (
                id, name, harness, model, workdir, instructions, state, created_at_ms, parent_id, role, description, launch_env
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'created', ?7, ?8, ?9, ?10, ?11)",
            params![
                agent.id.as_str(),
                agent.name,
                agent.harness,
                agent.model,
                agent.workdir,
                agent.instructions,
                agent.created_at_ms,
                agent.parent_id.as_ref().map(|id| id.as_str()),
                agent.role,
                agent.description,
                encode_env(&agent.launch_env),
            ],
        )?;
        tx.execute(
            "INSERT INTO mailbox_cursors (agent_id, delivered_seq) VALUES (?1, 0)",
            [agent.id.as_str()],
        )?;
        tx.commit()?;
        Ok(agent)
    }

    pub fn agent(&self, id: &AgentId) -> Result<Agent, Error> {
        agent_by_id(&self.read_conn(), id)
    }

    pub fn set_agent_admin(&self, agent_id: &AgentId, is_admin: bool) -> Result<(), Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let agent = agent_by_id(&tx, agent_id)?;
        if is_admin && agent.parent_id.is_some() {
            return Err(Error::Message(
                "a subagent cannot hold administrative authority".into(),
            ));
        }
        tx.execute(
            "UPDATE agents SET is_admin = ?2 WHERE id = ?1",
            params![agent_id.as_str(), is_admin],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn set_agent_model(&self, agent_id: &AgentId, model: &str) -> Result<(), Error> {
        self.agent(agent_id)?;
        self.conn().execute(
            "UPDATE agents SET model = ?2 WHERE id = ?1",
            params![agent_id.as_str(), model],
        )?;
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(())
    }

    pub fn set_agent_harness(&self, agent_id: &AgentId, harness: &str) -> Result<Agent, Error> {
        let clean = normalize_harness_name(harness);
        if clean.is_empty() {
            return Err(Error::Message("harness is empty".into()));
        }
        let updated = {
            let conn = self.conn();
            agent_by_id(&conn, agent_id)?;
            conn.execute(
                "UPDATE agents SET harness = ?2 WHERE id = ?1",
                params![agent_id.as_str(), clean],
            )?;
            agent_by_id(&conn, agent_id)?
        };
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(updated)
    }

    /// Rename an agent in place while preserving its immutable `AgentId`,
    /// mailbox, sessions, grants, and recording the old name in `agent_aliases`.
    pub fn rename_agent(&self, agent_id: &AgentId, new_name: &str) -> Result<Agent, Error> {
        let clean = new_name
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(new_name.trim());
        validate_token(clean, "agent name")?;
        let updated = {
            let mut conn = self.conn();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current = agent_by_id(&tx, agent_id)?;
            if current.name == clean {
                return Ok(current);
            }
            let taken: i64 = tx.query_row(
                "SELECT COUNT(*) FROM agents WHERE name = ?1 AND id != ?2",
                params![clean, agent_id.as_str()],
                |row| row.get(0),
            )?;
            if taken != 0 {
                return Err(Error::AgentExists(clean.to_string()));
            }
            tx.execute(
                "UPDATE agents SET name = ?2 WHERE id = ?1",
                params![agent_id.as_str(), clean],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO agent_aliases (alias, agent_id, created_at_ms) VALUES (?1, ?2, ?3)",
                params![current.name, agent_id.as_str(), now_ms()],
            )?;
            tx.execute("DELETE FROM agent_aliases WHERE alias = ?1", [clean])?;
            let _ = tx.execute(
                "UPDATE rag_chunks SET agent_name = ?2 WHERE agent_id = ?1",
                params![agent_id.as_str(), clean],
            );
            let _ = tx.execute(
                "UPDATE rag_chunks_fts SET agent_name = ?2 WHERE agent_name = ?1",
                params![current.name, clean],
            );
            let updated = agent_by_id(&tx, agent_id)?;
            tx.commit()?;
            updated
        };
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(updated)
    }

    /// Set or clear an agent's short role label (e.g. `"Research"`, `"Design"`, `"Support"`).
    pub fn set_agent_role(&self, id: &AgentId, role: Option<&str>) -> Result<Agent, Error> {
        let clean = role.map(str::trim).filter(|s| !s.is_empty());
        let conn = self.conn();
        agent_by_id(&conn, id)?;
        conn.execute(
            "UPDATE agents SET role = ?1 WHERE id = ?2",
            params![clean, id.as_str()],
        )?;
        agent_by_id(&conn, id)
    }

    /// Transfer custody (reparent) of an agent to a new parent agent (or `None` to promote to root).
    /// Rejects self-parenting and cycles in the hierarchy tree.
    pub fn set_agent_parent(
        &self,
        agent_id: &AgentId,
        new_parent_id: Option<&AgentId>,
    ) -> Result<Agent, Error> {
        let updated = {
            let conn = self.conn();
            let current = agent_by_id(&conn, agent_id)?;
            if let Some(np) = new_parent_id {
                if np == agent_id {
                    return Err(Error::Message(
                        "cannot transfer an agent to itself as parent".into(),
                    ));
                }
                let mut cursor = Some(np.clone());
                let mut hops = 0usize;
                while let Some(cid) = cursor {
                    if &cid == agent_id {
                        return Err(Error::Message(
                            "cannot transfer an agent under its own descendant (cycle detected)"
                                .into(),
                        ));
                    }
                    hops += 1;
                    if hops > 64 {
                        break;
                    }
                    let anc = agent_by_id(&conn, &cid)?;
                    cursor = anc.parent_id;
                }
            }
            let parent_str = new_parent_id.map(AgentId::as_str);
            conn.execute(
                "UPDATE agents SET parent_id = ?1 WHERE id = ?2",
                params![parent_str, current.id.as_str()],
            )?;
            agent_by_id(&conn, agent_id)?
        };
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(updated)
    }

    /// Former names recorded for this agent via `rename_agent`.
    pub fn agent_aliases(&self, id: &AgentId) -> Result<Vec<String>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(
            "SELECT alias FROM agent_aliases WHERE agent_id = ?1 ORDER BY created_at_ms DESC",
        )?;
        let mut rows = stmt.query([id.as_str()])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row.get(0)?);
        }
        Ok(out)
    }

    /// Administrative metadata only; callers must never pass credentials here.
    pub fn record_admin_audit(&self, actor: &str, action: &str, target: &str) -> Result<(), Error> {
        self.conn().execute(
            "INSERT INTO admin_audit (created_at, actor, action, target) VALUES (?1, ?2, ?3, ?4)",
            params![now_ms(), actor, action, target],
        )?;
        Ok(())
    }

    /// Set or clear an agent's public capability/charter description (`description` column, Layer 1).
    pub fn set_agent_description(
        &self,
        id: &AgentId,
        description: Option<&str>,
    ) -> Result<Agent, Error> {
        let clean = description.map(str::trim).filter(|s| !s.is_empty());
        let conn = self.conn();
        agent_by_id(&conn, id)?;
        conn.execute(
            "UPDATE agents SET description = ?1 WHERE id = ?2",
            params![clean, id.as_str()],
        )?;
        agent_by_id(&conn, id)
    }

    pub fn set_agent_instructions(
        &self,
        id: &AgentId,
        instructions: Option<&str>,
    ) -> Result<Agent, Error> {
        let clean = instructions.map(str::trim).filter(|s| !s.is_empty());
        let conn = self.conn();
        let current = agent_by_id(&conn, id)?;
        let derived_desc = if current.description.is_none() {
            clean
                .and_then(|raw| raw.lines().next())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|line| line.chars().take(300).collect::<String>())
        } else {
            current.description
        };
        conn.execute(
            "UPDATE agents SET instructions = ?1, description = ?2 WHERE id = ?3",
            params![clean, derived_desc, id.as_str()],
        )?;
        agent_by_id(&conn, id)
    }

    pub fn agent_by_name(&self, name: &str) -> Result<Agent, Error> {
        let lookup = name.strip_prefix("agent:").unwrap_or(name);
        let conn = self.read_conn();
        let found = conn
            .query_row(
                &format!("SELECT {AGENT_COLS} FROM agents WHERE name = ?1 OR id = ?1"),
                [lookup],
                agent_from_row,
            )
            .optional()?;
        if let Some(agent) = found {
            return Ok(agent);
        }
        let aliased = conn
            .query_row(
                "SELECT a.id, a.name, a.harness, a.model, a.workdir, a.instructions, a.state, \
                        a.created_at_ms, a.launch_env, a.parent_id, a.is_admin, a.role, a.description \
                 FROM agent_aliases al \
                 JOIN agents a ON a.id = al.agent_id \
                 WHERE al.alias = ?1",
                [lookup],
                agent_from_row,
            )
            .optional()?;
        aliased.ok_or_else(|| Error::AgentNotFound(name.to_string()))
    }

    pub fn list_agents(&self) -> Result<Vec<Agent>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(&format!("SELECT {AGENT_COLS} FROM agents ORDER BY name"))?;
        let mut rows = stmt.query([])?;
        let mut agents = Vec::new();
        while let Some(row) = rows.next()? {
            agents.push(agent_from_row(row)?);
        }
        Ok(agents)
    }

    /// Load roster metadata with a fixed number of SQL statements. Indexed range
    /// seeks avoid scanning the message history of archived agents.
    pub fn agent_query_metadata(&self) -> Result<AgentQueryMetadata, Error> {
        let mut metadata = AgentQueryMetadata::default();
        {
            let conn = self.read_conn();
            let mut stmt =
                conn.prepare("SELECT agent_id, alias FROM agent_aliases ORDER BY alias")?;
            for row in
                stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            {
                let (id, alias) = row?;
                metadata.aliases.entry(id).or_default().push(alias);
            }
            let mut stmt = conn.prepare(
                "SELECT a.id, (SELECT m.received_at_ms FROM messages m
                   WHERE m.agent_id = a.id AND m.seq > COALESCE(d.delivered_seq, 0)
                     AND (CASE WHEN json_valid(m.context) THEN json_extract(m.context, '$.event') END) IS NOT 'reaction_passive'
                   ORDER BY m.seq LIMIT 1)
                 FROM agents a LEFT JOIN mailbox_cursors d ON d.agent_id = a.id
                 WHERE a.state != 'archived'",
            )?;
            for row in stmt.query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
            })? {
                let (id, timestamp) = row?;
                if let Some(timestamp) = timestamp {
                    metadata.oldest_undelivered.insert(id, timestamp);
                }
            }
            let mut stmt = conn.prepare(
                "SELECT t.agent_id, COALESCE((SELECT m.received_at_ms FROM messages m
                    WHERE m.agent_id = t.agent_id AND m.seq > t.completed_seq AND m.seq <= t.opened_seq
                    ORDER BY m.seq LIMIT 1), t.opened_at_ms)
                 FROM turn_recovery t JOIN agents a ON a.id = t.agent_id
                 WHERE t.opened_seq > t.completed_seq AND a.state != 'archived'",
            )?;
            for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
                let (id, timestamp) = row?;
                metadata.open_turns.insert(id, timestamp);
            }
        }
        for schedule in self.list_schedules(None, true)? {
            metadata
                .schedules
                .entry(schedule.agent_id.to_string())
                .or_default()
                .push(schedule);
        }
        Ok(metadata)
    }

    pub fn children_of(&self, parent_id: &AgentId) -> Result<Vec<Agent>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {AGENT_COLS} FROM agents WHERE parent_id = ?1 ORDER BY created_at_ms, name"
        ))?;
        let mut rows = stmt.query([parent_id.as_str()])?;
        let mut agents = Vec::new();
        while let Some(row) = rows.next()? {
            agents.push(agent_from_row(row)?);
        }
        Ok(agents)
    }

    /// Append one message to every agent in a room: all of them except the
    /// sender, because a room message is not echoed back to its author.
    ///
    /// Each agent gets its own row with its own sequence, so a room message is
    /// one message per mailbox and every guarantee stays per mailbox. A
    /// `source_key` is namespaced per agent, so replaying the same room line is
    /// idempotent per recipient.
    ///
    /// A room named as a Telegram chat (`<chat id>` or `telegram:chat:<id>`)
    /// reaches only the agents whose own bot is attached to that chat, so one
    /// chat's posts never arrive in another chat's mailboxes. Membership comes
    /// from the channel state each bot saved, never from a hand-kept list.
    pub fn receive_room(
        &self,
        room: &str,
        incoming: Incoming,
    ) -> Result<Vec<(Agent, ReceiveOutcome)>, Error> {
        let sender = incoming.sender.clone();
        // Immutable IDs never fall back to a same-spelled display name. Legacy
        // names (with or without agent:) retain their existing alias lookup.
        let author_id = sender.as_deref().and_then(|value| {
            let reference = value.strip_prefix("agent:").unwrap_or(value);
            match AgentId::parse(reference) {
                Ok(id) => Some(id),
                Err(_) => self.agent_by_name(reference).ok().map(|agent| agent.id),
            }
        });
        let members = self.room_members(room)?;
        let mut delivered = Vec::new();
        for agent in self.list_agents()? {
            if agent.state == "archived" || agent.state == "paused" {
                continue;
            }
            if author_id.as_ref() == Some(&agent.id) {
                continue;
            }
            if let Some(members) = &members
                && !members.iter().any(|name| name == &agent.name)
            {
                continue;
            }
            let default_room_reply = incoming.reply_to.clone().or_else(|| {
                let clean = room
                    .strip_prefix("group:")
                    .or_else(|| room.strip_prefix("telegram:chat:"))
                    .or_else(|| room.strip_prefix("chat:"))
                    .unwrap_or(room);
                if clean == "all"
                    || clean == "peers"
                    || clean
                        .split(':')
                        .next()
                        .unwrap_or(clean)
                        .parse::<i64>()
                        .is_ok()
                {
                    None
                } else {
                    Some(format!("group:{clean}"))
                }
            });
            let mut one = Incoming {
                text: incoming.text.clone(),
                sender: sender.clone(),
                source_time_ms: incoming.source_time_ms,
                source_key: incoming
                    .source_key
                    .as_ref()
                    .map(|key| format!("room:{room}:{key}")),
                reply_to: default_room_reply,
                quote: incoming.quote.clone(),
                origin: incoming.origin,
                received_at_ms: incoming.received_at_ms,
            };
            // The body is built once, so every recipient reads the same text.
            one.text = one.body();
            one.quote = None;
            let outcome = self.receive(&agent.id, one)?;
            delivered.push((agent, outcome));
        }
        Ok(delivered)
    }

    /// Who a room reaches, when the room names a chat. `None` is a room that is
    /// not a chat: every agent, as a group by name always was.
    /// Who a room reaches.
    ///
    /// A chat (`<id>`, `chat:<id>`, `telegram:chat:<id>[:thread]`) reaches the
    /// agents whose own bot is attached to that chat. A named group reaches its
    /// own members, and nobody else. A room that is neither is empty rather
    /// than everyone: a name we do not know must never fan out to the world.
    pub fn room_members(&self, room: &str) -> Result<Option<Vec<String>>, Error> {
        let named = room
            .strip_prefix("telegram:chat:")
            .or_else(|| room.strip_prefix("chat:"))
            .unwrap_or(room);
        // A forum topic follows the chat id; the chat is the same either way.
        if let Ok(chat_id) = named.split(':').next().unwrap_or(named).parse::<i64>() {
            return Ok(Some(self.chat_members(chat_id)?));
        }
        let name = room.strip_prefix("group:").unwrap_or(room);
        if name == "all" || name == "peers" {
            let names = self
                .list_agents()?
                .into_iter()
                .filter(|agent| {
                    agent.parent_id.is_none()
                        && agent.state != "archived"
                        && agent.state != "paused"
                })
                .map(|agent| agent.name)
                .collect();
            return Ok(Some(names));
        }
        let members = self.group_members(name)?;
        Ok(Some(members))
    }

    /// The agents whose own bot is attached to one chat.
    pub fn chat_members(&self, chat_id: i64) -> Result<Vec<String>, Error> {
        let mut members = Vec::new();
        for channel in self.list_channels()? {
            for agent_id in &channel.agents {
                // The bot's own saved links say which chats it answers in.
                let linked = telegram_chat_links(&channel.state)
                    .iter()
                    .any(|(_, linked)| *linked == chat_id);
                if !linked {
                    continue;
                }
                if let Ok(agent) = self.agent(agent_id)
                    && !members.contains(&agent.name)
                {
                    members.push(agent.name);
                }
            }
        }
        Ok(members)
    }

    /// Put one agent in a named group. A group exists once it has a member.
    pub fn join_group(&self, name: &str, agent_id: &AgentId) -> Result<(), Error> {
        if name.is_empty() || name.contains(':') || name.chars().any(char::is_whitespace) {
            return Err(Error::Message(format!("invalid group name: {name}")));
        }
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        conn.execute(
            "INSERT OR IGNORE INTO groups (name, created_at_ms) VALUES (?1, ?2)",
            params![name, now_ms()],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO group_agents (group_name, agent_id) VALUES (?1, ?2)",
            params![name, agent_id.as_str()],
        )?;
        Ok(())
    }

    /// Take one agent out of a named group.
    pub fn leave_group(&self, name: &str, agent_id: &AgentId) -> Result<(), Error> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM group_agents WHERE group_name = ?1 AND agent_id = ?2",
            params![name, agent_id.as_str()],
        )?;
        Ok(())
    }

    /// Remove every group membership for an agent. Group names have no lifecycle privilege.
    pub fn leave_all_groups(&self, agent_id: &AgentId) -> Result<(), Error> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM group_agents WHERE agent_id = ?1",
            [agent_id.as_str()],
        )?;
        Ok(())
    }

    /// Set whether a named virtual group is `public` (`true` = open to any public agent;
    /// `false` = private/invite-only; participation requires membership or an invitation).
    pub fn set_group_public(&self, name: &str, public: bool) -> Result<(), Error> {
        if name.is_empty() || name.contains(':') || name.chars().any(char::is_whitespace) {
            return Err(Error::Message(format!("invalid group name: {name}")));
        }
        let conn = self.conn();
        conn.execute(
            "INSERT INTO groups (name, created_at_ms, public) VALUES (?1, ?2, ?3)
             ON CONFLICT(name) DO UPDATE SET public = excluded.public",
            params![name, now_ms(), i32::from(public)],
        )?;
        Ok(())
    }

    /// Whether a named virtual group is `public` (`true`) or `private` (`false`, default).
    pub fn is_group_public(&self, name: &str) -> bool {
        let conn = self.conn();
        conn.query_row("SELECT public FROM groups WHERE name = ?1", [name], |r| {
            Ok(r.get::<_, i64>(0)? != 0)
        })
        .unwrap_or(false)
    }

    /// The agents in a named group, by name. An unknown group is empty.
    pub fn group_members(&self, name: &str) -> Result<Vec<String>, Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT a.name FROM group_agents AS g JOIN agents AS a ON a.id = g.agent_id
             WHERE g.group_name = ?1 ORDER BY a.name",
        )?;
        let mut rows = stmt.query([name])?;
        let mut members = Vec::new();
        while let Some(row) = rows.next()? {
            members.push(row.get(0)?);
        }
        Ok(members)
    }

    /// Every named group and its current members, ordered by group name.
    pub fn list_groups(&self) -> Result<Vec<(String, Vec<String>)>, Error> {
        let names: Vec<String> = {
            let conn = self.conn();
            let mut stmt = conn.prepare("SELECT name FROM groups ORDER BY name")?;
            let mut rows = stmt.query([])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push(row.get(0)?);
            }
            out
        };
        let mut groups = Vec::with_capacity(names.len());
        for name in names {
            let members = self.group_members(&name)?;
            groups.push((name, members));
        }
        Ok(groups)
    }

    pub fn create_channel(&self, new: NewChannel) -> Result<Channel, Error> {
        if new.kind.is_empty() {
            return Err(Error::Message("channel kind is empty".into()));
        }
        let channel = Channel {
            id: ChannelId::generate(),
            kind: new.kind,
            identity: new.identity,
            config: if new.config.is_empty() {
                "{}".to_string()
            } else {
                new.config
            },
            credential_ref: new.credential_ref,
            state: "{}".to_string(),
            created_at_ms: now_ms(),
            agents: Vec::new(),
        };
        self.conn().execute(
            "INSERT INTO channels (id, kind, identity, config, credential_ref, state, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                channel.id.as_str(),
                channel.kind,
                channel.identity,
                channel.config,
                channel.credential_ref,
                channel.state,
                channel.created_at_ms,
            ],
        )?;
        Ok(channel)
    }

    /// Every channel, with the agents attached to it. No secret is in the rows.
    pub fn list_channels(&self) -> Result<Vec<Channel>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(
            "SELECT id, kind, identity, config, credential_ref, created_at_ms, state FROM channels
             ORDER BY created_at_ms",
        )?;
        let mut rows = stmt.query([])?;
        let mut channels = Vec::new();
        while let Some(row) = rows.next()? {
            let mut channel = channel_from_row(row)?;
            channel.agents = agents_of(&conn, &channel.id)?;
            channels.push(channel);
        }
        Ok(channels)
    }

    /// Point a channel at a secret that is already stored. The value stays out.
    pub fn set_credential_ref(
        &self,
        channel_id: &ChannelId,
        credential_ref: &str,
    ) -> Result<(), Error> {
        if credential_ref.is_empty() {
            return Err(Error::Message("credential ref is empty".into()));
        }
        let changed = self.conn().execute(
            "UPDATE channels SET credential_ref = ?2 WHERE id = ?1",
            params![channel_id.as_str(), credential_ref],
        )?;
        if changed == 0 {
            return Err(Error::ChannelNotFound(channel_id.to_string()));
        }
        Ok(())
    }

    /// Replace a channel's adapter memory.
    pub fn set_channel_state(&self, channel_id: &ChannelId, state: &str) -> Result<(), Error> {
        serde_json::from_str::<serde_json::Value>(state)
            .map_err(|_| Error::Message("channel state is not json".into()))?;
        let changed = self.conn().execute(
            "UPDATE channels SET state = ?2 WHERE id = ?1",
            params![channel_id.as_str(), state],
        )?;
        if changed == 0 {
            return Err(Error::ChannelNotFound(channel_id.to_string()));
        }
        Ok(())
    }

    pub fn attach_channel(&self, channel_id: &ChannelId, agent_id: &AgentId) -> Result<(), Error> {
        let conn = self.conn();
        channel_with(&conn, channel_id)?;
        agent_by_id(&conn, agent_id)?;
        conn.execute(
            "INSERT OR IGNORE INTO channel_agents (channel_id, agent_id) VALUES (?1, ?2)",
            params![channel_id.as_str(), agent_id.as_str()],
        )?;
        Ok(())
    }

    /// Update a channel's external identity (e.g. Telegram `@username` or `id:username`).
    pub fn set_channel_identity(
        &self,
        channel_id: &ChannelId,
        identity: Option<&str>,
    ) -> Result<(), Error> {
        let clean = identity.map(str::trim).filter(|s| !s.is_empty());
        let changed = self.conn().execute(
            "UPDATE channels SET identity = ?2 WHERE id = ?1",
            params![channel_id.as_str(), clean],
        )?;
        if changed == 0 {
            return Err(Error::ChannelNotFound(channel_id.to_string()));
        }
        Ok(())
    }

    /// Detach all agents currently bound to `channel_id` (used when re-assigning a channel).
    pub fn detach_channel_agents(&self, channel_id: &ChannelId) -> Result<(), Error> {
        let conn = self.conn();
        channel_with(&conn, channel_id)?;
        conn.execute(
            "DELETE FROM channel_agents WHERE channel_id = ?1",
            params![channel_id.as_str()],
        )?;
        Ok(())
    }

    /// Detach one agent from `channel_id`.
    pub fn detach_channel_agent(
        &self,
        channel_id: &ChannelId,
        agent_id: &AgentId,
    ) -> Result<(), Error> {
        self.conn().execute(
            "DELETE FROM channel_agents WHERE channel_id = ?1 AND agent_id = ?2",
            params![channel_id.as_str(), agent_id.as_str()],
        )?;
        Ok(())
    }

    /// Append one message, or return the row already stored for this source key.
    /// The sequence is assigned inside the insert transaction.
    pub fn receive(&self, agent_id: &AgentId, incoming: Incoming) -> Result<ReceiveOutcome, Error> {
        self.receive_with_wake(agent_id, incoming, false)
    }

    /// Append an intentional, directly addressed send and wake its recipient,
    /// including an archived agent, in the same transaction. The caller must
    /// authenticate the author, exclude fan-out and channel/system events, and
    /// hold the lifecycle lock through clearing any explicit archive marker and
    /// this call. Ordinary ingress must
    /// use `receive`; a duplicate never changes the recipient's lifecycle.
    pub fn receive_direct_send(
        &self,
        agent_id: &AgentId,
        incoming: Incoming,
    ) -> Result<ReceiveOutcome, Error> {
        self.receive_with_wake(agent_id, incoming, true)
    }

    fn receive_with_wake(
        &self,
        agent_id: &AgentId,
        incoming: Incoming,
        wake_archived: bool,
    ) -> Result<ReceiveOutcome, Error> {
        self.receive_with_admission(agent_id, incoming, wake_archived, |_| Ok(true))
            .map(|outcome| outcome.expect("unconditional message admission"))
    }

    fn receive_with_admission(
        &self,
        agent_id: &AgentId,
        incoming: Incoming,
        wake_archived: bool,
        admit: impl FnOnce(&Connection) -> Result<bool, Error>,
    ) -> Result<Option<ReceiveOutcome>, Error> {
        let mut incoming = incoming;
        let source_key = incoming.source_key.take().filter(|key| !key.is_empty());
        let received_at_ms = incoming.received_at_ms.unwrap_or_else(now_ms);
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Author validity shares the write transaction with delivery. A sender
        // deleted while waiting for publication cannot borrow a same-spelled
        // display name or produce a stale-principal mailbox/context receipt.
        if let Some(author) = incoming
            .sender
            .as_deref()
            .map(str::trim)
            .and_then(|sender| sender.strip_prefix("agent:"))
            .and_then(|reference| AgentId::parse(reference).ok())
        {
            agent_by_id(&tx, &author)?;
        }
        if !admit(&tx)? {
            return Ok(None);
        }
        agent_by_id(&tx, agent_id)?;
        // Erasure is authoritative over provider retries/edits, including basic
        // group relays and rows outside the normal bounded origin scan. Refuse
        // resurrection before source dedup, quote/mention enrichment, baseline
        // writes, mailbox allocation or indexing can retain the incoming text.
        let ingress_origin = incoming.origin.map(|mut origin| {
            origin.message_id = origin
                .message_id
                .or_else(|| parse_edited_message_text(&incoming.text).map(|(id, _)| id));
            origin
        });
        if let Some(existing) =
            redacted_message_by_ingress(&tx, agent_id, ingress_origin, source_key.as_deref())?
        {
            // A known alias can reveal another route (for example a new source
            // key on a known origin). Retain that link without changing the
            // canonical message or audit, so a source-only retry stays erased.
            retain_message_aliases(&tx, &existing, ingress_origin, source_key.as_deref())?;
            tx.commit()?;
            return Ok(Some(ReceiveOutcome::Duplicate(existing)));
        }
        if let Some(key) = &source_key
            && let Some(existing) = message_by_source(&tx, agent_id, key)?
        {
            retain_message_aliases(&tx, &existing, ingress_origin, Some(key))?;
            tx.commit()?;
            return Ok(Some(ReceiveOutcome::Duplicate(existing)));
        }
        if let Some(ref mut quote) = incoming.quote {
            enrich_quote_from_store(&tx, quote, incoming.origin);
        }
        let mut body = incoming.body();
        let mut reply_to_author = incoming
            .quote
            .as_ref()
            .and_then(|quote| resolve_quote_author(&tx, quote));
        if incoming.text.starts_with("[Reaction ")
            && let Some(origin) = incoming.origin
            && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            && let Some((orig_sender, orig_text)) =
                resolve_origin_message_author_and_excerpt(&tx, chat_id, message_id)
        {
            if reply_to_author.is_none() {
                reply_to_author = Some(orig_sender.clone());
            }
            if incoming.quote.is_none() && !orig_text.is_empty() {
                let synth_quote = crate::Quote {
                    author: orig_sender.clone(),
                    author_is_bot: orig_sender.starts_with("agent:"),
                    sender_id: None,
                    text: orig_text,
                    reference: Some(crate::QuoteRef {
                        chat_id,
                        message_id,
                    }),
                    mentions: Vec::new(),
                };
                body = crate::quote::compose(&incoming.text, Some(&synth_quote));
            }
        }
        let mut redelivery_identity = None;
        if let Some(origin) = incoming.origin
            && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            && !incoming.text.starts_with("[Edited ")
            && let Some(existing) = message_by_origin(
                &tx,
                agent_id,
                chat_id,
                message_id,
                incoming.sender.as_deref(),
                incoming.source_time_ms,
                &body,
                source_key.as_deref(),
                reply_to_author.as_deref(),
                &mut redelivery_identity,
            )?
        {
            tx.commit()?;
            return Ok(Some(ReceiveOutcome::Duplicate(existing)));
        }
        if let Some((edited_msg_id, new_raw_text)) = parse_edited_message_text(&incoming.text) {
            let msg_id = incoming
                .origin
                .and_then(|o| o.message_id)
                .unwrap_or(edited_msg_id);
            if let Some(chat_id) = incoming.origin.and_then(|o| o.chat_id) {
                let prev_text = resolve_previous_message_version(
                    &tx,
                    agent_id,
                    chat_id,
                    msg_id,
                    source_key.as_deref(),
                );
                let _ = tx.execute(
                    "INSERT INTO message_versions (agent_id, chat_id, message_id, body, updated_at_ms, origin_namespace)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT (agent_id, chat_id, message_id) DO UPDATE SET
                         body = excluded.body,
                         updated_at_ms = excluded.updated_at_ms
                     WHERE message_versions.origin_namespace = excluded.origin_namespace",
                    params![
                        agent_id.as_str(),
                        chat_id,
                        msg_id,
                        new_raw_text.trim(),
                        received_at_ms,
                        origin_namespace(chat_id, source_key.as_deref())
                    ],
                );
                if let Some(prev) = prev_text {
                    if prev.trim() == new_raw_text.trim()
                        && let Some(existing) = latest_message_for_origin(
                            &tx,
                            agent_id,
                            chat_id,
                            msg_id,
                            source_key.as_deref(),
                        )?
                    {
                        retain_message_aliases(
                            &tx,
                            &existing,
                            ingress_origin,
                            source_key.as_deref(),
                        )?;
                        tx.commit()?;
                        return Ok(Some(ReceiveOutcome::Duplicate(existing)));
                    }
                    let diff = format_edited_message_diff(&prev, new_raw_text);
                    let formatted_edit = format!("[Edited message #{msg_id}]:\n{diff}");
                    body = match incoming.quote.as_ref() {
                        Some(q) => crate::quote::compose(&formatted_edit, Some(q)),
                        None => formatted_edit,
                    };
                }
            }
        } else if let Some(origin) = incoming.origin
            && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            && !incoming.text.starts_with("[Reaction ")
            && !incoming.text.starts_with("[Deleted message #")
            && !incoming.text.starts_with("[Pinned message #")
            && !incoming.text.starts_with("[pinned message")
            && !incoming.text.starts_with("[Member ")
        {
            let _ = tx.execute(
                "INSERT INTO message_versions (agent_id, chat_id, message_id, body, updated_at_ms, origin_namespace)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT (agent_id, chat_id, message_id) DO NOTHING",
                params![
                    agent_id.as_str(),
                    chat_id,
                    message_id,
                    incoming.text.trim(),
                    received_at_ms,
                    origin_namespace(chat_id, source_key.as_deref())
                ],
            );
        }
        let mentions = resolve_mentions(&tx, &incoming.text);
        let recipient_agent = agent_by_id(&tx, agent_id).ok();
        let recipient = recipient_agent
            .as_ref()
            .map(|a| format!("agent:{}", a.name));
        let chat_id_val = incoming.origin.and_then(|origin| origin.chat_id);
        let (chat_title, chat_type) = match chat_id_val {
            Some(cid) => resolve_chat_metadata(&tx, cid),
            None => (None, None),
        };
        let event = if incoming.text.starts_with("[Pinned message #")
            || incoming.text.starts_with("[pinned message")
        {
            Some("pinned".to_string())
        } else if incoming.text.starts_with("[Edited message #") {
            Some("edited".to_string())
        } else if incoming.text.starts_with("[Deleted message #") {
            Some("deleted".to_string())
        } else if incoming.text.starts_with("[Reaction ") {
            let is_targeted_reaction = chat_id_val.is_none_or(|cid| cid > 0)
                || recipient_agent.as_ref().is_some_and(|a| {
                    let agent_tag = format!("agent:{}", a.name);
                    let bracket_tag = format!("[agent:{}]", a.name);
                    reply_to_author.as_deref().is_some_and(|rta| {
                        rta.eq_ignore_ascii_case(&agent_tag)
                            || rta.ends_with(&bracket_tag)
                            || rta.eq_ignore_ascii_case(&a.name)
                    })
                })
                || chat_id_val.is_some_and(|cid| chat_attached_agent_count(&tx, cid) <= 1);
            if is_targeted_reaction {
                Some("reaction".to_string())
            } else {
                Some("reaction_passive".to_string())
            }
        } else if incoming.text.starts_with("[Member joined group:")
            || incoming.text.starts_with("[Added to group chat")
        {
            Some("member_joined".to_string())
        } else if incoming.text.starts_with("[Member removed/left group:")
            || incoming.text.starts_with("[Removed from group chat")
        {
            Some("member_left".to_string())
        } else if incoming.text.starts_with("[Sub-Agent Completed]") {
            Some("subagent_completed".to_string())
        } else {
            None
        };
        let resolved_destination = incoming.reply_to.clone().or_else(|| {
            source_key
                .as_deref()
                .and_then(|k| k.strip_prefix("room:"))
                .and_then(|rest| rest.split(':').next())
                .map(|room| {
                    if room.starts_with("group:") || room.starts_with("telegram:") {
                        room.to_string()
                    } else {
                        format!("group:{room}")
                    }
                })
                .or_else(|| {
                    incoming
                        .sender
                        .as_deref()
                        .filter(|s| s.starts_with("agent:"))
                        .map(str::to_string)
                })
        });
        let participants =
            resolve_conversation_participants(&tx, chat_id_val, resolved_destination.as_deref());
        let (sender_role, sender_rel) = resolve_sender_relationship_and_role(
            &tx,
            recipient_agent.as_ref(),
            incoming.sender.as_deref(),
        );
        let topology = if event.as_deref() == Some("reaction_passive") {
            None
        } else {
            compute_and_diff_topology_event(&tx, recipient_agent.as_ref(), received_at_ms)
        };
        // The trusted header is built from the record that arrives, before any
        // field is moved into the row: the author, the channel, the chat and
        // topic, and the reply a bare answer reaches.
        let context = anyagents_harness::Context {
            sender: incoming.sender.clone(),
            sender_name: None,
            sender_role,
            sender_rel,
            recipient,
            channel: incoming
                .sender
                .as_deref()
                .filter(|sender| sender.starts_with("telegram:"))
                .map(|_| "telegram".to_string())
                .or_else(|| chat_id_val.map(|_| "telegram".to_string())),
            chat_id: chat_id_val,
            chat_title,
            chat_type,
            thread_id: incoming.origin.and_then(|origin| origin.thread_id),
            message_id: incoming.origin.and_then(|origin| origin.message_id),
            destination: resolved_destination,
            event,
            reply_to_author,
            mentions,
            participants,
            topology,
        };
        let message = Message {
            id: MessageId::generate(),
            agent_id: agent_id.clone(),
            seq: next_seq(&tx, agent_id)?,
            text: body,
            sender: incoming.sender,
            source_time_ms: incoming.source_time_ms,
            source_key,
            reply_to: incoming.reply_to,
            received_at_ms,
            ack: Ack::Received,
            delivered_at_ms: None,
            origin: incoming.origin,
            context,
        };
        tx.execute(
            "INSERT INTO messages (
                id, agent_id, seq, source_time_ms, source_key, sender, body, reply_to, received_at_ms, origin, context
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                message.id.as_str(),
                message.agent_id.as_str(),
                message.seq,
                message.source_time_ms,
                message.source_key,
                message.sender,
                message.text,
                message.reply_to,
                message.received_at_ms,
                message
                    .origin
                    .map(|origin| serde_json::to_string(&origin).unwrap_or_default()),
                serde_json::to_string(&message.context).ok(),
            ],
        )?;
        if let Some(identity_id) = redelivery_identity {
            tx.execute(
                "INSERT INTO message_identities (agent_id, target_id, identity_id) VALUES (?1, ?2, ?3)",
                params![agent_id.as_str(), message.id.as_str(), identity_id],
            )?;
        }
        retain_message_aliases(&tx, &message, ingress_origin, message.source_key.as_deref())?;
        let is_passive_reaction = message.context.event.as_deref() == Some("reaction_passive");
        let woke_agent = if wake_archived {
            tx.execute(
                "UPDATE agents SET state = 'running'
                 WHERE id = ?1 AND state IN ('idle', 'created', 'archived')",
                [agent_id.as_str()],
            )? > 0
        } else if is_passive_reaction {
            false
        } else {
            tx.execute(
                "UPDATE agents SET state = 'running' WHERE id = ?1 AND state = 'idle'",
                [agent_id.as_str()],
            )? > 0
        };
        let agent = agent_by_id(&tx, agent_id)?;
        index_rag_chunk(
            &tx,
            message.id.as_str(),
            message.agent_id.as_str(),
            &agent.name,
            "in",
            message.sender.as_deref().unwrap_or("unknown"),
            message
                .reply_to
                .as_deref()
                .unwrap_or(&format!("agent:{}", agent.name)),
            message.received_at_ms,
            &message.text,
            None,
            None,
            message.seq,
        )?;
        tx.commit()?;
        drop(conn);
        if woke_agent {
            self.notify(Wake::Agent {
                agent_id: agent_id.clone(),
            });
        }
        if !is_passive_reaction {
            self.notify(Wake::Mail {
                agent_id: agent_id.clone(),
            });
        }
        Ok(Some(ReceiveOutcome::New(message)))
    }

    /// Replace the harness launch environment, preserving communication policy.
    /// Only `set_agent_scope` owns `ANYAGENTS_SCOPE` / `ANYAGENTS_PUBLIC`;
    /// gateway refreshes and stale environment snapshots must not overwrite them.
    /// Never called with a provider key.
    pub fn set_agent_launch_env(
        &self,
        id: &AgentId,
        env: &[(String, String)],
    ) -> Result<Agent, Error> {
        self.update_agent_launch_env(id, None, |current| {
            let mut refreshed: Vec<_> = env
                .iter()
                .filter(|(key, _)| !is_policy_env_key(key))
                .cloned()
                .collect();
            refreshed.extend(
                current
                    .launch_env
                    .iter()
                    .filter(|(key, _)| is_policy_env_key(key))
                    .cloned(),
            );
            refreshed
        })
    }

    fn update_agent_launch_env(
        &self,
        id: &AgentId,
        audit: Option<(&str, &str)>,
        update: impl FnOnce(&Agent) -> Vec<(String, String)>,
    ) -> Result<Agent, Error> {
        let changed = {
            let mut conn = self.conn();
            // Read and merge under the same write transaction, including when
            // callers use separate Store connections.
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current = agent_by_id(&tx, id)?;
            let json = encode_env(&update(&current));
            // A database written before this column existed still reads: the
            // harness then keeps whatever sign-in it already has.
            let updated = tx
                .execute(
                    "UPDATE agents SET launch_env = ?1 WHERE id = ?2",
                    params![json, id.as_str()],
                )
                .map_err(|err| match err {
                    rusqlite::Error::SqliteFailure(_, Some(ref text))
                        if text.contains("no column named launch_env") =>
                    {
                        Error::Message(
                            "this database has no launch environment column yet".to_string(),
                        )
                    }
                    other => other.into(),
                })?;
            if updated == 0 {
                return Err(Error::AgentNotFound(id.to_string()));
            }
            let changed = agent_by_id(&tx, id)?;
            if let Some((actor, source)) = audit {
                tx.execute(
                    "INSERT INTO admin_audit (created_at, actor, action, target, before_scope, after_scope, source)
                     VALUES (?1, ?2, 'agents.scope', ?3, ?4, ?5, ?6)",
                    params![now_ms(), actor, changed.name, current.scope(), changed.scope(), source],
                )?;
            }
            tx.commit()?;
            changed
        };
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(changed)
    }

    /// Store the work directory and mark the agent running.
    pub fn set_agent_running(&self, id: &AgentId, workdir: &str) -> Result<Agent, Error> {
        let changed = {
            let conn = self.conn();
            let updated = conn.execute(
                "UPDATE agents SET workdir = ?1, state = 'running' WHERE id = ?2",
                params![workdir, id.as_str()],
            )?;
            if updated == 0 {
                return Err(Error::AgentNotFound(id.to_string()));
            }
            agent_by_id(&conn, id)?
        };
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(changed)
    }

    /// Update an agent's lifecycle state (`running`, `idle`, `paused`, `archived`, `stopped`) and
    /// notify the supervisor so it starts or stops the worker slot.
    pub fn set_agent_state(&self, id: &AgentId, state: &str) -> Result<Agent, Error> {
        let changed = {
            let conn = self.conn();
            let updated = conn.execute(
                "UPDATE agents SET state = ?1 WHERE id = ?2",
                params![state, id.as_str()],
            )?;
            if updated == 0 {
                return Err(Error::AgentNotFound(id.to_string()));
            }
            agent_by_id(&conn, id)?
        };
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(changed)
    }

    /// Atomically transition any running agent to
    /// `idle` (process released / scaled to zero) when it has delivered every
    /// message in its mailbox, completed its latest turn
    /// (`completed_seq >= opened_seq >= max_seq > 0`), and has no active
    /// (`running`) child sub-agents of its own.
    ///
    /// Returns `Ok(true)` when the agent transitioned to `idle` (and
    /// notifies the supervisor to stop its worker slot), or `Ok(false)` if new
    /// mail arrived, a turn is still open, or a child sub-agent is still running.
    pub fn complete_idle_agent(&self, id: &AgentId) -> Result<bool, Error> {
        let err_file =
            crate::agent_config_dir(self.data_dir(), id).join(".anyagents-last-error.json");
        if let Ok(raw) = std::fs::read_to_string(&err_file)
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            && val
                .get("error")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        {
            return Ok(false);
        }
        let (transitioned, parent_to_wake, silent_completion) = {
            let mut conn = self.conn();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let parent_id_opt: Option<AgentId> = tx
                .query_row(
                    "SELECT parent_id FROM agents WHERE id = ?1 AND parent_id IS NOT NULL",
                    [id.as_str()],
                    |r| {
                        let pid: String = r.get(0)?;
                        AgentId::parse(&pid)
                            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
                    },
                )
                .optional()?;
            let silent_info: Option<(AgentId, String, i64)> = tx
                .query_row(
                    "SELECT a.parent_id, a.name, tr.completed_seq
                     FROM agents a
                     JOIN agents p ON p.id = a.parent_id
                     JOIN turn_recovery tr ON tr.agent_id = a.id
                     WHERE a.id = ?1
                       AND a.parent_id IS NOT NULL
                       AND a.state = 'running'
                       AND EXISTS (
                           SELECT 1 FROM messages m
                           WHERE m.agent_id = ?1
                             AND m.seq >= tr.opened_seq
                             AND m.sender = 'agent:' || p.name
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM outbound_messages om
                           WHERE om.agent_id = ?1
                             AND om.sent_at_ms >= tr.opened_at_ms
                       )",
                    [id.as_str()],
                    |r| {
                        let pid: String = r.get(0)?;
                        let parsed = AgentId::parse(&pid).map_err(|err| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                        })?;
                        let name: String = r.get(1)?;
                        let cseq: i64 = r.get(2)?;
                        Ok((parsed, name, cseq))
                    },
                )
                .optional()?;
            let updated = tx.execute(
                "UPDATE agents
                 SET state = 'idle'
                 WHERE id = ?1
                   AND state = 'running'
                   AND NOT EXISTS (
                       SELECT 1 FROM agents child
                       WHERE child.parent_id = ?1
                         AND child.state = 'running'
                   )
                   AND EXISTS (
                       SELECT 1 FROM turn_recovery tr
                       WHERE tr.agent_id = ?1
                         AND tr.completed_seq >= tr.opened_seq
                         AND tr.completed_seq > 0
                         AND tr.completed_seq >= COALESCE((
                             SELECT MAX(m.seq) FROM messages m WHERE m.agent_id = ?1
                         ), 0)
                   )
                   AND COALESCE((
                       SELECT mc.delivered_seq FROM mailbox_cursors mc WHERE mc.agent_id = ?1
                   ), 0) >= COALESCE((
                       SELECT MAX(m.seq) FROM messages m WHERE m.agent_id = ?1
                   ), 0)",
                [id.as_str()],
            )?;
            tx.commit()?;
            (
                updated > 0,
                if updated > 0 { parent_id_opt } else { None },
                if updated > 0 { silent_info } else { None },
            )
        };
        if transitioned {
            self.notify(Wake::Agent {
                agent_id: id.clone(),
            });
            if let Some(pid) = parent_to_wake {
                self.notify(Wake::Agent { agent_id: pid });
            }
            if let Some((parent_id, sub_name, completed_seq)) = silent_completion {
                let notice = format!(
                    "[Sub-Agent Completed] Sub-agent `agent:{sub_name}` (`{id}`) finished its turn and scaled to zero (`state = idle`) without sending a direct reply. Call `subagents({{ \"action\": \"inspect\", \"agent\": \"{sub_name}\" }})` to read its output or `subagents({{ \"action\": \"steer\", \"agent\": \"{sub_name}\", \"prompt\": \"...\" }})` to continue."
                );
                let _ = self.receive(
                    &parent_id,
                    Incoming {
                        text: notice,
                        sender: Some(format!("agent:{sub_name}")),
                        source_time_ms: Some(now_ms()),
                        source_key: Some(format!("subagent_completed:{id}:{completed_seq}")),
                        reply_to: Some(format!("agent:{sub_name}")),
                        quote: None,
                        origin: None,
                        received_at_ms: None,
                    },
                );
            }
        }
        Ok(transitioned)
    }

    /// Compatibility alias; idle release now applies to roots and children alike.
    pub fn complete_idle_subagent(&self, id: &AgentId) -> Result<bool, Error> {
        self.complete_idle_agent(id)
    }

    /// Permanently delete an agent and its associated records from the store.
    pub fn delete_agent(&self, id: &AgentId) -> Result<Agent, Error> {
        let deleted = {
            let mut conn = self.conn();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let agent = agent_by_id(&tx, id)?;
            for table in [
                "agent_aliases",
                "agent_capabilities",
                "usage_records",
                "mailbox_cursors",
                "turn_recovery",
                "messages",
                "message_versions",
                "outbound_messages",
                "rag_chunks",
                "group_agents",
                "channel_agents",
            ] {
                tx.execute(
                    &format!("DELETE FROM {table} WHERE agent_id = ?1"),
                    [id.as_str()],
                )?;
            }
            let _ = tx.execute(
                "DELETE FROM rag_chunks_fts WHERE agent_name = ?1",
                [agent.name.as_str()],
            );
            tx.execute("DELETE FROM agents WHERE id = ?1", [id.as_str()])?;
            tx.commit()?;
            agent
        };
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(deleted)
    }

    /// Messages with `seq > since`, in sequence order. `None` starts at the beginning.
    pub fn inbox(&self, agent_id: &AgentId, since: Option<i64>) -> Result<Vec<Message>, Error> {
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        let delivered = cursor(&conn, agent_id)?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {MESSAGE_COLS}
             FROM messages WHERE agent_id = ?1 AND seq > ?2 ORDER BY seq"
        ))?;
        let mut rows = stmt.query(params![agent_id.as_str(), since.unwrap_or(i64::MIN)])?;
        let mut messages = Vec::new();
        while let Some(row) = rows.next()? {
            messages.push(message_from_row_with_cursor(row, delivered)?);
        }
        Ok(messages)
    }

    /// Latest matching messages, limited in SQLite and returned in sequence order.
    pub fn inbox_query(
        &self,
        agent_id: &AgentId,
        since: Option<i64>,
        limit: Option<usize>,
        unread_only: bool,
    ) -> Result<Vec<Message>, Error> {
        self.inbox_query_filtered(agent_id, since, limit, unread_only, None)
    }

    pub fn inbox_query_filtered(
        &self,
        agent_id: &AgentId,
        since: Option<i64>,
        limit: Option<usize>,
        unread_only: bool,
        sender: Option<&str>,
    ) -> Result<Vec<Message>, Error> {
        let conn = self.read_conn();
        agent_by_id(&conn, agent_id)?;
        let delivered = cursor(&conn, agent_id)?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {MESSAGE_COLS} FROM messages
             WHERE agent_id = ?1 AND seq > ?2
               AND (?3 IS NULL OR sender = ?3)
             ORDER BY seq DESC LIMIT ?4"
        ))?;
        let lower = since
            .unwrap_or(i64::MIN)
            .max(if unread_only { delivered } else { i64::MIN });
        let mut rows = stmt.query(params![
            agent_id.as_str(),
            lower,
            sender,
            limit
                .map(|n| i64::try_from(n).unwrap_or(i64::MAX))
                .unwrap_or(-1)
        ])?;
        let mut messages = Vec::new();
        while let Some(row) = rows.next()? {
            messages.push(message_from_row_with_cursor(row, delivered)?);
        }
        messages.reverse();
        Ok(messages)
    }

    /// Record an outbound message sent by `agent_id` to `destination` (a user,
    /// a channel chat/topic, a group, or another agent) so the agent's full
    /// two-way conversation can be read by authorized supervising agents.
    pub fn record_outbound(
        &self,
        agent_id: &AgentId,
        destination: &str,
        reply_to: Option<&str>,
        body: &str,
    ) -> Result<String, Error> {
        self.record_outbound_id(
            format!("out_{}", MessageId::generate()),
            agent_id,
            destination,
            reply_to,
            body,
        )
    }

    /// Settle the history of an already-completed effect without repeating it.
    /// Keys are scoped to the immutable sender. Exact retries keep the original
    /// ID/time; changed destination, reply or body refuses without mutation.
    pub fn record_outbound_once(
        &self,
        effect_key: &str,
        agent_id: &AgentId,
        destination: &str,
        reply_to: Option<&str>,
        body: &str,
    ) -> Result<String, Error> {
        if effect_key.trim().is_empty() {
            return Err(Error::Message("empty outbound effect key".into()));
        }
        let id = format!(
            "out_{}",
            crate::learned_memory::sha256(format!("{agent_id}:{effect_key}").as_bytes())
        );
        self.record_outbound_id(id, agent_id, destination, reply_to, body)
    }

    fn record_outbound_id(
        &self,
        id: String,
        agent_id: &AgentId,
        destination: &str,
        reply_to: Option<&str>,
        body: &str,
    ) -> Result<String, Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let agent = agent_by_id(&tx, agent_id)?;
        let ts_ms = now_ms();
        tx.execute(
            "INSERT INTO outbound_messages (id, agent_id, destination, reply_to, body, sent_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(id) DO NOTHING",
            params![id, agent_id.as_str(), destination, reply_to, body, ts_ms,],
        )?;
        let (matches, ts_ms): (bool, i64) = tx.query_row(
            "SELECT agent_id=?2 AND destination=?3 AND reply_to IS ?4 AND body=?5, sent_at_ms
             FROM outbound_messages WHERE id=?1",
            params![id, agent_id.as_str(), destination, reply_to, body],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if !matches {
            return Err(Error::Message(
                "outbound effect key payload conflict".into(),
            ));
        }
        let cur_seq: i64 = tx.query_row(
            "SELECT COALESCE((SELECT seq FROM rag_chunks WHERE agent_id=?1 AND chunk_id=?2),
                (SELECT COALESCE(MAX(seq),0) FROM messages WHERE agent_id=?1))",
            params![agent_id.as_str(), id],
            |r| r.get(0),
        )?;
        index_rag_chunk(
            &tx,
            &id,
            agent_id.as_str(),
            &agent.name,
            "out",
            &format!("agent:{}", agent.name),
            destination,
            ts_ms,
            body,
            None,
            None,
            cur_seq,
        )?;
        tx.commit()?;
        Ok(id)
    }

    /// Record a tool invocation or tool output in the history search index (`kind = 'tool'`)
    /// so agents can search with `include_tools` (`true` / `false`), `only_tools` (`true`),
    /// or `tool_name` (`"browser"`, `"agents"`, `"schedule"`, `"send"`, `"bash"`, etc.).
    pub fn record_tool_event(
        &self,
        agent_id: &AgentId,
        tool_name: &str,
        summary: &str,
    ) -> Result<String, Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let agent = agent_by_id(&tx, agent_id)?;
        let id = format!("tool_{}", MessageId::generate());
        let ts_ms = now_ms();
        let clean_tool = tool_name.trim().to_ascii_lowercase();
        let summary = crate::masking::mask_known_secrets(summary);
        let text = if summary.trim().starts_with("[tool") {
            summary.trim().to_string()
        } else {
            format!("[tool:{clean_tool}] {}", summary.trim())
        };
        let cur_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM messages WHERE agent_id = ?1",
            [agent_id.as_str()],
            |r| r.get(0),
        )?;
        index_rag_chunk(
            &tx,
            &id,
            agent_id.as_str(),
            &agent.name,
            "out",
            &format!("agent:{}", agent.name),
            &format!("tool:{clean_tool}"),
            ts_ms,
            &text,
            Some("tool"),
            Some(&clean_tool),
            cur_seq,
        )?;
        tx.commit()?;
        Ok(id)
    }

    pub fn turn_ownership_snapshot(
        &self,
        agent_id: &AgentId,
    ) -> Result<TurnOwnershipSnapshot, Error> {
        let conn = self.conn();
        let last_in: Option<(i64, i64, Option<String>, Option<String>)> = conn
            .query_row(
                "SELECT seq, delivered_at_ms, sender, source_key
                 FROM messages
                 WHERE agent_id = ?1 AND delivered_at_ms IS NOT NULL
                 ORDER BY seq DESC LIMIT 1",
                [agent_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let last_outbound_at_ms: Option<i64> = conn.query_row(
            "SELECT MAX(sent_at_ms) FROM outbound_messages WHERE agent_id = ?1",
            [agent_id.as_str()],
            |row| row.get(0),
        )?;
        Ok(match last_in {
            Some((seq, delivered_at_ms, sender, source_key)) => TurnOwnershipSnapshot {
                last_delivered_seq: Some(seq),
                last_delivered_at_ms: Some(delivered_at_ms),
                last_delivered_sender: sender,
                last_delivered_source_key: source_key,
                last_outbound_at_ms,
            },
            None => TurnOwnershipSnapshot {
                last_delivered_seq: None,
                last_delivered_at_ms: None,
                last_delivered_sender: None,
                last_delivered_source_key: None,
                last_outbound_at_ms,
            },
        })
    }

    pub fn has_message_source_key(
        &self,
        agent_id: &AgentId,
        source_key: &str,
    ) -> Result<bool, Error> {
        let conn = self.conn();
        let exists: i64 = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE agent_id = ?1 AND source_key = ?2)",
            params![agent_id.as_str(), source_key],
            |row| row.get(0),
        )?;
        Ok(exists != 0)
    }

    /// Read the complete two-way (`in` + `out`) conversation of `agent_id` across
    /// users, external channels, internal groups, and other agents, ordered
    /// chronologically. Optional `with_peer` filters to a specific user/chat/group/agent.
    pub fn conversation(
        &self,
        agent_id: &AgentId,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        self.conversation_scoped(agent_id, with_peer, since_ms, limit, None)
    }

    /// Read one agent's conversation after applying the caller's authorized
    /// agent set. The optional set is an authorization boundary, not a result
    /// filter: a target outside it is an empty internal result and is rejected
    /// by the API before this method is called.
    pub fn conversation_scoped(
        &self,
        agent_id: &AgentId,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
        visible_agent_ids: Option<&[String]>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        let conn = self.read_conn();
        let agent = agent_by_id(&conn, agent_id)?;
        if let Some(visible) = visible_agent_ids
            && !visible.iter().any(|name| name == agent.id.as_str())
        {
            return Ok(Vec::new());
        }
        let delivered = cursor(&conn, agent_id)?;
        let self_addr = format!("agent:{}", agent.name);
        let since = since_ms.unwrap_or(0);
        let peer_filter = with_peer.map(str::trim).filter(|s| !s.is_empty());
        let mut turns = Vec::new();

        // Fast SQL-pushed LIMIT when no post-query peer filter is active.
        let sql_limit = if peer_filter.is_none() {
            limit.map(|l| i64::try_from(l.max(1)).unwrap_or(i64::MAX))
        } else {
            None
        };

        // 1. Inbound messages received by this agent.
        let in_sql = match (sql_limit, since_ms.is_none()) {
            (Some(_), true) => format!(
                "SELECT {MESSAGE_COLS}
                 FROM messages WHERE agent_id = ?1 AND received_at_ms > ?2
                 ORDER BY received_at_ms DESC, seq DESC LIMIT ?3"
            ),
            (Some(_), false) => format!(
                "SELECT {MESSAGE_COLS}
                 FROM messages WHERE agent_id = ?1 AND received_at_ms > ?2
                 ORDER BY received_at_ms ASC, seq ASC LIMIT ?3"
            ),
            (None, _) => format!(
                "SELECT {MESSAGE_COLS}
                 FROM messages WHERE agent_id = ?1 AND received_at_ms > ?2
                 ORDER BY received_at_ms, seq"
            ),
        };
        let mut in_stmt = conn.prepare(&in_sql)?;
        let mut in_rows = if let Some(lim) = sql_limit {
            in_stmt.query(params![agent_id.as_str(), since, lim])?
        } else {
            in_stmt.query(params![agent_id.as_str(), since])?
        };
        while let Some(row) = in_rows.next()? {
            let msg = message_from_row_with_cursor(row, delivered)?;
            let from = msg.sender.unwrap_or_else(|| "unknown".to_string());
            let to = msg
                .reply_to
                .clone()
                .filter(|r| r.starts_with("telegram:chat:") || r.starts_with("group:"))
                .unwrap_or_else(|| self_addr.clone());
            turns.push(ConversationTurn {
                seq: msg.seq,
                id: msg.id.to_string(),
                direction: "in".to_string(),
                from,
                to,
                reply_to: msg.reply_to,
                text: msg.text,
                timestamp_ms: msg.received_at_ms,
            });
        }

        // 2. Outbound messages sent by this agent.
        let out_sql = match (sql_limit, since_ms.is_none()) {
            (Some(_), true) => {
                "SELECT id, destination, reply_to, body, sent_at_ms, COALESCE((SELECT seq FROM rag_chunks c WHERE c.chunk_id = outbound_messages.id), 0)
                 FROM outbound_messages WHERE agent_id = ?1 AND sent_at_ms > ?2
                 ORDER BY sent_at_ms DESC, id DESC LIMIT ?3"
            }
            (Some(_), false) => {
                "SELECT id, destination, reply_to, body, sent_at_ms, COALESCE((SELECT seq FROM rag_chunks c WHERE c.chunk_id = outbound_messages.id), 0)
                 FROM outbound_messages WHERE agent_id = ?1 AND sent_at_ms > ?2
                 ORDER BY sent_at_ms ASC, id ASC LIMIT ?3"
            }
            (None, _) => {
                "SELECT id, destination, reply_to, body, sent_at_ms, COALESCE((SELECT seq FROM rag_chunks c WHERE c.chunk_id = outbound_messages.id), 0)
                 FROM outbound_messages WHERE agent_id = ?1 AND sent_at_ms > ?2
                 ORDER BY sent_at_ms, id"
            }
        };
        let mut out_stmt = conn.prepare(out_sql)?;
        let mut out_rows = if let Some(lim) = sql_limit {
            out_stmt.query(params![agent_id.as_str(), since, lim])?
        } else {
            out_stmt.query(params![agent_id.as_str(), since])?
        };
        while let Some(row) = out_rows.next()? {
            turns.push(ConversationTurn {
                seq: row.get(5)?,
                id: row.get(0)?,
                direction: "out".to_string(),
                from: self_addr.clone(),
                to: row.get(1)?,
                reply_to: row.get(2)?,
                text: row.get(3)?,
                timestamp_ms: row.get(4)?,
            });
        }

        if let Some(peer) = peer_filter {
            turns.retain(|turn| matches_conversation_peer(turn, peer));
        }

        turns.sort_by(|a, b| {
            a.timestamp_ms
                .cmp(&b.timestamp_ms)
                .then_with(|| a.id.cmp(&b.id))
        });

        if let Some(limit) = limit {
            if since_ms.is_none() && turns.len() > limit {
                turns.drain(..turns.len() - limit);
            } else if turns.len() > limit {
                turns.truncate(limit);
            }
        }
        Ok(turns)
    }

    /// Read the recent chronological conversation across all agents.
    pub fn conversation_all(
        &self,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        self.conversation_all_scoped(with_peer, since_ms, limit, None)
    }

    /// Read recent turns across an authorized set of agents. `visible_agent_ids`
    /// is pushed into SQLite so a limited caller never relies on a post-query
    /// truncation that could omit an authorized hit behind another agent's
    /// rows.
    pub fn conversation_all_scoped(
        &self,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
        visible_agent_ids: Option<&[String]>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        let conn = self.read_conn();
        let since = since_ms.unwrap_or(0);
        let lim = limit
            .map(|n| i64::try_from(n).unwrap_or(i64::MAX))
            .unwrap_or(i64::MAX / 8);
        let peer_filter = with_peer.map(str::trim).filter(|s| !s.is_empty());
        let fetch_limit = if peer_filter.is_some() {
            lim.saturating_mul(4)
        } else {
            lim
        };
        let visibility_sql = visible_agent_ids_sql("agent_id", visible_agent_ids, 3);
        let sql = if since_ms.is_some() {
            format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, seq
                 FROM rag_chunks
                 WHERE timestamp_ms > ?1 {visibility_sql}
                 ORDER BY timestamp_ms ASC, chunk_id ASC
                 LIMIT ?2"
            )
        } else {
            format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, seq
                 FROM rag_chunks
                 WHERE timestamp_ms > ?1 {visibility_sql}
                 ORDER BY timestamp_ms DESC, chunk_id DESC
                 LIMIT ?2"
            )
        };
        let mut turns = Vec::new();
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(scoped_params(
            params![since, fetch_limit],
            visible_agent_ids,
        )))?;
        while let Some(row) = rows.next()? {
            turns.push(ConversationTurn {
                seq: row.get(6)?,
                id: row.get(0)?,
                direction: row.get(1)?,
                from: row.get(2)?,
                to: row.get(3)?,
                reply_to: None,
                text: row.get(4)?,
                timestamp_ms: row.get(5)?,
            });
        }
        if let Some(peer) = peer_filter {
            turns.retain(|turn| matches_conversation_peer(turn, peer));
        }
        turns.sort_by(|a, b| {
            a.timestamp_ms
                .cmp(&b.timestamp_ms)
                .then_with(|| a.id.cmp(&b.id))
        });
        if let Some(limit) = limit {
            if since_ms.is_none() && turns.len() > limit {
                turns.drain(..turns.len() - limit);
            } else if turns.len() > limit {
                turns.truncate(limit);
            }
        }
        Ok(turns)
    }

    /// Upgrade only legacy authorized derivative chunks, in bounded transactions.
    /// The format index makes current history a read-only no-op, independent of
    /// mailbox size. Standalone tool summaries retain their available text;
    /// redacted mailbox bodies remain the authoritative input when present.
    fn backfill_full_mailbox_history(&self, visible: Option<&[String]>) -> Result<(), Error> {
        {
            let conn = self.read_conn();
            let visibility = visible_agent_ids_sql("c.agent_id", visible, 1);
            let pending = conn
                .query_row(
                    &format!(
                        "SELECT 1 FROM rag_chunks c INDEXED BY idx_rag_chunks_format
                         WHERE c.text_format < 2 {visibility} LIMIT 1"
                    ),
                    rusqlite::params_from_iter(scoped_params(&[], visible)),
                    |_| Ok(()),
                )
                .optional()?;
            if pending.is_none() {
                return Ok(());
            }
        }
        let mut after = String::new();
        loop {
            let mut conn = self.conn();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let visibility = visible_agent_ids_sql("c.agent_id", visible, 2);
            let mut select = tx.prepare(&format!(
                "SELECT c.chunk_id, c.agent_id, a.name, c.direction,
                        COALESCE(m.sender, CASE WHEN o.id IS NOT NULL THEN 'agent:' || a.name ELSE c.sender END),
                        COALESCE(m.reply_to, o.destination, c.destination, 'agent:' || a.name),
                        COALESCE(m.received_at_ms, o.sent_at_ms, c.timestamp_ms),
                        COALESCE(m.body, o.body, c.text), c.kind, c.tool_name,
                        COALESCE(m.seq, c.seq)
                 FROM rag_chunks c INDEXED BY idx_rag_chunks_format
                 JOIN agents a ON a.id = c.agent_id
                 LEFT JOIN messages m ON m.id = c.chunk_id AND m.agent_id = c.agent_id
                 LEFT JOIN outbound_messages o ON o.id = c.chunk_id AND o.agent_id = c.agent_id
                 WHERE c.text_format < 2 AND c.chunk_id > ?1 {visibility}
                 ORDER BY c.chunk_id LIMIT 64"
            ))?;
            struct Chunk {
                id: String,
                owner: String,
                name: String,
                direction: String,
                sender: String,
                destination: String,
                timestamp: i64,
                text: String,
                kind: Option<String>,
                tool: Option<String>,
                seq: i64,
            }
            let chunks = select
                .query_map(
                    rusqlite::params_from_iter(scoped_params(&[&after], visible)),
                    |r| {
                        Ok(Chunk {
                            id: r.get(0)?,
                            owner: r.get(1)?,
                            name: r.get(2)?,
                            direction: r.get(3)?,
                            sender: r.get(4)?,
                            destination: r.get(5)?,
                            timestamp: r.get(6)?,
                            text: r.get(7)?,
                            kind: r.get(8)?,
                            tool: r.get(9)?,
                            seq: r.get(10)?,
                        })
                    },
                )?
                .collect::<Result<Vec<_>, _>>()?;
            drop(select);
            if chunks.is_empty() {
                break;
            }
            for chunk in chunks {
                after = chunk.id.clone();
                if clean_rag_text(&chunk.text).chars().count() < 2 {
                    tx.execute(
                        "DELETE FROM rag_chunks_fts WHERE chunk_id MATCH ?1",
                        [format!("\"{}\"", chunk.id.replace('"', "\"\""))],
                    )?;
                    tx.execute(
                        "DELETE FROM rag_chunks WHERE chunk_id=?1 AND agent_id=?2",
                        params![chunk.id, chunk.owner],
                    )?;
                    continue;
                }
                index_rag_chunk(
                    &tx,
                    &chunk.id,
                    &chunk.owner,
                    &chunk.name,
                    &chunk.direction,
                    &chunk.sender,
                    &chunk.destination,
                    chunk.timestamp,
                    &chunk.text,
                    chunk.kind.as_deref(),
                    chunk.tool.as_deref(),
                    chunk.seq,
                )?;
            }
            tx.commit()?;
        }
        Ok(())
    }

    /// Zero-Embedding Lossless Lexical + Structured Agentic History Search (`Raw-RAG`).
    ///
    /// Searches `rag_chunks` and `rag_chunks_fts` across `req.agent` (`None` or `"all"` searches all agents)
    /// combining SQLite FTS5 BM25 (hybrid ASCII + CJK bigrams/unigrams), exact phrase/substring boost,
    /// multi-query coverage (`query` + `queries`), negative term filtering (`exclude`), tool history
    /// toggles (`include_tools`, `only_tools`, `tool_name`), range bounds (`since_ms`, `until_ms`,
    /// `from_seq`, `to_seq`), ordering (`Relevance`, `Newest`, `Oldest`), and contiguous episode
    /// context expansion (`radius`).
    pub fn search_history(&self, req: &HistorySearchQuery) -> Result<Vec<ConversationTurn>, Error> {
        self.backfill_full_mailbox_history(req.visible_agent_ids.as_deref())?;
        let conn = self.read_conn();
        let limit = if req.limit == 0 {
            (i64::MAX / 16) as usize
        } else {
            req.limit
        };
        let radius = req.radius.min(10);
        let since = req.since_ms.unwrap_or(0);
        let until = req.until_ms.unwrap_or(i64::MAX);

        let agent_name_filter = req
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all") && *s != "*")
            .map(|s| {
                let stripped = s.strip_prefix("agent:").unwrap_or(s);
                if let Ok(aid) = AgentId::parse(stripped)
                    && let Ok(found) = agent_by_id(&conn, &aid)
                {
                    return found.name.to_lowercase();
                }
                stripped.to_lowercase()
            });
        let visible_agent_ids = req.visible_agent_ids.as_deref();
        let peer_filter = req
            .with_peer
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let dir_filter = req
            .direction
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
            .map(str::to_ascii_lowercase);
        let sender_filter = req
            .sender
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase);
        let tool_filter = req
            .tool_name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase);
        let only_tools = req.only_tools || tool_filter.is_some();
        let include_tools = req.include_tools || only_tools;
        let kind_sql_filter: Option<&str> = if only_tools {
            Some("tool")
        } else if !include_tools {
            Some("message")
        } else {
            None
        };

        let mut all_queries: Vec<String> = Vec::new();
        if let Some(q) = req
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            all_queries.push(q.to_string());
        }
        for q in &req.queries {
            let trimmed = q.trim();
            if !trimmed.is_empty() && !all_queries.iter().any(|existing| existing == trimmed) {
                all_queries.push(trimmed.to_string());
            }
        }
        let queries_lower: Vec<String> = all_queries.iter().map(|q| q.to_lowercase()).collect();
        let query_tokens: Vec<String> = {
            let mut toks = Vec::new();
            for q in &all_queries {
                for t in tokenize_hybrid_search_tokens(q) {
                    if !toks.contains(&t) {
                        toks.push(t);
                    }
                }
            }
            toks
        };
        let exclude_lower: Vec<String> = req
            .exclude
            .iter()
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

        let passes_filters =
            |turn: &ConversationTurn, aname: &str, kind: &str, tool_name: &str, seq: i64| -> bool {
                if let Some(ref wanted_agent) = agent_name_filter
                    && !aname.eq_ignore_ascii_case(wanted_agent)
                {
                    return false;
                }
                if only_tools && kind != "tool" {
                    return false;
                }
                if !include_tools && kind == "tool" {
                    return false;
                }
                if let Some(ref wanted_tool) = tool_filter
                    && !tool_name.eq_ignore_ascii_case(wanted_tool)
                    && !turn
                        .text
                        .to_lowercase()
                        .contains(&format!("[tool:{wanted_tool}]"))
                    && !turn
                        .text
                        .to_lowercase()
                        .contains(&format!("[tool_call:{wanted_tool}]"))
                {
                    return false;
                }
                if let Some(ref wanted_dir) = dir_filter
                    && !turn.direction.eq_ignore_ascii_case(wanted_dir)
                {
                    return false;
                }
                if let Some(ref wanted_sender) = sender_filter
                    && !turn.from.to_lowercase().contains(wanted_sender)
                {
                    return false;
                }
                if let Some(from_s) = req.from_seq
                    && seq < from_s
                {
                    return false;
                }
                if let Some(to_s) = req.to_seq
                    && seq > to_s
                {
                    return false;
                }
                if let Some(peer) = peer_filter
                    && !matches_conversation_peer(turn, peer)
                {
                    return false;
                }
                if !exclude_lower.is_empty() {
                    let text_l = turn.text.to_lowercase();
                    if exclude_lower.iter().any(|ex| text_l.contains(ex)) {
                        return false;
                    }
                }
                true
            };

        let mut scores: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        let mut chunk_meta: std::collections::HashMap<String, (ConversationTurn, String, i64)> =
            std::collections::HashMap::new();

        if !all_queries.is_empty() {
            // 1. Lexical FTS5 BM25 Leg with inverted-index filter pushdown (`agent_name`, `kind`, `tool_name`).
            let fts_query = build_fts5_query_with_filters(
                &all_queries,
                agent_name_filter.as_deref(),
                kind_sql_filter,
                tool_filter.as_deref(),
            );
            let visibility_sql = visible_agent_ids_sql("agent_id", visible_agent_ids, 5);
            if !fts_query.is_empty()
                && let Ok(mut fstmt) = conn.prepare(&format!(
                    "SELECT c.chunk_id, c.direction, c.sender, c.destination,
                            COALESCE(
                                (SELECT m.body FROM messages m WHERE m.agent_id = c.agent_id AND m.id = c.chunk_id),
                                (SELECT o.body FROM outbound_messages o WHERE o.agent_id = c.agent_id AND o.id = c.chunk_id),
                                c.text
                            ),
                            c.timestamp_ms, c.agent_name, c.kind, c.tool_name, c.seq, f.score
                     FROM (
                         SELECT chunk_id, bm25(rag_chunks_fts, 0.5, 1.0, 1.0, 1.0, 0.5, 2.0, 3.0, 2.5) as score
                         FROM rag_chunks_fts
                         WHERE rag_chunks_fts MATCH ?1
                           AND chunk_id IN (
                               SELECT chunk_id FROM rag_chunks WHERE 1=1{visibility_sql}
                           )
                         ORDER BY score ASC
                         LIMIT ?4
                     ) f
                     JOIN rag_chunks c ON c.chunk_id = f.chunk_id
                     WHERE c.timestamp_ms > ?2 AND c.timestamp_ms <= ?3
                     ORDER BY f.score ASC",
                ))
                && let Ok(mut frows) = fstmt.query(rusqlite::params_from_iter(scoped_params(
                    params![fts_query, since, until, i64::try_from((limit * 8).max(120)).unwrap_or(120)],
                    visible_agent_ids,
                )))
            {
                let mut rank = 0usize;
                while let Ok(Some(row)) = frows.next() {
                    let cid: String = row.get(0)?;
                    let aname: String = row.get(6)?;
                    let kind: String = row.get(7)?;
                    let tool_name: String = row.get(8)?;
                    let seq: i64 = row.get(9)?;
                    let bm25_raw: f64 = row.get(10).unwrap_or(0.0);
                    let turn = ConversationTurn {
                        seq,
                        id: cid.clone(),
                        direction: row.get(1)?,
                        from: row.get(2)?,
                        to: row.get(3)?,
                        reply_to: Some(format!(
                            "bm25_rank={}:agent={aname}:kind={kind}:seq={seq}",
                            rank + 1
                        )),
                        text: row.get(4)?,
                        timestamp_ms: row.get(5)?,
                    };
                    if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                        continue;
                    }
                    let text_lower = turn.text.to_lowercase();
                    let exact_matches = queries_lower
                        .iter()
                        .filter(|ql| !ql.is_empty() && text_lower.contains(ql.as_str()))
                        .count();
                    let exact_boost = (exact_matches as f64) * 1.5;
                    let token_hits = if !query_tokens.is_empty() {
                        let doc_tokens = tokenize_hybrid_search_tokens(&turn.text);
                        query_tokens
                            .iter()
                            .filter(|qt| doc_tokens.contains(qt))
                            .count() as f64
                            / (query_tokens.len() as f64)
                    } else {
                        0.0
                    };
                    let rrf = 2.0 / (30.0 + (rank as f64) + 1.0)
                        + (-bm25_raw).clamp(0.0, 25.0) * 0.06
                        + exact_boost
                        + token_hits * 0.8;
                    *scores.entry(cid.clone()).or_insert(0.0) += rrf;
                    chunk_meta.entry(cid).or_insert((turn, aname, seq));
                    rank += 1;
                }
            }

            // 2. Bounded index-pushed substring fallback (only when FTS5 returned fewer than `limit` hits
            //    or for symbol/punctuation queries that produce no FTS tokens).
            if scores.len() < limit {
                for q_raw in &all_queries {
                    let q_trim = q_raw.trim();
                    if q_trim.is_empty() {
                        continue;
                    }
                    let like_pat = format!("%{}%", q_trim.replace('%', "\\%").replace('_', "\\_"));
                    let mut lstmt = conn.prepare(&format!(
                        "SELECT chunk_id, direction, sender, destination, text, timestamp_ms,
                                agent_name, kind, tool_name, seq
                         FROM (
                             SELECT chunk_id, direction, sender, destination, text, timestamp_ms,
                                    agent_name, kind, tool_name, seq
                             FROM rag_chunks
                             WHERE timestamp_ms > ?1 AND timestamp_ms <= ?2
                               AND (?5 IS NULL OR LOWER(agent_name) = ?5)
                               AND (?6 IS NULL OR kind = ?6)
                               AND (?7 IS NULL OR seq >= ?7)
                               AND (?8 IS NULL OR seq <= ?8){}
                             ORDER BY timestamp_ms DESC
                             LIMIT 2000
                         )
                         WHERE text LIKE ?3 ESCAPE '\\'
                         ORDER BY timestamp_ms DESC
                         LIMIT ?4",
                        visible_agent_ids_sql("agent_id", visible_agent_ids, 9)
                    ))?;
                    let mut lrows = lstmt.query(rusqlite::params_from_iter(scoped_params(
                        params![
                            since,
                            until,
                            like_pat,
                            i64::try_from((limit * 4).max(80)).unwrap_or(80),
                            agent_name_filter.as_deref(),
                            kind_sql_filter,
                            req.from_seq,
                            req.to_seq,
                        ],
                        visible_agent_ids,
                    )))?;
                    let mut rank = 0usize;
                    while let Some(row) = lrows.next()? {
                        let cid: String = row.get(0)?;
                        let aname: String = row.get(6)?;
                        let kind: String = row.get(7)?;
                        let tool_name: String = row.get(8)?;
                        let seq: i64 = row.get(9)?;
                        let turn = ConversationTurn {
                            seq,
                            id: cid.clone(),
                            direction: row.get(1)?,
                            from: row.get(2)?,
                            to: row.get(3)?,
                            reply_to: Some(format!("exact:agent={aname}:kind={kind}:seq={seq}")),
                            text: row.get(4)?,
                            timestamp_ms: row.get(5)?,
                        };
                        if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                            continue;
                        }
                        *scores.entry(cid.clone()).or_insert(0.0) +=
                            1.6 / (30.0 + (rank as f64) + 1.0);
                        chunk_meta.entry(cid).or_insert((turn, aname, seq));
                        rank += 1;
                    }
                }
            }
        } else {
            // Structured Filter / Range / Tool History Browse Mode (no keyword query required)
            // with full B-tree index pushdown on `agent_name`, `kind`, `tool_name`, `direction`, and `seq`.
            let order_sql = if req.sort == HistorySort::Oldest {
                "ORDER BY timestamp_ms ASC, seq ASC, chunk_id ASC"
            } else {
                "ORDER BY timestamp_ms DESC, seq DESC, chunk_id DESC"
            };
            let sql = format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms,
                        agent_name, kind, tool_name, seq
                 FROM rag_chunks
                 WHERE timestamp_ms > ?1 AND timestamp_ms <= ?2
                   AND (?4 IS NULL OR LOWER(agent_name) = ?4)
                   AND (?5 IS NULL OR kind = ?5)
                   AND (?6 IS NULL OR LOWER(direction) = ?6)
                   AND (?7 IS NULL OR seq >= ?7)
                   AND (?8 IS NULL OR seq <= ?8){}
                 {order_sql}
                 LIMIT ?3",
                visible_agent_ids_sql("agent_id", visible_agent_ids, 9)
            );
            let mut bstmt = conn.prepare(&sql)?;
            let mut brows = bstmt.query(rusqlite::params_from_iter(scoped_params(
                params![
                    since,
                    until,
                    i64::try_from((limit * 8).max(200)).unwrap_or(200),
                    agent_name_filter.as_deref(),
                    kind_sql_filter,
                    dir_filter.as_deref(),
                    req.from_seq,
                    req.to_seq,
                ],
                visible_agent_ids,
            )))?;
            let mut rank = 0usize;
            while let Some(row) = brows.next()? {
                let cid: String = row.get(0)?;
                let aname: String = row.get(6)?;
                let kind: String = row.get(7)?;
                let tool_name: String = row.get(8)?;
                let seq: i64 = row.get(9)?;
                let turn = ConversationTurn {
                    seq,
                    id: cid.clone(),
                    direction: row.get(1)?,
                    from: row.get(2)?,
                    to: row.get(3)?,
                    reply_to: Some(format!("filter:agent={aname}:kind={kind}:seq={seq}")),
                    text: row.get(4)?,
                    timestamp_ms: row.get(5)?,
                };
                if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                    continue;
                }
                scores.insert(cid.clone(), 1.0 / (1.0 + rank as f64));
                chunk_meta.insert(cid, (turn, aname, seq));
                rank += 1;
            }
        }

        let mut ranked: Vec<(String, f64)> = scores.into_iter().collect();
        match req.sort {
            HistorySort::Relevance => {
                ranked.sort_by(|a, b| {
                    b.1.partial_cmp(&a.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| {
                            let ts_a = chunk_meta.get(&a.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                            let ts_b = chunk_meta.get(&b.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                            ts_b.cmp(&ts_a)
                        })
                });
            }
            HistorySort::Newest => {
                ranked.sort_by(|a, b| {
                    let ts_a = chunk_meta.get(&a.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                    let ts_b = chunk_meta.get(&b.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                    ts_b.cmp(&ts_a).then_with(|| b.0.cmp(&a.0))
                });
            }
            HistorySort::Oldest => {
                ranked.sort_by(|a, b| {
                    let ts_a = chunk_meta.get(&a.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                    let ts_b = chunk_meta.get(&b.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                    ts_a.cmp(&ts_b).then_with(|| a.0.cmp(&b.0))
                });
            }
        }

        // Deduplicate identical message text across multiple recipient agents in room broadcasts.
        let mut seen_text_sig: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut matched_hits: Vec<(ConversationTurn, String, i64)> = Vec::new();
        for (cid, score) in ranked {
            if let Some((mut turn, aname, seq)) = chunk_meta.remove(&cid) {
                let sig = crate::learned_memory::sha256(turn.text.as_bytes());
                if !seen_text_sig.insert(sig) {
                    continue;
                }
                turn.reply_to = Some(format!(
                    "score={score:.4} ({})",
                    turn.reply_to.unwrap_or_default()
                ));
                matched_hits.push((turn, aname, seq));
                if matched_hits.len() >= limit {
                    break;
                }
            }
        }

        if radius == 0 || matched_hits.is_empty() {
            return Ok(matched_hits.into_iter().map(|(t, _, _)| t).collect());
        }

        // Contiguous episode context expansion (`radius > 0`): attach up to `radius` preceding
        // and `radius` following turns of the same agent around each matched turn.
        let mut out: Vec<ConversationTurn> = Vec::new();
        let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        let rad_i64 = i64::try_from(radius).unwrap_or(2);
        for (hit_turn, aname, _) in matched_hits {
            let hit_ts = hit_turn.timestamp_ms;
            let mut before: Vec<ConversationTurn> = Vec::new();
            if let Ok(mut bstmt) = conn.prepare(&format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, kind, seq, tool_name
                 FROM rag_chunks
                 WHERE LOWER(agent_name) = LOWER(?1)
                   AND (timestamp_ms < ?2 OR (timestamp_ms = ?2 AND chunk_id != ?3))
                   AND timestamp_ms > ?5 AND timestamp_ms <= ?6{}
                 ORDER BY timestamp_ms DESC, seq DESC
                 LIMIT ?4",
                visible_agent_ids_sql("agent_id", visible_agent_ids, 7),
            )) && let Ok(mut brows) =
                bstmt.query(rusqlite::params_from_iter(scoped_params(
                    params![aname, hit_ts, hit_turn.id, rad_i64 * 2, since, until], visible_agent_ids)))
            {
                while let Ok(Some(r)) = brows.next() {
                    let kind: String = r.get(6)?;
                    if only_tools && kind != "tool" {
                        continue;
                    }
                    if !include_tools && kind == "tool" {
                        continue;
                    }
                    let seq: i64 = r.get(7)?;
                    let turn = ConversationTurn {
                        seq,
                        id: r.get(0)?,
                        direction: r.get(1)?,
                        from: r.get(2)?,
                        to: r.get(3)?,
                        reply_to: Some(format!(
                            "context:before={}:agent={aname}:seq={seq}",
                            hit_turn.id
                        )),
                        text: r.get(4)?,
                        timestamp_ms: r.get(5)?,
                    };
                    let tool_name: String = r.get(8)?;
                    if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                        continue;
                    }
                    before.push(turn);
                    if before.len() >= radius {
                        break;
                    }
                }
            }
            before.reverse();
            for b in before {
                if seen_ids.insert(b.id.clone()) {
                    out.push(b);
                }
            }
            let hit_id = hit_turn.id.clone();
            if seen_ids.insert(hit_id.clone()) {
                out.push(hit_turn);
            }
            if let Ok(mut astmt) = conn.prepare(&format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, kind, seq, tool_name
                 FROM rag_chunks
                 WHERE LOWER(agent_name) = LOWER(?1)
                   AND timestamp_ms >= ?2
                   AND chunk_id != ?3
                   AND timestamp_ms > ?5 AND timestamp_ms <= ?6{}
                 ORDER BY timestamp_ms ASC, seq ASC
                 LIMIT ?4",
                visible_agent_ids_sql("agent_id", visible_agent_ids, 7),
            )) && let Ok(mut arows) = astmt.query(rusqlite::params_from_iter(scoped_params(
                params![aname, hit_ts, hit_id, rad_i64 * 2, since, until], visible_agent_ids)))
            {
                let mut added_after = 0usize;
                while let Ok(Some(r)) = arows.next() {
                    let kind: String = r.get(6)?;
                    if only_tools && kind != "tool" {
                        continue;
                    }
                    if !include_tools && kind == "tool" {
                        continue;
                    }
                    let seq: i64 = r.get(7)?;
                    let turn = ConversationTurn {
                        seq,
                        id: r.get(0)?,
                        direction: r.get(1)?,
                        from: r.get(2)?,
                        to: r.get(3)?,
                        reply_to: Some(format!("context:after={hit_id}:agent={aname}:seq={seq}")),
                        text: r.get(4)?,
                        timestamp_ms: r.get(5)?,
                    };
                    let tool_name: String = r.get(8)?;
                    if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                        continue;
                    }
                    if seen_ids.insert(turn.id.clone()) {
                        out.push(turn);
                        added_after += 1;
                        if added_after >= radius {
                            break;
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Lexical + structured search over `rag_chunks` across `agent_filter` (`None` or `"all"` searches all agents).
    pub fn search_conversation(
        &self,
        agent_filter: Option<&str>,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
        query_text: &str,
    ) -> Result<Vec<ConversationTurn>, Error> {
        self.search_history(&HistorySearchQuery {
            query: Some(query_text.to_string()),
            agent: agent_filter.map(str::to_string),
            with_peer: with_peer.map(str::to_string),
            since_ms,
            limit: limit.unwrap_or(20),
            ..HistorySearchQuery::default()
        })
    }

    /// Neutral host-level sleep-time memory index and semantic consolidation.
    /// Synchronizes `<agent_root>/memory/MEMORY.md` and `HostIndex` whenever topic
    /// files in `<agent_root>/memory/<topic>.md` are added, modified, or deleted,
    /// and when unconsolidated substantive conversation turns exist and cooldown
    /// has elapsed, runs the 4-phase LLM sleep-time semantic consolidation pass.
    pub fn consolidate_agent_sleep_memory(&self, agent_id: &AgentId) -> Result<bool, Error> {
        self.consolidate_agent_sleep_memory_with_options(agent_id, true)
            .map(|(modified, _)| modified)
    }

    /// Performs deterministic disk topic synchronization and optionally executes
    /// the LLM sleep-time consolidation pass (`allow_llm`). Returns
    /// `(memory_modified, executed_llm_call)`.
    pub fn consolidate_agent_sleep_memory_with_options(
        &self,
        agent_id: &AgentId,
        allow_llm: bool,
    ) -> Result<(bool, bool), Error> {
        let agent = self.agent(agent_id)?;
        if agent.state == "archived" {
            return Ok((false, false));
        }
        let agent_root = self.data_dir().join("agents").join(agent.id.as_str());
        let mem_dir = agent_root.join("memory");
        if !mem_dir.is_dir() {
            return Ok((false, false));
        }
        let mut sleep = crate::learned_memory::read_sleep_state(&agent_root);
        let now = crate::now_ms();

        // Phase 1a: Fast deterministic disk topic check & index sync.
        let updated_topics = crate::learned_memory::list_memory_topics(&agent_root)?;
        let mem_path = mem_dir.join("MEMORY.md");
        let mem_exists = mem_path.is_file();
        let current_mem = std::fs::read_to_string(&mem_path).unwrap_or_default();
        let mem_mtime_ms = std::fs::metadata(&mem_path)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
            .unwrap_or(0);
        let any_newer_topic = updated_topics
            .iter()
            .any(|t| t.updated_at_ms > mem_mtime_ms);
        let is_truncated =
            crate::learned_memory::bounded_section(&current_mem).contains("[Memory truncated;");
        let is_legacy_index =
            current_mem.contains("\n## Topics (") && !current_mem.contains("\n## Active Topics");
        let missing_index_with_topics = !mem_exists && !updated_topics.is_empty();
        let mut modified = false;
        if missing_index_with_topics
            || any_newer_topic
            || is_truncated
            || is_legacy_index
            || sleep.consolidated_topics != updated_topics.len()
        {
            crate::learned_memory::rebuild_memory_index(
                &agent_root,
                &agent.name,
                agent.role.as_deref(),
            )?;
            let _ = self.sync_agent_memory_index(&agent);
            modified = true;
        }

        // Phase 1b: Delta turn watermarks & quote/noise filtering.
        let (max_in_seq, latest_in_ms, max_out_ms, substantive_turns) = {
            let conn = self.read_conn();
            let (max_in_seq, latest_in_ms): (i64, i64) = conn
                .query_row(
                    "SELECT COALESCE(MAX(seq), 0), COALESCE(MAX(received_at_ms), 0)
                     FROM messages WHERE agent_id = ?1",
                    params![agent.id.as_str()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap_or((0, 0));
            let max_out_ms: i64 = conn
                .query_row(
                    "SELECT COALESCE(MAX(sent_at_ms), 0) FROM outbound_messages WHERE agent_id = ?1",
                    params![agent.id.as_str()],
                    |r| r.get(0),
                )
                .unwrap_or(0);

            let has_new_turns =
                max_in_seq > sleep.last_inbound_seq || max_out_ms > sleep.last_outbound_ms;
            if !has_new_turns {
                (max_in_seq, latest_in_ms, max_out_ms, None)
            } else {
                let mut raw_turns: Vec<crate::learned_memory::SleepTurnDelta> = Vec::new();
                if max_in_seq > sleep.last_inbound_seq
                    && let Ok(mut stmt) = conn.prepare(
                        "SELECT received_at_ms, COALESCE(sender, 'unknown'), body
                         FROM messages
                         WHERE agent_id = ?1 AND seq > ?2
                         ORDER BY seq DESC
                         LIMIT 30",
                    )
                    && let Ok(mut rows) =
                        stmt.query(params![agent.id.as_str(), sleep.last_inbound_seq])
                {
                    while let Ok(Some(row)) = rows.next() {
                        let ts: i64 = row.get(0).unwrap_or(0);
                        let peer: String = row.get(1).unwrap_or_default();
                        let body: String = row.get(2).unwrap_or_default();
                        if let Some(clean) = crate::learned_memory::clean_sleep_turn_text(&body) {
                            raw_turns.push(crate::learned_memory::SleepTurnDelta {
                                timestamp_ms: ts,
                                direction: "in".to_string(),
                                peer,
                                text: clean,
                            });
                        }
                    }
                }
                if max_out_ms > sleep.last_outbound_ms
                    && let Ok(mut stmt) = conn.prepare(
                        "SELECT sent_at_ms, destination, body
                         FROM outbound_messages
                         WHERE agent_id = ?1 AND sent_at_ms > ?2
                         ORDER BY sent_at_ms DESC
                         LIMIT 30",
                    )
                    && let Ok(mut rows) =
                        stmt.query(params![agent.id.as_str(), sleep.last_outbound_ms])
                {
                    while let Ok(Some(row)) = rows.next() {
                        let ts: i64 = row.get(0).unwrap_or(0);
                        let peer: String = row.get(1).unwrap_or_default();
                        let body: String = row.get(2).unwrap_or_default();
                        if let Some(clean) = crate::learned_memory::clean_sleep_turn_text(&body) {
                            raw_turns.push(crate::learned_memory::SleepTurnDelta {
                                timestamp_ms: ts,
                                direction: "out".to_string(),
                                peer,
                                text: clean,
                            });
                        }
                    }
                }
                raw_turns.sort_by_key(|t| t.timestamp_ms);
                let filtered = crate::learned_memory::filter_substantive_sleep_turns(raw_turns);
                let start = filtered.len().saturating_sub(30);
                let bounded_turns = filtered[start..].to_vec();
                (max_in_seq, latest_in_ms, max_out_ms, Some(bounded_turns))
            }
        };

        let Some(substantive_turns) = substantive_turns else {
            if modified {
                sleep.last_consolidated_ms = now;
                sleep.consolidated_topics = updated_topics.len();
                let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            }
            return Ok((modified, false));
        };

        // If all delta turns were empty, "NO_REPLY", system prompt echoes, or
        // routine schedule ticks without outbound actions, advance watermarks
        // without invoking the LLM.
        if substantive_turns.is_empty() {
            sleep.last_consolidated_ms = now;
            sleep.last_inbound_seq = max_in_seq;
            sleep.last_outbound_ms = max_out_ms;
            sleep.consolidated_topics = updated_topics.len();
            let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            return Ok((modified, false));
        }

        if !allow_llm {
            if modified {
                sleep.consolidated_topics = updated_topics.len();
                let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            }
            return Ok((modified, false));
        }

        // Phase 2: Resolve LLM endpoint first; if none is configured (e.g. offline test mode),
        // advance watermarks cleanly without waiting for LLM cooldown.
        let Some(endpoint) =
            crate::learned_memory::resolve_sleep_model_endpoint(self.data_dir(), &agent.id)
        else {
            sleep.last_consolidated_ms = now;
            sleep.last_inbound_seq = max_in_seq;
            sleep.last_outbound_ms = max_out_ms;
            sleep.consolidated_topics = updated_topics.len();
            let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            return Ok((modified, false));
        };

        // Phase 2b: Quiescence & cooldown gate (2m turn quiescence + 10m consolidation cooldown).
        let force = std::env::var("ANYAGENTS_SLEEP_CONSOLIDATION_FORCE")
            .ok()
            .as_deref()
            == Some("1");
        let custom_url_set = std::env::var("ANYAGENTS_SLEEP_MODEL_URL")
            .ok()
            .is_some_and(|s| !s.trim().is_empty());
        let latest_turn_ms = latest_in_ms.max(max_out_ms);
        let turn_quiescent = custom_url_set || now.saturating_sub(latest_turn_ms) >= 120_000;
        let consolidation_cooled_down = sleep.last_consolidated_ms == 0
            || now.saturating_sub(sleep.last_consolidated_ms) >= 600_000;
        if !force && (!turn_quiescent || !consolidation_cooled_down) {
            if modified {
                sleep.consolidated_topics = updated_topics.len();
                let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            }
            return Ok((modified, false));
        }

        let Ok(plan) = crate::learned_memory::request_sleep_consolidation_plan(
            &endpoint,
            &agent_root,
            &agent.name,
            agent.role.as_deref(),
            &updated_topics,
            &substantive_turns,
        ) else {
            // Preserve `last_inbound_seq` / `last_outbound_ms` on transient HTTP failure
            // so the unconsolidated turns are retried on a subsequent pass.
            if modified {
                sleep.consolidated_topics = updated_topics.len();
                let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
            }
            return Ok((modified, true));
        };

        // Phase 4: Apply upsert & defragmentation delete operations, rebuild MEMORY.md,
        // synchronize HostIndex FTS5, and advance watermarks.
        for op in &plan.operations {
            match op {
                crate::learned_memory::SleepMemoryOp::Upsert {
                    topic,
                    description,
                    content,
                } => {
                    crate::learned_memory::upsert_memory_topic(
                        &agent_root,
                        &agent.name,
                        agent.role.as_deref(),
                        topic,
                        description.as_deref(),
                        content,
                        now,
                    )?;
                    modified = true;
                }
                crate::learned_memory::SleepMemoryOp::Delete { topic } => {
                    let (removed, _) = crate::learned_memory::delete_memory_topic(
                        &agent_root,
                        &agent.name,
                        agent.role.as_deref(),
                        topic,
                    )?;
                    if removed {
                        modified = true;
                    }
                }
            }
        }
        if !plan.operations.is_empty() {
            crate::learned_memory::rebuild_memory_index(
                &agent_root,
                &agent.name,
                agent.role.as_deref(),
            )?;
            let _ = self.sync_agent_memory_index(&agent);
        }

        let final_topics = crate::learned_memory::list_memory_topics(&agent_root)?;
        sleep.last_consolidated_ms = now;
        sleep.last_inbound_seq = max_in_seq;
        sleep.last_outbound_ms = max_out_ms;
        sleep.consolidated_topics = final_topics.len();
        let _ = crate::learned_memory::write_sleep_state(&agent_root, &sleep);
        Ok((modified, true))
    }

    /// The next message the harness has not taken, or `None` when delivery has
    /// caught up.
    pub fn next_undelivered(&self, agent_id: &AgentId) -> Result<Option<Message>, Error> {
        let delivered = self.delivered_cursor(agent_id)?;
        let next = delivered + 1;
        if let Some(message) = self.message_at(agent_id, next)? {
            return Ok(Some(message));
        }
        let received = self.received_cursor(agent_id)?;
        if received > delivered {
            return Err(Error::Message(format!(
                "mailbox sequence has a hole at {next}"
            )));
        }
        Ok(None)
    }

    /// True if there is at least one undelivered message that should wake an idle
    /// agent (`event != "reaction_passive"`). Passive group reactions remain in
    /// the mailbox and are delivered on the agent's next active turn.
    pub fn has_actionable_undelivered(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        let delivered = cursor(&conn, agent_id)?;
        let mut stmt = conn.prepare(
            "SELECT context FROM messages WHERE agent_id = ?1 AND seq > ?2 ORDER BY seq ASC",
        )?;
        let mut rows = stmt.query(params![agent_id.as_str(), delivered])?;
        while let Some(row) = rows.next()? {
            let ctx_raw: Option<String> = row.get(0)?;
            let is_passive = ctx_raw
                .as_deref()
                .and_then(|s| serde_json::from_str::<anyagents_harness::Context>(s).ok())
                .is_some_and(|ctx| ctx.event.as_deref() == Some("reaction_passive"));
            if !is_passive {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn received_cursor(&self, agent_id: &AgentId) -> Result<i64, Error> {
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        let seq: i64 = conn.query_row(
            "SELECT MAX(0, COALESCE(MAX(seq), 0)) FROM messages WHERE agent_id = ?1",
            [agent_id.as_str()],
            |row| row.get(0),
        )?;
        Ok(seq)
    }

    /// The one delivery cursor: the last sequence the harness took.
    pub fn delivered_cursor(&self, agent_id: &AgentId) -> Result<i64, Error> {
        let conn = self.read_conn();
        agent_by_id(&conn, agent_id)?;
        cursor(&conn, agent_id)
    }

    /// Move the delivery cursor to `seq`. `seq` must be exactly one past the
    /// current cursor, so nothing is skipped.
    pub fn advance_delivered(&self, agent_id: &AgentId, seq: i64) -> Result<i64, Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        agent_by_id(&tx, agent_id)?;
        bump_delivered(&tx, agent_id, seq)?;
        tx.commit()?;
        Ok(seq)
    }

    /// Move the cursor back to `seq` for a test that simulates a crash between
    /// the harness taking a line and the commit recording it.
    #[doc(hidden)]
    pub fn rewind_for_test(&self, agent_id: &AgentId, seq: i64) {
        let conn = self.conn();
        conn.execute(
            "UPDATE mailbox_cursors SET delivered_seq = ?1 WHERE agent_id = ?2",
            params![seq, agent_id.as_str()],
        )
        .unwrap();
    }

    /// Record that the harness took `seq`, and when, in one commit.
    pub fn note_delivered(&self, agent_id: &AgentId, seq: i64) -> Result<(), Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        agent_by_id(&tx, agent_id)?;
        bump_delivered(&tx, agent_id, seq)?;
        let now = now_ms();
        let prev_seq = (seq - 1).max(0);
        tx.execute(
            "UPDATE messages SET delivered_at_ms = ?1 WHERE agent_id = ?2 AND seq = ?3",
            params![now, agent_id.as_str(), seq],
        )?;
        tx.execute(
            "INSERT INTO turn_recovery (agent_id, opened_seq, completed_seq, recovery_count, opened_at_ms)
             VALUES (?1, ?2, ?3, 0, ?4)
             ON CONFLICT(agent_id) DO UPDATE SET
                 completed_seq = CASE
                     WHEN turn_recovery.completed_seq >= turn_recovery.opened_seq THEN ?3
                     WHEN EXISTS (
                         SELECT 1 FROM outbound_messages
                         WHERE outbound_messages.agent_id = ?1
                           AND outbound_messages.sent_at_ms >= turn_recovery.opened_at_ms
                     ) THEN ?3
                     ELSE turn_recovery.completed_seq
                 END,
                 opened_at_ms = CASE
                     WHEN turn_recovery.completed_seq >= turn_recovery.opened_seq THEN excluded.opened_at_ms
                     WHEN EXISTS (
                         SELECT 1 FROM outbound_messages
                         WHERE outbound_messages.agent_id = ?1
                           AND outbound_messages.sent_at_ms >= turn_recovery.opened_at_ms
                     ) THEN excluded.opened_at_ms
                     ELSE turn_recovery.opened_at_ms
                 END,
                 opened_seq = excluded.opened_seq,
                 recovery_count = 0",
            params![agent_id.as_str(), seq, prev_seq, now],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Ensure message IDs from a reconciled external input hold are recorded in
    /// `turn_recovery` as an open interrupted turn (and past `delivered_seq`)
    /// so recovery resumes them with `[Recovery]` rather than raw replay.
    pub fn promote_messages_to_open_turn(
        &self,
        agent_id: &AgentId,
        message_ids: &[String],
    ) -> Result<(), Error> {
        if message_ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        agent_by_id(&tx, agent_id)?;
        let mut seqs = Vec::new();
        for msg_id in message_ids {
            if let Some(seq) = tx
                .query_row(
                    "SELECT seq FROM messages WHERE agent_id = ?1 AND id = ?2",
                    params![agent_id.as_str(), msg_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
            {
                seqs.push(seq);
            }
        }
        let (Some(&min_seq), Some(&max_seq)) = (seqs.iter().min(), seqs.iter().max()) else {
            return Ok(());
        };
        let cur = cursor(&tx, agent_id)?;
        let now = now_ms();
        if max_seq > cur {
            tx.execute(
                "INSERT INTO mailbox_cursors (agent_id, delivered_seq) VALUES (?1, ?2)
                 ON CONFLICT(agent_id) DO UPDATE SET delivered_seq = MAX(delivered_seq, ?2)",
                params![agent_id.as_str(), max_seq],
            )?;
            tx.execute(
                "UPDATE messages
                 SET delivered_at_ms = COALESCE(delivered_at_ms, ?1)
                 WHERE agent_id = ?2 AND seq > ?3 AND seq <= ?4",
                params![now, agent_id.as_str(), cur, max_seq],
            )?;
        }
        let completed_floor = (min_seq - 1).max(0);
        tx.execute(
            "INSERT INTO turn_recovery (agent_id, opened_seq, completed_seq, recovery_count, opened_at_ms)
             VALUES (?1, ?2, ?3, 0, ?4)
             ON CONFLICT(agent_id) DO UPDATE SET
                 opened_seq = MAX(turn_recovery.opened_seq, ?2),
                 completed_seq = MIN(turn_recovery.completed_seq, ?3)",
            params![agent_id.as_str(), max_seq, completed_floor, now],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Rewind mailbox delivery custody for mid-turn steered messages whose
    /// execution became unconfirmed (e.g. cancelled before terminal commit).
    pub fn reclaim_unconfirmed_steers(
        &self,
        agent_id: &AgentId,
        message_ids: &[String],
    ) -> Result<(), Error> {
        if message_ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        agent_by_id(&tx, agent_id)?;
        let mut min_seq: Option<i64> = None;
        for msg_id in message_ids {
            if let Some(seq) = tx
                .query_row(
                    "SELECT seq FROM messages
                     WHERE agent_id = ?1 AND id = ?2 AND delivered_at_ms IS NOT NULL",
                    params![agent_id.as_str(), msg_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
            {
                min_seq = Some(min_seq.map_or(seq, |cur| cur.min(seq)));
            }
        }
        if let Some(first_reclaimed_seq) = min_seq {
            let rewind_to = (first_reclaimed_seq - 1).max(0);
            tx.execute(
                "UPDATE mailbox_cursors
                 SET delivered_seq = MIN(delivered_seq, ?1)
                 WHERE agent_id = ?2",
                params![rewind_to, agent_id.as_str()],
            )?;
            tx.execute(
                "UPDATE messages
                 SET delivered_at_ms = NULL
                 WHERE agent_id = ?1 AND seq >= ?2",
                params![agent_id.as_str(), first_reclaimed_seq],
            )?;
            tx.execute(
                "UPDATE turn_recovery
                 SET opened_seq = MIN(opened_seq, ?1)
                 WHERE agent_id = ?2",
                params![rewind_to, agent_id.as_str()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Set the internal communication and discovery scope (`"isolated"`, `"private"`, `"siblings"`, or `"public"`).
    pub fn set_agent_scope(&self, agent_id: &AgentId, scope: &str) -> Result<Agent, Error> {
        self.set_agent_scope_as(agent_id, scope, "system")
    }

    /// Set scope and atomically record its authenticated actor and before/after policy.
    pub fn set_agent_scope_as(
        &self,
        agent_id: &AgentId,
        scope: &str,
        actor: &str,
    ) -> Result<Agent, Error> {
        let normalized = parse_agent_scope(scope)?;
        self.update_agent_launch_env(agent_id, Some((actor, "scope")), |current| {
            scope_env(current, normalized)
        })
    }

    /// Set the agent lifecycle mode (`"task"` or `"standing"`).
    pub fn set_agent_lifecycle(&self, agent_id: &AgentId, lifecycle: &str) -> Result<Agent, Error> {
        self.set_agent_lifecycle_as(agent_id, lifecycle, "system")
    }

    /// Set lifecycle mode (`"task"` or `"standing"`).
    pub fn set_agent_lifecycle_as(
        &self,
        agent_id: &AgentId,
        lifecycle: &str,
        _actor: &str,
    ) -> Result<Agent, Error> {
        let normalized = parse_agent_lifecycle(lifecycle)?;
        self.update_agent_launch_env(agent_id, None, |current| lifecycle_env(current, normalized))
    }

    /// Set whether a sub-agent accepts external messages from agents outside its
    /// direct parent/child lineage (`ANYAGENTS_PUBLIC=1` in `launch_env`).
    pub fn set_agent_public(&self, agent_id: &AgentId, public: bool) -> Result<Agent, Error> {
        self.set_agent_public_as(agent_id, public, "system")
    }

    /// The legacy boolean only grants/revokes public reachability. Revocation
    /// restores the default for public agents and preserves all nonpublic scopes.
    pub fn set_agent_public_as(
        &self,
        agent_id: &AgentId,
        public: bool,
        actor: &str,
    ) -> Result<Agent, Error> {
        self.update_agent_launch_env(agent_id, Some((actor, "public")), |current| {
            if public {
                scope_env(current, "public")
            } else if current.scope() == "public" {
                scope_env(current, current.default_scope())
            } else {
                current.launch_env.clone()
            }
        })
    }

    /// Returns `true` when `agent_a` and `agent_b` are members of at least one shared
    /// internal virtual group (`group_agents`) or attached to the same Telegram chat.
    pub fn share_any_group_or_chat(&self, agent_a: &AgentId, agent_b: &AgentId) -> bool {
        let shares_group = {
            let conn = self.read_conn();
            conn.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM group_agents g1
                     JOIN group_agents g2 ON g1.group_name = g2.group_name
                     WHERE g1.agent_id = ?1 AND g2.agent_id = ?2
                 )",
                params![agent_a.as_str(), agent_b.as_str()],
                |r| r.get::<_, bool>(0),
            )
            .unwrap_or(false)
        };
        if shares_group {
            return true;
        }
        let Ok(channels) = self.list_channels() else {
            return false;
        };
        let mut chats_a = std::collections::HashSet::new();
        let mut chats_b = std::collections::HashSet::new();
        for ch in channels {
            let has_a = ch.agents.iter().any(|id| id == agent_a);
            let has_b = ch.agents.iter().any(|id| id == agent_b);
            if !has_a && !has_b {
                continue;
            }
            for (_, chat_id) in telegram_chat_links(&ch.state) {
                if has_a {
                    chats_a.insert(chat_id);
                }
                if has_b {
                    chats_b.insert(chat_id);
                }
            }
        }
        chats_a.iter().any(|cid| chats_b.contains(cid))
    }

    /// Stateful reply capability: returns `true` if `from_agent` has previously sent a direct
    /// message to `to_agent`, allowing `to_agent` to reply back even if `from_agent` is `public: false`.
    pub fn has_messaged(&self, from_agent: &Agent, to_agent: &Agent) -> bool {
        let dest_name = format!("agent:{}", to_agent.name);
        let conn = self.read_conn();
        conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM outbound_messages
                 WHERE agent_id = ?1 AND (destination = ?2 OR destination = ?3)
             )",
            params![from_agent.id.as_str(), dest_name, to_agent.id.as_str()],
            |r| r.get::<_, bool>(0),
        )
        .unwrap_or(false)
    }

    /// Mark the agent's current open turn as completed (`completed_seq = opened_seq`).
    pub fn note_turn_completed(&self, agent_id: &AgentId) -> Result<(), Error> {
        let mut conn = self.conn();
        // BEGIN IMMEDIATE: a SELECT-then-UPDATE under BEGIN DEFERRED raises
        // SQLITE_BUSY without honouring busy_timeout when a peer is writing.
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        agent_by_id(&tx, agent_id)?;
        tx.execute(
            "UPDATE turn_recovery
             SET completed_seq = opened_seq,
                 recovery_count = 0
             WHERE agent_id = ?1 AND completed_seq < opened_seq",
            [agent_id.as_str()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Record that one `[Recovery]` wake was delivered for `opened_seq`.
    pub fn note_turn_recovered(&self, agent_id: &AgentId, opened_seq: i64) -> Result<(), Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        agent_by_id(&tx, agent_id)?;
        tx.execute(
            "UPDATE turn_recovery
             SET recovery_count = recovery_count + 1
             WHERE agent_id = ?1 AND opened_seq = ?2",
            params![agent_id.as_str(), opened_seq],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Return `true` when the agent currently has an open turn (`opened_seq > completed_seq`).
    pub fn has_open_turn(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let conn = self.read_conn();
        let row: Option<(i64, i64)> = conn
            .query_row(
                "SELECT opened_seq, completed_seq FROM turn_recovery WHERE agent_id = ?1",
                [agent_id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        Ok(row.is_some_and(|(opened_seq, completed_seq)| opened_seq > completed_seq))
    }

    /// Return the agent's open turn (`opened_seq > completed_seq`) when it is
    /// eligible for automatic mid-turn recovery.
    pub fn open_turn(&self, agent_id: &AgentId) -> Result<Option<OpenTurn>, Error> {
        let conn = self.read_conn();
        agent_by_id(&conn, agent_id)?;
        let row: Option<(i64, i64, i64, i64)> = conn
            .query_row(
                "SELECT opened_seq, completed_seq, recovery_count, opened_at_ms
                 FROM turn_recovery WHERE agent_id = ?1",
                [agent_id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some((opened_seq, completed_seq, recovery_count, opened_at_ms)) = row else {
            return Ok(None);
        };
        if opened_seq <= completed_seq {
            return Ok(None);
        }
        let delivered = cursor(&conn, agent_id)?;
        let mut msg_stmt = conn.prepare(&format!(
            "SELECT {MESSAGE_COLS}
             FROM messages
             WHERE agent_id = ?1 AND seq > ?2 AND seq <= ?3
             ORDER BY seq ASC"
        ))?;
        let mut msg_rows = msg_stmt.query(params![agent_id.as_str(), completed_seq, opened_seq])?;
        let mut messages = Vec::new();
        while let Some(r) = msg_rows.next()? {
            messages.push(message_from_row_with_cursor(r, delivered)?);
        }
        if messages.is_empty() {
            return Ok(None);
        }
        let mut out_stmt = conn.prepare(
            "SELECT id, destination, reply_to, body, sent_at_ms
             FROM outbound_messages
             WHERE agent_id = ?1 AND sent_at_ms >= ?2
             ORDER BY sent_at_ms ASC, id ASC",
        )?;
        let mut out_rows = out_stmt.query(params![agent_id.as_str(), opened_at_ms])?;
        let mut completed_outbounds = Vec::new();
        while let Some(r) = out_rows.next()? {
            completed_outbounds.push(OutboundEffect {
                id: r.get(0)?,
                destination: r.get(1)?,
                reply_to: r.get(2)?,
                body: r.get(3)?,
                sent_at_ms: r.get(4)?,
            });
        }
        Ok(Some(OpenTurn {
            agent_id: agent_id.clone(),
            opened_seq,
            completed_seq,
            recovery_count,
            opened_at_ms,
            messages,
            completed_outbounds,
        }))
    }

    pub fn message(&self, agent_id: &AgentId, id: &str) -> Result<Option<Message>, Error> {
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        let found = conn
            .query_row(
                &format!("SELECT {MESSAGE_COLS} FROM messages WHERE agent_id = ?1 AND id = ?2"),
                params![agent_id.as_str(), id],
                |row| Ok(message_from_row(&conn, row)),
            )
            .optional()?;
        found.transpose()
    }

    /// Age of the oldest message the harness has not taken.
    /// Age of the oldest message the harness has not taken.
    pub fn lag(&self, agent_id: &AgentId, now_ms: i64) -> Result<Lag, Error> {
        let delivered = self.delivered_cursor(agent_id)?;
        Ok(Lag {
            oldest_undelivered_age_ms: self.age_at(agent_id, delivered + 1, now_ms)?,
        })
    }

    pub fn record_usage(&self, new: NewUsage) -> Result<UsageRecord, Error> {
        let record = UsageRecord {
            id: UsageId::generate(),
            agent_id: new.agent_id.clone(),
            harness: new.harness,
            model: new.model,
            input_tokens: new.input_tokens,
            output_tokens: new.output_tokens,
            cache_read_tokens: new.cache_read_tokens,
            cache_write_tokens: new.cache_write_tokens,
            reported_cost_micros: new.reported_cost_micros,
            estimated_cost_micros: new.estimated_cost_micros,
            billing_mode: new.billing_mode,
            quota_state: new.quota_state,
            day: new.day,
            recorded_at_ms: now_ms(),
        };
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        agent_by_id(&tx, &record.agent_id)?;
        tx.execute(
            "INSERT INTO usage_records (
                id, agent_id, harness, model, input_tokens, output_tokens,
                cache_read_tokens, cache_write_tokens, reported_cost_micros,
                estimated_cost_micros, billing_mode, quota_state, day, recorded_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                record.id.as_str(),
                record.agent_id.as_str(),
                record.harness,
                record.model,
                record.input_tokens,
                record.output_tokens,
                record.cache_read_tokens,
                record.cache_write_tokens,
                record.reported_cost_micros,
                record.estimated_cost_micros,
                record.billing_mode.as_str(),
                record.quota_state,
                record.day,
                record.recorded_at_ms,
            ],
        )?;
        tx.commit()?;
        Ok(record)
    }

    pub fn list_usage(&self, agent_id: Option<&AgentId>) -> Result<Vec<UsageRecord>, Error> {
        let conn = self.conn();
        let sql = match agent_id {
            Some(id) => {
                agent_by_id(&conn, id)?;
                "SELECT id, agent_id, harness, model, input_tokens, output_tokens,
                        cache_read_tokens, cache_write_tokens, reported_cost_micros,
                        estimated_cost_micros, billing_mode, quota_state, day, recorded_at_ms
                 FROM usage_records WHERE agent_id = ?1 ORDER BY recorded_at_ms, id"
            }
            None => {
                "SELECT id, agent_id, harness, model, input_tokens, output_tokens,
                        cache_read_tokens, cache_write_tokens, reported_cost_micros,
                        estimated_cost_micros, billing_mode, quota_state, day, recorded_at_ms
                 FROM usage_records ORDER BY recorded_at_ms, id"
            }
        };
        let mut stmt = conn.prepare(sql)?;
        let mut rows = match agent_id {
            Some(id) => stmt.query(params![id.as_str()])?,
            None => stmt.query([])?,
        };
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(usage_from_row(row)?);
        }
        Ok(records)
    }

    fn message_at(&self, agent_id: &AgentId, seq: i64) -> Result<Option<Message>, Error> {
        let conn = self.conn();
        let found = conn
            .query_row(
                &format!("SELECT {MESSAGE_COLS} FROM messages WHERE agent_id = ?1 AND seq = ?2"),
                params![agent_id.as_str(), seq],
                |row| Ok(message_from_row(&conn, row)),
            )
            .optional()?;
        found.transpose()
    }

    fn age_at(&self, agent_id: &AgentId, seq: i64, now_ms: i64) -> Result<Option<i64>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(
            "SELECT received_at_ms, context FROM messages WHERE agent_id = ?1 AND seq >= ?2 ORDER BY seq ASC",
        )?;
        let mut rows = stmt.query(params![agent_id.as_str(), seq])?;
        while let Some(row) = rows.next()? {
            let received_at: i64 = row.get(0)?;
            let ctx_raw: Option<String> = row.get(1)?;
            let is_passive = ctx_raw
                .as_deref()
                .and_then(|s| serde_json::from_str::<anyagents_harness::Context>(s).ok())
                .is_some_and(|ctx| ctx.event.as_deref() == Some("reaction_passive"));
            if !is_passive {
                return Ok(Some((now_ms - received_at).max(0)));
            }
        }
        Ok(None)
    }

    pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn read_conn(&self) -> ReadConn<'_> {
        if !self.db_path.as_os_str().is_empty()
            && let Ok(c) = Connection::open_with_flags(
                &self.db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
        {
            let _ = c.busy_timeout(Duration::from_secs(30));
            let _ = c.pragma_update(None, "query_only", "ON");
            return ReadConn::Owned(c);
        }
        ReadConn::Guard(self.conn())
    }

    pub fn create_schedule(&self, input: NewSchedule) -> Result<AgentSchedule, Error> {
        let now = now_ms();
        let mut rand_bytes = [0u8; 8];
        if let Ok(mut f) = File::open("/dev/urandom") {
            let _ = f.read_exact(&mut rand_bytes);
        }
        let id = format!("sch_{:012x}{}", now, hex(&rand_bytes));
        let conn = self.conn();
        agent_by_id(&conn, &input.agent_id)?;
        // Deduplicate identical active recurring/watch schedules for the same agent.
        if (input.kind == "every" || input.kind == "watch")
            && let Some(existing_id) = conn
                .query_row(
                    "SELECT id FROM agent_schedules
                     WHERE agent_id = ?1 AND kind = ?2 AND spec = ?3 AND prompt = ?4 AND state = 'active'
                     LIMIT 1",
                    params![input.agent_id.as_str(), input.kind, input.spec, input.prompt],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
        {
            conn.execute(
                "UPDATE agent_schedules
                 SET interval_ms = ?2, next_fire_at_ms = ?3, watch_baseline = COALESCE(?4, watch_baseline)
                 WHERE id = ?1",
                params![
                    existing_id,
                    input.interval_ms,
                    input.next_fire_at_ms,
                    input.watch_baseline
                ],
            )?;
            return schedule_by_id(&conn, &existing_id);
        }
        conn.execute(
            "INSERT INTO agent_schedules (
                id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms,
                last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?8, 'active', ?9)",
            params![
                id,
                input.agent_id.as_str(),
                input.kind,
                input.spec,
                input.prompt,
                input.interval_ms,
                input.next_fire_at_ms,
                input.watch_baseline,
                now,
            ],
        )?;
        schedule_by_id(&conn, &id)
    }

    pub fn list_schedules(
        &self,
        agent_id: Option<&AgentId>,
        active_only: bool,
    ) -> Result<Vec<AgentSchedule>, Error> {
        let conn = self.read_conn();
        let mut out = Vec::new();
        match (agent_id, active_only) {
            (Some(aid), true) => {
                let mut stmt = conn.prepare(
                    "SELECT id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms,
                            last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms
                     FROM agent_schedules
                     WHERE agent_id = ?1 AND state = 'active'
                     ORDER BY created_at_ms ASC, id ASC",
                )?;
                let rows = stmt.query_map([aid.as_str()], schedule_from_row)?;
                for row in rows {
                    out.push(row?);
                }
            }
            (Some(aid), false) => {
                let mut stmt = conn.prepare(
                    "SELECT id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms,
                            last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms
                     FROM agent_schedules
                     WHERE agent_id = ?1
                     ORDER BY created_at_ms ASC, id ASC",
                )?;
                let rows = stmt.query_map([aid.as_str()], schedule_from_row)?;
                for row in rows {
                    out.push(row?);
                }
            }
            (None, true) => {
                let mut stmt = conn.prepare(
                    "SELECT id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms,
                            last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms
                     FROM agent_schedules
                     WHERE state = 'active'
                     ORDER BY created_at_ms ASC, id ASC",
                )?;
                let rows = stmt.query_map([], schedule_from_row)?;
                for row in rows {
                    out.push(row?);
                }
            }
            (None, false) => {
                let mut stmt = conn.prepare(
                    "SELECT id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms,
                            last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms
                     FROM agent_schedules
                     ORDER BY created_at_ms ASC, id ASC",
                )?;
                let rows = stmt.query_map([], schedule_from_row)?;
                for row in rows {
                    out.push(row?);
                }
            }
        }
        Ok(out)
    }

    pub fn get_schedule(&self, id: &str) -> Result<Option<AgentSchedule>, Error> {
        let conn = self.conn();
        conn.query_row(
            "SELECT id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms,
                    last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms
             FROM agent_schedules WHERE id = ?1",
            [id.trim()],
            schedule_from_row,
        )
        .optional()
        .map_err(Error::from)
    }

    pub fn cancel_schedule(
        &self,
        agent_id: Option<&AgentId>,
        id_or_all: &str,
    ) -> Result<usize, Error> {
        let target = id_or_all.trim();
        let conn = self.conn();
        let count = if target.eq_ignore_ascii_case("all") || target == "*" {
            match agent_id {
                Some(aid) => conn.execute(
                    "UPDATE agent_schedules
                     SET state = 'cancelled', next_fire_at_ms = NULL
                     WHERE agent_id = ?1 AND state = 'active'",
                    [aid.as_str()],
                )?,
                None => conn.execute(
                    "UPDATE agent_schedules
                     SET state = 'cancelled', next_fire_at_ms = NULL
                     WHERE state = 'active'",
                    [],
                )?,
            }
        } else {
            match agent_id {
                Some(aid) => conn.execute(
                    "UPDATE agent_schedules
                     SET state = 'cancelled', next_fire_at_ms = NULL
                     WHERE id = ?1 AND agent_id = ?2 AND state = 'active'",
                    params![target, aid.as_str()],
                )?,
                None => conn.execute(
                    "UPDATE agent_schedules
                     SET state = 'cancelled', next_fire_at_ms = NULL
                     WHERE id = ?1 AND state = 'active'",
                    [target],
                )?,
            }
        };
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim_schedule_tick(
        &self,
        id: &str,
        expected_next_fire_at_ms: Option<i64>,
        new_state: &str,
        new_next_fire_at_ms: Option<i64>,
        fired_at_ms: Option<i64>,
        watch_baseline: Option<&str>,
        last_observed: Option<&str>,
    ) -> Result<bool, Error> {
        claim_schedule_tick_on(
            &self.conn(),
            id,
            expected_next_fire_at_ms,
            new_state,
            new_next_fire_at_ms,
            fired_at_ms,
            watch_baseline,
            last_observed,
        )
    }

    /// Atomically claim a timer/watch transition and retain its mailbox wake.
    /// The caller holds the agent lifecycle lock and checks external retirement
    /// markers. A failed index/mail insert rolls back the claim for retry.
    #[allow(clippy::too_many_arguments)]
    pub fn claim_schedule_tick_with_wake(
        &self,
        id: &str,
        expected_next_fire_at_ms: Option<i64>,
        new_state: &str,
        new_next_fire_at_ms: Option<i64>,
        fired_at_ms: Option<i64>,
        watch_baseline: Option<&str>,
        last_observed: Option<&str>,
        agent_id: &AgentId,
        incoming: Incoming,
    ) -> Result<bool, Error> {
        let outcome = self.receive_with_admission(agent_id, incoming, true, |conn| {
            let eligible: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM agent_schedules s JOIN agents a ON a.id=s.agent_id
                 WHERE s.id=?1 AND s.agent_id=?2 AND a.state NOT IN ('paused','archived'))",
                params![id, agent_id.as_str()],
                |r| r.get(0),
            )?;
            if !eligible {
                return Ok(false);
            }
            claim_schedule_tick_on(
                conn,
                id,
                expected_next_fire_at_ms,
                new_state,
                new_next_fire_at_ms,
                fired_at_ms,
                watch_baseline,
                last_observed,
            )
        })?;
        Ok(outcome.is_some())
    }

    pub fn restore_skipped_at_schedules(&self, min_next_fire_at_ms: i64) -> Result<usize, Error> {
        let conn = self.conn();
        let mut updated = conn.execute(
            "UPDATE agent_schedules
             SET state = 'active',
                 last_observed = NULL
             WHERE kind = 'at'
               AND state = 'fired'
               AND last_fired_at_ms IS NULL
               AND last_observed LIKE 'skipped:%'
               AND next_fire_at_ms >= ?1",
            params![min_next_fire_at_ms],
        )?;
        updated += conn.execute(
            "UPDATE agent_schedules
             SET state = 'active',
                 watch_baseline = 'checks_completed'
             WHERE kind = 'watch'
               AND state = 'fired'
               AND last_observed = 'checks_completed'
               AND (
                   spec LIKE 'github:pr:%'
                   OR spec LIKE 'gh:pr:%'
                   OR spec LIKE 'pr:%'
                   OR spec LIKE 'https://github.com/%/pull/%'
               )
               AND NOT EXISTS (
                   SELECT 1 FROM agent_schedules active_s
                   WHERE active_s.agent_id = agent_schedules.agent_id
                     AND active_s.spec = agent_schedules.spec
                     AND active_s.state = 'active'
               )",
            [],
        )?;
        Ok(updated)
    }

    pub fn update_schedule_watch_observation(
        &self,
        id: &str,
        watch_baseline: Option<&str>,
        last_observed: Option<&str>,
    ) -> Result<(), Error> {
        let conn = self.conn();
        conn.execute(
            "UPDATE agent_schedules
             SET watch_baseline = COALESCE(?2, watch_baseline),
                 last_observed = COALESCE(?3, last_observed)
             WHERE id = ?1 AND state = 'active'",
            params![id, watch_baseline, last_observed],
        )?;
        Ok(())
    }
}

fn schedule_by_id(conn: &Connection, id: &str) -> Result<AgentSchedule, Error> {
    conn.query_row(
        "SELECT id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms,
                last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms
         FROM agent_schedules WHERE id = ?1",
        [id],
        schedule_from_row,
    )
    .optional()?
    .ok_or_else(|| Error::Message(format!("schedule not found: {id}")))
}

fn schedule_from_row(row: &rusqlite::Row<'_>) -> Result<AgentSchedule, rusqlite::Error> {
    let raw_aid: String = row.get(1)?;
    let agent_id = AgentId::parse(&raw_aid).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err.to_string(),
            )),
        )
    })?;
    Ok(AgentSchedule {
        id: row.get(0)?,
        agent_id,
        kind: row.get(2)?,
        spec: row.get(3)?,
        prompt: row.get(4)?,
        interval_ms: row.get(5)?,
        next_fire_at_ms: row.get(6)?,
        last_fired_at_ms: row.get(7)?,
        watch_baseline: row.get(8)?,
        last_observed: row.get(9)?,
        state: row.get(10)?,
        created_at_ms: row.get(11)?,
    })
}

/// `{workspaces}/{id}` when `ANYAGENTS_WORKSPACES_DIR` is configured,
/// otherwise `{data}/agents/{id}/work`.
pub fn agent_work_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
    if let Some(workspaces) = crate::storage::configured_workspaces_dir() {
        return workspaces.join(id.as_str());
    }
    legacy_agent_work_dir(data_dir, id)
}

/// Single-root `{data}/agents/{id}/work` path.
pub fn legacy_agent_work_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
    data_dir.join("agents").join(id.as_str()).join("work")
}

/// `{data}/agents/{id}/config`.
pub fn agent_config_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
    data_dir.join("agents").join(id.as_str()).join("config")
}

/// Reads the active root agent roster `(name, role, public_description)` from
/// `{data}/state.db` read-only when present, without creating or locking the database.
pub fn read_root_roster(data_dir: &Path) -> Vec<(String, Option<String>, Option<String>)> {
    let db_path = data_dir.join("state.db");
    if !db_path.is_file() {
        return Vec::new();
    }
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let Ok(conn) = Connection::open_with_flags(&db_path, flags) else {
        return Vec::new();
    };
    let _ = conn.busy_timeout(Duration::from_millis(500));
    let query_with_desc = "SELECT name, role, description, instructions
         FROM agents
         WHERE parent_id IS NULL AND state = 'running'
         ORDER BY name ASC";
    let mut roster = Vec::new();
    if let Ok(mut stmt) = conn.prepare(query_with_desc)
        && let Ok(rows) = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let role: Option<String> = row.get(1)?;
            let description: Option<String> = row.get(2)?;
            let instructions: Option<String> = row.get(3)?;
            Ok((name, role, description, instructions))
        })
    {
        for item in rows.flatten() {
            let (name, role, description, instructions) = item;
            let dummy = Agent {
                id: AgentId::generate(),
                name: name.clone(),
                role: role.clone(),
                parent_id: None,
                harness: String::new(),
                model: None,
                workdir: None,
                description,
                instructions,
                is_admin: false,
                state: "running".to_string(),
                created_at_ms: 0,
                launch_env: Vec::new(),
            };
            roster.push((name, role, dummy.public_description()));
        }
        return roster;
    }
    if let Ok(mut stmt) = conn.prepare(
        "SELECT name, role, instructions
         FROM agents
         WHERE parent_id IS NULL AND state = 'running'
         ORDER BY name ASC",
    ) && let Ok(rows) = stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        let role: Option<String> = row.get(1)?;
        let instructions: Option<String> = row.get(2)?;
        Ok((name, role, instructions))
    }) {
        for item in rows.flatten() {
            let (name, role, instructions) = item;
            let dummy = Agent {
                id: AgentId::generate(),
                name: name.clone(),
                role: role.clone(),
                parent_id: None,
                harness: String::new(),
                model: None,
                workdir: None,
                description: None,
                instructions,
                is_admin: false,
                state: "running".to_string(),
                created_at_ms: 0,
                launch_env: Vec::new(),
            };
            roster.push((name, role, dummy.public_description()));
        }
    }
    roster
}

/// Reads the active direct child sub-agents `(name, role, public_description)` for `parent_id`
/// from `{data}/state.db` read-only, regardless of role label.
pub fn read_children_roster(
    data_dir: &Path,
    parent_id: &str,
) -> Vec<(String, Option<String>, Option<String>)> {
    let db_path = data_dir.join("state.db");
    if !db_path.is_file() {
        return Vec::new();
    }
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let Ok(conn) = Connection::open_with_flags(&db_path, flags) else {
        return Vec::new();
    };
    let _ = conn.busy_timeout(Duration::from_millis(500));
    let query = "SELECT name, role, description, instructions
         FROM agents
         WHERE parent_id = ?1 AND state != 'archived'
         ORDER BY name ASC";
    let mut roster = Vec::new();
    if let Ok(mut stmt) = conn.prepare(query)
        && let Ok(rows) = stmt.query_map([parent_id], |row| {
            let name: String = row.get(0)?;
            let role: Option<String> = row.get(1)?;
            let description: Option<String> = row.get(2)?;
            let instructions: Option<String> = row.get(3)?;
            Ok((name, role, description, instructions))
        })
    {
        for item in rows.flatten() {
            let (name, role, description, instructions) = item;
            let dummy = Agent {
                id: AgentId::generate(),
                name: name.clone(),
                role: role.clone(),
                parent_id: None,
                harness: String::new(),
                model: None,
                workdir: None,
                description,
                instructions,
                is_admin: false,
                state: "running".to_string(),
                created_at_ms: 0,
                launch_env: Vec::new(),
            };
            roster.push((name, role, dummy.public_description()));
        }
    }
    roster
}

/// UTC calendar day for a Unix millisecond timestamp, `YYYY-MM-DD`.
pub fn utc_day(unix_ms: i64) -> String {
    let days = unix_ms.div_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Howard Hinnant's `civil_from_days`. `days` is days since 1970-01-01.
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year as i32, month as u32, day as u32)
}

/// One summed usage row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageTotal {
    pub agent_id: AgentId,
    pub day: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reported_cost_micros: Option<i64>,
}

/// Sum usage rows by agent, or by agent and day.
pub fn sum_usage(records: &[UsageRecord], by_day: bool) -> Vec<UsageTotal> {
    use std::collections::BTreeMap;
    let mut totals: BTreeMap<(String, String), UsageTotal> = BTreeMap::new();
    for record in records {
        let day_key = if by_day {
            record.day.clone()
        } else {
            String::new()
        };
        let key = (record.agent_id.as_str().to_string(), day_key);
        let entry = totals.entry(key).or_insert_with(|| UsageTotal {
            agent_id: record.agent_id.clone(),
            day: if by_day {
                Some(record.day.clone())
            } else {
                None
            },
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            reported_cost_micros: None,
        });
        entry.input_tokens += record.input_tokens;
        entry.output_tokens += record.output_tokens;
        entry.cache_read_tokens += record.cache_read_tokens;
        entry.cache_write_tokens += record.cache_write_tokens;
        if let Some(cost) = record.reported_cost_micros {
            entry.reported_cost_micros = Some(entry.reported_cost_micros.unwrap_or(0) + cost);
        }
    }
    totals.into_values().collect()
}

/// How a structured usage query groups its detail buckets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageGroupBy {
    Agent,
    Day,
    Model,
    Harness,
}

impl UsageGroupBy {
    /// Parse a caller-supplied grouping. Empty text means per-agent buckets.
    pub fn parse(text: &str) -> Result<Self, String> {
        match text.trim().to_ascii_lowercase().as_str() {
            "" | "agent" => Ok(Self::Agent),
            "day" => Ok(Self::Day),
            "model" => Ok(Self::Model),
            "harness" => Ok(Self::Harness),
            other => Err(format!(
                "unsupported group_by `{other}`: expected agent, day, model or harness"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Day => "day",
            Self::Model => "model",
            Self::Harness => "harness",
        }
    }
}

/// Inclusive time-window and grouping options for a usage aggregation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UsageFilter {
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    /// `days = N` keeps records with `recorded_at_ms >= now_ms - N days`,
    /// intersected with any explicit bounds.
    pub days: Option<i64>,
    pub group_by: UsageGroupBy,
}

impl Default for UsageFilter {
    fn default() -> Self {
        Self {
            since_ms: None,
            until_ms: None,
            days: None,
            group_by: UsageGroupBy::Agent,
        }
    }
}

impl UsageFilter {
    /// Resolve the effective inclusive `(since_ms, until_ms)` window.
    pub fn window(self, now_ms: i64) -> Result<(Option<i64>, Option<i64>), String> {
        let mut since = self.since_ms;
        if let Some(days) = self.days {
            if days <= 0 {
                return Err("days must be a positive number of days".to_string());
            }
            let span = days
                .checked_mul(86_400_000)
                .ok_or_else(|| "days is too large".to_string())?;
            let bound = now_ms.checked_sub(span).unwrap_or(i64::MIN);
            since = Some(since.map_or(bound, |s| s.max(bound)));
        }
        if let (Some(since), Some(until)) = (since, self.until_ms)
            && since > until
        {
            return Err("since_ms must not be after until_ms".to_string());
        }
        Ok((since, self.until_ms))
    }
}

/// Aggregated usage metrics shared by grand totals and per-bucket detail.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageTotals {
    pub turns: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reported_cost_micros: Option<i64>,
}

impl UsageTotals {
    fn add_record(&mut self, record: &UsageRecord) {
        self.turns += 1;
        self.input_tokens += record.input_tokens;
        self.output_tokens += record.output_tokens;
        self.cache_read_tokens += record.cache_read_tokens;
        self.cache_write_tokens += record.cache_write_tokens;
        if let Some(cost) = record.reported_cost_micros {
            self.reported_cost_micros = Some(self.reported_cost_micros.unwrap_or(0) + cost);
        }
    }

    fn add_totals(&mut self, other: &Self) {
        self.turns += other.turns;
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_write_tokens += other.cache_write_tokens;
        if let Some(cost) = other.reported_cost_micros {
            self.reported_cost_micros = Some(self.reported_cost_micros.unwrap_or(0) + cost);
        }
    }
}

/// One grouped usage bucket. Buckets are always per-agent; the optional
/// `day`/`model`/`harness` fields carry the extra grouping dimension.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageBucket {
    pub agent_id: AgentId,
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(flatten)]
    pub totals: UsageTotals,
}

/// A structured usage aggregation: grand totals plus grouped buckets,
/// deterministically sorted by agent, then day/model/harness.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageReport {
    pub group_by: UsageGroupBy,
    pub agent_ids: Vec<AgentId>,
    pub totals: UsageTotals,
    pub buckets: Vec<UsageBucket>,
}

/// Aggregate `records` for `agent_ids` in the filter window.
///
/// `agent_names` supplies best-effort display names. The window is tested
/// against each record's `recorded_at_ms` with inclusive bounds.
pub fn aggregate_usage(
    records: &[UsageRecord],
    agent_ids: &[AgentId],
    agent_names: &std::collections::HashMap<AgentId, String>,
    filter: UsageFilter,
    now_ms: i64,
) -> Result<UsageReport, String> {
    use std::collections::{BTreeMap, HashSet};
    let (since, until) = filter.window(now_ms)?;
    let allowed: HashSet<&AgentId> = agent_ids.iter().collect();
    // Key: (agent id, day, model, harness); only the grouped dimension is set.
    type Key = (String, String, String, String);
    let mut buckets: BTreeMap<Key, UsageBucket> = BTreeMap::new();
    let mut totals = UsageTotals::default();
    for record in records {
        if !allowed.contains(&record.agent_id) {
            continue;
        }
        if since.is_some_and(|s| record.recorded_at_ms < s)
            || until.is_some_and(|u| record.recorded_at_ms > u)
        {
            continue;
        }
        let (day, model, harness) = match filter.group_by {
            UsageGroupBy::Agent => (None, None, None),
            UsageGroupBy::Day => (Some(record.day.clone()), None, None),
            UsageGroupBy::Model => (None, record.model.clone(), None),
            UsageGroupBy::Harness => (None, None, Some(record.harness.clone())),
        };
        let key: Key = (
            record.agent_id.as_str().to_string(),
            day.clone().unwrap_or_default(),
            model.clone().unwrap_or_default(),
            harness.clone().unwrap_or_default(),
        );
        let bucket = buckets.entry(key).or_insert_with(|| UsageBucket {
            agent_id: record.agent_id.clone(),
            agent_name: agent_names.get(&record.agent_id).cloned(),
            day: day.clone(),
            model: model.clone(),
            harness: harness.clone(),
            totals: UsageTotals::default(),
        });
        bucket.totals.add_record(record);
        totals.add_record(record);
    }
    let bucket_values: Vec<UsageBucket> = buckets.into_values().collect();
    // Fold the grand total from the buckets so the two can never diverge.
    let mut folded = UsageTotals::default();
    for bucket in &bucket_values {
        folded.add_totals(&bucket.totals);
    }
    debug_assert_eq!(totals, folded);
    Ok(UsageReport {
        group_by: filter.group_by,
        agent_ids: agent_ids.to_vec(),
        totals,
        buckets: bucket_values,
    })
}

/// Why a usage query was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UsageAccessError {
    /// The caller may not see usage for the requested target.
    Forbidden(String),
    /// The request itself is invalid (unknown agent, bad parameters).
    Invalid(String),
}

impl std::fmt::Display for UsageAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Forbidden(msg) | Self::Invalid(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for UsageAccessError {}

/// All causal descendants of `root` (excluding `root` itself), following
/// `parent_id` links over `agents`.
pub fn usage_descendants(root: &AgentId, agents: &[Agent]) -> Vec<AgentId> {
    let mut set = std::collections::HashSet::from([root.clone()]);
    let mut changed = true;
    while changed {
        changed = false;
        for agent in agents {
            if let Some(parent) = &agent.parent_id
                && set.contains(parent)
                && set.insert(agent.id.clone())
            {
                changed = true;
            }
        }
    }
    set.remove(root);
    let mut out: Vec<AgentId> = set.into_iter().collect();
    out.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    out
}

/// Whether `caller` may query usage across the whole workspace: the local
/// operator (`None`), a root caller (`parent_id` is `None`), or a caller
/// whose scope is `public`.
pub fn usage_caller_is_privileged(caller: Option<&Agent>) -> bool {
    match caller {
        None => true,
        Some(agent) => agent.parent_id.is_none() || agent.scope() == "public",
    }
}

fn find_agent<'a>(agents: &'a [Agent], target: &str) -> Option<&'a Agent> {
    agents
        .iter()
        .find(|a| a.name.eq_ignore_ascii_case(target) || a.id.as_str() == target)
}

/// Resolve the exact agent set a usage query may read. This is the causal
/// authorization boundary and it fails closed:
///
/// - privileged callers (operator, root, `public` scope) may read every
///   agent, or an explicit target with optional descendants;
/// - any other caller may read only itself and its causal descendants. An
///   omitted target means "my subtree"; naming a parent, sibling or any
///   unrelated agent, or asking for `all`, is forbidden and returns no data.
///
/// `include_descendants` defaults to `false` for an explicitly named target
/// and to `true` when the target is omitted for a non-privileged caller.
pub fn resolve_usage_agents(
    caller: Option<&Agent>,
    agents: &[Agent],
    target: Option<&str>,
    include_descendants: Option<bool>,
) -> Result<Vec<AgentId>, UsageAccessError> {
    let all_ids: Vec<AgentId> = agents.iter().map(|a| a.id.clone()).collect();
    let target = target.map(str::trim).filter(|t| !t.is_empty());
    let privileged = usage_caller_is_privileged(caller);

    if target.is_none_or(|t| t == "all" || t == "*") {
        if privileged {
            return Ok(all_ids);
        }
        if target.is_some() {
            return Err(UsageAccessError::Forbidden(
                "usage query for all agents requires a public or root caller".to_string(),
            ));
        }
        let caller = caller.expect("a non-privileged caller is an agent");
        let mut ids = vec![caller.id.clone()];
        ids.extend(usage_descendants(&caller.id, agents));
        return Ok(ids);
    }

    let target = target.expect("target checked above");
    let target_agent = if target.eq_ignore_ascii_case("self") {
        match caller {
            Some(caller) => agents.iter().find(|a| a.id == caller.id),
            None => None,
        }
        .ok_or_else(|| {
            UsageAccessError::Invalid("agent `self` requires an agent caller".to_string())
        })?
    } else {
        find_agent(agents, target)
            .ok_or_else(|| UsageAccessError::Invalid(format!("unknown agent `{target}`")))?
    };

    if !privileged {
        let caller = caller.expect("a non-privileged caller is an agent");
        let mut visible = std::collections::HashSet::from([caller.id.clone()]);
        visible.extend(usage_descendants(&caller.id, agents));
        if !visible.contains(&target_agent.id) {
            return Err(UsageAccessError::Forbidden(format!(
                "agent `{}` cannot query usage for `{}`",
                caller.name, target_agent.name
            )));
        }
    }

    let mut ids = vec![target_agent.id.clone()];
    if include_descendants.unwrap_or(false) {
        ids.extend(usage_descendants(&target_agent.id, agents));
    }
    Ok(ids)
}

/// A capability as it is stored: a digest under this server's key, which lives
/// only in the server's memory. The table is useless to anyone who reads it,
/// and a different key never reproduces the digest.
fn hash(key: &str, token: &str) -> String {
    let mut bytes = Vec::with_capacity(key.len() + token.len() + 1);
    bytes.extend_from_slice(key.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(token.as_bytes());
    format!("{:016x}", fnv1a(&bytes))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn data_dir_for(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn validate_token(value: &str, what: &str) -> Result<(), Error> {
    if value.is_empty() || value.contains(':') || value.chars().any(char::is_whitespace) {
        Err(Error::Message(format!(
            "{what} is empty or contains whitespace or a colon"
        )))
    } else {
        Ok(())
    }
}

fn channel_with(conn: &Connection, id: &ChannelId) -> Result<Channel, Error> {
    let found = conn
        .query_row(
            "SELECT id, kind, identity, config, credential_ref, created_at_ms, state FROM channels WHERE id = ?1",
            [id.as_str()],
            channel_from_row,
        )
        .optional()?;
    found.ok_or_else(|| Error::ChannelNotFound(id.to_string()))
}

fn agent_by_id(conn: &Connection, id: &AgentId) -> Result<Agent, Error> {
    let found = conn
        .query_row(
            &format!("SELECT {AGENT_COLS} FROM agents WHERE id = ?1"),
            [id.as_str()],
            agent_from_row,
        )
        .optional()?;
    found.ok_or_else(|| Error::AgentNotFound(id.to_string()))
}

/// A launch environment is a list of name and value pairs, stored as JSON so a
/// value with an `=` or a space survives. Values are never keys.
/// The (agent, chat) links a Telegram bot saved in its channel state, read
/// from the `chats` array without depending on the channel crate. Anything
/// unreadable is no link, so a corrupt state closes a room rather than opening
/// it to every agent.
fn telegram_chat_links(state: &str) -> Vec<(String, i64)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(state) else {
        return Vec::new();
    };
    value
        .get("chats")
        .and_then(serde_json::Value::as_array)
        .map(|links| {
            links
                .iter()
                .filter_map(|link| {
                    let agent = link.get("agent")?.as_str()?.to_string();
                    let chat = link.get("chat_id")?.as_i64()?;
                    Some((agent, chat))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn encode_env(env: &[(String, String)]) -> String {
    // A flat list of names and values. `decode_env` reads exactly this shape,
    // so the two are tested against each other.
    let mut out = String::from("[");
    for (index, (name, value)) in env.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&quote(name));
        out.push(',');
        out.push_str(&quote(value));
    }
    out.push(']');
    out
}

fn quote(text: &str) -> String {
    let mut out = String::from("\"");
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            control if control < ' ' => out.push_str(&format!("\\u{:04x}", u32::from(control))),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// Read what [`encode_env`] wrote: a flat JSON array of quoted names and values,
/// e.g. `["K","v","K2","v2"]`. Anything unreadable reads as no environment.
fn decode_env(text: &str) -> Vec<(String, String)> {
    let bytes = text.as_bytes();
    let Some(open) = text.find('[') else {
        return Vec::new();
    };
    let mut at = open + 1;
    let mut env = Vec::new();
    loop {
        let Some((name, next)) = read_string(bytes, at) else {
            return env;
        };
        let Some(comma) = text[next..].find(',') else {
            return env;
        };
        let Some((value, end)) = read_string(bytes, next + comma + 1) else {
            return env;
        };
        env.push((name, value));
        // Skip the comma before the next name, if there is one.
        let rest = &text[end..];
        let Some(comma) = rest.find(',') else {
            return env;
        };
        let after = end + comma + 1;
        if !rest[comma + 1..].trim_start().starts_with('"') {
            return env;
        }
        at = after + (rest[comma + 1..].len() - rest[comma + 1..].trim_start().len());
    }
}

/// One JSON string starting at `at`, and where the closing quote is.
fn read_string(bytes: &[u8], at: usize) -> Option<(String, usize)> {
    if bytes.get(at) != Some(&b'"') {
        return None;
    }
    let mut out = String::new();
    let mut index = at + 1;
    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'"' => return Some((out, index + 1)),
            b'\\' => {
                match bytes.get(index + 1)? {
                    b'n' => out.push('\n'),
                    b't' => out.push('\t'),
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'u' => {
                        let hex = std::str::from_utf8(bytes.get(index + 2..index + 6)?).ok()?;
                        let code = u32::from_str_radix(hex, 16).ok()?;
                        out.push(char::from_u32(code)?);
                        index += 4;
                    }
                    other => out.push(char::from(*other)),
                }
                index += 2;
            }
            _ => {
                let rest = std::str::from_utf8(&bytes[index..]).ok()?;
                let ch = rest.chars().next()?;
                out.push(ch);
                index += ch.len_utf8();
            }
        }
    }
    None
}

fn agent_from_row(row: &rusqlite::Row<'_>) -> Result<Agent, rusqlite::Error> {
    let id: String = row.get(0)?;
    let agent_id = AgentId::parse(&id)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
    let parent_id = match row.get::<_, Option<String>>(9)? {
        Some(raw) if !raw.is_empty() => Some(
            AgentId::parse(&raw)
                .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        ),
        _ => None,
    };
    Ok(Agent {
        is_admin: row.get(10)?,
        id: agent_id,
        name: row.get(1)?,
        role: row.get(11)?,
        description: row.get(12)?,
        harness: row.get(2)?,
        model: row.get(3)?,
        workdir: row.get(4)?,
        instructions: row.get(5)?,
        state: row.get(6)?,
        created_at_ms: row.get(7)?,
        launch_env: row
            .get::<_, Option<String>>(8)?
            .map(|json| decode_env(&json))
            .unwrap_or_default(),
        parent_id,
    })
}

fn channel_from_row(row: &rusqlite::Row<'_>) -> Result<Channel, rusqlite::Error> {
    let id: String = row.get(0)?;
    Ok(Channel {
        id: ChannelId::parse(&id)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        kind: row.get(1)?,
        identity: row.get(2)?,
        config: row.get(3)?,
        credential_ref: row.get(4)?,
        created_at_ms: row.get(5)?,
        state: row.get(6)?,
        agents: Vec::new(),
    })
}

fn agents_of(conn: &Connection, channel: &ChannelId) -> Result<Vec<AgentId>, Error> {
    let mut stmt = conn
        .prepare("SELECT agent_id FROM channel_agents WHERE channel_id = ?1 ORDER BY agent_id")?;
    let ids = stmt.query_map([channel.as_str()], |row| {
        let raw: String = row.get(0)?;
        Ok(raw)
    })?;
    let mut agents = Vec::new();
    for id in ids {
        agents.push(AgentId::parse(&id?)?);
    }
    Ok(agents)
}

fn usage_from_row(row: &rusqlite::Row<'_>) -> Result<UsageRecord, Error> {
    let id: String = row.get(0)?;
    let agent_id: String = row.get(1)?;
    let billing: String = row.get(10)?;
    Ok(UsageRecord {
        id: UsageId::parse(&id)?,
        agent_id: AgentId::parse(&agent_id)?,
        harness: row.get(2)?,
        model: row.get(3)?,
        input_tokens: row.get(4)?,
        output_tokens: row.get(5)?,
        cache_read_tokens: row.get(6)?,
        cache_write_tokens: row.get(7)?,
        reported_cost_micros: row.get(8)?,
        estimated_cost_micros: row.get(9)?,
        billing_mode: BillingMode::parse(&billing)?,
        quota_state: row.get(11)?,
        day: row.get(12)?,
        recorded_at_ms: row.get(13)?,
    })
}

fn message_from_row(conn: &Connection, row: &rusqlite::Row<'_>) -> Result<Message, Error> {
    let agent_id = AgentId::parse(&row.get::<_, String>(1)?)?;
    let delivered = cursor(conn, &agent_id)?;
    message_from_row_with_cursor(row, delivered)
}

fn message_from_row_with_cursor(row: &rusqlite::Row<'_>, delivered: i64) -> Result<Message, Error> {
    let id: String = row.get(0)?;
    let agent_id = AgentId::parse(&row.get::<_, String>(1)?)?;
    let seq: i64 = row.get(2)?;
    Ok(Message {
        id: MessageId::parse(&id)?,
        agent_id,
        seq,
        source_time_ms: row.get(3)?,
        source_key: row.get(4)?,
        sender: row.get(5)?,
        text: row.get(6)?,
        reply_to: row.get(7)?,
        received_at_ms: row.get(8)?,
        ack: if seq <= delivered {
            Ack::Delivered
        } else {
            Ack::Received
        },
        delivered_at_ms: row.get(9)?,
        origin: row
            .get::<_, Option<String>>(10)?
            .and_then(|text| serde_json::from_str(&text).ok()),
        context: row
            .get::<_, Option<String>>(11)?
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default(),
    })
}

/// Strip envelope headers so identical room messages share the same `content_hash`.
pub fn clean_rag_text(text: &str) -> String {
    let mut t = text.trim();
    if let Some(after_hdr) = t.strip_prefix("----- message -----")
        && let Some(idx) = after_hdr.find("-------------------")
    {
        t = after_hdr[idx + "-------------------".len()..].trim();
    }
    t.to_owned()
}

/// Deterministic 128-bit FNV-1a content hash (`32` hex chars) shared by Rust and Python indexers.
pub fn rag_content_hash(text: &str) -> String {
    let cleaned = clean_rag_text(text);
    let bytes = cleaned.as_bytes();
    let mut h1: u64 = 0xcbf29ce484222325;
    let mut h2: u64 = 0x84222325cbf29ce4;
    for (i, &b) in bytes.iter().enumerate() {
        h1 ^= u64::from(b);
        h1 = h1.wrapping_mul(0x100000001b3);
        h2 ^= u64::from(b) ^ ((i as u64) & 0xff);
        h2 = h2.wrapping_mul(0x100000001b3);
    }
    format!("{h1:016x}{h2:016x}")
}

fn is_cjk_char(c: char) -> bool {
    matches!(
        c as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x20000..=0x2A6DF
            | 0x2A700..=0x2B73F
            | 0x2B740..=0x2B81F
            | 0x2B820..=0x2CEAF
    )
}

/// Hybrid ASCII + CJK tokenizer: emits lowercased alphanumeric/identifier tokens
/// (`len >= 2`, plus sub-tokens split on `_` and `-`) together with individual CJK
/// characters and CJK bigrams so English, code identifiers, and Chinese/Cantonese
/// queries achieve high BM25 recall without dense vector embeddings.
pub fn tokenize_hybrid_search_tokens(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut ascii_buf = String::new();
    let mut cjk_run: Vec<char> = Vec::new();

    let flush_ascii = |buf: &mut String, out: &mut Vec<String>| {
        let trimmed = buf.trim_matches(['-', '_']);
        if trimmed.len() >= 2 {
            let lower = trimmed.to_ascii_lowercase();
            out.push(lower.clone());
            if lower.contains('_') || lower.contains('-') {
                for part in lower.split(['_', '-']) {
                    if part.len() >= 2 {
                        out.push(part.to_string());
                    }
                }
            }
        }
        buf.clear();
    };
    let flush_cjk = |run: &mut Vec<char>, out: &mut Vec<String>| {
        for &ch in run.iter() {
            out.push(ch.to_string());
        }
        for pair in run.windows(2) {
            let mut s = String::with_capacity(6);
            s.push(pair[0]);
            s.push(pair[1]);
            out.push(s);
        }
        run.clear();
    };

    for ch in text.chars() {
        if is_cjk_char(ch) {
            flush_ascii(&mut ascii_buf, &mut tokens);
            cjk_run.push(ch);
        } else if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            flush_cjk(&mut cjk_run, &mut tokens);
            ascii_buf.push(ch);
        } else {
            flush_ascii(&mut ascii_buf, &mut tokens);
            flush_cjk(&mut cjk_run, &mut tokens);
        }
    }
    flush_ascii(&mut ascii_buf, &mut tokens);
    flush_cjk(&mut cjk_run, &mut tokens);
    tokens
}

fn classify_chunk_kind_and_tool(
    kind_override: Option<&str>,
    tool_override: Option<&str>,
    sender: &str,
    destination: &str,
    text: &str,
) -> (String, String) {
    if let Some(k) = kind_override.map(str::trim).filter(|s| !s.is_empty()) {
        let t = tool_override
            .map(str::trim)
            .unwrap_or("")
            .to_ascii_lowercase();
        return (k.to_ascii_lowercase(), t);
    }
    let trimmed = text.trim();
    if let Some(rest) = trimmed
        .strip_prefix("[tool:")
        .or_else(|| trimmed.strip_prefix("[tool_call:"))
        .or_else(|| trimmed.strip_prefix("[tool_output:"))
        .or_else(|| trimmed.strip_prefix("[Tool:"))
    {
        let tool = rest
            .split([']', ':', ' '])
            .next()
            .unwrap_or("tool")
            .trim()
            .to_ascii_lowercase();
        return ("tool".to_string(), tool);
    }
    if trimmed.starts_with("[Sub-Agent Completed]") {
        return ("tool".to_string(), "subagents".to_string());
    }
    if trimmed.starts_with("[Schedule") || trimmed.starts_with("[Watch") {
        return ("tool".to_string(), "schedule".to_string());
    }
    if let Some(tool) = sender
        .strip_prefix("tool:")
        .or_else(|| destination.strip_prefix("tool:"))
    {
        return ("tool".to_string(), tool.trim().to_ascii_lowercase());
    }
    ("message".to_string(), String::new())
}

#[allow(clippy::too_many_arguments)]
fn claim_schedule_tick_on(
    conn: &Connection,
    id: &str,
    expected_next_fire_at_ms: Option<i64>,
    new_state: &str,
    new_next_fire_at_ms: Option<i64>,
    fired_at_ms: Option<i64>,
    watch_baseline: Option<&str>,
    last_observed: Option<&str>,
) -> Result<bool, Error> {
    let updated = match expected_next_fire_at_ms {
        Some(expected) => conn.execute(
            "UPDATE agent_schedules
                 SET state = ?3,
                     next_fire_at_ms = ?4,
                     last_fired_at_ms = COALESCE(?5, last_fired_at_ms),
                     watch_baseline = COALESCE(?6, watch_baseline),
                     last_observed = COALESCE(?7, last_observed)
                 WHERE id = ?1 AND state = 'active' AND next_fire_at_ms = ?2",
            params![
                id,
                expected,
                new_state,
                new_next_fire_at_ms,
                fired_at_ms,
                watch_baseline,
                last_observed
            ],
        )?,
        None => conn.execute(
            "UPDATE agent_schedules
                 SET state = ?2,
                     next_fire_at_ms = ?3,
                     last_fired_at_ms = COALESCE(?4, last_fired_at_ms),
                     watch_baseline = COALESCE(?5, watch_baseline),
                     last_observed = COALESCE(?6, last_observed)
                 WHERE id = ?1 AND state = 'active'",
            params![
                id,
                new_state,
                new_next_fire_at_ms,
                fired_at_ms,
                watch_baseline,
                last_observed
            ],
        )?,
    };
    Ok(updated == 1)
}

fn ensure_lexical_fts_backfilled(conn: &mut Connection) -> Result<(), Error> {
    let fts_complete_sql =
        "SELECT (SELECT count(*) FROM rag_chunks) = (SELECT count(*) FROM rag_chunks_fts)
         AND NOT EXISTS (
            SELECT chunk_id, agent_name, sender, destination, kind, tool_name, text FROM rag_chunks
            EXCEPT
            SELECT chunk_id, agent_name, sender, destination, kind, tool_name, text FROM rag_chunks_fts)";
    if conn.query_row(fts_complete_sql, [], |r| r.get::<_, bool>(0))? {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // A nonempty index can still be partial, duplicated or stale. Check the
    // entire derivative membership under the same lock as any rebuild.
    let complete: bool = tx.query_row(fts_complete_sql, [], |r| r.get(0))?;
    if !complete {
        tx.execute("DELETE FROM rag_chunks_fts", [])?;
        let mut sel = tx.prepare(
            "SELECT chunk_id, agent_name, sender, destination, kind, tool_name, text
             FROM rag_chunks",
        )?;
        let mut rows = sel.query([])?;
        let mut ins = tx.prepare(
            "INSERT INTO rag_chunks_fts (
                chunk_id, agent_name, sender, destination, kind, tool_name, text, search_tokens
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        while let Some(row) = rows.next()? {
            let text: String = row.get(6)?;
            let search_tokens = tokenize_hybrid_search_tokens(&text).join(" ");
            ins.execute(params![
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                text,
                search_tokens,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn index_rag_chunk(
    conn: &Connection,
    chunk_id: &str,
    agent_id: &str,
    agent_name: &str,
    direction: &str,
    sender: &str,
    destination: &str,
    timestamp_ms: i64,
    text: &str,
    kind_override: Option<&str>,
    tool_override: Option<&str>,
    seq: i64,
) -> Result<(), Error> {
    let cleaned = clean_rag_text(text);
    if cleaned.chars().count() < 2 {
        return Ok(());
    }
    let chash = rag_content_hash(&cleaned);
    let (kind, tool_name) =
        classify_chunk_kind_and_tool(kind_override, tool_override, sender, destination, &cleaned);
    let search_tokens = tokenize_hybrid_search_tokens(&cleaned).join(" ");
    conn.execute(
        "INSERT OR REPLACE INTO rag_chunks (
            chunk_id, agent_id, agent_name, direction, sender, destination,
            timestamp_ms, content_hash, text, kind, tool_name, seq, text_format
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 2)",
        params![
            chunk_id,
            agent_id,
            agent_name,
            direction,
            sender,
            destination,
            timestamp_ms,
            chash,
            cleaned,
            kind,
            tool_name,
            seq,
        ],
    )?;
    if seq <= 0 {
        conn.execute(
            "UPDATE rag_chunks SET seq = rowid WHERE chunk_id = ?1 AND seq <= 0",
            params![chunk_id],
        )?;
    }
    conn.execute(
        "DELETE FROM rag_chunks_fts WHERE chunk_id MATCH ?1",
        params![format!("\"{}\"", chunk_id.replace('"', ""))],
    )?;
    conn.execute(
        "INSERT INTO rag_chunks_fts (
            chunk_id, agent_name, sender, destination, kind, tool_name, text, search_tokens
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            chunk_id,
            agent_name,
            sender,
            destination,
            kind,
            tool_name,
            cleaned,
            search_tokens
        ],
    )?;
    Ok(())
}

fn build_fts5_query_with_filters(
    queries: &[String],
    agent_name: Option<&str>,
    kind: Option<&str>,
    tool_name: Option<&str>,
) -> String {
    let mut terms = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for raw in queries {
        for tok in tokenize_hybrid_search_tokens(raw) {
            let clean = tok.replace('"', "");
            if !clean.is_empty() && seen.insert(clean.clone()) {
                terms.push(format!("\"{clean}\""));
            }
        }
    }
    if terms.is_empty() {
        return String::new();
    }
    let text_expr = if terms.len() == 1 {
        terms.remove(0)
    } else {
        format!("({})", terms.join(" OR "))
    };
    let mut clauses = vec![text_expr];
    if let Some(a) = agent_name.map(str::trim).filter(|s| !s.is_empty()) {
        let clean = a.replace('"', "");
        if !clean.is_empty() {
            clauses.push(format!("agent_name : \"{clean}\""));
        }
    }
    if let Some(k) = kind.map(str::trim).filter(|s| !s.is_empty()) {
        let clean = k.replace('"', "");
        if !clean.is_empty() {
            clauses.push(format!("kind : \"{clean}\""));
        }
    }
    if let Some(t) = tool_name.map(str::trim).filter(|s| !s.is_empty()) {
        let clean = t.replace('"', "").to_ascii_lowercase();
        if !clean.is_empty() {
            clauses.push(format!("tool_name : \"{clean}\""));
        }
    }
    clauses.join(" AND ")
}

/// SQL fragment for the visibility predicate shared by recent and Raw-RAG
/// reads. Immutable IDs are exact matches; an empty set fails closed.
fn visible_agent_ids_sql(
    column: &str,
    visible_agent_ids: Option<&[String]>,
    first_param: usize,
) -> String {
    let Some(agents) = visible_agent_ids else {
        return String::new();
    };
    if agents.is_empty() {
        return " AND 0".to_string();
    }
    let placeholders = (0..agents.len())
        .map(|offset| format!("?{}", first_param + offset))
        .collect::<Vec<_>>()
        .join(", ");
    format!(" AND {column} IN ({placeholders})")
}

fn scoped_params<'a>(
    bind: &[&'a dyn rusqlite::ToSql],
    visible_agent_ids: Option<&'a [String]>,
) -> Vec<&'a dyn rusqlite::ToSql> {
    let mut bind = bind.to_vec();
    if let Some(agents) = visible_agent_ids {
        bind.extend(agents.iter().map(|agent| agent as &dyn rusqlite::ToSql));
    }
    bind
}

fn message_by_source(
    conn: &Connection,
    agent_id: &AgentId,
    source_key: &str,
) -> Result<Option<Message>, Error> {
    let found = conn
        .query_row(
            &format!(
                "SELECT {MESSAGE_COLS} FROM messages
                 WHERE agent_id = ?1 AND source_key = ?2"
            ),
            params![agent_id.as_str(), source_key],
            |row| Ok(message_from_row(conn, row)),
        )
        .optional()?;
    found.transpose()
}

/// Namespace of the ordinary numeric-origin matcher. Telegram basic-group
/// ids are bot/account-local; a relay's receiver is NOT the target agent's bot.
/// Unknown relay formats are scoped to their exact key, never guessed globally.
/// Non-basic chats retain the ordinary matcher's shared chat-id semantics.
pub(crate) fn origin_namespace(chat_id: i64, source_key: Option<&str>) -> String {
    if !(chat_id < 0 && chat_id > -1_000_000_000_000) {
        return "shared-chat".into();
    }
    let Some(key) = source_key.filter(|k| !k.is_empty()) else {
        return "unscoped".into();
    };
    if let Some(rest) = key.strip_prefix("tg-in:") {
        let parts: Vec<_> = rest.split(':').collect();
        if let [chat, account, message] = parts.as_slice()
            && chat.parse::<i64>() == Ok(chat_id)
            && !account.is_empty()
            && message.parse::<i64>().is_ok()
        {
            return format!("relay:{account}");
        }
        return format!("source:{key}");
    }
    if key.starts_with("room:") {
        return format!("source:{key}");
    }
    // Direct adapter keys are <bot>:<update_id>. The explicit webhook fixture
    // form also carries its account before the final event component.
    if let Some((account, _event)) = key.rsplit_once(':') {
        return format!("direct:{account}");
    }
    format!("source:{key}")
}

fn message_origin_namespace_matches(
    conn: &Connection,
    message: &Message,
    chat: i64,
    incoming_key: Option<&str>,
) -> Result<bool, Error> {
    let namespace = origin_namespace(chat, incoming_key);
    if namespace == "unscoped" {
        return Ok(false);
    }
    let (retained, matched): (i64, bool) = conn.query_row(
        "SELECT count(*), coalesce(max(a.namespace=?3), 0) FROM message_origin_aliases a
         JOIN message_identities i ON i.agent_id=a.agent_id AND i.identity_id=a.identity_id
         WHERE i.agent_id=?1 AND i.target_id=?2 AND a.chat_id=?4 AND a.message_id=?5",
        params![
            message.agent_id.as_str(),
            message.id.as_str(),
            namespace,
            chat,
            message.origin.and_then(|o| o.message_id)
        ],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    Ok(if retained != 0 {
        matched
    } else {
        namespace == origin_namespace(chat, message.source_key.as_deref())
    })
}

/// Retain both sides of every coalescence before an origin/source promotion.
/// This also lazily seeds a pre-upgrade row's still-available canonical identity.
/// Callers share ingress/redaction's IMMEDIATE transaction; errors roll it back.
fn retain_message_aliases(
    conn: &Connection,
    message: &Message,
    incoming_origin: Option<Origin>,
    incoming_source_key: Option<&str>,
) -> Result<String, Error> {
    let seeded = conn.execute(
        "INSERT INTO message_identities (agent_id, target_id, identity_id) VALUES (?1, ?2, ?2) ON CONFLICT DO NOTHING",
        params![message.agent_id.as_str(), message.id.as_str()],
    )?;
    let identity_id: String = conn.query_row(
        "SELECT identity_id FROM message_identities WHERE agent_id = ?1 AND target_id = ?2",
        params![message.agent_id.as_str(), message.id.as_str()],
        |r| r.get(0),
    )?;
    // Only seed the visible route once. After promotion source_key may be
    // cleared; re-inferring its account then would manufacture a broad alias.
    for (origin, key) in [
        (
            message.origin.filter(|_| seeded != 0),
            message.source_key.as_deref(),
        ),
        (incoming_origin, incoming_source_key),
    ] {
        if let Some(Origin {
            chat_id: Some(chat_id),
            message_id: Some(message_id),
            ..
        }) = origin
        {
            conn.execute(
                "INSERT INTO message_origin_aliases (agent_id, identity_id, namespace, chat_id, message_id)
                 VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT DO NOTHING",
                params![message.agent_id.as_str(), identity_id, origin_namespace(chat_id, key), chat_id, message_id],
            )?;
        }
    }
    for key in [message.source_key.as_deref(), incoming_source_key]
        .into_iter()
        .flatten()
        .filter(|key| !key.is_empty())
    {
        conn.execute(
            "INSERT INTO message_source_aliases (agent_id, identity_id, source_key)
             VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING",
            params![message.agent_id.as_str(), identity_id, key],
        )?;
    }
    Ok(identity_id)
}

/// Resolve every retained origin/source alias of an explicitly redacted row,
/// before any ordinary writer. No bounded history scan or body matching. A
/// missing canonical row is a storage error, not permission to append plaintext.
fn redacted_message_by_ingress(
    conn: &Connection,
    agent_id: &AgentId,
    origin: Option<Origin>,
    source_key: Option<&str>,
) -> Result<Option<Message>, Error> {
    let target_id: Option<String> = conn
        .query_row(
            "SELECT r.target_id FROM message_origin_aliases a
         JOIN message_identities i ON i.agent_id = a.agent_id AND i.identity_id = a.identity_id
         JOIN conversation_redactions r ON r.agent_id = i.agent_id AND r.target_id = i.target_id
         WHERE a.agent_id = ?1 AND a.chat_id = ?2 AND a.message_id = ?3 AND a.namespace = ?5 AND a.namespace != 'unscoped'
         UNION ALL
         SELECT r.target_id FROM message_source_aliases a
         JOIN message_identities i ON i.agent_id = a.agent_id AND i.identity_id = a.identity_id
         JOIN conversation_redactions r ON r.agent_id = i.agent_id AND r.target_id = i.target_id
         WHERE a.agent_id = ?1 AND a.source_key = ?4
         LIMIT 1",
            params![
                agent_id.as_str(),
                origin.and_then(|o| o.chat_id),
                origin.and_then(|o| o.message_id),
                source_key,
                origin
                    .and_then(|o| o.chat_id)
                    .map(|chat| origin_namespace(chat, source_key))
            ],
            |r| r.get(0),
        )
        .optional()?;
    let Some(target_id) = target_id else {
        return Ok(None);
    };
    let row = conn.query_row(
        &format!("SELECT {MESSAGE_COLS} FROM messages WHERE agent_id = ?1 AND id = ?2"),
        params![agent_id.as_str(), target_id],
        |r| Ok(message_from_row(conn, r)),
    )??;
    Ok(Some(row))
}

#[allow(clippy::too_many_arguments)]
fn message_by_origin(
    conn: &Connection,
    agent_id: &AgentId,
    chat_id: i64,
    message_id: i64,
    incoming_sender: Option<&str>,
    incoming_source_time_ms: Option<i64>,
    incoming_body: &str,
    incoming_source_key: Option<&str>,
    incoming_reply_to_author: Option<&str>,
    redelivery_identity: &mut Option<String>,
) -> Result<Option<Message>, Error> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {MESSAGE_COLS} FROM messages
         WHERE agent_id = ?1 AND origin IS NOT NULL ORDER BY seq DESC LIMIT 64"
    ))?;
    let mut rows = stmt.query([agent_id.as_str()])?;
    let in_user = telegram_sender_user_key(incoming_sender);
    let in_is_relay =
        incoming_source_key.is_some_and(|k| k.starts_with("tg-in:") || k.starts_with("room:"));
    let is_basic_group = chat_id < 0 && chat_id > -1_000_000_000_000;
    while let Some(row) = rows.next()? {
        let mut msg = message_from_row(conn, row)?;
        let Some(origin) = msg.origin else {
            continue;
        };
        if origin.chat_id != Some(chat_id) {
            continue;
        }
        let stored_is_relay = msg
            .source_key
            .as_deref()
            .is_some_and(|k| k.starts_with("tg-in:") || k.starts_with("room:"));
        let incoming_is_meta_event = incoming_body.starts_with("[Reaction ")
            || incoming_body.starts_with("[Deleted message #");
        let stored_is_meta_event =
            msg.text.starts_with("[Reaction ") || msg.text.starts_with("[Deleted message #");
        let matched_by_id = origin.message_id == Some(message_id)
            && message_origin_namespace_matches(conn, &msg, chat_id, incoming_source_key)?
            && if incoming_is_meta_event || stored_is_meta_event {
                strip_quoted_block(&msg.text) == strip_quoted_block(incoming_body)
                    && in_user == telegram_sender_user_key(msg.sender.as_deref())
            } else {
                true
            };
        // Basic Telegram groups (`chat_type == "group"`, negative non-`-100...` IDs)
        // assign each bot its own account-local `message_id` for the same human
        // message. Match by `(chat_id, sender user, source_time_ms, body)` so cross-bot
        // passive relay and direct webhook delivery coalesce into a single message.
        let matched_by_group_relay = (in_is_relay
            || stored_is_relay
            || (is_basic_group && origin.message_id == Some(message_id)))
            && normalize_telegram_body_for_dedup(strip_quoted_block(&msg.text))
                == normalize_telegram_body_for_dedup(strip_quoted_block(incoming_body))
            && in_user.is_some()
            && in_user == telegram_sender_user_key(msg.sender.as_deref())
            && match (msg.source_time_ms, incoming_source_time_ms) {
                (Some(a), Some(b)) => (a - b).abs() <= 5_000,
                _ => (now_ms() - msg.received_at_ms).abs() <= 15_000,
            };
        if matched_by_id || matched_by_group_relay {
            // Keep BOTH the old and incoming route before promotion clears the
            // source key or replaces the canonical origin, even without a body
            // change and before the late-quote redelivery branch returns None.
            let identity_id = retain_message_aliases(
                conn,
                &msg,
                Some(Origin {
                    chat_id: Some(chat_id),
                    message_id: Some(message_id),
                    thread_id: origin.thread_id,
                }),
                incoming_source_key,
            )?;
            // This canonical identity (including intentional late-quote
            // redeliveries) may have been redacted. Re-resolve after retaining
            // the new alias and return the actual frozen row before either
            // body UPDATE. This shares ingress/redaction's IMMEDIATE transaction.
            let redacted_target: Option<String> = conn
                .query_row(
                    "SELECT r.target_id FROM conversation_redactions r
                 JOIN message_identities i ON i.agent_id=r.agent_id AND i.target_id=r.target_id
                 WHERE i.agent_id=?1 AND i.identity_id=?2 LIMIT 1",
                    params![agent_id.as_str(), identity_id],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(target) = redacted_target {
                let redacted = conn.query_row(
                    &format!("SELECT {MESSAGE_COLS} FROM messages WHERE agent_id=?1 AND id=?2"),
                    params![agent_id.as_str(), target],
                    |r| Ok(message_from_row(conn, r)),
                )??;
                return Ok(Some(redacted));
            }
            let was_unquoted = !has_quoted_block(&msg.text) && has_quoted_block(incoming_body);
            let best_body = if was_unquoted || incoming_body.len() > msg.text.len() {
                Some(incoming_body.to_string())
            } else {
                None
            };
            if !in_is_relay && stored_is_relay {
                let mut updated_origin = origin;
                updated_origin.message_id = Some(message_id);
                let mut updated_context = msg.context.clone();
                updated_context.message_id = Some(message_id);
                if updated_context.reply_to_author.is_none()
                    && let Some(rta) = incoming_reply_to_author
                {
                    updated_context.reply_to_author = Some(rta.to_string());
                }
                let new_body = best_body.as_deref().unwrap_or(&msg.text);
                conn.execute(
                    "UPDATE messages SET origin = ?1, context = ?2, source_key = NULL, body = ?3 WHERE id = ?4",
                    params![
                        serde_json::to_string(&updated_origin).unwrap_or_default(),
                        serde_json::to_string(&updated_context).ok(),
                        new_body,
                        msg.id.as_str(),
                    ],
                )?;
                msg.origin = Some(updated_origin);
                msg.context = updated_context;
                msg.source_key = None;
                if let Some(b) = best_body {
                    msg.text = b;
                }
            } else if best_body.is_some()
                || (msg.context.reply_to_author.is_none() && incoming_reply_to_author.is_some())
            {
                let mut updated_context = msg.context.clone();
                if updated_context.reply_to_author.is_none()
                    && let Some(rta) = incoming_reply_to_author
                {
                    updated_context.reply_to_author = Some(rta.to_string());
                }
                let new_body = best_body.as_deref().unwrap_or(&msg.text);
                conn.execute(
                    "UPDATE messages SET body = ?1, context = ?2 WHERE id = ?3",
                    params![
                        new_body,
                        serde_json::to_string(&updated_context).ok(),
                        msg.id.as_str(),
                    ],
                )?;
                msg.context = updated_context;
                if let Some(b) = best_body {
                    msg.text = b;
                }
            }
            if was_unquoted && cursor(conn, agent_id).unwrap_or(0) >= msg.seq {
                *redelivery_identity = Some(identity_id);
                return Ok(None);
            }
            return Ok(Some(msg));
        }
    }
    Ok(None)
}

fn normalize_telegram_body_for_dedup(s: &str) -> String {
    let mut lines = Vec::new();
    for line in s.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("[Local media (") && trimmed.ends_with(']') {
            continue;
        }
        if (trimmed.starts_with("[Attachment (") || trimmed.starts_with("[Sticker"))
            && let Some(pos) = line.find("file_id=")
        {
            let after = &line[pos + "file_id=".len()..];
            let end_rel = after.find([',', ']']).unwrap_or(after.len());
            let mut norm = String::with_capacity(line.len());
            norm.push_str(&line[..pos + "file_id=".len()]);
            norm.push('*');
            norm.push_str(&after[end_rel..]);
            lines.push(norm);
        } else {
            lines.push(line.to_string());
        }
    }
    lines.join("\n")
}

fn resolve_quote_author(conn: &Connection, quote: &crate::Quote) -> Option<String> {
    let author = quote.author.trim();
    if author.is_empty() {
        return None;
    }
    let resolved = resolve_mentions(conn, author);
    for entry in &resolved {
        if let Some((_, rest)) = entry.split_once("(agent:")
            && let Some(agent) = rest.strip_suffix(')')
        {
            return Some(format!("{author} [agent:{agent}]"));
        }
    }
    if quote.author_is_bot {
        let first_word = author
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
            .next()
            .unwrap_or("");
        if !first_word.is_empty()
            && let Ok(Some(agent)) = conn
                .query_row(
                    "SELECT name FROM agents WHERE LOWER(name) = LOWER(?1)",
                    [first_word],
                    |row| row.get::<_, String>(0),
                )
                .optional()
        {
            return Some(format!("{author} [agent:{agent}]"));
        }
    }
    Some(author.to_string())
}

fn enrich_quote_from_store(
    conn: &Connection,
    quote: &mut crate::Quote,
    incoming_origin: Option<Origin>,
) {
    let chat_id = quote
        .reference
        .as_ref()
        .map(|r| r.chat_id)
        .or_else(|| incoming_origin.and_then(|o| o.chat_id));
    let Some(chat_id) = chat_id else {
        return;
    };
    let mut stored_excerpt = quote
        .reference
        .as_ref()
        .and_then(|r| resolve_origin_message_author_and_excerpt(conn, r.chat_id, r.message_id))
        .map(|(_, excerpt)| excerpt);
    if stored_excerpt.is_none() {
        let base_quote_text: String = quote
            .text
            .lines()
            .filter(|line| {
                let t = line.trim();
                !((t.starts_with("[Attachment (") || t.starts_with("[Local media ("))
                    && t.ends_with(']'))
            })
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string();
        if !base_quote_text.is_empty() {
            let dest_pattern = format!("telegram:chat:{chat_id}%");
            if let Ok(mut stmt) = conn.prepare(
                "SELECT body FROM outbound_messages
                 WHERE destination LIKE ?1
                 ORDER BY sent_at_ms DESC LIMIT 32",
            ) && let Ok(mut rows) = stmt.query([dest_pattern])
            {
                while let Ok(Some(row)) = rows.next() {
                    if let Ok(body) = row.get::<_, String>(0) {
                        let clean = strip_quoted_block(&body).trim().to_string();
                        if clean.starts_with(&base_quote_text)
                            && clean.len() > base_quote_text.len()
                        {
                            stored_excerpt = Some(clean);
                            break;
                        }
                    }
                }
            }
        }
    }
    let Some(excerpt) = stored_excerpt else {
        return;
    };
    if quote.text.trim().is_empty() {
        quote.text = excerpt;
        return;
    }
    for line in excerpt.lines() {
        let trimmed = line.trim();
        if (trimmed.starts_with("[Attachment (") || trimmed.starts_with("[Local media ("))
            && trimmed.ends_with(']')
            && !quote.text.contains(trimmed)
        {
            if !quote.text.ends_with('\n') {
                quote.text.push('\n');
            }
            quote.text.push_str(trimmed);
        }
    }
}

fn resolve_origin_message_author_and_excerpt(
    conn: &Connection,
    chat_id: i64,
    message_id: i64,
) -> Option<(String, String)> {
    let pattern = format!("%\"message_id\":{message_id}%");
    let mut stmt = conn
        .prepare(
            "SELECT sender, body, origin FROM messages
             WHERE origin LIKE ?1
               AND body NOT LIKE '[Reaction %'
               AND body NOT LIKE '[Deleted message #%'
             ORDER BY rowid DESC LIMIT 32",
        )
        .ok()?;
    let mut rows = stmt.query([pattern]).ok()?;
    while let Some(row) = rows.next().ok()? {
        let sender: Option<String> = row.get(0).ok()?;
        let body: String = row.get(1).ok()?;
        let origin_raw: Option<String> = row.get(2).ok()?;
        let Some(origin_str) = origin_raw else {
            continue;
        };
        let Ok(origin) = serde_json::from_str::<Origin>(&origin_str) else {
            continue;
        };
        if origin.chat_id == Some(chat_id) && origin.message_id == Some(message_id) {
            let author = sender.unwrap_or_else(|| "unknown".to_string());
            let clean = strip_quoted_block(&body).trim().to_string();
            return Some((author, clean));
        }
    }
    None
}

fn parse_edited_message_text(text: &str) -> Option<(i64, &str)> {
    let rest = text.strip_prefix("[Edited message #")?;
    let (id_part, body_part) = rest.split_once("]:")?;
    let msg_id = id_part.trim().parse::<i64>().ok()?;
    let raw = body_part.strip_prefix(' ').unwrap_or(body_part);
    Some((msg_id, raw))
}

fn resolve_previous_message_version(
    conn: &Connection,
    agent_id: &AgentId,
    chat_id: i64,
    message_id: i64,
    source_key: Option<&str>,
) -> Option<String> {
    if let Ok(Some(version)) = conn
        .query_row(
            "SELECT body FROM message_versions
             WHERE agent_id = ?1 AND chat_id = ?2 AND message_id = ?3 AND origin_namespace = ?4",
            params![
                agent_id.as_str(),
                chat_id,
                message_id,
                origin_namespace(chat_id, source_key)
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
    {
        return Some(version);
    }
    let pattern = format!("%\"message_id\":{message_id}%");
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {MESSAGE_COLS} FROM messages WHERE origin LIKE ?1
           AND body NOT LIKE '[Edited message #%'
           AND body NOT LIKE '[Reaction %'
           AND body NOT LIKE '[Deleted message #%'
         ORDER BY CASE WHEN agent_id=?2 THEN 0 ELSE 1 END, rowid DESC LIMIT 32"
        ))
        .ok()?;
    let mut rows = stmt.query(params![pattern, agent_id.as_str()]).ok()?;
    while let Some(row) = rows.next().ok()? {
        let message = message_from_row(conn, row).ok()?;
        if let Some(origin) = message.origin
            && origin.chat_id == Some(chat_id)
            && origin.message_id == Some(message_id)
            && message_origin_namespace_matches(conn, &message, chat_id, source_key).ok()?
        {
            return Some(strip_quoted_block(&message.text).trim().to_string());
        }
    }
    None
}

fn latest_message_for_origin(
    conn: &Connection,
    agent_id: &AgentId,
    chat_id: i64,
    message_id: i64,
    source_key: Option<&str>,
) -> Result<Option<Message>, Error> {
    let pattern = format!("%\"message_id\":{message_id}%");
    let mut stmt = conn.prepare(&format!(
        "SELECT {MESSAGE_COLS} FROM messages
         WHERE agent_id = ?1 AND origin LIKE ?2
         ORDER BY seq DESC LIMIT 16"
    ))?;
    let mut rows = stmt.query(params![agent_id.as_str(), pattern])?;
    while let Some(row) = rows.next()? {
        let msg = message_from_row(conn, row)?;
        if let Some(origin) = msg.origin
            && origin.chat_id == Some(chat_id)
            && origin.message_id == Some(message_id)
            && message_origin_namespace_matches(conn, &msg, chat_id, source_key)?
        {
            return Ok(Some(msg));
        }
    }
    Ok(None)
}

pub fn format_edited_message_diff(old: &str, new: &str) -> String {
    let old_trimmed = old.trim();
    let new_trimmed = new.trim();
    if old_trimmed.is_empty() {
        return new_trimmed.to_string();
    }
    let old_lines: Vec<&str> = old_trimmed.lines().map(str::trim_end).collect();
    let new_lines: Vec<&str> = new_trimmed.lines().map(str::trim_end).collect();
    let n = old_lines.len();
    let m = new_lines.len();
    if n * m > 120_000 {
        return new_trimmed.to_string();
    }
    let mut dp = vec![0u16; (n + 1) * (m + 1)];
    let stride = m + 1;
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i * stride + j] = if old_lines[i] == new_lines[j] {
                dp[(i + 1) * stride + j + 1].saturating_add(1)
            } else {
                dp[(i + 1) * stride + j].max(dp[i * stride + j + 1])
            };
        }
    }
    #[derive(Clone, Copy)]
    enum DiffOp<'a> {
        Equal(&'a str),
        Remove(&'a str),
        Add(&'a str),
    }
    let mut ops = Vec::with_capacity(n + m);
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if old_lines[i] == new_lines[j] {
            ops.push(DiffOp::Equal(old_lines[i]));
            i += 1;
            j += 1;
        } else if dp[(i + 1) * stride + j] >= dp[i * stride + j + 1] {
            ops.push(DiffOp::Remove(old_lines[i]));
            i += 1;
        } else {
            ops.push(DiffOp::Add(new_lines[j]));
            j += 1;
        }
    }
    while i < n {
        ops.push(DiffOp::Remove(old_lines[i]));
        i += 1;
    }
    while j < m {
        ops.push(DiffOp::Add(new_lines[j]));
        j += 1;
    }
    let mut keep = vec![false; ops.len()];
    for (idx, op) in ops.iter().enumerate() {
        if matches!(op, DiffOp::Remove(_) | DiffOp::Add(_)) {
            let lo = idx.saturating_sub(1);
            let hi = (idx + 1).min(ops.len().saturating_sub(1));
            for k in &mut keep[lo..=hi] {
                *k = true;
            }
        }
    }
    let mut out = Vec::new();
    let mut skipped_run = false;
    for (idx, op) in ops.iter().enumerate() {
        if !keep[idx] {
            if !skipped_run {
                out.push("...".to_string());
                skipped_run = true;
            }
            continue;
        }
        skipped_run = false;
        match op {
            DiffOp::Equal(line) => out.push(format!("  {line}")),
            DiffOp::Remove(line) => out.push(format!("- {line}")),
            DiffOp::Add(line) => out.push(format!("+ {line}")),
        }
    }
    out.join("\n")
}

fn chat_attached_agent_count(conn: &Connection, chat_id: i64) -> usize {
    let mut distinct_agents = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(
        "SELECT c.state, ca.agent_id
         FROM channels c
         JOIN channel_agents ca ON ca.channel_id = c.id
         WHERE c.kind = 'telegram'",
    ) && let Ok(mut rows) = stmt.query([])
    {
        while let Ok(Some(row)) = rows.next() {
            let Ok(state) = row.get::<_, String>(0) else {
                continue;
            };
            let Ok(aid) = row.get::<_, String>(1) else {
                continue;
            };
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&state) else {
                continue;
            };
            let Some(chats) = val.get("chats").and_then(serde_json::Value::as_array) else {
                continue;
            };
            if chats.iter().any(|link| {
                link.get("chat_id").and_then(serde_json::Value::as_i64) == Some(chat_id)
            }) {
                distinct_agents.insert(aid);
            }
        }
    }
    let pattern = format!("%\"chat_id\":{chat_id}%");
    if let Ok(mut stmt) =
        conn.prepare("SELECT DISTINCT agent_id FROM messages WHERE origin LIKE ?1 LIMIT 16")
        && let Ok(mut rows) = stmt.query([pattern])
    {
        while let Ok(Some(row)) = rows.next() {
            if let Ok(aid) = row.get::<_, String>(0) {
                distinct_agents.insert(aid);
            }
        }
    }
    distinct_agents.len()
}

fn resolve_conversation_participants(
    conn: &Connection,
    chat_id: Option<i64>,
    destination: Option<&str>,
) -> Vec<String> {
    let mut agent_ids = std::collections::BTreeSet::new();
    if let Some(cid) = chat_id
        && cid < 0
    {
        if let Ok(mut stmt) = conn.prepare(
            "SELECT c.state, ca.agent_id
             FROM channels c
             JOIN channel_agents ca ON ca.channel_id = c.id
             WHERE c.kind = 'telegram'",
        ) && let Ok(mut rows) = stmt.query([])
        {
            while let Ok(Some(row)) = rows.next() {
                let Ok(state) = row.get::<_, String>(0) else {
                    continue;
                };
                let Ok(aid) = row.get::<_, String>(1) else {
                    continue;
                };
                let Ok(val) = serde_json::from_str::<serde_json::Value>(&state) else {
                    continue;
                };
                let Some(chats) = val.get("chats").and_then(serde_json::Value::as_array) else {
                    continue;
                };
                if chats.iter().any(|link| {
                    link.get("chat_id").and_then(serde_json::Value::as_i64) == Some(cid)
                }) {
                    agent_ids.insert(aid);
                }
            }
        }
    } else if let Some(group_name) = destination.and_then(|d| d.strip_prefix("group:")) {
        if group_name == "all" {
            if let Ok(mut stmt) = conn.prepare(
                "SELECT id FROM agents WHERE parent_id IS NULL AND state IN ('running', 'idle')",
            ) && let Ok(mut rows) = stmt.query([])
            {
                while let Ok(Some(row)) = rows.next() {
                    if let Ok(aid) = row.get::<_, String>(0) {
                        agent_ids.insert(aid);
                    }
                }
            }
        } else if let Ok(mut stmt) =
            conn.prepare("SELECT agent_id FROM group_agents WHERE group_name = ?1")
            && let Ok(mut rows) = stmt.query([group_name])
        {
            while let Ok(Some(row)) = rows.next() {
                if let Ok(aid) = row.get::<_, String>(0) {
                    agent_ids.insert(aid);
                }
            }
        }
    }
    if agent_ids.len() <= 1 {
        return Vec::new();
    }
    let mut participants = Vec::new();
    for aid in agent_ids {
        if let Ok((name, role, state)) = conn.query_row(
            "SELECT name, role, state FROM agents WHERE id = ?1",
            [&aid],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        ) {
            if state == "archived" {
                continue;
            }
            if let Some(role) = role.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                participants.push(format!("{name} ({role})"));
            } else {
                participants.push(name);
            }
        }
    }
    participants.sort();
    participants
}

fn format_agent_topology_tag(name: &str, role: Option<&str>) -> String {
    match role.map(str::trim).filter(|s| !s.is_empty()) {
        Some(r) => format!("agent:{name} [{r}]"),
        None => format!("agent:{name}"),
    }
}

fn xml_escape_attr_local(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn is_ancestor_in_conn(conn: &Connection, ancestor_id: &AgentId, target: &Agent) -> bool {
    let mut cursor = target.parent_id.clone();
    let mut depth = 0usize;
    while let Some(pid) = cursor {
        if &pid == ancestor_id {
            return true;
        }
        depth += 1;
        if depth > 32 {
            break;
        }
        cursor = agent_by_id(conn, &pid).ok().and_then(|a| a.parent_id);
    }
    false
}

fn resolve_sender_relationship_and_role(
    conn: &Connection,
    recipient: Option<&Agent>,
    sender: Option<&str>,
) -> (Option<String>, Option<String>) {
    let Some(sender_raw) = sender.map(str::trim).filter(|s| !s.is_empty()) else {
        return (None, None);
    };
    if sender_raw.starts_with("telegram:user:") {
        return (None, Some("external_user".to_string()));
    }
    if sender_raw == "operator" {
        return (None, Some("operator".to_string()));
    }
    let Some(sender_reference) = sender_raw.strip_prefix("agent:") else {
        return (None, None);
    };
    // An admitted immutable sender cannot change identity while waiting for
    // publication. Syntactically valid IDs are never resolved as display names,
    // even if their exact ID is absent. Non-ID legacy senders keep name lookup.
    let sender_id = match AgentId::parse(sender_reference) {
        Ok(id) => id,
        Err(_) => {
            let Some(id) = conn
                .query_row(
                    "SELECT id FROM agents WHERE name = ?1",
                    [sender_reference],
                    |r| r.get::<_, String>(0),
                )
                .ok()
                .and_then(|id| AgentId::parse(&id).ok())
            else {
                return (None, None);
            };
            id
        }
    };
    let Ok(sender_agent) = agent_by_id(conn, &sender_id) else {
        return (None, None);
    };
    let sender_role = sender_agent
        .role
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let Some(rec) = recipient else {
        return (sender_role, None);
    };
    let rel = if sender_agent.id == rec.id {
        "self"
    } else if rec.parent_id.as_ref() == Some(&sender_agent.id) {
        "parent"
    } else if sender_agent.parent_id.as_ref() == Some(&rec.id) {
        "child"
    } else if rec.parent_id.is_some() && rec.parent_id == sender_agent.parent_id {
        "sibling"
    } else if is_ancestor_in_conn(conn, &sender_agent.id, rec) {
        "ancestor"
    } else if is_ancestor_in_conn(conn, &rec.id, &sender_agent) {
        "descendant"
    } else {
        "peer"
    };
    (sender_role, Some(rel.to_string()))
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PersistedTopologyState {
    scope: String,
    parent: Option<String>,
    peers: Vec<String>,
    children: Vec<String>,
    groups: Vec<String>,
}

fn compute_and_diff_topology_event(
    conn: &Connection,
    recipient: Option<&Agent>,
    now_ms: i64,
) -> Option<String> {
    let rec = recipient?;
    let scope = rec.scope().to_string();
    let parent = rec
        .parent_id
        .as_ref()
        .and_then(|pid| agent_by_id(conn, pid).ok())
        .map(|p| format_agent_topology_tag(&p.name, p.role.as_deref()));

    let mut peers = Vec::new();
    if rec.parent_id.is_none() {
        if let Ok(mut stmt) = conn.prepare(
            "SELECT id FROM agents WHERE parent_id IS NULL AND state != 'archived' AND id != ?1 ORDER BY name ASC",
        ) && let Ok(rows) = stmt.query_map([rec.id.as_str()], |r| r.get::<_, String>(0))
        {
            for id_str in rows.flatten() {
                if let Ok(aid) = AgentId::parse(&id_str)
                    && let Ok(peer) = agent_by_id(conn, &aid)
                {
                    peers.push(format_agent_topology_tag(&peer.name, peer.role.as_deref()));
                }
            }
        }
    } else if rec.allows_sibling_messages()
        && let Some(ref pid) = rec.parent_id
        && let Ok(mut stmt) = conn.prepare(
            "SELECT id FROM agents WHERE parent_id = ?1 AND state != 'archived' AND id != ?2 ORDER BY name ASC",
        )
        && let Ok(rows) = stmt.query_map(params![pid.as_str(), rec.id.as_str()], |r| {
            r.get::<_, String>(0)
        })
    {
        for id_str in rows.flatten() {
            if let Ok(aid) = AgentId::parse(&id_str)
                && let Ok(sib) = agent_by_id(conn, &aid)
                && sib.allows_sibling_messages()
            {
                peers.push(format_agent_topology_tag(&sib.name, sib.role.as_deref()));
            }
        }
    }

    let mut children = Vec::new();
    if let Ok(mut stmt) = conn.prepare(
        "SELECT id FROM agents WHERE parent_id = ?1 AND state != 'archived' ORDER BY name ASC",
    ) && let Ok(rows) = stmt.query_map([rec.id.as_str()], |r| r.get::<_, String>(0))
    {
        for id_str in rows.flatten() {
            if let Ok(aid) = AgentId::parse(&id_str)
                && let Ok(child) = agent_by_id(conn, &aid)
            {
                children.push(format_agent_topology_tag(
                    &child.name,
                    child.role.as_deref(),
                ));
            }
        }
    }

    let mut groups = Vec::new();
    if let Ok(mut stmt) = conn
        .prepare("SELECT group_name FROM group_agents WHERE agent_id = ?1 ORDER BY group_name ASC")
        && let Ok(rows) = stmt.query_map([rec.id.as_str()], |r| r.get::<_, String>(0))
    {
        for g in rows.flatten() {
            groups.push(format!("group:{g}"));
        }
    }

    let current = PersistedTopologyState {
        scope: scope.clone(),
        parent: parent.clone(),
        peers: peers.clone(),
        children: children.clone(),
        groups: groups.clone(),
    };
    let current_json = serde_json::to_string(&current).ok()?;

    let prev_json: Option<String> = conn
        .query_row(
            "SELECT state_json FROM agent_topology_state WHERE agent_id = ?1",
            [rec.id.as_str()],
            |r| r.get(0),
        )
        .optional()
        .ok()
        .flatten();

    match prev_json.and_then(|s| serde_json::from_str::<PersistedTopologyState>(&s).ok()) {
        None => {
            let _ = conn.execute(
                "INSERT INTO agent_topology_state (agent_id, state_json, updated_at_ms)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (agent_id) DO UPDATE SET state_json = excluded.state_json, updated_at_ms = excluded.updated_at_ms",
                params![rec.id.as_str(), current_json, now_ms],
            );
            let self_tag = format_agent_topology_tag(&rec.name, rec.role.as_deref());
            let mut tag = format!(
                "<topology event=\"initial\" self=\"{}\" scope=\"{}\"",
                xml_escape_attr_local(&self_tag),
                xml_escape_attr_local(&scope)
            );
            if let Some(ref p) = parent {
                tag.push_str(&format!(" parent=\"{}\"", xml_escape_attr_local(p)));
            }
            if !peers.is_empty() {
                let attr_name = if rec.parent_id.is_some() {
                    "siblings"
                } else {
                    "peers"
                };
                tag.push_str(&format!(
                    " {attr_name}=\"{}\"",
                    xml_escape_attr_local(&peers.join(", "))
                ));
            }
            if !children.is_empty() {
                tag.push_str(&format!(
                    " children=\"{}\"",
                    xml_escape_attr_local(&children.join(", "))
                ));
            }
            if !groups.is_empty() {
                tag.push_str(&format!(
                    " groups=\"{}\"",
                    xml_escape_attr_local(&groups.join(", "))
                ));
            }
            tag.push_str(" />");
            Some(tag)
        }
        Some(prev) if prev == current => None,
        Some(prev) => {
            let _ = conn.execute(
                "UPDATE agent_topology_state SET state_json = ?1, updated_at_ms = ?2 WHERE agent_id = ?3",
                params![current_json, now_ms, rec.id.as_str()],
            );
            let mut changes = Vec::new();
            if prev.scope != current.scope {
                changes.push(format!("scope: {} -> {}", prev.scope, current.scope));
            }
            if prev.parent != current.parent {
                changes.push(format!(
                    "parent: {}",
                    current.parent.as_deref().unwrap_or("none")
                ));
            }
            let peer_label = if rec.parent_id.is_some() {
                "sibling"
            } else {
                "peer"
            };
            for p in &current.peers {
                if !prev.peers.contains(p) {
                    changes.push(format!("+{peer_label}: {p}"));
                }
            }
            for p in &prev.peers {
                if !current.peers.contains(p) {
                    changes.push(format!("-{peer_label}: {p}"));
                }
            }
            for c in &current.children {
                if !prev.children.contains(c) {
                    changes.push(format!("+child: {c}"));
                }
            }
            for c in &prev.children {
                if !current.children.contains(c) {
                    changes.push(format!("-child: {c}"));
                }
            }
            for g in &current.groups {
                if !prev.groups.contains(g) {
                    changes.push(format!("+group: {g}"));
                }
            }
            for g in &prev.groups {
                if !current.groups.contains(g) {
                    changes.push(format!("-group: {g}"));
                }
            }
            if changes.is_empty() {
                None
            } else {
                Some(format!(
                    "<topology event=\"delta\" changes=\"{}\" />",
                    xml_escape_attr_local(&changes.join("; "))
                ))
            }
        }
    }
}

fn has_quoted_block(text: &str) -> bool {
    text.contains("\n\n----- quoted message -----\n")
        || text.starts_with("----- quoted message -----\n")
}

fn strip_quoted_block(text: &str) -> &str {
    text.split("\n\n----- quoted message -----\n")
        .next()
        .unwrap_or(text)
        .trim()
}

fn telegram_sender_user_key(sender: Option<&str>) -> Option<String> {
    let s = sender?;
    if let Some(rest) = s.strip_prefix("telegram:user:") {
        let uid = rest.split(':').next()?;
        return (!uid.is_empty()).then(|| format!("telegram:user:{uid}"));
    }
    if s.starts_with("agent:") {
        return Some(s.to_string());
    }
    None
}

fn matches_conversation_peer(turn: &ConversationTurn, peer: &str) -> bool {
    let bare = peer
        .strip_prefix("agent:")
        .or_else(|| peer.strip_prefix("group:"))
        .or_else(|| peer.strip_prefix("telegram:chat:"))
        .or_else(|| peer.strip_prefix("telegram:user:"))
        .unwrap_or(peer);
    let fields = [
        Some(turn.from.as_str()),
        Some(turn.to.as_str()),
        turn.reply_to.as_deref(),
    ];
    fields.into_iter().flatten().any(|field| {
        field == peer
            || field == bare
            || field.starts_with(&format!("{peer}:"))
            || field.ends_with(&format!(":{bare}"))
            || field.contains(&format!(":{bare}:"))
    })
}

fn resolve_chat_metadata(conn: &Connection, chat_id: i64) -> (Option<String>, Option<String>) {
    let mut found_title: Option<String> = None;
    let mut found_type: Option<String> = None;
    if let Ok(mut stmt) = conn.prepare("SELECT state FROM channels WHERE kind = 'telegram'")
        && let Ok(mut rows) = stmt.query([])
    {
        while let Ok(Some(row)) = rows.next() {
            let Ok(state) = row.get::<_, String>(0) else {
                continue;
            };
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&state) else {
                continue;
            };
            let Some(chats) = val.get("chats").and_then(serde_json::Value::as_array) else {
                continue;
            };
            for link in chats {
                if link.get("chat_id").and_then(serde_json::Value::as_i64) == Some(chat_id) {
                    if found_title.is_none()
                        && let Some(t) = link
                            .get("title")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty() && *s != "-")
                    {
                        found_title = Some(t.to_string());
                    }
                    if found_type.is_none()
                        && let Some(ct) = link
                            .get("chat_type")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                    {
                        found_type = Some(ct.to_string());
                    }
                }
            }
            if found_title.is_some() && found_type.is_some() {
                break;
            }
        }
    }
    (found_title, found_type)
}

fn resolve_mentions(conn: &Connection, text: &str) -> Vec<String> {
    if !text.contains('@') {
        return Vec::new();
    }
    let handles = extract_at_handles(text);
    if handles.is_empty() {
        return Vec::new();
    }
    let mut bot_to_agent: Vec<(String, String)> = Vec::new();
    if let Ok(mut stmt) = conn.prepare(
        "SELECT c.state, a.name
         FROM channels c
         JOIN channel_agents ca ON ca.channel_id = c.id
         JOIN agents a ON a.id = ca.agent_id
         WHERE c.kind = 'telegram'",
    ) && let Ok(mut rows) = stmt.query([])
    {
        while let Ok(Some(row)) = rows.next() {
            let Ok(state) = row.get::<_, String>(0) else {
                continue;
            };
            let Ok(agent_name) = row.get::<_, String>(1) else {
                continue;
            };
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&state)
                && let Some(bot) = val.get("bot").and_then(serde_json::Value::as_str)
                && let Some((_, username)) = bot.split_once(':')
                && !username.is_empty()
            {
                bot_to_agent.push((username.to_string(), agent_name));
            }
        }
    }
    let mut out = Vec::new();
    for handle in handles {
        let agent_match: Option<String> = conn
            .query_row(
                "SELECT name FROM agents WHERE LOWER(name) = LOWER(?1)",
                [&handle],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten()
            .or_else(|| {
                bot_to_agent
                    .iter()
                    .find(|(bot_user, _)| bot_user.eq_ignore_ascii_case(&handle))
                    .map(|(_, agent)| agent.clone())
            });
        let entry = match agent_match {
            Some(agent) => format!("@{handle} (agent:{agent})"),
            None => format!("@{handle}"),
        };
        if !out.contains(&entry) {
            out.push(entry);
        }
    }
    out
}

fn extract_at_handles(text: &str) -> Vec<String> {
    let mut handles = Vec::new();
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i].1 == '@' {
            let valid_prefix = i == 0 || {
                let prev = chars[i - 1].1;
                !prev.is_ascii_alphanumeric() && prev != '_' && prev != '.'
            };
            if valid_prefix {
                let mut j = i + 1;
                while j < chars.len() {
                    let c = chars[j].1;
                    if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                        j += 1;
                    } else {
                        break;
                    }
                }
                let len = j - (i + 1);
                let followed_by_domain = j + 1 < chars.len()
                    && chars[j].1 == '.'
                    && chars[j + 1].1.is_ascii_alphanumeric();
                if (1..=64).contains(&len) && !followed_by_domain {
                    let start_byte = chars[i + 1].0;
                    let end_byte = if j < chars.len() {
                        chars[j].0
                    } else {
                        text.len()
                    };
                    let handle = text[start_byte..end_byte]
                        .trim_end_matches(['-', '_'])
                        .to_string();
                    if !handle.is_empty() && !handles.contains(&handle) {
                        handles.push(handle);
                    }
                }
                i = j;
                continue;
            }
        }
        i += 1;
    }
    handles
}

fn next_seq(conn: &Connection, agent_id: &AgentId) -> Result<i64, Error> {
    let seq = conn.query_row(
        "SELECT MAX(0, COALESCE(MAX(seq), 0)) + 1 FROM messages WHERE agent_id = ?1",
        [agent_id.as_str()],
        |row| row.get(0),
    )?;
    Ok(seq)
}

fn cursor(conn: &Connection, agent_id: &AgentId) -> Result<i64, Error> {
    let found = conn
        .query_row(
            "SELECT delivered_seq FROM mailbox_cursors WHERE agent_id = ?1",
            [agent_id.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    found.ok_or_else(|| Error::Message(format!("mailbox cursor missing for {agent_id}")))
}

fn bump_delivered(conn: &Connection, agent_id: &AgentId, seq: i64) -> Result<(), Error> {
    let delivered = cursor(conn, agent_id)?;
    if seq != delivered + 1 || !message_exists(conn, agent_id, seq)? {
        return Err(Error::CursorGap {
            cursor: "delivered",
            at: delivered,
            attempted: seq,
        });
    }
    conn.execute(
        "UPDATE mailbox_cursors SET delivered_seq = ?1 WHERE agent_id = ?2",
        params![seq, agent_id.as_str()],
    )?;
    Ok(())
}

fn message_exists(conn: &Connection, agent_id: &AgentId, seq: i64) -> Result<bool, Error> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM messages WHERE agent_id = ?1 AND seq = ?2",
        params![agent_id.as_str(), seq],
        |row| row.get(0),
    )?;
    Ok(count == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, Store) {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        (dir, store)
    }

    fn agent(store: &Store, name: &str) -> Agent {
        store
            .create_agent(NewAgent::new(name, "claude-code"))
            .unwrap()
    }

    #[test]
    fn legacy_harness_rows_migrate_idempotently_without_changing_agent_state() {
        let (dir, store) = setup();
        let parent = agent(&store, "parent");
        let mut expected = Vec::new();
        for (index, legacy) in ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default"]
            .into_iter()
            .enumerate()
        {
            let mut new = NewAgent::new(format!("legacy-{index}"), "spiron");
            new.parent_id = Some(parent.id.clone());
            new.model = Some("sylphx/openai/gpt-6.1-sol#medium".into());
            new.workdir = Some(format!("/work/legacy-{index}"));
            new.instructions = Some("preserve instructions".into());
            let created = store.create_agent(new).unwrap();
            store.set_agent_state(&created.id, "paused").unwrap();
            store
                .receive(&created.id, Incoming::new("delivered"))
                .unwrap();
            store.note_delivered(&created.id, 1).unwrap();
            store
                .receive(&created.id, Incoming::new("pending"))
                .unwrap();
            let schedule = store
                .create_schedule(NewSchedule {
                    agent_id: created.id.clone(),
                    kind: "at".into(),
                    spec: "2099-01-01T00:00:00Z".into(),
                    prompt: "preserve schedule".into(),
                    interval_ms: None,
                    next_fire_at_ms: Some(4_070_908_800_000),
                    watch_baseline: None,
                })
                .unwrap();
            let before = store.agent(&created.id).unwrap();
            // Simulate an existing database, bypassing canonical writes.
            store
                .conn()
                .execute(
                    "UPDATE agents SET harness = ?1 WHERE id = ?2",
                    params![legacy, created.id.as_str()],
                )
                .unwrap();
            expected.push((before, schedule));
        }
        let mut third_party = NewAgent::new("gateway-codex", "codex");
        third_party.model = Some("sylphx/openai/gpt-6.1-sol#xhigh".into());
        let third_party = store.create_agent(third_party).unwrap();
        drop(store);

        for _ in 0..2 {
            let reopened = Store::open(dir.path().join("state.db")).unwrap();
            assert_eq!(reopened.agent(&parent.id).unwrap(), parent);
            assert_eq!(reopened.agent(&third_party.id).unwrap(), third_party);
            for (before, schedule) in &expected {
                assert_eq!(&reopened.agent(&before.id).unwrap(), before);
                assert_eq!(reopened.delivered_cursor(&before.id).unwrap(), 1);
                assert_eq!(
                    reopened.next_undelivered(&before.id).unwrap().unwrap().text,
                    "pending"
                );
                assert_eq!(
                    reopened.get_schedule(&schedule.id).unwrap().as_ref(),
                    Some(schedule)
                );
            }
        }
    }

    #[test]
    fn legacy_harness_writes_are_canonical_and_gateway_models_are_unchanged() {
        let (_dir, store) = setup();
        for (index, legacy) in ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default"]
            .into_iter()
            .enumerate()
        {
            let mut new = NewAgent::new(format!("alias-{index}"), legacy);
            new.model = Some("sylphx/openai/gpt-6.1-sol#medium".into());
            let created = store.create_agent(new).unwrap();
            assert_eq!(created.harness, "spiron");
            assert_eq!(
                created.model.as_deref(),
                Some("sylphx/openai/gpt-6.1-sol#medium")
            );
            store.set_agent_harness(&created.id, "codex").unwrap();
            let switched = store.set_agent_harness(&created.id, legacy).unwrap();
            assert_eq!(switched, created);
        }
    }

    #[test]
    fn inbox_query_limits_unread_and_sender_before_materializing() {
        let (_dir, store) = setup();
        let own = agent(&store, "limited");
        for i in 1..=12 {
            let mut incoming = Incoming::new(format!("message {i}\nsecond line"));
            incoming.sender = Some(
                if i % 2 == 0 {
                    "agent:even"
                } else {
                    "agent:odd"
                }
                .into(),
            );
            store.receive(&own.id, incoming).unwrap();
        }
        for seq in 1..=8 {
            store.note_delivered(&own.id, seq).unwrap();
        }
        let seqs = |rows: Vec<Message>| rows.into_iter().map(|m| m.seq).collect::<Vec<_>>();
        assert_eq!(
            seqs(store.inbox_query(&own.id, None, Some(3), false).unwrap()),
            vec![10, 11, 12]
        );
        assert_eq!(
            seqs(store.inbox_query(&own.id, None, None, true).unwrap()),
            vec![9, 10, 11, 12]
        );
        assert_eq!(
            seqs(
                store
                    .inbox_query(&own.id, Some(10), Some(20), true)
                    .unwrap()
            ),
            vec![11, 12]
        );
        assert_eq!(
            seqs(
                store
                    .inbox_query_filtered(&own.id, None, Some(2), true, Some("agent:odd"))
                    .unwrap()
            ),
            vec![9, 11]
        );
        assert!(
            store
                .inbox_query(&own.id, None, Some(0), false)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.inbox_query(&own.id, None, None, false).unwrap().len(),
            12
        );
        let conn = store.conn();
        let plan: String = conn.query_row("EXPLAIN QUERY PLAN SELECT seq FROM messages WHERE agent_id = ?1 AND seq > 8 ORDER BY seq DESC LIMIT 3", [own.id.as_str()], |r| r.get(3)).unwrap();
        assert!(
            plan.contains("SEARCH") && plan.contains("agent_id"),
            "{plan}"
        );
    }

    #[test]
    fn batch_metadata_matches_single_agent_queries() {
        let (_dir, store) = setup();
        let own = agent(&store, "before-rename");
        let own = store.rename_agent(&own.id, "after-rename").unwrap();
        for i in 0..3 {
            store
                .receive(&own.id, Incoming::new(format!("batch {i}")))
                .unwrap();
        }
        let metadata = store.agent_query_metadata().unwrap();
        assert_eq!(
            metadata.aliases.get(own.id.as_str()).unwrap(),
            &store.agent_aliases(&own.id).unwrap()
        );
        assert_eq!(
            metadata
                .oldest_undelivered
                .get(own.id.as_str())
                .map(|t| 123456789_i64.saturating_sub(*t).max(0)),
            store
                .lag(&own.id, 123456789)
                .unwrap()
                .oldest_undelivered_age_ms
        );
        assert_eq!(
            metadata.open_turns.contains_key(own.id.as_str()),
            store.has_open_turn(&own.id).unwrap()
        );
        // Batched lag must skip passive reactions just like the single-agent path.
        store
            .conn()
            .execute(
                "UPDATE messages SET context = ?1 WHERE agent_id = ?2 AND seq = 1",
                params![r#"{"event":"reaction_passive"}"#, own.id.as_str()],
            )
            .unwrap();
        let now = crate::now_ms();
        let metadata = store.agent_query_metadata().unwrap();
        assert_eq!(
            metadata
                .oldest_undelivered
                .get(own.id.as_str())
                .map(|t| now.saturating_sub(*t).max(0)),
            store.lag(&own.id, now).unwrap().oldest_undelivered_age_ms
        );
        for seq in 1..=3 {
            store.note_delivered(&own.id, seq).unwrap();
        }
        let metadata = store.agent_query_metadata().unwrap();
        assert!(!metadata.oldest_undelivered.contains_key(own.id.as_str()));
        assert!(metadata.open_turns.contains_key(own.id.as_str()));
        store.note_turn_completed(&own.id).unwrap();
        assert!(
            !store
                .agent_query_metadata()
                .unwrap()
                .open_turns
                .contains_key(own.id.as_str())
        );
        store.set_agent_state(&own.id, "archived").unwrap();
        assert!(
            !store
                .agent_query_metadata()
                .unwrap()
                .oldest_undelivered
                .contains_key(own.id.as_str())
        );
    }

    #[test]
    fn lexical_dedup_keeps_distinct_tails_but_collapses_identical_broadcasts() {
        let (_dir, store) = setup();
        let own = agent(&store, "dedup-own");
        let other = agent(&store, "dedup-other");
        let prefix = "shared-prefix ".repeat(300);
        let first = format!("{prefix} dedupneedle first-tail");
        let second = format!("{prefix} dedupneedle second-tail");
        store
            .record_outbound(&own.id, "agent:peer", None, &first)
            .unwrap();
        store
            .record_outbound(&other.id, "agent:peer", None, &second)
            .unwrap();
        store
            .record_outbound(&other.id, "agent:peer", None, &first)
            .unwrap();
        let hits = store
            .search_history(&HistorySearchQuery {
                query: Some("dedupneedle".into()),
                visible_agent_ids: Some(vec![own.id.to_string(), other.id.to_string()]),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|h| h.text == first));
        assert!(hits.iter().any(|h| h.text == second));
    }

    #[test]
    fn a_chat_room_reaches_only_the_agents_whose_bot_is_on_that_chat() {
        let (_dir, store) = setup();
        let alice = agent(&store, "alice");
        let bob = agent(&store, "bob");
        let carol = agent(&store, "carol");
        // Two bots on chat A, one bot on chat B.
        for (name, chat) in [(&alice, -1001_i64), (&bob, -1001), (&carol, -2002)] {
            let channel = store
                .create_channel(NewChannel {
                    kind: "telegram".to_string(),
                    identity: None,
                    config: "{}".to_string(),
                    credential_ref: None,
                })
                .unwrap();
            store.attach_channel(&channel.id, &name.id).unwrap();
            store
                .set_channel_state(
                    &channel.id,
                    &format!(
                        "{{\"chats\":[{{\"agent\":\"{}\",\"chat_id\":{chat}}}]}}",
                        name.name
                    ),
                )
                .unwrap();
        }

        let one = |text: &str, from: &str| Incoming {
            text: text.to_string(),
            sender: Some(from.to_string()),
            source_time_ms: None,
            source_key: None,
            reply_to: None,
            quote: None,
            origin: None,
            received_at_ms: None,
        };

        // A post in chat A reaches the other bot on chat A, and no one on B.
        let delivered = store
            .receive_room(&(-1001_i64).to_string(), one("in A", "agent:alice"))
            .unwrap();
        let names: Vec<&str> = delivered
            .iter()
            .map(|(agent, _)| agent.name.as_str())
            .collect();
        assert_eq!(names, vec!["bob"], "{names:?}");
        assert!(
            !store
                .inbox(&carol.id, None)
                .unwrap()
                .iter()
                .any(|m| m.text == "in A")
        );

        // A post in chat B reaches only its own bot.
        let delivered = store
            .receive_room("telegram:chat:-2002", one("in B", "agent:carol"))
            .unwrap();
        assert!(delivered.is_empty(), "{delivered:?}");

        // A room that is neither a chat nor a group reaches nobody. A name the
        // server does not know must never fan out to every agent.
        let delivered = store
            .receive_room("crew", one("everyone", "agent:alice"))
            .unwrap();
        assert!(delivered.is_empty(), "{delivered:?}");
    }

    #[test]
    fn a_launch_environment_survives_a_round_trip() {
        let env = vec![
            (
                "ANTHROPIC_BASE_URL".to_string(),
                "http://127.0.0.1:41727".to_string(),
            ),
            ("ANTHROPIC_AUTH_TOKEN".to_string(), "abc123".to_string()),
            ("ANTHROPIC_MODEL".to_string(), "claude-sonnet".to_string()),
        ];
        assert_eq!(decode_env(&encode_env(&env)), env);

        // And through the store, read back as an agent would see it.
        let (_dir, store) = setup();
        let alpha = agent(&store, "alpha");
        store.set_agent_launch_env(&alpha.id, &env).unwrap();
        let mut expected = env.clone();
        expected.extend([
            ("ANYAGENTS_SCOPE".to_string(), "public".to_string()),
            ("ANYAGENTS_PUBLIC".to_string(), "1".to_string()),
        ]);
        assert_eq!(store.agent(&alpha.id).unwrap().launch_env, expected);

        // A value with a quote, a backslash and a newline keeps its characters.
        let awkward = vec![("K".to_string(), "a\"b\\c\nd".to_string())];
        assert_eq!(decode_env(&encode_env(&awkward)), awkward);
    }

    #[test]
    fn explicit_scope_and_description_survive_launch_env_refresh_and_reopen() {
        let (dir, store) = setup();
        let root = agent(&store, "root");
        let mut new = NewAgent::new("worker", "claude-code");
        new.parent_id = Some(root.id);
        let worker = store.create_agent(new).unwrap();
        let db = dir.path().join("state.db");
        store
            .set_agent_description(&worker.id, Some("Public charter"))
            .unwrap();
        for scope in ["siblings", "isolated", "public", "private"] {
            store.set_agent_scope(&worker.id, scope).unwrap();
            assert_eq!(store.agent(&worker.id).unwrap().scope(), scope);
            // These are the gateway-only environments written at startup and
            // provider refresh; none owns communication policy.
            for port in [41001, 41002] {
                let refreshed = store
                    .set_agent_launch_env(
                        &worker.id,
                        &[("OPENAI_BASE_URL".into(), format!("http://127.0.0.1:{port}"))],
                    )
                    .unwrap();
                assert_eq!(refreshed.scope(), scope);
                assert_eq!(refreshed.description.as_deref(), Some("Public charter"));
                let reopened = Store::open(&db).unwrap().agent(&worker.id).unwrap();
                assert_eq!(reopened.scope(), scope);
                assert_eq!(reopened.description, refreshed.description);
                assert!(
                    reopened
                        .launch_env
                        .contains(&("OPENAI_BASE_URL".into(), format!("http://127.0.0.1:{port}"),))
                );
            }
        }
    }

    #[test]
    fn unrelated_agent_updates_preserve_explicit_scope() {
        let (_dir, store) = setup();
        let alpha = agent(&store, "alpha");
        store.set_agent_scope(&alpha.id, "siblings").unwrap();
        store
            .set_agent_description(&alpha.id, Some("Charter"))
            .unwrap();
        store
            .set_agent_instructions(&alpha.id, Some("Instructions"))
            .unwrap();
        store.set_agent_role(&alpha.id, Some("Mind")).unwrap();
        store.set_agent_model(&alpha.id, "model").unwrap();
        store.set_agent_harness(&alpha.id, "codex").unwrap();
        store.rename_agent(&alpha.id, "renamed").unwrap();
        assert_eq!(store.agent(&alpha.id).unwrap().scope(), "siblings");

        let stale = store.agent(&alpha.id).unwrap().launch_env;
        store.set_agent_scope(&alpha.id, "isolated").unwrap();
        // A stale transfer/harness snapshot cannot put the old policy back.
        store.set_agent_launch_env(&alpha.id, &stale).unwrap();
        assert_eq!(store.agent(&alpha.id).unwrap().scope(), "isolated");
    }

    #[test]
    fn legacy_public_revokes_only_public_reachability() {
        let (_dir, store) = setup();
        let parent = agent(&store, "parent");
        for (name, role, default) in [
            ("labelled", Some("Researcher"), "private"),
            ("task", None, "private"),
            ("liaison-assistant", Some("Liaison"), "private"),
        ] {
            let mut new = NewAgent::new(name, "codex");
            new.parent_id = Some(parent.id.clone());
            new.role = role.map(str::to_string);
            let child = store.create_agent(new).unwrap();
            assert_eq!(child.scope(), default);
            for scope in ["siblings", "isolated", "private"] {
                store.set_agent_scope(&child.id, scope).unwrap();
                assert_eq!(
                    store.set_agent_public(&child.id, false).unwrap().scope(),
                    scope
                );
            }
            assert_eq!(
                store.set_agent_public(&child.id, true).unwrap().scope(),
                "public"
            );
            assert_eq!(
                store.set_agent_public(&child.id, false).unwrap().scope(),
                default
            );
        }
    }

    #[test]
    fn scope_update_rolls_back_if_its_audit_cannot_commit() {
        let (_dir, store) = setup();
        let alpha = agent(&store, "alpha");
        store.set_agent_scope(&alpha.id, "siblings").unwrap();
        let before = store.agent(&alpha.id).unwrap();
        store
            .conn()
            .execute_batch(
                "CREATE TRIGGER refuse_scope_audit BEFORE INSERT ON admin_audit
             WHEN NEW.source IS NOT NULL BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END;",
            )
            .unwrap();
        let err = store
            .set_agent_scope_as(&alpha.id, "public", "agent:parent")
            .unwrap_err();
        assert!(err.to_string().contains("audit unavailable"), "{err}");
        assert_eq!(store.agent(&alpha.id).unwrap(), before);
    }

    #[test]
    fn opens_in_wal_and_migrates_once() {
        let (dir, store) = setup();
        let mode: String = store
            .conn()
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        // The newest migration's number, bumped whenever one is added.
        let newest = migrate::MIGRATIONS
            .last()
            .map(|(version, _)| *version)
            .unwrap();
        let version = migrate::current_version(&store.conn()).unwrap();
        assert_eq!(version, newest);
        let again = Store::open(dir.path().join("state.db")).unwrap();
        assert_eq!(migrate::current_version(&again.conn()).unwrap(), newest);
        assert_eq!(again.list_agents().unwrap(), store.list_agents().unwrap());
    }

    #[test]
    fn sequence_dedupes_without_consuming_a_number() {
        let (_dir, store) = setup();
        let agent = agent(&store, "alpha");
        let mut first = Incoming::new("one");
        first.source_key = Some("src:1".into());
        first.source_time_ms = Some(50);
        let ReceiveOutcome::New(first) = store.receive(&agent.id, first).unwrap() else {
            panic!("first receive should be new");
        };
        assert_eq!(first.seq, 1);
        assert_eq!(first.ack, Ack::Received);
        let mut again = Incoming::new("one again");
        again.source_key = Some("src:1".into());
        let ReceiveOutcome::Duplicate(again) = store.receive(&agent.id, again).unwrap() else {
            panic!("same source key should dedupe");
        };
        assert_eq!(again.id, first.id);
        assert_eq!(again.seq, 1);
        let mut blank_a = Incoming::new("a");
        blank_a.source_key = Some(String::new());
        let mut blank_b = Incoming::new("b");
        blank_b.source_key = None;
        let ReceiveOutcome::New(a) = store.receive(&agent.id, blank_a).unwrap() else {
            panic!("empty key is not a key");
        };
        let ReceiveOutcome::New(b) = store.receive(&agent.id, blank_b).unwrap() else {
            panic!("missing key is not a key");
        };
        assert_eq!((a.seq, b.seq), (2, 3));
        assert_eq!(store.received_cursor(&agent.id).unwrap(), 3);
    }

    #[test]
    fn late_source_time_gets_the_next_sequence() {
        let (_dir, store) = setup();
        let agent = agent(&store, "alpha");
        let mut early_arrival = Incoming::new("later clock");
        early_arrival.source_time_ms = Some(500);
        let mut late = Incoming::new("earlier clock");
        late.source_time_ms = Some(100);
        let ReceiveOutcome::New(first) = store.receive(&agent.id, early_arrival).unwrap() else {
            panic!("new");
        };
        let ReceiveOutcome::New(second) = store.receive(&agent.id, late).unwrap() else {
            panic!("new");
        };
        assert_eq!((first.seq, second.seq), (1, 2));
        assert!(second.source_time_ms.unwrap() < first.source_time_ms.unwrap());
    }

    #[test]
    fn one_delivery_cursor_refuses_a_gap_and_inbox_is_exclusive() {
        let (_dir, store) = setup();
        let agent = agent(&store, "alpha");
        store.receive(&agent.id, Incoming::new("a")).unwrap();
        let ReceiveOutcome::New(second) = store.receive(&agent.id, Incoming::new("b")).unwrap()
        else {
            panic!("new");
        };
        // A cursor always moves one step: it never skips a message, never moves
        // back, and never moves to a row that is not there.
        assert!(store.advance_delivered(&agent.id, 2).is_err());
        assert!(store.advance_delivered(&agent.id, 3).is_err());
        store.note_delivered(&agent.id, 1).unwrap();
        assert!(store.delivered_cursor(&agent.id).unwrap() == 1);
        let all = store.inbox(&agent.id, None).unwrap();
        assert!(all.len() == 2);
        assert!(all[0].ack == Ack::Delivered);
        assert!(all[1].ack == Ack::Received);
        assert!(all[1].id == second.id);
        // The next line to hand over is the one after the cursor.
        let next = store.next_undelivered(&agent.id).unwrap().expect("a line");
        assert!(next.seq == 2);
        assert!(next.ack == Ack::Received);
        assert!(store.advance_delivered(&agent.id, 2).unwrap() == 2);
        assert!(store.delivered_cursor(&agent.id).unwrap() == 2);
        assert!(store.advance_delivered(&agent.id, 2).is_err());
    }

    /// The live-desk shape before this migration: two cursors, a turn-state
    /// flag on every message, and the agent's own answers written back into its
    /// inbox. The upgrade must leave one cursor, no stale answer rows, and a
    /// cursor that still points at a row that exists.
    #[test]
    fn upgrading_a_two_cursor_store_keeps_one_cursor_and_drops_the_answer_rows() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("state.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
        )
        .unwrap();
        for (version, sql) in migrate::MIGRATIONS.iter().take(6) {
            conn.execute_batch(sql).unwrap();
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, 0)",
                [version],
            )
            .unwrap();
        }
        conn.execute_batch(
            "INSERT INTO agents (id, name, harness, state, created_at_ms)
                 VALUES ('agt_01h455vb4pex5vsknk084sn02q', 'migrated_worker', 'opencode', 'running', 1);
             INSERT INTO mailbox_cursors (agent_id, accepted_seq, processed_seq)
                 VALUES ('agt_01h455vb4pex5vsknk084sn02q', 4, 2);
             INSERT INTO messages (id, agent_id, seq, source_key, body, received_at_ms, delivered_at_ms, processed_at_delivery)
                 VALUES ('msg_01h455vb4pex5vsknk084sn02q', 'agt_01h455vb4pex5vsknk084sn02q', 1, NULL, 'one', 1, 5, 0);
             INSERT INTO messages (id, agent_id, seq, source_key, body, received_at_ms, delivered_at_ms, processed_at_delivery)
                 VALUES ('msg_01h455vb4pex5vsknk084sn02r', 'agt_01h455vb4pex5vsknk084sn02q', 2, NULL, 'two', 1, 6, 0);
             INSERT INTO messages (id, agent_id, seq, source_key, body, received_at_ms, delivered_at_ms, processed_at_delivery)
                 VALUES ('msg_01h455vb4pex5vsknk084sn02s', 'agt_01h455vb4pex5vsknk084sn02q', 3, 'answer:msg_01h455vb4pex5vsknk084sn02q', 'pong', 1, 7, 0);
             INSERT INTO messages (id, agent_id, seq, source_key, body, received_at_ms, delivered_at_ms, processed_at_delivery)
                 VALUES ('msg_01h455vb4pex5vsknk084sn02t', 'agt_01h455vb4pex5vsknk084sn02q', 4, NULL, 'four', 1, 8, 1);
             INSERT INTO failed_turns (message_id, agent_id, seq, reason, failed_at_ms)
                 VALUES ('msg_01h455vb4pex5vsknk084sn02t', 'agt_01h455vb4pex5vsknk084sn02q', 4, 'boom', 9);",
        )
        .unwrap();
        drop(conn);
        let store = Store::open(&path).unwrap();
        let agent = store.agent_by_name("migrated_worker").unwrap();
        // delivered = the higher of the two old cursors: 4 is kept even though
        // the answer row at 3 was removed, so the upgrade never re-sends what a
        // harness already took.
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 4);
        let inbox = store.inbox(&agent.id, None).unwrap();
        let seqs: Vec<i64> = inbox.iter().map(|message| message.seq).collect();
        assert_eq!(seqs, vec![1, 2, 4]);
        assert!(inbox.iter().all(|message| {
            !message
                .source_key
                .as_deref()
                .unwrap_or("")
                .starts_with("answer:")
        }));
        assert!(matches!(
            store.message(&agent.id, "msg_01h455vb4pex5vsknk084sn02s"),
            Ok(None)
        ));
        let columns: Vec<String> = store
            .conn()
            .prepare("SELECT name FROM pragma_table_info('mailbox_cursors')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(columns, vec!["agent_id", "delivered_seq"]);
        assert!(migrate::current_version(&store.conn()).unwrap() >= 7);
    }

    #[test]
    fn unknown_agent_is_rejected() {
        let (_dir, store) = setup();
        let missing = AgentId::generate();
        let err = store.receive(&missing, Incoming::new("nope")).unwrap_err();
        assert!(matches!(err, Error::AgentNotFound(_)));
    }

    #[test]
    fn channel_attaches_to_an_agent() {
        let (_dir, store) = setup();
        let agent = agent(&store, "alpha");
        let channel = store
            .create_channel(NewChannel {
                kind: "telegram".into(),
                identity: Some("bot".into()),
                config: "{}".into(),
                credential_ref: Some("channels/one".into()),
            })
            .unwrap();
        store.attach_channel(&channel.id, &agent.id).unwrap();
        let listed = store.list_channels().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].agents, vec![agent.id.clone()]);
        assert_eq!(listed[0].credential_ref.as_deref(), Some("channels/one"));
        assert!(!format!("{listed:?}").contains("token"));
        let missing = ChannelId::generate();
        assert!(matches!(
            store.attach_channel(&missing, &agent.id).unwrap_err(),
            Error::ChannelNotFound(_)
        ));
    }

    #[test]
    fn usage_round_trips() {
        let (_dir, store) = setup();
        let agent = agent(&store, "alpha");
        let record = store
            .record_usage(NewUsage {
                agent_id: agent.id.clone(),
                harness: "claude-code".into(),
                model: Some("model".into()),
                input_tokens: 3,
                output_tokens: 4,
                cache_read_tokens: 1,
                cache_write_tokens: 0,
                reported_cost_micros: None,
                estimated_cost_micros: Some(12),
                billing_mode: BillingMode::Subscription,
                quota_state: None,
                day: "2026-10-18".into(),
            })
            .unwrap();
        assert_eq!(record.input_tokens, 3);
        assert_eq!(store.list_usage(Some(&agent.id)).unwrap(), vec![record]);
    }

    fn usage_record(agent_id: &AgentId, at_ms: i64, day: &str, model: &str) -> UsageRecord {
        UsageRecord {
            id: UsageId::generate(),
            agent_id: agent_id.clone(),
            harness: "harness-a".into(),
            model: Some(model.into()),
            input_tokens: 10,
            output_tokens: 20,
            cache_read_tokens: 3,
            cache_write_tokens: 4,
            reported_cost_micros: Some(5),
            estimated_cost_micros: None,
            billing_mode: BillingMode::Subscription,
            quota_state: None,
            day: day.into(),
            recorded_at_ms: at_ms,
        }
    }

    #[test]
    fn aggregate_usage_groups_and_windows() {
        let (_dir, store) = setup();
        let alpha = agent(&store, "alpha");
        let beta = agent(&store, "beta");
        let now = 1_000_000_000_000i64;
        let day_ms = 86_400_000i64;
        let records = vec![
            usage_record(&alpha.id, now - day_ms, "2026-10-17", "m1"),
            usage_record(&alpha.id, now, "2026-10-18", "m1"),
            usage_record(&alpha.id, now, "2026-10-18", "m2"),
            usage_record(&beta.id, now, "2026-10-18", "m1"),
        ];
        let names = std::collections::HashMap::from([
            (alpha.id.clone(), alpha.name.clone()),
            (beta.id.clone(), beta.name.clone()),
        ]);
        let ids = vec![alpha.id.clone(), beta.id.clone()];
        // Inclusive window: only the two records at `now`.
        let report = aggregate_usage(
            &records,
            &ids,
            &names,
            UsageFilter {
                since_ms: Some(now),
                until_ms: Some(now),
                ..UsageFilter::default()
            },
            now,
        )
        .unwrap();
        assert_eq!(report.totals.turns, 3);
        assert_eq!(report.totals.input_tokens, 30);
        assert_eq!(report.totals.reported_cost_micros, Some(15));
        // group_by model -> alpha splits into m1/m2, beta is m1.
        let by_model = aggregate_usage(
            &records,
            &ids,
            &names,
            UsageFilter {
                group_by: UsageGroupBy::Model,
                ..UsageFilter::default()
            },
            now,
        )
        .unwrap();
        assert_eq!(by_model.buckets.len(), 3);
        assert_eq!(by_model.buckets[0].model.as_deref(), Some("m1"));
        // group_by day -> alpha has two days.
        let by_day = aggregate_usage(
            &records,
            &ids,
            &names,
            UsageFilter {
                group_by: UsageGroupBy::Day,
                ..UsageFilter::default()
            },
            now,
        )
        .unwrap();
        assert_eq!(by_day.buckets.len(), 3);
        // days=1 keeps everything recorded within the last day (all four).
        let by_days = aggregate_usage(
            &records,
            &ids,
            &names,
            UsageFilter {
                days: Some(1),
                ..UsageFilter::default()
            },
            now,
        )
        .unwrap();
        assert_eq!(by_days.totals.turns, 4);
        // group_by harness collapses to one bucket per agent.
        let by_harness = aggregate_usage(
            &records,
            &ids,
            &names,
            UsageFilter {
                group_by: UsageGroupBy::Harness,
                ..UsageFilter::default()
            },
            now,
        )
        .unwrap();
        assert_eq!(by_harness.buckets.len(), 2);
        assert!(
            by_harness
                .buckets
                .iter()
                .all(|b| b.harness.as_deref() == Some("harness-a"))
        );
        // Bad inputs fail rather than guessing.
        assert!(
            UsageFilter {
                days: Some(0),
                ..UsageFilter::default()
            }
            .window(now)
            .is_err()
        );
        assert!(UsageGroupBy::parse("week").is_err());
    }

    #[test]
    fn resolve_usage_agents_enforces_causal_boundary() {
        let (_dir, store) = setup();
        let root = agent(&store, "root");
        let child_new = {
            let mut n = NewAgent::new("child", "claude-code");
            n.parent_id = Some(root.id.clone());
            n
        };
        let child = store.create_agent(child_new).unwrap();
        let grand_new = {
            let mut n = NewAgent::new("grandchild", "claude-code");
            n.parent_id = Some(child.id.clone());
            n
        };
        let grandchild = store.create_agent(grand_new).unwrap();
        let sib_new = {
            let mut n = NewAgent::new("sibling", "claude-code");
            n.parent_id = Some(root.id.clone());
            n
        };
        let sibling = store.create_agent(sib_new).unwrap();
        let agents = store.list_agents().unwrap();
        let caller = agents.iter().find(|a| a.id == child.id).unwrap();

        // Omitted target: caller's own subtree only.
        let ids = resolve_usage_agents(Some(caller), &agents, None, None).unwrap();
        assert!(ids.contains(&child.id));
        assert!(ids.contains(&grandchild.id));
        assert!(!ids.contains(&root.id));
        assert!(!ids.contains(&sibling.id));
        // Self, child and grandchild are visible; parent and sibling are not.
        for name in ["self", "child", "grandchild"] {
            assert!(
                resolve_usage_agents(Some(caller), &agents, Some(name), None).is_ok(),
                "{name} should be visible"
            );
        }
        for name in ["root", "sibling", "all", "*"] {
            assert!(
                matches!(
                    resolve_usage_agents(Some(caller), &agents, Some(name), None),
                    Err(UsageAccessError::Forbidden(_))
                ),
                "{name} should be forbidden"
            );
        }
        assert!(matches!(
            resolve_usage_agents(Some(caller), &agents, Some("ghost"), None),
            Err(UsageAccessError::Invalid(_))
        ));
        // Explicit target without the subtree switch is exact-agent only.
        let exact = resolve_usage_agents(Some(caller), &agents, Some("child"), None).unwrap();
        assert_eq!(exact, vec![child.id.clone()]);
        let subtree =
            resolve_usage_agents(Some(caller), &agents, Some("child"), Some(true)).unwrap();
        assert_eq!(subtree.len(), 2);
        // A public-scope caller and a root caller are privileged.
        store.set_agent_scope(&child.id, "public").unwrap();
        let agents = store.list_agents().unwrap();
        let public_caller = agents.iter().find(|a| a.id == child.id).unwrap();
        let all = resolve_usage_agents(Some(public_caller), &agents, Some("all"), None).unwrap();
        assert_eq!(all.len(), 4);
        assert!(resolve_usage_agents(Some(public_caller), &agents, Some("root"), None).is_ok());
        let root_caller = agents.iter().find(|a| a.id == root.id).unwrap();
        assert_eq!(
            resolve_usage_agents(Some(root_caller), &agents, None, None)
                .unwrap()
                .len(),
            4
        );
        // The local operator (`None`) is privileged too.
        assert_eq!(
            resolve_usage_agents(None, &agents, Some("sibling"), Some(true)).unwrap(),
            vec![sibling.id.clone()]
        );
    }

    #[test]
    fn second_lock_is_busy_until_the_first_drops() {
        let (dir, store) = setup();
        let held_agent = agent(&store, "alpha");
        let other = agent(&store, "beta");
        let held = store.try_lock_agent(&held_agent.id).unwrap();
        let again = Store::open(dir.path().join("state.db")).unwrap();
        assert!(matches!(
            again.try_lock_agent(&held_agent.id).unwrap_err(),
            Error::AgentBusy
        ));
        let _other_lock = store.try_lock_agent(&other.id).unwrap();
        drop(held);
        again.try_lock_agent(&held_agent.id).unwrap();
    }

    #[test]
    fn dropping_agent_lock_releases_it_with_an_inherited_descriptor_open() {
        let (_dir, store) = setup();
        let owner = agent(&store, "owner");
        let lock = store.try_lock_agent(&owner.id).unwrap();
        // A concurrent fork can inherit this open file description until exec.
        // A clone deterministically models that lifetime without timing a fork.
        let inherited = lock._file.try_clone().unwrap();
        drop(lock);
        let _next_turn = store.try_lock_agent(&owner.id).unwrap();
        drop(inherited);
    }

    #[test]
    fn dropping_lifecycle_lock_releases_it_with_an_inherited_descriptor_open() {
        let (_dir, store) = setup();
        let owner = agent(&store, "owner");
        for nonblocking in [false, true] {
            let lock = if nonblocking {
                store.try_lock_agent_lifecycle(&owner.id).unwrap()
            } else {
                store.lock_agent_lifecycle(&owner.id).unwrap()
            };
            // A duplicate shares the open file description inherited by fork,
            // even when the descriptor is close-on-exec. No fork timing needed.
            let inherited = lock._file.try_clone().unwrap();
            assert!(matches!(
                store.try_lock_agent_lifecycle(&owner.id),
                Err(Error::AgentBusy)
            ));
            drop(lock);
            let _next_lifecycle = store.try_lock_agent_lifecycle(&owner.id).unwrap();
            drop(inherited);
        }
    }

    #[test]
    fn lag_uses_the_message_receive_time() {
        let (_dir, store) = setup();
        let agent = agent(&store, "alpha");
        let mut incoming = Incoming::new("held");
        incoming.received_at_ms = Some(1_000);
        store.receive(&agent.id, incoming).unwrap();
        let lag = store.lag(&agent.id, 6_000).unwrap();
        assert_eq!(lag.oldest_undelivered_age_ms, Some(5_000));
        store.advance_delivered(&agent.id, 1).unwrap();
        assert_eq!(
            store
                .lag(&agent.id, 6_000)
                .unwrap()
                .oldest_undelivered_age_ms,
            None
        );
    }

    #[test]
    fn rejects_a_bad_agent_name_and_a_duplicate() {
        let (_dir, store) = setup();
        assert!(
            store
                .create_agent(NewAgent::new("has space", "claude-code"))
                .is_err()
        );
        agent(&store, "alpha");
        assert!(matches!(
            store
                .create_agent(NewAgent::new("alpha", "claude-code"))
                .unwrap_err(),
            Error::AgentExists(_)
        ));
    }

    #[test]
    fn a_new_message_wakes_a_subscriber_and_a_duplicate_does_not() {
        let (_dir, store) = setup();
        let agent = agent(&store, "alpha");
        let wakes = store.subscribe();
        let mut incoming = Incoming::new("hello");
        incoming.source_key = Some("same".into());
        store.receive(&agent.id, incoming.clone()).unwrap();
        assert!(matches!(
            wakes
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
            Wake::Mail { .. }
        ));
        store.receive(&agent.id, incoming).unwrap();
        assert!(
            wakes
                .recv_timeout(std::time::Duration::from_millis(200))
                .is_err()
        );
    }

    #[test]
    fn set_agent_running_stores_the_workdir_and_wakes() {
        let (_dir, store) = setup();
        let agent = agent(&store, "alpha");
        let wakes = store.subscribe();
        let running = store.set_agent_running(&agent.id, "/work").unwrap();
        assert_eq!(running.state, "running");
        assert_eq!(running.workdir.as_deref(), Some("/work"));
        assert!(matches!(
            wakes
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
            Wake::Agent { .. }
        ));
        let missing = AgentId::generate();
        assert!(matches!(
            store.set_agent_running(&missing, "/work").unwrap_err(),
            Error::AgentNotFound(_)
        ));
    }

    #[test]
    fn utc_day_and_usage_sums() {
        assert_eq!(utc_day(0), "1970-01-01");
        assert_eq!(utc_day(1_700_000_000_000), "2023-11-14");
        let alpha = AgentId::generate();
        let beta = AgentId::generate();
        let records = [
            sample_usage(alpha.clone(), "2023-11-14", 1, Some(5)),
            sample_usage(alpha.clone(), "2023-11-15", 4, None),
            sample_usage(beta.clone(), "2023-11-14", 2, Some(7)),
        ];
        let by_agent = sum_usage(&records, false);
        assert_eq!(by_agent.len(), 2);
        let alpha_total = by_agent.iter().find(|row| row.agent_id == alpha).unwrap();
        assert_eq!(alpha_total.day, None);
        assert_eq!(alpha_total.input_tokens, 5);
        assert_eq!(alpha_total.reported_cost_micros, Some(5));
        let by_day = sum_usage(&records, true);
        assert_eq!(by_day.len(), 3);
        assert!(
            by_day
                .iter()
                .any(|row| row.day.as_deref() == Some("2023-11-15")
                    && row.reported_cost_micros.is_none())
        );
    }

    fn sample_usage(agent_id: AgentId, day: &str, input: i64, cost: Option<i64>) -> UsageRecord {
        UsageRecord {
            id: crate::UsageId::generate(),
            agent_id,
            harness: "claude-code".into(),
            model: None,
            input_tokens: input,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            reported_cost_micros: cost,
            estimated_cost_micros: None,
            billing_mode: BillingMode::Subscription,
            quota_state: None,
            day: day.into(),
            recorded_at_ms: 0,
        }
    }

    /// A named group has its own membership: a line reaches its members, a
    /// non-member receives nothing, and an unknown name reaches nobody.
    #[test]
    fn a_named_group_reaches_only_its_own_members() {
        let (_dir, store) = setup();
        let alice = agent(&store, "alice");
        let bob = agent(&store, "bob");
        let carol = agent(&store, "carol");
        store.join_group("crew", &alice.id).unwrap();
        store.join_group("crew", &bob.id).unwrap();

        let one = |text: &str, from: &str| Incoming {
            text: text.to_string(),
            sender: Some(from.to_string()),
            source_time_ms: None,
            source_key: None,
            reply_to: None,
            quote: None,
            origin: None,
            received_at_ms: None,
        };

        // A human line reaches every member.
        let delivered = store
            .receive_room("group:crew", one("hi all", "telegram:user:1"))
            .unwrap();
        let names: Vec<&str> = delivered
            .iter()
            .map(|(agent, _)| agent.name.as_str())
            .collect();
        assert_eq!(names, vec!["alice", "bob"], "{names:?}");
        assert!(
            !store
                .inbox(&carol.id, None)
                .unwrap()
                .iter()
                .any(|m| m.text == "hi all")
        );

        // An agent line reaches the other member, never the author.
        let delivered = store
            .receive_room("crew", one("from alice", "agent:alice"))
            .unwrap();
        let names: Vec<&str> = delivered
            .iter()
            .map(|(agent, _)| agent.name.as_str())
            .collect();
        assert_eq!(names, vec!["bob"], "{names:?}");

        // An unknown group reaches nobody rather than everybody.
        let delivered = store
            .receive_room("group:nobody", one("lost", "agent:alice"))
            .unwrap();
        assert!(delivered.is_empty(), "{delivered:?}");
    }

    /// A named group survives a restart: membership is stored, not held in
    /// memory, and a message already stored is still readable afterwards.
    #[test]
    fn a_group_message_survives_a_restart() {
        let (dir, store) = setup();
        let alice = agent(&store, "alice");
        let bob = agent(&store, "bob");
        store.join_group("crew", &alice.id).unwrap();
        store.join_group("crew", &bob.id).unwrap();
        store
            .receive_room(
                "group:crew",
                Incoming {
                    text: "kept".to_string(),
                    sender: Some("agent:alice".to_string()),
                    source_time_ms: None,
                    source_key: None,
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: None,
                },
            )
            .unwrap();
        drop(store);

        let reopened = Store::open(dir.path().join("state.db")).unwrap();
        assert_eq!(
            reopened.group_members("crew").unwrap(),
            vec!["alice", "bob"]
        );
        let bob_id = reopened.agent_by_name("bob").unwrap().id;
        assert!(
            reopened
                .inbox(&bob_id, None)
                .unwrap()
                .iter()
                .any(|message| message.text == "kept")
        );
    }

    #[test]
    fn origin_dedup_mentions_pinned_and_auto_wake() {
        let (_dir, store) = setup();
        let alice = agent(&store, "alice");
        let bob = agent(&store, "bob");
        let ch = store
            .create_channel(NewChannel {
                kind: "telegram".into(),
                identity: Some("999:bob_helper_bot".into()),
                config: "{}".into(),
                credential_ref: Some("secret-ref".into()),
            })
            .unwrap();
        store.attach_channel(&ch.id, &bob.id).unwrap();
        store
            .set_channel_state(
                &ch.id,
                r#"{"bot":"999:bob_helper_bot","offset":null,"chats":[]}"#,
            )
            .unwrap();

        // Put alice in idle (scaled-to-zero), then send a message with @bob_helper_bot, @alice, and pinned event.
        store.set_agent_state(&alice.id, "idle").unwrap();
        assert_eq!(store.agent_by_name("alice").unwrap().state, "idle");

        let origin = Origin {
            chat_id: Some(-100123),
            thread_id: Some(7),
            message_id: Some(555),
        };
        let outcome = store
            .receive(
                &alice.id,
                Incoming {
                    text: "[pinned message] Hey @bob_helper_bot and @alice please check"
                        .to_string(),
                    sender: Some("telegram:user:42:555".to_string()),
                    source_time_ms: None,
                    source_key: Some("100:1".to_string()),
                    reply_to: Some("telegram:chat:-100123:7".to_string()),
                    quote: None,
                    origin: Some(origin),
                    received_at_ms: None,
                },
            )
            .unwrap();
        let ReceiveOutcome::New(msg) = outcome else {
            panic!("expected New");
        };
        // Alice auto-woke to running!
        assert_eq!(store.agent_by_name("alice").unwrap().state, "running");
        // Context includes event: pinned and resolved mentions!
        assert_eq!(msg.context.event.as_deref(), Some("pinned"));
        assert!(
            msg.context
                .mentions
                .contains(&"@bob_helper_bot (agent:bob)".to_string()),
            "{:?}",
            msg.context.mentions
        );
        assert!(
            msg.context
                .mentions
                .contains(&"@alice (agent:alice)".to_string()),
            "{:?}",
            msg.context.mentions
        );

        // A second delivery of the same (chat_id, message_id) from a peer bot relay with a different source_key is deduplicated!
        let dup = store
            .receive(
                &alice.id,
                Incoming {
                    text: "[pinned message] Hey @bob_helper_bot and @alice please check"
                        .to_string(),
                    sender: Some("telegram:user:42:555".to_string()),
                    source_time_ms: None,
                    source_key: Some("peer:100:1:alice".to_string()),
                    reply_to: Some("telegram:chat:-100123:7".to_string()),
                    quote: None,
                    origin: Some(origin),
                    received_at_ms: None,
                },
            )
            .unwrap();
        assert!(matches!(dup, ReceiveOutcome::Duplicate(_)));
        assert_eq!(store.inbox(&alice.id, None).unwrap().len(), 1);
    }

    #[test]
    fn scoped_history_limits_every_candidate_path_before_limits() {
        let (_dir, store) = setup();
        let own = agent(&store, "own");
        let hidden = agent(&store, "hidden");
        let receive = |id: &AgentId, text: String, timestamp| {
            store
                .receive(
                    id,
                    Incoming {
                        text,
                        sender: Some("agent:peer".into()),
                        source_time_ms: None,
                        source_key: None,
                        reply_to: None,
                        quote: None,
                        origin: None,
                        received_at_ms: Some(timestamp),
                    },
                )
                .unwrap();
        };
        receive(
            &own.id,
            "scopeword fragmentpartial authorized".into(),
            1_000,
        );
        // More hidden rows than either the FTS, substring, structured or recent
        // candidate cap: filtering only the final result would lose the hit.
        for n in 0..2_010 {
            receive(
                &hidden.id,
                format!("scopeword fragmentpartial hidden {n}"),
                2_000 + n,
            );
        }
        let mut visible = vec![own.id.to_string()];
        for n in 0..17 {
            visible.push(agent(&store, &format!("also-visible-{n}")).id.to_string());
        }
        for query in [Some("scopeword"), Some("gmentpart"), None] {
            let hits = store
                .search_history(&HistorySearchQuery {
                    agent: Some("all".into()),
                    query: query.map(str::to_string),
                    visible_agent_ids: Some(visible.clone()),
                    limit: 1,
                    ..HistorySearchQuery::default()
                })
                .unwrap();
            assert_eq!(hits.len(), 1, "{query:?}");
            assert!(hits[0].text.ends_with("authorized"), "{hits:?}");
        }
        let recent = store
            .conversation_all_scoped(None, None, Some(1), Some(&visible))
            .unwrap();
        assert_eq!(recent.len(), 1);
        assert!(recent[0].text.ends_with("authorized"));
        assert!(
            store
                .conversation_all_scoped(None, None, Some(1), Some(&[]))
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .search_history(&HistorySearchQuery {
                    query: Some("scopeword".into()),
                    visible_agent_ids: Some(vec![]),
                    ..HistorySearchQuery::default()
                })
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .conversation_scoped(&hidden.id, None, None, None, Some(&visible))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn history_radius_preserves_visibility_and_all_structured_filters() {
        let (_dir, store) = setup();
        let own = agent(&store, "own");
        let hidden = agent(&store, "hidden");
        for (id, sender, text, timestamp) in [
            (&own.id, "agent:peer", "outside time", 1_000),
            (&own.id, "agent:peer", "outside seq", 2_000),
            (&own.id, "agent:peer", "radiusneedle authorized", 3_000),
            (&hidden.id, "agent:peer", "hidden neighbor", 3_001),
            (&own.id, "agent:other", "outside peer", 4_000),
            (&own.id, "agent:peer", "excluded neighbor", 5_000),
        ] {
            store
                .receive(
                    id,
                    Incoming {
                        text: text.into(),
                        sender: Some(sender.into()),
                        source_time_ms: None,
                        source_key: None,
                        reply_to: None,
                        quote: None,
                        origin: None,
                        received_at_ms: Some(timestamp),
                    },
                )
                .unwrap();
        }
        store
            .record_outbound(&own.id, "agent:peer", None, "outside direction")
            .unwrap();
        store
            .record_tool_event(&own.id, "browser", "radiusneedle own browser")
            .unwrap();
        store
            .record_tool_event(&hidden.id, "browser", "radiusneedle hidden browser")
            .unwrap();
        // Historical names are not a permission boundary (e.g. after rename).
        store
            .conn()
            .execute(
                "UPDATE rag_chunks SET agent_name = 'own' WHERE agent_id = ?1",
                [hidden.id.as_str()],
            )
            .unwrap();
        let visible = Some(vec![own.id.to_string()]);
        let hits = store
            .search_history(&HistorySearchQuery {
                query: Some("radiusneedle".into()),
                agent: Some("all".into()),
                visible_agent_ids: visible.clone(),
                with_peer: Some("peer".into()),
                sender: Some("peer".into()),
                direction: Some("in".into()),
                include_tools: false,
                since_ms: Some(1_500),
                until_ms: Some(5_000),
                from_seq: Some(3),
                to_seq: Some(5),
                exclude: vec!["excluded".into()],
                radius: 10,
                ..HistorySearchQuery::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].text, "radiusneedle authorized");
        let tools = store
            .search_history(&HistorySearchQuery {
                only_tools: true,
                tool_name: Some("browser".into()),
                radius: 10,
                visible_agent_ids: visible,
                ..HistorySearchQuery::default()
            })
            .unwrap();
        assert_eq!(tools.len(), 1, "{tools:?}");
        assert!(tools[0].text.ends_with("own browser"));
    }

    #[test]
    fn read_endpoints_and_current_history_do_not_wait_for_writer() {
        let (dir, store) = setup();
        let own = agent(&store, "reader");
        store.hold_capability_key(&"a".repeat(64)).unwrap();
        let token = store.issue_capability(&own.id).unwrap();
        let hidden = agent(&store, "legacy-hidden");
        store
            .record_tool_event(&hidden.id, "browser", "old tool")
            .unwrap();
        store
            .conn()
            .execute(
                "UPDATE rag_chunks SET text_format=1 WHERE agent_id=?1",
                [hidden.id.as_str()],
            )
            .unwrap();
        std::thread::scope(|scope| {
            let mut writer = store.conn();
            let tx = writer
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            tx.execute(
                "UPDATE agents SET description='uncommitted' WHERE id=?1",
                [own.id.as_str()],
            )
            .unwrap();
            let (send, receive) = std::sync::mpsc::channel();
            let store = &store;
            let id = &own.id;
            let db = dir.path().join("state.db");
            scope.spawn(move || {
                let result = (|| -> Result<(), Error> {
                    assert_eq!(store.agent(id)?.description, None);
                    store.agent_by_name("reader")?;
                    assert_eq!(store.authenticate_capability(&token)?, *id);
                    store.list_agents()?;
                    store.children_of(id)?;
                    store.list_channels()?;
                    store.share_any_group_or_chat(id, id);
                    store.has_messaged(&store.agent(id)?, &store.agent(id)?);
                    store.agent_query_metadata()?;
                    store.agent_aliases(id)?;
                    store.list_schedules(Some(id), true)?;
                    store.inbox_query_filtered(id, None, Some(5), false, None)?;
                    store.conversation_scoped(id, None, None, Some(5), None)?;
                    store.conversation_all_scoped(None, None, Some(5), None)?;
                    store.lag(id, now_ms())?;
                    store.has_open_turn(id)?;
                    store.open_turn(id)?;
                    // No legacy chunks at all, then only invisible legacy chunks:
                    // neither case may acquire the writer mutex or transaction.
                    store.backfill_full_mailbox_history(Some(&[]))?;
                    store.search_history(&HistorySearchQuery {
                        visible_agent_ids: Some(vec![id.to_string()]),
                        ..Default::default()
                    })?;
                    Store::open(db)?.list_agents()?;
                    Ok(())
                })();
                send.send(result).unwrap();
            });
            let result = receive.recv_timeout(Duration::from_secs(2));
            // Release before asserting so a regression cannot hang the test join.
            drop(tx);
            drop(writer);
            result
                .expect("read endpoint blocked on the writer")
                .unwrap();
        });
    }

    #[test]
    fn full_mailbox_history_upgrade_and_redaction() {
        let (_dir, store) = setup();
        let own = agent(&store, "batch-owner");
        // Cross multiple 64-row batches, retaining standalone tools as well.
        for n in 0..130 {
            store
                .record_outbound(&own.id, "agent:peer", None, &format!("full body {n}"))
                .unwrap();
        }
        store
            .record_tool_event(&own.id, "browser", "legacy tool summary")
            .unwrap();
        {
            let conn = store.conn();
            conn.execute(
                "UPDATE rag_chunks SET text='clipped',text_format=1 WHERE kind='message'",
                [],
            )
            .unwrap();
            conn.execute("UPDATE rag_chunks SET text_format=1 WHERE kind='tool'", [])
                .unwrap();
            // A redacted/short authoritative body removes its stale derivative.
            conn.execute("UPDATE outbound_messages SET body='x' WHERE id=(SELECT min(id) FROM outbound_messages)", []).unwrap();
        }
        store.backfill_full_mailbox_history(None).unwrap();
        let conn = store.read_conn();
        let (old, messages, tools): (i64, i64, i64) = conn.query_row(
            "SELECT count(*) FILTER (WHERE text_format<2), count(*) FILTER (WHERE kind='message'), count(*) FILTER (WHERE kind='tool') FROM rag_chunks",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).unwrap();
        assert_eq!((old, messages, tools), (0, 129, 1));
        let stale: i64 = conn.query_row("SELECT count(*) FROM rag_chunks_fts WHERE text='clipped' OR chunk_id IN (SELECT id FROM outbound_messages WHERE body='x')", [], |r| r.get(0)).unwrap();
        assert_eq!(stale, 0);
        let mut stmt = conn.prepare("EXPLAIN QUERY PLAN SELECT 1 FROM rag_chunks c INDEXED BY idx_rag_chunks_format WHERE c.text_format<2 LIMIT 1").unwrap();
        let plan = stmt
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(
            plan.contains("COVERING INDEX idx_rag_chunks_format"),
            "{plan}"
        );
    }

    #[test]
    fn full_mailbox_tails_upgrade_only_authorized_chunks_and_survive_redaction_restart() {
        let (dir, store) = setup();
        let own = agent(&store, "tail-owner");
        let hidden = agent(&store, "tail-hidden");
        let prefix = "padding ".repeat(500);
        let incoming = |text: String| Incoming {
            text,
            sender: Some("agent:peer".into()),
            source_time_ms: None,
            source_key: None,
            reply_to: None,
            quote: None,
            origin: None,
            received_at_ms: None,
        };
        store
            .receive(&own.id, incoming(format!("{prefix} firsttailneedle")))
            .unwrap();
        store
            .receive(&own.id, incoming(format!("{prefix} secondtailneedle")))
            .unwrap();
        store
            .receive(&hidden.id, incoming(format!("{prefix} hiddentailneedle")))
            .unwrap();
        store
            .record_outbound(
                &own.id,
                "agent:peer",
                None,
                &format!("{prefix} outboundtailneedle"),
            )
            .unwrap();
        store
            .record_tool_event(&own.id, "browser", &format!("{prefix} tooltailneedle"))
            .unwrap();
        let ids = [own.id.to_string()];
        let query = |term: &str| HistorySearchQuery {
            query: Some(term.into()),
            visible_agent_ids: Some(ids.to_vec()),
            ..Default::default()
        };
        for term in [
            "firsttailneedle",
            "secondtailneedle",
            "outboundtailneedle",
            "tooltailneedle",
        ] {
            assert_eq!(
                store.search_history(&query(term)).unwrap().len(),
                1,
                "{term}"
            );
        }
        let hashes: i64 = store.conn().query_row("SELECT count(DISTINCT content_hash) FROM rag_chunks WHERE agent_id=?1 AND direction='in'", [own.id.as_str()], |r| r.get(0)).unwrap();
        assert_eq!(
            hashes, 2,
            "identical prefixes must not merge distinct tails"
        );
        // Simulate the already-present pre-upgrade population, including FTS.
        // The real historical bodies remain untouched in the mailbox tables.
        {
            let conn = store.conn();
            conn.execute("UPDATE rag_chunks SET text=substr(text,1,2400),text_format=1 WHERE chunk_id NOT LIKE 'tool_%'", []).unwrap();
            conn.execute("DELETE FROM rag_chunks_fts", []).unwrap();
            conn.execute("INSERT INTO rag_chunks_fts(chunk_id,agent_name,sender,destination,kind,tool_name,text,search_tokens) SELECT chunk_id,agent_name,sender,destination,kind,tool_name,text,text FROM rag_chunks", []).unwrap();
        }
        assert_eq!(
            store
                .search_history(&query("firsttailneedle"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .search_history(&query("outboundtailneedle"))
                .unwrap()
                .len(),
            1
        );
        let hidden_format: i64 = store
            .conn()
            .query_row(
                "SELECT text_format FROM rag_chunks WHERE agent_id=?1",
                [hidden.id.as_str()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            hidden_format, 1,
            "hidden bodies must not be read/backfilled"
        );
        let message = store
            .inbox(&own.id, None)
            .unwrap()
            .into_iter()
            .find(|m| m.text.ends_with("firsttailneedle"))
            .unwrap();
        store.set_agent_state(&own.id, "archived").unwrap();
        store
            .redact_conversation(
                crate::redaction::Actor::Operator,
                &own.id,
                &crate::redaction::Request {
                    message_id: Some(message.id.to_string()),
                    seq: None,
                    span: Some(crate::redaction::Span {
                        unit: crate::redaction::SpanUnit::Bytes,
                        start: prefix.len() + 1,
                        end: message.text.len(),
                    }),
                    reason: "remove tail".into(),
                },
            )
            .unwrap();
        // Retrying the upgrade must consume the redacted authoritative body,
        // not resurrect text from an earlier derivative snapshot.
        store
            .conn()
            .execute(
                "UPDATE rag_chunks SET text_format=1 WHERE agent_id=?1",
                [own.id.as_str()],
            )
            .unwrap();
        drop(store);
        let store = Store::open(dir.path().join("state.db")).unwrap();
        assert!(
            store
                .search_history(&query("firsttailneedle"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .search_history(&query("secondtailneedle"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .search_history(&HistorySearchQuery {
                    query: Some("hiddentailneedle".into()),
                    visible_agent_ids: Some(vec![hidden.id.to_string()]),
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn search_history_supports_lexical_cjk_multi_query_exclude_tools_range_ordering_and_radius() {
        let (_dir, store) = setup();
        let alice = agent(&store, "alice");
        store
            .receive(
                &alice.id,
                Incoming {
                    text: "Step 1: durable turn_recovery checkpoint design and 記憶檢索"
                        .to_string(),
                    sender: Some("agent:bob".to_string()),
                    source_time_ms: None,
                    source_key: None,
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: Some(1_000),
                },
            )
            .unwrap();
        store
            .record_tool_event(
                &alice.id,
                "browser",
                "opened https://openrouter.ai/rankings and took snapshot",
            )
            .unwrap();
        store
            .receive(
                &alice.id,
                Incoming {
                    text: "Step 2: noisy heartbeat ping to ignore".to_string(),
                    sender: Some("agent:bob".to_string()),
                    source_time_ms: None,
                    source_key: None,
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: Some(2_000),
                },
            )
            .unwrap();
        store
            .receive(
                &alice.id,
                Incoming {
                    text: "Step 3: 生產環境部署完成 verified green".to_string(),
                    sender: Some("agent:carol".to_string()),
                    source_time_ms: None,
                    source_key: None,
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: Some(3_000),
                },
            )
            .unwrap();

        // 1. Sub-token (`recovery` from `turn_recovery`) + CJK 2-char bigram (`記憶`).
        let hits = store
            .search_conversation(Some("alice"), None, None, Some(10), "recovery")
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].text.contains("turn_recovery"));

        let cjk_hits = store
            .search_conversation(Some("alice"), None, None, Some(10), "記憶")
            .unwrap();
        assert_eq!(cjk_hits.len(), 1);
        assert!(cjk_hits[0].text.contains("記憶檢索"));

        // 2. Multi-query (`queries`) + negative filter (`exclude`) + ordering (`Oldest` vs `Newest`).
        let multi = store
            .search_history(&HistorySearchQuery {
                queries: vec![
                    "checkpoint".to_string(),
                    "生產環境".to_string(),
                    "Step".to_string(),
                ],
                exclude: vec!["heartbeat".to_string()],
                agent: Some("alice".to_string()),
                include_tools: false,
                sort: HistorySort::Oldest,
                limit: 10,
                ..HistorySearchQuery::default()
            })
            .unwrap();
        assert_eq!(multi.len(), 2);
        assert!(multi[0].text.contains("Step 1"));
        assert!(multi[1].text.contains("Step 3"));

        // 3. Tool history filtering (`only_tools: true` and `tool_name: Some("browser")`).
        let tools_only = store
            .search_history(&HistorySearchQuery {
                agent: Some("alice".to_string()),
                only_tools: true,
                tool_name: Some("browser".to_string()),
                limit: 10,
                ..HistorySearchQuery::default()
            })
            .unwrap();
        assert_eq!(tools_only.len(), 1);
        assert!(tools_only[0].text.contains("[tool:browser]"));

        // 4. Sequence range (`from_seq` / `to_seq`) and `radius` expansion.
        let ranged = store
            .search_history(&HistorySearchQuery {
                agent: Some("alice".to_string()),
                include_tools: false,
                from_seq: Some(3),
                to_seq: Some(3),
                limit: 10,
                ..HistorySearchQuery::default()
            })
            .unwrap();
        assert_eq!(ranged.len(), 1);
        assert!(ranged[0].text.contains("Step 3"));

        let with_radius = store
            .search_history(&HistorySearchQuery {
                query: Some("heartbeat".to_string()),
                agent: Some("alice".to_string()),
                include_tools: false,
                radius: 1,
                limit: 1,
                ..HistorySearchQuery::default()
            })
            .unwrap();
        assert_eq!(with_radius.len(), 3);
        assert!(with_radius[0].text.contains("Step 1"));
        assert!(with_radius[1].text.contains("Step 2"));
        assert!(with_radius[2].text.contains("Step 3"));

        // 5. FTS5 filter pushdown: even when another agent has 250 matching rows,
        // filtering by `agent = "alice"` with `limit = 1` finds Alice's row via inverted-index intersection.
        let bob = agent(&store, "bob");
        for i in 0..250 {
            store
                .receive(&bob.id, Incoming::new(format!("heartbeat noise {i}")))
                .unwrap();
        }
        let pushed_down = store
            .search_history(&HistorySearchQuery {
                query: Some("heartbeat".to_string()),
                agent: Some("alice".to_string()),
                limit: 1,
                ..HistorySearchQuery::default()
            })
            .unwrap();
        assert_eq!(pushed_down.len(), 1);
        assert!(pushed_down[0].to.contains("alice") || pushed_down[0].from.contains("alice"));
        assert!(pushed_down[0].text.contains("Step 2"));
    }

    #[test]
    fn idle_release_preserves_pending_work_and_role_independent_identity() {
        let (_dir, store) = setup();
        let parent = agent(&store, "parent");
        for (name, parent_id, role) in [
            ("root", None, Some("Researcher")),
            ("labelled", Some(parent.id.clone()), Some("Researcher")),
            ("plain", Some(parent.id.clone()), None),
        ] {
            let mut new = NewAgent::new(name, "codex");
            new.parent_id = parent_id;
            new.role = role.map(str::to_string);
            new.instructions = Some("Keep serving future requests.".into());
            let agent = store.create_agent(new).unwrap();
            store.set_agent_running(&agent.id, "/work").unwrap();
            store.receive(&agent.id, Incoming::new("task")).unwrap();
            assert!(!store.complete_idle_agent(&agent.id).unwrap());
            store.note_delivered(&agent.id, 1).unwrap();
            assert!(!store.complete_idle_agent(&agent.id).unwrap());
            store.note_turn_completed(&agent.id).unwrap();
            assert!(store.complete_idle_agent(&agent.id).unwrap());
            let idle = store.agent(&agent.id).unwrap();
            assert_eq!(idle.role, agent.role);
            assert_eq!(idle.instructions, agent.instructions);
            assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 1);
            store
                .receive(&agent.id, Incoming::new("follow-up"))
                .unwrap();
            assert_eq!(store.agent(&agent.id).unwrap().state, "running");
            assert!(!store.complete_idle_agent(&agent.id).unwrap());
        }
    }

    #[test]
    fn relationship_discovery_includes_children_and_siblings_without_roles() {
        let (dir, store) = setup();
        let parent = agent(&store, "parent");
        let mut new = NewAgent::new("first", "codex");
        new.parent_id = Some(parent.id.clone());
        let first = store.create_agent(new).unwrap();
        let mut new = NewAgent::new("second", "codex");
        new.parent_id = Some(parent.id.clone());
        let second = store.create_agent(new).unwrap();
        store.set_agent_scope(&first.id, "siblings").unwrap();
        store.set_agent_scope(&second.id, "siblings").unwrap();
        let roster = read_children_roster(dir.path(), parent.id.as_str());
        assert_eq!(
            roster.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
            ["first", "second"]
        );
        let ReceiveOutcome::New(parent_mail) =
            store.receive(&parent.id, Incoming::new("check")).unwrap()
        else {
            panic!("new mail expected")
        };
        let topology = parent_mail.context.topology.as_deref().unwrap();
        assert!(topology.contains("agent:first"));
        assert!(topology.contains("agent:second"));
        let ReceiveOutcome::New(child_mail) =
            store.receive(&first.id, Incoming::new("check")).unwrap()
        else {
            panic!("new mail expected")
        };
        assert!(
            child_mail
                .context
                .topology
                .as_deref()
                .unwrap()
                .contains("agent:second")
        );
        // A private sibling remains hidden even though it has a label.
        store
            .set_agent_role(&second.id, Some("Researcher"))
            .unwrap();
        store.set_agent_scope(&second.id, "private").unwrap();
        let ReceiveOutcome::New(changed) = store
            .receive(&first.id, Incoming::new("check again"))
            .unwrap()
        else {
            panic!("new mail expected")
        };
        assert!(
            changed
                .context
                .topology
                .as_deref()
                .unwrap()
                .contains("-sibling: agent:second")
        );
    }

    #[test]
    fn upgrade_materializes_legacy_effective_scopes_without_changing_work() {
        let (dir, store) = setup();
        let root = agent(&store, "root");
        let mut expected = Vec::new();
        let mut schedules = Vec::new();
        for (name, role, env, scope) in [
            ("labelled", Some("Researcher"), vec![], "siblings"),
            ("plain", None, vec![], "private"),
            ("liaison-assistant", Some("Liaison"), vec![], "isolated"),
            (
                "explicit",
                Some("Researcher"),
                vec![("ANYAGENTS_SCOPE".into(), "private".into())],
                "private",
            ),
            (
                "legacy-public",
                Some("Researcher"),
                vec![("ANYAGENTS_PUBLIC".into(), "1".into())],
                "public",
            ),
            (
                "invalid-scope",
                Some("Researcher"),
                vec![("ANYAGENTS_SCOPE".into(), "unknown".into())],
                "siblings",
            ),
        ] {
            let mut new = NewAgent::new(name, "codex");
            new.parent_id = Some(root.id.clone());
            new.role = role.map(str::to_string);
            new.instructions = Some("User-authored instructions".into());
            let agent = store.create_agent(new).unwrap();
            store.set_agent_running(&agent.id, "/work").unwrap();
            store
                .receive(&agent.id, Incoming::new("completed"))
                .unwrap();
            store.note_delivered(&agent.id, 1).unwrap();
            store.note_turn_completed(&agent.id).unwrap();
            store.receive(&agent.id, Incoming::new("pending")).unwrap();
            schedules.push(
                store
                    .create_schedule(NewSchedule {
                        agent_id: agent.id.clone(),
                        kind: "at".into(),
                        spec: "1h".into(),
                        prompt: "Future work".into(),
                        interval_ms: None,
                        next_fire_at_ms: Some(now_ms() + 3_600_000),
                        watch_baseline: None,
                    })
                    .unwrap(),
            );
            let mut env = env;
            env.push(("CUSTOM".into(), "preserve".into()));
            // Write the exact pre-upgrade flat-array representation.
            store
                .conn()
                .execute(
                    "UPDATE agents SET launch_env = ?1 WHERE id = ?2",
                    params![encode_env(&env), agent.id.as_str()],
                )
                .unwrap();
            expected.push((store.agent(&agent.id).unwrap(), scope));
        }
        for _ in 0..2 {
            let reopened = Store::open(dir.path().join("state.db")).unwrap();
            for schedule in &schedules {
                assert_eq!(
                    reopened.get_schedule(&schedule.id).unwrap().as_ref(),
                    Some(schedule)
                );
            }
            for (before, scope) in &expected {
                let after = reopened.agent(&before.id).unwrap();
                assert_eq!(after.scope(), *scope);
                assert!(
                    after
                        .launch_env
                        .contains(&("ANYAGENTS_SCOPE".into(), scope.to_string()))
                );
                assert!(
                    after
                        .launch_env
                        .contains(&("CUSTOM".into(), "preserve".into()))
                );
                let mut normalized = after.clone();
                normalized.launch_env = before.launch_env.clone();
                assert_eq!(&normalized, before);
                assert_eq!(reopened.delivered_cursor(&before.id).unwrap(), 1);
                assert_eq!(
                    reopened.next_undelivered(&before.id).unwrap().unwrap().text,
                    "pending"
                );
            }
        }
        let new = store
            .create_agent(NewAgent {
                name: "new-child".into(),
                parent_id: Some(root.id),
                role: Some("Researcher".into()),
                ..NewAgent::new("unused", "codex")
            })
            .unwrap();
        assert_eq!(new.scope(), "private");
    }

    #[test]
    fn subagent_scales_to_idle_when_done_wakes_on_message_and_stays_archived_when_retired() {
        let (_dir, store) = setup();
        let root = agent(&store, "root");
        let mut child_new = NewAgent::new("root-worker", "opencode");
        child_new.parent_id = Some(root.id.clone());
        let child = store.create_agent(child_new).unwrap();
        store.set_agent_running(&child.id, "/work").unwrap();

        // Send initial task message (seq = 1), deliver it, and complete turn 1.
        store.receive(&child.id, Incoming::new("task 1")).unwrap();
        store.note_delivered(&child.id, 1).unwrap();
        store.note_turn_completed(&child.id).unwrap();

        // complete_idle_subagent transitions the sub-agent to `idle` (scaled to zero).
        assert!(store.complete_idle_subagent(&child.id).unwrap());
        assert_eq!(store.agent(&child.id).unwrap().state, "idle");

        // Sending a follow-up message wakes an `idle` sub-agent back to `running`.
        store.receive(&child.id, Incoming::new("task 2")).unwrap();
        assert_eq!(store.agent(&child.id).unwrap().state, "running");

        // Explicit archive transitions the sub-agent to `archived` (retired), which does NOT auto-wake.
        store.set_agent_state(&child.id, "archived").unwrap();
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
        store.receive(&child.id, Incoming::new("task 3")).unwrap();
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    }

    #[test]
    fn only_direct_send_ingress_wakes_archived_and_duplicate_does_not_rewake() {
        let (dir, store) = setup();
        let recipient = agent(&store, "recipient");
        store.set_agent_state(&recipient.id, "archived").unwrap();
        // Lifecycle, channel and schedule ingress uses receive, even when a
        // generated notification names an agent as its sender.
        for (sender, key, text) in [
            (
                "agent:child",
                "subagent_completed:child:1",
                "[Sub-Agent Completed] done",
            ),
            (
                "agent:child",
                "subagent-error:child:1",
                "[Sub-Agent Error] failed",
            ),
            ("system", "idle:1", "idle event"),
            ("telegram:user:123", "telegram:123:1", "channel update"),
            ("schedule:timer", "schedule:timer:1", "timer prompt"),
            ("schedule:watch", "watch:watch:1", "watch prompt"),
        ] {
            let mut incoming = Incoming::new(text);
            incoming.sender = Some(sender.into());
            incoming.source_key = Some(key.into());
            store.receive(&recipient.id, incoming).unwrap();
            assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
        }
        let mut incoming = Incoming::new("intentional follow-up");
        incoming.sender = Some("agent:parent".into());
        incoming.source_key = Some("direct-1".into());
        let sent = store
            .receive_direct_send(&recipient.id, incoming.clone())
            .unwrap();
        assert!(matches!(sent, ReceiveOutcome::New(_)));
        assert_eq!(store.agent(&recipient.id).unwrap().state, "running");
        drop(store);
        let store = Store::open(dir.path().join("state.db")).unwrap();
        assert_eq!(store.agent(&recipient.id).unwrap().state, "running");
        store.set_agent_state(&recipient.id, "archived").unwrap();
        assert!(matches!(
            store.receive_direct_send(&recipient.id, incoming).unwrap(),
            ReceiveOutcome::Duplicate(_)
        ));
        assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
        assert_eq!(store.inbox(&recipient.id, None).unwrap().len(), 7);
    }

    #[test]
    fn rename_agent_preserves_id_grants_history_and_resolves_old_alias_and_role() {
        let (_dir, store) = setup();
        let original_worker = agent(&store, "original_worker");
        store.set_agent_admin(&original_worker.id, true).unwrap();
        store.join_group("owners", &original_worker.id).unwrap();
        store
            .receive(&original_worker.id, Incoming::new("revenue check"))
            .unwrap();

        let renamed = store
            .rename_agent(&original_worker.id, "renamed_worker")
            .unwrap();
        assert_eq!(renamed.id, original_worker.id);
        assert_eq!(renamed.name, "renamed_worker");
        assert!(renamed.is_admin);

        let with_role = store
            .set_agent_role(&original_worker.id, Some("Revenue"))
            .unwrap();
        assert_eq!(with_role.role.as_deref(), Some("Revenue"));

        // Old name resolves via alias table
        let by_old = store.agent_by_name("original_worker").unwrap();
        assert_eq!(by_old.id, original_worker.id);
        assert_eq!(by_old.name, "renamed_worker");
        assert_eq!(by_old.role.as_deref(), Some("Revenue"));

        let by_prefixed = store.agent_by_name("agent:original_worker").unwrap();
        assert_eq!(by_prefixed.id, original_worker.id);

        assert_eq!(
            store.agent_aliases(&original_worker.id).unwrap(),
            vec!["original_worker".to_string()]
        );
        assert_eq!(
            store.group_members("owners").unwrap(),
            vec!["renamed_worker".to_string()]
        );
        assert_eq!(store.inbox(&original_worker.id, None).unwrap().len(), 1);
    }

    #[test]
    fn telegram_reaction_is_not_dropped_by_origin_dedup_and_wakes_targeted_author_only() {
        let (_dir, store) = setup();
        let author_worker = agent(&store, "author_worker");
        let peer_worker = agent(&store, "peer_worker");

        // 1. AuthorWorker posts message #42 in group -100999, which is recorded in both bots' mailboxes.
        let mut msg42 = Incoming::new("Here is the sample economy simulation report.");
        msg42.sender = Some("agent:author_worker".to_string());
        msg42.origin = Some(Origin {
            chat_id: Some(-100999),
            thread_id: None,
            message_id: Some(42),
        });
        store.receive(&author_worker.id, msg42.clone()).unwrap();
        store.receive(&peer_worker.id, msg42).unwrap();
        store.note_delivered(&author_worker.id, 1).unwrap();
        store.note_delivered(&peer_worker.id, 1).unwrap();
        store.set_agent_state(&author_worker.id, "idle").unwrap();
        store.set_agent_state(&peer_worker.id, "idle").unwrap();

        // 2. Alex reacts 👍 on message #42. Neither bot drops it as a duplicate of #42!
        //    AuthorWorker (the author of #42) receives `event = "reaction"` and wakes to `running`.
        //    PeerWorker (a peer bot in the same multi-bot group) receives `event = "reaction_passive"`
        //    in full awareness without waking from `idle` on the reaction alone.
        let mut react = Incoming::new("[Reaction 👍 on message #42]");
        react.sender = Some("telegram:user:777:alex".to_string());
        react.origin = Some(Origin {
            chat_id: Some(-100999),
            thread_id: None,
            message_id: Some(42),
        });

        let author_worker_out = store.receive(&author_worker.id, react.clone()).unwrap();
        let ReceiveOutcome::New(author_worker_msg) = author_worker_out else {
            panic!("reaction must not be dropped as duplicate of original message #42");
        };
        assert_eq!(author_worker_msg.context.event.as_deref(), Some("reaction"));
        assert_eq!(
            author_worker_msg.context.reply_to_author.as_deref(),
            Some("agent:author_worker")
        );
        assert!(
            author_worker_msg
                .text
                .contains("Here is the sample economy simulation report.")
        );
        assert_eq!(store.agent(&author_worker.id).unwrap().state, "running");
        assert!(store.has_actionable_undelivered(&author_worker.id).unwrap());

        let peer_worker_out = store.receive(&peer_worker.id, react).unwrap();
        let ReceiveOutcome::New(peer_worker_msg) = peer_worker_out else {
            panic!("peer bot must also record reaction for full awareness");
        };
        assert_eq!(
            peer_worker_msg.context.event.as_deref(),
            Some("reaction_passive")
        );
        assert_eq!(store.agent(&peer_worker.id).unwrap().state, "idle");
        assert!(!store.has_actionable_undelivered(&peer_worker.id).unwrap());
    }

    #[test]
    fn edited_message_delivers_compact_line_diff_preserving_edit_awareness_and_skips_noop() {
        let (_dir, store) = setup();
        let root_worker = agent(&store, "root_worker");
        let origin = Origin {
            chat_id: Some(-100777),
            thread_id: None,
            message_id: Some(88),
        };

        let v0 = "\
## Daily Progress Report
- Line A: Unchanged 1
- Line B: Unchanged 2
- Line C: Unchanged 3
- Status: CI running
- Deploy: pending
- Line D: Unchanged 4
- Line E: Unchanged 5";
        let mut initial = Incoming::new(v0);
        initial.sender = Some("agent:helper".to_string());
        initial.origin = Some(origin);
        initial.source_key = Some("bot-a:1".into());
        let ReceiveOutcome::New(msg0) = store.receive(&root_worker.id, initial).unwrap() else {
            panic!("initial message must be stored");
        };
        assert_eq!(msg0.text, v0);

        // Edit 1: changes `Status: CI running` -> `Status: Merged (#67)`
        let v1 = v0.replace("Status: CI running", "Status: Merged (#67)");
        let mut edit1 = Incoming::new(format!("[Edited message #88]: {v1}"));
        edit1.sender = Some("agent:helper".to_string());
        edit1.origin = Some(origin);
        edit1.source_key = Some("bot-a:2".into());
        let ReceiveOutcome::New(msg1) = store.receive(&root_worker.id, edit1).unwrap() else {
            panic!("first edit must deliver a new edited event");
        };
        assert_eq!(msg1.context.event.as_deref(), Some("edited"));
        assert!(msg1.text.starts_with("[Edited message #88]:\n"));
        assert!(msg1.text.contains("- - Status: CI running"));
        assert!(msg1.text.contains("+ - Status: Merged (#67)"));
        assert!(!msg1.text.contains("Line A: Unchanged 1"));

        // Edit 2: changes `Deploy: pending` -> `Deploy: live` (must diff against v1, not v0!)
        let v2 = v1.replace("Deploy: pending", "Deploy: live");
        let mut edit2 = Incoming::new(format!("[Edited message #88]: {v2}"));
        edit2.sender = Some("agent:helper".to_string());
        edit2.origin = Some(origin);
        edit2.source_key = Some("bot-a:3".into());
        let ReceiveOutcome::New(msg2) = store.receive(&root_worker.id, edit2).unwrap() else {
            panic!("second edit must deliver a new edited event");
        };
        assert_eq!(msg2.context.event.as_deref(), Some("edited"));
        assert!(msg2.text.contains("- - Deploy: pending"));
        assert!(msg2.text.contains("+ - Deploy: live"));
        assert!(!msg2.text.contains("- - Status: CI running"));

        // Edit 3: no-op edit with identical v2 text must be skipped as Duplicate
        let mut edit3 = Incoming::new(format!("[Edited message #88]: {v2}"));
        edit3.sender = Some("agent:helper".to_string());
        edit3.origin = Some(origin);
        edit3.source_key = Some("bot-a:4".into());
        let out3 = store.receive(&root_worker.id, edit3).unwrap();
        assert!(matches!(out3, ReceiveOutcome::Duplicate(_)));
    }

    #[test]
    fn group_quote_enriches_stored_attachments_and_delivers_late_quote_upgrade() {
        let (_dir, store) = setup();
        let reader_worker = agent(&store, "reader_worker");
        let renamed_worker = agent(&store, "renamed_worker");
        let chat_id = -5290931492;

        // 1. Original post by renamed_worker (`message_id = 1750`) with local attachment path in `messages`
        let mut orig = Incoming::new(
            "Attachment render preview\n[Attachment (photo): /tmp/example/renders/preview.png]",
        );
        orig.sender = Some("agent:renamed_worker".to_string());
        orig.source_key = Some(format!("tg-out:{chat_id}:renamed_worker:1750"));
        orig.origin = Some(Origin {
            chat_id: Some(chat_id),
            thread_id: None,
            message_id: Some(1750),
        });
        store.receive(&reader_worker.id, orig).unwrap();

        // 2. Peer bot without quote relays human reply first (`message_id = 3255`, `quote = None`)
        let mut unquoted_relay = Incoming::new("仲有呢啲");
        unquoted_relay.sender =
            Some("telegram:user:107671190:3255 (Alex @alex_example)".to_string());
        unquoted_relay.source_time_ms = Some(1_793_798_602_000);
        unquoted_relay.source_key = Some(format!("tg-in:{chat_id}:vulcan:3255"));
        unquoted_relay.origin = Some(Origin {
            chat_id: Some(chat_id),
            thread_id: None,
            message_id: Some(3255),
        });
        let ReceiveOutcome::New(first_msg) =
            store.receive(&reader_worker.id, unquoted_relay).unwrap()
        else {
            panic!("first relay must be stored");
        };
        assert!(!has_quoted_block(&first_msg.text));

        // Simulate harness having already delivered `orig` (seq 1) and `first_msg` (seq 2) before the quoted webhook arrives
        store.note_delivered(&reader_worker.id, 1).unwrap();
        store
            .note_delivered(&reader_worker.id, first_msg.seq)
            .unwrap();

        // 3. Replied-to bot (`renamed_worker`, `message_id = 2506`) relays the same message WITH `quote`
        let mut quoted_relay = Incoming::new("仲有呢啲");
        quoted_relay.sender = Some("telegram:user:107671190:2506 (Alex @alex_example)".to_string());
        quoted_relay.source_time_ms = Some(1_793_798_602_000);
        quoted_relay.source_key = Some(format!("tg-in:{chat_id}:{}:2506", renamed_worker.id));
        quoted_relay.quote = Some(crate::Quote {
            author: "RenamedWorker (@renamed_worker_bot)".to_string(),
            author_is_bot: true,
            sender_id: Some(9002),
            text: "Attachment render preview".to_string(),
            reference: Some(crate::QuoteRef {
                chat_id,
                message_id: 1750,
            }),
            mentions: Vec::new(),
        });
        quoted_relay.origin = Some(Origin {
            chat_id: Some(chat_id),
            thread_id: None,
            message_id: Some(2506),
        });
        let ReceiveOutcome::New(upgraded_msg) =
            store.receive(&reader_worker.id, quoted_relay).unwrap()
        else {
            panic!("late quote arrival after unquoted delivery must deliver the quoted message");
        };
        assert!(has_quoted_block(&upgraded_msg.text));
        assert!(
            upgraded_msg
                .text
                .contains("[Attachment (photo): /tmp/example/renders/preview.png]"),
            "quote must be enriched with stored attachment path: {}",
            upgraded_msg.text
        );

        // 4. Subsequent webhook from reader_worker's own bot (`message_id = 14257`) is deduplicated
        let mut third_webhook = Incoming::new("仲有呢啲");
        third_webhook.sender =
            Some("telegram:user:107671190:14257 (Alex @alex_example)".to_string());
        third_webhook.source_time_ms = Some(1_793_798_602_000);
        third_webhook.source_key = Some("bot_reader_worker:99999".to_string());
        third_webhook.origin = Some(Origin {
            chat_id: Some(chat_id),
            thread_id: None,
            message_id: Some(14257),
        });
        let third_out = store.receive(&reader_worker.id, third_webhook).unwrap();
        assert!(matches!(third_out, ReceiveOutcome::Duplicate(_)));
    }

    #[test]
    fn event_driven_topology_initial_delta_and_isolated_scope() {
        let (_dir, store) = setup();
        let root_worker = store
            .create_agent(NewAgent {
                name: "root_worker".to_string(),
                role: Some("公司業務總管".to_string()),
                description: None,
                harness: "agy".to_string(),
                model: None,
                workdir: None,
                instructions: None,
                parent_id: None,
            })
            .unwrap();
        let helper = store
            .create_agent(NewAgent {
                name: "helper".to_string(),
                role: Some("產品與功能".to_string()),
                description: None,
                harness: "agy".to_string(),
                model: None,
                workdir: None,
                instructions: None,
                parent_id: Some(root_worker.id.clone()),
            })
            .unwrap();
        let renamed_worker = store
            .create_agent(NewAgent {
                name: "renamed_worker".to_string(),
                role: Some("商業化與營收".to_string()),
                description: None,
                harness: "agy".to_string(),
                model: None,
                workdir: None,
                instructions: None,
                parent_id: Some(root_worker.id.clone()),
            })
            .unwrap();
        assert_eq!(root_worker.scope(), "public");
        store.set_agent_scope(&helper.id, "siblings").unwrap();
        store
            .set_agent_scope(&renamed_worker.id, "siblings")
            .unwrap();

        // 1. First message to helper from root_worker -> emits `<topology event="initial" ... />`
        let mut m1 = Incoming::new("Start sprint");
        m1.sender = Some("agent:root_worker".to_string());
        let ReceiveOutcome::New(msg1) = store.receive(&helper.id, m1).unwrap() else {
            panic!("expected new message");
        };
        assert_eq!(msg1.context.sender_rel.as_deref(), Some("parent"));
        assert_eq!(msg1.context.sender_role.as_deref(), Some("公司業務總管"));
        let topo1 = msg1.context.topology.as_deref().expect("initial topology");
        assert!(topo1.contains("event=\"initial\""), "topo1={topo1}");
        assert!(topo1.contains("scope=\"siblings\""), "topo1={topo1}");
        assert!(
            topo1.contains("parent=\"agent:root_worker [公司業務總管]\""),
            "topo1={topo1}"
        );
        assert!(
            topo1.contains("agent:renamed_worker [商業化與營收]"),
            "topo1={topo1}"
        );

        // 2. Second message with no topology change -> `topology` is None
        let mut m2 = Incoming::new("Status check");
        m2.sender = Some("agent:renamed_worker".to_string());
        let ReceiveOutcome::New(msg2) = store.receive(&helper.id, m2).unwrap() else {
            panic!("expected new message");
        };
        assert_eq!(msg2.context.sender_rel.as_deref(), Some("sibling"));
        assert!(
            msg2.context.topology.is_none(),
            "no delta expected when unchanged"
        );

        // 3. New standing sibling `reader_worker` added -> next message to helper emits `<topology event="delta" ... />`
        let reader_worker = store
            .create_agent(NewAgent {
                name: "reader_worker".to_string(),
                role: Some("用戶體驗與設計".to_string()),
                description: None,
                harness: "agy".to_string(),
                model: None,
                workdir: None,
                instructions: None,
                parent_id: Some(root_worker.id.clone()),
            })
            .unwrap();
        store
            .set_agent_scope(&reader_worker.id, "siblings")
            .unwrap();
        let mut m3 = Incoming::new("Design sync");
        m3.sender = Some("agent:root_worker".to_string());
        let ReceiveOutcome::New(msg3) = store.receive(&helper.id, m3).unwrap() else {
            panic!("expected new message");
        };
        let topo3 = msg3
            .context
            .topology
            .as_deref()
            .expect("delta topology on add");
        assert!(topo3.contains("event=\"delta\""), "topo3={topo3}");
        assert!(
            topo3.contains("+sibling: agent:reader_worker [用戶體驗與設計]"),
            "topo3={topo3}"
        );

        // 4. Archive `reader_worker` -> next message to helper emits `-sibling: reader_worker`
        store
            .set_agent_state(&reader_worker.id, "archived")
            .unwrap();
        let mut m4 = Incoming::new("After archive");
        m4.sender = Some("agent:root_worker".to_string());
        let ReceiveOutcome::New(msg4) = store.receive(&helper.id, m4).unwrap() else {
            panic!("expected new message");
        };
        let topo4 = msg4
            .context
            .topology
            .as_deref()
            .expect("delta topology on archive");
        assert!(
            topo4.contains("-sibling: agent:reader_worker [用戶體驗與設計]"),
            "topo4={topo4}"
        );

        // 5. Explicitly isolated agents see no siblings regardless of name or role
        let stanley = store
            .create_agent(NewAgent {
                name: "stanley-assistant".to_string(),
                role: Some("Stanley 專屬助手".to_string()),
                description: None,
                harness: "agy".to_string(),
                model: None,
                workdir: None,
                instructions: None,
                parent_id: Some(root_worker.id.clone()),
            })
            .unwrap();
        store.set_agent_scope(&stanley.id, "isolated").unwrap();
        let mut m5 = Incoming::new("Hello Stanley");
        m5.sender = Some("agent:root_worker".to_string());
        let ReceiveOutcome::New(msg5) = store.receive(&stanley.id, m5).unwrap() else {
            panic!("expected new message");
        };
        let topo5 = msg5
            .context
            .topology
            .as_deref()
            .expect("initial topology for isolated");
        assert!(topo5.contains("scope=\"isolated\""), "topo5={topo5}");
        assert!(!topo5.contains("siblings="), "topo5={topo5}");
    }

    fn sleep_test_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    struct ScopedSleepModelEnv {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl ScopedSleepModelEnv {
        fn new(url: Option<&str>) -> Self {
            let guard = sleep_test_env_lock()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            unsafe {
                match url {
                    Some(u) => std::env::set_var("ANYAGENTS_SLEEP_MODEL_URL", u),
                    None => std::env::remove_var("ANYAGENTS_SLEEP_MODEL_URL"),
                }
                std::env::remove_var("ANYAGENTS_SLEEP_CONSOLIDATION_FORCE");
            }
            Self { _guard: guard }
        }
    }

    impl Drop for ScopedSleepModelEnv {
        fn drop(&mut self) {
            unsafe {
                std::env::remove_var("ANYAGENTS_SLEEP_MODEL_URL");
                std::env::remove_var("ANYAGENTS_SLEEP_CONSOLIDATION_FORCE");
            }
        }
    }

    fn spawn_mock_sleep_model_server(
        plan_json: String,
    ) -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        std::thread::JoinHandle<()>,
    ) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let req_clone = std::sync::Arc::clone(&requests);
        let handle = std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            let s = String::from_utf8_lossy(&buf);
                            if let Some((hdrs, body)) = s.split_once("\r\n\r\n") {
                                let content_len = hdrs
                                    .lines()
                                    .find_map(|l| {
                                        let (k, v) = l.split_once(':')?;
                                        if k.trim().eq_ignore_ascii_case("content-length") {
                                            v.trim().parse::<usize>().ok()
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or(0);
                                if body.len() >= content_len {
                                    break;
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
                req_clone
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf).into_owned());
                let resp_body = serde_json::json!({
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": plan_json
                            }
                        }
                    ]
                })
                .to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
            }
        });
        (
            format!("http://{addr}/v1/chat/completions"),
            requests,
            handle,
        )
    }

    #[test]
    fn sleep_memory_maintenance_rebuilds_index_for_disk_edits_without_hardcoded_topics() {
        let _env = ScopedSleepModelEnv::new(None);
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("sleep-neutral", "spiron"))
            .unwrap();
        let mut msg = Incoming::new("Directive message body that exceeds thirty characters easily");
        msg.sender = Some("operator".into());
        store.receive(&agent.id, msg).unwrap();

        let root = dir.path().join("agents").join(agent.id.as_str());
        std::fs::create_dir_all(root.join("memory")).unwrap();
        std::fs::write(
            root.join("memory/custom-topic.md"),
            "# Custom Topic\n\nAgent-authored invariant fact.\n",
        )
        .unwrap();

        let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
        assert!(modified);
        let mem_md = std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap();
        assert!(mem_md.contains("custom-topic"));
        let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].slug, "custom-topic");
        let sleep = crate::learned_memory::read_sleep_state(&root);
        assert_eq!(sleep.last_inbound_seq, 1);
        assert_eq!(sleep.consolidated_topics, 1);
    }

    #[test]
    fn sleep_consolidation_strips_quotes_and_rejects_noise() {
        let (url, requests, handle) =
            spawn_mock_sleep_model_server(r#"{"operations":[]}"#.to_string());
        let _env = ScopedSleepModelEnv::new(Some(&url));
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("sleep-noise", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        std::fs::create_dir_all(root.join("memory")).unwrap();

        let mut quoted_msg = Incoming::new("OK, checking now");
        quoted_msg.sender = Some("user:alex".into());
        quoted_msg.quote = Some(crate::quote::Quote {
            author: "Alex".into(),
            text: "quoted_echo_secret_should_be_stripped".into(),
            ..Default::default()
        });
        store.receive(&agent.id, quoted_msg).unwrap();

        let mut no_reply = Incoming::new("NO_REPLY");
        no_reply.sender = Some("agent:peer".into());
        store.receive(&agent.id, no_reply).unwrap();

        let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
        handle.join().unwrap();
        assert!(!modified, "noise rejection should not create topic files");

        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].contains("OK, checking now"));
        assert!(
            !captured[0].contains("quoted_echo_secret_should_be_stripped"),
            "quoted block must be stripped before LLM consolidation"
        );
        assert!(
            !captured[0].contains("NO_REPLY"),
            "NO_REPLY turns must be filtered out"
        );

        let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
        assert!(topics.is_empty());
        let sleep = crate::learned_memory::read_sleep_state(&root);
        assert_eq!(sleep.last_inbound_seq, 2);
        assert_eq!(sleep.consolidated_topics, 0);
    }

    #[test]
    fn sleep_consolidation_upserts_existing_topic_in_place_and_resolves_conflicts() {
        let plan = serde_json::json!({
            "operations": [
                {
                    "op": "upsert",
                    "topic": "service-endpoint",
                    "description": "Primary API endpoint migrated to port 9090 (replaced 8080)",
                    "content": "# Service Endpoint\n\nPrimary API endpoint runs on port 9090 in production."
                }
            ]
        })
        .to_string();
        let (url, requests, handle) = spawn_mock_sleep_model_server(plan);
        let _env = ScopedSleepModelEnv::new(Some(&url));
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("sleep-upsert", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        crate::learned_memory::upsert_memory_topic(
            &root,
            &agent.name,
            None,
            "service-endpoint",
            Some("Primary API endpoint on port 8080"),
            "# Service Endpoint\n\nPrimary API endpoint runs on port 8080.",
            1700000000000,
        )
        .unwrap();

        let mut msg = Incoming::new("We migrated the service-endpoint port from 8080 to 9090.");
        msg.sender = Some("operator".into());
        store.receive(&agent.id, msg).unwrap();

        let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
        handle.join().unwrap();
        assert!(modified);

        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert!(
            captured[0].contains("Primary API endpoint runs on port 8080."),
            "existing topic body must be included in consolidation prompt"
        );

        let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
        assert_eq!(
            topics.len(),
            1,
            "must update topic in place without duplicating"
        );
        assert_eq!(topics[0].slug, "service-endpoint");
        assert_eq!(
            topics[0].description,
            "Primary API endpoint migrated to port 9090 (replaced 8080)"
        );
        assert!(topics[0].updated_at_ms > 1700000000000);

        let on_disk = std::fs::read_to_string(root.join("memory/service-endpoint.md")).unwrap();
        assert!(on_disk.contains("created_at_ms: 1700000000000"));
        assert!(on_disk.contains("port 9090 in production"));
        assert!(!on_disk.contains("runs on port 8080"));
    }

    #[test]
    fn sleep_consolidation_defragments_duplicate_topics_and_syncs_fts5() {
        let plan = serde_json::json!({
            "operations": [
                {
                    "op": "upsert",
                    "topic": "cache-ttl",
                    "description": "Unified cache TTL policy (300s L1, 3600s L2)",
                    "content": "# Unified Cache TTL\n\nCanonical merged token unifiedcachetoken."
                },
                {
                    "op": "delete",
                    "topic": "cache-ttl-v1"
                },
                {
                    "op": "delete",
                    "topic": "cache-ttl-v2"
                }
            ]
        })
        .to_string();
        let (url, _requests, handle) = spawn_mock_sleep_model_server(plan);
        let _env = ScopedSleepModelEnv::new(Some(&url));
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("sleep-defrag", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        crate::learned_memory::upsert_memory_topic(
            &root,
            &agent.name,
            None,
            "cache-ttl-v1",
            Some("Legacy L1 cache TTL"),
            "Fragment one oldfragmenttoken",
            1700000000000,
        )
        .unwrap();
        crate::learned_memory::upsert_memory_topic(
            &root,
            &agent.name,
            None,
            "cache-ttl-v2",
            Some("Legacy L2 cache TTL"),
            "Fragment two secondfragmenttoken",
            1700000001000,
        )
        .unwrap();
        store.sync_agent_memory_index(&agent).unwrap();

        let mut msg = Incoming::new("Consolidate cache TTL notes into a single topic.");
        msg.sender = Some("operator".into());
        store.receive(&agent.id, msg).unwrap();

        let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
        handle.join().unwrap();
        assert!(modified);

        let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].slug, "cache-ttl");
        let mem_md = std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap();
        assert!(mem_md.contains("cache-ttl"));
        assert!(!mem_md.contains("cache-ttl-v1"));
        assert!(!mem_md.contains("cache-ttl-v2"));

        let ids = [agent.id.to_string()];
        let merged_hits = store
            .search_host_files(
                &ids,
                crate::host_index::MemoryScope::Memory,
                &HistorySearchQuery {
                    query: Some("unifiedcachetoken".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(merged_hits.len(), 1);
        let old_hits = store
            .search_host_files(
                &ids,
                crate::host_index::MemoryScope::Memory,
                &HistorySearchQuery {
                    query: Some("oldfragmenttoken".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            old_hits.is_empty(),
            "deleted fragmented topic must be evicted from HostIndex FTS5"
        );
    }

    #[test]
    fn sleep_consolidation_defers_turns_during_cooldown_without_data_loss() {
        let plan = serde_json::json!({
            "operations": [
                {
                    "op": "upsert",
                    "topic": "deferred-fact",
                    "description": "Fact consolidated after cooldown expiry",
                    "content": "Durable fact body after cooldown."
                }
            ]
        })
        .to_string();
        let (url, requests, handle) = spawn_mock_sleep_model_server(plan);
        let _env = ScopedSleepModelEnv::new(Some(&url));
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("sleep-cooldown", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        std::fs::create_dir_all(root.join("memory")).unwrap();

        // Simulate a consolidation that just ran at `now_ms()`.
        let now = crate::now_ms();
        crate::learned_memory::write_sleep_state(
            &root,
            &crate::learned_memory::SleepState {
                last_consolidated_ms: now,
                last_inbound_seq: 0,
                last_outbound_ms: 0,
                consolidated_topics: 0,
            },
        )
        .unwrap();

        let mut msg = Incoming::new("Durable fact arriving during the 10-minute cooldown window.");
        msg.sender = Some("operator".into());
        store.receive(&agent.id, msg).unwrap();

        // First call during cooldown: must defer LLM call and preserve last_inbound_seq = 0.
        let modified_in_cooldown = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
        assert!(!modified_in_cooldown);
        assert!(requests.lock().unwrap().is_empty());
        let sleep_during = crate::learned_memory::read_sleep_state(&root);
        assert_eq!(
            sleep_during.last_inbound_seq, 0,
            "must not advance watermark while deferred by cooldown"
        );

        // Expire the 10-minute cooldown and run again: now it consolidates and advances watermark.
        crate::learned_memory::write_sleep_state(
            &root,
            &crate::learned_memory::SleepState {
                last_consolidated_ms: now - 605_000,
                last_inbound_seq: 0,
                last_outbound_ms: 0,
                consolidated_topics: 0,
            },
        )
        .unwrap();
        let modified_after = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
        handle.join().unwrap();
        assert!(modified_after);
        assert_eq!(requests.lock().unwrap().len(), 1);
        let sleep_after = crate::learned_memory::read_sleep_state(&root);
        assert_eq!(sleep_after.last_inbound_seq, 1);
        assert_eq!(sleep_after.consolidated_topics, 1);
    }

    #[test]
    fn steered_active_turn_advances_cursor_without_lag_and_reclaim_rewinds_custody() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store.create_agent(NewAgent::new("steered", "agy")).unwrap();

        let mut first_in = Incoming::new("long running turn");
        first_in.received_at_ms = Some(1_000);
        let ReceiveOutcome::New(first) = store.receive(&agent.id, first_in).unwrap() else {
            panic!("expected new message");
        };
        store.note_delivered(&agent.id, first.seq).unwrap();

        let mut steer_in = Incoming::new("mid-turn steer");
        steer_in.received_at_ms = Some(60_000);
        let ReceiveOutcome::New(steer) = store.receive(&agent.id, steer_in).unwrap() else {
            panic!("expected new steer");
        };
        store.note_delivered(&agent.id, steer.seq).unwrap();

        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), steer.seq);
        assert!(store.has_open_turn(&agent.id).unwrap());
        assert_eq!(
            store
                .lag(&agent.id, 3_600_000)
                .unwrap()
                .oldest_undelivered_age_ms,
            None
        );
        let open = store.open_turn(&agent.id).unwrap().unwrap();
        assert_eq!(open.messages.first().unwrap().received_at_ms, 1_000);

        store
            .reclaim_unconfirmed_steers(&agent.id, &[steer.id.to_string()])
            .unwrap();
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), first.seq);
        assert!(store.has_open_turn(&agent.id).unwrap());
        assert_eq!(
            store.next_undelivered(&agent.id).unwrap().unwrap().id,
            steer.id
        );
        assert!(
            store
                .message(&agent.id, steer.id.as_str())
                .unwrap()
                .unwrap()
                .delivered_at_ms
                .is_none()
        );
    }
    #[test]
    fn concurrent_turn_completion_never_sees_database_locked() {
        let (dir, store) = setup();
        let db = dir.path().join("state.db");
        let ids: Vec<AgentId> = (0..16)
            .map(|i| agent(&store, &format!("concurrent-{i}")).id)
            .collect();
        // Each worker owns a separate Store (separate connection), mirroring
        // one supervisor process per agent finishing turns at once.
        let mut handles = Vec::new();
        for id in &ids {
            let db = db.clone();
            let id = id.clone();
            handles.push(std::thread::spawn(move || {
                let store = Store::open(&db).unwrap();
                for _ in 0..25 {
                    store.note_turn_completed(&id).unwrap();
                    store
                        .record_usage(NewUsage {
                            agent_id: id.clone(),
                            harness: "claude-code".into(),
                            model: Some("m".into()),
                            input_tokens: 1,
                            output_tokens: 2,
                            cache_read_tokens: 0,
                            cache_write_tokens: 0,
                            reported_cost_micros: Some(3),
                            estimated_cost_micros: None,
                            billing_mode: BillingMode::Subscription,
                            quota_state: None,
                            day: "2026-01-01".into(),
                        })
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(store.list_usage(None).unwrap().len(), 16 * 25);
    }
}
