//! SQLite mailbox. A message is received when its insert commits, and that
//! commit assigns the next per-agent sequence. Source time is stored and never
//! used to order delivery. The delivery cursor moves only over a contiguous
//! prefix.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

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
use crate::migrate::{self, now_ms as clock_now_ms};

/// Milliseconds since the Unix epoch. A clock that is before the epoch reads as 0.
pub fn now_ms() -> i64 {
    clock_now_ms()
}

const MESSAGE_COLS: &str = "id, agent_id, seq, source_time_ms, source_key, sender, body, \
     reply_to, received_at_ms, delivered_at_ms, origin, context";

const AGENT_COLS: &str = "id, name, harness, model, workdir, instructions, state, \
     created_at_ms, launch_env, parent_id, is_admin, role, description";

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
    /// Agent name (`"sage"`, `"all"`) or `agt_...` ID.
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

/// Held for the life of one agent loop. Drop releases the lock.
#[derive(Debug)]
pub struct AgentLock {
    _file: File,
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
        conn.busy_timeout(Duration::from_secs(15))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Message(format!(
                "sqlite journal mode is {mode}, not wal"
            )));
        }
        let mut conn = conn;
        migrate::migrate(&mut conn)?;
        if migrate::current_version(&conn)? < 1 {
            return Err(Error::Message("migrations did not apply".into()));
        }
        let has_legacy_state: bool = conn
            .query_row(
                "SELECT 1 FROM agents WHERE state NOT IN ('created', 'running', 'idle', 'paused', 'archived') LIMIT 1",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);
        if has_legacy_state {
            let _ = conn.execute(
                "UPDATE agents SET state = 'archived' WHERE state NOT IN ('created', 'running', 'idle', 'paused', 'archived')",
                [],
            );
        }
        let _ = ensure_lexical_fts_backfilled(&mut conn);
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
        let conn = self.conn();
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

    /// A receipt one named send already produced, or `None`.
    pub fn send_receipt(&self, id: &str) -> Result<Option<String>, Error> {
        let conn = self.conn();
        Ok(conn
            .query_row("SELECT receipt FROM sends WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .optional()?)
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

    pub fn create_agent(&self, new: NewAgent) -> Result<Agent, Error> {
        validate_token(&new.name, "agent name")?;
        if new.harness.is_empty() {
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
        let agent = Agent {
            is_admin: false,
            id: AgentId::generate(),
            name: new.name,
            role: clean_role,
            description: clean_description,
            harness: new.harness,
            model: new.model,
            workdir: new.workdir,
            instructions: new.instructions,
            state: "created".into(),
            created_at_ms: now_ms(),
            launch_env: Vec::new(),
            parent_id: new.parent_id,
        };
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
                id, name, harness, model, workdir, instructions, state, created_at_ms, parent_id, role, description
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'created', ?7, ?8, ?9, ?10)",
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
        agent_by_id(&self.conn(), id)
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
        let clean = harness.trim();
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

    /// Set or clear an agent's short role label (e.g. `"Spiron"`, `"Design"`, `"Revenue"`).
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

    /// Former names recorded for this agent via `rename_agent`.
    pub fn agent_aliases(&self, id: &AgentId) -> Result<Vec<String>, Error> {
        let conn = self.conn();
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
        let conn = self.conn();
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
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!("SELECT {AGENT_COLS} FROM agents ORDER BY name"))?;
        let mut rows = stmt.query([])?;
        let mut agents = Vec::new();
        while let Some(row) = rows.next()? {
            agents.push(agent_from_row(row)?);
        }
        Ok(agents)
    }

    pub fn children_of(&self, parent_id: &AgentId) -> Result<Vec<Agent>, Error> {
        let conn = self.conn();
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
        // A sender may name itself `alpha` or `agent:alpha`; both are the author.
        let author = sender
            .as_deref()
            .map(|value| value.strip_prefix("agent:").unwrap_or(value).to_string());
        let author_id = author
            .as_deref()
            .and_then(|a| self.agent_by_name(a).ok().map(|ag| ag.id));
        let members = self.room_members(room)?;
        let mut delivered = Vec::new();
        for agent in self.list_agents()? {
            if agent.state == "archived" || agent.state == "paused" {
                continue;
            }
            if author.as_deref() == Some(agent.name.as_str())
                || author.as_deref() == Some(agent.id.as_str())
                || author_id.as_ref() == Some(&agent.id)
            {
                continue;
            }
            if let Some(members) = &members
                && !members.iter().any(|name| name == &agent.name)
            {
                continue;
            }
            let mut one = Incoming {
                text: incoming.text.clone(),
                sender: sender.clone(),
                source_time_ms: incoming.source_time_ms,
                source_key: incoming
                    .source_key
                    .as_ref()
                    .map(|key| format!("room:{room}:{key}")),
                reply_to: incoming.reply_to.clone(),
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
        if name == "all" {
            let names = self
                .list_agents()?
                .into_iter()
                .filter(|agent| agent.state != "archived" && agent.state != "paused")
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
        let conn = self.conn();
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

    /// Append one message, or return the row already stored for this source key.
    /// The sequence is assigned inside the insert transaction.
    pub fn receive(&self, agent_id: &AgentId, incoming: Incoming) -> Result<ReceiveOutcome, Error> {
        let body = incoming.body();
        let source_key = incoming.source_key.filter(|key| !key.is_empty());
        let received_at_ms = incoming.received_at_ms.unwrap_or_else(now_ms);
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        agent_by_id(&tx, agent_id)?;
        if let Some(key) = &source_key
            && let Some(existing) = message_by_source(&tx, agent_id, key)?
        {
            return Ok(ReceiveOutcome::Duplicate(existing));
        }
        let mut body = body;
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
            )?
        {
            tx.commit()?;
            return Ok(ReceiveOutcome::Duplicate(existing));
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
        let participants = resolve_conversation_participants(
            &tx,
            chat_id_val,
            resolved_destination.as_deref(),
        );
        // The trusted header is built from the record that arrives, before any
        // field is moved into the row: the author, the channel, the chat and
        // topic, and the reply a bare answer reaches.
        let context = anyagents_harness::Context {
            sender: incoming.sender.clone(),
            sender_name: None,
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
        let is_passive_reaction =
            message.context.event.as_deref() == Some("reaction_passive");
        let woke_idle = if is_passive_reaction {
            false
        } else {
            tx.execute(
                "UPDATE agents SET state = 'running' WHERE id = ?1 AND state = 'idle'",
                [agent_id.as_str()],
            )? > 0
        };
        if let Ok(agent) = agent_by_id(&tx, agent_id) {
            let _ = index_rag_chunk(
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
            );
        }
        tx.commit()?;
        drop(conn);
        if woke_idle {
            self.notify(Wake::Agent {
                agent_id: agent_id.clone(),
            });
        }
        if !is_passive_reaction {
            self.notify(Wake::Mail {
                agent_id: agent_id.clone(),
            });
        }
        Ok(ReceiveOutcome::New(message))
    }

    /// Store the environment the agent's harness is started with, and wake it
    /// so the next start reads it. Never called with a provider key.
    pub fn set_agent_launch_env(
        &self,
        id: &AgentId,
        env: &[(String, String)],
    ) -> Result<Agent, Error> {
        let json = encode_env(env);
        let changed = {
            let conn = self.conn();
            // A database written before this column existed still reads: the
            // harness then keeps whatever sign-in it already has.
            let updated = conn
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
            agent_by_id(&conn, id)?
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

    /// Atomically transition a running sub-agent (`parent_id IS NOT NULL`) to
    /// `idle` (process released / scaled to zero) when it has delivered every
    /// message in its mailbox, completed its latest turn
    /// (`completed_seq >= opened_seq >= max_seq > 0`), and has no active
    /// (`running`) child sub-agents of its own.
    ///
    /// Returns `Ok(true)` when the sub-agent transitioned to `idle` (and
    /// notifies the supervisor to stop its worker slot), or `Ok(false)` if new
    /// mail arrived, a turn is still open, or a child sub-agent is still running.
    pub fn complete_idle_subagent(&self, id: &AgentId) -> Result<bool, Error> {
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
                     JOIN turn_recovery tr ON tr.agent_id = a.id
                     WHERE a.id = ?1
                       AND a.parent_id IS NOT NULL
                       AND a.state = 'running'
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
                   AND parent_id IS NOT NULL
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

    /// Record an outbound message sent by `agent_id` to `destination` (a user,
    /// a channel chat/topic, a group, or another agent) so the agent's full
    /// two-way conversation can be read by any peer agent.
    pub fn record_outbound(
        &self,
        agent_id: &AgentId,
        destination: &str,
        reply_to: Option<&str>,
        body: &str,
    ) -> Result<String, Error> {
        let conn = self.conn();
        let agent = agent_by_id(&conn, agent_id)?;
        let id = format!("out_{}", MessageId::generate());
        let ts_ms = now_ms();
        conn.execute(
            "INSERT INTO outbound_messages (id, agent_id, destination, reply_to, body, sent_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, agent_id.as_str(), destination, reply_to, body, ts_ms,],
        )?;
        let cur_seq: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM messages WHERE agent_id = ?1",
                [agent_id.as_str()],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let _ = index_rag_chunk(
            &conn,
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
        );
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
        let conn = self.conn();
        let agent = agent_by_id(&conn, agent_id)?;
        let id = format!("tool_{}", MessageId::generate());
        let ts_ms = now_ms();
        let clean_tool = tool_name.trim().to_ascii_lowercase();
        let text = if summary.trim().starts_with("[tool") {
            summary.trim().to_string()
        } else {
            format!("[tool:{clean_tool}] {}", summary.trim())
        };
        let cur_seq: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM messages WHERE agent_id = ?1",
                [agent_id.as_str()],
                |r| r.get(0),
            )
            .unwrap_or(0);
        index_rag_chunk(
            &conn,
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
        Ok(id)
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
        let conn = self.conn();
        let agent = agent_by_id(&conn, agent_id)?;
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
                "SELECT id, destination, reply_to, body, sent_at_ms
                 FROM outbound_messages WHERE agent_id = ?1 AND sent_at_ms > ?2
                 ORDER BY sent_at_ms DESC, id DESC LIMIT ?3"
            }
            (Some(_), false) => {
                "SELECT id, destination, reply_to, body, sent_at_ms
                 FROM outbound_messages WHERE agent_id = ?1 AND sent_at_ms > ?2
                 ORDER BY sent_at_ms ASC, id ASC LIMIT ?3"
            }
            (None, _) => {
                "SELECT id, destination, reply_to, body, sent_at_ms
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
        let conn = self.conn();
        let since = since_ms.unwrap_or(0);
        let lim = i64::try_from(limit.unwrap_or(50).max(1)).unwrap_or(50);
        let peer_filter = with_peer.map(str::trim).filter(|s| !s.is_empty());
        let fetch_limit = if peer_filter.is_some() {
            lim.saturating_mul(4)
        } else {
            lim
        };
        let sql = if since_ms.is_some() {
            "SELECT chunk_id, direction, sender, destination, text, timestamp_ms
             FROM rag_chunks
             WHERE timestamp_ms > ?1
             ORDER BY timestamp_ms ASC, chunk_id ASC
             LIMIT ?2"
        } else {
            "SELECT chunk_id, direction, sender, destination, text, timestamp_ms
             FROM rag_chunks
             WHERE timestamp_ms > ?1
             ORDER BY timestamp_ms DESC, chunk_id DESC
             LIMIT ?2"
        };
        let mut turns = Vec::new();
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params![since, fetch_limit])?;
        while let Some(row) = rows.next()? {
            turns.push(ConversationTurn {
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

    /// Zero-Embedding Lossless Lexical + Structured Agentic History Search (`Raw-RAG`).
    ///
    /// Searches `rag_chunks` and `rag_chunks_fts` across `req.agent` (`None` or `"all"` searches all agents)
    /// combining SQLite FTS5 BM25 (hybrid ASCII + CJK bigrams/unigrams), exact phrase/substring boost,
    /// multi-query coverage (`query` + `queries`), negative term filtering (`exclude`), tool history
    /// toggles (`include_tools`, `only_tools`, `tool_name`), range bounds (`since_ms`, `until_ms`,
    /// `from_seq`, `to_seq`), ordering (`Relevance`, `Newest`, `Oldest`), and contiguous episode
    /// context expansion (`radius`).
    pub fn search_history(&self, req: &HistorySearchQuery) -> Result<Vec<ConversationTurn>, Error> {
        let conn = self.read_conn();
        let limit = req.limit.clamp(1, 200);
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
            if !fts_query.is_empty()
                && let Ok(mut fstmt) = conn.prepare(
                    "SELECT c.chunk_id, c.direction, c.sender, c.destination, c.text, c.timestamp_ms,
                            c.agent_name, c.kind, c.tool_name, c.seq, f.score
                     FROM (
                         SELECT chunk_id, bm25(rag_chunks_fts, 0.5, 1.0, 1.0, 1.0, 0.5, 2.0, 3.0, 2.5) as score
                         FROM rag_chunks_fts
                         WHERE rag_chunks_fts MATCH ?1
                         ORDER BY score ASC
                         LIMIT ?4
                     ) f
                     JOIN rag_chunks c ON c.chunk_id = f.chunk_id
                     WHERE c.timestamp_ms > ?2 AND c.timestamp_ms <= ?3
                     ORDER BY f.score ASC",
                )
                && let Ok(mut frows) = fstmt.query(params![
                    fts_query,
                    since,
                    until,
                    i64::try_from((limit * 8).max(120)).unwrap_or(120)
                ])
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
                    let mut lstmt = conn.prepare(
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
                               AND (?8 IS NULL OR seq <= ?8)
                             ORDER BY timestamp_ms DESC
                             LIMIT 2000
                         )
                         WHERE text LIKE ?3 ESCAPE '\\'
                         ORDER BY timestamp_ms DESC
                         LIMIT ?4",
                    )?;
                    let mut lrows = lstmt.query(params![
                        since,
                        until,
                        like_pat,
                        i64::try_from((limit * 4).max(80)).unwrap_or(80),
                        agent_name_filter.as_deref(),
                        kind_sql_filter,
                        req.from_seq,
                        req.to_seq,
                    ])?;
                    let mut rank = 0usize;
                    while let Some(row) = lrows.next()? {
                        let cid: String = row.get(0)?;
                        let aname: String = row.get(6)?;
                        let kind: String = row.get(7)?;
                        let tool_name: String = row.get(8)?;
                        let seq: i64 = row.get(9)?;
                        let turn = ConversationTurn {
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
                   AND (?8 IS NULL OR seq <= ?8)
                 {order_sql}
                 LIMIT ?3"
            );
            let mut bstmt = conn.prepare(&sql)?;
            let mut brows = bstmt.query(params![
                since,
                until,
                i64::try_from((limit * 8).max(200)).unwrap_or(200),
                agent_name_filter.as_deref(),
                kind_sql_filter,
                dir_filter.as_deref(),
                req.from_seq,
                req.to_seq,
            ])?;
            let mut rank = 0usize;
            while let Some(row) = brows.next()? {
                let cid: String = row.get(0)?;
                let aname: String = row.get(6)?;
                let kind: String = row.get(7)?;
                let tool_name: String = row.get(8)?;
                let seq: i64 = row.get(9)?;
                let turn = ConversationTurn {
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
                let sig: String = turn.text.chars().take(180).collect();
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
        for (hit_turn, aname, hit_seq) in matched_hits {
            let hit_ts = hit_turn.timestamp_ms;
            let mut before: Vec<ConversationTurn> = Vec::new();
            if let Ok(mut bstmt) = conn.prepare(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, kind, seq
                 FROM rag_chunks
                 WHERE LOWER(agent_name) = LOWER(?1)
                   AND (timestamp_ms < ?2 OR (timestamp_ms = ?2 AND chunk_id != ?3))
                 ORDER BY timestamp_ms DESC, seq DESC
                 LIMIT ?4",
            ) && let Ok(mut brows) =
                bstmt.query(params![aname, hit_ts, hit_turn.id, rad_i64 * 2])
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
                    before.push(ConversationTurn {
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
                    });
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
            if let Ok(mut astmt) = conn.prepare(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, kind, seq
                 FROM rag_chunks
                 WHERE LOWER(agent_name) = LOWER(?1)
                   AND timestamp_ms >= ?2
                   AND chunk_id != ?3
                 ORDER BY timestamp_ms ASC, seq ASC
                 LIMIT ?4",
            ) && let Ok(mut arows) = astmt.query(params![aname, hit_ts, hit_id, rad_i64 * 2])
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
                    let _ = hit_seq;
                    let turn = ConversationTurn {
                        id: r.get(0)?,
                        direction: r.get(1)?,
                        from: r.get(2)?,
                        to: r.get(3)?,
                        reply_to: Some(format!("context:after={hit_id}:agent={aname}:seq={seq}")),
                        text: r.get(4)?,
                        timestamp_ms: r.get(5)?,
                    };
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
        let conn = self.conn();
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

    /// Mark the agent's current open turn as completed (`completed_seq = opened_seq`).
    pub fn note_turn_completed(&self, agent_id: &AgentId) -> Result<(), Error> {
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        conn.execute(
            "UPDATE turn_recovery
             SET completed_seq = opened_seq,
                 recovery_count = 0
             WHERE agent_id = ?1 AND completed_seq < opened_seq",
            [agent_id.as_str()],
        )?;
        Ok(())
    }

    /// Record that one `[Recovery]` wake was delivered for `opened_seq`.
    pub fn note_turn_recovered(&self, agent_id: &AgentId, opened_seq: i64) -> Result<(), Error> {
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        conn.execute(
            "UPDATE turn_recovery
             SET recovery_count = recovery_count + 1
             WHERE agent_id = ?1 AND opened_seq = ?2",
            params![agent_id.as_str(), opened_seq],
        )?;
        Ok(())
    }

    /// Return `true` when the agent currently has an open turn (`opened_seq > completed_seq`).
    pub fn has_open_turn(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let conn = self.conn();
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
        let conn = self.conn();
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
        let conn = self.conn();
        agent_by_id(&conn, &record.agent_id)?;
        conn.execute(
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
        let conn = self.conn();
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

    fn conn(&self) -> MutexGuard<'_, Connection> {
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
            let _ = c.busy_timeout(Duration::from_secs(5));
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
        let conn = self.conn();
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
        let conn = self.conn();
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

    pub fn restore_skipped_at_schedules(&self, min_next_fire_at_ms: i64) -> Result<usize, Error> {
        let conn = self.conn();
        let updated = conn.execute(
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

/// `{data}/agents/{id}/work`.
pub fn agent_work_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
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
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let Ok(conn) = Connection::open_with_flags(&db_path, flags) else {
        return Vec::new();
    };
    let _ = conn.busy_timeout(Duration::from_millis(500));
    let query_with_desc = "SELECT name, role, description, instructions
         FROM agents
         WHERE parent_id IS NULL AND state = 'running'
         ORDER BY name ASC";
    let mut roster = Vec::new();
    if let Ok(mut stmt) = conn.prepare(query_with_desc) {
        if let Ok(rows) = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let role: Option<String> = row.get(1)?;
            let description: Option<String> = row.get(2)?;
            let instructions: Option<String> = row.get(3)?;
            Ok((name, role, description, instructions))
        }) {
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
    let out: String = t.chars().take(2400).collect();
    out
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

fn ensure_lexical_fts_backfilled(conn: &mut Connection) -> Result<(), Error> {
    let has_chunks: i64 = conn
        .query_row("SELECT EXISTS(SELECT 1 FROM rag_chunks LIMIT 1)", [], |r| {
            r.get(0)
        })
        .unwrap_or(0);
    if has_chunks == 0 {
        return Ok(());
    }
    let has_fts: i64 = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM rag_chunks_fts_docsize LIMIT 1)",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if has_fts != 0 {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    {
        tx.execute("DELETE FROM rag_chunks_fts", [])?;
        let mut sel = tx.prepare(
            "SELECT chunk_id, agent_name, sender, destination, kind, tool_name, text
             FROM rag_chunks",
        )?;
        let rows: Vec<(String, String, String, String, String, String, String)> = sel
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                ))
            })?
            .filter_map(Result::ok)
            .collect();
        let mut ins = tx.prepare(
            "INSERT INTO rag_chunks_fts (
                chunk_id, agent_name, sender, destination, kind, tool_name, text, search_tokens
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for (cid, aname, sender, dest, kind, tool_name, text) in rows {
            let search_tokens = tokenize_hybrid_search_tokens(&text).join(" ");
            let _ = ins.execute(params![
                cid,
                aname,
                sender,
                dest,
                kind,
                tool_name,
                text,
                search_tokens
            ]);
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
            timestamp_ms, content_hash, text, kind, tool_name, seq
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
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
        let _ = conn.execute(
            "UPDATE rag_chunks SET seq = rowid WHERE chunk_id = ?1 AND seq <= 0",
            params![chunk_id],
        );
    }
    let _ = conn.execute(
        "DELETE FROM rag_chunks_fts WHERE chunk_id MATCH ?1",
        params![format!("\"{}\"", chunk_id.replace('"', ""))],
    );
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
        let stored_is_meta_event = msg.text.starts_with("[Reaction ")
            || msg.text.starts_with("[Deleted message #");
        if origin.message_id == Some(message_id)
            && (!is_basic_group || (!in_is_relay && !stored_is_relay))
        {
            if incoming_is_meta_event || stored_is_meta_event {
                if strip_quoted_block(&msg.text) == strip_quoted_block(incoming_body)
                    && in_user == telegram_sender_user_key(msg.sender.as_deref())
                {
                    return Ok(Some(msg));
                }
            } else {
                return Ok(Some(msg));
            }
        }
        // Basic Telegram groups (`chat_type == "group"`, negative non-`-100...` IDs)
        // assign each bot its own account-local `message_id` for the same human
        // message. Match by `(chat_id, sender user, source_time_ms, body)` so cross-bot
        // passive relay and direct webhook delivery coalesce into a single message.
        if (in_is_relay || stored_is_relay)
            && normalize_telegram_body_for_dedup(strip_quoted_block(&msg.text))
                == normalize_telegram_body_for_dedup(strip_quoted_block(incoming_body))
            && in_user.is_some()
            && in_user == telegram_sender_user_key(msg.sender.as_deref())
            && match (msg.source_time_ms, incoming_source_time_ms) {
                (Some(a), Some(b)) => (a - b).abs() <= 5_000,
                _ => (now_ms() - msg.received_at_ms).abs() <= 15_000,
            }
        {
            let best_body = if incoming_body.len() > msg.text.len() {
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
                let _ = conn.execute(
                    "UPDATE messages SET origin = ?1, context = ?2, source_key = NULL, body = ?3 WHERE id = ?4",
                    params![
                        serde_json::to_string(&updated_origin).unwrap_or_default(),
                        serde_json::to_string(&updated_context).ok(),
                        new_body,
                        msg.id.as_str(),
                    ],
                );
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
                let _ = conn.execute(
                    "UPDATE messages SET body = ?1, context = ?2 WHERE id = ?3",
                    params![
                        new_body,
                        serde_json::to_string(&updated_context).ok(),
                        msg.id.as_str(),
                    ],
                );
                msg.context = updated_context;
                if let Some(b) = best_body {
                    msg.text = b;
                }
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
            if chats
                .iter()
                .any(|link| link.get("chat_id").and_then(serde_json::Value::as_i64) == Some(chat_id))
            {
                distinct_agents.insert(aid);
            }
        }
    }
    let pattern = format!("%\"chat_id\":{chat_id}%");
    if let Ok(mut stmt) = conn.prepare(
        "SELECT DISTINCT agent_id FROM messages WHERE origin LIKE ?1 LIMIT 16",
    ) && let Ok(mut rows) = stmt.query([pattern])
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
                if chats
                    .iter()
                    .any(|link| link.get("chat_id").and_then(serde_json::Value::as_i64) == Some(cid))
                {
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
        } else if let Ok(mut stmt) = conn.prepare(
            "SELECT agent_id FROM group_members WHERE group_name = ?1",
        ) && let Ok(mut rows) = stmt.query([group_name])
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
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, String>(2)?)),
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
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        (dir, store)
    }

    fn agent(store: &Store, name: &str) -> Agent {
        store
            .create_agent(NewAgent::new(name, "claude-code"))
            .unwrap()
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
        assert_eq!(store.agent(&alpha.id).unwrap().launch_env, env);

        // A value with a quote, a backslash and a newline keeps its characters.
        let awkward = vec![("K".to_string(), "a\"b\\c\nd".to_string())];
        assert_eq!(decode_env(&encode_env(&awkward)), awkward);
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
        let dir = tempfile::tempdir().unwrap();
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
                 VALUES ('agt_01h455vb4pex5vsknk084sn02q', 'juno', 'opencode', 'running', 1);
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
        let agent = store.agent_by_name("juno").unwrap();
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
    fn rename_agent_preserves_id_grants_history_and_resolves_old_alias_and_role() {
        let (_dir, store) = setup();
        let argon = agent(&store, "argon");
        store.set_agent_admin(&argon.id, true).unwrap();
        store.join_group("owners", &argon.id).unwrap();
        store
            .receive(&argon.id, Incoming::new("revenue check"))
            .unwrap();

        let renamed = store.rename_agent(&argon.id, "plutus").unwrap();
        assert_eq!(renamed.id, argon.id);
        assert_eq!(renamed.name, "plutus");
        assert!(renamed.is_admin);

        let with_role = store.set_agent_role(&argon.id, Some("Revenue")).unwrap();
        assert_eq!(with_role.role.as_deref(), Some("Revenue"));

        // Old name resolves via alias table
        let by_old = store.agent_by_name("argon").unwrap();
        assert_eq!(by_old.id, argon.id);
        assert_eq!(by_old.name, "plutus");
        assert_eq!(by_old.role.as_deref(), Some("Revenue"));

        let by_prefixed = store.agent_by_name("agent:argon").unwrap();
        assert_eq!(by_prefixed.id, argon.id);

        assert_eq!(
            store.agent_aliases(&argon.id).unwrap(),
            vec!["argon".to_string()]
        );
        assert_eq!(
            store.group_members("owners").unwrap(),
            vec!["plutus".to_string()]
        );
        assert_eq!(store.inbox(&argon.id, None).unwrap().len(), 1);
    }

    #[test]
    fn telegram_reaction_is_not_dropped_by_origin_dedup_and_wakes_targeted_author_only() {
        let (_dir, store) = setup();
        let orion = agent(&store, "orion");
        let nova = agent(&store, "nova");

        // 1. Orion posts message #42 in group -100999, which is recorded in both bots' mailboxes.
        let mut msg42 = Incoming::new("Here is the HKMT economy simulation report.");
        msg42.sender = Some("agent:orion".to_string());
        msg42.origin = Some(Origin {
            chat_id: Some(-100999),
            thread_id: None,
            message_id: Some(42),
        });
        store.receive(&orion.id, msg42.clone()).unwrap();
        store.receive(&nova.id, msg42).unwrap();
        store.note_delivered(&orion.id, 1).unwrap();
        store.note_delivered(&nova.id, 1).unwrap();
        store.set_agent_state(&orion.id, "idle").unwrap();
        store.set_agent_state(&nova.id, "idle").unwrap();

        // 2. Kyle reacts 👍 on message #42. Neither bot drops it as a duplicate of #42!
        //    Orion (the author of #42) receives `event = "reaction"` and wakes to `running`.
        //    Nova (a peer bot in the same multi-bot group) receives `event = "reaction_passive"`
        //    in full awareness without waking from `idle` on the reaction alone.
        let mut react = Incoming::new("[Reaction 👍 on message #42]");
        react.sender = Some("telegram:user:777:kyle".to_string());
        react.origin = Some(Origin {
            chat_id: Some(-100999),
            thread_id: None,
            message_id: Some(42),
        });

        let orion_out = store.receive(&orion.id, react.clone()).unwrap();
        let ReceiveOutcome::New(orion_msg) = orion_out else {
            panic!("reaction must not be dropped as duplicate of original message #42");
        };
        assert_eq!(orion_msg.context.event.as_deref(), Some("reaction"));
        assert_eq!(
            orion_msg.context.reply_to_author.as_deref(),
            Some("agent:orion")
        );
        assert!(
            orion_msg
                .text
                .contains("Here is the HKMT economy simulation report.")
        );
        assert_eq!(store.agent(&orion.id).unwrap().state, "running");
        assert!(store.has_actionable_undelivered(&orion.id).unwrap());

        let nova_out = store.receive(&nova.id, react).unwrap();
        let ReceiveOutcome::New(nova_msg) = nova_out else {
            panic!("peer bot must also record reaction for full awareness");
        };
        assert_eq!(nova_msg.context.event.as_deref(), Some("reaction_passive"));
        assert_eq!(store.agent(&nova.id).unwrap().state, "idle");
        assert!(!store.has_actionable_undelivered(&nova.id).unwrap());
    }
}
