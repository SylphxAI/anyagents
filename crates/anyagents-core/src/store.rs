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
     created_at_ms, launch_env, parent_id, is_admin";

/// One agent row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Agent {
    pub is_admin: bool,
    pub id: AgentId,
    pub name: String,
    pub harness: String,
    pub model: Option<String>,
    pub workdir: Option<String>,
    pub instructions: Option<String>,
    pub state: String,
    pub created_at_ms: i64,
    /// The gateway origin and this agent's own gateway token, set at create.
    /// Never a provider key.
    pub launch_env: Vec<(String, String)>,
    /// The parent agent that spawned this agent, when it is a child/sub-agent.
    pub parent_id: Option<AgentId>,
}

/// Fields for `agents.create`. The harness value is a catalog key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewAgent {
    pub name: String,
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
            harness: harness.into(),
            model: None,
            workdir: None,
            instructions: None,
            parent_id: None,
        }
    }
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

/// Scoped external result subscription binding one recipient agent to a signed
/// CloudEvents / Standard Webhooks publisher source and optional subject (`promise_id` / `job_id`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResultSubscription {
    pub id: String,
    pub recipient_id: AgentId,
    pub recipient: String,
    pub source: String,
    pub subject: Option<String>,
    pub org_id: Option<String>,
    pub event_types: Vec<String>,
    pub secret_fingerprint: String,
    pub created_by: String,
    pub enabled: bool,
    pub registration_status: String,
    pub after_seq: i64,
    pub last_seq: i64,
    pub delivery_count: i64,
    pub latest_delivery_stage: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Parameters to register or update a scoped external result subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewResultSubscription {
    pub recipient_id: AgentId,
    pub source: String,
    pub subject: Option<String>,
    pub org_id: Option<String>,
    pub event_types: Vec<String>,
    pub secret: Option<String>,
    pub previous_secret: Option<String>,
    pub created_by: String,
    pub after_seq: i64,
}

/// Durable receipt and live delivery stage for an accepted external result webhook.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResultDeliveryView {
    pub id: String,
    pub subscription_id: String,
    pub recipient_id: AgentId,
    pub recipient: String,
    pub recipient_state: String,
    pub woke_from_state: String,
    pub webhook_id: String,
    pub webhook_timestamp: i64,
    pub event_id: String,
    pub event_type: String,
    pub source: String,
    pub subject: Option<String>,
    pub org_id: Option<String>,
    pub seq: Option<i64>,
    pub message_id: String,
    pub message_seq: i64,
    pub delivered_seq: i64,
    pub completed_turn_seq: i64,
    pub registration_status: String,
    pub event_status: String,
    pub delivery_stage: String,
    pub duplicate: bool,
    pub received_at_ms: i64,
    pub delivered_at_ms: Option<i64>,
}

/// Outcome of `Store::receive_result_webhook`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultWebhookOutcome {
    Accepted(ResultDeliveryView),
    Duplicate(ResultDeliveryView),
}

/// Fail-closed refusal reasons for `Store::receive_result_webhook`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultWebhookError {
    BadRequest(String),
    Unauthorized(String),
    Forbidden(String),
    NotFound(String),
    Store(String),
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
    data_dir: PathBuf,
    wakes: Mutex<Vec<Sender<Wake>>>,
    capabilities: Mutex<Option<String>>,
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
        Ok(Self {
            conn: Mutex::new(conn),
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
            let tx = conn.transaction()?;
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
        let agent = Agent {
            is_admin: false,
            id: AgentId::generate(),
            name: new.name,
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
                id, name, harness, model, workdir, instructions, state, created_at_ms, parent_id
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'created', ?7, ?8)",
            params![
                agent.id.as_str(),
                agent.name,
                agent.harness,
                agent.model,
                agent.workdir,
                agent.instructions,
                agent.created_at_ms,
                agent.parent_id.as_ref().map(|id| id.as_str()),
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
        Ok(())
    }

    pub fn set_agent_harness(&self, agent_id: &AgentId, harness: &str) -> Result<Agent, Error> {
        let clean = harness.trim();
        if clean.is_empty() {
            return Err(Error::Message("harness is empty".into()));
        }
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        conn.execute(
            "UPDATE agents SET harness = ?2 WHERE id = ?1",
            params![agent_id.as_str(), clean],
        )?;
        agent_by_id(&conn, agent_id)
    }

    /// Update an agent's `parent_id` in place (`None` promotes the agent to a
    /// standalone root agent) while preserving its identity, state, authority
    /// (`is_admin`), message history, cursors, and turn recovery.
    pub fn set_agent_parent(
        &self,
        agent_id: &AgentId,
        parent_id: Option<&AgentId>,
    ) -> Result<Agent, Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = agent_by_id(&tx, agent_id)?;
        let mut new_parent_name: Option<String> = None;
        if let Some(pid) = parent_id {
            if pid == agent_id {
                return Err(Error::Message(
                    "an agent cannot be its own parent".into(),
                ));
            }
            if existing.is_admin {
                return Err(Error::Message(
                    "an admin agent cannot become a subagent; revoke admin authority first".into(),
                ));
            }
            let target_parent = agent_by_id(&tx, pid)?;
            new_parent_name = Some(target_parent.name.clone());
            let mut cursor = target_parent.parent_id;
            let mut hops = 0usize;
            while let Some(anc_id) = cursor {
                if &anc_id == agent_id {
                    return Err(Error::Message(
                        "cannot set parent: cycle detected".into(),
                    ));
                }
                if hops >= 64 {
                    return Err(Error::Message(
                        "cannot set parent: hierarchy depth exceeded".into(),
                    ));
                }
                hops += 1;
                let anc = agent_by_id(&tx, &anc_id)?;
                cursor = anc.parent_id;
            }
        }
        let mut env = existing.launch_env.clone();
        if let Some(pname) = new_parent_name {
            if let Some((_, val)) = env.iter_mut().find(|(k, _)| k == "ANYAGENTS_PARENT_NAME") {
                *val = pname;
            } else {
                env.push(("ANYAGENTS_PARENT_NAME".to_string(), pname));
            }
        } else {
            env.retain(|(k, _)| k != "ANYAGENTS_PARENT_NAME");
        }
        let env_json = encode_env(&env);
        tx.execute(
            "UPDATE agents SET parent_id = ?1, launch_env = ?2 WHERE id = ?3",
            params![
                parent_id.map(AgentId::as_str),
                env_json,
                agent_id.as_str()
            ],
        )?;
        let updated = agent_by_id(&tx, agent_id)?;
        tx.commit()?;
        Ok(updated)
    }

    /// Register or idempotently update a scoped external result subscription.
    /// Returns `(subscription_view, generated_or_supplied_secret_on_change, created_or_rotated)`.
    pub fn register_result_subscription(
        &self,
        new: NewResultSubscription,
    ) -> Result<(ResultSubscription, Option<String>, bool), Error> {
        let source = new.source.trim().to_string();
        if source.is_empty() {
            return Err(Error::Message("subscription source is empty".into()));
        }
        let subject = new
            .subject
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let org_id = new
            .org_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let mut event_types: Vec<String> = new
            .event_types
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        event_types.sort();
        event_types.dedup();
        let event_types_json = serde_json::to_string(&event_types).unwrap_or_else(|_| "[]".into());
        let explicit_secret = match new.secret.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(raw) => {
                let decoded = decode_standard_webhooks_secret(raw)?;
                if decoded.len() < 16 {
                    return Err(Error::Message(
                        "subscription secret must decode to at least 16 bytes".into(),
                    ));
                }
                Some(if raw.starts_with("whsec_") {
                    raw.to_string()
                } else {
                    format!("whsec_{}", base64_encode(&decoded))
                })
            }
            None => None,
        };
        let explicit_prev_secret = match new
            .previous_secret
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(raw) => {
                let decoded = decode_standard_webhooks_secret(raw)?;
                if decoded.len() < 16 {
                    return Err(Error::Message(
                        "previous_secret must decode to at least 16 bytes".into(),
                    ));
                }
                Some(if raw.starts_with("whsec_") {
                    raw.to_string()
                } else {
                    format!("whsec_{}", base64_encode(&decoded))
                })
            }
            None => None,
        };

        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let _recipient = agent_by_id(&tx, &new.recipient_id)?;
        let now = now_ms();

        let existing_row: Option<(String, String, Option<String>, i64)> = tx
            .query_row(
                "SELECT id, secret, previous_secret, after_seq
                 FROM result_subscriptions
                 WHERE recipient_id = ?1 AND source = ?2 AND COALESCE(subject, '') = COALESCE(?3, '')
                 ORDER BY created_at_ms DESC LIMIT 1",
                params![new.recipient_id.as_str(), source, subject],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        let (sub_id, returned_secret, changed) = if let Some((
            existing_id,
            existing_secret,
            existing_prev,
            existing_after_seq,
        )) = existing_row
        {
            let (next_secret, next_prev, secret_changed, secret_out) = match explicit_secret {
                Some(ref s) if s != &existing_secret => (
                    s.clone(),
                    explicit_prev_secret.or(Some(existing_secret)),
                    true,
                    Some(s.clone()),
                ),
                Some(ref s) => (
                    s.clone(),
                    explicit_prev_secret.or(existing_prev),
                    false,
                    None,
                ),
                None => (
                    existing_secret,
                    explicit_prev_secret.or(existing_prev),
                    false,
                    None,
                ),
            };
            let next_after = new.after_seq.max(existing_after_seq);
            tx.execute(
                "UPDATE result_subscriptions
                 SET org_id = COALESCE(?1, org_id),
                     event_types = ?2,
                     secret = ?3,
                     previous_secret = ?4,
                     enabled = 1,
                     after_seq = ?5,
                     updated_at_ms = ?6
                 WHERE id = ?7",
                params![
                    org_id,
                    event_types_json,
                    next_secret,
                    next_prev,
                    next_after,
                    now,
                    existing_id
                ],
            )?;
            (existing_id, secret_out, secret_changed)
        } else {
            let sub_id = format!("rsub_{}", uuid::Uuid::now_v7().simple());
            let secret = match explicit_secret {
                Some(s) => s,
                None => generate_standard_webhooks_secret()?,
            };
            tx.execute(
                "INSERT INTO result_subscriptions (
                    id, recipient_id, source, subject, org_id, event_types,
                    secret, previous_secret, created_by, enabled, after_seq, last_seq,
                    created_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, 0, ?11, ?11)",
                params![
                    sub_id,
                    new.recipient_id.as_str(),
                    source,
                    subject,
                    org_id,
                    event_types_json,
                    secret,
                    explicit_prev_secret,
                    new.created_by,
                    new.after_seq.max(0),
                    now,
                ],
            )?;
            (sub_id, Some(secret), true)
        };

        let view = subscription_view_by_id(&tx, &sub_id)?;
        tx.commit()?;
        Ok((view, returned_secret, changed))
    }

    pub fn list_result_subscriptions(
        &self,
        recipient_id: Option<&AgentId>,
    ) -> Result<Vec<ResultSubscription>, Error> {
        let conn = self.conn();
        let mut ids: Vec<String> = Vec::new();
        if let Some(rid) = recipient_id {
            let mut stmt = conn.prepare(
                "SELECT id FROM result_subscriptions WHERE recipient_id = ?1 ORDER BY created_at_ms, id",
            )?;
            let mut rows = stmt.query([rid.as_str()])?;
            while let Some(row) = rows.next()? {
                ids.push(row.get(0)?);
            }
        } else {
            let mut stmt =
                conn.prepare("SELECT id FROM result_subscriptions ORDER BY created_at_ms, id")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                ids.push(row.get(0)?);
            }
        }
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(subscription_view_by_id(&conn, &id)?);
        }
        Ok(out)
    }

    pub fn result_subscription(&self, id: &str) -> Result<ResultSubscription, Error> {
        let conn = self.conn();
        subscription_view_by_id(&conn, id)
    }

    pub fn delete_result_subscription(&self, id: &str) -> Result<ResultSubscription, Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = subscription_view_by_id(&tx, id)?;
        tx.execute(
            "UPDATE result_subscriptions SET enabled = 0, updated_at_ms = ?1 WHERE id = ?2",
            params![now_ms(), id],
        )?;
        let updated = subscription_view_by_id(&tx, id)?;
        tx.commit()?;
        let _ = existing;
        Ok(updated)
    }

    pub fn list_result_deliveries(
        &self,
        recipient_id: Option<&AgentId>,
        subscription_id: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<ResultDeliveryView>, Error> {
        let conn = self.conn();
        let lim = i64::try_from(limit.unwrap_or(100).max(1)).unwrap_or(100);
        let mut ids: Vec<String> = Vec::new();
        match (recipient_id, subscription_id) {
            (Some(rid), Some(sid)) => {
                let mut stmt = conn.prepare(
                    "SELECT id FROM result_deliveries
                     WHERE recipient_id = ?1 AND subscription_id = ?2
                     ORDER BY received_at_ms DESC, id DESC LIMIT ?3",
                )?;
                let mut rows = stmt.query(params![rid.as_str(), sid, lim])?;
                while let Some(row) = rows.next()? {
                    ids.push(row.get(0)?);
                }
            }
            (Some(rid), None) => {
                let mut stmt = conn.prepare(
                    "SELECT id FROM result_deliveries
                     WHERE recipient_id = ?1
                     ORDER BY received_at_ms DESC, id DESC LIMIT ?2",
                )?;
                let mut rows = stmt.query(params![rid.as_str(), lim])?;
                while let Some(row) = rows.next()? {
                    ids.push(row.get(0)?);
                }
            }
            (None, Some(sid)) => {
                let mut stmt = conn.prepare(
                    "SELECT id FROM result_deliveries
                     WHERE subscription_id = ?1
                     ORDER BY received_at_ms DESC, id DESC LIMIT ?2",
                )?;
                let mut rows = stmt.query(params![sid, lim])?;
                while let Some(row) = rows.next()? {
                    ids.push(row.get(0)?);
                }
            }
            (None, None) => {
                let mut stmt = conn.prepare(
                    "SELECT id FROM result_deliveries
                     ORDER BY received_at_ms DESC, id DESC LIMIT ?1",
                )?;
                let mut rows = stmt.query(params![lim])?;
                while let Some(row) = rows.next()? {
                    ids.push(row.get(0)?);
                }
            }
        }
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(delivery_view_by_id(&conn, &id, false)?);
        }
        Ok(out)
    }

    /// Verify a signed Standard Webhooks + CloudEvents 1.0 external result event,
    /// enforce subscription scope and replay/timestamp fences, durably persist
    /// the receipt + recipient mailbox message in one SQLite WAL transaction,
    /// and wake the recipient (transitioning scale-to-zero `archived`/`created`
    /// recipients to `running` while holding `paused` recipients closed).
    pub fn receive_result_webhook(
        &self,
        subscription_id_hint: Option<&str>,
        webhook_id_raw: Option<&str>,
        webhook_timestamp_raw: Option<&str>,
        webhook_signature_raw: Option<&str>,
        raw_body: &[u8],
    ) -> Result<ResultWebhookOutcome, ResultWebhookError> {
        let webhook_id = webhook_id_raw
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ResultWebhookError::BadRequest("missing webhook-id header".into())
            })?;
        if webhook_id.contains('.') {
            return Err(ResultWebhookError::BadRequest(
                "invalid webhook-id header: must not contain '.'".into(),
            ));
        }
        let timestamp_str = webhook_timestamp_raw
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ResultWebhookError::BadRequest("missing webhook-timestamp header".into())
            })?;
        let webhook_timestamp: i64 = timestamp_str.parse().map_err(|_| {
            ResultWebhookError::BadRequest("webhook-timestamp is not a valid unix integer".into())
        })?;
        let now_secs = now_ms() / 1000;
        if (now_secs - webhook_timestamp).abs() > STANDARD_WEBHOOKS_TOLERANCE_SECS {
            return Err(ResultWebhookError::Unauthorized(
                "webhook-timestamp is outside the 300s replay tolerance window".into(),
            ));
        }
        let signature_header = webhook_signature_raw
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ResultWebhookError::Unauthorized("missing webhook-signature header".into())
            })?;
        if !signature_header
            .split_ascii_whitespace()
            .any(|part| part.starts_with("v1,") && part.len() > 3)
        {
            return Err(ResultWebhookError::Unauthorized(
                "webhook-signature contains no v1 signature".into(),
            ));
        }
        let body_str = std::str::from_utf8(raw_body).map_err(|_| {
            ResultWebhookError::BadRequest("webhook body is not valid UTF-8".into())
        })?;
        let payload: serde_json::Value = serde_json::from_str(body_str).map_err(|_| {
            ResultWebhookError::BadRequest("webhook body is not valid JSON".into())
        })?;
        if let Some(spec) = payload.get("specversion").and_then(serde_json::Value::as_str)
            && spec != "1.0"
        {
            return Err(ResultWebhookError::BadRequest(format!(
                "unsupported CloudEvents specversion `{spec}`"
            )));
        }
        let event_id = payload
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ResultWebhookError::BadRequest("CloudEvents `id` is required".into())
            })?
            .to_string();
        let event_type = payload
            .get("type")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ResultWebhookError::BadRequest("CloudEvents `type` is required".into())
            })?
            .to_string();
        let event_source = payload
            .get("source")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ResultWebhookError::BadRequest("CloudEvents `source` is required".into())
            })?
            .to_string();
        let event_subject = payload
            .get("subject")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                payload
                    .pointer("/data/promise_id")
                    .and_then(serde_json::Value::as_str)
            })
            .or_else(|| {
                payload
                    .pointer("/data/job_id")
                    .and_then(serde_json::Value::as_str)
            })
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let event_org_id = payload
            .pointer("/data/org_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let event_seq = payload
            .pointer("/data/seq")
            .and_then(serde_json::Value::as_i64);

        let (outcome, wake_agent_id, woke_from_archived) = {
            let mut conn = self.conn();
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?;

            #[allow(clippy::type_complexity)]
            let mut candidates: Vec<(
                String,
                AgentId,
                String,
                Option<String>,
                Option<String>,
                Vec<String>,
                String,
                Option<String>,
                bool,
                i64,
            )> = Vec::new();

            let load_row = |row: &rusqlite::Row<'_>| -> Result<_, rusqlite::Error> {
                let id: String = row.get(0)?;
                let rid_raw: String = row.get(1)?;
                let rid = AgentId::parse(&rid_raw).map_err(|err| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                })?;
                let src: String = row.get(2)?;
                let subj: Option<String> = row.get(3)?;
                let oid: Option<String> = row.get(4)?;
                let et_raw: String = row.get(5)?;
                let et: Vec<String> = serde_json::from_str(&et_raw).unwrap_or_default();
                let sec: String = row.get(6)?;
                let prev_sec: Option<String> = row.get(7)?;
                let en: i64 = row.get(8)?;
                let aseq: i64 = row.get(9)?;
                Ok((id, rid, src, subj, oid, et, sec, prev_sec, en != 0, aseq))
            };

            if let Some(hint) = subscription_id_hint.map(str::trim).filter(|s| !s.is_empty()) {
                let found = tx
                    .query_row(
                        "SELECT id, recipient_id, source, subject, org_id, event_types,
                                secret, previous_secret, enabled, after_seq
                         FROM result_subscriptions WHERE id = ?1",
                        [hint],
                        load_row,
                    )
                    .optional()
                    .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
                let Some(row) = found else {
                    return Err(ResultWebhookError::NotFound(format!(
                        "result subscription `{hint}` not found"
                    )));
                };
                if !row.8 {
                    return Err(ResultWebhookError::Forbidden(format!(
                        "result subscription `{hint}` is disabled"
                    )));
                }
                candidates.push(row);
            } else {
                let mut stmt = tx
                    .prepare(
                        "SELECT id, recipient_id, source, subject, org_id, event_types,
                                secret, previous_secret, enabled, after_seq
                         FROM result_subscriptions
                         WHERE enabled = 1
                         ORDER BY created_at_ms DESC",
                    )
                    .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
                let mut rows = stmt
                    .query([])
                    .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
                while let Some(row) = rows
                    .next()
                    .map_err(|e| ResultWebhookError::Store(e.to_string()))?
                {
                    candidates.push(
                        load_row(row).map_err(|e| ResultWebhookError::Store(e.to_string()))?,
                    );
                }
                if candidates.is_empty() {
                    return Err(ResultWebhookError::Forbidden(
                        "no active result subscriptions registered".into(),
                    ));
                }
            }

            let mut sig_verified_candidates = Vec::new();
            for cand in candidates {
                let primary_ok = standard_webhooks_verify(
                    &cand.6,
                    webhook_id,
                    timestamp_str,
                    raw_body,
                    signature_header,
                );
                let prev_ok = cand.7.as_deref().is_some_and(|prev| {
                    standard_webhooks_verify(
                        prev,
                        webhook_id,
                        timestamp_str,
                        raw_body,
                        signature_header,
                    )
                });
                if primary_ok || prev_ok {
                    sig_verified_candidates.push(cand);
                }
            }
            if sig_verified_candidates.is_empty() {
                return Err(ResultWebhookError::Unauthorized(
                    "Standard Webhooks signature verification failed".into(),
                ));
            }

            let matched = sig_verified_candidates.into_iter().find(|cand| {
                let source_ok = cand.2 == "*" || cand.2 == event_source;
                let subject_ok = match (&cand.3, &event_subject) {
                    (None, _) => true,
                    (Some(expected), Some(actual)) => expected == actual,
                    (Some(_), None) => false,
                };
                let org_ok = match (&cand.4, &event_org_id) {
                    (None, _) => true,
                    (Some(expected), Some(actual)) => expected == actual,
                    (Some(_), None) => false,
                };
                let type_ok = cand.5.is_empty()
                    || cand.5.iter().any(|allowed| {
                        allowed == "*"
                            || allowed == &event_type
                            || (allowed.ends_with(".*")
                                && event_type.starts_with(&allowed[..allowed.len() - 1]))
                    });
                source_ok && subject_ok && org_ok && type_ok
            });

            let Some((
                sub_id,
                recipient_id,
                _sub_source,
                _sub_subject,
                _sub_org,
                _sub_types,
                _sec,
                _prev,
                _en,
                after_seq,
            )) = matched
            else {
                return Err(ResultWebhookError::Forbidden(
                    "event source, subject, org_id, or type is outside the subscription's granted scope".into(),
                ));
            };

            let existing_delivery_id: Option<String> = tx
                .query_row(
                    "SELECT id FROM result_deliveries
                     WHERE subscription_id = ?1 AND (webhook_id = ?2 OR event_id = ?3)
                     LIMIT 1",
                    params![sub_id, webhook_id, event_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?;

            if let Some(del_id) = existing_delivery_id {
                let view = delivery_view_by_id(&tx, &del_id, true)
                    .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
                tx.commit()
                    .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
                return Ok(ResultWebhookOutcome::Duplicate(view));
            }

            if let Some(s) = event_seq
                && s <= after_seq
            {
                return Err(ResultWebhookError::Forbidden(format!(
                    "event seq {s} is at or before subscription after_seq fence ({after_seq})"
                )));
            }

            let agent = agent_by_id(&tx, &recipient_id)
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
            let prior_state = agent.state.clone();
            let now = now_ms();
            let source_key = format!("result_webhook:{sub_id}:{event_id}");
            let msg_text = format!(
                "[External Result Event]\n\
                 - subscription_id: `{sub_id}`\n\
                 - event_id: `{event_id}`\n\
                 - event_type: `{event_type}`\n\
                 - source: `{event_source}`\n\
                 - subject: `{}`\n\
                 - seq: `{}`\n\n\
                 ```json\n{}\n```",
                event_subject.as_deref().unwrap_or("-"),
                event_seq
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                serde_json::to_string_pretty(&payload).unwrap_or_else(|_| body_str.to_string()),
            );

            let (msg_id, msg_seq) = if let Some(existing_msg) =
                message_by_source(&tx, &recipient_id, &source_key)
                    .map_err(|e| ResultWebhookError::Store(e.to_string()))?
            {
                (existing_msg.id.to_string(), existing_msg.seq)
            } else {
                let seq = next_seq(&tx, &recipient_id)
                    .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
                let id = MessageId::generate();
                let sender = format!("webhook:{event_source}");
                let reply_to = format!("agent:{}", agent.name);
                let context = anyagents_harness::Context {
                    sender: Some(sender.clone()),
                    sender_name: Some(event_source.clone()),
                    recipient: Some(reply_to.clone()),
                    channel: Some("webhook".to_string()),
                    chat_id: None,
                    chat_title: event_subject.clone(),
                    chat_type: Some(event_type.clone()),
                    thread_id: event_seq,
                    message_id: None,
                    destination: Some(reply_to.clone()),
                    event: Some(event_type.clone()),
                    reply_to_author: None,
                    mentions: Vec::new(),
                };
                tx.execute(
                    "INSERT INTO messages (
                        id, agent_id, seq, source_time_ms, source_key, sender, body, reply_to,
                        received_at_ms, origin, context
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10)",
                    params![
                        id.as_str(),
                        recipient_id.as_str(),
                        seq,
                        webhook_timestamp.saturating_mul(1000),
                        source_key,
                        sender,
                        msg_text,
                        reply_to,
                        now,
                        serde_json::to_string(&context).ok(),
                    ],
                )
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
                let _ = index_rag_chunk(
                    &tx,
                    id.as_str(),
                    recipient_id.as_str(),
                    &agent.name,
                    "in",
                    &sender,
                    &reply_to,
                    now,
                    &msg_text,
                );
                (id.to_string(), seq)
            };

            let woke = if prior_state == "archived" || prior_state == "created" {
                tx.execute(
                    "UPDATE agents SET state = 'running' WHERE id = ?1 AND state IN ('archived', 'created')",
                    [recipient_id.as_str()],
                )
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?
                    > 0
            } else {
                false
            };

            let del_id = format!("rdel_{}", uuid::Uuid::now_v7().simple());
            tx.execute(
                "INSERT INTO result_deliveries (
                    id, subscription_id, recipient_id, webhook_id, webhook_timestamp,
                    event_id, event_type, source, subject, org_id, seq,
                    message_id, message_seq, woke_from_state, payload_json, received_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    del_id,
                    sub_id,
                    recipient_id.as_str(),
                    webhook_id,
                    webhook_timestamp,
                    event_id,
                    event_type,
                    event_source,
                    event_subject,
                    event_org_id,
                    event_seq,
                    msg_id,
                    msg_seq,
                    prior_state,
                    body_str,
                    now,
                ],
            )
            .map_err(|e| ResultWebhookError::Store(e.to_string()))?;

            if let Some(s) = event_seq {
                tx.execute(
                    "UPDATE result_subscriptions
                     SET last_seq = MAX(last_seq, ?1), updated_at_ms = ?2
                     WHERE id = ?3",
                    params![s, now, sub_id],
                )
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
            } else {
                tx.execute(
                    "UPDATE result_subscriptions SET updated_at_ms = ?1 WHERE id = ?2",
                    params![now, sub_id],
                )
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
            }

            let view = delivery_view_by_id(&tx, &del_id, false)
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
            tx.commit()
                .map_err(|e| ResultWebhookError::Store(e.to_string()))?;
            (
                ResultWebhookOutcome::Accepted(view),
                if prior_state == "paused" {
                    None
                } else {
                    Some(recipient_id)
                },
                woke,
            )
        };

        if let Some(agent_id) = wake_agent_id {
            if woke_from_archived {
                self.notify(Wake::Agent {
                    agent_id: agent_id.clone(),
                });
            }
            self.notify(Wake::Mail { agent_id });
        }
        Ok(outcome)
    }

    /// Administrative metadata only; callers must never pass credentials here.
    pub fn record_admin_audit(&self, actor: &str, action: &str, target: &str) -> Result<(), Error> {
        self.conn().execute(
            "INSERT INTO admin_audit (created_at, actor, action, target) VALUES (?1, ?2, ?3, ?4)",
            params![now_ms(), actor, action, target],
        )?;
        Ok(())
    }

    pub fn set_agent_instructions(
        &self,
        id: &AgentId,
        instructions: Option<&str>,
    ) -> Result<Agent, Error> {
        let clean = instructions.map(str::trim).filter(|s| !s.is_empty());
        let conn = self.conn();
        agent_by_id(&conn, id)?;
        conn.execute(
            "UPDATE agents SET instructions = ?1 WHERE id = ?2",
            params![clean, id.as_str()],
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
        found.ok_or_else(|| Error::AgentNotFound(name.to_string()))
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
        let members = self.room_members(room)?;
        let mut delivered = Vec::new();
        for agent in self.list_agents()? {
            if agent.state == "archived" || agent.state == "paused" {
                continue;
            }
            if author.as_deref() == Some(agent.name.as_str())
                || author.as_deref() == Some(agent.id.as_str())
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
        let reply_to_author = incoming
            .quote
            .as_ref()
            .and_then(|quote| resolve_quote_author(&tx, quote));
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
        let event = if incoming.text.starts_with("[Pinned message #")
            || incoming.text.starts_with("[pinned message")
        {
            Some("pinned".to_string())
        } else if incoming.text.starts_with("[Edited message #") {
            Some("edited".to_string())
        } else if incoming.text.starts_with("[Deleted message #") {
            Some("deleted".to_string())
        } else if incoming.text.starts_with("[Reaction ") {
            Some("reaction".to_string())
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
            destination: incoming.reply_to.clone().or_else(|| {
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
            }),
            event,
            reply_to_author,
            mentions,
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
        let woke_archived = tx.execute(
            "UPDATE agents SET state = 'running' WHERE id = ?1 AND state = 'archived'",
            [agent_id.as_str()],
        )? > 0;
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
            );
        }
        tx.commit()?;
        drop(conn);
        if woke_archived {
            self.notify(Wake::Agent {
                agent_id: agent_id.clone(),
            });
        }
        self.notify(Wake::Mail {
            agent_id: agent_id.clone(),
        });
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

    /// Update an agent's lifecycle state (`running`, `paused`, `archived`, `stopped`) and
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
    /// `archived` when it has delivered every message in its mailbox and
    /// completed its latest turn (`completed_seq >= opened_seq >= max_seq > 0`).
    ///
    /// Returns `Ok(true)` when the sub-agent transitioned to `archived` (and
    /// notifies the supervisor to stop its worker slot), or `Ok(false)` if new
    /// mail arrived or a turn is still open.
    pub fn complete_idle_subagent(&self, id: &AgentId) -> Result<bool, Error> {
        let (transitioned, silent_completion) = {
            let mut conn = self.conn();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
                 SET state = 'archived'
                 WHERE id = ?1
                   AND parent_id IS NOT NULL
                   AND state = 'running'
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
            (updated > 0, if updated > 0 { silent_info } else { None })
        };
        if transitioned {
            self.notify(Wake::Agent {
                agent_id: id.clone(),
            });
            if let Some((parent_id, sub_name, completed_seq)) = silent_completion {
                let notice = format!(
                    "[Sub-Agent Completed] Sub-agent `agent:{sub_name}` (`{id}`) finished its turn and scaled to zero (`state = archived`) without sending a direct reply. Call `subagents({{ \"action\": \"inspect\", \"agent\": \"{sub_name}\" }})` to read its output or `subagents({{ \"action\": \"steer\", \"agent\": \"{sub_name}\", \"prompt\": \"...\" }})` to continue."
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
                "agent_capabilities",
                "usage_records",
                "mailbox_cursors",
                "turn_recovery",
                "messages",
                "outbound_messages",
                "group_agents",
                "channel_agents",
            ] {
                tx.execute(
                    &format!("DELETE FROM {table} WHERE agent_id = ?1"),
                    [id.as_str()],
                )?;
            }
            for table in ["result_deliveries", "result_subscriptions"] {
                let _ = tx.execute(
                    &format!("DELETE FROM {table} WHERE recipient_id = ?1"),
                    [id.as_str()],
                );
            }
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
        );
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
        let mut turns = Vec::new();
        let mut stmt = conn.prepare(
            "SELECT chunk_id, direction, sender, destination, text, timestamp_ms
             FROM rag_chunks
             WHERE timestamp_ms > ?1
             ORDER BY timestamp_ms DESC
             LIMIT ?2",
        )?;
        let mut rows = stmt.query(params![since, lim * 4])?;
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
        if let Some(peer) = with_peer.map(str::trim).filter(|s| !s.is_empty()) {
            turns.retain(|turn| matches_conversation_peer(turn, peer));
        }
        turns.sort_by(|a, b| {
            a.timestamp_ms
                .cmp(&b.timestamp_ms)
                .then_with(|| a.id.cmp(&b.id))
        });
        if let Some(limit) = limit
            && turns.len() > limit
        {
            turns.drain(..turns.len() - limit);
        }
        Ok(turns)
    }

    /// Return the locked canonical embedding model and dimensionality for this store.
    pub fn locked_embedding_spec(&self) -> Result<(String, usize), Error> {
        let conn = self.conn();
        Self::locked_embedding_spec_conn(&conn)
    }

    fn locked_embedding_spec_conn(conn: &Connection) -> Result<(String, usize), Error> {
        let model: String = conn
            .query_row(
                "SELECT value FROM rag_meta WHERE key = 'embedding_model'",
                [],
                |r| r.get(0),
            )
            .unwrap_or_else(|_| "voyageai/voyage-4-large".to_string());
        let dim_str: String = conn
            .query_row(
                "SELECT value FROM rag_meta WHERE key = 'embedding_dimensions'",
                [],
                |r| r.get(0),
            )
            .unwrap_or_else(|_| "1024".to_string());
        let dim = dim_str.parse::<usize>().unwrap_or(1024);
        Ok((model, dim))
    }

    /// Look up a cached `i8` quantized embedding by `content_hash` for the currently locked model.
    pub fn get_embedding_i8(&self, content_hash: &str) -> Result<Option<Vec<i8>>, Error> {
        let conn = self.conn();
        let (locked_model, locked_dim) = Self::locked_embedding_spec_conn(&conn)?;
        let raw: Option<Vec<u8>> = conn
            .query_row(
                "SELECT vector_i8 FROM rag_embeddings
                 WHERE content_hash = ?1 AND model = ?2 AND dimensions = ?3",
                params![
                    content_hash,
                    locked_model,
                    i64::try_from(locked_dim).unwrap_or(1024)
                ],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|bytes| bytes.into_iter().map(|b| b as i8).collect()))
    }

    /// Store a quantized `i8` embedding for `content_hash`.
    pub fn store_embedding_i8(
        &self,
        content_hash: &str,
        model: &str,
        dimensions: usize,
        vec_i8: &[i8],
    ) -> Result<(), Error> {
        let conn = self.conn();
        let bytes: Vec<u8> = vec_i8.iter().map(|&x| x as u8).collect();
        conn.execute(
            "INSERT OR REPLACE INTO rag_embeddings (content_hash, model, dimensions, vector_i8, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![content_hash, model, i64::try_from(dimensions).unwrap_or(1024), bytes, now_ms()],
        )?;
        Ok(())
    }

    /// Hybrid RAG Semantic + BM25 Keyword Search with Reciprocal Rank Fusion (RRF).
    ///
    /// Searches `rag_chunks` across `agent_filter` (`None` or `"all"` searches all agents)
    /// combining Dense Vector Cosine Similarity (`rag_embeddings`) and SQLite FTS5 BM25
    /// (`rag_chunks_fts`), plus live message fallback so unindexed turns are also matched.
    pub fn search_conversation(
        &self,
        agent_filter: Option<&str>,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
        query_text: &str,
        query_vec_i8: Option<&[i8]>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        let conn = self.conn();
        let limit = limit.unwrap_or(20).clamp(1, 200);
        let since = since_ms.unwrap_or(0);
        let agent_name_filter = agent_filter
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all") && *s != "*")
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_lowercase());
        let peer_filter = with_peer.map(str::trim).filter(|s| !s.is_empty());

        let mut rrf_scores: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        let mut chunk_meta: std::collections::HashMap<String, ConversationTurn> =
            std::collections::HashMap::new();

        // 1. Dense Vector Leg (Cosine Similarity over int8-quantized L2-normalized vectors, strictly scoped to locked_embedding_spec).
        if let Some(qvec) = query_vec_i8
            && !qvec.is_empty()
        {
            let (locked_model, locked_dim) = Self::locked_embedding_spec_conn(&conn)?;
            let q_norm_sq: i32 = qvec.iter().map(|&x| (x as i32) * (x as i32)).sum();
            let q_norm = (q_norm_sq as f64).sqrt().max(1.0);
            let mut scored_hashes: Vec<(String, f64)> = Vec::new();
            let mut emb_stmt = conn.prepare(
                "SELECT content_hash, vector_i8 FROM rag_embeddings
                 WHERE model = ?1 AND dimensions = ?2",
            )?;
            let mut emb_rows = emb_stmt.query(params![
                locked_model,
                i64::try_from(locked_dim).unwrap_or(1024)
            ])?;
            while let Some(row) = emb_rows.next()? {
                let hash: String = row.get(0)?;
                let blob: Vec<u8> = row.get(1)?;
                if blob.len() != qvec.len() {
                    continue;
                }
                let mut dot: i32 = 0;
                let mut v_norm_sq: i32 = 0;
                for (&qb, &vb_u8) in qvec.iter().zip(blob.iter()) {
                    let q = qb as i32;
                    let v = (vb_u8 as i8) as i32;
                    dot += q * v;
                    v_norm_sq += v * v;
                }
                let denom = q_norm * (v_norm_sq as f64).sqrt().max(1.0);
                let cos = (dot as f64) / denom;
                if cos > 0.15 {
                    scored_hashes.push((hash, cos));
                }
            }
            let candidate_pool = (limit * 8).max(120);
            scored_hashes
                .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            scored_hashes.truncate(candidate_pool);

            for (rank, (hash, cos)) in scored_hashes.into_iter().enumerate() {
                let sql = if agent_name_filter.is_some() {
                    "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, agent_name
                     FROM rag_chunks
                     WHERE content_hash = ?1 AND timestamp_ms > ?2 AND LOWER(agent_name) = ?3
                     ORDER BY timestamp_ms DESC LIMIT 2"
                } else {
                    "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, agent_name
                     FROM rag_chunks
                     WHERE content_hash = ?1 AND timestamp_ms > ?2
                     ORDER BY timestamp_ms DESC LIMIT 2"
                };
                let mut cstmt = conn.prepare(sql)?;
                let mut crows = if let Some(ref aname) = agent_name_filter {
                    cstmt.query(params![hash, since, aname])?
                } else {
                    cstmt.query(params![hash, since])?
                };
                while let Some(crow) = crows.next()? {
                    let cid: String = crow.get(0)?;
                    let turn = ConversationTurn {
                        id: cid.clone(),
                        direction: crow.get(1)?,
                        from: crow.get(2)?,
                        to: crow.get(3)?,
                        reply_to: Some(format!(
                            "rag:cos={cos:.3}:agent={}",
                            crow.get::<_, String>(6)?
                        )),
                        text: crow.get(4)?,
                        timestamp_ms: crow.get(5)?,
                    };
                    if let Some(peer) = peer_filter
                        && !matches_conversation_peer(&turn, peer)
                    {
                        continue;
                    }
                    *rrf_scores.entry(cid.clone()).or_insert(0.0) +=
                        1.35 / (50.0 + (rank as f64) + 1.0) + (cos * 0.01);
                    chunk_meta.entry(cid).or_insert(turn);
                }
            }
        }

        // 2. Lexical FTS5 BM25 Leg (`rag_chunks_fts`) — rank inside FTS5 first, then join top hits.
        let fts_query = build_fts5_query(query_text);
        if !fts_query.is_empty()
            && let Ok(mut fstmt) = conn.prepare(
                "SELECT c.chunk_id, c.direction, c.sender, c.destination, c.text, c.timestamp_ms, c.agent_name
                 FROM (
                     SELECT chunk_id, bm25(rag_chunks_fts) as score
                     FROM rag_chunks_fts
                     WHERE rag_chunks_fts MATCH ?1
                     ORDER BY score ASC
                     LIMIT ?3
                 ) f
                 JOIN rag_chunks c ON c.chunk_id = f.chunk_id
                 WHERE c.timestamp_ms > ?2
                 ORDER BY f.score ASC",
            )
            && let Ok(mut frows) = fstmt.query(params![fts_query, since, i64::try_from((limit * 8).max(120)).unwrap_or(120)])
        {
            let mut rank = 0usize;
            while let Ok(Some(row)) = frows.next() {
                let cid: String = row.get(0)?;
                let aname: String = row.get(6)?;
                if let Some(ref wanted) = agent_name_filter
                    && !aname.eq_ignore_ascii_case(wanted)
                {
                    continue;
                }
                let turn = ConversationTurn {
                    id: cid.clone(),
                    direction: row.get(1)?,
                    from: row.get(2)?,
                    to: row.get(3)?,
                    reply_to: Some(format!("rag:bm25_rank={}:agent={aname}", rank + 1)),
                    text: row.get(4)?,
                    timestamp_ms: row.get(5)?,
                };
                if let Some(peer) = peer_filter
                    && !matches_conversation_peer(&turn, peer)
                {
                    continue;
                }
                *rrf_scores.entry(cid.clone()).or_insert(0.0) +=
                    1.85 / (50.0 + (rank as f64) + 1.0);
                chunk_meta.entry(cid).or_insert(turn);
                rank += 1;
            }
        }

        // 3. Direct substring fallback (covers CJK substrings or fresh turns in `messages` / `outbound_messages`).
        let q_trim = query_text.trim();
        if !q_trim.is_empty() && rrf_scores.len() < limit {
            let like_pat = format!("%{}%", q_trim.replace('%', "\\%").replace('_', "\\_"));
            let mut lstmt = conn.prepare(
                "SELECT m.id, a.name, COALESCE(m.sender, 'unknown'), COALESCE(m.reply_to, 'agent:' || a.name), m.body, m.received_at_ms
                 FROM messages m
                 JOIN agents a ON a.id = m.agent_id
                 WHERE m.received_at_ms > ?1 AND m.body LIKE ?2 ESCAPE '\\'
                 ORDER BY m.received_at_ms DESC LIMIT ?3",
            )?;
            let mut lrows = lstmt.query(params![
                since,
                like_pat,
                i64::try_from(limit * 2).unwrap_or(40)
            ])?;
            let mut rank = 0usize;
            while let Some(row) = lrows.next()? {
                let cid: String = row.get(0)?;
                let aname: String = row.get(1)?;
                if let Some(ref wanted) = agent_name_filter
                    && !aname.eq_ignore_ascii_case(wanted)
                {
                    continue;
                }
                let turn = ConversationTurn {
                    id: cid.clone(),
                    direction: "in".to_string(),
                    from: row.get(2)?,
                    to: row.get(3)?,
                    reply_to: Some(format!("rag:substring:agent={aname}")),
                    text: row.get(4)?,
                    timestamp_ms: row.get(5)?,
                };
                if let Some(peer) = peer_filter
                    && !matches_conversation_peer(&turn, peer)
                {
                    continue;
                }
                *rrf_scores.entry(cid.clone()).or_insert(0.0) += 0.8 / (60.0 + (rank as f64) + 1.0);
                chunk_meta.entry(cid).or_insert(turn);
                rank += 1;
            }
        }

        let mut ranked: Vec<(String, f64)> = rrf_scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Deduplicate identical message text across multiple recipient agents in room broadcasts.
        let mut seen_text_sig: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut out = Vec::new();
        for (cid, score) in ranked {
            if let Some(mut turn) = chunk_meta.remove(&cid) {
                let sig: String = turn.text.chars().take(180).collect();
                if !seen_text_sig.insert(sig) {
                    continue;
                }
                turn.reply_to = Some(format!(
                    "score={score:.4} ({})",
                    turn.reply_to.unwrap_or_default()
                ));
                out.push(turn);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
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
    /// eligible for automatic mid-turn recovery (`recovery_count < MAX_RECOVERY_ATTEMPTS`).
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
        if opened_seq <= completed_seq || recovery_count >= MAX_RECOVERY_ATTEMPTS {
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
        let received_at: Option<i64> = conn
            .query_row(
                "SELECT received_at_ms FROM messages WHERE agent_id = ?1 AND seq = ?2",
                params![agent_id.as_str(), seq],
                |row| row.get(0),
            )
            .optional()?;
        Ok(received_at.map(|at| (now_ms - at).max(0)))
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// `{data}/agents/{id}/work`.
pub fn agent_work_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
    data_dir.join("agents").join(id.as_str()).join("work")
}

/// `{data}/agents/{id}/config`.
pub fn agent_config_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
    data_dir.join("agents").join(id.as_str()).join("config")
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
) -> Result<(), Error> {
    let cleaned = clean_rag_text(text);
    if cleaned.chars().count() < 4 {
        return Ok(());
    }
    let chash = rag_content_hash(&cleaned);
    conn.execute(
        "INSERT OR REPLACE INTO rag_chunks (
            chunk_id, agent_id, agent_name, direction, sender, destination, timestamp_ms, content_hash, text
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            chunk_id,
            agent_id,
            agent_name,
            direction,
            sender,
            destination,
            timestamp_ms,
            chash,
            cleaned
        ],
    )?;
    conn.execute(
        "INSERT INTO rag_chunks_fts (chunk_id, agent_name, sender, destination, text)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![chunk_id, agent_name, sender, destination, cleaned],
    )?;
    Ok(())
}

fn build_fts5_query(raw: &str) -> String {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in raw.chars() {
        if ch.is_alphanumeric() || ch == '_' || ch == '-' {
            current.push(ch);
        } else if !current.is_empty() {
            let trimmed = current.trim_matches(['-', '_']);
            if !trimmed.is_empty() {
                tokens.push(format!("\"{}\"", trimmed.replace('"', "")));
            }
            current.clear();
        }
    }
    if !current.is_empty() {
        let trimmed = current.trim_matches(['-', '_']);
        if !trimmed.is_empty() {
            tokens.push(format!("\"{}\"", trimmed.replace('"', "")));
        }
    }
    tokens.join(" OR ")
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
        if origin.message_id == Some(message_id)
            && (!is_basic_group || (!in_is_relay && !stored_is_relay))
        {
            return Ok(Some(msg));
        }
        // Basic Telegram groups (`chat_type == "group"`, negative non-`-100...` IDs)
        // assign each bot its own account-local `message_id` for the same human
        // message. Match by `(chat_id, sender user, source_time_ms, body)` so cross-bot
        // passive relay and direct webhook delivery coalesce into a single message.
        if (in_is_relay || stored_is_relay)
            && strip_quoted_block(&msg.text) == strip_quoted_block(incoming_body)
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

pub const STANDARD_WEBHOOKS_TOLERANCE_SECS: i64 = 300;

fn subscription_view_by_id(conn: &Connection, id: &str) -> Result<ResultSubscription, Error> {
    let found = conn
        .query_row(
            "SELECT s.id, s.recipient_id, a.name, s.source, s.subject, s.org_id,
                    s.event_types, s.secret, s.created_by, s.enabled,
                    s.after_seq, s.last_seq, s.created_at_ms, s.updated_at_ms
             FROM result_subscriptions s
             JOIN agents a ON a.id = s.recipient_id
             WHERE s.id = ?1",
            [id],
            |row| {
                let sub_id: String = row.get(0)?;
                let rid_raw: String = row.get(1)?;
                let rid = AgentId::parse(&rid_raw).map_err(|err| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                })?;
                let recipient: String = row.get(2)?;
                let source: String = row.get(3)?;
                let subject: Option<String> = row.get(4)?;
                let org_id: Option<String> = row.get(5)?;
                let event_types_raw: String = row.get(6)?;
                let event_types: Vec<String> =
                    serde_json::from_str(&event_types_raw).unwrap_or_default();
                let secret: String = row.get(7)?;
                let created_by: String = row.get(8)?;
                let enabled_i64: i64 = row.get(9)?;
                let after_seq: i64 = row.get(10)?;
                let last_seq: i64 = row.get(11)?;
                let created_at_ms: i64 = row.get(12)?;
                let updated_at_ms: i64 = row.get(13)?;
                Ok((
                    sub_id,
                    rid,
                    recipient,
                    source,
                    subject,
                    org_id,
                    event_types,
                    secret,
                    created_by,
                    enabled_i64 != 0,
                    after_seq,
                    last_seq,
                    created_at_ms,
                    updated_at_ms,
                ))
            },
        )
        .optional()?;
    let Some((
        sub_id,
        recipient_id,
        recipient,
        source,
        subject,
        org_id,
        event_types,
        secret,
        created_by,
        enabled,
        after_seq,
        last_seq,
        created_at_ms,
        updated_at_ms,
    )) = found
    else {
        return Err(Error::Message(format!(
            "result subscription `{id}` not found"
        )));
    };
    let secret_fingerprint = decode_standard_webhooks_secret(&secret)
        .map(|bytes| hex(&sha256_bytes(&bytes)[..6]))
        .unwrap_or_else(|_| hex(&sha256_bytes(secret.as_bytes())[..6]));
    let delivery_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM result_deliveries WHERE subscription_id = ?1",
            [&sub_id],
            |row| row.get(0),
        )
        .unwrap_or(0);
    let latest_del_id: Option<String> = conn
        .query_row(
            "SELECT id FROM result_deliveries WHERE subscription_id = ?1 ORDER BY received_at_ms DESC, id DESC LIMIT 1",
            [&sub_id],
            |row| row.get(0),
        )
        .optional()?;
    let latest_delivery_stage = match latest_del_id {
        Some(del_id) => delivery_view_by_id(conn, &del_id, false)
            .map(|d| d.delivery_stage)
            .unwrap_or_else(|_| "awaiting_event".to_string()),
        None => "awaiting_event".to_string(),
    };
    Ok(ResultSubscription {
        id: sub_id,
        recipient_id,
        recipient,
        source,
        subject,
        org_id,
        event_types,
        secret_fingerprint,
        created_by,
        enabled,
        registration_status: if enabled {
            "registered".to_string()
        } else {
            "disabled".to_string()
        },
        after_seq,
        last_seq,
        delivery_count,
        latest_delivery_stage,
        created_at_ms,
        updated_at_ms,
    })
}

fn delivery_view_by_id(
    conn: &Connection,
    id: &str,
    duplicate: bool,
) -> Result<ResultDeliveryView, Error> {
    let found = conn
        .query_row(
            "SELECT d.id, d.subscription_id, d.recipient_id, a.name, a.state,
                    d.woke_from_state, d.webhook_id, d.webhook_timestamp,
                    d.event_id, d.event_type, d.source, d.subject, d.org_id, d.seq,
                    d.message_id, d.message_seq, d.received_at_ms,
                    COALESCE(mc.delivered_seq, 0),
                    COALESCE(tr.opened_seq, 0),
                    COALESCE(tr.completed_seq, 0),
                    m.delivered_at_ms,
                    COALESCE(s.enabled, 0)
             FROM result_deliveries d
             JOIN agents a ON a.id = d.recipient_id
             LEFT JOIN result_subscriptions s ON s.id = d.subscription_id
             LEFT JOIN mailbox_cursors mc ON mc.agent_id = d.recipient_id
             LEFT JOIN turn_recovery tr ON tr.agent_id = d.recipient_id
             LEFT JOIN messages m ON m.id = d.message_id
             WHERE d.id = ?1",
            [id],
            |row| {
                let del_id: String = row.get(0)?;
                let sub_id: String = row.get(1)?;
                let rid_raw: String = row.get(2)?;
                let rid = AgentId::parse(&rid_raw).map_err(|err| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                })?;
                let recipient: String = row.get(3)?;
                let recipient_state: String = row.get(4)?;
                let woke_from_state: String = row.get(5)?;
                let webhook_id: String = row.get(6)?;
                let webhook_timestamp: i64 = row.get(7)?;
                let event_id: String = row.get(8)?;
                let event_type: String = row.get(9)?;
                let source: String = row.get(10)?;
                let subject: Option<String> = row.get(11)?;
                let org_id: Option<String> = row.get(12)?;
                let seq: Option<i64> = row.get(13)?;
                let message_id: String = row.get(14)?;
                let message_seq: i64 = row.get(15)?;
                let received_at_ms: i64 = row.get(16)?;
                let delivered_seq: i64 = row.get(17)?;
                let opened_seq: i64 = row.get(18)?;
                let completed_seq: i64 = row.get(19)?;
                let delivered_at_ms: Option<i64> = row.get(20)?;
                let sub_enabled: i64 = row.get(21)?;
                let delivery_stage = if completed_seq >= message_seq {
                    "completed_turn".to_string()
                } else if delivered_seq >= message_seq || opened_seq >= message_seq {
                    "admitted_delivered".to_string()
                } else if recipient_state == "paused" {
                    "held_paused".to_string()
                } else {
                    "queued".to_string()
                };
                Ok(ResultDeliveryView {
                    id: del_id,
                    subscription_id: sub_id,
                    recipient_id: rid,
                    recipient,
                    recipient_state,
                    woke_from_state,
                    webhook_id,
                    webhook_timestamp,
                    event_id,
                    event_type,
                    source,
                    subject,
                    org_id,
                    seq,
                    message_id,
                    message_seq,
                    delivered_seq,
                    completed_turn_seq: completed_seq,
                    registration_status: if sub_enabled != 0 {
                        "registered".to_string()
                    } else {
                        "disabled".to_string()
                    },
                    event_status: if duplicate {
                        "event_duplicate".to_string()
                    } else {
                        "event_accepted".to_string()
                    },
                    delivery_stage,
                    duplicate,
                    received_at_ms,
                    delivered_at_ms,
                })
            },
        )
        .optional()?;
    found.ok_or_else(|| Error::Message(format!("result delivery `{id}` not found")))
}

fn generate_standard_webhooks_secret() -> Result<String, Error> {
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(format!("whsec_{}", base64_encode(&bytes)))
}

pub fn decode_standard_webhooks_secret(secret: &str) -> Result<Vec<u8>, Error> {
    let trimmed = secret.trim();
    let encoded = trimmed.strip_prefix("whsec_").unwrap_or(trimmed);
    if encoded.is_empty() {
        return Err(Error::Message("Standard Webhooks secret is empty".into()));
    }
    base64_decode(encoded)
}

/// Sign `(webhook_id, webhook_timestamp, raw_body)` using the Standard Webhooks
/// `v1,<base64_hmac_sha256>` specification.
pub fn standard_webhooks_sign(
    secret: &str,
    webhook_id: &str,
    webhook_timestamp: &str,
    raw_body: &[u8],
) -> Result<String, Error> {
    if webhook_id.is_empty() || webhook_id.contains('.') {
        return Err(Error::Message("invalid webhook_id".into()));
    }
    let key = decode_standard_webhooks_secret(secret)?;
    let mut signed = Vec::with_capacity(webhook_id.len() + webhook_timestamp.len() + raw_body.len() + 2);
    signed.extend_from_slice(webhook_id.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(webhook_timestamp.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(raw_body);
    let mac = hmac_sha256_bytes(&key, &signed);
    Ok(format!("v1,{}", base64_encode(&mac)))
}

/// Constant-time verification of a `webhook-signature` header (`v1,<b64> ...`)
/// against `secret` (`whsec_<b64>`).
pub fn standard_webhooks_verify(
    secret: &str,
    webhook_id: &str,
    webhook_timestamp: &str,
    raw_body: &[u8],
    signature_header: &str,
) -> bool {
    let Ok(expected_header) = standard_webhooks_sign(secret, webhook_id, webhook_timestamp, raw_body) else {
        return false;
    };
    let Some(expected_sig) = expected_header.strip_prefix("v1,") else {
        return false;
    };
    let mut matched = false;
    for token in signature_header.split_ascii_whitespace() {
        if let Some(candidate) = token.strip_prefix("v1,")
            && constant_time_eq(expected_sig.as_bytes(), candidate.as_bytes())
        {
            matched = true;
        }
    }
    matched
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |diff, (x, y)| diff | (x ^ y))
        == 0
}

fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (data.len() as u64) * 8;
    let mut padded = data.to_vec();
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for (i, word) in chunk.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*word);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (state, delta) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *state = state.wrapping_add(delta);
        }
    }
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn hmac_sha256_bytes(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut k_block = [0u8; 64];
    if key.len() > 64 {
        let digest = sha256_bytes(key);
        k_block[..32].copy_from_slice(&digest);
    } else {
        k_block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(64 + message.len());
    for byte in &k_block {
        inner.push(byte ^ 0x36);
    }
    inner.extend_from_slice(message);
    let inner_hash = sha256_bytes(&inner);

    let mut outer = Vec::with_capacity(64 + 32);
    for byte in &k_block {
        outer.push(byte ^ 0x5c);
    }
    outer.extend_from_slice(&inner_hash);
    sha256_bytes(&outer)
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };
        out.push(char::from(TABLE[(b0 >> 2) as usize]));
        out.push(char::from(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize]));
        if chunk.len() > 1 {
            out.push(char::from(TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize]));
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(char::from(TABLE[(b2 & 0x3f) as usize]));
        } else {
            out.push('=');
        }
    }
    out
}

fn base64_decode(input: &str) -> Result<Vec<u8>, Error> {
    let clean = input.trim().trim_end_matches('=');
    if clean.len() % 4 == 1 {
        return Err(Error::Message("invalid base64 length".into()));
    }
    let mut out = Vec::with_capacity((clean.len() * 3) / 4);
    let mut buf = 0u32;
    let mut bits = 0u8;
    for byte in clean.bytes() {
        let val = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => return Err(Error::Message("invalid base64 character".into())),
        };
        buf = (buf << 6) | u32::from(val);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xff) as u8);
        }
    }
    Ok(out)
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

        // Archive alice, then send a message with @bob_helper_bot, @alice, and pinned event.
        store.set_agent_state(&alice.id, "archived").unwrap();
        assert_eq!(store.agent_by_name("alice").unwrap().state, "archived");

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
    fn search_conversation_with_dense_query_vector_ranks_without_reentrant_lock() {
        let (_dir, store) = setup();
        let alice = agent(&store, "alice");
        let outcome = store
            .receive(
                &alice.id,
                Incoming {
                    text: "durable turn recovery checkpoint design".to_string(),
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
        let ReceiveOutcome::New(msg) = outcome else {
            panic!("expected New");
        };
        let hash = rag_content_hash(&msg.text);
        let (model, dim) = store.locked_embedding_spec().unwrap();
        let mut vec_i8 = vec![0i8; dim];
        vec_i8[0] = 100;
        store
            .store_embedding_i8(&hash, &model, dim, &vec_i8)
            .unwrap();

        let hits = store
            .search_conversation(
                Some("alice"),
                None,
                None,
                Some(10),
                "durable recovery",
                Some(&vec_i8),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].text.contains("durable turn recovery"));
    }

    #[test]
    fn complete_idle_subagent_scales_finished_subagent_to_zero_and_wakes_on_new_mail() {
        let (_dir, store) = setup();
        let root = agent(&store, "root");
        let mut child_new = NewAgent::new("root-worker", "opencode");
        child_new.parent_id = Some(root.id.clone());
        let child = store.create_agent(child_new).unwrap();
        store.set_agent_running(&child.id, "/work").unwrap();

        // Before any message/turn, does not auto-archive.
        assert!(!store.complete_idle_subagent(&child.id).unwrap());
        assert_eq!(store.agent(&child.id).unwrap().state, "running");

        // Send initial task message (seq = 1).
        store.receive(&child.id, Incoming::new("task 1")).unwrap();
        // While undelivered, does not auto-archive.
        assert!(!store.complete_idle_subagent(&child.id).unwrap());

        // Deliver seq = 1 (turn is open: opened_seq = 1, completed_seq = 0).
        store.note_delivered(&child.id, 1).unwrap();
        assert!(!store.complete_idle_subagent(&child.id).unwrap());

        // Complete turn 1 (completed_seq = 1 == max_seq).
        store.note_turn_completed(&child.id).unwrap();
        // Root agent is never archived by complete_idle_subagent.
        assert!(!store.complete_idle_subagent(&root.id).unwrap());
        // Sub-agent automatically transitions to archived!
        assert!(store.complete_idle_subagent(&child.id).unwrap());
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");

        // Sending a follow-up message (seq = 2) wakes the sub-agent back to running,
        // and it stays running until seq = 2 is delivered and its turn completes.
        store.receive(&child.id, Incoming::new("task 2")).unwrap();
        assert_eq!(store.agent(&child.id).unwrap().state, "running");
        assert!(!store.complete_idle_subagent(&child.id).unwrap());

        store.note_delivered(&child.id, 2).unwrap();
        assert!(!store.complete_idle_subagent(&child.id).unwrap());

        store.note_turn_completed(&child.id).unwrap();
        assert!(store.complete_idle_subagent(&child.id).unwrap());
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    }

    #[test]
    fn set_agent_parent_promotes_subagent_to_root_in_place_and_rejects_cycles() {
        let (_dir, store) = setup();
        let root = agent(&store, "prism");
        let mut mid_new = NewAgent::new("prism-owner-provision", "codex");
        mid_new.parent_id = Some(root.id.clone());
        let mid = store.create_agent(mid_new).unwrap();
        let mut child_new = NewAgent::new("owner-sylphx", "codex");
        child_new.parent_id = Some(mid.id.clone());
        let child = store.create_agent(child_new).unwrap();
        store.set_agent_state(&child.id, "paused").unwrap();
        store.receive(&child.id, Incoming::new("retained history")).unwrap();

        // Self-parenting and cycle parenting are refused.
        assert!(store.set_agent_parent(&root.id, Some(&root.id)).is_err());
        assert!(store.set_agent_parent(&root.id, Some(&child.id)).is_err());

        // Promoting child to root preserves ID, paused state, history, and clears ANYAGENTS_PARENT_NAME.
        let promoted = store.set_agent_parent(&child.id, None).unwrap();
        assert_eq!(promoted.id, child.id);
        assert_eq!(promoted.parent_id, None);
        assert_eq!(promoted.state, "paused");
        assert!(!promoted.is_admin);
        assert!(
            promoted
                .launch_env
                .iter()
                .all(|(k, _)| k != "ANYAGENTS_PARENT_NAME")
        );
        assert_eq!(store.inbox(&child.id, None).unwrap().len(), 1);
    }

    #[test]
    fn standard_webhooks_vector_and_durable_result_delivery() {
        // 1. Standard Webhooks specification test vector.
        let secret = concat!("whsec_", "MfKQ9r8G", "KYqrTwjU", "PD8ILPZI", "o2LaLaSw");
        let wh_id = concat!("msg_p5jXN8", "AQM9LWM0D4", "loKWxJek");
        let wh_ts = "1614265330";
        let wh_body = br#"{"test": 2432232314}"#;
        let sig = standard_webhooks_sign(secret, wh_id, wh_ts, wh_body).unwrap();
        let expected_sig = concat!(
            "v1,g0hM9SsE+",
            "OTPJTGt/tmIK",
            "tSyZlE3uFJEL",
            "VlNIOLJ1OE="
        );
        assert_eq!(sig, expected_sig);
        assert!(standard_webhooks_verify(secret, wh_id, wh_ts, wh_body, &sig));
        assert!(!standard_webhooks_verify(
            secret,
            wh_id,
            wh_ts,
            wh_body,
            "v1,tampered="
        ));

        // 2. End-to-end subscription, scale-to-zero wake, dedupe, and fail-closed checks.
        let (_dir, store) = setup();
        let owner = agent(&store, "owner-lockdocs");
        store.set_agent_state(&owner.id, "archived").unwrap();
        let (sub, gen_secret, changed) = store
            .register_result_subscription(NewResultSubscription {
                recipient_id: owner.id.clone(),
                source: "/spiron/promise-ledger".into(),
                subject: Some("01a184d5-3737-7165-a089-a97ad3d70b10".into()),
                org_id: Some("org_sylphx".into()),
                event_types: vec!["spiron.promise.kept".into(), "spiron.promise.broken".into()],
                secret: Some(secret.into()),
                previous_secret: None,
                created_by: "agent:owner-lockdocs".into(),
                after_seq: 0,
            })
            .unwrap();
        assert!(changed);
        assert_eq!(gen_secret.as_deref(), Some(secret));
        assert_eq!(sub.registration_status, "registered");
        assert_eq!(sub.latest_delivery_stage, "awaiting_event");

        let now_secs = (now_ms() / 1000).to_string();
        let event_body = serde_json::json!({
            "specversion": "1.0",
            "id": "promise-event:org_sylphx:7",
            "type": "spiron.promise.kept",
            "source": "/spiron/promise-ledger",
            "subject": "01a184d5-3737-7165-a089-a97ad3d70b10",
            "data": {
                "org_id": "org_sylphx",
                "promise_id": "01a184d5-3737-7165-a089-a97ad3d70b10",
                "seq": 7,
                "kind": "kept",
                "principal_id": "agt_worker"
            }
        })
        .to_string();
        let valid_sig =
            standard_webhooks_sign(secret, "msg_evt_7", &now_secs, event_body.as_bytes()).unwrap();

        // Negative: tampered signature fails closed without mutating state or mailbox.
        assert!(matches!(
            store.receive_result_webhook(
                Some(&sub.id),
                Some("msg_evt_7"),
                Some(&now_secs),
                Some("v1,bad_signature_value"),
                event_body.as_bytes(),
            ),
            Err(ResultWebhookError::Unauthorized(_))
        ));
        assert_eq!(store.agent(&owner.id).unwrap().state, "archived");
        assert!(store.inbox(&owner.id, None).unwrap().is_empty());

        // Negative: expired timestamp fails closed.
        let expired_ts = ((now_ms() / 1000) - 600).to_string();
        let expired_sig =
            standard_webhooks_sign(secret, "msg_evt_7", &expired_ts, event_body.as_bytes())
                .unwrap();
        assert!(matches!(
            store.receive_result_webhook(
                Some(&sub.id),
                Some("msg_evt_7"),
                Some(&expired_ts),
                Some(&expired_sig),
                event_body.as_bytes(),
            ),
            Err(ResultWebhookError::Unauthorized(_))
        ));

        // Negative: ungranted promise_id fails closed even when signed with the secret.
        let wrong_subject_body = serde_json::json!({
            "specversion": "1.0",
            "id": "promise-event:org_sylphx:8",
            "type": "spiron.promise.kept",
            "source": "/spiron/promise-ledger",
            "subject": "ungranted-promise-id",
            "data": {
                "org_id": "org_sylphx",
                "promise_id": "ungranted-promise-id",
                "seq": 8,
                "kind": "kept"
            }
        })
        .to_string();
        let wrong_subj_sig = standard_webhooks_sign(
            secret,
            "msg_evt_8",
            &now_secs,
            wrong_subject_body.as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            store.receive_result_webhook(
                Some(&sub.id),
                Some("msg_evt_8"),
                Some(&now_secs),
                Some(&wrong_subj_sig),
                wrong_subject_body.as_bytes(),
            ),
            Err(ResultWebhookError::Forbidden(_))
        ));

        // Positive: valid event wakes archived recipient to running and enqueues 1 message.
        let outcome = store
            .receive_result_webhook(
                None,
                Some("msg_evt_7"),
                Some(&now_secs),
                Some(&valid_sig),
                event_body.as_bytes(),
            )
            .unwrap();
        let view = match outcome {
            ResultWebhookOutcome::Accepted(v) => v,
            other => panic!("expected Accepted, got {other:?}"),
        };
        assert!(!view.duplicate);
        assert_eq!(view.event_status, "event_accepted");
        assert_eq!(view.delivery_stage, "queued");
        assert_eq!(view.woke_from_state, "archived");
        assert_eq!(store.agent(&owner.id).unwrap().state, "running");
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);

        // Duplicate redelivery returns Duplicate without adding another message.
        let dup = store
            .receive_result_webhook(
                Some(&sub.id),
                Some("msg_evt_7"),
                Some(&now_secs),
                Some(&valid_sig),
                event_body.as_bytes(),
            )
            .unwrap();
        let dup_view = match dup {
            ResultWebhookOutcome::Duplicate(v) => v,
            other => panic!("expected Duplicate, got {other:?}"),
        };
        assert!(dup_view.duplicate);
        assert_eq!(dup_view.event_status, "event_duplicate");
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);

        // Once delivered and turn completed, delivery_stage transitions through
        // `admitted_delivered` -> `completed_turn`.
        store.note_delivered(&owner.id, view.message_seq).unwrap();
        let after_deliver = store
            .list_result_deliveries(Some(&owner.id), Some(&sub.id), Some(10))
            .unwrap();
        assert_eq!(after_deliver[0].delivery_stage, "admitted_delivered");

        store.note_turn_completed(&owner.id).unwrap();
        let after_complete = store
            .list_result_deliveries(Some(&owner.id), Some(&sub.id), Some(10))
            .unwrap();
        assert_eq!(after_complete[0].delivery_stage, "completed_turn");
    }
}
