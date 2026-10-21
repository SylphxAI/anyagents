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
     reply_to, received_at_ms, delivered_at_ms, origin";

/// One agent row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Agent {
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
}

/// Fields for `agents.create`. The harness value is a catalog key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewAgent {
    pub name: String,
    pub harness: String,
    pub model: Option<String>,
    pub workdir: Option<String>,
    pub instructions: Option<String>,
}

impl NewAgent {
    pub fn new(name: impl Into<String>, harness: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            harness: harness.into(),
            model: None,
            workdir: None,
            instructions: None,
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
}

/// A new insert, or the row already stored for this source key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiveOutcome {
    New(Message),
    Duplicate(Message),
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
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(Duration::from_secs(5))?;
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
            id: AgentId::generate(),
            name: new.name,
            harness: new.harness,
            model: new.model,
            workdir: new.workdir,
            instructions: new.instructions,
            state: "created".into(),
            created_at_ms: now_ms(),
            launch_env: Vec::new(),
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
                id, name, harness, model, workdir, instructions, state, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'created', ?7)",
            params![
                agent.id.as_str(),
                agent.name,
                agent.harness,
                agent.model,
                agent.workdir,
                agent.instructions,
                agent.created_at_ms,
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

    pub fn agent_by_name(&self, name: &str) -> Result<Agent, Error> {
        let conn = self.conn();
        let found = conn
            .query_row(
                "SELECT id, name, harness, model, workdir, instructions, state, created_at_ms, launch_env
                 FROM agents WHERE name = ?1",
                [name],
                agent_from_row,
            )
            .optional()?;
        found.ok_or_else(|| Error::AgentNotFound(name.to_string()))
    }

    pub fn list_agents(&self) -> Result<Vec<Agent>, Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, name, harness, model, workdir, instructions, state, created_at_ms, launch_env
             FROM agents ORDER BY name",
        )?;
        let mut rows = stmt.query([])?;
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
            if author.as_deref() == Some(agent.name.as_str()) {
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
        };
        tx.execute(
            "INSERT INTO messages (
                id, agent_id, seq, source_time_ms, source_key, sender, body, reply_to, received_at_ms, origin
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
            ],
        )?;
        tx.commit()?;
        drop(conn);
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

    /// Messages with `seq > since`, in sequence order. `None` starts at the beginning.
    pub fn inbox(&self, agent_id: &AgentId, since: Option<i64>) -> Result<Vec<Message>, Error> {
        let conn = self.conn();
        agent_by_id(&conn, agent_id)?;
        let mut stmt = conn.prepare(&format!(
            "SELECT {MESSAGE_COLS}
             FROM messages WHERE agent_id = ?1 AND seq > ?2 ORDER BY seq"
        ))?;
        let mut rows = stmt.query(params![agent_id.as_str(), since.unwrap_or(0)])?;
        let mut messages = Vec::new();
        while let Some(row) = rows.next()? {
            messages.push(message_from_row(&conn, row)?);
        }
        Ok(messages)
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
            "SELECT COALESCE(MAX(seq), 0) FROM messages WHERE agent_id = ?1",
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
        tx.execute(
            "UPDATE messages SET delivered_at_ms = ?1 WHERE agent_id = ?2 AND seq = ?3",
            params![now_ms(), agent_id.as_str(), seq],
        )?;
        tx.commit()?;
        Ok(())
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
            "SELECT id, name, harness, model, workdir, instructions, state, created_at_ms, launch_env
             FROM agents WHERE id = ?1",
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
    Ok(Agent {
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
    let id: String = row.get(0)?;
    let agent_id = AgentId::parse(&row.get::<_, String>(1)?)?;
    let seq: i64 = row.get(2)?;
    let delivered = cursor(conn, &agent_id)?;
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
    })
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

fn next_seq(conn: &Connection, agent_id: &AgentId) -> Result<i64, Error> {
    let seq = conn.query_row(
        "SELECT COALESCE(MAX(seq), 0) + 1 FROM messages WHERE agent_id = ?1",
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
}
