//! JSON shapes shared by the unix-socket server and the MCP tools.

use anyagents_core::Quote;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AgentView {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub harness: String,
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_harness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_started_at_ms: Option<i64>,
    #[serde(default)]
    pub pending_switch: bool,
    pub workdir: Option<String>,
    pub state: String,
    pub process: String,
    #[serde(default)]
    pub turn_state: String,
    pub delivery_mode: String,
    pub steer_support: String,
    pub quota_reset_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Age of the oldest message the harness has not taken yet.
    pub oldest_undelivered_age_ms: Option<i64>,
    /// `installed`, `pending`, or `needed: <why>`: whether this agent's harness
    /// has been installed yet, and what stopped it.
    pub install: String,
    /// Standing role description / current responsibilities (`instructions` column in SQLite).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Assigned OS seat account name (e.g. `seat-atlas`, `seat-rhea`, `seat-worker`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_user: Option<String>,
    /// Assigned OS seat numeric UID from the agent's `seat_launch` spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_uid: Option<u32>,
    /// Assigned OS seat numeric GID from the agent's `seat_launch` spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_gid: Option<u32>,
    /// Active self-scheduled wakes (`at`, `every`) and event watches (`watch`) owned by this agent.
    #[serde(default)]
    pub schedules: Vec<ScheduleView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ScheduleView {
    pub id: String,
    pub agent_id: String,
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

impl From<anyagents_core::AgentSchedule> for ScheduleView {
    fn from(s: anyagents_core::AgentSchedule) -> Self {
        Self {
            id: s.id,
            agent_id: s.agent_id.to_string(),
            kind: s.kind,
            spec: s.spec,
            prompt: s.prompt,
            interval_ms: s.interval_ms,
            next_fire_at_ms: s.next_fire_at_ms,
            last_fired_at_ms: s.last_fired_at_ms,
            watch_baseline: s.watch_baseline,
            last_observed: s.last_observed,
            state: s.state,
            created_at_ms: s.created_at_ms,
        }
    }
}

fn default_harness() -> String {
    "claude-code".to_string()
}

/// Body of `POST /v1/agents`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateAgentRequest {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default = "default_harness")]
    pub harness: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default, alias = "description")]
    pub instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct GroupMemberView {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct GroupView {
    pub name: String,
    pub members: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roster: Vec<GroupMemberView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct GroupsResponse {
    pub groups: Vec<GroupView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct GroupActionRequest {
    pub action: String,
    pub group: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AgentsResponse {
    pub agents: Vec<AgentView>,
    /// The origin a harness points at for provider traffic, when the gateway is
    /// running. A client stores a key against it so the agent reaches the
    /// gateway on its next start.
    #[serde(default)]
    pub gateway: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct MessageView {
    pub id: String,
    pub seq: i64,
    pub text: String,
    pub sender: Option<String>,
    pub reply_to: Option<String>,
    pub source_time_ms: Option<i64>,
    pub received_at_ms: i64,
    /// `received` (committed to the mailbox) or `delivered` (the harness took it).
    pub ack: String,
    /// When the harness took it, when it has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InboxResponse {
    pub messages: Vec<MessageView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConversationTurnView {
    pub id: String,
    /// `"in"` (received by the agent) or `"out"` (sent by the agent).
    pub direction: String,
    pub from: String,
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub text: String,
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConversationResponse {
    pub agent: String,
    pub agent_id: String,
    pub turns: Vec<ConversationTurnView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendMedia {
    /// Media kind: `"photo"`, `"video"`, `"audio"`, `"music"`, `"voice"`, `"document"`, or `"animation"`.
    pub kind: String,
    /// Local file path, `https://` URL, or Telegram `file_id`.
    pub source: String,
    /// Optional track title (for `kind = "audio"` or `"music"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional artist/performer name (for `kind = "audio"` or `"music"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub performer: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendLocation {
    pub latitude: f64,
    pub longitude: f64,
    /// Optional venue title (when both `title` and `address` are set, sends a rich venue card).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional venue street address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendPoll {
    pub question: String,
    pub options: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_anonymous: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allows_multiple_answers: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendChecklist {
    /// Checklist title.
    pub title: String,
    /// Checklist items (`1..=30` task descriptions; prefix with `[x]` or `☑` for pre-checked items).
    pub tasks: Vec<String>,
    /// Optional Telegram Business Connection ID for native `sendChecklist`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub business_connection_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendArticle {
    /// Article title.
    pub title: String,
    /// Markdown or plain-text body to publish as a Telegram Telegraph (`telegra.ph`) Instant View article.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Optional external article URL (when already hosted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Optional author name shown on the Instant View header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_name: Option<String>,
}

pub fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    match Option::<OneOrMany>::deserialize(deserializer)? {
        None => Ok(Vec::new()),
        Some(OneOrMany::One(s)) => Ok(s
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_string)
            .collect()),
        Some(OneOrMany::Many(vec)) => Ok(vec),
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SendRequest {
    /// The principal this message is from. On the identity boundary the server
    /// already knows it from the capability, and this must agree or the call is
    /// refused; the operator names its own principal explicitly.
    #[serde(default)]
    pub sender: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub to: Vec<String>,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub reply_to: Option<String>,
    /// The message this one quotes, as the sender's channel sent it. Rendered
    /// into the stored body; a harness cannot forge a quoted author.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<Quote>,
    /// Optional media attachment (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<SendMedia>,
    /// Optional GPS location or venue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SendLocation>,
    /// Optional interactive poll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll: Option<SendPoll>,
    /// Optional interactive checklist (`title`, `tasks`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checklist: Option<SendChecklist>,
    /// Optional rich Instant View article (`title`, `content` or `url`, optional `author_name`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub article: Option<SendArticle>,
    /// Names one send, so a retry after a lost answer is the same delivery
    /// instead of a second one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Delivery {
    pub to: String,
    pub id: String,
    pub seq: i64,
    pub duplicate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SkippedDelivery {
    pub to: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendResponse {
    pub deliveries: Vec<Delivery>,
    #[serde(default)]
    pub skipped: Vec<SkippedDelivery>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct InteractRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    /// Channel action: `"react"` (set/clear emoji reaction), `"vote"` (cast a ballot on a poll),
    /// `"check"` / `"uncheck"` (toggle checklist items by 1-based index in `options`),
    /// `"edit"` (edit message text), `"delete"` (delete a message), `"pin"` (pin a message),
    /// `"unpin"` (unpin a message), or `"stop_poll"` / `"close_poll"` (close a poll).
    pub action: String,
    /// Target message: `anyagents:reply:msg_...` from the message header, `msg_...`,
    /// or `telegram:chat:<chat_id>:<message_id>`.
    pub target: String,
    /// Emoji reaction for `action = "react"` (e.g. `"👍"`, `"🔥"`, `"❤️"`, or `""` to clear).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    /// Selected poll option(s) for `action = "vote"` (1-based option numbers `"1"`, `"2"` or option text).
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub options: Vec<String>,
    /// New text for `action = "edit"`, or optional rationale/comment for `action = "vote"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InteractResponse {
    pub ok: bool,
    pub action: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}
