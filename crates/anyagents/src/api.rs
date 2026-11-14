//! JSON shapes shared by the unix-socket server and the MCP tools.

use anyagents_core::Quote;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AgentView {
    #[serde(default)]
    pub has_open_turn: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_turn_age_ms: Option<i64>,
    #[serde(default)]
    pub profile: Option<String>,
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Internal communication and discovery scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`).
    #[serde(default)]
    pub scope: String,
    /// Lifecycle mode (`"standing"` | `"task"`): `"standing"` stays on idle standby between assignments; `"task"` (default for sub-agents) is a single-mission freelancer closed (`archive`) by its parent upon completion.
    #[serde(default)]
    pub lifecycle: String,
    /// Structural relationship of this agent to the authenticated caller (`"self"`, `"parent"`, `"sibling"`, `"child"`, `"descendant"`, `"ancestor"`, `"peer"`, `"other"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship: Option<String>,
    /// Whether the authenticated caller is permitted to message this agent directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_message: Option<bool>,
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
    /// Public capability/charter summary (`description` column in SQLite, Layer 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Private operating instructions (`instructions` column in SQLite, Layer 2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    /// Assigned OS seat account name (e.g. `seat-atlas`, `seat-worker`, `seat-worker`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_user: Option<String>,
    /// Assigned OS seat numeric UID from the agent's `seat_launch` spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_uid: Option<u32>,
    /// Assigned OS seat numeric GID from the agent's `seat_launch` spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_gid: Option<u32>,
    /// Active self-scheduled wakes (`at`, `every`) and event watches (`watch`) owned by this agent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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

/// Body of `POST /v1/agents`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateAgentRequest {
    #[serde(alias = "agent", alias = "id", alias = "agent_id", alias = "target")]
    pub name: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "archetype",
        alias = "type",
        alias = "template",
        alias = "preset"
    )]
    pub role: Option<String>,
    /// Omitted HTTP harness retains the legacy `claude-code` default. An explicit
    /// empty string or `inherit` lets sub-agent defaults select the harness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "instruction",
        alias = "system_prompt"
    )]
    pub instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "allow_external"
    )]
    pub public: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
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
    /// Immutable creator ID; absent means operator-owned (including legacy groups).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_agent_id: Option<String>,
    #[serde(default)]
    pub public: bool,
    pub members: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roster: Vec<GroupMemberView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct GroupsResponse {
    pub groups: Vec<GroupView>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct GroupActionRequest {
    pub action: String,
    pub group: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "allow_external"
    )]
    pub public: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AgentsResponse {
    #[serde(default)]
    pub archived_count: usize,
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
pub struct RawCoordinateView {
    pub segment: String,
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
    pub seq: u64,
}
impl From<anyagents_core::raw_segment::RawCoordinate> for RawCoordinateView {
    fn from(c: anyagents_core::raw_segment::RawCoordinate) -> Self {
        Self {
            segment: c.segment.to_string_lossy().into(),
            offset: c.offset,
            length: c.length,
            sha256: c.sha256,
            seq: c.seq,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ConversationTurnView {
    #[serde(default)]
    pub seq: i64,
    pub id: String,
    /// Exact bytes for an explicit bounded raw-range read; `text` is a lossy preview.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_bytes: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<RawCoordinateView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<RawCoordinateView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// `"in"` (received by the agent) or `"out"` (sent by the agent).
    pub direction: String,
    pub from: String,
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub text: String,
    /// Null when the native historical event has no source timestamp.
    pub timestamp_ms: Option<i64>,
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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absent_if_null"
    )]
    pub title: Option<String>,
    /// Optional artist/performer name (for `kind = "audio"` or `"music"`).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absent_if_null"
    )]
    pub performer: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendLocation {
    pub latitude: f64,
    pub longitude: f64,
    /// Optional venue title (when both `title` and `address` are set, sends a rich venue card).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absent_if_null"
    )]
    pub title: Option<String>,
    /// Optional venue street address.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absent_if_null"
    )]
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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absent_if_null"
    )]
    pub business_connection_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SendArticle {
    /// Article title.
    pub title: String,
    /// Markdown or plain-text body to publish as a Telegram Telegraph (`telegra.ph`) Instant View article.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absent_if_null"
    )]
    pub content: Option<String>,
    /// Optional external article URL (when already hosted).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absent_if_null"
    )]
    pub url: Option<String>,
    /// Optional author name shown on the Instant View header.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absent_if_null"
    )]
    pub author_name: Option<String>,
}

/// An optional field written as `null`, `"null"`, `"undefined"`, `"none"` or
/// `""` is absent. Models fill unused optional fields with these, and a
/// `"null"` URL or object is never what they meant.
pub fn absent_if_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    absent_if(deserializer, is_null_word)
}

/// [`absent_if_null`] for a field where `""` means something (an emoji
/// reaction's `""` clears it).
pub fn absent_if_placeholder<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    absent_if(deserializer, |text| {
        !text.trim().is_empty() && is_null_word(text)
    })
}

fn absent_if<'de, D, T>(
    deserializer: D,
    absent: impl Fn(&str) -> bool,
) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) if absent(&text) => Ok(None),
        Some(value) => T::deserialize(value)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// Empty, or a word models write for "no value".
pub(crate) fn is_null_word(text: &str) -> bool {
    let text = text.trim();
    text.is_empty()
        || ["null", "undefined", "none", "nil"]
            .iter()
            .any(|word| text.eq_ignore_ascii_case(word))
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

impl SendRequest {
    /// A text-only closing token is a turn boundary, not an outbound message.
    pub(crate) fn is_turn_closing_sentinel(&self) -> bool {
        if self.media.is_some()
            || self.location.is_some()
            || self.poll.is_some()
            || self.checklist.is_some()
            || self.article.is_some()
            || self.quote.is_some()
        {
            return false;
        }
        let text = self.text.trim();
        let token = text.trim_matches(|c: char| {
            c.is_whitespace() || matches!(c, '.' | '。' | '!' | '！' | '?' | '？')
        });
        matches!(
            token.to_ascii_lowercase().as_str(),
            "no_reply" | "done" | "end" | "停止" | "no more" | "終了" | "final"
        ) || (!text.is_empty() && text.chars().all(|c| c == '。'))
    }
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

#[cfg(test)]
mod create_agent_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn description_and_instructions_remain_independent() {
        let request: CreateAgentRequest = serde_json::from_value(json!({
            "name": "worker",
            "description": "Public role",
            "instructions": "Private instructions"
        }))
        .unwrap();
        assert_eq!(request.description.as_deref(), Some("Public role"));
        assert_eq!(
            request.instructions.as_deref(),
            Some("Private instructions")
        );
        let serialized = serde_json::to_value(request).unwrap();
        assert_eq!(serialized["description"], "Public role");
        assert_eq!(serialized["instructions"], "Private instructions");
    }

    #[test]
    fn absent_instructions_are_omitted_for_older_daemons() {
        let request: CreateAgentRequest = serde_json::from_value(json!({
            "name": "worker", "description": "Legacy instructions"
        }))
        .unwrap();
        assert!(request.instructions.is_none());
        let serialized = serde_json::to_value(request).unwrap();
        assert!(serialized.get("instructions").is_none());

        // Older daemons treated description as an alias for instructions.
        #[derive(Deserialize)]
        struct LegacyRequest {
            #[serde(default, alias = "description")]
            instructions: Option<String>,
        }
        let legacy: LegacyRequest = serde_json::from_value(serialized).unwrap();
        assert_eq!(legacy.instructions.as_deref(), Some("Legacy instructions"));
    }

    #[test]
    fn archetype_aliases_deserialize_and_omit_when_absent() {
        for key in ["role", "archetype", "type", "template", "preset"] {
            let mut obj = serde_json::Map::new();
            obj.insert("name".into(), json!("worker"));
            obj.insert(key.into(), json!("writer"));
            let req: CreateAgentRequest =
                serde_json::from_value(serde_json::Value::Object(obj)).unwrap();
            assert_eq!(req.role.as_deref(), Some("writer"));
            assert!(req.harness.is_none());
        }
        let absent: CreateAgentRequest = serde_json::from_value(json!({
            "name": "worker"
        }))
        .unwrap();
        assert!(absent.role.is_none());
        let serialized = serde_json::to_value(absent).unwrap();
        assert!(serialized.get("role").is_none());
        assert!(serialized.get("archetype").is_none());
    }
}

/// End-exclusive coordinates in decoded body text, not serialized JSON bytes.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedactionSpan {
    /// `bytes` (UTF-8 boundaries required) or `chars` (Unicode scalar values).
    pub unit: String,
    pub start: usize,
    pub end: usize,
}

impl TryFrom<RedactionSpan> for anyagents_core::redaction::Span {
    type Error = anyagents_core::Error;
    fn try_from(span: RedactionSpan) -> Result<Self, Self::Error> {
        use anyagents_core::redaction::SpanUnit;
        let unit = match span.unit.as_str() {
            "bytes" => SpanUnit::Bytes,
            "chars" => SpanUnit::Chars,
            _ => {
                return Err(anyagents_core::Error::Message(
                    "span unit must be bytes or chars".into(),
                ));
            }
        };
        Ok(Self {
            unit,
            start: span.start,
            end: span.end,
        })
    }
}
