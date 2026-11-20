//! Unified send tool input schema.

use super::*;

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(in crate::mcp) struct SendArgs {
    /// Destinations: agent names or IDs (`planner`, `agent:planner`, `agt_...`, `parent`,
    /// `children`), internal groups (`group:owners`, `group:<name>`, `group:all`, `all`),
    /// or external channel targets (`telegram:chat:<chat_id>[:<topic_id>]`,
    /// `telegram:user:<user_id>[:<message_id>]`). A send never wakes an archived
    /// agent: one addressed only to archived agents fails with `409`; wake it
    /// first with `agents(action="wake")`. May be
    /// empty (`[]`) when `reply_to` names a mailbox reference (`anyagents:reply:<msg_id>`)
    /// or when performing a message interaction (`action`: `"react"` | `"vote"` | `"edit"` | `"delete"` | `"pin"` | `"unpin"` | `"stop_poll"`).
    #[serde(default, deserialize_with = "deserialize_send_targets")]
    pub(in crate::mcp) to: Vec<String>,
    /// Message text or caption, edit text, vote comment, or the new chat title (1–128 chars)
    /// or description (0–255 chars; use an explicit empty string to clear it).
    #[serde(default)]
    pub(in crate::mcp) text: Option<String>,
    /// Optional reply or interaction target: `anyagents:reply:<msg_id>` from the message header
    /// (answers or reacts in the same chat/topic/agent), or a plain destination address.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) reply_to: Option<String>,
    /// Optional quoted substring or message text to attach as a quote.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) quote: Option<String>,
    /// Optional media attachment (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`)
    /// by local file path, `https://` URL, or Telegram `file_id` (plus optional `title` and `performer` for `music`/`audio`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) media: Option<SendMedia>,
    /// Optional GPS coordinates or venue (`latitude`, `longitude`, optional `title` and `address`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) location: Option<SendLocation>,
    /// Optional interactive poll (`question`, `options`, optional `is_anonymous` and `allows_multiple_answers`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) poll: Option<SendPoll>,
    /// Optional interactive checklist (`title`, `tasks`, optional `business_connection_id`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) checklist: Option<SendChecklist>,
    /// Optional rich Instant View article (`title`, `content` or `url`, optional `author_name`).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) article: Option<SendArticle>,
    /// Message action (send, react, vote, check, uncheck, edit, delete, pin, unpin, stop_poll)
    /// or Telegram chat action (set_chat_title, set_chat_description, set_chat_photo).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    #[schemars(schema_with = "action_schema")]
    pub(in crate::mcp) action: Option<String>,
    /// Target message for interactions (`anyagents:reply:msg_...`, `msg_...`, or `telegram:chat:<chat_id>:<message_id>`).
    /// Chat actions take exactly one bare Telegram chat address in target or to.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) target: Option<String>,
    /// Emoji reaction when `action = "react"` (e.g. `"👍"`, `"🔥"`, `"❤️"`, `"🎉"`, or `""` to clear).
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) emoji: Option<String>,
    /// Selected poll option(s) when `action = "vote"` (1-based option numbers `"1"`, `"2"` or option text)
    /// or checklist item numbers when `action = "check" | "uncheck"`.
    #[serde(default, deserialize_with = "deserialize_send_targets")]
    pub(in crate::mcp) options: Vec<String>,
    /// Optional own channel bot to post or act through (`@username` or channel id),
    /// needed only when this agent has more than one bot. Unknown bots and other
    /// agents' bots are refused.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) bot: Option<String>,
    /// Names this send. Send it again with the same value only when an earlier
    /// attempt got no answer.
    #[serde(default, deserialize_with = "crate::api::absent_if_null")]
    pub(in crate::mcp) idempotency_key: Option<String>,
}

// Advertise canonical actions while retaining the existing trim/case normalization
// and legacy action aliases in the handler.
fn action_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "enum": [null, "send", "react", "vote", "check", "uncheck", "edit", "delete",
                 "pin", "unpin", "stop_poll", "close_poll", "set_chat_title",
                 "set_chat_description", "set_chat_photo"]
    })
}
