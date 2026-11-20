//! Request and response for message and Telegram chat interactions.

use super::*;

pub(crate) fn is_telegram_chat_admin_action(action: &str) -> bool {
    matches!(
        action,
        "set_chat_title" | "set_chat_description" | "set_chat_photo"
    )
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct InteractRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    /// Message interaction or Telegram chat administration action (see the send tool).
    pub action: String,
    /// Message reference, or `telegram:chat:<chat_id>` for chat administration.
    pub target: String,
    /// Emoji reaction for `action = "react"` (e.g. `"👍"`, `"🔥"`, `"❤️"`, or `""` to clear).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    /// Selected poll option(s) for `action = "vote"` (1-based option numbers `"1"`, `"2"` or option text).
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub options: Vec<String>,
    /// New message or chat metadata text, or optional comment for `vote`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// JPEG or PNG upload for set_chat_photo only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<SendMedia>,
    /// The sender's own channel bot to act through (`@username` or channel id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InteractResponse {
    pub ok: bool,
    pub action: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}
