//! Telegram Bot API calls for chat and message administration.

use super::BotCall;

/// `pinChatMessage` to pin a message in a chat.
pub fn pin_chat_message(
    chat_id: i64,
    message_id: i64,
    disable_notification: Option<bool>,
) -> BotCall {
    let mut body = serde_json::json!({
        "chat_id": chat_id,
        "message_id": message_id,
    });
    if let Some(silent) = disable_notification {
        body["disable_notification"] = serde_json::json!(silent);
    }
    BotCall {
        method: "pinChatMessage".to_string(),
        body,
    }
}

/// `unpinChatMessage` to unpin a message in a chat.
pub fn unpin_chat_message(chat_id: i64, message_id: i64) -> BotCall {
    BotCall {
        method: "unpinChatMessage".to_string(),
        body: serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
        }),
    }
}

/// `stopPoll` to close an active poll message.
pub fn stop_poll(chat_id: i64, message_id: i64) -> BotCall {
    BotCall {
        method: "stopPoll".to_string(),
        body: serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
        }),
    }
}

/// Set the title of a group, supergroup, or channel.
pub fn set_chat_title(chat_id: i64, title: &str) -> BotCall {
    BotCall {
        method: "setChatTitle".into(),
        body: serde_json::json!({ "chat_id": chat_id, "title": title }),
    }
}

/// Set or clear the description of a group, supergroup, or channel.
pub fn set_chat_description(chat_id: i64, description: &str) -> BotCall {
    BotCall {
        method: "setChatDescription".into(),
        body: serde_json::json!({ "chat_id": chat_id, "description": description }),
    }
}
