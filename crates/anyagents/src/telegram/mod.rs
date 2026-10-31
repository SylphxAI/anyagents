//! Telegram: one bot per agent, driven by webhook push when a public webhook
//! origin is configured or by long polling through the Bot API. A token is
//! read from the credential store only when a call is made, and it stays in
//! memory.

mod channel;
pub mod poller;

pub(crate) use channel::outbound_call;
pub use channel::{
    WebhookError, chat_member_is_in, close_poll, delete_chat_message, edit_message, pin_message,
    post_article, post_checklist, post_location, post_media, post_poll, post_text, react_message,
    receive_webhook, record_chat, sync_agent_telegram_bot_name,
};
pub use poller::Poller;
