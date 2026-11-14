//! Telegram: one bot per agent, driven by webhook push when a public webhook
//! origin is configured or by long polling through the Bot API. A token is
//! read from the credential store only when a call is made, and it stays in
//! memory.

mod channel;
pub mod poller;

#[allow(unused_imports)]
pub use channel::{
    MediaUpload, WebhookError, bind_agent_telegram_token, chat_member_is_in, chat_member_status,
    close_poll, delete_chat_message, edit_message, format_telegram_bot_display_name,
    is_local_media, pin_message, post_article, post_checklist, post_location, post_media,
    post_poll, post_text, react_message, receive_webhook, reconcile_channel_groups, record_chat,
    request_or_bind_telegram_channel, sync_agent_telegram_bot_name,
};
pub(crate) use channel::{detach_archived_agent_from_telegram_groups, outbound_call};
pub use poller::Poller;
