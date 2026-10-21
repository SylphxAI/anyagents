//! Telegram: one bot per agent, polled for updates and posted to through the
//! Bot API. The transport is long polling, because a local server usually has
//! no public URL. A token is read from the credential store only when a call is
//! made, and it stays in memory.

mod channel;
pub mod poller;

pub(crate) use channel::{bound_sender, outbound_call};
pub use channel::{post_text, record_chat};
pub use poller::Poller;
