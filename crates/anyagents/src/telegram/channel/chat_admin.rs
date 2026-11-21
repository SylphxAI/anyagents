//! Change Telegram chat metadata using the caller's own bot.

#[path = "chat_admin/photo.rs"]
mod photo;

use anyagents_channel_telegram::{set_chat_description, set_chat_title};
use anyagents_core::{AgentId, ChannelId, Error, Store};

use super::{SHORT_TIMEOUT_S, channel_for, channel_token, transport};

pub(crate) use photo::Photo;

pub(crate) enum Change<'a> {
    Title(&'a str),
    Description(&'a str),
    Photo(Photo),
}

/// This credential can only be obtained for a non-isolated caller's own bot.
pub(crate) struct ChatAdmin {
    channel: ChannelId,
    token: String,
}

impl ChatAdmin {
    pub(crate) fn for_agent(
        store: &Store,
        caller: &AgentId,
        bot: Option<&str>,
    ) -> Result<Self, Error> {
        let agent = store
            .agent(caller)
            .map_err(|_| Error::PermissionDenied("authenticated agent no longer exists".into()))?;
        if agent.is_isolated() {
            return Err(Error::PermissionDenied(
                "isolated agents cannot administer external Telegram chats".into(),
            ));
        }
        let channel = channel_for(store, caller.as_str(), bot)?;
        let token = channel_token(store, store.data_dir(), &channel)?;
        Ok(Self {
            channel: channel.id,
            token,
        })
    }

    pub(crate) fn apply(self, chat_id: i64, change: Change<'_>) -> Result<(), Error> {
        let call = match change {
            Change::Title(title) => set_chat_title(chat_id, title),
            Change::Description(description) => set_chat_description(chat_id, description),
            Change::Photo(photo) => {
                transport::set_chat_photo(&self.token, &self.channel, chat_id, &photo.upload)?;
                return Ok(());
            }
        };
        transport::call_api(&self.token, &call, SHORT_TIMEOUT_S, Some(&self.channel))?;
        Ok(())
    }
}
