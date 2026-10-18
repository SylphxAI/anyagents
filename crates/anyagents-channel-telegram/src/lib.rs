//! One Telegram bot per agent, reached by long-polling `getUpdates`.
//!
//! A bot does not reliably receive messages that other bots send, so a post an
//! agent makes is also handed to every other agent on the same chat by the
//! server. If Telegram later delivers that same post, the copy is dropped by
//! its outbound message id. The sender does not receive its own post.
//!
//! The token never leaves the caller. This crate only sees update payloads.

use serde::{Deserialize, Serialize};

/// One inbound event, already parsed out of an update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inbound {
    /// `bot id : update id`. The same update delivered twice is one event.
    pub source_key: String,
    pub chat_id: i64,
    pub message_id: i64,
    pub text: String,
    /// Telegram's `date`, seconds. Metadata only; it never orders delivery.
    pub source_time_s: i64,
}

/// What the server does with one accepted update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Routed {
    /// Deliver into this one agent's mailbox.
    ToAgent { agent: String, inbound: Inbound },
    /// A post this server sent. Deliver to every other agent on the chat.
    EchoToPeers {
        chat_id: i64,
        outbound_id: i64,
        text: String,
    },
    /// Already seen: the same update, or our own post coming back.
    Dropped,
}

#[derive(Clone, Debug, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<TgMessage>,
}

#[derive(Clone, Debug, Deserialize)]
struct TgMessage {
    message_id: i64,
    chat: TgChat,
    date: i64,
    text: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct TgChat {
    id: i64,
}

/// A post this server sent through a bot, remembered so the echo can be dropped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundRecord {
    pub chat_id: i64,
    pub message_id: i64,
    pub text: String,
    pub sender_agent: String,
}

/// One bot's poll state.
#[derive(Default)]
pub struct Poll {
    /// The highest update id confirmed, so the next poll starts after it.
    pub offset: i64,
    seen: Vec<String>,
    outbound: Vec<OutboundRecord>,
}

impl Poll {
    /// Remember a post this server sent, so its echo is not delivered twice.
    pub fn sent(&mut self, record: OutboundRecord) {
        self.outbound.push(record);
    }

    /// Route one `getUpdates` payload for `bot` owned by `agent`.
    pub fn route(
        &mut self,
        bot: &str,
        agent: &str,
        payload: &str,
    ) -> Result<Vec<Routed>, PollError> {
        let updates: Vec<Update> =
            serde_json::from_str(payload).map_err(|e| PollError::BadPayload(e.to_string()))?;
        let mut out = Vec::new();
        for update in updates {
            if update.update_id >= self.offset {
                self.offset = update.update_id + 1;
            }
            let Some(message) = update.message else {
                continue;
            };
            let key = format!("{bot}:{}", update.update_id);
            if self.seen.iter().any(|k| k == &key) {
                out.push(Routed::Dropped);
                continue;
            }
            self.seen.push(key.clone());
            if let Some(record) = self
                .outbound
                .iter()
                .find(|r| r.chat_id == message.chat.id && r.message_id == message.message_id)
            {
                out.push(Routed::EchoToPeers {
                    chat_id: record.chat_id,
                    outbound_id: record.message_id,
                    text: record.text.clone(),
                });
                continue;
            }
            out.push(Routed::ToAgent {
                agent: agent.to_string(),
                inbound: Inbound {
                    source_key: key,
                    chat_id: message.chat.id,
                    message_id: message.message_id,
                    text: message.text.unwrap_or_default(),
                    source_time_s: message.date,
                },
            });
        }
        Ok(out)
    }

    /// The other agents on `chat` that should see a post `sender` made.
    pub fn peers<'a>(&self, chat: i64, sender: &str, attached: &'a [(&str, i64)]) -> Vec<&'a str> {
        attached
            .iter()
            .filter(|(agent, chat_id)| *chat_id == chat && *agent != sender)
            .map(|(agent, _)| *agent)
            .collect()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PollError {
    #[error("telegram payload: {0}")]
    BadPayload(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(update_id: i64, chat: i64, message_id: i64, text: &str) -> String {
        format!(
            r#"[{{"update_id":{update_id},"message":{{"message_id":{message_id},"chat":{{"id":{chat}}},"date":1700000000,"text":"{text}"}}}}]"#
        )
    }

    #[test]
    fn the_same_update_is_delivered_once() {
        let mut poll = Poll::default();
        let body = payload(7, 100, 5, "hello");
        let first = poll.route("bot_a", "ada", &body).unwrap();
        let second = poll.route("bot_a", "ada", &body).unwrap();
        assert!(matches!(first[0], Routed::ToAgent { .. }));
        assert_eq!(second, vec![Routed::Dropped]);
        assert_eq!(poll.offset, 8);
    }

    #[test]
    fn two_bots_keep_their_own_update_ids() {
        let mut poll = Poll::default();
        let body = payload(7, 100, 5, "hello");
        poll.route("bot_a", "ada", &body).unwrap();
        let other = poll.route("bot_b", "bea", &body).unwrap();
        assert!(matches!(other[0], Routed::ToAgent { .. }));
    }

    #[test]
    fn our_own_post_coming_back_goes_to_peers_not_the_sender() {
        let mut poll = Poll::default();
        poll.sent(OutboundRecord {
            chat_id: 100,
            message_id: 9,
            text: "from ada".to_string(),
            sender_agent: "ada".to_string(),
        });
        let routed = poll
            .route("bot_a", "ada", &payload(3, 100, 9, "from ada"))
            .unwrap();
        assert_eq!(
            routed,
            vec![Routed::EchoToPeers {
                chat_id: 100,
                outbound_id: 9,
                text: "from ada".to_string(),
            }]
        );
        let attached = [("ada", 100), ("bea", 100), ("cy", 200)];
        assert_eq!(poll.peers(100, "ada", &attached), vec!["bea"]);
    }

    #[test]
    fn a_bad_payload_is_refused_without_moving_the_offset() {
        let mut poll = Poll::default();
        assert!(poll.route("bot_a", "ada", "not json").is_err());
        assert_eq!(poll.offset, 0);
    }
}
