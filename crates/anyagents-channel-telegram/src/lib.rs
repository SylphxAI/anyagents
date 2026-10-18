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

    /// The next `getUpdates` call: long-poll, starting after the last confirmed id.
    ///
    /// `timeout` is seconds. The token is not part of this request; the caller
    /// attaches it when the request leaves the process.
    pub fn get_updates(&self, timeout_s: u64) -> BotCall {
        BotCall {
            method: "getUpdates".to_string(),
            body: serde_json::json!({
                "offset": self.offset,
                "timeout": timeout_s,
                "allowed_updates": ["message"],
            }),
        }
    }
}

/// One Bot API call. The body is the JSON object; the token stays with the caller.
#[derive(Clone, Debug, PartialEq)]
pub struct BotCall {
    pub method: String,
    pub body: serde_json::Value,
}

/// `sendMessage` for one chat. `reply_to` is a Telegram message id, when the
/// agent was answering one.
pub fn send_message(chat_id: i64, text: &str, reply_to: Option<i64>) -> BotCall {
    let mut body = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
    });
    if let Some(id) = reply_to {
        body["reply_to_message_id"] = serde_json::json!(id);
    }
    BotCall {
        method: "sendMessage".to_string(),
        body,
    }
}

/// The message id Telegram assigned to a `sendMessage` we made.
pub fn sent_message_id(response: &str) -> Result<i64, PollError> {
    let value: serde_json::Value =
        serde_json::from_str(response).map_err(|e| PollError::BadPayload(e.to_string()))?;
    value
        .pointer("/result/message_id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| PollError::BadPayload("no result.message_id".to_string()))
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PollError {
    #[error("telegram payload: {0}")]
    BadPayload(String),
}

/// Write one routed update into the mailbox. A duplicate source key is kept as
/// the row already stored. Peers of a post this server sent each get a copy,
/// and the sender does not.
pub fn deliver_to_mailbox(
    store: &anyagents_core::Store,
    routed: &Routed,
    peers: &[(anyagents_core::AgentId, i64)],
) -> Result<(), anyagents_core::Error> {
    match routed {
        Routed::Dropped => Ok(()),
        Routed::ToAgent { agent, inbound } => {
            let agent_id = anyagents_core::AgentId::parse(agent)?;
            store.receive(&agent_id, incoming_from(inbound))?;
            Ok(())
        }
        Routed::EchoToPeers {
            chat_id,
            outbound_id,
            text,
        } => {
            for (agent_id, chat) in peers {
                if chat != chat_id {
                    continue;
                }
                let mut incoming = anyagents_core::Incoming::new(text.clone());
                incoming.source_key = Some(format!("echo:{chat_id}:{outbound_id}"));
                store.receive(agent_id, incoming)?;
            }
            Ok(())
        }
    }
}

fn incoming_from(inbound: &Inbound) -> anyagents_core::Incoming {
    anyagents_core::Incoming {
        text: inbound.text.clone(),
        sender: None,
        source_time_ms: Some(inbound.source_time_s * 1000),
        source_key: Some(inbound.source_key.clone()),
        reply_to: None,
        received_at_ms: None,
    }
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

    #[test]
    fn a_routed_update_lands_in_the_mailbox_once() {
        let dir = tempfile_dir();
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let mut poll = Poll::default();
        let routed = poll
            .route("bot_a", agent.id.as_str(), &payload(4, 100, 2, "hi"))
            .unwrap();
        deliver_to_mailbox(&store, &routed[0], &[]).unwrap();
        deliver_to_mailbox(&store, &routed[0], &[]).unwrap();
        let inbox = store.inbox(&agent.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "hi");
        assert_eq!(inbox[0].source_key.as_deref(), Some("bot_a:4"));
    }

    #[test]
    fn get_updates_starts_after_the_last_confirmed_id() {
        let mut poll = Poll::default();
        poll.route("bot_a", "ada", &payload(7, 100, 5, "hello"))
            .unwrap();
        let call = poll.get_updates(25);
        assert_eq!(call.method, "getUpdates");
        assert_eq!(call.body["offset"], 8);
        assert_eq!(call.body["timeout"], 25);
        assert!(!call.body.to_string().contains("token"));
    }

    #[test]
    fn send_message_names_the_chat_and_keeps_the_token_out() {
        let call = send_message(100, "hello", Some(5));
        assert_eq!(call.method, "sendMessage");
        assert_eq!(call.body["chat_id"], 100);
        assert_eq!(call.body["text"], "hello");
        assert_eq!(call.body["reply_to_message_id"], 5);
        let id = sent_message_id(r#"{"ok":true,"result":{"message_id":9}}"#).unwrap();
        assert_eq!(id, 9);
    }

    #[test]
    fn a_post_we_sent_is_relayed_once_and_not_to_the_sender() {
        let dir = tempfile_dir();
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        let ada = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let bea = store
            .create_agent(anyagents_core::NewAgent::new("bea", "codex"))
            .unwrap();
        let mut poll = Poll::default();
        poll.sent(OutboundRecord {
            chat_id: 100,
            message_id: 9,
            text: "from ada".to_string(),
            sender_agent: ada.id.as_str().to_string(),
        });
        let routed = poll
            .route("bot_a", ada.id.as_str(), &payload(3, 100, 9, "from ada"))
            .unwrap();
        let peers = [(bea.id.clone(), 100)];
        deliver_to_mailbox(&store, &routed[0], &peers).unwrap();
        assert!(store.inbox(&ada.id, None).unwrap().is_empty());
        let bea_inbox = store.inbox(&bea.id, None).unwrap();
        assert_eq!(bea_inbox.len(), 1);
        assert_eq!(bea_inbox[0].source_key.as_deref(), Some("echo:100:9"));
        deliver_to_mailbox(&store, &routed[0], &peers).unwrap();
        assert_eq!(store.inbox(&bea.id, None).unwrap().len(), 1);
    }

    fn tempfile_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }
}
