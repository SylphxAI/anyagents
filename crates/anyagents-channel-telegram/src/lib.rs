//! One Telegram bot per agent, reached by long-polling `getUpdates`.
//!
//! An inbound update becomes one mailbox message with the chat it came from,
//! the Telegram message id to reply to, and the forum topic it belongs to. A
//! bot does not reliably receive messages that other bots send, so a post an
//! agent makes is also handed to every other agent on the same chat by the
//! server, keyed by its outbound message id. If Telegram later delivers that
//! same post, the copy is dropped by its source key. The sender does not
//! receive its own post.
//!
//! The token never leaves the caller. This crate only sees update payloads and
//! the calls the server should make.

use serde::{Deserialize, Serialize};

/// The Bot API. A webhook is an option for a server with a public URL; a
/// server on a private network only long-polls, so that is the one transport
/// here.
pub const API: &str = "https://api.telegram.org";

/// One inbound event, already parsed out of an update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inbound {
    /// `bot id : update id`. The same update delivered twice is one event.
    pub source_key: String,
    /// The chat the message belongs to: a private chat, a group, or a forum
    /// supergroup (a forum topic is not a chat of its own).
    pub chat_id: i64,
    /// The chat's kind: `private`, `group`, `supergroup` or `channel`. Only a
    /// group is a room.
    pub chat_type: String,
    /// The `message_thread_id` of a forum topic. `None` outside a topic.
    pub thread_id: Option<i64>,
    /// The Telegram message id, which a reply names.
    pub message_id: i64,
    /// The person who wrote it, as Telegram names them.
    pub sender_name: Option<String>,
    /// The id of the person who wrote it, when Telegram sends one.
    pub sender_id: Option<i64>,
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
    /// This bot joined or left a chat: record the membership, deliver nothing.
    Membership {
        agent: String,
        chat_id: i64,
        chat_type: Option<String>,
        joined: bool,
    },
    /// Already seen: the same update, or our own post coming back.
    Dropped,
}

#[derive(Clone, Debug, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<TgMessage>,
    /// The bot's own membership in a chat changed. This is how a bot learns it
    /// was added to, or removed from, a group.
    my_chat_member: Option<ChatMemberUpdate>,
}

#[derive(Clone, Debug, Deserialize)]
struct ChatMemberUpdate {
    chat: TgChat,
    new_chat_member: ChatMember,
}

#[derive(Clone, Debug, Deserialize)]
struct ChatMember {
    status: String,
}

#[derive(Clone, Debug, Deserialize)]
struct TgMessage {
    message_id: i64,
    chat: TgChat,
    date: i64,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    caption: Option<String>,
    #[serde(default)]
    message_thread_id: Option<i64>,
    #[serde(default)]
    from: Option<TgUser>,
}

impl TgMessage {
    /// The text of a typed message, else the caption of a media message.
    fn body(&self) -> Option<&str> {
        self.text
            .as_deref()
            .or(self.caption.as_deref())
            .filter(|text| !text.is_empty())
    }
}

#[derive(Clone, Debug, Deserialize)]
struct TgChat {
    id: i64,
    /// `private`, `group`, `supergroup` or `channel`.
    #[serde(default, rename = "type")]
    kind: String,
}

#[derive(Clone, Debug, Deserialize)]
struct TgUser {
    id: i64,
    #[serde(default)]
    first_name: Option<String>,
    #[serde(default)]
    username: Option<String>,
}

impl TgUser {
    fn display_name(&self) -> String {
        match (&self.first_name, &self.username) {
            (Some(name), Some(username)) => format!("{name} (@{username})"),
            (Some(name), None) => name.clone(),
            (None, Some(username)) => format!("@{username}"),
            (None, None) => self.id.to_string(),
        }
    }
}

/// What one bot remembers between runs, stored as the channel's `state` JSON.
/// It holds no secret: the bot is named by its id and user name, never by token.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memory {
    /// The next update id to ask for. Only moves once the updates it covers
    /// are in the mailbox.
    #[serde(default)]
    pub offset: i64,
    /// The bot `getMe` last confirmed, as `id:username`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot: Option<String>,
    /// The Telegram chats each agent answers in, and each chat's title.
    #[serde(default)]
    pub chats: Vec<ChatLink>,
}

/// One chat an agent answers in.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatLink {
    pub agent: String,
    pub chat_id: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Telegram's chat type: `private`, `group`, `supergroup` or `channel`.
    /// Only a group is a room; a private chat is one person and one agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_type: Option<String>,
}

impl Memory {
    /// Read a channel `state` value. A missing or unreadable state starts empty.
    pub fn from_state(state: &str) -> Self {
        serde_json::from_str(state).unwrap_or_default()
    }

    pub fn to_state(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Record that `agent` answers in `chat`, keeping a title already learned.
    pub fn attach(&mut self, agent: &str, chat_id: i64) {
        self.attach_typed(agent, chat_id, None)
    }

    /// Attach an agent to a chat, keeping the chat's kind so a private chat is
    /// never treated as a room.
    pub fn attach_typed(&mut self, agent: &str, chat_id: i64, chat_type: Option<&str>) {
        if let Some(link) = self
            .chats
            .iter_mut()
            .find(|link| link.agent == agent && link.chat_id == chat_id)
        {
            if link.chat_type.is_none() {
                link.chat_type = chat_type.map(str::to_string);
            }
            return;
        }
        self.chats.push(ChatLink {
            agent: agent.to_string(),
            chat_id,
            title: None,
            chat_type: chat_type.map(str::to_string),
        });
    }

    /// Forget that this agent answers in a chat.
    pub fn detach(&mut self, agent: &str, chat_id: i64) {
        self.chats
            .retain(|link| !(link.agent == agent && link.chat_id == chat_id));
    }

    /// Whether this agent is known to answer in this chat.
    pub fn is_attached(&self, agent: &str, chat_id: i64) -> bool {
        self.chats
            .iter()
            .any(|link| link.agent == agent && link.chat_id == chat_id)
    }

    /// Whether this chat is a room: a group or a supergroup. A private chat is
    /// one person and one agent, so nothing fans out from it.
    pub fn is_room(&self, chat_id: i64) -> bool {
        self.chats
            .iter()
            .filter(|link| link.chat_id == chat_id)
            .filter_map(|link| link.chat_type.as_deref())
            .any(|kind| kind == "group" || kind == "supergroup")
    }

    /// The title of `chat`, when this bot has learned it.
    pub fn title_of(&self, chat_id: i64) -> Option<&str> {
        self.chats
            .iter()
            .find(|link| link.chat_id == chat_id)?
            .title
            .as_deref()
    }

    /// The other agents on `chat`, which see a post `sender` made there.
    pub fn peers(&self, chat_id: i64, sender: &str) -> Vec<&str> {
        let mut peers: Vec<&str> = self
            .chats
            .iter()
            .filter(|link| link.chat_id == chat_id && link.agent != sender)
            .map(|link| link.agent.as_str())
            .collect();
        peers.sort_unstable();
        peers.dedup();
        peers
    }
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
    /// A poll that starts after `offset`, as it was saved before a restart.
    pub fn resume(offset: i64) -> Self {
        Self {
            offset,
            ..Self::default()
        }
    }

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
            // A membership change is how a bot learns it is in, or out of, a
            // group. It carries no message and is recorded, not delivered.
            if let Some(member) = update.my_chat_member {
                let joined = matches!(
                    member.new_chat_member.status.as_str(),
                    "member" | "administrator" | "creator"
                );
                out.push(Routed::Membership {
                    agent: agent.to_string(),
                    chat_id: member.chat.id,
                    chat_type: Some(member.chat.kind.clone()),
                    joined,
                });
                continue;
            }
            let Some(message) = update.message else {
                // An edited message, a membership change, a callback: nothing
                // for a mailbox.
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
            let Some(text) = message.body() else {
                // A sticker, a photo with no caption, a location: recorded as
                // seen so it is not re-routed, and not delivered.
                continue;
            };
            out.push(Routed::ToAgent {
                agent: agent.to_string(),
                inbound: Inbound {
                    source_key: key,
                    chat_id: message.chat.id,
                    chat_type: message.chat.kind.clone(),
                    thread_id: message.message_thread_id,
                    message_id: message.message_id,
                    sender_name: message.from.as_ref().map(TgUser::display_name),
                    sender_id: message.from.as_ref().map(|user| user.id),
                    text: text.to_string(),
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
                "allowed_updates": ["message", "my_chat_member"],
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
/// agent was answering one. `thread` keeps the post in its forum topic.
pub fn send_message(
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> BotCall {
    let mut body = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
    });
    if let Some(id) = reply_to {
        body["reply_to_message_id"] = serde_json::json!(id);
    }
    if let Some(id) = thread {
        body["message_thread_id"] = serde_json::json!(id);
    }
    BotCall {
        method: "sendMessage".to_string(),
        body,
    }
}

/// `getMe`: the bot's own id and user name, so a recorded token is shown as the
/// bot it is before any message is posted with it.
pub fn get_me() -> BotCall {
    BotCall {
        method: "getMe".to_string(),
        body: serde_json::json!({}),
    }
}

/// The bot's id and user name from a `getMe` response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BotIdentity {
    pub id: i64,
    pub username: String,
}

pub fn bot_identity(response: &str) -> Result<BotIdentity, PollError> {
    let value = ok_result(response)?;
    let id = value
        .get("id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| PollError::BadPayload("no result.id".to_string()))?;
    let username = value
        .get("username")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| PollError::BadPayload("no result.username".to_string()))?
        .to_string();
    Ok(BotIdentity { id, username })
}

/// The message id Telegram assigned to a `sendMessage` we made.
pub fn sent_message_id(response: &str) -> Result<i64, PollError> {
    ok_result(response)?
        .get("message_id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| PollError::BadPayload("no result.message_id".to_string()))
}

/// `result` of a Bot API answer, which must say `ok: true`.
fn ok_result(response: &str) -> Result<serde_json::Value, PollError> {
    let value: serde_json::Value =
        serde_json::from_str(response).map_err(|e| PollError::BadPayload(e.to_string()))?;
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let description = value
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("telegram refused the call");
        return Err(PollError::Refused(description.to_string()));
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| PollError::BadPayload("no result".to_string()))
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PollError {
    #[error("telegram payload: {0}")]
    BadPayload(String),
    /// Telegram answered `ok: false` and said why.
    #[error("telegram refused: {0}")]
    Refused(String),
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
        // Membership is recorded by the caller, which owns the channel state.
        Routed::Membership { .. } => Ok(()),
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

/// The mailbox row for one inbound message: the reply target is the message
/// itself, so a bare reply goes to the chat and, in a forum, to its topic.
fn incoming_from(inbound: &Inbound) -> anyagents_core::Incoming {
    anyagents_core::Incoming {
        text: inbound.text.clone(),
        sender: telegram_sender(inbound),
        source_time_ms: Some(inbound.source_time_s * 1000),
        source_key: Some(inbound.source_key.clone()),
        reply_to: Some(reply_target(inbound)),
        received_at_ms: None,
    }
}

/// `telegram:chat:<id>` or `telegram:chat:<id>:<thread>`, which the send tool
/// accepts as an address.
pub fn reply_target(inbound: &Inbound) -> String {
    match inbound.thread_id {
        Some(thread) => format!("telegram:chat:{}:{thread}", inbound.chat_id),
        None => format!("telegram:chat:{}", inbound.chat_id),
    }
}

/// `telegram:user:<id>:<message id>` when Telegram names the writer, else the
/// chat target. The message id names what a reply answers.
pub fn telegram_sender(inbound: &Inbound) -> Option<String> {
    inbound
        .sender_id
        .map(|id| format!("telegram:user:{id}:{}", inbound.message_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(update_id: i64, chat: i64, message_id: i64, text: &str) -> String {
        format!(
            r#"[{{"update_id":{update_id},"message":{{"message_id":{message_id},"chat":{{"id":{chat}}},"date":1700000000,"text":"{text}"}}}}]"#
        )
    }

    fn group_update(update_id: i64, chat: i64, message_id: i64, thread: i64, text: &str) -> String {
        format!(
            r#"[{{"update_id":{update_id},"message":{{"message_id":{message_id},"message_thread_id":{thread},"is_topic_message":true,"chat":{{"id":{chat},"type":"supergroup","is_forum":true}},"from":{{"id":42,"is_bot":false,"first_name":"Kai","username":"kai"}},"date":1700000000,"text":"{text}"}}}}]"#
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
        assert_eq!(inbox[0].reply_to.as_deref(), Some("telegram:chat:100"));
    }

    #[test]
    fn a_group_message_keeps_its_chat_and_topic_and_can_be_answered_there() {
        let dir = tempfile_dir();
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let mut poll = Poll::default();
        let routed = poll
            .route(
                "bot_a",
                agent.id.as_str(),
                &group_update(11, -1007, 55, 3, "deploy?"),
            )
            .unwrap();
        let Routed::ToAgent { inbound, .. } = &routed[0] else {
            panic!("{routed:?}");
        };
        assert_eq!(inbound.chat_id, -1007);
        assert_eq!(inbound.thread_id, Some(3));
        assert_eq!(inbound.message_id, 55);
        assert_eq!(inbound.sender_name.as_deref(), Some("Kai (@kai)"));
        deliver_to_mailbox(&store, &routed[0], &[]).unwrap();
        let inbox = store.inbox(&agent.id, None).unwrap();
        assert_eq!(
            inbox[0].reply_to.as_deref(),
            Some("telegram:chat:-1007:3"),
            "a reply goes back to the topic"
        );
        assert_eq!(inbox[0].sender.as_deref(), Some("telegram:user:42:55"));
        let answer = send_message(-1007, "yes", Some(55), inbound.thread_id);
        assert_eq!(answer.body["chat_id"], -1007);
        assert_eq!(answer.body["reply_to_message_id"], 55);
        assert_eq!(answer.body["message_thread_id"], 3);
    }

    #[test]
    fn a_reply_in_telegram_is_delivered_as_new_text_that_answers_itself() {
        // `reply_to_message` is Telegram quoting an older message. The mailbox
        // reply target is the message the agent received, so the quoted one is
        // not needed, and a quote of a long message is not copied in.
        let update = r#"[{"update_id":20,"message":{"message_id":61,"chat":{"id":100},"from":{"id":42,"first_name":"Kai"},"reply_to_message":{"message_id":60,"chat":{"id":100},"date":1700000000,"text":"earlier"},"date":1700000001,"text":"that one"}}]"#;
        let mut poll = Poll::default();
        let routed = poll.route("bot_a", "ada", update).unwrap();
        let Routed::ToAgent { inbound, .. } = &routed[0] else {
            panic!("{routed:?}");
        };
        assert_eq!(inbound.message_id, 61);
        assert_eq!(inbound.text, "that one");
        // The message itself is the reply target; the quoted one is Telegram's
        // own reference and is not needed to answer.
        assert_eq!(reply_target(inbound), "telegram:chat:100");
    }

    #[test]
    fn two_identical_messages_in_one_second_are_two_events() {
        // The same person sends the same words twice, Telegram showing the same
        // second for both. The update id is what tells them apart: nothing is
        // folded by text or time, and the second is not dropped as a repeat.
        let first = r#"[{"update_id":900,"message":{"message_id":50,"chat":{"id":100},"from":{"id":42,"first_name":"Kai"},"date":1700000000,"text":"same words"}}]"#;
        let second = r#"[{"update_id":901,"message":{"message_id":51,"chat":{"id":100},"from":{"id":42,"first_name":"Kai"},"date":1700000000,"text":"same words"}}]"#;
        let mut poll = Poll::default();
        let one = poll.route("bot_a", "ada", first).unwrap();
        let two = poll.route("bot_a", "ada", second).unwrap();
        let Routed::ToAgent { inbound: a, .. } = &one[0] else {
            panic!("{one:?}");
        };
        let Routed::ToAgent { inbound: b, .. } = &two[0] else {
            panic!("{two:?}");
        };
        assert_eq!((a.message_id, a.text.as_str()), (50, "same words"));
        assert_eq!((b.message_id, b.text.as_str()), (51, "same words"));
        assert_ne!(a.source_key, b.source_key);
        // The same update offered again is the same event, dropped as a repeat.
        let again = poll.route("bot_a", "ada", second).unwrap();
        assert!(matches!(again[0], Routed::Dropped), "{again:?}");
        assert_eq!(poll.offset, 902);
    }

    #[test]
    fn media_with_a_caption_is_delivered_and_a_sticker_is_not() {
        let photo = r#"[{"update_id":30,"message":{"message_id":70,"chat":{"id":100},"date":1700000000,"caption":"look"}}]"#;
        let sticker =
            r#"[{"update_id":31,"message":{"message_id":71,"chat":{"id":100},"date":1700000000}}]"#;
        let edited = r#"[{"update_id":32,"edited_message":{"message_id":72,"chat":{"id":100},"date":1700000000,"text":"x"}}]"#;
        let mut poll = Poll::default();
        let routed = poll.route("bot_a", "ada", photo).unwrap();
        let Routed::ToAgent { inbound, .. } = &routed[0] else {
            panic!("{routed:?}");
        };
        assert_eq!(inbound.text, "look");
        assert!(poll.route("bot_a", "ada", sticker).unwrap().is_empty());
        assert!(poll.route("bot_a", "ada", edited).unwrap().is_empty());
        assert_eq!(poll.offset, 33);
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
    fn the_channel_memory_round_trips_and_names_peers_without_a_secret() {
        let mut memory = Memory::from_state("{}");
        memory.offset = 41;
        memory.bot = Some("777:ada_bot".into());
        memory.attach("ada", 100);
        memory.attach("bea", 100);
        memory.attach("bea", 200);
        memory.attach("bea", 100);
        assert_eq!(memory.chats.len(), 3);
        assert_eq!(memory.peers(100, "ada"), vec!["bea"]);
        assert!(memory.peers(200, "bea").is_empty());
        let state = memory.to_state();
        assert_eq!(Memory::from_state(&state), memory);
        assert_eq!(Memory::from_state("not json"), Memory::default());
    }

    #[test]
    fn a_poll_resumes_after_the_saved_offset() {
        let poll = Poll::resume(41);
        assert_eq!(poll.get_updates(0).body["offset"], 41);
    }

    #[test]
    fn send_message_names_the_chat_and_keeps_the_token_out() {
        let call = send_message(100, "hello", Some(5), None);
        assert_eq!(call.method, "sendMessage");
        assert_eq!(call.body["chat_id"], 100);
        assert_eq!(call.body["text"], "hello");
        assert_eq!(call.body["reply_to_message_id"], 5);
        assert!(call.body.get("message_thread_id").is_none());
        let id = sent_message_id(r#"{"ok":true,"result":{"message_id":9}}"#).unwrap();
        assert_eq!(id, 9);
    }

    #[test]
    fn a_refused_call_reports_telegrams_own_words() {
        let refused = sent_message_id(
            r#"{"ok":false,"error_code":400,"description":"Bad Request: message thread not found"}"#,
        )
        .unwrap_err();
        assert!(matches!(refused, PollError::Refused(_)));
        assert!(refused.to_string().contains("message thread not found"));
    }

    #[test]
    fn get_me_names_the_bot_without_its_token() {
        let identity = bot_identity(
            r#"{"ok":true,"result":{"id":777,"is_bot":true,"first_name":"Ada","username":"ada_bot"}}"#,
        )
        .unwrap();
        assert_eq!(
            identity,
            BotIdentity {
                id: 777,
                username: "ada_bot".into()
            }
        );
        assert_eq!(get_me().method, "getMe");
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

#[cfg(test)]
mod membership_tests {
    use super::*;

    #[test]
    fn a_membership_update_attaches_and_detaches_without_a_message() {
        let mut poll = Poll::resume(0);
        let joined = r#"[{"update_id":1,"my_chat_member":{"chat":{"id":-100,"type":"supergroup"},
            "new_chat_member":{"status":"member"}}}]"#;
        let routed = poll.route("77", "ada", joined).unwrap();
        match &routed[0] {
            Routed::Membership {
                agent,
                chat_id,
                chat_type,
                joined,
            } => {
                assert_eq!(agent, "ada");
                assert_eq!(*chat_id, -100);
                assert_eq!(chat_type.as_deref(), Some("supergroup"));
                assert!(joined);
            }
            other => panic!("expected membership, got {other:?}"),
        }

        let left = r#"[{"update_id":2,"my_chat_member":{"chat":{"id":-100,"type":"supergroup"},
            "new_chat_member":{"status":"left"}}}]"#;
        let routed = poll.route("77", "ada", left).unwrap();
        assert!(matches!(
            &routed[0],
            Routed::Membership { joined: false, .. }
        ));
    }

    #[test]
    fn a_private_chat_is_not_a_room_and_a_group_is() {
        let mut memory = Memory::default();
        memory.attach_typed("ada", 5, Some("private"));
        memory.attach_typed("ada", -6, Some("supergroup"));
        assert!(!memory.is_room(5), "a private chat is not a room");
        assert!(memory.is_room(-6), "a supergroup is a room");
        memory.detach("ada", -6);
        assert!(!memory.is_attached("ada", -6));
    }
}
