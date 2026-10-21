//! Long-poll Telegram bots recorded as channels, and post one agent's text back.
//!
//! The token is read from the credential store when a poll or a send connects,
//! and it stays in memory. `getUpdates`, `getMe` and `sendMessage` are built
//! without it; the token is attached only on the request path. Failures name
//! neither the URL nor the request body, so a token on that path cannot reach a
//! log. `ANYAGENTS_TELEGRAM_API` points the calls at a fake server in tests.
//!
//! A channel's offset is saved after the updates it covers are in the mailbox,
//! so a restart asks for what it has not stored and a repeated update is kept
//! once by its source key.

use std::time::Duration;

use anyagents_channel_telegram::{
    BotCall, Memory, Poll, Routed, bot_identity, deliver_to_mailbox, get_me, send_message,
    sent_message_id,
};
use anyagents_core::{Channel, CredentialStore, FileCredentialStore, Store};

/// Seconds one `getUpdates` may hold the connection open.
pub const LONG_POLL_S: u64 = 30;
/// A call that answers at once gets this long; a long poll gets its own plus ten.
const SHORT_TIMEOUT_S: u64 = 30;

fn api_base() -> String {
    std::env::var("ANYAGENTS_TELEGRAM_API")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| anyagents_channel_telegram::API.to_string())
}

/// One poll cycle for every telegram channel, waiting up to `timeout_s` for
/// each. A channel with no secret is skipped. It returns when every channel has
/// answered, so a runner that is about to stop does not wait out a long poll.
/// Poll one channel once. Each bot runs this on its own thread, so a quiet bot
/// never delays another bot's update.
pub fn poll_channel_once(
    store: &Store,
    secrets: &FileCredentialStore,
    channel: &Channel,
    timeout_s: u64,
) -> Result<(), anyagents_core::Error> {
    poll_channel(store, secrets, channel, timeout_s)
}

fn poll_channel(
    store: &Store,
    secrets: &FileCredentialStore,
    channel: &Channel,
    timeout_s: u64,
) -> Result<(), anyagents_core::Error> {
    if channel.kind != "telegram" {
        return Ok(());
    }
    let Some(credential_ref) = channel.credential_ref.as_deref() else {
        return Ok(());
    };
    let Some(agent_id) = channel.agents.first() else {
        return Ok(());
    };
    let mut memory = Memory::from_state(&channel.state);
    let token = token_of(secrets, credential_ref)?;
    if memory.bot.is_none() {
        let identity = bot_identity(&call_api(&token, &get_me(), SHORT_TIMEOUT_S)?)
            .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
        memory.bot = Some(format!("{}:{}", identity.id, identity.username));
        store.set_channel_state(&channel.id, &memory.to_state())?;
    }
    let mut poll = Poll::resume(memory.offset);
    let response = call_api(&token, &poll.get_updates(timeout_s), timeout_s + 10)?;
    let body = updates_payload(&response)?;
    let before = poll.offset;
    let routed = poll
        .route("bot", agent_id.as_str(), &body)
        .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
    let attached = attached_agents(store)?;
    let mut grew = false;
    for one in routed {
        // This bot answers in the chat its update came from, so a post it makes
        // there reaches the other agents on that chat.
        if let Routed::ToAgent { inbound, .. } = &one {
            let known = memory
                .chats
                .iter()
                .any(|link| link.agent == agent_id.as_str() && link.chat_id == inbound.chat_id);
            memory.attach_typed(agent_id.as_str(), inbound.chat_id, Some(&inbound.chat_type));
            if !known && probe_members(&token, inbound.chat_id, memory.bot.as_deref().unwrap_or(""))
            {
                // Confirmed by Telegram rather than inferred from a message.
                memory.attach_typed(agent_id.as_str(), inbound.chat_id, Some(&inbound.chat_type));
            }
            grew |= !known;
        }
        // The bot was added to, or removed from, a chat.
        if let Routed::Membership {
            chat_id,
            chat_type,
            joined,
            ..
        } = &one
        {
            if *joined {
                memory.attach_typed(agent_id.as_str(), *chat_id, chat_type.as_deref());
            } else {
                memory.detach(agent_id.as_str(), *chat_id);
            }
            grew = true;
        }
        let peers = peer_ids(store, &poll, &one, agent_id.as_str(), &attached)?;
        deliver_to_mailbox(store, &one, &peers)?;
    }
    // The offset and the chats are saved only once the updates are stored, so a
    // restart asks for what it did not keep and a repeat is one row.
    if poll.offset != before || grew {
        memory.offset = poll.offset.max(memory.offset);
        store.set_channel_state(&channel.id, &memory.to_state())?;
    }
    Ok(())
}

/// Whether this bot is in the chat, asked of Telegram at cold start. A chat
/// first seen after a restart has no `my_chat_member` update to replay, so the
/// membership is read directly instead of inferred.
fn probe_members(token: &str, chat_id: i64, bot: &str) -> bool {
    let Some(user) = bot.split(':').next().and_then(|id| id.parse::<i64>().ok()) else {
        return false;
    };
    crate::telegram::chat_member_is_in(token, chat_id, user)
}

/// The other agents on a chat a peer's post belongs to, with that chat.
fn peer_ids(
    store: &Store,
    poll: &Poll,
    one: &Routed,
    sender: &str,
    attached: &[(String, i64)],
) -> Result<Vec<(anyagents_core::AgentId, i64)>, anyagents_core::Error> {
    let Routed::EchoToPeers { chat_id, .. } = one else {
        return Ok(Vec::new());
    };
    let attached: Vec<(&str, i64)> = attached
        .iter()
        .map(|(name, chat)| (name.as_str(), *chat))
        .collect();
    let mut peers = Vec::new();
    for name in poll.peers(*chat_id, sender, &attached) {
        if let Ok(agent) = store.agent_by_name(name) {
            peers.push((agent.id, *chat_id));
        }
    }
    Ok(peers)
}

/// The agents on this chat across every bot, with the chats each answers in.
///
/// Two bots on one chat are peers even though each is a separate channel with
/// its own bot, so membership is read from every channel's saved links. An
/// agent on another chat is not a member of this one.
fn attached_agents(store: &Store) -> Result<Vec<(String, i64)>, anyagents_core::Error> {
    let mut links: Vec<(String, i64)> = Vec::new();
    for one in store.list_channels()? {
        let memory = Memory::from_state(&one.state);
        let names: Vec<String> = one
            .agents
            .iter()
            .filter_map(|id| store.agent(id).ok().map(|agent| agent.name))
            .collect();
        for link in memory.chats {
            if !names.iter().any(|name| name == &link.agent) {
                continue;
            }
            if !links
                .iter()
                .any(|(agent, chat)| agent == &link.agent && *chat == link.chat_id)
            {
                links.push((link.agent, link.chat_id));
            }
        }
    }
    Ok(links)
}

/// A `sendMessage` for one chat, plus the secret it must be sent with.
#[derive(Debug)]
pub(crate) struct Outbound {
    pub credential_ref: String,
    pub call: BotCall,
}

/// Build the outbound call. The token is not read here.
pub(crate) fn outbound_call(
    store: &Store,
    from: Option<&str>,
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<Outbound, anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    Ok(Outbound {
        credential_ref,
        call: send_message(chat_id, text, reply_to, thread),
    })
}

/// Post one text through the agent's bot. The token is read for this call only.
#[allow(clippy::too_many_arguments)]
pub fn post_text(
    store: &Store,
    data_dir: &std::path::Path,
    from: Option<&str>,
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<i64, anyagents_core::Error> {
    let outbound = outbound_call(store, from, chat_id, text, reply_to, thread)?;
    let token = token_of(
        &FileCredentialStore::new(data_dir),
        &outbound.credential_ref,
    )?;
    let response = call_api(&token, &outbound.call, SHORT_TIMEOUT_S)?;
    let message_id = sent_message_id(&response)
        .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
    // The post is already on the wire, so everything after it is local only.
    // A failure now must never post a second time: the receipt is the message
    // id Telegram returned, and the local step is retried from it.
    deliver_locally(store, from, chat_id, message_id, text);
    Ok(message_id)
}

/// Ask Telegram whether this bot is a member of a chat. The answer is the
/// membership; a failed call is not membership.
pub fn chat_member_is_in(token: &str, chat_id: i64, user_id: i64) -> bool {
    let call = BotCall {
        method: "getChatMember".to_string(),
        body: serde_json::json!({"chat_id": chat_id, "user_id": user_id}),
    };
    let Ok(body) = call_api(token, &call, SHORT_TIMEOUT_S) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) else {
        return false;
    };
    matches!(
        value
            .pointer("/result/status")
            .and_then(serde_json::Value::as_str),
        Some("member" | "administrator" | "creator")
    )
}

/// The local half of one post: remember the chat, and copy the message into the
/// room when the chat is a group. Nothing here posts; it can be retried from the
/// receipt alone, and a repeated fan-out is one row.
pub fn deliver_locally(
    store: &Store,
    from: Option<&str>,
    chat_id: i64,
    message_id: i64,
    text: &str,
) {
    let Some(agent) = from else {
        return;
    };
    if record_chat(store, agent, chat_id).is_err() {
        // One retry: the store may have been busy on the first attempt.
        let _ = record_chat(store, agent, chat_id);
    }
    // Only a group is a room. A private chat is one person and one agent, so a
    // direct message never reaches another agent.
    if chat_is_room(store, chat_id) && fan_out(store, agent, chat_id, message_id, text).is_err() {
        let _ = fan_out(store, agent, chat_id, message_id, text);
    }
}

/// Whether any bot remembers this chat as a group or supergroup.
pub fn chat_is_room(store: &Store, chat_id: i64) -> bool {
    store
        .list_channels()
        .unwrap_or_default()
        .into_iter()
        .any(|channel| Memory::from_state(&channel.state).is_room(chat_id))
}

/// Put one posted chat message into every other member's mailbox.
///
/// A post made by one bot reaches the other agents on the chat through
/// AnyAgents' own mailboxes, not by relying on Telegram delivering a bot's
/// message to another bot. The source key is the chat and the Telegram message
/// id, so a repeated fan-out is one row.
pub fn fan_out(
    store: &Store,
    from: &str,
    chat_id: i64,
    message_id: i64,
    text: &str,
) -> Result<usize, anyagents_core::Error> {
    let room = format!("telegram:chat:{chat_id}");
    let incoming = anyagents_core::Incoming {
        text: text.to_string(),
        // The convention the agent loop uses: `agent:<name>`. `receive_room`
        // skips its author, so the posting agent is never a recipient of its
        // own message.
        sender: Some(format!("agent:{from}")),
        source_time_ms: None,
        source_key: Some(format!("tg-out:{chat_id}:{message_id}")),
        reply_to: Some(room.clone()),
        received_at_ms: None,
    };
    Ok(store.receive_room(&room, incoming)?.len())
}

/// Record that `agent` answers in `chat`, and learn the chat's title if the
/// adapter has one. The state stays the sender's own memory.
pub fn record_chat(store: &Store, agent: &str, chat_id: i64) -> Result<(), anyagents_core::Error> {
    for channel in store.list_channels()? {
        let memory = Memory::from_state(&channel.state);
        if memory.chats.iter().any(|link| link.chat_id == chat_id) {
            continue;
        }
        let ours = channel
            .agents
            .iter()
            .filter_map(|id| store.agent(id).ok())
            .any(|row| row.name == agent);
        if ours {
            let mut memory = memory;
            memory.attach(agent, chat_id);
            store.set_channel_state(&channel.id, &memory.to_state())?;
            return Ok(());
        }
    }
    Ok(())
}

fn channel_for(store: &Store, from: Option<&str>) -> Result<Channel, anyagents_core::Error> {
    let mut channels = Vec::new();
    for channel in store.list_channels()? {
        if channel.kind == "telegram" && channel.credential_ref.is_some() {
            channels.push(channel);
        }
    }
    if let Some(name) = from {
        let agent = store.agent_by_name(name)?;
        return channels
            .into_iter()
            .find(|channel| channel.agents.iter().any(|id| id == &agent.id))
            .ok_or_else(|| {
                anyagents_core::Error::Message(format!("no telegram channel for {name}"))
            });
    }
    let names = agent_names(store, &channels)?;
    match channels.len() {
        1 => Ok(channels.remove(0)),
        0 => Err(anyagents_core::Error::Message("no telegram channel".into())),
        _ => Err(anyagents_core::Error::Message(format!(
            "telegram sender is ambiguous: {names}"
        ))),
    }
}

/// `None` means the caller named nobody. MCP always passes `agent:<name>`.
pub(crate) fn bound_sender(sender: Option<&str>) -> Result<Option<String>, anyagents_core::Error> {
    match sender {
        None => Ok(None),
        Some(raw) => match anyagents_core::Address::parse(raw)? {
            anyagents_core::Address::Agent(name) => Ok(Some(name)),
            _ => Err(anyagents_core::Error::Message(
                "telegram sender must be an agent".into(),
            )),
        },
    }
}

fn agent_names(store: &Store, channels: &[Channel]) -> Result<String, anyagents_core::Error> {
    let mut names = Vec::new();
    for channel in channels {
        for id in &channel.agents {
            names.push(store.agent(id)?.name);
        }
    }
    names.sort();
    names.dedup();
    Ok(names.join(", "))
}

fn token_of(
    secrets: &FileCredentialStore,
    credential_ref: &str,
) -> Result<String, anyagents_core::Error> {
    let secret = secrets.read(credential_ref)?;
    secret
        .as_str()
        .map(ToString::to_string)
        .map_err(|_| anyagents_core::Error::Message("telegram token is not text".into()))
}

/// The update list from a `getUpdates` response. Anything else is refused.
pub(crate) fn updates_payload(response: &str) -> Result<String, anyagents_core::Error> {
    let value: serde_json::Value = serde_json::from_str(response)
        .map_err(|_| anyagents_core::Error::Message("telegram response is not json".into()))?;
    match value.get("result") {
        Some(serde_json::Value::Array(items)) => {
            Ok(serde_json::Value::Array(items.clone()).to_string())
        }
        _ => Err(anyagents_core::Error::Message(
            "telegram response has no update list".into(),
        )),
    }
}

fn call_api(token: &str, call: &BotCall, timeout_s: u64) -> Result<String, anyagents_core::Error> {
    let url = format!("{}/bot{token}/{}", api_base(), call.method);
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_s)))
        .build()
        .new_agent();
    let response = agent
        .post(&url)
        .send_json(&call.body)
        .map_err(|_| anyagents_core::Error::Message("telegram request failed".into()))?;
    response
        .into_body()
        .read_to_string()
        .map_err(|_| anyagents_core::Error::Message("telegram response could not be read".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn updates_payload_keeps_only_the_result_array() {
        let payload = updates_payload(r#"{"ok":true,"result":[{"update_id":1}]}"#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed[0]["update_id"], 1);
        assert!(updates_payload(r#"{"ok":true,"result":{"message_id":9}}"#).is_err());
    }

    #[test]
    fn a_channel_without_a_secret_is_not_polled() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        // A channel with no credential is not polled; the per-channel call is
        // what skipped it.
        let secrets = FileCredentialStore::new(dir.path());
        for channel in store.list_channels().unwrap() {
            poll_channel_once(&store, &secrets, &channel, 0).unwrap();
        }
    }

    #[test]
    fn outbound_uses_the_named_agents_bot_and_keeps_the_token_out() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let ada = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let bea = store
            .create_agent(anyagents_core::NewAgent::new("bea", "codex"))
            .unwrap();
        let ada_channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/ada".into()),
            })
            .unwrap();
        let bea_channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/bea".into()),
            })
            .unwrap();
        store.attach_channel(&ada_channel.id, &ada.id).unwrap();
        store.attach_channel(&bea_channel.id, &bea.id).unwrap();

        let sent = outbound_call(&store, Some("bea"), 100, "hello", Some(5), Some(3)).unwrap();
        assert_eq!(sent.credential_ref, "channels/bea");
        assert_eq!(sent.call.method, "sendMessage");
        assert_eq!(sent.call.body["chat_id"], 100);
        assert_eq!(sent.call.body["text"], "hello");
        assert_eq!(sent.call.body["reply_to_message_id"], 5);
        assert_eq!(sent.call.body["message_thread_id"], 3);
        assert!(!sent.call.body.to_string().contains("token"));

        let ambiguous = outbound_call(&store, None, 100, "hello", None, None).unwrap_err();
        let message = ambiguous.to_string();
        assert!(message.contains("ada"), "{message}");
        assert!(message.contains("bea"), "{message}");
        assert!(outbound_call(&store, Some("cy"), 100, "hello", None, None).is_err());
    }

    #[test]
    fn one_telegram_channel_sends_without_a_named_agent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let ada = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/ada".into()),
            })
            .unwrap();
        store.attach_channel(&channel.id, &ada.id).unwrap();
        let sent = outbound_call(&store, None, 100, "hello", None, None).unwrap();
        assert_eq!(sent.credential_ref, "channels/ada");
        assert_eq!(sent.call.method, "sendMessage");
    }

    #[test]
    fn no_telegram_channel_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let err = outbound_call(&store, None, 100, "hello", None, None).unwrap_err();
        assert!(err.to_string().contains("no telegram channel"), "{err}");
    }

    #[test]
    fn a_bound_sender_is_that_agent_and_anything_else_is_refused() {
        assert_eq!(
            bound_sender(Some("agent:ada")).unwrap().as_deref(),
            Some("ada")
        );
        assert_eq!(bound_sender(None).unwrap(), None);
        assert!(bound_sender(Some("group:crew")).is_err());
    }

    #[test]
    fn a_chat_an_agent_answers_in_is_remembered_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let ada = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let bea = store
            .create_agent(anyagents_core::NewAgent::new("bea", "codex"))
            .unwrap();
        let channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/ada".into()),
            })
            .unwrap();
        store.attach_channel(&channel.id, &ada.id).unwrap();
        store.attach_channel(&channel.id, &bea.id).unwrap();
        record_chat(&store, "ada", -1007).unwrap();
        record_chat(&store, "ada", -1007).unwrap();
        let channel = store.list_channels().unwrap().remove(0);
        let memory = Memory::from_state(&channel.state);
        assert_eq!(memory.chats.len(), 1);
        assert_eq!(memory.chats[0].chat_id, -1007);
        assert_eq!(
            attached_agents(&store).unwrap(),
            vec![("ada".to_string(), -1007)]
        );
    }
}
