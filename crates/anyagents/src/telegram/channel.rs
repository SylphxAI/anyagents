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

#[derive(Debug, PartialEq, Eq)]
pub enum WebhookError {
    NotFound(String),
    Unauthorized,
    BadRequest(String),
    Store(String),
}

/// Deterministic HMAC-SHA256 secret token for a channel's Telegram webhook,
/// derived from the bot token and channel ID so the raw bot token never
/// leaves the host.
pub fn webhook_secret(token: &str, channel_id: &str) -> String {
    hmac_sha256_hex(
        token.as_bytes(),
        format!("anyagents-telegram-webhook:{channel_id}").as_bytes(),
    )
}

fn verify_webhook_secret(expected: &str, provided: Option<&str>) -> bool {
    let Some(provided) = provided.map(str::trim) else {
        return false;
    };
    if expected.len() != provided.len() {
        return false;
    }
    expected
        .as_bytes()
        .iter()
        .zip(provided.as_bytes().iter())
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

/// Register a Telegram webhook for `channel` pointing at
/// `{webhook_base}/v1/webhooks/telegram/{channel_id}` with its derived
/// `X-Telegram-Bot-Api-Secret-Token`.
pub fn ensure_webhook(
    store: &Store,
    secrets: &FileCredentialStore,
    channel: &Channel,
    webhook_base: &str,
) -> Result<(), anyagents_core::Error> {
    if channel.kind != "telegram" {
        return Ok(());
    }
    let Some(credential_ref) = channel.credential_ref.as_deref() else {
        return Ok(());
    };
    let mut memory = Memory::from_state(&channel.state);
    let token = token_of(secrets, credential_ref)?;
    if memory.bot.is_none() {
        let identity = bot_identity(&call_api(
            &token,
            &get_me(),
            SHORT_TIMEOUT_S,
            Some(&channel.id),
        )?)
        .map_err(|_| {
            api_error(
                "getMe",
                Some(&channel.id),
                "invalid_response",
                None,
                None,
                "Telegram identity response is invalid",
                None,
            )
        })?;
        memory.bot = Some(format!("{}:{}", identity.id, identity.username));
        store.set_channel_state(&channel.id, &memory.to_state())?;
    }
    let base = webhook_base.trim_end_matches('/');
    let url = format!("{base}/v1/webhooks/telegram/{}", channel.id);
    let secret = webhook_secret(&token, channel.id.as_str());
    let call = BotCall {
        method: "setWebhook".to_string(),
        body: serde_json::json!({
            "url": url,
            "secret_token": secret,
            "allowed_updates": ["message", "edited_message", "my_chat_member"],
        }),
    };
    call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id))?;
    Ok(())
}

/// Remove a Telegram webhook so long polling can resume cleanly.
pub fn delete_webhook(
    secrets: &FileCredentialStore,
    channel: &Channel,
) -> Result<(), anyagents_core::Error> {
    if channel.kind != "telegram" {
        return Ok(());
    }
    let Some(credential_ref) = channel.credential_ref.as_deref() else {
        return Ok(());
    };
    let token = token_of(secrets, credential_ref)?;
    let call = BotCall {
        method: "deleteWebhook".to_string(),
        body: serde_json::json!({ "drop_pending_updates": false }),
    };
    call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id))?;
    Ok(())
}

/// Receive one webhook update (or array of updates) pushed by Telegram for
/// `channel_id`, verify `X-Telegram-Bot-Api-Secret-Token`, and route into the
/// durable mailbox.
pub fn receive_webhook(
    store: &Store,
    data_dir: &std::path::Path,
    channel_id: &str,
    secret_header: Option<&str>,
    payload: &serde_json::Value,
) -> Result<(), WebhookError> {
    let channel = store
        .list_channels()
        .map_err(|err| WebhookError::Store(err.to_string()))?
        .into_iter()
        .find(|c| c.id.as_str() == channel_id && c.kind == "telegram")
        .ok_or_else(|| WebhookError::NotFound(format!("unknown telegram channel {channel_id}")))?;
    let credential_ref = channel
        .credential_ref
        .as_deref()
        .ok_or_else(|| WebhookError::NotFound(format!("channel {channel_id} has no credential")))?;
    let Some(agent_id) = channel.agents.first() else {
        return Err(WebhookError::NotFound(format!(
            "channel {channel_id} has no attached agent"
        )));
    };
    let secrets = FileCredentialStore::new(data_dir);
    let token = token_of(&secrets, credential_ref)
        .map_err(|err| WebhookError::Store(err.to_string()))?;
    let expected = webhook_secret(&token, channel.id.as_str());
    if !verify_webhook_secret(&expected, secret_header) {
        return Err(WebhookError::Unauthorized);
    }
    let body = match payload {
        serde_json::Value::Array(_) => payload.to_string(),
        serde_json::Value::Object(_) => serde_json::Value::Array(vec![payload.clone()]).to_string(),
        _ => {
            return Err(WebhookError::BadRequest(
                "webhook payload must be a JSON object or array".into(),
            ))
        }
    };
    let mut memory = Memory::from_state(&channel.state);
    ingest_updates(
        store,
        &channel,
        agent_id,
        &token,
        &mut memory,
        &body,
        "getUpdates",
    )
    .map_err(|err| WebhookError::BadRequest(err.to_string()))
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
        let identity = bot_identity(&call_api(
            &token,
            &get_me(),
            SHORT_TIMEOUT_S,
            Some(&channel.id),
        )?)
        .map_err(|_| {
            api_error(
                "getMe",
                Some(&channel.id),
                "invalid_response",
                None,
                None,
                "Telegram identity response is invalid",
                None,
            )
        })?;
        memory.bot = Some(format!("{}:{}", identity.id, identity.username));
        store.set_channel_state(&channel.id, &memory.to_state())?;
    }
    let poll = Poll::resume(memory.offset);
    let response = call_api(
        &token,
        &poll.get_updates(timeout_s),
        timeout_s + 10,
        Some(&channel.id),
    )?;
    let body = updates_payload(&response)?;
    ingest_updates(
        store,
        channel,
        agent_id,
        &token,
        &mut memory,
        &body,
        "getUpdates",
    )
}

fn ingest_updates(
    store: &Store,
    channel: &Channel,
    agent_id: &anyagents_core::AgentId,
    token: &str,
    memory: &mut Memory,
    body: &str,
    method: &str,
) -> Result<(), anyagents_core::Error> {
    let mut poll = Poll::resume(memory.offset);
    let before = poll.offset;
    let routed = poll.route("bot", agent_id.as_str(), body).map_err(|_| {
        api_error(
            method,
            Some(&channel.id),
            "invalid_response",
            None,
            None,
            "Telegram update response is invalid",
            None,
        )
    })?;
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
            if !known && probe_members(token, inbound.chat_id, memory.bot.as_deref().unwrap_or(""))
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
    pub channel_id: anyagents_core::ChannelId,
    pub credential_ref: String,
    pub call: BotCall,
}

/// Build the outbound call. The token is not read here.
pub(crate) fn outbound_call(
    store: &Store,
    from: &str,
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
        channel_id: channel.id,
        credential_ref,
        call: send_message(chat_id, text, reply_to, thread),
    })
}

/// Post one text through the agent's bot. The token is read for this call only.
#[allow(clippy::too_many_arguments)]
pub fn post_text(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
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
    let response = call_api(
        &token,
        &outbound.call,
        SHORT_TIMEOUT_S,
        Some(&outbound.channel_id),
    )?;
    let message_id = sent_message_id(&response).map_err(|_| {
        api_error(
            "sendMessage",
            Some(&outbound.channel_id),
            "invalid_response",
            None,
            None,
            "Telegram response has no message receipt; delivery may be uncertain",
            None,
        )
    })?;
    // Telegram accepted this post. Local retries below never issue another
    // API call. Crash-safe receipt custody is the server's responsibility.
    // Reuse the adapter's provider quote decoder, rather than trusting a caller
    // to supply a quoted author or message body.
    let (provider_thread, quote) = sent_metadata(&response);
    let thread = provider_thread.or(thread);
    deliver_locally_with_metadata(
        store,
        from,
        chat_id,
        message_id,
        text,
        thread,
        quote.as_ref(),
    );
    Ok(message_id)
}

/// Ask Telegram whether this bot is a member of a chat. The answer is the
/// membership; a failed call is not membership.
pub fn chat_member_is_in(token: &str, chat_id: i64, user_id: i64) -> bool {
    let call = BotCall {
        method: "getChatMember".to_string(),
        body: serde_json::json!({"chat_id": chat_id, "user_id": user_id}),
    };
    let Ok(body) = call_api(token, &call, SHORT_TIMEOUT_S, None) else {
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
/// room when the chat is a group. Nothing here posts; replaying the same
/// receipt is one row. Legacy callers have no topic/quote metadata to supply.
#[allow(dead_code)] // Preserve the legacy receipt-replay entry point.
pub fn deliver_locally(store: &Store, from: &str, chat_id: i64, message_id: i64, text: &str) {
    deliver_locally_with_metadata(store, from, chat_id, message_id, text, None, None);
}

#[allow(clippy::too_many_arguments)]
fn deliver_locally_with_metadata(
    store: &Store,
    from: &str,
    chat_id: i64,
    message_id: i64,
    text: &str,
    thread: Option<i64>,
    quote: Option<&anyagents_core::Quote>,
) {
    let agent = from;
    if record_chat(store, agent, chat_id).is_err() {
        // One retry: the store may have been busy on the first attempt.
        let _ = record_chat(store, agent, chat_id);
    }
    // Only a group is a room. A private chat is one person and one agent, so a
    // direct message never reaches another agent.
    if chat_is_room(store, chat_id)
        && fan_out_with_metadata(store, agent, chat_id, message_id, text, thread, quote).is_err()
    {
        let _ = fan_out_with_metadata(store, agent, chat_id, message_id, text, thread, quote);
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
#[allow(dead_code)] // Preserve callers that replay a receipt without metadata.
pub fn fan_out(
    store: &Store,
    from: &str,
    chat_id: i64,
    message_id: i64,
    text: &str,
) -> Result<usize, anyagents_core::Error> {
    fan_out_with_metadata(store, from, chat_id, message_id, text, None, None)
}

#[allow(clippy::too_many_arguments)]
fn fan_out_with_metadata(
    store: &Store,
    from: &str,
    chat_id: i64,
    message_id: i64,
    text: &str,
    thread: Option<i64>,
    quote: Option<&anyagents_core::Quote>,
) -> Result<usize, anyagents_core::Error> {
    // Membership is scoped to the chat, while replies retain the topic.
    let room = format!("telegram:chat:{chat_id}");
    let reply_target = match thread {
        Some(thread) => format!("{room}:{thread}"),
        None => room.clone(),
    };
    let incoming = anyagents_core::Incoming {
        text: text.to_string(),
        // The convention the agent loop uses: `agent:<name>`. `receive_room`
        // skips its author, so the posting agent is never a recipient of its
        // own message.
        sender: Some(format!("agent:{from}")),
        source_time_ms: None,
        source_key: Some(format!("tg-out:{chat_id}:{message_id}")),
        reply_to: Some(reply_target),
        quote: quote.cloned(),
        origin: Some(anyagents_core::Origin {
            chat_id: Some(chat_id),
            thread_id: thread,
            message_id: Some(message_id),
        }),
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

/// The one bot an agent posts through. An agent that has none, or more than
/// one, posts nothing: there is no default bot to fall back to.
fn channel_for(store: &Store, from: &str) -> Result<Channel, anyagents_core::Error> {
    let agent = store.agent_by_name(from)?;
    let mine: Vec<Channel> = store
        .list_channels()?
        .into_iter()
        .filter(|channel| channel.kind == "telegram" && channel.credential_ref.is_some())
        .filter(|channel| channel.agents.iter().any(|id| id == &agent.id))
        .collect();
    match mine.len() {
        1 => Ok(mine.into_iter().next().unwrap_or_else(|| unreachable!())),
        0 => Err(anyagents_core::Error::Message(format!(
            "no telegram channel for {from}"
        ))),
        _ => Err(anyagents_core::Error::Message(format!(
            "{from} has more than one telegram bot; name one"
        ))),
    }
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

/// Decode the provider's topic and authenticated reply metadata using the same
/// adapter as inbound updates. Missing quote metadata is never invented.
fn sent_metadata(response: &str) -> (Option<i64>, Option<anyagents_core::Quote>) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(response) else {
        return (None, None);
    };
    let result = &value["result"];
    let thread = result
        .get("message_thread_id")
        .and_then(serde_json::Value::as_i64);
    let payload = serde_json::json!([{"update_id": 1, "message": result}]).to_string();
    let mut poll = Poll::resume(0);
    let quote = poll.route("sent", "", &payload).ok().and_then(|items| {
        items.into_iter().find_map(|item| match item {
            Routed::ToAgent { inbound, .. } => inbound.quote,
            _ => None,
        })
    });
    (thread, quote)
}

/// Errors expose stable fields, never ureq's URL-bearing Display/Debug or a
/// provider body. An exact description allowlist prevents arbitrary echoed
/// credentials, URLs and personal message text from entering diagnostics.
fn api_error(
    method: &str,
    channel: Option<&anyagents_core::ChannelId>,
    class: &str,
    status: Option<u16>,
    code: Option<i64>,
    description: &str,
    retry_after: Option<u64>,
) -> anyagents_core::Error {
    // BotCall is internal today, but do not make that a redaction dependency.
    let method = match method {
        "sendMessage" | "getUpdates" | "getMe" | "getChatMember" | "setWebhook"
        | "deleteWebhook" => method,
        _ => "unknown",
    };
    anyagents_core::Error::Message(format!(
        "telegram {}",
        serde_json::json!({
            "method": method, "channel_id": channel.map(|id| id.as_str()),
            "class": class, "status": status,
            "error_code": code, "description": description, "retry_after": retry_after,
        })
    ))
}

fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (data.len() as u64) * 8;
    let mut padded = data.to_vec();
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for (i, word) in chunk.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*word);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (state, delta) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *state = state.wrapping_add(delta);
        }
    }
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn hmac_sha256_hex(key: &[u8], message: &[u8]) -> String {
    let mut k_block = [0u8; 64];
    if key.len() > 64 {
        let digest = sha256(key);
        k_block[..32].copy_from_slice(&digest);
    } else {
        k_block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(64 + message.len());
    for byte in &k_block {
        inner.push(byte ^ 0x36);
    }
    inner.extend_from_slice(message);
    let inner_hash = sha256(&inner);
    let mut outer = Vec::with_capacity(64 + 32);
    for byte in &k_block {
        outer.push(byte ^ 0x5c);
    }
    outer.extend_from_slice(&inner_hash);
    let digest = sha256(&outer);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn provider_description(description: Option<&str>) -> (&'static str, &'static str) {
    let normalized = description.map(str::to_ascii_lowercase);
    match normalized.as_deref() {
        Some("forbidden: bot was blocked by the user") => {
            ("bot_blocked", "Forbidden: bot was blocked by the user")
        }
        Some("forbidden: bot can't initiate conversation with a user") => (
            "human_start_required",
            "Forbidden: bot can't initiate conversation with a user",
        ),
        Some("bad request: chat not found") => ("chat_not_found", "Bad Request: chat not found"),
        Some("forbidden: user is deactivated") => {
            ("user_deactivated", "Forbidden: user is deactivated")
        }
        Some("forbidden: bot can't send messages to bots") => (
            "bot_destination_forbidden",
            "Forbidden: bot can't send messages to bots",
        ),
        Some("forbidden: bot is not a member of the supergroup chat") => (
            "not_member",
            "Forbidden: bot is not a member of the supergroup chat",
        ),
        Some("forbidden: bot is not a member of the group chat") => (
            "not_member",
            "Forbidden: bot is not a member of the group chat",
        ),
        Some("forbidden: bot is not a member of the channel chat") => (
            "not_member",
            "Forbidden: bot is not a member of the channel chat",
        ),
        Some("forbidden: bot was kicked from the group chat") => (
            "not_member",
            "Forbidden: bot was kicked from the group chat",
        ),
        Some("forbidden: bot was kicked from the supergroup chat") => (
            "not_member",
            "Forbidden: bot was kicked from the supergroup chat",
        ),
        Some("forbidden: bot was kicked from the channel chat") => (
            "not_member",
            "Forbidden: bot was kicked from the channel chat",
        ),
        Some("bad request: message thread not found" | "bad request: message_thread_invalid") => {
            ("invalid_topic", "Bad Request: message thread not found")
        }
        Some("bad request: topic_closed") => ("topic_closed", "Bad Request: topic closed"),
        Some(
            "bad request: message to be replied not found"
            | "bad request: reply message not found"
            | "bad request: reply_message_not_found",
        ) => (
            "invalid_reply",
            "Bad Request: message to be replied not found",
        ),
        Some(
            "bad request: not enough rights to send text messages to the chat"
            | "bad request: chat_write_forbidden",
        ) => (
            "send_forbidden",
            "Bad Request: not enough rights to send text messages to the chat",
        ),
        Some(
            "conflict: terminated by other getupdates request; make sure that only one bot instance is running",
        ) => (
            "poll_conflict",
            "Conflict: another getUpdates request is active",
        ),
        Some(
            "conflict: can't use getupdates method while webhook is active; use deletewebhook to delete the webhook first",
        ) => ("poll_conflict", "Conflict: webhook is active"),
        Some("unauthorized") => ("unauthorized", "Unauthorized"),
        _ => ("provider_refused", "provider description redacted"),
    }
}

fn call_api(
    token: &str,
    call: &BotCall,
    timeout_s: u64,
    channel: Option<&anyagents_core::ChannelId>,
) -> Result<String, anyagents_core::Error> {
    let url = format!("{}/bot{token}/{}", api_base(), call.method);
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_s)))
        .http_status_as_error(false)
        // A Telegram API request must never follow a redirect with a token or
        // message body. Refusal is returned once, without a retry.
        .max_redirects(0)
        .max_redirects_will_error(false)
        .build()
        .new_agent();
    let response = agent.post(&url).send_json(&call.body).map_err(|err| {
        let (class, description) = match err {
            ureq::Error::Timeout(_) => (
                "timeout",
                "Telegram request timed out; delivery may be uncertain",
            ),
            ureq::Error::HostNotFound => ("network", "Telegram hostname could not be resolved"),
            ureq::Error::Io(_) | ureq::Error::ConnectionFailed => (
                "network",
                "Telegram connection failed; delivery may be uncertain",
            ),
            _ => (
                "transport",
                "Telegram transport failed; delivery may be uncertain",
            ),
        };
        api_error(&call.method, channel, class, None, None, description, None)
    })?;
    let status = response.status().as_u16();
    let body = response.into_body().read_to_string().map_err(|_| {
        api_error(
            &call.method,
            channel,
            "response_read",
            Some(status),
            None,
            "Telegram response could not be read; delivery may be uncertain",
            None,
        )
    })?;
    let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| {
        api_error(
            &call.method,
            channel,
            "invalid_response",
            Some(status),
            None,
            "Telegram response is not JSON; delivery may be uncertain",
            None,
        )
    })?;
    let ok = value.get("ok").and_then(serde_json::Value::as_bool);
    if (200..300).contains(&status) && ok.is_none() {
        return Err(api_error(
            &call.method,
            channel,
            "invalid_response",
            Some(status),
            None,
            "Telegram response has no acknowledgement; delivery may be uncertain",
            None,
        ));
    }
    if !(200..300).contains(&status) || ok != Some(true) {
        let code = value.get("error_code").and_then(serde_json::Value::as_i64);
        let (mut class, mut description) =
            provider_description(value.get("description").and_then(serde_json::Value::as_str));
        let retry_after = value
            .pointer("/parameters/retry_after")
            .and_then(serde_json::Value::as_u64);
        if status == 429 || code == Some(429) {
            class = "rate_limited";
            description = "Telegram rate limit exceeded";
        }
        return Err(api_error(
            &call.method,
            channel,
            class,
            Some(status),
            code,
            description,
            retry_after,
        ));
    }
    if call.method == "sendMessage"
        && value
            .pointer("/result/message_id")
            .and_then(serde_json::Value::as_i64)
            .is_none()
    {
        return Err(api_error(
            &call.method,
            channel,
            "invalid_response",
            Some(status),
            None,
            "Telegram response has no message receipt; delivery may be uncertain",
            None,
        ));
    }
    Ok(body)
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

        let sent = outbound_call(&store, "bea", 100, "hello", Some(5), Some(3)).unwrap();
        assert_eq!(sent.credential_ref, "channels/bea");
        assert_eq!(sent.call.method, "sendMessage");
        assert_eq!(sent.call.body["chat_id"], 100);
        assert_eq!(sent.call.body["text"], "hello");
        assert_eq!(sent.call.body["reply_to_message_id"], 5);
        assert_eq!(sent.call.body["message_thread_id"], 3);
        assert!(!sent.call.body.to_string().contains("token"));

        // Nobody else's bot is ever used, and an unknown agent has none.
        assert!(outbound_call(&store, "cy", 100, "hello", None, None).is_err());
    }

    #[test]
    fn a_bot_belongs_to_one_agent() {
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
        let sent = outbound_call(&store, "ada", 100, "hello", None, None).unwrap();
        assert_eq!(sent.credential_ref, "channels/ada");
        assert_eq!(sent.call.method, "sendMessage");
        // There is no default bot: another agent does not post through it.
        let bea = store
            .create_agent(anyagents_core::NewAgent::new("bea", "codex"))
            .unwrap();
        assert!(outbound_call(&store, "bea", 100, "hi", None, None).is_err());
        assert!(bea.name == "bea");
    }

    #[test]
    fn no_telegram_channel_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let ada = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let err = outbound_call(&store, "ada", 100, "hello", None, None).unwrap_err();
        assert!(err.to_string().contains("no telegram channel"), "{err}");
        assert!(ada.name == "ada");
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

    #[test]
    fn webhook_update_verifies_secret_and_routes_into_mailbox() {
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
        FileCredentialStore::new(dir.path())
            .write("channels/ada", b"777:webhook-token")
            .unwrap();
        let update = serde_json::json!({
            "update_id": 42,
            "message": {
                "message_id": 9,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 42, "is_bot": false, "first_name": "Kyle"},
                "date": 1_700_000_000,
                "text": "pushed via webhook",
            }
        });
        assert_eq!(
            receive_webhook(
                &store,
                dir.path(),
                channel.id.as_str(),
                Some("wrong"),
                &update
            ),
            Err(WebhookError::Unauthorized)
        );
        assert!(store.inbox(&ada.id, None).unwrap().is_empty());

        let secret = webhook_secret("777:webhook-token", channel.id.as_str());
        assert_eq!(secret.len(), 64);
        assert!(!secret.contains("webhook-token"));
        receive_webhook(
            &store,
            dir.path(),
            channel.id.as_str(),
            Some(&secret),
            &update,
        )
        .unwrap();
        // Duplicate delivery of the same update_id stays one row.
        receive_webhook(
            &store,
            dir.path(),
            channel.id.as_str(),
            Some(&secret),
            &update,
        )
        .unwrap();
        let inbox = store.inbox(&ada.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "pushed via webhook");
        assert_eq!(inbox[0].source_key.as_deref(), Some("bot:42"));
    }
}
