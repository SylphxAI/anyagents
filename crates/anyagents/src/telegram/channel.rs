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
    BotCall, ManagedBotEvent, MediaAttachment, Memory, Poll, Routed, bot_identity, delete_message,
    deliver_to_mailbox, edit_message_text, file_path_of, format_checklist_markdown, get_file,
    get_managed_bot_token, get_me, managed_bot_token_of, pin_chat_message,
    replace_managed_bot_token, send_checklist, send_location, send_media_ref, send_message,
    send_poll, sent_message_id, set_message_reaction, stop_poll, unpin_chat_message,
};
use anyagents_core::{Channel, CredentialStore, FileCredentialStore, Store};

/// Something the Telegram poller acts on may have changed: a binding written
/// here, a local receipt left pending, a bound agent's lifecycle (the poller
/// forwards the store's agent wakes) or a stop. Every waiting poller thread in
/// this process wakes, looks, and waits again. It lives beside the writes that
/// raise it.
pub(super) struct Signal {
    generation: std::sync::Mutex<u64>,
    raised: std::sync::Condvar,
}

pub(super) static CHANGES: Signal = Signal::new();

impl Signal {
    pub(super) const fn new() -> Self {
        Self {
            generation: std::sync::Mutex::new(0),
            raised: std::sync::Condvar::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, u64> {
        self.generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn generation(&self) -> u64 {
        *self.lock()
    }

    pub(super) fn raise(&self) {
        *self.lock() += 1;
        self.raised.notify_all();
    }

    /// Wait until the signal is raised after `seen`, a stop flag is set, or
    /// `timeout` passes. A stop flag is set before the signal is raised, so a
    /// waiter never misses a stop.
    pub(super) fn wait(
        &self,
        seen: u64,
        timeout: Duration,
        stops: &[&std::sync::atomic::AtomicBool],
    ) {
        let deadline = std::time::Instant::now() + timeout;
        let mut generation = self.lock();
        while *generation == seen && !stopped(stops) {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return;
            }
            generation = self
                .raised
                .wait_timeout(generation, left)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }

    /// Wait out `timeout` unless a stop flag is set first; other changes do
    /// not cut the wait short.
    pub(super) fn pause(&self, timeout: Duration, stops: &[&std::sync::atomic::AtomicBool]) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() || stopped(stops) {
                return;
            }
            self.wait(self.generation(), left, stops);
        }
    }
}

fn stopped(stops: &[&std::sync::atomic::AtomicBool]) -> bool {
    stops
        .iter()
        .any(|stop| stop.load(std::sync::atomic::Ordering::SeqCst))
}

/// A Telegram binding, a bound agent's lifecycle or a pending receipt may
/// have changed: every poller in this process looks again now.
pub(super) fn changed() {
    CHANGES.raise();
}

/// Raises [`changed`] when dropped, so a binding that stops half way through
/// its writes is looked at too.
struct ChangedOnDrop;

impl Drop for ChangedOnDrop {
    fn drop(&mut self) {
        changed();
    }
}

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
    let mut memory = Memory::try_from_state(&channel.state)?;
    let token = token_of(secrets, credential_ref)?;
    if memory.bot.as_deref().unwrap_or("").is_empty() {
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
        save_channel_memory_merged(store, &channel.id, &mut memory, &[])?;
    }
    let base = webhook_base.trim_end_matches('/');
    let url = format!("{base}/v1/webhooks/telegram/{}", channel.id);
    let secret = webhook_secret(&token, channel.id.as_str());
    let call = BotCall {
        method: "setWebhook".to_string(),
        body: serde_json::json!({
            "url": url,
            "secret_token": secret,
            "allowed_updates": ["message", "edited_message", "my_chat_member", "message_reaction", "managed_bot"],
        }),
    };
    call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id))?;
    Ok(())
}

/// Merge `memory` with the latest persisted `channels.state` row so concurrent
/// webhook/discovery/poll updates never overwrite each other's `chats` or `offset`.
fn save_channel_memory_merged(
    store: &Store,
    channel_id: &anyagents_core::ChannelId,
    memory: &mut Memory,
    removed_chats: &[i64],
) -> Result<(), anyagents_core::Error> {
    save_channel_memory_merged_with_pending(store, channel_id, memory, removed_chats, &[])
}

fn save_channel_memory_merged_with_pending(
    store: &Store,
    channel_id: &anyagents_core::ChannelId,
    memory: &mut Memory,
    removed_chats: &[i64],
    removed_pending: &[String],
) -> Result<(), anyagents_core::Error> {
    if let Some(fresh) = store
        .list_channels()?
        .into_iter()
        .find(|c| &c.id == channel_id)
    {
        let fresh_mem = Memory::try_from_state(&fresh.state)?;
        memory.offset = memory.offset.max(fresh_mem.offset);
        if memory.bot.as_deref().unwrap_or("").is_empty() && fresh_mem.bot.is_some() {
            memory.bot = fresh_mem.bot;
        }
        for (k, v) in fresh_mem.pending_managed_bots {
            if !removed_pending.iter().any(|r| r == &k) {
                memory.pending_managed_bots.entry(k).or_insert(v);
            }
        }
        for link in fresh_mem.chats {
            if removed_chats.contains(&link.chat_id) {
                continue;
            }
            if let Some(existing) = memory
                .chats
                .iter_mut()
                .find(|l| l.agent == link.agent && l.chat_id == link.chat_id)
            {
                if existing.chat_type.is_none() {
                    existing.chat_type = link.chat_type;
                }
                if existing.title.is_none() {
                    existing.title = link.title;
                }
            } else {
                memory.chats.push(link);
            }
        }
    }
    for r in removed_pending {
        memory.pending_managed_bots.remove(r);
    }
    memory.chats.retain(|l| !removed_chats.contains(&l.chat_id));
    store.set_channel_state(channel_id, &memory.to_state())
}

fn seed_group_ids_from_env() -> Vec<i64> {
    std::env::var("ANYAGENTS_TELEGRAM_SEED_GROUPS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|s| s.trim().parse::<i64>().ok())
                .filter(|&id| id < 0)
                .collect()
        })
        .unwrap_or_default()
}

fn resolve_chat_title_and_type(
    token: &str,
    channel_id: &anyagents_core::ChannelId,
    chat_id: i64,
) -> Option<(String, Option<String>)> {
    let call = BotCall {
        method: "getChat".to_string(),
        body: serde_json::json!({ "chat_id": chat_id }),
    };
    call_api(token, &call, 5, Some(channel_id))
        .ok()
        .and_then(|resp| serde_json::from_str::<serde_json::Value>(&resp).ok())
        .and_then(|val| {
            let res = val.get("result")?;
            let ct = res
                .get("type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            let title = res
                .get("title")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .or_else(|| {
                    let f = res
                        .get("first_name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty());
                    let l = res
                        .get("last_name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty());
                    let u = res
                        .get("username")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty());
                    let full = match (f, l) {
                        (Some(a), Some(b)) => Some(format!("{a} {b}")),
                        (Some(a), None) => Some(a.to_string()),
                        (None, Some(b)) => Some(b.to_string()),
                        (None, None) => None,
                    };
                    match (full, u) {
                        (Some(n), Some(un)) => Some(format!("{n} (@{un})")),
                        (Some(n), None) => Some(n),
                        (None, Some(un)) => Some(format!("@{un}")),
                        (None, None) => None,
                    }
                });
            Some((title.unwrap_or_else(|| "-".to_string()), ct))
        })
}

/// Proactively reconcile `channel`'s group chat bindings (`chat_id < 0`) and
/// missing chat titles against Telegram's authoritative `getChatMember` and `getChat`.
/// Runs in both webhook mode and long-polling mode.
pub fn reconcile_channel_groups(store: &Store, secrets: &FileCredentialStore, channel: &Channel) {
    if std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some()
        || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
        || channel.kind != "telegram"
    {
        return;
    }
    let Some(credential_ref) = channel.credential_ref.as_deref() else {
        return;
    };
    let Some(agent_id) = channel.agents.first() else {
        return;
    };
    if let Ok(agent) = store.agent(agent_id) {
        if agent.state == "archived"
            || agent.state == "paused"
            || is_agent_explicitly_archived(store.data_dir(), &agent)
        {
            return;
        }
        sync_agent_telegram_bot_name(store, store.data_dir(), &agent);
    }
    let Ok(token) = token_of(secrets, credential_ref) else {
        return;
    };
    let mut memory = match Memory::try_from_state(&channel.state) {
        Ok(memory) => memory,
        Err(err) => {
            eprintln!("{err}");
            return;
        }
    };
    let mut changed = false;
    let mut removed_chats: Vec<i64> = Vec::new();
    if memory.bot.as_deref().unwrap_or("").is_empty()
        && let Ok(resp) = call_api(&token, &get_me(), SHORT_TIMEOUT_S, Some(&channel.id))
        && let Ok(identity) = bot_identity(&resp)
    {
        memory.bot = Some(format!("{}:{}", identity.id, identity.username));
        changed = true;
    }
    let Some(bot_tag) = memory.bot.clone().filter(|b| !b.is_empty()) else {
        if changed {
            let _ = save_channel_memory_merged(store, &channel.id, &mut memory, &[]);
        }
        return;
    };

    let mut candidate_groups: std::collections::BTreeMap<i64, (Option<String>, Option<String>)> =
        std::collections::BTreeMap::new();
    for gid in seed_group_ids_from_env() {
        let default_kind = if gid.to_string().starts_with("-100") {
            "supergroup"
        } else {
            "group"
        };
        candidate_groups.insert(gid, (Some(default_kind.to_string()), None));
    }
    if let Ok(all_channels) = store.list_channels() {
        for ch in all_channels {
            if ch.kind != "telegram" {
                continue;
            }
            let ch_mem = match Memory::try_from_state(&ch.state) {
                Ok(memory) => memory,
                Err(err) => {
                    eprintln!("{err}");
                    return;
                }
            };
            for link in ch_mem.chats {
                if link.chat_id < 0 {
                    let entry = candidate_groups.entry(link.chat_id).or_insert((None, None));
                    if entry.0.is_none() && link.chat_type.is_some() {
                        entry.0 = link.chat_type;
                    }
                    if entry.1.is_none()
                        && link
                            .title
                            .as_deref()
                            .is_some_and(|t| !t.trim().is_empty() && t.trim() != "-")
                    {
                        entry.1 = link.title;
                    }
                }
            }
        }
    }

    for (gid, (known_type, known_title)) in candidate_groups {
        let already_attached = memory
            .chats
            .iter()
            .any(|l| l.agent == agent_id.as_str() && l.chat_id == gid);
        match cached_probe_member_status(&token, gid, &bot_tag) {
            Some(true) => {
                if !already_attached {
                    let fallback_type =
                        known_type
                            .as_deref()
                            .unwrap_or(if gid.to_string().starts_with("-100") {
                                "supergroup"
                            } else {
                                "group"
                            });
                    memory.attach_typed_titled(
                        agent_id.as_str(),
                        gid,
                        Some(fallback_type),
                        known_title.as_deref(),
                    );
                    changed = true;
                } else if let Some(ref kt) = known_title {
                    for link in memory
                        .chats
                        .iter_mut()
                        .filter(|l| l.chat_id == gid && l.title.is_none())
                    {
                        link.title = Some(kt.clone());
                        changed = true;
                    }
                }
            }
            Some(false) => {
                let before_len = memory.chats.len();
                memory.chats.retain(|l| l.chat_id != gid);
                if memory.chats.len() != before_len {
                    removed_chats.push(gid);
                    changed = true;
                }
            }
            None => {}
        }
    }

    let missing_title_ids: Vec<i64> = memory
        .chats
        .iter()
        .filter(|c| c.title.is_none())
        .map(|c| c.chat_id)
        .collect();
    for cid in missing_title_ids {
        let Some((resolved_title, resolved_type)) =
            resolve_chat_title_and_type(&token, &channel.id, cid)
        else {
            continue;
        };
        for link in memory.chats.iter_mut().filter(|l| l.chat_id == cid) {
            link.title = Some(resolved_title.clone());
            if link.chat_type.is_none()
                && let Some(ref ct) = resolved_type
            {
                link.chat_type = Some(ct.clone());
            }
            changed = true;
        }
    }

    if changed {
        let _ = save_channel_memory_merged(store, &channel.id, &mut memory, &removed_chats);
    }
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

fn is_agent_explicitly_archived(data_dir: &std::path::Path, agent: &anyagents_core::Agent) -> bool {
    anyagents_core::agent_config_dir(data_dir, &agent.id)
        .join(".anyagents-archived")
        .exists()
        || (agent.state == "archived" && agent.parent_id.is_none())
}

/// Detach an archived agent from all Telegram group chat links (`chat_id < 0`)
/// and unregister its live Telegram webhook so group posts or membership events
/// never route to or wake a retired agent.
pub(crate) fn detach_archived_agent_from_telegram_groups(
    store: &Store,
    data_dir: &std::path::Path,
    agent: &anyagents_core::Agent,
    drop_live_webhook: bool,
) {
    let Ok(channels) = store.list_channels() else {
        return;
    };
    let is_live = std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && std::env::var_os("ANYAGENTS_TELEGRAM_API").is_none();
    for channel in channels {
        if channel.kind != "telegram" {
            continue;
        }
        let owns_channel = channel.agents.contains(&agent.id);
        let mut mem = match Memory::try_from_state(&channel.state) {
            Ok(memory) => memory,
            Err(err) => {
                eprintln!("{err}");
                return;
            }
        };
        let before = mem.chats.len();
        mem.chats.retain(|link| {
            if link.chat_id >= 0 {
                return true;
            }
            if owns_channel {
                return false;
            }
            link.agent != agent.id.as_str() && link.agent != agent.name
        });
        if mem.chats.len() != before
            && let Err(err) = store.set_channel_state(&channel.id, &mem.to_state())
        {
            eprintln!("Telegram archive membership write failed: {err}");
        }
        if drop_live_webhook && is_live && owns_channel {
            let dir = data_dir.to_path_buf();
            let ch = channel.clone();
            std::thread::spawn(move || {
                let secrets = FileCredentialStore::new(&dir);
                let _ = delete_webhook(&secrets, &ch);
            });
        }
    }
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
    let token =
        token_of(&secrets, credential_ref).map_err(|err| WebhookError::Store(err.to_string()))?;
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
            ));
        }
    };
    let mut memory = Memory::try_from_state(&channel.state)
        .map_err(|err| WebhookError::Store(err.to_string()))?;
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
    let mut memory = Memory::try_from_state(&channel.state)?;
    let token = token_of(secrets, credential_ref)?;
    if memory.bot.as_deref().unwrap_or("").is_empty() {
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
        save_channel_memory_merged(store, &channel.id, &mut memory, &[])?;
    }
    reconcile_channel_groups(store, secrets, channel);
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
    if let Some(fresh) = store
        .list_channels()?
        .into_iter()
        .find(|c| c.id == channel.id)
    {
        let fresh_mem = Memory::try_from_state(&fresh.state)?;
        memory.chats = fresh_mem.chats;
        memory.pending_managed_bots = fresh_mem.pending_managed_bots;
        if fresh_mem.bot.is_some() {
            memory.bot = fresh_mem.bot;
        }
    }
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
    if let Ok(owner_agent) = store.agent(agent_id)
        && (owner_agent.state == "archived"
            || owner_agent.state == "paused"
            || is_agent_explicitly_archived(store.data_dir(), &owner_agent))
    {
        if poll.offset != before {
            memory.offset = poll.offset.max(memory.offset);
            save_channel_memory_merged(store, &channel.id, memory, &[])?;
        }
        return Ok(());
    }
    let attached = attached_agents(store)?;
    let own_bots = known_bot_identities(store, memory.bot.as_deref())?;
    let mut grew = false;
    let mut removed_chats: Vec<i64> = Vec::new();
    let mut removed_pending: Vec<String> = Vec::new();
    for mut one in routed {
        // This bot answers in the chat its update came from, so a post it makes
        // there reaches the other agents on that chat.
        if let Routed::ToAgent { inbound, .. } = &mut one {
            if !inbound.member_changes.is_empty() {
                invalidate_membership_cache(inbound.chat_id);
                let removed_ids: Vec<i64> = inbound
                    .member_changes
                    .iter()
                    .filter(|change| !change.joined)
                    .map(|change| change.user_id)
                    .collect();
                if memory
                    .bot
                    .as_deref()
                    .and_then(|bot| bot.split_once(':'))
                    .and_then(|(id, _)| id.parse::<i64>().ok())
                    .is_some_and(|id| removed_ids.contains(&id))
                {
                    memory.chats.retain(|link| link.chat_id != inbound.chat_id);
                    removed_chats.push(inbound.chat_id);
                    grew = true;
                }
                detach_removed_own_bots_from_chat(store, inbound.chat_id, &removed_ids)?;
                if own_bots
                    .iter()
                    .any(|(id, _)| id.is_some_and(|id| removed_ids.contains(&id)))
                {
                    continue;
                }
            }
            let known = memory
                .chats
                .iter()
                .any(|link| link.agent == agent_id.as_str() && link.chat_id == inbound.chat_id);
            let old_title = memory.title_of(inbound.chat_id).map(str::to_string);
            let is_group = inbound.chat_type == "group" || inbound.chat_type == "supergroup";
            let is_live = std::env::var_os("ANYAGENTS_TELEGRAM_API").is_none();
            if known
                || !is_group
                || !is_live
                || method == "getUpdates"
                || cached_probe_member_status(
                    token,
                    inbound.chat_id,
                    memory.bot.as_deref().unwrap_or(""),
                ) != Some(false)
            {
                removed_chats.retain(|&id| id != inbound.chat_id);
                memory.attach_typed_titled(
                    agent_id.as_str(),
                    inbound.chat_id,
                    Some(&inbound.chat_type),
                    inbound.chat_title.as_deref(),
                );
            } else {
                continue;
            }
            if is_group {
                discover_peer_bots_in_chat(
                    store,
                    token,
                    &channel.id,
                    inbound.chat_id,
                    &inbound.chat_type,
                );
            }
            let title_changed = memory.title_of(inbound.chat_id) != old_title.as_deref();
            grew |= !known || title_changed;
            // Drop self-echo / peer-bot echo from Telegram when the sender is one
            // of our own configured bots; `post_text` already fans out bot posts
            // internally as authenticated `agent:<name>` messages.
            if inbound.sender_is_bot
                && is_own_bot(
                    inbound.sender_id,
                    inbound.sender_username.as_deref(),
                    &own_bots,
                )
            {
                continue;
            }
            if !inbound.media.is_empty() {
                let downloaded =
                    download_inbound_media(store.data_dir(), token, &channel.id, &inbound.media);
                if !downloaded.is_empty() {
                    inbound.text.push('\n');
                    inbound.text.push_str(&downloaded.join("\n"));
                }
            }
            if !inbound.quoted_media.is_empty() {
                let downloaded = download_inbound_media(
                    store.data_dir(),
                    token,
                    &channel.id,
                    &inbound.quoted_media,
                );
                if !downloaded.is_empty()
                    && let Some(ref mut quote) = inbound.quote
                {
                    if !quote.text.is_empty() {
                        quote.text.push('\n');
                    }
                    quote.text.push_str(&downloaded.join("\n"));
                }
            }
            // In multi-bot Telegram groups, only the bot whose message was replied to
            // receives `reply_to_message` (`inbound.quote.is_some()`), while peer bots
            // receive the same human message within ~50–100ms with `quote = None`.
            // Coalesce unquoted live group messages briefly so the bot carrying the
            // quote delivers and relays the full quoted context first.
            if is_group && is_live && inbound.quote.is_none() && !inbound.text.starts_with('[') {
                std::thread::sleep(std::time::Duration::from_millis(600));
            }
        }
        // The bot was added to, or removed from, a chat.
        if let Routed::Membership {
            update_id,
            chat_id,
            chat_type,
            joined,
            ..
        } = &one
        {
            invalidate_membership_cache(*chat_id);
            if *joined {
                removed_chats.retain(|&id| id != *chat_id);
                memory.attach_typed(agent_id.as_str(), *chat_id, chat_type.as_deref());
                store.receive(
                    agent_id,
                    anyagents_core::Incoming {
                        text: format!("[Added to group chat {chat_id}]"),
                        sender: Some(format!("telegram:chat:{chat_id}")),
                        source_time_ms: None,
                        source_key: Some(format!(
                            "tg-member-in:{}:{}:{}",
                            chat_id, agent_id, update_id
                        )),
                        reply_to: Some(format!("telegram:chat:{chat_id}")),
                        quote: None,
                        origin: Some(anyagents_core::Origin {
                            chat_id: Some(*chat_id),
                            thread_id: None,
                            message_id: None,
                        }),
                        received_at_ms: None,
                    },
                )?;
            } else {
                memory.chats.retain(|link| link.chat_id != *chat_id);
                removed_chats.push(*chat_id);
                store.receive(
                    agent_id,
                    anyagents_core::Incoming {
                        text: format!("[Removed from group chat {chat_id}]"),
                        sender: Some(format!("telegram:chat:{chat_id}")),
                        source_time_ms: None,
                        source_key: Some(format!(
                            "tg-member-out:{}:{}:{}",
                            chat_id, agent_id, update_id
                        )),
                        reply_to: None,
                        quote: None,
                        origin: Some(anyagents_core::Origin {
                            chat_id: Some(*chat_id),
                            thread_id: None,
                            message_id: None,
                        }),
                        received_at_ms: None,
                    },
                )?;
            }
            grew = true;
        }
        if let Routed::ManagedBot { event, .. } = &one {
            handle_managed_bot_event(
                store,
                channel,
                agent_id,
                token,
                memory,
                event,
                &mut removed_pending,
            )?;
            grew = true;
            continue;
        }
        let peers = peer_ids(store, &poll, &one, agent_id.as_str(), &attached)?;
        deliver_to_mailbox(store, &one, &peers)?;
        if let Routed::ToAgent { inbound, .. } = &one
            && (inbound.chat_type == "group" || inbound.chat_type == "supergroup")
        {
            relay_inbound_to_group_peers(store, agent_id, inbound)?;
        }
    }
    // The offset and the chats are saved only once the updates are stored, so a
    // restart asks for what it did not keep and a repeat is one row.
    if poll.offset != before || grew {
        memory.offset = poll.offset.max(memory.offset);
        save_channel_memory_merged_with_pending(
            store,
            &channel.id,
            memory,
            &removed_chats,
            &removed_pending,
        )?;
    }
    Ok(())
}

fn known_bot_identities(
    store: &Store,
    current_bot: Option<&str>,
) -> Result<Vec<(Option<i64>, String)>, anyagents_core::Error> {
    let mut out = Vec::new();
    fn push_bot(out: &mut Vec<(Option<i64>, String)>, raw: &str) {
        if let Some((id_str, username)) = raw.split_once(':') {
            let id = id_str.trim().parse::<i64>().ok();
            let user = username.trim().trim_start_matches('@').to_ascii_lowercase();
            if id.is_some() || !user.is_empty() {
                out.push((id, user));
            }
        }
    }
    if let Some(bot) = current_bot {
        push_bot(&mut out, bot);
    }
    let secrets = FileCredentialStore::new(store.data_dir());
    {
        for ch in store.list_channels()? {
            if ch.kind == "telegram" {
                let mem = Memory::try_from_state(&ch.state)?;
                if let Some(bot) = mem.bot.as_deref() {
                    push_bot(&mut out, bot);
                }
                let uname = ch
                    .identity
                    .as_deref()
                    .map(|u| u.trim().trim_start_matches('@').to_ascii_lowercase())
                    .unwrap_or_default();
                let tok_id = ch
                    .credential_ref
                    .as_deref()
                    .and_then(|cred| token_of(&secrets, cred).ok())
                    .and_then(|tok| tok.split(':').next()?.trim().parse::<i64>().ok());
                if tok_id.is_some() || !uname.is_empty() {
                    out.push((tok_id, uname));
                }
            }
        }
    }
    Ok(out)
}

fn is_own_bot(
    sender_id: Option<i64>,
    sender_username: Option<&str>,
    own_bots: &[(Option<i64>, String)],
) -> bool {
    let uname = sender_username
        .map(|u| u.trim().trim_start_matches('@').to_ascii_lowercase())
        .filter(|u| !u.is_empty());
    own_bots.iter().any(|(bot_id, bot_user)| {
        (sender_id.is_some() && sender_id == *bot_id)
            || (uname.is_some()
                && !bot_user.is_empty()
                && uname.as_deref() == Some(bot_user.as_str()))
    })
}

fn detach_removed_own_bots_from_chat(
    store: &Store,
    chat_id: i64,
    removed_ids: &[i64],
) -> Result<(), anyagents_core::Error> {
    for ch in store.list_channels()? {
        if ch.kind != "telegram" {
            continue;
        }
        let mut mem = Memory::try_from_state(&ch.state)?;
        let matched = mem
            .bot
            .as_deref()
            .and_then(|b| b.split_once(':'))
            .and_then(|(id, _)| id.parse::<i64>().ok())
            .is_some_and(|id| removed_ids.contains(&id));
        if matched {
            mem.chats.retain(|l| l.chat_id != chat_id);
            save_channel_memory_merged(store, &ch.id, &mut mem, &[chat_id])?;
        }
    }
    Ok(())
}

fn download_inbound_media(
    data_dir: &std::path::Path,
    token: &str,
    channel_id: &anyagents_core::ChannelId,
    media: &[MediaAttachment],
) -> Vec<String> {
    let media_dir = data_dir.join("media");
    let _ = std::fs::create_dir_all(&media_dir);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&media_dir, std::fs::Permissions::from_mode(0o755));
    }
    let mut notes = Vec::new();
    for att in media {
        if att.kind == "sticker" {
            continue;
        }
        // Telegram Bot API `getFile` supports files up to 20 MB.
        if att.file_size.is_some_and(|sz| sz > 20 * 1024 * 1024) {
            continue;
        }
        let Ok(resp) = call_api(
            token,
            &get_file(&att.file_id),
            SHORT_TIMEOUT_S,
            Some(channel_id),
        ) else {
            continue;
        };
        let Ok(remote_path) = file_path_of(&resp) else {
            continue;
        };
        let ext = std::path::Path::new(&remote_path)
            .extension()
            .and_then(|e| e.to_str())
            .filter(|e| !e.is_empty())
            .unwrap_or(match att.kind.as_str() {
                "photo" => "jpg",
                "video" | "video_note" | "animation" => "mp4",
                "voice" => "ogg",
                "audio" => "mp3",
                _ => "bin",
            });
        let base_id: String = att
            .file_unique_id
            .as_deref()
            .unwrap_or(&att.file_id)
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
            .take(32)
            .collect();
        let file_name = match &att.file_name {
            Some(orig) => {
                let sanitized: String = orig
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .take(64)
                    .collect();
                format!("{base_id}_{sanitized}")
            }
            None => format!("{}_{base_id}.{ext}", att.kind),
        };
        let dest = media_dir.join(format!("complete_{file_name}"));
        if !dest.is_file() {
            let url = format!("{}/file/bot{token}/{remote_path}", api_base());
            let agent = ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(SHORT_TIMEOUT_S)))
                .max_redirects(0)
                .build()
                .new_agent();
            if let Ok(response) = agent.get(&url).call()
                && response.status().as_u16() == 200
            {
                let mut reader = response.into_body().into_reader();
                if persist_media(&mut reader, &dest, att.file_size).is_err() {
                    eprintln!("Telegram media transfer incomplete; attachment not cached");
                }
            }
        }
        if dest.is_file() {
            notes.push(format!(
                "[Local media ({}) file: {}]",
                att.kind,
                dest.display()
            ));
        }
    }
    notes
}

/// A failed read/write never publishes a path that a subsequent call can reuse.
fn persist_media(
    reader: &mut impl std::io::Read,
    dest: &std::path::Path,
    expected: Option<i64>,
) -> std::io::Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let staging = dest.with_extension(format!(
        "partial-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staging)?;
        let copied = std::io::copy(reader, &mut file)?;
        if expected.is_some_and(|size| size < 0 || copied != size as u64) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "media length mismatch",
            ));
        }
        file.sync_all()?;
        std::fs::rename(&staging, dest)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(staging);
    }
    result
}

const MEMBERSHIP_CACHE_TTL: Duration = Duration::from_secs(120);

#[allow(clippy::type_complexity)]
fn live_membership_cache()
-> &'static std::sync::Mutex<std::collections::HashMap<(i64, String), (bool, std::time::Instant)>> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(i64, String), (bool, std::time::Instant)>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

fn invalidate_membership_cache(chat_id: i64) {
    if let Ok(mut guard) = live_membership_cache().lock() {
        guard.retain(|(cid, _), _| *cid != chat_id);
    }
}

fn cached_probe_member_status(token: &str, chat_id: i64, bot: &str) -> Option<bool> {
    if std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some() {
        return probe_member_status(token, chat_id, bot);
    }
    let Some(bot_id) = bot
        .split(':')
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Some(false);
    };
    let key = (chat_id, bot_id.to_string());
    if let Ok(guard) = live_membership_cache().lock()
        && let Some((is_in, checked_at)) = guard.get(&key)
        && checked_at.elapsed() < MEMBERSHIP_CACHE_TTL
    {
        return Some(*is_in);
    }
    let status = probe_member_status(token, chat_id, bot);
    if let Some(is_in) = status
        && let Ok(mut guard) = live_membership_cache().lock()
    {
        guard.insert(key, (is_in, std::time::Instant::now()));
    }
    status
}

#[allow(dead_code)]
fn cached_probe_members(token: &str, chat_id: i64, bot: &str) -> bool {
    cached_probe_member_status(token, chat_id, bot).unwrap_or(false)
}

/// Reconcile configured Telegram bots' membership in `chat_id` against
/// Telegram's authoritative `getChatMember` (backed by the event-invalidated
/// read-through TTL cache): attach bots that are present and prune bots that
/// have been removed or left.
fn discover_peer_bots_in_chat(
    store: &Store,
    token: &str,
    current_channel_id: &anyagents_core::ChannelId,
    chat_id: i64,
    chat_type: &str,
) {
    let is_live = std::env::var_os("ANYAGENTS_TELEGRAM_API").is_none();
    let Ok(channels) = store.list_channels() else {
        return;
    };
    for other in channels {
        if other.id == *current_channel_id || other.kind != "telegram" {
            continue;
        }
        let Some(other_agent_id) = other.agents.first() else {
            continue;
        };
        let Ok(other_agent) = store.agent(other_agent_id) else {
            continue;
        };
        let mut other_mem = match Memory::try_from_state(&other.state) {
            Ok(memory) => memory,
            Err(err) => {
                eprintln!("{err}");
                return;
            }
        };
        let already_attached = other_mem.chats.iter().any(|l| l.chat_id == chat_id);
        if other_agent.state == "archived"
            || other_agent.state == "paused"
            || is_agent_explicitly_archived(store.data_dir(), &other_agent)
        {
            if already_attached && chat_id < 0 {
                other_mem.chats.retain(|l| l.chat_id != chat_id);
                let _ = save_channel_memory_merged(store, &other.id, &mut other_mem, &[chat_id]);
            }
            continue;
        }
        if !is_live && already_attached {
            continue;
        }
        let Some(bot) = other_mem.bot.as_deref().map(str::to_string) else {
            continue;
        };
        let other_token = if is_live {
            other
                .credential_ref
                .as_deref()
                .and_then(|cred| token_of(&FileCredentialStore::new(store.data_dir()), cred).ok())
        } else {
            None
        };
        let probe_tok = other_token.as_deref().unwrap_or(token);
        match cached_probe_member_status(probe_tok, chat_id, &bot) {
            Some(true) if !already_attached => {
                other_mem.attach_typed(other_agent_id.as_str(), chat_id, Some(chat_type));
                let _ = save_channel_memory_merged(store, &other.id, &mut other_mem, &[]);
            }
            Some(false) if is_live && already_attached => {
                other_mem.chats.retain(|l| l.chat_id != chat_id);
                let _ = save_channel_memory_merged(store, &other.id, &mut other_mem, &[chat_id]);
            }
            _ => {}
        }
    }
}

/// Relay an inbound group message to any other agent attached to `inbound.chat_id`
/// so bots with Telegram Group Privacy enabled still receive every group message.
/// `Store::receive` deduplicates by `(chat_id, message_id)` so a bot that also
/// receives the update directly from Telegram stores it only once.
fn relay_inbound_to_group_peers(
    store: &Store,
    receiver_id: &anyagents_core::AgentId,
    inbound: &anyagents_channel_telegram::Inbound,
) -> Result<(), anyagents_core::Error> {
    for peer in telegram_chat_agents(store, inbound.chat_id)? {
        if &peer.id == receiver_id || peer.state == "archived" || peer.state == "paused" {
            continue;
        }
        let incoming = anyagents_core::Incoming {
            text: inbound.text.clone(),
            sender: anyagents_channel_telegram::telegram_sender(inbound),
            source_time_ms: Some(inbound.source_time_s * 1000),
            source_key: Some(format!(
                "tg-in:{}:{}:{}",
                inbound.chat_id, receiver_id, inbound.message_id
            )),
            reply_to: Some(anyagents_channel_telegram::reply_target(inbound)),
            quote: inbound.quote.clone(),
            origin: Some(anyagents_core::Origin {
                chat_id: Some(inbound.chat_id),
                thread_id: inbound.thread_id,
                message_id: Some(inbound.message_id),
            }),
            received_at_ms: None,
        };
        store.receive(&peer.id, incoming)?;
    }
    Ok(())
}

/// Whether this bot is in the chat, asked of Telegram at cold start. A chat
/// first seen after a restart has no `my_chat_member` update to replay, so the
/// membership is read directly instead of inferred.
fn probe_member_status(token: &str, chat_id: i64, bot: &str) -> Option<bool> {
    let Some(user) = bot.split(':').next().and_then(|id| id.parse::<i64>().ok()) else {
        return Some(false);
    };
    crate::telegram::chat_member_status(token, chat_id, user)
}

#[allow(dead_code)]
fn probe_members(token: &str, chat_id: i64, bot: &str) -> bool {
    probe_member_status(token, chat_id, bot).unwrap_or(false)
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
        if let Ok(agent) = store.agent_by_name(name)
            && agent.state != "archived"
            && agent.state != "paused"
        {
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
        let memory = Memory::try_from_state(&one.state)?;
        let channel_agents: Vec<(String, String)> = one
            .agents
            .iter()
            .filter_map(|id| {
                store
                    .agent(id)
                    .ok()
                    .filter(|agent| {
                        agent.state != "archived"
                            && agent.state != "paused"
                            && !is_agent_explicitly_archived(store.data_dir(), agent)
                    })
                    .map(|agent| (agent.name, agent.id.to_string()))
            })
            .collect();
        for link in memory.chats {
            let Some((canonical_name, _)) = channel_agents
                .iter()
                .find(|(name, id)| name == &link.agent || id == &link.agent)
            else {
                continue;
            };
            if !links
                .iter()
                .any(|(agent, chat)| agent == canonical_name && *chat == link.chat_id)
            {
                links.push((canonical_name.clone(), link.chat_id));
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

/// Maximum characters per Telegram `sendRichMessage` chunk (Bot API 10.1+ allows up to 32,768 UTF-8 chars).
const MAX_TELEGRAM_CHUNK_CHARS: usize = 30000;

/// Split a long message into chunks that stay within Telegram's 32,768-character limit.
fn split_telegram_chunks(text: &str) -> Vec<&str> {
    if text.chars().count() <= MAX_TELEGRAM_CHUNK_CHARS {
        return vec![text];
    }
    let mut chunks = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        if rest.chars().count() <= MAX_TELEGRAM_CHUNK_CHARS {
            chunks.push(rest);
            break;
        }
        let max_byte = rest
            .char_indices()
            .nth(MAX_TELEGRAM_CHUNK_CHARS)
            .map(|(idx, _)| idx)
            .unwrap_or(rest.len());
        let window = &rest[..max_byte];
        let split_at = window
            .rfind("\n\n")
            .filter(|&pos| pos >= max_byte / 2)
            .map(|pos| pos + 2)
            .or_else(|| {
                window
                    .rfind('\n')
                    .filter(|&pos| pos >= max_byte / 2)
                    .map(|pos| pos + 1)
            })
            .or_else(|| {
                window
                    .rfind(' ')
                    .filter(|&pos| pos >= max_byte / 2)
                    .map(|pos| pos + 1)
            })
            .unwrap_or(max_byte);
        let (head, tail) = rest.split_at(split_at);
        chunks.push(head.trim_end_matches('\n'));
        rest = tail.trim_start_matches('\n');
    }
    if chunks.is_empty() {
        vec![text]
    } else {
        chunks
    }
}

/// Rewrite internal `@<agent_name>` handles in outbound Telegram text to the
/// agent's actual Telegram `@<bot_username>` so mentions in Telegram groups
/// link to our real bots instead of unrelated Telegram accounts.
fn translate_agent_mentions_for_telegram(store: &Store, text: &str) -> String {
    if !text.contains('@') {
        return text.to_string();
    }
    let Ok(channels) = store.list_channels() else {
        return text.to_string();
    };
    let mut agent_to_bot: Vec<(String, String)> = Vec::new();
    for channel in channels {
        if channel.kind != "telegram" {
            continue;
        }
        let mem = match Memory::try_from_state(&channel.state) {
            Ok(memory) => memory,
            Err(err) => {
                eprintln!("{err}");
                continue;
            }
        };
        let Some(bot_username) = mem
            .bot
            .as_deref()
            .and_then(|b| b.split_once(':'))
            .map(|(_, u)| u.trim().trim_start_matches('@'))
            .filter(|u| !u.is_empty() && !u.eq_ignore_ascii_case("bot"))
        else {
            continue;
        };
        for aid in &channel.agents {
            if let Ok(agent) = store.agent(aid) {
                let name_lower = agent.name.to_ascii_lowercase();
                if !name_lower.eq_ignore_ascii_case(bot_username)
                    && !agent_to_bot.iter().any(|(n, _)| n == &name_lower)
                {
                    agent_to_bot.push((name_lower, bot_username.to_string()));
                }
                if let Ok(aliases) = store.agent_aliases(&agent.id) {
                    for alias in aliases {
                        let al = alias.to_ascii_lowercase();
                        if !al.eq_ignore_ascii_case(bot_username)
                            && !agent_to_bot.iter().any(|(n, _)| n == &al)
                        {
                            agent_to_bot.push((al, bot_username.to_string()));
                        }
                    }
                }
            }
        }
    }
    if agent_to_bot.is_empty() {
        return text.to_string();
    }

    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let mut out = String::with_capacity(text.len() + 32);
    let mut i = 0;
    let mut in_backticks = false;
    while i < chars.len() {
        let (byte_idx, ch) = chars[i];
        if ch == '`' {
            in_backticks = !in_backticks;
            out.push(ch);
            i += 1;
            continue;
        }
        if !in_backticks && ch == '@' {
            let valid_prefix = i == 0 || {
                let prev = chars[i - 1].1;
                !prev.is_ascii_alphanumeric()
                    && prev != '_'
                    && prev != '.'
                    && prev != '-'
                    && prev != '/'
            };
            if valid_prefix {
                let mut j = i + 1;
                while j < chars.len() {
                    let c = chars[j].1;
                    if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                        j += 1;
                    } else {
                        break;
                    }
                }
                if j > i + 1 {
                    let start = chars[i + 1].0;
                    let end = if j < chars.len() {
                        chars[j].0
                    } else {
                        text.len()
                    };
                    let handle = &text[start..end];
                    if let Some((_, bot_user)) = agent_to_bot
                        .iter()
                        .find(|(agent_name, _)| agent_name.eq_ignore_ascii_case(handle))
                    {
                        out.push('@');
                        out.push_str(bot_user);
                        i = j;
                        continue;
                    }
                }
            }
        }
        let next_byte = if i + 1 < chars.len() {
            chars[i + 1].0
        } else {
            text.len()
        };
        out.push_str(&text[byte_idx..next_byte]);
        i += 1;
    }
    out
}

/// Telegram bot display name: the agent name title-cased (`helper-bot` -> `Helper Bot`).
/// The role is deliberately excluded so role edits never rename the bot.
pub fn format_telegram_bot_display_name(name: &str) -> String {
    name.split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn url_encode_query_param(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else if byte == b' ' {
            out.push('+');
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn default_suggested_bot_username(agent_name: &str) -> String {
    let clean: String = agent_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    let mut collapsed = String::new();
    let mut prev_under = false;
    for ch in clean.chars() {
        if ch == '_' {
            if !prev_under && !collapsed.is_empty() {
                collapsed.push('_');
            }
            prev_under = true;
        } else {
            collapsed.push(ch);
            prev_under = false;
        }
    }
    let trimmed = collapsed.trim_matches('_');
    // Preserve the entire caller-supplied identity; no organization prefixes.
    // A stable digest prevents truncation or punctuation normalization collisions.
    use sha2::{Digest, Sha256};
    let hash = format!("{:x}", Sha256::digest(agent_name.as_bytes()));
    let stem: String = trimmed.chars().take(19).collect();
    format!(
        "{}_{hash}_bot",
        if stem.is_empty() { "agent" } else { &stem },
        hash = &hash[..8]
    )
}

fn normalize_bot_username(raw: &str, fallback_agent_name: &str) -> String {
    let clean = raw.trim().trim_start_matches('@');
    if clean.is_empty() {
        return default_suggested_bot_username(fallback_agent_name);
    }
    let sanitized: String = clean
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches('_');
    if trimmed.to_ascii_lowercase().ends_with("bot") {
        trimmed.chars().take(32).collect()
    } else {
        let max_len = 32usize.saturating_sub(4);
        let prefix: String = trimmed.chars().take(max_len).collect();
        format!("{}_bot", prefix.trim_end_matches('_'))
    }
}

/// Bind a Telegram bot token to `target_agent`, updating `channels.identity`,
/// preserving `target_agent` visibility, syncing its Telegram display name
/// and profile descriptions, and registering its webhook when webhook mode is active.
pub fn bind_agent_telegram_token(
    store: &Store,
    data_dir: &std::path::Path,
    target_agent: &anyagents_core::Agent,
    raw_token: &str,
    known_identity: Option<(i64, &str)>,
) -> Result<serde_json::Value, anyagents_core::Error> {
    let token = raw_token.trim();
    if token.is_empty() || token.chars().any(|c| c.is_whitespace() || c == '/') {
        return Err(anyagents_core::Error::Message(
            "invalid telegram bot token".into(),
        ));
    }
    let is_live = std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && std::env::var_os("RUST_TEST_THREADS").is_none()
        && std::env::var_os("CARGO_TARGET_TMPDIR").is_none()
        && std::env::var_os("NEXTEST").is_none();

    let resolved_identity = match known_identity {
        Some((id, uname)) if !uname.trim().is_empty() => {
            Some((id, uname.trim().trim_start_matches('@').to_string()))
        }
        _ if is_live || std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some() => {
            let response = call_api(token, &get_me(), SHORT_TIMEOUT_S, None)?;
            let ident = bot_identity(&response).map_err(|_| {
                anyagents_core::Error::Message(
                    "Telegram getMe returned invalid bot identity".into(),
                )
            })?;
            Some((ident.id, ident.username))
        }
        _ => token
            .split_once(':')
            .and_then(|(id_s, _)| id_s.parse::<i64>().ok())
            .map(|id| (id, format!("{}_bot", target_agent.name))),
    };

    if resolved_identity
        .as_ref()
        .is_none_or(|(id, username)| *id <= 0 || username.is_empty())
    {
        return Err(anyagents_core::Error::Message(
            "Telegram token has no validated bot identity".into(),
        ));
    }
    let secrets = FileCredentialStore::new(data_dir);
    let existing_channels = store.list_channels()?;
    for ch in &existing_channels {
        if ch.kind == "telegram" {
            Memory::try_from_state(&ch.state)?;
        }
    }

    // Detach target_agent from any other telegram channel it was previously bound to
    // unless that channel is the one we are updating.
    let mut reusable_channel: Option<Channel> = None;
    for ch in &existing_channels {
        if ch.kind != "telegram" {
            continue;
        }
        let same_token = ch
            .credential_ref
            .as_deref()
            .and_then(|cred| token_of(&secrets, cred).ok())
            .is_some_and(|t| t == token);
        let same_identity = resolved_identity.as_ref().is_some_and(|(_, uname)| {
            ch.identity.as_deref().is_some_and(|id_str| {
                let u = id_str.split_once(':').map(|(_, u)| u).unwrap_or(id_str);
                u.trim().trim_start_matches('@').eq_ignore_ascii_case(uname)
            })
        });
        if same_token || same_identity {
            reusable_channel = Some(ch.clone());
            break;
        }
    }
    if reusable_channel.is_none() {
        for ch in &existing_channels {
            if ch.kind == "telegram" && ch.agents.contains(&target_agent.id) && ch.agents.len() == 1
            {
                reusable_channel = Some(ch.clone());
                break;
            }
        }
    }

    // Every poller looks at the bindings again once these writes are done,
    // whether or not all of them succeed.
    let _changed = ChangedOnDrop;
    let channel_id = if let Some(ch) = reusable_channel {
        for other in &existing_channels {
            if other.id != ch.id && other.agents.contains(&target_agent.id) {
                store.detach_channel_agent(&other.id, &target_agent.id)?;
            }
        }
        // Legacy credentials can be files at `channels/<id>`. Keep replacements
        // in a separate namespace, preserving the old bytes on partial failure.
        let cred_ref = format!(
            "channel-bindings/{}/{}",
            ch.id,
            anyagents_core::MessageId::generate()
        );
        secrets.write(&cred_ref, token.as_bytes())?;
        store.set_credential_ref(&ch.id, &cred_ref)?;
        store.detach_channel_agents(&ch.id)?;
        store.attach_channel(&ch.id, &target_agent.id)?;
        ch.id
    } else {
        for other in &existing_channels {
            if other.agents.contains(&target_agent.id) {
                store.detach_channel_agent(&other.id, &target_agent.id)?;
            }
        }
        let channel = store.create_channel(anyagents_core::NewChannel {
            kind: "telegram".to_string(),
            identity: None,
            config: "{}".to_string(),
            credential_ref: None,
        })?;
        let cred_ref = format!("channels/{}", channel.id);
        secrets.write(&cred_ref, token.as_bytes())?;
        store.set_credential_ref(&channel.id, &cred_ref)?;
        store.attach_channel(&channel.id, &target_agent.id)?;
        channel.id
    };

    if let Some((bot_id, ref bot_username)) = resolved_identity {
        store.set_channel_identity(&channel_id, Some(bot_username.as_str()))?;
        let mut mem = store
            .list_channels()?
            .into_iter()
            .find(|c| c.id == channel_id)
            .ok_or_else(|| {
                anyagents_core::Error::Message("Telegram channel disappeared during binding".into())
            })
            .and_then(|c| Memory::try_from_state(&c.state))?;
        mem.bot = Some(format!("{bot_id}:{bot_username}"));
        save_channel_memory_merged(store, &channel_id, &mut mem, &[])?;
    }

    let updated_agent = store.agent(&target_agent.id)?;
    sync_agent_telegram_bot_name(store, data_dir, &updated_agent);

    if let Ok(webhook_base) = std::env::var("ANYAGENTS_TELEGRAM_WEBHOOK_URL") {
        let trimmed = webhook_base.trim();
        if !trimmed.is_empty()
            && let Ok(all) = store.list_channels()
            && let Some(fresh_ch) = all.into_iter().find(|c| c.id == channel_id)
        {
            let _ = ensure_webhook(store, &secrets, &fresh_ch, trimmed);
        }
    }

    let display_name = format_telegram_bot_display_name(&updated_agent.name);
    Ok(serde_json::json!({
        "status": "bound",
        "kind": "telegram",
        "agent": updated_agent.name,
        "agent_id": updated_agent.id.as_str(),
        "scope": updated_agent.scope(),
        "public": updated_agent.is_public(),
        "channel_id": channel_id.as_str(),
        "bot_id": resolved_identity.as_ref().map(|(id, _)| *id),
        "bot_username": resolved_identity.as_ref().map(|(_, u)| format!("@{u}")),
        "display_name": display_name,
    }))
}

fn managed_bot_target(
    store: &Store,
    memory: &Memory,
    username: &str,
) -> Result<Option<anyagents_core::Agent>, anyagents_core::Error> {
    memory
        .pending_managed_bots
        .get(username)
        .map(|target| {
            let id = anyagents_core::AgentId::parse(target)?;
            store.agent(&id)
        })
        .transpose()
}

fn handle_managed_bot_event(
    store: &Store,
    manager_channel: &Channel,
    manager_agent_id: &anyagents_core::AgentId,
    manager_token: &str,
    memory: &mut Memory,
    event: &ManagedBotEvent,
    removed_pending: &mut Vec<String>,
) -> Result<(), anyagents_core::Error> {
    let clean_uname = event
        .bot_username
        .trim()
        .trim_start_matches('@')
        .to_ascii_lowercase();
    let token_resp = call_api(
        manager_token,
        &get_managed_bot_token(event.bot_id),
        SHORT_TIMEOUT_S,
        Some(&manager_channel.id),
    )?;
    let new_bot_token = managed_bot_token_of(&token_resp).map_err(|_| {
        anyagents_core::Error::Message("Telegram returned invalid managed bot token".into())
    })?;
    // Correlate only the exact persisted creation request and immutable agent ID.
    // Unmatched events are retained as unassigned; names never select an owner.
    let matched_target = managed_bot_target(store, memory, &clean_uname)?;
    let creator_note = event
        .creator_name
        .as_deref()
        .map(|n| format!(" by {n}"))
        .unwrap_or_default();

    if let Some(target_agent) = matched_target {
        bind_agent_telegram_token(
            store,
            store.data_dir(),
            &target_agent,
            &new_bot_token,
            Some((event.bot_id, &event.bot_username)),
        )?;
        let disp = format_telegram_bot_display_name(&target_agent.name);
        let notice = format!(
            "[Telegram Managed Bot @{} (id={}) created{} and automatically bound to agent `{}` (`{}`). Display name synced to \"{}\" with visibility unchanged.]",
            event.bot_username,
            event.bot_id,
            creator_note,
            target_agent.name,
            target_agent.id,
            disp
        );
        store.receive(
            manager_agent_id,
            anyagents_core::Incoming {
                text: notice.clone(),
                sender: Some(format!("agent:{}", target_agent.name)),
                source_time_ms: None,
                source_key: Some(format!(
                    "tg-managed-bot:{}:{}:{}",
                    manager_channel.id, event.bot_id, event.update_id
                )),
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: None,
            },
        )?;
        if &target_agent.id != manager_agent_id {
            store.receive(
                &target_agent.id,
                anyagents_core::Incoming {
                    text: notice,
                    sender: store
                        .agent(manager_agent_id)
                        .ok()
                        .map(|a| format!("agent:{}", a.name)),
                    source_time_ms: None,
                    source_key: Some(format!(
                        "tg-managed-bot-self:{}:{}:{}",
                        target_agent.id, event.bot_id, event.update_id
                    )),
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: None,
                },
            )?;
        }
        removed_pending.push(clean_uname.clone());
        memory.pending_managed_bots.remove(&clean_uname);
        return Ok(());
    }

    // No pending target agent matched yet: store the managed bot channel unattached
    // so it can be bound immediately with `agents(action: "telegram_bot", name: "<subagent>", bot_id: <id>)`.
    let mut existing = None;
    for ch in store.list_channels()? {
        if ch.kind == "telegram" {
            let mem = Memory::try_from_state(&ch.state)?;
            if mem
                .bot
                .as_deref()
                .and_then(|bot| bot.split_once(':'))
                .and_then(|(id, _)| id.parse::<i64>().ok())
                == Some(event.bot_id)
                || (mem.bot.is_none()
                    && ch.agents.is_empty()
                    && ch.identity.as_deref() == Some(clean_uname.as_str()))
            {
                existing = Some(ch);
                break;
            }
        }
    }
    let ch = existing;
    let _changed = ChangedOnDrop;
    let ch = match ch {
        Some(ch) => ch,
        None => store.create_channel(anyagents_core::NewChannel {
            kind: "telegram".into(),
            identity: Some(clean_uname.clone()),
            config: "{}".into(),
            credential_ref: None,
        })?,
    };
    let mut mem = Memory::try_from_state(&ch.state)?;
    let cred_ref = format!("channel-bindings/{}/managed-{}", ch.id, event.update_id);
    let secrets = FileCredentialStore::new(store.data_dir());
    secrets.write(&cred_ref, new_bot_token.as_bytes())?;
    store.set_credential_ref(&ch.id, &cred_ref)?;
    mem.bot = Some(format!("{}:{}", event.bot_id, clean_uname));
    store.set_channel_state(&ch.id, &mem.to_state())?;
    store.receive(
        manager_agent_id,
        anyagents_core::Incoming {
            text: format!(
                "[Telegram Managed Bot @{} (id={}) created{}. Bind it to any sub-agent with `agents(action: \"telegram_bot\", name: \"<subagent>\", bot_id: {})`.]",
                event.bot_username, event.bot_id, creator_note, event.bot_id
            ),
            sender: event.creator_id.map(|id| format!("telegram:user:{id}")),
            source_time_ms: None,
            source_key: Some(format!(
                "tg-managed-bot-unassigned:{}:{}:{}",
                manager_channel.id, event.bot_id, event.update_id
            )),
            reply_to: None,
            quote: None,
            origin: None,
            received_at_ms: None,
        },
    )?;
    Ok(())
}

/// Request a new Telegram Managed Bot (`https://t.me/newbot/<manager_bot>/<bot_username>?name=...`)
/// or immediately bind an existing Managed Bot (`bot_id` via `getManagedBotToken`), existing spare
/// channel (`bot_username`), or raw `token` to `target_agent`.
#[allow(clippy::too_many_arguments)]
pub fn request_or_bind_telegram_channel(
    store: &Store,
    data_dir: &std::path::Path,
    caller_agent: Option<&anyagents_core::Agent>,
    target_agent: &anyagents_core::Agent,
    token: Option<&str>,
    bot_id: Option<i64>,
    bot_username: Option<&str>,
    manager_override: Option<&str>,
    replace_token: bool,
) -> Result<serde_json::Value, anyagents_core::Error> {
    if let Some(tok) = token.map(str::trim).filter(|s| !s.is_empty()) {
        return bind_agent_telegram_token(store, data_dir, target_agent, tok, None);
    }

    let secrets = FileCredentialStore::new(data_dir);
    let all_channels = store.list_channels()?;

    // If `bot_id` is supplied, first check if we already hold its token on disk;
    // otherwise call `getManagedBotToken` (or `replaceManagedBotToken`) through the manager bot.
    if let Some(wanted_bot_id) = bot_id {
        if !replace_token {
            for ch in &all_channels {
                if ch.kind != "telegram" {
                    continue;
                }
                let Some(cred) = ch.credential_ref.as_deref() else {
                    continue;
                };
                if let Ok(tok) = token_of(&secrets, cred)
                    && tok
                        .split(':')
                        .next()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        == Some(wanted_bot_id)
                {
                    return bind_agent_telegram_token(store, data_dir, target_agent, &tok, None);
                }
            }
        }
        let mut last_err = None;
        for ch in candidate_manager_channels(
            store,
            &all_channels,
            caller_agent,
            target_agent,
            manager_override,
        ) {
            let Some(cred) = ch.credential_ref.as_deref() else {
                continue;
            };
            let Ok(mgr_tok) = token_of(&secrets, cred) else {
                continue;
            };
            let call = if replace_token {
                replace_managed_bot_token(wanted_bot_id)
            } else {
                get_managed_bot_token(wanted_bot_id)
            };
            match call_api(&mgr_tok, &call, SHORT_TIMEOUT_S, Some(&ch.id)).and_then(|resp| {
                managed_bot_token_of(&resp)
                    .map_err(|e| anyagents_core::Error::Message(e.to_string()))
            }) {
                Ok(fetched_token) => {
                    return bind_agent_telegram_token(
                        store,
                        data_dir,
                        target_agent,
                        &fetched_token,
                        None,
                    );
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }
        }
        return Err(last_err.unwrap_or_else(|| {
            anyagents_core::Error::Message(format!(
                "could not fetch managed bot token for bot_id={wanted_bot_id}"
            ))
        }));
    }

    // If `bot_username` matches an already-recorded channel that is unattached or attached
    // only to an archived agent (such as a spare bot), bind it immediately!
    if let Some(req_uname) = bot_username
        .map(|s| s.trim().trim_start_matches('@'))
        .filter(|s| !s.is_empty())
    {
        for ch in &all_channels {
            if ch.kind != "telegram" {
                continue;
            }
            let mem = Memory::try_from_state(&ch.state)?;
            let ch_uname = ch
                .identity
                .as_deref()
                .or_else(|| {
                    mem.bot
                        .as_deref()
                        .and_then(|b| b.split_once(':').map(|(_, u)| u))
                })
                .map(|u| u.trim().trim_start_matches('@'));
            if ch_uname.is_some_and(|u| u.eq_ignore_ascii_case(req_uname)) {
                let is_spare = ch.agents.is_empty()
                    || ch.agents.iter().all(|aid| {
                        aid == &target_agent.id
                            || store
                                .agent(aid)
                                .is_ok_and(|a| a.state == "archived" || a.state == "paused")
                    });
                if is_spare
                    && let Some(cred) = ch.credential_ref.as_deref()
                    && let Ok(tok) = token_of(&secrets, cred)
                {
                    return bind_agent_telegram_token(store, data_dir, target_agent, &tok, None);
                }
            }
        }
    }

    // Resolve the Manager Bot with `can_manage_bots = true` to generate the `t.me/newbot/...` link
    // and register the pending binding.
    let candidates = candidate_manager_channels(
        store,
        &all_channels,
        caller_agent,
        target_agent,
        manager_override,
    );
    let is_live = std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && std::env::var_os("RUST_TEST_THREADS").is_none()
        && std::env::var_os("CARGO_TARGET_TMPDIR").is_none()
        && std::env::var_os("NEXTEST").is_none();

    let mut chosen_manager: Option<(Channel, String, bool)> = None;
    let mut primary_caller_bot_info: Option<(String, bool)> = None;

    for (idx, ch) in candidates.into_iter().enumerate() {
        let Some(cred) = ch.credential_ref.as_deref() else {
            continue;
        };
        let Ok(tok) = token_of(&secrets, cred) else {
            continue;
        };
        let mem = Memory::try_from_state(&ch.state)?;
        let fallback_uname = ch
            .identity
            .as_deref()
            .or_else(|| {
                mem.bot
                    .as_deref()
                    .and_then(|b| b.split_once(':').map(|(_, u)| u))
            })
            .unwrap_or("manager_bot")
            .trim()
            .trim_start_matches('@')
            .to_string();
        let (uname, can_manage) = if is_live || std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some()
        {
            match call_api(&tok, &get_me(), 10, Some(&ch.id))
                .ok()
                .and_then(|r| bot_identity(&r).ok())
            {
                Some(ident) => (ident.username, ident.can_manage_bots),
                None => (fallback_uname, false),
            }
        } else {
            (fallback_uname, true)
        };
        if idx == 0 {
            primary_caller_bot_info = Some((uname.clone(), can_manage));
        }
        if can_manage {
            chosen_manager = Some((ch, uname, true));
            break;
        }
        if chosen_manager.is_none() {
            chosen_manager = Some((ch, uname, false));
        }
    }

    let Some((mgr_channel, mgr_username, mgr_can_manage)) = chosen_manager else {
        return Err(anyagents_core::Error::Message(
            "no telegram manager bot channel is configured".into(),
        ));
    };

    let suggested = normalize_bot_username(bot_username.unwrap_or(""), &target_agent.name);
    let display_name = format_telegram_bot_display_name(&target_agent.name);
    let mut mem = Memory::try_from_state(&mgr_channel.state)?;
    mem.pending_managed_bots
        .insert(suggested.to_ascii_lowercase(), target_agent.id.to_string());
    save_channel_memory_merged(store, &mgr_channel.id, &mut mem, &[])?;

    let mgr_agent_name = mgr_channel
        .agents
        .first()
        .and_then(|id| store.agent(id).ok().map(|a| a.name))
        .unwrap_or_else(|| "manager".to_string());
    let newbot_url = format!(
        "https://t.me/newbot/{}/{}?name={}",
        mgr_username,
        suggested,
        url_encode_query_param(&display_name)
    );

    Ok(serde_json::json!({
        "status": "pending_creation",
        "kind": "telegram",
        "agent": target_agent.name,
        "agent_id": target_agent.id.as_str(),
        "manager_agent": mgr_agent_name,
        "manager_channel_id": mgr_channel.id.as_str(),
        "manager_bot": format!("@{mgr_username}"),
        "manager_can_manage_bots": mgr_can_manage,
        "caller_bot": primary_caller_bot_info.as_ref().map(|(u, _)| format!("@{u}")),
        "caller_bot_can_manage_bots": primary_caller_bot_info.as_ref().map(|(_, c)| *c),
        "suggested_bot_username": format!("@{suggested}"),
        "display_name": display_name,
        "newbot_url": newbot_url,
    }))
}

fn candidate_manager_channels(
    store: &Store,
    all_channels: &[Channel],
    caller_agent: Option<&anyagents_core::Agent>,
    target_agent: &anyagents_core::Agent,
    manager_override: Option<&str>,
) -> Vec<Channel> {
    let mut ordered: Vec<Channel> = Vec::new();
    let push_agent_channel = |ordered: &mut Vec<Channel>, agent_id: &anyagents_core::AgentId| {
        for ch in all_channels {
            if ch.kind == "telegram"
                && ch.credential_ref.is_some()
                && ch.agents.contains(agent_id)
                && !ordered.iter().any(|existing| existing.id == ch.id)
            {
                ordered.push(ch.clone());
            }
        }
    };

    if let Some(mgr_name) = manager_override.map(str::trim).filter(|s| !s.is_empty()) {
        let clean = mgr_name
            .strip_prefix("agent:")
            .unwrap_or(mgr_name)
            .trim_start_matches('@');
        if let Ok(mgr_agent) = store.agent_by_name(clean) {
            push_agent_channel(&mut ordered, &mgr_agent.id);
        }
        for ch in all_channels {
            if ch.kind == "telegram"
                && ch.credential_ref.is_some()
                && ch
                    .identity
                    .as_deref()
                    .is_some_and(|id| id.trim_start_matches('@').eq_ignore_ascii_case(clean))
                && !ordered.iter().any(|existing| existing.id == ch.id)
            {
                ordered.push(ch.clone());
            }
        }
    }

    if let Some(caller) = caller_agent {
        push_agent_channel(&mut ordered, &caller.id);
    }

    // Walk the explicitly requested target's parent chain.
    let mut cursor = target_agent.parent_id.clone();
    let mut hops = 0usize;
    while let Some(pid) = cursor {
        push_agent_channel(&mut ordered, &pid);
        cursor = store.agent(&pid).ok().and_then(|a| a.parent_id);
        hops += 1;
        if hops > 16 {
            break;
        }
    }

    // Stable channel order is the fallback, independent of agent names.
    for ch in all_channels {
        if ch.kind == "telegram"
            && ch.credential_ref.is_some()
            && !ordered.iter().any(|existing| existing.id == ch.id)
        {
            ordered.push(ch.clone());
        }
    }
    ordered
}

/// Synchronizes the attached Telegram bot's display name (`setMyName`) and
/// profile descriptions (`setMyShortDescription`, `setMyDescription`) with
/// `format_telegram_bot_display_name(&agent.name)` (e.g. `"Helper Bot"`); the role is not
/// part of the name, so a role-only edit leaves `setMyName` untouched.
pub fn sync_agent_telegram_bot_name(
    store: &Store,
    data_dir: &std::path::Path,
    agent: &anyagents_core::Agent,
) {
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
        || std::env::var_os("RUST_TEST_THREADS").is_some()
        || std::env::var_os("CARGO_TARGET_TMPDIR").is_some()
        || std::env::var_os("NEXTEST").is_some()
    {
        return;
    }
    let Ok(channel) = channel_for(store, &agent.name) else {
        return;
    };
    let Some(credential_ref) = channel.credential_ref.as_deref() else {
        return;
    };
    let Ok(token) = token_of(&FileCredentialStore::new(data_dir), credential_ref) else {
        return;
    };
    let raw_name = format_telegram_bot_display_name(&agent.name);
    let bot_name: String = raw_name.chars().take(64).collect();
    let clean_desc = agent.description.as_deref().unwrap_or("").trim();
    let short_desc: String = if clean_desc.is_empty() {
        bot_name.clone()
    } else {
        format!("{bot_name} — {clean_desc}")
            .chars()
            .take(120)
            .collect()
    };
    let full_desc: String = if clean_desc.is_empty() {
        bot_name.clone()
    } else {
        format!("{bot_name}\n\n{clean_desc}")
            .chars()
            .take(512)
            .collect()
    };

    type SyncedProfileMap = std::collections::HashMap<String, (String, String, String)>;
    static SYNCED_PROFILES: std::sync::OnceLock<std::sync::Mutex<SyncedProfileMap>> =
        std::sync::OnceLock::new();
    let cache =
        SYNCED_PROFILES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let desired = (bot_name.clone(), short_desc.clone(), full_desc.clone());
    if let Ok(guard) = cache.lock()
        && guard.get(channel.id.as_str()) == Some(&desired)
    {
        return;
    }

    let current_first_name = call_api(&token, &get_me(), 10, Some(&channel.id))
        .ok()
        .and_then(|resp| serde_json::from_str::<serde_json::Value>(&resp).ok())
        .and_then(|val| {
            val.pointer("/result/first_name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        });
    if current_first_name.as_deref() != Some(bot_name.as_str()) {
        let _ = call_api(
            &token,
            &BotCall {
                method: "setMyName".to_string(),
                body: serde_json::json!({ "name": bot_name }),
            },
            10,
            Some(&channel.id),
        );
        let _ = call_api(
            &token,
            &BotCall {
                method: "setMyShortDescription".to_string(),
                body: serde_json::json!({ "short_description": short_desc }),
            },
            10,
            Some(&channel.id),
        );
        let _ = call_api(
            &token,
            &BotCall {
                method: "setMyDescription".to_string(),
                body: serde_json::json!({ "description": full_desc }),
            },
            10,
            Some(&channel.id),
        );
    }
    if let Ok(mut guard) = cache.lock() {
        guard.insert(channel.id.as_str().to_string(), desired);
    }
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
    let translated = translate_agent_mentions_for_telegram(store, text);
    let text = translated.as_str();
    let chunks = split_telegram_chunks(text);
    let mut first_message_id = None;
    for (idx, chunk) in chunks.iter().enumerate() {
        let chunk_reply = if idx == 0 { reply_to } else { None };
        let outbound = outbound_call(store, from, chat_id, chunk, chunk_reply, thread)?;
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
                &outbound.call.method,
                Some(&outbound.channel_id),
                "invalid_response",
                None,
                None,
                "Telegram response has no message receipt; delivery may be uncertain",
                None,
            )
        })?;
        if first_message_id.is_none() {
            first_message_id = Some(message_id);
        }
        // Retain each accepted chunk before sending another. A later remote
        // error must not strand the already successful chunks' local relays.
        let (provider_thread, quote) = sent_metadata(&response);
        deliver_locally_with_metadata(
            store,
            from,
            chat_id,
            message_id,
            chunk,
            provider_thread.or(thread),
            quote.as_ref(),
        )?;
    }
    Ok(first_message_id.expect("at least one chunk"))
}

/// Post a GPS location or venue through the agent's bot.
#[allow(clippy::too_many_arguments)]
pub fn post_location(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    latitude: f64,
    longitude: f64,
    title: Option<&str>,
    address: Option<&str>,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<i64, anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
    let call = send_location(
        chat_id, latitude, longitude, title, address, reply_to, thread,
    );
    let response = match call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id)) {
        Ok(resp) => resp,
        Err(err)
            if reply_to.is_some() && err.to_string().contains("\"class\":\"invalid_reply\"") =>
        {
            let retry = send_location(chat_id, latitude, longitude, title, address, None, thread);
            call_api(&token, &retry, SHORT_TIMEOUT_S, Some(&channel.id))?
        }
        Err(err) => return Err(err),
    };
    let message_id = sent_message_id(&response).map_err(|_| {
        api_error(
            &call.method,
            Some(&channel.id),
            "invalid_response",
            None,
            None,
            "Telegram response has no message receipt; delivery may be uncertain",
            None,
        )
    })?;
    let summary = match (title, address) {
        (Some(t), Some(a)) if !t.is_empty() && !a.is_empty() => {
            format!("[Venue: {t} — {a} (GPS: {latitude:.6}, {longitude:.6})]")
        }
        _ => format!("[Location GPS: latitude={latitude:.6}, longitude={longitude:.6}]"),
    };
    deliver_locally_with_metadata(store, from, chat_id, message_id, &summary, thread, None)?;
    Ok(message_id)
}

/// Post an interactive poll through the agent's bot.
#[allow(clippy::too_many_arguments)]
pub fn post_poll(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    question: &str,
    options: &[String],
    is_anonymous: Option<bool>,
    allows_multiple_answers: Option<bool>,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<i64, anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
    let call = send_poll(
        chat_id,
        question,
        options,
        is_anonymous,
        allows_multiple_answers,
        reply_to,
        thread,
    );
    let response = match call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id)) {
        Ok(resp) => resp,
        Err(err)
            if reply_to.is_some() && err.to_string().contains("\"class\":\"invalid_reply\"") =>
        {
            let retry = send_poll(
                chat_id,
                question,
                options,
                is_anonymous,
                allows_multiple_answers,
                None,
                thread,
            );
            call_api(&token, &retry, SHORT_TIMEOUT_S, Some(&channel.id))?
        }
        Err(err) => return Err(err),
    };
    let message_id = sent_message_id(&response).map_err(|_| {
        api_error(
            "sendPoll",
            Some(&channel.id),
            "invalid_response",
            None,
            None,
            "Telegram response has no message receipt; delivery may be uncertain",
            None,
        )
    })?;
    let summary = format!("[Poll: \"{question}\" | Options: {}]", options.join(" / "));
    deliver_locally_with_metadata(store, from, chat_id, message_id, &summary, thread, None)?;
    Ok(message_id)
}

/// Post a media item (photo, video, audio/music, voice, document, animation) through
/// the agent's bot. Accepts an `https://` URL, an existing Telegram `file_id`,
/// or a local filesystem path (uploaded via `multipart/form-data`).
#[allow(clippy::too_many_arguments)]
pub fn post_media(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    kind: &str,
    source: &str,
    caption: Option<&str>,
    title: Option<&str>,
    performer: Option<&str>,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<i64, anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
    let local_path = std::path::Path::new(source);
    let response = if !source.starts_with("http://")
        && !source.starts_with("https://")
        && local_path.is_file()
    {
        match call_api_multipart(
            &token,
            &channel.id,
            chat_id,
            kind,
            local_path,
            caption,
            title,
            performer,
            reply_to,
            thread,
        ) {
            Ok(resp) => resp,
            Err(err)
                if reply_to.is_some()
                    && err.to_string().contains("\"class\":\"invalid_reply\"") =>
            {
                call_api_multipart(
                    &token,
                    &channel.id,
                    chat_id,
                    kind,
                    local_path,
                    caption,
                    title,
                    performer,
                    None,
                    thread,
                )?
            }
            Err(err) => return Err(err),
        }
    } else {
        let call = send_media_ref(
            chat_id, kind, source, caption, title, performer, reply_to, thread,
        );
        match call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id)) {
            Ok(resp) => resp,
            Err(err)
                if reply_to.is_some()
                    && err.to_string().contains("\"class\":\"invalid_reply\"") =>
            {
                let retry = send_media_ref(
                    chat_id, kind, source, caption, title, performer, None, thread,
                );
                call_api(&token, &retry, SHORT_TIMEOUT_S, Some(&channel.id))?
            }
            Err(err) => return Err(err),
        }
    };
    let message_id = sent_message_id(&response).map_err(|_| {
        api_error(
            "sendDocument",
            Some(&channel.id),
            "invalid_response",
            None,
            None,
            "Telegram response has no message receipt; delivery may be uncertain",
            None,
        )
    })?;
    let summary = match caption.filter(|c| !c.trim().is_empty()) {
        Some(cap) => format!("{cap}\n[Attachment ({kind}): {source}]"),
        None => format!("[Attachment ({kind}): {source}]"),
    };
    deliver_locally_with_metadata(store, from, chat_id, message_id, &summary, thread, None)?;
    Ok(message_id)
}

/// Post an interactive checklist through the agent's bot. Uses Telegram's native
/// `sendChecklist` when `business_connection_id` is supplied; otherwise posts a
/// rich interactive checklist that can be toggled in-place via `interact(action="check" | "uncheck")`.
#[allow(clippy::too_many_arguments)]
pub fn post_checklist(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    title: &str,
    tasks: &[String],
    business_connection_id: Option<&str>,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<i64, anyagents_core::Error> {
    if let Some(bc_id) = business_connection_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let channel = channel_for(store, from)?;
        let Some(credential_ref) = channel.credential_ref else {
            return Err(anyagents_core::Error::Message(
                "telegram channel has no credential".into(),
            ));
        };
        let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
        let call = send_checklist(chat_id, bc_id, title, tasks, reply_to, thread);
        if let Ok(resp) = call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id))
            && let Ok(message_id) = sent_message_id(&resp)
        {
            let formatted = format_checklist_markdown(title, tasks);
            deliver_locally_with_metadata(
                store, from, chat_id, message_id, &formatted, thread, None,
            )?;
            return Ok(message_id);
        }
    }
    let formatted = format_checklist_markdown(title, tasks);
    post_text(store, data_dir, from, chat_id, &formatted, reply_to, thread)
}

/// Post a rich Instant View article through the agent's bot. When `content` is
/// supplied without `url`, publishes a Telegraph (`telegra.ph`) Instant View page
/// and posts the resulting Instant View card to Telegram (falling back to inline
/// formatted HTML if Telegraph is offline).
#[allow(clippy::too_many_arguments)]
pub fn post_article(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    title: &str,
    content: Option<&str>,
    url: Option<&str>,
    author_name: Option<&str>,
    caption: Option<&str>,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<i64, anyagents_core::Error> {
    let resolved_url = match url.map(str::trim).filter(|s| !s.is_empty()) {
        Some(existing) => Some(existing.to_string()),
        None => content
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(|body| {
                publish_telegraph_article(data_dir, title, body, author_name.or(Some(from)))
            }),
    };
    let text = match resolved_url {
        Some(ref page_url) => match caption.map(str::trim).filter(|s| !s.is_empty()) {
            Some(cap) => format!("📰 **{title}**\n{cap}\n{page_url}"),
            None => format!("📰 [**{title}**]({page_url})"),
        },
        None => {
            let body = content.unwrap_or_default().trim();
            format!("📰 **{title}**\n\n{body}")
        }
    };
    post_text(store, data_dir, from, chat_id, &text, reply_to, thread)
}

fn publish_telegraph_article(
    data_dir: &std::path::Path,
    title: &str,
    markdown: &str,
    author_name: Option<&str>,
) -> Option<String> {
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
        || std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some()
    {
        return None;
    }
    let token_path = data_dir.join("secrets").join("telegraph-token");
    let access_token = std::fs::read_to_string(&token_path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            let agent = ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(10)))
                .build()
                .new_agent();
            let resp = agent
                .get("https://api.telegra.ph/createAccount?short_name=AnyAgents&author_name=AnyAgents")
                .call()
                .ok()?;
            let body = resp.into_body().read_to_string().ok()?;
            let val: serde_json::Value = serde_json::from_str(&body).ok()?;
            let tok = val
                .pointer("/result/access_token")
                .and_then(serde_json::Value::as_str)?
                .to_string();
            if let Err(err) =
                anyagents_harness::atomic_write_nonempty_file(&token_path, tok.as_bytes(), 0o600)
            {
                eprintln!("cannot persist {}: {err}", token_path.display());
            }
            Some(tok)
        })?;
    let nodes: Vec<serde_json::Value> = markdown
        .split("\n\n")
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|para| {
            if let Some(h) = para
                .strip_prefix("### ")
                .or_else(|| para.strip_prefix("#### "))
            {
                serde_json::json!({ "tag": "h4", "children": [h.trim()] })
            } else if let Some(h) = para.strip_prefix("# ").or_else(|| para.strip_prefix("## ")) {
                serde_json::json!({ "tag": "h3", "children": [h.trim()] })
            } else if let Some(q) = para.strip_prefix("> ") {
                serde_json::json!({ "tag": "blockquote", "children": [q.trim()] })
            } else if para.starts_with("```") {
                let code = para
                    .trim_start_matches("```")
                    .trim_end_matches("```")
                    .trim();
                serde_json::json!({ "tag": "pre", "children": [code] })
            } else {
                serde_json::json!({ "tag": "p", "children": [para] })
            }
        })
        .collect();
    let payload = serde_json::json!({
        "access_token": access_token,
        "title": title,
        "author_name": author_name.unwrap_or("AnyAgents"),
        "content": if nodes.is_empty() {
            vec![serde_json::json!({ "tag": "p", "children": [markdown] })]
        } else {
            nodes
        },
        "return_content": false
    });
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .new_agent();
    let resp = agent
        .post("https://api.telegra.ph/createPage")
        .header("content-type", "application/json")
        .send(payload.to_string().as_bytes())
        .ok()?;
    let body = resp.into_body().read_to_string().ok()?;
    let val: serde_json::Value = serde_json::from_str(&body).ok()?;
    val.pointer("/result/url")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// Set or clear an emoji reaction on a Telegram message through the agent's bot,
/// and fan out the reaction notice to peer agents in the room.
pub fn react_message(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    message_id: i64,
    emoji: Option<&str>,
    thread: Option<i64>,
) -> Result<(), anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
    let call = set_message_reaction(chat_id, message_id, emoji, None);
    call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id))?;
    if let Some(e) = emoji.map(str::trim).filter(|s| !s.is_empty()) {
        let summary = format!("[Reaction {e} on message #{message_id}]");
        let room = format!("telegram:chat:{chat_id}");
        let reply_target = match thread {
            Some(t) => format!("{room}:{t}"),
            None => room.clone(),
        };
        if chat_is_room(store, chat_id)? {
            let incoming = anyagents_core::Incoming {
                text: summary,
                sender: Some(format!("agent:{from}")),
                source_time_ms: None,
                source_key: Some(format!(
                    "tg-react:{chat_id}:{message_id}:{from}:{e}:{}",
                    anyagents_core::now_ms()
                )),
                reply_to: Some(reply_target),
                quote: None,
                origin: Some(anyagents_core::Origin {
                    chat_id: Some(chat_id),
                    thread_id: thread,
                    message_id: Some(message_id),
                }),
                received_at_ms: None,
            };
            store.receive_room(&room, incoming)?;
        }
    }
    Ok(())
}

/// Edit the text of a Telegram message sent by this agent's bot.
pub fn edit_message(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    message_id: i64,
    text: &str,
    thread: Option<i64>,
) -> Result<(), anyagents_core::Error> {
    let translated = translate_agent_mentions_for_telegram(store, text);
    let text = translated.as_str();
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
    let call = edit_message_text(chat_id, message_id, text);
    if let Err(err) = call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id)) {
        if err.to_string().contains("\"class\":\"invalid_entities\"") {
            let plain = BotCall {
                method: "editMessageText".to_string(),
                body: serde_json::json!({
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": text,
                }),
            };
            call_api(&token, &plain, SHORT_TIMEOUT_S, Some(&channel.id))?;
        } else {
            return Err(err);
        }
    }
    if chat_is_room(store, chat_id)? {
        let room = format!("telegram:chat:{chat_id}");
        let reply_target = match thread {
            Some(t) => format!("{room}:{t}"),
            None => room.clone(),
        };
        let incoming = anyagents_core::Incoming {
            text: format!("[Edited message #{message_id}]: {text}"),
            sender: Some(format!("agent:{from}")),
            source_time_ms: None,
            source_key: Some(format!(
                "tg-edit:{chat_id}:{from}:{message_id}:{}",
                anyagents_core::now_ms()
            )),
            reply_to: Some(reply_target),
            quote: None,
            origin: Some(anyagents_core::Origin {
                chat_id: Some(chat_id),
                thread_id: thread,
                message_id: Some(message_id),
            }),
            received_at_ms: None,
        };
        store.receive_room(&room, incoming)?;
    }
    Ok(())
}

/// Delete a Telegram message in `chat_id`.
pub fn delete_chat_message(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    message_id: i64,
) -> Result<(), anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
    let call = delete_message(chat_id, message_id);
    call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id))?;
    if chat_is_room(store, chat_id)? {
        let room = format!("telegram:chat:{chat_id}");
        let incoming = anyagents_core::Incoming {
            text: format!("[Deleted message #{message_id}]"),
            sender: Some(format!("agent:{from}")),
            source_time_ms: None,
            source_key: Some(format!(
                "tg-del:{chat_id}:{from}:{message_id}:{}",
                anyagents_core::now_ms()
            )),
            reply_to: Some(room.clone()),
            quote: None,
            origin: Some(anyagents_core::Origin {
                chat_id: Some(chat_id),
                thread_id: None,
                message_id: Some(message_id),
            }),
            received_at_ms: None,
        };
        store.receive_room(&room, incoming)?;
    }
    Ok(())
}

/// Pin or unpin a Telegram message in `chat_id`.
pub fn pin_message(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    message_id: i64,
    pin: bool,
) -> Result<(), anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
    let call = if pin {
        pin_chat_message(chat_id, message_id, Some(true))
    } else {
        unpin_chat_message(chat_id, message_id)
    };
    call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id))?;
    Ok(())
}

/// Close an active Telegram poll message.
pub fn close_poll(
    store: &Store,
    data_dir: &std::path::Path,
    from: &str,
    chat_id: i64,
    message_id: i64,
) -> Result<(), anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    let token = token_of(&FileCredentialStore::new(data_dir), &credential_ref)?;
    let call = stop_poll(chat_id, message_id);
    call_api(&token, &call, SHORT_TIMEOUT_S, Some(&channel.id))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn call_api_multipart(
    token: &str,
    channel_id: &anyagents_core::ChannelId,
    chat_id: i64,
    kind: &str,
    file_path: &std::path::Path,
    caption: Option<&str>,
    title: Option<&str>,
    performer: Option<&str>,
    reply_to: Option<i64>,
    thread: Option<i64>,
) -> Result<String, anyagents_core::Error> {
    let (method, field) = match kind {
        "photo" | "image" => ("sendPhoto", "photo"),
        "video" => ("sendVideo", "video"),
        "audio" | "music" => ("sendAudio", "audio"),
        "voice" => ("sendVoice", "voice"),
        "animation" | "gif" => ("sendAnimation", "animation"),
        _ => ("sendDocument", "document"),
    };
    let file_bytes = std::fs::read(file_path)?;
    let file_name = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("attachment.bin")
        .replace('"', "_");
    let boundary = format!("----anyagents-tg-{}", anyagents_core::MessageId::generate());
    let mut payload: Vec<u8> = Vec::with_capacity(file_bytes.len() + 1024);
    let mut add_field = |name: &str, val: &str| {
        payload.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{val}\r\n"
            )
            .as_bytes(),
        );
    };
    add_field("chat_id", &chat_id.to_string());
    if let Some(cap) = caption.filter(|c| !c.trim().is_empty()) {
        let html = anyagents_channel_telegram::markdown_to_telegram_html(cap);
        add_field("caption", &html);
        add_field("parse_mode", "HTML");
    }
    if field == "audio" {
        if let Some(t) = title.map(str::trim).filter(|s| !s.is_empty()) {
            add_field("title", t);
        }
        if let Some(p) = performer.map(str::trim).filter(|s| !s.is_empty()) {
            add_field("performer", p);
        }
    }
    if let Some(id) = reply_to {
        add_field("reply_to_message_id", &id.to_string());
    }
    if let Some(id) = thread {
        add_field("message_thread_id", &id.to_string());
    }
    payload.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{field}\"; filename=\"{file_name}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    payload.extend_from_slice(&file_bytes);
    payload.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let url = format!("{}/bot{token}/{method}", api_base());
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(SHORT_TIMEOUT_S)))
        .http_status_as_error(false)
        .max_redirects(0)
        .max_redirects_will_error(false)
        .build()
        .new_agent();
    let content_type = format!("multipart/form-data; boundary={boundary}");
    let response = agent
        .post(&url)
        .header("content-type", &content_type)
        .send(&payload[..])
        .map_err(|_| {
            api_error(
                method,
                Some(channel_id),
                "transport",
                None,
                None,
                "Telegram multipart upload failed; delivery may be uncertain",
                None,
            )
        })?;
    let status = response.status().as_u16();
    let body = response.into_body().read_to_string().map_err(|_| {
        api_error(
            method,
            Some(channel_id),
            "response_read",
            Some(status),
            None,
            "Telegram response could not be read; delivery may be uncertain",
            None,
        )
    })?;
    let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| {
        api_error(
            method,
            Some(channel_id),
            "invalid_response",
            Some(status),
            None,
            "Telegram response is not JSON; delivery may be uncertain",
            None,
        )
    })?;
    if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        return Ok(body);
    }
    let code = value.get("error_code").and_then(serde_json::Value::as_i64);
    let description = value.get("description").and_then(serde_json::Value::as_str);
    let (class, safe_description) = provider_description(description);
    Err(api_error(
        method,
        Some(channel_id),
        class,
        Some(status),
        code,
        safe_description,
        None,
    ))
}

/// Ask Telegram whether this bot is a member of a chat.
/// Returns `Some(true)` if present, `Some(false)` if explicitly left/kicked/not found,
/// and `None` on transient network or rate-limit errors.
pub fn chat_member_status(token: &str, chat_id: i64, user_id: i64) -> Option<bool> {
    let is_mock_test = std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some();
    let call = BotCall {
        method: "getChatMember".to_string(),
        body: serde_json::json!({"chat_id": chat_id, "user_id": user_id}),
    };
    let body = match call_api(token, &call, SHORT_TIMEOUT_S, None) {
        Ok(b) => b,
        Err(err) => {
            if is_mock_test {
                return Some(false);
            }
            let msg = err.to_string().to_ascii_lowercase();
            if msg.contains("chat not found")
                || msg.contains("bot was kicked")
                || msg.contains("bot is not a member")
                || msg.contains("user not found")
                || msg.contains("participant_id_invalid")
            {
                return Some(false);
            }
            return None;
        }
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) else {
        return if is_mock_test { Some(false) } else { None };
    };
    match value
        .pointer("/result/status")
        .and_then(serde_json::Value::as_str)
    {
        Some("member" | "administrator" | "creator") => Some(true),
        Some("restricted") => Some(
            value
                .pointer("/result/is_member")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true),
        ),
        Some("left" | "kicked") => Some(false),
        _ => {
            if is_mock_test {
                Some(false)
            } else {
                None
            }
        }
    }
}

#[allow(dead_code)]
pub fn chat_member_is_in(token: &str, chat_id: i64, user_id: i64) -> bool {
    chat_member_status(token, chat_id, user_id).unwrap_or(false)
}

/// The local half of one post: remember the chat, and copy the message into the
/// room when the chat is a group. Nothing here posts; replaying the same
/// receipt is one row. Legacy callers have no topic/quote metadata to supply.
#[allow(dead_code)] // Preserve the legacy receipt-replay entry point.
pub fn deliver_locally(store: &Store, from: &str, chat_id: i64, message_id: i64, text: &str) {
    if let Err(err) =
        deliver_locally_with_metadata(store, from, chat_id, message_id, text, None, None)
    {
        eprintln!("{err}");
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LocalReceipt {
    from: String,
    chat_id: i64,
    message_id: i64,
    text: String,
    thread: Option<i64>,
    quote: Option<anyagents_core::Quote>,
    #[serde(default)]
    recipients: Option<Vec<anyagents_core::AgentId>>,
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
) -> Result<(), anyagents_core::Error> {
    // Telegram has already accepted this receipt. Persist retry custody before
    // attempting the local projection. Replay never invokes a provider send.
    let receipt = LocalReceipt {
        from: from.into(),
        chat_id,
        message_id,
        text: text.into(),
        thread,
        quote: quote.cloned(),
        recipients: None,
    };
    let path = retain_local_receipt(store.data_dir(), &receipt).map_err(|_| anyagents_core::Error::Message(
        format!("Telegram accepted message {message_id} in chat {chat_id}, but local receipt custody failed; do not repost")))?;
    match project_local_receipt(store, &receipt) {
        Ok(()) => {
            if let Err(err) = remove_local_receipt(&path) {
                eprintln!("Telegram message {message_id} local receipt cleanup pending: {err}");
            }
        }
        Err(err) => {
            eprintln!(
                "Telegram message {message_id} local relay pending (durable receipt retained): {err}"
            );
            // The poller retries a pending receipt until it is projected.
            changed();
        }
    }
    Ok(())
}

fn retain_local_receipt(
    data_dir: &std::path::Path,
    receipt: &LocalReceipt,
) -> Result<std::path::PathBuf, anyagents_core::Error> {
    use sha2::{Digest, Sha256};
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let dir = data_dir.join("telegram-relay-pending");
    std::fs::create_dir_all(&dir)?;
    let key = format!(
        "{}:{}:{}",
        receipt.from, receipt.chat_id, receipt.message_id
    );
    let path = dir.join(format!("{:x}.json", Sha256::digest(key.as_bytes())));
    // The final name is published only after complete bytes are synced. The
    // journal remains independent of the potentially unavailable state store.
    let staging = dir.join(format!(
        ".{}-{}.tmp",
        std::process::id(),
        anyagents_core::MessageId::generate()
    ));
    let result = (|| -> Result<(), anyagents_core::Error> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&staging)?;
        file.write_all(
            &serde_json::to_vec(receipt)
                .map_err(|err| anyagents_core::Error::Message(err.to_string()))?,
        )?;
        file.sync_all()?;
        std::fs::rename(&staging, &path)?;
        std::fs::File::open(&dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&staging);
    }
    result?;
    Ok(path)
}

fn remove_local_receipt(path: &std::path::Path) -> Result<(), anyagents_core::Error> {
    match std::fs::remove_file(path) {
        Ok(()) => {
            std::fs::File::open(path.parent().expect("journal has a directory"))?.sync_all()?
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn project_local_receipt(
    store: &Store,
    receipt: &LocalReceipt,
) -> Result<(), anyagents_core::Error> {
    record_chat(store, &receipt.from, receipt.chat_id)?;
    let mut receipt = LocalReceipt {
        from: receipt.from.clone(),
        chat_id: receipt.chat_id,
        message_id: receipt.message_id,
        text: receipt.text.clone(),
        thread: receipt.thread,
        quote: receipt.quote.clone(),
        recipients: receipt.recipients.clone(),
    };
    if receipt.recipients.is_none() {
        let mut recipients = Vec::new();
        if chat_is_room(store, receipt.chat_id)? {
            let author = resolve_telegram_agent(store, &receipt.from)?;
            for member in telegram_chat_agents(store, receipt.chat_id)? {
                if member.id != author.id && member.state != "paused" && member.state != "archived"
                {
                    recipients.push(member.id);
                }
            }
        }
        receipt.recipients = Some(recipients);
        // Pin failed recipients before the first mailbox write; later removal
        // from a chat cannot erase an already accepted relay obligation.
        retain_local_receipt(store.data_dir(), &receipt)?;
    }
    let room = format!("telegram:chat:{}", receipt.chat_id);
    let mut incoming = anyagents_core::Incoming {
        text: receipt.text.clone(),
        sender: Some(format!("agent:{}", receipt.from)),
        source_time_ms: None,
        source_key: Some(format!(
            "room:{room}:tg-out:{}:{}:{}",
            receipt.chat_id, receipt.from, receipt.message_id
        )),
        reply_to: Some(
            receipt
                .thread
                .map(|thread| format!("{room}:{thread}"))
                .unwrap_or(room),
        ),
        quote: receipt.quote.clone(),
        origin: Some(anyagents_core::Origin {
            chat_id: Some(receipt.chat_id),
            thread_id: receipt.thread,
            message_id: Some(receipt.message_id),
        }),
        received_at_ms: None,
    };
    incoming.text = incoming.body();
    incoming.quote = None;
    for id in receipt
        .recipients
        .as_ref()
        .expect("recipients pinned before delivery")
    {
        store.receive(id, incoming.clone())?;
    }
    Ok(())
}

fn telegram_chat_agents(
    store: &Store,
    chat_id: i64,
) -> Result<Vec<anyagents_core::Agent>, anyagents_core::Error> {
    let mut agents = Vec::new();
    for ch in store.list_channels()? {
        if ch.kind != "telegram" {
            continue;
        }
        if Memory::try_from_state(&ch.state)?
            .chats
            .iter()
            .any(|link| link.chat_id == chat_id)
        {
            for id in &ch.agents {
                let agent = store.agent(id)?;
                if !agents
                    .iter()
                    .any(|existing: &anyagents_core::Agent| existing.id == agent.id)
                {
                    agents.push(agent);
                }
            }
        }
    }
    Ok(agents)
}

/// Retry only the local half of acknowledged provider sends, including after a
/// server restart. Unknown/corrupt journal bytes are preserved and reported.
pub(super) fn replay_local_receipts(store: &Store) -> Result<usize, anyagents_core::Error> {
    let dir = store.data_dir().join("telegram-relay-pending");
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err.into()),
    };
    let mut completed = 0;
    let mut failure = None;
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let result = (|| -> Result<(), anyagents_core::Error> {
            let bytes = std::fs::read(&path)?;
            let receipt: LocalReceipt = serde_json::from_slice(&bytes).map_err(|_| {
                anyagents_core::Error::Message("corrupt Telegram local receipt; preserved".into())
            })?;
            project_local_receipt(store, &receipt)?;
            remove_local_receipt(&path)
        })();
        match result {
            Ok(()) => completed += 1,
            Err(err) => failure = Some(err),
        }
    }
    if let Some(err) = failure {
        return Err(err);
    }
    Ok(completed)
}

/// Whether any bot remembers this chat as a group or supergroup.
pub fn chat_is_room(store: &Store, chat_id: i64) -> Result<bool, anyagents_core::Error> {
    for channel in store.list_channels()? {
        if channel.kind == "telegram" && Memory::try_from_state(&channel.state)?.is_room(chat_id) {
            return Ok(true);
        }
    }
    Ok(false)
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
        source_key: Some(format!("tg-out:{chat_id}:{from}:{message_id}")),
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
    let default_kind = if chat_id < 0 {
        if chat_id.to_string().starts_with("-100") {
            Some("supergroup")
        } else {
            Some("group")
        }
    } else {
        None
    };
    let agent_row = resolve_telegram_agent(store, agent)?;
    for channel in store.list_channels()? {
        if channel.kind != "telegram" || !channel.agents.contains(&agent_row.id) {
            continue;
        }
        let mut memory = Memory::try_from_state(&channel.state)?;
        let before = memory.to_state();
        let existing_kind = memory
            .chats
            .iter()
            .find(|l| l.chat_id == chat_id && l.chat_type.is_some())
            .and_then(|l| l.chat_type.clone());
        memory.attach_typed(
            agent_row.id.as_str(),
            chat_id,
            existing_kind.as_deref().or(default_kind),
        );
        let after = memory.to_state();
        if after != before {
            save_channel_memory_merged(store, &channel.id, &mut memory, &[])?;
        }
        return Ok(());
    }
    Ok(())
}

/// The one bot an agent posts through. An agent that has none, or more than
/// one, posts nothing: there is no default bot to fall back to.
fn resolve_telegram_agent(
    store: &Store,
    reference: &str,
) -> Result<anyagents_core::Agent, anyagents_core::Error> {
    match anyagents_core::AgentId::parse(reference) {
        Ok(id) => store.agent(&id),
        Err(_) => store.agent_by_name(reference),
    }
}

fn channel_for(store: &Store, from: &str) -> Result<Channel, anyagents_core::Error> {
    let agent = resolve_telegram_agent(store, from)?;
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
        .map(|s| s.trim().to_string())
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
    let payload = serde_json::json!([{"update_id": 1, "message": result}]).to_string();
    let mut poll = Poll::resume(0);
    poll.route("sent", "", &payload)
        .ok()
        .and_then(|items| {
            items.into_iter().find_map(|item| match item {
                Routed::ToAgent { inbound, .. } => Some((inbound.thread_id, inbound.quote)),
                _ => None,
            })
        })
        .unwrap_or((None, None))
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
        "sendMessage"
        | "sendRichMessage"
        | "editMessageText"
        | "sendPhoto"
        | "sendVideo"
        | "sendAudio"
        | "sendVoice"
        | "sendDocument"
        | "sendAnimation"
        | "sendLocation"
        | "sendVenue"
        | "sendPoll"
        | "getFile"
        | "getUpdates"
        | "getMe"
        | "getChat"
        | "getChatMember"
        | "setWebhook"
        | "deleteWebhook"
        | "setMyName"
        | "setMyShortDescription"
        | "setMyDescription"
        | "getManagedBotToken"
        | "replaceManagedBotToken" => method,
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
            | "bad request: chat_write_forbidden"
            | "bad request: have no rights to send a message"
            | "forbidden: not enough rights to send text messages to the chat",
        ) => (
            "send_forbidden",
            "Bad Request: not enough rights to send text messages to the chat",
        ),
        Some("bad request: message is too long") => {
            ("message_too_long", "Bad Request: message is too long")
        }
        Some("bad request: message text is empty" | "bad request: text must be non-empty") => {
            ("empty_message", "Bad Request: message text is empty")
        }
        Some(s) if s.starts_with("bad request: can't parse entities") => {
            ("invalid_entities", "Bad Request: can't parse entities")
        }
        Some("bad request: group chat was upgraded to a supergroup chat") => (
            "chat_migrated",
            "Bad Request: group chat was upgraded to a supergroup chat",
        ),
        Some("bad request: peer_id_invalid" | "bad request: user not found") => {
            ("chat_not_found", "Bad Request: peer_id_invalid")
        }
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
    if (call.method == "sendMessage" || call.method == "sendRichMessage")
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
pub(crate) mod tests {
    use super::*;

    // The integration harness includes this module, so its transport fixtures
    // must share this lock with these tests rather than defining another one.
    pub(crate) fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());
        ENV.lock().unwrap_or_else(|err| err.into_inner())
    }

    #[test]
    fn telegram_bot_display_name_is_the_title_cased_name_without_role() {
        // An agent `helper-bot` with role `Documentation` is named `Helper Bot`;
        // the sync skips `setMyName` when this computed name already matches, so a
        // role-only edit never renames the bot.
        assert_eq!(format_telegram_bot_display_name("helper-bot"), "Helper Bot");
        assert_eq!(
            format_telegram_bot_display_name("ops_desk--x"),
            "Ops Desk X"
        );
        assert!(!format_telegram_bot_display_name("helper-bot").contains('·'));
    }

    #[test]
    fn updates_payload_keeps_only_the_result_array() {
        let _env = env_lock();
        let payload = updates_payload(r#"{"ok":true,"result":[{"update_id":1}]}"#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed[0]["update_id"], 1);
        assert!(updates_payload(r#"{"ok":true,"result":{"message_id":9}}"#).is_err());
    }

    #[test]
    fn a_channel_without_a_secret_is_not_polled() {
        let _env = env_lock();
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
        let _env = env_lock();
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
        assert_eq!(sent.call.method, "sendRichMessage");
        assert_eq!(sent.call.body["chat_id"], 100);
        assert_eq!(sent.call.body["rich_message"]["markdown"], "hello");
        assert_eq!(sent.call.body["reply_to_message_id"], 5);
        assert_eq!(sent.call.body["reply_parameters"]["message_id"], 5);
        assert_eq!(sent.call.body["message_thread_id"], 3);
        assert!(!sent.call.body.to_string().contains("token"));

        // Nobody else's bot is ever used, and an unknown agent has none.
        assert!(outbound_call(&store, "cy", 100, "hello", None, None).is_err());
    }

    #[test]
    fn a_bot_belongs_to_one_agent() {
        let _env = env_lock();
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
        assert_eq!(sent.call.method, "sendRichMessage");
        // There is no default bot: another agent does not post through it.
        let bea = store
            .create_agent(anyagents_core::NewAgent::new("bea", "codex"))
            .unwrap();
        assert!(outbound_call(&store, "bea", 100, "hi", None, None).is_err());
        assert!(bea.name == "bea");
    }

    #[test]
    fn no_telegram_channel_is_refused() {
        let _env = env_lock();
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
        let _env = env_lock();
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
        let memory = Memory::try_from_state(&channel.state).unwrap();
        assert_eq!(memory.chats.len(), 1);
        assert_eq!(memory.chats[0].chat_id, -1007);
        assert_eq!(
            attached_agents(&store).unwrap(),
            vec![("ada".to_string(), -1007)]
        );
    }

    #[test]
    fn webhook_update_verifies_secret_and_routes_into_mailbox() {
        let _env = env_lock();
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
                "from": {"id": 42, "is_bot": false, "first_name": "Alex"},
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

    #[test]
    fn token_of_trims_trailing_newline() {
        let _env = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let secrets = FileCredentialStore::new(dir.path());
        secrets
            .write("channels/coordinator", b"8921438889:AAExampleTokenValue\n")
            .unwrap();
        assert_eq!(
            token_of(&secrets, "channels/coordinator").unwrap(),
            "8921438889:AAExampleTokenValue"
        );
    }
    fn custody_fixture() -> (tempfile::TempDir, Store, anyagents_core::Agent, Channel) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("helper", "codex"))
            .unwrap();
        let channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: Some("helper_bot".into()),
                config: "{}".into(),
                credential_ref: Some("channels/helper".into()),
            })
            .unwrap();
        store.attach_channel(&channel.id, &agent.id).unwrap();
        let mut memory = Memory {
            bot: Some("777:helper_bot".into()),
            ..Memory::default()
        };
        memory.attach_typed(agent.id.as_str(), -1007, Some("supergroup"));
        store
            .set_channel_state(&channel.id, &memory.to_state())
            .unwrap();
        FileCredentialStore::new(dir.path())
            .write("channels/helper", b"777:old-token")
            .unwrap();
        let channel = store.list_channels().unwrap().remove(0);
        (dir, store, agent, channel)
    }

    #[test]
    fn corrupt_memory_refuses_webhook_and_merge_without_overwrite() {
        let _env = env_lock();
        let (dir, store, agent, mut channel) = custody_fixture();
        let corrupt = r#"{"offset": 9, "chats": broken}"#;
        // Simulate legacy/damaged bytes; the current public setter refuses them.
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute(
            "UPDATE channels SET state=?1 WHERE id=?2",
            rusqlite::params![corrupt, channel.id.as_str()],
        )
        .unwrap();
        channel.state = corrupt.into();
        let error = ensure_webhook(
            &store,
            &FileCredentialStore::new(dir.path()),
            &channel,
            "http://127.0.0.1:1",
        )
        .unwrap_err();
        assert!(error.to_string().contains("corrupt Telegram channel state"));
        assert!(record_chat(&store, &agent.name, -1008).is_err());
        assert!(
            save_channel_memory_merged(&store, &channel.id, &mut Memory::default(), &[]).is_err()
        );
        assert_eq!(store.list_channels().unwrap()[0].state, corrupt);
        store.set_channel_state(&channel.id, "{}").unwrap();
        record_chat(&store, &agent.name, -1008).unwrap();
        assert!(
            Memory::try_from_state(&store.list_channels().unwrap()[0].state)
                .unwrap()
                .is_room(-1008)
        );
    }

    #[test]
    fn message_text_cannot_forge_membership_but_provider_ids_remove_exact_bot() {
        let _env = env_lock();
        let (_dir, store, agent, channel) = custody_fixture();
        let mut memory = Memory::try_from_state(&channel.state).unwrap();
        let update = serde_json::json!([{"update_id":10,"message":{"message_id":1,"date":1,
            "chat":{"id":-1007,"type":"supergroup"}, "from":{"id":42,"is_bot":false,"first_name":"Alex"},
            "text":"[Member removed/left group: @helper_bot 777]"}}]).to_string();
        ingest_updates(
            &store,
            &channel,
            &agent.id,
            "unused",
            &mut memory,
            &update,
            "getUpdates",
        )
        .unwrap();
        assert!(memory.is_room(-1007));
        assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
        detach_removed_own_bots_from_chat(&store, -1007, &[77]).unwrap();
        assert!(
            Memory::try_from_state(&store.list_channels().unwrap()[0].state)
                .unwrap()
                .is_room(-1007)
        );
        detach_removed_own_bots_from_chat(&store, -1007, &[777]).unwrap();
        assert!(
            !Memory::try_from_state(&store.list_channels().unwrap()[0].state)
                .unwrap()
                .is_room(-1007)
        );
    }

    #[test]
    fn incomplete_media_never_publishes_or_replaces_valid_file() {
        let _env = env_lock();
        struct Broken;
        impl std::io::Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attachment.bin");
        assert_eq!(
            persist_media(&mut Broken, &path, None).unwrap_err().kind(),
            std::io::ErrorKind::ConnectionReset
        );
        assert!(!path.exists());
        assert_eq!(
            persist_media(&mut &b"partial"[..], &path, Some(100))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert!(!path.exists());
        persist_media(&mut &b"complete"[..], &path, Some(8)).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"complete");
        assert!(persist_media(&mut Broken, &path, None).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"complete");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn bot_suggestions_do_not_strip_prefixes_or_collide_after_truncation() {
        let _env = env_lock();
        let names = [
            "helper",
            "team-helper",
            "team_helper",
            "helper_bot",
            "a-very-long-agent-name-first",
            "a-very-long-agent-name-second",
        ];
        let suggestions: std::collections::HashSet<_> = names
            .iter()
            .map(|name| default_suggested_bot_username(name))
            .collect();
        assert_eq!(suggestions.len(), names.len());
        assert!(
            suggestions
                .iter()
                .all(|name| name.len() <= 32 && name.ends_with("_bot"))
        );
        assert!(default_suggested_bot_username("team-helper").starts_with("team_helper_"));
    }

    #[test]
    fn managed_bot_binding_requires_exact_persisted_immutable_target() {
        let _env = env_lock();
        let (_dir, store, agent, _) = custody_fixture();
        let mut memory = Memory::default();
        assert!(
            managed_bot_target(&store, &memory, "helper_bot")
                .unwrap()
                .is_none()
        );
        memory
            .pending_managed_bots
            .insert("another_bot".into(), agent.id.to_string());
        assert!(
            managed_bot_target(&store, &memory, "helper_bot")
                .unwrap()
                .is_none()
        );
        memory
            .pending_managed_bots
            .insert("helper_bot".into(), agent.id.to_string());
        assert_eq!(
            managed_bot_target(&store, &memory, "helper_bot")
                .unwrap()
                .unwrap()
                .id,
            agent.id
        );
        memory
            .pending_managed_bots
            .insert("helper_bot".into(), agent.name);
        assert!(managed_bot_target(&store, &memory, "helper_bot").is_err());
    }

    #[test]
    fn manager_fallback_order_uses_relationship_then_channel_order() {
        let _env = env_lock();
        let (_dir, store, agent, channel) = custody_fixture();
        let other = store
            .create_agent(anyagents_core::NewAgent::new("coordinator", "codex"))
            .unwrap();
        let extra = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/other".into()),
            })
            .unwrap();
        store.attach_channel(&extra.id, &other.id).unwrap();
        let channels = vec![extra.clone(), channel.clone()];
        let ordered = candidate_manager_channels(&store, &channels, None, &agent, None);
        assert_eq!(
            ordered.iter().map(|c| &c.id).collect::<Vec<_>>(),
            vec![&extra.id, &channel.id]
        );
        let ordered = candidate_manager_channels(&store, &channels, Some(&agent), &agent, None);
        assert_eq!(ordered[0].id, channel.id);
    }

    #[test]
    fn inbound_peer_failure_preserves_update_offset_until_retry() {
        let _env = env_lock();
        let (dir, store, agent, channel) = custody_fixture();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("peer", "codex"))
            .unwrap();
        let extra = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        store.attach_channel(&extra.id, &peer.id).unwrap();
        let mut peer_mem = Memory::default();
        peer_mem.attach_typed(peer.id.as_str(), -1007, Some("supergroup"));
        store
            .set_channel_state(&extra.id, &peer_mem.to_state())
            .unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute_batch(&format!("CREATE TRIGGER reject_peer BEFORE INSERT ON messages WHEN NEW.agent_id='{}' BEGIN SELECT RAISE(ABORT, 'peer custody failure'); END;", peer.id)).unwrap();
        let update = serde_json::json!([{"update_id":10,"message":{"message_id":1,"date":1,"chat":{"id":-1007,"type":"supergroup"},"text":"[payload]"}}]).to_string();
        let mut memory = Memory::try_from_state(&channel.state).unwrap();
        let error = ingest_updates(
            &store,
            &channel,
            &agent.id,
            "unused",
            &mut memory,
            &update,
            "getUpdates",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("peer custody failure"),
            "{error}"
        );
        let fresh = store
            .list_channels()
            .unwrap()
            .into_iter()
            .find(|ch| ch.id == channel.id)
            .unwrap();
        assert_eq!(Memory::try_from_state(&fresh.state).unwrap().offset, 0);
        conn.execute_batch("DROP TRIGGER reject_peer").unwrap();
        ingest_updates(
            &store,
            &channel,
            &agent.id,
            "unused",
            &mut memory,
            &update,
            "getUpdates",
        )
        .unwrap();
        assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
        assert_eq!(store.inbox(&peer.id, None).unwrap().len(), 1);
        assert_eq!(memory.offset, 11);
    }

    #[test]
    fn outbound_receipt_survives_failed_local_projection_and_replays_once() {
        let _env = env_lock();
        let (dir, store, agent, channel) = custody_fixture();
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute(
            "UPDATE channels SET state='damaged' WHERE id=?1",
            [channel.id.as_str()],
        )
        .unwrap();
        deliver_locally_with_metadata(&store, &agent.name, -1007, 91, "sent once", Some(2), None)
            .unwrap();
        let journal = dir.path().join("telegram-relay-pending");
        assert_eq!(std::fs::read_dir(&journal).unwrap().count(), 1);
        assert!(
            replay_local_receipts(&store)
                .unwrap_err()
                .to_string()
                .contains("corrupt Telegram channel state")
        );
        store
            .set_channel_state(&channel.id, &channel.state)
            .unwrap();
        assert_eq!(replay_local_receipts(&store).unwrap(), 1);
        assert_eq!(replay_local_receipts(&store).unwrap(), 0);
        assert_eq!(std::fs::read_dir(&journal).unwrap().count(), 0);
    }

    #[test]
    fn corrupt_relay_journal_is_preserved_not_treated_as_complete() {
        let _env = env_lock();
        let (dir, store, _, _) = custody_fixture();
        let journal = dir.path().join("telegram-relay-pending");
        std::fs::create_dir_all(&journal).unwrap();
        let path = journal.join("corrupt.json");
        std::fs::write(&path, b"incomplete").unwrap();
        assert!(
            replay_local_receipts(&store)
                .unwrap_err()
                .to_string()
                .contains("corrupt Telegram local receipt")
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"incomplete");
    }
    struct EnvRestore(&'static str, Option<std::ffi::OsString>);
    impl EnvRestore {
        fn set(key: &'static str, value: &str) -> Self {
            let old = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self(key, old)
        }
    }
    impl Drop for EnvRestore {
        fn drop(&mut self) {
            unsafe {
                if let Some(ref value) = self.1 {
                    std::env::set_var(self.0, value)
                } else {
                    std::env::remove_var(self.0)
                }
            }
        }
    }
    fn provider_response(status: u16, body: &str) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let body = body.to_string();
        let task = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                let mut b = [0];
                socket.read_exact(&mut b).unwrap();
                bytes.push(b[0]);
            }
            let head = String::from_utf8(bytes).unwrap();
            let len: usize = head
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap_or(0);
            let mut request = vec![0; len];
            socket.read_exact(&mut request).unwrap();
            write!(socket,"HTTP/1.1 {status} Synthetic\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
        });
        (base, task)
    }

    #[test]
    fn provider_identity_failure_cannot_replace_existing_binding() {
        let _env = env_lock();
        let _fake = EnvRestore::set("ANYAGENTS_FAKE_MODE", "1");
        for (status, body) in [
            (401, r#"{"ok":false,"error_code":401}"#),
            (200, r#"{"ok":true,"result":{}}"#),
        ] {
            let (dir, store, agent, channel) = custody_fixture();
            let (base, task) = provider_response(status, body);
            let _api = EnvRestore::set("ANYAGENTS_TELEGRAM_API", &base);
            assert!(
                bind_agent_telegram_token(&store, dir.path(), &agent, "999:new-token", None)
                    .is_err()
            );
            task.join().unwrap();
            let fresh = store.list_channels().unwrap().remove(0);
            assert_eq!(fresh, channel);
            assert_eq!(
                token_of(&FileCredentialStore::new(dir.path()), "channels/helper").unwrap(),
                "777:old-token"
            );
        }
    }

    #[test]
    fn token_rotation_preserves_channel_id_credential() {
        let _env = env_lock();
        let _fake = EnvRestore::set("ANYAGENTS_FAKE_MODE", "1");
        let (dir, store, agent, original) = custody_fixture();
        let secrets = FileCredentialStore::new(dir.path());
        // This is the exact reference produced by the first successful bind.
        let legacy_ref = format!("channels/{}", original.id);
        secrets.write(&legacy_ref, b"777:old-token").unwrap();
        store.set_credential_ref(&original.id, &legacy_ref).unwrap();
        let mut retained = vec![(legacy_ref, "777:old-token".to_string())];
        for (id, token) in [(999, "999:new-token"), (1000, "1000:next-token")] {
            let result =
                bind_agent_telegram_token(&store, dir.path(), &agent, token, Some((id, "new_bot")))
                    .expect("rotation must preserve a channel-ID credential file");
            assert_eq!(result["status"], "bound");
            let reopened = Store::open(dir.path().join("state.db")).unwrap();
            let current = channel_for(&reopened, &agent.name).unwrap();
            assert_eq!(current.id, original.id);
            assert_eq!(reopened.list_channels().unwrap().len(), 1);
            let current_ref = current.credential_ref.unwrap();
            assert_eq!(token_of(&secrets, &current_ref).unwrap(), token);
            for (reference, old_token) in &retained {
                assert_ne!(&current_ref, reference);
                assert_eq!(token_of(&secrets, reference).unwrap(), *old_token);
            }
            retained.push((current_ref, token.to_string()));
        }
    }

    #[test]
    fn managed_rotation_preserves_channel_id_credential() {
        let _env = env_lock();
        let _fake = EnvRestore::set("ANYAGENTS_FAKE_MODE", "1");
        let (dir, store, agent, manager) = custody_fixture();
        let spare = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: Some("new_bot".into()),
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        let legacy_ref = format!("channels/{}", spare.id);
        let secrets = FileCredentialStore::new(dir.path());
        secrets.write(&legacy_ref, b"999:old-token").unwrap();
        store.set_credential_ref(&spare.id, &legacy_ref).unwrap();
        let spare_memory = Memory {
            bot: Some("999:new_bot".into()),
            ..Memory::default()
        };
        store
            .set_channel_state(&spare.id, &spare_memory.to_state())
            .unwrap();
        let (base, task) = provider_response(200, r#"{"ok":true,"result":"999:new-token"}"#);
        let _api = EnvRestore::set("ANYAGENTS_TELEGRAM_API", &base);
        let body = serde_json::json!([{"update_id":12,"managed_bot":{"user":{"id":42,"first_name":"Alex"},"bot":{"id":999,"is_bot":true,"username":"new_bot"}}}]).to_string();
        let mut memory = Memory::try_from_state(&manager.state).unwrap();
        let result = ingest_updates(
            &store,
            &manager,
            &agent.id,
            "manager-token",
            &mut memory,
            &body,
            "getUpdates",
        );
        task.join().unwrap();
        result.expect("managed rotation must preserve a channel-ID credential file");
        assert_eq!(memory.offset, 13);
        let reopened = Store::open(dir.path().join("state.db")).unwrap();
        let current = reopened
            .list_channels()
            .unwrap()
            .into_iter()
            .find(|ch| ch.id == spare.id)
            .unwrap();
        assert!(current.agents.is_empty());
        assert_eq!(reopened.list_channels().unwrap().len(), 2);
        assert_eq!(
            token_of(&secrets, current.credential_ref.as_deref().unwrap()).unwrap(),
            "999:new-token"
        );
        assert_eq!(token_of(&secrets, &legacy_ref).unwrap(), "999:old-token");
    }

    #[test]
    fn validated_identity_replaces_binding_without_erasing_prior_credential() {
        let _env = env_lock();
        let _fake = EnvRestore::set("ANYAGENTS_FAKE_MODE", "1");
        let (dir, store, agent, _) = custody_fixture();
        let (base, task) = provider_response(
            200,
            r#"{"ok":true,"result":{"id":999,"is_bot":true,"username":"new_bot"}}"#,
        );
        let _api = EnvRestore::set("ANYAGENTS_TELEGRAM_API", &base);
        let result =
            bind_agent_telegram_token(&store, dir.path(), &agent, "999:new-token", None).unwrap();
        task.join().unwrap();
        assert_eq!(result["status"], "bound");
        let channel = channel_for(&store, &agent.name).unwrap();
        assert_eq!(channel.identity.as_deref(), Some("new_bot"));
        assert_eq!(
            token_of(
                &FileCredentialStore::new(dir.path()),
                channel.credential_ref.as_deref().unwrap()
            )
            .unwrap(),
            "999:new-token"
        );
        assert_eq!(
            token_of(&FileCredentialStore::new(dir.path()), "channels/helper").unwrap(),
            "777:old-token"
        );
    }

    #[test]
    fn binding_detach_or_identity_write_failure_is_not_reported_bound() {
        let _env = env_lock();
        let _fake = EnvRestore::set("ANYAGENTS_FAKE_MODE", "1");
        for trigger in [
            "CREATE TRIGGER reject_change BEFORE DELETE ON channel_agents BEGIN SELECT RAISE(ABORT, 'detach failure'); END;",
            "CREATE TRIGGER reject_change BEFORE UPDATE OF identity ON channels BEGIN SELECT RAISE(ABORT, 'identity failure'); END;",
        ] {
            let (dir, store, agent, _) = custody_fixture();
            let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
            conn.execute_batch(trigger).unwrap();
            let err = bind_agent_telegram_token(
                &store,
                dir.path(),
                &agent,
                "999:new-token",
                Some((999, "new_bot")),
            )
            .unwrap_err();
            assert!(err.to_string().contains("failure"), "{err}");
            assert_eq!(
                token_of(&FileCredentialStore::new(dir.path()), "channels/helper").unwrap(),
                "777:old-token"
            );
        }
    }

    #[test]
    fn managed_bot_custody_failure_preserves_offset_and_retries_same_unassigned_channel() {
        let _env = env_lock();
        let _fake = EnvRestore::set("ANYAGENTS_FAKE_MODE", "1");
        let (dir, store, agent, channel) = custody_fixture();
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute_batch("CREATE TRIGGER reject_state BEFORE UPDATE OF state ON channels WHEN NEW.identity='new_bot' BEGIN SELECT RAISE(ABORT, 'managed custody failure'); END;").unwrap();
        let body=serde_json::json!([{"update_id":12,"managed_bot":{"user":{"id":42,"first_name":"Alex"},"bot":{"id":999,"is_bot":true,"username":"new_bot"}}}]).to_string();
        let mut memory = Memory::try_from_state(&channel.state).unwrap();
        for failed in [true, false] {
            let (base, task) = provider_response(200, r#"{"ok":true,"result":"999:new-token"}"#);
            let _api = EnvRestore::set("ANYAGENTS_TELEGRAM_API", &base);
            let result = ingest_updates(
                &store,
                &channel,
                &agent.id,
                "manager-token",
                &mut memory,
                &body,
                "getUpdates",
            );
            task.join().unwrap();
            if failed {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("managed custody failure")
                );
                let fresh = store
                    .list_channels()
                    .unwrap()
                    .into_iter()
                    .find(|ch| ch.id == channel.id)
                    .unwrap();
                assert_eq!(Memory::try_from_state(&fresh.state).unwrap().offset, 0);
                conn.execute_batch("DROP TRIGGER reject_state").unwrap();
            } else {
                result.unwrap();
                assert_eq!(memory.offset, 13);
            }
            assert_eq!(store.list_channels().unwrap().len(), 2);
        }
        let fresh = store
            .list_channels()
            .unwrap()
            .into_iter()
            .find(|ch| ch.identity.as_deref() == Some("new_bot"))
            .unwrap();
        assert!(fresh.agents.is_empty());
        assert_eq!(
            Memory::try_from_state(&fresh.state).unwrap().bot.as_deref(),
            Some("999:new_bot")
        );
    }
    #[test]
    fn immutable_sender_id_cannot_resolve_to_an_id_shaped_display_name() {
        let _env = env_lock();
        let (_dir, store, agent, channel) = custody_fixture();
        let impostor = store
            .create_agent(anyagents_core::NewAgent::new(agent.id.to_string(), "codex"))
            .unwrap();
        let other = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/impostor".into()),
            })
            .unwrap();
        store.attach_channel(&other.id, &impostor.id).unwrap();
        assert_eq!(
            channel_for(&store, agent.id.as_str()).unwrap().id,
            channel.id
        );
        assert_eq!(channel_for(&store, &agent.name).unwrap().id, channel.id);
        record_chat(&store, agent.id.as_str(), -1009).unwrap();
        let fresh = store
            .list_channels()
            .unwrap()
            .into_iter()
            .find(|ch| ch.id == other.id)
            .unwrap();
        assert!(!Memory::try_from_state(&fresh.state).unwrap().is_room(-1009));
    }
    #[test]
    fn outbound_peer_failure_keeps_pinned_recipients_and_replays_without_remote_send() {
        let _env = env_lock();
        let (dir, store, agent, _) = custody_fixture();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("peer", "codex"))
            .unwrap();
        let channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        store.attach_channel(&channel.id, &peer.id).unwrap();
        let mut memory = Memory::default();
        memory.attach_typed(peer.id.as_str(), -1007, Some("supergroup"));
        store
            .set_channel_state(&channel.id, &memory.to_state())
            .unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute_batch(&format!("CREATE TRIGGER reject_peer BEFORE INSERT ON messages WHEN NEW.agent_id='{}' BEGIN SELECT RAISE(ABORT, 'peer custody failure'); END;", peer.id)).unwrap();
        deliver_locally_with_metadata(
            &store,
            agent.id.as_str(),
            -1007,
            91,
            "sent once",
            Some(2),
            None,
        )
        .unwrap();
        assert!(store.inbox(&peer.id, None).unwrap().is_empty());
        assert_eq!(
            std::fs::read_dir(dir.path().join("telegram-relay-pending"))
                .unwrap()
                .count(),
            1
        );
        assert!(replay_local_receipts(&store).is_err());
        // A later membership change cannot remove the accepted obligation.
        store.set_channel_state(&channel.id, "{}").unwrap();
        conn.execute_batch("DROP TRIGGER reject_peer").unwrap();
        assert_eq!(replay_local_receipts(&store).unwrap(), 1);
        assert_eq!(replay_local_receipts(&store).unwrap(), 0);
        let messages = store.inbox(&peer.id, None).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].text.contains("sent once"));
    }
    #[test]
    fn later_chunk_failure_keeps_accepted_chunk_local_custody() {
        let _env = env_lock();
        let _fake = EnvRestore::set("ANYAGENTS_FAKE_MODE", "1");
        let (dir, store, agent, _) = custody_fixture();
        let peer = store
            .create_agent(anyagents_core::NewAgent::new("peer", "codex"))
            .unwrap();
        let channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        store.attach_channel(&channel.id, &peer.id).unwrap();
        let mut memory = Memory::default();
        memory.attach_typed(peer.id.as_str(), -1007, Some("supergroup"));
        store
            .set_channel_state(&channel.id, &memory.to_state())
            .unwrap();
        let (base, task) = provider_response(
            200,
            r#"{"ok":true,"result":{"message_id":91,"chat":{"id":-1007,"type":"supergroup"}}}"#,
        );
        let _api = EnvRestore::set("ANYAGENTS_TELEGRAM_API", &base);
        // The provider accepts one chunk, then closes its listener. No second
        // receipt exists, so only that accepted chunk may reach local peers.
        let text = "x".repeat(MAX_TELEGRAM_CHUNK_CHARS + 20);
        assert!(
            post_text(
                &store,
                dir.path(),
                agent.id.as_str(),
                -1007,
                &text,
                None,
                None
            )
            .is_err()
        );
        task.join().unwrap();
        let messages = store.inbox(&peer.id, None).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .text
                .contains(&"x".repeat(MAX_TELEGRAM_CHUNK_CHARS))
        );
        assert!(!messages[0].text.contains(&text));
    }
}
