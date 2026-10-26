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
    BotCall, MediaAttachment, Memory, Poll, Routed, bot_identity, delete_message,
    deliver_to_mailbox, edit_message_text, file_path_of, format_checklist_markdown, get_file,
    get_me, pin_chat_message, send_checklist, send_location, send_media_ref, send_message,
    send_poll, sent_message_id, set_message_reaction, stop_poll, unpin_chat_message,
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
            "allowed_updates": ["message", "edited_message", "my_chat_member", "message_reaction"],
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
        store.set_channel_state(&channel.id, &memory.to_state())?;
    }
    if std::env::var_os("ANYAGENTS_TELEGRAM_API").is_none() {
        let bot_tag = memory.bot.clone().unwrap_or_default();
        let group_ids: Vec<i64> = memory
            .chats
            .iter()
            .filter(|c| c.chat_id < 0)
            .map(|c| c.chat_id)
            .collect();
        let mut pruned_any = false;
        for gid in group_ids {
            let key = ( gid, format!("self:{}", channel.id) );
            let should_check = probed_chat_bots()
                .lock()
                .ok()
                .is_some_and(|mut g| g.insert(key));
            if should_check && !bot_tag.is_empty() && !probe_members(&token, gid, &bot_tag) {
                memory.chats.retain(|l| l.chat_id != gid);
                pruned_any = true;
            }
        }
        if pruned_any {
            let _ = store.set_channel_state(&channel.id, &memory.to_state());
        }
    }
    if std::env::var_os("ANYAGENTS_TELEGRAM_API").is_none()
        && memory.chats.iter().any(|c| c.title.is_none())
    {
        let mut updated_titles = false;
        let missing_ids: Vec<i64> = memory
            .chats
            .iter()
            .filter(|c| c.title.is_none())
            .map(|c| c.chat_id)
            .collect();
        for cid in missing_ids {
            let call = BotCall {
                method: "getChat".to_string(),
                body: serde_json::json!({ "chat_id": cid }),
            };
            let Some(resolved) = call_api(&token, &call, 5, Some(&channel.id))
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
            else {
                if cid < 0 {
                    memory.chats.retain(|l| l.chat_id != cid);
                    updated_titles = true;
                }
                continue;
            };
            for link in memory.chats.iter_mut().filter(|l| l.chat_id == cid) {
                link.title = Some(resolved.0.clone());
                if link.chat_type.is_none()
                    && let Some(ref ct) = resolved.1
                {
                    link.chat_type = Some(ct.clone());
                }
                updated_titles = true;
            }
        }
        if updated_titles {
            let _ = store.set_channel_state(&channel.id, &memory.to_state());
        }
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
    let own_bots = known_bot_identities(store, memory.bot.as_deref());
    let mut grew = false;
    for mut one in routed {
        // This bot answers in the chat its update came from, so a post it makes
        // there reaches the other agents on that chat.
        if let Routed::ToAgent { inbound, .. } = &mut one {
            if inbound.text.starts_with("[Member removed/left group:") {
                let lower_text = inbound.text.to_ascii_lowercase();
                let removed_own_bot = own_bots.iter().any(|(bot_id, bot_user)| {
                    (!bot_user.is_empty() && lower_text.contains(&format!("@{bot_user}")))
                        || bot_id.is_some_and(|id| lower_text.contains(&id.to_string()))
                });
                if removed_own_bot {
                    let before_len = memory.chats.len();
                    if let Some(my_bot) = memory.bot.as_deref()
                        && let Some((_, my_user)) = my_bot.split_once(':')
                        && lower_text.contains(&format!("@{}", my_user.to_ascii_lowercase()))
                    {
                        memory.chats.retain(|link| link.chat_id != inbound.chat_id);
                        if memory.chats.len() != before_len {
                            grew = true;
                        }
                    }
                    detach_removed_own_bots_from_chat(store, inbound.chat_id, &lower_text);
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
            if known || !is_group || !is_live {
                memory.attach_typed_titled(
                    agent_id.as_str(),
                    inbound.chat_id,
                    Some(&inbound.chat_type),
                    inbound.chat_title.as_deref(),
                );
            } else if probe_members(token, inbound.chat_id, memory.bot.as_deref().unwrap_or("")) {
                memory.attach_typed_titled(
                    agent_id.as_str(),
                    inbound.chat_id,
                    Some(&inbound.chat_type),
                    inbound.chat_title.as_deref(),
                );
            } else {
                continue;
            }
            if !known && is_group {
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
                let _ = store.receive(
                    agent_id,
                    anyagents_core::Incoming {
                        text: format!("[Added to group chat {chat_id}]"),
                        sender: Some(format!("telegram:chat:{chat_id}")),
                        source_time_ms: None,
                        source_key: Some(format!(
                            "tg-member-in:{}:{}:{}",
                            chat_id,
                            agent_id,
                            anyagents_core::now_ms()
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
                );
            } else {
                memory.chats.retain(|link| link.chat_id != *chat_id);
                let _ = store.receive(
                    agent_id,
                    anyagents_core::Incoming {
                        text: format!("[Removed from group chat {chat_id}]"),
                        sender: Some(format!("telegram:chat:{chat_id}")),
                        source_time_ms: None,
                        source_key: Some(format!(
                            "tg-member-out:{}:{}:{}",
                            chat_id,
                            agent_id,
                            anyagents_core::now_ms()
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
                );
            }
            grew = true;
        }
        let peers = peer_ids(store, &poll, &one, agent_id.as_str(), &attached)?;
        deliver_to_mailbox(store, &one, &peers)?;
        if let Routed::ToAgent { inbound, .. } = &one
            && (inbound.chat_type == "group" || inbound.chat_type == "supergroup")
        {
            let _ = relay_inbound_to_group_peers(store, agent_id, inbound);
        }
    }
    // The offset and the chats are saved only once the updates are stored, so a
    // restart asks for what it did not keep and a repeat is one row.
    if poll.offset != before || grew {
        memory.offset = poll.offset.max(memory.offset);
        store.set_channel_state(&channel.id, &memory.to_state())?;
    }
    Ok(())
}

fn known_bot_identities(store: &Store, current_bot: Option<&str>) -> Vec<(Option<i64>, String)> {
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
    if let Ok(channels) = store.list_channels() {
        for ch in channels {
            if ch.kind == "telegram" {
                let mem = Memory::from_state(&ch.state);
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
    out
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

fn detach_removed_own_bots_from_chat(store: &Store, chat_id: i64, lower_removal_text: &str) {
    let Ok(channels) = store.list_channels() else {
        return;
    };
    for ch in channels {
        if ch.kind != "telegram" {
            continue;
        }
        let mut mem = Memory::from_state(&ch.state);
        let matched = mem
            .bot
            .as_deref()
            .and_then(|b| b.split_once(':'))
            .is_some_and(|(id_s, u)| {
                let u_low = u.trim().trim_start_matches('@').to_ascii_lowercase();
                (!u_low.is_empty() && lower_removal_text.contains(&format!("@{u_low}")))
                    || (!id_s.trim().is_empty() && lower_removal_text.contains(id_s.trim()))
            });
        if matched {
            let before = mem.chats.len();
            mem.chats.retain(|l| l.chat_id != chat_id);
            if mem.chats.len() != before {
                let _ = store.set_channel_state(&ch.id, &mem.to_state());
            }
        }
    }
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
        let dest = media_dir.join(&file_name);
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
                if let Ok(mut file) = std::fs::File::create(&dest) {
                    let _ = std::io::copy(&mut reader, &mut file);
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt as _;
                        let _ =
                            std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o644));
                    }
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

fn probed_chat_bots() -> &'static std::sync::Mutex<std::collections::HashSet<(i64, String)>> {
    static PROBED: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<(i64, String)>>> =
        std::sync::OnceLock::new();
    PROBED.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

/// Passively check whether any other configured Telegram bot is also a member
/// of `chat_id`, and attach it so internal group relay reaches all member bots
/// even before they receive a direct mention.
fn discover_peer_bots_in_chat(
    store: &Store,
    token: &str,
    current_channel_id: &anyagents_core::ChannelId,
    chat_id: i64,
    chat_type: &str,
) {
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
        let mut other_mem = Memory::from_state(&other.state);
        if other_mem.chats.iter().any(|l| l.chat_id == chat_id) {
            continue;
        }
        if std::env::var_os("ANYAGENTS_TELEGRAM_API").is_none() {
            let key = (chat_id, other.id.to_string());
            if let Ok(mut guard) = probed_chat_bots().lock()
                && !guard.insert(key)
            {
                continue;
            }
        }
        if let Some(bot) = other_mem.bot.as_deref()
            && probe_members(token, chat_id, bot)
        {
            other_mem.attach_typed(other_agent_id.as_str(), chat_id, Some(chat_type));
            let _ = store.set_channel_state(&other.id, &other_mem.to_state());
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
    let members = store.chat_members(inbound.chat_id)?;
    for member_name in members {
        let Ok(peer) = store.agent_by_name(&member_name) else {
            continue;
        };
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
        let _ = store.receive(&peer.id, incoming);
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
        let memory = Memory::from_state(&one.state);
        let channel_agents: Vec<(String, String)> = one
            .agents
            .iter()
            .filter_map(|id| {
                store
                    .agent(id)
                    .ok()
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
    let Ok(agents) = store.list_agents() else {
        return text.to_string();
    };
    let mut agent_to_bot: Vec<(String, String)> = Vec::new();
    for channel in channels {
        if channel.kind != "telegram" {
            continue;
        }
        let mem = Memory::from_state(&channel.state);
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
            if let Some(agent) = agents
                .iter()
                .find(|a| &a.id == aid || a.name.eq_ignore_ascii_case(aid.as_str()))
            {
                let name_lower = agent.name.to_ascii_lowercase();
                if !name_lower.eq_ignore_ascii_case(bot_username)
                    && !agent_to_bot.iter().any(|(n, _)| n == &name_lower)
                {
                    agent_to_bot.push((name_lower, bot_username.to_string()));
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
    let mut first_response = String::new();
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
            first_response = response;
        }
    }
    let message_id = first_message_id.expect("at least one chunk");
    // Telegram accepted this post. Local retries below never issue another
    // API call. Crash-safe receipt custody is the server's responsibility.
    // Reuse the adapter's provider quote decoder, rather than trusting a caller
    // to supply a quoted author or message body.
    let (provider_thread, quote) = sent_metadata(&first_response);
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
    deliver_locally_with_metadata(store, from, chat_id, message_id, &summary, thread, None);
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
    deliver_locally_with_metadata(store, from, chat_id, message_id, &summary, thread, None);
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
    deliver_locally_with_metadata(store, from, chat_id, message_id, &summary, thread, None);
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
            );
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
            if let Some(parent) = token_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(&token_path, &tok);
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
        if chat_is_room(store, chat_id) {
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
            let _ = store.receive_room(&room, incoming);
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
    if chat_is_room(store, chat_id) {
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
        let _ = store.receive_room(&room, incoming);
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
    if chat_is_room(store, chat_id) {
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
        let _ = store.receive_room(&room, incoming);
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
    if chat_id < 0
        && let Ok(ch) = channel_for(store, agent)
        && let Some(ref cred) = ch.credential_ref
        && let Ok(token) = token_of(&FileCredentialStore::new(store.data_dir()), cred)
    {
        discover_peer_bots_in_chat(store, &token, &ch.id, chat_id, "supergroup");
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
    for channel in store.list_channels()? {
        let matched_agent = channel
            .agents
            .iter()
            .filter_map(|id| store.agent(id).ok())
            .find(|row| row.name == agent || row.id.as_str() == agent);
        let Some(agent_row) = matched_agent else {
            continue;
        };
        let mut memory = Memory::from_state(&channel.state);
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
            store.set_channel_state(&channel.id, &after)?;
        }
        return Ok(());
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
        "sendMessage" | "sendRichMessage" | "editMessageText" | "sendPhoto" | "sendVideo"
        | "sendAudio" | "sendVoice" | "sendDocument" | "sendAnimation" | "sendLocation"
        | "sendVenue" | "sendPoll" | "getFile" | "getUpdates" | "getMe" | "getChatMember"
        | "setWebhook" | "deleteWebhook" => method,
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
        let dir = tempfile::tempdir().unwrap();
        let secrets = FileCredentialStore::new(dir.path());
        secrets
            .write("channels/xenon", b"8921438889:AAExampleTokenValue\n")
            .unwrap();
        assert_eq!(
            token_of(&secrets, "channels/xenon").unwrap(),
            "8921438889:AAExampleTokenValue"
        );
    }
}
