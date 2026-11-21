//! Interactive message actions and agent, group, operator, and Telegram delivery.

use anyagents_core::{Address, Agent, Incoming, ReceiveOutcome, Store};
use axum::Json;
use axum::http::StatusCode;

use crate::api::{Delivery, InteractRequest, InteractResponse, SendRequest};
use crate::auth::Principal;

use super::agents_lifecycle::is_explicitly_archived;
use super::messaging::{Actor, SendPlan, can_actor_message_agent, own_message};
use super::types::{ApiError, resolve_remote_sandbox_workdir};

#[path = "messaging_delivery/chat_admin.rs"]
mod chat_admin;

#[path = "messaging_delivery/interact_route.rs"]
mod interact_route;
pub(super) use interact_route::interact_route;

fn interact_sync(
    store: &Store,
    principal: &Principal,
    actor_name: &str,
    body: InteractRequest,
) -> Result<Json<InteractResponse>, ApiError> {
    let action = body.action.trim().to_ascii_lowercase();
    if crate::api::is_telegram_chat_admin_action(&action) {
        return chat_admin::apply(store, principal, action, body).map(Json);
    }
    let raw_target = body.target.trim();
    if raw_target.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "interact requires a non-empty target",
        ));
    }
    let actor = Actor::Agent(actor_name.to_string());
    let bot = body.bot.as_deref();
    let mut tg_coords: Option<(i64, i64, Option<i64>)> = None;
    let mut stored_msg: Option<anyagents_core::Message> = None;
    if let Some(msg_id_str) = raw_target
        .strip_prefix("anyagents:reply:")
        .or_else(|| raw_target.starts_with("msg_").then_some(raw_target))
    {
        if let Ok(Some(msg)) = own_message(store, principal, &actor, msg_id_str) {
            if let Some(ref origin) = msg.origin
                && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            {
                tg_coords = Some((chat_id, message_id, origin.thread_id));
            }
            stored_msg = Some(msg);
        } else if let Ok(agent_row) = store.agent_by_name(actor_name)
            && let Ok(Some(msg)) = store.message(&agent_row.id, msg_id_str)
        {
            if let Some(ref origin) = msg.origin
                && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            {
                tg_coords = Some((chat_id, message_id, origin.thread_id));
            }
            stored_msg = Some(msg);
        }
    } else if let Some(rest) = raw_target
        .strip_prefix("telegram:chat:")
        .or_else(|| raw_target.strip_prefix("telegram:user:"))
    {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() >= 2
            && let (Ok(chat_id), Ok(message_id)) = (
                parts[0].parse::<i64>(),
                parts[parts.len() - 1].parse::<i64>(),
            )
        {
            let thread_id = if parts.len() == 3 {
                parts[1].parse::<i64>().ok()
            } else {
                None
            };
            tg_coords = Some((chat_id, message_id, thread_id));
        }
    }
    let data_dir = store.data_dir();
    match action.as_str() {
        "react" => {
            let emoji = body.emoji.as_deref().map(str::trim);
            if let Some((chat_id, message_id, thread_id)) = tg_coords {
                crate::telegram::react_message(
                    store,
                    data_dir,
                    actor_name,
                    bot,
                    crate::telegram::ChatMessage {
                        chat_id,
                        message_id,
                        thread: thread_id,
                    },
                    emoji,
                )
                .map_err(ApiError::from_store)?;
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(format!(
                        "reacted {} on telegram:chat:{chat_id}:{message_id}",
                        emoji.unwrap_or("(cleared)")
                    )),
                }))
            } else if let Some(msg) = stored_msg {
                let e = emoji.filter(|s| !s.is_empty()).unwrap_or("👍");
                let summary = format!("[Reaction {e} on message {}]", msg.id);
                if let Some(ref sender) = msg.sender
                    && let Some(peer_name) = sender.strip_prefix("agent:")
                    && let Ok(peer) = store.agent_by_name(peer_name)
                    && peer.state != "archived"
                    && peer.state != "paused"
                    && !is_explicitly_archived(store.data_dir(), &peer)
                    && can_actor_message_agent(store, &actor, &peer).is_ok()
                {
                    let mut incoming = Incoming::new(summary.clone());
                    incoming.sender = Some(format!("agent:{actor_name}"));
                    incoming.reply_to = msg.reply_to.clone();
                    store
                        .receive(&peer.id, incoming)
                        .map_err(ApiError::from_store)?;
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(summary),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "react target must be anyagents:reply:msg_... or telegram:chat:<chat_id>:<message_id>",
                ))
            }
        }
        "vote" => {
            if body.options.is_empty() {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "vote requires at least one option in `options`",
                ));
            }
            let resolved_opts =
                resolve_poll_options(stored_msg.as_ref().map(|m| m.text.as_str()), &body.options);
            let comment = body
                .text
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let ballot = match comment {
                Some(note) => format!("🗳️ **Poll Vote**: {} — {note}", resolved_opts.join(", ")),
                None => format!("🗳️ **Poll Vote**: {}", resolved_opts.join(", ")),
            };
            if let Some((chat_id, message_id, thread_id)) = tg_coords {
                let _ = crate::telegram::react_message(
                    store,
                    data_dir,
                    actor_name,
                    bot,
                    crate::telegram::ChatMessage {
                        chat_id,
                        message_id,
                        thread: thread_id,
                    },
                    Some("👍"),
                );
                let posted_id = crate::telegram::post_text(
                    store,
                    data_dir,
                    actor_name,
                    bot,
                    chat_id,
                    &ballot,
                    Some(message_id),
                    thread_id,
                )
                .map_err(ApiError::from_store)?;
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(format!(
                        "voted ({}) on telegram:chat:{chat_id}:{message_id} (receipt #{posted_id})",
                        resolved_opts.join(", ")
                    )),
                }))
            } else if let Some(msg) = stored_msg {
                if let Some(ref sender) = msg.sender
                    && let Some(peer_name) = sender.strip_prefix("agent:")
                    && let Ok(peer) = store.agent_by_name(peer_name)
                    && peer.state != "archived"
                    && peer.state != "paused"
                    && !is_explicitly_archived(store.data_dir(), &peer)
                    && can_actor_message_agent(store, &actor, &peer).is_ok()
                {
                    let mut incoming = Incoming::new(ballot.clone());
                    incoming.sender = Some(format!("agent:{actor_name}"));
                    incoming.reply_to = msg.reply_to.clone();
                    store
                        .receive(&peer.id, incoming)
                        .map_err(ApiError::from_store)?;
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(ballot),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "vote target must be anyagents:reply:msg_... or telegram:chat:<chat_id>:<message_id>",
                ))
            }
        }
        "check" | "uncheck" => {
            if body.options.is_empty() {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "check/uncheck requires at least one task number or text in `options`",
                ));
            }
            let mark_done = action == "check";
            if let Some(ref msg) = stored_msg {
                let updated = toggle_checklist_items(&msg.text, &body.options, mark_done);
                if let Some((chat_id, message_id, thread_id)) = tg_coords {
                    crate::telegram::edit_message(
                        store,
                        data_dir,
                        actor_name,
                        bot,
                        crate::telegram::ChatMessage {
                            chat_id,
                            message_id,
                            thread: thread_id,
                        },
                        &updated,
                    )
                    .map_err(ApiError::from_store)?;
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(updated),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "check/uncheck target must resolve to a stored checklist message",
                ))
            }
        }
        "edit" => {
            let new_text = body
                .text
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "edit requires non-empty `text`")
                })?;
            let (chat_id, message_id, thread_id) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "edit target must resolve to a Telegram message",
                )
            })?;
            crate::telegram::edit_message(
                store,
                data_dir,
                actor_name,
                bot,
                crate::telegram::ChatMessage {
                    chat_id,
                    message_id,
                    thread: thread_id,
                },
                new_text,
            )
            .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("edited telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        "delete" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "delete target must resolve to a Telegram message",
                )
            })?;
            crate::telegram::delete_chat_message(
                store, data_dir, actor_name, bot, chat_id, message_id,
            )
            .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("deleted telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        "pin" | "unpin" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "pin/unpin target must resolve to a Telegram message",
                )
            })?;
            let pin = action == "pin";
            crate::telegram::pin_message(
                store, data_dir, actor_name, bot, chat_id, message_id, pin,
            )
            .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!(
                    "{} telegram:chat:{chat_id}:{message_id}",
                    if pin { "pinned" } else { "unpinned" }
                )),
            }))
        }
        "stop_poll" | "close_poll" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "stop_poll target must resolve to a Telegram poll message",
                )
            })?;
            crate::telegram::close_poll(store, data_dir, actor_name, bot, chat_id, message_id)
                .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("closed poll telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        other => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "unsupported interact action `{other}`; expected react, vote, check, uncheck, edit, delete, pin, unpin, stop_poll, set_chat_title, set_chat_description, or set_chat_photo"
            ),
        )),
    }
}

fn toggle_checklist_items(message_text: &str, selected: &[String], mark_done: bool) -> String {
    let target_box = if mark_done { "☑" } else { "☐" };
    message_text
        .lines()
        .map(|line| {
            let trimmed = line.trim_start();
            let rest = trimmed
                .strip_prefix("☐ ")
                .or_else(|| trimmed.strip_prefix("☑ "));
            if let Some(after_box) = rest {
                let idx_str = after_box.split('.').next().unwrap_or("").trim();
                let matched = selected.iter().any(|sel| {
                    let s = sel.trim();
                    s == idx_str
                        || after_box
                            .to_ascii_lowercase()
                            .contains(&s.to_ascii_lowercase())
                });
                if matched {
                    return format!("{target_box} {after_box}");
                }
            }
            line.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn resolve_poll_options(message_text: Option<&str>, chosen: &[String]) -> Vec<String> {
    let mut parsed_options: Vec<String> = Vec::new();
    if let Some(text) = message_text
        && let Some(idx) = text.find("| Options: ")
    {
        let rest = &text[idx + "| Options: ".len()..];
        let end = rest.rfind(']').unwrap_or(rest.len());
        for item in rest[..end].split("; ") {
            let cleaned = item.split(" (").next().unwrap_or(item).trim().to_string();
            if !cleaned.is_empty() {
                parsed_options.push(cleaned);
            }
        }
    }
    chosen
        .iter()
        .map(|sel| {
            let trimmed = sel.trim();
            if let Ok(num) = trimmed.parse::<usize>()
                && num >= 1
                && num <= parsed_options.len()
            {
                return parsed_options[num - 1].clone();
            }
            trimmed.to_string()
        })
        .collect()
}

/// Everyone a room message will reach, without writing anything: the room's
/// members (its chat, when it names one) minus the sender.
fn body_author_is(body: &SendRequest, agent: &Agent) -> bool {
    let Some(sender) = body.sender.as_deref() else {
        return false;
    };
    let sender = sender.strip_prefix("agent:").unwrap_or(sender);
    match anyagents_core::AgentId::parse(sender) {
        Ok(id) => id == agent.id,
        Err(_) => sender == agent.name,
    }
}

pub(super) fn room_recipients(store: &Store, body: &SendRequest, room: &str) -> Vec<Agent> {
    let members = match store.room_members(room) {
        Ok(members) => members,
        Err(_) => return Vec::new(),
    };
    store
        .list_live_agents()
        .unwrap_or_default()
        .into_iter()
        .filter(|agent| {
            (agent.state == "running" || agent.state == "idle")
                && !is_explicitly_archived(store.data_dir(), agent)
        })
        .filter(|agent| members.is_some() || agent.parent_id.is_none())
        .filter(|agent| !body_author_is(body, agent))
        .filter(|agent| {
            members
                .as_ref()
                .is_none_or(|members| members.iter().any(|name| name == &agent.name))
        })
        .collect()
}

/// Archived and otherwise inactive agents a room send skipped. Broadcasts do
/// not address retired agents; explicit group membership can.
pub(super) fn room_skipped(
    store: &Store,
    body: &SendRequest,
    room: &str,
) -> (
    Vec<crate::api::SkippedDelivery>,
    Vec<crate::api::SkippedDelivery>,
) {
    let name = room.strip_prefix("group:").unwrap_or(room);
    if name == "all" || name == "peers" {
        let inactive = store
            .list_live_agents()
            .unwrap_or_default()
            .into_iter()
            .filter(|agent| agent.parent_id.is_none())
            .filter(|agent| !body_author_is(body, agent))
            .filter(|agent| {
                agent.state != "running" && agent.state != "idle" && agent.state != "archived"
            })
            .map(|agent| crate::api::SkippedDelivery {
                to: format!("agent:{}", agent.name),
                reason: agent.state,
            })
            .collect();
        return (Vec::new(), inactive);
    }
    let members = match store.room_members(room) {
        Ok(Some(members)) => members,
        _ => return (Vec::new(), Vec::new()),
    };
    // One row per member name, in name order.
    let members: std::collections::BTreeMap<_, _> = members
        .iter()
        .filter_map(|member| store.agent_by_name(member).ok())
        .filter(|agent| members.contains(&agent.name))
        .map(|agent| (agent.name.clone(), agent))
        .collect();
    let mut archived = Vec::new();
    let mut inactive = Vec::new();
    for agent in members
        .into_values()
        .filter(|agent| !body_author_is(body, agent))
    {
        if is_explicitly_archived(store.data_dir(), &agent) {
            archived.push(archived_skip(store, &agent));
        } else if agent.state != "running" && agent.state != "idle" {
            inactive.push(crate::api::SkippedDelivery {
                to: format!("agent:{}", agent.name),
                reason: agent.state,
            });
        }
    }
    (archived, inactive)
}

/// One room message, one delivery per recipient.
/// `upload` was read once as the authenticated principal, before any delivery
/// effects. Each recipient gets a private copy, never a sender-private path.
fn recipient_media_text(
    store: &Store,
    body: &SendRequest,
    agent: &anyagents_core::Agent,
    upload: Option<&crate::telegram::MediaUpload>,
) -> Result<(String, Option<crate::seat_tree::InboundMedia>), anyagents_core::Error> {
    let (Some(media), Some(upload)) = (&body.media, upload) else {
        return Ok((format_internal_send_text(body), None));
    };
    let cfg_dir = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    if media.source.trim().starts_with("media://")
        && crate::remote::runs_off_host_in(store, agent, &cfg_dir)
    {
        let href = super::private_media::copy_for_recipient(store, &agent.id, upload)?;
        let line = format!("[Media ({}): {href}]", media.kind);
        return Ok((
            format_internal_send_text_with_attachment(body, Some(&line)),
            None,
        ));
    }
    let uid = crate::sandbox::seat_for_agent(agent.id.as_str()).map(|(_, uid, _, _)| uid);
    if uid.is_none() && crate::sandbox::seats_configured() {
        return Err(anyagents_core::Error::PermissionDenied(
            "recipient has no admitted seat".into(),
        ));
    }
    let staged = crate::seat_tree::persist_inbound_media(
        store.data_dir(),
        &agent.id,
        uid,
        &upload.file_name,
        &upload.bytes,
    )?;
    let line = anyagents_harness::media::local_media_line(&media.kind, staged.path());
    Ok((
        format_internal_send_text_with_attachment(body, Some(&line)),
        Some(staged),
    ))
}

pub(super) fn deliver_room_with_media(
    store: &Store,
    body: &SendRequest,
    room: &str,
    key: Option<&str>,
    upload: Option<&crate::telegram::MediaUpload>,
) -> Result<Vec<Delivery>, ApiError> {
    let delivered = store
        .try_receive_room_with(
            room,
            Incoming {
                text: body.text.clone(),
                sender: body.sender.clone(),
                source_time_ms: None,
                source_key: key.map(str::to_string),
                reply_to: body.reply_to.clone(),
                quote: body.quote.clone(),
                origin: None,
                received_at_ms: None,
            },
            |agent, mut incoming| {
                let _lifecycle = store.lock_agent_lifecycle(&agent.id)?;
                let agent = store.agent(&agent.id)?;
                if is_explicitly_archived(store.data_dir(), &agent) {
                    return Ok(None);
                }
                let (text, staged) = recipient_media_text(store, body, &agent, upload)?;
                incoming.text = text;
                let outcome = store.receive(&agent.id, incoming)?;
                if matches!(outcome, ReceiveOutcome::New(_))
                    && let Some(staged) = staged
                {
                    staged.retain();
                }
                Ok(Some(outcome))
            },
        )
        .map_err(ApiError::from_store)?;
    Ok(delivered
        .into_iter()
        .map(|(agent, outcome)| {
            let (message, duplicate) = match outcome {
                ReceiveOutcome::New(message) => (message, false),
                ReceiveOutcome::Duplicate(message) => (message, true),
            };
            Delivery {
                to: format!("agent:{}", agent.name),
                id: message.id.to_string(),
                seq: message.seq,
                duplicate,
            }
        })
        .collect())
}

/// The refusal of a send to `name` when that agent is archived: it reads
/// nothing until it is explicitly woken. Names who answers for it instead.
pub(super) fn archived_refusal(
    store: &Store,
    recipient: &Agent,
) -> Option<crate::api::SkippedDelivery> {
    if !is_explicitly_archived(store.data_dir(), recipient) {
        return None;
    }
    Some(archived_skip(store, recipient))
}

fn archived_skip(store: &Store, recipient: &Agent) -> crate::api::SkippedDelivery {
    let answers = match store.live_ancestor(recipient).ok().flatten() {
        Some(parent) if recipient.parent_id.as_ref() == Some(&parent.id) => {
            format!("its parent is `{}`", parent.name)
        }
        Some(ancestor) => format!("the nearest running agent above it is `{}`", ancestor.name),
        None => "no running agent is above it".to_string(),
    };
    crate::api::SkippedDelivery {
        to: format!("agent:{}", recipient.name),
        reason: format!("agent `{}` is archived; {answers}", recipient.name),
    }
}

/// A send never unarchives a recipient: only an explicit wake does.
pub(super) fn archived_conflict(reasons: &str) -> ApiError {
    ApiError::conflict(format!(
        "{reasons}. A send never wakes an archived agent: message a live agent instead, or \
         explicitly wake the archived recipient first with `agents(action=\"wake\")`."
    ))
}

/// Store one send for an already authorized recipient ID. An archived recipient is refused here too,
/// under its lifecycle lock, so a send racing an archive can never store
/// mail it would not read nor flip it back to `running`.
pub(super) fn deliver_agent_with_media(
    store: &Store,
    body: &SendRequest,
    agent_id: &anyagents_core::AgentId,
    key: Option<&str>,
    upload: Option<&crate::telegram::MediaUpload>,
    turn_context: Option<(&anyagents_core::AgentId, &str)>,
) -> Result<Delivery, ApiError> {
    let _lifecycle = store
        .lock_agent_lifecycle(agent_id)
        .map_err(ApiError::from_store)?;
    let agent = store.agent(agent_id).map_err(ApiError::from_store)?;
    if is_explicitly_archived(store.data_dir(), &agent) {
        return Err(archived_conflict(&archived_skip(store, &agent).reason));
    }
    // A crash may leave the mailbox row without a whole-send receipt. Check it
    // before staging media or changing lifecycle state.
    if let Some(key) = key
        && let Some(message) = store
            .message_by_source_key(&agent.id, key)
            .map_err(ApiError::from_store)?
    {
        return Ok(Delivery {
            to: format!("agent:{}", agent.name),
            id: message.id.to_string(),
            seq: message.seq,
            duplicate: true,
        });
    }
    let (text, staged) =
        recipient_media_text(store, body, &agent, upload).map_err(ApiError::from_store)?;
    let trimmed = body.text.trim_start();
    let is_auto_subagent_lifecycle =
        trimmed.starts_with("[Sub-Agent Completed]") || trimmed.starts_with("[Sub-Agent Error]");
    if (agent.state == "idle" || agent.state == "created")
        && !(agent.parent_id.is_none() && is_auto_subagent_lifecycle)
    {
        let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
        if !crate::remote::is_agent_cloud(&agent, Some(store.data_dir()), Some(&cfg)) {
            let remote_workdir = resolve_remote_sandbox_workdir(store, &agent);
            match crate::sandbox_exec::ensure_sandbox_for_agent(
                store,
                &agent,
                &cfg,
                Some(remote_workdir.as_str()),
                &std::collections::BTreeMap::new(),
            ) {
                Ok(Some(_)) | Ok(None) => {}
                Err(err)
                    if crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
                        || !matches!(store.sandbox_credential_ref(&agent.id), Ok(None)) =>
                {
                    return Err(ApiError::new(StatusCode::BAD_GATEWAY, err));
                }
                Err(err) => {
                    eprintln!("anyagents-sandbox: delivery lease ensure failed: {err}");
                }
            }
        }
        let _ = store.set_agent_state(&agent.id, "running");
    }
    let incoming = Incoming {
        text,
        sender: body.sender.clone(),
        source_time_ms: None,
        source_key: key.map(str::to_string),
        reply_to: body.reply_to.clone(),
        quote: body.quote.clone(),
        origin: None,
        received_at_ms: None,
    };
    let outcome = if let Some((sender, token)) = turn_context {
        store.receive_turn_send(&agent.id, incoming, sender, token)
    } else {
        store.receive(&agent.id, incoming)
    }
    .map_err(ApiError::from_store)?;
    let (message, duplicate) = match outcome {
        ReceiveOutcome::New(message) => (message, false),
        ReceiveOutcome::Duplicate(message) => (message, true),
    };
    if !duplicate {
        if let Some(staged) = staged {
            staged.retain();
        }
        crate::create::poke_agent_wake_socket(store.data_dir(), &agent.id);
        crate::create::poke_agent_wake_socket_mail(store.data_dir(), &agent.id);
    }
    Ok(Delivery {
        to: format!("agent:{}", agent.name),
        id: message.id.to_string(),
        seq: message.seq,
        duplicate,
    })
}

/// Who a report to the operator is from: the admitted agent's immutable id,
/// which keys its rate limit, and its display name; or the operator's own
/// address.
pub(super) fn operator_sender(actor: &Actor, plan: &SendPlan) -> (String, Option<String>) {
    match (&plan.sender_id, actor) {
        (Some(id), Actor::Agent(name)) => (format!("agent:{id}"), Some(name.clone())),
        (Some(id), Actor::Operator(_)) => (format!("agent:{id}"), None),
        (None, actor) => (actor.as_address(), None),
    }
}

pub(super) fn operator_limited(retry_after_ms: i64) -> ApiError {
    let seconds = (retry_after_ms + 999) / 1000;
    ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        format!(
            "the operator inbox takes at most {} reports a minute and {} an hour from one sender, \
             and {} a minute and {} an hour in all; nothing was sent. Retry in {seconds}s with \
             one report that covers everything open",
            anyagents_core::OPERATOR_BURST,
            anyagents_core::OPERATOR_HOURLY_CAP,
            anyagents_core::OPERATOR_FLEET_BURST,
            anyagents_core::OPERATOR_FLEET_HOURLY_CAP,
        ),
    )
}

/// Store one report, or count it on the report it repeats. A new report is
/// also recorded in its agent sender's own history.
pub(super) fn deliver_operator(
    store: &Store,
    actor: &Actor,
    plan: &SendPlan,
    reply_to: Option<&str>,
    body: &str,
) -> Result<Delivery, ApiError> {
    let (sender, sender_name) = operator_sender(actor, plan);
    let receipt = store
        .report_to_operator(anyagents_core::OperatorReport {
            sender,
            sender_name,
            body: body.to_string(),
            received_at_ms: None,
        })
        .map_err(ApiError::from_store)?;
    let (message, duplicate) = match receipt {
        anyagents_core::OperatorReceipt::New(message) => (message, false),
        anyagents_core::OperatorReceipt::Duplicate(message) => (message, true),
        anyagents_core::OperatorReceipt::Limited { retry_after_ms } => {
            return Err(operator_limited(retry_after_ms));
        }
    };
    if !duplicate && let Some(sid) = &plan.sender_id {
        store
            .record_outbound_once(message.id.as_str(), sid, "operator", reply_to, body)
            .map_err(ApiError::from_store)?;
    }
    Ok(Delivery {
        to: "operator".to_string(),
        id: message.id.to_string(),
        seq: message.seq,
        duplicate,
    })
}

/// Resolve the bot, the chat, the topic and the reply before any network
/// call. This does not post.
pub(super) fn prepare_telegram(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
) -> Result<(), ApiError> {
    let from = telegram_sender(actor)?;
    let (chat_id, thread_id, reply_to) = telegram_target(chat, thread, reply, body)?;
    crate::telegram::outbound_call(
        store,
        from,
        body.bot.as_deref(),
        chat_id,
        &body.text,
        reply_to,
        thread_id,
    )
    .map_err(ApiError::from_store)?;
    Ok(())
}

/// The bot a send posts through: always the authenticated actor's own.
fn telegram_sender(actor: &Actor) -> Result<&str, ApiError> {
    match actor {
        Actor::Agent(name) => Ok(name),
        Actor::Operator(_) => Err(ApiError::forbidden(
            "a post to a channel needs an agent's own bot",
        )),
    }
}

/// The chat, forum topic and replied-to message as numbers. The explicit
/// `reply_to` wins over the one an address names.
fn telegram_target(
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
    body: &SendRequest,
) -> Result<(i64, Option<i64>, Option<i64>), ApiError> {
    let chat_id: i64 = chat
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "telegram chat id is not a number"))?;
    let thread_id = match thread {
        None => None,
        Some(value) => Some(number(value, "telegram topic id is not a number")?),
    };
    let reply_to = telegram_reply(body, reply)?;
    Ok((chat_id, thread_id, reply_to))
}

fn number(value: &str, what: &'static str) -> Result<i64, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, what))
}

fn telegram_reply(body: &SendRequest, reply: Option<&str>) -> Result<Option<i64>, ApiError> {
    let candidate = reply.or_else(|| {
        body.reply_to
            .as_deref()
            .filter(|v| crate::reply::ref_message(v).is_none() && Address::parse(v).is_err())
    });
    match candidate {
        None => Ok(None),
        Some(value) => number(value, "reply_to is not a telegram message id").map(Some),
    }
}

pub(super) fn format_internal_send_text(body: &SendRequest) -> String {
    format_internal_send_text_with_attachment(body, None)
}

fn format_internal_send_text_with_attachment(
    body: &SendRequest,
    attachment: Option<&str>,
) -> String {
    let mut parts = Vec::new();
    if !body.text.trim().is_empty() {
        parts.push(body.text.clone());
    }
    if let Some(ref media) = body.media {
        parts.push(
            attachment
                .map(str::to_owned)
                .unwrap_or_else(|| format!("[Attachment ({}): {}]", media.kind, media.source)),
        );
    }
    if let Some(ref loc) = body.location {
        match (&loc.title, &loc.address) {
            (Some(t), Some(a)) if !t.is_empty() && !a.is_empty() => {
                parts.push(format!(
                    "[Venue: {t} — {a} (GPS: {:.6}, {:.6})]",
                    loc.latitude, loc.longitude
                ));
            }
            _ => {
                parts.push(format!(
                    "[Location GPS: latitude={:.6}, longitude={:.6}]",
                    loc.latitude, loc.longitude
                ));
            }
        }
    }
    if let Some(ref poll) = body.poll {
        parts.push(format!(
            "[Poll: \"{}\" | Options: {}]",
            poll.question,
            poll.options.join(" / ")
        ));
    }
    if let Some(ref cl) = body.checklist {
        parts.push(anyagents_channel_telegram::format_checklist_markdown(
            &cl.title, &cl.tasks,
        ));
    }
    if let Some(ref art) = body.article {
        match (&art.url, &art.content) {
            (Some(u), _) if !u.trim().is_empty() => {
                parts.push(format!("📰 [**{}**]({})", art.title, u.trim()));
            }
            (_, Some(c)) if !c.trim().is_empty() => {
                parts.push(format!("📰 **{}**\n\n{}", art.title, c.trim()));
            }
            _ => {
                parts.push(format!("📰 **{}**", art.title));
            }
        }
    }
    if parts.is_empty() {
        body.text.clone()
    } else {
        parts.join("\n")
    }
}

/// `id` is the Telegram message id. This delivery has no mailbox sequence.
#[allow(clippy::too_many_arguments)]
pub(super) fn deliver_telegram(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    upload: Option<&crate::telegram::MediaUpload>,
    address: &str,
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
) -> Result<Delivery, ApiError> {
    let from = telegram_sender(actor)?;
    let (chat_id, thread_id, reply_to) = telegram_target(chat, thread, reply, body)?;
    let message_id = if let Some(ref media) = body.media {
        let caption = body
            .text
            .strip_suffix(&format!(
                "\n[Attachment ({}): {}]",
                media.kind, media.source
            ))
            .or_else(|| {
                body.text
                    .strip_prefix(&format!("[Attachment ({}): {}]", media.kind, media.source))
            })
            .map(str::trim)
            .filter(|s| !s.is_empty());
        crate::telegram::post_media(
            store,
            store.data_dir(),
            from,
            body.bot.as_deref(),
            chat_id,
            &media.kind,
            &media.source,
            upload,
            caption,
            media.title.as_deref(),
            media.performer.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref loc) = body.location {
        crate::telegram::post_location(
            store,
            store.data_dir(),
            from,
            body.bot.as_deref(),
            chat_id,
            loc.latitude,
            loc.longitude,
            loc.title.as_deref(),
            loc.address.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref poll) = body.poll {
        crate::telegram::post_poll(
            store,
            store.data_dir(),
            from,
            body.bot.as_deref(),
            chat_id,
            &poll.question,
            &poll.options,
            poll.is_anonymous,
            poll.allows_multiple_answers,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref cl) = body.checklist {
        crate::telegram::post_checklist(
            store,
            store.data_dir(),
            from,
            body.bot.as_deref(),
            chat_id,
            &cl.title,
            &cl.tasks,
            cl.business_connection_id.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref art) = body.article {
        crate::telegram::post_article(
            store,
            store.data_dir(),
            from,
            body.bot.as_deref(),
            chat_id,
            &art.title,
            art.content.as_deref(),
            art.url.as_deref(),
            art.author_name.as_deref(),
            None,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else {
        crate::telegram::post_text(
            store,
            store.data_dir(),
            from,
            body.bot.as_deref(),
            chat_id,
            &body.text,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    };
    // The post itself remembered the chat on the bot it went through
    // (`deliver_locally_with_metadata`), so later posts there reach the other
    // agents on that chat as mailbox messages.
    Ok(Delivery {
        to: address.to_string(),
        id: message_id.to_string(),
        seq: 0,
        duplicate: false,
    })
}
