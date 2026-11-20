//! Message planning, authorization, send pipeline, and outbound recovery routes.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyagents_core::{Address, Agent, Error, Store};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::api::{SendRequest, SendResponse};
use crate::auth::Principal;

use super::agents_lifecycle::is_explicitly_archived;
use super::messaging_conversation::{
    conversation_route, inbox, media_file_route, operator_inbox, operator_reports,
    redact_conversation_route,
};
use super::messaging_delivery::{
    archived_conflict, archived_refusal, deliver_agent_with_media, deliver_operator,
    deliver_room_with_media, deliver_telegram, format_internal_send_text, interact_route,
    operator_limited, operator_sender, prepare_telegram, room_recipients, room_skipped,
};
use super::types::{
    ApiError, AppState, is_self_or_ancestor, refuse_host_media_over_http, require_operator,
    spawn_store, via_http_mcp,
};
use super::{outbound_history, serverless};

#[path = "messaging_memory.rs"]
mod messaging_memory;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/agents/{name}/inbox", get(inbox))
        .merge(messaging_memory::router())
        .route("/v1/agents/{name}/conversation", get(conversation_route))
        .route(
            "/v1/agents/{name}/conversation/redact",
            post(redact_conversation_route),
        )
        .route("/v1/media/{file_name}", get(media_file_route))
        .route("/v1/send", post(send))
        .route("/v1/interact", post(interact_route))
        .route("/v1/outbound", get(outbound_list_route))
        .route("/v1/outbound/{key}/retry", post(outbound_retry_route))
        .route("/v1/outbound/{key}/discard", post(outbound_discard_route))
        .route("/v1/operator/agents/{name}/inbox", get(operator_inbox))
        .route("/v1/operator/inbox", get(operator_reports))
}

/// `GET /v1/outbound`: every sent message whose delivery could not be
/// confirmed, held instead of resent. Operator only.
async fn outbound_list_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<crate::api::UnsettledOutboundResponse>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        Ok(Json(crate::api::UnsettledOutboundResponse {
            unsettled: outbound_history::unsettled(&state.store)
                .iter()
                .map(unsettled_view)
                .collect(),
        }))
    })
    .await
}

/// `POST /v1/outbound/{key}/retry`: send a held message again, as its agent
/// and to its destination, through the ordinary send path. The held record
/// closes with the new receipt only when that send succeeds. Operator only.
async fn outbound_retry_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> Result<Json<SendResponse>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        let store = &state.store;
        let mut skipped = Vec::new();
        let deliveries = outbound_history::retry(store, &key, |held| {
            if non_text_post(&held.body) {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    format!(
                        "{key} was a media, location, poll, checklist or article post; its record keeps only its text. Discard it and ask the agent to post it again."
                    ),
                ));
            }
            let sender = store.agent(&held.agent_id).map_err(ApiError::from_store)?;
            let actor = Actor::Agent(sender.name.clone());
            let body = held_send(held, &actor);
            let plan = plan_send(store, &Principal::Operator, &actor, &body)?;
            let response = send_sync(store, &actor, body, plan, None)?.0;
            skipped = response.skipped;
            Ok(response.deliveries)
        })?;
        Ok(Json(SendResponse {
            deliveries,
            skipped,
        }))
    })
    .await
}

/// The send a retry of `held` makes: the same text to the same destination,
/// as its agent and through the bot it named.
pub(super) fn held_send(held: &outbound_history::Unsettled, actor: &Actor) -> SendRequest {
    SendRequest {
        to: vec![held.destination.clone()],
        text: held.body.clone(),
        sender: Some(actor.as_address()),
        reply_to: held.reply_to.clone(),
        bot: held.bot.clone(),
        ..SendRequest::default()
    }
}

/// `POST /v1/outbound/{key}/discard`: settle a held message without sending
/// it. Operator only.
async fn outbound_discard_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> Result<Json<crate::api::UnsettledOutboundView>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        outbound_history::discard(&state.store, &key)
            .map(|held| Json(unsettled_view(&held)))
            .map_err(ApiError::from_store)
    })
    .await
}

/// Whether a sent body is the text record of a post that was not plain text
/// (`format_internal_send_text`): a retry from it would post its description.
fn non_text_post(body: &str) -> bool {
    [
        "[Attachment (",
        "[Location GPS:",
        "[Venue: ",
        "[Poll: \"",
        "📰 ",
    ]
    .iter()
    .any(|marker| body.contains(marker))
        || body.starts_with("☑️ **")
}

/// How the API shows an outbound message whose delivery is unconfirmed.
pub(crate) fn unsettled_view(
    held: &outbound_history::Unsettled,
) -> crate::api::UnsettledOutboundView {
    crate::api::UnsettledOutboundView {
        key: held.effect_key.clone(),
        agent_id: held.agent_id.to_string(),
        destination: held.destination.clone(),
        bot: held.bot.clone(),
        since_ms: held.since_ms,
        error: held.error.clone(),
        excerpt: held.excerpt(),
    }
}

pub(super) async fn send(
    principal: Principal,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<SendRequest>,
) -> Result<Json<SendResponse>, ApiError> {
    let via_http = via_http_mcp(&headers);
    let turn_context = if let Some(header) = headers.get(crate::mcp::TASK_TURN_CONTEXT_HEADER) {
        let token = header
            .to_str()
            .ok()
            .filter(|token| crate::mcp::is_task_turn_context(token));
        match (&principal, token, via_http) {
            (Principal::Agent { agent_id, .. }, Some(token), false) => {
                Some((agent_id.clone(), token.to_owned()))
            }
            _ => return Err(ApiError::forbidden("invalid task turn context")),
        }
    } else {
        None
    };
    if via_http {
        refuse_host_media_over_http(&body)?;
    }
    // The authenticated principal decides who the message is from; a body
    // `sender` is only a check, and a disagreement is refused with no write.
    // On a machine without the boundary, its own agent route names the sender;
    // that is the one place a body chooses the author, and an inbox is still
    // only ever its own.
    let dispatch_store = Arc::clone(&state.store);
    let dispatch_timer = Arc::clone(&state.timer_backend);
    let response = spawn_store(move || {
        let route_named = match &principal {
            Principal::Agent { name, .. } if state.auth.is_trusted() => body
                .sender
                .as_deref()
                .and_then(|named| Actor::parse(named).ok())
                .filter(|named| named.as_address() != Actor::Agent(name.clone()).as_address()),
            _ => None,
        };
        let actor = match (&principal, route_named) {
            (_, Some(named)) => named,
            (Principal::Agent { agent_id, name }, None) => {
                if let Some(named) = body.sender.as_deref() {
                    Actor::parse(named).and_then(|named| {
                        let matches_self = named.as_address()
                            == Actor::Agent(name.clone()).as_address()
                            || match &named {
                                Actor::Agent(n) => state
                                    .store
                                    .agent_by_name(n)
                                    .is_ok_and(|a| a.id == *agent_id),
                                _ => false,
                            };
                        if matches_self {
                            Ok(())
                        } else {
                            Err(ApiError::forbidden(
                                "a session sends as itself, not as the sender it names",
                            ))
                        }
                    })?;
                }
                Actor::Agent(name.clone())
            }
            (Principal::Operator, _) => {
                let sender = body.sender.as_deref().ok_or_else(|| {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "an operator send needs an explicit sender",
                    )
                })?;
                Actor::parse(sender)?
            }
        };
        let body = SendRequest {
            sender: Some(actor.as_address()),
            ..body
        };
        let mut plan = plan_send(&state.store, &principal, &actor, &body)?;
        if let Some((caller, token)) = turn_context {
            if plan.sender_id.as_ref() != Some(&caller) {
                return Err(ApiError::forbidden(
                    "task turn context belongs to the authenticated author",
                ));
            }
            plan.turn_context = Some(token);
        }
        let key_body = SendRequest {
            sender: plan
                .sender_id
                .as_ref()
                .map(|id| format!("agent:{id}"))
                .or_else(|| body.sender.clone()),
            ..body.clone()
        };
        let key = send_key(&key_body);
        send_sync(&state.store, &actor, body, plan, key)
    })
    .await?;
    if serverless::is_serverless_mode() {
        let mut recipient_names: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for delivery in &response.0.deliveries {
            if delivery.duplicate {
                continue;
            }
            if let Some(agent_name) = delivery.to.strip_prefix("agent:")
                && seen.insert(agent_name.to_string())
            {
                recipient_names.push(agent_name.to_string());
            }
        }
        // Dispatch asynchronously with an `in_flight` guard so cold sandbox provisioning
        // never blocks `/v1/send` (or group fanout) and client disconnects cannot cancel
        // a dispatch mid-flight after recording `pending_remote_turns`.
        for agent_name in recipient_names {
            let store_disp = Arc::clone(&dispatch_store);
            let timer_disp = Arc::clone(&dispatch_timer);
            let call = super::in_flight::carry();
            tokio::spawn(async move {
                let _call = call;
                let store_lookup = Arc::clone(&store_disp);
                let lookup_name = agent_name.clone();
                let Ok(Ok(agent)) =
                    tokio::task::spawn_blocking(move || store_lookup.agent_by_name(&lookup_name))
                        .await
                else {
                    return;
                };
                if let Err(err) = serverless::dispatch_next_mailbox_message_with_timer(
                    store_disp,
                    &agent.id,
                    Some(timer_disp),
                )
                .await
                {
                    eprintln!(
                        "anyagents-serverless: /v1/send dispatch failed for {}: {err}",
                        agent.name
                    );
                }
            });
        }
    }
    Ok(response)
}

/// One send, resolved to the destinations it will write, before any of them is
/// validated or posted. `reply_to` is a mailbox reference the server resolves
/// against this agent's own mailbox, or a destination written plainly.
pub(super) fn plan_send(
    store: &Store,
    principal: &Principal,
    actor: &Actor,
    body: &SendRequest,
) -> Result<SendPlan, ApiError> {
    let authenticated = match principal {
        Principal::Agent { agent_id, .. } => Some(
            store
                .agent(agent_id)
                .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
        ),
        Principal::Operator => None,
    };
    let principal_id = authenticated.as_ref().map(|agent| agent.id.clone());
    let own_sender = match (principal, actor, authenticated.as_ref()) {
        (Principal::Agent { name, .. }, Actor::Agent(actor_name), Some(agent))
            if actor_name == name
                || actor_name == &agent.name
                || store
                    .agent_by_name(actor_name)
                    .is_ok_and(|resolved| resolved.id == agent.id) =>
        {
            Some(agent)
        }
        _ => None,
    };
    let mut plan = if let Some(sender) = own_sender {
        SendPlan::parse_for_sender(store, actor, body, Some(sender.clone()))?
    } else {
        SendPlan::parse(store, actor, body)?
    };
    plan.principal_id = principal_id;
    let has_telegram_target = plan
        .targets
        .iter()
        .any(|t| matches!(t, Target::Telegram { .. }));
    let reply = match crate::reply::resolve(
        principal,
        |id| own_message(store, principal, actor, id),
        body.reply_to.as_deref(),
    ) {
        Ok(dest) => dest,
        Err(_why) if !plan.targets.is_empty() && !has_telegram_target => None,
        Err(why) => return Err(ApiError::new(StatusCode::BAD_REQUEST, why)),
    };
    if let Some(destination) = reply {
        if plan.targets.is_empty() {
            plan.targets = vec![target_of(&destination)];
            plan.reply = destination.reply().map(str::to_string);
        } else {
            // When `to` addresses are explicitly given alongside `reply_to`:
            // - A Telegram target must name the same `chat` as a Telegram reply
            //   destination (so a group reply never goes to a private Telegram
            //   chat or another Telegram chat), and inherits the recorded topic
            //   and message id for that chat when not specified on the target.
            // - Internal `agent:<name>` and `group:<room>` targets in `to` are
            //   honoured as written without wiping out `plan.targets`.
            let mut matched_telegram = false;
            for target in &mut plan.targets {
                if let Target::Telegram {
                    address,
                    chat,
                    thread,
                    reply,
                } = target
                {
                    match &destination {
                        crate::reply::Destination::Telegram {
                            chat: wanted_chat,
                            thread: wanted_thread,
                            reply: wanted_reply,
                        } if chat == wanted_chat => {
                            if thread.is_none() {
                                *thread = wanted_thread.clone();
                                *address = destination.address();
                            }
                            if reply.is_none() {
                                *reply = wanted_reply.clone();
                            }
                            matched_telegram = true;
                        }
                        other => {
                            return Err(ApiError::new(
                                StatusCode::BAD_REQUEST,
                                format!(
                                    "this reply names one place ({}), and the addresses name another ({address})",
                                    other.address()
                                ),
                            ));
                        }
                    }
                }
            }
            if matched_telegram {
                plan.reply = destination.reply().map(str::to_string);
            }
        }
    }
    Ok(plan)
}

/// One message of the mailbox a reply speaks for, by id: the calling agent's
/// own mailbox, or, for the operator, the agent it is sending as.
pub(super) fn own_message(
    store: &Store,
    principal: &Principal,
    actor: &Actor,
    id: &str,
) -> Result<Option<anyagents_core::Message>, String> {
    let agent_id = match (principal, actor) {
        (Principal::Agent { agent_id, .. }, _) => agent_id.clone(),
        (_, Actor::Agent(name)) => store.agent_by_name(name).map_err(|err| err.to_string())?.id,
        (Principal::Operator, Actor::Operator(_)) => return Ok(None),
    };
    store.message(&agent_id, id).map_err(|err| err.to_string())
}

fn target_of(destination: &crate::reply::Destination) -> Target {
    match destination {
        crate::reply::Destination::Telegram {
            chat,
            thread,
            reply,
        } => Target::Telegram {
            address: destination.address(),
            chat: chat.clone(),
            thread: thread.clone(),
            reply: reply.clone(),
        },
        crate::reply::Destination::Agent(name) => Target::Agent(name.clone()),
        crate::reply::Destination::Group(name) => Target::Room(name.clone()),
    }
}

/// One send, resolved to the destinations it will write, before any of them is
/// validated or posted.
#[derive(Debug)]
pub(super) struct SendPlan {
    pub(super) targets: Vec<Target>,
    /// The provider message id a reply answers, when the reply named one.
    pub(super) reply: Option<String>,
    /// Immutable identity admitted at planning; a reused display name is not authority.
    pub(super) sender_id: Option<anyagents_core::AgentId>,
    pub(super) principal_id: Option<anyagents_core::AgentId>,
    pub(super) turn_context: Option<String>,
}

#[derive(Debug)]
pub(super) enum Target {
    Agent(String),
    Room(String),
    /// The operator's inbox: agents write, only the operator reads.
    Operator,
    Telegram {
        address: String,
        chat: String,
        thread: Option<String>,
        reply: Option<String>,
    },
}

impl SendPlan {
    pub(super) fn parse(
        store: &Store,
        actor: &Actor,
        body: &SendRequest,
    ) -> Result<Self, ApiError> {
        let sender = match actor {
            Actor::Agent(name) => Some(
                store
                    .agent_by_name(name)
                    .map_err(|_| ApiError::forbidden("sender agent no longer exists"))?,
            ),
            Actor::Operator(_) => None,
        };
        Self::parse_for_sender(store, actor, body, sender)
    }

    pub(super) fn parse_for_sender(
        store: &Store,
        _actor: &Actor,
        body: &SendRequest,
        sender: Option<Agent>,
    ) -> Result<Self, ApiError> {
        if body.to.is_empty() && body.reply_to.is_none() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "send needs an address",
            ));
        }
        let mut targets = Vec::with_capacity(body.to.len());
        for raw in &body.to {
            match Address::parse(raw)
                .map_err(Error::from)
                .map_err(ApiError::from_store)?
            {
                Address::Agent(name)
                    if matches!(name.as_str(), "all" | "peers" | "siblings" | "team") =>
                {
                    if let Some(sender) = &sender
                        && let Some(ref parent_id) = sender.parent_id
                    {
                        let siblings: Vec<_> = store
                            .children_of(parent_id)
                            .map_err(ApiError::from_store)?
                            .into_iter()
                            .filter(|c| {
                                c.id != sender.id
                                    && c.state != "archived"
                                    && !is_explicitly_archived(store.data_dir(), c)
                                    && can_sender_message_agent(store, sender, c).is_ok()
                            })
                            .collect();
                        if siblings.is_empty() {
                            return Err(ApiError::forbidden(
                                "no reachable sibling sub-agents (`public: true` or shared group); message your parent (`to: [\"parent\"]`) instead",
                            ));
                        }
                        for sib in siblings {
                            targets.push(Target::Agent(sib.name));
                        }
                    } else if name == "siblings" {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "root agents have no parent; use `peers` or `group:owners` to message root peers",
                        ));
                    } else {
                        targets.push(Target::Room("all".to_string()));
                    }
                }
                Address::Group(room)
                    if matches!(room.as_str(), "all" | "peers" | "siblings" | "team") =>
                {
                    if let Some(sender) = &sender
                        && let Some(ref parent_id) = sender.parent_id
                    {
                        let siblings: Vec<_> = store
                            .children_of(parent_id)
                            .map_err(ApiError::from_store)?
                            .into_iter()
                            .filter(|c| {
                                c.id != sender.id
                                    && c.state != "archived"
                                    && !is_explicitly_archived(store.data_dir(), c)
                                    && can_sender_message_agent(store, sender, c).is_ok()
                            })
                            .collect();
                        if siblings.is_empty() {
                            return Err(ApiError::forbidden(
                                "no reachable sibling sub-agents (`public: true` or shared group); message your parent (`to: [\"parent\"]`) instead",
                            ));
                        }
                        for sib in siblings {
                            targets.push(Target::Agent(sib.name));
                        }
                    } else if room == "siblings" {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "root agents have no parent; use `peers` or `group:owners` to message root peers",
                        ));
                    } else {
                        targets.push(Target::Room("all".to_string()));
                    }
                }
                Address::Agent(name) if name == "parent" => {
                    let Some(sender) = &sender else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has a parent",
                        ));
                    };
                    let parent_id = sender.parent_id.as_ref().ok_or_else(|| {
                        ApiError::new(StatusCode::BAD_REQUEST, "this agent has no parent")
                    })?;
                    let parent = store
                        .agent_by_name(parent_id.as_str())
                        .map_err(ApiError::from_store)?;
                    targets.push(Target::Agent(parent.name));
                }
                Address::Agent(name) if name == "children" => {
                    let Some(sender) = &sender else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has children",
                        ));
                    };
                    let children: Vec<_> = store
                        .children_of(&sender.id)
                        .map_err(ApiError::from_store)?
                        .into_iter()
                        .filter(|c| {
                            c.state != "archived" && !is_explicitly_archived(store.data_dir(), c)
                        })
                        .collect();
                    if children.is_empty() {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "this agent has no active children",
                        ));
                    }
                    for child in children {
                        targets.push(Target::Agent(child.name));
                    }
                }
                Address::Group(room) if room == "children" => {
                    let Some(sender) = &sender else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has children",
                        ));
                    };
                    let children: Vec<_> = store
                        .children_of(&sender.id)
                        .map_err(ApiError::from_store)?
                        .into_iter()
                        .filter(|c| {
                            c.state != "archived" && !is_explicitly_archived(store.data_dir(), c)
                        })
                        .collect();
                    if children.is_empty() {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "this agent has no active children",
                        ));
                    }
                    for child in children {
                        targets.push(Target::Agent(child.name));
                    }
                }
                Address::Agent(name) if name == "operator" => {
                    if !targets.iter().any(|t| matches!(t, Target::Operator)) {
                        targets.push(Target::Operator);
                    }
                }
                Address::Agent(name) => targets.push(Target::Agent(name)),
                Address::TelegramChat { chat, thread } => {
                    targets.push(Target::Telegram {
                        address: raw.clone(),
                        chat,
                        thread,
                        reply: None,
                    });
                }
                Address::TelegramUser {
                    user,
                    message: reply,
                } => {
                    targets.push(Target::Telegram {
                        address: raw.clone(),
                        chat: user,
                        thread: None,
                        reply,
                    });
                }
                Address::Group(room) => targets.push(Target::Room(room)),
            }
        }
        Ok(Self {
            targets,
            reply: None,
            sender_id: sender.as_ref().map(|agent| agent.id.clone()),
            principal_id: None,
            turn_context: None,
        })
    }
}

pub(super) fn can_actor_read_agent_private(
    store: &Store,
    principal: &Principal,
    target: &anyagents_core::Agent,
) -> bool {
    match principal {
        Principal::Operator => true,
        Principal::Agent { agent_id, .. } => is_self_or_ancestor(store, agent_id, target),
    }
}

/// Enforces sub-agent communication encapsulation across scopes (`isolated`, `private`, `siblings`, `public`):
/// - An `isolated` sub-agent can only message itself, its direct parent, its own direct children, or a shared group/chat.
/// - Otherwise, any agent can message:
///   1. Itself (`sender.id == recipient.id`)
///   2. Its own direct Child Sub-Agents (`recipient.parent_id == Some(sender.id)`)
///   3. Its own direct Parent Agent (`sender.parent_id == Some(recipient.id)`)
///   4. Any non-isolated descendant: an ancestor that may wake, pause or archive
///      a grandchild may also message it
///   5. A Sibling Sub-Agent under the same Parent when both have `scope: "siblings"` or `"public"` (`allows_sibling_messages()`)
///   6. Any Root Agent (when `sender.allows_sibling_messages()`) or Sub-Agent with `scope: "public"` (`recipient.is_public_subagent()`)
///   7. Any agent with which it shares an internal virtual group (`group:<name>`) or external Telegram chat
pub(super) fn can_actor_message_agent(
    store: &Store,
    actor: &Actor,
    recipient: &anyagents_core::Agent,
) -> Result<(), ApiError> {
    let Actor::Agent(sender_name) = actor else {
        return Ok(());
    };
    let sender = store
        .agent_by_name(sender_name)
        .map_err(|_| ApiError::forbidden("sender agent no longer exists"))?;
    can_sender_message_agent(store, &sender, recipient)
}

pub(super) fn can_sender_message_agent(
    store: &Store,
    sender: &Agent,
    recipient: &Agent,
) -> Result<(), ApiError> {
    can_sender_message_agent_with(
        store,
        sender,
        recipient,
        || store.share_any_group_or_chat(&sender.id, &recipient.id),
        || is_self_or_ancestor(store, &sender.id, recipient),
    )
}

/// [`can_sender_message_agent`] with the group-or-chat share and the
/// ancestry answered by the caller, so a roster can answer them from its rows.
pub(super) fn can_sender_message_agent_with(
    store: &Store,
    sender: &Agent,
    recipient: &Agent,
    share_group_or_chat: impl FnOnce() -> bool,
    sender_is_ancestor: impl FnOnce() -> bool,
) -> Result<(), ApiError> {
    if sender.id == recipient.id
        || recipient.parent_id.as_ref() == Some(&sender.id)
        || sender.parent_id.as_ref() == Some(&recipient.id)
    {
        return Ok(());
    }
    if sender.is_isolated() || recipient.is_isolated() {
        let sender_parent = sender
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok().map(|p| p.name))
            .unwrap_or_else(|| "parent".to_string());
        return Err(ApiError::forbidden(format!(
            "sub-agent `{}` or `{}` is isolated (`scope: isolated`) and only communicates 1-to-1 with its direct parent and children; report to your parent `agent:{sender_parent}` (`to: [\"parent\"]`) instead",
            sender.name, recipient.name
        )));
    }
    if sender_is_ancestor() || share_group_or_chat() || store.has_messaged(recipient, sender) {
        return Ok(());
    }
    let same_parent_siblings = sender.parent_id.is_some()
        && sender.parent_id == recipient.parent_id
        && sender.allows_sibling_messages()
        && recipient.allows_sibling_messages();
    if same_parent_siblings
        || recipient.is_public_subagent()
        || (recipient.parent_id.is_none() && sender.allows_sibling_messages())
    {
        return Ok(());
    }
    if recipient.parent_id.is_none() {
        let sender_parent = sender
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok().map(|p| p.name))
            .unwrap_or_else(|| "parent".to_string());
        return Err(ApiError::forbidden(format!(
            "sub-agent `{}` is private (`public: false`) and cannot message root agent `{}` directly; report to your parent `agent:{sender_parent}` (`to: [\"parent\"]`) instead",
            sender.name, recipient.name
        )));
    }
    let owner_label = recipient
        .parent_id
        .as_ref()
        .and_then(|pid| store.agent(pid).ok().map(|p| p.name))
        .unwrap_or_else(|| "parent".to_string());
    if sender.parent_id.is_some() && sender.parent_id == recipient.parent_id {
        return Err(ApiError::forbidden(format!(
            "cannot message sub-agent `{}` directly: it is your sibling, and siblings message each other only when your parent `agent:{owner_label}` sets `scope: \"siblings\"` on both or puts you in one group. Send it through your parent (`to: [\"parent\"]`), or ask `agent:{owner_label}` for that scope or group",
            recipient.name
        )));
    }
    Err(ApiError::forbidden(format!(
        "cannot message sub-agent `{}` directly: it is private (`scope: \"{}\"`) to its parent `agent:{owner_label}`, and only its ancestors, its children and agents in a shared group reach it. Message `agent:{owner_label}` instead, or ask `agent:{owner_label}` to set `scope: \"public\"` on it or to put you both in a group",
        recipient.name,
        recipient.scope()
    )))
}

/// Every destination is checked before any write, so a refused address never
/// leaves half a send behind.
pub(super) fn check_targets(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    plan: &SendPlan,
) -> Result<(), ApiError> {
    // Authorization uses the immutable identity retained during planning, not
    // a mutable name that might have been assigned to a replacement agent.
    let sender =
        match actor {
            Actor::Agent(_) => Some(
                store
                    .agent(plan.sender_id.as_ref().ok_or_else(|| {
                        ApiError::forbidden("agent send has no admitted identity")
                    })?)
                    .map_err(|_| ApiError::forbidden("sender agent no longer exists"))?,
            ),
            Actor::Operator(_) => None,
        };
    for target in &plan.targets {
        match target {
            Target::Agent(name) => {
                let recipient = store.agent_by_name(name).map_err(ApiError::from_store)?;
                if let Some(sender) = &sender {
                    can_sender_message_agent(store, sender, &recipient)?;
                }
            }
            // Counted without writing, so an empty room fails the whole send
            // instead of silently delivering nothing.
            Target::Room(room) => {
                let clean_room = room.strip_prefix("group:").unwrap_or(room);
                if let Some(sender) = &sender {
                    if clean_room != "all"
                        && clean_room != "peers"
                        && !clean_room.starts_with("telegram:")
                        && !store.is_group_public(clean_room)
                        && let Ok(Some(members)) = store.room_members(room)
                    {
                        let is_member_or_parent = members.iter().any(|m| {
                            if m == &sender.name {
                                return true;
                            }
                            store
                                .agent_by_name(m)
                                .ok()
                                .and_then(|ma| ma.parent_id)
                                .as_ref()
                                == Some(&sender.id)
                        });
                        if !is_member_or_parent {
                            return Err(ApiError::forbidden(format!(
                                "cannot post to private group `{clean_room}` (`public: false`) because `{}` is not a member; join or ask a group member/parent to add you first",
                                sender.name
                            )));
                        }
                    } else if (clean_room == "all" || clean_room == "peers") && !sender.is_public()
                    {
                        return Err(ApiError::forbidden(format!(
                            "private sub-agent `{}` (`public: false`) cannot broadcast to `{clean_room}`; message `parent`, `children`, or a shared `group:<name>` instead",
                            sender.name
                        )));
                    }
                }
                if room_recipients(store, body, room).is_empty() {
                    let (archived, _) = room_skipped(store, body, room);
                    if let Some(archived) = archived.first() {
                        return Err(archived_conflict(&archived.reason));
                    }
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        format!("room {room} has no agent to deliver to"),
                    ));
                }
            }
            Target::Telegram {
                chat,
                thread,
                reply,
                ..
            } => {
                let channel_actor = plan
                    .sender_id
                    .as_ref()
                    .map(|id| Actor::Agent(id.to_string()))
                    .unwrap_or_else(|| actor.clone());
                prepare_telegram(
                    store,
                    &channel_actor,
                    body,
                    chat,
                    thread.as_deref(),
                    reply.as_deref(),
                )?;
            }
            // An invalid or limited report is refused here without a write.
            // The binding decision is the first write of `send_sync`.
            Target::Operator => {
                let composed = anyagents_core::quote::compose(&body.text, body.quote.as_ref());
                let receipt = store
                    .check_operator_report(&operator_sender(actor, plan).0, &composed)
                    .map_err(ApiError::from_store)?;
                if let Some(anyagents_core::OperatorReceipt::Limited { retry_after_ms }) = receipt {
                    return Err(operator_limited(retry_after_ms));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn send_sync(
    store: &Store,
    actor: &Actor,
    body: SendRequest,
    plan: SendPlan,
    key: Option<String>,
) -> Result<Json<SendResponse>, ApiError> {
    // Reauthorize the immutable identities before replay, target checks or effects.
    if let Some(id) = &plan.principal_id {
        store
            .agent(id)
            .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?;
    }
    let resolved_actor = match &plan.sender_id {
        Some(id) => Actor::Agent(
            store
                .agent(id)
                .map_err(|_| ApiError::forbidden("sender agent no longer exists"))?
                .name,
        ),
        None => actor.clone(),
    };
    let actor = &resolved_actor;
    let body = SendRequest {
        // A display name may be reassigned while a journal lock is held. Carry
        // the admitted identity through mailbox storage and trusted context.
        sender: Some(
            plan.sender_id
                .as_ref()
                .map(|id| format!("agent:{id}"))
                .unwrap_or_else(|| actor.as_address()),
        ),
        ..body
    };
    // A model's closing token is not a message and must not wake its target.
    // Keep this after identity reauthorization, including for replayed calls.
    if body.is_turn_closing_sentinel() {
        return Ok(Json(SendResponse {
            deliveries: Vec::new(),
            skipped: Vec::new(),
        }));
    }
    // A call repeated after its answer was lost is the same delivery: the
    // receipt it already produced comes back unchanged.
    if let Some(key) = key.as_deref()
        && let Some(text) = store.send_receipt(key).map_err(ApiError::from_store)?
    {
        let receipt: SendResponse = serde_json::from_str(&text)
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        return Ok(Json(receipt));
    }
    // The request's `reply_to` is a reference, not a Telegram id; a Telegram
    // target carries the id the reference resolved to instead.
    let reply = plan.reply.clone();
    let authored_text = body.text.clone();
    let effective_text = format_internal_send_text(&body);
    let body = SendRequest {
        text: effective_text,
        // A reply the caller resolved to a message id, or a reference the
        // server resolved to one, is what this post answers.
        reply_to: reply.clone().or_else(|| {
            body.reply_to
                .filter(|v| crate::reply::ref_message(v).is_none())
        }),
        ..body
    };
    let internal_body = SendRequest {
        text: authored_text,
        ..body.clone()
    };
    check_targets(store, actor, &body, &plan)?;
    let upload = media_upload(store, &body, &plan)?;
    let sender_agent_id = plan.sender_id.clone();
    let composed_body = anyagents_core::quote::compose(&body.text, body.quote.as_ref());
    // The operator inbox decides its limits in its own write transaction, so
    // it is written before every other target: a refusal there, even one a
    // concurrent send caused after the check above, leaves nothing written.
    let operator_delivery = if plan.targets.iter().any(|t| matches!(t, Target::Operator)) {
        Some(deliver_operator(
            store,
            actor,
            &plan,
            body.reply_to.as_deref(),
            &composed_body,
        )?)
    } else {
        None
    };
    let mut deliveries = Vec::with_capacity(plan.targets.len());
    let mut skipped = Vec::new();
    let mut refused_archived = Vec::new();
    for (target_index, target) in plan.targets.iter().enumerate() {
        let history_key = key.as_ref().map(|key| format!("{key}:{target_index}"));
        match target {
            Target::Agent(name) => {
                // No send unarchives its recipient, however it is addressed:
                // only an explicit `agents(action="wake")` does. An archived
                // recipient would never read it, so it is refused with the
                // reason and never stored unread.
                if let Some(refusal) = archived_refusal(store, name) {
                    refused_archived.push(refusal.reason.clone());
                    skipped.push(refusal);
                    continue;
                }
                let sent = if let Some(sid) = &sender_agent_id {
                    outbound_history::deliver(
                        store,
                        sid,
                        &format!("agent:{name}"),
                        None,
                        body.reply_to.as_deref(),
                        &composed_body,
                        history_key.as_deref(),
                        true,
                        || {
                            Ok(vec![deliver_agent_with_media(
                                store,
                                &internal_body,
                                name,
                                key.as_deref(),
                                upload.as_ref(),
                                plan.turn_context.as_deref().map(|token| (sid, token)),
                            )?])
                        },
                    )?
                } else {
                    vec![deliver_agent_with_media(
                        store,
                        &internal_body,
                        name,
                        key.as_deref(),
                        upload.as_ref(),
                        None,
                    )?]
                };
                deliveries.extend(sent);
            }
            Target::Room(room) => {
                let dest = if room.starts_with("group:") || room.starts_with("telegram:") {
                    room.clone()
                } else {
                    format!("group:{room}")
                };
                let mut all = if let Some(sid) = &sender_agent_id {
                    outbound_history::deliver(
                        store,
                        sid,
                        &dest,
                        None,
                        body.reply_to.as_deref(),
                        &composed_body,
                        history_key.as_deref(),
                        true,
                        || {
                            deliver_room_with_media(
                                store,
                                &internal_body,
                                room,
                                key.as_deref(),
                                upload.as_ref(),
                            )
                        },
                    )?
                } else {
                    deliver_room_with_media(
                        store,
                        &internal_body,
                        room,
                        key.as_deref(),
                        upload.as_ref(),
                    )?
                };
                let (archived, inactive) = room_skipped(store, &body, room);
                let archived = archived
                    .into_iter()
                    .filter(|s| !all.iter().any(|d| d.to == s.to));
                for s in archived {
                    refused_archived.push(s.reason.clone());
                    if !skipped.iter().any(|existing| existing.to == s.to) {
                        skipped.push(s);
                    }
                }
                deliveries.append(&mut all);
                for s in inactive {
                    if !skipped
                        .iter()
                        .any(|existing: &crate::api::SkippedDelivery| existing.to == s.to)
                    {
                        skipped.push(s);
                    }
                }
            }
            Target::Operator => deliveries.extend(operator_delivery.clone()),
            Target::Telegram {
                address,
                chat,
                thread,
                reply: address_reply,
            } => {
                let channel_actor = sender_agent_id
                    .as_ref()
                    .map(|id| Actor::Agent(id.to_string()))
                    .unwrap_or_else(|| actor.clone());
                let send = || {
                    deliver_telegram(
                        store,
                        &channel_actor,
                        &body,
                        upload.as_ref(),
                        address,
                        chat,
                        thread.as_deref(),
                        address_reply.as_deref().or(reply.as_deref()),
                    )
                    .map(|delivery| vec![delivery])
                };
                let sent = if let Some(sid) = &sender_agent_id {
                    outbound_history::deliver(
                        store,
                        sid,
                        address,
                        body.bot.as_deref(),
                        body.reply_to.as_deref(),
                        &composed_body,
                        history_key.as_deref(),
                        false,
                        send,
                    )?
                } else {
                    send()?
                };
                deliveries.extend(sent);
            }
        }
    }
    if deliveries.is_empty() && !refused_archived.is_empty() {
        return Err(archived_conflict(&refused_archived.join("; ")));
    }
    let response = SendResponse {
        deliveries,
        skipped,
    };
    if let Some(key) = key.as_deref() {
        let text = serde_json::to_string(&response)
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        store
            .record_send_receipt(key, &text)
            .map_err(ApiError::from_store)?;
    }
    Ok(Json(response))
}

/// The bytes of a send's local media file: read once, before any target is
/// written, with the caller's own access and never the server's (ADR 0010 D3).
/// A URL or a Telegram `file_id` reads nothing; internal recipients receive
/// daemon-owned private copies rather than a sender-private path.
pub(super) fn media_upload(
    store: &Store,
    body: &SendRequest,
    plan: &SendPlan,
) -> Result<Option<crate::telegram::MediaUpload>, ApiError> {
    let Some(media) = &body.media else {
        return Ok(None);
    };
    let source = media.source.trim();
    if !crate::telegram::is_local_media(source) {
        return Ok(None);
    }
    // The authenticated agent, never a sender the operator names.
    let authority = match &plan.principal_id {
        Some(id) => crate::agent_file::FileAuthority::of_agent_id(store, id),
        None => crate::agent_file::FileAuthority::Own,
    };
    let path = Path::new(source);
    let bytes = authority
        .read(path, crate::agent_file::MEDIA_UPLOAD_LIMIT)
        .map_err(|why| ApiError::forbidden(format!("media not sent: {why}")))?;
    Ok(Some(crate::telegram::MediaUpload::new(path, bytes)))
}

/// The idempotency key of one call. The same call repeated after a lost
/// answer keeps the key its tool call was given; two identical calls without
/// one are two messages, so a key never comes from the text. Receipts live in
/// one store and must not depend on process entropy: retries after restart or
/// through the other server socket must retain their mailbox source key.
pub(super) fn send_key(body: &SendRequest) -> Option<String> {
    let idempotency = body.idempotency_key.as_deref()?.trim();
    if idempotency.is_empty() {
        return None;
    }
    let sender = body.sender.as_deref().unwrap_or("");
    let reply_to = body.reply_to.as_deref().unwrap_or("");
    let to_joined = body.to.join("\x1f");
    let mut bytes = Vec::new();
    for part in [
        sender,
        idempotency,
        to_joined.as_str(),
        reply_to,
        body.text.as_str(),
    ] {
        bytes.extend_from_slice(part.as_bytes());
        bytes.push(0);
    }
    // A named bot is part of the send: the same key through another bot is
    // another send, resolved and authorized on its own. A send that names no
    // bot keeps the key it always had.
    if let Some(bot) = body.bot.as_deref().map(str::trim).filter(|b| !b.is_empty()) {
        bytes.extend_from_slice(b"bot\0");
        bytes.extend_from_slice(bot.to_ascii_lowercase().as_bytes());
        bytes.push(0);
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in &bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Some(format!("send:{hash:016x}"))
}

/// Who writes a message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Actor {
    Agent(String),
    /// The operator, as an explicit principal: `operator`, `telegram:user:<id>`
    /// or `telegram:chat:<id>`.
    Operator(String),
}

impl Actor {
    pub(super) fn parse(raw: &str) -> Result<Self, ApiError> {
        if raw.trim() == "operator" {
            return Ok(Self::Operator("operator".to_string()));
        }
        match anyagents_core::Address::parse(raw) {
            Ok(anyagents_core::Address::Agent(name)) => Ok(Self::Agent(name)),
            Ok(
                address @ (anyagents_core::Address::TelegramUser { .. }
                | anyagents_core::Address::TelegramChat { .. }),
            ) => Ok(Self::Operator(address.canonical())),
            Ok(other) => Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("{other} cannot write a message"),
            )),
            Err(err) => Err(ApiError::new(StatusCode::BAD_REQUEST, err.to_string())),
        }
    }

    /// The sender stored with the message.
    pub(super) fn as_address(&self) -> String {
        match self {
            Self::Agent(name) => format!("agent:{name}"),
            Self::Operator(raw) => raw.clone(),
        }
    }
}
