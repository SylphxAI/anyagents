//! Channels, Telegram webhook ingress, and group membership HTTP routes.

use std::sync::Arc;

use anyagents_core::Store;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::{GroupActionRequest, GroupView, GroupsResponse};
use crate::auth::Principal;

use super::messaging::{Actor, can_actor_message_agent};
use super::serverless;
use super::types::{
    ApiError, AppState, is_self_or_ancestor, require_admin_or_operator, spawn_store,
};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/channels", get(channels_route).post(add_channel_route))
        .route(
            "/v1/groups",
            get(list_groups_route).post(group_action_route),
        )
}

pub(super) fn webhook_router() -> Router<AppState> {
    Router::new().route("/v1/webhooks/telegram/{channel_id}", post(telegram_webhook))
}

/// `POST /v1/webhooks/telegram/{channel_id}`: authenticated channel intake.
async fn telegram_webhook(
    State(state): State<AppState>,
    AxumPath(channel_id): AxumPath<String>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let secret = headers
        .get("x-telegram-bot-api-secret-token")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let store = Arc::clone(&state.store);
    let channel_for_dispatch = channel_id.clone();
    let target_agents = spawn_store(move || {
        crate::telegram::receive_webhook(
            &store,
            store.data_dir(),
            &channel_id,
            secret.as_deref(),
            &payload,
        )
        .map_err(|err| match err {
            crate::telegram::WebhookError::Unauthorized => {
                ApiError::new(StatusCode::UNAUTHORIZED, "invalid webhook secret token")
            }
            crate::telegram::WebhookError::NotFound(why) => {
                ApiError::new(StatusCode::NOT_FOUND, why)
            }
            crate::telegram::WebhookError::BadRequest(why) => {
                ApiError::new(StatusCode::BAD_REQUEST, why)
            }
            crate::telegram::WebhookError::Store(why) => {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why)
            }
        })?;
        let agents = if serverless::is_serverless_mode() {
            store
                .list_channels()
                .ok()
                .and_then(|channels| {
                    channels
                        .into_iter()
                        .find(|c| c.id.as_str() == channel_for_dispatch)
                        .map(|c| c.agents)
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        Ok(agents)
    })
    .await?;
    if serverless::is_serverless_mode() {
        for agent_id in target_agents {
            if let Err(err) = serverless::dispatch_next_mailbox_message_if_serverless(
                Arc::clone(&state.store),
                &agent_id,
            )
            .await
            {
                eprintln!(
                    "anyagents-serverless: telegram webhook dispatch failed for {agent_id}: {err}"
                );
            }
        }
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub(super) async fn channels_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "channels.list", "channels")?;
        let channels: Vec<_> = state
            .store
            .list_channels()
            .map_err(ApiError::from_store)?
            .into_iter()
            .map(|c| {
                let mem = anyagents_channel_telegram::Memory::try_from_state(&c.state)
                    .map_err(ApiError::from_store)?;
                let bot_username = c
                    .identity
                    .clone()
                    .or_else(|| {
                        mem.bot
                            .as_deref()
                            .and_then(|b| b.split_once(':').map(|(_, u)| u.to_string()))
                    })
                    .map(|u| {
                        let clean = u.trim().trim_start_matches('@');
                        format!("@{clean}")
                    });
                let agents: Vec<String> = c
                    .agents
                    .iter()
                    .filter_map(|id| state.store.agent(id).ok().map(|a| a.name))
                    .collect();
                Ok(serde_json::json!({
                    "id": c.id.to_string(),
                    "kind": c.kind,
                    "bot": bot_username,
                    "agents": agents,
                    "pending_managed_bots": mem.pending_managed_bots,
                }))
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok(Json(serde_json::json!({"channels": channels})))
    })
    .await
}

fn default_channel_kind() -> String {
    "telegram".to_string()
}

#[derive(Deserialize)]
struct ChannelRequest {
    #[serde(default = "default_channel_kind")]
    kind: String,
    agent: String,
    #[serde(default)]
    token: Option<String>,
    #[serde(default, alias = "managed_bot_id", alias = "user_id")]
    bot_id: Option<i64>,
    #[serde(
        default,
        alias = "username",
        alias = "suggested_username",
        alias = "bot"
    )]
    bot_username: Option<String>,
    #[serde(default)]
    manager: Option<String>,
    #[serde(default)]
    replace_token: Option<bool>,
}

async fn add_channel_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<ChannelRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        if body.kind != "telegram" {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid channel kind",
            ));
        }
        if let Some(ref tok) = body.token {
            let trimmed = tok.trim();
            if trimmed.is_empty() || trimmed.chars().any(|c| c.is_whitespace() || c == '/') {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid channel kind or token",
                ));
            }
        }
        let target_clean = body
            .agent
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(body.agent.trim());
        let agent = state
            .store
            .agent_by_name(target_clean)
            .map_err(ApiError::from_store)?;
        let caller_agent = match &principal {
            Principal::Agent { agent_id, .. } => Some(
                state
                    .store
                    .agent(agent_id)
                    .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
            ),
            Principal::Operator => None,
        };
        let authorized_by_lineage = match &principal {
            Principal::Agent { agent_id, .. } => {
                is_self_or_ancestor(&state.store, agent_id, &agent)
            }
            Principal::Operator => true,
        };
        if !authorized_by_lineage {
            require_admin_or_operator(&state.store, &principal, "channels.add", &body.agent)?;
        }
        let result = crate::telegram::request_or_bind_telegram_channel(
            &state.store,
            state.store.data_dir(),
            caller_agent.as_ref(),
            &agent,
            body.token.as_deref(),
            body.bot_id,
            body.bot_username.as_deref(),
            body.manager.as_deref(),
            body.replace_token.unwrap_or(false),
        )
        .map_err(|err| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("could not bind or request channel: {err}"),
            )
        })?;
        Ok(Json(result))
    })
    .await
}

fn build_group_roster(store: &Store, members: &[String]) -> Vec<crate::api::GroupMemberView> {
    let mut out = Vec::with_capacity(members.len());
    for name in members {
        if let Ok(agent) = store.agent_by_name(name) {
            let description = agent.public_description();
            out.push(crate::api::GroupMemberView {
                id: agent.id.to_string(),
                name: agent.name,
                role: agent.role,
                harness: agent.harness,
                model: agent.model,
                state: agent.state,
                description,
            });
        }
    }
    out
}

async fn list_groups_route(
    _principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<GroupsResponse>, ApiError> {
    spawn_store(move || {
        let rows = state.store.list_groups().map_err(ApiError::from_store)?;
        let groups = rows
            .into_iter()
            .map(|(name, members)| {
                let public = state.store.is_group_public(&name);
                let roster = build_group_roster(&state.store, &members);
                Ok(GroupView {
                    owner_agent_id: state
                        .store
                        .group(&name)
                        .map_err(ApiError::from_store)?
                        .and_then(|g| g.owner_agent_id)
                        .map(|id| id.to_string()),
                    name,
                    public,
                    members,
                    roster,
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok(Json(GroupsResponse { groups }))
    })
    .await
}

pub(super) fn group_actor(principal: &Principal) -> anyagents_core::GroupActor {
    match principal {
        Principal::Operator => anyagents_core::GroupActor::Operator,
        Principal::Agent { agent_id, .. } => anyagents_core::GroupActor::Agent(agent_id.clone()),
    }
}

async fn group_action_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<GroupActionRequest>,
) -> Result<Json<GroupView>, ApiError> {
    let default_agent = match &principal {
        Principal::Agent { name, .. } => Some(name.clone()),
        Principal::Operator => None,
    };
    spawn_store(move || {
        let group = body
            .group
            .trim()
            .strip_prefix("group:")
            .unwrap_or(body.group.trim())
            .to_string();
        if group.is_empty() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "group name is empty",
            ));
        }
        let action = match body.action.trim() {
            "join" => anyagents_core::GroupAction::Join,
            "leave" => anyagents_core::GroupAction::Leave,
            "members" => anyagents_core::GroupAction::Members,
            other => {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    format!("unknown group action: {other}"),
                ));
            }
        };
        let actor = group_actor(&principal);
        let mut targets = Vec::new();
        if action != anyagents_core::GroupAction::Members {
            let who_raw = body
                .agent
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .or(default_agent)
                .ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "group action needs an agent name")
                })?;
            for who in who_raw
                .split(',')
                .map(str::trim)
                .map(|s| s.strip_prefix("agent:").unwrap_or(s))
                .filter(|s| !s.is_empty())
            {
                let agent = if who.eq_ignore_ascii_case("self")
                    && let Principal::Agent { ref agent_id, .. } = principal
                {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                } else {
                    state
                        .store
                        .agent_by_name(who)
                        .map_err(ApiError::from_store)?
                };
                if action == anyagents_core::GroupAction::Join
                    && let Principal::Agent { ref name, .. } = principal
                {
                    can_actor_message_agent(&state.store, &Actor::Agent(name.clone()), &agent)?;
                }
                targets.push(agent.id);
            }
        }
        state
            .store
            .group_action_as(&actor, &group, action, &targets, body.public)
            .map_err(ApiError::from_store)?;
        let public = state.store.is_group_public(&group);
        let members = state
            .store
            .room_members(&group)
            .map_err(ApiError::from_store)?
            .unwrap_or_default();
        let roster = build_group_roster(&state.store, &members);
        Ok(Json(GroupView {
            owner_agent_id: state
                .store
                .group(&group)
                .map_err(ApiError::from_store)?
                .and_then(|g| g.owner_agent_id)
                .map(|id| id.to_string()),
            name: group,
            public,
            members,
            roster,
        }))
    })
    .await
}
