//! Management of one receiving agent's external ingress rules.

use agent_ingress::{
    BlockWriteContext, IngressGuardToolRequest as Request, IngressGuardToolResponse as Receipt,
    SuggestedChannelAction, ToolError,
};
use anyagents_core::{AgentId, IngressBlocks, Store, now_ms};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::Serialize;

use crate::auth::Principal;
use crate::telegram::ingress_guard::{self, Authority, LeaveResult};

use super::{ApiError, AppState, require_operator, spawn_store};

const BODY_LIMIT: usize = 16 * 1024;

pub(super) fn agent_router() -> Router<AppState> {
    Router::new().route(
        "/v1/ingress-blocks",
        post(own_action).layer(DefaultBodyLimit::max(BODY_LIMIT)),
    )
}

pub(super) fn admin_router() -> Router<AppState> {
    Router::new().route(
        "/admin/api/agents/{id}/ingress-blocks",
        get(admin_list)
            .post(admin_block)
            .delete(admin_unblock)
            .layer(DefaultBodyLimit::max(BODY_LIMIT)),
    )
}

#[derive(Serialize)]
struct Response {
    #[serde(flatten)]
    receipt: Receipt,
    #[serde(skip_serializing_if = "Option::is_none")]
    leave_chat: Option<LeaveResult>,
}

async fn own_action(
    principal: Principal,
    State(state): State<AppState>,
    Json(request): Json<Request>,
) -> Result<Json<Response>, ApiError> {
    let Principal::Agent { agent_id, .. } = principal else {
        return Err(ApiError::forbidden("an agent session is required"));
    };
    let actor = format!("agent:{agent_id}");
    spawn_store(move || perform(&state.store, &agent_id, &actor, request).map(Json)).await
}

async fn admin_list(
    principal: Principal,
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<Response>, ApiError> {
    admin(principal, id, state, Request::ListBlocks).await
}

async fn admin_block(
    principal: Principal,
    Path(id): Path<String>,
    State(state): State<AppState>,
    Json(request): Json<Request>,
) -> Result<Json<Response>, ApiError> {
    require_operator(&principal)?;
    if !matches!(request, Request::BlockSender { .. }) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "POST requires op=block_sender",
        ));
    }
    admin(principal, id, state, request).await
}

async fn admin_unblock(
    principal: Principal,
    Path(id): Path<String>,
    State(state): State<AppState>,
    Json(request): Json<Request>,
) -> Result<Json<Response>, ApiError> {
    require_operator(&principal)?;
    if !matches!(request, Request::UnblockSender { .. }) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "DELETE requires op=unblock_sender",
        ));
    }
    admin(principal, id, state, request).await
}

async fn admin(
    principal: Principal,
    id: String,
    state: AppState,
    request: Request,
) -> Result<Json<Response>, ApiError> {
    require_operator(&principal)?;
    let id = AgentId::parse(&id).map_err(ApiError::from_store)?;
    spawn_store(move || perform(&state.store, &id, "operator", request).map(Json)).await
}

fn perform(
    store: &Store,
    agent: &AgentId,
    actor: &str,
    mut request: Request,
) -> Result<Response, ApiError> {
    store.agent(agent).map_err(ApiError::from_store)?;
    let authority;
    let context = if let Request::BlockSender { target, reason, .. } = &mut request {
        if let Some(reason) = reason {
            *reason = anyagents_core::masking::mask_known_secrets(reason).into_owned();
            if reason.contains('\0') {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "reason must not contain NUL",
                ));
            }
        }
        authority = Some(Authority::load().map_err(ApiError::from_store)?);
        let bindings = ingress_guard::bindings(store, agent).map_err(ApiError::from_store)?;
        let (relationship, chat_type) =
            ingress_guard::resolve_target(target, authority.as_ref().unwrap(), &bindings)
                .ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "unresolved Telegram block target")
                })?;
        Some((target.clone(), relationship, chat_type))
    } else {
        authority = None;
        None
    };
    let context = context
        .as_ref()
        .map(|(target, relationship, chat_type)| BlockWriteContext {
            target,
            relationship: *relationship,
            chat_type: *chat_type,
            rule_id: anyagents_core::MessageId::generate().to_string(),
            blocked_by: actor.into(),
            now_ms: now_ms(),
        });
    let scoped = IngressBlocks::new(store, agent);
    let receipt = futures::executor::block_on(request.execute(&scoped, context)).map_err(
        |error| match error {
            ToolError::Policy(_) => ApiError::new(StatusCode::BAD_REQUEST, error.to_string()),
            ToolError::Store(_) => {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
            }
        },
    )?;
    let leave_chat = match (&receipt, &authority) {
        (
            Receipt::Blocked {
                rule,
                suggested_channel_action:
                    Some(SuggestedChannelAction::LeaveChat { channel, chat_id }),
            },
            Some(authority),
        ) if channel == "telegram" => ingress_guard::numeric_id(chat_id).map(|id| {
            ingress_guard::leave_group(store, agent, authority, id, rule.expires_at_ms.is_some())
        }),
        _ => None,
    };
    Ok(Response {
        receipt,
        leave_chat,
    })
}

#[cfg(test)]
#[path = "ingress_blocks_tests.rs"]
mod tests;
