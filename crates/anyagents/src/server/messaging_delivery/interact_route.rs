//! Authenticated entry point for the shared interaction route.

use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};

use crate::api::{InteractRequest, InteractResponse};
use crate::auth::Principal;
use crate::server::types::{
    ApiError, AppState, require_remote_media_source, spawn_store, via_http_mcp,
};

pub(in crate::server) async fn interact_route(
    principal: Principal,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<InteractRequest>,
) -> Result<Json<InteractResponse>, ApiError> {
    if via_http_mcp(&headers)
        && body.action.trim().eq_ignore_ascii_case("set_chat_photo")
        && let Some(media) = &body.media
    {
        require_remote_media_source(&media.source)?;
    }
    let actor_name = match &principal {
        Principal::Agent { name, .. } => name.clone(),
        Principal::Operator => {
            let sender = body.sender.as_deref().ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "an operator interact call needs an explicit sender",
                )
            })?;
            sender
                .trim()
                .strip_prefix("agent:")
                .unwrap_or(sender.trim())
                .to_string()
        }
    };
    let store = Arc::clone(&state.store);
    spawn_store(move || super::interact_sync(&store, &principal, &actor_name, body)).await
}
