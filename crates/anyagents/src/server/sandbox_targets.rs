//! Operator-managed sandbox billing credentials and references. Keep this API
//! separate from model profiles, messaging roles, and guest deployment secrets.
use anyagents_core::sandbox_targets::{SandboxCredential, SandboxCredentialSource};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use super::types::{ApiError, AppState, require_operator, require_operator_audited, spawn_store};
use crate::auth::Principal;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/sandbox-credentials/{name}",
            put(put_credential).delete(delete_credential),
        )
        .route("/v1/sandbox-profiles/{name}", put(put_profile))
        .route("/v1/groups/{name}/sandbox-target", put(put_group_target))
        .route(
            "/v1/agents/{name}/sandbox-target",
            get(get_agent_target).put(put_agent_target),
        )
}

async fn put_credential(
    State(state): State<AppState>,
    principal: Principal,
    Path(name): Path<String>,
    body: Result<Json<SandboxCredential>, axum::extract::rejection::JsonRejection>,
) -> Result<StatusCode, ApiError> {
    require_operator(&principal)?;
    // JSON errors can quote the offending input. Never return parser detail on
    // a credential-writing endpoint.
    let Json(credential) = body.map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid sandbox credential payload",
        )
    })?;
    crate::sandbox_exec::validate_credential(&credential)
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err))?;
    spawn_store(move || {
        require_operator_audited(&state.store, &principal, "sandbox_credentials.put", &name)?;
        state
            .store
            .put_sandbox_credential(&name, &credential)
            .map_err(ApiError::from_store)?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
}

async fn delete_credential(
    State(state): State<AppState>,
    principal: Principal,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        require_operator_audited(
            &state.store,
            &principal,
            "sandbox_credentials.delete",
            &name,
        )?;
        state
            .store
            .delete_sandbox_credential(&name)
            .map_err(ApiError::from_store)?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileTarget {
    credential: String,
}

async fn put_profile(
    State(state): State<AppState>,
    principal: Principal,
    Path(name): Path<String>,
    Json(body): Json<ProfileTarget>,
) -> Result<StatusCode, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        require_operator_audited(&state.store, &principal, "sandbox_profiles.put", &name)?;
        state
            .store
            .set_sandbox_profile(&name, &body.credential)
            .map_err(ApiError::from_store)?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupTarget {
    profile: Option<String>,
}

async fn put_group_target(
    State(state): State<AppState>,
    principal: Principal,
    Path(name): Path<String>,
    Json(body): Json<GroupTarget>,
) -> Result<StatusCode, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        require_operator_audited(&state.store, &principal, "sandbox_targets.set_group", &name)?;
        state
            .store
            .set_group_sandbox_profile(&name, body.profile.as_deref())
            .map_err(ApiError::from_store)?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentTarget {
    profile: Option<String>,
    credential: Option<String>,
}

async fn put_agent_target(
    State(state): State<AppState>,
    principal: Principal,
    Path(name): Path<String>,
    Json(body): Json<AgentTarget>,
) -> Result<StatusCode, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        require_operator_audited(&state.store, &principal, "sandbox_targets.set_agent", &name)?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let _guard = state
            .store
            .lock_agent_lifecycle(&agent.id)
            .map_err(ApiError::from_store)?;
        state
            .store
            .set_agent_sandbox_target(
                &agent.id,
                body.profile.as_deref(),
                body.credential.as_deref(),
            )
            .map_err(ApiError::from_store)?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
}

async fn get_agent_target(
    State(state): State<AppState>,
    principal: Principal,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let reference = state
            .store
            .sandbox_credential_ref(&agent.id)
            .map_err(ApiError::from_store)?;
        Ok(Json(match reference {
            Some(r) => json!({"credential": r.name, "source": r.source}),
            None => json!({"source": SandboxCredentialSource::Default}),
        }))
    })
    .await
}
