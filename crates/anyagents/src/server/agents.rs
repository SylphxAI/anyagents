//! Agent CRUD, spawning, and runtime configuration HTTP routes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyagents_core::{Agent, Incoming, ReceiveOutcome, Store};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::{AgentView, AgentsResponse, CreateAgentRequest, Delivery};
use crate::auth::Principal;
use crate::create::{self, CreateRequest};
use crate::operator;

use super::agents_lifecycle::{
    archive_agent_route, delete_agent_route, pause_agent_route, verdict_route, wake_agent_route,
};
use super::agents_roster::{
    ListAgentsQuery, agent_view, compute_caller_relationship, list_agents_sync_for_principal,
};
use super::channels_groups::group_actor;
use super::messaging::{Actor, can_actor_message_agent, can_actor_read_agent_private};
use super::types::{
    ApiError, AppState, is_self_or_ancestor, owner_workdir, refuse_host_placement_over_http,
    require_operator_audited, require_remote_callback, spawn_store, via_http_mcp,
};
use super::{outbound_history, serverless};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/agents", get(list_agents).post(create_agent_route))
        .route(
            "/v1/agents/{name}",
            get(get_agent)
                .patch(super::secrets::patch_agent_route)
                .delete(delete_agent_route),
        )
        .route("/v1/agents/{name}/rename", post(rename_agent_route))
        .route("/v1/agents/{name}/role", post(role_agent_route))
        .route("/v1/agents/{name}/transfer", post(transfer_agent_route))
        .route("/v1/agents/{name}/model", post(model_route))
        .route("/v1/agents/{name}/harness", post(harness_route))
        .route(
            "/v1/agents/{name}/runtime",
            post(super::runtime::runtime_route),
        )
        .route("/v1/agents/{name}/session/reset", post(reset_session_route))
        .route(
            "/v1/agents/{name}/description",
            get(get_description_route).post(set_description_route),
        )
        .route(
            "/v1/agents/{name}/instructions",
            get(get_description_route).post(set_description_route),
        )
        .route("/v1/agents/{name}/archive", post(archive_agent_route))
        .route("/v1/agents/{name}/pause", post(pause_agent_route))
        .route("/v1/agents/{name}/wake", post(wake_agent_route))
        .route("/v1/agents/{name}/verdict", post(verdict_route))
}

// Socket requests carry secret values, never host filesystem paths.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelRequest {
    model: Option<String>,
    harness: Option<String>,
    base_url: Option<String>,
    key: Option<String>,
    profile: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HarnessRequest {
    pub(super) harness: String,
    #[serde(default)]
    pub(super) runtime: Option<String>,
    #[serde(default)]
    pub(super) budget_cap_micros: Option<u64>,
    #[serde(default)]
    pub(super) model: Option<String>,
    #[serde(default)]
    pub(super) profile: Option<String>,
}

pub(super) async fn reset_session_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let _lifecycle = state
            .store
            .try_lock_agent_lifecycle(&found.id)
            .map_err(ApiError::from_store)?;
        let found = state.store.agent(&found.id).map_err(ApiError::from_store)?;
        match &principal {
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && is_self_or_ancestor(&state.store, agent_id, &found) => {
            }
            _ => {
                require_operator_audited(
                    &state.store,
                    &principal,
                    "agents.reset_session",
                    &found.name,
                )?;
            }
        }
        create::reset_agent_session_locked(&state.store, &found)
            .map(Json)
            .map_err(ApiError::from_store)
    })
    .await
}

pub(super) async fn harness_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    headers: axum::http::HeaderMap,
    Json(body): Json<HarnessRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let via_http = via_http_mcp(&headers);
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let actor = match &principal {
            Principal::Agent {
                agent_id,
                name: caller_name,
            } if agent_id != &found.id && is_self_or_ancestor(&state.store, agent_id, &found) => {
                format!("agent:{caller_name}")
            }
            _ => require_operator_audited(&state.store, &principal, "agents.harness", &found.name)?,
        };
        require_remote_callback(
            state.store.data_dir(),
            &body.harness,
            body.runtime.as_deref(),
        )?;
        refuse_host_placement_over_http(
            via_http,
            state.store.data_dir(),
            &body.harness,
            body.runtime.as_deref(),
        )?;
        let out = create::set_agent_harness_with_options(
            &state.store,
            state.store.data_dir(),
            &actor,
            &found.name,
            &body.harness,
            body.runtime.as_deref(),
            body.model.as_deref(),
            body.profile.as_deref(),
            body.budget_cap_micros.map(|cap| (cap > 0).then_some(cap)),
        )
        .map_err(ApiError::from_store)?;
        // The agent's profile binding may have moved.
        state.profiles.invalidate(&state.store);
        Ok(Json(out))
    })
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferAgentRequest {
    parent: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

async fn transfer_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<TransferAgentRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                agent_id != &found.id && is_self_or_ancestor(&state.store, agent_id, &found)
            }
            Principal::Operator => false,
        };
        let old_parent_name = found
            .parent_id
            .as_ref()
            .and_then(|pid| state.store.agent(pid).ok())
            .map(|p| p.name);
        let new_parent = match body
            .parent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            None | Some("none" | "root" | "null") => None,
            Some(raw) => {
                let clean = raw.strip_prefix("agent:").unwrap_or(raw);
                let target = if clean.eq_ignore_ascii_case("self")
                    && let Principal::Agent { ref agent_id, .. } = principal
                {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                } else {
                    state
                        .store
                        .agent_by_name(clean)
                        .map_err(ApiError::from_store)?
                };
                if target.state == "archived" {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        format!("cannot transfer to archived parent `{}`", target.name),
                    ));
                }
                Some(target)
            }
        };
        let actor = match &principal {
            Principal::Agent {
                agent_id,
                name: caller_name,
            } if is_ancestor
                && new_parent
                    .as_ref()
                    .is_some_and(|np| is_self_or_ancestor(&state.store, agent_id, np)) =>
            {
                format!("agent:{caller_name}")
            }
            _ => {
                require_operator_audited(&state.store, &principal, "agents.transfer", &found.name)?
            }
        };
        let updated = state
            .store
            .set_agent_parent(&found.id, new_parent.as_ref().map(|p| &p.id))
            .map_err(ApiError::from_store)?;

        let mut env_pairs = updated.launch_env.clone();
        env_pairs.retain(|(k, _)| k != "ANYAGENTS_PARENT_NAME");
        if let Some(ref np) = new_parent {
            env_pairs.push(("ANYAGENTS_PARENT_NAME".to_string(), np.name.clone()));
        }
        state.store.set_agent_launch_env(&updated.id, &env_pairs).map_err(ApiError::from_store)?;

        let config = anyagents_core::agent_config_dir(state.store.data_dir(), &updated.id);
        let work_dir = updated
            .workdir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                state
                    .store
                    .data_dir()
                    .join("agents")
                    .join(updated.id.as_str())
                    .join("work")
            });
        let started_ms = state
            .health
            .get(&updated.id)
            .and_then(|h| h.process_started_at_ms);
        create::reconcile_agent_runtime_config_for_live(
            &state.store,
            state.store.data_dir(),
            &updated,
            &config,
            &work_dir,
            started_ms,
        ).map_err(ApiError::from_store)?;

        let reason_suffix = body
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|r| format!(" Reason: {r}"))
            .unwrap_or_default();
        if let Some(ref np) = new_parent {
            let notice = format!(
                "[Custody Transferred] Your parent agent is now `agent:{}` (previously `{}`). Report subsequent progress and completion via `send(to: [\"parent\"], ...)` to `agent:{}`.{reason_suffix}",
                np.name,
                old_parent_name.as_deref().unwrap_or("root"),
                np.name
            );
            let mut incoming = Incoming::new(notice);
            incoming.sender = Some(format!("agent:{actor}"));
            state.store.receive(&updated.id, incoming).map_err(ApiError::from_store)?;
        }

        let mut view =
            serde_json::to_value(agent_view(&state.store, &state.health, &updated)?).unwrap();
        if let Some(obj) = view.as_object_mut() {
            obj.insert(
                "previous_parent".to_string(),
                serde_json::json!(old_parent_name),
            );
            obj.insert(
                "new_parent".to_string(),
                serde_json::json!(new_parent.as_ref().map(|p| p.name.as_str())),
            );
            obj.insert("transferred_by".to_string(), serde_json::json!(actor));
        }
        Ok(Json(view))
    })
    .await
}

async fn model_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<ModelRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let actor = require_operator_audited(&state.store, &principal, "agents.model", &name)?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if body.profile.is_some() && (body.key.is_some() || body.base_url.is_some()) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "profile cannot be combined with a key or base URL",
            ));
        }
        if body.key.is_none() && body.base_url.is_none() {
            let has_harness = body
                .harness
                .as_deref()
                .map(str::trim)
                .is_some_and(|s| !s.is_empty());
            let has_model = body
                .model
                .as_deref()
                .map(str::trim)
                .is_some_and(|s| !s.is_empty());
            let has_profile = body
                .profile
                .as_deref()
                .map(str::trim)
                .is_some_and(|s| !s.is_empty());
            if !has_harness && !has_model && !has_profile {
                return Err(ApiError::new(StatusCode::BAD_REQUEST, "model is required"));
            }
            let out = create::configure_agent_runtime(
                &state.store,
                state.store.data_dir(),
                &actor,
                &agent.name,
                body.harness.as_deref(),
                body.model.as_deref(),
                body.profile.as_deref(),
            )
            .map_err(ApiError::from_store)?;
            // The agent's profile binding may have moved.
            state.profiles.invalidate(&state.store);
            return Ok(Json(out));
        }
        let model_str = body
            .model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "model is required"))?;
        let key = body.key.as_deref().unwrap_or("").trim();
        if key.is_empty() || key.contains(char::is_whitespace) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "a nonempty single-line key is required",
            ));
        }
        let base_url = body
            .base_url
            .as_deref()
            .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "base URL is required"))?;
        crate::gateway::store_key(state.store.data_dir(), &agent, base_url, model_str, key)
            .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "could not bind provider key"))?;
        Ok(Json(
            serde_json::json!({"agent": agent.name, "model": model_str}),
        ))
    })
    .await
}

#[derive(Deserialize)]
struct RenameAgentRequest {
    #[serde(alias = "new_name", alias = "to")]
    name: String,
}

async fn rename_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<RenameAgentRequest>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let old = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id != old.id && is_self_or_ancestor(&state.store, agent_id, &old)
            }
            Principal::Operator => true,
        };
        if !is_ancestor {
            require_operator_audited(&state.store, &principal, "agents.rename", &old.name)?;
        }
        let updated = state
            .store
            .rename_agent(&old.id, &body.name)
            .map_err(ApiError::from_store)?;
        let data_dir = state.store.data_dir();
        if old.name != updated.name {
            let browsers_dir = data_dir.join("browsers");
            let old_browser = browsers_dir.join(&old.name);
            let new_browser = browsers_dir.join(&updated.name);
            if old_browser.is_dir()
                && !new_browser.exists()
                && std::fs::rename(&old_browser, &new_browser).is_ok()
            {
                #[cfg(unix)]
                {
                    let _ = std::os::unix::fs::symlink(&updated.name, &old_browser);
                }
            }
        }
        let cfg = anyagents_core::agent_config_dir(data_dir, &updated.id);
        let work_dir = updated
            .workdir
            .as_deref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| anyagents_core::agent_work_dir(data_dir, &updated.id));
        let started_ms = state
            .health
            .get(&updated.id)
            .and_then(|h| h.process_started_at_ms);
        create::reconcile_agent_runtime_config_for_live(
            &state.store,
            data_dir,
            &updated,
            &cfg,
            &work_dir,
            started_ms,
        )
        .map_err(ApiError::from_store)?;
        crate::telegram::sync_agent_telegram_bot_name(&state.store, data_dir, &updated);
        Ok(Json(agent_view(&state.store, &state.health, &updated)?))
    })
    .await
}

#[derive(Deserialize, Default)]
struct RoleAgentRequest {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    clear: Option<bool>,
}

async fn role_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<RoleAgentRequest>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id != found.id && is_self_or_ancestor(&state.store, agent_id, &found)
            }
            Principal::Operator => true,
        };
        if !is_ancestor {
            require_operator_audited(&state.store, &principal, "agents.role", &found.name)?;
        }
        let data_dir = state.store.data_dir();
        let parent_wd = found
            .parent_id
            .as_ref()
            .and_then(|pid| state.store.agent(pid).ok())
            .and_then(|p| p.workdir);
        let new_role = if body.clear.unwrap_or(false) {
            None
        } else if let Some(raw) = body
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(
                create::normalize_or_validate_agent_role(
                    raw,
                    found.workdir.as_deref().map(Path::new),
                    parent_wd.as_deref().map(Path::new),
                    data_dir,
                )
                .map_err(ApiError::from_store)?,
            )
        } else {
            None
        };
        let updated = state
            .store
            .set_agent_role(&found.id, new_role.as_deref())
            .map_err(ApiError::from_store)?;
        let cfg = anyagents_core::agent_config_dir(data_dir, &updated.id);
        let work_dir = updated
            .workdir
            .as_deref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| anyagents_core::agent_work_dir(data_dir, &updated.id));
        let started_ms = state
            .health
            .get(&updated.id)
            .and_then(|h| h.process_started_at_ms);
        create::reconcile_agent_runtime_config_for_live(
            &state.store,
            data_dir,
            &updated,
            &cfg,
            &work_dir,
            started_ms,
        )
        .map_err(ApiError::from_store)?;
        crate::telegram::sync_agent_telegram_bot_name(&state.store, data_dir, &updated);
        Ok(Json(agent_view(&state.store, &state.health, &updated)?))
    })
    .await
}

#[derive(Deserialize, Default)]
pub(super) struct DescriptionRequest {
    #[serde(default)]
    pub(super) description: Option<String>,
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    pub(super) instructions: Option<String>,
    #[serde(default)]
    pub(super) role: Option<String>,
    #[serde(default)]
    pub(super) scope: Option<String>,
    #[serde(default)]
    pub(super) lifecycle: Option<String>,
    #[serde(default, alias = "allow_external")]
    pub(super) public: Option<bool>,
    #[serde(default)]
    pub(super) clear: Option<bool>,
}

pub(super) async fn get_description_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let rec = if name.eq_ignore_ascii_case("self") {
            match &principal {
                Principal::Agent { agent_id, .. } => {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                }
                Principal::Operator => {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "`self` requires an agent caller",
                    ));
                }
            }
        } else {
            state
                .store
                .agent_by_name(&name)
                .map_err(ApiError::from_store)?
        };
        let can_read_private = can_actor_read_agent_private(&state.store, &principal, &rec);
        let aliases = state.store.agent_aliases(&rec.id).unwrap_or_default();
        let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &rec.id);
        let system_prompt = if can_read_private {
            std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).ok()
        } else {
            None
        };
        let mut resp = serde_json::json!({
            "id": rec.id.as_str(),
            "name": rec.name,
            "role": rec.role,
            "scope": rec.scope(),
            "lifecycle": rec.lifecycle(),
            "public": rec.is_public(),
            "harness": rec.harness,
            "model": rec.model,
            "aliases": aliases,
            "description": rec.public_description(),
        });
        if can_read_private {
            resp["instructions"] = serde_json::json!(rec.instructions);
            resp["system_prompt"] = serde_json::json!(system_prompt);
        }
        Ok(Json(resp))
    })
    .await
}

pub(super) async fn set_description_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<DescriptionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        if name.eq_ignore_ascii_case("all")
            && body.description.is_none()
            && body.instructions.is_none()
            && body.role.is_none()
            && body.scope.is_none()
            && body.lifecycle.is_none()
            && body.public.is_none()
            && !body.clear.unwrap_or(false)
        {
            require_operator_audited(&state.store, &principal, "agents.sync_instructions", "all")?;
            create::sync_all_agent_instructions(&state.store, state.store.data_dir())
                .map_err(ApiError::from_store)?;
            return Ok(Json(serde_json::json!({
                "synced": "all",
            })));
        }
        let target = if name.eq_ignore_ascii_case("self") {
            match &principal {
                Principal::Agent { agent_id, .. } => {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                }
                Principal::Operator => {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "`self` requires an agent caller",
                    ));
                }
            }
        } else {
            state
                .store
                .agent_by_name(&name)
                .map_err(ApiError::from_store)?
        };
        let is_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id != target.id && is_self_or_ancestor(&state.store, agent_id, &target)
            }
            Principal::Operator => true,
        };
        if !is_ancestor {
            require_operator_audited(
                &state.store,
                &principal,
                "agents.set_description",
                &target.name,
            )?;
        }
        let actor = match &principal {
            Principal::Operator => "operator".to_string(),
            Principal::Agent { name, .. } => format!("agent:{name}"),
        };
        let mut rec = target;
        if let Some(ref scope_val) = body.scope {
            if !is_ancestor {
                require_operator_audited(&state.store, &principal, "agents.scope", &rec.name)?;
            }
            rec = state
                .store
                .set_agent_scope_as(&rec.id, scope_val, &actor)
                .map_err(ApiError::from_store)?;
        } else if let Some(pub_val) = body.public {
            if !is_ancestor {
                require_operator_audited(&state.store, &principal, "agents.public", &rec.name)?;
            }
            rec = state
                .store
                .set_agent_public_as(&rec.id, pub_val, &actor)
                .map_err(ApiError::from_store)?;
        }
        if let Some(ref lifecycle_val) = body.lifecycle {
            if !is_ancestor {
                require_operator_audited(&state.store, &principal, "agents.lifecycle", &rec.name)?;
            }
            rec = state
                .store
                .set_agent_lifecycle_as(&rec.id, lifecycle_val, &actor)
                .map_err(ApiError::from_store)?;
        }
        if let Some(ref role_str) = body.role {
            if !is_ancestor {
                require_operator_audited(&state.store, &principal, "agents.role", &rec.name)?;
            }
            let new_role = if role_str.trim().is_empty() {
                None
            } else {
                let parent_wd = rec
                    .parent_id
                    .as_ref()
                    .and_then(|pid| state.store.agent(pid).ok())
                    .and_then(|p| p.workdir);
                Some(
                    create::normalize_or_validate_agent_role(
                        role_str,
                        rec.workdir.as_deref().map(Path::new),
                        parent_wd.as_deref().map(Path::new),
                        state.store.data_dir(),
                    )
                    .map_err(ApiError::from_store)?,
                )
            };
            rec = state
                .store
                .set_agent_role(&rec.id, new_role.as_deref())
                .map_err(ApiError::from_store)?;
            crate::telegram::sync_agent_telegram_bot_name(
                &state.store,
                state.store.data_dir(),
                &rec,
            );
        }
        if body.clear.unwrap_or(false) {
            rec = state
                .store
                .set_agent_description(&rec.id, None)
                .map_err(ApiError::from_store)?;
            rec = state
                .store
                .set_agent_instructions(&rec.id, None)
                .map_err(ApiError::from_store)?;
        } else {
            if let Some(ref desc) = body.description {
                let trimmed = desc.trim();
                let clean_opt = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                };
                rec = state
                    .store
                    .set_agent_description(&rec.id, clean_opt)
                    .map_err(ApiError::from_store)?;
                if body.instructions.is_none() && rec.instructions.is_none() {
                    rec = state
                        .store
                        .set_agent_instructions(&rec.id, clean_opt)
                        .map_err(ApiError::from_store)?;
                }
                crate::telegram::sync_agent_telegram_bot_name(
                    &state.store,
                    state.store.data_dir(),
                    &rec,
                );
            }
            if let Some(ref inst) = body.instructions {
                let trimmed = inst.trim();
                rec = state
                    .store
                    .set_agent_instructions(
                        &rec.id,
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed)
                        },
                    )
                    .map_err(ApiError::from_store)?;
            }
        }
        let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &rec.id);
        let started_ms = state
            .health
            .get(&rec.id)
            .and_then(|h| h.process_started_at_ms);
        create::sync_agent_description_files_for_live(&state.store, &rec, &cfg, started_ms)
            .map_err(ApiError::from_store)?;
        let effective_instructions =
            create::effective_agent_instructions(Some(&state.store), state.store.data_dir(), &rec);
        let aliases = state.store.agent_aliases(&rec.id).unwrap_or_default();
        Ok(Json(serde_json::json!({
            "id": rec.id.as_str(),
            "name": rec.name,
            "role": rec.role,
            "scope": rec.scope(),
            "lifecycle": rec.lifecycle(),
            "public": rec.is_public(),
            "aliases": aliases,
            "description": rec.public_description(),
            "instructions": effective_instructions,
            "system_prompt_synced": true,
        })))
    })
    .await
}

pub(super) async fn get_agent(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let caller = match &principal {
            Principal::Operator => None,
            Principal::Agent { agent_id, .. } => Some(
                state
                    .store
                    .agent(agent_id)
                    .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
            ),
        };
        let name = if name == "self" {
            caller.as_ref().map(|a| a.id.as_str()).unwrap_or(&name)
        } else {
            &name
        };
        let agent = state
            .store
            .agent_by_name(name)
            .map_err(ApiError::from_store)?;
        if let Some(caller) = &caller
            && caller.is_isolated()
            && !is_self_or_ancestor(&state.store, &caller.id, &agent)
            && caller.parent_id.as_ref() != Some(&agent.id)
            && !state.store.share_any_group_or_chat(&caller.id, &agent.id)
        {
            return Err(ApiError::forbidden("agent is outside the caller's scope"));
        }
        let mut view = agent_view(&state.store, &state.health, &agent)?;
        if let Some(caller) = &caller {
            view.relationship = Some(compute_caller_relationship(&state.store, caller, &agent));
            view.can_message = Some(
                can_actor_message_agent(&state.store, &Actor::Agent(caller.name.clone()), &agent)
                    .is_ok(),
            );
            if !can_actor_read_agent_private(&state.store, &principal, &agent) {
                view.instructions = None;
                view.secrets.clear();
            }
        }
        Ok(Json(view))
    })
    .await
}

pub(super) async fn list_agents(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<ListAgentsQuery>,
) -> Result<Json<AgentsResponse>, ApiError> {
    let operator = principal.is_operator();
    spawn_store(move || {
        let mut listed = list_agents_sync_for_principal(&state, &principal, &query)?;
        if !operator {
            listed.0.gateway = None;
        }
        Ok(listed)
    })
    .await
}

pub(super) async fn create_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(mut body): Json<CreateAgentRequest>,
) -> Result<(StatusCode, Json<AgentView>), ApiError> {
    let via_http = via_http_mcp(&headers);
    let state_for_wake = state.clone();
    let caller_agent = match &principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, name } => Some((agent_id.clone(), name.clone())),
    };
    let created = spawn_store(move || {
        let requested_groups: Vec<String> = body
            .group
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .map(|s| s.strip_prefix("group:").unwrap_or(s))
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let requested_group_names: Vec<&str> =
            requested_groups.iter().map(String::as_str).collect();
        let group_actor = group_actor(&principal);
        state
            .store
            .check_group_joins(&group_actor, &requested_group_names)
            .map_err(ApiError::from_store)?;
        let parent_agent = match (&caller_agent, body.parent.as_deref()) {
            (_, Some("root" | "")) => {
                require_operator_audited(
                    &state.store,
                    &principal,
                    "agents.create_root",
                    &body.name,
                )?;
                None
            }
            (_, Some(parent_ref)) => Some(
                state
                    .store
                    .agent_by_name(parent_ref)
                    .map_err(ApiError::from_store)?,
            ),
            (Some((caller_id, _)), None) => {
                Some(state.store.agent(caller_id).map_err(ApiError::from_store)?)
            }
            (None, None) => None,
        };
        create::apply_archetype_defaults(
            Some(&state.store),
            state.store.data_dir(),
            parent_agent.as_ref(),
            &mut body,
        )
        .map_err(ApiError::from_store)?;
        if let Some(scope) = body.scope.as_deref() {
            anyagents_core::parse_agent_scope(scope).map_err(ApiError::from_store)?;
        }
        if let Some(lifecycle) = body.lifecycle.as_deref() {
            anyagents_core::parse_agent_lifecycle(lifecycle).map_err(ApiError::from_store)?;
        }
        let parent_id = parent_agent.as_ref().map(|p| p.id.clone());
        let granted_browser_profiles = super::s3_media_profiles::validate_create_browser_profiles(
            &state.store,
            &principal,
            parent_id.as_ref(),
            body.browser_profiles.as_deref(),
        )?;
        let requested_secrets = super::secrets::validate_create_agent_secrets(
            &state.store,
            &principal,
            parent_id.as_ref(),
            body.secrets.as_deref(),
        )?;
        let seat = crate::sandbox::RootSeatRequest {
            user: body.seat_user,
            uid: body.seat_uid,
        };
        let operator_root = matches!(&principal, Principal::Operator) && parent_id.is_none();
        if seat.explicit() && !operator_root {
            return Err(ApiError::from_store(
                anyagents_core::Error::PermissionDenied(
                    "seat admission requires an operator-created root".into(),
                ),
            ));
        }
        let (harness, model, profile) = create::resolve_subagent_defaults(
            state.store.data_dir(),
            parent_agent.as_ref(),
            body.harness.as_deref().unwrap_or("claude-code"),
            body.model,
            body.profile,
        );
        require_remote_callback(state.store.data_dir(), &harness, body.runtime.as_deref())?;
        refuse_host_placement_over_http(
            via_http,
            state.store.data_dir(),
            &harness,
            body.runtime.as_deref(),
        )?;
        let request = CreateRequest {
            name: body.name,
            role: Some(
                body.role
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(create::DEFAULT_AGENT_ROLE)
                    .to_string(),
            ),
            harness,
            runtime: body.runtime,
            budget_cap_micros: body.budget_cap_micros,
            model,
            profile,
            repo: body.repo,
            branch: body.branch,
            base: body.base,
            workdir: body.workdir.map(PathBuf::from),
            workdir_default: owner_workdir(),
            description: body.description,
            instructions: body.instructions,
            parent_id,
        };
        let agent = if operator_root {
            create::create_operator_root(&state.store, state.store.data_dir(), request, seat)
        } else {
            create::create(&state.store, state.store.data_dir(), request)
        }
        .map_err(ApiError::from_store)?;
        super::s3_media_profiles::apply_create_browser_profiles(
            &state.store,
            &principal,
            &agent.id,
            &granted_browser_profiles,
        )?;
        let actor = match &principal {
            Principal::Operator => "operator".to_string(),
            Principal::Agent { name, .. } => format!("agent:{name}"),
        };
        super::secrets::apply_create_agent_secrets(
            &state.store,
            &agent.id,
            &requested_secrets,
            &actor,
        )?;
        let finish = SpawnFinish {
            workdir: agent.workdir.clone(),
            actor,
            caller: caller_agent.as_ref().map(|(id, _)| id.to_string()),
            scope: body.scope,
            public: body.public,
            lifecycle: body.lifecycle,
            groups: requested_groups,
            prompt: body
                .prompt
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        };
        // The agent is admitted. What the spawn still does is recorded beside
        // it first, so a server stopped part way finishes it at its next start
        // (`finish_interrupted_spawns`) instead of leaving an agent without its
        // prompt and a parent waiting for it.
        let marker = spawn_finish_marker(state.store.data_dir(), &agent.id);
        if let Err(error) = record_spawn_finish(&marker, &finish) {
            eprintln!(
                "warning: spawn of {} not recorded for recovery ({error}); finishing it now",
                agent.name
            );
        }
        let finished = finish_spawn(&state.store, state.store.data_dir(), agent, &finish);
        // Done, or refused: either way nothing is left for a restart to redo.
        let _ = std::fs::remove_file(&marker);
        let (agent, has_prompt) = finished?;
        if has_prompt {
            create::poke_agent_wake_socket_mail(state.store.data_dir(), &agent.id);
        } else {
            create::poke_agent_wake_socket(state.store.data_dir(), &agent.id);
        }
        let view = agent_view(&state.store, &state.health, &agent)?;
        Ok((agent.id, has_prompt, view))
    })
    .await?;
    if created.1 && serverless::is_serverless_mode() {
        let store_wake = Arc::clone(&state_for_wake.store);
        let timer_wake = Arc::clone(&state_for_wake.timer_backend);
        let aid_wake = created.0;
        let call = super::in_flight::carry();
        tokio::spawn(async move {
            let _call = call;
            if let Err(err) = serverless::dispatch_next_mailbox_message_with_timer(
                store_wake,
                &aid_wake,
                Some(timer_wake),
            )
            .await
            {
                eprintln!(
                    "anyagents-serverless: initial prompt dispatch failed for {aid_wake}: {err}"
                );
            }
        });
    }
    Ok((StatusCode::CREATED, Json(created.2)))
}

/// What a spawn does once its agent is admitted: scope, lifecycle, groups,
/// capability and the first prompt. Recorded in the agent's config directory
/// while it runs ([`SPAWN_FINISH_FILE`]); every step is idempotent, so it can
/// be run again after a stop.
#[derive(Clone, Debug, PartialEq, serde::Serialize, Deserialize)]
pub(super) struct SpawnFinish {
    /// Allocated workdir, for recovery of rows missing the allocation.
    #[serde(default)]
    pub(super) workdir: Option<String>,
    /// Who spawned: `operator` or `agent:<name>`, for the audit.
    pub(super) actor: String,
    /// The spawning agent's id: the prompt's sender and a group member.
    pub(super) caller: Option<String>,
    pub(super) scope: Option<String>,
    pub(super) public: Option<bool>,
    pub(super) lifecycle: Option<String>,
    pub(super) groups: Vec<String>,
    pub(super) prompt: Option<String>,
}

/// The record of a spawn still finishing, beside the agent's config.
const SPAWN_FINISH_FILE: &str = ".anyagents-spawn-finish.json";

pub(super) fn spawn_finish_marker(data_dir: &Path, agent_id: &anyagents_core::AgentId) -> PathBuf {
    anyagents_core::agent_config_dir(data_dir, agent_id).join(SPAWN_FINISH_FILE)
}

pub(super) fn record_spawn_finish(marker: &Path, finish: &SpawnFinish) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(finish).map_err(std::io::Error::other)?;
    anyagents_harness::atomic_write_nonempty_file(marker, &bytes, 0o600)
}

/// Run what a spawn does after admission. The prompt carries a source key of
/// its own, so a second run delivers it once.
fn finish_spawn(
    store: &Store,
    data_dir: &Path,
    mut agent: Agent,
    finish: &SpawnFinish,
) -> Result<(Agent, bool), ApiError> {
    let caller = finish
        .caller
        .as_deref()
        .map(anyagents_core::AgentId::parse)
        .transpose()
        .map_err(ApiError::from_store)?;
    if let Some(workdir) = finish.workdir.as_deref() {
        agent = store
            .set_agent_workdir_if_missing(&agent.id, workdir)
            .map_err(ApiError::from_store)?;
    }
    let actor = finish.actor.as_str();
    let mut scope_changed = false;
    if let Some(ref scope_val) = finish.scope {
        agent = store
            .set_agent_scope_as(&agent.id, scope_val, actor)
            .map_err(ApiError::from_store)?;
        scope_changed = true;
    } else if let Some(public) = finish.public {
        agent = store
            .set_agent_public_as(&agent.id, public, actor)
            .map_err(ApiError::from_store)?;
        scope_changed = true;
    }
    if let Some(ref lifecycle_val) = finish.lifecycle {
        agent = store
            .set_agent_lifecycle_as(&agent.id, lifecycle_val, actor)
            .map_err(ApiError::from_store)?;
    }
    if scope_changed {
        let cfg = anyagents_core::agent_config_dir(data_dir, &agent.id);
        create::sync_agent_description_files(store, &agent, &cfg).map_err(ApiError::from_store)?;
    }
    if !finish.groups.is_empty() {
        let names: Vec<&str> = finish.groups.iter().map(String::as_str).collect();
        let mut targets = vec![agent.id.clone()];
        let group_actor = match &caller {
            Some(caller_id) => {
                targets.push(caller_id.clone());
                anyagents_core::GroupActor::Agent(caller_id.clone())
            }
            None => anyagents_core::GroupActor::Operator,
        };
        store
            .join_groups_as(&group_actor, &names, &targets)
            .map_err(ApiError::from_store)?;
    }
    if !anyagents_core::agent_config_dir(data_dir, &agent.id)
        .join("mcp-capability")
        .is_file()
    {
        operator::mint(store, &agent.id).map_err(ApiError::from_store)?;
    }
    let Some(prompt) = finish.prompt.as_deref() else {
        return Ok((agent, false));
    };
    let sender = caller
        .as_ref()
        .map(|id| format!("agent:{id}"))
        .unwrap_or_else(|| "operator".to_string());
    let key = format!("spawn-prompt:{}", agent.id);
    let enqueue = || {
        let message = store
            .receive(
                &agent.id,
                Incoming {
                    text: prompt.to_string(),
                    sender: Some(sender),
                    source_time_ms: None,
                    source_key: Some(key.clone()),
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: None,
                },
            )
            .map_err(ApiError::from_store)?;
        let (message, duplicate) = match message {
            ReceiveOutcome::New(message) => (message, false),
            ReceiveOutcome::Duplicate(message) => (message, true),
        };
        Ok(vec![Delivery {
            to: format!("agent:{}", agent.name),
            id: message.id.to_string(),
            seq: message.seq,
            duplicate,
        }])
    };
    if let Some(ref caller_id) = caller {
        outbound_history::deliver(
            store,
            caller_id,
            &format!("agent:{}", agent.name),
            None,
            None,
            prompt,
            Some(&key),
            true,
            enqueue,
        )?;
    } else {
        enqueue()?;
    }
    Ok((agent, true))
}

/// Finish the spawns a stopped server left part way: each live agent whose
/// config still holds [`SPAWN_FINISH_FILE`] gets the rest of its spawn (most
/// often its first prompt). Run once at start, before any call is answered;
/// a spawn that cannot be finished keeps its record for the next start.
pub(super) fn finish_interrupted_spawns(store: &Store, data_dir: &Path) -> usize {
    let Ok(agents) = store.list_live_agents() else {
        return 0;
    };
    let mut finished = 0;
    for agent in agents {
        let marker = spawn_finish_marker(data_dir, &agent.id);
        let Ok(bytes) = std::fs::read(&marker) else {
            continue;
        };
        let name = agent.name.clone();
        let id = agent.id.clone();
        match serde_json::from_slice::<SpawnFinish>(&bytes)
            .map_err(|error| ApiError::new(StatusCode::BAD_REQUEST, error.to_string()))
            .and_then(|finish| finish_spawn(store, data_dir, agent, &finish))
        {
            Ok((_, has_prompt)) => {
                let _ = std::fs::remove_file(&marker);
                eprintln!("anyagents: finished the interrupted spawn of {name}");
                if has_prompt {
                    create::poke_agent_wake_socket_mail(data_dir, &id);
                } else {
                    create::poke_agent_wake_socket(data_dir, &id);
                }
                finished += 1;
            }
            // Kept: the next start tries again (every step is idempotent),
            // so a passing failure (a busy store) never loses the spawn.
            Err(error) => eprintln!(
                "anyagents: the interrupted spawn of {name} could not be finished (kept for the \
                 next start): {}",
                error.message
            ),
        }
    }
    finished
}
