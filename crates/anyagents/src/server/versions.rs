//! Operator version inventory and safe, lease-scoped drain requests.
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth::Principal;
use crate::sandbox_providers::image_version::digest;
use crate::sandbox_upgrade as upgrade;

use super::{ApiError, AppState, require_operator, spawn_store};

const POLICY_NS: &str = "config";
const POLICY_PATH: &str = "coordinator-update-policy.json";

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Policy {
    Auto,
    #[default]
    Manual,
    Pinned,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PolicySetting {
    auto_update_policy: Policy,
}

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/admin/api/versions", get(versions))
        .route("/admin/api/versions/policy", patch(set_policy))
        .route(
            "/admin/api/agents/{name}/drain-upgrade",
            post(drain_upgrade),
        )
}

fn env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

async fn versions(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        let store = &state.store;
        let settings = store.blob_store().get_deployment_blob(POLICY_NS, POLICY_PATH).map_err(ApiError::from_store)?;
        let policy = if let Some(saved) = settings {
            serde_json::from_slice::<PolicySetting>(&saved.content)
                .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid stored update policy"))?.auto_update_policy
        } else {
            env("ANYAGENTS_AUTO_UPDATE_POLICY").and_then(|raw| serde_json::from_value(json!(raw)).ok()).unwrap_or_default()
        };
        let configured = upgrade::configured_image_spec(store);
        let desired = upgrade::desired_image_ref(configured.as_ref(), None, None);
        let now = anyagents_core::now_ms();
        let mut counts = [0, 0, 0, 0];
        let mut agents = Vec::new();
        for agent in store.sandbox_inventory(upgrade::DRAIN).map_err(ApiError::from_store)? {
            let binding = agent.binding.as_ref();
            let active = binding.filter(|b| b.active_lease_key().is_some());
            let open = agent.has_open_turn
                || upgrade::heartbeat_has_live_children(
                    agent.inflight_children, agent.heartbeat_at_ms, now,
                );
            let desired_for_agent = upgrade::desired_image_ref(
                configured.as_ref(),
                binding.and_then(|b| b.image.as_deref()),
                binding.and_then(|b| b.template.as_deref()),
            );
            let version = active.map(|b| upgrade::version_from_spec(&b.spec_json)).unwrap_or_default();
            let requested = active.is_some() && agent.drain_requested;
            let status = match active {
                None => 3,
                Some(b) if b.provider != "sylphx" || (!requested && version.matches(&desired_for_agent)) => 0,
                Some(_) if open || requested => 2,
                Some(_) => 1,
            };
            counts[status] += 1;
            let upgrade_status = ["up_to_date", "outdated_idle", "outdated_draining", "cold_uses_latest"][status];
            agents.push(json!({
                "id": agent.id, "name": agent.name, "state": agent.state, "harness": agent.harness, "has_open_turn": open,
                "running_image_digest": version.digest(), "desired_image_digest": digest(&desired_for_agent),
                "harness_version": version.harness_version, "tpl_base_version": version.tpl_base_version,
                "sbx_guest_version": version.sbx_guest_version,
                "upgrade_status": upgrade_status,
                "drain_requested": requested,
            }));
        }
        Ok(Json(json!({
            "coordinator": {
                "version": env!("CARGO_PKG_VERSION"), "git_sha": option_env!("ANYAGENTS_GIT_SHA").filter(|v| !v.is_empty()).or(Some(env!("ANYAGENTS_SOURCE_SHA")).filter(|v| *v != "unknown")),
                "image_digest": env("ANYAGENTS_COORDINATOR_IMAGE_DIGEST").as_deref().and_then(digest),
                "latest_available": env("ANYAGENTS_COORDINATOR_LATEST_IMAGE"), "auto_update_policy": policy,
            },
            "runtime": {
                "desired_image_digest": digest(&desired), "desired_sandbox_image": desired,
                "desired_harness_versions": env("ANYAGENTS_SANDBOX_HARNESS_VERSIONS").and_then(|raw| serde_json::from_str::<std::collections::BTreeMap<String,String>>(&raw).ok()).unwrap_or_default(),
                "tpl_base_version": env("ANYAGENTS_SANDBOX_TPL_BASE_VERSION"), "sbx_guest_version": env("ANYAGENTS_SANDBOX_GUEST_VERSION"),
            },
            "summary": {"total": agents.len(), "up_to_date": counts[0], "outdated": counts[1]+counts[2], "draining": counts[2], "cold": counts[3]},
            "agents": agents,
        })))
    }).await
}

async fn drain_upgrade(
    principal: Principal,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let action = upgrade::request_drain(&state.store, &agent.id).map_err(|e| {
            eprintln!(
                "anyagents-sandbox: operator drain failed for {}: {e}",
                agent.id
            );
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "sandbox upgrade could not be reconciled; retry",
            )
        })?;
        eprintln!(
            "anyagents-sandbox: operator drain for {}: {action:?}",
            agent.id
        );
        Ok(Json(json!({"agent": agent.name, "action": action})))
    })
    .await
}

async fn set_policy(
    principal: Principal,
    State(state): State<AppState>,
    Json(setting): Json<PolicySetting>,
) -> Result<Json<PolicySetting>, ApiError> {
    require_operator(&principal)?;
    spawn_store(move || {
        let bytes = serde_json::to_vec(&setting)
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        state
            .store
            .blob_store()
            .put_deployment_blob(POLICY_NS, POLICY_PATH, &bytes, false)
            .map_err(ApiError::from_store)?;
        eprintln!(
            "anyagents-version: operator set coordinator policy to {}",
            String::from_utf8_lossy(&bytes)
        );
        Ok(Json(setting))
    })
    .await
}
