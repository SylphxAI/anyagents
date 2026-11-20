//! Named encrypted deployment secrets HTTP API, parent-delegated capability
//! attenuation (`child ⊆ parent`), sandbox env resolution, and output masking.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use anyagents_core::{Agent, AgentId, DeploymentSecretMetadata, SettleRemoteTurn, Store};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Json, Router};

use crate::api::{
    AgentSecretsResponse, AgentView, DeleteSecretResponse, PatchAgentRequest, SecretMetadataView,
    SecretsListResponse, SetAgentSecretsRequest, UpsertSecretRequest,
};
use crate::auth::Principal;
use crate::create;

use super::agents_roster::agent_view;
use super::messaging::can_actor_read_agent_private;
use super::types::{
    ApiError, AppState, is_self_or_ancestor, require_admin_or_operator, spawn_store,
};

#[cfg(test)]
#[path = "secrets_tests.rs"]
mod tests;

/// Sub-router for deployment secrets (`/v1/secrets*`) and agent secret grants
/// (`/v1/agents/{name}/secrets`). `PATCH /v1/agents/{name}` is wired on
/// `agents::router()` to avoid overlapping path merges.
pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/secrets", get(list_secrets_route))
        .route(
            "/v1/secrets/{name}",
            put(upsert_secret_route).delete(delete_secret_route),
        )
        .route(
            "/v1/agents/{name}/secrets",
            get(get_agent_secrets_route).put(set_agent_secrets_route),
        )
}

fn is_admin_agent(agent: &Agent) -> bool {
    agent.is_admin && agent.parent_id.is_none()
}

fn resolve_target_agent(
    store: &Store,
    principal: &Principal,
    name: &str,
) -> Result<Agent, ApiError> {
    let clean = name.strip_prefix("agent:").unwrap_or(name);
    if clean.eq_ignore_ascii_case("self") {
        match principal {
            Principal::Agent { agent_id, .. } => {
                store.agent(agent_id).map_err(ApiError::from_store)
            }
            Principal::Operator => Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "`self` requires an agent caller",
            )),
        }
    } else {
        store.agent_by_name(clean).map_err(ApiError::from_store)
    }
}

fn normalize_requested_secrets(raw: &[String]) -> Result<Vec<String>, ApiError> {
    let mut unique = BTreeSet::new();
    for item in raw {
        let trimmed = item.trim();
        anyagents_core::validate_secret_name(trimmed).map_err(ApiError::from_store)?;
        unique.insert(trimmed.to_string());
    }
    Ok(unique.into_iter().collect())
}

fn metadata_to_view(
    meta: DeploymentSecretMetadata,
    agent_names: &HashMap<String, String>,
) -> SecretMetadataView {
    let granted_agents = meta
        .granted_agent_ids
        .iter()
        .map(|id| agent_names.get(id).cloned().unwrap_or_else(|| id.clone()))
        .collect();
    SecretMetadataView {
        name: meta.name,
        env_keys: meta.env_keys,
        updated_at_ms: meta.updated_at_ms,
        granted_agent_ids: meta.granted_agent_ids,
        granted_agents,
    }
}

fn load_agent_name_map(store: &Store) -> HashMap<String, String> {
    store
        .list_agents()
        .unwrap_or_default()
        .into_iter()
        .map(|a| (a.id.to_string(), a.name))
        .collect()
}

/// Enforce the exact pure Capability Delegation Law for replacing `target`'s
/// granted secrets:
/// - Operator or `is_admin=true` root agent: any existing `deployment_secrets` on any agent.
/// - Non-admin agent:
///   - Self-mutation is ALWAYS denied (`caller.id == target.id` -> 403 Forbidden).
///   - Target MUST be a direct child or descendant (`is_self_or_ancestor(caller, target)` -> else 403 Forbidden).
///   - Every requested secret AND every revoked secret MUST be held by the caller
///     (`requested ⊆ caller_held` and `(existing \ requested) ⊆ caller_held` -> else 403 Forbidden).
/// - All referenced secret names must exist in `deployment_secrets` (else 400 Bad Request).
fn authorize_agent_secret_grants(
    store: &Store,
    principal: &Principal,
    target: &Agent,
    requested_raw: &[String],
) -> Result<(String, Vec<String>), ApiError> {
    match principal {
        Principal::Operator => {
            let requested = normalize_requested_secrets(requested_raw)?;
            store
                .check_deployment_secrets_exist(&requested)
                .map_err(ApiError::from_store)?;
            store
                .record_admin_audit("operator", "secrets.set_grants", &target.name)
                .map_err(ApiError::from_store)?;
            Ok(("operator".to_string(), requested))
        }
        Principal::Agent { agent_id, name } => {
            let caller = store.agent(agent_id).map_err(ApiError::from_store)?;
            if is_admin_agent(&caller) {
                let requested = normalize_requested_secrets(requested_raw)?;
                store
                    .check_deployment_secrets_exist(&requested)
                    .map_err(ApiError::from_store)?;
                let actor = format!("agent:{name}");
                store
                    .record_admin_audit(&actor, "secrets.set_grants", &target.name)
                    .map_err(ApiError::from_store)?;
                return Ok((actor, requested));
            }
            if caller.id == target.id {
                return Err(ApiError::forbidden(
                    "an agent cannot modify its own secret grants",
                ));
            }
            if !is_self_or_ancestor(store, &caller.id, target) {
                return Err(ApiError::forbidden(
                    "only the operator, an admin, or an ancestor of the target agent may modify its secret grants",
                ));
            }
            let requested = normalize_requested_secrets(requested_raw)?;
            let parent_held: BTreeSet<String> = store
                .list_agent_secrets(&caller.id)
                .map_err(ApiError::from_store)?
                .into_iter()
                .collect();
            for secret_name in &requested {
                if !parent_held.contains(secret_name) {
                    return Err(ApiError::forbidden(format!(
                        "cannot grant secret `{secret_name}`: parent agent does not hold it"
                    )));
                }
            }
            let existing: BTreeSet<String> = store
                .list_agent_secrets(&target.id)
                .map_err(ApiError::from_store)?
                .into_iter()
                .collect();
            let requested_set: BTreeSet<&str> = requested.iter().map(String::as_str).collect();
            for prev in &existing {
                if !requested_set.contains(prev.as_str()) && !parent_held.contains(prev) {
                    return Err(ApiError::forbidden(format!(
                        "cannot revoke secret `{prev}`: parent agent does not hold it"
                    )));
                }
            }
            store
                .check_deployment_secrets_exist(&requested)
                .map_err(ApiError::from_store)?;
            Ok((format!("agent:{name}"), requested))
        }
    }
}

/// Pre-validate optional `secrets` passed to `POST /v1/agents` BEFORE admitting
/// the new agent, enforcing the exact Capability Delegation Law.
pub(crate) fn validate_create_agent_secrets(
    store: &Store,
    principal: &Principal,
    parent_id: Option<&AgentId>,
    requested_raw: Option<&[String]>,
) -> Result<Vec<String>, ApiError> {
    let Some(raw) = requested_raw else {
        return Ok(Vec::new());
    };
    let requested = normalize_requested_secrets(raw)?;
    match principal {
        Principal::Operator => {
            store
                .check_deployment_secrets_exist(&requested)
                .map_err(ApiError::from_store)?;
            Ok(requested)
        }
        Principal::Agent { agent_id, .. } => {
            let caller = store.agent(agent_id).map_err(ApiError::from_store)?;
            if is_admin_agent(&caller) {
                store
                    .check_deployment_secrets_exist(&requested)
                    .map_err(ApiError::from_store)?;
                return Ok(requested);
            }
            if requested.is_empty() {
                return Ok(Vec::new());
            }
            let permitted_parent = parent_id.is_some_and(|pid| {
                pid == &caller.id
                    || store.agent(pid).is_ok_and(|parent_agent| {
                        is_self_or_ancestor(store, &caller.id, &parent_agent)
                    })
            });
            if !permitted_parent {
                return Err(ApiError::forbidden(
                    "a non-admin agent may only grant secrets to its own descendants",
                ));
            }
            let parent_held: BTreeSet<String> = store
                .list_agent_secrets(&caller.id)
                .map_err(ApiError::from_store)?
                .into_iter()
                .collect();
            for secret_name in &requested {
                if !parent_held.contains(secret_name) {
                    return Err(ApiError::forbidden(format!(
                        "cannot grant secret `{secret_name}`: parent agent does not hold it"
                    )));
                }
            }
            store
                .check_deployment_secrets_exist(&requested)
                .map_err(ApiError::from_store)?;
            Ok(requested)
        }
    }
}

/// Persist validated initial secret grants immediately after agent creation
/// (before initial prompt dispatch).
pub(crate) fn apply_create_agent_secrets(
    store: &Store,
    agent_id: &AgentId,
    secret_names: &[String],
    actor: &str,
) -> Result<(), ApiError> {
    if secret_names.is_empty() {
        return Ok(());
    }
    store
        .set_agent_secrets(agent_id, secret_names, Some(actor))
        .map_err(ApiError::from_store)?;
    Ok(())
}

/// `GET /v1/secrets`: list write-only metadata for every deployment secret.
/// Operator or `is_admin=true` root agent only. NEVER returns plaintext values.
async fn list_secrets_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<SecretsListResponse>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "secrets.list", "*")?;
        let items = state
            .store
            .list_deployment_secrets()
            .map_err(ApiError::from_store)?;
        let names = load_agent_name_map(&state.store);
        let secrets = items
            .into_iter()
            .map(|meta| metadata_to_view(meta, &names))
            .collect();
        Ok(Json(SecretsListResponse { secrets }))
    })
    .await
}

/// `PUT /v1/secrets/{name}`: write-only encrypted upsert of a named secret bundle.
/// Operator or `is_admin=true` root agent only. NEVER returns plaintext values.
async fn upsert_secret_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<UpsertSecretRequest>,
) -> Result<Json<SecretMetadataView>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "secrets.upsert", &name)?;
        let meta = state
            .store
            .upsert_deployment_secret(&name, &body.env)
            .map_err(ApiError::from_store)?;
        let names = load_agent_name_map(&state.store);
        Ok(Json(metadata_to_view(meta, &names)))
    })
    .await
}

/// `DELETE /v1/secrets/{name}`: delete a secret bundle and cascade its grants.
/// Operator or `is_admin=true` root agent only.
async fn delete_secret_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<DeleteSecretResponse>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "secrets.delete", &name)?;
        let deleted = state
            .store
            .delete_deployment_secret(&name)
            .map_err(ApiError::from_store)?;
        if !deleted {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                format!("deployment secret `{name}` not found"),
            ));
        }
        Ok(Json(DeleteSecretResponse {
            deleted: true,
            name,
        }))
    })
    .await
}

/// `GET /v1/agents/{name}/secrets`: list secret bundle names granted to an agent.
/// Allowed for operator, `is_admin=true` root agent, self, or ancestor.
/// Returns secret names ONLY, NEVER plaintext values.
async fn get_agent_secrets_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentSecretsResponse>, ApiError> {
    spawn_store(move || {
        let target = resolve_target_agent(&state.store, &principal, &name)?;
        if !can_actor_read_agent_private(&state.store, &principal, &target) {
            return Err(ApiError::forbidden(
                "only the operator, an admin, the agent itself, or an ancestor may view secret grants",
            ));
        }
        let secrets = state
            .store
            .list_agent_secrets(&target.id)
            .map_err(ApiError::from_store)?;
        Ok(Json(AgentSecretsResponse {
            agent: target.name,
            agent_id: target.id.to_string(),
            secrets,
        }))
    })
    .await
}

/// `PUT /v1/agents/{name}/secrets`: replace an agent's secret bundle grants,
/// enforcing the exact Capability Delegation Law.
async fn set_agent_secrets_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<SetAgentSecretsRequest>,
) -> Result<Json<AgentSecretsResponse>, ApiError> {
    spawn_store(move || {
        let target = resolve_target_agent(&state.store, &principal, &name)?;
        let (actor, desired) =
            authorize_agent_secret_grants(&state.store, &principal, &target, &body.secrets)?;
        let secrets = state
            .store
            .set_agent_secrets(&target.id, &desired, Some(&actor))
            .map_err(ApiError::from_store)?;
        Ok(Json(AgentSecretsResponse {
            agent: target.name,
            agent_id: target.id.to_string(),
            secrets,
        }))
    })
    .await
}

/// `PATCH /v1/agents/{name}`: update agent metadata and/or secret grants.
/// Never loosens self-mutation deny (`caller.id != target.id`) and enforces the
/// exact Capability Delegation Law for `secrets`.
pub(super) async fn patch_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<PatchAgentRequest>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let target = resolve_target_agent(&state.store, &principal, &name)?;
        let has_meta = body.description.is_some()
            || body.instructions.is_some()
            || body.role.is_some()
            || body.scope.is_some()
            || body.lifecycle.is_some()
            || body.public.is_some();
        let has_secrets = body.secrets.is_some();
        let has_browser_profiles = body.browser_profiles.is_some();

        let is_strict_ancestor = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id != target.id && is_self_or_ancestor(&state.store, agent_id, &target)
            }
            Principal::Operator => true,
        };

        if !has_meta && !has_secrets && !has_browser_profiles {
            if !is_strict_ancestor {
                require_admin_or_operator(&state.store, &principal, "agents.update", &target.name)?;
            }
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "no fields provided to update",
            ));
        }

        // Pre-validate all permissions before performing any mutation.
        if has_meta && !is_strict_ancestor {
            require_admin_or_operator(&state.store, &principal, "agents.update", &target.name)?;
        }
        let prepared_secrets = if let Some(ref requested) = body.secrets {
            Some(authorize_agent_secret_grants(
                &state.store,
                &principal,
                &target,
                requested,
            )?)
        } else {
            None
        };
        let prepared_browser_profiles = if let Some(ref requested) = body.browser_profiles {
            Some(super::s3_media_profiles::validate_patch_browser_profiles(
                &state.store,
                &principal,
                &target,
                requested,
            )?)
        } else {
            None
        };

        let actor = match &principal {
            Principal::Operator => "operator".to_string(),
            Principal::Agent { name, .. } => format!("agent:{name}"),
        };
        let mut rec = target;
        if let Some(ref scope_val) = body.scope {
            rec = state
                .store
                .set_agent_scope_as(&rec.id, scope_val, &actor)
                .map_err(ApiError::from_store)?;
        } else if let Some(pub_val) = body.public {
            rec = state
                .store
                .set_agent_public_as(&rec.id, pub_val, &actor)
                .map_err(ApiError::from_store)?;
        }
        if let Some(ref lifecycle_val) = body.lifecycle {
            rec = state
                .store
                .set_agent_lifecycle_as(&rec.id, lifecycle_val, &actor)
                .map_err(ApiError::from_store)?;
        }
        if let Some(ref role_str) = body.role {
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
        if let Some(ref desc) = body.description {
            let trimmed = desc.trim();
            let clean_opt = (!trimmed.is_empty()).then_some(trimmed);
            rec = state
                .store
                .set_agent_description(&rec.id, clean_opt)
                .map_err(ApiError::from_store)?;
            crate::telegram::sync_agent_telegram_bot_name(
                &state.store,
                state.store.data_dir(),
                &rec,
            );
        }
        if let Some(ref inst) = body.instructions {
            let trimmed = inst.trim();
            let clean_opt = (!trimmed.is_empty()).then_some(trimmed);
            rec = state
                .store
                .set_agent_instructions(&rec.id, clean_opt)
                .map_err(ApiError::from_store)?;
        }
        if has_meta {
            let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &rec.id);
            let started_ms = state
                .health
                .get(&rec.id)
                .and_then(|h| h.process_started_at_ms);
            create::sync_agent_description_files_for_live(&state.store, &rec, &cfg, started_ms)
                .map_err(ApiError::from_store)?;
        }
        if let Some((secret_actor, desired_secrets)) = prepared_secrets {
            state
                .store
                .set_agent_secrets(&rec.id, &desired_secrets, Some(&secret_actor))
                .map_err(ApiError::from_store)?;
        }
        if let Some(desired_profiles) = prepared_browser_profiles {
            super::s3_media_profiles::apply_patch_browser_profiles(
                &state.store,
                &principal,
                &rec.id,
                &desired_profiles,
            )?;
        }

        let mut view = agent_view(&state.store, &state.health, &rec)?;
        if !can_actor_read_agent_private(&state.store, &principal, &rec) {
            view.instructions = None;
            view.secrets.clear();
        }
        Ok(Json(view))
    })
    .await
}

/// Resolve and decrypt all granted `deployment_secrets` for `agent_id`, register
/// their plaintext values in `anyagents_core::masking`, and return the safe env
/// map (never containing reserved `ANYAGENTS_*` / `HOME` / `XDG_*` keys).
pub(crate) fn resolve_agent_secret_env_for_sandbox(
    store: &Store,
    agent_id: &AgentId,
) -> Result<BTreeMap<String, String>, String> {
    let resolved = store
        .resolve_agent_secret_env(agent_id)
        .map_err(|err| format!("failed to resolve granted secrets for {agent_id}: {err}"))?;
    if !resolved.is_empty() {
        anyagents_core::masking::register_secret_values(resolved.values());
    }
    Ok(resolved
        .into_iter()
        .filter(|(k, _)| !anyagents_core::is_reserved_secret_env_key(k))
        .collect())
}

/// Assemble sandbox execution environment from filtered `agent.launch_env` plus
/// decrypted granted `deployment_secrets` (never overriding `ANYAGENTS_*` or
/// protected sandbox runtime variables).
pub(crate) fn merge_sandbox_exec_env(
    store: &Store,
    agent: &Agent,
    target: &mut BTreeMap<String, String>,
) -> Result<(), String> {
    target.extend(crate::sandbox_exec::filter_sandbox_launch_env(
        &agent.launch_env,
    ));
    let secrets = resolve_agent_secret_env_for_sandbox(store, &agent.id)?;
    for (k, v) in secrets {
        if !anyagents_core::is_reserved_secret_env_key(&k) {
            target.insert(k, v);
        }
    }
    Ok(())
}

/// Merge decrypted granted `deployment_secrets` for `agent_id_str` into `target`
/// when `store` is available, never overriding `ANYAGENTS_*` or protected vars.
pub(crate) fn merge_agent_secrets_into_env(
    store: Option<&Store>,
    agent_id_str: &str,
    target: &mut BTreeMap<String, String>,
) -> Result<Vec<String>, String> {
    let (Some(store), Ok(aid)) = (store, agent_id_str.parse::<AgentId>()) else {
        return Ok(Vec::new());
    };
    let secrets = resolve_agent_secret_env_for_sandbox(store, &aid)?;
    let values: Vec<String> = secrets.values().cloned().collect();
    for (k, v) in secrets {
        if !anyagents_core::is_reserved_secret_env_key(&k) {
            target.insert(k, v);
        }
    }
    Ok(values)
}

/// Apply decrypted granted secrets plus protected sandbox runtime env defaults
/// (`HOME`, `ANYAGENTS_GUEST_STATE_DIR`, `ANYAGENTS_GUEST_WORKDIR`) to an
/// [`crate::sandbox_providers::providers::ExecRequest`], returning the request
/// and the list of resolved secret values for output masking.
pub(crate) fn prepare_sandbox_exec_request(
    mut req: crate::sandbox_providers::providers::ExecRequest,
    handle: &crate::sandbox_providers::providers::SandboxHandle,
    store: Option<&Store>,
    agent_id_str: &str,
) -> Result<
    (
        crate::sandbox_providers::providers::ExecRequest,
        Vec<String>,
    ),
    String,
> {
    let secret_values = merge_agent_secrets_into_env(store, agent_id_str, &mut req.env)?;
    let req = req
        .with_cwd(&handle.workdir)
        .with_env("HOME", &handle.state_dir)
        .with_env("ANYAGENTS_GUEST_STATE_DIR", &handle.state_dir)
        .with_env("ANYAGENTS_GUEST_WORKDIR", &handle.workdir)
        .with_timeout_ms(300_000);
    Ok((req, secret_values))
}

/// Scrub known credential patterns and granted secret values in-place from a
/// sandbox provider execution's stdout and stderr.
pub(crate) fn mask_exec_output(
    out: &mut crate::sandbox_providers::providers::ExecOutput,
    secrets: &[String],
) {
    if !out.stdout.is_empty()
        && let std::borrow::Cow::Owned(masked) =
            anyagents_core::masking::mask_with_secrets(&out.stdout, secrets)
    {
        out.stdout = masked;
    }
    if !out.stderr.is_empty()
        && let std::borrow::Cow::Owned(masked) =
            anyagents_core::masking::mask_with_secrets(&out.stderr, secrets)
    {
        out.stderr = masked;
    }
}

/// Best-effort load of granted secret values for `agent_id` (also registering
/// them in the process-wide masker) so multi-replica Pure Serverless callbacks
/// scrub secrets even after scale-to-zero.
pub(crate) fn load_agent_mask_values(store: &Store, agent_id: &AgentId) -> Vec<String> {
    match store.resolve_agent_secret_env(agent_id) {
        Ok(map) if !map.is_empty() => {
            anyagents_core::masking::register_secret_values(map.values());
            map.into_values().collect()
        }
        _ => Vec::new(),
    }
}

/// Mask known credential patterns and all decrypted secret values granted to `agent_id`.
pub(crate) fn mask_text_for_agent(store: &Store, agent_id: &AgentId, text: &str) -> String {
    let values = load_agent_mask_values(store, agent_id);
    anyagents_core::masking::mask_with_secrets(text, &values).into_owned()
}

/// Scrub granted secret values from a remote turn settlement before durable
/// persistence, transcript archival, and outbound delivery.
pub(crate) fn mask_settlement_for_agent(
    store: &Store,
    agent_id: &AgentId,
    settlement: &mut SettleRemoteTurn,
) {
    let values = load_agent_mask_values(store, agent_id);
    if let Some(ref mut result) = settlement.result_text {
        *result = anyagents_core::masking::mask_with_secrets(result, &values).into_owned();
    }
    if let Some(ref mut err) = settlement.error_text {
        *err = anyagents_core::masking::mask_with_secrets(err, &values).into_owned();
    }
    if let Some(ref mut usage) = settlement.usage_json {
        anyagents_core::masking::mask_json_with_secrets(usage, &values);
    }
}
