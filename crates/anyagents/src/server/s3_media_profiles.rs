//! Coordinator routes & helpers for pure-OSS S3-compatible persistent browser
//! profiles and channel media storage (`ANYAGENTS_S3_*`).
//!
//! Security boundary: S3 credentials (`ANYAGENTS_S3_*`) and `ANYAGENTS_MASTER_KEY`
//! stay strictly inside the coordinator. Agents authenticate with their own
//! capability token (`x-anyagents-agent`) and can access only browser profiles
//! granted to them and media owned by themselves or their descendants.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use anyagents_core::s3_media::{media_content_type, media_s3_key};
use anyagents_core::{
    Agent, AgentId, BrowserProfileActor, Error, MasterKey, S3Store, Store,
    validate_browser_profile_name,
};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Deserialize;

use crate::auth::Principal;

use super::{ApiError, AppState, spawn_store};

/// Upper bound for encrypted browser profile archive uploads/downloads (256 MiB).
pub(crate) const MAX_PROFILE_ARCHIVE_BYTES: usize = 256 * 1024 * 1024;
const BROWSER_PROFILES_PREFIX: &str = "browser_profiles/";
const BROWSER_PROFILE_SUFFIX: &str = ".tar.zst";

/// Mount all S3 browser-profile and scoped media endpoints onto the authenticated
/// coordinator router.
pub(super) fn router() -> Router<AppState> {
    let archive_router = Router::new()
        .route(
            "/v1/browser-profiles/{profile}/archive",
            get(get_browser_profile_archive_route).put(put_browser_profile_archive_route),
        )
        .layer(DefaultBodyLimit::max(MAX_PROFILE_ARCHIVE_BYTES));

    Router::new()
        .route("/v1/browser-profiles", get(list_browser_profiles_route))
        .route(
            "/v1/browser-profiles/{profile}",
            delete(delete_browser_profile_route),
        )
        .route(
            "/v1/agents/{name}/browser-profiles",
            get(get_agent_browser_profiles_route).put(put_agent_browser_profiles_route),
        )
        .route(
            "/v1/media/{agent_id}/{media_id}",
            get(scoped_media_file_route),
        )
        .merge(archive_router)
}

pub(crate) fn browser_profile_s3_key(profile: &str) -> String {
    format!("{BROWSER_PROFILES_PREFIX}{profile}{BROWSER_PROFILE_SUFFIX}")
}

fn principal_is_operator(_store: &Store, principal: &Principal) -> Result<bool, ApiError> {
    Ok(principal.is_operator())
}

fn require_operator_audited_local(
    store: &Store,
    principal: &Principal,
    action: &str,
    target: &str,
) -> Result<String, ApiError> {
    if !principal.is_operator() {
        return Err(ApiError::forbidden("administrative authority required"));
    }
    store
        .record_admin_audit("operator", action, target)
        .map_err(ApiError::from_store)?;
    Ok("operator".to_string())
}

fn is_self_or_ancestor_local(store: &Store, caller_id: &AgentId, target: &Agent) -> bool {
    if caller_id == &target.id {
        return true;
    }
    let mut cursor = target.parent_id.clone();
    let mut depth = 0usize;
    while let Some(pid) = cursor {
        if &pid == caller_id {
            return true;
        }
        cursor = store.agent(&pid).ok().and_then(|a| a.parent_id);
        depth += 1;
        if depth > 16 {
            break;
        }
    }
    false
}

fn resolve_target_agent(
    store: &Store,
    principal: &Principal,
    raw_name: &str,
) -> Result<Agent, ApiError> {
    let clean = raw_name
        .trim()
        .strip_prefix("agent:")
        .unwrap_or(raw_name.trim());
    if clean.eq_ignore_ascii_case("self") {
        return match principal {
            Principal::Agent { agent_id, .. } => {
                store.agent(agent_id).map_err(ApiError::from_store)
            }
            Principal::Operator => Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "`self` requires an agent caller",
            )),
        };
    }
    store.agent_by_name(clean).map_err(ApiError::from_store)
}

fn actor_from_principal(principal: &Principal) -> BrowserProfileActor {
    match principal {
        Principal::Operator => BrowserProfileActor::Operator,
        Principal::Agent { agent_id, .. } => BrowserProfileActor::Agent(agent_id.clone()),
    }
}

fn require_s3_store() -> Result<S3Store, ApiError> {
    match S3Store::from_env() {
        Ok(Some(s3)) => Ok(s3),
        Ok(None) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "S3 object storage is not configured (set ANYAGENTS_S3_BUCKET, ANYAGENTS_S3_ACCESS_KEY_ID, ANYAGENTS_S3_SECRET_ACCESS_KEY)",
        )),
        Err(err) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("invalid S3 configuration: {err}"),
        )),
    }
}

fn map_s3_crypto_error(err: Error) -> ApiError {
    let msg = err.to_string();
    if msg.contains("encryption key required") || msg.contains("ANYAGENTS_MASTER_KEY") {
        ApiError::new(StatusCode::SERVICE_UNAVAILABLE, msg)
    } else {
        ApiError::new(StatusCode::BAD_GATEWAY, msg)
    }
}

fn authorize_profile_archive_access(
    store: &Store,
    principal: &Principal,
    profile: &str,
) -> Result<(), ApiError> {
    if principal_is_operator(store, principal)? {
        return Ok(());
    }
    match principal {
        Principal::Operator => Ok(()),
        Principal::Agent { agent_id, name } => {
            let granted = store
                .agent_has_browser_profile(agent_id, profile)
                .map_err(ApiError::from_store)?;
            if granted {
                Ok(())
            } else {
                Err(ApiError::forbidden(format!(
                    "agent `{name}` is not granted access to browser profile `{profile}`"
                )))
            }
        }
    }
}

/// Validate `browser_profiles` requested during `POST /v1/agents` before the
/// new agent is admitted so unauthorized grants fail closed without creating
/// an orphaned agent.
pub(crate) fn validate_create_browser_profiles(
    store: &Store,
    principal: &Principal,
    parent_id: Option<&AgentId>,
    requested: Option<&[String]>,
) -> Result<Vec<String>, ApiError> {
    let Some(list) = requested else {
        return Ok(Vec::new());
    };
    if list.is_empty() {
        return Ok(Vec::new());
    }
    let actor = actor_from_principal(principal);
    store
        .validate_browser_profile_delegation(&actor, parent_id, list)
        .map_err(ApiError::from_store)
}

/// Apply pre-validated `browser_profiles` grants right after an agent is created.
pub(crate) fn apply_create_browser_profiles(
    store: &Store,
    principal: &Principal,
    agent_id: &AgentId,
    profiles: &[String],
) -> Result<(), ApiError> {
    if profiles.is_empty() {
        return Ok(());
    }
    let actor = actor_from_principal(principal);
    store
        .set_agent_browser_profiles_as(&actor, agent_id, profiles)
        .map_err(ApiError::from_store)?;
    Ok(())
}

/// Pre-validate `browser_profiles` requested during `PATCH /v1/agents/{name}`
/// before any metadata or secret mutation runs, enforcing the exact
/// `child ⊆ parent` Capability Attenuation Law (including denying non-admin
/// self-mutation even when revoking to `[]`).
pub(crate) fn validate_patch_browser_profiles(
    store: &Store,
    principal: &Principal,
    target: &Agent,
    requested: &[String],
) -> Result<Vec<String>, ApiError> {
    let mut normalized = BTreeSet::new();
    for raw in requested {
        normalized.insert(validate_browser_profile_name(raw).map_err(ApiError::from_store)?);
    }
    let normalized: Vec<String> = normalized.into_iter().collect();
    if principal_is_operator(store, principal)? {
        return Ok(normalized);
    }
    let Principal::Agent { agent_id, .. } = principal else {
        return Err(ApiError::forbidden("administrative authority required"));
    };
    if agent_id == &target.id {
        return Err(ApiError::forbidden(
            "non-admin agent cannot modify its own browser profile grants",
        ));
    }
    if target.parent_id.as_ref() != Some(agent_id) {
        return Err(ApiError::forbidden(
            "only an operator, admin, or the agent's direct parent may modify browser profile grants",
        ));
    }
    let held: BTreeSet<String> = store
        .list_agent_browser_profiles(agent_id)
        .map_err(ApiError::from_store)?
        .into_iter()
        .collect();
    for profile in &normalized {
        if !held.contains(profile) {
            return Err(ApiError::forbidden(format!(
                "cannot grant browser profile `{profile}`: parent does not hold grant for `{profile}`"
            )));
        }
    }
    Ok(normalized)
}

/// Apply pre-validated `browser_profiles` replacement during `PATCH /v1/agents/{name}`
/// (unconditionally replacing grants so `[]` revokes existing grants).
pub(crate) fn apply_patch_browser_profiles(
    store: &Store,
    principal: &Principal,
    agent_id: &AgentId,
    profiles: &[String],
) -> Result<(), ApiError> {
    let actor = actor_from_principal(principal);
    store
        .set_agent_browser_profiles_as(&actor, agent_id, profiles)
        .map_err(ApiError::from_store)?;
    Ok(())
}

async fn list_browser_profiles_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let is_privileged = principal_is_operator(&state.store, &principal)?;
        if !is_privileged {
            let Principal::Agent { agent_id, name } = &principal else {
                return Err(ApiError::forbidden("administrative authority required"));
            };
            let s3_configured = anyagents_core::S3Config::from_env().is_some();
            let granted = state
                .store
                .list_agent_browser_profiles(agent_id)
                .map_err(ApiError::from_store)?;
            let profiles: Vec<serde_json::Value> = granted
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "profile": p,
                        "granted": true,
                    })
                })
                .collect();
            return Ok(Json(serde_json::json!({
                "s3_configured": s3_configured,
                "agent_id": agent_id.as_str(),
                "agent": name,
                "profiles": profiles,
                "browser_profiles": granted,
            })));
        }

        require_operator_audited_local(
            &state.store,
            &principal,
            "browser_profiles.list",
            "browser_profiles",
        )?;
        let (s3_configured, stored_set) = match S3Store::from_env() {
            Ok(Some(s3)) => {
                let keys = s3
                    .list_prefix(BROWSER_PROFILES_PREFIX)
                    .map_err(map_s3_crypto_error)?;
                let mut names = BTreeSet::new();
                for key in keys {
                    if let Some(stem) = key
                        .strip_prefix(BROWSER_PROFILES_PREFIX)
                        .and_then(|s| s.strip_suffix(BROWSER_PROFILE_SUFFIX))
                        && validate_browser_profile_name(stem).is_ok()
                    {
                        names.insert(stem.to_string());
                    }
                }
                (true, names)
            }
            Ok(None) => (false, BTreeSet::new()),
            Err(err) => {
                return Err(ApiError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("invalid S3 configuration: {err}"),
                ));
            }
        };

        let grants = state
            .store
            .list_all_browser_profile_grants()
            .map_err(ApiError::from_store)?;
        let mut agent_names: BTreeMap<String, String> = BTreeMap::new();
        let mut grants_by_profile: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
        let mut grant_names_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut all_profiles: BTreeSet<String> = stored_set.clone();

        for grant in grants {
            let aid = grant.agent_id.as_str().to_string();
            let agent_name = agent_names
                .entry(aid.clone())
                .or_insert_with(|| {
                    state
                        .store
                        .agent(&grant.agent_id)
                        .map(|a| a.name)
                        .unwrap_or_else(|_| aid.clone())
                })
                .clone();
            all_profiles.insert(grant.profile_name.clone());
            grant_names_map
                .entry(grant.profile_name.clone())
                .or_default()
                .push(agent_name.clone());
            grants_by_profile
                .entry(grant.profile_name.clone())
                .or_default()
                .push(serde_json::json!({
                    "agent_id": aid,
                    "agent": agent_name,
                    "granted_by": grant.granted_by,
                    "granted_at_ms": grant.granted_at_ms,
                }));
        }

        let profiles: Vec<serde_json::Value> = all_profiles
            .iter()
            .map(|name| {
                serde_json::json!({
                    "profile": name,
                    "stored_in_s3": stored_set.contains(name),
                    "grants": grants_by_profile.remove(name).unwrap_or_default(),
                })
            })
            .collect();
        let stored_profiles: Vec<String> = stored_set.into_iter().collect();

        Ok(Json(serde_json::json!({
            "s3_configured": s3_configured,
            "profiles": profiles,
            "stored_profiles": stored_profiles,
            "grants": grant_names_map,
        })))
    })
    .await
}

async fn get_agent_browser_profiles_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let target = resolve_target_agent(&state.store, &principal, &name)?;
        let allowed = match &principal {
            Principal::Operator => true,
            Principal::Agent { agent_id, .. } => {
                is_self_or_ancestor_local(&state.store, agent_id, &target)
                    || principal_is_operator(&state.store, &principal)?
            }
        };
        if !allowed {
            return Err(ApiError::forbidden(
                "only the operator, an admin, self, or an ancestor may list an agent's browser profiles",
            ));
        }
        let browser_profiles = state
            .store
            .list_agent_browser_profiles(&target.id)
            .map_err(ApiError::from_store)?;
        Ok(Json(serde_json::json!({
            "agent_id": target.id.as_str(),
            "agent": target.name,
            "browser_profiles": browser_profiles,
        })))
    })
    .await
}

#[derive(Deserialize)]
struct SetBrowserProfilesRequest {
    #[serde(alias = "profiles")]
    browser_profiles: Vec<String>,
}

async fn put_agent_browser_profiles_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<SetBrowserProfilesRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let target = resolve_target_agent(&state.store, &principal, &name)?;
        let actor = actor_from_principal(&principal);
        let updated = state
            .store
            .set_agent_browser_profiles_as(&actor, &target.id, &body.browser_profiles)
            .map_err(ApiError::from_store)?;
        if principal_is_operator(&state.store, &principal).unwrap_or(false) {
            let audit_actor = match &principal {
                Principal::Operator => "operator".to_string(),
                Principal::Agent { name, .. } => format!("agent:{name}"),
            };
            let _ = state.store.record_admin_audit(
                &audit_actor,
                "agents.set_browser_profiles",
                &target.name,
            );
        }
        Ok(Json(serde_json::json!({
            "agent_id": target.id.as_str(),
            "agent": target.name,
            "browser_profiles": updated,
        })))
    })
    .await
}

async fn get_browser_profile_archive_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(profile): AxumPath<String>,
) -> Result<impl IntoResponse, ApiError> {
    let valid = validate_browser_profile_name(&profile).map_err(ApiError::from_store)?;
    let store = Arc::clone(&state.store);
    let bytes = spawn_store(move || {
        authorize_profile_archive_access(&store, &principal, &valid)?;
        let s3 = require_s3_store()?;
        let key = browser_profile_s3_key(&valid);
        let data = s3
            .get_encrypted_object(&key, None)
            .map_err(map_s3_crypto_error)?;
        Ok(data)
    })
    .await?
    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "browser profile archive not found"))?;

    if bytes.len() > MAX_PROFILE_ARCHIVE_BYTES {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "browser profile archive exceeds maximum size ({MAX_PROFILE_ARCHIVE_BYTES} bytes)"
            ),
        ));
    }
    let content_type = if bytes.starts_with(b"\x1f\x8b") {
        "application/gzip"
    } else {
        "application/zstd"
    };
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        bytes,
    ))
}

async fn put_browser_profile_archive_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(profile): AxumPath<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let valid = validate_browser_profile_name(&profile).map_err(ApiError::from_store)?;
    if body.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "browser profile archive payload must be non-empty",
        ));
    }
    if body.len() > MAX_PROFILE_ARCHIVE_BYTES {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "browser profile archive exceeds maximum size ({MAX_PROFILE_ARCHIVE_BYTES} bytes)"
            ),
        ));
    }
    let size = body.len();
    let store = Arc::clone(&state.store);
    let profile_for_resp = valid.clone();
    spawn_store(move || {
        authorize_profile_archive_access(&store, &principal, &valid)?;
        let s3 = require_s3_store()?;
        let key = browser_profile_s3_key(&valid);
        s3.put_encrypted_object(&key, &body, "application/zstd", None)
            .map_err(map_s3_crypto_error)?;
        Ok(())
    })
    .await?;

    Ok(Json(serde_json::json!({
        "profile": profile_for_resp,
        "size_bytes": size,
        "encrypted": true,
        "stored": true,
    })))
}

async fn delete_browser_profile_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(profile): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let valid = validate_browser_profile_name(&profile).map_err(ApiError::from_store)?;
    let store = Arc::clone(&state.store);
    let (deleted_from_s3, revoked_grants) = spawn_store(move || {
        require_operator_audited_local(&store, &principal, "browser_profiles.delete", &valid)?;
        let deleted = match S3Store::from_env() {
            Ok(Some(s3)) => s3
                .delete_object(&browser_profile_s3_key(&valid))
                .map_err(map_s3_crypto_error)?,
            Ok(None) => false,
            Err(err) => {
                return Err(ApiError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("invalid S3 configuration: {err}"),
                ));
            }
        };
        let revoked = store
            .delete_browser_profile_grants(&valid)
            .map_err(ApiError::from_store)?;
        Ok((deleted, revoked))
    })
    .await?;

    Ok(Json(serde_json::json!({
        "profile": profile,
        "deleted_from_s3": deleted_from_s3,
        "revoked_grants": revoked_grants,
    })))
}

fn validate_media_segment(raw: &str) -> Result<String, ApiError> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > 200
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains("..")
        || trimmed.contains('\0')
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid media identifier",
        ));
    }
    Ok(trimmed.to_string())
}

fn load_scoped_media_bytes(
    store: &Store,
    agent_id: &AgentId,
    media_id: &str,
) -> Result<Option<Vec<u8>>, ApiError> {
    if let Some(s3) = S3Store::from_env().map_err(map_s3_crypto_error)? {
        let key = media_s3_key(agent_id.as_str(), media_id);
        if let Some(raw) = s3.get_object(&key).map_err(map_s3_crypto_error)? {
            let plain = if S3Store::is_encrypted_envelope(&raw) {
                s3.decrypt_object_bytes(&key, &raw, None)
                    .map_err(map_s3_crypto_error)?
            } else {
                raw
            };
            return Ok(Some(plain));
        }
    }

    let scoped_path = format!("{}/{media_id}", agent_id.as_str());
    let master_key = MasterKey::from_env().map_err(map_s3_crypto_error)?;
    let blobs = store.blob_store_with_key(master_key);
    if let Ok(Some(blob)) = blobs.get_deployment_blob("media", &scoped_path) {
        return Ok(Some(blob.content));
    }
    if let Ok(Some(blob)) = blobs.get_deployment_blob("media", media_id) {
        return Ok(Some(blob.content));
    }

    let media_dir = store.data_dir().join(anyagents_harness::media::MEDIA_DIR);
    for candidate in [
        media_dir.join(agent_id.as_str()).join(media_id),
        media_dir.join(media_id),
    ] {
        if candidate.is_file()
            && let Ok(raw) = std::fs::read(&candidate)
        {
            return Ok(Some(raw));
        }
    }
    Ok(None)
}

async fn scoped_media_file_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath((agent_ref, media_id)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let clean_media = validate_media_segment(&media_id)?;
    let store = Arc::clone(&state.store);
    let media_for_ct = clean_media.clone();
    let bytes = spawn_store(move || {
        let target = resolve_target_agent(&store, &principal, &agent_ref)?;
        let allowed = match &principal {
            Principal::Operator => true,
            Principal::Agent { agent_id, .. } => {
                is_self_or_ancestor_local(&store, agent_id, &target)
                    || principal_is_operator(&store, &principal)?
            }
        };
        if !allowed {
            return Err(ApiError::forbidden(
                "not authorized to read media belonging to this agent",
            ));
        }
        load_scoped_media_bytes(&store, &target.id, &clean_media)
    })
    .await?
    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "media file not found"))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, media_content_type(&media_for_ct))],
        bytes,
    ))
}

/// Hydrate both portable `/v1/media/<agent_id>/<media_id>` refs (from S3 or PG
/// `deployment_blobs`) and legacy `<data_dir>/media/<file_name>` refs into
/// `<cfg_dir>/media/<file_name>`, rewriting paths in `prompt` to
/// `<state_dir>/media/<file_name>` for isolated sandbox turns.
pub(crate) fn hydrate_prompt_media_for_sandbox(
    store: &Store,
    cfg_dir: &Path,
    state_dir: &str,
    prompt: &str,
) -> String {
    let remote_media_prefix = format!("{}/media", state_dir.trim_end_matches('/'));
    let mut rewritten = prompt.to_string();
    let stage_dir = cfg_dir.join("media");

    // 1. Portable scoped refs: `/v1/media/<agent_id>/<media_id>`.
    let scoped_prefix = "/v1/media/";
    let mut scoped_refs: BTreeSet<(String, String)> = BTreeSet::new();
    let mut search_from = 0usize;
    while let Some(rel_idx) = rewritten[search_from..].find(scoped_prefix) {
        let start = search_from + rel_idx + scoped_prefix.len();
        let rest = &rewritten[start..];
        let end_offset = rest
            .find(|c: char| {
                c.is_whitespace() || matches!(c, ']' | ')' | '"' | '\'' | '>' | ',' | ';')
            })
            .unwrap_or(rest.len());
        let tail = rest[..end_offset].trim();
        if let Some((aid, mid)) = tail.split_once('/')
            && !aid.is_empty()
            && !mid.is_empty()
            && !mid.contains('/')
            && !mid.contains('\\')
            && !mid.contains("..")
            && AgentId::parse(aid).is_ok()
        {
            scoped_refs.insert((aid.to_string(), mid.to_string()));
        }
        search_from = start + end_offset;
    }
    if !scoped_refs.is_empty() {
        let _ = std::fs::create_dir_all(&stage_dir);
        for (aid_str, mid) in scoped_refs {
            let Ok(aid) = AgentId::parse(&aid_str) else {
                continue;
            };
            if let Ok(Some(bytes)) = load_scoped_media_bytes(store, &aid, &mid)
                && !bytes.is_empty()
                && std::fs::write(stage_dir.join(&mid), &bytes).is_ok()
            {
                let from = format!("{scoped_prefix}{aid_str}/{mid}");
                let to = format!("{remote_media_prefix}/{mid}");
                rewritten = rewritten.replace(&from, &to);
            }
        }
    }

    // 2. Legacy `<data_dir>/media/<file_name>` and `/var/lib/anyagents/media/<file_name>` refs.
    let media_dir = store.data_dir().join(anyagents_harness::media::MEDIA_DIR);
    let mut prefixes = Vec::new();
    let dir_str = media_dir
        .to_string_lossy()
        .trim_end_matches('/')
        .to_string();
    if !dir_str.is_empty() {
        prefixes.push(format!("{dir_str}/"));
    }
    if dir_str != "/var/lib/anyagents/media" {
        prefixes.push("/var/lib/anyagents/media/".to_string());
    }
    let mut legacy_names: BTreeSet<String> = BTreeSet::new();
    for prefix in &prefixes {
        let mut cursor = 0usize;
        while let Some(rel_idx) = rewritten[cursor..].find(prefix.as_str()) {
            let start = cursor + rel_idx + prefix.len();
            let rest = &rewritten[start..];
            let end_offset = rest
                .find(|c: char| {
                    c.is_whitespace() || matches!(c, ']' | ')' | '"' | '\'' | '>' | ',' | ';')
                })
                .unwrap_or(rest.len());
            let fname = rest[..end_offset].trim();
            if !fname.is_empty()
                && !fname.contains('/')
                && !fname.contains('\\')
                && !fname.contains("..")
            {
                legacy_names.insert(fname.to_string());
            }
            cursor = start + end_offset;
        }
    }
    if !legacy_names.is_empty() {
        let _ = std::fs::create_dir_all(&stage_dir);
        for fname in legacy_names {
            let bytes_opt = store
                .blob_store()
                .get_deployment_blob("media", &fname)
                .ok()
                .flatten()
                .map(|b| b.content)
                .or_else(|| std::fs::read(media_dir.join(&fname)).ok());
            if let Some(bytes) = bytes_opt
                && !bytes.is_empty()
                && std::fs::write(stage_dir.join(&fname), &bytes).is_ok()
            {
                for prefix in &prefixes {
                    let from = format!("{prefix}{fname}");
                    let to = format!("{remote_media_prefix}/{fname}");
                    rewritten = rewritten.replace(&from, &to);
                }
            }
        }
    }

    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::Mutex;

    use anyagents_core::s3_media::persist_inbound_media_bytes;
    use anyagents_core::{
        MASTER_KEY_ENV, NewAgent, S3_ACCESS_KEY_ID_ENV, S3_BUCKET_ENV, S3_ENDPOINT_ENV,
        S3_FORCE_PATH_STYLE_ENV, S3_PREFIX_ENV, S3_REGION_ENV, S3_SECRET_ACCESS_KEY_ENV,
    };
    use anyagents_supervisor::HealthBoard;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    struct EnvRestore(Vec<(&'static str, Option<String>)>);

    impl EnvRestore {
        fn capture(keys: &[&'static str]) -> Self {
            Self(keys.iter().map(|&k| (k, std::env::var(k).ok())).collect())
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (k, v) in &self.0 {
                match v {
                    Some(val) => unsafe { std::env::set_var(k, val) },
                    None => unsafe { std::env::remove_var(k) },
                }
            }
        }
    }

    type MockS3Objects = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

    fn spawn_mock_s3(bucket: &str) -> (String, MockS3Objects) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock S3");
        let addr = listener.local_addr().unwrap();
        let objects: MockS3Objects = Arc::new(Mutex::new(BTreeMap::new()));
        let objects_srv = Arc::clone(&objects);
        let bucket_prefix = format!("/{bucket}/");
        let bucket_root = format!("/{bucket}");

        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut req_line = String::new();
                if reader.read_line(&mut req_line).unwrap_or(0) == 0 {
                    continue;
                }
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let target = parts.next().unwrap_or("/").to_string();
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0
                        || line == "\r\n"
                        || line == "\n"
                    {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':')
                        && k.trim().eq_ignore_ascii_case("content-length")
                    {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    let _ = reader.read_exact(&mut body);
                }
                let (path, query) = target
                    .split_once('?')
                    .map(|(p, q)| (p, Some(q)))
                    .unwrap_or((target.as_str(), None));
                let mut map = objects_srv.lock().unwrap();
                let mut stream = stream;

                if method == "GET" && path == bucket_root && query.is_some() {
                    let mut xml =
                        String::from("<ListBucketResult><IsTruncated>false</IsTruncated>");
                    for k in map.keys() {
                        xml.push_str("<Contents><Key>");
                        xml.push_str(k);
                        xml.push_str("</Key></Contents>");
                    }
                    xml.push_str("</ListBucketResult>");
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        xml.len(),
                        xml
                    );
                    continue;
                }
                let key = path.strip_prefix(&bucket_prefix).unwrap_or("").to_string();
                match method.as_str() {
                    "PUT" => {
                        map.insert(key, body);
                        let _ = write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                    }
                    "GET" => {
                        if let Some(data) = map.get(&key) {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                data.len()
                            );
                            let _ = stream.write_all(data);
                        } else {
                            let _ = write!(
                                stream,
                                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            );
                        }
                    }
                    "DELETE" => {
                        let code = if map.remove(&key).is_some() {
                            "204 No Content"
                        } else {
                            "404 Not Found"
                        };
                        let _ = write!(
                            stream,
                            "HTTP/1.1 {code}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                    }
                    _ => {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                    }
                }
            }
        });
        (format!("http://{addr}"), objects)
    }

    fn make_test_app(store: Arc<Store>) -> Router {
        let state = AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(super::super::ProcessRunner)),
            auth: crate::auth::Mode::Confined {
                operator_digest: Some(crate::auth::operator_digest("test-op-token")),
            },
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        };
        router().with_state(state)
    }

    fn request_as(
        principal: Principal,
        method: &str,
        uri: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(ct) = content_type {
            builder = builder.header(header::CONTENT_TYPE, ct);
        }
        let mut req = builder.body(Body::from(body)).unwrap();
        req.extensions_mut()
            .insert(crate::auth::InjectedPrincipal(principal));
        req
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn browser_profile_routes_enforce_grants_and_encrypt_archives_in_s3() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            S3_ENDPOINT_ENV,
            S3_BUCKET_ENV,
            S3_ACCESS_KEY_ID_ENV,
            S3_SECRET_ACCESS_KEY_ENV,
            S3_REGION_ENV,
            S3_PREFIX_ENV,
            S3_FORCE_PATH_STYLE_ENV,
            MASTER_KEY_ENV,
        ]);

        let (endpoint, s3_objects) = spawn_mock_s3("profiles-test");
        unsafe {
            std::env::set_var(S3_ENDPOINT_ENV, &endpoint);
            std::env::set_var(S3_BUCKET_ENV, "profiles-test");
            std::env::set_var(S3_ACCESS_KEY_ID_ENV, "TESTKEY000001");
            std::env::set_var(S3_SECRET_ACCESS_KEY_ENV, "test-secret-000001");
            std::env::set_var(S3_REGION_ENV, "us-east-1");
            std::env::remove_var(S3_PREFIX_ENV);
            std::env::set_var(S3_FORCE_PATH_STYLE_ENV, "true");
            std::env::set_var(MASTER_KEY_ENV, "master-key-for-server-route-tests-123456");
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let parent = store
            .create_agent(NewAgent::new("parent-a", "claude-code"))
            .unwrap();
        let mut child_req = NewAgent::new("child-a", "claude-code");
        child_req.parent_id = Some(parent.id.clone());
        let child = store.create_agent(child_req).unwrap();
        let other = store
            .create_agent(NewAgent::new("other-b", "claude-code"))
            .unwrap();

        let app = make_test_app(Arc::clone(&store));
        let op = Principal::Operator;
        let parent_p = Principal::Agent {
            agent_id: parent.id.clone(),
            name: parent.name.clone(),
        };
        let child_p = Principal::Agent {
            agent_id: child.id.clone(),
            name: child.name.clone(),
        };
        let other_p = Principal::Agent {
            agent_id: other.id.clone(),
            name: other.name.clone(),
        };

        // 1. Non-admin self-mutation is denied with 403.
        let resp = app
            .clone()
            .oneshot(request_as(
                parent_p.clone(),
                "PUT",
                "/v1/agents/parent-a/browser-profiles",
                serde_json::to_vec(&serde_json::json!({ "browser_profiles": ["team-site"] }))
                    .unwrap(),
                Some("application/json"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // 2. Operator grants parent [team-site, docs-site].
        let resp = app
            .clone()
            .oneshot(request_as(
                op.clone(),
                "PUT",
                "/v1/agents/parent-a/browser-profiles",
                serde_json::to_vec(
                    &serde_json::json!({ "browser_profiles": ["team-site", "docs-site"] }),
                )
                .unwrap(),
                Some("application/json"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // 3. Parent delegates subset [team-site] to direct child, but fails on unheld [secret-site].
        let resp = app
            .clone()
            .oneshot(request_as(
                parent_p.clone(),
                "PUT",
                "/v1/agents/child-a/browser-profiles",
                serde_json::to_vec(&serde_json::json!({ "browser_profiles": ["secret-site"] }))
                    .unwrap(),
                Some("application/json"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let resp = app
            .clone()
            .oneshot(request_as(
                parent_p.clone(),
                "PUT",
                "/v1/agents/child-a/browser-profiles",
                serde_json::to_vec(&serde_json::json!({ "browser_profiles": ["team-site"] }))
                    .unwrap(),
                Some("application/json"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // 4. Child downloads before upload -> 404; ungranted `other` -> 403.
        let resp = app
            .clone()
            .oneshot(request_as(
                child_p.clone(),
                "GET",
                "/v1/browser-profiles/team-site/archive",
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = app
            .clone()
            .oneshot(request_as(
                other_p.clone(),
                "GET",
                "/v1/browser-profiles/team-site/archive",
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // 5. Child uploads archive; S3 bytes are encrypted; parent downloads exact plaintext.
        let payload = b"\x28\xb5\x2f\xfdfake-zstd-browser-cookies-session-payload".to_vec();
        let resp = app
            .clone()
            .oneshot(request_as(
                child_p.clone(),
                "PUT",
                "/v1/browser-profiles/team-site/archive",
                payload.clone(),
                Some("application/zstd"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        {
            let map = s3_objects.lock().unwrap();
            let raw = map
                .get("browser_profiles/team-site.tar.zst")
                .expect("stored in S3");
            assert!(raw.starts_with(b"AAENC1"));
            assert!(!raw.windows(16).any(|w| w == &payload[..16]));
        }

        let resp = app
            .clone()
            .oneshot(request_as(
                parent_p.clone(),
                "GET",
                "/v1/browser-profiles/team-site/archive",
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let got = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(got.as_ref(), payload.as_slice());

        // 5b. Non-admin GET /v1/browser-profiles NEVER leaks other agents' profiles.
        let resp = app
            .clone()
            .oneshot(request_as(
                child_p.clone(),
                "GET",
                "/v1/browser-profiles",
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let child_list: serde_json::Value =
            serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(
            child_list["browser_profiles"],
            serde_json::json!(["team-site"])
        );
        assert!(child_list.get("stored_profiles").is_none());

        let resp = app
            .clone()
            .oneshot(request_as(
                other_p.clone(),
                "GET",
                "/v1/browser-profiles",
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let other_list: serde_json::Value =
            serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(other_list["browser_profiles"], serde_json::json!([]));

        // 6. Operator lists profiles and deletes team-site (removing S3 object + grants).
        let resp = app
            .clone()
            .oneshot(request_as(
                op.clone(),
                "GET",
                "/v1/browser-profiles",
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let list_json: serde_json::Value =
            serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(list_json["s3_configured"], true);
        assert!(
            list_json["stored_profiles"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "team-site")
        );

        let resp = app
            .clone()
            .oneshot(request_as(
                op.clone(),
                "DELETE",
                "/v1/browser-profiles/team-site",
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            store
                .list_agent_browser_profiles(&child.id)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn s3_media_persistence_authenticated_retrieval_and_sandbox_hydration() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            S3_ENDPOINT_ENV,
            S3_BUCKET_ENV,
            S3_ACCESS_KEY_ID_ENV,
            S3_SECRET_ACCESS_KEY_ENV,
            S3_REGION_ENV,
            S3_PREFIX_ENV,
            S3_FORCE_PATH_STYLE_ENV,
            MASTER_KEY_ENV,
        ]);

        let (endpoint, s3_objects) = spawn_mock_s3("media-test");
        unsafe {
            std::env::set_var(S3_ENDPOINT_ENV, &endpoint);
            std::env::set_var(S3_BUCKET_ENV, "media-test");
            std::env::set_var(S3_ACCESS_KEY_ID_ENV, "TESTKEY000002");
            std::env::set_var(S3_SECRET_ACCESS_KEY_ENV, "test-secret-000002");
            std::env::set_var(S3_REGION_ENV, "us-east-1");
            std::env::remove_var(S3_PREFIX_ENV);
            std::env::set_var(S3_FORCE_PATH_STYLE_ENV, "true");
            std::env::set_var(MASTER_KEY_ENV, "master-key-for-media-s3-tests-123456");
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let parent = store
            .create_agent(NewAgent::new("media-parent", "claude-code"))
            .unwrap();
        let mut child_req = NewAgent::new("media-child", "claude-code");
        child_req.parent_id = Some(parent.id.clone());
        let child = store.create_agent(child_req).unwrap();
        let stranger = store
            .create_agent(NewAgent::new("media-stranger", "claude-code"))
            .unwrap();

        let img_bytes = b"PNG-BINARY-SECRET-PHOTO-PAYLOAD-12345";
        let note = persist_inbound_media_bytes(&store, &child.id, "photo", "cat.png", img_bytes)
            .expect("persist to S3");
        let expected_prefix = format!("[Media (photo): /v1/media/{}/", child.id.as_str());
        assert!(note.starts_with(&expected_prefix), "got {note}");
        let uri = note
            .trim_start_matches("[Media (photo): ")
            .trim_end_matches(']');

        // Ciphertext stored in S3 must be encrypted and must not contain plaintext.
        {
            let map = s3_objects.lock().unwrap();
            assert_eq!(map.len(), 1);
            let (stored_key, raw) = map.iter().next().unwrap();
            assert!(stored_key.starts_with(&format!("media/{}/", child.id.as_str())));
            assert!(raw.starts_with(b"AAENC1"));
            assert!(!raw.windows(img_bytes.len()).any(|w| w == img_bytes));
        }

        let app = make_test_app(Arc::clone(&store));

        // Self can read.
        let resp = app
            .clone()
            .oneshot(request_as(
                Principal::Agent {
                    agent_id: child.id.clone(),
                    name: child.name.clone(),
                },
                "GET",
                uri,
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("image/png")
        );
        let got = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(got.as_ref(), img_bytes);

        // Parent (ancestor) can read.
        let resp = app
            .clone()
            .oneshot(request_as(
                Principal::Agent {
                    agent_id: parent.id.clone(),
                    name: parent.name.clone(),
                },
                "GET",
                uri,
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Unrelated non-admin agent is denied with 403.
        let resp = app
            .clone()
            .oneshot(request_as(
                Principal::Agent {
                    agent_id: stranger.id.clone(),
                    name: stranger.name.clone(),
                },
                "GET",
                uri,
                Vec::new(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // Sandbox hydration fetches from S3, stages into `<cfg_dir>/media/`, and rewrites prompt.
        let cfg_dir = dir.path().join("sandbox-cfg");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        let hydrated = hydrate_prompt_media_for_sandbox(
            &store,
            &cfg_dir,
            "/home/user/agent-data/state",
            &format!("Look at this photo:\n{note}"),
        );
        assert!(
            hydrated.contains("/home/user/agent-data/state/media/"),
            "hydrated prompt: {hydrated}"
        );
        assert!(!hydrated.contains("/v1/media/"), "rewrote ref: {hydrated}");
        let staged_files: Vec<_> = std::fs::read_dir(cfg_dir.join("media"))
            .unwrap()
            .flatten()
            .collect();
        assert_eq!(staged_files.len(), 1);
        assert_eq!(std::fs::read(staged_files[0].path()).unwrap(), img_bytes);
    }
}
