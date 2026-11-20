//! Profiles, subagent defaults, usage, health, sign-in, and operator auth routes.

use std::sync::Arc;

use anyagents_core::{Agent, Error, Store, now_ms};
use anyagents_installer::{builtin_catalog, sign_in};
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde::Deserialize;

use crate::auth::{self, Principal};
use crate::create;
use crate::operator;

use super::types::{
    ApiError, AppState, SignInLine, require_operator, require_operator_audited, spawn_store,
};
use super::{ServedProfile, in_flight, quota_rotation, serverless};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/profiles", get(profiles_route).post(set_profile_route))
        .route("/v1/profiles/native-signin", post(native_signin_route))
        .route("/v1/profiles/models", get(profile_models_route))
        .route(
            "/v1/subagent-defaults",
            get(get_subagent_defaults_route).post(set_subagent_defaults_route),
        )
        .route("/v1/harnesses/{id}/sign-in", post(sign_in_route))
        .route(
            "/v1/operator/agents/{name}/capability",
            post(mint_capability),
        )
        .route(
            "/v1/operator/agents/{name}/remote-sessions/revoke",
            post(revoke_remote_sessions_route),
        )
        .route("/v1/whoami", get(whoami))
        .route("/v1/usage", get(usage_report))
}

pub(super) fn public_health_router() -> Router<AppState> {
    Router::new()
        .route("/", get(public_healthz))
        .route("/healthz", get(public_healthz))
        .route("/v1/webhooks/healthz", get(public_healthz))
}

/// Mint an agent's capability after its instance was made (a create, a new
/// sign-in), so its harness starts with one and the last one is retired.
fn capability_mark(store: Arc<Store>) -> impl Fn() -> Result<(), Error> + Send + Sync + 'static {
    move || {
        let agents = store.list_agents()?;
        for agent in &agents {
            crate::operator::mint(&store, &agent.id)?;
        }
        Ok(())
    }
}

/// `POST /v1/operator/agents/{name}/capability`: the operator or a root admin
/// gives one agent a fresh capability, retiring the last one. The value is
/// written into that agent's own config directory and never returned.
async fn mint_capability(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_operator_audited(&state.store, &principal, "agents.mint_capability", &name)?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        operator::mint(&state.store, &agent.id).map_err(ApiError::from_store)?;
        Ok(Json(
            serde_json::json!({ "agent": agent.name, "minted": true }),
        ))
    })
    .await
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RevokeRemoteSessionsRequest {
    /// One session to revoke; every session of the agent when absent.
    #[serde(default)]
    pub(super) session_id: Option<String>,
}

/// `POST /v1/operator/agents/{name}/remote-sessions/revoke`: the operator or a
/// root admin ends an agent's remote sessions now. Every token of a revoked
/// session stops authenticating on its next call.
pub(super) async fn revoke_remote_sessions_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<RevokeRemoteSessionsRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_operator_audited(
            &state.store,
            &principal,
            "agents.revoke_remote_sessions",
            &name,
        )?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let session = body
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let revoked = crate::remote::revoke(&state.store, &agent.id, session)
            .map_err(ApiError::from_store)?;
        Ok(Json(serde_json::json!({
            "agent": agent.name,
            "session_id": session,
            "revoked": revoked,
        })))
    })
    .await
}

/// `GET /v1/webhooks/healthz` on `anyagents.sock`: the serving identity
/// (version, source SHA, binary SHA-256) for the operator and the release
/// tooling. The public listener answers [`public_healthz`] instead.
pub(super) async fn webhook_healthz(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(state.identity.status()),
    )
}

/// `GET /`, `/healthz` and `/v1/webhooks/healthz` on the public TCP listener:
/// readiness only. The build identity names the exact binary this server
/// runs, which tells a scanner which advisories apply, so it stays on the
/// Unix socket.
async fn public_healthz() -> impl IntoResponse {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({ "ok": true })),
    )
}

pub(super) async fn public_not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": "not found" })),
    )
}

/// Query parameters for `GET /v1/usage`. `agent` names a target by name or id
/// (or `self`); `include_descendants` (alias `subtree`) widens an explicit
/// target to its subtree; `since_ms`/`until_ms` bound the recording window
/// inclusively; `days` is a relative window from now; `group_by` is one of
/// `agent`, `day`, `model`, `harness`.
#[derive(Debug, Default, Deserialize)]
pub(super) struct UsageQuery {
    pub(super) agent: Option<String>,
    pub(super) include_descendants: Option<bool>,
    pub(super) subtree: Option<bool>,
    pub(super) since_ms: Option<i64>,
    pub(super) until_ms: Option<i64>,
    pub(super) days: Option<i64>,
    pub(super) group_by: Option<String>,
}

/// `GET /v1/usage`: structured usage aggregation over the daemon socket. The
/// caller is resolved from its credential, never from a parameter: the
/// operator, a public-scope caller and a root caller may aggregate the whole
/// workspace or any target, while every other caller is confined to itself
/// and its causal descendants, and any wider request fails closed.
/// Every agent, and the ones `principal` may read usage and outcomes for:
/// the operator, a public-scope caller and a root caller may read the whole
/// workspace or any target; every other caller only itself and its causal
/// descendants, and any wider request fails closed.
fn usage_scope(
    store: &Store,
    principal: &Principal,
    target: Option<&str>,
    include_descendants: Option<bool>,
) -> Result<(Vec<Agent>, Vec<anyagents_core::AgentId>), ApiError> {
    let caller = match principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, .. } => Some(
            store
                .agent(agent_id)
                .map_err(|_| ApiError::forbidden("authenticated agent no longer exists"))?,
        ),
    };
    let agents = store.list_agents().map_err(ApiError::from_store)?;
    let agent_ids =
        anyagents_core::resolve_usage_agents(caller.as_ref(), &agents, target, include_descendants)
            .map_err(|err| match err {
                anyagents_core::UsageAccessError::Forbidden(why) => ApiError::forbidden(why),
                anyagents_core::UsageAccessError::Invalid(why) => {
                    ApiError::new(StatusCode::BAD_REQUEST, why)
                }
            })?;
    Ok((agents, agent_ids))
}

pub(super) async fn usage_report(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<UsageQuery>,
) -> Result<Json<anyagents_core::UsageReport>, ApiError> {
    spawn_store(move || {
        let (agents, agent_ids) = usage_scope(
            &state.store,
            &principal,
            query.agent.as_deref(),
            query.include_descendants.or(query.subtree),
        )?;
        let group_by = match query.group_by.as_deref() {
            None => anyagents_core::UsageGroupBy::Agent,
            Some(raw) => anyagents_core::UsageGroupBy::parse(raw)
                .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?,
        };
        let filter = anyagents_core::UsageFilter {
            since_ms: query.since_ms,
            until_ms: query.until_ms,
            days: query.days,
            group_by,
        };
        let names = agents
            .iter()
            .map(|agent| (agent.id.clone(), agent.name.clone()))
            .collect();
        let agent_profiles: std::collections::HashMap<anyagents_core::AgentId, String> =
            if matches!(group_by, anyagents_core::UsageGroupBy::Profile) {
                agents
                    .iter()
                    .map(|agent| {
                        let cfg =
                            anyagents_core::agent_config_dir(state.store.data_dir(), &agent.id);
                        let bound = create::bound_profile_name(&cfg).or_else(|| {
                            if !state.store.is_postgres() {
                                return None;
                            }
                            state
                                .store
                                .blob_store()
                                .get_agent_file(&agent.id, "markers", ".anyagents-profile")
                                .ok()
                                .flatten()
                                .and_then(|e| String::from_utf8(e.content).ok())
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                        });
                        let resolved = match bound {
                            Some(b) if b.contains('/') => b,
                            Some(b) => {
                                let prov = create::upstream_provider_of(
                                    &agent.harness,
                                    agent.model.as_deref(),
                                );
                                format!("{prov}/{b}")
                            }
                            None => {
                                let prov = create::upstream_provider_of(
                                    &agent.harness,
                                    agent.model.as_deref(),
                                );
                                format!("{prov}/default")
                            }
                        };
                        (agent.id.clone(), resolved)
                    })
                    .collect()
            } else {
                std::collections::HashMap::new()
            };
        let records = state.store.list_usage(None).map_err(ApiError::from_store)?;
        let report = anyagents_core::aggregate_usage_with_profiles(
            &records,
            &agent_ids,
            &names,
            &agent_profiles,
            filter,
            now_ms(),
        )
        .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?;
        Ok(Json(report))
    })
    .await
}

/// `GET /v1/whoami`: the agent this call acts as. A tool server asks before it
/// speaks for anyone, and gets the id the server resolved from the capability,
/// never the name a caller wrote.
async fn whoami(
    State(state): State<AppState>,
    ctx: Whoami,
) -> Result<Json<serde_json::Value>, ApiError> {
    match ctx.principal {
        Principal::Agent { agent_id, name } => {
            spawn_store(move || {
                let agent_record = state.store.agent(&agent_id).ok();
                let workdir = agent_record.as_ref().and_then(|a| a.workdir.clone());
                let harness = agent_record.as_ref().map(|a| a.harness.clone());
                Ok(Json(serde_json::json!({
                    "agent_id": agent_id.as_str(),
                    "name": name,
                    "workdir": workdir,
                    "harness": harness,
                })))
            })
            .await
        }
        Principal::Operator => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "the operator authenticated, but this call names an agent",
        )),
    }
}

/// The principal a `whoami` call resolved, from the capability header or, on a
/// deployment without the boundary, from the credential the tool server reads.
struct Whoami {
    principal: Principal,
}

impl axum::extract::FromRequestParts<AppState> for Whoami {
    type Rejection = auth::AuthRejection;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if state.auth.is_trusted()
            && parts.headers.get(crate::operator::AGENT_HEADER).is_none()
            && let Some(id) = parts
                .headers
                .get("x-anyagents-agent-id")
                .and_then(|value| value.to_str().ok())
        {
            // A machine without the boundary: the tool server says which agent
            // its own config directory belongs to, and the server checks that
            // the agent exists. Only here, and only for this read.
            let store = Arc::clone(&state.store);
            let id = id.trim().to_string();
            let principal = auth::off_worker(move || auth::agent_by_name(&store, &id)).await?;
            return Ok(Self { principal });
        }
        use axum::extract::FromRequestParts;
        let principal =
            <Principal as FromRequestParts<AppState>>::from_request_parts(parts, state).await?;
        Ok(Self { principal })
    }
}

#[derive(Deserialize, Default)]
struct ProfileModelsQuery {
    #[serde(default)]
    profile: Option<String>,
    #[serde(default)]
    filter: Option<String>,
}

async fn profile_models_route(
    _principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<ProfileModelsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let target = query.profile.as_deref().or(query.filter.as_deref());
        Ok(Json(super::profile_models::catalog_for_store(
            &state.store,
            target,
        )))
    })
    .await
}

#[derive(Debug, Default, Deserialize)]
struct ProfilesQuery {
    /// Answer with a fresh live-probed listing (`?fresh=1` or `?fresh=true`).
    #[serde(default)]
    fresh: Option<String>,
}

fn parse_fresh_flag(raw: Option<&str>) -> Result<bool, ApiError> {
    let Some(val) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(false);
    };
    match val.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        other => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("invalid `fresh` query value `{other}`: expected 1, 0, true, or false"),
        )),
    }
}

/// `GET /v1/profiles`: the last listing at once, each entry marked with the
/// age of its usage reading; pass `?fresh=1` (or `?fresh=true`) to bypass
/// stale caches and probe live 5h + Weekly quotas across all providers.
async fn profiles_route(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<ProfilesQuery>,
) -> Result<Json<Vec<ServedProfile>>, ApiError> {
    let fresh = parse_fresh_flag(query.fresh.as_deref())?;
    let store = Arc::clone(&state.store);
    spawn_store(move || {
        require_operator_audited(&store, &principal, "profiles.list", "profiles").map(drop)
    })
    .await?;
    state
        .profiles
        .list(&state.store, fresh)
        .await
        .map(Json)
        .map_err(|why| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetProfileRequest {
    profile: String,
    key: String,
}

async fn set_profile_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<SetProfileRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        require_operator_audited(&state.store, &principal, "profiles.set", &body.profile)?;
        let (provider, profile) =
            create::set_provider_profile(state.store.data_dir(), &body.profile, &body.key)
                .map_err(ApiError::from_store)?;
        if state.store.is_postgres() {
            serverless::persist_profile_dir_to_deployment_blobs(&state.store, &provider, &profile)
                .map_err(ApiError::from_store)?;
        }
        state.profiles.invalidate(&state.store);
        quota_rotation::profiles_changed(Some(provider.as_str()));
        Ok(Json(serde_json::json!({
            "provider": provider,
            "profile": profile,
        })))
    })
    .await
}

async fn get_subagent_defaults_route(
    _principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<create::SubagentDefaultsConfig>, ApiError> {
    spawn_store(move || Ok(Json(create::load_subagent_defaults(state.store.data_dir())))).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetSubagentDefaultsRequest {
    agent: Option<String>,
    clear: Option<bool>,
    mode: String,
    harness: Option<String>,
    model: Option<String>,
    profile: Option<String>,
}

async fn set_subagent_defaults_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<SetSubagentDefaultsRequest>,
) -> Result<Json<create::SubagentDefaultsConfig>, ApiError> {
    spawn_store(move || {
        let target_label = body.agent.as_deref().unwrap_or("global");
        require_operator_audited(
            &state.store,
            &principal,
            "subagents.set_default",
            target_label,
        )?;
        let mut cfg = create::load_subagent_defaults(state.store.data_dir());
        if body.clear.unwrap_or(false) {
            if let Some(ref ag) = body.agent {
                cfg.agents.remove(ag.trim());
            } else {
                cfg.default = create::SubagentDefaultSpec {
                    mode: "inherit".to_string(),
                    harness: None,
                    model: None,
                    profile: None,
                };
            }
        } else {
            let spec = create::SubagentDefaultSpec {
                mode: body.mode,
                harness: body
                    .harness
                    .as_deref()
                    .map(anyagents_harness::normalize_harness_name)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
                model: body.model.filter(|s| !s.trim().is_empty()),
                profile: body.profile.filter(|s| !s.trim().is_empty()),
            };
            if let Some(ag) = body
                .agent
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                cfg.agents.insert(ag.to_string(), spec);
            } else {
                cfg.default = spec;
            }
        }
        create::save_subagent_defaults(state.store.data_dir(), &cfg)
            .map_err(ApiError::from_store)?;
        Ok(Json(cfg))
    })
    .await
}

#[derive(Deserialize, Default)]
struct SignInQuery {
    #[serde(default)]
    profile: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeSignInRequest {
    /// Target harness or provider (`claude-code`, `codex`, `agy`, `anthropic`, `openai`, `google-antigravity`, ...).
    #[serde(alias = "provider")]
    harness: String,
    /// Target profile name (defaults to `"default"`).
    #[serde(default)]
    profile: Option<String>,
}

fn canonical_signin_harness(raw: &str) -> String {
    match anyagents_harness::normalize_harness_name(raw) {
        "anthropic" => "claude-code".to_string(),
        "openai" => "codex".to_string(),
        "google-antigravity" => "agy".to_string(),
        "deepseek" => "dsh".to_string(),
        "xai" => "grok".to_string(),
        other => other.to_string(),
    }
}

/// `POST /v1/profiles/native-signin`: canonical Control-Plane REST route to
/// stream a native provider/harness sign-in flow into a profile.
async fn native_signin_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<NativeSignInRequest>,
) -> Result<Response, ApiError> {
    let harness = canonical_signin_harness(&body.harness);
    if harness.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "harness (or provider) is required",
        ));
    }
    start_native_signin_stream(principal, state, harness, body.profile).await
}

/// `POST /v1/harnesses/{id}/sign-in`: delegates to the canonical native sign-in stream.
async fn sign_in_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<SignInQuery>,
) -> Result<Response, ApiError> {
    start_native_signin_stream(
        principal,
        state,
        canonical_signin_harness(&id),
        query.profile,
    )
    .await
}

async fn start_native_signin_stream(
    principal: Principal,
    state: AppState,
    id: String,
    profile_opt: Option<String>,
) -> Result<Response, ApiError> {
    require_operator(&principal)?;
    let profile = profile_opt
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("default")
        .to_string();
    if profile == "."
        || profile == ".."
        || !profile
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid profile name",
        ));
    }
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(16);
    // The response streams while the sign-in runs, so the call's future ends
    // before its work: the work holds the call open for a stop (`in_flight`).
    let call = in_flight::carry();
    if serverless::is_serverless_mode() {
        let store = Arc::clone(&state.store);
        let profiles = Arc::clone(&state.profiles);
        tokio::spawn(async move {
            let _call = call;
            serverless::run_serverless_sandbox_sign_in(store, profiles, id, profile, tx).await;
        });
    } else {
        let runner = Arc::clone(&state.sign_in);
        let root = state.store.data_dir().to_path_buf();
        let mark = capability_mark(Arc::clone(&state.store));
        let spawned = tokio::task::spawn_blocking(move || {
            let _call = call;
            let mut sink = ChannelSink { tx };
            let mut guard = runner.lock().unwrap_or_else(|err| err.into_inner());
            let result = sign_in(
                &builtin_catalog(),
                &root,
                &id,
                &profile,
                &mut *guard,
                &mut sink,
            );
            let line = match &result {
                Ok(outcome) => {
                    // A new sign-in is a new instance: the harness restarts with a
                    // capability this sign-in minted, and the last one is retired.
                    if let Err(err) = mark() {
                        eprintln!("capability after sign-in: {err}");
                    }
                    SignInLine {
                        kind: "done",
                        text: outcome.account.clone().unwrap_or_default(),
                    }
                }
                Err(err) => SignInLine {
                    kind: "error",
                    text: err.to_string(),
                },
            };
            sink.send(&line);
            result
        });
        // The task sends the final line before it returns, so the stream ends only
        // after the command has finished. Dropping the handle detaches it; the
        // channel keeps the lines ordered.
        drop(spawned);
    }
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream.map(|line| Ok::<_, std::io::Error>(format!("{line}\n"))));
    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

struct ChannelSink {
    tx: tokio::sync::mpsc::Sender<String>,
}

impl ChannelSink {
    fn send(&self, line: &SignInLine) {
        let text = serde_json::to_string(line).unwrap_or_default();
        let _ = self.tx.blocking_send(text);
    }
}

impl anyagents_installer::SignInSink for ChannelSink {
    fn prompt(&mut self, prompt: &anyagents_installer::SignInPrompt) {
        let (kind, text) = match prompt {
            anyagents_installer::SignInPrompt::Link(text) => ("link", text.clone()),
            anyagents_installer::SignInPrompt::DeviceCode(text) => ("code", text.clone()),
            anyagents_installer::SignInPrompt::Prompt(text) => ("prompt", text.clone()),
        };
        self.send(&SignInLine { kind, text });
    }

    fn done(&mut self, _: &anyagents_installer::SignInOutcome) {}
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use anyagents_core::{BillingMode, NewAgent, NewUsage, Store, UsageGroupBy, UsageReport};
    use anyagents_installer::{CommandRunner, InstallError, LineSink};
    use anyagents_supervisor::HealthBoard;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt as _;
    use serde_json::{Value, json};
    use tower::ServiceExt as _;

    use super::*;
    use crate::create::{ProviderProfileEntry, QuotaWindow, parse_quota_windows_from_usage};
    use crate::server::{build_router, profiles::ProfileSnapshots};

    const OP_TOKEN: &str = "ops-unit-test-operator-token-0123456789";

    struct FakeSignInRunner;
    impl CommandRunner for FakeSignInRunner {
        fn run(
            &mut self,
            _program: &str,
            _args: &[String],
            _dir: &Path,
            _env: &[(&str, &str)],
            on_line: &mut dyn LineSink,
        ) -> Result<(), InstallError> {
            on_line.line("Open https://auth.example.com/device and enter code ABCD-EFGH");
            Ok(())
        }
    }

    fn make_app(store: Arc<Store>, profiles: Arc<ProfileSnapshots>) -> axum::Router {
        let state = AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(FakeSignInRunner)),
            auth: auth::Mode::Confined {
                operator_digest: Some(auth::operator_digest(OP_TOKEN)),
            },
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles,
        };
        build_router(state)
    }

    #[test]
    fn openai_and_all_provider_5h_and_weekly_quota_parsing() {
        // OpenAI/Codex payload with BOTH primary_window (5h) and secondary_window (7d).
        let openai_val = json!({
            "plan_type": "pro",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 25.0,
                    "limit_window_seconds": 18000,
                    "reset_after_seconds": 7200
                },
                "secondary_window": {
                    "used_percent": 40.0,
                    "limit_window_seconds": 604800,
                    "reset_after_seconds": 259200
                }
            }
        });
        let summary = crate::create::format_openai_usage_summary(&openai_val).unwrap();
        assert!(summary.contains("5h: 75% left ↻2h"), "{summary}");
        assert!(summary.contains("7d: 60% left ↻3d"), "{summary}");
        assert!(summary.ends_with(" (pro)"), "{summary}");
        let (q5, q7) = parse_quota_windows_from_usage(&summary);
        assert_eq!(
            q5,
            Some(QuotaWindow {
                remaining_pct: 75,
                reset_eta: Some("2h".into())
            })
        );
        assert_eq!(
            q7,
            Some(QuotaWindow {
                remaining_pct: 60,
                reset_eta: Some("3d".into())
            })
        );

        // Claude/Anthropic & Google Antigravity summary strings also parse cleanly into both windows.
        let (c5, c7) =
            parse_quota_windows_from_usage("5h: 82% left ↻1h30m · 7d: 45% left ↻4d02h (max 20x)");
        assert_eq!(c5.unwrap().remaining_pct, 82);
        assert_eq!(c7.unwrap().remaining_pct, 45);

        let (g5, g7) = parse_quota_windows_from_usage(
            "5h: 95% left ↻45m · 7d: 70% left ↻5d · opus: 70% left (ultra)",
        );
        assert_eq!(g5.unwrap().remaining_pct, 95);
        assert_eq!(g7.unwrap().remaining_pct, 70);
    }

    #[tokio::test]
    async fn profiles_fresh_1_bypasses_cache_and_returns_5h_and_weekly_across_providers() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let seen_fresh_flags = Arc::new(Mutex::new(Vec::<bool>::new()));
        let flags_ref = Arc::clone(&seen_fresh_flags);

        let snapshots = Arc::new(ProfileSnapshots::with_refresh_for_test(Arc::new(
            move |_, _, fresh| {
                flags_ref.lock().unwrap().push(fresh);
                let pct5 = if fresh { 91 } else { 50 };
                let pct7 = if fresh { 84 } else { 40 };
                vec![
                    ProviderProfileEntry {
                        provider: "anthropic".into(),
                        profile: "default".into(),
                        account: Some("claude@example.com".into()),
                        kind: "subscription".into(),
                        compatible_harnesses: vec!["claude-code".into()],
                        assigned_agents: vec![],
                        subagent_count: 0,
                        usage: Some(format!("5h: {pct5}% left ↻2h · 7d: {pct7}% left ↻3d (max)")),
                        quota_5h: None,
                        quota_weekly: None,
                    },
                    ProviderProfileEntry {
                        provider: "openai".into(),
                        profile: "default".into(),
                        account: Some("codex@example.com".into()),
                        kind: "subscription".into(),
                        compatible_harnesses: vec!["codex".into()],
                        assigned_agents: vec![],
                        subagent_count: 0,
                        usage: Some(format!("5h: {pct5}% left ↻1h · 7d: {pct7}% left ↻4d (pro)")),
                        quota_5h: None,
                        quota_weekly: None,
                    },
                    ProviderProfileEntry {
                        provider: "google-antigravity".into(),
                        profile: "default".into(),
                        account: Some("agy@example.com".into()),
                        kind: "subscription".into(),
                        compatible_harnesses: vec!["agy".into()],
                        assigned_agents: vec![],
                        subagent_count: 0,
                        usage: Some(format!(
                            "5h: {pct5}% left · 7d: {pct7}% left · opus: {pct7}% left (ultra)"
                        )),
                        quota_5h: None,
                        quota_weekly: None,
                    },
                ]
            },
        )));
        let app = make_app(store, snapshots);

        // Initial cached call (fresh=false)
        let r1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/profiles")
                    .header(operator::HEADER, OP_TOKEN)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r1.status(), StatusCode::OK);
        let list1: Vec<Value> =
            serde_json::from_slice(&r1.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(list1.len(), 3);
        assert_eq!(list1[0]["quota_5h"]["remaining_pct"], json!(50));
        assert_eq!(list1[0]["quota_weekly"]["remaining_pct"], json!(40));

        // Fresh call (`?fresh=1`) bypasses cache and returns updated 5h + Weekly across ALL providers!
        let r2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/profiles?fresh=1")
                    .header(operator::HEADER, OP_TOKEN)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r2.status(), StatusCode::OK);
        let list2: Vec<Value> =
            serde_json::from_slice(&r2.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(list2.len(), 3);
        for item in &list2 {
            assert_eq!(item["quota_5h"]["remaining_pct"], json!(91), "{item}");
            assert_eq!(item["quota_weekly"]["remaining_pct"], json!(84), "{item}");
        }
        assert_eq!(*seen_fresh_flags.lock().unwrap(), vec![false, true]);
    }

    #[tokio::test]
    async fn profiles_native_signin_route_requires_operator_and_streams_ndjson() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        store
            .hold_capability_key("ops-signin-cap-key-0123456789abcdef")
            .unwrap();
        let ag = store
            .create_agent(NewAgent::new("regular", "codex"))
            .unwrap();
        let cap = store.issue_capability(&ag.id).unwrap();
        let app = make_app(store, Arc::new(ProfileSnapshots::default()));

        // Agent caller is forbidden (403).
        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/profiles/native-signin")
                    .header("content-type", "application/json")
                    .header(operator::AGENT_HEADER, &cap)
                    .body(Body::from(r#"{"harness":"codex","profile":"acct-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        // Operator caller succeeds and receives NDJSON stream with link/code + terminal line.
        let ok = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/profiles/native-signin")
                    .header("content-type", "application/json")
                    .header(operator::HEADER, OP_TOKEN)
                    .body(Body::from(r#"{"harness":"openai","profile":"acct-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(
            ok.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/x-ndjson")
        );
        let bytes = ok.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("\"kind\""), "expected NDJSON stream: {body}");
    }

    #[tokio::test]
    async fn usage_group_by_profile_rolls_up_agents_sharing_a_profile() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let a1 = store
            .create_agent(NewAgent::new("alpha", "claude-code"))
            .unwrap();
        let a2 = store
            .create_agent(NewAgent::new("beta", "claude-code"))
            .unwrap();
        let a3 = store.create_agent(NewAgent::new("gamma", "codex")).unwrap();

        // Bind alpha and beta to anthropic/acct-1, gamma to openai/acct-2.
        for (ag, prof) in [
            (&a1, "anthropic/acct-1\n"),
            (&a2, "anthropic/acct-1\n"),
            (&a3, "openai/acct-2\n"),
        ] {
            let cfg = anyagents_core::agent_config_dir(dir.path(), &ag.id);
            std::fs::create_dir_all(&cfg).unwrap();
            std::fs::write(cfg.join(".anyagents-profile"), prof).unwrap();
        }

        store
            .record_usage(NewUsage {
                agent_id: a1.id.clone(),
                harness: "claude-code".into(),
                model: Some("claude-sonnet-4-6".into()),
                input_tokens: 1000,
                output_tokens: 400,
                cache_read_tokens: 50,
                cache_write_tokens: 10,
                reported_cost_micros: Some(100_000),
                estimated_cost_micros: None,
                billing_mode: BillingMode::Subscription,
                quota_state: None,
                day: "2026-11-20".into(),
            })
            .unwrap();
        store
            .record_usage(NewUsage {
                agent_id: a2.id.clone(),
                harness: "claude-code".into(),
                model: Some("claude-sonnet-4-6".into()),
                input_tokens: 2000,
                output_tokens: 600,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                reported_cost_micros: Some(250_000),
                estimated_cost_micros: None,
                billing_mode: BillingMode::Subscription,
                quota_state: None,
                day: "2026-11-20".into(),
            })
            .unwrap();
        store
            .record_usage(NewUsage {
                agent_id: a3.id.clone(),
                harness: "codex".into(),
                model: Some("gpt-5".into()),
                input_tokens: 500,
                output_tokens: 200,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                reported_cost_micros: Some(50_000),
                estimated_cost_micros: None,
                billing_mode: BillingMode::Subscription,
                quota_state: None,
                day: "2026-11-20".into(),
            })
            .unwrap();

        let app = make_app(store, Arc::new(ProfileSnapshots::default()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/usage?group_by=profile")
                    .header(operator::HEADER, OP_TOKEN)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let report: UsageReport =
            serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(report.group_by, UsageGroupBy::Profile);
        assert_eq!(report.profiles.len(), 2);

        let p_anth = &report.profiles[0];
        assert_eq!(p_anth.profile, "anthropic/acct-1");
        assert_eq!(p_anth.totals.turns, 2);
        assert_eq!(p_anth.totals.input_tokens, 3000);
        assert_eq!(p_anth.totals.output_tokens, 1000);
        assert_eq!(p_anth.totals.reported_cost_micros, Some(350_000));
        assert_eq!(p_anth.agent_names.len(), 2);

        let p_oai = &report.profiles[1];
        assert_eq!(p_oai.profile, "openai/acct-2");
        assert_eq!(p_oai.totals.turns, 1);
        assert_eq!(p_oai.totals.input_tokens, 500);
        assert_eq!(p_oai.totals.reported_cost_micros, Some(50_000));
    }
}
