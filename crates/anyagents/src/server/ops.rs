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
    ApiError, AppState, SignInLine, require_admin_or_operator, require_operator, spawn_store,
};
use super::{ServedProfile, in_flight, quota_rotation, serverless};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/profiles", get(profiles_route).post(set_profile_route))
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
        require_admin_or_operator(&state.store, &principal, "agents.mint_capability", &name)?;
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
        require_admin_or_operator(
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
        let records = state.store.list_usage(None).map_err(ApiError::from_store)?;
        let report =
            anyagents_core::aggregate_usage(&records, &agent_ids, &names, filter, now_ms())
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
    /// Answer with a listing made after this call rather than the snapshot.
    #[serde(default)]
    fresh: Option<bool>,
}

/// `GET /v1/profiles`: the last listing at once, each entry marked with the
/// age of its usage reading; see [`super::profiles`].
async fn profiles_route(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<ProfilesQuery>,
) -> Result<Json<Vec<ServedProfile>>, ApiError> {
    let store = Arc::clone(&state.store);
    spawn_store(move || {
        require_admin_or_operator(&store, &principal, "profiles.list", "profiles").map(drop)
    })
    .await?;
    state
        .profiles
        .list(&state.store, query.fresh.unwrap_or(false))
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
        require_admin_or_operator(&state.store, &principal, "profiles.set", &body.profile)?;
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
        require_admin_or_operator(
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

/// `POST /v1/harnesses/{id}/sign-in`. The body stays open while the vendor
/// command runs. Each link or code is one JSON line, and the last line is the
/// outcome. The credential never appears here.
async fn sign_in_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<SignInQuery>,
) -> Result<Response, ApiError> {
    require_operator(&principal)?;
    let profile = query
        .profile
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
