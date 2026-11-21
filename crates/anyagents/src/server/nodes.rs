//! Coordinator REST API and outbound WebSocket reverse-tunnel for native
//! Remote User-Device / Edge Nodes (`anyagents node`).

use std::sync::Arc;

use anyagents_core::masking::mask_known_secrets;
use anyagents_core::{
    AgentId, Error, NODE_CAPABILITY_EXEC, NewNodeAuditEvent, NewNodePair, NodeId, NodeRecord,
    is_node_token, now_ms,
};
use axum::extract::{FromRequestParts, Path as AxumPath, Query, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth::{
    AuthError, AuthRejection, Principal, RequestAuthority, authenticate_request, off_worker,
};
use crate::node_protocol::{
    DEFAULT_EXEC_TIMEOUT_SECS, MAX_EXEC_TIMEOUT_SECS, MAX_TUNNEL_FRAME_BYTES,
};
use crate::oauth_scope;

use super::types::{ApiError, AppState, require_operator, spawn_store};

mod tunnel;
pub(crate) use tunnel::registry_for_store;

/// Public-listener node routes: device pairing (OIDC `node:connect` / `admin:*`)
/// and outbound WebSocket reverse-tunnel (`Authorization: Bearer ndt_...`).
pub(super) fn public_router() -> Router<AppState> {
    Router::new()
        .route("/v1/nodes/pair", post(pair_node_route))
        .route("/v1/nodes/connect", get(connect_node_ws_route))
}

/// Full authenticated node control-plane routes.
pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/nodes", get(list_nodes_route))
        .route("/v1/nodes/{id}", get(get_node_route))
        .route("/v1/nodes/{id}/pause", post(pause_node_route))
        .route("/v1/nodes/{id}/resume", post(resume_node_route))
        .route("/v1/nodes/{id}/revoke", post(revoke_node_route))
        .route(
            "/v1/nodes/{id}/grants",
            get(list_grants_route).post(create_grant_route),
        )
        .route(
            "/v1/nodes/{id}/grants/{grant_id}",
            delete(revoke_grant_route),
        )
        .route("/v1/nodes/{id}/audit", get(list_audit_route))
        .route("/v1/nodes/{id}/exec", post(exec_node_route))
}

struct PairActor {
    owner_id: String,
}

impl FromRequestParts<AppState> for PairActor {
    type Rejection = AuthRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        match authenticate_request(parts, state).await? {
            RequestAuthority::Principal(Principal::Operator) => Ok(Self {
                owner_id: "operator".into(),
            }),
            RequestAuthority::Bearer(bearer)
                if bearer.has_scope(oauth_scope::NODE_CONNECT)
                    || bearer.has_scope(oauth_scope::ADMIN) =>
            {
                let owner_id = bearer
                    .identity
                    .email
                    .clone()
                    .unwrap_or_else(|| format!("oidc:{}", bearer.identity.sub));
                Ok(Self { owner_id })
            }
            RequestAuthority::Bearer(_) => Err(AuthRejection(AuthError::Forbidden(
                "node pairing requires OAuth scope `node:connect` or `admin:*`".into(),
            ))),
            RequestAuthority::Principal(Principal::Agent { .. }) => Err(AuthRejection(
                AuthError::Forbidden("agent sessions cannot pair edge nodes".into()),
            )),
        }
    }
}

enum NodeControlActor {
    Operator(String),
    NodeSelf(NodeRecord),
    Agent(AgentId),
}

fn bearer_from_headers(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let trimmed = token.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

impl FromRequestParts<AppState> for NodeControlActor {
    type Rejection = AuthRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(token) = bearer_from_headers(&parts.headers)
            && is_node_token(&token)
        {
            let store = Arc::clone(&state.store);
            let node = off_worker(move || {
                store
                    .authenticate_node_token(&token)
                    .map_err(|err| match err {
                        Error::PermissionDenied(msg) => AuthError::Forbidden(msg),
                        Error::InvalidCredential(msg) => AuthError::Unauthenticated(msg),
                        other => AuthError::Internal(other.to_string()),
                    })
            })
            .await?;
            return Ok(Self::NodeSelf(node));
        }
        match authenticate_request(parts, state).await? {
            RequestAuthority::Principal(Principal::Operator) => {
                Ok(Self::Operator("operator".into()))
            }
            RequestAuthority::Bearer(bearer) if bearer.has_scope(oauth_scope::ADMIN) => {
                let id = bearer
                    .identity
                    .email
                    .unwrap_or_else(|| format!("oidc:{}", bearer.identity.sub));
                Ok(Self::Operator(id))
            }
            RequestAuthority::Bearer(_) => Err(AuthRejection(AuthError::Forbidden(
                "this endpoint requires OAuth scope `admin:*`".into(),
            ))),
            RequestAuthority::Principal(Principal::Agent { agent_id, .. }) => {
                Ok(Self::Agent(agent_id))
            }
        }
    }
}

fn parse_node_id(raw: &str) -> Result<NodeId, ApiError> {
    NodeId::parse(raw.trim()).map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "invalid node_id"))
}

fn map_node_store_err(err: Error) -> ApiError {
    match &err {
        Error::PermissionDenied(_) => ApiError::forbidden(err.to_string()),
        Error::StateConflict { .. } => ApiError::conflict(err.to_string()),
        Error::AgentNotFound(_) => ApiError::new(StatusCode::NOT_FOUND, err.to_string()),
        Error::Message(msg) if msg.starts_with("node not found:") => {
            ApiError::new(StatusCode::NOT_FOUND, err.to_string())
        }
        _ => ApiError::from_store(err),
    }
}

#[derive(Deserialize)]
struct PairNodeRequest {
    name: String,
    hostname: String,
    os: String,
    arch: String,
    #[serde(default)]
    capabilities: Option<Vec<String>>,
}

async fn pair_node_route(
    actor: PairActor,
    State(state): State<AppState>,
    Json(req): Json<PairNodeRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let capabilities = req
        .capabilities
        .unwrap_or_else(|| vec![NODE_CAPABILITY_EXEC.to_string()]);
    let store = Arc::clone(&state.store);
    let paired = spawn_store(move || {
        store
            .pair_node(NewNodePair {
                owner_id: actor.owner_id,
                name: req.name,
                hostname: req.hostname,
                os: req.os,
                arch: req.arch,
                capabilities,
            })
            .map_err(map_node_store_err)
    })
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "node_id": paired.node.id,
            "node_token": paired.node_token,
            "node": paired.node,
        })),
    ))
}

async fn connect_node_ws_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(token) = bearer_from_headers(&headers) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "missing Authorization: Bearer <node_token>" })),
        )
            .into_response();
    };
    let store = Arc::clone(&state.store);
    let auth_res = spawn_store(move || {
        store
            .authenticate_node_token(&token)
            .map_err(|err| match err {
                Error::PermissionDenied(msg) => ApiError::forbidden(msg),
                Error::InvalidCredential(msg) => ApiError::new(StatusCode::UNAUTHORIZED, msg),
                other => ApiError::from_store(other),
            })
    })
    .await;
    let node = match auth_res {
        Ok(node) => node,
        Err(api_err) => return api_err.into_response(),
    };
    let registry = registry_for_store(&state.store);
    let store = Arc::clone(&state.store);
    ws.max_message_size(MAX_TUNNEL_FRAME_BYTES)
        .max_frame_size(MAX_TUNNEL_FRAME_BYTES)
        .on_upgrade(move |socket| tunnel::run_node_tunnel(socket, registry, store, node.id))
}

async fn list_nodes_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let store = Arc::clone(&state.store);
    let nodes = spawn_store(move || match principal {
        Principal::Operator => store.list_nodes().map_err(map_node_store_err),
        Principal::Agent { agent_id, .. } => store
            .list_nodes_for_agent(&agent_id)
            .map_err(map_node_store_err),
    })
    .await?;
    Ok(Json(json!({ "nodes": nodes })))
}

async fn get_node_route(
    actor: NodeControlActor,
    State(state): State<AppState>,
    AxumPath(raw_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let node_id = parse_node_id(&raw_id)?;
    let store = Arc::clone(&state.store);
    spawn_store(move || match actor {
        NodeControlActor::Operator(_) => {
            let node = store
                .get_node(&node_id)
                .map_err(map_node_store_err)?
                .ok_or_else(|| {
                    ApiError::new(StatusCode::NOT_FOUND, format!("node not found: {node_id}"))
                })?;
            let grants = store
                .list_node_grants(&node_id)
                .map_err(map_node_store_err)?;
            let audit = store
                .list_node_audit_events(&node_id, 50)
                .map_err(map_node_store_err)?;
            Ok(Json(json!({
                "node": node,
                "grants": grants,
                "audit_events": audit,
            })))
        }
        NodeControlActor::NodeSelf(self_node) => {
            if self_node.id != node_id {
                return Err(ApiError::forbidden(
                    "node token is scoped to its own node_id",
                ));
            }
            Ok(Json(json!({ "node": self_node })))
        }
        NodeControlActor::Agent(agent_id) => {
            let allowed = store
                .agent_has_any_node_grant(&node_id, &agent_id)
                .map_err(map_node_store_err)?;
            if !allowed {
                return Err(ApiError::forbidden(
                    "agent has no active grant on this node",
                ));
            }
            let node = store
                .get_node(&node_id)
                .map_err(map_node_store_err)?
                .ok_or_else(|| {
                    ApiError::new(StatusCode::NOT_FOUND, format!("node not found: {node_id}"))
                })?;
            Ok(Json(json!({ "node": node })))
        }
    })
    .await
}

fn authorize_node_control(actor: NodeControlActor, node_id: &NodeId) -> Result<String, ApiError> {
    match actor {
        NodeControlActor::Operator(who) => Ok(who),
        NodeControlActor::NodeSelf(node) if &node.id == node_id => Ok(format!("node:{}", node.id)),
        NodeControlActor::NodeSelf(_) => Err(ApiError::forbidden(
            "node token is scoped to its own node_id",
        )),
        NodeControlActor::Agent(_) => {
            Err(ApiError::forbidden("agents cannot control node lifecycle"))
        }
    }
}

async fn pause_node_route(
    actor: NodeControlActor,
    State(state): State<AppState>,
    AxumPath(raw_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let node_id = parse_node_id(&raw_id)?;
    let who = authorize_node_control(actor, &node_id)?;
    let store = Arc::clone(&state.store);
    let nid = node_id.clone();
    let node =
        spawn_store(move || store.pause_node(&nid, &who).map_err(map_node_store_err)).await?;
    registry_for_store(&state.store)
        .pause_node(&node_id, "node paused via kill-switch")
        .await;
    Ok(Json(json!({ "node": node })))
}

async fn resume_node_route(
    actor: NodeControlActor,
    State(state): State<AppState>,
    AxumPath(raw_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let node_id = parse_node_id(&raw_id)?;
    let who = authorize_node_control(actor, &node_id)?;
    let registry = registry_for_store(&state.store);
    let connected = registry.is_connected(&node_id).await;
    let store = Arc::clone(&state.store);
    let nid = node_id.clone();
    let node = spawn_store(move || {
        store
            .resume_node(&nid, connected, &who)
            .map_err(map_node_store_err)
    })
    .await?;
    if connected {
        registry.resume_node(&node_id).await;
    }
    Ok(Json(json!({ "node": node })))
}

async fn revoke_node_route(
    actor: NodeControlActor,
    State(state): State<AppState>,
    AxumPath(raw_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let node_id = parse_node_id(&raw_id)?;
    let who = authorize_node_control(actor, &node_id)?;
    let store = Arc::clone(&state.store);
    let nid = node_id.clone();
    let node =
        spawn_store(move || store.revoke_node(&nid, &who).map_err(map_node_store_err)).await?;
    registry_for_store(&state.store)
        .revoke_node(&node_id, "node revoked")
        .await;
    Ok(Json(json!({ "node": node })))
}

async fn list_grants_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(raw_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    require_operator(&principal)?;
    let node_id = parse_node_id(&raw_id)?;
    let store = Arc::clone(&state.store);
    let grants = spawn_store(move || {
        if store
            .get_node(&node_id)
            .map_err(map_node_store_err)?
            .is_none()
        {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                format!("node not found: {node_id}"),
            ));
        }
        store.list_node_grants(&node_id).map_err(map_node_store_err)
    })
    .await?;
    Ok(Json(json!({ "grants": grants })))
}

#[derive(Deserialize)]
struct CreateGrantRequest {
    agent_id: String,
    #[serde(default = "default_exec_cap")]
    capability: String,
}

fn default_exec_cap() -> String {
    NODE_CAPABILITY_EXEC.to_string()
}

async fn create_grant_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(raw_id): AxumPath<String>,
    Json(req): Json<CreateGrantRequest>,
) -> Result<impl IntoResponse, ApiError> {
    require_operator(&principal)?;
    let node_id = parse_node_id(&raw_id)?;
    let agent_id = AgentId::parse(req.agent_id.trim())
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "invalid agent_id"))?;
    let store = Arc::clone(&state.store);
    let grant = spawn_store(move || {
        store
            .grant_node_capability(&node_id, &agent_id, &req.capability, "operator")
            .map_err(map_node_store_err)
    })
    .await?;
    Ok((StatusCode::CREATED, Json(json!({ "grant": grant }))))
}

async fn revoke_grant_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath((raw_id, grant_id)): AxumPath<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_operator(&principal)?;
    let node_id = parse_node_id(&raw_id)?;
    let store = Arc::clone(&state.store);
    let revoked = spawn_store(move || {
        store
            .revoke_node_grant(&node_id, grant_id.trim(), "operator")
            .map_err(map_node_store_err)
    })
    .await?;
    Ok(Json(json!({ "revoked": revoked })))
}

#[derive(Deserialize)]
struct AuditQuery {
    limit: Option<usize>,
}

async fn list_audit_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(raw_id): AxumPath<String>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<Value>, ApiError> {
    require_operator(&principal)?;
    let node_id = parse_node_id(&raw_id)?;
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let store = Arc::clone(&state.store);
    let events = spawn_store(move || {
        if store
            .get_node(&node_id)
            .map_err(map_node_store_err)?
            .is_none()
        {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                format!("node not found: {node_id}"),
            ));
        }
        store
            .list_node_audit_events(&node_id, limit)
            .map_err(map_node_store_err)
    })
    .await?;
    Ok(Json(json!({ "events": events })))
}

#[derive(Deserialize)]
pub(crate) struct ExecNodeRequest {
    pub(crate) command: String,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    #[serde(default)]
    pub(crate) cwd: Option<String>,
    #[serde(default)]
    pub(crate) timeout_secs: Option<u64>,
}

#[derive(Serialize)]
struct ExecNodeResponse {
    exec_id: String,
    node_id: NodeId,
    exit_code: Option<i32>,
    timed_out: bool,
    cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    stdout: String,
    stderr: String,
    stdout_truncated: bool,
    stderr_truncated: bool,
    duration_ms: u64,
}

async fn exec_node_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(raw_id): AxumPath<String>,
    Json(req): Json<ExecNodeRequest>,
) -> Result<Json<ExecNodeResponse>, ApiError> {
    let node_id = parse_node_id(&raw_id)?;
    let command = req.command.trim().to_string();
    if command.is_empty() || command.contains('\0') {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "exec `command` must be non-empty",
        ));
    }
    if req.args.iter().any(|a| a.contains('\0'))
        || req.cwd.as_deref().is_some_and(|c| c.contains('\0'))
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "exec arguments must not contain NUL bytes",
        ));
    }
    let timeout_secs = req
        .timeout_secs
        .unwrap_or(DEFAULT_EXEC_TIMEOUT_SECS)
        .clamp(1, MAX_EXEC_TIMEOUT_SECS);
    let actor_agent_id = match &principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, .. } => Some(agent_id.clone()),
    };
    let summary = format_command_summary(&command, &req.args, req.cwd.as_deref());
    let started_at_ms = now_ms();

    let store_auth = Arc::clone(&state.store);
    let nid_auth = node_id.clone();
    let agent_auth = actor_agent_id.clone();
    let summary_auth = summary.clone();
    spawn_store(move || {
        match store_auth.check_node_capability_authorized(
            &nid_auth,
            agent_auth.as_ref(),
            NODE_CAPABILITY_EXEC,
        ) {
            Ok(_) => Ok(()),
            Err(err) => {
                if store_auth.get_node(&nid_auth).ok().flatten().is_some() {
                    let _ = store_auth.record_node_audit_event(NewNodeAuditEvent {
                        node_id: nid_auth,
                        agent_id: agent_auth.map(|id| id.to_string()),
                        capability: NODE_CAPABILITY_EXEC.into(),
                        action: "exec".into(),
                        summary: summary_auth,
                        exit_code: None,
                        outcome: "denied".into(),
                        started_at_ms,
                        finished_at_ms: Some(now_ms()),
                    });
                }
                Err(map_node_store_err(err))
            }
        }
    })
    .await?;

    let exec_id = format!(
        "nex_{}",
        anyagents_core::MessageId::generate()
            .as_str()
            .trim_start_matches("msg_")
    );
    let registry = registry_for_store(&state.store);
    let outcome = registry
        .dispatch_exec(
            &node_id,
            exec_id.clone(),
            command,
            req.args,
            req.cwd,
            timeout_secs,
        )
        .await;

    let finished_at_ms = now_ms();
    match outcome {
        Ok(res) => {
            let outcome_label = if res.timed_out {
                "timed_out"
            } else if res.cancelled {
                "cancelled"
            } else if res.exit_code == Some(0) {
                "ok"
            } else {
                "failed"
            };
            let store_audit = Arc::clone(&state.store);
            let nid_audit = node_id.clone();
            let agent_audit = actor_agent_id.map(|id| id.to_string());
            let exit_code = res.exit_code.map(i64::from);
            let _ = spawn_store(move || {
                store_audit
                    .record_node_audit_event(NewNodeAuditEvent {
                        node_id: nid_audit,
                        agent_id: agent_audit,
                        capability: NODE_CAPABILITY_EXEC.into(),
                        action: "exec".into(),
                        summary,
                        exit_code,
                        outcome: outcome_label.into(),
                        started_at_ms,
                        finished_at_ms: Some(finished_at_ms),
                    })
                    .map_err(map_node_store_err)
            })
            .await;

            Ok(Json(ExecNodeResponse {
                exec_id,
                node_id,
                exit_code: res.exit_code,
                timed_out: res.timed_out,
                cancelled: res.cancelled,
                error: res.error.map(|e| mask_known_secrets(&e).into_owned()),
                stdout: mask_known_secrets(&res.stdout).into_owned(),
                stderr: mask_known_secrets(&res.stderr).into_owned(),
                stdout_truncated: res.stdout_truncated,
                stderr_truncated: res.stderr_truncated,
                duration_ms: res.duration_ms,
            }))
        }
        Err(why) => {
            let store_audit = Arc::clone(&state.store);
            let nid_audit = node_id.clone();
            let agent_audit = actor_agent_id.map(|id| id.to_string());
            let _ = spawn_store(move || {
                store_audit
                    .record_node_audit_event(NewNodeAuditEvent {
                        node_id: nid_audit,
                        agent_id: agent_audit,
                        capability: NODE_CAPABILITY_EXEC.into(),
                        action: "exec".into(),
                        summary,
                        exit_code: None,
                        outcome: "unavailable".into(),
                        started_at_ms,
                        finished_at_ms: Some(finished_at_ms),
                    })
                    .map_err(map_node_store_err)
            })
            .await;
            Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, why))
        }
    }
}

fn format_command_summary(command: &str, args: &[String], cwd: Option<&str>) -> String {
    let mut parts = Vec::with_capacity(args.len() + 1);
    parts.push(command.to_string());
    parts.extend(args.iter().cloned());
    let joined = parts.join(" ");
    let with_cwd = match cwd {
        Some(dir) if !dir.trim().is_empty() => format!("[cwd={}] {joined}", dir.trim()),
        _ => joined,
    };
    let masked = mask_known_secrets(&with_cwd);
    if masked.len() > 512 {
        let mut end = 512;
        while !masked.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &masked[..end])
    } else {
        masked.into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{NewAgent, Store};
    use anyagents_supervisor::HealthBoard;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::Mutex;
    use tower::ServiceExt as _;

    #[tokio::test]
    async fn node_rest_enforces_agent_and_node_self_boundaries() {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        let agent = store
            .create_agent(NewAgent::new("agent-a", "codex"))
            .unwrap();
        let agent_cap = store.issue_capability(&agent.id).unwrap();
        let node_a = store
            .pair_node(NewNodePair {
                owner_id: "owner".into(),
                name: "node-a".into(),
                hostname: "a.local".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                capabilities: vec!["exec".into()],
            })
            .unwrap();
        let node_b = store
            .pair_node(NewNodePair {
                owner_id: "owner".into(),
                name: "node-b".into(),
                hostname: "b.local".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                capabilities: vec!["exit".into()],
            })
            .unwrap();

        let state = AppState {
            store: Arc::clone(&store),
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(super::super::types::ProcessRunner)),
            auth: crate::auth::Mode::Confined {
                operator_digest: Some(crate::auth::operator_digest("op-secret-0123456789")),
            },
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        };
        let app = crate::server::build_router(state);

        // Agent cannot pair, pause, grant, or read audit.
        for (method, path, body) in [
            (
                "POST",
                "/v1/nodes/pair".to_string(),
                r#"{"name":"x","hostname":"h","os":"linux","arch":"x86_64"}"#,
            ),
            ("POST", format!("/v1/nodes/{}/pause", node_a.node.id), "{}"),
            (
                "POST",
                format!("/v1/nodes/{}/grants", node_a.node.id),
                r#"{"agent_id":"agt_01h455vb4pex5vsknk084sn02q","capability":"exec"}"#,
            ),
            ("GET", format!("/v1/nodes/{}/audit", node_a.node.id), ""),
        ] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header("x-anyagents-agent", &agent_cap)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        }

        // NodeSelf token can pause/resume its own node, never another node or grants.
        let own_pause = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/nodes/{}/pause", node_a.node.id))
                    .header("authorization", format!("Bearer {}", node_a.node_token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(own_pause.status(), StatusCode::OK);

        let cross_pause = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/nodes/{}/pause", node_b.node.id))
                    .header("authorization", format!("Bearer {}", node_a.node_token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_pause.status(), StatusCode::FORBIDDEN);

        let cross_grant = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/nodes/{}/grants", node_a.node.id))
                    .header("authorization", format!("Bearer {}", node_a.node_token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_grant.status(), StatusCode::UNAUTHORIZED);
    }
}
