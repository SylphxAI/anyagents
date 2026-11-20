//! Generic event webhooks and Streamable HTTP MCP transport routes.

use std::path::PathBuf;
use std::sync::Arc;

use anyagents_core::{Agent, Store};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::auth::Principal;

use super::bootstrap::socket_path;
use super::types::{ApiError, AppState, spawn_store};
use super::{build_router, in_flight, serverless};

pub(super) fn public_router() -> Router<AppState> {
    Router::new()
        .route("/v1/webhooks/events/{*topic}", post(event_webhook))
        .route(
            "/v1/webhooks/mcp",
            get(public_mcp_get_route).post(public_mcp_post_route),
        )
}

pub(super) fn local_router() -> Router<AppState> {
    Router::new()
        .route("/v1/webhooks/events/{*topic}", post(event_webhook))
        .route("/v1/webhooks/mcp", get(mcp_get_route).post(mcp_post_route))
        .route("/v1/mcp", get(mcp_get_route).post(mcp_post_route))
        .route("/mcp", get(mcp_get_route).post(mcp_post_route))
}

/// `POST /v1/webhooks/events/{*topic}`: generic authenticated event trigger for
/// `webhook:<topic>` / `event:<topic>` watches.
///
/// Authenticates raw request bytes against `[server].webhook_secret` (or
/// `ANYAGENTS_WEBHOOK_SECRET`) via HMAC-SHA256 (`x-anyagents-signature-256` or
/// `x-hub-signature-256`) or `Authorization: Bearer <secret>`.
async fn event_webhook(
    State(state): State<AppState>,
    AxumPath(topic): AxumPath<String>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let settings = match crate::remote::Settings::load(state.store.data_dir()) {
        Ok(settings) => settings,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let Some(secret) = settings.webhook_secret() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let hmac_valid = headers
        .get("x-anyagents-signature-256")
        .or_else(|| headers.get("x-hub-signature-256"))
        .and_then(|v| v.to_str().ok())
        .is_some_and(|sig| {
            anyagents_core::verify_hmac_sha256_signature(secret.as_bytes(), &body, sig)
        });
    let bearer_valid = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| {
            let trimmed = raw.trim();
            trimmed
                .strip_prefix("Bearer ")
                .or_else(|| trimmed.strip_prefix("bearer "))
        })
        .map(str::trim)
        .is_some_and(|token| anyagents_core::constant_time_eq(token.as_bytes(), secret.as_bytes()));
    if !hmac_valid && !bearer_valid {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(val) => val,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let delivery_id = headers
        .get("x-anyagents-delivery")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("evt-{}-{}", anyagents_core::now_ms(), body.len()));
    let topic = topic.trim().trim_start_matches('/').to_string();
    if topic.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let store = Arc::clone(&state.store);
    let dispatch_topic = topic.clone();
    let dispatch = match spawn_store(move || {
        crate::schedule::fire_webhook_event(
            &store,
            &dispatch_topic,
            &delivery_id,
            &payload,
            anyagents_core::now_ms(),
        )
        .map_err(ApiError::from_store)
    })
    .await
    {
        Ok(dispatch) => dispatch,
        Err(err) => return err.into_response(),
    };
    if serverless::is_serverless_mode() {
        for agent_id in &dispatch.fired_agents {
            if let Err(err) = serverless::dispatch_next_mailbox_message_if_serverless(
                Arc::clone(&state.store),
                agent_id,
            )
            .await
            {
                eprintln!(
                    "anyagents-serverless: webhook schedule dispatch failed for {agent_id}: {err}"
                );
            }
        }
    }
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "accepted": true,
            "topic": topic,
            "matched": dispatch.matched,
        })),
    )
        .into_response()
}

/// Which listener an MCP call arrived on. Every listener takes remote session
/// tokens only; the public one also answers anything else with a plain 401,
/// so a network peer never learns whether a token is a valid local capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum McpListener {
    Local,
    Public,
}

/// Who made one MCP call, and with which kind of credential.
struct McpCaller {
    agent: Agent,
    /// The remote session the presented token belongs to.
    remote: Option<anyagents_core::RemoteSessionGrant>,
}

/// Authenticate one MCP call over HTTP. Only a remote session token (bound to
/// one agent and session, unexpired, unrevoked) is accepted. A valid local
/// mailbox capability is refused with 403, and anything else with 401.
fn authenticate_mcp_request(
    store: &Store,
    headers: &axum::http::HeaderMap,
    listener: McpListener,
) -> Result<McpCaller, ApiError> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| {
            let trimmed = raw.trim();
            trimmed
                .strip_prefix("Bearer ")
                .or_else(|| trimmed.strip_prefix("bearer "))
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            headers
                .get(crate::operator::AGENT_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "missing MCP capability token (expected `Authorization: Bearer <mcp-capability>` or `x-anyagents-agent`)",
            )
        })?;
    if token.starts_with(anyagents_core::REMOTE_SESSION_TOKEN_PREFIX) {
        let unauthenticated = || {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "invalid, expired or revoked remote session token",
            )
        };
        let map_auth_err = |err: anyagents_core::Error| match err {
            anyagents_core::Error::Db(_)
            | anyagents_core::Error::Postgres(_)
            | anyagents_core::Error::Io(_) => ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily unable to verify remote session token; retry shortly",
            ),
            _ => unauthenticated(),
        };
        let grant = store
            .authenticate_remote_session(token)
            .map_err(map_auth_err)?;
        let agent = store.agent(&grant.agent_id).map_err(map_auth_err)?;
        // A remote token is for an agent that runs off this host. Consult
        // PostgreSQL `agent_sandbox_bindings` (plus local config fallback) so
        // peer replicas without pod-local `sandbox*.json` admit requests cleanly
        // and never call `operator::ensure()` (which revokes peer capabilities!).
        let config_dir = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
        if !crate::remote::runs_off_host_in(store, &agent, &config_dir) {
            return Err(unauthenticated());
        }
        return Ok(McpCaller {
            agent,
            remote: Some(grant),
        });
    }
    // The public listener never tells a network peer whether a token is a
    // valid local capability: anything but a remote session token is 401.
    if listener == McpListener::Public {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "this listener accepts only remote session tokens",
        ));
    }
    // MCP over HTTP runs the tools inside the server process, as the server's
    // own user, on every listener including the Unix socket. A local agent's
    // tools run over stdio (`anyagents mcp`) as the agent itself, so its
    // capability is never accepted here.
    if store.authenticate_agent(token).is_ok() {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "MCP over HTTP accepts only remote session tokens; a local agent's tools run over stdio \
             (`anyagents mcp`), as the agent itself",
        ));
    }
    Err(ApiError::new(
        StatusCode::UNAUTHORIZED,
        "invalid or expired MCP capability token",
    ))
}

pub(super) async fn mcp_get_route(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    mcp_get(McpListener::Local, state, headers).await
}

async fn public_mcp_get_route(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    mcp_get(McpListener::Public, state, headers).await
}

async fn mcp_get(
    listener: McpListener,
    state: AppState,
    headers: axum::http::HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    let presented = headers.clone();
    let caller =
        spawn_store(move || authenticate_mcp_request(&state.store, &presented, listener)).await?;
    let agent = &caller.agent;
    let remote_session = caller.remote.as_ref().map(|grant| {
        serde_json::json!({
            "session_id": grant.session_id,
            "expires_at_ms": grant.expires_at_ms,
        })
    });
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.contains("application/json") && !accept.contains("text/event-stream") {
        let body = serde_json::json!({
            "ok": true,
            "transport": "streamable-http",
            "protocolVersion": "2025-03-26",
            "agent": agent.name,
            "harness": agent.harness,
            "tools": crate::mcp::UNIVERSAL_MCP_TOOLS,
            "remote_session": remote_session,
        });
        return Ok(axum::response::IntoResponse::into_response(Json(body)));
    }
    let event_payload = serde_json::json!({
        "type": "endpoint",
        "transport": "streamable-http",
        "protocolVersion": "2025-03-26",
        "agent": agent.name,
    });
    let sse_body = format!(": anyagents-mcp\nevent: endpoint\ndata: {event_payload}\n\n");
    Ok(axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            "text/event-stream; charset=utf-8",
        )
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .body(axum::body::Body::from(sse_body))
        .unwrap_or_else(|_| {
            axum::response::IntoResponse::into_response(StatusCode::INTERNAL_SERVER_ERROR)
        }))
}

pub(super) const MCP_PAYLOAD_ENCODING_HEADER: &str = "x-anyagents-payload-encoding";

pub(super) fn decode_mcp_request_payload(
    headers: &axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<serde_json::Value, ApiError> {
    let raw_bytes = if headers
        .get(MCP_PAYLOAD_ENCODING_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("base64"))
    {
        use base64::Engine as _;
        let encoded = std::str::from_utf8(&body)
            .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "invalid UTF-8 base64 payload"))?
            .trim();
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "invalid base64 MCP payload"))?
    } else {
        body.to_vec()
    };
    serde_json::from_slice::<serde_json::Value>(&raw_bytes)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, format!("invalid MCP JSON: {e}")))
}

pub(super) async fn mcp_post_route(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiError> {
    let payload = decode_mcp_request_payload(&headers, body)?;
    mcp_post(McpListener::Local, state, headers, payload).await
}

async fn public_mcp_post_route(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiError> {
    let payload = decode_mcp_request_payload(&headers, body)?;
    mcp_post(McpListener::Public, state, headers, payload).await
}

fn mcp_hydration_needs(payload: &serde_json::Value) -> (bool, bool) {
    let method = payload
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if method != "tools/call" {
        return (false, false);
    }
    let tool = payload
        .pointer("/params/name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let needs_memory = tool == "memory"
        || tool == "conversation"
        || tool.starts_with("memory_")
        || tool.starts_with("conversation_");
    let action = payload
        .pointer("/params/arguments/action")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    let is_fast_read_agent_action = matches!(
        action,
        "list" | "inspect" | "status" | "get" | "info" | "show" | "models" | "catalog" | "profiles"
    );
    let needs_lease = tool == "workspace"
        || tool.starts_with("workspace_")
        || (matches!(tool, "agents" | "subagents") && !is_fast_read_agent_action);
    (needs_lease, needs_memory)
}

async fn mcp_post(
    listener: McpListener,
    state: AppState,
    headers: axum::http::HeaderMap,
    payload: serde_json::Value,
) -> Result<axum::response::Response, ApiError> {
    let (needs_lease, needs_memory) = mcp_hydration_needs(&payload);
    let mcp = spawn_store(move || {
        let caller = authenticate_mcp_request(&state.store, &headers, listener)?;
        let McpCaller { agent, .. } = caller;
        let data_dir = state.store.data_dir();
        let config_dir = anyagents_core::agent_config_dir(data_dir, &agent.id);
        let sock = socket_path(data_dir);
        let router = build_router(state.clone());
        let principal = Principal::Agent {
            agent_id: agent.id.clone(),
            name: agent.name.clone(),
        };
        let _ = std::fs::create_dir_all(&config_dir);
        // Hydrate remote sandbox lease metadata + learned memory lazily only for
        // tools that read them so fast read calls (`agents(inspect)`, `groups(list)`,
        // `schedules(list)`, `inbox`, `send`) avoid redundant PG blob round-trips.
        let hydrated_workdir = if needs_lease {
            crate::sandbox_exec::hydrate_sandbox_lease_file_from_store(
                &state.store,
                &agent.id,
                &config_dir,
            )
        } else {
            None
        };
        if needs_memory {
            let _ = crate::sandbox_exec::hydrate_agent_memory_from_store(&state.store, &agent.id);
        }
        let effective_workdir = hydrated_workdir
            .map(PathBuf::from)
            .or_else(|| agent.workdir.as_deref().map(PathBuf::from));
        Ok(crate::mcp::AgentMcp::new_for_http(
            crate::mcp::AgentRef {
                id: agent.id.clone(),
                name: agent.name.clone(),
                workdir: effective_workdir,
            },
            sock,
            config_dir,
            principal,
            router,
        ))
    })
    .await?;
    // A tool call mixes file, process and socket work into its async steps
    // (`crate::mcp`), so the whole call is driven from a blocking-pool thread:
    // whatever it waits on holds that thread, never an async worker.
    let runtime = tokio::runtime::Handle::current();
    let call = in_flight::carry();
    let answered = tokio::task::spawn_blocking(move || {
        let _call = call;
        runtime.block_on(mcp.handle_jsonrpc_request(payload))
    })
    .await
    .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    let mut resp = match answered {
        Some(response_json) => {
            axum::response::IntoResponse::into_response((StatusCode::OK, Json(response_json)))
        }
        None => axum::response::IntoResponse::into_response(StatusCode::ACCEPTED),
    };
    resp.headers_mut().insert(
        axum::http::header::HeaderName::from_static(MCP_PAYLOAD_ENCODING_HEADER),
        axum::http::HeaderValue::from_static("base64"),
    );
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mcp_hydration_skips_fast_read_tools_and_enables_memory_and_workspace_tools() {
        assert_eq!(
            mcp_hydration_needs(&json!({
                "method": "tools/call",
                "params": { "name": "agents", "arguments": { "action": "inspect", "name": "self" } }
            })),
            (false, false)
        );
        assert_eq!(
            mcp_hydration_needs(&json!({
                "method": "tools/call",
                "params": { "name": "groups", "arguments": { "action": "list" } }
            })),
            (false, false)
        );
        assert_eq!(
            mcp_hydration_needs(&json!({
                "method": "tools/call",
                "params": { "name": "schedules", "arguments": { "action": "list" } }
            })),
            (false, false)
        );
        assert_eq!(
            mcp_hydration_needs(&json!({
                "method": "tools/call",
                "params": { "name": "memory", "arguments": { "action": "search", "query": "test" } }
            })),
            (false, true)
        );
        assert_eq!(
            mcp_hydration_needs(&json!({
                "method": "tools/call",
                "params": { "name": "agents", "arguments": { "action": "spawn", "name": "worker" } }
            })),
            (true, false)
        );
    }
}
