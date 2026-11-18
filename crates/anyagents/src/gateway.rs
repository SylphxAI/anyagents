//! The protocol gateway as the server runs it: one loopback port for the life
//! of the server, one route and one token per agent that has a provider key.
//!
//! An agent's provider key is stored once, in a 0600 file under the agent's own
//! directory (`provider.json`), and reaches only the gateway. The harness
//! process is handed the gateway's origin and that agent's own token, so the
//! key never appears in a harness configuration, an environment or a log. A
//! vendor subscription never comes near this: the gateway is for API keys only.
//!
//! The route table is in memory. A restart rebuilds it from the stored images,
//! before the supervisor starts any agent, and a token is kept on disk so an
//! agent that resumes keeps the same one. The port is kept on disk too: a
//! harness process that outlived the last server in its keeper still has the
//! origin it was started with, and a restarted server listens there again.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

use anyagents_core::{Agent, AgentId, Error, Store};
use anyagents_gateway::{
    MemoryRoutes, MemoryUsage, Protocol, ProviderRoute, RouteResolver as _, UsageEvent,
    UsageSink as _, encode_request, encode_response, parse_request, parse_response,
};
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;

/// Environment variable specifying the external HTTPS origin of the coordinator's
/// authenticated protocol gateway (for example `https://agents.example.com` on port 443).
pub const ENV_PUBLIC_GATEWAY_URL: &str = "ANYAGENTS_PUBLIC_GATEWAY_URL";

/// The running gateway: where it listens, and what it resolves.
pub struct Gateway {
    pub port: u16,
    /// What each agent's token resolves to. The router reads it on every request.
    pub routes: MemoryRoutes,
}

static GATEWAY: OnceLock<Gateway> = OnceLock::new();
static SHARED_ROUTES: OnceLock<MemoryRoutes> = OnceLock::new();
static SHARED_USAGE: OnceLock<MemoryUsage> = OnceLock::new();
static START: Mutex<()> = Mutex::new(());

/// Shared gateway route table mounted on loopback and authenticated public/webhook TCP routers.
pub fn shared_routes() -> &'static MemoryRoutes {
    SHARED_ROUTES.get_or_init(MemoryRoutes::new)
}

/// Shared gateway usage recorder mounted on loopback and authenticated public/webhook TCP routers.
pub fn shared_usage() -> &'static MemoryUsage {
    SHARED_USAGE.get_or_init(MemoryUsage::new)
}

/// The running gateway, if `start` was called.
pub fn running() -> Option<&'static Gateway> {
    GATEWAY.get()
}

/// What a start found.
pub struct Started {
    /// The port the last start used could not be bound again. Anything may
    /// listen there now, so every token was replaced, and no harness started
    /// on the old origin may be taken over.
    pub moved: bool,
    /// The agents that route through the gateway.
    pub agents: Vec<AgentId>,
}

/// Start the gateway once, as part of the server starting. It is bound to the
/// server's lifetime: a server that cannot serve harnesses reports why.
pub fn start(data: &Path) -> Result<Started, Error> {
    let guard = START.lock().unwrap_or_else(PoisonError::into_inner);
    if GATEWAY.get().is_some() {
        return Ok(Started {
            moved: false,
            agents: Vec::new(),
        });
    }
    let (port, listener, moved) = bind(&data.join("gateway.port"))?;
    let routes = shared_routes().clone();
    let store = Store::open(data.join("state.db"))?;
    let mut provider_agents = Vec::new();
    // An archived agent runs nothing; a wake gives it its route back.
    for agent in store.list_live_agents()? {
        if let Some(image) = ProviderImage::load(data, &agent.id)? {
            if moved {
                rotate_token(data, &agent.id)?;
            }
            routes.insert(token(data, &agent.id)?, image.route(&agent.name));
            provider_agents.push(agent.id);
        }
    }
    let usage = shared_usage().clone();
    crate::server::serve_gateway(listener, routes.clone(), usage)?;
    let _ = GATEWAY.set(Gateway { port, routes });
    // The port is this process's, so every agent pointed at the gateway is
    // repointed now: a recorded origin from an earlier start no longer answers.
    let origin = format!("http://127.0.0.1:{port}");
    for agent_id in &provider_agents {
        let env = env_for(data, agent_id, &origin);
        if !env.is_empty() {
            store.set_agent_launch_env(agent_id, &env)?;
        }
    }
    drop(guard);
    Ok(Started {
        moved,
        agents: provider_agents,
    })
}

/// The loopback listener: on the port the last start used, when it is free,
/// else on any, recorded for the next start. `moved` when a recorded port
/// could not be bound again.
fn bind(port_file: &Path) -> Result<(u16, tokio::net::TcpListener, bool), Error> {
    let previous = std::fs::read_to_string(port_file)
        .ok()
        .and_then(|text| text.trim().parse::<u16>().ok());
    let (port, listener, moved) = match previous.map(crate::server::listen_on_loopback) {
        Some(Ok((port, listener))) => (port, listener, false),
        Some(Err(_)) => {
            let (port, listener) = crate::server::listen_on_loopback(0)?;
            (port, listener, true)
        }
        None => {
            let (port, listener) = crate::server::listen_on_loopback(0)?;
            (port, listener, false)
        }
    };
    if previous != Some(port) {
        anyagents_harness::atomic_write_file(port_file, format!("{port}\n").as_bytes(), 0o600)?;
    }
    Ok((port, listener, moved))
}

/// Replace an agent's gateway token: what was handed out before is refused.
fn rotate_token(data: &Path, id: &AgentId) -> Result<(), Error> {
    write_private(&token_file(data, id), &random_token()?)
}

fn token_file(data: &Path, id: &AgentId) -> PathBuf {
    data.join("agents")
        .join(id.to_string())
        .join("gateway-token.txt")
}

/// The origin a harness sends its requests to.
pub fn origin() -> Option<String> {
    running().map(|gateway| format!("http://127.0.0.1:{}", gateway.port))
}

/// External gateway origin advertised to isolated remote sandboxes (via Ingress/443).
/// Never falls back to `127.0.0.1`.
pub fn public_origin(data: Option<&Path>) -> Option<String> {
    if let Ok(raw) = std::env::var(ENV_PUBLIC_GATEWAY_URL) {
        let trimmed = raw.trim().trim_end_matches('/');
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    data.and_then(|d| crate::remote::require_public_url(d).ok())
        .and_then(|s| s.public_url)
        .map(|u| u.trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
}

/// Populate `shared_routes()` from stored agent `provider.json` images on startup.
pub fn ensure_routes_loaded(data: &Path) -> Result<Vec<AgentId>, Error> {
    let routes = shared_routes();
    let store = Store::open(data.join("state.db"))?;
    let mut loaded = Vec::new();
    for agent in store.list_live_agents()? {
        if let Some(image) = ProviderImage::load(data, &agent.id)? {
            routes.insert(token(data, &agent.id)?, image.route(&agent.name));
            loaded.push(agent.id);
        }
    }
    Ok(loaded)
}

/// Environment variables injecting the coordinator's public gateway URL and
/// per-agent gateway token (without raw provider API keys) into an isolated sandbox.
pub fn sandbox_env(data: &Path, id: &AgentId) -> Result<Vec<(String, String)>, String> {
    let Some(image) = ProviderImage::load(data, id).map_err(|e| e.to_string())? else {
        return Ok(Vec::new());
    };
    let tok = token(data, id).map_err(|e| e.to_string())?;
    let agent_name = Store::open(data.join("state.db"))
        .ok()
        .and_then(|s| s.agent(id).ok())
        .map(|a| a.name)
        .unwrap_or_else(|| id.to_string());
    shared_routes().insert(tok, image.route(&agent_name));
    let origin = public_origin(Some(data)).ok_or_else(|| {
        format!(
            "agent `{agent_name}` requires a public gateway URL ({ENV_PUBLIC_GATEWAY_URL} or server public_url) so the remote sandbox never dials 127.0.0.1"
        )
    })?;
    Ok(env_for(data, id, &origin))
}

/// Build the Axum router serving `/v1/messages`, `/v1/responses`, and `/v1/chat/completions`
/// authenticated exclusively by per-agent gateway bearer tokens.
pub fn http_router() -> Router {
    http_router_with(shared_routes().clone(), shared_usage().clone())
}

/// Build the protocol gateway Axum router against explicit route/usage stores.
/// Serves both `/v1/{messages,responses,chat/completions}` and
/// `/gateway/v1/{messages,responses,chat/completions}` so ingress routing without
/// path rewrite works cleanly.
pub fn http_router_with(routes: MemoryRoutes, usage: MemoryUsage) -> Router {
    let responses_state = std::sync::Arc::new((routes.clone(), usage.clone()));
    let core = anyagents_gateway::router(routes, usage).merge(
        Router::new()
            .route("/v1/responses", post(on_responses))
            .with_state(responses_state),
    );
    core.clone().nest("/gateway", core)
}

async fn on_responses(
    State(state): State<std::sync::Arc<(MemoryRoutes, MemoryUsage)>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({ "error": { "message": "unauthorized" } })),
            )
                .into_response();
        }
    };
    let (routes, usage) = &*state;
    let Some(route) = routes.resolve(&token) else {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({ "error": { "message": "unauthorized" } })),
        )
            .into_response();
    };
    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({ "error": { "message": "invalid json" } })),
            )
                .into_response();
        }
    };
    let stream = payload
        .get("stream")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let passthrough = route.protocol == Protocol::OpenAiResponses;
    let outbound_bytes = if passthrough {
        let mut obj = payload.clone();
        if let Some(map) = obj.as_object_mut() {
            map.insert("model".into(), serde_json::json!(route.model));
        }
        serde_json::to_vec(&obj).unwrap_or_default()
    } else {
        let req = match parse_request(Protocol::OpenAiResponses, &payload) {
            Ok(r) => r,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(serde_json::json!({ "error": { "message": err.to_string() } })),
                )
                    .into_response();
            }
        };
        match encode_request(route.protocol, &req, &route.model, false)
            .and_then(|v| serde_json::to_vec(&v).map_err(|_| anyagents_gateway::Fail::Upstream))
        {
            Ok(b) => b,
            Err(err) => return err.into_response(),
        }
    };

    let route_clone = route.clone();
    let version = headers
        .get("anthropic-version")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let forwarded = tokio::task::spawn_blocking(move || {
        forward_upstream_sync(&route_clone, outbound_bytes, version.as_deref())
    })
    .await;
    let (status, content_type, resp_bytes) = match forwarded {
        Ok(Ok(tuple)) => tuple,
        Ok(Err(_)) | Err(_) => {
            return (
                StatusCode::BAD_GATEWAY,
                axum::Json(serde_json::json!({ "error": { "message": "upstream gateway error" } })),
            )
                .into_response();
        }
    };

    if !(200..300).contains(&status) {
        return Response::builder()
            .status(StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY))
            .header(
                header::CONTENT_TYPE,
                content_type.unwrap_or_else(|| "application/json".into()),
            )
            .body(axum::body::Body::from(resp_bytes))
            .unwrap();
    }

    if passthrough && (stream || content_type.as_deref().is_some_and(|c| c.contains("text/event-stream")))
    {
        return Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                content_type.unwrap_or_else(|| "text/event-stream".into()),
            )
            .body(axum::body::Body::from(resp_bytes))
            .unwrap();
    }

    let upstream_val: serde_json::Value = match serde_json::from_slice(&resp_bytes) {
        Ok(v) => v,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(axum::body::Body::from(resp_bytes))
                .unwrap();
        }
    };
    let final_val = if passthrough {
        upstream_val
    } else {
        match parse_response(route.protocol, &upstream_val) {
            Ok(completion) => match encode_response(Protocol::OpenAiResponses, &completion, &route.model) {
                Ok(v) => v,
                Err(err) => return err.into_response(),
            },
            Err(err) => return err.into_response(),
        }
    };
    let (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens) =
        extract_usage_counts(&final_val);
    usage.record(UsageEvent {
        agent: route.agent.clone(),
        provider: route.provider.clone(),
        model: route.model.clone(),
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
    });
    axum::Json(final_val).into_response()
}

fn extract_usage_counts(val: &serde_json::Value) -> (u64, u64, u64, u64) {
    let usage = val.get("usage");
    let get_u64 = |obj: Option<&serde_json::Value>, key: &str| {
        obj.and_then(|u| u.get(key))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    let input = get_u64(usage, "input_tokens").max(get_u64(usage, "prompt_tokens"));
    let output = get_u64(usage, "output_tokens").max(get_u64(usage, "completion_tokens"));
    let cache_read = usage
        .and_then(|u| u.pointer("/input_tokens_details/cached_tokens"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| get_u64(usage, "cache_read_input_tokens"));
    let cache_write = get_u64(usage, "cache_creation_input_tokens");
    (input, output, cache_read, cache_write)
}

fn forward_upstream_sync(
    route: &ProviderRoute,
    body: Vec<u8>,
    anthropic_version: Option<&str>,
) -> Result<(u16, Option<String>, Vec<u8>), String> {
    let base = route.base_url.trim_end_matches('/');
    let suffix = match route.protocol {
        Protocol::AnthropicMessages => "/v1/messages",
        Protocol::OpenAiChat => "/v1/chat/completions",
        Protocol::OpenAiResponses => "/v1/responses",
    };
    let url = format!("{base}{suffix}");
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(300)))
        .http_status_as_error(false)
        .build()
        .new_agent();
    let mut req = agent.post(&url).header("content-type", "application/json");
    match route.protocol {
        Protocol::AnthropicMessages => {
            req = req
                .header("x-api-key", &route.api_key)
                .header("anthropic-version", anthropic_version.unwrap_or("2023-06-01"));
        }
        Protocol::OpenAiChat | Protocol::OpenAiResponses => {
            req = req.header("authorization", &format!("Bearer {}", route.api_key));
        }
    }
    let resp = req.send(body.as_slice()).map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let ctype = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = resp
        .into_body()
        .read_to_vec()
        .map_err(|e| e.to_string())?;
    Ok((status, ctype, bytes))
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, tok) = value.split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let trimmed = tok.trim();
    (!trimmed.is_empty() && !trimmed.contains(char::is_whitespace)).then(|| trimmed.to_string())
}

/// Point one agent at the gateway. Called when the agent's key is stored, with
/// the server running or not: the next start rebuilds it from the same file.
pub fn add_route(data: &Path, agent: &Agent) -> Result<Option<String>, Error> {
    let Some(image) = ProviderImage::load(data, &agent.id)? else {
        return Ok(None);
    };
    let token = token(data, &agent.id)?;
    shared_routes().insert(token.clone(), image.route(&agent.name));
    if let Some(gateway) = running() {
        gateway
            .routes
            .insert(token.clone(), image.route(&agent.name));
    }
    // Local agents keep using the loopback listener; remote-sandbox agents must
    // use the external public gateway origin so sandboxes never dial 127.0.0.1.
    let config_dir = anyagents_core::agent_config_dir(data, &agent.id);
    let runs_in_sandbox =
        anyagents_tools::sylphx::SylphxLeaseConfig::from_config_dir(&config_dir).is_some()
            || anyagents_tools::SandboxConfig::resolve(Some(&config_dir))
                .is_some_and(|s| s.remote_spawn)
            || anyagents_tools::SandboxMode::from_env().is_remote_required();
    let origin = if runs_in_sandbox {
        public_origin(Some(data))
    } else {
        origin()
            .or_else(|| published_origin(data))
            .or_else(|| public_origin(Some(data)))
    };
    if let Some(origin) = origin {
        store_launch_env(data, &agent.id, &origin)?;
    }
    Ok(Some(token))
}

/// Store the provider key for one agent and point its route at the gateway.
/// The file is 0600 and nothing prints the key. Called by `anyagents model`.
pub fn store_key(
    data: &Path,
    agent: &Agent,
    base_url: &str,
    model: &str,
    api_key: &str,
) -> Result<(), Error> {
    ProviderImage::store(data, agent, base_url, model, api_key)?;
    // With the gateway running the route answers at once; either way the agent
    // is told the gateway origin and its own token from its next start.
    add_route(data, agent)?;
    Ok(())
}

/// The origin the running server published on its socket, for a client process
/// that has no gateway of its own.
fn published_origin(data: &Path) -> Option<String> {
    let data = std::path::absolute(data).unwrap_or_else(|_| data.to_path_buf());
    crate::client::call::<crate::api::AgentsResponse>(
        &data,
        "GET",
        "/v1/agents",
        None,
        &crate::client::authority(&data),
    )
    .ok()
    .flatten()
    .and_then(|listed| listed.gateway)
}

/// Record an agent's launch environment from an origin a caller already has.
fn store_launch_env(data: &Path, id: &AgentId, origin: &str) -> Result<(), Error> {
    let env = env_for(data, id, origin);
    if env.is_empty() {
        return Ok(());
    }
    Store::open(data.join("state.db"))?.set_agent_launch_env(id, &env)?;
    Ok(())
}

/// The environment that points an agent's harness at the gateway: the origin
/// for the protocol that harness speaks, and that agent's own token. It is the
/// key the harness would otherwise have asked the user for, so nothing else is
/// inherited from the gateway's side.
pub fn agent_env(data: &Path, id: &AgentId) -> Vec<(String, String)> {
    // Before the gateway listens the agent keeps the key's vendor sign-in, and
    // picks the gateway up at the next start.
    match origin() {
        Some(origin) => env_for(data, id, &origin),
        None => Vec::new(),
    }
}

/// `agent_env` against one known origin, so a client that read the origin from
/// the running server records the same environment the server would.
fn env_for(data: &Path, id: &AgentId, origin: &str) -> Vec<(String, String)> {
    let Ok(Some(image)) = ProviderImage::load(data, id) else {
        return Vec::new();
    };
    let Ok(token) = token(data, id) else {
        return Vec::new();
    };
    let origin = origin.to_string();
    let mut env = match protocol_of(&image.harness) {
        Protocol::AnthropicMessages => vec![
            ("ANTHROPIC_BASE_URL".to_string(), origin),
            ("ANTHROPIC_AUTH_TOKEN".to_string(), token),
        ],
        Protocol::OpenAiChat | Protocol::OpenAiResponses => vec![
            ("OPENAI_BASE_URL".to_string(), origin),
            ("OPENAI_API_KEY".to_string(), token),
        ],
    };
    if !image.model.is_empty() && image.harness == "claude-code" {
        env.push(("ANTHROPIC_MODEL".to_string(), image.model.clone()));
    }
    env
}

/// The gateway token of one agent. It is generated once, kept 0600 beside the
/// agent's other state, and never printed.
pub fn token(data: &Path, id: &AgentId) -> Result<String, Error> {
    let file = token_file(data, id);
    match std::fs::read_to_string(&file) {
        Ok(text) if !text.trim().is_empty() => Ok(text.trim().to_string()),
        _ => {
            let token = random_token()?;
            write_private(&file, &token)?;
            Ok(token)
        }
    }
}

fn random_token() -> Result<String, Error> {
    use std::io::Read;
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn write_private(file: &Path, text: &str) -> Result<(), Error> {
    anyagents_harness::atomic_write_nonempty_file(file, text.as_bytes(), 0o600)?;
    Ok(())
}

/// Where an agent's provider key is kept.
fn image_file(data: &Path, id: &AgentId) -> PathBuf {
    data.join("agents")
        .join(id.to_string())
        .join("provider.json")
}

/// The provider an agent's model runs on, with the key the user stored. The key
/// is read here and handed to the gateway; nothing prints it.
pub struct ProviderImage {
    pub harness: String,
    pub model: String,
    pub base_url: String,
    api_key: String,
}

impl ProviderImage {
    /// Store the key the user typed. `anyagents model <agent> --base-url ...`
    /// is the only caller; the file is 0600 and the command prints no key.
    pub fn store(
        data: &Path,
        agent: &Agent,
        base_url: &str,
        model: &str,
        api_key: &str,
    ) -> Result<Self, Error> {
        let file = image_file(data, &agent.id);
        let value = serde_json::json!({
            "harness": agent.harness,
            "model": model,
            "baseUrl": base_url,
            "apiKey": api_key,
        });
        let text =
            serde_json::to_string_pretty(&value).map_err(|err| Error::Message(err.to_string()))?;
        write_private(&file, &text)?;
        Ok(Self {
            harness: agent.harness.clone(),
            model: model.to_string(),
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
        })
    }

    /// The image stored for this agent, or `None` when it runs on a subscription.
    pub fn load(data: &Path, id: &AgentId) -> Result<Option<Self>, Error> {
        let raw = match std::fs::read(image_file(data, id)) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let value: serde_json::Value =
            serde_json::from_slice(&raw).map_err(|err| Error::Message(err.to_string()))?;
        let text = |key: &str| {
            value
                .get(key)
                .and_then(|value| value.as_str())
                .filter(|text| !text.trim().is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    Error::Message(format!("provider configuration requires nonempty {key}"))
                })
        };
        Ok(Some(Self {
            harness: text("harness")?,
            model: text("model")?,
            base_url: text("baseUrl")?,
            api_key: text("apiKey")?,
        }))
    }

    /// The route an agent's token resolves to. The inbound protocol is the one
    /// the harness speaks; the provider is the image's own origin.
    pub fn route(&self, agent: &str) -> ProviderRoute {
        ProviderRoute {
            agent: agent.to_string(),
            provider: provider_of(&self.base_url).to_string(),
            protocol: protocol_of(&self.harness),
            base_url: self.base_url.trim_end_matches('/').to_string(),
            model: self.model.clone(),
            api_key: self.api_key.clone(),
        }
    }
}

/// Which harness API a harness calls the gateway with.
pub fn protocol_of(harness: &str) -> Protocol {
    match harness {
        "codex" => Protocol::OpenAiResponses,
        "opencode" => Protocol::OpenAiChat,
        _ => Protocol::AnthropicMessages,
    }
}

/// The provider name recorded on usage events: the first host label that is not
/// `api`, so `https://api.deepseek.com` is `deepseek`.
fn provider_of(base_url: &str) -> &str {
    let host = base_url
        .split("://")
        .nth(1)
        .unwrap_or(base_url)
        .split(['/', ':'])
        .next()
        .unwrap_or(base_url);
    let labels: Vec<&str> = host.split('.').filter(|l| !l.is_empty()).collect();
    match labels.as_slice() {
        [first, _, ..] if *first != "api" => first,
        [_, second, ..] => second,
        [only] => only,
        [] => base_url,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_provider_name_is_the_first_label_after_api() {
        assert_eq!(provider_of("https://api.deepseek.com"), "deepseek");
        assert_eq!(provider_of("https://api.openai.com/v1"), "openai");
        assert_eq!(provider_of("https://api.anthropic.com"), "anthropic");
        assert_eq!(provider_of("http://127.0.0.1:8080"), "127");
        assert_eq!(provider_of("deepseek"), "deepseek");
    }

    #[test]
    fn each_harness_calls_the_api_it_speaks() {
        assert_eq!(protocol_of("claude-code"), Protocol::AnthropicMessages);
        assert_eq!(protocol_of("codex"), Protocol::OpenAiResponses);
        assert_eq!(protocol_of("opencode"), Protocol::OpenAiChat);
    }
    /// A restarted server listens where the last one did, so a harness that
    /// outlived it in its keeper still reaches it. When that port is taken
    /// (by anything: a squatter), the gateway moves and says so, and the
    /// token it handed out before is replaced.
    #[test]
    fn the_gateway_keeps_its_port_and_rotates_tokens_when_it_cannot() {
        let dir = tempfile::tempdir().unwrap();
        let port_file = dir.path().join("gateway.port");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let (first, listener, moved) = bind(&port_file).unwrap();
        assert!(!moved, "a first start has nothing to move from");
        drop(listener);
        let (again, listener, moved) = bind(&port_file).unwrap();
        assert_eq!((again, moved), (first, false), "a free port is bound again");
        // The port is held by someone else across the restart.
        let squatter = listener.into_std().unwrap();
        let (moved_to, _listener, moved) = bind(&port_file).unwrap();
        assert!(moved, "a taken port is not reported");
        assert_ne!(moved_to, first);
        assert_eq!(
            std::fs::read_to_string(&port_file).unwrap().trim(),
            moved_to.to_string()
        );
        drop(squatter);
        let id = AgentId::generate();
        std::fs::create_dir_all(token_file(dir.path(), &id).parent().unwrap()).unwrap();
        let before = token(dir.path(), &id).unwrap();
        assert_eq!(token(dir.path(), &id).unwrap(), before, "a token is kept");
        rotate_token(dir.path(), &id).unwrap();
        assert_ne!(
            token(dir.path(), &id).unwrap(),
            before,
            "the token was not replaced"
        );
    }

    #[test]
    fn present_provider_images_require_every_routing_and_authentication_field() {
        let dir = tempfile::tempdir().unwrap();
        let id = AgentId::generate();
        assert!(ProviderImage::load(dir.path(), &id).unwrap().is_none());
        let file = image_file(dir.path(), &id);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        let valid = serde_json::json!({"harness":"opencode","model":"example-model","baseUrl":"https://provider.example.test/v1","apiKey":"fixture-token"});
        std::fs::write(&file, serde_json::to_vec(&valid).unwrap()).unwrap();
        assert!(ProviderImage::load(dir.path(), &id).unwrap().is_some());
        for key in ["harness", "model", "baseUrl", "apiKey"] {
            for invalid in [
                serde_json::Value::Null,
                serde_json::json!(""),
                serde_json::json!(17),
            ] {
                let mut value = valid.clone();
                value[key] = invalid;
                std::fs::write(&file, serde_json::to_vec(&value).unwrap()).unwrap();
                assert!(ProviderImage::load(dir.path(), &id).is_err(), "{key}");
            }
        }
        std::fs::write(&file, b"{}").unwrap();
        assert!(ProviderImage::load(dir.path(), &id).is_err());
    }

    #[test]
    fn responses_route_translates_chat_provider_and_rejects_unauthorized() {
        use std::io::{BufRead as _, BufReader, Read as _, Write as _};
        use std::net::TcpListener;
        use std::sync::{Arc, Mutex};

        let upstream_auths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let auth_ref = Arc::clone(&upstream_auths);
        let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in upstream.incoming().flatten() {
                let auth_ref = Arc::clone(&auth_ref);
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream);
                    let mut req_line = String::new();
                    let _ = reader.read_line(&mut req_line);
                    let path = req_line.split_whitespace().nth(1).unwrap_or("").to_string();
                    let mut len = 0usize;
                    let mut auth = String::new();
                    loop {
                        let mut h = String::new();
                        if reader.read_line(&mut h).is_err() || h == "\r\n" || h == "\n" || h.is_empty() {
                            break;
                        }
                        if let Some((k, v)) = h.split_once(':') {
                            if k.trim().eq_ignore_ascii_case("content-length") {
                                len = v.trim().parse().unwrap_or(0);
                            } else if k.trim().eq_ignore_ascii_case("authorization") {
                                auth = v.trim().to_string();
                            }
                        }
                    }
                    let mut body = vec![0u8; len];
                    if len > 0 {
                        let _ = reader.read_exact(&mut body);
                    }
                    auth_ref.lock().unwrap().push(auth);
                    let resp_body = if path == "/v1/responses" {
                        serde_json::json!({
                            "id": "resp_1",
                            "object": "response",
                            "model": "gpt-test",
                            "output": [{
                                "type": "message",
                                "role": "assistant",
                                "content": [{ "type": "output_text", "text": "passthrough ok" }]
                            }],
                            "usage": { "input_tokens": 3, "output_tokens": 2, "total_tokens": 5 }
                        })
                    } else {
                        serde_json::json!({
                            "id": "chatcmpl_1",
                            "object": "chat.completion",
                            "model": "gpt-test",
                            "choices": [{
                                "index": 0,
                                "message": { "role": "assistant", "content": "translated ok" },
                                "finish_reason": "stop"
                            }],
                            "usage": { "prompt_tokens": 4, "completion_tokens": 3 }
                        })
                    };
                    let raw = serde_json::to_vec(&resp_body).unwrap();
                    let mut stream = reader.into_inner();
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        raw.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&raw);
                });
            }
        });

        let routes = MemoryRoutes::new();
        routes.insert(
            "tok-resp",
            ProviderRoute {
                agent: "a1".into(),
                provider: "openai".into(),
                base_url: format!("http://{upstream_addr}"),
                protocol: Protocol::OpenAiResponses,
                model: "gpt-test".into(),
                api_key: "secret-upstream-key".into(),
            },
        );
        routes.insert(
            "tok-chat",
            ProviderRoute {
                agent: "a2".into(),
                provider: "openai".into(),
                base_url: format!("http://{upstream_addr}"),
                protocol: Protocol::OpenAiChat,
                model: "gpt-test".into(),
                api_key: "secret-upstream-key".into(),
            },
        );
        let usage = MemoryUsage::new();
        let app = http_router_with(routes, usage.clone());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let srv = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });

            let http = ureq::Agent::config_builder()
                .http_status_as_error(false)
                .build()
                .new_agent();

            let url = format!("http://{addr}/v1/responses");
            let unauth = tokio::task::spawn_blocking({
                let http = http.clone();
                let url = url.clone();
                move || {
                    http.post(&url)
                        .header("content-type", "application/json")
                        .send(br#"{"input":"hi"}"#.as_slice())
                        .unwrap()
                        .status()
                        .as_u16()
                }
            })
            .await
            .unwrap();
            assert_eq!(unauth, 401);

            let resp_pass: serde_json::Value = tokio::task::spawn_blocking({
                let http = http.clone();
                let url = url.clone();
                move || {
                    let r = http
                        .post(&url)
                        .header("authorization", "Bearer tok-resp")
                        .header("content-type", "application/json")
                        .send(br#"{"input":"hi"}"#.as_slice())
                        .unwrap();
                    assert_eq!(r.status().as_u16(), 200);
                    serde_json::from_str(&r.into_body().read_to_string().unwrap()).unwrap()
                }
            })
            .await
            .unwrap();
            assert_eq!(resp_pass["output"][0]["content"][0]["text"], "passthrough ok");

            let resp_trans: serde_json::Value = tokio::task::spawn_blocking(move || {
                let r = http
                    .post(&url)
                    .header("authorization", "Bearer tok-chat")
                    .header("content-type", "application/json")
                    .send(br#"{"model":"ignored","input":"hi"}"#.as_slice())
                    .unwrap();
                assert_eq!(r.status().as_u16(), 200);
                serde_json::from_str(&r.into_body().read_to_string().unwrap()).unwrap()
            })
            .await
            .unwrap();
            assert_eq!(resp_trans["object"], "response");

            let gw_url = format!("http://{addr}/gateway/v1/responses");
            let gw_resp: serde_json::Value = tokio::task::spawn_blocking(move || {
                let r = ureq::Agent::config_builder()
                    .http_status_as_error(false)
                    .build()
                    .new_agent()
                    .post(&gw_url)
                    .header("authorization", "Bearer tok-resp")
                    .header("content-type", "application/json")
                    .send(br#"{"input":"via gateway prefix"}"#.as_slice())
                    .unwrap();
                assert_eq!(r.status().as_u16(), 200);
                serde_json::from_str(&r.into_body().read_to_string().unwrap()).unwrap()
            })
            .await
            .unwrap();
            assert_eq!(gw_resp["output"][0]["content"][0]["text"], "passthrough ok");

            let auths = upstream_auths.lock().unwrap();
            assert!(
                auths.iter().all(|a| a == "Bearer secret-upstream-key"),
                "gateway must rewrite auth with stored upstream key: {auths:?}"
            );
            srv.abort();
        });
    }

    #[test]
    fn sandbox_env_requires_public_origin_and_never_exposes_raw_key() {
        let _env = crate::server::tests::env_lock();
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("gw-sbx", "codex"))
            .unwrap();
        ProviderImage::store(
            dir.path(),
            &agent,
            "https://provider.example.test/v1",
            "gpt-test",
            "raw-provider-secret",
        )
        .unwrap();

        unsafe {
            std::env::remove_var(ENV_PUBLIC_GATEWAY_URL);
        }
        assert!(sandbox_env(dir.path(), &agent.id).is_err());

        unsafe {
            std::env::set_var(ENV_PUBLIC_GATEWAY_URL, "https://agents.example.com/");
        }
        let env = sandbox_env(dir.path(), &agent.id).unwrap();
        unsafe {
            std::env::remove_var(ENV_PUBLIC_GATEWAY_URL);
        }
        let base_url = env
            .iter()
            .find(|(k, _)| k == "OPENAI_BASE_URL")
            .map(|(_, v)| v.as_str())
            .unwrap();
        let api_key = env
            .iter()
            .find(|(k, _)| k == "OPENAI_API_KEY")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(base_url, "https://agents.example.com");
        assert!(!base_url.contains("127.0.0.1"));
        assert_ne!(api_key, "raw-provider-secret");
    }
}
