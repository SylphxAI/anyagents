//! HTTP/JSON mailbox API on a user-only unix socket.
//!
//! Every call is authenticated before it is routed: an agent instance presents
//! the mailbox capability the server minted for it, and the operator presents
//! its own token. A sender, an inbox, a bot or an agent name in a body is a
//! destination, never an identity.

use std::io::{BufRead, BufReader};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyagents_core::{Address, Agent, Error, Incoming, ReceiveOutcome, Store, now_ms};
use anyagents_gateway::MemoryRoutes;
use anyagents_installer::{CommandRunner, InstallError, LineSink, builtin_catalog, sign_in};
use anyagents_supervisor::{HealthBoard, Supervisor, SystemClock, declared, mode_name, steer_name};
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde::Deserialize;

use crate::api::{
    AgentView, AgentsResponse, ConversationResponse, ConversationTurnView, CreateAgentRequest,
    Delivery, GroupActionRequest, GroupView, GroupsResponse, InboxResponse, InteractRequest,
    InteractResponse, MessageView, SendRequest, SendResponse,
};
use crate::auth::{self, Principal};
use crate::create::{self, CreateRequest};
use crate::operator;
use crate::vendor::{self, VendorHarness};

const SOCKET_NAME: &str = "anyagents.sock";
/// `sockaddr_un.sun_path` holds 108 bytes, including the trailing NUL.
const MAX_SOCKET_PATH: usize = 107;

pub fn socket_path(data: &Path) -> PathBuf {
    data.join(SOCKET_NAME)
}

pub fn run(data: &Path) -> Result<(), Error> {
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    run_with(data, Arc::new(Mutex::new(ProcessRunner)), stop_rx)
}

/// Run the server with a sign-in runner the caller supplies. Tests use this so
/// no vendor command is started. `stop` ends the server when the caller drops it.
pub fn run_with(
    data: &Path,
    runner: Arc<Mutex<dyn CommandRunner + Send>>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Error> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        tokio::select! {
            result = serve(data, runner) => result,
            _ = &mut stop => Ok(()),
        }
    })
}

#[derive(Clone)]
pub(crate) struct AppState {
    /// This deployment's idempotency namespace for one agent's sends. It never
    /// leaves the process, so a key names one call of this server.
    pub(crate) send_namespace: String,
    pub(crate) store: Arc<Store>,
    pub(crate) health: HealthBoard,
    pub(crate) sign_in: Arc<Mutex<dyn CommandRunner + Send>>,
    pub(crate) auth: auth::Mode,
}

/// This process's capability key. Tests name one so a server and the fixture
/// it reads share the same key.
fn capability_key() -> Result<String, Error> {
    use std::io::Read as _;
    if let Ok(named) = std::env::var("ANYAGENTS_CAPABILITY_KEY")
        && named.len() >= 32
    {
        return Ok(named);
    }
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn acquire_server_lock(data: &Path) -> Result<std::fs::File, Error> {
    std::fs::create_dir_all(data)?;
    let lock_path = data.join("server.lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(Error::Message(format!(
                "anyagents serve already running on {}",
                data.display()
            )));
        }
    }
    Ok(file)
}

async fn serve(data: &Path, sign_in: Arc<Mutex<dyn CommandRunner + Send>>) -> Result<(), Error> {
    let _server_lock = acquire_server_lock(data)?;
    let store = Arc::new(Store::open(data.join("state.db"))?);
    let key = capability_key()?;
    store.hold_capability_key(&key)?;
    let agents = store.list_agents()?;
    let mut mint_failures = Vec::new();
    for agent in &agents {
        if let Err(err) = crate::operator::ensure(&store, &agent.id) {
            mint_failures.push(format!("{}: {err}", agent.name));
        }
    }
    if !mint_failures.is_empty() {
        eprintln!(
            "warning: a capability was not minted for {}",
            mint_failures.join(", ")
        );
    }
    if crate::sandbox::seat_named() && crate::sandbox::configured_seat().is_none() {
        return Err(Error::Message(
            "a seat is named without its group: set ANYAGENTS_AGENT_UID, ANYAGENTS_AGENT_GID and \
             ANYAGENTS_SOCKET_GID together, or none"
                .into(),
        ));
    }
    // An operator token is the identity boundary; it holds only when every
    // harness it starts is in its own OS seat. Otherwise the boundary contract
    // says so, and no run claims it.
    let seats: Vec<String> = store
        .list_agents()?
        .into_iter()
        .map(|agent| agent.name)
        .collect();
    if operator::operator_token(data).is_some() && !crate::sandbox::launch_boundary(&seats) {
        return Err(Error::Message(
            "this server has an operator token, so it is the identity boundary, but no OS seat is \
             in place for its agents: configure ANYAGENTS_AGENT_UID, ANYAGENTS_AGENT_GID and \
             ANYAGENTS_SOCKET_GID and install /usr/local/bin/anyagents-setpriv, or start it without \
             an operator token on a single-user machine"
                .into(),
        ));
    }
    let auth = deployment_mode(data);
    if !auth.is_confined() {
        not_confined_note(data);
    }
    // The boundary contract is written and read back before the socket is
    // bound, so a client never reaches an API whose boundary is not in place.
    let boundary = anyagents_core::boundary::write(data)?;
    let path = socket_path(data);
    if path.as_os_str().len() > MAX_SOCKET_PATH {
        return Err(Error::Message(format!(
            "socket path is longer than {MAX_SOCKET_PATH} bytes"
        )));
    }
    // The gateway is up and every agent repointed before anything is bound or
    // started: a client that sees the socket must find a live origin, not the
    // port of the server that just stopped.
    crate::gateway::start(data)?;
    // The API is bound before any vendor starts: an agent that is slow, silent
    // or wedged must never stop the server from answering.
    prepare_socket(&path)?;
    let api_listener = tokio::net::UnixListener::bind(&path)?;
    set_socket_mode(&path)?;
    if auth.is_confined() {
        crate::sandbox::share_socket_with_seats(&path);
    }
    let supervisor = Supervisor::<VendorHarness>::start(
        Arc::clone(&store),
        Arc::new(SystemClock::new()),
        Arc::new(vendor::vendor_launch),
        vendor::child_env(),
    )
    .map_err(Error::Message)?;
    if auth.is_confined() {
        crate::sandbox::share_socket_with_seats(&data.join("wake.sock"));
    }
    let state = AppState {
        send_namespace: capability_key()?[..16].to_string(),
        store: Arc::clone(&store),
        health: supervisor.board(),
        sign_in,
        auth,
    };
    let _ = boundary;
    let listener = api_listener;
    let poller = crate::telegram::Poller::start(Arc::clone(&store), data.to_path_buf())
        .map_err(Error::Message)?;
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() && !crate::sandbox::is_isolated_test_env()
    {
        let reaper_root = data.to_path_buf();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let root = reaper_root.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    crate::browser::reap_idle_browsers(&root);
                })
                .await;
            }
        });
        let updater_root = data.to_path_buf();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 3600));
            loop {
                interval.tick().await;
                let root = updater_root.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    for entry in anyagents_installer::builtin_catalog() {
                        if let Ok((before, after, true)) =
                            crate::install::update_harness(&root, &entry.id)
                        {
                            eprintln!(
                                "anyagents-updater: upgraded harness `{}` {} -> {}",
                                entry.id, before, after
                            );
                        }
                    }
                })
                .await;
            }
        });
    }
    if let Some(addr) = webhook_listen_addr() {
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(tcp_listener) => {
                if let Ok(local) = tcp_listener.local_addr() {
                    let _ = std::fs::write(data.join("webhook.addr"), format!("{local}\n"));
                }
                let webhook_app = Router::new()
                    .route("/", get(webhook_healthz))
                    .route("/healthz", get(webhook_healthz))
                    .route("/v1/webhooks/healthz", get(webhook_healthz))
                    .route("/v1/webhooks/telegram/{channel_id}", post(telegram_webhook))
                    .with_state(state.clone());
                tokio::spawn(async move {
                    let _ = axum::serve(tcp_listener, webhook_app).await;
                });
            }
            Err(err) => {
                eprintln!("warning: webhook TCP listener ({addr}) did not bind: {err}");
            }
        }
    }
    let app = Router::new()
        .route("/v1/agents", get(list_agents).post(create_agent_route))
        .route("/v1/agents/{name}/admin", post(set_admin_route))
        .route("/v1/agents/{name}/model", post(model_route))
        .route(
            "/v1/agents/{name}/description",
            get(get_description_route).post(set_description_route),
        )
        .route(
            "/v1/agents/{name}/instructions",
            get(get_description_route).post(set_description_route),
        )
        .route("/v1/browser", post(browser_route))
        .route("/v1/profiles", get(profiles_route).post(set_profile_route))
        .route(
            "/v1/subagent-defaults",
            get(get_subagent_defaults_route).post(set_subagent_defaults_route),
        )
        .route("/v1/channels", get(channels_route).post(add_channel_route))
        .route("/v1/agents/{name}/inbox", get(inbox))
        .route("/v1/agents/{name}/conversation", get(conversation_route))
        .route("/v1/agents/{name}/archive", post(archive_agent_route))
        .route("/v1/agents/{name}/pause", post(pause_agent_route))
        .route("/v1/agents/{name}/wake", post(wake_agent_route))
        .route(
            "/v1/agents/{name}",
            axum::routing::delete(delete_agent_route),
        )
        .route(
            "/v1/groups",
            get(list_groups_route).post(group_action_route),
        )
        .route("/v1/send", post(send))
        .route("/v1/interact", post(interact_route))
        .route("/v1/harnesses/{id}/sign-in", post(sign_in_route))
        .route(
            "/v1/operator/agents/{name}/capability",
            post(mint_capability),
        )
        .route("/v1/operator/agents/{name}/inbox", get(operator_inbox))
        .route("/v1/whoami", get(whoami))
        .route("/v1/webhooks/healthz", get(webhook_healthz))
        .route("/v1/webhooks/telegram/{channel_id}", post(telegram_webhook))
        .with_state(state);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        })
        .await;
    // A long poll ends at Telegram, not here: signal the poller to stop first
    // so released long polls exit instead of re-entering a 30s wait, then
    // release every bot so each bot's last offset is written before shutdown.
    poller.signal_stop();
    if let Err(reason) = crate::telegram::poller::release_every_bot(
        &store,
        store.data_dir(),
        std::time::Duration::from_secs(30),
    ) {
        eprintln!("anyagents-telegram: shutdown did not release every bot: {reason}");
    }
    poller.stop();
    drop(supervisor);
    let _ = std::fs::remove_file(&path);
    result.map_err(Error::from)
}

/// How this server authenticates. A machine started without an operator token
/// serves the agents on its own machine, and names them by the route it serves:
/// its data directory, as a start script exports it to every harness. With an
/// operator token the server is the identity boundary and reads nothing from a
/// name.
fn deployment_mode(data: &Path) -> auth::Mode {
    match auth::Mode::from_env(data) {
        auth::Mode::Confined {
            operator_digest: None,
        } => match std::fs::canonicalize(data) {
            Ok(dir) => {
                let dir = dir.display().to_string();
                // [`not_confined_note`] says it once, on stderr, and the note
                // names no secret.
                auth::Mode::Trusted {
                    data_dir: std::path::PathBuf::from(dir),
                }
            }
            Err(_) => auth::Mode::Confined {
                operator_digest: None,
            },
        },
        other => other,
    }
}

/// The one line a single-user deployment says at start.
pub fn not_confined_note(data: &Path) {
    let dir = std::fs::canonicalize(data).unwrap_or_else(|_| data.to_path_buf());
    eprintln!(
        "warning: single-user route: an unauthenticated call acts as the agent it names on {}; start with an operator token for the identity boundary",
        dir.display()
    );
}

/// Mint an agent's capability after its instance was made (a create, a new
/// sign-in), so its harness starts with one and the last one is retired.
fn capability_mark(data_dir: &Path) -> impl Fn() -> Result<(), Error> + Send + Sync + 'static {
    let data_dir = data_dir.to_path_buf();
    move || {
        let store = Store::open(data_dir.join("state.db"))?;
        let agents = store.list_agents()?;
        for agent in &agents {
            crate::operator::mint(&store, &agent.id)?;
        }
        Ok(())
    }
}

/// `POST /v1/operator/agents/{name}/capability`: the operator gives one agent a
/// fresh capability, retiring the last one. The value is written into that
/// agent's own config directory and never returned.
async fn mint_capability(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_operator(&principal)?;
    let agent = state
        .store
        .agent_by_name(&name)
        .map_err(ApiError::from_store)?;
    operator::mint(&state.store, &agent.id).map_err(ApiError::from_store)?;
    Ok(Json(
        serde_json::json!({ "agent": agent.name, "minted": true }),
    ))
}

/// `GET /v1/webhooks/healthz`: unauthenticated readiness probe for the public
/// webhook listener (`https://webhooks.example.com/v1/webhooks/healthz`).
async fn webhook_healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

/// `POST /v1/webhooks/telegram/{channel_id}`: receives one update pushed by
/// Telegram, verifies `X-Telegram-Bot-Api-Secret-Token` against the channel's
/// derived HMAC-SHA256 secret, and routes the update into the durable mailbox.
async fn telegram_webhook(
    State(state): State<AppState>,
    AxumPath(channel_id): AxumPath<String>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let secret = headers
        .get("x-telegram-bot-api-secret-token")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let store = Arc::clone(&state.store);
    spawn_store(move || {
        crate::telegram::receive_webhook(
            &store,
            store.data_dir(),
            &channel_id,
            secret.as_deref(),
            &payload,
        )
        .map_err(|err| match err {
            crate::telegram::WebhookError::Unauthorized => {
                ApiError::new(StatusCode::UNAUTHORIZED, "invalid webhook secret token")
            }
            crate::telegram::WebhookError::NotFound(why) => {
                ApiError::new(StatusCode::NOT_FOUND, why)
            }
            crate::telegram::WebhookError::BadRequest(why) => {
                ApiError::new(StatusCode::BAD_REQUEST, why)
            }
            crate::telegram::WebhookError::Store(why) => {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why)
            }
        })?;
        Ok(Json(serde_json::json!({ "ok": true })))
    })
    .await
}

fn webhook_listen_addr() -> Option<String> {
    if let Ok(addr) = std::env::var("ANYAGENTS_WEBHOOK_LISTEN") {
        let trimmed = addr.trim();
        if trimmed.is_empty() || trimmed == "0" || trimmed.eq_ignore_ascii_case("off") {
            return None;
        }
        return Some(trimmed.to_string());
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
        || std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some()
    {
        return None;
    }
    Some("0.0.0.0:8788".to_string())
}

/// `GET /v1/whoami`: the agent this call acts as. A tool server asks before it
/// speaks for anyone, and gets the id the server resolved from the capability,
/// never the name a caller wrote.
async fn whoami(ctx: Whoami) -> Result<Json<serde_json::Value>, ApiError> {
    match ctx.principal {
        Principal::Agent { agent_id, name } => Ok(Json(serde_json::json!({
            "agent_id": agent_id.as_str(),
            "name": name,
        }))),
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
            let principal =
                auth::agent_by_name(&state.store, id.trim()).map_err(auth::AuthRejection)?;
            return Ok(Self { principal });
        }
        use axum::extract::FromRequestParts;
        let principal =
            <Principal as FromRequestParts<AppState>>::from_request_parts(parts, state).await?;
        Ok(Self { principal })
    }
}

/// `GET /v1/operator/agents/{name}/inbox`: any agent's mailbox, for the
/// operator's own reading. A different path from the agent's own route, so a
/// name can never be taken for an identity.
async fn operator_inbox(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<Since>,
) -> Result<Json<InboxResponse>, ApiError> {
    require_operator(&principal)?;
    let agent = state
        .store
        .agent_by_name(&name)
        .map_err(ApiError::from_store)?;
    spawn_store(move || inbox_of(&state.store, &agent.id, query.since, query.limit)).await
}

/// Only the operator administers the deployment.
fn require_operator(principal: &Principal) -> Result<(), ApiError> {
    if principal.is_operator() {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "an agent session cannot administer the server",
        ))
    }
}

/// Authority is read from SQLite on every call, not from the capability.
fn require_admin_or_operator(
    store: &Store,
    principal: &Principal,
    action: &str,
    target: &str,
) -> Result<String, ApiError> {
    let actor = match principal {
        Principal::Operator => "operator".to_string(),
        Principal::Agent { agent_id, name } => {
            let agent = store.agent(agent_id).map_err(ApiError::from_store)?;
            if agent.parent_id.is_some() || !agent.is_admin {
                return Err(ApiError::forbidden("administrative authority required"));
            }
            format!("agent:{name}")
        }
    };
    store
        .record_admin_audit(&actor, action, target)
        .map_err(ApiError::from_store)?;
    Ok(actor)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdminRequest {
    admin: bool,
}

async fn set_admin_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<AdminRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_operator(&principal)?;
    let agent = state
        .store
        .agent_by_name(&name)
        .map_err(ApiError::from_store)?;
    state
        .store
        .set_agent_admin(&agent.id, body.admin)
        .map_err(ApiError::from_store)?;
    state
        .store
        .record_admin_audit(
            "operator",
            if body.admin {
                "agents.grant_admin"
            } else {
                "agents.revoke_admin"
            },
            &agent.name,
        )
        .map_err(ApiError::from_store)?;
    Ok(Json(
        serde_json::json!({"agent": agent.name, "admin": body.admin}),
    ))
}

// Socket requests carry secret values, never host filesystem paths.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelRequest {
    model: String,
    base_url: Option<String>,
    key: Option<String>,
    profile: Option<String>,
}

async fn model_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<ModelRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin_or_operator(&state.store, &principal, "agents.model", &name)?;
    let agent = state
        .store
        .agent_by_name(&name)
        .map_err(ApiError::from_store)?;
    if let Some(profile) = body.profile {
        if body.key.is_some() || body.base_url.is_some() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "profile cannot be combined with a key or base URL",
            ));
        }
        create::bind_profile(&state.store, &agent, &profile, &body.model)
            .map_err(ApiError::from_store)?;
    } else {
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
        crate::gateway::store_key(state.store.data_dir(), &agent, base_url, &body.model, key)
            .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "could not bind provider key"))?;
    }
    Ok(Json(
        serde_json::json!({"agent": agent.name, "model": body.model}),
    ))
}

async fn profiles_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<Vec<create::ProviderProfileEntry>>, ApiError> {
    require_admin_or_operator(&state.store, &principal, "profiles.list", "profiles")?;
    Ok(Json(create::sync_and_list_provider_profiles(
        &state.store,
        state.store.data_dir(),
    )))
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
    require_admin_or_operator(&state.store, &principal, "profiles.set", &body.profile)?;
    let (provider, profile) =
        create::set_provider_profile(state.store.data_dir(), &body.profile, &body.key)
            .map_err(ApiError::from_store)?;
    Ok(Json(serde_json::json!({
        "provider": provider,
        "profile": profile,
    })))
}

async fn get_subagent_defaults_route(
    _principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<create::SubagentDefaultsConfig>, ApiError> {
    Ok(Json(create::load_subagent_defaults(state.store.data_dir())))
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
            harness: body.harness.filter(|s| !s.trim().is_empty()),
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
    create::save_subagent_defaults(state.store.data_dir(), &cfg).map_err(ApiError::from_store)?;
    Ok(Json(cfg))
}

#[derive(Deserialize, Default)]
struct DescriptionRequest {
    #[serde(default, alias = "instructions", alias = "role")]
    description: Option<String>,
    #[serde(default)]
    clear: Option<bool>,
}

async fn get_description_route(
    _principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let rec = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &rec.id);
        let system_prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).ok();
        Ok(Json(serde_json::json!({
            "id": rec.id.as_str(),
            "name": rec.name,
            "description": rec.instructions,
            "system_prompt": system_prompt,
        })))
    })
    .await
}

async fn set_description_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<DescriptionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        if name.eq_ignore_ascii_case("all")
            && body.description.is_none()
            && !body.clear.unwrap_or(false)
        {
            require_admin_or_operator(&state.store, &principal, "agents.sync_instructions", "all")?;
            create::sync_all_agent_instructions(&state.store, state.store.data_dir());
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
        let is_self_or_parent = match &principal {
            Principal::Agent { agent_id, .. } => {
                *agent_id == target.id || target.parent_id.as_ref() == Some(agent_id)
            }
            Principal::Operator => false,
        };
        if !is_self_or_parent {
            require_admin_or_operator(
                &state.store,
                &principal,
                "agents.set_description",
                &target.name,
            )?;
        }
        let mut rec = target;
        if body.clear.unwrap_or(false) {
            rec = state
                .store
                .set_agent_instructions(&rec.id, None)
                .map_err(ApiError::from_store)?;
        } else if let Some(ref desc) = body.description {
            let trimmed = desc.trim();
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
        let parent_name = rec
            .parent_id
            .as_ref()
            .and_then(|pid| state.store.agent(pid).ok())
            .map(|p| p.name);
        let cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &rec.id);
        create::write_agent_instructions(&cfg, &rec, parent_name.as_deref());
        if let Some((_, _, gid, _)) =
            crate::sandbox::seat_for_agent_with_hint(&rec.name, parent_name.as_deref())
        {
            crate::sandbox::share_config_with_seat(&cfg, gid);
        }
        Ok(Json(serde_json::json!({
            "id": rec.id.as_str(),
            "name": rec.name,
            "description": rec.instructions,
            "system_prompt_synced": true,
        })))
    })
    .await
}

async fn browser_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<crate::browser::BrowserRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let default_profile = match &principal {
        Principal::Agent { name, .. } => name.clone(),
        Principal::Operator => "default".to_string(),
    };
    spawn_store(move || {
        let out = crate::browser::execute(state.store.data_dir(), &default_profile, body)
            .map_err(ApiError::from_store)?;
        Ok(Json(out))
    })
    .await
}

async fn channels_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin_or_operator(&state.store, &principal, "channels.list", "channels")?;
    let channels: Vec<_> = state.store.list_channels().map_err(ApiError::from_store)?.into_iter().map(|c| serde_json::json!({"id": c.id.to_string(), "kind": c.kind, "agents": c.agents.iter().filter_map(|id| state.store.agent(id).ok().map(|a| a.name)).collect::<Vec<_>>()})).collect();
    Ok(Json(serde_json::json!({"channels": channels})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelRequest {
    kind: String,
    agent: String,
    token: String,
}

async fn add_channel_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<ChannelRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin_or_operator(&state.store, &principal, "channels.add", &body.agent)?;
    if body.kind != "telegram"
        || body.token.trim().is_empty()
        || body
            .token
            .trim()
            .chars()
            .any(|c| c.is_whitespace() || c == '/')
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid channel kind or token",
        ));
    }
    let agent = state
        .store
        .agent_by_name(&body.agent)
        .map_err(ApiError::from_store)?;
    crate::channel::record(
        &state.store,
        state.store.data_dir(),
        &body.kind,
        &agent.id,
        body.token.trim().as_bytes(),
    )
    .map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not record channel",
        )
    })?;
    Ok(Json(
        serde_json::json!({"agent": agent.name, "kind": body.kind}),
    ))
}

/// A loopback port the operating system picked, held open until the caller
/// serves on it, or why none could be bound.
pub fn listen_on_loopback() -> Result<(u16, tokio::net::TcpListener), Error> {
    // A non-blocking listener before it joins the runtime; the runtime makes it
    // blocking-polled itself.
    let address = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    address.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(address)?;
    let port = listener.local_addr()?.port();
    Ok((port, listener))
}

/// Serve the gateway on the loopback listener it was given, for the life of the
/// process. Each harness reaches it with its own token; nothing else can. It
/// runs on the server's runtime, so the caller must be inside one.
pub fn serve_gateway(
    listener: tokio::net::TcpListener,
    routes: MemoryRoutes,
    usage: anyagents_gateway::MemoryUsage,
) -> Result<(), Error> {
    let app = anyagents_gateway::router(routes, usage);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(())
}

fn prepare_socket(path: &Path) -> Result<(), Error> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(path)?,
        Ok(_) => {
            return Err(Error::Message(format!(
                "socket path exists and is not a socket: {}",
                path.display()
            )));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn set_socket_mode(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// One line of a sign-in, as the client reads it.
#[derive(serde::Serialize)]
struct SignInLine {
    kind: &'static str,
    text: String,
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
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(16);
    let runner = Arc::clone(&state.sign_in);
    let root = state.store.data_dir().to_path_buf();
    let mark = capability_mark(state.store.data_dir());
    let spawned = tokio::task::spawn_blocking(move || {
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

/// Runs the vendor command for real. Each line it prints reaches the sink
/// before the command returns.
struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        let env_pairs: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let mut command = anyagents_harness::spawn_command(Path::new(program), &env_pairs);
        command
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        if let Some(stdout) = child.stdout.take() {
            for line in BufReader::new(stdout).lines() {
                let line = line.map_err(|err| InstallError::Failed(err.to_string()))?;
                on_line.line(&line);
            }
        }
        let status = child
            .wait()
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        if status.success() {
            Ok(())
        } else {
            Err(InstallError::Failed(format!("exit {status}")))
        }
    }
}

async fn list_agents(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<AgentsResponse>, ApiError> {
    // Any authenticated principal may see who is on the server. The operator
    // sees the gateway origin; an agent learns it from its own start.
    let operator = principal.is_operator();
    spawn_store(move || {
        let mut listed = list_agents_sync(&state)?;
        if !operator {
            listed.0.gateway = None;
        }
        Ok(listed)
    })
    .await
}

fn list_agents_sync(state: &AppState) -> Result<Json<AgentsResponse>, ApiError> {
    let mut agents = Vec::new();
    for agent in state.store.list_agents().map_err(ApiError::from_store)? {
        agents.push(agent_view(&state.store, &state.health, &agent)?);
    }
    Ok(Json(AgentsResponse {
        agents,
        gateway: crate::gateway::origin(),
    }))
}

fn agent_view(store: &Store, health: &HealthBoard, agent: &Agent) -> Result<AgentView, ApiError> {
    let now = now_ms();
    let lag = store.lag(&agent.id, now).map_err(ApiError::from_store)?;
    let has_open_turn = store.has_open_turn(&agent.id).unwrap_or(false);
    let session_quota = if agent.harness == "claude-code" {
        let cfg = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
        anyagents_harness_claude::detect_session_quota_reset_ms(&cfg, now)
    } else {
        None
    };
    let (process, turn_state, mode, steer, quota) = match health.get(&agent.id) {
        Some(live) => {
            let effective_quota = live.quota_reset_at_ms.or(session_quota);
            let working = live.process_alive
                && effective_quota.is_none()
                && (live.turn_state == anyagents_harness::TurnState::InTurn || has_open_turn);
            (
                if live.process_alive { "alive" } else { "down" },
                if working { "working" } else { "idle" },
                mode_name(live.delivery_mode),
                steer_name(live.steer_support),
                effective_quota,
            )
        }
        None => {
            let (mode, steer) = declared(&agent.harness);
            (
                "down",
                "idle",
                mode_name(mode),
                steer_name(steer),
                session_quota,
            )
        }
    };
    Ok(AgentView {
        id: agent.id.to_string(),
        name: agent.name.clone(),
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        harness: agent.harness.clone(),
        model: agent.model.clone(),
        workdir: agent.workdir.clone(),
        state: agent.state.clone(),
        process: process.to_string(),
        turn_state: turn_state.to_string(),
        delivery_mode: mode.to_string(),
        steer_support: steer.to_string(),
        quota_reset_at_ms: quota,
        oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
        install: install_state(store, agent),
        description: agent.instructions.clone(),
    })
}

/// `installed` once this harness is on the machine, `needed` when the reason it
/// is not, `pending` when nothing has been recorded yet.
fn install_state(store: &Store, agent: &Agent) -> String {
    match crate::install::recorded(&agent.harness) {
        Some(Ok(())) => "installed".to_string(),
        Some(Err(reason)) => reason,
        None => {
            if crate::install::program(store.data_dir(), &agent.harness).is_file() {
                "installed".to_string()
            } else {
                "pending".to_string()
            }
        }
    }
}

async fn create_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<CreateAgentRequest>,
) -> Result<(StatusCode, Json<AgentView>), ApiError> {
    let caller_agent = match &principal {
        Principal::Operator => None,
        Principal::Agent { agent_id, name } => Some((agent_id.clone(), name.clone())),
    };
    spawn_store(move || {
        let parent_agent = match (&caller_agent, body.parent.as_deref()) {
            (_, Some("root" | "")) => {
                require_admin_or_operator(
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
        let parent_id = parent_agent.as_ref().map(|p| p.id.clone());
        let (harness, model, profile) = create::resolve_subagent_defaults(
            state.store.data_dir(),
            parent_agent.as_ref(),
            &body.harness,
            body.model,
            body.profile,
        );
        let agent = create::create(
            &state.store,
            state.store.data_dir(),
            CreateRequest {
                name: body.name,
                harness,
                model,
                profile,
                workdir: body.workdir.map(PathBuf::from),
                workdir_default: owner_workdir(),
                instructions: body.instructions,
                parent_id,
            },
        )
        .map_err(ApiError::from_store)?;
        if !anyagents_core::agent_config_dir(state.store.data_dir(), &agent.id)
            .join("mcp-capability")
            .is_file()
        {
            operator::mint(&state.store, &agent.id).map_err(ApiError::from_store)?;
        }
        if let Some(prompt) = body
            .prompt
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let sender = caller_agent
                .as_ref()
                .map(|(_, name)| format!("agent:{name}"))
                .unwrap_or_else(|| "operator".to_string());
            state
                .store
                .receive(
                    &agent.id,
                    Incoming {
                        text: prompt.to_string(),
                        sender: Some(sender),
                        source_time_ms: None,
                        source_key: None,
                        reply_to: None,
                        quote: None,
                        origin: None,
                        received_at_ms: None,
                    },
                )
                .map_err(ApiError::from_store)?;
            if let Some((ref caller_id, _)) = caller_agent {
                let _ = state.store.record_outbound(
                    caller_id,
                    &format!("agent:{}", agent.name),
                    None,
                    prompt,
                );
            }
        }
        let view = agent_view(&state.store, &state.health, &agent)?;
        Ok((StatusCode::CREATED, Json(view)))
    })
    .await
}

async fn archive_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if found.parent_id.is_none() {
            if matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "a root agent cannot archive itself; pass a target agent name or id in `agent`",
                ));
            }
            require_admin_or_operator(
                &state.store,
                &principal,
                "agents.archive_root",
                &found.name,
            )?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && found.parent_id.as_ref() != Some(agent_id)
        ) {
            require_admin_or_operator(&state.store, &principal, "agents.archive", &found.name)?;
        }
        let agent = state
            .store
            .set_agent_state(&found.id, "archived")
            .map_err(ApiError::from_store)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if state
                .health
                .get(&found.id)
                .is_none_or(|live| !live.process_alive)
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        if found.parent_id.is_some() {
            reap_subagent_ephemeral_dirs(state.store.data_dir(), &found.id);
        }
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

pub(crate) fn reap_subagent_ephemeral_dirs(data_dir: &Path, agent_id: &anyagents_core::AgentId) {
    let agent_root = data_dir.join("agents").join(agent_id.as_str());
    let config_dir = agent_root.join("config");
    let seat_ids = detect_seat_ids(&config_dir);
    for rel in [
        "cache-home",
        "state-home",
        "tmp",
        ".rustup",
        ".cargo",
        "data-home/opencode",
        "opencode-session-id",
        "claude-session-id",
        ".claude/projects",
        ".codex/sessions",
    ] {
        let target = config_dir.join(rel);
        remove_path_with_seat(&target, seat_ids);
    }
    let work_dir = agent_root.join("work");
    remove_path_with_seat(&work_dir, seat_ids);
}

#[cfg(unix)]
fn detect_seat_ids(config_dir: &Path) -> Option<(u32, u32, u32)> {
    use std::os::unix::fs::MetadataExt as _;
    let (me, my_gid) = std::fs::metadata("/proc/self")
        .map(|m| (m.uid(), m.gid()))
        .unwrap_or((0, 0));
    let cfg_meta = std::fs::metadata(config_dir).ok()?;
    let gid = cfg_meta.gid();
    let sock_gid = config_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| std::fs::metadata(p).ok())
        .map(|m| m.gid())
        .unwrap_or(gid);
    if cfg_meta.uid() != me && cfg_meta.uid() != 0 {
        return Some((cfg_meta.uid(), gid, sock_gid));
    }
    if let Ok(entries) = std::fs::read_dir(config_dir) {
        for entry in entries.flatten() {
            if let Ok(m) = entry.metadata() {
                if m.uid() != me && m.uid() != 0 {
                    return Some((m.uid(), gid, sock_gid));
                }
                if m.is_dir()
                    && let Ok(sub) = std::fs::read_dir(entry.path())
                {
                    for child in sub.flatten() {
                        if let Ok(cm) = child.metadata()
                            && cm.uid() != me
                            && cm.uid() != 0
                        {
                            return Some((cm.uid(), gid, sock_gid));
                        }
                    }
                }
            }
        }
    }
    if gid != my_gid && gid != 0 {
        return Some((gid, gid, sock_gid));
    }
    None
}

#[cfg(not(unix))]
fn detect_seat_ids(_config_dir: &Path) -> Option<(u32, u32, u32)> {
    None
}

fn remove_path_with_seat(target: &Path, seat_ids: Option<(u32, u32, u32)>) {
    let Ok(meta) = std::fs::symlink_metadata(target) else {
        return;
    };
    if meta.file_type().is_symlink() || meta.is_file() {
        if std::fs::remove_file(target).is_ok() {
            return;
        }
    } else if meta.is_dir() && std::fs::remove_dir_all(target).is_ok() {
        return;
    }
    if let Some((uid, gid, sock_gid)) = seat_ids
        && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
    {
        let uid_s = uid.to_string();
        let gid_s = gid.to_string();
        let groups_s = if sock_gid != gid && sock_gid != 0 {
            format!("{gid},{sock_gid}")
        } else {
            gid_s.clone()
        };
        let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
            .args([
                "--reuid",
                &uid_s,
                "--regid",
                &gid_s,
                "--groups",
                &groups_s,
                "--no-new-privs",
                "--",
                "/bin/rm",
                "-rf",
            ])
            .arg(target)
            .stderr(std::process::Stdio::null())
            .status();
        let _ = std::fs::remove_dir_all(target);
        let _ = std::fs::remove_file(target);
    }
}

async fn pause_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if found.parent_id.is_none() {
            if matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "a root agent cannot pause itself; pass a target agent name or id in `agent`",
                ));
            }
            require_admin_or_operator(&state.store, &principal, "agents.pause_root", &found.name)?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && found.parent_id.as_ref() != Some(agent_id)
        ) {
            require_admin_or_operator(&state.store, &principal, "agents.pause", &found.name)?;
        }
        let agent = state
            .store
            .set_agent_state(&found.id, "paused")
            .map_err(ApiError::from_store)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if state
                .health
                .get(&found.id)
                .is_none_or(|live| !live.process_alive)
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

async fn delete_agent_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        if found.parent_id.is_none() {
            if matches!(&principal, Principal::Agent { agent_id, .. } if agent_id == &found.id) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "a root agent cannot delete itself",
                ));
            }
            require_admin_or_operator(&state.store, &principal, "agents.delete_root", &found.name)?;
        } else if matches!(
            &principal,
            Principal::Agent { agent_id, .. }
                if agent_id != &found.id && found.parent_id.as_ref() != Some(agent_id)
        ) {
            require_admin_or_operator(&state.store, &principal, "agents.delete", &found.name)?;
        }
        let _ = state.store.set_agent_state(&found.id, "archived");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if state
                .health
                .get(&found.id)
                .is_none_or(|live| !live.process_alive)
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let deleted = state
            .store
            .delete_agent(&found.id)
            .map_err(ApiError::from_store)?;
        let agent_dir = state
            .store
            .data_dir()
            .join("agents")
            .join(deleted.id.as_str());
        let seat_ids = detect_seat_ids(&agent_dir.join("config"));
        reap_subagent_ephemeral_dirs(state.store.data_dir(), &deleted.id);
        remove_path_with_seat(&agent_dir.join("config"), seat_ids);
        remove_path_with_seat(&agent_dir, seat_ids);
        let _ = std::fs::remove_dir_all(&agent_dir);
        Ok(Json(serde_json::json!({
            "id": deleted.id.as_str(),
            "name": deleted.name,
            "deleted": true,
        })))
    })
    .await
}

async fn wake_agent_route(
    _principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AgentView>, ApiError> {
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let agent = state
            .store
            .set_agent_state(&found.id, "running")
            .map_err(ApiError::from_store)?;
        Ok(Json(agent_view(&state.store, &state.health, &agent)?))
    })
    .await
}

fn build_group_roster(store: &Store, members: &[String]) -> Vec<crate::api::GroupMemberView> {
    let mut out = Vec::with_capacity(members.len());
    for name in members {
        if let Ok(agent) = store.agent_by_name(name) {
            out.push(crate::api::GroupMemberView {
                id: agent.id.to_string(),
                name: agent.name,
                harness: agent.harness,
                model: agent.model,
                state: agent.state,
                description: agent.instructions,
            });
        }
    }
    out
}

async fn list_groups_route(
    _principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<GroupsResponse>, ApiError> {
    spawn_store(move || {
        let rows = state.store.list_groups().map_err(ApiError::from_store)?;
        let groups = rows
            .into_iter()
            .map(|(name, members)| {
                let roster = build_group_roster(&state.store, &members);
                GroupView {
                    name,
                    members,
                    roster,
                }
            })
            .collect();
        Ok(Json(GroupsResponse { groups }))
    })
    .await
}

async fn group_action_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<GroupActionRequest>,
) -> Result<Json<GroupView>, ApiError> {
    let default_agent = match &principal {
        Principal::Agent { name, .. } => Some(name.clone()),
        Principal::Operator => None,
    };
    spawn_store(move || {
        let group = body
            .group
            .trim()
            .strip_prefix("group:")
            .unwrap_or(body.group.trim())
            .to_string();
        if group.is_empty() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "group name is empty",
            ));
        }
        let target_name = body
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or(default_agent);
        match body.action.trim() {
            "join" => {
                let who = target_name.ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "join needs an agent name")
                })?;
                let agent = state
                    .store
                    .agent_by_name(&who)
                    .map_err(ApiError::from_store)?;
                state
                    .store
                    .join_group(&group, &agent.id)
                    .map_err(ApiError::from_store)?;
            }
            "leave" => {
                let who = target_name.ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "leave needs an agent name")
                })?;
                let agent = state
                    .store
                    .agent_by_name(&who)
                    .map_err(ApiError::from_store)?;
                state
                    .store
                    .leave_group(&group, &agent.id)
                    .map_err(ApiError::from_store)?;
            }
            "members" => {}
            other => {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    format!("unknown group action: {other}"),
                ));
            }
        }
        let members = state
            .store
            .room_members(&group)
            .map_err(ApiError::from_store)?
            .unwrap_or_default();
        let roster = build_group_roster(&state.store, &members);
        Ok(Json(GroupView {
            name: group,
            members,
            roster,
        }))
    })
    .await
}

#[derive(Deserialize)]
struct Since {
    since: Option<i64>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct ConversationQuery {
    with: Option<String>,
    since_ms: Option<i64>,
    limit: Option<usize>,
    #[serde(default, alias = "q", alias = "search")]
    query: Option<String>,
}

async fn inbox(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<Since>,
) -> Result<Json<InboxResponse>, ApiError> {
    // An agent's `inbox` is strictly its own incoming queue. To inspect another
    // agent's full two-way communication (`in` + `out` with users, groups, and
    // peers), agents use `/v1/agents/{name}/conversation`.
    let agent_id = match &principal {
        Principal::Operator => {
            state
                .store
                .agent_by_name(&name)
                .map_err(ApiError::from_store)?
                .id
        }
        Principal::Agent {
            agent_id,
            name: own_name,
        } => {
            if name != agent_id.as_str() && name != own_name.as_str() {
                return Err(ApiError::forbidden(
                    "a session reads its own inbox, not another agent's; use conversation to read another agent's conversation",
                ));
            }
            agent_id.clone()
        }
    };
    spawn_store(move || inbox_of(&state.store, &agent_id, query.since, query.limit)).await
}

async fn conversation_route(
    _principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<ConversationQuery>,
) -> Result<Json<ConversationResponse>, ApiError> {
    spawn_store(move || {
        let clean_q = query
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let is_all = name.eq_ignore_ascii_case("all") || name == "*";
        let (agent_name, agent_id_str, raw_turns) = if let Some(q) = clean_q {
            let qvec =
                crate::channel::embed_query_for_store(&state.store, state.store.data_dir(), q);
            let turns = state
                .store
                .search_conversation(
                    Some(&name),
                    query.with.as_deref(),
                    query.since_ms,
                    query.limit,
                    q,
                    qvec.as_deref(),
                )
                .map_err(ApiError::from_store)?;
            let (aname, aid) = if is_all {
                ("all".to_string(), "all".to_string())
            } else if let Ok(a) = state.store.agent_by_name(&name) {
                (a.name, a.id.to_string())
            } else {
                (name.clone(), name.clone())
            };
            (aname, aid, turns)
        } else if is_all {
            let turns = state
                .store
                .conversation_all(query.with.as_deref(), query.since_ms, query.limit)
                .map_err(ApiError::from_store)?;
            ("all".to_string(), "all".to_string(), turns)
        } else {
            let agent = state
                .store
                .agent_by_name(&name)
                .map_err(ApiError::from_store)?;
            let turns = state
                .store
                .conversation(
                    &agent.id,
                    query.with.as_deref(),
                    query.since_ms,
                    query.limit,
                )
                .map_err(ApiError::from_store)?;
            (agent.name, agent.id.to_string(), turns)
        };
        let turns = raw_turns
            .into_iter()
            .map(|t| ConversationTurnView {
                id: t.id,
                direction: t.direction,
                from: t.from,
                to: t.to,
                reply_to: t.reply_to,
                text: t.text,
                timestamp_ms: t.timestamp_ms,
            })
            .collect();
        Ok(Json(ConversationResponse {
            agent: agent_name,
            agent_id: agent_id_str,
            turns,
        }))
    })
    .await
}

/// One agent's messages past `since`, in sequence order.
fn inbox_of(
    store: &Store,
    agent_id: &anyagents_core::AgentId,
    since: Option<i64>,
    limit: Option<usize>,
) -> Result<Json<InboxResponse>, ApiError> {
    let mut rows = store.inbox(agent_id, since).map_err(ApiError::from_store)?;
    if let Some(limit) = limit {
        if since.is_none() && rows.len() > limit {
            rows.drain(..rows.len() - limit);
        } else if rows.len() > limit {
            rows.truncate(limit);
        }
    }
    let messages = rows
        .into_iter()
        .map(|message| MessageView {
            id: message.id.to_string(),
            seq: message.seq,
            text: message.text,
            sender: message.sender,
            reply_to: message.reply_to,
            source_time_ms: message.source_time_ms,
            received_at_ms: message.received_at_ms,
            delivered_at_ms: message.delivered_at_ms,
            ack: message.ack.to_string(),
        })
        .collect();
    Ok(Json(InboxResponse { messages }))
}

async fn send(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<SendRequest>,
) -> Result<Json<SendResponse>, ApiError> {
    // The authenticated principal decides who the message is from; a body
    // `sender` is only a check, and a disagreement is refused with no write.
    // On a machine without the boundary, its own agent route names the sender;
    // that is the one place a body chooses the author, and an inbox is still
    // only ever its own.
    let route_named = match &principal {
        Principal::Agent { name, .. } if state.auth.is_trusted() => body
            .sender
            .as_deref()
            .and_then(|named| Actor::parse(named).ok())
            .filter(|named| named.as_address() != Actor::Agent(name.clone()).as_address()),
        _ => None,
    };
    let actor = match (&principal, route_named) {
        (_, Some(named)) => named,
        (Principal::Agent { name, .. }, None) => {
            if let Some(named) = body.sender.as_deref() {
                Actor::parse(named).and_then(|named| {
                    if named.as_address() == Actor::Agent(name.clone()).as_address() {
                        Ok(())
                    } else {
                        Err(ApiError::forbidden(
                            "a session sends as itself, not as the sender it names",
                        ))
                    }
                })?;
            }
            Actor::Agent(name.clone())
        }
        (Principal::Operator, _) => {
            let sender = body.sender.as_deref().ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "an operator send needs an explicit sender",
                )
            })?;
            Actor::parse(sender)?
        }
    };
    let body = SendRequest {
        sender: Some(actor.as_address()),
        ..body
    };
    let key = send_key(&state.send_namespace, &body);
    let store = Arc::clone(&state.store);
    let principal_for_plan = principal.clone();
    let actor_for_plan = actor.clone();
    let body_for_plan = body.clone();
    let plan = spawn_store(move || {
        plan_send(&store, &principal_for_plan, &actor_for_plan, &body_for_plan)
    })
    .await?;
    let store = Arc::clone(&state.store);
    spawn_store(move || send_sync(&store, &actor, body, plan, key)).await
}

async fn interact_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<InteractRequest>,
) -> Result<Json<InteractResponse>, ApiError> {
    let actor_name = match &principal {
        Principal::Agent { name, .. } => name.clone(),
        Principal::Operator => {
            let sender = body.sender.as_deref().ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "an operator interact call needs an explicit sender",
                )
            })?;
            sender
                .trim()
                .strip_prefix("agent:")
                .unwrap_or(sender.trim())
                .to_string()
        }
    };
    let store = Arc::clone(&state.store);
    spawn_store(move || interact_sync(&store, &principal, &actor_name, body)).await
}

fn interact_sync(
    store: &Store,
    principal: &Principal,
    actor_name: &str,
    body: InteractRequest,
) -> Result<Json<InteractResponse>, ApiError> {
    let action = body.action.trim().to_ascii_lowercase();
    let raw_target = body.target.trim();
    if raw_target.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "interact requires a non-empty target",
        ));
    }
    let actor = Actor::Agent(actor_name.to_string());
    // Resolve target into optional (chat_id, message_id, thread_id, stored_message).
    let mut tg_coords: Option<(i64, i64, Option<i64>)> = None;
    let mut stored_msg: Option<anyagents_core::Message> = None;
    if let Some(msg_id_str) = raw_target
        .strip_prefix("anyagents:reply:")
        .or_else(|| raw_target.starts_with("msg_").then_some(raw_target))
    {
        if let Ok(Some(msg)) = own_message(store, principal, &actor, msg_id_str) {
            if let Some(ref origin) = msg.origin
                && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            {
                tg_coords = Some((chat_id, message_id, origin.thread_id));
            }
            stored_msg = Some(msg);
        } else if let Ok(agent_row) = store.agent_by_name(actor_name)
            && let Ok(Some(msg)) = store.message(&agent_row.id, msg_id_str)
        {
            if let Some(ref origin) = msg.origin
                && let (Some(chat_id), Some(message_id)) = (origin.chat_id, origin.message_id)
            {
                tg_coords = Some((chat_id, message_id, origin.thread_id));
            }
            stored_msg = Some(msg);
        }
    } else if let Some(rest) = raw_target
        .strip_prefix("telegram:chat:")
        .or_else(|| raw_target.strip_prefix("telegram:user:"))
    {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() >= 2
            && let (Ok(chat_id), Ok(message_id)) = (
                parts[0].parse::<i64>(),
                parts[parts.len() - 1].parse::<i64>(),
            )
        {
            let thread_id = if parts.len() == 3 {
                parts[1].parse::<i64>().ok()
            } else {
                None
            };
            tg_coords = Some((chat_id, message_id, thread_id));
        }
    }
    let data_dir = store.data_dir();
    match action.as_str() {
        "react" => {
            let emoji = body.emoji.as_deref().map(str::trim);
            if let Some((chat_id, message_id, thread_id)) = tg_coords {
                crate::telegram::react_message(
                    store, data_dir, actor_name, chat_id, message_id, emoji, thread_id,
                )
                .map_err(ApiError::from_store)?;
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(format!(
                        "reacted {} on telegram:chat:{chat_id}:{message_id}",
                        emoji.unwrap_or("(cleared)")
                    )),
                }))
            } else if let Some(msg) = stored_msg {
                let e = emoji.filter(|s| !s.is_empty()).unwrap_or("👍");
                let summary = format!("[Reaction {e} on message {}]", msg.id);
                if let Some(ref sender) = msg.sender
                    && let Some(peer_name) = sender.strip_prefix("agent:")
                    && let Ok(peer) = store.agent_by_name(peer_name)
                {
                    let mut incoming = Incoming::new(summary.clone());
                    incoming.sender = Some(format!("agent:{actor_name}"));
                    incoming.reply_to = msg.reply_to.clone();
                    let _ = store.receive(&peer.id, incoming);
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(summary),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "react target must be anyagents:reply:msg_... or telegram:chat:<chat_id>:<message_id>",
                ))
            }
        }
        "vote" => {
            if body.options.is_empty() {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "vote requires at least one option in `options`",
                ));
            }
            let resolved_opts =
                resolve_poll_options(stored_msg.as_ref().map(|m| m.text.as_str()), &body.options);
            let comment = body
                .text
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let ballot = match comment {
                Some(note) => format!("🗳️ **Poll Vote**: {} — {note}", resolved_opts.join(", ")),
                None => format!("🗳️ **Poll Vote**: {}", resolved_opts.join(", ")),
            };
            if let Some((chat_id, message_id, thread_id)) = tg_coords {
                let _ = crate::telegram::react_message(
                    store,
                    data_dir,
                    actor_name,
                    chat_id,
                    message_id,
                    Some("👍"),
                    thread_id,
                );
                let posted_id = crate::telegram::post_text(
                    store,
                    data_dir,
                    actor_name,
                    chat_id,
                    &ballot,
                    Some(message_id),
                    thread_id,
                )
                .map_err(ApiError::from_store)?;
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(format!(
                        "voted ({}) on telegram:chat:{chat_id}:{message_id} (receipt #{posted_id})",
                        resolved_opts.join(", ")
                    )),
                }))
            } else if let Some(msg) = stored_msg {
                if let Some(ref sender) = msg.sender
                    && let Some(peer_name) = sender.strip_prefix("agent:")
                    && let Ok(peer) = store.agent_by_name(peer_name)
                {
                    let mut incoming = Incoming::new(ballot.clone());
                    incoming.sender = Some(format!("agent:{actor_name}"));
                    incoming.reply_to = msg.reply_to.clone();
                    let _ = store.receive(&peer.id, incoming);
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(ballot),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "vote target must be anyagents:reply:msg_... or telegram:chat:<chat_id>:<message_id>",
                ))
            }
        }
        "check" | "uncheck" => {
            if body.options.is_empty() {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "check/uncheck requires at least one task number or text in `options`",
                ));
            }
            let mark_done = action == "check";
            if let Some(ref msg) = stored_msg {
                let updated = toggle_checklist_items(&msg.text, &body.options, mark_done);
                if let Some((chat_id, message_id, thread_id)) = tg_coords {
                    let _ = crate::telegram::edit_message(
                        store, data_dir, actor_name, chat_id, message_id, &updated, thread_id,
                    );
                }
                Ok(Json(InteractResponse {
                    ok: true,
                    action,
                    target: body.target,
                    detail: Some(updated),
                }))
            } else {
                Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "check/uncheck target must resolve to a stored checklist message",
                ))
            }
        }
        "edit" => {
            let new_text = body
                .text
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "edit requires non-empty `text`")
                })?;
            let (chat_id, message_id, thread_id) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "edit target must resolve to a Telegram message",
                )
            })?;
            crate::telegram::edit_message(
                store, data_dir, actor_name, chat_id, message_id, new_text, thread_id,
            )
            .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("edited telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        "delete" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "delete target must resolve to a Telegram message",
                )
            })?;
            crate::telegram::delete_chat_message(store, data_dir, actor_name, chat_id, message_id)
                .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("deleted telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        "pin" | "unpin" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "pin/unpin target must resolve to a Telegram message",
                )
            })?;
            let pin = action == "pin";
            crate::telegram::pin_message(store, data_dir, actor_name, chat_id, message_id, pin)
                .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!(
                    "{} telegram:chat:{chat_id}:{message_id}",
                    if pin { "pinned" } else { "unpinned" }
                )),
            }))
        }
        "stop_poll" | "close_poll" => {
            let (chat_id, message_id, _) = tg_coords.ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "stop_poll target must resolve to a Telegram poll message",
                )
            })?;
            crate::telegram::close_poll(store, data_dir, actor_name, chat_id, message_id)
                .map_err(ApiError::from_store)?;
            Ok(Json(InteractResponse {
                ok: true,
                action,
                target: body.target,
                detail: Some(format!("closed poll telegram:chat:{chat_id}:{message_id}")),
            }))
        }
        other => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "unsupported interact action `{other}`; expected react, vote, check, uncheck, edit, delete, pin, unpin, or stop_poll"
            ),
        )),
    }
}

fn toggle_checklist_items(message_text: &str, selected: &[String], mark_done: bool) -> String {
    let target_box = if mark_done { "☑" } else { "☐" };
    message_text
        .lines()
        .map(|line| {
            let trimmed = line.trim_start();
            let rest = trimmed
                .strip_prefix("☐ ")
                .or_else(|| trimmed.strip_prefix("☑ "));
            if let Some(after_box) = rest {
                let idx_str = after_box.split('.').next().unwrap_or("").trim();
                let matched = selected.iter().any(|sel| {
                    let s = sel.trim();
                    s == idx_str
                        || after_box
                            .to_ascii_lowercase()
                            .contains(&s.to_ascii_lowercase())
                });
                if matched {
                    return format!("{target_box} {after_box}");
                }
            }
            line.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn resolve_poll_options(message_text: Option<&str>, chosen: &[String]) -> Vec<String> {
    let mut parsed_options: Vec<String> = Vec::new();
    if let Some(text) = message_text
        && let Some(idx) = text.find("| Options: ")
    {
        let rest = &text[idx + "| Options: ".len()..];
        let end = rest.rfind(']').unwrap_or(rest.len());
        for item in rest[..end].split("; ") {
            let cleaned = item.split(" (").next().unwrap_or(item).trim().to_string();
            if !cleaned.is_empty() {
                parsed_options.push(cleaned);
            }
        }
    }
    chosen
        .iter()
        .map(|sel| {
            let trimmed = sel.trim();
            if let Ok(num) = trimmed.parse::<usize>()
                && num >= 1
                && num <= parsed_options.len()
            {
                return parsed_options[num - 1].clone();
            }
            trimmed.to_string()
        })
        .collect()
}

/// One send, resolved to the destinations it will write, before any of them is
/// validated or posted. `reply_to` is a mailbox reference the server resolves
/// against this agent's own mailbox, or a destination written plainly.
fn plan_send(
    store: &Store,
    principal: &Principal,
    actor: &Actor,
    body: &SendRequest,
) -> Result<SendPlan, ApiError> {
    let mut plan = SendPlan::parse(store, actor, body)?;
    let reply = crate::reply::resolve(
        principal,
        |id| own_message(store, principal, actor, id),
        body.reply_to.as_deref(),
    )
    .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?;
    if let Some(destination) = reply {
        if plan.targets.is_empty() {
            plan.targets = vec![target_of(&destination)];
            plan.reply = destination.reply().map(str::to_string);
        } else {
            // When `to` addresses are explicitly given alongside `reply_to`:
            // - A Telegram target must name the same `chat` as a Telegram reply
            //   destination (so a group reply never goes to a private Telegram
            //   chat or another Telegram chat), and inherits the recorded topic
            //   and message id for that chat when not specified on the target.
            // - Internal `agent:<name>` and `group:<room>` targets in `to` are
            //   honoured as written without wiping out `plan.targets`.
            let mut matched_telegram = false;
            for target in &mut plan.targets {
                if let Target::Telegram {
                    address,
                    chat,
                    thread,
                    reply,
                } = target
                {
                    match &destination {
                        crate::reply::Destination::Telegram {
                            chat: wanted_chat,
                            thread: wanted_thread,
                            reply: wanted_reply,
                        } if chat == wanted_chat => {
                            if thread.is_none() {
                                *thread = wanted_thread.clone();
                                *address = destination.address();
                            }
                            if reply.is_none() {
                                *reply = wanted_reply.clone();
                            }
                            matched_telegram = true;
                        }
                        other => {
                            return Err(ApiError::new(
                                StatusCode::BAD_REQUEST,
                                format!(
                                    "this reply names one place ({}), and the addresses name another ({address})",
                                    other.address()
                                ),
                            ));
                        }
                    }
                }
            }
            if matched_telegram {
                plan.reply = destination.reply().map(str::to_string);
            }
        }
    }
    Ok(plan)
}

/// One message of the mailbox a reply speaks for, by id: the calling agent's
/// own mailbox, or, for the operator, the agent it is sending as.
fn own_message(
    store: &Store,
    principal: &Principal,
    actor: &Actor,
    id: &str,
) -> Result<Option<anyagents_core::Message>, String> {
    let agent_id = match (principal, actor) {
        (Principal::Agent { agent_id, .. }, _) => agent_id.clone(),
        (_, Actor::Agent(name)) => store.agent_by_name(name).map_err(|err| err.to_string())?.id,
        (Principal::Operator, Actor::Operator(_)) => return Ok(None),
    };
    store.message(&agent_id, id).map_err(|err| err.to_string())
}

fn target_of(destination: &crate::reply::Destination) -> Target {
    match destination {
        crate::reply::Destination::Telegram {
            chat,
            thread,
            reply,
        } => Target::Telegram {
            address: destination.address(),
            chat: chat.clone(),
            thread: thread.clone(),
            reply: reply.clone(),
        },
        crate::reply::Destination::Agent(name) => Target::Agent(name.clone()),
        crate::reply::Destination::Group(name) => Target::Room(name.clone()),
    }
}

/// One send, resolved to the destinations it will write, before any of them is
/// validated or posted.
struct SendPlan {
    targets: Vec<Target>,
    /// The provider message id a reply answers, when the reply named one.
    reply: Option<String>,
}

enum Target {
    Agent(String),
    Room(String),
    Telegram {
        address: String,
        chat: String,
        thread: Option<String>,
        reply: Option<String>,
    },
}

impl SendPlan {
    fn parse(store: &Store, actor: &Actor, body: &SendRequest) -> Result<Self, ApiError> {
        if body.to.is_empty() && body.reply_to.is_none() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "send needs an address",
            ));
        }
        let mut targets = Vec::with_capacity(body.to.len());
        for raw in &body.to {
            match Address::parse(raw)
                .map_err(Error::from)
                .map_err(ApiError::from_store)?
            {
                Address::Agent(name) if name == "all" => {
                    targets.push(Target::Room("all".to_string()));
                }
                Address::Agent(name) if name == "parent" => {
                    let Actor::Agent(sender_name) = actor else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has a parent",
                        ));
                    };
                    let sender = store
                        .agent_by_name(sender_name)
                        .map_err(ApiError::from_store)?;
                    let parent_id = sender.parent_id.ok_or_else(|| {
                        ApiError::new(StatusCode::BAD_REQUEST, "this agent has no parent")
                    })?;
                    let parent = store
                        .agent_by_name(parent_id.as_str())
                        .map_err(ApiError::from_store)?;
                    targets.push(Target::Agent(parent.name));
                }
                Address::Agent(name) if name == "children" => {
                    let Actor::Agent(sender_name) = actor else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has children",
                        ));
                    };
                    let sender = store
                        .agent_by_name(sender_name)
                        .map_err(ApiError::from_store)?;
                    let children = store
                        .children_of(&sender.id)
                        .map_err(ApiError::from_store)?;
                    if children.is_empty() {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "this agent has no active children",
                        ));
                    }
                    for child in children {
                        targets.push(Target::Agent(child.name));
                    }
                }
                Address::Group(room) if room == "children" => {
                    let Actor::Agent(sender_name) = actor else {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "only an agent has children",
                        ));
                    };
                    let sender = store
                        .agent_by_name(sender_name)
                        .map_err(ApiError::from_store)?;
                    let children = store
                        .children_of(&sender.id)
                        .map_err(ApiError::from_store)?;
                    if children.is_empty() {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "this agent has no active children",
                        ));
                    }
                    for child in children {
                        targets.push(Target::Agent(child.name));
                    }
                }
                Address::Agent(name) => targets.push(Target::Agent(name)),
                Address::TelegramChat { chat, thread } => {
                    targets.push(Target::Telegram {
                        address: raw.clone(),
                        chat,
                        thread,
                        reply: None,
                    });
                }
                Address::TelegramUser {
                    user,
                    message: reply,
                } => {
                    targets.push(Target::Telegram {
                        address: raw.clone(),
                        chat: user,
                        thread: None,
                        reply,
                    });
                }
                Address::Group(room) => targets.push(Target::Room(room)),
            }
        }
        Ok(Self {
            targets,
            reply: None,
        })
    }
}

/// Every destination is checked before any write, so a refused address never
/// leaves half a send behind.
fn check_targets(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    plan: &SendPlan,
) -> Result<(), ApiError> {
    for target in &plan.targets {
        match target {
            Target::Agent(name) => {
                store.agent_by_name(name).map_err(ApiError::from_store)?;
            }
            // Counted without writing, so an empty room fails the whole send
            // instead of silently delivering nothing.
            Target::Room(room) => {
                if room_recipients(store, body, room).is_empty() {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        format!("room {room} has no agent to deliver to"),
                    ));
                }
            }
            Target::Telegram {
                chat,
                thread,
                reply,
                ..
            } => {
                prepare_telegram(
                    store,
                    actor,
                    body,
                    chat,
                    thread.as_deref(),
                    reply.as_deref(),
                )?;
            }
        }
    }
    Ok(())
}

fn send_sync(
    store: &Store,
    actor: &Actor,
    body: SendRequest,
    plan: SendPlan,
    key: Option<String>,
) -> Result<Json<SendResponse>, ApiError> {
    // A call repeated after its answer was lost is the same delivery: the
    // receipt it already produced comes back unchanged.
    if let Some(key) = key.as_deref()
        && let Some(text) = store.send_receipt(key).map_err(ApiError::from_store)?
    {
        let receipt: SendResponse = serde_json::from_str(&text)
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        return Ok(Json(receipt));
    }
    // The request's `reply_to` is a reference, not a Telegram id; a Telegram
    // target carries the id the reference resolved to instead.
    let reply = plan.reply.clone();
    let effective_text = format_internal_send_text(&body);
    let body = SendRequest {
        text: effective_text,
        // A reply the caller resolved to a message id, or a reference the
        // server resolved to one, is what this post answers.
        reply_to: reply.clone().or_else(|| {
            body.reply_to
                .filter(|v| crate::reply::ref_message(v).is_none())
        }),
        ..body
    };
    check_targets(store, actor, &body, &plan)?;
    let sender_agent_id = match actor {
        Actor::Agent(name) => store.agent_by_name(name).ok().map(|a| a.id),
        Actor::Operator(_) => None,
    };
    let composed_body = anyagents_core::quote::compose(&body.text, body.quote.as_ref());
    let mut deliveries = Vec::with_capacity(plan.targets.len());
    for target in &plan.targets {
        match target {
            Target::Agent(name) => {
                let d = deliver_agent(store, &body, name, key.as_deref())?;
                if let Some(ref sid) = sender_agent_id
                    && !d.duplicate
                {
                    let _ =
                        store.record_outbound(sid, &d.to, body.reply_to.as_deref(), &composed_body);
                }
                deliveries.push(d);
            }
            Target::Room(room) => {
                let mut all = deliver_room(store, &body, room, key.as_deref())?;
                if let Some(ref sid) = sender_agent_id
                    && all.iter().any(|d| !d.duplicate)
                {
                    let dest = if room.starts_with("group:") || room.starts_with("telegram:") {
                        room.clone()
                    } else {
                        format!("group:{room}")
                    };
                    let _ =
                        store.record_outbound(sid, &dest, body.reply_to.as_deref(), &composed_body);
                }
                deliveries.append(&mut all);
            }
            Target::Telegram {
                address,
                chat,
                thread,
                reply: address_reply,
            } => {
                let d = deliver_telegram(
                    store,
                    actor,
                    &body,
                    address,
                    chat,
                    thread.as_deref(),
                    address_reply.as_deref().or(reply.as_deref()),
                )?;
                if let Some(ref sid) = sender_agent_id {
                    let _ =
                        store.record_outbound(sid, &d.to, body.reply_to.as_deref(), &composed_body);
                }
                deliveries.push(d);
            }
        }
    }
    let response = SendResponse { deliveries };
    if let Some(key) = key.as_deref() {
        let text = serde_json::to_string(&response)
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
        store
            .record_send_receipt(key, &text)
            .map_err(ApiError::from_store)?;
    }
    Ok(Json(response))
}

/// The idempotency key of one call. The same call repeated after a lost
/// answer keeps the key its tool call was given; two identical calls without
/// one are two messages, so a key never comes from the text.
fn send_key(namespace: &str, body: &SendRequest) -> Option<String> {
    let idempotency = body.idempotency_key.as_deref()?.trim();
    if idempotency.is_empty() {
        return None;
    }
    let sender = body.sender.as_deref().unwrap_or("");
    let reply_to = body.reply_to.as_deref().unwrap_or("");
    let to_joined = body.to.join("\x1f");
    let mut bytes = Vec::new();
    for part in [
        namespace,
        sender,
        idempotency,
        to_joined.as_str(),
        reply_to,
        body.text.as_str(),
    ] {
        bytes.extend_from_slice(part.as_bytes());
        bytes.push(0);
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in &bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Some(format!("send:{hash:016x}"))
}

/// Who writes a message.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Actor {
    Agent(String),
    /// The operator, as an explicit principal: `operator`, `telegram:user:<id>`
    /// or `telegram:chat:<id>`.
    Operator(String),
}

impl Actor {
    fn parse(raw: &str) -> Result<Self, ApiError> {
        if raw.trim() == "operator" {
            return Ok(Self::Operator("operator".to_string()));
        }
        match anyagents_core::Address::parse(raw) {
            Ok(anyagents_core::Address::Agent(name)) => Ok(Self::Agent(name)),
            Ok(
                address @ (anyagents_core::Address::TelegramUser { .. }
                | anyagents_core::Address::TelegramChat { .. }),
            ) => Ok(Self::Operator(address.canonical())),
            Ok(other) => Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("{other} cannot write a message"),
            )),
            Err(err) => Err(ApiError::new(StatusCode::BAD_REQUEST, err.to_string())),
        }
    }

    /// The sender stored with the message.
    fn as_address(&self) -> String {
        match self {
            Self::Agent(name) => format!("agent:{name}"),
            Self::Operator(raw) => raw.clone(),
        }
    }
}

/// Everyone a room message will reach, without writing anything: the room's
/// members (its chat, when it names one) minus the sender.
fn room_recipients(store: &Store, body: &SendRequest, room: &str) -> Vec<Agent> {
    let sender = body
        .sender
        .as_deref()
        .map(|value| value.strip_prefix("agent:").unwrap_or(value).to_string());
    let members = match store.room_members(room) {
        Ok(members) => members,
        Err(_) => return Vec::new(),
    };
    store
        .list_agents()
        .unwrap_or_default()
        .into_iter()
        .filter(|agent| agent.state != "archived" && agent.state != "paused")
        .filter(|agent| sender.as_deref() != Some(agent.name.as_str()))
        .filter(|agent| {
            members
                .as_ref()
                .is_none_or(|members| members.iter().any(|name| name == &agent.name))
        })
        .collect()
}

/// One room message, one delivery per recipient.
fn deliver_room(
    store: &Store,
    body: &SendRequest,
    room: &str,
    key: Option<&str>,
) -> Result<Vec<Delivery>, ApiError> {
    let delivered = store
        .receive_room(
            room,
            Incoming {
                text: body.text.clone(),
                sender: body.sender.clone(),
                source_time_ms: None,
                source_key: key.map(str::to_string),
                reply_to: body.reply_to.clone(),
                quote: body.quote.clone(),
                origin: None,
                received_at_ms: None,
            },
        )
        .map_err(ApiError::from_store)?;
    Ok(delivered
        .into_iter()
        .map(|(agent, outcome)| {
            let (message, duplicate) = match outcome {
                ReceiveOutcome::New(message) => (message, false),
                ReceiveOutcome::Duplicate(message) => (message, true),
            };
            Delivery {
                to: format!("agent:{}", agent.name),
                id: message.id.to_string(),
                seq: message.seq,
                duplicate,
            }
        })
        .collect())
}

fn deliver_agent(
    store: &Store,
    body: &SendRequest,
    name: &str,
    key: Option<&str>,
) -> Result<Delivery, ApiError> {
    let agent = store.agent_by_name(name).map_err(ApiError::from_store)?;
    let outcome = store
        .receive(
            &agent.id,
            Incoming {
                text: body.text.clone(),
                sender: body.sender.clone(),
                source_time_ms: None,
                source_key: key.map(str::to_string),
                reply_to: body.reply_to.clone(),
                quote: body.quote.clone(),
                origin: None,
                received_at_ms: None,
            },
        )
        .map_err(ApiError::from_store)?;
    let (message, duplicate) = match outcome {
        ReceiveOutcome::New(message) => (message, false),
        ReceiveOutcome::Duplicate(message) => (message, true),
    };
    Ok(Delivery {
        to: format!("agent:{}", agent.name),
        id: message.id.to_string(),
        seq: message.seq,
        duplicate,
    })
}

/// Resolve the bot, the chat, the topic and the reply before any network
/// call. This does not post.
fn prepare_telegram(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
) -> Result<(), ApiError> {
    let from = telegram_sender(actor)?;
    let (chat_id, thread_id, reply_to) = telegram_target(chat, thread, reply, body)?;
    crate::telegram::outbound_call(store, from, chat_id, &body.text, reply_to, thread_id)
        .map_err(ApiError::from_store)?;
    Ok(())
}

/// The bot a send posts through: always the authenticated actor's own.
fn telegram_sender(actor: &Actor) -> Result<&str, ApiError> {
    match actor {
        Actor::Agent(name) => Ok(name),
        Actor::Operator(_) => Err(ApiError::forbidden(
            "a post to a channel needs an agent's own bot",
        )),
    }
}

/// The chat, forum topic and replied-to message as numbers. The explicit
/// `reply_to` wins over the one an address names.
fn telegram_target(
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
    body: &SendRequest,
) -> Result<(i64, Option<i64>, Option<i64>), ApiError> {
    let chat_id: i64 = chat
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "telegram chat id is not a number"))?;
    let thread_id = match thread {
        None => None,
        Some(value) => Some(number(value, "telegram topic id is not a number")?),
    };
    let reply_to = telegram_reply(body, reply)?;
    Ok((chat_id, thread_id, reply_to))
}

fn number(value: &str, what: &'static str) -> Result<i64, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, what))
}

fn telegram_reply(body: &SendRequest, reply: Option<&str>) -> Result<Option<i64>, ApiError> {
    let candidate = reply.or_else(|| {
        body.reply_to
            .as_deref()
            .filter(|v| crate::reply::ref_message(v).is_none() && Address::parse(v).is_err())
    });
    match candidate {
        None => Ok(None),
        Some(value) => number(value, "reply_to is not a telegram message id").map(Some),
    }
}

fn format_internal_send_text(body: &SendRequest) -> String {
    let mut parts = Vec::new();
    if !body.text.trim().is_empty() {
        parts.push(body.text.clone());
    }
    if let Some(ref media) = body.media {
        parts.push(format!("[Attachment ({}): {}]", media.kind, media.source));
    }
    if let Some(ref loc) = body.location {
        match (&loc.title, &loc.address) {
            (Some(t), Some(a)) if !t.is_empty() && !a.is_empty() => {
                parts.push(format!(
                    "[Venue: {t} — {a} (GPS: {:.6}, {:.6})]",
                    loc.latitude, loc.longitude
                ));
            }
            _ => {
                parts.push(format!(
                    "[Location GPS: latitude={:.6}, longitude={:.6}]",
                    loc.latitude, loc.longitude
                ));
            }
        }
    }
    if let Some(ref poll) = body.poll {
        parts.push(format!(
            "[Poll: \"{}\" | Options: {}]",
            poll.question,
            poll.options.join(" / ")
        ));
    }
    if let Some(ref cl) = body.checklist {
        parts.push(anyagents_channel_telegram::format_checklist_markdown(
            &cl.title, &cl.tasks,
        ));
    }
    if let Some(ref art) = body.article {
        match (&art.url, &art.content) {
            (Some(u), _) if !u.trim().is_empty() => {
                parts.push(format!("📰 [**{}**]({})", art.title, u.trim()));
            }
            (_, Some(c)) if !c.trim().is_empty() => {
                parts.push(format!("📰 **{}**\n\n{}", art.title, c.trim()));
            }
            _ => {
                parts.push(format!("📰 **{}**", art.title));
            }
        }
    }
    if parts.is_empty() {
        body.text.clone()
    } else {
        parts.join("\n")
    }
}

/// `id` is the Telegram message id. This delivery has no mailbox sequence.
#[allow(clippy::too_many_arguments)]
fn deliver_telegram(
    store: &Store,
    actor: &Actor,
    body: &SendRequest,
    address: &str,
    chat: &str,
    thread: Option<&str>,
    reply: Option<&str>,
) -> Result<Delivery, ApiError> {
    let from = telegram_sender(actor)?;
    let (chat_id, thread_id, reply_to) = telegram_target(chat, thread, reply, body)?;
    let message_id = if let Some(ref media) = body.media {
        let caption = body
            .text
            .strip_suffix(&format!(
                "\n[Attachment ({}): {}]",
                media.kind, media.source
            ))
            .or_else(|| {
                body.text
                    .strip_prefix(&format!("[Attachment ({}): {}]", media.kind, media.source))
            })
            .map(str::trim)
            .filter(|s| !s.is_empty());
        crate::telegram::post_media(
            store,
            store.data_dir(),
            from,
            chat_id,
            &media.kind,
            &media.source,
            caption,
            media.title.as_deref(),
            media.performer.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref loc) = body.location {
        crate::telegram::post_location(
            store,
            store.data_dir(),
            from,
            chat_id,
            loc.latitude,
            loc.longitude,
            loc.title.as_deref(),
            loc.address.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref poll) = body.poll {
        crate::telegram::post_poll(
            store,
            store.data_dir(),
            from,
            chat_id,
            &poll.question,
            &poll.options,
            poll.is_anonymous,
            poll.allows_multiple_answers,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref cl) = body.checklist {
        crate::telegram::post_checklist(
            store,
            store.data_dir(),
            from,
            chat_id,
            &cl.title,
            &cl.tasks,
            cl.business_connection_id.as_deref(),
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else if let Some(ref art) = body.article {
        crate::telegram::post_article(
            store,
            store.data_dir(),
            from,
            chat_id,
            &art.title,
            art.content.as_deref(),
            art.url.as_deref(),
            art.author_name.as_deref(),
            None,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    } else {
        crate::telegram::post_text(
            store,
            store.data_dir(),
            from,
            chat_id,
            &body.text,
            reply_to,
            thread_id,
        )
        .map_err(ApiError::from_store)?
    };
    // Remember the chat this agent answers in, so a post it makes there reaches
    // the other agents on that chat as a mailbox message.
    crate::telegram::record_chat(store, from, chat_id).map_err(ApiError::from_store)?;
    Ok(Delivery {
        to: address.to_string(),
        id: message_id.to_string(),
        seq: 0,
        duplicate: false,
    })
}

async fn spawn_store<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(err) => Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )),
    }
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    fn from_store(err: Error) -> Self {
        let status = match &err {
            Error::AgentNotFound(_) => StatusCode::NOT_FOUND,
            Error::AgentExists(_) => StatusCode::CONFLICT,
            Error::InvalidAddress(_) | Error::Message(_) => StatusCode::BAD_REQUEST,
            Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::new(status, err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

/// The working directory every owner agent shares, when the deployment sets one.
/// `ANYAGENTS_OWNER_WORKDIR` names it; without it an agent keeps its own
/// managed directory.
pub fn owner_workdir() -> Option<std::path::PathBuf> {
    std::env::var_os("ANYAGENTS_OWNER_WORKDIR").map(std::path::PathBuf::from)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::MutexGuard;

    /// Answers one sign-in with two lines and records where it ran.
    struct Scripted {
        dir: Arc<Mutex<Option<PathBuf>>>,
    }

    impl CommandRunner for Scripted {
        fn run(
            &mut self,
            _program: &str,
            _args: &[String],
            dir: &Path,
            env: &[(&str, &str)],
            on_line: &mut dyn LineSink,
        ) -> Result<(), InstallError> {
            *self.dir.lock().unwrap() = Some(dir.to_path_buf());
            assert!(env.iter().any(|(key, _)| *key == "CODEX_HOME"));
            on_line.line("LINK https://auth.example/device");
            on_line.line("SIGNED_IN ada@example");
            Ok(())
        }
    }

    /// These tests start a server in this process, so the environment they set
    /// is read under this lock and put back before the next one runs.
    static ENV: Mutex<()> = Mutex::new(());

    /// One lock for every test that sets this process's environment, so a
    /// server started by one never reads another's settings.
    pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
        ENV.lock().unwrap_or_else(|err| err.into_inner())
    }

    #[test]
    fn a_sign_in_streams_the_link_before_the_outcome() {
        let _env = env_lock();
        let data = tempfile::tempdir().unwrap();
        let socket = socket_path(data.path());
        let dir = Arc::new(Mutex::new(None));
        let runner = Arc::new(Mutex::new(Scripted {
            dir: Arc::clone(&dir),
        }));
        let data_path = data.path().to_path_buf();
        // This deployment has an operator, so the sign-in presents one.
        let token_file = data.path().join("operator-token");
        std::fs::write(&token_file, "an-operator-token-0123456789abcdef\n").unwrap();
        // SAFETY: the test process sets these before the server thread reads
        // them, and sets them for its own duration only.
        unsafe {
            std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_file);
            // One OS seat is all this host has: this process's own, kept out of
            // root by the launcher check.
            std::env::set_var("ANYAGENTS_AGENT_UID", libc::getuid().to_string());
            std::env::set_var("ANYAGENTS_AGENT_GID", libc::getgid().to_string());
            std::env::set_var("ANYAGENTS_SOCKET_GID", libc::getgid().to_string());
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let started = Arc::new(Mutex::new(None));
        let outcome = Arc::clone(&started);
        let server = std::thread::spawn(move || {
            let result = run_with(&data_path, runner, stop_rx);
            if let Err(err) = &result {
                *outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.to_string());
            }
            result
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !socket.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            socket.exists(),
            "the server did not start: {:?}",
            started.lock().unwrap_or_else(|e| e.into_inner())
        );
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(
                format!(
                    "POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\n{}: an-operator-token-0123456789abcdef\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    crate::operator::HEADER
                )
                .as_bytes(),
            )
            .unwrap();
        let mut buf = String::new();
        let mut chunk = [0u8; 256];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline && !buf.contains("\"kind\":\"done\"") {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.push_str(&String::from_utf8_lossy(&chunk[..n])),
                Err(_) => break,
            }
        }
        let link = buf.find("\"kind\":\"link\"").expect(&buf);
        let done = buf.find("\"kind\":\"done\"").expect(&buf);
        assert!(link < done, "{buf}");
        assert!(buf.contains("https://auth.example/device"), "{buf}");
        assert!(buf.contains("ada@example"), "{buf}");
        let recorded = dir.lock().unwrap().clone().unwrap();
        assert!(recorded.ends_with("config/codex/default"), "{recorded:?}");
        drop(stream);
        let _ = stop_tx.send(());
        let _ = server.join();
        // SAFETY: as above, for this test only.
        unsafe {
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
            std::env::remove_var("ANYAGENTS_AGENT_UID");
            std::env::remove_var("ANYAGENTS_AGENT_GID");
            std::env::remove_var("ANYAGENTS_SOCKET_GID");
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
            std::env::remove_var("ANYAGENTS_LAUNCHER");
        }
    }

    /// A sign-in call from an agent's own capability is refused, and no vendor
    /// command runs.
    #[test]
    fn an_agent_capability_cannot_sign_a_harness_in() {
        let _env = env_lock();
        let data = tempfile::tempdir().unwrap();
        let socket = socket_path(data.path());
        let dir = Arc::new(Mutex::new(None));
        let runner = Arc::new(Mutex::new(Scripted {
            dir: Arc::clone(&dir),
        }));
        let data_path = data.path().to_path_buf();
        let token_file = data.path().join("operator-token");
        std::fs::write(&token_file, "an-operator-token-0123456789abcdef\n").unwrap();
        // SAFETY: the test process sets these before the server thread reads
        // them, and sets them for its own duration only.
        unsafe {
            std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_file);
            std::env::set_var(
                "ANYAGENTS_CAPABILITY_KEY",
                "0123456789abcdef0123456789abcdef",
            );
        }
        // SAFETY: as above, for this test's own duration under the lock.
        unsafe {
            std::env::set_var("ANYAGENTS_AGENT_UID", libc::getuid().to_string());
            std::env::set_var("ANYAGENTS_AGENT_GID", libc::getgid().to_string());
            std::env::set_var("ANYAGENTS_SOCKET_GID", libc::getgid().to_string());
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        let store = Store::open(data.path().join("state.db")).unwrap();
        let configured = "0123456789abcdef0123456789abcdef";
        store.hold_capability_key(configured).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        drop(store);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let started = Arc::new(Mutex::new(None));
        let outcome = Arc::clone(&started);
        let server = std::thread::spawn(move || {
            let result = run_with(&data_path, runner, stop_rx);
            if let Err(err) = &result {
                *outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.to_string());
            }
            result
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !socket.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            socket.exists(),
            "the server did not start: {:?}",
            started.lock().unwrap_or_else(|e| e.into_inner())
        );
        // The server mints each agent's capability when it starts, so this test
        // reads the one it left and the server recognises.
        let capability = std::fs::read_to_string(
            data.path()
                .join("agents")
                .join(agent.id.as_str())
                .join("config")
                .join(crate::operator::CAPABILITY_FILE),
        )
        .unwrap()
        .trim()
        .to_string();
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(
                format!(
                    "POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\n{}: {capability}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    crate::operator::AGENT_HEADER
                )
                .as_bytes(),
            )
            .unwrap();
        let mut buf = String::new();
        let _ = stream.read_to_string(&mut buf);
        assert!(buf.starts_with("HTTP/1.1 403"), "{buf}");
        assert!(buf.contains("cannot administer"), "{buf}");
        assert!(dir.lock().unwrap().is_none(), "the vendor command ran");
        drop(stream);
        let _ = stop_tx.send(());
        let _ = server.join();
        // SAFETY: as above, under `ENV`.
        unsafe {
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
            std::env::remove_var("ANYAGENTS_CAPABILITY_KEY");
            std::env::remove_var("ANYAGENTS_AGENT_UID");
            std::env::remove_var("ANYAGENTS_AGENT_GID");
            std::env::remove_var("ANYAGENTS_SOCKET_GID");
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
            std::env::remove_var("ANYAGENTS_LAUNCHER");
        }
    }
}
