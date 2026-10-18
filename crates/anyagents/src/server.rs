//! HTTP/JSON mailbox API on a user-only unix socket.

use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyagents_core::{Address, Agent, Error, Incoming, ReceiveOutcome, Store, now_ms};
use anyagents_supervisor::{HealthBoard, Supervisor, SystemClock, declared, mode_name, steer_name};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::{
    AgentView, AgentsResponse, CreateAgentRequest, Delivery, InboxResponse, MessageView,
    SendRequest, SendResponse,
};
use crate::create::{self, CreateRequest};
use crate::vendor::{self, VendorHarness};

const SOCKET_NAME: &str = "anyagents.sock";
/// `sockaddr_un.sun_path` holds 108 bytes, including the trailing NUL.
const MAX_SOCKET_PATH: usize = 107;

pub fn socket_path(data: &Path) -> PathBuf {
    data.join(SOCKET_NAME)
}

pub fn run(data: &Path) -> Result<(), Error> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(serve(data))
}

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    health: HealthBoard,
}

async fn serve(data: &Path) -> Result<(), Error> {
    let store = Arc::new(Store::open(data.join("state.db"))?);
    let path = socket_path(data);
    if path.as_os_str().len() > MAX_SOCKET_PATH {
        return Err(Error::Message(format!(
            "socket path is longer than {MAX_SOCKET_PATH} bytes"
        )));
    }
    prepare_socket(&path)?;
    let supervisor = Supervisor::<VendorHarness>::start(
        Arc::clone(&store),
        Arc::new(SystemClock::new()),
        Arc::new(vendor::vendor_launch),
        vendor::child_env(),
    )
    .map_err(Error::Message)?;
    let state = AppState {
        store,
        health: supervisor.board(),
    };
    let listener = tokio::net::UnixListener::bind(&path)?;
    set_socket_mode(&path)?;
    let app = Router::new()
        .route("/v1/agents", get(list_agents).post(create_agent_route))
        .route("/v1/agents/{name}/inbox", get(inbox))
        .route("/v1/send", post(send))
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
    drop(supervisor);
    let _ = std::fs::remove_file(&path);
    result.map_err(Error::from)
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

async fn list_agents(State(state): State<AppState>) -> Result<Json<AgentsResponse>, ApiError> {
    spawn_store(move || list_agents_sync(&state)).await
}

fn list_agents_sync(state: &AppState) -> Result<Json<AgentsResponse>, ApiError> {
    let mut agents = Vec::new();
    for agent in state.store.list_agents().map_err(ApiError::from_store)? {
        agents.push(agent_view(&state.store, &state.health, &agent)?);
    }
    Ok(Json(AgentsResponse { agents }))
}

fn agent_view(store: &Store, health: &HealthBoard, agent: &Agent) -> Result<AgentView, ApiError> {
    let lag = store
        .lag(&agent.id, now_ms())
        .map_err(ApiError::from_store)?;
    let (process, mode, steer, quota) = match health.get(&agent.id) {
        Some(live) => (
            if live.process_alive { "alive" } else { "down" },
            mode_name(live.delivery_mode),
            steer_name(live.steer_support),
            live.quota_reset_at_ms,
        ),
        None => {
            let (mode, steer) = declared(&agent.harness);
            ("down", mode_name(mode), steer_name(steer), None)
        }
    };
    Ok(AgentView {
        id: agent.id.to_string(),
        name: agent.name.clone(),
        harness: agent.harness.clone(),
        model: agent.model.clone(),
        workdir: agent.workdir.clone(),
        state: agent.state.clone(),
        process: process.to_string(),
        delivery_mode: mode.to_string(),
        steer_support: steer.to_string(),
        quota_reset_at_ms: quota,
        oldest_unaccepted_age_ms: lag.oldest_unaccepted_age_ms,
        oldest_unprocessed_age_ms: lag.oldest_unprocessed_age_ms,
    })
}

async fn create_agent_route(
    State(state): State<AppState>,
    Json(body): Json<CreateAgentRequest>,
) -> Result<(StatusCode, Json<AgentView>), ApiError> {
    spawn_store(move || {
        let agent = create::create(
            &state.store,
            state.store.data_dir(),
            CreateRequest {
                name: body.name,
                harness: body.harness,
                model: body.model,
                workdir: body.workdir.map(PathBuf::from),
                instructions: body.instructions,
            },
        )
        .map_err(ApiError::from_store)?;
        let view = agent_view(&state.store, &state.health, &agent)?;
        Ok((StatusCode::CREATED, Json(view)))
    })
    .await
}

#[derive(Deserialize)]
struct Since {
    since: Option<i64>,
}

async fn inbox(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<Since>,
) -> Result<Json<InboxResponse>, ApiError> {
    spawn_store(move || inbox_sync(&state.store, &name, query.since)).await
}

fn inbox_sync(
    store: &Store,
    name: &str,
    since: Option<i64>,
) -> Result<Json<InboxResponse>, ApiError> {
    let agent = store.agent_by_name(name).map_err(ApiError::from_store)?;
    let messages = store
        .inbox(&agent.id, since)
        .map_err(ApiError::from_store)?
        .into_iter()
        .map(|message| MessageView {
            id: message.id.to_string(),
            seq: message.seq,
            text: message.text,
            sender: message.sender,
            reply_to: message.reply_to,
            source_time_ms: message.source_time_ms,
            received_at_ms: message.received_at_ms,
            ack: message.ack.to_string(),
        })
        .collect();
    Ok(Json(InboxResponse { messages }))
}

async fn send(
    State(state): State<AppState>,
    Json(body): Json<SendRequest>,
) -> Result<Json<SendResponse>, ApiError> {
    spawn_store(move || send_sync(&state.store, body)).await
}

fn send_sync(store: &Store, body: SendRequest) -> Result<Json<SendResponse>, ApiError> {
    if body.to.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "send needs an address",
        ));
    }
    let mut names = Vec::with_capacity(body.to.len());
    for raw in &body.to {
        match Address::parse(raw).map_err(Error::from) {
            Ok(Address::Agent(name)) => names.push(name),
            Ok(Address::Group(_) | Address::TelegramChat(_) | Address::TelegramUser(_)) => {
                return Err(ApiError::from_store(Error::NotImplemented(
                    "send to a group or a telegram target".into(),
                )));
            }
            Err(err) => return Err(ApiError::from_store(err)),
        }
    }
    let mut agents = Vec::with_capacity(names.len());
    for name in &names {
        agents.push(store.agent_by_name(name).map_err(ApiError::from_store)?);
    }
    let mut deliveries = Vec::with_capacity(agents.len());
    for agent in &agents {
        let outcome = store
            .receive(
                &agent.id,
                Incoming {
                    text: body.text.clone(),
                    sender: body.sender.clone(),
                    source_time_ms: None,
                    source_key: None,
                    reply_to: body.reply_to.clone(),
                    received_at_ms: None,
                },
            )
            .map_err(ApiError::from_store)?;
        let (message, duplicate) = match outcome {
            ReceiveOutcome::New(message) => (message, false),
            ReceiveOutcome::Duplicate(message) => (message, true),
        };
        deliveries.push(Delivery {
            to: format!("agent:{}", agent.name),
            id: message.id.to_string(),
            seq: message.seq,
            duplicate,
        });
    }
    Ok(Json(SendResponse { deliveries }))
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
