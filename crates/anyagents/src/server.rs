//! HTTP/JSON mailbox API on a user-only unix socket.

use std::io::{BufRead, BufReader};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use anyagents_core::{Address, Agent, Error, Incoming, ReceiveOutcome, Store, now_ms};
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
struct AppState {
    store: Arc<Store>,
    health: HealthBoard,
    sign_in: Arc<Mutex<dyn CommandRunner + Send>>,
}

async fn serve(data: &Path, sign_in: Arc<Mutex<dyn CommandRunner + Send>>) -> Result<(), Error> {
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
        sign_in,
    };
    let listener = tokio::net::UnixListener::bind(&path)?;
    set_socket_mode(&path)?;
    let (poll_stop_tx, poll_stop_rx) = tokio::sync::watch::channel(false);
    let poller = tokio::spawn(crate::telegram::run(
        Arc::clone(&state.store),
        data.to_path_buf(),
        poll_stop_rx,
    ));
    let app = Router::new()
        .route("/v1/agents", get(list_agents).post(create_agent_route))
        .route("/v1/agents/{name}/inbox", get(inbox))
        .route("/v1/send", post(send))
        .route("/v1/harnesses/{id}/sign-in", post(sign_in_route))
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
    let _ = poll_stop_tx.send(true);
    poller.abort();
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

/// One line of a sign-in, as the client reads it.
#[derive(serde::Serialize)]
struct SignInLine {
    kind: &'static str,
    text: String,
}

/// `POST /v1/harnesses/{id}/sign-in`. The body stays open while the vendor
/// command runs. Each link or code is one JSON line, and the last line is the
/// outcome. The credential never appears here.
async fn sign_in_route(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Response, ApiError> {
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(16);
    let runner = Arc::clone(&state.sign_in);
    let root = state.store.data_dir().to_path_buf();
    let spawned = tokio::task::spawn_blocking(move || {
        let mut sink = ChannelSink { tx };
        let mut guard = runner.lock().unwrap_or_else(|err| err.into_inner());
        let result = sign_in(
            &builtin_catalog(),
            &root,
            &id,
            "default",
            &mut *guard,
            &mut sink,
        );
        let line = match &result {
            Ok(outcome) => SignInLine {
                kind: "done",
                text: outcome.account.clone().unwrap_or_default(),
            },
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
        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in env {
            command.env(key, value);
        }
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
            failed_reason: message.failed_reason,
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
    enum Target {
        Agent(String),
        Telegram { address: String, chat: String },
    }
    let mut targets = Vec::with_capacity(body.to.len());
    for raw in &body.to {
        match Address::parse(raw)
            .map_err(Error::from)
            .map_err(ApiError::from_store)?
        {
            Address::Agent(name) => targets.push(Target::Agent(name)),
            Address::TelegramChat(id) | Address::TelegramUser(id) => {
                targets.push(Target::Telegram {
                    address: raw.clone(),
                    chat: id,
                });
            }
            Address::Group(_) => {
                return Err(ApiError::from_store(Error::NotImplemented(
                    "send to a group".into(),
                )));
            }
        }
    }
    for target in &targets {
        match target {
            Target::Agent(name) => {
                store.agent_by_name(name).map_err(ApiError::from_store)?;
            }
            Target::Telegram { chat, .. } => {
                prepare_telegram(store, &body, chat)?;
            }
        }
    }
    let mut deliveries = Vec::with_capacity(targets.len());
    for target in targets {
        let delivery = match target {
            Target::Agent(name) => deliver_agent(store, &body, &name)?,
            Target::Telegram { address, chat } => deliver_telegram(store, &body, &address, &chat)?,
        };
        deliveries.push(delivery);
    }
    Ok(Json(SendResponse { deliveries }))
}

fn deliver_agent(store: &Store, body: &SendRequest, name: &str) -> Result<Delivery, ApiError> {
    let agent = store.agent_by_name(name).map_err(ApiError::from_store)?;
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
    Ok(Delivery {
        to: format!("agent:{}", agent.name),
        id: message.id.to_string(),
        seq: message.seq,
        duplicate,
    })
}

/// Resolve the bot and the chat before any mailbox write. This does not post.
fn prepare_telegram(store: &Store, body: &SendRequest, chat: &str) -> Result<(), ApiError> {
    let chat_id: i64 = chat
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "telegram chat id is not a number"))?;
    let reply_to = telegram_reply(body)?;
    let from =
        crate::telegram::bound_sender(body.sender.as_deref()).map_err(ApiError::from_store)?;
    crate::telegram::outbound_call(store, from.as_deref(), chat_id, &body.text, reply_to)
        .map_err(ApiError::from_store)?;
    Ok(())
}

fn telegram_reply(body: &SendRequest) -> Result<Option<i64>, ApiError> {
    match body.reply_to.as_deref() {
        None => Ok(None),
        Some(value) => Ok(Some(value.parse().map_err(|_| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "reply_to is not a telegram message id",
            )
        })?)),
    }
}

/// `id` is the Telegram message id. This delivery has no mailbox sequence.
fn deliver_telegram(
    store: &Store,
    body: &SendRequest,
    address: &str,
    chat: &str,
) -> Result<Delivery, ApiError> {
    let chat_id: i64 = chat
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "telegram chat id is not a number"))?;
    let reply_to = telegram_reply(body)?;
    let from =
        crate::telegram::bound_sender(body.sender.as_deref()).map_err(ApiError::from_store)?;
    let message_id = crate::telegram::post_text(
        store,
        store.data_dir(),
        from.as_deref(),
        chat_id,
        &body.text,
        reply_to,
    )
    .map_err(ApiError::from_store)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

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

    #[test]
    fn a_sign_in_streams_the_link_before_the_outcome() {
        let data = tempfile::tempdir().unwrap();
        let socket = socket_path(data.path());
        let dir = Arc::new(Mutex::new(None));
        let runner = Arc::new(Mutex::new(Scripted {
            dir: Arc::clone(&dir),
        }));
        let data_path = data.path().to_path_buf();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = std::thread::spawn(move || run_with(&data_path, runner, stop_rx));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !socket.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(b"POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
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
    }
}
