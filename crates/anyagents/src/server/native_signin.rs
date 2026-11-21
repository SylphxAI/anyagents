//! Operator-only, streamed native login. The login runs in an isolated home and
//! never receives a destination profile from the browser.

mod identity;
mod sandbox;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use anyagents_core::stateless::generate_callback_token;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::{Json, Router};
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use url::Url;

use super::types::{ApiError, AppState, SignInLine, require_operator};
use super::{in_flight, serverless};
use crate::auth::Principal;

const MAX_INPUT: usize = 8192;
const MAX_SESSIONS: usize = 8;
type Output = mpsc::Sender<String>;
struct Input {
    value: String,
    result: oneshot::Sender<Result<(), &'static str>>,
}
struct Session {
    store: Arc<anyagents_core::Store>,
    input: mpsc::Sender<Input>,
}
static SESSIONS: LazyLock<Mutex<HashMap<String, Session>>> = LazyLock::new(Mutex::default);

struct Lease(String);
impl Drop for Lease {
    fn drop(&mut self) {
        SESSIONS
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove(&self.0);
    }
}

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/profiles/native-signin", post(start_route))
        .route("/v1/profiles/native-signin/{id}/input", post(input_route))
        .layer(DefaultBodyLimit::max(MAX_INPUT * 2 + 128))
}

#[derive(Deserialize)]
struct StartRequest {
    #[serde(alias = "provider")]
    harness: String,
    // Unknown fields (including the legacy `profile`) never influence persistence.
}

async fn start_route(
    principal: Principal,
    State(state): State<AppState>,
    Json(body): Json<StartRequest>,
) -> Result<Response, ApiError> {
    start(principal, state, &body.harness).await
}

pub(super) fn canonical_harness(raw: &str) -> &str {
    match anyagents_harness::normalize_harness_name(raw) {
        "anthropic" => "claude-code",
        "openai" => "codex",
        "google-antigravity" => "agy",
        "deepseek" => "dsh",
        "xai" => "grok",
        other => other,
    }
}

pub(super) async fn start(
    principal: Principal,
    state: AppState,
    raw: &str,
) -> Result<Response, ApiError> {
    require_operator(&principal)?;
    let spec = sandbox::Spec::for_harness(canonical_harness(raw)).ok_or_else(|| ApiError::new(
        StatusCode::BAD_REQUEST, "Native sign-in supports Google Antigravity, Claude and OpenAI / Codex. For other providers, use their credential setup."))?;
    let serverless = serverless::is_serverless_mode();
    start_session(state, move |_, state, output, input| async move {
        sandbox::run(state, spec, serverless, &output, input).await
    })
}

fn start_session<F, Fut>(state: AppState, run: F) -> Result<Response, ApiError>
where
    F: FnOnce(String, AppState, Output, mpsc::Receiver<Input>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
{
    let id = generate_callback_token();
    let (output, rx) = mpsc::channel(16);
    let (input, requests) = mpsc::channel(1);
    {
        let mut sessions = SESSIONS.lock().unwrap_or_else(|err| err.into_inner());
        if sessions.len() >= MAX_SESSIONS {
            return Err(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "Too many active sign-ins. Close an existing sign-in and try again.",
            ));
        }
        sessions.insert(
            id.clone(),
            Session {
                store: Arc::clone(&state.store),
                input,
            },
        );
    }
    let lease = Lease(id.clone());
    let call = in_flight::carry();
    tokio::spawn(async move {
        let _call = call;
        send(&output, "session", id.clone()).await;
        let outcome = run(id, state, output.clone(), requests).await;
        drop(lease);
        match outcome {
            Ok(label) => send(&output, "done", label).await,
            Err(err) => send(&output, "error", err).await,
        }
    });
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .header("cache-control", "no-store, private")
        .header("x-accel-buffering", "no")
        .body(Body::from_stream(
            stream.map(|line| Ok::<_, std::io::Error>(format!("{line}\n"))),
        ))
        .expect("static sign-in headers"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputRequest {
    value: String,
}

async fn input_route(
    principal: Principal,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<InputRequest>,
) -> Result<StatusCode, ApiError> {
    require_operator(&principal)?;
    let value = body.value.trim();
    if value.is_empty()
        || value.len() > MAX_INPUT
        || value.chars().any(char::is_whitespace)
        || value.chars().any(char::is_control)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Paste one callback URL or authorization code, without spaces.",
        ));
    }
    let input = SESSIONS
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .get(&id)
        .filter(|session| Arc::ptr_eq(&session.store, &state.store))
        .map(|session| session.input.clone());
    let expired = || {
        ApiError::new(
            StatusCode::GONE,
            "This sign-in session is no longer available on this coordinator. Start a new sign-in.",
        )
    };
    let (result, reply) = oneshot::channel();
    input
        .ok_or_else(expired)?
        .try_send(Input {
            value: value.to_owned(),
            result,
        })
        .map_err(|err| match err {
            mpsc::error::TrySendError::Closed(_) => expired(),
            mpsc::error::TrySendError::Full(_) => ApiError::new(
                StatusCode::CONFLICT,
                "A callback is already pending. Wait before trying again.",
            ),
        })?;
    match tokio::time::timeout(std::time::Duration::from_secs(18), reply).await {
        Ok(Ok(Ok(()))) => Ok(StatusCode::NO_CONTENT),
        Ok(Ok(Err(message))) => Err(ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, message)),
        Ok(Err(_)) => Err(expired()),
        Err(_) => Err(ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "The sandbox did not confirm receipt. Check the sign-in status before retrying.",
        )),
    }
}

async fn send(output: &Output, kind: &'static str, text: impl Into<String>) {
    if let Ok(line) = serde_json::to_string(&SignInLine {
        kind,
        text: text.into(),
    }) {
        let _ = output.send(line).await;
    }
}

fn is_loopback(uri: &Url) -> bool {
    matches!(uri.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
}

#[cfg(test)]
mod tests;
