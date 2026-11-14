//! A scripted Claude Managed Agents API on loopback, speaking the documented
//! REST shapes: environments, agents, vaults and credentials, sessions with a
//! dollar budget, `user.message` / `user.interrupt` events, the SSE stream
//! (no replay) and the paged event history. It records every request so a
//! test can check what was sent. No request leaves the machine.
//!
//! A session runs one turn per idle `user.message`: `status_running`, the
//! message processed, one model request (usage and cost), an `agent.message`,
//! then `status_idle`. With `hold` set, the turn stays open until
//! [`Vendor::finish`]: messages that arrive meanwhile are queued and folded
//! into the same turn when it finishes. A turn whose cost reaches the budget
//! pauses at `budget_reached`; raising the budget resumes it.
//!
//! Faults are scripted on [`Inner`]: answers lost before or after a message
//! was applied, throttling with `retry-after`, unreadable successes, a slow
//! session read, failed session creates, vanished vaults, and sessions the
//! vendor terminates. Compiled only with the `mock` feature, for tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum::http::HeaderValue;

use anyagents_core::{AgentId, NewAgent, Store};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::sync::mpsc;

pub const KEY: &str = "sk-ant-test-key";
pub const MCP_URL: &str = "https://agents.example.com/v1/webhooks/mcp";

#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub body: Value,
    pub api_key: Option<String>,
    pub beta: Option<String>,
    pub version: Option<String>,
}

#[derive(Default)]
pub struct Session {
    pub events: Vec<Value>,
    pub terminated: bool,
    pub status: String,
    pub paused_at_budget: bool,
    pub budget_cents: Option<i64>,
    pub cost_cents: i64,
    pub queue: Vec<String>,
    pub archived: bool,
    pub vault: Option<String>,
    subscribers: Vec<mpsc::UnboundedSender<Value>>,
}

#[derive(Default)]
pub struct Inner {
    next: u64,
    pub requests: Vec<Request>,
    pub sessions: HashMap<String, Session>,
    /// credential id -> (vault id, token)
    pub credentials: HashMap<String, (String, String)>,
    pub archived_vaults: Vec<String>,
    pub hold: bool,
    pub cost_per_request_cents: i64,
    /// History page size.
    pub page: usize,
    /// Answer `user.message` POSTs with this status after applying them.
    pub fail_after_apply: Option<u16>,
    /// Answer `user.message` POSTs with this status without applying them.
    pub fail_before_apply: Option<u16>,
    /// Sent as `retry-after` (seconds) with a 429 or 529.
    pub retry_after_secs: Option<u64>,
    /// Answer the next `user.message` POST with an unreadable 200 after
    /// applying it.
    pub unreadable_answer: bool,
    /// Delay `GET /v1/sessions/{id}` by this many ms.
    pub snapshot_delay_ms: u64,
    /// Fail this many `POST /v1/sessions` with a 500.
    pub fail_session_creates: u32,
    /// Delay `POST /v1/sessions/{id}/archive` by this many ms.
    pub archive_delay_ms: u64,
}

#[derive(Clone)]
pub struct Vendor {
    inner: Arc<Mutex<Inner>>,
    pub base: String,
    _runtime: Arc<tokio::runtime::Runtime>,
}

impl Vendor {
    pub fn start() -> Self {
        let inner = Arc::new(Mutex::new(Inner {
            cost_per_request_cents: 7,
            page: 3,
            ..Inner::default()
        }));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let app = Router::new()
            .route("/v1/environments", post(environment))
            .route("/v1/agents", post(create_agent))
            .route("/v1/agents/{id}", post(update_agent))
            .route("/v1/vaults", post(vault))
            .route("/v1/vaults/{id}/credentials", post(credential))
            .route("/v1/vaults/{id}/credentials/{cid}", post(update_credential))
            .route("/v1/vaults/{id}/archive", post(archive_vault))
            .route("/v1/sessions", post(create_session))
            .route("/v1/sessions/{id}", get(get_session).post(update_session))
            .route("/v1/sessions/{id}/archive", post(archive_session))
            .route("/v1/sessions/{id}/events", get(history).post(send_events))
            .route("/v1/sessions/{id}/events/stream", get(stream))
            .with_state(Arc::clone(&inner));
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        runtime.spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            inner,
            base,
            _runtime: Arc::new(runtime),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap()
    }

    pub fn set_hold(&self, hold: bool) {
        self.lock().hold = hold;
    }

    pub fn requests(&self, method: &str, path_starts: &str) -> Vec<Request> {
        self.lock()
            .requests
            .iter()
            .filter(|r| r.method == method && r.path.starts_with(path_starts))
            .cloned()
            .collect()
    }

    /// Every `events` POST of one kind (`user.message`, `user.interrupt`).
    pub fn sent(&self, kind: &str) -> Vec<Value> {
        self.lock()
            .requests
            .iter()
            .filter(|r| r.method == "POST" && r.path.ends_with("/events"))
            .flat_map(|r| r.body["events"].as_array().cloned().unwrap_or_default())
            .filter(|e| e["type"] == kind)
            .collect()
    }

    pub fn only_session(&self) -> String {
        let inner = self.lock();
        assert_eq!(inner.sessions.len(), 1, "one session");
        inner.sessions.keys().next().unwrap().clone()
    }

    pub fn status(&self, session: &str) -> String {
        self.lock().sessions[session].status.clone()
    }

    /// Close every live stream of the session: the client must reconnect.
    pub fn drop_streams(&self, session: &str) {
        self.lock()
            .sessions
            .get_mut(session)
            .unwrap()
            .subscribers
            .clear();
    }

    /// Finish the held turn: queued messages are folded in, then it ends.
    pub fn finish(&self, session: &str) {
        let mut inner = self.lock();
        let inner = &mut *inner;
        let queue = std::mem::take(&mut inner.sessions.get_mut(session).unwrap().queue);
        for id in queue {
            process(inner, session, &id);
        }
        end_turn(inner, session);
    }

    /// The vendor ends the session for good.
    pub fn terminate(&self, session: &str) {
        let mut inner = self.lock();
        let inner = &mut *inner;
        status_event(inner, session, "session.status_terminated", None);
        let found = inner.sessions.get_mut(session).unwrap();
        found.status = "terminated".into();
        found.terminated = true;
    }

    /// The session's vault credential disappears at the vendor.
    pub fn forget_credentials(&self, session: &str) {
        let mut inner = self.lock();
        let vault = inner.sessions[session].vault.clone();
        inner
            .credentials
            .retain(|_, (v, _)| Some(&*v) != vault.as_ref());
    }

    /// The `GET /v1/sessions/{id}/events` requests, with their query.
    pub fn history_reads(&self, session: &str) -> Vec<Value> {
        self.requests("GET", &format!("/v1/sessions/{session}/events"))
            .into_iter()
            .filter(|r| r.path.ends_with("/events"))
            .map(|r| r.body)
            .collect()
    }

    /// The `user.message` records the session holds.
    pub fn stored_messages(&self, session: &str) -> usize {
        self.lock().sessions[session]
            .events
            .iter()
            .filter(|e| e["type"] == "user.message")
            .count()
    }

    pub fn token_of_session(&self, session: &str) -> Option<String> {
        let inner = self.lock();
        let vault = inner.sessions[session].vault.clone()?;
        inner
            .credentials
            .values()
            .find(|(v, _)| *v == vault)
            .map(|(_, token)| token.clone())
    }
}

/// Wait up to 30 s for `ready`, failing the test when it never is.
pub fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if ready() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(ready(), "never: {what}");
}

/// A store with one agent on the Claude Managed Agents harness.
pub fn store_with_agent(state: &str) -> (tempfile::TempDir, Arc<Store>, anyagents_core::Agent) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("cloudy", "claude-cloud"))
        .unwrap();
    let agent = if state == "running" {
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        store
            .set_agent_running(&agent.id, &work.display().to_string())
            .unwrap();
        store.agent(&agent.id).unwrap()
    } else {
        agent
    };
    (dir, store, agent)
}

pub fn config(
    vendor: &Vendor,
    store: &Arc<Store>,
    agent: &anyagents_core::Agent,
    budget_cents: i64,
) -> crate::Config {
    crate::Config {
        store: Arc::clone(store),
        agent: agent.clone(),
        adapter: Arc::new(
            crate::claude_managed::ClaudeManaged::new(KEY, Some(&vendor.base)).unwrap(),
        ),
        system: Some("Answer briefly.".into()),
        mcp_url: Some(MCP_URL.into()),
        token_ttl_ms: 3_600_000,
        budget_cents: Some(budget_cents),
        host_tools: None,
    }
}

pub fn transcript_lines(store: &Store, agent: &AgentId) -> Vec<Value> {
    let dir = store
        .data_dir()
        .join("agents")
        .join(agent.as_str())
        .join("transcripts");
    let mut lines = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let text = std::fs::read_to_string(entry.path()).unwrap();
        lines.extend(
            text.lines()
                .map(|l| serde_json::from_str::<Value>(l).unwrap()),
        );
    }
    lines
}

// --- the scripted API ---

type Shared = Arc<Mutex<Inner>>;

fn id(inner: &mut Inner, prefix: &str) -> String {
    inner.next += 1;
    format!("{prefix}_{:04}", inner.next)
}

fn log(inner: &mut Inner, method: &str, path: String, headers: &HeaderMap, body: Value) {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    inner.requests.push(Request {
        method: method.into(),
        path,
        body,
        api_key: header("x-api-key"),
        beta: header("anthropic-beta"),
        version: header("anthropic-version"),
    });
}

/// The refusal for a request without the key or the beta header, if any.
fn refused(headers: &HeaderMap) -> Option<Response> {
    let key = headers.get("x-api-key").and_then(|v| v.to_str().ok());
    let beta = headers.get("anthropic-beta").and_then(|v| v.to_str().ok());
    if key != Some(KEY) {
        return Some(error(StatusCode::UNAUTHORIZED, "invalid x-api-key"));
    }
    if beta != Some("managed-agents-2026-04-01") {
        return Some(error(StatusCode::NOT_FOUND, "not found"));
    }
    None
}

fn error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({"type": "error", "error": {"type": "invalid_request_error", "message": message}})),
    )
        .into_response()
}

fn now() -> String {
    "2026-11-14T10:00:00.000Z".into()
}

fn emit(inner: &mut Inner, session: &str, event: Value, persist: bool) {
    let s = inner.sessions.get_mut(session).unwrap();
    if persist {
        s.events.push(event.clone());
    }
    s.subscribers.retain(|tx| tx.send(event.clone()).is_ok());
}

fn status_event(inner: &mut Inner, session: &str, kind: &str, stop: Option<&str>) {
    let mut event = json!({"type": kind, "id": id(inner, "sevt"), "processed_at": now()});
    if let Some(stop) = stop {
        event["stop_reason"] = json!({"type": stop});
    }
    emit(inner, session, event, true);
}

/// The model takes one queued message: it is marked processed, one model
/// request runs, and the agent answers.
fn process(inner: &mut Inner, session: &str, message: &str) {
    let updated = {
        let s = inner.sessions.get_mut(session).unwrap();
        let record = s.events.iter_mut().find(|e| e["id"] == message).unwrap();
        record["processed_at"] = json!(now());
        record.clone()
    };
    // The stream shows the input again, processed; the history keeps one.
    emit(inner, session, updated.clone(), false);
    let cost = inner.cost_per_request_cents;
    inner.sessions.get_mut(session).unwrap().cost_cents += cost;
    let span = json!({
        "type": "span.model_request_end", "id": id(inner, "sevt"), "processed_at": now(),
        "is_error": false,
        "model_usage": {"input_tokens": 100, "output_tokens": 10,
                        "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
    });
    emit(inner, session, span, true);
    let text = updated["content"][0]["text"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let reply = json!({
        "type": "agent.message", "id": id(inner, "sevt"), "processed_at": now(),
        "content": [{"type": "text", "text": format!("done: {}", text.lines().last().unwrap_or(""))}],
    });
    emit(inner, session, reply, true);
}

fn end_turn(inner: &mut Inner, session: &str) {
    let (cost, budget) = {
        let s = &inner.sessions[session];
        (s.cost_cents, s.budget_cents)
    };
    if budget.is_some_and(|cap| cost >= cap) {
        let usage = json!({
            "type": "session.usage", "id": id(inner, "sevt"), "processed_at": now(),
            "list_cost": {"amount": cost.to_string(), "currency": "USD"},
        });
        emit(inner, session, usage, true);
        status_event(
            inner,
            session,
            "session.status_idle",
            Some("budget_reached"),
        );
        let s = inner.sessions.get_mut(session).unwrap();
        s.paused_at_budget = true;
        s.status = "idle".into();
        return;
    }
    status_event(inner, session, "session.status_idle", Some("end_turn"));
    inner.sessions.get_mut(session).unwrap().status = "idle".into();
}

fn start_turn(inner: &mut Inner, session: &str) {
    inner.sessions.get_mut(session).unwrap().status = "running".into();
    status_event(inner, session, "session.status_running", None);
    let queue = std::mem::take(&mut inner.sessions.get_mut(session).unwrap().queue);
    for message in queue {
        process(inner, session, &message);
    }
    if !inner.hold {
        end_turn(inner, session);
    }
}

async fn environment(
    State(s): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        "/v1/environments".into(),
        &headers,
        body,
    );
    let id = id(&mut inner, "env");
    Json(json!({"id": id, "type": "environment"})).into_response()
}

async fn create_agent(
    State(s): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(&mut inner, "POST", "/v1/agents".into(), &headers, body);
    let id = id(&mut inner, "agent");
    Json(json!({"id": id, "version": 1, "type": "agent"})).into_response()
}

async fn update_agent(
    State(s): State<Shared>,
    Path(agent): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        format!("/v1/agents/{agent}"),
        &headers,
        body,
    );
    Json(json!({"id": agent, "version": 2})).into_response()
}

async fn vault(State(s): State<Shared>, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(&mut inner, "POST", "/v1/vaults".into(), &headers, body);
    let id = id(&mut inner, "vlt");
    Json(json!({"id": id})).into_response()
}

async fn credential(
    State(s): State<Shared>,
    Path(vault): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        format!("/v1/vaults/{vault}/credentials"),
        &headers,
        body.clone(),
    );
    let id = id(&mut inner, "vcrd");
    let token = body["auth"]["token"].as_str().unwrap_or("").to_string();
    inner.credentials.insert(id.clone(), (vault, token));
    // Secrets are write-only: never in the answer.
    Json(json!({"id": id, "auth": {"type": "static_bearer", "mcp_server_url": body["auth"]["mcp_server_url"]}}))
        .into_response()
}

async fn update_credential(
    State(s): State<Shared>,
    Path((vault, credential)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        format!("/v1/vaults/{vault}/credentials/{credential}"),
        &headers,
        body.clone(),
    );
    match inner.credentials.get_mut(&credential) {
        Some(entry) if entry.0 == vault => {
            entry.1 = body["auth"]["token"].as_str().unwrap_or("").to_string();
            Json(json!({"id": credential})).into_response()
        }
        _ => error(StatusCode::NOT_FOUND, "no such credential"),
    }
}

async fn archive_vault(
    State(s): State<Shared>,
    Path(vault): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        format!("/v1/vaults/{vault}/archive"),
        &headers,
        Value::Null,
    );
    inner.archived_vaults.push(vault.clone());
    Json(json!({"id": vault, "archived_at": now()})).into_response()
}

async fn create_session(
    State(s): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        "/v1/sessions".into(),
        &headers,
        body.clone(),
    );
    if inner.fail_session_creates > 0 {
        inner.fail_session_creates -= 1;
        return error(StatusCode::INTERNAL_SERVER_ERROR, "overloaded");
    }
    let budget_cents = body
        .pointer("/budget/max_list_cost/amount")
        .and_then(Value::as_str)
        .map(|a| a.parse::<i64>().unwrap());
    let id = id(&mut inner, "sesn");
    inner.sessions.insert(
        id.clone(),
        Session {
            status: "idle".into(),
            budget_cents,
            vault: body["vault_ids"][0].as_str().map(str::to_string),
            ..Session::default()
        },
    );
    Json(json!({"id": id, "type": "session", "status": "idle"})).into_response()
}

fn session_json(id: &str, s: &Session) -> Value {
    let mut out = json!({
        "id": id, "type": "session", "status": s.status,
        "usage": {"list_cost": {"amount": s.cost_cents.to_string(), "currency": "USD"}},
    });
    if let Some(cap) = s.budget_cents {
        out["budget"] = json!({"type": "limit", "max_list_cost": {"amount": cap.to_string(), "currency": "USD"}});
    }
    out
}

async fn get_session_delayed(s: &Shared) {
    let delay = s.lock().unwrap().snapshot_delay_ms;
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
}

async fn get_session(
    State(s): State<Shared>,
    Path(session): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    get_session_delayed(&s).await;
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "GET",
        format!("/v1/sessions/{session}"),
        &headers,
        Value::Null,
    );
    match inner.sessions.get(&session) {
        Some(found) => Json(session_json(&session, found)).into_response(),
        None => error(StatusCode::NOT_FOUND, "no such session"),
    }
}

async fn update_session(
    State(s): State<Shared>,
    Path(session): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        format!("/v1/sessions/{session}"),
        &headers,
        body.clone(),
    );
    let Some(found) = inner.sessions.get(&session) else {
        return error(StatusCode::NOT_FOUND, "no such session");
    };
    let cost = found.cost_cents;
    let Some(cap) = body
        .pointer("/budget/max_list_cost/amount")
        .and_then(Value::as_str)
        .map(|a| a.parse::<i64>().unwrap())
    else {
        return error(StatusCode::BAD_REQUEST, "only budget changes are scripted");
    };
    if cap <= cost {
        return error(
            StatusCode::BAD_REQUEST,
            "budget.max_list_cost must be greater than the session's consumed list cost",
        );
    }
    let resume = {
        let found = inner.sessions.get_mut(&session).unwrap();
        found.budget_cents = Some(cap);
        std::mem::take(&mut found.paused_at_budget)
    };
    if resume {
        // An accepted update resumes the paused work.
        inner.sessions.get_mut(&session).unwrap().status = "running".into();
        status_event(&mut inner, &session, "session.status_running", None);
        if !inner.hold {
            end_turn(&mut inner, &session);
        }
    }
    let answer = session_json(&session, &inner.sessions[&session]);
    Json(answer).into_response()
}

async fn archive_session(
    State(s): State<Shared>,
    Path(session): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let delay = s.lock().unwrap().archive_delay_ms;
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        format!("/v1/sessions/{session}/archive"),
        &headers,
        Value::Null,
    );
    match inner.sessions.get_mut(&session) {
        Some(found) if found.status == "running" => error(
            StatusCode::BAD_REQUEST,
            "cannot archive a session while it is running",
        ),
        Some(found) => {
            found.archived = true;
            found.subscribers.clear();
            Json(json!({"id": session, "archived_at": now()})).into_response()
        }
        None => error(StatusCode::NOT_FOUND, "no such session"),
    }
}

async fn send_events(
    State(s): State<Shared>,
    Path(session): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "POST",
        format!("/v1/sessions/{session}/events"),
        &headers,
        body.clone(),
    );
    let Some(found) = inner.sessions.get(&session) else {
        return error(StatusCode::NOT_FOUND, "no such session");
    };
    if found.archived {
        return error(StatusCode::BAD_REQUEST, "the session is archived");
    }
    if found.terminated {
        return error(StatusCode::BAD_REQUEST, "the session is terminated");
    }
    let paused = found.paused_at_budget;
    if let Some(code) = inner.fail_before_apply
        && body["events"][0]["type"] == "user.message"
    {
        let mut refused = error(StatusCode::from_u16(code).unwrap(), "overloaded");
        if let Some(secs) = inner.retry_after_secs {
            refused.headers_mut().insert(
                "retry-after",
                HeaderValue::from_str(&secs.to_string()).unwrap(),
            );
        }
        return refused;
    }
    let mut answered = Vec::new();
    for event in body["events"].as_array().cloned().unwrap_or_default() {
        match event["type"].as_str() {
            Some("user.message") => {
                if paused {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "the session is paused at its budget; only settle events are accepted \
                         (user.tool_confirmation, user.tool_result, user.custom_tool_result, user.interrupt)",
                    );
                }
                let id = id(&mut inner, "sevt");
                let record = json!({
                    "type": "user.message", "id": id, "processed_at": null,
                    "content": event["content"],
                });
                emit(&mut inner, &session, record.clone(), true);
                answered.push(record);
                let running = inner.sessions[&session].status == "running";
                inner.sessions.get_mut(&session).unwrap().queue.push(id);
                if !running {
                    start_turn(&mut inner, &session);
                }
            }
            Some("user.interrupt") => {
                if paused {
                    // Accepted and ignored at the cap.
                    continue;
                }
                if inner.sessions[&session].status == "running" {
                    inner.sessions.get_mut(&session).unwrap().queue.clear();
                    status_event(
                        &mut inner,
                        &session,
                        "session.status_idle",
                        Some("end_turn"),
                    );
                    inner.sessions.get_mut(&session).unwrap().status = "idle".into();
                }
            }
            other => {
                return error(
                    StatusCode::BAD_REQUEST,
                    &format!("unscripted event {other:?}"),
                );
            }
        }
    }
    if let Some(code) = inner.fail_after_apply.take() {
        return error(StatusCode::from_u16(code).unwrap(), "gateway timeout");
    }
    if std::mem::take(&mut inner.unreadable_answer) {
        return (StatusCode::OK, "<html>upstream</html>").into_response();
    }
    Json(json!({"data": answered})).into_response()
}

async fn history(
    State(s): State<Shared>,
    Path(session): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "GET",
        format!("/v1/sessions/{session}/events"),
        &headers,
        json!(query),
    );
    let page = inner.page;
    let Some(found) = inner.sessions.get(&session) else {
        return error(StatusCode::NOT_FOUND, "no such session");
    };
    let limit: usize = query
        .get("limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(1000)
        .min(page);
    let offset: usize = query.get("page").and_then(|p| p.parse().ok()).unwrap_or(0);
    let ordered: Vec<Value> = if query.get("order").map(String::as_str) == Some("desc") {
        found.events.iter().rev().cloned().collect()
    } else {
        found.events.clone()
    };
    let end = (offset + limit).min(ordered.len());
    let data = ordered[offset.min(end)..end].to_vec();
    let next = (end < ordered.len()).then(|| end.to_string());
    Json(json!({"data": data, "next_page": next})).into_response()
}

async fn stream(
    State(s): State<Shared>,
    Path(session): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(r) = refused(&headers) {
        return r;
    }
    let mut inner = s.lock().unwrap();
    log(
        &mut inner,
        "GET",
        format!("/v1/sessions/{session}/events/stream"),
        &headers,
        Value::Null,
    );
    let Some(found) = inner.sessions.get_mut(&session) else {
        return error(StatusCode::NOT_FOUND, "no such session");
    };
    let (tx, rx) = mpsc::unbounded_channel::<Value>();
    found.subscribers.push(tx);
    let body = futures::stream::unfold((rx, true), |(mut rx, first)| async move {
        if first {
            return Some((
                Ok::<_, std::io::Error>(": ping\n\n".to_string()),
                (rx, false),
            ));
        }
        let event = rx.recv().await?;
        let frame = format!(
            "event: {}\ndata: {}\n\n",
            event["type"].as_str().unwrap_or("message"),
            event
        );
        Some((Ok(frame), (rx, false)))
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(body))
        .unwrap()
}
