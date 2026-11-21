use super::super::messaging::{SendPlan, send_key, send_sync};
use super::super::webhooks_mcp::mcp_post_route;
use super::super::*;
use crate::api::{SendRequest, SendResponse};
use anyagents_core::Agent;
use axum::http::StatusCode;
use std::sync::MutexGuard;

/// These tests start a server in this process, so the environment they set
/// is read under this lock and put back before the next one runs.
static ENV: Mutex<()> = Mutex::new(());

/// One lock for every test that sets this process's environment, so a
/// server started by one never reads another's settings.
pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
    ENV.lock().unwrap_or_else(|err| err.into_inner())
}

/// Process-local environment locks do not isolate forked children or
/// process-global PATH. Keep one-tick lifecycle assertions in one worker.
pub(super) fn single_test_worker(module: &str, name: &str) -> bool {
    // libtest lists paths relative to the crate, unlike module_path!().
    let (_, module) = module.split_once("::").unwrap();
    let name = format!("{module}::{name}");
    if std::env::var("ANYAGENTS_SINGLE_TEST_WORKER").as_deref() == Ok(name.as_str()) {
        return false;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &name, "--nocapture"])
        .env("ANYAGENTS_SINGLE_TEST_WORKER", &name)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("1 passed; 0 failed;"),
        "{name} worker failed or did not execute: {stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    true
}

pub(super) struct SingleUidEnvironment(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl SingleUidEnvironment {
    /// Call only while env_lock is held, before starting the fixture server.
    pub(super) fn new() -> Self {
        let keys = [
            "ANYAGENTS_PRINCIPALS",
            "ANYAGENTS_PRINCIPAL_REGISTRY",
            "ANYAGENTS_AGENTS",
            "ANYAGENTS_AGENT_UID",
            "ANYAGENTS_AGENT_GID",
            "ANYAGENTS_SOCKET_GID",
        ];
        let saved = keys
            .iter()
            .map(|key| (*key, std::env::var_os(key)))
            .collect();
        for key in keys {
            // SAFETY: these server fixtures hold env_lock through shutdown.
            unsafe { std::env::remove_var(key) };
        }
        assert!(!crate::sandbox::seats_configured());
        assert!(crate::sandbox::launch_boundary(&[]));
        Self(saved)
    }
}

impl Drop for SingleUidEnvironment {
    fn drop(&mut self) {
        for (key, value) in &self.0 {
            // SAFETY: the fixture server has stopped and env_lock is held.
            unsafe {
                if let Some(value) = value {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }
}

pub(super) fn mark_explicitly_archived(store: &Store, data_dir: &Path, agent: &Agent) {
    let Ok(_lifecycle) = store.lock_agent_lifecycle(&agent.id) else {
        return;
    };
    let _ = store.set_agent_state(&agent.id, "archived");
    mark_explicitly_archived_locked(store, data_dir, agent).unwrap();
}

pub(super) fn authored_send(
    store: &Store,
    actor: &Actor,
    to: &str,
    text: &str,
    key: &str,
) -> SendResponse {
    try_authored_send(store, actor, to, text, key).unwrap_or_else(|e| panic!("{}", e.message))
}

pub(super) fn try_authored_send(
    store: &Store,
    actor: &Actor,
    to: &str,
    text: &str,
    key: &str,
) -> Result<SendResponse, ApiError> {
    let body = SendRequest {
        sender: Some(actor.as_address()),
        to: vec![to.to_string()],
        text: text.to_string(),
        idempotency_key: Some(key.to_string()),
        ..Default::default()
    };
    let key = send_key(&body);
    let plan = SendPlan::parse(store, actor, &body)?;
    send_sync(store, actor, body, plan, key).map(|response| response.0)
}

pub(super) fn archetype_validation_state(data: &Path) -> AppState {
    state_for_store(Store::open(data.join("state.db")).unwrap())
}

pub(super) fn state_for_store(store: Store) -> AppState {
    let data = store.data_dir().to_path_buf();
    let store = Arc::new(store);
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    AppState {
        store,
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        auth: auth::Mode::Trusted { data_dir: data },
    }
}

/// One MCP POST with a bearer token.
pub(super) async fn post_mcp_as(
    state: &AppState,
    token: &str,
    _profile: Option<&str>,
    body: serde_json::Value,
) -> Result<(StatusCode, serde_json::Value), ApiError> {
    use http_body_util::BodyExt as _;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    let raw = axum::body::Bytes::from(serde_json::to_vec(&body).unwrap());
    let resp = mcp_post_route(State(state.clone()), headers, raw).await?;
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    Ok((status, json))
}

pub(super) fn listed_tools(list: &serde_json::Value) -> Vec<String> {
    list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect()
}

pub(super) fn tools_list() -> serde_json::Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {} })
}

pub(super) fn tool_call(name: &str, arguments: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": { "name": name, "arguments": arguments }
    })
}

/// One HTTP/1.1 exchange with a TCP listener: the status and the JSON body
/// (`Null` when there is none).
pub(super) fn raw_http(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, serde_json::Value) {
    use std::io::{Read as _, Write as _};
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .unwrap();
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: public.example\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !body.is_empty() {
        request.push_str("Content-Type: application/json\r\n");
    }
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();
    let (head, rest) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (
        status,
        serde_json::from_str(rest).unwrap_or(serde_json::Value::Null),
    )
}

pub(super) async fn http(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    capability: Option<&str>,
    operator: Option<&str>,
) -> (hyper::StatusCode, serde_json::Value) {
    let body = body.map(|body| serde_json::to_vec(&body).unwrap());
    let (status, bytes) = crate::mcp::request(socket, method, path, body, capability, operator)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, value)
}
