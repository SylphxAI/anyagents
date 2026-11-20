//! Exercise the shipped guest stdio proxy and two real coordinator processes
//! behind one stable loopback endpoint. Inject failures by request count, not
//! by racing a timer against coordinator startup.

mod support;

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use anyagents_core::{NewAgent, Store};
use anyagents_harness::ExecutionRuntime;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde_json::{Value, json};

const MCP_PATH: &str = "/v1/webhooks/mcp";
const STALE_SESSION: &str = "retired-transport-session";

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn http() -> ureq::Agent {
    ureq::Agent::config_builder()
        .proxy(None)
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(2)))
        .build()
        .new_agent()
}

fn coordinator(data: &Path) -> (Process, SocketAddr) {
    let addr_file = data.join("webhook.addr");
    if addr_file.exists() {
        std::fs::remove_file(&addr_file).unwrap();
    }
    let mut server = Process(
        support::contract_server()
            .env("ANYAGENTS_SERVERLESS", "true")
            .env("ANYAGENTS_WEBHOOK_LISTEN", "127.0.0.1:0")
            .env_remove("ANYAGENTS_DATABASE_URL")
            .env_remove("ANYAGENTS_DATABASE_URL_FILE")
            .arg("--data")
            .arg(data)
            .arg("serve")
            .stdin(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let http = http();
    loop {
        if let Ok(raw) = std::fs::read_to_string(&addr_file)
            && let Ok(addr) = raw.trim().parse::<SocketAddr>()
            && http
                .get(format!("http://{addr}/readyz"))
                .call()
                .is_ok_and(|response| response.status() == 200)
        {
            return (server, addr);
        }
        assert_eq!(server.0.try_wait().unwrap(), None, "coordinator exited");
        assert!(Instant::now() < deadline, "coordinator did not listen");
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct Edge {
    upstream: SocketAddr,
    failures: VecDeque<StatusCode>,
    seen_sessions: Vec<Option<String>>,
    advertise_session: bool,
}

async fn edge(State(state): State<Arc<Mutex<Edge>>>, headers: HeaderMap, body: Bytes) -> Response {
    let (upstream, advertise_session) = {
        let mut edge = state.lock().unwrap();
        edge.seen_sessions.push(
            headers
                .get("mcp-session-id")
                .map(|id| id.to_str().unwrap().to_owned()),
        );
        if let Some(status) = edge.failures.pop_front() {
            return status.into_response();
        }
        (edge.upstream, std::mem::take(&mut edge.advertise_session))
    };
    tokio::task::spawn_blocking(move || {
        let mut post = http().post(format!("http://{upstream}{MCP_PATH}"));
        for name in [
            "authorization",
            "accept",
            "content-type",
            "mcp-session-id",
            "x-anyagents-payload-encoding",
        ] {
            if let Some(value) = headers.get(name) {
                post = post.header(name, value);
            }
        }
        let response = post.send(&body[..]).unwrap();
        let (mut parts, mut body) = response.into_parts();
        if advertise_session {
            // Act as an older transport that advertised a session. The
            // replacement coordinator never issues one and ignores this header.
            parts
                .headers
                .insert("mcp-session-id", STALE_SESSION.parse().unwrap());
        }
        Response::from_parts(parts, Body::from(body.read_to_vec().unwrap()))
    })
    .await
    .unwrap()
}

struct Guest {
    stdin: ChildStdin,
    replies: mpsc::Receiver<Value>,
    _process: Process,
}

impl Guest {
    fn spawn(dir: &Path, addr: SocketAddr, token_file: &Path) -> Self {
        // The shipped multi-call executable selects the real guest by argv[0].
        let binary = dir.join("anyagents-guest");
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_anyagents"), &binary).unwrap();
        let mut child = Process(
            Command::new(&binary)
                .args([
                    "mcp-proxy",
                    "--url",
                    &format!("http://{addr}{MCP_PATH}"),
                    "--token-file",
                ])
                .arg(token_file)
                .env_remove("ANYAGENTS_MCP_TOKEN")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stdin = child.0.stdin.take().unwrap();
        let stdout = child.0.stdout.take().unwrap();
        let (tx, replies) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let response = serde_json::from_str(&line.unwrap()).unwrap();
                if tx.send(response).is_err() {
                    break;
                }
            }
        });
        Self {
            stdin,
            replies,
            _process: child,
        }
    }

    fn call(&mut self, id: u32, method: &str, params: Value) -> Value {
        writeln!(
            self.stdin,
            "{}",
            json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
        )
        .unwrap();
        let result = self.replies.recv_timeout(Duration::from_secs(20)).unwrap();
        assert_eq!(result["id"], id);
        result
    }

    fn initialize(&mut self, id: u32) -> Value {
        self.call(id, "initialize", json!({"protocolVersion":"2025-03-26", "capabilities":{}, "clientInfo":{"name":"fixture", "version":"1"}}))
    }

    fn groups(&mut self, id: u32) -> Value {
        self.call(
            id,
            "tools/call",
            json!({"name":"groups", "arguments":{"action":"list"}}),
        )
    }
}

fn assert_groups(result: Value) {
    assert_eq!(result["result"]["isError"], false, "{result}");
    let content: Value =
        serde_json::from_str(result["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert!(content["groups"].is_array(), "{content}");
}

#[test]
fn mcp_guest_recovers_across_coordinator_rollover() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    assert!(!store.is_postgres(), "expected isolated SQLite fixture");
    let agent = store
        .create_agent(
            NewAgent::new("rollover-fixture", "agy")
                .with_runtime(ExecutionRuntime::Sandbox { provider: None }),
        )
        .unwrap();
    store
        .set_agent_running(&agent.id, data.to_str().unwrap())
        .unwrap();
    let grant = store.begin_remote_session(&agent.id, 120_000).unwrap();
    let token_file = data.join("remote-token");
    std::fs::write(&token_file, &grant.token).unwrap();

    let (first, addr) = coordinator(data);
    let state = Arc::new(Mutex::new(Edge {
        upstream: addr,
        failures: [StatusCode::NOT_FOUND].into(),
        seen_sessions: Vec::new(),
        advertise_session: true,
    }));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let addr = runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = axum::Router::new()
            .route(MCP_PATH, post(edge))
            .with_state(Arc::clone(&state));
        tokio::spawn(async { axum::serve(listener, router).await.unwrap() });
        addr
    });
    let mut guest = Guest::spawn(data, addr, &token_file);
    let hello = guest.initialize(1);
    assert_eq!(
        hello["result"]["serverInfo"]["name"], "anyagents",
        "first initialize: {hello}"
    );
    assert_groups(guest.groups(2));
    assert_eq!(
        state.lock().unwrap().seen_sessions,
        [None, None, Some(STALE_SESSION.to_owned())]
    );

    // Kill and reap the process, then open the same durable store in a new one.
    // Keep the same guest, pinned URL and token file throughout.
    let old_pid = first.0.id();
    drop(first);
    let (second, replacement) = coordinator(data);
    assert_ne!(second.0.id(), old_pid);
    state.lock().unwrap().upstream = replacement;

    // A tool call with a stale HTTP session header works before any re-init.
    assert_groups(guest.groups(3));
    assert_eq!(
        state
            .lock()
            .unwrap()
            .seen_sessions
            .last()
            .unwrap()
            .as_deref(),
        Some(STALE_SESSION)
    );

    {
        let mut edge = state.lock().unwrap();
        edge.seen_sessions.clear();
        edge.failures = [
            StatusCode::NOT_FOUND,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ]
        .into();
    }
    let hello = guest.initialize(4);
    assert_eq!(
        hello["result"]["serverInfo"]["name"], "anyagents",
        "replacement initialize: {hello}"
    );
    assert_groups(guest.groups(5));
    assert_eq!(state.lock().unwrap().seen_sessions, [const { None }; 5]);

    // An ambiguous tool failure must not silently replay an RPC. The same
    // connection can issue a new call after reporting the error.
    state
        .lock()
        .unwrap()
        .failures
        .push_back(StatusCode::BAD_GATEWAY);
    let failed = guest.groups(6);
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("HTTP 502"),
        "{failed}"
    );
    assert_groups(guest.groups(7));
    assert_eq!(state.lock().unwrap().seen_sessions.len(), 7);

    let assert_denied = |mut guest_call: Value| {
        let message = guest_call["error"]["message"].take();
        assert!(
            message
                .as_str()
                .unwrap()
                .starts_with("MCP server returned HTTP 401:"),
            "{message}"
        );
        assert!(guest_call.get("result").is_none());
    };
    let forged = format!(
        "{}{}",
        anyagents_core::REMOTE_SESSION_TOKEN_PREFIX,
        "0".repeat(64)
    );
    for (id, token) in [(8, forged.as_str()), (9, "invalid"), (10, "")] {
        std::fs::write(&token_file, token).unwrap();
        assert_denied(guest.initialize(id));
    }
    let revoked = store
        .ensure_remote_session_token(&agent.id, 120_000)
        .unwrap();
    assert!(store.revoke_remote_session_token(&revoked.token).unwrap());
    std::fs::write(&token_file, &revoked.token).unwrap();
    assert_denied(guest.initialize(11));
    std::fs::write(&token_file, &grant.token).unwrap();
    assert_groups(guest.groups(12)); // The original grant is still valid.
    store.set_agent_state(&agent.id, "archived").unwrap();
    assert_denied(guest.initialize(13));
    assert_denied(guest.groups(14));
    assert_eq!(
        state.lock().unwrap().seen_sessions.len(),
        14,
        "no retries on authentication failures"
    );
}
