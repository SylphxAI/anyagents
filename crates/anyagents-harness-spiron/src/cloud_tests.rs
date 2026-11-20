use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryLevel, Harness, OutboundMessage, Proof, RemoteDeliveryAck,
    RemoteDeliveryInput, RemoteEvent, RemoteSessionConfig, RemoteSessionError,
    RemoteSessionProvider, TurnUsage,
};
use serde_json::{Value, json};

use crate::cloud::{
    REMOTE_MCP_URL_FILE, REMOTE_SESSION_TOKEN_FILE, SpironCloudConfig, SpironCloudProvider,
    is_remote_session_token, is_sylphx_gateway_key, validate_uuid,
};

const AGENT_ID: &str = "00000000-0000-4000-8000-000000000001";
const RECEIPT_1: &str = "00000000-0000-4000-8000-000000000011";
const RECEIPT_2: &str = "00000000-0000-4000-8000-000000000012";
const RECEIPT_3: &str = "00000000-0000-4000-8000-000000000013";
const REQ_1: &str = "00000000-0000-4000-8000-000000000101";
const REQ_2: &str = "00000000-0000-4000-8000-000000000102";
const REQ_3: &str = "00000000-0000-4000-8000-000000000103";
const TEST_TOKEN: &str = "spiron-cloud-test-token";

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    uri: String,
    headers: HashMap<String, String>,
    body: Value,
}

#[derive(Clone, Debug)]
struct MockResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Value,
}

impl MockResponse {
    fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body,
        }
    }

    fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

type HandlerFn = dyn Fn(&RecordedRequest, usize) -> MockResponse + Send + Sync + 'static;

struct MockSpironServer {
    addr: SocketAddr,
    base_url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl MockSpironServer {
    fn start<F>(handler: F) -> Self
    where
        F: Fn(&RecordedRequest, usize) -> MockResponse + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock spiron server");
        let addr = listener.local_addr().expect("mock local addr");
        let base_url = format!("http://{addr}");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let req_log = Arc::clone(&requests);
        let stop_flag = Arc::clone(&stop);
        let handler: Arc<HandlerFn> = Arc::new(handler);

        let join = thread::spawn(move || {
            for stream in listener.incoming() {
                if stop_flag.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(mut stream) = stream else {
                    continue;
                };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
                let Some(req) = read_http_request(&mut stream) else {
                    continue;
                };
                let idx = {
                    let mut guard = req_log.lock().unwrap();
                    let i = guard.len();
                    guard.push(req.clone());
                    i
                };
                let resp = handler(&req, idx);
                write_http_response(&mut stream, &resp);
            }
        });

        Self {
            addr,
            base_url,
            requests,
            stop,
            join: Some(join),
        }
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for MockSpironServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> Option<RecordedRequest> {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        raw.extend_from_slice(&chunk[..n]);
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
    };
    let head = String::from_utf8_lossy(&raw[..header_end]);
    let mut lines = head.split("\r\n");
    let req_line = lines.next()?;
    let mut parts = req_line.split_whitespace();
    let method = parts.next()?.to_string();
    let uri = parts.next()?.to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let content_len = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let body_start = header_end + 4;
    while raw.len() < body_start + content_len {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&chunk[..n]);
    }
    let body_end = (body_start + content_len).min(raw.len());
    let body_bytes = &raw[body_start..body_end];
    let body = if body_bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body_bytes).unwrap_or(Value::Null)
    };
    Some(RecordedRequest {
        method,
        uri,
        headers,
        body,
    })
}

fn write_http_response(stream: &mut TcpStream, resp: &MockResponse) {
    let body_str = serde_json::to_string(&resp.body).unwrap();
    let reason = match resp.status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        resp.status,
        body_str.len()
    );
    for (k, v) in &resp.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body_str.as_bytes());
    let _ = stream.flush();
}

fn strict_openapi() -> Value {
    json!({
        "openapi": "3.1.0",
        "x-spiron-input-delivery": {
            "protocol": "session-entry-v1",
            "level": "next-step",
            "idempotent": true,
            "proof": "input_delivery"
        }
    })
}

fn input_proof(entry_id: &str, turn_entry_id: &str) -> Value {
    json!({
        "session_id": AGENT_ID,
        "entry_id": entry_id,
        "turn_entry_id": turn_entry_id,
        "recorded_at": "2026-11-20T04:00:00Z"
    })
}

fn sample_outbound(id: &str, seq: i64, text: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.to_string(),
        seq,
        text: text.to_string(),
        sender: Some("operator".to_string()),
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    }
}

fn sample_session_config(resume: Option<&str>) -> RemoteSessionConfig {
    RemoteSessionConfig {
        agent_id: "agt_local_1".to_string(),
        agent_name: "cloud-worker".to_string(),
        model: Some("openai/gpt-5.4".to_string()),
        instructions: Some("Use coordination MCP only.".to_string()),
        resume_session_id: resume.map(str::to_string),
        ..Default::default()
    }
}

fn test_config(dir: &std::path::Path, base_url: &str) -> SpironCloudConfig {
    SpironCloudConfig::new(dir)
        .with_base_url(base_url)
        .with_api_token(TEST_TOKEN)
        .with_request_timeout(Duration::from_secs(5))
}

fn remote_token(fill: &str) -> String {
    format!("aars_{}", fill.repeat(64))
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("condition not satisfied within {timeout:?}");
}

#[test]
fn openapi_session_entry_v1_capability_negotiation_and_turn_end_downgrade() {
    let strict_server =
        MockSpironServer::start(|req, _| match (req.method.as_str(), req.uri.as_str()) {
            ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
            ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
            _ => MockResponse::json(500, json!({"unexpected": req.uri})),
        });
    let dir1 = tempfile::tempdir().unwrap();
    let mut provider1 = SpironCloudProvider::new(test_config(dir1.path(), &strict_server.base_url));
    let handle1 = provider1
        .create(&sample_session_config(Some(AGENT_ID)))
        .expect("create strict session");
    assert_eq!(handle1.session_id, AGENT_ID);
    assert_eq!(handle1.remote_agent_id.as_deref(), Some(AGENT_ID));
    assert_eq!(handle1.capabilities.provider, "spiron-cloud");
    assert_eq!(handle1.capabilities.delivery.level, DeliveryLevel::NextStep);
    assert!(handle1.capabilities.delivery.idempotent);
    assert!(handle1.capabilities.supports_steer);
    assert!(handle1.capabilities.supports_interrupt);
    assert!(handle1.capabilities.supports_event_resume);
    assert!(!handle1.capabilities.supports_host_tools);
    assert!(handle1.capabilities.supports_cost_reporting);
    assert!(handle1.capabilities.supports_budget_cap);
    provider1.stop().unwrap();

    // Missing or non-idempotent x-spiron-input-delivery must downgrade honestly to TurnEnd.
    let legacy_server =
        MockSpironServer::start(|req, _| match (req.method.as_str(), req.uri.as_str()) {
            ("GET", "/v1/openapi.json") => MockResponse::json(
                200,
                json!({
                    "x-spiron-input-delivery": {
                        "protocol": "session-entry-v1",
                        "level": "next-step",
                        "idempotent": false,
                        "proof": "input_delivery"
                    }
                }),
            ),
            ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
            _ => MockResponse::json(500, json!({})),
        });
    let dir2 = tempfile::tempdir().unwrap();
    let mut provider2 = SpironCloudProvider::new(test_config(dir2.path(), &legacy_server.base_url));
    let handle2 = provider2
        .create(&sample_session_config(Some(AGENT_ID)))
        .expect("create legacy session");
    assert_eq!(handle2.capabilities.delivery.level, DeliveryLevel::TurnEnd);
    assert!(!handle2.capabilities.delivery.idempotent);
    assert!(!handle2.capabilities.supports_steer);
    provider2.stop().unwrap();
}

#[test]
fn agent_auto_creation_persistence_and_coordination_only_mcp_rotation() {
    let put_status = Arc::new(AtomicUsize::new(200));
    let put_status_srv = Arc::clone(&put_status);
    let server = MockSpironServer::start(move |req, _| {
        match (req.method.as_str(), req.uri.as_str()) {
            ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
            ("POST", "/v1/agents") => MockResponse::json(201, json!({ "id": AGENT_ID })),
            ("PUT", u) if u == format!("/v1/agents/{AGENT_ID}/mcp-servers") => {
                let code = put_status_srv.load(Ordering::SeqCst) as u16;
                MockResponse::json(code, json!({ "ok": code == 200 }))
            }
            ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
            ("POST", u) if u == format!("/v1/agents/{AGENT_ID}/messages") => {
                let key = req
                    .headers
                    .get("idempotency-key")
                    .cloned()
                    .unwrap_or_default();
                let rid = if key == REQ_1 { RECEIPT_1 } else { RECEIPT_2 };
                MockResponse::json(
                    202,
                    json!({
                        "id": rid,
                        "agent_id": AGENT_ID,
                        "provider_message_id": key,
                        "status": "completed",
                        "reply": "ok",
                        "input_delivery": input_proof(&key, &key),
                        "usage": { "input_tokens": 10, "output_tokens": 5, "cost_micros": 1_000 }
                    }),
                )
            }
            _ => MockResponse::json(500, json!({ "unexpected": req.uri })),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let tok1 = remote_token("a");
    let tok2 = remote_token("b");
    std::fs::write(
        dir.path().join(REMOTE_MCP_URL_FILE),
        "https://agents.example.com/v1/webhooks/mcp\n",
    )
    .unwrap();
    std::fs::write(dir.path().join(REMOTE_SESSION_TOKEN_FILE), &tok1).unwrap();

    let mut provider = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    let handle = provider
        .create(&sample_session_config(None))
        .expect("auto-create agent");
    assert_eq!(handle.session_id, AGENT_ID);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("spiron-cloud-session-id"))
            .unwrap()
            .trim(),
        AGENT_ID
    );

    // Turn 1 uses tok1; then rotate token on disk before Turn 2.
    let ack1 = provider
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_1.into(),
            text: "turn 1".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap();
    assert!(matches!(
        ack1,
        RemoteDeliveryAck::Accepted {
            turn_finished: true,
            ..
        }
    ));

    std::fs::write(dir.path().join(REMOTE_SESSION_TOKEN_FILE), &tok2).unwrap();
    assert_eq!(
        provider.refresh_mcp_credentials(),
        Some((
            "https://agents.example.com/v1/webhooks/mcp".to_string(),
            tok2.clone()
        ))
    );
    let ack2 = provider
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_2.into(),
            text: "turn 2".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap();
    assert!(matches!(
        ack2,
        RemoteDeliveryAck::Accepted {
            turn_finished: true,
            ..
        }
    ));
    provider.stop().unwrap();

    // Verify exact requests: POST /v1/agents (no host tools), PUT mcp-servers, and rotated per-turn tokens.
    let reqs = server.requests();
    let create_req = reqs
        .iter()
        .find(|r| r.method == "POST" && r.uri == "/v1/agents")
        .expect("POST /v1/agents");
    assert_eq!(
        create_req.headers.get("prefer").map(String::as_str),
        Some("respond-async")
    );
    assert!(create_req.headers.contains_key("idempotency-key"));
    assert_eq!(
        create_req.headers.get("authorization").map(String::as_str),
        Some("Bearer spiron-cloud-test-token")
    );
    assert!(create_req.body.get("mcp_servers").is_none());

    let put_req = reqs
        .iter()
        .find(|r| r.method == "PUT" && r.uri == format!("/v1/agents/{AGENT_ID}/mcp-servers"))
        .expect("PUT /mcp-servers");
    assert_eq!(
        put_req.body["mcp_servers"],
        json!([{
            "name": "anyagents",
            "url": "https://agents.example.com/v1/webhooks/mcp",
            "bearer_token": tok1
        }])
    );

    let msg_reqs: Vec<_> = reqs
        .iter()
        .filter(|r| r.method == "POST" && r.uri == format!("/v1/agents/{AGENT_ID}/messages"))
        .collect();
    assert_eq!(msg_reqs.len(), 2);
    assert_eq!(msg_reqs[0].body["mcp_servers"][0]["bearer_token"], tok1);
    assert_eq!(msg_reqs[1].body["mcp_servers"][0]["bearer_token"], tok2);

    // Restarting on the same config_dir reuses persisted spiron-cloud-session-id and tolerates 404 on PUT /mcp-servers.
    put_status.store(404, Ordering::SeqCst);
    let mut provider2 = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    let h2 = provider2
        .create(&sample_session_config(None))
        .expect("resume persisted session with 404 PUT");
    assert_eq!(h2.session_id, AGENT_ID);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.method == "POST" && r.uri == "/v1/agents")
            .count(),
        1,
        "must not create a second remote agent when spiron-cloud-session-id exists"
    );

    // Insecure http:// MCP URL and local mailbox token must fail closed.
    std::fs::write(
        dir.path().join(REMOTE_MCP_URL_FILE),
        "http://callback.example/v1/webhooks/mcp",
    )
    .unwrap();
    let err = provider2
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_3.into(),
            text: "insecure url".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap_err();
    assert!(matches!(err, RemoteSessionError::Protocol(m) if m.contains("requires HTTPS")));

    std::fs::write(
        dir.path().join(REMOTE_MCP_URL_FILE),
        "https://agents.example.com/v1/webhooks/mcp",
    )
    .unwrap();
    std::fs::write(
        dir.path().join(REMOTE_SESSION_TOKEN_FILE),
        "local-mailbox-capability",
    )
    .unwrap();
    let err = provider2
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_3.into(),
            text: "local token".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, RemoteSessionError::Protocol(m) if m.contains("valid remote session token"))
    );
    provider2.stop().unwrap();
}

#[test]
fn origin_credential_binding_config_lock_and_gateway_key_fail_closed() {
    let gateway_key = format!("{}gateway_fixture_placeholder", "sylphx_sk_");
    assert!(is_sylphx_gateway_key(&gateway_key));
    assert!(!is_sylphx_gateway_key(TEST_TOKEN));
    assert!(is_remote_session_token(&remote_token("f")));
    assert!(!is_remote_session_token("local-capability"));
    assert!(validate_uuid("../escape", "agent_id").is_err());

    let server = MockSpironServer::start(|req, _| match (req.method.as_str(), req.uri.as_str()) {
        ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
        ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
        _ => MockResponse::json(500, json!({})),
    });

    // 1. Gateway key is refused fail-closed with Auth error and never echoed; Debug redacts api_token.
    let dir_gw = tempfile::tempdir().unwrap();
    let gw_cfg = SpironCloudConfig::new(dir_gw.path())
        .with_base_url(&server.base_url)
        .with_api_token(&gateway_key);
    assert!(!format!("{gw_cfg:?}").contains(&gateway_key));
    let mut gw_provider = SpironCloudProvider::new(gw_cfg);
    let err = gw_provider
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap_err();
    assert!(
        matches!(err, RemoteSessionError::Auth(ref m) if !m.contains(&gateway_key)),
        "expected secret-safe Auth refusal, got {err:?}"
    );
    assert!(server.requests().is_empty());

    // 2. Exclusive config lock prevents concurrent controllers on the same config_dir.
    let dir = tempfile::tempdir().unwrap();
    let mut p1 = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    p1.create(&sample_session_config(Some(AGENT_ID))).unwrap();

    let mut p_concurrent = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    let lock_err = p_concurrent
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap_err();
    assert!(matches!(lock_err, RemoteSessionError::Protocol(m) if m.contains("owns this config")));
    p1.stop().unwrap();

    // 3. Switching API origin fails closed.
    let mut p_other_origin = SpironCloudProvider::new(
        SpironCloudConfig::new(dir.path())
            .with_base_url("https://other.spiron.invalid")
            .with_api_token(TEST_TOKEN),
    );
    let origin_err = p_other_origin
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap_err();
    assert!(
        matches!(origin_err, RemoteSessionError::Protocol(m) if m.contains("another API origin"))
    );

    // 4. Switching API credential fails closed with Auth error.
    let mut p_other_cred = SpironCloudProvider::new(
        SpironCloudConfig::new(dir.path())
            .with_base_url(&server.base_url)
            .with_api_token("different-spiron-token"),
    );
    let cred_err = p_other_cred
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap_err();
    assert!(matches!(cred_err, RemoteSessionError::Auth(m) if m.contains("API key changed")));

    // 5. Invalid non-UUID resume_session_id fails closed before any agent mutation.
    let dir_bad = tempfile::tempdir().unwrap();
    let mut p_bad = SpironCloudProvider::new(test_config(dir_bad.path(), &server.base_url));
    let bad_err = p_bad
        .create(&sample_session_config(Some("../bad-agent-id")))
        .unwrap_err();
    assert!(matches!(bad_err, RemoteSessionError::Protocol(m) if m.contains("invalid agent_id")));
}

#[test]
fn full_turn_lifecycle_via_remote_session_harness_with_proof_usage_and_no_local_pid() {
    let poll_count = Arc::new(AtomicUsize::new(0));
    let poll_count_srv = Arc::clone(&poll_count);
    let release_complete = Arc::new(AtomicBool::new(false));
    let release_complete_srv = Arc::clone(&release_complete);

    let server = MockSpironServer::start(move |req, _| {
        match (req.method.as_str(), req.uri.as_str()) {
            ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
            ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
            ("POST", u) if u == format!("/v1/agents/{AGENT_ID}/messages") => MockResponse::json(
                202,
                json!({
                    "id": RECEIPT_1,
                    "agent_id": AGENT_ID,
                    "provider_message_id": REQ_1,
                    "status": "working",
                    "input_delivery": null
                }),
            ),
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_1}") => {
                poll_count_srv.fetch_add(1, Ordering::SeqCst);
                if !release_complete_srv.load(Ordering::SeqCst) {
                    // Emit durable next-step consumption proof while turn is still working.
                    MockResponse::json(
                        200,
                        json!({
                            "id": RECEIPT_1,
                            "agent_id": AGENT_ID,
                            "provider_message_id": REQ_1,
                            "status": "working",
                            "input_delivery": input_proof(REQ_1, REQ_1)
                        }),
                    )
                } else {
                    MockResponse::json(
                        200,
                        json!({
                            "id": RECEIPT_1,
                            "agent_id": AGENT_ID,
                            "provider_message_id": REQ_1,
                            "status": "completed",
                            "reply": "All checks passed.",
                            "input_delivery": input_proof(REQ_1, REQ_1),
                            "usage": {
                                "input_tokens": 1200,
                                "output_tokens": 340,
                                "cache_read_tokens": 500,
                                "cache_write_tokens": 80,
                                "cost_micros": 420_000
                            }
                        }),
                    )
                }
            }
            _ => MockResponse::json(500, json!({ "unexpected": req.uri })),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let mut harness = SpironCloudProvider::start_harness_with_poll_interval(
        test_config(dir.path(), &server.base_url),
        &sample_session_config(Some(AGENT_ID)),
        Duration::from_millis(15),
    )
    .expect("start SpironCloudHarness");

    // ADR 0010 D6/D7: zero local OS child process.
    assert_eq!(harness.process_id(), None);
    assert_eq!(harness.session_id().as_deref(), Some(AGENT_ID));
    assert!(harness.health().process_alive);

    let out = harness
        .deliver(&sample_outbound(REQ_1, 1, "run checks"))
        .unwrap();
    assert_eq!(out, DeliverOutcome::InFlight);

    // Wait for next-step InputConsumed proof BEFORE completion.
    wait_until(Duration::from_secs(3), || {
        harness.proven(REQ_1)
            == Some(Proof::Taken {
                turn_finished: false,
            })
    });
    assert_eq!(harness.turns_served(), 0);

    // Release terminal completed response with real usage + cost_micros.
    release_complete.store(true, Ordering::SeqCst);
    wait_until(Duration::from_secs(3), || {
        harness.proven(REQ_1)
            == Some(Proof::Taken {
                turn_finished: true,
            })
    });
    assert_eq!(harness.turns_served(), 1);
    assert_eq!(
        harness.reply_for(REQ_1).as_deref(),
        Some("All checks passed.")
    );
    assert_eq!(
        harness.served_usage(),
        TurnUsage {
            input_tokens: 1200,
            output_tokens: 340,
            cache_read_tokens: 500,
            cache_write_tokens: 80,
            reported_cost_micros: Some(420_000),
        }
    );
    assert!(poll_count.load(Ordering::SeqCst) >= 2);
    harness.stop_verified().unwrap();
}

#[test]
fn completed_reply_supports_output_items_conversation_pagination_and_proof_immutability() {
    let poll_calls = Arc::new(AtomicUsize::new(0));
    let poll_calls_srv = Arc::clone(&poll_calls);

    let server = MockSpironServer::start(move |req, _| {
        match (req.method.as_str(), req.uri.as_str()) {
            ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
            ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
            ("POST", u) if u == format!("/v1/agents/{AGENT_ID}/messages") => {
                let key = req.headers.get("idempotency-key").map(String::as_str);
                match key {
                    Some(REQ_1) => MockResponse::json(
                        202,
                        json!({
                            "id": RECEIPT_1,
                            "agent_id": AGENT_ID,
                            "provider_message_id": REQ_1,
                            "status": "working",
                            "input_delivery": input_proof(REQ_1, REQ_1)
                        }),
                    ),
                    Some(REQ_2) => MockResponse::json(
                        202,
                        json!({
                            "id": RECEIPT_2,
                            "agent_id": AGENT_ID,
                            "provider_message_id": REQ_2,
                            "status": "working",
                            "input_delivery": input_proof(REQ_2, REQ_2)
                        }),
                    ),
                    _ => MockResponse::json(
                        202,
                        json!({
                            "id": RECEIPT_3,
                            "agent_id": AGENT_ID,
                            "provider_message_id": REQ_3,
                            "status": "working",
                            "input_delivery": input_proof(REQ_3, REQ_3)
                        }),
                    ),
                }
            }
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_1}") => MockResponse::json(
                200,
                json!({
                    "id": RECEIPT_1,
                    "agent_id": AGENT_ID,
                    "provider_message_id": REQ_1,
                    "status": "completed",
                    "reply": null,
                    "output": [
                        { "type": "text", "text": "part one" },
                        { "type": "text", "text": "part two" }
                    ],
                    "input_delivery": input_proof(REQ_1, REQ_1),
                    "usage": { "input_tokens": 50, "output_tokens": 20, "cached_tokens": 15, "cost_usd": 0.25 }
                }),
            ),
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_2}") => MockResponse::json(
                200,
                json!({
                    "id": RECEIPT_2,
                    "agent_id": AGENT_ID,
                    "provider_message_id": REQ_2,
                    "status": "completed",
                    "reply": null,
                    "input_delivery": input_proof(REQ_2, REQ_2)
                }),
            ),
            ("GET", u) if u == format!("/v1/agents/{AGENT_ID}/conversation?limit=100") => {
                MockResponse::json(
                    200,
                    json!({
                        "agent_id": AGENT_ID,
                        "data": [
                            { "id": "item-2", "role": "agent", "task_id": RECEIPT_2, "output": { "type": "text", "text": "second page" } }
                        ],
                        "has_more": true,
                        "next_cursor": "cur/1+2"
                    }),
                )
            }
            ("GET", u)
                if u == format!(
                    "/v1/agents/{AGENT_ID}/conversation?limit=100&before=cur%2F1%2B2"
                ) =>
            {
                MockResponse::json(
                    200,
                    json!({
                        "agent_id": AGENT_ID,
                        "data": [
                            { "id": "item-1", "role": "agent", "task_id": RECEIPT_2, "output": { "type": "text", "text": "first page" } },
                            { "id": "item-2", "role": "agent", "task_id": RECEIPT_2, "output": { "type": "text", "text": "second page" } }
                        ],
                        "has_more": false,
                        "next_cursor": null
                    }),
                )
            }
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_3}") => {
                poll_calls_srv.fetch_add(1, Ordering::SeqCst);
                // Mutate entry_id after initial proof was already recorded -> must fail closed!
                MockResponse::json(
                    200,
                    json!({
                        "id": RECEIPT_3,
                        "agent_id": AGENT_ID,
                        "provider_message_id": REQ_3,
                        "status": "completed",
                        "reply": "tampered",
                        "input_delivery": input_proof(REQ_1, REQ_3)
                    }),
                )
            }
            _ => MockResponse::json(500, json!({ "unexpected": req.uri })),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let mut provider = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    provider
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap();

    // 1. Output items array + cost_usd conversion + cached_tokens alias.
    provider
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_1.into(),
            text: "output items".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap();
    let evs1 = provider.poll_events(None).unwrap();
    assert!(matches!(
        &evs1[0],
        RemoteEvent::Completed {
            reply_text: Some(text),
            usage: Some(TurnUsage {
                input_tokens: 50,
                output_tokens: 20,
                cache_read_tokens: 15,
                reported_cost_micros: Some(250_000),
                ..
            }),
            ..
        } if text == "part one\npart two"
    ));

    // 2. Paginated conversation fallback with URL-encoded cursor and deduplication.
    provider
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_2.into(),
            text: "conversation fallback".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap();
    let evs2 = provider.poll_events(None).unwrap();
    assert!(matches!(
        &evs2[0],
        RemoteEvent::Completed {
            reply_text: Some(text),
            ..
        } if text == "first page\nsecond page"
    ));

    // 3. Proof immutability: mutating input_delivery after observation fails closed.
    provider
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_3.into(),
            text: "immutable proof".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap();
    let err = provider.poll_events(None).unwrap_err();
    assert!(matches!(err, RemoteSessionError::Protocol(m) if m.contains("proof changed")));
    assert_eq!(poll_calls.load(Ordering::SeqCst), 1);
    provider.stop().unwrap();
}

#[test]
fn duplicate_request_id_idempotent_replay_content_reuse_refusal_and_503_recovery() {
    let post_attempts = Arc::new(AtomicUsize::new(0));
    let post_attempts_srv = Arc::clone(&post_attempts);

    let server = MockSpironServer::start(move |req, _| {
        match (req.method.as_str(), req.uri.as_str()) {
            ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
            ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
            ("POST", u) if u == format!("/v1/agents/{AGENT_ID}/messages") => {
                let attempt = post_attempts_srv.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    // Simulate transient 503 with secret-looking body (must retain pending file & never echo).
                    return MockResponse::json(503, json!({ "secret": "do-not-echo" }));
                }
                MockResponse::json(
                    202,
                    json!({
                        "id": RECEIPT_1,
                        "agent_id": AGENT_ID,
                        "provider_message_id": REQ_1,
                        "status": "completed",
                        "reply": "done once",
                        "input_delivery": input_proof(REQ_1, REQ_1)
                    }),
                )
            }
            _ => MockResponse::json(500, json!({ "unexpected": req.uri })),
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let mut provider = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    provider
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap();

    let input = RemoteDeliveryInput {
        request_id: REQ_1.into(),
        text: "same payload".into(),
        steer: false,
        mcp_url: None,
        mcp_token: None,
    };

    // First POST gets 503 -> error is secret-safe and durable record stays on disk.
    let err = provider.deliver(&input).unwrap_err();
    assert!(
        matches!(err, RemoteSessionError::Transport(ref m) if m.contains("503") && !m.contains("do-not-echo"))
    );
    assert!(
        dir.path()
            .join(format!("spiron-cloud-inputs/{REQ_1}.json"))
            .exists()
    );

    // Reusing the same UUID with DIFFERENT content fails closed before any HTTP call.
    let mismatch = provider
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_1.into(),
            text: "different payload".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap_err();
    assert!(matches!(mismatch, RemoteSessionError::Protocol(m) if m.contains("different content")));

    // Retrying with original payload succeeds using the exact same Idempotency-Key.
    let ack = provider.deliver(&input).unwrap();
    assert!(matches!(
        ack,
        RemoteDeliveryAck::Accepted {
            turn_finished: true,
            ..
        }
    ));

    // Replaying the same request_id again returns Duplicate with ZERO additional POST!
    let dup1 = provider.deliver(&input).unwrap();
    assert!(matches!(dup1, RemoteDeliveryAck::Duplicate { .. }));
    provider.stop().unwrap();

    // Even across a fresh provider restart, the durable completed record replays as Duplicate without POST.
    let mut provider2 = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    provider2
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap();
    let dup2 = provider2.deliver(&input).unwrap();
    assert!(matches!(dup2, RemoteDeliveryAck::Duplicate { .. }));
    provider2.stop().unwrap();

    // Total POST /messages calls: 1 failed (503) + 1 succeeded = 2; zero duplicates!
    let post_reqs: Vec<_> = server
        .requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.uri == format!("/v1/agents/{AGENT_ID}/messages"))
        .collect();
    assert_eq!(post_reqs.len(), 2);
    assert_eq!(
        post_reqs[0].headers.get("idempotency-key"),
        post_reqs[1].headers.get("idempotency-key")
    );
    assert_eq!(post_reqs[0].body, post_reqs[1].body);
}

#[test]
fn hard_remote_budget_cap_and_workspace_spend_cap_enforcement() {
    let server = MockSpironServer::start(|req, _| match (req.method.as_str(), req.uri.as_str()) {
        ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
        ("GET", "/v1/spend-cap") => MockResponse::json(
            200,
            json!({ "reached": false, "spent_micros": 0, "limit_micros": 10_000_000 }),
        ),
        ("POST", u) if u == format!("/v1/agents/{AGENT_ID}/messages") => {
            let key = req.headers.get("idempotency-key").cloned().unwrap();
            let rid = if key == REQ_1 { RECEIPT_1 } else { RECEIPT_2 };
            MockResponse::json(
                202,
                json!({
                    "id": rid,
                    "agent_id": AGENT_ID,
                    "provider_message_id": key,
                    "status": "working",
                    "input_delivery": input_proof(&key, &key)
                }),
            )
        }
        ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_1}") => MockResponse::json(
            200,
            json!({
                "id": RECEIPT_1,
                "agent_id": AGENT_ID,
                "provider_message_id": REQ_1,
                "status": "completed",
                "reply": "turn 1 ok",
                "input_delivery": input_proof(REQ_1, REQ_1),
                "usage": { "input_tokens": 500, "output_tokens": 100, "cost_micros": 600_000 }
            }),
        ),
        ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_2}") => MockResponse::json(
            200,
            json!({
                "id": RECEIPT_2,
                "agent_id": AGENT_ID,
                "provider_message_id": REQ_2,
                "status": "completed",
                "reply": "turn 2 crosses cap",
                "input_delivery": input_proof(REQ_2, REQ_2),
                "usage": { "input_tokens": 500, "output_tokens": 100, "cost_micros": 600_000 }
            }),
        ),
        _ => MockResponse::json(500, json!({ "unexpected": req.uri })),
    });

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = sample_session_config(Some(AGENT_ID));
    cfg.budget_cap_micros = Some(1_000_000); // $1.00 hard cap

    let mut harness = SpironCloudProvider::start_harness_with_poll_interval(
        test_config(dir.path(), &server.base_url),
        &cfg,
        Duration::from_millis(10),
    )
    .expect("start budgeted harness");

    // Turn 1: $0.60 spent (< $1.00).
    let out1 = harness
        .deliver(&sample_outbound(REQ_1, 1, "turn 1"))
        .unwrap();
    assert_eq!(
        out1,
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    wait_until(Duration::from_secs(3), || harness.turns_served() == 1);
    let b1 = harness.budget().unwrap();
    assert_eq!(b1.spent_micros, 600_000);
    assert!(!b1.is_exceeded());

    // Turn 2: $1.20 cumulative (>= $1.00) -> crosses cap and triggers BudgetExceeded.
    let out2 = harness
        .deliver(&sample_outbound(REQ_2, 2, "turn 2"))
        .unwrap();
    assert_eq!(
        out2,
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    wait_until(Duration::from_secs(3), || harness.turns_served() == 2);
    let b2 = harness.budget().unwrap();
    assert_eq!(b2.spent_micros, 1_200_000);
    assert!(b2.is_exceeded());
    assert_eq!(harness.served_usage().reported_cost_micros, Some(1_200_000));

    // Turn 3: refused fail-closed BEFORE any HTTP POST.
    let out3 = harness
        .deliver(&sample_outbound(REQ_3, 3, "turn 3"))
        .unwrap();
    assert!(matches!(
        out3,
        DeliverOutcome::Rejected {
            message_specific: false,
            ..
        }
    ));
    let post_count = server
        .requests()
        .iter()
        .filter(|r| r.method == "POST" && r.uri.ends_with("/messages"))
        .count();
    assert_eq!(post_count, 2, "Turn 3 must not hit POST /messages");
    harness.stop_verified().unwrap();

    // Also verify workspace GET /v1/spend-cap reached=true blocks deliver fail-closed before HTTP POST.
    let cap_server =
        MockSpironServer::start(|req, _| match (req.method.as_str(), req.uri.as_str()) {
            ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
            ("GET", "/v1/spend-cap") => MockResponse::json(
                200,
                json!({ "reached": true, "spent_micros": 5_000_000, "limit_micros": 5_000_000 }),
            ),
            _ => MockResponse::json(500, json!({ "unexpected": req.uri })),
        });
    let dir2 = tempfile::tempdir().unwrap();
    let mut cap_provider = SpironCloudProvider::new(test_config(dir2.path(), &cap_server.base_url));
    cap_provider
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap();
    assert!(cap_provider.spend_cap_reached());
    let ack = cap_provider
        .deliver(&RemoteDeliveryInput {
            request_id: REQ_1.into(),
            text: "blocked by workspace spend cap".into(),
            steer: false,
            mcp_url: None,
            mcp_token: None,
        })
        .unwrap();
    assert_eq!(
        ack,
        RemoteDeliveryAck::BudgetExceeded {
            spent_micros: 5_000_000,
            cap_micros: 5_000_000,
        }
    );
    assert!(
        cap_server
            .requests()
            .iter()
            .all(|r| !(r.method == "POST" && r.uri.ends_with("/messages")))
    );
    cap_provider.stop().unwrap();
}

#[test]
fn interrupt_cancel_with_404_and_409_noop_and_stop_verified_releases_lock() {
    let cancel_code = Arc::new(AtomicUsize::new(200));
    let cancel_code_srv = Arc::clone(&cancel_code);

    let server =
        MockSpironServer::start(
            move |req, _| match (req.method.as_str(), req.uri.as_str()) {
                ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
                ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
                ("POST", u) if u == format!("/v1/agents/{AGENT_ID}/messages") => {
                    let key = req.headers.get("idempotency-key").cloned().unwrap();
                    let rid = if key == REQ_1 { RECEIPT_1 } else { RECEIPT_2 };
                    MockResponse::json(
                        202,
                        json!({
                            "id": rid,
                            "agent_id": AGENT_ID,
                            "provider_message_id": key,
                            "status": "working",
                            "input_delivery": input_proof(&key, &key)
                        }),
                    )
                }
                ("GET", u) if u.starts_with("/v1/message-receipts/") => {
                    let rid = u.trim_start_matches("/v1/message-receipts/");
                    let key = if rid == RECEIPT_1 { REQ_1 } else { REQ_2 };
                    MockResponse::json(
                        200,
                        json!({
                            "id": rid,
                            "agent_id": AGENT_ID,
                            "provider_message_id": key,
                            "status": "working",
                            "input_delivery": input_proof(key, key)
                        }),
                    )
                }
                ("POST", u) if u.ends_with(":cancel") => {
                    let status = cancel_code_srv.load(Ordering::SeqCst) as u16;
                    let rid = u
                        .trim_start_matches("/v1/message-receipts/")
                        .trim_end_matches(":cancel");
                    if status == 200 {
                        MockResponse::json(200, json!({ "id": rid, "status": "canceled" }))
                    } else {
                        MockResponse::json(status, json!({ "error": "already terminal" }))
                    }
                }
                _ => MockResponse::json(500, json!({ "unexpected": req.uri })),
            },
        );

    // 1. Harness cancel + stop_verified cancels active receipts and releases spiron-cloud.lock.
    let dir = tempfile::tempdir().unwrap();
    let mut harness = SpironCloudProvider::start_harness_with_poll_interval(
        test_config(dir.path(), &server.base_url),
        &sample_session_config(Some(AGENT_ID)),
        Duration::from_millis(50),
    )
    .unwrap();
    harness
        .deliver(&sample_outbound(REQ_1, 1, "cancel me"))
        .unwrap();
    assert!(dir.path().join("spiron-cloud-active-run.json").exists());
    harness.cancel().unwrap();
    assert!(!dir.path().join("spiron-cloud-active-run.json").exists());

    // Start another active turn and shut down cleanly via stop_verified.
    harness
        .deliver(&sample_outbound(REQ_2, 2, "stop me"))
        .unwrap();
    harness.stop_verified().unwrap();
    assert!(!harness.health().process_alive);
    assert!(!dir.path().join("spiron-cloud-active-run.json").exists());

    let cancel_uris: Vec<_> = server
        .requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.uri.ends_with(":cancel"))
        .map(|r| r.uri)
        .collect();
    assert!(cancel_uris.contains(&format!("/v1/message-receipts/{RECEIPT_1}:cancel")));
    assert!(cancel_uris.contains(&format!("/v1/message-receipts/{RECEIPT_2}:cancel")));

    // 2. Lock was released cleanly -> new provider opens immediately and handles 404 & 409 as idempotent Ok(()).
    let mut p2 = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    p2.create(&sample_session_config(Some(AGENT_ID))).unwrap();
    cancel_code.store(404, Ordering::SeqCst);
    p2.interrupt(Some(RECEIPT_1))
        .expect("404 on terminal receipt is idempotent Ok(()) per ADR-1515");
    cancel_code.store(409, Ordering::SeqCst);
    p2.interrupt(Some(RECEIPT_2))
        .expect("409 on completion race is idempotent Ok(())");
    p2.stop().unwrap();
}

#[test]
fn auth_401_quota_429_retry_after_4xx_size_rejection_and_transient_5xx_tolerance() {
    let secret_prompt = "CONFIDENTIAL_PROMPT_CANARY_9911";
    let mode = Arc::new(AtomicUsize::new(0));
    let mode_srv = Arc::clone(&mode);
    let poll_5xx = Arc::new(AtomicUsize::new(0));
    let poll_5xx_srv = Arc::clone(&poll_5xx);

    let server =
        MockSpironServer::start(
            move |req, _| match (req.method.as_str(), req.uri.as_str()) {
                ("GET", "/v1/openapi.json") => MockResponse::json(200, strict_openapi()),
                ("GET", "/v1/spend-cap") => MockResponse::json(404, json!({})),
                ("POST", u) if u == format!("/v1/agents/{AGENT_ID}/messages") => {
                    match mode_srv.load(Ordering::SeqCst) {
                        0 => MockResponse::json(
                            401,
                            json!({
                                "error": format!("invalid token {TEST_TOKEN} for {secret_prompt}")
                            }),
                        ),
                        1 => MockResponse::json(
                            429,
                            json!({ "error": format!("rate limited {TEST_TOKEN}") }),
                        )
                        .with_header("Retry-After", "45"),
                        2 => MockResponse::json(429, json!({ "error": "rate limited default" })),
                        3 => MockResponse::json(
                            413,
                            json!({
                                "error": {
                                    "code": "input_too_large",
                                    "message": format!("Payload too large: {secret_prompt}")
                                }
                            }),
                        ),
                        4 => MockResponse::json(
                            402,
                            json!({
                                "code": "spend_cap_reached",
                                "spent_micros": 2_000_000,
                                "limit_micros": 2_000_000
                            }),
                        ),
                        _ => MockResponse::json(
                            202,
                            json!({
                                "id": RECEIPT_1,
                                "agent_id": AGENT_ID,
                                "provider_message_id": REQ_2,
                                "status": "working",
                                "input_delivery": input_proof(REQ_2, REQ_2)
                            }),
                        ),
                    }
                }
                ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_1}") => {
                    let n = poll_5xx_srv.fetch_add(1, Ordering::SeqCst);
                    if n < 5 {
                        MockResponse::json(503, json!({ "error": "transient upstream 503" }))
                    } else {
                        MockResponse::json(
                            200,
                            json!({
                                "id": RECEIPT_1,
                                "agent_id": AGENT_ID,
                                "provider_message_id": REQ_2,
                                "status": "completed",
                                "reply": "recovered after 5 transient 503s",
                                "input_delivery": input_proof(REQ_2, REQ_2)
                            }),
                        )
                    }
                }
                _ => MockResponse::json(500, json!({ "unexpected": req.uri })),
            },
        );

    let dir = tempfile::tempdir().unwrap();
    let mut provider = SpironCloudProvider::new(test_config(dir.path(), &server.base_url));
    provider
        .create(&sample_session_config(Some(AGENT_ID)))
        .unwrap();

    let make_input = |id: &str| RemoteDeliveryInput {
        request_id: id.into(),
        text: secret_prompt.into(),
        steer: false,
        mcp_url: None,
        mcp_token: None,
    };

    // 1. 401 Auth fault — secret-safe (never echoes token or prompt).
    mode.store(0, Ordering::SeqCst);
    let err401 = provider.deliver(&make_input(REQ_1)).unwrap_err();
    match err401 {
        RemoteSessionError::Auth(msg) => {
            assert!(!msg.contains(TEST_TOKEN), "leaked token: {msg}");
            assert!(!msg.contains(secret_prompt), "leaked prompt: {msg}");
        }
        other => panic!("expected Auth error, got {other:?}"),
    }

    // 2. 429 Quota with Retry-After: 45s — honors header and never echoes secrets.
    let before_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    mode.store(1, Ordering::SeqCst);
    let err429 = provider.deliver(&make_input(REQ_1)).unwrap_err();
    match err429 {
        RemoteSessionError::Quota {
            reset_at_ms,
            message,
        } => {
            assert!(!message.contains(TEST_TOKEN), "leaked token: {message}");
            let delta = reset_at_ms - before_ms;
            assert!(
                (40_000..=55_000).contains(&delta),
                "expected ~45s Retry-After delta, got {delta}ms"
            );
        }
        other => panic!("expected Quota error, got {other:?}"),
    }

    // 3. 429 Quota without Retry-After — defaults to 60s.
    let before_default = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    mode.store(2, Ordering::SeqCst);
    let err429_default = provider.deliver(&make_input(REQ_1)).unwrap_err();
    match err429_default {
        RemoteSessionError::Quota { reset_at_ms, .. } => {
            let delta = reset_at_ms - before_default;
            assert!(
                (55_000..=70_000).contains(&delta),
                "expected ~60s default delta, got {delta}ms"
            );
        }
        other => panic!("expected default Quota error, got {other:?}"),
    }

    // 4. 413 Payload Too Large -> Rejected { message_specific: true } without echoing prompt.
    mode.store(3, Ordering::SeqCst);
    let rej = provider.deliver(&make_input(REQ_1)).unwrap();
    match rej {
        RemoteDeliveryAck::Rejected {
            message_specific,
            reason,
            ..
        } => {
            assert!(message_specific);
            assert!(!reason.contains(secret_prompt), "leaked prompt: {reason}");
        }
        other => panic!("expected Rejected ack, got {other:?}"),
    }

    // 5. 5 consecutive transient 503s on receipt poll are tolerated; 6th poll succeeds cleanly.
    mode.store(5, Ordering::SeqCst);
    provider.deliver(&make_input(REQ_2)).unwrap();
    for _ in 0..5 {
        assert!(provider.poll_events(None).unwrap().is_empty());
    }
    let settled = provider.poll_events(None).unwrap();
    assert!(matches!(
        &settled[0],
        RemoteEvent::Completed {
            reply_text: Some(text),
            ..
        } if text == "recovered after 5 transient 503s"
    ));

    // 6. 402 spend-cap response maps to BudgetExceeded.
    mode.store(4, Ordering::SeqCst);
    let cap_ack = provider.deliver(&make_input(REQ_3)).unwrap();
    assert_eq!(
        cap_ack,
        RemoteDeliveryAck::BudgetExceeded {
            spent_micros: 2_000_000,
            cap_micros: 2_000_000,
        }
    );
    assert!(provider.spend_cap_reached());
    provider.stop().unwrap();
}
