use super::*;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::IntoResponse,
};
use std::{collections::VecDeque, sync::Arc, thread};

const AGENT: &str = "00000000-0000-4000-8000-000000000001";
const TASK: &str = "00000000-0000-4000-8000-000000000002";
#[derive(Clone, Debug)]
pub(super) struct Request {
    pub(super) method: Method,
    pub(super) uri: String,
    pub(super) headers: HeaderMap,
    pub(super) body: Value,
}
type Script = Arc<Mutex<(VecDeque<(u16, Value)>, Vec<Request>)>>;
pub(super) struct Mock {
    pub(super) base: String,
    state: Script,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<thread::JoinHandle<()>>,
}
impl Mock {
    pub(super) fn new(replies: Vec<(u16, Value)>) -> Self {
        let state: Script = Arc::new(Mutex::new((replies.into(), Vec::new())));
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let shared = state.clone();
        let join = thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    addr_tx.send(listener.local_addr().unwrap()).unwrap();
                    let app = Router::new().fallback(record).with_state(shared);
                    axum::serve(listener, app)
                        .with_graceful_shutdown(async {
                            let _ = stopped.await;
                        })
                        .await
                        .unwrap();
                });
        });
        Self {
            base: format!("http://{}", addr_rx.recv().unwrap()),
            state,
            stop: Some(stop),
            join: Some(join),
        }
    }
    pub(super) fn config(&self) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("cloud.json"),
            json!({"base_url":self.base,"token":"api-test-token"}).to_string(),
        )
        .unwrap();
        dir
    }
    pub(super) fn requests(&self) -> Vec<Request> {
        self.state.lock().unwrap().1.clone()
    }
    pub(super) fn consumed(&self) {
        assert!(self.state.lock().unwrap().0.is_empty());
    }
}
impl Drop for Mock {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.join.take().unwrap().join().unwrap();
    }
}
async fn record(
    State(state): State<Script>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let mut state = state.lock().unwrap();
    state.1.push(Request {
        method,
        uri: uri.to_string(),
        headers,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    });
    let (status, body) = state
        .0
        .pop_front()
        .unwrap_or((500, json!({"unexpected":"request"})));
    (StatusCode::from_u16(status).unwrap(), axum::Json(body))
}
fn receipt(status: &str) -> Value {
    json!({"id":TASK,"agent_id":AGENT,"status":status,"reply":null})
}
fn output(id: &str, text: &str) -> Value {
    json!({"id":id,"role":"agent","task_id":TASK,"output":{"id":id,"type":"text","text":text}})
}
fn page(data: Vec<Value>) -> Value {
    json!({"agent_id":AGENT,"data":data,"has_more":false,"next_cursor":null})
}
fn turn(config: &Path, prompt: &str) -> Result<CloudTurnOutcome, String> {
    dispatch(
        config,
        None,
        None,
        prompt,
        &TurnCancel::default(),
        &Mutex::new(None),
    )
}
fn resumed(config: &Path) {
    std::fs::write(config.join(session_filename_for(HARNESS)), AGENT).unwrap();
}

#[test]
fn creates_agent_then_message_and_reads_producer_output() {
    let mock = Mock::new(vec![
        (201, json!({"id":AGENT})),
        (202, receipt("submitted")),
        (200, receipt("completed")),
        (200, page(vec![output("reply-1", "done")])),
    ]);
    let config = mock.config();
    std::fs::write(
        config.path().join(".anyagents-managed-worktree.json"),
        json!({"cloud_sandbox":true,"repo":"example/project","branch":"feature","base":"abc"})
            .to_string(),
    )
    .unwrap();
    let result = turn(config.path(), "work").unwrap();
    assert_eq!(result.session_id, AGENT);
    assert_eq!(result.reply_text, "done");
    let requests = mock.requests();
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[0].uri, "/v1/agents");
    assert!(requests[0].body["tools"]["disabled"].is_array());
    assert_eq!(
        requests[0].body["github"]["repositories"],
        json!(["example/project"])
    );
    assert!(requests[0].body.get("mcp_servers").is_none());
    assert_eq!(requests[1].uri, format!("/v1/agents/{AGENT}/messages"));
    assert!(
        requests[1].body["content"]
            .as_str()
            .unwrap()
            .contains("feature")
    );
    assert!(requests[1].body.get("mcp_servers").is_none());
    for r in &requests[..2] {
        assert_eq!(r.headers["prefer"], "respond-async");
        assert!(!r.headers["idempotency-key"].is_empty());
        assert_eq!(r.headers["authorization"], "Bearer api-test-token");
    }
    assert_eq!(requests[2].uri, format!("/v1/message-receipts/{TASK}"));
    assert_eq!(
        requests[3].uri,
        format!("/v1/agents/{AGENT}/conversation?limit=100")
    );
    assert!(!config.path().join("spiron-cloud-message.json").exists());
    mock.consumed();
}

#[test]
fn attaches_exact_mcp_schema_and_refreshes_remote_capability() {
    let mock = Mock::new(vec![
        (202, receipt("completed")),
        (200, page(vec![output("one", "done")])),
        (202, receipt("completed")),
        (200, page(vec![output("two", "done")])),
    ]);
    let config = mock.config();
    resumed(config.path());
    std::fs::write(
        config.path().join(crate::remote::MCP_URL_FILE),
        "https://callback.example/v1/webhooks/mcp",
    )
    .unwrap();
    for ch in ["a", "b"] {
        let token = format!("aars_{}", ch.repeat(64));
        std::fs::write(config.path().join(crate::remote::TOKEN_FILE), &token).unwrap();
        turn(config.path(), "work").unwrap();
        let reqs = mock.requests();
        assert!(reqs.iter().all(|r| r.method != Method::PUT));
        let post = reqs
            .iter()
            .rev()
            .find(|r| r.method == Method::POST)
            .unwrap();
        assert_eq!(
            post.body["mcp_servers"],
            json!([{"name":"anyagents","url":"https://callback.example/v1/webhooks/mcp?profile=remote","bearer_token":token}])
        );
    }
    mock.consumed();
}

#[test]
fn rejects_insecure_mcp_callback_and_local_mailbox_capability() {
    let mock = Mock::new(vec![]);
    let config = mock.config();
    resumed(config.path());
    std::fs::write(
        config.path().join(crate::remote::TOKEN_FILE),
        format!("aars_{}", "a".repeat(64)),
    )
    .unwrap();
    for url in [
        "http://callback.example/mcp",
        "http://127.0.0.1/mcp",
        "https://user:password@callback.example/mcp",
    ] {
        std::fs::write(config.path().join(crate::remote::MCP_URL_FILE), url).unwrap();
        assert!(
            turn(config.path(), "work")
                .unwrap_err()
                .contains("requires HTTPS")
        );
    }
    std::fs::write(
        config.path().join(crate::remote::MCP_URL_FILE),
        "https://callback.example/mcp",
    )
    .unwrap();
    std::fs::write(
        config.path().join(crate::remote::TOKEN_FILE),
        "local-mailbox-capability",
    )
    .unwrap();
    assert!(
        turn(config.path(), "work")
            .unwrap_err()
            .contains("does not hold a remote session token")
    );
    assert!(mock.requests().is_empty());
}

#[test]
fn replays_original_message_after_unknown_acknowledgement() {
    let mock = Mock::new(vec![
        (503, json!({"secret":"must-not-echo"})),
        (202, receipt("completed")),
        (200, page(vec![output("one", "done")])),
    ]);
    let config = mock.config();
    resumed(config.path());
    assert_eq!(
        turn(config.path(), "work").unwrap_err(),
        "Spiron API returned HTTP 503"
    );
    assert!(config.path().join("spiron-cloud-message.json").exists());
    turn(config.path(), "work").unwrap();
    let requests = mock.requests();
    assert_eq!(requests[0].body, requests[1].body);
    assert_eq!(
        requests[0].headers["idempotency-key"],
        requests[1].headers["idempotency-key"]
    );
    mock.consumed();
}

#[test]
fn cannot_replace_unfinished_prompt_or_change_api_origin() {
    let mock = Mock::new(vec![(503, json!({}))]);
    let config = mock.config();
    resumed(config.path());
    assert!(turn(config.path(), "first").is_err());
    assert!(
        turn(config.path(), "second")
            .unwrap_err()
            .contains("unfinished Spiron turn")
    );
    std::fs::write(
        config.path().join("spiron-cloud-origin.json"),
        json!("https://different.example").to_string(),
    )
    .unwrap();
    assert!(
        turn(config.path(), "first")
            .unwrap_err()
            .contains("another API origin")
    );
    assert_eq!(mock.requests().len(), 1);
}

#[test]
fn paginates_conversation_pages_and_deduplicates_outputs() {
    let mock = Mock::new(vec![
        (202, receipt("completed")),
        (
            200,
            json!({
                "agent_id": AGENT,
                "data": [
                    {"id": "user-1", "role": "user", "task_id": TASK, "content": [{"type": "text", "text": "prompt"}]},
                    output("out-2", "second"),
                    {"id": "other-task", "role": "agent", "task_id": "00000000-0000-4000-8000-000000000099", "output": {"id": "other-task", "type": "text", "text": "ignored"}}
                ],
                "has_more": true,
                "next_cursor": "cur/1+2"
            }),
        ),
        (
            200,
            json!({
                "agent_id": AGENT,
                "data": [
                    output("out-1", "first"),
                    output("out-2", "second")
                ],
                "has_more": false,
                "next_cursor": null
            }),
        ),
    ]);
    let config = mock.config();
    resumed(config.path());
    std::fs::write(
        config.path().join("cloud.json"),
        json!({"base_url": mock.base, "token": "api-test-token", "allow_loopback_mcp": true})
            .to_string(),
    )
    .unwrap();
    std::fs::write(
        config.path().join(crate::remote::MCP_URL_FILE),
        "http://127.0.0.1:8787/v1/webhooks/mcp",
    )
    .unwrap();
    std::fs::write(
        config.path().join(crate::remote::TOKEN_FILE),
        format!("aars_{}", "c".repeat(64)),
    )
    .unwrap();
    let result = turn(config.path(), "work").unwrap();
    assert_eq!(result.reply_text, "first\nsecond");
    let requests = mock.requests();
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(
        requests[0].body["mcp_servers"][0]["url"],
        json!("http://127.0.0.1:8787/v1/webhooks/mcp?profile=remote")
    );
    assert_eq!(
        requests[1].uri,
        format!("/v1/agents/{AGENT}/conversation?limit=100")
    );
    assert_eq!(
        requests[2].uri,
        format!("/v1/agents/{AGENT}/conversation?limit=100&before=cur%2F1%2B2")
    );
    mock.consumed();
}

#[test]
fn cancels_in_flight_receipt_and_tolerates_settled_conflict() {
    let mock = Mock::new(vec![
        (202, receipt("submitted")),
        (200, receipt("working")),
        (200, receipt("canceled")),
    ]);
    let config = mock.config();
    resumed(config.path());
    let cancel = Arc::new(TurnCancel::default());
    let active = Arc::new(Mutex::new(None));
    let cancel_trigger = Arc::clone(&cancel);
    let active_reader = Arc::clone(&active);
    let stopper = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while active_reader.lock().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "turn never recorded active receipt"
            );
            thread::sleep(Duration::from_millis(10));
        }
        cancel_trigger.cancel();
    });
    let err = dispatch(config.path(), None, None, "slow turn", &cancel, &active).unwrap_err();
    stopper.join().unwrap();
    assert!(err.contains("cancellation requested"), "{err}");
    assert!(active.lock().unwrap().is_none());
    let requests = mock.requests();
    let cancel_req = requests.last().unwrap();
    assert_eq!(cancel_req.method, Method::POST);
    assert_eq!(
        cancel_req.uri,
        format!("/v1/message-receipts/{TASK}:cancel")
    );
    assert_eq!(cancel_req.headers["authorization"], "Bearer api-test-token");
    assert_eq!(cancel_req.body, Value::Null);
    mock.consumed();

    let conflict_mock = Mock::new(vec![
        (409, json!({"title": "Conflict"})),
        (200, json!({"id": AGENT, "status": "canceled"})),
    ]);
    let base_with_slash = format!("{}/", conflict_mock.base);
    cancel_cloud_turn(&base_with_slash, HARNESS, Some("api-test-token"), TASK).unwrap();
    assert!(
        cancel_cloud_turn(&base_with_slash, HARNESS, Some("api-test-token"), TASK)
            .unwrap_err()
            .contains("not confirmed")
    );
    assert!(
        cancel_cloud_turn(&base_with_slash, HARNESS, Some("api-test-token"), "../bad")
            .unwrap_err()
            .contains("invalid job_id")
    );
    conflict_mock.consumed();
}

#[test]
fn spiron_cloud_syncs_mcp_servers_streams_sse_and_reattaches_active_run_on_crash() {
    use std::io::{BufRead as _, Read as _};

    // 1. Verify fresh agent with remote MCP config calls PUT /v1/agents/{id}/mcp-servers
    // and POST /v1/agents/{id}/messages handles incremental 200 text/event-stream responses.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let seen_uris = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let seen_clone = Arc::clone(&seen_uris);
    let server = thread::spawn(move || {
        for (idx, stream) in listener.incoming().take(3).enumerate() {
            let mut stream = stream.unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut req_line = String::new();
            let _ = reader.read_line(&mut req_line);
            let mut parts = req_line.split_whitespace();
            let method = parts.next().unwrap_or("").to_string();
            let uri = parts.next().unwrap_or("").to_string();
            let mut len = 0usize;
            loop {
                let mut h = String::new();
                if reader.read_line(&mut h).unwrap_or(0) == 0 || h == "\r\n" {
                    break;
                }
                if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0u8; len];
            let _ = reader.read_exact(&mut body);
            seen_clone.lock().unwrap().push((method, uri));
            match idx {
                0 => {
                    let b = json!({"id": AGENT}).to_string();
                    let _ = write!(
                        stream,
                        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
                        b.len()
                    );
                }
                1 => {
                    let b = json!({"ok": true}).to_string();
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
                        b.len()
                    );
                }
                _ => {
                    let sse = format!(
                        "event: message.delta\ndata: {{\"type\":\"content.delta\",\"delta\":\"streamed \"}}\n\nevent: message.completed\ndata: {{\"type\":\"message.completed\",\"agent_id\":\"{AGENT}\",\"delta\":\"reply ok\"}}\n\ndata: [DONE]\n\n"
                    );
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}",
                        sse.len()
                    );
                }
            }
        }
    });

    let cfg = tempfile::tempdir().unwrap();
    std::fs::write(
        cfg.path().join("cloud.json"),
        json!({"base_url": base, "token": "spiron_sk_test_key_123", "sync_mcp_servers": true})
            .to_string(),
    )
    .unwrap();
    std::fs::write(
        cfg.path().join(crate::remote::MCP_URL_FILE),
        "https://callback.example/v1/webhooks/mcp",
    )
    .unwrap();
    std::fs::write(
        cfg.path().join(crate::remote::TOKEN_FILE),
        format!("aars_{}", "e".repeat(64)),
    )
    .unwrap();
    let out = turn(cfg.path(), "stream prompt").unwrap();
    assert_eq!(out.session_id, AGENT);
    assert_eq!(out.reply_text, "streamed reply ok");
    server.join().unwrap();
    let recorded = seen_uris.lock().unwrap().clone();
    assert_eq!(recorded[0], ("POST".into(), "/v1/agents".into()));
    assert_eq!(
        recorded[1],
        ("PUT".into(), format!("/v1/agents/{AGENT}/mcp-servers"))
    );
    assert_eq!(
        recorded[2],
        ("POST".into(), format!("/v1/agents/{AGENT}/messages"))
    );
    assert!(!cfg.path().join("spiron-cloud-active-run.json").exists());

    // 2. Verify crash re-attach persisted in spiron-cloud-active-run.json and run cancel URL.
    let reattach_mock = Mock::new(vec![
        (200, receipt("working")),
        (
            200,
            json!({"id": TASK, "agent_id": AGENT, "status": "completed", "reply": "reattached ok"}),
        ),
    ]);
    let rcfg = reattach_mock.config();
    resumed(rcfg.path());
    persist_active_run(
        rcfg.path(),
        &ActiveRunState {
            agent_id: AGENT.into(),
            run_id: Some(TASK.into()),
            receipt_id: Some(TASK.into()),
            idempotency_key: Some("idem-1".into()),
            content: Some("recover me".into()),
            started_at_ms: 1,
        },
    )
    .unwrap();
    let recovered = turn(rcfg.path(), "recover me").unwrap();
    assert_eq!(recovered.reply_text, "reattached ok");
    assert!(!rcfg.path().join("spiron-cloud-active-run.json").exists());
    reattach_mock.consumed();

    let run_cancel_mock = Mock::new(vec![(200, json!({"status": "canceled"}))]);
    cancel_spiron_run(
        &run_cancel_mock.base,
        Some("spiron_sk_test"),
        AGENT,
        TASK,
        Some(TASK),
    )
    .unwrap();
    assert_eq!(
        run_cancel_mock.requests()[0].uri,
        format!("/v1/agents/{AGENT}/runs/{TASK}/cancel")
    );
    run_cancel_mock.consumed();
}
