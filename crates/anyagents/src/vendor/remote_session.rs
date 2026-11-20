//! In-process ADR 0010 D7 remote-session adapter bridging vendor-hosted cloud
//! providers directly into [`anyagents_harness::Harness`] and
//! [`anyagents_keeper::Hosted`] with zero local child processes or keepers.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyagents_harness::{
    DeliverOutcome, Harness, HarnessError, HarnessHealth, OutboundMessage, Proof,
    RemoteSessionConfig, TurnUsage, decompose_harness_selector, normalize_harness_name,
    resolve_stored_budget_micros,
};
use anyagents_harness_spiron::{SpironCloudConfig, SpironCloudHarness, SpironCloudProvider};
use anyagents_keeper::Hosted;

/// In-process [`Harness`] + [`Hosted`] adapter wrapping an ADR 0010 D7
/// [`SpironCloudHarness`] (`RemoteSessionHarness<SpironCloudProvider>`).
pub struct RemoteSessionAdapter {
    inner: SpironCloudHarness,
}

impl RemoteSessionAdapter {
    #[cfg(test)]
    pub(crate) fn delivery(&self) -> anyagents_harness::Delivery {
        self.inner.capabilities().delivery
    }

    #[cfg(test)]
    pub(crate) fn budget(&self) -> Option<anyagents_harness::RemoteBudget> {
        self.inner.budget()
    }

    #[cfg(test)]
    pub(crate) fn reply_for(&self, request_id: &str) -> Option<String> {
        self.inner.reply_for(request_id)
    }
}

impl Hosted for RemoteSessionAdapter {
    fn session_id(&self) -> Option<String> {
        // Persist the durable remote agent handle when distinct from per-turn
        // conversation session IDs so restarts resume `/v1/agents/{agent_id}/...`.
        self.inner
            .remote_agent_id()
            .or_else(|| self.inner.session_id())
    }

    fn session_invalidated(&self) -> bool {
        false
    }
}

impl Harness for RemoteSessionAdapter {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.inner.deliver(message)
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        self.inner.proven(message_id)
    }

    fn stop(&mut self) {
        self.inner.stop();
    }

    fn stop_verified(&mut self) -> Result<(), HarnessError> {
        self.inner.stop_verified()
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.inner.resume(session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.inner.cancel()
    }

    fn health(&self) -> HarnessHealth {
        self.inner.health()
    }

    fn served_usage(&self) -> TurnUsage {
        self.inner.served_usage()
    }

    fn turns_served(&self) -> u32 {
        self.inner.turns_served()
    }

    fn clear_quota(&mut self) {
        self.inner.clear_quota();
    }

    fn exit_diagnostic(&self) -> Option<String> {
        self.inner.exit_diagnostic()
    }

    fn process_id(&self) -> Option<u32> {
        self.inner.process_id()
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.inner.watch_activity(notify);
    }
}

fn env_lookup(env: &[(String, String)], keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env.iter()
            .rev()
            .find(|(k, _)| k.as_str() == *key)
            .map(|(_, v)| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

fn env_is_truthy(env: &[(String, String)], key: &str) -> bool {
    env.iter()
        .rev()
        .find(|(k, _)| k.as_str() == key)
        .is_some_and(|(_, v)| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true"))
}

fn read_nonempty_trimmed(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Start an ADR 0010 D7 in-process remote session for a vendor-hosted cloud
/// harness (`spiron@cloud` / `spiron-cloud`), failing closed for harnesses
/// without a configured interactive [`anyagents_harness::RemoteSessionProvider`].
#[allow(clippy::too_many_arguments)]
pub fn start_remote_session(
    harness: &str,
    config_dir: &Path,
    agent_id: &str,
    agent_name: &str,
    model: Option<&str>,
    session: Option<&str>,
    remote_budget_micros: Option<u64>,
    env: &[(String, String)],
) -> Result<RemoteSessionAdapter, String> {
    let canonical_harness = decompose_harness_selector(harness)
        .map(|(h, _)| h)
        .unwrap_or_else(|_| normalize_harness_name(harness).to_string());

    if canonical_harness != "spiron" {
        return Err(format!(
            "cloud harness `{canonical_harness}` has no ADR-0010 D7 interactive RemoteSessionProvider \
             configured (the deprecated `cloud-harness` stream-json shim was removed per ADR-0009 D12 \
             / ADR-0010 D7). Use `spiron@cloud`, run `{canonical_harness}` with `runtime=sandbox` \
             (for example `{canonical_harness}@sandbox`), or configure a serverless \
             `GenericHttpCloudDriver` endpoint."
        ));
    }

    let budget_cap_micros = remote_budget_micros
        .filter(|&cap| cap > 0)
        .or_else(|| resolve_stored_budget_micros(env));

    let mut spiron_cfg =
        SpironCloudConfig::new(config_dir).with_budget_cap_micros(budget_cap_micros);
    if let Some(base_url) = env_lookup(
        env,
        &[
            "ANYAGENTS_SPIRON_CLOUD_URL",
            "SPIRON_API_URL",
            "SPIRON_CLOUD_URL",
        ],
    ) {
        spiron_cfg = spiron_cfg.with_base_url(base_url);
    }
    if let Some(api_token) = env_lookup(
        env,
        &[
            "ANYAGENTS_SPIRON_CLOUD_API_KEY",
            "ANYAGENTS_SPIRON_API_KEY",
            "SPIRON_API_KEY",
            "SPIRON_CLOUD_API_KEY",
        ],
    ) {
        spiron_cfg = spiron_cfg.with_api_token(api_token);
    }
    let allow_loopback_mcp = env_is_truthy(env, "ANYAGENTS_ALLOW_LOOPBACK_MCP")
        || std::env::var("ANYAGENTS_ALLOW_LOOPBACK_MCP")
            .ok()
            .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true"));
    if allow_loopback_mcp {
        spiron_cfg = spiron_cfg.with_allow_loopback_mcp(true);
    }

    let instructions = read_nonempty_trimmed(&config_dir.join("SYSTEM_PROMPT.md"));
    let mcp_url = crate::remote::resolve_public_mcp_url(config_dir).ok();
    let mcp_token = read_nonempty_trimmed(&config_dir.join(crate::remote::TOKEN_FILE));

    let session_cfg = RemoteSessionConfig {
        agent_id: agent_id.to_string(),
        agent_name: agent_name.to_string(),
        model: model
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .map(str::to_string),
        instructions,
        workdir: None,
        mcp_url,
        mcp_token,
        resume_session_id: session
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        budget_cap_micros,
        extra: BTreeMap::new(),
    };

    let inner = SpironCloudProvider::start_harness(spiron_cfg, &session_cfg)
        .map_err(|err| err.to_string())?;
    Ok(RemoteSessionAdapter { inner })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use anyagents_harness::{Context, DeliveryLevel, RemoteBudget, TurnState};
    use serde_json::{Value, json};

    const AGENT_ID: &str = "00000000-0000-4000-8000-000000000001";
    const RECEIPT_1: &str = "00000000-0000-4000-8000-000000000011";
    const RECEIPT_2: &str = "00000000-0000-4000-8000-000000000012";
    const RECEIPT_3: &str = "00000000-0000-4000-8000-000000000013";
    const REQ_1: &str = "00000000-0000-4000-8000-000000000101";
    const REQ_2: &str = "00000000-0000-4000-8000-000000000102";
    const REQ_3: &str = "00000000-0000-4000-8000-000000000103";
    const REQ_4: &str = "00000000-0000-4000-8000-000000000104";

    struct MockServer {
        addr: SocketAddr,
        base_url: String,
        stop: Arc<AtomicBool>,
        join: Option<JoinHandle<()>>,
    }

    impl MockServer {
        fn start<F>(handler: F) -> Self
        where
            F: Fn(&str, &str, Option<&str>, &Value) -> (u16, Value) + Send + Sync + 'static,
        {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock spiron");
            let addr = listener.local_addr().expect("local addr");
            let stop = Arc::new(AtomicBool::new(false));
            let (stop_flag, handler) = (Arc::clone(&stop), Arc::new(handler));
            let join = thread::spawn(move || {
                for mut stream in listener.incoming().flatten() {
                    if stop_flag.load(Ordering::SeqCst) {
                        break;
                    }
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                    let Some((method, uri, key, body)) = read_req(&mut stream) else {
                        continue;
                    };
                    let (status, resp) = handler(&method, &uri, key.as_deref(), &body);
                    let raw = serde_json::to_string(&resp).unwrap();
                    let head = format!(
                        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        raw.len()
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(raw.as_bytes());
                }
            });
            Self {
                addr,
                base_url: format!("http://{addr}"),
                stop,
                join: Some(join),
            }
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.addr);
            if let Some(join) = self.join.take() {
                let _ = join.join();
            }
        }
    }

    fn read_req(stream: &mut TcpStream) -> Option<(String, String, Option<String>, Value)> {
        let (mut raw, mut buf) = (Vec::new(), [0u8; 4096]);
        let end = loop {
            let n = stream.read(&mut buf).ok()?;
            if n == 0 {
                return None;
            }
            raw.extend_from_slice(&buf[..n]);
            if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos;
            }
        };
        let head = String::from_utf8_lossy(&raw[..end]);
        let mut lines = head.split("\r\n");
        let mut req_line = lines.next()?.split_whitespace();
        let (method, uri) = (req_line.next()?.to_string(), req_line.next()?.to_string());
        let (mut key, mut len) = (None, 0usize);
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().eq_ignore_ascii_case("idempotency-key") {
                    key = Some(v.trim().to_string());
                } else if k.trim().eq_ignore_ascii_case("content-length") {
                    len = v.trim().parse().unwrap_or(0);
                }
            }
        }
        let body_start = end + 4;
        while raw.len() < body_start + len {
            let n = stream.read(&mut buf).ok()?;
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        let slice = &raw[body_start..(body_start + len).min(raw.len())];
        let body = if slice.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(slice).unwrap_or(Value::Null)
        };
        Some((method, uri, key, body))
    }

    fn proof(req_id: &str) -> Value {
        json!({
            "session_id": AGENT_ID,
            "entry_id": req_id,
            "turn_entry_id": req_id,
            "recorded_at": "2026-11-20T04:00:00Z"
        })
    }

    fn receipt(id: &str, req_id: &str, status: &str, input_delivery: Value) -> Value {
        json!({
            "id": id,
            "agent_id": AGENT_ID,
            "provider_message_id": req_id,
            "status": status,
            "input_delivery": input_delivery
        })
    }

    fn completed(id: &str, req_id: &str, reply: &str, usage: Value) -> Value {
        let mut r = receipt(id, req_id, "completed", proof(req_id));
        r["reply"] = json!(reply);
        r["usage"] = usage;
        r
    }

    fn msg(id: &str, seq: i64, text: &str) -> OutboundMessage {
        OutboundMessage {
            id: id.into(),
            seq,
            text: text.into(),
            sender: Some("operator".into()),
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        }
    }

    fn wait_until(timeout: Duration, mut pred: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pred() {
                return;
            }
            thread::sleep(Duration::from_millis(15));
        }
        panic!("condition not satisfied within {timeout:?}");
    }

    #[test]
    fn spiron_remote_session_completes_turn_tracks_usage_enforces_budget_and_cancels() {
        let saw_inst = Arc::new(AtomicBool::new(false));
        let saw_mcp = Arc::new(AtomicBool::new(false));
        let saw_cancel = Arc::new(AtomicBool::new(false));
        let (inst_s, mcp_s, cancel_s) = (
            Arc::clone(&saw_inst),
            Arc::clone(&saw_mcp),
            Arc::clone(&saw_cancel),
        );

        let server = MockServer::start(move |method, uri, key, body| {
            match (method, uri) {
            ("GET", "/v1/openapi.json") => (
                200,
                serde_json::from_str(
                    r#"{"openapi":"3.1.0","x-spiron-input-delivery":{"protocol":"session-entry-v1","level":"next-step","idempotent":true,"proof":"input_delivery"}}"#,
                )
                .unwrap(),
            ),
            ("POST", "/v1/agents") => {
                if body.get("instructions").and_then(Value::as_str) == Some("Coordinate via MCP.") {
                    inst_s.store(true, Ordering::SeqCst);
                }
                (201, json!({"id": AGENT_ID}))
            }
            ("PUT", u) if u == format!("/v1/agents/{AGENT_ID}/mcp-servers") => {
                mcp_s.store(true, Ordering::SeqCst);
                (200, json!({"ok": true}))
            }
            ("GET", "/v1/spend-cap") => (404, json!({})),
            ("POST", u) if u == format!("/v1/agents/{AGENT_ID}/messages") => match key {
                Some(REQ_1) => (202, receipt(RECEIPT_1, REQ_1, "working", Value::Null)),
                Some(REQ_2) => (202, receipt(RECEIPT_2, REQ_2, "working", proof(REQ_2))),
                _ => (202, receipt(RECEIPT_3, REQ_3, "working", proof(REQ_3))),
            },
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_1}") => (
                200,
                completed(
                    RECEIPT_1,
                    REQ_1,
                    "all green",
                    json!({"input_tokens": 120, "output_tokens": 45, "cache_read_tokens": 30, "cache_write_tokens": 10, "cost_micros": 600_000}),
                ),
            ),
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_2}") => {
                (200, receipt(RECEIPT_2, REQ_2, "working", proof(REQ_2)))
            }
            ("POST", u) if u == format!("/v1/message-receipts/{RECEIPT_2}:cancel") => {
                cancel_s.store(true, Ordering::SeqCst);
                (200, json!({"id": RECEIPT_2, "status": "cancelled"}))
            }
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_3}") => (
                200,
                completed(
                    RECEIPT_3,
                    REQ_3,
                    "second complete",
                    json!({"input_tokens": 80, "output_tokens": 20, "cost_micros": 500_000}),
                ),
            ),
            _ => (500, json!({"unexpected": uri})),
        }
        });

        let dir = crate::test_tempdir();
        std::fs::write(dir.path().join("SYSTEM_PROMPT.md"), "Coordinate via MCP.\n").unwrap();
        std::fs::write(
            dir.path().join(crate::remote::MCP_URL_FILE),
            "http://127.0.0.1:8788/v1/webhooks/mcp\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join(crate::remote::TOKEN_FILE),
            format!("aars_{}\n", "a".repeat(64)),
        )
        .unwrap();
        let env = vec![
            ("ANYAGENTS_SPIRON_CLOUD_URL".into(), server.base_url.clone()),
            (
                "ANYAGENTS_SPIRON_CLOUD_API_KEY".into(),
                "spiron-test".into(),
            ),
            ("ANYAGENTS_ALLOW_LOOPBACK_MCP".into(), "1".into()),
        ];

        let mut adapter = start_remote_session(
            "spiron@cloud",
            dir.path(),
            "agt_remote_1",
            "cloud-worker",
            Some("openai/gpt-5.4"),
            None,
            Some(1_000_000),
            &env,
        )
        .expect("start spiron remote session");

        assert!(saw_inst.load(Ordering::SeqCst) && saw_mcp.load(Ordering::SeqCst));
        assert_eq!(adapter.process_id(), None);
        assert_eq!(adapter.session_id().as_deref(), Some(AGENT_ID));
        assert!(!adapter.session_invalidated());
        assert_eq!(adapter.delivery().level, DeliveryLevel::NextStep);
        assert!(adapter.health().process_alive && adapter.health().turn_state == TurnState::Idle);

        assert_eq!(
            adapter.deliver(&msg(REQ_1, 1, "check")).unwrap(),
            DeliverOutcome::InFlight
        );
        wait_until(Duration::from_secs(3), || {
            adapter.proven(REQ_1)
                == Some(Proof::Taken {
                    turn_finished: true,
                })
        });
        assert_eq!(adapter.turns_served(), 1);
        assert_eq!(adapter.reply_for(REQ_1).as_deref(), Some("all green"));
        assert_eq!(
            adapter.served_usage(),
            TurnUsage {
                input_tokens: 120,
                output_tokens: 45,
                cache_read_tokens: 30,
                cache_write_tokens: 10,
                reported_cost_micros: Some(600_000)
            }
        );
        assert_eq!(
            adapter.budget(),
            Some(RemoteBudget {
                cap_micros: 1_000_000,
                spent_micros: 600_000
            })
        );

        assert_eq!(
            adapter.deliver(&msg(REQ_2, 2, "cancel")).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );
        adapter.cancel().unwrap();
        assert!(
            saw_cancel.load(Ordering::SeqCst) && adapter.health().turn_state == TurnState::Idle
        );

        assert_eq!(
            adapter.deliver(&msg(REQ_3, 3, "spend")).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );
        wait_until(Duration::from_secs(3), || {
            adapter.proven(REQ_3)
                == Some(Proof::Taken {
                    turn_finished: true,
                })
        });
        assert_eq!(adapter.turns_served(), 2);
        assert_eq!(adapter.served_usage().reported_cost_micros, Some(1_100_000));
        assert!(adapter.budget().unwrap().is_exceeded());
        assert!(matches!(
            adapter.deliver(&msg(REQ_4, 4, "over cap")).unwrap(),
            DeliverOutcome::Rejected {
                message_specific: false,
                ..
            }
        ));
        assert!(adapter.exit_diagnostic().is_some());

        adapter.stop_verified().unwrap();
        assert!(!adapter.health().process_alive);
    }

    #[test]
    fn spiron_cloud_restart_preserves_remote_agent_id_across_distinct_conversation_session_id() {
        const CONV_SESSION_ID: &str = "00000000-0000-4000-8000-000000000999";
        let create_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let create_count_s = Arc::clone(&create_count);
        let msg_agents = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let msg_agents_s = Arc::clone(&msg_agents);

        let distinct_proof = |req_id: &str| {
            json!({
                "session_id": CONV_SESSION_ID,
                "entry_id": req_id,
                "turn_entry_id": req_id,
                "recorded_at": "2026-04-20T00:00:00Z"
            })
        };

        let server = MockServer::start(move |method, uri, key, _body| match (method, uri) {
            ("GET", "/v1/openapi.json") => (
                200,
                json!({
                    "openapi": "3.1.0",
                    "x-spiron-input-delivery": {
                        "protocol": "session-entry-v1",
                        "level": "next-step",
                        "idempotent": true,
                        "proof": "input_delivery"
                    }
                }),
            ),
            ("POST", "/v1/agents") => {
                create_count_s.fetch_add(1, Ordering::SeqCst);
                (201, json!({"id": AGENT_ID, "status": "idle"}))
            }
            ("PUT", u) if u == format!("/v1/agents/{AGENT_ID}/mcp-servers") => {
                (200, json!({"ok": true}))
            }
            ("GET", "/v1/spend-cap") => (
                200,
                json!({
                    "reached": false,
                    "used_micros": 100,
                    "effective_cap_micros": 5_000_000,
                    "plan_cap_micros": 5_000_000,
                    "spend_cap_micros": 5_000_000
                }),
            ),
            ("POST", u) if u.starts_with("/v1/agents/") && u.ends_with("/messages") => {
                let target = u
                    .trim_start_matches("/v1/agents/")
                    .trim_end_matches("/messages")
                    .to_string();
                msg_agents_s.lock().unwrap().push(target.clone());
                if target != AGENT_ID {
                    return (
                        404,
                        json!({
                            "type": "https://api.spiron.ai/errors/not-found",
                            "title": "Not Found",
                            "detail": "agent not found"
                        }),
                    );
                }
                match key {
                    Some(REQ_1) => (
                        202,
                        json!({
                            "id": RECEIPT_1,
                            "agent_id": AGENT_ID,
                            "provider_message_id": REQ_1,
                            "status": "working",
                            "input_delivery": distinct_proof(REQ_1)
                        }),
                    ),
                    Some(REQ_2) => (
                        202,
                        json!({
                            "id": RECEIPT_2,
                            "agent_id": AGENT_ID,
                            "provider_message_id": REQ_2,
                            "status": "working",
                            "input_delivery": distinct_proof(REQ_2)
                        }),
                    ),
                    _ => (400, json!({"detail": "unexpected request id"})),
                }
            }
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_1}") => (
                200,
                json!({
                    "id": RECEIPT_1,
                    "agent_id": AGENT_ID,
                    "provider_message_id": REQ_1,
                    "status": "completed",
                    "reply": "turn 1 ok",
                    "input_delivery": distinct_proof(REQ_1),
                    "usage": {"input_tokens": 20, "output_tokens": 10, "cost_micros": 500}
                }),
            ),
            ("GET", u) if u == format!("/v1/message-receipts/{RECEIPT_2}") => (
                200,
                json!({
                    "id": RECEIPT_2,
                    "agent_id": AGENT_ID,
                    "provider_message_id": REQ_2,
                    "status": "completed",
                    "reply": "turn 2 resumed",
                    "input_delivery": distinct_proof(REQ_2),
                    "usage": {"input_tokens": 25, "output_tokens": 12, "cost_micros": 600}
                }),
            ),
            _ => (404, json!({"detail": format!("unexpected {method} {uri}")})),
        });

        let dir = crate::test_tempdir();
        let env = vec![
            ("ANYAGENTS_SPIRON_CLOUD_URL".into(), server.base_url.clone()),
            (
                "ANYAGENTS_SPIRON_CLOUD_API_KEY".into(),
                "spiron_sk_test_resume".into(),
            ),
        ];

        // Turn 1: creates remote agent, receives distinct conversation session_id in input_delivery.
        let mut adapter1 = start_remote_session(
            "spiron",
            dir.path(),
            "agt_resume_1",
            "resume-worker",
            Some("z-ai/glm-5.3-flash"),
            None,
            Some(500_000),
            &env,
        )
        .expect("start turn 1 session");
        assert_eq!(adapter1.session_id().as_deref(), Some(AGENT_ID));

        assert_eq!(
            adapter1.deliver(&msg(REQ_1, 1, "turn 1")).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );
        wait_until(Duration::from_secs(3), || {
            adapter1.proven(REQ_1)
                == Some(Proof::Taken {
                    turn_finished: true,
                })
        });
        assert_eq!(adapter1.reply_for(REQ_1).as_deref(), Some("turn 1 ok"));
        // Hosted::session_id MUST stay anchored to remote AGENT_ID, never CONV_SESSION_ID.
        assert_eq!(
            adapter1.session_id().as_deref(),
            Some(AGENT_ID),
            "Hosted::session_id must not be overwritten with conversation session_id"
        );
        // Mirror SessionPointer::sync_session writing adapter.session_id() to disk.
        let persisted = adapter1.session_id().unwrap();
        let ptr_path = dir.path().join("spiron-cloud-session-id");
        std::fs::write(&ptr_path, format!("{persisted}\n")).unwrap();
        adapter1.stop_verified().unwrap();

        // Restart: pass persisted pointer as `session` (matching vendor.rs plan_adapter).
        let resume_id = std::fs::read_to_string(&ptr_path).unwrap();
        let mut adapter2 = start_remote_session(
            "spiron",
            dir.path(),
            "agt_resume_1",
            "resume-worker",
            Some("z-ai/glm-5.3-flash"),
            Some(resume_id.trim()),
            Some(500_000),
            &env,
        )
        .expect("restart session on persisted remote agent_id");
        assert_eq!(adapter2.session_id().as_deref(), Some(AGENT_ID));

        assert_eq!(
            adapter2.deliver(&msg(REQ_2, 2, "turn 2")).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );
        wait_until(Duration::from_secs(3), || {
            adapter2.proven(REQ_2)
                == Some(Proof::Taken {
                    turn_finished: true,
                })
        });
        assert_eq!(adapter2.reply_for(REQ_2).as_deref(), Some("turn 2 resumed"));
        assert_eq!(
            create_count.load(Ordering::SeqCst),
            1,
            "must not create a second agent"
        );
        assert_eq!(
            *msg_agents.lock().unwrap(),
            vec![AGENT_ID.to_string(), AGENT_ID.to_string()]
        );
        adapter2.stop_verified().unwrap();
    }

    #[test]
    fn non_spiron_cloud_harness_fails_closed_with_migration_guidance() {
        let dir = crate::test_tempdir();
        for harness in [
            "claude-code",
            "claude-cloud",
            "codex",
            "openai-cloud",
            "cursor",
            "cursor-cloud",
        ] {
            let Err(err) =
                start_remote_session(harness, dir.path(), "agt_x", "w", None, None, None, &[])
            else {
                panic!("expected fail-closed error for `{harness}`");
            };
            assert!(
                err.contains("ADR-0010 D7")
                    && err.contains("spiron@cloud")
                    && err.contains("runtime=sandbox")
                    && err.contains("GenericHttpCloudDriver"),
                "{harness}: {err}"
            );
        }
    }
}
