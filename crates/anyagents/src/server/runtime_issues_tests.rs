use super::*;

#[tokio::test]
async fn automatic_runtime_issue_capture_is_deduplicated() {
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    assert!(missing_guest_harness(Some(
        "failed to start guest harness fixture: No such file or directory (os error 2)"
    )));
    assert!(!missing_guest_harness(Some(
        "task command failed: (os error 2)"
    )));
    assert!(!missing_guest_harness(Some(
        "failed to start guest harness fixture: (os error 20)"
    )));
    for _ in 0..2 {
        capture(
            Arc::clone(&store),
            None,
            RuntimeComponent::Harness,
            "Harness launch failed",
            "Executable missing",
        );
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let rows = store.runtime_issues(None, None).unwrap();
            if let [issue] = &rows[..]
                && issue.occurrence_count == 2
            {
                assert_eq!(issue.source, RuntimeIssueSource::AutoCapture);
                assert_eq!(issue.component, RuntimeComponent::Harness);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn runtime_issue_mcp_uniform_at_every_depth_and_admin_only() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, header};
    use serde_json::{Value, json};
    use tower::ServiceExt as _;

    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-runtime-issues")
        .unwrap();
    let admin_token = "fixture-admin-token";
    let state = AppState {
        store: Arc::clone(&store),
        health: Default::default(),
        sign_in: Arc::new(std::sync::Mutex::new(crate::server::types::ProcessRunner)),
        identity: Arc::new(crate::server::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        auth: crate::auth::Mode::Confined {
            operator_digest: Some(crate::auth::operator_digest(admin_token)),
        },
    };
    let app = crate::server::build_router(state.clone());
    let invoke = |method, path, token: Option<&str>, body: Value| {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            request = if token == admin_token {
                request.header(crate::operator::HEADER, token)
            } else if path == "/mcp" {
                request.header(header::AUTHORIZATION, ["Bearer ", token].concat())
            } else {
                request.header(crate::operator::AGENT_HEADER, token)
            };
        }
        let req = request
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let app = app.clone();
        async move {
            let res = app.oneshot(req).await.unwrap();
            let status = res.status();
            let bytes = to_bytes(res.into_body(), 1_048_576).await.unwrap();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        }
    };
    let call = |name, args| json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": args}});
    let report_args = json!({"component": "harness", "summary": "Fixture harness failed", "exact_error": "Runtime fixture: No such file", "repro_steps": "Start the fixture runner"});

    let mut parent = None;
    let mut first = None;
    let mut last_cap = None;
    for name in ["root", "child", "grandchild"] {
        let mut new = anyagents_core::NewAgent::new(name, "claude-cloud");
        new.parent_id = parent.clone();
        let agent = store.create_agent(new).unwrap();
        let grant = store.begin_remote_session(&agent.id, 60_000).unwrap();
        let (status, list) = invoke(
            "POST",
            "/mcp",
            Some(&grant.token),
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        let tools = list["result"]["tools"].as_array().unwrap();
        let send = tools.iter().find(|tool| tool["name"] == "send").unwrap();
        assert!(!send.to_string().contains("operator"));
        let bug = tools
            .iter()
            .find(|tool| tool["name"] == "report_bug")
            .unwrap();
        assert!(
            bug["description"]
                .as_str()
                .unwrap()
                .contains("NEVER call for task blockers")
        );
        assert_eq!(
            bug["inputSchema"]["required"],
            json!(["component", "summary", "exact_error", "repro_steps"])
        );
        for to in [json!(["operator"]), json!(["parent", "agent:operator"])] {
            let (status, refusal) = invoke(
                "POST",
                "/mcp",
                Some(&grant.token),
                call("send", json!({"to": to, "text": "Must not deliver"})),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(refusal["result"]["isError"], true, "{refusal}");
            assert!(
                refusal["result"]["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("report_bug")
            );
        }
        let (status, receipt) = invoke(
            "POST",
            "/mcp",
            Some(&grant.token),
            call("report_bug", report_args.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let result = &receipt["result"]["structuredContent"];
        let id = result["issue_id"]
            .as_str()
            .unwrap_or_else(|| panic!("missing issue_id: {receipt}"));
        assert_eq!(result, &json!({"issue_id": id}));
        if let Some((expected_id, _)) = &first {
            assert_eq!(id, expected_id);
        } else {
            first = Some((id.to_owned(), agent.id.clone()));
        }
        assert!(store.list_undelivered(&agent.id, 10).unwrap().is_empty());
        if let Some(parent_id) = &parent {
            assert!(store.list_undelivered(parent_id, 10).unwrap().is_empty());
        }
        parent = Some(agent.id.clone());
        last_cap = Some(store.issue_capability(&agent.id).unwrap());
    }
    assert!(store.operator_inbox(None, None).unwrap().is_empty());
    let cap = last_cap.as_deref().unwrap();
    let (status, _) = invoke("GET", "/admin/api/runtime-issues", None, Value::Null).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = invoke("GET", "/admin/api/runtime-issues", Some(cap), Value::Null).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, rows) = invoke(
        "GET",
        "/admin/api/runtime-issues?status=open&component=harness",
        Some(admin_token),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    let (id, agent) = first.unwrap();
    let [issue] = rows["issues"].as_array().unwrap().as_slice() else {
        panic!("expected one deduplicated issue: {rows}")
    };
    assert_eq!(issue["id"], id);
    assert_eq!(issue["sample_agent_id"], agent.as_str());
    assert_eq!(issue["occurrence_count"], 3);
    assert_eq!(issue["source"], "agent_report");
    let (status, rows) = invoke(
        "GET",
        "/admin/api/runtime-issues?status=resolved",
        Some(admin_token),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows, json!({"issues": []}));

    // The existing mailbox HTTP contract is independent from MCP.
    let (status, body) = invoke(
        "POST",
        "/v1/send",
        Some(cap),
        json!({"to": ["operator"], "text": "Legacy HTTP mail"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, result) =
        invoke("GET", "/v1/operator/inbox", Some(admin_token), Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert!(result.to_string().contains("Legacy HTTP mail"));
}
