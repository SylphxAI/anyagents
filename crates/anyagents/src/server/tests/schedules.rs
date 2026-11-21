use super::super::schedules::{
    ScheduleRequest, authorize_schedule_target, handle_schedule_action, list_fleet_schedules,
    list_schedules_route,
};
use super::super::*;
use super::helpers::{archetype_validation_state, tool_call, tools_list};
use axum::http::StatusCode;

#[tokio::test]
async fn fleet_schedules_enforce_owner_authority_and_reject_all_mutations() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let store = &state.store;
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("fleet-parent", "codex"))
        .unwrap();
    let child = store
        .create_agent(anyagents_core::NewAgent {
            parent_id: Some(parent.id.clone()),
            ..anyagents_core::NewAgent::new("fleet-child", "codex")
        })
        .unwrap();
    let unrelated = store
        .create_agent(anyagents_core::NewAgent::new("fleet-unrelated", "codex"))
        .unwrap();
    let unrelated_private = store
        .create_agent(anyagents_core::NewAgent {
            parent_id: Some(unrelated.id.clone()),
            ..anyagents_core::NewAgent::new("fleet-unrelated-private", "codex")
        })
        .unwrap();
    for agent in [&parent, &child, &unrelated, &unrelated_private] {
        store
            .create_schedule(anyagents_core::NewSchedule {
                agent_id: agent.id.clone(),
                kind: "watch".into(),
                spec: "file:/tmp/fleet-fixture".into(),
                prompt: format!("private prompt for {}", agent.name),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
    }
    let principal = Principal::Agent {
        agent_id: parent.id.clone(),
        name: parent.name.clone(),
    };
    let child_principal = Principal::Agent {
        agent_id: child.id.clone(),
        name: child.name.clone(),
    };
    assert_eq!(
        authorize_schedule_target(store, &principal, Some(unrelated.id.as_str()))
            .unwrap_err()
            .status,
        StatusCode::FORBIDDEN
    );
    // Parent & child see self + descendants + scope=="public" peers (parent, unrelated),
    // never another peer's scope=="private" sub-agent (`unrelated_private`). Operator sees all 4.
    for (actor, expected) in [
        (&principal, 3),
        (&child_principal, 3),
        (&Principal::Operator, 4),
    ] {
        for target in ["all", "ALL", " agent:All "] {
            let request: ScheduleRequest =
                serde_json::from_value(serde_json::json!({"action": "list", "agent": target}))
                    .unwrap();
            let result = handle_schedule_action(actor.clone(), state.clone(), request)
                .await
                .unwrap()
                .0;
            let rows = result["schedules"].as_array().unwrap();
            assert_eq!(rows.len(), expected);
            for row in rows {
                assert!(row["agent_name"].is_string());
                assert!(row["agent_id"].is_string());
                assert!(row.get("next_fire_at_ms").unwrap().is_null());
                assert!(row.get("last_observed").unwrap().is_null());
                if expected < 4 {
                    assert_ne!(row["agent_name"], unrelated_private.name);
                }
            }
        }
    }
    // Both HTTP GET and the MCP-backed action share the same authorized population.
    let get = list_schedules_route(
        principal.clone(),
        State(state.clone()),
        AxumPath("all".into()),
    )
    .await
    .unwrap()
    .0;
    assert_eq!(get["schedules"].as_array().unwrap().len(), 3);
    for actor in [&principal, &Principal::Operator] {
        for payload in [
            serde_json::json!({"action": "create", "at": "1h", "prompt": "no"}),
            serde_json::json!({"action": "at", "at": "1h", "prompt": "no"}),
            serde_json::json!({"action": "every", "every": "1h", "prompt": "no"}),
            serde_json::json!({"action": "watch", "watch": "file:/tmp/no", "prompt": "no"}),
            serde_json::json!({"action": "cancel", "id": "all"}),
            serde_json::json!({"at": "1h", "prompt": "no"}),
        ] {
            let mut payload = payload;
            payload["agent"] = serde_json::json!("all");
            let request = serde_json::from_value(payload).unwrap();
            assert_eq!(
                handle_schedule_action(actor.clone(), state.clone(), request)
                    .await
                    .unwrap_err()
                    .status,
                StatusCode::BAD_REQUEST
            );
        }
    }
    assert_eq!(store.list_schedules(None, true).unwrap().len(), 4);
    assert!(authorize_schedule_target(store, &Principal::Operator, None).is_err());
    // Child sees self + public roots (3), parent sees self + child + public peer (3), operator sees all (4).
    assert_eq!(
        list_fleet_schedules(store, &child_principal).unwrap()["schedules"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        list_fleet_schedules(store, &Principal::Operator).unwrap()["schedules"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        list_fleet_schedules(store, &principal).unwrap()["schedules"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    // An unreadable caller is an error, not a fleet-wide visibility grant.
    let absent = Principal::Agent {
        agent_id: anyagents_core::AgentId::generate(),
        name: "missing".into(),
    };
    assert!(list_fleet_schedules(store, &absent).is_err());
}

#[tokio::test]
async fn persistent_webhook_schedules_through_http_and_mcp() {
    use serde_json::{Value, json};

    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let watcher = state
        .store
        .create_agent(anyagents_core::NewAgent::new("watcher", "claude-cloud"))
        .unwrap();
    let remote = state
        .store
        .begin_remote_session(&watcher.id, 60_000)
        .unwrap()
        .token;
    let capability = state.store.issue_capability(&watcher.id).unwrap();
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\nwebhook_secret = 'test-event-secret'\n",
    )
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = build_router(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let send = |path: &str, auth: (&str, &str), delivery_id: Option<&str>, body: Value| {
        use http_body_util::BodyExt as _;
        let mut request = hyper::Request::post(path)
            .header("content-type", "application/json")
            .header(auth.0, auth.1);
        if let Some(id) = delivery_id {
            request = request.header("x-anyagents-delivery", id);
        }
        let request = request
            .body(http_body_util::Full::new(bytes::Bytes::from(
                body.to_string(),
            )))
            .unwrap();
        async move {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (mut client, connection) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                    .await
                    .unwrap();
            tokio::spawn(connection);
            let response = client.send_request(request).await.unwrap();
            let status = response.status();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            (
                status,
                serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null),
            )
        }
    };
    let post_schedule = |body| {
        send(
            "/v1/schedules",
            (crate::operator::AGENT_HEADER, &capability),
            None,
            body,
        )
    };
    let mcp_auth = format!("Bearer {remote}");
    let call_mcp = |body| async {
        let (status, body) = send("/v1/mcp", ("authorization", &mcp_auth), None, body).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    };
    let event = |id, value| {
        send(
            "/v1/webhooks/events/sample",
            ("authorization", "Bearer test-event-secret"),
            id,
            json!({"value": value}),
        )
    };
    let accepted = |id, value| async move {
        let (status, body) = event(Some(id), value).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        body["matched"].clone()
    };

    for trigger in [
        json!({"at": "5m"}),
        json!({"every": "5m"}),
        json!({"watch": "agent:idle:self"}),
    ] {
        let mut body = trigger;
        body["persistent"] = json!(true);
        body["prompt"] = json!("invalid watch");
        let (status, body) = post_schedule(body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body.to_string()
                .contains("only supported for webhook/event watches"),
            "{body}"
        );
    }

    let (status, response) =
        post_schedule(json!({"watch":"webhook:sample", "prompt":"inspect sample"})).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["schedule"]["persistent"], false);
    assert_eq!(accepted("one-shot-first", 1).await, 1);
    assert_eq!(accepted("one-shot-second", 2).await, 0);
    let one_shot = response["schedule"]["id"].as_str().unwrap();
    assert_eq!(
        state.store.get_schedule(one_shot).unwrap().unwrap().state,
        "fired"
    );

    let tools = call_mcp(tools_list()).await;
    let schedules = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "schedules")
        .unwrap();
    assert!(schedules["inputSchema"]["properties"]["persistent"].is_object());
    assert!(
        schedules["description"]
            .as_str()
            .unwrap()
            .contains("standing webhook/event subscription add `persistent: true`")
    );
    let created = call_mcp(tool_call("schedules", json!({"action":"watch", "watch":"event:sample", "prompt":"inspect sample", "persistent":true}))).await;
    let subscription = &created["result"]["structuredContent"]["schedule"];
    assert_eq!(subscription["persistent"], true, "{created}");
    assert_eq!(subscription["kind"], "watch");
    assert_eq!(subscription["spec"], "event:sample");
    let id = subscription["id"].as_str().unwrap();
    assert_eq!(accepted("first", 3).await, 1);
    let first = state.store.get_schedule(id).unwrap().unwrap();
    assert_eq!(first.state, "active");
    assert!(first.last_fired_at_ms.is_some());
    assert_eq!(first.last_observed.as_deref(), Some("event:sample:first"));
    assert_eq!(accepted("second", 4).await, 1);
    assert_eq!(accepted("first", 9).await, 1);
    let inbox = state.store.inbox(&watcher.id, None).unwrap();
    assert_eq!(inbox.len(), 3);
    assert!(inbox[1].text.contains("\"value\": 3"));
    assert!(inbox[2].text.contains("\"value\": 4"));
    for message in &inbox {
        state
            .store
            .note_delivered(&watcher.id, message.seq)
            .unwrap();
    }
    assert_eq!(accepted("second", 9).await, 1);
    assert_eq!(state.store.inbox(&watcher.id, None).unwrap().len(), 3);
    assert_eq!(
        state
            .store
            .get_schedule(id)
            .unwrap()
            .unwrap()
            .last_observed
            .as_deref(),
        Some("event:sample:second")
    );

    // A temporarily busy owner returns a retryable response, not silent success.
    let lifecycle = state.store.try_lock_agent_lifecycle(&watcher.id).unwrap();
    assert_eq!(
        event(Some("retryable"), 5).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(lifecycle);
    assert_eq!(accepted("retryable", 5).await, 1);
    for _ in 0..2 {
        assert_eq!(event(None, 6).await.0, StatusCode::ACCEPTED);
    }
    assert_eq!(state.store.inbox(&watcher.id, None).unwrap().len(), 6);

    let list = call_mcp(tool_call("schedules", json!({"action":"list"}))).await;
    let rows = &list["result"]["structuredContent"]["schedules"];
    assert_eq!(rows[0]["id"], id, "{list}");
    assert_eq!(rows[0]["state"], "active");
    assert_eq!(rows[0]["persistent"], true);
    let cancel = call_mcp(tool_call("schedules", json!({"action":"cancel", "id":id}))).await;
    assert_eq!(
        cancel["result"]["structuredContent"]["cancelled"], 1,
        "{cancel}"
    );
    assert_eq!(accepted("after-cancel", 7).await, 0);
    assert_eq!(state.store.inbox(&watcher.id, None).unwrap().len(), 6);
    server.abort();
}

#[tokio::test]
async fn cloud_file_watch_is_rejected_without_creating_a_schedule() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("cloud-watch", "claude-cloud"))
        .unwrap();
    let principal = Principal::Agent {
        agent_id: agent.id.clone(),
        name: agent.name,
    };
    let request = serde_json::from_value(serde_json::json!({
        "action": "create", "watch": "file:signal", "prompt": "report the change"
    }))
    .unwrap();
    let error = handle_schedule_action(principal, state.clone(), request)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("no persistent filesystem"));
    assert!(error.message.contains("event/webhook"));
    assert!(
        state
            .store
            .list_schedules(Some(&agent.id), false)
            .unwrap()
            .is_empty()
    );
}
