use serde_json::{Value, json};

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_list_and_read_use_only_public_scope_and_bound_the_mcp_frame() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let create = |name: &str, scope: &str| {
        let agent = state
            .store
            .create_agent(anyagents_core::NewAgent::new(name, "spiron-cloud"))
            .unwrap();
        state.store.set_agent_scope(&agent.id, scope).unwrap()
    };
    let caller = create("caller", "private");
    let isolated = create("isolated", "isolated");
    let public = create("visible", "public");
    let second = create("second-public", "public");
    let private = create("hidden", "private");
    let sibling_only = create("sibling-only", "siblings");
    let mut child = anyagents_core::NewAgent::new("public-child", "spiron-cloud");
    child.parent_id = Some(caller.id.clone());
    let child = state.store.create_agent(child).unwrap();
    let child = state.store.set_agent_scope(&child.id, "public").unwrap();

    let seed = |agent: &Agent, topic: &str, raw: &str| {
        let path = state
            .store
            .data_dir()
            .join("agents")
            .join(agent.id.as_str())
            .join("memory");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(format!("{topic}.md")), raw).unwrap();
        path
    };
    let path = seed(
        &public,
        "anchor",
        "---\npinned: true\nupdated_at_ms: 1\ndescription: durable\n---\nVisible content\n",
    );
    for i in 0..205 {
        seed(
            &public,
            &format!("event-{i:03}"),
            &format!(
                "---\ndescription: Audit {}\nupdated_at_ms: {}\n---\nFull body never belongs in list.\n",
                "\\".repeat(175),
                i + 2,
            ),
        );
    }
    seed(&caller, "own", "Self-only fixture");
    seed(&isolated, "only-mine", "Isolated self fixture");
    seed(&private, "private-note", "Not visible to peer");
    seed(&sibling_only, "sibling-note", "Not public");
    seed(
        &child,
        "child-note",
        "Cannot be modified by parent via memory",
    );
    #[cfg(unix)]
    std::os::unix::fs::symlink(path.join("anchor.md"), path.join("link.md")).unwrap();

    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    crate::remote::begin_session(&state.store, &caller).unwrap();
    let remote = crate::remote::session_token(&anyagents_core::agent_config_dir(
        state.store.data_dir(),
        &caller.id,
    ))
    .unwrap();
    let caller_cap = state.store.issue_capability(&caller.id).unwrap();
    let isolated_cap = state.store.issue_capability(&isolated.id).unwrap();
    let socket = dir.path().join("memory.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let app = build_router(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let get = |path: String, token: String| {
        let socket = socket.clone();
        async move { http(&socket, "GET", &path, None, Some(&token), None).await }
    };
    let mcp = |args: Value| {
        let app = build_router(state.clone());
        let remote = remote.clone();
        async move {
            let mut req = axum::http::Request::builder().method("POST").uri("/mcp");
            req = req.header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {remote}"),
            );
            let body = json!({"jsonrpc":"2.0", "id":1, "method":"tools/call", "params":{"name":"memory", "arguments":args}});
            // Use the real HTTP router with a Bearer token; the raw response
            // length includes both structuredContent and the text copy.
            use http_body_util::BodyExt as _;
            use tower::ServiceExt as _;
            let resp = app
                .oneshot(req.body(axum::body::Body::from(body.to_string())).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            let doc: Value = serde_json::from_slice(&bytes).unwrap();
            (bytes.len(), doc["result"].clone())
        }
    };

    let (size, reply) = mcp(json!({"action":"list", "agent":public.id.as_str()})).await;
    assert!(size < 8_000, "MCP list returned {size} bytes");
    let first = &reply["structuredContent"];
    assert_eq!(first["total_count"], 206, "{reply}");
    assert_eq!(first["topics"][0]["topic"], "anchor");
    assert_eq!(first["topics"][0]["pinned"], true);
    assert_eq!(first["topics"][1]["topic"], "event-204");
    assert_eq!(first["topics"][1]["updated_at"], 206);
    assert!(
        first["topics"][1]["description"]
            .as_str()
            .unwrap()
            .chars()
            .count()
            <= 120
    );
    assert_eq!(first["has_more"], true);
    assert!(!first.to_string().contains("Full body"));
    let offset = first["next_offset"].as_u64().unwrap();
    assert_eq!(first["count"], offset);
    let (_, next) =
        mcp(json!({"action":"list", "agent":"visible", "offset":offset, "limit":1})).await;
    let (_, raw_next) = get(
        format!("/v1/agents/visible/memory?limit=1&offset={offset}"),
        caller_cap.clone(),
    )
    .await;
    assert_eq!(next["structuredContent"], raw_next);
    assert_ne!(
        first["topics"][offset as usize - 1]["topic"],
        raw_next["topics"][0]["topic"]
    );
    let (_, filtered) = mcp(json!({"action":"list", "agent":"visible", "query":"AUDIT", "prefix":"EVENT-00", "limit":1000})).await;
    assert_eq!(filtered["structuredContent"]["total_count"], 10);
    let (_, read) = mcp(json!({"topic":"anchor", "agent":"visible"})).await;
    assert!(
        read["structuredContent"]["content"]
            .as_str()
            .unwrap()
            .contains("Visible content")
    );

    let (_, shared) = mcp(json!({"scope":"shared", "prefix":"anchor"})).await;
    assert_eq!(shared["structuredContent"]["total_count"], 1);
    assert_eq!(
        shared["structuredContent"]["topics"][0]["agent_id"],
        public.id.as_str()
    );
    let (_, private_search) =
        mcp(json!({"action":"list", "scope":"shared", "prefix":"private"})).await;
    assert_eq!(private_search["structuredContent"]["total_count"], 0);
    let (_, shared_read) = mcp(json!({"scope":"shared", "topic":"anchor"})).await;
    assert_eq!(shared_read["structuredContent"]["agent"], "visible");
    seed(&second, "anchor", "Duplicate public slug");
    assert_eq!(
        get(
            "/v1/agents/self/memory?scope=shared&topic=anchor".into(),
            caller_cap.clone()
        )
        .await
        .0,
        StatusCode::CONFLICT
    );

    for name in ["hidden", "sibling-only", "nonexistent"] {
        for suffix in ["", "?topic=private-note"] {
            let (status, _) = get(
                format!("/v1/agents/{name}/memory{suffix}"),
                caller_cap.clone(),
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{name}{suffix}");
        }
    }
    for path in [
        "/v1/agents/visible/memory",
        "/v1/agents/visible/memory?topic=anchor",
        "/v1/agents/self/memory?scope=shared",
    ] {
        assert_eq!(
            get(path.into(), isolated_cap.clone()).await.0,
            StatusCode::FORBIDDEN,
            "{path}"
        );
    }
    let (_, own) = get("/v1/agents/self/memory".into(), isolated_cap).await;
    assert_eq!(own["topics"][0]["topic"], "only-mine");
    let (_, own) = mcp(json!({})).await;
    assert_eq!(own["structuredContent"]["topics"][0]["topic"], "own");
    assert_eq!(
        get(
            "/v1/agents/visible/memory?topic=link".into(),
            caller_cap.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(
            "/v1/agents/visible/conversation?scope=transcripts".into(),
            caller_cap.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get(
            "/v1/agents/visible/conversation?topic=anchor".into(),
            caller_cap.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    for (target, topic) in [
        (public.name.as_str(), "anchor"),
        (child.id.as_str(), "child-note"),
    ] {
        for mutation in ["content=denied", "delete=true"] {
            let route = format!("/v1/agents/{target}/conversation?topic={topic}&{mutation}");
            assert_eq!(
                get(route, caller_cap.clone()).await.0,
                StatusCode::FORBIDDEN
            );
        }
    }
    for args in [
        json!({"agent":"visible", "topic":"anchor", "content":"denied"}),
        json!({"scope":"shared", "topic":"anchor", "delete":true}),
    ] {
        let (_, reply) = mcp(args).await;
        assert_eq!(reply["isError"], true, "{reply}");
        assert!(reply.to_string().contains("403 Forbidden"), "{reply}");
    }
    let (_, reply) = mcp(json!({"topic":"own", "content":"allowed self write"})).await;
    assert_ne!(reply["isError"], true, "{reply}");
    let (_, reply) = mcp(json!({"topic":"own", "delete":true})).await;
    assert_eq!(reply["structuredContent"]["deleted"], true, "{reply}");
    assert!(
        std::fs::read_to_string(path.join("anchor.md"))
            .unwrap()
            .contains("Visible content")
    );

    state.store.set_agent_scope(&public.id, "private").unwrap();
    assert_eq!(
        get("/v1/agents/visible/memory?topic=anchor".into(), caller_cap)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    server.abort();
}
