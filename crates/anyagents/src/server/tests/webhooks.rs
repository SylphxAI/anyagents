use super::super::webhooks_mcp::{MCP_PAYLOAD_ENCODING_HEADER, decode_mcp_request_payload};
use super::super::*;
use super::helpers::{archetype_validation_state, env_lock, listed_tools, raw_http, tools_list};
use axum::http::StatusCode;

/// An admitted HTTP MCP call runs against the server's in-process router
/// instead of dialing `anyagents.sock`: when `stopping` has already closed
/// the external socket listener (leaving a refusing socket file on disk),
/// an in-flight `POST /v1/webhooks/mcp` finishes at once instead of
/// self-deadlocking the mutation drain on `ECONNREFUSED`.
#[test]
fn http_mcp_post_completes_in_process_while_external_socket_refuses() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new("cloud-drain", "claude-cloud"))
        .unwrap();
    let peer = state
        .store
        .create_agent(anyagents_core::NewAgent::new("peer-drain", "claude-code"))
        .unwrap();
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let cloud_cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud.id);
    let remote = crate::remote::session_token(&cloud_cfg).unwrap();

    // Simulate shutdown after `stopping`: `anyagents.sock` stays on disk
    // while its external listener is closed (`ECONNREFUSED`).
    let sock = socket_path(dir.path());
    drop(std::os::unix::net::UnixListener::bind(&sock).unwrap());
    assert!(sock.exists());

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use http_body_util::BodyExt as _;
        use hyper::service::Service as _;
        let public = build_public_router(state.clone());
        let service = hyper_util::service::TowerToHyperService::new(public);
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "send",
                "arguments": {
                    "to": ["peer-drain"],
                    "text": "draining cleanly"
                }
            }
        });
        let req = hyper::Request::builder()
            .method("POST")
            .uri("http://localhost/v1/webhooks/mcp")
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {remote}"),
            )
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(http_body_util::Full::new(bytes::Bytes::from(
                serde_json::to_vec(&payload).unwrap(),
            )))
            .unwrap();
        let started = std::time::Instant::now();
        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "HTTP MCP call stalled on closed external socket: {:?}",
            started.elapsed()
        );
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_ne!(body["result"]["isError"], serde_json::json!(true), "{body}");
        let messages = state.store.inbox(&peer.id, None).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].text, "draining cleanly");
    });
}

#[test]
fn event_webhook_routes_authenticate_original_bytes() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let watcher = state
        .store
        .create_agent(anyagents_core::NewAgent::new("deploy-bot", "codex"))
        .unwrap();
    state.store.set_agent_running(&watcher.id, "/work").unwrap();
    crate::schedule::create_agent_schedule(
        &state.store,
        &state.health,
        &watcher,
        None,
        None,
        Some("webhook:deploy/prod"),
        "verify deployment",
    )
    .unwrap();
    let config = state.store.data_dir().join("anyagents.toml");
    std::fs::write(&config, "[server]\nwebhook_secret = 'secret'\n").unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use hyper::service::Service as _;
        for router in [
            build_router(state.clone()),
            build_public_router(state.clone()),
            build_webhook_router(state.clone()),
        ] {
            let service = hyper_util::service::TowerToHyperService::new(router);
            for (body, signed, expected) in [
                (r#"{"release":"v2"}"#, true, StatusCode::ACCEPTED),
                ("{", false, StatusCode::UNAUTHORIZED),
                ("{", true, StatusCode::BAD_REQUEST),
            ] {
                let signature = if signed {
                    format!(
                        "sha256={}",
                        anyagents_core::hmac_sha256_hex(b"secret", body.as_bytes())
                    )
                } else {
                    "sha256=invalid".into()
                };
                let request = hyper::Request::builder()
                    .method("POST")
                    .uri("/v1/webhooks/events/deploy/prod")
                    .header("x-anyagents-signature-256", signature)
                    .header("x-anyagents-delivery", format!("route-{}", body.len()))
                    .body(http_body_util::Full::new(bytes::Bytes::copy_from_slice(
                        body.as_bytes(),
                    )))
                    .unwrap();
                assert_eq!(service.call(request).await.unwrap().status(), expected);
            }
        }
        assert_eq!(state.store.inbox(&watcher.id, None).unwrap().len(), 1);
        std::fs::write(&config, "[server]\nwebhook_secret = ''\n").unwrap();
        let service = hyper_util::service::TowerToHyperService::new(build_webhook_router(state));
        let request = hyper::Request::builder()
            .method("POST")
            .uri("/v1/webhooks/events/deploy/prod")
            .body(http_body_util::Full::new(bytes::Bytes::new()))
            .unwrap();
        assert_eq!(
            service.call(request).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    });
}

#[test]
fn webhook_router_serves_mcp_profile_full_for_sandboxed_only_and_gateway_prefix() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.test\"\n",
    )
    .unwrap();

    let sbx_agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("sbx-codex", "codex"))
        .unwrap();
    let sbx_cfg = anyagents_core::agent_config_dir(dir.path(), &sbx_agent.id);
    std::fs::create_dir_all(&sbx_cfg).unwrap();
    crate::sandbox_providers::sylphx::SylphxLeaseConfig {
        scoped: false,
        endpoint: "https://sandbox.example.test".into(),
        token: None,
        parent: "orgs/o1/projects/p1/envs/e1".into(),
        template: "agent".into(),
        shape: "linux-m".into(),
        volume_size_gib: Some(10),
        mount_path: "/home/user/agent-state".into(),
        workdir: "/home/user/workspace".into(),
        volume_id: "vol_1".into(),
        volume_resource: "orgs/o1/projects/p1/envs/e1/volumes/vol_1".into(),
        lease_id: Some("lease_1".into()),
        lease_resource: Some("orgs/o1/projects/p1/envs/e1/leases/lease_1".into()),
        data_plane_resource: Some("orgs/o1/projects/p1/envs/e1/leases/lease_1".into()),
        envd_url: None,
        envd_token: None,
        expires_at: None,
    }
    .save_to_config_dir(&sbx_cfg)
    .unwrap();
    crate::remote::begin_session(&state.store, &sbx_agent).unwrap();
    let sbx_remote = crate::remote::session_token(&sbx_cfg).unwrap();

    let cloud_agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("cloud-only", "claude-cloud"))
        .unwrap();
    let cloud_cfg = anyagents_core::agent_config_dir(dir.path(), &cloud_agent.id);
    std::fs::create_dir_all(&cloud_cfg).unwrap();
    crate::remote::begin_session(&state.store, &cloud_agent).unwrap();
    let cloud_remote = crate::remote::session_token(&cloud_cfg).unwrap();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // Parse and consume the complete request before replying. TCP may split
        // headers or the body across reads, even on loopback.
        let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_app = axum::Router::new().route(
            "/v1/responses",
            axum::routing::post(
                |headers: axum::http::HeaderMap,
                 axum::Json(payload): axum::Json<serde_json::Value>| async move {
                    if headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        != Some("Bearer upstream-secret-token")
                    {
                        return Err(StatusCode::UNAUTHORIZED);
                    }
                    if payload["model"] != "gpt-test" || payload["input"] != "hi" {
                        return Err(StatusCode::BAD_REQUEST);
                    }
                    Ok(axum::Json(serde_json::json!({
                        "id": "resp_1", "object": "response", "status": "completed"
                    })))
                },
            ),
        );
        let upstream_task = tokio::spawn(async move {
            axum::serve(upstream, upstream_app).await.unwrap();
        });
        crate::gateway::ProviderImage::store(
            dir.path(),
            &sbx_agent,
            &format!("http://{upstream_addr}"),
            "gpt-test",
            "upstream-secret-token",
        )
        .unwrap();
        let gw_token = crate::gateway::add_route(dir.path(), &sbx_agent)
            .unwrap()
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_webhook_router(state.clone());
        let served = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let list = tools_list().to_string();

        // 1) Both sandboxed and cloud agents receive the identical universal MCP surface
        // on `/mcp`, `/v1/webhooks/mcp`, and legacy `?profile=...` URLs.
        for (tok, path) in [
            (sbx_remote.clone(), "/mcp"),
            (sbx_remote.clone(), "/v1/webhooks/mcp"),
            (sbx_remote.clone(), "/mcp?profile=full"),
            (cloud_remote.clone(), "/mcp?profile=full"),
        ] {
            let list_body = list.clone();
            let (status, body) = tokio::task::spawn_blocking(move || {
                let hdrs = [("authorization", format!("Bearer {tok}"))];
                let refs: Vec<(&str, &str)> = hdrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
                raw_http(addr, "POST", path, &refs, &list_body)
            })
            .await
            .unwrap();
            assert_eq!(status, 200, "{path}: {body}");
            assert_eq!(
                listed_tools(&body),
                crate::mcp::UNIVERSAL_MCP_TOOLS,
                "{path}"
            );
        }

        // 3) Both `/v1/responses` and `/gateway/v1/responses` work on the webhook router.
        for path in ["/gateway/v1/responses", "/v1/responses"] {
            let tok = gw_token.clone();
            let (status, body) = tokio::task::spawn_blocking(move || {
                let hdrs = [("authorization", format!("Bearer {tok}"))];
                let refs: Vec<(&str, &str)> = hdrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
                raw_http(
                    addr,
                    "POST",
                    path,
                    &refs,
                    r#"{"model":"gpt-test","input":"hi"}"#,
                )
            })
            .await
            .unwrap();
            assert_eq!(status, 200, "{path}: {body}");
            assert_eq!(body["id"], "resp_1", "{path}: {body}");
        }

        served.abort();
        upstream_task.abort();
    });
}

#[test]
fn decode_mcp_request_payload_supports_plain_json_and_base64_waf_framing() {
    use base64::Engine as _;

    let raw = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "send",
            "arguments": { "message": "ls -la | grep foo && cat /etc/hosts; SELECT * FROM t" }
        }
    });
    let raw_bytes = serde_json::to_vec(&raw).unwrap();

    // Plain JSON works without any encoding header.
    let plain_headers = axum::http::HeaderMap::new();
    let decoded_plain =
        decode_mcp_request_payload(&plain_headers, axum::body::Bytes::from(raw_bytes.clone()))
            .unwrap();
    assert_eq!(decoded_plain, raw);

    // Opaque base64 framing decodes to the identical JSON-RPC value.
    let mut b64_headers = axum::http::HeaderMap::new();
    b64_headers.insert(
        axum::http::header::HeaderName::from_static(MCP_PAYLOAD_ENCODING_HEADER),
        axum::http::HeaderValue::from_static("base64"),
    );
    let b64_body = base64::engine::general_purpose::STANDARD.encode(&raw_bytes);
    let decoded_b64 =
        decode_mcp_request_payload(&b64_headers, axum::body::Bytes::from(b64_body)).unwrap();
    assert_eq!(decoded_b64, raw);
}
