use super::super::webhooks_mcp::{mcp_get_route, mcp_post_route};
use super::super::*;
use super::helpers::{
    archetype_validation_state, env_lock, listed_tools, post_mcp_as, tool_call, tools_list,
};
use anyagents_core::Agent;
use axum::http::StatusCode;

#[test]
fn streamable_http_mcp_enforces_bearer_auth_and_three_mode_profiles() {
    use http_body_util::BodyExt as _;

    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let cloud_agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "cloud-worker",
            "claude-cloud",
        ))
        .unwrap();
    let spiron_agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "spiron-worker",
            "spiron-cloud",
        ))
        .unwrap();
    // HTTP MCP takes remote session tokens only; a local capability is
    // refused below.
    let remote_token = |agent: &Agent| {
        crate::remote::begin_session(&state.store, agent).unwrap();
        crate::remote::session_token(&anyagents_core::agent_config_dir(
            state.store.data_dir(),
            &agent.id,
        ))
        .unwrap()
    };
    let cloud_cap = remote_token(&cloud_agent);
    let spiron_cap = remote_token(&spiron_agent);
    let local_cap = state.store.issue_capability(&cloud_agent.id).unwrap();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // Verify both `build_router` and `build_webhook_router` compile and bind `/mcp`.
        let _main_app = build_router(state.clone());
        let _webhook_app = build_webhook_router(state.clone());

        // 1. Missing or invalid bearer token returns 401 Unauthorized.
        let unauth_err = mcp_post_route(
            State(state.clone()),
            axum::http::HeaderMap::new(),
            axum::body::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {}
                }))
                .unwrap(),
            ),
        )
        .await
        .expect_err("missing auth must fail with 401");
        assert_eq!(unauth_err.status, StatusCode::UNAUTHORIZED);

        let mut bad_headers = axum::http::HeaderMap::new();
        bad_headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer invalid-capability-token".parse().unwrap(),
        );
        let bad_err = mcp_post_route(
            State(state.clone()),
            bad_headers,
            axum::body::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {}
                }))
                .unwrap(),
            ),
        )
        .await
        .expect_err("invalid token must fail with 401");
        assert_eq!(bad_err.status, StatusCode::UNAUTHORIZED);

        // A valid local capability is refused with 403: local agents use
        // stdio MCP, which runs as the agent, never HTTP inside the server.
        for header in [
            axum::http::header::AUTHORIZATION.as_str(),
            crate::operator::AGENT_HEADER,
        ] {
            let mut local_headers = axum::http::HeaderMap::new();
            let value = if header == "authorization" {
                format!("Bearer {local_cap}")
            } else {
                local_cap.clone()
            };
            local_headers.insert(
                axum::http::HeaderName::from_static(header),
                value.parse().unwrap(),
            );
            let refused = mcp_post_route(
                State(state.clone()),
                local_headers,
                axum::body::Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                        "params": {}
                    }))
                    .unwrap(),
                ),
            )
            .await
            .expect_err("a local capability must be refused over HTTP");
            assert_eq!(refused.status, StatusCode::FORBIDDEN, "{header}");
            assert!(
                refused.message.contains("only remote session tokens"),
                "{}",
                refused.message
            );
        }

        // Helper to call `mcp_post_route` with a Bearer token.
        let post_mcp = |token: &str, body: serde_json::Value| {
            let state = state.clone();
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            );
            let raw = axum::body::Bytes::from(serde_json::to_vec(&body).unwrap());
            async move {
                let resp = mcp_post_route(State(state), headers, raw).await.unwrap();
                let status = resp.status();
                let bytes = resp.into_body().collect().await.unwrap().to_bytes();
                let json: serde_json::Value = if bytes.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&bytes).unwrap()
                };
                (status, json)
            }
        };

        // 2. GET /mcp returns 200 OK text/event-stream by default and JSON when Accept: application/json.
        let mut get_headers = axum::http::HeaderMap::new();
        get_headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {cloud_cap}").parse().unwrap(),
        );
        let sse_resp = mcp_get_route(State(state.clone()), get_headers)
            .await
            .unwrap();
        assert_eq!(sse_resp.status(), StatusCode::OK);
        assert!(
            sse_resp
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
        );

        // 3. POST /mcp with valid Bearer token exposes the exact universal MCP tools.
        let (status, init_json) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(init_json["result"]["serverInfo"]["name"], "anyagents");

        let (status, list_json) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let remote_tools: Vec<String> = list_json["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        let expected_universal: Vec<String> = crate::mcp::UNIVERSAL_MCP_TOOLS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(remote_tools, expected_universal);

        // Calling `send`, `inbox`, `agents`, `memory` succeeds on the universal surface.
        let (_, send_res) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "send",
                    "arguments": {
                        "to": ["spiron-worker"],
                        "text": "hello from remote mcp"
                    }
                }
            }),
        )
        .await;
        assert_ne!(
            send_res["result"]["isError"],
            serde_json::json!(true),
            "{send_res}"
        );

        let (_, inbox_res) = post_mcp(
            &spiron_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "inbox",
                    "arguments": { "limit": 10 }
                }
            }),
        )
        .await;
        assert_ne!(
            inbox_res["result"]["isError"],
            serde_json::json!(true),
            "{inbox_res}"
        );
        assert_eq!(
            inbox_res["result"]["structuredContent"]["messages"][0]["text"],
            "hello from remote mcp"
        );

        let (_, agents_res) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "agents",
                    "arguments": { "action": "list" }
                }
            }),
        )
        .await;
        assert_ne!(
            agents_res["result"]["isError"],
            serde_json::json!(true),
            "{agents_res}"
        );

        let (_, mem_res) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "memory",
                    "arguments": {
                        "action": "write",
                        "topic": "remote-note",
                        "description": "Note written over remote MCP",
                        "content": "# Remote Note\nVerified.\n"
                    }
                }
            }),
        )
        .await;
        assert_ne!(
            mem_res["result"]["isError"],
            serde_json::json!(true),
            "{mem_res}"
        );

        // 4. `spiron` receives the exact same universal MCP surface as every other harness,
        // and removed/non-core tools are blocked uniformly.
        let (_, spiron_default_json) = post_mcp(
            &spiron_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/list",
                "params": {}
            }),
        )
        .await;
        let spiron_default_tools: Vec<String> = spiron_default_json["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(spiron_default_tools, expected_universal);

        for omitted in [
            "code",
            "files",
            "skills",
            "web",
            "web_search",
            "web_fetch",
            "browser",
            "computer",
            "device",
            "media",
        ] {
            assert!(!spiron_default_tools.contains(&omitted.to_string()));
            let (_, err_json) = post_mcp(
                &spiron_cap,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": omitted,
                        "arguments": {}
                    }
                }),
            )
            .await;
            assert!(
                err_json["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("not available on the AnyAgents MCP surface"),
                "expected `{omitted}` to be blocked on universal MCP surface, got {err_json}"
            );
        }
    });
}

#[test]
fn http_mcp_serves_only_universal_tools_and_blocks_host_tools() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let local = state
        .store
        .create_agent(anyagents_core::NewAgent::new("seat-agent", "claude-cloud"))
        .unwrap();
    crate::remote::begin_session(&state.store, &local).unwrap();
    let cap =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &local.id))
            .unwrap();
    let cfg = anyagents_core::agent_config_dir(dir.path(), &local.id);
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::write(cfg.join("cloud.json"), r#"{"disable_builtin_tools":true}"#).unwrap();
    agent_tools::SandboxConfig {
        endpoint: "https://sandbox.example.com".into(),
        sandbox_id: "sbx-1".into(),
        token: None,
        workdir: "/workspace".into(),
        remote_spawn: false,
    }
    .write_to_config_dir(&cfg)
    .unwrap();
    let marker = dir.path().join("exec-ran");

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (status, list) = post_mcp_as(&state, &cap, None, tools_list()).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed_tools(&list), crate::mcp::UNIVERSAL_MCP_TOOLS);

        let exec = serde_json::json!({
            "action": "exec",
            "command": format!("touch {}", marker.display()),
        });
        for (tool, args) in [
            ("files", exec.clone()),
            ("mcp__anyagents__files", exec.clone()),
            ("browser", serde_json::json!({ "action": "status" })),
            ("computer", serde_json::json!({ "action": "screenshot" })),
            ("device", serde_json::json!({ "action": "screenshot" })),
            ("media", serde_json::json!({ "action": "speech" })),
            ("web", serde_json::json!({ "url": "https://example.com" })),
            ("web_search", serde_json::json!({ "query": "x" })),
            (
                "web_fetch",
                serde_json::json!({ "url": "https://example.com" }),
            ),
            ("code", serde_json::json!({ "code": "return 1;" })),
            ("skills", serde_json::json!({ "action": "list" })),
        ] {
            let (_, refused) = post_mcp_as(&state, &cap, None, tool_call(tool, args))
                .await
                .unwrap();
            assert!(
                refused["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("not available on the AnyAgents MCP surface"),
                "{tool}: {refused}"
            );
        }
        assert!(!marker.exists(), "an HTTP call ran a host command");
    });
}

#[test]
fn calls_over_http_mcp_cannot_start_or_read_on_the_host() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let local = state
        .store
        .create_agent(anyagents_core::NewAgent::new("http-local", "claude-code"))
        .unwrap();
    let local_cap = state.store.issue_capability(&local.id).unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new("http-cloud", "claude-cloud"))
        .unwrap();
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let remote =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
            .unwrap();
    let mut children = Vec::new();
    {
        let parent = &cloud;
        let child = state
            .store
            .create_agent(anyagents_core::NewAgent {
                parent_id: Some(parent.id.clone()),
                ..anyagents_core::NewAgent::new(format!("{}-child", parent.name), "claude-cloud")
            })
            .unwrap();
        let sleeper = state
            .store
            .create_agent(anyagents_core::NewAgent {
                parent_id: Some(parent.id.clone()),
                ..anyagents_core::NewAgent::new(format!("{}-sleeper", parent.name), "claude-code")
            })
            .unwrap();
        state
            .store
            .set_agent_state(&sleeper.id, "archived")
            .unwrap();
        children.push((child, sleeper));
    }
    let secret = dir.path().join("host-secret");
    std::fs::write(&secret, "not for a remote caller").unwrap();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // A local capability never reaches these tools over HTTP.
        let refused_local = post_mcp_as(
            &state,
            &local_cap,
            None,
            tool_call("agents", serde_json::json!({ "action": "list" })),
        )
        .await
        .unwrap_err();
        assert_eq!(refused_local.status, StatusCode::FORBIDDEN);
        {
            let token = &remote;
            let (child, sleeper) = &children[0];
            let call = |tool: &str, args: serde_json::Value| {
                let state = state.clone();
                let token = token.to_string();
                let request = tool_call(tool, args);
                async move {
                    post_mcp_as(&state, &token, None, request)
                        .await
                        .unwrap()
                        .1
                }
            };
            let refused = |result: &serde_json::Value, why: &str| {
                assert_eq!(result["result"]["isError"], true, "{result}");
                let text = result["result"]["content"][0]["text"]
                    .as_str()
                    .expect("MCP tool error text");
                assert!(text.contains(why), "{why}: {result}");
            };

            // Spawning a host-executed agent, through either tool.
            let prefix = token.chars().take(6).collect::<String>();
            let named = |n: &str| format!("host-{prefix}-{n}");
            let attempts = vec![
                (
                    "agents",
                    serde_json::json!({ "action": "spawn", "name": named("a"), "harness": "claude-code" }),
                    named("a"),
                ),
                (
                    "spawn",
                    serde_json::json!({ "name": named("c"), "harness": "codex" }),
                    named("c"),
                ),
            ];
            for (tool, args, name) in attempts {
                refused(&call(tool, args).await, "only on a cloud harness");
                assert!(state.store.agent_by_name(&name).is_err(), "{name}");
            }
            // Moving a child onto a host harness.
            refused(
                &call(
                    "agents",
                    serde_json::json!({ "action": "harness", "name": child.name, "harness": "claude-code" }),
                )
                .await,
                "only on a cloud harness",
            );
            let child_after = state.store.agent(&child.id).unwrap();
            assert_eq!(child_after.harness, "claude-code");
            assert_eq!(child_after.runtime, anyagents_core::ExecutionRuntime::Cloud);
            // HTTP MCP cannot explicitly wake an archived host agent,
            // and a send cannot wake it over any transport.
            refused(
                &call("wake", serde_json::json!({ "agent": sleeper.name })).await,
                "cannot wake it",
            );
            refused(
                &call(
                    "send",
                    serde_json::json!({ "to": [sleeper.name], "text": "wake up" }),
                )
                .await,
                "agents(action=\"wake\")",
            );
            assert_eq!(state.store.agent(&sleeper.id).unwrap().state, "archived");
            // Attaching a host file as media, by path or by bare name.
            for source in [secret.display().to_string(), "operator-token".to_string()] {
                refused(
                    &call(
                        "send",
                        serde_json::json!({
                            "to": [child.name],
                            "text": "here",
                            "media": { "kind": "document", "source": source },
                        }),
                    )
                    .await,
                    "by `https://` URL only",
                );
            }
            // A URL attachment and an ordinary send still work.
            let sent = call(
                "send",
                serde_json::json!({
                    "to": [child.name],
                    "text": "with a link",
                    "media": { "kind": "photo", "source": "https://example.com/a.png" },
                }),
            )
            .await;
            assert_ne!(sent["result"]["isError"], true, "{sent}");
            assert!(
                state
                    .store
                    .inbox(&child.id, None)
                    .unwrap()
                    .iter()
                    .any(|message| message.text.contains("with a link"))
            );
        }
    });
}

/// The review's probe, kept as a regression: a seat plants links in the
/// memory and skill directories it can write, then asks over HTTP MCP,
/// where the tools run as the server. A local capability is refused
/// outright; a remote session token's reads never follow the links.
#[test]
fn http_mcp_never_reads_a_link_a_seat_planted() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let canary = "OPERATOR-SECRET-CANARY-7731";
    let secrets = dir.path().join("secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    let secret = secrets.join("operator-token");
    std::fs::write(&secret, canary).unwrap();
    // Makes the linked directory look like a skill to discovery.
    std::fs::write(secrets.join("SKILL.md"), "---\nname: x\n---\n").unwrap();

    let seat = state
        .store
        .create_agent(anyagents_core::NewAgent::new("mem-seat", "claude-code"))
        .unwrap();
    let seat_cap = state.store.issue_capability(&seat.id).unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new("mem-cloud", "claude-cloud"))
        .unwrap();
    for agent in [&seat, &cloud] {
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&config).unwrap();
        let root = config.parent().unwrap().to_path_buf();
        let memory = root.join("memory");
        std::fs::create_dir_all(&memory).unwrap();
        std::os::unix::fs::symlink(&secret, memory.join("MEMORY.md")).unwrap();
        std::os::unix::fs::symlink(&secret, memory.join("leak.md")).unwrap();
        let overlay = root.join("skills").join("leaky");
        std::fs::create_dir_all(&overlay).unwrap();
        std::os::unix::fs::symlink(&secret, overlay.join("SKILL.md")).unwrap();
        let view = root.join("skills-view");
        std::fs::create_dir_all(&view).unwrap();
        std::os::unix::fs::symlink(&secrets, view.join("borrowed")).unwrap();
    }
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let remote =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
            .unwrap();

    let probes = [
        ("memory", serde_json::json!({ "action": "list" })),
        (
            "memory",
            serde_json::json!({ "action": "read", "topic": "leak" }),
        ),
        (
            "memory",
            serde_json::json!({ "action": "read", "topic": "MEMORY" }),
        ),
        ("memory", serde_json::json!({ "action": "status" })),
        ("skills", serde_json::json!({ "action": "list" })),
        (
            "skills",
            serde_json::json!({ "action": "read", "name": "leaky" }),
        ),
        (
            "skills",
            serde_json::json!({ "action": "read", "name": "borrowed", "file": "operator-token" }),
        ),
        (
            "skills",
            serde_json::json!({ "action": "search", "query": "CANARY" }),
        ),
    ];
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (tool, args) in &probes {
            let refused = post_mcp_as(&state, &seat_cap, None, tool_call(tool, args.clone()))
                .await
                .unwrap_err();
            assert_eq!(refused.status, StatusCode::FORBIDDEN, "{tool} {args}");
            assert!(!refused.message.contains(canary));

            let (status, body) = post_mcp_as(&state, &remote, None, tool_call(tool, args.clone()))
                .await
                .unwrap();
            assert_eq!(status, StatusCode::OK, "{tool} {args}");
            assert!(
                !body.to_string().contains(canary),
                "{tool} {args} leaked: {body}"
            );
        }
    });
}
