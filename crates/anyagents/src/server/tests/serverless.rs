use super::super::webhooks_mcp::mcp_post_route;
use super::super::*;
use super::helpers::{env_lock, listed_tools};
use axum::http::StatusCode;

#[test]
fn stateless_remote_sandbox_workdir_rejects_host_launcher_paths() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sbx-wd", "claude-code"))
        .unwrap();

    // Default logical workspace when nothing is configured (never a coordinator host path).
    let default_wd = resolve_remote_sandbox_workdir(&store, &agent);
    assert_eq!(
        default_wd,
        anyagents_core::stateless::DEFAULT_REMOTE_SANDBOX_WORKDIR
    );

    // Host `<data>/agents/<id>/launcher` path in `agent.workdir` or launch_env MUST be rejected!
    let host_launcher = dir
        .path()
        .join("agents")
        .join(agent.id.as_str())
        .join("launcher")
        .display()
        .to_string();
    store
        .set_agent_launch_env(
            &agent.id,
            &[("ANYAGENTS_SANDBOX_WORKDIR".into(), host_launcher)],
        )
        .unwrap();
    let agent_host = store.agent(&agent.id).unwrap();
    assert_eq!(
        resolve_remote_sandbox_workdir(&store, &agent_host),
        anyagents_core::stateless::DEFAULT_REMOTE_SANDBOX_WORKDIR
    );

    // Explicit logical remote workspace in `agent_sandbox_bindings` is honored.
    let mut binding = anyagents_core::stateless::SandboxBinding::new(agent.id.clone(), "sylphx");
    binding.remote_workdir = "/home/user/agent-data/workspace/repo".to_string();
    store.sandbox_binding_store().upsert(&binding).unwrap();
    assert_eq!(
        resolve_remote_sandbox_workdir(&store, &agent_host),
        "/home/user/agent-data/workspace/repo"
    );
}

#[test]
fn stateless_in_process_remote_harness_delivers_turn_without_os_child_and_keeper_host_is_in_process()
 {
    use anyagents_harness::{Context, DeliverOutcome, Harness, OutboundMessage, Proof};
    use anyagents_keeper::Hosted as _;
    use std::io::{BufRead as _, Write as _};

    let _env = env_lock();
    let prev_mode = std::env::var_os("ANYAGENTS_SANDBOX_MODE");
    unsafe {
        std::env::set_var("ANYAGENTS_SANDBOX_MODE", "remote_required");
    }
    assert!(matches!(
        crate::keeper::host(),
        Ok(crate::vendor::HarnessHost::InProcess)
    ));
    match prev_mode {
        Some(v) => unsafe { std::env::set_var("ANYAGENTS_SANDBOX_MODE", v) },
        None => unsafe { std::env::remove_var("ANYAGENTS_SANDBOX_MODE") },
    }

    // Exercise `InProcessRemoteHarness` end-to-end with an in-memory stream-json worker.
    let mut harness = crate::vendor::InProcessRemoteHarness::start_with_worker(
        true,
        "test",
        |reader, mut writer| {
            for line in reader.lines() {
                let Ok(line) = line else { break };
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(ev) = serde_json::from_str::<serde_json::Value>(trimmed) else {
                    continue;
                };
                if ev.get("type").and_then(|v| v.as_str()) == Some("user") {
                    writeln!(writer, "{trimmed}")?;
                    let res = serde_json::json!({
                        "type": "result",
                        "subtype": "success",
                        "is_error": false,
                        "session_id": "inproc-sess-42",
                        "result": "ok",
                        "usage": { "input_tokens": 11, "output_tokens": 7 }
                    });
                    writeln!(writer, "{res}")?;
                    writer.flush()?;
                }
            }
            Ok(())
        },
    )
    .expect("start InProcessRemoteHarness");

    assert!(
        harness.process_id().is_none(),
        "InProcessRemoteHarness must spawn ZERO OS child processes"
    );
    let msg = OutboundMessage {
        id: "msg-inproc-1".to_string(),
        seq: 1,
        text: "ping".to_string(),
        sender: Some("operator".to_string()),
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    };
    let outcome = harness.deliver(&msg).expect("deliver to in-process worker");
    assert!(
        matches!(outcome, DeliverOutcome::Accepted { .. }),
        "unexpected deliver outcome: {outcome:?}"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while harness.proven("msg-inproc-1")
        != Some(Proof::Taken {
            turn_finished: true,
        })
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(
        harness.proven("msg-inproc-1"),
        Some(Proof::Taken {
            turn_finished: true
        })
    );
    assert_eq!(harness.session_id().as_deref(), Some("inproc-sess-42"));
    assert_eq!(harness.turns_served(), 1);
    assert_eq!(harness.served_usage().input_tokens, 11);
    assert_eq!(harness.served_usage().output_tokens, 7);
    harness.stop();
}

#[test]
fn stateless_dockerfile_sets_anyagents_data_not_data_dir() {
    let dockerfile = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Dockerfile");
    let body = std::fs::read_to_string(&dockerfile)
        .unwrap_or_else(|e| panic!("read {}: {e}", dockerfile.display()));
    assert!(
        body.contains("ANYAGENTS_DATA=/var/lib/anyagents"),
        "Dockerfile must export ANYAGENTS_DATA=/var/lib/anyagents"
    );
    assert!(
        !body.contains("ANYAGENTS_DATA_DIR"),
        "Dockerfile must not reference dead ANYAGENTS_DATA_DIR env var"
    );
    for heavy in [
        "chromium",
        "sway",
        "wayvnc",
        "fonts-noto-core",
        "nodejs",
        "oven/bun",
    ] {
        assert!(
            !body.contains(heavy),
            "slim coordinator Dockerfile must not install `{heavy}`"
        );
    }
}

#[test]
fn stateless_two_replicas_mcp_and_gateway_never_revoke_capabilities_or_overwrite_launch_env() {
    use anyagents_gateway::RouteResolver as _;
    use http_body_util::BodyExt as _;

    let url = match std::env::var("ANYAGENTS_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        Some(u) => u,
        None => {
            eprintln!(
                "skipping stateless_two_replicas_mcp_and_gateway test: ANYAGENTS_TEST_POSTGRES_URL not set"
            );
            return;
        }
    };
    let _env = env_lock();
    let tag = format!("{}_{}", std::process::id(), anyagents_core::now_ms());
    let dir_a = crate::test_tempdir();
    let dir_b = crate::test_tempdir();
    let store_a = Arc::new(
        Store::open_postgres_isolated(&url, dir_a.path(), format!("srv-a-{tag}"))
            .expect("open replica A store"),
    );
    let store_b = Arc::new(
        Store::open_postgres_isolated(&url, dir_b.path(), format!("srv-b-{tag}"))
            .expect("open replica B store"),
    );
    let shared_cap_key = "a".repeat(64);
    store_a.hold_capability_key(&shared_cap_key).unwrap();
    store_b.hold_capability_key(&shared_cap_key).unwrap();

    let state_a = AppState {
        store: Arc::clone(&store_a),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir_a.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    };
    let state_b = AppState {
        store: Arc::clone(&store_b),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir_b.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    };

    // 1. Replica A creates a remote-sandbox-backed agent with a live local capability,
    //    a PG `agent_sandbox_bindings` row, a remote session token, a PG gateway route,
    //    and a public cloud `launch_env`.
    let agent = store_a
        .create_agent(anyagents_core::NewAgent::new(
            format!("rep_sbx_{tag}"),
            "claude-code",
        ))
        .unwrap();
    let cap_a = store_a.issue_capability(&agent.id).unwrap();
    let mut binding = anyagents_core::stateless::SandboxBinding::new(agent.id.clone(), "sylphx");
    binding.volume_id = format!("vol-srv-{tag}");
    binding.lease_id = Some(format!("lease-srv-{tag}"));
    binding.lease_state = "ready".to_string();
    store_a.sandbox_binding_store().upsert(&binding).unwrap();

    let remote_tok = store_a
        .begin_remote_session(&agent.id, 3_600_000)
        .unwrap()
        .token;

    let key = anyagents_core::stateless::MasterKey::from_bytes(&[0x55u8; 32]).unwrap();
    let gw_tok = crate::gateway::store_key_in(
        &store_a,
        &agent,
        "https://api.example.test/v1",
        "gpt-5",
        "sk-srv-synthetic",
        Some(key.clone()),
    )
    .expect("store gateway route in PG");
    let cloud_env = vec![
        (
            "OPENAI_BASE_URL".to_string(),
            "https://public-gateway.example.test/v1".to_string(),
        ),
        ("OPENAI_API_KEY".to_string(), gw_tok.clone()),
    ];
    store_a.set_agent_launch_env(&agent.id, &cloud_env).unwrap();
    let expected_launch_env = store_a.agent(&agent.id).unwrap().launch_env;

    // 2. Verify replica B has ZERO local sandbox/capability/provider files on disk!
    let cfg_b = anyagents_core::agent_config_dir(dir_b.path(), &agent.id);
    assert!(!cfg_b.exists());

    // 3. Replica B serves HTTP MCP using PG `agent_sandbox_bindings`
    //    and direct `Principal::Agent` injection WITHOUT calling `operator::ensure()`.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {remote_tok}").parse().unwrap(),
        );
        let resp = mcp_post_route(
            State(state_b.clone()),
            headers.clone(),
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
        .unwrap_or_else(|e| {
            panic!(
                "replica B MCP tools/list failed: {}: {}",
                e.status, e.message
            )
        });
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            listed_tools(&parsed),
            crate::mcp::UNIVERSAL_MCP_TOOLS,
            "replica B must expose the universal MCP surface"
        );

        // Also call an MCP tool (`inbox`) to verify the in-process router accepts
        // `InjectedPrincipal` cleanly with zero local capability file on replica B!
        let call_resp = mcp_post_route(
            State(state_b.clone()),
            headers,
            axum::body::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "inbox",
                        "arguments": {}
                    }
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "replica B MCP inbox call failed: {}: {}",
                e.status, e.message
            )
        });
        assert_eq!(call_resp.status(), StatusCode::OK);
    });

    // 4. Replica A's original capability token MUST STILL BE VALID in PG (never revoked by replica B!).
    let authed = store_a
        .authenticate_agent(&cap_a)
        .expect("replica A capability MUST NOT be revoked when replica B serves MCP");
    assert_eq!(authed.id, agent.id);
    let _ = state_a;

    // 5. Replica B resolves replica A's gateway route directly from PG `gateway_routes`!
    let resolver_b =
        crate::gateway::PgGatewayRoutes::with_master_key(Arc::clone(&store_b), Some(key));
    let route_b = resolver_b
        .resolve(&gw_tok)
        .expect("replica B resolves gateway token from PG");
    assert_eq!(route_b.agent, agent.name);
    assert_eq!(route_b.api_key, "sk-srv-synthetic");

    // 6. Under `ANYAGENTS_SANDBOX_MODE=remote_required`, `gateway::start` on replica B
    //    MUST NOT bind a random loopback port and MUST NOT overwrite `agents.launch_env` in PG!
    std::fs::write(dir_b.path().join("database-url"), format!("{url}\n")).unwrap();
    let prev_mode = std::env::var_os("ANYAGENTS_SANDBOX_MODE");
    unsafe {
        std::env::set_var("ANYAGENTS_SANDBOX_MODE", "remote_required");
    }
    let started_b = crate::gateway::start(dir_b.path()).expect("gateway::start in remote_required");
    match prev_mode {
        Some(v) => unsafe { std::env::set_var("ANYAGENTS_SANDBOX_MODE", v) },
        None => unsafe { std::env::remove_var("ANYAGENTS_SANDBOX_MODE") },
    }
    assert!(!started_b.moved);
    assert!(
        !dir_b.path().join("gateway.port").exists(),
        "stateless remote_required replica MUST NOT bind pod-local gateway.port"
    );
    let reloaded = store_a.agent(&agent.id).unwrap();
    assert_eq!(
        reloaded.launch_env, expected_launch_env,
        "stateless replica gateway::start MUST NOT overwrite agents.launch_env in PG"
    );
    assert!(
        reloaded.launch_env.iter().any(|(k, v)| {
            k == "OPENAI_BASE_URL" && v == "https://public-gateway.example.test/v1"
        }),
        "public gateway URL must remain intact: {:?}",
        reloaded.launch_env
    );

    // Clean up test agent in PG.
    store_a.delete_agent(&agent.id).unwrap();
}
