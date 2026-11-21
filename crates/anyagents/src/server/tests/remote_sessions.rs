use super::super::agents::{HarnessRequest, create_agent_route, harness_route};
use super::super::ops::{RevokeRemoteSessionsRequest, revoke_remote_sessions_route};
use super::super::webhooks_mcp::mcp_get_route;
use super::super::*;
use super::helpers::{
    archetype_validation_state, env_lock, listed_tools, post_mcp_as, tool_call, tools_list,
};
use axum::Json;
use axum::http::StatusCode;

#[test]
fn remote_session_tokens_get_universal_mcp_surface() {
    use http_body_util::BodyExt as _;
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
        .create_agent(anyagents_core::NewAgent::new(
            "cloud-remote",
            "claude-cloud",
        ))
        .unwrap();
    let peer = state
        .store
        .create_agent(anyagents_core::NewAgent::new("local-peer", "claude-code"))
        .unwrap();
    let peer_cap = state.store.issue_capability(&peer.id).unwrap();
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let cloud_cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud.id);
    let remote = crate::remote::session_token(&cloud_cfg).unwrap();
    let expected_universal: Vec<String> = crate::mcp::UNIVERSAL_MCP_TOOLS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    assert_eq!(
        expected_universal,
        [
            "agents",
            "send",
            "report_bug",
            "inbox",
            "ingress_guard",
            "conversation",
            "memory",
            "schedules",
            "groups",
        ]
    );

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (status, list) = post_mcp_as(&state, &remote, None, tools_list())
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed_tools(&list), expected_universal);

        // Nothing outside the canonical tools can be called, even by vendor prefix.
        for host_tool in [
            "browser",
            "computer",
            "device",
            "media",
            "files",
            "web",
            "web_search",
            "web_fetch",
            "code",
            "skills",
            "mcp__anyagents__browser",
            "mcp__anyagents__computer",
        ] {
            let (_, refused) = post_mcp_as(
                &state,
                &remote,
                None,
                tool_call(host_tool, serde_json::json!({})),
            )
            .await
            .unwrap();
            assert!(
                refused["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("not available on the AnyAgents MCP surface"),
                "{host_tool}: {refused}"
            );
        }

        // Coordination works, as the agent the token names.
        let (_, sent) = post_mcp_as(
            &state,
            &remote,
            None,
            tool_call(
                "send",
                serde_json::json!({ "to": ["local-peer"], "text": "from the cloud" }),
            ),
        )
        .await
        .unwrap();
        assert_ne!(sent["result"]["isError"], serde_json::json!(true), "{sent}");
        let inbox = state.store.inbox(&peer.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "from the cloud");
        assert!(
            inbox[0]
                .sender
                .as_deref()
                .is_some_and(|sender| sender.contains(cloud.id.as_str())),
            "{:?}",
            inbox[0].sender
        );

        // The GET route names the session, its expiry, and the universal tools.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {remote}").parse().unwrap(),
        );
        headers.insert(
            axum::http::header::ACCEPT,
            "application/json".parse().unwrap(),
        );
        let described = mcp_get_route(State(state.clone()), headers).await.unwrap();
        let bytes = described.into_body().collect().await.unwrap().to_bytes();
        let described: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(described["agent"], "cloud-remote");
        assert_eq!(
            described["tools"],
            serde_json::json!(crate::mcp::UNIVERSAL_MCP_TOOLS)
        );
        let grant = state.store.authenticate_remote_session(&remote).unwrap();
        assert_eq!(described["remote_session"]["session_id"], grant.session_id);
        assert_eq!(
            described["remote_session"]["expires_at_ms"],
            grant.expires_at_ms
        );

        // Over HTTP a local capability is refused outright.
        let refused = post_mcp_as(&state, &peer_cap, None, tools_list())
            .await
            .unwrap_err();
        assert_eq!(refused.status, StatusCode::FORBIDDEN);
    });
}

#[test]
fn remote_session_tokens_stop_at_expiry_revocation_relaunch_and_stop() {
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
        .create_agent(anyagents_core::NewAgent::new(
            "cloud-expiring",
            "claude-cloud",
        ))
        .unwrap();
    let cloud_cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud.id);
    let begin = || {
        crate::remote::begin_session(&state.store, &cloud).unwrap();
        crate::remote::session_token(&cloud_cfg).unwrap()
    };
    let set_expiry = |at_ms: i64| {
        rusqlite::Connection::open(dir.path().join("state.db"))
            .unwrap()
            .execute(
                "UPDATE remote_session_tokens SET expires_at_ms = ?1",
                [at_ms],
            )
            .unwrap();
    };

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let accepted = |token: String| {
            let state = state.clone();
            async move {
                post_mcp_as(&state, &token, None, tools_list())
                    .await
                    .map(|(status, _)| status)
                    .map_err(|err| (err.status, err.message))
            }
        };
        let refused = |result: Result<StatusCode, (StatusCode, String)>| {
            let (status, message) = result.unwrap_err();
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert!(message.contains("remote session token"), "{message}");
        };

        // Expiry.
        let first = begin();
        assert_eq!(accepted(first.clone()).await.unwrap(), StatusCode::OK);
        set_expiry(anyagents_core::now_ms() - 1);
        refused(accepted(first.clone()).await);

        // On demand, through the operator route.
        let second = begin();
        assert_eq!(accepted(second.clone()).await.unwrap(), StatusCode::OK);
        let revoked = revoke_remote_sessions_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("cloud-expiring".into()),
            Json(RevokeRemoteSessionsRequest::default()),
        )
        .await
        .unwrap();
        assert_eq!(revoked.0["revoked"], 1);
        refused(accepted(second.clone()).await);
        assert!(!cloud_cfg.join(crate::remote::TOKEN_FILE).exists());

        // A relaunch supersedes the previous session.
        let third = begin();
        let fourth = begin();
        refused(accepted(third).await);
        assert_eq!(accepted(fourth.clone()).await.unwrap(), StatusCode::OK);

        // Pausing (stop) and archiving revoke in the store commit itself.
        state.store.set_agent_state(&cloud.id, "paused").unwrap();
        refused(accepted(fourth.clone()).await);
        state.store.set_agent_state(&cloud.id, "running").unwrap();
        refused(accepted(fourth).await);
        let fifth = begin();
        assert_eq!(accepted(fifth.clone()).await.unwrap(), StatusCode::OK);
        state.store.set_agent_state(&cloud.id, "archived").unwrap();
        refused(accepted(fifth).await);

        // A remote token is never a credential on the internal API.
        state.store.set_agent_state(&cloud.id, "running").unwrap();
        let sixth = begin();
        let principal = state.auth.principal(&state.store, &{
            let request = axum::http::Request::builder()
                .header(crate::operator::AGENT_HEADER, sixth.as_str())
                .body(())
                .unwrap();
            let (parts, ()) = request.into_parts();
            crate::auth::Presented::of(&parts)
        });
        assert!(principal.is_err(), "{principal:?}");
    });
}

#[test]
fn remote_agents_cannot_be_created_without_a_public_url() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let refused = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "cloud-unconfigured",
                    "harness": "claude-cloud",
                    "parent": "root",
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(refused.status, StatusCode::BAD_REQUEST);
        assert!(
            refused.message.contains("public_url"),
            "{}",
            refused.message
        );
        assert!(
            refused.message.contains(crate::remote::CONFIG_FILE),
            "{}",
            refused.message
        );
        assert!(state.store.agent_by_name("cloud-unconfigured").is_err());

        // Moving an existing agent onto a remote harness is refused too.
        state
            .store
            .create_agent(anyagents_core::NewAgent::new("local-first", "claude-code"))
            .unwrap();
        let moved = harness_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("local-first".into()),
            axum::http::HeaderMap::new(),
            Json(HarnessRequest {
                harness: "openai-cloud".into(),
                runtime: None,
                budget_cap_micros: None,
                model: None,
                profile: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(moved.status, StatusCode::BAD_REQUEST);
        assert!(moved.message.contains("public_url"), "{}", moved.message);
        assert_eq!(
            state.store.agent_by_name("local-first").unwrap().harness,
            "claude-code"
        );
    });
}

#[test]
fn a_remote_token_stops_authenticating_once_its_agent_runs_on_the_host() {
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
        .create_agent(anyagents_core::NewAgent::new("cloud-moved", "claude-cloud"))
        .unwrap();
    let sandboxed = state
        .store
        .create_agent(anyagents_core::NewAgent::new("sandboxed", "claude-code"))
        .unwrap();
    let sandbox_cfg = anyagents_core::agent_config_dir(dir.path(), &sandboxed.id);
    agent_tools::SandboxConfig {
        endpoint: "https://sandbox.example.com".into(),
        sandbox_id: "sbx-1".into(),
        token: None,
        workdir: "/workspace".into(),
        remote_spawn: true,
    }
    .write_to_config_dir(&sandbox_cfg)
    .unwrap();
    assert!(crate::remote::runs_off_host(&sandboxed, &sandbox_cfg));
    assert!(crate::remote::runs_off_host(
        &cloud,
        &anyagents_core::agent_config_dir(dir.path(), &cloud.id)
    ));

    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let remote =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
            .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (status, _) = post_mcp_as(&state, &remote, None, tools_list())
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        state
            .store
            .set_agent_runtime(&cloud.id, &anyagents_core::ExecutionRuntime::Local)
            .unwrap();
        let err = post_mcp_as(&state, &remote, None, tools_list())
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
    });
}
