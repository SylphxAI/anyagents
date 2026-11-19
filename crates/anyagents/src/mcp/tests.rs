//! Unit and integration tests for MCP tools, profiles, code mode, and workspace backends.

use super::*;

#[cfg(test)]
mod spawn_args_tests {
    #[tokio::test]
    async fn mcp_and_cli_use_only_primary_socket_and_preserve_its_errors() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let primary = tokio::net::UnixListener::bind(&socket).unwrap();
        let admin = tokio::net::UnixListener::bind(dir.path().join("admin.sock")).unwrap();
        let admin_calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&admin_calls);
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, status, body) = tokio::select! {
                    accepted = primary.accept() => (accepted.unwrap().0, "500 Internal Server Error", r#"{"error":"primary refused"}"#),
                    accepted = admin.accept() => {
                        observed.fetch_add(1, Ordering::SeqCst);
                        (accepted.unwrap().0, "200 OK", r#"{"agents":[]}"#)
                    }
                };
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                }
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let client = Client::new(socket, dir.path().join("config"), "test-capability".into());
        assert_eq!(
            client
                .request_method::<serde_json::Value>("POST", "/v1/agents", None)
                .await
                .unwrap_err(),
            "primary refused"
        );
        assert_eq!(client.get_agents().await.err().unwrap(), "primary refused");
        let data = dir.path().to_owned();
        let error = tokio::task::spawn_blocking(move || {
            crate::client::call::<serde_json::Value>(
                &data,
                "POST",
                "/v1/agents",
                None,
                &crate::client::Authority::None,
            )
            .unwrap_err()
        })
        .await
        .unwrap();
        assert_eq!(error, "primary refused");
        assert_eq!(admin_calls.load(Ordering::SeqCst), 0);
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn send_turn_result_ends_closing_tokens_without_an_interrupt_failure() {
        let closed = send_turn_result(
            SendResponse {
                deliveries: vec![],
                skipped: vec![],
            },
            true,
        )
        .unwrap();
        assert_eq!(closed["status"], "turn_complete_no_reply_sent");
        assert!(
            closed["turn_complete_hint"]
                .as_str()
                .unwrap()
                .contains("Do not call send or any other tool again")
        );
        let normal = send_turn_result(
            SendResponse {
                deliveries: vec![],
                skipped: vec![],
            },
            false,
        )
        .unwrap();
        assert!(normal.get("status").is_none());
        assert!(
            normal["turn_complete_hint"]
                .as_str()
                .unwrap()
                .contains("without calling send again")
        );
        assert_eq!(normal["deliveries"], json!([]));
    }

    #[test]
    fn send_closing_sentinels_are_exact_text_only_tokens() {
        for text in [
            "NO_REPLY",
            " End. ",
            "end",
            "停止。",
            "停止",
            "No more.",
            "NO MORE",
            "。",
            "Done.",
            "done",
            "終了。",
            "Final!",
            "done...",
            " \nNO_REPLY。 \n",
        ] {
            assert!(
                SendRequest {
                    text: text.into(),
                    ..Default::default()
                }
                .is_turn_closing_sentinel(),
                "{text:?}"
            );
        }
        for text in [
            "",
            " ",
            "End of report.",
            "Done. The build passed.",
            "NO_REPLY is a token",
            "Final version",
            "Stopping",
            "!",
            ".",
        ] {
            assert!(
                !SendRequest {
                    text: text.into(),
                    ..Default::default()
                }
                .is_turn_closing_sentinel(),
                "{text:?}"
            );
        }
        for attachment in [
            json!({"media":{"kind":"photo","source":"fixture.png"}}),
            json!({"location":{"latitude":1.0,"longitude":2.0}}),
            json!({"poll":{"question":"End?","options":["yes","no"]}}),
            json!({"checklist":{"title":"End","tasks":["finish"]}}),
            json!({"article":{"title":"End","content":"report"}}),
            json!({"quote":{"text":"real quoted content"}}),
        ] {
            let mut value = attachment;
            value["text"] = json!("End.");
            let request: SendRequest = serde_json::from_value(value).unwrap();
            assert!(!request.is_turn_closing_sentinel());
        }
    }

    #[test]
    fn explicit_sequence_range_refuses_invalid_bounds() {
        for range in [
            "seq:bad-bad",
            "seq:1-bad",
            "seq:bad-2",
            "seq:5-2",
            "seq:-1",
            "seq:1-2-3",
            "seq:9223372036854775808",
        ] {
            assert!(super::parse_sequence_range(Some(range)).is_err(), "{range}");
        }
        assert_eq!(
            super::parse_sequence_range(Some("seq:1-2")).unwrap(),
            (Some(1), Some(2))
        );
        assert_eq!(
            super::parse_sequence_range(Some("3")).unwrap(),
            (Some(3), Some(3))
        );
        assert_eq!(super::parse_sequence_range(None).unwrap(), (None, None));
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn archetype_schemas_offer_neutral_examples_without_restricting_templates() {
        for schema in [
            schemars::schema_for!(AgentsArgs),
            schemars::schema_for!(SpawnArgs),
            schemars::schema_for!(SubagentsArgs),
        ] {
            let value = serde_json::to_value(schema).unwrap();
            assert!(
                value["properties"].get("archetype").is_none(),
                "MCP schema must expose only `role`, not a duplicate `archetype` property"
            );
            let role = &value["properties"]["role"];
            let description = role["description"].as_str().unwrap();
            assert!(description.contains("`.agents/agents/<role>.md`"));
            assert!(description.contains("`\"planner\"`, `\"builder\"`, `\"tester\"`"));
            assert!(role.get("enum").is_none());
        }
        for key in ["role", "archetype", "type", "template", "preset"] {
            let mut obj = serde_json::Map::new();
            obj.insert(key.into(), json!("custom-template"));
            let input = serde_json::Value::Object(obj);
            let agents: AgentsArgs = serde_json::from_value(input.clone()).unwrap();
            let spawn: SpawnArgs = serde_json::from_value(input.clone()).unwrap();
            let subagents: SubagentsArgs = serde_json::from_value(input).unwrap();
            for role in [agents.role, spawn.role, subagents.role] {
                assert_eq!(role.as_deref(), Some("custom-template"));
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let templates_dir = dir.path().join(".agents/agents");
        std::fs::create_dir_all(&templates_dir).unwrap();
        std::fs::write(
            templates_dir.join("builder.md"),
            "---\nname: builder\ndescription: Isolated builder specialist\n---\nBuild.",
        )
        .unwrap();
        let tools = tool_definitions_with_roles(McpProfile::Full, None, Some(dir.path()));
        let role_tools: Vec<_> = tools
            .iter()
            .filter(|t| {
                t.input_schema
                    .get("properties")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|props| props.contains_key("role"))
            })
            .map(|t| t.name.to_string())
            .collect();
        assert!(role_tools.iter().any(|n| n == "agents"), "{role_tools:?}");
        for name in &role_tools {
            let tool = tools.iter().find(|t| t.name == name.as_str()).unwrap();
            let desc = tool.description.as_deref().unwrap_or_default();
            assert!(
                desc.contains(
                    "Available roles (`general` is the built-in default): `general` (Default general-purpose end-to-end execution worker), `builder` (Isolated builder specialist)."
                ),
                "{name}: {desc}"
            );
        }
        let send_tool = tools.iter().find(|t| t.name == "send").unwrap();
        assert!(
            send_tool
                .description
                .as_deref()
                .unwrap_or_default()
                .contains("Use `operator` to report a defect in AnyAgents")
        );
        assert!(
            !send_tool
                .description
                .as_deref()
                .unwrap_or_default()
                .contains("Available roles (")
        );
        let empty_dir_tools =
            tool_definitions_with_roles(McpProfile::Full, None, Some(&dir.path().join("empty")));
        let empty_agents_desc = empty_dir_tools
            .iter()
            .find(|t| t.name == "agents")
            .and_then(|t| t.description.as_deref())
            .unwrap_or_default();
        assert!(
            empty_agents_desc.contains(
                "Available roles (`general` is the built-in default): `general` (Default general-purpose end-to-end execution worker)."
            ),
            "{empty_agents_desc}"
        );
        let without_data_dir = tool_definitions_with_roles(McpProfile::Full, None, None);
        assert_eq!(
            serde_json::to_value(&without_data_dir).unwrap(),
            serde_json::to_value(tool_definitions(McpProfile::Full)).unwrap()
        );
        let agents_schema = serde_json::to_value(schemars::schema_for!(AgentsArgs)).unwrap();
        let role_doc = agents_schema["properties"]["role"]["description"]
            .as_str()
            .unwrap();
        assert!(role_doc.contains("`\"general\"`"), "{role_doc}");
        assert!(
            role_doc.find("general").unwrap() < role_doc.find("planner").unwrap(),
            "{role_doc}"
        );
    }

    #[test]
    fn describe_and_agents_tool_docs_require_parent_ancestor_or_admin_for_mutations() {
        let schema = serde_json::to_value(schemars::schema_for!(AgentsArgs)).unwrap();
        let action_doc = schema["properties"]["action"]["description"]
            .as_str()
            .unwrap();
        assert!(action_doc.contains("Parent/Ancestor agent or an Admin"));
        assert!(action_doc.contains("Parent/Ancestor or Admin: rename"));
        let router = AgentMcp::tool_router();
        let tools = router.list_all();
        let agents_tool = tools.iter().find(|t| t.name == "agents").unwrap();
        let describe_tool = tools.iter().find(|t| t.name == "describe").unwrap();
        assert!(
            agents_tool
                .description
                .as_deref()
                .unwrap()
                .contains("requires the target's Parent/Ancestor agent or an Admin")
        );
        let describe_doc = describe_tool.description.as_deref().unwrap();
        assert!(describe_doc.contains("requires the target's Parent/Ancestor agent or an Admin"));
        assert!(!describe_doc.contains("Any agent can update its own description"));
    }

    #[test]
    fn spawn_routes_accept_separate_description_and_instructions() {
        let input = json!({
            "action": "spawn",
            "name": "worker",
            "description": "Public role",
            "instructions": "Private instructions"
        });
        let agents: AgentsArgs = serde_json::from_value(input.clone()).unwrap();
        let spawn: SpawnArgs = serde_json::from_value(input.clone()).unwrap();
        let subagents: SubagentsArgs = serde_json::from_value(input).unwrap();
        for (description, instructions) in [
            (agents.description, agents.instructions),
            (spawn.description, spawn.instructions),
            (subagents.description, subagents.instructions),
        ] {
            assert_eq!(description.as_deref(), Some("Public role"));
            assert_eq!(instructions.as_deref(), Some("Private instructions"));
        }
    }

    #[test]
    fn subagents_preserves_description_without_synthesizing_instructions() {
        let args: SubagentsArgs = serde_json::from_value(json!({
            "action": "spawn", "description": "Public role", "role": "engineer"
        }))
        .unwrap();
        assert_eq!(args.description.as_deref(), Some("Public role"));
        assert_eq!(args.role.as_deref(), Some("engineer"));
        assert!(args.instructions.is_none());
    }

    fn dummy_agent_view(
        id: &str,
        name: &str,
        state: &str,
        turn_state: &str,
        description: Option<&str>,
    ) -> AgentView {
        AgentView {
            has_open_turn: false,
            open_turn_age_ms: None,
            profile: None,
            id: id.to_string(),
            name: name.to_string(),
            role: None,
            aliases: Vec::new(),
            parent: Some("agt_parent".to_string()),
            scope: "private".to_string(),
            lifecycle: "task".to_string(),
            relationship: Some("child".to_string()),
            can_message: Some(true),
            harness: "opencode".to_string(),
            model: Some("gpt-5.4-mini".to_string()),
            live_harness: None,
            live_model: None,
            process_started_at_ms: None,
            pending_switch: false,
            workdir: Some(format!("/data/agents/{id}/work")),
            state: state.to_string(),
            process: "none".to_string(),
            turn_state: turn_state.to_string(),
            delivery: Default::default(),
            delivery_mode: String::new(),
            steer_support: String::new(),
            quota_reset_at_ms: None,
            block_reason: None,
            last_error: None,
            oldest_undelivered_age_ms: None,
            install: "installed".to_string(),
            description: description.map(str::to_string),
            instructions: None,
            seat_user: None,
            seat_uid: None,
            seat_gid: None,
            automations: Default::default(),
            schedules: Vec::new(),
            turn_errors: None,
            unsettled_outbound: Vec::new(),
            retry_at_ms: None,
        }
    }

    #[test]
    fn list_query_and_pagination_preserve_summary_and_prioritize_working_agents() {
        let mut agents = Vec::new();
        for i in 0..120 {
            agents.push(dummy_agent_view(
                &format!("agt_idle_{i}"),
                &format!("idle-sub-{i}"),
                "idle",
                "idle",
                Some("Idle worker"),
            ));
        }
        agents.push(dummy_agent_view(
            "agt_work_1",
            "active-builder",
            "running",
            "working",
            Some("Implementing archetype parser"),
        ));
        agents.push(dummy_agent_view(
            "agt_err_1",
            "failed-tester",
            "running",
            "errored",
            Some("OOM failure"),
        ));
        agents.push(dummy_agent_view(
            "agt_run_idle_1",
            "idle-planner",
            "running",
            "idle",
            Some("Planning tasks"),
        ));

        let mut default_capped = agents.clone();
        let summary = filter_and_summarize_agent_list(&mut default_capped, None, None, None);
        assert_eq!(summary["total"], 123);
        assert_eq!(summary["matched"], 123);
        assert_eq!(summary["returned"], 100);
        assert_eq!(summary["running"], 3);
        assert_eq!(summary["working"], 1);
        assert_eq!(summary["errored"], 1);
        assert_eq!(summary["scaled_to_zero"], 120);
        assert_eq!(summary["idle"], 121);
        assert_eq!(default_capped.len(), 100);
        assert!(default_capped.iter().any(|a| a.name == "active-builder"));
        assert!(default_capped.iter().any(|a| a.name == "failed-tester"));
        assert!(default_capped.iter().any(|a| a.name == "idle-planner"));

        let mut queried = agents.clone();
        let q_summary =
            filter_and_summarize_agent_list(&mut queried, Some("archetype active"), Some(10), None);
        assert_eq!(q_summary["total"], 123);
        assert_eq!(q_summary["matched"], 1);
        assert_eq!(q_summary["idle"], 121);
        assert_eq!(q_summary["running"], 3);
        assert_eq!(q_summary["returned"], 1);
        assert_eq!(queried[0].name, "active-builder");

        let mut limited = agents.clone();
        let limited_summary =
            filter_and_summarize_agent_list(&mut limited, Some("idle-sub"), Some(2), None);
        assert_eq!(limited_summary["total"], 123);
        assert_eq!(limited_summary["matched"], 120);
        assert_eq!(limited_summary["returned"], 2);
        assert_eq!(limited_summary["running"], 3);
        assert_eq!(limited_summary["idle"], 121);
        assert_eq!(limited.len(), 2);
        assert!(
            limited
                .iter()
                .all(|agent| agent.name.starts_with("idle-sub-"))
        );

        let mut unmatched = agents.clone();
        let no_matches =
            filter_and_summarize_agent_list(&mut unmatched, Some("no-such-agent"), Some(1), None);
        assert_eq!(no_matches["total"], 123);
        assert_eq!(no_matches["matched"], 0);
        assert_eq!(no_matches["returned"], 0);
        assert_eq!(no_matches["running"], 3);
        assert!(unmatched.is_empty());
    }

    #[test]
    fn list_query_preserves_archived_and_paused_population_counts() {
        let mut agents = vec![dummy_agent_view(
            "paused",
            "paused-worker",
            "paused",
            "idle",
            None,
        )];
        for i in 0..30 {
            agents.push(dummy_agent_view(
                &format!("archive-{i}"),
                &format!("archived-worker-{i}"),
                "archived",
                "idle",
                None,
            ));
        }
        // There are 30 archived agents in scope but only 11 match this query.
        // Pagination must preserve the full archived population.
        let summary =
            filter_and_summarize_agent_list(&mut agents, Some("archived-worker-1"), Some(2), None);
        assert_eq!(summary["total"], 31);
        assert_eq!(summary["archived"], 30);
        assert_eq!(summary["paused"], 1);
        assert_eq!(summary["matched"], 11);
        assert_eq!(summary["returned"], 2);
        assert_eq!(agents.len(), 2);
        assert!(
            agents
                .iter()
                .all(|agent| agent.name.starts_with("archived-worker-1"))
        );
    }

    #[test]
    fn compact_roster_150_agents_preserves_reachability_and_final_result_budget() {
        let population: Vec<_> = (0..150)
            .map(|index| {
                let mut row = dummy_agent_view(
                    &format!("agt_{index:026}"),
                    &format!("worker-{index:03}"),
                    "idle",
                    "none",
                    None,
                );
                row.name = format!("worker-{index:03}");
                row.instructions = Some("private instructions ".repeat(2000));
                row.description =
                    Some("Description with Unicode 繁體 and escaped \"text\" ".repeat(200));
                row.automations = crate::api::AutomationCountsView {
                    total: 6,
                    watches: 3,
                    timers: 2,
                    crons: 1,
                };
                row
            })
            .collect();
        for key in ["agents", "subagents"] {
            let mut cursor = None;
            let mut seen = std::collections::BTreeSet::new();
            let mut page_sizes = Vec::new();
            loop {
                let mut agents = population.clone();
                let summary = filter_and_summarize_agent_list(&mut agents, None, Some(100), cursor);
                let value = serde_json::json!({key: agents, "summary": summary});
                let page = bound_agent_list(value, key, cursor).unwrap();
                let encoded = encoded_agent_list_result(&page).unwrap();
                let wire: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
                assert_eq!(wire["structuredContent"], page);
                // Text-only and structured clients receive identical data.
                let text = wire["content"][0]["text"].as_str().unwrap();
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(text).unwrap(),
                    page
                );
                assert!(encoded.len() < AGENT_LIST_RESULT_BUDGET);
                let rows = page[key].as_array().unwrap();
                assert!(!rows.is_empty());
                page_sizes.push((
                    rows.len(),
                    serde_json::to_vec(rows).unwrap().len(),
                    encoded.len(),
                ));
                for row in rows {
                    assert!(seen.insert(row["id"].as_str().unwrap().to_owned()));
                    for omitted in [
                        "instructions",
                        "schedules",
                        "turn_errors",
                        "unsettled_outbound",
                    ] {
                        assert!(row.get(omitted).is_none(), "{omitted}");
                    }
                    assert_eq!(row["automations"]["total"], 6);
                    assert_eq!(row["description_truncated"], true);
                    assert_eq!(row["description"].as_str().unwrap().chars().count(), 160);
                    for retained in [
                        "id",
                        "name",
                        "state",
                        "turn_state",
                        "harness",
                        "model",
                        "delivery",
                    ] {
                        assert!(row.get(retained).is_some(), "{retained}");
                    }
                }
                cursor = page["next_cursor"].as_u64().map(|v| v as usize);
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(seen.len(), 150);
            eprintln!(
                "compact roster {key}: pages (rows, row bytes, CallToolResult bytes) = {page_sizes:?}"
            );
        }
        let oversized =
            serde_json::json!({"agents": [{"id": "x".repeat(31000)}], "summary": {"matched": 1}});
        assert!(
            bound_agent_list(oversized, "agents", None)
                .unwrap_err()
                .contains("one agent summary exceeds")
        );
    }

    #[test]
    fn list_order_is_identical_on_both_sides_of_limit_boundary() {
        let agents = vec![
            dummy_agent_view("paused", "a-paused", "paused", "idle", None),
            dummy_agent_view("running", "z-running", "running", "working", None),
        ];
        for limit in [1, 2, 100, usize::MAX] {
            let mut page = agents.clone();
            filter_and_summarize_agent_list(&mut page, None, Some(limit), None);
            assert_eq!(page[0].id, "running");
        }
        let mut tied = vec![
            dummy_agent_view("z", "same", "idle", "idle", None),
            dummy_agent_view("a", "same", "idle", "idle", None),
        ];
        filter_and_summarize_agent_list(&mut tied, None, None, None);
        assert_eq!(tied[0].id, "a");
    }

    #[test]
    fn list_pages_clamp_limits_and_reach_all_10002_agents_including_archives() {
        let agents: Vec<_> = (0..10002)
            .rev()
            .map(|i| {
                dummy_agent_view(
                    &format!("id-{i:05}"),
                    &format!("population-{i:05}"),
                    if i < 25 { "archived" } else { "idle" },
                    "idle",
                    None,
                )
            })
            .collect();
        let mut cursor = None;
        let mut seen = std::collections::HashSet::new();
        let mut pages = 0;
        loop {
            let mut page = agents.clone();
            let summary = filter_and_summarize_agent_list(
                &mut page,
                Some("population"),
                Some(usize::MAX),
                cursor,
            );
            assert_eq!(summary["total"], 10002);
            assert_eq!(summary["matched"], 10002);
            assert_eq!(summary["archived"], 25);
            assert_eq!(summary["idle"], 9977);
            assert!(page.len() <= MAX_LIST_LIMIT);
            for agent in page {
                assert!(seen.insert(agent.id), "duplicate page member");
            }
            pages += 1;
            cursor = summary["next_cursor"].as_u64().map(|v| v as usize);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(pages, 101);
        assert_eq!(seen.len(), 10002);
        let mut zero = agents.clone();
        let summary = filter_and_summarize_agent_list(&mut zero, None, Some(0), None);
        assert_eq!(zero.len(), 1);
        assert_eq!(summary["next_cursor"], 1);
        let mut beyond = agents;
        let summary = filter_and_summarize_agent_list(&mut beyond, None, None, Some(usize::MAX));
        assert!(beyond.is_empty());
        assert!(summary["next_cursor"].is_null());
    }

    #[tokio::test]
    async fn list_public_mcp_tools_expose_bounded_continuation_and_population_summary() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let parent_id = AgentId::generate();
        let mut population: Vec<_> = (0..103)
            .rev()
            .map(|i| {
                dummy_agent_view(
                    &format!("id-{i:03}"),
                    &format!("worker-{i:03}"),
                    "idle",
                    "idle",
                    None,
                )
            })
            .collect();
        population.push(dummy_agent_view(
            "paused", "a-paused", "paused", "idle", None,
        ));
        population.push(dummy_agent_view(
            "working",
            "z-running",
            "running",
            "working",
            None,
        ));
        for a in &mut population {
            a.instructions = Some("PRIVATE_PROMPT".into());
            a.schedules = vec![crate::api::ScheduleView {
                id: "sch_test".into(),
                agent_id: a.id.clone(),
                kind: "at".into(),
                spec: "2026-11-14T00:00:00Z".into(),
                prompt: "LONG_SCHEDULE_PROMPT".into(),
                interval_ms: None,
                next_fire_at_ms: Some(1_794_614_400_000),
                last_fired_at_ms: None,
                watch_baseline: None,
                last_observed: None,
                state: "active".into(),
                created_at_ms: 1_794_500_000_000,
            }];
            a.parent = Some(parent_id.to_string());
        }
        let body = serde_json::to_vec(&AgentsResponse {
            archived_count: 0,
            agents: population,
            gateway: None,
        })
        .unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..8 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let byte = stream.read_u8().await.unwrap();
                    request.push(byte);
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let request_line = std::str::from_utf8(&request)
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap();
                let mut parts = request_line.split_whitespace();
                assert_eq!(parts.next(), Some("GET"));
                let uri: hyper::Uri = parts.next().unwrap().parse().unwrap();
                assert_eq!(uri.path(), "/v1/agents");
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let mcp = AgentMcp {
            agent: AgentRef {
                id: parent_id,
                name: "parent".into(),
                workdir: None,
            },
            client: Client::new(
                socket,
                dir.path().join("config"),
                "synthetic-test-capability".into(),
            ),
            profile: McpProfile::Full,
        };
        for legacy in [false, true] {
            for (arguments, expected_len, next, first) in [
                (json!({"limit": 1000000}), 100, Some(100), "z-running"),
                (
                    json!({"limit": 1000000, "offset": 100}),
                    5,
                    None,
                    "worker-099",
                ),
                (json!({"limit": 1}), 1, Some(1), "z-running"),
                (json!({"limit": 2}), 2, Some(2), "z-running"),
            ] {
                let mut args = arguments;
                args["action"] = json!("list");
                args["state"] = json!("all");
                let result = if legacy {
                    mcp.subagents(Parameters(serde_json::from_value(args).unwrap()))
                        .await
                        .unwrap()
                        .0
                } else {
                    mcp.agents(Parameters(serde_json::from_value(args).unwrap()))
                        .await
                        .unwrap()
                        .0
                };
                let rows = result[if legacy { "subagents" } else { "agents" }]
                    .as_array()
                    .unwrap();
                if expected_len == 100 {
                    assert!(!rows.is_empty() && rows.len() <= 100);
                } else {
                    assert_eq!(rows.len(), expected_len);
                }
                assert!(
                    encoded_agent_list_result(&result).unwrap().len() < AGENT_LIST_RESULT_BUDGET
                );
                assert_eq!(rows[0]["name"], first);
                assert!(
                    rows.iter()
                        .all(|a| a.get("instructions").is_none() && a.get("schedules").is_none())
                );
                if expected_len == 100 {
                    assert_eq!(result["next_cursor"], json!(rows.len()));
                } else {
                    assert_eq!(result["next_cursor"], json!(next));
                }
                assert_eq!(result["summary"]["total"], 105);
                assert_eq!(result["summary"]["matched"], 105);
                assert_eq!(result["summary"]["working"], 1);
                assert_eq!(result["summary"]["paused"], 1);
            }
        }
        server.await.unwrap();
    }
    #[tokio::test]
    async fn inspect_preserves_full_view_without_duplicate_alias_or_top_level_schedules() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("server.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let parent_id = AgentId::generate();
        let mut child = dummy_agent_view("agt_child", "worker", "running", "working", None);
        child.parent = Some(parent_id.to_string());
        child.instructions = Some("FULL_PRIVATE_INSTRUCTIONS".into());
        child.schedules = vec![crate::api::ScheduleView {
            id: "sch_1".into(),
            agent_id: child.id.clone(),
            kind: "watch".into(),
            spec: "github:pr:example/repo#1".into(),
            prompt: "Verify checks".into(),
            interval_ms: None,
            next_fire_at_ms: None,
            last_fired_at_ms: None,
            watch_baseline: None,
            last_observed: None,
            state: "active".into(),
            created_at_ms: 1_794_500_000_000,
        }];
        let agents_body = serde_json::to_vec(&AgentsResponse {
            archived_count: 0,
            agents: vec![child],
            gateway: None,
        })
        .unwrap();
        let conv_body = serde_json::to_vec(&ConversationResponse {
            retrieval: None,
            agent_id: "agt_child".into(),
            agent: "worker".into(),
            turns: Vec::new(),
        })
        .unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let byte = stream.read_u8().await.unwrap();
                    request.push(byte);
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let line = std::str::from_utf8(&request)
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap();
                let path = line.split_whitespace().nth(1).unwrap();
                let body = if path.contains("/conversation") {
                    &conv_body
                } else {
                    &agents_body
                };
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });
        let mcp = AgentMcp {
            agent: AgentRef {
                id: parent_id,
                name: "parent".into(),
                workdir: None,
            },
            client: Client::new(
                socket,
                dir.path().join("config"),
                "synthetic-test-capability".into(),
            ),
            profile: McpProfile::Full,
        };
        let inspected = mcp
            .agents(Parameters(
                serde_json::from_value(json!({"action": "inspect", "name": "worker"})).unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(
            inspected["agent"]["instructions"],
            "FULL_PRIVATE_INSTRUCTIONS"
        );
        assert_eq!(inspected["agent"]["schedules"].as_array().unwrap().len(), 1);
        assert!(inspected.get("subagent").is_none());
        assert!(inspected.get("schedules").is_none());

        let legacy = mcp
            .subagents(Parameters(
                serde_json::from_value(json!({"action": "inspect", "agent": "worker"})).unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(
            legacy["subagent"]["instructions"],
            "FULL_PRIVATE_INSTRUCTIONS"
        );
        assert_eq!(legacy["subagent"]["schedules"].as_array().unwrap().len(), 1);
        assert!(legacy.get("agent").is_none());
        assert!(legacy.get("schedules").is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn list_lifecycle_states_fetch_the_requested_daemon_population() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let parent_id = AgentId::generate();
        let mut live = vec![
            dummy_agent_view("idle", "idle", "running", "idle", None),
            dummy_agent_view("working", "working", "running", "working", None),
        ];
        let mut archived = vec![dummy_agent_view(
            "archived", "archived", "archived", "idle", None,
        )];
        for row in live.iter_mut().chain(archived.iter_mut()) {
            row.parent = Some(parent_id.to_string());
        }
        // The mock has the daemon's live-only default, not an all-rows response
        // that could hide a missing lifecycle query in either public tool.
        let server = tokio::spawn(async move {
            for _ in 0..16 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    request.push(stream.read_u8().await.unwrap());
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let request = std::str::from_utf8(&request).unwrap();
                let mut parts = request.lines().next().unwrap().split_whitespace();
                assert_eq!(parts.next(), Some("GET"));
                let uri: hyper::Uri = parts.next().unwrap().parse().unwrap();
                assert_eq!(uri.path(), "/v1/agents");
                let agents = match uri.query() {
                    None => live.clone(),
                    Some("state=archived") => archived.clone(),
                    Some("state=all") => live.iter().chain(archived.iter()).cloned().collect(),
                    query => panic!("unexpected population query: {query:?}"),
                };
                let body = serde_json::to_vec(&AgentsResponse {
                    agents,
                    archived_count: 1,
                    gateway: None,
                })
                .unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let mcp = AgentMcp {
            agent: AgentRef {
                id: parent_id,
                name: "parent".into(),
                workdir: None,
            },
            client: Client::new(
                socket,
                dir.path().join("config"),
                "synthetic-test-capability".into(),
            ),
            profile: McpProfile::Full,
        };
        for legacy in [false, true] {
            for (state, expected) in [
                (None, vec!["idle", "working"]),
                (Some("  "), vec!["idle", "working"]),
                (Some("archived"), vec!["archived"]),
                (Some(" ARCHIVED "), vec!["archived"]),
                (Some("all"), vec!["archived", "idle", "working"]),
                (Some(" ALL "), vec!["archived", "idle", "working"]),
                (Some("working"), vec!["working"]),
                (Some(" WORKING "), vec!["working"]),
            ] {
                let args = json!({ "action": "list", "state": state });
                let result = if legacy {
                    mcp.subagents(Parameters(serde_json::from_value(args).unwrap()))
                        .await
                        .unwrap()
                        .0
                } else {
                    mcp.agents(Parameters(serde_json::from_value(args).unwrap()))
                        .await
                        .unwrap()
                        .0
                };
                let mut actual: Vec<_> = result[if legacy { "subagents" } else { "agents" }]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["id"].as_str().unwrap())
                    .collect();
                actual.sort_unstable();
                assert_eq!(actual, expected, "legacy={legacy}, state={state:?}");
            }
        }
        server.await.unwrap();
    }

    #[test]
    fn mcp_and_api_accept_singular_instruction_and_system_prompt_aliases() {
        for key in ["instruction", "system_prompt"] {
            let input = json!({
                "action": "spawn",
                "name": "worker",
                "description": "Public summary",
                key: "Private standing SOP"
            });
            let agents: AgentsArgs = serde_json::from_value(input.clone()).unwrap();
            let spawn: SpawnArgs = serde_json::from_value(input.clone()).unwrap();
            let describe: DescribeArgs = serde_json::from_value(input.clone()).unwrap();
            let subagents: SubagentsArgs = serde_json::from_value(input.clone()).unwrap();
            let api_req: crate::api::CreateAgentRequest = serde_json::from_value(input).unwrap();
            assert_eq!(agents.instructions.as_deref(), Some("Private standing SOP"));
            assert_eq!(spawn.instructions.as_deref(), Some("Private standing SOP"));
            assert_eq!(
                describe.instructions.as_deref(),
                Some("Private standing SOP")
            );
            assert_eq!(
                subagents.instructions.as_deref(),
                Some("Private standing SOP")
            );
            assert_eq!(
                api_req.instructions.as_deref(),
                Some("Private standing SOP")
            );
        }
    }

    #[tokio::test]
    async fn files_tool_supports_read_write_atomic_multi_edit_glob_and_grep() {
        let dir = tempfile::tempdir().unwrap();
        let workdir = dir.path().join("work");
        std::fs::create_dir_all(&workdir).unwrap();
        std::fs::write(workdir.join(".gitignore"), "ignored_dir\n*.log\n").unwrap();
        std::fs::create_dir_all(workdir.join("target")).unwrap();
        std::fs::write(workdir.join("target/skip.rs"), "fn skipped() {}\n").unwrap();
        std::fs::create_dir_all(workdir.join("ignored_dir")).unwrap();
        std::fs::write(workdir.join("ignored_dir/skip2.rs"), "fn skipped2() {}\n").unwrap();

        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "tester".into(),
                workdir: Some(workdir.clone()),
            },
            client: Client::new(
                dir.path().join("unused.sock"),
                dir.path().join("config"),
                "test-cap".into(),
            ),
            profile: McpProfile::Full,
        };

        // 1. Write a nested file (auto-creating parent directories).
        let write_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "write",
                    "path": "src/engine.rs",
                    "content": "pub fn alpha() -> i32 {\n    10\n}\npub fn beta() -> i32 {\n    20\n}\n"
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(write_res["action"], "write");
        assert_eq!(write_res["bytes_written"], 65);
        let initial_sha = write_res["sha256"].as_str().unwrap().to_string();
        assert_eq!(initial_sha.len(), 64);

        // 2. Append to the file.
        let append_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "write",
                    "path": "src/engine.rs",
                    "content": "pub fn gamma() -> i32 {\n    30\n}\n",
                    "append": true
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(append_res["append"], true);
        assert_ne!(append_res["sha256"].as_str().unwrap(), initial_sha);

        // 3. Read line range.
        let read_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "read",
                    "path": "src/engine.rs",
                    "start_line": 4,
                    "end_line": 6
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(read_res["start_line"], 4);
        assert_eq!(read_res["end_line"], 6);
        assert_eq!(read_res["total_lines"], 9);
        assert_eq!(read_res["content"], "pub fn beta() -> i32 {\n    20\n}");
        assert!(
            read_res["numbered"]
                .as_str()
                .unwrap()
                .contains("4: pub fn beta()")
        );

        // 4. Atomic multi_edit: if second edit fails, first edit must NOT be written to disk.
        let failed_edit = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "multi_edit",
                    "path": "src/engine.rs",
                    "edits": [
                        { "target": "    10", "replacement": "    100" },
                        { "target": "pub fn beta() -> u64", "replacement": "pub fn beta() -> i64" }
                    ]
                }))
                .unwrap(),
            ))
            .await;
        let err_msg = match failed_edit {
            Err(msg) => msg,
            Ok(_) => panic!("expected missing target edit to fail"),
        };
        assert!(err_msg.contains("target` not found"), "{err_msg}");
        assert!(err_msg.contains("4: pub fn beta() -> i32"), "{err_msg}");
        let unchanged = std::fs::read_to_string(workdir.join("src/engine.rs")).unwrap();
        assert!(unchanged.contains("    10\n"));

        // 5. Valid multi_edit succeeds atomically.
        let edit_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "multi_edit",
                    "path": "src/engine.rs",
                    "edits": [
                        { "target": "    10", "replacement": "    100" },
                        { "target": "    20", "replacement": "    200\n    + 1" }
                    ]
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(edit_res["replacements_made"], 2);
        assert_eq!(edit_res["lines_removed"], 2);
        assert_eq!(edit_res["lines_added"], 3);

        // 6. Glob respects .gitignore and skips target/.git by default.
        let glob_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "glob",
                    "pattern": "**/*.rs"
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(glob_res["files"], json!(["src/engine.rs"]));

        // 7. Grep with regex and context_lines.
        let grep_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "grep",
                    "query": r"pub fn (beta|gamma)\(\)",
                    "glob": "*.rs",
                    "context_lines": 1
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        let hits = grep_res["matches"].as_array().unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["path"], "src/engine.rs");
        assert_eq!(hits[0]["line"], 4);
        assert_eq!(hits[0]["context_after"], json!(["    200"]));
        assert_eq!(hits[1]["line"], 8);

        // 8. Null bytes in path are rejected.
        let null_err = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "read",
                    "path": "bad\0path.rs"
                }))
                .unwrap(),
            ))
            .await;
        assert!(null_err.is_err());
    }

    #[tokio::test]
    async fn code_delivery_receipts_are_host_observed_not_snippet_authored() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("proof.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "receipt-worker".into(),
                workdir: Some(dir.path().into()),
            },
            client: Client::new(
                socket,
                dir.path().join("config"),
                "synthetic-receipt-capability".into(),
            ),
            profile: McpProfile::Full,
        };
        let fabricated = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                console.log('send(');
                try { await anyagents.send({to: 123}); } catch {}
                return {deliveries: [{to:'agent:parent', id:'invented'}]};
            "#
                .into(),
                timeout_ms: Some(15000),
            }))
            .await
            .unwrap()
            .0;
        assert_eq!(fabricated["result"]["deliveries"][0]["id"], "invented");
        assert_eq!(fabricated["deliveries"], json!([]));
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                request.push(stream.read_u8().await.unwrap());
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let header = String::from_utf8(request).unwrap();
            let mut request_line = header.lines().next().unwrap().split_whitespace();
            assert_eq!(request_line.next(), Some("POST"));
            let uri: hyper::Uri = request_line.next().unwrap().parse().unwrap();
            assert_eq!(uri.path(), "/v1/send");
            let len: usize = header
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|n| n.trim().parse().unwrap())
                })
                .unwrap();
            let mut body = vec![0; len];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["text"],
                "real delivery"
            );
            let body = json!({"deliveries":[{"to":"agent:parent","id":"msg-observed","seq":1,"duplicate":false}]}).to_string();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body.as_bytes()).await.unwrap();
        });
        let observed = mcp
            .code(Parameters(CodeArgs {
                code:
                    "await anyagents.send({to:'agent:parent', text:'real delivery'}); return null;"
                        .into(),
                timeout_ms: Some(15000),
            }))
            .await
            .unwrap()
            .0;
        assert_eq!(observed["result"], serde_json::Value::Null);
        assert_eq!(observed["deliveries"][0]["id"], "msg-observed");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn code_tool_executes_typescript_parallel_rpc_and_blocks_process_and_child_process() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let workdir = dir.path().join("work");
        std::fs::create_dir_all(&workdir).unwrap();
        std::fs::write(
            workdir.join("notes.txt"),
            "alpha-line\nbeta-target-line\ngamma-line\n",
        )
        .unwrap();

        let socket = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let parent_id = AgentId::generate();
        let population = vec![
            dummy_agent_view("agt_1", "alpha-worker", "running", "working", Some("First")),
            dummy_agent_view("agt_2", "beta-worker", "idle", "idle", Some("Second")),
        ];
        let body = serde_json::to_vec(&AgentsResponse {
            archived_count: 0,
            agents: population,
            gateway: None,
        })
        .unwrap();
        let (served, server) = tokio::sync::oneshot::channel::<()>();
        let later = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let byte = stream.read_u8().await.unwrap();
                request.push(byte);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            drop(stream);
            let _ = served.send(());
            // The server stays up, as a real one does: what it does not
            // serve here (memory reads) is answered `404`, and the tools fall
            // back to the agent's own files.
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    match stream.read_u8().await {
                        Ok(byte) => request.push(byte),
                        Err(_) => break,
                    }
                }
                let body = br#"{"error":"not served here"}"#;
                let head = format!(
                    "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body).await;
            }
        });

        let mcp = AgentMcp {
            agent: AgentRef {
                id: parent_id,
                name: "parent".into(),
                workdir: Some(workdir.clone()),
            },
            client: Client::new(
                socket,
                dir.path().join("config"),
                "synthetic-test-capability".into(),
            ),
            profile: McpProfile::Full,
        };

        // 1. Parallel Promise.all across `tools.anyagents.agents` and `tools.anyagents.files` / `tools.workspace` with TypeScript syntax.
        let code_res = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                    interface Summary {
                        activeAgents: string[];
                        matchedLines: number[];
                        editedSha: string;
                    }
                    const [roster, grepRes, editRes] = await Promise.all([
                        tools.anyagents.agents({ action: "list", state: "all" }),
                        tools.workspace.grep({ query: "beta-target", path: "notes.txt" }),
                        tools.workspace.write({ path: "computed.txt", content: "ok\n" }),
                    ]);
                    console.log("parallel calls done", roster.agents.length);
                    const out: Summary = {
                        activeAgents: roster.agents
                            .filter((a: any) => a.state === "running")
                            .map((a: any) => a.name),
                        matchedLines: grepRes.matches.map((m: any) => m.line),
                        editedSha: editRes.sha256,
                    };
                    return out;
                "#
                .to_string(),
                timeout_ms: Some(15_000),
            }))
            .await
            .unwrap()
            .0;

        assert_eq!(code_res["truncated"], false);
        assert_eq!(code_res["result"]["activeAgents"], json!(["alpha-worker"]));
        assert_eq!(code_res["result"]["matchedLines"], json!([2]));
        assert_eq!(code_res["logs"], json!(["parallel calls done 2"]));
        assert!(workdir.join("computed.txt").is_file());
        server.await.unwrap();
        let _serving = &later;

        // 2. Verify capability sandbox blocks `process`, `process.env`, `require`, `child_process`, `fs`, `fetch`, and Function constructor escapes.
        let sandbox_probe = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                    let constructorEscapeBlocked = false;
                    try {
                        (tools as any).constructor.constructor("return process")();
                    } catch {
                        constructorEscapeBlocked = true;
                    }
                    let fnEscapeBlocked = false;
                    try {
                        tools.anyagents.files.constructor("return process")();
                    } catch {
                        fnEscapeBlocked = true;
                    }
                    return {
                        typeofProcess: typeof (globalThis as any).process,
                        typeofRequire: typeof (globalThis as any).require,
                        typeofChildProcess: typeof (globalThis as any).child_process,
                        typeofFs: typeof (globalThis as any).fs,
                        typeofFetch: typeof (globalThis as any).fetch,
                        typeofBun: typeof (globalThis as any).Bun,
                        constructorEscapeBlocked,
                        fnEscapeBlocked,
                    };
                "#
                .to_string(),
                timeout_ms: Some(10_000),
            }))
            .await
            .unwrap()
            .0;

        assert_eq!(
            sandbox_probe["result"],
            json!({
                "typeofProcess": "undefined",
                "typeofRequire": "undefined",
                "typeofChildProcess": "undefined",
                "typeofFs": "undefined",
                "typeofFetch": "undefined",
                "typeofBun": "undefined",
                "constructorEscapeBlocked": true,
                "fnEscapeBlocked": true,
            })
        );

        // 3. Direct attempt to call `require("child_process")` or read `process.env` throws an error.
        for forbidden_snippet in [
            r#"return require("child_process").execSync("id").toString();"#,
            r#"return process.env.PATH;"#,
        ] {
            let res = mcp
                .code(Parameters(CodeArgs {
                    code: forbidden_snippet.to_string(),
                    timeout_ms: Some(5_000),
                }))
                .await;
            assert!(
                res.is_err(),
                "expected `{forbidden_snippet}` to fail in sandbox"
            );
        }

        // Oversized compact Unicode JSON is reconstructed through the supported
        // code -> workspace.read consumer, not by opening the spill in the test.
        let expected = serde_json::json!({"text":"é漢字\"\\".repeat(20_000),"meta":{"ok":true}});
        let huge_res = mcp
            .code(Parameters(CodeArgs {
                code: r#"return {text:'é漢字"\\'.repeat(20000),meta:{ok:true}};"#.to_string(),
                timeout_ms: Some(10_000),
            }))
            .await
            .unwrap()
            .0;
        assert_eq!(huge_res["truncated"], false);
        assert_eq!(huge_res["offloaded"], true);
        // Independently recover a complete MCP frame (not just a code value),
        // including structured content and the original error status.
        let frame_expected = serde_json::json!({
            "jsonrpc":"2.0", "id":"unicode-frame",
            "result":{"content":[{"type":"text","text":"\u{0001}é漢字\"\\".repeat(20_000)}],
                      "isError":true,"structuredContent":{"nested":[1,{"preserved":true}]}}
        });
        let full_frame = serde_json::to_vec(&frame_expected).unwrap();
        let mut gate = FrameArtifactStdout::with_spill_dir(Vec::new(), workdir.clone());
        gate.write_all(&full_frame).await.unwrap();
        gate.write_all(b"\n").await.unwrap();
        gate.flush().await.unwrap();
        let reference: serde_json::Value = serde_json::from_slice(&gate.inner).unwrap();
        assert_eq!(reference["id"], "unicode-frame");
        assert_eq!(reference["result"]["isError"], true);
        let reference: serde_json::Value =
            serde_json::from_str(reference["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        let frame_res = serde_json::json!({"result_file":reference["artifact"]["path"],"artifact":reference["artifact"]});
        for (expected, huge_res) in [(expected, huge_res), (frame_expected, frame_res)] {
            let path = huge_res["result_file"].as_str().unwrap();
            let mut rebuilt = String::new();
            let mut offset = 0;
            loop {
                let code = format!(
                    "const p=await workspace.read({{path:{},byte_offset:{offset},max_bytes:4096}}); return {{content:p.content,byte_offset:p.byte_offset,bytes_read:p.bytes_read,total_bytes:p.total_bytes,sha256:p.sha256,truncated:p.truncated}};",
                    serde_json::to_string(path).unwrap()
                );
                let page = mcp
                    .code(Parameters(CodeArgs {
                        code,
                        timeout_ms: Some(10_000),
                    }))
                    .await
                    .unwrap()
                    .0;
                assert!(
                    page.get("result_file").is_none(),
                    "reader page offloaded: {page}"
                );
                let frame = serde_json::to_vec(&serde_json::json!({
                "jsonrpc":"2.0", "id":offset,
                "result":{"content":[{"type":"text","text":serde_json::to_string(&page).unwrap()}],"isError":false}
            })).unwrap();
                assert!(frame.len() < MCP_TEXT_SPILL_BYTES);
                let mut gate = FrameArtifactStdout::with_spill_dir(Vec::new(), workdir.clone());
                use tokio::io::AsyncWriteExt as _;
                gate.write_all(&frame).await.unwrap();
                gate.write_all(b"\n").await.unwrap();
                gate.flush().await.unwrap();
                assert_eq!(&gate.inner[..frame.len()], frame.as_slice());
                assert_eq!(gate.inner.len(), frame.len() + 1);
                let page = &page["result"];
                assert_eq!(page["byte_offset"].as_u64().unwrap(), offset);
                let content = page["content"].as_str().unwrap();
                assert_eq!(content.len() as u64, page["bytes_read"].as_u64().unwrap());
                rebuilt.push_str(content);
                offset += page["bytes_read"].as_u64().unwrap();
                assert_eq!(page["sha256"], huge_res["artifact"]["sha256"]);
                assert_eq!(page["total_bytes"], huge_res["artifact"]["bytes"]);
                if page["truncated"] == false {
                    break;
                }
                assert!(!content.is_empty());
            }
            assert_eq!(
                rebuilt.len() as u64,
                huge_res["artifact"]["bytes"].as_u64().unwrap()
            );
            assert_eq!(
                sha256_hex(rebuilt.as_bytes()),
                huge_res["artifact"]["sha256"]
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&rebuilt).unwrap(),
                expected
            );
        }
        // Encoded media nested inside code results remains available inline.
        let media = mcp
            .code(Parameters(CodeArgs {
                code:
                    r#"return {nested:{type:'image',mimeType:'image/png',data:'A'.repeat(70000)}};"#
                        .to_string(),
                timeout_ms: Some(10_000),
            }))
            .await
            .unwrap()
            .0;
        assert_eq!(
            media["result"]["nested"]["data"].as_str().unwrap().len(),
            70000
        );
        assert!(media.get("result_file").is_none());

        // 5. Pure Code Mode exposes only `code` in CANONICAL_MCP_TOOLS, and `anyagents.memory` + `workspace.{list,patch,edit}` work end-to-end inside `code`.
        assert_eq!(CANONICAL_MCP_TOOLS, &["code"]);
        std::fs::write(
            workdir.join("indent.rs"),
            "fn outer() {\n        let value = 41;\n        println!(\"{value}\");\n}\n",
        )
        .unwrap();

        let evo_res = mcp
            .code(Parameters(CodeArgs {
                code: r##"
                    await anyagents.memory.write({
                        topic: "build-invariants",
                        description: "Verified build and deployment invariants",
                        content: "# Build Invariants\n\nAlways verify live receipts.\n",
                    });
                    const readTopic = await anyagents.memory.read("build-invariants");
                    const listedMem = await anyagents.memory.list();
                    const searchedMem = await anyagents.memory.search("receipts");
                    const deletedMem = await anyagents.memory.delete("build-invariants");

                    // Indentation-tolerant edit (target has 0 spaces, file has 8 spaces)
                    await workspace.edit({
                        path: "indent.rs",
                        target: "let value = 41;\nprintln!(\"{value}\");",
                        replacement: "let value = 42;\nprintln!(\"answer={value}\");",
                    });

                    // V4A patch (Add + Update)
                    await workspace.patch([
                        "*** Begin Patch",
                        "*** Add File: src/patched.txt",
                        "+line one",
                        "+line two",
                        "*** Update File: src/patched.txt",
                        "@@",
                        " line one",
                        "-line two",
                        "+line two updated",
                        "*** End Patch",
                    ].join("\n"));

                    // Unified diff patch
                    await workspace.patch([
                        "--- a/src/patched.txt",
                        "+++ b/src/patched.txt",
                        "@@ -1,2 +1,3 @@",
                        " line one",
                        " line two updated",
                        "+line three unified",
                    ].join("\n"));

                    const dirList = await workspace.list("src");
                    const patchedRead = await workspace.read("src/patched.txt");
                    const indentRead = await workspace.read("indent.rs");
                    const execRes = await tools.workspace.exec({ cmd: "printf 'hello-%s' exec", cwd: "src" });
                    const hasWebHelpers = typeof tools.web.search === "function"
                        && typeof tools.web.fetch === "function"
                        && typeof web.search === "function"
                        && typeof anyagents.web.fetch === "function";

                    return {
                        topicSlug: readTopic.topic,
                        hasFact: readTopic.content.includes("Always verify live receipts."),
                        listedCount: listedMem.count,
                        searchCount: searchedMem.count,
                        deleted: deletedMem.deleted,
                        dirNames: dirList.entries.map((e: any) => e.name),
                        patchedContent: patchedRead.content,
                        indentContent: indentRead.content,
                        execExit: execRes.exit_code,
                        execStdout: execRes.stdout,
                        hasWebHelpers,
                    };
                "##
                .to_string(),
                timeout_ms: Some(15_000),
            }))
            .await
            .unwrap()
            .0;

        assert_eq!(evo_res["result"]["topicSlug"], "build-invariants");
        assert_eq!(evo_res["result"]["hasFact"], true);
        assert_eq!(evo_res["result"]["listedCount"], 1);
        assert_eq!(evo_res["result"]["searchCount"], 1);
        assert_eq!(evo_res["result"]["deleted"], true);
        assert_eq!(evo_res["result"]["dirNames"], json!(["patched.txt"]));
        assert_eq!(
            evo_res["result"]["patchedContent"],
            "line one\nline two updated\nline three unified"
        );
        assert_eq!(
            evo_res["result"]["indentContent"],
            "fn outer() {\n        let value = 42;\n        println!(\"answer={value}\");\n}"
        );
        assert_eq!(evo_res["result"]["execExit"], 0);
        assert_eq!(evo_res["result"]["execStdout"], "hello-exec");
        assert_eq!(evo_res["result"]["hasWebHelpers"], true);
        later.abort();
    }

    #[tokio::test]
    async fn files_and_code_tools_emit_native_mcp_multimedia_content_blocks() {
        use rmcp::handler::server::tool::IntoCallToolResult as _;
        let dir = tempfile::tempdir().unwrap();
        let workdir = dir.path().join("work");
        std::fs::create_dir_all(&workdir).unwrap();

        // 1. Write a 640x480 PNG, a WAV header, and a minimal PDF file.
        let mut png_bytes = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgba8(image::RgbaImage::new(640, 480))
            .write_to(&mut png_bytes, image::ImageFormat::Png)
            .unwrap();
        let png_bytes = png_bytes.into_inner();
        std::fs::write(workdir.join("frame.png"), &png_bytes).unwrap();

        let mut wav_bytes = Vec::new();
        wav_bytes.extend_from_slice(b"RIFF");
        wav_bytes.extend_from_slice(&36u32.to_le_bytes());
        wav_bytes.extend_from_slice(b"WAVEfmt ");
        wav_bytes.extend_from_slice(&[16, 0, 0, 0, 1, 0, 1, 0]);
        std::fs::write(workdir.join("clip.wav"), &wav_bytes).unwrap();

        let pdf_bytes = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n";
        std::fs::write(workdir.join("spec.pdf"), pdf_bytes).unwrap();

        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "multimedia-worker".into(),
                workdir: Some(workdir.clone()),
            },
            client: Client::new(
                dir.path().join("unused.sock"),
                dir.path().join("config"),
                "test-cap".into(),
            ),
            profile: McpProfile::Full,
        };
        assert!(
            rmcp::ServerHandler::get_info(&mcp)
                .capabilities
                .tools
                .is_some(),
            "stdio ServerHandler::get_info() must advertise tools capability so MCP clients call tools/list"
        );

        // Direct `files` read on PNG returns clean metadata + native MCP Image ContentBlock.
        let MultimediaJson(png_json, png_media) = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "read",
                    "path": "frame.png",
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(png_json["mime_type"], "image/png");
        assert_eq!(png_json["media_kind"], "image");
        assert_eq!(png_json["width"], 640);
        assert_eq!(png_json["height"], 480);
        assert_eq!(png_json["image_attached"], true);
        assert_eq!(png_media.len(), 1);
        match &png_media[0] {
            rmcp::model::ContentBlock::Image(img) => {
                assert_eq!(img.mime_type, "image/png");
                assert_eq!(img.data, crate::computer::base64_encode(&png_bytes));
            }
            other => panic!("expected ContentBlock::Image, got {other:?}"),
        }

        // Calling `workspace.read` inside `code` attaches Image, Audio, and PDF Resource ContentBlocks to the outer `code` tool result!
        let code_multimedia = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                    const [img, aud, doc] = await Promise.all([
                        workspace.read("frame.png"),
                        workspace.read("clip.wav"),
                        workspace.read("spec.pdf"),
                    ]);
                    return {
                        imgDims: [img.width, img.height],
                        audMime: aud.mime_type,
                        docMime: doc.mime_type,
                    };
                "#
                .to_string(),
                timeout_ms: Some(15_000),
            }))
            .await
            .unwrap();

        assert_eq!(code_multimedia.0["result"]["imgDims"], json!([640, 480]));
        assert_eq!(code_multimedia.0["result"]["audMime"], "audio/wav");
        assert_eq!(code_multimedia.0["result"]["docMime"], "application/pdf");
        assert!(
            code_multimedia.1.len() >= 3,
            "expected at least 3 native multimedia content blocks, got {}",
            code_multimedia.1.len()
        );

        let call_tool_resp = code_multimedia.into_call_tool_result().unwrap();
        let rmcp::model::CallToolResponse::Complete(call_res) = call_tool_resp else {
            panic!("expected CallToolResponse::Complete");
        };
        // The JSON goes as text, not also as structured content, which a
        // client that prefers it would give its model instead of the media.
        assert!(call_res.structured_content.is_none());
        assert!(matches!(
            &call_res.content[0],
            rmcp::model::ContentBlock::Text(text) if text.text.contains("\"imgDims\":[640,480]")
        ));
        assert!(call_res.content.iter().any(
            |c| matches!(c, rmcp::model::ContentBlock::Image(i) if i.mime_type == "image/png")
        ));
        assert!(call_res.content.iter().any(
            |c| matches!(c, rmcp::model::ContentBlock::Audio(a) if a.mime_type == "audio/wav")
        ));
        assert!(call_res.content.iter().any(|c| matches!(
            c,
            rmcp::model::ContentBlock::Resource(r)
                if matches!(
                    &r.resource,
                    rmcp::model::ResourceContents::BlobResourceContents { mime_type: Some(m), .. }
                        if m == "application/pdf"
                )
        )));
    }

    /// What each harness's model is given when its agent reads a media path
    /// with `code`, by that vendor's MCP client rules
    /// (`tests/invariants/fakes/mcp_client.py`): the photo itself on every
    /// harness whose client gives MCP images to its model; on AGY, which
    /// gives one only up to 10,240 bytes, the path of the file it offloaded,
    /// which its native `read_file` opens; and a small image inline there.
    #[tokio::test]
    async fn an_image_an_agent_reads_from_the_media_directory_reaches_each_model() {
        use rmcp::handler::server::tool::IntoCallToolResult as _;
        use sha2::Digest as _;
        let dir = tempfile::tempdir().unwrap();
        let workdir = dir.path().join("work");
        let media = dir.path().join(anyagents_harness::media::MEDIA_DIR);
        std::fs::create_dir_all(&workdir).unwrap();
        std::fs::create_dir_all(&media).unwrap();
        // A Telegram photo's shape: a 1280x697 JPEG of tens of kilobytes.
        let jpeg = |width: u32, height: u32| {
            let image = image::RgbImage::from_fn(width, height, |x, y| {
                let noise = (x.wrapping_mul(2_654_435_761) ^ y.wrapping_mul(40_503)) >> 13;
                image::Rgb([(x % 251) as u8, (y % 241) as u8, noise as u8])
            });
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::ImageRgb8(image)
                .write_to(&mut bytes, image::ImageFormat::Jpeg)
                .unwrap();
            bytes.into_inner()
        };
        let photo = jpeg(1280, 697);
        assert!(photo.len() > 10_240, "{} bytes", photo.len());
        let thumb = jpeg(32, 32);
        assert!(thumb.len() <= 10_240, "{} bytes", thumb.len());
        let digest = |bytes: &[u8]| {
            sha2::Sha256::digest(bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "media-reader".into(),
                workdir: Some(workdir.clone()),
            },
            client: Client::new(
                dir.path().join("unused.sock"),
                dir.path().join("config"),
                "test-cap".into(),
            ),
            profile: McpProfile::Full,
        };
        let read = |name: &str, bytes: &[u8]| {
            let path = media.join(name);
            std::fs::write(&path, bytes).unwrap();
            let line = anyagents_harness::media::local_media_line("photo", &path);
            assert!(line.contains(&path.display().to_string()));
            let mcp = mcp.clone();
            async move {
                let result = mcp
                    .code(Parameters(CodeArgs {
                        code: format!(
                            "const img = await workspace.read({{ path: {:?} }}); return img.mime_type;",
                            path.display().to_string()
                        ),
                        timeout_ms: Some(15_000),
                    }))
                    .await
                    .unwrap();
                let rmcp::model::CallToolResponse::Complete(result) =
                    result.into_call_tool_result().unwrap()
                else {
                    panic!("expected a complete result");
                };
                serde_json::to_vec(&result).unwrap()
            }
        };
        let seen_by = |harness: &str, result: &[u8]| -> serde_json::Value {
            use std::io::Write as _;
            let mut child = std::process::Command::new("python3")
                .args(["-I", anyagents_invariants::conformance::MCP_CLIENT, harness])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(result).unwrap();
            let out = child.wait_with_output().unwrap();
            assert!(out.status.success(), "{harness}");
            serde_json::from_slice(&out.stdout).unwrap()
        };
        let photo_result = read("complete_photo_large.jpg", &photo).await;
        for harness in ["claude-code", "codex", "muse", "omp", "opencode"] {
            let seen = seen_by(harness, &photo_result);
            assert_eq!(
                seen["images"],
                json!([{"mime": "image/jpeg", "sha256": digest(&photo)}]),
                "{harness}: {seen}"
            );
        }
        let agy = seen_by("agy", &photo_result);
        assert_eq!(agy["images"], json!([]), "{agy}");
        assert_eq!(agy["offloaded"].as_array().unwrap().len(), 1, "{agy}");
        let thumb_result = read("complete_photo_small.jpg", &thumb).await;
        let agy = seen_by("agy", &thumb_result);
        assert_eq!(
            agy["images"],
            json!([{"mime": "image/jpeg", "sha256": digest(&thumb)}]),
            "{agy}"
        );
    }

    #[tokio::test]
    async fn remote_sandbox_backend_executes_files_and_code_and_never_upgrades_http_mcp_profile() {
        use axum::{
            Router,
            body::Bytes,
            extract::{Path as AxumPath, Query},
            http::{HeaderMap, StatusCode},
            routing::{get, post},
        };
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        #[derive(Default)]
        struct MockSandboxState {
            files: Mutex<HashMap<String, Vec<u8>>>,
            exec_log: Mutex<Vec<serde_json::Value>>,
        }

        let sandbox_state = Arc::new(MockSandboxState::default());
        let app = Router::new()
            .route(
                "/v1/sandboxes/{id}/exec",
                post({
                    let state = Arc::clone(&sandbox_state);
                    move |AxumPath(id): AxumPath<String>,
                          headers: HeaderMap,
                          axum::Json(payload): axum::Json<serde_json::Value>| {
                        let state = Arc::clone(&state);
                        async move {
                            let auth = headers
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("");
                            if auth != "Bearer sbx-token-123" || id != "sbx-42" {
                                return (
                                    StatusCode::UNAUTHORIZED,
                                    axum::Json(json!({"error": "unauthorized"})),
                                );
                            }
                            state.exec_log.lock().unwrap().push(payload.clone());
                            let cmd = payload
                                .get("command")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let cwd = payload.get("cwd").and_then(|v| v.as_str()).unwrap_or("");
                            (
                                StatusCode::OK,
                                axum::Json(json!({
                                    "stdout": format!("remote-exec:{cmd}@{cwd}"),
                                    "stderr": "",
                                    "exit_code": 0
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                "/v1/sandboxes/{id}/files",
                get({
                    let state = Arc::clone(&sandbox_state);
                    move |AxumPath(id): AxumPath<String>,
                          headers: HeaderMap,
                          Query(params): Query<HashMap<String, String>>| {
                        let state = Arc::clone(&state);
                        async move {
                            let auth = headers
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("");
                            if auth != "Bearer sbx-token-123" || id != "sbx-42" {
                                return (
                                    StatusCode::UNAUTHORIZED,
                                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                                    Bytes::from_static(b"{\"error\":\"unauthorized\"}"),
                                );
                            }
                            let path = params.get("path").cloned().unwrap_or_default();
                            if params.get("list").is_some_and(|v| v == "true") {
                                let files = state.files.lock().unwrap();
                                let prefix = format!("{}/", path.trim_end_matches('/'));
                                let mut entries = Vec::new();
                                for (k, v) in files.iter() {
                                    if let Some(rel) = k.strip_prefix(&prefix)
                                        && !rel.contains('/')
                                    {
                                        entries.push(json!({
                                            "name": rel,
                                            "path": rel,
                                            "kind": "file",
                                            "bytes": v.len()
                                        }));
                                    }
                                }
                                let body =
                                    serde_json::to_vec(&json!({ "entries": entries })).unwrap();
                                return (
                                    StatusCode::OK,
                                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                                    Bytes::from(body),
                                );
                            }
                            let files = state.files.lock().unwrap();
                            if let Some(bytes) = files.get(&path) {
                                (
                                    StatusCode::OK,
                                    [(
                                        axum::http::header::CONTENT_TYPE,
                                        "application/octet-stream",
                                    )],
                                    Bytes::from(bytes.clone()),
                                )
                            } else {
                                (
                                    StatusCode::NOT_FOUND,
                                    [(axum::http::header::CONTENT_TYPE, "text/plain")],
                                    Bytes::from_static(b"not found"),
                                )
                            }
                        }
                    }
                })
                .put({
                    let state = Arc::clone(&sandbox_state);
                    move |AxumPath(id): AxumPath<String>,
                          headers: HeaderMap,
                          Query(params): Query<HashMap<String, String>>,
                          body: Bytes| {
                        let state = Arc::clone(&state);
                        async move {
                            let auth = headers
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("");
                            if auth != "Bearer sbx-token-123" || id != "sbx-42" {
                                return StatusCode::UNAUTHORIZED;
                            }
                            let path = params.get("path").cloned().unwrap_or_default();
                            state.files.lock().unwrap().insert(path, body.to_vec());
                            StatusCode::OK
                        }
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join("config");
        let host_workdir = dir.path().join("host-work");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&host_workdir).unwrap();

        // HTTP MCP defaults to the Remote profile (code disabled).
        assert_eq!(McpProfile::resolve_http(None), Ok(McpProfile::Remote));

        // Write sandbox.json into agent config_dir.
        std::fs::write(
            config_dir.join("sandbox.json"),
            serde_json::to_vec(&json!({
                "endpoint": format!("http://{addr}"),
                "sandbox_id": "sbx-42",
                "token": "sbx-token-123",
                "workdir": "/workspace"
            }))
            .unwrap(),
        )
        .unwrap();

        // A sandbox.json never upgrades HTTP MCP: over HTTP the tools would run
        // as the server, so `full` is refused there and served only over stdio.
        assert_eq!(McpProfile::resolve_http(None), Ok(McpProfile::Remote));
        assert!(McpProfile::resolve_http(Some("full")).is_err());

        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "remote-sandbox-worker".into(),
                workdir: Some(host_workdir.clone()),
            },
            client: Client::new(
                dir.path().join("unused.sock"),
                config_dir.clone(),
                "test-cap".into(),
            ),
            profile: McpProfile::Full,
        };

        // 1. Direct `files` write/edit/read/list/exec routes through RemoteSandbox, NOT host_workdir.
        let write_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "write",
                    "path": "src/hello.txt",
                    "content": "hello world\nline two\n",
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(write_res["path"], "/workspace/src/hello.txt");
        assert!(!host_workdir.join("src/hello.txt").exists());

        let edit_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "edit",
                    "path": "src/hello.txt",
                    "target": "hello world",
                    "replacement": "hello remote sandbox",
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(edit_res["replacements_made"], 1);

        let read_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "read",
                    "path": "src/hello.txt",
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(read_res["content"], "hello remote sandbox\nline two");

        // 2. `code` tool's `workspace.read`, `workspace.write`, `workspace.list`, and `workspace.exec` transparently route through RemoteSandbox.
        let code_res = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                    await workspace.write("src/from_code.txt", "created via code");
                    const readBack = await workspace.read("src/hello.txt");
                    const listed = await workspace.list("src");
                    const execOut = await workspace.exec({ cmd: "cargo --version", cwd: "src" });
                    return {
                        readContent: readBack.content,
                        entryCount: listed.count,
                        execStdout: execOut.stdout,
                        execExit: execOut.exit_code,
                    };
                "#
                .to_string(),
                timeout_ms: Some(15_000),
            }))
            .await
            .unwrap()
            .0;

        assert_eq!(
            code_res["result"]["readContent"],
            "hello remote sandbox\nline two"
        );
        assert_eq!(code_res["result"]["entryCount"], 2);
        assert_eq!(
            code_res["result"]["execStdout"],
            "remote-exec:cargo --version@/workspace/src"
        );
        assert_eq!(code_res["result"]["execExit"], 0);

        server_handle.abort();
    }
}

#[cfg(test)]
mod tool_server_runtime_tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// A server that answers every call with an empty JSON object.
    async fn answering_server(socket: &Path) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::UnixListener::bind(socket).unwrap();
        let app = axum::Router::new().fallback(|| async { axum::Json(serde_json::json!({})) });
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        })
    }

    #[tokio::test]
    async fn memory_search_defaults_to_self_without_changing_conversation_search() {
        use axum::{extract::Path as RoutePath, http::Uri, routing::get};
        use serde_json::json;

        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let app = axum::Router::new().route(
            "/v1/agents/{target}/conversation",
            get(
                |RoutePath(target): RoutePath<String>, uri: Uri| async move {
                    assert!(matches!(target.as_str(), "worker" | "all"));
                    assert_eq!(uri.query(), Some("scope=memory&query=needle&limit=20"));
                    axum::Json(json!({"agent": target, "agent_id": target, "turns": []}))
                },
            ),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "worker".into(),
                workdir: Some(dir.path().to_path_buf()),
            },
            client: Client::new(socket, dir.path().join("config"), "test-cap".into()),
            profile: McpProfile::Full,
        };

        for (agent, expected) in [
            (None, "worker"),
            (Some("agent:"), "worker"),
            (Some("self"), "worker"),
            (Some("all"), "all"),
        ] {
            let mut args = json!({"action": "search", "query": "needle"});
            if let Some(agent) = agent {
                args["agent"] = json!(agent);
            }
            let (result, _) = mcp.dispatch_any_tool("memory", args).await.unwrap();
            assert_eq!(result["agent"], expected, "agent argument: {agent:?}");
        }

        let result = mcp
            .conversation(Parameters(ConversationArgs {
                scope: Some("memory".into()),
                query: Some("needle".into()),
                ..ConversationArgs::default()
            }))
            .await
            .unwrap();
        assert_eq!(result.0.agent, "all");
        server.abort();
    }

    #[test]
    fn one_thread_answers_a_call_while_another_waits_on_blocking_work() {
        let runtime = tool_server_runtime().unwrap();
        assert_eq!(
            runtime.handle().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        );
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let socket = dir.path().join("anyagents.sock");
            let server = answering_server(&socket).await;
            let mcp = AgentMcp {
                agent: AgentRef {
                    id: AgentId::generate(),
                    name: "worker".into(),
                    workdir: Some(dir.path().to_path_buf()),
                },
                client: Client::new(socket, dir.path().join("config"), "test-cap".into()),
                profile: McpProfile::Full,
            };
            // A command that blocks for two seconds runs on the blocking
            // pool, so the one runtime thread keeps answering other calls.
            let slow = mcp.dispatch_any_tool(
                "files",
                serde_json::json!({"action": "exec", "cmd": "sleep 2"}),
            );
            let quick = async {
                let started = Instant::now();
                mcp.dispatch_any_tool("schedule", serde_json::json!({"action": "list"}))
                    .await
                    .unwrap();
                started.elapsed()
            };
            let (slow, quick) = tokio::join!(slow, quick);
            slow.unwrap();
            assert!(quick < Duration::from_secs(1), "quick call took {quick:?}");
            server.abort();
        });
    }

    #[tokio::test]
    async fn remote_profile_code_mode_forbids_host_fs_and_supports_sandbox_and_memory() {
        use axum::{
            Router,
            body::Bytes,
            extract::{Path as AxumPath, Query},
            http::{HeaderMap, StatusCode},
            routing::{get, post},
        };
        use serde_json::json;
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join("agents/agt_remote/config");
        let host_workdir = dir.path().join("host-work");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&host_workdir).unwrap();
        let host_secret = host_workdir.join("secret.txt");
        std::fs::write(&host_secret, "coordinator-host-secret").unwrap();

        let socket = dir.path().join("test.sock");
        let uds = tokio::net::UnixListener::bind(&socket).unwrap();
        let uds_srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            loop {
                let Ok((mut stream, _)) = uds.accept().await else {
                    return;
                };
                let mut req = Vec::new();
                while !req.ends_with(b"\r\n\r\n") {
                    match stream.read_u8().await {
                        Ok(b) => req.push(b),
                        Err(_) => break,
                    }
                }
                let body = br#"{"error":"not served here"}"#;
                let head = format!(
                    "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body).await;
            }
        });

        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "remote-parity-worker".into(),
                workdir: Some(host_workdir.clone()),
            },
            client: Client::new(socket, config_dir.clone(), "test-cap".into()),
            profile: McpProfile::Remote,
        };

        // 1. Without a remote sandbox, `McpProfile::Remote` executes host-free `code`
        //    (`memory.*` + `Promise.all`) while failing closed on `workspace.*` and `web.*`.
        let parity_res = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                    await memory.write({
                        topic: "remote-parity",
                        description: "Verified under Remote profile",
                        content: "host-free code mode works",
                    });
                    const [readBack, listed] = await Promise.all([
                        memory.read("remote-parity"),
                        memory.list(),
                    ]);
                    let wsBlocked = false;
                    try {
                        await workspace.read("secret.txt");
                    } catch (e) {
                        wsBlocked = String(e).includes("coordinator-host filesystem access is forbidden");
                    }
                    let webBlocked = false;
                    try {
                        await web.fetch("https://example.com");
                    } catch (e) {
                        webBlocked = String(e).includes("disabled by active MCP profile");
                    }
                    return {
                        content: readBack.content,
                        topicCount: listed.topics.length,
                        wsBlocked,
                        webBlocked,
                    };
                "#
                .into(),
                timeout_ms: Some(15_000),
            }))
            .await
            .unwrap()
            .0;
        assert!(
            parity_res["result"]["content"]
                .as_str()
                .is_some_and(|s| s.contains("host-free code mode works")),
            "{parity_res}"
        );
        assert_eq!(parity_res["result"]["topicCount"], 1);
        assert_eq!(parity_res["result"]["wsBlocked"], true);
        assert_eq!(parity_res["result"]["webBlocked"], true);

        // 2. With a remote sandbox bound, `McpProfile::Remote` executes `workspace.*`
        //    strictly against the remote sandbox (never touching `host_workdir`).
        #[derive(Default)]
        struct Sbx {
            files: Mutex<HashMap<String, Vec<u8>>>,
        }
        let sbx = Arc::new(Sbx::default());
        let app = Router::new()
            .route(
                "/v1/sandboxes/{id}/exec",
                post(|AxumPath(_id): AxumPath<String>, body: Bytes| async move {
                    let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
                    let cmd = v["command"].as_str().unwrap_or("");
                    (
                        StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        serde_json::to_vec(&json!({
                            "stdout": format!("sbx-ok:{cmd}"),
                            "stderr": "",
                            "exit_code": 0
                        }))
                        .unwrap(),
                    )
                }),
            )
            .route(
                "/v1/sandboxes/{id}/files",
                get({
                    let sbx = Arc::clone(&sbx);
                    move |Query(q): Query<HashMap<String, String>>| {
                        let sbx = Arc::clone(&sbx);
                        async move {
                            let path = q.get("path").cloned().unwrap_or_default();
                            if let Some(bytes) = sbx.files.lock().unwrap().get(&path) {
                                (
                                    StatusCode::OK,
                                    [(
                                        axum::http::header::CONTENT_TYPE,
                                        "application/octet-stream",
                                    )],
                                    Bytes::from(bytes.clone()),
                                )
                            } else {
                                (
                                    StatusCode::NOT_FOUND,
                                    [(axum::http::header::CONTENT_TYPE, "text/plain")],
                                    Bytes::from_static(b"not found"),
                                )
                            }
                        }
                    }
                })
                .put({
                    let sbx = Arc::clone(&sbx);
                    move |Query(q): Query<HashMap<String, String>>,
                          _headers: HeaderMap,
                          body: Bytes| {
                        let sbx = Arc::clone(&sbx);
                        async move {
                            let path = q.get("path").cloned().unwrap_or_default();
                            sbx.files.lock().unwrap().insert(path, body.to_vec());
                            StatusCode::OK
                        }
                    }
                }),
            )
            .with_state(());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        std::fs::write(
            config_dir.join("sandbox.json"),
            serde_json::to_vec(&json!({
                "endpoint": format!("http://{addr}"),
                "sandbox_id": "sbx-remote-1",
                "workdir": "/workspace"
            }))
            .unwrap(),
        )
        .unwrap();

        let sbx_res = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                    await workspace.write("hello.txt", "inside-sandbox");
                    const [rd, ex] = await Promise.all([
                        workspace.read("hello.txt"),
                        workspace.exec("echo hi"),
                    ]);
                    return { content: rd.content, stdout: ex.stdout };
                "#
                .into(),
                timeout_ms: Some(15_000),
            }))
            .await
            .unwrap()
            .0;
        assert_eq!(sbx_res["result"]["content"], "inside-sandbox");
        assert_eq!(sbx_res["result"]["stdout"], "sbx-ok:echo hi");
        assert!(!host_workdir.join("hello.txt").exists());

        // 3. Mounted ToolRegistry generates TypeScript declarations and serves JSON-RPC 2.0 batch RPC.
        let dts = mcp.code_mode_ts_declarations();
        for ns in ["workspace", "web", "memory", "workstation", "coordination"] {
            assert!(
                dts.contains(&format!("declare namespace {ns}")),
                "missing namespace `{ns}` in .d.ts"
            );
        }
        let runner = agent_tools::code_mode::CodeModeRunner::new(
            mcp.code_mode_registry_for_inspection().into_dispatcher(),
        );
        let rpc = runner
            .serve_http_rpc("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let endpoint = rpc.endpoint_url();
        let token = rpc.token().to_string();
        let resp_val = tokio::task::spawn_blocking(move || {
            let mut resp = ureq::post(&endpoint)
                .header("authorization", &format!("Bearer {token}"))
                .header("content-type", "application/json")
                .send_json(json!([
                    {"jsonrpc": "2.0", "id": 1, "method": "memory.list", "params": {}},
                    {"jsonrpc": "2.0", "id": 2, "method": "workspace.read", "params": {"path": "hello.txt"}}
                ]))
                .unwrap();
            resp.body_mut().read_json::<serde_json::Value>().unwrap()
        })
        .await
        .unwrap();
        let batch = resp_val.as_array().unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0]["id"], 1);
        assert_eq!(batch[1]["id"], 2);
        assert_eq!(batch[1]["result"]["content"], "inside-sandbox");
        drop(rpc);
        srv.abort();
        uds_srv.abort();
    }
}
