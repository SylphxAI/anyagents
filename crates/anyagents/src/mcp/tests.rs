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
        let tools = tool_definitions_with_roles(None, Some(dir.path()));
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
        let empty_dir_tools = tool_definitions_with_roles(None, Some(&dir.path().join("empty")));
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
        let without_data_dir = tool_definitions_with_roles(None, None);
        assert_eq!(
            serde_json::to_value(&without_data_dir).unwrap(),
            serde_json::to_value(tool_definitions()).unwrap()
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

        // Hydrated `<data>/roles/<role>.md` roles populate both the `agents` tool
        // description and the dynamic `role` input_schema property (`examples` + `description`).
        let hydrated_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(hydrated_dir.path().join("roles")).unwrap();
        std::fs::write(
            hydrated_dir.path().join("roles/auditor.md"),
            "---\nname: auditor\ndescription: Security auditor\n---\nReview security.\n",
        )
        .unwrap();
        let hydrated_tools = tool_definitions_with_roles(None, Some(hydrated_dir.path()));
        let agents_tool = hydrated_tools.iter().find(|t| t.name == "agents").unwrap();
        let agents_desc = agents_tool.description.as_deref().unwrap_or_default();
        assert!(
            agents_desc.contains("`auditor` (Security auditor)"),
            "{agents_desc}"
        );
        let role_prop = &agents_tool.input_schema["properties"]["role"];
        assert_eq!(
            role_prop["examples"],
            serde_json::json!(["general", "auditor"])
        );
        assert!(
            role_prop["description"]
                .as_str()
                .unwrap_or_default()
                .contains("auditor"),
            "{role_prop}"
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

    pub(super) fn dummy_agent_view(
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
            runtime: "local".to_string(),
            remote_budget_micros: None,
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
            secrets: Vec::new(),
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
        let oversized = serde_json::json!({
            "agents": [{"id": "x".repeat(AGENT_LIST_RESULT_BUDGET + 1_000)}],
            "summary": {"matched": 1}
        });
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
                persistent: false,
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
            persistent: false,
            state: "active".into(),
            created_at_ms: 1_794_500_000_000,
        }];
        let single_agent_body = serde_json::to_vec(&child).unwrap();
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
                } else if path.contains("/v1/agents/") {
                    &single_agent_body
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

    #[test]
    fn universal_mcp_surface_exposes_exact_seven_tools_and_blocks_removed_tools() {
        let tools = tool_definitions();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert_eq!(names, UNIVERSAL_MCP_TOOLS);
        assert_eq!(
            names,
            [
                "agents",
                "send",
                "inbox",
                "conversation",
                "memory",
                "schedules",
                "groups",
            ]
        );
        for blocked in [
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
            assert!(
                !profile::allows_tool(blocked),
                "{blocked} must not be allowed on the universal MCP surface"
            );
        }
    }

    #[test]
    fn send_reads_null_placeholders_as_absent() {
        use serde_json::json;
        let args: SendArgs = serde_json::from_value(json!({
            "action": "send",
            "article": "null",
            "bot": "null",
            "checklist": "null",
            "emoji": "null",
            "idempotency_key": "null",
            "location": "null",
            "media": "null",
            "options": "null",
            "poll": "null",
            "quote": "null",
            "reply_to": "null",
            "target": "null",
            "text": "report",
            "to": ["agent:planner", "null"],
        }))
        .unwrap();
        assert!(args.article.is_none() && args.media.is_none() && args.poll.is_none());
        assert!(args.location.is_none() && args.checklist.is_none());
        assert!(args.reply_to.is_none() && args.target.is_none() && args.quote.is_none());
        assert!(args.idempotency_key.is_none() && args.bot.is_none());
        assert!(args.options.is_empty());
        assert_eq!(args.to, vec!["agent:planner".to_string()]);
        assert!(args.emoji.is_none());
        let clear: SendArgs =
            serde_json::from_value(json!({"action": "react", "emoji": "", "target": "msg_1"}))
                .unwrap();
        assert_eq!(clear.emoji.as_deref(), Some(""));
        let article: SendArgs = serde_json::from_value(json!({
            "to": "planner",
            "article": {"title": "Report", "content": "body", "url": "undefined", "author_name": ""},
        }))
        .unwrap();
        let article = article.article.unwrap();
        assert_eq!(article.content.as_deref(), Some("body"));
        assert!(article.url.is_none());
        assert_eq!(article.author_name.as_deref(), Some(""));
        let vote: SendArgs = serde_json::from_value(json!({
            "action": "vote",
            "target": "msg_1",
            "options": ["None", "nil", "Null", "NULL"],
            "quote": "None",
            "reply_to": null,
        }))
        .unwrap();
        assert_eq!(vote.options, vec!["None", "nil", "Null", "NULL"]);
        assert_eq!(vote.quote.as_deref(), Some("None"));
        assert!(vote.reply_to.is_none());
    }

    #[test]
    fn interact_reads_a_null_bot_as_absent_and_keeps_a_named_one() {
        use serde_json::json;
        let unnamed: InteractArgs = serde_json::from_value(json!({
            "action": "react",
            "target": "telegram:chat:-1007:91",
            "emoji": "👍",
            "bot": "null",
        }))
        .unwrap();
        assert!(unnamed.bot.is_none());
        let named: InteractArgs = serde_json::from_value(json!({
            "action": "react",
            "target": "telegram:chat:-1007:91",
            "bot": "@ada_promo_bot",
        }))
        .unwrap();
        assert_eq!(named.bot.as_deref(), Some("@ada_promo_bot"));
    }

    #[test]
    fn relation_words_read_alike_in_any_case() {
        assert_eq!(relation_word("Parent"), Some("parent"));
        assert_eq!(relation_word(" agent:parent "), Some("parent"));
        assert_eq!(relation_word("children"), Some("children"));
        assert_eq!(relation_word("planner"), None);
        assert_eq!(relation_word("agent:parent-of-x"), None);
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
            let result = mcp.dispatch_any_tool("memory", args).await.unwrap();
            assert_eq!(result["agent"], expected, "agent argument: {agent:?}");
            assert!(result.get("matches").is_some(), "missing canonical matches");
            assert!(
                result.get("turns").is_none(),
                "memory(search) must not duplicate hits under turns"
            );
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
            };
            // A command that blocks for two seconds runs on the blocking
            // pool, so the one runtime thread keeps answering other calls.
            let slow = tokio::task::spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(500));
            });
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

    fn agent_view_with_parent(
        id: &str,
        name: &str,
        state: &str,
        turn_state: &str,
        parent: Option<&str>,
    ) -> AgentView {
        let mut view = super::spawn_args_tests::dummy_agent_view(id, name, state, turn_state, None);
        view.parent = parent.map(str::to_string);
        view
    }

    #[tokio::test]
    async fn agents_list_include_descendants_traverses_three_generations_and_respects_limit() {
        use serde_json::json;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("server.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let root_id = AgentId::generate();
        let child_id = AgentId::generate();
        let grandchild_id = AgentId::generate();

        let mut population = vec![
            agent_view_with_parent(root_id.as_str(), "root", "running", "idle", None),
            agent_view_with_parent(
                child_id.as_str(),
                "child-lead",
                "running",
                "idle",
                Some(root_id.as_str()),
            ),
            agent_view_with_parent(
                grandchild_id.as_str(),
                "grandchild-worker",
                "running",
                "working",
                Some(child_id.as_str()),
            ),
            agent_view_with_parent(
                "agt_great_grandchild",
                "great-grandchild",
                "idle",
                "idle",
                Some(grandchild_id.as_str()),
            ),
            agent_view_with_parent("agt_unrelated", "other-root", "running", "idle", None),
        ];
        // Add 35 more direct children so total descendants = 38 (> old 18-row budget cap).
        for idx in 0..35 {
            population.push(agent_view_with_parent(
                &format!("agt_sub_{idx:02}"),
                &format!("sub-{idx:02}"),
                "running",
                "idle",
                Some(root_id.as_str()),
            ));
        }
        let body = serde_json::to_vec(&AgentsResponse {
            archived_count: 0,
            agents: population,
            gateway: None,
        })
        .unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut req = Vec::new();
                loop {
                    req.push(stream.read_u8().await.unwrap());
                    if req.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let hdr = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(hdr.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let mcp = AgentMcp {
            agent: AgentRef {
                id: root_id,
                name: "root".into(),
                workdir: None,
            },
            client: Client::new(socket, dir.path().join("config"), "test-cap".into()),
        };

        // `include_descendants=true` traverses child -> grandchild -> great-grandchild and returns all 38 rows.
        let with_desc = mcp
            .agents(Parameters(
                serde_json::from_value(json!({
                    "action": "list",
                    "parent": "self",
                    "include_descendants": true,
                    "limit": 50
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        let rows = with_desc["agents"].as_array().unwrap();
        assert_eq!(rows.len(), 38);
        assert!(rows.iter().any(|r| r["name"] == "child-lead"));
        assert!(rows.iter().any(|r| r["name"] == "grandchild-worker"));
        assert!(rows.iter().any(|r| r["name"] == "great-grandchild"));
        assert!(!rows.iter().any(|r| r["name"] == "other-root"));

        // Without `include_descendants`, only the 36 direct children appear.
        let direct_only = mcp
            .agents(Parameters(
                serde_json::from_value(json!({
                    "action": "list",
                    "parent": "self",
                    "limit": 50
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        let direct_rows = direct_only["agents"].as_array().unwrap();
        assert_eq!(direct_rows.len(), 36);
        assert!(!direct_rows.iter().any(|r| r["name"] == "grandchild-worker"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn inspect_falls_back_to_single_agent_route_for_archived_agents() {
        use serde_json::json;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("server.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let parent_id = AgentId::generate();
        let mut archived_child = agent_view_with_parent(
            "agt_archived_1",
            "archived-worker",
            "archived",
            "idle",
            Some(parent_id.as_str()),
        );
        archived_child.instructions = Some("ARCHIVED_INSTRUCTIONS".into());
        let single_body = serde_json::to_vec(&archived_child).unwrap();
        let conv_body = serde_json::to_vec(&ConversationResponse {
            retrieval: None,
            agent_id: "agt_archived_1".into(),
            agent: "archived-worker".into(),
            turns: Vec::new(),
        })
        .unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut req = Vec::new();
                loop {
                    req.push(stream.read_u8().await.unwrap());
                    if req.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let line = std::str::from_utf8(&req).unwrap().lines().next().unwrap();
                let uri: hyper::Uri = line.split_whitespace().nth(1).unwrap().parse().unwrap();
                let path = uri.path();
                let body = if path == "/v1/agents/archived-worker" {
                    &single_body
                } else {
                    &conv_body
                };
                let hdr = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(hdr.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });
        let mcp = AgentMcp {
            agent: AgentRef {
                id: parent_id,
                name: "parent".into(),
                workdir: None,
            },
            client: Client::new(socket, dir.path().join("config"), "test-cap".into()),
        };
        let inspected = mcp
            .agents(Parameters(
                serde_json::from_value(json!({"action": "inspect", "name": "archived-worker"}))
                    .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(inspected["agent"]["id"], "agt_archived_1");
        assert_eq!(inspected["agent"]["state"], "archived");
        assert_eq!(inspected["agent"]["instructions"], "ARCHIVED_INSTRUCTIONS");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn memory_search_and_recall_return_single_canonical_matches_array() {
        use axum::{http::StatusCode, response::IntoResponse, routing::get};
        use serde_json::json;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("memory")).unwrap();
        let note = "---\ndescription: x\n---\nlocal needle\n";
        std::fs::write(dir.path().join("memory/n.md"), note).unwrap();
        let sock = dir.path().join("anyagents.sock");
        let lis = tokio::net::UnixListener::bind(&sock).unwrap();
        let app = axum::Router::new().route(
            "/v1/agents/{t}/conversation",
            get(|axum::extract::Path(t): axum::extract::Path<String>| async move {
                if t == "down" {
                    return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(json!({ "error": "down" }))).into_response();
                }
                let turn = json!({
                    "seq": 1, "id": "memory:agt_1:topic", "direction": "in",
                    "from": "agent:w", "to": "agent:w", "reply_to": "topic.md",
                    "text": "deployment needle", "timestamp_ms": 1700000000000i64, "source": "memory"
                });
                axum::Json(json!({ "agent": t, "agent_id": t, "turns": [turn] })).into_response()
            }),
        );
        let srv = tokio::spawn(async move { axum::serve(lis, app).await.unwrap() });
        let mk = |name: &str| AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: name.into(),
                workdir: Some(dir.path().into()),
            },
            client: Client::new(sock.clone(), dir.path().join("config"), "test-cap".into()),
        };
        for (name, act) in [("w", "search"), ("w", "recall"), ("down", "search")] {
            let args = json!({ "action": act, "query": "needle" });
            let res = mk(name).dispatch_any_tool("memory", args).await.unwrap();
            assert_eq!(res["count"], 1);
            assert_eq!(res["matches"].as_array().map(Vec::len), Some(1));
            assert!(res.get("turns").is_none());
        }
        srv.abort();
    }

    #[tokio::test]
    async fn agents_verdict_works_over_mcp_and_enforces_downward_supervision() {
        use anyagents_core::{NewAgent, Store};
        use anyagents_installer::{CommandRunner, InstallError, LineSink};
        use serde_json::json;
        use std::sync::Mutex;

        struct Noop;
        impl CommandRunner for Noop {
            fn run(
                &mut self,
                _: &str,
                _: &[String],
                _: &Path,
                _: &[(&str, &str)],
                _: &mut dyn LineSink,
            ) -> Result<(), InstallError> {
                Ok(())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let add = |n: &str, p: Option<&AgentId>| {
            let mut a = NewAgent::new(n, "codex");
            a.parent_id = p.cloned();
            s.create_agent(a).unwrap()
        };
        let p = add("lead", None);
        let c = add("child-v", Some(&p.id));
        let sib = add("sib-v", Some(&p.id));
        let router = server::build_router(server::AppState {
            store: Arc::clone(&s),
            health: anyagents_supervisor::HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(Noop)),
            auth: crate::auth::Mode::Trusted {
                data_dir: dir.path().into(),
            },
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        });
        let mk = |a: &anyagents_core::Agent| {
            let r = AgentRef {
                id: a.id.clone(),
                name: a.name.clone(),
                workdir: None,
            };
            let pr = crate::auth::Principal::Agent {
                agent_id: a.id.clone(),
                name: a.name.clone(),
            };
            let cfg = anyagents_core::agent_config_dir(dir.path(), &a.id);
            AgentMcp::new_for_http(r, dir.path().join("s.sock"), cfg, pr, router.clone())
        };
        let (pm, cm, sm) = (mk(&p), mk(&c), mk(&sib));
        let v = |k: &str| json!({ "action": "verdict", "name": "child-v", "verdict": k });
        assert!(cm.dispatch_any_tool("agents", v("accepted")).await.is_err());
        assert!(sm.dispatch_any_tool("agents", v("accepted")).await.is_err());
        assert!(pm.dispatch_any_tool("agents", v("bogus")).await.is_err());

        let rw = json!({ "action": "verdict", "agent": "child-v", "verdict": "rework", "reason": "fix" });
        let out = pm.dispatch_any_tool("agents", rw).await.unwrap();
        assert_eq!(
            (&out["kind"], &out["actor"], &out["note"]),
            (&json!("rework"), &json!("agent:lead"), &json!("fix"))
        );
        assert_eq!(s.agent(&c.id).unwrap().state, "running");

        let ok = pm.dispatch_any_tool("agents", v("accepted")).await.unwrap();
        assert_eq!(ok["kind"], "accepted");
        assert_eq!(s.agent(&c.id).unwrap().state, "archived");
    }
}
