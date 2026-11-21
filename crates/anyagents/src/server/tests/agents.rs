use super::super::agents::{DescriptionRequest, get_description_route, set_description_route};
use super::super::*;
use super::helpers::{archetype_validation_state, env_lock};
use crate::create;
use anyagents_core::{Agent, now_ms};
use axum::Json;
use axum::http::StatusCode;

#[test]
fn describe_preserves_scope_and_audits_authorized_policy_updates() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("mind", "codex");
    new.parent_id = Some(parent.id.clone());
    new.role = Some("Mind".into());
    let mind = store.create_agent(new).unwrap();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().to_path_buf(),
        },
    };
    let caller = |agent: &Agent| Principal::Agent {
        agent_id: agent.id.clone(),
        name: agent.name.clone(),
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut expected_audit = Vec::new();
        for (body, before, after, source) in [
            (serde_json::json!({"scope":"siblings"}), "private", "siblings", "scope"),
            (serde_json::json!({"description":"Public charter", "public":false}), "siblings", "siblings", "public"),
            (serde_json::json!({"instructions":"Private instructions"}), "siblings", "siblings", ""),
            (serde_json::json!({"role":"Updated Mind"}), "siblings", "siblings", ""),
            (serde_json::json!({"public":true}), "siblings", "public", "public"),
            (serde_json::json!({"public":false}), "public", "private", "public"),
            (serde_json::json!({"scope":"isolated", "public":true}), "private", "isolated", "scope"),
            (serde_json::json!({"public":false}), "isolated", "isolated", "public"),
            (serde_json::json!({"scope":"private"}), "isolated", "private", "scope"),
            (serde_json::json!({"public":false}), "private", "private", "public"),
            (serde_json::json!({"scope":"siblings"}), "private", "siblings", "scope"),
        ] {
            let Json(updated) = set_description_route(
                caller(&parent), State(state.clone()), AxumPath("mind".into()),
                Json(serde_json::from_value(body).unwrap()),
            ).await.unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
            assert_eq!(updated["scope"], after);
            if !source.is_empty() {
                expected_audit.push(("agent:parent".to_string(), before.to_string(), after.to_string(), source.to_string()));
            }
        }
        let before = store.agent(&mind.id).unwrap();
        let outsider = store.create_agent(anyagents_core::NewAgent::new("outsider", "codex")).unwrap();
        let mut sibling_new = anyagents_core::NewAgent::new("sibling", "codex");
        sibling_new.parent_id = Some(parent.id.clone());
        let sibling = store.create_agent(sibling_new).unwrap();
        for agent in [&outsider, &sibling] {
            for body in [serde_json::json!({"scope":"public"}), serde_json::json!({"description":"Overwrite", "instructions":"Overwrite"})] {
                let err = set_description_route(caller(agent), State(state.clone()), AxumPath("mind".into()), Json(serde_json::from_value(body).unwrap()))
                    .await.expect_err("unrelated caller must be refused");
                assert_eq!(err.status, StatusCode::FORBIDDEN);
                assert_eq!(store.agent(&mind.id).unwrap(), before);
            }
        }
        for self_caller in [&mind, &parent] {
            for body in [
                serde_json::json!({"scope":"public"}),
                serde_json::json!({"description":"Self-modified description"}),
                serde_json::json!({"instructions":"Self-modified instructions"}),
            ] {
                let err = set_description_route(caller(self_caller), State(state.clone()), AxumPath("self".into()), Json(serde_json::from_value(body).unwrap()))
                    .await.expect_err("non-admin self cannot mutate description, instructions, or scope");
                assert_eq!(err.status, StatusCode::FORBIDDEN);
            }
            let Json(read_self) = get_description_route(caller(self_caller), State(state.clone()), AxumPath("self".into()))
                .await.expect("self can read own description");
            assert_eq!(read_self["name"], self_caller.name);
        }
        assert_eq!(store.agent(&mind.id).unwrap(), before);

        // Operator update on a root agent is allowed and leaves an atomic audit row.
        let _ = set_description_route(Principal::Operator, State(state.clone()), AxumPath("parent".into()), Json(serde_json::from_value(serde_json::json!({"scope":"isolated", "description":"Operator charter"})).unwrap()))
            .await.unwrap_or_else(|e| panic!("{}", e.message));
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        let mut stmt = conn.prepare("SELECT actor,before_scope,after_scope,source FROM admin_audit WHERE target='mind' AND source IS NOT NULL ORDER BY id").unwrap();
        let rows: Vec<(String,String,String,String)> = stmt.query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap().map(Result::unwrap).collect();
        assert_eq!(rows, expected_audit);
        let op_audit: (String,String,String,String) = conn.query_row("SELECT actor,before_scope,after_scope,source FROM admin_audit WHERE target='parent' AND source IS NOT NULL", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
        assert_eq!(op_audit, ("operator".into(),"public".into(),"isolated".into(),"scope".into()));
        assert_eq!(store.agent(&mind.id).unwrap().description.as_deref(), Some("Public charter"));
        // Every runtime-config pass reads the store's policy; none owns it.
        for _ in 0..2 {
            let current = store.agent(&mind.id).unwrap();
            let config = anyagents_core::agent_config_dir(dir.path(), &mind.id);
            let work = anyagents_core::agent_work_dir(dir.path(), &mind.id);
            create::reconcile_agent_runtime_config(&store, dir.path(), &current, &config, &work).unwrap();
            let reloaded = Store::open(dir.path().join("state.db")).unwrap().agent(&mind.id).unwrap();
            assert_eq!(reloaded.scope(), "siblings");
            assert_eq!(reloaded.description, current.description);
        }
    });
}

#[test]
fn describe_instruction_update_marks_rotation_and_sets_pending_switch() {
    let dir = tempfile::tempdir().unwrap();
    let state = archetype_validation_state(dir.path());
    let ws = dir.path().join("ws");
    std::fs::create_dir_all(ws.join(".agents/agents")).unwrap();
    std::fs::write(
        ws.join(".agents/agents/specialist.md"),
        "---\nname: specialist\ndescription: Role summary\n---\nRole SSOT instructions.",
    )
    .unwrap();
    let mut new = anyagents_core::NewAgent::new("spiron-worker", "spiron");
    new.role = Some("specialist".into());
    new.workdir = Some(ws.display().to_string());
    let created = state.store.create_agent(new).unwrap();
    let running = state
        .store
        .set_agent_running(&created.id, &ws.display().to_string())
        .unwrap();
    let cfg = anyagents_core::agent_config_dir(dir.path(), &running.id);
    create::sync_agent_description_files(&state.store, &running, &cfg).unwrap();
    let started_ms = now_ms();
    state.health.publish(
        &running.id,
        anyagents_supervisor::LiveState {
            process_alive: true,
            turn_state: anyagents_harness::TurnState::Idle,
            delivery: Some(crate::vendor::declared_delivery("spiron")),
            quota_reset_at_ms: None,
            restart_at_ms: None,
            pid: Some(4242),
            live_harness: Some("spiron".to_string()),
            live_model: running.model.clone(),
            process_started_at_ms: Some(started_ms),
            last_error: None,
            retry_at_ms: None,
        },
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let Json(resp) = rt
        .block_on(set_description_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("spiron-worker".to_string()),
            Json(DescriptionRequest {
                description: None,
                instructions: Some("Custom task v2.".to_string()),
                role: None,
                scope: None,
                lifecycle: None,
                public: None,
                clear: None,
            }),
        ))
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
    assert_eq!(
        resp["instructions"].as_str(),
        Some(
            format!(
                "{}\n\nRole SSOT instructions.\n\nCustom task v2.",
                create::BUILTIN_GENERAL_ROLE_BODY
            )
            .as_str()
        )
    );
    let prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(prompt.contains("Role SSOT instructions."));
    assert!(prompt.contains("Custom task v2."));
    let marker: i64 = std::fs::read_to_string(cfg.join(create::INSTRUCTIONS_ROTATED_MARKER))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(marker > started_ms, "marker={marker} started={started_ms}");
    let updated = state.store.agent(&running.id).unwrap();
    assert_eq!(updated.instructions.as_deref(), Some("Custom task v2."));
    file_cache::cache().catch_up();
    let view = agent_view(&state.store, &state.health, &updated).unwrap();
    assert!(view.pending_switch);
    assert_eq!(
        view.instructions.as_deref(),
        Some(
            format!(
                "{}\n\nRole SSOT instructions.\n\nCustom task v2.",
                create::BUILTIN_GENERAL_ROLE_BODY
            )
            .as_str()
        )
    );
}
