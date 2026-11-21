use super::super::helpers::decode_env;
use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn legacy_harness_rows_migrate_idempotently_without_changing_agent_state() {
    let (dir, store) = setup();
    let parent = agent(&store, "parent");
    let mut expected = Vec::new();
    for (index, legacy) in ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default"]
        .into_iter()
        .enumerate()
    {
        let mut new = NewAgent::new(format!("legacy-{index}"), "spiron");
        new.parent_id = Some(parent.id.clone());
        new.model = Some("sylphx/openai/gpt-6.1-sol#medium".into());
        new.workdir = Some(format!("/work/legacy-{index}"));
        new.instructions = Some("preserve instructions".into());
        let created = store.create_agent(new).unwrap();
        store.set_agent_state(&created.id, "paused").unwrap();
        store
            .receive(&created.id, Incoming::new("delivered"))
            .unwrap();
        store.note_delivered(&created.id, 1).unwrap();
        store
            .receive(&created.id, Incoming::new("pending"))
            .unwrap();
        let schedule = store
            .create_schedule(NewSchedule {
                agent_id: created.id.clone(),
                kind: "at".into(),
                spec: "2099-01-01T00:00:00Z".into(),
                prompt: "preserve schedule".into(),
                interval_ms: None,
                next_fire_at_ms: Some(4_070_908_800_000),
                watch_baseline: None,
            })
            .unwrap();
        let before = store.agent(&created.id).unwrap();
        // Simulate an existing database, bypassing canonical writes.
        store
            .conn()
            .execute(
                "UPDATE agents SET harness = ?1 WHERE id = ?2",
                params![legacy, created.id.as_str()],
            )
            .unwrap();
        expected.push((before, schedule));
    }
    let mut third_party = NewAgent::new("gateway-codex", "codex");
    third_party.model = Some("sylphx/openai/gpt-6.1-sol#xhigh".into());
    let third_party = store.create_agent(third_party).unwrap();
    drop(store);

    for _ in 0..2 {
        let reopened = Store::open(dir.path().join("state.db")).unwrap();
        assert_eq!(reopened.agent(&parent.id).unwrap(), parent);
        assert_eq!(reopened.agent(&third_party.id).unwrap(), third_party);
        for (before, schedule) in &expected {
            assert_eq!(&reopened.agent(&before.id).unwrap(), before);
            assert_eq!(reopened.delivered_cursor(&before.id).unwrap(), 1);
            assert_eq!(
                reopened.next_undelivered(&before.id).unwrap().unwrap().text,
                "pending"
            );
            assert_eq!(
                reopened.get_schedule(&schedule.id).unwrap().as_ref(),
                Some(schedule)
            );
        }
    }
}

#[test]
fn legacy_harness_writes_are_canonical_and_gateway_models_are_unchanged() {
    let (_dir, store) = setup();
    for (index, legacy) in ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default"]
        .into_iter()
        .enumerate()
    {
        let mut new = NewAgent::new(format!("alias-{index}"), legacy);
        new.model = Some("sylphx/openai/gpt-6.1-sol#medium".into());
        let created = store.create_agent(new).unwrap();
        assert_eq!(created.harness, "spiron");
        assert_eq!(
            created.model.as_deref(),
            Some("sylphx/openai/gpt-6.1-sol#medium")
        );
        store.set_agent_harness(&created.id, "codex").unwrap();
        let switched = store.set_agent_harness(&created.id, legacy).unwrap();
        assert_eq!(switched, created);
    }
}

#[test]
fn model_selector_matrix_survives_create_update_harness_switch_and_reopen() {
    let (dir, store) = setup();
    let mut expected = Vec::new();
    for base in [
        "gpt-6.2-sol",
        "gpt-6.2-astra",
        "gpt-6.1-sol",
        "unknown-model",
    ] {
        for prefix in ["openai/", "sylphx/openai/"] {
            for effort in ["low", "medium", "high", "xhigh", "max"] {
                let selector = format!("{prefix}{base}#{effort}");
                let mut new =
                    NewAgent::new(format!("selector-{}", expected.len()), "sylphx/default");
                new.model = Some(selector.clone());
                let created = store.create_agent(new).unwrap();
                assert_eq!(created.model.as_deref(), Some(selector.as_str()));
                let switched = store.set_agent_harness(&created.id, "codex").unwrap();
                assert_eq!(switched.model, created.model);
                store
                    .set_agent_model(&created.id, "vendor/unknown#future")
                    .unwrap();
                store.set_agent_model(&created.id, &selector).unwrap();
                let updated = store.agent(&created.id).unwrap();
                assert_eq!(updated.model.as_deref(), Some(selector.as_str()));
                expected.push((created.id, selector));
            }
        }
    }
    drop(store);
    let reopened = Store::open(dir.path().join("state.db")).unwrap();
    for (id, selector) in expected {
        assert_eq!(
            reopened.agent(&id).unwrap().model.as_deref(),
            Some(selector.as_str())
        );
    }
}

#[test]
fn a_launch_environment_survives_a_round_trip() {
    let env = vec![
        (
            "ANTHROPIC_BASE_URL".to_string(),
            "http://127.0.0.1:41727".to_string(),
        ),
        ("ANTHROPIC_AUTH_TOKEN".to_string(), "abc123".to_string()),
        ("ANTHROPIC_MODEL".to_string(), "claude-sonnet".to_string()),
    ];
    assert_eq!(decode_env(&encode_env(&env)), env);

    // And through the store, read back as an agent would see it.
    let (_dir, store) = setup();
    let alpha = agent(&store, "alpha");
    store.set_agent_launch_env(&alpha.id, &env).unwrap();
    let mut expected = env.clone();
    expected.extend([
        ("ANYAGENTS_SCOPE".to_string(), "public".to_string()),
        ("ANYAGENTS_PUBLIC".to_string(), "1".to_string()),
    ]);
    assert_eq!(store.agent(&alpha.id).unwrap().launch_env, expected);

    // A value with a quote, a backslash and a newline keeps its characters.
    let awkward = vec![("K".to_string(), "a\"b\\c\nd".to_string())];
    assert_eq!(decode_env(&encode_env(&awkward)), awkward);
}

#[test]
fn explicit_scope_and_description_survive_launch_env_refresh_and_reopen() {
    let (dir, store) = setup();
    let root = agent(&store, "root");
    let mut new = NewAgent::new("worker", "claude-code");
    new.parent_id = Some(root.id);
    let worker = store.create_agent(new).unwrap();
    let db = dir.path().join("state.db");
    store
        .set_agent_description(&worker.id, Some("Public charter"))
        .unwrap();
    for scope in ["siblings", "isolated", "public", "private"] {
        store.set_agent_scope(&worker.id, scope).unwrap();
        assert_eq!(store.agent(&worker.id).unwrap().scope(), scope);
        // These are the gateway-only environments written at startup and
        // provider refresh; none owns communication policy.
        for port in [41001, 41002] {
            let refreshed = store
                .set_agent_launch_env(
                    &worker.id,
                    &[("OPENAI_BASE_URL".into(), format!("http://127.0.0.1:{port}"))],
                )
                .unwrap();
            assert_eq!(refreshed.scope(), scope);
            assert_eq!(refreshed.description.as_deref(), Some("Public charter"));
            let reopened = Store::open(&db).unwrap().agent(&worker.id).unwrap();
            assert_eq!(reopened.scope(), scope);
            assert_eq!(reopened.description, refreshed.description);
            assert!(
                reopened
                    .launch_env
                    .contains(&("OPENAI_BASE_URL".into(), format!("http://127.0.0.1:{port}"),))
            );
        }
    }
}

#[test]
fn unrelated_agent_updates_preserve_explicit_scope() {
    let (_dir, store) = setup();
    let alpha = agent(&store, "alpha");
    store.set_agent_scope(&alpha.id, "siblings").unwrap();
    store
        .set_agent_description(&alpha.id, Some("Charter"))
        .unwrap();
    store
        .set_agent_instructions(&alpha.id, Some("Instructions"))
        .unwrap();
    store.set_agent_role(&alpha.id, Some("Mind")).unwrap();
    store.set_agent_model(&alpha.id, "model").unwrap();
    store.set_agent_harness(&alpha.id, "codex").unwrap();
    store.rename_agent(&alpha.id, "renamed").unwrap();
    assert_eq!(store.agent(&alpha.id).unwrap().scope(), "siblings");

    let stale = store.agent(&alpha.id).unwrap().launch_env;
    store.set_agent_scope(&alpha.id, "isolated").unwrap();
    // A stale transfer/harness snapshot cannot put the old policy back.
    store.set_agent_launch_env(&alpha.id, &stale).unwrap();
    assert_eq!(store.agent(&alpha.id).unwrap().scope(), "isolated");
}

#[test]
fn legacy_public_revokes_only_public_reachability() {
    let (_dir, store) = setup();
    let parent = agent(&store, "parent");
    for (name, role, default) in [
        ("labelled", Some("Researcher"), "private"),
        ("task", None, "private"),
        ("liaison-assistant", Some("Liaison"), "private"),
    ] {
        let mut new = NewAgent::new(name, "codex");
        new.parent_id = Some(parent.id.clone());
        new.role = role.map(str::to_string);
        let child = store.create_agent(new).unwrap();
        assert_eq!(child.scope(), default);
        for scope in ["siblings", "isolated", "private"] {
            store.set_agent_scope(&child.id, scope).unwrap();
            assert_eq!(
                store.set_agent_public(&child.id, false).unwrap().scope(),
                scope
            );
        }
        assert_eq!(
            store.set_agent_public(&child.id, true).unwrap().scope(),
            "public"
        );
        assert_eq!(
            store.set_agent_public(&child.id, false).unwrap().scope(),
            default
        );
    }
}

#[test]
fn scope_update_rolls_back_if_its_audit_cannot_commit() {
    let (_dir, store) = setup();
    let alpha = agent(&store, "alpha");
    store.set_agent_scope(&alpha.id, "siblings").unwrap();
    let before = store.agent(&alpha.id).unwrap();
    store
        .conn()
        .execute_batch(
            "CREATE TRIGGER refuse_scope_audit BEFORE INSERT ON admin_audit
         WHEN NEW.source IS NOT NULL BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END;",
        )
        .unwrap();
    let err = store
        .set_agent_scope_as(&alpha.id, "public", "agent:parent")
        .unwrap_err();
    assert!(err.to_string().contains("audit unavailable"), "{err}");
    assert_eq!(store.agent(&alpha.id).unwrap(), before);
}

#[test]
fn rejects_a_bad_agent_name_and_a_duplicate() {
    let (_dir, store) = setup();
    assert!(
        store
            .create_agent(NewAgent::new("has space", "claude-code"))
            .is_err()
    );
    agent(&store, "alpha");
    assert!(matches!(
        store
            .create_agent(NewAgent::new("alpha", "claude-code"))
            .unwrap_err(),
        Error::AgentExists(_)
    ));
}

#[test]
fn upgrade_materializes_legacy_effective_scopes_without_changing_work() {
    let (dir, store) = setup();
    let root = agent(&store, "root");
    let mut expected = Vec::new();
    let mut schedules = Vec::new();
    for (name, role, env, scope) in [
        ("labelled", Some("Researcher"), vec![], "siblings"),
        ("plain", None, vec![], "private"),
        ("liaison-assistant", Some("Liaison"), vec![], "isolated"),
        (
            "explicit",
            Some("Researcher"),
            vec![("ANYAGENTS_SCOPE".into(), "private".into())],
            "private",
        ),
        (
            "legacy-public",
            Some("Researcher"),
            vec![("ANYAGENTS_PUBLIC".into(), "1".into())],
            "public",
        ),
        (
            "invalid-scope",
            Some("Researcher"),
            vec![("ANYAGENTS_SCOPE".into(), "unknown".into())],
            "siblings",
        ),
    ] {
        let mut new = NewAgent::new(name, "codex");
        new.parent_id = Some(root.id.clone());
        new.role = role.map(str::to_string);
        new.instructions = Some("User-authored instructions".into());
        let agent = store.create_agent(new).unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        store
            .receive(&agent.id, Incoming::new("completed"))
            .unwrap();
        store.note_delivered(&agent.id, 1).unwrap();
        store.note_turn_completed(&agent.id).unwrap();
        store.receive(&agent.id, Incoming::new("pending")).unwrap();
        schedules.push(
            store
                .create_schedule(NewSchedule {
                    agent_id: agent.id.clone(),
                    kind: "at".into(),
                    spec: "1h".into(),
                    prompt: "Future work".into(),
                    interval_ms: None,
                    next_fire_at_ms: Some(now_ms() + 3_600_000),
                    watch_baseline: None,
                })
                .unwrap(),
        );
        let mut env = env;
        env.push(("CUSTOM".into(), "preserve".into()));
        // Write the exact pre-upgrade flat-array representation.
        store
            .conn()
            .execute(
                "UPDATE agents SET launch_env = ?1 WHERE id = ?2",
                params![encode_env(&env), agent.id.as_str()],
            )
            .unwrap();
        expected.push((store.agent(&agent.id).unwrap(), scope));
    }
    // The upgrade runs when a new process first opens the database.
    drop(store);
    for _ in 0..2 {
        let reopened = Store::open(dir.path().join("state.db")).unwrap();
        for schedule in &schedules {
            assert_eq!(
                reopened.get_schedule(&schedule.id).unwrap().as_ref(),
                Some(schedule)
            );
        }
        for (before, scope) in &expected {
            let after = reopened.agent(&before.id).unwrap();
            assert_eq!(after.scope(), *scope);
            assert!(
                after
                    .launch_env
                    .contains(&("ANYAGENTS_SCOPE".into(), scope.to_string()))
            );
            assert!(
                after
                    .launch_env
                    .contains(&("CUSTOM".into(), "preserve".into()))
            );
            let mut normalized = after.clone();
            normalized.launch_env = before.launch_env.clone();
            assert_eq!(&normalized, before);
            assert_eq!(reopened.delivered_cursor(&before.id).unwrap(), 1);
            assert_eq!(
                reopened.next_undelivered(&before.id).unwrap().unwrap().text,
                "pending"
            );
        }
    }
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let new = store
        .create_agent(NewAgent {
            name: "new-child".into(),
            parent_id: Some(root.id),
            role: Some("Researcher".into()),
            ..NewAgent::new("unused", "codex")
        })
        .unwrap();
    assert_eq!(new.scope(), "private");
}

#[test]
fn rename_agent_preserves_id_grants_history_and_resolves_old_alias_and_role() {
    let (_dir, store) = setup();
    let original_worker = agent(&store, "original_worker");
    store.join_group("owners", &original_worker.id).unwrap();
    store
        .receive(&original_worker.id, Incoming::new("revenue check"))
        .unwrap();

    let renamed = store
        .rename_agent(&original_worker.id, "renamed_worker")
        .unwrap();
    assert_eq!(renamed.id, original_worker.id);
    assert_eq!(renamed.name, "renamed_worker");

    let with_role = store
        .set_agent_role(&original_worker.id, Some("Revenue"))
        .unwrap();
    assert_eq!(with_role.role.as_deref(), Some("Revenue"));

    // Old name resolves via alias table
    let by_old = store.agent_by_name("original_worker").unwrap();
    assert_eq!(by_old.id, original_worker.id);
    assert_eq!(by_old.name, "renamed_worker");
    assert_eq!(by_old.role.as_deref(), Some("Revenue"));

    let by_prefixed = store.agent_by_name("agent:original_worker").unwrap();
    assert_eq!(by_prefixed.id, original_worker.id);

    assert_eq!(
        store.agent_aliases(&original_worker.id).unwrap(),
        vec!["original_worker".to_string()]
    );
    assert_eq!(
        store.group_members("owners").unwrap(),
        vec!["renamed_worker".to_string()]
    );
    assert_eq!(store.inbox(&original_worker.id, None).unwrap().len(), 1);
}
