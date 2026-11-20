//! Tests for first-class `runtime` (`local | sandbox[:provider] | cloud`) and
//! `remote_budget_micros` persistence on agents across Store CRUD and queries.

use anyagents_harness::{ExecutionRuntime, REMOTE_BUDGET_MICROS_ENV_KEY, RUNTIME_ENV_KEY};

use super::{NewAgent, RosterPageQuery, Store};

fn setup() -> (tempfile::TempDir, Store) {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    (dir, store)
}

#[test]
fn create_agent_resolves_orthogonal_harness_runtime_aliases_and_fails_closed() {
    let (dir, store) = setup();

    let spiron_at_cloud = store
        .create_agent(NewAgent::new("sp-at-cloud", "spiron@cloud").with_budget_micros(2_500_000))
        .unwrap();
    assert_eq!(spiron_at_cloud.harness, "spiron");
    assert_eq!(spiron_at_cloud.canonical_harness(), "spiron");
    assert_eq!(spiron_at_cloud.runtime, ExecutionRuntime::Cloud);
    assert_eq!(spiron_at_cloud.execution_runtime(), ExecutionRuntime::Cloud);
    assert_eq!(spiron_at_cloud.remote_budget_micros, Some(2_500_000));
    assert_eq!(spiron_at_cloud.remote_budget_micros(), Some(2_500_000));

    // Deprecated legacy `*-cloud` aliases map cleanly to canonical harness + `Cloud`.
    for (name, legacy, want_harness) in [
        ("legacy-spiron", "spiron-cloud", "spiron"),
        ("legacy-claude", "claude-cloud", "claude-code"),
        ("legacy-openai", "openai-cloud", "codex"),
        ("legacy-cursor", "cursor-cloud", "cursor"),
    ] {
        let agent = store.create_agent(NewAgent::new(name, legacy)).unwrap();
        assert_eq!(agent.harness, want_harness, "{legacy}");
        assert_eq!(agent.runtime, ExecutionRuntime::Cloud, "{legacy}");
        assert_eq!(agent.remote_budget_micros, None, "{legacy}");
    }

    // Explicit sandbox provider + budget round-trip.
    let sbx_runtime = ExecutionRuntime::Sandbox {
        provider: Some("sylphx".into()),
    };
    let sbx = store
        .create_agent(
            NewAgent::new("claude-sbx", "claude-code")
                .with_runtime(sbx_runtime.clone())
                .with_budget_micros(1_250_000),
        )
        .unwrap();
    assert_eq!(sbx.harness, "claude-code");
    assert_eq!(sbx.runtime, sbx_runtime);
    assert_eq!(sbx.remote_budget_micros, Some(1_250_000));

    // Zero budget normalizes to None.
    let zero_budget = store
        .create_agent(NewAgent::new("zero-cap", "spiron").with_budget_micros(0))
        .unwrap();
    assert_eq!(zero_budget.runtime, ExecutionRuntime::Local);
    assert_eq!(zero_budget.remote_budget_micros, None);

    // Unsupported combination fails closed.
    let agy_cloud = store.create_agent(NewAgent::new("bad-agy", "agy@cloud"));
    assert!(agy_cloud.is_err(), "agy@cloud must fail closed");
    assert!(store.agent_by_name("bad-agy").is_err());

    // Conflicting runtime fails closed.
    let conflict = store.create_agent(
        NewAgent::new("bad-conflict", "spiron-cloud").with_runtime(ExecutionRuntime::Local),
    );
    assert!(conflict.is_err(), "spiron-cloud + local must fail closed");

    // Overflowing budget fails closed.
    let mut overflow = NewAgent::new("bad-budget", "spiron");
    overflow.remote_budget_micros = Some(u64::MAX);
    assert!(store.create_agent(overflow).is_err());

    // Reopen database and verify durable readback.
    drop(store);
    let reopened = Store::open(dir.path().join("state.db")).unwrap();
    let got_cloud = reopened.agent(&spiron_at_cloud.id).unwrap();
    assert_eq!(got_cloud.harness, "spiron");
    assert_eq!(got_cloud.runtime, ExecutionRuntime::Cloud);
    assert_eq!(got_cloud.remote_budget_micros, Some(2_500_000));

    let got_sbx = reopened.agent(&sbx.id).unwrap();
    assert_eq!(got_sbx.runtime, sbx_runtime);
    assert_eq!(got_sbx.remote_budget_micros, Some(1_250_000));
}

#[test]
fn set_runtime_harness_and_budget_persist_across_all_read_paths() {
    let (dir, store) = setup();

    let agent = store
        .create_agent(NewAgent::new("worker", "spiron"))
        .unwrap();
    assert_eq!(agent.runtime, ExecutionRuntime::Local);
    assert_eq!(agent.remote_budget_micros, None);

    // Switch runtime Local -> Sandbox(sylphx) -> Cloud.
    let sbx = ExecutionRuntime::Sandbox {
        provider: Some("sylphx".into()),
    };
    store.set_agent_runtime(&agent.id, &sbx).unwrap();
    assert_eq!(store.agent(&agent.id).unwrap().runtime, sbx);

    store
        .set_agent_runtime(&agent.id, &ExecutionRuntime::Cloud)
        .unwrap();
    assert_eq!(
        store.agent(&agent.id).unwrap().runtime,
        ExecutionRuntime::Cloud
    );

    // Budget cap set, update, and clear.
    store
        .set_agent_budget_micros(&agent.id, Some(3_000_000))
        .unwrap();
    assert_eq!(
        store.agent(&agent.id).unwrap().remote_budget_micros,
        Some(3_000_000)
    );

    // Verify all read queries agree: agent(), list_agents(), list_live_agents(),
    // agent_summaries(), roster_agents(), roster_agents_paged(), rename alias, groups.
    store.join_group("team", &agent.id).unwrap();
    store.rename_agent(&agent.id, "worker-v2").unwrap();

    let by_alias = store.agent_by_name("worker").unwrap();
    assert_eq!(by_alias.runtime, ExecutionRuntime::Cloud);
    assert_eq!(by_alias.remote_budget_micros, Some(3_000_000));

    let listed = store.list_agents().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].runtime, ExecutionRuntime::Cloud);
    assert_eq!(listed[0].remote_budget_micros, Some(3_000_000));

    let live = store.list_live_agents().unwrap();
    assert_eq!(live[0].runtime, ExecutionRuntime::Cloud);
    assert_eq!(live[0].remote_budget_micros, Some(3_000_000));

    let summaries = store.agent_summaries(true).unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].runtime, ExecutionRuntime::Cloud);
    assert_eq!(summaries[0].remote_budget_micros, Some(3_000_000));

    let (paged, _) = store
        .roster_agents_paged(RosterPageQuery {
            state: Some("all"),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(paged[0].runtime, ExecutionRuntime::Cloud);
    assert_eq!(paged[0].remote_budget_micros, Some(3_000_000));

    let groups = store.list_groups_with_roster().unwrap();
    assert_eq!(groups[0].members[0].runtime, ExecutionRuntime::Cloud);
    assert_eq!(groups[0].members[0].remote_budget_micros, Some(3_000_000));

    // Atomic harness + runtime + model update, then preserve model on model = None.
    store
        .set_agent_harness_and_runtime(&agent.id, "claude-code", &sbx, Some("claude-sonnet-4-6"))
        .unwrap();
    let updated = store.agent(&agent.id).unwrap();
    assert_eq!(updated.harness, "claude-code");
    assert_eq!(updated.runtime, sbx);
    assert_eq!(updated.model.as_deref(), Some("claude-sonnet-4-6"));

    store
        .set_agent_harness_and_runtime(&agent.id, "codex", &ExecutionRuntime::Cloud, None)
        .unwrap();
    let kept_model = store.agent(&agent.id).unwrap();
    assert_eq!(kept_model.harness, "codex");
    assert_eq!(kept_model.runtime, ExecutionRuntime::Cloud);
    assert_eq!(kept_model.model.as_deref(), Some("claude-sonnet-4-6"));

    // Fail-closed transitions do not mutate stored state.
    assert!(
        store
            .set_agent_harness_and_runtime(&agent.id, "agy", &ExecutionRuntime::Cloud, None)
            .is_err()
    );
    assert!(
        store
            .set_agent_harness_and_runtime(
                &agent.id,
                "spiron-cloud",
                &ExecutionRuntime::Local,
                None
            )
            .is_err()
    );
    let unchanged = store.agent(&agent.id).unwrap();
    assert_eq!(unchanged.harness, "codex");
    assert_eq!(unchanged.runtime, ExecutionRuntime::Cloud);

    // Local-only harness rejects set_agent_runtime(Cloud).
    let agy = store
        .create_agent(NewAgent::new("local-agy", "agy"))
        .unwrap();
    assert!(
        store
            .set_agent_runtime(&agy.id, &ExecutionRuntime::Cloud)
            .is_err()
    );
    assert_eq!(
        store.agent(&agy.id).unwrap().runtime,
        ExecutionRuntime::Local
    );

    // Clear budget cap and verify across reopen.
    store.set_agent_budget_micros(&agent.id, None).unwrap();
    assert_eq!(store.agent(&agent.id).unwrap().remote_budget_micros, None);
    assert!(
        store
            .set_agent_budget_micros(&agent.id, Some(u64::MAX))
            .is_err()
    );

    drop(store);
    let reopened = Store::open(dir.path().join("state.db")).unwrap();
    assert_eq!(
        reopened.agent(&agent.id).unwrap().remote_budget_micros,
        None
    );
    assert_eq!(
        reopened.agent(&agent.id).unwrap().runtime,
        ExecutionRuntime::Cloud
    );
}

#[test]
fn row_decoder_falls_back_to_launch_env_when_runtime_column_is_blank() {
    let (dir, store) = setup();
    let agent = store
        .create_agent(NewAgent::new("env-fb", "codex"))
        .unwrap();
    store
        .set_agent_launch_env(
            &agent.id,
            &[
                (RUNTIME_ENV_KEY.into(), "sandbox:e2b".into()),
                (REMOTE_BUDGET_MICROS_ENV_KEY.into(), "750000".into()),
            ],
        )
        .unwrap();
    drop(store);

    // Simulate a legacy row whose `runtime` column is empty and `remote_budget_micros` is NULL.
    let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    conn.execute(
        "UPDATE agents SET runtime = '', remote_budget_micros = NULL WHERE id = ?1",
        [agent.id.as_str()],
    )
    .unwrap();
    drop(conn);

    let reopened = Store::open(dir.path().join("state.db")).unwrap();
    let loaded = reopened.agent(&agent.id).unwrap();
    assert_eq!(
        loaded.runtime,
        ExecutionRuntime::Sandbox {
            provider: Some("e2b".into())
        }
    );
    assert_eq!(loaded.remote_budget_micros, Some(750_000));
}
