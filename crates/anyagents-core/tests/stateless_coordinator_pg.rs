use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyagents_core::db::sqlite_to_pg::{MigrateToPostgresOptions, migrate_sqlite_dir_to_postgres};
use anyagents_core::host_index::MemoryScope;
use anyagents_core::host_retrieval::{CitedRange, HostRawRead};
use anyagents_core::stateless::{
    DEFAULT_REMOTE_SANDBOX_WORKDIR, DurableObligation, MasterKey, NewRemoteTurn,
    RemoteTurnEventUpdate, SandboxBinding, SettleRemoteTurn, UpsertGatewayRoute,
    generate_callback_token, generate_turn_id, hash_callback_token,
};
use anyagents_core::{
    HistorySearchQuery, Incoming, NewAgent, ReceiveOutcome, RuntimeHeartbeat, Store, Wake,
};

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn pg_url() -> Option<String> {
    std::env::var("ANYAGENTS_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn unique_tag() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}_{}", std::process::id(), nanos)
}

#[test]
fn pg_two_replicas_share_blobs_locks_bindings_routes_obligations_heartbeats_and_wakes() {
    let Some(url) = pg_url() else {
        eprintln!("skipping stateless_coordinator_pg test: ANYAGENTS_TEST_POSTGRES_URL not set");
        return;
    };
    let _guard = test_lock();
    let tag = unique_tag();

    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let store_a = Store::open_postgres_isolated(&url, dir_a.path(), format!("replica-a-{tag}"))
        .expect("open replica A");
    let store_b = Store::open_postgres_isolated(&url, dir_b.path(), format!("replica-b-{tag}"))
        .expect("open replica B");
    assert!(store_a.is_postgres());
    assert!(store_b.is_postgres());
    assert_ne!(store_a.replica_id(), store_b.replica_id());

    // Create agent on replica A; replica B sees it immediately with zero local files.
    let agent = store_a
        .create_agent(NewAgent::new(
            format!("stateless_agent_{tag}"),
            "claude-code",
        ))
        .unwrap();
    let agent_b = store_b.agent(&agent.id).unwrap();
    assert_eq!(agent_b.name, agent.name);
    assert!(!dir_b.path().join("agents").exists());

    // 1. StateBlobStore: agent_files + deployment_blobs (plain & AEAD encrypted).
    let key = MasterKey::from_bytes(&[0x42u8; 32]).unwrap();
    let wrong_key = MasterKey::from_bytes(&[0x99u8; 32]).unwrap();
    let blobs_a = store_a.blob_store_with_key(Some(key.clone()));
    let blobs_b = store_b.blob_store_with_key(Some(key.clone()));
    let blobs_b_wrong = store_b.blob_store_with_key(Some(wrong_key));

    let v1 = blobs_a
        .put_agent_file(
            &agent.id,
            "memory",
            "MEMORY.md",
            b"# Shared Memory\nAlpha replica wrote this.\n",
            false,
        )
        .unwrap();
    assert_eq!(v1.version, 1);
    let read_b = blobs_b
        .get_agent_file(&agent.id, "memory", "MEMORY.md")
        .unwrap()
        .expect("replica B reads memory blob");
    assert_eq!(
        read_b.content,
        b"# Shared Memory\nAlpha replica wrote this.\n"
    );
    assert_eq!(read_b.version, 1);

    // Encrypted secret file cannot be decrypted with wrong key.
    blobs_a
        .put_agent_file(
            &agent.id,
            "secrets",
            "api.key",
            b"sk-synthetic-stateless-test-only",
            true,
        )
        .unwrap();
    let decrypted = blobs_b
        .get_agent_file(&agent.id, "secrets", "api.key")
        .unwrap()
        .expect("replica B decrypts secret blob with matching key");
    assert!(decrypted.encrypted);
    assert_eq!(decrypted.content, b"sk-synthetic-stateless-test-only");
    assert!(
        blobs_b_wrong
            .get_agent_file(&agent.id, "secrets", "api.key")
            .is_err(),
        "wrong master key must fail closed"
    );

    // Deployment blobs shared across replicas.
    let dep_path = format!("stateless_{tag}/default/config.json");
    blobs_a
        .put_deployment_blob(
            "profiles",
            &dep_path,
            br#"{"endpoint":"https://example.test"}"#,
            true,
        )
        .unwrap();
    let dep_b = blobs_b
        .get_deployment_blob("profiles", &dep_path)
        .unwrap()
        .expect("replica B reads encrypted deployment blob");
    assert_eq!(dep_b.content, br#"{"endpoint":"https://example.test"}"#);
    assert!(
        blobs_b
            .list_deployment_blobs("profiles")
            .unwrap()
            .iter()
            .any(|m| m.path == dep_path)
    );
    let _ = blobs_b.delete_deployment_blob("profiles", &dep_path);

    // 2. CoordinatorLockBackend + distributed AgentLock mutual exclusion across replicas.
    let lock_a = store_a
        .try_lock_agent(&agent.id)
        .expect("replica A acquires agent process lock");
    let token_1 = lock_a.fencing_token().expect("PG fencing token");
    assert!(
        matches!(
            store_b.try_lock_agent(&agent.id),
            Err(anyagents_core::Error::AgentBusy)
        ),
        "replica B MUST NOT acquire agent process lock held by replica A"
    );
    drop(lock_a);
    let lock_b = store_b
        .try_lock_agent(&agent.id)
        .expect("replica B acquires agent process lock after A releases");
    let token_2 = lock_b.fencing_token().expect("second PG fencing token");
    assert!(
        token_2 > token_1,
        "fencing token must advance monotonically: {token_2} <= {token_1}"
    );
    drop(lock_b);

    // Coordinator lease singleton election & release handover.
    let lease_key = format!("maint:singleton:{tag}");
    let guard_a = store_a
        .try_acquire_coordinator_lease(&lease_key, Duration::from_secs(5))
        .unwrap()
        .expect("replica A wins singleton lease");
    assert!(
        store_b
            .try_acquire_coordinator_lease(&lease_key, Duration::from_secs(5))
            .unwrap()
            .is_none(),
        "replica B loses singleton election while A holds active lease"
    );
    drop(guard_a);
    let guard_b = store_b
        .try_acquire_coordinator_lease(&lease_key, Duration::from_secs(5))
        .unwrap()
        .expect("replica B wins singleton lease after A drops");
    drop(guard_b);

    // 3. PgSandboxBindingStore shared across replicas (with encrypted transport token).
    let sbx_a = store_a.sandbox_binding_store_with_key(Some(key.clone()));
    let sbx_b = store_b.sandbox_binding_store_with_key(Some(key.clone()));
    let now = anyagents_core::now_ms();
    let mut binding = SandboxBinding::new(agent.id.clone(), "sylphx");
    binding.volume_id = format!("vol-{tag}");
    binding.volume_resource = format!("volumes/vol-{tag}");
    binding.lease_id = Some(format!("lease-{tag}"));
    binding.lease_resource = Some(format!("leases/lease-{tag}"));
    binding.template = Some("anyagents-sandbox".to_string());
    binding.remote_workdir = DEFAULT_REMOTE_SANDBOX_WORKDIR.to_string();
    binding.guest_endpoint = Some("https://envd.example.test".to_string());
    binding.transport_token = Some("tok-envd-synthetic".to_string());
    binding.lease_state = "ready".to_string();
    binding.idle_since_ms = Some(now - 600_000);
    binding.idle_ttl_ms = 60_000;
    binding.last_heartbeat_ms = Some(now);
    binding.updated_at_ms = now;

    sbx_a.upsert(&binding).unwrap();
    let loaded_b = sbx_b
        .get(&agent.id)
        .unwrap()
        .expect("replica B reads sandbox binding from PG");
    assert_eq!(loaded_b.provider, "sylphx");
    assert_eq!(loaded_b.remote_workdir, DEFAULT_REMOTE_SANDBOX_WORKDIR);
    assert_eq!(
        loaded_b.transport_token.as_deref(),
        Some("tok-envd-synthetic")
    );
    assert_eq!(
        loaded_b.lease_id.as_deref(),
        Some(format!("lease-{tag}").as_str())
    );
    assert!(
        sbx_b
            .list_idle_due(now)
            .unwrap()
            .iter()
            .any(|b| b.agent_id == agent.id),
        "idle_due query must return binding idle past idle_ttl_ms"
    );
    sbx_b.clear_lease(&agent.id).unwrap();
    let paused = sbx_a.get(&agent.id).unwrap().unwrap();
    assert_eq!(paused.lease_state, "detached");
    assert!(paused.lease_id.is_none());
    assert_eq!(paused.volume_id, format!("vol-{tag}"));

    // 4. PgGatewayRouteStore shared across replicas + live rename visibility.
    let gw_a = store_a
        .gateway_route_store_with_key(Some(key.clone()))
        .expect("PG gateway route store A");
    let gw_b = store_b
        .gateway_route_store_with_key(Some(key.clone()))
        .expect("PG gateway route store B");
    let gw_tok = format!("gw_tok_{tag}");
    gw_a.upsert(&UpsertGatewayRoute {
        agent_id: agent.id.clone(),
        agent_name: agent.name.clone(),
        harness: agent.harness.clone(),
        protocol: "openai_responses".to_string(),
        model: "gpt-5".to_string(),
        base_url: "https://api.example.test/v1".to_string(),
        api_key: "sk-gw-synthetic".to_string(),
        token: gw_tok.clone(),
    })
    .unwrap();
    let resolved = gw_b
        .resolve_by_token(&gw_tok)
        .unwrap()
        .expect("replica B resolves gateway token in PG");
    assert_eq!(resolved.agent_id, agent.id);
    assert_eq!(resolved.agent_name, agent.name);
    assert_eq!(resolved.api_key, "sk-gw-synthetic");

    let renamed_name = format!("stateless_renamed_{tag}");
    store_a.rename_agent(&agent.id, &renamed_name).unwrap();
    let resolved_after_rename = gw_b.resolve_by_token(&gw_tok).unwrap().unwrap();
    assert_eq!(resolved_after_rename.agent_name, renamed_name);

    // 5. PgDurableObligationStore shared across replicas.
    let ob_a = store_a.durable_obligation_store();
    let ob_b = store_b.durable_obligation_store();
    let ob_key = format!("ob-{tag}");
    ob_a.upsert(&DurableObligation {
        kind: "outbound_history".to_string(),
        key: ob_key.clone(),
        agent_id: Some(agent.id.clone()),
        state: "pending".to_string(),
        payload_json: r#"{"lease":"lease-1"}"#.to_string(),
        attempts: 0,
        next_at_ms: now - 1_000,
        created_at_ms: now,
        updated_at_ms: now,
    })
    .unwrap();
    assert!(
        ob_b.list_due("outbound_history", now, 50)
            .unwrap()
            .iter()
            .any(|o| o.key == ob_key)
    );
    ob_b.set_state("outbound_history", &ob_key, "done", 1, now + 60_000)
        .unwrap();
    assert!(
        !ob_a
            .list_due("outbound_history", now, 50)
            .unwrap()
            .iter()
            .any(|o| o.key == ob_key)
    );
    ob_b.delete("outbound_history", &ob_key).unwrap();

    // 6. agent_runtime heartbeat/owner columns visible across replicas.
    store_a
        .update_runtime_heartbeat(
            &agent.id,
            &RuntimeHeartbeat {
                owner_replica_id: Some(store_a.replica_id().to_string()),
                turn_state: "in_turn".to_string(),
                retry_at_ms: None,
                heartbeat_at_ms: Some(now),
                idle_since_ms: None,
                inflight_children: 2,
            },
        )
        .unwrap();
    let hb_b = store_b
        .runtime_heartbeat(&agent.id)
        .unwrap()
        .expect("replica B sees runtime heartbeat");
    assert_eq!(hb_b.owner_replica_id.as_deref(), Some(store_a.replica_id()));
    assert_eq!(hb_b.turn_state, "in_turn");
    assert_eq!(hb_b.inflight_children, 2);

    // 7. Cross-replica PG LISTEN/NOTIFY wakes on `anyagents_wakes`.
    let wake_rx_b = store_b.subscribe();
    let changes_rx_b = store_b.subscribe_changes();
    let initial_change_seq = *changes_rx_b.borrow();
    assert!(
        store_b.wait_pg_wake_listener_ready(Duration::from_secs(5)),
        "replica B PG wake listener must connect within 5s"
    );

    let ReceiveOutcome::New(_) = store_a
        .receive(&agent.id, Incoming::new("hello across replicas via NOTIFY"))
        .unwrap()
    else {
        panic!("expected new inbound message");
    };

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut received_wake = false;
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        match wake_rx_b.recv_timeout(remaining) {
            Ok(Wake::Mail { agent_id }) if agent_id == agent.id => {
                received_wake = true;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    assert!(
        received_wake,
        "replica B must receive Wake::Mail from replica A over PostgreSQL LISTEN/NOTIFY"
    );
    assert!(
        *changes_rx_b.borrow() > initial_change_seq,
        "replica B change watch must advance on cross-replica wake"
    );

    // 8. Stale unadmitted name release respects age threshold (does NOT reap fresh concurrent create).
    let fresh_unadmitted = store_a
        .create_agent(NewAgent::new(
            format!("fresh_unadmitted_{tag}"),
            "claude-code",
        ))
        .unwrap();
    let released_recent = store_b
        .release_unadmitted_names_older_than(300_000)
        .unwrap();
    assert!(
        !released_recent
            .iter()
            .any(|(id, _)| id == &fresh_unadmitted.id),
        "freshly created unadmitted agent must NOT be reaped by peer replica: {released_recent:?}"
    );
    assert_eq!(
        store_a.agent(&fresh_unadmitted.id).unwrap().state,
        "created"
    );

    // Clean up test agents (and verify cascade delete removes gateway route).
    store_a.delete_agent(&fresh_unadmitted.id).unwrap();
    store_a.delete_agent(&agent.id).unwrap();
    assert!(gw_b.resolve_by_token(&gw_tok).unwrap().is_none());
}

#[test]
fn pg_host_retrieval_works_with_zero_local_sqlite_or_agent_files_on_disk() {
    let Some(url) = pg_url() else {
        eprintln!("skipping pg_host_retrieval test: ANYAGENTS_TEST_POSTGRES_URL not set");
        return;
    };
    let _guard = test_lock();
    let tag = unique_tag();

    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let store_a = Store::open_postgres_isolated(&url, dir_a.path(), format!("ret-a-{tag}"))
        .expect("open replica A");
    let store_b = Store::open_postgres_isolated(&url, dir_b.path(), format!("ret-b-{tag}"))
        .expect("open replica B");

    let agent = store_a
        .create_agent(NewAgent::new(
            format!("retrieval_agent_{tag}"),
            "claude-code",
        ))
        .unwrap();

    // Write agent memory directly into Postgres `agent_files` from replica A (NO local files on replica B!).
    let needle = format!("stateless_pg_needle_{tag}");
    let memory_body = format!(
        "# Persistent Agent Memory\nLine two has {needle} and 香港 Stateless Coordinator check.\nLine three closes the note.\n"
    );
    store_a
        .blob_store()
        .put_agent_file(
            &agent.id,
            "memory",
            "MEMORY.md",
            memory_body.as_bytes(),
            false,
        )
        .unwrap();

    // Confirm replica B has ZERO local SQLite files and ZERO local agent directories.
    assert!(!dir_b.path().join("host-history-index-v1.db").exists());
    assert!(!dir_b.path().join("state.db").exists());
    assert!(!dir_b.path().join("agents").exists());

    // 1. Search on replica B against PG with zero local files.
    let authorized = vec![agent.id.to_string()];
    let query = HistorySearchQuery {
        query: Some(needle.clone()),
        limit: 10,
        ..HistorySearchQuery::default()
    };
    let hits = store_b
        .search_host_files(&authorized, MemoryScope::Memory, &query)
        .expect("search_host_files on stateless PG replica B");
    assert!(
        !hits.is_empty(),
        "expected PG agent_files hit on empty replica B"
    );
    let (hit, bytes) = &hits[0];
    let text = String::from_utf8_lossy(bytes);
    assert!(
        hit.raw
            .segment
            .to_string_lossy()
            .starts_with("pg://agent_files/"),
        "expected pg://agent_files path, got: {}",
        hit.raw.segment.display()
    );
    assert!(text.contains(&needle), "text: {text}");

    // 2. Read exact byte range on replica B directly from PG (without local host-history-index-v1.db).
    let (range_hit, range_bytes) = store_b
        .read_host_file_range(
            &authorized,
            MemoryScope::Memory,
            None,
            HostRawRead {
                id: &hit.id,
                sha256: &hit.raw.sha256,
                offset: 0,
                length: 512,
            },
        )
        .expect("read_host_file_range on PG replica B")
        .expect("expected raw range hit on PG replica B");
    let range_text = String::from_utf8_lossy(&range_bytes);
    assert_eq!(range_hit.id, hit.id);
    assert!(range_text.contains(&needle), "range: {range_text}");
    assert!(range_text.contains("Stateless Coordinator"));

    // 3. Resolve host ranges on replica B directly from PG.
    let cited = vec![CitedRange {
        document_id: hit.id.clone(),
        offset: hit.raw.offset,
        length: hit.raw.length,
        sha256: hit.raw.sha256.clone(),
    }];
    let resolved = store_b
        .resolve_host_ranges(&authorized, MemoryScope::Memory, &query, &cited)
        .expect("resolve_host_ranges on PG replica B");
    assert_eq!(resolved.len(), 1);
    assert!(String::from_utf8_lossy(&resolved[0].1).contains(&needle));

    // 4. Verify host history source on replica B directly from PG (must NOT short-circuit or wipe PG source).
    let _ = store_b
        .verify_one_host_history_source(anyagents_core::now_ms() + 86_400_000)
        .expect("verify_one_host_history_source on PG replica B");
    let hits_after = store_b
        .search_host_files(&authorized, MemoryScope::Memory, &query)
        .expect("search_host_files after verification on PG replica B");
    assert_eq!(hits_after.len(), 1);

    // 5. Replica B STILL has ZERO local SQLite files on disk!
    assert!(
        !dir_b.path().join("host-history-index-v1.db").exists(),
        "stateless PG replica MUST NOT create local host-history-index-v1.db"
    );
    assert!(
        !dir_b.path().join("state.db").exists(),
        "stateless PG replica MUST NOT create local state.db"
    );

    store_a.delete_agent(&agent.id).unwrap();
}

#[test]
fn pg_migrate_to_postgres_backfills_sqlite_and_disk_files_idempotently() {
    let Some(url) = pg_url() else {
        eprintln!("skipping pg_migrate_to_postgres test: ANYAGENTS_TEST_POSTGRES_URL not set");
        return;
    };
    let _guard = test_lock();
    let tag = unique_tag();

    // Build a realistic local SQLite + on-disk data directory.
    let src_dir = tempfile::tempdir().unwrap();
    let sqlite_store = Store::open(src_dir.path().join("state.db")).unwrap();
    assert!(!sqlite_store.is_postgres());

    let agent = sqlite_store
        .create_agent(NewAgent::new(format!("mig_agent_{tag}"), "claude-code"))
        .unwrap();
    sqlite_store
        .receive(&agent.id, Incoming::new("migrated inbox payload"))
        .unwrap();

    // Populate on-disk agent files: memory, skills, config, sessions, provider+gateway-token, sandbox-sylphx.json.
    let agent_root = src_dir.path().join("agents").join(agent.id.as_str());
    let mem_dir = agent_root.join("memory");
    let skill_dir = agent_root.join("skills").join("ops-playbook");
    let cfg_dir = agent_root.join("config");
    let sess_dir = cfg_dir.join("sessions");
    std::fs::create_dir_all(&mem_dir).unwrap();
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::create_dir_all(&sess_dir).unwrap();

    std::fs::write(mem_dir.join("MEMORY.md"), b"# Memory v1\nOriginal notes.\n").unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), b"# Ops Playbook\n").unwrap();
    std::fs::write(cfg_dir.join("instructions.md"), b"Be concise.\n").unwrap();
    std::fs::write(sess_dir.join("s1.jsonl"), b"{\"type\":\"user\"}\n").unwrap();
    // Runtime lock files must be skipped cleanly.
    std::fs::write(cfg_dir.join("server.lock"), b"12345").unwrap();

    // Gateway route files (`agents/<id>/provider.json` + `gateway-token.txt`, matching `gateway::ProviderImage`).
    let gw_tok = format!("mig_gw_tok_{tag}");
    std::fs::write(
        agent_root.join("provider.json"),
        serde_json::json!({
            "harness": "codex",
            "model": "gpt-5",
            "baseUrl": "https://api.example.test/v1",
            "apiKey": "sk-mig-synthetic"
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(agent_root.join("gateway-token.txt"), format!("{gw_tok}\n")).unwrap();

    // Sylphx sandbox binding (`sandbox-sylphx.json`).
    std::fs::write(
        cfg_dir.join("sandbox-sylphx.json"),
        serde_json::json!({
            "volume_id": format!("vol-mig-{tag}"),
            "volume_resource": format!("volumes/vol-mig-{tag}"),
            "lease_id": format!("lease-mig-{tag}"),
            "lease_resource": format!("leases/lease-mig-{tag}"),
            "template_id": "anyagents-sandbox",
            "workdir": "/home/user/agent-data/workspace",
            "envd_url": "https://envd.example.test",
            "envd_token": "tok-mig-envd"
        })
        .to_string(),
    )
    .unwrap();

    // Deployment-level files + secret file.
    std::fs::write(
        src_dir.path().join("anyagents.toml"),
        b"[coordinator]\nmode = \"stateless\"\n",
    )
    .unwrap();
    std::fs::write(
        src_dir.path().join("subagent-defaults.json"),
        br#"{"harness":"claude-code"}"#,
    )
    .unwrap();
    let prof_dir = src_dir.path().join("profiles").join(format!("mig_{tag}"));
    std::fs::create_dir_all(&prof_dir).unwrap();
    std::fs::write(prof_dir.join("profile.json"), br#"{"name":"default"}"#).unwrap();
    let secrets_dir = src_dir.path().join("secrets");
    std::fs::create_dir_all(&secrets_dir).unwrap();
    std::fs::write(
        secrets_dir.join(format!("mig_{tag}.secret")),
        b"top-secret-test",
    )
    .unwrap();

    drop(sqlite_store);

    let key = MasterKey::from_bytes(&[0x77u8; 32]).unwrap();
    let mut opts = MigrateToPostgresOptions::new(src_dir.path(), &url);
    opts.master_key = Some(key.clone());
    let report1 =
        migrate_sqlite_dir_to_postgres(&opts).expect("first migrate_sqlite_dir_to_postgres");

    assert!(
        report1
            .tables
            .iter()
            .any(|t| t.pg_table == "public.agents" && t.sqlite_rows >= 1),
        "expected migrated agents table: {report1:?}"
    );
    assert!(
        report1.agent_files_migrated >= 4,
        "expected >= 4 migrated agent files (memory/skill/config/session): {report1:?}"
    );
    assert!(
        report1.deployment_blobs_migrated >= 4,
        "expected >= 4 migrated deployment blobs: {report1:?}"
    );
    assert_eq!(report1.gateway_routes_migrated, 1, "report: {report1:?}");
    assert_eq!(report1.sandbox_bindings_migrated, 1, "report: {report1:?}");
    assert_eq!(report1.skipped_secret_files, 0, "report: {report1:?}");

    // Verify everything is readable from a fresh stateless PG replica with an empty local dir!
    let pg_dir = tempfile::tempdir().unwrap();
    let pg_store = Store::open_postgres_isolated(&url, pg_dir.path(), format!("mig-verify-{tag}"))
        .expect("open PG verification store");
    let blobs = pg_store.blob_store_with_key(Some(key.clone()));

    let mem_v1 = blobs
        .get_agent_file(&agent.id, "memory", "MEMORY.md")
        .unwrap()
        .expect("migrated MEMORY.md in PG");
    assert_eq!(mem_v1.content, b"# Memory v1\nOriginal notes.\n");
    assert_eq!(mem_v1.version, 1);

    let skill_v1 = blobs
        .get_agent_file(&agent.id, "skills", "ops-playbook/SKILL.md")
        .unwrap()
        .expect("migrated skill in PG");
    assert_eq!(skill_v1.content, b"# Ops Playbook\n");

    let secret_blob = blobs
        .get_deployment_blob("secrets", &format!("mig_{tag}.secret"))
        .unwrap()
        .expect("migrated encrypted deployment secret in PG");
    assert!(secret_blob.encrypted);
    assert_eq!(secret_blob.content, b"top-secret-test");

    let gw = pg_store
        .gateway_route_store_with_key(Some(key.clone()))
        .unwrap()
        .resolve_by_token(&gw_tok)
        .unwrap()
        .expect("migrated gateway route in PG");
    assert_eq!(gw.agent_id, agent.id);
    assert_eq!(gw.api_key, "sk-mig-synthetic");

    let sbx = pg_store
        .sandbox_binding_store_with_key(Some(key.clone()))
        .get(&agent.id)
        .unwrap()
        .expect("migrated sandbox binding in PG");
    assert_eq!(sbx.volume_id, format!("vol-mig-{tag}"));
    assert_eq!(sbx.transport_token.as_deref(), Some("tok-mig-envd"));
    assert_eq!(sbx.remote_workdir, "/home/user/agent-data/workspace");

    // Re-run migration after editing one file: verify ON CONFLICT DO UPDATE idempotency!
    std::fs::write(mem_dir.join("MEMORY.md"), b"# Memory v2\nUpdated notes.\n").unwrap();
    let report2 = migrate_sqlite_dir_to_postgres(&opts)
        .expect("second idempotent migrate_sqlite_dir_to_postgres");
    assert_eq!(report2.agent_files_migrated, report1.agent_files_migrated);
    assert_eq!(report2.gateway_routes_migrated, 1);
    assert_eq!(report2.sandbox_bindings_migrated, 1);

    let mem_v2 = blobs
        .get_agent_file(&agent.id, "memory", "MEMORY.md")
        .unwrap()
        .expect("updated MEMORY.md in PG");
    assert_eq!(mem_v2.content, b"# Memory v2\nUpdated notes.\n");
    assert!(
        mem_v2.version >= 2,
        "expected version bump on content change: {}",
        mem_v2.version
    );

    let skill_after = blobs
        .get_agent_file(&agent.id, "skills", "ops-playbook/SKILL.md")
        .unwrap()
        .expect("unchanged skill in PG");
    assert_eq!(skill_after.content, b"# Ops Playbook\n");

    // Re-run with `resume = true`: already-matched SQLite tables skip re-streaming while
    // `valid_agents` and on-disk blob backfill still run end-to-end.
    std::fs::write(mem_dir.join("MEMORY.md"), b"# Memory v3\nResumed notes.\n").unwrap();
    opts.resume = true;
    let report3 = migrate_sqlite_dir_to_postgres(&opts)
        .expect("third resumed migrate_sqlite_dir_to_postgres");
    let agents_stat = report3
        .tables
        .iter()
        .find(|t| t.pg_table == "public.agents")
        .expect("public.agents stat in resumed report");
    assert_eq!(agents_stat.pg_rows_before, agents_stat.pg_rows_after);
    assert_eq!(report3.agent_files_migrated, report1.agent_files_migrated);
    let mem_v3 = blobs
        .get_agent_file(&agent.id, "memory", "MEMORY.md")
        .unwrap()
        .expect("resumed MEMORY.md update in PG");
    assert_eq!(mem_v3.content, b"# Memory v3\nResumed notes.\n");

    // Clean up test rows.
    let _ = blobs.delete_deployment_blob("secrets", &format!("mig_{tag}.secret"));
    let _ = blobs.delete_deployment_blob("profiles", &format!("mig_{tag}/profile.json"));
    pg_store.delete_agent(&agent.id).unwrap();
}

#[test]
fn pg_remote_turn_store_survives_replica_handoff_and_settles_idempotently() {
    let Some(url) = pg_url() else {
        eprintln!("skipping pg_remote_turn_store test: ANYAGENTS_TEST_POSTGRES_URL not set");
        return;
    };
    let _guard = test_lock();
    let tag = unique_tag();

    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let store_a = Store::open_postgres_isolated(&url, dir_a.path(), format!("rturn-a-{tag}"))
        .expect("open replica A");
    let store_b = Store::open_postgres_isolated(&url, dir_b.path(), format!("rturn-b-{tag}"))
        .expect("open replica B");

    assert!(anyagents_core::db::PG_LATEST_VERSION >= 4);

    let agent = store_a
        .create_agent(NewAgent::new(
            format!("serverless_turn_agent_{tag}"),
            "claude-code",
        ))
        .unwrap();

    let turns_a = store_a.remote_turn_store();
    let turns_b = store_b.remote_turn_store();
    let turn_id = generate_turn_id();
    let token = generate_callback_token();
    let now = anyagents_core::now_ms();

    let created = turns_a
        .create_turn(&NewRemoteTurn {
            turn_id: turn_id.clone(),
            agent_id: agent.id.clone(),
            harness_kind: "spiron".to_string(),
            provider_kind: Some("cloud".to_string()),
            remote_run_id: Some(format!("tsk_{tag}")),
            callback_token_hash: hash_callback_token(&token),
            cursor: None,
            status: "dispatched".to_string(),
            next_poll_at_ms: Some(now - 500),
            metadata_json: Some(serde_json::json!({
                "reply_target": { "kind": "agent", "agent_id": agent.id.as_str() }
            })),
        })
        .expect("replica A inserts pending remote turn");
    assert_eq!(created.status, "dispatched");

    // Replica B verifies constant-time callback token and rejects wrong tokens.
    assert!(
        turns_b
            .verify_callback_token(&turn_id, &token)
            .unwrap()
            .is_some(),
        "replica B must verify exact callback token"
    );
    assert!(
        turns_b
            .verify_callback_token(&turn_id, "wrong-callback-token")
            .unwrap()
            .is_none(),
        "replica B must reject wrong callback token"
    );

    // Replica B sees the turn as due for polling and active for the agent.
    let due = turns_b.list_due_polls(now, 64).unwrap();
    assert!(due.iter().any(|t| t.turn_id == turn_id));
    let active = turns_b.list_active_for_agent(&agent.id).unwrap();
    assert!(active.iter().any(|t| t.turn_id == turn_id));

    // Replica B records a streaming delta + cursor update.
    let progressed = turns_b
        .record_event(
            &turn_id,
            &RemoteTurnEventUpdate {
                status: Some("running".to_string()),
                remote_run_id: None,
                cursor: Some("evt_42".to_string()),
                last_event_at_ms: now + 100,
                next_poll_at_ms: Some(Some(now + 10_000)),
            },
        )
        .unwrap()
        .expect("turn exists for event update");
    assert_eq!(progressed.status, "running");
    assert_eq!(progressed.cursor.as_deref(), Some("evt_42"));

    // Drop replica A completely; replica B settles idempotently (including NUL byte sanitization).
    drop(turns_a);
    drop(store_a);
    let settled = turns_b
        .settle_turn(
            &turn_id,
            &SettleRemoteTurn {
                status: "completed".to_string(),
                result_text: Some("verified\0serverless output".to_string()),
                error_text: None,
                usage_json: Some(serde_json::json!({
                    "input_tokens": 120,
                    "output_tokens": 45
                })),
                cursor: Some("evt_99".to_string()),
                settled_at_ms: now + 200,
            },
        )
        .unwrap()
        .expect("turn exists for settlement");
    assert!(settled.transitioned);
    assert_eq!(settled.turn.status, "completed");
    assert_eq!(
        settled.turn.result_text.as_deref(),
        Some("verified\u{FFFD}serverless output")
    );
    assert!(settled.turn.next_poll_at_ms.is_none());

    // Duplicate settlement (e.g. webhook + tick race) is a clean idempotent no-op.
    let duplicate = turns_b
        .settle_turn(
            &turn_id,
            &SettleRemoteTurn {
                status: "failed".to_string(),
                result_text: None,
                error_text: Some("late duplicate must be ignored".to_string()),
                usage_json: None,
                cursor: None,
                settled_at_ms: now + 300,
            },
        )
        .unwrap()
        .expect("turn exists for duplicate settlement");
    assert!(!duplicate.transitioned);
    assert_eq!(duplicate.turn.status, "completed");
    assert_eq!(
        duplicate.turn.result_text.as_deref(),
        Some("verified\u{FFFD}serverless output")
    );

    // Settled turn is no longer due or active.
    assert!(
        !turns_b
            .list_due_polls(now + 60_000, 64)
            .unwrap()
            .iter()
            .any(|t| t.turn_id == turn_id)
    );
    assert!(turns_b.list_active_for_agent(&agent.id).unwrap().is_empty());

    // Clean up test rows.
    turns_b.delete_turn(&turn_id).unwrap();
    store_b.delete_agent(&agent.id).unwrap();
}
