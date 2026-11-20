use std::sync::{Arc, Mutex, OnceLock};

use anyagents_core::host_index::{HostIndex, IndexMetadata, MemoryScope};
use anyagents_core::raw_segment::RawSegment;
use anyagents_core::redaction::{Actor, Request as RedactRequest, Span, SpanUnit};
use anyagents_core::{
    BillingMode, HistorySearchQuery, Incoming, NewAgent, NewSchedule, NewUsage, Origin,
    ReceiveOutcome, RuntimeChange, RuntimeState, Store, read_children_roster, read_root_roster,
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

#[test]
fn postgres_persistent_webhook_wakes_serialize_retries_and_survive_reopen() {
    let Some(url) = pg_url() else {
        eprintln!("skipping PostgreSQL webhook test: ANYAGENTS_TEST_POSTGRES_URL not set");
        return;
    };
    let _guard = test_lock();
    let dir = tempfile::tempdir().unwrap();
    let first = Store::open_postgres(&url, dir.path()).unwrap();
    let peer = Store::open_postgres(&url, dir.path()).unwrap();
    let agent = first
        .create_agent(NewAgent::new(format!("sub_{}", uuid_tag()), "codex"))
        .unwrap();
    let input = || NewSchedule {
        agent_id: agent.id.clone(),
        kind: "watch".into(),
        spec: "webhook:sample".into(),
        prompt: "inspect sample".into(),
        interval_ms: None,
        next_fire_at_ms: None,
        watch_baseline: None,
    };
    let one_shot = first.create_schedule(input()).unwrap();
    let sub = first.create_persistent_webhook_watch(input()).unwrap();
    assert_eq!(
        peer.create_persistent_webhook_watch(input()).unwrap().id,
        sub.id
    );
    assert_ne!(one_shot.id, sub.id);
    assert!(!one_shot.persistent);
    let deliver = |store: &Store, id: &str, text: &str, now| {
        store
            .deliver_persistent_webhook_wake(&sub.id, &agent.id, id, text.into(), id, now)
            .unwrap()
    };
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let concurrent = scope.spawn(|| {
            barrier.wait();
            deliver(&peer, "first", "first payload", 1)
        });
        barrier.wait();
        assert_ne!(
            deliver(&first, "first", "first payload", 1),
            concurrent.join().unwrap()
        );
    });
    assert!(deliver(&peer, "second", "second payload", 2));
    let inbox = first.inbox(&agent.id, None).unwrap();
    assert_eq!(
        inbox.iter().map(|m| m.text.as_str()).collect::<Vec<_>>(),
        ["first payload", "second payload"]
    );
    for message in &inbox {
        first.note_delivered(&agent.id, message.seq).unwrap();
    }
    drop(first);
    drop(peer);
    let reopened = Store::open_postgres(&url, dir.path()).unwrap();
    assert!(!deliver(&reopened, "first", "retry", 3));
    let loaded = reopened.get_schedule(&sub.id).unwrap().unwrap();
    assert!(loaded.persistent);
    assert_eq!(loaded.state, "active");
    assert_eq!(loaded.last_fired_at_ms, Some(2));
    assert_eq!(loaded.last_observed.as_deref(), Some("second"));
    assert_eq!(
        reopened.cancel_schedule(Some(&agent.id), &sub.id).unwrap(),
        1
    );
    assert!(!deliver(&reopened, "third", "cancelled", 4));
    assert_eq!(reopened.inbox(&agent.id, None).unwrap().len(), 2);
}

#[test]
fn postgres_end_to_end_store_search_host_index_redaction_and_concurrency() {
    let Some(url) = pg_url() else {
        eprintln!("skipping postgres_store test: ANYAGENTS_TEST_POSTGRES_URL not set");
        return;
    };
    let _guard = test_lock();
    let dir = tempfile::tempdir().unwrap();
    let tag = uuid_tag();

    // Verify Store::open auto-detects `<data_dir>/database-url` too.
    let db_url_dir = dir.path().join("auto-url");
    std::fs::create_dir_all(&db_url_dir).unwrap();
    std::fs::write(db_url_dir.join("database-url"), format!("{url}\n")).unwrap();
    let auto_store = Store::open(db_url_dir.join("state.db")).unwrap();
    assert!(auto_store.is_postgres());
    drop(auto_store);

    let store = Store::open_postgres(&url, dir.path()).unwrap();
    assert!(store.is_postgres());

    // 1. Agent creation, hierarchy, scopes, rosters.
    let root_name = format!("pg_root_{tag}");
    let child_name = format!("pg_child_{tag}");
    let peer_name = format!("pg_peer_{tag}");

    let root = store
        .create_agent(NewAgent::new(&root_name, "codex"))
        .unwrap();
    let mut child_new = NewAgent::new(&child_name, "claude-code");
    child_new.parent_id = Some(root.id.clone());
    child_new.role = Some("builder".into());
    let child = store.create_agent(child_new).unwrap();
    let peer = store
        .create_agent(NewAgent::new(&peer_name, "spiron"))
        .unwrap();

    let root = store.set_agent_state(&root.id, "running").unwrap();
    assert_eq!(root.state, "running");
    let child = store.set_agent_state(&child.id, "running").unwrap();
    let child = store.set_agent_scope(&child.id, "public").unwrap();
    assert_eq!(child.scope(), "public");

    let root_roster = read_root_roster(&db_url_dir);
    assert!(
        root_roster.iter().any(|(n, _, _)| n == &root_name),
        "root roster via database-url should contain {root_name}: {root_roster:?}"
    );
    let children_roster = read_children_roster(&db_url_dir, root.id.as_str());
    assert!(
        children_roster.iter().any(|(n, _, _)| n == &child_name),
        "children roster should contain {child_name}"
    );

    // 2. Origin deduplication + alias coalescing (`message_origin_namespace_matches` CASE SQL).
    let mut first_in = Incoming::new("hello pg needle alpha");
    first_in.sender = Some("telegram:user:100".into());
    first_in.source_key = Some(format!("bot1:upd_{tag}_1"));
    first_in.origin = Some(Origin {
        chat_id: Some(-4242),
        message_id: Some(777),
        thread_id: None,
    });
    let ReceiveOutcome::New(msg1) = store.receive(&root.id, first_in).unwrap() else {
        panic!("expected new inbound message");
    };
    assert_eq!(msg1.seq, 1);

    // Duplicate delivery with same origin & namespace coalesces without failing on MAX(boolean).
    let mut dup_in = Incoming::new("hello pg needle alpha extended");
    dup_in.sender = Some("telegram:user:100".into());
    dup_in.source_key = Some(format!("bot1:upd_{tag}_2"));
    dup_in.origin = Some(Origin {
        chat_id: Some(-4242),
        message_id: Some(777),
        thread_id: None,
    });
    let ReceiveOutcome::Duplicate(msg1_dup) = store.receive(&root.id, dup_in).unwrap() else {
        panic!("expected duplicate coalescing by origin");
    };
    assert_eq!(msg1_dup.id, msg1.id);

    // 3. Cursors, open_turn, outbound, usage, outcomes (SnapshotGuard::Postgres).
    store.note_delivered(&root.id, 1).unwrap();
    assert_eq!(store.delivered_cursor(&root.id).unwrap(), 1);
    let open = store.open_turn(&root.id).unwrap().expect("open turn");
    assert_eq!(open.opened_seq, 1);
    assert_eq!(open.completed_seq, 0);

    let mut second_in = Incoming::new("廣東話搜尋測試 needle beta");
    second_in.source_key = Some(format!("src_{tag}_2"));
    let ReceiveOutcome::New(msg2) = store.receive(&root.id, second_in).unwrap() else {
        panic!("expected msg2");
    };
    assert_eq!(store.advance_delivered(&root.id, msg2.seq).unwrap(), 2);

    store
        .record_outbound(
            &root.id,
            &format!("agent:{child_name}"),
            None,
            "outbound reply needle gamma",
        )
        .unwrap();
    store
        .record_tool_event(&root.id, "bash", "tool output grep_canary")
        .unwrap();

    let conv = store
        .conversation_scoped(&root.id, None, None, Some(10), None)
        .unwrap();
    assert!(conv.len() >= 3, "conv={conv:?}");

    store
        .record_usage(NewUsage {
            agent_id: child.id.clone(),
            harness: "claude-code".into(),
            model: Some("opus".into()),
            input_tokens: 100,
            output_tokens: 50,
            cache_read_tokens: 20,
            cache_write_tokens: 10,
            reported_cost_micros: Some(1234),
            estimated_cost_micros: Some(1234),
            billing_mode: BillingMode::ApiKey,
            quota_state: None,
            day: "2026-11-18".into(),
        })
        .unwrap();
    let usage = store.list_usage(Some(&child.id)).unwrap();
    assert_eq!(usage.len(), 1);

    // 4. Schedules + claim_schedule_tick_with_wake.
    let sched = store
        .create_schedule(NewSchedule {
            agent_id: root.id.clone(),
            kind: "every".into(),
            spec: "30s".into(),
            prompt: "tick".into(),
            interval_ms: Some(30_000),
            next_fire_at_ms: Some(1_000),
            watch_baseline: None,
        })
        .unwrap();
    let active_list = store.list_schedules(Some(&root.id), true).unwrap();
    assert_eq!(active_list.len(), 1);
    let claimed = store
        .claim_schedule_tick_with_wake(
            &sched.id,
            Some(1_000),
            "active",
            Some(31_000),
            Some(1_000),
            None,
            None,
            &root.id,
            Incoming::new("[Schedule tick]"),
        )
        .unwrap();
    assert!(claimed);
    store.cancel_schedule(Some(&root.id), &sched.id).unwrap();

    // 5. High-concurrency multi-thread PG `next_seq` test on SAME agent.
    let shared_store = Arc::new(Store::open_postgres(&url, dir.path()).unwrap());
    let target_id = Arc::new(peer.id.clone());
    let mut handles = Vec::new();
    for t in 0..8usize {
        let s = Arc::clone(&shared_store);
        let aid = Arc::clone(&target_id);
        let tag_c = tag.clone();
        handles.push(std::thread::spawn(move || {
            let mut seqs = Vec::new();
            for i in 0..10usize {
                let mut inc = Incoming::new(format!("concurrent_{tag_c}_{t}_{i}"));
                inc.source_key = Some(format!("conc:{tag_c}:{t}:{i}"));
                let ReceiveOutcome::New(msg) = s.receive(&aid, inc).unwrap() else {
                    panic!("expected unique new message for {t}/{i}");
                };
                seqs.push(msg.seq);
            }
            seqs
        }));
    }
    let mut all_seqs: Vec<i64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all_seqs.sort_unstable();
    let expected: Vec<i64> = (1..=80).collect();
    assert_eq!(all_seqs, expected, "concurrent next_seq must be contiguous");

    // 6. Search (`memory_index` PG FTS + visibility empty set `AND 1 = 0`).
    let cjk_hits = store
        .search_history(&HistorySearchQuery {
            query: Some("廣東話".into()),
            agent: Some(root_name.clone()),
            ..Default::default()
        })
        .unwrap();
    assert!(
        !cjk_hits.is_empty(),
        "expected CJK search hit in PG memory_index"
    );

    let tool_hits = store
        .search_history(&HistorySearchQuery {
            query: Some("grep_canary".into()),
            include_tools: true,
            visible_agent_ids: Some(vec![root.id.to_string()]),
            ..Default::default()
        })
        .unwrap();
    assert!(!tool_hits.is_empty());

    // Fail closed with empty visible set (`AND 1 = 0`, valid boolean in PG).
    let empty_vis = store
        .search_history(&HistorySearchQuery {
            query: Some("needle".into()),
            visible_agent_ids: Some(vec![]),
            ..Default::default()
        })
        .unwrap();
    assert!(empty_vis.is_empty());

    // 7. HostIndex on PostgreSQL (explicit 15-col INSERT, tokens_tsv, -ts_rank_cd, savepoints).
    let seg_path = dir.path().join("raw").join("seg.raw");
    let seg = RawSegment::open(&seg_path).unwrap();
    let payload = r#"{"text":"HostIndexPGNeedle 繁體中文搜尋測試 in segment"}"#.as_bytes();
    let coord = seg.append(payload).unwrap();
    let mut hindex = HostIndex::open_postgres(&url).unwrap();
    let indexed = hindex
        .index(
            &format!("doc_{tag}_1"),
            &IndexMetadata {
                agent_id: root.id.to_string(),
                source: MemoryScope::Transcripts,
                timestamp_ms: Some(123456),
                direction: "in".into(),
                sender: "operator".into(),
                destination: format!("agent:{root_name}"),
                tool_name: Some("bash".into()),
                tool_names: vec!["bash".into()],
                role: Some("user".into()),
            },
            &coord,
        )
        .unwrap();
    assert!(indexed);
    let h_hits = hindex
        .search(
            &[root.id.to_string()],
            MemoryScope::Transcripts,
            &HistorySearchQuery {
                query: Some("HostIndexPGNeedle".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(h_hits.len(), 1);
    let h_cjk = hindex
        .search(
            &[root.id.to_string()],
            MemoryScope::All,
            &HistorySearchQuery {
                query: Some("繁體中文".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(h_cjk.len(), 1);
    let doc_all = hindex
        .document(&[root.id.to_string()], MemoryScope::All, &h_hits[0].id)
        .unwrap()
        .unwrap();
    assert_eq!(doc_all.id, h_hits[0].id);
    let doc_transcript = hindex
        .document(
            &[root.id.to_string()],
            MemoryScope::Transcripts,
            &h_hits[0].id,
        )
        .unwrap()
        .unwrap();
    assert_eq!(doc_transcript.id, h_hits[0].id);

    // 8. Redaction on PostgreSQL.
    store
        .transition_runtime(
            &root.id,
            RuntimeState::Released,
            RuntimeState::Paused,
            RuntimeChange::default(),
        )
        .ok();
    let root = store.set_agent_state(&root.id, "paused").unwrap();
    assert_eq!(root.state, "paused");
    let receipt = store
        .redact_conversation(
            Actor::Operator,
            &root.id,
            &RedactRequest {
                message_id: Some(msg1.id.to_string()),
                seq: None,
                span: None,
                reason: "erase sensitive".into(),
            },
        )
        .unwrap();
    assert!(receipt.full_scrub);
    let _out_of_range = store
        .redact_conversation(
            Actor::Operator,
            &root.id,
            &RedactRequest {
                message_id: Some(msg1.id.to_string()),
                seq: None,
                span: Some(Span {
                    unit: SpanUnit::Bytes,
                    start: 0,
                    end: 999,
                }),
                reason: "out of range".into(),
            },
        )
        .unwrap_err();
    let replay_exact = store
        .redact_conversation(
            Actor::Operator,
            &root.id,
            &RedactRequest {
                message_id: None,
                seq: Some(msg1.seq),
                span: None,
                reason: "erase sensitive".into(),
            },
        )
        .unwrap();
    assert_eq!(replay_exact.audit_id, receipt.audit_id);

    // Redacted origin stays tombstoned on redelivery.
    let mut redeliv = Incoming::new("re-injected secret");
    redeliv.source_key = Some(format!("bot1:upd_{tag}_1"));
    let ReceiveOutcome::Duplicate(tomb) = store.receive(&root.id, redeliv).unwrap() else {
        panic!("expected redacted duplicate");
    };
    assert_ne!(tomb.text, "re-injected secret");

    // 9. Capabilities, operator inbox, remote sessions.
    store.hold_capability_key(&"a".repeat(64)).unwrap();
    let tok = store.issue_capability(&child.id).unwrap();
    let (holder_id, holder_name) = store.capability_holder(&tok).unwrap().unwrap();
    assert_eq!(holder_id, child.id);
    assert_eq!(holder_name.as_deref(), Some(child_name.as_str()));

    let session = store.begin_remote_session(&child.id, 60_000).unwrap();
    let grant = store.authenticate_remote_session(&session.token).unwrap();
    assert_eq!(grant.agent_id, child.id);
}

#[test]
fn sqlite_to_postgres_migration_preserves_data_and_sequences() {
    let Some(url) = pg_url() else {
        eprintln!(
            "skipping sqlite_to_postgres_migration_preserves_data_and_sequences (ANYAGENTS_TEST_POSTGRES_URL unset)"
        );
        return;
    };
    let _guard = test_lock();
    let tag = uuid_tag();
    let src_dir = tempfile::tempdir().unwrap();
    let src_store = Store::open(src_dir.path().join("state.db")).unwrap();

    let parent_name = format!("mig_parent_{tag}");
    let child_name = format!("mig_child_{tag}");
    let arch_name = format!("mig_arch_{tag}");

    let parent = src_store
        .create_agent(NewAgent {
            name: parent_name.clone(),
            harness: "claude-code".into(),
            runtime: None,
            remote_budget_micros: None,
            model: Some("sonnet".into()),
            workdir: Some("/tmp/mig".into()),
            instructions: Some("parent".into()),
            parent_id: None,
            role: Some("director".into()),
            description: None,
        })
        .unwrap();
    let child = src_store
        .create_agent(NewAgent {
            name: child_name.clone(),
            harness: "spiron".into(),
            runtime: Some(anyagents_core::ExecutionRuntime::Cloud),
            remote_budget_micros: Some(5_000_000),
            model: Some("sonnet".into()),
            workdir: Some("/tmp/mig".into()),
            instructions: Some("child".into()),
            parent_id: Some(parent.id.clone()),
            role: Some("worker".into()),
            description: None,
        })
        .unwrap();
    let arch = src_store
        .create_agent(NewAgent {
            name: arch_name.clone(),
            harness: "codex".into(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(parent.id.clone()),
            role: None,
            description: None,
        })
        .unwrap();
    // Simulate archived agent whose runtime row was left at 'released'.
    src_store.set_agent_state(&arch.id, "archived").unwrap();

    let inc = Incoming::new(format!("mig\0nul_{tag} 繁體中文資料遷移測試"));
    let ReceiveOutcome::New(msg) = src_store.receive(&child.id, inc).unwrap() else {
        panic!("expected new message in SQLite");
    };
    let _sqlite_hits = src_store
        .search_history(&HistorySearchQuery {
            query: Some("資料遷移".into()),
            visible_agent_ids: Some(vec![child.id.to_string()]),
            ..Default::default()
        })
        .unwrap();

    // Populate SQLite HostIndex too.
    let seg_path = src_dir.path().join("raw/seg1.raw");
    let seg = RawSegment::open(&seg_path).unwrap();
    let coord = seg.append(br#"{"text":"MigHostNeedle"}"#).unwrap();
    let mut src_hidx = HostIndex::open(
        &src_dir
            .path()
            .join(anyagents_core::host_retrieval::INDEX_FILE),
    )
    .unwrap();
    src_hidx
        .index(
            &format!("migdoc_{tag}"),
            &IndexMetadata {
                agent_id: child.id.to_string(),
                source: MemoryScope::Transcripts,
                timestamp_ms: Some(12345),
                direction: "in".into(),
                sender: "op".into(),
                destination: child_name.clone(),
                tool_name: None,
                tool_names: vec![],
                role: Some("assistant".into()),
            },
            &coord,
        )
        .unwrap();
    drop(src_hidx);
    drop(src_store);

    let opts = anyagents_core::db::MigrateToPostgresOptions::new(src_dir.path(), &url);
    let report = anyagents_core::db::migrate_sqlite_dir_to_postgres(&opts).unwrap();
    assert!(
        report
            .tables
            .iter()
            .any(|t| t.pg_table == "public.agents" && t.sqlite_rows == 3)
    );
    assert!(
        report
            .tables
            .iter()
            .any(|t| t.pg_table == "memory_index.rag_chunks" && t.sqlite_rows >= 1)
    );
    assert!(
        report
            .tables
            .iter()
            .any(|t| t.pg_table == "host_index.host_documents" && t.sqlite_rows == 1)
    );
    assert!(report.reconciled_archived_runtime >= 1);

    let pg_dir = tempfile::tempdir().unwrap();
    let pg_store = Store::open_postgres(&url, pg_dir.path()).unwrap();
    let migrated_child = pg_store.agent(&child.id).unwrap();
    assert_eq!(migrated_child.parent_id.as_ref(), Some(&parent.id));
    assert_eq!(
        migrated_child.runtime,
        anyagents_core::ExecutionRuntime::Cloud
    );
    assert_eq!(migrated_child.remote_budget_micros, Some(5_000_000));

    // Verify NUL sanitization on migrated text and IDENTITY sequence advancement after migration.
    let inbox = pg_store.inbox(&child.id, None).unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].id, msg.id);
    assert!(!inbox[0].text.contains('\0'));
    assert!(inbox[0].text.contains("繁體中文資料遷移測試"));

    let ReceiveOutcome::New(post_mig) = pg_store
        .receive(&child.id, Incoming::new(format!("post_mig_{tag}")))
        .unwrap()
    else {
        panic!("expected post-migration insert");
    };
    assert_eq!(post_mig.seq, 2);

    // Verify migrated CJK text is immediately searchable in PG memory_index.
    let hits = pg_store
        .search_history(&HistorySearchQuery {
            query: Some("資料遷移".into()),
            visible_agent_ids: Some(vec![child.id.to_string()]),
            ..Default::default()
        })
        .unwrap();
    assert!(
        !hits.is_empty(),
        "expected CJK search hit after PG migration"
    );

    // Verify migrated HostIndex document metadata exists on PG.
    let pg_hidx = HostIndex::open_postgres(&url).unwrap();
    let doc = pg_hidx
        .document(
            &[child.id.to_string()],
            MemoryScope::All,
            &format!("migdoc_{tag}"),
        )
        .unwrap()
        .unwrap();
    assert_eq!(doc.metadata.agent_id, child.id.as_str());
}

fn uuid_tag() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", now & 0xffff_ffff_ffff)
}
