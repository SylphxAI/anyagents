use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn sqlite_resident_cache_limits_cover_writer_reader_and_attachment() {
    let (_dir, store) = setup();
    let check = |conn: &Connection, schema| {
        let cache: i64 = conn
            .pragma_query_value(schema, "cache_size", |r| r.get(0))
            .unwrap();
        let mmap: i64 = conn
            .pragma_query_value(schema, "mmap_size", |r| r.get(0))
            .unwrap();
        assert_eq!(cache, -CACHE_SIZE_KIB);
        assert_eq!(mmap, 0);
    };
    {
        let writer = store.conn();
        check(&writer, None);
        let temporary: i64 = writer
            .pragma_query_value(None, "temp_store", |r| r.get(0))
            .unwrap();
        assert_eq!(temporary, 1, "large sorts must be able to spill to disk");
    }
    let reader = store.read_conn();
    match &reader {
        ReadConn::Pooled { conn: Some(c), .. } | ReadConn::Owned(Some(c)) => {
            check(c, None);
            check(c, Some(memory_index::ATTACHED));
        }
        ReadConn::Guard(c, _) => {
            check(c, None);
            check(c, Some(memory_index::ATTACHED));
        }
        _ => panic!("expected SQLite reader"),
    }
}

#[test]
fn opens_in_wal_and_migrates_once() {
    let (dir, store) = setup();
    let mode: String = store
        .conn()
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    // The newest migration's number, bumped whenever one is added.
    let newest = migrate::MIGRATIONS
        .last()
        .map(|(version, _)| *version)
        .unwrap();
    let version = migrate::current_version(&store.conn()).unwrap();
    assert_eq!(version, newest);
    let again = Store::open(dir.path().join("state.db")).unwrap();
    assert_eq!(migrate::current_version(&again.conn()).unwrap(), newest);
    assert_eq!(again.list_agents().unwrap(), store.list_agents().unwrap());
}

#[test]
fn read_endpoints_and_current_history_do_not_wait_for_writer() {
    let (dir, store) = setup();
    let own = agent(&store, "reader");
    store.hold_capability_key(&"a".repeat(64)).unwrap();
    let token = store.issue_capability(&own.id).unwrap();
    let hidden = agent(&store, "legacy-hidden");
    store
        .record_tool_event(&hidden.id, "browser", "old tool")
        .unwrap();
    let mut keyed = Incoming::new("keyed line");
    keyed.source_key = Some("src:read".into());
    let ReceiveOutcome::New(line) = store.receive(&own.id, keyed).unwrap() else {
        panic!("a new line");
    };
    store.record_send_receipt("send-1", "receipt").unwrap();
    store.join_group("readers", &own.id).unwrap();
    let watch = store
        .create_schedule(NewSchedule {
            agent_id: own.id.clone(),
            kind: "watch".into(),
            spec: "file:/tmp/never".into(),
            prompt: "check".into(),
            interval_ms: None,
            next_fire_at_ms: None,
            watch_baseline: Some("base".into()),
        })
        .unwrap();
    std::thread::scope(|scope| {
        let mut writer = store.conn();
        let tx = writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        tx.execute(
            "UPDATE agents SET description='uncommitted' WHERE id=?1",
            [own.id.as_str()],
        )
        .unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let store = &store;
        let id = &own.id;
        let data_dir = dir.path();
        let db = data_dir.join("state.db");
        scope.spawn(move || {
            let result = (|| -> Result<(), Error> {
                assert_eq!(store.agent(id)?.description, None);
                store.agent_by_name("reader")?;
                assert_eq!(store.authenticate_capability(&token)?, *id);
                store.list_agents()?;
                store.children_of(id)?;
                store.list_channels()?;
                store.share_any_group_or_chat(id, id);
                store.has_messaged(&store.agent(id)?, &store.agent(id)?);
                store.agent_query_metadata()?;
                store.agent_aliases(id)?;
                store.list_schedules(Some(id), true)?;
                store.inbox_query_filtered(id, None, Some(5), false, None)?;
                store.conversation_scoped(id, None, None, Some(5), None)?;
                store.conversation_all_scoped(None, None, Some(5), None)?;
                store.lag(id, now_ms())?;
                store.has_open_turn(id)?;
                store.open_turn(id)?;
                assert_eq!(store.send_receipt("send-1")?.as_deref(), Some("receipt"));
                assert!(store.message_by_source_key(id, "src:read")?.is_some());
                assert!(store.has_message_source_key(id, "src:read")?);
                assert!(store.ever_authenticated());
                assert!(!store.is_group_public("readers"));
                assert_eq!(store.group_members("readers")?, ["reader"]);
                assert_eq!(store.list_groups()?.len(), 1);
                store.group("readers")?;
                store.check_group_joins(&GroupActor::Agent(id.clone()), &["readers"])?;
                assert_eq!(store.inbox(id, None)?.len(), 1);
                store.turn_ownership_snapshot(id)?;
                assert!(store.has_actionable_undelivered(id)?);
                assert!(!store.has_undelivered_message_from(id, "nobody")?);
                let _ = read_root_roster(data_dir);
                let _ = read_children_roster(data_dir, id.as_str());
                assert_eq!(store.received_cursor(id)?, 1);
                assert_eq!(store.delivered_cursor(id)?, 0);
                assert_eq!(
                    store.next_undelivered(id)?.map(|m| m.id),
                    Some(line.id.clone())
                );
                assert!(store.message(id, line.id.as_str())?.is_some());
                store.list_usage(Some(id))?;
                assert!(store.get_schedule(&watch.id)?.is_some());
                // An unchanged observation is decided without the writer.
                store.update_schedule_watch_observation(&watch.id, Some("base"), None)?;
                // Search syncs the index in the index's own file: it
                // never needs the state writer.
                store.search_history(&HistorySearchQuery {
                    visible_agent_ids: Some(vec![id.to_string()]),
                    ..Default::default()
                })?;
                Store::open(db)?.list_agents()?;
                Ok(())
            })();
            send.send(result).unwrap();
        });
        let result = receive.recv_timeout(Duration::from_secs(2));
        // Release before asserting so a regression cannot hang the test join.
        drop(tx);
        drop(writer);
        result
            .expect("read endpoint blocked on the writer")
            .unwrap();
    });
}

fn wal_len(db: &Path) -> u64 {
    std::fs::metadata(db.with_extension("db-wal"))
        .map(|m| m.len())
        .unwrap_or(0)
}

#[test]
fn writer_bounds_the_wal_file_size() {
    let (_dir, store) = setup();
    let limit: i64 = store
        .conn()
        .query_row("PRAGMA journal_size_limit", [], |r| r.get(0))
        .unwrap();
    assert_eq!(limit, WAL_SIZE_LIMIT_BYTES);
}

#[test]
fn wal_checkpoint_truncates_when_no_reader_needs_the_log() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    for i in 0..20 {
        agent(&store, &format!("wal-{i}"));
    }
    assert!(wal_len(&db) > 0);
    let outcome = store.checkpoint_wal().unwrap();
    assert!(!outcome.passive_busy);
    assert_eq!(outcome.checkpointed_frames, outcome.log_frames);
    assert!(outcome.truncated, "{outcome:?}");
    assert_eq!(wal_len(&db), 0);
    // The writer keeps working after the truncate.
    agent(&store, "after-truncate");
    assert!(store.agent_by_name("after-truncate").is_ok());
}

#[test]
fn wal_checkpoint_never_waits_on_a_pinned_reader_and_recovers_after_it() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    agent(&store, "pin-0");
    // A reader holds a snapshot while the writer keeps appending frames.
    let reader = Connection::open(&db).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _: i64 = reader
        .query_row("SELECT count(*) FROM agents", [], |r| r.get(0))
        .unwrap();
    for i in 1..20 {
        agent(&store, &format!("pin-{i}"));
    }
    let started = std::time::Instant::now();
    let pinned = store.checkpoint_wal().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "must not busy-wait"
    );
    assert!(pinned.passive_busy || pinned.checkpointed_frames < pinned.log_frames);
    assert!(!pinned.truncated);
    assert!(wal_len(&db) > 0);

    reader.execute_batch("COMMIT").unwrap();
    drop(reader);
    let freed = store.checkpoint_wal().unwrap();
    assert!(freed.truncated, "{freed:?}");
    assert_eq!(wal_len(&db), 0);
}
