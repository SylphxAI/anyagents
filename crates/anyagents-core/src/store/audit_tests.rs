//! Regression coverage through production Store APIs and the SQLite driver.
use super::*;

fn setup() -> (tempfile::TempDir, Store, Agent) {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
    (dir, store, agent)
}

fn assert_fault(error: Error, needle: &str) {
    assert!(matches!(&error, Error::Db(_)), "{error:?}");
    assert!(error.to_string().contains(needle), "{error:?}");
}

fn reject_chunks(store: &Store) {
    store
        .conn()
        .execute_batch(
            "CREATE TRIGGER reject_chunk BEFORE INSERT ON rag_chunks BEGIN
         SELECT RAISE(ABORT, 'fixture-rag-write'); END;",
        )
        .unwrap();
}

fn count(store: &Store, table: &str) -> i64 {
    store
        .conn()
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

fn assert_fts(store: &Store, term: &str) {
    let n: i64 = store
        .conn()
        .query_row(
            "SELECT count(*) FROM rag_chunks_fts WHERE rag_chunks_fts MATCH ?1",
            [term],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1, "{term}");
}

#[test]
fn current_schema_reopens_while_another_connection_holds_the_writer_lock() {
    let (dir, store, agent) = setup();
    store
        .receive(&agent.id, Incoming::new("indexedneedle"))
        .unwrap();
    let writer = Connection::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let reopened = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    assert_fts(&reopened, "indexedneedle");
    writer.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn only_pre_23_schemas_probe_and_backfill_fts_on_open() {
    for baseline in [22, 23] {
        let dir = crate::test_tempdir();
        let path = dir.path().join("state.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
        ).unwrap();
        for (version, sql) in migrate::MIGRATIONS.iter().filter(|(v, _)| *v <= baseline) {
            conn.execute_batch(sql).unwrap();
            conn.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
                .unwrap();
        }
        conn.execute_batch(
            "INSERT INTO rag_chunks(chunk_id,agent_id,agent_name,direction,sender,destination,timestamp_ms,content_hash,text)
             VALUES ('legacy','agent','worker','in','peer','worker',0,'legacy','legacyftsneedle');",
        ).unwrap();
        drop(conn);
        let store = Store::open(&path).unwrap();
        assert_eq!(count(&store, "rag_chunks_fts"), i64::from(baseline < 23));
        if baseline < 23 {
            assert_fts(&store, "legacyftsneedle");
        }
    }
}

#[test]
fn group_column_upgrade_is_idempotent_and_unexpected_ddl_failure_is_visible() {
    let (dir, store, _) = setup();
    store.set_group_public("shared", true).unwrap();
    assert!(store.is_group_public("shared"));
    drop(store);
    let path = dir.path().join("custom-mailbox.sqlite");
    let store = Store::open(&path).unwrap();
    assert!(store.is_group_public("shared"));
    // Simulate a legacy schema without the runtime-added column.
    store
        .conn()
        .execute_batch("ALTER TABLE groups DROP COLUMN public")
        .unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    store.set_group_public("shared", true).unwrap();
    assert!(store.is_group_public("shared"));
    store
        .conn()
        .execute_batch("DROP TABLE groups; CREATE VIEW groups AS SELECT 'shared' AS name;")
        .unwrap();
    drop(store);
    let error = Store::open(path)
        .err()
        .expect("a view cannot be upgraded as a table");
    assert_fault(error, "view");
}

#[test]
fn legacy_backfill_repairs_partial_duplicate_orphan_and_stale_fts_generations() {
    for fault in ["partial", "duplicate", "orphan", "stale"] {
        let (_dir, store, agent) = setup();
        store
            .receive(&agent.id, Incoming::new("ftsneedlefirst"))
            .unwrap();
        store
            .receive(&agent.id, Incoming::new("ftsneedlesecond"))
            .unwrap();
        let conn = store.conn();
        match fault {
            "partial" => {
                conn.execute(
                    "DELETE FROM rag_chunks_fts WHERE text='ftsneedlesecond'",
                    [],
                )
                .unwrap();
            }
            "duplicate" => {
                conn.execute("INSERT INTO rag_chunks_fts SELECT * FROM rag_chunks_fts WHERE text='ftsneedlefirst'", []).unwrap();
            }
            "orphan" => {
                conn.execute(
                    "UPDATE rag_chunks_fts SET chunk_id='orphan' WHERE text='ftsneedlesecond'",
                    [],
                )
                .unwrap();
            }
            "stale" => {
                conn.execute(
                    "UPDATE rag_chunks_fts SET text='stale' WHERE text='ftsneedlesecond'",
                    [],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        drop(conn);
        ensure_lexical_fts_backfilled(&mut store.conn()).unwrap();
        assert_eq!(count(&store, "rag_chunks_fts"), 2, "{fault}");
        assert_fts(&store, "ftsneedlefirst");
        assert_fts(&store, "ftsneedlesecond");
    }
}

#[test]
fn failed_legacy_fts_backfill_rolls_back_and_retries_the_generation() {
    let (dir, store, agent) = setup();
    store
        .receive(&agent.id, Incoming::new("ftsneedlefirst"))
        .unwrap();
    store
        .receive(&agent.id, Incoming::new("ftsneedlesecond"))
        .unwrap();
    // A real SQL constraint injects a later insert failure after one good row.
    store.conn().execute_batch(
        "DROP TABLE rag_chunks_fts;
         CREATE TABLE rag_chunks_fts(chunk_id,agent_name,sender,destination,kind,tool_name,text,
             search_tokens,CONSTRAINT fixture_fts_write CHECK(text != 'ftsneedlesecond'));
         INSERT INTO rag_chunks_fts SELECT chunk_id,agent_name,sender,destination,kind,tool_name,text,text
             FROM rag_chunks WHERE text='ftsneedlefirst';",
    ).unwrap();
    assert_fault(
        ensure_lexical_fts_backfilled(&mut store.conn()).unwrap_err(),
        "fixture_fts_write",
    );
    drop(store);
    let path = dir.path().join("custom-mailbox.sqlite");
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM rag_chunks_fts", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    conn.execute_batch(
        "DROP TABLE rag_chunks_fts;
         CREATE VIRTUAL TABLE rag_chunks_fts USING fts5(chunk_id,agent_name,sender,destination,
             kind,tool_name,text,search_tokens,tokenize='unicode61');",
    )
    .unwrap();
    drop(conn);
    let store = Store::open(&path).unwrap();
    ensure_lexical_fts_backfilled(&mut store.conn()).unwrap();
    assert_fts(&store, "ftsneedlefirst");
    assert_fts(&store, "ftsneedlesecond");
}

#[test]
fn incoming_outgoing_and_tool_index_failures_refuse_receipts_and_roll_back() {
    let (dir, store, agent) = setup();
    store.set_agent_state(&agent.id, "idle").unwrap();
    reject_chunks(&store);
    let mut incoming = Incoming::new("inboundneedle");
    incoming.source_key = Some("retry-one".into());
    assert_fault(
        store.receive(&agent.id, incoming.clone()).unwrap_err(),
        "fixture-rag-write",
    );
    assert_fault(
        store
            .record_outbound(&agent.id, "peer", None, "outboundneedle")
            .unwrap_err(),
        "fixture-rag-write",
    );
    assert_fault(
        store
            .record_tool_event(&agent.id, "browser", "toolneedle")
            .unwrap_err(),
        "fixture-rag-write",
    );
    assert!(store.inbox(&agent.id, None).unwrap().is_empty());
    assert_eq!(store.agent(&agent.id).unwrap().state, "idle");
    assert_eq!(count(&store, "outbound_messages"), 0);
    assert_eq!(count(&store, "rag_chunks"), 0);
    store
        .conn()
        .execute_batch("DROP TRIGGER reject_chunk")
        .unwrap();
    let ReceiveOutcome::New(message) = store.receive(&agent.id, incoming.clone()).unwrap() else {
        panic!("first accepted receipt");
    };
    assert_eq!(message.seq, 1);
    assert!(matches!(
        store.receive(&agent.id, incoming).unwrap(),
        ReceiveOutcome::Duplicate(_)
    ));
    store
        .record_outbound(&agent.id, "peer", None, "outboundneedle")
        .unwrap();
    store
        .record_tool_event(&agent.id, "browser", "toolneedle")
        .unwrap();
    drop(store);
    let store = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    assert_eq!(count(&store, "rag_chunks"), 3);
    for term in ["inboundneedle", "outboundneedle", "toolneedle"] {
        assert_fts(&store, term);
    }
}

#[test]
fn authorized_upgrade_reads_only_legacy_chunks_without_indexing_hidden_bodies() {
    let (dir, store, own) = setup();
    let hidden = store.create_agent(NewAgent::new("hidden", "test")).unwrap();
    store
        .receive(&own.id, Incoming::new("legacyinboundneedle"))
        .unwrap();
    store
        .record_outbound(&own.id, "peer", None, "legacyoutboundneedle")
        .unwrap();
    store.receive(&own.id, Incoming::new("x")).unwrap();
    store
        .receive(&hidden.id, Incoming::new("hiddenlegacyneedle"))
        .unwrap();
    store
        .conn()
        // Ingress maintains chunk membership transactionally. Only legacy text
        // needs repair; searching must not enumerate the entire raw mailbox.
        .execute_batch(
            "UPDATE rag_chunks SET text='legacy prefix',text_format=1; DELETE FROM rag_chunks_fts;",
        )
        .unwrap();
    drop(store);
    let store = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    for term in [
        "legacyinboundneedle",
        "legacyoutboundneedle",
        "hiddenlegacyneedle",
    ] {
        let result = store
            .search_history(&HistorySearchQuery {
                query: Some(term.into()),
                visible_agent_ids: Some(vec![own.id.to_string()]),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.len(), usize::from(term != "hiddenlegacyneedle"));
    }
    assert_eq!(count(&store, "rag_chunks"), 3);
    let hidden_format: i64 = store
        .conn()
        .query_row(
            "SELECT text_format FROM rag_chunks WHERE agent_id=?1",
            [hidden.id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(hidden_format, 1);
    assert_fts(&store, "legacyinboundneedle");
    assert_fts(&store, "legacyoutboundneedle");
}

#[test]
fn leaving_all_groups_treats_arbitrary_and_former_reserved_names_equally() {
    let (_dir, store, own) = setup();
    let other = store.create_agent(NewAgent::new("other", "test")).unwrap();
    for name in ["owners", "support", "team"] {
        for member in [&own, &other] {
            store.join_group(name, &member.id).unwrap();
        }
    }
    store.leave_all_groups(&own.id).unwrap();
    store.leave_all_groups(&own.id).unwrap();
    for name in ["owners", "support", "team"] {
        assert_eq!(store.group_members(name).unwrap(), vec!["other"]);
    }
}

#[test]
fn schedule_claim_and_wake_share_a_transaction_and_retry_without_loss() {
    for state in ["idle", "running", "paused"] {
        schedule_claim_and_wake_retry_in_state(state);
    }
}

fn schedule_claim_and_wake_retry_in_state(state: &str) {
    let (dir, store, agent) = setup();
    store.set_agent_state(&agent.id, "idle").unwrap();
    let schedule = store
        .create_schedule(NewSchedule {
            agent_id: agent.id.clone(),
            kind: "at".into(),
            spec: "1m".into(),
            prompt: "timerneedle".into(),
            interval_ms: None,
            next_fire_at_ms: Some(100),
            watch_baseline: None,
        })
        .unwrap();
    let deliver = |store: &Store, target: &AgentId, expected| {
        let mut incoming = Incoming::new("timerneedle");
        incoming.source_key = Some(format!("{}:100", schedule.id));
        store.claim_schedule_tick_with_wake(
            &schedule.id,
            Some(expected),
            "fired",
            None,
            Some(100),
            None,
            Some("fired"),
            target,
            incoming,
        )
    };
    let other = store
        .create_agent(NewAgent::new("wrong-recipient", "test"))
        .unwrap();
    assert!(!deliver(&store, &other.id, 100).unwrap());
    // Retirement rejects publication; pause only delays normal mailbox delivery.
    store.set_agent_state(&agent.id, "archived").unwrap();
    assert!(!deliver(&store, &agent.id, 100).unwrap());
    store.set_agent_state(&agent.id, state).unwrap();
    assert!(!deliver(&store, &agent.id, 99).unwrap());
    reject_chunks(&store);
    assert_fault(
        deliver(&store, &agent.id, 100).unwrap_err(),
        "fixture-rag-write",
    );
    let pending = store.list_schedules(Some(&agent.id), true).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].next_fire_at_ms, Some(100));
    assert_eq!(pending[0].last_fired_at_ms, None);
    assert!(store.inbox(&agent.id, None).unwrap().is_empty());
    assert_eq!(store.agent(&agent.id).unwrap().state, state);
    store
        .conn()
        .execute_batch("DROP TRIGGER reject_chunk")
        .unwrap();
    assert!(deliver(&store, &agent.id, 100).unwrap());
    assert!(!deliver(&store, &agent.id, 100).unwrap());
    drop(store);
    let store = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    assert!(
        store
            .list_schedules(Some(&agent.id), true)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
    assert_eq!(
        store.agent(&agent.id).unwrap().state,
        if state == "paused" {
            "paused"
        } else {
            "running"
        }
    );
    assert_fts(&store, "timerneedle");
}

#[test]
fn outbound_effect_history_replays_once_and_refuses_changed_payloads() {
    let (dir, store, agent) = setup();
    reject_chunks(&store);
    assert_fault(
        store
            .record_outbound_once("effect-1", &agent.id, "peer", None, "settlementneedle")
            .unwrap_err(),
        "fixture-rag-write",
    );
    assert_eq!(count(&store, "outbound_messages"), 0);
    store
        .conn()
        .execute_batch("DROP TRIGGER reject_chunk")
        .unwrap();
    let id = store
        .record_outbound_once("effect-1", &agent.id, "peer", None, "settlementneedle")
        .unwrap();
    let sequence: i64 = store
        .conn()
        .query_row("SELECT seq FROM rag_chunks WHERE chunk_id=?1", [&id], |r| {
            r.get(0)
        })
        .unwrap();
    store
        .receive(&agent.id, Incoming::new("new later inbound"))
        .unwrap();
    let before = store.conversation(&agent.id, None, None, None).unwrap();
    drop(store);
    let store = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    assert_eq!(
        store
            .record_outbound_once("effect-1", &agent.id, "peer", None, "settlementneedle")
            .unwrap(),
        id
    );
    assert_eq!(
        store.conversation(&agent.id, None, None, None).unwrap(),
        before
    );
    for (destination, reply, body) in [
        ("other", None, "settlementneedle"),
        ("peer", Some("reply"), "settlementneedle"),
        ("peer", None, "changed"),
    ] {
        let error = store
            .record_outbound_once("effect-1", &agent.id, destination, reply, body)
            .unwrap_err();
        assert!(error.to_string().contains("payload conflict"), "{error:?}");
    }
    assert!(
        store
            .record_outbound_once(" ", &agent.id, "peer", None, "body")
            .unwrap_err()
            .to_string()
            .contains("empty outbound effect key")
    );
    assert_eq!(count(&store, "outbound_messages"), 1);
    assert_eq!(count(&store, "rag_chunks_fts"), 2);
    let replay_sequence: i64 = store
        .conn()
        .query_row("SELECT seq FROM rag_chunks WHERE chunk_id=?1", [&id], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(replay_sequence, sequence);
    assert_fts(&store, "settlementneedle");
    let other = store.create_agent(NewAgent::new("other", "test")).unwrap();
    assert_ne!(
        store
            .record_outbound_once("effect-1", &other.id, "peer", None, "settlementneedle")
            .unwrap(),
        id
    );
    assert_eq!(count(&store, "outbound_messages"), 2);
}

#[test]
fn concurrent_schedule_claims_publish_exactly_one_searchable_wake() {
    let (dir, store, agent) = setup();
    let schedule = store
        .create_schedule(NewSchedule {
            agent_id: agent.id.clone(),
            kind: "at".into(),
            spec: "1m".into(),
            prompt: "concurrenttimerneedle".into(),
            interval_ms: None,
            next_fire_at_ms: Some(100),
            watch_baseline: None,
        })
        .unwrap();
    let path = dir.path().join("custom-mailbox.sqlite");
    let stores = [Store::open(&path).unwrap(), Store::open(&path).unwrap()];
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for store in stores {
            let barrier = barrier.clone();
            let schedule = &schedule;
            let agent = &agent;
            threads.push(scope.spawn(move || {
                barrier.wait();
                store
                    .claim_schedule_tick_with_wake(
                        &schedule.id,
                        Some(100),
                        "fired",
                        None,
                        Some(100),
                        None,
                        None,
                        &agent.id,
                        Incoming::new("concurrenttimerneedle"),
                    )
                    .unwrap()
            }));
        }
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.into_iter().filter(|claimed| *claimed).count(), 1);
    assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
    assert_fts(&store, "concurrenttimerneedle");
}

#[test]
fn fts_rebuild_propagates_chunk_decode_errors_without_committing_partial_rows() {
    let (dir, store, agent) = setup();
    store
        .receive(&agent.id, Incoming::new("decodeneedle"))
        .unwrap();
    store
        .conn()
        .execute("UPDATE rag_chunks SET sender=X'FF'", [])
        .unwrap();
    drop(store);
    let path = dir.path().join("custom-mailbox.sqlite");
    let store = Store::open(&path).unwrap();
    let error = ensure_lexical_fts_backfilled(&mut store.conn()).unwrap_err();
    assert!(
        matches!(error, Error::Db(rusqlite::Error::InvalidColumnType(..))),
        "{error:?}"
    );
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM rag_chunks_fts", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    conn.execute("UPDATE rag_chunks SET sender='unknown'", [])
        .unwrap();
    drop(conn);
    assert_fts(&Store::open(path).unwrap(), "decodeneedle");
}

#[test]
fn late_fts_insert_failure_rolls_back_already_inserted_mail_and_chunk() {
    let (_dir, store, agent) = setup();
    store
        .conn()
        .execute_batch(
            "DROP TABLE rag_chunks_fts;
         CREATE TABLE rag_chunks_fts(chunk_id,agent_name,sender,destination,kind,tool_name,text,
            search_tokens,CONSTRAINT fixture_fts_write CHECK(text != 'lateindexneedle'));",
        )
        .unwrap();
    assert_fault(
        store
            .receive(&agent.id, Incoming::new("lateindexneedle"))
            .unwrap_err(),
        "fixture_fts_write",
    );
    assert!(store.inbox(&agent.id, None).unwrap().is_empty());
    assert_eq!(count(&store, "rag_chunks"), 0);
    store
        .conn()
        .execute_batch(
            "DROP TABLE rag_chunks_fts;
         CREATE VIRTUAL TABLE rag_chunks_fts USING fts5(chunk_id,agent_name,sender,destination,
             kind,tool_name,text,search_tokens,tokenize='unicode61');",
        )
        .unwrap();
    store
        .receive(&agent.id, Incoming::new("lateindexneedle"))
        .unwrap();
    assert_fts(&store, "lateindexneedle");
}

#[test]
fn immutable_sender_metadata_survives_id_name_collision_rename_and_reuse() {
    let (dir, store, _) = setup();
    let mut source = NewAgent::new("sender-original", "test");
    source.role = Some("Source capability".into());
    let source = store.create_agent(source).unwrap();
    let mut child = NewAgent::new("recipient-child", "test");
    child.parent_id = Some(source.id.clone());
    let child = store.create_agent(child).unwrap();
    let mut impostor = NewAgent::new(source.id.as_str(), "test");
    impostor.role = Some("Impostor capability".into());
    store.create_agent(impostor).unwrap();
    let send = |store: &Store, from: &str, text: &str, role: Option<&str>, rel: Option<&str>| {
        let mut incoming = Incoming::new(text);
        incoming.sender = Some(format!("agent:{from}"));
        let ReceiveOutcome::New(message) = store.receive(&child.id, incoming).unwrap() else {
            panic!("new metadata fixture message");
        };
        assert_eq!(message.context.sender_role.as_deref(), role, "{from}");
        assert_eq!(message.context.sender_rel.as_deref(), rel, "{from}");
    };
    send(
        &store,
        source.id.as_str(),
        "before rename",
        Some("Source capability"),
        Some("parent"),
    );
    store.rename_agent(&source.id, "sender-renamed").unwrap();
    let mut reused = NewAgent::new("sender-original", "test");
    reused.role = Some("Reused name capability".into());
    store.create_agent(reused).unwrap();
    drop(store);
    let store = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    send(
        &store,
        source.id.as_str(),
        "after rename and reopen",
        Some("Source capability"),
        Some("parent"),
    );
    send(
        &store,
        "sender-original",
        "legacy current name",
        Some("Reused name capability"),
        Some("peer"),
    );
    send(
        &store,
        "sender-renamed",
        "legacy renamed name",
        Some("Source capability"),
        Some("parent"),
    );
    let missing = AgentId::generate();
    let mut named_like_missing = NewAgent::new(missing.as_str(), "test");
    named_like_missing.role = Some("Not an ID owner".into());
    store.create_agent(named_like_missing).unwrap();
    let before = store.inbox(&child.id, None).unwrap();
    let mut incoming = Incoming::new("unknown ID never falls back to a display name");
    incoming.sender = Some(format!("agent:{missing}"));
    let error = store.receive(&child.id, incoming).unwrap_err();
    assert!(
        matches!(error, Error::AgentNotFound(ref id) if id == missing.as_str()),
        "{error:?}"
    );
    assert_eq!(store.inbox(&child.id, None).unwrap(), before);
}

#[test]
fn room_author_exclusion_uses_immutable_id_without_display_name_collision() {
    let (_dir, store, source) = setup();
    let collision = store
        .create_agent(NewAgent::new(source.id.as_str(), "test"))
        .unwrap();
    let peer = store
        .create_agent(NewAgent::new("room-peer", "test"))
        .unwrap();
    for member in [&source, &collision, &peer] {
        store.join_group("room-fixture", &member.id).unwrap();
    }
    let send = |from: &str, text: &str| {
        let mut incoming = Incoming::new(text);
        incoming.sender = Some(format!("agent:{from}"));
        store
            .receive_room("group:room-fixture", incoming)
            .unwrap()
            .into_iter()
            .map(|(agent, _)| agent.id.to_string())
            .collect::<std::collections::BTreeSet<_>>()
    };
    let mut expected =
        std::collections::BTreeSet::from([collision.id.to_string(), peer.id.to_string()]);
    assert_eq!(send(source.id.as_str(), "ID room message"), expected);
    store
        .rename_agent(&source.id, "room-author-renamed")
        .unwrap();
    let reused = store
        .create_agent(NewAgent::new(&source.name, "test"))
        .unwrap();
    store.join_group("room-fixture", &reused.id).unwrap();
    expected.insert(reused.id.to_string());
    assert_eq!(send(source.id.as_str(), "ID after rename"), expected);
    assert_eq!(send("room-author-renamed", "legacy name control"), expected);
    let missing = AgentId::generate();
    let named_like_missing = store
        .create_agent(NewAgent::new(missing.as_str(), "test"))
        .unwrap();
    store
        .join_group("room-fixture", &named_like_missing.id)
        .unwrap();
    let before = count(&store, "messages");
    let mut incoming = Incoming::new("missing ID excludes no display-name owner");
    incoming.sender = Some(format!("agent:{missing}"));
    let error = store.receive_room("room-fixture", incoming).unwrap_err();
    assert!(
        matches!(error, Error::AgentNotFound(ref id) if id == missing.as_str()),
        "{error:?}"
    );
    assert_eq!(count(&store, "messages"), before);
}

#[test]
fn deleted_immutable_sender_refuses_receive_before_wake_mail_or_index_mutation() {
    let (_dir, store, recipient) = setup();
    let source = store
        .create_agent(NewAgent::new("sender-to-delete", "test"))
        .unwrap();
    // The display-name lookalike must not substitute for the deleted identity.
    store
        .create_agent(NewAgent::new(source.id.as_str(), "test"))
        .unwrap();
    let mut incoming = Incoming::new("liveauthorneedle");
    incoming.sender = Some(format!("agent:{}", source.id));
    incoming.source_key = Some("live-author".into());
    assert!(matches!(
        store.receive(&recipient.id, incoming.clone()).unwrap(),
        ReceiveOutcome::New(_)
    ));
    store.delete_agent(&source.id).unwrap();
    store.set_agent_state(&recipient.id, "idle").unwrap();
    let before = store.inbox(&recipient.id, None).unwrap();
    let chunks = count(&store, "rag_chunks");
    for source_key in ["new-after-delete", "live-author"] {
        incoming.text = "deletedauthorneedle".into();
        incoming.source_key = Some(source_key.into());
        let error = store.receive(&recipient.id, incoming.clone()).unwrap_err();
        assert!(
            matches!(error, Error::AgentNotFound(ref id) if id == source.id.as_str()),
            "{error:?}"
        );
        assert_eq!(store.inbox(&recipient.id, None).unwrap(), before);
        assert_eq!(store.agent(&recipient.id).unwrap().state, "idle");
        assert_eq!(count(&store, "rag_chunks"), chunks);
    }
    // An absent synthetic name was never an immutable principal. Preserve the
    // legacy ingestion contract instead of broadening this check to all names.
    incoming.sender = Some("agent:legacy-synthetic-name".into());
    incoming.source_key = Some("legacy-name".into());
    assert!(matches!(
        store.receive(&recipient.id, incoming).unwrap(),
        ReceiveOutcome::New(_)
    ));
    assert_eq!(
        store.inbox(&recipient.id, None).unwrap().len(),
        before.len() + 1
    );
}

/// Search rows filed under `name`, found through the index.
fn fts_rows_named(store: &Store, name: &str) -> i64 {
    store
        .conn()
        .query_row(
            "SELECT count(*) FROM rag_chunks_fts WHERE agent_name MATCH ?1 AND agent_name = ?2",
            params![fts_exact_phrase(name), name],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn deleting_or_renaming_an_agent_changes_only_its_own_search_rows() {
    let (_dir, store, kept) = setup();
    let leaving = store
        .create_agent(NewAgent::new("leaving", "test"))
        .unwrap();
    let renamed = store
        .create_agent(NewAgent::new("before-rename", "test"))
        .unwrap();
    for (agent, text) in [
        (&kept, "keptneedle"),
        (&leaving, "leavingneedle"),
        (&renamed, "renamedneedle"),
    ] {
        store.receive(&agent.id, Incoming::new(text)).unwrap();
    }
    assert_eq!(fts_rows_named(&store, "leaving"), 1);

    store.delete_agent(&leaving.id).unwrap();
    assert_eq!(fts_rows_named(&store, "leaving"), 0);
    assert_fts(&store, "keptneedle");
    assert_eq!(count(&store, "rag_chunks_fts"), count(&store, "rag_chunks"));

    store.rename_agent(&renamed.id, "after-rename").unwrap();
    assert_eq!(fts_rows_named(&store, "before-rename"), 0);
    assert_eq!(fts_rows_named(&store, "after-rename"), 1);
    assert_eq!(fts_rows_named(&store, "worker"), 1);
    assert_fts(&store, "renamedneedle");
}

/// Pages a statement reads on a connection without mmap: every page access
/// goes through the pager and counts (hit or miss), so the number does not
/// depend on what is cached or on timing.
fn pages_read(conn: &Connection, sql: &str, params: &[&dyn rusqlite::ToSql]) -> (i64, i64) {
    fn accesses(conn: &Connection) -> i64 {
        let mut total = 0;
        for op in [
            rusqlite::ffi::SQLITE_DBSTATUS_CACHE_HIT,
            rusqlite::ffi::SQLITE_DBSTATUS_CACHE_MISS,
        ] {
            let (mut current, mut highwater) = (0, 0);
            // SAFETY: the handle is this live connection's; the call only
            // reads its counters.
            unsafe {
                rusqlite::ffi::sqlite3_db_status(
                    conn.handle(),
                    op,
                    &mut current,
                    &mut highwater,
                    0,
                );
            }
            total += i64::from(current);
        }
        total
    }
    let before = accesses(conn);
    let rows: i64 = conn.query_row(sql, params, |r| r.get(0)).unwrap();
    (rows, accesses(conn) - before)
}

/// A store shaped like the fleet's: `fleet_rows` chunks of one large agent,
/// every id starting with the `msg` token, plus one small agent.
fn fleet_store(fleet_rows: usize) -> (tempfile::TempDir, Store, Agent, Connection) {
    let (dir, store, small) = setup();
    for i in 0..20 {
        store
            .receive(&small.id, Incoming::new(format!("smallneedle {i}")))
            .unwrap();
    }
    {
        let conn = store.conn();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..fleet_rows {
            let id = format!("msg_01m7{:022}", i * 7919);
            let text = format!("fleet history line {i} about the build and its review");
            tx.execute(
                "INSERT INTO rag_chunks (chunk_id, agent_id, agent_name, direction, sender,
                     destination, timestamp_ms, content_hash, text, kind, tool_name, seq, text_format)
                 VALUES (?1, 'agt_fleet', 'fleet', 'in', 'agent:x', 'agent:fleet', ?2, 'h', ?3,
                     'message', '', ?2, 2)",
                params![id, i as i64, text],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO rag_chunks_fts (chunk_id, agent_name, sender, destination, kind,
                     tool_name, text, search_tokens)
                 VALUES (?1, 'fleet', 'agent:x', 'agent:fleet', 'message', '', ?2, ?2)",
                params![id, text],
            )
            .unwrap();
        }
        // One merged segment, so page counts compare across fleet sizes
        // without depending on how many segments the inserts left.
        tx.execute(
            "INSERT INTO rag_chunks_fts(rag_chunks_fts) VALUES('optimize')",
            [],
        )
        .unwrap();
        tx.commit().unwrap();
    }
    let reader = Connection::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    reader.pragma_update(None, "mmap_size", 0).unwrap();
    (dir, store, small, reader)
}

/// The rows a maintenance statement touches, read instead of written.
fn as_count(statement: &str) -> String {
    let count = statement.replacen(
        "DELETE FROM rag_chunks_fts",
        "SELECT count(*) FROM rag_chunks_fts",
        1,
    );
    assert_ne!(count, statement, "{statement}");
    count
}

/// Pages each maintenance lookup reads on a fleet of `fleet` rows: the
/// unique-token chunk lookup, one agent's rows, and the name scan the
/// latter replaced.
fn maintenance_pages(fleet: usize) -> [i64; 3] {
    let (_dir, _store, small, reader) = fleet_store(fleet);
    let id = format!("msg_01m7{:022}", (fleet / 2) * 7919);
    let lookup = fts_chunk_lookup(&id).unwrap();
    let (found, token) = pages_read(&reader, &as_count(FTS_DELETE_CHUNK), &[&id, &lookup]);
    assert_eq!(found, 1);
    let (own, agent) = pages_read(
        &reader,
        &as_count(FTS_DELETE_AGENT_ROWS),
        &[&"worker", &fts_exact_phrase("worker"), &small.id.as_str()],
    );
    assert_eq!(own, 20);
    let (_, scan) = pages_read(
        &reader,
        "SELECT count(*) FROM rag_chunks_fts WHERE agent_name = ?1",
        &[&"worker"],
    );
    [token, agent, scan]
}

#[test]
fn search_index_maintenance_cost_does_not_grow_with_the_fleet() {
    // Report #4/#5: a failed spawn's rollback scanned the whole index with
    // the writer held. Cost is counted in pages read, so it does not depend
    // on caching or timing. On a fleet eight times larger, a name scan reads
    // about eight times the pages; what maintenance reads grows only with the
    // B-tree's depth.
    let small = maintenance_pages(2_000);
    let large = maintenance_pages(16_000);
    eprintln!("pages [lookup, agent rows, scan] at 2k rows {small:?}, at 16k rows {large:?}");
    let [token_s, agent_s, scan_s] = small;
    let [token_l, agent_l, scan_l] = large;
    assert!(scan_l > scan_s * 6, "scan control: {scan_s} -> {scan_l}");
    assert!(
        token_l * 2 < token_s * 3,
        "chunk lookup grew: {token_s} -> {token_l}"
    );
    assert!(
        agent_l < agent_s * 2,
        "agent rows grew: {agent_s} -> {agent_l}"
    );
    assert!(
        agent_l * 5 < scan_l,
        "agent rows {agent_l} against a scan of {scan_l}"
    );
}

#[test]
fn deleting_renaming_and_rolling_back_keep_the_index_exact_on_a_fleet() {
    let fleet = 3_000;
    let (_dir, store, small, _reader) = fleet_store(fleet);
    let failed = store
        .create_agent(NewAgent::new("failed-spawn", "test"))
        .unwrap();
    store.delete_agent(&failed.id).unwrap();
    store.rename_agent(&small.id, "renamed-small").unwrap();
    assert_eq!(fts_rows_named(&store, "renamed-small"), 20);
    assert_eq!(fts_rows_named(&store, "worker"), 0);
    assert_eq!(fts_rows_named(&store, "fleet"), fleet as i64);
    store.delete_agent(&small.id).unwrap();
    assert_eq!(fts_rows_named(&store, "renamed-small"), 0);
    assert_eq!(fts_rows_named(&store, "fleet"), fleet as i64);
    assert_eq!(count(&store, "rag_chunks_fts"), count(&store, "rag_chunks"));
}

#[test]
fn reindexing_a_chunk_replaces_only_its_own_search_row() {
    let (_dir, store, _small, _reader) = fleet_store(50);
    let id = format!("msg_01m7{:022}", 7 * 7919);
    let longer = format!("{id}_part");
    {
        let conn = store.conn();
        conn.execute(
            "INSERT INTO rag_chunks_fts (chunk_id, agent_name, sender, destination, kind,
                 tool_name, text, search_tokens)
             VALUES (?1, 'fleet', 'agent:x', 'agent:fleet', 'message', '', 'part', 'part')",
            [&longer],
        )
        .unwrap();
        assert_eq!(delete_fts_chunk(&conn, &id).unwrap(), 1);
        let left: i64 = conn
            .query_row(
                "SELECT count(*) FROM rag_chunks_fts WHERE chunk_id MATCH ?2 AND chunk_id = ?1",
                params![longer, fts_chunk_lookup(&longer).unwrap()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 1, "a chunk whose id extends this one keeps its row");
    }
    assert_eq!(fts_chunk_lookup("msg_01ABC"), Some("\"01ABC\"".to_string()));
    assert_eq!(fts_chunk_lookup("___"), None);
}
