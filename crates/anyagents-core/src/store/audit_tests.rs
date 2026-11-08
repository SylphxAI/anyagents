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
fn reopen_repairs_partial_duplicate_orphan_and_stale_fts_generations() {
    for fault in ["partial", "duplicate", "orphan", "stale"] {
        let (dir, store, agent) = setup();
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
        drop(store);
        let store = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
        assert_eq!(count(&store, "rag_chunks_fts"), 2, "{fault}");
        assert_fts(&store, "ftsneedlefirst");
        assert_fts(&store, "ftsneedlesecond");
    }
}

#[test]
fn failed_fts_rebuild_rolls_back_and_reopen_retries_the_generation() {
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
    drop(store);
    let path = dir.path().join("custom-mailbox.sqlite");
    assert_fault(Store::open(&path).err().unwrap(), "fixture_fts_write");
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
fn authorized_repair_enumerates_missing_chunks_without_indexing_hidden_bodies() {
    let (dir, store, own) = setup();
    let hidden = store.create_agent(NewAgent::new("hidden", "test")).unwrap();
    store
        .receive(&own.id, Incoming::new("missinginboundneedle"))
        .unwrap();
    store
        .record_outbound(&own.id, "peer", None, "missingoutboundneedle")
        .unwrap();
    store.receive(&own.id, Incoming::new("x")).unwrap();
    store
        .receive(&hidden.id, Incoming::new("hiddenmissingneedle"))
        .unwrap();
    store
        .conn()
        .execute_batch("DELETE FROM rag_chunks; DELETE FROM rag_chunks_fts;")
        .unwrap();
    drop(store);
    let store = Store::open(dir.path().join("custom-mailbox.sqlite")).unwrap();
    for term in [
        "missinginboundneedle",
        "missingoutboundneedle",
        "hiddenmissingneedle",
    ] {
        let result = store
            .search_history(&HistorySearchQuery {
                query: Some(term.into()),
                visible_agent_ids: Some(vec![own.id.to_string()]),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.len(), usize::from(term != "hiddenmissingneedle"));
    }
    assert_eq!(count(&store, "rag_chunks"), 2);
    assert_fts(&store, "missinginboundneedle");
    assert_fts(&store, "missingoutboundneedle");
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
    for state in ["paused", "archived"] {
        store.set_agent_state(&agent.id, state).unwrap();
        assert!(!deliver(&store, &agent.id, 100).unwrap());
    }
    store.set_agent_state(&agent.id, "idle").unwrap();
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
    assert_eq!(store.agent(&agent.id).unwrap().state, "idle");
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
    assert_eq!(store.agent(&agent.id).unwrap().state, "running");
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
