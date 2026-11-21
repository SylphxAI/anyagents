use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn an_open_turn_never_quotes_a_message_parked_inside_it() {
    let (_dir, store) = setup();
    let bob = store.create_agent(NewAgent::new("bob", "codex")).unwrap();
    let mut seqs = Vec::new();
    for text in ["first", "refused", "third"] {
        let ReceiveOutcome::New(message) = store.receive(&bob.id, Incoming::new(text)).unwrap()
        else {
            panic!("new message");
        };
        seqs.push(message);
    }
    store.note_delivered(&bob.id, seqs[0].seq).unwrap();
    store
        .reject_message(&bob.id, seqs[1].seq, "too large")
        .unwrap();
    store.note_delivered(&bob.id, seqs[2].seq).unwrap();
    let open = store.open_turn(&bob.id).unwrap().expect("open turn");
    let quoted: Vec<_> = open.messages.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(quoted, [seqs[0].id.as_str(), seqs[2].id.as_str()]);
}

#[test]
fn a_rejected_message_is_parked_past_the_cursor_and_its_agent_sender_told_once() {
    let (_dir, store) = setup();
    let alice = store.create_agent(NewAgent::new("alice", "codex")).unwrap();
    let bob = store.create_agent(NewAgent::new("bob", "codex")).unwrap();
    let mut incoming = Incoming::new("a request bob's vendor cannot take");
    incoming.sender = Some("agent:alice".into());
    let ReceiveOutcome::New(message) = store.receive(&bob.id, incoming).unwrap() else {
        panic!("new message");
    };
    let rejected = store
        .reject_message(&bob.id, message.seq, "invalid params (-32602)")
        .unwrap();
    assert_eq!(rejected.message_id, message.id.as_str());
    assert_eq!(store.delivered_cursor(&bob.id).unwrap(), message.seq);
    // Parked, not delivered: it opened no turn.
    assert!(store.open_turn(&bob.id).unwrap().is_none());
    let parked = store.rejected_messages(&bob.id).unwrap();
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].reason, "invalid params (-32602)");
    assert_eq!(parked[0].sender.as_deref(), Some("agent:alice"));
    assert!(parked[0].notified_at_ms.is_some(), "the notice is recorded");
    // A notice already sent is not sent again.
    store.send_pending_rejection_notices(&bob.id).unwrap();
    let told = store.inbox(&alice.id, None).unwrap();
    assert_eq!(told.len(), 1);
    assert!(told[0].text.starts_with("[Undeliverable]"));
    assert!(told[0].text.contains(message.id.as_str()));
    assert!(told[0].text.contains("-32602"));
    // A message that is not next in line is not parked.
    assert!(store.reject_message(&bob.id, message.seq, "again").is_err());
    assert_eq!(store.inbox(&alice.id, None).unwrap().len(), 1);
    // Re-queued, it is a new message at the end of the mailbox, from the
    // same author with the same body, and no longer parked.
    let requeued = store
        .requeue_rejected(&bob.id, message.id.as_str())
        .unwrap();
    assert!(requeued.seq > message.seq);
    assert_eq!(requeued.text, message.text);
    assert_eq!(requeued.sender.as_deref(), Some("agent:alice"));
    assert_eq!(
        store.next_undelivered(&bob.id).unwrap().unwrap().id,
        requeued.id
    );
    assert!(store.rejected_messages(&bob.id).unwrap().is_empty());
    assert!(
        store
            .requeue_rejected(&bob.id, message.id.as_str())
            .is_err()
    );
    store
        .reject_message(&bob.id, requeued.seq, "again")
        .unwrap();
    // Deleting the agent clears its parked rows.
    store.delete_agent_by(&bob.id, "test").unwrap();
    assert!(store.rejected_messages(&bob.id).unwrap().is_empty());
}

#[test]
fn inbox_query_limits_unread_and_sender_before_materializing() {
    let (_dir, store) = setup();
    let own = agent(&store, "limited");
    for i in 1..=12 {
        let mut incoming = Incoming::new(format!("message {i}\nsecond line"));
        incoming.sender = Some(
            if i % 2 == 0 {
                "agent:even"
            } else {
                "agent:odd"
            }
            .into(),
        );
        store.receive(&own.id, incoming).unwrap();
    }
    for seq in 1..=8 {
        store.note_delivered(&own.id, seq).unwrap();
    }
    let seqs = |rows: Vec<Message>| rows.into_iter().map(|m| m.seq).collect::<Vec<_>>();
    assert_eq!(
        seqs(store.inbox_query(&own.id, None, Some(3), false).unwrap()),
        vec![10, 11, 12]
    );
    assert_eq!(
        seqs(store.inbox_query(&own.id, None, None, true).unwrap()),
        vec![9, 10, 11, 12]
    );
    assert_eq!(
        seqs(
            store
                .inbox_query(&own.id, Some(10), Some(20), true)
                .unwrap()
        ),
        vec![11, 12]
    );
    assert_eq!(
        seqs(
            store
                .inbox_query_filtered(&own.id, None, Some(2), true, Some("agent:odd"))
                .unwrap()
        ),
        vec![9, 11]
    );
    assert!(
        store
            .inbox_query(&own.id, None, Some(0), false)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.inbox_query(&own.id, None, None, false).unwrap().len(),
        12
    );
    let conn = store.conn();
    let plan: String = conn.query_row("EXPLAIN QUERY PLAN SELECT seq FROM messages WHERE agent_id = ?1 AND seq > 8 ORDER BY seq DESC LIMIT 3", [own.id.as_str()], |r| r.get(3)).unwrap();
    assert!(
        plan.contains("SEARCH") && plan.contains("agent_id"),
        "{plan}"
    );
}

#[test]
fn sequence_dedupes_without_consuming_a_number() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let mut first = Incoming::new("one");
    first.source_key = Some("src:1".into());
    first.source_time_ms = Some(50);
    let ReceiveOutcome::New(first) = store.receive(&agent.id, first).unwrap() else {
        panic!("first receive should be new");
    };
    assert_eq!(first.seq, 1);
    assert_eq!(first.ack, Ack::Received);
    let mut again = Incoming::new("one again");
    again.source_key = Some("src:1".into());
    let ReceiveOutcome::Duplicate(again) = store.receive(&agent.id, again).unwrap() else {
        panic!("same source key should dedupe");
    };
    assert_eq!(again.id, first.id);
    assert_eq!(again.seq, 1);
    let mut blank_a = Incoming::new("a");
    blank_a.source_key = Some(String::new());
    let mut blank_b = Incoming::new("b");
    blank_b.source_key = None;
    let ReceiveOutcome::New(a) = store.receive(&agent.id, blank_a).unwrap() else {
        panic!("empty key is not a key");
    };
    let ReceiveOutcome::New(b) = store.receive(&agent.id, blank_b).unwrap() else {
        panic!("missing key is not a key");
    };
    assert_eq!((a.seq, b.seq), (2, 3));
    assert_eq!(store.received_cursor(&agent.id).unwrap(), 3);
}

#[test]
fn late_source_time_gets_the_next_sequence() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let mut early_arrival = Incoming::new("later clock");
    early_arrival.source_time_ms = Some(500);
    let mut late = Incoming::new("earlier clock");
    late.source_time_ms = Some(100);
    let ReceiveOutcome::New(first) = store.receive(&agent.id, early_arrival).unwrap() else {
        panic!("new");
    };
    let ReceiveOutcome::New(second) = store.receive(&agent.id, late).unwrap() else {
        panic!("new");
    };
    assert_eq!((first.seq, second.seq), (1, 2));
    assert!(second.source_time_ms.unwrap() < first.source_time_ms.unwrap());
}

#[test]
fn one_delivery_cursor_refuses_a_gap_and_inbox_is_exclusive() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    store.receive(&agent.id, Incoming::new("a")).unwrap();
    let ReceiveOutcome::New(second) = store.receive(&agent.id, Incoming::new("b")).unwrap() else {
        panic!("new");
    };
    // A cursor always moves one step: it never skips a message, never moves
    // back, and never moves to a row that is not there.
    assert!(store.advance_delivered(&agent.id, 2).is_err());
    assert!(store.advance_delivered(&agent.id, 3).is_err());
    store.note_delivered(&agent.id, 1).unwrap();
    assert!(store.delivered_cursor(&agent.id).unwrap() == 1);
    let all = store.inbox(&agent.id, None).unwrap();
    assert!(all.len() == 2);
    assert!(all[0].ack == Ack::Delivered);
    assert!(all[1].ack == Ack::Received);
    assert!(all[1].id == second.id);
    // The next line to hand over is the one after the cursor.
    let next = store.next_undelivered(&agent.id).unwrap().expect("a line");
    assert!(next.seq == 2);
    assert!(next.ack == Ack::Received);
    assert!(store.advance_delivered(&agent.id, 2).unwrap() == 2);
    assert!(store.delivered_cursor(&agent.id).unwrap() == 2);
    assert!(store.advance_delivered(&agent.id, 2).is_err());
}

/// The live-desk shape before this migration: two cursors, a turn-state
/// flag on every message, and the agent's own answers written back into its
/// inbox. The upgrade must leave one cursor, no stale answer rows, and a
/// cursor that still points at a row that exists.
#[test]
fn upgrading_a_two_cursor_store_keeps_one_cursor_and_drops_the_answer_rows() {
    let dir = crate::test_tempdir();
    let path = dir.path().join("state.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
    )
    .unwrap();
    for (version, sql) in migrate::MIGRATIONS.iter().take(6) {
        conn.execute_batch(sql).unwrap();
        conn.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, 0)",
            [version],
        )
        .unwrap();
    }
    conn.execute_batch(
        "INSERT INTO agents (id, name, harness, state, created_at_ms)
             VALUES ('agt_01h455vb4pex5vsknk084sn02q', 'migrated_worker', 'opencode', 'running', 1);
         INSERT INTO mailbox_cursors (agent_id, accepted_seq, processed_seq)
             VALUES ('agt_01h455vb4pex5vsknk084sn02q', 4, 2);
         INSERT INTO messages (id, agent_id, seq, source_key, body, received_at_ms, delivered_at_ms, processed_at_delivery)
             VALUES ('msg_01h455vb4pex5vsknk084sn02q', 'agt_01h455vb4pex5vsknk084sn02q', 1, NULL, 'one', 1, 5, 0);
         INSERT INTO messages (id, agent_id, seq, source_key, body, received_at_ms, delivered_at_ms, processed_at_delivery)
             VALUES ('msg_01h455vb4pex5vsknk084sn02r', 'agt_01h455vb4pex5vsknk084sn02q', 2, NULL, 'two', 1, 6, 0);
         INSERT INTO messages (id, agent_id, seq, source_key, body, received_at_ms, delivered_at_ms, processed_at_delivery)
             VALUES ('msg_01h455vb4pex5vsknk084sn02s', 'agt_01h455vb4pex5vsknk084sn02q', 3, 'answer:msg_01h455vb4pex5vsknk084sn02q', 'pong', 1, 7, 0);
         INSERT INTO messages (id, agent_id, seq, source_key, body, received_at_ms, delivered_at_ms, processed_at_delivery)
             VALUES ('msg_01h455vb4pex5vsknk084sn02t', 'agt_01h455vb4pex5vsknk084sn02q', 4, NULL, 'four', 1, 8, 1);
         INSERT INTO failed_turns (message_id, agent_id, seq, reason, failed_at_ms)
             VALUES ('msg_01h455vb4pex5vsknk084sn02t', 'agt_01h455vb4pex5vsknk084sn02q', 4, 'boom', 9);",
    )
    .unwrap();
    drop(conn);
    let store = Store::open(&path).unwrap();
    let agent = store.agent_by_name("migrated_worker").unwrap();
    // delivered = the higher of the two old cursors: 4 is kept even though
    // the answer row at 3 was removed, so the upgrade never re-sends what a
    // harness already took.
    assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 4);
    let inbox = store.inbox(&agent.id, None).unwrap();
    let seqs: Vec<i64> = inbox.iter().map(|message| message.seq).collect();
    assert_eq!(seqs, vec![1, 2, 4]);
    assert!(inbox.iter().all(|message| {
        !message
            .source_key
            .as_deref()
            .unwrap_or("")
            .starts_with("answer:")
    }));
    assert!(matches!(
        store.message(&agent.id, "msg_01h455vb4pex5vsknk084sn02s"),
        Ok(None)
    ));
    let columns: Vec<String> = store
        .conn()
        .prepare("SELECT name FROM pragma_table_info('mailbox_cursors')")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(columns, vec!["agent_id", "delivered_seq"]);
    assert!(migrate::current_version(&store.conn()).unwrap() >= 7);
}

#[test]
fn unknown_agent_is_rejected() {
    let (_dir, store) = setup();
    let missing = AgentId::generate();
    let err = store.receive(&missing, Incoming::new("nope")).unwrap_err();
    assert!(matches!(err, Error::AgentNotFound(_)));
}

#[test]
fn lag_uses_the_message_receive_time() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let mut incoming = Incoming::new("held");
    incoming.received_at_ms = Some(1_000);
    store.receive(&agent.id, incoming).unwrap();
    let lag = store.lag(&agent.id, 6_000).unwrap();
    assert_eq!(lag.oldest_undelivered_age_ms, Some(5_000));
    store.advance_delivered(&agent.id, 1).unwrap();
    assert_eq!(
        store
            .lag(&agent.id, 6_000)
            .unwrap()
            .oldest_undelivered_age_ms,
        None
    );
}

#[test]
fn a_new_message_wakes_a_subscriber_and_a_duplicate_does_not() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let wakes = store.subscribe();
    let mut incoming = Incoming::new("hello");
    incoming.source_key = Some("same".into());
    store.receive(&agent.id, incoming.clone()).unwrap();
    assert!(matches!(
        wakes
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        Wake::Mail { .. }
    ));
    store.receive(&agent.id, incoming).unwrap();
    assert!(
        wakes
            .recv_timeout(std::time::Duration::from_millis(200))
            .is_err()
    );
}

#[test]
fn shared_registry_fans_wakes_across_handles_without_wake_socket() {
    let dir = crate::test_tempdir();
    let db = dir.path().join("nested").join("state.db");
    let a = Store::open(&db).unwrap();
    let b = Store::open(&db).unwrap();
    let peer = agent(&a, "wake-peer");
    let wakes = a.subscribe();
    b.receive(&peer.id, Incoming::new("ping")).unwrap();
    assert_eq!(
        wakes.recv_timeout(Duration::from_secs(1)).unwrap(),
        Wake::Mail { agent_id: peer.id }
    );
}

#[test]
fn invalid_json_context_counts_as_actionable_without_scan_parse() {
    let (_dir, store) = setup();
    let worker = agent(&store, "noisy_worker");
    // Invalid JSON in context must count as actionable (same as the
    // old per-row parse, which treated unparseable as not-passive).
    let mut msg = Incoming::new("hello");
    msg.sender = Some("agent:tester".to_string());
    let out = store.receive(&worker.id, msg).unwrap();
    let seq = match out {
        ReceiveOutcome::New(m) => m.seq,
        _ => panic!("expected new message"),
    };
    store
        .conn()
        .execute(
            "UPDATE messages SET context = 'not-json' WHERE agent_id = ?1 AND seq = ?2",
            params![worker.id.as_str(), seq],
        )
        .unwrap();
    assert!(store.has_actionable_undelivered(&worker.id).unwrap());
}

#[test]
fn mark_delivered_up_to_is_monotonic_and_clears_undelivered() {
    let (_dir, store) = setup();
    let worker = agent(&store, "serverless-cursor");
    store.set_agent_running(&worker.id, "/work").unwrap();
    for i in 1..=5 {
        store
            .receive(&worker.id, Incoming::new(format!("batch msg {i}")))
            .unwrap();
    }
    assert_eq!(store.delivered_cursor(&worker.id).unwrap(), 0);
    assert_eq!(store.list_undelivered(&worker.id, 10).unwrap().len(), 5);

    let cur = store.mark_delivered_up_to(&worker.id, 3).unwrap();
    assert_eq!(cur, 3);
    assert_eq!(store.delivered_cursor(&worker.id).unwrap(), 3);
    assert_eq!(store.list_undelivered(&worker.id, 10).unwrap().len(), 2);

    // Monotonic: a smaller sequence never rewinds the cursor.
    let cur2 = store.mark_delivered_up_to(&worker.id, 1).unwrap();
    assert_eq!(cur2, 3);
    assert_eq!(store.delivered_cursor(&worker.id).unwrap(), 3);

    let cur3 = store.mark_delivered_up_to(&worker.id, 5).unwrap();
    assert_eq!(cur3, 5);
    assert!(store.list_undelivered(&worker.id, 10).unwrap().is_empty());
}

#[test]
fn list_running_agents_with_undelivered_orders_oldest_first_and_skips_active_turns() {
    let (_dir, store) = setup();
    let older = agent(&store, "older-unread");
    let newer = agent(&store, "newer-unread");
    store.set_agent_running(&older.id, "/work/a").unwrap();
    store.set_agent_running(&newer.id, "/work/b").unwrap();

    store
        .receive(&older.id, Incoming::new("old backlog"))
        .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    store.receive(&newer.id, Incoming::new("fresh dm")).unwrap();

    let ranked = store.list_running_agents_with_undelivered(10).unwrap();
    assert_eq!(ranked, vec![older.id.clone(), newer.id.clone()]);

    // An agent with an active remote turn must be skipped by tick backlog drain.
    let _ = store
        .remote_turn_store()
        .create_turn(&crate::NewRemoteTurn {
            turn_id: "turn_active_skip_test".to_string(),
            agent_id: older.id.clone(),
            harness_kind: "sandbox".to_string(),
            provider_kind: Some("sylphx".to_string()),
            remote_run_id: None,
            callback_token_hash: "hash".to_string(),
            cursor: None,
            status: "dispatched".to_string(),
            metadata_json: None,
            next_poll_at_ms: None,
        })
        .unwrap();
    let ranked_after = store.list_running_agents_with_undelivered(10).unwrap();
    assert_eq!(ranked_after, vec![newer.id]);
}

#[test]
fn overview_mailbox_counts_match_the_capped_delivery_view() {
    let (_dir, store) = setup();
    let busy = agent(&store, "busy");
    let archived = agent(&store, "gone");
    let quiet = agent(&store, "quiet");
    for _ in 0..104 {
        store.receive(&busy.id, Incoming::new("fixture")).unwrap();
    }
    store
        .receive(&archived.id, Incoming::new("fixture"))
        .unwrap();
    store.set_agent_state(&archived.id, "archived").unwrap();
    let (meta, sandboxes) = store.agent_overview_metadata(&store.agent_summaries(true).unwrap());
    assert_eq!(meta[&busy.id].unread_count, 100);
    assert_eq!(meta[&archived.id].unread_count, 0);
    assert_eq!(meta[&quiet.id].unread_count, 0);
    assert_eq!(sandboxes, 0);
}
