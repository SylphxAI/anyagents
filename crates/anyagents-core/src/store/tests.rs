use super::helpers::decode_env;
use super::ingress::HISTORY_READ_PROBE;
use super::topology::has_quoted_block;
use super::*;
use crate::db_params;
use crate::id::UsageId;
use crate::quote::Quote;

fn setup() -> (tempfile::TempDir, Store) {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    (dir, store)
}

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

fn agent(store: &Store, name: &str) -> Agent {
    store
        .create_agent(NewAgent::new(name, "claude-code"))
        .unwrap()
}

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
fn batch_metadata_matches_single_agent_queries() {
    let (_dir, store) = setup();
    let own = agent(&store, "before-rename");
    let own = store.rename_agent(&own.id, "after-rename").unwrap();
    for i in 0..3 {
        store
            .receive(&own.id, Incoming::new(format!("batch {i}")))
            .unwrap();
    }
    let metadata = store.agent_query_metadata().unwrap();
    assert_eq!(
        metadata.aliases.get(own.id.as_str()).unwrap(),
        &store.agent_aliases(&own.id).unwrap()
    );
    assert_eq!(
        metadata
            .oldest_undelivered
            .get(own.id.as_str())
            .map(|t| 123456789_i64.saturating_sub(*t).max(0)),
        store
            .lag(&own.id, 123456789)
            .unwrap()
            .oldest_undelivered_age_ms
    );
    assert_eq!(
        metadata.open_turns.contains_key(own.id.as_str()),
        store.has_open_turn(&own.id).unwrap()
    );
    // Batched lag must skip passive reactions just like the single-agent path.
    store
        .conn()
        .execute(
            "UPDATE messages SET context = ?1 WHERE agent_id = ?2 AND seq = 1",
            params![r#"{"event":"reaction_passive"}"#, own.id.as_str()],
        )
        .unwrap();
    let now = crate::now_ms();
    let metadata = store.agent_query_metadata().unwrap();
    assert_eq!(
        metadata
            .oldest_undelivered
            .get(own.id.as_str())
            .map(|t| now.saturating_sub(*t).max(0)),
        store.lag(&own.id, now).unwrap().oldest_undelivered_age_ms
    );
    for seq in 1..=3 {
        store.note_delivered(&own.id, seq).unwrap();
    }
    let metadata = store.agent_query_metadata().unwrap();
    assert!(!metadata.oldest_undelivered.contains_key(own.id.as_str()));
    assert!(metadata.open_turns.contains_key(own.id.as_str()));
    store.note_turn_completed(&own.id).unwrap();
    assert!(
        !store
            .agent_query_metadata()
            .unwrap()
            .open_turns
            .contains_key(own.id.as_str())
    );
    store.set_agent_state(&own.id, "archived").unwrap();
    assert!(
        !store
            .agent_query_metadata()
            .unwrap()
            .oldest_undelivered
            .contains_key(own.id.as_str())
    );
}

#[test]
fn lexical_dedup_keeps_distinct_tails_but_collapses_identical_broadcasts() {
    let (_dir, store) = setup();
    let own = agent(&store, "dedup-own");
    let other = agent(&store, "dedup-other");
    let prefix = "shared-prefix ".repeat(300);
    let first = format!("{prefix} dedupneedle first-tail");
    let second = format!("{prefix} dedupneedle second-tail");
    store
        .record_outbound(&own.id, "agent:peer", None, &first)
        .unwrap();
    store
        .record_outbound(&other.id, "agent:peer", None, &second)
        .unwrap();
    store
        .record_outbound(&other.id, "agent:peer", None, &first)
        .unwrap();
    let hits = store
        .search_history(&HistorySearchQuery {
            query: Some("dedupneedle".into()),
            visible_agent_ids: Some(vec![own.id.to_string(), other.id.to_string()]),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(hits.iter().any(|h| h.text == first));
    assert!(hits.iter().any(|h| h.text == second));
}

#[test]
fn a_chat_room_reaches_only_the_agents_whose_bot_is_on_that_chat() {
    let (_dir, store) = setup();
    let alice = agent(&store, "alice");
    let bob = agent(&store, "bob");
    let carol = agent(&store, "carol");
    // Two bots on chat A, one bot on chat B.
    for (name, chat) in [(&alice, -1001_i64), (&bob, -1001), (&carol, -2002)] {
        let channel = store
            .create_channel(NewChannel {
                kind: "telegram".to_string(),
                identity: None,
                config: "{}".to_string(),
                credential_ref: None,
            })
            .unwrap();
        store.attach_channel(&channel.id, &name.id).unwrap();
        store
            .set_channel_state(
                &channel.id,
                &format!(
                    "{{\"chats\":[{{\"agent\":\"{}\",\"chat_id\":{chat}}}]}}",
                    name.name
                ),
            )
            .unwrap();
    }

    let one = |text: &str, from: &str| Incoming {
        text: text.to_string(),
        sender: Some(from.to_string()),
        source_time_ms: None,
        source_key: None,
        reply_to: None,
        quote: None,
        origin: None,
        received_at_ms: None,
    };

    // A post in chat A reaches the other bot on chat A, and no one on B.
    let delivered = store
        .receive_room(&(-1001_i64).to_string(), one("in A", "agent:alice"))
        .unwrap();
    let names: Vec<&str> = delivered
        .iter()
        .map(|(agent, _)| agent.name.as_str())
        .collect();
    assert_eq!(names, vec!["bob"], "{names:?}");
    assert!(
        !store
            .inbox(&carol.id, None)
            .unwrap()
            .iter()
            .any(|m| m.text == "in A")
    );

    // A post in chat B reaches only its own bot.
    let delivered = store
        .receive_room("telegram:chat:-2002", one("in B", "agent:carol"))
        .unwrap();
    assert!(delivered.is_empty(), "{delivered:?}");

    // A room that is neither a chat nor a group reaches nobody. A name the
    // server does not know must never fan out to every agent.
    let delivered = store
        .receive_room("crew", one("everyone", "agent:alice"))
        .unwrap();
    assert!(delivered.is_empty(), "{delivered:?}");
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
fn channel_state_compare_set_writes_only_over_its_own_snapshot() {
    let (_dir, store) = setup();
    let owner = agent(&store, "alpha");
    let channel = store
        .create_channel(NewChannel {
            kind: "telegram".into(),
            identity: Some("bot".into()),
            config: "{}".into(),
            credential_ref: Some("channels/one".into()),
        })
        .unwrap();
    store.attach_channel(&channel.id, &owner.id).unwrap();
    store
        .set_channel_state(&channel.id, r#"{"offset":1}"#)
        .unwrap();
    let snapshot = store.channel(&channel.id).unwrap().unwrap();
    let state_of = |store: &Store| store.channel(&channel.id).unwrap().unwrap().state;

    assert!(
        store
            .compare_set_channel_state(&snapshot, &owner, r#"{"offset":2}"#)
            .unwrap()
    );
    assert_eq!(state_of(&store), r#"{"offset":2}"#);

    // The state moved on since the snapshot: a stale write is refused.
    assert!(
        !store
            .compare_set_channel_state(&snapshot, &owner, r#"{"offset":9}"#)
            .unwrap()
    );
    assert_eq!(state_of(&store), r#"{"offset":2}"#);

    // The owner's lifecycle changed since it was read: refused.
    let snapshot = store.channel(&channel.id).unwrap().unwrap();
    store.set_agent_state(&owner.id, "paused").unwrap();
    assert!(
        !store
            .compare_set_channel_state(&snapshot, &owner, r#"{"offset":9}"#)
            .unwrap()
    );
    let owner = store.agent(&owner.id).unwrap();

    // The owner no longer holds the channel: refused.
    store.detach_channel_agents(&channel.id).unwrap();
    assert!(
        !store
            .compare_set_channel_state(&snapshot, &owner, r#"{"offset":9}"#)
            .unwrap()
    );
    assert_eq!(state_of(&store), r#"{"offset":2}"#);
    assert!(
        store
            .compare_set_channel_state(&snapshot, &owner, "not json")
            .is_err()
    );
}

#[test]
fn channel_attaches_to_an_agent() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let channel = store
        .create_channel(NewChannel {
            kind: "telegram".into(),
            identity: Some("bot".into()),
            config: "{}".into(),
            credential_ref: Some("channels/one".into()),
        })
        .unwrap();
    store.attach_channel(&channel.id, &agent.id).unwrap();
    let listed = store.list_channels().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].agents, vec![agent.id.clone()]);
    assert_eq!(listed[0].credential_ref.as_deref(), Some("channels/one"));
    assert!(!format!("{listed:?}").contains("token"));
    let missing = ChannelId::generate();
    assert!(matches!(
        store.attach_channel(&missing, &agent.id).unwrap_err(),
        Error::ChannelNotFound(_)
    ));
}

#[test]
fn usage_round_trips() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let record = store
        .record_usage(NewUsage {
            agent_id: agent.id.clone(),
            harness: "claude-code".into(),
            model: Some("model".into()),
            input_tokens: 3,
            output_tokens: 4,
            cache_read_tokens: 1,
            cache_write_tokens: 0,
            reported_cost_micros: None,
            estimated_cost_micros: Some(12),
            billing_mode: BillingMode::Subscription,
            quota_state: None,
            day: "2026-10-18".into(),
        })
        .unwrap();
    assert_eq!(record.input_tokens, 3);
    assert_eq!(store.list_usage(Some(&agent.id)).unwrap(), vec![record]);
}

fn usage_record(agent_id: &AgentId, at_ms: i64, day: &str, model: &str) -> UsageRecord {
    UsageRecord {
        id: UsageId::generate(),
        agent_id: agent_id.clone(),
        harness: "harness-a".into(),
        model: Some(model.into()),
        input_tokens: 10,
        output_tokens: 20,
        cache_read_tokens: 3,
        cache_write_tokens: 4,
        reported_cost_micros: Some(5),
        estimated_cost_micros: None,
        billing_mode: BillingMode::Subscription,
        quota_state: None,
        day: day.into(),
        recorded_at_ms: at_ms,
    }
}

#[test]
fn aggregate_usage_groups_and_windows() {
    let (_dir, store) = setup();
    let alpha = agent(&store, "alpha");
    let beta = agent(&store, "beta");
    let now = 1_000_000_000_000i64;
    let day_ms = 86_400_000i64;
    let records = vec![
        usage_record(&alpha.id, now - day_ms, "2026-10-17", "m1"),
        usage_record(&alpha.id, now, "2026-10-18", "m1"),
        usage_record(&alpha.id, now, "2026-10-18", "m2"),
        usage_record(&beta.id, now, "2026-10-18", "m1"),
    ];
    let names = std::collections::HashMap::from([
        (alpha.id.clone(), alpha.name.clone()),
        (beta.id.clone(), beta.name.clone()),
    ]);
    let ids = vec![alpha.id.clone(), beta.id.clone()];
    // Inclusive window: only the two records at `now`.
    let report = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            since_ms: Some(now),
            until_ms: Some(now),
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(report.totals.turns, 3);
    assert_eq!(report.totals.input_tokens, 30);
    assert_eq!(report.totals.reported_cost_micros, Some(15));
    // group_by model -> alpha splits into m1/m2, beta is m1.
    let by_model = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            group_by: UsageGroupBy::Model,
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(by_model.buckets.len(), 3);
    assert_eq!(by_model.buckets[0].model.as_deref(), Some("m1"));
    // group_by day -> alpha has two days.
    let by_day = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            group_by: UsageGroupBy::Day,
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(by_day.buckets.len(), 3);
    // days=1 keeps everything recorded within the last day (all four).
    let by_days = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            days: Some(1),
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(by_days.totals.turns, 4);
    // group_by harness collapses to one bucket per agent.
    let by_harness = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            group_by: UsageGroupBy::Harness,
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(by_harness.buckets.len(), 2);
    assert!(
        by_harness
            .buckets
            .iter()
            .all(|b| b.harness.as_deref() == Some("harness-a"))
    );
    // Bad inputs fail rather than guessing.
    assert!(
        UsageFilter {
            days: Some(0),
            ..UsageFilter::default()
        }
        .window(now)
        .is_err()
    );
    assert!(UsageGroupBy::parse("week").is_err());
}

#[test]
fn resolve_usage_agents_enforces_causal_boundary() {
    let (_dir, store) = setup();
    let root = agent(&store, "root");
    let child_new = {
        let mut n = NewAgent::new("child", "claude-code");
        n.parent_id = Some(root.id.clone());
        n
    };
    let child = store.create_agent(child_new).unwrap();
    let grand_new = {
        let mut n = NewAgent::new("grandchild", "claude-code");
        n.parent_id = Some(child.id.clone());
        n
    };
    let grandchild = store.create_agent(grand_new).unwrap();
    let sib_new = {
        let mut n = NewAgent::new("sibling", "claude-code");
        n.parent_id = Some(root.id.clone());
        n
    };
    let sibling = store.create_agent(sib_new).unwrap();
    let agents = store.list_agents().unwrap();
    let caller = agents.iter().find(|a| a.id == child.id).unwrap();

    // Omitted target: caller's own subtree only.
    let ids = resolve_usage_agents(Some(caller), &agents, None, None).unwrap();
    assert!(ids.contains(&child.id));
    assert!(ids.contains(&grandchild.id));
    assert!(!ids.contains(&root.id));
    assert!(!ids.contains(&sibling.id));
    // Self, child and grandchild are visible; parent and sibling are not.
    for name in ["self", "child", "grandchild"] {
        assert!(
            resolve_usage_agents(Some(caller), &agents, Some(name), None).is_ok(),
            "{name} should be visible"
        );
    }
    for name in ["root", "sibling", "all", "*"] {
        assert!(
            matches!(
                resolve_usage_agents(Some(caller), &agents, Some(name), None),
                Err(UsageAccessError::Forbidden(_))
            ),
            "{name} should be forbidden"
        );
    }
    assert!(matches!(
        resolve_usage_agents(Some(caller), &agents, Some("ghost"), None),
        Err(UsageAccessError::Invalid(_))
    ));
    // Explicit target without the subtree switch is exact-agent only.
    let exact = resolve_usage_agents(Some(caller), &agents, Some("child"), None).unwrap();
    assert_eq!(exact, vec![child.id.clone()]);
    let subtree = resolve_usage_agents(Some(caller), &agents, Some("child"), Some(true)).unwrap();
    assert_eq!(subtree.len(), 2);
    // A public-scope caller and a root caller are privileged.
    store.set_agent_scope(&child.id, "public").unwrap();
    let agents = store.list_agents().unwrap();
    let public_caller = agents.iter().find(|a| a.id == child.id).unwrap();
    let all = resolve_usage_agents(Some(public_caller), &agents, Some("all"), None).unwrap();
    assert_eq!(all.len(), 4);
    assert!(resolve_usage_agents(Some(public_caller), &agents, Some("root"), None).is_ok());
    let root_caller = agents.iter().find(|a| a.id == root.id).unwrap();
    assert_eq!(
        resolve_usage_agents(Some(root_caller), &agents, None, None)
            .unwrap()
            .len(),
        4
    );
    // The local operator (`None`) is privileged too.
    assert_eq!(
        resolve_usage_agents(None, &agents, Some("sibling"), Some(true)).unwrap(),
        vec![sibling.id.clone()]
    );
}

#[test]
fn second_lock_is_busy_until_the_first_drops() {
    let (dir, store) = setup();
    let held_agent = agent(&store, "alpha");
    let other = agent(&store, "beta");
    let held = store.try_lock_agent(&held_agent.id).unwrap();
    let again = Store::open(dir.path().join("state.db")).unwrap();
    assert!(matches!(
        again.try_lock_agent(&held_agent.id).unwrap_err(),
        Error::AgentBusy
    ));
    let _other_lock = store.try_lock_agent(&other.id).unwrap();
    drop(held);
    again.try_lock_agent(&held_agent.id).unwrap();
}

#[test]
fn dropping_agent_lock_releases_it_with_an_inherited_descriptor_open() {
    let (_dir, store) = setup();
    let owner = agent(&store, "owner");
    let lock = store.try_lock_agent(&owner.id).unwrap();
    // A concurrent fork can inherit this open file description until exec.
    // A clone deterministically models that lifetime without timing a fork.
    let inherited = lock.flock_file().try_clone().unwrap();
    drop(lock);
    let _next_turn = store.try_lock_agent(&owner.id).unwrap();
    drop(inherited);
}

#[test]
fn a_shared_copy_holds_the_lock_only_until_the_owner_releases_it() {
    let (_dir, store) = setup();
    let owner = agent(&store, "owner");
    let lock = store.try_lock_agent(&owner.id).unwrap();
    let keeper = lock.share().unwrap();
    // A copy never unlocks: dropping it leaves the owner holding the lock.
    drop(keeper);
    assert!(matches!(
        store.try_lock_agent(&owner.id),
        Err(Error::AgentBusy)
    ));
    let keeper = lock.share().unwrap();
    drop(lock);
    // The owner released the open file description for every copy.
    let _next = store.try_lock_agent(&owner.id).unwrap();
    drop(keeper);
}

#[test]
fn a_retained_lock_stays_held_by_the_copy_a_keeper_keeps() {
    let (_dir, store) = setup();
    let owner = agent(&store, "owner");
    let lock = store.try_lock_agent(&owner.id).unwrap();
    let keeper = lock.share().unwrap();
    let kernel_copy = lock.share().unwrap();
    kernel_copy.retain();
    drop(kernel_copy);
    drop(lock);
    // The keeper's descriptor still holds it after the kernel let go.
    assert!(matches!(
        store.try_lock_agent(&owner.id),
        Err(Error::AgentBusy)
    ));
    // The next kernel adopts the keeper's descriptor and owns release.
    let adopted = AgentLock::adopt(std::os::fd::OwnedFd::from(
        keeper.flock_file().try_clone().unwrap(),
    ));
    drop(keeper);
    assert!(matches!(
        store.try_lock_agent(&owner.id),
        Err(Error::AgentBusy)
    ));
    drop(adopted);
    let _next = store.try_lock_agent(&owner.id).unwrap();
}

#[test]
fn dropping_lifecycle_lock_releases_it_with_an_inherited_descriptor_open() {
    let (_dir, store) = setup();
    let owner = agent(&store, "owner");
    for nonblocking in [false, true] {
        let lock = if nonblocking {
            store.try_lock_agent_lifecycle(&owner.id).unwrap()
        } else {
            store.lock_agent_lifecycle(&owner.id).unwrap()
        };
        // A duplicate shares the open file description inherited by fork,
        // even when the descriptor is close-on-exec. No fork timing needed.
        let inherited = lock.flock_file().try_clone().unwrap();
        assert!(matches!(
            store.try_lock_agent_lifecycle(&owner.id),
            Err(Error::AgentBusy)
        ));
        drop(lock);
        let _next_lifecycle = store.try_lock_agent_lifecycle(&owner.id).unwrap();
        drop(inherited);
    }
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
fn failed_creation_releases_its_name_and_admitted_agents_keep_theirs() {
    let (_dir, store) = setup();
    let failed = agent(&store, "reserved");
    store
        .set_agent_workdir_if_missing(&failed.id, "/allocated")
        .unwrap();
    assert_eq!(store.agent(&failed.id).unwrap().state, "created");

    let released = store.release_unadmitted_agent(&failed.id).unwrap();
    assert_eq!(released.name, failed.id.as_str());
    assert_eq!(released.state, "archived");
    assert_eq!(released.workdir, None);
    assert_eq!(store.agent(&failed.id).unwrap().workdir, None);
    assert!(matches!(
        store.agent_by_name("reserved"),
        Err(Error::AgentNotFound(_))
    ));
    assert_eq!(store.agent(&failed.id).unwrap().id, failed.id);
    agent(&store, "reserved");

    for state in ["running", "paused", "archived"] {
        let admitted = agent(&store, state);
        store.set_agent_running(&admitted.id, "/work").unwrap();
        store.set_agent_state(&admitted.id, state).unwrap();
        let refused = store.release_unadmitted_agent(&admitted.id).unwrap_err();
        assert!(refused.to_string().contains("was admitted"), "{refused}");
        let retained = store.agent(&admitted.id).unwrap();
        assert_eq!(retained.name, state);
        assert_eq!(retained.state, state);
        assert_eq!(retained.workdir.as_deref(), Some("/work"));
    }
}

#[test]
fn startup_repair_releases_every_ghost_reservation_once() {
    let (_dir, store) = setup();
    let interrupted = agent(&store, "interrupted");
    let allocated = agent(&store, "allocated");
    store
        .set_agent_workdir_if_missing(&allocated.id, "/allocated")
        .unwrap();
    let ghost = agent(&store, "ghost");
    store.set_agent_state(&ghost.id, "archived").unwrap();
    let running = agent(&store, "running");
    store.set_agent_running(&running.id, "/work").unwrap();
    let retired = agent(&store, "retired");
    store.set_agent_running(&retired.id, "/work").unwrap();
    store.set_agent_state(&retired.id, "archived").unwrap();

    let mut released = store.release_unadmitted_names().unwrap();
    released.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(
        released,
        vec![
            (allocated.id.clone(), "allocated".to_string()),
            (ghost.id.clone(), "ghost".to_string()),
            (interrupted.id.clone(), "interrupted".to_string()),
        ]
    );
    assert!(store.release_unadmitted_names().unwrap().is_empty());
    for released in [&allocated, &ghost, &interrupted] {
        let retained = store.agent(&released.id).unwrap();
        assert_eq!(retained.name, released.id.as_str());
        assert_eq!(retained.state, "archived");
        assert_eq!(retained.workdir, None);
        agent(&store, &released.name);
    }
    assert_eq!(store.agent(&running.id).unwrap().name, "running");
    assert_eq!(store.agent(&retired.id).unwrap().name, "retired");
}

#[test]
fn allocated_workdir_is_durable_without_starting_or_waking() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let wakes = store.subscribe();
    let allocated = store
        .set_agent_workdir_if_missing(&agent.id, "/allocated")
        .unwrap();
    assert_eq!(allocated.state, agent.state);
    assert_eq!(allocated.workdir.as_deref(), Some("/allocated"));
    assert_eq!(store.agent(&agent.id).unwrap().workdir, allocated.workdir);
    let unchanged = store
        .set_agent_workdir_if_missing(&agent.id, "/stale")
        .unwrap();
    assert_eq!(unchanged.workdir, allocated.workdir);
    assert!(wakes.try_recv().is_err());
    assert!(matches!(
        store
            .set_agent_workdir_if_missing(&AgentId::generate(), "/missing")
            .unwrap_err(),
        Error::AgentNotFound(_)
    ));
}

#[test]
fn set_agent_running_stores_the_workdir_and_wakes() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let wakes = store.subscribe();
    let running = store.set_agent_running(&agent.id, "/work").unwrap();
    assert_eq!(running.state, "running");
    assert_eq!(running.workdir.as_deref(), Some("/work"));
    assert!(matches!(
        wakes
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        Wake::Agent { .. }
    ));
    let missing = AgentId::generate();
    assert!(matches!(
        store.set_agent_running(&missing, "/work").unwrap_err(),
        Error::AgentNotFound(_)
    ));
}

#[test]
fn utc_day_and_usage_sums() {
    assert_eq!(utc_day(0), "1970-01-01");
    assert_eq!(utc_day(1_700_000_000_000), "2023-11-14");
    let alpha = AgentId::generate();
    let beta = AgentId::generate();
    let records = [
        sample_usage(alpha.clone(), "2023-11-14", 1, Some(5)),
        sample_usage(alpha.clone(), "2023-11-15", 4, None),
        sample_usage(beta.clone(), "2023-11-14", 2, Some(7)),
    ];
    let by_agent = sum_usage(&records, false);
    assert_eq!(by_agent.len(), 2);
    let alpha_total = by_agent.iter().find(|row| row.agent_id == alpha).unwrap();
    assert_eq!(alpha_total.day, None);
    assert_eq!(alpha_total.input_tokens, 5);
    assert_eq!(alpha_total.reported_cost_micros, Some(5));
    let by_day = sum_usage(&records, true);
    assert_eq!(by_day.len(), 3);
    assert!(
        by_day
            .iter()
            .any(|row| row.day.as_deref() == Some("2023-11-15")
                && row.reported_cost_micros.is_none())
    );
}

fn sample_usage(agent_id: AgentId, day: &str, input: i64, cost: Option<i64>) -> UsageRecord {
    UsageRecord {
        id: crate::UsageId::generate(),
        agent_id,
        harness: "claude-code".into(),
        model: None,
        input_tokens: input,
        output_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        reported_cost_micros: cost,
        estimated_cost_micros: None,
        billing_mode: BillingMode::Subscription,
        quota_state: None,
        day: day.into(),
        recorded_at_ms: 0,
    }
}

/// A named group has its own membership: a line reaches its members, a
/// non-member receives nothing, and an unknown name reaches nobody.
#[test]
fn a_named_group_reaches_only_its_own_members() {
    let (_dir, store) = setup();
    let alice = agent(&store, "alice");
    let bob = agent(&store, "bob");
    let carol = agent(&store, "carol");
    store.join_group("crew", &alice.id).unwrap();
    store.join_group("crew", &bob.id).unwrap();

    let one = |text: &str, from: &str| Incoming {
        text: text.to_string(),
        sender: Some(from.to_string()),
        source_time_ms: None,
        source_key: None,
        reply_to: None,
        quote: None,
        origin: None,
        received_at_ms: None,
    };

    // A human line reaches every member.
    let delivered = store
        .receive_room("group:crew", one("hi all", "telegram:user:1"))
        .unwrap();
    let names: Vec<&str> = delivered
        .iter()
        .map(|(agent, _)| agent.name.as_str())
        .collect();
    assert_eq!(names, vec!["alice", "bob"], "{names:?}");
    assert!(
        !store
            .inbox(&carol.id, None)
            .unwrap()
            .iter()
            .any(|m| m.text == "hi all")
    );

    // An agent line reaches the other member, never the author.
    let delivered = store
        .receive_room("crew", one("from alice", "agent:alice"))
        .unwrap();
    let names: Vec<&str> = delivered
        .iter()
        .map(|(agent, _)| agent.name.as_str())
        .collect();
    assert_eq!(names, vec!["bob"], "{names:?}");

    // An unknown group reaches nobody rather than everybody.
    let delivered = store
        .receive_room("group:nobody", one("lost", "agent:alice"))
        .unwrap();
    assert!(delivered.is_empty(), "{delivered:?}");
}

/// A named group survives a restart: membership is stored, not held in
/// memory, and a message already stored is still readable afterwards.
#[test]
fn a_group_message_survives_a_restart() {
    let (dir, store) = setup();
    let alice = agent(&store, "alice");
    let bob = agent(&store, "bob");
    store.join_group("crew", &alice.id).unwrap();
    store.join_group("crew", &bob.id).unwrap();
    store
        .receive_room(
            "group:crew",
            Incoming {
                text: "kept".to_string(),
                sender: Some("agent:alice".to_string()),
                source_time_ms: None,
                source_key: None,
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: None,
            },
        )
        .unwrap();
    drop(store);

    let reopened = Store::open(dir.path().join("state.db")).unwrap();
    assert_eq!(
        reopened.group_members("crew").unwrap(),
        vec!["alice", "bob"]
    );
    let bob_id = reopened.agent_by_name("bob").unwrap().id;
    assert!(
        reopened
            .inbox(&bob_id, None)
            .unwrap()
            .iter()
            .any(|message| message.text == "kept")
    );
}

#[test]
fn origin_dedup_mentions_pinned_and_auto_wake() {
    let (_dir, store) = setup();
    let alice = agent(&store, "alice");
    let bob = agent(&store, "bob");
    let ch = store
        .create_channel(NewChannel {
            kind: "telegram".into(),
            identity: Some("999:bob_helper_bot".into()),
            config: "{}".into(),
            credential_ref: Some("secret-ref".into()),
        })
        .unwrap();
    store.attach_channel(&ch.id, &bob.id).unwrap();
    store
        .set_channel_state(
            &ch.id,
            r#"{"bot":"999:bob_helper_bot","offset":null,"chats":[]}"#,
        )
        .unwrap();

    // Put alice in idle (scaled-to-zero), then send a message with @bob_helper_bot, @alice, and pinned event.
    store.set_agent_state(&alice.id, "idle").unwrap();
    assert_eq!(store.agent_by_name("alice").unwrap().state, "idle");

    let origin = Origin {
        chat_id: Some(-100123),
        thread_id: Some(7),
        message_id: Some(555),
    };
    let outcome = store
        .receive(
            &alice.id,
            Incoming {
                text: "[pinned message] Hey @bob_helper_bot and @alice please check".to_string(),
                sender: Some("telegram:user:42:555".to_string()),
                source_time_ms: None,
                source_key: Some("100:1".to_string()),
                reply_to: Some("telegram:chat:-100123:7".to_string()),
                quote: None,
                origin: Some(origin),
                received_at_ms: None,
            },
        )
        .unwrap();
    let ReceiveOutcome::New(msg) = outcome else {
        panic!("expected New");
    };
    // Alice auto-woke to running!
    assert_eq!(store.agent_by_name("alice").unwrap().state, "running");
    // Context includes event: pinned and resolved mentions!
    assert_eq!(msg.context.event.as_deref(), Some("pinned"));
    assert!(
        msg.context
            .mentions
            .contains(&"@bob_helper_bot (agent:bob)".to_string()),
        "{:?}",
        msg.context.mentions
    );
    assert!(
        msg.context
            .mentions
            .contains(&"@alice (agent:alice)".to_string()),
        "{:?}",
        msg.context.mentions
    );

    // A second delivery of the same (chat_id, message_id) from a peer bot relay with a different source_key is deduplicated!
    let dup = store
        .receive(
            &alice.id,
            Incoming {
                text: "[pinned message] Hey @bob_helper_bot and @alice please check".to_string(),
                sender: Some("telegram:user:42:555".to_string()),
                source_time_ms: None,
                source_key: Some("peer:100:1:alice".to_string()),
                reply_to: Some("telegram:chat:-100123:7".to_string()),
                quote: None,
                origin: Some(origin),
                received_at_ms: None,
            },
        )
        .unwrap();
    assert!(matches!(dup, ReceiveOutcome::Duplicate(_)));
    assert_eq!(store.inbox(&alice.id, None).unwrap().len(), 1);
}

#[test]
fn scoped_history_limits_every_candidate_path_before_limits() {
    let (_dir, store) = setup();
    let own = agent(&store, "own");
    let hidden = agent(&store, "hidden");
    let receive = |id: &AgentId, text: String, timestamp| {
        store
            .receive(
                id,
                Incoming {
                    text,
                    sender: Some("agent:peer".into()),
                    source_time_ms: None,
                    source_key: None,
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: Some(timestamp),
                },
            )
            .unwrap();
    };
    receive(
        &own.id,
        "scopeword fragmentpartial authorized".into(),
        1_000,
    );
    // More hidden rows than either the FTS, substring, structured or recent
    // candidate cap: filtering only the final result would lose the hit.
    for n in 0..2_010 {
        receive(
            &hidden.id,
            format!("scopeword fragmentpartial hidden {n}"),
            2_000 + n,
        );
    }
    let mut visible = vec![own.id.to_string()];
    for n in 0..17 {
        visible.push(agent(&store, &format!("also-visible-{n}")).id.to_string());
    }
    for query in [Some("scopeword"), Some("gmentpart"), None] {
        let hits = store
            .search_history(&HistorySearchQuery {
                agent: Some("all".into()),
                query: query.map(str::to_string),
                visible_agent_ids: Some(visible.clone()),
                limit: 1,
                ..HistorySearchQuery::default()
            })
            .unwrap();
        assert_eq!(hits.len(), 1, "{query:?}");
        assert!(hits[0].text.ends_with("authorized"), "{hits:?}");
    }
    let recent = store
        .conversation_all_scoped(None, None, Some(1), Some(&visible))
        .unwrap();
    assert_eq!(recent.len(), 1);
    assert!(recent[0].text.ends_with("authorized"));
    assert!(
        store
            .conversation_all_scoped(None, None, Some(1), Some(&[]))
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .search_history(&HistorySearchQuery {
                query: Some("scopeword".into()),
                visible_agent_ids: Some(vec![]),
                ..HistorySearchQuery::default()
            })
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .conversation_scoped(&hidden.id, None, None, None, Some(&visible))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn history_radius_preserves_visibility_and_all_structured_filters() {
    let (_dir, store) = setup();
    let own = agent(&store, "own");
    let hidden = agent(&store, "hidden");
    for (id, sender, text, timestamp) in [
        (&own.id, "agent:peer", "outside time", 1_000),
        (&own.id, "agent:peer", "outside seq", 2_000),
        (&own.id, "agent:peer", "radiusneedle authorized", 3_000),
        (&hidden.id, "agent:peer", "hidden neighbor", 3_001),
        (&own.id, "agent:other", "outside peer", 4_000),
        (&own.id, "agent:peer", "excluded neighbor", 5_000),
    ] {
        store
            .receive(
                id,
                Incoming {
                    text: text.into(),
                    sender: Some(sender.into()),
                    source_time_ms: None,
                    source_key: None,
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: Some(timestamp),
                },
            )
            .unwrap();
    }
    store
        .record_outbound(&own.id, "agent:peer", None, "outside direction")
        .unwrap();
    store
        .record_tool_event(&own.id, "browser", "radiusneedle own browser")
        .unwrap();
    store
        .record_tool_event(&hidden.id, "browser", "radiusneedle hidden browser")
        .unwrap();
    // Historical names are not a permission boundary (e.g. after rename).
    store
        .memory_index()
        .sqlite_writer()
        .unwrap()
        .execute(
            "UPDATE rag_chunks SET agent_name = 'own' WHERE agent_id = ?1",
            [hidden.id.as_str()],
        )
        .unwrap();
    let visible = Some(vec![own.id.to_string()]);
    let hits = store
        .search_history(&HistorySearchQuery {
            query: Some("radiusneedle".into()),
            agent: Some("all".into()),
            visible_agent_ids: visible.clone(),
            with_peer: Some("peer".into()),
            sender: Some("peer".into()),
            direction: Some("in".into()),
            include_tools: false,
            since_ms: Some(1_500),
            until_ms: Some(5_000),
            from_seq: Some(3),
            to_seq: Some(5),
            exclude: vec!["excluded".into()],
            radius: 10,
            ..HistorySearchQuery::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].text, "radiusneedle authorized");
    let tools = store
        .search_history(&HistorySearchQuery {
            only_tools: true,
            tool_name: Some("browser".into()),
            radius: 10,
            visible_agent_ids: visible,
            ..HistorySearchQuery::default()
        })
        .unwrap();
    assert_eq!(tools.len(), 1, "{tools:?}");
    assert!(tools[0].text.ends_with("own browser"));
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
fn a_rebuilt_index_holds_authoritative_bodies_and_keeps_tool_summaries() {
    let (dir, store) = setup();
    let own = agent(&store, "batch-owner");
    // Cross several projection batches, retaining a standalone tool.
    for n in 0..2600 {
        store
            .record_outbound(&own.id, "agent:peer", None, &format!("full body {n}"))
            .unwrap();
    }
    store
        .record_tool_event(&own.id, "browser", "legacy tool summary")
        .unwrap();
    {
        let index = store.memory_index().sqlite_writer().unwrap();
        index
            .execute(
                "UPDATE rag_chunks SET text='clipped' WHERE kind='message'",
                [],
            )
            .unwrap();
        index
            .execute("UPDATE index_meta SET value = 0 WHERE key = 'layout'", [])
            .unwrap();
    }
    // A body too short to search leaves no row.
    store
        .conn()
        .execute(
            "UPDATE outbound_messages SET body='x' WHERE id=(SELECT min(id) FROM outbound_messages)",
            [],
        )
        .unwrap();
    drop(store);
    let store = Store::open(dir.path().join("state.db")).unwrap();
    // More than one read's budget: the rest is filled in the background,
    // and every search drains up to its budget.
    for _ in 0..100 {
        store.sync_memory_index().unwrap();
    }
    let index = store.memory_index().sqlite_writer().unwrap();
    let (messages, tools, stale): (i64, i64, i64) = index
        .query_row(
            "SELECT count(*) FILTER (WHERE kind='message'), count(*) FILTER (WHERE kind='tool'),
                    count(*) FILTER (WHERE text='clipped' OR text='x') FROM rag_chunks",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((messages, tools, stale), (2599, 1, 0));
    let fts: i64 = index
        .query_row("SELECT count(*) FROM rag_chunks_fts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(fts, 2600);
}

#[test]
fn state_holds_no_search_index_and_mail_never_waits_for_it() {
    let (dir, store) = setup();
    let own = agent(&store, "oltp-owner");
    for n in 0..200 {
        store
            .receive(&own.id, Incoming::new(format!("oltp warmup body {n}")))
            .unwrap();
    }
    // State has no search table, index, trigger or FTS shadow table.
    let state_search: Vec<String> = store
        .conn()
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE name LIKE '%rag%' OR name LIKE '%fts%' OR name LIKE '%embedding%'
                OR sql LIKE '%fts5%'",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(state_search.is_empty(), "{state_search:?}");
    let indexed: i64 = store
        .read_conn()
        .db_query_one("SELECT count(*) FROM rag_chunks", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(indexed, 200);

    // Another process holds the index's write lock, as a long rebuild
    // would. Mail, sends and the state writer go on regardless.
    let blocker = Connection::open(store.memory_index_path()).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    blocker
        .execute(
            "DELETE FROM rag_chunks WHERE rowid IN (SELECT rowid FROM rag_chunks LIMIT 1)",
            [],
        )
        .unwrap();
    let mut writes = Vec::new();
    for n in 0..50 {
        let started = std::time::Instant::now();
        store
            .receive(
                &own.id,
                Incoming::new(format!("oltpneedle{n} during rebuild")),
            )
            .unwrap();
        store
            .record_outbound(&own.id, "agent:peer", None, &format!("oltpsend{n}"))
            .unwrap();
        writes.push(started.elapsed());
    }
    // The state writer itself: one bare write transaction.
    let mut held = Vec::new();
    for _ in 0..50 {
        let started = std::time::Instant::now();
        store
            .write_tx(|tx| {
                tx.db_execute(
                    "UPDATE agents SET description = 'x' WHERE id = ?1",
                    &db_params![own.id.as_str()],
                )?;
                Ok(())
            })
            .unwrap();
        held.push(started.elapsed());
    }
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    writes.sort();
    held.sort();
    eprintln!(
        "receive+send p50 {:?} max {:?}; state write p50 {:?} max {:?}",
        writes[25], writes[49], held[25], held[49]
    );
    // Waiting on the index would take its 30 s busy timeout.
    assert!(writes[49] < Duration::from_secs(2), "{:?}", writes[49]);
    assert!(held[25] < Duration::from_millis(5), "{:?}", held[25]);
    // Everything committed meanwhile is searchable once the index is free.
    let hits = store
        .search_history(&HistorySearchQuery {
            query: Some("oltpneedle49".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 1);
    let indexed: i64 = store
        .read_conn()
        .db_query_one("SELECT count(*) FROM rag_chunks", &[], |r| r.get(0))
        .unwrap();
    assert_eq!(indexed, 300);
    let state_bytes = std::fs::metadata(dir.path().join("state.db"))
        .unwrap()
        .len();
    let index_bytes = std::fs::metadata(store.memory_index_path()).unwrap().len();
    eprintln!("state.db {state_bytes} bytes, memory-index.db {index_bytes} bytes");
}

#[test]
fn full_mailbox_tails_upgrade_only_authorized_chunks_and_survive_redaction_restart() {
    let (dir, store) = setup();
    let own = agent(&store, "tail-owner");
    let hidden = agent(&store, "tail-hidden");
    let prefix = "padding ".repeat(500);
    let incoming = |text: String| Incoming {
        text,
        sender: Some("agent:peer".into()),
        source_time_ms: None,
        source_key: None,
        reply_to: None,
        quote: None,
        origin: None,
        received_at_ms: None,
    };
    store
        .receive(&own.id, incoming(format!("{prefix} firsttailneedle")))
        .unwrap();
    store
        .receive(&own.id, incoming(format!("{prefix} secondtailneedle")))
        .unwrap();
    store
        .receive(&hidden.id, incoming(format!("{prefix} hiddentailneedle")))
        .unwrap();
    store
        .record_outbound(
            &own.id,
            "agent:peer",
            None,
            &format!("{prefix} outboundtailneedle"),
        )
        .unwrap();
    store
        .record_tool_event(&own.id, "browser", &format!("{prefix} tooltailneedle"))
        .unwrap();
    let ids = [own.id.to_string()];
    let query = |term: &str| HistorySearchQuery {
        query: Some(term.into()),
        visible_agent_ids: Some(ids.to_vec()),
        ..Default::default()
    };
    for term in [
        "firsttailneedle",
        "secondtailneedle",
        "outboundtailneedle",
        "tooltailneedle",
    ] {
        assert_eq!(
            store.search_history(&query(term)).unwrap().len(),
            1,
            "{term}"
        );
    }
    let hashes: i64 = store.memory_index().sqlite_writer().unwrap().query_row("SELECT count(DISTINCT content_hash) FROM rag_chunks WHERE agent_id=?1 AND direction='in'", [own.id.as_str()], |r| r.get(0)).unwrap();
    assert_eq!(
        hashes, 2,
        "identical prefixes must not merge distinct tails"
    );
    let message = store
        .inbox(&own.id, None)
        .unwrap()
        .into_iter()
        .find(|m| m.text.ends_with("firsttailneedle"))
        .unwrap();
    store.set_agent_state(&own.id, "archived").unwrap();
    store
        .redact_conversation(
            crate::redaction::Actor::Operator,
            &own.id,
            &crate::redaction::Request {
                message_id: Some(message.id.to_string()),
                seq: None,
                span: Some(crate::redaction::Span {
                    unit: crate::redaction::SpanUnit::Bytes,
                    start: prefix.len() + 1,
                    end: message.text.len(),
                }),
                reason: "remove tail".into(),
            },
        )
        .unwrap();
    // A rebuild reads the redacted authoritative body, never an earlier
    // derivative snapshot.
    store
        .memory_index()
        .sqlite_writer()
        .unwrap()
        .execute("UPDATE index_meta SET value = 0 WHERE key = 'layout'", [])
        .unwrap();
    drop(store);
    let store = Store::open(dir.path().join("state.db")).unwrap();
    assert!(
        store
            .search_history(&query("firsttailneedle"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .search_history(&query("secondtailneedle"))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .search_history(&HistorySearchQuery {
                query: Some("hiddentailneedle".into()),
                visible_agent_ids: Some(vec![hidden.id.to_string()]),
                ..Default::default()
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn search_history_supports_lexical_cjk_multi_query_exclude_tools_range_ordering_and_radius() {
    let (_dir, store) = setup();
    let alice = agent(&store, "alice");
    store
        .receive(
            &alice.id,
            Incoming {
                text: "Step 1: durable turn_recovery checkpoint design and 記憶檢索".to_string(),
                sender: Some("agent:bob".to_string()),
                source_time_ms: None,
                source_key: None,
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: Some(1_000),
            },
        )
        .unwrap();
    store
        .record_tool_event(
            &alice.id,
            "browser",
            "opened https://openrouter.ai/rankings and took snapshot",
        )
        .unwrap();
    store
        .receive(
            &alice.id,
            Incoming {
                text: "Step 2: noisy heartbeat ping to ignore".to_string(),
                sender: Some("agent:bob".to_string()),
                source_time_ms: None,
                source_key: None,
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: Some(2_000),
            },
        )
        .unwrap();
    store
        .receive(
            &alice.id,
            Incoming {
                text: "Step 3: 生產環境部署完成 verified green".to_string(),
                sender: Some("agent:carol".to_string()),
                source_time_ms: None,
                source_key: None,
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: Some(3_000),
            },
        )
        .unwrap();

    // 1. Sub-token (`recovery` from `turn_recovery`) + CJK 2-char bigram (`記憶`).
    let hits = store
        .search_conversation(Some("alice"), None, None, Some(10), "recovery")
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(hits[0].text.contains("turn_recovery"));

    let cjk_hits = store
        .search_conversation(Some("alice"), None, None, Some(10), "記憶")
        .unwrap();
    assert_eq!(cjk_hits.len(), 1);
    assert!(cjk_hits[0].text.contains("記憶檢索"));

    // 2. Multi-query (`queries`) + negative filter (`exclude`) + ordering (`Oldest` vs `Newest`).
    let multi = store
        .search_history(&HistorySearchQuery {
            queries: vec![
                "checkpoint".to_string(),
                "生產環境".to_string(),
                "Step".to_string(),
            ],
            exclude: vec!["heartbeat".to_string()],
            agent: Some("alice".to_string()),
            include_tools: false,
            sort: HistorySort::Oldest,
            limit: 10,
            ..HistorySearchQuery::default()
        })
        .unwrap();
    assert_eq!(multi.len(), 2);
    assert!(multi[0].text.contains("Step 1"));
    assert!(multi[1].text.contains("Step 3"));

    // 3. Tool history filtering (`only_tools: true` and `tool_name: Some("browser")`).
    let tools_only = store
        .search_history(&HistorySearchQuery {
            agent: Some("alice".to_string()),
            only_tools: true,
            tool_name: Some("browser".to_string()),
            limit: 10,
            ..HistorySearchQuery::default()
        })
        .unwrap();
    assert_eq!(tools_only.len(), 1);
    assert!(tools_only[0].text.contains("[tool:browser]"));

    // 4. Sequence range (`from_seq` / `to_seq`) and `radius` expansion.
    let ranged = store
        .search_history(&HistorySearchQuery {
            agent: Some("alice".to_string()),
            include_tools: false,
            from_seq: Some(3),
            to_seq: Some(3),
            limit: 10,
            ..HistorySearchQuery::default()
        })
        .unwrap();
    assert_eq!(ranged.len(), 1);
    assert!(ranged[0].text.contains("Step 3"));

    let with_radius = store
        .search_history(&HistorySearchQuery {
            query: Some("heartbeat".to_string()),
            agent: Some("alice".to_string()),
            include_tools: false,
            radius: 1,
            limit: 1,
            ..HistorySearchQuery::default()
        })
        .unwrap();
    assert_eq!(with_radius.len(), 3);
    assert!(with_radius[0].text.contains("Step 1"));
    assert!(with_radius[1].text.contains("Step 2"));
    assert!(with_radius[2].text.contains("Step 3"));

    // 5. FTS5 filter pushdown: even when another agent has 250 matching rows,
    // filtering by `agent = "alice"` with `limit = 1` finds Alice's row via inverted-index intersection.
    let bob = agent(&store, "bob");
    for i in 0..250 {
        store
            .receive(&bob.id, Incoming::new(format!("heartbeat noise {i}")))
            .unwrap();
    }
    let pushed_down = store
        .search_history(&HistorySearchQuery {
            query: Some("heartbeat".to_string()),
            agent: Some("alice".to_string()),
            limit: 1,
            ..HistorySearchQuery::default()
        })
        .unwrap();
    assert_eq!(pushed_down.len(), 1);
    assert!(pushed_down[0].to.contains("alice") || pushed_down[0].from.contains("alice"));
    assert!(pushed_down[0].text.contains("Step 2"));
}

#[test]
fn idle_release_preserves_pending_work_and_role_independent_identity() {
    let (_dir, store) = setup();
    let parent = agent(&store, "parent");
    for (name, parent_id, role) in [
        ("root", None, Some("Researcher")),
        ("labelled", Some(parent.id.clone()), Some("Researcher")),
        ("plain", Some(parent.id.clone()), None),
    ] {
        let mut new = NewAgent::new(name, "codex");
        new.parent_id = parent_id;
        new.role = role.map(str::to_string);
        new.instructions = Some("Keep serving future requests.".into());
        let agent = store.create_agent(new).unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        store.receive(&agent.id, Incoming::new("task")).unwrap();
        assert!(!store.complete_idle_agent(&agent.id).unwrap());
        store.note_delivered(&agent.id, 1).unwrap();
        assert!(!store.complete_idle_agent(&agent.id).unwrap());
        store.note_turn_completed(&agent.id).unwrap();
        assert!(store.complete_idle_agent(&agent.id).unwrap());
        let idle = store.agent(&agent.id).unwrap();
        assert_eq!(idle.role, agent.role);
        assert_eq!(idle.instructions, agent.instructions);
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 1);
        store
            .receive(&agent.id, Incoming::new("follow-up"))
            .unwrap();
        assert_eq!(store.agent(&agent.id).unwrap().state, "running");
        assert!(!store.complete_idle_agent(&agent.id).unwrap());
    }
}

#[test]
fn idle_release_review_of_an_ineligible_agent_never_takes_the_writer_lock() {
    let (dir, store) = setup();
    let parent = agent(&store, "parent");
    store.set_agent_running(&parent.id, "/work").unwrap();
    store.receive(&parent.id, Incoming::new("task")).unwrap();
    store.note_delivered(&parent.id, 1).unwrap();
    store.note_turn_completed(&parent.id).unwrap();
    let mut new = NewAgent::new("child", "codex");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    store.set_agent_running(&child.id, "/work").unwrap();
    // A peer holds the writer lock and this store never waits for it: any
    // write attempt fails at once with SQLITE_BUSY.
    store.set_lock_wait(Duration::ZERO, Duration::ZERO).unwrap();
    let peer = Connection::open(dir.path().join("state.db")).unwrap();
    peer.execute_batch("BEGIN IMMEDIATE").unwrap();
    // The parent has a running child: not eligible, answered by a read.
    assert!(!store.complete_idle_agent(&parent.id).unwrap());
    // The child never completed a turn: not eligible either.
    assert!(!store.complete_idle_agent(&child.id).unwrap());
    // An eligible agent still takes the guarded write once the lock frees.
    peer.execute_batch("ROLLBACK").unwrap();
    store.set_agent_state(&child.id, "paused").unwrap();
    assert!(store.complete_idle_agent(&parent.id).unwrap());
    assert_eq!(store.agent(&parent.id).unwrap().state, "idle");
}

#[test]
fn relationship_discovery_includes_children_and_siblings_without_roles() {
    let (dir, store) = setup();
    let parent = agent(&store, "parent");
    let mut new = NewAgent::new("first", "codex");
    new.parent_id = Some(parent.id.clone());
    let first = store.create_agent(new).unwrap();
    let mut new = NewAgent::new("second", "codex");
    new.parent_id = Some(parent.id.clone());
    let second = store.create_agent(new).unwrap();
    store.set_agent_scope(&first.id, "siblings").unwrap();
    store.set_agent_scope(&second.id, "siblings").unwrap();
    let roster = read_children_roster(dir.path(), parent.id.as_str());
    assert_eq!(
        roster.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
        ["first", "second"]
    );
    let ReceiveOutcome::New(parent_mail) =
        store.receive(&parent.id, Incoming::new("check")).unwrap()
    else {
        panic!("new mail expected")
    };
    let topology = parent_mail.context.topology.as_deref().unwrap();
    assert!(topology.contains("agent:first"));
    assert!(topology.contains("agent:second"));
    let ReceiveOutcome::New(child_mail) = store.receive(&first.id, Incoming::new("check")).unwrap()
    else {
        panic!("new mail expected")
    };
    assert!(
        child_mail
            .context
            .topology
            .as_deref()
            .unwrap()
            .contains("agent:second")
    );
    // A private sibling remains hidden even though it has a label.
    store
        .set_agent_role(&second.id, Some("Researcher"))
        .unwrap();
    store.set_agent_scope(&second.id, "private").unwrap();
    let ReceiveOutcome::New(changed) = store
        .receive(&first.id, Incoming::new("check again"))
        .unwrap()
    else {
        panic!("new mail expected")
    };
    assert!(
        changed
            .context
            .topology
            .as_deref()
            .unwrap()
            .contains("-sibling: agent:second")
    );
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
fn subagent_scales_to_idle_when_done_wakes_on_message_and_stays_archived_when_retired() {
    let (_dir, store) = setup();
    let root = agent(&store, "root");
    let mut child_new = NewAgent::new("root-worker", "opencode");
    child_new.parent_id = Some(root.id.clone());
    let child = store.create_agent(child_new).unwrap();
    store.set_agent_running(&child.id, "/work").unwrap();

    // Send initial task message (seq = 1), deliver it, and complete turn 1.
    store.receive(&child.id, Incoming::new("task 1")).unwrap();
    store.note_delivered(&child.id, 1).unwrap();
    store.note_turn_completed(&child.id).unwrap();

    // complete_idle_subagent transitions the sub-agent to `idle` (scaled to zero).
    assert!(store.complete_idle_subagent(&child.id).unwrap());
    assert_eq!(store.agent(&child.id).unwrap().state, "idle");

    // Sending a follow-up message wakes an `idle` sub-agent back to `running`.
    store.receive(&child.id, Incoming::new("task 2")).unwrap();
    assert_eq!(store.agent(&child.id).unwrap().state, "running");

    // Explicit archive transitions the sub-agent to `archived` (retired), which does NOT auto-wake.
    store.set_agent_state(&child.id, "archived").unwrap();
    assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    store.receive(&child.id, Incoming::new("task 3")).unwrap();
    assert_eq!(store.agent(&child.id).unwrap().state, "archived");
}

#[test]
fn only_direct_send_ingress_wakes_archived_and_duplicate_does_not_rewake() {
    let (dir, store) = setup();
    let recipient = agent(&store, "recipient");
    store.set_agent_state(&recipient.id, "archived").unwrap();
    // Lifecycle, channel and schedule ingress uses receive, even when a
    // generated notification names an agent as its sender.
    for (sender, key, text) in [
        (
            "agent:child",
            "subagent_completed:child:1",
            "[Sub-Agent Completed] done",
        ),
        (
            "agent:child",
            "subagent-error:child:1",
            "[Sub-Agent Error] failed",
        ),
        ("system", "idle:1", "idle event"),
        ("telegram:user:123", "telegram:123:1", "channel update"),
        ("schedule:timer", "schedule:timer:1", "timer prompt"),
        ("schedule:watch", "watch:watch:1", "watch prompt"),
    ] {
        let mut incoming = Incoming::new(text);
        incoming.sender = Some(sender.into());
        incoming.source_key = Some(key.into());
        store.receive(&recipient.id, incoming).unwrap();
        assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
    }
    let mut incoming = Incoming::new("intentional follow-up");
    incoming.sender = Some("agent:parent".into());
    incoming.source_key = Some("direct-1".into());
    let sent = store
        .receive_direct_send(&recipient.id, incoming.clone())
        .unwrap();
    assert!(matches!(sent, ReceiveOutcome::New(_)));
    assert_eq!(store.agent(&recipient.id).unwrap().state, "running");
    drop(store);
    let store = Store::open(dir.path().join("state.db")).unwrap();
    assert_eq!(store.agent(&recipient.id).unwrap().state, "running");
    store.set_agent_state(&recipient.id, "archived").unwrap();
    assert!(matches!(
        store.receive_direct_send(&recipient.id, incoming).unwrap(),
        ReceiveOutcome::Duplicate(_)
    ));
    assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
    assert_eq!(store.inbox(&recipient.id, None).unwrap().len(), 7);
}

#[test]
fn rename_agent_preserves_id_grants_history_and_resolves_old_alias_and_role() {
    let (_dir, store) = setup();
    let original_worker = agent(&store, "original_worker");
    store.set_agent_admin(&original_worker.id, true).unwrap();
    store.join_group("owners", &original_worker.id).unwrap();
    store
        .receive(&original_worker.id, Incoming::new("revenue check"))
        .unwrap();

    let renamed = store
        .rename_agent(&original_worker.id, "renamed_worker")
        .unwrap();
    assert_eq!(renamed.id, original_worker.id);
    assert_eq!(renamed.name, "renamed_worker");
    assert!(renamed.is_admin);

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

#[test]
fn telegram_reaction_is_not_dropped_by_origin_dedup_and_wakes_targeted_author_only() {
    let (_dir, store) = setup();
    let author_worker = agent(&store, "author_worker");
    let peer_worker = agent(&store, "peer_worker");

    // 1. AuthorWorker posts message #42 in group -100999, which is recorded in both bots' mailboxes.
    let mut msg42 = Incoming::new("Here is the sample economy simulation report.");
    msg42.sender = Some("agent:author_worker".to_string());
    msg42.origin = Some(Origin {
        chat_id: Some(-100999),
        thread_id: None,
        message_id: Some(42),
    });
    store.receive(&author_worker.id, msg42.clone()).unwrap();
    store.receive(&peer_worker.id, msg42).unwrap();
    store.note_delivered(&author_worker.id, 1).unwrap();
    store.note_delivered(&peer_worker.id, 1).unwrap();
    store.set_agent_state(&author_worker.id, "idle").unwrap();
    store.set_agent_state(&peer_worker.id, "idle").unwrap();

    // 2. Alex reacts 👍 on message #42. Neither bot drops it as a duplicate of #42!
    //    AuthorWorker (the author of #42) receives `event = "reaction"` and wakes to `running`.
    //    PeerWorker (a peer bot in the same multi-bot group) receives `event = "reaction_passive"`
    //    in full awareness without waking from `idle` on the reaction alone.
    let mut react = Incoming::new("[Reaction 👍 on message #42]");
    react.sender = Some("telegram:user:777:alex".to_string());
    react.origin = Some(Origin {
        chat_id: Some(-100999),
        thread_id: None,
        message_id: Some(42),
    });

    let author_worker_out = store.receive(&author_worker.id, react.clone()).unwrap();
    let ReceiveOutcome::New(author_worker_msg) = author_worker_out else {
        panic!("reaction must not be dropped as duplicate of original message #42");
    };
    assert_eq!(author_worker_msg.context.event.as_deref(), Some("reaction"));
    assert_eq!(
        author_worker_msg.context.reply_to_author.as_deref(),
        Some("agent:author_worker")
    );
    assert!(
        author_worker_msg
            .text
            .contains("Here is the sample economy simulation report.")
    );
    assert_eq!(store.agent(&author_worker.id).unwrap().state, "running");
    assert!(store.has_actionable_undelivered(&author_worker.id).unwrap());

    let peer_worker_out = store.receive(&peer_worker.id, react).unwrap();
    let ReceiveOutcome::New(peer_worker_msg) = peer_worker_out else {
        panic!("peer bot must also record reaction for full awareness");
    };
    assert_eq!(
        peer_worker_msg.context.event.as_deref(),
        Some("reaction_passive")
    );
    assert_eq!(store.agent(&peer_worker.id).unwrap().state, "idle");
    assert!(!store.has_actionable_undelivered(&peer_worker.id).unwrap());
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

/// A Telegram reply, reaction or edit makes ingress look the referenced
/// message up in the stored history by text pattern, which can read every
/// stored message, and a group reaction also counts the chat's agents the
/// same way. Those reads ran inside the ingress transaction, so the
/// daemon's single writer, and every other write, waited for them (a reply
/// to apollo on 2026-11-13 held it for 100 s). Here another thread writes
/// while each lookup runs; it must finish while the lookup is still going.
/// Lock order: file locks that can wait (an agent's lifecycle, the
/// outbound barrier and key locks) come before the Store writer. A thread
/// holding the writer that waits for one is refused in debug builds; a
/// non-blocking attempt never waits and stays allowed.
#[cfg(debug_assertions)]
#[test]
fn waiting_for_a_lifecycle_while_holding_the_writer_is_refused() {
    let (_dir, store) = setup();
    let held = agent(&store, "lock_order");
    assert!(!holds_store_writer());
    let writer = store.conn();
    assert!(holds_store_writer());
    drop(store.try_lock_agent_lifecycle(&held.id).unwrap());
    let waited = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.lock_agent_lifecycle(&held.id)
    }));
    assert!(
        waited.is_err(),
        "a lifecycle wait under the writer was allowed"
    );
    drop(writer);
    assert!(!holds_store_writer());
    drop(store.lock_agent_lifecycle(&held.id).unwrap());
}

#[test]
fn ingress_reads_reply_reaction_and_edit_history_without_holding_the_writer() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (dir, store) = setup();
    let reader_agent = agent(&store, "history_reader");
    let other_agent = agent(&store, "history_other");
    let bystander = agent(&store, "history_bystander");
    let chat = Some(-100_555);
    let original = |message_id: i64, text: &str| {
        let mut line = Incoming::new(text);
        line.sender = Some("telegram:user:7:alex".into());
        line.origin = Some(Origin {
            chat_id: chat,
            thread_id: None,
            message_id: Some(message_id),
        });
        line
    };
    store
        .receive(&reader_agent.id, original(7, "the referenced line"))
        .unwrap();
    // Recorded for another agent only, so the edit below finds no version
    // of its own and falls back to the history.
    store
        .receive(&other_agent.id, original(8, "first wording"))
        .unwrap();

    let lookups = std::sync::Arc::new(AtomicUsize::new(0));
    let concurrent = std::sync::Arc::new(AtomicUsize::new(0));
    let db = dir.path().join("state.db");
    let bystander_id = bystander.id.clone();
    let (seen, done) = (
        std::sync::Arc::clone(&lookups),
        std::sync::Arc::clone(&concurrent),
    );
    HISTORY_READ_PROBE.with(|probe| {
        *probe.borrow_mut() = Some(Box::new(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            let (db, id) = (db.clone(), bystander_id.clone());
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let store = Store::open(db).unwrap();
                let _ = tx.send(store.receive(&id, Incoming::new("meanwhile")).is_ok());
            });
            if rx.recv_timeout(Duration::from_secs(10)) == Ok(true) {
                done.fetch_add(1, Ordering::SeqCst);
            }
        }));
    });

    let mut reply = original(9, "answering it");
    reply.quote = Some(Quote {
        reference: Some(crate::quote::QuoteRef {
            chat_id: -100_555,
            message_id: 7,
        }),
        author: "Alex".into(),
        ..Default::default()
    });
    store.receive(&reader_agent.id, reply).unwrap();
    let reaction = original(7, "[Reaction 👍 on message #7]");
    let ReceiveOutcome::New(reacted) = store.receive(&reader_agent.id, reaction).unwrap() else {
        panic!("a reaction is a new line");
    };
    let edit = original(8, "[Edited message #8]: second wording");
    let ReceiveOutcome::New(edited) = store.receive(&reader_agent.id, edit).unwrap() else {
        panic!("an edit is a new line");
    };
    HISTORY_READ_PROBE.with(|probe| probe.borrow_mut().take());

    // Reply, reaction target, reaction chat's agents, edit original.
    assert_eq!(lookups.load(Ordering::SeqCst), 4);
    assert_eq!(
        concurrent.load(Ordering::SeqCst),
        4,
        "a write waited for an ingress history lookup"
    );
    // The lookups still shape what is delivered: the group has two
    // agents and the reaction targets neither, so it is passive.
    assert!(reacted.text.contains("the referenced line"));
    assert_eq!(reacted.context.event.as_deref(), Some("reaction_passive"));
    assert!(edited.text.starts_with("[Edited message #8]"));
}

#[test]
fn edited_message_delivers_compact_line_diff_preserving_edit_awareness_and_skips_noop() {
    let (_dir, store) = setup();
    let root_worker = agent(&store, "root_worker");
    let origin = Origin {
        chat_id: Some(-100777),
        thread_id: None,
        message_id: Some(88),
    };

    let v0 = "\
## Daily Progress Report
- Line A: Unchanged 1
- Line B: Unchanged 2
- Line C: Unchanged 3
- Status: CI running
- Deploy: pending
- Line D: Unchanged 4
- Line E: Unchanged 5";
    let mut initial = Incoming::new(v0);
    initial.sender = Some("agent:helper".to_string());
    initial.origin = Some(origin);
    initial.source_key = Some("bot-a:1".into());
    let ReceiveOutcome::New(msg0) = store.receive(&root_worker.id, initial).unwrap() else {
        panic!("initial message must be stored");
    };
    assert_eq!(msg0.text, v0);

    // Edit 1: changes `Status: CI running` -> `Status: Merged (#67)`
    let v1 = v0.replace("Status: CI running", "Status: Merged (#67)");
    let mut edit1 = Incoming::new(format!("[Edited message #88]: {v1}"));
    edit1.sender = Some("agent:helper".to_string());
    edit1.origin = Some(origin);
    edit1.source_key = Some("bot-a:2".into());
    let ReceiveOutcome::New(msg1) = store.receive(&root_worker.id, edit1).unwrap() else {
        panic!("first edit must deliver a new edited event");
    };
    assert_eq!(msg1.context.event.as_deref(), Some("edited"));
    assert!(msg1.text.starts_with("[Edited message #88]:\n"));
    assert!(msg1.text.contains("- - Status: CI running"));
    assert!(msg1.text.contains("+ - Status: Merged (#67)"));
    assert!(!msg1.text.contains("Line A: Unchanged 1"));

    // Edit 2: changes `Deploy: pending` -> `Deploy: live` (must diff against v1, not v0!)
    let v2 = v1.replace("Deploy: pending", "Deploy: live");
    let mut edit2 = Incoming::new(format!("[Edited message #88]: {v2}"));
    edit2.sender = Some("agent:helper".to_string());
    edit2.origin = Some(origin);
    edit2.source_key = Some("bot-a:3".into());
    let ReceiveOutcome::New(msg2) = store.receive(&root_worker.id, edit2).unwrap() else {
        panic!("second edit must deliver a new edited event");
    };
    assert_eq!(msg2.context.event.as_deref(), Some("edited"));
    assert!(msg2.text.contains("- - Deploy: pending"));
    assert!(msg2.text.contains("+ - Deploy: live"));
    assert!(!msg2.text.contains("- - Status: CI running"));

    // Edit 3: no-op edit with identical v2 text must be skipped as Duplicate
    let mut edit3 = Incoming::new(format!("[Edited message #88]: {v2}"));
    edit3.sender = Some("agent:helper".to_string());
    edit3.origin = Some(origin);
    edit3.source_key = Some("bot-a:4".into());
    let out3 = store.receive(&root_worker.id, edit3).unwrap();
    assert!(matches!(out3, ReceiveOutcome::Duplicate(_)));
}

#[test]
fn group_quote_enriches_stored_attachments_and_delivers_late_quote_upgrade() {
    let (_dir, store) = setup();
    let reader_worker = agent(&store, "reader_worker");
    let renamed_worker = agent(&store, "renamed_worker");
    let chat_id = -5290931492;

    // 1. Original post by renamed_worker (`message_id = 1750`) with local attachment path in `messages`
    let mut orig = Incoming::new(
        "Attachment render preview\n[Attachment (photo): /tmp/example/renders/preview.png]",
    );
    orig.sender = Some("agent:renamed_worker".to_string());
    orig.source_key = Some(format!("tg-out:{chat_id}:renamed_worker:1750"));
    orig.origin = Some(Origin {
        chat_id: Some(chat_id),
        thread_id: None,
        message_id: Some(1750),
    });
    store.receive(&reader_worker.id, orig).unwrap();

    // 2. Peer bot without quote relays human reply first (`message_id = 3255`, `quote = None`)
    let mut unquoted_relay = Incoming::new("仲有呢啲");
    unquoted_relay.sender = Some("telegram:user:107671190:3255 (Alex @alex_example)".to_string());
    unquoted_relay.source_time_ms = Some(1_793_798_602_000);
    unquoted_relay.source_key = Some(format!("tg-in:{chat_id}:vulcan:3255"));
    unquoted_relay.origin = Some(Origin {
        chat_id: Some(chat_id),
        thread_id: None,
        message_id: Some(3255),
    });
    let ReceiveOutcome::New(first_msg) = store.receive(&reader_worker.id, unquoted_relay).unwrap()
    else {
        panic!("first relay must be stored");
    };
    assert!(!has_quoted_block(&first_msg.text));

    // Simulate harness having already delivered `orig` (seq 1) and `first_msg` (seq 2) before the quoted webhook arrives
    store.note_delivered(&reader_worker.id, 1).unwrap();
    store
        .note_delivered(&reader_worker.id, first_msg.seq)
        .unwrap();

    // 3. Replied-to bot (`renamed_worker`, `message_id = 2506`) relays the same message WITH `quote`
    let mut quoted_relay = Incoming::new("仲有呢啲");
    quoted_relay.sender = Some("telegram:user:107671190:2506 (Alex @alex_example)".to_string());
    quoted_relay.source_time_ms = Some(1_793_798_602_000);
    quoted_relay.source_key = Some(format!("tg-in:{chat_id}:{}:2506", renamed_worker.id));
    quoted_relay.quote = Some(crate::Quote {
        author: "RenamedWorker (@renamed_worker_bot)".to_string(),
        author_is_bot: true,
        sender_id: Some(9002),
        text: "Attachment render preview".to_string(),
        reference: Some(crate::QuoteRef {
            chat_id,
            message_id: 1750,
        }),
        mentions: Vec::new(),
    });
    quoted_relay.origin = Some(Origin {
        chat_id: Some(chat_id),
        thread_id: None,
        message_id: Some(2506),
    });
    let ReceiveOutcome::New(upgraded_msg) = store.receive(&reader_worker.id, quoted_relay).unwrap()
    else {
        panic!("late quote arrival after unquoted delivery must deliver the quoted message");
    };
    assert!(has_quoted_block(&upgraded_msg.text));
    assert!(
        upgraded_msg
            .text
            .contains("[Attachment (photo): /tmp/example/renders/preview.png]"),
        "quote must be enriched with stored attachment path: {}",
        upgraded_msg.text
    );

    // 4. Subsequent webhook from reader_worker's own bot (`message_id = 14257`) is deduplicated
    let mut third_webhook = Incoming::new("仲有呢啲");
    third_webhook.sender = Some("telegram:user:107671190:14257 (Alex @alex_example)".to_string());
    third_webhook.source_time_ms = Some(1_793_798_602_000);
    third_webhook.source_key = Some("bot_reader_worker:99999".to_string());
    third_webhook.origin = Some(Origin {
        chat_id: Some(chat_id),
        thread_id: None,
        message_id: Some(14257),
    });
    let third_out = store.receive(&reader_worker.id, third_webhook).unwrap();
    assert!(matches!(third_out, ReceiveOutcome::Duplicate(_)));
}

#[test]
fn event_driven_topology_initial_delta_and_isolated_scope() {
    let (_dir, store) = setup();
    let root_worker = store
        .create_agent(NewAgent {
            name: "root_worker".to_string(),
            role: Some("公司業務總管".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: None,
        })
        .unwrap();
    let helper = store
        .create_agent(NewAgent {
            name: "helper".to_string(),
            role: Some("產品與功能".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(root_worker.id.clone()),
        })
        .unwrap();
    let renamed_worker = store
        .create_agent(NewAgent {
            name: "renamed_worker".to_string(),
            role: Some("商業化與營收".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(root_worker.id.clone()),
        })
        .unwrap();
    assert_eq!(root_worker.scope(), "public");
    store.set_agent_scope(&helper.id, "siblings").unwrap();
    store
        .set_agent_scope(&renamed_worker.id, "siblings")
        .unwrap();

    // 1. First message to helper from root_worker -> emits `<topology event="initial" ... />`
    let mut m1 = Incoming::new("Start sprint");
    m1.sender = Some("agent:root_worker".to_string());
    let ReceiveOutcome::New(msg1) = store.receive(&helper.id, m1).unwrap() else {
        panic!("expected new message");
    };
    assert_eq!(msg1.context.sender_rel.as_deref(), Some("parent"));
    assert_eq!(msg1.context.sender_role.as_deref(), Some("公司業務總管"));
    let topo1 = msg1.context.topology.as_deref().expect("initial topology");
    assert!(topo1.contains("event=\"initial\""), "topo1={topo1}");
    assert!(topo1.contains("scope=\"siblings\""), "topo1={topo1}");
    assert!(
        topo1.contains("parent=\"agent:root_worker [公司業務總管]\""),
        "topo1={topo1}"
    );
    assert!(
        topo1.contains("agent:renamed_worker [商業化與營收]"),
        "topo1={topo1}"
    );

    // 2. Second message with no topology change -> `topology` is None
    let mut m2 = Incoming::new("Status check");
    m2.sender = Some("agent:renamed_worker".to_string());
    let ReceiveOutcome::New(msg2) = store.receive(&helper.id, m2).unwrap() else {
        panic!("expected new message");
    };
    assert_eq!(msg2.context.sender_rel.as_deref(), Some("sibling"));
    assert!(
        msg2.context.topology.is_none(),
        "no delta expected when unchanged"
    );

    // 3. New standing sibling `reader_worker` added -> next message to helper emits `<topology event="delta" ... />`
    let reader_worker = store
        .create_agent(NewAgent {
            name: "reader_worker".to_string(),
            role: Some("用戶體驗與設計".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(root_worker.id.clone()),
        })
        .unwrap();
    store
        .set_agent_scope(&reader_worker.id, "siblings")
        .unwrap();
    let mut m3 = Incoming::new("Design sync");
    m3.sender = Some("agent:root_worker".to_string());
    let ReceiveOutcome::New(msg3) = store.receive(&helper.id, m3).unwrap() else {
        panic!("expected new message");
    };
    let topo3 = msg3
        .context
        .topology
        .as_deref()
        .expect("delta topology on add");
    assert!(topo3.contains("event=\"delta\""), "topo3={topo3}");
    assert!(
        topo3.contains("+sibling: agent:reader_worker [用戶體驗與設計]"),
        "topo3={topo3}"
    );

    // 4. Archive `reader_worker` -> next message to helper emits `-sibling: reader_worker`
    store
        .set_agent_state(&reader_worker.id, "archived")
        .unwrap();
    let mut m4 = Incoming::new("After archive");
    m4.sender = Some("agent:root_worker".to_string());
    let ReceiveOutcome::New(msg4) = store.receive(&helper.id, m4).unwrap() else {
        panic!("expected new message");
    };
    let topo4 = msg4
        .context
        .topology
        .as_deref()
        .expect("delta topology on archive");
    assert!(
        topo4.contains("-sibling: agent:reader_worker [用戶體驗與設計]"),
        "topo4={topo4}"
    );

    // 5. Explicitly isolated agents see no siblings regardless of name or role
    let stanley = store
        .create_agent(NewAgent {
            name: "stanley-assistant".to_string(),
            role: Some("Stanley 專屬助手".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(root_worker.id.clone()),
        })
        .unwrap();
    store.set_agent_scope(&stanley.id, "isolated").unwrap();
    let mut m5 = Incoming::new("Hello Stanley");
    m5.sender = Some("agent:root_worker".to_string());
    let ReceiveOutcome::New(msg5) = store.receive(&stanley.id, m5).unwrap() else {
        panic!("expected new message");
    };
    let topo5 = msg5
        .context
        .topology
        .as_deref()
        .expect("initial topology for isolated");
    assert!(topo5.contains("scope=\"isolated\""), "topo5={topo5}");
    assert!(!topo5.contains("siblings="), "topo5={topo5}");
}

fn sleep_test_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

struct ScopedSleepModelEnv {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl ScopedSleepModelEnv {
    fn new(url: Option<&str>) -> Self {
        let guard = sleep_test_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            match url {
                Some(u) => std::env::set_var("ANYAGENTS_SLEEP_MODEL_URL", u),
                None => std::env::remove_var("ANYAGENTS_SLEEP_MODEL_URL"),
            }
            std::env::remove_var("ANYAGENTS_SLEEP_CONSOLIDATION_FORCE");
        }
        Self { _guard: guard }
    }
}

impl Drop for ScopedSleepModelEnv {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("ANYAGENTS_SLEEP_MODEL_URL");
            std::env::remove_var("ANYAGENTS_SLEEP_CONSOLIDATION_FORCE");
        }
    }
}

fn spawn_mock_sleep_model_server(
    plan_json: String,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    std::thread::JoinHandle<()>,
) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let req_clone = std::sync::Arc::clone(&requests);
    let handle = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                        let s = String::from_utf8_lossy(&buf);
                        if let Some((hdrs, body)) = s.split_once("\r\n\r\n") {
                            let content_len = hdrs
                                .lines()
                                .find_map(|l| {
                                    let (k, v) = l.split_once(':')?;
                                    if k.trim().eq_ignore_ascii_case("content-length") {
                                        v.trim().parse::<usize>().ok()
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(0);
                            if body.len() >= content_len {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            req_clone
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&buf).into_owned());
            let resp_body = serde_json::json!({
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": plan_json
                        }
                    }
                ]
            })
            .to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                resp_body.len(),
                resp_body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });
    (
        format!("http://{addr}/v1/chat/completions"),
        requests,
        handle,
    )
}

#[test]
fn sleep_memory_maintenance_rebuilds_index_for_disk_edits_without_hardcoded_topics() {
    let _env = ScopedSleepModelEnv::new(None);
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-neutral", "spiron"))
        .unwrap();
    let mut msg = Incoming::new("Directive message body that exceeds thirty characters easily");
    msg.sender = Some("operator".into());
    store.receive(&agent.id, msg).unwrap();

    let root = dir.path().join("agents").join(agent.id.as_str());
    std::fs::create_dir_all(root.join("memory")).unwrap();
    std::fs::write(
        root.join("memory/custom-topic.md"),
        "# Custom Topic\n\nAgent-authored invariant fact.\n",
    )
    .unwrap();

    let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    assert!(modified);
    let mem_md = std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap();
    assert!(mem_md.contains("custom-topic"));
    let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].slug, "custom-topic");
    let sleep = crate::learned_memory::read_sleep_state(&root);
    assert_eq!(sleep.last_inbound_seq, 1);
    assert_eq!(sleep.consolidated_topics, 1);
}

#[test]
fn sleep_model_selector_matrix_reaches_chat_completions_without_route_or_fragment() {
    use crate::learned_memory::{SleepModelEndpoint, request_sleep_consolidation_plan};
    let root = crate::test_tempdir();
    let mut cases = Vec::new();
    for base in [
        "gpt-6.2-sol",
        "gpt-6.2-astra",
        "gpt-6.1-sol",
        "unknown-model",
    ] {
        for prefix in ["openai/", "sylphx/openai/"] {
            for effort in ["low", "medium", "high", "xhigh", "max"] {
                cases.push((
                    format!("{prefix}{base}#{effort}"),
                    format!("openai/{base}"),
                    Some(effort),
                ));
            }
        }
    }
    for opaque in [
        "vendor/custom#future",
        "sylphx/vendor/custom#high",
        "openai/custom#future",
        "openai/gpt-6.1-sol",
    ] {
        cases.push((opaque.into(), opaque.into(), None));
    }
    for (selector, model, effort) in cases {
        let (url, requests, handle) = spawn_mock_sleep_model_server(r#"{"operations":[]}"#.into());
        let endpoint = SleepModelEndpoint {
            url,
            bearer_key: None,
            model: selector.clone(),
        };
        request_sleep_consolidation_plan(&endpoint, root.path(), "worker", None, &[], &[]).unwrap();
        handle.join().unwrap();
        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
        let (_, payload) = captured[0].split_once("\r\n\r\n").unwrap();
        let body: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(body["model"].as_str(), Some(model.as_str()), "{selector}");
        assert_eq!(
            body.get("reasoning_effort").and_then(|v| v.as_str()),
            effort
        );
        assert_eq!(body["messages"].as_array().unwrap().len(), 2);
        assert_eq!(
            endpoint.model, selector,
            "wire conversion must not rewrite the stored selector"
        );
    }
    let (url, _, handle) = spawn_mock_sleep_model_server("not a consolidation plan".into());
    let endpoint = SleepModelEndpoint {
        url,
        bearer_key: None,
        model: "sylphx/openai/gpt-6.2-sol#max".into(),
    };
    assert!(
        request_sleep_consolidation_plan(&endpoint, root.path(), "worker", None, &[], &[]).is_err()
    );
    handle.join().unwrap();
}

#[test]
fn sleep_consolidation_strips_quotes_and_rejects_noise() {
    let (url, requests, handle) = spawn_mock_sleep_model_server(r#"{"operations":[]}"#.to_string());
    let _env = ScopedSleepModelEnv::new(Some(&url));
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-noise", "spiron"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    std::fs::create_dir_all(root.join("memory")).unwrap();

    let mut quoted_msg = Incoming::new("OK, checking now");
    quoted_msg.sender = Some("user:alex".into());
    quoted_msg.quote = Some(crate::quote::Quote {
        author: "Alex".into(),
        text: "quoted_echo_secret_should_be_stripped".into(),
        ..Default::default()
    });
    store.receive(&agent.id, quoted_msg).unwrap();

    let mut no_reply = Incoming::new("NO_REPLY");
    no_reply.sender = Some("agent:peer".into());
    store.receive(&agent.id, no_reply).unwrap();

    let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    handle.join().unwrap();
    assert!(!modified, "noise rejection should not create topic files");

    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].contains("OK, checking now"));
    assert!(
        !captured[0].contains("quoted_echo_secret_should_be_stripped"),
        "quoted block must be stripped before LLM consolidation"
    );
    assert!(
        !captured[0].contains("NO_REPLY"),
        "NO_REPLY turns must be filtered out"
    );

    let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
    assert!(topics.is_empty());
    let sleep = crate::learned_memory::read_sleep_state(&root);
    assert_eq!(sleep.last_inbound_seq, 2);
    assert_eq!(sleep.consolidated_topics, 0);
}

#[test]
fn sleep_consolidation_upserts_existing_topic_in_place_and_resolves_conflicts() {
    let plan = serde_json::json!({
        "operations": [
            {
                "op": "upsert",
                "topic": "service-endpoint",
                "description": "Primary API endpoint migrated to port 9090 (replaced 8080)",
                "content": "# Service Endpoint\n\nPrimary API endpoint runs on port 9090 in production."
            }
        ]
    })
    .to_string();
    let (url, requests, handle) = spawn_mock_sleep_model_server(plan);
    let _env = ScopedSleepModelEnv::new(Some(&url));
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-upsert", "spiron"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    crate::learned_memory::upsert_memory_topic(
        &root,
        &agent.name,
        None,
        "service-endpoint",
        Some("Primary API endpoint on port 8080"),
        "# Service Endpoint\n\nPrimary API endpoint runs on port 8080.",
        1700000000000,
    )
    .unwrap();

    let body = "We migrated the service-endpoint port from 8080 to 9090.";
    let mut msg = Incoming::new(body);
    msg.sender = Some("operator".into());
    let ReceiveOutcome::New(received) = store.receive(&agent.id, msg).unwrap() else {
        panic!("a new message");
    };

    let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    handle.join().unwrap();
    assert!(modified);

    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert!(
        captured[0].contains("Primary API endpoint runs on port 8080."),
        "existing topic body must be included in consolidation prompt"
    );

    let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
    assert_eq!(
        topics.len(),
        1,
        "must update topic in place without duplicating"
    );
    assert_eq!(topics[0].slug, "service-endpoint");
    assert_eq!(
        topics[0].description,
        "Primary API endpoint migrated to port 9090 (replaced 8080)"
    );
    assert!(topics[0].updated_at_ms > 1700000000000);

    let on_disk = std::fs::read_to_string(root.join("memory/service-endpoint.md")).unwrap();
    assert!(on_disk.contains("created_at_ms: 1700000000000"));
    assert!(on_disk.contains("port 9090 in production"));
    assert!(!on_disk.contains("runs on port 8080"));
    // The turns were numbered for the model, and the written topic cites
    // the immutable raw turn it was distilled from.
    assert!(captured[0].contains("- #0 [ts="), "{}", captured[0]);
    let (_, cited) = crate::learned_memory::split_topic_sources(&on_disk);
    assert_eq!(
        cited,
        vec![crate::learned_memory::RawCitation {
            raw_id: received.id.to_string(),
            raw_sha256: crate::learned_memory::sha256(body.as_bytes()),
            offset: 0,
            length: body.len() as u64,
            seq: received.seq,
            direction: "in".into(),
        }]
    );
    // Nothing was destroyed: the replaced version is archived history,
    // and the raw turn is byte-for-byte what it was.
    let archived: Vec<_> = std::fs::read_dir(root.join("history/archive/memory"))
        .unwrap()
        .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap())
        .collect();
    assert_eq!(archived.len(), 1);
    assert!(archived[0].contains("Primary API endpoint runs on port 8080."));
    assert_eq!(
        store
            .message(&agent.id, received.id.as_str())
            .unwrap()
            .unwrap()
            .text,
        body
    );
}

#[test]
fn sleep_consolidation_defragments_duplicate_topics_and_syncs_fts5() {
    let plan = serde_json::json!({
        "operations": [
            {
                "op": "upsert",
                "topic": "cache-ttl",
                "description": "Unified cache TTL policy (300s L1, 3600s L2)",
                "content": "# Unified Cache TTL\n\nCanonical merged token unifiedcachetoken."
            },
            {
                "op": "delete",
                "topic": "cache-ttl-v1"
            },
            {
                "op": "delete",
                "topic": "cache-ttl-v2"
            }
        ]
    })
    .to_string();
    let (url, _requests, handle) = spawn_mock_sleep_model_server(plan);
    let _env = ScopedSleepModelEnv::new(Some(&url));
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-defrag", "spiron"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    crate::learned_memory::upsert_memory_topic(
        &root,
        &agent.name,
        None,
        "cache-ttl-v1",
        Some("Legacy L1 cache TTL"),
        "Fragment one oldfragmenttoken",
        1700000000000,
    )
    .unwrap();
    crate::learned_memory::upsert_memory_topic(
        &root,
        &agent.name,
        None,
        "cache-ttl-v2",
        Some("Legacy L2 cache TTL"),
        "Fragment two secondfragmenttoken",
        1700000001000,
    )
    .unwrap();
    store.sync_agent_memory_index(&agent).unwrap();

    let mut msg = Incoming::new("Consolidate cache TTL notes into a single topic.");
    msg.sender = Some("operator".into());
    store.receive(&agent.id, msg).unwrap();

    let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    handle.join().unwrap();
    assert!(modified);

    let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].slug, "cache-ttl");
    let mem_md = std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap();
    assert!(mem_md.contains("cache-ttl"));
    assert!(!mem_md.contains("cache-ttl-v1"));
    assert!(!mem_md.contains("cache-ttl-v2"));

    let ids = [agent.id.to_string()];
    let merged_hits = store
        .search_host_files(
            &ids,
            crate::host_index::MemoryScope::Memory,
            &HistorySearchQuery {
                query: Some("unifiedcachetoken".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(merged_hits.len(), 1);
    let old_hits = store
        .search_host_files(
            &ids,
            crate::host_index::MemoryScope::Memory,
            &HistorySearchQuery {
                query: Some("oldfragmenttoken".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        old_hits.is_empty(),
        "deleted fragmented topic must be evicted from HostIndex FTS5"
    );
    // Merged-away topics are retired to the history archive, not
    // destroyed: still searchable as history, never pinned memory.
    assert!(!root.join("memory/cache-ttl-v1.md").exists());
    let archived = std::fs::read_dir(root.join("history/archive/memory"))
        .unwrap()
        .count();
    assert_eq!(archived, 2);
    let history_hits = store
        .search_host_files(
            &ids,
            crate::host_index::MemoryScope::Transcripts,
            &HistorySearchQuery {
                query: Some("oldfragmenttoken".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(history_hits.len(), 1);
}

#[test]
fn sleep_consolidation_defers_turns_during_cooldown_without_data_loss() {
    let plan = serde_json::json!({
        "operations": [
            {
                "op": "upsert",
                "topic": "deferred-fact",
                "description": "Fact consolidated after cooldown expiry",
                "content": "Durable fact body after cooldown."
            }
        ]
    })
    .to_string();
    let (url, requests, handle) = spawn_mock_sleep_model_server(plan);
    let _env = ScopedSleepModelEnv::new(Some(&url));
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-cooldown", "spiron"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    std::fs::create_dir_all(root.join("memory")).unwrap();

    // Simulate a consolidation that just ran at `now_ms()`.
    let now = crate::now_ms();
    crate::learned_memory::write_sleep_state(
        &root,
        &crate::learned_memory::SleepState {
            last_consolidated_ms: now,
            last_inbound_seq: 0,
            last_outbound_ms: 0,
            consolidated_topics: 0,
        },
    )
    .unwrap();

    let mut msg = Incoming::new("Durable fact arriving during the 10-minute cooldown window.");
    msg.sender = Some("operator".into());
    store.receive(&agent.id, msg).unwrap();

    // First call during cooldown: must defer LLM call and preserve last_inbound_seq = 0.
    let modified_in_cooldown = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    assert!(!modified_in_cooldown);
    assert!(requests.lock().unwrap().is_empty());
    let sleep_during = crate::learned_memory::read_sleep_state(&root);
    assert_eq!(
        sleep_during.last_inbound_seq, 0,
        "must not advance watermark while deferred by cooldown"
    );

    // Expire the 10-minute cooldown and run again: now it consolidates and advances watermark.
    crate::learned_memory::write_sleep_state(
        &root,
        &crate::learned_memory::SleepState {
            last_consolidated_ms: now - 605_000,
            last_inbound_seq: 0,
            last_outbound_ms: 0,
            consolidated_topics: 0,
        },
    )
    .unwrap();
    let modified_after = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    handle.join().unwrap();
    assert!(modified_after);
    assert_eq!(requests.lock().unwrap().len(), 1);
    let sleep_after = crate::learned_memory::read_sleep_state(&root);
    assert_eq!(sleep_after.last_inbound_seq, 1);
    assert_eq!(sleep_after.consolidated_topics, 1);
}

#[test]
fn steered_active_turn_advances_cursor_without_lag() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("steered", "agy")).unwrap();

    let mut first_in = Incoming::new("long running turn");
    first_in.received_at_ms = Some(1_000);
    let ReceiveOutcome::New(first) = store.receive(&agent.id, first_in).unwrap() else {
        panic!("expected new message");
    };
    store.note_delivered(&agent.id, first.seq).unwrap();

    let mut steer_in = Incoming::new("mid-turn steer");
    steer_in.received_at_ms = Some(60_000);
    let ReceiveOutcome::New(steer) = store.receive(&agent.id, steer_in).unwrap() else {
        panic!("expected new steer");
    };
    store.note_delivered(&agent.id, steer.seq).unwrap();

    assert_eq!(store.delivered_cursor(&agent.id).unwrap(), steer.seq);
    assert!(store.has_open_turn(&agent.id).unwrap());
    assert_eq!(
        store
            .lag(&agent.id, 3_600_000)
            .unwrap()
            .oldest_undelivered_age_ms,
        None
    );
    let open = store.open_turn(&agent.id).unwrap().unwrap();
    assert_eq!(open.messages.first().unwrap().received_at_ms, 1_000);
}
#[test]
fn concurrent_turn_completion_never_sees_database_locked() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let ids: Vec<AgentId> = (0..16)
        .map(|i| agent(&store, &format!("concurrent-{i}")).id)
        .collect();
    // Each worker owns a separate Store (separate connection), mirroring
    // one supervisor process per agent finishing turns at once.
    let mut handles = Vec::new();
    for id in &ids {
        let db = db.clone();
        let id = id.clone();
        handles.push(std::thread::spawn(move || {
            let store = Store::open(&db).unwrap();
            for _ in 0..25 {
                store.note_turn_completed(&id).unwrap();
                store
                    .record_usage(NewUsage {
                        agent_id: id.clone(),
                        harness: "claude-code".into(),
                        model: Some("m".into()),
                        input_tokens: 1,
                        output_tokens: 2,
                        cache_read_tokens: 0,
                        cache_write_tokens: 0,
                        reported_cost_micros: Some(3),
                        estimated_cost_micros: None,
                        billing_mode: BillingMode::Subscription,
                        quota_state: None,
                        day: "2026-01-01".into(),
                    })
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(store.list_usage(None).unwrap().len(), 16 * 25);
}

fn usage_for(id: &AgentId) -> NewUsage {
    NewUsage {
        agent_id: id.clone(),
        harness: "claude-code".into(),
        model: Some("m".into()),
        input_tokens: 1,
        output_tokens: 2,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        reported_cost_micros: None,
        estimated_cost_micros: None,
        billing_mode: BillingMode::Subscription,
        quota_state: None,
        day: "2026-01-01".into(),
    }
}

/// Hold the writer lock from another connection for `hold`, then release.
fn hold_writer_lock(db: &Path, hold: Duration) -> std::thread::JoinHandle<()> {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    std::thread::spawn(move || {
        std::thread::sleep(hold);
        conn.execute_batch("COMMIT").unwrap();
    })
}

#[test]
fn turn_persistence_outlasts_a_writer_lock_longer_than_busy_timeout() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let worker = agent(&store, "lock-wait");
    store.set_agent_running(&worker.id, "/work").unwrap();
    store.receive(&worker.id, Incoming::new("task")).unwrap();
    store.note_delivered(&worker.id, 1).unwrap();
    assert!(store.has_open_turn(&worker.id).unwrap());
    // Each attempt gives up after 20 ms; the peer holds the lock ~15x longer.
    store
        .set_lock_wait(Duration::from_millis(20), Duration::from_secs(10))
        .unwrap();

    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_millis(300));
    store.note_turn_completed(&worker.id).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(250));
    assert!(!store.has_open_turn(&worker.id).unwrap());
    peer.join().unwrap();

    let peer = hold_writer_lock(&db, Duration::from_millis(300));
    let record = store.record_usage(usage_for(&worker.id)).unwrap();
    peer.join().unwrap();
    let rows = store.list_usage(Some(&worker.id)).unwrap();
    assert_eq!(rows.len(), 1, "a retried write records exactly one row");
    assert_eq!(rows[0].id, record.id);
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[test]
fn turn_persistence_surfaces_the_lock_error_once_the_budget_is_spent() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let worker = agent(&store, "lock-budget");
    store.set_agent_running(&worker.id, "/work").unwrap();
    store.receive(&worker.id, Incoming::new("task")).unwrap();
    store.note_delivered(&worker.id, 1).unwrap();
    store
        .set_lock_wait(Duration::from_millis(10), Duration::from_millis(150))
        .unwrap();

    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_secs(2));
    let completed = store.note_turn_completed(&worker.id);
    let usage = store.record_usage(usage_for(&worker.id));
    let elapsed = started.elapsed();
    peer.join().unwrap();

    let err = completed.expect_err("lock outlasting the budget must surface");
    assert!(is_transient_lock_error(&err), "unexpected error: {err}");
    assert!(usage.is_err_and(|e| is_transient_lock_error(&e)));
    assert!(
        elapsed < Duration::from_secs(2),
        "budget bounds the wait: {elapsed:?}"
    );
    assert!(store.has_open_turn(&worker.id).unwrap());
    assert!(store.list_usage(Some(&worker.id)).unwrap().is_empty());
}

fn child(store: &Store, name: &str, parent: &Agent) -> Agent {
    let mut new = NewAgent::new(name, "claude-code");
    new.parent_id = Some(parent.id.clone());
    store.create_agent(new).unwrap()
}

fn names(agents: &[Agent]) -> Vec<&str> {
    agents.iter().map(|a| a.name.as_str()).collect()
}

#[test]
fn roster_agents_reads_live_agents_and_their_ancestors_but_not_the_archive() {
    let (_dir, store) = setup();
    let root = agent(&store, "root");
    let old = child(&store, "old", &root);
    let live = child(&store, "live", &old);
    let gone = child(&store, "gone", &root);
    let paused = agent(&store, "paused");
    store.set_agent_state(&old.id, "archived").unwrap();
    store.set_agent_state(&gone.id, "archived").unwrap();
    store.set_agent_state(&paused.id, "paused").unwrap();
    let _ = live;

    // Default: every non-archived agent plus the archived ancestor on a
    // live agent's path; the unrelated archived leaf is never read.
    let (agents, archived) = store.roster_agents(None).unwrap();
    assert_eq!(names(&agents), ["live", "old", "paused", "root"]);
    assert_eq!(archived, 2);

    let (agents, _) = store.roster_agents(Some("paused")).unwrap();
    assert_eq!(names(&agents), ["paused"]);
    let (agents, _) = store.roster_agents(Some("archived")).unwrap();
    assert_eq!(names(&agents), ["gone", "old"]);
    let (agents, archived) = store.roster_agents(Some("all")).unwrap();
    assert_eq!(agents, store.list_agents().unwrap());
    assert_eq!(archived, 2);
    let (agents, _) = store.roster_agents(Some("bogus")).unwrap();
    assert!(agents.is_empty());
}

#[test]
fn roster_agents_seeds_from_indexes_instead_of_scanning_agent_rows() {
    let (_dir, store) = setup();
    let conn = store.conn();
    let plan = |sql: &str| -> String {
        conn.db_query_vec(&format!("EXPLAIN QUERY PLAN {sql}"), &[], |r| {
            r.get::<String>(3)
        })
        .unwrap()
        .join("\n")
    };
    let count = plan("SELECT count(*) FROM agents WHERE state = 'archived'");
    assert!(count.contains("COVERING INDEX idx_agents_state"), "{count}");
    let live =
        plan("SELECT id FROM agents WHERE state != 'archived' AND (NULL IS NULL OR state = NULL)");
    assert!(live.contains("idx_agents_live_name"), "{live}");
}

#[test]
fn hot_path_queries_use_their_indexes() {
    let (_dir, store) = setup();
    let conn = store.read_conn();
    let plan = |sql: &str| -> String {
        conn.db_query_vec(&format!("EXPLAIN QUERY PLAN {sql}"), &[], |r| {
            r.get::<String>(3)
        })
        .unwrap()
        .join("\n")
    };
    let active = plan(concat!(
        "SELECT ",
        schedule_cols!(),
        " FROM agent_schedules WHERE state = 'active' ORDER BY created_at_ms ASC, id ASC"
    ));
    assert!(active.contains("idx_agent_schedules_active"), "{active}");
    assert!(!active.contains("TEMP B-TREE"), "{active}");
    let skipped = plan(
        "SELECT id FROM agent_schedules WHERE kind = 'at' AND state = 'fired'
           AND last_fired_at_ms IS NULL AND last_observed LIKE 'skipped:%'
           AND next_fire_at_ms >= 1",
    );
    assert!(
        skipped.contains("idx_agent_schedules_kind_state"),
        "{skipped}"
    );
    let children = plan(concat!(
        "SELECT ",
        agent_cols!(),
        " FROM agents WHERE parent_id = 'x' AND state != 'archived'
         ORDER BY created_at_ms, name"
    ));
    assert!(children.contains("idx_agents_live_parent"), "{children}");
    assert!(!children.contains("TEMP B-TREE"), "{children}");
}

#[test]
fn narrow_agent_reads_match_the_full_rows() {
    let (_dir, store) = setup();
    let root = agent(&store, "root");
    let live = child(&store, "live", &root);
    let gone = child(&store, "gone", &root);
    let paused = child(&store, "paused", &root);
    store.set_agent_state(&gone.id, "archived").unwrap();
    store.set_agent_state(&paused.id, "paused").unwrap();
    store.set_agent_running(&live.id, "/work").unwrap();

    assert_eq!(
        names(&store.live_children_of(&root.id).unwrap()),
        ["live", "paused"]
    );
    let children = store.children_of(&root.id).unwrap();
    let mut all_children = names(&children);
    all_children.sort_unstable();
    assert_eq!(all_children, ["gone", "live", "paused"]);
    assert!(store.live_children_of(&live.id).unwrap().is_empty());

    assert_eq!(
        store.agent_ids_in_state("running").unwrap(),
        std::slice::from_ref(&live.id)
    );
    assert_eq!(
        store.agent_ids_in_state("archived").unwrap(),
        std::slice::from_ref(&gone.id)
    );

    let full = store.list_agents().unwrap();
    let all = store.agent_summaries(true).unwrap();
    assert_eq!(all.len(), full.len());
    for (summary, agent) in all.iter().zip(&full) {
        assert_eq!(
            summary,
            &AgentSummary {
                id: agent.id.clone(),
                name: agent.name.clone(),
                harness: agent.harness.clone(),
                runtime: agent.runtime.clone(),
                remote_budget_micros: agent.remote_budget_micros,
                model: agent.model.clone(),
                state: agent.state.clone(),
                parent_id: agent.parent_id.clone(),
                role: agent.role.clone(),
                is_admin: agent.is_admin,
                created_at_ms: agent.created_at_ms,
            }
        );
    }
    let live_names: Vec<_> = store
        .agent_summaries(false)
        .unwrap()
        .into_iter()
        .map(|a| a.name)
        .collect();
    assert_eq!(live_names, ["live", "paused", "root"]);
}

#[test]
fn roster_agents_survives_a_parent_cycle() {
    let (_dir, store) = setup();
    let a = agent(&store, "a");
    let b = child(&store, "b", &a);
    store
        .conn()
        .execute(
            "UPDATE agents SET parent_id = ?1 WHERE id = ?2",
            params![b.id.as_str(), a.id.as_str()],
        )
        .unwrap();
    let (agents, _) = store.roster_agents(None).unwrap();
    assert_eq!(names(&agents), ["a", "b"]);
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

#[test]
fn non_lock_errors_are_not_retried() {
    let (_dir, store) = setup();
    let missing = AgentId::generate();
    let started = std::time::Instant::now();
    assert!(matches!(
        store.note_turn_completed(&missing),
        Err(Error::AgentNotFound(_))
    ));
    assert!(started.elapsed() < Duration::from_millis(100));
}

#[test]
fn turn_lifecycle_recovery_outlasts_writer_lock_longer_than_busy_timeout() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let worker = agent(&store, "recovery-lock-wait");
    store.set_agent_running(&worker.id, "/work").unwrap();
    for i in 1..=6 {
        store
            .receive(&worker.id, Incoming::new(format!("task {i}")))
            .unwrap();
    }
    store.note_delivered(&worker.id, 1).unwrap();
    assert!(store.has_open_turn(&worker.id).unwrap());
    store
        .set_lock_wait(Duration::from_millis(20), Duration::from_secs(10))
        .unwrap();

    // note_turn_recovered: the fleet-wide P0 error path.
    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_millis(400));
    store.note_turn_recovered(&worker.id, 1).unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(350),
        "must wait for lock"
    );
    assert_eq!(
        store.open_turn(&worker.id).unwrap().unwrap().recovery_count,
        1
    );
    peer.join().unwrap();

    // advance_delivered: the next cursor step.
    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_millis(400));
    let seq = store.advance_delivered(&worker.id, 2).unwrap();
    assert_eq!(seq, 2);
    assert!(started.elapsed() >= Duration::from_millis(350));
    peer.join().unwrap();

    // note_delivered: records a delivery and re-opens the turn.
    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_millis(400));
    store.note_delivered(&worker.id, 3).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(350));
    assert!(store.has_open_turn(&worker.id).unwrap());
    peer.join().unwrap();

    assert!(
        started.elapsed() < Duration::from_secs(10),
        "budget bounds the wait"
    );
}

#[test]
fn turn_lifecycle_recovery_surfaces_the_lock_error_once_the_budget_is_spent() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let worker = agent(&store, "recovery-lock-budget");
    store.set_agent_running(&worker.id, "/work").unwrap();
    for i in 1..=3 {
        store
            .receive(&worker.id, Incoming::new(format!("task {i}")))
            .unwrap();
    }
    store.note_delivered(&worker.id, 1).unwrap();
    store
        .set_lock_wait(Duration::from_millis(10), Duration::from_millis(150))
        .unwrap();

    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_secs(2));
    let recovered = store.note_turn_recovered(&worker.id, 1);
    let advanced: Result<(), Error> = store.advance_delivered(&worker.id, 2).map(|_| ());
    let noted = store.note_delivered(&worker.id, 2);
    let elapsed = started.elapsed();
    peer.join().unwrap();

    for (name, res) in [
        ("note_turn_recovered", recovered),
        ("advance_delivered", advanced),
        ("note_delivered", noted),
    ] {
        let err = res.expect_err(&format!("{name} must surface lock after budget"));
        assert!(
            is_transient_lock_error(&err),
            "{name} unexpected error: {err:?}"
        );
    }
    assert!(
        elapsed < Duration::from_secs(2),
        "budget bounds the wait: {elapsed:?}"
    );
    // No partial state change escaped the failed attempts.
    assert_eq!(
        store.open_turn(&worker.id).unwrap().unwrap().recovery_count,
        0
    );
}

#[test]
fn turn_lifecycle_non_lock_errors_are_not_retried() {
    let (_dir, store) = setup();
    let missing = AgentId::generate();

    {
        let started = std::time::Instant::now();
        let result = store.note_turn_recovered(&missing, 1);
        assert!(
            matches!(result, Err(Error::AgentNotFound(_))),
            "note_turn_recovered: {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "note_turn_recovered must not retry"
        );
    }
    {
        let started = std::time::Instant::now();
        let result = store.advance_delivered(&missing, 1).map(|_| ());
        assert!(
            matches!(result, Err(Error::AgentNotFound(_))),
            "advance_delivered: {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "advance_delivered must not retry"
        );
    }
    {
        let started = std::time::Instant::now();
        let result = store.note_delivered(&missing, 1);
        assert!(
            matches!(result, Err(Error::AgentNotFound(_))),
            "note_delivered: {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "note_delivered must not retry"
        );
    }
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
