use super::super::ingress::HISTORY_READ_PROBE;
use super::super::topology::has_quoted_block;
use super::super::*;
use super::helpers::{agent, setup};
use crate::quote::Quote;

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
