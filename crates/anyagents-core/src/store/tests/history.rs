use super::super::*;
use super::helpers::{agent, setup};
use crate::db_params;

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
fn history_han_phrase_beats_character_noise_outside_the_substring_window() {
    let (_dir, store) = setup();
    let alice = agent(&store, "alice");
    let target = "城市生活方式探索";
    let mut exact = Incoming::new(target);
    exact.received_at_ms = Some(100);
    store.receive(&alice.id, exact).unwrap();

    // Use a single fixture transaction; the actual index is populated by
    // the same bounded projection path as an existing mailbox on reopen.
    {
        let writer = store.sqlite_shared().writer();
        writer.execute(
            "WITH RECURSIVE sample(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM sample WHERE n < 2001)
             INSERT INTO messages (id, agent_id, seq, received_at_ms, body)
             SELECT 'msg_noise_' || n, ?1, n + 1, n + 100,
                    CASE WHEN n % 2 = 0 THEN '生，活方式' ELSE '生活環境另有活方' END
             FROM sample",
            [alice.id.as_str()],
        ).unwrap();
    }
    for (query, limit) in [("生活方式", 2), ("生活", 1)] {
        let hits = store
            .search_conversation(Some("alice"), None, None, Some(limit), query)
            .unwrap();
        assert_eq!(hits[0].text, target, "query: {query}");
        if query == "生活方式" {
            assert_eq!(
                hits.len(),
                1,
                "disjoint bigrams must not appear in the results"
            );
        }
    }
    let hits = store
        .search_conversation(Some("alice"), None, None, Some(10), "活")
        .unwrap();
    assert!(
        !hits.is_empty(),
        "single-character queries still find Han text"
    );
}

#[test]
fn history_cjk_bigrams_cover_han_kana_hangul_and_mixed_queries() {
    let (_dir, store) = setup();
    let alice = agent(&store, "alice");
    let target = "Telegram 新頭像風格 ひらがな カタカナ 한국어";
    for text in [
        target,
        "Telegram 由頭跟到尾 ひ、ら カ、タ 한、국",
        "由頭像到像風但沒有連續三字",
    ] {
        store.receive(&alice.id, Incoming::new(text)).unwrap();
    }
    for query in ["頭像風", "ひら", "カタ", "한국", "Telegram 頭像"] {
        let hits = store
            .search_conversation(Some("alice"), None, None, Some(10), query)
            .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.text.as_str()).collect::<Vec<_>>(),
            [target],
            "{query}"
        );
    }
    let hits = store
        .search_conversation(Some("alice"), None, None, Some(10), "頭像")
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(hits.iter().all(|hit| hit.text.contains("頭像")));
    for query in ["頭", "Telegram"] {
        let hits = store
            .search_conversation(Some("alice"), None, None, Some(10), query)
            .unwrap();
        assert!(hits.len() >= 2, "{query}");
    }
}
