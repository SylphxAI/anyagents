use super::*;
use crate::db::{TEST_POSTGRES_URL_ENV, block_on_pg, shared_pg_handle};
use crate::store::memory_index::Chunk;
use crate::{HistorySearchQuery, Incoming, NewAgent, ReceiveOutcome, Store, rag_content_hash};
use sqlx::ConnectOptions as _;
use sqlx::postgres::PgConnectOptions;

#[test]
fn postgres_projection_caps_redundant_data_skips_replays_and_resumes_reset() {
    let Ok(url) = std::env::var(TEST_POSTGRES_URL_ENV) else {
        eprintln!("skipping isolated PostgreSQL projection test: {TEST_POSTGRES_URL_ENV} not set");
        return;
    };
    let options: PgConnectOptions = url.parse().unwrap();
    let db = format!("rag_fixture_{}", uuid::Uuid::new_v4().simple());
    // A disposable database is required: reset tests manipulate global marks.
    let mut admin = block_on_pg(options.connect()).unwrap();
    block_on_pg(sqlx::query(&format!("CREATE DATABASE {db}")).execute(&mut admin)).unwrap();
    let fixture_url = options.database(&db).to_url_lossy().to_string();
    let outcome = std::panic::catch_unwind(|| exercise(&fixture_url));
    block_on_pg(sqlx::query(&format!("DROP DATABASE {db} WITH (FORCE)")).execute(&mut admin))
        .unwrap();
    if let Err(error) = outcome {
        std::panic::resume_unwind(error);
    }
}

fn exercise(url: &str) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open_postgres(url, dir.path()).unwrap();
    let handle = shared_pg_handle(url).unwrap();
    let pool = &handle.pool;
    let agent = store
        .create_agent(NewAgent::new("worker", "spiron"))
        .unwrap();
    let body = format!("字{} tailcanary", "生活😀".repeat(3_000));
    let ReceiveOutcome::New(message) = store.receive(&agent.id, Incoming::new(&body)).unwrap()
    else {
        panic!("expected new message");
    };
    let tool_body = format!("[tool:probe] {body}");
    let tool = store
        .record_tool_event(&agent.id, "probe", &tool_body)
        .unwrap();
    sync_on_postgres(pool, pool, &[], true, BATCH).unwrap();
    let tuple = |id: &str| -> (String, String, String, i64, bool) {
        pool.db_query_one(
            "SELECT xmin::text, text, content_hash, octet_length(search_tokens)::bigint,
                    search_tsv @@ to_tsquery('simple', '生活')
             FROM memory_index.rag_chunks WHERE chunk_id = ?1",
            &db_params![id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap()
    };
    let before = tuple(message.id.as_str());
    assert!(before.1.len() <= 16 * 1024 && body.starts_with(&before.1));
    assert_eq!(before.2, rag_content_hash(&body));
    assert!(before.3 <= 8 * 1024 && before.4);
    let standalone = tuple(&tool);
    assert_eq!(standalone.1, tool_body, "standalone text is authoritative");
    assert!(standalone.3 <= 8 * 1024 && standalone.4);
    let hits = store
        .search_history(&HistorySearchQuery {
            query: Some("tailcanary".into()),
            agent: Some(agent.name.clone()),
            include_tools: false,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, message.id.as_str());
    assert!(
        hits[0].text == body,
        "search returns the full canonical body"
    );
    let raw = store.message(&agent.id, &hits[0].id).unwrap().unwrap();
    assert!(raw.text == body, "lookup by raw ID preserves every byte");

    let b64 = "aB9+/w==".repeat(5_000);
    let data_body = format!("marker data:image/png;base64,{b64}");
    let ReceiveOutcome::New(data_msg) =
        store.receive(&agent.id, Incoming::new(&data_body)).unwrap()
    else {
        panic!("expected data message");
    };
    let search = store
        .search_history(&HistorySearchQuery {
            query: Some("marker".into()),
            agent: Some(agent.name.clone()),
            include_tools: false,
            ..Default::default()
        })
        .unwrap();
    assert!(search.iter().any(|hit| hit.id == data_msg.id.as_str()));
    let projected: bool = pool
        .db_query_one(
            "SELECT EXISTS(SELECT 1 FROM memory_index.rag_chunks WHERE chunk_id = ?1)",
            &db_params![data_msg.id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        !projected,
        "mailbox search must not depend on eager projection"
    );
    store.sync_memory_index().unwrap();
    let projected_tokens: String = pool
        .db_query_one(
            "SELECT search_tokens FROM memory_index.rag_chunks WHERE chunk_id = ?1",
            &db_params![data_msg.id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        projected_tokens, "marker",
        "data URIs don't consume the lexical budget"
    );
    assert_eq!(tuple(data_msg.id.as_str()).2, rag_content_hash(&data_body));

    let ids = [message.id.to_string()];
    sync_on_postgres(pool, pool, &ids, true, BATCH).unwrap();
    assert_eq!(
        tuple(message.id.as_str()),
        before,
        "equal replay must not create a new tuple"
    );
    pool.db_execute(
        "UPDATE messages SET body = ?2 WHERE id = ?1",
        &db_params![message.id.as_str(), format!("{body} amended")],
    )
    .unwrap();
    sync_on_postgres(pool, pool, &ids, true, BATCH).unwrap();
    let edited = tuple(message.id.as_str());
    assert_ne!(edited.0, before.0);
    assert_eq!(edited.1, before.1, "edit is beyond the projection prefix");
    assert_ne!(edited.2, before.2, "the hash covers the full body");

    for id in ["stale_first", "stale_second"] {
        upsert_chunk(
            pool,
            &Chunk {
                id,
                agent_id: agent.id.as_str(),
                agent_name: &agent.name,
                direction: "in",
                sender: "peer",
                destination: "worker",
                timestamp_ms: 1,
                text: "stale row without a mailbox entry",
                kind: None,
                tool: None,
                seq: 1,
            },
        )
        .unwrap();
    }
    pool.db_execute(
        "UPDATE index_meta SET value = value + 100 WHERE key = 'messages_rowid'",
        &[],
    )
    .unwrap();
    assert_eq!(
        sync_on_postgres(pool, pool, &[], true, 1).unwrap(),
        Some(false)
    );
    assert!(reset_cursor(pool).unwrap().is_some());
    assert_eq!(
        stale_count(pool),
        2,
        "the first transaction only inspected one row"
    );
    for _ in 0..5 {
        if reset_cursor(pool).unwrap().is_none() {
            break;
        }
        sync_on_postgres(pool, pool, &[], true, 1).unwrap();
    }
    assert!(reset_cursor(pool).unwrap().is_none());
    assert_eq!(
        sync_on_postgres(pool, pool, &[], true, BATCH).unwrap(),
        Some(true)
    );
    assert_eq!(stale_count(pool), 0);
    assert_eq!(
        tuple(message.id.as_str()),
        edited,
        "reset must not rewrite a live unchanged row"
    );
    assert_eq!(tuple(&tool), standalone);

    // Both the direct mailbox path and the standalone tool projection must
    // reject separated CJK characters. Mixed queries still match both terms
    // even when they are not next to each other in the source.
    let target = "Telegram 新頭像風格 ひらがな カタカナ 한국어";
    for text in [
        target,
        "Telegram 由頭跟到尾 ひ、ら カ、タ 한、국",
        "由頭像到像風",
    ] {
        store.receive(&agent.id, Incoming::new(text)).unwrap();
        store.record_tool_event(&agent.id, "probe", text).unwrap();
    }
    let hidden = store
        .create_agent(NewAgent::new("hidden", "spiron"))
        .unwrap();
    store.receive(&hidden.id, Incoming::new(target)).unwrap();
    store
        .record_tool_event(&hidden.id, "probe", target)
        .unwrap();
    for only_tools in [false, true] {
        for query in ["頭像風", "ひら", "カタ", "한국", "Telegram 頭像"] {
            let hits = store
                .search_history(&HistorySearchQuery {
                    query: Some(query.into()),
                    include_tools: only_tools,
                    only_tools,
                    visible_agent_ids: Some(vec![agent.id.to_string()]),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(hits.len(), 1, "{query}, tools={only_tools}: {hits:?}");
            assert!(hits[0].text.ends_with(target));
            let expected = if only_tools { "tool_" } else { "msg_" };
            assert!(hits[0].id.starts_with(expected));
            assert!(hits[0].from != "agent:hidden");
        }
        for query in ["頭", "Telegram"] {
            let hits = store
                .search_history(&HistorySearchQuery {
                    query: Some(query.into()),
                    include_tools: only_tools,
                    only_tools,
                    ..Default::default()
                })
                .unwrap();
            assert!(hits.len() >= 2, "{query}, tools={only_tools}");
        }
    }
}

fn stale_count(pool: &sqlx::PgPool) -> i64 {
    pool.db_query_one(
        "SELECT count(*) FROM memory_index.rag_chunks WHERE chunk_id IN ('stale_first', 'stale_second')",
        &[],
        |r| r.get(0),
    ).unwrap()
}
