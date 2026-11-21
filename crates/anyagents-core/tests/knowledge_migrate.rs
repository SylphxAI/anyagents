use anyagents_core::db::{
    MigrateToPostgresOptions, configured_direct_database_url, migrate_sqlite_dir_to_postgres,
};
use anyagents_core::knowledge::{Actor, Edit, Mode, Scope};
use anyagents_core::{GroupId, NewAgent, Store};
use sqlx::{ConnectOptions, postgres::PgConnectOptions};

#[test]
fn knowledge_survives_copy_and_resume_with_optional_search() {
    let Ok(url) = std::env::var("ANYAGENTS_TEST_POSTGRES_URL") else {
        eprintln!("skipping knowledge migration: ANYAGENTS_TEST_POSTGRES_URL unset");
        return;
    };
    let config: PgConnectOptions = url.parse().unwrap();
    let db = format!("knowledge_fixture_{}", uuid::Uuid::new_v4().simple());
    let url = config.clone().database(&db).to_url_lossy().to_string();
    assert_eq!(
        configured_direct_database_url(&url),
        url,
        "disable the direct database override for this fixture"
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut admin = rt.block_on(config.connect()).unwrap();
    rt.block_on(sqlx::query(&format!("CREATE DATABASE {db}")).execute(&mut admin))
        .unwrap();
    let outcome = std::panic::catch_unwind(|| check_migration(&rt, &url));
    rt.block_on(sqlx::query(&format!("DROP DATABASE {db} WITH (FORCE)")).execute(&mut admin))
        .unwrap();
    if let Err(err) = outcome {
        std::panic::resume_unwind(err);
    }
}

fn check_migration(rt: &tokio::runtime::Runtime, url: &str) {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state.db");
    let source = Store::open(&state_path).unwrap();
    let owner = Actor::Owner("fixture-owner".into());
    let agent = source
        .create_agent(NewAgent::new("knowledge-reader", "test"))
        .unwrap();
    let reader = Actor::Agent(agent.id.clone());
    let group = GroupId::parse("knowledge-readers").unwrap();
    source.join_group(group.as_str(), &agent.id).unwrap();
    let ns = "org:instructions";
    let topic = "readiness";
    let phrase = "部署守則";
    let edit = Edit {
        title: Some("Release checklist".into()),
        description: Some("Verify the production rollout".into()),
        body: Some(phrase.into()),
        reason: "fixture".into(),
        sim_expected: Some("expected result".into()),
        ..Edit::default()
    };
    source
        .knowledge_write(&owner, ns, topic, Some(0), &edit, false)
        .unwrap();
    source
        .knowledge_grant_group(&owner, &group, ns, Mode::Read)
        .unwrap();
    source
        .knowledge_grant(&owner, &agent.id, ns, Mode::Propose)
        .unwrap();
    source
        .knowledge_propose(&reader, ns, topic, 1, &edit)
        .unwrap();

    let live = source
        .knowledge_write(&owner, ns, topic, Some(1), &edit, false)
        .unwrap();
    source
        .knowledge_write(&owner, ns, "retired", Some(0), &edit, false)
        .unwrap();
    source
        .knowledge_write(&owner, ns, "retired", Some(1), &edit, true)
        .unwrap();

    // Never search on SQLite. Even if the memory cache was opened, migrate
    // without it or any legacy projection in state.db.
    drop(source);
    for file in [
        "memory-index.db",
        "memory-index.db-wal",
        "memory-index.db-shm",
    ] {
        let path = dir.path().join(file);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    let state = rusqlite::Connection::open_with_flags(
        &state_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let projections: i64 = state
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name GLOB 'knowledge_rag_chunks*'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        projections, 0,
        "state.db must not contain a knowledge projection"
    );
    drop(state);

    let mut opts = MigrateToPostgresOptions::new(dir.path(), url);
    opts.master_key = None;
    opts.skip_index = true;
    let first = migrate_sqlite_dir_to_postgres(&opts).unwrap();
    for (table, count) in [
        ("knowledge_docs", 2),
        ("knowledge_revisions", 4),
        ("knowledge_grants", 1),
        ("knowledge_group_grants", 1),
        ("knowledge_proposals", 1),
    ] {
        let stat = first
            .tables
            .iter()
            .find(|s| s.pg_table == format!("public.{table}"))
            .unwrap();
        assert_eq!(
            (stat.sqlite_rows, stat.pg_rows_before, stat.pg_rows_after),
            (count, 0, count)
        );
    }
    assert!(
        !first
            .tables
            .iter()
            .any(|s| s.pg_table == "knowledge_index.rag_chunks")
    );
    let target_dir = tempfile::tempdir().unwrap();
    let target = Store::open_postgres(url, target_dir.path()).unwrap();
    let scope = Scope::Namespace(ns.into());
    assert_eq!(
        target.knowledge_read(&reader, ns, topic).unwrap().body,
        phrase
    );
    assert!(
        target
            .knowledge_search(&reader, &scope, phrase, 5)
            .unwrap()
            .is_empty()
    );

    // Resume the same snapshot with the optional search projection enabled.
    opts.skip_index = false;
    opts.resume = true;
    let resumed = migrate_sqlite_dir_to_postgres(&opts).unwrap();
    let stat = resumed
        .tables
        .iter()
        .find(|s| s.pg_table == "public.knowledge_revisions")
        .unwrap();
    assert_eq!(
        (stat.sqlite_rows, stat.pg_rows_before, stat.pg_rows_after),
        (4, 4, 4)
    );
    let stat = resumed
        .tables
        .iter()
        .find(|s| s.pg_table == "knowledge_index.rag_chunks")
        .unwrap();
    assert_eq!(
        (stat.sqlite_rows, stat.pg_rows_before, stat.pg_rows_after),
        (1, 0, 1),
        "only live documents should contribute to the projection"
    );
    let history = target
        .knowledge_revisions(&reader, ns, topic, None, 5)
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[0].version,
        target.knowledge_read(&reader, ns, topic).unwrap().version
    );
    assert_eq!(history[0].prior_body.as_deref(), Some(phrase));
    assert_eq!(history[0].sim_expected.as_deref(), Some("expected result"));
    assert_eq!(
        target
            .knowledge_search(&reader, &scope, phrase, 5)
            .unwrap()
            .len(),
        1
    );
    assert!(
        target
            .knowledge_search(&reader, &scope, "部署守", 5)
            .unwrap()
            .len()
            == 1
    );
    assert!(
        target
            .knowledge_search(&reader, &scope, "署部守", 5)
            .unwrap()
            .is_empty()
    );

    let rerun = migrate_sqlite_dir_to_postgres(&opts).unwrap();
    let stat = rerun
        .tables
        .iter()
        .find(|s| s.pg_table == "knowledge_index.rag_chunks")
        .unwrap();
    assert_eq!(
        (stat.sqlite_rows, stat.pg_rows_before, stat.pg_rows_after),
        (1, 1, 1)
    );

    opts.resume = false;
    migrate_sqlite_dir_to_postgres(&opts).unwrap();
    let native = target
        .knowledge_write(&owner, "org:parity", topic, Some(0), &edit, false)
        .unwrap();
    rt.block_on(async {
        let config: PgConnectOptions = url.parse().unwrap();
        let mut conn = config.connect().await.unwrap();
        let same_projection: bool = sqlx::query_scalar(
            "SELECT migrated.content_hash = native.content_hash AND migrated.search_tsv = native.search_tsv
             FROM knowledge_index.rag_chunks migrated, knowledge_index.rag_chunks native
             WHERE migrated.chunk_id = $1 AND native.chunk_id = $2",
        )
        .bind(&live.id)
        .bind(&native.id)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(same_projection, "migrated and native hashes and tokens must agree");
        let err = sqlx::query("UPDATE public.knowledge_revisions SET reason = reason")
            .execute(&mut conn)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("knowledge revisions are append-only")
        );
    });
}
