use anyagents_core::db::{
    MigrateToPostgresOptions, configured_direct_database_url, migrate_sqlite_dir_to_postgres,
};
use anyagents_core::knowledge::{Actor, Edit, Mode, Scope};
use anyagents_core::{NewAgent, Store};
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
    let source = Store::open(dir.path().join("state.db")).unwrap();
    let owner = Actor::Owner("fixture-owner".into());
    let agent = source
        .create_agent(NewAgent::new("knowledge-reader", "test"))
        .unwrap();
    let reader = Actor::Agent(agent.id.clone());
    let ns = "org:instructions";
    let topic = "readiness";
    let phrase = "部署守則";
    let edit = Edit {
        body: Some(phrase.into()),
        reason: "fixture".into(),
        sim_expected: Some("expected result".into()),
        ..Edit::default()
    };
    source
        .knowledge_write(&owner, ns, topic, Some(0), &edit, false)
        .unwrap();
    source
        .knowledge_grant(&owner, &agent.id, ns, Mode::Read)
        .unwrap();
    source
        .knowledge_grant(&owner, &agent.id, ns, Mode::Propose)
        .unwrap();
    source
        .knowledge_propose(&reader, ns, topic, 1, &edit)
        .unwrap();

    source
        .knowledge_write(&owner, ns, topic, Some(1), &edit, false)
        .unwrap();

    let mut opts = MigrateToPostgresOptions::new(dir.path(), url);
    opts.master_key = None;
    opts.skip_index = true;
    let first = migrate_sqlite_dir_to_postgres(&opts).unwrap();
    for (table, count) in [
        ("knowledge_docs", 1),
        ("knowledge_revisions", 2),
        ("knowledge_grants", 2),
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
        (2, 2, 2)
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

    opts.resume = false;
    migrate_sqlite_dir_to_postgres(&opts).unwrap();
    rt.block_on(async {
        let config: PgConnectOptions = url.parse().unwrap();
        let mut conn = config.connect().await.unwrap();
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
