use super::*;
use crate::db::{TEST_POSTGRES_URL_ENV, block_on_pg};
use crate::migrate::{MIGRATIONS, current_version};
use rusqlite::Connection;
use sqlx::ConnectOptions as _;
use sqlx::postgres::PgConnectOptions;

const OLD: &str = "chn_00000000000000000000000003";
const TIE: &str = "chn_00000000000000000000000001";
const KEEP: &str = "chn_00000000000000000000000002";
const FIRST: &str = "agt_00000000000000000000000001";
const SECOND: &str = "agt_00000000000000000000000002";
const IDENTITY: &str = "test_bot";
const STATE: &str = r#"{"version":2}"#;

fn seed() -> String {
    format!(
        "INSERT INTO agents (id, name, harness, state, created_at_ms)
         VALUES ('{FIRST}', 'worker-one', 'spiron', 'stopped', 1),
                ('{SECOND}', 'worker-two', 'spiron', 'stopped', 1);
         INSERT INTO channels (id, kind, identity, created_at_ms, credential_ref, state)
         VALUES ('{OLD}', 'telegram', '{IDENTITY}', 1, 'channels/old', '{{}}'),
                ('{TIE}', 'telegram', '{IDENTITY}', 2, 'channels/tie', '{{}}'),
                ('{KEEP}', 'telegram', '{IDENTITY}', 2, 'channels/current', '{STATE}'),
                ('chn_00000000000000000000000004', 'telegram', NULL, 1, NULL, '{{}}'),
                ('chn_00000000000000000000000005', 'telegram', NULL, 1, NULL, '{{}}'),
                ('chn_00000000000000000000000006', 'other', '{IDENTITY}', 1, NULL, '{{}}');
         INSERT INTO channel_agents (channel_id, agent_id)
         VALUES ('{OLD}', '{FIRST}'), ('{TIE}', '{FIRST}'),
                ('{OLD}', '{SECOND}'), ('{KEEP}', '{SECOND}');"
    )
}

fn assert_migrated_channels(store: &Store) {
    let channels = store.list_channels().unwrap();
    assert_eq!(channels.len(), 4);
    assert_eq!(channels.iter().filter(|c| c.identity.is_none()).count(), 2);
    assert!(
        channels
            .iter()
            .any(|c| c.kind == "other" && c.identity.as_deref() == Some(IDENTITY))
    );
    let canonical = channels.iter().find(|c| c.id.as_str() == KEEP).unwrap();
    assert_eq!(
        canonical.credential_ref.as_deref(),
        Some("channels/current")
    );
    assert_eq!(canonical.state, STATE);
    assert_eq!(
        canonical
            .agents
            .iter()
            .map(AgentId::as_str)
            .collect::<Vec<_>>(),
        [FIRST, SECOND]
    );

    let new = |identity: Option<&str>| NewChannel {
        kind: "telegram".into(),
        identity: identity.map(str::to_owned),
        config: "{}".into(),
        credential_ref: Some("channels/conflict".into()),
    };
    let err = store.create_channel(new(Some(IDENTITY))).unwrap_err();
    assert_eq!(
        err.to_string(),
        "channel kind and identity already registered"
    );
    let unclaimed = store.create_channel(new(None)).unwrap();
    assert!(
        store
            .set_channel_identity(&unclaimed.id, Some(IDENTITY))
            .is_err()
    );
    assert_eq!(
        store.channel(&unclaimed.id).unwrap().unwrap().identity,
        None
    );
    assert_eq!(
        store
            .channel(&canonical.id)
            .unwrap()
            .unwrap()
            .credential_ref,
        canonical.credential_ref
    );
}

#[test]
fn sqlite_channel_identity_upgrade_preserves_canonical_state_and_attachments() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA foreign_keys = ON; CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);").unwrap();
    for (version, sql) in MIGRATIONS.iter().take_while(|(v, _)| *v < 42) {
        conn.execute_batch(sql).unwrap();
        conn.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
            .unwrap();
    }
    conn.execute_batch(&seed()).unwrap();
    drop(conn);

    let store = Store::open(&path).unwrap();
    assert_migrated_channels(&store);
    let conn = Connection::open(&path).unwrap();
    assert!(current_version(&conn).unwrap() >= 42);
    assert!(
        conn.prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query([])
            .unwrap()
            .next()
            .unwrap()
            .is_none()
    );
}

#[test]
fn postgres_channel_identity_upgrade_preserves_canonical_state_and_attachments() {
    let Ok(url) = std::env::var(TEST_POSTGRES_URL_ENV) else {
        eprintln!("skipping PostgreSQL channel migration test: {TEST_POSTGRES_URL_ENV} not set");
        return;
    };
    let options: PgConnectOptions = url.parse().unwrap();
    let db = format!("channel_fixture_{}", uuid::Uuid::new_v4().simple());
    let mut admin = block_on_pg(options.connect()).unwrap();
    block_on_pg(sqlx::query(&format!("CREATE DATABASE {db}")).execute(&mut admin)).unwrap();
    let outcome = std::panic::catch_unwind(|| {
        let fixture = options.database(&db);
        block_on_pg(async {
            let mut conn = fixture.connect().await.unwrap();
            sqlx::raw_sql(crate::db::PG_INIT_SQL)
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::query("INSERT INTO pg_schema_migrations VALUES (1, 0)")
                .execute(&mut conn)
                .await
                .unwrap();
            sqlx::raw_sql(&seed()).execute(&mut conn).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open_postgres(fixture.to_url_lossy().as_str(), dir.path()).unwrap();
        assert_migrated_channels(&store);
        let version: i64 = block_on_pg(async {
            let mut conn = fixture.connect().await.unwrap();
            sqlx::query_scalar("SELECT max(version) FROM pg_schema_migrations")
                .fetch_one(&mut conn)
                .await
                .unwrap()
        });
        assert!(version >= 9);
    });
    block_on_pg(sqlx::query(&format!("DROP DATABASE {db} WITH (FORCE)")).execute(&mut admin))
        .unwrap();
    if let Err(error) = outcome {
        std::panic::resume_unwind(error);
    }
}
