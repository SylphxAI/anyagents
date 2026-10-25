use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

use crate::error::Error;

/// Milliseconds since the Unix epoch. A clock that is before the epoch reads as 0.
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub const MIGRATIONS: &[(i64, &str)] = &[
    (1, include_str!("../migrations/001_init.sql")),
    (2, include_str!("../migrations/002_channel_credential.sql")),
    (3, include_str!("../migrations/003_failed_turns.sql")),
    (4, include_str!("../migrations/004_channel_state.sql")),
    (5, include_str!("../migrations/005_launch_env.sql")),
    (6, include_str!("../migrations/006_groups.sql")),
    (7, include_str!("../migrations/008_one_delivery_cursor.sql")),
    (8, include_str!("../migrations/009_capabilities.sql")),
    (9, include_str!("../migrations/010_sends.sql")),
    (10, include_str!("../migrations/011_message_origin.sql")),
    (11, include_str!("../migrations/012_message_context.sql")),
    (12, include_str!("../migrations/013_agent_parent.sql")),
    (13, include_str!("../migrations/014_outbound_messages.sql")),
    (14, include_str!("../migrations/015_rag_search.sql")),
    (15, include_str!("../migrations/016_turn_recovery.sql")),
    (16, include_str!("../migrations/017_admin_authority.sql")),
    (17, "-- Reconcile the two published version-14 histories."),
];

/// Apply pending migrations. Each migration commits on its own, in order.
pub fn migrate(conn: &mut Connection) -> Result<(), Error> {
    let latest = MIGRATIONS.last().map(|(version, _)| *version).unwrap_or(0);
    if current_version(conn)? >= latest {
        return Ok(());
    }
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at_ms INTEGER NOT NULL
        );",
    )?;
    let current = current_version(conn)?;
    // Main shipped admin authority at v14; the desk shipped RAG at v14,
    // turn recovery at v15 and admin at v16. Never relabel existing ledger
    // rows. A main store takes an atomic bridge to v17 instead of replaying
    // the desk's incompatible v14-v16 sequence.
    let main_history = (14..=15).contains(&current)
        && conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('agents') WHERE name = 'is_admin')",
            [],
            |row| row.get::<_, bool>(0),
        )?;
    if main_history {
        let tx = conn.transaction()?;
        tx.execute_batch(MIGRATIONS[13].1)?;
        tx.execute_batch(MIGRATIONS[14].1)?;
        tx.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (17, ?1)",
            [now_ms()],
        )?;
        tx.commit()?;
        return Ok(());
    }
    for (version, sql) in MIGRATIONS {
        if *version <= current {
            continue;
        }
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.execute(
            "INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?1, ?2)",
            rusqlite::params![version, now_ms()],
        )?;
        tx.commit()?;
    }
    Ok(())
}

/// Highest applied migration, or 0 when the migrations table does not exist yet.
pub fn current_version(conn: &Connection) -> Result<i64, Error> {
    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
        [],
        |row| row.get(0),
    )?;
    if exists == 0 {
        return Ok(0);
    }
    let version = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )?;
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(conn: &Connection, version: i64, sql: &str) {
        conn.execute_batch(sql).unwrap();
        conn.execute(
            "INSERT INTO schema_migrations VALUES (?1, ?2)",
            [version, 1000 + version],
        )
        .unwrap();
    }

    fn ledger(conn: &Connection) -> Vec<(i64, i64)> {
        conn.prepare("SELECT version, applied_at_ms FROM schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    // Capture every value in the durable tables, not just their row counts.
    fn records(conn: &Connection) -> Vec<String> {
        let mut snapshot = Vec::new();
        for table in [
            "agents",
            "messages",
            "channels",
            "channel_agents",
            "agent_capabilities",
            "admin_audit",
            "mailbox_cursors",
        ] {
            let mut stmt = conn
                .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
                .unwrap();
            let columns = stmt.column_count();
            let rows = stmt
                .query_map([], |row| {
                    let values = (0..columns)
                        .map(|i| row.get::<_, rusqlite::types::Value>(i))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(format!("{table}: {values:?}"))
                })
                .unwrap();
            snapshot.extend(rows.map(Result::unwrap));
        }
        snapshot
    }

    fn seed(conn: &Connection) {
        conn.execute_batch(
            "INSERT INTO agents (id, name, harness, state, created_at_ms, is_admin)
                 VALUES ('a', 'owner', 'claude-code', 'running', 123, 1);
             INSERT INTO channels (id, kind, identity, credential_ref, created_at_ms)
                 VALUES ('c', 'telegram', 'room', 'fixture-ref', 456);
             INSERT INTO channel_agents VALUES ('c', 'a');
             INSERT INTO messages (id, agent_id, seq, body, received_at_ms)
                 VALUES ('m', 'a', 1, 'keep this message', 789);
             INSERT INTO mailbox_cursors (agent_id, delivered_seq) VALUES ('a', 1);
             INSERT INTO agent_capabilities VALUES ('fixture-hash', 'a', 7, 123, NULL);
             INSERT INTO admin_audit VALUES (1, 123, 'operator', 'grant', 'owner');",
        )
        .unwrap();
    }

    fn upgrade(history: &str) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let mut conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON; CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);").unwrap();
        for (version, sql) in MIGRATIONS.iter().take(13) {
            apply(&conn, *version, sql);
        }
        match history {
            "main" | "partial-main" => {
                apply(
                    &conn,
                    14,
                    include_str!("../migrations/015_admin_authority.sql"),
                );
                seed(&conn);
                if history == "partial-main" {
                    apply(&conn, 15, MIGRATIONS[14].1);
                }
            }
            "live" => {
                for (version, sql) in &MIGRATIONS[13..16] {
                    apply(&conn, *version, sql);
                }
                seed(&conn);
                conn.execute_batch("INSERT INTO rag_meta VALUES ('fixture', 'preserve'); INSERT INTO turn_recovery VALUES ('a', 9, 3, 2, 456);").unwrap();
            }
            _ => unreachable!(),
        }
        let before = records(&conn);
        let old_ledger = ledger(&conn);
        migrate(&mut conn).unwrap();
        assert_eq!(current_version(&conn).unwrap(), 17);
        assert_eq!(records(&conn), before);
        assert_eq!(&ledger(&conn)[..old_ledger.len()], old_ledger);
        if history != "live" {
            assert_eq!(ledger(&conn).len(), old_ledger.len() + 1);
        }
        conn.execute_batch("SELECT * FROM rag_chunks; SELECT * FROM rag_chunks_fts; SELECT * FROM rag_embeddings; SELECT * FROM turn_recovery;").unwrap();
        let upgraded_ledger = ledger(&conn);
        let schema: String = conn
            .query_row("SELECT group_concat(sql) FROM sqlite_master", [], |r| {
                r.get(0)
            })
            .unwrap();
        drop(conn);
        // Exercise the same open path the server uses, twice.
        for _ in 0..2 {
            drop(crate::Store::open(&path).unwrap());
            let conn = Connection::open(&path).unwrap();
            assert_eq!(records(&conn), before);
            assert_eq!(ledger(&conn), upgraded_ledger);
            assert_eq!(
                conn.query_row("SELECT group_concat(sql) FROM sqlite_master", [], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
                schema
            );
            if history == "live" {
                assert_eq!(conn.query_row("SELECT opened_seq, completed_seq, recovery_count, opened_at_ms FROM turn_recovery WHERE agent_id='a'", [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?))).unwrap(), (9, 3, 2, 456));
                assert_eq!(
                    conn.query_row("SELECT value FROM rag_meta WHERE key='fixture'", [], |r| {
                        r.get::<_, String>(0)
                    })
                    .unwrap(),
                    "preserve"
                );
            }
        }
    }

    #[test]
    fn failed_main_bridge_rolls_back_schema_and_ledger() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);").unwrap();
        for (version, sql) in MIGRATIONS.iter().take(13) {
            apply(&conn, *version, sql);
        }
        apply(
            &conn,
            14,
            include_str!("../migrations/015_admin_authority.sql"),
        );
        seed(&conn);
        let before = records(&conn);
        let before_ledger = ledger(&conn);
        // The second half of the bridge must fail after RAG DDL ran.
        conn.execute_batch("CREATE VIEW turn_recovery AS SELECT 1 AS invalid;")
            .unwrap();
        assert!(migrate(&mut conn).is_err());
        assert_eq!(ledger(&conn), before_ledger);
        assert_eq!(records(&conn), before);
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='rag_chunks'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        conn.execute_batch("DROP VIEW turn_recovery;").unwrap();
        migrate(&mut conn).unwrap();
        assert_eq!(current_version(&conn).unwrap(), 17);
        assert_eq!(records(&conn), before);
    }

    #[test]
    fn main_admin_v14_upgrades_without_relabeling_history() {
        upgrade("main");
    }

    #[test]
    fn interrupted_main_upgrade_at_v15_is_recoverable() {
        upgrade("partial-main");
    }

    #[test]
    fn live_rag_turn_admin_history_survives_restart() {
        upgrade("live");
    }

    #[test]
    fn fresh_database_and_restarts_are_identical() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        drop(crate::Store::open(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        seed(&conn);
        let before = records(&conn);
        let before_ledger = ledger(&conn);
        assert_eq!(before_ledger.len(), 17);
        drop(conn);
        for _ in 0..2 {
            drop(crate::Store::open(&path).unwrap());
            let conn = Connection::open(&path).unwrap();
            assert_eq!(records(&conn), before);
            assert_eq!(ledger(&conn), before_ledger);
        }
    }
}
