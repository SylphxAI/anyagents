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
    let current: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )?;
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
