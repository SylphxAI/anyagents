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
    (
        17,
        include_str!("../migrations/018_agent_aliases_and_role.sql"),
    ),
    (18, include_str!("../migrations/019_agent_schedules.sql")),
    (
        19,
        include_str!("../migrations/020_lexical_history_search.sql"),
    ),
    (
        20,
        include_str!("../migrations/021_lexical_fts_perf_and_seq.sql"),
    ),
    (21, include_str!("../migrations/022_agent_description.sql")),
    (22, include_str!("../migrations/023_scope_audit.sql")),
    (23, include_str!("../migrations/024_group_owner.sql")),
    (
        24,
        include_str!("../migrations/025_conversation_redactions.sql"),
    ),
    (
        25,
        include_str!("../migrations/026_full_mailbox_history.sql"),
    ),
    (26, include_str!("../migrations/027_runtime_state.sql")),
    (27, include_str!("../migrations/028_hot_path_indexes.sql")),
    (28, include_str!("../migrations/029_outcomes.sql")),
    (29, include_str!("../migrations/030_operator_inbox.sql")),
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
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
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

    #[test]
    fn redaction_migration_upgrades_scope_audit_and_group_owner_without_replacing_them() {
        let latest = MIGRATIONS.last().unwrap().0;
        for baseline in [22, 23, 24] {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
        )
        .unwrap();
            for (version, sql) in MIGRATIONS
                .iter()
                .filter(|(version, _)| *version <= baseline)
            {
                conn.execute_batch(sql).unwrap();
                conn.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
                    .unwrap();
            }
            migrate(&mut conn).unwrap();
            assert_eq!(current_version(&conn).unwrap(), latest);
            conn.prepare("SELECT before_scope, after_scope, source FROM admin_audit")
                .unwrap();
            conn.prepare("SELECT owner_agent_id FROM groups").unwrap();
            conn.prepare("SELECT text_format FROM rag_chunks").unwrap();
            conn.prepare("SELECT id, target_id, receipt FROM conversation_redactions")
                .unwrap();
            conn.prepare(
            "SELECT agent_id, identity_id, namespace, chat_id, message_id FROM message_origin_aliases",
        )
        .unwrap();
            conn.prepare("SELECT agent_id, identity_id, source_key FROM message_source_aliases")
                .unwrap();
            conn.prepare("SELECT agent_id, target_id, identity_id FROM message_identities")
                .unwrap();
            migrate(&mut conn).unwrap();
            assert_eq!(current_version(&conn).unwrap(), latest);
        }
    }
}
