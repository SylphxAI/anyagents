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
    (29, include_str!("../migrations/030_rejected_messages.sql")),
    (
        30,
        include_str!("../migrations/031_remote_session_tokens.sql"),
    ),
    (31, include_str!("../migrations/032_operator_inbox.sql")),
    (32, include_str!("../migrations/033_turn_errors.sql")),
    (
        33,
        include_str!("../migrations/034_message_alias_indexes.sql"),
    ),
    (
        MEMORY_INDEX_SPLIT,
        include_str!("../migrations/035_memory_index_split.sql"),
    ),
    (
        35,
        include_str!("../migrations/036_normalize_agent_roles.sql"),
    ),
    (36, include_str!("../migrations/037_turn_events.sql")),
    (37, include_str!("../migrations/038_task_turn_sends.sql")),
    (38, include_str!("../migrations/039_deployment_secrets.sql")),
    (
        39,
        include_str!("../migrations/040_browser_profile_grants.sql"),
    ),
    (40, include_str!("../migrations/041_agent_runtime.sql")),
    (
        41,
        include_str!("../migrations/042_schedule_external_timer.sql"),
    ),
    (
        42,
        include_str!("../migrations/043_channel_identity_unique.sql"),
    ),
];

/// The version that moves the mailbox search index out of the state
/// database. Opening an older database first copies what only its search
/// tables hold into the index (`store/memory_index.rs`).
pub const MEMORY_INDEX_SPLIT: i64 = 34;

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
            conn.prepare("SELECT seq FROM outbound_messages").unwrap();
            assert!(conn.prepare("SELECT 1 FROM rag_chunks").is_err());
            assert!(conn.prepare("SELECT 1 FROM rag_chunks_fts").is_err());
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

    #[test]
    fn normalize_agent_roles_migration_backfills_null_and_blank_without_data_loss() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
        )
        .unwrap();
        for (version, sql) in MIGRATIONS
            .iter()
            .filter(|(version, _)| *version <= MEMORY_INDEX_SPLIT)
        {
            conn.execute_batch(sql).unwrap();
            conn.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
                .unwrap();
        }
        conn.execute(
            "INSERT INTO agents (id, name, role, harness, workdir, instructions, state, created_at_ms)
             VALUES (?1, ?2, ?3, 'claude-code', '/tmp/w', ?4, 'stopped', 100)",
            rusqlite::params!["agt_null", "null-role", Option::<String>::None, "Keep custom prompt"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agents (id, name, role, harness, workdir, instructions, state, created_at_ms)
             VALUES (?1, ?2, ?3, 'claude-code', '/tmp/w', ?4, 'stopped', 100)",
            rusqlite::params!["agt_blank", "blank-role", "   ", "Keep blank-role prompt"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agents (id, name, role, harness, workdir, instructions, state, created_at_ms)
             VALUES (?1, ?2, ?3, 'claude-code', '/tmp/w', ?4, 'stopped', 100)",
            rusqlite::params!["agt_keep", "named-role", "engineer", "Keep engineer prompt"],
        )
        .unwrap();

        migrate(&mut conn).unwrap();
        assert_eq!(
            current_version(&conn).unwrap(),
            MIGRATIONS.last().unwrap().0
        );

        let fetch = |id: &str| -> (Option<String>, Option<String>) {
            conn.query_row(
                "SELECT role, instructions FROM agents WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
        };
        assert_eq!(
            fetch("agt_null"),
            (Some("general".into()), Some("Keep custom prompt".into()))
        );
        assert_eq!(
            fetch("agt_blank"),
            (
                Some("general".into()),
                Some("Keep blank-role prompt".into())
            )
        );
        assert_eq!(
            fetch("agt_keep"),
            (Some("engineer".into()), Some("Keep engineer prompt".into()))
        );
    }

    #[test]
    fn agent_runtime_migration_upgrades_v39_preserving_rows_and_backfilling_cloud() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
        )
        .unwrap();
        for (version, sql) in MIGRATIONS.iter().filter(|(version, _)| *version <= 39) {
            conn.execute_batch(sql).unwrap();
            conn.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
                .unwrap();
        }
        assert_eq!(current_version(&conn).unwrap(), 39);

        let fixtures = [
            ("agt_local_cc", "local-cc", "claude-code", "Keep cc prompt"),
            ("agt_local_sp", "local-sp", "spiron", "Keep spiron prompt"),
            ("agt_cld_sp", "cld-sp", "spiron-cloud", "Keep spiron cloud"),
            ("agt_cld_cc", "cld-cc", "claude-cloud", "Keep claude cloud"),
            ("agt_cld_oa", "cld-oa", "openai-cloud", "Keep openai cloud"),
            ("agt_cld_cu", "cld-cu", "cursor-cloud", "Keep cursor cloud"),
        ];
        for (id, name, harness, instructions) in fixtures {
            conn.execute(
                "INSERT INTO agents (id, name, role, harness, instructions, state, created_at_ms)
                 VALUES (?1, ?2, 'engineer', ?3, ?4, 'idle', 1234)",
                rusqlite::params![id, name, harness, instructions],
            )
            .unwrap();
        }

        migrate(&mut conn).unwrap();
        assert_eq!(
            current_version(&conn).unwrap(),
            MIGRATIONS.last().unwrap().0
        );

        let fetch = |id: &str| -> (String, String, Option<String>, String, Option<i64>) {
            conn.query_row(
                "SELECT name, harness, instructions, runtime, remote_budget_micros FROM agents WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap()
        };

        assert_eq!(
            fetch("agt_local_cc"),
            (
                "local-cc".into(),
                "claude-code".into(),
                Some("Keep cc prompt".into()),
                "local".into(),
                None
            )
        );
        assert_eq!(
            fetch("agt_local_sp"),
            (
                "local-sp".into(),
                "spiron".into(),
                Some("Keep spiron prompt".into()),
                "local".into(),
                None
            )
        );
        for (id, name, harness, instructions) in &fixtures[2..] {
            assert_eq!(
                fetch(id),
                (
                    (*name).into(),
                    (*harness).into(),
                    Some((*instructions).into()),
                    "cloud".into(),
                    None
                ),
                "legacy cloud agent {id} must backfill runtime = 'cloud'"
            );
        }

        // Idempotent re-run at latest version.
        migrate(&mut conn).unwrap();
        assert_eq!(
            current_version(&conn).unwrap(),
            MIGRATIONS.last().unwrap().0
        );
    }
}
