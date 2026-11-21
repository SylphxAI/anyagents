//! Embedded PostgreSQL schema migrations applied in order by [`super::PgHandle::ensure_schema`].

pub const PG_INIT_SQL: &str = include_str!("../../migrations_pg/001_init.sql");
pub const PG_STATELESS_SQL: &str =
    include_str!("../../migrations_pg/002_stateless_coordinator.sql");
pub const PG_SERVERLESS_TURNS_SQL: &str =
    include_str!("../../migrations_pg/003_serverless_turns.sql");
pub const PG_DEPLOYMENT_SECRETS_SQL: &str =
    include_str!("../../migrations_pg/004_deployment_secrets.sql");
pub const PG_BROWSER_PROFILE_GRANTS_SQL: &str =
    include_str!("../../migrations_pg/005_browser_profile_grants.sql");
pub const PG_AGENT_RUNTIME_SQL: &str = include_str!("../../migrations_pg/006_agent_runtime.sql");
pub const PG_SCHEDULE_EXTERNAL_TIMER_SQL: &str =
    include_str!("../../migrations_pg/007_schedule_external_timer.sql");
pub const PG_ACTIVE_TURN_UNIQUE_SQL: &str =
    include_str!("../../migrations_pg/008_active_turn_unique.sql");
const PG_CHANNEL_IDENTITY_UNIQUE_SQL: &str =
    include_str!("../../migrations_pg/009_channel_identity_unique.sql");
pub const PG_TURN_TRANSCRIPT_EVENTS_SQL: &str =
    include_str!("../../migrations_pg/012_turn_transcript_events.sql");
pub const PG_LATEST_VERSION: i64 = PG_MIGRATIONS[PG_MIGRATIONS.len() - 1].0;

pub(super) const PG_MIGRATIONS: &[(i64, &str)] = &[
    (1, PG_INIT_SQL),
    (2, PG_STATELESS_SQL),
    (3, PG_SERVERLESS_TURNS_SQL),
    (4, PG_DEPLOYMENT_SECRETS_SQL),
    (5, PG_BROWSER_PROFILE_GRANTS_SQL),
    (6, PG_AGENT_RUNTIME_SQL),
    (7, PG_SCHEDULE_EXTERNAL_TIMER_SQL),
    (8, PG_ACTIVE_TURN_UNIQUE_SQL),
    (9, PG_CHANNEL_IDENTITY_UNIQUE_SQL),
    (10, include_str!("../../migrations_pg/010_knowledge.sql")),
    (
        11,
        include_str!("../../migrations_pg/011_runtime_issues.sql"),
    ),
    (12, PG_TURN_TRANSCRIPT_EVENTS_SQL),
    (
        13,
        include_str!("../../migrations_pg/013_ingress_blocks.sql"),
    ),
    (
        14,
        include_str!("../../migrations_pg/014_sandbox_targets.sql"),
    ),
    (
        15,
        include_str!("../../migrations_pg/015_archive_disposition.sql"),
    ),
    (
        16,
        include_str!("../../migrations_pg/016_repair_archive_disposition.sql"),
    ),
    (
        17,
        include_str!("../../migrations_pg/017_sandbox_takeover.sql"),
    ),
    (
        18,
        include_str!("../../migrations_pg/018_takeover_links.sql"),
    ),
    (
        19,
        include_str!("../../migrations_pg/019_turn_limit_pause.sql"),
    ),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::{SQLITE_LATEST_VERSION, SQLITE_MIGRATIONS};

    #[test]
    fn migration_versions_are_contiguous_for_both_backends() {
        for (migrations, latest) in [
            (PG_MIGRATIONS, PG_LATEST_VERSION),
            (SQLITE_MIGRATIONS, SQLITE_LATEST_VERSION),
        ] {
            assert!(
                migrations
                    .iter()
                    .map(|&(version, _)| version)
                    .eq(1..=latest)
            );
        }
    }
}
