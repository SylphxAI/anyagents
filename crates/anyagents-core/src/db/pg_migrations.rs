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
pub const PG_LATEST_VERSION: i64 = 11;

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
];
