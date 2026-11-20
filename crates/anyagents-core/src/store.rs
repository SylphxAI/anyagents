//! SQLite mailbox. A message is received when its insert commits, and that
//! commit assigns the next per-agent sequence. Source time is stored and never
//! used to order delivery. The delivery cursor moves only over a contiguous
//! prefix.

use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::{Connection, TransactionBehavior, params};

use crate::db::{DbBackend, DbExec, DbExecExt, DbParam, DbRow, DbTx, PgTx, SnapshotGuard};
use crate::error::Error;
use crate::id::{AgentId, ChannelId, MessageId};
#[cfg(test)]
use crate::migrate;
use crate::migrate::now_ms as clock_now_ms;

// Column lists as macros, so hot statements are `concat!` literals that
// `prepare_cached` reuses instead of a `format!` and a fresh prepare per call.
macro_rules! message_cols {
    () => {
        "id, agent_id, seq, source_time_ms, source_key, sender, body, \
         reply_to, received_at_ms, delivered_at_ms, origin, context"
    };
}
macro_rules! agent_cols {
    () => {
        "id, name, harness, model, workdir, instructions, state, \
         created_at_ms, launch_env, parent_id, is_admin, role, description, \
         runtime, remote_budget_micros"
    };
}
macro_rules! schedule_cols {
    () => {
        "id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms, \
         last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms"
    };
}

/// Rows of `agents` (bound as `?1`) that may scale to zero: running, no
/// running child, the last turn completed past every mailbox line, and every
/// line delivered. Shared by the read-only precheck and the guarded UPDATE.
macro_rules! idle_release_predicate {
    () => {
        "id = ?1
    AND state = 'running'
    AND NOT EXISTS (
        SELECT 1 FROM agents child
        WHERE child.parent_id = ?1
          AND child.state = 'running'
    )
    AND EXISTS (
        SELECT 1 FROM turn_recovery tr
        WHERE tr.agent_id = ?1
          AND tr.completed_seq >= tr.opened_seq
          AND tr.completed_seq > 0
          AND tr.completed_seq >= COALESCE((
              SELECT MAX(m.seq) FROM messages m WHERE m.agent_id = ?1
          ), 0)
    )
    AND COALESCE((
        SELECT mc.delivered_seq FROM mailbox_cursors mc WHERE mc.agent_id = ?1
    ), 0) >= COALESCE((
        SELECT MAX(m.seq) FROM messages m WHERE m.agent_id = ?1
    ), 0)"
    };
}

macro_rules! agent_summary_cols {
    () => {
        "id, name, harness, model, state, parent_id, role, is_admin, created_at_ms, \
         runtime, remote_budget_micros"
    };
}

#[cfg(test)]
#[path = "store/agent_runtime_tests.rs"]
mod agent_runtime_tests;
mod agents;
#[cfg(test)]
#[path = "store/audit_tests.rs"]
mod audit_tests;
mod capabilities;
mod channels;
mod credentials;
mod dedup;
mod group_policy;
mod groups;
mod harness_leases;
mod helpers;
mod history;
mod history_ops;
mod index_sync;
mod ingress;
mod lifecycle;
mod locks;
mod mail_notices;
mod mailbox;
mod mcp_ops;
pub mod memory_index;
mod operator_inbox;
mod origin;
mod pg_conversation;
mod registry;
mod remote_sessions;
mod retired_search;
mod runtime;
#[cfg(test)]
#[path = "store/runtime_tests.rs"]
mod runtime_tests;
#[cfg(test)]
#[path = "store/schedule_archive_tests.rs"]
mod schedule_archive_tests;
mod schedules;
mod server_ops;
mod sleep_memory;
mod steer_ops;
#[cfg(test)]
mod tests;
mod topology;
mod turn_errors;
mod turn_events;
mod types;
mod usage;

pub use agents::{
    DEFAULT_ARCHIVE_ROSTER_LIMIT, RosterPageQuery, agent_config_dir, agent_lock_path,
    agent_work_dir, legacy_agent_work_dir, parse_agent_lifecycle, parse_agent_scope,
    read_children_roster, read_root_roster,
};
pub use credentials::{
    CredentialBinding, CredentialBindingAudit, CredentialKind, CredentialObservation,
    CredentialSlot, CredentialTarget,
};
pub use group_policy::{Group, GroupAction, GroupActor};
pub use groups::GroupSummary;
pub use harness_leases::HarnessLease;
pub use index_sync::{clean_rag_text, rag_content_hash, tokenize_hybrid_search_tokens};
pub use lifecycle::{LifecycleKind, LifecycleOp, LifecyclePhase, NewLifecycleOp, SchedulePending};
pub use locks::AgentLock;
pub use mail_notices::SYSTEM_SENDER;
pub use operator_inbox::{
    OPERATOR_BURST, OPERATOR_FLEET_BURST, OPERATOR_FLEET_HOURLY_CAP, OPERATOR_HOURLY_CAP,
    OperatorMessage, OperatorReceipt, OperatorReport,
};
pub(crate) use origin::origin_namespace;
pub(crate) use pg_conversation::ensure_pg_conversation_indexes;
pub use registry::holds_store_writer;
pub use remote_sessions::{
    REMOTE_SESSION_TOKEN_PREFIX, RemoteSession, RemoteSessionGrant, RemoteSessionToken,
    is_remote_session_token,
};
pub use runtime::{
    AgentRuntime, BackfillReport, Fault, FaultKind, ProcessIdentity, QUOTA_ESTIMATE_MS, QuotaBlock,
    QuotaReset, QuotaSource, RecoveryOffer, RuntimeChange, RuntimeHeartbeat, RuntimeState,
    SessionLease, Turn, TurnOutcome, TurnState,
};
pub use turn_errors::{ErroredTurn, MAX_ERRORED_TURNS, TurnErrors, is_session_turn_error};
pub use turn_events::AgentTurnSnapshot;
pub use types::{
    Ack, Agent, AgentQueryMetadata, AgentSchedule, AgentSummary, BillingMode, Channel,
    ConversationTurn, HistorySearchQuery, HistorySort, Incoming, Lag, MAX_RECOVERY_ATTEMPTS,
    Message, NewAgent, NewChannel, NewSchedule, NewUsage, OpenTurn, Origin, OutboundEffect,
    ReceiveOutcome, RejectedMessage, TurnOwnershipSnapshot, UsageRecord, Wake, WalCheckpoint,
};
pub use usage::{
    UsageAccessError, UsageBucket, UsageFilter, UsageGroupBy, UsageReport, UsageTotal, UsageTotals,
    aggregate_usage, resolve_usage_agents, sum_usage, utc_day,
};

// Re-exports kept at `store` root for existing sibling submodules (`use super::...`).
use agents::scope_env;
use channels::{agents_of, channel_from_row_db, telegram_chat_links};
use helpers::{agent_by_id, agent_exists, data_dir_for, encode_env, hash, legacy_last_error};
use index_sync::classify_chunk_kind_and_tool;
use ingress::{IngressHistory, ReceiveTx};
use mailbox::{cursor, last_seq, message_at_on};
use schedules::cancel_archived_schedules;
use topology::xml_escape_attr_local;

const MESSAGE_COLS: &str = message_cols!();

/// Milliseconds since the Unix epoch. A clock that is before the epoch reads as 0.
pub fn now_ms() -> i64 {
    clock_now_ms()
}

/// A handle on an open database. Every handle a process opens on one file
/// shares that file's single read-write connection, read pool, and in-process
/// wake subscribers (see `registry`). SQLite writes serialize on the writer's mutex,
/// while PostgreSQL writes execute concurrently via `sqlx::PgPool`.
/// SQLite file-backed reads use independent read-only connections; when a reader
/// cannot open, or for an in-memory database, reads use the authoritative writer
/// and may wait for its mutex. The capability key belongs to the handle.
pub struct Store {
    backend: registry::Backend,
    db_path: PathBuf,
    data_dir: PathBuf,
    capabilities: Mutex<Option<String>>,
    replica_id: String,
}

fn default_replica_id() -> String {
    static REPLICA_ID: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        if let Ok(explicit) = std::env::var("ANYAGENTS_REPLICA_ID") {
            let trimmed = explicit.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
        let host = std::env::var("HOSTNAME")
            .ok()
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "replica".to_string());
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        format!("{host}-{}-{suffix}", std::process::id())
    });
    REPLICA_ID.clone()
}

/// Default per-statement wait for the writer lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);
/// Default total budget for writes that must not fail on a transient lock:
/// several busy-timeout rounds, after which the error is surfaced.
const WRITE_RETRY_BUDGET: Duration = Duration::from_secs(120);

/// Size the WAL file is truncated back to after a checkpoint reset.
const WAL_SIZE_LIMIT_BYTES: i64 = 64 * 1024 * 1024;

/// `true` for SQLite lock contention that a later attempt can clear.
fn is_transient_lock_error(error: &Error) -> bool {
    error.is_transient_lock_error()
}

/// Human-readable check for a rusqlite error without wrapping.
fn is_transient_sqlite_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(e, _)
            if matches!(e.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

/// Idle read-only connections kept for reuse. Opening a connection per read
/// discards SQLite's page cache and re-reads the schema every time; on a
/// multi-GB store that turned every read into a burst of `pread` syscalls and
/// pinned the daemon in the kernel. Readers beyond this many simply close.
const MAX_IDLE_READERS: usize = 8;
/// Use the bounded page cache instead of retaining resident file mappings
/// across pooled connections. The OS still caches ordinary file reads.
const MMAP_SIZE_BYTES: i64 = 0;
/// Per-schema page cache, in KiB (negative `cache_size` units).
const CACHE_SIZE_KIB: i64 = 2 * 1024;
/// Prepared statements kept per connection; read paths reuse a few dozen.
const STATEMENT_CACHE: usize = 128;

/// Apply resource bounds to every store/index connection. Do not admit a
/// connection whose pager settings could not be established.
pub(crate) fn tune_connection(conn: &Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "mmap_size", MMAP_SIZE_BYTES)?;
    conn.pragma_update(None, "cache_size", -CACHE_SIZE_KIB)?;
    conn.pragma_update(None, "temp_store", "FILE")?;
    conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE);
    Ok(())
}

/// Open one read-only connection for the reader pool.
fn open_reader(path: &Path) -> Option<Connection> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    let _ = conn.busy_timeout(BUSY_TIMEOUT);
    let _ = conn.pragma_update(None, "query_only", "ON");
    tune_connection(&conn).ok()?;
    // The mailbox search index, read-only like the rest of this connection:
    // `rag_chunks` resolves to it, as state has no table of that name. A
    // reader without it is not a reader of this store; none is pooled.
    attach_memory_index(&conn, path).ok()?;
    Some(conn)
}

/// Attach the search index of the state database at `path` to `conn`.
fn attach_memory_index(conn: &Connection, path: &Path) -> Result<(), Error> {
    let index = memory_index::path_for(path);
    if index.as_os_str().is_empty() {
        return Err(Error::Message(
            "a private in-memory store's search index is not attachable".into(),
        ));
    }
    conn.execute(
        &format!("ATTACH DATABASE ?1 AS {}", memory_index::ATTACHED),
        [index.as_os_str().to_string_lossy()],
    )?;
    // Pager settings are per schema: tuning main does not tune an attachment.
    let schema = Some(memory_index::ATTACHED);
    conn.pragma_update(schema, "mmap_size", MMAP_SIZE_BYTES)?;
    conn.pragma_update(schema, "cache_size", -CACHE_SIZE_KIB)?;
    Ok(())
}

/// A pooled read-only connection, with the existing writer as a fallback.
pub(crate) enum ReadConn<'a> {
    /// A reader borrowed from the pool; returned on drop.
    Pooled {
        conn: Option<Connection>,
        pool: Arc<Mutex<Vec<Connection>>>,
    },
    Owned(Option<Connection>),
    /// The writer, when no reader opens; `true` while it has the search
    /// index attached for this borrow (detached on drop).
    Guard(registry::WriterGuard<'a>, bool),
    /// A shared PostgreSQL connection pool handle.
    Postgres(sqlx::PgPool),
}

impl ReadConn<'_> {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn as_sqlite(&self) -> &Connection {
        match self {
            Self::Pooled { conn, .. } => conn.as_ref().expect("pooled reader present until drop"),
            Self::Owned(conn) => conn.as_ref().expect("owned reader present until drop"),
            Self::Guard(g, _) => g,
            Self::Postgres(_) => panic!("expected SQLite ReadConn, found Postgres"),
        }
    }

    pub(crate) fn snapshot(&self) -> Result<SnapshotGuard<'_>, Error> {
        match self {
            Self::Pooled { conn, .. } => Ok(SnapshotGuard::Sqlite(
                conn.as_ref()
                    .expect("pooled reader present until drop")
                    .unchecked_transaction()?,
            )),
            Self::Owned(conn) => Ok(SnapshotGuard::Sqlite(
                conn.as_ref()
                    .expect("owned reader present until drop")
                    .unchecked_transaction()?,
            )),
            Self::Guard(g, _) => Ok(SnapshotGuard::Sqlite(g.unchecked_transaction()?)),
            Self::Postgres(pool) => Ok(SnapshotGuard::Postgres(PgTx::begin_read_only(pool)?)),
        }
    }
}

impl DbExec for ReadConn<'_> {
    fn backend(&self) -> DbBackend {
        match self {
            Self::Pooled { .. } | Self::Owned(_) | Self::Guard(_, _) => DbBackend::Sqlite,
            Self::Postgres(_) => DbBackend::Postgres,
        }
    }

    fn execute_raw(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
        match self {
            Self::Pooled { conn, .. } => conn
                .as_ref()
                .expect("pooled reader present until drop")
                .execute_raw(sql, params),
            Self::Owned(conn) => conn
                .as_ref()
                .expect("owned reader present until drop")
                .execute_raw(sql, params),
            Self::Guard(g, _) => g.execute_raw(sql, params),
            Self::Postgres(pool) => pool.execute_raw(sql, params),
        }
    }

    fn execute_batch_raw(&self, sql: &str) -> Result<(), Error> {
        match self {
            Self::Pooled { conn, .. } => conn
                .as_ref()
                .expect("pooled reader present until drop")
                .execute_batch_raw(sql),
            Self::Owned(conn) => conn
                .as_ref()
                .expect("owned reader present until drop")
                .execute_batch_raw(sql),
            Self::Guard(g, _) => g.execute_batch_raw(sql),
            Self::Postgres(pool) => pool.execute_batch_raw(sql),
        }
    }

    fn for_each_row_raw(
        &self,
        sql: &str,
        params: &[DbParam],
        f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        match self {
            Self::Pooled { conn, .. } => conn
                .as_ref()
                .expect("pooled reader present until drop")
                .for_each_row_raw(sql, params, f),
            Self::Owned(conn) => conn
                .as_ref()
                .expect("owned reader present until drop")
                .for_each_row_raw(sql, params, f),
            Self::Guard(g, _) => g.for_each_row_raw(sql, params, f),
            Self::Postgres(pool) => pool.for_each_row_raw(sql, params, f),
        }
    }
}

impl Drop for ReadConn<'_> {
    fn drop(&mut self) {
        if let Self::Pooled { conn, pool } = self
            && let Some(conn) = conn.take()
        {
            registry::Shared::recycle(pool, conn);
        }
        if let Self::Guard(writer, attached) = self
            && *attached
        {
            let _ = writer.execute_batch(&format!("DETACH DATABASE {}", memory_index::ATTACHED));
        }
    }
}

/// Freeze the old effective policy before removing implicit role/name defaults.
/// New rows always have an explicit scope. This bounded, transactional upgrade
/// touches only legacy environment metadata, never identities or work state.
fn preserve_legacy_scopes(conn: &mut Connection) -> Result<(), Error> {
    let missing_scope_sql = "SELECT EXISTS(SELECT 1 FROM agents WHERE NOT EXISTS (
                SELECT 1 FROM json_each(CASE WHEN json_valid(launch_env) THEN launch_env ELSE '[]' END) entry
                WHERE entry.key % 2 = 0 AND entry.value = 'ANYAGENTS_SCOPE'
                  AND lower(trim(json_extract(launch_env, '$[' || (entry.key + 1) || ']'))) IN (
                    'isolated','none','private','false','0','siblings','sibling','team',
                    'public','org','all','true','1')))";
    if !conn.query_row(missing_scope_sql, [], |r| r.get::<_, bool>(0))? {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let ids: Vec<String> = {
        let mut stmt = tx.prepare(
            "SELECT id FROM agents WHERE NOT EXISTS (
                SELECT 1 FROM json_each(CASE WHEN json_valid(launch_env) THEN launch_env ELSE '[]' END) entry
                WHERE entry.key % 2 = 0 AND entry.value = 'ANYAGENTS_SCOPE'
                  AND lower(trim(json_extract(launch_env, '$[' || (entry.key + 1) || ']'))) IN (
                    'isolated','none','private','false','0','siblings','sibling','team',
                    'public','org','all','true','1'))",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?
    };
    for id in ids {
        let agent = agent_by_id(&tx, &AgentId::parse(&id)?)?;
        // Compatibility only: role and name never select policy for new agents.
        let legacy_default = if agent.parent_id.is_none() {
            "public"
        } else if agent.name.ends_with("-assistant") {
            "isolated"
        } else if agent.role.as_deref().is_some_and(|r| !r.trim().is_empty()) {
            "siblings"
        } else {
            "private"
        };
        let scope = agent.scope_or_default(legacy_default);
        tx.execute(
            "UPDATE agents SET launch_env = ?1 WHERE id = ?2",
            params![encode_env(&scope_env(&agent, scope)), id],
        )?;
    }
    tx.commit()?;
    Ok(())
}

impl Store {
    /// A handle on the store: SQLite at `path` by default, or PostgreSQL when
    /// `ANYAGENTS_DATABASE_URL` / `--database-url` / `<data_dir>/database-url`
    /// is configured.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let data_dir = data_dir_for(path);
        if let Some(url) = crate::db::configured_database_url_for_data_dir(&data_dir) {
            return Self::open_postgres_with_paths(&url, path, &data_dir);
        }
        Ok(Self {
            backend: registry::Backend::Sqlite(registry::shared(path)?),
            db_path: path.to_path_buf(),
            data_dir,
            capabilities: Mutex::new(None),
            replica_id: default_replica_id(),
        })
    }

    /// Explicitly open the store backed by PostgreSQL at `url`, keeping local
    /// workspace/config artifacts anchored under `data_dir`.
    pub fn open_postgres(url: &str, data_dir: impl AsRef<Path>) -> Result<Self, Error> {
        let data_dir = data_dir.as_ref();
        Self::open_postgres_with_paths(url, &data_dir.join("state.db"), data_dir)
    }

    /// Open a PostgreSQL-backed store with its own isolated in-process wake
    /// bus, `LISTEN` connection, and explicit `replica_id`.
    ///
    /// Useful for multi-replica coordinator integration tests simulating
    /// independent pods sharing one PostgreSQL database.
    pub fn open_postgres_isolated(
        url: &str,
        data_dir: impl AsRef<Path>,
        replica_id: impl Into<String>,
    ) -> Result<Self, Error> {
        let data_dir = data_dir.as_ref();
        let replica_id = replica_id.into();
        let pg = registry::isolated_postgres(url, data_dir, &replica_id)?;
        Ok(Self {
            backend: registry::Backend::Postgres(pg),
            db_path: data_dir.join("state.db"),
            data_dir: data_dir.to_path_buf(),
            capabilities: Mutex::new(None),
            replica_id,
        })
    }

    fn open_postgres_with_paths(url: &str, db_path: &Path, data_dir: &Path) -> Result<Self, Error> {
        let replica_id = default_replica_id();
        let pg = registry::shared_postgres(url, data_dir, &replica_id)?;
        Ok(Self {
            backend: registry::Backend::Postgres(pg),
            db_path: db_path.to_path_buf(),
            data_dir: data_dir.to_path_buf(),
            capabilities: Mutex::new(None),
            replica_id,
        })
    }

    pub fn replica_id(&self) -> &str {
        &self.replica_id
    }

    pub fn pg_pool(&self) -> Option<sqlx::PgPool> {
        match &self.backend {
            registry::Backend::Postgres(pg) => Some(pg.handle.pool.clone()),
            registry::Backend::Sqlite(_) => None,
        }
    }

    /// Wait up to `timeout` for the background PostgreSQL `LISTEN anyagents_wakes`
    /// subscriber to become active. Returns `true` immediately on SQLite.
    pub fn wait_pg_wake_listener_ready(&self, timeout: Duration) -> bool {
        match &self.backend {
            registry::Backend::Postgres(pg) => pg.wait_wake_listener_ready(timeout),
            registry::Backend::Sqlite(_) => true,
        }
    }

    /// State blob store (`agent_files` + `deployment_blobs` on PostgreSQL;
    /// local `<data_dir>` filesystem on SQLite).
    pub fn blob_store(&self) -> Box<dyn crate::stateless::StateBlobStore> {
        self.blob_store_with_key(None)
    }

    pub fn blob_store_with_key(
        &self,
        master_key: Option<crate::stateless::MasterKey>,
    ) -> Box<dyn crate::stateless::StateBlobStore> {
        match &self.backend {
            registry::Backend::Postgres(pg) => Box::new(crate::stateless::PgStateBlobStore::new(
                pg.handle.pool.clone(),
                master_key,
            )),
            registry::Backend::Sqlite(_) => {
                Box::new(crate::stateless::FsStateBlobStore::new(&self.data_dir))
            }
        }
    }

    /// Distributed coordinator lock backend (`public.coordinator_leases` on
    /// PostgreSQL; local `flock` files on SQLite).
    pub fn lock_backend(&self) -> Box<dyn crate::stateless::CoordinatorLockBackend> {
        match &self.backend {
            registry::Backend::Postgres(pg) => {
                Box::new(crate::stateless::PgCoordinatorLockBackend::new(
                    pg.handle.pool.clone(),
                    self.replica_id.clone(),
                ))
            }
            registry::Backend::Sqlite(_) => Box::new(crate::stateless::LocalFlockLockBackend::new(
                &self.data_dir,
                self.replica_id.clone(),
            )),
        }
    }

    /// Try to acquire a distributed PostgreSQL coordinator lease with automatic
    /// background heartbeat renewal. Returns `Ok(None)` when another replica
    /// currently holds an unexpired lease (or when backed by SQLite).
    pub fn try_acquire_coordinator_lease(
        &self,
        lease_key: &str,
        ttl: Duration,
    ) -> Result<Option<crate::stateless::CoordinatorLeaseGuard>, Error> {
        let Some(pool) = self.pg_pool() else {
            return Ok(None);
        };
        let backend =
            crate::stateless::PgCoordinatorLockBackend::new(pool, self.replica_id.clone());
        backend.try_acquire_guard(lease_key, ttl)
    }

    /// PostgreSQL protocol gateway route store (`None` on SQLite).
    pub fn gateway_route_store(&self) -> Option<crate::stateless::PgGatewayRouteStore> {
        self.gateway_route_store_with_key(None)
    }

    pub fn gateway_route_store_with_key(
        &self,
        master_key: Option<crate::stateless::MasterKey>,
    ) -> Option<crate::stateless::PgGatewayRouteStore> {
        self.pg_pool()
            .map(|pool| crate::stateless::PgGatewayRouteStore::new(pool, master_key))
    }

    /// Durable sandbox binding store (`public.agent_sandbox_bindings` on
    /// PostgreSQL; local config JSON on SQLite).
    pub fn sandbox_binding_store(&self) -> Box<dyn crate::stateless::SandboxBindingStore> {
        self.sandbox_binding_store_with_key(None)
    }

    pub fn sandbox_binding_store_with_key(
        &self,
        master_key: Option<crate::stateless::MasterKey>,
    ) -> Box<dyn crate::stateless::SandboxBindingStore> {
        match &self.backend {
            registry::Backend::Postgres(pg) => Box::new(
                crate::stateless::PgSandboxBindingStore::new(pg.handle.pool.clone(), master_key),
            ),
            registry::Backend::Sqlite(_) => {
                Box::new(crate::stateless::FsSandboxBindingStore::new(&self.data_dir))
            }
        }
    }

    /// Convenience lookup for one agent's persisted sandbox binding.
    pub fn sandbox_binding(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<crate::stateless::SandboxBinding>, Error> {
        self.sandbox_binding_store().get(agent_id)
    }

    /// Durable obligation store (`public.durable_obligations` on PostgreSQL;
    /// local `<data_dir>/obligations/` on SQLite).
    pub fn durable_obligation_store(&self) -> Box<dyn crate::stateless::DurableObligationStore> {
        match &self.backend {
            registry::Backend::Postgres(pg) => Box::new(
                crate::stateless::PgDurableObligationStore::new(pg.handle.pool.clone()),
            ),
            registry::Backend::Sqlite(_) => Box::new(
                crate::stateless::FsDurableObligationStore::new(&self.data_dir),
            ),
        }
    }

    /// Durable serverless remote turn ledger (`public.pending_remote_turns` on PostgreSQL;
    /// local `<data_dir>/remote-turns/` on SQLite).
    pub fn remote_turn_store(&self) -> Box<dyn crate::stateless::RemoteTurnStore> {
        match &self.backend {
            registry::Backend::Postgres(pg) => Box::new(crate::stateless::PgRemoteTurnStore::new(
                pg.handle.pool.clone(),
            )),
            registry::Backend::Sqlite(_) => {
                Box::new(crate::stateless::FsRemoteTurnStore::new(&self.data_dir))
            }
        }
    }

    pub fn backend_kind(&self) -> DbBackend {
        match &self.backend {
            registry::Backend::Sqlite(_) => DbBackend::Sqlite,
            registry::Backend::Postgres(_) => DbBackend::Postgres,
        }
    }

    pub fn is_postgres(&self) -> bool {
        matches!(self.backend, registry::Backend::Postgres(_))
    }

    fn sqlite_shared(&self) -> &Arc<registry::Shared> {
        match &self.backend {
            registry::Backend::Sqlite(s) => s,
            registry::Backend::Postgres(_) => panic!("expected SQLite Store backend"),
        }
    }

    fn memory_index(&self) -> &Arc<memory_index::MemoryIndex> {
        match &self.backend {
            registry::Backend::Sqlite(s) => &s.memory_index,
            registry::Backend::Postgres(p) => &p.memory_index,
        }
    }

    fn wakes(&self) -> &Mutex<Vec<Sender<Wake>>> {
        match &self.backend {
            registry::Backend::Sqlite(s) => s.wakes(),
            registry::Backend::Postgres(p) => p.wakes(),
        }
    }

    fn changes_sender(&self) -> &tokio::sync::watch::Sender<u64> {
        match &self.backend {
            registry::Backend::Sqlite(s) => &s.changes,
            registry::Backend::Postgres(p) => &p.changes,
        }
    }

    /// Run a SQLite `wal_checkpoint(TRUNCATE)` on `state.db`, or return `None`
    /// when backed by PostgreSQL (no local SQLite WAL exists).
    pub(crate) fn sqlite_wal_checkpoint_truncate(&self) -> Option<Result<(i64, i64, i64), Error>> {
        match &self.backend {
            registry::Backend::Sqlite(shared) => {
                let conn = shared.writer();
                let _ = conn.busy_timeout(Duration::ZERO);
                let res = conn
                    .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, i64>(2)?,
                        ))
                    })
                    .map_err(Error::from);
                let _ = conn.busy_timeout(self.busy_timeout());
                Some(res)
            }
            registry::Backend::Postgres(_) => None,
        }
    }

    /// Set the writer's per-statement lock wait and the total budget that
    /// retried writes may spend on transient lock errors. The writer is shared,
    /// so this applies to every handle this process has on the database.
    pub fn set_lock_wait(
        &self,
        busy_timeout: Duration,
        retry_budget: Duration,
    ) -> Result<(), Error> {
        match &self.backend {
            registry::Backend::Sqlite(shared) => {
                shared.writer().busy_timeout(busy_timeout)?;
                shared
                    .busy_timeout_ms
                    .store(busy_timeout.as_millis() as u64, Ordering::Relaxed);
                shared
                    .write_retry_budget_ms
                    .store(retry_budget.as_millis() as u64, Ordering::Relaxed);
            }
            registry::Backend::Postgres(pg) => {
                pg.write_retry_budget_ms
                    .store(retry_budget.as_millis() as u64, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Copy committed WAL frames back into the database without ever making
    /// readers or writers wait. A passive pass backfills what no reader still
    /// needs; only when that pass reaches the end of the log is a TRUNCATE
    /// attempted, with no busy wait, so a concurrent reader or writer makes it
    /// a cheap no-op instead of a stall. Call it periodically from one place.
    pub fn checkpoint_wal(&self) -> Result<WalCheckpoint, Error> {
        let registry::Backend::Sqlite(shared) = &self.backend else {
            return Ok(WalCheckpoint {
                log_frames: 0,
                checkpointed_frames: 0,
                passive_busy: false,
                truncated: true,
            });
        };
        let conn = shared.writer();
        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
        let mut outcome = WalCheckpoint {
            log_frames,
            checkpointed_frames,
            passive_busy: busy != 0,
            truncated: false,
        };
        if busy == 0 && log_frames >= 0 && checkpointed_frames == log_frames {
            conn.busy_timeout(Duration::ZERO)?;
            let truncated = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            });
            let restored = conn.busy_timeout(Duration::from_millis(
                shared.busy_timeout_ms.load(Ordering::Relaxed),
            ));
            match truncated.map_err(Error::from) {
                Ok((0, 0)) => outcome.truncated = true,
                Ok(_) => {}
                Err(error) if is_transient_lock_error(&error) => {}
                Err(error) => return Err(error),
            }
            restored?;
        }
        Ok(outcome)
    }

    pub(crate) fn busy_timeout(&self) -> Duration {
        match &self.backend {
            registry::Backend::Sqlite(shared) => {
                Duration::from_millis(shared.busy_timeout_ms.load(Ordering::Relaxed))
            }
            registry::Backend::Postgres(_) => BUSY_TIMEOUT,
        }
    }

    /// Run one transaction (`BEGIN IMMEDIATE` on SQLite, pooled PostgreSQL tx
    /// on Postgres), retrying transient lock/serialization errors within budget.
    pub(crate) fn write_tx<T>(
        &self,
        op: impl FnOnce(&DbTx<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let budget = Duration::from_millis(match &self.backend {
            registry::Backend::Sqlite(shared) => {
                shared.write_retry_budget_ms.load(Ordering::Relaxed)
            }
            registry::Backend::Postgres(pg) => pg.write_retry_budget_ms.load(Ordering::Relaxed),
        });
        let started = std::time::Instant::now();
        let mut backoff = Duration::from_millis(10);
        let mut op = Some(op);
        match &self.backend {
            registry::Backend::Sqlite(shared) => loop {
                // End the transaction-start temporary and writer guard's scope
                // before backing off. Never replay a closure with external effects.
                let error = {
                    let mut conn = shared.writer();
                    let before = conn.total_changes();
                    let attempt = conn.transaction_with_behavior(TransactionBehavior::Immediate);
                    match attempt {
                        Ok(tx) => {
                            let wrapped = DbTx::Sqlite(&tx);
                            let result =
                                (op.take().expect("write_tx closure called once"))(&wrapped)?;
                            let changed = tx.total_changes() != before;
                            tx.commit()?;
                            if changed {
                                self.poke_changes();
                                self.notify_changes();
                            }
                            return Ok(result);
                        }
                        Err(err) => Error::from(err),
                    }
                };
                let elapsed = started.elapsed();
                if !is_transient_lock_error(&error) || elapsed >= budget {
                    return Err(error);
                }
                std::thread::sleep(backoff.min(budget - elapsed));
                backoff = (backoff * 2).min(Duration::from_secs(1));
            },
            registry::Backend::Postgres(pg) => {
                let pg_tx = PgTx::begin(&pg.handle.pool)?;
                let wrapped = DbTx::Postgres(pg_tx);
                let result = (op.take().expect("write_tx closure called once"))(&wrapped)?;
                let changed = match wrapped {
                    DbTx::Postgres(tx) => {
                        let wrote = tx.wrote();
                        tx.commit()?;
                        wrote
                    }
                    DbTx::Sqlite(_) => false,
                };
                if changed {
                    self.poke_changes();
                    self.notify_changes();
                }
                Ok(result)
            }
        }
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.db_path
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// A channel for later mailbox inserts and running transitions.
    ///
    /// Subscribe before scanning durable state. Generations are coalesced hints,
    /// never proof that any particular predicate became true.
    pub fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        if let registry::Backend::Postgres(pg) = &self.backend {
            pg.ensure_wake_listener();
        }
        self.changes_sender().subscribe()
    }

    /// Import a cross-process state invalidation without starting any worker.
    pub fn poke_changes(&self) {
        self.changes_sender()
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    fn notify_changes(&self) {
        if let registry::Backend::Postgres(pg) = &self.backend {
            let _ =
                crate::stateless::publish_pg_wake(&pg.handle.pool, Some(&self.replica_id), None);
            return;
        }
        let path = self.data_dir().join("wake.sock");
        if path.as_os_str().len() > 107 {
            return;
        }
        if let Ok(socket) = UnixDatagram::unbound() {
            let _ = socket.set_nonblocking(true);
            let _ = socket.send_to(b"s", path);
        }
    }

    /// Subscribe to mailbox/agent hints; committed events are not replayed.
    pub fn subscribe(&self) -> Receiver<Wake> {
        if let registry::Backend::Postgres(pg) = &self.backend {
            pg.ensure_wake_listener();
        }
        let (sender, receiver) = mpsc::channel();
        self.wakes()
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(sender);
        receiver
    }

    /// Drop every subscriber so a blocked `recv` returns.
    pub fn close_wakes(&self) {
        self.wakes()
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clear();
    }

    /// Fan a wake to in-process subscribers. Does not touch the datagram socket.
    pub fn poke(&self, wake: Wake) {
        self.fanout(&wake);
    }

    fn notify(&self, wake: Wake) {
        if let registry::Backend::Postgres(pg) = &self.backend {
            let _ = crate::stateless::publish_pg_wake(
                &pg.handle.pool,
                Some(&self.replica_id),
                Some(&wake),
            );
        } else {
            self.send_datagram(&wake);
        }
        self.fanout(&wake);
    }

    fn fanout(&self, wake: &Wake) {
        let mut wakes = self.wakes().lock().unwrap_or_else(|err| err.into_inner());
        wakes.retain(|sender| sender.send(wake.clone()).is_ok());
    }

    fn send_datagram(&self, wake: &Wake) {
        let (tag, id) = match wake {
            Wake::Mail { agent_id } => (b'm', agent_id.as_str()),
            Wake::Agent { agent_id } => (b'a', agent_id.as_str()),
        };
        let path = self.data_dir.join("wake.sock");
        if path.as_os_str().len() > 107 {
            return;
        }
        let mut payload = Vec::with_capacity(1 + id.len());
        payload.push(tag);
        payload.extend_from_slice(id.as_bytes());
        if let Ok(socket) = UnixDatagram::unbound() {
            let _ = socket.send_to(&payload, &path);
        }
    }

    /// The process's one read-write connection to this database. Use it only
    /// to write: reads go through [`Store::read_conn`] and never wait here.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn conn(&self) -> registry::WriterGuard<'_> {
        self.sqlite_shared().writer()
    }

    /// A read-only connection from the shared pool. Each statement outside an
    /// explicit transaction reads the latest commit, including this process's
    /// own writes. Falls back to the writer only when no reader can be opened.
    pub(crate) fn read_conn(&self) -> ReadConn<'_> {
        match &self.backend {
            registry::Backend::Sqlite(shared) => {
                if let Some(reader) = shared.reader() {
                    return reader;
                }
                // Empty-path databases cannot share a reader. If a file-backed reader
                // cannot open, use the authoritative connection rather than fabricate
                // an empty database that could turn a failed read into missing state.
                // The writer never keeps the index attached (a write transaction
                // would lock both files); it has it only for this read.
                let writer = shared.writer();
                let attached = attach_memory_index(&writer, self.database_path()).is_ok();
                ReadConn::Guard(writer, attached)
            }
            registry::Backend::Postgres(pg) => ReadConn::Postgres(pg.handle.pool.clone()),
        }
    }
}
