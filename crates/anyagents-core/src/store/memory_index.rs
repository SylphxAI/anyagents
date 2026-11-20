//! The mailbox search index, in its own file beside the state database.
//!
//! `rag_chunks` (one row per searchable mailbox turn) and `rag_chunks_fts`
//! (its FTS5 index) are a projection of `messages` and `outbound_messages`.
//! They live in `memory-index.db`, never in `state.db`: projecting,
//! rebuilding, renaming and redacting index rows lock only this file, so no
//! index work ever holds the state writer, and the state file holds only
//! operational rows. State readers attach this file read-only as
//! [`ATTACHED`], so search SQL reads `rag_chunks` and joins it with the
//! mailbox as before.
//!
//! The projection is exact and rebuildable:
//!
//! - New rows are found by rowid marks over `messages` and
//!   `outbound_messages`. SQLite commits rowids in order, so every row past a
//!   mark is unprojected and every row at or below it was projected, except
//!   where a deleted agent's rows held the highest rowids: deleting an agent
//!   lowers the marks to the highest rowid left after the delete.
//! - A change to a projected row (redaction, an edit, a rename, a deletion)
//!   first records a durable pending entry here, then changes state, then
//!   re-projects the entry from state. Re-projecting reads whatever state
//!   committed, so an interruption at any point is repaired by the next drain.
//! - A missing, damaged or older-layout file is recreated empty and refilled
//!   from state in bounded batches, each in its own short transaction.
//!
//! Standalone tool summaries (`record_tool_event`, chunk ids `tool_*`) have no
//! mailbox row; they are kept here as recorded and are the only rows a rebuild
//! cannot reproduce.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior};

use super::{clean_rag_text, rag_content_hash, tune_connection};
use crate::db::{DbBackend, DbExec, DbExecExt, DbParam, DbTx, PgTx};
use crate::db_params;
use crate::error::Error;

mod postgres;
mod projection;

use postgres::sync_on_postgres;
#[cfg(test)]
pub(super) use projection::{
    FTS_DELETE_AGENT_ROWS, FTS_DELETE_CHUNK, fts_chunk_lookup, fts_exact_phrase,
};
pub(super) use projection::{remove_chunk, rewrite_agent_fts_rows, upsert_chunk};

/// The index file beside a state database named `state.db`.
pub const FILE_NAME: &str = "memory-index.db";
/// The schema name state readers attach the index under.
pub(crate) const ATTACHED: &str = "memory_index";
/// Layout version; a file with any other version is rebuilt from state.
// Kana and Hangul now have the same auxiliary tokens as Han. Existing
// SQLite indexes need a rebuild to index them in older mailbox rows.
const LAYOUT: i64 = 3;
/// Rows projected per index transaction.
const BATCH: usize = 512;
/// Rows a reader projects before answering; the rest is left to the
/// background rebuild.
pub(super) const READ_BUDGET: usize = 4 * BATCH;
/// How long a blocking index write waits for another process's.
const BUSY: Duration = Duration::from_secs(30);
/// Recent mailbox rows checked against the index on open, repairing a
/// projection an interrupted writer did not finish.
const TAIL_CHECK: i64 = 1024;

/// `memory-index.db` for `state.db`; `<file>.memory-index.db` for any other
/// state file, so two stores in one directory never share an index.
pub fn path_for(state: &Path) -> PathBuf {
    if state.as_os_str().is_empty() {
        return PathBuf::new();
    }
    match state.file_name().and_then(|n| n.to_str()) {
        Some("state.db") | None => state.with_file_name(FILE_NAME),
        Some(name) => state.with_file_name(format!("{name}.{FILE_NAME}")),
    }
}

const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS index_meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS index_pending (
    kind TEXT NOT NULL,
    id TEXT NOT NULL,
    -- For an agent about to be deleted: the highest rowids of every other
    -- agent's rows, read before the delete. Rows inserted after the delete
    -- lie above them.
    cap_messages INTEGER,
    cap_outbound INTEGER,
    PRIMARY KEY (kind, id)
);
-- A standalone summary's redaction, written with its scrubbed text before
-- state commits the audit: a retry that finds no audit completes it from
-- this receipt instead of redacting already-scrubbed text again.
CREATE TABLE IF NOT EXISTS index_redactions (
    chunk_id TEXT PRIMARY KEY,
    request_key TEXT NOT NULL,
    receipt TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rag_chunks (
    chunk_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    direction TEXT NOT NULL,
    sender TEXT NOT NULL,
    destination TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    text TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'message',
    tool_name TEXT NOT NULL DEFAULT '',
    seq INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_time ON rag_chunks(agent_name, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_time ON rag_chunks(timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_hash ON rag_chunks(content_hash);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_seq ON rag_chunks(agent_name, seq, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_kind_tool ON rag_chunks(kind, tool_name, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_time_seq ON rag_chunks(timestamp_ms DESC, seq DESC);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_kind_time ON rag_chunks(LOWER(agent_name), kind, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_id_time ON rag_chunks(agent_id, timestamp_ms DESC, chunk_id);
CREATE VIRTUAL TABLE IF NOT EXISTS rag_chunks_fts USING fts5(
    chunk_id, agent_name, sender, destination, kind, tool_name, text, search_tokens,
    tokenize='unicode61'
);
";

/// The rowids of the chunks in state's own (pre-split) `rag_chunks` that no
/// mailbox row reproduces. Only `chunk_id` and the rowid are referenced, so
/// SQLite answers it from the chunk-id key's index without reading a body.
pub(super) const LEGACY_STANDALONE_IDS: &str = "SELECT r.rowid FROM rag_chunks r
     WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.id = r.chunk_id)
       AND NOT EXISTS (SELECT 1 FROM outbound_messages o WHERE o.id = r.chunk_id)";

/// One searchable turn, as projected.
pub(super) struct Chunk<'a> {
    pub id: &'a str,
    pub agent_id: &'a str,
    pub agent_name: &'a str,
    pub direction: &'a str,
    pub sender: &'a str,
    pub destination: &'a str,
    pub timestamp_ms: i64,
    pub text: &'a str,
    pub kind: Option<&'a str>,
    pub tool: Option<&'a str>,
    pub seq: i64,
}

enum MemoryIndexBackend {
    Sqlite { writer: Mutex<Connection> },
    Postgres { pool: sqlx::PgPool },
}

/// The process's one writer on a SQLite index file, or pooled connection on PostgreSQL.
pub(super) struct MemoryIndex {
    path: PathBuf,
    backend: MemoryIndexBackend,
    /// Chunks a non-blocking re-projection found the index busy for; the
    /// next sync queues them as pending entries.
    deferred: Mutex<Vec<String>>,
}

impl MemoryIndex {
    /// Open (creating or rebuilding when needed) the SQLite index at `path`. An empty
    /// path is a private in-memory index, for a private in-memory state.
    pub(super) fn open(path: &Path) -> Result<Self, Error> {
        owner_only(path)?;
        let conn = match open_checked(path) {
            Ok(conn) => conn,
            // Another process is writing it (or this is private memory):
            // nothing is wrong with the file.
            Err(error) if path.as_os_str().is_empty() || error.is_transient_lock_error() => {
                return Err(error);
            }
            // Unreadable as an index: it is derived, so start it over.
            Err(_) => {
                for suffix in ["", "-wal", "-shm"] {
                    let mut file = path.as_os_str().to_owned();
                    file.push(suffix);
                    match std::fs::remove_file(PathBuf::from(file)) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                owner_only(path)?;
                open_checked(path)?
            }
        };
        Ok(Self {
            path: path.to_path_buf(),
            backend: MemoryIndexBackend::Sqlite {
                writer: Mutex::new(conn),
            },
            deferred: Mutex::new(Vec::new()),
        })
    }

    pub(super) fn open_postgres(path: &Path, pool: sqlx::PgPool) -> Result<Self, Error> {
        pool.db_execute(
            "INSERT INTO memory_index.index_meta (key, value)
             VALUES ('layout', ?1), ('messages_rowid', 0), ('outbound_rowid', 0)
             ON CONFLICT (key) DO NOTHING",
            &db_params![LAYOUT],
        )?;
        Ok(Self {
            path: path.to_path_buf(),
            backend: MemoryIndexBackend::Postgres { pool },
            deferred: Mutex::new(Vec::new()),
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn sqlite_writer(&self) -> Option<MutexGuard<'_, Connection>> {
        match &self.backend {
            MemoryIndexBackend::Sqlite { writer } => {
                Some(writer.lock().unwrap_or_else(|p| p.into_inner()))
            }
            MemoryIndexBackend::Postgres { .. } => None,
        }
    }

    fn try_sqlite_writer(&self) -> Option<MutexGuard<'_, Connection>> {
        match &self.backend {
            MemoryIndexBackend::Sqlite { writer } => match writer.try_lock() {
                Ok(guard) => Some(guard),
                Err(TryLockError::Poisoned(p)) => Some(p.into_inner()),
                Err(TryLockError::WouldBlock) => None,
            },
            MemoryIndexBackend::Postgres { .. } => None,
        }
    }

    fn run_read<T>(&self, f: impl FnOnce(&dyn DbExec) -> Result<T, Error>) -> Result<T, Error> {
        match &self.backend {
            MemoryIndexBackend::Sqlite { writer } => {
                let conn = writer.lock().unwrap_or_else(|p| p.into_inner());
                f(&*conn)
            }
            MemoryIndexBackend::Postgres { pool } => f(pool),
        }
    }

    pub(super) fn run_write_tx<T>(
        &self,
        wait: bool,
        f: impl FnOnce(&DbTx<'_>) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        match &self.backend {
            MemoryIndexBackend::Sqlite { writer } => {
                let Some(mut conn) = (if wait {
                    Some(writer.lock().unwrap_or_else(|p| p.into_inner()))
                } else {
                    self.try_sqlite_writer()
                }) else {
                    return Ok(None);
                };
                conn.busy_timeout(if wait { BUSY } else { Duration::ZERO })?;
                let res = (|| -> Result<T, Error> {
                    let raw = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let wrapped = DbTx::Sqlite(&raw);
                    let out = f(&wrapped)?;
                    raw.commit()?;
                    Ok(out)
                })();
                let _ = conn.busy_timeout(BUSY);
                match res {
                    Ok(v) => Ok(Some(v)),
                    Err(err) if !wait && err.is_transient_lock_error() => Ok(None),
                    Err(err) => Err(err),
                }
            }
            MemoryIndexBackend::Postgres { pool } => {
                let tx = PgTx::begin(pool)?;
                if !wait {
                    let got: bool = tx.db_query_one(
                        "SELECT pg_try_advisory_xact_lock(78697969)", // "memory_idx"
                        &[],
                        |r| r.get(0),
                    )?;
                    if !got {
                        return Ok(None);
                    }
                } else {
                    let _: () =
                        tx.db_query_one("SELECT pg_advisory_xact_lock(78697969)", &[], |_| Ok(()))?;
                }
                let wrapped = DbTx::Postgres(tx);
                let out = f(&wrapped)?;
                if let DbTx::Postgres(tx) = wrapped {
                    tx.commit()?;
                }
                Ok(Some(out))
            }
        }
    }

    /// Bring the index up to `state`: drain pending entries, then project up
    /// to `budget` new mailbox rows. `wait: false` gives up at once when
    /// another writer holds the index, leaving the rows to the next sync.
    /// Returns `true` when nothing is left to project.
    pub(super) fn sync(
        &self,
        state: &dyn DbExec,
        wait: bool,
        budget: usize,
    ) -> Result<bool, Error> {
        match &self.backend {
            MemoryIndexBackend::Sqlite { .. } => {
                let Some(mut conn) = (if wait {
                    self.sqlite_writer()
                } else {
                    self.try_sqlite_writer()
                }) else {
                    return Ok(false);
                };
                conn.busy_timeout(if wait { BUSY } else { Duration::ZERO })?;
                let deferred =
                    std::mem::take(&mut *self.deferred.lock().unwrap_or_else(|p| p.into_inner()));
                let mut outcome = Ok(true);
                if !deferred.is_empty() {
                    let tx_res = (|| -> Result<(), Error> {
                        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                        let wrapped = DbTx::Sqlite(&tx);
                        queue_chunks(&wrapped, &deferred)?;
                        tx.commit()?;
                        Ok(())
                    })();
                    if let Err(err) = tx_res {
                        outcome = Err(err);
                    }
                }
                if outcome.is_ok() {
                    outcome = sync_on_sqlite(&mut conn, state, budget);
                }
                if outcome.is_err() && !deferred.is_empty() {
                    self.deferred
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .extend(deferred);
                }
                conn.busy_timeout(BUSY)?;
                match outcome {
                    Err(error) if !wait && error.is_transient_lock_error() => Ok(false),
                    other => other,
                }
            }
            MemoryIndexBackend::Postgres { pool } => {
                let deferred =
                    std::mem::take(&mut *self.deferred.lock().unwrap_or_else(|p| p.into_inner()));
                let res = sync_on_postgres(pool, state, &deferred, wait, budget);
                if !matches!(res, Ok(Some(_))) && !deferred.is_empty() {
                    self.deferred
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .extend(deferred);
                }
                match res {
                    Ok(Some(done)) => Ok(done),
                    Ok(None) => Ok(false),
                    Err(error) if !wait && error.is_transient_lock_error() => Ok(false),
                    Err(error) => Err(error),
                }
            }
        }
    }

    /// Record that `chunks` and `agents` are about to change in state, and
    /// take the chunks' projected rows out of the index until they are
    /// re-projected from what state commits. Call before the state write;
    /// [`MemoryIndex::sync`] afterwards.
    pub(super) fn invalidate(
        &self,
        state: &(impl DbExec + ?Sized),
        chunks: &[&str],
        agents: &[&str],
        wait: bool,
    ) -> Result<(), Error> {
        let ran = self.run_write_tx(wait, |tx| invalidate_on(tx, state, chunks, agents))?;
        if ran.is_none() {
            return Err(Error::Message("the search index is busy; retry".into()));
        }
        Ok(())
    }

    /// Take the index's write lock and open a transaction, for a change that
    /// must be durable here before state commits (a standalone summary's
    /// redaction). Taken before the state writer, so the state transaction
    /// never waits on the index.
    pub(super) fn begin_staged(&self) -> Result<Staged<'_>, Error> {
        match &self.backend {
            MemoryIndexBackend::Sqlite { .. } => {
                let conn = self.sqlite_writer().expect("sqlite backend");
                conn.execute_batch("BEGIN IMMEDIATE")?;
                Ok(Staged::Sqlite { conn, open: true })
            }
            MemoryIndexBackend::Postgres { pool } => {
                let tx = PgTx::begin(pool)?;
                Ok(Staged::Postgres { tx: Some(tx) })
            }
        }
    }

    /// Remove a deleted agent's rows. `max_rowids` are the highest
    /// `messages` and `outbound_messages` rowids left by the delete: any row
    /// inserted after it lies above them, even where it reuses a rowid the
    /// agent's rows held.
    pub(super) fn agent_deleted(
        &self,
        agent_id: &str,
        max_rowids: (i64, i64),
    ) -> Result<(), Error> {
        self.run_write_tx(true, |tx| {
            forget_agent(tx, agent_id)?;
            let least = tx.backend().sql_least("value", "?2");
            let sql = format!("UPDATE index_meta SET value = {least} WHERE key = ?1");
            for (key, max) in [
                ("messages_rowid", max_rowids.0),
                ("outbound_rowid", max_rowids.1),
            ] {
                tx.db_execute(&sql, &db_params![key, max])?;
            }
            tx.db_execute(
                "DELETE FROM index_pending WHERE kind = 'agent' AND id = ?1",
                &db_params![agent_id],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Store a standalone tool summary, which has no mailbox row.
    pub(super) fn record_standalone(&self, chunk: &Chunk<'_>) -> Result<(), Error> {
        self.run_write_tx(true, |tx| upsert_chunk(tx, chunk))?;
        Ok(())
    }

    /// The text of a standalone summary `chunk_id` of `agent_id`, if any.
    pub(super) fn standalone_text(
        &self,
        agent_id: &str,
        chunk_id: &str,
    ) -> Result<Option<String>, Error> {
        self.run_read(|conn| {
            conn.db_query_opt(
                "SELECT text FROM rag_chunks WHERE agent_id = ?1 AND chunk_id = ?2",
                &db_params![agent_id, chunk_id],
                |r| r.get(0),
            )
        })
    }

    /// Forget a standalone redaction's staged receipt.
    pub(super) fn clear_staged_redaction(&self, chunk_id: &str) -> Result<(), Error> {
        self.run_write_tx(true, |tx| {
            tx.db_execute(
                "DELETE FROM index_redactions WHERE chunk_id = ?1",
                &db_params![chunk_id],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Before state retires its own search tables: keep the standalone tool
    /// summaries they hold, the rows no rebuild can reproduce. `state` is the
    /// state writer, used here only to read.
    pub(super) fn adopt_legacy(&self, state: &Connection) -> Result<(), Error> {
        let legacy: bool = state.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'rag_chunks')",
            [],
            |r| r.get(0),
        )?;
        if !legacy {
            return Ok(());
        }
        // This runs before the server answers. Which chunks have no mailbox
        // row is decided from the chunk-id key alone ([`LEGACY_STANDALONE_IDS`]),
        // so only those few rows' bodies are read, never the whole table.
        let mut stmt = state.prepare(&format!(
            "SELECT c.chunk_id, c.agent_id, c.agent_name, c.direction, c.sender, c.destination,
                    c.timestamp_ms, c.text, c.kind, c.tool_name, c.seq
             FROM rag_chunks c
             WHERE c.rowid IN ({LEGACY_STANDALONE_IDS})"
        ))?;
        let rows = stmt
            .query_map([], StandaloneRow::read)?
            .collect::<Result<Vec<_>, _>>()?;
        let Some(mut conn) = self.sqlite_writer() else {
            return Ok(());
        };
        // State drops its copy right after this returns: the index's copy
        // must be on disk first, not only in a WAL a power loss can cut.
        conn.pragma_update(None, "synchronous", "FULL")?;
        let written = (|| -> Result<(), Error> {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for row in &rows {
                upsert_chunk(&tx, &row.chunk())?;
            }
            tx.commit()?;
            Ok(())
        })();
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        written
    }

    /// Queue the most recent mailbox rows whose projection is missing, which
    /// only a writer interrupted between its state commit and its index sync
    /// leaves behind.
    pub(super) fn check_tail(&self, state: &(impl DbExec + ?Sized)) -> Result<(), Error> {
        let marks = self.run_read(|c| marks(c))?;
        let mut missing = Vec::new();
        // Rows past a mark are projected by the next sync anyway; a body too
        // short to search never has a row (the few the SQL length lets
        // through are dropped again by the drain).
        for (table, mark) in [("messages", marks.0), ("outbound_messages", marks.1)] {
            let ids: Vec<String> = state.db_query_vec(
                &format!(
                    "SELECT id FROM {table} WHERE rowid <= ?1 AND length(body) >= 2
                     ORDER BY rowid DESC LIMIT ?2"
                ),
                &db_params![mark, TAIL_CHECK],
                |r| r.get(0),
            )?;
            self.run_read(|conn| {
                for id in &ids {
                    let exists: Option<i64> = conn.db_query_opt(
                        "SELECT 1 FROM rag_chunks WHERE chunk_id = ?1",
                        &db_params![id],
                        |r| r.get(0),
                    )?;
                    if exists.is_none() {
                        missing.push(id.clone());
                    }
                }
                Ok(())
            })?;
        }
        // A channel message's stored body can be merged with a fuller copy
        // after it was projected; a writer stopped before re-projecting it
        // leaves a row whose text no longer matches.
        let merged: Vec<(String, String)> = state.db_query_vec(
            "SELECT id, body FROM messages WHERE rowid <= ?1 AND origin IS NOT NULL
             ORDER BY rowid DESC LIMIT ?2",
            &db_params![marks.0, TAIL_CHECK],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        self.run_read(|conn| {
            for (id, body) in &merged {
                let cleaned = clean_rag_text(body);
                if cleaned.chars().take(2).count() < 2 {
                    continue;
                }
                let hash: Option<String> = conn.db_query_opt(
                    "SELECT content_hash FROM rag_chunks WHERE chunk_id = ?1",
                    &db_params![id],
                    |r| r.get(0),
                )?;
                if hash.is_some_and(|h| h != rag_content_hash(&cleaned)) {
                    missing.push(id.clone());
                }
            }
            Ok(())
        })?;
        if missing.is_empty() {
            return Ok(());
        }
        self.run_write_tx(true, |tx| {
            for id in &missing {
                tx.db_execute(
                    "INSERT INTO index_pending (kind, id) VALUES ('chunk', ?1) ON CONFLICT DO NOTHING",
                    &db_params![id],
                )?;
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Retry committed state changes on the next in-process sync.
    pub(super) fn defer(&self, chunks: &[String]) {
        self.deferred
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(chunks.iter().cloned());
    }

    /// Re-project `chunks` from state without waiting: `false` when another
    /// writer holds the index, and nothing was done.
    pub(super) fn try_reproject(
        &self,
        state: &(impl DbExec + ?Sized),
        chunks: &[String],
    ) -> Result<bool, Error> {
        let res = self.run_write_tx(false, |tx| {
            for id in chunks {
                match source_row(state, id)? {
                    Some(row) => upsert_chunk(tx, &row.chunk())?,
                    None => remove_chunk(tx, id)?,
                }
            }
            Ok(())
        });
        match res {
            Ok(Some(())) => Ok(true),
            Ok(None) => {
                self.defer(chunks);
                Ok(false)
            }
            Err(error) => {
                self.defer(chunks);
                if error.is_transient_lock_error() {
                    Ok(false)
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Whether more than one read's budget of rows waits to be projected.
    pub(super) fn far_behind(&self, state: &(impl DbExec + ?Sized)) -> Result<bool, Error> {
        let marks = self.run_read(|c| marks(c))?;
        let (messages, outbound) = max_rowids(state)?;
        if let MemoryIndexBackend::Postgres { pool } = &self.backend
            && (marks.0 > messages || marks.1 > outbound || postgres::reset_cursor(pool)?.is_some())
        {
            return Ok(true);
        }
        Ok((messages - marks.0).max(0) + (outbound - marks.1).max(0) > READ_BUDGET as i64)
    }
}

fn invalidate_on(
    tx: &DbTx<'_>,
    state: &(impl DbExec + ?Sized),
    chunks: &[&str],
    agents: &[&str],
) -> Result<(), Error> {
    for id in chunks {
        tx.db_execute(
            "INSERT INTO index_pending (kind, id) VALUES ('chunk', ?1) ON CONFLICT DO NOTHING",
            &db_params![id],
        )?;
        // A standalone summary has nothing to be re-projected from.
        if source_row(state, id)?.is_some() {
            remove_chunk(tx, id)?;
        }
    }
    let least_msg = tx.backend().sql_least(
        "COALESCE(index_pending.cap_messages, excluded.cap_messages)",
        "excluded.cap_messages",
    );
    let least_out = tx.backend().sql_least(
        "COALESCE(index_pending.cap_outbound, excluded.cap_outbound)",
        "excluded.cap_outbound",
    );
    let agent_sql = format!(
        "INSERT INTO index_pending (kind, id, cap_messages, cap_outbound)
         VALUES ('agent', ?1, ?2, ?3)
         ON CONFLICT (kind, id) DO UPDATE SET
             cap_messages = {least_msg},
             cap_outbound = {least_out}"
    );
    for id in agents {
        let (cap_messages, cap_outbound): (i64, i64) = state.db_query_one(
            "SELECT (SELECT COALESCE(MAX(rowid), 0) FROM messages WHERE agent_id != ?1),
                    (SELECT COALESCE(MAX(rowid), 0) FROM outbound_messages WHERE agent_id != ?1)",
            &db_params![id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        tx.db_execute(&agent_sql, &db_params![id, cap_messages, cap_outbound])?;
    }
    Ok(())
}

/// An open index write transaction, rolled back unless committed.
pub(crate) enum Staged<'a> {
    Sqlite {
        conn: MutexGuard<'a, Connection>,
        open: bool,
    },
    Postgres {
        tx: Option<PgTx>,
    },
}

impl Staged<'_> {
    fn as_exec(&self) -> &dyn DbExec {
        match self {
            Self::Sqlite { conn, .. } => &**conn,
            Self::Postgres { tx } => tx.as_ref().expect("staged postgres transaction open"),
        }
    }

    /// Any receipt staged for this chunk; a different selector must be refused.
    pub(crate) fn staged_receipt(&self, chunk_id: &str) -> Result<Option<(String, String)>, Error> {
        self.as_exec().db_query_opt(
            "SELECT request_key, receipt FROM index_redactions WHERE chunk_id = ?1",
            &db_params![chunk_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
    }

    /// Scrub a standalone summary to `text` and stage its receipt.
    pub(crate) fn stage_redaction(
        &self,
        agent_id: &str,
        chunk_id: &str,
        text: &str,
        request_key: &str,
        receipt: &str,
    ) -> Result<(), Error> {
        let exec = self.as_exec();
        rewrite_standalone_on(exec, agent_id, chunk_id, text)?;
        exec.db_execute(
            "INSERT INTO index_redactions (chunk_id, request_key, receipt)
             VALUES (?1, ?2, ?3)
             ON CONFLICT (chunk_id) DO UPDATE SET
                 request_key = excluded.request_key,
                 receipt = excluded.receipt",
            &db_params![chunk_id, request_key, receipt],
        )?;
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        match self {
            Self::Sqlite { conn, open } => {
                if *open {
                    conn.execute_batch("COMMIT")?;
                    *open = false;
                }
            }
            Self::Postgres { tx } => {
                if let Some(tx) = tx.take() {
                    tx.commit()?;
                }
            }
        }
        Ok(())
    }
}

impl Drop for Staged<'_> {
    fn drop(&mut self) {
        match self {
            Self::Sqlite { conn, open } => {
                if *open {
                    let _ = conn.execute_batch("ROLLBACK");
                }
            }
            Self::Postgres { tx } => {
                drop(tx.take());
            }
        }
    }
}

fn rewrite_standalone_on(
    conn: &dyn DbExec,
    agent_id: &str,
    chunk_id: &str,
    text: &str,
) -> Result<(), Error> {
    let row = conn.db_query_opt(
        "SELECT chunk_id, agent_id, agent_name, direction, sender, destination,
                timestamp_ms, text, kind, tool_name, seq
         FROM rag_chunks WHERE agent_id = ?1 AND chunk_id = ?2",
        &db_params![agent_id, chunk_id],
        StandaloneRow::read_db,
    )?;
    remove_chunk(conn, chunk_id)?;
    if let Some(mut row) = row {
        row.text = text.to_string();
        upsert_chunk(conn, &row.chunk())?;
    }
    Ok(())
}

/// Fill the index in the background until it has caught up with state or the
/// last handle on it is gone. `open_state` opens a state reader.
pub(super) fn spawn_rebuild<S: DbExec + Send + 'static>(
    index: &Arc<MemoryIndex>,
    open_state: impl Fn() -> Option<S> + Send + 'static,
) {
    let weak: Weak<MemoryIndex> = Arc::downgrade(index);
    let _ = std::thread::Builder::new()
        .name("memory-index-rebuild".into())
        .spawn(move || {
            let Some(state) = open_state() else { return };
            let mut failures = 0u32;
            while let Some(index) = weak.upgrade() {
                match index.sync(&state, true, BATCH) {
                    Ok(true) => return,
                    Ok(false) => failures = 0,
                    Err(error) => {
                        failures += 1;
                        if failures > 5 {
                            eprintln!(
                                "anyagents memory index: rebuild stopped ({error}); reads resume it"
                            );
                            return;
                        }
                        drop(index);
                        std::thread::sleep(Duration::from_secs(1 << failures));
                    }
                }
            }
        });
}

/// Make the index file and its log readable by its owner only, creating the
/// file that way when it is missing. The index holds every mailbox body, so
/// it is no more readable than the state database it projects; SQLite gives
/// the log files it creates the database file's mode.
fn owner_only(path: &Path) -> Result<(), Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        if path.as_os_str().is_empty() {
            return Ok(());
        }
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;
        for suffix in ["", "-wal", "-shm"] {
            let mut file = path.as_os_str().to_owned();
            file.push(suffix);
            let file = PathBuf::from(file);
            let mode = match std::fs::metadata(&file) {
                Ok(meta) => meta.permissions().mode(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if mode & 0o077 != 0 {
                std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode & 0o700))?;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn open_checked(path: &Path) -> Result<Connection, Error> {
    let conn = if path.as_os_str().is_empty() {
        Connection::open_in_memory()?
    } else {
        Connection::open(path)?
    };
    conn.busy_timeout(BUSY)?;
    if !path.as_os_str().is_empty() {
        let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Message(format!(
                "memory index journal mode is {mode}"
            )));
        }
    }
    // Derived data: a lost last commit is re-projected from state.
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    // Deleted or replaced text (a redaction) is overwritten, not left in
    // free pages.
    conn.pragma_update(None, "secure_delete", "ON")?;
    conn.pragma_update(None, "journal_size_limit", 64 * 1024 * 1024)?;
    tune_connection(&conn)?;
    let layout: Option<i64> = conn
        .query_row(
            "SELECT value FROM index_meta WHERE key = 'layout'",
            [],
            |r| r.get(0),
        )
        .optional()
        .or_else(|error| match error {
            rusqlite::Error::SqliteFailure(_, Some(ref m)) if m.contains("no such table") => {
                Ok(None)
            }
            other => Err(other),
        })?;
    if layout != Some(LAYOUT) {
        reset(&conn, None, false)?;
    }
    super::knowledge::sqlite_index::initialize(&conn)?;
    // A damaged file fails here and is recreated by the caller.
    conn.query_row(
        "SELECT count(*) FROM rag_chunks_fts WHERE rowid = 0",
        [],
        |r| r.get::<_, i64>(0),
    )?;
    Ok(conn)
}

/// Drop everything and create the current SQLite layout, empty, with marks at zero.
/// Unless `force`, a file another process brought to the current layout
/// meanwhile is left as it is.
pub(super) fn reset(
    conn: &Connection,
    state: Option<&dyn DbExec>,
    force: bool,
) -> Result<(), Error> {
    use rusqlite::params;
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> Result<(), Error> {
        let current = conn
            .query_row(
                "SELECT value = ?1 FROM index_meta WHERE key = 'layout'",
                [LAYOUT],
                |r| r.get::<_, bool>(0),
            )
            .unwrap_or(false);
        if current && !force {
            return Ok(());
        }
        // Standalone tool summaries (ids `record_tool_event` mints) are kept
        // across a rebuild: state cannot reproduce them.
        let kept: Vec<StandaloneRow> = conn
            .prepare(
                "SELECT chunk_id, agent_id, agent_name, direction, sender, destination,
                        timestamp_ms, text, kind, tool_name, seq
                 FROM rag_chunks WHERE chunk_id LIKE 'tool\\_%' ESCAPE '\\'",
            )
            .and_then(|mut stmt| {
                stmt.query_map([], StandaloneRow::read)?
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_default();
        // A summary whose agent state no longer has would be an orphan
        // nothing removes.
        let kept: Vec<StandaloneRow> = match state {
            Some(state) => {
                let mut alive = Vec::new();
                for row in kept {
                    if state
                        .db_query_opt(
                            "SELECT 1 FROM agents WHERE id = ?1",
                            &db_params![row.agent_id],
                            |_| Ok(()),
                        )?
                        .is_some()
                    {
                        alive.push(row);
                    }
                }
                alive
            }
            None => kept,
        };
        // Pending entries describe state changes still to apply; a rebuild
        // does not apply them (an agent deleted meanwhile, a redaction).
        type Pending = (String, String, Option<i64>, Option<i64>);
        let pending: Vec<Pending> = conn
            .prepare("SELECT kind, id, cap_messages, cap_outbound FROM index_pending")
            .and_then(|mut stmt| {
                stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
                    .collect::<Result<Vec<_>, _>>()
            })
            .or_else(|_| {
                conn.prepare("SELECT kind, id FROM index_pending")
                    .and_then(|mut stmt| {
                        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, None, None)))?
                            .collect::<Result<Vec<_>, _>>()
                    })
            })
            .unwrap_or_default();
        let staged: Vec<(String, String, String)> = conn
            .prepare("SELECT chunk_id, request_key, receipt FROM index_redactions")
            .and_then(|mut stmt| {
                stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_default();
        let names: Vec<(String, String)> = conn
            .prepare(
                "SELECT type, name FROM sqlite_master
                 WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
                   AND name NOT LIKE 'rag_chunks_fts_%'
                   AND name NOT LIKE 'knowledge_rag_chunks_fts_%'",
            )?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?;
        for (kind, name) in names {
            let kind = if kind == "view" { "VIEW" } else { "TABLE" };
            conn.execute_batch(&format!(
                "DROP {kind} IF EXISTS \"{}\"",
                name.replace('"', "\"\"")
            ))?;
        }
        conn.execute_batch(SCHEMA_SQL)?;
        super::knowledge::sqlite_index::create(conn)?;
        conn.execute(
            "INSERT INTO rag_chunks_fts(rag_chunks_fts, rank) VALUES('secure-delete', 1)",
            [],
        )?;
        conn.execute(
            "INSERT INTO index_meta (key, value) VALUES ('layout', ?1),
                 ('messages_rowid', 0), ('outbound_rowid', 0)",
            [LAYOUT],
        )?;
        for row in &kept {
            upsert_chunk(conn, &row.chunk())?;
        }
        for (kind, id, cap_messages, cap_outbound) in &pending {
            conn.execute(
                "INSERT OR IGNORE INTO index_pending (kind, id, cap_messages, cap_outbound)
                 VALUES (?1, ?2, ?3, ?4)",
                params![kind, id, cap_messages, cap_outbound],
            )?;
        }
        for (chunk_id, key, receipt) in &staged {
            conn.execute(
                "INSERT OR IGNORE INTO index_redactions (chunk_id, request_key, receipt)
                 VALUES (?1, ?2, ?3)",
                params![chunk_id, key, receipt],
            )?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => conn.execute_batch("COMMIT")?,
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(error);
        }
    }
    Ok(())
}

/// A chunk read back from an index or legacy table, with its own kind.
struct StandaloneRow {
    id: String,
    agent_id: String,
    agent_name: String,
    direction: String,
    sender: String,
    destination: String,
    timestamp_ms: i64,
    text: String,
    kind: String,
    tool: String,
    seq: i64,
}

impl StandaloneRow {
    /// Columns: chunk_id, agent_id, agent_name, direction, sender,
    /// destination, timestamp_ms, text, kind, tool_name, seq.
    fn read(r: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: r.get(0)?,
            agent_id: r.get(1)?,
            agent_name: r.get(2)?,
            direction: r.get(3)?,
            sender: r.get(4)?,
            destination: r.get(5)?,
            timestamp_ms: r.get(6)?,
            text: r.get(7)?,
            kind: r.get(8)?,
            tool: r.get(9)?,
            seq: r.get(10)?,
        })
    }

    fn read_db(r: &crate::db::DbRow<'_>) -> Result<Self, Error> {
        Ok(Self {
            id: r.get(0)?,
            agent_id: r.get(1)?,
            agent_name: r.get(2)?,
            direction: r.get(3)?,
            sender: r.get(4)?,
            destination: r.get(5)?,
            timestamp_ms: r.get(6)?,
            text: r.get(7)?,
            kind: r.get(8)?,
            tool: r.get(9)?,
            seq: r.get(10)?,
        })
    }

    fn chunk(&self) -> Chunk<'_> {
        Chunk {
            id: &self.id,
            agent_id: &self.agent_id,
            agent_name: &self.agent_name,
            direction: &self.direction,
            sender: &self.sender,
            destination: &self.destination,
            timestamp_ms: self.timestamp_ms,
            text: &self.text,
            kind: Some(&self.kind),
            tool: Some(&self.tool),
            seq: self.seq,
        }
    }
}

fn marks(conn: &(impl DbExec + ?Sized)) -> Result<(i64, i64), Error> {
    let get = |key: &str| -> Result<i64, Error> {
        Ok(conn
            .db_query_opt(
                "SELECT value FROM index_meta WHERE key = ?1",
                &db_params![key],
                |r| r.get(0),
            )?
            .unwrap_or(0))
    };
    Ok((get("messages_rowid")?, get("outbound_rowid")?))
}

fn max_rowids(state: &(impl DbExec + ?Sized)) -> Result<(i64, i64), Error> {
    state.db_query_one(
        "SELECT (SELECT COALESCE(MAX(rowid), 0) FROM messages),
                (SELECT COALESCE(MAX(rowid), 0) FROM outbound_messages)",
        &[],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
}

/// One source row of a projected chunk: `(rowid, chunk)` fields owned.
pub(super) struct SourceRow {
    rowid: i64,
    id: String,
    agent_id: String,
    agent_name: String,
    direction: &'static str,
    sender: String,
    destination: String,
    timestamp_ms: i64,
    body: String,
    pub(super) seq: i64,
}

impl SourceRow {
    fn chunk(&self) -> Chunk<'_> {
        Chunk {
            id: &self.id,
            agent_id: &self.agent_id,
            agent_name: &self.agent_name,
            direction: self.direction,
            sender: &self.sender,
            destination: &self.destination,
            timestamp_ms: self.timestamp_ms,
            text: &self.body,
            kind: None,
            tool: None,
            seq: self.seq,
        }
    }
}

const INBOUND_SELECT: &str =
    "SELECT m.rowid, m.id, m.agent_id, a.name, COALESCE(m.sender, 'unknown'),
        COALESCE(m.reply_to, 'agent:' || a.name), m.received_at_ms, m.body, m.seq
     FROM messages m JOIN agents a ON a.id = m.agent_id";
const OUTBOUND_SELECT: &str = "SELECT o.rowid, o.id, o.agent_id, a.name, 'agent:' || a.name,
        o.destination, o.sent_at_ms, o.body, o.seq
     FROM outbound_messages o JOIN agents a ON a.id = o.agent_id";
/// [`OUTBOUND_SELECT`] while state still has its retired pre-split chunks: a
/// turn whose mailbox position has not been moved onto its row yet
/// (`retired_search.rs`) takes it from its retired chunk.
const OUTBOUND_SELECT_RETIRING: &str = "SELECT o.rowid, o.id, o.agent_id, a.name,
        'agent:' || a.name, o.destination, o.sent_at_ms, o.body,
        COALESCE(NULLIF(o.seq, 0),
                 (SELECT c.seq FROM retired_rag_chunks c WHERE c.chunk_id = o.id), 0)
     FROM outbound_messages o JOIN agents a ON a.id = o.agent_id";

/// Mailbox rows of one direction (`"in"`: `messages m`, `"out"`:
/// `outbound_messages o`) matching `filter`, as projected.
pub(super) fn read_source_rows(
    state: &(impl DbExec + ?Sized),
    direction: &'static str,
    filter: &str,
    params: &[DbParam],
) -> Result<Vec<SourceRow>, Error> {
    if direction == "in" {
        return read_rows(
            state,
            &format!("{INBOUND_SELECT} {filter}"),
            direction,
            params,
        );
    }
    let retiring =
        state.backend() == DbBackend::Sqlite && super::retired_search::present_db(state)?;
    let select = if retiring {
        OUTBOUND_SELECT_RETIRING
    } else {
        OUTBOUND_SELECT
    };
    match read_rows(state, &format!("{select} {filter}"), direction, params) {
        // The retired chunks were dropped since they were looked up; every
        // position had been moved onto its row first.
        Err(_) if retiring && !super::retired_search::present_db(state)? => read_rows(
            state,
            &format!("{OUTBOUND_SELECT} {filter}"),
            direction,
            params,
        ),
        other => other,
    }
}

fn read_rows(
    state: &(impl DbExec + ?Sized),
    sql: &str,
    direction: &'static str,
    params: &[DbParam],
) -> Result<Vec<SourceRow>, Error> {
    state.db_query_vec(sql, params, |r| {
        Ok(SourceRow {
            rowid: r.get(0)?,
            id: r.get(1)?,
            agent_id: r.get(2)?,
            agent_name: r.get(3)?,
            direction,
            sender: r.get(4)?,
            destination: r.get(5)?,
            timestamp_ms: r.get(6)?,
            body: r.get(7)?,
            seq: r.get(8)?,
        })
    })
}

/// The mailbox row a chunk id is projected from, if state has one.
fn source_row(state: &(impl DbExec + ?Sized), id: &str) -> Result<Option<SourceRow>, Error> {
    let inbound = read_source_rows(state, "in", "WHERE m.id = ?1", &db_params![id])?;
    if let Some(row) = inbound.into_iter().next() {
        return Ok(Some(row));
    }
    Ok(
        read_source_rows(state, "out", "WHERE o.id = ?1", &db_params![id])?
            .into_iter()
            .next(),
    )
}

fn sync_on_sqlite(conn: &mut Connection, state: &dyn DbExec, budget: usize) -> Result<bool, Error> {
    let (messages, outbound) = marks(conn)?;
    let (max_messages, max_outbound) = max_rowids(state)?;
    let pending: bool =
        conn.db_query_one("SELECT EXISTS(SELECT 1 FROM index_pending)", &[], |r| {
            r.get(0)
        })?;
    // Caught up: answered by reads, without the index's write lock.
    if !pending && messages == max_messages && outbound == max_outbound {
        return Ok(true);
    }
    // First the pending changes: a deleted agent's entry lowers the marks
    // past the rowids it held, which is not state moving backwards.
    if pending {
        drain_pending_on_sqlite(conn, state)?;
    }
    // State moved backwards (restored, replaced): the projection describes
    // rows state no longer has. Start over.
    let (messages, outbound) = marks(conn)?;
    if messages > max_messages || outbound > max_outbound {
        reset(conn, Some(state), true)?;
    }
    let mut left = budget;
    for (key, rowid, direction) in [
        ("messages_rowid", "m.rowid", "in"),
        ("outbound_rowid", "o.rowid", "out"),
    ] {
        loop {
            if left == 0 {
                return Ok(false);
            }
            let take = left.min(BATCH);
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mark: i64 = tx
                .db_query_opt(
                    "SELECT value FROM index_meta WHERE key = ?1",
                    &db_params![key],
                    |r| r.get(0),
                )?
                .unwrap_or(0);
            // Rows whose agent is gone have no owner to file them under;
            // they are skipped by the join and covered by the mark.
            let max: i64 = state.db_query_one(
                &format!(
                    "SELECT COALESCE(MAX(rowid), 0) FROM (SELECT rowid FROM {} WHERE rowid > ?1 ORDER BY rowid LIMIT ?2) AS _batch",
                    if direction == "in" {
                        "messages"
                    } else {
                        "outbound_messages"
                    }
                ),
                &db_params![mark, take as i64],
                |r| r.get(0),
            )?;
            if max <= mark {
                tx.commit()?;
                break;
            }
            let rows = read_source_rows(
                state,
                direction,
                &format!("WHERE {rowid} > ?1 AND {rowid} <= ?2 ORDER BY {rowid}"),
                &db_params![mark, max],
            )?;
            for row in &rows {
                upsert_chunk(&tx, &row.chunk())?;
            }
            debug_assert!(rows.iter().all(|r| r.rowid <= max));
            tx.db_execute(
                "UPDATE index_meta SET value = ?2 WHERE key = ?1",
                &db_params![key, max],
            )?;
            tx.commit()?;
            left = left.saturating_sub(take);
        }
    }
    Ok(true)
}

fn queue_chunks(tx: &(impl DbExec + ?Sized), chunks: &[String]) -> Result<(), Error> {
    if chunks.is_empty() {
        return Ok(());
    }
    for id in chunks {
        tx.db_execute(
            "INSERT INTO index_pending (kind, id) VALUES ('chunk', ?1) ON CONFLICT DO NOTHING",
            &db_params![id],
        )?;
    }
    Ok(())
}

fn drain_pending_on_sqlite(
    conn: &mut Connection,
    state: &(impl DbExec + ?Sized),
) -> Result<(), Error> {
    loop {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let wrapped = DbTx::Sqlite(&tx);
        let more = drain_pending_step(&wrapped, state)?;
        tx.commit()?;
        if !more {
            return Ok(());
        }
    }
}

fn drain_pending_step(tx: &DbTx<'_>, state: &(impl DbExec + ?Sized)) -> Result<bool, Error> {
    type Pending = (String, String, Option<i64>, Option<i64>);
    let pending: Vec<Pending> = tx.db_query_vec(
        "SELECT kind, id, cap_messages, cap_outbound FROM index_pending LIMIT 256",
        &[],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    if pending.is_empty() {
        return Ok(false);
    }
    let least_sql = format!(
        "UPDATE index_meta SET value = {} WHERE key = ?1",
        tx.backend().sql_least("value", "?2")
    );
    for (kind, id, cap_messages, cap_outbound) in &pending {
        match kind.as_str() {
            // A standalone summary keeps its row; a mailbox row leaves
            // state only with its agent, below.
            "chunk" => {
                if let Some(row) = source_row(state, id)? {
                    upsert_chunk(tx, &row.chunk())?;
                }
            }
            "agent" => {
                let name: Option<String> = state.db_query_opt(
                    "SELECT name FROM agents WHERE id = ?1",
                    &db_params![id],
                    |r| r.get(0),
                )?;
                match name {
                    Some(name) => {
                        rewrite_agent_fts_rows(tx, id, Some(&name))?;
                        if tx.backend() == DbBackend::Sqlite {
                            tx.db_execute(
                                "UPDATE rag_chunks SET agent_name = ?2 WHERE agent_id = ?1 AND agent_name != ?2",
                                &db_params![id, name],
                            )?;
                        }
                    }
                    None => {
                        forget_agent(tx, id)?;
                        // The bound read before the delete: rows a new
                        // agent added since may reuse the rowids.
                        let (now_messages, now_outbound) = max_rowids(state)?;
                        let messages = cap_messages.unwrap_or(now_messages).min(now_messages);
                        let outbound = cap_outbound.unwrap_or(now_outbound).min(now_outbound);
                        for (key, max) in
                            [("messages_rowid", messages), ("outbound_rowid", outbound)]
                        {
                            tx.db_execute(&least_sql, &db_params![key, max])?;
                        }
                    }
                }
            }
            _ => {}
        }
        tx.db_execute(
            "DELETE FROM index_pending WHERE kind = ?1 AND id = ?2",
            &db_params![kind, id],
        )?;
    }
    Ok(true)
}

fn forget_agent(conn: &(impl DbExec + ?Sized), agent_id: &str) -> Result<(), Error> {
    rewrite_agent_fts_rows(conn, agent_id, None)?;
    conn.db_execute(
        "DELETE FROM rag_chunks WHERE agent_id = ?1",
        &db_params![agent_id],
    )?;
    Ok(())
}
