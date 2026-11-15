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

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{
    classify_chunk_kind_and_tool, clean_rag_text, rag_content_hash, tokenize_hybrid_search_tokens,
    tune_connection,
};
use crate::error::Error;

/// The index file beside a state database named `state.db`.
pub const FILE_NAME: &str = "memory-index.db";
/// The schema name state readers attach the index under.
pub(crate) const ATTACHED: &str = "memory_index";
/// Layout version; a file with any other version is rebuilt from state.
const LAYOUT: i64 = 1;
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
    PRIMARY KEY (kind, id)
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

/// The process's one writer on an index file.
pub(super) struct MemoryIndex {
    path: PathBuf,
    writer: Mutex<Connection>,
}

impl MemoryIndex {
    /// Open (creating or rebuilding when needed) the index at `path`. An empty
    /// path is a private in-memory index, for a private in-memory state.
    pub(super) fn open(path: &Path) -> Result<Self, Error> {
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
                open_checked(path)?
            }
        };
        Ok(Self {
            path: path.to_path_buf(),
            writer: Mutex::new(conn),
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn writer(&self) -> MutexGuard<'_, Connection> {
        self.writer.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn try_writer(&self) -> Option<MutexGuard<'_, Connection>> {
        match self.writer.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(p)) => Some(p.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    /// Bring the index up to `state`: drain pending entries, then project up
    /// to `budget` new mailbox rows. `wait: false` gives up at once when
    /// another writer holds the index, leaving the rows to the next sync.
    /// Returns `true` when nothing is left to project.
    pub(super) fn sync(
        &self,
        state: &Connection,
        wait: bool,
        budget: usize,
    ) -> Result<bool, Error> {
        let Some(mut conn) = (if wait {
            Some(self.writer())
        } else {
            self.try_writer()
        }) else {
            return Ok(false);
        };
        conn.busy_timeout(if wait { BUSY } else { Duration::ZERO })?;
        let outcome = sync_on(&mut conn, state, budget);
        conn.busy_timeout(BUSY)?;
        match outcome {
            Err(error) if !wait && error.is_transient_lock_error() => Ok(false),
            other => other,
        }
    }

    /// Record that `chunks` and `agents` are about to change in state, and
    /// take the chunks' projected rows out of the index until they are
    /// re-projected from what state commits. Call before the state write;
    /// [`MemoryIndex::sync`] afterwards.
    pub(super) fn invalidate(
        &self,
        state: &Connection,
        chunks: &[&str],
        agents: &[&str],
    ) -> Result<(), Error> {
        let mut conn = self.writer();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for id in chunks {
            tx.execute(
                "INSERT OR IGNORE INTO index_pending (kind, id) VALUES ('chunk', ?1)",
                [id],
            )?;
            // A standalone summary has nothing to be re-projected from.
            if source_row(state, id)?.is_some() {
                remove_chunk(&tx, id)?;
            }
        }
        for id in agents {
            tx.execute(
                "INSERT OR IGNORE INTO index_pending (kind, id) VALUES ('agent', ?1)",
                [id],
            )?;
        }
        tx.commit()?;
        Ok(())
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
        let mut conn = self.writer();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        forget_agent(&tx, agent_id)?;
        for (key, max) in [
            ("messages_rowid", max_rowids.0),
            ("outbound_rowid", max_rowids.1),
        ] {
            tx.execute(
                "UPDATE index_meta SET value = MIN(value, ?2) WHERE key = ?1",
                params![key, max],
            )?;
        }
        tx.execute(
            "DELETE FROM index_pending WHERE kind = 'agent' AND id = ?1",
            [agent_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Store a standalone tool summary, which has no mailbox row.
    pub(super) fn record_standalone(&self, chunk: &Chunk<'_>) -> Result<(), Error> {
        let mut conn = self.writer();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        upsert_chunk(&tx, chunk)?;
        tx.commit()?;
        Ok(())
    }

    /// The text of a standalone summary `chunk_id` of `agent_id`, if any.
    pub(super) fn standalone_text(
        &self,
        agent_id: &str,
        chunk_id: &str,
    ) -> Result<Option<String>, Error> {
        Ok(self
            .writer()
            .query_row(
                "SELECT text FROM rag_chunks WHERE agent_id = ?1 AND chunk_id = ?2",
                params![agent_id, chunk_id],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Replace a standalone summary's text (a redaction), erasing the old
    /// text from the table and the full-text index.
    pub(super) fn rewrite_standalone(
        &self,
        agent_id: &str,
        chunk_id: &str,
        text: &str,
    ) -> Result<(), Error> {
        let mut conn = self.writer();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = tx
            .query_row(
                "SELECT agent_name, direction, sender, destination, timestamp_ms, kind, tool_name, seq
                 FROM rag_chunks WHERE agent_id = ?1 AND chunk_id = ?2",
                params![agent_id, chunk_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, i64>(7)?,
                    ))
                },
            )
            .optional()?;
        remove_chunk(&tx, chunk_id)?;
        if let Some((name, direction, sender, destination, ts, kind, tool, seq)) = row {
            upsert_chunk(
                &tx,
                &Chunk {
                    id: chunk_id,
                    agent_id,
                    agent_name: &name,
                    direction: &direction,
                    sender: &sender,
                    destination: &destination,
                    timestamp_ms: ts,
                    text,
                    kind: Some(&kind),
                    tool: Some(&tool),
                    seq,
                },
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Before state drops its own search tables: keep the standalone tool
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
        let mut stmt = state.prepare(
            "SELECT c.chunk_id, c.agent_id, c.agent_name, c.direction, c.sender, c.destination,
                    c.timestamp_ms, c.text, c.kind, c.tool_name, c.seq
             FROM rag_chunks c
             WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.id = c.chunk_id)
               AND NOT EXISTS (SELECT 1 FROM outbound_messages o WHERE o.id = c.chunk_id)",
        )?;
        let rows = stmt
            .query_map([], StandaloneRow::read)?
            .collect::<Result<Vec<_>, _>>()?;
        let mut conn = self.writer();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for row in &rows {
            upsert_chunk(&tx, &row.chunk())?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Queue the most recent mailbox rows whose projection is missing, which
    /// only a writer interrupted between its state commit and its index sync
    /// leaves behind.
    pub(super) fn check_tail(&self, state: &Connection) -> Result<(), Error> {
        let marks = marks(&self.writer())?;
        let mut missing = Vec::new();
        // Rows past a mark are projected by the next sync anyway; a body too
        // short to search never has a row (the few the SQL length lets
        // through are dropped again by the drain).
        for (table, mark) in [("messages", marks.0), ("outbound_messages", marks.1)] {
            let ids: Vec<String> = state
                .prepare(&format!(
                    "SELECT id FROM {table} WHERE rowid <= ?1 AND length(body) >= 2
                     ORDER BY rowid DESC LIMIT ?2"
                ))?
                .query_map(params![mark, TAIL_CHECK], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            let conn = self.writer();
            let mut probe = conn.prepare_cached("SELECT 1 FROM rag_chunks WHERE chunk_id = ?1")?;
            for id in ids {
                if probe.query_row([&id], |_| Ok(())).optional()?.is_none() {
                    missing.push(id);
                }
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let mut conn = self.writer();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for id in &missing {
            tx.execute(
                "INSERT OR IGNORE INTO index_pending (kind, id) VALUES ('chunk', ?1)",
                [id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Whether more than one read's budget of rows waits to be projected.
    pub(super) fn far_behind(&self, state: &Connection) -> Result<bool, Error> {
        let marks = marks(&self.writer())?;
        let (messages, outbound) = max_rowids(state)?;
        Ok((messages - marks.0).max(0) + (outbound - marks.1).max(0) > READ_BUDGET as i64)
    }
}

/// Fill the index in the background until it has caught up with state or the
/// last handle on it is gone. `open_state` opens a state reader.
pub(super) fn spawn_rebuild(
    index: &Arc<MemoryIndex>,
    open_state: impl Fn() -> Option<Connection> + Send + 'static,
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
    tune_connection(&conn);
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
        reset(&conn, false)?;
    }
    // A damaged file fails here and is recreated by the caller.
    conn.query_row(
        "SELECT count(*) FROM rag_chunks_fts WHERE rowid = 0",
        [],
        |r| r.get::<_, i64>(0),
    )?;
    Ok(conn)
}

/// Drop everything and create the current layout, empty, with marks at zero.
/// Unless `force`, a file another process brought to the current layout
/// meanwhile is left as it is.
fn reset(conn: &Connection, force: bool) -> Result<(), Error> {
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
        let names: Vec<(String, String)> = conn
            .prepare(
                "SELECT type, name FROM sqlite_master
                 WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
                   AND name NOT LIKE 'rag_chunks_fts_%'",
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

fn marks(conn: &Connection) -> Result<(i64, i64), Error> {
    let get = |key: &str| -> Result<i64, Error> {
        Ok(conn
            .query_row("SELECT value FROM index_meta WHERE key = ?1", [key], |r| {
                r.get(0)
            })
            .optional()?
            .unwrap_or(0))
    };
    Ok((get("messages_rowid")?, get("outbound_rowid")?))
}

fn max_rowids(state: &Connection) -> Result<(i64, i64), Error> {
    Ok(state.query_row(
        "SELECT (SELECT COALESCE(MAX(rowid), 0) FROM messages),
                (SELECT COALESCE(MAX(rowid), 0) FROM outbound_messages)",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?)
}

/// One source row of a projected chunk: `(rowid, chunk)` fields owned.
struct SourceRow {
    rowid: i64,
    id: String,
    agent_id: String,
    agent_name: String,
    direction: &'static str,
    sender: String,
    destination: String,
    timestamp_ms: i64,
    body: String,
    seq: i64,
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

fn read_rows(
    state: &Connection,
    sql: &str,
    direction: &'static str,
    params: impl rusqlite::Params,
) -> Result<Vec<SourceRow>, Error> {
    let mut stmt = state.prepare_cached(sql)?;
    let rows = stmt
        .query_map(params, |r| {
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
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// The mailbox row a chunk id is projected from, if state has one.
fn source_row(state: &Connection, id: &str) -> Result<Option<SourceRow>, Error> {
    let inbound = read_rows(
        state,
        &format!("{INBOUND_SELECT} WHERE m.id = ?1"),
        "in",
        [id],
    )?;
    if let Some(row) = inbound.into_iter().next() {
        return Ok(Some(row));
    }
    Ok(read_rows(
        state,
        &format!("{OUTBOUND_SELECT} WHERE o.id = ?1"),
        "out",
        [id],
    )?
    .into_iter()
    .next())
}

fn sync_on(conn: &mut Connection, state: &Connection, budget: usize) -> Result<bool, Error> {
    let (messages, outbound) = marks(conn)?;
    let (max_messages, max_outbound) = max_rowids(state)?;
    let pending: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM index_pending)", [], |r| {
        r.get(0)
    })?;
    // Caught up: answered by reads, without the index's write lock.
    if !pending && messages == max_messages && outbound == max_outbound {
        return Ok(true);
    }
    // State moved backwards (restored, replaced): the projection describes
    // rows state no longer has. Start over.
    if messages > max_messages || outbound > max_outbound {
        reset(conn, true)?;
    }
    drain_pending(conn, state)?;
    let mut left = budget;
    for (key, select, rowid, direction) in [
        ("messages_rowid", INBOUND_SELECT, "m.rowid", "in"),
        ("outbound_rowid", OUTBOUND_SELECT, "o.rowid", "out"),
    ] {
        loop {
            if left == 0 {
                return Ok(false);
            }
            let take = left.min(BATCH);
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mark: i64 = tx
                .query_row("SELECT value FROM index_meta WHERE key = ?1", [key], |r| {
                    r.get(0)
                })
                .optional()?
                .unwrap_or(0);
            // Rows whose agent is gone have no owner to file them under;
            // they are skipped by the join and covered by the mark.
            let (max,): (i64,) = state.query_row(
                &format!(
                    "SELECT COALESCE(MAX(rowid), 0) FROM (SELECT rowid FROM {} WHERE rowid > ?1 ORDER BY rowid LIMIT ?2)",
                    if direction == "in" { "messages" } else { "outbound_messages" }
                ),
                params![mark, take as i64],
                |r| Ok((r.get(0)?,)),
            )?;
            if max <= mark {
                tx.commit()?;
                break;
            }
            let rows = read_rows(
                state,
                &format!("{select} WHERE {rowid} > ?1 AND {rowid} <= ?2 ORDER BY {rowid}"),
                direction,
                params![mark, max],
            )?;
            for row in &rows {
                upsert_chunk(&tx, &row.chunk())?;
            }
            debug_assert!(rows.iter().all(|r| r.rowid <= max));
            tx.execute(
                "UPDATE index_meta SET value = ?2 WHERE key = ?1",
                params![key, max],
            )?;
            tx.commit()?;
            left = left.saturating_sub(take);
        }
    }
    Ok(true)
}

fn drain_pending(conn: &mut Connection, state: &Connection) -> Result<(), Error> {
    loop {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending: Vec<(String, String)> = tx
            .prepare_cached("SELECT kind, id FROM index_pending LIMIT 256")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?;
        if pending.is_empty() {
            tx.commit()?;
            return Ok(());
        }
        for (kind, id) in &pending {
            match kind.as_str() {
                // A standalone summary keeps its row; a mailbox row leaves
                // state only with its agent, below.
                "chunk" => {
                    if let Some(row) = source_row(state, id)? {
                        upsert_chunk(&tx, &row.chunk())?;
                    }
                }
                "agent" => {
                    let name: Option<String> = state
                        .query_row("SELECT name FROM agents WHERE id = ?1", [id], |r| r.get(0))
                        .optional()?;
                    match name {
                        Some(name) => {
                            rewrite_agent_fts_rows(&tx, id, Some(&name))?;
                            tx.execute(
                                "UPDATE rag_chunks SET agent_name = ?2 WHERE agent_id = ?1 AND agent_name != ?2",
                                params![id, name],
                            )?;
                        }
                        None => {
                            forget_agent(&tx, id)?;
                            let (messages, outbound) = max_rowids(state)?;
                            for (key, max) in
                                [("messages_rowid", messages), ("outbound_rowid", outbound)]
                            {
                                tx.execute(
                                    "UPDATE index_meta SET value = MIN(value, ?2) WHERE key = ?1",
                                    params![key, max],
                                )?;
                            }
                        }
                    }
                }
                _ => {}
            }
            tx.execute(
                "DELETE FROM index_pending WHERE kind = ?1 AND id = ?2",
                params![kind, id],
            )?;
        }
        tx.commit()?;
    }
}

fn forget_agent(conn: &Connection, agent_id: &str) -> Result<(), Error> {
    rewrite_agent_fts_rows(conn, agent_id, None)?;
    conn.execute("DELETE FROM rag_chunks WHERE agent_id = ?1", [agent_id])?;
    Ok(())
}

/// Write one chunk and its full-text row, replacing any earlier projection.
/// A turn with fewer than two characters of text is not searchable and
/// leaves no row.
pub(super) fn upsert_chunk(conn: &Connection, chunk: &Chunk<'_>) -> Result<(), Error> {
    let cleaned = clean_rag_text(chunk.text);
    remove_chunk(conn, chunk.id)?;
    if cleaned.chars().count() < 2 {
        return Ok(());
    }
    let (kind, tool_name) = classify_chunk_kind_and_tool(
        chunk.kind,
        chunk.tool,
        chunk.sender,
        chunk.destination,
        &cleaned,
    );
    conn.prepare_cached(
        "INSERT INTO rag_chunks (
            chunk_id, agent_id, agent_name, direction, sender, destination,
            timestamp_ms, content_hash, text, kind, tool_name, seq
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
    )?
    .execute(params![
        chunk.id,
        chunk.agent_id,
        chunk.agent_name,
        chunk.direction,
        chunk.sender,
        chunk.destination,
        chunk.timestamp_ms,
        rag_content_hash(&cleaned),
        cleaned,
        kind,
        tool_name,
        chunk.seq,
    ])?;
    if chunk.seq <= 0 {
        conn.execute(
            "UPDATE rag_chunks SET seq = rowid WHERE chunk_id = ?1 AND seq <= 0",
            [chunk.id],
        )?;
    }
    conn.prepare_cached(
        "INSERT INTO rag_chunks_fts (
            chunk_id, agent_name, sender, destination, kind, tool_name, text, search_tokens
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )?
    .execute(params![
        chunk.id,
        chunk.agent_name,
        chunk.sender,
        chunk.destination,
        kind,
        tool_name,
        cleaned,
        tokenize_hybrid_search_tokens(&cleaned).join(" "),
    ])?;
    Ok(())
}

/// Remove one chunk and its full-text row (and any duplicate of it).
pub(super) fn remove_chunk(conn: &Connection, chunk_id: &str) -> Result<(), Error> {
    match fts_chunk_lookup(chunk_id) {
        Some(lookup) => conn
            .prepare_cached(FTS_DELETE_CHUNK)?
            .execute(params![chunk_id, lookup])?,
        None => conn.execute("DELETE FROM rag_chunks_fts WHERE chunk_id = ?1", [chunk_id])?,
    };
    conn.prepare_cached("DELETE FROM rag_chunks WHERE chunk_id = ?1")?
        .execute([chunk_id])?;
    Ok(())
}

/// The full-text index (`rag_chunks_fts`, FTS5) answers only `rowid` and
/// `MATCH` constraints from its index: a plain `column = ?` filter reads every
/// row. Every statement that maintains it finds its rows through `MATCH` and
/// keeps the result exact with `=`.
///
/// One chunk's row, through the chunk id's own token (see
/// [`fts_chunk_lookup`]). `?1` the chunk id, `?2` its lookup phrase.
pub(crate) const FTS_DELETE_CHUNK: &str =
    "DELETE FROM rag_chunks_fts WHERE chunk_id MATCH ?2 AND chunk_id = ?1";

/// One agent's rows filed under one name: `MATCH` on the name, `=` for the
/// exact name, and only chunks `rag_chunks` gives the agent. `?1` the name,
/// `?2` its phrase, `?3` the agent id.
pub(crate) const FTS_DELETE_AGENT_ROWS: &str = "DELETE FROM rag_chunks_fts \
     WHERE agent_name MATCH ?2 AND agent_name = ?1 \
       AND chunk_id IN (SELECT chunk_id FROM rag_chunks WHERE agent_id = ?3)";

/// [`FTS_DELETE_AGENT_ROWS`]' rows renamed to `?4`.
pub(crate) const FTS_RENAME_AGENT_ROWS: &str = "UPDATE rag_chunks_fts SET agent_name = ?4 \
     WHERE agent_name MATCH ?2 AND agent_name = ?1 \
       AND chunk_id IN (SELECT chunk_id FROM rag_chunks WHERE agent_id = ?3)";

/// One chunk's row renamed to `?3`, found as in [`FTS_DELETE_CHUNK`].
pub(crate) const FTS_RENAME_CHUNK: &str =
    "UPDATE rag_chunks_fts SET agent_name = ?3 WHERE chunk_id MATCH ?2 AND chunk_id = ?1";

/// `value` as one FTS5 phrase: quoted, with any quote doubled.
pub(crate) fn fts_exact_phrase(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// The tokens the index's `unicode61` tokenizer makes of `value`: runs of
/// letters and digits (everything else separates).
fn fts_tokens(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
}

/// The phrase that finds one chunk's row: its id's longest token, the unique
/// part. The whole id (`msg_<id>`, `out_<hash>`) starts with a token most of
/// the index shares, and a phrase with it reads that token's doclist.
/// `None` for an id without a token, which only a full scan can find.
pub(crate) fn fts_chunk_lookup(chunk_id: &str) -> Option<String> {
    fts_tokens(chunk_id)
        .max_by_key(|token| token.len())
        .map(fts_exact_phrase)
}

/// Remove an agent's full-text rows, or (`rename_to`) refile them under a
/// new name. Work is proportional to the agent's own rows: one indexed
/// statement per name its chunks are filed under, chunk by chunk only for a
/// name that has no token to match.
pub(super) fn rewrite_agent_fts_rows(
    conn: &Connection,
    agent_id: &str,
    rename_to: Option<&str>,
) -> Result<usize, Error> {
    let names: Vec<String> = conn
        .prepare_cached("SELECT DISTINCT agent_name FROM rag_chunks WHERE agent_id = ?1")?
        .query_map([agent_id], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    let mut changed = 0;
    for name in names {
        if rename_to == Some(name.as_str()) {
            continue;
        }
        if fts_tokens(&name).next().is_some() {
            let phrase = fts_exact_phrase(&name);
            changed += match rename_to {
                None => conn.execute(FTS_DELETE_AGENT_ROWS, params![name, phrase, agent_id])?,
                Some(new) => {
                    conn.execute(FTS_RENAME_AGENT_ROWS, params![name, phrase, agent_id, new])?
                }
            };
            continue;
        }
        let ids: Vec<String> = conn
            .prepare_cached(
                "SELECT chunk_id FROM rag_chunks WHERE agent_id = ?1 AND agent_name = ?2",
            )?
            .query_map(params![agent_id, name], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        for chunk_id in ids {
            changed += match (rename_to, fts_chunk_lookup(&chunk_id)) {
                (None, Some(lookup)) => {
                    conn.execute(FTS_DELETE_CHUNK, params![chunk_id, lookup])?
                }
                (None, None) => conn.execute(
                    "DELETE FROM rag_chunks_fts WHERE chunk_id = ?1",
                    [&chunk_id],
                )?,
                (Some(new), Some(lookup)) => {
                    conn.execute(FTS_RENAME_CHUNK, params![chunk_id, lookup, new])?
                }
                (Some(new), None) => conn.execute(
                    "UPDATE rag_chunks_fts SET agent_name = ?2 WHERE chunk_id = ?1",
                    params![chunk_id, new],
                )?,
            };
        }
    }
    Ok(changed)
}
