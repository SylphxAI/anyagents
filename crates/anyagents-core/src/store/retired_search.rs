//! Dismantling the state database's own pre-split search tables.
//!
//! Migration 035 (`035_memory_index_split.sql`) moves the mailbox search
//! index to `memory-index.db` and renames state's old copies to
//! `retired_rag_chunks` and `retired_rag_chunks_fts` instead of dropping
//! them: freeing their pages reads every one of them, which on a large store
//! takes far longer than a server may stay unready. They are dismantled
//! here, after the server answers, in short transactions that each hold the
//! state writer briefly:
//!
//! 1. Each outbound turn whose mailbox position only its retired chunk held
//!    gets it on its own row (`outbound_messages.seq`). Until then the
//!    projection reads it from the retired chunk
//!    (`memory_index::OUTBOUND_SELECT_RETIRING`).
//! 2. The retired rows are deleted in batches: the full-text table's stored
//!    bodies, sizes and index segments first, then the chunks.
//! 3. The emptied tables are dropped, which no longer has pages to visit.
//!
//! Nothing reads the retired tables except step 1 and the projection, so
//! deleting the full-text table's rows directly from its storage tables
//! (never through the full-text module) is safe: no query of the table
//! itself runs again before it is dropped. Every step reads its progress
//! from the database, so a process that stops part way is resumed by the
//! next one that opens the store; the standalone summaries only these tables
//! held were copied to the index before the migration (`adopt_legacy`).

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::registry::Shared;
use crate::db::{DbBackend, DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;

/// The retired chunk table.
const CHUNKS: &str = "retired_rag_chunks";
/// The retired full-text table.
const FTS: &str = "retired_rag_chunks_fts";
/// The retired full-text table's rowid storage tables that hold its bulk, in
/// the order they are emptied (stored bodies, sizes, then index segments).
/// Its small `_idx` and `_config` tables go with the table.
const FTS_STORAGE: [&str; 3] = [
    "retired_rag_chunks_fts_content",
    "retired_rag_chunks_fts_docsize",
    "retired_rag_chunks_fts_data",
];
/// Outbound rows given their position per transaction.
const BACKFILL_BATCH: i64 = 2048;
/// Retired rows deleted per transaction.
const DELETE_BATCH: i64 = 256;
/// The least pause between two transactions; each also pauses at least as
/// long as it held the writer, so this work never holds it more than half
/// the time.
const MIN_PAUSE: Duration = Duration::from_millis(20);

/// Whether state still has its retired chunk table.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn present(conn: &Connection) -> Result<bool, Error> {
    present_db(conn)
}

pub(super) fn present_db(conn: &(impl DbExec + ?Sized)) -> Result<bool, Error> {
    if conn.backend() == DbBackend::Postgres {
        return Ok(false);
    }
    conn.db_query_one(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?1)",
        &db_params![CHUNKS],
        |r| r.get(0),
    )
}

/// Whether anything retired is left to dismantle.
pub(super) fn pending(conn: &(impl DbExec + ?Sized)) -> Result<bool, Error> {
    if conn.backend() == DbBackend::Postgres {
        return Ok(false);
    }
    conn.db_query_one(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name IN (?1, ?2))",
        &db_params![CHUNKS, FTS],
        |r| r.get(0),
    )
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool, Error> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?1)",
        [name],
        |r| r.get(0),
    )?)
}

fn has_rows(conn: &Connection, table: &str) -> Result<bool, Error> {
    Ok(table_exists(conn, table)?
        && conn
            .query_row(
                &format!("SELECT 1 FROM main.\"{table}\" LIMIT 1"),
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
}

/// Progress of one process's pass.
#[derive(Debug, Default)]
pub(super) struct Retirement {
    /// Outbound rowids up to here have their position.
    backfilled: i64,
    /// The highest outbound rowid when this pass began; a row written after
    /// the migration gets its position when it is inserted.
    backfill_end: Option<i64>,
}

/// What one [`Retirement::step`] left.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Step {
    More,
    Done,
}

impl Retirement {
    /// Run one short transaction of the work left.
    pub(super) fn step(&mut self, conn: &mut Connection) -> Result<Step, Error> {
        if !pending(conn)? {
            return Ok(Step::Done);
        }
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if table_exists(&tx, CHUNKS)? {
            let end = match self.backfill_end {
                Some(end) => end,
                None => {
                    let end = tx.query_row(
                        "SELECT COALESCE(MAX(rowid), 0) FROM outbound_messages",
                        [],
                        |r| r.get(0),
                    )?;
                    *self.backfill_end.insert(end)
                }
            };
            if self.backfilled < end {
                let upto = self.backfilled.saturating_add(BACKFILL_BATCH).min(end);
                tx.execute(
                    &format!(
                        "UPDATE outbound_messages
                         SET seq = (SELECT c.seq FROM {CHUNKS} c WHERE c.chunk_id = outbound_messages.id)
                         WHERE rowid > ?1 AND rowid <= ?2 AND seq = 0
                           AND EXISTS (SELECT 1 FROM {CHUNKS} c
                                       WHERE c.chunk_id = outbound_messages.id AND c.seq != 0)"
                    ),
                    params![self.backfilled, upto],
                )?;
                tx.commit()?;
                self.backfilled = upto;
                return Ok(Step::More);
            }
        }
        for table in FTS_STORAGE.into_iter().chain([CHUNKS]) {
            if has_rows(&tx, table)? {
                tx.execute(
                    &format!(
                        "DELETE FROM main.\"{table}\" WHERE rowid IN
                         (SELECT rowid FROM main.\"{table}\" ORDER BY rowid LIMIT ?1)"
                    ),
                    [DELETE_BATCH],
                )?;
                tx.commit()?;
                return Ok(Step::More);
            }
        }
        tx.execute_batch(&format!(
            "DROP TABLE IF EXISTS main.{FTS}; DROP TABLE IF EXISTS main.{CHUNKS};"
        ))?;
        tx.commit()?;
        Ok(Step::Done)
    }
}

/// Dismantle what is retired on this open: at once when the retired tables
/// hold no document (a store that never held mail; only the full-text
/// table's fixed structure records are left to drop), otherwise in the
/// background, sharing the store's writer between short pauses, while any
/// handle on the store is alive.
pub(super) fn start(shared: &Arc<Shared>, in_memory: bool) -> Result<(), Error> {
    {
        let mut conn = shared.writer();
        if !pending(&*conn)? {
            return Ok(());
        }
        let holds_documents = has_rows(&conn, CHUNKS)?
            || has_rows(&conn, "retired_rag_chunks_fts_content")?
            || has_rows(&conn, "retired_rag_chunks_fts_docsize")?;
        if !holds_documents || in_memory {
            return finish(&mut conn);
        }
    }
    let alive = Arc::downgrade(shared);
    std::thread::Builder::new()
        .name("retired-search".into())
        .spawn(move || run(&alive))
        .map_err(Error::from)?;
    Ok(())
}

/// Run what is left to completion on `conn`.
pub(super) fn finish(conn: &mut Connection) -> Result<(), Error> {
    let mut pass = Retirement::default();
    while pass.step(conn)? == Step::More {}
    Ok(())
}

fn run(alive: &Weak<Shared>) {
    let started = Instant::now();
    eprintln!("anyagents-store: dismantling the pre-split search tables in the background");
    let mut pass = Retirement::default();
    let mut failures = 0u32;
    // Never holds the store across sleeps: the last handle dropping ends this.
    while let Some(shared) = alive.upgrade() {
        let (outcome, held) = {
            let mut conn = shared.writer();
            let began = Instant::now();
            let outcome = pass.step(&mut conn);
            (outcome, began.elapsed())
        };
        drop(shared);
        match outcome {
            Ok(Step::Done) => {
                eprintln!(
                    "anyagents-store: pre-split search tables dismantled in {}s",
                    started.elapsed().as_secs()
                );
                return;
            }
            Ok(Step::More) => {
                failures = 0;
                std::thread::sleep(held.max(MIN_PAUSE));
            }
            // Another writer holds the lock: wait for it.
            Err(error) if error.is_transient_lock_error() => {
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(error) => {
                failures += 1;
                if failures > 5 {
                    eprintln!(
                        "anyagents-store: dismantling the pre-split search tables stopped ({error}); \
                         the next open resumes it"
                    );
                    return;
                }
                std::thread::sleep(Duration::from_secs(1 << failures));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate;

    /// A state database at the last pre-split version, with one inbound and
    /// one outbound turn, the outbound turn's position held only by its
    /// chunk, a standalone summary, and `extra` more chunks with full-text rows.
    fn pre_split(extra: i64) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
        )
        .unwrap();
        for (version, sql) in migrate::MIGRATIONS
            .iter()
            .filter(|(v, _)| *v < migrate::MEMORY_INDEX_SPLIT)
        {
            conn.execute_batch(sql).unwrap();
            conn.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
                .unwrap();
        }
        conn.execute_batch(
            "INSERT INTO agents (id, name, harness, state, created_at_ms)
                 VALUES ('agt_old', 'veteran', 'test', 'idle', 0);
             INSERT INTO messages (id, agent_id, seq, body, received_at_ms)
                 VALUES ('msg_old1', 'agt_old', 1, 'inbound body', 1);
             INSERT INTO outbound_messages (id, agent_id, destination, body, sent_at_ms)
                 VALUES ('out_old1', 'agt_old', 'peer', 'outbound body', 2),
                        ('out_old2', 'agt_old', 'peer', 'unplaced body', 3);
             INSERT INTO rag_chunks (chunk_id, agent_id, agent_name, direction, sender,
                     destination, timestamp_ms, content_hash, text, kind, tool_name, seq)
                 VALUES ('msg_old1', 'agt_old', 'veteran', 'in', 'peer', 'agent:veteran', 1,
                         'h', 'inbound body', 'message', '', 1),
                        ('out_old1', 'agt_old', 'veteran', 'out', 'agent:veteran', 'peer', 2,
                         'h', 'outbound body', 'message', '', 7),
                        ('wk_ses_1', 'agt_old', 'veteran', 'out', 'agent:veteran', 'peer', 3,
                         'h', 'standalone body', 'message', '', 2);",
        )
        .unwrap();
        for n in 0..extra {
            conn.execute(
                "INSERT INTO rag_chunks (chunk_id, agent_id, agent_name, direction, sender,
                     destination, timestamp_ms, content_hash, text, kind, tool_name, seq)
                 VALUES (?1, 'agt_old', 'veteran', 'in', 'peer', 'agent:veteran', ?2, 'h',
                         'filler body', 'message', '', ?2)",
                params![format!("msg_filler{n}"), n + 10],
            )
            .unwrap();
        }
        conn.execute_batch(
            "INSERT INTO rag_chunks_fts (chunk_id, agent_name, sender, destination, kind,
                 tool_name, text, search_tokens)
             SELECT chunk_id, agent_name, sender, destination, kind, tool_name, text, text
             FROM rag_chunks;",
        )
        .unwrap();
        conn
    }

    fn seq_of(conn: &Connection, id: &str) -> i64 {
        conn.query_row(
            "SELECT seq FROM outbound_messages WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn standalone_chunks_are_found_from_the_chunk_id_key_alone() {
        let conn = pre_split(0);
        let plan: Vec<String> = conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                super::super::memory_index::LEGACY_STANDALONE_IDS
            ))
            .unwrap()
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|step| step.contains("SCAN r USING COVERING INDEX")),
            "{plan:?}"
        );
        let ids: Vec<String> = conn
            .prepare(&format!(
                "SELECT chunk_id FROM rag_chunks WHERE rowid IN ({})",
                super::super::memory_index::LEGACY_STANDALONE_IDS
            ))
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(ids, ["wk_ses_1"]);
    }

    #[test]
    fn the_split_migration_renames_and_the_retirement_moves_positions_then_drops() {
        let mut conn = pre_split(3 * DELETE_BATCH);
        migrate::migrate(&mut conn).unwrap();
        // The migration only renamed: every retired row is still there.
        let retired: i64 = conn
            .query_row("SELECT count(*) FROM retired_rag_chunks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(retired, 3 + 3 * DELETE_BATCH);
        assert!(present(&conn).unwrap() && pending(&conn).unwrap());
        assert_eq!(seq_of(&conn, "out_old1"), 0);
        // Until it is moved, the projection reads the position from the
        // retired chunk; a turn without one keeps none.
        let read = |conn: &Connection, id: &str| {
            super::super::memory_index::read_source_rows(
                conn,
                "out",
                "WHERE o.id = ?1",
                &crate::db_params![id],
            )
            .unwrap()
            .remove(0)
        };
        assert_eq!(read(&conn, "out_old1").seq, 7);
        assert_eq!(read(&conn, "out_old2").seq, 0);

        let mut pass = Retirement::default();
        assert_eq!(pass.step(&mut conn).unwrap(), Step::More);
        assert_eq!(seq_of(&conn, "out_old1"), 7);
        assert_eq!(seq_of(&conn, "out_old2"), 0);
        let mut steps = 1;
        while pass.step(&mut conn).unwrap() == Step::More {
            steps += 1;
            assert!(steps < 100, "retirement does not finish");
        }
        // Bounded batches: several transactions, each deleting at most a batch.
        assert!(steps > 3, "{steps}");
        assert!(!present(&conn).unwrap() && !pending(&conn).unwrap());
        let left: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name LIKE 'retired%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
        // Positions survive the retired chunks, and the projection no longer
        // needs them.
        assert_eq!(seq_of(&conn, "out_old1"), 7);
        assert_eq!(read(&conn, "out_old1").seq, 7);
        // A later pass has nothing to do.
        assert_eq!(Retirement::default().step(&mut conn).unwrap(), Step::Done);
    }
}
