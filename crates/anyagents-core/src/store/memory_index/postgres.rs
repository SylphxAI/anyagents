//! PostgreSQL projection and incremental repair.

use super::{
    BATCH, drain_pending_step, marks, max_rowids, queue_chunks, read_source_rows, upsert_chunk,
};
use crate::db::{DbExec, DbExecExt, DbTx, PgTx};
use crate::db_params;
use crate::error::Error;

// A restore can leave rows whose canonical mailbox entries no longer exist.
// Scan a bounded rowid window per commit and keep a durable cursor so a crash
// or competing replica can resume. Surviving rows are re-projected by the
// normal marks below: unchanged rows don't incur a delete and re-insert.
pub(super) fn reset_cursor(conn: &(impl DbExec + ?Sized)) -> Result<Option<i64>, Error> {
    conn.db_query_opt(
        "SELECT value FROM index_meta WHERE key = 'pg_reset_after'",
        &[],
        |r| r.get(0),
    )
}

fn reset_step(tx: &DbTx<'_>, after: i64, take: usize) -> Result<bool, Error> {
    let last: Option<i64> = tx.db_query_one(
        "SELECT MAX(rowid) FROM (
             SELECT rowid FROM memory_index.rag_chunks WHERE rowid > ?1
             ORDER BY rowid LIMIT ?2
         ) AS batch",
        &db_params![after, take as i64],
        |r| r.get(0),
    )?;
    let Some(last) = last else {
        tx.db_execute(
            "UPDATE index_meta SET value = 0 WHERE key IN ('messages_rowid', 'outbound_rowid')",
            &[],
        )?;
        tx.db_execute("DELETE FROM index_meta WHERE key = 'pg_reset_after'", &[])?;
        return Ok(false);
    };
    tx.db_execute(
        "DELETE FROM memory_index.rag_chunks AS c
         WHERE c.rowid > ?1 AND c.rowid <= ?2
           AND (NOT EXISTS (SELECT 1 FROM public.agents AS a WHERE a.id = c.agent_id)
                OR (left(c.chunk_id, 5) <> 'tool_'
                    AND NOT EXISTS (SELECT 1 FROM public.messages AS m WHERE m.id = c.chunk_id)
                    AND NOT EXISTS (SELECT 1 FROM public.outbound_messages AS o WHERE o.id = c.chunk_id)))",
        &db_params![after, last],
    )?;
    tx.db_execute(
        "INSERT INTO index_meta (key, value) VALUES ('pg_reset_after', ?1)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        &db_params![last],
    )?;
    Ok(true)
}

pub(super) fn sync_on_postgres(
    pool: &sqlx::PgPool,
    state: &(impl DbExec + ?Sized),
    deferred: &[String],
    wait: bool,
    budget: usize,
) -> Result<Option<bool>, Error> {
    if deferred.is_empty() {
        let (messages, outbound) = marks(pool)?;
        let (max_messages, max_outbound) = max_rowids(state)?;
        let pending: bool =
            pool.db_query_one("SELECT EXISTS(SELECT 1 FROM index_pending)", &[], |r| {
                r.get(0)
            })?;
        if !pending
            && messages == max_messages
            && outbound == max_outbound
            && reset_cursor(pool)?.is_none()
        {
            return Ok(Some(true));
        }
    }
    // Acquire advisory sync lock per-batch so other pods aren't blocked for the whole rebuild.
    if !deferred.is_empty() {
        let queued = with_pg_sync_lock(pool, wait, |tx| queue_chunks(tx, deferred))?;
        if queued.is_none() {
            return Ok(None);
        }
    }
    loop {
        let Some(drained_more) = with_pg_sync_lock(pool, wait, |tx| drain_pending_step(tx, state))?
        else {
            return Ok(None);
        };
        if !drained_more {
            break;
        }
    }
    let mut left = budget;
    while left > 0 {
        let take = left.min(BATCH);
        let Some(more) = with_pg_sync_lock(pool, wait, |tx| {
            if let Some(after) = reset_cursor(tx)? {
                return reset_step(tx, after, take);
            }
            let (messages, outbound) = marks(tx)?;
            let (max_messages, max_outbound) = max_rowids(state)?;
            if messages > max_messages || outbound > max_outbound {
                return reset_step(tx, 0, take);
            }
            Ok(false)
        })?
        else {
            return Ok(None);
        };
        if !more {
            break;
        }
        left -= take;
    }

    for (key, rowid, direction) in [
        ("messages_rowid", "m.rowid", "in"),
        ("outbound_rowid", "o.rowid", "out"),
    ] {
        loop {
            if left == 0 {
                return Ok(Some(false));
            }
            let take = left.min(BATCH);
            let Some(advanced) = with_pg_sync_lock(pool, wait, |tx| {
                if reset_cursor(tx)?.is_some() {
                    return Ok(None);
                }
                let mark: i64 = tx
                    .db_query_opt(
                        "SELECT value FROM index_meta WHERE key = ?1",
                        &db_params![key],
                        |r| r.get(0),
                    )?
                    .unwrap_or(0);
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
                    return Ok(Some(0));
                }
                let rows = read_source_rows(
                    state,
                    direction,
                    &format!("WHERE {rowid} > ?1 AND {rowid} <= ?2 ORDER BY {rowid}"),
                    &db_params![mark, max],
                )?;
                for row in &rows {
                    upsert_chunk(tx, &row.chunk())?;
                }
                tx.db_execute(
                    "UPDATE index_meta SET value = ?2 WHERE key = ?1",
                    &db_params![key, max],
                )?;
                // Sparse ranges may contain fewer rows than the requested batch.
                // Charge at least one unit if the range disappeared before the read.
                Ok(Some(rows.len().max(1)))
            })?
            else {
                return Ok(None);
            };
            match advanced {
                None => return Ok(Some(false)),
                Some(0) => break,
                Some(written) => left = left.saturating_sub(written),
            }
        }
    }
    Ok(Some(true))
}

fn with_pg_sync_lock<T>(
    pool: &sqlx::PgPool,
    wait: bool,
    f: impl FnOnce(&DbTx<'_>) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    let tx = PgTx::begin(pool)?;
    if !wait {
        let got: bool =
            tx.db_query_one("SELECT pg_try_advisory_xact_lock(78697969)", &[], |r| {
                r.get(0)
            })?;
        if !got {
            return Ok(None);
        }
    } else {
        let _: () = tx.db_query_one("SELECT pg_advisory_xact_lock(78697969)", &[], |_| Ok(()))?;
    }
    let wrapped = DbTx::Postgres(tx);
    let res = f(&wrapped)?;
    if let DbTx::Postgres(tx) = wrapped {
        tx.commit()?;
    }
    Ok(Some(res))
}

#[cfg(test)]
mod tests;
