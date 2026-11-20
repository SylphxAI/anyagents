//! Rebuildable knowledge partition in memory-index.db. Revisions are the
//! durable change stream; advancing the cursor and updating FTS are one index
//! transaction. Knowledge and mailbox writes never wait for this work.

use rusqlite::Connection;

use super::search::{indexed_text, search_tokens};
use super::{DOC, Doc, Store, read_doc};
use crate::db::{DbExec, DbExecExt, DbTx};
use crate::store::{ReadConn, rag_content_hash, registry};
use crate::{Error, db_params};

const BATCH: i64 = 128;
const MARK: &str = "knowledge_revision_seq";

pub(in crate::store) fn create(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS knowledge_rag_chunks (
            rowid INTEGER PRIMARY KEY,
            chunk_id TEXT NOT NULL UNIQUE,
            version INTEGER NOT NULL,
            content_hash TEXT NOT NULL
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_rag_chunks_fts
            USING fts5(search_tokens, tokenize='unicode61');
        INSERT INTO knowledge_rag_chunks_fts(knowledge_rag_chunks_fts, rank)
            VALUES ('secure-delete', 1);",
    )?;
    Ok(())
}

/// Add the partition to an already-current mailbox index without rebuilding it.
pub(in crate::store) fn initialize(conn: &Connection) -> Result<(), Error> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'knowledge_rag_chunks_fts')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        let tx = conn.unchecked_transaction()?;
        create(&tx)?;
        tx.commit()?;
    }
    Ok(())
}

fn mark(index: &impl DbExec, schema: &str) -> Result<i64, Error> {
    Ok(index
        .db_query_opt(
            &format!("SELECT value FROM {schema}.index_meta WHERE key = ?1"),
            &db_params![MARK],
            |r| r.get(0),
        )?
        .unwrap_or(0))
}

fn project(tx: &DbTx<'_>, doc: &Doc) -> Result<(), Error> {
    let prior: Option<(i64, String, i64)> = tx.db_query_opt(
        "SELECT rowid, content_hash, version FROM knowledge_rag_chunks WHERE chunk_id = ?1",
        &db_params![doc.id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    if doc.deleted_at_ms.is_some() {
        if let Some((rowid, _, _)) = prior {
            tx.db_execute(
                "DELETE FROM knowledge_rag_chunks_fts WHERE rowid = ?1",
                &db_params![rowid],
            )?;
            tx.db_execute(
                "DELETE FROM knowledge_rag_chunks WHERE rowid = ?1",
                &db_params![rowid],
            )?;
        }
        return Ok(());
    }
    if prior
        .as_ref()
        .is_some_and(|(_, _, version)| *version == doc.version)
    {
        return Ok(());
    }
    let text = indexed_text(&doc.topic, &doc.title, &doc.description, &doc.body);
    let hash = rag_content_hash(&text);
    if let Some((rowid, ref old_hash, _)) = prior {
        if old_hash == &hash {
            tx.db_execute(
                "UPDATE knowledge_rag_chunks SET version = ?1 WHERE rowid = ?2",
                &db_params![doc.version, rowid],
            )?;
            return Ok(());
        }
        tx.db_execute(
            "DELETE FROM knowledge_rag_chunks_fts WHERE rowid = ?1",
            &db_params![rowid],
        )?;
    }
    let rowid: i64 = tx.db_query_one(
        "INSERT INTO knowledge_rag_chunks(chunk_id, version, content_hash) VALUES (?1, ?2, ?3)
        ON CONFLICT(chunk_id) DO UPDATE SET version = excluded.version, content_hash = excluded.content_hash
        RETURNING rowid",
        &db_params![doc.id, doc.version, hash],
        |r| r.get(0),
    )?;
    tx.db_execute(
        "INSERT INTO knowledge_rag_chunks_fts(rowid, search_tokens) VALUES (?1, ?2)",
        &db_params![rowid, search_tokens(&text)],
    )?;
    Ok(())
}

impl Store {
    pub(super) fn knowledge_search_reader(&self) -> Result<ReadConn<'_>, Error> {
        match &self.backend {
            registry::Backend::Sqlite(shared) => shared.reader().ok_or_else(|| {
                Error::Message("could not open the SQLite knowledge search reader".into())
            }),
            registry::Backend::Postgres(_) => Ok(self.read_conn()),
        }
    }

    pub(super) fn sync_knowledge_sqlite(&self, state: &ReadConn<'_>) -> Result<(), Error> {
        // Capture the committed horizon once: continuous writes cannot extend
        // this search's replay. No state writer is acquired, even on failure.
        let through: i64 = state.db_query_one(
            "SELECT COALESCE(MAX(projection_seq), 0) FROM knowledge_revisions",
            &[],
            |r| r.get(0),
        )?;
        let mut current = mark(state, "memory_index")?;
        while current < through {
            current = self.memory_index().run_write_tx(true, |tx| {
                // Re-read under the index write lock: a concurrent reader may
                // already have projected this batch and advanced the cursor.
                let current = mark(tx, "main")?;
                if current >= through {
                    return Ok(current);
                }
                let rows = state.db_query_vec(
                    &format!("WITH batch AS (
                         SELECT projection_seq, doc_id FROM knowledge_revisions
                         WHERE projection_seq > ?1 AND projection_seq <= ?2
                         ORDER BY projection_seq LIMIT ?3
                     ), latest AS (
                         SELECT doc_id, MAX(projection_seq) AS seq FROM batch GROUP BY doc_id
                     )
                     SELECT {DOC}, latest.seq FROM latest JOIN knowledge_docs d ON d.id = latest.doc_id
                     ORDER BY latest.seq"),
                    &db_params![current, through, BATCH],
                    |r| Ok((read_doc(r)?, r.get::<i64>(13)?)),
                )?;
                let mut next = current;
                for (doc, seq) in rows {
                    project(tx, &doc)?;
                    next = seq;
                }
                if next == current {
                    return Err(Error::Message("knowledge projection could not advance".into()));
                }
                tx.db_execute(
                    "INSERT INTO index_meta(key, value) VALUES (?1, ?2)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    &db_params![MARK, next],
                )?;
                Ok(next)
            })?.expect("blocking index transaction");
        }
        Ok(())
    }
}
