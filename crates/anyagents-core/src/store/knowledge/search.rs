//! Knowledge uses a separate RAG partition so legacy mailbox readers (including
//! pre-upgrade processes) cannot retrieve it without namespace authorization.
use serde::Serialize;

use super::{Actor, Doc, Error, Scope, Store, db_params, invalid, require_scope, visibility};
use crate::db::{DbBackend, DbExec, DbExecExt, DbTx};
use crate::store::index_sync::{
    cjk_phrase_tokens, cjk_runs, is_cjk_char, tokenize_hybrid_search_tokens,
};
use crate::store::rag_content_hash;

#[derive(Clone, Debug, Serialize)]
pub struct SearchHit {
    pub namespace: String,
    pub topic: String,
    pub version: i64,
    pub title: String,
    pub description: String,
}

fn indexed_text(doc: &Doc) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        doc.topic, doc.title, doc.description, doc.body
    )
}

// Store CJK phrase bigrams in uninterrupted order. Unigrams and ASCII terms
// are separate so a single-character query does not break phrase matching.
fn search_tokens(text: &str) -> String {
    let mut tokens = tokenize_hybrid_search_tokens(text);
    for run in cjk_runs(text) {
        tokens.push("zzragboundaryzz".into());
        tokens.extend(cjk_phrase_tokens(run));
    }
    tokens.join(" ")
}

pub(super) fn project(tx: &DbTx<'_>, doc: &Doc) -> Result<(), Error> {
    let sqlite = tx.backend() == DbBackend::Sqlite;
    let table = if sqlite {
        "knowledge_rag_chunks"
    } else {
        "knowledge_index.rag_chunks"
    };
    if doc.deleted_at_ms.is_some() {
        let changed = tx.db_execute(
            &format!("DELETE FROM {table} WHERE chunk_id = ?1"),
            &db_params![doc.id],
        )?;
        if sqlite && changed != 0 {
            tx.db_execute(
                "DELETE FROM knowledge_rag_chunks_fts WHERE chunk_id = ?1",
                &db_params![doc.id],
            )?;
        }
        return Ok(());
    }
    let text = indexed_text(doc);
    let hash = rag_content_hash(&text);
    let prior: Option<String> = tx.db_query_opt(
        &format!("SELECT content_hash FROM {table} WHERE chunk_id = ?1"),
        &db_params![doc.id],
        |r| r.get(0),
    )?;
    if prior.as_ref() == Some(&hash) {
        return Ok(());
    }
    let tokens = search_tokens(&text);
    if sqlite {
        tx.db_execute(
            "INSERT INTO knowledge_rag_chunks(chunk_id, content_hash) VALUES (?1, ?2)
            ON CONFLICT(chunk_id) DO UPDATE SET content_hash = excluded.content_hash",
            &db_params![doc.id, hash],
        )?;
        if prior.is_some() {
            tx.db_execute(
                "DELETE FROM knowledge_rag_chunks_fts WHERE chunk_id = ?1",
                &db_params![doc.id],
            )?;
        }
        tx.db_execute(
            "INSERT INTO knowledge_rag_chunks_fts(chunk_id, search_tokens) VALUES (?1, ?2)",
            &db_params![doc.id, tokens],
        )?;
    } else {
        tx.db_execute(
            "INSERT INTO knowledge_index.rag_chunks(chunk_id, content_hash, search_tsv)
            VALUES (?1, ?2, to_tsvector('simple', ?3)) ON CONFLICT(chunk_id) DO UPDATE
            SET content_hash = excluded.content_hash, search_tsv = excluded.search_tsv",
            &db_params![doc.id, hash, tokens],
        )?;
    }
    Ok(())
}

impl Store {
    pub fn knowledge_search(
        &self,
        actor: &Actor,
        scope: &Scope,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>, Error> {
        if query.trim().is_empty() || query.len() > 256 || query.contains('\0') {
            return Err(invalid("search requires 1–256 bytes"));
        }
        let conn = self.read_conn();
        require_scope(&conn, actor, scope)?;
        let mut groups: Vec<Vec<String>> = cjk_runs(query).map(cjk_phrase_tokens).collect();
        groups.extend(
            query
                .split(is_cjk_char)
                .flat_map(tokenize_hybrid_search_tokens)
                .filter(|token| token.bytes().all(|b| b.is_ascii_alphanumeric()))
                .map(|token| vec![token]),
        );
        groups.sort();
        groups.dedup();
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let sqlite = conn.backend() == DbBackend::Sqlite;
        let query = if sqlite {
            groups
                .iter()
                .map(|g| format!("\"{}\"", g.join(" ")))
                .collect::<Vec<_>>()
                .join(" AND ")
        } else {
            groups
                .iter()
                .map(|g| format!("({})", g.join(" <-> ")))
                .collect::<Vec<_>>()
                .join(" & ")
        };
        let mut params = db_params![query, limit.clamp(1, 50) as i64];
        let visible = visibility(actor, scope, &mut params);
        let (join, matches, rank) = if sqlite {
            (
                "knowledge_rag_chunks_fts c",
                "knowledge_rag_chunks_fts MATCH ?1",
                "bm25(knowledge_rag_chunks_fts)",
            )
        } else {
            (
                "knowledge_index.rag_chunks c",
                "c.search_tsv @@ to_tsquery('simple', ?1)",
                "-ts_rank_cd(c.search_tsv, to_tsquery('simple', ?1))",
            )
        };
        conn.db_query_vec(
            &format!(
                "SELECT d.namespace, d.topic, d.version, d.title, d.description
            FROM {join} JOIN knowledge_docs d ON d.id = c.chunk_id
            WHERE d.deleted_at_ms IS NULL AND ({visible}) AND ({matches})
            ORDER BY {rank}, d.pinned DESC, d.namespace, d.topic LIMIT ?2"
            ),
            &params,
            |r| {
                Ok(SearchHit {
                    namespace: r.get(0)?,
                    topic: r.get(1)?,
                    version: r.get(2)?,
                    title: r.get(3)?,
                    description: r.get(4)?,
                })
            },
        )
    }
}
