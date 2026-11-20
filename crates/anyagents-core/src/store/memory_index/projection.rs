//! Individual chunk and FTS row maintenance.

use super::Chunk;
use crate::db::{DbBackend, DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;
use crate::store::{
    classify_chunk_kind_and_tool, clean_rag_text, rag_content_hash, tokenize_hybrid_search_tokens,
};

// PostgreSQL history reads the complete body from the mailbox. Bound only
// the redundant projection, not the canonical mailbox or standalone summaries.
const PG_TEXT_BYTES: usize = 16 * 1024;
const PG_TOKEN_BYTES: usize = 8 * 1024;
// Metadata plus one token stream build the vector; don't add the raw body
// again. Keep individual ASCII tokens useful, but omit long base64 runs
// (including wrapped runs) rather than handing them to the lexer/GIN.
const MAX_LEXEME_BYTES: usize = 256;

fn utf8_prefix(text: &str, limit: usize) -> &str {
    &text[..text.floor_char_boundary(limit.min(text.len()))]
}

fn pg_search_tokens(text: &str) -> String {
    let mut encoded = String::new();
    for word in utf8_prefix(text, PG_TEXT_BYTES).split_whitespace() {
        // A data URI can otherwise contribute misleading MIME/payload terms.
        // Drop the URI, not surrounding prose or the authoritative source.
        if word
            .get(..5)
            .is_some_and(|p| p.eq_ignore_ascii_case("data:"))
            && word.contains(";base64,")
        {
            continue;
        }
        // Drop an opaque long field before the ASCII identifier tokenizer
        // can emit many short subtokens from it (e.g. URL-safe base64).
        if word.len() >= 64
            && word
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=' | b'_' | b'-'))
        {
            continue;
        }
        for token in tokenize_hybrid_search_tokens(word) {
            if token.len() > MAX_LEXEME_BYTES {
                continue;
            }
            let sep = usize::from(!encoded.is_empty());
            if encoded.len() + sep + token.len() > PG_TOKEN_BYTES {
                return encoded;
            }
            if sep != 0 {
                encoded.push(' ');
            }
            encoded.push_str(&token);
        }
    }
    encoded
}

/// Write one chunk and its full-text row, replacing any earlier projection.
/// A turn with fewer than two characters of text is not searchable and
/// leaves no row.
pub(in crate::store) fn upsert_chunk(
    conn: &(impl DbExec + ?Sized),
    chunk: &Chunk<'_>,
) -> Result<(), Error> {
    let cleaned = clean_rag_text(chunk.text);
    if cleaned.chars().take(2).count() < 2 {
        return remove_chunk(conn, chunk.id);
    }
    let (kind, tool_name) = classify_chunk_kind_and_tool(
        chunk.kind,
        chunk.tool,
        chunk.sender,
        chunk.destination,
        &cleaned,
    );
    let content_hash = rag_content_hash(&cleaned);
    if conn.backend() == DbBackend::Postgres {
        let stored_text = if chunk.id.starts_with("tool_") {
            &cleaned
        } else {
            utf8_prefix(&cleaned, PG_TEXT_BYTES)
        };
        let search_tokens = pg_search_tokens(stored_text);
        conn.db_execute(
            "INSERT INTO memory_index.rag_chunks AS prev (
                chunk_id, agent_id, agent_name, direction, sender, destination,
                timestamp_ms, content_hash, text, kind, tool_name, seq,
                search_tokens, search_tsv
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                to_tsvector(
                    'simple',
                    coalesce(?1, '') || ' ' || coalesce(?3, '') || ' ' || coalesce(?5, '')
                    || ' ' || coalesce(?6, '') || ' ' || coalesce(?10, '') || ' '
                    || coalesce(?11, '') || ' ' || coalesce(?13, '')
                )
             )
             ON CONFLICT (chunk_id) DO UPDATE SET
                agent_id = excluded.agent_id,
                agent_name = excluded.agent_name,
                direction = excluded.direction,
                sender = excluded.sender,
                destination = excluded.destination,
                timestamp_ms = excluded.timestamp_ms,
                content_hash = excluded.content_hash,
                text = excluded.text,
                kind = excluded.kind,
                tool_name = excluded.tool_name,
                seq = CASE WHEN excluded.seq <= 0 THEN prev.rowid ELSE excluded.seq END,
                search_tokens = excluded.search_tokens,
                search_tsv = excluded.search_tsv
             WHERE prev.content_hash IS DISTINCT FROM excluded.content_hash
                OR (prev.agent_id, prev.agent_name, prev.direction, prev.sender, prev.destination,
                    prev.timestamp_ms, prev.text, prev.kind, prev.tool_name, prev.seq,
                    prev.search_tokens, prev.search_tsv)
                   IS DISTINCT FROM
                   (excluded.agent_id, excluded.agent_name, excluded.direction, excluded.sender,
                    excluded.destination, excluded.timestamp_ms, excluded.text, excluded.kind,
                    excluded.tool_name, CASE WHEN excluded.seq <= 0 THEN prev.rowid ELSE excluded.seq END,
                    excluded.search_tokens, excluded.search_tsv)",
            &db_params![
                chunk.id, chunk.agent_id, chunk.agent_name, chunk.direction, chunk.sender,
                chunk.destination, chunk.timestamp_ms, content_hash, stored_text, kind, tool_name,
                chunk.seq, search_tokens,
            ],
        )?;
        if chunk.seq <= 0 {
            conn.db_execute(
                "UPDATE memory_index.rag_chunks SET seq = rowid WHERE chunk_id = ?1 AND seq <= 0",
                &db_params![chunk.id],
            )?;
        }
        return Ok(());
    }

    let existed: bool = conn.db_query_one(
        "SELECT EXISTS(SELECT 1 FROM rag_chunks WHERE chunk_id = ?1)",
        &db_params![chunk.id],
        |r| r.get(0),
    )?;
    if existed {
        remove_chunk(conn, chunk.id)?;
    }
    conn.db_execute(
        "INSERT INTO rag_chunks (
            chunk_id, agent_id, agent_name, direction, sender, destination,
            timestamp_ms, content_hash, text, kind, tool_name, seq
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        &db_params![
            chunk.id,
            chunk.agent_id,
            chunk.agent_name,
            chunk.direction,
            chunk.sender,
            chunk.destination,
            chunk.timestamp_ms,
            content_hash,
            cleaned,
            kind,
            tool_name,
            chunk.seq,
        ],
    )?;
    if chunk.seq <= 0 {
        conn.db_execute(
            "UPDATE rag_chunks SET seq = rowid WHERE chunk_id = ?1 AND seq <= 0",
            &db_params![chunk.id],
        )?;
    }
    conn.db_execute(
        "INSERT INTO rag_chunks_fts (
            chunk_id, agent_name, sender, destination, kind, tool_name, text, search_tokens
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        &db_params![
            chunk.id,
            chunk.agent_name,
            chunk.sender,
            chunk.destination,
            kind,
            tool_name,
            cleaned,
            tokenize_hybrid_search_tokens(&cleaned).join(" "),
        ],
    )?;
    Ok(())
}

/// Remove one chunk and its full-text row (and any duplicate of it).
pub(in crate::store) fn remove_chunk(
    conn: &(impl DbExec + ?Sized),
    chunk_id: &str,
) -> Result<(), Error> {
    if conn.backend() == DbBackend::Sqlite {
        match fts_chunk_lookup(chunk_id) {
            Some(lookup) => conn.db_execute(FTS_DELETE_CHUNK, &db_params![chunk_id, lookup])?,
            None => conn.db_execute(
                "DELETE FROM rag_chunks_fts WHERE chunk_id = ?1",
                &db_params![chunk_id],
            )?,
        };
    }
    conn.db_execute(
        "DELETE FROM rag_chunks WHERE chunk_id = ?1",
        &db_params![chunk_id],
    )?;
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

/// The phrase that finds one chunk's row: intersecting its non-prefix tokens
/// (`"tok1" AND "tok2"`), or all tokens when only prefix markers exist.
/// Skipping shared leading markers (`msg`, `out`, `tool`) avoids reading those
/// whole-index doclists, while intersecting remaining tokens keeps composite ids
/// narrow even when one component token is shared across many chunks.
/// `None` for an id without a token, which only a full scan can find.
pub(crate) fn fts_chunk_lookup(chunk_id: &str) -> Option<String> {
    let mut all = Vec::new();
    for token in fts_tokens(chunk_id) {
        if !all.contains(&token) {
            all.push(token);
        }
    }
    if all.is_empty() {
        return None;
    }
    let mut chosen: Vec<&str> = all
        .iter()
        .copied()
        .filter(|token| !matches!(*token, "msg" | "out" | "tool"))
        .collect();
    if chosen.is_empty() {
        chosen = all;
    }
    chosen.truncate(16);
    Some(
        chosen
            .into_iter()
            .map(fts_exact_phrase)
            .collect::<Vec<_>>()
            .join(" AND "),
    )
}

/// Remove an agent's full-text rows, or (`rename_to`) refile them under a
/// new name. Work is proportional to the agent's own rows: one indexed
/// statement per name its chunks are filed under, chunk by chunk only for a
/// name that has no token to match.
pub(in crate::store) fn rewrite_agent_fts_rows(
    conn: &(impl DbExec + ?Sized),
    agent_id: &str,
    rename_to: Option<&str>,
) -> Result<usize, Error> {
    if conn.backend() == DbBackend::Postgres {
        return match rename_to {
            None => Ok(0),
            Some(new) => {
                // Reuse the bounded body terms without reading the full summary.
                conn.db_execute(
                    "UPDATE memory_index.rag_chunks
                     SET agent_name = ?2,
                         search_tsv = to_tsvector(
                             'simple',
                             coalesce(chunk_id, '') || ' ' || coalesce(?2, '') || ' ' || coalesce(sender, '')
                             || ' ' || coalesce(destination, '') || ' ' || coalesce(kind, '') || ' '
                             || coalesce(tool_name, '') || ' ' || coalesce(search_tokens, '')
                         )
                     WHERE agent_id = ?1 AND agent_name != ?2",
                    &db_params![agent_id, new],
                )
            }
        };
    }

    let names: Vec<String> = conn.db_query_vec(
        "SELECT DISTINCT agent_name FROM rag_chunks WHERE agent_id = ?1",
        &db_params![agent_id],
        |row| row.get(0),
    )?;
    let mut changed = 0;
    for name in names {
        if rename_to == Some(name.as_str()) {
            continue;
        }
        if fts_tokens(&name).next().is_some() {
            let phrase = fts_exact_phrase(&name);
            changed += match rename_to {
                None => {
                    conn.db_execute(FTS_DELETE_AGENT_ROWS, &db_params![name, phrase, agent_id])?
                }
                Some(new) => conn.db_execute(
                    FTS_RENAME_AGENT_ROWS,
                    &db_params![name, phrase, agent_id, new],
                )?,
            };
            continue;
        }
        let ids: Vec<String> = conn.db_query_vec(
            "SELECT chunk_id FROM rag_chunks WHERE agent_id = ?1 AND agent_name = ?2",
            &db_params![agent_id, name],
            |row| row.get(0),
        )?;
        for chunk_id in ids {
            changed += match (rename_to, fts_chunk_lookup(&chunk_id)) {
                (None, Some(lookup)) => {
                    conn.db_execute(FTS_DELETE_CHUNK, &db_params![chunk_id, lookup])?
                }
                (None, None) => conn.db_execute(
                    "DELETE FROM rag_chunks_fts WHERE chunk_id = ?1",
                    &db_params![chunk_id],
                )?,
                (Some(new), Some(lookup)) => {
                    conn.db_execute(FTS_RENAME_CHUNK, &db_params![chunk_id, lookup, new])?
                }
                (Some(new), None) => conn.db_execute(
                    "UPDATE rag_chunks_fts SET agent_name = ?2 WHERE chunk_id = ?1",
                    &db_params![chunk_id, new],
                )?,
            };
        }
    }
    Ok(changed)
}
