//! From one host-index document to stored vectors: read the raw event through
//! its verified coordinate, project its readable values, cut them into pieces
//! that fit the model, and embed each piece once.
use crate::{
    dense::{DenseIndex, Document, Piece, quantize},
    providers::Embedder,
    runtime::Error,
    text::Span,
};
use anyagents_core::{
    db::configured_database_url_for_data_dir,
    host_index::{HostCatalogEntry, HostIndex},
    raw_segment::{RawCoordinate, read_raw},
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
};

/// A document is embedded up to this many pieces; the rest of a very large
/// event (a long log or file dump) stays reachable through lexical search.
pub const MAX_PIECES: usize = 16;

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// One document row of the host index.
#[derive(Clone, Debug)]
pub struct HostDocument {
    pub rowid: i64,
    pub document: Document,
    pub raw: RawCoordinate,
}

/// One worker's share of the catalog: the documents whose rowid is `index`
/// modulo `of`. Each stripe keeps its own marks, so workers never wait on or
/// skip past each other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stripe {
    pub index: usize,
    pub of: usize,
}

impl Stripe {
    pub const ALL: Stripe = Stripe { index: 0, of: 1 };
}

enum HostCatalogBackend {
    Sqlite(Connection),
    Index {
        index: HostIndex,
        contains_cache: Mutex<Option<(i64, HashMap<String, String>)>>,
    },
}

/// Returns whether a host index is reachable via an on-disk SQLite file or a
/// configured PostgreSQL database for `host_path`'s data directory.
pub(crate) fn host_index_available(host_path: &Path) -> bool {
    host_path.exists()
        || configured_database_url_for_data_dir(host_path.parent().unwrap_or_else(|| Path::new("")))
            .is_some()
}

/// A read-only view of the kernel's host index: the catalog of documents and
/// their raw coordinates. The service never writes it.
pub struct HostCatalog {
    backend: HostCatalogBackend,
}

impl HostCatalog {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let data_dir = path.parent().unwrap_or_else(|| Path::new(""));
        if let Some(url) = configured_database_url_for_data_dir(data_dir) {
            let index = HostIndex::open_postgres(&url)?;
            return Ok(Self::from_host_index(index));
        }
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self {
            backend: HostCatalogBackend::Sqlite(conn),
        })
    }

    pub fn from_host_index(index: HostIndex) -> Self {
        Self {
            backend: HostCatalogBackend::Index {
                index,
                contains_cache: Mutex::new(None),
            },
        }
    }

    const COLUMNS: &str =
        "rowid, id, agent_id, source, timestamp_ms, segment, offset, length, sha256, seq";

    fn entry_to_document(entry: HostCatalogEntry) -> HostDocument {
        HostDocument {
            rowid: entry.rowid,
            document: Document {
                id: entry.id,
                agent_id: entry.agent_id,
                source: entry.source,
                timestamp_ms: entry.timestamp_ms,
                raw_sha256: entry.raw.sha256.clone(),
            },
            raw: entry.raw,
        }
    }

    fn rows(
        &self,
        sql: &str,
        bound: i64,
        limit: usize,
        stripe: Stripe,
    ) -> Result<Vec<HostDocument>, Error> {
        let HostCatalogBackend::Sqlite(conn) = &self.backend else {
            unreachable!("SQLite row query called on non-SQLite backend");
        };
        let rows = conn
            .prepare_cached(&format!(
                "SELECT {} FROM host_documents WHERE rowid % ?3 = ?4 AND {sql} LIMIT ?2",
                Self::COLUMNS
            ))?
            .query_map(
                params![bound, limit as i64, stripe.of as i64, stripe.index as i64],
                |r| {
                    Ok(HostDocument {
                        rowid: r.get(0)?,
                        document: Document {
                            id: r.get(1)?,
                            agent_id: r.get(2)?,
                            source: r.get(3)?,
                            timestamp_ms: r.get(4)?,
                            raw_sha256: r.get(8)?,
                        },
                        raw: RawCoordinate {
                            segment: PathBuf::from(r.get::<_, String>(5)?),
                            offset: r.get::<_, i64>(6)? as u64,
                            length: r.get::<_, i64>(7)? as u64,
                            sha256: r.get(8)?,
                            seq: r.get::<_, i64>(9)? as u64,
                        },
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Documents of `stripe` published after `rowid`, oldest first.
    pub fn after(
        &self,
        rowid: i64,
        limit: usize,
        stripe: Stripe,
    ) -> Result<Vec<HostDocument>, Error> {
        match &self.backend {
            HostCatalogBackend::Sqlite(_) => {
                self.rows("rowid>?1 ORDER BY rowid", rowid, limit, stripe)
            }
            HostCatalogBackend::Index { index, .. } => Ok(index
                .list_catalog_entries(
                    Some(rowid),
                    None,
                    Some((stripe.index, stripe.of)),
                    false,
                    limit,
                )?
                .into_iter()
                .map(Self::entry_to_document)
                .collect()),
        }
    }

    /// Documents of `stripe` published before `rowid`, newest first.
    pub fn before(
        &self,
        rowid: i64,
        limit: usize,
        stripe: Stripe,
    ) -> Result<Vec<HostDocument>, Error> {
        match &self.backend {
            HostCatalogBackend::Sqlite(_) => {
                self.rows("rowid<?1 ORDER BY rowid DESC", rowid, limit, stripe)
            }
            HostCatalogBackend::Index { index, .. } => Ok(index
                .list_catalog_entries(
                    None,
                    Some(rowid),
                    Some((stripe.index, stripe.of)),
                    false,
                    limit,
                )?
                .into_iter()
                .map(Self::entry_to_document)
                .collect()),
        }
    }

    /// Up to `n` documents chosen uniformly at random, to estimate from.
    pub fn sample(&self, n: usize) -> Result<Vec<HostDocument>, Error> {
        match &self.backend {
            HostCatalogBackend::Sqlite(_) => self.rows(
                "rowid IN (SELECT rowid FROM host_documents ORDER BY random() LIMIT ?1)",
                n as i64,
                n,
                Stripe::ALL,
            ),
            HostCatalogBackend::Index { index, .. } => Ok(index
                .list_catalog_entries(None, None, None, true, n)?
                .into_iter()
                .map(Self::entry_to_document)
                .collect()),
        }
    }

    /// The host index's generation (`host_generation`), which changes when
    /// its documents table is created again and rowids start over; `legacy`
    /// for an index made before generations.
    pub fn generation(&self) -> Result<String, Error> {
        match &self.backend {
            HostCatalogBackend::Sqlite(conn) => {
                match conn.query_row(
                    "SELECT generation FROM host_generation WHERE id=1",
                    [],
                    |r| r.get::<_, String>(0),
                ) {
                    Ok(generation) => Ok(generation),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok("legacy".into()),
                    Err(rusqlite::Error::SqliteFailure(_, Some(message)))
                        if message.contains("no such table") =>
                    {
                        Ok("legacy".into())
                    }
                    Err(error) => Err(error.into()),
                }
            }
            HostCatalogBackend::Index { index, .. } => {
                Ok(index.generation()?.unwrap_or_else(|| "legacy".into()))
            }
        }
    }

    pub fn max_rowid(&self) -> Result<i64, Error> {
        match &self.backend {
            HostCatalogBackend::Sqlite(conn) => Ok(conn.query_row(
                "SELECT COALESCE(MAX(rowid),0) FROM host_documents",
                [],
                |r| r.get(0),
            )?),
            HostCatalogBackend::Index { index, .. } => Ok(index
                .list_catalog_entries(None, Some(i64::MAX), None, false, 1)?
                .first()
                .map_or(0, |e| e.rowid)),
        }
    }

    pub fn count(&self) -> Result<u64, Error> {
        match &self.backend {
            HostCatalogBackend::Sqlite(conn) => Ok(conn
                .query_row("SELECT count(*) FROM host_documents", [], |r| {
                    r.get::<_, i64>(0)
                })
                .map(|n| n as u64)?),
            HostCatalogBackend::Index { index, .. } => {
                let mut total = 0u64;
                let mut cursor = 0i64;
                loop {
                    let batch =
                        index.list_catalog_entries(Some(cursor), None, None, false, 4096)?;
                    let Some(last) = batch.last() else { break };
                    total = total.saturating_add(batch.len() as u64);
                    cursor = last.rowid;
                    if batch.len() < 4096 {
                        break;
                    }
                }
                Ok(total)
            }
        }
    }

    pub fn contains(&self, document_id: &str, raw_sha256: &str) -> Result<bool, Error> {
        match &self.backend {
            HostCatalogBackend::Sqlite(conn) => Ok(conn
                .prepare_cached("SELECT 1 FROM host_documents WHERE id=?1 AND sha256=?2")?
                .query_row([document_id, raw_sha256], |_| Ok(()))
                .optional()?
                .is_some()),
            HostCatalogBackend::Index {
                index,
                contains_cache,
            } => {
                let mut guard = contains_cache
                    .lock()
                    .map_err(|_| "catalog cache poisoned")?;
                let (cursor, map) = guard.get_or_insert_with(|| (0i64, HashMap::new()));
                if map.get(document_id).map(String::as_str) == Some(raw_sha256) {
                    return Ok(true);
                }
                loop {
                    let batch =
                        index.list_catalog_entries(Some(*cursor), None, None, false, 4096)?;
                    let Some(last) = batch.last() else { break };
                    *cursor = last.rowid;
                    let batch_len = batch.len();
                    for entry in batch {
                        map.insert(entry.id, entry.raw.sha256);
                    }
                    if batch_len < 4096 {
                        break;
                    }
                }
                Ok(map.get(document_id).map(String::as_str) == Some(raw_sha256))
            }
        }
    }
}

/// Token character offsets of a text, as the model's tokenizer cuts it.
pub type TokenOffsets<'a> = dyn FnMut(&str) -> Result<Vec<(usize, usize)>, Error> + 'a;

/// A piece of text to embed and the raw byte range it cites, relative to the
/// start of its event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub start: usize,
    pub end: usize,
    pub text: String,
    /// Tokens the text holds, special tokens excluded.
    pub tokens: usize,
}

/// Cut projected spans into chunks of at most `budget` tokens. Consecutive
/// spans share a chunk while they fit; a span longer than the budget is cut
/// at token boundaries, and each cut cites the whole value it came from.
pub fn chunk(
    spans: &[Span],
    count: &mut TokenOffsets<'_>,
    budget: usize,
    max_chunks: usize,
) -> Result<Vec<Chunk>, Error> {
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut current: Option<Chunk> = None;
    for span in spans {
        if chunks.len() >= max_chunks {
            break;
        }
        let offsets = count(&span.text)?;
        let tokens = offsets.len();
        if tokens > budget {
            if let Some(chunk) = current.take() {
                chunks.push(chunk);
            }
            for window in offsets.chunks(budget) {
                if chunks.len() >= max_chunks {
                    break;
                }
                let from = window.first().map_or(0, |o| o.0);
                let to = window.last().map_or(span.text.len(), |o| o.1);
                let text = span.text.get(from..to).unwrap_or_default().trim();
                if !text.is_empty() {
                    chunks.push(Chunk {
                        start: span.start,
                        end: span.end,
                        text: text.to_owned(),
                        tokens: window.len(),
                    });
                }
            }
            continue;
        }
        match current.as_mut() {
            Some(chunk) if chunk.tokens + tokens <= budget => {
                chunk.end = span.end;
                chunk.text.push('\n');
                chunk.text.push_str(&span.text);
                chunk.tokens += tokens;
            }
            _ => {
                if let Some(chunk) = current.take() {
                    chunks.push(chunk);
                }
                current = Some(Chunk {
                    start: span.start,
                    end: span.end,
                    text: span.text.clone(),
                    tokens,
                });
            }
        }
    }
    if let Some(chunk) = current
        && chunks.len() < max_chunks
    {
        chunks.push(chunk);
    }
    Ok(chunks)
}

/// What embedding a batch of documents did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Embedded {
    /// Documents newly embedded (not already current, not unreadable).
    pub documents: usize,
    pub pieces: usize,
    /// Pieces whose vector came from identical text already embedded.
    pub reused: usize,
    /// Tokens the model read (approximate for a remote model).
    pub tokens: usize,
    /// Time spent reading and cutting text, and in the model.
    pub prepare_ms: u64,
    pub model_ms: u64,
}

impl std::ops::AddAssign for Embedded {
    fn add_assign(&mut self, other: Self) {
        self.documents += other.documents;
        self.pieces += other.pieces;
        self.reused += other.reused;
        self.tokens += other.tokens;
        self.prepare_ms += other.prepare_ms;
        self.model_ms += other.model_ms;
    }
}

/// Embed `documents` and store each with its rowid as the `progress` mark,
/// in order, so a restart resumes after the last one stored. A document the
/// index already holds at this raw digest is only marked; one whose bytes no
/// longer match its coordinate is skipped (the host index republishes it).
/// The uncached pieces of the whole batch go through the model together,
/// sorted by length so a local batch pads little.
///
/// When a model request fails partway, every document whose pieces were all
/// embedded before it is still stored, so vectors already paid for are kept
/// (identical text is then reused, not bought again); the mark moves only
/// over the leading run of stored documents, and the error is returned.
pub fn embed_documents(
    encoder: &mut Embedder,
    dense: &mut DenseIndex,
    documents: &[HostDocument],
    progress: &str,
) -> Result<Embedded, Error> {
    let mut stats = Embedded::default();
    let prepare = std::time::Instant::now();
    // Per document: `None` when only its mark moves, else its pieces.
    let mut planned: Vec<Option<Vec<Piece>>> = Vec::with_capacity(documents.len());
    let mut pending: Vec<(usize, usize, String, usize)> = Vec::new();
    for (d, host) in documents.iter().enumerate() {
        if dense.embedded_sha(&host.document.id)?.as_deref() == Some(host.raw.sha256.as_str()) {
            planned.push(None);
            continue;
        }
        let Ok(raw) = read_raw(&host.raw) else {
            planned.push(None);
            continue;
        };
        let mut pieces = Vec::new();
        for (i, chunk) in encoder.chunks_of(&raw)?.into_iter().enumerate() {
            let text_sha256 = sha256(chunk.text.as_bytes());
            let (scale, vector) = match dense.cached(&text_sha256)? {
                Some(cached) => {
                    stats.reused += 1;
                    cached
                }
                None => {
                    pending.push((d, i, chunk.text.clone(), chunk.tokens));
                    (0.0, Vec::new())
                }
            };
            pieces.push(Piece {
                offset: host.raw.offset + chunk.start as u64,
                length: (chunk.end - chunk.start) as u64,
                sha256: sha256(&raw[chunk.start..chunk.end]),
                text_sha256,
                scale,
                vector,
            });
        }
        stats.documents += 1;
        stats.pieces += pieces.len();
        planned.push(Some(pieces));
    }
    // Equal text in one batch is embedded once.
    pending.sort_by_key(|p| p.3);
    let mut unique: Vec<(String, usize)> = Vec::new();
    let mut slot_of: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (_, _, text, tokens) in &pending {
        if !slot_of.contains_key(text) {
            slot_of.insert(text.clone(), unique.len());
            unique.push((text.clone(), *tokens));
        }
    }
    stats.prepare_ms = prepare.elapsed().as_millis() as u64;
    let model = std::time::Instant::now();
    let mut vectors: Vec<(f32, Vec<u8>)> = Vec::with_capacity(unique.len());
    let mut failure = None;
    for batch in unique.chunks(encoder.batch()) {
        let texts: Vec<&str> = batch.iter().map(|(t, _)| t.as_str()).collect();
        match encoder.embed_passages(&texts) {
            Ok(embedded) => {
                stats.tokens += batch.iter().map(|(_, n)| n).sum::<usize>();
                vectors.extend(embedded.iter().map(|v| quantize(v)));
            }
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    stats.model_ms = model.elapsed().as_millis() as u64;
    let mut complete = vec![true; documents.len()];
    for (d, i, text, _) in &pending {
        match vectors.get(slot_of[text]) {
            Some((scale, bytes)) => {
                if let Some(Some(pieces)) = planned.get_mut(*d) {
                    pieces[*i].scale = *scale;
                    pieces[*i].vector = bytes.clone();
                }
            }
            None => complete[*d] = false,
        }
    }
    let mut marking = true;
    for ((host, plan), complete) in documents.iter().zip(planned).zip(complete) {
        if !complete {
            marking = false;
            stats.documents -= 1;
            continue;
        }
        let mark = [(progress, host.rowid)];
        let marks: &[(&str, i64)] = if marking { &mark } else { &[] };
        match plan {
            Some(pieces) => dense.put(&host.document, &pieces, marks)?,
            None if marking => dense.set_progress(progress, host.rowid)?,
            None => {}
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(stats),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(start: usize, text: &str) -> Span {
        Span {
            start,
            end: start + text.len(),
            text: text.into(),
        }
    }

    #[test]
    fn stripes_split_the_catalog_into_disjoint_shares_in_both_directions() {
        use anyagents_core::{
            host_index::{HostIndex, IndexMetadata, MemoryScope},
            raw_segment::retained_jsonl_records,
        };
        let dir = crate::test_tempdir();
        let file = dir.path().join("history.jsonl");
        let lines: String = (0..7)
            .map(|i| format!("{{\"content\":\"line {i}\"}}\n"))
            .collect();
        std::fs::write(&file, lines).unwrap();
        let path = dir.path().join("host.db");
        let mut index = HostIndex::open(&path).unwrap();
        for (i, raw) in retained_jsonl_records(&file).unwrap().iter().enumerate() {
            let metadata = IndexMetadata {
                agent_id: "agt".into(),
                source: MemoryScope::Transcripts,
                timestamp_ms: Some(i as i64),
                direction: "in".into(),
                sender: "s".into(),
                destination: "d".into(),
                tool_name: None,
                tool_names: Vec::new(),
                role: None,
            };
            index.index(&format!("doc{i}"), &metadata, raw).unwrap();
        }
        drop(index);
        let catalog = HostCatalog::open(&path).unwrap();
        let all: Vec<i64> = catalog
            .after(0, 100, Stripe::ALL)
            .unwrap()
            .iter()
            .map(|d| d.rowid)
            .collect();
        assert_eq!(all.len(), 7);
        let mut seen = Vec::new();
        for index in 0..3 {
            let stripe = Stripe { index, of: 3 };
            let forward: Vec<i64> = catalog
                .after(0, 100, stripe)
                .unwrap()
                .iter()
                .map(|d| d.rowid)
                .collect();
            let backward: Vec<i64> = catalog
                .before(i64::MAX, 100, stripe)
                .unwrap()
                .iter()
                .map(|d| d.rowid)
                .collect();
            assert!(forward.iter().all(|r| *r as usize % 3 == index));
            assert_eq!(backward, forward.iter().rev().copied().collect::<Vec<_>>());
            seen.extend(forward);
        }
        seen.sort();
        assert_eq!(seen, all);
    }

    #[test]
    fn spans_pack_into_budgeted_chunks_and_long_values_are_cut() {
        // One token per whitespace-separated word, offsets into the text.
        let mut count = |text: &str| -> Result<Vec<(usize, usize)>, Error> {
            let mut out = Vec::new();
            let mut at = 0;
            for word in text.split(' ') {
                out.push((at, at + word.len()));
                at += word.len() + 1;
            }
            Ok(out)
        };
        let spans = vec![
            span(0, "a b"),
            span(10, "c d"),
            span(20, "e f g h i j"),
            span(40, "k"),
        ];
        let chunks = chunk(&spans, &mut count, 4, 16).unwrap();
        let texts: Vec<&str> = chunks.iter().map(|c| c.text.as_str()).collect();
        assert_eq!(texts, vec!["a b\nc d", "e f g h", "i j", "k"]);
        assert_eq!((chunks[0].start, chunks[0].end), (0, 13));
        assert_eq!(
            (chunks[1].start, chunks[1].end),
            (20, 31),
            "a cut cites its whole value"
        );
        assert_eq!(chunk(&spans, &mut count, 4, 2).unwrap().len(), 2);
    }

    #[test]
    fn host_catalog_via_host_index_matches_sqlite_queries_and_handles_pg_fallback() {
        use anyagents_core::{
            host_index::{HostIndex, IndexMetadata, MemoryScope},
            raw_segment::retained_jsonl_records,
        };
        let dir = crate::test_tempdir();
        let file = dir.path().join("history.jsonl");
        let lines: String = (0..5)
            .map(|i| format!("{{\"content\":\"doc line {i}\"}}\n"))
            .collect();
        std::fs::write(&file, lines).unwrap();
        let path = dir.path().join("host-history-index-v1.db");
        let mut index = HostIndex::open(&path).unwrap();
        let records = retained_jsonl_records(&file).unwrap();
        for (i, raw) in records.iter().enumerate() {
            let metadata = IndexMetadata {
                agent_id: "agt".into(),
                source: MemoryScope::Transcripts,
                timestamp_ms: Some(i as i64),
                direction: "in".into(),
                sender: "s".into(),
                destination: "d".into(),
                tool_name: None,
                tool_names: Vec::new(),
                role: None,
            };
            index.index(&format!("doc{i}"), &metadata, raw).unwrap();
        }

        let catalog = HostCatalog::from_host_index(index);
        assert_eq!(catalog.count().unwrap(), 5);
        assert_eq!(catalog.max_rowid().unwrap(), 5);
        assert!(!catalog.generation().unwrap().is_empty());
        assert_eq!(catalog.sample(3).unwrap().len(), 3);
        assert!(catalog.contains("doc2", &records[2].sha256).unwrap());
        assert!(!catalog.contains("doc2", "wrong-sha").unwrap());
        let forward = catalog.after(0, 10, Stripe { index: 1, of: 2 }).unwrap();
        let backward = catalog
            .before(i64::MAX, 10, Stripe { index: 1, of: 2 })
            .unwrap();
        assert_eq!(
            forward.iter().map(|d| d.rowid).collect::<Vec<_>>(),
            backward.iter().rev().map(|d| d.rowid).collect::<Vec<_>>()
        );

        // When `database-url` configures PostgreSQL and no SQLite file exists on disk,
        // `host_index_available` reports true and `HostCatalog::open` routes to PG
        // rather than failing with SQLite CANTOPEN or creating an empty file.
        let pg_dir = crate::test_tempdir();
        let missing_host = pg_dir.path().join("host-history-index-v1.db");
        assert!(!host_index_available(&missing_host));
        std::fs::write(
            pg_dir.path().join("database-url"),
            "postgres://user:pass@127.0.0.1:1/nonexistent_db\n",
        )
        .unwrap();
        assert!(host_index_available(&missing_host));
        assert!(HostCatalog::open(&missing_host).is_err());
        assert!(!missing_host.exists());
    }
}
