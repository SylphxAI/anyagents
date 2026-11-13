//! From one host-index document to stored vectors: read the raw event through
//! its verified coordinate, project its readable values, cut them into pieces
//! that fit the model, and embed each piece once.
use crate::{
    dense::{DenseIndex, Document, Piece, quantize},
    runtime::{Encoder, Error},
    text::{Span, project},
};
use anyagents_core::raw_segment::{RawCoordinate, read_raw};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// A document is embedded up to this many pieces; the rest of a very large
/// event (a long log or file dump) stays reachable through lexical search.
pub const MAX_PIECES: usize = 16;
/// Tokens kept free for special tokens and the passage prefix.
const RESERVED_TOKENS: usize = 16;

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

/// A read-only view of the kernel's host index: the catalog of documents and
/// their raw coordinates. The service never writes it.
pub struct HostCatalog {
    conn: Connection,
}

impl HostCatalog {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self { conn })
    }

    const COLUMNS: &str =
        "rowid, id, agent_id, source, timestamp_ms, segment, offset, length, sha256, seq";

    fn rows(&self, sql: &str, bound: i64, limit: usize) -> rusqlite::Result<Vec<HostDocument>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {} FROM host_documents {sql} LIMIT ?2",
                Self::COLUMNS
            ))?
            .query_map(params![bound, limit as i64], |r| {
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
            })?
            .collect()
    }

    /// Documents published after `rowid`, oldest first.
    pub fn after(&self, rowid: i64, limit: usize) -> rusqlite::Result<Vec<HostDocument>> {
        self.rows("WHERE rowid>?1 ORDER BY rowid", rowid, limit)
    }

    /// Documents published before `rowid`, newest first.
    pub fn before(&self, rowid: i64, limit: usize) -> rusqlite::Result<Vec<HostDocument>> {
        self.rows("WHERE rowid<?1 ORDER BY rowid DESC", rowid, limit)
    }

    pub fn max_rowid(&self) -> rusqlite::Result<i64> {
        self.conn.query_row(
            "SELECT COALESCE(MAX(rowid),0) FROM host_documents",
            [],
            |r| r.get(0),
        )
    }

    pub fn count(&self) -> rusqlite::Result<u64> {
        self.conn
            .query_row("SELECT count(*) FROM host_documents", [], |r| {
                r.get::<_, i64>(0)
            })
            .map(|n| n as u64)
    }

    pub fn contains(&self, document_id: &str, raw_sha256: &str) -> rusqlite::Result<bool> {
        Ok(self
            .conn
            .prepare_cached("SELECT 1 FROM host_documents WHERE id=?1 AND sha256=?2")?
            .query_row([document_id, raw_sha256], |_| Ok(()))
            .optional()?
            .is_some())
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

/// The pieces of one raw event as the embedder will read them.
pub fn chunks_of(encoder: &Encoder, raw: &[u8]) -> Result<Vec<Chunk>, Error> {
    let spans = project(raw);
    let tokenizer = encoder.counter();
    let budget = encoder
        .pin()
        .max_tokens
        .saturating_sub(RESERVED_TOKENS)
        .max(16);
    let mut count = |text: &str| -> Result<Vec<(usize, usize)>, Error> {
        Ok(tokenizer
            .encode(text, false)
            .map_err(|e| e.to_string())?
            .get_offsets()
            .to_vec())
    };
    chunk(&spans, &mut count, budget, MAX_PIECES)
}

/// What embedding a batch of documents did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Embedded {
    /// Documents newly embedded (not already current, not unreadable).
    pub documents: usize,
    pub pieces: usize,
    /// Pieces whose vector came from identical text already embedded.
    pub reused: usize,
    /// Tokens the model read.
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

/// Pieces one model run reads together. Pieces are sorted by length first,
/// so a batch pads little.
const MODEL_BATCH: usize = 16;

/// Embed `documents` and store each with its rowid as the `progress` mark,
/// in order, so a restart resumes after the last one stored. A document the
/// index already holds at this raw digest is only marked; one whose bytes no
/// longer match its coordinate is skipped (the host index republishes it).
/// The uncached pieces of the whole batch go through the model together.
pub fn embed_documents(
    encoder: &mut Encoder,
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
        for (i, chunk) in chunks_of(encoder, &raw)?.into_iter().enumerate() {
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
    for batch in unique.chunks(MODEL_BATCH) {
        let texts: Vec<&str> = batch.iter().map(|(t, _)| t.as_str()).collect();
        stats.tokens += batch.iter().map(|(_, n)| n).sum::<usize>();
        for vector in encoder.embed_passages(&texts)? {
            vectors.push(quantize(&vector));
        }
    }
    stats.model_ms = model.elapsed().as_millis() as u64;
    for (d, i, text, _) in &pending {
        if let Some(Some(pieces)) = planned.get_mut(*d) {
            let (scale, bytes) = &vectors[slot_of[text]];
            pieces[*i].scale = *scale;
            pieces[*i].vector = bytes.clone();
        }
    }
    for (host, plan) in documents.iter().zip(planned) {
        match plan {
            Some(pieces) => dense.put(&host.document, &pieces, &[(progress, host.rowid)])?,
            None => dense.set_progress(progress, host.rowid)?,
        }
    }
    Ok(stats)
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
}
