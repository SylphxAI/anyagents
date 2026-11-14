//! The dense index: one int8 vector per embedded piece of a raw event, each
//! citing the exact raw byte range it was read from. It is a rebuildable
//! projection kept in its own SQLite file, never in `state.db` and never in
//! the host index; deleting it loses nothing but the time to rebuild it.
use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value};
use std::path::Path;

use crate::protocol::DenseHit;

/// One embedded piece, ready to store.
#[derive(Clone, Debug)]
pub struct Piece {
    /// Absolute byte range of the raw bytes the piece was read from, in the
    /// document's segment, and their SHA-256: the piece's citation.
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
    /// SHA-256 of the text that was embedded: equal text, equal vector.
    pub text_sha256: String,
    pub scale: f32,
    pub vector: Vec<u8>,
}

/// One document as the host index describes it.
#[derive(Clone, Debug)]
pub struct Document {
    pub id: String,
    pub agent_id: String,
    pub source: String,
    pub timestamp_ms: Option<i64>,
    pub raw_sha256: String,
}

pub struct DenseIndex {
    conn: Connection,
}

/// Symmetric per-vector int8 quantisation: `v ≈ scale * q`.
pub fn quantize(vector: &[f32]) -> (f32, Vec<u8>) {
    let max = vector.iter().fold(0f32, |m, x| m.max(x.abs()));
    if max == 0.0 {
        return (0.0, vec![0; vector.len()]);
    }
    let scale = max / 127.0;
    let bytes = vector
        .iter()
        .map(|x| ((x / scale).round().clamp(-127.0, 127.0) as i8) as u8)
        .collect();
    (scale, bytes)
}

fn dot(query: &[f32], scale: f32, bytes: &[u8]) -> f32 {
    let sum: f32 = query
        .iter()
        .zip(bytes)
        .map(|(q, &b)| q * f32::from(b as i8))
        .sum();
    sum * scale
}

impl DenseIndex {
    /// Open or create the index of one vector space, named by `space` (a
    /// `providers::Stamp` key: provider, model, dimension, preprocessing).
    /// A file built in another space is emptied: its vectors are not
    /// comparable.
    pub fn open(path: &Path, space: &str, dims: usize) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL;",
        )?;
        // Workers open the same file at once: creating the schema and
        // checking the space happen in one write transaction each, taken in
        // turn, never as a read upgraded to a write (which SQLite refuses
        // without waiting).
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        let prepared = (|| -> rusqlite::Result<()> {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS documents(
                    document_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, source TEXT NOT NULL,
                    timestamp_ms INTEGER, raw_sha256 TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS documents_owner ON documents(agent_id, source, timestamp_ms);
                CREATE TABLE IF NOT EXISTS vectors(
                    document_id TEXT NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
                    piece INTEGER NOT NULL, offset INTEGER NOT NULL, length INTEGER NOT NULL,
                    sha256 TEXT NOT NULL, text_sha256 TEXT NOT NULL, scale REAL NOT NULL,
                    vec BLOB NOT NULL, PRIMARY KEY(document_id, piece));
                CREATE INDEX IF NOT EXISTS vectors_content ON vectors(text_sha256);
                CREATE TABLE IF NOT EXISTS progress(k TEXT PRIMARY KEY, v INTEGER NOT NULL);",
            )?;
            let identity = format!("{space}:{dims}");
            let current: Option<String> = conn
                .query_row("SELECT v FROM meta WHERE k='model'", [], |r| r.get(0))
                .optional()?;
            if current.as_deref() != Some(identity.as_str()) {
                conn.execute_batch(
                    "DELETE FROM vectors; DELETE FROM documents; DELETE FROM progress;",
                )?;
                conn.execute(
                    "INSERT INTO meta VALUES('model', ?1) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                    [identity],
                )?;
            }
            Ok(())
        })();
        match prepared {
            Ok(()) => conn.execute_batch("COMMIT;")?,
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK;");
                return Err(error);
            }
        }
        Ok(Self { conn })
    }

    /// An existing index, read-only, if it holds the vector space `space`;
    /// `None` when it was built in another. Nothing is created or emptied.
    pub fn open_read_only(path: &Path, space: &str, dims: usize) -> rusqlite::Result<Option<Self>> {
        let conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let current: Option<String> = conn
            .query_row("SELECT v FROM meta WHERE k='model'", [], |r| r.get(0))
            .optional()?;
        Ok(
            (current.as_deref() == Some(format!("{space}:{dims}").as_str()))
                .then_some(Self { conn }),
        )
    }

    /// The raw digest a document was embedded at, if it was.
    pub fn embedded_sha(&self, document_id: &str) -> rusqlite::Result<Option<String>> {
        self.conn
            .prepare_cached("SELECT raw_sha256 FROM documents WHERE document_id=?1")?
            .query_row([document_id], |r| r.get(0))
            .optional()
    }

    /// A stored vector for identical text, from any document: the same text
    /// embeds to the same vector, so it is embedded once.
    pub fn cached(&self, text_sha256: &str) -> rusqlite::Result<Option<(f32, Vec<u8>)>> {
        self.conn
            .prepare_cached("SELECT scale, vec FROM vectors WHERE text_sha256=?1 LIMIT 1")?
            .query_row([text_sha256], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
    }

    /// Replace a document's pieces atomically, with the progress mark that
    /// records it, so an interrupted run resumes where it stopped.
    pub fn put(
        &mut self,
        document: &Document,
        pieces: &[Piece],
        progress: &[(&str, i64)],
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM documents WHERE document_id=?1", [&document.id])?;
        tx.execute(
            "INSERT INTO documents VALUES(?1,?2,?3,?4,?5)",
            params![
                document.id,
                document.agent_id,
                document.source,
                document.timestamp_ms,
                document.raw_sha256
            ],
        )?;
        for (n, piece) in pieces.iter().enumerate() {
            tx.execute(
                "INSERT INTO vectors VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    document.id,
                    n as i64,
                    piece.offset as i64,
                    piece.length as i64,
                    piece.sha256,
                    piece.text_sha256,
                    piece.scale,
                    piece.vector
                ],
            )?;
        }
        for (key, value) in progress {
            tx.execute(
                "INSERT INTO progress VALUES(?1,?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                params![key, value],
            )?;
        }
        tx.commit()
    }

    pub fn set_progress(&self, key: &str, value: i64) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO progress VALUES(?1,?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            params![key, value],
        )?;
        Ok(())
    }

    /// Add `delta` to a counter in one statement: workers on their own
    /// connections never lose each other's additions.
    pub fn add_progress(&self, key: &str, delta: i64) -> rusqlite::Result<i64> {
        self.conn.query_row(
            "INSERT INTO progress VALUES(?1,?2) ON CONFLICT(k) DO UPDATE SET v=v+excluded.v
             RETURNING v",
            params![key, delta],
            |r| r.get(0),
        )
    }

    /// Every progress mark whose key starts with `prefix`.
    pub fn progress_with_prefix(&self, prefix: &str) -> rusqlite::Result<Vec<(String, i64)>> {
        self.conn
            .prepare("SELECT k, v FROM progress WHERE substr(k, 1, length(?1)) = ?1")?
            .query_map([prefix], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect()
    }

    pub fn progress(&self, key: &str) -> rusqlite::Result<Option<i64>> {
        self.conn
            .query_row("SELECT v FROM progress WHERE k=?1", [key], |r| r.get(0))
            .optional()
    }

    /// Drop documents the host index no longer holds.
    pub fn forget(&self, document_ids: &[String]) -> rusqlite::Result<usize> {
        let mut removed = 0;
        for id in document_ids {
            removed += self
                .conn
                .execute("DELETE FROM documents WHERE document_id=?1", [id])?;
        }
        Ok(removed)
    }

    /// Every embedded document with the raw digest it was embedded at.
    pub fn documents(&self) -> rusqlite::Result<Vec<(String, String)>> {
        self.conn
            .prepare("SELECT document_id, raw_sha256 FROM documents")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect()
    }

    pub fn counts(&self) -> rusqlite::Result<(u64, u64)> {
        self.conn.query_row(
            "SELECT (SELECT count(*) FROM documents), (SELECT count(*) FROM vectors)",
            [],
            |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
        )
    }

    /// The best-matching documents of `owners`, each with its best piece.
    /// Owner selection precedes ranking: an empty owner list selects nothing.
    pub fn search(
        &self,
        query: &[f32],
        owners: &[String],
        source: Option<&str>,
        since_ms: Option<i64>,
        until_ms: Option<i64>,
        limit: usize,
    ) -> rusqlite::Result<Vec<DenseHit>> {
        if owners.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut values: Vec<Value> = owners.iter().cloned().map(Value::Text).collect();
        let mut sql = format!(
            "SELECT d.document_id, d.agent_id, v.offset, v.length, v.sha256, v.scale, v.vec
             FROM documents d JOIN vectors v ON v.document_id=d.document_id
             WHERE d.agent_id IN ({})",
            vec!["?"; owners.len()].join(",")
        );
        if let Some(source) = source {
            sql.push_str(" AND d.source=?");
            values.push(source.to_owned().into());
        }
        if let Some(since) = since_ms {
            sql.push_str(" AND d.timestamp_ms>?");
            values.push(since.into());
        }
        if let Some(until) = until_ms {
            sql.push_str(" AND d.timestamp_ms<=?");
            values.push(until.into());
        }
        let mut statement = self.conn.prepare(&sql)?;
        let mut rows = statement.query(params_from_iter(values))?;
        let mut best: std::collections::HashMap<String, DenseHit> =
            std::collections::HashMap::new();
        while let Some(row) = rows.next()? {
            let scale: f32 = row.get(5)?;
            let vec: Vec<u8> = row.get(6)?;
            if vec.len() != query.len() {
                continue;
            }
            let score = dot(query, scale, &vec);
            let document_id: String = row.get(0)?;
            if best.get(&document_id).is_some_and(|hit| hit.score >= score) {
                continue;
            }
            best.insert(
                document_id.clone(),
                DenseHit {
                    document_id,
                    agent_id: row.get(1)?,
                    offset: row.get::<_, i64>(2)? as u64,
                    length: row.get::<_, i64>(3)? as u64,
                    sha256: row.get(4)?,
                    score,
                },
            );
        }
        let mut hits: Vec<DenseHit> = best.into_values().collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.document_id.cmp(&b.document_id))
        });
        hits.truncate(limit);
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(v: &[f32]) -> Vec<f32> {
        let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.iter().map(|x| x / n).collect()
    }

    fn piece(offset: u64, v: &[f32]) -> Piece {
        let (scale, vector) = quantize(&unit(v));
        Piece {
            offset,
            length: 10,
            sha256: format!("sha{offset}"),
            text_sha256: format!("text{offset}"),
            scale,
            vector,
        }
    }

    fn doc(id: &str, agent: &str, time: i64) -> Document {
        Document {
            id: id.into(),
            agent_id: agent.into(),
            source: "transcripts".into(),
            timestamp_ms: Some(time),
            raw_sha256: format!("raw-{id}"),
        }
    }

    #[test]
    fn quantised_search_ranks_by_best_piece_within_authorized_owners() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = DenseIndex::open(&dir.path().join("dense.db"), "m", 3).unwrap();
        index
            .put(
                &doc("d1", "a", 1),
                &[piece(0, &[1.0, 0.0, 0.0]), piece(20, &[0.0, 1.0, 0.0])],
                &[],
            )
            .unwrap();
        index
            .put(
                &doc("d2", "a", 2),
                &[piece(0, &[0.7, 0.7, 0.0])],
                &[("tail", 9)],
            )
            .unwrap();
        index
            .put(&doc("d3", "b", 3), &[piece(0, &[0.0, 1.0, 0.0])], &[])
            .unwrap();
        let q = unit(&[0.0, 1.0, 0.0]);
        let hits = index
            .search(&q, &["a".into()], None, None, None, 10)
            .unwrap();
        let ids: Vec<&str> = hits.iter().map(|h| h.document_id.as_str()).collect();
        assert_eq!(ids, vec!["d1", "d2"], "d3 belongs to an unauthorized owner");
        assert_eq!(hits[0].offset, 20, "the best piece is cited");
        assert!((hits[0].score - 1.0).abs() < 0.02);
        assert!(
            index
                .search(&q, &[], None, None, None, 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            index
                .search(&q, &["a".into()], None, Some(1), None, 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(index.progress("tail").unwrap(), Some(9));
        assert_eq!(index.embedded_sha("d2").unwrap().as_deref(), Some("raw-d2"));
        assert!(index.cached("text20").unwrap().is_some());

        // Replacing a document replaces all its pieces.
        index
            .put(&doc("d1", "a", 1), &[piece(5, &[0.0, 0.0, 1.0])], &[])
            .unwrap();
        assert_eq!(index.counts().unwrap(), (3, 3));
        assert_eq!(index.forget(&["d1".into()]).unwrap(), 1);
        assert_eq!(index.counts().unwrap(), (2, 2));

        // Another model's file starts empty.
        drop(index);
        let index = DenseIndex::open(&dir.path().join("dense.db"), "other", 3).unwrap();
        assert_eq!(index.counts().unwrap(), (0, 0));
    }
}
