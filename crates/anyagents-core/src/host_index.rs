//! Rebuildable host index: metadata and contentless FTS windows only.
//! Raw bytes stay in their original filesystem segment. Every result requires
//! an explicit host-authorized agent set, including learned-memory results.
use crate::{HistorySearchQuery, HistorySort, raw_segment::RawCoordinate};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

const WINDOW_BYTES: usize = 4096;
const OVERLAP_BYTES: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MemoryScope {
    #[default]
    All,
    Memory,
    Transcripts,
}
impl MemoryScope {
    fn source(self) -> Option<&'static str> {
        match self {
            Self::All => None,
            Self::Memory => Some("memory"),
            Self::Transcripts => Some("transcripts"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexMetadata {
    /// Stable agent ID, never a caller-supplied display name.
    pub agent_id: String,
    pub source: MemoryScope,
    pub timestamp_ms: i64,
    pub direction: String,
    pub sender: String,
    pub destination: String,
    pub tool_name: Option<String>,
    pub role: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexHit {
    pub id: String,
    pub metadata: IndexMetadata,
    /// The whole lossless event/document; not an SQLite copy.
    pub raw: RawCoordinate,
    /// Exact bounded matching window, with its own SHA for readback.
    pub window: RawCoordinate,
    pub lexical_score: f64,
}

pub struct HostIndex {
    conn: Connection,
}
impl HostIndex {
    pub fn open(path: &Path) -> Result<Self, IndexError> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA secure_delete=ON;
            CREATE TABLE IF NOT EXISTS host_documents (
                id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, source TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL, direction TEXT NOT NULL,
                sender TEXT NOT NULL, destination TEXT NOT NULL, tool_name TEXT,
                role TEXT, segment TEXT NOT NULL, offset INTEGER NOT NULL,
                length INTEGER NOT NULL, sha256 TEXT NOT NULL, seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS host_sources (
                agent_id TEXT NOT NULL, segment TEXT NOT NULL, cursor TEXT NOT NULL,
                PRIMARY KEY(agent_id,segment));
            CREATE INDEX IF NOT EXISTS host_documents_owner ON host_documents(agent_id,source,timestamp_ms);
            CREATE TABLE IF NOT EXISTS host_windows (
                id INTEGER PRIMARY KEY, document_id TEXT NOT NULL REFERENCES host_documents(id) ON DELETE CASCADE,
                offset INTEGER NOT NULL, length INTEGER NOT NULL, sha256 TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS host_windows_document ON host_windows(document_id);
            CREATE VIRTUAL TABLE IF NOT EXISTS host_fts USING fts5(tokens,content='',contentless_delete=1);
            CREATE TRIGGER IF NOT EXISTS host_windows_delete AFTER DELETE ON host_windows BEGIN
                DELETE FROM host_fts WHERE rowid=old.id;
            END;")?;
        conn.execute(
            "INSERT INTO host_fts(host_fts,rank) VALUES('secure-delete',1)",
            [],
        )?;
        Ok(Self { conn })
    }

    /// Delete stale filesystem coordinates after a complete authorized scan.
    pub fn retain_agent_documents(
        &mut self,
        owner: &str,
        keep: &std::collections::HashSet<String>,
    ) -> Result<(), IndexError> {
        self.retain_agent_scope(owner, MemoryScope::All, keep)
    }

    /// A scoped refresh must not prune the other source class or its cursors.
    pub(crate) fn retain_agent_scope(
        &mut self,
        owner: &str,
        scope: MemoryScope,
        keep: &std::collections::HashSet<String>,
    ) -> Result<(), IndexError> {
        let ids = self
            .conn
            .prepare(
                "SELECT id FROM host_documents WHERE agent_id=?1 AND (?2 IS NULL OR source=?2)",
            )?
            .query_map(params![owner, scope.source()], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let tx = self.conn.transaction()?;
        for id in ids {
            if !keep.contains(&id) {
                tx.execute("DELETE FROM host_documents WHERE id=?1", [id])?;
            }
        }
        tx.execute(
            "DELETE FROM host_sources WHERE agent_id=?1 AND segment NOT IN
            (SELECT segment FROM host_documents WHERE agent_id=?1)",
            [owner],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Full verification is hourly per source, serviced one source per daemon
    /// maintenance tick. Attempt ordering prevents a corrupt source monopolizing
    /// the verifier. Backlog may extend the detection delay beyond one hour.
    pub(crate) fn verification_candidate(
        &self,
        now_ms: i64,
    ) -> Result<Option<(String, std::path::PathBuf)>, IndexError> {
        let mut due = self
            .conn
            .prepare("SELECT agent_id,segment,cursor FROM host_sources")?
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .map(|r| -> Result<_, IndexError> {
                let (owner, path, raw) = r?;
                let cursor: crate::host_cursor::SourceCursor = serde_json::from_str(&raw)?;
                Ok((owner, std::path::PathBuf::from(path), cursor))
            })
            .collect::<Result<Vec<_>, _>>()?;
        due.retain(|(_, _, c)| {
            c.policy.starts_with("native-jsonl-v2:")
                && c.full_verified_ms <= now_ms.saturating_sub(3_600_000)
        });
        due.sort_by(|a, b| {
            (a.2.verification_attempt_ms, &a.0, &a.1).cmp(&(
                b.2.verification_attempt_ms,
                &b.0,
                &b.1,
            ))
        });
        Ok(due.into_iter().next().map(|(owner, path, _)| (owner, path)))
    }

    pub(crate) fn source_cursor(
        &self,
        owner: &str,
        path: &Path,
    ) -> Result<Option<crate::host_cursor::SourceCursor>, IndexError> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT cursor FROM host_sources WHERE agent_id=?1 AND segment=?2",
                params![owner, path.to_string_lossy()],
                |r| r.get(0),
            )
            .optional()?;
        raw.map(|s| serde_json::from_str(&s).map_err(IndexError::from))
            .transpose()
    }
    pub(crate) fn source_documents(
        &self,
        owner: &str,
        path: &Path,
    ) -> Result<Vec<String>, IndexError> {
        Ok(self
            .conn
            .prepare("SELECT id FROM host_documents WHERE agent_id=?1 AND segment=?2")?
            .query_map(params![owner, path.to_string_lossy()], |r| r.get(0))?
            .collect::<Result<Vec<_>, _>>()?)
    }
    pub(crate) fn clear_source(&self, owner: &str, path: &Path) -> Result<(), IndexError> {
        self.conn.execute(
            "DELETE FROM host_documents WHERE agent_id=?1 AND segment=?2",
            params![owner, path.to_string_lossy()],
        )?;
        self.conn.execute(
            "DELETE FROM host_sources WHERE agent_id=?1 AND segment=?2",
            params![owner, path.to_string_lossy()],
        )?;
        Ok(())
    }
    pub(crate) fn save_source(
        &self,
        owner: &str,
        path: &Path,
        cursor: &crate::host_cursor::SourceCursor,
    ) -> Result<(), IndexError> {
        self.conn.execute(
            "INSERT INTO host_sources VALUES(?1,?2,?3)
            ON CONFLICT(agent_id,segment) DO UPDATE SET cursor=excluded.cursor",
            params![
                owner,
                path.to_string_lossy(),
                serde_json::to_string(cursor)?
            ],
        )?;
        Ok(())
    }
    /// A source frontier and all its documents/windows publish together. Nested
    /// document savepoints cannot advance a cursor after an interrupted batch.
    pub(crate) fn refresh_source<T>(
        &mut self,
        apply: impl FnOnce(&mut Self) -> Result<T, IndexError>,
    ) -> Result<T, IndexError> {
        self.conn.execute_batch("SAVEPOINT host_source_refresh")?;
        match apply(self) {
            Ok(value) => {
                self.conn.execute_batch("RELEASE host_source_refresh")?;
                Ok(value)
            }
            Err(error) => {
                self.conn.execute_batch(
                    "ROLLBACK TO host_source_refresh; RELEASE host_source_refresh",
                )?;
                Err(error)
            }
        }
    }

    pub fn checkpoint(&self) -> Result<(), IndexError> {
        let result: (i64, i64, i64) =
            self.conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
        if result.0 != 0 {
            return Err(io::Error::other("host index reader prevented secure checkpoint").into());
        }
        Ok(())
    }

    /// Idempotently index a trusted coordinate. Invalid/changed bytes never
    /// publish a partial replacement; the prior index remains available.
    /// At most one 4KiB raw window and its token projection are resident.
    pub fn index(
        &mut self,
        id: &str,
        metadata: &IndexMetadata,
        raw: &RawCoordinate,
    ) -> Result<bool, IndexError> {
        let source = metadata
            .source
            .source()
            .ok_or_else(|| io::Error::other("an indexed document needs one source"))?;
        let unchanged: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT sha256, agent_id FROM host_documents WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if unchanged
            .as_ref()
            .is_some_and(|(_, owner)| owner != &metadata.agent_id)
        {
            return Err(io::Error::other("indexed document owner cannot change").into());
        }
        // Check actual bytes even on an otherwise eligible cache hit.
        verify_coordinate(raw)?;
        let old: Option<(String, String)> = self.conn.query_row(
            "SELECT json_array(segment,offset,length,sha256,seq),json_array(agent_id,source,timestamp_ms,direction,sender,destination,tool_name,role) FROM host_documents WHERE id=?1",
            [id], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
        let raw_identity =
            serde_json::json!([raw.segment, raw.offset, raw.length, raw.sha256, raw.seq])
                .to_string();
        let meta_identity = serde_json::json!([
            metadata.agent_id,
            source,
            metadata.timestamp_ms,
            metadata.direction,
            metadata.sender,
            metadata.destination,
            metadata.tool_name,
            metadata.role
        ])
        .to_string();
        if old
            .as_ref()
            .is_some_and(|(r, m)| r == &raw_identity && m == &meta_identity)
        {
            return Ok(false);
        }
        let tx = self.conn.savepoint()?;
        tx.execute("DELETE FROM host_documents WHERE id=?1", [id])?;
        tx.execute(
            "INSERT INTO host_documents VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![
                id,
                metadata.agent_id,
                source,
                metadata.timestamp_ms,
                metadata.direction,
                metadata.sender,
                metadata.destination,
                metadata.tool_name,
                metadata.role,
                raw.segment.to_string_lossy(),
                raw.offset,
                raw.length,
                raw.sha256,
                raw.seq
            ],
        )?;
        let mut file = crate::raw_segment::open_readonly(&raw.segment)?;
        let mut relative = 0u64;
        let mut json_state = JsonProjection::default();
        let mut bytes = [0u8; WINDOW_BYTES];
        while relative < raw.length {
            let length = (raw.length - relative).min(WINDOW_BYTES as u64) as usize;
            file.seek(SeekFrom::Start(raw.offset + relative))?;
            file.read_exact(&mut bytes[..length])?;
            // Lossy projection is only for search over binary tool outputs.
            // The stored coordinate and the returned range remain exact bytes.
            let mut tokens =
                crate::tokenize_hybrid_search_tokens(&String::from_utf8_lossy(&bytes[..length]));
            // Decode string escapes without materializing an unbounded JSON event.
            // Raw and decoded projections share the original exact-byte window.
            let mut window_state = json_state.clone();
            let decoded = window_state.feed(&bytes[..length]);
            tokens.extend(crate::tokenize_hybrid_search_tokens(
                &String::from_utf8_lossy(&decoded),
            ));
            let tokens = tokens.join(" ");
            let step = if relative + length as u64 == raw.length {
                length
            } else {
                length - OVERLAP_BYTES
            };
            json_state.feed(&bytes[..step]);
            tx.execute(
                "INSERT INTO host_windows(document_id,offset,length,sha256) VALUES(?1,?2,?3,?4)",
                params![
                    id,
                    raw.offset + relative,
                    length,
                    crate::learned_memory::sha256(&bytes[..length])
                ],
            )?;
            let row = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO host_fts(rowid,tokens) VALUES(?1,?2)",
                params![row, tokens],
            )?;
            if relative + length as u64 == raw.length {
                break;
            }
            relative += (length - OVERLAP_BYTES) as u64;
        }
        // Catch mutation during the streaming projection before commit.
        verify_coordinate(raw)?;
        tx.commit()?;
        Ok(true)
    }

    /// ACL selection precedes ranking and LIMIT. An empty visibility set has no
    /// results, not public access. Callers resolve ancestry/privacy in the host.
    pub fn search(
        &self,
        visible_agents: &[String],
        scope: MemoryScope,
        query: &HistorySearchQuery,
    ) -> Result<Vec<IndexHit>, IndexError> {
        self.search_in_segment(visible_agents, scope, query, None)
    }
    fn search_in_segment(
        &self,
        visible_agents: &[String],
        scope: MemoryScope,
        query: &HistorySearchQuery,
        segment: Option<&Path>,
    ) -> Result<Vec<IndexHit>, IndexError> {
        if visible_agents.is_empty() {
            return Ok(Vec::new());
        }
        let mut values: Vec<Value> = visible_agents.iter().cloned().map(Value::Text).collect();
        let placeholders = vec!["?"; values.len()].join(",");
        let mut where_sql = format!("d.agent_id IN ({placeholders})");
        let terms: Vec<String> = query
            .query
            .iter()
            .chain(query.queries.iter())
            .flat_map(|s| crate::tokenize_hybrid_search_tokens(s))
            .collect();
        let lexical = !terms.is_empty();
        let mut push = |clause: &str, value: Value| {
            where_sql.push_str(" AND ");
            where_sql.push_str(clause);
            values.push(value);
        };
        if let Some(segment) = segment {
            push("d.segment=?", segment.to_string_lossy().to_string().into());
        }
        if let Some(agent) = query.agent.as_deref().filter(|a| *a != "all") {
            push("d.agent_id=?", agent.to_owned().into());
        }
        if let Some(source) = scope.source() {
            push("d.source=?", source.to_owned().into());
        }
        if let Some(direction) = query.direction.as_deref().filter(|s| *s != "all") {
            push("d.direction=?", direction.to_owned().into());
        }
        if let Some(sender) = &query.sender {
            push(
                "instr(lower(d.sender),lower(?))>0",
                sender.trim().to_owned().into(),
            );
        }
        if let Some(peer) = &query.with_peer {
            let peer = peer.trim();
            let address = if peer.contains(':') {
                peer.to_owned()
            } else {
                format!("agent:{peer}")
            };
            push(
                "(d.sender=? OR d.destination=? OR d.sender=? OR d.destination=?)",
                peer.to_owned().into(),
            );
            values.push(peer.to_owned().into());
            values.push(address.clone().into());
            values.push(address.into());
        }
        // Tool filters apply identically to both sources; memory does not gain
        // a bypass merely because it is a filesystem document.
        if query.only_tools {
            where_sql.push_str(" AND d.tool_name IS NOT NULL");
        }
        if !query.include_tools && !query.only_tools && query.tool_name.is_none() {
            where_sql.push_str(" AND d.tool_name IS NULL");
        }
        if let Some(tool) = &query.tool_name {
            where_sql.push_str(" AND lower(d.tool_name)=lower(?)");
            values.push(tool.trim().to_owned().into());
        }
        for (clause, value) in [
            ("d.timestamp_ms>?", query.since_ms),
            ("d.timestamp_ms<=?", query.until_ms),
            ("d.seq>=?", query.from_seq),
            ("d.seq<=?", query.to_seq),
        ] {
            if let Some(value) = value {
                where_sql.push_str(" AND ");
                where_sql.push_str(clause);
                values.push(value.into());
            }
        }
        if lexical {
            where_sql.push_str(" AND host_fts MATCH ?");
            values.push(
                terms
                    .into_iter()
                    .map(|s| format!("\"{}\"", s.replace('"', "\"\"")))
                    .collect::<Vec<_>>()
                    .join(" OR ")
                    .into(),
            );
        }
        let tail_default = !lexical && query.sort == HistorySort::Relevance;
        let order = if query.sort == HistorySort::Oldest {
            "d.timestamp_ms ASC,d.seq ASC,d.id ASC"
        } else if tail_default || query.sort == HistorySort::Newest {
            "d.timestamp_ms DESC,d.seq DESC,d.id DESC"
        } else {
            "score ASC,d.timestamp_ms DESC,d.seq DESC"
        };
        let score = if lexical { "bm25(host_fts)" } else { "0.0" };
        let join = if lexical {
            "JOIN host_fts ON host_fts.rowid=w.id"
        } else {
            ""
        };
        let sql = format!("SELECT d.id,d.agent_id,d.source,d.timestamp_ms,d.direction,d.sender,d.destination,d.tool_name,d.role,
            d.segment,d.offset,d.length,d.sha256,d.seq,w.offset,w.length,w.sha256,{score} AS score
            FROM host_documents d JOIN host_windows w ON w.document_id=d.id {join}
            WHERE {where_sql} ORDER BY {order}");
        let mut statement = self.conn.prepare(&sql)?;
        let mut rows = statement.query(params_from_iter(values))?;
        let mut seen = std::collections::HashSet::new();
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            if !seen.insert(id.clone()) {
                continue;
            }
            let raw = RawCoordinate {
                segment: row.get::<_, String>(9)?.into(),
                offset: row.get(10)?,
                length: row.get(11)?,
                sha256: row.get(12)?,
                seq: row.get(13)?,
            };
            if has_excluded_text(&raw, &query.exclude)? {
                continue;
            }
            hits.push(IndexHit {
                id,
                metadata: IndexMetadata {
                    agent_id: row.get(1)?,
                    source: match row.get::<_, String>(2)?.as_str() {
                        "memory" => MemoryScope::Memory,
                        _ => MemoryScope::Transcripts,
                    },
                    timestamp_ms: row.get(3)?,
                    direction: row.get(4)?,
                    sender: row.get(5)?,
                    destination: row.get(6)?,
                    tool_name: row.get(7)?,
                    role: row.get(8)?,
                },
                window: RawCoordinate {
                    segment: raw.segment.clone(),
                    offset: row.get(14)?,
                    length: row.get(15)?,
                    sha256: row.get(16)?,
                    seq: raw.seq,
                },
                raw,
                lexical_score: row.get(17)?,
            });
            if hits.len() >= query.limit.clamp(1, 200) {
                break;
            }
        }
        if tail_default {
            hits.reverse();
        }
        if query.radius > 0 {
            let mut expanded = hits.clone();
            let mut ids: std::collections::HashSet<_> =
                expanded.iter().map(|h| h.id.clone()).collect();
            for hit in &hits {
                let mut neighbors = query.clone();
                neighbors.query = None;
                neighbors.queries.clear();
                neighbors.radius = 0;
                neighbors.sort = HistorySort::Oldest;
                let radius = query.radius.min(10) as i64;
                let seq = hit.raw.seq as i64;
                neighbors.from_seq =
                    Some(query.from_seq.unwrap_or(0).max(seq.saturating_sub(radius)));
                neighbors.to_seq = Some(
                    query
                        .to_seq
                        .unwrap_or(i64::MAX)
                        .min(seq.saturating_add(radius)),
                );
                neighbors.limit = 21;
                for neighbor in self.search_in_segment(
                    std::slice::from_ref(&hit.metadata.agent_id),
                    scope,
                    &neighbors,
                    Some(&hit.raw.segment),
                )? {
                    // Sequence coordinates are local to one segment.
                    if neighbor.raw.segment == hit.raw.segment && ids.insert(neighbor.id.clone()) {
                        expanded.push(neighbor);
                    }
                }
            }
            expanded.sort_by(|a, b| {
                (a.metadata.timestamp_ms, a.raw.seq, &a.id).cmp(&(
                    b.metadata.timestamp_ms,
                    b.raw.seq,
                    &b.id,
                ))
            });
            return Ok(expanded);
        }
        Ok(hits)
    }
}

/// Streaming JSON-string projection; raw coverage remains independent. State
/// survives window boundaries, including escapes and UTF-16 surrogate pairs.
#[derive(Clone, Default)]
struct JsonProjection {
    in_string: bool,
    escaped: bool,
    unicode: Option<(u16, u8)>,
    high: Option<u16>,
}
impl JsonProjection {
    fn feed(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes.len());
        for &b in bytes {
            if let Some((value, n)) = self.unicode {
                if let Some(digit) = (b as char).to_digit(16) {
                    let value = (value << 4) | digit as u16;
                    if n == 3 {
                        self.unicode = None;
                        if (0xd800..=0xdbff).contains(&value) {
                            self.high = Some(value);
                        } else {
                            let code = if (0xdc00..=0xdfff).contains(&value) {
                                self.high.take().map(|high| {
                                    0x10000
                                        + (((high as u32 - 0xd800) << 10) | (value as u32 - 0xdc00))
                                })
                            } else {
                                self.high = None;
                                Some(value as u32)
                            };
                            if let Some(ch) = code.and_then(char::from_u32) {
                                let mut buffer = [0; 4];
                                out.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
                            }
                        }
                    } else {
                        self.unicode = Some((value, n + 1));
                    }
                } else {
                    self.unicode = None;
                    out.push(b' ');
                }
                continue;
            }
            if self.escaped {
                self.escaped = false;
                match b {
                    b'u' => self.unicode = Some((0, 0)),
                    b'n' | b'r' | b't' | b'b' | b'f' => out.push(b' '),
                    b'"' | b'\\' | b'/' => out.push(b),
                    _ => out.push(b' '),
                }
                continue;
            }
            if b == b'"' {
                self.in_string = !self.in_string;
                out.push(b' ');
            } else if self.in_string && b == b'\\' {
                self.escaped = true;
            } else if self.in_string {
                out.push(b);
            }
        }
        out
    }
}

fn verify_coordinate(raw: &RawCoordinate) -> io::Result<()> {
    let mut file = crate::raw_segment::open_readonly(&raw.segment)?;
    file.seek(SeekFrom::Start(raw.offset))?;
    let mut reader = file.take(raw.length);
    let mut hash = Sha256::new();
    let mut count = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        count += n as u64;
    }
    if count != raw.length || format!("{:x}", hash.finalize()) != raw.sha256 {
        return Err(io::Error::other("index coordinate integrity mismatch"));
    }
    Ok(())
}

fn has_excluded_text(raw: &RawCoordinate, excludes: &[String]) -> io::Result<bool> {
    if excludes.is_empty() {
        return Ok(false);
    }
    let terms: Vec<String> = excludes
        .iter()
        .map(|s| s.to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    if terms.is_empty() {
        return Ok(false);
    }
    let overlap = terms.iter().map(String::len).max().unwrap_or_default() + 4;
    let mut file = crate::raw_segment::open_readonly(&raw.segment)?;
    file.seek(SeekFrom::Start(raw.offset))?;
    let mut reader = file.take(raw.length);
    let mut buffer = [0u8; WINDOW_BYTES];
    let mut carry = Vec::new();
    let mut decoded_carry = Vec::new();
    let mut json_state = JsonProjection::default();
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        carry.extend_from_slice(&buffer[..n]);
        decoded_carry.extend(json_state.feed(&buffer[..n]));
        let text = String::from_utf8_lossy(&carry).to_lowercase();
        let decoded = String::from_utf8_lossy(&decoded_carry).to_lowercase();
        if terms
            .iter()
            .any(|s| text.contains(s) || decoded.contains(s))
        {
            return Ok(true);
        }
        if carry.len() > overlap {
            carry.drain(..carry.len() - overlap);
        }
        if decoded_carry.len() > overlap {
            decoded_carry.drain(..decoded_carry.len() - overlap);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw_segment::{RawSegment, read_raw};
    fn metadata(owner: &str, source: MemoryScope, time: i64) -> IndexMetadata {
        IndexMetadata {
            agent_id: owner.into(),
            source,
            timestamp_ms: time,
            direction: "in".into(),
            sender: "user".into(),
            destination: owner.into(),
            tool_name: None,
            role: Some("user".into()),
        }
    }
    #[test]
    fn serialized_json_escapes_are_searchable_with_exact_raw_coordinates() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = HostIndex::open(&dir.path().join("index.db")).unwrap();
        let segment = RawSegment::open(&dir.path().join("events.raw")).unwrap();
        let event = serde_json::json!({"tool":"browser","output":format!("{}prefix\nsuffixneedle\ttabneedle 繁體中文","padding ".repeat(1800))});
        let bytes = serde_json::to_vec(&event).unwrap();
        let escaped = br#"{"unicode":"\u7e41\u9ad4\u4e2d\u6587","surrogate":"\ud83d\ude00"}"#;
        assert!(serde_json::from_slice::<serde_json::Value>(escaped).is_ok());
        index
            .index(
                "escaped",
                &metadata("a", MemoryScope::Transcripts, 0),
                &segment.append(escaped).unwrap(),
            )
            .unwrap();
        let raw = segment.append(&bytes).unwrap();
        index
            .index("json", &metadata("a", MemoryScope::Transcripts, 1), &raw)
            .unwrap();
        for term in ["suffixneedle", "tabneedle", "繁體中文"] {
            let q = HistorySearchQuery {
                query: Some(term.into()),
                ..Default::default()
            };
            let hits = index.search(&["a".into()], MemoryScope::All, &q).unwrap();
            assert_eq!(hits.len(), if term == "繁體中文" { 2 } else { 1 }, "{term}");
            assert_eq!(
                read_raw(&hits.iter().find(|h| h.id == "json").unwrap().raw).unwrap(),
                bytes
            );
        }
        let q = HistorySearchQuery {
            query: Some("absentneedle".into()),
            ..Default::default()
        };
        assert!(
            index
                .search(&["a".into()], MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
        let mut state = JsonProjection::default();
        let mut decoded = Vec::new();
        for part in br#"{"x":"\u7e41\u9ad4\ud83d\ude00\nsuffix"}"#.chunks(3) {
            decoded.extend(state.feed(part));
        }
        assert!(
            String::from_utf8(decoded)
                .unwrap()
                .contains("繁體😀 suffix")
        );
    }
    #[test]
    fn decoded_exclusion_and_crowded_segment_neighbors_keep_same_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = HostIndex::open(&dir.path().join("index.db")).unwrap();
        for i in 0..25 {
            let segment = RawSegment::open(&dir.path().join(format!("old-{i}.raw"))).unwrap();
            index
                .index(
                    &format!("old-{i}"),
                    &metadata("a", MemoryScope::Transcripts, i),
                    &segment.append(b"other segment").unwrap(),
                )
                .unwrap();
        }
        let segment = RawSegment::open(&dir.path().join("wanted.raw")).unwrap();
        for (id, text) in [
            ("before", b"neighbor before".as_slice()),
            ("match", br#"{"x":"\u7e41\u9ad4"}"#.as_slice()),
            ("after", b"neighbor after".as_slice()),
        ] {
            index
                .index(
                    id,
                    &metadata("a", MemoryScope::Transcripts, 100),
                    &segment.append(text).unwrap(),
                )
                .unwrap();
        }
        let secret = segment.append(b"private neighbor").unwrap();
        index
            .index(
                "secret",
                &metadata("private", MemoryScope::Transcripts, 100),
                &secret,
            )
            .unwrap();
        let mut q = HistorySearchQuery {
            query: Some("繁體".into()),
            radius: 2,
            limit: 1,
            ..Default::default()
        };
        let hits = index
            .search(&["a".into()], MemoryScope::Transcripts, &q)
            .unwrap();
        assert_eq!(
            hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(),
            ["before", "match", "after"]
        );
        q.exclude = vec!["繁體".into()];
        assert!(
            index
                .search(&["a".into()], MemoryScope::Transcripts, &q)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn latest_tail_explicit_order_tool_precedence_and_neighbor_acl() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = HostIndex::open(&dir.path().join("index.db")).unwrap();
        let segment = RawSegment::open(&dir.path().join("events.raw")).unwrap();
        for (i, owner, tool, text) in [
            (1, "a", None, "first"),
            (2, "a", Some("browser"), "matchneedle"),
            (3, "private", Some("browser"), "secret neighbor"),
            (4, "a", Some("browser"), "last"),
        ] {
            let mut m = metadata(owner, MemoryScope::Transcripts, i);
            m.tool_name = tool.map(str::to_owned);
            index
                .index(
                    &i.to_string(),
                    &m,
                    &segment.append(text.as_bytes()).unwrap(),
                )
                .unwrap();
        }
        let visible = vec!["a".into()];
        let mut q = HistorySearchQuery {
            limit: 2,
            ..Default::default()
        };
        let ids = |hits: Vec<IndexHit>| hits.into_iter().map(|h| h.id).collect::<Vec<_>>();
        assert_eq!(
            ids(index.search(&visible, MemoryScope::All, &q).unwrap()),
            ["2", "4"]
        );
        q.sort = HistorySort::Newest;
        assert_eq!(
            ids(index.search(&visible, MemoryScope::All, &q).unwrap()),
            ["4", "2"]
        );
        q.sort = HistorySort::Oldest;
        assert_eq!(
            ids(index.search(&visible, MemoryScope::All, &q).unwrap()),
            ["1", "2"]
        );
        q.include_tools = false;
        q.only_tools = true;
        assert_eq!(
            ids(index.search(&visible, MemoryScope::All, &q).unwrap()),
            ["2", "4"]
        );
        q.only_tools = false;
        q.tool_name = Some("browser".into());
        assert_eq!(
            ids(index.search(&visible, MemoryScope::All, &q).unwrap()),
            ["2", "4"]
        );
        q.query = Some("matchneedle".into());
        q.radius = 2;
        q.limit = 1;
        assert_eq!(
            ids(index.search(&visible, MemoryScope::All, &q).unwrap()),
            ["2", "4"]
        );
        q.agent = Some("private".into());
        assert!(
            index
                .search(&visible, MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn full_window_coverage_exact_readback_restart_and_idempotence() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let raw = RawSegment::open(&dir.path().join("events.raw")).unwrap();
        let text = format!(
            "{}末端繁體中文 needle_late_after_2400 {}",
            "padding ".repeat(2000),
            "x".repeat(6000)
        );
        let coordinate = raw.append(text.as_bytes()).unwrap();
        let meta = metadata("agent-a", MemoryScope::Transcripts, 10);
        let mut index = HostIndex::open(&db).unwrap();
        assert!(index.index("event-1", &meta, &coordinate).unwrap());
        assert!(!index.index("event-1", &meta, &coordinate).unwrap());
        let mut q = HistorySearchQuery {
            query: Some("needle_late_after_2400".into()),
            ..Default::default()
        };
        let hit = index
            .search(&["agent-a".into()], MemoryScope::All, &q)
            .unwrap()
            .remove(0);
        assert!(
            String::from_utf8(read_raw(&hit.window).unwrap())
                .unwrap()
                .contains("needle_late_after_2400")
        );
        assert_eq!(read_raw(&hit.raw).unwrap(), text.as_bytes());
        drop(index);
        let index = HostIndex::open(&db).unwrap();
        q.query = Some("繁體中文".into());
        assert_eq!(
            index
                .search(&["agent-a".into()], MemoryScope::Transcripts, &q)
                .unwrap()
                .len(),
            1
        );
        // The FTS table has no recoverable text/token payload column.
        let stored: Option<String> = index
            .conn
            .query_row("SELECT tokens FROM host_fts LIMIT 1", [], |r| r.get(0))
            .unwrap();
        assert!(stored.is_none());
    }
    #[test]
    fn visibility_scope_filters_chronology_and_full_record_exclusion() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = HostIndex::open(&dir.path().join("index.db")).unwrap();
        let segment = RawSegment::open(&dir.path().join("events.raw")).unwrap();
        for (id, owner, source, time, tool, text) in [
            (
                "a",
                "agent-a",
                MemoryScope::Memory,
                20,
                None,
                "shared match memory".to_string(),
            ),
            (
                "b",
                "agent-a",
                MemoryScope::Transcripts,
                10,
                Some("browser"),
                format!("match {} hidden_exclude", "padding ".repeat(2000)),
            ),
            (
                "private",
                "agent-b",
                MemoryScope::Memory,
                1,
                None,
                "shared match private".to_string(),
            ),
        ] {
            let mut m = metadata(owner, source, time);
            m.tool_name = tool.map(str::to_owned);
            index
                .index(id, &m, &segment.append(text.as_bytes()).unwrap())
                .unwrap();
        }
        let visible = vec!["agent-a".into()];
        let mut q = HistorySearchQuery::default();
        assert_eq!(
            index
                .search(&visible, MemoryScope::All, &q)
                .unwrap()
                .iter()
                .map(|h| h.id.as_str())
                .collect::<Vec<_>>(),
            ["b", "a"]
        );
        assert!(index.search(&[], MemoryScope::All, &q).unwrap().is_empty());
        assert_eq!(
            index.search(&visible, MemoryScope::Memory, &q).unwrap()[0].id,
            "a"
        );
        q.query = Some("match".into());
        q.tool_name = Some("browser".into());
        assert_eq!(
            index
                .search(&visible, MemoryScope::Transcripts, &q)
                .unwrap()[0]
                .id,
            "b"
        );
        q.exclude = vec!["hidden_exclude".into()];
        assert!(
            index
                .search(&visible, MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
        q.exclude.clear();
        q.since_ms = Some(10);
        assert!(
            index
                .search(&visible, MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn changed_metadata_reindexes_but_tampered_bytes_and_owner_do_not_publish() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = HostIndex::open(&dir.path().join("index.db")).unwrap();
        let path = dir.path().join("memory.md");
        std::fs::write(&path, b"original learned fact").unwrap();
        let raw = RawCoordinate {
            segment: path.clone(),
            offset: 0,
            length: 21,
            sha256: crate::learned_memory::sha256(b"original learned fact"),
            seq: 1,
        };
        let mut m = metadata("agent-a", MemoryScope::Memory, 1);
        index.index("doc", &m, &raw).unwrap();
        m.tool_name = Some("browser".into());
        assert!(index.index("doc", &m, &raw).unwrap());
        m.agent_id = "agent-b".into();
        assert!(
            index
                .index("doc", &m, &raw)
                .unwrap_err()
                .to_string()
                .contains("owner")
        );
        m.agent_id = "agent-a".into();
        std::fs::write(&path, b"changed learned facts").unwrap();
        assert!(
            index
                .index("doc", &m, &raw)
                .unwrap_err()
                .to_string()
                .contains("integrity")
        );
        let q = HistorySearchQuery {
            query: Some("original".into()),
            ..Default::default()
        };
        let hit = index
            .search(&["agent-a".into()], MemoryScope::All, &q)
            .unwrap()
            .remove(0);
        assert!(read_raw(&hit.window).is_err());
    }
}
