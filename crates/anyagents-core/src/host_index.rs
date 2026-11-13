//! Rebuildable host index: metadata and contentless FTS windows only.
//! Raw bytes stay in their original filesystem segment. Every result requires
//! an explicit host-authorized agent set, including learned-memory results.
use crate::{
    HistorySearchQuery, HistorySort,
    raw_segment::{RawCoordinate, RawSource},
};
use rusqlite::{
    Connection, OptionalExtension, TransactionBehavior, params, params_from_iter, types::Value,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Read},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
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
    pub(crate) fn source(self) -> Option<&'static str> {
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
    /// Native events without a source timestamp are explicitly unknown.
    pub timestamp_ms: Option<i64>,
    pub direction: String,
    pub sender: String,
    pub destination: String,
    pub tool_name: Option<String>,
    /// All tool memberships for native events containing multiple tool blocks.
    #[serde(default)]
    pub tool_names: Vec<String>,
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

fn decode_tool_names(value: String) -> rusqlite::Result<Vec<String>> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

pub struct HostIndex {
    conn: Connection,
}

/// Index connections kept open for reuse by this process: opening one maps
/// the WAL and probes the schema, so callers borrow an idle connection and
/// return it when done. At most `MAX_IDLE_PER_FILE` stay idle per index
/// file, and a connection whose file was deleted or replaced is closed.
static IDLE: Mutex<Vec<IdleIndex>> = Mutex::new(Vec::new());
const MAX_IDLE_PER_FILE: usize = 4;
struct IdleIndex {
    path: PathBuf,
    identity: (u64, u64),
    index: HostIndex,
}
impl IdleIndex {
    fn is_current(&self) -> bool {
        file_identity(&self.path).is_ok_and(|now| now == Some(self.identity))
    }
}
fn file_identity(path: &Path) -> io::Result<Option<(u64, u64)>> {
    use std::os::unix::fs::MetadataExt;
    match fs::metadata(path) {
        Ok(m) => Ok(Some((m.dev(), m.ino()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
fn idle_pool() -> MutexGuard<'static, Vec<IdleIndex>> {
    IDLE.lock().unwrap_or_else(|p| p.into_inner())
}

/// A [`HostIndex`] borrowed from this process's pool; returned on drop.
pub(crate) struct SharedHostIndex(Option<IdleIndex>);
impl Deref for SharedHostIndex {
    type Target = HostIndex;
    fn deref(&self) -> &HostIndex {
        &self.0.as_ref().expect("borrowed until drop").index
    }
}
impl DerefMut for SharedHostIndex {
    fn deref_mut(&mut self) -> &mut HostIndex {
        &mut self.0.as_mut().expect("borrowed until drop").index
    }
}
impl Drop for SharedHostIndex {
    fn drop(&mut self) {
        // A connection left inside a transaction is closed, never reused.
        let Some(idle) = self.0.take().filter(|i| i.index.conn.is_autocommit()) else {
            return;
        };
        let closed: Vec<IdleIndex> = {
            let mut pool = idle_pool();
            pool.push(idle);
            let mut closed = Vec::new();
            let mut kept: Vec<IdleIndex> = Vec::with_capacity(pool.len());
            for entry in pool.drain(..).rev() {
                let same_file = kept.iter().filter(|k| k.path == entry.path).count();
                if same_file < MAX_IDLE_PER_FILE && entry.is_current() {
                    kept.push(entry);
                } else {
                    closed.push(entry);
                }
            }
            kept.reverse();
            *pool = kept;
            closed
        };
        // Closing a connection can checkpoint; never under the pool lock.
        drop(closed);
    }
}

impl HostIndex {
    /// Borrow an idle connection to the index at `path`, opening (and
    /// creating) one only when none is idle.
    pub(crate) fn shared(path: &Path) -> Result<SharedHostIndex, IndexError> {
        let reused = file_identity(path)?.and_then(|identity| {
            let mut pool = idle_pool();
            let at = pool
                .iter()
                .rposition(|i| i.path == path && i.identity == identity)?;
            Some(pool.remove(at))
        });
        if let Some(idle) = reused {
            return Ok(SharedHostIndex(Some(idle)));
        }
        let index = Self::open(path)?;
        let identity = file_identity(path)?
            .ok_or_else(|| io::Error::other("host index vanished while opening"))?;
        Ok(SharedHostIndex(Some(IdleIndex {
            path: path.to_owned(),
            identity,
            index,
        })))
    }
}

#[cfg(test)]
thread_local! {
    /// Connections opened on this thread.
    static OPENS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
impl HostIndex {
    fn schema_state(conn: &Connection) -> Result<(bool, bool), rusqlite::Error> {
        let old_required_time = conn
            .prepare("PRAGMA table_info(host_documents)")?
            .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(3)?)))?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .any(|(name, required)| name == "timestamp_ms" && required != 0);
        let already_initialized = !old_required_time
            && conn
                .query_row(
                    "SELECT count(*)=2 FROM sqlite_master WHERE type='table' AND name IN ('host_fts','host_agent_trees')",
                    [],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(false)
            && conn
                .query_row(
                    "SELECT 1 FROM host_fts_config WHERE k='secure-delete' AND v=1",
                    [],
                    |_| Ok(true),
                )
                .optional()
                .unwrap_or(None)
                .unwrap_or(false);
        Ok((old_required_time, already_initialized))
    }

    /// `host_calls.document_id` references `host_documents(id) ON DELETE
    /// CASCADE`. Without an index on the child column, SQLite enforces the
    /// cascade by scanning the whole `host_calls` table once per deleted
    /// document: tens of megabytes per document on a large index, gigabytes
    /// for one cleared source, all under the index's write lock and the
    /// owner's lifecycle lock. Files created before this index get it on
    /// their next open; a peer holding the write lock defers it to a later one.
    fn ensure_call_document_index(conn: &Connection) -> Result<(), IndexError> {
        let present: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='host_calls_document')",
            [],
            |r| r.get(0),
        )?;
        if present {
            return Ok(());
        }
        match conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS host_calls_document ON host_calls(document_id);",
        ) {
            Ok(()) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, _))
                if matches!(
                    error.code,
                    rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn open(path: &Path) -> Result<Self, IndexError> {
        #[cfg(test)]
        OPENS.with(|n| n.set(n.get() + 1));
        match Self::open_inner(path) {
            Err(IndexError::Sql(error))
                if error.to_string().contains("malformed")
                    || error.to_string().contains("not a database") =>
            {
                // Derived projection, not a raw-history authority: a
                // malformed image (e.g. restored across volumes) is
                // rebuilt from source instead of failing every verify.
                let _ = fs::remove_file(path);
                let _ = fs::remove_file(path.with_extension("db-wal"));
                let _ = fs::remove_file(path.with_extension("db-shm"));
                Self::open_inner(path)
            }
            other => other,
        }
    }

    fn open_inner(path: &Path) -> Result<Self, IndexError> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA secure_delete=ON;",
        )?;
        // This index is a rebuildable projection, not a raw-history authority.
        // Early unreleased PR1 caches used mandatory mtime fallbacks. Invalidate
        // those projections rather than presenting guessed historical times.
        let (_, already_initialized) = Self::schema_state(&conn)?;
        if already_initialized {
            Self::ensure_call_document_index(&conn)?;
            return Ok(Self { conn });
        }
        // Serialize first-time schema creation and disposable-schema upgrades so
        // concurrent readers/writers never race unserialized DDL or FTS config writes.
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        let init_result = (|| -> Result<bool, IndexError> {
            let (still_old_required, now_initialized) = Self::schema_state(&conn)?;
            if now_initialized {
                return Ok(false);
            }
            if still_old_required {
                conn.execute_batch(
                    "
                DROP TRIGGER IF EXISTS host_windows_delete;
                DROP TABLE IF EXISTS host_windows;
                DROP TABLE IF EXISTS host_calls;
                DROP TABLE IF EXISTS host_document_tools;
                DROP TABLE IF EXISTS host_documents;
                DROP TABLE IF EXISTS host_sources;
                DROP TABLE IF EXISTS host_agent_trees;
                DROP TABLE IF EXISTS host_fts;",
                )?;
            }
            conn.execute_batch("CREATE TABLE IF NOT EXISTS host_documents (
                id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, source TEXT NOT NULL,
                timestamp_ms INTEGER, direction TEXT NOT NULL,
                sender TEXT NOT NULL, destination TEXT NOT NULL, tool_name TEXT,
                role TEXT, segment TEXT NOT NULL, offset INTEGER NOT NULL,
                length INTEGER NOT NULL, sha256 TEXT NOT NULL, seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS host_document_tools (
                document_id TEXT NOT NULL REFERENCES host_documents(id) ON DELETE CASCADE,
                name TEXT NOT NULL, PRIMARY KEY(document_id,name));
            CREATE TABLE IF NOT EXISTS host_calls (
                agent_id TEXT NOT NULL, segment TEXT NOT NULL, call_id TEXT NOT NULL,
                tool_name TEXT NOT NULL, document_id TEXT NOT NULL REFERENCES host_documents(id) ON DELETE CASCADE,
                PRIMARY KEY(agent_id,segment,call_id));
            CREATE INDEX IF NOT EXISTS host_calls_document ON host_calls(document_id);
            CREATE TABLE IF NOT EXISTS host_sources (
                agent_id TEXT NOT NULL, segment TEXT NOT NULL, cursor TEXT NOT NULL,
                PRIMARY KEY(agent_id,segment));
            CREATE TABLE IF NOT EXISTS host_agent_trees (
                agent_id TEXT NOT NULL, class TEXT NOT NULL, policy TEXT NOT NULL,
                tree TEXT NOT NULL, PRIMARY KEY(agent_id,class));
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
            Ok(still_old_required)
        })();
        match init_result {
            Ok(upgraded) => {
                conn.execute_batch("COMMIT;")?;
                if upgraded {
                    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
                }
                Ok(Self { conn })
            }
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK;");
                Err(error)
            }
        }
    }

    /// Delete stale filesystem coordinates after a complete authorized scan.
    /// Documents can vanish here while their files stay unchanged, so the
    /// agent's recorded walks go too and its next search walks again.
    pub fn retain_agent_documents(
        &mut self,
        owner: &str,
        keep: &std::collections::HashSet<String>,
    ) -> Result<(), IndexError> {
        let ids = self
            .conn
            .prepare("SELECT id FROM host_documents WHERE agent_id=?1")?
            .query_map([owner], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let stale: Vec<String> = ids.into_iter().filter(|id| !keep.contains(id)).collect();
        if stale.is_empty() {
            let has_orphan_source: bool = self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM host_sources WHERE agent_id=?1 AND segment NOT IN (SELECT segment FROM host_documents WHERE agent_id=?1))",
                [owner],
                |r| r.get(0),
            )?;
            if !has_orphan_source {
                return Ok(());
            }
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for id in stale {
            tx.execute("DELETE FROM host_documents WHERE id=?1", [id])?;
        }
        tx.execute(
            "DELETE FROM host_sources WHERE agent_id=?1 AND segment NOT IN
            (SELECT segment FROM host_documents WHERE agent_id=?1)",
            [owner],
        )?;
        tx.execute("DELETE FROM host_agent_trees WHERE agent_id=?1", [owner])?;
        tx.commit()?;
        Ok(())
    }

    /// Every published source of one agent with its cursor: one row per file,
    /// never one per document.
    pub(crate) fn agent_sources(&self, owner: &str) -> Result<Vec<(String, String)>, IndexError> {
        Ok(self
            .conn
            .prepare_cached("SELECT segment, cursor FROM host_sources WHERE agent_id=?1")?
            .query_map([owner], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?)
    }

    /// Drop sources whose files a complete walk no longer finds, with all
    /// their documents. Every document belongs to a published source.
    pub(crate) fn forget_sources(
        &mut self,
        owner: &str,
        segments: &[String],
    ) -> Result<(), IndexError> {
        if segments.is_empty() {
            return Ok(());
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for segment in segments {
            tx.execute(
                "DELETE FROM host_documents WHERE agent_id=?1 AND segment=?2",
                params![owner, segment],
            )?;
            tx.execute(
                "DELETE FROM host_sources WHERE agent_id=?1 AND segment=?2",
                params![owner, segment],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// The walk last recorded for one class of an agent: `(policy, tree)`.
    pub(crate) fn agent_tree(
        &self,
        owner: &str,
        class: &str,
    ) -> Result<Option<(String, String)>, IndexError> {
        Ok(self
            .conn
            .prepare_cached(
                "SELECT policy, tree FROM host_agent_trees WHERE agent_id=?1 AND class=?2",
            )?
            .query_row([owner, class], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?)
    }
    pub(crate) fn save_agent_tree(
        &self,
        owner: &str,
        class: &str,
        policy: &str,
        tree: &str,
    ) -> Result<(), IndexError> {
        self.conn
            .prepare_cached(
                "INSERT INTO host_agent_trees VALUES(?1,?2,?3,?4)
                ON CONFLICT(agent_id,class) DO UPDATE SET policy=excluded.policy, tree=excluded.tree",
            )?
            .execute([owner, class, policy, tree])?;
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
            c.policy.starts_with("native-jsonl-v4:")
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
    #[cfg(test)]
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
    pub(crate) fn call_tool(
        &self,
        owner: &str,
        path: &Path,
        call_id: &str,
    ) -> Result<Option<String>, IndexError> {
        Ok(self
            .conn
            .query_row(
                "SELECT tool_name FROM host_calls WHERE agent_id=?1 AND segment=?2 AND call_id=?3",
                params![owner, path.to_string_lossy(), call_id],
                |r| r.get(0),
            )
            .optional()?)
    }
    pub(crate) fn save_call(
        &self,
        owner: &str,
        path: &Path,
        call_id: &str,
        name: &str,
        document: &str,
    ) -> Result<(), IndexError> {
        self.conn.execute("INSERT INTO host_calls VALUES(?1,?2,?3,?4,?5) ON CONFLICT(agent_id,segment,call_id) DO UPDATE SET tool_name=excluded.tool_name,document_id=excluded.document_id",
            params![owner,path.to_string_lossy(),call_id,name,document])?;
        Ok(())
    }
    fn document_tools(&self, document: &str) -> Result<Vec<String>, rusqlite::Error> {
        self.conn
            .prepare("SELECT name FROM host_document_tools WHERE document_id=?1 ORDER BY name")?
            .query_map([document], |r| r.get(0))?
            .collect()
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
    fn in_write_scope<T>(
        &mut self,
        savepoint: &str,
        apply: impl FnOnce(&mut Self) -> Result<T, IndexError>,
    ) -> Result<T, IndexError> {
        let started_tx = self.conn.is_autocommit();
        if started_tx {
            self.conn.execute_batch("BEGIN IMMEDIATE")?;
        }
        if let Err(error) = self.conn.execute_batch(&format!("SAVEPOINT {savepoint}")) {
            if started_tx {
                let _ = self.conn.execute_batch("ROLLBACK");
            }
            return Err(error.into());
        }
        match apply(self) {
            Ok(value) => {
                self.conn.execute_batch(&format!("RELEASE {savepoint}"))?;
                if started_tx {
                    self.conn.execute_batch("COMMIT")?;
                }
                Ok(value)
            }
            Err(error) => {
                let _ = self
                    .conn
                    .execute_batch(&format!("ROLLBACK TO {savepoint}; RELEASE {savepoint}"));
                if started_tx {
                    let _ = self.conn.execute_batch("ROLLBACK");
                }
                Err(error)
            }
        }
    }

    pub(crate) fn refresh_source<T>(
        &mut self,
        apply: impl FnOnce(&mut Self) -> Result<T, IndexError>,
    ) -> Result<T, IndexError> {
        self.in_write_scope("host_source_refresh", apply)
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
        let mut source = RawSource::open(&raw.segment)?;
        // Check actual bytes even on an otherwise eligible cache hit.
        source.verify(raw)?;
        self.index_from(&mut source, id, metadata, raw)
    }

    /// Index a coordinate that was just hashed from `source`'s descriptor, so
    /// an unchanged document needs no reread. A changed one is projected from
    /// the same descriptor and refused unless the projected bytes are exactly
    /// the coordinate's bytes.
    pub(crate) fn index_from(
        &mut self,
        source: &mut RawSource,
        id: &str,
        metadata: &IndexMetadata,
        raw: &RawCoordinate,
    ) -> Result<bool, IndexError> {
        self.in_write_scope("host_document_index", |this| {
            this.index_in_write_scope(source, id, metadata, raw)
        })
    }

    fn index_in_write_scope(
        &mut self,
        file: &mut RawSource,
        id: &str,
        metadata: &IndexMetadata,
        raw: &RawCoordinate,
    ) -> Result<bool, IndexError> {
        if raw.segment != file.path() {
            return Err(io::Error::other("indexed coordinate names another source").into());
        }
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
        let mut names = metadata.tool_names.clone();
        names.extend(metadata.tool_name.iter().cloned());
        names.sort();
        names.dedup();
        if self.document_tools(id)? == names
            && old
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
        for name in names {
            tx.execute(
                "INSERT INTO host_document_tools VALUES(?1,?2)",
                params![id, name],
            )?;
        }
        // One sequential pass: each window keeps the previous window's last
        // OVERLAP_BYTES and reads only its new bytes, which are also hashed.
        let mut reader = file.range(raw.offset, raw.length);
        let mut hash = Sha256::new();
        let mut carried = 0;
        let mut relative = 0u64;
        let mut json_state = JsonProjection::default();
        let mut bytes = [0u8; WINDOW_BYTES];
        while relative < raw.length {
            let length = (raw.length - relative).min(WINDOW_BYTES as u64) as usize;
            reader
                .read_exact(&mut bytes[carried..length])
                .map_err(|e| match e.kind() {
                    io::ErrorKind::UnexpectedEof => integrity_mismatch(),
                    _ => e,
                })?;
            hash.update(&bytes[carried..length]);
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
            bytes.copy_within(length - OVERLAP_BYTES..length, 0);
            carried = OVERLAP_BYTES;
            relative += (length - OVERLAP_BYTES) as u64;
        }
        // Never publish a projection of bytes other than the coordinate's.
        if format!("{:x}", hash.finalize()) != raw.sha256 {
            return Err(integrity_mismatch().into());
        }
        tx.commit()?;
        Ok(true)
    }

    /// Resolve an opaque document ID only within the host-authorized owner set.
    /// This lookup performs no filesystem reads and accepts no caller path.
    pub(crate) fn document(
        &self,
        visible: &[String],
        scope: MemoryScope,
        id: &str,
    ) -> Result<Option<IndexHit>, IndexError> {
        if visible.is_empty() {
            return Ok(None);
        }
        let mut values: Vec<Value> = visible.iter().cloned().map(Value::Text).collect();
        let placeholders = vec!["?"; values.len()].join(",");
        values.push(id.to_owned().into());
        values.push(
            scope
                .source()
                .map_or(Value::Null, |s| Value::Text(s.into())),
        );
        let sql = format!(
            "SELECT id,agent_id,source,timestamp_ms,direction,sender,destination,tool_name,role,
                segment,offset,length,sha256,seq,
                (SELECT json_group_array(name) FROM (SELECT name FROM host_document_tools WHERE document_id=d.id ORDER BY name))
                FROM host_documents d
                WHERE agent_id IN ({placeholders}) AND id=? AND (? IS NULL OR source=?)"
        );
        values.push(values.last().unwrap().clone());
        Ok(self
            .conn
            .query_row(&sql, params_from_iter(values), |r| {
                let raw = RawCoordinate {
                    segment: r.get::<_, String>(9)?.into(),
                    offset: r.get(10)?,
                    length: r.get(11)?,
                    sha256: r.get(12)?,
                    seq: r.get(13)?,
                };
                Ok(IndexHit {
                    id: r.get(0)?,
                    metadata: IndexMetadata {
                        agent_id: r.get(1)?,
                        source: if r.get::<_, String>(2)? == "memory" {
                            MemoryScope::Memory
                        } else {
                            MemoryScope::Transcripts
                        },
                        timestamp_ms: r.get(3)?,
                        direction: r.get(4)?,
                        sender: r.get(5)?,
                        destination: r.get(6)?,
                        tool_name: r.get(7)?,
                        tool_names: decode_tool_names(r.get(14)?)?,
                        role: r.get(8)?,
                    },
                    window: raw.clone(),
                    raw,
                    lexical_score: 0.0,
                })
            })
            .optional()?)
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
            where_sql.push_str(" AND (d.tool_name IS NOT NULL OR EXISTS(SELECT 1 FROM host_document_tools t WHERE t.document_id=d.id))");
        }
        if !query.include_tools && !query.only_tools && query.tool_name.is_none() {
            where_sql.push_str(" AND d.tool_name IS NULL AND NOT EXISTS(SELECT 1 FROM host_document_tools t WHERE t.document_id=d.id)");
        }
        if let Some(tool) = &query.tool_name {
            where_sql.push_str(" AND (lower(d.tool_name)=lower(?) OR EXISTS(SELECT 1 FROM host_document_tools t WHERE t.document_id=d.id AND lower(t.name)=lower(?)))");
            values.push(tool.trim().to_owned().into());
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
            d.segment,d.offset,d.length,d.sha256,d.seq,w.offset,w.length,w.sha256,{score} AS score,
            (SELECT json_group_array(name) FROM (SELECT name FROM host_document_tools WHERE document_id=d.id ORDER BY name))
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
                    tool_names: decode_tool_names(row.get(18)?)?,
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

fn integrity_mismatch() -> io::Error {
    io::Error::other("index coordinate integrity mismatch")
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
    let mut file = RawSource::open(&raw.segment)?;
    let mut reader = file.range(raw.offset, raw.length);
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
            timestamp_ms: Some(time),
            direction: "in".into(),
            sender: "user".into(),
            destination: owner.into(),
            tool_name: None,
            tool_names: Vec::new(),
            role: Some("user".into()),
        }
    }
    /// One connection serves every sequential borrower; concurrent borrowers
    /// get their own; a replaced file or an open transaction is never reused.
    #[test]
    fn shared_index_reuses_connections_but_never_a_replaced_file() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("index.db");
        let opens = || OPENS.with(std::cell::Cell::get);
        let start = opens();
        drop(HostIndex::shared(&path).unwrap());
        for _ in 0..5 {
            drop(HostIndex::shared(&path).unwrap());
        }
        assert_eq!(opens() - start, 1);
        let first = HostIndex::shared(&path).unwrap();
        let second = HostIndex::shared(&path).unwrap();
        assert_eq!(opens() - start, 2);
        drop((first, second));
        // Delete the file under two idle connections: neither may serve it.
        let segment = RawSegment::open(&dir.path().join("events.raw")).unwrap();
        let raw = segment.append(b"replacedneedle").unwrap();
        HostIndex::shared(&path)
            .unwrap()
            .index("old", &metadata("a", MemoryScope::Memory, 1), &raw)
            .unwrap();
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        let start = opens();
        let mut fresh = HostIndex::shared(&path).unwrap();
        assert_eq!(opens() - start, 1);
        let q = HistorySearchQuery {
            query: Some("replacedneedle".into()),
            ..Default::default()
        };
        assert!(
            fresh
                .search(&["a".into()], MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
        fresh
            .index("new", &metadata("a", MemoryScope::Memory, 2), &raw)
            .unwrap();
        drop(fresh);
        let independent = HostIndex::open(&path).unwrap();
        assert_eq!(
            independent
                .search(&["a".into()], MemoryScope::All, &q)
                .unwrap()[0]
                .id,
            "new"
        );
        // A connection dropped inside a transaction is closed, not pooled.
        let start = opens();
        let borrowed = HostIndex::shared(&path).unwrap();
        borrowed.conn.execute_batch("BEGIN").unwrap();
        drop(borrowed);
        let next = HostIndex::shared(&path).unwrap();
        assert!(next.conn.is_autocommit());
        assert_eq!(opens() - start, 1);
    }
    #[test]
    fn required_time_cache_is_invalidated_without_touching_raw_history() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("index.db");
        let raw = dir.path().join("source.jsonl");
        std::fs::write(&raw, b"{\"content\":\"authoritative history\"}\n").unwrap();
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE host_documents (timestamp_ms INTEGER NOT NULL); INSERT INTO host_documents VALUES (123);").unwrap();
        drop(conn);
        let index = HostIndex::open(&path).unwrap();
        let required: i64 = index.conn.query_row("SELECT [notnull] FROM pragma_table_info('host_documents') WHERE name='timestamp_ms'", [], |r| r.get(0)).unwrap();
        assert_eq!(required, 0);
        let journal: String = index
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(journal, "wal");
        assert_eq!(
            std::fs::read(&raw).unwrap(),
            b"{\"content\":\"authoritative history\"}\n"
        );
    }

    /// Deleting a document cascades into `host_calls`; that lookup must use an
    /// index, also on an index built before the index existed, or every
    /// deleted document scans the whole table.
    #[test]
    fn deleting_a_document_finds_its_calls_by_index_on_old_and_new_files() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("index.db");
        let plan = |conn: &Connection| -> String {
            conn.query_row(
                "EXPLAIN QUERY PLAN SELECT 1 FROM host_calls WHERE document_id = 'x'",
                [],
                |r| r.get::<_, String>(3),
            )
            .unwrap()
        };
        let fresh = HostIndex::open(&path).unwrap();
        assert!(plan(&fresh.conn).contains("host_calls_document"));
        fresh
            .conn
            .execute_batch("DROP INDEX host_calls_document")
            .unwrap();
        assert!(plan(&fresh.conn).starts_with("SCAN host_calls"));
        drop(fresh);
        let upgraded = HostIndex::open(&path).unwrap();
        assert!(plan(&upgraded.conn).contains("host_calls_document"));
        // The cascade still removes a deleted document's calls.
        let segment = RawSegment::open(&dir.path().join("events.raw")).unwrap();
        let raw = segment.append(b"callneedle").unwrap();
        let mut upgraded = upgraded;
        upgraded
            .index("doc", &metadata("a", MemoryScope::Transcripts, 1), &raw)
            .unwrap();
        upgraded
            .save_call("a", &raw.segment, "call-1", "shell", "doc")
            .unwrap();
        upgraded
            .conn
            .execute("DELETE FROM host_documents WHERE id = 'doc'", [])
            .unwrap();
        let calls: i64 = upgraded
            .conn
            .query_row("SELECT COUNT(*) FROM host_calls", [], |r| r.get(0))
            .unwrap();
        assert_eq!(calls, 0);
    }

    #[test]
    fn malformed_index_is_rebuilt_from_source_instead_of_failing() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("index.db");
        std::fs::write(&path, b"garbage, not a sqlite database").unwrap();
        // Must succeed via the rebuild path, not error `malformed`.
        let index = HostIndex::open(&path).unwrap();
        // The rebuilt projection is queryable (empty).
        let count: i64 = index
            .conn
            .query_row("SELECT COUNT(*) FROM host_documents", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn serialized_json_escapes_are_searchable_with_exact_raw_coordinates() {
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
