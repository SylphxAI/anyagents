//! Native history that a harness keeps in its own SQLite store: Spiron's
//! `sessions.sqlite3` and OpenCode's `opencode.db`. Each row is one raw event.
//! Its coordinate names the row as `<database>/<table>/<rowid>`, and its bytes
//! are the row's event text exactly as stored. A database is a regular file, so
//! no filesystem path lies below it and the row namespace never collides with a
//! real file. Stores are opened read-only; AnyAgents never writes them.
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::{
    io,
    path::{Path, PathBuf},
};

/// The stores whose layout is known. Anything else is not read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreKind {
    /// `sessions.sqlite3`: `session_entries(session_id, idx, event)`. Entries
    /// are a journal: a row is written once and never updated.
    SpironSessions,
    /// `opencode.db`: `session_message(id, session_id, type, seq, time_created,
    /// time_updated, data)`. A message row is updated while it streams.
    OpenCodeMessages,
}

impl StoreKind {
    pub(crate) fn file_name(self) -> &'static str {
        match self {
            Self::SpironSessions => "sessions.sqlite3",
            Self::OpenCodeMessages => "opencode.db",
        }
    }
    fn table(self) -> &'static str {
        match self {
            Self::SpironSessions => "session_entries",
            Self::OpenCodeMessages => "session_message",
        }
    }
    /// Event text, role and time of one row. The role and time come from
    /// columns beside the text where the store keeps them there.
    fn select(self) -> &'static str {
        match self {
            Self::SpironSessions => {
                "SELECT rowid, CAST(event AS BLOB), NULL, NULL, 0 FROM session_entries"
            }
            Self::OpenCodeMessages => {
                "SELECT rowid, CAST(data AS BLOB), type, time_created, COALESCE(time_updated, 0) FROM session_message"
            }
        }
    }
    /// The column a store bumps when it rewrites a row in place, if any.
    fn changed_column(self) -> Option<&'static str> {
        match self {
            Self::SpironSessions => None,
            Self::OpenCodeMessages => Some("time_updated"),
        }
    }
    fn of_table(table: &str) -> Option<Self> {
        [Self::SpironSessions, Self::OpenCodeMessages]
            .into_iter()
            .find(|kind| kind.table() == table)
    }
}

/// One row of one store, named by its coordinate segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowRef {
    pub db: PathBuf,
    pub kind: StoreKind,
    pub rowid: i64,
}

impl RowRef {
    pub(crate) fn new(db: &Path, kind: StoreKind, rowid: i64) -> Self {
        Self {
            db: db.to_owned(),
            kind,
            rowid,
        }
    }
    /// `<database>/<table>/<rowid>` names a row only when the database's file
    /// name and the table belong to one known store kind.
    pub(crate) fn parse(segment: &Path) -> Option<Self> {
        let rowid = segment.file_name()?.to_str()?;
        if rowid.is_empty() || !rowid.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let table_path = segment.parent()?;
        let kind = StoreKind::of_table(table_path.file_name()?.to_str()?)?;
        let db = table_path.parent()?;
        if db.file_name()?.to_str()? != kind.file_name() {
            return None;
        }
        Some(Self {
            db: db.to_owned(),
            kind,
            rowid: rowid.parse().ok()?,
        })
    }
    pub(crate) fn segment(&self) -> PathBuf {
        self.db.join(self.kind.table()).join(self.rowid.to_string())
    }
    /// The row's event text exactly as stored.
    pub(crate) fn read(&self) -> io::Result<Vec<u8>> {
        let store = Store::open(&self.db, self.kind)?
            .ok_or_else(|| io::Error::other("native store layout changed"))?;
        store.row(self.rowid)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "native store row no longer exists")
        })
    }
}

/// The published source a segment belongs to: the store for a row, the file
/// itself otherwise.
pub(crate) fn source_of(segment: &Path) -> PathBuf {
    RowRef::parse(segment).map_or_else(|| segment.to_owned(), |row| row.db)
}

/// One row read for indexing.
pub(crate) struct StoreRow {
    pub rowid: i64,
    pub bytes: Vec<u8>,
    pub role: Option<String>,
    pub timestamp_ms: Option<i64>,
    /// The store's change mark for the row (0 where rows never change).
    pub changed: i64,
}

/// A read-only connection to one store whose layout was checked.
pub(crate) struct Store {
    conn: Connection,
    kind: StoreKind,
}

impl Store {
    /// `None` when the file is not a store of this kind (another schema, an
    /// older harness version, not SQLite at all): such a file is not history
    /// this reader understands, and it is not read. The path is checked the
    /// way every raw source is: a single-link regular file reached without a
    /// symlink.
    pub(crate) fn open(db: &Path, kind: StoreKind) -> io::Result<Option<Self>> {
        crate::raw_segment::open_readonly(db)?;
        // A reader of a WAL database creates the `-wal` and `-shm` files when
        // they are absent, owned by the reader. In a harness's directory that
        // could leave its writer unable to open its own store, so a store
        // without both files is read as immutable: no lock and no new file.
        // Without them no writer is attached, and the database holds every row.
        let live = wal_path(db).exists() && sibling(db, "-shm").exists();
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
        let uri = format!(
            "file:{}{}",
            uri_path(db)?,
            if live {
                "?mode=ro"
            } else {
                "?mode=ro&immutable=1"
            }
        );
        let conn = Connection::open_with_flags(uri, flags).map_err(io::Error::other)?;
        // A writer holding the WAL lock during its own recovery is the only
        // wait a reader can see; it is brief and bounded here.
        conn.busy_timeout(std::time::Duration::from_secs(2))
            .map_err(io::Error::other)?;
        let columns: Vec<String> = match conn
            .prepare(&format!("PRAGMA table_info({})", kind.table()))
            .and_then(|mut s| {
                s.query_map([], |r| r.get::<_, String>(1))?
                    .collect::<Result<Vec<_>, _>>()
            }) {
            Ok(columns) => columns,
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::NotADatabase =>
            {
                return Ok(None);
            }
            Err(error) => return Err(io::Error::other(error)),
        };
        let wanted: &[&str] = match kind {
            StoreKind::SpironSessions => &["session_id", "idx", "event"],
            StoreKind::OpenCodeMessages => &["type", "time_created", "time_updated", "data"],
        };
        if !wanted.iter().all(|w| columns.iter().any(|c| c == w)) {
            return Ok(None);
        }
        Ok(Some(Self { conn, kind }))
    }

    pub(crate) fn row(&self, rowid: i64) -> io::Result<Option<Vec<u8>>> {
        self.conn
            .query_row(
                &format!("{} WHERE rowid=?1", self.kind.select()),
                [rowid],
                |r| r.get::<_, Vec<u8>>(1),
            )
            .optional()
            .map_err(io::Error::other)
    }

    /// The highest rowid and the latest in-place change, read in one snapshot
    /// with the rows a refresh then reads.
    pub(crate) fn frontier(&self) -> io::Result<(i64, i64)> {
        let changed = self.kind.changed_column().unwrap_or("0");
        self.conn
            .query_row(
                &format!(
                    "SELECT COALESCE(MAX(rowid),0), COALESCE(MAX({changed}),0) FROM {}",
                    self.kind.table()
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(io::Error::other)
    }

    fn read_rows(
        &self,
        sql: &str,
        bind: &[&dyn rusqlite::ToSql],
        limit_bytes: u64,
    ) -> io::Result<Vec<StoreRow>> {
        let mut statement = self.conn.prepare(sql).map_err(io::Error::other)?;
        let mut rows = statement.query(bind).map_err(io::Error::other)?;
        let mut out = Vec::new();
        let mut bytes_read = 0u64;
        while let Some(row) = rows.next().map_err(io::Error::other)? {
            let bytes: Vec<u8> = row.get(1).map_err(io::Error::other)?;
            bytes_read += bytes.len() as u64;
            out.push(StoreRow {
                rowid: row.get(0).map_err(io::Error::other)?,
                bytes,
                role: row.get(2).map_err(io::Error::other)?,
                timestamp_ms: row.get(3).map_err(io::Error::other)?,
                changed: row.get(4).map_err(io::Error::other)?,
            });
            if bytes_read >= limit_bytes {
                break;
            }
        }
        Ok(out)
    }

    /// Rows after `after_rowid` up to `upto_rowid`, in rowid order: at most
    /// `limit` rows, stopping after the row that crosses `limit_bytes`.
    pub(crate) fn rows_after(
        &self,
        after_rowid: i64,
        upto_rowid: i64,
        limit: usize,
        limit_bytes: u64,
    ) -> io::Result<Vec<StoreRow>> {
        let sql = format!(
            "{} WHERE rowid>?1 AND rowid<=?2 ORDER BY rowid LIMIT ?3",
            self.kind.select()
        );
        self.read_rows(
            &sql,
            &[&after_rowid, &upto_rowid, &(limit as i64)],
            limit_bytes,
        )
    }

    /// Rows up to `upto_rowid` the store rewrote after `changed_since`, in
    /// change order. A store that never rewrites a row has none.
    pub(crate) fn rows_changed(
        &self,
        upto_rowid: i64,
        changed_since: i64,
        limit: usize,
        limit_bytes: u64,
    ) -> io::Result<Vec<StoreRow>> {
        let Some(changed) = self.kind.changed_column() else {
            return Ok(Vec::new());
        };
        let sql = format!(
            "{} WHERE rowid<=?1 AND {changed}>?2 ORDER BY {changed}, rowid LIMIT ?3",
            self.kind.select()
        );
        self.read_rows(
            &sql,
            &[&upto_rowid, &changed_since, &(limit as i64)],
            limit_bytes,
        )
    }

    /// Every rowid up to `upto_rowid`: what a completed rebuild keeps.
    pub(crate) fn rowids(&self, upto_rowid: i64) -> io::Result<Vec<i64>> {
        self.conn
            .prepare(&format!(
                "SELECT rowid FROM {} WHERE rowid<=?1",
                self.kind.table()
            ))
            .and_then(|mut s| {
                s.query_map([upto_rowid], |r| r.get(0))?
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(io::Error::other)
    }

    /// Rows added after `after_rowid` or rewritten after `changed_since`, up to
    /// the frontier, in rowid order. `None` bounds read every row.
    #[cfg(test)]
    pub(crate) fn rows(
        &self,
        after: Option<(i64, i64)>,
        upto_rowid: i64,
        mut each: impl FnMut(StoreRow) -> io::Result<()>,
    ) -> io::Result<()> {
        let (after_rowid, changed_since) = after.unwrap_or((0, i64::MIN));
        let changed = self.kind.changed_column().unwrap_or("0");
        let sql = format!(
            "{} WHERE rowid<=?1 AND (rowid>?2 OR {changed}>?3) ORDER BY rowid",
            self.kind.select()
        );
        for row in self.read_rows(&sql, &[&upto_rowid, &after_rowid, &changed_since], u64::MAX)? {
            each(row)?;
        }
        Ok(())
    }

    /// Begin a read transaction so the frontier and the rows come from one
    /// snapshot of the store.
    pub(crate) fn snapshot(&self) -> io::Result<()> {
        self.conn
            .execute_batch("BEGIN DEFERRED; SELECT 1 FROM sqlite_master LIMIT 1;")
            .map_err(io::Error::other)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if !self.conn.is_autocommit() {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

/// The write-ahead log beside a store: new rows land there before a
/// checkpoint touches the database file, so both stamps describe the store.
pub(crate) fn wal_path(db: &Path) -> PathBuf {
    sibling(db, "-wal")
}
fn sibling(db: &Path, suffix: &str) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}
/// An absolute path as a `file:` URI path: every byte outside the unreserved
/// set and `/` is percent-encoded, so `?`, `#` and `%` stay part of the name.
fn uri_path(db: &Path) -> io::Result<String> {
    use std::os::unix::ffi::OsStrExt;
    if !db.is_absolute() {
        return Err(io::Error::other("native store path must be absolute"));
    }
    let mut out = String::new();
    for &b in db.as_os_str().as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn spiron_store(path: &Path, events: &[&str]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS session_entries(session_id TEXT NOT NULL, idx INTEGER NOT NULL,
                event TEXT NOT NULL CHECK(json_valid(event)), PRIMARY KEY(session_id, idx));",
        )
        .unwrap();
        for event in events {
            conn.execute(
                "INSERT INTO session_entries VALUES('s1',(SELECT COALESCE(MAX(idx),0)+1 FROM session_entries),?1)",
                [event],
            )
            .unwrap();
        }
    }

    pub(crate) fn sibling_for_test(db: &Path, suffix: &str) -> PathBuf {
        sibling(db, suffix)
    }

    #[test]
    fn row_names_parse_only_for_known_store_tables() {
        let db = Path::new("/data/agents/a/config/sessions.sqlite3");
        let row = RowRef::new(db, StoreKind::SpironSessions, 42);
        assert_eq!(
            row.segment(),
            Path::new("/data/agents/a/config/sessions.sqlite3/session_entries/42")
        );
        assert_eq!(RowRef::parse(&row.segment()), Some(row));
        for not_a_row in [
            "/data/agents/a/config/sessions.sqlite3",
            "/data/agents/a/config/sessions.sqlite3/session_entries/4x",
            "/data/agents/a/config/sessions.sqlite3/session_message/4",
            "/data/agents/a/config/other.sqlite3/session_entries/4",
            "/data/agents/a/transcripts/session_entries/4",
        ] {
            assert_eq!(RowRef::parse(Path::new(not_a_row)), None, "{not_a_row}");
        }
        let opencode = RowRef::new(
            Path::new("/d/config/data-home/opencode/opencode.db"),
            StoreKind::OpenCodeMessages,
            7,
        );
        assert_eq!(RowRef::parse(&opencode.segment()), Some(opencode));
    }

    #[test]
    fn rows_are_read_exactly_read_only_and_unknown_layouts_are_not_stores() {
        let dir = crate::test_tempdir();
        let db = dir.path().join("sessions.sqlite3");
        let event = r#"{"type":"text","data":{"role":"user","content":"粵語 hello"}}"#;
        spiron_store(&db, &[event]);
        let store = Store::open(&db, StoreKind::SpironSessions)
            .unwrap()
            .unwrap();
        assert_eq!(store.frontier().unwrap(), (1, 0));
        let mut seen = Vec::new();
        store
            .rows(None, 1, |row| {
                seen.push(row.bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, vec![event.as_bytes().to_vec()]);
        assert_eq!(
            RowRef::new(&db, StoreKind::SpironSessions, 1)
                .read()
                .unwrap(),
            seen[0]
        );
        assert!(
            RowRef::new(&db, StoreKind::SpironSessions, 2)
                .read()
                .is_err_and(|e| e.kind() == io::ErrorKind::NotFound)
        );
        // Opened read-only: a write through the reader is refused.
        assert!(
            store
                .conn
                .execute("DELETE FROM session_entries", [])
                .is_err()
        );

        // Same file name, another layout: not this store.
        let other = dir.path().join("other").join("sessions.sqlite3");
        std::fs::create_dir_all(other.parent().unwrap()).unwrap();
        Connection::open(&other)
            .unwrap()
            .execute_batch("CREATE TABLE session_entries(x TEXT);")
            .unwrap();
        assert!(
            Store::open(&other, StoreKind::SpironSessions)
                .unwrap()
                .is_none()
        );
        let junk = dir.path().join("junk").join("opencode.db");
        std::fs::create_dir_all(junk.parent().unwrap()).unwrap();
        std::fs::write(
            &junk,
            b"not sqlite at all, but long enough to have a header....",
        )
        .unwrap();
        assert!(
            Store::open(&junk, StoreKind::OpenCodeMessages)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reading_a_closed_store_creates_no_file_beside_it() {
        let dir = crate::test_tempdir();
        let db = dir.path().join("odd #?% name").join("sessions.sqlite3");
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        spiron_store(&db, &[r#"{"type":"text"}"#]);
        // A clean close removes the writer's `-wal` and `-shm`.
        for suffix in ["-wal", "-shm"] {
            assert!(!sibling(&db, suffix).exists(), "{suffix}");
        }
        let store = Store::open(&db, StoreKind::SpironSessions)
            .unwrap()
            .unwrap();
        assert_eq!(store.frontier().unwrap(), (1, 0));
        assert_eq!(store.row(1).unwrap().unwrap(), br#"{"type":"text"}"#);
        drop(store);
        for suffix in ["-wal", "-shm", "-journal"] {
            assert!(!sibling(&db, suffix).exists(), "{suffix}");
        }

        // A store with a live writer is read through its WAL, including rows
        // not yet checkpointed into the database file.
        let writer = Connection::open(&db).unwrap();
        writer
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        writer
            .execute(
                "INSERT INTO session_entries VALUES('s1', 2, '{\"type\":\"done\"}')",
                [],
            )
            .unwrap();
        assert!(wal_path(&db).exists());
        let store = Store::open(&db, StoreKind::SpironSessions)
            .unwrap()
            .unwrap();
        assert_eq!(store.frontier().unwrap(), (2, 0));
    }

    #[cfg(unix)]
    #[test]
    fn a_linked_store_is_refused() {
        let dir = crate::test_tempdir();
        let db = dir.path().join("real.sqlite3");
        spiron_store(&db, &[r#"{"type":"text"}"#]);
        let link = dir.path().join("sessions.sqlite3");
        std::os::unix::fs::symlink(&db, &link).unwrap();
        assert!(Store::open(&link, StoreKind::SpironSessions).is_err());
    }
}
