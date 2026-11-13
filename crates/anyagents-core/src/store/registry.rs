//! One read-write connection and one read pool per database file per process.
//!
//! [`Store::open`](super::Store::open) hands out cheap handles onto this shared
//! state. Migrations and legacy repairs run when a process first opens a path,
//! writers in one process queue on one mutex instead of contending through
//! SQLite's file lock, and a handle holds no descriptors of its own. Entries
//! are held weakly: when the last handle on a path drops, its connections
//! close and the next open starts from the file again.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};

use rusqlite::{Connection, TransactionBehavior};

use super::{
    BUSY_TIMEOUT, MAX_IDLE_READERS, ReadConn, WAL_SIZE_LIMIT_BYTES, WRITE_RETRY_BUDGET, Wake,
    data_dir_for, ensure_lexical_fts_backfilled, is_transient_lock_error,
    is_transient_sqlite_error, legacy_last_error, open_reader, preserve_legacy_scopes, runtime,
    tune_connection,
};
use crate::error::Error;
use crate::migrate;

/// What every handle on one database file shares.
pub(super) struct Shared {
    /// The file the read pool opens. Empty for SQLite's private temporary
    /// database, which no second connection can reach.
    path: PathBuf,
    writer: Mutex<Connection>,
    readers: Arc<Mutex<Vec<Connection>>>,
    /// In-process wake subscribers shared across every handle on this file.
    wakes: Mutex<Vec<Sender<Wake>>>,
    /// Total time a bounded write may keep retrying transient lock errors.
    pub(super) write_retry_budget_ms: AtomicU64,
    /// The writer connection's configured busy timeout.
    pub(super) busy_timeout_ms: AtomicU64,
    /// A legacy repair was deferred because another process held the write
    /// lock; the next open on this path retries it.
    repairs_pending: AtomicBool,
}

/// Borrow a pooled reader for `path` only if this process already holds the
/// store open; never opens a writer, runs migrations, or creates a file.
pub(super) fn reader_if_open(path: &Path) -> Option<ReadConn<'static>> {
    if path.as_os_str().is_empty() {
        return None;
    }
    let key = canonical(path).ok()?;
    let slot = {
        let open = lock(&OPEN);
        open.get(&key).cloned()
    }?;
    let shared = { lock(&slot).upgrade() }?;
    shared.reader()
}

/// The shared state for `path`, opened (and migrated) on first use in this
/// process and reused while any handle on it is alive.
pub(super) fn shared(path: &Path) -> Result<Arc<Shared>, Error> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    if path.as_os_str().is_empty() {
        return Ok(Arc::new(Shared::open(path)?));
    }
    let key = canonical(path)?;
    let slot = {
        let mut open = lock(&OPEN);
        // Forget paths whose last handle dropped. A slot another opener holds
        // (strong count above one) is never inspected, so its lock is free.
        open.retain(|_, slot| Arc::strong_count(slot) > 1 || lock(slot).strong_count() > 0);
        Arc::clone(open.entry(key.clone()).or_default())
    };
    // Openers of one path wait here for its first open; other paths proceed.
    let mut current = lock(&slot);
    if let Some(shared) = current.upgrade() {
        drop(current);
        shared.retry_deferred_repairs()?;
        return Ok(shared);
    }
    let shared = Arc::new(Shared::open(&key)?);
    *current = Arc::downgrade(&shared);
    Ok(shared)
}

/// One path's entry: its shared state while any handle on it is alive.
type Slot = Mutex<Weak<Shared>>;

/// Open paths, keyed by canonical file path.
static OPEN: LazyLock<Mutex<HashMap<PathBuf, Arc<Slot>>>> = LazyLock::new(Mutex::default);

#[cfg(test)]
thread_local! {
    /// Migration and repair passes run on this thread, for tests.
    pub(super) static OPEN_PASSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn note_open_pass() {
    #[cfg(test)]
    OPEN_PASSES.with(|passes| passes.set(passes.get() + 1));
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One key per file however it is spelled: relative, through `.` or `..`,
/// or through a symlinked directory. A file that does not exist yet is keyed
/// by its canonical directory, which is what it resolves to once created.
fn canonical(path: &Path) -> Result<PathBuf, Error> {
    if let Ok(found) = std::fs::canonicalize(path) {
        return Ok(found);
    }
    let Some(name) = path.file_name() else {
        return Err(Error::Message(format!(
            "not a database file path: {}",
            path.display()
        )));
    };
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    Ok(std::fs::canonicalize(parent)?.join(name))
}

impl Shared {
    fn open(path: &Path) -> Result<Self, Error> {
        note_open_pass();
        let mut conn = open_writer(path)?;
        let schema_version = migrate::current_version(&conn)?;
        migrate::migrate(&mut conn)?;
        if migrate::current_version(&conn)? < 1 {
            return Err(Error::Message("migrations did not apply".into()));
        }
        let mut complete = repair_legacy_schema(&mut conn)?;
        // Current schemas maintain FTS transactionally on every history write.
        // Only legacy upgrades need the full derivative-index validation.
        if schema_version < 23 {
            ensure_lexical_fts_backfilled(&mut conn)?;
        }
        complete &= backfill_runtime(&mut conn, &data_dir_for(path))?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: Mutex::new(conn),
            readers: Arc::new(Mutex::new(Vec::new())),
            wakes: Mutex::new(Vec::new()),
            write_retry_budget_ms: AtomicU64::new(WRITE_RETRY_BUDGET.as_millis() as u64),
            busy_timeout_ms: AtomicU64::new(BUSY_TIMEOUT.as_millis() as u64),
            repairs_pending: AtomicBool::new(!complete),
        })
    }

    /// Rerun the legacy repairs a busy peer deferred. A no-op, without the
    /// writer lock, once they completed.
    fn retry_deferred_repairs(&self) -> Result<(), Error> {
        if !self.repairs_pending.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut conn = self.writer();
        if !self.repairs_pending.load(Ordering::Acquire) {
            return Ok(());
        }
        note_open_pass();
        let mut complete = repair_legacy_schema(&mut conn)?;
        complete &= backfill_runtime(&mut conn, &data_dir_for(&self.path))?;
        self.repairs_pending.store(!complete, Ordering::Release);
        Ok(())
    }

    pub(super) fn wakes(&self) -> &Mutex<Vec<Sender<Wake>>> {
        &self.wakes
    }

    pub(super) fn writer(&self) -> MutexGuard<'_, Connection> {
        lock(&self.writer)
    }

    /// A pooled read-only connection, or `None` when none can be opened.
    pub(super) fn reader(&self) -> Option<ReadConn<'static>> {
        if self.path.as_os_str().is_empty() {
            return None;
        }
        let idle = lock(&self.readers).pop();
        let conn = idle.or_else(|| open_reader(&self.path))?;
        Some(ReadConn::Pooled {
            conn: Some(conn),
            pool: Arc::clone(&self.readers),
        })
    }

    /// Return a reader to the pool. One left inside a transaction is closed:
    /// its stale snapshot would hide later commits from the next borrower.
    pub(super) fn recycle(pool: &Mutex<Vec<Connection>>, conn: Connection) {
        if !conn.is_autocommit() {
            return;
        }
        let mut idle = lock(pool);
        if idle.len() < MAX_IDLE_READERS {
            idle.push(conn);
        }
    }

    #[cfg(test)]
    pub(super) fn mark_repairs_pending(&self) {
        self.repairs_pending.store(true, Ordering::Release);
    }
}

/// The process's one read-write connection to `path`, in WAL mode.
fn open_writer(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open(path)?;
    // A 30s wait: a peer process's write queues here instead of failing with
    // `database is locked`. BEGIN IMMEDIATE (not the DEFERRED default) is what
    // makes the wait apply to read-then-write paths.
    conn.busy_timeout(BUSY_TIMEOUT)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    // Without a limit the WAL file keeps its high-water size forever after
    // one burst; with it, each checkpoint reset truncates the file back.
    conn.pragma_update(None, "journal_size_limit", WAL_SIZE_LIMIT_BYTES)?;
    let mut mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        mode = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    }
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(Error::Message(format!(
            "sqlite journal mode is {mode}, not wal"
        )));
    }
    tune_connection(&conn);
    Ok(conn)
}

/// `Some(value)`, or `None` with `complete` cleared when a peer process holds
/// the write lock, so the step is retried by the next open.
fn deferrable<T>(complete: &mut bool, result: rusqlite::Result<T>) -> Result<Option<T>, Error> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if is_transient_sqlite_error(&error) => {
            *complete = false;
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

/// Bring rows and schema written by older binaries up to date. Each step reads
/// first, so a current database takes no write lock. Returns `false` when a
/// busy peer deferred any step.
fn repair_legacy_schema(conn: &mut Connection) -> Result<bool, Error> {
    let mut complete = true;
    match preserve_legacy_scopes(conn) {
        Ok(()) => {}
        Err(error) if is_transient_lock_error(&error) => complete = false,
        Err(error) => return Err(error),
    }
    // The UPDATEs are idempotent and change only the legacy selector.
    for (probe, repair) in [
        (
            "SELECT EXISTS(SELECT 1 FROM agents WHERE harness IN ('sylphx', 'sylphx-agent', 'sylphx_agent', 'sylphx/default'))",
            "UPDATE agents SET harness = 'spiron' WHERE harness IN ('sylphx', 'sylphx-agent', 'sylphx_agent', 'sylphx/default')",
        ),
        (
            "SELECT EXISTS(SELECT 1 FROM agents WHERE state NOT IN ('created', 'running', 'idle', 'paused', 'archived'))",
            "UPDATE agents SET state = 'archived' WHERE state NOT IN ('created', 'running', 'idle', 'paused', 'archived')",
        ),
    ] {
        let needed = deferrable(&mut complete, conn.query_row(probe, [], |row| row.get(0)))?;
        if needed == Some(true) {
            deferrable(&mut complete, conn.execute(repair, []))?;
        }
    }
    let has_aux_schema = deferrable(
        &mut complete,
        conn.query_row(
            "SELECT (SELECT count(*) FROM sqlite_master WHERE name IN ('message_versions', 'agent_topology_state', 'idx_rag_chunks_agent_id_time')) = 3",
            [],
            |row| row.get(0),
        ),
    )?;
    if has_aux_schema == Some(false) {
        deferrable(
            &mut complete,
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS message_versions (
                    agent_id TEXT NOT NULL,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    body TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (agent_id, chat_id, message_id)
                );
                CREATE TABLE IF NOT EXISTS agent_topology_state (
                    agent_id TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_id_time
                    ON rag_chunks(agent_id, timestamp_ms DESC, chunk_id);",
            ),
        )?;
    }
    // UNIQUE(agent_id, seq) already supports reverse mailbox sequence scans.
    // Probe first: even idempotent DDL needs the writer lock.
    for (name, sql) in [
        (
            "idx_schedules_active_agent",
            "CREATE INDEX IF NOT EXISTS idx_schedules_active_agent ON agent_schedules(agent_id, created_at_ms, id) WHERE state = 'active'",
        ),
        (
            "idx_messages_agent_time",
            "CREATE INDEX IF NOT EXISTS idx_messages_agent_time ON messages(agent_id, received_at_ms DESC, seq DESC)",
        ),
        (
            "idx_agents_state",
            "CREATE INDEX IF NOT EXISTS idx_agents_state ON agents(state)",
        ),
        (
            "idx_agents_live_name",
            "CREATE INDEX IF NOT EXISTS idx_agents_live_name ON agents(name) WHERE state != 'archived'",
        ),
        (
            "idx_outbound_agent_time",
            "CREATE INDEX IF NOT EXISTS idx_outbound_agent_time ON outbound_messages(agent_id, sent_at_ms DESC, id DESC)",
        ),
    ] {
        let exists = deferrable(
            &mut complete,
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1)",
                [name],
                |row| row.get(0),
            ),
        )?;
        if exists == Some(false) {
            deferrable(&mut complete, conn.execute_batch(sql))?;
        }
    }
    // Older databases may already have this column from an earlier runtime.
    // Presence, not an arbitrary failed ALTER, is the idempotency condition.
    let has_public = deferrable(
        &mut complete,
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('groups') WHERE name = 'public')",
            [],
            |row| row.get(0),
        ),
    )?;
    if has_public == Some(false) {
        deferrable(
            &mut complete,
            conn.execute(
                "ALTER TABLE groups ADD COLUMN public INTEGER NOT NULL DEFAULT 0",
                [],
            ),
        )?;
    }
    Ok(complete)
}

/// Give every agent without one its runtime row and turns, derived from
/// legacy state. A read decides first, so a current store takes no writer
/// lock. Returns `false` when a busy peer deferred the backfill.
fn backfill_runtime(conn: &mut Connection, data_dir: &Path) -> Result<bool, Error> {
    let mut complete = true;
    let missing = deferrable(
        &mut complete,
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM agents a
                 WHERE NOT EXISTS (SELECT 1 FROM agent_runtime r WHERE r.agent_id = a.id))",
            [],
            |row| row.get(0),
        ),
    )?;
    if missing != Some(true) {
        return Ok(complete);
    }
    let backfilled = (|| -> Result<(), Error> {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        runtime::backfill_tx(&tx, &|id| legacy_last_error(data_dir, id))?;
        tx.commit()?;
        Ok(())
    })();
    match backfilled {
        Ok(()) => Ok(true),
        Err(error) if is_transient_lock_error(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;
    use std::sync::Barrier;
    use std::time::Duration;

    use super::*;
    use crate::store::{Incoming, NewAgent, Store};

    fn passes() -> usize {
        OPEN_PASSES.with(std::cell::Cell::get)
    }

    /// Descriptors this process holds on `file` opened for writing. SQLite
    /// opens a read-only connection's file `O_RDONLY`, so this counts
    /// read-write connections. `None` where the platform has no `/proc/self/fd`.
    fn read_write_descriptors(file: &Path) -> Option<usize> {
        // Compare inodes, not link text: /proc may render a live file's link
        // differently, and a replaced file must not count.
        let file = std::fs::metadata(file).unwrap();
        let fds = std::fs::read_dir("/proc/self/fd").ok()?;
        let count = fds
            .filter_map(Result::ok)
            .filter(|entry| {
                std::fs::metadata(entry.path())
                    .is_ok_and(|open| open.dev() == file.dev() && open.ino() == file.ino())
            })
            .filter(|entry| {
                let info =
                    std::fs::read_to_string(Path::new("/proc/self/fdinfo").join(entry.file_name()))
                        .unwrap_or_default();
                info.lines()
                    .filter_map(|line| line.strip_prefix("flags:"))
                    .filter_map(|flags| i64::from_str_radix(flags.trim(), 8).ok())
                    .any(|flags| flags & i64::from(libc::O_ACCMODE) == i64::from(libc::O_RDWR))
            })
            .count();
        Some(count)
    }

    /// Every descriptor under `dir` with its open flags, for failure messages.
    fn descriptors_under(dir: &Path) -> Vec<String> {
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let target = std::fs::read_link(entry.path()).ok()?;
                target.starts_with(dir).then(|| {
                    let info = std::fs::read_to_string(
                        Path::new("/proc/self/fdinfo").join(entry.file_name()),
                    )
                    .unwrap_or_default();
                    let flags = info.lines().find(|l| l.starts_with("flags:")).unwrap_or("");
                    let ino = std::fs::metadata(entry.path())
                        .map(|m| m.ino())
                        .unwrap_or(0);
                    format!("{} ino {ino} {flags}", target.display())
                })
            })
            .collect()
    }

    #[test]
    fn handles_on_one_path_share_one_writer_and_migrate_once() {
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let before = passes();
        let first = Store::open(&db).unwrap();
        assert_eq!(passes(), before + 1);
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(dir.path(), &link).unwrap();
        let mut handles = vec![
            Store::open(dir.path().join(".").join("state.db")).unwrap(),
            Store::open(link.join("state.db")).unwrap(),
        ];
        handles.extend((0..8).map(|_| Store::open(&db).unwrap()));
        // No later open migrated, repaired or probed the schema again.
        assert_eq!(passes(), before + 1);
        for (index, handle) in handles.iter().enumerate() {
            assert!(Arc::ptr_eq(&first.shared, &handle.shared));
            // Every handle writes and reads through the shared connections.
            let agent = handle
                .create_agent(NewAgent::new(format!("handle-{index}"), "codex"))
                .unwrap();
            handle.receive(&agent.id, Incoming::new("hello")).unwrap();
            assert_eq!(
                first.next_undelivered(&agent.id).unwrap().unwrap().text,
                "hello"
            );
        }
        assert_eq!(first.list_agents().unwrap().len(), handles.len());
        if let Some(count) = read_write_descriptors(&db) {
            assert_eq!(count, 1);
        }
        // A handle keeps its own spelling of the path and its own data dir.
        assert_eq!(handles[1].data_dir(), link.as_path());
    }

    #[test]
    fn concurrent_first_opens_of_one_path_migrate_once() {
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let start = Barrier::new(16);
        let (stores, passes): (Vec<Store>, Vec<usize>) = std::thread::scope(|scope| {
            let openers: Vec<_> = (0..16)
                .map(|_| {
                    scope.spawn(|| {
                        let before = passes();
                        start.wait();
                        let store = Store::open(&db).unwrap();
                        (store, passes() - before)
                    })
                })
                .collect();
            openers.into_iter().map(|t| t.join().unwrap()).unzip()
        });
        assert_eq!(passes.iter().sum::<usize>(), 1, "{passes:?}");
        assert!(
            stores
                .iter()
                .all(|s| Arc::ptr_eq(&s.shared, &stores[0].shared))
        );
        if let Some(count) = read_write_descriptors(&db) {
            assert_eq!(
                count,
                1,
                "file ino {}: {:?}",
                std::fs::metadata(&db).unwrap().ino(),
                descriptors_under(dir.path())
            );
        }
    }

    #[test]
    fn a_path_reopens_from_the_file_after_its_last_handle_drops() {
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let first = Store::open(&db).unwrap();
        let second = Store::open(&db).unwrap();
        let agent = first.create_agent(NewAgent::new("kept", "codex")).unwrap();
        let weak = Arc::downgrade(&first.shared);
        drop(first);
        assert!(weak.upgrade().is_some(), "a live handle keeps the writer");
        drop(second);
        assert!(weak.upgrade().is_none(), "the last handle closes it");
        if let Some(count) = read_write_descriptors(&db) {
            assert_eq!(count, 0);
        }
        // Opening any path forgets entries whose handles are all gone.
        let other = crate::test_tempdir();
        let _other = Store::open(other.path().join("state.db")).unwrap();
        assert!(!lock(&OPEN).contains_key(&canonical(&db).unwrap()));

        let before = passes();
        let reopened = Store::open(&db).unwrap();
        assert_eq!(passes(), before + 1, "a fresh open runs its checks again");
        assert_eq!(reopened.agent(&agent.id).unwrap().name, "kept");
    }

    #[test]
    fn a_deferred_repair_reruns_on_the_next_open_and_then_stops() {
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let agent = store
            .create_agent(NewAgent::new("legacy", "codex"))
            .unwrap();
        store
            .conn()
            .execute(
                "UPDATE agents SET harness = 'sylphx' WHERE id = ?1",
                [agent.id.as_str()],
            )
            .unwrap();
        // As if a peer process's write lock had deferred the repairs at open.
        store.shared.mark_repairs_pending();
        let before = passes();
        let _again = Store::open(&db).unwrap();
        assert_eq!(passes(), before + 1);
        assert_eq!(store.agent(&agent.id).unwrap().harness, "spiron");
        let _settled = Store::open(&db).unwrap();
        assert_eq!(passes(), before + 1);
    }

    #[test]
    fn reader_open_failure_preserves_authoritative_state() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("state.db");
        let store = Store::open(&path).unwrap();
        let agent = store
            .create_agent(NewAgent::new("reader-fallback", "test"))
            .unwrap();
        lock(&store.shared.readers).clear();
        let moved = dir.path().join("moved.db");
        std::fs::rename(&path, &moved).unwrap();
        assert!(store.shared.reader().is_none());
        let reader = store.read_conn();
        assert!(matches!(&reader, ReadConn::Guard(_)));
        let name: String = reader
            .query_row(
                "SELECT name FROM agents WHERE id = ?1",
                [agent.id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "reader-fallback");
        drop(reader);
        std::fs::rename(&moved, &path).unwrap();
    }

    #[test]
    fn a_long_read_never_blocks_a_writer() {
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let agent = store
            .create_agent(NewAgent::new("reader", "codex"))
            .unwrap();
        let reader = store.read_conn();
        reader.execute_batch("BEGIN").unwrap();
        fn count(conn: &Connection) -> i64 {
            conn.query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
                .unwrap()
        }
        assert_eq!(count(&reader), 0);
        std::thread::scope(|scope| {
            let (done, finished) = std::sync::mpsc::channel();
            let db = &db;
            let id = &agent.id;
            scope.spawn(move || {
                let writer = Store::open(db).unwrap();
                for n in 1..=20 {
                    writer.receive(id, Incoming::new(format!("m{n}"))).unwrap();
                    writer.note_delivered(id, n).unwrap();
                }
                done.send(()).unwrap();
            });
            let outcome = finished.recv_timeout(Duration::from_secs(10));
            // The open read keeps its snapshot while the writer commits.
            assert_eq!(count(&reader), 0);
            reader.execute_batch("COMMIT").unwrap();
            outcome.expect("a writer waited for a long read");
        });
        drop(reader);
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 20);
    }
}
