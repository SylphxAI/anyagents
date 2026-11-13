//! The small files an agent view shows, kept in memory.
//!
//! A roster call used to read about five marker files per live agent (the last
//! fault record, the rotation and quota-clear stamps, the parent-notice marker,
//! the bound profile), plus the principal registry and install receipts. Under
//! host I/O pressure each of those reads can wait on the disk, so a roster of a
//! hundred agents waited on hundreds of them.
//!
//! Those files have many writers: the supervisor's adapters, the launcher,
//! operator commands, other processes on the machine. Rather than teach every
//! writer about a cache, the cache watches the directories that hold the files
//! (inotify), so every completed write, rename, unlink or mode change
//! invalidates exactly the entries it touched, whoever made it. A view first
//! applies every pending notification ([`FileCache::catch_up`]), so it sees
//! each write that finished before it began, exactly as a fresh read would.
//! A background thread keeps the notification queue drained and re-reads what
//! changed, and every [`SAFETY_REREAD`] it re-reads everything still in use and
//! forgets what is not. A value whose directory cannot be watched (missing up
//! to its ancestors, unreadable, out of watches) or a platform without inotify
//! is read directly every time, as before.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

#[cfg(target_os = "linux")]
use std::any::Any;
#[cfg(target_os = "linux")]
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::ffi::{OsStr, OsString};
#[cfg(target_os = "linux")]
use std::sync::Mutex;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

/// How often everything cached is re-read regardless of notifications.
#[cfg(target_os = "linux")]
const SAFETY_REREAD: Duration = Duration::from_secs(10 * 60);
/// How long an entry no view reads is kept, with its watch.
#[cfg(target_os = "linux")]
const EVICT_UNUSED: Duration = Duration::from_secs(60 * 60);

/// What a cached value depends on.
#[derive(Clone, Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) enum Dep {
    /// One file: its directory is watched for that name.
    File(PathBuf),
    /// Everything directly inside one directory.
    Dir(PathBuf),
}

#[cfg(target_os = "linux")]
type Value = Arc<dyn Any + Send + Sync>;
#[cfg(target_os = "linux")]
type Loader = Arc<dyn Fn() -> Value + Send + Sync>;

/// The process-wide cache. One inotify instance and one thread serve every
/// view in this process.
pub(crate) fn cache() -> &'static FileCache {
    static CACHE: OnceLock<FileCache> = OnceLock::new();
    let cache = CACHE.get_or_init(FileCache::new);
    #[cfg(target_os = "linux")]
    cache.start_once();
    cache
}

pub(crate) struct FileCache {
    #[cfg(target_os = "linux")]
    watcher: Option<linux::Watcher>,
    #[cfg(target_os = "linux")]
    started: OnceLock<()>,
}

impl FileCache {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            watcher: linux::Watcher::new()
                .map_err(|err| eprintln!("view file cache: no inotify, reading directly: {err}"))
                .ok(),
            #[cfg(target_os = "linux")]
            started: OnceLock::new(),
        }
    }

    /// Apply every notification queued so far. A view calls this once before it
    /// reads, so a write that completed before the view began is never served
    /// stale.
    pub(crate) fn catch_up(&self) {
        #[cfg(target_os = "linux")]
        if let Some(watcher) = &self.watcher {
            watcher.catch_up();
        }
    }

    /// The contents of `path`, `None` when it is absent or unreadable: exactly
    /// what `std::fs::read_to_string(path).ok()` answers.
    pub(crate) fn file(&self, path: &Path) -> Option<Arc<str>> {
        let owned = path.to_path_buf();
        self.fact(
            format!("file:{}", path.display()),
            Dep::File(path.to_path_buf()),
            move || std::fs::read_to_string(&owned).ok().map(Arc::<str>::from),
        )
    }

    /// A value computed from files, cached under `key` until `dep` changes.
    /// `compute` must read nothing outside `dep` (and the environment the key
    /// names).
    pub(crate) fn fact<T>(
        &self,
        key: String,
        dep: Dep,
        compute: impl Fn() -> T + Send + Sync + 'static,
    ) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        #[cfg(test)]
        let compute = {
            let label = key.clone();
            move || {
                loads().push(label.clone());
                compute()
            }
        };
        #[cfg(target_os = "linux")]
        if let Some(watcher) = &self.watcher {
            let loader: Loader = Arc::new(move || Arc::new(compute()) as Value);
            let value = watcher.get(key, dep, loader);
            return value
                .downcast_ref::<T>()
                .cloned()
                .expect("one cache key holds one type");
        }
        let _ = dep;
        compute()
    }

    /// How many loads this process performed for keys that mention `prefix`:
    /// every disk read a cache in this process made for that tree.
    #[cfg(test)]
    pub(crate) fn loads_under(prefix: &Path) -> usize {
        let prefix = prefix.display().to_string();
        loads().iter().filter(|key| key.contains(&prefix)).count()
    }

    #[cfg(all(test, target_os = "linux"))]
    fn watcher(&self) -> Option<&linux::Watcher> {
        self.watcher.as_ref()
    }

    #[cfg(target_os = "linux")]
    fn start_once(&'static self) {
        let Some(watcher) = &self.watcher else {
            return;
        };
        self.started.get_or_init(|| {
            if let Err(err) = std::thread::Builder::new()
                .name("anyagents-view-files".into())
                .spawn(move || watcher.run())
            {
                eprintln!("view file cache: no refresher thread: {err}");
            }
        });
    }
}

/// Every load any cache in this process performed, for tests that count reads.
#[cfg(test)]
fn loads() -> std::sync::MutexGuard<'static, Vec<String>> {
    static LOADS: OnceLock<std::sync::Mutex<Vec<String>>> = OnceLock::new();
    LOADS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(target_os = "linux")]
mod linux {
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    use std::os::unix::ffi::OsStrExt as _;

    use super::*;

    /// Events that can change what a read of a watched name returns.
    const MASK: u32 = libc::IN_MODIFY
        | libc::IN_ATTRIB
        | libc::IN_CLOSE_WRITE
        | libc::IN_MOVED_FROM
        | libc::IN_MOVED_TO
        | libc::IN_CREATE
        | libc::IN_DELETE
        | libc::IN_DELETE_SELF
        | libc::IN_MOVE_SELF
        | libc::IN_ONLYDIR;
    /// The watched directory itself went away or was replaced.
    const GONE: u32 =
        libc::IN_IGNORED | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_UNMOUNT;
    /// How far up a missing directory's ancestors a watch may move.
    const MAX_ANCESTORS: usize = 4;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct Key(String);

    struct Entry {
        value: Option<Value>,
        /// Bumped by every invalidation; a load installs only into the
        /// generation it started from.
        generation: u64,
        /// When a view last read it.
        used: Instant,
        load: Loader,
        wd: i32,
    }

    struct Watched {
        dependents: Vec<Dependent>,
    }

    struct Dependent {
        key: Key,
        /// The one name the entry reads here; `None`: any name.
        name: Option<OsString>,
        /// Watched in place of a missing directory below this one: a change
        /// here means the entry must be watched again from scratch.
        ancestor: bool,
    }

    /// Where one dependency is watched.
    struct Watch {
        wd: i32,
        name: Option<OsString>,
        ancestor: bool,
    }

    #[derive(Default)]
    struct State {
        entries: HashMap<Key, Entry>,
        by_dir: HashMap<PathBuf, i32>,
        by_wd: HashMap<i32, Watched>,
        generation: u64,
        next_sweep: Option<Instant>,
    }

    impl State {
        fn bump(&mut self) -> u64 {
            self.generation += 1;
            self.generation
        }
    }

    pub(super) struct Watcher {
        fd: OwnedFd,
        state: Mutex<State>,
    }

    impl Watcher {
        pub(super) fn new() -> std::io::Result<Self> {
            // SAFETY: plain syscall; the result is checked before use.
            let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
            if fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self {
                // SAFETY: `fd` is a fresh descriptor this value now owns.
                fd: unsafe { OwnedFd::from_raw_fd(fd) },
                state: Mutex::new(State::default()),
            })
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, State> {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        pub(super) fn catch_up(&self) {
            let mut state = self.lock();
            self.drain(&mut state);
        }

        pub(super) fn get(&self, key: String, dep: Dep, load: Loader) -> Value {
            let key = Key(key);
            let mut state = self.lock();
            let generation = match state.entries.get_mut(&key) {
                Some(entry) => {
                    entry.used = Instant::now();
                    if let Some(value) = &entry.value {
                        return Arc::clone(value);
                    }
                    entry.generation
                }
                None => {
                    // Watch before reading, so a write after the read is seen.
                    let Some(watch) = self.watch_dep(&mut state, &dep) else {
                        drop(state);
                        return load();
                    };
                    let generation = state.bump();
                    if let Some(watched) = state.by_wd.get_mut(&watch.wd) {
                        watched.dependents.push(Dependent {
                            key: key.clone(),
                            name: watch.name,
                            ancestor: watch.ancestor,
                        });
                    }
                    state.entries.insert(
                        key.clone(),
                        Entry {
                            value: None,
                            generation,
                            used: Instant::now(),
                            load: Arc::clone(&load),
                            wd: watch.wd,
                        },
                    );
                    generation
                }
            };
            drop(state);
            let value = load();
            self.install(&key, generation, &value);
            value
        }

        /// Keep `value` unless a notification arrived since `generation`
        /// (including one still queued from the read just done).
        fn install(&self, key: &Key, generation: u64, value: &Value) {
            let mut state = self.lock();
            self.drain(&mut state);
            if let Some(entry) = state.entries.get_mut(key)
                && entry.generation == generation
            {
                entry.value = Some(Arc::clone(value));
            }
        }

        fn reload(&self, key: &Key) {
            let (generation, load) = match self.lock().entries.get(key) {
                Some(entry) => (entry.generation, Arc::clone(&entry.load)),
                None => return,
            };
            let value = load();
            self.install(key, generation, &value);
        }

        /// Where `dep` is watched, or `None` when it cannot be. A missing
        /// directory is watched from its nearest existing ancestor, for the
        /// name that leads down to it.
        fn watch_dep(&self, state: &mut State, dep: &Dep) -> Option<Watch> {
            let (mut dir, mut name) = match dep {
                Dep::File(path) => (
                    path.parent()?.to_path_buf(),
                    Some(path.file_name()?.to_os_string()),
                ),
                Dep::Dir(dir) => (dir.clone(), None),
            };
            let mut ancestors = 0;
            loop {
                match self.watch(state, &dir) {
                    Ok(wd) => {
                        return Some(Watch {
                            wd,
                            name,
                            ancestor: ancestors > 0,
                        });
                    }
                    Err(err)
                        if err.kind() == std::io::ErrorKind::NotFound
                            && ancestors < MAX_ANCESTORS =>
                    {
                        ancestors += 1;
                        name = Some(dir.file_name()?.to_os_string());
                        dir = dir.parent()?.to_path_buf();
                    }
                    Err(_) => return None,
                }
            }
        }

        fn watch(&self, state: &mut State, dir: &Path) -> std::io::Result<i32> {
            if let Some(wd) = state.by_dir.get(dir) {
                return Ok(*wd);
            }
            let path = std::ffi::CString::new(dir.as_os_str().as_bytes())
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
            // SAFETY: a valid descriptor and NUL-terminated path.
            let wd = unsafe { libc::inotify_add_watch(self.fd.as_raw_fd(), path.as_ptr(), MASK) };
            if wd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            state.by_dir.insert(dir.to_path_buf(), wd);
            // Two paths to one directory share its descriptor.
            state.by_wd.entry(wd).or_insert_with(|| Watched {
                dependents: Vec::new(),
            });
            Ok(wd)
        }

        /// Read every queued notification and apply it. Returns the keys that
        /// lost a loaded value, for the refresher to read again.
        fn drain(&self, state: &mut State) -> Vec<Key> {
            let mut stale = Vec::new();
            // Aligned for the event header; one read holds many events.
            let mut buf = [0u64; 512];
            let header = std::mem::size_of::<libc::inotify_event>();
            loop {
                // SAFETY: the buffer is valid for its whole length in bytes.
                let read = unsafe {
                    libc::read(
                        self.fd.as_raw_fd(),
                        buf.as_mut_ptr().cast(),
                        buf.len() * std::mem::size_of::<u64>(),
                    )
                };
                if read < 0 {
                    if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    break;
                }
                let read = read as usize;
                if read == 0 {
                    break;
                }
                // SAFETY: the kernel wrote `read` bytes into the buffer.
                let bytes = unsafe { std::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), read) };
                let mut at = 0;
                while at + header <= read {
                    // SAFETY: a whole event header lies at `at`.
                    let event: libc::inotify_event =
                        unsafe { std::ptr::read_unaligned(bytes[at..].as_ptr().cast()) };
                    let start = at + header;
                    let end = (start + event.len as usize).min(read);
                    let raw = &bytes[start..end];
                    let name = OsStr::from_bytes(
                        &raw[..raw.iter().position(|b| *b == 0).unwrap_or(raw.len())],
                    );
                    self.apply(state, event.wd, event.mask, name, &mut stale);
                    at = end;
                }
            }
            stale
        }

        fn apply(&self, state: &mut State, wd: i32, mask: u32, name: &OsStr, stale: &mut Vec<Key>) {
            if mask & libc::IN_Q_OVERFLOW != 0 {
                // Notifications were lost: nothing cached can be trusted.
                let keys: Vec<Key> = state.entries.keys().cloned().collect();
                for key in keys {
                    invalidate(state, &key, stale);
                }
                return;
            }
            let Some(watched) = state.by_wd.get(&wd) else {
                return;
            };
            if mask & GONE != 0 {
                // The directory itself is gone or moved: forget its entries so
                // the next read watches whatever is at the path now.
                let keys: Vec<Key> = watched
                    .dependents
                    .iter()
                    .map(|dependent| dependent.key.clone())
                    .collect();
                if mask & libc::IN_MOVE_SELF != 0 {
                    // SAFETY: a valid descriptor and watch.
                    unsafe { libc::inotify_rm_watch(self.fd.as_raw_fd(), wd) };
                }
                forget_watch(state, wd);
                for key in keys {
                    forget(self, state, &key);
                }
                return;
            }
            let (moved, changed): (Vec<&Dependent>, Vec<&Dependent>) = watched
                .dependents
                .iter()
                .filter(|dependent| {
                    name.is_empty()
                        || dependent
                            .name
                            .as_deref()
                            .is_none_or(|wanted| wanted == name)
                })
                .partition(|dependent| dependent.ancestor);
            let moved: Vec<Key> = moved.into_iter().map(|d| d.key.clone()).collect();
            let changed: Vec<Key> = changed.into_iter().map(|d| d.key.clone()).collect();
            for key in moved {
                forget(self, state, &key);
            }
            for key in changed {
                invalidate(state, &key, stale);
            }
        }

        /// The refresher: drain notifications as they come, re-read what they
        /// invalidated, and sweep every [`SAFETY_REREAD`].
        pub(super) fn run(&self) {
            loop {
                let due = *self
                    .lock()
                    .next_sweep
                    .get_or_insert_with(|| Instant::now() + SAFETY_REREAD);
                self.wait(due.saturating_duration_since(Instant::now()));
                let reload = {
                    let mut state = self.lock();
                    let mut stale = self.drain(&mut state);
                    if Instant::now() >= due {
                        stale.extend(self.sweep(&mut state));
                        state.next_sweep = Some(Instant::now() + SAFETY_REREAD);
                    }
                    stale
                };
                for key in reload {
                    self.reload(&key);
                }
            }
        }

        fn wait(&self, timeout: Duration) {
            let mut poll = libc::pollfd {
                fd: self.fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let millis = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
            // SAFETY: one valid pollfd.
            unsafe { libc::poll(&mut poll, 1, millis) };
        }

        /// Forget entries no view read for [`EVICT_UNUSED`], and hand back the
        /// rest to be read again.
        fn sweep(&self, state: &mut State) -> Vec<Key> {
            let unused: Vec<Key> = state
                .entries
                .iter()
                .filter(|(_, entry)| entry.used.elapsed() >= EVICT_UNUSED)
                .map(|(key, _)| key.clone())
                .collect();
            for key in &unused {
                forget(self, state, key);
            }
            state.entries.keys().cloned().collect()
        }

        #[cfg(test)]
        pub(super) fn sweep_now(&self) {
            let reload = {
                let mut state = self.lock();
                self.drain(&mut state);
                self.sweep(&mut state)
            };
            for key in reload {
                self.reload(&key);
            }
        }

        #[cfg(test)]
        pub(super) fn watches(&self) -> usize {
            self.lock().by_wd.len()
        }
    }

    fn invalidate(state: &mut State, key: &Key, stale: &mut Vec<Key>) {
        let generation = state.bump();
        if let Some(entry) = state.entries.get_mut(key) {
            entry.generation = generation;
            if entry.value.take().is_some() {
                stale.push(key.clone());
            }
        }
    }

    /// Drop one entry, and its watch when nothing else depends on it.
    fn forget(watcher: &Watcher, state: &mut State, key: &Key) {
        let Some(entry) = state.entries.remove(key) else {
            return;
        };
        let Some(watched) = state.by_wd.get_mut(&entry.wd) else {
            return;
        };
        watched.dependents.retain(|dependent| &dependent.key != key);
        if watched.dependents.is_empty() {
            // SAFETY: a valid descriptor; a watch the kernel already dropped
            // only makes this fail.
            unsafe { libc::inotify_rm_watch(watcher.fd.as_raw_fd(), entry.wd) };
            forget_watch(state, entry.wd);
        }
    }

    fn forget_watch(state: &mut State, wd: i32) {
        state.by_wd.remove(&wd);
        state.by_dir.retain(|_, watched| *watched != wd);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn text(value: Option<Arc<str>>) -> Option<String> {
        value.map(|text| text.to_string())
    }

    /// Reads served from memory see every completed write, rename and unlink,
    /// and read the disk only after one.
    #[test]
    fn every_change_is_seen_and_nothing_else_is_read() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("marker");
        std::fs::write(&path, "one").unwrap();
        let cache = FileCache::new();
        let reads = || FileCache::loads_under(dir.path());

        assert_eq!(text(cache.file(&path)).as_deref(), Some("one"));
        assert_eq!(reads(), 1);
        for _ in 0..3 {
            cache.catch_up();
            assert_eq!(text(cache.file(&path)).as_deref(), Some("one"));
        }
        assert_eq!(reads(), 1, "an unchanged file is never read again");

        std::fs::write(&path, "two").unwrap();
        cache.catch_up();
        assert_eq!(text(cache.file(&path)).as_deref(), Some("two"));
        let tmp = dir.path().join(".marker.tmp");
        std::fs::write(&tmp, "three").unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        cache.catch_up();
        assert_eq!(text(cache.file(&path)).as_deref(), Some("three"));
        std::fs::remove_file(&path).unwrap();
        cache.catch_up();
        assert_eq!(cache.file(&path), None);
        assert_eq!(reads(), 4);

        // A write to another file in the directory costs nothing.
        std::fs::write(dir.path().join("transcript.jsonl"), "{}").unwrap();
        cache.catch_up();
        assert_eq!(cache.file(&path), None);
        assert_eq!(reads(), 4);
    }

    /// A file whose directory does not exist yet is watched from the nearest
    /// ancestor, and watched in place once the directory appears.
    #[test]
    fn a_missing_directory_is_watched_until_it_appears() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("agents/a1/config/.anyagents-profile");
        let cache = FileCache::new();
        assert_eq!(cache.file(&path), None);
        cache.catch_up();
        assert_eq!(cache.file(&path), None);
        assert_eq!(FileCache::loads_under(dir.path()), 1);

        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "openai/default").unwrap();
        cache.catch_up();
        assert_eq!(text(cache.file(&path)).as_deref(), Some("openai/default"));
        std::fs::write(&path, "openai/second").unwrap();
        cache.catch_up();
        assert_eq!(text(cache.file(&path)).as_deref(), Some("openai/second"));
    }

    /// A directory that is removed and made again is watched again.
    #[test]
    fn a_replaced_directory_is_watched_again() {
        let dir = crate::test_tempdir();
        let config = dir.path().join("config");
        let path = config.join("marker");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(&path, "1").unwrap();
        let cache = FileCache::new();
        assert_eq!(text(cache.file(&path)).as_deref(), Some("1"));
        std::fs::remove_dir_all(&config).unwrap();
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(&path, "2").unwrap();
        cache.catch_up();
        assert_eq!(text(cache.file(&path)).as_deref(), Some("2"));
        std::fs::write(&path, "3").unwrap();
        cache.catch_up();
        assert_eq!(text(cache.file(&path)).as_deref(), Some("3"));
    }

    /// A fact over a directory follows any change inside it; the safety pass
    /// re-reads what is in use and keeps its watches.
    #[test]
    fn a_fact_follows_its_directory_and_the_safety_pass_rereads_it() {
        let dir = crate::test_tempdir();
        let harness = dir.path().join("harnesses/codex");
        std::fs::create_dir_all(&harness).unwrap();
        let cache = FileCache::new();
        let count = {
            let harness = harness.clone();
            move || std::fs::read_dir(&harness).map(|d| d.count()).unwrap_or(0)
        };
        let key = format!("entries:{}", harness.display());
        let fact =
            |cache: &FileCache| cache.fact(key.clone(), Dep::Dir(harness.clone()), count.clone());
        assert_eq!(fact(&cache), 0);
        std::fs::write(harness.join("current.json"), "{}").unwrap();
        cache.catch_up();
        assert_eq!(fact(&cache), 1);
        let before = FileCache::loads_under(dir.path());
        let watches = cache.watcher().unwrap().watches();
        cache.watcher().unwrap().sweep_now();
        assert_eq!(FileCache::loads_under(dir.path()), before + 1);
        assert_eq!(cache.watcher().unwrap().watches(), watches);
        cache.catch_up();
        assert_eq!(fact(&cache), 1);
        assert_eq!(FileCache::loads_under(dir.path()), before + 1);
    }
}
