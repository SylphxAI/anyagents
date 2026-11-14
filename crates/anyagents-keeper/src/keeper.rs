//! The keeper process: `anyagents keeper --dir <dir> [--lock-fd <n>]`.
//!
//! It reads its [`Spec`] from stdin, starts the adapter (which starts the
//! harness), binds its socket, says `ready` on its start pipe and then serves
//! kernels until one asks it to `stop`. A kernel that disconnects, cleanly or
//! not, changes nothing: the keeper never ends its harness for that. A request
//! already read is carried out even if the kernel that sent it is gone; its
//! answer is lost, and the next kernel's repeat of it is answered from the
//! adapter's memory.
//!
//! Only the current owner (the kernel whose `hello` asked to own the keeper
//! most recently) may do anything but `hello`. Every refusal is an explicit
//! fault, so a client never mistakes a live keeper for a dead one.

use std::io::{Read as _, Write as _};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};
use std::time::Duration;

use anyagents_harness::{HarnessError, HarnessHealth};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::identity::ProcessIdentity;
use crate::wire::{self, Event, Fault, FrameReader, Reply, Request};
use crate::{Hello, Hosted, LOG, RECORD, SOCKET, Spec};

/// The most a keeper's log holds before it is rotated to `keeper.log.1`.
const LOG_LIMIT: u64 = 1 << 20;

/// How long a refusal may take to write before the connection is dropped.
const REFUSAL_WRITE: Duration = Duration::from_secs(2);

/// What a keeper advertises.
#[derive(Clone, Debug)]
pub struct KeeperOptions {
    /// The protocol versions it speaks, oldest and newest.
    pub protocol: (u32, u32),
    /// The binary it runs, for an operator reading `keepers list`.
    pub binary: String,
}

impl Default for KeeperOptions {
    fn default() -> Self {
        Self {
            protocol: crate::protocol(),
            binary: String::new(),
        }
    }
}

/// What a keeper records about itself in its directory. A kernel that finds
/// the keeper unreachable acts on the identities, never on bare pids.
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub(crate) struct Record {
    pub protocol: (u32, u32),
    pub binary: String,
    pub agent: String,
    pub harness: String,
    pub model: Option<String>,
    /// The seat the harness runs in, so a kernel that finds this keeper dead
    /// can still end the harness group as that seat.
    pub seat: Option<String>,
    pub started_at_ms: i64,
    pub keeper: Option<ProcessIdentity>,
    /// The harness process, which leads its own process group.
    pub harness_process: Option<ProcessIdentity>,
}

/// Parse `--dir <dir> [--lock-fd <n>]` and run the keeper. Returns the exit
/// code.
pub fn keeper_main<A, F>(args: &[String], options: KeeperOptions, start: F) -> i32
where
    A: Hosted + 'static,
    F: FnOnce(&Spec) -> Result<A, String>,
{
    let mut dir = None;
    let mut lock = None;
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--dir" => dir = rest.next().map(PathBuf::from),
            "--lock-fd" => lock = rest.next().and_then(|n| n.parse::<RawFd>().ok()),
            _ => {
                eprintln!("anyagents keeper: unknown argument {arg}");
                return 2;
            }
        }
    }
    let Some(dir) = dir else {
        eprintln!("anyagents keeper: --dir is required");
        return 2;
    };
    // Nothing the kernel had open reaches the keeper or its harness, beyond
    // its standard streams and the lock it was handed.
    close_inherited(lock);
    // A write to a harness pipe or a kernel socket whose reader left is an
    // error the adapter handles, never a signal that ends the keeper.
    // SAFETY: setting a signal disposition before any thread starts.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let lock = match lock {
        Some(fd) => {
            if let Err(error) = wire::set_cloexec(fd) {
                eprintln!("anyagents keeper: lock descriptor {fd}: {error}");
                return 2;
            }
            // SAFETY: the kernel passed this descriptor for the keeper to own.
            Some(unsafe { OwnedFd::from_raw_fd(fd) })
        }
        None => None,
    };
    run_keeper(&dir, lock, options, start)
}

/// Close every descriptor from 3 up except `keep`.
fn close_inherited(keep: Option<RawFd>) {
    #[cfg(target_os = "linux")]
    {
        const SYS_CLOSE_RANGE: libc::c_long = 436;
        let first: u32 = match keep {
            Some(3) => 4,
            Some(_) => 3,
            None => 3,
        };
        if keep.is_none_or(|fd| fd == 3) {
            // SAFETY: close_range on this process's own descriptor table.
            let rc = unsafe { libc::syscall(SYS_CLOSE_RANGE, first, u32::MAX, 0u32) };
            if rc == 0 {
                return;
            }
        }
    }
    #[cfg(target_os = "linux")]
    let listing = "/proc/self/fd";
    #[cfg(not(target_os = "linux"))]
    let listing = "/dev/fd";
    let Ok(entries) = std::fs::read_dir(listing) else {
        return;
    };
    let open: Vec<RawFd> = entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse().ok())
        .filter(|fd| *fd >= 3 && Some(*fd) != keep)
        .collect();
    for fd in open {
        // SAFETY: closing an inherited descriptor nothing in this process
        // uses yet (no thread has started). The directory's own descriptor
        // is among them and is closed harmlessly after its listing was read.
        unsafe { libc::close(fd) };
    }
}

/// Run a keeper in `dir`, holding `lock` (the agent's process lock) for as
/// long as it lives. Returns the exit code; a `stop` exits the process.
pub fn run_keeper<A, F>(dir: &Path, lock: Option<OwnedFd>, options: KeeperOptions, start: F) -> i32
where
    A: Hosted + 'static,
    F: FnOnce(&Spec) -> Result<A, String>,
{
    // The start report goes to the kernel's pipe, and nothing else does: the
    // adapter and anything it runs write to /dev/null instead.
    let mut reporter = match take_stdout() {
        Ok(reporter) => reporter,
        Err(error) => {
            eprintln!("anyagents keeper: cannot keep the start pipe: {error}");
            return 1;
        }
    };
    bound_log(&dir.join(LOG));
    let mut input = Vec::new();
    if let Err(error) = std::io::stdin().read_to_end(&mut input) {
        report(
            &mut reporter,
            &format!("error cannot read the launch: {error}"),
        );
        return 1;
    }
    let spec: Spec = match serde_json::from_slice(&input) {
        Ok(spec) => spec,
        Err(error) => {
            report(
                &mut reporter,
                &format!("error the launch is not a keeper spec: {error}"),
            );
            return 1;
        }
    };
    drop(input);
    redirect_to_null(libc::STDIN_FILENO);
    let adapter = match start(&spec) {
        Ok(adapter) => adapter,
        Err(error) => {
            report(
                &mut reporter,
                &format!("error {}", error.replace('\n', " ")),
            );
            return 1;
        }
    };
    let socket = dir.join(SOCKET);
    let listener = match bind(&socket) {
        Ok(listener) => listener,
        Err(error) => {
            drop(adapter);
            report(
                &mut reporter,
                &format!("error cannot bind {}: {error}", socket.display()),
            );
            return 1;
        }
    };
    let harness_pid = adapter.process_id();
    let shared = Arc::new(Shared {
        harness_process: Mutex::new(harness_pid.and_then(ProcessIdentity::of)),
        harness_pid: Mutex::new(harness_pid),
        health: Mutex::new(adapter.health()),
        adapter: Mutex::new(Some(adapter)),
        owner: Mutex::new(None),
        note: Mutex::new(None),
        activity: Mutex::new(0),
        changed: Condvar::new(),
        generation: AtomicU64::new(0),
        lock: Mutex::new(lock),
        dir: dir.to_path_buf(),
        started_at_ms: crate::now_ms(),
        keeper: ProcessIdentity::of(std::process::id()),
        keeper_pid: std::process::id(),
        spec,
        options,
    });
    shared.write_record();
    let watcher = Arc::clone(&shared);
    if let Some(adapter) = lock_unpoisoned(&shared.adapter).as_ref() {
        adapter.watch_activity(Arc::new(move || watcher.bump()));
    }
    let events = Arc::clone(&shared);
    if let Err(error) = std::thread::Builder::new()
        .name("keeper-events".into())
        .spawn(move || events.send_events())
    {
        report(
            &mut reporter,
            &format!("error cannot start the event thread: {error}"),
        );
        shared.end();
    }
    report(&mut reporter, "ready");
    drop(reporter);
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                eprintln!("anyagents keeper: accept: {error}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        if wire::quiet_pipe(&stream).is_err() {
            continue;
        }
        // Only this user (the server's) may drive the keeper; a harness runs
        // as its own seat. Another user is told so, never silently dropped.
        // SAFETY: geteuid takes no arguments and cannot fail.
        let me = unsafe { libc::geteuid() };
        if wire::peer_uid(&stream).ok() != Some(me) {
            refuse(
                &stream,
                Fault::Forbidden {
                    message: "the keeper serves only its own user".into(),
                },
            );
            continue;
        }
        let conn = Arc::new(Conn {
            stream,
            write: Mutex::new(()),
        });
        let serving = Arc::clone(&shared);
        let refused = Arc::clone(&conn);
        if std::thread::Builder::new()
            .name("keeper-conn".into())
            .spawn(move || serving.serve(conn))
            .is_err()
        {
            refuse(&refused.stream, Fault::Busy);
        }
    }
    0
}

/// Answer a connection the keeper will not serve, then close it.
fn refuse(stream: &UnixStream, fault: Fault) {
    let _ = stream.set_write_timeout(Some(REFUSAL_WRITE));
    let _ = wire::send_frame(stream, &Reply::Err { id: 0, err: fault }, &[]);
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

fn bind(path: &Path) -> std::io::Result<UnixListener> {
    if path.as_os_str().len() > crate::MAX_SOCKET_PATH {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "socket path is too long",
        ));
    }
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// The kernel's start pipe, moved off standard output, which then points at
/// `/dev/null`.
fn take_stdout() -> std::io::Result<std::fs::File> {
    // SAFETY: duplicating this process's own standard output.
    let fd = unsafe { libc::fcntl(libc::STDOUT_FILENO, libc::F_DUPFD_CLOEXEC, 3) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `fd` was just created and is owned here.
    let pipe = unsafe { std::fs::File::from_raw_fd(fd) };
    redirect_to_null(libc::STDOUT_FILENO);
    Ok(pipe)
}

/// Standard error goes through a pipe into `path`, which is rotated once it
/// passes [`LOG_LIMIT`]: what the adapter and its harness print is bounded.
/// Left as it is when the pipe cannot be made.
fn bound_log(path: &Path) {
    let mut fds = [0 as RawFd; 2];
    // SAFETY: pipe fills two descriptors.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return;
    }
    // SAFETY: both were just created and are owned here.
    let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    let _ = wire::set_cloexec(read.as_raw_fd());
    let path = path.to_path_buf();
    let copied = std::thread::Builder::new()
        .name("keeper-log".into())
        .spawn(move || copy_log(std::fs::File::from(read), &path));
    if copied.is_ok() {
        // SAFETY: dup2 onto this process's own standard error.
        unsafe { libc::dup2(write.as_raw_fd(), libc::STDERR_FILENO) };
    }
}

fn copy_log(mut from: std::fs::File, path: &Path) {
    let open = |path: &Path| {
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)
    };
    let Ok(mut log) = open(path) else { return };
    let mut written = log.metadata().map(|meta| meta.len()).unwrap_or(0);
    let mut chunk = [0u8; 8192];
    loop {
        let read = match from.read(&mut chunk) {
            Ok(0) => return,
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return,
        };
        if written + read as u64 > LOG_LIMIT {
            let _ = std::fs::rename(path, path.with_extension("log.1"));
            match open(path) {
                Ok(fresh) => log = fresh,
                Err(_) => return,
            }
            written = 0;
        }
        if log.write_all(&chunk[..read]).is_ok() {
            written += read as u64;
        }
    }
}

/// One line for the kernel waiting on this keeper's start.
fn report(pipe: &mut std::fs::File, line: &str) {
    let _ = writeln!(pipe, "{line}");
    let _ = pipe.flush();
}

/// Point `fd` at `/dev/null`: the keeper never holds the kernel's pipes, so a
/// supervisor reading the kernel's output sees it end when the kernel does.
fn redirect_to_null(fd: RawFd) {
    if let Ok(null) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
    {
        // SAFETY: dup2 onto a standard descriptor this process owns.
        unsafe { libc::dup2(null.as_raw_fd(), fd) };
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

struct Conn {
    stream: UnixStream,
    write: Mutex<()>,
}

impl Conn {
    fn send(&self, reply: &Reply, fds: &[RawFd]) -> std::io::Result<()> {
        let _guard = lock_unpoisoned(&self.write);
        wire::send_frame(&self.stream, reply, fds)
    }

    fn answer(&self, id: u64, answer: Result<Value, Fault>) {
        let reply = match answer {
            Ok(ok) => Reply::Ok { id, ok },
            Err(err) => Reply::Err { id, err },
        };
        let _ = self.send(&reply, &[]);
    }
}

struct Owner {
    generation: u64,
    conn: Arc<Conn>,
}

struct Shared<A> {
    adapter: Mutex<Option<A>>,
    /// The adapter's health as last read, so `hello` answers even while a
    /// call holds the adapter.
    health: Mutex<HarnessHealth>,
    owner: Mutex<Option<Owner>>,
    note: Mutex<Option<String>>,
    activity: Mutex<u64>,
    changed: Condvar,
    generation: AtomicU64,
    harness_pid: Mutex<Option<u32>>,
    harness_process: Mutex<Option<ProcessIdentity>>,
    /// The keeper's copy of the agent lock, closed once its harness is gone.
    lock: Mutex<Option<OwnedFd>>,
    dir: PathBuf,
    started_at_ms: i64,
    keeper: Option<ProcessIdentity>,
    keeper_pid: u32,
    spec: Spec,
    options: KeeperOptions,
}

#[derive(serde::Deserialize)]
struct HelloArgs {
    protocol: (u32, u32),
    #[serde(default)]
    owner: bool,
}

impl<A: Hosted + 'static> Shared<A> {
    fn bump(&self) {
        *lock_unpoisoned(&self.activity) += 1;
        self.changed.notify_all();
    }

    /// Tell the owning kernel each time something it reads changed. Changes
    /// with no kernel connected are not queued: a kernel that connects reads
    /// the whole state anyway. The cached health is refreshed when no call
    /// holds the adapter.
    fn send_events(&self) {
        let mut seen = 0;
        loop {
            {
                let mut count = lock_unpoisoned(&self.activity);
                while *count == seen {
                    count = self
                        .changed
                        .wait(count)
                        .unwrap_or_else(|error| error.into_inner());
                }
                seen = *count;
            }
            if let Ok(adapter) = self.adapter.try_lock()
                && let Some(adapter) = adapter.as_ref()
            {
                *lock_unpoisoned(&self.health) = adapter.health();
            }
            let owner = lock_unpoisoned(&self.owner)
                .as_ref()
                .map(|owner| Arc::clone(&owner.conn));
            if let Some(conn) = owner {
                let _ = conn.send(
                    &Reply::Event {
                        event: Event::Activity,
                    },
                    &[],
                );
            }
        }
    }

    fn record(&self) -> Record {
        Record {
            protocol: self.options.protocol,
            binary: self.options.binary.clone(),
            agent: self.spec.agent.clone(),
            harness: self.spec.harness.clone(),
            model: self.spec.model.clone(),
            seat: self.spec.seat.clone(),
            started_at_ms: self.started_at_ms,
            keeper: self.keeper.clone(),
            harness_process: lock_unpoisoned(&self.harness_process).clone(),
        }
    }

    fn write_record(&self) {
        if let Ok(bytes) = serde_json::to_vec_pretty(&self.record()) {
            let _ = anyagents_harness::atomic_write_file(&self.dir.join(RECORD), &bytes, 0o600);
        }
    }

    fn is_owner(&self, owned: Option<u64>) -> bool {
        owned.is_some_and(|generation| {
            lock_unpoisoned(&self.owner)
                .as_ref()
                .is_some_and(|owner| owner.generation == generation)
        })
    }

    fn serve(self: Arc<Self>, conn: Arc<Conn>) {
        let mut reader = FrameReader::new(&conn.stream);
        let mut owned = None;
        while let Ok(Some(frame)) = reader.next_frame() {
            let request: Request = match serde_json::from_slice(&frame) {
                Ok(request) => request,
                Err(error) => {
                    conn.answer(
                        0,
                        Err(Fault::BadRequest {
                            message: error.to_string(),
                        }),
                    );
                    continue;
                }
            };
            let id = request.id;
            if request.op == "hello" {
                let (generation, answer) = self.hello(&conn, id, &request.args);
                if generation.is_some() {
                    owned = generation;
                }
                if let Err(fault) = answer {
                    conn.answer(id, Err(fault));
                }
                continue;
            }
            // Everything but `hello` is the owner's alone: an observer, or a
            // kernel a newer one superseded, changes nothing.
            if !self.is_owner(owned) {
                conn.answer(
                    id,
                    Err(Fault::Forbidden {
                        message: "only the keeper's current owner may do that".into(),
                    }),
                );
                continue;
            }
            match request.op.as_str() {
                // Frozen in every protocol version: an owner that speaks no
                // version this keeper speaks can still end it.
                "stop" => self.stop(&conn, id),
                "detach" => {
                    conn.answer(id, Ok(Value::Null));
                    break;
                }
                "note" => {
                    let note = request
                        .args
                        .get("note")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    *lock_unpoisoned(&self.note) = note;
                    conn.answer(id, Ok(Value::Null));
                }
                op => conn.answer(id, self.call(op, &request.args)),
            }
        }
        if let Some(generation) = owned {
            let mut owner = lock_unpoisoned(&self.owner);
            if owner
                .as_ref()
                .is_some_and(|owner| owner.generation == generation)
            {
                *owner = None;
            }
        }
    }

    /// An owning `hello` supersedes the last owner, whatever version it
    /// speaks, so that it can always `stop` the keeper. It then agrees on a
    /// protocol version and receives a copy of the process lock with the
    /// answer, sent here. Never takes the adapter: a wedged call cannot hang
    /// it.
    fn hello(&self, conn: &Arc<Conn>, id: u64, args: &Value) -> (Option<u64>, Result<(), Fault>) {
        let args: HelloArgs = match serde_json::from_value(args.clone()) {
            Ok(args) => args,
            Err(error) => {
                return (
                    None,
                    Err(Fault::BadRequest {
                        message: error.to_string(),
                    }),
                );
            }
        };
        let generation = args.owner.then(|| {
            let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            let previous = lock_unpoisoned(&self.owner).replace(Owner {
                generation,
                conn: Arc::clone(conn),
            });
            if let Some(previous) = previous {
                let _ = previous.conn.send(
                    &Reply::Event {
                        event: Event::Superseded,
                    },
                    &[],
                );
                let _ = previous.conn.stream.shutdown(std::net::Shutdown::Both);
            }
            generation
        });
        let (keeper_min, keeper_max) = self.options.protocol;
        let protocol = args.protocol.1.min(keeper_max);
        if protocol < args.protocol.0.max(keeper_min) {
            return (
                generation,
                Err(Fault::Protocol {
                    keeper: self.options.protocol,
                }),
            );
        }
        let held = lock_unpoisoned(&self.lock);
        let lock = match (held.as_ref(), args.owner) {
            (Some(lock), true) => Some(lock.as_raw_fd()),
            _ => None,
        };
        let hello = Hello {
            protocol,
            binary: self.options.binary.clone(),
            keeper_pid: self.keeper_pid,
            keeper: self.keeper.clone(),
            harness_pid: *lock_unpoisoned(&self.harness_pid),
            agent: self.spec.agent.clone(),
            harness: self.spec.harness.clone(),
            model: self.spec.model.clone(),
            seat: self.spec.seat.clone(),
            started_at_ms: self.started_at_ms,
            labels: self.spec.labels.clone(),
            note: lock_unpoisoned(&self.note).clone(),
            health: lock_unpoisoned(&self.health).clone(),
            lock: lock.is_some(),
        };
        let answer = serde_json::to_value(&hello)
            .map_err(|error| Fault::BadRequest {
                message: error.to_string(),
            })
            .map(|ok| {
                // Sent while the guard holds the descriptor open: `stop` cannot
                // close it under this answer.
                let fds: Vec<RawFd> = lock.into_iter().collect();
                let _ = conn.send(&Reply::Ok { id, ok }, &fds);
            });
        drop(held);
        (generation, answer)
    }

    fn call(&self, op: &str, args: &Value) -> Result<Value, Fault> {
        let mut adapter = lock_unpoisoned(&self.adapter);
        let Some(adapter) = adapter.as_mut() else {
            return Err(Fault::Stopped);
        };
        let answer = dispatch(adapter, op, args);
        *lock_unpoisoned(&self.health) = adapter.health();
        let pid = adapter.process_id();
        let changed = {
            let mut known = lock_unpoisoned(&self.harness_pid);
            let changed = *known != pid;
            *known = pid;
            changed
        };
        if changed {
            *lock_unpoisoned(&self.harness_process) = pid.and_then(ProcessIdentity::of);
            self.write_record();
        }
        answer
    }

    /// End the harness, answer, remove the keeper's directory and exit. When
    /// no call holds the adapter, its drop ends the harness group as it
    /// always does; when one does (a call that never returns), the group is
    /// ended directly from its recorded identity. The process lock is
    /// released when the last copy of it closes.
    fn stop(&self, conn: &Conn, id: u64) -> ! {
        match self.adapter.try_lock() {
            Ok(mut adapter) => drop(adapter.take()),
            Err(TryLockError::Poisoned(poisoned)) => drop(poisoned.into_inner().take()),
            Err(TryLockError::WouldBlock) => self.end_harness_group(),
        }
        // The harness is gone: the keeper's copy of the lock goes before the
        // answer, so whoever reads `stopped` finds no copy here.
        drop(lock_unpoisoned(&self.lock).take());
        let _ = std::fs::remove_dir_all(&self.dir);
        conn.answer(id, Ok(json!({"stopped": true})));
        std::process::exit(0)
    }

    fn end_harness_group(&self) {
        let seat: Vec<(String, String)> = self
            .spec
            .seat
            .iter()
            .map(|seat| (anyagents_harness::SEAT_LAUNCH_ENV.to_string(), seat.clone()))
            .collect();
        if let Some(harness) = lock_unpoisoned(&self.harness_process).as_ref() {
            harness.end_group(&seat);
        }
    }

    /// The keeper cannot serve: end the harness and exit.
    fn end(&self) -> ! {
        drop(lock_unpoisoned(&self.adapter).take());
        let _ = std::fs::remove_dir_all(&self.dir);
        std::process::exit(1)
    }
}

fn arg<T: DeserializeOwned>(args: &Value, key: &str) -> Result<T, Fault> {
    serde_json::from_value(args.get(key).cloned().unwrap_or(Value::Null)).map_err(|error| {
        Fault::BadRequest {
            message: format!("{key}: {error}"),
        }
    })
}

fn value(answer: impl Serialize) -> Result<Value, Fault> {
    serde_json::to_value(answer).map_err(|error| Fault::BadRequest {
        message: error.to_string(),
    })
}

fn harness<T: Serialize>(answer: Result<T, HarnessError>) -> Result<Value, Fault> {
    match answer {
        Ok(answer) => value(answer),
        Err(error) => Err(Fault::Harness { error }),
    }
}

fn millis(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// One adapter call. The op names are the [`anyagents_harness::Harness`] and
/// [`Hosted`] method names.
fn dispatch<A: Hosted>(adapter: &mut A, op: &str, args: &Value) -> Result<Value, Fault> {
    match op {
        "deliver" => harness(adapter.deliver(&arg(args, "message")?)),
        "proven" => value(adapter.proven(&arg::<String>(args, "id")?)),
        // `Harness::stop`: the vendor process ends and is read to the end;
        // the adapter stays to answer `proven`. Not the keeper's own `stop`.
        "stop_harness" => {
            adapter.stop();
            value(())
        }
        "background" => harness(adapter.background()),
        "resume" => harness(adapter.resume(&arg::<String>(args, "session")?)),
        "cancel" => harness(adapter.cancel()),
        "health" => value(adapter.health()),
        "input_ready" => value(adapter.input_ready()),
        "ends_on_quiet" => value(
            adapter
                .ends_on_quiet()
                .map(|quiet| u64::try_from(quiet.as_millis()).unwrap_or(u64::MAX)),
        ),
        "quiet_for" => value(adapter.quiet_for(
            &arg::<String>(args, "id")?,
            millis(arg(args, "quiet_ms")?),
            millis(arg(args, "patience_ms")?),
        )),
        "served_usage" => value(adapter.served_usage()),
        "turns_served" => value(adapter.turns_served()),
        "last_usage" => value(adapter.last_usage()),
        "clear_quota" => {
            adapter.clear_quota();
            value(())
        }
        "exit_diagnostic" => value(adapter.exit_diagnostic()),
        "process_id" => value(adapter.process_id()),
        "session_id" => value(adapter.session_id()),
        "session_invalidated" => value(adapter.session_invalidated()),
        _ => Err(Fault::UnknownOp),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a harness prints through its keeper is bounded: past the limit
    /// the log is rotated, and only the newest output stays beside it.
    #[test]
    fn the_keeper_log_is_rotated_at_its_limit() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("printed");
        std::fs::write(&source, vec![b'x'; (LOG_LIMIT + LOG_LIMIT / 2) as usize]).unwrap();
        let log = dir.path().join(LOG);
        copy_log(std::fs::File::open(&source).unwrap(), &log);
        let current = std::fs::metadata(&log).unwrap().len();
        let rotated = std::fs::metadata(log.with_extension("log.1"))
            .unwrap()
            .len();
        assert!(
            current <= LOG_LIMIT && rotated <= LOG_LIMIT,
            "{current} {rotated}"
        );
        assert_eq!(current + rotated, LOG_LIMIT + LOG_LIMIT / 2);
    }
}
