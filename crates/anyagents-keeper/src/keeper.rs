//! The keeper process: `anyagents keeper --dir <dir> [--lock-fd <n>]`.
//!
//! It reads its [`Spec`] from stdin, starts the adapter (which starts the
//! harness), binds its socket, says `ready` on stdout and then serves kernels
//! until one asks it to `stop`. A kernel that disconnects, cleanly or not,
//! changes nothing: the keeper never ends its harness for that. A request
//! already read is carried out even if the kernel that sent it is gone; its
//! answer is lost, and the next kernel's repeat of it is answered from the
//! adapter's memory.

use std::io::{Read as _, Write as _};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use anyagents_harness::HarnessError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::wire::{self, Event, Fault, FrameReader, Reply, Request};
use crate::{Hello, Hosted, RECORD, SOCKET, Spec};

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
            protocol: (crate::PROTOCOL_MIN, crate::PROTOCOL),
            binary: String::new(),
        }
    }
}

/// What a keeper records about itself in its directory.
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub(crate) struct Record {
    pub protocol: (u32, u32),
    pub binary: String,
    pub keeper_pid: u32,
    pub harness_pid: Option<u32>,
    pub agent: String,
    pub harness: String,
    pub model: Option<String>,
    /// The seat the harness runs in, so a kernel that finds this keeper dead
    /// can still end the harness group as that seat.
    pub seat: Option<String>,
    pub started_at_ms: i64,
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
    let shared = Arc::new(Shared {
        harness_pid: Mutex::new(adapter.process_id()),
        adapter: Mutex::new(Some(adapter)),
        owner: Mutex::new(None),
        note: Mutex::new(None),
        activity: Mutex::new(0),
        changed: Condvar::new(),
        generation: AtomicU64::new(0),
        lock,
        dir: dir.to_path_buf(),
        started_at_ms: crate::now_ms(),
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
        // Only this user (the server's) may drive the keeper; a harness runs
        // as its own seat and never reaches this socket.
        // SAFETY: geteuid takes no arguments and cannot fail.
        let me = unsafe { libc::geteuid() };
        if wire::peer_uid(&stream).ok() != Some(me) || wire::quiet_pipe(&stream).is_err() {
            continue;
        }
        let conn = Arc::new(Conn {
            stream,
            write: Mutex::new(()),
        });
        let serving = Arc::clone(&shared);
        let _ = std::thread::Builder::new()
            .name("keeper-conn".into())
            .spawn(move || serving.serve(conn));
    }
    0
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
}

struct Owner {
    generation: u64,
    conn: Arc<Conn>,
}

struct Shared<A> {
    adapter: Mutex<Option<A>>,
    owner: Mutex<Option<Owner>>,
    note: Mutex<Option<String>>,
    activity: Mutex<u64>,
    changed: Condvar,
    generation: AtomicU64,
    harness_pid: Mutex<Option<u32>>,
    lock: Option<OwnedFd>,
    dir: PathBuf,
    started_at_ms: i64,
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
    /// the whole state anyway.
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
            keeper_pid: self.keeper_pid,
            harness_pid: *lock_unpoisoned(&self.harness_pid),
            agent: self.spec.agent.clone(),
            harness: self.spec.harness.clone(),
            model: self.spec.model.clone(),
            seat: self.spec.seat.clone(),
            started_at_ms: self.started_at_ms,
        }
    }

    fn write_record(&self) {
        if let Ok(bytes) = serde_json::to_vec_pretty(&self.record()) {
            let _ = anyagents_harness::atomic_write_file(&self.dir.join(RECORD), &bytes, 0o600);
        }
    }

    fn serve(self: Arc<Self>, conn: Arc<Conn>) {
        let mut reader = FrameReader::new(&conn.stream);
        let mut greeted = false;
        let mut owned = None;
        while let Ok(Some(frame)) = reader.next_frame() {
            let request: Request = match serde_json::from_slice(&frame) {
                Ok(request) => request,
                Err(error) => {
                    let _ = conn.send(
                        &Reply::Err {
                            id: 0,
                            err: Fault::BadRequest {
                                message: error.to_string(),
                            },
                        },
                        &[],
                    );
                    continue;
                }
            };
            let id = request.id;
            match request.op.as_str() {
                // Frozen in every protocol version: a kernel that speaks no
                // version this keeper speaks can still end it.
                "stop" => self.stop(&conn, id),
                "hello" => match self.hello(&conn, id, &request.args) {
                    Ok(generation) => {
                        greeted = true;
                        if generation.is_some() {
                            owned = generation;
                        }
                    }
                    Err(fault) => {
                        let _ = conn.send(&Reply::Err { id, err: fault }, &[]);
                    }
                },
                _ if !greeted => {
                    let _ = conn.send(
                        &Reply::Err {
                            id,
                            err: Fault::BadRequest {
                                message: "hello first".into(),
                            },
                        },
                        &[],
                    );
                }
                "detach" => {
                    let _ = conn.send(
                        &Reply::Ok {
                            id,
                            ok: Value::Null,
                        },
                        &[],
                    );
                    break;
                }
                "note" => {
                    let note = request
                        .args
                        .get("note")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    *lock_unpoisoned(&self.note) = note;
                    let _ = conn.send(
                        &Reply::Ok {
                            id,
                            ok: Value::Null,
                        },
                        &[],
                    );
                }
                op => {
                    let reply = match self.call(op, &request.args) {
                        Ok(ok) => Reply::Ok { id, ok },
                        Err(err) => Reply::Err { id, err },
                    };
                    let _ = conn.send(&reply, &[]);
                }
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

    /// Agree on a protocol version; an owning kernel supersedes the last one
    /// and receives a copy of the process lock with the answer. Returns the
    /// owner generation when this connection took ownership.
    fn hello(&self, conn: &Arc<Conn>, id: u64, args: &Value) -> Result<Option<u64>, Fault> {
        let args: HelloArgs =
            serde_json::from_value(args.clone()).map_err(|error| Fault::BadRequest {
                message: error.to_string(),
            })?;
        let (keeper_min, keeper_max) = self.options.protocol;
        let protocol = args.protocol.1.min(keeper_max);
        if protocol < args.protocol.0.max(keeper_min) {
            return Err(Fault::Protocol {
                keeper: self.options.protocol,
            });
        }
        let health = match lock_unpoisoned(&self.adapter).as_ref() {
            Some(adapter) => adapter.health(),
            None => return Err(Fault::Stopped),
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
        let lock = match (&self.lock, args.owner) {
            (Some(lock), true) => Some(lock.as_raw_fd()),
            _ => None,
        };
        let hello = Hello {
            protocol,
            binary: self.options.binary.clone(),
            keeper_pid: self.keeper_pid,
            harness_pid: *lock_unpoisoned(&self.harness_pid),
            agent: self.spec.agent.clone(),
            harness: self.spec.harness.clone(),
            model: self.spec.model.clone(),
            seat: self.spec.seat.clone(),
            started_at_ms: self.started_at_ms,
            labels: self.spec.labels.clone(),
            note: lock_unpoisoned(&self.note).clone(),
            health,
            lock: lock.is_some(),
        };
        let ok = serde_json::to_value(&hello).map_err(|error| Fault::BadRequest {
            message: error.to_string(),
        })?;
        let fds: Vec<RawFd> = lock.into_iter().collect();
        let _ = conn.send(&Reply::Ok { id, ok }, &fds);
        Ok(generation)
    }

    fn call(&self, op: &str, args: &Value) -> Result<Value, Fault> {
        let mut adapter = lock_unpoisoned(&self.adapter);
        let Some(adapter) = adapter.as_mut() else {
            return Err(Fault::Stopped);
        };
        let answer = dispatch(adapter, op, args);
        let pid = adapter.process_id();
        let changed = {
            let mut known = lock_unpoisoned(&self.harness_pid);
            let changed = *known != pid;
            *known = pid;
            changed
        };
        if changed {
            self.write_record();
        }
        answer
    }

    /// End the harness (the adapter's drop ends its process group), answer,
    /// remove the keeper's directory and exit. The process lock is released
    /// when the last copy of it closes.
    fn stop(&self, conn: &Conn, id: u64) -> ! {
        let adapter = lock_unpoisoned(&self.adapter).take();
        drop(adapter);
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = conn.send(
            &Reply::Ok {
                id,
                ok: json!({"stopped": true}),
            },
            &[],
        );
        std::process::exit(0)
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
        "take_reclaimed_steers" => value(adapter.take_reclaimed_steers()),
        "has_unconfirmed_hold" => value(adapter.has_unconfirmed_hold()),
        "has_unconfirmed_idle_hold" => value(adapter.has_unconfirmed_idle_hold()),
        "peek_unconfirmed_request_ids" => value(adapter.peek_unconfirmed_request_ids()),
        "reconcile_unconfirmed_for_recovery" => {
            harness(adapter.reconcile_unconfirmed_for_recovery())
        }
        "poll_steer" => harness(adapter.poll_steer()),
        "inject" => harness(adapter.inject(&arg(args, "message")?)),
        "release_steer" => {
            adapter.release_steer(&arg::<String>(args, "keep")?);
            value(())
        }
        "exit_diagnostic" => value(adapter.exit_diagnostic()),
        "process_id" => value(adapter.process_id()),
        "session_id" => value(adapter.session_id()),
        "session_invalidated" => value(adapter.session_invalidated()),
        "state_dir" => value(adapter.state_dir()),
        _ => Err(Fault::UnknownOp),
    }
}
