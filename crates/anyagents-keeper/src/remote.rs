//! The kernel's side: start a keeper, take one over, and drive its adapter as
//! a [`Harness`].
//!
//! Every wait has a deadline: the keeper's start, `hello`, each call and
//! `stop`. A keeper that answers nothing within one is treated as wedged and
//! ended through a fresh connection, then, failing that, by signal. A keeper
//! is only ever treated as gone on proof: a socket that refuses (or no
//! socket), then a keeper process that is provably not running and an agent
//! lock that nobody holds. Recorded pids are only signalled when their
//! identity proves them the recorded processes ([`ProcessIdentity`]).
//!
//! A keeper whose answers this kernel cannot read (a newer or older shape
//! than the protocol it agreed to) is not a keeper that failed to answer: it
//! is stopped with the frozen `stop` and replaced, never retried while it
//! holds the agent's lock.

use std::io::{BufRead as _, BufReader, Write as _};
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Harness, HarnessError, HarnessHealth, OutboundMessage, Proof, TurnState,
    TurnUsage,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::identity::{Presence, ProcessIdentity, leads_session, session_groups};
use crate::keeper::Record;
use crate::wire::{self, Event, Fault, FrameReader, Reply, Request};
use crate::{AGENT, GROUPS, Hello, Hosted, LOCK, LOG, RECORD, SOCKET, SPAWNED, Spec};

/// The descriptor number the process lock has in a keeper.
const KEEPER_LOCK_FD: i32 = 3;

/// The longest a keeper may take to start its adapter and say `ready`, unless
/// the adapter declares a longer start of its own (Muse's resume retries, for
/// one): the kernel then waits that long, and a little more.
pub const READY_DEADLINE: Duration = Duration::from_secs(180);
/// The longest a keeper may take to answer `hello` (it never waits on the
/// adapter to answer it).
pub const HELLO_DEADLINE: Duration = Duration::from_secs(10);
/// The longest one adapter call may take before its keeper is treated as
/// wedged. `quiet_for` gets its own patience on top.
pub const CALL_DEADLINE: Duration = Duration::from_secs(120);
/// The longest a keeper may take to end its harness and exit on `stop`.
pub const STOP_DEADLINE: Duration = Duration::from_secs(30);

/// How often a wait for a process to exit looks again: nothing reports a
/// non-child's exit, and these waits are bounded by their deadline.
const EXIT_PROBE: Duration = Duration::from_millis(20);

/// The command that runs a keeper: the kernel's own binary and the arguments
/// that select keeper mode (`anyagents keeper`).
#[derive(Clone, Debug)]
pub struct Program {
    pub path: PathBuf,
    pub args: Vec<String>,
    /// How long its start may take ([`READY_DEADLINE`]).
    pub ready: Duration,
}

impl Program {
    pub fn new(path: PathBuf, args: Vec<String>) -> Self {
        Self {
            path,
            args,
            ready: READY_DEADLINE,
        }
    }
}

/// One keeper directory: `<root>/<instance>`, with the agent it serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instance {
    pub agent: String,
    pub dir: PathBuf,
}

fn euid() -> u32 {
    // SAFETY: geteuid takes no arguments and cannot fail.
    unsafe { libc::geteuid() }
}

/// `root` is this user's private directory, or absent: not a symlink, owned
/// by this effective user, open to nobody else. Every keeper operation checks
/// it before trusting anything inside.
pub fn check_root(root: &Path) -> Result<(), String> {
    let meta = match std::fs::symlink_metadata(root) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("cannot read {}: {error}", root.display())),
    };
    if !meta.file_type().is_dir() {
        return Err(format!("{} is not a directory", root.display()));
    }
    if meta.uid() != euid() {
        return Err(format!(
            "{} belongs to uid {}, not this user ({})",
            root.display(),
            meta.uid(),
            euid()
        ));
    }
    if meta.mode() & 0o077 != 0 {
        return Err(format!(
            "{} is open to other users (mode {:o})",
            root.display(),
            meta.mode() & 0o777
        ));
    }
    Ok(())
}

/// Every keeper directory under `root`, newest first. Empty when `root` is
/// not this user's private directory.
pub fn instances(root: &Path) -> Vec<Instance> {
    if check_root(root).is_err() {
        return Vec::new();
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut found: Vec<Instance> = entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| {
            let dir = entry.path();
            let agent = std::fs::read_to_string(dir.join(AGENT)).ok()?;
            Some(Instance {
                agent: agent.trim().to_string(),
                dir,
            })
        })
        .collect();
    found.sort_by(|left, right| right.dir.cmp(&left.dir));
    found
}

/// The agent lock a keeper directory names.
pub fn lock_path(dir: &Path) -> Option<PathBuf> {
    let text = std::fs::read_to_string(dir.join(LOCK)).ok()?;
    let path = PathBuf::from(text.trim());
    path.is_absolute().then_some(path)
}

/// The keeper directories of `agent`, newest first.
fn agent_instances(root: &Path, agent: &str) -> Vec<PathBuf> {
    instances(root)
        .into_iter()
        .filter(|instance| instance.agent == agent)
        .map(|instance| instance.dir)
        .collect()
}

/// Start a keeper for `spec` under `root`, handing it a copy of the agent's
/// process lock, and connect to it as its owner.
///
/// The keeper starts in its own session, so a signal to the kernel's process
/// group never reaches it, and with its output on its own log, so it never
/// holds a pipe the kernel's supervisor reads.
pub fn spawn(
    program: &Program,
    root: &Path,
    spec: &Spec,
    lock: Option<BorrowedFd<'_>>,
) -> Result<RemoteHarness, String> {
    private_dir(root)?;
    let dir = new_instance(root, spec)?;
    let mut child = match start_keeper(program, &dir, spec, lock) {
        Ok(child) => child,
        Err(Start::Failed(error)) => {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(error);
        }
        // The keeper finishes its start on its own, holding the agent's
        // lock, and the next kernel takes it over.
        Err(Start::Abandoned) => return Err(ABANDONED_START.into()),
    };
    match connect(&dir, true) {
        Ok(link) => Ok(RemoteHarness::new(link, dir, Some(child))),
        Err(error) => {
            end_start(&mut child, spec.seat.as_deref());
            let _ = std::fs::remove_dir_all(&dir);
            Err(match error {
                ConnectError::Protocol { keeper, .. } => {
                    let (min, max) = crate::protocol();
                    format!(
                        "keeper speaks protocol {}..={}, this kernel {min}..={max}",
                        keeper.0, keeper.1
                    )
                }
                ConnectError::Unreadable { why, .. } => why,
                ConnectError::MaybeGone => "keeper exited after it was ready".into(),
                ConnectError::Io(error) => error,
            })
        }
    }
}

/// Create `path` as this user's private directory, or check that it is one.
fn private_dir(path: &Path) -> Result<(), String> {
    match std::fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(format!("cannot create {}: {error}", path.display())),
    }
    check_root(path)
}

/// A fresh directory named by its start time (twelve hex digits of
/// milliseconds, so a name sorts with its age), recording the agent and the
/// agent's lock before anything starts.
fn new_instance(root: &Path, spec: &Spec) -> Result<PathBuf, String> {
    let start = crate::now_ms();
    for offset in 0..1000 {
        let dir = root.join(format!("{:012x}", start + offset));
        if dir.join(SOCKET).as_os_str().len() > crate::MAX_SOCKET_PATH {
            return Err(format!(
                "keeper socket path {} is longer than {} bytes",
                dir.join(SOCKET).display(),
                crate::MAX_SOCKET_PATH
            ));
        }
        match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => {
                let lock = spec
                    .lock_path
                    .as_ref()
                    .map(|path| std::fs::write(dir.join(LOCK), path.as_os_str().as_encoded_bytes()))
                    .transpose();
                if let Err(error) = std::fs::write(dir.join(AGENT), &spec.agent).and(lock) {
                    let _ = std::fs::remove_dir_all(&dir);
                    return Err(format!("cannot record the keeper's agent: {error}"));
                }
                return Ok(dir);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("cannot create {}: {error}", dir.display())),
        }
    }
    Err(format!("no free keeper directory in {}", root.display()))
}

/// Why a keeper did not start for this kernel.
enum Start {
    Failed(String),
    /// This kernel stops ([`abandon_starts`]): the keeper is left to finish.
    Abandoned,
}

impl From<String> for Start {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

/// What [`spawn`] answers for a start this kernel stopped waiting for
/// ([`abandon_starts`]): the keeper goes on starting and holds the agent's
/// lock, so the caller must not unlock its own copy.
pub const ABANDONED_START: &str =
    "this kernel is stopping; the keeper finishes its start for the next";

/// Set once this kernel stops: a start in progress is no longer waited for.
static ABANDONED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Wakes a start waiting for its keeper's `ready`: the keeper answered, or
/// this kernel stops.
static START_WAKE: Condvar = Condvar::new();
static START_LOCK: Mutex<()> = Mutex::new(());

/// Stop waiting for every keeper start in progress, now and later: this
/// kernel is stopping and leaves its keepers to the next one. A keeper in
/// its start goes on starting, holding the agent's lock, and is taken over
/// once it answers; nothing is ended. Called once, as the kernel exits.
pub fn abandon_starts() {
    ABANDONED.store(true, std::sync::atomic::Ordering::SeqCst);
    let _guard = lock(&START_LOCK);
    START_WAKE.notify_all();
}

fn start_keeper(
    program: &Program,
    dir: &Path,
    spec: &Spec,
    lock: Option<BorrowedFd<'_>>,
) -> Result<Child, Start> {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join(LOG))
        .map_err(|error| format!("cannot open the keeper log: {error}"))?;
    let mut command = Command::new(&program.path);
    command
        .args(&program.args)
        .arg("--dir")
        .arg(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(log);
    let lock_fd = lock.map(|fd| fd.as_raw_fd());
    if lock_fd.is_some() {
        command.arg("--lock-fd").arg(KEEPER_LOCK_FD.to_string());
    }
    // SAFETY: only async-signal-safe calls between fork and exec: setsid,
    // dup2 and fcntl on descriptors the parent keeps open across the spawn.
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if let Some(fd) = lock_fd {
                if fd == KEEPER_LOCK_FD {
                    let flags = libc::fcntl(fd, libc::F_GETFD);
                    if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                } else if libc::dup2(fd, KEEPER_LOCK_FD) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("cannot start {}: {error}", program.path.display()))?;
    record_spawned(dir, child.id(), spec.seat.as_deref());
    let launch = serde_json::to_vec(spec).map_err(|error| error.to_string());
    let written = launch.and_then(|bytes| {
        let mut stdin = child.stdin.take().ok_or("keeper stdin missing")?;
        stdin
            .write_all(&bytes)
            .map_err(|error| format!("cannot hand the launch to the keeper: {error}"))
    });
    type Line = std::io::Result<(usize, String)>;
    let answer: Arc<Mutex<Option<Line>>> = Arc::default();
    let sent = Arc::clone(&answer);
    match (written, child.stdout.take()) {
        (Ok(()), Some(stdout)) => {
            let reading = std::thread::Builder::new()
                .name("anyagents-keeper-start".into())
                .spawn(move || {
                    let mut line = String::new();
                    let read = BufReader::new(stdout)
                        .read_line(&mut line)
                        .map(|count| (count, line));
                    *self::lock(&sent) = Some(read);
                    let _guard = self::lock(&START_LOCK);
                    START_WAKE.notify_all();
                });
            if let Err(error) = reading {
                end_start(&mut child, spec.seat.as_deref());
                return Err(format!("cannot read the keeper's start: {error}").into());
            }
        }
        (Err(error), _) => {
            end_start(&mut child, spec.seat.as_deref());
            return Err(error.into());
        }
        (Ok(()), None) => {
            end_start(&mut child, spec.seat.as_deref());
            return Err(String::from("keeper stdout missing").into());
        }
    }
    let deadline = Instant::now() + program.ready;
    let read = {
        let mut guard = self::lock(&START_LOCK);
        loop {
            if let Some(read) = self::lock(&answer).take() {
                break Some(read);
            }
            if ABANDONED.load(std::sync::atomic::Ordering::SeqCst) {
                // Left running, unreaped: the next kernel takes it over.
                drop(child);
                return Err(Start::Abandoned);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break None;
            }
            guard = START_WAKE
                .wait_timeout(guard, left)
                .unwrap_or_else(|error| error.into_inner())
                .0;
        }
    };
    let failure = match read {
        Some(Ok((_, line))) if line.trim_end() == "ready" => return Ok(child),
        Some(Ok((0, _))) => {
            let tail = log_tail(dir);
            if tail.is_empty() {
                "keeper exited before it was ready".to_string()
            } else {
                format!("keeper exited before it was ready: {tail}")
            }
        }
        Some(Ok((_, line))) => line
            .trim_end()
            .strip_prefix("error ")
            .unwrap_or(line.trim_end())
            .to_string(),
        Some(Err(error)) => error.to_string(),
        None => format!(
            "keeper was not ready within {} s (its harness did not finish starting)",
            program.ready.as_secs()
        ),
    };
    end_start(&mut child, spec.seat.as_deref());
    Err(failure.into())
}

/// End a keeper this kernel started and has not reaped, with everything in
/// the session it leads: its harness and whatever that started. Its pid, and
/// with it the session id, cannot have been reused before the reap here.
fn end_start(child: &mut Child, seat: Option<&str>) {
    let keeper = child.id();
    let groups = session_groups(keeper);
    let _ = child.kill();
    if let Some(environment) = trusted_seat_env(seat) {
        for group in groups.into_iter().filter(|group| *group != keeper) {
            anyagents_harness::terminate_group(group, &environment);
        }
    }
    let _ = child.wait();
}

/// What the kernel records about a keeper the moment it starts it.
#[derive(serde::Serialize, serde::Deserialize)]
struct Spawned {
    keeper: ProcessIdentity,
    /// The seat its harness runs in, to end that harness as the seat.
    seat: Option<String>,
}

/// Write the identity of the keeper just started into its directory, before
/// anything else can happen to it.
fn record_spawned(dir: &Path, pid: u32, seat: Option<&str>) {
    let Some(keeper) = ProcessIdentity::of(pid) else {
        return;
    };
    let spawned = Spawned {
        keeper,
        seat: seat.map(str::to_string),
    };
    match serde_json::to_vec(&spawned) {
        Ok(bytes) => {
            if let Err(error) =
                anyagents_harness::atomic_write_file(&dir.join(SPAWNED), &bytes, 0o600)
            {
                eprintln!(
                    "anyagents-keeper: {}: cannot record the keeper started: {error}",
                    dir.display()
                );
            }
        }
        Err(error) => eprintln!("anyagents-keeper: cannot record a keeper: {error}"),
    }
}

fn read_spawned(dir: &Path) -> Option<Spawned> {
    let bytes = std::fs::read(dir.join(SPAWNED)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// The keeper process of `dir` and its harness's seat: as the keeper
/// recorded them, else as the kernel that started it did.
fn keeper_identity(
    dir: &Path,
    record: Option<&Record>,
) -> (Option<ProcessIdentity>, Option<String>) {
    match record {
        Some(record) if record.keeper.is_some() => (record.keeper.clone(), record.seat.clone()),
        _ => match read_spawned(dir) {
            Some(spawned) => (Some(spawned.keeper), spawned.seat),
            None => (None, record.and_then(|record| record.seat.clone())),
        },
    }
}

fn log_tail(dir: &Path) -> String {
    let text = std::fs::read_to_string(dir.join(LOG)).unwrap_or_default();
    let tail: Vec<&str> = text.lines().rev().take(3).collect();
    tail.into_iter().rev().collect::<Vec<_>>().join(" | ")
}

/// Take over the keeper a previous kernel left for `agent`: the newest one
/// that answers. An older live keeper of the same agent (a crash in the
/// middle of a replacement) is stopped; [`attach_all`] hands them over
/// instead, for a kernel that commits what they proved first.
pub fn attach(root: &Path, agent: &str) -> Result<Option<RemoteHarness>, String> {
    let mut found = attach_all(root, agent)?.into_iter();
    let newest = found.next();
    for older in found {
        let dir = older.dir().to_path_buf();
        if let Err(error) = older.end() {
            eprintln!("anyagents-keeper: keeper {}: {error}", dir.display());
        }
    }
    Ok(newest)
}

/// Take over every live keeper a previous kernel left for `agent`, newest
/// first. A keeper that speaks no protocol this kernel speaks is stopped. A
/// keeper that does not answer is cleared only once it is proven gone;
/// otherwise this is an error when no keeper newer than it answered, and
/// nothing is started beside it.
pub fn attach_all(root: &Path, agent: &str) -> Result<Vec<RemoteHarness>, String> {
    check_root(root)?;
    let mut found: Vec<RemoteHarness> = Vec::new();
    for dir in agent_instances(root, agent) {
        let outcome = match connect(&dir, true) {
            Ok(link) => {
                let remote = RemoteHarness::new(link, dir.clone(), None);
                remote.prime();
                found.push(remote);
                Ok(())
            }
            Err(ConnectError::MaybeGone) => clear_if_gone(&dir),
            Err(
                ConnectError::Protocol { stream, .. } | ConnectError::Unreadable { stream, .. },
            ) => stop_unspoken(stream, &dir),
            Err(ConnectError::Io(error)) => Err(error),
        };
        if let Err(error) = outcome {
            let error = format!("keeper {}: {error}", dir.display());
            if found.is_empty() {
                return Err(error);
            }
            // The newest keeper is taken; an older one that cannot be
            // settled now is left for the operator, and said so.
            eprintln!("anyagents-keeper: {error}");
        }
    }
    Ok(found)
}

/// Stop every keeper of `agent`, ending its harness. Every keeper that could
/// not be stopped, or proven gone, is an error.
pub fn stop_agent(root: &Path, agent: &str) -> Result<(), String> {
    stop_agent_with(root, agent, false)
}

/// [`stop_agent`], and with `force`, also end a keeper that does not answer
/// while its process runs (its socket is gone, or it is wedged): by signal,
/// once its recorded identity proves the process is that keeper, with the
/// same start time, in this boot and namespace, leading its own session.
pub fn stop_agent_with(root: &Path, agent: &str, force: bool) -> Result<(), String> {
    check_root(root)?;
    let mut failures = Vec::new();
    for dir in agent_instances(root, agent) {
        let outcome = match connect(&dir, true) {
            Ok(link) => RemoteHarness::new(link, dir.clone(), None).end(),
            Err(ConnectError::MaybeGone) => clear_if_gone(&dir),
            Err(
                ConnectError::Protocol { stream, .. } | ConnectError::Unreadable { stream, .. },
            ) => stop_unspoken(stream, &dir),
            Err(ConnectError::Io(error)) => Err(error),
        };
        let outcome = match outcome {
            Err(error) if force => {
                force_end(&dir).map_err(|forced| format!("{error}; forced: {forced}"))
            }
            outcome => outcome,
        };
        if let Err(error) = outcome {
            failures.push(format!("{}: {error}", dir.display()));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

/// End the keeper of `dir` by signal, with everything in its session, then
/// clear the directory. Only a keeper whose recorded identity proves it the
/// running process at that pid, leading its own session, is signalled.
fn force_end(dir: &Path) -> Result<(), String> {
    let record = read_record(dir);
    let (keeper, seat) = keeper_identity(dir, record.as_ref());
    let keeper = keeper.ok_or("nothing records which process the keeper is")?;
    match keeper.presence() {
        Presence::Ended => return clear_if_gone(dir),
        Presence::Unknown => {
            return Err(format!(
                "pid {} cannot be proven to be the keeper (another boot, namespace or process)",
                keeper.pid
            ));
        }
        Presence::Running => {}
    }
    if !leads_session(keeper.pid) {
        return Err(format!(
            "pid {} does not lead its own session, as a keeper does",
            keeper.pid
        ));
    }
    let environment = trusted_seat_env(seat.as_deref())
        .ok_or("the recorded seat launcher is not trusted; nothing is signalled")?;
    // SAFETY: SIGKILL to the process its recorded identity proves is this
    // keeper, checked just above.
    unsafe { libc::kill(keeper.pid as i32, libc::SIGKILL) };
    let deadline = Instant::now() + STOP_DEADLINE;
    while keeper.presence() == Presence::Running {
        if Instant::now() >= deadline {
            return Err(format!("pid {} did not end on SIGKILL", keeper.pid));
        }
        std::thread::sleep(EXIT_PROBE);
    }
    if let Some(harness) = record
        .as_ref()
        .and_then(|record| record.harness_process.as_ref())
    {
        harness.end_group(&environment);
    }
    for group in recorded_groups(dir) {
        group.end_group(&environment);
    }
    let _ = std::fs::remove_dir_all(dir);
    Ok(())
}

/// The process groups the keeper in `dir` recorded starting, by the
/// identity of each leader.
fn recorded_groups(dir: &Path) -> Vec<ProcessIdentity> {
    std::fs::read(dir.join(GROUPS))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// What the keeper in `dir` says about itself, without taking it over.
pub fn inspect(dir: &Path) -> Result<Hello, String> {
    match connect(dir, false) {
        Ok(link) => {
            let hello = link.hello.clone();
            let _ = link.stream.shutdown(std::net::Shutdown::Both);
            Ok(hello)
        }
        Err(ConnectError::MaybeGone) => Err("does not answer".into()),
        Err(ConnectError::Protocol { keeper, .. }) => {
            Err(format!("speaks protocol {}..={} only", keeper.0, keeper.1))
        }
        Err(ConnectError::Unreadable { why, .. }) => Err(why),
        Err(ConnectError::Io(error)) => Err(error),
    }
}

/// A keeper whose socket refuses is gone only on proof: its process is not
/// running, and nobody holds the agent lock it held for its whole life. Then
/// the harness group it recorded is ended (when its identity proves it the
/// same group) and its directory cleared. Without proof nothing is touched.
fn clear_if_gone(dir: &Path) -> Result<(), String> {
    let record = read_record(dir);
    let (keeper, seat) = keeper_identity(dir, record.as_ref());
    let presence = keeper.as_ref().map(ProcessIdentity::presence);
    if presence == Some(Presence::Running) {
        return Err("does not answer, but its process still runs".into());
    }
    // A keeper proven ended is gone, whoever holds the agent lock now (an
    // older keeper of the same agent may). Without that proof, only a lock
    // nobody holds proves it.
    let lock = match (presence, lock_path(dir)) {
        (Some(Presence::Ended), _) => None,
        (_, Some(path)) => Some(lock_if_free(&path).ok_or_else(|| {
            format!(
                "does not answer, and the agent lock {} is still held",
                path.display()
            )
        })?),
        (_, None) => {
            return Err("does not answer, and nothing proves its keeper gone".into());
        }
    };
    match trusted_seat_env(seat.as_deref()) {
        Some(environment) => {
            if let Some(harness) = record
                .as_ref()
                .and_then(|record| record.harness_process.as_ref())
            {
                harness.end_group(&environment);
            }
            // A keeper that died before it recorded its harness leaves that
            // harness in a process group it recorded as it started it: each
            // is ended when its leader is still the one recorded, or gone
            // (a pid is not reused while it numbers a live group). Nothing
            // it did not record is touched.
            if presence == Some(Presence::Ended) {
                for group in recorded_groups(dir) {
                    group.end_group(&environment);
                }
            }
        }
        None => eprintln!(
            "anyagents-keeper: {}: the recorded seat launcher is not trusted; its harness group is left alone",
            dir.display()
        ),
    }
    let _ = std::fs::remove_dir_all(dir);
    drop(lock);
    Ok(())
}

/// Take the lock at `path` on a fresh descriptor, when nobody holds it. The
/// returned file holds it until dropped.
fn lock_if_free(path: &Path) -> Option<std::fs::File> {
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => file,
        // Never created: nobody ever held it.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return std::fs::File::open("/dev/null").ok();
        }
        Err(_) => return None,
    };
    // SAFETY: flock on a descriptor this function owns.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    (rc == 0).then_some(file)
}

fn read_record(dir: &Path) -> Option<Record> {
    let bytes = std::fs::read(dir.join(RECORD)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// The environment that signals a seat's processes as that seat, when its
/// launcher can be trusted to run: an absolute path to a regular file owned
/// by root or by this user and writable by nobody else. `None` when not.
fn trusted_seat_env(seat: Option<&str>) -> Option<Vec<(String, String)>> {
    let Some(seat) = seat else {
        return Some(Vec::new());
    };
    let launcher = Path::new(seat.split('\t').next()?);
    let meta = std::fs::metadata(launcher).ok()?;
    let trusted = launcher.is_absolute()
        && meta.is_file()
        && (meta.uid() == 0 || meta.uid() == euid())
        && meta.mode() & 0o022 == 0;
    trusted.then(|| {
        vec![(
            anyagents_harness::SEAT_LAUNCH_ENV.to_string(),
            seat.to_string(),
        )]
    })
}

/// `stop` from an owner that shares no protocol version with the keeper:
/// the owning `hello` already took the keeper over, and `stop` is frozen.
fn stop_unspoken(stream: UnixStream, dir: &Path) -> Result<(), String> {
    let request = Request {
        id: u64::MAX,
        op: "stop".into(),
        args: Value::Null,
    };
    let _ = stream.set_read_timeout(Some(STOP_DEADLINE));
    let _ = stream.set_write_timeout(Some(HELLO_DEADLINE));
    wire::send_frame(&stream, &request, &[])
        .map_err(|error| format!("cannot ask it to stop: {error}"))?;
    let mut reader = FrameReader::new(&stream);
    loop {
        match reader.next_frame() {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(error) => return Err(format!("did not stop: {error}")),
        }
    }
    if dir.exists() {
        Err("did not stop".into())
    } else {
        Ok(())
    }
}

enum ConnectError {
    /// Nothing listens (no socket, or it refuses): the keeper may be gone.
    /// Only proof makes it so ([`clear_if_gone`]).
    MaybeGone,
    /// It answers, but in no protocol this kernel speaks. This connection
    /// owns the keeper, so it can still `stop` it.
    Protocol {
        keeper: (u32, u32),
        stream: UnixStream,
    },
    /// It answers, in a shape this kernel cannot read: a binary that claims a
    /// version it does not write. It cannot be driven, so it is stopped (the
    /// owning `hello` took it over) and replaced, never retried.
    Unreadable { why: String, stream: UnixStream },
    /// Anything else, including a refusal or no answer in time: the keeper
    /// may well be alive.
    Io(String),
}

struct Connected {
    stream: UnixStream,
    reader: FrameReader,
    hello: Hello,
    lock: Option<OwnedFd>,
}

fn connect(dir: &Path, owner: bool) -> Result<Connected, ConnectError> {
    let stream = match UnixStream::connect(dir.join(SOCKET)) {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Err(ConnectError::MaybeGone);
        }
        Err(error) => return Err(ConnectError::Io(format!("cannot connect: {error}"))),
    };
    let io = |what: &str, error: std::io::Error| ConnectError::Io(format!("{what}: {error}"));
    wire::quiet_pipe(&stream).map_err(|error| io("cannot set up the socket", error))?;
    stream
        .set_read_timeout(Some(HELLO_DEADLINE))
        .and_then(|()| stream.set_write_timeout(Some(HELLO_DEADLINE)))
        .map_err(|error| io("cannot bound the greeting", error))?;
    let (min, max) = crate::protocol();
    let hello = Request {
        id: 1,
        op: "hello".into(),
        args: json!({"protocol": [min, max], "owner": owner}),
    };
    wire::send_frame(&stream, &hello, &[]).map_err(|error| io("cannot greet the keeper", error))?;
    let mut reader = FrameReader::new(&stream);
    // An owner may be told of activity before its hello is answered. An
    // event this kernel does not know is skipped, as the reader does.
    let reply = loop {
        let frame = match reader.next_frame() {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                return Err(ConnectError::Io(
                    "the keeper closed the connection without answering".into(),
                ));
            }
            Err(error) => return Err(io("the keeper did not answer hello", error)),
        };
        if is_event(&frame) {
            continue;
        }
        match serde_json::from_slice::<Reply>(&frame) {
            Ok(reply) => break reply,
            Err(error) => {
                return Err(ConnectError::Unreadable {
                    why: format!("unreadable answer to hello: {error}"),
                    stream,
                });
            }
        }
    };
    match reply {
        Reply::Ok { ok, .. } => {
            let hello: Hello = match serde_json::from_value(ok) {
                Ok(hello) => hello,
                Err(error) => {
                    return Err(ConnectError::Unreadable {
                        why: format!("unreadable hello: {error}"),
                        stream,
                    });
                }
            };
            let lock = if hello.lock {
                reader.take_fds().into_iter().next()
            } else {
                None
            };
            // Calls carry their own deadlines from here.
            stream
                .set_read_timeout(None)
                .and_then(|()| stream.set_write_timeout(Some(HELLO_DEADLINE)))
                .map_err(|error| io("cannot set up the socket", error))?;
            Ok(Connected {
                stream,
                reader,
                hello,
                lock,
            })
        }
        Reply::Err {
            err: Fault::Protocol { keeper },
            ..
        } => Err(ConnectError::Protocol { keeper, stream }),
        Reply::Err { err, .. } => Err(ConnectError::Io(format!("hello refused: {err:?}"))),
        Reply::Event { .. } => unreachable!("events are skipped above"),
    }
}

/// A frame the keeper sent unasked: it has an `event` and no `id`.
fn is_event(frame: &[u8]) -> bool {
    serde_json::from_slice::<Value>(frame)
        .is_ok_and(|value| value.get("event").is_some() && value.get("id").is_none())
}

/// Why a call has no answer.
enum CallError {
    /// The keeper is gone (or going): the harness is dead to this kernel.
    Dead,
    /// No answer within the call's deadline: the keeper is wedged.
    Timeout(Duration),
    /// The keeper does not know the op; use the contract's default.
    Unknown,
    Fault(Fault),
}

#[derive(Default)]
struct LinkState {
    answer: Option<(u64, Result<Value, Fault>)>,
    /// The call waiting for an answer.
    waiting: Option<u64>,
    closed: bool,
    /// The reader has finished what an unasked close requires.
    settled: bool,
    superseded: bool,
    /// A call went unanswered past its deadline, or was answered in a shape
    /// this kernel cannot read: no further call is sent.
    wedged: bool,
    /// This kernel asked for the connection to end (stop or detach).
    ending: bool,
}

#[derive(Default)]
struct Cache {
    health: Option<HarnessHealth>,
    served_usage: TurnUsage,
    last_usage: TurnUsage,
    turns_served: u32,
}

type Notify = Arc<dyn Fn() + Send + Sync>;

struct Link {
    stream: UnixStream,
    calls: Mutex<u64>,
    state: Mutex<LinkState>,
    changed: Condvar,
    notify: Mutex<Option<Notify>>,
    child: Mutex<Option<Child>>,
    harness_pid: Mutex<Option<u32>>,
    keeper: Option<ProcessIdentity>,
    seat: Option<String>,
    dir: PathBuf,
    cache: Mutex<Cache>,
    deadline: Mutex<Duration>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

impl Link {
    fn call(&self, op: &str, args: Value) -> Result<Value, CallError> {
        let deadline = *lock(&self.deadline);
        self.call_within(op, args, deadline)
    }

    fn call_within(&self, op: &str, args: Value, within: Duration) -> Result<Value, CallError> {
        let mut next = lock(&self.calls);
        {
            let state = lock(&self.state);
            if state.wedged {
                return Err(CallError::Dead);
            }
            if state.closed {
                drop(state);
                self.wait_settled();
                return Err(CallError::Dead);
            }
        }
        *next += 1;
        let id = *next;
        lock(&self.state).waiting = Some(id);
        let request = Request {
            id,
            op: op.to_string(),
            args,
        };
        if wire::send_frame(&self.stream, &request, &[]).is_err() {
            lock(&self.state).waiting = None;
            self.wait_settled();
            return Err(CallError::Dead);
        }
        let deadline = Instant::now() + within;
        let mut state = lock(&self.state);
        loop {
            if let Some((answered, _)) = &state.answer
                && *answered == id
            {
                let (_, answer) = state.answer.take().expect("answer present");
                state.waiting = None;
                return match answer {
                    Ok(value) => Ok(value),
                    Err(Fault::UnknownOp) => Err(CallError::Unknown),
                    Err(Fault::Stopped) => Err(CallError::Dead),
                    Err(fault) => Err(CallError::Fault(fault)),
                };
            }
            if state.closed {
                state.waiting = None;
                drop(state);
                self.wait_settled();
                return Err(CallError::Dead);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                state.waiting = None;
                state.wedged = true;
                return Err(CallError::Timeout(within));
            }
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|error| error.into_inner())
                .0;
        }
    }

    /// Wait, up to `within`, for the keeper to close its end.
    fn await_close(&self, within: Duration) {
        let deadline = Instant::now() + within;
        let mut state = lock(&self.state);
        while !state.closed {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return;
            }
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|error| error.into_inner())
                .0;
        }
    }

    fn wait_settled(&self) {
        let mut state = lock(&self.state);
        while !state.settled {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    fn fire(&self) {
        let notify = lock(&self.notify).clone();
        if let Some(notify) = notify {
            notify();
        }
    }

    /// The reader thread: answers to the waiting call, events to the
    /// activity callback, and the end of the connection.
    fn read(self: Arc<Self>, mut reader: FrameReader) {
        while let Ok(Some(frame)) = reader.next_frame() {
            if is_event(&frame) {
                // An event this kernel does not know changes nothing it reads.
                match serde_json::from_slice::<Reply>(&frame) {
                    Ok(Reply::Event {
                        event: Event::Activity,
                    }) => self.fire(),
                    Ok(Reply::Event {
                        event: Event::Superseded,
                    }) => {
                        // A kernel that ends the link itself took the keeper
                        // over on a fresh connection to stop it: that is not
                        // another server.
                        let mut state = lock(&self.state);
                        if !state.ending {
                            state.superseded = true;
                        }
                    }
                    _ => {}
                }
                continue;
            }
            let reply = match serde_json::from_slice::<Reply>(&frame) {
                Ok(reply) => reply,
                Err(error) => {
                    // An answer this kernel cannot read is still the answer:
                    // its caller gets an error, never a wait without end, and
                    // the keeper is driven no further.
                    let id = serde_json::from_slice::<Value>(&frame)
                        .ok()
                        .and_then(|value| value.get("id").and_then(Value::as_u64))
                        .or(lock(&self.state).waiting);
                    self.unreadable(&format!("unreadable answer: {error}"));
                    if let Some(id) = id {
                        lock(&self.state).answer = Some((
                            id,
                            Err(Fault::BadRequest {
                                message: format!("unreadable answer from the keeper: {error}"),
                            }),
                        ));
                        self.changed.notify_all();
                    }
                    continue;
                }
            };
            match reply {
                Reply::Ok { id, ok } => {
                    lock(&self.state).answer = Some((id, Ok(ok)));
                    self.changed.notify_all();
                }
                Reply::Err { id, err } => {
                    lock(&self.state).answer = Some((id, Err(err)));
                    self.changed.notify_all();
                }
                Reply::Event { .. } => {}
            }
        }
        let (asked, superseded) = {
            let mut state = lock(&self.state);
            state.closed = true;
            (state.ending, state.superseded)
        };
        self.changed.notify_all();
        if !asked && !superseded {
            if self.keeper_exits_within(HELLO_DEADLINE) {
                // The keeper died. Its harness group has no owner now, so it
                // is ended before anything starts a replacement.
                self.bury();
            } else {
                // A live keeper closed the owner's connection without being
                // asked: another kernel holds it. Stand down; never start a
                // second harness beside it.
                lock(&self.state).superseded = true;
            }
        }
        lock(&self.state).settled = true;
        self.changed.notify_all();
        self.fire();
    }

    /// The keeper answered in a shape this kernel cannot read: it is driven
    /// no further. Its harness reads as dead, so the supervisor replaces it,
    /// and `stop` reaches it over a fresh connection.
    fn unreadable(&self, why: &str) {
        let mut state = lock(&self.state);
        if !state.wedged {
            eprintln!("anyagents-keeper: {}: {why}", self.dir.display());
        }
        state.wedged = true;
        drop(state);
        self.changed.notify_all();
    }

    /// Whether the keeper process ends within `within`: by its exit status
    /// when this kernel started it, else by its recorded identity.
    fn keeper_exits_within(&self, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        loop {
            let gone = match lock(&self.child).as_mut() {
                Some(child) => !matches!(child.try_wait(), Ok(None)),
                None => match &self.keeper {
                    Some(keeper) => keeper.presence() != Presence::Running,
                    // Nothing can prove it gone.
                    None => false,
                },
            };
            if gone {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(EXIT_PROBE);
        }
    }

    /// End a keeper that does not end: by signal, as a child or by its
    /// proven identity. `false` when it can be neither ended nor proven gone.
    fn force_end_keeper(&self) -> bool {
        if let Some(child) = lock(&self.child).as_mut() {
            let _ = child.kill();
            let _ = child.wait();
            return true;
        }
        match &self.keeper {
            Some(keeper) if keeper.presence() == Presence::Running => {
                // SAFETY: SIGKILL to the process its identity proves is the
                // keeper this link spoke to.
                unsafe { libc::kill(keeper.pid as i32, libc::SIGKILL) };
                self.keeper_exits_within(STOP_DEADLINE)
            }
            Some(_) => true,
            None => false,
        }
    }

    /// End what a dead keeper left: its harness group (when its recorded
    /// identity proves it the same group), its directory, and (when this
    /// kernel started it) its process entry.
    fn bury(&self) {
        let seat = trusted_seat_env(self.seat.as_deref());
        if let Some(record) = read_record(&self.dir)
            && let Some(harness) = &record.harness_process
            && let Some(environment) = &seat
        {
            harness.end_group(environment);
        }
        if let Some(mut child) = lock(&self.child).take() {
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Ask the keeper in `dir` to stop over a fresh owner connection, for when
/// the usual one is wedged or gone.
fn stop_afresh(dir: &Path) -> bool {
    match connect(dir, true) {
        Ok(connected) => stop_unspoken(connected.stream, dir).is_ok(),
        Err(ConnectError::Protocol { stream, .. } | ConnectError::Unreadable { stream, .. }) => {
            stop_unspoken(stream, dir).is_ok()
        }
        Err(_) => false,
    }
}

/// A harness driven through its keeper. Dropping it stops the keeper, which
/// ends the harness as an in-process adapter's drop would; [`detach`] leaves
/// both running for the next kernel instead.
///
/// [`detach`]: RemoteHarness::detach
pub struct RemoteHarness {
    link: Arc<Link>,
    reader: Option<JoinHandle<()>>,
    hello: Hello,
    lock: Option<OwnedFd>,
    done: bool,
}

impl RemoteHarness {
    fn new(connected: Connected, dir: PathBuf, child: Option<Child>) -> Self {
        let Connected {
            stream,
            reader,
            hello,
            lock: process_lock,
        } = connected;
        let link = Arc::new(Link {
            stream,
            calls: Mutex::new(1),
            state: Mutex::new(LinkState::default()),
            changed: Condvar::new(),
            notify: Mutex::new(None),
            child: Mutex::new(child),
            harness_pid: Mutex::new(hello.harness_pid),
            keeper: hello.keeper.clone(),
            seat: hello.seat.clone(),
            dir,
            cache: Mutex::new(Cache {
                health: Some(hello.health.clone()),
                ..Cache::default()
            }),
            deadline: Mutex::new(CALL_DEADLINE),
        });
        let reading = Arc::clone(&link);
        let reader = std::thread::Builder::new()
            .name("anyagents-keeper-link".into())
            .spawn(move || reading.read(reader))
            .ok();
        if reader.is_none() {
            // Without a reader no answer can arrive: the link is wedged from
            // the start, and stop goes through a fresh connection.
            let mut state = lock(&link.state);
            state.wedged = true;
            state.settled = true;
        }
        Self {
            link,
            reader,
            hello,
            lock: process_lock,
            done: false,
        }
    }

    /// Read the usage totals while the link answers, so a keeper that dies
    /// after it is taken over still reports the last ones it had, never none:
    /// none would read as usage and turns that changed.
    fn prime(&self) {
        let _ = self.turns_served();
        let _ = self.served_usage();
        let _ = self.last_usage();
    }

    /// What the keeper said when this kernel connected.
    pub fn hello(&self) -> &Hello {
        &self.hello
    }

    /// The keeper's directory.
    pub fn dir(&self) -> &Path {
        &self.link.dir
    }

    /// The copy of the agent's process lock the keeper handed over, once.
    pub fn take_lock(&mut self) -> Option<OwnedFd> {
        self.lock.take()
    }

    /// Ask the keeper to keep `note` for the next kernel that takes it over.
    /// It lives exactly as long as this process generation.
    pub fn set_note(&self, note: &str) -> Result<(), String> {
        match self.link.call("note", json!({"note": note})) {
            Ok(_) => Ok(()),
            Err(CallError::Dead) => Err("keeper is gone".into()),
            Err(CallError::Timeout(within)) => Err(format!(
                "keeper did not answer within {} s",
                within.as_secs()
            )),
            Err(CallError::Unknown) => Err("keeper keeps no notes".into()),
            Err(CallError::Fault(fault)) => Err(format!("{fault:?}")),
        }
    }

    /// The keeper's connection to this kernel ended (or is ending).
    pub fn keeper_gone(&self) -> bool {
        lock(&self.link.state).closed
    }

    /// What this harness answered came from its keeper: the link is open and
    /// answering. A closed or wedged link answers with what it last knew.
    pub fn answers_live(&self) -> bool {
        let state = lock(&self.link.state);
        !state.closed && !state.wedged
    }

    /// Another kernel owns the keeper now: this one must stand down for the
    /// agent, and neither stop the keeper nor start another harness.
    pub fn superseded(&self) -> bool {
        lock(&self.link.state).superseded
    }

    /// Leave the keeper and its harness running for the next kernel. The
    /// process lock stays held by the keeper's copy.
    pub fn detach(mut self) {
        self.done = true;
        lock(&self.link.state).ending = true;
        let _ = self.link.call_within("detach", Value::Null, HELLO_DEADLINE);
        let _ = self.link.stream.shutdown(std::net::Shutdown::Both);
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        // The keeper is this kernel's child only until this kernel exits;
        // dropping the handle neither waits for nor signals it.
        drop(lock(&self.link.child).take());
    }

    /// End the harness and its keeper, and wait until both are gone.
    pub fn end(mut self) -> Result<(), String> {
        self.stop_now()
    }

    fn stop_now(&mut self) -> Result<(), String> {
        if self.done {
            return Ok(());
        }
        self.done = true;
        let (closed, wedged) = {
            let mut state = lock(&self.link.state);
            state.ending = true;
            (state.closed, state.wedged)
        };
        let mut answered = false;
        if !closed && !wedged {
            answered = self
                .link
                .call_within("stop", Value::Null, STOP_DEADLINE)
                .is_ok();
        }
        if !answered && !closed && !self.superseded() {
            // The usual connection cannot carry `stop` (it is wedged in a call
            // that never returns): a fresh owner connection can.
            answered = stop_afresh(&self.link.dir);
        }
        if answered {
            // The keeper exits right after its answer: its end of this
            // connection closing is that exit.
            self.link.await_close(STOP_DEADLINE);
        }
        let _ = self.link.stream.shutdown(std::net::Shutdown::Both);
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        if self.superseded() && !answered {
            // Another kernel owns the keeper now: let go of it, end nothing.
            drop(lock(&self.link.child).take());
            return Ok(());
        }
        if !answered {
            // It did not answer `stop`: it died, or it does not end. Either
            // way nothing ended its harness.
            if !self.link.keeper_exits_within(STOP_DEADLINE) && !self.link.force_end_keeper() {
                return Err("the keeper did not stop, and cannot be proven gone".into());
            }
            self.link.bury();
        }
        // A keeper answers `stop` only once its harness group is gone, and
        // exits right after: its end of the socket closing is that exit.
        if let Some(mut child) = lock(&self.link.child).take() {
            let _ = child.wait();
        }
        Ok(())
    }

    fn call(&self, op: &str, args: Value) -> Result<Value, CallError> {
        self.link.call(op, args)
    }

    /// An answer of a plain type: the contract default when the keeper does
    /// not know the op, `dead` when the keeper is gone or wedged.
    fn plain<T: DeserializeOwned>(&self, op: &str, args: Value, default: T, dead: T) -> T {
        match self.call(op, args) {
            Ok(value) => self.read(op, value).unwrap_or(dead),
            Err(CallError::Unknown) => default,
            Err(CallError::Dead | CallError::Timeout(_) | CallError::Fault(_)) => dead,
        }
    }

    /// An answer in the shape this kernel expects, or `None` with the link
    /// marked unreadable.
    fn read<T: DeserializeOwned>(&self, op: &str, value: Value) -> Option<T> {
        match serde_json::from_value(value) {
            Ok(answer) => Some(answer),
            Err(error) => {
                self.link
                    .unreadable(&format!("unreadable answer to {op}: {error}"));
                None
            }
        }
    }

    /// An answer that is the adapter's own `Result`.
    fn fallible<T: DeserializeOwned>(
        &self,
        op: &str,
        args: Value,
        default: T,
    ) -> Result<T, HarnessError> {
        match self.call(op, args) {
            Ok(value) => self.read(op, value).ok_or(HarnessError::Dead),
            Err(CallError::Unknown) => Ok(default),
            Err(CallError::Dead) => Err(HarnessError::Dead),
            Err(CallError::Timeout(within)) => Err(HarnessError::Failed(format!(
                "the keeper did not answer {op} within {} s",
                within.as_secs()
            ))),
            Err(CallError::Fault(Fault::Harness { error })) => Err(error),
            Err(CallError::Fault(fault)) => {
                Err(HarnessError::Failed(format!("keeper refused: {fault:?}")))
            }
        }
    }

    fn message(message: &OutboundMessage) -> Value {
        json!({ "message": message })
    }
}

impl Drop for RemoteHarness {
    fn drop(&mut self) {
        if let Err(error) = self.stop_now() {
            eprintln!("anyagents-keeper: {}: {error}", self.link.dir.display());
        }
    }
}

impl Harness for RemoteHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        match self.fallible("deliver", Self::message(message), None) {
            Ok(Some(outcome)) => Ok(outcome),
            Ok(None) => Err(HarnessError::Failed("keeper cannot deliver".into())),
            Err(error) => Err(error),
        }
    }

    /// What the adapter in the keeper proved. A keeper that is gone took
    /// its proofs with it: nothing is proven, and the mail is offered again
    /// to the next process (`docs/delivery.md`, when the keeper dies).
    fn proven(&self, message_id: &str) -> Option<Proof> {
        self.plain("proven", json!({ "id": message_id }), None, None)
    }

    /// The vendor process ends and is read to the end; the keeper stays, so
    /// `proven` still answers. A keeper that cannot do that within its
    /// deadline is ended with its harness group, so no old process shares
    /// the vendor session with the one launched next.
    fn stop(&mut self) {
        match self
            .link
            .call_within("stop_harness", Value::Null, STOP_DEADLINE)
        {
            Ok(_) => {}
            Err(_) => {
                if let Err(error) = self.stop_now() {
                    eprintln!("anyagents-keeper: {}: {error}", self.link.dir.display());
                }
            }
        }
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        self.fallible("background", Value::Null, ())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.fallible("resume", json!({ "session": session }), ())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.fallible("cancel", Value::Null, ())
    }

    /// A keeper that is gone or wedged reports its harness dead, so the
    /// supervisor replaces it (or, superseded, stands down).
    fn health(&self) -> HarnessHealth {
        if let Ok(value) = self.call("health", Value::Null)
            && let Some(health) = self.read::<HarnessHealth>("health", value)
        {
            lock(&self.link.cache).health = Some(health.clone());
            return health;
        }
        let mut health = lock(&self.link.cache)
            .health
            .clone()
            .unwrap_or_else(|| self.hello.health.clone());
        let state = lock(&self.link.state);
        if state.closed || state.wedged {
            health.process_alive = false;
            health.turn_state = TurnState::Idle;
        }
        health
    }

    fn input_ready(&self) -> bool {
        self.plain("input_ready", Value::Null, true, false)
    }

    fn ends_on_quiet(&self) -> Option<Duration> {
        self.plain::<Option<u64>>("ends_on_quiet", Value::Null, None, None)
            .map(Duration::from_millis)
    }

    fn quiet_for(&self, id: &str, quiet: Duration, patience: Duration) -> bool {
        let millis = |duration: Duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let within = patience.saturating_add(*lock(&self.link.deadline));
        match self.link.call_within(
            "quiet_for",
            json!({"id": id, "quiet_ms": millis(quiet), "patience_ms": millis(patience)}),
            within,
        ) {
            Ok(value) => self.read("quiet_for", value).unwrap_or(false),
            Err(_) => false,
        }
    }

    fn served_usage(&self) -> TurnUsage {
        let known = lock(&self.link.cache).served_usage.clone();
        let usage = self.plain("served_usage", Value::Null, TurnUsage::default(), known);
        lock(&self.link.cache).served_usage = usage.clone();
        usage
    }

    fn turns_served(&self) -> u32 {
        let known = lock(&self.link.cache).turns_served;
        let turns = self.plain("turns_served", Value::Null, 0, known);
        lock(&self.link.cache).turns_served = turns;
        turns
    }

    fn last_usage(&self) -> TurnUsage {
        let known = lock(&self.link.cache).last_usage.clone();
        match self.call("last_usage", Value::Null) {
            Ok(value) => {
                let Some(usage) = self.read::<TurnUsage>("last_usage", value) else {
                    return known;
                };
                lock(&self.link.cache).last_usage = usage.clone();
                usage
            }
            Err(CallError::Unknown) => self.served_usage(),
            Err(CallError::Dead | CallError::Timeout(_) | CallError::Fault(_)) => known,
        }
    }

    fn clear_quota(&mut self) {
        let _ = self.call("clear_quota", Value::Null);
    }

    fn exit_diagnostic(&self) -> Option<String> {
        self.plain("exit_diagnostic", Value::Null, None, None)
    }

    fn process_id(&self) -> Option<u32> {
        let known = *lock(&self.link.harness_pid);
        let pid = self.plain("process_id", Value::Null, known, None);
        if pid.is_some() {
            *lock(&self.link.harness_pid) = pid;
        }
        pid
    }

    /// The keeper reports every change the adapter's own watcher reports; the
    /// callback also runs once now, because changes while no kernel was
    /// connected were not queued.
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        *lock(&self.link.notify) = Some(Arc::clone(&notify));
        notify();
    }
}

impl Hosted for RemoteHarness {
    fn session_id(&self) -> Option<String> {
        self.plain("session_id", Value::Null, None, None)
    }

    fn session_invalidated(&self) -> bool {
        self.plain("session_invalidated", Value::Null, false, false)
    }
}

#[doc(hidden)]
pub mod testing {
    //! Raw access for the process tests: a kernel that dies in the middle of
    //! a request is exactly a connection that stops halfway.

    use super::*;

    /// Start a keeper and leave it unconnected, as a kernel that died right
    /// after starting it would.
    pub fn start_unattached(program: &Program, root: &Path, spec: &Spec) -> (PathBuf, Child) {
        private_dir(root).unwrap();
        let dir = new_instance(root, spec).unwrap();
        let Ok(child) = start_keeper(program, &dir, spec, None) else {
            panic!("the keeper did not start");
        };
        (dir, child)
    }

    /// Connect to the keeper in `dir` and return the raw socket, as its
    /// owner or as an observer.
    pub fn raw_connection(dir: &Path, owner: bool) -> UnixStream {
        match connect(dir, owner) {
            Ok(connected) => connected.stream,
            Err(_) => panic!("keeper in {} does not answer", dir.display()),
        }
    }

    /// Connect to the keeper in `dir` as its owner and return the raw socket.
    pub fn raw_owner(dir: &Path) -> UnixStream {
        raw_connection(dir, true)
    }

    impl RemoteHarness {
        /// Lose the connection the way a crashed kernel does: no detach.
        /// Returns the keeper's process handle when this kernel started it.
        pub fn crash(mut self) -> Option<Child> {
            self.done = true;
            lock(&self.link.state).ending = true;
            let _ = self.link.stream.shutdown(std::net::Shutdown::Both);
            if let Some(reader) = self.reader.take() {
                let _ = reader.join();
            }
            lock(&self.link.child).take()
        }

        /// The pid of the keeper process.
        pub fn keeper_pid(&self) -> u32 {
            self.hello.keeper_pid
        }

        /// Shorten every call's deadline, to see a wedged keeper in a test.
        pub fn set_call_deadline(&self, within: Duration) {
            *lock(&self.link.deadline) = within;
        }

        /// Send an op this kernel may not know, for protocol tests.
        pub fn raw_call(&self, op: &str) -> Result<Value, String> {
            match self.call(op, Value::Null) {
                Ok(value) => Ok(value),
                Err(CallError::Unknown) => Err("unknown_op".into()),
                Err(CallError::Dead) => Err("dead".into()),
                Err(CallError::Timeout(_)) => Err("timeout".into()),
                Err(CallError::Fault(fault)) => Err(format!("{fault:?}")),
            }
        }
    }
}
