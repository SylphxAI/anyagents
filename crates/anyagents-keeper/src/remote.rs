//! The kernel's side: start a keeper, take one over, and drive its adapter as
//! a [`Harness`].

use std::io::{BufRead as _, BufReader, Write as _};
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, Harness, HarnessError, HarnessHealth, OutboundMessage, ReconciledUnconfirmed,
    SteerItem, TurnState, TurnUsage,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::keeper::Record;
use crate::wire::{self, Event, Fault, FrameReader, Reply, Request};
use crate::{AGENT, Hello, Hosted, LOG, PROTOCOL, PROTOCOL_MIN, RECORD, SOCKET, Spec};

/// The descriptor number the process lock has in a keeper.
const KEEPER_LOCK_FD: i32 = 3;

/// The command that runs a keeper: the kernel's own binary and the arguments
/// that select keeper mode (`anyagents keeper`).
#[derive(Clone, Debug)]
pub struct Program {
    pub path: PathBuf,
    pub args: Vec<String>,
}

/// One keeper directory: `<root>/<instance>`, with the agent it serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instance {
    pub agent: String,
    pub dir: PathBuf,
}

/// Every keeper directory under `root`, newest first.
pub fn instances(root: &Path) -> Vec<Instance> {
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
    let dir = new_instance(root, &spec.agent)?;
    let started = start_keeper(program, &dir, spec, lock);
    let child = match started {
        Ok(child) => child,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(error);
        }
    };
    match connect(&dir, true) {
        Ok(link) => Ok(RemoteHarness::new(link, dir, Some(child))),
        Err(error) => {
            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
            clean_dead(&dir);
            Err(match error {
                ConnectError::Protocol { keeper, .. } => format!(
                    "keeper speaks protocol {}..={}, this kernel {PROTOCOL_MIN}..={PROTOCOL}",
                    keeper.0, keeper.1
                ),
                ConnectError::Gone => "keeper exited after it was ready".into(),
                ConnectError::Io(error) => error,
            })
        }
    }
}

fn private_dir(path: &Path) -> Result<(), String> {
    match std::fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(format!("cannot create {}: {error}", path.display())),
    }
}

/// A fresh directory named by its start time (twelve hex digits of
/// milliseconds, so a name sorts with its age), recording `agent`.
fn new_instance(root: &Path, agent: &str) -> Result<PathBuf, String> {
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
                if let Err(error) = std::fs::write(dir.join(AGENT), agent) {
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

fn start_keeper(
    program: &Program,
    dir: &Path,
    spec: &Spec,
    lock: Option<BorrowedFd<'_>>,
) -> Result<Child, String> {
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
    let lock = lock.map(|fd| fd.as_raw_fd());
    if lock.is_some() {
        command.arg("--lock-fd").arg(KEEPER_LOCK_FD.to_string());
    }
    // SAFETY: only async-signal-safe calls between fork and exec: setsid,
    // dup2 and fcntl on descriptors the parent keeps open across the spawn.
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if let Some(fd) = lock {
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
    let launch = serde_json::to_vec(spec).map_err(|error| error.to_string());
    let written = launch.and_then(|bytes| {
        let mut stdin = child.stdin.take().ok_or("keeper stdin missing")?;
        stdin
            .write_all(&bytes)
            .map_err(|error| format!("cannot hand the launch to the keeper: {error}"))
    });
    let mut line = String::new();
    let read = match (&written, child.stdout.take()) {
        (Ok(()), Some(stdout)) => BufReader::new(stdout)
            .read_line(&mut line)
            .map_err(|error| error.to_string()),
        (Err(error), _) => Err(error.clone()),
        (Ok(()), None) => Err("keeper stdout missing".into()),
    };
    let failure = match read {
        Ok(_) if line.trim_end() == "ready" => return Ok(child),
        Ok(0) => {
            let tail = log_tail(dir);
            if tail.is_empty() {
                "keeper exited before it was ready".to_string()
            } else {
                format!("keeper exited before it was ready: {tail}")
            }
        }
        Ok(_) => line
            .trim_end()
            .strip_prefix("error ")
            .unwrap_or(line.trim_end())
            .to_string(),
        Err(error) => error,
    };
    let _ = child.kill();
    let _ = child.wait();
    Err(failure)
}

fn log_tail(dir: &Path) -> String {
    let text = std::fs::read_to_string(dir.join(LOG)).unwrap_or_default();
    let tail: Vec<&str> = text.lines().rev().take(3).collect();
    tail.into_iter().rev().collect::<Vec<_>>().join(" | ")
}

/// Take over the keeper a previous kernel left for `agent`: the newest one
/// that answers in a protocol this kernel speaks. Older live keepers of the
/// same agent (a crash in the middle of a replacement) are stopped, a keeper
/// that speaks no shared protocol is stopped, and a directory whose keeper is
/// gone is cleared after the harness group it recorded is ended.
pub fn attach(root: &Path, agent: &str) -> Result<Option<RemoteHarness>, String> {
    let mut found: Option<RemoteHarness> = None;
    for dir in agent_instances(root, agent) {
        match connect(&dir, found.is_none()) {
            Ok(link) if found.is_none() => found = Some(RemoteHarness::new(link, dir, None)),
            Ok(link) => RemoteHarness::new(link, dir, None).stop(),
            Err(ConnectError::Gone) => clean_dead(&dir),
            Err(ConnectError::Protocol { stream, .. }) => stop_unspoken(stream),
            Err(ConnectError::Io(error)) => {
                return Err(format!("keeper {}: {error}", dir.display()));
            }
        }
    }
    Ok(found)
}

/// Stop every keeper of `agent`, ending its harness.
pub fn stop_agent(root: &Path, agent: &str) {
    for dir in agent_instances(root, agent) {
        match connect(&dir, false) {
            Ok(link) => RemoteHarness::new(link, dir, None).stop(),
            Err(ConnectError::Gone) => clean_dead(&dir),
            Err(ConnectError::Protocol { stream, .. }) => stop_unspoken(stream),
            Err(ConnectError::Io(_)) => {}
        }
    }
}

/// What the keeper in `dir` says about itself, without taking it over.
pub fn inspect(dir: &Path) -> Result<Hello, String> {
    match connect(dir, false) {
        Ok(link) => {
            let hello = link.hello.clone();
            let _ = link.stream.shutdown(std::net::Shutdown::Both);
            Ok(hello)
        }
        Err(ConnectError::Gone) => Err("gone".into()),
        Err(ConnectError::Protocol { keeper, .. }) => {
            Err(format!("speaks protocol {}..={} only", keeper.0, keeper.1))
        }
        Err(ConnectError::Io(error)) => Err(error),
    }
}

/// A keeper that cannot answer is gone: end the harness group it recorded,
/// which nothing can reach any more, and clear its directory.
fn clean_dead(dir: &Path) {
    if let Some(record) = read_record(dir)
        && let Some(pid) = record.harness_pid
    {
        anyagents_harness::terminate_group(pid, &seat_env(record.seat.as_deref()));
    }
    let _ = std::fs::remove_dir_all(dir);
}

fn read_record(dir: &Path) -> Option<Record> {
    let bytes = std::fs::read(dir.join(RECORD)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn seat_env(seat: Option<&str>) -> Vec<(String, String)> {
    seat.map(|seat| {
        vec![(
            anyagents_harness::SEAT_LAUNCH_ENV.to_string(),
            seat.to_string(),
        )]
    })
    .unwrap_or_default()
}

/// `stop` is frozen in every protocol version, so a keeper this kernel cannot
/// otherwise speak to can still be ended. It exits after answering.
fn stop_unspoken(stream: UnixStream) {
    let request = Request {
        id: u64::MAX,
        op: "stop".into(),
        args: Value::Null,
    };
    if wire::send_frame(&stream, &request, &[]).is_ok() {
        let mut reader = FrameReader::new(&stream);
        while let Ok(Some(_)) = reader.next_frame() {}
    }
}

enum ConnectError {
    /// Nothing listens: the keeper is gone.
    Gone,
    /// It answers, but in no protocol this kernel speaks.
    Protocol {
        keeper: (u32, u32),
        stream: UnixStream,
    },
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
            return Err(ConnectError::Gone);
        }
        Err(error) => return Err(ConnectError::Io(error.to_string())),
    };
    wire::quiet_pipe(&stream).map_err(|error| ConnectError::Io(error.to_string()))?;
    let hello = Request {
        id: 1,
        op: "hello".into(),
        args: json!({"protocol": [PROTOCOL_MIN, PROTOCOL], "owner": owner}),
    };
    if wire::send_frame(&stream, &hello, &[]).is_err() {
        return Err(ConnectError::Gone);
    }
    let mut reader = FrameReader::new(&stream);
    // An owner may be told of activity before its hello is answered.
    let reply = loop {
        let frame = match reader.next_frame() {
            Ok(Some(frame)) => frame,
            Ok(None) => return Err(ConnectError::Gone),
            Err(error) => return Err(ConnectError::Io(error.to_string())),
        };
        let reply: Reply =
            serde_json::from_slice(&frame).map_err(|error| ConnectError::Io(error.to_string()))?;
        if !matches!(reply, Reply::Event { .. }) {
            break reply;
        }
    };
    match reply {
        Reply::Ok { ok, .. } => {
            let hello: Hello =
                serde_json::from_value(ok).map_err(|error| ConnectError::Io(error.to_string()))?;
            let lock = if hello.lock {
                reader.take_fds().into_iter().next()
            } else {
                None
            };
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
        Reply::Err {
            err: Fault::Stopped,
            ..
        } => Err(ConnectError::Gone),
        Reply::Err { err, .. } => Err(ConnectError::Io(format!("hello refused: {err:?}"))),
        Reply::Event { .. } => unreachable!("events are skipped above"),
    }
}

/// Why a call has no answer.
enum CallError {
    /// The keeper is gone (or going): the harness is dead to this kernel.
    Dead,
    /// The keeper does not know the op; use the contract's default.
    Unknown,
    Fault(Fault),
}

#[derive(Default)]
struct LinkState {
    answer: Option<(u64, Result<Value, Fault>)>,
    closed: bool,
    /// The reader has finished what an unasked close requires.
    settled: bool,
    superseded: bool,
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
    seat: Option<String>,
    dir: PathBuf,
    cache: Mutex<Cache>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

impl Link {
    fn call(&self, op: &str, args: Value) -> Result<Value, CallError> {
        let mut next = lock(&self.calls);
        if lock(&self.state).closed {
            self.wait_settled();
            return Err(CallError::Dead);
        }
        *next += 1;
        let id = *next;
        let request = Request {
            id,
            op: op.to_string(),
            args,
        };
        if wire::send_frame(&self.stream, &request, &[]).is_err() {
            self.wait_settled();
            return Err(CallError::Dead);
        }
        let mut state = lock(&self.state);
        loop {
            if let Some((answered, _)) = &state.answer
                && *answered == id
            {
                let (_, answer) = state.answer.take().expect("answer present");
                return match answer {
                    Ok(value) => Ok(value),
                    Err(Fault::UnknownOp) => Err(CallError::Unknown),
                    Err(Fault::Stopped) => Err(CallError::Dead),
                    Err(fault) => Err(CallError::Fault(fault)),
                };
            }
            if state.closed {
                drop(state);
                self.wait_settled();
                return Err(CallError::Dead);
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
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
            let Ok(reply) = serde_json::from_slice::<Reply>(&frame) else {
                continue;
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
                Reply::Event {
                    event: Event::Activity,
                } => self.fire(),
                Reply::Event {
                    event: Event::Superseded,
                } => lock(&self.state).superseded = true,
            }
        }
        let (asked, superseded) = {
            let mut state = lock(&self.state);
            state.closed = true;
            (state.ending, state.superseded)
        };
        self.changed.notify_all();
        if !asked && !superseded {
            // Nobody asked the keeper to close this connection and no newer
            // kernel took it over: the keeper died. Its harness group has no
            // owner now, so it is ended before anything starts a replacement.
            self.bury();
        }
        lock(&self.state).settled = true;
        self.changed.notify_all();
        self.fire();
    }

    /// End what a dead keeper left: its harness group, its directory, and
    /// (when this kernel started it) its process entry.
    fn bury(&self) {
        let recorded = read_record(&self.dir).and_then(|record| record.harness_pid);
        let pid = recorded.or(*lock(&self.harness_pid));
        if let Some(pid) = pid {
            anyagents_harness::terminate_group(pid, &seat_env(self.seat.as_deref()));
        }
        if let Some(mut child) = lock(&self.child).take() {
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
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
            seat: hello.seat.clone(),
            dir,
            cache: Mutex::new(Cache {
                health: Some(hello.health.clone()),
                ..Cache::default()
            }),
        });
        let reading = Arc::clone(&link);
        let reader = std::thread::Builder::new()
            .name("anyagents-keeper-link".into())
            .spawn(move || reading.read(reader))
            .ok();
        if reader.is_none() {
            // Without a reader no answer arrives: the link is unusable, and
            // the keeper is left as it is for the next attach.
            let mut state = lock(&link.state);
            state.closed = true;
            state.settled = true;
            state.ending = true;
        }
        Self {
            link,
            reader,
            hello,
            lock: process_lock,
            done: false,
        }
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
            Err(CallError::Unknown) => Err("keeper keeps no notes".into()),
            Err(CallError::Fault(fault)) => Err(format!("{fault:?}")),
        }
    }

    /// The keeper ended without being asked (or is ending).
    pub fn keeper_gone(&self) -> bool {
        lock(&self.link.state).closed
    }

    /// Leave the keeper and its harness running for the next kernel. The
    /// process lock stays held by the keeper's copy.
    pub fn detach(mut self) {
        self.done = true;
        lock(&self.link.state).ending = true;
        let _ = self.link.call("detach", Value::Null);
        let _ = self.link.stream.shutdown(std::net::Shutdown::Both);
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        // The keeper is this kernel's child only until this kernel exits;
        // dropping the handle neither waits for nor signals it.
        drop(lock(&self.link.child).take());
    }

    /// End the harness and its keeper, and wait until both are gone.
    pub fn stop(mut self) {
        self.stop_now();
    }

    fn stop_now(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        let already_closed = {
            let mut state = lock(&self.link.state);
            state.ending = true;
            state.closed
        };
        let answered = !already_closed && self.link.call("stop", Value::Null).is_ok();
        let _ = self.link.stream.shutdown(std::net::Shutdown::Both);
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        if lock(&self.link.state).superseded {
            // A newer kernel owns the keeper now: let go of it, end nothing.
            drop(lock(&self.link.child).take());
            return;
        }
        if !answered && !already_closed {
            // It died while being stopped: nothing ended its harness.
            self.link.bury();
        }
        if let Some(mut child) = lock(&self.link.child).take() {
            let _ = child.wait();
        }
    }

    fn call(&self, op: &str, args: Value) -> Result<Value, CallError> {
        self.link.call(op, args)
    }

    /// An answer of a plain type: the contract default when the keeper does
    /// not know the op, `dead` when the keeper is gone.
    fn plain<T: DeserializeOwned>(&self, op: &str, args: Value, default: T, dead: T) -> T {
        match self.call(op, args) {
            Ok(value) => serde_json::from_value(value).unwrap_or(default),
            Err(CallError::Unknown) => default,
            Err(CallError::Dead | CallError::Fault(_)) => dead,
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
            Ok(value) => serde_json::from_value(value)
                .map_err(|error| HarnessError::Failed(format!("keeper answer: {error}"))),
            Err(CallError::Unknown) => Ok(default),
            Err(CallError::Dead) => Err(HarnessError::Dead),
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
        self.stop_now();
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

    fn background(&mut self) -> Result<(), HarnessError> {
        self.fallible("background", Value::Null, ())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.fallible("resume", json!({ "session": session }), ())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.fallible("cancel", Value::Null, ())
    }

    fn health(&self) -> HarnessHealth {
        match self.call("health", Value::Null) {
            Ok(value) => {
                if let Ok(health) = serde_json::from_value::<HarnessHealth>(value) {
                    lock(&self.link.cache).health = Some(health.clone());
                    return health;
                }
            }
            Err(CallError::Unknown | CallError::Fault(_)) => {}
            Err(CallError::Dead) => {}
        }
        let mut health = lock(&self.link.cache)
            .health
            .clone()
            .unwrap_or_else(|| self.hello.health.clone());
        if self.keeper_gone() {
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
        self.plain(
            "quiet_for",
            json!({"id": id, "quiet_ms": millis(quiet), "patience_ms": millis(patience)}),
            false,
            false,
        )
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
                let usage: TurnUsage = serde_json::from_value(value).unwrap_or_default();
                lock(&self.link.cache).last_usage = usage.clone();
                usage
            }
            Err(CallError::Unknown) => self.served_usage(),
            Err(CallError::Dead | CallError::Fault(_)) => known,
        }
    }

    fn clear_quota(&mut self) {
        let _ = self.call("clear_quota", Value::Null);
    }

    fn take_reclaimed_steers(&mut self) -> Vec<String> {
        self.plain("take_reclaimed_steers", Value::Null, Vec::new(), Vec::new())
    }

    fn has_unconfirmed_hold(&self) -> bool {
        self.plain("has_unconfirmed_hold", Value::Null, false, false)
    }

    fn has_unconfirmed_idle_hold(&self) -> bool {
        self.plain("has_unconfirmed_idle_hold", Value::Null, false, false)
    }

    fn peek_unconfirmed_request_ids(&self) -> Vec<String> {
        self.plain(
            "peek_unconfirmed_request_ids",
            Value::Null,
            Vec::new(),
            Vec::new(),
        )
    }

    fn reconcile_unconfirmed_for_recovery(
        &mut self,
    ) -> Result<Option<ReconciledUnconfirmed>, HarnessError> {
        self.fallible("reconcile_unconfirmed_for_recovery", Value::Null, None)
    }

    fn poll_steer(&mut self) -> Result<Option<SteerItem>, HarnessError> {
        self.fallible("poll_steer", Value::Null, None)
    }

    fn inject(&mut self, message: &OutboundMessage) -> Result<bool, HarnessError> {
        self.fallible("inject", Self::message(message), false)
    }

    fn release_steer(&mut self, keep: &str) {
        let _ = self.call("release_steer", json!({ "keep": keep }));
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

    fn state_dir(&self) -> Option<PathBuf> {
        self.plain("state_dir", Value::Null, None, None)
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
        let dir = new_instance(root, &spec.agent).unwrap();
        let child = start_keeper(program, &dir, spec, None).unwrap();
        (dir, child)
    }

    /// Connect to the keeper in `dir` as its owner and return the raw socket.
    pub fn raw_owner(dir: &Path) -> UnixStream {
        match connect(dir, true) {
            Ok(connected) => connected.stream,
            Err(_) => panic!("keeper in {} does not answer", dir.display()),
        }
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

        /// Send an op this kernel may not know, for protocol tests.
        pub fn raw_call(&self, op: &str) -> Result<Value, String> {
            match self.call(op, Value::Null) {
                Ok(value) => Ok(value),
                Err(CallError::Unknown) => Err("unknown_op".into()),
                Err(CallError::Dead) => Err("dead".into()),
                Err(CallError::Fault(fault)) => Err(format!("{fault:?}")),
            }
        }
    }
}
