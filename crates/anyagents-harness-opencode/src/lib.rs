//! OpenCode adapter.
//!
//! `opencode serve` is an HTTP server, not a line protocol on stdin. This
//! adapter starts one private service per agent and drives it with `opencode
//! api`, one process per request, as the desk's own worker does. It speaks the
//! OpenCode 2.x API:
//!
//! - the service must be 2.x. A 1.x `serve` announces `opencode server
//!   listening on …` and no password, and has neither `opencode api` nor `GET
//!   /api/info`: the start fails at once, saying so. A service whose `GET
//!   /api/info` answers 404 (OpenCode's own sign of an incompatible service) or
//!   names a version below 2 fails the start the same way, before any session
//!   is touched;
//! - the session is a fixed id (`--session`), resumed across restarts;
//! - a mailbox line is a prompt: `POST /api/session/{id}/prompt` with the text
//!   `anyagents-message:<id>` on its first line, `delivery: "steer"`, and the
//!   mailbox id as the prompt's own message id (`msg_…`). The service admits
//!   it to the session's durable inbox, and its turn takes every pending
//!   steer at its next step, in admission order, or a new turn starts with
//!   it. Given an id it already holds, as an inbox row or as a message of the
//!   session, the service answers with that record and admits nothing;
//! - the admission is the proof delivery rests on (`MidTurn`): the service
//!   reports consumption only on its event stream, which a one-shot `opencode
//!   api` call does not read. Because the id travels with the prompt, writing
//!   the same line again, from this process or a later one, never applies it
//!   twice, and a record that predates the request and is already in the
//!   conversation is answered as a duplicate;
//! - prompts are written one at a time, in mailbox order, each on a thread of
//!   its own: a slow service holds the mailbox, never the host, and no prompt
//!   call has a timeout. What the service answers decides the line:
//!   - a 400 (a malformed request) or a 409 (the id belongs to another durable
//!     record) refuses it for good: it is `Rejected`, and answered so again
//!     whenever it is offered again, without another call;
//!   - a 503 `service_starting` or `service_stopping` is the service's
//!     readiness gate, which answers before any route and admits nothing: the
//!     line waits out the one-second `retry-after` the service asks for, goes
//!     back to the mailbox, and the host is woken to offer it again;
//!   - an outcome that may or may not have admitted it (another server error,
//!     a lost connection) is settled by writing the same id again; a second
//!     such outcome is a fault of the service;
//!   - a 401 or 403, or a 503 `service_failed`, is a fault of the process;
//! - a turn ends when `POST /api/experimental/session/{id}/wait`, asked after
//!   the newest admission, answers: the service's own idle signal;
//! - before the host replaces the process (`stop`), a prompt call that is out
//!   is waited for, up to a minute, so what it proved is known; one still
//!   unanswered then is unknown, and the next process writes its id again.
//!   The proofs outlive the process.
//!
//! The mailbox id on the prompt's first line also makes the agent's own `send`
//! (its MCP tool) the reply path, recorded against the same agent. Nothing
//! here reads a credential: the service's address and password come from the
//! private service this adapter starts for this agent.

mod port;

use std::collections::HashMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState, TurnUsage, compose_message,
};
use serde_json::{Value, json};

/// How long a call off the delivery path may take: startup, usage, answers and
/// cancel; and how long `stop` lets a prompt call that is out answer before
/// the service goes. A prompt, and the wait for a turn to end, have no limit.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the adapter waits for a started service to announce itself.
///
/// This wait runs inside `agents.create`/server start, so it is a hard cap: a
/// service that stays silent is reported as not started, and the server binds
/// its socket anyway rather than waiting on a vendor.
const START_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the adapter waits for the started service's API to answer.
const READY_TIMEOUT: Duration = Duration::from_secs(20);

/// How often an open turn is checked when the service cannot wait for it.
const TURN_POLL: Duration = Duration::from_secs(1);

/// The `retry-after` OpenCode's readiness gate sends with a 503
/// `service_starting` or `service_stopping` (one second, always). `opencode
/// api` prints no header, so the service's fixed value is kept here.
const RETRY_AFTER: Duration = Duration::from_secs(1);

/// The prefix a 1.x `opencode serve` announces its address with. A 2.x
/// service prints `server listening on <url>` and then `server password …`.
const LEGACY_ANNOUNCEMENT: &str = "opencode server listening on ";

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::MidTurn,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed("durable inbox admission, by id"),
    peer: None,
};

/// One long-lived OpenCode service and the session it serves.
pub struct OpenCodeHarness {
    vendor: Arc<Vendor>,
    child: Option<Child>,
    port: Option<port::PortLease>,
}

/// What a started service printed before it was waited for no longer.
enum Announcement {
    /// Its private address and password: a 2.x service.
    Ready,
    /// A 1.x service's address line, with no password.
    Legacy(String),
    /// Nothing usable before the start deadline, or it exited.
    Silent,
}

/// What every call to the service needs, shared with the threads that carry a
/// prompt or wait for a turn to end.
struct Vendor {
    program: PathBuf,
    /// The agent's config directory; its session id is kept here.
    config_dir: Option<PathBuf>,
    work_dir: Option<PathBuf>,
    environment: Vec<(String, String)>,
    state: Mutex<State>,
    /// Signalled whenever `State::epoch` moves or the process goes.
    changed: Condvar,
}

#[derive(Clone, Debug)]
struct PriorPrompt {
    id: String,
    sender: Option<String>,
    text: String,
}

/// What became of one mailbox line this process wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Prompt {
    /// Its prompt call is out.
    Sending,
    /// The session's inbox holds it.
    Admitted,
    /// An earlier process wrote it and the session already consumed it.
    Applied,
    /// The service's readiness gate turned it away and admitted nothing; it
    /// waits for its next offer.
    Refused,
    /// The service refused it for good and admitted nothing.
    Rejected(String),
    /// The call ended without saying whether the service admitted it.
    Unknown,
    /// The service failed in a way only a new process can clear.
    Fault(String),
}

#[derive(Default)]
struct State {
    session: Option<String>,
    alive: bool,
    /// The harness is being dropped: background calls stop acting.
    closing: bool,
    /// Which service start this is, so the output of an earlier one that
    /// closes late cannot mark this one gone.
    launch: u64,
    launch_ended: bool,
    /// Moves on every change a host waits for: an answered prompt, a turn
    /// that ended, the process gone.
    epoch: u64,
    /// The service this adapter started, and the password it printed. A
    /// separate `opencode api` must be pointed at that address, or it talks to
    /// whatever service the machine happens to run.
    service: Option<String>,
    password: Option<String>,
    /// `opencode <version>`, as `GET /api/info` reports it.
    peer: Option<String>,
    /// What became of each line this process wrote, by mailbox id.
    prompts: HashMap<String, Prompt>,
    /// The line whose prompt call is out. One at a time: the service admits
    /// them in the order the calls arrive.
    sending: Option<String>,
    /// Ids the service admitted from this process, in order.
    written: Vec<String>,
    /// Prior messages accepted in this adapter session, used to restore
    /// conversation context if the OpenCode session later returns HTTP 404.
    history: Vec<PriorPrompt>,
    /// Initial `<topology>` snapshot seen on this adapter instance so a
    /// replacement session created after a 404 retains topology awareness.
    initial_topology: Option<String>,
    /// Stale session ID replaced after an HTTP 404, pending context
    /// restoration on the first prompt delivered to the replacement session.
    recovered_from_session: Option<String>,
    turn_open: bool,
    /// Lines admitted so far. A turn ends only when the service went idle
    /// after the newest of them.
    admissions: u64,
    /// A thread is waiting for the open turn to end.
    watching: bool,
    turns_completed: u32,
    /// `turns_completed` when the session's totals were last read.
    usage_read_at: Option<u32>,
    cached_usage: TurnUsage,
}

impl OpenCodeHarness {
    /// Start the service and resume `session_id`.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        Self::start_in(program, session_id, environment, None)
    }

    /// Start the service with its working directory set to `work_dir`.
    pub fn start_in(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        Self::start_with(program, session_id, environment, work_dir, None)
    }

    fn start_with(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
        config_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let mut environment: Vec<(String, String)> = environment
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        if let Some(dir) = config_dir {
            // Override inherited host roots, including OpenCode's explicit
            // config override, before any vendor command can discover a service.
            for (key, value) in [
                ("XDG_CONFIG_HOME", dir.to_path_buf()),
                ("OPENCODE_CONFIG_DIR", dir.join("opencode")),
                ("XDG_DATA_HOME", dir.join("data-home")),
                ("XDG_STATE_HOME", dir.join("state-home")),
            ] {
                environment.retain(|(name, _)| name != key);
                environment.push((key.into(), value.display().to_string()));
            }
        }
        let mut harness = Self {
            vendor: Arc::new(Vendor {
                program: program.into(),
                config_dir: config_dir.map(Path::to_path_buf),
                work_dir: work_dir.map(Path::to_path_buf),
                environment,
                state: Mutex::new(State {
                    session: session_id.map(str::to_string),
                    ..State::default()
                }),
                changed: Condvar::new(),
            }),
            child: None,
            port: None,
        };
        harness.begin()?;
        Ok(harness)
    }

    /// Start with this agent's config directory, where its session id is kept.
    pub fn start_in_config(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
        config_dir: &Path,
    ) -> Result<Self, HarnessError> {
        Self::start_with(program, session_id, environment, work_dir, Some(config_dir))
    }

    /// Ids the service admitted from this process, in the order they were written.
    pub fn written_ids(&self) -> Vec<String> {
        self.vendor.lock().written.clone()
    }
    pub fn session_id(&self) -> Option<String> {
        self.vendor.lock().session.clone()
    }
    /// Whether the service has admitted `id` from this process.
    pub fn seen(&self, id: &str) -> bool {
        self.vendor
            .lock()
            .written
            .iter()
            .any(|written| written == id)
    }
    pub fn wait_seen(&self, id: &str, timeout: Duration) -> bool {
        let state = self.vendor.lock();
        let (state, _) = self
            .vendor
            .changed
            .wait_timeout_while(state, timeout, |state| {
                !state.written.iter().any(|written| written == id)
            })
            .unwrap_or_else(|err| err.into_inner());
        state.written.iter().any(|written| written == id)
    }
    /// Whether the session has answered `id`, as read from the session.
    pub fn wait_answered(&self, id: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.answer(id).is_some() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    /// The answer the session holds for `id`, as read from the session now.
    pub fn answer(&self, id: &str) -> Option<String> {
        self.answer_in_session(id)
    }

    /// The answer the session holds for this mailbox id: the assistant message
    /// that follows the user message carrying `anyagents-message:<id>`.
    ///
    /// The session lists messages newest first, so the answer is the first
    /// assistant message *before* that user message in that order. Reasoning
    /// parts are never the answer.
    pub fn answer_in_session(&self, id: &str) -> Option<String> {
        let session = self.vendor.lock().session.clone()?;
        let listing = self
            .vendor
            .call(
                "GET",
                &format!("/api/session/{session}/message"),
                None,
                Some(CALL_TIMEOUT),
            )
            .ok()?;
        answer_of(listing.get("data")?.as_array()?, id)
    }

    /// Start the service, wait until it answers, and resume the session.
    fn begin(&mut self) -> Result<(), HarnessError> {
        // Reap any prior service or PPID=1 orphan for this config before
        // acquiring the port so a daemon restart reclaims its stable port and
        // never leaves duplicate OpenCode servers sharing one session store.
        self.reap_prior_service();
        // Keep a stable private port across restarts; never use the managed
        // service default or allow `api` to discover a different service.
        for attempt in 0..3 {
            let previous = self.port.take().map(|lease| lease.port);
            self.port = Some(port::PortLease::acquire(
                self.vendor.config_dir.as_deref(),
                previous,
            )?);
            let port = self.port.as_ref().expect("allocated port").port;
            let (announced, collision) = self.launch_service(port)?;
            match announced {
                Announcement::Ready => return self.resume_session(),
                Announcement::Legacy(line) => {
                    self.stop_child();
                    return Err(HarnessError::Failed(format!(
                        "OpenCode 1.x service ({line:?}, no password): this adapter speaks the OpenCode 2.x API (`opencode api`, the session inbox); install OpenCode 2.x. The session is untouched"
                    )));
                }
                Announcement::Silent => {}
            }
            self.stop_child();
            // The holder may already have released the port. Preserve the
            // child's bind result rather than inferring it from a later probe.
            let reported_collision = collision
                .recv_timeout(Duration::from_secs(1))
                .unwrap_or(false);
            // An unrelated process can win the bind after socket handoff.
            // Only retry a witnessed collision, never a generic startup error.
            if attempt == 2
                || (!reported_collision
                    && !matches!(std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)),
                        Err(err) if err.kind() == std::io::ErrorKind::AddrInUse))
            {
                return Err(HarnessError::Failed(
                    "the OpenCode service did not announce its private endpoint; the session is untouched".into(),
                ));
            }
        }
        unreachable!("bounded startup attempts")
    }

    fn launch_service(
        &mut self,
        port: u16,
    ) -> Result<(Announcement, std::sync::mpsc::Receiver<bool>), HarnessError> {
        let vendor = Arc::clone(&self.vendor);
        let mut command = anyagents_harness::spawn_command(&vendor.program, &vendor.environment)?;
        command
            .args([
                "serve",
                "--hostname",
                "127.0.0.1",
                "--port",
                &port.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &vendor.work_dir {
            command.current_dir(dir);
        }
        // Inherited credentials suppress the password announcement. Generate a
        // fresh child credential instead; only API children receive it below.
        command
            .env_remove("OPENCODE_PASSWORD")
            .env_remove("OPENCODE_SERVER_PASSWORD");
        self.port.as_mut().expect("allocated port").handoff();
        let launch = {
            let mut state = vendor.lock();
            state.launch += 1;
            state.launch_ended = false;
            state.service = None;
            state.password = None;
            state.launch
        };
        let mut child = command
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        self.write_service_pid(child.id());
        let collision = port::watch_collision(child.stderr.take().expect("piped stderr"));
        let stdout = child.stdout.take().expect("piped stdout");
        self.child = Some(child);
        use std::io::{BufRead, BufReader};
        use std::sync::mpsc;
        // A service that never announces itself must not wedge the adapter,
        // so the lines are read on their own thread and waited for with a
        // deadline. The same thread then drains the output, so the child never
        // blocks on a full pipe, and says when it closes: the service is gone.
        let (lines, heard) = mpsc::channel();
        let reader_vendor = Arc::clone(&vendor);
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if lines.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
            let mut buffer = [0u8; 4096];
            while reader.read(&mut buffer).unwrap_or(0) > 0 {}
            let mut state = reader_vendor.lock();
            if state.launch == launch {
                state.launch_ended = true;
                state.alive = false;
                state.epoch += 1;
            }
            drop(state);
            reader_vendor.changed.notify_all();
        });
        let deadline = Instant::now() + START_TIMEOUT;
        let mut legacy = None;
        while Instant::now() < deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = match heard.recv_timeout(left.min(Duration::from_millis(500))) {
                Ok(line) => line,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
            };
            let line = line.trim();
            // A 1.x service says who it is in its first line; nothing it
            // prints later makes it one this adapter can drive.
            if line.starts_with(LEGACY_ANNOUNCEMENT) {
                legacy = Some(line.to_string());
                break;
            }
            let mut state = vendor.lock();
            if let Some(rest) = line.strip_prefix("server listening on ") {
                let endpoint = rest.trim_end_matches('/');
                if endpoint == format!("http://127.0.0.1:{port}") {
                    state.service = Some(endpoint.to_string());
                }
            }
            if let Some(password) = line.strip_prefix("server password ") {
                state.password = Some(password.trim().to_string());
            }
            if state.service.is_some() && state.password.is_some() {
                break;
            }
        }
        let mut state = vendor.lock();
        let ready = legacy.is_none() && state.service.is_some() && state.password.is_some();
        state.alive = ready && !state.launch_ended;
        let announced = match legacy {
            Some(line) => Announcement::Legacy(line),
            None if ready => Announcement::Ready,
            None => Announcement::Silent,
        };
        Ok((announced, collision))
    }

    fn resume_session(&mut self) -> Result<(), HarnessError> {
        let vendor = Arc::clone(&self.vendor);
        let ready = Instant::now() + READY_TIMEOUT;
        let mut up = false;
        while Instant::now() < ready {
            if vendor
                .call("GET", "/api/session", None, Some(CALL_TIMEOUT))
                .is_ok()
            {
                up = true;
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }
        if !up {
            return Err(HarnessError::Failed(
                "the OpenCode service did not answer; the session is untouched".into(),
            ));
        }
        // Who answers, as the service names itself, before any session is
        // looked up or created: an adapter that speaks 2.x must never drive
        // an older service.
        let peer = match vendor.call("GET", "/api/info", None, Some(CALL_TIMEOUT)) {
            Ok(info) => info
                .get("version")
                .and_then(Value::as_str)
                .map(str::to_string),
            Err(err) if err.status == Some(404) => {
                return Err(HarnessError::Failed(
                    "OpenCode service without GET /api/info (HTTP 404): an OpenCode 1.x service; this adapter speaks the OpenCode 2.x API. The session is untouched".into(),
                ));
            }
            Err(err) => {
                return Err(HarnessError::Failed(format!(
                    "cannot learn the OpenCode service's version: {err}; the session is untouched"
                )));
            }
        };
        if let Some(version) = &peer
            && major_version(version).is_none_or(|major| major < 2)
        {
            return Err(HarnessError::Failed(format!(
                "OpenCode {version} speaks the OpenCode 1.x API; this adapter speaks 2.x. The session is untouched"
            )));
        }
        vendor.lock().peer = peer.map(|version| format!("opencode {version}"));
        // The stored session is resumed. A new one is created only when it is
        // truly gone from this agent's own store, and that is said out loud so
        // a lost conversation is never silent.
        let wanted = vendor.lock().session.clone();
        // A stored session is resumed unless the service clearly says it does
        // not exist. A transient failure is retried, never read as "gone": a
        // new session there would strand the answers already in the old one.
        let usable = match &wanted {
            Some(session) => {
                let mut state = Missing::Unknown;
                for _ in 0..5 {
                    match vendor.call(
                        "GET",
                        &format!("/api/session/{session}"),
                        None,
                        Some(CALL_TIMEOUT),
                    ) {
                        Ok(_) => {
                            state = Missing::Present;
                            break;
                        }
                        Err(err) if err.status == Some(404) => {
                            state = Missing::Gone;
                            break;
                        }
                        Err(_) => {
                            state = Missing::Unknown;
                            thread::sleep(Duration::from_millis(300));
                        }
                    }
                }
                match state {
                    Missing::Present => true,
                    Missing::Gone => false,
                    Missing::Unknown => {
                        return Err(HarnessError::Failed(
                            "OpenCode session availability is unknown; refusing a new conversation"
                                .into(),
                        ));
                    }
                }
            }
            None => false,
        };
        if usable {
            vendor.remember_session()?;
        } else {
            vendor.recreate_session(wanted.as_deref(), Some(CALL_TIMEOUT))?;
        }
        // OpenCode boots location services and MCP servers lazily on the first
        // directory-scoped call (`GET /api/mcp` or `POST /api/session/<id>/prompt`).
        // Poll `GET /api/mcp` briefly so the `anyagents` MCP server is connected
        // before the very first turn builds its tool catalog.
        for _ in 0..15 {
            let Ok(mcp) = vendor.call("GET", "/api/mcp", None, Some(CALL_TIMEOUT)) else {
                break;
            };
            let Some(items) = mcp.get("data").and_then(Value::as_array) else {
                break;
            };
            if items.iter().any(|item| {
                item.get("name").and_then(Value::as_str) == Some("anyagents")
                    && item.pointer("/status/status").and_then(Value::as_str) == Some("connected")
            }) {
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }
        // The inbox is durable: a service that picked up where an earlier one
        // stopped is running a turn already, and its end is waited for too.
        let session = vendor.lock().session.clone().unwrap_or_default();
        if let Ok(active) = vendor.call("GET", "/api/session/active", None, Some(CALL_TIMEOUT))
            && active
                .get("data")
                .and_then(|data| data.get(&session))
                .is_some()
        {
            let mut state = vendor.lock();
            state.turn_open = true;
            watch(&vendor, &mut state);
        }
        Ok(())
    }
}

impl Vendor {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Say what became of a line, unless the harness is gone and nobody waits.
    fn note(&self, line: &str) {
        if !self.lock().closing {
            eprintln!("anyagents-opencode: {line}");
        }
    }

    /// One `opencode api` call. Its own process, so a hung call holds only the
    /// thread that made it, and a failure names the call and never a secret.
    /// `limit` bounds a call off the delivery path; a prompt and a turn's wait
    /// pass `None` and take as long as the service does.
    fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        limit: Option<Duration>,
    ) -> Result<Value, ApiError> {
        let (service, password) = {
            let state = self.lock();
            (state.service.clone(), state.password.clone())
        };
        let service = service.ok_or_else(|| {
            ApiError::local(
                method,
                path,
                "the OpenCode private endpoint is unavailable; refusing service discovery",
            )
        })?;
        let mut command = anyagents_harness::spawn_command(&self.program, &self.environment)
            .map_err(|err| ApiError::local(method, path, err.to_string()))?;
        // `opencode api --server <url> <METHOD> <path>`, as the CLI reads it:
        // the subcommand comes first, then its flags.
        command.args(["api", "--server", &service, method, path]);
        if let Some(body) = body {
            command.args(["-d", &body.to_string()]);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        if let Some(password) = password {
            command.env("OPENCODE_PASSWORD", password);
        }
        let mut child = command
            .spawn()
            .map_err(|err| ApiError::local(method, path, err.to_string()))?;
        // Both pipes are read while the call runs: an answer larger than a pipe
        // buffer must never hold the child.
        let drain = |pipe: Option<Box<dyn std::io::Read + Send>>| {
            thread::spawn(move || {
                let mut bytes = Vec::new();
                if let Some(mut pipe) = pipe {
                    let _ = pipe.read_to_end(&mut bytes);
                }
                bytes
            })
        };
        let stdout = drain(child.stdout.take().map(|pipe| Box::new(pipe) as _));
        let stderr = drain(child.stderr.take().map(|pipe| Box::new(pipe) as _));
        let status = match limit {
            None => child.wait(),
            Some(limit) => {
                let deadline = Instant::now() + limit;
                loop {
                    match child.try_wait() {
                        Ok(Some(status)) => break Ok(status),
                        Ok(None) if Instant::now() < deadline => {
                            thread::sleep(Duration::from_millis(20));
                        }
                        Ok(None) => {
                            anyagents_harness::kill_child(&mut child, &self.environment);
                            let _ = child.wait();
                            return Err(ApiError::local(
                                method,
                                path,
                                format!("no answer within {}s", limit.as_secs()),
                            ));
                        }
                        Err(err) => break Err(err),
                    }
                }
            }
        }
        .map_err(|err| ApiError::local(method, path, err.to_string()))?;
        let stdout = stdout.join().unwrap_or_default();
        let stderr = stderr.join().unwrap_or_default();
        if !status.success() {
            return Err(ApiError::answered(method, path, &stdout, &stderr));
        }
        let text = String::from_utf8_lossy(&stdout);
        let text = text.trim();
        // A 204 prints nothing.
        if text.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(text)
            .map_err(|err| ApiError::local(method, path, format!("undecodable answer: {err}")))
    }

    /// Keep this agent's session id beside its config, so the next start
    /// resumes this exact conversation.
    fn remember_session(&self) -> Result<(), HarnessError> {
        let session =
            self.lock().session.clone().ok_or_else(|| {
                HarnessError::Failed("OpenCode did not supply a session id".into())
            })?;
        if let Some(dir) = &self.config_dir {
            anyagents_harness::atomic_write_nonempty_file(
                &dir.join("opencode-session-id"),
                format!("{session}\n").as_bytes(),
                0o664,
            )
            .map_err(|err| HarnessError::Failed(format!("persist OpenCode session id: {err}")))?;
        }
        Ok(())
    }

    /// Create a fresh OpenCode session, persist its id to `opencode-session-id`,
    /// and mark recovery context to be seeded when replacing a missing session.
    fn recreate_session(
        &self,
        stale_session: Option<&str>,
        limit: Option<Duration>,
    ) -> Result<String, HarnessError> {
        if let Some(session) = stale_session {
            eprintln!(
                "anyagents-opencode: session {session} is gone from this agent's store; starting a new one"
            );
        }
        let fresh = self
            .call("POST", "/api/session", Some(&json!({})), limit)
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let id = fresh
            .pointer("/data/id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| HarnessError::Failed("OpenCode session response missing id".into()))?
            .to_string();
        let previous = self.lock().session.replace(id.clone());
        if let Err(err) = self.remember_session() {
            self.lock().session = previous;
            return Err(err);
        }
        if let Some(stale) = stale_session {
            self.sync_config_recovery_instructions();
            let mut state = self.lock();
            if state.recovered_from_session.is_none() {
                state.recovered_from_session = Some(stale.to_string());
            }
            state.turn_open = false;
        }
        Ok(id)
    }

    fn sync_config_recovery_instructions(&self) {
        let Some(dir) = self.config_dir.as_deref() else {
            return;
        };
        let Ok(summary) = std::fs::read_to_string(dir.join(".anyagents-session-recovery.md"))
        else {
            return;
        };
        let trimmed = summary.trim();
        if trimmed.is_empty() {
            return;
        }
        let bridge = json!({
            "harness": "opencode",
            "previous_harness": Value::Null,
            "summary": trimmed,
        });
        if let Ok(bytes) = serde_json::to_vec_pretty(&bridge)
            && let Err(err) = anyagents_harness::atomic_write_nonempty_file(
                &dir.join(".anyagents-session-bridge.json"),
                &bytes,
                0o664,
            )
        {
            eprintln!(
                "cannot persist {}: {err}",
                dir.join(".anyagents-session-bridge.json").display()
            );
        }
        const MAX_DOC_BYTES: usize = 23_200;
        for rel in [
            "SYSTEM_PROMPT.md",
            "AGENTS.md",
            "opencode/AGENTS.md",
            ".config/opencode/AGENTS.md",
        ] {
            let path = dir.join(rel);
            let Ok(mut doc) = std::fs::read_to_string(&path) else {
                continue;
            };
            if doc.contains("Cross-Harness Session Continuity") {
                continue;
            }
            let remaining = MAX_DOC_BYTES.saturating_sub(doc.len() + 2);
            if remaining >= 256 {
                doc.push('\n');
                if trimmed.len() <= remaining {
                    doc.push_str(trimmed);
                } else {
                    let mut cut = remaining;
                    while cut > 0 && !trimmed.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    if let Some(nl) = trimmed[..cut].rfind('\n')
                        && nl > 0
                    {
                        cut = nl;
                    }
                    doc.push_str(&trimmed[..cut]);
                }
                doc.push('\n');
                if let Err(err) =
                    anyagents_harness::atomic_write_nonempty_file(&path, doc.as_bytes(), 0o664)
                {
                    eprintln!("cannot persist {}: {err}", path.display());
                }
            }
        }
    }

    fn read_recovery_context(&self) -> Option<String> {
        let dir = self.config_dir.as_deref()?;
        if let Ok(recovery) = std::fs::read_to_string(dir.join(".anyagents-session-recovery.md"))
            && !recovery.trim().is_empty()
        {
            return Some(recovery.trim().to_string());
        }
        if let Ok(bridge) = std::fs::read_to_string(dir.join(".anyagents-session-bridge.json"))
            && let Ok(val) = serde_json::from_str::<Value>(&bridge)
            && let Some(summary) = val
                .get("summary")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            return Some(summary.to_string());
        }
        for candidate in ["SYSTEM_PROMPT.md", "opencode/AGENTS.md", "AGENTS.md"] {
            if let Ok(text) = std::fs::read_to_string(dir.join(candidate))
                && !text.trim().is_empty()
            {
                return Some(text.trim().to_string());
            }
        }
        None
    }

    fn compose_prompt(
        &self,
        message: &OutboundMessage,
        recovery_from: Option<(&str, &str)>,
    ) -> String {
        let composed = compose_message(&message.id, &message.context, &message.text);
        let Some((old_session, new_session)) = recovery_from else {
            return composed;
        };
        let (first_line, rest) = composed.split_once('\n').unwrap_or((composed.as_str(), ""));
        let mut recovery_lines = vec![
            format!(
                "<session_recovery stale_session=\"{}\" new_session=\"{}\">",
                old_session, new_session
            ),
            format!(
                "[Recovery] Session {old_session} was not found (HTTP 404); resumed on new session {new_session}."
            ),
        ];
        if let Some(config_context) = self.read_recovery_context() {
            recovery_lines.push(String::new());
            recovery_lines.push(config_context);
        }
        let (prior_history, saved_topology) = {
            let state = self.lock();
            (state.history.clone(), state.initial_topology.clone())
        };
        if !prior_history.is_empty() {
            recovery_lines.push(String::new());
            recovery_lines
                .push("The message(s) in the previous session before recovery:".to_string());
            for prev in &prior_history {
                let from = prev.sender.as_deref().unwrap_or("unknown");
                recovery_lines.push(format!("- message {} (from {from}):", prev.id));
                for line in prev.text.lines() {
                    recovery_lines.push(format!("  > {line}"));
                }
            }
        }
        recovery_lines.push("</session_recovery>".to_string());
        let mut out = format!("{first_line}\n{}", recovery_lines.join("\n"));
        if message.context.topology.is_none()
            && let Some(topo) = saved_topology
        {
            out.push('\n');
            out.push_str(&topo);
        }
        if !rest.is_empty() {
            out.push('\n');
            out.push_str(rest);
        }
        out
    }

    /// Write one line and say what became of it. A session the service says
    /// is gone is replaced once, and the line written there with the context
    /// of the old one.
    fn write_prompt(&self, message: &OutboundMessage, replay: bool) -> Prompt {
        let (session, stale) = {
            let state = self.lock();
            (state.session.clone(), state.recovered_from_session.clone())
        };
        let Some(session) = session else {
            return Prompt::Fault("no OpenCode session".into());
        };
        let text = self.compose_prompt(
            message,
            stale.as_deref().map(|stale| (stale, session.as_str())),
        );
        match self.admit(&session, message, &text, replay) {
            Ok(prompt) => prompt,
            Err(_) => {
                let stale = stale.unwrap_or(session);
                let fresh = match self.recreate_session(Some(&stale), None) {
                    Ok(fresh) => fresh,
                    Err(err) => return Prompt::Fault(err.to_string()),
                };
                let text = self.compose_prompt(message, Some((&stale, &fresh)));
                match self.admit(&fresh, message, &text, replay) {
                    Ok(prompt) => prompt,
                    Err(err) => Prompt::Fault(err.to_string()),
                }
            }
        }
    }

    /// `POST /api/session/{session}/prompt` with the line's own id. `Err` is
    /// the service saying the session does not exist.
    fn admit(
        &self,
        session: &str,
        message: &OutboundMessage,
        text: &str,
        replay: bool,
    ) -> Result<Prompt, ApiError> {
        let id = vendor_message_id(&message.id);
        let body = json!({ "id": id, "text": text, "delivery": "steer" });
        let sent_at = now_ms();
        let answer = match self.call(
            "POST",
            &format!("/api/session/{session}/prompt"),
            Some(&body),
            None,
        ) {
            Ok(answer) => answer,
            Err(err) => {
                return match err.refusal() {
                    Refusal::SessionGone => Err(err),
                    Refusal::Permanent => {
                        self.note(&format!("{err}; the line is rejected"));
                        Ok(Prompt::Rejected(err.to_string()))
                    }
                    Refusal::NotReady => {
                        self.note(&format!(
                            "{err}; the line goes back to the mailbox after {}s",
                            RETRY_AFTER.as_secs()
                        ));
                        Ok(Prompt::Refused)
                    }
                    Refusal::Fault => Ok(Prompt::Fault(err.to_string())),
                    Refusal::Unknown if replay => Ok(Prompt::Fault(err.to_string())),
                    Refusal::Unknown => {
                        self.note(&format!("{err}; the same id is written again"));
                        Ok(Prompt::Unknown)
                    }
                };
            }
        };
        let Some(record) = answer.get("data").filter(|record| record.is_object()) else {
            return Ok(if replay {
                Prompt::Fault(format!("prompt answered without its record: {answer}"))
            } else {
                Prompt::Unknown
            });
        };
        // The service answers an id it already holds with that record, created
        // before this request. If the session already took it into its
        // conversation, an earlier process applied the line.
        let existed = record
            .pointer("/time/created")
            .and_then(Value::as_i64)
            .is_some_and(|created| created < sent_at);
        if existed
            && self
                .call(
                    "GET",
                    &format!("/api/session/{session}/message/{id}"),
                    None,
                    None,
                )
                .is_ok()
        {
            return Ok(Prompt::Applied);
        }
        Ok(Prompt::Admitted)
    }
}

/// Write one line on this thread and record what became of it. Whatever the
/// answer, the host is woken to offer the line, or the next one, again.
fn carry(vendor: &Arc<Vendor>, message: &OutboundMessage, replay: bool) {
    let outcome = vendor.write_prompt(message, replay);
    if outcome == Prompt::Refused {
        // The service asked to be called again after its `retry-after`. The
        // line still holds the send slot, so nothing overtakes it meanwhile.
        thread::sleep(RETRY_AFTER);
    }
    let mut state = vendor.lock();
    if state.sending.as_deref() == Some(message.id.as_str()) {
        state.sending = None;
    }
    if state.closing {
        drop(state);
        vendor.changed.notify_all();
        return;
    }
    if outcome == Prompt::Admitted {
        if state.initial_topology.is_none()
            && let Some(topo) = &message.context.topology
        {
            state.initial_topology = Some(topo.clone());
        }
        state.recovered_from_session = None;
        state.history.push(PriorPrompt {
            id: message.id.clone(),
            sender: message
                .sender
                .clone()
                .or_else(|| message.context.sender.clone()),
            text: message.text.clone(),
        });
        state.written.push(message.id.clone());
        state.admissions += 1;
        state.turn_open = true;
        watch(vendor, &mut state);
    }
    state.prompts.insert(message.id.clone(), outcome);
    state.epoch += 1;
    drop(state);
    vendor.changed.notify_all();
}

/// Wait for the open turn to end on a thread of its own, unless one already does.
fn watch(vendor: &Arc<Vendor>, state: &mut State) {
    if state.watching {
        return;
    }
    let waiter = Arc::clone(vendor);
    match thread::Builder::new()
        .name("anyagents-opencode-turn".into())
        .spawn(move || watch_turn(&waiter))
    {
        Ok(_) => state.watching = true,
        Err(err) => eprintln!("anyagents-opencode: cannot wait for the turn to end: {err}"),
    }
}

/// End the open turn once the service is idle after the newest admission.
///
/// `POST /api/experimental/session/{id}/wait` answers when the session's
/// agent loop is idle, and a prompt wakes that loop before its call answers,
/// so a wait asked after an admission returns only once the turn that took it
/// has ended. A service without that route is asked which sessions run, once
/// per `TURN_POLL`.
fn watch_turn(vendor: &Arc<Vendor>) {
    // Whether the service has the wait route at all.
    let mut can_wait = true;
    loop {
        let (session, admitted) = {
            let mut state = vendor.lock();
            let Some(session) = state
                .session
                .clone()
                .filter(|_| state.alive && !state.closing)
            else {
                state.watching = false;
                return;
            };
            (session, state.admissions)
        };
        let waited = if can_wait {
            match vendor.call(
                "POST",
                &format!("/api/experimental/session/{session}/wait"),
                None,
                None,
            ) {
                Ok(_) => Some(true),
                Err(err) if err.tag.as_deref() == Some("SessionNotFoundError") => Some(true),
                Err(err) => {
                    can_wait = err.status != Some(404);
                    None
                }
            }
        } else {
            None
        };
        let idle = waited.unwrap_or_else(|| {
            thread::sleep(TURN_POLL);
            matches!(
                vendor.call("GET", "/api/session/active", None, Some(CALL_TIMEOUT)),
                Ok(active) if active.get("data").and_then(|data| data.get(&session)).is_none()
            )
        });
        let mut state = vendor.lock();
        if !state.alive || state.closing {
            state.watching = false;
            return;
        }
        // A later prompt woke the session again: its turn ends after that one.
        if !idle || state.admissions != admitted {
            continue;
        }
        state.watching = false;
        state.turn_open = false;
        state.turns_completed = state.turns_completed.saturating_add(1);
        state.epoch += 1;
        drop(state);
        vendor.changed.notify_all();
        return;
    }
}

impl Harness for OpenCodeHarness {
    /// Write one mailbox line into the session's inbox as a steer that
    /// carries the line's own id, and accept it once the service admitted it.
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let mut state = self.vendor.lock();
        // What the service said stands even after the process went.
        match state.prompts.get(&message.id) {
            Some(Prompt::Admitted) => {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                });
            }
            Some(Prompt::Applied) => return Ok(DeliverOutcome::Duplicate),
            Some(Prompt::Rejected(reason)) => {
                // A 409 names this line's id, held by another record; a size
                // refusal is this line's own. A 400 may be every line's.
                let message_specific = reason.contains("(HTTP 409)")
                    || reason.contains("(HTTP 413)")
                    || anyagents_harness::names_message_size(reason);
                return Ok(DeliverOutcome::rejected(reason.clone(), message_specific));
            }
            _ => {}
        }
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        let replay = match state.prompts.get(&message.id) {
            Some(Prompt::Sending) => return Ok(DeliverOutcome::InFlight),
            Some(Prompt::Refused) => {
                // Nothing was admitted, and the service's wait is over: the
                // line goes back to the mailbox and is written again on its
                // next offer, which the wake that recorded this asks for.
                state.prompts.remove(&message.id);
                return Ok(DeliverOutcome::Deferred);
            }
            Some(Prompt::Fault(reason)) => {
                let reason = reason.clone();
                state.prompts.remove(&message.id);
                return Err(HarnessError::Failed(reason));
            }
            // The service may or may not hold it: the same id again settles
            // that, since the service admits an id once.
            Some(Prompt::Unknown) => true,
            _ => false,
        };
        // One prompt call at a time keeps the inbox in mailbox order.
        if state.sending.is_some() {
            return Ok(DeliverOutcome::Deferred);
        }
        state.sending = Some(message.id.clone());
        state.prompts.insert(message.id.clone(), Prompt::Sending);
        drop(state);
        let vendor = Arc::clone(&self.vendor);
        let line = message.clone();
        if let Err(err) = thread::Builder::new()
            .name("anyagents-opencode-prompt".into())
            .spawn(move || carry(&vendor, &line, replay))
        {
            let mut state = self.vendor.lock();
            state.sending = None;
            state.prompts.remove(&message.id);
            return Err(HarnessError::Failed(format!(
                "cannot start the prompt call: {err}"
            )));
        }
        Ok(DeliverOutcome::InFlight)
    }

    /// What the service admitted, by this process or (as a record already in
    /// the conversation) by an earlier one. Read only.
    fn proven(&self, message_id: &str) -> Option<Proof> {
        match self.vendor.lock().prompts.get(message_id)? {
            Prompt::Admitted => Some(Proof::Taken {
                turn_finished: false,
            }),
            Prompt::Applied => Some(Proof::Duplicate),
            _ => None,
        }
    }

    /// Wait for the prompt call that is out to answer or fail, so what it
    /// proved is known, then stop the service. The proofs stay readable. A
    /// service that does not answer within [`CALL_TIMEOUT`] is stopped
    /// anyway, so a stuck service never holds its replacement: the call then
    /// fails, its outcome is unknown, and the next process writes the same id
    /// once more, which OpenCode admits once.
    fn stop(&mut self) {
        {
            let state = self.vendor.lock();
            let _state = self
                .vendor
                .changed
                .wait_timeout_while(state, CALL_TIMEOUT, |state| state.sending.is_some())
                .unwrap_or_else(|err| err.into_inner());
        }
        self.stop_child();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let previous = self.vendor.lock().session.replace(session.to_string());
        if let Err(err) = self.vendor.remember_session() {
            self.vendor.lock().session = previous;
            return Err(err);
        }
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let Some(session) = self.vendor.lock().session.clone() else {
            return Ok(());
        };
        self.vendor
            .call(
                "POST",
                &format!("/api/session/{session}/interrupt"),
                None,
                Some(CALL_TIMEOUT),
            )
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        self.vendor.lock().turn_open = false;
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let state = self.vendor.lock();
        HarnessHealth {
            process_alive: state.alive,
            // A prompt still being written is work under way, not an idle agent.
            turn_state: if state.turn_open || state.sending.is_some() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: DELIVERY.with_peer(state.peer.clone()),
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    fn turns_served(&self) -> u32 {
        self.vendor.lock().turns_completed
    }

    /// The session's own running totals, read from the session itself once
    /// per finished turn. The host records the difference.
    fn served_usage(&self) -> TurnUsage {
        let (session, turns) = {
            let state = self.vendor.lock();
            let Some(session) = state.session.clone() else {
                return TurnUsage::default();
            };
            if state.usage_read_at == Some(state.turns_completed) {
                return state.cached_usage.clone();
            }
            (session, state.turns_completed)
        };
        let info = self.vendor.call(
            "GET",
            &format!("/api/session/{session}"),
            None,
            Some(CALL_TIMEOUT),
        );
        let Some(info) = info.ok().and_then(|info| info.get("data").cloned()) else {
            return self.vendor.lock().cached_usage.clone();
        };
        let tokens = info.get("tokens").cloned().unwrap_or(Value::Null);
        let number = |key: &str| tokens.get(key).and_then(Value::as_u64).unwrap_or(0);
        let usage = TurnUsage {
            input_tokens: number("input"),
            output_tokens: number("output"),
            cache_read_tokens: tokens
                .pointer("/cache/read")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            cache_write_tokens: tokens
                .pointer("/cache/write")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            reported_cost_micros: info
                .get("cost")
                .and_then(Value::as_f64)
                .map(|cost| (cost * 1_000_000.0) as i64),
        };
        let mut state = self.vendor.lock();
        state.usage_read_at = Some(turns);
        state.cached_usage = usage.clone();
        usage
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let vendor = Arc::clone(&self.vendor);
        let _ = thread::Builder::new()
            .name("anyagents-opencode-watch".into())
            .spawn(move || {
                let mut state = vendor.lock();
                let mut seen = state.epoch;
                loop {
                    if !state.alive || state.closing {
                        drop(state);
                        notify();
                        return;
                    }
                    if state.epoch != seen {
                        seen = state.epoch;
                        drop(state);
                        notify();
                        state = vendor.lock();
                        continue;
                    }
                    state = vendor
                        .changed
                        .wait(state)
                        .unwrap_or_else(|err| err.into_inner());
                }
            });
    }
}

const SERVICE_PID_FILE: &str = "opencode-service-pid";

impl OpenCodeHarness {
    fn write_service_pid(&self, pid: u32) {
        let Some(dir) = self.vendor.config_dir.as_deref() else {
            return;
        };
        let path = dir.join(SERVICE_PID_FILE);
        if let Err(err) = anyagents_harness::atomic_write_nonempty_file(
            &path,
            format!("{pid}\n").as_bytes(),
            0o664,
        ) {
            eprintln!("cannot persist {}: {err}", path.display());
        }
    }

    fn clear_service_pid(&self, expected: Option<u32>) {
        let Some(dir) = self.vendor.config_dir.as_deref() else {
            return;
        };
        let path = dir.join(SERVICE_PID_FILE);
        if let Some(expected_pid) = expected
            && let Ok(text) = std::fs::read_to_string(&path)
            && text.trim().parse::<u32>().ok() != Some(expected_pid)
        {
            return;
        }
        let _ = std::fs::remove_file(path);
    }

    fn reap_prior_service(&self) {
        let Some(dir) = self.vendor.config_dir.as_deref() else {
            return;
        };
        let pid_path = dir.join(SERVICE_PID_FILE);
        if let Ok(text) = std::fs::read_to_string(&pid_path)
            && let Ok(pid) = text.trim().parse::<u32>()
            && pid > 1
            && pid != std::process::id()
            && is_opencode_service_pid(pid)
        {
            self.terminate_service_pid(pid);
        }
        let _ = std::fs::remove_file(&pid_path);
        for orphan_pid in find_orphan_service_pids(dir) {
            if orphan_pid > 1 && orphan_pid != std::process::id() {
                self.terminate_service_pid(orphan_pid);
            }
        }
        port::PortLease::release_stored(Some(dir));
    }

    fn stored_port_occupied(&self) -> bool {
        let Some(port) = self.vendor.config_dir.as_deref().and_then(|dir| {
            std::fs::read_to_string(dir.join("opencode-service-port"))
                .ok()?
                .trim()
                .parse::<u16>()
                .ok()
        }) else {
            return false;
        };
        matches!(
            std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)),
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse
        )
    }

    fn terminate_service_pid(&self, pid: u32) {
        let environment = &self.vendor.environment;
        let children = direct_child_pids(pid);
        anyagents_harness::kill_pid(pid, "-TERM", environment);
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline && (process_exists(pid) || self.stored_port_occupied()) {
            thread::sleep(Duration::from_millis(25));
        }
        if process_exists(pid) || self.stored_port_occupied() {
            anyagents_harness::kill_pid(pid, "-KILL", environment);
            let kill_deadline = Instant::now() + Duration::from_millis(500);
            while Instant::now() < kill_deadline
                && (process_exists(pid) || self.stored_port_occupied())
            {
                thread::sleep(Duration::from_millis(25));
            }
        }
        for child_pid in children {
            if child_pid > 1 && process_exists(child_pid) {
                anyagents_harness::kill_pid(child_pid, "-KILL", environment);
            }
        }
    }

    /// Stop the service. It is reported gone once it has exited (or never
    /// ran); its stdout reader says the same at end of file. Nothing it proves
    /// arrives on stdout: a proof is a prompt call's answer, which `stop`
    /// waits for first.
    fn stop_child(&mut self) {
        if let Some(mut child) = self.child.take() {
            let environment = &self.vendor.environment;
            let pid = child.id();
            let children = direct_child_pids(pid);
            // The exact child, never a pattern or a group: a shared desk refuses
            // a group signal it does not own, and it is right to.
            anyagents_harness::kill_pid(pid, "-TERM", environment);
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) if Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(50));
                    }
                    _ => {
                        anyagents_harness::kill_child(&mut child, environment);
                        break;
                    }
                }
            }
            let _ = child.wait();
            for child_pid in children {
                if child_pid > 1 && process_exists(child_pid) {
                    anyagents_harness::kill_pid(child_pid, "-KILL", environment);
                }
            }
            self.clear_service_pid(Some(pid));
        }
        let mut state = self.vendor.lock();
        state.alive = false;
        state.service = None;
        state.password = None;
        state.epoch += 1;
        drop(state);
        self.vendor.changed.notify_all();
    }
}

fn process_exists(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false;
        };
        let Some(after_comm) = stat.rfind(')') else {
            return true;
        };
        let state = stat[after_comm + 1..]
            .split_whitespace()
            .next()
            .unwrap_or("");
        !matches!(state, "Z" | "X" | "x")
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        false
    }
}

fn is_opencode_service_pid(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        match std::fs::read(format!("/proc/{pid}/cmdline")) {
            Ok(bytes) if !bytes.is_empty() => {
                let args: Vec<&str> = bytes
                    .split(|b| *b == 0)
                    .filter(|s| !s.is_empty())
                    .filter_map(|s| std::str::from_utf8(s).ok())
                    .collect();
                args.contains(&"serve") && (args.contains(&"--port") || args.contains(&"--service"))
            }
            Ok(_) => false,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
            Err(_) => true,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        true
    }
}

#[cfg(target_os = "linux")]
fn proc_ppid(pid: u32) -> Option<u32> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = stat.rfind(')')?;
    let mut fields = stat[after_comm + 1..].split_whitespace();
    let _state = fields.next()?;
    fields.next()?.parse::<u32>().ok()
}

fn direct_child_pids(parent_pid: u32) -> Vec<u32> {
    #[cfg(target_os = "linux")]
    {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return out;
        };
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            if proc_ppid(pid) == Some(parent_pid) {
                out.push(pid);
            }
        }
        out
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = parent_pid;
        Vec::new()
    }
}

fn find_orphan_service_pids(config_dir: &Path) -> Vec<u32> {
    #[cfg(target_os = "linux")]
    {
        let mut out = Vec::new();
        let stored_port = std::fs::read_to_string(config_dir.join("opencode-service-port"))
            .ok()
            .and_then(|s| s.trim().parse::<u16>().ok())
            .map(|p| p.to_string());
        let config_str = config_dir.to_string_lossy();
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return out;
        };
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            let Ok(bytes) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
                continue;
            };
            let args: Vec<&str> = bytes
                .split(|b| *b == 0)
                .filter(|s| !s.is_empty())
                .filter_map(|s| std::str::from_utf8(s).ok())
                .collect();
            if args.is_empty() {
                continue;
            }
            if let Some(port_str) = stored_port.as_deref()
                && proc_ppid(pid) == Some(1)
                && args.contains(&"serve")
                && args.windows(2).any(|w| w == ["--port", port_str])
                && !out.contains(&pid)
            {
                out.push(pid);
            }
            if args.contains(&"mcp")
                && args
                    .windows(2)
                    .any(|w| w == ["--config", config_str.as_ref()])
                && let Some(parent) = proc_ppid(pid)
                && parent > 1
                && proc_ppid(parent) == Some(1)
                && is_opencode_service_pid(parent)
                && !out.contains(&parent)
            {
                out.push(parent);
            }
        }
        out
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = config_dir;
        Vec::new()
    }
}

impl Drop for OpenCodeHarness {
    fn drop(&mut self) {
        self.vendor.lock().closing = true;
        self.stop_child();
    }
}

/// What the service said about a stored session.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Missing {
    Present,
    Gone,
    Unknown,
}

/// One `opencode api` call that did not answer 2xx, or did not answer at all.
#[derive(Debug)]
struct ApiError {
    call: String,
    /// The HTTP status `opencode api` reported (`HTTP <status> <text>` on
    /// stderr, after the answer's body on stdout). `None`: no answer arrived.
    status: Option<u16>,
    /// The error's `_tag`, from the answer's body.
    tag: Option<String>,
    /// The error's `code`, from the answer's body (the readiness gate's 503).
    code: Option<String>,
    reason: String,
}

/// What a prompt call that did not answer 2xx says about the line.
#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    /// 404: the session does not exist.
    SessionGone,
    /// 400 or 409: admitted nothing, and never will.
    Permanent,
    /// 503 `service_starting` or `service_stopping`: admitted nothing yet.
    NotReady,
    /// 401, 403, 503 `service_failed`: the process is at fault.
    Fault,
    /// Anything else, or no answer: it may or may not have been admitted.
    Unknown,
}

impl ApiError {
    fn local(method: &str, path: &str, reason: impl Into<String>) -> Self {
        Self {
            call: format!("{method} {path}"),
            status: None,
            tag: None,
            code: None,
            reason: reason.into(),
        }
    }

    fn answered(method: &str, path: &str, stdout: &[u8], stderr: &[u8]) -> Self {
        let stderr = String::from_utf8_lossy(stderr);
        let status = stderr.lines().find_map(|line| {
            line.trim()
                .strip_prefix("HTTP ")?
                .split_whitespace()
                .next()?
                .parse::<u16>()
                .ok()
        });
        let body = serde_json::from_slice::<Value>(stdout).ok();
        let field = |key: &str| {
            body.as_ref()
                .and_then(|body| body.get(key))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        let reason = field("message")
            .or_else(|| field("code"))
            .or_else(|| {
                stderr
                    .lines()
                    .map(str::trim)
                    .find(|line| !line.is_empty() && !line.starts_with("HTTP "))
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "request failed".into());
        Self {
            call: format!("{method} {path}"),
            status,
            tag: field("_tag"),
            code: field("code"),
            reason,
        }
    }

    /// What this answer to `POST /api/session/{id}/prompt` means for the line.
    /// OpenCode 2.x declares 400 (`BadRequest`: a payload that does not
    /// decode, an attachment or a skill it cannot take), 409
    /// (`PromptConflictError`: the id belongs to another durable record) and
    /// 404 (`Session.NotFoundError`) for the route; its readiness gate answers
    /// 503 before any route.
    fn refusal(&self) -> Refusal {
        match self.status {
            Some(404) => Refusal::SessionGone,
            Some(400 | 409) => Refusal::Permanent,
            Some(401 | 403) => Refusal::Fault,
            Some(503) => match self.code.as_deref() {
                Some("service_starting" | "service_stopping") => Refusal::NotReady,
                Some("service_failed") => Refusal::Fault,
                _ => Refusal::Unknown,
            },
            _ => Refusal::Unknown,
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.status {
            Some(status) => write!(f, "{}: {} (HTTP {status})", self.call, self.reason),
            None => write!(f, "{}: {}", self.call, self.reason),
        }
    }
}

/// The id a line's prompt carries. OpenCode takes any id that starts with
/// `msg_` and recognises it when it is sent again. A mailbox id already is
/// one; another id (a recovery note's `recovery_msg_…`) gets the prefix, so
/// the same line always carries the same id.
fn vendor_message_id(id: &str) -> String {
    if id.starts_with("msg_") {
        id.to_string()
    } else {
        format!("msg_{id}")
    }
}

/// The major number of a version such as `2.1.4`.
fn major_version(version: &str) -> Option<u64> {
    version
        .trim()
        .trim_start_matches('v')
        .split('.')
        .next()?
        .parse()
        .ok()
}

/// Milliseconds since the Unix epoch, the clock the service stamps its records with.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// The answer the session holds for one mailbox id: the assistant text created
/// after the newest user message carrying `anyagents-message:<id>`.
fn answer_of(messages: &[Value], id: &str) -> Option<String> {
    let marker = format!("anyagents-message:{id}");
    let asked_at = messages.iter().position(|message| {
        message.get("type").and_then(Value::as_str) == Some("user")
            && message
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.contains(&marker))
    })?;
    let asked = messages[asked_at]
        .pointer("/time/created")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    messages[..asked_at]
        .iter()
        .find(|message| {
            message.get("type").and_then(Value::as_str) == Some("assistant")
                && message
                    .pointer("/time/created")
                    .and_then(Value::as_i64)
                    .is_some_and(|created| created > asked)
        })
        .map(text_of)
        .filter(|text| !text.is_empty())
}

/// An assistant message's text parts, joined. Reasoning parts are skipped.
fn text_of(message: &Value) -> String {
    let mut text = String::new();
    for part in message
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if part.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        if let Some(part) = part.get("text").and_then(Value::as_str) {
            text.push_str(part);
        }
    }
    text.trim().to_string()
}
