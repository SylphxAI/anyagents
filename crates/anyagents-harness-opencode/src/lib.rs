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
//! - the admission only queues the line (`InFlight`). The proof is the
//!   service's `session.inbox.delivered` event naming the line's id as
//!   `inboxID`, which it announces when the session takes the input: the
//!   step that opens a turn with it, or the running turn's next step
//!   (`NextStep`). The event stream (`GET /api/event`) is read over a direct
//!   connection to the private service (`events`), because `opencode api`
//!   prints nothing of a body that never ends. The start waits for its
//!   `server.connected`, so no proof is missed before the first prompt. The
//!   stream replays nothing: after a reconnect each line still unproven is
//!   looked up in the conversation under its id (`GET …/message/{id}`), which
//!   proves it as well. A line the service announces `session.inbox.cancelled`
//!   goes back to the mailbox. Because the id travels with the prompt,
//!   writing the same line again, from this process or a later one, never
//!   applies it twice, and a record that predates the request and is already
//!   in the conversation is answered as a duplicate;
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
//!   is waited for, up to a minute, then the event stream is read to its end,
//!   so what the service proved is known; a line still unproven then is
//!   offered to the next process, which writes its id again. The proofs
//!   outlive the process.
//!
//! The mailbox id on the prompt's first line also makes the agent's own `send`
//! (its MCP tool) the reply path, recorded against the same agent. Nothing
//! here reads a credential: the service's address and password come from the
//! private service this adapter starts for this agent.

mod events;
mod port;

use std::collections::{HashMap, HashSet};
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

/// How long `stop` waits for the event stream of a stopped service to end.
const EVENTS_DRAIN: Duration = Duration::from_secs(5);

/// How long a lost event stream waits before it connects again.
const EVENTS_RETRY: Duration = Duration::from_millis(100);

/// The prefix a 1.x `opencode serve` announces its address with. A 2.x
/// service prints `server listening on <url>` and then `server password …`.
const LEGACY_ANNOUNCEMENT: &str = "opencode server listening on ";

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed("session.inbox.delivered, by id"),
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
    /// The session's inbox holds it; its proof is awaited.
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
    /// Prompt ids the session took into its conversation, as its event stream
    /// announced them or the conversation holds them.
    consumed: HashSet<String>,
    /// Prompt ids the service took out of its inbox unconsumed.
    cancelled: HashSet<String>,
    /// The event stream is read now (`server.connected` arrived).
    events_connected: bool,
    /// A thread reads the event stream of the running service.
    events_reading: bool,
    /// The service refused its event stream: nothing can be proven.
    events_fault: Option<String>,
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
    /// Active `settle_unproven` passes fencing mailbox delivery until all
    /// unproven prompts settle atomically.
    settling: usize,
    turns_completed: u32,
    /// Set when OpenCode emits an SSE error or quota event during an active
    /// prompt/turn so `watch_turn` never counts an errored turn as completed.
    turn_errored: bool,
    /// Quota reset instant (epoch ms) observed from an OpenCode SSE quota/rate-limit event.
    quota_reset_at_ms: Option<i64>,
    /// Upstream AI/auth/session error reported on OpenCode's SSE event stream.
    sign_in_fault: Option<String>,
    /// `turns_completed` when the session's totals were last read.
    usage_read_at: Option<u32>,
    /// What this process served, as last read: the host records differences
    /// of it, so it counts from this process's start, never the session's.
    cached_usage: TurnUsage,
    /// The session this process serves now, and its totals when this process
    /// began serving it: what came before is an earlier process's.
    usage_base: Option<(String, TurnUsage)>,
    /// What this process served in sessions it served before this one.
    usage_earlier: TurnUsage,
}

/// `a` and `b` added, field by field.
fn plus(a: &TurnUsage, b: &TurnUsage) -> TurnUsage {
    TurnUsage {
        input_tokens: a.input_tokens + b.input_tokens,
        output_tokens: a.output_tokens + b.output_tokens,
        cache_read_tokens: a.cache_read_tokens + b.cache_read_tokens,
        cache_write_tokens: a.cache_write_tokens + b.cache_write_tokens,
        reported_cost_micros: match (a.reported_cost_micros, b.reported_cost_micros) {
            (Some(a), Some(b)) => Some(a + b),
            (a, b) => a.or(b),
        },
    }
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
                Announcement::Ready => {
                    // The stream is read from before the first prompt, so
                    // no proof of it can pass unread.
                    read_events(&self.vendor)?;
                    self.resume_session()?;
                    return self.await_events();
                }
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
        let mut child = anyagents_harness::spawn_child(&mut command)
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
            if let Some(session) = wanted.as_deref() {
                // A resumed session's totals so far are earlier processes'.
                vendor.begin_usage(session, vendor.session_totals(session));
            }
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

impl OpenCodeHarness {
    /// Wait until the event stream is read, or fail the start: without it
    /// nothing written could be proven.
    fn await_events(&self) -> Result<(), HarnessError> {
        let state = self.vendor.lock();
        let (state, _) = self
            .vendor
            .changed
            .wait_timeout_while(state, READY_TIMEOUT, |state| {
                !state.events_connected && state.events_fault.is_none() && state.events_reading
            })
            .unwrap_or_else(|err| err.into_inner());
        if state.events_connected {
            return Ok(());
        }
        Err(HarnessError::Failed(match &state.events_fault {
            Some(fault) => fault.clone(),
            None => "the OpenCode event stream (GET /api/event) did not connect".into(),
        }))
    }
}

/// Read the running service's event stream on a thread of its own, for as long
/// as that service runs, connecting again whenever the connection is lost.
fn read_events(vendor: &Arc<Vendor>) -> Result<(), HarnessError> {
    let launch = {
        let mut state = vendor.lock();
        state.events_connected = false;
        state.events_fault = None;
        state.events_reading = true;
        state.launch
    };
    let reader = Arc::clone(vendor);
    thread::Builder::new()
        .name("anyagents-opencode-events".into())
        .spawn(move || follow_events(&reader, launch))
        .map(|_| ())
        .map_err(|err| {
            vendor.lock().events_reading = false;
            HarnessError::Failed(format!("cannot read the OpenCode event stream: {err}"))
        })
}

fn follow_events(vendor: &Arc<Vendor>, launch: u64) {
    let mut connected_before = false;
    loop {
        let (service, password) = {
            let state = vendor.lock();
            if state.launch != launch || state.launch_ended || state.closing {
                break;
            }
            match (state.service.clone(), state.password.clone()) {
                (Some(service), Some(password)) => (service, password),
                _ => break,
            }
        };
        let ended = events::subscribe(&service, &password, |event| {
            let connected = on_event(vendor, launch, &event, connected_before);
            if connected && connected_before {
                // Events during the gap are gone: the conversation settles
                // every line still unproven, off this thread so the stream
                // keeps being read.
                let settler = Arc::clone(vendor);
                if thread::Builder::new()
                    .name("anyagents-opencode-settle".into())
                    .spawn(move || settle_unproven(&settler, false))
                    .is_err()
                {
                    let mut state = vendor.lock();
                    state.settling = state.settling.saturating_sub(1);
                    state.epoch += 1;
                    drop(state);
                    vendor.changed.notify_all();
                }
            }
            connected_before |= connected;
        });
        let mut state = vendor.lock();
        if state.launch == launch {
            state.events_connected = false;
            if let events::Ended::Refused(status) = ended {
                state.events_fault = Some(format!(
                    "the OpenCode event stream (GET /api/event) answered HTTP {status}"
                ));
                state.epoch += 1;
                break;
            }
        }
        drop(state);
        thread::sleep(match ended {
            events::Ended::NotReady => RETRY_AFTER,
            events::Ended::Lost | events::Ended::Refused(_) => EVENTS_RETRY,
        });
    }
    let mut state = vendor.lock();
    if state.launch == launch {
        state.events_reading = false;
        state.events_connected = false;
    }
    drop(state);
    vendor.changed.notify_all();
}

fn event_str<'a>(event: &'a Value, pointers: &[&str]) -> Option<&'a str> {
    pointers
        .iter()
        .find_map(|ptr| event.pointer(ptr).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn event_i64(event: &Value, pointers: &[&str]) -> Option<i64> {
    pointers
        .iter()
        .find_map(|ptr| event.pointer(ptr).and_then(Value::as_i64))
}

fn event_f64(event: &Value, pointers: &[&str]) -> Option<f64> {
    pointers.iter().find_map(|ptr| {
        let v = event.pointer(ptr)?;
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
    })
}

fn find_quota_reset_in_event(event: &Value, message: &str, observed_ms: i64) -> Option<i64> {
    if let Some(ms) = event_i64(
        event,
        &[
            "/data/quota_reset_at_ms",
            "/data/resets_at_ms",
            "/data/reset_at_ms",
            "/data/error/data/quota_reset_at_ms",
            "/data/error/data/resets_at_ms",
            "/data/error/data/reset_at_ms",
            "/properties/quota_reset_at_ms",
            "/properties/resets_at_ms",
            "/properties/error/data/resets_at_ms",
            "/quota_reset_at_ms",
            "/resets_at_ms",
            "/reset_at_ms",
        ],
    ) {
        return Some(ms);
    }
    if let Some(n) = event_i64(
        event,
        &[
            "/data/resetsAt",
            "/data/resetAt",
            "/data/next",
            "/data/status/next",
            "/properties/status/next",
            "/resetsAt",
            "/resetAt",
        ],
    ) {
        return Some(if n >= 1_000_000_000_000 {
            n
        } else {
            n.saturating_mul(1_000)
        });
    }
    if let Some(s) = event_str(
        event,
        &[
            "/data/resetsAt",
            "/data/resetAt",
            "/data/resets_at",
            "/data/error/data/resets_at",
            "/properties/resetsAt",
            "/resets_at",
        ],
    ) && let Some(ms) = anyagents_harness::quota::parse_rfc3339_utc_ms(s)
    {
        return Some(ms);
    }
    if let Some(delay_ms) = event_i64(
        event,
        &[
            "/data/retry_after_ms",
            "/data/retryAfterMs",
            "/data/error/data/retry_after_ms",
            "/properties/retry_after_ms",
            "/retry_after_ms",
        ],
    ) && delay_ms >= 0
    {
        return Some(observed_ms.saturating_add(delay_ms));
    }
    if let Some(secs) = event_f64(
        event,
        &[
            "/data/retry_after",
            "/data/retryAfter",
            "/data/error/data/retry_after",
            "/data/error/data/responseHeaders/retry-after",
            "/properties/error/data/responseHeaders/retry-after",
            "/retry_after",
        ],
    ) && secs.is_finite()
        && secs >= 0.0
    {
        return Some(observed_ms.saturating_add((secs * 1_000.0).round() as i64));
    }
    anyagents_harness::quota::reset_at_ms(message, observed_ms)
}

fn is_quota_or_rate_limit(
    kind: Option<&str>,
    name: Option<&str>,
    status: Option<i64>,
    msg: &str,
) -> bool {
    let lower = msg.to_ascii_lowercase();
    matches!(
        kind,
        Some("QuotaExceeded" | "quota.exceeded" | "rate_limit" | "rate_limit_exceeded")
    ) || matches!(name, Some("QuotaExceeded" | "RateLimitError"))
        || status == Some(429)
        || lower.contains("quota")
        || lower.contains("rate limit")
        || lower.contains("rate_limit")
        || lower.contains("too many requests")
        || lower.contains("usage limit")
}

struct ParsedSseError {
    message: String,
    quota_reset_at_ms: Option<i64>,
    is_quota: bool,
}

fn parse_sse_error(event: &Value, state_session: Option<&str>) -> Option<ParsedSseError> {
    let kind = event.get("type").and_then(Value::as_str);
    let event_session = event_str(
        event,
        &[
            "/data/sessionID",
            "/data/session_id",
            "/properties/sessionID",
            "/properties/session_id",
            "/sessionID",
            "/session_id",
        ],
    );
    if let (Some(ev_sid), Some(cur_sid)) = (event_session, state_session)
        && ev_sid != cur_sid
    {
        return None;
    }

    let err_name = event_str(
        event,
        &[
            "/data/error/name",
            "/data/name",
            "/properties/error/name",
            "/properties/name",
            "/error/name",
            "/name",
        ],
    );
    let status_type = event_str(event, &["/data/status/type", "/properties/status/type"]);
    let has_message_updated_error = kind == Some("message.updated")
        && (event
            .pointer("/data/info/error")
            .is_some_and(|v| !v.is_null())
            || event
                .pointer("/properties/info/error")
                .is_some_and(|v| !v.is_null()));

    let is_retry_quota = kind == Some("session.status")
        && status_type == Some("retry")
        && event_str(
            event,
            &["/data/status/message", "/properties/status/message"],
        )
        .is_some_and(|m| is_quota_or_rate_limit(kind, err_name, None, m));

    let is_error_event = matches!(
        kind,
        Some(
            "session.error"
                | "AI.Error"
                | "ai.error"
                | "QuotaExceeded"
                | "quota.exceeded"
                | "rate_limit"
                | "error"
                | "message.error"
        )
    ) || matches!(err_name, Some("AI.Error" | "QuotaExceeded"))
        || (kind == Some("session.status") && status_type == Some("error"))
        || is_retry_quota
        || has_message_updated_error;

    if !is_error_event {
        return None;
    }

    let message = event_str(
        event,
        &[
            "/data/error/data/message",
            "/data/error/message",
            "/data/error",
            "/data/info/error/data/message",
            "/data/info/error/message",
            "/data/status/message",
            "/data/message",
            "/properties/error/data/message",
            "/properties/error/message",
            "/properties/info/error/data/message",
            "/properties/info/error/message",
            "/properties/status/message",
            "/properties/message",
            "/error/message",
            "/error",
            "/message",
        ],
    )
    .map(str::to_string)
    .unwrap_or_else(|| {
        err_name
            .or(kind)
            .unwrap_or("opencode SSE error")
            .to_string()
    });

    let status_code = event_i64(
        event,
        &[
            "/data/error/data/statusCode",
            "/data/statusCode",
            "/data/status",
            "/data/info/error/data/statusCode",
            "/properties/error/data/statusCode",
            "/properties/statusCode",
            "/statusCode",
        ],
    );
    let quota_reset_at_ms = find_quota_reset_in_event(event, &message, now_ms());
    let is_quota = is_quota_or_rate_limit(kind, err_name, status_code, &message);

    Some(ParsedSseError {
        message,
        quota_reset_at_ms,
        is_quota,
    })
}

/// Record one event of the service started as `launch`. Answers whether it
/// was `server.connected`.
fn on_event(vendor: &Vendor, launch: u64, event: &Value, connected_before: bool) -> bool {
    let kind = event.get("type").and_then(Value::as_str);
    let data = event.get("data");
    let field = |key: &str| data.and_then(|data| data.get(key)).and_then(Value::as_str);
    let mut state = vendor.lock();
    if state.launch != launch {
        return false;
    }
    let connected = kind == Some("server.connected");
    match kind {
        Some("server.connected") => {
            state.events_connected = true;
            if connected_before {
                state.settling += 1;
            }
        }
        Some(kind @ ("session.inbox.delivered" | "session.inbox.cancelled")) => {
            let Some(inbox) = field("inboxID") else {
                return false;
            };
            if field("sessionID").is_none() || field("sessionID") != state.session.as_deref() {
                return false;
            }
            let inbox = inbox.to_string();
            if kind == "session.inbox.delivered" {
                state.cancelled.remove(&inbox);
                state.consumed.insert(inbox);
            } else if !state.consumed.contains(&inbox) {
                state.cancelled.insert(inbox);
            }
        }
        _ => {
            let Some(err) = parse_sse_error(event, state.session.as_deref()) else {
                return false;
            };
            state.turn_errored = true;
            if let Some(reset_at_ms) = err.quota_reset_at_ms {
                state.quota_reset_at_ms = Some(reset_at_ms);
            }
            if !err.is_quota || err.quota_reset_at_ms.is_none() {
                state.sign_in_fault = Some(err.message);
            }
        }
    }
    state.epoch += 1;
    drop(state);
    vendor.changed.notify_all();
    connected
}

/// Look up every line this process admitted and has no proof of in the
/// session's conversation, which holds a consumed input under its own id.
fn downgrade_unconsumed_admitted(state: &mut State) {
    let mut downgraded = Vec::new();
    for (id, prompt) in &mut state.prompts {
        let vid = vendor_message_id(id);
        if matches!(prompt, Prompt::Admitted)
            && !state.consumed.contains(&vid)
            && !state.cancelled.contains(&vid)
        {
            *prompt = Prompt::Unknown;
            downgraded.push(id.clone());
        }
    }
    if !downgraded.is_empty() {
        state
            .history
            .retain(|prior| !downgraded.iter().any(|id| id == &prior.id));
    }
}

fn settle_unproven(vendor: &Vendor, known_idle: bool) {
    let (session, admitted, unproven) = {
        let mut state = vendor.lock();
        let Some(session) = state.session.clone() else {
            state.settling = state.settling.saturating_sub(1);
            state.epoch += 1;
            drop(state);
            vendor.changed.notify_all();
            return;
        };
        let unproven: Vec<(String, String, bool)> = state
            .prompts
            .iter()
            .filter(|(_, prompt)| {
                matches!(prompt, Prompt::Sending | Prompt::Admitted | Prompt::Unknown)
            })
            .map(|(id, prompt)| {
                (
                    id.clone(),
                    vendor_message_id(id),
                    matches!(prompt, Prompt::Admitted),
                )
            })
            .filter(|(_, vid, _)| !state.consumed.contains(vid) && !state.cancelled.contains(vid))
            .collect();
        if unproven.is_empty() {
            state.settling = state.settling.saturating_sub(1);
            state.epoch += 1;
            drop(state);
            vendor.changed.notify_all();
            return;
        }
        (session, state.admissions, unproven)
    };
    let mut found_ids = Vec::new();
    let mut missing_admitted = Vec::new();
    for (msg_id, id, was_admitted) in unproven {
        match vendor.call(
            "GET",
            &format!("/api/session/{session}/message/{id}"),
            None,
            Some(CALL_TIMEOUT),
        ) {
            Ok(found) if found.pointer("/data/id").and_then(Value::as_str) == Some(id.as_str()) => {
                found_ids.push(id);
            }
            Err(err) if err.status == Some(404) && was_admitted => {
                missing_admitted.push((msg_id, id));
            }
            _ => {}
        }
    }
    let session_idle = if known_idle || missing_admitted.is_empty() {
        known_idle
    } else {
        matches!(
            vendor.call("GET", "/api/session/active", None, Some(CALL_TIMEOUT)),
            Ok(active) if active.get("data").and_then(|data| data.get(&session)).is_none()
        )
    };
    let mut state = vendor.lock();
    for id in found_ids {
        state.cancelled.remove(&id);
        state.consumed.insert(id);
    }
    if session_idle
        && state.session.as_deref() == Some(session.as_str())
        && state.admissions == admitted
    {
        if known_idle {
            downgrade_unconsumed_admitted(&mut state);
        } else {
            for (msg_id, id) in missing_admitted {
                if !state.consumed.contains(&id)
                    && matches!(state.prompts.get(&msg_id), Some(Prompt::Admitted))
                {
                    state.prompts.insert(msg_id.clone(), Prompt::Unknown);
                    state.history.retain(|prior| prior.id != msg_id);
                }
            }
        }
    }
    state.settling = state.settling.saturating_sub(1);
    state.epoch += 1;
    drop(state);
    vendor.changed.notify_all();
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
        let mut child = anyagents_harness::spawn_child(&mut command)
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
        // A session this process created holds nothing of an earlier one.
        self.begin_usage(&id, Some(TurnUsage::default()));
        if let Some(stale) = stale_session {
            self.sync_config_recovery_instructions();
            let mut state = self.lock();
            if state.recovered_from_session.is_none() {
                state.recovered_from_session = Some(stale.to_string());
            }
            let consumed = state.consumed.clone();
            let mut dropped = Vec::new();
            state.prompts.retain(|msg_id, prompt| {
                let clear = matches!(prompt, Prompt::Admitted | Prompt::Unknown)
                    && !consumed.contains(&vendor_message_id(msg_id));
                if clear {
                    dropped.push(msg_id.clone());
                }
                !clear
            });
            if !dropped.is_empty() {
                state
                    .history
                    .retain(|prior| !dropped.iter().any(|id| id == &prior.id));
            }
            state.turn_open = false;
        }
        Ok(id)
    }

    /// The session's own running totals, as the service reports them.
    fn session_totals(&self, session: &str) -> Option<TurnUsage> {
        let info = self
            .call(
                "GET",
                &format!("/api/session/{session}"),
                None,
                Some(CALL_TIMEOUT),
            )
            .ok()?
            .get("data")
            .cloned()?;
        let tokens = info.get("tokens").cloned().unwrap_or(Value::Null);
        let number = |key: &str| tokens.get(key).and_then(Value::as_u64).unwrap_or(0);
        Some(TurnUsage {
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
        })
    }

    /// This process begins serving `session`, whose totals are `base` now:
    /// what it serves from here is counted from them, on top of what it
    /// served in sessions before. Unknown totals are taken at the first
    /// reading, so an earlier process's usage is never counted as this one's.
    fn begin_usage(&self, session: &str, base: Option<TurnUsage>) {
        let mut state = self.lock();
        if state
            .usage_base
            .as_ref()
            .is_some_and(|(serving, _)| serving == session)
        {
            return;
        }
        state.usage_earlier = state.cached_usage.clone();
        state.usage_base = base.map(|base| (session.to_string(), base));
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
        // before this request. If the session already holds it in its
        // conversation without announcing it on this process's event stream,
        // an earlier process applied the line.
        let existed = record
            .pointer("/time/created")
            .and_then(Value::as_i64)
            .is_some_and(|created| created < sent_at);
        if (existed || replay)
            && !self.lock().consumed.contains(&id)
            && self
                .call(
                    "GET",
                    &format!("/api/session/{session}/message/{id}"),
                    None,
                    None,
                )
                .is_ok_and(|found| {
                    found.pointer("/data/id").and_then(Value::as_str) == Some(id.as_str())
                })
            && !self.lock().consumed.contains(&id)
        {
            if replay {
                let mut state = self.lock();
                state.cancelled.remove(&id);
                state.consumed.insert(id);
                return Ok(Prompt::Admitted);
            }
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
    let outcome =
        if outcome == Prompt::Unknown && state.consumed.contains(&vendor_message_id(&message.id)) {
            Prompt::Admitted
        } else {
            outcome
        };
    if outcome == Prompt::Admitted {
        if state.initial_topology.is_none()
            && let Some(topo) = &message.context.topology
        {
            state.initial_topology = Some(topo.clone());
        }
        state.recovered_from_session = None;
        if !state.history.iter().any(|prior| prior.id == message.id) {
            state.history.push(PriorPrompt {
                id: message.id.clone(),
                sender: message
                    .sender
                    .clone()
                    .or_else(|| message.context.sender.clone()),
                text: message.text.clone(),
            });
        }
        if !state.written.contains(&message.id) {
            state.written.push(message.id.clone());
        }
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
        let has_unproven = state.prompts.iter().any(|(id, prompt)| {
            let vid = vendor_message_id(id);
            matches!(prompt, Prompt::Admitted)
                && !state.consumed.contains(&vid)
                && !state.cancelled.contains(&vid)
        });
        if has_unproven {
            state.settling += 1;
            drop(state);
            settle_unproven(vendor, true);
            state = vendor.lock();
            if !state.alive || state.closing {
                state.watching = false;
                return;
            }
            if state.admissions != admitted {
                continue;
            }
        }
        downgrade_unconsumed_admitted(&mut state);
        state.watching = false;
        state.turn_open = false;
        if !state.turn_errored {
            state.turns_completed = state.turns_completed.saturating_add(1);
        }
        state.epoch += 1;
        drop(state);
        vendor.changed.notify_all();
        return;
    }
}

impl Harness for OpenCodeHarness {
    /// Write one mailbox line into the session's inbox as a steer that
    /// carries the line's own id, and accept it once the session consumed it.
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let mut state = self.vendor.lock();
        let vid = vendor_message_id(&message.id);
        // What the service said stands even after the process went.
        match state.prompts.get(&message.id) {
            Some(Prompt::Applied) => return Ok(DeliverOutcome::Duplicate),
            Some(Prompt::Rejected(reason)) => {
                // A 409 names this line's id, held by another record; a size
                // refusal is this line's own. A 400 may be every line's.
                let message_specific = reason.contains("(HTTP 409)")
                    || reason.contains("(HTTP 413)")
                    || anyagents_harness::names_message_size(reason);
                return Ok(DeliverOutcome::rejected(reason.clone(), message_specific));
            }
            // Wait for the prompt call itself to finish while the service
            // runs, so the send slot and admission state settle before the
            // mailbox advances to the next line.
            Some(Prompt::Sending) if state.alive => {}
            _ if state.consumed.contains(&vid) => {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                });
            }
            _ => {}
        }
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        if let Some(fault) = state.events_fault.clone() {
            return Err(HarnessError::Failed(fault));
        }
        let replay = match state.prompts.get(&message.id) {
            Some(Prompt::Sending) => return Ok(DeliverOutcome::InFlight),
            Some(Prompt::Admitted) if state.cancelled.contains(&vid) => {
                // The service dropped it unconsumed: it goes back to the
                // mailbox and is written again on its next offer.
                state.cancelled.remove(&vid);
                state.prompts.remove(&message.id);
                state.history.retain(|prior| prior.id != message.id);
                return Ok(DeliverOutcome::Deferred);
            }
            // Written; its proof is awaited.
            Some(Prompt::Admitted) => return Ok(DeliverOutcome::InFlight),
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
        // One prompt call at a time — and none while unproven admissions are
        // settling — keeps the inbox in mailbox order.
        if state.settling > 0 || state.sending.is_some() {
            return Ok(DeliverOutcome::Deferred);
        }
        if !state.turn_open {
            state.turn_errored = false;
            state.quota_reset_at_ms = None;
            state.sign_in_fault = None;
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

    /// What the session consumed, as this process's event stream or the
    /// conversation proved it, or (as a record already in the conversation)
    /// an earlier process applied. Read only.
    fn proven(&self, message_id: &str) -> Option<Proof> {
        let state = self.vendor.lock();
        match state.prompts.get(message_id) {
            Some(Prompt::Applied) => Some(Proof::Duplicate),
            Some(Prompt::Rejected(_)) => None,
            _ if state.consumed.contains(&vendor_message_id(message_id)) => Some(Proof::Taken {
                turn_finished: false,
            }),
            _ => None,
        }
    }

    /// Wait for any in-flight prompt call (bounded by [`CALL_TIMEOUT`]), stop the
    /// service, and drain its event stream so proofs remain readable.
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
        let state = self.vendor.lock();
        let _state = self
            .vendor
            .changed
            .wait_timeout_while(state, EVENTS_DRAIN, |state| state.events_reading)
            .unwrap_or_else(|err| err.into_inner());
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        let alive = self.vendor.lock().alive;
        alive.then_some(()).ok_or(HarnessError::Dead)
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
        let state = self.vendor.lock();
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        let Some(session) = state.session.clone() else {
            return Ok(());
        };
        drop(state);
        self.vendor
            .call(
                "POST",
                &format!("/api/session/{session}/interrupt"),
                None,
                Some(CALL_TIMEOUT),
            )
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let mut state = self.vendor.lock();
        state.turn_open = false;
        downgrade_unconsumed_admitted(&mut state);
        state.epoch += 1;
        drop(state);
        self.vendor.changed.notify_all();
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
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: state.sign_in_fault.clone(),
        }
    }

    fn turns_served(&self) -> u32 {
        self.vendor.lock().turns_completed
    }

    /// What this process served: the session's running totals, read from
    /// the session once per finished turn, less what the session held when
    /// this process began serving it (the session outlives a process, and the
    /// host counts from each process's start). The host records the
    /// difference.
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
        let Some(total) = self.vendor.session_totals(&session) else {
            return self.vendor.lock().cached_usage.clone();
        };
        let mut state = self.vendor.lock();
        let base = match &state.usage_base {
            Some((serving, base)) if *serving == session => base.clone(),
            _ => {
                // Totals read before this process began counting are none of
                // its own.
                state.usage_earlier = state.cached_usage.clone();
                state.usage_base = Some((session.clone(), total.clone()));
                total.clone()
            }
        };
        let usage = plus(&state.usage_earlier, &total.difference(base));
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
    /// arrives on stdout: a proof is an event on its event stream, which
    /// `stop` reads to the end after this.
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

    /// Classify non-2xx `POST /api/session/{id}/prompt` responses.
    fn refusal(&self) -> Refusal {
        match self.status {
            Some(404) => Refusal::SessionGone,
            Some(400 | 409 | 413 | 422) => Refusal::Permanent,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn vendor_with_session(session: &str) -> Arc<Vendor> {
        let dir = std::env::temp_dir().join("opencode-test");
        Arc::new(Vendor {
            program: PathBuf::from("opencode"),
            config_dir: Some(dir.clone()),
            work_dir: Some(dir),
            environment: Vec::new(),
            state: Mutex::new(State {
                session: Some(session.to_string()),
                alive: true,
                launch: 1,
                turn_open: true,
                admissions: 1,
                ..State::default()
            }),
            changed: Condvar::new(),
        })
    }

    fn health_of(vendor: &Vendor) -> HarnessHealth {
        let state = vendor.lock();
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.turn_open || state.sending.is_some() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: DELIVERY.with_peer(state.peer.clone()),
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: state.sign_in_fault.clone(),
        }
    }

    #[test]
    fn on_event_captures_ai_error_and_prevents_turn_completion_count() {
        let vendor = vendor_with_session("ses_1");
        let event = json!({
            "type": "session.error",
            "data": {
                "sessionID": "ses_1",
                "error": {
                    "name": "AI.Error",
                    "data": {
                        "message": "API Error: 401 unauthorized key",
                        "statusCode": 401
                    }
                }
            }
        });
        on_event(&vendor, 1, &event, true);

        let health = health_of(&vendor);
        assert_eq!(
            health.sign_in_fault.as_deref(),
            Some("API Error: 401 unauthorized key")
        );
        assert_eq!(health.quota_reset_at_ms, None);

        {
            let mut state = vendor.lock();
            assert!(state.turn_errored);
            state.watching = false;
            state.turn_open = false;
            if !state.turn_errored {
                state.turns_completed = state.turns_completed.saturating_add(1);
            }
            assert_eq!(state.turns_completed, 0);
        }
    }

    #[test]
    fn on_event_captures_quota_exceeded_reset_time_and_ignores_other_sessions() {
        let vendor = vendor_with_session("ses_target");
        // Other session's error must be ignored.
        let other = json!({
            "type": "QuotaExceeded",
            "data": {
                "sessionID": "ses_other",
                "resets_at_ms": 1_800_000_000_000i64,
                "message": "quota exceeded"
            }
        });
        assert!(!on_event(&vendor, 1, &other, true));
        assert!(health_of(&vendor).quota_reset_at_ms.is_none());

        let mine = json!({
            "type": "QuotaExceeded",
            "data": {
                "sessionID": "ses_target",
                "resets_at_ms": 1_850_000_000_000i64,
                "message": "Quota exceeded. Resets in 2h."
            }
        });
        on_event(&vendor, 1, &mine, true);
        let health = health_of(&vendor);
        assert_eq!(health.quota_reset_at_ms, Some(1_850_000_000_000i64));
        assert!(vendor.lock().turn_errored);

        // Fallback AI.Error event with top-level type + retry-after header in error data.
        let vendor2 = vendor_with_session("ses_2");
        let retry_ev = json!({
            "type": "AI.Error",
            "data": {
                "sessionID": "ses_2",
                "message": "429 rate limit exceeded",
                "statusCode": 429,
                "retry_after": 30
            }
        });
        on_event(&vendor2, 1, &retry_ev, true);
        let health2 = health_of(&vendor2);
        assert!(health2.quota_reset_at_ms.is_some());
        assert!(vendor2.lock().turn_errored);
    }
}
