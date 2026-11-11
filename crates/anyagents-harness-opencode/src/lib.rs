//! OpenCode adapter.
//!
//! `opencode serve` is an HTTP server, not a line protocol on stdin. The desk's
//! own worker drives it through `opencode api`, and so does this adapter:
//!
//! - the session is a fixed id (`--session`), resumed across restarts;
//! - a mailbox line is a prompt: `POST /api/session/{id}/prompt` with the text
//!   `anyagents-message:<id>` on its first line and `delivery: "steer"`;
//! - the session takes the prompt as accepted as soon as the API answers with
//!   `data`, and its own inbox decides whether that joins the running turn or
//!   starts the next one. The host does not wait for a turn to end: there is
//!   nothing here to wait for, and a mailbox that stopped until an idle would
//!   stop for good one the frame that carries it was missed;
//! - the answer is the assistant text that followed the prompt, which the agent
//!   itself usually sends through its MCP `send` tool.
//!
//! The mailbox id travels in the prompt text, so the agent's own `send` (its
//! MCP tool) is the reply path and the server records it against the same
//! agent. Nothing here reads a credential: the service's address and password
//! come from the private service this adapter starts for this agent.

mod port;

use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage, SteerItem,
    SteerQueue, SteerSupport, TurnState, TurnUsage, compose_message,
};
use serde_json::{Value, json};

/// How long one API call may take.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the adapter waits for a started service to announce itself.
///
/// This wait runs inside `agents.create`/server start, so it is a hard cap: a
/// service that stays silent is reported as not started, and the server binds
/// its socket anyway rather than waiting on a vendor.
const START_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the adapter waits for the started service's API to answer.
const READY_TIMEOUT: Duration = Duration::from_secs(20);

/// One long-lived OpenCode service and the session it serves.
pub struct OpenCodeHarness {
    program: PathBuf,
    /// The agent's config directory; its session id is kept here.
    config_dir: Option<PathBuf>,
    work_dir: Option<PathBuf>,
    environment: Vec<(String, String)>,
    child: Option<Child>,
    port: Option<port::PortLease>,
    shared: Arc<Mutex<State>>,
    steer: Option<SteerQueue>,
}

#[derive(Clone, Debug)]
struct PriorPrompt {
    id: String,
    sender: Option<String>,
    text: String,
}

#[derive(Default)]
struct State {
    session: Option<String>,
    alive: bool,
    /// Ids the prompt API took, in the order they were written.
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
    /// Turns the session reported failed, by message id.
    failures: Vec<(String, String)>,
    /// The service this adapter started, and the password it printed. A
    /// separate `opencode api` must be pointed at that address, or it talks to
    /// whatever service the machine happens to run.
    service: Option<String>,
    password: Option<String>,
    turn_open: bool,
    last_prompt_at: Option<Instant>,
    turns_completed: u32,
    last_usage_poll: Option<Instant>,
    last_usage_written: usize,
    cached_usage: TurnUsage,
    quota_reset_at_ms: Option<i64>,
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
        let mut harness = Self {
            program: program.into(),
            config_dir: config_dir.map(Path::to_path_buf),
            work_dir: work_dir.map(Path::to_path_buf),
            environment: environment
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            child: None,
            port: None,
            shared: Arc::new(Mutex::new(State::default())),
            steer: None,
        };
        if let Some(dir) = config_dir {
            // Override inherited host roots, including OpenCode's explicit
            // config override, before any vendor command can discover a service.
            for (key, value) in [
                ("XDG_CONFIG_HOME", dir.to_path_buf()),
                ("OPENCODE_CONFIG_DIR", dir.join("opencode")),
                ("XDG_DATA_HOME", dir.join("data-home")),
                ("XDG_STATE_HOME", dir.join("state-home")),
            ] {
                harness.environment.retain(|(name, _)| name != key);
                harness
                    .environment
                    .push((key.into(), value.display().to_string()));
            }
        }
        harness.lock().session = session_id.map(str::to_string);
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

    /// Also watch `queue` for lines a channel pushed between model steps.
    pub fn with_steer(mut self, queue: SteerQueue) -> Self {
        self.steer = Some(queue);
        self
    }

    /// Ids the prompt API took, in the order they were written.
    pub fn written_ids(&self) -> Vec<String> {
        self.lock().written.clone()
    }
    pub fn session_id(&self) -> Option<String> {
        self.lock().session.clone()
    }
    /// Whether the prompt API has taken `id`.
    pub fn seen(&self, id: &str) -> bool {
        self.lock().written.iter().any(|written| written == id)
    }
    pub fn wait_seen(&self, id: &str, timeout: Duration) -> bool {
        self.wait(timeout, |state| {
            state.written.iter().any(|written| written == id)
        })
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
            let _ = self.poll();
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    /// The answer the session holds for `id`, as read from the session now.
    pub fn answer(&self, id: &str) -> Option<String> {
        self.answer_in_session(id)
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
                self.config_dir.as_deref(),
                previous,
            )?);
            let port = self.port.as_ref().expect("allocated port").port;
            let (announced, collision) = self.launch_service(port)?;
            if announced {
                return self.resume_session();
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
    ) -> Result<(bool, std::sync::mpsc::Receiver<bool>), HarnessError> {
        let mut command = anyagents_harness::spawn_command(&self.program, &self.environment)?;
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
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        // Inherited credentials suppress the password announcement. Generate a
        // fresh child credential instead; only API children receive it below.
        command
            .env_remove("OPENCODE_PASSWORD")
            .env_remove("OPENCODE_SERVER_PASSWORD");
        self.port.as_mut().expect("allocated port").handoff();
        let mut child = command
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        self.write_service_pid(child.id());
        let collision = port::watch_collision(child.stderr.take().expect("piped stderr"));
        self.child = Some(child);
        if let Some(stdout) = self.child.as_mut().and_then(|child| child.stdout.take()) {
            use std::io::{BufRead, BufReader, Read};
            use std::sync::mpsc;
            // A service that never announces itself must not wedge the
            // adapter, so the lines are read on their own thread and waited
            // for with a deadline.
            let (lines, heard) = mpsc::channel();
            let mut reader = BufReader::new(stdout);
            std::thread::spawn(move || {
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
                // Keep draining so the child never blocks on a full pipe.
                let mut sink = reader;
                let mut buffer = [0u8; 4096];
                while sink.read(&mut buffer).unwrap_or(0) > 0 {}
            });
            let deadline = Instant::now() + START_TIMEOUT;
            while Instant::now() < deadline {
                let left = deadline.saturating_duration_since(Instant::now());
                let line = match heard.recv_timeout(left.min(Duration::from_millis(500))) {
                    Ok(line) => line,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                };
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("server listening on ") {
                    let endpoint = rest.trim_end_matches('/');
                    if endpoint == format!("http://127.0.0.1:{port}") {
                        self.lock().service = Some(endpoint.to_string());
                    }
                }
                if let Some(password) = line.strip_prefix("server password ") {
                    self.lock().password = Some(password.trim().to_string());
                }
                if self.lock().service.is_some() && self.lock().password.is_some() {
                    break;
                }
            }
        }
        let announced = self.lock().service.is_some() && self.lock().password.is_some();
        self.lock().alive = announced;
        Ok((announced, collision))
    }

    fn resume_session(&mut self) -> Result<(), HarnessError> {
        let ready = Instant::now() + READY_TIMEOUT;
        let mut up = false;
        while Instant::now() < ready {
            if self.api("GET", "/api/session", None).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        if !up {
            return Err(HarnessError::Failed(
                "the OpenCode service did not answer; the session is untouched".into(),
            ));
        }
        // The stored session is resumed. A new one is created only when it is
        // truly gone from this agent's own store, and that is said out loud so
        // a lost conversation is never silent.
        let wanted = self.lock().session.clone();
        // A stored session is resumed unless the service clearly says it does
        // not exist. A transient failure is retried, never read as "gone": a
        // new session there would strand the answers already in the old one.
        let usable =
            match &wanted {
                Some(session) => {
                    let mut state = Missing::Unknown;
                    for _ in 0..5 {
                        match self.api("GET", &format!("/api/session/{session}"), None) {
                            Ok(_) => {
                                state = Missing::Present;
                                break;
                            }
                            Err(err) => {
                                state = if is_session_not_found(&err) {
                                    Missing::Gone
                                } else {
                                    Missing::Unknown
                                };
                                if state == Missing::Gone {
                                    break;
                                }
                                std::thread::sleep(Duration::from_millis(300));
                            }
                        }
                    }
                    match state {
                        Missing::Present => true,
                        Missing::Gone => false,
                        Missing::Unknown => return Err(HarnessError::Failed(
                            "OpenCode session availability is unknown; refusing a new conversation"
                                .into(),
                        )),
                    }
                }
                None => false,
            };
        if !usable {
            self.recreate_session(wanted.as_deref())?;
        } else {
            self.remember_session()?;
        }
        if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() {
            // OpenCode boots location services and MCP servers lazily on the first
            // directory-scoped call (`GET /api/mcp` or `POST /api/session/<id>/prompt`).
            // Poll `GET /api/mcp` briefly so the `anyagents` MCP server is connected
            // before the very first turn builds its tool catalog.
            for _ in 0..15 {
                let Ok(mcp) = self.api("GET", "/api/mcp", None) else {
                    break;
                };
                let Some(items) = mcp.get("data").and_then(Value::as_array) else {
                    break;
                };
                if items.iter().any(|item| {
                    item.get("name").and_then(Value::as_str) == Some("anyagents")
                        && item.pointer("/status/status").and_then(Value::as_str)
                            == Some("connected")
                }) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        Ok(())
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
    fn recreate_session(&mut self, stale_session: Option<&str>) -> Result<String, HarnessError> {
        if let Some(session) = stale_session {
            eprintln!(
                "anyagents-opencode: session {session} is gone from this agent's store; starting a new one"
            );
        }
        let fresh = self.api("POST", "/api/session", Some(json!({})))?;
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

    fn send_prompt(&mut self, message: &OutboundMessage) -> Result<Value, HarnessError> {
        let Some(session) = self.lock().session.clone() else {
            return Err(HarnessError::Failed("no session".into()));
        };
        let pending_recovery = self.lock().recovered_from_session.clone();
        let initial_text = match pending_recovery.as_deref() {
            Some(stale) => self.compose_prompt(message, Some((stale, &session))),
            None => self.compose_prompt(message, None),
        };
        let body = json!({
            "text": initial_text,
            "delivery": "steer",
        });
        match self.api(
            "POST",
            &format!("/api/session/{session}/prompt"),
            Some(body),
        ) {
            Ok(answer) => Ok(answer),
            Err(err) if is_session_not_found(&err) => {
                let stale = pending_recovery.unwrap_or_else(|| session.clone());
                let new_session = self.recreate_session(Some(&stale))?;
                let retry_body = json!({
                    "text": self.compose_prompt(message, Some((&stale, &new_session))),
                    "delivery": "steer",
                });
                self.api(
                    "POST",
                    &format!("/api/session/{new_session}/prompt"),
                    Some(retry_body),
                )
            }
            Err(err) => Err(err),
        }
    }

    fn record_prompt_accepted(&self, message: &OutboundMessage) {
        let mut state = self.lock();
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
        state.turn_open = true;
        state.last_prompt_at = Some(Instant::now());
    }

    /// One `opencode api` call. Its own process, so a hung vendor call cannot
    /// hold the adapter, and a failure names the call and never a secret.
    fn api(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value, HarnessError> {
        let mut command = anyagents_harness::spawn_command(&self.program, &self.environment)?;
        // `opencode api --server <url> <METHOD> <path>`, as the CLI reads it:
        // the subcommand comes first, then its flags.
        command.arg("api");
        let service = self.lock().service.clone().ok_or_else(|| {
            HarnessError::Failed(
                "OpenCode private endpoint is unavailable; refusing service discovery".into(),
            )
        })?;
        command.args(["--server", &service]);
        command.args([method, path]);
        if let Some(body) = &body {
            command.args(["-d", &body.to_string()]);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        if let Some(password) = self.lock().password.clone() {
            command.env("OPENCODE_PASSWORD", password);
        }
        // A vendor that never answers is a failure after `CALL_TIMEOUT`, not a
        // wedged adapter.
        let mut child = command
            .spawn()
            .map_err(|err| HarnessError::Failed(format!("{method} {path}: {err}")))?;
        let deadline = Instant::now() + CALL_TIMEOUT;
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                let output = child
                    .wait_with_output()
                    .map_err(|err| HarnessError::Failed(format!("{method} {path}: {err}")))?;
                if !status.success() {
                    let text = String::from_utf8_lossy(&output.stderr);
                    let error_body = serde_json::from_slice::<Value>(&output.stdout).ok();
                    let reason = if error_body
                        .as_ref()
                        .and_then(|body| body.pointer("/error/code").or_else(|| body.get("code")))
                        .and_then(Value::as_u64)
                        == Some(404)
                        || error_body
                            .as_ref()
                            .and_then(|body| body.get("name"))
                            .and_then(Value::as_str)
                            == Some("NotFoundError")
                    {
                        "resource not found (404)".to_string()
                    } else if let Some(line) = text.lines().map(str::trim).find(|l| !l.is_empty()) {
                        line.to_string()
                    } else if let Some(body) = &error_body {
                        let code = body
                            .pointer("/error/code")
                            .or_else(|| body.get("code"))
                            .and_then(Value::as_u64);
                        let msg = body
                            .pointer("/error/message")
                            .or_else(|| body.get("message"))
                            .or_else(|| body.get("error"))
                            .and_then(Value::as_str)
                            .unwrap_or("request failed");
                        match code {
                            Some(code) => format!("{msg} ({code})"),
                            None => msg.to_string(),
                        }
                    } else {
                        "no reason".to_string()
                    };
                    return Err(HarnessError::Failed(format!("{method} {path}: {reason}")));
                }
                return serde_json::from_slice(&output.stdout)
                    .map_err(|err| HarnessError::Failed(format!("{method} {path}: {err}")));
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(HarnessError::Failed(format!(
                    "{method} {path}: no answer within {}s",
                    CALL_TIMEOUT.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Read the session's newest messages and record a turn it reported failed.
    /// A delivery never waits for this: the session takes every prompt on its
    /// own, and the answer is read from the session when it is asked for.
    fn poll(&self) -> Result<(), HarnessError> {
        let Some(session) = self.lock().session.clone() else {
            return Ok(());
        };
        let listing = self.api("GET", &format!("/api/session/{session}/message"), None)?;
        let Some(messages) = listing.get("data").and_then(Value::as_array) else {
            return Ok(());
        };
        // Only the newest turn is read: the listing is newest first, and an
        // older idle frame belongs to a turn that is already settled.
        let Some(outcome) = messages.iter().find_map(|message| {
            (message.get("type").and_then(Value::as_str) == Some("idle"))
                .then(|| {
                    message
                        .get("outcome")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .flatten()
        }) else {
            return Ok(());
        };
        if outcome != "failed" && outcome != "interrupted" {
            return Ok(());
        }
        // The idle frame marks the end of the newest turn; every line the
        // session has taken and not answered belongs to it.
        let mut state = self.lock();
        for id in state.written.clone() {
            if !state.failures.iter().any(|(item, _)| item == &id) {
                state.failures.push((id, outcome.clone()));
            }
        }
        Ok(())
    }

    /// The answer the session holds for this mailbox id: the assistant message
    /// that follows the user message carrying `anyagents-message:<id>`.
    ///
    /// The session lists messages newest first, so the answer is the first
    /// assistant message *before* that user message in that order. Reasoning
    /// parts are never the answer.
    pub fn answer_in_session(&self, id: &str) -> Option<String> {
        let session = self.lock().session.clone()?;
        let listing = self
            .api("GET", &format!("/api/session/{session}/message"), None)
            .ok()?;
        answer_of(listing.get("data")?.as_array()?, id)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.shared.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Wait until `ready` holds for the state, polling the session.
    fn wait(&self, timeout: Duration, ready: impl Fn(&State) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if ready(&self.lock()) {
                return true;
            }
            if Instant::now() >= deadline {
                return ready(&self.lock());
            }
            let _ = self.poll();
            if ready(&self.lock()) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

impl Harness for OpenCodeHarness {
    /// Write one mailbox line into the session's inbox as a prompt with
    /// `delivery: "steer"`, and accept it when the prompt API answers with
    /// `data`.
    ///
    /// The session owns the turn: the prompt joins the running turn or starts
    /// the next one, whichever its inbox decides. Nothing here waits for a turn
    /// to end, so a missed idle frame cannot hold every line behind it.
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        if !self.lock().alive {
            return Err(HarnessError::Dead);
        }
        if let Some(reset_at_ms) = self.lock().quota_reset_at_ms {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            if reset_at_ms > now_ms {
                return Ok(DeliverOutcome::Quota { reset_at_ms });
            }
            self.lock().quota_reset_at_ms = None;
        }
        // An id already written is not written again: a replay after a crash
        // must not prompt the agent twice.
        if self.lock().written.iter().any(|one| one == &message.id) {
            return Ok(DeliverOutcome::Accepted {
                turn_finished: false,
            });
        }
        let answer = self.send_prompt(message)?;
        if answer.get("data").is_none() {
            return Err(HarnessError::Failed(format!(
                "prompt was not taken: {answer}"
            )));
        }
        self.record_prompt_accepted(message);
        Ok(DeliverOutcome::Accepted {
            turn_finished: false,
        })
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let previous = self.lock().session.replace(session.to_string());
        if let Err(err) = self.remember_session() {
            self.lock().session = previous;
            return Err(err);
        }
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let Some(session) = self.lock().session.clone() else {
            return Ok(());
        };
        self.api(
            "POST",
            &format!("/api/session/{session}/interrupt"),
            Some(json!({})),
        )?;
        self.lock().turn_open = false;
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let state = self.lock();
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.turn_open && state.quota_reset_at_ms.is_none() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }

    fn clear_quota(&mut self) {
        self.lock().quota_reset_at_ms = None;
    }

    fn turns_served(&self) -> u32 {
        self.lock().turns_completed
    }

    /// The session's own running totals, read from the session itself rather
    /// than from anything this adapter kept. The host records the difference.
    fn served_usage(&self) -> TurnUsage {
        let fake_mode = std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
            || self
                .environment
                .iter()
                .any(|(k, _)| k == "ANYAGENTS_FAKE_MODE");
        let (session, written_len) = {
            let state = self.lock();
            let Some(session) = state.session.clone() else {
                return TurnUsage::default();
            };
            if !fake_mode {
                if !state.turn_open || state.written.is_empty() {
                    return state.cached_usage.clone();
                }
                if state
                    .last_prompt_at
                    .is_some_and(|t| t.elapsed() < Duration::from_millis(500))
                {
                    return state.cached_usage.clone();
                }
                if state
                    .last_usage_poll
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(1))
                {
                    return state.cached_usage.clone();
                }
            }
            (session, state.written.len())
        };
        if !fake_mode
            && let Ok(active) = self.api("GET", "/api/session/active", None)
            && active
                .pointer(&format!("/data/{session}/type"))
                .and_then(Value::as_str)
                == Some("running")
        {
            let mut state = self.lock();
            state.last_usage_poll = Some(Instant::now());
            return state.cached_usage.clone();
        }
        let Ok(info) = self.api("GET", &format!("/api/session/{session}"), None) else {
            return self.lock().cached_usage.clone();
        };
        let Some(info) = info.get("data") else {
            return self.lock().cached_usage.clone();
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
        let mut state = self.lock();
        state.last_usage_poll = Some(Instant::now());
        if written_len > state.last_usage_written {
            state.turns_completed = state.turns_completed.saturating_add(1);
        }
        if state.written.len() == written_len {
            state.turn_open = false;
        }
        state.last_usage_written = written_len;
        state.cached_usage = usage.clone();
        usage
    }

    fn poll_steer(&mut self) -> Result<Option<SteerItem>, HarnessError> {
        let Some(queue) = &self.steer else {
            return Ok(None);
        };
        queue
            .try_take()
            .map_err(|err| HarnessError::Failed(format!("steer queue: {err}")))
    }

    fn inject(&mut self, message: &OutboundMessage) -> Result<bool, HarnessError> {
        // A mailbox line already written is not written a second time. The
        // prompt API told the session it has it, so this is a success.
        if self.seen(&message.id) {
            return Ok(true);
        }
        if !self.lock().alive {
            return Err(HarnessError::Dead);
        }
        if self.lock().session.is_none() {
            return Ok(false);
        }
        // The same prompt `deliver` writes: the session decides where it goes.
        match self.send_prompt(message) {
            Ok(answer) => {
                if answer.get("data").is_some() {
                    self.record_prompt_accepted(message);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    fn release_steer(&mut self, keep: &str) {
        if let Some(queue) = &self.steer {
            queue.release_for(keep);
        }
    }
}

const SERVICE_PID_FILE: &str = "opencode-service-pid";

impl OpenCodeHarness {
    fn write_service_pid(&self, pid: u32) {
        let Some(dir) = self.config_dir.as_deref() else {
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
        let Some(dir) = self.config_dir.as_deref() else {
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
        let Some(dir) = self.config_dir.as_deref() else {
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
        let Some(port) = self.config_dir.as_deref().and_then(|dir| {
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
        let children = direct_child_pids(pid);
        anyagents_harness::kill_pid(pid, "-TERM", &self.environment);
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline && (process_exists(pid) || self.stored_port_occupied()) {
            std::thread::sleep(Duration::from_millis(25));
        }
        if process_exists(pid) || self.stored_port_occupied() {
            anyagents_harness::kill_pid(pid, "-KILL", &self.environment);
            let kill_deadline = Instant::now() + Duration::from_millis(500);
            while Instant::now() < kill_deadline
                && (process_exists(pid) || self.stored_port_occupied())
            {
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        for child_pid in children {
            if child_pid > 1 && process_exists(child_pid) {
                anyagents_harness::kill_pid(child_pid, "-KILL", &self.environment);
            }
        }
    }

    fn stop_child(&mut self) {
        self.lock().alive = false;
        if let Some(mut child) = self.child.take() {
            let pid = child.id();
            let children = direct_child_pids(pid);
            // The exact child, never a pattern or a group: a shared desk refuses
            // a group signal it does not own, and it is right to.
            anyagents_harness::kill_pid(pid, "-TERM", &self.environment);
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    _ => {
                        anyagents_harness::kill_child(&mut child, &self.environment);
                        break;
                    }
                }
            }
            let _ = child.wait();
            for child_pid in children {
                if child_pid > 1 && process_exists(child_pid) {
                    anyagents_harness::kill_pid(child_pid, "-KILL", &self.environment);
                }
            }
            self.clear_service_pid(Some(pid));
        }
        self.lock().service = None;
        self.lock().password = None;
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

fn is_session_not_found(err: &HarnessError) -> bool {
    let text = err.to_string();
    let reason = text
        .split_once(": ")
        .map_or(text.as_str(), |(_, reason)| reason);
    let lower = reason.to_ascii_lowercase();
    reason.contains("404") || lower.contains("not found") || reason.contains("NotFound")
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
