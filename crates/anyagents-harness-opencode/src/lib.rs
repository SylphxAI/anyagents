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
    shared: Arc<Mutex<State>>,
    steer: Option<SteerQueue>,
}

#[derive(Default)]
struct State {
    session: Option<String>,
    alive: bool,
    /// Ids the prompt API took, in the order they were written.
    written: Vec<String>,
    /// Turns the session reported failed, by message id.
    failures: Vec<(String, String)>,
    /// The service this adapter started, and the password it printed. A
    /// separate `opencode api` must be pointed at that address, or it talks to
    /// whatever service the machine happens to run.
    service: Option<String>,
    password: Option<String>,
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
        Self::start_with(program, session_id, environment, work_dir)
    }

    fn start_with(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let mut harness = Self {
            program: program.into(),
            config_dir: None,
            work_dir: work_dir.map(Path::to_path_buf),
            environment: environment
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            child: None,
            shared: Arc::new(Mutex::new(State::default())),
            steer: None,
        };
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
        let mut harness = Self::start_with(program, session_id, environment, work_dir)?;
        harness.config_dir = Some(config_dir.to_path_buf());
        harness.remember_session();
        Ok(harness)
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
        // The service prints its address and password on stdout, so it is read
        // here and every later call is pointed at exactly this one.
        let mut command = anyagents_harness::spawn_command(&self.program, &self.environment);
        command
            .args(["serve", "--port", "0"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        for (key, value) in &self.environment {
            command.env(key, value);
        }
        let mut child = command
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        if let Some(stdout) = child.stdout.take() {
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
                let Ok(line) = heard.recv_timeout(left.min(Duration::from_millis(500))) else {
                    continue;
                };
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("server listening on ") {
                    self.lock().service = Some(rest.trim_end_matches('/').to_string());
                }
                if let Some(password) = line.strip_prefix("server password ") {
                    self.lock().password = Some(password.trim().to_string());
                }
                if self.lock().service.is_some() && self.lock().password.is_some() {
                    break;
                }
            }
        }
        self.child = Some(child);
        self.lock().alive = true;
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
        let usable = match &wanted {
            Some(session) => {
                let mut state = Missing::Unknown;
                for _ in 0..5 {
                    match self.api("GET", &format!("/api/session/{session}"), None) {
                        Ok(_) => {
                            state = Missing::Present;
                            break;
                        }
                        Err(err) => {
                            let text = err.to_string();
                            state = if text.contains("404")
                                || text.contains("not found")
                                || text.contains("NotFound")
                            {
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
                state == Missing::Present
            }
            None => false,
        };
        if !usable {
            if let Some(session) = &wanted {
                eprintln!(
                    "anyagents-opencode: session {session} is gone from this agent's store; starting a new one"
                );
            }
            let fresh = self.api("POST", "/api/session", Some(json!({})))?;
            if let Some(id) = fresh.pointer("/data/id").and_then(Value::as_str) {
                self.lock().session = Some(id.to_string());
            }
        }
        self.remember_session();
        Ok(())
    }

    /// Keep this agent's session id beside its config, so the next start
    /// resumes this exact conversation.
    fn remember_session(&self) {
        let (Some(dir), Some(session)) = (self.config_dir.clone(), self.lock().session.clone())
        else {
            return;
        };
        let _ = std::fs::write(dir.join("opencode-session-id"), format!("{session}\n"));
    }

    /// One `opencode api` call. Its own process, so a hung vendor call cannot
    /// hold the adapter, and a failure names the call and never a secret.
    fn api(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value, HarnessError> {
        let mut command = anyagents_harness::spawn_command(&self.program, &self.environment);
        // `opencode api --server <url> <METHOD> <path>`, as the CLI reads it:
        // the subcommand comes first, then its flags.
        command.arg("api");
        if let Some(service) = self.lock().service.clone() {
            command.args(["--server", &service]);
        }
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
        for (key, value) in &self.environment {
            command.env(key, value);
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
                    let reason = text.lines().next().unwrap_or("no reason").to_string();
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
        // An id already written is not written again: a replay after a crash
        // must not prompt the agent twice.
        if self.lock().written.iter().any(|one| one == &message.id) {
            return Ok(DeliverOutcome::Accepted {
                turn_finished: false,
            });
        }
        let Some(session) = self.lock().session.clone() else {
            return Err(HarnessError::Failed("no session".into()));
        };
        let body = json!({
            "text": compose_message(&message.id, &message.context, &message.text),
            "delivery": "steer",
        });
        let answer = self.api(
            "POST",
            &format!("/api/session/{session}/prompt"),
            Some(body),
        )?;
        if answer.get("data").is_none() {
            return Err(HarnessError::Failed(format!(
                "prompt was not taken: {answer}"
            )));
        }
        self.lock().written.push(message.id.clone());
        Ok(DeliverOutcome::Accepted {
            turn_finished: false,
        })
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.lock().session = Some(session.to_string());
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let Some(session) = self.lock().session.clone() else {
            return Ok(());
        };
        let _ = self.api(
            "POST",
            &format!("/api/session/{session}/interrupt"),
            Some(json!({})),
        );
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let state = self.lock();
        HarnessHealth {
            process_alive: state.alive,
            // The adapter never holds a turn open: a line is written and
            // forgotten, and the session runs it. What a line is doing lives in
            // the session's own inbox, not here.
            turn_state: TurnState::Idle,
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    /// The session's own running totals, read from the session itself rather
    /// than from anything this adapter kept. The host records the difference.
    fn served_usage(&self) -> TurnUsage {
        let Some(session) = self.lock().session.clone() else {
            return TurnUsage::default();
        };
        let Ok(info) = self.api("GET", &format!("/api/session/{session}"), None) else {
            return TurnUsage::default();
        };
        let Some(info) = info.get("data") else {
            return TurnUsage::default();
        };
        let tokens = info.get("tokens").cloned().unwrap_or(Value::Null);
        let number = |key: &str| tokens.get(key).and_then(Value::as_u64).unwrap_or(0);
        TurnUsage {
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
        }
    }

    fn poll_steer(&mut self) -> Result<Option<SteerItem>, HarnessError> {
        let Some(queue) = &self.steer else {
            return Ok(None);
        };
        Ok(queue.take())
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
        let Some(session) = self.lock().session.clone() else {
            return Ok(false);
        };
        // The same prompt `deliver` writes: the session decides where it goes.
        let body = json!({
            "text": compose_message(&message.id, &message.context, &message.text),
            "delivery": "steer"
        });
        match self.api(
            "POST",
            &format!("/api/session/{session}/prompt"),
            Some(body),
        ) {
            Ok(answer) => {
                if answer.get("data").is_some() {
                    self.lock().written.push(message.id.clone());
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

impl Drop for OpenCodeHarness {
    fn drop(&mut self) {
        self.lock().alive = false;
        if let Some(mut child) = self.child.take() {
            // The exact child, never a pattern or a group: a shared desk refuses
            // a group signal it does not own, and it is right to.
            anyagents_harness::kill_pid(child.id(), "-TERM", &self.environment);
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
        }
    }
}

/// What the service said about a stored session.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Missing {
    Present,
    Gone,
    Unknown,
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
