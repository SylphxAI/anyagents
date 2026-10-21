//! OpenCode adapter.
//!
//! `opencode serve` is an HTTP server, not a line protocol on stdin. The desk's
//! own worker drives it through `opencode api`, and so does this adapter:
//!
//! - the session is a fixed id (`--session`), resumed across restarts;
//! - a mailbox line is a prompt: `POST /api/session/{id}/prompt` with the text
//!   `anyagents-message:<id>` on its first line and `delivery: "queue"` (or
//!   `"steer"` to join the running turn);
//! - the turn is over when the session reports an `idle` message with
//!   `outcome: "succeeded"`, or an assistant message whose `finish` is set;
//! - the answer is the assistant message's text.
//!
//! The mailbox id travels in the prompt text, so the agent's own `send` (its
//! MCP tool) is the reply path and the server records it against the same
//! agent. Nothing here reads a credential: `opencode api` uses the service the
//! machine already runs, or the private one this adapter starts.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage, SteerItem,
    SteerQueue, SteerSupport, TurnReport, TurnState, TurnUsage, marked_message,
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
    /// Ids handed to the prompt endpoint, in the order they were written.
    written: Vec<String>,
    /// Every id seen in an answer or an idle message.
    finished: HashSet<String>,
    /// The turn's answer and usage, by owning message id.
    answers: BTreeMap<String, String>,
    usages: BTreeMap<String, TurnUsage>,
    /// The message id the open turn belongs to.
    open: Vec<String>,
    /// When the current turn's prompt was accepted, so the poll reads only the
    /// messages after it. A session keeps its whole history; without this, an
    /// earlier turn's idle and answer would close this one.
    turn_since: Option<i64>,
    failure: Option<String>,
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

    pub fn written_ids(&self) -> Vec<String> {
        self.lock().written.clone()
    }
    pub fn turns_started(&self) -> usize {
        self.lock().open.len()
    }
    pub fn session_id(&self) -> Option<String> {
        self.lock().session.clone()
    }
    pub fn seen(&self, id: &str) -> bool {
        let state = self.lock();
        state.written.iter().any(|written| written == id) || state.finished.contains(id)
    }
    pub fn wait_seen(&self, id: &str, timeout: Duration) -> bool {
        self.wait(timeout, |state| {
            state.written.iter().any(|written| written == id) || state.finished.contains(id)
        })
    }
    pub fn wait_finished(&self, id: &str, timeout: Duration) -> bool {
        self.wait(timeout, |state| state.finished.contains(id))
    }
    pub fn answer(&self, id: &str) -> Option<String> {
        self.lock().answers.get(id).cloned()
    }
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        self.lock().usages.get(id).cloned()
    }
    pub fn turn_report_for(&self, id: &str) -> Option<TurnReport> {
        let state = self.lock();
        if let Some(reason) = &state.failure
            && state.finished.contains(id)
        {
            return Some(TurnReport::Failed(reason.clone()));
        }
        state.usages.get(id).cloned().map(TurnReport::Succeeded)
    }

    /// Start the service, wait until it answers, and resume the session.
    fn begin(&mut self) -> Result<(), HarnessError> {
        // The service prints its address and password on stdout, so it is read
        // here and every later call is pointed at exactly this one.
        let mut command = Command::new(&self.program);
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
        let mut command = Command::new(&self.program);
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
        let stdout_pipe = child.stdout.take();
        let stderr_pipe = child.stderr.take();
        let stdout_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(mut out) = stdout_pipe {
                let _ = std::io::Read::read_to_end(&mut out, &mut buf);
            }
            buf
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(mut err) = stderr_pipe {
                let _ = std::io::Read::read_to_end(&mut err, &mut buf);
            }
            buf
        });
        let deadline = Instant::now() + CALL_TIMEOUT;
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                let stdout = stdout_reader.join().unwrap_or_default();
                let stderr = stderr_reader.join().unwrap_or_default();
                if !status.success() {
                    let text = String::from_utf8_lossy(&stderr);
                    let reason = text.lines().next().unwrap_or("no reason").to_string();
                    return Err(HarnessError::Failed(format!("{method} {path}: {reason}")));
                }
                return serde_json::from_slice(&stdout)
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

    /// Read the session's messages and record what the newest turn says.
    fn poll(&self) -> Result<(), HarnessError> {
        let Some(session) = self.lock().session.clone() else {
            return Ok(());
        };
        let listing = self.api("GET", &format!("/api/session/{session}/message"), None)?;
        let Some(messages) = listing.get("data").and_then(Value::as_array) else {
            return Ok(());
        };
        let mut state = self.lock();
        // The floor is the prompt of the turn being waited for. A message handed
        // over again after its turn already ran is settled from what the session
        // holds, so it is not bounded by a newer floor.
        let since = state.turn_since;
        let mut text = String::new();
        let mut idle_succeeded = false;
        let mut failed = None;
        let mut usage = TurnUsage::default();
        // The service lists messages newest first; walk oldest to newest so the
        // newest assistant text and usage win over earlier steps in the turn.
        for message in messages.iter().rev() {
            // Only this turn's messages count. A session keeps its history, so
            // an earlier turn's answer or idle would otherwise close this one.
            let created = message
                .pointer("/time/created")
                .and_then(Value::as_i64)
                .unwrap_or(i64::MAX);
            if since.is_some_and(|since| created < since) {
                continue;
            }
            match message.get("type").and_then(Value::as_str) {
                Some("assistant") => {
                    // The newest assistant message with text is the answer; an
                    // earlier one belonged to a turn already settled, and a
                    // tool-only step must not wipe out the turn's text.
                    let candidate = text_of(message);
                    if !candidate.is_empty() {
                        text = candidate;
                    }
                    usage = usage_of(message);
                    if message.get("finish").and_then(Value::as_str) == Some("stop") {
                        idle_succeeded = true;
                    }
                }
                Some("idle") => match message.get("outcome").and_then(Value::as_str) {
                    Some("succeeded") | None => idle_succeeded = true,
                    Some(other) => failed = Some(other.to_string()),
                },
                _ => {}
            }
        }
        // Each open message is settled on its own evidence: its own answer in
        // the session, or the idle frame of the turn it belongs to. One stale
        // entry must never hold the turn open and defer every line behind it.
        let open_now: Vec<String> = state.open.clone();
        let anchored = self.answers_for(messages, &open_now).unwrap_or_default();
        let mut settled = Vec::new();
        for id in &open_now {
            if let Some((_, answer)) = anchored.iter().find(|(one, _)| one == id) {
                state.answers.insert(id.clone(), answer.clone());
                state.usages.insert(id.clone(), usage.clone());
                settled.push(id.clone());
            } else if idle_succeeded || failed.is_some() {
                if !text.is_empty() {
                    state.answers.insert(id.clone(), text.clone());
                }
                state.usages.insert(id.clone(), usage.clone());
                settled.push(id.clone());
            }
        }
        for id in &settled {
            state.finished.insert(id.clone());
            if let Some(reason) = &failed {
                state.failure = Some(reason.clone());
            }
        }
        state.open.retain(|id| !settled.contains(id));
        if state.open.is_empty() {
            state.turn_since = None;
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

    /// The answer for each of `ids` that the session already holds.
    fn answers_for(&self, messages: &[Value], ids: &[String]) -> Option<Vec<(String, String)>> {
        let mut found = Vec::new();
        for id in ids {
            if let Some(answer) = answer_of(messages, id) {
                found.push((id.clone(), answer));
            }
        }
        (!found.is_empty()).then_some(found)
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
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        if !self.lock().alive {
            return Err(HarnessError::Dead);
        }
        // An id already written is not written again: a replay after a crash
        // must not prompt the agent twice.
        {
            let state = self.lock();
            if state.written.iter().any(|written| written == &message.id) {
                // Already written: report where its turn stands. It is never
                // re-opened here, or every retry would hold the turn open and
                // defer every line behind it.
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: state.finished.contains(&message.id),
                });
            }
        }
        // One prompt per turn. While a turn is still open the message stays in
        // the mailbox: `run_once` stops here and hands it over after the open
        // turn settles. Without this every waiting line is prompted at once and
        // the vendor answers them together as one batched reply.
        if !self.lock().open.is_empty() {
            let _ = self.poll();
            if !self.lock().open.is_empty() {
                return Ok(DeliverOutcome::Deferred);
            }
        }
        let Some(session) = self.lock().session.clone() else {
            return Err(HarnessError::Failed("no session".into()));
        };
        let text = marked_message(&message.id, &message.text);
        let body = json!({"text": text, "delivery": "queue"});
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
        let mut state = self.lock();
        // This prompt's own write is the floor: nothing created before it can
        // close this turn, and a retry moves the floor with it.
        state.turn_since = Some(now_ms());
        state.written.push(message.id.clone());
        state.open.push(message.id.clone());
        Ok(DeliverOutcome::Accepted {
            turn_finished: state.finished.contains(&message.id),
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
            turn_state: if state.open.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery_mode: DeliveryMode::Followup,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    /// The service prints no closing line, so a turn that has gone quiet is
    /// over. The desk's own worker treats session idle the same way.
    fn ends_on_quiet(&self) -> Option<Duration> {
        Some(Duration::from_millis(500))
    }

    fn quiet_for(&self, id: &str, quiet: Duration, patience: Duration) -> bool {
        // The service prints no closing line, so a turn is over when the session
        // says so, or when its answer has stopped changing. The answer is
        // recorded before this returns: the supervisor settles the message the
        // moment it does, and nothing read afterwards is kept.
        // A message already settled is done at once; an accepted message after
        // a restart is tracked as open so the first poll can settle it.
        {
            let mut state = self.lock();
            if state.finished.contains(id) {
                return true;
            }
            if !state.open.iter().any(|open| open == id) {
                state.open.push(id.to_string());
            }
        }
        let deadline = Instant::now() + patience;
        let mut last: Option<String> = None;
        let mut still_since = Instant::now();
        loop {
            let _ = self.poll();
            {
                let state = self.lock();
                if state.finished.contains(id) {
                    return true;
                }
            }
            // Only this message's own answer ends its turn. The CLI streams the
            // text as it goes, so an answer is final once it has stood still.
            let answer = self.answer(id).or_else(|| self.answer_in_session(id));
            if answer != last {
                last = answer;
                still_since = Instant::now();
            } else if still_since.elapsed() >= quiet && last.is_some() {
                // Stand still with an answer in hand: this turn is done.
                let mut state = self.lock();
                let text = last.clone();
                let mut settled = state.open.clone();
                if settled.is_empty() {
                    settled.push(id.to_string());
                }
                for open in settled {
                    state.finished.insert(open.clone());
                    if let Some(text) = &text {
                        state.answers.insert(open.clone(), text.clone());
                    }
                    state.usages.insert(open.clone(), TurnUsage::default());
                }
                state.open.clear();
                state.turn_since = None;
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn answer_for(&self, id: &str) -> Option<String> {
        // The turn may have ended since the last poll; read the session before
        // answering so the answer the vendor sent is the one recorded.
        if self.answer(id).is_none() {
            let _ = self.poll();
        }
        if let Some(answer) = self.answer(id) {
            return Some(answer);
        }
        // Not recorded against this id yet: read the answer the session holds
        // for exactly this message.
        self.answer_in_session(id)
    }

    fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        OpenCodeHarness::usage_for(self, id)
    }

    fn turn_report_for(&self, id: &str) -> Option<TurnReport> {
        OpenCodeHarness::turn_report_for(self, id)
    }

    fn poll_steer(&mut self) -> Result<Option<SteerItem>, HarnessError> {
        let Some(queue) = &self.steer else {
            return Ok(None);
        };
        Ok(queue.take())
    }

    fn inject(&mut self, message: &OutboundMessage) -> Result<bool, HarnessError> {
        if self.steer.is_none() || self.seen(&message.id) {
            return Ok(false);
        }
        let Some(session) = self.lock().session.clone() else {
            return Ok(false);
        };
        // A prompt written while the turn is open joins it.
        let body = json!({
            "text": marked_message(&message.id, &message.text),
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
            let _ = Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    _ => {
                        let _ = child.kill();
                        break;
                    }
                }
            }
            let _ = child.wait();
        }
    }
}

fn usage_of(message: &Value) -> TurnUsage {
    let usage = message.get("tokens").cloned().unwrap_or(Value::Null);
    let number = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
    let cache = number("cache");
    TurnUsage {
        input_tokens: number("input"),
        output_tokens: number("output"),
        cache_read_tokens: if cache > 0 {
            cache
        } else {
            usage
                .pointer("/cache/read")
                .and_then(Value::as_u64)
                .unwrap_or(0)
        },
        cache_write_tokens: usage
            .pointer("/cache/write")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        reported_cost_micros: message
            .get("cost")
            .and_then(Value::as_f64)
            .map(|cost| (cost * 1_000_000.0) as i64),
    }
}

/// Milliseconds since the Unix epoch.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
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
    messages[..asked_at].iter().find_map(|message| {
        if message.get("type").and_then(Value::as_str) != Some("assistant") {
            return None;
        }
        let created = message.pointer("/time/created").and_then(Value::as_i64)?;
        if created <= asked {
            return None;
        }
        let text = text_of(message);
        (!text.is_empty()).then_some(text)
    })
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
