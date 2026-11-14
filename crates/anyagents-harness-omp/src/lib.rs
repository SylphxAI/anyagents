//! OMP (oh-my-pi) adapter: one `omp --mode rpc` child per agent.
//!
//! RPC mode is one JSON object per line each way. The child announces itself
//! with `ready`; every command carries a string `id` and is answered with a
//! `response` (`success`, `error`); the session's events (`message_start`,
//! `message_end`, `agent_end`, `prompt_result`, `session_settled`, ...) are
//! printed as they happen. The launch arguments come from the catalog entry
//! (`StreamSpec::for_program("omp")`); this adapter adds `--mode rpc`, without
//! which a child whose stdin is not a terminal exits with a usage error.
//!
//! Delivery is `next-step` and idempotent (`docs/delivery.md`):
//!
//! - Every mailbox message is one `prompt` with `streamingBehavior: "steer"`:
//!   an idle session starts a run with it, a busy one queues it as steering.
//!   OMP answers `success: true` once it is admitted, and `success: false`
//!   when it was not (a refusal: the message goes back to the mailbox).
//! - The session runs with `set_steering_mode` `all`, so a run's next step
//!   (after its tool batch, before its next model call) takes every queued
//!   message, in order, instead of one per step.
//! - The proof that a message is in the conversation is its own `message_end`
//!   with `role: "user"`, whose text starts with the marker line: OMP mints
//!   its own message ids, so the marker is the link. A message is committed
//!   only on that proof, and the proof outlives the process.
//! - Each prompt gets one `prompt_result` when the run that took it yields.
//!   OMP never ends a run with steering queued, so a `prompt_result` for a
//!   message with no proof means OMP dropped it before its run (an abort
//!   racing the dispatch): it goes back to the mailbox.
//! - At start the adapter asks `get_state` for the session file and reads the
//!   markers its user messages carry: a message an earlier process put into
//!   the session is answered `Duplicate` and never written again. OMP writes
//!   a new session's file only once it holds an assistant message, so a file
//!   that does not exist yet holds nothing: the run that took a message never
//!   finished a model reply, never ran a tool, and left nothing to resume. A
//!   file that exists and cannot be read stops delivery: what it holds is
//!   unknown. For the same reason the session is named to the host
//!   (`session_id`) only once its file exists: OMP refuses to resume a
//!   session it never wrote ("Session not found") and exits.
//! - A message is written only once every earlier one is admitted, so a
//!   refused message is never overtaken by a later one.
//! - The process counts as gone only once its output ended, so every proof it
//!   printed is read first: a write that fails forgets that write, and
//!   stopping the process kills it and reads the rest of its output. What it
//!   proved stays answerable (`proven`) after it is gone.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ExitStatus, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth,
    MESSAGE_MARKER_PREFIX, OutboundMessage, Proof, TurnState, TurnUsage, compose_message,
    message_id_in_text,
};
use anyagents_harness_stream::StreamSpec;
use serde_json::{Value, json};

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed(
        "user message_end carrying the marker; session file read at start",
    ),
    peer: None,
};

/// The ids of the commands the adapter sends before any message.
const STEERING_REQUEST: &str = "anyagents-steering";
const STATE_REQUEST: &str = "anyagents-state";

/// How much of the child's stderr is kept for an exit diagnostic.
const STDERR_TAIL: usize = 4096;

/// One long-lived OMP child.
pub struct OmpHarness {
    program: PathBuf,
    args: Vec<String>,
    resume_flag: &'static str,
    environment: Vec<(String, String)>,
    work_dir: Option<PathBuf>,
    shared: Arc<Mutex<State>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct State {
    child: Option<Child>,
    alive: bool,
    /// Bumped when something the host reads changes: what the activity
    /// watcher waits on.
    changes: u64,
    stderr: Vec<u8>,
    exit: Option<ExitStatus>,
    /// What the child's `ready` named.
    peer: Option<String>,
    /// The session this child runs: the one it resumed, then the one
    /// `get_state` named.
    session_id: Option<String>,
    /// The session this child was started on, which OMP had on disk.
    resumed: Option<String>,
    /// The file `get_state` named for `session_id`, until it is seen on disk.
    pending_file: Option<PathBuf>,
    /// `get_state` answered and the session file read: messages may go.
    known: bool,
    /// The session's record could not be read: nothing may go, since what it
    /// already holds is unknown.
    unreadable: Option<String>,
    /// Markers the session file held at start: an earlier process put
    /// these messages into the session.
    applied_before: HashSet<String>,
    /// Each message this process wrote, by mailbox id.
    prompts: HashMap<String, Prompt>,
    /// The mailbox id behind each request id this process wrote.
    requests: HashMap<String, String>,
    /// Written, and not yet answered.
    unadmitted: HashSet<String>,
    /// Written, and its `prompt_result` not yet read.
    open: usize,
    /// A `prompt_result` said work may still wake the session; its
    /// `session_settled` ends that.
    unsettled: bool,
    /// Proved in the conversation. A proof outlives the process.
    consumed: HashSet<String>,
    /// The run that took it has yielded.
    finished: HashSet<String>,
    /// A message was taken since the session was last idle.
    serving: bool,
    turns_served: u32,
    usage: TurnUsage,
    fault: Option<String>,
    request_seq: u64,
}

/// One `prompt` this process wrote.
struct Prompt {
    request: String,
    /// Answered `success: false`, or its run yielded without it: it goes
    /// back to the mailbox.
    returned: bool,
    /// Its `prompt_result` was read, or none will come.
    closed: bool,
}

impl State {
    /// What this adapter proved about `id`, whatever became of the process.
    fn proof(&self, id: &str) -> Option<Proof> {
        if self.consumed.contains(id) {
            return Some(Proof::Taken {
                turn_finished: self.finished.contains(id),
            });
        }
        self.applied_before.contains(id).then_some(Proof::Duplicate)
    }

    /// A session the next process can resume: the one this child was
    /// started on, or the one it runs once OMP wrote its file.
    fn resumable_session(&mut self) -> Option<String> {
        if let Some(file) = &self.pending_file {
            if !file.exists() {
                return self.resumed.clone();
            }
            self.pending_file = None;
        }
        self.session_id.clone()
    }

    fn busy(&self) -> bool {
        self.alive && (self.open > 0 || self.unsettled)
    }

    /// The prompt behind `request` closed: no `prompt_result` is still owed.
    fn close(&mut self, id: &str) {
        if let Some(prompt) = self.prompts.get_mut(id)
            && !prompt.closed
        {
            prompt.closed = true;
            self.open = self.open.saturating_sub(1);
        }
        self.unadmitted.remove(id);
    }

    /// Hand a written message back: the next offer writes it again.
    fn forget(&mut self, id: &str) {
        self.close(id);
        if let Some(prompt) = self.prompts.remove(id) {
            self.requests.remove(&prompt.request);
        }
    }

    /// The mailbox id of a request this process wrote and still tracks.
    fn prompt_for(&self, frame: &Value) -> Option<String> {
        let request = frame.get("id").and_then(Value::as_str)?;
        let id = self.requests.get(request)?;
        (self.prompts.get(id)?.request == request).then(|| id.clone())
    }
}

impl OmpHarness {
    /// Start one child with the catalog entry's arguments.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        Self::start_with_spec(
            StreamSpec::for_program("omp"),
            program,
            session_id,
            environment,
            None,
        )
    }

    /// Start one child with `spec`'s arguments, in `work_dir`. `session_id`,
    /// when set, is resumed with `spec.resume_flag`.
    pub fn start_with_spec(
        spec: StreamSpec,
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let mut args = spec.args;
        if !args.iter().any(|arg| arg == "--mode") {
            args.splice(0..0, ["--mode".to_string(), "rpc".to_string()]);
        }
        let mut harness = Self {
            program: program.into(),
            args,
            resume_flag: spec.resume_flag,
            environment: environment
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            work_dir: work_dir.map(Path::to_path_buf),
            shared: Arc::new(Mutex::new(State::default())),
            stdin: Arc::new(Mutex::new(None)),
            wake: Arc::new(Condvar::new()),
            reader: None,
            stderr_reader: None,
        };
        harness.launch(session_id)?;
        Ok(harness)
    }

    /// The session the next process should resume: the one this child
    /// resumed, then the one `get_state` named once OMP wrote its file. A
    /// session OMP never wrote cannot be resumed, so it is not named.
    pub fn session_id(&self) -> Option<String> {
        lock(&self.shared).resumable_session()
    }

    fn launch(&mut self, session_id: Option<&str>) -> Result<(), HarnessError> {
        let mut command = anyagents_harness::spawn_command(&self.program, &self.environment)?;
        command.args(&self.args);
        if let Some(session) = session_id {
            command.arg(self.resume_flag).arg(session);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        let mut child = command
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let (Some(stdin), Some(stdout), Some(stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
            return Err(HarnessError::Failed("child pipes missing".into()));
        };
        {
            let mut state = lock(&self.shared);
            // What the last process proved stays proved; everything it
            // only wrote is the new process's to judge again.
            state.child = Some(child);
            state.alive = true;
            state.stderr.clear();
            state.exit = None;
            state.peer = None;
            state.known = false;
            state.unreadable = None;
            state.pending_file = None;
            state.resumed = session_id.map(str::to_string);
            state.applied_before.clear();
            state.prompts.clear();
            state.requests.clear();
            state.unadmitted.clear();
            state.open = 0;
            state.unsettled = false;
            state.serving = false;
            state.fault = None;
            state.session_id = session_id.map(str::to_string);
        }
        *lock(&self.stdin) = Some(stdin);
        // Read in order before any message: all steering at each step, then
        // the session file to recognise what it already holds.
        for line in [
            json!({"id": STEERING_REQUEST, "type": "set_steering_mode", "mode": "all"}),
            json!({"id": STATE_REQUEST, "type": "get_state"}),
        ] {
            if write_line(&self.stdin, &line).is_err() {
                self.stop_child();
                return Err(HarnessError::Failed("omp closed its input".into()));
            }
        }
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        match thread::Builder::new()
            .name("anyagents-omp-stderr".into())
            .spawn(move || drain_stderr(stderr, shared, wake))
        {
            Ok(handle) => self.stderr_reader = Some(handle),
            Err(err) => {
                self.stop_child();
                return Err(HarnessError::Failed(err.to_string()));
            }
        }
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        match thread::Builder::new()
            .name("anyagents-omp".into())
            .spawn(move || read_stdout(stdout, shared, wake))
        {
            Ok(handle) => self.reader = Some(handle),
            Err(err) => {
                self.stop_child();
                return Err(HarnessError::Failed(err.to_string()));
            }
        }
        Ok(())
    }

    /// Stop the child and read its output to the end, so every proof it
    /// printed is known. The reader marks it gone at that end; what it
    /// proved stays.
    fn stop_child(&mut self) {
        *lock(&self.stdin) = None;
        let mut child = lock(&self.shared).child.take();
        if let Some(child) = child.as_mut() {
            anyagents_harness::kill_child(child, &self.environment);
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        if let Some(mut child) = child
            && let Ok(status) = child.wait()
        {
            lock(&self.shared).exit = Some(status);
        }
        if let Some(handle) = self.stderr_reader.take() {
            let _ = handle.join();
        }
        // Without a reader (it never started), nothing else says it is gone.
        let mut state = lock(&self.shared);
        if state.alive {
            state.alive = false;
            state.changes += 1;
        }
        drop(state);
        self.wake.notify_all();
    }
}

impl Drop for OmpHarness {
    fn drop(&mut self) {
        self.stop_child();
    }
}

impl Harness for OmpHarness {
    fn proven(&self, message_id: &str) -> Option<Proof> {
        lock(&self.shared).proof(message_id)
    }

    /// Kill the child and read its output to the end: a proof it printed
    /// before it went is known to `proven` afterwards.
    fn stop(&mut self) {
        self.stop_child();
    }

    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let id = message.id.as_str();
        let line = {
            let mut state = lock(&self.shared);
            if let Some(known) = written_outcome(&mut state, id) {
                return Ok(known);
            }
            if state.applied_before.contains(id) {
                return Ok(DeliverOutcome::Duplicate);
            }
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            if state.prompts.contains_key(id) {
                return Ok(DeliverOutcome::InFlight);
            }
            if let Some(reason) = &state.unreadable {
                return Err(HarnessError::Failed(reason.clone()));
            }
            // Nothing goes before the session file is read, nor behind a
            // message OMP has not admitted yet.
            if !state.known || !state.unadmitted.is_empty() {
                return Ok(DeliverOutcome::Deferred);
            }
            state.request_seq += 1;
            let request = format!("{id}/{}", state.request_seq);
            state.requests.insert(request.clone(), id.to_string());
            state.prompts.insert(
                id.to_string(),
                Prompt {
                    request: request.clone(),
                    returned: false,
                    closed: false,
                },
            );
            state.unadmitted.insert(id.to_string());
            state.open += 1;
            json!({
                "id": request,
                "type": "prompt",
                "message": compose_message(id, &message.context, &message.text),
                "streamingBehavior": "steer",
            })
        };
        // Never hold the state across the pipe: the reader needs it to drain
        // the child's output.
        let written = write_line(&self.stdin, &line);
        let mut state = lock(&self.shared);
        if written.is_err() {
            // A broken pipe took no whole line: nothing was admitted. The
            // process counts as gone once its output ends, after every proof
            // it printed is read; that end wakes the host.
            state.forget(id);
            return Ok(DeliverOutcome::Deferred);
        }
        Ok(written_outcome(&mut state, id).unwrap_or(DeliverOutcome::InFlight))
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if lock(&self.shared).alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    /// Run `session` in a new child, unless this one already runs it.
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let same = {
            let state = lock(&self.shared);
            state.alive && state.session_id.as_deref() == Some(session)
        };
        if same {
            return Ok(());
        }
        self.stop_child();
        self.launch(Some(session))
    }

    /// Ask OMP to stop the running turn. Its steering queue stays for the
    /// next run, and every prompt settles by its own `prompt_result`.
    fn cancel(&mut self) -> Result<(), HarnessError> {
        let line = {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            state.request_seq += 1;
            json!({"id": format!("anyagents-abort/{}", state.request_seq), "type": "abort"})
        };
        write_line(&self.stdin, &line).map_err(|_| HarnessError::Dead)
    }

    fn health(&self) -> HarnessHealth {
        let state = lock(&self.shared);
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.busy() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: DELIVERY.with_peer(state.peer.clone()),
            quota_reset_at_ms: None,
            sign_in_fault: state.fault.clone(),
        }
    }

    fn exit_diagnostic(&self) -> Option<String> {
        let mut state = lock(&self.shared);
        if let Some(child) = state.child.as_mut()
            && let Ok(Some(status)) = child.try_wait()
        {
            state.exit = Some(status);
        }
        if state.alive {
            return None;
        }
        let stderr = String::from_utf8_lossy(&state.stderr).trim().to_string();
        let stderr = anyagents_harness::masking::mask_known_secrets(&stderr);
        Some(match state.exit {
            Some(status) if stderr.is_empty() => format!("omp exited with {status}"),
            Some(status) => format!("omp exited with {status}; stderr: {stderr}"),
            None if stderr.is_empty() => "omp closed its output".to_string(),
            None => format!("omp closed its output; stderr: {stderr}"),
        })
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).turns_served
    }

    /// The total of every model response this process reported.
    fn served_usage(&self) -> TurnUsage {
        lock(&self.shared).usage.clone()
    }

    fn process_id(&self) -> Option<u32> {
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let _ = thread::Builder::new()
            .name("anyagents-omp-watch".into())
            .spawn(move || {
                let mut state = lock(&shared);
                let mut seen = None;
                loop {
                    if !state.alive {
                        drop(state);
                        notify();
                        break;
                    }
                    if seen != Some(state.changes) {
                        seen = Some(state.changes);
                        drop(state);
                        notify();
                        state = lock(&shared);
                        continue;
                    }
                    state = wake.wait(state).unwrap_or_else(|err| err.into_inner());
                }
            });
    }
}

/// What the mailbox may record for a message this process wrote, once OMP
/// has shown something about it. A message handed back is forgotten, so the
/// next offer writes it again.
fn written_outcome(state: &mut State, id: &str) -> Option<DeliverOutcome> {
    if state.consumed.contains(id) {
        return Some(DeliverOutcome::Accepted {
            turn_finished: state.finished.contains(id),
        });
    }
    if state.prompts.get(id)?.returned {
        state.forget(id);
        return Some(DeliverOutcome::Deferred);
    }
    None
}

fn read_stdout(stdout: impl Read, shared: Arc<Mutex<State>>, wake: Arc<Condvar>) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let mut state = lock(&shared);
        if on_frame(&mut state, &frame) {
            state.changes += 1;
            wake.notify_all();
        }
    }
    let mut state = lock(&shared);
    state.alive = false;
    if let Some(child) = state.child.as_mut()
        && let Ok(Some(status)) = child.try_wait()
    {
        state.exit = Some(status);
    }
    state.changes += 1;
    wake.notify_all();
}

/// Keep the end of the child's stderr for its exit diagnostic.
fn drain_stderr(stderr: impl Read, shared: Arc<Mutex<State>>, wake: Arc<Condvar>) {
    let mut stderr = stderr;
    let mut buffer = [0u8; 4096];
    loop {
        match stderr.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                let mut state = lock(&shared);
                state.stderr.extend_from_slice(&buffer[..read]);
                let excess = state.stderr.len().saturating_sub(STDERR_TAIL);
                state.stderr.drain(..excess);
            }
        }
    }
    wake.notify_all();
}

/// Apply one line the child printed. Answers whether the host may see a
/// change.
fn on_frame(state: &mut State, frame: &Value) -> bool {
    let was_busy = state.busy();
    match frame.get("type").and_then(Value::as_str) {
        Some("ready") => {
            let version = frame.get("protocolVersion").and_then(Value::as_u64);
            state.peer = Some(match version {
                Some(version) => format!("omp rpc/{version}"),
                None => "omp rpc".to_string(),
            });
        }
        Some("response") => on_response(state, frame),
        Some("message_end") => on_message(state, frame.get("message").unwrap_or(&Value::Null)),
        Some("prompt_result") => on_prompt_result(state, frame),
        Some("session_settled") => state.unsettled = false,
        // Streamed output and progress: nothing the host reads changed.
        _ => return false,
    }
    if state.serving && was_busy && !state.busy() {
        state.serving = false;
        state.turns_served += 1;
    }
    true
}

fn on_response(state: &mut State, frame: &Value) {
    let success = frame.get("success").and_then(Value::as_bool) == Some(true);
    let error = || {
        frame
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("no reason given")
            .to_string()
    };
    match frame.get("id").and_then(Value::as_str) {
        Some(STEERING_REQUEST) => {
            if !success {
                state.fault = Some(format!("omp refused steering mode all: {}", error()));
            }
        }
        Some(STATE_REQUEST) => {
            if success {
                read_state(state, frame.get("data").unwrap_or(&Value::Null));
            } else {
                // Without the session file, a message an earlier process
                // put into the session cannot be recognised.
                state.unreadable = Some(format!("omp refused get_state: {}", error()));
            }
            state.known = true;
        }
        _ if frame.get("command").and_then(Value::as_str) == Some("prompt") => {
            let Some(id) = state.prompt_for(frame) else {
                return;
            };
            if state.prompts[&id].closed {
                return;
            }
            if success {
                state.unadmitted.remove(&id);
            } else if !state.consumed.contains(&id) {
                // Thrown before it was queued: no `prompt_result` follows.
                if let Some(prompt) = state.prompts.get_mut(&id) {
                    prompt.returned = true;
                }
                state.close(&id);
            }
        }
        _ => {}
    }
}

/// `get_state`: the session, the steering mode the session took, and the
/// session file's record of what earlier processes put into it.
fn read_state(state: &mut State, data: &Value) {
    let session = data.get("sessionId").and_then(Value::as_str);
    if let Some(session) = session {
        state.session_id = Some(session.to_string());
    }
    if data.get("steeringMode").and_then(Value::as_str) != Some("all") {
        state.fault = Some("omp did not take steering mode all".to_string());
    }
    let file = data
        .get("sessionFile")
        .and_then(Value::as_str)
        .map(PathBuf::from);
    if session.is_some() && session != state.resumed.as_deref() {
        // Named to the host once OMP has written it.
        state.pending_file = Some(file.clone().unwrap_or_default());
    }
    if let Some(file) = file {
        match session_markers(&file) {
            Ok(markers) => state.applied_before = markers,
            Err(err) => {
                state.unreadable = Some(format!(
                    "cannot read the omp session file {}: {err}",
                    file.display()
                ));
            }
        }
    }
}

/// The mailbox id on the first line of every user message in the session
/// file at `path`. A file OMP has not written yet holds none: it writes a
/// new session's file with its first assistant message, and before that no
/// run finished a reply or ran a tool.
fn session_markers(path: &Path) -> std::io::Result<HashSet<String>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(err) => return Err(err),
    };
    Ok(bytes
        .split(|byte| *byte == b'\n')
        .map(String::from_utf8_lossy)
        .filter(|line| line.contains(MESSAGE_MARKER_PREFIX))
        .filter_map(|line| serde_json::from_str::<Value>(&line).ok())
        .filter(|entry| entry.get("type").and_then(Value::as_str) == Some("message"))
        .filter_map(|entry| user_marker(entry.get("message")?).map(str::to_string))
        .collect())
}

/// The mailbox id a user message's text starts with.
fn user_marker(message: &Value) -> Option<&str> {
    if message.get("role").and_then(Value::as_str) != Some("user") {
        return None;
    }
    match message.get("content")? {
        Value::String(text) => message_id_in_text(text),
        Value::Array(blocks) => blocks
            .iter()
            .find(|block| block.get("type").and_then(Value::as_str) == Some("text"))
            .and_then(|block| block.get("text").and_then(Value::as_str))
            .and_then(message_id_in_text),
        _ => None,
    }
}

/// A message entered the conversation: a user message carries its proof,
/// an assistant message its usage.
fn on_message(state: &mut State, message: &Value) {
    if let Some(id) = user_marker(message) {
        if state.prompts.contains_key(id) {
            let id = id.to_string();
            state.unadmitted.remove(&id);
            state.consumed.insert(id);
            state.serving = true;
        }
        return;
    }
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return;
    }
    let Some(usage) = message.get("usage") else {
        return;
    };
    let count = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
    state.usage.input_tokens += count("input");
    state.usage.output_tokens += count("output");
    state.usage.cache_read_tokens += count("cacheRead");
    state.usage.cache_write_tokens += count("cacheWrite");
    if let Some(cost) = usage.pointer("/cost/total").and_then(Value::as_f64) {
        *state.usage.reported_cost_micros.get_or_insert(0) += (cost * 1_000_000.0).round() as i64;
    }
}

/// The run that took a prompt yielded. A prompt without its proof was
/// dropped before the run read it.
fn on_prompt_result(state: &mut State, frame: &Value) {
    if frame.get("sessionSettled").and_then(Value::as_bool) == Some(false) {
        state.unsettled = true;
    }
    if let Some(status) = frame.pointer("/error/httpStatus").and_then(Value::as_u64)
        && (status == 401 || status == 403)
    {
        let message = frame
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("authentication failed");
        state.fault = Some(message.to_string());
    }
    let Some(id) = state.prompt_for(frame) else {
        return;
    };
    if state.prompts[&id].closed {
        return;
    }
    if state.consumed.contains(&id) {
        state.finished.insert(id.clone());
    } else if let Some(prompt) = state.prompts.get_mut(&id) {
        prompt.returned = true;
    }
    state.close(&id);
}

fn write_line(stdin: &Mutex<Option<ChildStdin>>, line: &Value) -> std::io::Result<()> {
    let mut stdin = lock(stdin);
    let Some(pipe) = stdin.as_mut() else {
        return Err(std::io::Error::other("stdin closed"));
    };
    writeln!(pipe, "{line}")?;
    pipe.flush()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn written(state: &mut State, id: &str) -> String {
        let request = format!("{id}/1");
        state.requests.insert(request.clone(), id.to_string());
        state.prompts.insert(
            id.to_string(),
            Prompt {
                request: request.clone(),
                returned: false,
                closed: false,
            },
        );
        state.unadmitted.insert(id.to_string());
        state.open += 1;
        state.alive = true;
        request
    }

    fn user(id: &str, steering: bool) -> Value {
        json!({"type": "message_end", "messageId": "msg-1", "message": {
            "role": "user", "steering": steering,
            "content": [{"type": "text", "text": format!("{MESSAGE_MARKER_PREFIX}{id}\nhello")}]
        }})
    }

    #[test]
    fn a_message_is_proved_by_its_own_user_message_end() {
        let mut state = State::default();
        let request = written(&mut state, "m1");
        on_frame(
            &mut state,
            &json!({"id": request, "type": "response", "command": "prompt", "success": true}),
        );
        assert_eq!(written_outcome(&mut state, "m1"), None);
        // A message that only mentions the marker below its first line is
        // not the proof.
        on_frame(
            &mut state,
            &json!({"type": "message_end", "message": {"role": "user",
                "content": "[Recovery]\n- anyagents-message:m1 (from a):"}}),
        );
        assert_eq!(written_outcome(&mut state, "m1"), None);
        on_frame(&mut state, &user("m1", true));
        assert_eq!(
            written_outcome(&mut state, "m1"),
            Some(DeliverOutcome::Accepted {
                turn_finished: false
            })
        );
        on_frame(
            &mut state,
            &json!({"type": "prompt_result", "id": request, "agentInvoked": true,
                "status": "completed", "sessionSettled": true}),
        );
        assert_eq!(
            written_outcome(&mut state, "m1"),
            Some(DeliverOutcome::Accepted {
                turn_finished: true
            })
        );
        assert!(!state.busy());
        assert_eq!(state.turns_served, 1);
    }

    #[test]
    fn a_refusal_or_a_result_without_proof_hands_the_message_back() {
        let mut state = State::default();
        let refused = written(&mut state, "refused");
        let dropped = written(&mut state, "dropped");
        on_frame(
            &mut state,
            &json!({"id": refused, "type": "response", "command": "prompt",
                "success": false, "error": "Agent is already processing."}),
        );
        // A late success for the same request does not undo the refusal.
        on_frame(
            &mut state,
            &json!({"id": refused, "type": "response", "command": "prompt", "success": true}),
        );
        on_frame(
            &mut state,
            &json!({"id": dropped, "type": "response", "command": "prompt", "success": true}),
        );
        on_frame(
            &mut state,
            &json!({"type": "prompt_result", "id": dropped, "agentInvoked": true,
                "status": "aborted", "sessionSettled": true}),
        );
        for id in ["refused", "dropped"] {
            assert_eq!(
                written_outcome(&mut state, id),
                Some(DeliverOutcome::Deferred),
                "{id}"
            );
            assert!(!state.prompts.contains_key(id), "{id} is written again");
        }
        assert!(!state.busy());
        assert_eq!(state.turns_served, 0, "nothing was served");
    }

    #[test]
    fn a_settled_answer_that_promises_more_work_keeps_the_session_busy() {
        let mut state = State::default();
        let request = written(&mut state, "m1");
        on_frame(&mut state, &user("m1", false));
        on_frame(
            &mut state,
            &json!({"type": "prompt_result", "id": request, "agentInvoked": true,
                "status": "completed", "sessionSettled": false}),
        );
        assert!(state.busy());
        on_frame(&mut state, &json!({"type": "session_settled"}));
        assert!(!state.busy());
        assert_eq!(state.turns_served, 1);
    }

    #[test]
    fn usage_is_the_sum_of_every_assistant_message() {
        let mut state = State::default();
        let usage = json!({"input": 3, "output": 2, "cacheRead": 1, "cacheWrite": 4,
            "totalTokens": 10, "cost": {"total": 0.000005}});
        for _ in 0..2 {
            on_frame(
                &mut state,
                &json!({"type": "message_end", "message": {"role": "assistant", "usage": usage}}),
            );
        }
        assert_eq!(
            state.usage,
            TurnUsage {
                input_tokens: 6,
                output_tokens: 4,
                cache_read_tokens: 2,
                cache_write_tokens: 8,
                reported_cost_micros: Some(10),
            }
        );
    }

    #[test]
    fn the_session_file_names_what_earlier_processes_put_into_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        let lines = [
            json!({"type": "session", "version": 3, "id": "s1", "cwd": "/w"}),
            json!({"type": "message", "id": "a", "parentId": null, "message": {"role": "user",
                "content": [{"type": "text", "text": "anyagents-message:m1\nhello"}]}}),
            json!({"type": "message", "id": "b", "parentId": "a", "message": {"role": "user",
                "steering": true, "content": "anyagents-message:m2\nmore"}}),
            json!({"type": "message", "id": "c", "parentId": "b", "message": {"role": "assistant",
                "content": [{"type": "text", "text": "anyagents-message:m3\nquoted"}]}}),
            json!({"type": "message", "id": "d", "parentId": "c", "message": {"role": "user",
                "content": [{"type": "text", "text": "[Recovery]\n- anyagents-message:m4"}]}}),
        ];
        let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
        std::fs::write(&path, text).unwrap();
        let mut state = State::default();
        on_frame(
            &mut state,
            &json!({"id": STATE_REQUEST, "type": "response", "command": "get_state",
                "success": true, "data": {"sessionId": "s1", "steeringMode": "all",
                "sessionFile": path.display().to_string()}}),
        );
        assert!(state.known);
        assert_eq!(state.fault, None);
        assert_eq!(state.session_id.as_deref(), Some("s1"));
        let mut found: Vec<_> = state.applied_before.iter().cloned().collect();
        found.sort();
        assert_eq!(found, ["m1", "m2"]);
    }

    #[test]
    fn a_session_that_keeps_one_steer_per_step_is_a_fault() {
        let mut state = State::default();
        on_frame(
            &mut state,
            &json!({"id": STATE_REQUEST, "type": "response", "command": "get_state",
                "success": true, "data": {"sessionId": "s1", "steeringMode": "one-at-a-time"}}),
        );
        assert_eq!(
            state.fault.as_deref(),
            Some("omp did not take steering mode all")
        );
    }

    fn get_state(file: &Path) -> Value {
        json!({"id": STATE_REQUEST, "type": "response", "command": "get_state",
            "success": true, "data": {"sessionId": "s1", "steeringMode": "all",
            "sessionFile": file.display().to_string()}})
    }

    #[test]
    fn a_session_file_omp_has_not_written_holds_nothing_and_names_no_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        let mut state = State::default();
        on_frame(&mut state, &get_state(&path));
        // Nothing to recognise, and nothing to stop delivery.
        assert!(state.known);
        assert!(state.applied_before.is_empty());
        assert_eq!(state.unreadable, None);
        // OMP exits on `--session` for a session it never wrote.
        assert_eq!(state.resumable_session(), None);
        std::fs::write(&path, "{\"type\":\"session\",\"id\":\"s1\"}\n").unwrap();
        assert_eq!(state.resumable_session().as_deref(), Some("s1"));

        // A child started on a session OMP had names it from the start.
        let mut state = State {
            resumed: Some("s0".into()),
            session_id: Some("s0".into()),
            ..State::default()
        };
        assert_eq!(state.resumable_session().as_deref(), Some("s0"));
        // A session it moved on to stays unnamed until it is on disk.
        on_frame(&mut state, &get_state(&dir.path().join("fork.jsonl")));
        assert_eq!(state.resumable_session().as_deref(), Some("s0"));
    }

    #[test]
    fn a_session_record_that_cannot_be_read_stops_delivery() {
        let dir = tempfile::tempdir().unwrap();
        // A file that exists and cannot be read as one.
        let mut state = State::default();
        on_frame(&mut state, &get_state(dir.path()));
        assert!(state.known);
        assert!(
            state
                .unreadable
                .as_deref()
                .is_some_and(|reason| reason.contains("cannot read the omp session file")),
            "{:?}",
            state.unreadable
        );
        // So is a session whose record OMP would not name.
        let mut state = State::default();
        on_frame(
            &mut state,
            &json!({"id": STATE_REQUEST, "type": "response", "command": "get_state",
                "success": false, "error": "boom"}),
        );
        assert!(state.known);
        assert_eq!(
            state.unreadable.as_deref(),
            Some("omp refused get_state: boom")
        );
    }

    /// An adapter over `/bin/sh -c script`: the script plays OMP.
    #[cfg(unix)]
    fn scripted(script: &str) -> OmpHarness {
        let spec = StreamSpec {
            args: vec!["-c".into(), script.into(), "--mode".into()],
            ..StreamSpec::for_program("omp")
        };
        OmpHarness::start_with_spec(spec, "/bin/sh", None, &[], None).unwrap()
    }

    /// The two answers OMP gives before any message, for a session whose
    /// file is `file`.
    #[cfg(unix)]
    fn handshake(file: &Path) -> String {
        let steering = json!({"id": STEERING_REQUEST, "type": "response",
            "command": "set_steering_mode", "success": true});
        format!(
            "read -r a; read -r b; printf '%s\\n' '{steering}' '{}';",
            get_state(file)
        )
    }

    #[cfg(unix)]
    fn message(id: &str) -> OutboundMessage {
        OutboundMessage {
            id: id.into(),
            seq: 1,
            text: "hello".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: anyagents_harness::Context::default(),
        }
    }

    #[cfg(unix)]
    fn until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "never: {what}");
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    /// A write the child cannot take forgets that write; the process counts
    /// as gone only once its output ends.
    #[cfg(unix)]
    #[test]
    fn a_failed_write_hands_the_message_back_and_dead_waits_for_the_end_of_output() {
        let dir = tempfile::tempdir().unwrap();
        let (closed, go) = (dir.path().join("closed"), dir.path().join("go"));
        let script = format!(
            "{} exec 0<&-; : > '{}'; while [ ! -e '{}' ]; do sleep 0.01; done",
            handshake(&dir.path().join("session.jsonl")),
            closed.display(),
            go.display()
        );
        let mut harness = scripted(&script);
        until("the child closed its input", || {
            closed.exists() && lock(&harness.shared).known
        });
        assert_eq!(
            harness.deliver(&message("m1")),
            Ok(DeliverOutcome::Deferred)
        );
        assert!(!lock(&harness.shared).prompts.contains_key("m1"));
        assert!(
            harness.health().process_alive,
            "dead before its output ended"
        );
        std::fs::write(&go, "").unwrap();
        until("the child is gone", || !harness.health().process_alive);
    }

    /// A proof the child printed before the host stopped it is read, and
    /// answered by `proven` once the process is gone.
    #[cfg(unix)]
    #[test]
    fn a_proof_printed_before_stop_is_proven_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let printed = dir.path().join("printed");
        let proof = json!({"type": "message_end", "messageId": "msg-1", "message": {
            "role": "user", "content": [{"type": "text",
            "text": format!("{MESSAGE_MARKER_PREFIX}m1\nhello")}]}});
        let script = format!(
            "{} read -r prompt; printf '%s\\n' '{proof}'; : > '{}'; exec sleep 30",
            handshake(&dir.path().join("session.jsonl")),
            printed.display()
        );
        let mut harness = scripted(&script);
        until("the session file is read", || lock(&harness.shared).known);
        // The child proves the prompt as soon as it reads it: the first offer
        // can already find the proof in.
        let first = harness.deliver(&message("m1"));
        assert!(
            matches!(
                first,
                Ok(DeliverOutcome::InFlight
                    | DeliverOutcome::Accepted {
                        turn_finished: false
                    })
            ),
            "{first:?}"
        );
        until("the child printed its proof", || printed.exists());
        harness.stop();
        assert!(!harness.health().process_alive);
        assert_eq!(
            harness.proven("m1"),
            Some(Proof::Taken {
                turn_finished: false
            })
        );
        assert_eq!(
            harness.deliver(&message("m1")),
            Ok(DeliverOutcome::Accepted {
                turn_finished: false
            })
        );
        assert_eq!(harness.proven("m2"), None);
    }
}
