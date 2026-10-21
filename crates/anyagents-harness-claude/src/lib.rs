//! Claude Code adapter.
//!
//! One child runs `claude -p` with stream-json on stdin and stdout. The mailbox
//! id is the first line of the user text (`anyagents-message:<id>`). An id already
//! written, or echoed back on stdout, is not written again.
//!
//! Declared delivery mode is steer. A stream-json child reads a `user` message
//! at any point, mid-turn included, so every mailbox line is written the moment
//! it arrives and the running conversation takes it. `turn_finished` becomes
//! true when a `result` event closes the open turn. Token counts and
//! `total_cost_usd` from that event are kept on the finished ids.
//!
//! Mid-turn delivery: with `--replay-user-messages` the child echoes back every
//! line written to its stdin as a `user` event, which is the harness's own
//! acknowledgement that the line entered the running conversation. A line
//! written during a turn reaches the agent before its next model step; the
//! echo is what marks it taken, and an id already echoed is never written twice.
//!
//! A [`SteerQueue`] ([`ClaudeHarness::with_steer`]) holds lines a channel pushed
//! between model steps even when the mailbox has not stored them yet; they are
//! written by [`Harness::inject`] with the same acknowledgement.
//!
//! Resume starts the child again with `--resume` when the session id changed or the
//! process has exited. A restart asks for the stored session and keeps whichever
//! id the child reports; a child that answers "no conversation found" begins a
//! new conversation instead, never silently. Cancel sends a `control_request`
//! whose subtype is `interrupt`.

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage, SteerItem,
    SteerQueue, SteerSupport, TurnState, TurnUsage, marked_message, message_id_in_text,
};
use serde_json::{Value, json};

/// How long a restart waits for a resumed child to answer or end.
const RESUME_SETTLE: Duration = Duration::from_secs(3);

/// A `result` is the vendor saying the session it was given is not there.
fn result_refused(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("result")
        && value.get("is_error").and_then(Value::as_bool) == Some(true)
        && value
            .get("result")
            .and_then(Value::as_str)
            .is_some_and(|text| {
                text.contains("No conversation found")
                    || text.contains("no conversation found")
                    || text.contains("Session not found")
                    || text.contains("session not found")
            })
}

/// One long-lived Claude Code child.
pub struct ClaudeHarness {
    program: PathBuf,
    environment: Vec<(String, String)>,
    work_dir: Option<PathBuf>,
    shared: Arc<Mutex<State>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    steer: Option<SteerQueue>,
}

#[derive(Default)]
struct State {
    child: Option<Child>,
    alive: bool,
    epoch: u64,
    /// Lines the child printed since the launch.
    lines: usize,
    /// The child answered a session it was given with "no conversation found".
    /// A refusal stays for the life of this adapter: a later launch that was
    /// never asked to resume is not a fresh answer to the question.
    refused: bool,
    seen: HashSet<String>,
    written: Vec<String>,
    open: Vec<String>,
    followups: Vec<String>,
    finished: HashSet<String>,
    turns_started: usize,
    quota_reset_at_ms: Option<i64>,
    session_id: Option<String>,
    usages: Vec<(String, TurnUsage)>,
    failures: Vec<(String, String)>,
    turns: std::collections::BTreeMap<String, u64>,
    request_seq: u64,
    /// Ids an echoed `user` event proved the child took.
    echoed: HashSet<String>,
    /// For each written id, whether no turn was open when it was written.
    phases: std::collections::HashMap<String, &'static str>,
}

impl ClaudeHarness {
    /// Start one child.
    ///
    /// `program` is the `claude` executable, or a test double. `session_id`, when
    /// set, is passed as `--resume`. `environment` overrides those variables in
    /// the child; pass an empty slice for a normal launch.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        Self::start_in(program, session_id, environment, None)
    }

    /// Start one child with its working directory set to `work_dir`.
    pub fn start_in(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        // Every start gives a child it resumes the same brief moment a restart
        // gives it, so the vendor's answer to the resume is read, not raced.
        let started = Self::launch_now(program, session_id, environment, work_dir)?;
        if session_id.is_some() && started.refused_launch(RESUME_SETTLE) == Some(false) {
            // A resume that answered is the conversation the child runs.
            started.wait_seen_resume(RESUME_SETTLE);
        }
        Ok(started)
    }

    /// Wait until the resumed child has printed anything, or `wait` passes.
    fn wait_seen_resume(&self, wait: Duration) {
        let deadline = Instant::now() + wait;
        loop {
            if lock(&self.shared).lines > 0 || Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn launch_now(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let mut harness = Self {
            program: program.into(),
            environment: environment
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            work_dir: work_dir.map(Path::to_path_buf),
            shared: Arc::new(Mutex::new(State::default())),
            stdin: Arc::new(Mutex::new(None)),
            wake: Arc::new(Condvar::new()),
            reader: None,
            steer: None,
        };
        harness.launch(session_id)?;
        Ok(harness)
    }

    /// Also watch `queue` for lines a channel pushed between model steps.
    pub fn with_steer(mut self, queue: SteerQueue) -> Self {
        self.steer = Some(queue);
        self
    }

    /// Start the child again with `session`: the same one when it is already
    /// the conversation it runs, else a child that resumes it.
    ///
    /// A session the vendor says it does not have is answered with an error
    /// result and nothing else. A caller with `restart` then starts a child
    /// without it, and the reason is returned: a conversation that had to be
    /// left behind is never dropped silently. A child that printed anything
    /// else was running, whatever it reported.
    pub fn resume_or_start(
        &mut self,
        session: Option<&str>,
        restart: bool,
    ) -> Result<Option<String>, HarnessError> {
        let Some(session) = session else {
            return Ok(None);
        };
        if self.session_id().as_deref() != Some(session) {
            self.resume(session)?;
        }
        let refused = self.refused_launch(RESUME_SETTLE);

        if !(restart && refused == Some(true)) {
            return Ok(None);
        }
        let reason = format!(
            "anyagents-claude: session {session} was not resumed (the vendor does not have it); starting a new one"
        );
        self.stop_child();
        self.launch(None)?;
        Ok(Some(reason))
    }

    /// The start just asked for was a resume, and its child said the session
    /// is not there: `Some(true)`. `None` when the child was launched fresh or
    /// answered with anything else, and `Some(false)` while it still runs.
    pub fn refused_launch(&self, wait: Duration) -> Option<bool> {
        let deadline = Instant::now() + wait;
        loop {
            {
                let state = lock(&self.shared);
                if state.refused {
                    return Some(true);
                }
                if state.lines > 0 || !state.alive {
                    return Some(false);
                }
            }
            if Instant::now() >= deadline {
                return Some(false);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The create input line Claude Code reads: one `user` event whose text is
    /// the marked message. Its echo is the acknowledgement.
    pub fn steer_input_line(message: &OutboundMessage) -> Value {
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": marked_message(&message.id, &message.text)
                }]
            }
        })
    }

    /// The `user` message a mailbox line becomes: one per line, whether the
    /// child is idle or busy. A stream-json child reads it either way.
    pub fn create_input(message: &OutboundMessage) -> Value {
        Self::steer_input_line(message)
    }

    /// When a mailbox line reached the child: `"idle"` when no turn was open,
    /// `"turn"` when it joined the open one.
    pub fn phase_of(&self, id: &str) -> Option<&'static str> {
        lock(&self.shared).phases.get(id).copied()
    }

    /// Whether the child echoed `id` back as a `user` event.
    pub fn echoed(&self, id: &str) -> bool {
        lock(&self.shared).echoed.contains(id)
    }

    /// Wait until the child echoes `id`.
    pub fn wait_echoed(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.echoed.contains(id))
    }

    /// Ids this process actually wrote, in write order.
    pub fn written_ids(&self) -> Vec<String> {
        lock(&self.shared).written.clone()
    }

    /// Turns opened so far, including a followup promoted by a result.
    pub fn turns_started(&self) -> usize {
        lock(&self.shared).turns_started
    }

    /// Session id passed at launch, or the id a later `result` reported.
    pub fn session_id(&self) -> Option<String> {
        lock(&self.shared).session_id.clone()
    }

    /// Usage from the result that finished `id`.
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        let state = lock(&self.shared);
        state
            .usages
            .iter()
            .rev()
            .find(|(item, _)| item == id)
            .map(|(_, usage)| usage.clone())
    }

    /// Why the result that finished `id` reported an error, if it did.
    pub fn failure_for(&self, id: &str) -> Option<String> {
        let state = lock(&self.shared);
        state
            .failures
            .iter()
            .rev()
            .find(|(item, _)| item == id)
            .map(|(_, reason)| reason.clone())
    }

    /// Whether a result has finished `id`.
    pub fn is_finished(&self, id: &str) -> bool {
        lock(&self.shared).finished.contains(id)
    }

    /// Whether `id` was written or echoed.
    pub fn seen(&self, id: &str) -> bool {
        lock(&self.shared).seen.contains(id)
    }

    /// Wait until a result finishes `id`, or the deadline passes.
    pub fn wait_finished(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.finished.contains(id))
    }

    /// Wait until `id` is written or echoed.
    pub fn wait_seen(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.seen.contains(id))
    }

    /// Wait until a rate-limit event with a reset time has been seen.
    pub fn wait_quota(&self, timeout: Duration) -> Option<i64> {
        let mut found = None;
        self.wait_pred(timeout, |state| {
            found = state.quota_reset_at_ms;
            found.is_some()
        });
        found
    }

    /// Wait until the child has exited or its stdout has closed.
    pub fn wait_dead(&self, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| !state.alive)
    }

    fn wait_pred(&self, timeout: Duration, mut pred: impl FnMut(&State) -> bool) -> bool {
        let mut state = lock(&self.shared);
        let deadline = Instant::now() + timeout;
        loop {
            if pred(&state) {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, wait) = self
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|err| err.into_inner());
            state = guard;
            if wait.timed_out() && !pred(&state) {
                return false;
            }
        }
    }

    /// Start one child and resume `session_id` when the vendor still has it.
    ///
    /// The new server's step: a child starts, then the stored session is
    /// resumed on it. A vendor that answers "no conversation found" begins a
    /// new conversation instead, and stderr says why. Anything else the
    /// resumed child reported stands.
    pub fn start_after_restart(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let mut harness = Self::start_in(program, None, environment, work_dir)?;
        if let Some(said) = harness.resume_or_start(session_id, true)? {
            eprintln!("{said}");
        }
        Ok(harness)
    }

    fn launch(&mut self, session_id: Option<&str>) -> Result<(), HarnessError> {
        let mut cmd = Command::new(&self.program);
        cmd.arg("-p")
            .arg("--input-format")
            .arg("stream-json")
            .arg("--output-format")
            .arg("stream-json")
            .arg("--verbose")
            .arg("--replay-user-messages");
        if let Some(session_id) = session_id {
            cmd.arg("--resume").arg(session_id);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = &self.work_dir {
            cmd.current_dir(dir);
        }
        for (key, value) in &self.environment {
            cmd.env(key, value);
        }
        let mut child = cmd
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let Some(child_stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdin missing".into()));
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdout missing".into()));
        };
        {
            let mut state = lock(&self.shared);
            state.child = Some(child);
            state.alive = true;
            state.lines = 0;
            state.open.clear();
            state.followups.clear();
            if let Some(session_id) = session_id {
                state.session_id = Some(session_id.to_string());
            }
        }
        *lock(&self.stdin) = Some(child_stdin);
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        match thread::Builder::new()
            .name("anyagents-claude".into())
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

    fn stop_child(&mut self) {
        *lock(&self.stdin) = None;
        {
            let mut state = lock(&self.shared);
            state.alive = false;
            if let Some(child) = state.child.as_mut() {
                let _ = child.kill();
            }
        }
        self.wake.notify_all();
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        let mut state = lock(&self.shared);
        if let Some(mut child) = state.child.take() {
            let _ = child.wait();
        }
        state.open.clear();
        state.followups.clear();
        state.alive = false;
    }
}

impl Drop for ClaudeHarness {
    fn drop(&mut self) {
        self.stop_child();
    }
}

impl Harness for ClaudeHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let mut state = lock(&self.shared);
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        // A message whose last turn failed is not a duplicate: the turn that
        // succeeds has to carry it. The adapter remembers it failed, so the
        // host handing it over again always means a new attempt.
        let failed_before = state.failures.iter().any(|(item, _)| item == &message.id);
        if !failed_before
            && (state.seen.contains(&message.id)
                || state.open.contains(&message.id)
                || state.followups.contains(&message.id))
        {
            return Ok(DeliverOutcome::Accepted {
                turn_finished: state.finished.contains(&message.id),
            });
        }
        if let Some(reset_at_ms) = state.quota_reset_at_ms {
            return Ok(DeliverOutcome::Quota { reset_at_ms });
        }
        let phase = if state.open.is_empty() {
            "idle"
        } else {
            "turn"
        };
        let line = Self::create_input(message);
        if write_stdin(&self.stdin, &line).is_err() {
            state.alive = false;
            self.wake.notify_all();
            return Err(HarnessError::Dead);
        }
        state.phases.insert(message.id.clone(), phase);
        accept_message(&mut state, &message.id);
        Ok(DeliverOutcome::Accepted {
            turn_finished: state.finished.contains(&message.id)
                && !state.failures.iter().any(|(item, _)| item == &message.id),
        })
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if lock(&self.shared).alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

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

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let mut state = lock(&self.shared);
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        state.request_seq += 1;
        let request_id = state.request_seq.to_string();
        let line = json!({
            "type": "control_request",
            "request_id": request_id,
            "request": {"subtype": "interrupt"}
        });
        if write_stdin(&self.stdin, &line).is_err() {
            state.alive = false;
            self.wake.notify_all();
            return Err(HarnessError::Dead);
        }
        state.open.clear();
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let state = lock(&self.shared);
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.open.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).usages.len() as u32
    }

    /// The total of every turn this process has finished. A total rather than
    /// the newest turn: the host reads it when the mailbox has drained, and a
    /// burst is several turns.
    fn served_usage(&self) -> TurnUsage {
        let state = lock(&self.shared);

        let mut total = TurnUsage::default();

        for (_, usage) in &state.usages {
            total.input_tokens += usage.input_tokens;
            total.output_tokens += usage.output_tokens;
            total.cache_read_tokens += usage.cache_read_tokens;
            total.cache_write_tokens += usage.cache_write_tokens;
            total.reported_cost_micros =
                match (total.reported_cost_micros, usage.reported_cost_micros) {
                    (Some(total), Some(one)) => Some(total + one),
                    (total, one) => total.or(one),
                };
        }
        total
    }

    fn clear_quota(&mut self) {
        lock(&self.shared).quota_reset_at_ms = None;
    }

    fn poll_steer(&mut self) -> Result<Option<SteerItem>, HarnessError> {
        let Some(queue) = &self.steer else {
            return Ok(None);
        };
        if !lock(&self.shared).alive {
            return Err(HarnessError::Dead);
        }
        // A line the child already echoed, or whose turn already finished, is
        // taken off the queue without a second write.
        while let Some(line) = queue.lines().into_iter().next() {
            let settled = {
                let state = lock(&self.shared);
                state.echoed.contains(&line.id) || state.finished.contains(&line.id)
            };
            if !settled {
                break;
            }
            queue.consume(&line.id);
        }
        Ok(queue.take())
    }

    fn inject(&mut self, message: &OutboundMessage) -> Result<bool, HarnessError> {
        let mut state = lock(&self.shared);
        if !state.alive {
            return Err(HarnessError::Dead);
        }
        if state.echoed.contains(&message.id) || state.finished.contains(&message.id) {
            return Ok(true);
        }
        // An id the mailbox already handed over is not written twice. The line
        // stays held for the step where the host settles it.
        if state.seen.contains(&message.id) {
            return Ok(false);
        }
        state.seen.insert(message.id.clone());
        state.written.push(message.id.clone());
        if state.open.is_empty() {
            state.turns_started += 1;
            state.open.push(message.id.clone());
        } else {
            state.followups.push(message.id.clone());
        }
        let line = Self::steer_input_line(message);
        if write_stdin(&self.stdin, &line).is_err() {
            state.alive = false;
            self.wake.notify_all();
            return Err(HarnessError::Dead);
        }
        Ok(true)
    }

    /// The ids the host settled are `keep`; every other held line goes back on
    /// the queue, so a line the child never took is offered again.
    fn release_steer(&mut self, keep: &str) {
        if let Some(queue) = &self.steer {
            queue.release_for(keep);
        }
    }

    fn process_id(&self) -> Option<u32> {
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        watch_child(Arc::clone(&self.shared), Arc::clone(&self.wake), notify);
    }
}

fn accept_message(state: &mut State, id: &str) {
    *state.turns.entry(id.to_string()).or_insert(0) += 1;
    if state.open.is_empty() {
        state.turns_started += 1;
        state.open.push(id.to_string());
    } else {
        state.followups.push(id.to_string());
    }
    state.seen.insert(id.to_string());
    state.written.push(id.to_string());
}

fn finish_open(state: &mut State, usage: TurnUsage) {
    if state.open.is_empty() {
        return;
    }
    for id in state.open.drain(..) {
        state.finished.insert(id.clone());
        state.usages.push((id, usage.clone()));
    }
    promote_followups(state);
}

/// Messages written while a turn was open start the next turn when it ends.
fn promote_followups(state: &mut State) {
    if !state.followups.is_empty() {
        state.turns_started += 1;
        state.open.append(&mut state.followups);
    }
}

fn read_stdout(stdout: ChildStdout, shared: Arc<Mutex<State>>, wake: Arc<Condvar>) {
    for line in BufReader::new(stdout).lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        {
            let mut state = lock(&shared);
            state.lines += 1;
            if result_refused(&value) {
                state.refused = true;
            }
            // An id the vendor wrote or echoed starts no second turn. An id that
            // only appears inside an error or a system notice is not proof it
            // wrote the message: the turn that failed must be able to try again.
            note_markers(&mut state, &value);
            note_echo(&mut state, &value);
            if let Some(reset_at_ms) = quota_from_claude(&value) {
                state.quota_reset_at_ms = Some(reset_at_ms);
            }
            if value.get("type").and_then(Value::as_str) == Some("result") {
                apply_result(&mut state, &value);
            }
            state.epoch = state.epoch.wrapping_add(1);
        }
        wake.notify_all();
    }
    {
        let mut state = lock(&shared);
        state.alive = false;
        state.epoch = state.epoch.wrapping_add(1);
    }
    wake.notify_all();
}

fn watch_child(shared: Arc<Mutex<State>>, wake: Arc<Condvar>, notify: Arc<dyn Fn() + Send + Sync>) {
    let _ = thread::Builder::new()
        .name("anyagents-claude-watch".into())
        .spawn(move || {
            let mut state = lock(&shared);
            let mut seen = 0u64;
            loop {
                if !state.alive {
                    drop(state);
                    notify();
                    break;
                }
                if state.epoch != seen {
                    seen = state.epoch;
                    drop(state);
                    notify();
                    state = lock(&shared);
                    continue;
                }
                state = wake.wait(state).unwrap_or_else(|err| err.into_inner());
            }
        });
}

fn apply_result(state: &mut State, value: &Value) {
    if let Some(id) = value.get("session_id").and_then(Value::as_str)
        && !id.is_empty()
    {
        state.session_id = Some(id.to_string());
    }
    if state.open.is_empty() {
        return;
    }
    // An errored result (a sign-in failure, a vendor error) is not a successful
    // turn: its ids finish, but with the vendor's own reason instead of usage.
    if let Some(reason) = result_error(value) {
        let ids = std::mem::take(&mut state.open);
        for id in ids {
            state.finished.insert(id.clone());
            state.failures.push((id, reason.clone()));
        }
        promote_followups(state);
        return;
    }
    finish_open(state, usage_from_claude(value));
}

/// The vendor's error text from a `result` event, or `None` when it succeeded.
fn result_error(value: &Value) -> Option<String> {
    let subtype = value
        .get("subtype")
        .and_then(Value::as_str)
        .unwrap_or("error");
    let is_error =
        value.get("is_error").and_then(Value::as_bool) == Some(true) || subtype != "success";
    if !is_error {
        return None;
    }
    let text = match value.get("result") {
        Some(Value::String(text)) if !text.trim().is_empty() => text.clone(),
        _ => subtype.to_string(),
    };
    if text == subtype {
        Some(text)
    } else {
        Some(format!("{subtype}: {text}"))
    }
}

fn usage_from_claude(value: &Value) -> TurnUsage {
    let usage = value.get("usage");
    TurnUsage {
        input_tokens: u64_field(usage, "input_tokens"),
        output_tokens: u64_field(usage, "output_tokens"),
        cache_read_tokens: u64_field(usage, "cache_read_input_tokens"),
        cache_write_tokens: u64_field(usage, "cache_creation_input_tokens"),
        reported_cost_micros: value.get("total_cost_usd").and_then(usd_to_micros),
    }
}

fn usd_to_micros(value: &Value) -> Option<i64> {
    let number = value.as_f64()?;
    if !number.is_finite() {
        return None;
    }
    let micros = (number * 1_000_000.0).round();
    if !(i64::MIN as f64..=i64::MAX as f64).contains(&micros) {
        return None;
    }
    Some(micros as i64)
}

fn u64_field(value: Option<&Value>, key: &str) -> u64 {
    value
        .and_then(|item| item.get(key))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn note_markers(state: &mut State, value: &Value) {
    if !mentions_user_text(value) {
        return;
    }
    collect_markers(state, value);
}

/// Whether a line carries user or assistant content, the only place the vendor
/// writes a mailbox id back.
fn mentions_user_text(value: &Value) -> bool {
    match value {
        Value::String(_) => false,
        Value::Array(items) => items.iter().any(mentions_user_text),
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str).is_some_and(|kind| {
                matches!(
                    kind,
                    "user" | "assistant" | "message" | "text" | "content_block"
                )
            }) {
                return true;
            }
            map.values().any(mentions_user_text)
        }
        _ => false,
    }
}

/// A `user` event the child sends back is proof it took the line: the replay
/// echo of a written line, or a message it reports itself. An error or a notice
/// is not an echo. An echoed id is taken, and a turn that failed before it
/// arrived no longer counts against it.
fn note_echo(state: &mut State, value: &Value) {
    if value.get("type").and_then(Value::as_str) != Some("user") {
        return;
    }
    let mut ids = Vec::new();
    collect_ids(value, &mut ids);
    for id in ids {
        state.echoed.insert(id.clone());
        state.seen.insert(id.clone());
        state.finished.remove(&id);
        state.failures.retain(|(item, _)| item != &id);
    }
}

fn collect_ids(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => {
            if let Some(id) = message_id_in_text(text) {
                out.push(id.to_string());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_ids(item, out);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                collect_ids(item, out);
            }
        }
        _ => {}
    }
}

fn collect_markers(state: &mut State, value: &Value) {
    match value {
        Value::String(text) => {
            if let Some(id) = message_id_in_text(text) {
                state.seen.insert(id.to_string());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_markers(state, item);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                collect_markers(state, item);
            }
        }
        _ => {}
    }
}

fn quota_from_claude(value: &Value) -> Option<i64> {
    if !mentions_rate_limit(value) {
        return None;
    }
    find_named_reset(value).or_else(|| find_delay(value))
}

fn mentions_rate_limit(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            let category = map.get("category").and_then(Value::as_str);
            let type_ = map.get("type").and_then(Value::as_str);
            let subtype = map.get("subtype").and_then(Value::as_str);
            if category == Some("rate_limit")
                || type_ == Some("rate_limit")
                || subtype.is_some_and(|text| text.contains("rate_limit"))
            {
                return true;
            }
            map.values().any(mentions_rate_limit)
        }
        Value::Array(items) => items.iter().any(mentions_rate_limit),
        _ => false,
    }
}

fn find_named_reset(value: &Value) -> Option<i64> {
    match value {
        Value::Object(map) => {
            for key in ["resets_at_ms", "reset_at_ms", "resetsAt", "resetAt"] {
                if let Some(n) = map.get(key).and_then(Value::as_i64) {
                    return Some(interpret_reset(key, n));
                }
            }
            map.values().find_map(find_named_reset)
        }
        Value::Array(items) => items.iter().find_map(find_named_reset),
        _ => None,
    }
}

fn interpret_reset(key: &str, n: i64) -> i64 {
    if key.ends_with("_ms") || n >= 1_000_000_000_000 {
        n
    } else {
        n.saturating_mul(1_000)
    }
}

fn find_delay(value: &Value) -> Option<i64> {
    match value {
        Value::Object(map) => {
            if let Some(delay) = map.get("retry_delay_ms").and_then(Value::as_i64) {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()?
                    .as_millis();
                let now = i64::try_from(now).ok()?;
                return Some(now.saturating_add(delay));
            }
            map.values().find_map(find_delay)
        }
        Value::Array(items) => items.iter().find_map(find_delay),
        _ => None,
    }
}

fn write_stdin(slot: &Mutex<Option<ChildStdin>>, value: &Value) -> std::io::Result<()> {
    let mut guard = lock(slot);
    let Some(stdin) = guard.as_mut() else {
        return Err(std::io::Error::other("stdin closed"));
    };
    writeln!(stdin, "{value}")?;
    stdin.flush()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}
