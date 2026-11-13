//! Claude Code adapter.
//!
//! One child runs `claude -p` with stream-json on stdin and stdout. The mailbox
//! id is the first line of the user text (`anyagents-message:<id>`). An id this
//! process already wrote is not written again.
//!
//! Declared delivery mode is steer. A stream-json child reads a `user` message
//! at any point, mid-turn included, so every mailbox line is written the moment
//! it arrives and the running conversation takes it. `turn_finished` becomes
//! true when a `result` event closes the open turn. Token counts and
//! `total_cost_usd` from that event are kept on the finished ids.
//!
//! Mid-turn delivery: every line carries a `uuid` derived from its mailbox id.
//! Claude Code queues a line that arrives during a turn and folds every queued
//! line, in order, into the conversation at the turn's next step (after the
//! running tools, before the next model request); a line still queued when
//! the turn ends starts the next one. It proves each line entered the
//! conversation, keyed by that uuid: `command_lifecycle` `started`, the
//! `--replay-user-messages` echo (`isReplay`), and the result's
//! `user_message_uuids`. A line is `InFlight` until one of them arrives, then
//! `Accepted`. Claude Code drops a second line with a uuid its transcript
//! already holds; a resumed session's transcript is also read at launch, so
//! a line an earlier process folded in mid-turn is answered `Duplicate`.
//!
//! Resume starts the child again with `--resume` when the session id changed or the
//! process has exited. Cancel sends a `control_request` whose subtype is `interrupt`.

mod diagnostics;

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState, TurnUsage, compose_message, message_id_in_text,
};
use serde_json::{Value, json};

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed(
        "the line's transcript row, or the result's user_message_uuids, by uuid",
    ),
    peer: None,
};

/// `anyagents cloud-harness`, which speaks this protocol for a cloud agent:
/// it runs one turn per line and echoes a line when its turn starts.
pub const CLOUD_DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::TurnEnd,
    idempotent: false,
    proof: std::borrow::Cow::Borrowed("replay echo when the line's turn starts"),
    peer: None,
};

/// The uuid a mailbox line carries, derived from its id so that a line
/// handed to a resumed session again carries the same one.
pub fn uuid_for(id: &str) -> String {
    let fnv = |basis: u64| {
        id.bytes().fold(basis, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
    };
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&fnv(0xcbf2_9ce4_8422_2325).to_be_bytes());
    bytes[8..].copy_from_slice(&fnv(0x6c62_272e_07bb_0142).to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

/// One long-lived Claude Code child.
pub struct ClaudeHarness {
    program: PathBuf,
    extra_args: Vec<String>,
    environment: Vec<(String, String)>,
    work_dir: Option<PathBuf>,
    shared: Arc<Mutex<State>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
    delivery: Delivery,
}

#[derive(Default)]
struct State {
    child: Option<Child>,
    stderr: diagnostics::Evidence,
    alive: bool,
    /// Bumped when something the host reads changes: what the activity
    /// watcher waits on.
    epoch: u64,
    seen: HashSet<String>,
    written: Vec<String>,
    open: Vec<String>,
    followups: Vec<String>,
    finished: HashSet<String>,
    turns_started: usize,
    quota_reset_at_ms: Option<i64>,
    sign_in_fault: Option<String>,
    session_id: Option<String>,
    usages: Vec<(String, TurnUsage)>,
    failures: Vec<(String, String)>,
    turns: std::collections::BTreeMap<String, u64>,
    request_seq: u64,
    /// The mailbox id behind each uuid this process wrote.
    uuid_ids: HashMap<String, String>,
    /// Written, and not yet proved to have entered the conversation.
    in_flight: HashSet<String>,
    /// Written to the running process.
    written_now: HashSet<String>,
    /// Proved to have entered the conversation.
    consumed: HashSet<String>,
    /// Ids the resumed session's transcript shows an earlier process applied.
    applied_before: HashSet<String>,
    /// Uuids the resumed session's transcript holds: a row's own, the other
    /// members of a merged row, a folded line's `source_uuid`.
    applied_uuids: HashSet<String>,
    /// Taken into the conversation (`started`, the replay echo), and not yet
    /// seen in the session transcript. Claude Code announces a line before it
    /// appends it: a kill in between loses it from the resumed session.
    taken: HashSet<String>,
    /// Ids Claude Code refused or discarded, with its verdict.
    rejected: HashMap<String, String>,
    /// The session transcript, read as it grows.
    tail: TranscriptTail,
    /// `CLAUDE_CONFIG_DIR`, where the session transcripts live.
    config_dir: Option<PathBuf>,
    /// When the child was seen to have exited while its stdout stayed open.
    exited_at: Option<Instant>,
    /// The take is the proof: for a harness with no transcript to read (the
    /// cloud harness, which starts a turn with the line it echoes).
    commit_on_take: bool,
    /// What the child's `system` `init` named: version and capabilities.
    peer: Option<String>,
    /// For each written id, whether no turn was open when it was written.
    phases: std::collections::HashMap<String, &'static str>,
}

/// What the host reads through the harness API.
#[derive(PartialEq, Eq)]
struct Observed {
    alive: bool,
    open: usize,
    followups: usize,
    seen: usize,
    in_flight: usize,
    consumed: usize,
    finished: usize,
    failures: usize,
    usages: usize,
    quota_reset_at_ms: Option<i64>,
    sign_in_fault: Option<String>,
    session_id: Option<String>,
}

impl State {
    fn observed(&self) -> Observed {
        Observed {
            alive: self.alive,
            open: self.open.len(),
            followups: self.followups.len(),
            seen: self.seen.len(),
            in_flight: self.in_flight.len(),
            consumed: self.consumed.len(),
            finished: self.finished.len(),
            failures: self.failures.len(),
            usages: self.usages.len(),
            quota_reset_at_ms: self.quota_reset_at_ms,
            sign_in_fault: self.sign_in_fault.clone(),
            session_id: self.session_id.clone(),
        }
    }
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
        Self::start_in_with_args(program, &[], session_id, environment, work_dir)
    }

    /// Start one child with extra CLI arguments.
    pub fn start_in_with_args(
        program: impl Into<PathBuf>,
        extra_args: &[String],
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let mut harness = Self {
            program: program.into(),
            extra_args: extra_args.to_vec(),
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
            delivery: DELIVERY,
        };
        harness.launch(session_id)?;
        Ok(harness)
    }

    /// The delivery this child's protocol peer offers, when it is not Claude
    /// Code itself (`anyagents cloud-harness`).
    pub fn with_delivery(mut self, delivery: Delivery) -> Self {
        // A turn-end harness starts a turn with the line it echoes, and keeps
        // no transcript to read: the take is the proof.
        if delivery.level == DeliveryLevel::TurnEnd {
            lock(&self.shared).commit_on_take = true;
        }
        self.delivery = delivery;
        self
    }

    /// The input line Claude Code reads: one `user` event whose text is the
    /// marked message, carrying the uuid every proof of it is keyed by.
    pub fn steer_input_line(message: &OutboundMessage) -> Value {
        json!({
            "type": "user",
            "uuid": uuid_for(&message.id),
            "message": {
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": compose_message(&message.id, &message.context, &message.text)
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

    /// Whether the child proved `id` entered the conversation.
    pub fn consumed(&self, id: &str) -> bool {
        lock(&self.shared).consumed.contains(id)
    }

    /// Wait until the child proves `id` entered the conversation.
    pub fn wait_consumed(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.consumed.contains(id))
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

    /// Whether `id` was written, or proved taken.
    pub fn seen(&self, id: &str) -> bool {
        lock(&self.shared).seen.contains(id)
    }

    /// Wait until a result finishes `id`, or the deadline passes.
    pub fn wait_finished(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.finished.contains(id))
    }

    /// Wait until `id` is written, or proved taken.
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

    fn launch(&mut self, session_id: Option<&str>) -> Result<(), HarnessError> {
        let mut cmd = anyagents_harness::spawn_command(&self.program, &self.environment)?;
        cmd.env("CLAUDE_CODE_DISABLE_AUTO_MEMORY", "1");
        // Append transcript rows before the result is printed, so the result's
        // `user_message_uuids` names lines already on disk.
        cmd.env("CLAUDE_CODE_EAGER_FLUSH", "1");
        cmd.arg("-p")
            .arg("--input-format")
            .arg("stream-json")
            .arg("--output-format")
            .arg("stream-json")
            .arg("--verbose")
            .arg("--replay-user-messages");
        cmd.args(&self.extra_args);
        if let Some(session_id) = session_id {
            cmd.arg("--resume").arg(session_id);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &self.work_dir {
            cmd.current_dir(dir);
        }
        let mut child = cmd
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let Some(child_stdin) = child.stdin.take() else {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdin missing".into()));
        };
        let Some(stdout) = child.stdout.take() else {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdout missing".into()));
        };
        let stderr = child.stderr.take().expect("piped child stderr");
        {
            let initial_quota = self
                .environment
                .iter()
                .find(|(k, _)| k == "CLAUDE_CONFIG_DIR")
                .map(|(_, v)| Path::new(v))
                .and_then(|cfg_dir| {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .and_then(|d| i64::try_from(d.as_millis()).ok())
                        .unwrap_or(0);
                    detect_session_quota_reset_ms(cfg_dir, now_ms)
                });
            let config_dir = self
                .environment
                .iter()
                .rev()
                .find(|(key, _)| key == "CLAUDE_CONFIG_DIR")
                .map(|(_, value)| PathBuf::from(value));
            let scan = session_id
                .map(|session| transcript_marks(&self.environment, session))
                .unwrap_or_default();
            let mut state = lock(&self.shared);
            state.child = Some(child);
            state.stderr = diagnostics::Evidence::default();
            state.alive = true;
            state.exited_at = None;
            state.sign_in_fault = None;
            state.open.clear();
            state.followups.clear();
            // What the last process wrote and the session holds is the
            // session's: a line it never proved goes to this process again.
            state.in_flight.clear();
            // A line the last process took but never stored is the
            // transcript's to settle: found there, it is a duplicate.
            state.taken.clear();
            state.written_now.clear();
            state.applied_before.extend(scan.ids);
            state.applied_uuids.extend(scan.uuids);
            // Without a config directory there is no transcript to read.
            state.commit_on_take |= config_dir.is_none();
            state.config_dir = config_dir;
            state.tail = scan.tail;
            state.peer = None;
            if initial_quota.is_some() {
                state.quota_reset_at_ms = initial_quota;
            }
            if let Some(session_id) = session_id {
                state.session_id = Some(session_id.to_string());
            }
        }
        *lock(&self.stdin) = Some(child_stdin);
        let stderr_shared = Arc::clone(&self.shared);
        let stderr_wake = Arc::clone(&self.wake);
        match thread::Builder::new()
            .name("anyagents-claude-stderr".into())
            .spawn(move || diagnostics::drain(stderr, stderr_shared, stderr_wake))
        {
            Ok(handle) => self.stderr_reader = Some(handle),
            Err(error) => {
                self.stop_child();
                return Err(HarnessError::Failed(error.to_string()));
            }
        }
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
        let mut child = {
            let mut state = lock(&self.shared);
            state.alive = false;
            state.child.take()
        };
        self.wake.notify_all();
        if let Some(child) = child.as_mut() {
            anyagents_harness::kill_child(child, &self.environment);
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        if let Some(mut child) = child
            && let Ok(status) = child.wait()
        {
            lock(&self.shared).stderr.status = Some(status);
        }
        if let Some(handle) = self.stderr_reader.take() {
            let _ = handle.join();
        }
        let mut state = lock(&self.shared);
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
        let id = &message.id;
        // A proof outlives the process that gave it.
        match self.proven(id) {
            Some(Proof::Taken { turn_finished }) => {
                return Ok(DeliverOutcome::Accepted { turn_finished });
            }
            Some(Proof::Duplicate) => return Ok(DeliverOutcome::Duplicate),
            None => {}
        }
        {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            // The running process's verdict; a new process may take it.
            // Claude Code does not say why it refused: a reason that may
            // hold for every line.
            if let Some(reason) = state.rejected.get(id) {
                return Ok(DeliverOutcome::rejected(reason.clone(), false));
            }
            if let Some(reset_at_ms) = state.quota_reset_at_ms
                && !state.finished.contains(id)
            {
                return Ok(DeliverOutcome::Quota { reset_at_ms });
            }
            // A line a failed turn handed back is not in flight: it is
            // written again for the turn that succeeds. A taken line waits
            // for its transcript row.
            if state.in_flight.contains(id) || state.taken.contains(id) {
                return Ok(DeliverOutcome::InFlight);
            }
            let phase = if state.open.is_empty() {
                "idle"
            } else {
                "turn"
            };
            state.phases.insert(id.clone(), phase);
            accept_message(&mut state, id);
            state.in_flight.insert(id.clone());
            state.consumed.remove(id);
            state.uuid_ids.insert(uuid_for(id), id.clone());
        }
        let line = Self::create_input(message);
        let write_res = write_stdin(&self.stdin, &line);
        let mut state = lock(&self.shared);
        if write_res.is_err() {
            // The line did not reach the vendor. It is forgotten and offered
            // again; the process is dead only once its output ends, so a
            // proof still in the pipe is read first.
            unwrite(&mut state, id);
            return Ok(DeliverOutcome::Deferred);
        }
        // A proof can beat this lock.
        Ok(if state.consumed.contains(id) {
            DeliverOutcome::Accepted {
                turn_finished: state.finished.contains(id),
            }
        } else {
            DeliverOutcome::InFlight
        })
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        if lock(&self.shared).taken.contains(message_id) {
            refresh_transcript(&self.shared);
        }
        let state = lock(&self.shared);
        if state.consumed.contains(message_id) {
            return Some(Proof::Taken {
                turn_finished: state.finished.contains(message_id),
            });
        }
        let applied = state.applied_before.contains(message_id)
            || state.applied_uuids.contains(&uuid_for(message_id));
        (applied && !state.written_now.contains(message_id)).then_some(Proof::Duplicate)
    }

    /// Kill the child and read its output to the end, keeping every proof.
    fn stop(&mut self) {
        self.stop_child();
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
        let line = {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            state.request_seq += 1;
            let request_id = state.request_seq.to_string();
            json!({
                "type": "control_request",
                "request_id": request_id,
                "request": {"subtype": "interrupt"}
            })
        };
        let write_res = write_stdin(&self.stdin, &line);
        let mut state = lock(&self.shared);
        if write_res.is_err() {
            // Dead once its output ends, not before.
            return Err(HarnessError::Dead);
        }
        state.open.clear();
        Ok(())
    }

    fn exit_diagnostic(&self) -> Option<String> {
        let mut state = lock(&self.shared);
        if let Some(child) = state.child.as_mut()
            && let Ok(Some(status)) = child.try_wait()
        {
            // The process exited; it is dead once the reader drained its
            // output, which may still hold proofs.
            state.stderr.status = Some(status);
        }
        (!state.alive).then(|| state.stderr.projection())
    }

    fn health(&self) -> HarnessHealth {
        let mut state = lock(&self.shared);
        // The child exited, and a descendant keeps its stdout open: past the
        // grace no proof can still arrive from it.
        if state.alive && exited_past_grace(&mut state) {
            state.alive = false;
        }
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if state.open.is_empty() && state.in_flight.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery: self.delivery.clone().with_peer(state.peer.clone()),
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: state.sign_in_fault.clone(),
        }
    }

    fn turns_served(&self) -> u32 {
        let state = lock(&self.shared);
        let non_auth_failures = if state.sign_in_fault.is_some() {
            0
        } else {
            state.failures.len()
        };
        (state.usages.len() + non_auth_failures) as u32
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

    fn process_id(&self) -> Option<u32> {
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        watch_child(Arc::clone(&self.shared), Arc::clone(&self.wake), notify);
    }
}

/// How long a child that exited may keep its stdout open (a descendant
/// holding it) before it is declared dead anyway.
const EXIT_GRACE: Duration = Duration::from_secs(5);

/// Whether the child exited more than [`EXIT_GRACE`] ago while its stdout
/// never ended. Dead is otherwise only declared at that end.
fn exited_past_grace(state: &mut State) -> bool {
    if state.exited_at.is_none()
        && let Some(child) = state.child.as_mut()
        && let Ok(Some(status)) = child.try_wait()
    {
        state.stderr.status = Some(status);
        state.exited_at = Some(Instant::now());
    }
    state
        .exited_at
        .is_some_and(|exited| exited.elapsed() >= EXIT_GRACE)
}

/// Undo `accept_message` for a line whose write failed.
fn unwrite(state: &mut State, id: &str) {
    state.in_flight.remove(id);
    state.written_now.remove(id);
    state.seen.remove(id);
    state.phases.remove(id);
    if let Some(pos) = state.written.iter().rposition(|written| written == id) {
        state.written.remove(pos);
    }
    state.followups.retain(|queued| queued != id);
    let opened = state.open.iter().any(|open| open == id);
    state.open.retain(|open| open != id);
    if opened && state.open.is_empty() {
        state.turns_started = state.turns_started.saturating_sub(1);
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
    state.written_now.insert(id.to_string());
}

fn finish_open(state: &mut State, usage: TurnUsage) {
    if state.open.is_empty() {
        return;
    }
    state.sign_in_fault = None;
    for (idx, id) in state.open.drain(..).enumerate() {
        state.finished.insert(id.clone());
        let item_usage = if idx == 0 {
            usage.clone()
        } else {
            TurnUsage::default()
        };
        state.usages.push((id, item_usage));
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
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        // A line that ends a turn, hands lines back or cancels one is judged
        // against what the transcript already stores: read it first, outside
        // the lock.
        if settles_lines(&value) {
            refresh_transcript(&shared);
        }
        {
            let mut state = lock(&shared);
            let before = state.observed();
            // Only Claude Code's own proof that a line entered the
            // conversation takes it; a mention in text or an error is none.
            note_proof(&mut state, &value);
            if let Some(reset_at_ms) = quota_from_claude(&value) {
                state.quota_reset_at_ms = Some(reset_at_ms);
                let rejected_ids = std::mem::take(&mut state.open);
                for id in rejected_ids {
                    hand_back(&mut state, &id, "rate_limit");
                }
            }
            if let Some(reason) = synthetic_auth_error_from_claude(&value) {
                if let Some(id) = value.get("session_id").and_then(Value::as_str)
                    && !id.is_empty()
                {
                    state.session_id = Some(id.to_string());
                }
                fail_oauth_turn(&mut state, reason);
                state.epoch = state.epoch.wrapping_add(1);
                drop(state);
                wake.notify_all();
                break;
            }
            if value.get("type").and_then(Value::as_str) == Some("result") {
                apply_result(&mut state, &value);
            }
            // A streamed assistant or tool event changes nothing the host
            // reads, and wakes nobody.
            if state.observed() != before {
                state.epoch = state.epoch.wrapping_add(1);
            }
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

/// Whether `value` can end a turn, hand lines back or cancel one: a result,
/// a quota or sign-in failure, or a command's `cancelled` or `discarded`.
fn settles_lines(value: &Value) -> bool {
    match value.get("type").and_then(Value::as_str) {
        Some("result") => true,
        Some("command_lifecycle") => matches!(
            value.get("state").and_then(Value::as_str),
            Some("cancelled" | "discarded")
        ),
        _ => {
            quota_from_claude(value).is_some() || synthetic_auth_error_from_claude(value).is_some()
        }
    }
}

fn watch_child(shared: Arc<Mutex<State>>, wake: Arc<Condvar>, notify: Arc<dyn Fn() + Send + Sync>) {
    let _ = thread::Builder::new()
        .name("anyagents-claude-watch".into())
        .spawn(move || {
            let mut state = lock(&shared);
            let mut seen = None;
            loop {
                if !state.alive {
                    drop(state);
                    notify();
                    break;
                }
                if seen != Some(state.epoch) {
                    seen = Some(state.epoch);
                    drop(state);
                    notify();
                    state = lock(&shared);
                    continue;
                }
                state = wake.wait(state).unwrap_or_else(|err| err.into_inner());
            }
        });
}

fn is_oauth_auth_error_text(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    text.contains("OAuth session expired")
        || text.contains("OAuth access token has expired")
        || text.contains("Failed to authenticate")
        || text.contains("Not logged in")
        || text.contains("authentication_failed")
        || text.contains("authentication_error")
        || (lower.contains("401")
            && (lower.contains("oauth")
                || lower.contains("authenticate")
                || lower.contains("authentication")
                || lower.contains("access token")))
}

fn collect_text_fields(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) if !s.trim().is_empty() => out.push(s.clone()),
        Value::Array(items) => {
            for item in items {
                collect_text_fields(item, out);
            }
        }
        Value::Object(map) => {
            if let Some(Value::String(text)) = map.get("text") {
                if !text.trim().is_empty() {
                    out.push(text.clone());
                }
            } else {
                for (k, v) in map {
                    if matches!(
                        k.as_str(),
                        "content" | "message" | "error" | "result" | "delta"
                    ) {
                        collect_text_fields(v, out);
                    }
                }
            }
        }
        _ => {}
    }
}

pub fn synthetic_auth_error_from_claude(value: &Value) -> Option<String> {
    let kind = value
        .get("type")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str);
    let model = value
        .pointer("/message/model")
        .or_else(|| value.get("model"))
        .and_then(Value::as_str);
    if model == Some("<synthetic>") {
        let mut texts = Vec::new();
        collect_text_fields(
            value.pointer("/message/content").unwrap_or(value),
            &mut texts,
        );
        let joined = texts.join("\n");
        if is_oauth_auth_error_text(&joined) {
            return Some(joined.trim().to_string());
        }
    }
    if kind == Some("result")
        && let Some(text) = value.get("result").and_then(Value::as_str)
    {
        let trimmed = text.trim();
        let is_synthetic_prefix = trimmed.starts_with("Failed to authenticate")
            || trimmed.starts_with("API Error: 401")
            || trimmed.starts_with("OAuth access token has expired")
            || trimmed.starts_with("OAuth session expired")
            || trimmed.starts_with("Not logged in");
        if is_synthetic_prefix && is_oauth_auth_error_text(trimmed) {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn fail_oauth_turn(state: &mut State, reason: String) {
    let mut ids = std::mem::take(&mut state.open);
    ids.append(&mut state.followups);
    for id in ids {
        state.finished.remove(&id);
        hand_back(state, &id, &reason);
    }
    state.sign_in_fault = Some(reason);
    // Its output ends once it is killed, and the reader marks it dead then.
    if let Some(child) = state.child.as_mut() {
        let _ = child.kill();
    }
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
        if is_oauth_auth_error_text(&reason) {
            fail_oauth_turn(state, reason);
            return;
        }
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

/// A line whose turn failed before it entered the conversation goes back to
/// the mailbox, to be written again for the turn that succeeds. A line the
/// conversation already took stays taken.
fn hand_back(state: &mut State, id: &str, reason: &str) {
    state.failures.push((id.to_string(), reason.to_string()));
    if state.consumed.contains(id) {
        return;
    }
    state.in_flight.remove(id);
    state.taken.remove(id);
    state.seen.remove(id);
}

/// What the child proves about the lines this process gave it, keyed by
/// their uuid: `command_lifecycle` `started` (its turn started with it, or a
/// step folded it in), the replay echo of an input line, and the
/// `user_message_uuids` a result lists. `cancelled` hands a line back to the
/// mailbox; `refused` and `discarded` are Claude Code's own verdict on it, so
/// it is taken and not offered again.
fn note_proof(state: &mut State, value: &Value) {
    let by_uuid = |state: &State, uuid: Option<&Value>| {
        uuid.and_then(Value::as_str)
            .and_then(|uuid| state.uuid_ids.get(uuid))
            .cloned()
    };
    match value.get("type").and_then(Value::as_str) {
        Some("system") if value.get("subtype").and_then(Value::as_str) == Some("init") => {
            let version = value
                .get("claude_code_version")
                .and_then(Value::as_str)
                .unwrap_or("unknown version");
            state.peer = Some(format!("claude-code {version}"));
            if let Some(session) = value
                .get("session_id")
                .and_then(Value::as_str)
                .filter(|session| !session.is_empty())
            {
                state.session_id = Some(session.to_string());
            }
        }
        Some("command_lifecycle") => {
            let Some(id) = by_uuid(state, value.get("command_uuid").or(value.get("uuid"))) else {
                return;
            };
            match value.get("state").and_then(Value::as_str) {
                Some("started") => take(state, &id),
                // A batch that failed to set up is cancelled after `started`,
                // and a command still queued when the session ended is
                // discarded (Claude Code re-emits its uuid on the next
                // worker): neither entered the conversation, so it goes back.
                // The transcript was read first: a stored line stays taken.
                Some("cancelled" | "discarded") if !state.consumed.contains(&id) => {
                    state.open.retain(|item| item != &id);
                    state.followups.retain(|item| item != &id);
                    state.in_flight.remove(&id);
                    state.taken.remove(&id);
                    state.seen.remove(&id);
                }
                // Claude Code's own verdict: it will never take this line.
                Some(verdict @ "refused") if !state.consumed.contains(&id) => {
                    state.open.retain(|item| item != &id);
                    state.followups.retain(|item| item != &id);
                    state.in_flight.remove(&id);
                    state.taken.remove(&id);
                    state
                        .rejected
                        .insert(id, format!("claude code {verdict} the input"));
                }
                _ => {}
            }
        }
        Some("user") if value.get("isReplay").and_then(Value::as_bool) == Some(true) => {
            let id = by_uuid(state, value.get("uuid")).or_else(|| {
                let mut ids = Vec::new();
                collect_ids(value.get("message").unwrap_or(&Value::Null), &mut ids);
                ids.into_iter().find(|id| state.in_flight.contains(id))
            });
            if let Some(id) = id {
                take(state, &id);
            }
        }
        // With the eager flush, a result follows the appends of every row
        // its turn recorded: the lines it names are on disk.
        Some("result") => {
            let ids: Vec<String> = value
                .get("user_message_uuids")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|uuid| by_uuid(state, Some(uuid)))
                .collect();
            for id in ids {
                take(state, &id);
                persist(state, &id);
            }
        }
        _ => {}
    }
}

/// Claude Code took `id` into the conversation. A line folded into the
/// running turn belongs to that turn, not the next. It is committed once
/// its transcript row is on disk (`persist`), unless the take is the proof.
fn take(state: &mut State, id: &str) {
    state.in_flight.remove(id);
    if state.consumed.contains(id) || !state.taken.insert(id.to_string()) {
        return;
    }
    state.seen.insert(id.to_string());
    state.failures.retain(|(item, _)| item != id);
    if !state.open.is_empty()
        && let Some(at) = state.followups.iter().position(|item| item == id)
    {
        let id = state.followups.remove(at);
        state.open.push(id);
    }
    if state.commit_on_take {
        persist(state, id);
    }
}

/// `id` is in the session as Claude Code stores it: committed.
fn persist(state: &mut State, id: &str) {
    state.taken.remove(id);
    state.in_flight.remove(id);
    state.consumed.insert(id.to_string());
}

/// The session transcript as it grows: where it is and how far it was read.
#[derive(Clone, Default)]
struct TranscriptTail {
    path: Option<PathBuf>,
    offset: u64,
}

/// Read what the session transcript gained since the last read, and commit
/// every taken line it now holds: a user row (its own uuid, or a merged
/// row's `mergedSendUuids`) or a folded line's `queued_command` attachment
/// (`source_uuid`), and the marker each carries. The file is read outside
/// the state lock (a live transcript reaches gigabytes), from where the
/// last read, or the launch's scan, ended.
fn refresh_transcript(shared: &Mutex<State>) {
    use std::io::{Seek, SeekFrom};
    let (config, session, tail) = {
        let state = lock(shared);
        if state.taken.is_empty() {
            return;
        }
        let (Some(config), Some(session)) = (state.config_dir.clone(), state.session_id.clone())
        else {
            return;
        };
        (config, session, state.tail.clone())
    };
    let name = format!("{session}.jsonl");
    let tail = match tail.path {
        Some(ref path) if path.file_name().is_some_and(|file| file == name.as_str()) => tail,
        _ => {
            let found = std::fs::read_dir(config.join("projects"))
                .into_iter()
                .flatten()
                .flatten()
                .map(|project| project.path().join(&name))
                .find(|path| path.is_file());
            let Some(found) = found else {
                return;
            };
            TranscriptTail {
                path: Some(found),
                offset: 0,
            }
        }
    };
    let Some(path) = tail.path.clone() else {
        return;
    };
    let Ok(mut file) = std::fs::File::open(&path) else {
        return;
    };
    if file.seek(SeekFrom::Start(tail.offset)).is_err() {
        return;
    }
    let mut ids = Vec::new();
    let mut uuids = Vec::new();
    let read = each_row(file, |row| {
        let (row_ids, row_uuids) = row_marks(row);
        ids.extend(row_ids);
        uuids.extend(row_uuids);
    });
    let mut state = lock(shared);
    // Another read covered these bytes first.
    if state.tail.path.as_ref() == Some(&path) && state.tail.offset != tail.offset {
        return;
    }
    state.tail = TranscriptTail {
        path: Some(path),
        offset: tail.offset + read,
    };
    let mut held: Vec<String> = uuids
        .iter()
        .filter_map(|uuid| state.uuid_ids.get(uuid).cloned())
        .collect();
    held.extend(ids);
    for id in held {
        if state.taken.contains(&id) || state.in_flight.contains(&id) {
            take(&mut state, &id);
            persist(&mut state, &id);
        }
    }
}

/// The mailbox ids and uuids one transcript row holds as input: a user row's
/// text and uuids, or a folded line's `queued_command` attachment.
fn row_marks(row: &Value) -> (Vec<String>, Vec<String>) {
    let mut ids = Vec::new();
    let mut uuids = Vec::new();
    let content = match row.get("type").and_then(Value::as_str) {
        Some("user") => {
            uuids.extend(row.get("uuid").and_then(Value::as_str).map(str::to_string));
            uuids.extend(
                row.get("mergedSendUuids")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_string),
            );
            row.pointer("/message/content")
        }
        Some("attachment")
            if row.pointer("/attachment/type").and_then(Value::as_str)
                == Some("queued_command") =>
        {
            uuids.extend(
                row.pointer("/attachment/source_uuid")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            );
            row.pointer("/attachment/prompt")
        }
        _ => None,
    };
    // Each text block starts with its own line's marker; a merged row holds
    // one block per line.
    for text in content.into_iter().flat_map(text_blocks) {
        if let Some(id) = message_id_in_text(text) {
            ids.push(id.to_string());
        }
    }
    (ids, uuids)
}

/// The mailbox ids, and the uuids, a resumed session's transcript shows
/// entered its conversation: a user row, or a line folded in mid-turn (a
/// `queued_command` attachment). Claude Code itself recognises a replayed
/// uuid only for a user row; this covers the folded ones too.
fn transcript_marks(environment: &[(String, String)], session: &str) -> TranscriptScan {
    let mut scan = TranscriptScan::default();
    let Some(config) = environment
        .iter()
        .rev()
        .find(|(key, _)| key == "CLAUDE_CONFIG_DIR")
        .map(|(_, value)| PathBuf::from(value))
    else {
        return scan;
    };
    let name = format!("{session}.jsonl");
    let Ok(projects) = std::fs::read_dir(config.join("projects")) else {
        return scan;
    };
    for project in projects.flatten() {
        let path = project.path().join(&name);
        let Ok(file) = std::fs::File::open(&path) else {
            continue;
        };
        let read = each_row(file, |row| {
            let (ids, uuids) = row_marks(row);
            scan.ids.extend(ids);
            scan.uuids.extend(uuids);
        });
        // The live read starts where this scan ended.
        scan.tail = TranscriptTail {
            path: Some(path),
            offset: read,
        };
    }
    scan
}

/// What a launch read from a resumed session's transcript.
#[derive(Default)]
struct TranscriptScan {
    ids: HashSet<String>,
    uuids: HashSet<String>,
    tail: TranscriptTail,
}

/// Call `row` for each complete JSONL row of `reader`, decoding lossily so
/// one invalid byte costs one row, never the rest. Returns the bytes read up
/// to the end of the last complete row; a row still being written is left.
fn each_row(reader: impl std::io::Read, mut row: impl FnMut(&Value)) -> u64 {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    let mut read = 0u64;
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) if line.last() != Some(&b'\n') => break,
            Ok(n) => {
                read += n as u64;
                if let Ok(value) = serde_json::from_str::<Value>(&String::from_utf8_lossy(&line)) {
                    row(&value);
                }
            }
        }
    }
    read
}

/// The text of a message content: the string itself, or each text block.
fn text_blocks(content: &Value) -> Vec<&str> {
    match content {
        Value::String(text) => vec![text.as_str()],
        Value::Array(blocks) => blocks
            .iter()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect(),
        _ => Vec::new(),
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

pub fn quota_from_claude(value: &Value) -> Option<i64> {
    if !mentions_rate_limit(value) {
        return None;
    }
    find_named_reset(value).or_else(|| find_delay(value))
}

/// Inspects the tail of the latest Claude Code session JSONL under
/// `<config_dir>/projects/*/*.jsonl` and returns `Some(reset_at_ms)` if the
/// latest assistant/result turn ended with a rate-limit rejection whose reset
/// time is still in the future (`> now_ms`).
pub fn detect_session_quota_reset_ms(config_dir: &Path, now_ms: i64) -> Option<i64> {
    static CACHE: std::sync::LazyLock<Mutex<HashMap<PathBuf, SessionEvidence>>> =
        std::sync::LazyLock::new(Mutex::default);
    let newest = lock(&CACHE)
        .get(config_dir)
        .map(|entry| entry.inputs.session_paths());
    if let Some(newest) = newest {
        let inputs = SessionInputs::stamp(config_dir, newest);
        if let Some(entry) = lock(&CACHE).get(config_dir)
            && entry.inputs == inputs
        {
            return entry.reset_at_ms.filter(|reset| *reset > now_ms);
        }
    }
    let (inputs, reset_at_ms) = SessionInputs::read(config_dir);
    lock(&CACHE).insert(
        config_dir.to_path_buf(),
        SessionEvidence {
            inputs,
            reset_at_ms,
        },
    );
    reset_at_ms.filter(|reset| *reset > now_ms)
}

/// The reset time the newest session transcript ends on, kept until one of
/// the files it was read from changes: the scheduler asks every second.
struct SessionEvidence {
    inputs: SessionInputs,
    reset_at_ms: Option<i64>,
}

/// Stamps of what the reset time is read from: the operator markers, the
/// session pointer, the project directories (a transcript created or
/// removed) and the three newest transcripts.
#[derive(PartialEq, Eq)]
struct SessionInputs {
    rotated: Option<anyagents_harness::FileStamp>,
    cleared: Option<anyagents_harness::FileStamp>,
    pointer: Option<anyagents_harness::FileStamp>,
    projects: Vec<(PathBuf, Option<anyagents_harness::FileStamp>)>,
    newest: Vec<(PathBuf, Option<anyagents_harness::FileStamp>)>,
}

impl SessionInputs {
    fn session_paths(&self) -> Vec<PathBuf> {
        self.newest.iter().map(|(path, _)| path.clone()).collect()
    }

    fn markers(
        config_dir: &Path,
        projects: Vec<(PathBuf, Option<anyagents_harness::FileStamp>)>,
        newest: Vec<(PathBuf, Option<anyagents_harness::FileStamp>)>,
    ) -> Self {
        use anyagents_harness::FileStamp;
        Self {
            rotated: FileStamp::of(&config_dir.join(".anyagents-profile-rotated-ms")),
            cleared: FileStamp::of(&config_dir.join(".anyagents-quota-cleared-ms")),
            pointer: FileStamp::of(&config_dir.join("claude-session-id")),
            projects,
            newest,
        }
    }

    /// The projects directory and each project directory, stamped.
    fn project_dirs(config_dir: &Path) -> Vec<(PathBuf, Option<anyagents_harness::FileStamp>)> {
        let projects_dir = config_dir.join("projects");
        let mut dirs = vec![(
            projects_dir.clone(),
            anyagents_harness::FileStamp::of(&projects_dir),
        )];
        if let Ok(entries) = std::fs::read_dir(&projects_dir) {
            let mut found: Vec<_> = entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .map(|path| {
                    let stamp = anyagents_harness::FileStamp::of(&path);
                    (path, stamp)
                })
                .collect();
            found.sort_by(|a, b| a.0.cmp(&b.0));
            dirs.extend(found);
        }
        dirs
    }

    /// The stamps a cached reset is checked against: the project directories
    /// and the transcripts the last read found newest, without opening any.
    fn stamp(config_dir: &Path, newest: Vec<PathBuf>) -> Self {
        let newest = newest
            .into_iter()
            .map(|path| {
                let stamp = anyagents_harness::FileStamp::of(&path);
                (path, stamp)
            })
            .collect();
        Self::markers(config_dir, Self::project_dirs(config_dir), newest)
    }

    /// Stamp every input, then read the reset time, so a change during the
    /// read is seen on the next call.
    fn read(config_dir: &Path) -> (Self, Option<i64>) {
        let projects = Self::project_dirs(config_dir);
        let mut transcripts: Vec<(SystemTime, PathBuf, anyagents_harness::FileStamp)> = Vec::new();
        for (dir, _) in projects.iter().skip(1) {
            let Ok(files) = std::fs::read_dir(dir) else {
                continue;
            };
            for file_entry in files.flatten() {
                let p = file_entry.path();
                if p.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                    continue;
                }
                if let Ok(meta) = file_entry.metadata()
                    && let Ok(mtime) = meta.modified()
                {
                    transcripts.push((
                        mtime,
                        p,
                        anyagents_harness::FileStamp::from_metadata(&meta),
                    ));
                }
            }
        }
        transcripts.sort_by_key(|(mtime, _, _)| std::cmp::Reverse(*mtime));
        let inputs = Self::markers(
            config_dir,
            projects,
            transcripts
                .iter()
                .take(3)
                .map(|(_, path, stamp)| (path.clone(), Some(*stamp)))
                .collect(),
        );
        let reset_at_ms = transcripts
            .first()
            .and_then(|(_, newest, _)| session_reset_ms(config_dir, newest));
        (inputs, reset_at_ms)
    }
}

/// The reset time a rate-limit rejection at the end of the transcript names,
/// unless the transcript has not changed since the quota was last cleared.
fn session_reset_ms(config_dir: &Path, transcript: &Path) -> Option<i64> {
    use std::io::{Read, Seek, SeekFrom};
    let rotated_ms = std::fs::read_to_string(config_dir.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let cleared_ms = std::fs::read_to_string(config_dir.join(".anyagents-quota-cleared-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
        .max(rotated_ms);
    let mut file = std::fs::File::open(transcript).ok()?;
    let meta = file.metadata().ok()?;
    if cleared_ms > 0
        && let Ok(mtime) = meta.modified()
        && let Ok(dur) = mtime.duration_since(UNIX_EPOCH)
        && (dur.as_millis() as i64) <= cleared_ms
    {
        return None;
    }
    let len = meta.len();
    let tail_len = len.min(32_768);
    if len > tail_len {
        let _ = file.seek(SeekFrom::End(-(tail_len as i64)));
    }
    let mut buf = Vec::with_capacity(tail_len as usize);
    file.read_to_end(&mut buf).ok()?;
    let text = String::from_utf8_lossy(&buf);
    for line in text.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(val) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if let Some(reset_at_ms) = quota_from_claude(&val) {
            return Some(reset_at_ms);
        }
        if matches!(
            val.get("type").and_then(Value::as_str),
            Some("assistant" | "user" | "api-request")
        ) && val.get("error").is_none()
            && val.get("apiError").is_none()
        {
            return None;
        }
    }
    None
}

fn mentions_rate_limit(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            let category = map.get("category").and_then(Value::as_str);
            let type_ = map.get("type").and_then(Value::as_str);
            let subtype = map.get("subtype").and_then(Value::as_str);
            let error = map.get("error").and_then(Value::as_str);
            let api_error = map.get("apiError").and_then(Value::as_str);
            let api_status = map.get("apiErrorStatus").and_then(Value::as_u64);
            let quota_rejected = map
                .get("quotaLimits")
                .and_then(|q| q.get("status"))
                .and_then(Value::as_str)
                == Some("rejected")
                || map
                    .get("rate_limit_info")
                    .and_then(|q| q.get("status"))
                    .and_then(Value::as_str)
                    == Some("rejected");
            if category == Some("rate_limit")
                || type_ == Some("rate_limit")
                || subtype.is_some_and(|text| text.contains("rate_limit"))
                || error == Some("rate_limit")
                || api_error == Some("usage_limit_reached")
                || api_status == Some(429)
                || quota_rejected
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_uuid_is_stable_per_message_and_well_formed() {
        let uuid = uuid_for("msg_01m7ar22a9e2e929v2kn4afrgg");
        assert_eq!(uuid, uuid_for("msg_01m7ar22a9e2e929v2kn4afrgg"));
        assert_ne!(uuid, uuid_for("msg_01m7ar22a9e2e929v2kn4afrgh"));
        let groups: Vec<&str> = uuid.split('-').collect();
        assert_eq!(
            groups.iter().map(|g| g.len()).collect::<Vec<_>>(),
            [8, 4, 4, 4, 12]
        );
        assert!(groups[2].starts_with('4'), "version 4: {uuid}");
        assert!(matches!(&groups[3][..1], "8" | "9" | "a" | "b"), "{uuid}");
    }

    #[test]
    fn only_a_proof_for_a_line_this_process_wrote_takes_it() {
        let mut state = State::default();
        state.uuid_ids.insert(uuid_for("m1"), "m1".into());
        state.in_flight.insert("m1".into());
        // A user event that only mentions the marker, without isReplay, proves nothing.
        note_proof(
            &mut state,
            &serde_json::json!({"type": "user", "message": {"content": [
                {"type": "text", "text": "anyagents-message:m1\nquoted"}]}}),
        );
        assert!(!state.consumed.contains("m1"));
        // An unknown uuid proves nothing either.
        note_proof(
            &mut state,
            &serde_json::json!({"type": "command_lifecycle", "command_uuid": "other", "state": "started"}),
        );
        assert!(!state.consumed.contains("m1"));
        note_proof(
            &mut state,
            &serde_json::json!({"type": "command_lifecycle", "command_uuid": uuid_for("m1"), "state": "started"}),
        );
        // Taken, but not committed until it is on disk.
        assert!(state.taken.contains("m1"));
        assert!(!state.consumed.contains("m1"));
        assert!(!state.in_flight.contains("m1"));
    }

    #[test]
    fn a_taken_line_is_committed_once_its_transcript_row_is_on_disk() {
        let config = tempfile::tempdir().unwrap();
        let project = config.path().join("projects").join("work");
        std::fs::create_dir_all(&project).unwrap();
        let transcript = project.join("sess.jsonl");
        // The launch scanned what an earlier process stored, an invalid byte
        // included; the live read starts where that scan ended.
        let mut text = String::from("{\"type\":\"user\",\"uuid\":\"earlier\"}\n");
        let mut bytes = text.clone().into_bytes();
        bytes.extend_from_slice(b"{\"type\":\"assistant\",\"x\":\"\xff\"}\n");
        std::fs::write(&transcript, &bytes).unwrap();
        let env = [(
            "CLAUDE_CONFIG_DIR".to_string(),
            config.path().display().to_string(),
        )];
        let scan = transcript_marks(&env, "sess");
        assert!(scan.uuids.contains("earlier"));
        assert_eq!(scan.tail.offset, bytes.len() as u64);
        let mut state = State {
            config_dir: Some(config.path().to_path_buf()),
            session_id: Some("sess".into()),
            tail: scan.tail,
            ..State::default()
        };
        for id in ["m1", "m2", "m3"] {
            state.uuid_ids.insert(uuid_for(id), id.into());
            state.in_flight.insert(id.into());
        }
        for id in ["m1", "m2", "m3"] {
            note_proof(
                &mut state,
                &serde_json::json!({"type": "command_lifecycle", "command_uuid": uuid_for(id), "state": "started"}),
            );
        }
        let shared = Mutex::new(state);
        refresh_transcript(&shared);
        assert!(lock(&shared).consumed.is_empty(), "nothing is on disk yet");
        // A merged user row names its other members in `mergedSendUuids`, a
        // folded line is an attachment with its `source_uuid`; the last row
        // is still being written.
        let merged = serde_json::json!({"type": "user", "uuid": uuid_for("m2"),
                "mergedSendUuids": [uuid_for("m1")],
                "message": {"content": [{"type": "text", "text": "anyagents-message:m1\na"},
                                        {"type": "text", "text": "anyagents-message:m2\nb"}]}});
        let folded = serde_json::json!({"type": "attachment", "attachment": {
            "type": "queued_command", "source_uuid": uuid_for("m3"),
            "prompt": [{"type": "text", "text": "anyagents-message:m3\nc"}]}})
        .to_string();
        text = format!("{merged}\n{}", &folded[..10]);
        bytes.extend_from_slice(text.as_bytes());
        std::fs::write(&transcript, &bytes).unwrap();
        refresh_transcript(&shared);
        {
            let state = lock(&shared);
            assert!(state.consumed.contains("m1") && state.consumed.contains("m2"));
            assert!(
                !state.consumed.contains("m3"),
                "a half-written row proves nothing"
            );
        }
        bytes.extend_from_slice(&folded.as_bytes()[10..]);
        bytes.push(b'\n');
        std::fs::write(&transcript, &bytes).unwrap();
        refresh_transcript(&shared);
        let state = lock(&shared);
        assert!(state.consumed.contains("m3"));
        assert!(state.taken.is_empty());
    }

    #[test]
    fn a_line_claude_code_cancels_or_discards_after_starting_it_goes_back() {
        let mut state = State::default();
        state.uuid_ids.insert(uuid_for("m1"), "m1".into());
        for ending in ["cancelled", "discarded"] {
            state.in_flight.insert("m1".into());
            for verdict in ["started", ending] {
                note_proof(
                    &mut state,
                    &serde_json::json!({"type": "command_lifecycle", "command_uuid": uuid_for("m1"), "state": verdict}),
                );
            }
            assert!(!state.consumed.contains("m1") && !state.taken.contains("m1"));
            assert!(!state.seen.contains("m1"), "{ending}: it is written again");
            assert!(!state.rejected.contains_key("m1"), "{ending}");
        }
        state.in_flight.insert("m1".into());
        note_proof(
            &mut state,
            &serde_json::json!({"type": "command_lifecycle", "command_uuid": uuid_for("m1"), "state": "refused"}),
        );
        assert!(state.rejected.contains_key("m1"));
    }
}
