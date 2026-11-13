//! Codex adapter: one `codex app-server` child, JSON-RPC 2.0, one object per
//! line.
//!
//! Startup sends `initialize` (whose `userAgent` names the peer's version),
//! `initialized`, then `thread/start`, or `thread/resume` when a session id is
//! already known. A resume that fails starts a new thread only when Codex says
//! the thread certainly does not exist (`no rollout found for thread id`, or an
//! id that is not a thread id); any other failure fails the start, so a
//! conversation is never dropped in silence. Before anything is written into
//! a resumed thread, its newest turns are read (`thread/items/list`): a
//! message an earlier process already applied is answered `Duplicate` and
//! never written again. Only these handshake calls (and `cancel`) wait for an
//! answer, and only for a bounded time; delivery never waits.
//!
//! Delivery is `next-step`. A message carries its mailbox id as
//! `clientUserMessageId` and on the first line of its text
//! (`anyagents-message:<id>`). `deliver` writes it as `turn/start` when no turn
//! runs, or as `turn/steer` naming the running turn as `expectedTurnId`, and
//! answers `InFlight`. The vendor proves consumption with the `userMessage`
//! item (`item/started`, `item/completed`) that carries our id as `clientId`
//! when the input enters the conversation; only then is the message
//! `Accepted`. A message is written once per process:
//! - a refusal the turn clears (`-32600`: no active turn, another turn, a turn
//!   that cannot be steered, a draining server) certainly did not apply it: it
//!   is forgotten and offered again once the turn changes, which the turn's
//!   own events announce;
//! - a refusal no retry clears (`-32602`: input too long or invalid; `-32600`
//!   on `turn/start` for a request Codex finds invalid) is answered `Rejected`
//!   on this and every later offer: the loop parks it and tells its sender;
//! - a quota error forgets it and reports the reset time;
//! - any other error leaves its fate unknown, and so does a turn that ends
//!   without a proof for input written into it (a steer can race the turn's
//!   last step into history without an item event; an interrupt drops pending
//!   input). The thread's history settles it once no turn runs: found, it is
//!   consumed; missing, it is forgotten and written again. Nothing new is
//!   written while one is unsettled, so mail keeps its order.
//!
//! A new turn starts only once every earlier input is settled. A turn that
//! completes finishes the messages it consumed. A failed turn surfaces its
//! error as a fault at once and is never re-sent: the supervisor restarts the
//! process and sends a recovery note. `cancel` sends `turn/interrupt`.
//!
//! The process counts as gone only once its output reached its end, so every
//! proof it printed is read first: a write that fails forgets that write, and
//! stopping the process kills it and reads the rest of its output. What it
//! proved stays answerable (`proven`) after it is gone.
//!
//! Token counts come from `thread/tokenUsage/updated`, one usage per completed
//! turn. This vendor reports no currency amount, so the cost stays empty. A
//! rate-limit notice is a wait only when it includes a reset time. Values of
//! `resetsAt` at or above `10^12` are epoch milliseconds; smaller values are
//! epoch seconds.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState, TurnUsage, compose_message, message_id_in_text,
};
use serde_json::{Value, json};

/// How long a handshake call (`initialize`, `thread/start`, `thread/resume`,
/// `turn/interrupt`) may take. Delivery never waits for an answer.
const CALL_WAIT: Duration = Duration::from_secs(60);

/// Items asked for per `thread/items/list` page.
const HISTORY_PAGE: u64 = 50;

/// How many of a resumed thread's newest turns are read for messages an
/// earlier process applied. Mail is written in mailbox order and a turn starts
/// only once every earlier input is settled, so a message an earlier process
/// applied but whose commit was lost is in the last turn or two it ran.
const RESUME_TURNS: usize = 4;

/// How many of the newest turns are read to settle a message whose fate is
/// unknown. Nothing new is written while one is unsettled, so the turn it
/// joined is the newest.
const SETTLE_TURNS: usize = 2;

/// How long a child that exited may keep its stdout open (a descendant
/// holding it) before it is declared dead anyway.
const EXIT_GRACE: Duration = Duration::from_secs(5);

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed(
        "userMessage item carrying our clientId; thread/items/list history",
    ),
    peer: None,
};

/// One long-lived Codex app-server child.
pub struct CodexHarness {
    environment: Vec<(String, String)>,
    shared: Arc<Mutex<State>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct State {
    child: Option<Child>,
    alive: bool,
    /// When the child was seen to have exited while its stdout stayed open
    /// (a descendant holds it).
    exited_at: Option<Instant>,
    /// Bumped when something the host reads changes: what the activity
    /// watcher waits on.
    epoch: u64,
    /// `codex <version>`, from the `initialize` answer.
    peer: Option<String>,
    thread_id: Option<String>,
    thread_invalidated: bool,
    config_dir: Option<PathBuf>,

    /// The running turn, as far as the vendor has said.
    turn: Option<String>,
    /// How each turn this process saw end, ended.
    ended: HashMap<String, TurnEnd>,
    /// The running turn refused a steer it will refuse again (a review or
    /// compact turn): nothing more is written into it.
    unsteerable: Option<String>,
    /// The server is shutting down and takes no more input.
    draining: bool,
    /// The process cannot take input at all (its thread is gone, or it lacks
    /// `turn/start`): only a new process can.
    broken: Option<String>,

    /// Messages written and not yet proved, refused or settled.
    inflight: HashMap<String, Custody>,
    /// Proved consumed by this process, with the turn that consumed each.
    consumed: HashMap<String, Option<String>>,
    /// Consumed into a turn that has not ended yet.
    members: HashMap<String, Vec<String>>,
    /// Consumed into a turn that completed.
    finished: HashSet<String>,
    /// Refused for good, with the vendor's reason. Valid after the process
    /// exits: the same message is refused again.
    rejected: HashMap<String, String>,
    /// Found in a resumed thread's history: an earlier process applied them.
    applied_before: HashSet<String>,
    /// Ids written to the vendor and not refused, in write order.
    written: Vec<String>,
    turns_started: usize,

    /// False until a resumed thread's history has been read.
    history_ready: bool,
    history: Option<Scan>,
    scans: u64,
    /// The peer has no history API: nothing can be checked against it.
    history_missing: bool,
    /// The history could not be read: nothing can be written safely.
    history_failure: Option<String>,

    next_rpc: i64,
    requests: HashMap<i64, Request>,

    last_usage: Option<TurnUsage>,
    /// One per completed turn: the turn and its usage.
    usages: Vec<(String, TurnUsage)>,
    quota_reset_at_ms: Option<i64>,
    pending_turn_error: Option<String>,
    fault_error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TurnEnd {
    Completed,
    Interrupted,
    Failed,
}

/// One message this process wrote and has not settled.
struct Custody {
    stage: Stage,
    /// The turn it joined, once known.
    turn: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// The request carrying it is unanswered.
    Sent { start: bool },
    /// The vendor holds it as pending input of its turn.
    Pending,
    /// Its fate is unknown: the thread's history decides.
    Doubtful,
}

enum Request {
    /// A handshake call the caller waits for, and its answer once it came.
    Call(Option<Result<Value, Value>>),
    /// `turn/start` or `turn/steer` carrying message `id`.
    Input { id: String, start: bool },
    /// One page of history scan `scan`.
    History { scan: u64 },
}

/// A read of the thread's newest turns, newest first, a page at a time.
struct Scan {
    serial: u64,
    purpose: Purpose,
    /// How many turns to read.
    budget: usize,
    /// The next page's cursor.
    cursor: Option<String>,
    /// A page request is on its way.
    waiting: bool,
    turns: Vec<String>,
    /// Message id, and the turn its `userMessage` item is in.
    found: HashMap<String, String>,
}

enum Purpose {
    /// Every message the resumed thread already holds.
    Resume,
    /// Settle these doubtful messages.
    Settle(Vec<String>),
}

/// How a message is written.
enum Target {
    Start,
    Steer(String),
}

/// What an error answer to `turn/start` or `turn/steer` says about the input.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// Certainly not applied; offered again once the turn changes.
    Refused(Refusal),
    /// Certainly not applied, and never will be.
    Rejected(String),
    /// Certainly not applied, and this process can apply nothing: a new one
    /// must.
    Broken(String),
    Quota(i64),
    /// It may or may not have been applied.
    Unknown,
}

#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    /// `no active turn to steer`.
    NoTurn,
    /// `expected active turn id X but found Y`, with Y when it parses.
    OtherTurn(Option<String>),
    /// `Server is draining`.
    Draining,
    /// The turn takes no steer (review, compact, another output schema), or
    /// a steer refusal this adapter does not name: held until the turn ends.
    Held,
}

enum CallFail {
    Dead,
    /// The vendor answered with this error.
    Refused(Value),
    Failed(String),
}

impl State {
    fn touch(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
    }

    fn register(&mut self, request: Request) -> i64 {
        self.next_rpc += 1;
        self.requests.insert(self.next_rpc, request);
        self.next_rpc
    }

    /// A consumption this process proved, or an earlier process's found in
    /// history. Valid after the process exits.
    fn proof(&self, id: &str) -> Option<Proof> {
        if self.consumed.contains_key(id) {
            return Some(Proof::Taken {
                turn_finished: self.finished.contains(id),
            });
        }
        self.applied_before.contains(id).then_some(Proof::Duplicate)
    }

    /// The answer for a message this process already knows about, if any:
    /// what it proved or refused for good first, whether or not it still
    /// runs.
    fn known(&self, id: &str) -> Option<Result<DeliverOutcome, HarnessError>> {
        match self.proof(id) {
            Some(Proof::Taken { turn_finished }) => {
                return Some(Ok(DeliverOutcome::Accepted { turn_finished }));
            }
            Some(Proof::Duplicate) => return Some(Ok(DeliverOutcome::Duplicate)),
            None => {}
        }
        if let Some(reason) = self.rejected.get(id) {
            // An input over Codex's length limit, or an empty one, is this
            // message's own; any other invalid request may be every one's.
            let message_specific = anyagents_harness::names_message_size(reason)
                || reason.contains("must not be empty");
            return Some(Ok(DeliverOutcome::rejected(
                reason.clone(),
                message_specific,
            )));
        }
        if !self.alive {
            return Some(Err(HarnessError::Dead));
        }
        let custody = self.inflight.get(id)?;
        // A message only history can settle, when history cannot be read: a
        // new process reads it again when it resumes the thread.
        if custody.stage == Stage::Doubtful
            && let Some(reason) = &self.history_failure
        {
            return Some(Err(HarnessError::Failed(reason.clone())));
        }
        Some(Ok(DeliverOutcome::InFlight))
    }

    /// Where a new message may be written now, if anywhere.
    fn write_target(&self) -> Option<Target> {
        if !self.history_ready || self.draining {
            return None;
        }
        // A message behind one whose fate is unknown could overtake it, and
        // a steer before the turn it joins is known could miss it.
        if self
            .inflight
            .values()
            .any(|custody| matches!(custody.stage, Stage::Doubtful | Stage::Sent { start: true }))
        {
            return None;
        }
        match &self.turn {
            Some(turn) => {
                (self.unsteerable.as_ref() != Some(turn)).then(|| Target::Steer(turn.clone()))
            }
            None => self.inflight.is_empty().then_some(Target::Start),
        }
    }

    /// The vendor proved `id` consumed into `turn`.
    fn consume(&mut self, id: &str, turn: Option<&str>) {
        let Some(custody) = self.inflight.remove(id) else {
            return;
        };
        let turn = turn.map(str::to_owned).or(custody.turn);
        if let Some(turn) = &turn {
            match self.ended.get(turn) {
                Some(TurnEnd::Completed) => {
                    self.finished.insert(id.to_owned());
                }
                Some(_) => {}
                None => self
                    .members
                    .entry(turn.clone())
                    .or_default()
                    .push(id.to_owned()),
            }
        }
        self.consumed.insert(id.to_owned(), turn);
        self.touch();
    }

    /// Hand `id` back to the mailbox: the vendor does not have it.
    fn forget(&mut self, id: &str) {
        self.inflight.remove(id);
        self.written.retain(|written| written != id);
        self.touch();
    }

    fn turn_started(&mut self, turn: &str) {
        if self.ended.contains_key(turn) || self.turn.as_deref() == Some(turn) {
            return;
        }
        self.turn = Some(turn.to_owned());
        self.touch();
    }

    /// The vendor holds no running turn `turn` any more: input pending in it
    /// is settled from history.
    fn turn_gone(&mut self, turn: &str) {
        if self.turn.as_deref() == Some(turn) {
            self.turn = None;
        }
        if self.unsteerable.as_deref() == Some(turn) {
            self.unsteerable = None;
        }
        for custody in self.inflight.values_mut() {
            if custody.stage == Stage::Pending && custody.turn.as_deref() == Some(turn) {
                custody.stage = Stage::Doubtful;
            }
        }
        self.touch();
    }

    fn on_input_answer(&mut self, id: String, start: bool, answer: Result<Value, Value>) {
        match answer {
            Ok(result) => {
                let turn = if start {
                    result.pointer("/turn/id")
                } else {
                    result.get("turnId")
                }
                .and_then(Value::as_str)
                .filter(|turn| !turn.is_empty())
                .map(str::to_owned);
                if start && let Some(turn) = &turn {
                    self.turn_started(turn);
                }
                let ended = turn
                    .as_ref()
                    .is_some_and(|turn| self.ended.contains_key(turn));
                let Some(custody) = self.inflight.get_mut(&id) else {
                    return;
                };
                if custody.turn.is_none() {
                    custody.turn = turn;
                }
                if matches!(custody.stage, Stage::Sent { .. }) {
                    // Pending in a turn that already ended, or in a turn the
                    // answer does not name: history decides.
                    custody.stage = if ended || custody.turn.is_none() {
                        Stage::Doubtful
                    } else {
                        Stage::Pending
                    };
                    self.touch();
                }
            }
            Err(error) => {
                let Some(custody) = self.inflight.get_mut(&id) else {
                    return;
                };
                let turn = custody.turn.clone();
                match classify(&error, start) {
                    Answer::Unknown => {
                        custody.stage = Stage::Doubtful;
                        self.touch();
                    }
                    Answer::Quota(reset_at_ms) => {
                        self.forget(&id);
                        self.quota_reset_at_ms = Some(reset_at_ms);
                    }
                    Answer::Rejected(reason) => {
                        self.forget(&id);
                        self.rejected.insert(id, reason);
                    }
                    Answer::Broken(reason) => {
                        self.forget(&id);
                        self.broken = Some(reason);
                    }
                    Answer::Refused(refusal) => {
                        self.forget(&id);
                        match refusal {
                            Refusal::NoTurn => {
                                if let Some(turn) = turn {
                                    self.turn_gone(&turn);
                                }
                            }
                            Refusal::OtherTurn(Some(found)) => {
                                if let Some(turn) = turn {
                                    self.turn_gone(&turn);
                                }
                                self.turn_started(&found);
                            }
                            Refusal::Draining => self.draining = true,
                            // Held until the turn ends; its `turn/completed`
                            // announces when the message can go again.
                            Refusal::OtherTurn(None) | Refusal::Held => self.unsteerable = turn,
                        }
                    }
                }
            }
        }
    }

    fn on_notification(&mut self, method: &str, params: &Value) {
        match method {
            "item/started" | "item/completed" => {
                let turn = params.get("turnId").and_then(Value::as_str);
                if let Some(id) = params.get("item").and_then(user_message_id) {
                    if let Some(turn) = turn
                        && self.turn.is_none()
                    {
                        self.turn_started(turn);
                    }
                    let id = id.to_owned();
                    self.consume(&id, turn);
                }
            }
            "turn/started" => {
                if let Some(turn) = turn_id(params) {
                    self.turn_started(turn);
                }
            }
            "turn/completed" => self.turn_completed(params),
            "thread/tokenUsage/updated" => self.last_usage = Some(usage_from_codex(params)),
            "error" => {
                // An error the server retries is not the turn's outcome.
                if params.get("willRetry").and_then(Value::as_bool) != Some(true)
                    && let Some(message) = extract_turn_error(params)
                {
                    self.pending_turn_error = Some(message);
                }
                if is_unrecoverable_thread_error(params) {
                    self.invalidate_thread();
                }
            }
            "account/rateLimits/updated" => {
                if let Some(reset_at_ms) = quota_from_limits(params) {
                    self.quota_reset_at_ms = Some(reset_at_ms);
                    self.touch();
                }
            }
            _ => {}
        }
    }

    fn turn_completed(&mut self, params: &Value) {
        let Some(turn) = turn_id(params).map(str::to_owned) else {
            return;
        };
        let status = params
            .pointer("/turn/status")
            .and_then(Value::as_str)
            .unwrap_or("");
        let error = extract_turn_error(params);
        let end = match status {
            "completed" if error.is_none() => TurnEnd::Completed,
            "interrupted" if error.is_none() => TurnEnd::Interrupted,
            _ => TurnEnd::Failed,
        };
        self.ended.insert(turn.clone(), end);
        self.turn_gone(&turn);
        let members = self.members.remove(&turn).unwrap_or_default();
        match end {
            TurnEnd::Completed => {
                self.finished.extend(members);
                let usage = self.last_usage.take().unwrap_or_default();
                self.usages.push((turn, usage));
                self.pending_turn_error = None;
                self.fault_error = None;
                persist_codex_fault_error(self.config_dir.as_deref(), None);
            }
            TurnEnd::Interrupted => self.pending_turn_error = None,
            TurnEnd::Failed => {
                let message = error
                    .or_else(|| self.pending_turn_error.take())
                    .unwrap_or_else(|| {
                        format!("turn {}", if status.is_empty() { "failed" } else { status })
                    });
                if let Some(reset_at_ms) = quota_from_rpc_error(params) {
                    self.quota_reset_at_ms = Some(reset_at_ms);
                } else if self.quota_reset_at_ms.is_some() && strings_contain_limit(params) {
                    // The quota notice already set the wait.
                } else {
                    self.fault_error = Some(message.clone());
                    persist_codex_fault_error(self.config_dir.as_deref(), Some(&message));
                    if is_unrecoverable_thread_error(params) {
                        self.invalidate_thread();
                    }
                }
            }
        }
    }

    /// The thread can never be continued: the process is ended and the
    /// session pointer retired, so the next process starts a fresh thread.
    /// The process counts as gone once its output ends.
    fn invalidate_thread(&mut self) {
        self.thread_id = None;
        self.turn = None;
        self.thread_invalidated = true;
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
        }
        self.touch();
    }

    fn on_history_page(&mut self, serial: u64, answer: Result<Value, Value>) {
        let Some(mut scan) = self.history.take_if(|scan| scan.serial == serial) else {
            return;
        };
        scan.waiting = false;
        self.touch();
        let result = match answer {
            Ok(result) => result,
            // A peer without the history API: nothing to check against.
            Err(error) if error.get("code").and_then(Value::as_i64) == Some(-32601) => {
                self.history_missing = true;
                self.finish_scan(scan);
                return;
            }
            Err(error) => {
                self.history_failure = Some(format!(
                    "cannot read the thread's history: {}",
                    rpc_failure(&error)
                ));
                return;
            }
        };
        let mut done = true;
        if let Some(entries) = result.get("data").and_then(Value::as_array) {
            done = entries.is_empty();
            for entry in entries {
                let turn = entry.get("turnId").and_then(Value::as_str).unwrap_or("");
                if !scan.turns.iter().any(|seen| seen == turn) {
                    if scan.turns.len() == scan.budget {
                        done = true;
                        break;
                    }
                    scan.turns.push(turn.to_owned());
                }
                if let Some(id) = entry.get("item").and_then(user_message_id) {
                    scan.found.insert(id.to_owned(), turn.to_owned());
                }
            }
        }
        match result.get("nextCursor").and_then(Value::as_str) {
            Some(cursor) if !done => {
                scan.cursor = Some(cursor.to_owned());
                self.history = Some(scan);
            }
            _ => self.finish_scan(scan),
        }
    }

    fn finish_scan(&mut self, scan: Scan) {
        match scan.purpose {
            Purpose::Resume => {
                self.applied_before.extend(scan.found.into_keys());
                self.history_ready = true;
            }
            Purpose::Settle(ids) => {
                for id in ids {
                    if self
                        .inflight
                        .get(&id)
                        .is_none_or(|custody| custody.stage != Stage::Doubtful)
                    {
                        continue;
                    }
                    match scan.found.get(&id) {
                        Some(turn) => {
                            let turn = turn.clone();
                            self.consume(&id, Some(&turn));
                        }
                        None => self.forget(&id),
                    }
                }
            }
        }
        self.touch();
    }

    /// Start a scan for a resumed thread; it gates every write until done.
    fn begin_resume_scan(&mut self) {
        self.history_ready = false;
        self.scans += 1;
        self.history = Some(Scan::new(self.scans, Purpose::Resume, RESUME_TURNS));
    }

    /// The next request the adapter itself owes the vendor: a history page.
    fn next_request(&mut self) -> Option<Value> {
        if !self.alive || self.history_failure.is_some() {
            return None;
        }
        let thread_id = self.thread_id.clone()?;
        if self.history.is_none() {
            // Settle doubtful input once no turn runs and every input has its
            // answer, when the history it joined is final.
            let quiet = self.turn.is_none()
                && !self
                    .inflight
                    .values()
                    .any(|custody| matches!(custody.stage, Stage::Sent { .. }));
            let doubtful: Vec<String> = self
                .inflight
                .iter()
                .filter(|(_, custody)| custody.stage == Stage::Doubtful)
                .map(|(id, _)| id.clone())
                .collect();
            if !quiet || doubtful.is_empty() {
                return None;
            }
            self.scans += 1;
            self.history = Some(Scan::new(
                self.scans,
                Purpose::Settle(doubtful),
                SETTLE_TURNS,
            ));
        }
        let scan = self.history.as_mut()?;
        if scan.waiting {
            return None;
        }
        scan.waiting = true;
        let mut params = json!({
            "threadId": thread_id,
            "sortDirection": "desc",
            "limit": HISTORY_PAGE,
        });
        if let Some(cursor) = &scan.cursor {
            params["cursor"] = json!(cursor);
        }
        let serial = scan.serial;
        let rpc = self.register(Request::History { scan: serial });
        Some(json!({
            "jsonrpc": "2.0",
            "id": rpc,
            "method": "thread/items/list",
            "params": params,
        }))
    }
}

impl Scan {
    fn new(serial: u64, purpose: Purpose, budget: usize) -> Self {
        Self {
            serial,
            purpose,
            budget,
            cursor: None,
            waiting: false,
            turns: Vec::new(),
            found: HashMap::new(),
        }
    }
}

impl CodexHarness {
    /// Start one child and complete the handshake.
    ///
    /// `program` is the `codex` executable, or a test double; it is invoked with
    /// `app-server`. `session_id`, when set, is passed to `thread/resume` instead
    /// of `thread/start`. `environment` overrides those variables in the child;
    /// pass an empty slice for a normal launch.
    ///
    /// If the child exits during startup, the returned adapter is already dead
    /// so the caller can observe that. A failure to spawn is an error.
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
        let harness = Self::launch(program.into(), environment, work_dir)?;
        match harness.handshake(session_id) {
            Ok(()) => Ok(harness),
            Err(err) => {
                if harness.health().process_alive {
                    Err(err)
                } else {
                    Ok(harness)
                }
            }
        }
    }

    /// Ids written to the vendor and not refused, in write order.
    pub fn written_ids(&self) -> Vec<String> {
        lock(&self.shared).written.clone()
    }

    /// Turns opened with `turn/start`.
    pub fn turns_started(&self) -> usize {
        lock(&self.shared).turns_started
    }

    /// Thread id from `thread/start` or `thread/resume`.
    pub fn session_id(&self) -> Option<String> {
        lock(&self.shared).thread_id.clone()
    }

    /// Whether the current thread became permanently unresumable (e.g. sealed
    /// compacted residual or pre-sampling compact failure) and must be replaced.
    pub fn thread_invalidated(&self) -> bool {
        lock(&self.shared).thread_invalidated
    }

    /// Usage of the completed turn that consumed `id`.
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        let state = lock(&self.shared);
        let turn = state.consumed.get(id)?.as_ref()?;
        state
            .usages
            .iter()
            .find(|(done, _)| done == turn)
            .map(|(_, usage)| usage.clone())
    }

    /// Whether a completed turn consumed `id`.
    pub fn is_finished(&self, id: &str) -> bool {
        lock(&self.shared).finished.contains(id)
    }

    /// Wait until a completed turn consumed `id`, or the deadline passes.
    pub fn wait_finished(&self, id: &str, timeout: Duration) -> bool {
        self.wait_pred(timeout, |state| state.finished.contains(id))
    }

    /// Wait until a rate-limit notice with a reset time has been seen.
    pub fn wait_quota(&self, timeout: Duration) -> Option<i64> {
        let mut found = None;
        self.wait_pred(timeout, |state| {
            found = state.quota_reset_at_ms;
            found.is_some()
        });
        found
    }

    /// Wait until a turn fault (a failed turn, or a sign-in failure) has been
    /// recorded.
    pub fn wait_error(&self, timeout: Duration) -> Option<String> {
        let mut found = None;
        self.wait_pred(timeout, |state| {
            found = state.fault_error.clone();
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
            let (guard, _) = self
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|err| err.into_inner());
            state = guard;
        }
    }

    fn launch(
        program: PathBuf,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let env_pairs: Vec<(String, String)> = environment
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let mut cmd = anyagents_harness::spawn_command(&program, &env_pairs)?;
        cmd.arg("app-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = work_dir {
            cmd.current_dir(dir);
        }
        let mut child = cmd
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let (Some(child_stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            anyagents_harness::kill_child(&mut child, &env_pairs);
            let _ = child.wait();
            return Err(HarnessError::Failed("child pipes missing".into()));
        };
        let config_dir = env_pairs
            .iter()
            .find(|(k, _)| k == "CODEX_HOME")
            .or_else(|| env_pairs.iter().find(|(k, _)| k == "HOME"))
            .map(|(_, v)| PathBuf::from(v));
        let shared = Arc::new(Mutex::new(State {
            child: Some(child),
            alive: true,
            config_dir,
            history_ready: true,
            ..State::default()
        }));
        let stdin = Arc::new(Mutex::new(Some(child_stdin)));
        let wake = Arc::new(Condvar::new());
        let mut harness = Self {
            environment: env_pairs,
            shared: Arc::clone(&shared),
            stdin: Arc::clone(&stdin),
            wake: Arc::clone(&wake),
            reader: None,
        };
        // A reader that cannot start leaves the child to `Drop`.
        harness.reader = Some(
            thread::Builder::new()
                .name("anyagents-codex".into())
                .spawn(move || read_stdout(stdout, shared, wake, stdin))
                .map_err(|err| HarnessError::Failed(err.to_string()))?,
        );
        Ok(harness)
    }

    fn handshake(&self, session: Option<&str>) -> Result<(), HarnessError> {
        let hello = self.rpc(
            "initialize",
            json!({"clientInfo": {"name": "anyagents", "version": env!("CARGO_PKG_VERSION")}}),
        )?;
        lock(&self.shared).peer = hello
            .get("userAgent")
            .and_then(Value::as_str)
            .and_then(peer_from_user_agent);
        self.notify("initialized", json!({}))?;
        let (result, resumed) = match session {
            Some(session) => match self.call("thread/resume", self.thread_params(Some(session))) {
                Ok(result) => (result, true),
                // Only a thread Codex certainly does not have is replaced.
                Err(CallFail::Refused(error)) if thread_missing(&error) => {
                    (self.rpc("thread/start", self.thread_params(None))?, false)
                }
                Err(fail) => return Err(fail.into_error("thread/resume")),
            },
            None => (self.rpc("thread/start", self.thread_params(None))?, false),
        };
        let Some(thread_id) = result
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .or(session)
            .map(ToOwned::to_owned)
        else {
            return Err(HarnessError::Failed("thread id missing".into()));
        };
        {
            let mut state = lock(&self.shared);
            state.thread_id = Some(thread_id);
            if resumed {
                state.begin_resume_scan();
            }
        }
        self.pump();
        Ok(())
    }

    /// `thread/start` or `thread/resume` parameters: full access, no
    /// approvals, and the provider and model the agent's `config.toml` names.
    fn thread_params(&self, session: Option<&str>) -> Value {
        let mut params = serde_json::Map::new();
        if let Some(session) = session {
            params.insert("threadId".into(), json!(session));
        }
        if fake_mode() {
            return Value::Object(params);
        }
        params.insert("approvalPolicy".into(), json!("never"));
        params.insert("sandbox".into(), json!("danger-full-access"));
        let (provider, model) = self.codex_config_overrides();
        if let Some(provider) = provider {
            params.insert("modelProvider".into(), json!(provider));
        }
        if let Some(model) = model {
            params.insert("model".into(), json!(model));
        }
        Value::Object(params)
    }

    fn codex_config_overrides(&self) -> (Option<String>, Option<String>) {
        let home = self
            .environment
            .iter()
            .find(|(k, _)| k == "CODEX_HOME")
            .or_else(|| self.environment.iter().find(|(k, _)| k == "HOME"))
            .map(|(_, v)| PathBuf::from(v));
        let Some(dir) = home else {
            return (None, None);
        };
        let Ok(text) = std::fs::read_to_string(dir.join("config.toml")) else {
            return (None, None);
        };
        let mut provider = None;
        let mut model = None;
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                break;
            }
            if let Some(rest) = trimmed.strip_prefix("model_provider") {
                let rest = rest.trim_start();
                if let Some(val) = rest.strip_prefix('=') {
                    let v = val.trim().trim_matches('"').trim();
                    if !v.is_empty() {
                        provider = Some(v.to_string());
                    }
                }
            } else if let Some(rest) = trimmed.strip_prefix("model") {
                if rest.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
                    continue;
                }
                let rest = rest.trim_start();
                if let Some(val) = rest.strip_prefix('=') {
                    let v = val.trim().trim_matches('"').trim();
                    if !v.is_empty() {
                        model = Some(v.to_string());
                    }
                }
            }
        }
        (provider, model)
    }

    fn rpc(&self, method: &str, params: Value) -> Result<Value, HarnessError> {
        self.call(method, params)
            .map_err(|fail| fail.into_error(method))
    }

    /// A notification. One that cannot be written fails the call; the
    /// process counts as gone only once its output ends.
    fn notify(&self, method: &str, params: Value) -> Result<(), HarnessError> {
        let line = json!({"jsonrpc": "2.0", "method": method, "params": params});
        write_stdin(&self.stdin, &line)
            .map_err(|err| HarnessError::Failed(format!("{method}: cannot write: {err}")))
    }

    /// One handshake call: write it and wait, at most [`CALL_WAIT`], for its
    /// answer. A call that cannot be written waits for the process's output
    /// to end, which says it is gone.
    fn call(&self, method: &str, params: Value) -> Result<Value, CallFail> {
        let id = {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(CallFail::Dead);
            }
            state.register(Request::Call(None))
        };
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let unwritten = write_stdin(&self.stdin, &line).err();
        let deadline = Instant::now() + CALL_WAIT;
        let mut state = lock(&self.shared);
        loop {
            if let Some(Request::Call(slot)) = state.requests.get_mut(&id)
                && let Some(answer) = slot.take()
            {
                state.requests.remove(&id);
                return answer.map_err(|error| {
                    if let Some(reset_at_ms) = quota_from_rpc_error(&error) {
                        state.quota_reset_at_ms = Some(reset_at_ms);
                    }
                    CallFail::Refused(error)
                });
            }
            if !state.alive {
                state.requests.remove(&id);
                return Err(CallFail::Dead);
            }
            let now = Instant::now();
            if now >= deadline {
                state.requests.remove(&id);
                return Err(CallFail::Failed(match &unwritten {
                    Some(err) => format!("{method}: cannot write: {err}"),
                    None => format!("{method}: no answer within {}s", CALL_WAIT.as_secs()),
                }));
            }
            let (guard, _) = self
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|err| err.into_inner());
            state = guard;
        }
    }

    /// Send the request the adapter owes the vendor, if any.
    fn pump(&self) {
        let request = lock(&self.shared).next_request();
        if let Some(request) = request
            && write_stdin(&self.stdin, &request).is_err()
        {
            unsent(&self.shared, &request);
        }
    }

    /// Stop the child and read what it printed up to the end of its output,
    /// so every proof it gave is known. Proof state stays.
    fn stop_child(&mut self) {
        *lock(&self.stdin) = None;
        let child = lock(&self.shared).child.take();
        if let Some(mut child) = child {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        // Without a reader (it never started), nothing else says it is gone.
        let mut state = lock(&self.shared);
        if state.alive {
            state.alive = false;
            state.touch();
        }
        drop(state);
        self.wake.notify_all();
    }
}

impl Drop for CodexHarness {
    fn drop(&mut self) {
        self.stop_child();
    }
}

impl Harness for CodexHarness {
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
        let (rpc, line, start) = {
            let mut state = lock(&self.shared);
            if let Some(known) = state.known(id) {
                return known;
            }
            if let Some(reason) = state.history_failure.as_ref().or(state.broken.as_ref()) {
                return Err(HarnessError::Failed(reason.clone()));
            }
            if let Some(reset_at_ms) = state.quota_reset_at_ms {
                return Ok(DeliverOutcome::Quota { reset_at_ms });
            }
            let Some(thread_id) = state.thread_id.clone() else {
                return Err(HarnessError::Failed("no thread".into()));
            };
            let Some(target) = state.write_target() else {
                return Ok(DeliverOutcome::Deferred);
            };
            let input = json!([{
                "type": "text",
                "text": compose_message(id, &message.context, &message.text)
            }]);
            let (method, params, start, turn) = match target {
                Target::Start => (
                    "turn/start",
                    json!({"threadId": thread_id, "input": input, "clientUserMessageId": id}),
                    true,
                    None,
                ),
                Target::Steer(turn) => (
                    "turn/steer",
                    json!({
                        "threadId": thread_id,
                        "expectedTurnId": turn,
                        "input": input,
                        "clientUserMessageId": id,
                    }),
                    false,
                    Some(turn),
                ),
            };
            let rpc = state.register(Request::Input {
                id: id.to_owned(),
                start,
            });
            state.inflight.insert(
                id.to_owned(),
                Custody {
                    stage: Stage::Sent { start },
                    turn,
                },
            );
            state.written.push(id.to_owned());
            state.fault_error = None;
            if start {
                state.turns_started += 1;
            }
            let line = json!({"jsonrpc": "2.0", "id": rpc, "method": method, "params": params});
            (rpc, line, start)
        };
        if write_stdin(&self.stdin, &line).is_err() {
            // A broken pipe took no whole line: nothing was applied. The
            // process counts as gone once its output ends, after every proof
            // it printed is read; that end wakes the host.
            let mut state = lock(&self.shared);
            state.requests.remove(&rpc);
            state.inflight.remove(id);
            state.written.retain(|written| written != id);
            if start {
                state.turns_started -= 1;
            }
            return Ok(DeliverOutcome::Deferred);
        }
        // The answer may already be in: a refusal hands the message back.
        lock(&self.shared)
            .known(id)
            .unwrap_or(Ok(DeliverOutcome::Deferred))
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if lock(&self.shared).alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        if !lock(&self.shared).alive {
            return Err(HarnessError::Dead);
        }
        let result = self.rpc("thread/resume", self.thread_params(Some(session)))?;
        let thread_id = result
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .unwrap_or(session)
            .to_string();
        {
            let mut state = lock(&self.shared);
            state.thread_id = Some(thread_id);
            state.turn = None;
            // Input written to the thread left behind is offered to this one.
            let left: Vec<String> = state.inflight.keys().cloned().collect();
            for id in left {
                state.forget(&id);
            }
            state.begin_resume_scan();
        }
        self.pump();
        Ok(())
    }

    /// Ask the running turn to stop. Its `turn/completed` settles the input
    /// it held.
    fn cancel(&mut self) -> Result<(), HarnessError> {
        let (thread_id, turn_id) = {
            let state = lock(&self.shared);
            if !state.alive {
                return Err(HarnessError::Dead);
            }
            (state.thread_id.clone(), state.turn.clone())
        };
        if let (Some(thread_id), Some(turn_id)) = (thread_id, turn_id) {
            self.rpc(
                "turn/interrupt",
                json!({"threadId": thread_id, "turnId": turn_id}),
            )?;
        }
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let mut state = lock(&self.shared);
        // The child exited, and a descendant keeps its stdout open: past the
        // grace no proof can still arrive from it.
        if state.alive {
            if state.exited_at.is_none()
                && let Some(child) = state.child.as_mut()
                && let Ok(Some(_)) = child.try_wait()
            {
                state.exited_at = Some(Instant::now());
            }
            if state
                .exited_at
                .is_some_and(|exited| exited.elapsed() >= EXIT_GRACE)
            {
                state.alive = false;
            }
        }
        let busy = state.turn.is_some() || !state.inflight.is_empty();
        HarnessHealth {
            process_alive: state.alive,
            turn_state: if busy && state.fault_error.is_none() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: Delivery {
                idempotent: !state.history_missing,
                ..DELIVERY
            }
            .with_peer(state.peer.clone()),
            quota_reset_at_ms: state.quota_reset_at_ms,
            sign_in_fault: state.fault_error.clone(),
        }
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).usages.len() as u32
    }

    /// The total of every turn this process has finished, read when the
    /// mailbox has drained.
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

fn fake_mode() -> bool {
    std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
}

impl CallFail {
    fn into_error(self, method: &str) -> HarnessError {
        match self {
            Self::Dead => HarnessError::Dead,
            Self::Refused(error) => {
                HarnessError::Failed(format!("{method}: {}", rpc_failure(&error)))
            }
            Self::Failed(reason) => HarnessError::Failed(reason),
        }
    }
}

/// Whether a `thread/resume` error says Codex certainly has no such thread:
/// no rollout is recorded under its id, or the id is not a thread id at all.
/// Anything else (a rollout it cannot read or locate, a thread still
/// closing, a timeout) may leave the conversation intact.
fn thread_missing(error: &Value) -> bool {
    let message = error.get("message").and_then(Value::as_str).unwrap_or("");
    message.starts_with("no rollout found for thread id")
        || message.starts_with("invalid thread id")
}

/// A request the adapter owed the vendor could not be written: it is owed
/// again. The process's output ending says it is gone.
fn unsent(shared: &Mutex<State>, request: &Value) {
    let mut state = lock(shared);
    if let Some(rpc) = request.get("id").and_then(Value::as_i64) {
        state.requests.remove(&rpc);
    }
    if let Some(scan) = state.history.as_mut() {
        scan.waiting = false;
    }
}

/// Read the child's output until it closes, which is the one place the
/// process is marked gone: every proof it printed is read first.
fn read_stdout(
    stdout: ChildStdout,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        // A request from the server (an approval): approve it.
        if value.get("method").is_some()
            && let Some(id) = value.get("id")
        {
            let reply = json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"decision": "approved", "approved": true}
            });
            if write_stdin(&stdin, &reply).is_err() {
                // The child waits for an answer it can never get: end it,
                // and its output's end says it is gone.
                if let Some(child) = lock(&shared).child.as_mut() {
                    let _ = child.kill();
                }
            }
            continue;
        }
        let request = {
            let mut state = lock(&shared);
            apply_line(&mut state, &value);
            state.next_request()
        };
        if let Some(request) = request
            && write_stdin(&stdin, &request).is_err()
        {
            unsent(&shared, &request);
        }
        wake.notify_all();
    }
    {
        let mut state = lock(&shared);
        state.alive = false;
        if let Some(child) = state.child.as_mut() {
            let _ = child.try_wait();
        }
        state.touch();
    }
    wake.notify_all();
}

fn watch_child(shared: Arc<Mutex<State>>, wake: Arc<Condvar>, notify: Arc<dyn Fn() + Send + Sync>) {
    let _ = thread::Builder::new()
        .name("anyagents-codex-watch".into())
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

/// One line from the server: a notification or the answer to a request.
fn apply_line(state: &mut State, value: &Value) {
    if let Some(method) = value.get("method").and_then(Value::as_str) {
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        state.on_notification(method, &params);
        return;
    }
    let Some(id) = value.get("id").and_then(Value::as_i64) else {
        return;
    };
    let answer = match (value.get("result"), value.get("error")) {
        (_, Some(error)) => Err(error.clone()),
        (Some(result), None) => Ok(result.clone()),
        (None, None) => return,
    };
    match state.requests.remove(&id) {
        Some(Request::Call(_)) => {
            state.requests.insert(id, Request::Call(Some(answer)));
        }
        Some(Request::Input { id, start }) => state.on_input_answer(id, start, answer),
        Some(Request::History { scan }) => state.on_history_page(scan, answer),
        None => {}
    }
}

/// The mailbox id a `userMessage` item carries: its `clientId`, else the
/// marker on the first line of its first text part.
fn user_message_id(item: &Value) -> Option<&str> {
    if item.get("type").and_then(Value::as_str) != Some("userMessage") {
        return None;
    }
    if let Some(id) = item
        .get("clientId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    {
        return Some(id);
    }
    item.get("content")?
        .as_array()?
        .iter()
        .find(|part| part.get("type").and_then(Value::as_str) == Some("text"))?
        .get("text")
        .and_then(Value::as_str)
        .and_then(message_id_in_text)
}

fn turn_id(params: &Value) -> Option<&str> {
    params
        .pointer("/turn/id")
        .or_else(|| params.get("turnId"))
        .and_then(Value::as_str)
        .filter(|turn| !turn.is_empty())
}

/// `codex <version>` from an `initialize` `userAgent` such as
/// `codex_cli_rs/0.169.0 (Linux; x86_64)`.
fn peer_from_user_agent(agent: &str) -> Option<String> {
    let (_, rest) = agent.split_once('/')?;
    let version = rest.split_whitespace().next()?;
    Some(format!("codex {version}"))
}

/// What an error answer to `turn/start` (`start`) or `turn/steer` means for
/// its input. A refusal is kept for a refusal the running turn's end clears;
/// a refusal that no retry clears is a rejection.
fn classify(error: &Value, start: bool) -> Answer {
    if let Some(reset_at_ms) = quota_from_rpc_error(error) {
        return Answer::Quota(reset_at_ms);
    }
    let message = error.get("message").and_then(Value::as_str).unwrap_or("");
    match error.get("code").and_then(Value::as_i64) {
        Some(-32600) => {
            if message.starts_with("no active turn") {
                Answer::Refused(Refusal::NoTurn)
            } else if message.starts_with("expected active turn id") {
                let found = message
                    .split_once("but found `")
                    .and_then(|(_, rest)| rest.split_once('`'))
                    .map(|(turn, _)| turn.to_owned())
                    .filter(|turn| !turn.is_empty());
                Answer::Refused(Refusal::OtherTurn(found))
            } else if error.pointer("/data/reason").and_then(Value::as_str)
                == Some("serverShuttingDown")
                || message.contains("draining")
            {
                Answer::Refused(Refusal::Draining)
            } else if message.starts_with("thread not found") {
                Answer::Broken(rpc_failure(error))
            } else if message.contains("must not be empty") {
                Answer::Rejected(rpc_failure(error))
            } else if start {
                // No running turn holds a `turn/start` back: Codex found the
                // request itself invalid, and finds it so again.
                Answer::Rejected(rpc_failure(error))
            } else {
                // A turn that takes no steer (review, compact, another output
                // schema): the message waits for the turn to end.
                Answer::Refused(Refusal::Held)
            }
        }
        // A peer without `turn/steer` takes the message as the next turn; one
        // without `turn/start` takes nothing.
        Some(-32601) if start => Answer::Broken(rpc_failure(error)),
        Some(-32601) => Answer::Refused(Refusal::Held),
        // Invalid parameters, such as input over Codex's length limit.
        Some(-32602) => Answer::Rejected(rpc_failure(error)),
        _ => Answer::Unknown,
    }
}

fn is_unrecoverable_thread_error(params: &Value) -> bool {
    let raw = params.to_string().to_ascii_lowercase();
    raw.contains("sealed to another residual")
        || raw.contains("cannot be continued here")
        || raw.contains("pre-sampling compact")
        || raw.contains("context_length_exceeded")
        || raw.contains("payload too large")
        || raw.contains("exceeds the maximum accepted size")
        || raw.contains("\"http_status_code\":413")
}

fn extract_turn_error(params: &Value) -> Option<String> {
    for ptr in [
        "/turn/error/message",
        "/error/message",
        "/turn/error",
        "/error",
        "/message",
    ] {
        if let Some(s) = params
            .pointer(ptr)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(s.to_string());
        }
    }
    for ptr in ["/turn/error", "/error"] {
        if let Some(obj) = params.pointer(ptr).filter(|v| v.is_object()) {
            let raw = obj.to_string();
            if raw != "{}" {
                return Some(raw);
            }
        }
    }
    None
}

fn persist_codex_fault_error(config_dir: Option<&Path>, err: Option<&str>) {
    let Some(dir) = config_dir else {
        return;
    };
    let path = dir.join(".anyagents-last-error.json");
    match err.map(str::trim).filter(|s| !s.is_empty()) {
        Some(msg) => {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
                .unwrap_or(0);
            let payload = json!({
                "error": msg,
                "quota_reset_at_ms": null,
                "timestamp_ms": ts,
            });
            if let Err(err) = anyagents_harness::atomic_write_in_existing_dir(
                &path,
                payload.to_string().as_bytes(),
                0o664,
            ) {
                eprintln!("cannot persist {}: {err}", path.display());
            }
        }
        None => {
            let _ = std::fs::remove_file(&path);
        }
    }
}

fn usage_from_codex(params: &Value) -> TurnUsage {
    let last = params
        .pointer("/tokenUsage/last")
        .or_else(|| params.pointer("/tokenUsage/total"));
    TurnUsage {
        input_tokens: u64_field(last, "inputTokens"),
        output_tokens: u64_field(last, "outputTokens"),
        cache_read_tokens: u64_field(last, "cachedInputTokens"),
        cache_write_tokens: u64_field(last, "cacheWriteInputTokens"),
        reported_cost_micros: None,
    }
}

fn u64_field(value: Option<&Value>, key: &str) -> u64 {
    value
        .and_then(|item| item.get(key))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn quota_from_limits(params: &Value) -> Option<i64> {
    let limits = params.get("rateLimits").unwrap_or(params);
    let reached = limits
        .get("rateLimitReachedType")
        .and_then(Value::as_str)
        .is_some_and(|text| !text.is_empty());
    if !reached && !any_window_full(limits) {
        return None;
    }
    for pointer in [
        "/primary/resetsAt",
        "/secondary/resetsAt",
        "/individualLimit/resetsAt",
    ] {
        if let Some(n) = limits.pointer(pointer).and_then(Value::as_i64) {
            return Some(epoch_ms(n));
        }
    }
    None
}

fn any_window_full(limits: &Value) -> bool {
    [
        "/primary/usedPercent",
        "/secondary/usedPercent",
        "/individualLimit/usedPercent",
    ]
    .iter()
    .any(|pointer| limits.pointer(pointer).is_some_and(percent_full))
}

fn percent_full(value: &Value) -> bool {
    value.as_f64().is_some_and(|number| number >= 100.0)
}

fn quota_from_rpc_error(error: &Value) -> Option<i64> {
    if !strings_contain_limit(error) {
        return None;
    }
    find_resets_at(error).map(epoch_ms)
}

fn strings_contain_limit(value: &Value) -> bool {
    match value {
        Value::String(text) => {
            text.contains("usageLimitExceeded") || text.contains("rateLimitExceeded")
        }
        Value::Array(items) => items.iter().any(strings_contain_limit),
        Value::Object(map) => map.values().any(strings_contain_limit),
        _ => false,
    }
}

fn find_resets_at(value: &Value) -> Option<i64> {
    match value {
        Value::Object(map) => {
            if let Some(n) = map.get("resetsAt").and_then(Value::as_i64) {
                return Some(n);
            }
            map.values().find_map(find_resets_at)
        }
        Value::Array(items) => items.iter().find_map(find_resets_at),
        _ => None,
    }
}

fn epoch_ms(n: i64) -> i64 {
    if n >= 1_000_000_000_000 {
        n
    } else {
        n.saturating_mul(1_000)
    }
}

fn rpc_failure(error: &Value) -> String {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("request failed");
    match error.get("code").and_then(Value::as_i64) {
        Some(code) => format!("{code} {message}"),
        None => message.to_string(),
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

    fn user_message(client_id: Value, text: &str) -> Value {
        json!({
            "type": "userMessage",
            "id": "item-1",
            "clientId": client_id,
            "content": [{"type": "text", "text": text, "text_elements": []}],
        })
    }

    #[test]
    fn a_user_message_names_its_message_by_client_id_then_by_marker() {
        assert_eq!(
            user_message_id(&user_message(json!("msg_1"), "anyagents-message:msg_2\nhi")),
            Some("msg_1")
        );
        assert_eq!(
            user_message_id(&user_message(Value::Null, "anyagents-message:msg_2\nhi")),
            Some("msg_2")
        );
        assert_eq!(
            user_message_id(&user_message(Value::Null, "hi\nanyagents-message:msg_2")),
            None
        );
        let mut agent = user_message(json!("msg_1"), "anyagents-message:msg_1");
        agent["type"] = json!("agentMessage");
        assert_eq!(user_message_id(&agent), None);
    }

    #[test]
    fn the_peer_is_the_version_the_user_agent_names() {
        assert_eq!(
            peer_from_user_agent("codex_cli_rs/0.169.0 (Linux 6.18; x86_64) xterm").as_deref(),
            Some("codex 0.169.0")
        );
        assert_eq!(peer_from_user_agent("fake"), None);
    }

    #[test]
    fn errors_say_whether_the_input_was_applied() {
        let error = |code: i64, message: &str| json!({"code": code, "message": message});
        for start in [false, true] {
            assert_eq!(
                classify(&error(-32600, "no active turn to steer"), start),
                Answer::Refused(Refusal::NoTurn)
            );
            assert_eq!(
                classify(
                    &json!({
                        "code": -32600,
                        "message": "Server is draining; retry after reconnecting",
                        "data": {"reason": "serverShuttingDown"}
                    }),
                    start
                ),
                Answer::Refused(Refusal::Draining)
            );
            // Refused for good: no retry and no new process clears these.
            for refused in [
                error(
                    -32602,
                    "Input exceeds the maximum length of 1048576 characters.",
                ),
                error(-32600, "input must not be empty"),
            ] {
                assert!(
                    matches!(classify(&refused, start), Answer::Rejected(_)),
                    "{refused}"
                );
            }
            assert!(matches!(
                classify(&error(-32600, "thread not found: thread-1"), start),
                Answer::Broken(_)
            ));
            assert_eq!(
                classify(
                    &error(-32603, "failed to steer turn: channel closed"),
                    start
                ),
                Answer::Unknown
            );
            assert_eq!(
                classify(
                    &json!({
                        "code": -32000,
                        "message": "usageLimitExceeded",
                        "data": {"resetsAt": 1_700_000_000}
                    }),
                    start
                ),
                Answer::Quota(1_700_000_000_000)
            );
        }
        assert_eq!(
            classify(
                &error(
                    -32600,
                    "expected active turn id `turn-1` but found `turn-2`"
                ),
                false
            ),
            Answer::Refused(Refusal::OtherTurn(Some("turn-2".into())))
        );
        // A steer the running turn will not take waits for the turn's end;
        // a `turn/start` Codex refuses is invalid in itself.
        let unsteerable = error(-32600, "cannot steer a review turn");
        assert_eq!(
            classify(&unsteerable, false),
            Answer::Refused(Refusal::Held)
        );
        assert!(matches!(classify(&unsteerable, true), Answer::Rejected(_)));
        let missing = error(-32601, "method not found: turn/steer");
        assert_eq!(classify(&missing, false), Answer::Refused(Refusal::Held));
        assert!(matches!(classify(&missing, true), Answer::Broken(_)));
    }

    #[test]
    fn only_a_thread_codex_certainly_lacks_is_replaced_on_resume() {
        let error = |message: &str| json!({"code": -32600, "message": message});
        assert!(thread_missing(&error(
            "no rollout found for thread id 0199"
        )));
        assert!(thread_missing(&error("invalid thread id: bad uuid")));
        for kept in [
            "failed to locate thread id 0199: Permission denied (os error 13)",
            "cannot resume running thread 0199 with history while it is already running",
            "thread 0199 is closing; retry thread/resume after the thread is closed",
            "",
        ] {
            assert!(!thread_missing(&error(kept)), "{kept}");
        }
    }

    #[cfg(unix)]
    fn reader_fixture(script: &str, input: Stdio) -> CodexHarness {
        reader_fixture_with(script, input, State::default())
    }

    /// A child running `script`, read by the adapter's reader, with the
    /// adapter in `state` (alive, its history read).
    #[cfg(unix)]
    fn reader_fixture_with(script: &str, input: Stdio, state: State) -> CodexHarness {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", script])
            .stdin(input)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = Arc::new(Mutex::new(child.stdin.take()));
        let stdout = child.stdout.take().unwrap();
        let shared = Arc::new(Mutex::new(State {
            alive: true,
            child: Some(child),
            history_ready: true,
            ..state
        }));
        let wake = Arc::new(Condvar::new());
        let (s, w, i) = (Arc::clone(&shared), Arc::clone(&wake), Arc::clone(&stdin));
        let reader = thread::spawn(move || read_stdout(stdout, s, w, i));
        CodexHarness {
            environment: Vec::new(),
            shared,
            stdin,
            wake,
            reader: Some(reader),
        }
    }

    // A failed write has no input pipe before the reader starts, rather than
    // depending on the child shell closing it before a protocol write.
    #[cfg(unix)]
    #[test]
    fn failed_required_approval_reply_marks_the_protocol_dead() {
        let harness = reader_fixture(
            "printf '%s\n' '{\"id\":99,\"method\":\"approval/request\"}'; exec sleep 30",
            Stdio::null(),
        );
        assert!(harness.wait_pred(Duration::from_secs(5), |state| !state.alive));
    }

    #[cfg(unix)]
    #[test]
    fn successful_required_approval_reply_preserves_protocol_progress() {
        let harness = reader_fixture(
            "printf '%s\n' '{\"id\":99,\"method\":\"approval/request\"}'; IFS= read -r reply; printf '%s\n' '{\"method\":\"turn/started\",\"params\":{\"turnId\":\"reply-received\"}}'; exec sleep 30",
            Stdio::piped(),
        );
        assert!(
            harness.wait_pred(Duration::from_secs(5), |state| state.turn.as_deref()
                == Some("reply-received"))
        );
        assert!(harness.health().process_alive);
    }

    /// A write the child cannot take forgets that write and hands the message
    /// back; the process counts as gone only when its output ends.
    #[cfg(unix)]
    #[test]
    fn a_failed_write_hands_the_message_back_and_dead_waits_for_the_end_of_output() {
        let dir = tempfile::tempdir().unwrap();
        let go = dir.path().join("go");
        let script = format!("while [ ! -e '{}' ]; do sleep 0.01; done", go.display());
        let mut harness = reader_fixture_with(
            &script,
            Stdio::null(),
            State {
                thread_id: Some("thread-1".into()),
                ..State::default()
            },
        );
        let message = OutboundMessage {
            id: "m1".into(),
            seq: 1,
            text: "hello".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: anyagents_harness::Context::default(),
        };
        assert_eq!(harness.deliver(&message), Ok(DeliverOutcome::Deferred));
        {
            let state = lock(&harness.shared);
            assert!(state.inflight.is_empty() && state.written.is_empty());
            assert_eq!(state.turns_started, 0);
        }
        assert!(
            harness.health().process_alive,
            "dead before its output ended"
        );
        std::fs::write(&go, "").unwrap();
        assert!(harness.wait_dead(Duration::from_secs(5)));
    }

    /// A proof the child printed before the host stopped it is read, and
    /// answered by `proven` once the process is gone.
    #[cfg(unix)]
    #[test]
    fn a_proof_printed_before_stop_is_proven_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let printed = dir.path().join("printed");
        let proof = json!({"method": "item/completed", "params": {"turnId": "t1",
            "item": {"type": "userMessage", "id": "item-1", "clientId": "m1",
                "content": [{"type": "text", "text": "anyagents-message:m1\nhello"}]}}});
        let script = format!(
            "printf '%s\\n' '{proof}'; : > '{}'; exec sleep 30",
            printed.display()
        );
        let mut inflight = HashMap::new();
        inflight.insert(
            "m1".to_string(),
            Custody {
                stage: Stage::Pending,
                turn: Some("t1".into()),
            },
        );
        let mut harness = reader_fixture_with(
            &script,
            Stdio::null(),
            State {
                thread_id: Some("thread-1".into()),
                turn: Some("t1".into()),
                inflight,
                ..State::default()
            },
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !printed.exists() {
            assert!(Instant::now() < deadline, "the child never printed");
            std::thread::sleep(Duration::from_millis(1));
        }
        harness.stop();
        assert!(!harness.health().process_alive);
        assert_eq!(
            harness.proven("m1"),
            Some(Proof::Taken {
                turn_finished: false
            })
        );
        assert_eq!(harness.proven("m2"), None);
    }
}
