//! DeepSeek Harness (`dsh`) web gateway adapter (`dsh --profile web --no-open --port 0`).
//!
//! `dsh` prints its gateway's address with its process launch token (`dsh
//! web: http://127.0.0.1:<port>/?token=<t>`). The token buys a `dsh-auth-*`
//! cookie (`GET /?token=`), and every call is one JSON envelope, `POST
//! /api/<method>` with `{type: "client-request", rpcId, method, payload: {args:
//! {<parameter>: …}}}`, answered `200` with `{type: "server-response", rpcId,
//! result: {ok, value | error: {code, message}}}`. A streaming method goes
//! over the gateway's WebSocket, `GET /api/remote.mux`: `{type: "open",
//! streamId, endpoint, payload}` in, `{type: "item" | "end" | "error",
//! streamId, …}` out (`@deepseek-ai/dsh` 0.1.5-rc.2, workspace packages
//! `dsh-api-gateway`, `dsh-api-session-controller`, `dsh-agent-loop`,
//! `dsh-session` and `dsh-session-persistence-jsonl` 0.1.5-rc.3; earlier
//! gateway notes read 0.2.2-rc.1).
//!
//! - start waits for the address, takes the cookie, creates or adopts the
//!   session (`session/create` with its id), and opens `session/follow` on it
//!   before anything is written. The gateway names no version, so the
//!   handshake names none: the program is never run to ask (`dsh -V` may
//!   update itself);
//! - the follow stream opens with a snapshot: the log's cursor and the
//!   session's projections at it, the pending inbox among them. Every event
//!   after it follows in order, without a gap (`dsh-api-session-controller`).
//!   The opening reads the session's record up to the cursor with
//!   `session/page`; a message it holds as a `user/message` event, whose
//!   `source` carries the mailbox id as `rpcId`, an earlier process consumed:
//!   it is a duplicate and never written again. Unconsumed mailbox items still
//!   sitting in a resumed session's inbox are removed with
//!   `session/updateQueue` (`remove`) before delivery begins. A stream that
//!   ends while dsh runs is opened again and the record read from the last
//!   event seen, so no event is missed; one that cannot be opened again is a
//!   fault;
//! - a mailbox line is `session/prompt` with `mode: "steer"` and the mailbox
//!   id as `requestId`. dsh appends it to the session's `next-step` inbox
//!   (an `agent/inbox/spliced` event) and answers `accepted`, which proves
//!   nothing yet. The running turn claims the whole `next-step` inbox at its
//!   next step and appends each message as a `user/message` event, in order,
//!   before the model request; an idle agent starts a turn that does the
//!   same. That event is the proof (`dsh-agent-loop`); `dsh-session-persistence-jsonl`
//!   batches live appends to disk every 200 ms and drains them on `SIGTERM`,
//!   so only a `SIGKILL` in that 200 ms window can drop a proved row from
//!   disk. When a turn halts (`aborted`, `interrupted`, `blocked`, `error`),
//!   dsh returns to `idle` while retaining pending inbox items
//!   (`session/cancel` passes `keepInbox: true`), so `session/updateQueue`
//!   removes any mailbox item left in the inbox. Once the stream shows an
//!   admitted line out of the inbox, unproved, and no turn running, it is sent
//!   again as input of its own. dsh drops a `requestId` its session holds,
//!   pending or consumed, and still answers `accepted`, so a resend never
//!   applies a line twice;
//! - lines are written one at a time, in mailbox order, each on a thread of its
//!   own and with no timeout. What dsh answers decides the line:
//!   - `session/not-found`, raised before the inbox: the session is created
//!     again and the same request sent once more; HTTP 401: a new cookie, then
//!     the same request once more;
//!   - a malformed request or content (`gateway/bad-request`,
//!     `gateway/arguments-invalid`, `gateway/input-invalid`,
//!     `session/attachment-invalid`, `session/invalid-time-zone`, HTTP 400, 413
//!     or 415): refused for good, so the line is parked as rejected and never
//!     sent again;
//!   - a fault of the process (HTTP 403, `remote-access/denied`,
//!     `session/writer-held`, any other status): the process is replaced;
//!   - an outcome that may have admitted it (`session/agent-busy`,
//!     `gateway/internal`, any other code, a 5xx, a lost connection) is settled
//!     by sending the same `requestId` once more; a second such outcome is a
//!     fault;
//! - a turn runs from its `turn/start` event to its `turn/end`, and the agent
//!   is busy while its inbox holds anything; both come from the same stream.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState, compose_message, marked_message, spawn_command,
};
use serde_json::{Value, json};

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed(
        "the user/message event carrying the request id, on the session's follow stream",
    ),
    peer: None,
};

/// How long a started dsh may take to print its address.
const START_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a call off the delivery path may take: the handshake, the follow
/// opening and each page of the record, cancel and resume; and how long
/// `stop` lets a prompt call that is out answer, and the stream reach its
/// end, before the process goes. A prompt has no limit.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// How many messages one `session/page` call reads of the record.
const PAGE_MESSAGES: u64 = 200;

pub struct DshHarness {
    environment: Vec<(String, String)>,
    work_dir: PathBuf,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    followers: Vec<JoinHandle<()>>,
}

/// What became of one mailbox line this process wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Prompt {
    /// Its `session/prompt` is out.
    Sending,
    /// The session's inbox took it, or held it already. Not a proof.
    Admitted,
    /// dsh refused it for good: no retry and no new process makes it take
    /// this line.
    Rejected(String),
    /// The call ended without saying whether dsh admitted it.
    Unknown,
    /// dsh failed in a way only a new process can clear.
    Fault(String),
}

/// What one `session/prompt` answer says about its line.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Admitted,
    /// HTTP 401: the cookie no longer opens the gateway. Nothing admitted.
    Unauthorized,
    /// `session/not-found`, raised before the inbox. Nothing admitted.
    SessionMissing,
    Rejected(String),
    Fault(String),
    /// It may or may not have been admitted.
    Unknown(String),
}

/// One message in dsh's `next-turn` or `next-step` inbox: its own item id
/// (named to `session/updateQueue`) and the mailbox id it carries, if any.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Queued {
    item_id: String,
    rpc_id: Option<String>,
}

/// What the session's follow stream has shown: its inbox, its turn, and the
/// mailbox ids it consumed.
#[derive(Debug, Default)]
struct Follow {
    /// Which stream is current; one a `resume` replaced stops at once.
    generation: u64,
    /// The `next-turn` and `next-step` inboxes, in order.
    inbox: [Vec<Queued>; 2],
    /// A `turn/start` with no `turn/end` after it.
    turn: bool,
    /// The last event applied.
    seq: i64,
    /// Every mailbox id the session holds as a `user/message`, and whether
    /// an earlier process consumed it.
    consumed: HashMap<String, bool>,
    /// Every mailbox id this process's stream showed entering the inbox.
    seen: HashSet<String>,
    /// The stream's connection, to end it when the process goes.
    socket: Option<TcpStream>,
    /// The current stream's reader runs.
    reading: bool,
    /// A halted turn left mailbox items in the inbox that `clear_stranded` is removing.
    clearing_stranded: bool,
    /// The stream ended and could not be opened again.
    fault: Option<String>,
}

impl Follow {
    /// A turn runs, or one is due: dsh starts one for a pending inbox.
    fn busy(&self) -> bool {
        self.turn || self.inbox.iter().any(|list| !list.is_empty())
    }

    fn pending(&self, id: &str) -> bool {
        self.inbox
            .iter()
            .flatten()
            .any(|item| item.rpc_id.as_deref() == Some(id))
    }

    /// Mailbox item ids still sitting in the inbox after a turn halted.
    fn stranded_items(&self) -> Vec<String> {
        self.inbox
            .iter()
            .flatten()
            .filter(|item| item.rpc_id.is_some())
            .map(|item| item.item_id.clone())
            .collect()
    }

    /// A line the stream showed entering the inbox, and leaving it without
    /// its `user/message`, with no turn left to append one.
    fn dropped(&self, id: &str) -> bool {
        self.seen.contains(id)
            && !self.consumed.contains_key(id)
            && !self.pending(id)
            && !self.busy()
    }

    /// The mailbox ids one event shows entering the inbox or consumed.
    fn note(&mut self, event: &Value, earlier: bool) {
        match event.get("type").and_then(Value::as_str) {
            Some("agent/inbox/spliced") if !earlier => {
                let inserted = event.pointer("/data/inserted").and_then(Value::as_array);
                for id in inserted.into_iter().flatten().filter_map(rpc_id) {
                    self.seen.insert(id);
                }
            }
            Some("user/message") => {
                if let Some(id) = event.get("data").and_then(rpc_id) {
                    self.consumed.entry(id).or_insert(earlier);
                }
            }
            _ => {}
        }
    }

    /// Track a turn's start and end: `(ended, halted)`.
    fn track_turn(&mut self, event: &Value) -> Result<(bool, bool), String> {
        match event.get("type").and_then(Value::as_str) {
            Some("turn/start") => {
                self.turn = true;
                self.clearing_stranded = false;
                Ok((false, false))
            }
            Some("turn/end") => {
                let halted = halted_turn_end(event)?;
                Ok((std::mem::replace(&mut self.turn, false), halted))
            }
            _ => Ok((false, false)),
        }
    }

    /// Apply one live event, the next in the log: whether a turn ended, and
    /// any mailbox items stranded in the inbox by a halted turn.
    fn apply(&mut self, event: &Value) -> Result<(bool, Vec<String>), String> {
        let seq = event.get("seq").and_then(Value::as_i64);
        if seq != Some(self.seq + 1) {
            return Err(format!(
                "dsh session/follow: event {seq:?} after {}",
                self.seq
            ));
        }
        self.seq += 1;
        if event.get("type").and_then(Value::as_str) == Some("agent/inbox/spliced") {
            self.splice(event.get("data").unwrap_or(&Value::Null))?;
        }
        self.note(event, false);
        let (ended, halted) = self.track_turn(event)?;
        let stranded = if halted && !self.turn {
            self.stranded_items()
        } else {
            Vec::new()
        };
        if !stranded.is_empty() {
            self.clearing_stranded = true;
        } else if self.clearing_stranded && self.stranded_items().is_empty() {
            self.clearing_stranded = false;
        }
        Ok((ended, stranded))
    }

    /// One `agent/inbox/spliced`, as dsh's own inbox projection folds it.
    fn splice(&mut self, data: &Value) -> Result<(), String> {
        splice_inbox(&mut self.inbox, data)
    }

    /// Take a stream's opening: the record it read after the last event seen,
    /// then the inbox at its cursor, then the events that arrived meanwhile.
    /// On the first opening of a session the record is an earlier process's
    /// and no turn of this process runs. Returns how many turns ended and any
    /// mailbox items left stranded in the inbox by a halted turn.
    fn open(&mut self, opening: Opening, first: bool) -> Result<(u32, Vec<String>), String> {
        let mut ended = 0u32;
        let mut halted = false;
        for event in &opening.history {
            self.note(event, first);
            if !first {
                if event.get("type").and_then(Value::as_str) == Some("turn/start") {
                    halted = false;
                }
                let (turn_ended, turn_halted) = self.track_turn(event)?;
                if turn_ended {
                    ended = ended.saturating_add(1);
                }
                if turn_halted {
                    halted = true;
                }
            }
        }
        self.inbox = opening.inbox;
        if !first {
            for id in self
                .inbox
                .iter()
                .flatten()
                .filter_map(|item| item.rpc_id.clone())
            {
                self.seen.insert(id);
            }
        }
        self.seq = opening.cursor;
        for event in &opening.live {
            if event.get("type").and_then(Value::as_str) == Some("turn/start") {
                halted = false;
            }
            let (turn_ended, live_stranded) = self.apply(event)?;
            if turn_ended {
                ended = ended.saturating_add(1);
            }
            if !live_stranded.is_empty()
                || (event.get("type").and_then(Value::as_str) == Some("turn/end")
                    && halted_turn_end(event)?)
            {
                halted = true;
            }
        }
        let stranded = if !first && halted && !self.turn {
            self.stranded_items()
        } else {
            Vec::new()
        };
        if !stranded.is_empty() {
            self.clearing_stranded = true;
        } else if self.turn || self.stranded_items().is_empty() {
            self.clearing_stranded = false;
        }
        Ok((ended, stranded))
    }
}

/// Fold one `agent/inbox/spliced` payload into `inbox`.
fn splice_inbox(inbox: &mut [Vec<Queued>; 2], data: &Value) -> Result<(), String> {
    let list = match data.get("target").and_then(Value::as_str) {
        Some("next-turn") => &mut inbox[0],
        Some("next-step") => &mut inbox[1],
        _ => return Err(format!("dsh inbox splice names no inbox: {data}")),
    };
    let start = data.get("start").and_then(Value::as_u64);
    let removed = match data.get("removedCount") {
        None => Some(0),
        Some(count) => count.as_u64(),
    };
    let inserted = data.get("inserted").and_then(Value::as_array);
    let (Some(start), Some(removed), Some(inserted)) = (start, removed, inserted) else {
        return Err(format!("dsh inbox splice is malformed: {data}"));
    };
    let (start, removed) = (start as usize, removed as usize);
    if start > list.len() || removed > list.len() - start {
        return Err(format!(
            "dsh inbox splice out of range: {data} on {} pending",
            list.len()
        ));
    }
    let items = inserted.iter().map(queued).collect::<Result<Vec<_>, _>>()?;
    list.splice(start..start + removed, items);
    Ok(())
}

/// Parse one queued message into its `item_id` and optional mailbox `rpc_id`.
fn queued(message: &Value) -> Result<Queued, String> {
    let item_id = message
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| format!("dsh inbox item has no id: {message}"))?;
    Ok(Queued {
        item_id: item_id.to_string(),
        rpc_id: rpc_id(message),
    })
}

/// Whether a `turn/end` event halted the driver without draining its inbox
/// (`dsh-agent-loop`: `completed` and `max-tokens` keep running while `inbox`
/// holds work; `aborted`, `interrupted`, `blocked` and `error` return to
/// `idle`).
fn halted_turn_end(event: &Value) -> Result<bool, String> {
    match event.pointer("/data/reason/kind").and_then(Value::as_str) {
        Some("completed" | "max-tokens") => Ok(false),
        Some("aborted" | "interrupted" | "blocked" | "error") => Ok(true),
        other => Err(format!(
            "dsh turn/end has unknown reason {other:?}: {event}"
        )),
    }
}

/// The mailbox id a message carries: `source: {kind: "user", rpcId}`.
fn rpc_id(message: &Value) -> Option<String> {
    let source = message.get("source")?;
    (source.get("kind")?.as_str()? == "user")
        .then(|| source.get("rpcId")?.as_str().map(str::to_string))
        .flatten()
}

struct State {
    child: Option<Child>,
    /// False once the reader reached the end of dsh's output.
    alive: bool,
    /// The process is being stopped: a stream that ends is not opened again.
    closing: bool,
    /// Moves on every change a host waits for: an answered prompt, a proof,
    /// a turn that ended, the process gone.
    epoch: u64,
    port: Option<u16>,
    launch_token: Option<String>,
    cookie: Option<String>,
    session: String,
    /// What became of each line this process wrote, by mailbox id. A
    /// rejection stands after the process is gone.
    prompts: HashMap<String, Prompt>,
    /// The line whose `session/prompt` is out. One at a time: dsh admits them
    /// in the order the calls arrive.
    sending: Option<String>,
    follow: Follow,
    turns_completed: u32,
}

impl State {
    fn new(child: Option<Child>, session: String) -> Self {
        Self {
            child,
            alive: true,
            closing: false,
            epoch: 0,
            port: None,
            launch_token: None,
            cookie: None,
            session,
            prompts: HashMap::new(),
            sending: None,
            follow: Follow::default(),
            turns_completed: 0,
        }
    }

    fn changed(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// A line admitted whose entry the stream has not shown yet: dsh appended
    /// it before it answered, so it is on its way.
    fn awaiting(&self) -> bool {
        self.prompts.iter().any(|(id, prompt)| {
            *prompt == Prompt::Admitted
                && !self.follow.consumed.contains_key(id)
                && !self.follow.seen.contains(id)
        })
    }

    /// Once no turn runs and the inbox is empty, any admitted line that entered
    /// the inbox without producing a `user/message` will never be consumed by
    /// that admission: clear its admission so `deliver` offers it again even
    /// after an earlier line starts the next turn.
    fn settle_dropped(&mut self) {
        if self.follow.busy() {
            return;
        }
        let seen = &mut self.follow.seen;
        let consumed = &self.follow.consumed;
        self.prompts.retain(|id, prompt| {
            if *prompt == Prompt::Admitted && seen.contains(id) && !consumed.contains_key(id) {
                seen.remove(id);
                false
            } else {
                true
            }
        });
    }
}

impl DshHarness {
    /// Start dsh, wait for its gateway, create or adopt the session, and open
    /// its follow stream.
    pub fn start(
        program: impl Into<PathBuf>,
        patch_file: Option<&Path>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: &Path,
    ) -> Result<Self, HarnessError> {
        let program = program.into();
        let env_vec: Vec<(String, String)> = environment
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let session = session_id
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(uuid_v4);
        let mut args = vec![
            "--profile".to_string(),
            "web".to_string(),
            "--no-open".to_string(),
        ];
        if let Some(patch) = patch_file {
            args.push("--patch".to_string());
            args.push(patch.display().to_string());
        }
        args.push("--port".to_string());
        args.push("0".to_string());

        let mut cmd = spawn_command(&program, &env_vec)?;
        cmd.args(&args)
            .current_dir(work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = cmd
            .spawn()
            .map_err(|err| HarnessError::Failed(format!("spawn dsh: {err}")))?;
        let Some(stdout) = child.stdout.take() else {
            anyagents_harness::kill_child(&mut child, &env_vec);
            let _ = child.wait();
            return Err(HarnessError::Failed("dsh stdout missing".into()));
        };
        let shared = Arc::new(Mutex::new(State::new(Some(child), session)));
        let wake = Arc::new(Condvar::new());
        let reader_shared = Arc::clone(&shared);
        let reader_wake = Arc::clone(&wake);
        let reader = thread::Builder::new()
            .name("anyagents-dsh-stdout".into())
            .spawn(move || read_stdout(stdout, &reader_shared, &reader_wake))
            .map_err(|err| HarnessError::Failed(err.to_string()))?;

        let mut harness = Self {
            environment: env_vec,
            work_dir: work_dir.to_path_buf(),
            shared,
            wake,
            reader: Some(reader),
            followers: Vec::new(),
        };
        harness.connect()?;
        Ok(harness)
    }

    pub fn session_id(&self) -> Option<String> {
        let st = lock(&self.shared);
        (!st.session.is_empty()).then(|| st.session.clone())
    }

    /// Wait for the gateway's address, take the cookie, create or adopt the
    /// session, and follow it.
    fn connect(&mut self) -> Result<(), HarnessError> {
        let deadline = Instant::now() + START_TIMEOUT;
        let (port, token, session) = {
            let mut st = lock(&self.shared);
            while st.alive && (st.port.is_none() || st.launch_token.is_none()) {
                let now = Instant::now();
                if now >= deadline {
                    return Err(HarnessError::Failed(
                        "dsh did not print its web address".into(),
                    ));
                }
                st = self
                    .wake
                    .wait_timeout(st, deadline - now)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            if !st.alive {
                return Err(HarnessError::Dead);
            }
            (
                st.port.expect("checked"),
                st.launch_token.clone().expect("checked"),
                st.session.clone(),
            )
        };
        let cookie =
            dsh_exchange_token(port, &token, Some(CALL_TIMEOUT)).map_err(HarnessError::Failed)?;
        create_session(port, &cookie, &session, &self.work_dir, Some(CALL_TIMEOUT))?;
        lock(&self.shared).cookie = Some(cookie.clone());
        self.follow(port, &cookie, &session)
    }

    /// Open `session`'s follow stream, read its record, and read it from now
    /// on, in place of any stream before it.
    fn follow(&mut self, port: u16, cookie: &str, session: &str) -> Result<(), HarnessError> {
        let mut opening = open_follow(port, cookie, session, -1).map_err(HarnessError::Failed)?;
        let socket = opening.socket.take().expect("an opening holds its socket");
        let handle = socket.stream.try_clone().ok();
        let generation = {
            let mut st = lock(&self.shared);
            let generation = st.follow.generation + 1;
            if let Some(old) = st.follow.socket.take() {
                let _ = old.shutdown(Shutdown::Both);
            }
            st.follow = Follow {
                generation,
                socket: handle,
                reading: true,
                ..Follow::default()
            };
            st.follow
                .open(opening, true)
                .map_err(HarnessError::Failed)?;
            st.settle_dropped();
            st.changed();
            generation
        };
        self.wake.notify_all();
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let follower = thread::Builder::new()
            .name("anyagents-dsh-follow".into())
            .spawn(move || read_follow(&shared, &wake, generation, socket))
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        self.followers.push(follower);
        Ok(())
    }

    /// The gateway's address and cookie, for a call off the delivery path.
    fn gateway(&self) -> Result<(u16, String, String), HarnessError> {
        let st = lock(&self.shared);
        if !st.alive {
            return Err(HarnessError::Dead);
        }
        match (st.port, st.cookie.clone()) {
            (Some(port), Some(cookie)) => Ok((port, cookie, st.session.clone())),
            _ => Err(HarnessError::Failed("dsh gateway is not connected".into())),
        }
    }

    /// End the process and read its output and its stream to the end. The
    /// readers mark the end, never before, so everything dsh sent is read.
    fn shut_down(&mut self) {
        let child = {
            let mut st = lock(&self.shared);
            st.closing = true;
            st.child.take()
        };
        if let Some(mut child) = child {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        // dsh gone, its end of the stream closes; a stream it left open is
        // closed here.
        let st = lock(&self.shared);
        let mut st = self
            .wake
            .wait_timeout_while(st, CALL_TIMEOUT, |st| st.follow.reading)
            .unwrap_or_else(|e| e.into_inner())
            .0;
        if let Some(socket) = st.follow.socket.take() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        drop(st);
        for handle in self.followers.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Harness for DshHarness {
    /// Write one mailbox line into the session's `next-step` inbox, carrying
    /// its own id as `requestId`, and accept it once the stream shows its
    /// `user/message`.
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let mut st = lock(&self.shared);
        // What dsh proved or refused stands even after the process went.
        match st.follow.consumed.get(&message.id) {
            Some(true) => return Ok(DeliverOutcome::Duplicate),
            Some(false) => {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                });
            }
            None => {}
        }
        if let Some(Prompt::Rejected(reason)) = st.prompts.get(&message.id) {
            // Its content or attachment, or a body too large: this line's
            // own. A malformed call may be every line's.
            let message_specific = reason.contains("gateway/input-invalid")
                || reason.contains("session/attachment-invalid")
                || reason.contains("HTTP 413")
                || anyagents_harness::names_message_size(reason);
            return Ok(DeliverOutcome::rejected(reason.clone(), message_specific));
        }
        if !st.alive {
            return Err(HarnessError::Dead);
        }
        if let Some(fault) = st.follow.fault.clone() {
            return Err(HarnessError::Failed(fault));
        }
        if let Some(Prompt::Fault(reason)) = st.prompts.get(&message.id) {
            let reason = reason.clone();
            st.prompts.remove(&message.id);
            return Err(HarnessError::Failed(reason));
        }
        st.settle_dropped();
        // While a halted turn's stranded inbox items are being removed, hold
        // every unconsumed line at the head so no later arrival jumps ahead.
        if st.follow.clearing_stranded
            || st.prompts.iter().any(|(other, prompt)| {
                other != &message.id && *prompt == Prompt::Admitted && st.follow.dropped(other)
            })
        {
            return Ok(DeliverOutcome::Deferred);
        }
        let replay = match st.prompts.get(&message.id) {
            Some(Prompt::Sending) => return Ok(DeliverOutcome::InFlight),
            // dsh may or may not hold it: the same `requestId` again settles
            // that, since dsh admits a request once.
            Some(Prompt::Unknown) => true,
            // Pending, or claimed by a turn still running.
            Some(Prompt::Admitted) if !st.follow.dropped(&message.id) => {
                return Ok(DeliverOutcome::InFlight);
            }
            // Dropped unconsumed: offered again as input of its own.
            _ => false,
        };
        // One call at a time keeps the inbox in mailbox order.
        if st.sending.is_some() {
            return Ok(DeliverOutcome::Deferred);
        }
        st.sending = Some(message.id.clone());
        st.prompts.insert(message.id.clone(), Prompt::Sending);
        // A fresh send looks for a new inbox entry; a replay after an unknown
        // answer keeps any entry the first call already made, because dsh
        // deduplicates a held requestId without emitting a second splice.
        if !replay {
            st.follow.seen.remove(&message.id);
        }
        drop(st);
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let work_dir = self.work_dir.clone();
        let line = message.clone();
        if let Err(err) = thread::Builder::new()
            .name("anyagents-dsh-prompt".into())
            .spawn(move || carry(&shared, &wake, &work_dir, &line, replay))
        {
            let mut st = lock(&self.shared);
            st.sending = None;
            st.prompts.remove(&message.id);
            return Err(HarnessError::Failed(format!(
                "cannot start the prompt call: {err}"
            )));
        }
        Ok(DeliverOutcome::InFlight)
    }

    /// A `user/message` the stream showed stands after the process is gone.
    fn proven(&self, message_id: &str) -> Option<Proof> {
        lock(&self.shared)
            .follow
            .consumed
            .get(message_id)
            .map(|&earlier| {
                if earlier {
                    Proof::Duplicate
                } else {
                    Proof::Taken {
                        turn_finished: false,
                    }
                }
            })
    }

    /// Let a prompt call that is out answer, then end dsh and read its output
    /// and its stream to the end. A call that does not answer within
    /// [`CALL_TIMEOUT`] ends with the process: the line stays unproved and
    /// the next process sends the same `requestId`, which dsh admits once.
    fn stop(&mut self) {
        {
            let st = lock(&self.shared);
            let _ = self
                .wake
                .wait_timeout_while(st, CALL_TIMEOUT, |st| st.alive && st.sending.is_some())
                .unwrap_or_else(|e| e.into_inner());
        }
        self.shut_down();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if lock(&self.shared).alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let (port, cookie, _) = self.gateway()?;
        create_session(port, &cookie, session, &self.work_dir, Some(CALL_TIMEOUT))?;
        lock(&self.shared).session = session.to_string();
        self.follow(port, &cookie, session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let (port, cookie, session) = self.gateway()?;
        let args = json!({ "request": { "sessionId": session } });
        dsh_call(
            port,
            &cookie,
            "session/cancel",
            "cancel",
            &args,
            Some(CALL_TIMEOUT),
        )
        .map_err(HarnessError::Failed)?;
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let st = lock(&self.shared);
        HarnessHealth {
            process_alive: st.alive,
            // A prompt still being written, or admitted and not yet on the
            // stream, is work under way, not an idle agent.
            turn_state: if st.sending.is_some() || st.follow.busy() || st.awaiting() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: DELIVERY,
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    fn process_id(&self) -> Option<u32> {
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).turns_completed
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let _ = thread::Builder::new()
            .name("anyagents-dsh-watch".into())
            .spawn(move || {
                let mut st = lock(&shared);
                let mut seen = 0u64;
                loop {
                    if !st.alive {
                        drop(st);
                        notify();
                        break;
                    }
                    if st.epoch != seen {
                        seen = st.epoch;
                        drop(st);
                        notify();
                        st = lock(&shared);
                        continue;
                    }
                    st = wake.wait(st).unwrap_or_else(|e| e.into_inner());
                }
            });
    }
}

impl Drop for DshHarness {
    /// A prompt call still out ends with the process: its connection closes.
    fn drop(&mut self) {
        self.shut_down();
    }
}

/// Read dsh's output until it ends: the gateway's address, then nothing the
/// adapter needs. Its end is the process gone.
fn read_stdout(stdout: impl Read, shared: &Mutex<State>, wake: &Condvar) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if let Some((port, token)) = parse_dsh_web_url(&line) {
            let mut st = lock(shared);
            st.port = Some(port);
            st.launch_token = Some(token);
            st.changed();
            drop(st);
            wake.notify_all();
        }
    }
    let mut st = lock(shared);
    st.alive = false;
    if let Some(child) = st.child.as_mut() {
        let _ = child.try_wait();
    }
    st.changed();
    drop(st);
    wake.notify_all();
}

/// Read one follow stream until it ends, applying each event; a stream that
/// ends while dsh runs is opened again from the last event seen, and one that
/// cannot be is a fault. Stops at once when a newer stream replaced it.
fn read_follow(shared: &Mutex<State>, wake: &Condvar, generation: u64, mut socket: Socket) {
    let current = |st: &State| st.follow.generation == generation;
    loop {
        let ended = loop {
            let event = match socket
                .next()
                .map(|text| text.map(|text| follow_item(&text)))
            {
                Ok(Some(Ok(Some(event)))) => event,
                Ok(None | Some(Ok(None))) => continue,
                Ok(Some(Err(err))) | Err(err) => break err,
            };
            let mut st = lock(shared);
            if !current(&st) {
                return;
            }
            let stranded = match st.follow.apply(&event) {
                Ok((turn_ended, stranded)) => {
                    if turn_ended {
                        st.turns_completed = st.turns_completed.saturating_add(1);
                    }
                    st.settle_dropped();
                    st.changed();
                    drop(st);
                    wake.notify_all();
                    stranded
                }
                Err(err) => break err,
            };
            if let Err(err) = clear_stranded(shared, wake, generation, &stranded) {
                let mut st = lock(shared);
                if current(&st) {
                    if st.alive && !st.closing {
                        st.follow.fault = Some(err);
                    }
                    st.follow.reading = false;
                    st.changed();
                    drop(st);
                    wake.notify_all();
                }
                return;
            }
        };
        let (port, token, cookie, session, after) = {
            let mut st = lock(shared);
            if !current(&st) {
                return;
            }
            if !st.alive || st.closing {
                st.follow.reading = false;
                st.changed();
                drop(st);
                wake.notify_all();
                return;
            }
            (
                st.port,
                st.launch_token.clone(),
                st.cookie.clone(),
                st.session.clone(),
                st.follow.seq,
            )
        };
        let reopened = match (port, cookie) {
            (Some(port), Some(cookie)) => match open_follow(port, &cookie, &session, after) {
                Err(err) if err.contains("401") => match token {
                    Some(token) => {
                        dsh_exchange_token(port, &token, Some(CALL_TIMEOUT)).and_then(|fresh| {
                            lock(shared).cookie = Some(fresh.clone());
                            open_follow(port, &fresh, &session, after)
                        })
                    }
                    None => Err(err),
                },
                other => other,
            },
            _ => Err("dsh gateway is not connected".into()),
        };
        let mut st = lock(shared);
        if !current(&st) {
            return;
        }
        let opened = reopened.and_then(|mut opening| {
            let next = opening.socket.take().expect("an opening holds its socket");
            st.follow.socket = next.stream.try_clone().ok();
            let (turns_ended, stranded) = st.follow.open(opening, false)?;
            Ok((next, turns_ended, stranded))
        });
        let stranded = match opened {
            Ok((next, turns_ended, stranded)) => {
                socket = next;
                st.turns_completed = st.turns_completed.saturating_add(turns_ended);
                st.settle_dropped();
                stranded
            }
            Err(err) => {
                if st.alive && !st.closing {
                    st.follow.fault = Some(format!(
                        "{ended}; dsh session/follow could not be opened again: {err}"
                    ));
                }
                st.follow.reading = false;
                st.changed();
                drop(st);
                wake.notify_all();
                return;
            }
        };
        st.changed();
        drop(st);
        wake.notify_all();
        if let Err(err) = clear_stranded(shared, wake, generation, &stranded) {
            let mut st = lock(shared);
            if current(&st) {
                if st.alive && !st.closing {
                    st.follow.fault = Some(err);
                }
                st.follow.reading = false;
                st.changed();
                drop(st);
                wake.notify_all();
            }
            return;
        }
    }
}

/// Remove mailbox items left in dsh's inbox by a halted turn, renewing the
/// cookie once on HTTP 401.
fn clear_stranded(
    shared: &Mutex<State>,
    wake: &Condvar,
    generation: u64,
    stranded: &[String],
) -> Result<(), String> {
    for item_id in stranded {
        let (port, token, cookie, session) = {
            let st = lock(shared);
            if st.follow.generation != generation || !st.alive || st.closing {
                return Ok(());
            }
            (
                st.port,
                st.launch_token.clone(),
                st.cookie.clone(),
                st.session.clone(),
            )
        };
        let (Some(port), Some(mut cookie)) = (port, cookie) else {
            return Err("dsh gateway is not connected".into());
        };
        match remove_queued(port, &cookie, &session, item_id, Some(CALL_TIMEOUT)) {
            Ok(_) => {}
            Err(err) if err.contains("HTTP 401") => {
                let Some(token) = token else {
                    return Err(err);
                };
                cookie = dsh_exchange_token(port, &token, Some(CALL_TIMEOUT))?;
                lock(shared).cookie = Some(cookie.clone());
                remove_queued(port, &cookie, &session, item_id, Some(CALL_TIMEOUT))?;
            }
            Err(err) => return Err(err),
        }
    }
    if !stranded.is_empty() {
        let mut st = lock(shared);
        if st.follow.generation == generation
            && st.follow.clearing_stranded
            && (st.follow.turn || st.follow.stranded_items().is_empty())
        {
            st.follow.clearing_stranded = false;
            st.settle_dropped();
            st.changed();
            drop(st);
            wake.notify_all();
        }
    }
    Ok(())
}

/// The event one follow message carries; `None` for anything else on the
/// stream. The stream's end or error ends it.
fn follow_item(text: &str) -> Result<Option<Value>, String> {
    let frame: Value =
        serde_json::from_str(text).map_err(|e| format!("dsh session/follow: {e}: {text}"))?;
    match frame.get("type").and_then(Value::as_str) {
        Some("item") => {
            let value = frame.get("value").unwrap_or(&Value::Null);
            Ok((value.get("type").and_then(Value::as_str) == Some("event"))
                .then(|| value.get("event").cloned())
                .flatten())
        }
        Some("end") => Err("dsh session/follow ended".into()),
        Some("error") => Err(format!(
            "dsh session/follow failed: {}",
            frame.get("error").unwrap_or(&Value::Null)
        )),
        _ => Ok(None),
    }
}

/// A follow stream just opened: its socket, the cursor and inbox of its
/// snapshot, the record after the last event seen up to the cursor, and the
/// events that arrived while the record was read.
#[derive(Debug)]
struct Opening {
    socket: Option<Socket>,
    cursor: i64,
    inbox: [Vec<Queued>; 2],
    history: Vec<Value>,
    live: Vec<Value>,
}

/// Open `session/follow` and read the record from event `after + 1` to its
/// snapshot's cursor, whole and in order. The snapshot asks for one message
/// only: the record is read by `session/page`, which pages back from the
/// cursor, answering the stream's heartbeat between pages. On the first
/// opening of a session (`after == -1`), any unconsumed mailbox items left in
/// its inbox by an earlier process are removed via `session/updateQueue` and
/// folded out before returning.
fn open_follow(port: u16, cookie: &str, session: &str, after: i64) -> Result<Opening, String> {
    let mut socket = Socket::connect(port, cookie)?;
    let open = json!({
        "type": "open",
        "streamId": "follow",
        "endpoint": "session/follow",
        "payload": {"args": {"request": {
            "address": {"kind": "session", "sessionId": session},
            "maxMessages": 1,
        }}},
    });
    socket.send(0x1, open.to_string().as_bytes())?;
    socket.timeout(Some(CALL_TIMEOUT))?;
    let text = socket
        .next()?
        .ok_or("dsh session/follow sent no snapshot in time")?;
    let frame: Value =
        serde_json::from_str(&text).map_err(|e| format!("dsh session/follow: {e}: {text}"))?;
    let snapshot = frame
        .get("value")
        .filter(|_| frame.get("type").and_then(Value::as_str) == Some("item"))
        .filter(|value| value.get("type").and_then(Value::as_str) == Some("snapshot"))
        .ok_or_else(|| format!("dsh session/follow opened without a snapshot: {text}"))?;
    let (cursor, inbox) = snapshot_inbox(snapshot)?;
    let mut live = Vec::new();
    let mut history = Vec::new();
    let mut before: Option<i64> = None;
    while before.map_or(cursor > after, |first| first > after + 1) {
        socket.drain(&mut live)?;
        let mut request = json!({
            "address": {"kind": "session", "sessionId": session},
            "throughSeq": cursor,
            "maxMessages": PAGE_MESSAGES,
        });
        if let Some(first) = before {
            request["beforeSeq"] = json!(first);
        }
        let page = dsh_call(
            port,
            cookie,
            "session/page",
            "page",
            &json!({ "request": request }),
            Some(CALL_TIMEOUT),
        )?;
        let records = page.get("records").and_then(Value::as_array);
        let events: Vec<Value> = records
            .into_iter()
            .flatten()
            .filter_map(|record| record.get("event").cloned())
            .collect();
        let first = events.first().and_then(|event| event.get("seq")?.as_i64());
        let Some(first) = first else {
            return Err(format!("dsh session/page read nothing before {before:?}"));
        };
        history.splice(0..0, events);
        before = Some(first);
        if page.get("hasMore").and_then(Value::as_bool) != Some(true) {
            break;
        }
    }
    history.retain(|event| event.get("seq").and_then(Value::as_i64) > Some(after));
    let whole = history
        .iter()
        .zip(after + 1..)
        .all(|(event, seq)| event.get("seq").and_then(Value::as_i64) == Some(seq))
        && history.len() as i64 == cursor - after;
    if !whole {
        return Err(format!(
            "dsh session/page did not read events {}..={cursor} whole",
            after + 1
        ));
    }
    socket.drain(&mut live)?;
    if after == -1 {
        let mut folded = inbox.clone();
        for event in &live {
            if event.get("type").and_then(Value::as_str) == Some("agent/inbox/spliced") {
                splice_inbox(&mut folded, event.get("data").unwrap_or(&Value::Null))?;
            }
        }
        let mut removed = HashSet::new();
        for item in folded.iter().flatten().filter(|q| q.rpc_id.is_some()) {
            if remove_queued(port, cookie, session, &item.item_id, Some(CALL_TIMEOUT))? {
                removed.insert(item.item_id.clone());
            }
        }
        while folded
            .iter()
            .flatten()
            .any(|item| removed.contains(&item.item_id))
        {
            let text = socket
                .next()?
                .ok_or("dsh session/follow did not report queue removal in time")?;
            if let Some(event) = follow_item(&text)? {
                if event.get("type").and_then(Value::as_str) == Some("agent/inbox/spliced") {
                    splice_inbox(&mut folded, event.get("data").unwrap_or(&Value::Null))?;
                }
                live.push(event);
            }
        }
    }
    socket.timeout(None)?;
    Ok(Opening {
        socket: Some(socket),
        cursor,
        inbox,
        history,
        live,
    })
}

/// Remove one item from `session`'s pending inbox. `Ok(true)` when dsh removed
/// it and appended `agent/inbox/spliced`, `Ok(false)` when it was already out.
fn remove_queued(
    port: u16,
    cookie: &str,
    session: &str,
    item_id: &str,
    timeout: Option<Duration>,
) -> Result<bool, String> {
    let args = json!({
        "request": {
            "sessionId": session,
            "itemId": item_id,
            "action": { "kind": "remove" },
        }
    });
    match dsh_rpc(port, cookie, "session/updateQueue", item_id, &args, timeout)? {
        Answer::Value(_) => Ok(true),
        Answer::Error { code, .. } if code == "session/queue-item-not-found" => Ok(false),
        Answer::Error { code, message } => {
            Err(format!("dsh session/updateQueue: {code}: {message}"))
        }
        Answer::Status { status, body } => {
            Err(format!("dsh session/updateQueue HTTP {status}: {body}"))
        }
    }
}

/// A snapshot's cursor and the inbox projected at it.
fn snapshot_inbox(snapshot: &Value) -> Result<(i64, [Vec<Queued>; 2]), String> {
    let cursor = snapshot.get("cursor").and_then(Value::as_i64);
    let projections = snapshot.get("projections");
    let as_of = projections
        .and_then(|p| p.get("asOfSeq"))
        .and_then(Value::as_i64);
    let inbox = projections.and_then(|p| p.pointer("/values/inbox"));
    let list = |name: &str| {
        inbox
            .and_then(|inbox| inbox.get(name))
            .and_then(Value::as_array)
            .ok_or_else(|| format!("dsh session/follow snapshot missing inbox.{name}"))?
            .iter()
            .map(queued)
            .collect::<Result<Vec<_>, _>>()
    };
    match (cursor, list("next-turn"), list("next-step")) {
        (Some(cursor), Ok(next_turn), Ok(next_step)) if as_of == Some(cursor) => {
            Ok((cursor, [next_turn, next_step]))
        }
        (_, Err(err), _) | (_, _, Err(err)) => Err(err),
        _ => Err(format!(
            "dsh session/follow snapshot has no inbox at its cursor: cursor {cursor:?}, projections as of {as_of:?}"
        )),
    }
}

/// A WebSocket to the gateway's stream carrier (RFC 6455, client side).
#[derive(Debug)]
struct Socket {
    stream: TcpStream,
    /// Bytes read and not yet parsed into a frame.
    buffer: Vec<u8>,
    /// The fragments of a message still arriving.
    partial: Vec<u8>,
}

impl Socket {
    /// `GET /api/remote.mux` with the cookie, upgraded to a WebSocket.
    fn connect(port: u16, cookie: &str) -> Result<Self, String> {
        let mut stream = open_stream(port, Some(CALL_TIMEOUT))?;
        let key = ["Sec-WebSocket", "Key"].join("-");
        let request = format!(
            "GET /api/remote.mux HTTP/1.1\r\n\
             Host: 127.0.0.1:{port}\r\n\
             Origin: http://127.0.0.1:{port}\r\n\
             Cookie: {cookie}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             {key}: AAAAAAAAAAAAAAAAAAAAAA==\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .map_err(|e| format!("write dsh remote.mux: {e}"))?;
        let mut buffer = Vec::new();
        let end = loop {
            if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
            let mut chunk = [0u8; 1024];
            match stream.read(&mut chunk) {
                Ok(0) => return Err("dsh remote.mux closed during its upgrade".into()),
                Ok(n) => buffer.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(format!("read dsh remote.mux upgrade: {e}")),
            }
        };
        let head = String::from_utf8_lossy(&buffer[..end]).into_owned();
        let status = head.lines().next().unwrap_or_default();
        if status.split_whitespace().nth(1) != Some("101") {
            return Err(format!("dsh remote.mux refused the upgrade: {status}"));
        }
        buffer.drain(..end);
        Ok(Self {
            stream,
            buffer,
            partial: Vec::new(),
        })
    }

    fn timeout(&self, timeout: Option<Duration>) -> Result<(), String> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(|e| e.to_string())
    }

    /// One masked frame.
    fn send(&mut self, opcode: u8, data: &[u8]) -> Result<(), String> {
        let mask = [0x5au8, 0x13, 0xc7, 0x2e];
        let mut frame = vec![0x80 | opcode];
        match data.len() {
            len if len < 126 => frame.push(0x80 | len as u8),
            len if len <= 0xffff => {
                frame.push(0x80 | 126);
                frame.extend_from_slice(&(len as u16).to_be_bytes());
            }
            len => {
                frame.push(0x80 | 127);
                frame.extend_from_slice(&(len as u64).to_be_bytes());
            }
        }
        frame.extend_from_slice(&mask);
        frame.extend(data.iter().enumerate().map(|(i, b)| b ^ mask[i & 3]));
        self.stream
            .write_all(&frame)
            .map_err(|e| format!("write dsh remote.mux: {e}"))
    }

    /// The next text message, answering pings on the way. `None`: nothing
    /// whole arrived within the read timeout. An error: the stream is gone.
    fn next(&mut self) -> Result<Option<String>, String> {
        loop {
            if let Some(text) = self.parse()? {
                return Ok(Some(text));
            }
            let mut chunk = [0u8; 16 * 1024];
            match self.stream.read(&mut chunk) {
                Ok(0) => return Err("dsh remote.mux closed".into()),
                Ok(n) => self.buffer.extend_from_slice(&chunk[..n]),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    return Ok(None);
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(format!("read dsh remote.mux: {e}")),
            }
        }
    }

    /// Take every event already arriving, without waiting for more.
    fn drain(&mut self, events: &mut Vec<Value>) -> Result<(), String> {
        self.timeout(Some(Duration::from_millis(1)))?;
        while let Some(text) = self.next()? {
            events.extend(follow_item(&text)?);
        }
        self.timeout(Some(CALL_TIMEOUT))
    }

    /// The next whole message in the buffer, if one is there.
    fn parse(&mut self) -> Result<Option<String>, String> {
        loop {
            let buf = &self.buffer;
            if buf.len() < 2 {
                return Ok(None);
            }
            let (fin, opcode, masked) = (buf[0] & 0x80 != 0, buf[0] & 0x0f, buf[1] & 0x80 != 0);
            let (len, mut at) = match buf[1] & 0x7f {
                126 if buf.len() >= 4 => (u16::from_be_bytes([buf[2], buf[3]]) as usize, 4),
                127 if buf.len() >= 10 => {
                    let mut be = [0u8; 8];
                    be.copy_from_slice(&buf[2..10]);
                    (u64::from_be_bytes(be) as usize, 10)
                }
                126 | 127 => return Ok(None),
                len => (len as usize, 2),
            };
            let mask = if masked {
                if buf.len() < at + 4 {
                    return Ok(None);
                }
                at += 4;
                Some([buf[at - 4], buf[at - 3], buf[at - 2], buf[at - 1]])
            } else {
                None
            };
            if buf.len() < at + len {
                return Ok(None);
            }
            let mut payload: Vec<u8> = self.buffer.drain(..at + len).skip(at).collect();
            if let Some(mask) = mask {
                payload
                    .iter_mut()
                    .enumerate()
                    .for_each(|(i, b)| *b ^= mask[i & 3]);
            }
            match opcode {
                0x8 => return Err("dsh remote.mux closed by dsh".into()),
                0x9 => self.send(0xA, &payload)?,
                0x0..=0x2 => {
                    self.partial.extend_from_slice(&payload);
                    if fin {
                        let message = std::mem::take(&mut self.partial);
                        return String::from_utf8(message)
                            .map(Some)
                            .map_err(|e| format!("dsh remote.mux sent invalid text: {e}"));
                    }
                }
                _ => {}
            }
        }
    }
}

/// Write one line on this thread and record what became of it. Every outcome
/// wakes the host: it parks a rejection, sends an unsettled line again and
/// replaces a faulty process; an admission waits for its proof.
fn carry(
    shared: &Mutex<State>,
    wake: &Condvar,
    work_dir: &Path,
    message: &OutboundMessage,
    replay: bool,
) {
    let outcome = write_prompt(shared, work_dir, message, replay);
    let mut st = lock(shared);
    if st.sending.as_deref() == Some(message.id.as_str()) {
        st.sending = None;
    }
    st.prompts.insert(message.id.clone(), outcome);
    st.settle_dropped();
    st.changed();
    drop(st);
    wake.notify_all();
}

/// `session/prompt` for one line, and what dsh said about it. A refusal that
/// a new cookie or a new session clears is cleared here, and the same request
/// sent once more.
fn write_prompt(
    shared: &Mutex<State>,
    work_dir: &Path,
    message: &OutboundMessage,
    replay: bool,
) -> Prompt {
    // Nobody waits for a line of a harness that is gone.
    let note = |line: String| {
        if lock(shared).alive {
            eprintln!("anyagents-dsh: {line}");
        }
    };
    let (port, token, cookie, session) = {
        let st = lock(shared);
        (
            st.port,
            st.launch_token.clone(),
            st.cookie.clone(),
            st.session.clone(),
        )
    };
    let (Some(port), Some(token)) = (port, token) else {
        return Prompt::Fault("dsh gateway is not connected".into());
    };
    let renew = || match dsh_exchange_token(port, &token, None) {
        Ok(cookie) => {
            lock(shared).cookie = Some(cookie.clone());
            Ok(cookie)
        }
        Err(err) => Err(Prompt::Fault(format!(
            "dsh refused its own launch token: {err}"
        ))),
    };
    let mut cookie = match cookie {
        Some(cookie) => cookie,
        None => match renew() {
            Ok(cookie) => cookie,
            Err(fault) => return fault,
        },
    };
    let args = json!({
        "request": {
            "sessionId": session,
            "requestId": message.id,
            "content": [{"type": "text", "text": compose_text(message)}],
            "mode": "steer",
        }
    });
    let (mut renewed, mut recreated) = (false, false);
    loop {
        let answer = dsh_rpc(port, &cookie, "session/prompt", &message.id, &args, None);
        match verdict(answer) {
            Verdict::Admitted => return Prompt::Admitted,
            Verdict::Rejected(reason) => {
                note(format!(
                    "{reason}; the line is refused for good and never sent again"
                ));
                return Prompt::Rejected(reason);
            }
            Verdict::Fault(reason) => return Prompt::Fault(reason),
            Verdict::Unknown(reason) if replay => return Prompt::Fault(reason),
            Verdict::Unknown(reason) => {
                note(format!("{reason}; the same request is sent again"));
                return Prompt::Unknown;
            }
            Verdict::Unauthorized if !renewed => {
                renewed = true;
                cookie = match renew() {
                    Ok(cookie) => cookie,
                    Err(fault) => return fault,
                };
            }
            Verdict::Unauthorized => {
                return Prompt::Fault("dsh session/prompt: HTTP 401 with a fresh cookie".into());
            }
            Verdict::SessionMissing if !recreated => {
                recreated = true;
                note(format!(
                    "session {session} not found; it is created again and the line sent once more"
                ));
                if let Err(err) = create_session(port, &cookie, &session, work_dir, None) {
                    return Prompt::Fault(err.to_string());
                }
            }
            Verdict::SessionMissing => {
                return Prompt::Fault(format!(
                    "dsh session/prompt: session {session} not found after it was created again"
                ));
            }
        }
    }
}

/// What one `session/prompt` answer says about its line
/// (`dsh-api-session-controller`, `dsh-api-gateway`, `dsh-client-connection`).
fn verdict(answer: Result<Answer, String>) -> Verdict {
    let answer = match answer {
        Ok(answer) => answer,
        Err(err) => return Verdict::Unknown(err),
    };
    match answer {
        Answer::Value(value) if value.get("accepted") == Some(&Value::Bool(true)) => {
            Verdict::Admitted
        }
        Answer::Value(value) => Verdict::Unknown(format!("dsh session/prompt answered {value}")),
        Answer::Error { code, message } => {
            let reason = format!("dsh session/prompt: {code}: {message}");
            match code.as_str() {
                // Raised before the inbox: the session is not attached, or
                // was disposed during admission.
                "session/not-found" => Verdict::SessionMissing,
                // The request or its content, which no retry changes.
                "gateway/bad-request"
                | "gateway/arguments-invalid"
                | "gateway/input-invalid"
                | "session/attachment-invalid"
                | "session/invalid-time-zone" => Verdict::Rejected(reason),
                // The process: another writer holds the session, or access
                // is denied to this connection.
                "session/writer-held" | "remote-access/denied" | "gateway/peer-disposed" => {
                    Verdict::Fault(reason)
                }
                // The catch-all `session/agent-busy` is raised after the
                // inbox as well as before it; anything else is not known to
                // be raised before it.
                _ => Verdict::Unknown(reason),
            }
        }
        Answer::Status { status, body } => {
            let reason = format!("dsh session/prompt: HTTP {status}: {body}");
            match status {
                401 => Verdict::Unauthorized,
                // Not JSON, larger than the gateway buffers, not JSON typed.
                400 | 413 | 415 => Verdict::Rejected(reason),
                // A handler that threw: before or after the inbox.
                500.. => Verdict::Unknown(reason),
                // 403: a Host or Origin the gateway does not trust.
                _ => Verdict::Fault(reason),
            }
        }
    }
}

/// Create the session, or adopt it when dsh already has it.
fn create_session(
    port: u16,
    cookie: &str,
    session: &str,
    work_dir: &Path,
    timeout: Option<Duration>,
) -> Result<(), HarnessError> {
    let args = json!({
        "request": {
            "sessionId": session,
            "cwd": work_dir.display().to_string(),
        }
    });
    dsh_call(port, cookie, "session/create", "init", &args, timeout)
        .map(|_| ())
        .map_err(HarnessError::Failed)
}

pub fn write_dsh_mcp_patch(
    config_dir: &Path,
    model: Option<&str>,
    command: &Path,
    args: &[String],
) -> Result<PathBuf, anyagents_core::Error> {
    let dsh_home = config_dir.join(".dsh");
    std::fs::create_dir_all(&dsh_home)?;
    let patch_path = config_dir.join("dsh-mcp-patch.yml");
    let args_yaml = args
        .iter()
        .map(|a| format!("      - '{}'\n", a.replace('\'', "''")))
        .collect::<String>();
    let mut yaml = String::from("- id: agent-instructions\n  disabled: false\n");
    if let Some(raw_model) = model.map(str::trim).filter(|m| !m.is_empty()) {
        let clean = raw_model
            .strip_prefix("deepseek/")
            .unwrap_or(raw_model)
            .strip_prefix("deepseek-official/")
            .unwrap_or(raw_model);
        yaml.push_str(&format!(
            "- id: agent-default-model\n  config:\n    provider: deepseek-official\n    model: '{clean}'\n"
        ));
    }
    yaml.push_str(&format!(
        "- insert:\n    - id: mcp-anyagents\n      name: '@deepseek-ai/dsh-mcp-client'\n      config:\n        serverName: anyagents\n        transport: stdio\n        command: '{}'\n        args:\n{args_yaml}",
        command.display().to_string().replace('\'', "''")
    ));
    anyagents_harness::atomic_write_nonempty_file(&patch_path, yaml.as_bytes(), 0o664)?;
    Ok(patch_path)
}

fn parse_dsh_web_url(line: &str) -> Option<(u16, String)> {
    let idx = line.find("http://127.0.0.1:")?;
    let rest = &line[idx + "http://127.0.0.1:".len()..];
    let (port_str, query_part) = rest.split_once("/?token=")?;
    let port = port_str.trim().parse::<u16>().ok()?;
    let token = query_part.split_whitespace().next()?.trim().to_string();
    (!token.is_empty()).then_some((port, token))
}

/// Connect to the gateway, with `timeout` bounding every read and write.
fn open_stream(port: u16, timeout: Option<Duration>) -> Result<TcpStream, String> {
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|e| format!("connect dsh 127.0.0.1:{port}: {e}"))?;
    let _ = stream.set_read_timeout(timeout);
    let _ = stream.set_write_timeout(timeout);
    Ok(stream)
}

fn dsh_exchange_token(port: u16, token: &str, timeout: Option<Duration>) -> Result<String, String> {
    let mut stream = open_stream(port, timeout)?;
    let req = format!(
        "GET /?token={token} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .and_then(|_| stream.flush())
        .map_err(|e| format!("write dsh auth: {e}"))?;
    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .map_err(|e| format!("read dsh auth: {e}"))?;
    for line in resp.lines() {
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("set-cookie:")
            && let Some((_, val)) = line.split_once(':')
            && let Some(cookie_pair) = val.trim().split(';').next()
            && cookie_pair.trim().starts_with("dsh-auth-")
        {
            return Ok(cookie_pair.trim().to_string());
        }
    }
    Err("dsh auth response missing Set-Cookie".into())
}

/// What the gateway answered one call with.
#[derive(Debug, PartialEq)]
enum Answer {
    /// `result: {ok: true, value}`.
    Value(Value),
    /// `result: {ok: false, error: {code, message}}`.
    Error { code: String, message: String },
    /// No envelope: the gateway refused the request itself (401 without the
    /// cookie, 403 for a foreign origin, 415, 500 when a handler threw).
    Status { status: u16, body: String },
}

/// One call through the gateway. `Err`: no complete answer arrived.
fn dsh_rpc(
    port: u16,
    cookie: &str,
    method: &str,
    rpc_id: &str,
    args: &Value,
    timeout: Option<Duration>,
) -> Result<Answer, String> {
    let payload = json!({
        "type": "client-request",
        "rpcId": rpc_id,
        "method": method,
        "payload": { "args": args },
    });
    let body = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    let mut stream = open_stream(port, timeout)?;
    let mut req = format!(
        "POST /api/{method} HTTP/1.0\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Origin: http://127.0.0.1:{port}\r\n\
         Cookie: {cookie}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    req.extend_from_slice(&body);
    stream
        .write_all(&req)
        .and_then(|_| stream.flush())
        .map_err(|e| format!("write dsh {method}: {e}"))?;
    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .map_err(|e| format!("read dsh {method}: {e}"))?;
    parse_answer(method, &resp)
}

/// Read one HTTP answer from the gateway: a `server-response` envelope on
/// 200, the status and body otherwise.
fn parse_answer(method: &str, resp: &str) -> Result<Answer, String> {
    let (head, body) = resp
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("dsh {method}: answer ended before its body"))?;
    let status = head
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("dsh {method}: answer has no status line"))?;
    if status != 200 {
        return Ok(Answer::Status {
            status,
            body: body.trim().to_string(),
        });
    }
    let envelope: Value = serde_json::from_str(body.trim())
        .map_err(|e| format!("dsh {method}: undecodable answer: {e}"))?;
    let result = envelope
        .get("result")
        .filter(|_| envelope.get("type").and_then(Value::as_str) == Some("server-response"))
        .ok_or_else(|| format!("dsh {method}: answer is not a server-response: {envelope}"))?;
    match result.get("ok").and_then(Value::as_bool) {
        Some(true) => Ok(Answer::Value(
            result.get("value").cloned().unwrap_or(Value::Null),
        )),
        Some(false) => Ok(Answer::Error {
            code: result
                .pointer("/error/code")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            message: result
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        None => Err(format!("dsh {method}: answer has no result: {envelope}")),
    }
}

/// One call off the delivery path: its value, or why there is none.
fn dsh_call(
    port: u16,
    cookie: &str,
    method: &str,
    rpc_id: &str,
    args: &Value,
    timeout: Option<Duration>,
) -> Result<Value, String> {
    match dsh_rpc(port, cookie, method, rpc_id, args, timeout)? {
        Answer::Value(value) => Ok(value),
        Answer::Error { code, message } => Err(format!("dsh {method}: {code}: {message}")),
        Answer::Status { status, body } => Err(format!("dsh {method} HTTP {status}: {body}")),
    }
}

fn compose_text(message: &OutboundMessage) -> String {
    let context = &message.context;
    let known = context.sender.is_some()
        || context.chat_id.is_some()
        || context.message_id.is_some()
        || context.destination.is_some();
    if !known {
        return marked_message(&message.id, &message.text);
    }
    compose_message(&message.id, context, &message.text)
}

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = u128::from(std::process::id());
    let a = nanos ^ (pid << 64);
    let b = nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C834);
    let raw = (a & 0xFFFF_FFFF_FFFF_0FFF_FFFF_FFFF_FFFF_FFFF)
        | (0x4u128 << 76)
        | ((b & 0x3FFF_FFFF_FFFF_FFFF) | (0x2u128 << 62));
    let hex = format!("{raw:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http(status: &str, body: &str) -> String {
        format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\n\r\n{body}")
    }

    #[test]
    fn an_answer_is_read_from_its_envelope_never_from_the_status_alone() {
        // Every endpoint answers 200; whether it took the call is in `result`.
        assert_eq!(
            parse_answer(
                "session/prompt",
                &http(
                    "200 OK",
                    r#"{"type":"server-response","rpcId":"m1","result":{"ok":true,"value":{"accepted":true}}}"#
                )
            ),
            Ok(Answer::Value(json!({"accepted": true})))
        );
        assert_eq!(
            parse_answer(
                "session/prompt",
                &http(
                    "200 OK",
                    r#"{"type":"server-response","rpcId":"m1","result":{"ok":false,"error":{"code":"session/model-unavailable","message":"no adapter","details":{}}}}"#
                )
            ),
            Ok(Answer::Error {
                code: "session/model-unavailable".into(),
                message: "no adapter".into()
            })
        );
        assert_eq!(
            parse_answer("session/prompt", &http("401 Unauthorized", "unauthorized")),
            Ok(Answer::Status {
                status: 401,
                body: "unauthorized".into()
            })
        );
        // A cut answer, or one that is not an envelope, says nothing.
        assert!(parse_answer("session/prompt", "HTTP/1.1 200 OK\r\n").is_err());
        assert!(parse_answer("session/prompt", &http("200 OK", r#"{"ok":true}"#)).is_err());
    }

    fn connected_state(port: u16) -> Arc<Mutex<State>> {
        let mut state = State::new(None, "s".into());
        state.port = Some(port);
        state.cookie = Some("dsh-auth-test=1".into());
        Arc::new(Mutex::new(state))
    }

    #[test]
    fn a_prompt_names_its_request_and_carries_the_mailbox_id() {
        let (port, seen) = scripted(vec![envelope(json!({"ok": true, "value": {"items": []}}))]);
        let shared = connected_state(port);
        lock(&shared).launch_token = Some("t".into());
        let message = OutboundMessage {
            id: "msg_01prompt".into(),
            seq: 1,
            text: "hello".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Default::default(),
        };
        // The scripted gateway answers a listing, not `accepted`: an answer
        // that does not say the line was taken is not taken.
        assert_eq!(
            write_prompt(&shared, Path::new("/"), &message, false),
            Prompt::Unknown
        );
        assert!(matches!(
            write_prompt(&shared, Path::new("/"), &message, true),
            Prompt::Fault(_)
        ));
        let request = prompts(&seen)[0].body.clone();
        assert_eq!(request["type"], "client-request");
        assert_eq!(request["method"], "session/prompt");
        assert_eq!(request["rpcId"], "msg_01prompt");
        let args = &request["payload"]["args"]["request"];
        assert_eq!(args["sessionId"], "s");
        assert_eq!(args["requestId"], "msg_01prompt");
        assert_eq!(args["mode"], "steer");
        assert!(
            args["content"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("anyagents-message:msg_01prompt\n")
        );
    }

    fn message(id: &str) -> OutboundMessage {
        OutboundMessage {
            id: id.into(),
            seq: 1,
            text: "hello".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Default::default(),
        }
    }

    /// One request the scripted gateway took.
    #[derive(Clone, Debug)]
    struct Seen {
        target: String,
        cookie: Option<String>,
        body: Value,
    }

    /// A gateway that answers each request with the next of `replies` (whole
    /// HTTP answers), and the last one again once they run out.
    fn scripted(replies: Vec<String>) -> (u16, Arc<Mutex<Vec<Seen>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&seen);
        thread::spawn(move || {
            let mut replies = replies.into_iter().peekable();
            let mut last = String::new();
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                let _ = reader.read_line(&mut request_line);
                let (mut length, mut cookie) = (0usize, None);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                    if lower.starts_with("cookie:") {
                        cookie = line.split_once(':').map(|(_, v)| v.trim().to_string());
                    }
                }
                let mut body = vec![0u8; length];
                let _ = reader.read_exact(&mut body);
                lock(&recorded).push(Seen {
                    target: request_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_string(),
                    cookie,
                    body: serde_json::from_slice(&body).unwrap_or(Value::Null),
                });
                if let Some(next) = replies.next() {
                    last = next;
                }
                let _ = stream.write_all(last.as_bytes());
            }
        });
        (port, seen)
    }

    fn envelope(result: Value) -> String {
        http(
            "200 OK",
            &json!({"type": "server-response", "rpcId": "r", "result": result}).to_string(),
        )
    }

    fn accepted() -> String {
        envelope(json!({"ok": true, "value": {"accepted": true}}))
    }

    fn refusal(code: &str) -> String {
        envelope(json!({"ok": false, "error": {"code": code, "message": "no", "details": {}}}))
    }

    /// An adapter connected to the gateway on `port`, without a process.
    fn harness_on(port: u16) -> DshHarness {
        let shared = connected_state(port);
        lock(&shared).launch_token = Some("t".into());
        DshHarness {
            environment: Vec::new(),
            work_dir: PathBuf::from("/"),
            shared,
            wake: Arc::new(Condvar::new()),
            reader: None,
            followers: Vec::new(),
        }
    }

    /// Offer `message`, wait until its prompt call has answered, and offer it
    /// again: what the adapter then says about it.
    fn settle(
        harness: &mut DshHarness,
        message: &OutboundMessage,
    ) -> Result<DeliverOutcome, HarnessError> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let first = harness.deliver(message);
        if first != Ok(DeliverOutcome::InFlight) {
            return first;
        }
        while lock(&harness.shared).prompts.get(&message.id) == Some(&Prompt::Sending) {
            assert!(Instant::now() < deadline, "the prompt never answered");
            thread::sleep(Duration::from_millis(5));
        }
        harness.deliver(message)
    }

    /// The line is admitted: in flight, and proved by nothing yet.
    fn assert_admitted(harness: &DshHarness, id: &str) {
        assert_eq!(
            lock(&harness.shared).prompts.get(id),
            Some(&Prompt::Admitted)
        );
        assert_eq!(harness.proven(id), None);
    }

    fn prompts(seen: &Mutex<Vec<Seen>>) -> Vec<Seen> {
        lock(seen)
            .iter()
            .filter(|seen| seen.target == "/api/session/prompt")
            .cloned()
            .collect()
    }

    #[test]
    fn each_answer_names_what_became_of_the_line() {
        let error = |code: &str| {
            verdict(Ok(Answer::Error {
                code: code.into(),
                message: "m".into(),
            }))
        };
        let status = |status: u16| {
            verdict(Ok(Answer::Status {
                status,
                body: "b".into(),
            }))
        };
        assert_eq!(
            verdict(Ok(Answer::Value(json!({"accepted": true})))),
            Verdict::Admitted
        );
        assert_eq!(error("session/not-found"), Verdict::SessionMissing);
        assert_eq!(status(401), Verdict::Unauthorized);
        for code in [
            "gateway/bad-request",
            "gateway/arguments-invalid",
            "gateway/input-invalid",
            "session/attachment-invalid",
            "session/invalid-time-zone",
        ] {
            assert!(matches!(error(code), Verdict::Rejected(_)), "{code}");
        }
        for code in [400, 413, 415] {
            assert!(matches!(status(code), Verdict::Rejected(_)), "{code}");
        }
        for code in [
            "session/writer-held",
            "remote-access/denied",
            "gateway/peer-disposed",
        ] {
            assert!(matches!(error(code), Verdict::Fault(_)), "{code}");
        }
        assert!(matches!(status(403), Verdict::Fault(_)));
        assert!(matches!(status(404), Verdict::Fault(_)));
        for code in ["session/agent-busy", "gateway/internal", "dsh/unheard-of"] {
            assert!(matches!(error(code), Verdict::Unknown(_)), "{code}");
        }
        assert!(matches!(status(500), Verdict::Unknown(_)));
        assert!(matches!(status(502), Verdict::Unknown(_)));
        assert!(matches!(
            verdict(Ok(Answer::Value(json!({"accepted": false})))),
            Verdict::Unknown(_)
        ));
        assert!(matches!(
            verdict(Err("connection reset".into())),
            Verdict::Unknown(_)
        ));
    }

    #[test]
    fn a_poison_line_is_rejected_once_and_never_sent_again() {
        let (port, seen) = scripted(vec![refusal("gateway/input-invalid")]);
        let mut harness = harness_on(port);
        let line = message("msg_poison");
        let Ok(DeliverOutcome::Rejected { reason, .. }) = settle(&mut harness, &line) else {
            panic!("not rejected");
        };
        assert!(reason.contains("gateway/input-invalid"), "{reason}");
        for _ in 0..20 {
            assert_eq!(
                harness.deliver(&line),
                Ok(DeliverOutcome::Rejected {
                    reason: reason.clone(),
                    message_specific: true,
                })
            );
        }
        // The answer stands after the process is gone, and proves nothing.
        lock(&harness.shared).alive = false;
        assert_eq!(
            harness.deliver(&line),
            Ok(DeliverOutcome::Rejected {
                reason: reason.clone(),
                message_specific: true,
            })
        );
        assert_eq!(harness.proven(&line.id), None);
        assert_eq!(prompts(&seen).len(), 1, "sent once");
    }

    #[test]
    fn a_missing_session_is_created_again_and_the_same_request_sent_once_more() {
        let (port, seen) = scripted(vec![
            refusal("session/not-found"),
            envelope(json!({"ok": true, "value": {"sessionId": "s"}})),
            accepted(),
        ]);
        let mut harness = harness_on(port);
        let line = message("msg_again");
        assert_eq!(settle(&mut harness, &line), Ok(DeliverOutcome::InFlight));
        let seen = lock(&seen).clone();
        let targets: Vec<&str> = seen.iter().map(|seen| seen.target.as_str()).collect();
        assert_eq!(
            targets,
            [
                "/api/session/prompt",
                "/api/session/create",
                "/api/session/prompt"
            ]
        );
        assert_eq!(seen[1].body["payload"]["args"]["request"]["sessionId"], "s");
        for prompt in [&seen[0], &seen[2]] {
            assert_eq!(
                prompt.body["payload"]["args"]["request"]["requestId"],
                "msg_again"
            );
        }
        assert_admitted(&harness, "msg_again");

        // A session that is still missing once created again is a fault, not
        // a loop.
        let (port, seen) = scripted(vec![
            refusal("session/not-found"),
            envelope(json!({"ok": true, "value": {"sessionId": "s"}})),
            refusal("session/not-found"),
        ]);
        let mut harness = harness_on(port);
        assert!(matches!(
            settle(&mut harness, &line),
            Err(HarnessError::Failed(_))
        ));
        assert_eq!(prompts(&seen).len(), 2);
    }

    #[test]
    fn a_refused_cookie_is_bought_again_and_the_same_request_sent_once_more() {
        let cookie = "HTTP/1.1 303 See Other\r\nset-cookie: dsh-auth-new=2; Path=/; HttpOnly\r\nlocation: ./\r\n\r\n".to_string();
        let (port, seen) = scripted(vec![
            http("401 Unauthorized", "unauthorized"),
            cookie.clone(),
            accepted(),
        ]);
        let mut harness = harness_on(port);
        let line = message("msg_cookie");
        assert_eq!(settle(&mut harness, &line), Ok(DeliverOutcome::InFlight));
        assert_admitted(&harness, "msg_cookie");
        let seen = lock(&seen).clone();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].cookie.as_deref(), Some("dsh-auth-test=1"));
        assert_eq!(seen[1].target, "/?token=t");
        assert_eq!(seen[2].target, "/api/session/prompt");
        assert_eq!(seen[2].cookie.as_deref(), Some("dsh-auth-new=2"));
        assert_eq!(
            lock(&harness.shared).cookie.as_deref(),
            Some("dsh-auth-new=2")
        );

        // A fresh cookie that is refused as well is a fault, not a loop.
        let (port, seen) = scripted(vec![
            http("401 Unauthorized", "unauthorized"),
            cookie,
            http("401 Unauthorized", "unauthorized"),
        ]);
        let mut harness = harness_on(port);
        assert!(matches!(
            settle(&mut harness, &line),
            Err(HarnessError::Failed(reason)) if reason.contains("401")
        ));
        assert_eq!(prompts(&seen).len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn the_process_is_dead_only_once_its_output_ended() {
        let (output, mut input) = std::os::unix::net::UnixStream::pair().unwrap();
        let shared = Arc::new(Mutex::new(State::new(None, "s".into())));
        let wake = Arc::new(Condvar::new());
        let reader = {
            let (shared, wake) = (Arc::clone(&shared), Arc::clone(&wake));
            thread::spawn(move || read_stdout(output, &shared, &wake))
        };
        writeln!(input, "dsh web: http://127.0.0.1:4242/?token=abc").unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while lock(&shared).port.is_none() {
            assert!(Instant::now() < deadline, "the address was never read");
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(lock(&shared).launch_token.as_deref(), Some("abc"));
        thread::sleep(Duration::from_millis(100));
        assert!(lock(&shared).alive, "dead before its output ended");
        drop(input);
        reader.join().unwrap();
        assert!(!lock(&shared).alive);
    }

    fn event(seq: i64, kind: &str, data: Value) -> Value {
        json!({"type": kind, "seq": seq, "time": 0, "data": data})
    }

    /// A message a mailbox line made.
    fn line(id: &str) -> Value {
        json!({"role": "user", "id": format!("m-{id}"), "content": [{"type": "text", "text": id}], "source": {"kind": "user", "rpcId": id}})
    }

    fn spliced(seq: i64, start: u64, removed: u64, inserted: Vec<Value>) -> Value {
        event(
            seq,
            "agent/inbox/spliced",
            json!({"target": "next-step", "start": start, "removedCount": removed, "inserted": inserted}),
        )
    }

    fn opening(cursor: i64, history: Vec<Value>, next_step: Vec<Queued>) -> Opening {
        Opening {
            socket: None,
            cursor,
            inbox: [Vec::new(), next_step],
            history,
            live: Vec::new(),
        }
    }

    fn turn_end(seq: i64, kind: &str) -> Value {
        event(
            seq,
            "turn/end",
            json!({"turn": 1, "reason": {"kind": kind}}),
        )
    }

    #[test]
    fn a_line_is_proved_by_its_user_message_and_dropped_only_once_no_turn_can_take_it() {
        let mut follow = Follow::default();
        let record = vec![
            event(0, "user/message", line("old")),
            event(1, "turn/start", json!({"turn": 1})),
        ];
        // An earlier process's record: its turn is not this process's.
        assert_eq!(
            follow.open(opening(1, record, Vec::new()), true),
            Ok((0, Vec::new()))
        );
        assert_eq!(follow.consumed.get("old"), Some(&true));
        assert!(!follow.busy());

        follow.apply(&spliced(2, 0, 0, vec![line("a")])).unwrap();
        assert!(follow.pending("a") && follow.busy());
        follow
            .apply(&event(3, "turn/start", json!({"turn": 1})))
            .unwrap();
        // Claimed: out of the inbox, its turn still to append it.
        follow.apply(&spliced(4, 0, 1, Vec::new())).unwrap();
        assert!(!follow.pending("a") && !follow.dropped("a"));
        follow.apply(&event(5, "user/message", line("a"))).unwrap();
        assert_eq!(follow.consumed.get("a"), Some(&false));
        // A message the adapter did not write proves nothing.
        let mut runtime = line("x");
        runtime["source"] = json!({"kind": "plugin", "plugin": "p"});
        follow.apply(&event(6, "user/message", runtime)).unwrap();
        assert!(!follow.consumed.contains_key("x"));

        follow.apply(&spliced(7, 0, 0, vec![line("b")])).unwrap();
        follow.apply(&spliced(8, 0, 1, Vec::new())).unwrap();
        assert!(!follow.dropped("b"), "its turn still runs");
        assert_eq!(
            follow.apply(&turn_end(9, "completed")),
            Ok((true, Vec::new()))
        );
        assert!(follow.dropped("b") && !follow.dropped("a"));
        // Never seen entering the inbox: on its way, not dropped.
        assert!(!follow.dropped("c"));

        // A gap, an unknown turn-end reason, or a splice past the inbox is
        // never guessed over.
        assert!(follow.apply(&event(11, "turn/start", json!({}))).is_err());
        assert!(follow.apply(&spliced(10, 1, 0, vec![line("d")])).is_err());
        assert!(follow.apply(&event(10, "turn/end", json!({}))).is_err());
    }

    #[test]
    fn a_halted_turn_names_stranded_inbox_items_and_settles_every_dropped_line() {
        let mut state = State::new(None, "s".into());
        state
            .follow
            .open(opening(0, Vec::new(), Vec::new()), true)
            .unwrap();
        state
            .follow
            .apply(&event(1, "turn/start", json!({"turn": 1})))
            .unwrap();
        let mut plugin = line("p");
        plugin["source"] = json!({"kind": "plugin", "plugin": "p"});
        state
            .follow
            .apply(&spliced(2, 0, 0, vec![line("a"), plugin, line("b")]))
            .unwrap();
        state.prompts.insert("a".into(), Prompt::Admitted);
        state.prompts.insert("b".into(), Prompt::Admitted);
        assert_eq!(
            state.follow.apply(&turn_end(3, "aborted")),
            Ok((true, vec!["m-a".to_string(), "m-b".to_string()]))
        );
        // Once removed from the inbox with no turn running, both admissions
        // settle at once so a new turn started by `a` cannot strand `b`.
        state.follow.apply(&spliced(4, 0, 3, Vec::new())).unwrap();
        state.settle_dropped();
        assert!(!state.prompts.contains_key("a") && !state.prompts.contains_key("b"));
    }

    #[test]
    fn a_replay_after_an_unknown_answer_keeps_an_inbox_entry_the_first_call_made() {
        let (port, _seen) = scripted(vec![accepted()]);
        let mut harness = harness_on(port);
        let msg = message("msg_replay");
        {
            let mut st = lock(&harness.shared);
            st.follow
                .open(opening(0, Vec::new(), Vec::new()), true)
                .unwrap();
            st.follow
                .apply(&spliced(1, 0, 0, vec![line(&msg.id)]))
                .unwrap();
            st.prompts.insert(msg.id.clone(), Prompt::Unknown);
        }
        assert_eq!(settle(&mut harness, &msg), Ok(DeliverOutcome::InFlight));
        assert!(lock(&harness.shared).follow.seen.contains(&msg.id));
        assert_admitted(&harness, &msg.id);
    }

    #[test]
    fn a_reopened_stream_takes_the_record_it_missed_as_this_process_s() {
        let mut follow = Follow::default();
        follow
            .open(opening(0, Vec::new(), Vec::new()), true)
            .unwrap();
        follow.apply(&spliced(1, 0, 0, vec![line("a")])).unwrap();
        follow.apply(&event(2, "turn/start", json!({}))).unwrap();
        // While the stream was down, the turn took `a` and ended.
        let missed = vec![
            spliced(3, 0, 1, Vec::new()),
            event(4, "user/message", line("a")),
            turn_end(5, "completed"),
        ];
        assert_eq!(
            follow.open(opening(5, missed, Vec::new()), false),
            Ok((1, Vec::new()))
        );
        assert_eq!(follow.consumed.get("a"), Some(&false));
        assert!(!follow.busy());
        assert_eq!(follow.seq, 5);
    }

    #[test]
    fn a_snapshot_names_its_inbox_at_its_own_cursor_or_none() {
        let snapshot = |as_of: i64| {
            json!({"type": "snapshot", "cursor": 4, "records": [], "hasMore": true,
                   "projections": {"asOfSeq": as_of, "values": {"inbox": {"next-turn": [], "next-step": [line("a")]}}}})
        };
        assert_eq!(
            snapshot_inbox(&snapshot(4)),
            Ok((
                4,
                [
                    Vec::new(),
                    vec![Queued {
                        item_id: "m-a".to_string(),
                        rpc_id: Some("a".to_string()),
                    }]
                ]
            ))
        );
        assert!(snapshot_inbox(&snapshot(3)).is_err());
        assert!(snapshot_inbox(&json!({"type": "snapshot", "cursor": 4, "projections": {"asOfSeq": 4, "values": {}}})).is_err());
        let mut missing_id = line("a");
        missing_id.as_object_mut().unwrap().remove("id");
        assert!(snapshot_inbox(&json!({"type": "snapshot", "cursor": 4, "records": [], "hasMore": true,
            "projections": {"asOfSeq": 4, "values": {"inbox": {"next-turn": [], "next-step": [missing_id]}}}})).is_err());
    }

    #[test]
    fn a_follow_message_is_read_whole_across_fragments_and_pings() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let peer = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap() > 2 {
                line.clear();
            }
            let text = br#"{"type":"item","streamId":"follow","value":{"type":"event","event":{"seq":1}}}"#;
            let (head, tail) = text.split_at(10);
            let mut out = b"HTTP/1.1 101 Switching Protocols\r\n\r\n".to_vec();
            out.extend([0x01, head.len() as u8]);
            out.extend_from_slice(head);
            out.extend([0x89, 2, b'h', b'i']);
            out.extend([0x80, 126]);
            out.extend((tail.len() as u16).to_be_bytes());
            out.extend_from_slice(tail);
            stream.write_all(&out).unwrap();
            // The pong comes back masked, carrying the ping's payload.
            let mut pong = [0u8; 8];
            reader.read_exact(&mut pong).unwrap();
            assert_eq!(pong[0], 0x8A);
            assert_eq!(pong[1], 0x82);
            let masked = [pong[6] ^ pong[2], pong[7] ^ pong[3]];
            assert_eq!(&masked, b"hi");
        });
        let mut socket = Socket::connect(port, "dsh-auth-test=1").unwrap();
        let text = socket.next().unwrap().unwrap();
        assert_eq!(follow_item(&text), Ok(Some(json!({"seq": 1}))));
        peer.join().unwrap();
        assert!(socket.next().is_err(), "the end of the stream ends it");
        assert!(follow_item(r#"{"type":"end","streamId":"follow"}"#).is_err());
    }
}

#[cfg(all(test, unix))]
mod conformance {
    use super::DshHarness;
    use anyagents_invariants::conformance::{Fixture, Subject, World, ids};

    /// `dsh --profile web --no-open --port 0`, as the server starts it.
    pub struct Dsh;

    impl Subject for Dsh {
        type Harness = DshHarness;
        const HARNESS: &'static str = "dsh";
        const PROTOCOL: &'static str = "dsh";

        fn launch(fixture: &Fixture, session: Option<&str>) -> DshHarness {
            DshHarness::start(fixture.program(), None, session, &[], fixture.dir())
                .expect("dsh starts")
        }

        fn session(harness: &DshHarness) -> Option<String> {
            harness.session_id()
        }
    }

    anyagents_invariants::conformance_suite!(Dsh);

    use std::time::{Duration, Instant};

    use anyagents_harness::{DeliverOutcome, Harness, OutboundMessage, Proof, TurnState};

    fn line(id: &str) -> OutboundMessage {
        OutboundMessage {
            id: id.into(),
            seq: 1,
            text: "hello".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Default::default(),
        }
    }

    /// Offer `message` until the adapter answers something other than
    /// `InFlight`.
    fn offer(harness: &mut DshHarness, message: &OutboundMessage) -> DeliverOutcome {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match harness.deliver(message).expect("delivers") {
                DeliverOutcome::InFlight | DeliverOutcome::Deferred => {
                    assert!(Instant::now() < deadline, "never settled");
                    std::thread::sleep(Duration::from_millis(5));
                }
                outcome => return outcome,
            }
        }
    }

    /// dsh runs once per start: nothing asks the program its version.
    #[test]
    fn a_start_runs_dsh_once_and_names_no_version() {
        let fixture = Fixture::new("dsh");
        let harness = Dsh::launch(&fixture, None);
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(fixture.launches(), 1);
        assert_eq!(harness.health().delivery.peer, None);
    }

    /// `stop` lets a prompt call that is out answer and reads the stream to
    /// its end, so a consumption the stream showed is proved after the
    /// process is gone.
    #[test]
    fn a_consumption_shown_just_before_stop_is_proved_after_it() {
        let fixture = Fixture::new("dsh");
        let mut harness = Dsh::launch(&fixture, None);
        fixture.set_slow(400);
        let line = line("msg_before_stop");
        assert_eq!(harness.deliver(&line), Ok(DeliverOutcome::InFlight));
        harness.stop();
        assert!(!harness.health().process_alive);
        let taken = Proof::Taken {
            turn_finished: false,
        };
        assert_eq!(harness.proven(&line.id), Some(taken));
        assert_eq!(
            harness.deliver(&line),
            Ok(DeliverOutcome::Accepted {
                turn_finished: false
            })
        );
        assert_eq!(fixture.wire(), std::slice::from_ref(&line.id));
    }

    /// Steers a turn never consumed (the turn was cancelled with them
    /// pending) are cleared from dsh's retained inbox and sent again in
    /// mailbox order: the first starts the next turn, and the next joins it.
    #[test]
    fn a_line_dropped_unconsumed_is_offered_again_as_input_of_its_own() {
        let fixture = Fixture::new("dsh");
        let mut harness = Dsh::launch(&fixture, None);
        let first = line("msg_first");
        let second = line("msg_second");
        let third = line("msg_third");
        assert!(matches!(
            offer(&mut harness, &first),
            DeliverOutcome::Accepted { .. }
        ));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let _ = harness.deliver(&second).expect("delivers second");
            let _ = harness.deliver(&third).expect("delivers third");
            let wire = fixture.wire();
            if wire.contains(&second.id) && wire.contains(&third.id) {
                break;
            }
            assert!(Instant::now() < deadline, "the steers never reached dsh");
            std::thread::sleep(Duration::from_millis(5));
        }
        // Admitted, pending: in flight, not proved.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(harness.deliver(&second), Ok(DeliverOutcome::InFlight));
        assert_eq!(harness.deliver(&third), Ok(DeliverOutcome::InFlight));
        assert_eq!(harness.proven(&second.id), None);
        assert_eq!(harness.proven(&third.id), None);
        harness.cancel().expect("cancel");
        assert!(matches!(
            offer(&mut harness, &second),
            DeliverOutcome::Accepted { .. }
        ));
        // `second` started a new turn, and `third` was settled at the same
        // halt boundary, so `third` is immediately re-offered as a steer.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let _ = harness.deliver(&third).expect("redelivers third");
            if fixture.wire().iter().filter(|id| **id == third.id).count() == 2 {
                break;
            }
            assert!(Instant::now() < deadline, "third was not sent again");
            std::thread::sleep(Duration::from_millis(5));
        }
        fixture.command("step");
        assert!(matches!(
            offer(&mut harness, &third),
            DeliverOutcome::Accepted { .. }
        ));
        let wire = fixture.wire();
        assert_eq!(wire.iter().filter(|id| **id == second.id).count(), 2);
        assert_eq!(wire.iter().filter(|id| **id == third.id).count(), 2);
        let journal = fixture.journal();
        let ids: Vec<_> = journal.iter().map(|seen| seen.id.as_str()).collect();
        assert_eq!(
            ids,
            [first.id.as_str(), second.id.as_str(), third.id.as_str()]
        );
        assert_eq!(journal[1].kind, "prompt", "input of its own");
        assert_eq!(journal[2].kind, "steer", "steered into the new turn");
        fixture.command("finish");
        let deadline = Instant::now() + Duration::from_secs(10);
        while harness.health().turn_state != TurnState::Idle {
            assert!(Instant::now() < deadline, "the turn never ended");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// When a turn halts with queued steers and a fresh message behind them,
    /// `deliver` defers while `clear_stranded` removes the halted inbox so
    /// the fresh message never overtakes the stranded steers.
    #[test]
    fn cancelling_a_turn_with_queued_and_fresh_mail_redelivers_in_fifo_order() {
        let mut world = World::<Dsh>::new();
        let first = world.prompt();
        let queued = world.receive(2);
        world.pump_until("both steers queued", |w| w.fixture.wire().len() >= 3);
        assert_eq!(world.cursor(), first.seq);
        let fresh = world.receive(1);
        world.delivery.harness_mut().cancel().unwrap();
        world.drain(fresh[0].seq);
        assert_eq!(
            ids(&world.fixture.journal()),
            vec![
                first.id.to_string(),
                queued[0].id.to_string(),
                queued[1].id.to_string(),
                fresh[0].id.to_string(),
            ]
        );
        assert_eq!(
            world.fixture.wire(),
            vec![
                first.id.to_string(),
                queued[0].id.to_string(),
                queued[1].id.to_string(),
                queued[0].id.to_string(),
                queued[1].id.to_string(),
                fresh[0].id.to_string(),
            ]
        );
    }
}
