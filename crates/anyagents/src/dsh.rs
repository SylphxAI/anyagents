//! DeepSeek Harness (`dsh`) web gateway adapter (`dsh --profile web --no-open --port 0`).
//!
//! `dsh` prints its gateway's address with a one-time launch token (`dsh web:
//! http://127.0.0.1:<port>/?token=<t>`). The token buys a `dsh-auth-*` cookie
//! (`GET /?token=`), and every call is one JSON envelope, `POST /api/<method>`
//! with `{type: "client-request", rpcId, method, payload: {args: {<parameter>:
//! …}}}`, answered `200` with `{type: "server-response", rpcId, result: {ok,
//! value | error: {code, message}}}`:
//!
//! - start waits for the address, takes the cookie, and creates or adopts the
//!   session (`session/create` with its id);
//! - a mailbox line is `session/prompt` with `mode: "steer"` and the mailbox id
//!   as `requestId`. dsh puts it in the session's durable `next-step` inbox,
//!   which the running turn takes whole at its next step, in order, or which
//!   starts a turn. A `requestId` the session already holds, pending or
//!   consumed, is answered `accepted` and admitted nothing, across restarts.
//!   The admission is the proof delivery rests on (`MidTurn`): the gateway
//!   reports consumption only on its event stream. Its answer does not say
//!   whether the request was new, so a line an earlier process applied is
//!   accepted again (and not applied twice), never reported as a duplicate;
//! - lines are written one at a time, in mailbox order, each on a thread of its
//!   own and with no timeout. A certain refusal (an error dsh raises before the
//!   inbox) leaves the line in the mailbox; an outcome that may have admitted
//!   it (the catch-all `session/agent-busy`, a lost connection) is settled by
//!   sending the same `requestId` again; a second such outcome is a fault;
//! - a turn ends when `session/list` reports the session not running after the
//!   newest admission. The gateway announces turn ends only on its event
//!   stream, so an open turn is polled; an idle harness asks nothing.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    TurnState, compose_message, marked_message, spawn_command,
};
use serde_json::{Value, json};

/// What this adapter guarantees for mail that reaches a busy agent
/// (`docs/delivery.md`).
pub const DELIVERY: Delivery = Delivery {
    level: DeliveryLevel::MidTurn,
    idempotent: true,
    proof: "durable inbox admission, by request id",
    peer: None,
};

/// How long a started dsh may take to print its address.
const START_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a call off the delivery path may take: the handshake, cancel,
/// resume and the turn poll. A prompt has no limit.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// How often an open turn's session state is asked for.
const TURN_POLL: Duration = Duration::from_millis(250);

pub struct DshHarness {
    environment: Vec<(String, String)>,
    work_dir: PathBuf,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    poller: Option<JoinHandle<()>>,
}

/// What became of one mailbox line this process wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Prompt {
    /// Its `session/prompt` is out.
    Sending,
    /// The session's inbox holds it, or held it already.
    Admitted,
    /// dsh refused it before its inbox.
    Refused,
    /// The call ended without saying whether dsh admitted it.
    Unknown,
    /// dsh failed in a way only a new process can clear.
    Fault(String),
}

struct State {
    child: Option<Child>,
    alive: bool,
    /// Moves on every change a host waits for: an answered prompt, a turn
    /// that ended, the process gone.
    epoch: u64,
    port: Option<u16>,
    launch_token: Option<String>,
    cookie: Option<String>,
    session: String,
    /// `session/create` answered for `session` in this process.
    session_ready: bool,
    /// `dsh <version>`, as the program reports it (`dsh -V`).
    peer: Option<String>,
    /// What became of each line this process wrote, by mailbox id.
    prompts: HashMap<String, Prompt>,
    /// The line whose `session/prompt` is out. One at a time: dsh admits them
    /// in the order the calls arrive.
    sending: Option<String>,
    /// Lines admitted so far. A turn ends only when dsh reports the session
    /// idle after the newest of them.
    admissions: u64,
    turn_open: bool,
    turns_completed: u32,
}

impl State {
    fn new(child: Option<Child>, session: String) -> Self {
        Self {
            child,
            alive: true,
            epoch: 0,
            port: None,
            launch_token: None,
            cookie: None,
            session,
            session_ready: false,
            peer: None,
            prompts: HashMap::new(),
            sending: None,
            admissions: 0,
            turn_open: false,
            turns_completed: 0,
        }
    }
}

impl DshHarness {
    /// Start dsh, wait for its gateway, and create or adopt the session.
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
            .spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if let Some((port, token)) = parse_dsh_web_url(&line) {
                        let mut st = lock(&reader_shared);
                        st.port = Some(port);
                        st.launch_token = Some(token);
                        st.epoch = st.epoch.wrapping_add(1);
                        drop(st);
                        reader_wake.notify_all();
                    }
                }
                let mut st = lock(&reader_shared);
                st.alive = false;
                if let Some(child) = st.child.as_mut() {
                    let _ = child.try_wait();
                }
                st.epoch = st.epoch.wrapping_add(1);
                drop(st);
                reader_wake.notify_all();
            })
            .map_err(|err| HarnessError::Failed(err.to_string()))?;

        let poll_shared = Arc::clone(&shared);
        let poll_wake = Arc::clone(&wake);
        let poller = thread::Builder::new()
            .name("anyagents-dsh-poll".into())
            .spawn(move || poll_dsh_turns(poll_shared, poll_wake))
            .map_err(|err| HarnessError::Failed(err.to_string()))?;

        let harness = Self {
            environment: env_vec,
            work_dir: work_dir.to_path_buf(),
            shared,
            wake,
            reader: Some(reader),
            poller: Some(poller),
        };
        harness.connect()?;
        // The gateway names no version; the program does.
        let peer_shared = Arc::clone(&harness.shared);
        let peer_env = harness.environment.clone();
        let _ = thread::Builder::new()
            .name("anyagents-dsh-version".into())
            .spawn(move || {
                if let Some(version) = program_version(&program, &peer_env) {
                    lock(&peer_shared).peer = Some(format!("dsh {version}"));
                }
            });
        Ok(harness)
    }

    pub fn session_id(&self) -> Option<String> {
        let st = lock(&self.shared);
        (!st.session.is_empty()).then(|| st.session.clone())
    }

    /// Wait for the gateway's address, take the cookie, and create or adopt
    /// the session.
    fn connect(&self) -> Result<(), HarnessError> {
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
        let mut st = lock(&self.shared);
        st.cookie = Some(cookie);
        st.session_ready = true;
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
}

impl Harness for DshHarness {
    /// Write one mailbox line into the session's `next-step` inbox, carrying
    /// its own id as `requestId`, and accept it once dsh admitted it.
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let mut st = lock(&self.shared);
        // What dsh said stands even after the process went.
        if st.prompts.get(&message.id) == Some(&Prompt::Admitted) {
            return Ok(DeliverOutcome::Accepted {
                turn_finished: false,
            });
        }
        if !st.alive {
            return Err(HarnessError::Dead);
        }
        let replay = match st.prompts.get(&message.id) {
            Some(Prompt::Sending) => return Ok(DeliverOutcome::InFlight),
            Some(Prompt::Refused) => {
                // Nothing was admitted. The line waits in the mailbox and is
                // sent again when it is offered again.
                st.prompts.remove(&message.id);
                return Ok(DeliverOutcome::Deferred);
            }
            Some(Prompt::Fault(reason)) => {
                let reason = reason.clone();
                st.prompts.remove(&message.id);
                return Err(HarnessError::Failed(reason));
            }
            // dsh may or may not hold it: the same `requestId` again settles
            // that, since dsh admits a request once.
            Some(Prompt::Unknown) => true,
            _ => false,
        };
        // One call at a time keeps the inbox in mailbox order.
        if st.sending.is_some() {
            return Ok(DeliverOutcome::Deferred);
        }
        st.sending = Some(message.id.clone());
        st.prompts.insert(message.id.clone(), Prompt::Sending);
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
        let mut st = lock(&self.shared);
        st.session = session.to_string();
        st.session_ready = true;
        st.turn_open = false;
        Ok(())
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
        let mut st = lock(&self.shared);
        st.turn_open = false;
        st.epoch = st.epoch.wrapping_add(1);
        drop(st);
        self.wake.notify_all();
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let st = lock(&self.shared);
        HarnessHealth {
            process_alive: st.alive,
            // A prompt still being written is work under way, not an idle agent.
            turn_state: if st.turn_open || st.sending.is_some() {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: DELIVERY.with_peer(st.peer.clone()),
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
    fn drop(&mut self) {
        let child = {
            let mut st = lock(&self.shared);
            st.alive = false;
            st.child.take()
        };
        self.wake.notify_all();
        // A prompt call still out ends with the process: its connection closes.
        if let Some(mut child) = child {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.poller.take() {
            let _ = handle.join();
        }
    }
}

/// Write one line on this thread and record what became of it.
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
    if outcome == Prompt::Admitted {
        st.admissions += 1;
        st.turn_open = true;
    }
    // A refusal wakes nobody: the line is offered again on the host's next
    // pass, and a refusal that holds is not retried in a loop.
    let woken = outcome != Prompt::Refused;
    st.prompts.insert(message.id.clone(), outcome);
    if woken {
        st.epoch = st.epoch.wrapping_add(1);
        drop(st);
        wake.notify_all();
    }
}

/// `session/prompt` for one line, and what dsh said about it.
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
    let unsettled = |reason: String| {
        if replay {
            Prompt::Fault(reason)
        } else {
            note(format!("{reason}; the same request is sent again"));
            Prompt::Unknown
        }
    };
    let (port, token, cookie, session, ready) = {
        let st = lock(shared);
        (
            st.port,
            st.launch_token.clone(),
            st.cookie.clone(),
            st.session.clone(),
            st.session_ready,
        )
    };
    let (Some(port), Some(token)) = (port, token) else {
        return Prompt::Fault("dsh gateway is not connected".into());
    };
    // A cookie the gateway stopped taking is bought again with the launch token.
    let cookie = match cookie {
        Some(cookie) => cookie,
        None => match dsh_exchange_token(port, &token, None) {
            Ok(cookie) => {
                lock(shared).cookie = Some(cookie.clone());
                cookie
            }
            Err(err) => return unsettled(err),
        },
    };
    if !ready {
        match create_session(port, &cookie, &session, work_dir, None) {
            Ok(()) => lock(shared).session_ready = true,
            Err(err) => return Prompt::Fault(err.to_string()),
        }
    }
    let args = json!({
        "request": {
            "sessionId": session,
            "requestId": message.id,
            "content": [{"type": "text", "text": compose_text(message)}],
            "mode": "steer",
        }
    });
    let answer = match dsh_rpc(port, &cookie, "session/prompt", &message.id, &args, None) {
        Ok(answer) => answer,
        Err(err) => return unsettled(err),
    };
    match answer {
        Answer::Value(value) if value.get("accepted") == Some(&Value::Bool(true)) => {
            Prompt::Admitted
        }
        Answer::Value(value) => unsettled(format!("dsh session/prompt answered {value}")),
        // The catch-all is raised after the inbox as well as before it.
        Answer::Error {
            code,
            message: reason,
        } if matches!(code.as_str(), "session/agent-busy" | "gateway/internal") => {
            unsettled(format!("dsh session/prompt: {code}: {reason}"))
        }
        Answer::Error {
            code,
            message: reason,
        } => {
            if code == "session/not-found" {
                lock(shared).session_ready = false;
            }
            note(format!(
                "session/prompt refused: {code}: {reason}; the line stays in the mailbox"
            ));
            Prompt::Refused
        }
        Answer::Status { status, body } if status >= 500 => {
            unsettled(format!("dsh session/prompt HTTP {status}: {body}"))
        }
        Answer::Status { status, body } => {
            if status == 401 {
                lock(shared).cookie = None;
            }
            note(format!(
                "session/prompt refused: HTTP {status}: {body}; the line stays in the mailbox"
            ));
            Prompt::Refused
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

/// Whether `session/list`'s value says `session` is running.
fn session_running(list: &Value, session: &str) -> Option<bool> {
    list.get("items")?
        .as_array()?
        .iter()
        .find(|item| item.get("sessionId").and_then(Value::as_str) == Some(session))?
        .get("running")?
        .as_bool()
}

/// End the open turn once dsh reports the session not running after the
/// newest admission. Asleep while no turn is open.
fn poll_dsh_turns(shared: Arc<Mutex<State>>, wake: Arc<Condvar>) {
    loop {
        let (port, cookie, session, admitted) = {
            let st = lock(&shared);
            // `carry` notifies when it opens a turn, and the reader and `Drop`
            // when the process goes.
            let st = wake
                .wait_while(st, |s| {
                    s.alive && (!s.turn_open || s.port.is_none() || s.cookie.is_none())
                })
                .unwrap_or_else(|e| e.into_inner());
            if !st.alive {
                return;
            }
            // Wait one poll interval, or less when the turn is cancelled or
            // the process goes.
            let st = wake
                .wait_timeout_while(st, TURN_POLL, |s| s.alive && s.turn_open)
                .unwrap_or_else(|e| e.into_inner())
                .0;
            if !st.alive {
                return;
            }
            if !st.turn_open {
                continue;
            }
            let (Some(port), Some(cookie)) = (st.port, st.cookie.clone()) else {
                continue;
            };
            (port, cookie, st.session.clone(), st.admissions)
        };
        let args = json!({ "_request": {} });
        let Ok(list) = dsh_call(
            port,
            &cookie,
            "session/list",
            "poll",
            &args,
            Some(CALL_TIMEOUT),
        ) else {
            continue;
        };
        if session_running(&list, &session) != Some(false) {
            continue;
        }
        let mut st = lock(&shared);
        // A line admitted after the poll was asked may have started the
        // session again.
        if st.turn_open && st.admissions == admitted {
            st.turn_open = false;
            st.turns_completed = st.turns_completed.saturating_add(1);
            st.epoch = st.epoch.wrapping_add(1);
            drop(st);
            wake.notify_all();
        }
    }
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

/// `dsh -V`: the version the program reports, read within the start timeout.
fn program_version(program: &Path, environment: &[(String, String)]) -> Option<String> {
    let mut command = spawn_command(program, environment).ok()?;
    command
        .arg("-V")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = command.spawn().ok()?;
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
            _ => {
                anyagents_harness::kill_child(&mut child, environment);
                let _ = child.wait();
                return None;
            }
        }
    }
    let mut out = String::new();
    child.stdout.take()?.read_to_string(&mut out).ok()?;
    out.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
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

    /// A `session/list` value naming `session`'s `running` state.
    fn listing(session: &str, running: Value) -> Value {
        json!({"items": [{"sessionId": session, "updatedAt": 0, "running": running, "blank": false}], "hasMore": false})
    }

    #[test]
    fn session_poll_requires_explicit_terminal_state_for_exact_session() {
        assert_eq!(
            session_running(&listing("selected", json!(false)), "selected"),
            Some(false)
        );
        assert_eq!(
            session_running(&listing("selected", json!(true)), "selected"),
            Some(true)
        );
        for malformed in [
            json!({}),
            json!({"items":[]}),
            listing("other", json!(false)),
            json!({"items":[{"sessionId":"selected"}]}),
            listing("selected", json!("false")),
        ] {
            assert_eq!(session_running(&malformed, "selected"), None);
        }
    }

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

    /// A dsh gateway that answers every call with the `session/list` envelope
    /// for session `s`, `running` as given, records each request body, and
    /// counts the calls.
    fn gateway(
        running: Arc<std::sync::atomic::AtomicBool>,
    ) -> (
        u16,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<Mutex<Vec<Value>>>,
    ) {
        use std::sync::atomic::Ordering;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let counted = Arc::clone(&calls);
        let recorded = Arc::clone(&requests);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; length];
                let _ = reader.read_exact(&mut body);
                if let Ok(request) = serde_json::from_slice(&body) {
                    lock(&recorded).push(request);
                }
                counted.fetch_add(1, Ordering::SeqCst);
                let answer = json!({
                    "type": "server-response",
                    "rpcId": "poll",
                    "result": {"ok": true, "value": listing("s", json!(running.load(Ordering::SeqCst)))},
                })
                .to_string();
                let _ = write!(
                    stream,
                    "HTTP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n{answer}",
                    answer.len()
                );
            }
        });
        (port, calls, requests)
    }

    fn connected_state(port: u16) -> Arc<Mutex<State>> {
        let mut state = State::new(None, "s".into());
        state.port = Some(port);
        state.cookie = Some("dsh-auth-test=1".into());
        state.session_ready = true;
        Arc::new(Mutex::new(state))
    }

    fn open_turn(shared: &Mutex<State>, wake: &Condvar) {
        let mut st = lock(shared);
        st.admissions += 1;
        st.turn_open = true;
        st.epoch = st.epoch.wrapping_add(1);
        drop(st);
        wake.notify_all();
    }

    fn stop(shared: &Mutex<State>, wake: &Condvar) {
        lock(shared).alive = false;
        wake.notify_all();
    }

    #[test]
    fn an_idle_harness_asks_dsh_nothing_and_an_open_turn_is_polled_to_its_end() {
        use std::sync::atomic::Ordering;
        let running = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, calls, requests) = gateway(Arc::clone(&running));
        let shared = connected_state(port);
        let wake = Arc::new(Condvar::new());
        let poller = {
            let (shared, wake) = (Arc::clone(&shared), Arc::clone(&wake));
            thread::spawn(move || poll_dsh_turns(shared, wake))
        };
        thread::sleep(Duration::from_millis(400));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        open_turn(&shared, &wake);
        let deadline = Instant::now() + Duration::from_secs(5);
        while lock(&shared).turns_completed == 0 {
            assert!(
                Instant::now() < deadline,
                "the finished turn was never seen"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!lock(&shared).turn_open);
        let after_turn = calls.load(Ordering::SeqCst);
        thread::sleep(Duration::from_millis(600));
        assert_eq!(calls.load(Ordering::SeqCst), after_turn);
        // The poll names its one parameter, as the gateway's descriptor does.
        let request = lock(&requests)[0].clone();
        assert_eq!(request["method"], "session/list");
        assert_eq!(request["payload"], json!({"args": {"_request": {}}}));

        let started = Instant::now();
        stop(&shared, &wake);
        poller.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn a_harness_stopped_mid_turn_stops_polling_at_once() {
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (port, _calls, _requests) = gateway(running);
        let shared = connected_state(port);
        let wake = Arc::new(Condvar::new());
        let poller = {
            let (shared, wake) = (Arc::clone(&shared), Arc::clone(&wake));
            thread::spawn(move || poll_dsh_turns(shared, wake))
        };
        open_turn(&shared, &wake);
        thread::sleep(Duration::from_millis(100));
        let started = Instant::now();
        stop(&shared, &wake);
        poller.join().unwrap();
        assert!(started.elapsed() < TURN_POLL + Duration::from_millis(200));
        assert_eq!(lock(&shared).turns_completed, 0);
    }

    #[test]
    fn a_prompt_names_its_request_and_carries_the_mailbox_id() {
        let running = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, _calls, requests) = gateway(running);
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
        let request = lock(&requests)[0].clone();
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
}

#[cfg(all(test, unix))]
mod conformance {
    use super::DshHarness;
    use anyagents_invariants::conformance::{Fixture, Subject};

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
}
