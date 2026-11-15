//! The cloud harness over `acp/v1`: a peer that runs the agent's session
//! and proves, item by item, when the session took each mailbox line.
//!
//! `cloud.json` names the peer under `acp`: `{"command": [argv...]}` starts
//! it and speaks JSON lines on its stdin and stdout, `{"socket": path}`
//! connects to a Unix socket that speaks the same. `harness` names the
//! peer's harness (the cloud harness's own name when absent).
//!
//! Each start attaches with a `lease_epoch` one above the last one this
//! agent used, and with the last `seq` it read as `after_seq`: frames the
//! peer replays are read for their `seq` and not forwarded, since the
//! process that read them before is gone and the next one re-offers every
//! line it never saw proved. After `ready`, every frame must carry this
//! lease and the next `seq`: a frame of another lease is ignored, a gap or an
//! unreadable frame ends the process.
//!
//! Towards `ClaudeHarness` it speaks Claude Code's stream-json. Its `system`
//! `init` declares the delivery the peer offers: `next-step` when `ready`
//! names `next_step_steer`, else `turn-end`. Every line goes out as its own
//! `deliver` (one item, its mailbox id as the item id); its proof, the
//! line's replay echo, is written on `accepted` when the line opened a turn
//! or was already consumed, and on `steer_consumed` when the running turn
//! took it at a step. `steer_ack` proves nothing. Ids a `result` hands back
//! in `undelivered_ids` are delivered again at once, and open the next turn.
//! At `turn-end` a line waits here while a turn runs, and opens the next.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyagents_harness::{Delivery, DeliveryLevel};
use serde_json::{Value, json};

pub const PROTOCOL: &str = "acp/v1";
/// What `ready.capabilities` names when the peer proves steers per item.
pub const NEXT_STEP_STEER: &str = "next_step_steer";
/// How long the peer may take to answer `attach` with `ready`.
const READY_WAIT: Duration = Duration::from_secs(30);
/// How long a peer may take to say `stopped` once input ends.
const STOP_WAIT: Duration = Duration::from_secs(5);
/// Backoff before returning transient turn-state refusals to the host.
const REFUSAL_BACKOFF: Duration = Duration::from_millis(250);

pub const NEXT_STEP: Delivery = Delivery {
    level: DeliveryLevel::NextStep,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed("acp/v1 accepted or steer_consumed, by item id"),
    peer: None,
};

pub const TURN_END: Delivery = Delivery {
    level: DeliveryLevel::TurnEnd,
    idempotent: true,
    proof: std::borrow::Cow::Borrowed("acp/v1 accepted when the line's turn starts"),
    peer: None,
};

/// Where the peer is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    Command(Vec<String>),
    Socket(PathBuf),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub endpoint: Endpoint,
    pub harness: String,
}

/// The `acp` entry of `cloud.json`: `None` without one, an error for one
/// that names no endpoint, or both.
pub fn config(cloud_json: Option<&Value>, cloud_harness: &str) -> Result<Option<Config>, String> {
    let Some(acp) = cloud_json.and_then(|value| value.get("acp")) else {
        return Ok(None);
    };
    let command = match acp.get("command") {
        None => None,
        Some(Value::Array(items)) if !items.is_empty() => Some(
            items
                .iter()
                .map(|item| item.as_str().map(str::to_string))
                .collect::<Option<Vec<_>>>()
                .ok_or("cloud.json `acp.command` must be an array of strings")?,
        ),
        Some(_) => return Err("cloud.json `acp.command` must be a non-empty array".into()),
    };
    let socket = match acp.get("socket") {
        None => None,
        Some(Value::String(path)) if !path.trim().is_empty() => Some(PathBuf::from(path.trim())),
        Some(_) => return Err("cloud.json `acp.socket` must be a path".into()),
    };
    let endpoint = match (command, socket) {
        (Some(argv), None) => Endpoint::Command(argv),
        (None, Some(path)) => Endpoint::Socket(path),
        _ => {
            return Err("cloud.json `acp` names exactly one of `command` and `socket`".into());
        }
    };
    let harness = match acp.get("harness") {
        None => cloud_harness.to_string(),
        Some(Value::String(name)) if !name.trim().is_empty() => name.trim().to_string(),
        Some(_) => return Err("cloud.json `acp.harness` must be a name".into()),
    };
    Ok(Some(Config { endpoint, harness }))
}

/// What this agent last used on its `acp/v1` session, kept beside its
/// session id so a new process attaches above it and replays after it.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Lease {
    pub session_id: String,
    pub lease_epoch: u64,
    pub last_seq: Option<u64>,
}

fn lease_path(config: &Path, harness: &str) -> PathBuf {
    config.join(format!("{harness}-acp.json"))
}

fn read_lease(config: &Path, harness: &str) -> Result<Option<Lease>, String> {
    let path = lease_path(config, harness);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("cannot read {}: {err}", path.display())),
    };
    serde_json::from_str(&raw)
        .map(Some)
        .map_err(|err| format!("corrupt lease {}: {err}", path.display()))
}

fn write_lease(config: &Path, harness: &str, lease: &Lease) -> Result<(), String> {
    let path = lease_path(config, harness);
    let bytes = serde_json::to_vec(lease).map_err(|err| err.to_string())?;
    anyagents_harness::atomic_write_nonempty_file(&path, &bytes, 0o664)
        .map_err(|err| format!("cannot write {}: {err}", path.display()))
}

/// Everything the session asks the peer for at `attach`.
#[derive(Clone, Debug)]
pub struct Attach {
    pub session_id: String,
    pub lease_epoch: u64,
    pub after_seq: Option<u64>,
    pub harness: String,
    pub model: Option<String>,
    pub instructions: Option<String>,
}

/// One event the client reacts to, in arrival order.
#[derive(Debug)]
pub enum Event {
    Host(String),
    HostClosed,
    Peer(String),
    PeerClosed,
}

/// What the client does after an event.
#[derive(Debug, PartialEq, Eq)]
pub enum Flow {
    Continue,
    /// The peer ended the session (`stopped`).
    Done,
}

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    /// `attach` is out and awaiting `ready`.
    Attaching,
    Live,
}

/// The `acp/v1` session and the stream-json it owes the host.
pub struct Client<H: Write, P: Write> {
    host: H,
    peer: P,
    attach: Attach,
    phase: Phase,
    last_seq: Option<u64>,
    next_step: bool,
    next_request: u64,
    /// The item each `deliver` carried, by request id.
    requests: HashMap<String, String>,
    /// The host's line for each item id, echoed as its proof.
    lines: HashMap<String, Value>,
    proven: HashSet<String>,
    /// The peer's running turn.
    turn: Option<String>,
    /// True while `turn` is a synthetic `duplicate` turn awaiting its `result`.
    duplicate_turn: bool,
    /// A turn-opening `deliver` (`mode: "turn"`) is out and unanswered.
    opening: bool,
    /// The in-flight `deliver` (`(request_id, item_id)`), one at a time.
    delivering: Option<(String, String)>,
    /// Items waiting for the opening `accepted`, prior `steer_ack`, or turn end.
    waiting: VecDeque<String>,
    /// Items cancelled by a transient refusal, held briefly in `drive` to avoid spin.
    cancelled_batch: Vec<String>,
    defer_cancel: bool,
    stopping: bool,
    cost_micros_total: Option<u64>,
    reply: String,
    /// Called with every `seq` read, to keep it for the next attach.
    on_seq: Box<dyn FnMut(u64)>,
}

impl<H: Write, P: Write> Client<H, P> {
    pub fn new(host: H, peer: P, attach: Attach, on_seq: Box<dyn FnMut(u64)>) -> Self {
        let last_seq = attach.after_seq;
        Self {
            host,
            peer,
            attach,
            phase: Phase::Attaching,
            last_seq,
            next_step: false,
            next_request: 0,
            requests: HashMap::new(),
            lines: HashMap::new(),
            proven: HashSet::new(),
            turn: None,
            duplicate_turn: false,
            opening: false,
            delivering: None,
            waiting: VecDeque::new(),
            cancelled_batch: Vec::new(),
            defer_cancel: false,
            stopping: false,
            cost_micros_total: None,
            reply: String::new(),
            on_seq,
        }
    }

    pub fn is_live(&self) -> bool {
        self.phase == Phase::Live
    }

    pub fn has_work(&self) -> bool {
        self.turn.is_some()
            || self.opening
            || self.delivering.is_some()
            || !self.requests.is_empty()
            || !self.waiting.is_empty()
            || !self.cancelled_batch.is_empty()
    }

    fn request_id(&mut self) -> String {
        self.next_request += 1;
        format!("{}.{}", self.attach.lease_epoch, self.next_request)
    }

    fn control(&mut self, kind: &str, mut frame: Value) -> Result<String, String> {
        let request_id = self.request_id();
        frame["type"] = json!(kind);
        frame["session_id"] = json!(self.attach.session_id);
        frame["lease_epoch"] = json!(self.attach.lease_epoch);
        frame["request_id"] = json!(request_id);
        writeln!(self.peer, "{frame}")
            .and_then(|()| self.peer.flush())
            .map_err(|err| format!("acp/v1 peer input closed: {err}"))?;
        Ok(request_id)
    }

    fn write_host(&mut self, line: &Value) -> Result<(), String> {
        writeln!(self.host, "{line}")
            .and_then(|()| self.host.flush())
            .map_err(|err| format!("host output closed: {err}"))
    }

    /// Send `attach`, replaying after the last `seq` read unless the peer
    /// no longer has it.
    pub fn send_attach(&mut self) -> Result<(), String> {
        let mut frame = json!({
            "protocol": PROTOCOL,
            "after_seq": self.last_seq,
            "harness": self.attach.harness,
        });
        if let Some(model) = &self.attach.model {
            frame["model"] = json!(model);
        }
        if let Some(instructions) = &self.attach.instructions {
            frame["instructions"] = json!(instructions);
        }
        self.control("attach", frame).map(|_| ())
    }

    /// One line from the host (`ClaudeHarness`).
    pub fn on_host(&mut self, line: &str) -> Result<(), String> {
        let Ok(event) = serde_json::from_str::<Value>(line.trim()) else {
            return Ok(());
        };
        match event.get("type").and_then(Value::as_str) {
            Some("user") => self.on_user(event),
            Some("cancel") => self.interrupt(),
            Some("stop") => self.send_stop(),
            Some("control_request") => {
                match event.pointer("/request/subtype").and_then(Value::as_str) {
                    Some("interrupt" | "cancel") => self.interrupt(),
                    Some("stop") => self.send_stop(),
                    _ => Ok(()),
                }
            }
            _ => Ok(()),
        }
    }

    pub fn send_stop(&mut self) -> Result<(), String> {
        if self.stopping {
            return Ok(());
        }
        self.stopping = true;
        self.control("stop", json!({})).map(|_| ())
    }

    fn interrupt(&mut self) -> Result<(), String> {
        if self.turn.is_none() && !self.opening {
            return Ok(());
        }
        self.control("interrupt", json!({})).map(|_| ())
    }

    fn on_user(&mut self, event: Value) -> Result<(), String> {
        let text = super::extract_user_prompt(&event);
        let id = anyagents_harness::message_id_in_text(&text)
            .map(str::to_string)
            .or_else(|| {
                event
                    .get("uuid")
                    .and_then(Value::as_str)
                    .filter(|uuid| !uuid.is_empty())
                    .map(str::to_string)
            });
        let Some(id) = id else {
            // Nothing to key a proof on: the host never wrote such a line.
            return Ok(());
        };
        if text.is_empty() {
            // `acp/v1` takes no empty item; the host is told it never will.
            return self.lifecycle(&event, "refused");
        }
        self.lines.insert(id.clone(), event);
        if !self.cancelled_batch.is_empty() {
            // A refused head item has not been returned to the host yet;
            // pipelined arrivals behind it return in the same FIFO batch.
            self.cancelled_batch.push(id);
            return Ok(());
        }
        self.waiting.push_back(id);
        self.advance()
    }

    fn send_deliver(&mut self, id: &str, mode: &str, opening: bool) -> Result<(), String> {
        let Some(line) = self.lines.get(id) else {
            return Ok(());
        };
        let text = super::extract_user_prompt(line);
        let request_id = self.control(
            "deliver",
            json!({"mode": mode, "items": [{"id": id, "text": text}]}),
        )?;
        self.opening = opening;
        self.delivering = Some((request_id.clone(), id.to_string()));
        self.requests.insert(request_id, id.to_string());
        Ok(())
    }

    fn advance(&mut self) -> Result<(), String> {
        if !self.cancelled_batch.is_empty() || self.delivering.is_some() || self.opening {
            return Ok(());
        }
        if self.turn.is_none() {
            if let Some(id) = self.waiting.pop_front() {
                return self.send_deliver(&id, "turn", true);
            }
            return Ok(());
        }
        if self.next_step
            && !self.duplicate_turn
            && let Some(id) = self.waiting.pop_front()
        {
            return self.send_deliver(&id, "steer", false);
        }
        Ok(())
    }

    /// The proof of `id`: the host's own line, echoed as Claude Code's
    /// `--replay-user-messages` does when the line enters the conversation.
    fn echo(&mut self, id: &str, duplicate: bool) -> Result<(), String> {
        // Taken once: the host never writes a proved line again.
        let Some(mut echo) = self.lines.remove(id) else {
            return Ok(());
        };
        echo["isReplay"] = Value::Bool(true);
        if duplicate {
            echo["duplicate"] = Value::Bool(true);
        }
        echo["session_id"] = json!(self.attach.session_id);
        self.proven.insert(id.to_string());
        self.write_host(&echo)
    }

    /// Hand a line back (`cancelled`) or refuse it for good (`refused`).
    fn lifecycle(&mut self, line: &Value, state: &str) -> Result<(), String> {
        let Some(uuid) = line.get("uuid").and_then(Value::as_str) else {
            return Ok(());
        };
        let frame = json!({
            "type": "command_lifecycle", "command_uuid": uuid, "uuid": uuid,
            "state": state, "session_id": self.attach.session_id,
        });
        self.write_host(&frame)
    }

    /// One frame from the peer.
    pub fn on_peer(&mut self, raw: &str) -> Result<Flow, String> {
        let frame: Value = serde_json::from_str(raw.trim())
            .map_err(|err| format!("acp/v1 peer wrote an unreadable frame: {err}"))?;
        let kind = frame
            .get("type")
            .and_then(Value::as_str)
            .ok_or("acp/v1 peer wrote a frame without a type")?
            .to_string();
        if kind == "hello" {
            if frame.get("protocol").and_then(Value::as_str) != Some(PROTOCOL) {
                return Err(format!(
                    "acp/v1 peer sent hello for another protocol: {frame}"
                ));
            }
            let ready = json!({
                "type": "tunnel_ready",
                "protocol": PROTOCOL,
                "connection_id": self.attach.session_id,
            });
            writeln!(self.peer, "{ready}")
                .and_then(|()| self.peer.flush())
                .map_err(|err| format!("acp/v1 peer input closed: {err}"))?;
            return Ok(Flow::Continue);
        }
        if kind == "tunnel_ready" {
            if frame.get("protocol").and_then(Value::as_str) != Some(PROTOCOL) {
                return Err(format!(
                    "acp/v1 peer sent tunnel_ready for another protocol: {frame}"
                ));
            }
            return Ok(Flow::Continue);
        }
        if kind == "protocol_error" {
            return match frame.get("session_id") {
                None | Some(Value::Null) => {
                    Err(format!("acp/v1 peer reported a protocol error: {frame}"))
                }
                Some(Value::String(sid)) if sid == &self.attach.session_id => {
                    Err(format!("acp/v1 peer reported a protocol error: {frame}"))
                }
                Some(Value::String(sid)) if !sid.is_empty() => Ok(Flow::Continue),
                Some(_) => Err(format!(
                    "acp/v1 peer wrote a malformed protocol_error: {frame}"
                )),
            };
        }
        let session_id = non_empty_str(&frame, "session_id")?;
        if session_id != self.attach.session_id {
            // Another session's frame on a shared stream.
            return Ok(Flow::Continue);
        }
        let lease = frame
            .get("lease_epoch")
            .and_then(Value::as_u64)
            .filter(|lease| *lease >= 1)
            .ok_or_else(|| format!("acp/v1 peer wrote a frame without a lease_epoch: {frame}"))?;
        let _ = nullable_id(&frame, "request_id")?;
        if kind == "rejected" {
            let live = self.phase == Phase::Live;
            match frame.get("seq") {
                None | Some(Value::Null) => {
                    if lease != self.attach.lease_epoch {
                        return Ok(Flow::Continue);
                    }
                    return self.on_rejected(&frame).map(|()| Flow::Continue);
                }
                Some(Value::Number(num)) => {
                    let seq = num
                        .as_u64()
                        .filter(|seq| *seq >= 1)
                        .ok_or_else(|| format!("acp/v1 rejected has invalid seq: {frame}"))?;
                    if (live && lease != self.attach.lease_epoch) || lease > self.attach.lease_epoch
                    {
                        return Ok(Flow::Continue);
                    }
                    if !self.advance_seq(seq, true)? {
                        return Ok(Flow::Continue);
                    }
                    if !live {
                        return Ok(Flow::Continue);
                    }
                    return self.on_rejected(&frame).map(|()| Flow::Continue);
                }
                Some(_) => return Err(format!("acp/v1 rejected has non-integer seq: {frame}")),
            }
        }
        let seq = frame
            .get("seq")
            .and_then(Value::as_u64)
            .filter(|seq| *seq >= 1)
            .ok_or_else(|| format!("acp/v1 peer wrote a frame without a seq: {frame}"))?;
        let live = self.phase == Phase::Live || kind == "ready";
        if (live && lease != self.attach.lease_epoch) || lease > self.attach.lease_epoch {
            // A frame of another lease never touches this one.
            return Ok(Flow::Continue);
        }
        if !self.advance_seq(seq, kind != "text_delta")? {
            return Ok(Flow::Continue);
        }
        if !live {
            // Replay: a frame an earlier process read, or never could.
            return Ok(Flow::Continue);
        }
        self.on_frame(&kind, &frame)
    }

    fn advance_seq(&mut self, seq: u64, persist: bool) -> Result<bool, String> {
        match self.last_seq {
            Some(last) if seq <= last => return Ok(false),
            Some(last) if seq != last + 1 => {
                return Err(format!(
                    "acp/v1 peer skipped from seq {last} to {seq}; the next attach replays them"
                ));
            }
            _ => {}
        }
        self.last_seq = Some(seq);
        if persist {
            (self.on_seq)(seq);
        }
        Ok(true)
    }

    fn on_frame(&mut self, kind: &str, frame: &Value) -> Result<Flow, String> {
        let request = nullable_id(frame, "request_id")?.map(str::to_string);
        match kind {
            "ready" => {
                if self.phase != Phase::Live {
                    self.on_ready(frame)?;
                }
            }
            "accepted" => {
                let turn_id = non_empty_str(frame, "turn_id")?.to_string();
                let mode = non_empty_str(frame, "mode")?;
                let duplicate = match mode {
                    "turn" => false,
                    "duplicate" => true,
                    _ => {
                        return Err(format!(
                            "acp/v1 accepted has unknown mode `{mode}`: {frame}"
                        ));
                    }
                };
                let item = request.and_then(|request| self.requests.remove(&request));
                self.turn = Some(turn_id);
                self.duplicate_turn = duplicate;
                self.opening = false;
                self.delivering = None;
                if let Some(item) = item {
                    self.echo(&item, duplicate)?;
                }
                self.advance()?;
            }
            "steer_ack" => {
                let turn_id = non_empty_str(frame, "turn_id")?.to_string();
                let items = string_array(frame, "item_ids")?;
                if items.is_empty() {
                    return Err(format!("acp/v1 steer_ack has empty item_ids: {frame}"));
                }
                if let Some(request) = &request {
                    self.requests.remove(request);
                }
                self.turn = Some(turn_id);
                self.duplicate_turn = false;
                if self.delivering.as_ref().is_some_and(|(req, item)| {
                    Some(req.as_str()) == request.as_deref() || items.contains(item)
                }) {
                    self.delivering = None;
                }
                self.advance()?;
            }
            "steer_consumed" => {
                let item = non_empty_str(frame, "steer_id")?.to_string();
                let _ = non_empty_str(frame, "turn_id")?;
                let duplicate = bool_field(frame, "duplicate")?;
                self.requests.retain(|req, mapped| {
                    Some(req.as_str()) != request.as_deref() && mapped != &item
                });
                if self.delivering.as_ref().is_some_and(|(req, mapped)| {
                    Some(req.as_str()) == request.as_deref() || mapped == &item
                }) {
                    self.delivering = None;
                }
                self.echo(&item, duplicate)?;
                self.advance()?;
            }
            "interrupt_ack" => {
                let _ = non_empty_str(frame, "turn_id")?;
                if non_empty_str(frame, "status")? != "cancelling" {
                    return Err(format!("acp/v1 interrupt_ack has invalid status: {frame}"));
                }
            }
            "text_delta" => {
                let text = frame
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("acp/v1 text_delta missing text: {frame}"))?;
                self.reply.push_str(text);
            }
            "event" => {
                let event_kind = non_empty_str(frame, "kind")?;
                if !matches!(
                    event_kind,
                    "text" | "tool_call" | "tool_result" | "compaction" | "status" | "error"
                ) {
                    return Err(format!(
                        "acp/v1 event has unknown kind `{event_kind}`: {frame}"
                    ));
                }
                let _ = non_empty_str(frame, "timestamp")?;
                if !frame.get("data").is_some_and(Value::is_object) {
                    return Err(format!("acp/v1 event missing object data: {frame}"));
                }
            }
            "host_tool_call" => {
                // This client serves no host tool: the call fails at once,
                // so the turn never waits on it.
                let call = non_empty_str(frame, "call_id")?.to_string();
                let _ = non_empty_str(frame, "tool_call_id")?;
                let _ = non_empty_str(frame, "name")?;
                self.control(
                    "host_tool_result",
                    json!({"call_id": call, "output": "no host tool is served", "is_error": true}),
                )?;
            }
            "stream_gap" => {
                if non_empty_str(frame, "stream")? != "deltas"
                    || frame
                        .get("skipped")
                        .and_then(Value::as_u64)
                        .is_none_or(|n| n < 1)
                {
                    return Err(format!("acp/v1 stream_gap is malformed: {frame}"));
                }
            }
            "result" => self.on_result(frame)?,
            "stopped" => return Ok(Flow::Done),
            _ => {
                return Err(format!(
                    "acp/v1 peer wrote an unknown frame `{kind}`: {frame}"
                ));
            }
        }
        Ok(Flow::Continue)
    }

    fn on_ready(&mut self, frame: &Value) -> Result<(), String> {
        if frame.get("protocol").and_then(Value::as_str) != Some(PROTOCOL) {
            return Err(format!(
                "acp/v1 peer answered attach with another protocol: {frame}"
            ));
        }
        let caps = string_array(frame, "capabilities")?;
        self.next_step = caps.iter().any(|cap| cap == NEXT_STEP_STEER);
        self.phase = Phase::Live;
        let delivery = if self.next_step { NEXT_STEP } else { TURN_END };
        let peer = format!(
            "{PROTOCOL} {} {}",
            non_empty_str(frame, "harness")?,
            non_empty_str(frame, "version")?,
        );
        let init = json!({
            "type": "system", "subtype": "init",
            "session_id": self.attach.session_id,
            "peer": peer,
            "delivery": delivery,
        });
        self.write_host(&init)
    }

    fn on_result(&mut self, frame: &Value) -> Result<(), String> {
        let _ = non_empty_str(frame, "turn_id")?;
        let status = non_empty_str(frame, "status")?;
        if !matches!(status, "completed" | "cancelled" | "failed") {
            return Err(format!(
                "acp/v1 result has invalid status `{status}`: {frame}"
            ));
        }
        let is_error = bool_field(frame, "is_error")?;
        let duplicate = bool_field(frame, "duplicate")?;
        let raw_undelivered = string_array(frame, "undelivered_ids")?;
        let cost_micros = match frame.get("cost_micros") {
            None | Some(Value::Null) => None,
            Some(Value::Number(num)) => Some(
                num.as_u64()
                    .ok_or_else(|| format!("acp/v1 result has invalid cost_micros: {frame}"))?,
            ),
            Some(_) => return Err(format!("acp/v1 result has invalid cost_micros: {frame}")),
        };
        self.turn = None;
        self.duplicate_turn = false;
        self.opening = false;
        if self
            .delivering
            .as_ref()
            .is_some_and(|(_, item)| raw_undelivered.contains(item) || self.proven.contains(item))
            && let Some((req, _)) = self.delivering.take()
        {
            self.requests.remove(&req);
        }
        let reply = std::mem::take(&mut self.reply);
        if !duplicate {
            let completed = status == "completed" && !is_error;
            if completed && !reply.is_empty() {
                let assistant = json!({
                    "type": "assistant", "session_id": self.attach.session_id,
                    "message": {"role": "assistant", "content": [{"type": "text", "text": reply}]},
                });
                self.write_host(&assistant)?;
            }
            let usage = frame.get("usage");
            let tokens = |name: &str| {
                usage
                    .and_then(|usage| usage.get(name))
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
            };
            let mut result = json!({
                "type": "result",
                "session_id": self.attach.session_id,
                "usage": {
                    "input_tokens": tokens("input_tokens"),
                    "output_tokens": tokens("output_tokens"),
                    "cache_read_input_tokens": tokens("cached_tokens"),
                    "cache_creation_input_tokens": tokens("cache_write_tokens"),
                },
            });
            if let Some(cost) = cost_micros {
                let total = self.cost_micros_total.unwrap_or(0).saturating_add(cost);
                self.cost_micros_total = Some(total);
                result["total_cost_usd"] = json!(total as f64 / 1_000_000.0);
            }
            if completed {
                result["subtype"] = json!("success");
                result["is_error"] = json!(false);
                result["result"] = json!(reply);
            } else {
                let error = frame.get("error").and_then(Value::as_str).unwrap_or(status);
                result["subtype"] = json!("error_during_execution");
                result["is_error"] = json!(true);
                result["result"] = json!(format!("acp/v1 turn {status}: {error}"));
            }
            self.write_host(&result)?;
        }
        // What the turn never took goes out again, first, in its order.
        let undelivered: Vec<String> = raw_undelivered
            .into_iter()
            .filter(|id| !self.proven.contains(id) && !self.waiting.contains(id))
            .collect();
        for id in undelivered.into_iter().rev() {
            self.waiting.push_front(id);
        }
        self.advance()
    }

    fn flush_cancelled(&mut self) -> Result<(), String> {
        let batch = std::mem::take(&mut self.cancelled_batch);
        for id in batch {
            if let Some(line) = self.lines.remove(&id) {
                self.lifecycle(&line, "cancelled")?;
            }
        }
        Ok(())
    }

    fn on_rejected(&mut self, frame: &Value) -> Result<(), String> {
        let code = non_empty_str(frame, "code")?;
        let item = frame
            .get("request_id")
            .and_then(Value::as_str)
            .and_then(|request| self.requests.remove(request));
        match code {
            // The running turn's state refused it: the host offers it again,
            // and what waits behind it goes back too so mailbox FIFO holds.
            "turn_open" | "turn_closed" | "no_active_turn" | "turn_not_started" => {
                let Some(item) = item else { return Ok(()) };
                if code == "turn_open" && self.turn.is_none() {
                    self.turn = Some(String::new());
                    self.duplicate_turn = false;
                } else if matches!(code, "no_active_turn" | "turn_not_started") {
                    self.turn = None;
                    self.duplicate_turn = false;
                }
                self.opening = false;
                self.delivering = None;
                self.cancelled_batch.push(item);
                self.cancelled_batch.extend(self.waiting.drain(..));
                if !self.defer_cancel {
                    self.flush_cancelled()?;
                }
                Ok(())
            }
            // An answer to a host tool call that the peer no longer awaits.
            "unknown_call" => Ok(()),
            // Fenced, unknown or refused session, or a frame this client
            // should never have sent: nothing more goes to this peer.
            _ => Err(format!("acp/v1 peer rejected the session: {frame}")),
        }
    }
}

/// The peer's stream: its frames, the writer for ours, and the process
/// that serves it, if this client started it.
struct Peer {
    frames: Box<dyn BufRead + Send>,
    input: Box<dyn Write + Send>,
    child: Option<std::process::Child>,
}

fn connect(endpoint: &Endpoint, work_dir: &Path) -> Result<Peer, String> {
    match endpoint {
        Endpoint::Command(argv) => {
            let mut command = std::process::Command::new(&argv[0]);
            command
                .args(&argv[1..])
                .current_dir(work_dir)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::inherit());
            let mut child = command
                .spawn()
                .map_err(|err| format!("cannot start acp/v1 peer `{}`: {err}", argv[0]))?;
            let input = child.stdin.take().ok_or("acp/v1 peer stdin missing")?;
            let output = child.stdout.take().ok_or("acp/v1 peer stdout missing")?;
            Ok(Peer {
                frames: Box::new(std::io::BufReader::new(output)),
                input: Box::new(input),
                child: Some(child),
            })
        }
        #[cfg(unix)]
        Endpoint::Socket(path) => {
            let stream = std::os::unix::net::UnixStream::connect(path)
                .map_err(|err| format!("cannot reach acp/v1 peer at {}: {err}", path.display()))?;
            let reader = stream.try_clone().map_err(|err| err.to_string())?;
            Ok(Peer {
                frames: Box::new(std::io::BufReader::new(reader)),
                input: Box::new(SocketWriter(stream)),
                child: None,
            })
        }
        #[cfg(not(unix))]
        Endpoint::Socket(path) => Err(format!(
            "acp/v1 over a Unix socket ({}) needs a Unix host",
            path.display()
        )),
    }
}

// Closing the writer must also wake the reader's cloned socket.
#[cfg(unix)]
struct SocketWriter(std::os::unix::net::UnixStream);

#[cfg(unix)]
impl Write for SocketWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

#[cfg(unix)]
impl Drop for SocketWriter {
    fn drop(&mut self) {
        let _ = self.0.shutdown(std::net::Shutdown::Both);
    }
}

fn forward_lines(
    source: impl BufRead,
    sink: mpsc::Sender<Event>,
    wrap: fn(String) -> Event,
    end: Event,
) {
    for line in source.lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        if sink.send(wrap(line)).is_err() {
            return;
        }
    }
    let _ = sink.send(end);
}

/// Serve the host's stream-json over the `acp/v1` peer `acp` names, until
/// the host closes its input or the peer ends.
#[allow(clippy::too_many_arguments)]
pub fn serve<R: BufRead + Send + 'static, W: Write>(
    acp: &Config,
    config_dir: &Path,
    cloud_harness: &str,
    session_id: Option<String>,
    model: Option<String>,
    instructions: Option<String>,
    host_in: R,
    host_out: W,
) -> Result<(), String> {
    let session_id = session_id.unwrap_or_else(|| new_session_id(cloud_harness));
    super::persist_session_id(config_dir, cloud_harness, &session_id);
    let previous =
        read_lease(config_dir, cloud_harness)?.filter(|lease| lease.session_id == session_id);
    let lease = Lease {
        session_id: session_id.clone(),
        lease_epoch: previous.as_ref().map_or(1, |lease| lease.lease_epoch + 1),
        last_seq: previous.and_then(|lease| lease.last_seq),
    };
    // The epoch is taken before it is used: a crash never reuses it.
    write_lease(config_dir, cloud_harness, &lease)?;
    let work_dir = std::env::current_dir().unwrap_or_else(|_| config_dir.to_path_buf());
    let Peer {
        frames,
        input,
        mut child,
    } = connect(&acp.endpoint, &work_dir)?;
    let attach = Attach {
        session_id,
        lease_epoch: lease.lease_epoch,
        after_seq: lease.last_seq,
        harness: acp.harness.clone(),
        model,
        instructions,
    };
    let (config, harness) = (config_dir.to_path_buf(), cloud_harness.to_string());
    let mut kept = lease;
    let on_seq = Box::new(move |seq| {
        kept.last_seq = Some(seq);
        if let Err(err) = write_lease(&config, &harness, &kept) {
            eprintln!("{err}");
        }
    });
    let result = drive(
        Client::new(host_out, input, attach, on_seq),
        host_in,
        frames,
    );
    if let Some(child) = child.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }
    result
}

/// Run `client` over its two streams until either side ends it.
pub fn drive<H: Write, P: Write, R: BufRead + Send + 'static>(
    mut client: Client<H, P>,
    host_in: R,
    peer_out: Box<dyn BufRead + Send>,
) -> Result<(), String> {
    let (sink, events) = mpsc::channel();
    let peer_sink = sink.clone();
    std::thread::spawn(move || forward_lines(peer_out, peer_sink, Event::Peer, Event::PeerClosed));
    std::thread::spawn(move || forward_lines(host_in, sink, Event::Host, Event::HostClosed));
    client.send_attach()?;
    // The host's lines wait until the peer said `ready`.
    let mut early = VecDeque::new();
    let ready_by = Instant::now() + READY_WAIT;
    while !client.is_live() {
        let left = ready_by.saturating_duration_since(Instant::now());
        match events.recv_timeout(left) {
            Ok(Event::Peer(line)) => {
                if client.on_peer(&line)? == Flow::Done {
                    return Ok(());
                }
            }
            Ok(Event::PeerClosed) => return Err("acp/v1 peer closed before ready".into()),
            Ok(host) => early.push_back(host),
            Err(_) => return Err(format!("acp/v1 peer sent no ready within {READY_WAIT:?}")),
        }
    }
    client.defer_cancel = true;
    let mut host_closed = false;
    let mut stop_by: Option<Instant> = None;
    let mut cancel_by: Option<Instant> = None;
    loop {
        if let Some(deadline) = cancel_by
            && (Instant::now() >= deadline || client.stopping || host_closed)
        {
            cancel_by = None;
            client.flush_cancelled()?;
            client.advance()?;
        }
        if !client.cancelled_batch.is_empty() && cancel_by.is_none() {
            cancel_by = Some(Instant::now() + REFUSAL_BACKOFF);
        }
        if host_closed && !client.stopping && !client.has_work() {
            // The local adapter disconnected without archiving the session:
            // leave the remote `acp/v1` session alive for the next `attach`.
            return Ok(());
        }
        if client.stopping && stop_by.is_none() {
            stop_by = Some(Instant::now() + STOP_WAIT);
        }
        let next_deadline = match (stop_by, cancel_by) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        let event = match early.pop_front() {
            Some(event) => event,
            None => match next_deadline {
                None => match events.recv() {
                    Ok(event) => event,
                    Err(_) => return Ok(()),
                },
                Some(deadline) => {
                    match events.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                        Ok(event) => event,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            if stop_by.is_some_and(|stop| Instant::now() >= stop) {
                                return Ok(());
                            }
                            continue;
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                    }
                }
            },
        };
        match event {
            Event::Host(line) => client.on_host(&line)?,
            Event::HostClosed => host_closed = true,
            Event::Peer(line) => {
                if client.on_peer(&line)? == Flow::Done {
                    return Ok(());
                }
            }
            Event::PeerClosed if stop_by.is_some() || (host_closed && !client.has_work()) => {
                return Ok(());
            }
            Event::PeerClosed => return Err("acp/v1 peer closed its stream".into()),
        }
    }
}

fn non_empty_str<'a>(frame: &'a Value, field: &str) -> Result<&'a str, String> {
    frame
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("acp/v1 frame missing `{field}`: {frame}"))
}

fn nullable_id<'a>(frame: &'a Value, field: &str) -> Result<Option<&'a str>, String> {
    match frame.get(field) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.as_str())),
        _ => Err(format!("acp/v1 frame missing `{field}`: {frame}")),
    }
}

fn bool_field(frame: &Value, field: &str) -> Result<bool, String> {
    frame
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("acp/v1 frame missing boolean `{field}`: {frame}"))
}

fn string_array(frame: &Value, field: &str) -> Result<Vec<String>, String> {
    let items = frame
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("acp/v1 frame missing array `{field}`: {frame}"))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    format!("acp/v1 `{field}` contains a non-string/empty item: {frame}")
                })
        })
        .collect()
}

fn new_session_id(harness: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    format!("{harness}-{nanos:x}-{:x}", std::process::id())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl Write for Buf {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Buf {
        /// Every line written since the last call.
        fn take(&self) -> Vec<Value> {
            let bytes = std::mem::take(&mut *self.0.lock().unwrap());
            String::from_utf8(bytes)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }
    }

    struct Rig {
        client: Client<Buf, Buf>,
        host: Buf,
        peer: Buf,
        seqs: Arc<Mutex<Vec<u64>>>,
    }

    const SESSION: &str = "s-1";

    fn make_rig(lease_epoch: u64, after_seq: Option<u64>) -> Rig {
        let (host, peer) = (Buf::default(), Buf::default());
        let seqs = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&seqs);
        let attach = Attach {
            session_id: SESSION.into(),
            lease_epoch,
            after_seq,
            harness: "runner".into(),
            model: None,
            instructions: None,
        };
        let mut client = Client::new(
            host.clone(),
            peer.clone(),
            attach,
            Box::new(move |seq| kept.lock().unwrap().push(seq)),
        );
        client.send_attach().unwrap();
        Rig {
            client,
            host,
            peer,
            seqs,
        }
    }

    fn user(id: &str) -> String {
        json!({
            "type": "user", "uuid": format!("u-{id}"),
            "message": {"role": "user", "content": [
                {"type": "text", "text": format!("anyagents-message:{id}\nhello")}
            ]},
        })
        .to_string()
    }

    fn frame(seq: u64, lease: u64, request: Option<&str>, body: Value) -> String {
        let mut frame = body;
        frame["seq"] = json!(seq);
        frame["session_id"] = json!(SESSION);
        frame["lease_epoch"] = json!(lease);
        frame["request_id"] = json!(request);
        frame.to_string()
    }

    fn ready(seq: u64, lease: u64, capabilities: &[&str]) -> String {
        frame(
            seq,
            lease,
            Some("1.1"),
            json!({"type": "ready", "protocol": "acp/v1", "protocols": ["acp/v1"],
                   "harness": "runner", "version": "1.0.0", "capabilities": capabilities}),
        )
    }

    /// A live client at `next-step`, its attach and init already read.
    fn live() -> Rig {
        let mut rig = make_rig(1, None);
        rig.client
            .on_peer(&ready(1, 1, &[NEXT_STEP_STEER]))
            .unwrap();
        rig.peer.take();
        rig.host.take();
        rig
    }

    fn echoed(lines: &[Value]) -> Vec<String> {
        lines
            .iter()
            .filter(|line| line["isReplay"] == json!(true))
            .map(|line| line["uuid"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn config_names_exactly_one_endpoint() {
        assert_eq!(config(None, "claude-cloud"), Ok(None));
        assert_eq!(
            config(Some(&json!({"base_url": "x"})), "claude-cloud"),
            Ok(None)
        );
        assert_eq!(
            config(
                Some(&json!({"acp": {"command": ["runner", "--stdio"]}})),
                "claude-cloud"
            ),
            Ok(Some(Config {
                endpoint: Endpoint::Command(vec!["runner".into(), "--stdio".into()]),
                harness: "claude-cloud".into(),
            }))
        );
        assert_eq!(
            config(
                Some(&json!({"acp": {"socket": "/run/acp.sock", "harness": "claude"}})),
                "claude-cloud"
            ),
            Ok(Some(Config {
                endpoint: Endpoint::Socket("/run/acp.sock".into()),
                harness: "claude".into(),
            }))
        );
        for bad in [
            json!({"acp": {}}),
            json!({"acp": {"command": [], "socket": "/s"}}),
            json!({"acp": {"command": ["a"], "socket": "/s"}}),
            json!({"acp": {"command": "runner"}}),
            json!({"acp": {"socket": "/s", "harness": ""}}),
        ] {
            assert!(config(Some(&bad), "claude-cloud").is_err(), "{bad}");
        }
    }

    /// `attach` carries the lease, the replay point and the harness, and
    /// `ready` with `next_step_steer` makes the init declare `next-step`.
    #[test]
    fn attach_and_ready_declare_the_peers_level() {
        let mut rig = make_rig(3, Some(7));
        let attach = rig.peer.take();
        assert_eq!(attach.len(), 1);
        assert_eq!(attach[0]["type"], "attach");
        assert_eq!(attach[0]["protocol"], "acp/v1");
        assert_eq!(attach[0]["session_id"], SESSION);
        assert_eq!(attach[0]["lease_epoch"], 3);
        assert_eq!(attach[0]["after_seq"], 7);
        assert_eq!(attach[0]["harness"], "runner");
        assert_eq!(attach[0]["request_id"], "3.1");
        rig.client
            .on_peer(&ready(8, 3, &[NEXT_STEP_STEER]))
            .unwrap();
        let init = rig.host.take();
        assert_eq!(init[0]["type"], "system");
        assert_eq!(init[0]["subtype"], "init");
        assert_eq!(init[0]["delivery"]["level"], "next-step");
        assert_eq!(init[0]["delivery"]["idempotent"], true);
        assert_eq!(init[0]["peer"], "acp/v1 runner 1.0.0");

        let mut plain = rig_with_ready(&[]);
        assert_eq!(plain.host.take()[0]["delivery"]["level"], "turn-end");
        plain.client.on_host(&user("a")).unwrap();
        assert_eq!(plain.peer.take()[0]["mode"], "turn");
    }

    fn rig_with_ready(capabilities: &[&str]) -> Rig {
        let mut rig = make_rig(1, None);
        rig.client.on_peer(&ready(1, 1, capabilities)).unwrap();
        rig.peer.take();
        rig
    }

    /// `steer_ack` is admission only: no echo, so nothing commits. Each
    /// `steer_consumed` echoes its own line, in the order consumed.
    #[test]
    fn only_acceptance_and_consumption_prove_a_line() {
        let mut rig = live();
        rig.client.on_host(&user("a")).unwrap();
        let deliver = rig.peer.take();
        assert_eq!(deliver[0]["type"], "deliver");
        assert_eq!(deliver[0]["mode"], "turn");
        assert_eq!(
            deliver[0]["items"],
            json!([{"id": "a", "text": "anyagents-message:a\nhello"}])
        );
        let a = deliver[0]["request_id"].as_str().unwrap().to_string();
        rig.client
            .on_peer(&frame(
                2,
                1,
                Some(&a),
                json!({"type": "accepted", "turn_id": "t1", "mode": "turn"}),
            ))
            .unwrap();
        assert_eq!(echoed(&rig.host.take()), ["u-a"]);

        rig.client.on_host(&user("b")).unwrap();
        rig.client.on_host(&user("c")).unwrap();
        let first_steer = rig.peer.take();
        assert_eq!(first_steer.len(), 1, "fenced until b is acknowledged");
        assert_eq!(first_steer[0]["mode"], "steer");
        assert_eq!(first_steer[0]["items"][0]["id"], "b");
        let req_b = first_steer[0]["request_id"].as_str().unwrap().to_string();
        rig.client
            .on_peer(&frame(
                3,
                1,
                Some(&req_b),
                json!({"type": "steer_ack", "turn_id": "t1", "item_ids": ["b"]}),
            ))
            .unwrap();
        let second_steer = rig.peer.take();
        assert_eq!(second_steer.len(), 1);
        assert_eq!(second_steer[0]["mode"], "steer");
        assert_eq!(second_steer[0]["items"][0]["id"], "c");
        let req_c = second_steer[0]["request_id"].as_str().unwrap().to_string();
        rig.client
            .on_peer(&frame(
                4,
                1,
                Some(&req_c),
                json!({"type": "steer_ack", "turn_id": "t1", "item_ids": ["c"]}),
            ))
            .unwrap();
        assert!(rig.host.take().is_empty(), "steer_ack proves nothing");
        for (seq, id) in [(5, "b"), (6, "c")] {
            rig.client
                .on_peer(&frame(seq, 1, None, json!({"type": "steer_consumed", "steer_id": id, "turn_id": "t1", "duplicate": false})))
                .unwrap();
        }
        assert_eq!(echoed(&rig.host.take()), ["u-b", "u-c"]);
        rig.client
            .on_peer(&frame(
                7,
                1,
                None,
                json!({"type": "text_delta", "text": "done"}),
            ))
            .unwrap();
        rig.client
            .on_peer(&frame(8, 1, None, json!({"type": "result", "turn_id": "t1", "status": "completed",
                "is_error": false, "error": null, "duplicate": false,
                "usage": {"input_tokens": 3, "output_tokens": 4, "cached_tokens": 0, "cache_write_tokens": 0},
                "cost_micros": null, "context": null, "undelivered_ids": []})))
            .unwrap();
        let out = rig.host.take();
        assert_eq!(out[0]["type"], "assistant");
        assert_eq!(out[1]["type"], "result");
        assert_eq!(out[1]["subtype"], "success");
        assert_eq!(out[1]["usage"]["output_tokens"], 4);
        assert!(rig.peer.take().is_empty());
        // Every seq but the delta's is kept for the next attach.
        assert_eq!(*rig.seqs.lock().unwrap(), [1, 2, 3, 4, 5, 6, 8]);
    }

    /// Ids a result hands back go out again at once, one deliver each, in
    /// their order, after the host has the result.
    #[test]
    fn undelivered_ids_are_offered_again_in_order() {
        let mut rig = live();
        for id in ["a", "b", "c"] {
            rig.client.on_host(&user(id)).unwrap();
        }
        let opening = rig.peer.take();
        assert_eq!(opening.len(), 1);
        assert_eq!(opening[0]["mode"], "turn");
        assert_eq!(opening[0]["items"][0]["id"], "a");
        rig.client
            .on_peer(&frame(
                2,
                1,
                Some("1.2"),
                json!({"type": "accepted", "turn_id": "t1", "mode": "turn"}),
            ))
            .unwrap();
        let steer_b = rig.peer.take();
        assert_eq!(steer_b.len(), 1);
        assert_eq!(steer_b[0]["mode"], "steer");
        assert_eq!(steer_b[0]["items"][0]["id"], "b");
        rig.client
            .on_peer(&frame(
                3,
                1,
                Some("1.3"),
                json!({"type": "steer_ack", "turn_id": "t1", "item_ids": ["b"]}),
            ))
            .unwrap();
        let steer_c = rig.peer.take();
        assert_eq!(steer_c.len(), 1);
        assert_eq!(steer_c[0]["mode"], "steer");
        assert_eq!(steer_c[0]["items"][0]["id"], "c");
        rig.client
            .on_peer(&frame(
                4,
                1,
                Some("1.4"),
                json!({"type": "steer_ack", "turn_id": "t1", "item_ids": ["c"]}),
            ))
            .unwrap();
        rig.host.take();
        rig.client
            .on_peer(&frame(
                5,
                1,
                None,
                json!({"type": "result", "turn_id": "t1", "status": "cancelled",
                "is_error": false, "error": null, "duplicate": false, "usage": null,
                "cost_micros": null, "context": null, "undelivered_ids": ["b", "c"]}),
            ))
            .unwrap();
        let out = rig.host.take();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "result");
        assert_eq!(out[0]["is_error"], true);
        // Only `b` opens `t2` with `mode: "turn"`; `c` waits in `waiting` so
        // a fresh arrival `d` cannot jump ahead of `c`.
        let again = rig.peer.take();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0]["mode"], "turn");
        assert_eq!(again[0]["items"][0]["id"], "b");
        assert_eq!(again[0]["request_id"], "1.5");
        rig.client.on_host(&user("d")).unwrap();
        assert!(
            rig.peer.take().is_empty(),
            "d waits behind c until t2 accepts b"
        );
        rig.client
            .on_peer(&frame(
                6,
                1,
                Some("1.5"),
                json!({"type": "accepted", "turn_id": "t2", "mode": "turn"}),
            ))
            .unwrap();
        assert_eq!(echoed(&rig.host.take()), ["u-b"]);
        let next_steer = rig.peer.take();
        assert_eq!(next_steer.len(), 1);
        assert_eq!(next_steer[0]["mode"], "steer");
        assert_eq!(next_steer[0]["items"][0]["id"], "c");
        let req_c2 = next_steer[0]["request_id"].as_str().unwrap().to_string();
        rig.client
            .on_peer(&frame(
                7,
                1,
                Some(&req_c2),
                json!({"type": "steer_ack", "turn_id": "t2", "item_ids": ["c"]}),
            ))
            .unwrap();
        let after_c = rig.peer.take();
        assert_eq!(after_c.len(), 1);
        assert_eq!(after_c[0]["mode"], "steer");
        assert_eq!(after_c[0]["items"][0]["id"], "d");
    }

    /// A line the session already consumed is proved as a duplicate: idle,
    /// by `accepted` `duplicate` and its result; mid-turn, by
    /// `steer_consumed` `duplicate`.
    #[test]
    fn a_consumed_line_is_proved_once_more_as_a_duplicate() {
        let mut rig = live();
        rig.client.on_host(&user("a")).unwrap();
        rig.client
            .on_peer(&frame(
                2,
                1,
                Some("1.2"),
                json!({"type": "accepted", "turn_id": "d", "mode": "duplicate"}),
            ))
            .unwrap();
        rig.client
            .on_peer(&frame(
                3,
                1,
                Some("1.2"),
                json!({"type": "result", "turn_id": "d", "status": "completed",
                "is_error": false, "error": null, "duplicate": true, "usage": null,
                "cost_micros": null, "context": null, "undelivered_ids": []}),
            ))
            .unwrap();
        let out = rig.host.take();
        assert_eq!(out.len(), 1, "a duplicate turn emits no synthetic result");
        assert_eq!(echoed(&out), ["u-a"]);
        assert_eq!(out[0]["duplicate"], json!(true));

        rig.client.on_host(&user("b")).unwrap();
        rig.client
            .on_peer(&frame(
                4,
                1,
                Some("1.3"),
                json!({"type": "accepted", "turn_id": "t", "mode": "turn"}),
            ))
            .unwrap();
        rig.client.on_host(&user("a")).unwrap();
        rig.client
            .on_peer(&frame(5, 1, Some("1.4"), json!({"type": "steer_consumed", "steer_id": "a", "turn_id": "t", "duplicate": true})))
            .unwrap();
        let out = rig.host.take();
        assert_eq!(echoed(&out), ["u-b", "u-a"]);
        assert_eq!(out[0].get("duplicate"), None);
        assert_eq!(out[1]["duplicate"], json!(true));
    }

    /// A fenced or unknown session ends the process; a refusal the running
    /// turn's state explains hands the line back to the host.
    #[test]
    fn a_fenced_session_ends_and_a_turn_refusal_hands_the_line_back() {
        let mut rig = make_rig(2, None);
        let stale = json!({"type": "rejected", "seq": null, "session_id": SESSION, "lease_epoch": 2,
            "request_id": "2.1", "code": "stale_lease_epoch", "active_lease_epoch": 5, "error": null});
        assert!(
            rig.client
                .on_peer(&stale.to_string())
                .unwrap_err()
                .contains("stale_lease_epoch")
        );

        let mut rig = live();
        rig.client.on_host(&user("a")).unwrap();
        let fenced = json!({"type": "rejected", "seq": null, "session_id": SESSION, "lease_epoch": 1,
            "request_id": "1.2", "code": "not_attached", "active_lease_epoch": 2, "error": null});
        assert!(
            rig.client
                .on_peer(&fenced.to_string())
                .unwrap_err()
                .contains("not_attached")
        );

        let mut rig = live();
        rig.client.on_host(&user("a")).unwrap();
        let closed = json!({"type": "rejected", "seq": null, "session_id": SESSION, "lease_epoch": 1,
            "request_id": "1.2", "code": "turn_closed", "error": null});
        rig.client.on_peer(&closed.to_string()).unwrap();
        let out = rig.host.take();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "command_lifecycle");
        assert_eq!(out[0]["state"], "cancelled");
        assert_eq!(out[0]["uuid"], "u-a");
        assert!(rig.client.lines.is_empty(), "cancelled line is removed");
        // `turn_open` marks a turn active so the re-offered line steers into it.
        let mut rig = live();
        rig.client.on_host(&user("a")).unwrap();
        assert_eq!(rig.peer.take()[0]["mode"], "turn");
        let open = json!({"type": "rejected", "seq": null, "session_id": SESSION, "lease_epoch": 1,
            "request_id": "1.2", "code": "turn_open", "error": null});
        rig.client.on_peer(&open.to_string()).unwrap();
        assert_eq!(rig.host.take()[0]["state"], "cancelled");
        assert!(rig.client.lines.is_empty());
        rig.client.on_host(&user("a")).unwrap();
        assert_eq!(rig.peer.take()[0]["mode"], "steer");
        // An unknown code is no reason to go on.
        let mut rig = live();
        let odd = json!({"type": "rejected", "seq": null, "session_id": SESSION, "lease_epoch": 1,
            "request_id": "1.9", "code": "something_new", "error": null});
        assert!(rig.client.on_peer(&odd.to_string()).is_err());
    }

    #[test]
    fn rejected_interrupt_does_not_clobber_in_flight_deliver_fence() {
        // Case 1: opening `deliver` (`m1`) is in flight when an `interrupt` is
        // rejected with `no_active_turn`. Neither `opening` nor `waiting` is
        // disturbed, so `m2` and `m3` stay fenced behind `m1` in FIFO order.
        let mut rig = live();
        rig.client.on_host(&user("m1")).unwrap();
        let sent_m1 = rig.peer.take();
        assert_eq!(sent_m1.len(), 1);
        assert_eq!(sent_m1[0]["mode"], "turn");
        let req_m1 = sent_m1[0]["request_id"].as_str().unwrap().to_string();

        rig.client
            .on_host(r#"{"type":"control_request","request":{"subtype":"interrupt"}}"#)
            .unwrap();
        let sent_int = rig.peer.take();
        assert_eq!(sent_int.len(), 1);
        assert_eq!(sent_int[0]["type"], "interrupt");
        let req_int = sent_int[0]["request_id"].as_str().unwrap().to_string();

        rig.client.on_host(&user("m2")).unwrap();
        assert!(rig.peer.take().is_empty(), "m2 waits behind opening m1");

        let rejected_int = json!({
            "type": "rejected",
            "seq": null,
            "session_id": SESSION,
            "lease_epoch": 1,
            "request_id": req_int,
            "code": "no_active_turn",
            "error": null,
        });
        rig.client.on_peer(&rejected_int.to_string()).unwrap();
        assert!(rig.host.take().is_empty());
        assert!(rig.client.opening, "opening fence remains intact");

        rig.client.on_host(&user("m3")).unwrap();
        assert!(rig.peer.take().is_empty(), "m3 must not race m1 or m2");

        rig.client
            .on_peer(&frame(
                2,
                1,
                Some(&req_m1),
                json!({"type": "accepted", "turn_id": "t1", "mode": "turn"}),
            ))
            .unwrap();
        assert_eq!(echoed(&rig.host.take()), ["u-m1"]);
        let sent_m2 = rig.peer.take();
        assert_eq!(sent_m2.len(), 1);
        assert_eq!(sent_m2[0]["mode"], "steer");
        assert_eq!(sent_m2[0]["items"][0]["id"], "m2");
        let req_m2 = sent_m2[0]["request_id"].as_str().unwrap().to_string();

        // Case 2: steer `deliver` (`m2`) is in flight when an `interrupt` is
        // rejected with `no_active_turn`. `delivering` stays intact so a fresh
        // `m4` cannot overtake `m2` or `m3`.
        rig.client
            .on_host(r#"{"type":"control_request","request":{"subtype":"interrupt"}}"#)
            .unwrap();
        let sent_int2 = rig.peer.take();
        let req_int2 = sent_int2[0]["request_id"].as_str().unwrap().to_string();
        let rejected_int2 = json!({
            "type": "rejected",
            "seq": null,
            "session_id": SESSION,
            "lease_epoch": 1,
            "request_id": req_int2,
            "code": "no_active_turn",
            "error": null,
        });
        rig.client.on_peer(&rejected_int2.to_string()).unwrap();
        assert_eq!(
            rig.client.delivering.as_ref().map(|(req, _)| req.as_str()),
            Some(req_m2.as_str())
        );

        rig.client.on_host(&user("m4")).unwrap();
        assert!(rig.peer.take().is_empty(), "m4 waits behind m2 and m3");

        rig.client
            .on_peer(&frame(
                3,
                1,
                Some(&req_m2),
                json!({"type": "steer_ack", "turn_id": "t1", "item_ids": ["m2"]}),
            ))
            .unwrap();
        let sent_m3 = rig.peer.take();
        assert_eq!(sent_m3.len(), 1);
        assert_eq!(sent_m3[0]["mode"], "steer");
        assert_eq!(sent_m3[0]["items"][0]["id"], "m3");
        let req_m3 = sent_m3[0]["request_id"].as_str().unwrap().to_string();
        rig.client
            .on_peer(&frame(
                4,
                1,
                Some(&req_m3),
                json!({"type": "steer_ack", "turn_id": "t1", "item_ids": ["m3"]}),
            ))
            .unwrap();
        let sent_m4 = rig.peer.take();
        assert_eq!(sent_m4.len(), 1);
        assert_eq!(sent_m4[0]["mode"], "steer");
        assert_eq!(sent_m4[0]["items"][0]["id"], "m4");
    }

    #[test]
    fn corrupt_lease_file_fails_closed_without_resetting_epoch() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_lease(dir.path(), "claude-cloud").unwrap().is_none());
        std::fs::write(lease_path(dir.path(), "claude-cloud"), "{not-valid-json").unwrap();
        let err = read_lease(dir.path(), "claude-cloud").unwrap_err();
        assert!(err.contains("corrupt lease"), "unexpected error: {err}");
        let acp_cfg = Config {
            endpoint: Endpoint::Command(vec!["/bin/true".into()]),
            harness: "claude-cloud".into(),
        };
        let serve_err = serve(
            &acp_cfg,
            dir.path(),
            "claude-cloud",
            Some(SESSION.into()),
            None,
            None,
            std::io::Cursor::new(Vec::<u8>::new()),
            Buf::default(),
        )
        .unwrap_err();
        assert!(
            serve_err.contains("corrupt lease"),
            "unexpected error: {serve_err}"
        );
        assert_eq!(
            std::fs::read_to_string(lease_path(dir.path(), "claude-cloud")).unwrap(),
            "{not-valid-json"
        );
    }

    /// Replayed frames are read for their seq, never forwarded; after
    /// `ready` a frame of another lease is ignored and a gap ends the
    /// process. A peer that lost the replay fails closed.
    #[test]
    fn replay_is_read_for_its_seq_and_live_frames_must_be_contiguous() {
        let mut rig = make_rig(2, Some(4));
        rig.peer.take();
        let old = |seq| {
            frame(
                seq,
                1,
                None,
                json!({"type": "steer_consumed", "steer_id": "x", "turn_id": "t", "duplicate": false}),
            )
        };
        rig.client.on_peer(&old(4)).unwrap();
        rig.client.on_peer(&old(5)).unwrap();
        rig.client
            .on_peer(&frame(
                6,
                1,
                None,
                json!({"type": "result", "turn_id": "t", "status": "completed",
                "is_error": false, "error": null, "duplicate": false, "usage": null,
                "cost_micros": null, "context": null, "undelivered_ids": []}),
            ))
            .unwrap();
        assert!(
            rig.host.take().is_empty(),
            "a replayed frame reached the host"
        );
        assert!(!rig.client.is_live());
        rig.client
            .on_peer(&ready(7, 2, &[NEXT_STEP_STEER]))
            .unwrap();
        assert!(rig.client.is_live());
        assert_eq!(*rig.seqs.lock().unwrap(), [5, 6, 7]);
        rig.host.take();
        // A frame of the old lease, after ready, changes nothing.
        rig.client.on_peer(&old(8)).unwrap();
        assert_eq!(*rig.seqs.lock().unwrap(), [5, 6, 7]);
        let skipped = frame(
            9,
            2,
            None,
            json!({"type": "steer_ack", "turn_id": "t", "item_ids": ["y"]}),
        );
        assert!(
            rig.client
                .on_peer(&skipped)
                .unwrap_err()
                .contains("skipped")
        );

        let mut rig = make_rig(2, Some(4));
        rig.peer.take();
        let gone = json!({"type": "rejected", "seq": null, "session_id": SESSION, "lease_epoch": 2,
            "request_id": "2.1", "code": "replay_unavailable", "error": null});
        assert!(rig.client.on_peer(&gone.to_string()).is_err());
        assert!(rig.peer.take().is_empty());
    }

    /// A frame that cannot be read, or lacks its envelope, ends the process.
    #[test]
    fn an_unreadable_frame_ends_the_process() {
        let mut rig = live();
        assert!(rig.client.on_peer("{not json").is_err());
        assert!(rig.client.on_peer(r#"{"session_id":"s-1"}"#).is_err());
        assert!(
            rig.client
                .on_peer(r#"{"type":"accepted","session_id":"s-1","lease_epoch":1}"#)
                .is_err()
        );
        assert!(
            rig.client
                .on_peer(r#"{"type":"protocol_error","code":"invalid_envelope","session_id":null}"#)
                .is_err()
        );
        // Missing required fields on result, ready, steer_consumed, or unknown types fail closed.
        assert!(
            rig.client
                .on_peer(&frame(2, 1, None, json!({"type": "result", "turn_id": "t", "status": "completed", "is_error": false, "duplicate": false})))
                .is_err()
        );
        let mut bad_ready = make_rig(1, None);
        assert!(
            bad_ready
                .client
                .on_peer(&frame(
                    1,
                    1,
                    Some("1.1"),
                    json!({"type": "ready", "protocol": "acp/v1", "harness": "r", "version": "1"})
                ))
                .is_err()
        );
        let mut unknown = live();
        assert!(
            unknown
                .client
                .on_peer(&frame(2, 1, None, json!({"type": "mystery_frame"})))
                .is_err()
        );
        // Another session's frame or protocol_error is not ours to judge.
        let mut mux = live();
        let other =
            r#"{"type":"accepted","session_id":"s-2","lease_epoch":1,"seq":99,"request_id":null}"#;
        assert_eq!(mux.client.on_peer(other), Ok(Flow::Continue));
        let other_err = r#"{"type":"protocol_error","code":"frame_too_large","session_id":"s-2"}"#;
        assert_eq!(mux.client.on_peer(other_err), Ok(Flow::Continue));
    }

    /// At `turn-end` a line waits here while a turn runs, and opens the next.
    #[test]
    fn at_turn_end_a_line_waits_for_the_running_turn() {
        let mut rig = rig_with_ready(&[]);
        rig.host.take();
        rig.client.on_host(&user("a")).unwrap();
        rig.client.on_host(&user("b")).unwrap();
        let sent = rig.peer.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["items"][0]["id"], "a");
        rig.client
            .on_peer(&frame(
                2,
                1,
                Some("1.2"),
                json!({"type": "accepted", "turn_id": "t1", "mode": "turn"}),
            ))
            .unwrap();
        assert_eq!(echoed(&rig.host.take()), ["u-a"]);
        assert!(rig.peer.take().is_empty());
        rig.client
            .on_peer(&frame(
                3,
                1,
                None,
                json!({"type": "result", "turn_id": "t1", "status": "completed",
                "is_error": false, "error": null, "duplicate": false, "usage": null,
                "cost_micros": null, "context": null, "undelivered_ids": []}),
            ))
            .unwrap();
        let next = rig.peer.take();
        assert_eq!(next[0]["mode"], "turn");
        assert_eq!(next[0]["items"][0]["id"], "b");
    }

    /// `hello` answers `tunnel_ready`, a journaled `rejected` advances `seq`,
    /// `host_tool_call` fails fast, `cost_micros` accumulates, and `stop`
    /// ends on `stopped`.
    #[test]
    fn journaled_rejections_hello_stop_and_cost_follow_the_contract() {
        let mut rig = make_rig(1, None);
        let _ = rig.peer.take();
        let hello = json!({
            "type": "hello", "protocol": "acp/v1", "runner_id": "r-1",
            "harnesses": ["runner"], "capabilities": [NEXT_STEP_STEER],
        });
        rig.client.on_peer(&hello.to_string()).unwrap();
        let reply = rig.peer.take();
        assert_eq!(reply[0]["type"], "tunnel_ready");
        assert_eq!(reply[0]["protocol"], "acp/v1");
        rig.client
            .on_peer(&ready(1, 1, &[NEXT_STEP_STEER]))
            .unwrap();
        let _ = rig.host.take();

        rig.client.on_host(&user("a")).unwrap();
        let req_a = rig.peer.take()[0]["request_id"]
            .as_str()
            .unwrap()
            .to_string();
        let journaled_reject = frame(
            2,
            1,
            Some(&req_a),
            json!({"type": "rejected", "code": "turn_closed", "error": null}),
        );
        rig.client.on_peer(&journaled_reject).unwrap();
        assert_eq!(rig.host.take()[0]["state"], "cancelled");

        rig.client.on_host(&user("b")).unwrap();
        let req_b = rig.peer.take()[0]["request_id"]
            .as_str()
            .unwrap()
            .to_string();
        rig.client
            .on_peer(&frame(
                3,
                1,
                Some(&req_b),
                json!({"type": "accepted", "turn_id": "t1", "mode": "turn"}),
            ))
            .unwrap();
        rig.client
            .on_peer(&frame(4, 1, None, json!({"type": "host_tool_call", "call_id": "c1", "tool_call_id": "tc1", "name": "bash", "input": {}})))
            .unwrap();
        let tool_res = rig.peer.take();
        assert_eq!(tool_res[0]["type"], "host_tool_result");
        assert_eq!(tool_res[0]["call_id"], "c1");
        assert_eq!(tool_res[0]["is_error"], true);

        rig.client
            .on_host(r#"{"type":"control_request","request":{"subtype":"interrupt"}}"#)
            .unwrap();
        assert_eq!(rig.peer.take()[0]["type"], "interrupt");
        rig.client
            .on_peer(&frame(
                5,
                1,
                None,
                json!({"type": "interrupt_ack", "turn_id": "t1", "status": "cancelling"}),
            ))
            .unwrap();
        rig.client
            .on_peer(&frame(
                6,
                1,
                None,
                json!({"type": "result", "turn_id": "t1", "status": "completed",
                "is_error": false, "error": null, "duplicate": false, "usage": null,
                "cost_micros": 25_000, "context": null, "undelivered_ids": []}),
            ))
            .unwrap();
        let out = rig.host.take();
        assert_eq!(out.last().unwrap()["total_cost_usd"], json!(0.025));

        rig.client
            .on_host(r#"{"type":"control_request","request":{"subtype":"stop"}}"#)
            .unwrap();
        assert_eq!(rig.peer.take()[0]["type"], "stop");
        assert_eq!(
            rig.client
                .on_peer(&frame(7, 1, None, json!({"type": "stopped"}))),
            Ok(Flow::Done)
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_socket_transport_advances_lease_epoch_and_replays_after_seq() {
        use std::io::{BufReader, Cursor};
        use std::os::unix::net::UnixListener;

        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("acp.sock");
        let cfg = dir.path().join("cfg");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(
            cfg.join("cloud.json"),
            json!({"acp": {"socket": sock.display().to_string(), "harness": "claude"}}).to_string(),
        )
        .unwrap();
        let listener = UnixListener::bind(&sock).unwrap();
        let server = std::thread::spawn(move || {
            let mut seen_attaches = Vec::new();
            for run in 0..2u64 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let attach: Value = serde_json::from_str(line.trim()).unwrap();
                let sid = attach["session_id"].as_str().unwrap().to_string();
                let epoch = attach["lease_epoch"].as_u64().unwrap();
                let req = attach["request_id"].as_str().unwrap().to_string();
                seen_attaches.push(attach);
                let base = run * 3;
                let rdy = json!({
                    "seq": base + 1, "session_id": sid, "lease_epoch": epoch, "request_id": req,
                    "type": "ready", "protocol": "acp/v1", "protocols": ["acp/v1"],
                    "harness": "claude", "version": "1.0.0", "capabilities": [NEXT_STEP_STEER],
                });
                writeln!(stream, "{rdy}").unwrap();
                stream.flush().unwrap();
                line.clear();
                reader.read_line(&mut line).unwrap();
                let deliver: Value = serde_json::from_str(line.trim()).unwrap();
                let dreq = deliver["request_id"].as_str().unwrap().to_string();
                let acc = json!({
                    "seq": base + 2, "session_id": sid, "lease_epoch": epoch, "request_id": dreq,
                    "type": "accepted", "turn_id": format!("t{run}"), "mode": "turn",
                });
                let res = json!({
                    "seq": base + 3, "session_id": sid, "lease_epoch": epoch, "request_id": null,
                    "type": "result", "turn_id": format!("t{run}"), "status": "completed",
                    "is_error": false, "error": null, "duplicate": false, "usage": null,
                    "cost_micros": null, "context": null, "undelivered_ids": [],
                });
                writeln!(stream, "{acc}").unwrap();
                writeln!(stream, "{res}").unwrap();
                stream.flush().unwrap();
                line.clear();
                // Local adapter disconnect leaves the session alive (no `stop`).
                assert_eq!(
                    reader.read_line(&mut line).unwrap(),
                    0,
                    "unexpected trailing frame: {line}"
                );
            }
            seen_attaches
        });

        for id in ["m1", "m2"] {
            let args = super::super::CloudHarnessArgs {
                config: Some(cfg.clone()),
                harness: Some("claude-cloud".into()),
                model: Some("sonnet".into()),
                resume: None,
            };
            let out = Buf::default();
            super::super::run_with_io(args, Cursor::new(format!("{}\n", user(id))), out).unwrap();
        }
        let attaches = server.join().unwrap();
        assert_eq!(attaches[0]["lease_epoch"], 1);
        assert_eq!(attaches[0]["after_seq"], Value::Null);
        assert_eq!(attaches[1]["session_id"], attaches[0]["session_id"]);
        assert_eq!(attaches[1]["lease_epoch"], 2);
        assert_eq!(attaches[1]["after_seq"], 3);
    }

    /// Every control/tunnel frame this client emits validates against the
    /// Draft 2020-12 `acp/v1` wire schema when Python `jsonschema` is installed.
    #[test]
    fn emitted_frames_match_the_acp_v1_json_schema() {
        let probe = std::process::Command::new("/usr/bin/python3")
            .args(["-c", "import jsonschema"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        if !probe.is_ok_and(|status| status.success()) {
            return;
        }
        let (host, peer) = (Buf::default(), Buf::default());
        let attach = Attach {
            session_id: SESSION.into(),
            lease_epoch: 2,
            after_seq: Some(5),
            harness: "claude".into(),
            model: Some("claude-sonnet".into()),
            instructions: Some("be concise".into()),
        };
        let mut client = Client::new(host, peer.clone(), attach, Box::new(|_| {}));
        client.send_attach().unwrap();
        let hello = json!({"type": "hello", "protocol": "acp/v1", "runner_id": "r", "harnesses": ["claude"], "capabilities": [NEXT_STEP_STEER]});
        client.on_peer(&hello.to_string()).unwrap();
        client.on_peer(&ready(6, 2, &[NEXT_STEP_STEER])).unwrap();
        client.on_host(&user("m1")).unwrap();
        let req = peer.take();
        let mut emitted = req;
        let acc_req = emitted.last().unwrap()["request_id"]
            .as_str()
            .unwrap()
            .to_string();
        client
            .on_peer(&frame(
                7,
                2,
                Some(&acc_req),
                json!({"type": "accepted", "turn_id": "t1", "mode": "turn"}),
            ))
            .unwrap();
        client
            .on_peer(&frame(8, 2, None, json!({"type": "host_tool_call", "call_id": "c1", "tool_call_id": "tc1", "name": "bash", "input": {}})))
            .unwrap();
        client
            .on_host(r#"{"type":"control_request","request":{"subtype":"interrupt"}}"#)
            .unwrap();
        client
            .on_host(r#"{"type":"control_request","request":{"subtype":"stop"}}"#)
            .unwrap();
        emitted.extend(peer.take());
        assert_eq!(emitted.len(), 6);

        let schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "oneOf": [{"$ref": "#/$defs/tunnel_ready"}, {"$ref": "#/$defs/control_frame"}],
            "$defs": {
                "protocol": {"const": "acp/v1"},
                "non_empty_id": {"type": "string", "minLength": 1, "maxLength": 256},
                "tool_name": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,64}$"},
                "lease_epoch": {"type": "integer", "minimum": 1},
                "tunnel_ready": {
                    "type": "object",
                    "required": ["type", "protocol", "connection_id"],
                    "properties": {
                        "type": {"const": "tunnel_ready"},
                        "protocol": {"$ref": "#/$defs/protocol"},
                        "connection_id": {"$ref": "#/$defs/non_empty_id"},
                        "max_frame_bytes": {"type": "integer", "minimum": 1024}
                    }
                },
                "host_tool_def": {
                    "type": "object",
                    "required": ["name", "description", "input_schema"],
                    "properties": {
                        "name": {"$ref": "#/$defs/tool_name"},
                        "description": {"type": "string"},
                        "input_schema": {"type": "object"}
                    }
                },
                "deliver_item": {
                    "type": "object",
                    "required": ["id", "text"],
                    "properties": {
                        "id": {"$ref": "#/$defs/non_empty_id"},
                        "text": {"type": "string", "minLength": 1},
                        "metadata": {"type": "object"}
                    }
                },
                "control_common": {
                    "type": "object",
                    "required": ["session_id", "lease_epoch", "request_id"],
                    "properties": {
                        "session_id": {"$ref": "#/$defs/non_empty_id"},
                        "lease_epoch": {"$ref": "#/$defs/lease_epoch"},
                        "request_id": {"$ref": "#/$defs/non_empty_id"}
                    }
                },
                "control_frame": {
                    "allOf": [{"$ref": "#/$defs/control_common"}],
                    "oneOf": [
                        {"$ref": "#/$defs/attach"},
                        {"$ref": "#/$defs/deliver"},
                        {"$ref": "#/$defs/interrupt"},
                        {"$ref": "#/$defs/host_tool_result"},
                        {"$ref": "#/$defs/stop"}
                    ]
                },
                "attach": {
                    "type": "object",
                    "required": ["type", "protocol", "after_seq"],
                    "properties": {
                        "type": {"const": "attach"},
                        "protocol": {"$ref": "#/$defs/protocol"},
                        "after_seq": {"type": ["integer", "null"], "minimum": 0},
                        "harness": {"$ref": "#/$defs/non_empty_id"},
                        "model": {"$ref": "#/$defs/non_empty_id"},
                        "cwd": {"type": "string", "minLength": 1},
                        "instructions": {"type": "string"},
                        "host_tools": {"type": "array", "items": {"$ref": "#/$defs/host_tool_def"}}
                    }
                },
                "deliver": {
                    "type": "object",
                    "required": ["type", "items"],
                    "properties": {
                        "type": {"const": "deliver"},
                        "mode": {"enum": ["auto", "turn", "steer"]},
                        "items": {"type": "array", "minItems": 1, "items": {"$ref": "#/$defs/deliver_item"}}
                    }
                },
                "interrupt": {
                    "type": "object",
                    "required": ["type"],
                    "properties": {"type": {"const": "interrupt"}, "reason": {"type": ["string", "null"]}}
                },
                "host_tool_result": {
                    "type": "object",
                    "required": ["type", "call_id", "output", "is_error"],
                    "properties": {
                        "type": {"const": "host_tool_result"},
                        "call_id": {"$ref": "#/$defs/non_empty_id"},
                        "output": {},
                        "is_error": {"type": "boolean"}
                    }
                },
                "stop": {
                    "type": "object",
                    "required": ["type"],
                    "properties": {"type": {"const": "stop"}}
                }
            }
        });
        let payload = json!({"schema": schema, "frames": emitted}).to_string();
        let mut child = std::process::Command::new("/usr/bin/python3")
            .args([
                "-c",
                "import json, jsonschema, sys; d = json.load(sys.stdin); v = jsonschema.Draft202012Validator(d['schema']); [v.validate(f) for f in d['frames']]",
            ])
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(payload.as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

#[cfg(all(test, unix))]
mod conformance {
    //! The delivery conformance suite (`anyagents_invariants::conformance`)
    //! against the cloud harness over a scripted `acp/v1` runner
    //! (`fakes/proto_acp.py`), as the Claude adapter supervises it.
    use std::io::BufReader;
    use std::path::{Path, PathBuf};

    use anyagents_harness_claude::{CLOUD_DELIVERY, ClaudeHarness};
    use anyagents_invariants::conformance::Fixture;

    /// The child's agent config directory.
    const CHILD: &str = "ANYAGENTS_TEST_CLOUD_ACP_CONFIG";

    /// This test binary run as `cloud-harness`: libtest takes none of the
    /// Claude adapter's flags, so a script drops them.
    fn program() -> &'static Path {
        static PROGRAM: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        PROGRAM.get_or_init(|| {
            use std::os::unix::fs::PermissionsExt as _;
            let dir = tempfile::Builder::new()
                .prefix("anyagents-cloud-acp-")
                .tempdir()
                .expect("program directory")
                .keep();
            let path = dir.join("cloud-harness.sh");
            let exe = std::env::current_exe().expect("test binary");
            std::fs::write(
                &path,
                format!(
                    "#!/bin/sh\nexec '{}' --exact cloud_harness::acp::conformance::cloud_harness_child --nocapture --format terse --test-threads=1\n",
                    exe.display()
                ),
            )
            .expect("program");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                .expect("program mode");
            path
        })
    }

    /// Runs only as the child `program` starts.
    #[test]
    fn cloud_harness_child() {
        let Some(config) = std::env::var_os(CHILD) else {
            return;
        };
        let args = super::super::CloudHarnessArgs {
            config: Some(config.into()),
            harness: Some("claude-cloud".into()),
            model: None,
            resume: None,
        };
        let code = match super::super::run_with_io(
            args,
            BufReader::new(std::io::stdin()),
            std::io::stdout(),
        ) {
            Ok(()) => 0,
            Err(err) => {
                eprintln!("{err}");
                1
            }
        };
        std::process::exit(code);
    }

    fn launch(fixture: &Fixture, session: Option<&str>) -> ClaudeHarness {
        let config = fixture.dir().join("cloud");
        std::fs::create_dir_all(&config).expect("config");
        let cloud = serde_json::json!({"acp": {"command": [fixture.program()]}});
        std::fs::write(config.join("cloud.json"), cloud.to_string()).expect("cloud.json");
        let config = config.display().to_string();
        ClaudeHarness::start_in_with_args(
            program(),
            &[],
            session,
            &[(CHILD, config.as_str())],
            Some(fixture.dir()),
        )
        .expect("cloud harness starts")
        .with_delivery(CLOUD_DELIVERY)
    }

    mod next_step {
        use anyagents_harness::{DeliveryLevel, Harness};
        use anyagents_harness_claude::ClaudeHarness;
        use anyagents_invariants::conformance::{Fixture, Subject, World, ids};

        pub struct CloudAcp;

        impl Subject for CloudAcp {
            type Harness = ClaudeHarness;
            const HARNESS: &'static str = "claude-cloud";
            const PROTOCOL: &'static str = "acp";

            fn launch(fixture: &Fixture, session: Option<&str>) -> ClaudeHarness {
                super::launch(fixture, session)
            }

            fn session(harness: &ClaudeHarness) -> Option<String> {
                harness.session_id()
            }
        }

        anyagents_invariants::conformance_suite!(CloudAcp);

        #[test]
        fn a_peer_with_next_step_steer_is_held_to_next_step() {
            let mut world = World::<CloudAcp>::new();
            world.prompt();
            let delivery = world.delivery_level();
            assert_eq!(delivery.level, DeliveryLevel::NextStep);
            assert!(delivery.idempotent);
            assert_eq!(delivery.peer.as_deref(), Some("acp/v1 claude-cloud 1.0.0"));
        }

        #[test]
        fn refused_steer_ahead_of_fresh_mail_redelivers_in_fifo_order() {
            let mut world = World::<CloudAcp>::new();
            let first = world.prompt();
            world.fixture.command("refuse");
            let queued = world.receive(2);
            world.drain(queued[1].seq);
            assert_eq!(
                ids(&world.fixture.journal()),
                vec![
                    first.id.to_string(),
                    queued[0].id.to_string(),
                    queued[1].id.to_string(),
                ]
            );
        }

        #[test]
        fn cancelling_a_turn_with_queued_and_fresh_mail_redelivers_in_fifo_order() {
            let mut world = World::<CloudAcp>::new();
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
        }
    }

    mod turn_end {
        use anyagents_harness::DeliveryLevel;
        use anyagents_harness_claude::ClaudeHarness;
        use anyagents_invariants::conformance::{Fixture, Subject, World};

        pub struct CloudAcpTurnEnd;

        impl Subject for CloudAcpTurnEnd {
            type Harness = ClaudeHarness;
            const HARNESS: &'static str = "claude-cloud";
            const PROTOCOL: &'static str = "acp-turn-end";

            fn launch(fixture: &Fixture, session: Option<&str>) -> ClaudeHarness {
                super::launch(fixture, session)
            }

            fn session(harness: &ClaudeHarness) -> Option<String> {
                harness.session_id()
            }
        }

        anyagents_invariants::conformance_suite!(CloudAcpTurnEnd);

        #[test]
        fn a_peer_without_next_step_steer_stays_at_turn_end() {
            let mut world = World::<CloudAcpTurnEnd>::new();
            world.prompt();
            assert_eq!(world.delivery_level().level, DeliveryLevel::TurnEnd);
        }
    }
}
