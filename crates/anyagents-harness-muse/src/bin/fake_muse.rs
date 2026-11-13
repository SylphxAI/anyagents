//! A scripted `muse serve` for the adapter tests, speaking the parts of the
//! Muse JSON-RPC protocol the adapter uses: the handshake, `turn/start` with
//! `ifBusy: "steer"` answered by an admission ack, the `userMessage` item that
//! proves an input entered the conversation, the successor turn a steer starts
//! when its turn ended before consuming it, the command table that replays a
//! command's recorded decision, and the session record a resume serves.
//!
//! What it holds to, as Muse 1.4.20 documents it:
//! - An ack carries `queuedAtUs` only on a fresh `queued` or `steered`
//!   admission, and only to a connection that opted into `experimentalApi`;
//!   never on `started`, and never on the replay of a command an earlier
//!   process admitted.
//! - A command id reused with another payload is refused `-32030` with
//!   reason `command_id_conflict`; any other `-32030` is recorded against
//!   the id, and the same id answers it again.
//! - `-32031` (the command-admission queue is full) is refused before
//!   admission and records nothing.
//! - A resume without `excludeItems: true` serves the session's items as its
//!   history (`inline`), or none when they do not fit the budget, and then
//!   `view/page` pages them.
//!
//! - `ANYAGENTS_FAKE_MODE` picks the script: `hold` (default: a turn runs
//!   until `finish`, and asks one tool approval), `finish` (a turn completes
//!   as soon as it starts), `early_complete` (as `finish`, with
//!   `turn/completed` before `turn/started` and the answer), `quota` (every
//!   `turn/start` is answered 429), `refuse_steer` (the first steer into a
//!   running turn is refused, -32031), `interrupt_once` (the first
//!   `turn/start` is answered -32013 and not applied), `unloaded` (every
//!   `turn/start` is answered -32024), `too_large` (-32002), `invalid_params`
//!   (-32602), `rejected` (-32030 `runtime_busy`, recorded).
//! - `ANYAGENTS_FAKE_MUSE_HISTORY=paged`: a resume serves no history, as when
//!   it is over the budget; `view/page` serves it.
//! - `ANYAGENTS_FAKE_MUSE_DIR` holds the session's durable state (command
//!   table, items, waiting steers), so a second process on the session
//!   replays and serves it, and takes the control files `step` (the running
//!   turn consumes its waiting steers), `finish` (it ends), `carry` (steers
//!   an earlier process left waiting run in a successor turn) and `exit`.
//! - `ANYAGENTS_FAKE_LOG` records argv, every line received (`in:`) and every
//!   answer printed (`out:`).
//! - While `ANYAGENTS_FAKE_MUSE_LOCK_FILE` exists, `session/resume` answers
//!   that the session is in use (-32021).

use std::collections::BTreeMap;
use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

const SESSION: &str = "sess-from-vendor";
const USAGE: &str = r#"{"inputTokens":11,"outputTokens":7,"cachedInputTokens":3}"#;

struct Fake {
    mode: String,
    dir: Option<PathBuf>,
    log: Option<PathBuf>,
    /// The connection opted into `experimentalApi`.
    experimental: bool,
    running: Option<String>,
    /// Steers waiting in the running turn's inbox: (command id, text).
    inbox: Vec<(String, String)>,
    /// Steers an earlier process left waiting, run on `carry`.
    leftover: Vec<(String, String)>,
    /// Every command's recorded decision, by command id; `earlier` marks one
    /// an earlier process admitted.
    commands: BTreeMap<String, Value>,
    /// Every `userMessage` item of the session, in order.
    items: Vec<Value>,
    refused: bool,
    interrupted: bool,
}

fn main() {
    let log_path = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from);
    log(
        &log_path,
        &format!("argv:{}", env::args().skip(1).collect::<Vec<_>>().join(" ")),
    );
    let dir = env::var_os("ANYAGENTS_FAKE_MUSE_DIR").map(PathBuf::from);
    let read = |name: &str| -> Option<Value> {
        let text = std::fs::read_to_string(dir.as_deref()?.join(name)).ok()?;
        serde_json::from_str(&text).ok()
    };
    let mut commands: BTreeMap<String, Value> = read("commands.json")
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    for answer in commands.values_mut() {
        answer["earlier"] = json!(true);
    }
    let items = read("items.json")
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    let leftover = read("inbox.json")
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    let fake = Arc::new(Mutex::new(Fake {
        mode: env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".into()),
        dir: dir.clone(),
        log: log_path.clone(),
        experimental: false,
        running: None,
        inbox: Vec::new(),
        leftover,
        commands,
        items,
        refused: false,
        interrupted: false,
    }));
    if let Some(dir) = dir {
        let fake = Arc::clone(&fake);
        std::thread::spawn(move || control(&dir, &fake));
    }
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &format!("in:{line}"));
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let mut fake = fake.lock().unwrap_or_else(|err| err.into_inner());
        fake.on_line(&value);
    }
}

/// Act on the test's control files.
fn control(dir: &Path, fake: &Mutex<Fake>) {
    loop {
        for name in ["step", "finish", "carry", "exit"] {
            let path = dir.join(name);
            if path.exists() {
                let mut fake = fake.lock().unwrap_or_else(|err| err.into_inner());
                match name {
                    "step" => fake.step(),
                    "finish" => fake.finish(),
                    "carry" => fake.carry(),
                    _ => {
                        let _ = std::fs::remove_file(path);
                        std::process::exit(0);
                    }
                }
                let _ = std::fs::remove_file(path);
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

impl Fake {
    fn on_line(&mut self, value: &Value) {
        let method = value.get("method").and_then(Value::as_str).unwrap_or("");
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        let params = value.get("params").cloned().unwrap_or_default();
        match method {
            "initialize" => {
                self.experimental = params
                    .pointer("/capabilities/experimentalApi")
                    .and_then(Value::as_bool)
                    == Some(true);
                self.answer(
                    &id,
                    json!({"serverInfo": "1.4.20-test", "userAgent": "muse/1.4.20-test",
                       "schema": {"version": 1, "fingerprint": "test"}}),
                );
            }
            "session/start" => {
                self.answer(&id, json!({"session": {"sessionId": SESSION}}));
            }
            "session/resume" => {
                let session = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or(SESSION);
                if env::var_os("ANYAGENTS_FAKE_MUSE_LOCK_FILE")
                    .is_some_and(|lock| Path::new(&lock).exists())
                {
                    self.fail(
                        &id,
                        -32021,
                        &format!("session {session} is already in use"),
                        json!({"kind": "writerLeaseHeld"}),
                    );
                    return;
                }
                let history = if params["excludeItems"] == true {
                    json!({"mode": "none", "items": null, "snapshot": null,
                        "noneReason": "excludeItems"})
                } else if env::var("ANYAGENTS_FAKE_MUSE_HISTORY").as_deref() == Ok("paged") {
                    json!({"mode": "none", "items": null, "snapshot": null,
                        "noneReason": "budgetExceeded"})
                } else {
                    json!({"mode": "inline", "items": self.items, "snapshot": null})
                };
                self.answer(
                    &id,
                    json!({"session": {"sessionId": session}, "viewCursor": "head",
                        "history": history}),
                );
            }
            "view/page" => self.page(&id, &params),
            "session/setApprovalMode" => self.answer(
                &id,
                json!({"commandId": params["commandId"], "status": "accepted"}),
            ),
            "turn/start" => self.turn_start(&id, &params),
            _ if !id.is_null() && value.get("method").is_some() => {
                self.fail(&id, -32601, "method not found", Value::Null)
            }
            // The host's answer to an approval request: the turn carries on.
            _ => {}
        }
    }

    /// Forward pages of the session's items, two at a time; the anchor is
    /// refused when no compaction boundary exists.
    fn page(&mut self, id: &Value, params: &Value) {
        if params.get("anchor").is_some() {
            return self.fail(
                id,
                -32042,
                "no installed boundary",
                json!({"kind": "boundaryAbsent"}),
            );
        }
        let start: usize = params["cursor"]
            .as_str()
            .and_then(|cursor| cursor.parse().ok())
            .unwrap_or(0);
        let end = (start + 2).min(self.items.len());
        let events: Vec<Value> = self.items[start..end]
            .iter()
            .map(|item| json!({"method": "item/completed", "params": {"item": item}}))
            .collect();
        let next = if end < self.items.len() {
            json!(end.to_string())
        } else {
            Value::Null
        };
        self.answer(id, json!({"events": events, "nextCursor": next}));
    }

    fn turn_start(&mut self, id: &Value, params: &Value) {
        let command = params["commandId"].as_str().unwrap_or_default().to_string();
        let text = params["input"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let mut payload = params.clone();
        if let Some(map) = payload.as_object_mut() {
            map.remove("commandId");
        }
        let fingerprint = payload.to_string();
        if let Some(known) = self.commands.get(&command).cloned() {
            if known["fingerprint"] != fingerprint.as_str() {
                self.fail(
                    id,
                    -32030,
                    "command id replayed with a different payload",
                    json!({"kind": "commandRejected", "retryable": false,
                        "commandId": command, "reason": "command_id_conflict"}),
                );
            } else if let Some(error) = known.get("error") {
                self.emit(&json!({"jsonrpc": "2.0", "id": id, "error": error}));
            } else {
                // A replay after a restart drops `queuedAtUs`.
                let mut ack = known["ack"].clone();
                if known["earlier"] == true
                    && let Some(map) = ack.as_object_mut()
                {
                    map.remove("queuedAtUs");
                }
                self.answer(id, ack);
            }
            return;
        }
        let refusal = match self.mode.as_str() {
            "quota" => Some((429, "rate limited", json!({"resetsAt": 1_700_000_000}))),
            "unloaded" => Some((
                -32024,
                "session is not loaded",
                json!({"kind": "sessionNotLoaded", "retryable": false}),
            )),
            "too_large" => Some((
                -32002,
                "input too large",
                json!({"kind": "inputTooLarge", "limitBytes": 1024}),
            )),
            "invalid_params" => Some((-32602, "invalid params", json!({"kind": "invalidParams"}))),
            "interrupt_once" if !self.interrupted => {
                self.interrupted = true;
                Some((
                    -32013,
                    "interrupted; the outcome is on the view",
                    json!({"kind": "interrupted", "retryable": true, "commandId": command}),
                ))
            }
            _ => None,
        };
        if let Some((code, message, data)) = refusal {
            return self.fail(id, code, message, data);
        }
        if self.mode == "rejected" {
            let error = json!({"code": -32030, "message": "command rejected",
                "data": {"kind": "commandRejected", "commandId": command,
                    "reason": "runtime_busy"}});
            self.record(&command, &fingerprint, json!({"error": error}));
            self.emit(&json!({"jsonrpc": "2.0", "id": id, "error": error}));
            return;
        }
        if let Some(turn) = self.running.clone()
            && params["ifBusy"] == "steer"
        {
            if self.mode == "refuse_steer" && !self.refused {
                // Refused before admission: nothing is recorded.
                self.refused = true;
                return self.fail(
                    id,
                    -32031,
                    "the command-admission queue is full",
                    json!({"kind": "backpressured", "retryable": true}),
                );
            }
            let ack = self.ack(&command, "steered", &turn);
            self.record(&command, &fingerprint, json!({"ack": ack}));
            self.inbox.push((command, text));
            self.save_inbox();
            self.answer(id, ack);
            return;
        }
        let ack = self.ack(&command, "started", &command);
        self.record(&command, &fingerprint, json!({"ack": ack}));
        let turn = command.clone();
        if self.mode == "early_complete" {
            self.user_message(&turn, &command, &text, false);
            turn_completed(&turn);
            turn_started(&turn);
            self.answer(id, ack);
            return;
        }
        self.answer(id, ack);
        turn_started(&turn);
        self.user_message(&turn, &command, &text, false);
        if self.mode == "finish" {
            turn_completed(&turn);
            return;
        }
        self.running = Some(turn);
        if self.mode == "hold" {
            self.emit(
                &json!({"jsonrpc": "2.0", "id": 900, "method": "approval/request",
                "params": {"approvalId": "a1", "toolName": "shell",
                    "availableChoices": [{"choiceId": "c1", "decision": "approved"}]}}),
            );
        }
    }

    fn ack(&self, command: &str, disposition: &str, turn: &str) -> Value {
        let mut ack = json!({"commandId": command, "status": "accepted",
            "disposition": disposition, "turnId": turn,
            "startedNewTurn": disposition == "started"});
        if disposition != "started" && self.experimental {
            let queued = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| elapsed.as_micros() as u64)
                .unwrap_or(0);
            ack["queuedAtUs"] = json!(queued);
        }
        ack
    }

    /// The running turn's next step: every waiting steer, oldest first.
    fn step(&mut self) {
        let Some(turn) = self.running.clone() else {
            return;
        };
        for (command, text) in std::mem::take(&mut self.inbox) {
            self.user_message(&turn, &command, &text, true);
        }
        self.save_inbox();
    }

    /// The running turn ends; a steer it never consumed starts a successor
    /// named after the oldest one's command, which consumes them all.
    fn finish(&mut self) {
        let Some(turn) = self.running.take() else {
            return;
        };
        turn_completed(&turn);
        let waiting = std::mem::take(&mut self.inbox);
        self.successor(waiting);
    }

    /// Steers an earlier process left waiting run in a successor turn.
    fn carry(&mut self) {
        if self.running.is_none() {
            let waiting = std::mem::take(&mut self.leftover);
            self.successor(waiting);
        }
    }

    fn successor(&mut self, waiting: Vec<(String, String)>) {
        let Some((first, _)) = waiting.first() else {
            self.save_inbox();
            return;
        };
        let successor = first.clone();
        turn_started(&successor);
        for (command, text) in waiting {
            self.user_message(&successor, &command, &text, true);
        }
        self.running = Some(successor);
        self.save_inbox();
    }

    /// One input enters the conversation: recorded, then announced.
    fn user_message(&mut self, turn: &str, command: &str, text: &str, steered: bool) {
        let mut item = json!({"itemId": format!("item-{command}"), "kind": "userMessage",
            "turnId": turn, "commandId": command, "revision": 1, "status": "completed",
            "text": text});
        if steered {
            item["steered"] = json!(true);
        }
        self.items.push(item.clone());
        self.store("items.json", json!(self.items));
        notify(
            "item/completed",
            json!({"sessionId": SESSION, "item": item}),
        );
    }

    fn save_inbox(&self) {
        let mut waiting = self.inbox.clone();
        waiting.extend(self.leftover.iter().cloned());
        self.store("inbox.json", json!(waiting));
    }

    fn record(&mut self, command: &str, fingerprint: &str, mut answer: Value) {
        answer["fingerprint"] = json!(fingerprint);
        self.commands.insert(command.to_string(), answer);
        self.store("commands.json", json!(self.commands));
    }

    fn store(&self, name: &str, value: Value) {
        if let Some(dir) = &self.dir {
            let _ = std::fs::write(dir.join(name), value.to_string());
        }
    }

    fn answer(&self, id: &Value, result: Value) {
        self.emit(&json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }

    fn fail(&self, id: &Value, code: i64, message: &str, data: Value) {
        self.emit(&json!({"jsonrpc": "2.0", "id": id,
            "error": {"code": code, "message": message, "data": data}}));
    }

    fn emit(&self, line: &Value) {
        log(&self.log, &format!("out:{line}"));
        emit(line);
    }
}

fn turn_started(turn: &str) {
    notify(
        "turn/started",
        json!({"sessionId": SESSION, "turnId": turn, "commandId": turn}),
    );
}

fn turn_completed(turn: &str) {
    let usage: Value = serde_json::from_str(USAGE).unwrap_or_default();
    notify(
        "turn/completed",
        json!({"sessionId": SESSION, "turnId": turn, "terminal": "completed", "usage": usage}),
    );
}

fn notify(method: &str, params: Value) {
    emit(&json!({"jsonrpc": "2.0", "method": method, "params": params}));
}

fn emit(line: &Value) {
    let mut stdout = io::stdout().lock();
    let _ = writeln!(stdout, "{line}");
    let _ = stdout.flush();
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}
