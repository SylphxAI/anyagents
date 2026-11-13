//! A scripted `muse serve` for the adapter tests, speaking the parts of the
//! Muse JSON-RPC protocol the adapter uses: the handshake, `turn/start` with
//! `ifBusy: "steer"` answered by an admission ack, the `userMessage` item that
//! proves an input entered the conversation, the successor turn a steer starts
//! when its turn ended before consuming it, and the command table that replays
//! a command's first answer.
//!
//! - `ANYAGENTS_FAKE_MODE` picks the script: `hold` (default: a turn runs
//!   until `finish`, and asks one tool approval), `finish` (a turn completes
//!   as soon as it starts), `early_complete` (as `finish`, with
//!   `turn/completed` before `turn/started` and the answer), `quota` (every
//!   `turn/start` is answered 429), `refuse_steer` (the first steer into a
//!   running turn is rejected, -32030), `interrupt_once` (the first
//!   `turn/start` is answered -32013 and not applied), `unloaded` (every
//!   `turn/start` is answered -32024).
//! - `ANYAGENTS_FAKE_MUSE_DIR` holds the session's command table, so a second
//!   process on the session replays it, and takes the control files `step`
//!   (the running turn consumes its waiting steers) and `finish` (it ends).
//! - `ANYAGENTS_FAKE_LOG` records argv and every line received.
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
    running: Option<String>,
    /// Steers waiting in the running turn's inbox: (command id, text).
    inbox: Vec<(String, String)>,
    /// Every command's first answer, by command id.
    commands: BTreeMap<String, Value>,
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
    let commands = dir
        .as_deref()
        .and_then(|dir| std::fs::read_to_string(dir.join("commands.json")).ok())
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default();
    let fake = Arc::new(Mutex::new(Fake {
        mode: env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".into()),
        dir: dir.clone(),
        running: None,
        inbox: Vec::new(),
        commands,
        refused: false,
        interrupted: false,
    }));
    if let Some(dir) = dir {
        let fake = Arc::clone(&fake);
        std::thread::spawn(move || control(&dir, &fake));
    }
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &line);
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
        for name in ["step", "finish"] {
            let path = dir.join(name);
            if path.exists() {
                let mut fake = fake.lock().unwrap_or_else(|err| err.into_inner());
                if fake.running.is_some() {
                    if name == "step" {
                        fake.step();
                    } else {
                        fake.finish();
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
            "initialize" => answer(
                &id,
                json!({"serverInfo": "1.4.18-test", "userAgent": "muse/1.4.18-test",
                       "schema": {"version": 1, "fingerprint": "test"}}),
            ),
            "session/start" => answer(&id, json!({"session": {"sessionId": SESSION}})),
            "session/resume" => {
                let session = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or(SESSION);
                if env::var_os("ANYAGENTS_FAKE_MUSE_LOCK_FILE")
                    .is_some_and(|lock| Path::new(&lock).exists())
                {
                    fail(
                        &id,
                        -32021,
                        &format!("session {session} is already in use"),
                        json!({"kind": "writerLeaseHeld"}),
                    );
                } else {
                    answer(&id, json!({"session": {"sessionId": session}}));
                }
            }
            "session/setApprovalMode" => answer(
                &id,
                json!({"commandId": params["commandId"], "status": "accepted"}),
            ),
            "turn/start" => self.turn_start(&id, &params),
            _ if !id.is_null() && value.get("method").is_some() => {
                fail(&id, -32601, "method not found", Value::Null)
            }
            // The host's answer to an approval request: the turn carries on.
            _ => {}
        }
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
        if let Some(known) = self.commands.get(&command) {
            if known["fingerprint"] != fingerprint.as_str() {
                fail(
                    id,
                    -32030,
                    "commandId reused with another payload",
                    json!({"kind": "commandRejected", "retryable": false, "commandId": command}),
                );
            } else if let Some(error) = known.get("error") {
                emit(&json!({"jsonrpc": "2.0", "id": id, "error": error}));
            } else {
                answer(id, known["ack"].clone());
            }
            return;
        }
        match self.mode.as_str() {
            "quota" => {
                return fail(id, 429, "rate limited", json!({"resetsAt": 1_700_000_000}));
            }
            "unloaded" => {
                return fail(
                    id,
                    -32024,
                    "session is not loaded",
                    json!({"kind": "sessionNotLoaded", "retryable": false}),
                );
            }
            "interrupt_once" if !self.interrupted => {
                self.interrupted = true;
                return fail(
                    id,
                    -32013,
                    "interrupted; the outcome is on the view",
                    json!({"kind": "interrupted", "retryable": true, "commandId": command}),
                );
            }
            _ => {}
        }
        if let Some(turn) = self.running.clone()
            && params["ifBusy"] == "steer"
        {
            if self.mode == "refuse_steer" && !self.refused {
                self.refused = true;
                let error = json!({"code": -32030, "message": "the running turn refused the steer",
                    "data": {"kind": "commandRejected", "retryable": true, "commandId": command}});
                self.record(&command, &fingerprint, json!({"error": error}));
                emit(&json!({"jsonrpc": "2.0", "id": id, "error": error}));
                return;
            }
            let ack = ack(&command, "steered", &turn, false);
            self.record(&command, &fingerprint, json!({"ack": ack}));
            self.inbox.push((command, text));
            answer(id, ack);
            return;
        }
        let ack = ack(&command, "started", &command, true);
        self.record(&command, &fingerprint, json!({"ack": ack}));
        let turn = command.clone();
        if self.mode == "early_complete" {
            user_message(&turn, &command, &text, false);
            turn_completed(&turn);
            turn_started(&turn);
            answer(id, ack);
            return;
        }
        answer(id, ack);
        turn_started(&turn);
        user_message(&turn, &command, &text, false);
        if self.mode == "finish" {
            turn_completed(&turn);
            return;
        }
        self.running = Some(turn);
        if self.mode == "hold" {
            emit(
                &json!({"jsonrpc": "2.0", "id": 900, "method": "approval/request",
                "params": {"approvalId": "a1", "toolName": "shell",
                    "availableChoices": [{"choiceId": "c1", "decision": "approved"}]}}),
            );
        }
    }

    /// The running turn's next step: every waiting steer, oldest first.
    fn step(&mut self) {
        let Some(turn) = self.running.clone() else {
            return;
        };
        for (command, text) in std::mem::take(&mut self.inbox) {
            user_message(&turn, &command, &text, true);
        }
    }

    /// The running turn ends; a steer it never consumed starts a successor
    /// named after the oldest one's command, which consumes them all.
    fn finish(&mut self) {
        let Some(turn) = self.running.take() else {
            return;
        };
        turn_completed(&turn);
        let waiting = std::mem::take(&mut self.inbox);
        let Some((first, _)) = waiting.first() else {
            return;
        };
        let successor = first.clone();
        turn_started(&successor);
        for (command, text) in waiting {
            user_message(&successor, &command, &text, true);
        }
        self.running = Some(successor);
    }

    fn record(&mut self, command: &str, fingerprint: &str, mut answer: Value) {
        answer["fingerprint"] = json!(fingerprint);
        self.commands.insert(command.to_string(), answer);
        if let Some(dir) = &self.dir {
            let _ = std::fs::write(
                dir.join("commands.json"),
                serde_json::to_string(&self.commands).unwrap_or_default(),
            );
        }
    }
}

fn ack(command: &str, disposition: &str, turn: &str, started: bool) -> Value {
    let queued = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0);
    json!({"commandId": command, "status": "accepted", "disposition": disposition,
        "turnId": turn, "startedNewTurn": started, "queuedAtUs": queued})
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

fn user_message(turn: &str, command: &str, text: &str, steered: bool) {
    let mut item = json!({"itemId": format!("item-{command}"), "kind": "userMessage",
        "turnId": turn, "commandId": command, "revision": 1, "status": "completed",
        "text": text});
    if steered {
        item["steered"] = json!(true);
    }
    notify(
        "item/completed",
        json!({"sessionId": SESSION, "item": item}),
    );
}

fn notify(method: &str, params: Value) {
    emit(&json!({"jsonrpc": "2.0", "method": method, "params": params}));
}

fn answer(id: &Value, result: Value) {
    emit(&json!({"jsonrpc": "2.0", "id": id, "result": result}));
}

fn fail(id: &Value, code: i64, message: &str, data: Value) {
    emit(&json!({"jsonrpc": "2.0", "id": id,
        "error": {"code": code, "message": message, "data": data}}));
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
