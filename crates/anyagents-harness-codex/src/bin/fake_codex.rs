//! Speaks just enough of `codex app-server` (newline-delimited JSON-RPC) for
//! the adapter tests.
//!
//! A turn's prompt enters the conversation as soon as the turn starts: the
//! server answers `turn/start`, announces `turn/started`, and records the
//! prompt as a `userMessage` item carrying `clientUserMessageId` as
//! `clientId`. A steer is answered `{turnId}` and, unless the mode says
//! otherwise, taken in at once the same way. `thread/items/list` pages
//! through every `userMessage` recorded, newest first when asked.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script:
//! - `hold`: turns stay open; steers are taken in
//! - `steer`: a steer is taken in, then its turn completes
//! - `finish`: every turn completes at once
//! - `refuse-steer`: a steer is refused (`cannot steer a compact turn`)
//! - `steer-unknown-applied`: a steer reaches history without an item event,
//!   its answer is an internal error, and the turn completes
//! - `steer-unknown-lost`: a steer's answer is an internal error, nothing is
//!   applied, and the turn completes
//! - `steer-silent`: a steer is answered, reaches history without an item
//!   event (it raced the turn's last step), and the turn completes
//! - `quota`: a rate-limit notice arrives with the thread
//! - `rpc-quota`: `turn/start` is refused for quota
//! - `401`, `turn-error`: a turn fails with that error
//! - `resume-missing`: `thread/resume` answers that no rollout is recorded
//!   under the id (`-32600 no rollout found for thread id X`)
//! - `resume-unreadable`: `thread/resume` answers that the rollout cannot be
//!   located (`-32600 failed to locate thread id X: ...`)
//! - `die`: the process exits at once
//!
//! In every mode, as Codex does, an input longer than 1,048,576 characters is
//! refused with `-32602 Input exceeds the maximum length of 1048576
//! characters.` and nothing is recorded.
//!
//! `ANYAGENTS_FAKE_LOG` receives argv and stdin, and `proved:<id>` once the
//! items proving an input are written. `ANYAGENTS_FAKE_HISTORY` names a file
//! of `{"turnId", "clientId", "text"}` lines the thread already holds.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use serde_json::{Value, json};

const THREAD: &str = "thread-1";

/// Codex's limit on one input's text, in characters.
const MAX_INPUT_CHARS: usize = 1 << 20;

struct Fake {
    mode: String,
    log: Option<PathBuf>,
    out: io::Stdout,
    turn: Option<String>,
    turns: u32,
    /// Every `userMessage` the thread holds, oldest first.
    history: Vec<Value>,
}

fn main() {
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".to_string());
    let log_path = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from);
    let argv = env::args().skip(1).collect::<Vec<_>>().join(" ");
    log(&log_path, &format!("argv:{argv}"));
    if mode == "die" {
        return;
    }
    let history = env::var_os("ANYAGENTS_FAKE_HISTORY")
        .and_then(|path| std::fs::read_to_string(path).ok())
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .map(|row| {
            entry(
                row["turnId"].as_str().unwrap_or_default(),
                &row["clientId"],
                row["text"].as_str().unwrap_or_default(),
            )
        })
        .collect();
    let mut fake = Fake {
        mode,
        log: log_path.clone(),
        out: io::stdout(),
        turn: None,
        turns: 0,
        history,
    };
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &line);
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        fake.handle(&value);
    }
}

impl Fake {
    fn handle(&mut self, value: &Value) {
        let method = value.get("method").and_then(Value::as_str).unwrap_or("");
        let Some(id) = value.get("id").cloned() else {
            return;
        };
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "initialize" => self.reply(
                &id,
                json!({
                    "userAgent": "codex_cli_rs/0.169.0 (Linux; fake)",
                    "codexHome": "",
                    "platformFamily": "unix",
                    "platformOs": "linux"
                }),
            ),
            "thread/resume" if self.mode == "resume-missing" => {
                let thread = params["threadId"].as_str().unwrap_or_default();
                self.error(
                    &id,
                    json!({"code": -32600, "message": format!("no rollout found for thread id {thread}")}),
                );
            }
            "thread/resume" if self.mode == "resume-unreadable" => {
                let thread = params["threadId"].as_str().unwrap_or_default();
                self.error(
                    &id,
                    json!({
                        "code": -32600,
                        "message": format!("failed to locate thread id {thread}: Permission denied (os error 13)")
                    }),
                );
            }
            "thread/start" | "thread/resume" => {
                if self.mode == "quota" {
                    self.note(
                        "account/rateLimits/updated",
                        json!({
                            "rateLimits": {
                                "rateLimitReachedType": "rate_limit_reached",
                                "primary": {"usedPercent": 100, "resetsAt": 1_700_000_000_000_i64}
                            }
                        }),
                    );
                }
                let thread = params["threadId"].as_str().unwrap_or(THREAD);
                self.reply(&id, json!({"thread": {"id": thread}}));
            }
            "thread/items/list" => self.items(&id, &params),
            "turn/start" if self.mode == "rpc-quota" => self.error(
                &id,
                json!({
                    "code": -32000,
                    "message": "usageLimitExceeded",
                    "data": {"resetsAt": 1_700_000_000_000_i64}
                }),
            ),
            "turn/start" | "turn/steer" if too_long(&params) => self.error(
                &id,
                json!({
                    "code": -32602,
                    "message": format!("Input exceeds the maximum length of {MAX_INPUT_CHARS} characters.")
                }),
            ),
            "turn/start" => self.start(&id, &params),
            "turn/steer" => self.steer(&id, &params),
            "turn/interrupt" => {
                self.reply(&id, json!({}));
                if let Some(turn) = self.turn.take() {
                    self.completed(&turn, "interrupted", Value::Null);
                }
            }
            _ => self.error(
                &id,
                json!({"code": -32601, "message": format!("method not found: {method}")}),
            ),
        }
    }

    fn start(&mut self, id: &Value, params: &Value) {
        if let Some(turn) = self.turn.clone() {
            // `turn/start` while a turn runs steers into it.
            self.reply(id, json!({"turn": turn_value(&turn, "inProgress")}));
            self.record(&turn, params, true);
            return;
        }
        self.turns += 1;
        let turn = format!("turn-{}", self.turns);
        self.turn = Some(turn.clone());
        self.reply(id, json!({"turn": turn_value(&turn, "inProgress")}));
        self.note(
            "turn/started",
            json!({"threadId": THREAD, "turn": turn_value(&turn, "inProgress")}),
        );
        self.record(&turn, params, true);
        match self.mode.as_str() {
            "finish" => self.complete(),
            "401" => self.fail("workspace routing discovery unauthorized (401)"),
            "turn-error" => self.fail("upstream model stream closed unexpectedly"),
            _ => {}
        }
    }

    fn steer(&mut self, id: &Value, params: &Value) {
        let Some(turn) = self.turn.clone() else {
            self.error(
                id,
                json!({"code": -32600, "message": "no active turn to steer"}),
            );
            return;
        };
        let expected = params["expectedTurnId"].as_str().unwrap_or_default();
        if expected != turn {
            self.error(
                id,
                json!({
                    "code": -32600,
                    "message": format!("expected active turn id `{expected}` but found `{turn}`")
                }),
            );
            return;
        }
        let mode = self.mode.clone();
        match mode.as_str() {
            "refuse-steer" => self.error(
                id,
                json!({
                    "code": -32600,
                    "message": "cannot steer a compact turn",
                    "data": {"codexErrorInfo": {"activeTurnNotSteerable": {"turnKind": "compact"}}}
                }),
            ),
            "steer-unknown-applied" => {
                self.record(&turn, params, false);
                self.error(
                    id,
                    json!({"code": -32603, "message": "failed to steer turn: channel closed"}),
                );
                self.complete();
            }
            "steer-unknown-lost" => {
                self.error(
                    id,
                    json!({"code": -32603, "message": "failed to steer turn: channel closed"}),
                );
                self.complete();
            }
            "steer-silent" => {
                self.reply(id, json!({"turnId": turn}));
                self.record(&turn, params, false);
                self.complete();
            }
            mode => {
                self.reply(id, json!({"turnId": turn}));
                self.record(&turn, params, true);
                match mode {
                    "steer" | "finish" => self.complete(),
                    "401" => self.fail("workspace routing discovery unauthorized (401)"),
                    "turn-error" => self.fail("upstream model stream closed unexpectedly"),
                    _ => {}
                }
            }
        }
    }

    /// The input in `params` enters the conversation of `turn`, announced as
    /// items unless it raced past them.
    fn record(&mut self, turn: &str, params: &Value, announce: bool) {
        let text: String = params["input"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|part| part["text"].as_str())
            .collect();
        let row = entry(turn, &params["clientUserMessageId"], &text);
        if announce {
            for (method, stamp) in [
                ("item/started", "startedAtMs"),
                ("item/completed", "completedAtMs"),
            ] {
                self.note(
                    method,
                    json!({"threadId": THREAD, "turnId": turn, "item": row["item"], stamp: 0}),
                );
            }
            if let Some(id) = params["clientUserMessageId"].as_str() {
                log(&self.log, &format!("proved:{id}"));
            }
        }
        self.history.push(row);
    }

    fn items(&mut self, id: &Value, params: &Value) {
        let mut entries: Vec<Value> = self
            .history
            .iter()
            .filter(|row| {
                params["turnId"]
                    .as_str()
                    .is_none_or(|turn| row["turnId"] == turn)
            })
            .cloned()
            .collect();
        if params["sortDirection"] == "desc" {
            entries.reverse();
        }
        let start = params["cursor"]
            .as_str()
            .and_then(|cursor| cursor.parse::<usize>().ok())
            .unwrap_or(0)
            .min(entries.len());
        let limit = params["limit"].as_u64().unwrap_or(100) as usize;
        let end = (start + limit).min(entries.len());
        let next = (end < entries.len()).then(|| end.to_string());
        self.reply(
            id,
            json!({"data": entries[start..end], "nextCursor": next, "backwardsCursor": null}),
        );
    }

    fn complete(&mut self) {
        let Some(turn) = self.turn.take() else {
            return;
        };
        self.note(
            "thread/tokenUsage/updated",
            json!({
                "threadId": THREAD,
                "turnId": turn,
                "tokenUsage": {
                    "last": {
                        "inputTokens": 11,
                        "outputTokens": 7,
                        "cachedInputTokens": 3,
                        "cacheWriteInputTokens": 2,
                        "reasoningOutputTokens": 0,
                        "totalTokens": 23
                    },
                    "total": {
                        "inputTokens": 11,
                        "outputTokens": 7,
                        "cachedInputTokens": 3,
                        "reasoningOutputTokens": 0,
                        "totalTokens": 21
                    }
                }
            }),
        );
        self.completed(&turn, "completed", Value::Null);
    }

    fn fail(&mut self, message: &str) {
        let Some(turn) = self.turn.take() else {
            return;
        };
        self.note(
            "error",
            json!({
                "error": {"message": message},
                "willRetry": false,
                "threadId": THREAD,
                "turnId": turn
            }),
        );
        self.completed(&turn, "failed", json!({"message": message}));
    }

    fn completed(&mut self, turn: &str, status: &str, error: Value) {
        let mut value = turn_value(turn, status);
        value["error"] = error;
        self.note("turn/completed", json!({"threadId": THREAD, "turn": value}));
    }

    fn reply(&mut self, id: &Value, result: Value) {
        self.write(json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }

    fn error(&mut self, id: &Value, error: Value) {
        self.write(json!({"jsonrpc": "2.0", "id": id, "error": error}));
    }

    fn note(&mut self, method: &str, params: Value) {
        self.write(json!({"jsonrpc": "2.0", "method": method, "params": params}));
    }

    fn write(&mut self, value: Value) {
        let _ = writeln!(self.out, "{value}");
        let _ = self.out.flush();
    }
}

/// Whether an input's text is over Codex's limit.
fn too_long(params: &Value) -> bool {
    let chars: usize = params["input"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| part["text"].as_str())
        .map(|text| text.chars().count())
        .sum();
    chars > MAX_INPUT_CHARS
}

fn turn_value(turn: &str, status: &str) -> Value {
    json!({"id": turn, "items": [], "status": status, "error": null})
}

/// A `thread/items/list` entry for one `userMessage`.
fn entry(turn: &str, client_id: &Value, text: &str) -> Value {
    json!({
        "turnId": turn,
        "startedAtMs": null,
        "completedAtMs": null,
        "item": {
            "type": "userMessage",
            "id": format!("item-{}", client_id.as_str().unwrap_or("none")),
            "clientId": client_id,
            "content": [{"type": "text", "text": text, "text_elements": []}]
        }
    })
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}
