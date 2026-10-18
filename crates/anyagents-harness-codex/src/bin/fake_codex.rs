//! Speaks just enough newline-delimited JSON-RPC for the adapter tests.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script: `hold`, `finish`, `steer`, `quota`,
//! `rpc-quota`, or `die`. `ANYAGENTS_FAKE_LOG` receives argv and stdin.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use serde_json::{Value, json};

fn main() {
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".to_string());
    let log_path = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from);
    let argv = env::args().skip(1).collect::<Vec<_>>().join(" ");
    log(&log_path, &format!("argv:{argv}"));
    if mode == "die" {
        return;
    }

    let mut stdout = io::stdout();
    let mut turn_id: Option<String> = None;
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &line);
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let method = value.get("method").and_then(Value::as_str).unwrap_or("");
        let id = value.get("id").cloned();
        match method {
            "initialize" => {
                if let Some(id) = id {
                    reply(
                        &mut stdout,
                        &id,
                        json!({
                            "userAgent": "fake",
                            "codexHome": "",
                            "platformFamily": "unix",
                            "platformOs": "linux"
                        }),
                    );
                }
            }
            "initialized" => {}
            "thread/start" | "thread/resume" => {
                let thread_id = value
                    .pointer("/params/threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("thread-1");
                if mode == "quota" {
                    note(
                        &mut stdout,
                        "account/rateLimits/updated",
                        json!({
                            "rateLimits": {
                                "rateLimitReachedType": "rate_limit_reached",
                                "primary": {"usedPercent": 100, "resetsAt": 1_700_000_000_000_i64}
                            }
                        }),
                    );
                }
                if let Some(id) = id {
                    reply(&mut stdout, &id, json!({"thread": {"id": thread_id}}));
                }
            }
            "turn/start" if mode == "rpc-quota" => {
                if let Some(id) = id {
                    error(
                        &mut stdout,
                        &id,
                        json!({
                            "code": -32000,
                            "message": "usageLimitExceeded",
                            "data": {"resetsAt": 1_700_000_000_000_i64}
                        }),
                    );
                }
            }
            "turn/start" => {
                let tid = "turn-1".to_string();
                if let Some(id) = id {
                    reply(
                        &mut stdout,
                        &id,
                        json!({"turn": {"id": tid, "items": [], "status": "inProgress"}}),
                    );
                }
                turn_id = Some(tid.clone());
                if mode == "finish" {
                    emit_usage_and_complete(&mut stdout, &tid);
                }
            }
            "turn/steer" => {
                let tid = value
                    .pointer("/params/expectedTurnId")
                    .and_then(Value::as_str)
                    .unwrap_or("turn-1")
                    .to_string();
                if let Some(id) = id {
                    reply(&mut stdout, &id, json!({"turnId": tid}));
                }
                if mode == "steer" || mode == "finish" {
                    emit_usage_and_complete(&mut stdout, &tid);
                }
            }
            "turn/interrupt" => {
                if let Some(id) = id {
                    reply(&mut stdout, &id, json!({}));
                }
                let tid = turn_id.clone().unwrap_or_else(|| "turn-1".to_string());
                note(
                    &mut stdout,
                    "turn/completed",
                    json!({
                        "threadId": "thread-1",
                        "turn": {"id": tid, "items": [], "status": "interrupted"}
                    }),
                );
            }
            _ => {}
        }
    }
}

fn emit_usage_and_complete(stdout: &mut impl Write, turn_id: &str) {
    note(
        stdout,
        "thread/tokenUsage/updated",
        json!({
            "threadId": "thread-1",
            "turnId": turn_id,
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
    note(
        stdout,
        "turn/completed",
        json!({
            "threadId": "thread-1",
            "turn": {"id": turn_id, "items": [], "status": "completed"}
        }),
    );
}

fn reply(stdout: &mut impl Write, id: &Value, result: Value) {
    write_json(
        stdout,
        json!({"jsonrpc": "2.0", "id": id, "result": result}),
    );
}

fn error(stdout: &mut impl Write, id: &Value, error: Value) {
    write_json(stdout, json!({"jsonrpc": "2.0", "id": id, "error": error}));
}

fn note(stdout: &mut impl Write, method: &str, params: Value) {
    write_json(
        stdout,
        json!({"jsonrpc": "2.0", "method": method, "params": params}),
    );
}

fn write_json(stdout: &mut impl Write, value: Value) {
    let _ = writeln!(stdout, "{value}");
    let _ = stdout.flush();
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}
