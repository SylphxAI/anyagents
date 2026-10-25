//! Speaks just enough stream-json for the adapter tests.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script: `hold`, `finish`, `batch`, `quota`,
//! `echo`, `failed_turn`, `steer` or `die`. `ANYAGENTS_FAKE_LOG` is a file that
//! receives argv and stdin.
//!
//! The steer script echoes idle stdin messages. Tests explicitly trigger SDK
//! PreToolUse callbacks through a file; a tool only proceeds once its control
//! response arrives. An stdin echo alone is not evidence of mid-turn delivery.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use serde_json::{Value, json};

const RESULT: &str = r#"{"type":"result","subtype":"success","session_id":"sess-from-vendor","total_cost_usd":0.5,"usage":{"input_tokens":11,"output_tokens":7,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}"#;
const QUOTA: &str = r#"{"type":"system","subtype":"api_retry","category":"rate_limit","resets_at_ms":1700000000000}"#;
const FAILED: &str = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"session_id":"sess-from-vendor","result":"Invalid API key · Please run /login","usage":{"input_tokens":0,"output_tokens":0}}"#;
const ECHO: &str = r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"anyagents-message:echoed\nprior"}]}}"#;

fn main() {
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".to_string());
    let log_path = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from);
    let argv = env::args().skip(1).collect::<Vec<_>>().join(" ");
    log(&log_path, &format!("argv:{argv}"));
    if let Ok(path) = env::var("ANYAGENTS_FAKE_TRIGGER") {
        std::thread::spawn(move || {
            let mut previous = String::new();
            loop {
                let next = std::fs::read_to_string(&path).unwrap_or_default();
                if !next.is_empty() && next != previous {
                    previous = next.clone();
                    let value = if next == "result" {
                        serde_json::from_str::<Value>(RESULT).unwrap()
                    } else {
                        json!({"type":"control_request", "request_id":next,
                            "request":{"subtype":"hook_callback", "callback_id":"mailbox",
                            "input":{"hook_event_name":"PreToolUse"}}})
                    };
                    let mut out = io::stdout().lock();
                    writeln!(out, "{value}").unwrap();
                    out.flush().unwrap();
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        });
    }

    if mode == "steer" && env::var_os("ANYAGENTS_FAKE_TRIGGER").is_none() {
        std::thread::spawn(|| {
            for seq in 0u64.. {
                std::thread::sleep(std::time::Duration::from_millis(50));
                let request = json!({"type":"control_request", "request_id":format!("tool-{seq}"),
                    "request":{"subtype":"hook_callback", "callback_id":"mailbox",
                    "input":{"hook_event_name":"PreToolUse"}}});
                let mut out = io::stdout().lock();
                if writeln!(out, "{request}")
                    .and_then(|_| out.flush())
                    .is_err()
                {
                    break;
                }
            }
        });
    }

    let mut stdout = io::stdout();
    if mode == "quota" {
        let _ = writeln!(stdout, "{QUOTA}");
        let _ = stdout.flush();
    }
    if mode == "echo" {
        let _ = writeln!(stdout, "{ECHO}");
        let _ = stdout.flush();
    }

    let mut users = 0usize;
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &line);
        let value: Value = serde_json::from_str(&line).unwrap();
        if value.pointer("/request/subtype").and_then(Value::as_str) == Some("initialize") {
            writeln!(
                stdout,
                "{}",
                json!({"type":"control_response", "response":{
                "subtype":if mode == "reject_init" { "error" } else { "success" }, "request_id":"initialize", "response":{}}})
            )
            .unwrap();
            stdout.flush().unwrap();
            if mode == "die" {
                return;
            }
            continue;
        }
        if value.get("type").and_then(Value::as_str) == Some("control_response") {
            log(&log_path, "tool:proceed");
            continue;
        }
        if mode == "steer" {
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && value.get("type").and_then(Value::as_str) == Some("user")
            {
                let text = value
                    .pointer("/message/content/0/text")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                users += 1;
                log(&log_path, &format!("user:{users}:{}", text.len()));
                let echo = json!({
                    "type": "user",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": text}]
                    }
                });
                let _ = writeln!(stdout, "{echo}");
                let _ = stdout.flush();
            }
            continue;
        }
        if !line.contains("\"type\":\"user\"") {
            continue;
        }
        users += 1;
        if mode == "finish" || (mode == "batch" && users == 2) {
            let _ = writeln!(stdout, "{RESULT}");
            let _ = stdout.flush();
        }
        if mode == "failed_turn" && users == 1 {
            let _ = writeln!(stdout, "{FAILED}");
            let _ = stdout.flush();
        }
        if mode == "finish" && users == 1 {
            let _ = writeln!(stdout, "{QUOTA}");
            let _ = stdout.flush();
        }
    }
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}
