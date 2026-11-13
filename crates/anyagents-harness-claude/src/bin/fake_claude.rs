//! Speaks just enough stream-json for the adapter tests.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script: `hold`, `finish`, `batch`,
//! `batch_late`, `quota`, `failed_turn`, `synthetic_401`, `steer` or `die`.
//! `ANYAGENTS_FAKE_LOG` is a file that receives argv and stdin.
//!
//! Every line enters the conversation as soon as it is read: the child proves
//! it as Claude Code does, with `command_lifecycle` `started` and the
//! `--replay-user-messages` echo, both carrying the line's `uuid`. `batch_late`
//! closes the first turn before it proves the second line, so that line starts
//! the next turn. The `steer` script logs each prompt's length so a test can
//! tell the lines apart.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use serde_json::{Value, json};

const RESULT: &str = r#"{"type":"result","subtype":"success","session_id":"sess-from-vendor","total_cost_usd":0.5,"usage":{"input_tokens":11,"output_tokens":7,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}"#;
const QUOTA: &str = r#"{"type":"system","subtype":"api_retry","category":"rate_limit","resets_at_ms":1700000000000}"#;
const FAILED: &str = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"session_id":"sess-from-vendor","result":"Invalid API key · Please run /login","usage":{"input_tokens":0,"output_tokens":0}}"#;
const SYNTHETIC_401_ASSISTANT: &str = r#"{"type":"assistant","session_id":"sess-from-vendor","message":{"model":"<synthetic>","role":"assistant","content":[{"type":"text","text":"Failed to authenticate. API Error: 401 {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"OAuth access token has expired.\"}}"}]}}"#;
const SYNTHETIC_401_RESULT: &str = r#"{"type":"result","subtype":"success","is_error":false,"session_id":"sess-from-vendor","result":"Failed to authenticate. API Error: 401 OAuth access token has expired","usage":{"input_tokens":0,"output_tokens":0}}"#;

fn main() {
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".to_string());
    let log_path = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from);
    let argv = env::args().skip(1).collect::<Vec<_>>().join(" ");
    log(&log_path, &format!("argv:{argv}"));
    if mode == "stderr_failure" {
        let mut stderr = io::stderr().lock();
        // More than a pipe buffer; no newline and a split diagnostic phrase.
        stderr
            .write_all(b"synthetic-password=do-not-publish Permis")
            .unwrap();
        stderr.write_all(b"sion denied\0\xff").unwrap();
        for _ in 0..256 {
            stderr.write_all(&[b'x'; 4096]).unwrap();
        }
        stderr.flush().unwrap();
        std::process::exit(23);
    }
    if mode == "die" {
        return;
    }

    let mut stdout = io::stdout();
    if mode == "quota" {
        let _ = writeln!(stdout, "{QUOTA}");
        let _ = stdout.flush();
    }

    let mut users = 0usize;
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &line);
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) != Some("user") {
            continue;
        }
        users += 1;
        if mode == "steer" {
            let text = value
                .pointer("/message/content/0/text")
                .and_then(Value::as_str)
                .unwrap_or("");
            log(&log_path, &format!("user:{users}:{}", text.len()));
        }
        let late = mode == "batch_late" && users == 2;
        if late {
            let _ = writeln!(stdout, "{RESULT}");
        }
        prove(&mut stdout, &value);
        if mode == "finish" || (mode == "batch" && users == 2) {
            let _ = writeln!(stdout, "{RESULT}");
        }
        if mode == "failed_turn" && users == 1 {
            let _ = writeln!(stdout, "{FAILED}");
        }
        if mode == "synthetic_401" && users == 1 {
            let _ = writeln!(stdout, "{SYNTHETIC_401_ASSISTANT}");
            let _ = writeln!(stdout, "{SYNTHETIC_401_RESULT}");
        }
        if mode == "finish" && users == 1 {
            let _ = writeln!(stdout, "{QUOTA}");
        }
        let _ = stdout.flush();
    }
}

/// The line entered the conversation: its lifecycle start and its replay
/// echo, keyed by its uuid.
fn prove(stdout: &mut impl Write, line: &Value) {
    let uuid = line.get("uuid").cloned().unwrap_or(Value::Null);
    let started = json!({
        "type": "command_lifecycle",
        "command_uuid": uuid,
        "state": "started",
        "uuid": uuid,
        "session_id": "sess-from-vendor",
    });
    let echo = json!({
        "type": "user",
        "message": line.get("message").cloned().unwrap_or(Value::Null),
        "session_id": "sess-from-vendor",
        "parent_tool_use_id": null,
        "uuid": uuid,
        "isReplay": true,
    });
    let _ = writeln!(stdout, "{started}");
    let _ = writeln!(stdout, "{echo}");
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}
