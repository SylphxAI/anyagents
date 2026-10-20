//! Speaks just enough of the OpenCode serve line protocol for the adapter tests.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script: `hold`, `finish`, `quota`, `echo`,
//! `stream`, `steer`. `ANYAGENTS_FAKE_LOG` is a file that receives argv and stdin. A test
//! that starts this program through a symlink gets `<link path>.log` instead,
//! so it can use `env!` for one binary and still start a real process.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

const RESULT: &str = r#"{"type":"result","session_id":"sess-from-vendor","cost_micros":250000,"usage":{"input_tokens":11,"output_tokens":7,"cache_read_tokens":3,"cache_write_tokens":2}}"#;
const QUOTA: &str = r#"{"type":"rate_limit","resets_at_ms":1700000000000}"#;
const ECHO: &str = r#"{"type":"prompt","text":"anyagents-message:echoed\nprior"}"#;

fn main() {
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".into());
    let log_path = log_path();
    log(
        &log_path,
        &format!("argv:{}", env::args().skip(1).collect::<Vec<_>>().join(" ")),
    );
    let mut stdout = io::stdout();
    if mode == "quota" {
        let _ = writeln!(stdout, "{QUOTA}");
        let _ = stdout.flush();
    }
    if mode == "echo" {
        let _ = writeln!(stdout, "{ECHO}");
        let _ = stdout.flush();
    }
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &line);
        match mode.as_str() {
            "finish" if line.contains("\"type\":\"prompt\"") => {
                let _ = writeln!(stdout, "{RESULT}");
                let _ = stdout.flush();
            }
            // A steered prompt is echoed at once and the turn stays open; a
            // plain prompt opens the turn.
            "steer" => {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
                    let steer = value.get("delivery").and_then(|d| d.as_str()) == Some("steer");
                    let text = value.get("text").and_then(|t| t.as_str()).unwrap_or("");
                    log(&log_path, &format!("prompt:steer={steer}"));
                    if steer {
                        let echo = serde_json::json!({"type": "text", "text": text});
                        let _ = writeln!(stdout, "{echo}");
                        let _ = stdout.flush();
                    }
                }
            }
            // Text only, and no closing result line, as a streaming vendor does.
            "stream" if line.contains("\"type\":\"prompt\"") => {
                let _ = writeln!(
                    stdout,
                    r#"{{"type":"text","session_id":"sess-1","text":"pong"}}"#
                );
                let _ = stdout.flush();
            }
            _ => {}
        }
    }
}

/// `ANYAGENTS_FAKE_LOG`, or beside a symlinked name for this program.
fn log_path() -> Option<PathBuf> {
    if let Some(path) = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from) {
        return Some(path);
    }
    let exe = env::current_exe().ok()?;
    match std::fs::read_link(&exe) {
        Ok(link) => Some(link.with_extension("log")),
        Err(_) => None,
    }
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}
