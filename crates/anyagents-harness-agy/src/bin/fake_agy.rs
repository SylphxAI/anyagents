//! Speaks just enough stream-json for the Antigravity adapter tests.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script: `hold`, `finish`, `quota`, `echo`.
//! `ANYAGENTS_FAKE_LOG` is a file that receives argv and stdin.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

const RESULT: &str = r#"{"type":"result","session_id":"sess-from-vendor","usage":{"input_tokens":11,"output_tokens":7,"cache_read_tokens":3,"cache_write_tokens":2}}"#;
const QUOTA: &str = r#"{"type":"rate_limit","resets_at_ms":1700000000000}"#;
const ECHO: &str = r#"{"type":"user","text":"anyagents-message:echoed\nprior"}"#;

fn main() {
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".into());
    let log_path = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from);
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
        if mode == "finish" && line.contains("\"type\":\"user\"") {
            let _ = writeln!(stdout, "{RESULT}");
            let _ = stdout.flush();
        }
    }
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}
