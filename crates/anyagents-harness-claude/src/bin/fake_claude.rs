//! Speaks just enough stream-json for the adapter tests.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script: `hold`, `finish`, `batch`, `quota`,
//! `echo`, or `die`. `ANYAGENTS_FAKE_LOG` is a file that receives argv and stdin.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

const RESULT: &str = r#"{"type":"result","subtype":"success","session_id":"sess-from-vendor","total_cost_usd":0.5,"usage":{"input_tokens":11,"output_tokens":7,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}"#;
const QUOTA: &str = r#"{"type":"system","subtype":"api_retry","category":"rate_limit","resets_at_ms":1700000000000}"#;
const ECHO: &str = r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"anyagents-message:echoed\nprior"}]}}"#;

fn main() {
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".to_string());
    let log_path = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from);
    let argv = env::args().skip(1).collect::<Vec<_>>().join(" ");
    log(&log_path, &format!("argv:{argv}"));
    if mode == "die" {
        return;
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
        if !line.contains("\"type\":\"user\"") {
            continue;
        }
        users += 1;
        if mode == "finish" || (mode == "batch" && users == 2) {
            let _ = writeln!(stdout, "{RESULT}");
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
