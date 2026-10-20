//! A vendor double for the supervisor tests: the turn that carries
//! `anyagents-message:fail-once` ends in an auth error, every other turn
//! succeeds. Built by cargo, so the test lane runs a compiled program rather
//! than a file it wrote.

use std::io::{self, BufRead, Write};

const FAILED: &str = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"session_id":"s","result":"Invalid API key - Please run /login","usage":{"input_tokens":0,"output_tokens":0}}"#;
const GOOD: &str = r#"{"type":"result","subtype":"success","session_id":"s","total_cost_usd":0,"usage":{"input_tokens":1,"output_tokens":1}}"#;

fn main() {
    let mut stdout = io::stdout();
    let mut failed_once = false;
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if !line.contains("\"type\":\"user\"") {
            continue;
        }
        if !failed_once && line.contains("fail-once") {
            failed_once = true;
            let _ = writeln!(stdout, "{FAILED}");
        } else {
            let _ = writeln!(stdout, "{GOOD}");
        }
        let _ = stdout.flush();
    }
}
