//! An `agy` `PreInvocation` hook: reads the lines a channel pushed for this
//! agent and answers with the vendor's own mid-turn input,
//! `{"injectSteps": [{"userMessage": ...}]}`, so they reach the next model call.
//!
//! The hook directory comes from `ANYAGENTS_AGY_HOOK_DIR`, which the adapter
//! sets for its child. The queue is `<dir>/steer.jsonl`, one JSON line per
//! message; the oldest [`BATCH`] are injected in order and then removed. A
//! missing directory, an unreadable event, or another agent's call answers `{}`
//! so a turn is never broken.
//!
//! Stdin is one JSON object: `{"invocationNum": n, ...}`. Invocation 0 already
//! carries the turn's own prompt, so nothing is added there.

use std::io::{Read, Write};

use anyagents_harness::SteerLine;
use serde_json::{Value, json};

/// The most lines one model call takes; the rest follows on the next call.
const BATCH: usize = 40;

fn main() {
    let mut event = String::new();
    if std::io::stdin().read_to_string(&mut event).is_err() {
        answer(json!({}));
        return;
    }
    let event: Value = serde_json::from_str(&event).unwrap_or(Value::Null);
    if event
        .get("invocationNum")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        == 0
    {
        answer(json!({}));
        return;
    }
    let Some(dir) = std::env::var_os("ANYAGENTS_AGY_HOOK_DIR") else {
        answer(json!({}));
        return;
    };
    let path = std::path::PathBuf::from(dir).join("steer.jsonl");
    let Ok(text) = std::fs::read_to_string(&path) else {
        answer(json!({}));
        return;
    };
    let mut lines: Vec<SteerLine> = text
        .lines()
        .filter_map(|line| serde_json::from_str::<SteerLine>(line).ok())
        .collect();
    if lines.is_empty() {
        answer(json!({}));
        return;
    }
    let taken: Vec<SteerLine> = lines.drain(..lines.len().min(BATCH)).collect();
    let rest: String = lines
        .iter()
        .filter_map(|line| serde_json::to_string(line).ok())
        .map(|line| line + "\n")
        .collect();
    if std::fs::write(&path, rest).is_err() {
        answer(json!({}));
        return;
    }
    let body = taken
        .iter()
        .map(|line| format!("anyagents-message:{}\n{}", line.id, line.message))
        .collect::<Vec<_>>()
        .join("\n\n");
    answer(json!({"injectSteps": [{"userMessage": body}]}));
}

fn answer(value: Value) {
    let mut stdout = std::io::stdout();
    let _ = writeln!(stdout, "{value}");
    let _ = stdout.flush();
}
