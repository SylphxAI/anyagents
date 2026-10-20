//! Speaks just enough stream-json for the Antigravity adapter tests.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script: `hold`, `finish`, `quota`, `echo`,
//! or `hook`. `hook` runs the vendor's before-step hook the way `agy` does: on
//! every model call after the first it runs the program named by
//! the `PreInvocation` hook in `CONFIG_DIR/.agents/hooks.json` (or the program
//! `ANYAGENTS_FAKE_HOOK` names), passes it
//! `{"invocationNum": n}`, logs the answer as `hook:<answer>`, and treats each
//! injected `userMessage` line with a mailbox marker as a message it has read.
//! `ANYAGENTS_FAKE_LOG` is a file that receives argv and stdin.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

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
    // A turn is one `user` line. With `hook`, the child also calls its hook
    // once before the turn's first model call and again while it works.
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &line);
        if !line.contains("\"type\":\"user\"") {
            continue;
        }
        if mode == "hook" {
            run_hook(&mut stdout, &log_path, 0);
            run_hook(&mut stdout, &log_path, 1);
            run_hook(&mut stdout, &log_path, 2);
        }
        if mode == "finish" || mode == "hook" {
            let _ = writeln!(stdout, "{RESULT}");
            let _ = stdout.flush();
        }
    }
}

fn run_hook(stdout: &mut impl Write, log_path: &Option<PathBuf>, invocation: u64) {
    let program = env::var_os("ANYAGENTS_FAKE_HOOK")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env::var("CONFIG_DIR").unwrap_or_default())
                .join("hooks")
                .join("pre_invocation")
        });
    let settings = PathBuf::from(env::var("CONFIG_DIR").unwrap_or_default())
        .join(".agents")
        .join("hooks.json");
    let entry = std::fs::read(&settings)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|settings| {
            settings
                .pointer("/hooks/PreInvocation/0/hooks/0")
                .cloned()
        });
    let program = entry
        .as_ref()
        .and_then(|entry| entry.get("command"))
        .and_then(|command| command.as_str())
        .map(PathBuf::from)
        .unwrap_or(program);
    let dir = entry
        .as_ref()
        .and_then(|entry| entry.pointer("/env/ANYAGENTS_AGY_HOOK_DIR"))
        .and_then(|dir| dir.as_str())
        .map(str::to_string)
        .or_else(|| env::var("ANYAGENTS_AGY_HOOK_DIR").ok())
        .unwrap_or_default();
    log(log_path, &format!("hook-program:{} env:{dir}", program.display()));
    let Ok(mut child) = Command::new(&program)
        .env("ANYAGENTS_AGY_HOOK_DIR", &dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        log(log_path, "hook:spawn-failed");
        return;
    };
    if let Some(mut stdin) = child.stdin.take() {
        let _ = writeln!(stdin, "{{\"invocationNum\":{invocation}}}");
    }
    let Ok(output) = child.wait_with_output() else {
        return;
    };
    let answer = String::from_utf8_lossy(&output.stdout).trim().to_string();
    log(log_path, &format!("hook:{invocation}:{answer}"));
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&answer) else {
        return;
    };
    let Some(steps) = value.get("injectSteps").and_then(|steps| steps.as_array()) else {
        return;
    };
    for step in steps {
        let Some(text) = step.get("userMessage").and_then(|text| text.as_str()) else {
            continue;
        };
        for chunk in text.split("\n\n") {
            if chunk.starts_with("anyagents-message:") {
                // The child now holds the line: it is part of this turn.
                let text = serde_json::to_string(chunk).unwrap_or_default();
                let _ = writeln!(stdout, "{{\"type\":\"user\",\"text\":{text}}}");
            }
        }
    }
    let _ = stdout.flush();
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}
