//! One scripted vendor for every harness's restart test.
//!
//! It is the double `anyagents` starts, so each test runs the adapter's own
//! startup against a vendor that answers that harness's protocol. It names the
//! session it opens, answers one message, and writes down every session it was
//! asked to resume and every one it refused.
//!
//! A test writes the sessions it does not have, one per line, to the file
//! named by `ANYAGENTS_VENDOR_REFUSE`. `agy` and `claude` answer such a session
//! with an error and stay up, as the CLIs do; `muse` and `codex` refuse the
//! resume call and start a session of their own.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

/// The session this vendor opens when it is given none.
pub const OPENED: &str = "sess-vendor-double";

fn main() {
    let exe = std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().to_string())
        })
        .unwrap_or_default();
    if !exe.starts_with("restart-") {
        // Another harness's test binary: it was named this way by the server.
        return;
    }
    let harness = harness_of(&exe);
    let files = Files::new();
    match harness.as_str() {
        "muse" => run_muse(&files),
        "claude-code" => run_claude(&files),
        "codex" => run_codex(&files),
        other => run_agy(&files, other),
    }
}

/// The harness whose test this binary is: `restart-agy` runs `agy`.
fn harness_of(exe: &str) -> String {
    let name = exe
        .trim_start_matches("restart-")
        .split('-')
        .next()
        .unwrap_or("agy");
    match name {
        "muse" => "muse".to_string(),
        "claude" => "claude-code".to_string(),
        "codex" => "codex".to_string(),
        _ => "agy".to_string(),
    }
}

/// Where the files live: the directory of the log the server hands this vendor.
struct Files {
    log: PathBuf,
}

impl Files {
    fn new() -> Self {
        let log = std::env::var("ANYAGENTS_FAKE_LOG")
            .or_else(|_| std::env::var("ANYAGENTS_SHARED_LOG"))
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join("anyagents-vendor-double.log"));
        Self { log }
    }

    fn dir(&self) -> PathBuf {
        self.log
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(std::env::temp_dir)
    }

    /// Record `line` under `name` in this vendor's own files.
    fn write(&self, name: &str, line: &str) {
        let path = self.dir().join(name);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        let _ = writeln!(file, "{line}");
    }
}

/// Mark the session this vendor was asked for: resumed, or refused when the
/// test wrote it to the refusal file.
fn asked(files: &Files, session: &str) -> bool {
    let refuse = std::env::var_os("ANYAGENTS_VENDOR_REFUSE")
        .map(PathBuf::from)
        .unwrap_or_else(|| files.dir().join("refuse"));
    let refused = std::fs::read_to_string(refuse)
        .unwrap_or_default()
        .lines()
        .any(|gone| gone == session);
    files.write(if refused { "refused" } else { "resumed" }, session);
    refused
}

/// The name this vendor answers a resume of `session` with.
fn named(session: &str) -> &str {
    if session.is_empty() { OPENED } else { session }
}

fn run_agy(files: &Files, _harness: &str) {
    let dir = files.dir();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let resume = args
        .iter()
        .position(|arg| arg == "--resume")
        .and_then(|at| args.get(at + 1))
        .cloned();
    // The refusal is answered before any line is read, as the CLI does: a
    // `--resume` of a session it does not have is a result line and no
    // conversation.
    let refused = match &resume {
        Some(session) => asked(files, session),
        None => false,
    };
    let mut stdout = std::io::stdout();
    if refused {
        let _ = writeln!(stdout, "{}", refused_line(resume.as_deref().unwrap_or("")));
        let _ = stdout.flush();
    }
    let session = named(resume.as_deref().unwrap_or(""));
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if let Ok(value) = serde_json::from_str::<Value>(&line)
            && value.get("type").and_then(Value::as_str) == Some("user")
        {
            let answer = if refused {
                refused_line(resume.as_deref().unwrap_or(""))
            } else {
                result(session)
            };
            let _ = writeln!(stdout, "{answer}");
            let _ = stdout.flush();
        }
    }
    let _ = dir;
}

fn run_claude(files: &Files) {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let resume = args
        .iter()
        .position(|arg| arg == "--resume")
        .and_then(|at| args.get(at + 1))
        .cloned();
    // The refusal is answered before any line is read, as the CLI does: a
    // `--resume` of a session it does not have is a result line and no
    // conversation.
    let refused = match &resume {
        Some(session) => asked(files, session),
        None => false,
    };
    let mut stdout = std::io::stdout();
    if refused {
        let _ = writeln!(stdout, "{}", refused_line(resume.as_deref().unwrap_or("")));
        let _ = stdout.flush();
    }
    let session = named(resume.as_deref().unwrap_or(""));
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if line.contains("\"type\":\"user\"") {
            let answer = if refused {
                refused_line(resume.as_deref().unwrap_or(""))
            } else {
                result(session)
            };
            let _ = writeln!(stdout, "{answer}");
            let _ = stdout.flush();
        }
    }
}

fn run_muse(files: &Files) {
    let mut stdout = std::io::stdout();
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let method = value.get("method").and_then(Value::as_str).unwrap_or("");
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "initialize" => reply(&mut stdout, &id, json!({"protocolVersion": 1})),
            "session/start" => reply(&mut stdout, &id, json!({"session": {"sessionId": OPENED}})),
            "session/resume" => {
                let session = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if asked(files, session) {
                    reply_error(&mut stdout, &id, 404, "session not found");
                } else {
                    reply(
                        &mut stdout,
                        &id,
                        json!({"session": {"sessionId": named(session)}}),
                    );
                }
            }
            "session/setApprovalMode" => reply(&mut stdout, &id, json!({})),
            "turn/start" => {
                reply(
                    &mut stdout,
                    &id,
                    json!({"status": "accepted", "disposition": "started", "turnId": "turn-1"}),
                );
                note(
                    &mut stdout,
                    "turn/completed",
                    json!({"turnId": "turn-1", "status": "completed"}),
                );
            }
            _ => {}
        }
    }
}

fn run_codex(files: &Files) {
    let mut stdout = std::io::stdout();
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let method = value.get("method").and_then(Value::as_str).unwrap_or("");
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "initialize" => reply(
                &mut stdout,
                &id,
                json!({
                    "userAgent": "double",
                    "codexHome": "",
                    "platformFamily": "unix",
                    "platformOs": "linux"
                }),
            ),
            "thread/start" => reply(&mut stdout, &id, json!({"thread": {"id": OPENED}})),
            "thread/resume" => {
                let session = params.get("threadId").and_then(Value::as_str).unwrap_or("");
                if asked(files, session) {
                    reply_error(&mut stdout, &id, -32000, "thread not found");
                } else {
                    reply(&mut stdout, &id, json!({"thread": {"id": named(session)}}));
                }
            }
            "turn/start" | "turn/steer" => {
                reply(
                    &mut stdout,
                    &id,
                    json!({"turn": {"id": "turn-1", "items": [], "status": "inProgress"}}),
                );
                note(
                    &mut stdout,
                    "turn/completed",
                    json!({"threadId": "t", "turn": {"id": "turn-1", "items": [], "status": "completed"}}),
                );
            }
            _ => {}
        }
    }
}

/// The one line each stream vendor prints when its turn ends well.
fn result(session: &str) -> String {
    json!({
        "type": "result",
        "subtype": "success",
        "session_id": session,
        "usage": {"input_tokens": 1, "output_tokens": 1}
    })
    .to_string()
}

/// The line a stream vendor prints for a session it does not have.
fn refused_line(session: &str) -> String {
    json!({
        "type": "result",
        "subtype": "error",
        "is_error": true,
        "session_id": session,
        "result": "no conversation found with session ID"
    })
    .to_string()
}

fn reply(stdout: &mut impl Write, id: &Value, result: Value) {
    let _ = writeln!(
        stdout,
        "{}",
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    );
    let _ = stdout.flush();
}

fn reply_error(stdout: &mut impl Write, id: &Value, code: i64, message: &str) {
    let _ = writeln!(
        stdout,
        "{}",
        json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
    );
    let _ = stdout.flush();
}

fn note(stdout: &mut impl Write, method: &str, params: Value) {
    let _ = writeln!(
        stdout,
        "{}",
        json!({"jsonrpc": "2.0", "method": method, "params": params})
    );
    let _ = stdout.flush();
}
