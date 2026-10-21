//! Speaks just enough of the Muse `serve` JSON-RPC protocol for the adapter
//! tests.
//!
//! `ANYAGENTS_FAKE_MODE` selects the script for a `turn/start`.
//! `ANYAGENTS_FAKE_LOG` records argv and every line.

use std::collections::VecDeque;
use std::env;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

fn main() {
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "hold".into());
    let steer = env::var("ANYAGENTS_FAKE_STEER").is_ok();
    let log_path = env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from);
    log(
        &log_path,
        &format!("argv:{}", env::args().skip(1).collect::<Vec<_>>().join(" ")),
    );
    let mut stdout = io::stdout();
    let mut users = 0usize;
    let mut open: Option<String> = None;
    let mut pending: VecDeque<serde_json::Value> = VecDeque::new();
    let mut collected: Vec<String> = Vec::new();
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        log(&log_path, &line);
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let method = value.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let id = value.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let params = value.get("params").cloned().unwrap_or_default();
        match method {
            "initialize" => answer(&mut stdout, &id, r#"{"protocolVersion":1}"#),
            "initialized" | "noop" => {}
            "session/start" => answer(
                &mut stdout,
                &id,
                r#"{"session":{"sessionId":"sess-from-vendor"}}"#,
            ),
            "session/resume" => answer(
                &mut stdout,
                &id,
                &format!(
                    r#"{{"session":{{"sessionId":"{}"}}}}"#,
                    params
                        .get("sessionId")
                        .and_then(|s| s.as_str())
                        .unwrap_or("sess-from-vendor")
                ),
            ),
            "session/setApprovalMode" => answer(&mut stdout, &id, "{}"),
            "turn/start" => {
                if mode == "quota" {
                    notify(
                        &mut stdout,
                        &format!(
                            r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":429,"message":"rate limited","data":{{"resetsAt":1700000000}}}}}}"#
                        ),
                    );
                    continue;
                }
                // A turn stays open until its approval is answered, even while
                // its `turn/start` reply is still queued: `open` is the truth.
                if params.get("ifBusy").is_some() && open.is_some() {
                    let user = params["input"][0]["text"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    collected.push(user);
                    let turn = open.take().unwrap_or_default();
                    // The host learns the steer was accepted before the turn
                    // closes, so it can record the message as carried.
                    answer(
                        &mut stdout,
                        &id,
                        r#"{"status":"accepted","disposition":"steered"}"#,
                    );
                    // Both messages are carried now, so the turn closes.
                    notify(
                        &mut stdout,
                        &format!(
                            r#"{{"jsonrpc":"2.0","method":"turn/completed","params":{{"turnId":"{turn}","status":"completed","usage":{{"inputTokens":11,"outputTokens":7}}}}}}"#
                        ),
                    );
                    continue;
                }
                users += 1;
                let turn = format!("turn-{users}");
                if mode == "finish" {
                    let user = params["input"][0]["text"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    let text = user
                        .split_once('\n')
                        .map(|(_, text)| text)
                        .unwrap_or("")
                        .to_string();
                    notify(
                        &mut stdout,
                        &format!(
                            r#"{{"jsonrpc":"2.0","method":"turn/started","params":{{"turnId":"{turn}"}}}}"#
                        ),
                    );
                    notify(
                        &mut stdout,
                        &format!(
                            r#"{{"jsonrpc":"2.0","method":"turn/completed","params":{{"turnId":"{turn}","status":"completed","usage":{{"inputTokens":11,"outputTokens":7,"cachedInputTokens":3}},"items":[{{"type":"text","text":"pong from muse: {text}"}}]}}}}"#
                        ),
                    );
                    answer(
                        &mut stdout,
                        &id,
                        &format!(
                            r#"{{"status":"accepted","disposition":"started","turnId":"{turn}"}}"#
                        ),
                    );
                    continue;
                }
                if mode == "failed" {
                    notify(
                        &mut stdout,
                        &format!(
                            r#"{{"jsonrpc":"2.0","method":"turn/started","params":{{"turnId":"{turn}"}}}}"#
                        ),
                    );
                    notify(
                        &mut stdout,
                        &format!(
                            r#"{{"jsonrpc":"2.0","method":"turn/completed","params":{{"turnId":"{turn}","status":"failed","error":{{"message":"provider returned 500"}}}}}}"#
                        ),
                    );
                    answer(
                        &mut stdout,
                        &id,
                        &format!(
                            r#"{{"status":"accepted","disposition":"started","turnId":"{turn}"}}"#
                        ),
                    );
                    continue;
                }
                // hold: the turn stays open until the host answers the approval.
                notify(
                    &mut stdout,
                    &format!(
                        r#"{{"jsonrpc":"2.0","method":"turn/started","params":{{"turnId":"{turn}"}}}}"#
                    ),
                );
                pending.push_back(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"status": "accepted", "disposition": "started", "turnId": turn}
                }));
                collected.push(
                    params["input"][0]["text"]
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                );
                open = Some(turn);
                let _ = steer;
                // A tool approval exchange inside the open turn.
                notify(
                    &mut stdout,
                    r#"{"jsonrpc":"2.0","method":"approval/request","id":900,"params":{"approvalId":"a1","toolName":"shell","availableChoices":[{"choiceId":"c1","decision":"approved"}]}}"#,
                );
            }
            _ => {
                // Our answer to the vendor's approval request: the turn carries
                // on, as it does while a steer joins it. Nothing closes here.
                let _ = value;
            }
        }
        while let Some(reply) = pending.pop_front() {
            notify(&mut stdout, &reply.to_string());
        }
    }
}

fn answer(stdout: &mut io::Stdout, id: &serde_json::Value, result: &str) {
    notify(
        stdout,
        &format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#),
    );
}

fn notify(stdout: &mut io::Stdout, line: &str) {
    let _ = writeln!(stdout, "{line}");
    let _ = stdout.flush();
}

fn log(path: &Option<PathBuf>, line: &str) {
    let Some(path) = path else { return };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}
