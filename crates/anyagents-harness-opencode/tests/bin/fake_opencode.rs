//! A scripted `opencode` for the adapter tests.
//!
//! It speaks the same surface the real CLI does: `opencode serve` starts a
//! service that answers nothing on stdin, and `opencode api <METHOD> <path>`
//! prints one JSON answer. Every call is appended to `ANYAGENTS_FAKE_LOG`, so a
//! test can read what the adapter asked for.
//!
//! `ANYAGENTS_FAKE_MODE` chooses what a prompt does:
//! - `finish` (default): the next `GET .../message` carries the answer and an
//!   `idle` with `outcome: succeeded`;
//! - `hold`: the turn never finishes;
//! - `failed`: the turn ends `idle` with `outcome: failed`;
//! - `steer`: like `finish`, and a prompt with `delivery: steer` is accepted
//!   into the open turn.
//! - `inbox`: every accepted prompt is appended to `ANYAGENTS_FAKE_STATE/prompts`
//!   and the session lists all of them, with no answer and no idle until the
//!   test's own file says the turn ended. This is a turn that is open for the
//!   whole test, so a prompt written mid-turn stays in the session's inbox.

use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::process::exit;
use std::thread;
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("serve") => serve(),
        Some("api") => {
            // `--server <url>` names the service; the scripted one ignores it,
            // but it must accept it as the real CLI does.
            let mut rest: Vec<String> = args[1..].to_vec();
            if let Some(at) = rest.iter().position(|arg| arg == "--server") {
                rest.drain(at..(at + 2).min(rest.len()));
            }
            api(&rest)
        }
        _ => exit(2),
    }
}

/// Hold the foreground like a service, forever, announcing the address and
/// password exactly as the real CLI prints them. The adapter kills it.
fn serve() {
    let port = env::var("ANYAGENTS_FAKE_PORT").unwrap_or_else(|_| "0".into());
    println!("server listening on http://127.0.0.1:{port}");
    println!("server password scripted-password");
    let _ = std::io::stdout().flush();
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

fn api(args: &[String]) {
    let method = args.first().map(String::as_str).unwrap_or("");
    let path = args.get(1).map(String::as_str).unwrap_or("");
    let body = args
        .iter()
        .position(|arg| arg == "-d")
        .and_then(|at| args.get(at + 1))
        .cloned()
        .unwrap_or_default();
    log(&format!("api:{method} {path} {body}"));

    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_else(|_| "finish".into());
    let dir =
        env::var("ANYAGENTS_FAKE_STATE").unwrap_or_else(|_| env::temp_dir().display().to_string());
    let _ = dir;
    match (method, path) {
        ("GET", "/api/session") => {
            print!("{{\"data\":[{{\"id\":\"ses_fake\"}}]}}");
        }
        ("GET", p) if p.starts_with("/api/session/ses_") && !p.contains("/message") => {
            // `gone` mode: the stored session no longer exists in this store.
            if mode == "gone" {
                print!("{{\"error\":{{\"code\":404,\"message\":\"no such session\"}}}}");
                exit(1);
            }
            print!("{{\"data\":{{\"id\":\"ses_fake\",\"title\":\"fake\"}}}}");
        }
        ("POST", "/api/session") => {
            let id = if mode == "gone" {
                "ses_fresh"
            } else {
                "ses_fake"
            };
            print!("{{\"data\":{{\"id\":\"{id}\"}}}}");
        }
        ("POST", p) if p.ends_with("/prompt") => {
            let steered = body.contains("\"delivery\":\"steer\"");
            let text = prompt_text(&body);
            let dir = env::var("ANYAGENTS_FAKE_STATE").unwrap_or_default();
            if !dir.is_empty() {
                let _ = std::fs::write(format!("{dir}/last-prompt"), &text);
            }
            if mode == "inbox" && !dir.is_empty() {
                log(&format!("accepted:{}", text.lines().next().unwrap_or("")));
                let timestamp = now_ms();
                let line = format!(
                    "{{\"type\":\"user\",\"text\":{},\"time\":{{\"created\":{timestamp}}}}}\n",
                    serde_json::to_string(&text).unwrap_or_else(|_| "\"\"".into())
                );
                let _ = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(format!("{dir}/prompts"))
                    .and_then(|mut file| file.write_all(line.as_bytes()));
            }
            if mode == "steer" && steered {
                log(&format!("steered:{text}"));
            } else {
                log(&format!("prompt:{text}"));
            }
            print!("{{\"data\":{{\"id\":\"msg_1\"}}}}");
        }
        ("GET", p) if p.ends_with("/message") => {
            // The user message this service last received, as JSON, so the
            // listing can carry its mailbox marker like the real one.
            let state_dir = env::var("ANYAGENTS_FAKE_STATE").unwrap_or_default();
            if mode == "inbox" {
                // Every prompt the session took, and nothing else: the turn is
                // still open, so there is no answer and no idle frame yet.
                let listed =
                    std::fs::read_to_string(format!("{state_dir}/prompts")).unwrap_or_default();
                print!(
                    "{{\"data\":[{}]}}",
                    listed
                        .lines()
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                        .join(",")
                );
                return;
            }
            let prompt =
                std::fs::read_to_string(format!("{state_dir}/last-prompt")).unwrap_or_default();
            let prompt = serde_json::to_string(&prompt).unwrap_or_else(|_| "\"\"".into());
            // A finished turn from earlier in the session, with its own answer
            // and idle. It must not close the turn being waited for.
            if mode == "history" {
                print!(
                    "{{\"data\":[{{\"type\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"an old answer\"}}],\"finish\":\"stop\",\"time\":{{\"created\":1}}}},{{\"type\":\"idle\",\"outcome\":\"succeeded\",\"time\":{{\"created\":2}}}}]}}"
                );
                return;
            }
            // A session holding two finished turns: only the newest is the
            // answer, never both concatenated.
            if mode == "two-turns" {
                // The older turn is from long ago; the newest is this one.
                let now = now_ms();
                print!(
                    "{{\"data\":[{{\"type\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"second answer\"}}],\"finish\":\"stop\",\"time\":{{\"created\":{}}}}},{{\"type\":\"user\",\"text\":{prompt},\"time\":{{\"created\":{}}}}},{{\"type\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"first answer\"}}],\"finish\":\"stop\",\"time\":{{\"created\":{}}}}},{{\"type\":\"idle\",\"outcome\":\"succeeded\",\"time\":{{\"created\":{}}}}}]}}",
                    now,
                    now - 1,
                    now - 60_000,
                    now - 59_000
                );
                return;
            }
            // An answer with the model's thinking beside it: only the `text`
            // part is recorded, never the `reasoning` part.
            if mode == "reasoning" {
                let now = now_ms();
                print!(
                    "{{\"data\":[{{\"type\":\"assistant\",\"content\":[{{\"type\":\"reasoning\",\"text\":\"I should think carefully\"}},{{\"type\":\"text\",\"text\":\"the real answer\"}}],\"finish\":\"stop\",\"time\":{{\"created\":{}}}}},{{\"type\":\"user\",\"text\":{prompt},\"time\":{{\"created\":{}}}}},{{\"type\":\"idle\",\"outcome\":\"succeeded\",\"time\":{{\"created\":{}}}}}]}}",
                    now + 1,
                    now,
                    now + 2
                );
                return;
            }
            if mode == "hold" {
                print!("{{\"data\":[]}}");
                return;
            }
            if mode == "failed" {
                print!("{{\"data\":[{{\"type\":\"idle\",\"outcome\":\"failed\"}}]}}");
                return;
            }
            let text = scripted_answer();
            // One clock reading for the whole listing: the answer is created
            // after the prompt it answers, and the idle after both, so a fast
            // test can never read two messages out of order.
            let now = now_ms();
            let answer = format!(
                "{{\"type\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"{text}\"}}],\"finish\":\"stop\",\"cost\":0.0001,\"time\":{{\"created\":{}}},\"tokens\":{{\"input\":11,\"output\":7,\"cache\":{{\"read\":3,\"write\":2}}}}}}",
                now + 1
            );
            // Newest first, as the real service lists them: the answer, the
            // user message carrying its mailbox marker, then the idle.
            print!(
                "{{\"data\":[{answer},{{\"type\":\"user\",\"text\":{prompt},\"time\":{{\"created\":{}}}}},{{\"type\":\"idle\",\"outcome\":\"succeeded\",\"time\":{{\"created\":{}}}}}]}}",
                now,
                now + 2
            );
        }
        _ => {
            print!("{{}}");
        }
    }
}

/// The answer the scripted service gives, from `ANYAGENTS_FAKE_ANSWER`.
fn scripted_answer() -> String {
    env::var("ANYAGENTS_FAKE_ANSWER").unwrap_or_else(|_| "pong from opencode".into())
}

/// The prompt's text, unescaped enough to log and to read the marker.
fn prompt_text(body: &str) -> String {
    let Some(at) = body.find("\"text\":\"") else {
        return String::new();
    };
    let rest = &body[at + 8..];
    let mut out = String::new();
    let mut chars = rest.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => break,
            '\\' => match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some(other) => out.push(other),
                None => break,
            },
            other => out.push(other),
        }
    }
    out
}

fn log(line: &str) {
    let Some(path) = env::var_os("ANYAGENTS_FAKE_LOG") else {
        return;
    };
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

/// Milliseconds since the Unix epoch, for the messages' creation time.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}
