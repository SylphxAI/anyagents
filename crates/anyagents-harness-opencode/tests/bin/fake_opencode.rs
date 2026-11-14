//! A scripted `opencode` for the adapter tests.
//!
//! It speaks the same surface the real CLI does: `opencode serve` starts a
//! service that answers nothing on stdin, and `opencode api <METHOD> <path>`
//! prints the answer's body; a non-2xx answer also prints `HTTP <status>
//! <text>` on stderr and exits 1, its body an error tagged `_tag`. Every call
//! is appended to `ANYAGENTS_FAKE_LOG`, so a test can read what the adapter
//! asked for.
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
//!
//! - `applied`: every prompt names a line an earlier process wrote and the
//!   session already consumed; `parked`: one it admitted and has not yet.
//! - `prompt-503-once`: the first prompt meets the readiness gate (503
//!   `{"code":"service_starting"}`, nothing admitted); `prompt-502-once`: it
//!   fails without saying whether it was admitted.
//! - `prompt-400` / `prompt-409` / `prompt-401` / `prompt-500` /
//!   `prompt-503-failed`: every prompt fails so, with the body OpenCode 2.1.4
//!   gives (`InvalidRequestError`, `ConflictError`, …).
//! - `v1`: `serve` announces itself as OpenCode 1.19.5 does (`opencode server
//!   listening on …`, no password) and `api` is not a command; `info-404` and
//!   `info-1x`: a service whose `GET /api/info` answers 404, or names 1.19.5.
//!
//! `ANYAGENTS_FAKE_DELAY_MS` holds every prompt's answer back that long.
//!
//! `POST /api/experimental/session/{id}/wait` answers at once (the turn is
//! over) except in `hold`, `inbox` and `interrupt-failed`, where the turn never
//! ends: it answers only when the service stops, as the real one does.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::exit;
use std::thread;
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..]),
        // OpenCode 1.x has no `api` command.
        Some("api") if env::var("ANYAGENTS_FAKE_MODE").as_deref() == Ok("v1") => {
            log("api:unknown command");
            exit(1)
        }
        Some("api") => {
            // A missing explicit endpoint would discover a managed service.
            // Refuse that in the fixture instead of masking isolation bugs.
            let mut rest: Vec<String> = args[1..].to_vec();
            let Some(at) = rest.iter().position(|arg| arg == "--server") else {
                panic!("refusing managed service discovery");
            };
            let endpoint = rest.get(at + 1).expect("server URL").clone();
            log(&format!("server:{endpoint}"));
            rest.drain(at..(at + 2).min(rest.len()));
            api(&rest, &endpoint)
        }
        _ => exit(2),
    }
}

/// Hold the foreground like a service, forever, announcing the address and
/// password exactly as the real CLI prints them. The adapter kills it.
fn serve(args: &[String]) {
    log(&format!("serve:{}", args.join(" ")));
    log(&format!(
        "config-root:{}",
        env::var("OPENCODE_CONFIG_DIR").unwrap_or_default()
    ));
    if env::var("ANYAGENTS_FAKE_MODE").as_deref() == Ok("no-announcement") {
        return;
    }
    // OpenCode 1.19.5: a warning when no password is set, its address with
    // its name in front, and never a password.
    if env::var("ANYAGENTS_FAKE_MODE").as_deref() == Ok("v1") {
        let port = args
            .windows(2)
            .find(|pair| pair[0] == "--port")
            .map_or("4096", |pair| pair[1].as_str());
        println!("Warning: OPENCODE_SERVER_PASSWORD is not set; server is unsecured.");
        println!("opencode server listening on http://127.0.0.1:{port}");
        let _ = std::io::stdout().flush();
        loop {
            thread::sleep(Duration::from_secs(3600));
        }
    }
    let port = args
        .windows(2)
        .find(|pair| pair[0] == "--port")
        .expect("explicit port")[1]
        .parse::<u16>()
        .expect("port");
    assert_ne!(port, 0, "a stable port, not an ephemeral request");
    assert!(env::var_os("OPENCODE_PASSWORD").is_none());
    assert!(env::var_os("OPENCODE_SERVER_PASSWORD").is_none());
    let mode = env::var("ANYAGENTS_FAKE_MODE").unwrap_or_default();
    let mut collision_holder = None;
    let mut witnessed_bind_error = None;
    if matches!(mode.as_str(), "bind-race" | "transient-bind-race") {
        let state = std::path::PathBuf::from(env::var("ANYAGENTS_FAKE_STATE").unwrap());
        let announced = state.join("first-serve-port");
        if !announced.exists() {
            // Own the collision before publishing the barrier. Asking the test
            // to bind after handoff races every other process on the runner.
            // If one already won, retain that acquisition-time error instead.
            match std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)) {
                Ok(holder) => collision_holder = Some(holder),
                Err(error) => witnessed_bind_error = Some(error),
            }
            let pending = state.join("first-serve-port.new");
            std::fs::write(&pending, port.to_string()).unwrap();
            std::fs::rename(pending, &announced).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !state.join("collision-bound").exists() {
                assert!(std::time::Instant::now() < deadline, "collision barrier");
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
    let binding = match witnessed_bind_error {
        Some(error) => Err(error),
        None => std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)),
    };
    let listener = binding.unwrap_or_else(|error| {
        log(&format!("bind failed:{port}:{error}"));
        eprintln!("{error}");
        if mode == "transient-bind-race" {
            // The parent acknowledges this marker before allowing exit:
            // the adapter's later probe cannot see our former holder.
            drop(collision_holder.take());
            let state = std::path::PathBuf::from(env::var("ANYAGENTS_FAKE_STATE").unwrap());
            std::fs::write(state.join("collision-reported"), "reported").unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !state.join("collision-released").exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "release collision barrier"
                );
                thread::sleep(Duration::from_millis(10));
            }
        }
        exit(1);
    });
    std::thread::spawn(move || {
        for connection in listener.incoming() {
            if connection.is_err() {
                break;
            }
        }
    });
    println!("server listening on http://127.0.0.1:{port}");
    println!("server password scripted-password");
    let _ = std::io::stdout().flush();
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

fn api(args: &[String], endpoint: &str) {
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
    match (method, path) {
        ("GET", "/api/info") if mode == "info-404" => {
            // No such route: an older service.
            eprintln!("HTTP 404 Not Found");
            exit(1);
        }
        ("GET", "/api/info") if mode == "info-1x" => {
            print!("{{\"version\":\"1.19.5\"}}");
        }
        ("GET", "/api/info") => {
            print!(
                "{{\"version\":\"2.1.4\",\"pid\":{},\"urls\":[\"{endpoint}\"],\"paths\":{{\"tmp\":\"/tmp\"}},\"capabilities\":{{}}}}",
                std::process::id()
            );
        }
        ("GET", "/api/session") => {
            print!("{{\"data\":[{{\"id\":\"ses_fake\"}}]}}");
        }
        ("GET", "/api/session/active") => {
            print!("{{\"data\":{{}}}}");
        }
        ("POST", p) if p.starts_with("/api/experimental/session/") && p.ends_with("/wait") => {
            // A turn that never ends answers only when the service stops.
            if matches!(mode.as_str(), "hold" | "inbox" | "interrupt-failed") {
                let address = endpoint.trim_start_matches("http://");
                while std::net::TcpStream::connect(address).is_ok() {
                    thread::sleep(Duration::from_millis(50));
                }
                eprintln!("error: Unable to connect. Is the computer able to access the url?");
                exit(1);
            }
            // 204: the session is idle, and the CLI prints nothing.
        }
        ("GET", p) if p.starts_with("/api/session/ses_") && p.contains("/message/") => {
            // `applied`: the session took the line into its conversation.
            if mode == "applied" {
                let id = p.rsplit('/').next().unwrap_or_default();
                print!(
                    "{{\"data\":{{\"id\":\"{id}\",\"type\":\"user\",\"text\":\"\",\"time\":{{\"created\":1}}}}}}"
                );
                return;
            }
            fail(
                404,
                "Not Found",
                "MessageNotFoundError",
                "Message not found",
            );
        }
        ("GET", p) if p.starts_with("/api/session/ses_") && !p.contains("/message") => {
            if mode == "session-unavailable" {
                fail(
                    503,
                    "Service Unavailable",
                    "ServiceUnavailableError",
                    "service unavailable",
                );
            }
            // `gone` mode: the stored session no longer exists in this store.
            if mode == "gone" {
                fail(
                    404,
                    "Not Found",
                    "SessionNotFoundError",
                    "Session not found",
                );
            }
            // The session's running totals, when the test wrote them: they
            // outlive any one process, as a real session's do.
            let mut session = serde_json::json!({"id": "ses_fake", "title": "fake"});
            let usage = env::var("ANYAGENTS_FAKE_STATE")
                .ok()
                .and_then(|dir| fs::read_to_string(Path::new(&dir).join("session-usage.json")).ok())
                .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
            if let (Some(session), Some(serde_json::Value::Object(usage))) =
                (session.as_object_mut(), usage)
            {
                session.extend(usage);
            }
            print!("{}", serde_json::json!({ "data": session }));
        }
        ("POST", "/api/session") => {
            let id = if mode == "gone" || mode.starts_with("prompt-404") {
                "ses_fresh"
            } else {
                "ses_fake"
            };
            print!("{{\"data\":{{\"id\":\"{id}\"}}}}");
        }
        ("POST", p) if p.ends_with("/prompt") => {
            let dir = env::var("ANYAGENTS_FAKE_STATE").unwrap_or_default();
            if mode == "prompt-500" {
                fail(
                    500,
                    "Internal Server Error",
                    "UnknownError",
                    "internal server error",
                );
            }
            if mode == "prompt-400" {
                fail(
                    400,
                    "Bad Request",
                    "InvalidRequestError",
                    "Skill not found: missing",
                );
            }
            if mode == "prompt-409" {
                let request: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                let id = request["id"].as_str().unwrap_or_default();
                fail(
                    409,
                    "Conflict",
                    "ConflictError",
                    &format!("Prompt message ID conflicts with an existing durable record: {id}"),
                );
            }
            if mode == "prompt-503-failed" {
                fail_with(
                    503,
                    "Service Unavailable",
                    &serde_json::json!({
                        "code": "service_failed",
                        "message": "The background service could not start.",
                        "action": "Run `opencode service restart` after checking the service logs.",
                    }),
                );
            }
            if mode == "prompt-401" {
                fail(401, "Unauthorized", "UnauthorizedError", "Unauthorized");
            }
            if mode == "prompt-404-always" {
                fail(
                    404,
                    "Not Found",
                    "SessionNotFoundError",
                    "Session not found",
                );
            }
            if mode == "prompt-404" && p == "/api/session/ses_fake/prompt" {
                fail(
                    404,
                    "Not Found",
                    "SessionNotFoundError",
                    "Session not found",
                );
            }
            // A 404 whose body says nothing still names the session gone.
            if mode == "prompt-404-stderr" && p == "/api/session/ses_fake/prompt" {
                eprintln!("HTTP 404 Not Found");
                exit(1);
            }
            // The first prompt fails once: the readiness gate turns it away,
            // or an outcome does not say whether the line was admitted.
            if matches!(mode.as_str(), "prompt-503-once" | "prompt-502-once") {
                let marker = format!("{dir}/failed-once");
                if !std::path::Path::new(&marker).exists() {
                    let _ = std::fs::write(&marker, "1");
                    if mode == "prompt-503-once" {
                        fail_with(
                            503,
                            "Service Unavailable",
                            &serde_json::json!({"code": "service_starting"}),
                        );
                    }
                    fail(502, "Bad Gateway", "UnknownError", "bad gateway");
                }
            }
            if let Some(delay) = env::var("ANYAGENTS_FAKE_DELAY_MS")
                .ok()
                .and_then(|delay| delay.parse().ok())
            {
                thread::sleep(Duration::from_millis(delay));
            }
            if mode == "prompt-404-after-first" && p == "/api/session/ses_fake/prompt" {
                let first_marker = format!("{dir}/first-prompt-delivered");
                if std::path::Path::new(&first_marker).exists() {
                    fail(
                        404,
                        "Not Found",
                        "SessionNotFoundError",
                        "Session not found",
                    );
                }
                if !dir.is_empty() {
                    let _ = std::fs::write(&first_marker, "1");
                }
            }
            let request: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            let steered = request["delivery"] == "steer";
            let text = request["text"].as_str().unwrap_or_default().to_string();
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
            // The inbox record the service admitted, under the prompt's own id.
            // `applied` and `parked`: an earlier process wrote this id, so the
            // service answers with the record it already holds.
            let created = if matches!(mode.as_str(), "applied" | "parked") {
                1
            } else {
                now_ms()
            };
            let session = p
                .trim_start_matches("/api/session/")
                .trim_end_matches("/prompt");
            let record = serde_json::json!({
                "id": request["id"].as_str().unwrap_or("msg_minted"),
                "sessionID": session,
                "time": {"created": created},
                "type": "user",
                "payload": {"text": text},
                "delivery": request["delivery"],
            });
            print!("{}", serde_json::json!({ "data": record }));
        }
        ("POST", p) if p.ends_with("/interrupt") => {
            if mode == "interrupt-failed" {
                fail(
                    503,
                    "Service Unavailable",
                    "ServiceUnavailableError",
                    "interruption refused",
                );
            }
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

/// Answer the way `opencode api` reports a non-2xx answer: the error's body on
/// stdout, `HTTP <status> <text>` on stderr, exit status 1.
fn fail(status: u16, text: &str, tag: &str, message: &str) -> ! {
    fail_with(
        status,
        text,
        &serde_json::json!({ "_tag": tag, "message": message }),
    )
}

/// The same, with the body as the service writes it.
fn fail_with(status: u16, text: &str, body: &serde_json::Value) -> ! {
    print!("{body}");
    eprintln!("HTTP {status} {text}");
    exit(1)
}

/// The answer the scripted service gives, from `ANYAGENTS_FAKE_ANSWER`.
fn scripted_answer() -> String {
    env::var("ANYAGENTS_FAKE_ANSWER").unwrap_or_else(|_| "pong from opencode".into())
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
