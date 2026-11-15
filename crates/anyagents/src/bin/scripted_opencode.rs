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
//!
//! The service serves its event stream (`GET /api/event`), where every prompt
//! is announced as taken (`session.inbox.delivered`) under its own id.

use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::process::exit;
use std::thread;
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..]),
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
    let port = args
        .windows(2)
        .find(|pair| pair[0] == "--port")
        .expect("explicit port")[1]
        .parse::<u16>()
        .expect("port");
    assert_ne!(port, 0, "a stable port, not an ephemeral request");
    assert!(env::var_os("OPENCODE_PASSWORD").is_none());
    assert!(env::var_os("OPENCODE_SERVER_PASSWORD").is_none());
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
        .unwrap_or_else(|error| {
            log(&format!("bind failed:{port}:{error}"));
            exit(1);
        });
    std::thread::spawn(move || events(listener));
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
    let dir =
        env::var("ANYAGENTS_FAKE_STATE").unwrap_or_else(|_| env::temp_dir().display().to_string());
    let _ = dir;
    match (method, path) {
        ("GET", "/api/session") => {
            print!("{{\"data\":[{{\"id\":\"ses_fake\"}}]}}");
        }
        ("GET", p) if p.starts_with("/api/session/ses_") && p.contains("/message/") => {
            let session = p
                .trim_start_matches("/api/session/")
                .split('/')
                .next()
                .unwrap_or_default();
            let id = p.rsplit('/').next().unwrap_or_default();
            if was_delivered(endpoint, session, id) {
                print!("{}", serde_json::json!({"data": {"id": id}}));
            } else {
                eprintln!("HTTP 404 Not Found");
                print!("{{\"_tag\":\"MessageNotFoundError\",\"message\":\"Message not found\"}}");
                exit(1);
            }
        }
        ("GET", p) if p.starts_with("/api/session/ses_") && !p.contains("/message") => {
            print!("{{\"data\":{{\"id\":\"ses_fake\",\"title\":\"fake\"}}}}");
        }
        ("POST", "/api/session") => {
            let log_has_stale_404 = env::var_os("ANYAGENTS_FAKE_LOG")
                .and_then(|p| std::fs::read_to_string(p).ok())
                .is_some_and(|text| text.contains("api:POST /api/session/ses_stale/prompt"));
            let id = if log_has_stale_404 {
                "ses_recovered"
            } else {
                "ses_fake"
            };
            print!("{{\"data\":{{\"id\":\"{id}\"}}}}");
        }
        ("POST", p) if p.ends_with("/prompt") => {
            if p == "/api/session/ses_stale/prompt" {
                eprintln!("HTTP 404 Not Found");
                print!("{{\"error\":{{\"code\":404,\"message\":\"session not found\"}}}}");
                exit(1);
            }
            let steered = body.contains("\"delivery\":\"steer\"");
            let text = prompt_text(&body);
            if mode == "steer" && steered {
                log(&format!("steered:{text}"));
            } else {
                log(&format!("prompt:{text}"));
            }
            // The session takes the prompt at once and says so on its event
            // stream, under the prompt's own id.
            let id = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|request| request["id"].as_str().map(str::to_string))
                .unwrap_or_else(|| "msg_1".into());
            let session = p
                .trim_start_matches("/api/session/")
                .trim_end_matches("/prompt");
            announce(endpoint, session, &id);
            print!("{}", serde_json::json!({"data": {"id": id}}));
        }
        ("GET", p) if p.ends_with("/message") => {
            if mode == "hold" {
                print!("{{\"data\":[]}}");
                return;
            }
            if mode == "failed" {
                print!("{{\"data\":[{{\"type\":\"idle\",\"outcome\":\"failed\"}}]}}");
                return;
            }
            let text = scripted_answer();
            let answer = format!(
                "{{\"type\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"{text}\"}}],\"finish\":\"stop\",\"cost\":0.0001,\"time\":{{\"created\":{}}},\"tokens\":{{\"input\":11,\"output\":7,\"cache\":{{\"read\":3,\"write\":2}}}}}}",
                now_ms()
            );
            print!(
                "{{\"data\":[{answer},{{\"type\":\"idle\",\"outcome\":\"succeeded\",\"time\":{{\"created\":{}}}}}]}}",
                now_ms()
            );
        }
        _ => {
            print!("{{}}");
        }
    }
}

/// The service's event stream, `GET /api/event`: `server.connected`, then a
/// `session.inbox.delivered` for each input the session takes. A one-shot
/// `api` process announces the input it admitted with `POST
/// /fake/delivered/<session>/<id>`, since it is not the service itself.
fn events(listener: std::net::TcpListener) {
    use std::collections::HashSet;
    use std::io::{BufRead, BufReader};
    let state = std::sync::Arc::new(std::sync::Mutex::new((
        Vec::<std::net::TcpStream>::new(),
        HashSet::<(String, String)>::new(),
    )));
    for connection in listener.incoming() {
        let Ok(mut connection) = connection else {
            break;
        };
        let state = std::sync::Arc::clone(&state);
        thread::spawn(move || {
            let Ok(clone) = connection.try_clone() else {
                return;
            };
            let mut request = BufReader::new(clone);
            let mut line = String::new();
            if request.read_line(&mut line).is_err() {
                return;
            }
            let mut header = String::new();
            while request.read_line(&mut header).is_ok_and(|read| read > 2) {
                header.clear();
            }
            let mut words = line.split_whitespace();
            match (words.next(), words.next()) {
                (Some("GET"), Some("/api/event")) => {
                    let mut state = state.lock().unwrap();
                    if connection
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\ndata: {\"type\":\"server.connected\",\"data\":{}}\n\n",
                        )
                        .is_ok()
                    {
                        state.0.push(connection);
                    }
                }
                (Some("POST"), Some(path)) if path.starts_with("/fake/delivered/") => {
                    let mut parts = path.trim_start_matches("/fake/delivered/").split('/');
                    let session = parts.next().unwrap_or_default().to_string();
                    let id = parts.next().unwrap_or_default().to_string();
                    let event = serde_json::json!({
                        "type": "session.inbox.delivered",
                        "data": {"sessionID": &session, "inboxID": &id},
                    });
                    let frame = format!("data: {event}\n\n");
                    let mut state = state.lock().unwrap();
                    state.1.insert((session, id));
                    state
                        .0
                        .retain_mut(|stream| stream.write_all(frame.as_bytes()).is_ok());
                    let _ = connection.write_all(b"HTTP/1.1 204 No Content\r\n\r\n");
                }
                (Some("GET"), Some(path)) if path.starts_with("/fake/delivered/") => {
                    let mut parts = path.trim_start_matches("/fake/delivered/").split('/');
                    let key = (
                        parts.next().unwrap_or_default().to_string(),
                        parts.next().unwrap_or_default().to_string(),
                    );
                    let found = state.lock().unwrap().1.contains(&key);
                    let status = if found {
                        b"HTTP/1.1 204 No Content\r\n\r\n".as_slice()
                    } else {
                        b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n".as_slice()
                    };
                    let _ = connection.write_all(status);
                }
                _ => {
                    let _ = connection
                        .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n");
                }
            }
        });
    }
}

/// Tell the service's event stream that its session took input `id`.
fn announce(endpoint: &str, session: &str, id: &str) {
    use std::io::Read as _;
    let address = endpoint.trim_start_matches("http://");
    let Ok(mut connection) = std::net::TcpStream::connect(address) else {
        return;
    };
    let _ = write!(
        connection,
        "POST /fake/delivered/{session}/{id} HTTP/1.1\r\nhost: {address}\r\n\r\n"
    );
    let mut answer = [0u8; 64];
    let _ = connection.read(&mut answer);
}

fn was_delivered(endpoint: &str, session: &str, id: &str) -> bool {
    use std::io::Read as _;
    let address = endpoint.trim_start_matches("http://");
    let Ok(mut connection) = std::net::TcpStream::connect(address) else {
        return false;
    };
    let _ = write!(
        connection,
        "GET /fake/delivered/{session}/{id} HTTP/1.1\r\nhost: {address}\r\n\r\n"
    );
    let mut answer = [0u8; 64];
    let n = connection.read(&mut answer).unwrap_or(0);
    std::str::from_utf8(&answer[..n]).is_ok_and(|head| head.starts_with("HTTP/1.1 204"))
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
