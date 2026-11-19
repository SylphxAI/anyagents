use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

fn write_executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

type MockCallbackServer = (
    String,
    Arc<Mutex<Vec<(String, Value)>>>,
    std::thread::JoinHandle<()>,
);

fn spawn_mock_callback_server(fail_first_complete_once: bool) -> MockCallbackServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let events = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let events_srv = Arc::clone(&events);

    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut injected_503 = false;
        let mut saw_terminal = false;
        while Instant::now() < deadline && !saw_terminal {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut content_len = 0usize;
                    let mut auth = String::new();
                    let mut line = String::new();
                    loop {
                        line.clear();
                        let Ok(n) = reader.read_line(&mut line) else {
                            break;
                        };
                        if n == 0 || line == "\r\n" || line == "\n" {
                            break;
                        }
                        let lower = line.to_ascii_lowercase();
                        if let Some(v) = lower.strip_prefix("content-length:") {
                            content_len = v.trim().parse().unwrap_or(0);
                        } else if lower.starts_with("authorization:") {
                            auth = line.split_once(':').unwrap().1.trim().to_string();
                        }
                    }
                    let mut body = vec![0u8; content_len];
                    if reader.read_exact(&mut body).is_err() {
                        continue;
                    }
                    let Ok(val) = serde_json::from_slice::<Value>(&body) else {
                        continue;
                    };
                    let ev_type = val.get("event_type").and_then(Value::as_str).unwrap_or("");
                    if fail_first_complete_once && ev_type == "turn_complete" && !injected_503 {
                        injected_503 = true;
                        let _ = stream.write_all(
                            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 10\r\n\r\ncold-start",
                        );
                        continue;
                    }
                    events_srv.lock().unwrap().push((auth, val.clone()));
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}",
                    );
                    if matches!(ev_type, "turn_complete" | "turn_failed") {
                        saw_terminal = true;
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });

    (
        format!("http://{addr}/v1/internal/guest/events"),
        events,
        handle,
    )
}

#[test]
fn detached_guest_turn_returns_immediately_and_pushes_heartbeat_and_completion_through_503() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    // Fake Claude CLI that takes ~120ms so at least one periodic heartbeat fires.
    let fake_claude = tmp.path().join("fake-claude");
    write_executable(
        &fake_claude,
        r#"#!/bin/sh
echo '{"type":"system","subtype":"init","session_id":"detached-sess-7","claude_code_version":"1.0","peer":"claude-code 1.0"}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"user"'*)
      uuid=$(printf '%s\n' "$line" | sed -n 's/.*"uuid":"\([^"]*\)".*/\1/p')
      printf '{"type":"user","isReplay":true,"uuid":"%s","message":{"role":"user","content":[{"type":"text","text":"ping"}]}}\n' "$uuid"
      sleep 0.15
      printf '{"type":"assistant","session_id":"detached-sess-7","message":{"role":"assistant","content":[{"type":"text","text":"detached serverless ok"}]}}\n'
      printf '{"type":"result","subtype":"success","is_error":false,"session_id":"detached-sess-7","user_message_uuids":["%s"],"result":"detached serverless ok","usage":{"input_tokens":21,"output_tokens":9},"total_cost_usd":0.02}\n' "$uuid"
      ;;
  esac
done
"#,
    );

    let (callback_url, events, server_handle) = spawn_mock_callback_server(true);
    let guest_bin = PathBuf::from(env!("CARGO_BIN_EXE_anyagents-guest"));

    let started = Instant::now();
    let mut child = Command::new(&guest_bin)
        .env("ANYAGENTS_GUEST_CALLBACK_BACKOFF_MS", "20")
        .args([
            "turn",
            "--detached",
            "--turn-id",
            "turn_detached_1",
            "--agent-id",
            "agt_01testdetached",
            "--harness",
            "claude",
            "--program",
            fake_claude.to_str().unwrap(),
            "--prompt-stdin",
            "--callback-url",
            &callback_url,
            "--callback-token",
            "rtk_detached_secret_token",
            "--heartbeat-interval-ms",
            "40",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--workdir",
            work_dir.to_str().unwrap(),
            "--",
            "--guest-bare",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn anyagents-guest turn --detached");

    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"Run detached turn from stdin")
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let elapsed = started.elapsed();

    assert!(
        out.status.success(),
        "detached parent failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Parent MUST exit quickly without waiting for the 150ms+ child harness turn!
    assert!(
        elapsed < Duration::from_secs(2),
        "detached parent took too long: {elapsed:?}"
    );

    let accepted: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid accepted JSON ({e}): stdout={}",
            String::from_utf8_lossy(&out.stdout)
        )
    });
    assert_eq!(accepted["status"], "accepted");
    assert_eq!(accepted["turn_id"], "turn_detached_1");
    assert!(accepted["pid"].as_u64().unwrap_or(0) > 0);

    server_handle.join().unwrap();

    let got = events.lock().unwrap();
    assert!(
        got.iter()
            .all(|(auth, _)| auth == "Bearer rtk_detached_secret_token"),
        "every callback must send Bearer token: {got:?}"
    );
    let heartbeats: Vec<&Value> = got
        .iter()
        .map(|(_, v)| v)
        .filter(|v| v["event_type"] == "heartbeat")
        .collect();
    assert!(
        !heartbeats.is_empty(),
        "expected >=1 heartbeat callback: {got:?}"
    );
    assert_eq!(heartbeats[0]["turn_id"], "turn_detached_1");
    assert_eq!(heartbeats[0]["active_turns"], 1);
    assert_eq!(heartbeats[0]["busy"], true);

    let completed = got
        .iter()
        .map(|(_, v)| v)
        .find(|v| v["event_type"] == "turn_complete")
        .expect("expected turn_complete callback after 503 retry");
    assert_eq!(completed["turn_id"], "turn_detached_1");
    assert_eq!(completed["agent_id"], "agt_01testdetached");
    assert_eq!(completed["session_id"], "detached-sess-7");
    assert_eq!(completed["result_text"], "detached serverless ok");
    assert_eq!(completed["usage"]["input_tokens"], 21);
    assert_eq!(completed["usage"]["output_tokens"], 9);

    // Log file was created under `<state-dir>/turns/<turn-id>.log`.
    assert!(state_dir.join("turns/turn_detached_1.log").is_file());
    assert!(
        state_dir
            .join("turns/turn_detached_1.result.json")
            .is_file()
    );
    // Wait briefly for child exit cleanup of the temporary token file.
    let clean_deadline = Instant::now() + Duration::from_secs(2);
    while state_dir.join("turns/turn_detached_1.token").exists() && Instant::now() < clean_deadline
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !state_dir.join("turns/turn_detached_1.token").exists(),
        "temporary callback token file should be cleaned up on completion"
    );
}

#[test]
fn detached_guest_turn_drives_gemini_harness() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    let fake_gemini = tmp.path().join("fake-gemini");
    write_executable(
        &fake_gemini,
        r#"#!/bin/sh
echo '{"type":"init","session_id":"gem-sess-9"}'
echo '{"type":"message","role":"assistant","content":"hello from gemini"}'
echo '{"type":"result","status":"success","session_id":"gem-sess-9","result":"hello from gemini","usage":{"input_tokens":14,"output_tokens":6}}'
"#,
    );

    let prompt_file = tmp.path().join("prompt.txt");
    std::fs::write(&prompt_file, "Say hi from gemini").unwrap();

    let (callback_url, events, server_handle) = spawn_mock_callback_server(false);
    let guest_bin = PathBuf::from(env!("CARGO_BIN_EXE_anyagents-guest"));

    let out = Command::new(&guest_bin)
        .args([
            "turn",
            "--detached",
            "--turn-id",
            "turn_gemini_1",
            "--agent-id",
            "agt_01testgemini",
            "--harness",
            "gemini",
            "--program",
            fake_gemini.to_str().unwrap(),
            "--prompt-file",
            prompt_file.to_str().unwrap(),
            "--callback-url",
            &callback_url,
            "--callback-token",
            "rtk_gemini_secret",
            "--heartbeat-interval-ms",
            "50",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--workdir",
            work_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "gemini detached turn failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    server_handle.join().unwrap();

    let got = events.lock().unwrap();
    let completed = got
        .iter()
        .map(|(_, v)| v)
        .find(|v| v["event_type"] == "turn_complete")
        .expect("expected gemini turn_complete callback");
    assert_eq!(completed["harness"], "gemini");
    assert_eq!(completed["session_id"], "gem-sess-9");
    assert_eq!(completed["result_text"], "hello from gemini");
    assert_eq!(completed["usage"]["input_tokens"], 14);
    assert_eq!(completed["usage"]["output_tokens"], 6);
}
