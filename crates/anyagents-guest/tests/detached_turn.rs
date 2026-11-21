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

#[cfg(target_os = "linux")]
#[test]
fn detached_spiron_resume_retires_live_and_orphaned_session_holders() {
    const SESSION: &str = "test-resume-session";
    // Reuse the real socket transport and a real advisory session lock. An
    // extra process in the harness group inherits the lock and ignores TERM.
    // It must also exit before the next Spiron accepts the same session.
    let fixture = r#"#!/usr/bin/env python3
import fcntl, json, os, pathlib, signal, socket, sys

state = pathlib.Path(sys.argv[sys.argv.index('--state-dir') + 1])
session = sys.argv[sys.argv.index('--session') + 1]
lock = (state / (session + '.lock')).open('w')
try:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    sys.exit('session_in_use: another process serves session ' + session)

if not (state / 'resume').exists():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child = os.fork()
    if child == 0:
        while True:
            signal.pause()
    tmp = state / 'holder.tmp'
    tmp.write_text(f'{os.getpid()} {child}')
    tmp.rename(state / 'holder')
    while True:
        signal.pause()

listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
listener.bind(sys.argv[sys.argv.index('--listen') + 1])
listener.listen()
seq = 0
while True:
    conn, _ = listener.accept()
    with conn, conn.makefile() as stream:
        for line in stream:
            msg = json.loads(line)
            seq += 1
            reply = dict(seq=seq, session_id=session, request_id=msg['request_id'])
            if msg['type'] == 'attach':
                reply.update(type='ready', protocol='spiron.headless/v2', features=['turn-tool-context-v1'])
            elif msg['type'] == 'prompt':
                accepted = dict(reply, type='accepted', mode='turn')
                conn.sendall((json.dumps(accepted) + '\n').encode())
                seq += 1
                reply.update(seq=seq, type='result', status='completed', is_error=False,
                             duplicate=False, error=None, tool_context_bound=True, final_text='resumed',
                             consumed_request_ids=[msg['request_id']], undelivered_steer_ids=[])
            elif msg['type'] == 'stop':
                sys.exit(0)
            else:
                continue
            conn.sendall((json.dumps(reply) + '\n').encode())
"#;

    fn wait_for<T>(mut observe: impl FnMut() -> Option<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(value) = observe() {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "fixture did not reach its barrier"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn exited(pid: i32) -> bool {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat
                .rsplit_once(')')
                .unwrap()
                .1
                .trim_start()
                .starts_with('Z'),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(e) => panic!("read process state: {e}"),
        }
    }

    struct Cleanup(Vec<i32>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            for pid in self.0.drain(..) {
                if !exited(pid) {
                    // SAFETY: these are fixture processes we have confirmed alive.
                    unsafe { libc::kill(pid, libc::SIGKILL) };
                }
            }
        }
    }

    for crash_worker in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        let work = tmp.path().join("workspace");
        let fake = tmp.path().join("fake-spiron");
        write_executable(&fake, fixture);
        let (url, events, server) = spawn_mock_callback_server(false);
        let launch = |turn| {
            let out = Command::new(env!("CARGO_BIN_EXE_anyagents-guest"))
                .env_remove("ANYAGENTS_GUEST_SPIRON_STDIO")
                .args([
                    "turn",
                    "--detached",
                    "--agent-id",
                    "test-agent",
                    "--turn-id",
                    turn,
                    "--harness",
                    "spiron",
                    "--program",
                    fake.to_str().unwrap(),
                    "--state-dir",
                    state.to_str().unwrap(),
                    "--workdir",
                    work.to_str().unwrap(),
                    "--session",
                    SESSION,
                    "--prompt",
                    "hello",
                    "--heartbeat-interval-ms",
                    "0",
                    "--callback-url",
                    &url,
                    "--callback-token",
                    "test-token",
                ])
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let json: Value = serde_json::from_slice(&out.stdout).unwrap();
            i32::try_from(json["pid"].as_i64().unwrap()).unwrap()
        };

        let worker = launch("turn_prior");
        let mut cleanup = Cleanup(vec![worker]);
        let holders = wait_for(|| std::fs::read_to_string(state.join("holder")).ok());
        let holders: Vec<i32> = holders
            .split_whitespace()
            .map(|s| s.parse().unwrap())
            .collect();
        assert_eq!(holders.len(), 2);
        cleanup.0.extend_from_slice(&holders);
        // The first turn sent only its initial heartbeat and is now hung in
        // harness startup. Dispatch a replacement as after a coordinator timeout.
        if crash_worker {
            // SAFETY: the worker pid was obtained from the local launcher and is live.
            assert_eq!(unsafe { libc::kill(worker, libc::SIGKILL) }, 0);
            wait_for(|| exited(worker).then_some(()));
            assert!(holders.iter().all(|&pid| !exited(pid)));
        }
        std::fs::write(state.join("resume"), "").unwrap();
        cleanup.0.push(launch("turn_resumed"));
        server.join().unwrap();
        let events = events.lock().unwrap();
        assert!(
            events.iter().any(|(_, event)| {
                event["turn_id"] == "turn_resumed"
                    && event["event_type"] == "turn_complete"
                    && event["session_id"] == SESSION
                    && event["result_text"] == "resumed"
            }),
            "replacement did not complete (crash_worker={crash_worker}): {events:?}\nworker: {}\nharness: {}",
            std::fs::read_to_string(state.join("turns/turn_resumed.log")).unwrap_or_default(),
            std::fs::read_to_string(state.join("spiron.log")).unwrap_or_default(),
        );
        assert!(exited(worker));
        assert!(holders.iter().all(|&pid| exited(pid)));
    }
}
