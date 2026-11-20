#![cfg(unix)]

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_guest::steer::{STEER_DIR_SUFFIX, SteerEnvelope, steer_filename};
use anyagents_harness::{Context, OutboundMessage};
use serde_json::Value;

fn write_executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

type CallbackEvents = Arc<Mutex<Vec<(String, Value)>>>;

struct MockCallbackServer {
    url: String,
    events: CallbackEvents,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl MockCallbackServer {
    fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let events = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
        let events_srv = Arc::clone(&events);

        let handle = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(35);
            let mut saw_terminal = false;
            while Instant::now() < deadline && !saw_terminal {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
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

        Self {
            url: format!("http://{addr}/v1/internal/guest/events"),
            events,
            handle: Some(handle),
        }
    }

    fn join(mut self) -> Vec<(String, Value)> {
        if let Some(h) = self.handle.take() {
            h.join().unwrap();
        }
        self.events.lock().unwrap().clone()
    }
}

fn wait_for_file(
    path: &Path,
    timeout: Duration,
    label: &str,
    events: Option<&CallbackEvents>,
    state_dir: Option<&Path>,
    turn_id: Option<&str>,
) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        if let Some(ev) = events {
            let guard = ev.lock().unwrap();
            if guard
                .iter()
                .any(|(_, v)| v.get("event_type").and_then(Value::as_str) == Some("turn_failed"))
            {
                let log = state_dir
                    .zip(turn_id)
                    .and_then(|(sd, tid)| {
                        std::fs::read_to_string(sd.join(format!("turns/{tid}.log"))).ok()
                    })
                    .unwrap_or_default();
                panic!(
                    "turn failed while waiting for {label} ({}).\nevents={:?}\nguest_log={log}",
                    path.display(),
                    *guard
                );
            }
        }
        if Instant::now() >= deadline {
            let ev_dump = events
                .map(|ev| format!("{:?}", *ev.lock().unwrap()))
                .unwrap_or_default();
            let log = state_dir
                .zip(turn_id)
                .and_then(|(sd, tid)| {
                    std::fs::read_to_string(sd.join(format!("turns/{tid}.log"))).ok()
                })
                .unwrap_or_default();
            panic!(
                "timed out waiting for {label} ({}).\nevents={ev_dump}\nguest_log={log}",
                path.display()
            );
        }
        std::thread::sleep(Duration::from_millis(15));
    }
}

fn wait_for_event(events: &CallbackEvents, event_type: &str, timeout: Duration, label: &str) {
    let deadline = Instant::now() + timeout;
    loop {
        if events
            .lock()
            .unwrap()
            .iter()
            .any(|(_, v)| v.get("event_type").and_then(Value::as_str) == Some(event_type))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for callback event `{event_type}` ({label}); seen={:?}",
            events.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(15));
    }
}

fn drop_atomic_steer_file(
    state_dir: &Path,
    turn_id: &str,
    message_id: &str,
    seq: i64,
    text: &str,
) -> PathBuf {
    let spool_dir = state_dir
        .join("turns")
        .join(format!("{turn_id}{STEER_DIR_SUFFIX}"));
    std::fs::create_dir_all(&spool_dir).unwrap();
    let envelope = SteerEnvelope {
        turn_id: turn_id.to_string(),
        message_id: message_id.to_string(),
        seq,
        outbound: OutboundMessage {
            id: message_id.to_string(),
            seq,
            text: text.to_string(),
            sender: Some("user:tester".to_string()),
            reply_to: None,
            source_time_ms: Some(1_700_000_000_000),
            context: Context::default(),
        },
        submitted_at_ms: Some(1_700_000_000_000),
    };
    let filename = steer_filename(seq, message_id);
    let tmp = spool_dir.join(format!(".tmp.{filename}"));
    let final_path = spool_dir.join(&filename);
    std::fs::write(&tmp, serde_json::to_vec(&envelope).unwrap()).unwrap();
    std::fs::rename(&tmp, &final_path).unwrap();
    final_path
}

#[allow(clippy::too_many_arguments)]
fn spawn_detached_guest_turn(
    harness: &str,
    program: &Path,
    turn_id: &str,
    agent_id: &str,
    callback_url: &str,
    callback_token: &str,
    state_dir: &Path,
    work_dir: &Path,
    prompt: &str,
    extra_args: &[&str],
    extra_env: &[(&str, &str)],
) {
    let guest_bin = PathBuf::from(env!("CARGO_BIN_EXE_anyagents-guest"));
    let mut cmd = Command::new(&guest_bin);
    for (k, _) in std::env::vars() {
        if k.starts_with("ANYAGENTS_") {
            cmd.env_remove(k);
        }
    }
    cmd.env("ANYAGENTS_GUEST_CALLBACK_BACKOFF_MS", "15");
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.args([
        "turn",
        "--detached",
        "--turn-id",
        turn_id,
        "--agent-id",
        agent_id,
        "--harness",
        harness,
        "--program",
        program.to_str().unwrap(),
        "--prompt-stdin",
        "--callback-url",
        callback_url,
        "--callback-token",
        callback_token,
        "--heartbeat-interval-ms",
        "100",
        "--state-dir",
        state_dir.to_str().unwrap(),
        "--workdir",
        work_dir.to_str().unwrap(),
    ]);
    if !extra_args.is_empty() {
        cmd.arg("--");
        cmd.args(extra_args);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn anyagents-guest turn --detached");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(prompt.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{harness} detached parent failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let accepted: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(accepted["status"], "accepted");
    assert_eq!(accepted["turn_id"], turn_id);
}

#[allow(clippy::too_many_arguments)]
fn assert_next_step_steer_proof(
    harness_label: &str,
    events: &[(String, Value)],
    expected_token: &str,
    expected_msg_id: &str,
    expected_seq: i64,
    expected_answer_needle: Option<&str>,
    state_dir: &Path,
    turn_id: &str,
) {
    let bearer = format!("Bearer {expected_token}");
    assert!(
        events.iter().all(|(auth, _)| auth == &bearer),
        "{harness_label}: callback token mismatch: {events:?}"
    );
    let consumed_idx = events
        .iter()
        .position(|(_, v)| {
            v["event_type"] == "steer_consumed"
                && v["message_id"] == expected_msg_id
                && v["seq"] == expected_seq
        })
        .unwrap_or_else(|| {
            let log = std::fs::read_to_string(state_dir.join(format!("turns/{turn_id}.log")))
                .unwrap_or_default();
            panic!(
                "{harness_label}: missing mid-turn steer_consumed for {expected_msg_id} seq={expected_seq}.\nevents={events:?}\nguest_log={log}"
            );
        });
    let complete_idx = events
        .iter()
        .position(|(_, v)| v["event_type"] == "turn_complete")
        .unwrap_or_else(|| {
            let log = std::fs::read_to_string(state_dir.join(format!("turns/{turn_id}.log")))
                .unwrap_or_default();
            panic!("{harness_label}: missing turn_complete.\nevents={events:?}\nguest_log={log}");
        });
    assert!(
        consumed_idx < complete_idx,
        "{harness_label}: steer_consumed (idx={consumed_idx}) MUST arrive strictly BEFORE turn_complete (idx={complete_idx})! events={events:?}"
    );
    let completed = &events[complete_idx].1;
    if let Some(needle) = expected_answer_needle {
        let result_text = completed["result_text"].as_str().unwrap_or("");
        assert!(
            result_text.contains(needle),
            "{harness_label}: turn_complete result_text should reflect step-2 steer `{needle}`, got `{result_text}`"
        );
    }
    let consumed_arr = completed["consumed_steers"]
        .as_array()
        .expect("turn_complete should carry consumed_steers");
    assert!(
        consumed_arr
            .iter()
            .any(|c| c["message_id"] == expected_msg_id && c["seq"] == expected_seq),
        "{harness_label}: turn_complete consumed_steers missing {expected_msg_id}: {consumed_arr:?}"
    );

    let spool_dir = state_dir
        .join("turns")
        .join(format!("{turn_id}{STEER_DIR_SUFFIX}"));
    let leftover: Vec<_> = std::fs::read_dir(&spool_dir)
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| !n.starts_with('.') && n.ends_with(".json"))
        })
        .collect();
    assert!(
        leftover.is_empty(),
        "{harness_label}: expected zero unconsumed steer files, found {leftover:?}"
    );
}

#[test]
fn midturn_steer_e2e_claude_next_step() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();
    let step1_flag = tmp.path().join("claude-step1.ready");
    let release_flag = tmp.path().join("claude-release.go");
    let fake_claude = tmp.path().join("fake-claude.py");

    write_executable(
        &fake_claude,
        &format!(
            r#"#!/usr/bin/env python3
import json, os, sys, time, pathlib

cfg_dir = os.environ.get("CLAUDE_CONFIG_DIR", "")
proj_dir = pathlib.Path(cfg_dir) / "projects" / "default"
proj_dir.mkdir(parents=True, exist_ok=True)
transcript = proj_dir / "claude-e2e-sess.jsonl"

print(json.dumps({{
    "type": "system",
    "subtype": "init",
    "session_id": "claude-e2e-sess",
    "claude_code_version": "2.1.0",
    "peer": "claude-code 2.1.0"
}}), flush=True)

opener_uuid = None
steer_uuid = None
steer_text = ""

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    if msg.get("type") != "user":
        continue
    uuid = msg.get("uuid", "")
    text = msg.get("message", {{}}).get("content", "")
    with transcript.open("a", encoding="utf-8") as f:
        f.write(json.dumps({{"type": "user", "uuid": uuid, "message": {{"role": "user", "content": text}}}}) + "\n")
        f.flush()
    print(json.dumps({{
        "type": "user",
        "isReplay": True,
        "uuid": uuid,
        "message": {{"role": "user", "content": [{{"type": "text", "text": text}}]}}
    }}), flush=True)

    if opener_uuid is None:
        opener_uuid = uuid
        pathlib.Path({step1:?}).write_text("step1", encoding="utf-8")
        # Stay inside Step 1 waiting for mid-turn steer line on stdin!
        continue

    steer_uuid = uuid
    steer_text = text
    # Wait until test observes mid-turn steer_consumed callback BEFORE finishing Step 2!
    deadline = time.time() + 10.0
    while time.time() < deadline and not pathlib.Path({release:?}).exists():
        time.sleep(0.02)
    final_reply = f"claude step1+step2 acted on: {{steer_text}}"
    print(json.dumps({{
        "type": "assistant",
        "session_id": "claude-e2e-sess",
        "message": {{"role": "assistant", "content": [{{"type": "text", "text": final_reply}}]}}
    }}), flush=True)
    print(json.dumps({{
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "session_id": "claude-e2e-sess",
        "user_message_uuids": [opener_uuid, steer_uuid],
        "result": final_reply,
        "usage": {{"input_tokens": 33, "output_tokens": 14}},
        "total_cost_usd": 0.03
    }}), flush=True)
    break
"#,
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
        ),
    );

    let srv = MockCallbackServer::spawn();
    let events_ref = Arc::clone(&srv.events);
    spawn_detached_guest_turn(
        "claude-code",
        &fake_claude,
        "turn_claude_e2e",
        "agt_01claude",
        &srv.url,
        "tok_claude",
        &state_dir,
        &work_dir,
        "Step 1 initial work",
        &["--guest-bare"],
        &[],
    );

    wait_for_file(
        &step1_flag,
        Duration::from_secs(15),
        "claude step1",
        Some(&events_ref),
        Some(&state_dir),
        Some("turn_claude_e2e"),
    );
    drop_atomic_steer_file(
        &state_dir,
        "turn_claude_e2e",
        "msg_steer_claude",
        2,
        "STEER_TOKEN_CLAUDE",
    );
    wait_for_event(
        &events_ref,
        "steer_consumed",
        Duration::from_secs(10),
        "claude mid-turn steer_consumed",
    );
    std::fs::write(&release_flag, "go").unwrap();

    let events = srv.join();
    assert_next_step_steer_proof(
        "claude-code",
        &events,
        "tok_claude",
        "msg_steer_claude",
        2,
        Some("STEER_TOKEN_CLAUDE"),
        &state_dir,
        "turn_claude_e2e",
    );
}

#[test]
fn midturn_steer_e2e_codex_next_step() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();
    let step1_flag = tmp.path().join("codex-step1.ready");
    let release_flag = tmp.path().join("codex-release.go");
    let wire_log = tmp.path().join("codex-wire.jsonl");
    let fake_codex = tmp.path().join("fake-codex.py");

    write_executable(
        &fake_codex,
        &format!(
            r#"#!/usr/bin/env python3
import json, sys, time, pathlib

wire = pathlib.Path({wire:?})

def send(obj):
    print(json.dumps(obj), flush=True)

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    req = json.loads(raw)
    with wire.open("a", encoding="utf-8") as f:
        f.write(raw + "\n")
    method = req.get("method")
    rid = req.get("id")
    params = req.get("params", {{}})
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": rid, "result": {{"userAgent": "codex_cli_rs/0.170.0 (Linux; x86_64)"}}}})
    elif method == "initialized":
        continue
    elif method == "thread/start":
        send({{"jsonrpc": "2.0", "id": rid, "result": {{"thread": {{"id": "thr-e2e-1"}}}}}})
    elif method == "turn/start":
        cid = params.get("clientUserMessageId", "")
        text = params.get("input", [{{}}])[0].get("text", "")
        send({{"jsonrpc": "2.0", "id": rid, "result": {{"turn": {{"id": "codex-turn-1", "status": "inProgress"}}}}}})
        send({{"jsonrpc": "2.0", "method": "turn/started", "params": {{"threadId": "thr-e2e-1", "turn": {{"id": "codex-turn-1"}}}}}})
        send({{"jsonrpc": "2.0", "method": "item/completed", "params": {{
            "threadId": "thr-e2e-1",
            "turnId": "codex-turn-1",
            "item": {{"type": "userMessage", "id": "u-1", "clientId": cid, "content": [{{"type": "text", "text": text}}]}}
        }}}})
        pathlib.Path({step1:?}).write_text("step1", encoding="utf-8")
    elif method == "turn/steer":
        expected_turn = params.get("expectedTurnId")
        cid = params.get("clientUserMessageId", "")
        text = params.get("input", [{{}}])[0].get("text", "")
        assert expected_turn == "codex-turn-1", f"wrong expectedTurnId: {{expected_turn}}"
        send({{"jsonrpc": "2.0", "id": rid, "result": {{"turnId": "codex-turn-1"}}}})
        send({{"jsonrpc": "2.0", "method": "item/completed", "params": {{
            "threadId": "thr-e2e-1",
            "turnId": "codex-turn-1",
            "item": {{"type": "userMessage", "id": "u-2", "clientId": cid, "content": [{{"type": "text", "text": text}}]}}
        }}}})
        deadline = time.time() + 10.0
        while time.time() < deadline and not pathlib.Path({release:?}).exists():
            time.sleep(0.02)
        reply = f"codex step1+step2 acted on: {{text}}"
        send({{"jsonrpc": "2.0", "method": "item/completed", "params": {{
            "threadId": "thr-e2e-1",
            "turnId": "codex-turn-1",
            "item": {{"type": "agentMessage", "id": "a-1", "text": reply}}
        }}}})
        send({{"jsonrpc": "2.0", "method": "thread/tokenUsage/updated", "params": {{
            "threadId": "thr-e2e-1",
            "tokenUsage": {{"last": {{"inputTokens": 20, "outputTokens": 11}}}}
        }}}})
        send({{"jsonrpc": "2.0", "method": "turn/completed", "params": {{
            "threadId": "thr-e2e-1",
            "turn": {{"id": "codex-turn-1", "status": "completed"}}
        }}}})
        break
"#,
            wire = wire_log.display().to_string(),
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
        ),
    );

    let srv = MockCallbackServer::spawn();
    let events_ref = Arc::clone(&srv.events);
    spawn_detached_guest_turn(
        "codex",
        &fake_codex,
        "turn_codex_e2e",
        "agt_01codex",
        &srv.url,
        "tok_codex",
        &state_dir,
        &work_dir,
        "Step 1 codex work",
        &["--guest-bare"],
        &[],
    );

    wait_for_file(
        &step1_flag,
        Duration::from_secs(15),
        "codex step1",
        Some(&events_ref),
        Some(&state_dir),
        Some("turn_codex_e2e"),
    );
    drop_atomic_steer_file(
        &state_dir,
        "turn_codex_e2e",
        "msg_steer_codex",
        2,
        "STEER_TOKEN_CODEX",
    );
    wait_for_event(
        &events_ref,
        "steer_consumed",
        Duration::from_secs(10),
        "codex mid-turn steer_consumed",
    );
    std::fs::write(&release_flag, "go").unwrap();

    let events = srv.join();
    assert_next_step_steer_proof(
        "codex",
        &events,
        "tok_codex",
        "msg_steer_codex",
        2,
        Some("STEER_TOKEN_CODEX"),
        &state_dir,
        "turn_codex_e2e",
    );
    let wire_text = std::fs::read_to_string(&wire_log).unwrap();
    assert!(
        wire_text.contains("\"method\":\"turn/steer\"")
            && wire_text.contains("\"expectedTurnId\":\"codex-turn-1\""),
        "codex must use native turn/steer JSON-RPC: {wire_text}"
    );
}

#[test]
fn midturn_steer_e2e_opencode_next_step() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();
    let step1_flag = tmp.path().join("opencode-step1.ready");
    let release_flag = tmp.path().join("opencode-release.go");
    let wire_log = tmp.path().join("opencode-wire.jsonl");
    let fake_opencode = tmp.path().join("fake-opencode.py");

    write_executable(
        &fake_opencode,
        &format!(
            r#"#!/usr/bin/env python3
import base64, http.client, http.server, json, os, queue, socketserver, sys, threading, time, urllib.parse, pathlib

STEP1 = pathlib.Path({step1:?})
RELEASE = pathlib.Path({release:?})
WIRE = pathlib.Path({wire:?})

def run_api(args):
    server = None
    body = None
    rest = []
    i = 0
    while i < len(args):
        if args[i] == "--server":
            server = args[i + 1]
            i += 2
        elif args[i] == "-d":
            body = args[i + 1]
            i += 2
        else:
            rest.append(args[i])
            i += 1
    method, path = rest[0], rest[1]
    url = urllib.parse.urlsplit(server)
    pw = os.environ.get("OPENCODE_PASSWORD", "")
    cred = base64.b64encode(f"opencode:{{pw}}".encode()).decode()
    data = body.encode("utf-8") if body is not None else None
    headers = {{"Authorization": f"Basic {{cred}}", "Connection": "close"}}
    if data is not None:
        headers["Content-Type"] = "application/json"
    try:
        conn = http.client.HTTPConnection(url.hostname, url.port, timeout=30)
        conn.request(method, path, body=data, headers=headers)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8")
        conn.close()
    except OSError as e:
        sys.stderr.write(f"error: Unable to connect. {{e}}\n")
        sys.stderr.flush()
        sys.exit(1)
    if raw:
        sys.stdout.write(raw)
        sys.stdout.flush()
    if not (200 <= resp.status < 300):
        sys.stderr.write(f"HTTP {{resp.status}} {{resp.reason}}\n")
        sys.stderr.flush()
        sys.exit(1)

def run_serve(args):
    port = 0
    for i, a in enumerate(args):
        if a == "--port":
            port = int(args[i + 1])
    pw = "oc-test-pw"
    state_lock = threading.Lock()
    sse_queues = []
    messages = []
    turn_done = threading.Event()
    prompts_seen = [0]

    def emit_sse(ev):
        with state_lock:
            for q in list(sse_queues):
                q.put(ev)

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass
        def send_json(self, code, obj):
            raw = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        def do_GET(self):
            req_path = urllib.parse.urlsplit(self.path).path
            if req_path == "/api/event":
                q = queue.Queue()
                with state_lock:
                    sse_queues.append(q)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(b'data: {{"type":"server.connected"}}\n\n')
                self.wfile.flush()
                try:
                    while True:
                        try:
                            ev = q.get(timeout=30)
                        except queue.Empty:
                            continue
                        payload = f"data: {{json.dumps(ev)}}\n\n".encode("utf-8")
                        self.wfile.write(payload)
                        self.wfile.flush()
                except Exception:
                    return
            elif req_path == "/api/session":
                self.send_json(200, {{"data": []}})
            elif req_path == "/api/info":
                self.send_json(200, {{"version": "2.3.0"}})
            elif req_path == "/api/mcp":
                self.send_json(200, {{"data": [{{"name": "anyagents", "status": {{"status": "connected"}}}}]}})
            elif req_path == "/api/session/active":
                self.send_json(200, {{"data": {{}}}})
            elif req_path == "/api/session/ses_oc_1/message":
                with state_lock:
                    # OpenCode lists messages newest first!
                    ordered = list(reversed(messages))
                self.send_json(200, {{"data": ordered}})
            elif req_path.startswith("/api/session/ses_oc_1/message/"):
                wanted = req_path.rsplit("/", 1)[-1]
                with state_lock:
                    found = next((m for m in messages if m.get("id") == wanted), None)
                if found is not None:
                    self.send_json(200, {{"data": found}})
                else:
                    self.send_json(404, {{"_tag": "MessageNotFoundError", "message": "not found"}})
            elif req_path == "/api/session/ses_oc_1":
                self.send_json(200, {{"data": {{"id": "ses_oc_1"}}}})
            else:
                self.send_json(404, {{"message": "not found"}})
        def do_POST(self):
            req_path = urllib.parse.urlsplit(self.path).path
            cl = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(cl) if cl > 0 else b""
            if req_path == "/api/session":
                self.send_json(200, {{"data": {{"id": "ses_oc_1"}}}})
            elif req_path == "/api/session/ses_oc_1/prompt":
                body = json.loads(raw.decode("utf-8"))
                with WIRE.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(body) + "\n")
                mid = body["id"]
                text = body["text"]
                now = int(time.time() * 1000)
                with state_lock:
                    prompts_seen[0] += 1
                    n = prompts_seen[0]
                    messages.append({{"id": mid, "type": "user", "text": text, "time": {{"created": now}}}})
                self.send_json(200, {{"data": {{"id": mid, "time": {{"created": now}}}}}})
                emit_sse({{"type": "session.inbox.delivered", "data": {{"sessionID": "ses_oc_1", "inboxID": mid}}}})
                if n == 1:
                    STEP1.write_text("step1", encoding="utf-8")
                else:
                    def finish_step2():
                        deadline = time.time() + 15.0
                        while time.time() < deadline and not RELEASE.exists():
                            time.sleep(0.02)
                        with state_lock:
                            last_ts = max((m.get("time", {{}}).get("created", 0) for m in messages), default=0)
                            asst_ts = max(int(time.time() * 1000), last_ts + 5)
                            messages.append({{
                                "id": "msg_asst_oc",
                                "type": "assistant",
                                "content": [{{"type": "text", "text": f"opencode step1+step2 acted on: {{text}}"}}],
                                "time": {{"created": asst_ts}}
                            }})
                        turn_done.set()
                    threading.Thread(target=finish_step2, daemon=True).start()
            elif req_path == "/api/experimental/session/ses_oc_1/wait":
                turn_done.wait(timeout=20.0)
                self.send_json(200, {{"data": {{"idle": True}}}})
            else:
                self.send_json(404, {{"message": "not found"}})

    class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
        daemon_threads = True
        allow_reuse_address = True
        def server_bind(self):
            socketserver.TCPServer.server_bind(self)
            self.server_name = "127.0.0.1"
            self.server_port = self.server_address[1]

    srv = Server(("127.0.0.1", port), Handler)
    print(f"server listening on http://127.0.0.1:{{port}}", flush=True)
    print(f"server password {{pw}}", flush=True)
    srv.serve_forever()

if sys.argv[1] == "serve":
    run_serve(sys.argv[2:])
elif sys.argv[1] == "api":
    run_api(sys.argv[2:])
else:
    sys.exit(2)
"#,
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
            wire = wire_log.display().to_string(),
        ),
    );

    let srv = MockCallbackServer::spawn();
    let events_ref = Arc::clone(&srv.events);
    spawn_detached_guest_turn(
        "opencode",
        &fake_opencode,
        "turn_opencode_e2e",
        "agt_01opencode",
        &srv.url,
        "tok_opencode",
        &state_dir,
        &work_dir,
        "Step 1 opencode work",
        &["--guest-bare"],
        &[],
    );

    wait_for_file(
        &step1_flag,
        Duration::from_secs(20),
        "opencode step1",
        Some(&events_ref),
        Some(&state_dir),
        Some("turn_opencode_e2e"),
    );
    drop_atomic_steer_file(
        &state_dir,
        "turn_opencode_e2e",
        "msg_steer_opencode",
        2,
        "STEER_TOKEN_OPENCODE",
    );
    wait_for_event(
        &events_ref,
        "steer_consumed",
        Duration::from_secs(10),
        "opencode mid-turn steer_consumed",
    );
    std::fs::write(&release_flag, "go").unwrap();

    let events = srv.join();
    assert_next_step_steer_proof(
        "opencode",
        &events,
        "tok_opencode",
        "msg_steer_opencode",
        2,
        Some("STEER_TOKEN_OPENCODE"),
        &state_dir,
        "turn_opencode_e2e",
    );
    let wire_text = std::fs::read_to_string(&wire_log).unwrap();
    assert!(
        wire_text.contains("\"delivery\": \"steer\"")
            || wire_text.contains("\"delivery\":\"steer\""),
        "opencode must submit native delivery=steer prompt: {wire_text}"
    );
}

#[test]
fn midturn_steer_e2e_agy_next_step() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();
    let step1_flag = tmp.path().join("agy-step1.ready");
    let release_flag = tmp.path().join("agy-release.go");
    let wire_log = tmp.path().join("agy-wire.jsonl");
    let fake_agy = tmp.path().join("fake-agy.py");

    write_executable(
        &fake_agy,
        &format!(
            r#"#!/usr/bin/env python3
import http.server, json, os, socketserver, sys, threading, time, pathlib

STEP1 = pathlib.Path({step1:?})
RELEASE = pathlib.Path({release:?})
WIRE = pathlib.Path({wire:?})

csrf = ""
for i, a in enumerate(sys.argv):
    if a == "--csrf_token" and i + 1 < len(sys.argv):
        csrf = sys.argv[i + 1]

cfg_dir = pathlib.Path(os.environ.get("AGY_CONFIG_DIR") or os.environ.get("ANYAGENTS_AGENT_CONFIG") or ".")
cascade = "cas_e2e_1"
logs_dir = cfg_dir / ".gemini" / "antigravity-cli" / "brain" / cascade / ".system_generated" / "logs"
logs_dir.mkdir(parents=True, exist_ok=True)
transcript = logs_dir / "transcript_full.jsonl"
cli_log = cfg_dir / ".gemini" / "antigravity-cli" / "cli.log"
cli_log.parent.mkdir(parents=True, exist_ok=True)

steer_received = threading.Event()
steer_payload = [""]

class RpcHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass
    def do_POST(self):
        cl = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(cl) if cl > 0 else b"{{}}"
        tok = self.headers.get("x-codeium-csrf-token", "")
        with WIRE.open("a", encoding="utf-8") as f:
            f.write(json.dumps({{"path": self.path, "csrf_ok": tok == csrf, "body": json.loads(raw.decode("utf-8"))}}) + "\n")
        if self.path.endswith("/SendUserCascadeMessage"):
            body = json.loads(raw.decode("utf-8"))
            items = body.get("items", [])
            text = items[0].get("text", "") if items else ""
            steer_payload[0] = text
            with transcript.open("a", encoding="utf-8") as f:
                f.write(json.dumps({{"type": "USER_INPUT", "content": f"<USER_REQUEST>\n{{text}}\n</USER_REQUEST>"}}, separators=(",", ":")) + "\n")
                f.flush()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{{}}")
            steer_received.set()
        elif self.path.endswith("/GetAllCascadeTrajectories"):
            resp = json.dumps({{"trajectorySummaries": {{cascade: {{"status": "CASCADE_RUN_STATUS_RUNNING"}}}}}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
        else:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{{}}")

class RpcServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

srv = RpcServer(("127.0.0.1", 0), RpcHandler)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()

cli_log.write_text(
    f"Starting language server process with pid {{os.getpid()}}\n"
    f"Language server version: 2.0.0\n"
    f"Language server listening on random port at {{port}} for HTTP\n",
    encoding="utf-8",
)

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    opener_text = msg.get("text", "")
    with transcript.open("a", encoding="utf-8") as f:
        f.write(json.dumps({{"type": "USER_INPUT", "content": f"<USER_REQUEST>\n{{opener_text}}\n</USER_REQUEST>"}}, separators=(",", ":")) + "\n")
        f.flush()
    print(json.dumps({{
        "type": "step_update",
        "session_id": cascade,
        "step_update": {{"step_type": "user_input"}}
    }}), flush=True)
    STEP1.write_text("step1", encoding="utf-8")
    assert steer_received.wait(timeout=10.0), "never received SendUserCascadeMessage"
    deadline = time.time() + 10.0
    while time.time() < deadline and not RELEASE.exists():
        time.sleep(0.02)
    print(json.dumps({{
        "type": "step_update",
        "session_id": cascade,
        "step_update": {{"step_type": "planner_response", "text": f"agy step1+step2 acted on: {{steer_payload[0]}}"}}
    }}), flush=True)
    print(json.dumps({{
        "type": "result",
        "session_id": cascade,
        "usage": {{"input_tokens": 18, "output_tokens": 9}}
    }}), flush=True)
    break
"#,
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
            wire = wire_log.display().to_string(),
        ),
    );

    let srv = MockCallbackServer::spawn();
    let events_ref = Arc::clone(&srv.events);
    spawn_detached_guest_turn(
        "agy",
        &fake_agy,
        "turn_agy_e2e",
        "agt_01agy",
        &srv.url,
        "tok_agy",
        &state_dir,
        &work_dir,
        "Step 1 agy work",
        &["--guest-bare"],
        &[],
    );

    wait_for_file(
        &step1_flag,
        Duration::from_secs(15),
        "agy step1",
        Some(&events_ref),
        Some(&state_dir),
        Some("turn_agy_e2e"),
    );
    drop_atomic_steer_file(
        &state_dir,
        "turn_agy_e2e",
        "msg_steer_agy",
        2,
        "STEER_TOKEN_AGY",
    );
    wait_for_event(
        &events_ref,
        "steer_consumed",
        Duration::from_secs(10),
        "agy mid-turn steer_consumed",
    );
    std::fs::write(&release_flag, "go").unwrap();

    let events = srv.join();
    assert_next_step_steer_proof(
        "agy",
        &events,
        "tok_agy",
        "msg_steer_agy",
        2,
        None,
        &state_dir,
        "turn_agy_e2e",
    );
    let wire_text = std::fs::read_to_string(&wire_log).unwrap();
    assert!(
        wire_text.contains("SendUserCascadeMessage")
            && wire_text.contains("\"csrf_ok\": true")
            && wire_text.contains("STEER_TOKEN_AGY"),
        "agy must push native ConnectRPC SendUserCascadeMessage mid-turn: {wire_text}"
    );
}

#[test]
fn midturn_steer_e2e_omp_next_step() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();
    let step1_flag = tmp.path().join("omp-step1.ready");
    let release_flag = tmp.path().join("omp-release.go");
    let wire_log = tmp.path().join("omp-wire.jsonl");
    let session_file = state_dir.join("omp-session.jsonl");
    let fake_omp = tmp.path().join("fake-omp.py");

    write_executable(
        &fake_omp,
        &format!(
            r#"#!/usr/bin/env python3
import json, sys, time, pathlib

STEP1 = pathlib.Path({step1:?})
RELEASE = pathlib.Path({release:?})
WIRE = pathlib.Path({wire:?})
SESSION_FILE = pathlib.Path({sfile:?})

def send(obj):
    print(json.dumps(obj), flush=True)

send({{"type": "ready", "protocolVersion": 1}})
opener_req_id = None

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    cmd = json.loads(raw)
    with WIRE.open("a", encoding="utf-8") as f:
        f.write(raw + "\n")
    t = cmd.get("type")
    rid = cmd.get("id")
    if t == "set_steering_mode":
        send({{"id": rid, "type": "response", "command": "set_steering_mode", "success": True}})
    elif t == "get_state":
        SESSION_FILE.write_text('{{"type":"session","id":"omp-sess-1"}}\n', encoding="utf-8")
        send({{
            "id": rid,
            "type": "response",
            "command": "get_state",
            "success": True,
            "data": {{
                "sessionId": "omp-sess-1",
                "steeringMode": "all",
                "sessionFile": str(SESSION_FILE)
            }}
        }})
    elif t == "prompt":
        assert cmd.get("streamingBehavior") == "steer", f"expected streamingBehavior=steer, got {{cmd}}"
        text = cmd.get("message", "")
        send({{"id": rid, "type": "response", "command": "prompt", "success": True}})
        if opener_req_id is None:
            opener_req_id = rid
            send({{
                "type": "message_end",
                "messageId": "m-u1",
                "message": {{"role": "user", "steering": False, "content": [{{"type": "text", "text": text}}]}}
            }})
            STEP1.write_text("step1", encoding="utf-8")
            continue
        send({{
            "type": "message_end",
            "messageId": "m-u2",
            "message": {{"role": "user", "steering": True, "content": [{{"type": "text", "text": text}}]}}
        }})
        deadline = time.time() + 10.0
        while time.time() < deadline and not RELEASE.exists():
            time.sleep(0.02)
        send({{
            "type": "message_end",
            "messageId": "m-a1",
            "message": {{
                "role": "assistant",
                "content": [{{"type": "text", "text": f"omp step1+step2 acted on: {{text}}"}}],
                "usage": {{"input": 12, "output": 8, "cost": {{"total": 0.001}}}}
            }}
        }})
        send({{"type": "prompt_result", "id": opener_req_id, "agentInvoked": True, "status": "completed", "sessionSettled": False}})
        send({{"type": "prompt_result", "id": rid, "agentInvoked": True, "status": "completed", "sessionSettled": True}})
        send({{"type": "session_settled"}})
        break
"#,
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
            wire = wire_log.display().to_string(),
            sfile = session_file.display().to_string(),
        ),
    );

    let srv = MockCallbackServer::spawn();
    let events_ref = Arc::clone(&srv.events);
    spawn_detached_guest_turn(
        "omp",
        &fake_omp,
        "turn_omp_e2e",
        "agt_01omp",
        &srv.url,
        "tok_omp",
        &state_dir,
        &work_dir,
        "Step 1 omp work",
        &["--guest-bare"],
        &[],
    );

    wait_for_file(
        &step1_flag,
        Duration::from_secs(15),
        "omp step1",
        Some(&events_ref),
        Some(&state_dir),
        Some("turn_omp_e2e"),
    );
    drop_atomic_steer_file(
        &state_dir,
        "turn_omp_e2e",
        "msg_steer_omp",
        2,
        "STEER_TOKEN_OMP",
    );
    wait_for_event(
        &events_ref,
        "steer_consumed",
        Duration::from_secs(10),
        "omp mid-turn steer_consumed",
    );
    std::fs::write(&release_flag, "go").unwrap();

    let events = srv.join();
    assert_next_step_steer_proof(
        "omp",
        &events,
        "tok_omp",
        "msg_steer_omp",
        2,
        None,
        &state_dir,
        "turn_omp_e2e",
    );
    let wire_text = std::fs::read_to_string(&wire_log).unwrap();
    assert!(
        wire_text.contains("\"streamingBehavior\":\"steer\"")
            && wire_text.contains("STEER_TOKEN_OMP"),
        "omp must use native streamingBehavior=steer RPC: {wire_text}"
    );
}

#[test]
fn midturn_steer_e2e_spiron_v2_socket_next_step() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();
    let step1_flag = tmp.path().join("spiron-step1.ready");
    let release_flag = tmp.path().join("spiron-release.go");
    let wire_log = tmp.path().join("spiron-wire.jsonl");
    let fake_spiron = tmp.path().join("fake-spiron.py");

    write_executable(
        &fake_spiron,
        &format!(
            r#"#!/usr/bin/env python3
import json, os, socket, sys, time, pathlib

STEP1 = pathlib.Path({step1:?})
RELEASE = pathlib.Path({release:?})
WIRE = pathlib.Path({wire:?})

sock_path = None
for i, a in enumerate(sys.argv):
    if a == "--listen" and i + 1 < len(sys.argv):
        sock_path = sys.argv[i + 1]
    elif a.startswith("--listen="):
        sock_path = a.split("=", 1)[1]
assert sock_path, f"spiron guest must pass --listen <socket>, got {{sys.argv}}"

if os.path.exists(sock_path):
    os.unlink(sock_path)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(sock_path)
srv.listen(4)

conn = None
while True:
    c, _ = srv.accept()
    # SpironHarness::launch_listening probes socket readiness with an immediate connect+close.
    c.settimeout(2.0)
    try:
        data = c.recv(4096, socket.MSG_PEEK)
        if not data:
            c.close()
            continue
    except Exception:
        c.close()
        continue
    c.settimeout(None)
    conn = c
    break

rfile = conn.makefile("r", encoding="utf-8")
wfile = conn.makefile("w", encoding="utf-8")
seq = [0]

def send(req_id, obj):
    seq[0] += 1
    out = dict(obj)
    out["seq"] = seq[0]
    out["session_id"] = "spiron-e2e-sess"
    out["request_id"] = req_id
    wfile.write(json.dumps(out) + "\n")
    wfile.flush()

opener_id = None
for raw in rfile:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    with WIRE.open("a", encoding="utf-8") as f:
        f.write(raw + "\n")
    t = msg.get("type")
    rid = msg.get("request_id", "")
    if t == "attach":
        send(rid, {{
            "type": "ready",
            "protocol": "spiron.headless/v2",
            "version": "0.2.0",
            "commit": "0123456789abcdef",
            "features": ["turn-tool-context-v1"]
        }})
    elif t == "prompt":
        opener_id = rid
        send(rid, {{"type": "accepted", "mode": "turn"}})
        STEP1.write_text("step1", encoding="utf-8")
    elif t == "steer":
        steer_text = msg.get("text", "")
        send(rid, {{"type": "steer_ack", "accepted": True, "turn_id": opener_id}})
        send(opener_id, {{"type": "steer_consumed", "steer_request_id": rid, "turn_id": opener_id, "duplicate": False}})
        deadline = time.time() + 10.0
        while time.time() < deadline and not RELEASE.exists():
            time.sleep(0.02)
        send(opener_id, {{
            "type": "result",
            "status": "completed",
            "is_error": False,
            "error": None,
            "duplicate": False,
            "tool_context_bound": True,
            "final_text": f"spiron step1+step2 acted on: {{steer_text}}",
            "consumed_request_ids": [opener_id, rid],
            "undelivered_steer_ids": [],
            "usage": {{"input_tokens": 25, "output_tokens": 13, "cached_tokens": 0, "cache_write_tokens": None}},
            "cost_micros": 90
        }})
        break
"#,
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
            wire = wire_log.display().to_string(),
        ),
    );

    let srv = MockCallbackServer::spawn();
    let events_ref = Arc::clone(&srv.events);
    spawn_detached_guest_turn(
        "spiron",
        &fake_spiron,
        "turn_spiron_e2e",
        "agt_01spiron",
        &srv.url,
        "tok_spiron",
        &state_dir,
        &work_dir,
        "Step 1 spiron work",
        &["--guest-bare"],
        &[],
    );

    wait_for_file(
        &step1_flag,
        Duration::from_secs(15),
        "spiron step1",
        Some(&events_ref),
        Some(&state_dir),
        Some("turn_spiron_e2e"),
    );
    drop_atomic_steer_file(
        &state_dir,
        "turn_spiron_e2e",
        "msg_steer_spiron",
        2,
        "STEER_TOKEN_SPIRON",
    );
    wait_for_event(
        &events_ref,
        "steer_consumed",
        Duration::from_secs(10),
        "spiron mid-turn steer_consumed",
    );
    std::fs::write(&release_flag, "go").unwrap();

    let events = srv.join();
    assert_next_step_steer_proof(
        "spiron",
        &events,
        "tok_spiron",
        "msg_steer_spiron",
        2,
        Some("STEER_TOKEN_SPIRON"),
        &state_dir,
        "turn_spiron_e2e",
    );
    let wire_text = std::fs::read_to_string(&wire_log).unwrap();
    assert!(
        wire_text.contains("\"type\":\"steer\"") && wire_text.contains("STEER_TOKEN_SPIRON"),
        "spiron guest must drive native spiron.headless/v2 steer frame over Unix socket: {wire_text}"
    );
}

#[test]
fn midturn_steer_e2e_gemini_fails_closed_without_claiming_spool_file() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let work_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();
    let step1_flag = tmp.path().join("gemini-step1.ready");
    let fake_gemini = tmp.path().join("fake-gemini");

    write_executable(
        &fake_gemini,
        &format!(
            r#"#!/bin/sh
echo '{{"type":"init","session_id":"gem-e2e-1"}}'
printf 'step1' > '{}'
sleep 0.25
echo '{{"type":"message","role":"assistant","content":"gemini step1 only"}}'
echo '{{"type":"result","status":"success","session_id":"gem-e2e-1","result":"gemini step1 only","usage":{{"input_tokens":10,"output_tokens":5}}}}'
"#,
            step1_flag.display()
        ),
    );

    let srv = MockCallbackServer::spawn();
    spawn_detached_guest_turn(
        "gemini",
        &fake_gemini,
        "turn_gemini_fc",
        "agt_01geminifc",
        &srv.url,
        "tok_gemini",
        &state_dir,
        &work_dir,
        "Step 1 gemini work",
        &[],
        &[],
    );

    wait_for_file(
        &step1_flag,
        Duration::from_secs(15),
        "gemini step1",
        Some(&srv.events),
        Some(&state_dir),
        Some("turn_gemini_fc"),
    );
    let steer_path = drop_atomic_steer_file(
        &state_dir,
        "turn_gemini_fc",
        "msg_steer_gemini",
        2,
        "SHOULD_STAY_UNCLAIMED",
    );

    let events = srv.join();
    assert!(
        !events.iter().any(|(_, v)| matches!(
            v["event_type"].as_str(),
            Some("steer_consumed" | "steer_accepted")
        )),
        "gemini is TurnEnd-only and must NEVER emit steer_consumed/steer_accepted: {events:?}"
    );
    let completed = events
        .iter()
        .map(|(_, v)| v)
        .find(|v| v["event_type"] == "turn_complete")
        .expect("gemini turn_complete");
    assert_eq!(completed["result_text"], "gemini step1 only");
    assert!(
        steer_path.is_file(),
        "fail-closed invariant: TurnEnd gemini must leave steer file untouched at {}",
        steer_path.display()
    );
}
