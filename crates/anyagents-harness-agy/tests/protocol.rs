//! Scripted stand-in for the Antigravity child.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{Context, DeliverOutcome, DeliveryLevel, Harness, OutboundMessage};
use anyagents_harness_agy::AgyHarness;
use tempfile::TempDir;

fn msg(id: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.to_string(),
        seq: 1,
        text: "hello".to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    }
}

fn start(dir: &TempDir, mode: &str, session: Option<&str>) -> AgyHarness {
    let log = dir.path().join("log");
    AgyHarness::start(
        env!("CARGO_BIN_EXE_fake_agy"),
        session,
        &[
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
        ],
    )
    .expect("start")
}

fn read_log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

fn wait_log(path: &Path, needle: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if read_log(path).contains(needle) {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

#[test]
fn a_result_records_usage_and_a_second_deliver_is_not_written() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    let usage = harness.usage_for("m1").expect("usage");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.reported_cost_micros, None);
    assert_eq!(harness.session_id().as_deref(), Some("sess-from-vendor"));
    assert_eq!(harness.health().delivery.level, DeliveryLevel::TurnEnd);
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn a_second_message_during_a_turn_waits() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(
        harness.deliver(&msg("m2")).unwrap(),
        DeliverOutcome::InFlight
    );
    assert_eq!(harness.turns_started(), 1);
}

#[test]
fn a_rate_limit_is_a_wait_and_not_a_write() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "quota", None);
    assert_eq!(
        harness.wait_quota(Duration::from_secs(5)),
        Some(1_700_000_000_000)
    );
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::Quota {
            reset_at_ms: 1_700_000_000_000
        }
    );
    assert!(harness.written_ids().is_empty());
}

#[test]
fn an_echoed_id_is_not_written_again() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "echo", None);
    assert!(harness.wait_seen("echoed", Duration::from_secs(5)));
    harness.deliver(&msg("echoed")).unwrap();
    assert!(harness.written_ids().is_empty());
}

#[test]
fn resume_passes_the_session_and_the_id_is_the_first_line() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", Some("sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    let log = dir.path().join("log");
    assert!(wait_log(&log, "anyagents-message:m1"));
    assert!(read_log(&log).contains("argv:--output-format stream-json --resume sess-1"));
}

#[test]
fn without_the_language_server_mail_waits_for_the_turn_end() {
    use anyagents_harness_stream::StreamSpec;
    let dir = TempDir::new().unwrap();
    let log = dir.path().join("log");
    // A work directory but no agent config directory: no language server
    // to steer through, so a line for a busy agent waits for the turn.
    let mut harness = AgyHarness::start_with_spec(
        StreamSpec::for_program("agy"),
        env!("CARGO_BIN_EXE_fake_agy"),
        None,
        &[
            ("ANYAGENTS_FAKE_MODE", "hold"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
        ],
        Some(dir.path()),
    )
    .expect("start");
    assert_eq!(harness.health().delivery.level, DeliveryLevel::TurnEnd);
    harness.deliver(&msg("m1")).unwrap();
    harness.deliver(&msg("m2")).unwrap();
    assert_eq!(harness.turns_started(), 1);
    assert!(wait_log(&log, "anyagents-message:m2"));
}

#[test]
fn with_the_language_server_mail_reaches_the_running_turn() {
    use anyagents_harness_stream::StreamSpec;
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    let harness = AgyHarness::start_with_spec(
        StreamSpec::for_program("agy"),
        env!("CARGO_BIN_EXE_fake_agy"),
        None,
        &[("AGY_CONFIG_DIR", config.to_str().unwrap())],
        Some(dir.path()),
    )
    .expect("start");
    let delivery = harness.health().delivery;
    assert_eq!(delivery, anyagents_harness_agy::DELIVERY);
    assert_eq!(delivery.level, DeliveryLevel::NextStep);
    assert!(delivery.idempotent);
}

#[cfg(unix)]
#[test]
fn agy_multi_step_next_step_steer_incorporates_into_step2_and_fails_closed() {
    use std::os::unix::fs::PermissionsExt as _;

    use anyagents_harness::{HarnessError, Proof, TurnState};
    use anyagents_harness_stream::StreamSpec;

    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let cfg_root = root.join("agent-cfg");
    let nested_agy_dir = cfg_root.join(".gemini").join("antigravity-cli");
    std::fs::create_dir_all(&nested_agy_dir).unwrap();

    let py = root.join("mock_agy.py");
    let sh = root.join("mock_agy");
    std::fs::write(
        &py,
        r#"
import base64, json, os, pathlib, socketserver, sys, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class FastHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    def server_bind(self):
        socketserver.TCPServer.server_bind(self)
        self.server_name = "127.0.0.1"
        self.server_port = self.server_address[1]

state_dir = pathlib.Path(os.environ["MOCK_STATE_DIR"])
home = pathlib.Path(os.environ["MOCK_AGY_HOME"])
trace_path = state_dir / "agy_trace.jsonl"
args = sys.argv[1:]
csrf = args[args.index("--csrf_token") + 1]
session = "cas-multi-step-1"
generated = home / "brain" / session / ".system_generated"
(generated / "messages" / "undelivered").mkdir(parents=True, exist_ok=True)
(generated / "logs").mkdir(parents=True, exist_ok=True)
transcript = generated / "logs" / "transcript_full.jsonl"

lock = threading.Lock()
steps = 0
in_turn = False
pending_steers = []
refuse_next = False
exit_on_next_steer = False

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def trace(event, **fields):
    with trace_path.open("a") as f:
        f.write(json.dumps({"event": event, **fields}) + "\n")

def varint(val):
    out = bytearray()
    while True:
        lo, val = val & 0x7F, val >> 7
        out.append(lo | (0x80 if val else 0))
        if not val:
            return bytes(out)

def cortex_step(text):
    data = text.encode()
    ui = b"\x12" + varint(len(data)) + data
    return b"\x08\x0e\x9a\x01" + varint(len(ui)) + ui

def append_transcript(source, kind, **fields):
    global steps
    row = {"step_index": steps, "source": source, "type": kind, "status": "DONE", "created_at": "2026-11-20T00:00:00Z"}
    row.update(fields)
    with transcript.open("a") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")
    emit({"event": "step_update", "step_update": {
        "conversation_id": session, "step_index": steps, "state": "DONE", "step_type": kind.lower(),
    }})
    steps += 1

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    def log_message(self, *a): pass
    def address_string(self): return "127.0.0.1"
    def do_POST(self):
        global refuse_next, exit_on_next_steer
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.headers.get("x-codeium-csrf-token") != csrf:
            return self.reply(403, {"code": "permission_denied", "message": "bad csrf"})
        req = json.loads(body or b"{}")
        method = self.path.rsplit("/", 1)[-1]
        trace("rpc", method=method, delivery=req.get("deliveryStrategy"))
        if method == "GetAllCascadeTrajectories":
            status = "CASCADE_RUN_STATUS_RUNNING" if in_turn else "CASCADE_RUN_STATUS_IDLE"
            return self.reply(200, {"trajectorySummaries": {session: {"status": status, "stepCount": steps}}})
        if method == "GetCascadeTrajectoryGeneratorMetadata":
            return self.reply(200, {"generatorMetadata": [{"plannerConfig": {"planModel": "MODEL_TEST"}}]})
        if method == "SendUserCascadeMessage":
            text = "".join(i.get("text", "") for i in (req.get("items") or []))
            if (state_dir / "die_on_steer").exists():
                trace("die_during_steer", text=text)
                os._exit(1)
            if (state_dir / "refuse_steer").exists():
                (state_dir / "refuse_steer").unlink(missing_ok=True)
                trace("refuse_steer", text=text)
                return self.reply(500, {"code": "unknown", "message": "cannot send message: there is an ongoing battle"})
            mail = str(uuid.uuid4())
            messages = generated / "messages"
            (messages / f"{mail}.json").write_text(json.dumps({
                "id": mail, "recipient": session, "sender": "user",
                "timestamp": "2026-11-20T00:00:01Z",
                "stepPayload": base64.b64encode(cortex_step(text)).decode(),
                "deliveryStrategy": req.get("deliveryStrategy"),
            }))
            (messages / "undelivered" / mail).write_text("")
            with lock:
                pending_steers.append((mail, text))
            trace("queued_steer", mail=mail, text=text, strategy=req.get("deliveryStrategy"))
            return self.reply(200, {})
        return self.reply(200, {})

    def reply(self, status, payload):
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

server = FastHTTPServer(("127.0.0.1", 0), Handler)
port = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()

logs = home / "log"
logs.mkdir(parents=True, exist_ok=True)
log_file = logs / "cli-mock.log"
log_file.write_text(
    f"I1120 00:00:00.000000 1 server.go:1] Starting language server process with pid {os.getpid()}\n"
    f"I1120 00:00:00.000000 1 server.go:1] Language server version: 1.5.13\n"
    f"I1120 00:00:00.000000 1 server.go:1] Language server listening on random port at {port} for HTTP\n"
)
link = home / "cli.log"
if link.exists() or link.is_symlink():
    link.unlink()
link.symlink_to(log_file.relative_to(home))

def watch_step2():
    global in_turn
    while True:
        gate = state_dir / "allow_step2"
        if gate.exists():
            with lock:
                batch = list(pending_steers)
                pending_steers.clear()
            if batch:
                gate.unlink(missing_ok=True)
                messages = generated / "messages"
                read_path = messages / "read.json"
                read = json.loads(read_path.read_text()) if read_path.exists() else {}
                incorporated = []
                for mail, text in batch:
                    (messages / "undelivered" / mail).unlink(missing_ok=True)
                    read[mail] = True
                    append_transcript("USER_EXPLICIT", "USER_INPUT", content=f"<USER_REQUEST>\n{text}\n</USER_REQUEST>")
                    body = "agy-steer-payload" if "agy-steer-payload" in text else text.splitlines()[-1]
                    incorporated.append(body)
                read_path.write_text(json.dumps(read, separators=(",", ":")))
                append_transcript("MODEL", "PLANNER_RESPONSE")
                out = "step2:incorporated:" + ",".join(incorporated)
                emit({"event": "message", "role": "assistant", "content": out})
                trace("consumed_before_result", out=out)
                in_turn = False
                emit({"event": "result", "result": {
                    "conversation_id": session, "status": "SUCCESS",
                    "usage": {"input_tokens": 12, "output_tokens": 8},
                }})
        time.sleep(0.01)

threading.Thread(target=watch_step2, daemon=True).start()

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    text = msg.get("text") or (msg.get("message") or {}).get("content") or ""
    trace("stdin", text=text)
    in_turn = True
    emit({"event": "init", "session_id": session})
    append_transcript("USER_EXPLICIT", "USER_INPUT", content=f"<USER_REQUEST>\n{text}\n</USER_REQUEST>")
    append_transcript("MODEL", "PLANNER_RESPONSE")
    emit({"event": "message", "role": "assistant", "content": "step1:started"})
"#,
    )
    .unwrap();
    std::fs::write(
        &sh,
        format!("#!/bin/sh\nexec python3 -u '{}' \"$@\"\n", py.display()),
    )
    .unwrap();
    std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();

    // Pass nested `<root>/.gemini/antigravity-cli` via `AGY_CONFIG_DIR` to prove
    // root normalization resolves `cli.log` and `brain/<cid>/.system_generated`.
    let mut harness = AgyHarness::start_with_spec(
        StreamSpec::for_program("agy"),
        &sh,
        None,
        &[
            ("AGY_CONFIG_DIR", nested_agy_dir.to_str().unwrap()),
            ("MOCK_AGY_HOME", nested_agy_dir.to_str().unwrap()),
            ("MOCK_STATE_DIR", root.to_str().unwrap()),
        ],
        Some(root),
    )
    .expect("agy starts");

    assert_eq!(harness.health().delivery.level, DeliveryLevel::NextStep);
    assert!(harness.health().delivery.idempotent);

    let mk = |id: &str, text: &str| OutboundMessage {
        id: id.to_string(),
        seq: 1,
        text: text.to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    };
    fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            thread::sleep(Duration::from_millis(10));
        }
    }

    // Step 1 opens turn on stdin and pauses mid-turn.
    let m1 = mk("m1", "step-1-task");
    let _ = harness.deliver(&m1).unwrap();
    wait_until("m1 proven in turn", || {
        harness.proven("m1")
            == Some(Proof::Taken {
                turn_finished: false,
            })
    });
    assert_eq!(harness.health().turn_state, TurnState::InTurn);

    // Mid-turn steer m2 goes over ConnectRPC SendUserCascadeMessage (NEXT_INVOCATION).
    let m2 = mk("m2", "agy-steer-payload");
    assert_eq!(harness.deliver(&m2).unwrap(), DeliverOutcome::InFlight);
    wait_until("SendUserCascadeMessage queued m2", || {
        std::fs::read_to_string(root.join("agy_trace.jsonl"))
            .unwrap_or_default()
            .contains("MESSAGE_DELIVERY_STRATEGY_NEXT_INVOCATION")
    });
    // ConnectRPC `{}` admission MUST remain InFlight until transcript / read.json proves it.
    thread::sleep(Duration::from_millis(80));
    assert_eq!(
        harness.proven("m2"),
        None,
        "ConnectRPC {{}} must not prove m2"
    );
    assert_eq!(harness.deliver(&m2).unwrap(), DeliverOutcome::InFlight);

    // Step 2 consumes m2 BEFORE closing the turn and incorporates it into output.
    std::fs::write(root.join("allow_step2"), "1").unwrap();
    wait_until("m1 and m2 finished in one turn", || {
        harness.proven("m1")
            == Some(Proof::Taken {
                turn_finished: true,
            })
            && harness.proven("m2")
                == Some(Proof::Taken {
                    turn_finished: true,
                })
    });
    assert_eq!(harness.turns_started(), 1);
    assert_eq!(harness.turns_served(), 2);
    let trace = std::fs::read_to_string(root.join("agy_trace.jsonl")).unwrap();
    assert!(
        trace.contains("step2:incorporated:agy-steer-payload"),
        "{trace}"
    );
    let transcript = std::fs::read_to_string(
        nested_agy_dir
            .join("brain")
            .join("cas-multi-step-1")
            .join(".system_generated")
            .join("logs")
            .join("transcript_full.jsonl"),
    )
    .unwrap();
    assert!(
        transcript.contains("anyagents-message:m2") && transcript.contains("agy-steer-payload"),
        "expected m2 USER_INPUT recorded in cascade transcript: {transcript}"
    );

    // Turn 2: ConnectRPC refusal never falsely proves m4; dying mid-steer returns Err(Dead).
    let m3 = mk("m3", "turn-2");
    let _ = harness.deliver(&m3).unwrap();
    wait_until("m3 taken", || harness.proven("m3").is_some());
    std::fs::write(root.join("refuse_steer"), "1").unwrap();
    let m4 = mk("m4", "refused-mid-turn");
    assert_eq!(harness.deliver(&m4).unwrap(), DeliverOutcome::InFlight);
    wait_until("m4 refused by language server", || {
        std::fs::read_to_string(root.join("agy_trace.jsonl"))
            .unwrap_or_default()
            .contains("\"event\": \"refuse_steer\"")
    });
    thread::sleep(Duration::from_millis(80));
    assert_eq!(
        harness.proven("m4"),
        None,
        "refused steer must never be proven"
    );

    // Child dies while a steer RPC is in flight -> adapter MUST return Err(Dead), never InFlight.
    std::fs::write(root.join("die_on_steer"), "1").unwrap();
    wait_until("agy child dead after retry steer hits crash", || {
        let _ = harness.deliver(&m4);
        !harness.health().process_alive
    });
    assert_eq!(harness.deliver(&m4), Err(HarnessError::Dead));
    assert_eq!(harness.background(), Err(HarnessError::Dead));
}
