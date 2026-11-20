//! Deterministic multi-step mock-process protocol tests for Spiron headless
//! steering (both `spiron.headless/v2` over stdio and Unix socket `--listen`,
//! plus legacy `v1` honest downgrade to `TurnEnd`).
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryLevel, Harness, HarnessError, OutboundMessage, Proof,
    TurnState,
};
use anyagents_harness_spiron::{SpironHarness, SpironLaunch};
use tempfile::TempDir;

const PATIENCE: Duration = Duration::from_secs(10);

fn msg(id: &str, text: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.to_string(),
        seq: 1,
        text: text.to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    }
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn write_mock_executable(dir: &Path, name: &str, py_body: &str) -> PathBuf {
    let py = dir.join(format!("{name}.py"));
    let sh = dir.join(name);
    std::fs::write(&py, py_body).unwrap();
    std::fs::write(
        &sh,
        format!("#!/bin/sh\nexec python3 -u '{}' \"$@\"\n", py.display()),
    )
    .unwrap();
    std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();
    sh
}

#[test]
fn spiron_stdio_v2_multi_step_next_step_steer_incorporates_into_step2_and_fails_closed() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let script = write_mock_executable(
        root,
        "mock_spiron_stdio_v2",
        r#"
import json, os, pathlib, sys, time

state_dir = pathlib.Path(os.environ["MOCK_STATE_DIR"])
trace_path = state_dir / "trace.jsonl"

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def trace(event, **fields):
    with trace_path.open("a") as f:
        f.write(json.dumps({"event": event, **fields}) + "\n")

def wait_gate(name):
    gate = state_dir / name
    deadline = time.time() + 10.0
    while not gate.exists():
        if time.time() > deadline:
            sys.exit(99)
        time.sleep(0.01)

emit({
    "type": "ready",
    "protocol": "spiron.headless/v2",
    "session_id": "sp-stdio-v2",
    "capabilities": ["steer_consumed", "idempotent_input", "turn_tool_context"],
})

active_turn = None
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    frame = json.loads(raw)
    kind = frame.get("type")
    rid = frame.get("request_id", "")
    text = frame.get("text", "")
    trace("rx", kind=kind, request_id=rid, text=text)

    if kind == "prompt":
        if rid == "m_empty":
            emit({"type": "rejected", "request_id": rid, "code": "empty_prompt"})
            continue
        if rid == "m1":
            wait_gate("allow_accept_m1")
            active_turn = rid
            emit({"type": "accepted", "request_id": rid})
            emit({"type": "text_delta", "request_id": rid, "delta": "step1:started;"})
            continue
        if rid == "m_undelivered_open":
            active_turn = rid
            emit({"type": "accepted", "request_id": rid})
            continue
        if rid == "m_closing_open":
            active_turn = rid
            emit({"type": "accepted", "request_id": rid})
            continue
    elif kind == "steer":
        if rid == "m2":
            emit({"type": "steer_ack", "request_id": rid, "accepted": True, "turn_id": active_turn})
            wait_gate("allow_step2")
            payload = "redirect-to-step-2" if "redirect-to-step-2" in text else text.splitlines()[-1]
            emit({
                "type": "steer_consumed",
                "turn_id": active_turn,
                "steer_request_id": rid,
            })
            step2_text = f"step1:started;step2:incorporated:{payload}"
            emit({"type": "text_delta", "request_id": active_turn, "delta": f"step2:incorporated:{payload}"})
            trace("consumed_before_result", steer=rid, turn=active_turn, text=step2_text)
            emit({
                "type": "result",
                "request_id": active_turn,
                "session_id": "sp-stdio-v2",
                "text": step2_text,
                "usage": {"input_tokens": 13, "output_tokens": 9, "cost_usd": 0.00025},
            })
            active_turn = None
            continue
        if rid == "m_undelivered_steer":
            emit({"type": "steer_ack", "request_id": rid, "accepted": True, "turn_id": active_turn})
            emit({
                "type": "result",
                "request_id": active_turn,
                "session_id": "sp-stdio-v2",
                "text": "closed-without-steer",
                "undelivered_steer_ids": [rid],
                "usage": {"input_tokens": 4, "output_tokens": 2},
            })
            active_turn = None
            continue
        if rid == "m_turn_closed":
            emit({"type": "rejected", "request_id": rid, "code": "turn_closed"})
            emit({
                "type": "result",
                "request_id": active_turn,
                "session_id": "sp-stdio-v2",
                "text": "closing-turn-finished",
                "usage": {"input_tokens": 3, "output_tokens": 1},
            })
            active_turn = None
            continue
    elif kind == "cancel":
        emit({"type": "cancelled", "request_id": rid})
        active_turn = None
        continue
    elif kind == "stop":
        sys.exit(0)
"#,
    );

    let mut harness = SpironHarness::launch_with_patience(
        SpironLaunch {
            program: script,
            args: vec!["--headless".into(), "--json".into()],
            environment: vec![("MOCK_STATE_DIR".into(), root.display().to_string())],
            work_dir: root.to_path_buf(),
            session: None,
            state_dir: root.to_path_buf(),
        },
        PATIENCE,
    )
    .expect("stdio v2 launch");

    let health = harness.health();
    assert_eq!(health.delivery.level, DeliveryLevel::NextStep);
    assert!(health.delivery.idempotent);
    assert!(harness.proves_consumption());
    assert!(harness.supports_turn_tool_context());

    // Step 1: prompt m1 stays InFlight until `accepted` arrives (never fabricated on write).
    assert_eq!(
        harness.deliver(&msg("m1", "step-1-initial")).unwrap(),
        DeliverOutcome::InFlight
    );
    assert_eq!(harness.proven("m1"), None);
    std::fs::write(root.join("allow_accept_m1"), "1").unwrap();
    until("m1 accepted", || {
        harness.proven("m1")
            == Some(Proof::Taken {
                turn_finished: false,
            })
    });
    assert_eq!(harness.health().turn_state, TurnState::InTurn);

    // Mid-turn steer m2: `steer_ack` alone MUST remain InFlight (never falsely proven).
    assert_eq!(
        harness.deliver(&msg("m2", "redirect-to-step-2")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("mock received steer m2", || {
        std::fs::read_to_string(root.join("trace.jsonl"))
            .unwrap_or_default()
            .contains("\"kind\": \"steer\", \"request_id\": \"m2\"")
    });
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(harness.proven("m2"), None, "steer_ack must not prove m2");
    assert_eq!(
        harness.deliver(&msg("m2", "redirect-to-step-2")).unwrap(),
        DeliverOutcome::InFlight
    );

    // Step 2 consumes m2 BEFORE closing the turn and incorporates it into Step 2 output.
    std::fs::write(root.join("allow_step2"), "1").unwrap();
    until("turn 1 finished for m1 and m2", || {
        harness.proven("m1")
            == Some(Proof::Taken {
                turn_finished: true,
            })
            && harness.proven("m2")
                == Some(Proof::Taken {
                    turn_finished: true,
                })
    });
    assert_eq!(harness.turns_served(), 1);
    assert_eq!(harness.health().turn_state, TurnState::Idle);
    assert_eq!(
        harness.text_for("m2").as_deref(),
        Some("step1:started;step2:incorporated:redirect-to-step-2")
    );
    let (opener, result_text) = harness.task_turn_result("m2").expect("task turn result");
    assert_eq!(opener, "m1");
    assert!(result_text.contains("step2:incorporated:redirect-to-step-2"));
    assert_eq!(
        harness.usage_for("m1").unwrap().reported_cost_micros,
        Some(250)
    );

    // Permanent rejection (`empty_prompt`) -> DeliverOutcome::Rejected, never proven.
    assert_eq!(
        harness.deliver(&msg("m_empty", "")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("m_empty rejected", || {
        matches!(
            harness.deliver(&msg("m_empty", "")).unwrap(),
            DeliverOutcome::Rejected {
                message_specific: true,
                ..
            }
        )
    });
    assert_eq!(harness.proven("m_empty"), None);

    // Undelivered steer on turn close -> NEVER falsely proven.
    assert_eq!(
        harness.deliver(&msg("m_undelivered_open", "t2")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("t2 accepted", || {
        harness.proven("m_undelivered_open").is_some()
    });
    assert_eq!(
        harness
            .deliver(&msg("m_undelivered_steer", "late"))
            .unwrap(),
        DeliverOutcome::InFlight
    );
    until("t2 finished", || harness.turns_served() == 2);
    assert_eq!(
        harness.proven("m_undelivered_steer"),
        None,
        "undelivered_steer_ids must never be marked proven"
    );

    // `turn_closed` rejection on mid-turn steer -> Deferred, never proven.
    assert_eq!(
        harness.deliver(&msg("m_closing_open", "t3")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("t3 accepted", || harness.proven("m_closing_open").is_some());
    assert_eq!(
        harness.deliver(&msg("m_turn_closed", "race")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("t3 finished", || harness.turns_served() == 3);
    assert_eq!(harness.proven("m_turn_closed"), None);

    // Dead child fail-closed: deliver/background/cancel return Err(Dead), proofs persist.
    harness.stop();
    assert!(!harness.health().process_alive);
    assert_eq!(
        harness.deliver(&msg("m2", "redirect-to-step-2")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    assert_eq!(
        harness.deliver(&msg("m_dead", "after-exit")),
        Err(HarnessError::Dead)
    );
    assert_eq!(harness.background(), Err(HarnessError::Dead));
    assert_eq!(harness.cancel(), Err(HarnessError::Dead));
}

#[test]
fn spiron_stdio_v1_honestly_downgrades_to_turn_end_without_false_steer_claim() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let script = write_mock_executable(
        root,
        "mock_spiron_stdio_v1",
        r#"
import json, os, pathlib, sys

trace_path = pathlib.Path(os.environ["MOCK_STATE_DIR"]) / "v1_trace.jsonl"

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

emit({"type": "ready", "protocol": "spiron.headless/v1", "session_id": "sp-v1"})
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    frame = json.loads(raw)
    with trace_path.open("a") as f:
        f.write(json.dumps(frame) + "\n")
    if frame.get("type") == "prompt":
        emit({"type": "accepted", "request_id": frame["request_id"]})
    elif frame.get("type") == "stop":
        sys.exit(0)
"#,
    );

    let mut harness = SpironHarness::launch_with_patience(
        SpironLaunch {
            program: script,
            args: vec!["--headless".into(), "--json".into()],
            environment: vec![("MOCK_STATE_DIR".into(), root.display().to_string())],
            work_dir: root.to_path_buf(),
            session: None,
            state_dir: root.to_path_buf(),
        },
        PATIENCE,
    )
    .expect("stdio v1 launch");

    let health = harness.health();
    assert_eq!(health.delivery.level, DeliveryLevel::TurnEnd);
    assert!(!health.delivery.idempotent);
    assert!(!harness.proves_consumption());

    assert_eq!(
        harness.deliver(&msg("m1", "first")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("m1 accepted", || harness.proven("m1").is_some());
    // Mid-turn steer MUST defer cleanly rather than falsely claiming next-step.
    assert_eq!(
        harness.deliver(&msg("m2", "second")).unwrap(),
        DeliverOutcome::Deferred
    );
    assert_eq!(harness.proven("m2"), None);
    let trace = std::fs::read_to_string(root.join("v1_trace.jsonl")).unwrap_or_default();
    assert!(!trace.contains("\"type\": \"steer\""), "{trace}");
    harness.stop();
}

#[test]
fn spiron_socket_v2_multi_step_next_step_steer_incorporates_into_step2_and_fails_closed() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let sock = root.join("spiron.sock");
    let log = root.join("spiron.log");
    let script = write_mock_executable(
        root,
        "mock_spiron_socket_v2",
        r#"
import json, os, pathlib, socket, sys, time

args = sys.argv[1:]
sock_path = args[args.index("--listen") + 1]
state_dir = pathlib.Path(os.environ["MOCK_STATE_DIR"])
trace_path = state_dir / "sock_trace.jsonl"

if os.path.exists(sock_path):
    os.unlink(sock_path)

srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(sock_path)
srv.listen(4)

seq = 0
def send(conn, obj):
    global seq
    seq += 1
    obj = dict(obj, seq=seq)
    conn.sendall((json.dumps(obj) + "\n").encode())

def wait_gate(name):
    gate = state_dir / name
    deadline = time.time() + 10.0
    while not gate.exists():
        if time.time() > deadline:
            sys.exit(99)
        time.sleep(0.01)

while True:
    conn, _ = srv.accept()
    f = conn.makefile("r", encoding="utf-8")
    first = f.readline()
    if not first:
        conn.close()
        continue
    attach = json.loads(first)
    if attach.get("type") != "attach":
        conn.close()
        continue
    send(conn, {
        "type": "ready",
        "protocol": "spiron.headless/v2",
        "version": "0.1.0",
        "request_id": attach.get("request_id", "attach-1"),
        "session_id": "sp-sock-v2",
        "capabilities": ["steer_consumed", "idempotent_input"],
        "features": ["turn-tool-context-v1"],
        "in_turn": False,
    })
    active_turn = None
    for raw in f:
        raw = raw.strip()
        if not raw:
            continue
        frame = json.loads(raw)
        kind = frame.get("type")
        rid = frame.get("request_id", "")
        text = frame.get("text", "")
        with trace_path.open("a") as tf:
            tf.write(json.dumps({"kind": kind, "request_id": rid, "text": text}) + "\n")
        if kind == "prompt":
            if rid == "m_empty":
                send(conn, {"type": "rejected", "request_id": rid, "session_id": "sp-sock-v2", "code": "empty_prompt"})
                continue
            if rid == "m_die":
                conn.close()
                srv.close()
                sys.exit(0)
            active_turn = rid
            send(conn, {"type": "accepted", "request_id": rid, "session_id": "sp-sock-v2", "mode": "turn"})
            send(conn, {"type": "text_delta", "request_id": rid, "session_id": "sp-sock-v2", "text": "sock-step1;"})
        elif kind == "steer":
            if rid == "m2":
                send(conn, {"type": "steer_ack", "request_id": rid, "session_id": "sp-sock-v2", "accepted": True, "turn_id": active_turn})
                wait_gate("allow_sock_step2")
                payload = "socket-steer-payload" if "socket-steer-payload" in text else text.splitlines()[-1]
                send(conn, {
                    "type": "steer_consumed",
                    "request_id": active_turn,
                    "session_id": "sp-sock-v2",
                    "turn_id": active_turn,
                    "steer_request_id": rid,
                    "duplicate": False,
                })
                final = f"sock-step1;sock-step2:incorporated:{payload}"
                send(conn, {"type": "text_delta", "request_id": active_turn, "session_id": "sp-sock-v2", "text": f"sock-step2:incorporated:{payload}"})
                send(conn, {
                    "type": "result",
                    "request_id": active_turn,
                    "session_id": "sp-sock-v2",
                    "status": "completed",
                    "is_error": False,
                    "error": None,
                    "duplicate": False,
                    "tool_context_bound": True,
                    "consumed_request_ids": [active_turn, rid],
                    "final_text": final,
                    "text": final,
                    "usage": {"input_tokens": 10, "output_tokens": 6, "cached_tokens": 0},
                    "cost_micros": 100,
                    "undelivered_steer_ids": [],
                })
                active_turn = None
        elif kind == "stop":
            conn.close()
            srv.close()
            sys.exit(0)
    conn.close()
"#,
    );

    let mut harness = SpironHarness::launch_listening(
        SpironLaunch {
            program: script,
            args: vec!["--headless".into(), "--json".into()],
            environment: vec![("MOCK_STATE_DIR".into(), root.display().to_string())],
            work_dir: root.to_path_buf(),
            session: None,
            state_dir: root.to_path_buf(),
        },
        &sock,
        &log,
        PATIENCE,
        |_| Ok(()),
    )
    .expect("socket v2 launch");

    assert!(harness.listening());
    assert_eq!(harness.health().delivery.level, DeliveryLevel::NextStep);
    assert!(harness.health().delivery.idempotent);
    assert!(harness.proves_consumption());
    assert!(harness.supports_turn_tool_context());

    // Step 1 opens turn and pauses mid-turn.
    assert_eq!(
        harness.deliver(&msg("m1", "sock-start")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("m1 accepted over socket", || {
        harness.proven("m1")
            == Some(Proof::Taken {
                turn_finished: false,
            })
    });
    assert_eq!(harness.health().turn_state, TurnState::InTurn);

    // Mid-turn steer m2: adapter rewrites to `{"type":"steer", ...}`, `steer_ack` stays InFlight.
    assert_eq!(
        harness.deliver(&msg("m2", "socket-steer-payload")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("mock received socket steer", || {
        std::fs::read_to_string(root.join("sock_trace.jsonl"))
            .unwrap_or_default()
            .contains("\"kind\": \"steer\", \"request_id\": \"m2\"")
    });
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(harness.proven("m2"), None, "steer_ack must not prove m2");

    // Step 2 consumes steer before result and incorporates it into Step 2 output.
    std::fs::write(root.join("allow_sock_step2"), "1").unwrap();
    until("m1 and m2 finished in 1 turn", || {
        harness.proven("m1")
            == Some(Proof::Taken {
                turn_finished: true,
            })
            && harness.proven("m2")
                == Some(Proof::Taken {
                    turn_finished: true,
                })
    });
    assert_eq!(harness.turns_served(), 2);
    assert_eq!(harness.health().turn_state, TurnState::Idle);
    let (opener, text) = harness.task_turn_result("m2").expect("task turn result");
    assert_eq!(opener, "m1");
    assert!(
        text.contains("sock-step2:incorporated:socket-steer-payload"),
        "{text}"
    );

    // Permanent rejection returns Rejected, never proven.
    assert_eq!(
        harness.deliver(&msg("m_empty", "")).unwrap(),
        DeliverOutcome::InFlight
    );
    until("socket m_empty rejected", || {
        matches!(
            harness.deliver(&msg("m_empty", "")).unwrap(),
            DeliverOutcome::Rejected {
                message_specific: true,
                ..
            }
        )
    });
    assert_eq!(harness.proven("m_empty"), None);

    // Server closes/exits -> Err(HarnessError::Dead) fail-closed.
    let _ = harness.deliver(&msg("m_die", "bye"));
    until("socket process dead", || !harness.health().process_alive);
    assert_eq!(
        harness.deliver(&msg("m_after_death", "nope")),
        Err(HarnessError::Dead)
    );
    assert_eq!(harness.background(), Err(HarnessError::Dead));
}
