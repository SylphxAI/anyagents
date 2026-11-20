//! Deterministic mock-process protocol verification for turn-end-only
//! (`DeliveryLevel::TurnEnd`, `steer_support == "no"`) stream harnesses such as
//! `gemini` / `gemini-cli`, proving honest capability reporting, no mid-turn
//! injection into an active turn, and fail-closed `Err(HarnessError::Dead)` on
//! child exit.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;
use std::time::{Duration, Instant};

use anyagents_harness::{
    Context, DeliveryLevel, DeliveryMode, HarnessError, OutboundMessage, TurnState,
};
use anyagents_harness_stream::{LINE_PER_TURN, Launch, StreamHarness, StreamSpec, message_line};
use tempfile::TempDir;

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

#[test]
fn gemini_turn_end_harness_declares_no_mid_turn_steer_and_fails_closed_on_exit() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let py = root.join("mock_gemini.py");
    let sh = root.join("mock_gemini");
    std::fs::write(
        &py,
        r#"
import json, os, pathlib, sys, time

state_dir = pathlib.Path(os.environ["MOCK_STATE_DIR"])
trace_path = state_dir / "gemini_trace.jsonl"

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def wait_gate(name):
    gate = state_dir / name
    deadline = time.time() + 10.0
    while not gate.exists():
        if time.time() > deadline:
            sys.exit(99)
        time.sleep(0.01)

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    frame = json.loads(raw)
    text = frame.get("text") or (frame.get("message") or {}).get("content") or ""
    with trace_path.open("a") as f:
        f.write(json.dumps({"text": text}) + "\n")
    if "anyagents-message:m1" in text:
        emit({"type": "init", "session_id": "gem-sess-1"})
        wait_gate("finish_m1")
        emit({"type": "result", "session_id": "gem-sess-1", "usage": {"input_tokens": 5, "output_tokens": 3}})
    elif "anyagents-message:m2" in text:
        emit({"type": "result", "session_id": "gem-sess-1", "usage": {"input_tokens": 4, "output_tokens": 2}})
        sys.exit(0)
"#,
    )
    .unwrap();
    std::fs::write(
        &sh,
        format!("#!/bin/sh\nexec python3 -u '{}' \"$@\"\n", py.display()),
    )
    .unwrap();
    std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();

    let spec = StreamSpec::for_program("gemini");
    let mut harness = StreamHarness::launch(Launch {
        spec: spec.clone(),
        program: sh,
        args: spec.args.clone(),
        environment: vec![("MOCK_STATE_DIR".into(), root.display().to_string())],
        work_dir: Some(root.to_path_buf()),
        session_id: None,
        mode: DeliveryMode::Followup,
        delivery: LINE_PER_TURN,
    })
    .expect("launch gemini turn-end stream");

    // Honest declaration: TurnEnd, non-idempotent (`steer_support == "no"`).
    let health = harness.health();
    assert_eq!(health.delivery.level, DeliveryLevel::TurnEnd);
    assert!(!health.delivery.idempotent);
    assert_eq!(
        serde_json::to_value(health.delivery.level).unwrap(),
        serde_json::json!("turn-end")
    );

    // Turn 1 opens and pauses mid-turn.
    let m1 = msg("m1", "turn-1");
    let _ = harness.deliver(&m1, &message_line(&spec, &m1)).unwrap();
    assert_eq!(harness.health().turn_state, TurnState::InTurn);

    // While Turn 1 is active, Followup mode must NOT start a second turn concurrently.
    let m2 = msg("m2", "queued-for-next-turn");
    let _ = harness.deliver(&m2, &message_line(&spec, &m2)).unwrap();
    assert_eq!(harness.turns_started(), 1);

    // Finish Turn 1; Turn 2 then completes and the child exits.
    std::fs::write(root.join("finish_m1"), "1").unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    assert!(harness.wait_finished("m2", Duration::from_secs(5)));
    assert_eq!(harness.turns_served(), 2);

    let deadline = Instant::now() + Duration::from_secs(5);
    while harness.health().process_alive {
        assert!(Instant::now() < deadline, "mock gemini never exited");
        std::thread::sleep(Duration::from_millis(10));
    }

    // Dead process fails closed with Err(HarnessError::Dead).
    let m3 = msg("m3", "after-exit");
    assert_eq!(
        harness.deliver(&m3, &message_line(&spec, &m3)),
        Err(HarnessError::Dead)
    );
    assert_eq!(harness.background(), Err(HarnessError::Dead));
}
