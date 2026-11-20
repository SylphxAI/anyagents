//! The OMP adapter against the scripted `omp --mode rpc` the delivery
//! conformance suite uses (`tests/invariants/fakes/proto_omp.py`): what the
//! suite does not hold every harness to.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyagents_harness::{Context, DeliverOutcome, Harness, OutboundMessage, Proof, TurnUsage};
use anyagents_harness_omp::OmpHarness;
use anyagents_harness_stream::StreamSpec;
use tempfile::TempDir;

const VENDOR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/invariants/fakes/vendor.py"
);
const PATIENCE: Duration = Duration::from_secs(30);

/// One scripted OMP: its state directory. Every vendor runs the same
/// program, written once before any test starts a process: a script written
/// while another thread forks can be held open for writing by that fork and
/// fail to start (`ETXTBSY`).
struct Vendor {
    dir: TempDir,
}

fn program() -> &'static Path {
    static PROGRAM: OnceLock<(TempDir, PathBuf)> = OnceLock::new();
    let (_, program) = PROGRAM.get_or_init(|| {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = TempDir::new().unwrap();
        let program = dir.path().join("omp");
        std::fs::write(
            &program,
            format!(
                "#!/bin/sh\nexec /usr/bin/python3 -u '{VENDOR}' --fixture \"$OMP_FIXTURE\" --protocol omp \"$@\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        (dir, program)
    });
    program
}

impl Vendor {
    fn new() -> Self {
        program();
        Self {
            dir: TempDir::new().unwrap(),
        }
    }

    /// The adapter as the server starts it: the catalog entry's arguments,
    /// in the agent's working directory.
    fn start(&self, session: Option<&str>) -> OmpHarness {
        let mut spec = StreamSpec::for_program("omp");
        spec.args.push("--no-extensions".to_string());
        let fixture = self.dir.path().display().to_string();
        OmpHarness::start_with_spec(
            spec,
            program(),
            session,
            &[("OMP_FIXTURE", fixture.as_str())],
            Some(self.dir.path()),
        )
        .expect("omp starts")
    }

    /// Ask the vendor to act and wait until it has.
    fn command(&self, name: &str) {
        let path = self.dir.path().join(name);
        std::fs::write(&path, "").unwrap();
        until(&format!("the vendor acts on {name}"), || !path.exists());
    }

    /// The id of every input that reached the vendor.
    fn wire(&self) -> Vec<String> {
        read_ids(&self.dir.path().join("wire.jsonl"))
    }

    /// The id of every input its model saw.
    fn journal(&self) -> Vec<String> {
        read_ids(&self.dir.path().join("journal.jsonl"))
    }
}

fn read_ids(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            row["id"].as_str().unwrap().to_string()
        })
        .collect()
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn message(id: &str) -> OutboundMessage {
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

/// Offer `id` until the vendor proves it consumed it.
fn deliver(harness: &mut OmpHarness, id: &str) -> DeliverOutcome {
    let mut outcome = DeliverOutcome::Deferred;
    until(&format!("{id} is proved"), || {
        outcome = harness.deliver(&message(id)).unwrap();
        matches!(
            outcome,
            DeliverOutcome::Accepted { .. } | DeliverOutcome::Duplicate
        )
    });
    outcome
}

/// Offer `id` until it is written into the running turn.
fn steer(harness: &mut OmpHarness, id: &str) {
    until(&format!("{id} is written"), || {
        harness.deliver(&message(id)).unwrap() == DeliverOutcome::InFlight
    });
}

/// Offer `id` until it is written, and wait until OMP read it, so a `step`
/// commanded next takes it.
fn steer_read(harness: &mut OmpHarness, vendor: &Vendor, id: &str) {
    steer(harness, id);
    until(&format!("OMP read {id}"), || {
        vendor.wire().iter().any(|seen| seen == id)
    });
}

#[test]
fn omp_runs_in_rpc_mode_and_names_its_protocol_and_session() {
    let vendor = Vendor::new();
    let mut harness = vendor.start(None);
    until("the peer is known", || {
        harness.health().delivery.peer.is_some()
    });
    let health = harness.health();
    assert!(health.process_alive, "a launch without --mode rpc exits");
    assert_eq!(health.delivery.peer.as_deref(), Some("omp rpc/1"));
    assert_eq!(health.sign_in_fault, None, "steering mode all was taken");
    // OMP writes a new session only with its first reply, and refuses to
    // resume one it never wrote: it is named once it is on disk.
    deliver(&mut harness, "m1");
    assert_eq!(harness.session_id(), None);
    vendor.command("finish");
    until("the session is on disk", || harness.session_id().is_some());
    assert_eq!(harness.session_id().as_deref(), Some("omp-session-1"));
}

#[test]
fn a_run_that_died_before_its_first_reply_leaves_nothing_to_resume_or_recognise() {
    let vendor = Vendor::new();
    let mut first = vendor.start(None);
    assert_eq!(
        deliver(&mut first, "m1"),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    vendor.command("crash");
    until("the child is gone", || !first.health().process_alive);
    // Proved, so the host commits it; but no session reached the disk.
    assert_eq!(
        first.proven("m1"),
        Some(Proof::Taken {
            turn_finished: false
        })
    );
    assert_eq!(first.session_id(), None, "OMP would refuse to resume it");
    // A host that lost the commit hands m1 to a fresh session: the vendor's
    // own record shows it never arrived, and nothing it began was kept.
    let mut second = vendor.start(first.session_id().as_deref());
    assert_eq!(
        deliver(&mut second, "m1"),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(vendor.wire(), ["m1", "m1"]);
}

#[test]
fn a_session_omp_never_wrote_cannot_be_resumed() {
    let vendor = Vendor::new();
    let harness = vendor.start(Some("omp-session-9"));
    until("the child is gone", || !harness.health().process_alive);
    let exit = harness.exit_diagnostic().expect("an exit diagnostic");
    assert!(
        exit.contains("Session \"omp-session-9\" not found"),
        "{exit}"
    );
}

#[test]
fn a_crash_after_a_step_leaves_a_session_that_holds_what_it_took() {
    let vendor = Vendor::new();
    let mut first = vendor.start(None);
    deliver(&mut first, "m1");
    steer_read(&mut first, &vendor, "m2");
    steer_read(&mut first, &vendor, "m3");
    // The step after a tool batch takes m2 and dies before m3: the tool
    // call's reply put the session on disk.
    vendor.command("step-1");
    until("the child is gone", || !first.health().process_alive);
    first.stop();
    assert_eq!(
        first.proven("m2"),
        Some(Proof::Taken {
            turn_finished: false
        })
    );
    assert_eq!(first.proven("m3"), None);
    let session = first.session_id().expect("the session is on disk");
    let mut second = vendor.start(Some(&session));
    for id in ["m1", "m2"] {
        assert_eq!(deliver(&mut second, id), DeliverOutcome::Duplicate, "{id}");
    }
    assert_eq!(
        deliver(&mut second, "m3"),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(vendor.journal(), ["m1", "m2", "m3"]);
}

#[test]
fn a_finished_run_counts_one_turn_and_reports_its_usage() {
    let vendor = Vendor::new();
    let mut harness = vendor.start(None);
    assert_eq!(
        deliver(&mut harness, "m1"),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    vendor.command("finish");
    until("the turn ends", || harness.turns_served() == 1);
    assert_eq!(
        harness.deliver(&message("m1")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    assert_eq!(
        harness.served_usage(),
        TurnUsage {
            input_tokens: 3,
            output_tokens: 2,
            cache_read_tokens: 1,
            cache_write_tokens: 0,
            reported_cost_micros: Some(5),
        }
    );
    assert_eq!(vendor.wire(), ["m1"], "written once");
}

#[test]
fn a_resumed_session_recognises_what_an_earlier_process_put_into_it() {
    let vendor = Vendor::new();
    let mut first = vendor.start(None);
    deliver(&mut first, "m1");
    steer_read(&mut first, &vendor, "m2");
    vendor.command("step");
    deliver(&mut first, "m2");
    vendor.command("finish");
    let session = first.session_id().unwrap();
    drop(first);
    let mut second = vendor.start(Some(&session));
    for id in ["m1", "m2"] {
        assert_eq!(deliver(&mut second, id), DeliverOutcome::Duplicate, "{id}");
    }
    assert_eq!(
        deliver(&mut second, "m3"),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(second.session_id(), Some(session));
    assert_eq!(vendor.wire(), ["m1", "m2", "m3"]);
    assert_eq!(vendor.journal(), ["m1", "m2", "m3"]);
}

#[test]
fn a_dead_child_keeps_its_proofs_and_reports_its_exit() {
    let vendor = Vendor::new();
    let mut harness = vendor.start(None);
    deliver(&mut harness, "m1");
    vendor.command("crash");
    until("the child is gone", || !harness.health().process_alive);
    assert_eq!(
        harness.deliver(&message("m1")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(
        harness.deliver(&message("m2")),
        Err(anyagents_harness::HarnessError::Dead)
    );
    let exit = harness.exit_diagnostic().expect("an exit diagnostic");
    assert!(exit.starts_with("omp "), "{exit}");
}

#[test]
fn omp_multi_step_next_step_steer_incorporates_into_step2_and_fails_closed() {
    use std::os::unix::fs::PermissionsExt as _;

    use anyagents_harness::{DeliveryLevel, HarnessError, TurnState};

    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let py = root.join("mock_omp.py");
    let sh = root.join("mock_omp");
    std::fs::write(
        &py,
        r#"
import json, os, pathlib, sys, time

state_dir = pathlib.Path(os.environ["MOCK_STATE_DIR"])
session_file = state_dir / "omp-multi.jsonl"
trace_path = state_dir / "omp_trace.jsonl"

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

emit({"type": "ready", "protocol": "omp rpc/1"})
turn_requests = []

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    cmd = json.loads(raw)
    kind = cmd.get("type")
    rid = cmd.get("id")
    trace("rx", kind=kind, id=rid, behavior=cmd.get("streamingBehavior"))

    if kind == "set_steering_mode":
        emit({"type": "response", "id": rid, "command": "set_steering_mode", "success": True})
    elif kind == "get_state":
        emit({
            "type": "response",
            "id": rid,
            "command": "get_state",
            "success": True,
            "data": {
                "sessionId": "omp-multi",
                "sessionFile": str(session_file),
                "steeringMode": "all",
            },
        })
    elif kind == "prompt":
        assert cmd.get("streamingBehavior") == "steer", f"expected steer, got {cmd}"
        text = cmd.get("message", "")
        marker = text.splitlines()[0]
        payload = "omp-steer-payload" if "omp-steer-payload" in text else text.splitlines()[-1]
        if marker == "anyagents-message:m_refused_no_cmd":
            # Omit `command` to verify error response still clears `unadmitted`.
            emit({"type": "response", "id": rid, "success": False, "error": "busy"})
            continue
        if marker == "anyagents-message:m_die":
            sys.exit(0)
        emit({"type": "response", "id": rid, "command": "prompt", "success": True})
        if marker == "anyagents-message:m1":
            turn_requests = [rid]
            emit({"type": "message_end", "message": {"role": "user", "content": text}})
        elif marker == "anyagents-message:m2":
            turn_requests.append(rid)
            # Hold `message_end` behind gate to verify `response success:true` alone stays InFlight.
            wait_gate("allow_step2")
            emit({"type": "message_end", "message": {"role": "user", "content": text}})
            out = f"step2:incorporated:{payload}"
            session_file.write_text(json.dumps({"type": "message", "message": {"role": "user", "content": text}}) + "\n")
            emit({
                "type": "message_end",
                "message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": out}],
                    "usage": {"input": 9, "output": 5, "cacheRead": 0, "cacheWrite": 0, "cost": {"total": 0.0001}},
                },
            })
            trace("consumed_before_result", out=out)
            for req in turn_requests:
                emit({"type": "prompt_result", "id": req, "status": "completed"})
            turn_requests = []
        elif marker == "anyagents-message:m_after_refusal":
            emit({"type": "message_end", "message": {"role": "user", "content": text}})
            emit({"type": "message_end", "message": {"role": "assistant", "content": [{"type": "text", "text": "recovered"}]}})
            emit({"type": "prompt_result", "id": rid, "status": "completed"})
"#,
    )
    .unwrap();
    std::fs::write(
        &sh,
        format!("#!/bin/sh\nexec python3 -u '{}' \"$@\"\n", py.display()),
    )
    .unwrap();
    std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut harness = OmpHarness::start_with_spec(
        StreamSpec::for_program("omp"),
        &sh,
        None,
        &[("MOCK_STATE_DIR", root.to_str().unwrap())],
        Some(root),
    )
    .expect("omp mock starts");

    until("omp peer known", || {
        harness.health().delivery.peer.is_some()
    });
    assert_eq!(harness.health().delivery.level, DeliveryLevel::NextStep);
    assert!(harness.health().delivery.idempotent);
    assert_eq!(harness.health().sign_in_fault, None);

    let mk = |id: &str, text: &str| OutboundMessage {
        id: id.to_string(),
        seq: 1,
        text: text.to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    };

    // Step 1 opens turn and pauses mid-turn.
    let m1 = mk("m1", "step-1-work");
    until("m1 taken", || {
        harness.deliver(&m1).unwrap()
            == DeliverOutcome::Accepted {
                turn_finished: false,
            }
    });
    assert_eq!(harness.health().turn_state, TurnState::InTurn);

    // Mid-turn steer m2: `response { success: true }` alone MUST stay InFlight.
    let m2 = mk("m2", "omp-steer-payload");
    until("m2 in flight", || {
        harness.deliver(&m2).unwrap() == DeliverOutcome::InFlight
    });
    until("mock received m2", || {
        std::fs::read_to_string(root.join("omp_trace.jsonl"))
            .unwrap_or_default()
            .contains("\"behavior\": \"steer\"")
            && std::fs::read_to_string(root.join("omp_trace.jsonl"))
                .unwrap_or_default()
                .matches("\"kind\": \"prompt\"")
                .count()
                >= 2
    });
    std::thread::sleep(Duration::from_millis(60));
    assert_eq!(
        harness.proven("m2"),
        None,
        "OMP response success:true alone must not prove m2"
    );
    assert_eq!(harness.deliver(&m2).unwrap(), DeliverOutcome::InFlight);

    // Step 2 consumes m2 BEFORE closing turn and incorporates it into Step 2 assistant text.
    std::fs::write(root.join("allow_step2"), "1").unwrap();
    until("m1 and m2 finished in one turn", || {
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
    let (_, reply) = harness.task_turn_result("m2").expect("task turn result");
    assert!(
        reply.contains("step2:incorporated:omp-steer-payload"),
        "{reply}"
    );

    // Error response WITHOUT `command` field must return Deferred cleanly (no deadlock!).
    let m_ref = mk("m_refused_no_cmd", "refused");
    assert_eq!(harness.deliver(&m_ref).unwrap(), DeliverOutcome::InFlight);
    until("m_refused_no_cmd returned as Deferred", || {
        harness.deliver(&m_ref).unwrap() == DeliverOutcome::Deferred
    });
    assert_eq!(harness.proven("m_refused_no_cmd"), None);

    // Queue is NOT wedged: next message succeeds cleanly.
    let m_ok = mk("m_after_refusal", "go");
    until("m_after_refusal accepted", || {
        matches!(
            harness.deliver(&m_ok).unwrap(),
            DeliverOutcome::Accepted { .. }
        )
    });

    // Dead child fail-closed: deliver returns Err(HarnessError::Dead).
    let _ = harness.deliver(&mk("m_die", "bye"));
    until("omp mock dead", || !harness.health().process_alive);
    assert_eq!(
        harness.deliver(&mk("m_after_death", "nope")),
        Err(HarnessError::Dead)
    );
}

#[test]
fn omp_steering_mode_refusal_honestly_downgrades_delivery_level_to_turn_end() {
    use std::os::unix::fs::PermissionsExt as _;

    use anyagents_harness::DeliveryLevel;

    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let py = root.join("mock_omp_no_all.py");
    let sh = root.join("mock_omp_no_all");
    std::fs::write(
        &py,
        r#"
import json, sys

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

emit({"type": "ready", "protocol": "omp rpc/1"})
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    cmd = json.loads(raw)
    kind = cmd.get("type")
    rid = cmd.get("id")
    if kind == "set_steering_mode":
        emit({"type": "response", "id": rid, "command": "set_steering_mode", "success": False, "error": "unsupported mode"})
    elif kind == "get_state":
        emit({
            "type": "response",
            "id": rid,
            "command": "get_state",
            "success": True,
            "data": {"sessionId": "omp-no-all", "steeringMode": "one-at-a-time"},
        })
"#,
    )
    .unwrap();
    std::fs::write(
        &sh,
        format!("#!/bin/sh\nexec python3 -u '{}' \"$@\"\n", py.display()),
    )
    .unwrap();
    std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();

    let harness =
        OmpHarness::start_with_spec(StreamSpec::for_program("omp"), &sh, None, &[], Some(root))
            .expect("omp mock starts");

    until("steering fault recorded", || {
        harness.health().sign_in_fault.is_some()
    });
    let health = harness.health();
    assert_eq!(
        health.delivery.level,
        DeliveryLevel::TurnEnd,
        "refused steeringMode=all must downgrade DeliveryLevel honestly"
    );
    assert!(
        health
            .sign_in_fault
            .as_deref()
            .is_some_and(|f| f.contains("steering")),
        "{:?}",
        health.sign_in_fault
    );
}
