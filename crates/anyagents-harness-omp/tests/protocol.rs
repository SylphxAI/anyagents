//! The OMP adapter against the scripted `omp --mode rpc` the delivery
//! conformance suite uses (`tests/invariants/fakes/proto_omp.py`): what the
//! suite does not hold every harness to.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyagents_harness::{Context, DeliverOutcome, Harness, OutboundMessage, TurnUsage};
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

#[test]
fn omp_runs_in_rpc_mode_and_names_its_protocol_and_session() {
    let vendor = Vendor::new();
    let harness = vendor.start(None);
    until("the session is known", || harness.session_id().is_some());
    let health = harness.health();
    assert!(health.process_alive, "a launch without --mode rpc exits");
    assert_eq!(health.delivery.peer.as_deref(), Some("omp rpc/1"));
    assert_eq!(health.sign_in_fault, None, "steering mode all was taken");
    assert_eq!(harness.session_id().as_deref(), Some("omp-session-1"));
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
    steer(&mut first, "m2");
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
