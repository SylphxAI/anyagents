//! The Spiron adapter against the real `spiron` binary over
//! `spiron.headless/v2` (`docs/adr/0011-spiron-headless-v2.md`, D9): launch a
//! process that listens on its socket, deliver, steer mid-turn, let the
//! server go (detach), attach again from the launch's seq, and check that the
//! replay rebuilt the adapter (the open turn, every proof, nothing written
//! twice) before the turn ends with its usage and cost; then stop.
//!
//! Set `SPIRON_BIN` to a Spiron build with v2 (Spiron's own CI builds one;
//! `cargo build -p spiron-cli --bin spiron` in SylphxAI/spiron). Without it
//! the test says so and passes, unless `ANYAGENTS_REQUIRE_SPIRON=1`.
//! Spiron's own conformance suite (`spiron-cli/tests/headless_v2_conformance.rs`)
//! drives the same binary frame by frame against the protocol's schema.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyagents_harness::{Context, DeliverOutcome, Harness, OutboundMessage, Proof, TurnState};
use anyagents_harness_spiron::{ListenError, Reattach, SpironHarness, SpironLaunch};

const WAIT: Duration = Duration::from_secs(60);

/// A model that runs `sleep` (the window to steer and to lose the server
/// in), then answers, reporting usage and a cost of 78 micro-USD.
const SLEEP_THEN_ANSWER: &str = r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_sleep","function":{"name":"bash","arguments":"{\"command\":\"sleep 6\"}"}}]}}]}
data: [DONE]
---
data: {"choices":[{"delta":{"content":"done"}}]}
data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":2,"cost":0.000078}}
data: [DONE]
"#;

fn spiron_bin() -> Option<PathBuf> {
    match std::env::var_os("SPIRON_BIN") {
        Some(bin) => Some(PathBuf::from(bin)),
        None => {
            assert!(
                std::env::var("ANYAGENTS_REQUIRE_SPIRON").as_deref() != Ok("1"),
                "ANYAGENTS_REQUIRE_SPIRON=1 needs SPIRON_BIN"
            );
            eprintln!("SPIRON_BIN is not set: the real-Spiron v2 test is not run");
            None
        }
    }
}

fn launch(bin: &Path, root: &Path) -> SpironLaunch {
    let state = root.join("config");
    let work = root.join("work");
    std::fs::create_dir_all(&state).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    // As AnyAgents writes it: with its MCP mount file present, Spiron runs
    // tools without asking.
    let mcp = state.join("spiron-mcp.json");
    std::fs::write(&mcp, r#"{"mcpServers":{}}"#).unwrap();
    let fixture = root.join("fixture.sse");
    std::fs::write(&fixture, SLEEP_THEN_ANSWER).unwrap();
    SpironLaunch {
        program: bin.to_path_buf(),
        args: [
            "--headless",
            "--json",
            "--mcp-config",
            mcp.to_str().unwrap(),
            "--state-dir",
            state.to_str().unwrap(),
            "--workdir",
            work.to_str().unwrap(),
            "--agent-id",
            "agent-1",
            "--agent-name",
            "adapter-test",
        ]
        .map(str::to_string)
        .to_vec(),
        environment: vec![(
            "SPIRON_GATEWAY_FIXTURE".to_string(),
            fixture.display().to_string(),
        )],
        work_dir: work,
        session: None,
        state_dir: state,
    }
}

fn message(id: &str, text: &str) -> OutboundMessage {
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
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn pid_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
        && !std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .unwrap_or_default()
            .contains(") Z ")
}

#[test]
fn the_adapter_reattaches_after_its_server_goes_and_rebuilds_what_it_knew() {
    let Some(bin) = spiron_bin() else { return };
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("s.sock");
    let log = root.path().join("spiron.log");
    let mut first =
        SpironHarness::launch_listening(launch(&bin, root.path()), &socket, &log, WAIT, |_| Ok(()))
            .unwrap_or_else(|error| {
                panic!(
                    "launch: {error:?}; log: {}",
                    std::fs::read_to_string(&log).unwrap_or_default()
                )
            });
    assert!(first.listening());
    assert!(
        first
            .health()
            .delivery
            .peer
            .as_deref()
            .is_some_and(|peer| peer.starts_with("spiron.headless/v2"))
    );
    let after_seq = first.attach_after_seq().expect("a v2 launch has a seq");
    let session = first.session_id().expect("the session is named in ready");
    let pid = first.process_id().unwrap();

    // Deliver; the turn runs `sleep`. Steer into it.
    assert!(matches!(
        first.deliver(&message("m1", "start")).unwrap(),
        DeliverOutcome::InFlight
            | DeliverOutcome::Accepted {
                turn_finished: false
            }
    ));
    until("m1 accepted", || first.proven("m1").is_some());
    // The model's first step is immediate; the steer lands in the `sleep`.
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(
        first.deliver(&message("m2", "also")).unwrap(),
        DeliverOutcome::InFlight
    );
    assert_eq!(first.health().turn_state, TurnState::InTurn);
    assert_eq!(first.written_ids(), ["m1", "m2"]);

    // The server goes: the connection closes, the process and its turn go on.
    let record = root.path().join("record.json");
    std::fs::write(&record, "{}").unwrap();
    first.detach();
    std::thread::sleep(Duration::from_millis(200));
    assert!(pid_alive(pid), "spiron outlives its server");

    // A new server attaches from the launch's seq.
    let mut second = SpironHarness::reattach(
        Reattach {
            program: bin.clone(),
            socket: socket.clone(),
            pid,
            after_seq,
            session: Some(session.clone()),
            environment: Vec::new(),
            state_dir: root.path().join("config"),
            // The process ends on `stop` here; its group is not signalled.
            end: Box::new(|| {}),
            record: record.clone(),
        },
        WAIT,
    )
    .unwrap();
    assert_eq!(second.session_id().as_deref(), Some(session.as_str()));
    // The replay rebuilt the open turn and its proofs; nothing is written again.
    assert_eq!(
        second.proven("m1"),
        Some(Proof::Taken {
            turn_finished: false
        })
    );
    assert_eq!(
        second.deliver(&message("m1", "start")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(
        second.deliver(&message("m2", "also")).unwrap(),
        DeliverOutcome::InFlight
    );
    assert!(
        second.written_ids().is_empty(),
        "a replayed input is never written again"
    );
    assert_eq!(second.health().turn_state, TurnState::InTurn);

    // The turn ends: the steer was consumed at the next step, and the usage
    // and cost are the turn's.
    until("the turn's end", || {
        second.health().turn_state == TurnState::Idle
    });
    assert_eq!(
        second.proven("m1"),
        Some(Proof::Taken {
            turn_finished: true
        })
    );
    assert_eq!(
        second.proven("m2"),
        Some(Proof::Taken {
            turn_finished: true
        })
    );
    let usage = second.served_usage();
    assert_eq!((usage.input_tokens, usage.output_tokens), (11, 2));
    assert_eq!(usage.reported_cost_micros, Some(78));
    assert!(second.health().sign_in_fault.is_none());

    // A redelivery after the turn is answered from what the replay proved.
    assert_eq!(
        second.deliver(&message("m2", "also")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    assert!(second.written_ids().is_empty());

    // stop ends the process and its record.
    second.stop();
    until("spiron's exit", || !pid_alive(pid));
    assert!(!record.exists());
    assert!(!socket.exists());
}

#[test]
fn a_binary_without_listen_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    // A v1-only Spiron rejects `--listen` as clap does: status 2, naming it.
    let fake = root.path().join("spiron");
    std::fs::write(
        &fake,
        "#!/bin/sh\necho \"error: unexpected argument '--listen' found\" >&2\nexit 2\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let launch = SpironLaunch {
        program: fake,
        args: vec!["--headless".to_string(), "--json".to_string()],
        environment: Vec::new(),
        work_dir: root.path().to_path_buf(),
        session: None,
        state_dir: root.path().to_path_buf(),
    };
    let result = SpironHarness::launch_listening(
        launch,
        &root.path().join("s.sock"),
        &root.path().join("spiron.log"),
        WAIT,
        |_| Ok(()),
    );
    let Some(ListenError::Failed(error)) = result.err() else {
        panic!("launch must fail")
    };
    assert!(error.contains("--listen"), "{error}");
}
