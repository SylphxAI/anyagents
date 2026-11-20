//! The Codex adapter against a scripted `codex app-server` (`fake_codex`).

use std::path::Path;
use std::time::{Duration, Instant};

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryLevel, Harness, HarnessError, OutboundMessage, Proof,
    TurnState,
};
use anyagents_harness_codex::CodexHarness;
use tempfile::TempDir;

const PATIENCE: Duration = Duration::from_secs(10);

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

fn log_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("log")
}

fn start(dir: &TempDir, mode: &str, session: Option<&str>) -> CodexHarness {
    try_start(dir, mode, session).expect("start")
}

fn try_start(
    dir: &TempDir,
    mode: &str,
    session: Option<&str>,
) -> Result<CodexHarness, HarnessError> {
    let log = log_path(dir);
    let log = log.to_str().expect("log path");
    let history = dir.path().join("history");
    let history = history.to_str().expect("history path");
    CodexHarness::start(
        env!("CARGO_BIN_EXE_fake_codex"),
        session,
        &[
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log),
            ("ANYAGENTS_FAKE_HISTORY", history),
        ],
    )
}

fn read_log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// Requests in the log that carry `id` as their input.
fn writes_of(dir: &TempDir, method: &str, id: &str) -> usize {
    read_log(&log_path(dir))
        .lines()
        .filter(|line| {
            line.contains(&format!("\"method\":\"{method}\""))
                && line.contains(&format!("\"clientUserMessageId\":\"{id}\""))
        })
        .count()
}

/// Offer `message` the way the delivery loop does until it has an answer
/// other than "not yet".
fn settle(harness: &mut CodexHarness, message: &OutboundMessage) -> DeliverOutcome {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let outcome = harness.deliver(message).expect("deliver");
        if !matches!(outcome, DeliverOutcome::InFlight | DeliverOutcome::Deferred) {
            return outcome;
        }
        assert!(
            Instant::now() < deadline,
            "{} never settled: {outcome:?}",
            message.id
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Offer `message` until the answer is `wanted`.
fn offer_until(harness: &mut CodexHarness, message: &OutboundMessage, wanted: &DeliverOutcome) {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let outcome = harness.deliver(message).expect("deliver");
        if outcome == *wanted {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} answered {outcome:?}, not {wanted:?}",
            message.id
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn wait_idle(harness: &CodexHarness) {
    let deadline = Instant::now() + PATIENCE;
    while harness.health().turn_state != TurnState::Idle {
        assert!(Instant::now() < deadline, "the turn never ended");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn a_prompt_is_accepted_only_once_its_user_message_item_proves_it() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    let health = harness.health();
    assert_eq!(health.delivery.level, DeliveryLevel::NextStep);
    assert!(health.delivery.idempotent);
    assert_eq!(health.delivery.peer.as_deref(), Some("codex 0.169.0"));
    // The vendor proves the prompt right behind its answer to `turn/start`,
    // so the first offer can already find the proof in. That the answer
    // alone proves nothing is the unit test
    // `the_answer_to_turn_start_is_not_a_proof`.
    let first = harness.deliver(&msg("m1")).unwrap();
    assert!(
        matches!(
            first,
            DeliverOutcome::InFlight
                | DeliverOutcome::Accepted {
                    turn_finished: false
                }
        ),
        "{first:?}"
    );
    assert_eq!(
        settle(&mut harness, &msg("m1")),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    let log = read_log(&log_path(&dir));
    assert!(log.contains("\"method\":\"initialize\",\"params\""));
    assert!(log.contains("\"method\":\"initialized\""));
    assert!(log.contains("anyagents-message:m1"));
    assert!(
        !log.contains("thread/items/list"),
        "a new thread has no history"
    );
    for _ in 0..3 {
        harness.deliver(&msg("m1")).unwrap();
    }
    assert_eq!(writes_of(&dir, "turn/start", "m1"), 1);
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn a_completed_turn_finishes_its_messages_and_counts_one_usage() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish", None);
    settle(&mut harness, &msg("m1"));
    assert!(harness.wait_finished("m1", PATIENCE));
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    let usage = harness.usage_for("m1").expect("usage");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.cache_read_tokens, 3);
    assert_eq!(usage.cache_write_tokens, 2);
    assert_eq!(usage.reported_cost_micros, None);
    assert_eq!(harness.turns_served(), 1);
    assert_eq!(harness.session_id().as_deref(), Some("thread-1"));
    assert_eq!(harness.health().turn_state, TurnState::Idle);
}

#[test]
fn steers_join_the_running_turn_and_finish_with_it() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer", None);
    settle(&mut harness, &msg("m1"));
    assert_eq!(harness.turns_started(), 1);
    settle(&mut harness, &msg("m2"));
    assert!(harness.wait_finished("m1", PATIENCE));
    assert!(harness.wait_finished("m2", PATIENCE));
    assert_eq!(harness.turns_started(), 1);
    assert_eq!(harness.turns_served(), 1, "one turn, one usage");
    let log = read_log(&log_path(&dir));
    assert!(log.contains("\"expectedTurnId\":\"turn-1\""));
    assert_eq!(writes_of(&dir, "turn/steer", "m2"), 1);
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
}

#[test]
fn a_refused_steer_waits_for_its_turn_to_end_and_never_restarts() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "refuse-steer", None);
    settle(&mut harness, &msg("m1"));
    // A refusal hands the message back.
    offer_until(&mut harness, &msg("m2"), &DeliverOutcome::Deferred);
    for _ in 0..20 {
        assert_eq!(
            harness.deliver(&msg("m2")).unwrap(),
            DeliverOutcome::Deferred
        );
    }
    assert_eq!(
        writes_of(&dir, "turn/steer", "m2"),
        1,
        "not steered again into a turn that refuses steers"
    );
    assert_eq!(writes_of(&dir, "turn/start", "m2"), 0, "no fallback");
    assert!(harness.health().process_alive);
    assert!(harness.written_ids().iter().all(|id| id != "m2"));
    harness.cancel().unwrap();
    assert_eq!(
        settle(&mut harness, &msg("m2")),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(writes_of(&dir, "turn/start", "m2"), 1);
}

#[test]
fn an_unknown_steer_outcome_found_in_history_is_not_sent_again() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer-unknown-applied", None);
    settle(&mut harness, &msg("m1"));
    assert_eq!(
        settle(&mut harness, &msg("m2")),
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    assert!(read_log(&log_path(&dir)).contains("\"method\":\"thread/items/list\""));
    assert_eq!(writes_of(&dir, "turn/steer", "m2"), 1);
    assert_eq!(writes_of(&dir, "turn/start", "m2"), 0);
}

#[test]
fn an_unknown_steer_outcome_missing_from_history_is_written_once_more() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer-unknown-lost", None);
    settle(&mut harness, &msg("m1"));
    assert_eq!(
        settle(&mut harness, &msg("m2")),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert!(read_log(&log_path(&dir)).contains("\"method\":\"thread/items/list\""));
    assert_eq!(writes_of(&dir, "turn/steer", "m2"), 1);
    assert_eq!(writes_of(&dir, "turn/start", "m2"), 1);
}

#[test]
fn a_steer_that_raced_the_end_of_its_turn_is_settled_from_history() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer-silent", None);
    settle(&mut harness, &msg("m1"));
    assert_eq!(
        settle(&mut harness, &msg("m2")),
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    assert_eq!(writes_of(&dir, "turn/steer", "m2"), 1);
    assert_eq!(writes_of(&dir, "turn/start", "m2"), 0);
}

#[test]
fn a_resumed_thread_recognises_what_an_earlier_process_applied() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("history"),
        concat!(
            r#"{"turnId":"turn-old","clientId":"m1","text":"anyagents-message:m1\nhello"}"#,
            "\n",
            r#"{"turnId":"turn-old","clientId":null,"text":"anyagents-message:m0\nolder history"}"#,
            "\n",
        ),
    )
    .unwrap();
    let mut harness = start(&dir, "hold", Some("thread-9"));
    offer_until(&mut harness, &msg("m0"), &DeliverOutcome::Duplicate);
    offer_until(&mut harness, &msg("m1"), &DeliverOutcome::Duplicate);
    assert_eq!(writes_of(&dir, "turn/start", "m1"), 0);
    assert_eq!(
        settle(&mut harness, &msg("m2")),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    let log = read_log(&log_path(&dir));
    assert!(log.contains("\"method\":\"thread/resume\""));
    assert!(log.contains("\"sortDirection\":\"desc\""));
    assert!(log.contains("thread-9"));
    assert!(!log.contains("\"method\":\"thread/start\""));
}

#[test]
fn a_failed_turn_surfaces_its_error_and_is_never_sent_again() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "turn-error", None);
    assert_eq!(
        settle(&mut harness, &msg("m1")),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    let err = harness.wait_error(PATIENCE).expect("turn error surfaced");
    assert!(
        err.contains("upstream model stream closed unexpectedly"),
        "unexpected error: {err}"
    );
    assert_eq!(harness.health().turn_state, TurnState::Idle);
    assert_eq!(harness.turns_served(), 0, "a failed turn completes nothing");
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(harness.turns_started(), 1);
    assert_eq!(writes_of(&dir, "turn/start", "m1"), 1);
}

#[test]
fn harness_returning_401_surfaces_a_sign_in_fault() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "401", None);
    settle(&mut harness, &msg("m1"));
    let err = harness
        .wait_error(PATIENCE)
        .expect("401 turn error surfaced");
    assert!(err.contains("401"), "unexpected error: {err}");
    let health = harness.health();
    assert_eq!(health.turn_state, TurnState::Idle);
    assert!(
        health
            .sign_in_fault
            .as_deref()
            .is_some_and(|s| s.contains("401")),
        "expected sign_in_fault with 401, got {:?}",
        health.sign_in_fault
    );
    assert_eq!(harness.turns_started(), 1);
}

#[test]
fn interrupt_does_not_finish_the_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    settle(&mut harness, &msg("m1"));
    harness.cancel().unwrap();
    assert!(read_log(&log_path(&dir)).contains("\"method\":\"turn/interrupt\""));
    wait_idle(&harness);
    assert!(!harness.is_finished("m1"));
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
}

#[test]
fn quota_blocks_before_write() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "quota", None);
    assert_eq!(harness.health().quota_reset_at_ms, Some(1_700_000_000_000));
    let blocked = harness.deliver(&msg("m1")).unwrap();
    assert!(matches!(
        blocked,
        DeliverOutcome::Quota {
            reset_at_ms: 1_700_000_000_000
        }
    ));
    assert!(!read_log(&log_path(&dir)).contains("anyagents-message:m1"));
}

#[test]
fn rpc_quota_hands_the_message_back() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "rpc-quota", None);
    assert!(matches!(
        settle(&mut harness, &msg("m1")),
        DeliverOutcome::Quota {
            reset_at_ms: 1_700_000_000_000
        }
    ));
    assert!(harness.written_ids().is_empty());
    assert!(!harness.is_finished("m1"));
    assert_eq!(writes_of(&dir, "turn/start", "m1"), 1);
}

#[test]
fn dead_child_rejects_every_call() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "die", None);
    assert!(harness.wait_dead(PATIENCE));
    assert!(matches!(
        harness.deliver(&msg("m1")),
        Err(HarnessError::Dead)
    ));
    assert!(matches!(harness.background(), Err(HarnessError::Dead)));
    assert!(matches!(harness.cancel(), Err(HarnessError::Dead)));
    assert!(matches!(harness.resume("sess-1"), Err(HarnessError::Dead)));
}

#[test]
fn resume_sends_thread_resume_and_reads_its_history() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.resume("sess-9").unwrap();
    assert_eq!(harness.session_id().as_deref(), Some("sess-9"));
    settle(&mut harness, &msg("m1"));
    let log = read_log(&log_path(&dir));
    assert!(log.contains("\"method\":\"thread/resume\""));
    assert!(log.contains("sess-9"));
    assert!(log.contains("\"method\":\"thread/items/list\""));
}

#[test]
fn launch_resumes_when_a_session_is_known() {
    let dir = TempDir::new().unwrap();
    let harness = start(&dir, "hold", Some("sess-1"));
    let log = read_log(&log_path(&dir));
    assert!(log.contains("\"method\":\"thread/resume\""));
    assert!(!log.contains("\"method\":\"thread/start\""));
    drop(harness);
}

#[test]
fn an_input_codex_refuses_for_good_is_parked_and_never_written_again() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    let mut poison = msg("m1");
    poison.text = "x".repeat((1 << 20) + 1);
    let rejected = settle(&mut harness, &poison);
    let DeliverOutcome::Rejected { reason, .. } = &rejected else {
        panic!("not parked: {rejected:?}");
    };
    assert!(reason.contains("-32602"), "{reason}");
    // Every later offer gets the same answer, with nothing written and no
    // restart.
    for _ in 0..20 {
        assert_eq!(harness.deliver(&poison).unwrap(), rejected);
    }
    assert_eq!(writes_of(&dir, "turn/start", "m1"), 1);
    assert_eq!(writes_of(&dir, "turn/steer", "m1"), 0);
    assert!(harness.health().process_alive);
    assert_eq!(harness.proven("m1"), None, "a rejection is not a proof");
    // The mail behind it goes on.
    assert_eq!(
        settle(&mut harness, &msg("m2")),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    // The refusal outlives the process.
    harness.stop();
    assert!(!harness.health().process_alive);
    assert_eq!(harness.deliver(&poison).unwrap(), rejected);
}

#[test]
fn a_resume_of_a_thread_codex_does_not_have_starts_a_new_one() {
    let dir = TempDir::new().unwrap();
    let harness = start(&dir, "resume-missing", Some("thread-gone"));
    let log = read_log(&log_path(&dir));
    assert!(log.contains("\"method\":\"thread/resume\""), "{log}");
    assert!(log.contains("\"method\":\"thread/start\""), "{log}");
    assert_eq!(harness.session_id().as_deref(), Some("thread-1"));
}

#[test]
fn a_resume_that_fails_for_another_reason_fails_the_start() {
    let dir = TempDir::new().unwrap();
    let err = try_start(&dir, "resume-unreadable", Some("thread-9"))
        .err()
        .expect("the start fails");
    assert!(
        matches!(&err, HarnessError::Failed(reason)
            if reason.contains("thread/resume") && reason.contains("failed to locate")),
        "{err:?}"
    );
    let log = read_log(&log_path(&dir));
    assert!(
        !log.contains("\"method\":\"thread/start\""),
        "the conversation is never replaced in silence: {log}"
    );
}

#[test]
fn a_proof_given_just_before_stop_is_proven_after_it() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    // The vendor proves m1 right behind its answer: the first offer can
    // already find the proof in.
    let first = harness.deliver(&msg("m1")).unwrap();
    assert!(
        matches!(
            first,
            DeliverOutcome::InFlight
                | DeliverOutcome::Accepted {
                    turn_finished: false
                }
        ),
        "{first:?}"
    );
    // The vendor wrote its proof; the adapter may not have read it yet.
    let deadline = Instant::now() + PATIENCE;
    while !read_log(&log_path(&dir)).contains("proved:m1") {
        assert!(Instant::now() < deadline, "the vendor never proved m1");
        std::thread::sleep(Duration::from_millis(2));
    }
    harness.stop();
    assert!(!harness.health().process_alive);
    assert_eq!(
        harness.proven("m1"),
        Some(Proof::Taken {
            turn_finished: false
        })
    );
    // `deliver` answers the proof before it looks at liveness.
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(harness.deliver(&msg("m2")), Err(HarnessError::Dead));
}

/// Config effort is inherited by Codex itself; thread/start and thread/resume
/// must preserve the writer's native model/provider, not a harness selector.
#[test]
fn model_selector_matrix_reaches_app_server_start_and_resume() {
    assert!(
        std::env::var_os("ANYAGENTS_FAKE_MODE").is_none(),
        "parent fake mode would skip production configuration"
    );
    for model in [
        "gpt-6.2-sol",
        "gpt-6.2-astra",
        "gpt-6.1-sol",
        "unknown-model",
    ] {
        for provider in ["openai", "sylphx"] {
            for effort in ["low", "medium", "high", "xhigh", "max"] {
                for session in [None, Some("thread-existing")] {
                    let dir = TempDir::new().unwrap();
                    let config = format!(
                        "model = \"{model}\"\nmodel_provider = \"{provider}\"\nmodel_reasoning_effort = \"{effort}\"\n[caller]\nmodel = \"not-the-top-level-model\"\n"
                    );
                    std::fs::write(dir.path().join("config.toml"), &config).unwrap();
                    let log = log_path(&dir);
                    let harness = CodexHarness::start(
                        env!("CARGO_BIN_EXE_fake_codex"),
                        session,
                        &[
                            ("CODEX_HOME", dir.path().to_str().unwrap()),
                            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
                            ("ANYAGENTS_FAKE_MODE", "hold"),
                        ],
                    )
                    .unwrap();
                    let method = if session.is_some() {
                        "thread/resume"
                    } else {
                        "thread/start"
                    };
                    let requests = read_log(&log);
                    let request = requests
                        .lines()
                        .filter_map(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                        .find(|v| v["method"] == method)
                        .expect("thread request recorded by child");
                    assert_eq!(request["params"]["model"], model);
                    assert_eq!(request["params"]["modelProvider"], provider);
                    assert_eq!(request["params"]["approvalPolicy"], "never");
                    if let Some(session) = session {
                        assert_eq!(request["params"]["threadId"], session);
                    }
                    assert_eq!(
                        std::fs::read_to_string(dir.path().join("config.toml")).unwrap(),
                        config
                    );
                    drop(harness);
                }
            }
        }
    }
}

#[cfg(unix)]
#[test]
fn codex_multi_step_next_step_steer_incorporates_into_step2_and_fails_closed() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let py = root.join("mock_codex.py");
    let sh = root.join("mock_codex");
    std::fs::write(
        &py,
        r#"
import json, os, pathlib, sys, time

state_dir = pathlib.Path(os.environ["MOCK_STATE_DIR"])
trace_path = state_dir / "codex_trace.jsonl"

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

active_turn = None
turn_seq = 0
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    req = json.loads(raw)
    method = req.get("method")
    rpc_id = req.get("id")
    params = req.get("params") or {}
    trace("rx", method=method, id=rpc_id, params=params)

    if method == "initialize":
        emit({"id": rpc_id, "result": {"userAgent": "codex_cli_rs/0.171.0 (Linux; x86_64)"}})
    elif method == "initialized":
        continue
    elif method == "thread/start":
        emit({"id": rpc_id, "result": {"thread": {"id": "thread-multi"}}})
    elif method == "turn/start":
        cid = params.get("clientUserMessageId", "")
        text = params["input"][0]["text"]
        if cid == "m_bad":
            emit({"id": rpc_id, "error": {"code": -32602, "message": "input exceeds length limit"}})
            continue
        if cid == "m_die":
            sys.exit(0)
        turn_seq += 1
        active_turn = f"turn-{turn_seq}"
        emit({"id": rpc_id, "result": {"turn": {"id": active_turn}}})
        emit({"method": "turn/started", "params": {"turn": {"id": active_turn}}})
        emit({
            "method": "item/started",
            "params": {
                "turnId": active_turn,
                "item": {
                    "type": "userMessage",
                    "id": f"item-{cid}",
                    "clientId": cid,
                    "content": [{"type": "text", "text": text}],
                },
            },
        })
        emit({"method": "item/agentMessage/delta", "params": {"turnId": active_turn, "delta": "step1:working;"}})
    elif method == "turn/steer":
        cid = params.get("clientUserMessageId", "")
        expected = params.get("expectedTurnId")
        text = params["input"][0]["text"]
        payload = "codex-steer-payload" if "codex-steer-payload" in text else text.splitlines()[-1]
        if cid == "m4":
            emit({"id": rpc_id, "error": {"code": -32600, "message": "cannot steer during review"}})
            continue
        assert expected == active_turn, f"expected {active_turn}, got {expected}"
        # Reply to RPC first while holding `userMessage` item behind gate to prove
        # RPC reply alone keeps m2 InFlight.
        emit({"id": rpc_id, "result": {"turnId": active_turn}})
        wait_gate("allow_step2")
        emit({
            "method": "item/started",
            "params": {
                "turnId": active_turn,
                "item": {
                    "type": "userMessage",
                    "id": f"item-{cid}",
                    "clientId": cid,
                    "content": [{"type": "text", "text": text}],
                },
            },
        })
        out = f"step1:working;step2:incorporated:{payload}"
        trace("consumed_before_complete", client_id=cid, turn=active_turn, out=out)
        emit({
            "method": "item/completed",
            "params": {
                "turnId": active_turn,
                "item": {"type": "agentMessage", "id": "am-step2", "text": out},
            },
        })
        emit({
            "method": "thread/tokenUsage/updated",
            "params": {"tokenUsage": {"last": {"inputTokens": 12, "outputTokens": 7, "cachedInputTokens": 0}}},
        })
        emit({"method": "turn/completed", "params": {"turn": {"id": active_turn, "status": "completed"}}})
        active_turn = None
    elif method == "turn/interrupt":
        emit({"id": rpc_id, "result": {}})
        emit({"method": "turn/completed", "params": {"turn": {"id": active_turn, "status": "interrupted"}}})
        active_turn = None
"#,
    )
    .unwrap();
    std::fs::write(
        &sh,
        format!("#!/bin/sh\nexec python3 -u '{}' \"$@\"\n", py.display()),
    )
    .unwrap();
    std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut harness = CodexHarness::start(&sh, None, &[("MOCK_STATE_DIR", root.to_str().unwrap())])
        .expect("codex mock starts");

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
        let deadline = Instant::now() + PATIENCE;
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    // Step 1: turn/start opens Turn 1 and pauses mid-turn.
    let m1 = mk("m1", "step-1-work");
    assert_eq!(
        settle(&mut harness, &m1),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(harness.health().turn_state, TurnState::InTurn);

    // Mid-turn steer m2: `turn/steer` RPC reply alone MUST stay InFlight until `userMessage` item.
    let m2 = mk("m2", "codex-steer-payload");
    let first_m2 = harness.deliver(&m2).unwrap();
    assert_eq!(first_m2, DeliverOutcome::InFlight);
    wait_until("mock received turn/steer", || {
        std::fs::read_to_string(root.join("codex_trace.jsonl"))
            .unwrap_or_default()
            .contains("\"method\": \"turn/steer\"")
    });
    std::thread::sleep(Duration::from_millis(60));
    assert_eq!(
        harness.proven("m2"),
        None,
        "turn/steer RPC response alone must not prove m2"
    );
    assert_eq!(harness.deliver(&m2).unwrap(), DeliverOutcome::InFlight);

    // Step 2 consumes m2 BEFORE turn/completed and incorporates it into Step 2 output.
    std::fs::write(root.join("allow_step2"), "1").unwrap();
    assert!(harness.wait_finished("m1", PATIENCE));
    assert!(harness.wait_finished("m2", PATIENCE));
    assert_eq!(harness.turns_started(), 1);
    assert_eq!(harness.turns_served(), 1);
    let (thread, reply) = harness.task_turn_result("m2").expect("task turn result");
    assert_eq!(thread, "thread-multi");
    assert!(
        reply.contains("step2:incorporated:codex-steer-payload"),
        "{reply}"
    );

    // Turn 2: refused steer (-32600) -> Deferred, never proven.
    let m3 = mk("m3", "turn-2");
    assert_eq!(
        settle(&mut harness, &m3),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    let m4 = mk("m4", "refused-steer");
    offer_until(&mut harness, &m4, &DeliverOutcome::Deferred);
    assert_eq!(harness.proven("m4"), None);
    harness.cancel().unwrap();
    wait_idle(&harness);

    // Invalid params (-32602) -> Rejected, never proven.
    let m_bad = mk("m_bad", "too-big");
    assert!(matches!(
        settle(&mut harness, &m_bad),
        DeliverOutcome::Rejected { .. }
    ));
    assert_eq!(harness.proven("m_bad"), None);

    // Dead child fail-closed -> Err(HarnessError::Dead).
    let _ = harness.deliver(&mk("m_die", "exit"));
    assert!(harness.wait_dead(PATIENCE));
    assert_eq!(
        harness.deliver(&mk("m_after_death", "nope")),
        Err(HarnessError::Dead)
    );
    assert_eq!(harness.background(), Err(HarnessError::Dead));
    assert_eq!(harness.cancel(), Err(HarnessError::Dead));
}
