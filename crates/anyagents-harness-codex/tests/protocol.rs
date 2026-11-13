//! The Codex adapter against a scripted `codex app-server` (`fake_codex`).

use std::path::Path;
use std::time::{Duration, Instant};

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryLevel, Harness, HarnessError, OutboundMessage, TurnState,
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
    .expect("start")
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
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::InFlight,
        "the answer to turn/start is not a proof"
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
