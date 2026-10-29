//! Scripted JSON-RPC stand-in for the Codex app-server child.

use std::path::Path;
use std::time::Duration;

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryMode, Harness, HarnessError, OutboundMessage, SteerSupport,
    TurnState,
};
use anyagents_harness_codex::CodexHarness;
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

fn log_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("log")
}

fn start(dir: &TempDir, mode: &str, session: Option<&str>) -> CodexHarness {
    let log = log_path(dir);
    let log = log.to_str().expect("log path");
    CodexHarness::start(
        env!("CARGO_BIN_EXE_fake_codex"),
        session,
        &[("ANYAGENTS_FAKE_MODE", mode), ("ANYAGENTS_FAKE_LOG", log)],
    )
    .expect("start")
}

fn read_log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

#[test]
fn finish_records_usage_and_dedupes() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    let usage = harness.usage_for("m1").expect("usage");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.cache_read_tokens, 3);
    assert_eq!(usage.cache_write_tokens, 2);
    assert_eq!(usage.reported_cost_micros, None);
    assert_eq!(harness.session_id().as_deref(), Some("thread-1"));
    let health = harness.health();
    assert_eq!(health.delivery_mode, DeliveryMode::Steer);
    assert_eq!(health.steer_support, SteerSupport::Yes);
    let log = read_log(&log_path(&dir));
    assert_eq!(log.matches("\"method\":\"turn/start\"").count(), 1);
    assert!(log.contains("\"clientUserMessageId\":\"m1\""));
    assert!(log.contains("anyagents-message:m1"));
    assert!(log.contains("\"method\":\"initialize\",\"params\""));
    assert!(log.contains("\"method\":\"initialized\""));
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(
        read_log(&log_path(&dir))
            .matches("\"method\":\"turn/start\"")
            .count(),
        1
    );
}

#[test]
fn steer_stays_on_one_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer", None);
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    assert_eq!(harness.turns_started(), 1);
    harness.deliver(&msg("m2")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    assert!(harness.wait_finished("m2", Duration::from_secs(5)));
    assert_eq!(harness.turns_started(), 1);
    assert!(read_log(&log_path(&dir)).contains("\"method\":\"turn/steer\""));
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
}

#[test]
fn interrupt_does_not_finish_the_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    harness.cancel().unwrap();
    assert!(read_log(&log_path(&dir)).contains("\"method\":\"turn/interrupt\""));
    assert!(!harness.is_finished("m1"));
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
fn rpc_quota_does_not_mark_the_id_written() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "rpc-quota", None);
    let blocked = harness.deliver(&msg("m1")).unwrap();
    assert!(matches!(blocked, DeliverOutcome::Quota { .. }));
    assert!(harness.written_ids().is_empty());
    assert!(!harness.is_finished("m1"));
}

#[test]
fn dead_child_rejects_every_call() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "die", None);
    assert!(harness.wait_dead(Duration::from_secs(5)));
    assert!(matches!(
        harness.deliver(&msg("m1")),
        Err(HarnessError::Dead)
    ));
    assert!(matches!(harness.background(), Err(HarnessError::Dead)));
    assert!(matches!(harness.cancel(), Err(HarnessError::Dead)));
    assert!(matches!(harness.resume("sess-1"), Err(HarnessError::Dead)));
}

#[test]
fn resume_sends_thread_resume() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.resume("sess-9").unwrap();
    let log = read_log(&log_path(&dir));
    assert!(log.contains("\"method\":\"thread/resume\""));
    assert!(log.contains("sess-9"));
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
fn harness_returning_401_surfaces_errored_within_three_turns() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "401", None);
    let _ = harness.deliver(&msg("m1"));
    let err = harness
        .wait_error(Duration::from_secs(5))
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
    assert!(
        harness.turns_started() <= 3,
        "expected errored within 3 turns, started {}",
        harness.turns_started()
    );
}

#[test]
fn consecutive_turn_failures_surface_errored_within_three_turns() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "turn-error", None);
    let _ = harness.deliver(&msg("m1"));
    let err = harness
        .wait_error(Duration::from_secs(5))
        .expect("repeated turn error surfaced within 3 turns");
    assert!(
        err.contains("upstream model stream closed unexpectedly"),
        "unexpected error: {err}"
    );
    assert_eq!(harness.turns_started(), 3);
    assert_eq!(harness.health().turn_state, TurnState::Idle);
}
