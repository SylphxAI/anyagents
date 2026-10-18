//! Scripted stand-in for the OMP child. The field names are unverified.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{DeliverOutcome, DeliveryMode, Harness, OutboundMessage, SteerSupport};
use anyagents_harness_omp::OmpHarness;
use tempfile::TempDir;

fn msg(id: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.to_string(),
        seq: 1,
        text: "hello".to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
    }
}

fn start(dir: &TempDir, mode: &str, session: Option<&str>) -> OmpHarness {
    let log = dir.path().join("log");
    OmpHarness::start(
        env!("CARGO_BIN_EXE_fake_omp"),
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
fn a_done_event_records_usage_and_a_second_deliver_is_not_written() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    let usage = harness.usage_for("m1").expect("usage");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(harness.health().delivery_mode, DeliveryMode::Followup);
    assert_eq!(harness.health().steer_support, SteerSupport::Unproven);
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn a_second_message_during_a_turn_waits() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(matches!(
        harness.deliver(&msg("m2")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    ));
    assert_eq!(harness.turns_started(), 1);
}

#[test]
fn a_rate_limit_is_a_wait_and_not_a_write() {
    let dir = TempDir::new().unwrap();
    let harness = start(&dir, "quota", None);
    assert_eq!(
        harness.wait_quota(Duration::from_secs(5)),
        Some(1_700_000_000_000)
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
fn the_message_id_is_the_first_line() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", Some("sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    let log = dir.path().join("log");
    assert!(wait_log(&log, "anyagents-message:m1"));
    assert!(read_log(&log).contains("argv:--session sess-1"));
}
