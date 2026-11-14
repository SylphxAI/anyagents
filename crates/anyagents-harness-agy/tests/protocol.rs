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
    assert_eq!(harness.health().delivery, anyagents_harness_agy::DELIVERY);
}
