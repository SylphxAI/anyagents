//! Scripted stand-in for the Muse `serve` child.

use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{Context, DeliverOutcome, DeliveryMode, Harness, OutboundMessage, SteerSupport};
use anyagents_harness_muse::MuseHarness;
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

fn start(dir: &TempDir, mode: &str, session: Option<&str>) -> MuseHarness {
    let log = dir.path().join("log");
    MuseHarness::start(
        env!("CARGO_BIN_EXE_fake_muse"),
        session,
        &[
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_STEER", "1"),
        ],
    )
    .expect("start")
}

#[test]
fn a_completed_turn_records_usage_and_dedupes() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish", None);
    assert_eq!(harness.session_id().as_deref(), Some("sess-from-vendor"));
    assert_eq!(harness.health().delivery_mode, DeliveryMode::Steer);
    assert_eq!(harness.health().steer_support, SteerSupport::Yes);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    // The vendor's own turn reports its tokens, and the harness offers the last
    // of them to the host.
    assert!(harness.usage_for("m1").is_some());
    let usage = harness.last_usage();
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.cache_read_tokens, 3);
    assert_eq!(harness.turns_started(), 1);
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
    assert_eq!(harness.turns_started(), 1);
}

#[test]
fn a_message_during_a_turn_steers_into_it_in_order() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_in_turn(Duration::from_secs(5)));
    // The vendor may answer the steer and close the turn before this call
    // returns, so either turn state is a correct acceptance here. What the test
    // is about is that m2 joined the open turn, not that it opened a new one.
    assert!(matches!(
        harness.deliver(&msg("m2")).unwrap(),
        DeliverOutcome::Accepted { .. }
    ));
    // The vendor's `turn/started` for the steer arrives on its own thread, so
    // the count is read once it has stopped moving rather than immediately.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut turns = harness.turns_started();
    while turns != 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
        turns = harness.turns_started();
    }
    assert_eq!(turns, 1, "a steer is not a new turn");
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
    // Both lines are in the one turn, which the vendor has not closed.
    assert!(harness.in_turn());
    // The steer joined the open turn as one `turn/start` with `ifBusy`, not a
    // second turn, and the host held the approval it was asked for.
    let log = std::fs::read_to_string(dir.path().join("log")).unwrap_or_default();
    let starts = log.matches("\"method\":\"turn/start\"").count();
    assert_eq!(starts, 2, "{log}");
    assert_eq!(log.matches("\"ifBusy\":\"steer\"").count(), 1, "{log}");
}

#[test]
fn three_lines_written_during_a_turn_are_all_steered_into_it() {
    // The vendor keeps the turn open; each line is a `turn/start` with
    // `ifBusy: "steer"`, so all three join the one running turn.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_in_turn(Duration::from_secs(5)));
    for id in ["m2", "m3"] {
        assert!(matches!(
            harness.deliver(&msg(id)).unwrap(),
            DeliverOutcome::Accepted { .. }
        ));
    }
    assert_eq!(harness.turns_started(), 1, "a steer is not a new turn");
    assert!(harness.in_turn(), "the turn is still running");
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string(), "m3".to_string()]
    );
    // A replay writes nothing.
    for id in ["m1", "m2", "m3"] {
        harness.deliver(&msg(id)).unwrap();
    }
    assert_eq!(harness.written_ids().len(), 3);
}

#[test]
fn a_rate_limited_start_is_a_wait_with_its_reset_time() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "quota", None);
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::Quota {
            reset_at_ms: 1_700_000_000_000
        }
    );
    assert!(harness.written_ids().is_empty());
    assert_eq!(harness.health().quota_reset_at_ms, Some(1_700_000_000_000));
    harness.clear_quota();
    assert_eq!(harness.health().quota_reset_at_ms, None);
}

#[test]
fn the_launch_is_muse_serve_and_the_first_line_is_the_id() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", Some("sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    let log = dir.path().join("log");
    let deadline = Instant::now() + Duration::from_secs(5);
    let started = Instant::now();
    let mut text = String::new();
    while Instant::now() < started + Duration::from_secs(5) {
        text = std::fs::read_to_string(&log).unwrap_or_default();
        if text.contains("anyagents-message:m1") {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(Instant::now() < deadline, "{text}");
    assert!(text.contains("argv:serve"), "{text}");
    assert!(text.contains("anyagents-message:m1\\nhello"), "{text}");
}
