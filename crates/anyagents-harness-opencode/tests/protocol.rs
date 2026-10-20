//! Scripted stand-in for the OpenCode child.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{DeliverOutcome, DeliveryMode, Harness, OutboundMessage, SteerSupport};
use anyagents_harness_opencode::OpenCodeHarness;
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

fn start(dir: &TempDir, mode: &str, session: Option<&str>) -> OpenCodeHarness {
    let log = dir.path().join("log");
    OpenCodeHarness::start(
        env!("CARGO_BIN_EXE_fake_opencode"),
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
    assert_eq!(usage.reported_cost_micros, Some(250_000));
    assert_eq!(harness.session_id().as_deref(), Some("sess-from-vendor"));
    let health = harness.health();
    assert_eq!(health.delivery_mode, DeliveryMode::Followup);
    assert_eq!(health.steer_support, SteerSupport::Unproven);
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn a_second_message_during_a_turn_waits_for_the_result() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    let second = harness.deliver(&msg("m2")).unwrap();
    assert!(matches!(
        second,
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    ));
    assert_eq!(harness.turns_started(), 1);
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
}

#[test]
fn text_without_a_result_line_is_the_answer_once_it_stands_still() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "stream", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(!harness.wait_finished("m1", Duration::from_millis(100)));
    assert!(harness.wait_quiet(Duration::from_millis(500), Duration::from_secs(5)));
    assert!(harness.closed_on_quiet("m1"));
    assert_eq!(harness.answer("m1").as_deref(), Some("pong"));
    assert!(harness.usage_for("m1").is_none());
}

#[test]
fn a_rate_limit_is_a_wait_and_not_a_write() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "quota", None);
    let reset = harness.wait_quota(Duration::from_secs(5)).expect("quota");
    assert_eq!(reset, 1_700_000_000_000);
    let outcome = harness.deliver(&msg("m1")).unwrap();
    assert_eq!(
        outcome,
        DeliverOutcome::Quota {
            reset_at_ms: 1_700_000_000_000
        }
    );
    assert!(harness.written_ids().is_empty());
}

#[test]
fn an_id_the_vendor_echoes_is_not_written_again() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "echo", None);
    assert!(harness.wait_seen("echoed", Duration::from_secs(5)));
    harness.deliver(&msg("echoed")).unwrap();
    assert!(harness.written_ids().is_empty());
}

#[test]
fn the_message_id_is_the_first_line_of_the_prompt() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", Some("sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    let log = dir.path().join("log");
    assert!(wait_log(&log, "anyagents-message:m1"));
    let text = read_log(&log);
    assert!(text.contains("argv:serve --session sess-1"));
}

#[test]
fn a_pushed_line_is_delivered_as_a_steered_prompt_once() {
    use anyagents_harness::SteerLine;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("steer.jsonl");
    std::fs::write(
        &path,
        serde_json::to_string(&SteerLine {
            id: "m7".to_string(),
            seq: 7,
            message: "later".to_string(),
        })
        .unwrap()
            + "\n",
    )
    .unwrap();
    let mut harness = start(&dir, "hold", None)
        .with_steer(anyagents_harness::SteerQueue::new(&path, 4));
    harness.deliver(&msg("m1")).unwrap();
    let item = harness.poll_steer().unwrap().expect("taken");
    assert_eq!(item.message.id, "m7");
    assert!(harness.inject(&item.message).unwrap());
    assert!(harness.wait_seen("m7", Duration::from_secs(5)));
    let log = dir.path().join("log");
    let text = read_log(&log);
    assert!(text.contains("\"delivery\":\"steer\""), "{text}");
    assert_eq!(text.matches("anyagents-message:m7").count(), 1, "{text}");
    assert_eq!(harness.turns_started(), 1);
}
