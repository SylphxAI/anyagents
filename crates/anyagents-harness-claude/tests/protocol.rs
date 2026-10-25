//! Scripted stream-json stand-in for the Claude Code child.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryMode, Harness, OutboundMessage, SteerSupport, TurnState,
};
use anyagents_harness_claude::ClaudeHarness;
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

fn start(dir: &TempDir, mode: &str, session: Option<&str>) -> ClaudeHarness {
    let log = log_path(dir);
    let log = log.to_str().expect("log path");
    ClaudeHarness::start(
        env!("CARGO_BIN_EXE_fake_claude"),
        session,
        &[("ANYAGENTS_FAKE_MODE", mode), ("ANYAGENTS_FAKE_LOG", log)],
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
fn finish_records_usage_and_dedupes() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish", None);
    let first = harness.deliver(&msg("m1")).unwrap();
    assert!(matches!(
        first,
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    ));
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    let usage = harness.usage_for("m1").expect("usage");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.cache_read_tokens, 3);
    assert_eq!(usage.cache_write_tokens, 2);
    assert_eq!(usage.reported_cost_micros, Some(500_000));
    assert_eq!(harness.session_id().as_deref(), Some("sess-from-vendor"));
    let health = harness.health();
    // Claude Code injects pending context through its tool hook mid-turn, so
    // the catalog steers rather than following up.
    assert_eq!(health.delivery_mode, DeliveryMode::Steer);
    assert_eq!(health.steer_support, SteerSupport::Yes);
    let again = harness.deliver(&msg("m1")).unwrap();
    assert!(matches!(
        again,
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    ));
    let log = log_path(&dir);
    assert_eq!(read_log(&log).matches("anyagents-message:m1").count(), 1);
    assert_eq!(
        harness.wait_quota(Duration::from_secs(5)),
        Some(1_700_000_000_000)
    );
    let blocked = harness.deliver(&msg("m2")).unwrap();
    assert!(matches!(
        blocked,
        DeliverOutcome::Quota {
            reset_at_ms: 1_700_000_000_000
        }
    ));
    assert!(!read_log(&log).contains("anyagents-message:m2"));
}

#[test]
fn hold_keeps_one_turn_open() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(
        harness.deliver(&msg("m2")).unwrap(),
        DeliverOutcome::Deferred
    );
    assert_eq!(harness.turns_started(), 1);
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    harness.background().unwrap();
    assert!(wait_log(&log_path(&dir), "--input-format stream-json"));
    let argv = read_log(&log_path(&dir));
    assert!(argv.contains("-p"));
    assert!(argv.contains("--output-format stream-json"));
    assert!(argv.contains("--verbose"));
}

#[test]
fn quota_blocks_before_write() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "quota", None);
    assert_eq!(
        harness.wait_quota(Duration::from_secs(5)),
        Some(1_700_000_000_000)
    );
    let blocked = harness.deliver(&msg("m1")).unwrap();
    assert!(matches!(blocked, DeliverOutcome::Quota { .. }));
    let log = read_log(&log_path(&dir));
    assert!(log.contains("argv:"));
    assert!(!log.contains("anyagents-message:m1"));
}

#[test]
fn dead_child_rejects_delivery() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "die", None);
    assert!(harness.wait_dead(Duration::from_secs(5)));
    assert!(!harness.health().process_alive);
    assert!(matches!(
        harness.deliver(&msg("m1")),
        Err(anyagents_harness::HarnessError::Dead)
    ));
    assert!(matches!(
        harness.background(),
        Err(anyagents_harness::HarnessError::Dead)
    ));
    assert!(matches!(
        harness.cancel(),
        Err(anyagents_harness::HarnessError::Dead)
    ));
}

#[test]
fn interrupt_clears_the_open_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    harness.cancel().unwrap();
    let log = log_path(&dir);
    // Initialization also logs a control_request; wait for the specific
    // asynchronous interrupt rather than mistaking initialization for it.
    assert!(wait_log(&log, "interrupt"));
    assert!(!harness.is_finished("m1"));
    assert_eq!(harness.health().turn_state, TurnState::Idle);
}

#[test]
fn resume_relaunches_with_the_session_and_keeps_seen() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(wait_log(&log_path(&dir), "anyagents-message:m1"));
    harness.resume("sess-1").unwrap();
    assert!(wait_log(&log_path(&dir), "--resume sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(
        read_log(&log_path(&dir))
            .matches("anyagents-message:m1")
            .count(),
        1
    );
}

#[test]
fn launch_passes_resume_when_a_session_is_known() {
    let dir = TempDir::new().unwrap();
    let harness = start(&dir, "hold", Some("sess-9"));
    assert!(wait_log(&log_path(&dir), "--resume sess-9"));
    drop(harness);
}

#[test]
fn echoed_id_is_not_written_again() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "echo", None);
    assert!(harness.wait_seen("echoed", Duration::from_secs(5)));
    harness.deliver(&msg("echoed")).unwrap();
    assert!(harness.written_ids().is_empty());
    assert!(!read_log(&log_path(&dir)).contains("echoed"));
}

#[test]
fn an_errored_result_is_a_failed_turn_with_no_usage() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "failed_turn", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(
        harness.wait_finished("m1", Duration::from_secs(5)),
        "the errored result is still the end of its turn"
    );
    assert!(harness.usage_for("m1").is_none());
    // The vendor's own reason is kept on the harness and never shown to the
    // switchboard: delivery does not depend on how the turn ended.
    let reason = harness.failure_for("m1").expect("the vendor's reason");
    assert!(reason.contains("error_during_execution"), "{reason}");
    assert!(reason.contains("Invalid API key"), "{reason}");
    assert_eq!(harness.health().turn_state, TurnState::Idle);
    // Handing the failed message back writes it once more, because the turn that
    // succeeds has to carry it; the replay starts a second turn.
    assert!(
        matches!(
            harness.deliver(&msg("m1")).unwrap(),
            DeliverOutcome::Accepted { .. }
        ),
        "the hand-over is accepted"
    );
    assert_eq!(harness.turns_started(), 2);
}

#[test]
fn the_child_is_asked_to_replay_the_lines_it_reads() {
    let dir = TempDir::new().unwrap();
    let harness = start(&dir, "hold", None);
    // The child logs its argv as its first act; wait for it rather than racing.
    assert!(wait_log(&log_path(&dir), "argv:"));
    assert!(
        read_log(&log_path(&dir)).contains("--replay-user-messages"),
        "{}",
        read_log(&log_path(&dir))
    );
    drop(harness);
}

fn triggered(dir: &TempDir) -> ClaudeHarness {
    ClaudeHarness::start(
        env!("CARGO_BIN_EXE_fake_claude"),
        None,
        &[
            ("ANYAGENTS_FAKE_MODE", "steer"),
            ("ANYAGENTS_FAKE_LOG", log_path(dir).to_str().unwrap()),
            (
                "ANYAGENTS_FAKE_TRIGGER",
                dir.path().join("trigger").to_str().unwrap(),
            ),
        ],
    )
    .unwrap()
}

#[test]
fn mid_turn_messages_enter_hook_context_before_tool_without_duplicates() {
    let dir = TempDir::new().unwrap();
    let mut harness = triggered(&dir);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_echoed("m1", Duration::from_secs(5)));
    for id in ["m2", "m3", "m2"] {
        let mut message = msg(id);
        message.context.sender = Some("agent:teammate".into());
        message.context.channel = Some("room".into());
        assert_eq!(harness.deliver(&message).unwrap(), DeliverOutcome::Deferred);
    }
    // A realistic stdin consumer would queue these until the turn ends. They
    // must not be sent as user events or acknowledged just for being queued.
    assert_eq!(harness.written_ids(), vec!["m1"]);
    std::fs::write(dir.path().join("trigger"), "hook-1").unwrap();
    assert!(wait_log(&log_path(&dir), "tool:proceed"));
    let text = read_log(&log_path(&dir));
    let response = text
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|v| v.pointer("/response/request_id").and_then(|v| v.as_str()) == Some("hook-1"))
        .unwrap();
    let context = response
        .pointer("/response/response/hookSpecificOutput/additionalContext")
        .unwrap()
        .as_str()
        .unwrap();
    assert!(
        context.contains("from=\"agent:teammate\"") || context.contains("from: agent:teammate")
    );
    assert!(context.contains("channel=\"room\"") || context.contains("channel: room"));
    assert!(
        context.find("anyagents-message:m2").unwrap()
            < context.find("anyagents-message:m3").unwrap()
    );
    assert!(text.find("additionalContext").unwrap() < text.find("tool:proceed").unwrap());
    for id in ["m2", "m3"] {
        assert!(matches!(
            harness.deliver(&msg(id)).unwrap(),
            DeliverOutcome::Accepted { .. }
        ));
        assert_eq!(harness.phase_of(id), Some("turn"));
    }
    std::fs::write(dir.path().join("trigger"), "hook-2").unwrap();
    assert!(wait_log(&log_path(&dir), "hook-2"));
    let text = read_log(&log_path(&dir));
    for id in ["m1", "m2", "m3"] {
        assert_eq!(
            text.matches(&format!("anyagents-message:{id}")).count(),
            1,
            "{text}"
        );
    }
    assert_eq!(harness.turns_started(), 1);
    std::fs::write(dir.path().join("trigger"), "result").unwrap();
    for id in ["m1", "m2", "m3"] {
        assert!(harness.wait_finished(id, Duration::from_secs(5)));
    }
    assert_eq!(harness.turns_served(), 1);
    assert_eq!(harness.served_usage().input_tokens, 11);
}

#[test]
fn no_tool_leaves_message_undelivered_until_next_idle_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = triggered(&dir);
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(
        harness.deliver(&msg("m2")).unwrap(),
        DeliverOutcome::Deferred
    );
    std::fs::write(dir.path().join("trigger"), "result").unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    assert!(!harness.seen("m2"));
    assert!(matches!(
        harness.deliver(&msg("m2")).unwrap(),
        DeliverOutcome::Accepted { .. }
    ));
    assert!(harness.wait_echoed("m2", Duration::from_secs(5)));
    assert_eq!(harness.phase_of("m2"), Some("idle"));
    assert_eq!(harness.turns_started(), 2);
}

#[test]
fn a_pushed_line_is_injected_once_and_settled_on_its_echo() {
    use anyagents_harness::SteerQueue;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("steer.jsonl");
    std::fs::write(&path, "{\"id\":\"m7\",\"seq\":7,\"message\":\"later\"}\n").unwrap();
    let queue = SteerQueue::new(&path, 4);
    let mut harness = start(&dir, "steer", None).with_steer(queue.clone());
    harness.deliver(&msg("m1")).unwrap();
    let item = harness.poll_steer().unwrap().expect("taken");
    // Still held while nothing is settled: it is not taken a second time into
    // the same turn.
    assert!(harness.poll_steer().unwrap().is_none());
    // Once the host releases it, the line goes back on the queue and is offered
    // again rather than being lost.
    harness.release_steer("settled-elsewhere");
    let lines = std::fs::read_to_string(&path).unwrap();
    assert!(lines.contains("\"m7\""), "{lines}");
    let again = harness.poll_steer().unwrap().expect("offered again");
    assert_eq!(again.message.id, "m7");
    let _ = item;
}

#[test]
fn rejected_initialization_does_not_advertise_working_delivery() {
    let result = ClaudeHarness::start(
        env!("CARGO_BIN_EXE_fake_claude"),
        None,
        &[("ANYAGENTS_FAKE_MODE", "reject_init")],
    );
    assert!(matches!(
        result,
        Err(anyagents_harness::HarnessError::Failed(_))
    ));
}
