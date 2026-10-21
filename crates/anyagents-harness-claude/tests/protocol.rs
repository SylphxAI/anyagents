//! Scripted stream-json stand-in for the Claude Code child.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, OutboundMessage, SteerSupport, TurnState,
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
    // Claude Code reads a stream-json `user` line mid-turn, so the catalog
    // steers rather than following up.
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
    harness.deliver(&msg("m2")).unwrap();
    assert_eq!(harness.turns_started(), 1);
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    harness.background().unwrap();
    assert!(wait_log(&log_path(&dir), "--input-format stream-json"));
    let argv = read_log(&log_path(&dir));
    assert!(argv.contains("-p"));
    assert!(argv.contains("--output-format stream-json"));
    assert!(argv.contains("--verbose"));
}

#[test]
fn batch_promotes_the_followup() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "batch", None);
    harness.deliver(&msg("m1")).unwrap();
    harness.deliver(&msg("m2")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    assert!(!harness.is_finished("m2"));
    assert_eq!(harness.turns_started(), 2);
    assert!(harness.usage_for("m1").is_some());
    assert!(harness.usage_for("m2").is_none());
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
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
    assert!(wait_log(&log, "control_request"));
    assert!(read_log(&log).contains("interrupt"));
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

#[test]
fn three_lines_written_while_a_turn_is_open_are_all_taken_by_the_running_stream() {
    // The child holds its turn open and echoes every line it reads. All three
    // mailbox lines must reach it mid-turn: the adapter writes each one the
    // moment it arrives, and the child's echo proves the running conversation
    // took it. None is written twice, and no line waits for the turn to end.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer", Some("sess-1"));
    assert_eq!(harness.health().delivery_mode, DeliveryMode::Steer);
    assert_eq!(harness.health().steer_support, SteerSupport::Yes);
    for id in ["m1", "m2", "m3"] {
        assert_eq!(
            harness.deliver(&msg(id)).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            },
            "{id} is taken without waiting for a turn to end"
        );
        assert!(
            harness.wait_echoed(id, Duration::from_secs(5)),
            "{id} was not read by the running stream"
        );
    }
    assert_eq!(
        harness.turns_started(),
        1,
        "one running turn took all three"
    );
    // A replay of any of them writes nothing.
    for id in ["m1", "m2", "m3"] {
        harness.deliver(&msg(id)).unwrap();
    }
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string(), "m3".to_string()]
    );
    let text = read_log(&log_path(&dir));
    for id in ["m1", "m2", "m3"] {
        assert_eq!(
            text.matches(&format!("anyagents-message:{id}")).count(),
            1,
            "{id} is written exactly once: {text}"
        );
    }
    drop(harness);
}

#[test]
fn a_line_written_into_an_idle_stream_is_marked_idle_and_one_mid_turn_is_not() {
    // The adapter writes each line into the stream-json child as it arrives and
    // remembers whether a turn was open: the first line opens one, the second
    // joins it.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer", Some("sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_echoed("m1", Duration::from_secs(5)));
    assert_eq!(harness.phase_of("m1"), Some("idle"));
    harness.deliver(&msg("m2")).unwrap();
    assert!(harness.wait_echoed("m2", Duration::from_secs(5)));
    assert_eq!(harness.phase_of("m2"), Some("turn"));
    assert_eq!(harness.turns_started(), 1);
}

#[test]
fn a_line_steered_mid_turn_is_echoed_and_never_written_twice() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer", Some("sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    assert!(harness.wait_echoed("m1", Duration::from_secs(5)));
    // The turn is still open after the echo: the agent is working.
    assert!(!harness.is_finished("m1"));
    let steered = harness.deliver(&msg("m2")).unwrap();
    assert_eq!(
        steered,
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert!(harness.wait_echoed("m2", Duration::from_secs(5)));
    assert_eq!(harness.turns_started(), 1);
    // Running the mailbox again, or a replay after a crash, writes nothing.
    harness.deliver(&msg("m2")).unwrap();
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
    // The child logs each line it reads. Each was written to it exactly once:
    // the echo proves the child took it, and `written_ids` above proves the
    // hand-over was not repeated.
    assert!(wait_log(&log_path(&dir), "anyagents-message:m2"));
    let text = read_log(&log_path(&dir));
    assert_eq!(text.matches("anyagents-message:m1").count(), 1, "{text}");
    assert_eq!(text.matches("anyagents-message:m2").count(), 1, "{text}");
    // Closing stdin lets the child exit; the turn ends with the child.
    drop(harness);
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
