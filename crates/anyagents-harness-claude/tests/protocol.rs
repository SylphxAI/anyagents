//! Scripted stream-json stand-in for the Claude Code child.

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryLevel, Harness, OutboundMessage, TurnState,
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

/// Offer `id` the way the delivery loop does until Claude Code proves it
/// entered the conversation.
fn taken(harness: &mut ClaudeHarness, id: &str) -> DeliverOutcome {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let outcome = harness.deliver(&msg(id)).unwrap();
        if outcome != DeliverOutcome::InFlight || Instant::now() > deadline {
            return outcome;
        }
        thread::sleep(Duration::from_millis(10));
    }
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
    let first = taken(&mut harness, "m1");
    assert!(
        matches!(first, DeliverOutcome::Accepted { .. }),
        "{first:?}"
    );
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    let usage = harness.usage_for("m1").expect("usage");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.cache_read_tokens, 3);
    assert_eq!(usage.cache_write_tokens, 2);
    assert_eq!(usage.reported_cost_micros, Some(500_000));
    assert_eq!(harness.session_id().as_deref(), Some("sess-from-vendor"));
    let health = harness.health();
    // Claude Code proves each line entered the conversation, by its uuid.
    assert_eq!(health.delivery.level, DeliveryLevel::NextStep);
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
fn a_line_proved_before_the_result_finishes_with_the_running_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "batch", None);
    harness.deliver(&msg("m1")).unwrap();
    harness.deliver(&msg("m2")).unwrap();
    assert!(harness.wait_finished("m2", Duration::from_secs(5)));
    assert!(harness.is_finished("m1"));
    assert_eq!(harness.turns_started(), 1, "a step folded m2 into the turn");
    assert!(harness.usage_for("m1").is_some());
    assert_eq!(harness.health().turn_state, TurnState::Idle);
}

#[test]
fn a_line_still_queued_when_the_turn_ends_starts_the_next_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "batch_late", None);
    assert!(matches!(
        taken(&mut harness, "m1"),
        DeliverOutcome::Accepted { .. }
    ));
    harness.deliver(&msg("m2")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    assert!(harness.wait_consumed("m2", Duration::from_secs(5)));
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
    assert!(harness.wait_consumed("m1", Duration::from_secs(5)));
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
    assert!(harness.wait_consumed("m1", Duration::from_secs(5)));
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
fn a_line_the_resumed_transcript_holds_is_a_duplicate_and_not_written() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("claude");
    let project = config.join("projects").join("-work");
    std::fs::create_dir_all(&project).unwrap();
    // A prompt row and a line an earlier process folded in mid-turn.
    std::fs::write(
        project.join("sess-7.jsonl"),
        concat!(
            r#"{"type":"user","uuid":"u1","message":{"role":"user","content":[{"type":"text","text":"anyagents-message:prior\nhello"}]}}"#,
            "\n",
            r#"{"type":"attachment","attachment":{"type":"queued_command","prompt":[{"type":"text","text":"anyagents-message:folded\nmore"}],"source_uuid":"u2"}}"#,
            "\n",
            r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","content":"anyagents-message:tool\nnot input"}]}}"#,
            "\n",
        ),
    )
    .unwrap();
    let log = log_path(&dir);
    let config = config.display().to_string();
    let mut harness = ClaudeHarness::start(
        env!("CARGO_BIN_EXE_fake_claude"),
        Some("sess-7"),
        &[
            ("ANYAGENTS_FAKE_MODE", "hold"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("CLAUDE_CONFIG_DIR", config.as_str()),
        ],
    )
    .unwrap();
    for id in ["prior", "folded"] {
        assert_eq!(
            harness.deliver(&msg(id)).unwrap(),
            DeliverOutcome::Duplicate
        );
    }
    assert!(harness.written_ids().is_empty());
    // Tool output that happens to start with the marker is not input.
    assert_eq!(
        harness.deliver(&msg("tool")).unwrap(),
        DeliverOutcome::InFlight
    );
}

#[test]
fn an_errored_result_is_a_failed_turn_with_no_usage() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "failed_turn", None);
    taken(&mut harness, "m1");
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
    // It entered the conversation before its turn failed: it is taken, and
    // the failed turn is the supervisor's to recover, not a second write.
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    assert_eq!(harness.turns_started(), 1);
    assert_eq!(harness.written_ids(), ["m1"]);
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
    // The child holds its turn open and proves every line it folds in. All
    // three mailbox lines reach it mid-turn, each written once, and each is
    // taken only on its proof.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer", Some("sess-1"));
    assert_eq!(harness.health().delivery.level, DeliveryLevel::NextStep);
    for id in ["m1", "m2", "m3"] {
        assert_ne!(harness.deliver(&msg(id)).unwrap(), DeliverOutcome::Deferred);
    }
    for id in ["m1", "m2", "m3"] {
        assert_eq!(
            taken(&mut harness, id),
            DeliverOutcome::Accepted {
                turn_finished: false
            },
            "{id} is taken without waiting for a turn to end"
        );
    }
    assert_eq!(
        harness.turns_started(),
        1,
        "one running turn took all three"
    );
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
        assert!(text.contains(&anyagents_harness_claude::uuid_for(id)));
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
    assert!(harness.wait_consumed("m1", Duration::from_secs(5)));
    assert_eq!(harness.phase_of("m1"), Some("idle"));
    harness.deliver(&msg("m2")).unwrap();
    assert!(harness.wait_consumed("m2", Duration::from_secs(5)));
    assert_eq!(harness.phase_of("m2"), Some("turn"));
    assert_eq!(harness.turns_started(), 1);
}

#[test]
fn a_line_steered_mid_turn_is_proved_and_never_written_twice() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer", Some("sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    assert!(harness.wait_consumed("m1", Duration::from_secs(5)));
    // The turn is still open after the proof: the agent is working.
    assert!(!harness.is_finished("m1"));
    assert_eq!(
        taken(&mut harness, "m2"),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(harness.turns_started(), 1);
    // Running the mailbox again writes nothing.
    harness.deliver(&msg("m2")).unwrap();
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
    assert!(wait_log(&log_path(&dir), "anyagents-message:m2"));
    let text = read_log(&log_path(&dir));
    assert_eq!(text.matches("anyagents-message:m1").count(), 1, "{text}");
    assert_eq!(text.matches("anyagents-message:m2").count(), 1, "{text}");
    drop(harness);
}

/// Claude Code's `total_cost_usd` is the running total of one process: each
/// turn costs what it added, and a relaunched process counts from nothing.
#[test]
fn each_turn_costs_what_it_added_across_a_relaunch() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "turns", Some("sess-1"));
    let cost = |harness: &mut ClaudeHarness, id: &str| {
        let outcome = taken(harness, id);
        assert!(
            matches!(outcome, DeliverOutcome::Accepted { .. }),
            "{id}: {outcome:?}"
        );
        assert!(harness.wait_finished(id, Duration::from_secs(5)));
        harness.usage_for(id).expect("usage").reported_cost_micros
    };
    assert_eq!(cost(&mut harness, "m1"), Some(500_000));
    assert_eq!(cost(&mut harness, "m2"), Some(500_000));
    harness.resume("sess-2").unwrap();
    assert_eq!(cost(&mut harness, "m3"), Some(500_000));
    assert_eq!(harness.served_usage().reported_cost_micros, Some(1_500_000));
}

#[test]
fn a_proof_stays_valid_across_a_relaunch() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "steer", Some("sess-1"));
    assert!(matches!(
        taken(&mut harness, "m1"),
        DeliverOutcome::Accepted { .. }
    ));
    harness.resume("sess-2").unwrap();
    assert!(matches!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::Accepted { .. }
    ));
    assert_eq!(harness.written_ids(), ["m1"]);
}

#[test]
fn claude_code_2_1_assistant_rate_limit_event_and_session_jsonl_detection() {
    use anyagents_harness_claude::{detect_session_quota_reset_ms, quota_from_claude};
    use serde_json::json;

    let rejected = json!({
        "type": "assistant",
        "message": {
            "model": "<synthetic>",
            "role": "assistant",
            "content": [{"type": "text", "text": "You've hit your session limit · resets 3:10am (UTC)"}]
        },
        "apiError": "usage_limit_reached",
        "apiErrorParams": {
            "rate_limit_info": {
                "status": "rejected",
                "resetsAt": 1792984200_i64,
                "rateLimitType": "five_hour"
            }
        },
        "quotaLimits": {
            "status": "rejected",
            "resetsAt": 1792984200_i64,
            "rateLimitType": "five_hour"
        },
        "error": "rate_limit",
        "isApiErrorMessage": true,
        "apiErrorStatus": 429
    });
    assert_eq!(quota_from_claude(&rejected), Some(1_792_984_200_000));

    // A normal rate_limit_event with status="allowed" must NOT be treated as a quota rejection.
    let allowed = json!({
        "type": "rate_limit_event",
        "rate_limit_info": {
            "status": "allowed",
            "resetsAt": 1792984200_i64,
            "rateLimitType": "five_hour"
        }
    });
    assert_eq!(quota_from_claude(&allowed), None);

    let dir = TempDir::new().unwrap();
    let proj_dir = dir.path().join("projects").join("-scratch-wt-owners-main");
    std::fs::create_dir_all(&proj_dir).unwrap();
    let jsonl = proj_dir.join("b66c1988-a2af-4bd3-adc5-d4f31d3e175f.jsonl");
    std::fs::write(
        dir.path().join("claude-session-id"),
        "b66c1988-a2af-4bd3-adc5-d4f31d3e175f\n",
    )
    .unwrap();
    std::fs::write(&jsonl, format!("{rejected}\n")).unwrap();
    assert_eq!(
        detect_session_quota_reset_ms(dir.path(), 1_792_970_000_000),
        Some(1_792_984_200_000)
    );
    assert_eq!(
        detect_session_quota_reset_ms(dir.path(), 1_792_990_000_000),
        None
    );
}

#[test]
fn stderr_is_drained_bounded_and_classified_without_publishing_vendor_secrets() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", None);
    assert!(harness.exit_diagnostic().is_none(), "live control");
    drop(harness);
    harness = start(&dir, "stderr_failure", None);
    assert!(
        harness.wait_dead(Duration::from_secs(5)),
        "stderr must not fill the pipe"
    );
    // EOF on stdout and stderr can race; wait for the complete evidence rather
    // than asserting a scheduler-specific order of reader notifications.
    let deadline = Instant::now() + Duration::from_secs(5);
    let diagnostic = loop {
        let text = harness.exit_diagnostic().unwrap();
        if text.contains("exit_code=23") && text.contains("stderr_complete=true") {
            break text;
        }
        assert!(Instant::now() < deadline, "{text}");
        thread::sleep(Duration::from_millis(5));
    };
    assert!(
        diagnostic.contains("mentions_permission_denied"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("raw_stderr=withheld"));
    assert!(
        !diagnostic.contains("synthetic-password")
            && !diagnostic.contains("do-not-publish")
            && !diagnostic.contains("xxx")
    );
    assert!(diagnostic.len() < 300);
    harness.resume("new-session").unwrap();
    assert!(
        harness.wait_dead(Duration::from_secs(5)),
        "relaunch drains stderr independently"
    );
}

#[test]
fn synthetic_401_auth_error_fails_turn_and_stops_process_without_recording_usage() {
    use anyagents_harness_claude::synthetic_auth_error_from_claude;
    use serde_json::json;

    let synthetic_assistant = json!({
        "type": "assistant",
        "message": {
            "model": "<synthetic>",
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": "Failed to authenticate. API Error: 401 {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"OAuth access token has expired.\"}}"
            }]
        }
    });
    assert!(
        synthetic_auth_error_from_claude(&synthetic_assistant)
            .unwrap()
            .contains("OAuth access token has expired")
    );

    let normal_assistant = json!({
        "type": "assistant",
        "message": {
            "model": "claude-opus-4-6",
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": "Investigating: Failed to authenticate. API Error: 401 OAuth access token has expired"
            }]
        }
    });
    assert!(synthetic_auth_error_from_claude(&normal_assistant).is_none());

    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "synthetic_401", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(
        harness.wait_dead(Duration::from_secs(5)),
        "synthetic 401 must terminate the child so the supervisor recycles and recovers the turn"
    );
    assert!(!harness.is_finished("m1"));
    assert!(harness.usage_for("m1").is_none());
    assert_eq!(harness.turns_served(), 0);
    let fault = harness
        .health()
        .sign_in_fault
        .expect("sign_in_fault recorded");
    assert!(fault.contains("401"), "{fault}");
    let failure = harness.failure_for("m1").expect("failure recorded for m1");
    assert!(
        failure.contains("OAuth access token has expired"),
        "{failure}"
    );
}

#[test]
fn the_session_quota_reset_is_kept_until_a_transcript_or_marker_changes() {
    use anyagents_harness_claude::detect_session_quota_reset_ms;
    use serde_json::json;

    let rejected = |resets_at: i64| {
        json!({
            "type": "assistant",
            "message": {"model": "<synthetic>", "role": "assistant", "content": []},
            "apiError": "usage_limit_reached",
            "quotaLimits": {"status": "rejected", "resetsAt": resets_at},
            "error": "rate_limit",
            "apiErrorStatus": 429
        })
    };
    let dir = TempDir::new().unwrap();
    let project = dir.path().join("projects").join("-work");
    std::fs::create_dir_all(&project).unwrap();
    let first = project.join("first.jsonl");
    std::fs::write(&first, format!("{}\n", rejected(1_792_984_200))).unwrap();
    assert_eq!(
        detect_session_quota_reset_ms(dir.path(), 1_792_970_000_000),
        Some(1_792_984_200_000)
    );
    // Unchanged files: the same answer, with the current time applied.
    assert_eq!(
        detect_session_quota_reset_ms(dir.path(), 1_792_990_000_000),
        None
    );
    // An append to the transcript is read.
    std::thread::sleep(std::time::Duration::from_millis(20));
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&first)
        .unwrap()
        .write_all(format!("{}\n", json!({"type": "user", "message": {}})).as_bytes())
        .unwrap();
    assert_eq!(
        detect_session_quota_reset_ms(dir.path(), 1_792_970_000_000),
        None
    );
    // A newer transcript is found.
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(
        project.join("second.jsonl"),
        format!("{}\n", rejected(1_792_999_000)),
    )
    .unwrap();
    assert_eq!(
        detect_session_quota_reset_ms(dir.path(), 1_792_970_000_000),
        Some(1_792_999_000_000)
    );
    // A clearing marker newer than the transcript hides it.
    std::fs::write(
        dir.path().join(".anyagents-quota-cleared-ms"),
        format!("{}\n", i64::MAX - 1),
    )
    .unwrap();
    assert_eq!(
        detect_session_quota_reset_ms(dir.path(), 1_792_970_000_000),
        None
    );
}

/// Claude Code announces a line (`started`, the replay echo) before it
/// stores it in the session transcript. A line is committed only once it is
/// stored: a process killed in between leaves it uncommitted, for the next
/// process.
#[test]
fn a_line_is_committed_only_once_claude_code_stored_it() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    let log = log_path(&dir);
    let config_text = config.display().to_string();
    let env = |mode: &'static str| {
        vec![
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("CLAUDE_CONFIG_DIR", config_text.as_str()),
        ]
    };
    let mut harness =
        ClaudeHarness::start(env!("CARGO_BIN_EXE_fake_claude"), None, &env("unpersisted")).unwrap();
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::InFlight
    );
    assert!(
        harness.wait_dead(Duration::from_secs(5)),
        "the fake exits after announcing the line"
    );
    // Announced, never stored: not committed, and no proof survives.
    assert!(harness.proven("m1").is_none());
    assert!(!harness.consumed("m1"));

    let mut stored =
        ClaudeHarness::start(env!("CARGO_BIN_EXE_fake_claude"), None, &env("hold")).unwrap();
    assert!(matches!(
        taken(&mut stored, "m2"),
        DeliverOutcome::Accepted { .. }
    ));
    assert!(stored.consumed("m2"));
}
