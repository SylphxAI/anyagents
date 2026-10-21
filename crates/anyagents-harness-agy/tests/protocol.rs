//! Scripted stand-in for the Antigravity child.

use std::collections::HashSet;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, OutboundMessage, SteerSupport, TurnReport,
};
use anyagents_harness_agy::{AgyHarness, PendingLine, hook_answer, run_hook, write_settings};
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
fn a_result_records_usage_the_answer_and_a_second_deliver_is_not_written() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    let usage = harness.usage_for("m1").expect("usage");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.reported_cost_micros, None);
    assert_eq!(harness.session_id().as_deref(), Some("sess-from-vendor"));
    assert_eq!(harness.health().delivery_mode, DeliveryMode::Followup);
    assert_eq!(harness.health().steer_support, SteerSupport::Unproven);
    assert_eq!(
        harness.turn_report_for("m1"),
        Some(TurnReport::Succeeded(usage))
    );
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn a_failed_result_is_a_failed_turn_with_the_vendors_reason() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "failed", None);
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    assert_eq!(
        harness.turn_report_for("m1"),
        Some(TurnReport::Failed("no output produced".to_string()))
    );
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
    harness.clear_quota();
    harness.deliver(&msg("m1")).unwrap();
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn an_echoed_id_is_not_written_again_and_is_the_floor_for_the_hook() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "echo", None);
    assert!(harness.wait_seen("echoed", Duration::from_secs(5)));
    harness.deliver(&msg("echoed")).unwrap();
    assert!(harness.written_ids().is_empty());
    assert_eq!(harness.newest_id().as_deref(), Some("echoed"));
}

#[test]
fn the_launch_reads_and_writes_stream_json_and_resumes_by_conversation() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold", Some("sess-1"));
    harness.deliver(&msg("m1")).unwrap();
    let log = dir.path().join("log");
    assert!(wait_log(&log, "anyagents-message:m1"));
    assert!(
        // No `--print`: the CLI takes the word after it as the prompt, so print
        // mode comes from the stream formats themselves.
        read_log(&log).contains(
            "argv:--input-format stream-json --output-format stream-json --conversation sess-1"
        ),
        "{}",
        read_log(&log)
    );
}

fn lines() -> Vec<PendingLine> {
    vec![
        PendingLine {
            id: "m1".into(),
            seq: 1,
            text: "one".into(),
        },
        PendingLine {
            id: "m2".into(),
            seq: 2,
            text: "two".into(),
        },
        PendingLine {
            id: "m3".into(),
            seq: 3,
            text: "three".into(),
        },
    ]
}

#[test]
fn the_hook_answers_every_line_after_the_newest_and_never_one_the_server_wrote() {
    let delivered = HashSet::from(["m1".to_string()]);
    let answer = hook_answer(&lines(), Some(1), &delivered, 40);
    let text = answer["injectSteps"][0]["userMessage"]
        .as_str()
        .expect("user message");
    assert!(text.ends_with("anyagents-message:m2\ntwo\n\nanyagents-message:m3\nthree"));
    assert!(!text.contains("anyagents-message:m1\n"), "{text}");
    // The server already wrote m2: the hook starts after it.
    let answer = hook_answer(&lines(), Some(2), &delivered, 40);
    assert!(
        answer["injectSteps"][0]["userMessage"]
            .as_str()
            .unwrap()
            .ends_with("anyagents-message:m3\nthree")
    );
    // The limit takes the oldest lines; the rest follows on the next call.
    let answer = hook_answer(&lines(), None, &HashSet::new(), 2);
    let text = answer["injectSteps"][0]["userMessage"].as_str().unwrap();
    assert!(text.ends_with("anyagents-message:m1\none\n\nanyagents-message:m2\ntwo"));
    assert!(!text.contains("m3"), "{text}");
    assert_eq!(hook_answer(&lines(), Some(3), &delivered, 40), serde_json::json!({}));
}

#[test]
fn the_hook_answers_nothing_on_the_first_invocation_or_another_conversation() {
    let mut out = Vec::new();
    run_hook(
        r#"{"invocationNum":0,"conversationId":"c1"}"#.as_bytes(),
        &mut out,
        "c1",
        lines,
        None,
        &HashSet::new(),
        40,
    );
    assert_eq!(String::from_utf8(out).unwrap().trim(), "{}");
    let mut out = Vec::new();
    run_hook(
        r#"{"invocationNum":2,"conversationId":"other"}"#.as_bytes(),
        &mut out,
        "c1",
        lines,
        None,
        &HashSet::new(),
        40,
    );
    assert_eq!(String::from_utf8(out).unwrap().trim(), "{}");
    let mut out = Vec::new();
    run_hook(
        r#"{"invocationNum":2,"conversationId":"c1"}"#.as_bytes(),
        &mut out,
        "c1",
        lines,
        Some(2),
        &HashSet::new(),
        40,
    );
    let answer: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert!(
        answer["injectSteps"][0]["userMessage"]
            .as_str()
            .unwrap()
            .ends_with("anyagents-message:m3\nthree")
    );
}

#[test]
fn the_hook_is_registered_as_a_before_invocation_command() {
    let dir = TempDir::new().unwrap();
    let hook = dir.path().join("hooks").join("agy-steer-hook");
    std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
    std::fs::write(&hook, "#!/bin/sh\n").unwrap();
    write_settings(dir.path(), &hook).unwrap();

    // `agy` reads `hooks.PreInvocation`, a list of entries each holding hooks.
    let text = std::fs::read_to_string(dir.path().join("hooks.json")).unwrap();
    let settings: serde_json::Value = serde_json::from_str(&text).unwrap();
    let entries = settings["hooks"]["PreInvocation"].as_array().unwrap();
    let command = &entries[0]["hooks"][0];
    let shown = command["command"].as_str().unwrap().to_string();
    assert!(shown.ends_with("agy-steer-hook"), "command is {shown}");
    assert_eq!(
        command["env"]["ANYAGENTS_AGY_HOOK_DIR"].as_str(),
        Some(hook.display().to_string().as_str())
    );

    // Registering again replaces this agent's own entry instead of adding one.
    write_settings(dir.path(), &hook).unwrap();
    let text = std::fs::read_to_string(dir.path().join("hooks.json")).unwrap();
    let settings: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(settings["hooks"]["PreInvocation"].as_array().unwrap().len(), 1);
}

fn start_with_hook(dir: &TempDir, mode: &str) -> AgyHarness {
    let log = dir.path().join("log");
    let config = dir.path().join("config");
    let log2 = dir.path().join("debug");
    std::fs::create_dir_all(&config).unwrap();
    AgyHarness::start(
        env!("CARGO_BIN_EXE_fake_agy"),
        None,
        &[
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_HOOK", env!("CARGO_BIN_EXE_anyagents-steer-hook")),
            ("CONFIG_DIR", config.to_str().unwrap()),
            ("ANYAGENTS_AGY_DEBUG", log2.to_str().unwrap()),
            ("ANYAGENTS_DEBUG_LOG", log2.to_str().unwrap()),
        ],
    )
    .expect("start")
    .with_hook(config)
    .expect("hook")
}

fn write_line(harness: &AgyHarness, id: &str, seq: i64, text: &str) {
    use anyagents_harness::SteerLine;
    let queue = harness.steer_queue().expect("queue");
    let mut lines = queue.lines();
    lines.push(SteerLine {
        id: id.to_string(),
        seq,
        message: text.to_string(),
    });
    let body: String = lines
        .iter()
        .map(|line| serde_json::to_string(line).unwrap() + "\n")
        .collect();
    std::fs::write(queue.path(), body).unwrap();
}

#[test]
fn a_line_pushed_mid_turn_reaches_the_next_step_once() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    let mut harness = start_with_hook(&dir, "hook");
    let hook = harness.hook_dir().expect("hook dir").to_path_buf();
    assert!(hook.starts_with(dir.path()));
    harness.deliver(&msg("m1")).expect("first turn");
    // The agent's own settings name the hook and its directory.
    let mut settings: serde_json::Value =
        serde_json::from_slice(&std::fs::read(config.join(".agents").join("hooks.json")).unwrap())
            .unwrap();
    // The hook is the test build of the program.
    settings["hooks"]["PreInvocation"][0]["hooks"][0]["command"] =
        serde_json::json!(env!("CARGO_BIN_EXE_anyagents-steer-hook"));
    std::fs::write(
        config.join(".agents").join("hooks.json"),
        serde_json::to_vec_pretty(&settings).unwrap(),
    )
    .unwrap();
    let entry = &settings["hooks"]["PreInvocation"][0]["hooks"][0];
    assert!(
        entry["command"]
            .as_str()
            .is_some_and(|command| command.ends_with("anyagents-steer-hook")),
        "{settings}"
    );
    assert_eq!(
        entry["env"]["ANYAGENTS_AGY_HOOK_DIR"].as_str(),
        Some(hook.display().to_string().as_str()),
        "{settings}"
    );
    write_line(&harness, "m2", 2, "mid-turn line");
    let item = harness.poll_steer().expect("poll").expect("taken");
    assert_eq!(item.message.id, "m2");
    assert!(harness.inject(&item.message).unwrap());
    eprintln!(
        "DEBUG in-queue={:?} live={:?}",
        harness.steer_queue().unwrap().lines(),
        std::env::var("ANYAGENTS_AGY_HOOK_DIR")
    );
    std::thread::sleep(std::time::Duration::from_millis(300));
    eprintln!("DEBUG after-sleep log={}", read_log(&dir.path().join("log")));
    let seen = harness.wait_seen("m2", Duration::from_secs(5));
    let lines = harness.steer_queue().unwrap().lines();
    let log = read_log(&dir.path().join("log"));
    assert!(seen, "queue={lines:?} log={log}");
    let log = read_log(&dir.path().join("log"));
    // The queue line, the hook's answer, and the child's report that it read it.
    assert!(log.contains(
        "hook:1:{\"injectSteps\":[{\"userMessage\":\"anyagents-message:m2\\nmid-turn line\"}]}"
    ), "{log}");
    // The hook answered with the line once. The child echoed it on stdout, not
    // into this log, so one occurrence here is the whole injected line.
    assert_eq!(log.matches("anyagents-message:m2").count(), 1, "{log}");
    assert!(harness.wait_seen("m2", Duration::from_secs(5)), "{log}");
    // The hook took the line once; the queue is empty, so no later step repeats it.
    assert!(harness.steer_queue().unwrap().lines().is_empty());
    // The child never wrote the line as turn input; the mailbox was the writer.
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
    // Handing the same id over again writes nothing.
    harness.inject(&item.message).unwrap();
    // Still written once: the second hand-off adds nothing.
    assert_eq!(
        read_log(&dir.path().join("log"))
            .matches("anyagents-message:m2")
            .count(),
        1
    );
}

#[test]
fn the_hook_answers_nothing_without_a_queue_or_on_the_first_call() {
    let hook = env!("CARGO_BIN_EXE_anyagents-steer-hook");
    let run = |event: &str, hook_dir: Option<&Path>| {
        use std::io::Write;
        let mut command = std::process::Command::new(hook);
        command.env_remove("ANYAGENTS_AGY_HOOK_DIR");
        if let Some(dir) = hook_dir {
            command.env("ANYAGENTS_AGY_HOOK_DIR", dir);
        }
        let mut child = command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(event.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    };
    assert_eq!(run("{\"invocationNum\":3}", None), "{}");
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("steer.jsonl"), "").unwrap();
    assert_eq!(run("{\"invocationNum\":2}", Some(dir.path())), "{}");
    std::fs::write(
        dir.path().join("steer.jsonl"),
        "{\"id\":\"m2\",\"seq\":2,\"message\":\"hi\"}\n",
    )
    .unwrap();
    assert_eq!(run("{\"invocationNum\":0}", Some(dir.path())), "{}");
    let answer = run("{\"invocationNum\":1}", Some(dir.path()));
    assert!(answer.contains("injectSteps"), "{answer}");
    assert!(answer.contains("anyagents-message:m2"), "{answer}");
}
