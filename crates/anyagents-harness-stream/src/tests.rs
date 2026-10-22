//! The catalog entry and the printed line, decided without a process. The child
//! itself is covered by each adapter's scripted test.

use super::{LineProtocol, StreamSpec};
use serde_json::json;

#[test]
fn each_catalog_entry_starts_and_resumes_with_its_own_arguments() {
    let agy = StreamSpec::for_program("agy");
    assert_eq!(agy.name, "anyagents-agy");
    assert_eq!(
        agy.resume_args(Some("s1")),
        ["--output-format", "stream-json", "--resume", "s1"]
    );
    assert_eq!(agy.resume_args(None), ["--output-format", "stream-json"]);

    let opencode = StreamSpec::for_program("opencode");
    assert_eq!(
        opencode.resume_args(Some("s1")),
        ["serve", "--session", "s1"]
    );

    let omp = StreamSpec::for_program("omp");
    assert_eq!(omp.name, "anyagents-omp");
    assert_eq!(omp.protocol, LineProtocol::Done);
    assert_eq!(omp.resume_args(Some("s1")), ["--session", "s1"]);
    assert_eq!(omp.resume_args(None), Vec::<String>::new());
}

#[test]
fn the_closing_line_decides_the_turn_and_carries_its_usage() {
    let done = json!({
        "type": "done",
        "session_id": "sess-1",
        "usage": {"input_tokens": 11, "output_tokens": 7, "cache_read_tokens": 3, "cache_write_tokens": 2}
    });
    let effect = StreamSpec::for_program("omp").effect(&done);
    assert!(effect.finishes_turn);
    let usage = effect.usage.expect("usage on the closing line");
    assert_eq!((usage.input_tokens, usage.output_tokens), (11, 7));
    assert_eq!((usage.cache_read_tokens, usage.cache_write_tokens), (3, 2));
    assert_eq!(usage.reported_cost_micros, None);
    assert_eq!(effect.session_id.as_deref(), Some("sess-1"));
    assert_eq!(effect.text, None);

    // `result` does not close a harness that ends on `done`.
    let other = StreamSpec::for_program("omp").effect(&json!({"type": "result"}));
    assert!(!other.finishes_turn);
    // A result-only harness ignores `done`.
    let agy = StreamSpec::for_program("agy").effect(&json!({"type": "done"}));
    assert!(!agy.finishes_turn);
}

#[test]
fn a_cost_is_kept_only_where_the_vendor_reports_one() {
    let result = json!({
        "type": "result",
        "cost_micros": 250_000,
        "usage": {"input_tokens": 1, "output_tokens": 2, "cache_read_tokens": 3, "cache_write_tokens": 4}
    });
    let with_cost = StreamSpec::for_program("opencode").effect(&result);
    let usage = with_cost.usage.expect("usage");
    assert_eq!(usage.reported_cost_micros, Some(250_000));
    let without = StreamSpec::for_program("agy").effect(&result);
    assert_eq!(without.usage.expect("usage").reported_cost_micros, None);
}

#[test]
fn answer_text_is_read_only_from_a_streaming_vendor() {
    let text = json!({"type": "text", "text": "pong"});
    let opencode = StreamSpec::for_program("opencode").effect(&text);
    assert!(!opencode.finishes_turn);
    assert_eq!(opencode.text.as_deref(), Some("pong"));
    // An empty or absent text is no answer.
    let empty = StreamSpec::for_program("opencode").effect(&json!({"type": "text", "text": ""}));
    assert_eq!(empty.text, None);
    // The result-only harness never takes text.
    assert_eq!(StreamSpec::for_program("agy").effect(&text).text, None);
}

#[test]
fn a_rate_limit_anywhere_is_a_wait_with_its_reset_time() {
    for program in ["agy", "omp", "opencode"] {
        let effect = StreamSpec::for_program(program)
            .effect(&json!({"type": "rate_limit", "resets_at_ms": 1_700_000_000_000i64}));
        assert_eq!(effect.quota_reset_at_ms, Some(1_700_000_000_000));
        assert!(!effect.finishes_turn);
    }
    let done = StreamSpec::for_program("omp").effect(&json!({"type": "done"}));
    assert_eq!(done.quota_reset_at_ms, None);
}

#[cfg(target_os = "linux")]
fn pipe_harness(script: &str) -> super::StreamHarness {
    super::StreamHarness::launch(super::Launch {
        spec: StreamSpec::for_program("agy"),
        program: "/usr/bin/python3".into(),
        args: vec!["-c".into(), script.into()],
        environment: vec![],
        work_dir: None,
        session_id: None,
        mode: anyagents_harness::DeliveryMode::Followup,
        steer: anyagents_harness::SteerSupport::Unproven,
    })
    .unwrap()
}

#[cfg(target_os = "linux")]
fn large_message() -> anyagents_harness::OutboundMessage {
    anyagents_harness::OutboundMessage {
        id: "large".into(),
        seq: 1,
        text: "x".repeat(2 * 1024 * 1024),
        sender: Some("sender".into()),
        reply_to: Some("reply".into()),
        source_time_ms: Some(123),
        context: anyagents_harness::Context::default(),
    }
}

// The child stops consuming stdin while it fills stdout. The old deliver held
// the reader's state lock while filling stdin, making both pipes stop forever.
// The watchdog kills only this test's child so a regression fails, never hangs.
#[test]
#[cfg(target_os = "linux")]
fn bidirectional_pipe_pressure_does_not_deadlock() {
    use std::{
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };
    let mut harness = pipe_harness(
        r#"
import sys
sys.stdin.buffer.read(1)
for i in range(16384):
    print('{"type":"progress","text":"' + 'x' * 128 + '"}')
sys.stdout.flush()
sys.stdin.buffer.readline()
print('{"type":"result"}', flush=True)
sys.stdin.buffer.read()
"#,
    );
    let pid = harness.process_id().unwrap();
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let message = large_message();
        let line = super::message_line(&StreamSpec::for_program("agy"), &message);
        let deadline = Instant::now() + Duration::from_secs(10);
        let outcome = loop {
            match harness.deliver(&message, &line) {
                Ok(anyagents_harness::DeliverOutcome::Deferred) if Instant::now() < deadline => {
                    assert!(harness.health().process_alive);
                    thread::yield_now();
                }
                outcome => break outcome,
            }
        };
        tx.send((harness, outcome)).unwrap();
    });
    let result = rx.recv_timeout(Duration::from_secs(15));
    if result.is_err() {
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
    }
    let (harness, outcome) = result.expect("stdin and stdout must both make progress");
    worker.join().unwrap();
    assert!(matches!(
        outcome,
        Ok(anyagents_harness::DeliverOutcome::Accepted { .. })
    ));
    assert!(harness.wait_finished("large", Duration::from_secs(3)));
    assert_eq!(harness.written_ids(), ["large"]);
}

#[test]
#[cfg(target_os = "linux")]
fn a_full_stdin_keeps_custody_order_and_stdout_health_responsive() {
    use std::time::{Duration, Instant};
    let mut harness = pipe_harness(
        r#"
import signal
print('{"type":"progress","text":"anyagents-message:ready\\nready"}', flush=True)
for i in range(16384):
    print('{"type":"progress","text":"' + 'x' * 128 + '"}')
print('{"type":"progress","text":"anyagents-message:drained\\nready"}', flush=True)
signal.pause()
"#,
    );
    assert!(harness.wait_seen("ready", Duration::from_secs(3)));
    let message = large_message();
    let line = super::message_line(&StreamSpec::for_program("agy"), &message);
    let began = Instant::now();
    assert_eq!(
        harness.deliver(&message, &line).unwrap(),
        anyagents_harness::DeliverOutcome::Deferred
    );
    assert!(began.elapsed() < Duration::from_secs(1));
    assert!(harness.health().process_alive);
    assert!(harness.wait_seen("drained", Duration::from_secs(3)));
    assert!(!harness.seen("large"));
    assert!(harness.written_ids().is_empty());
    let mut second = message.clone();
    second.id = "second".into();
    assert_eq!(
        harness.deliver(&second, &line).unwrap(),
        anyagents_harness::DeliverOutcome::Deferred
    );
    assert_eq!(harness.turns_started(), 1);
    assert!(matches!(
        harness.cancel(&serde_json::json!({"type":"interrupt"})),
        Err(anyagents_harness::HarnessError::Failed(_))
    ));
    assert_eq!(harness.pending.as_ref().unwrap().id, "large");
    let generation = super::lock(&harness.shared).generation;
    harness.stop_child();
    harness.spawn().unwrap();
    assert!(super::lock(&harness.shared).generation > generation);
    assert!(harness.pending.is_none());
    assert_eq!(
        harness.deliver(&message, &line).unwrap(),
        anyagents_harness::DeliverOutcome::Deferred
    );
    assert!(harness.pending.as_ref().unwrap().offset > 0);
    assert!(harness.written_ids().is_empty());
}

#[test]
#[cfg(target_os = "linux")]
fn replacement_process_does_not_inherit_pipe_acceptance() {
    use std::time::Duration;
    let mut harness = pipe_harness(
        r#"
import sys
for line in sys.stdin:
    print('{"type":"result"}', flush=True)
"#,
    );
    let mut message = large_message();
    message.text = "small".into();
    let line = super::message_line(&StreamSpec::for_program("agy"), &message);
    assert!(matches!(
        harness.deliver(&message, &line).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted { .. }
    ));
    assert!(harness.wait_finished("large", Duration::from_secs(3)));
    assert!(matches!(
        harness.deliver(&message, &line).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: true
        }
    ));
    assert_eq!(harness.written_ids(), ["large"]);
    harness.stop_child();
    harness.spawn().unwrap();
    assert!(!harness.seen("large"));
    assert!(!super::lock(&harness.shared).finished.contains("large"));
    assert!(matches!(
        harness.deliver(&message, &line).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted { .. }
    ));
    assert!(harness.wait_finished("large", Duration::from_secs(3)));
    assert_eq!(harness.written_ids(), ["large"]);
}

#[test]
#[cfg(target_os = "linux")]
fn pipe_writability_notifies_the_existing_activity_owner() {
    use std::{
        sync::{Arc, mpsc},
        time::{Duration, Instant},
    };
    let mut harness = pipe_harness(
        r#"
import signal, sys
signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1})
print('{"type":"progress","text":"anyagents-message:ready\\nready"}', flush=True)
signal.sigwait({signal.SIGUSR1})
for line in sys.stdin:
    pass
"#,
    );
    assert!(harness.wait_seen("ready", Duration::from_secs(3)));
    let message = large_message();
    let line = super::message_line(&StreamSpec::for_program("agy"), &message);
    assert_eq!(
        harness.deliver(&message, &line).unwrap(),
        anyagents_harness::DeliverOutcome::Deferred
    );
    assert!(!harness.input_ready());
    let (tx, rx) = mpsc::channel();
    let shared = Arc::clone(&harness.shared);
    harness.watch_activity(Arc::new(move || {
        let _ = tx.send(!super::lock(&shared).input_blocked);
    }));
    // The activity watcher first reports the earlier stdout event. It must not
    // report writable until the child is explicitly released to read stdin.
    assert!(!rx.recv_timeout(Duration::from_secs(3)).unwrap());
    unsafe {
        libc::kill(harness.process_id().unwrap() as i32, libc::SIGUSR1);
    }
    assert!(rx.recv_timeout(Duration::from_secs(3)).unwrap());
    assert!(harness.input_ready());
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match harness.deliver(&message, &line).unwrap() {
            anyagents_harness::DeliverOutcome::Accepted { .. } => break,
            anyagents_harness::DeliverOutcome::Deferred => {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
    assert_eq!(harness.written_ids(), ["large"]);
}
