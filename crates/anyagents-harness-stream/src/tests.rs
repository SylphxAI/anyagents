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

#[test]
#[cfg(target_os = "linux")]
fn agy_rpc_steers_mid_turn_via_language_server_and_cancels_natively() {
    use std::{
        io::{Read as _, Write as _},
        net::TcpListener,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    };

    let dir = std::env::temp_dir();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let running = Arc::new(AtomicBool::new(true));
    let steered: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let cancelled: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));

    let running_srv = Arc::clone(&running);
    let steered_srv = Arc::clone(&steered);
    let cancelled_srv = Arc::clone(&cancelled);
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut data = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = stream.read(&mut chunk).unwrap_or(0);
                if n == 0 {
                    break;
                }
                data.extend_from_slice(&chunk[..n]);
                if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&data[..pos]);
                    let content_len = head
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("Content-Length:")
                                .or_else(|| line.strip_prefix("content-length:"))
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if data.len() >= pos + 4 + content_len {
                        break;
                    }
                }
            }
            let req = String::from_utf8_lossy(&data);
            assert!(req.contains("x-codeium-csrf-token: test-csrf-123"));
            let (head, body) = req.split_once("\r\n\r\n").unwrap_or(("", ""));
            let first_line = head.lines().next().unwrap_or("");
            let resp_body = if first_line.contains("GetAllCascadeTrajectories") {
                let status = if running_srv.load(Ordering::SeqCst) {
                    "CASCADE_RUN_STATUS_RUNNING"
                } else {
                    "CASCADE_RUN_STATUS_IDLE"
                };
                json!({
                    "trajectorySummaries": {
                        "sess-agy-1": { "status": status }
                    }
                })
                .to_string()
            } else if first_line.contains("GetCascadeTrajectoryGeneratorMetadata") {
                json!({
                    "generatorMetadata": [
                        {
                            "plannerConfig": {
                                "planModel": 42,
                                "modelName": "gemini-4-pro-high"
                            }
                        }
                    ]
                })
                .to_string()
            } else if first_line.contains("SendUserCascadeMessage") {
                let val: serde_json::Value = serde_json::from_str(body).unwrap();
                steered_srv.lock().unwrap().push(val);
                "{}".to_string()
            } else if first_line.contains("CancelCascadeInvocation") {
                let val: serde_json::Value = serde_json::from_str(body).unwrap();
                cancelled_srv.lock().unwrap().push(val);
                "{}".to_string()
            } else {
                "{}".to_string()
            };
            let resp = format!(
                "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                resp_body.len(),
                resp_body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });

    let spec = StreamSpec::for_program("agy");
    let mut harness = super::StreamHarness::launch_with_agy_rpc(
        super::Launch {
            spec: spec.clone(),
            program: "/usr/bin/python3".into(),
            args: vec![
                "-u".into(),
                "-c".into(),
                r#"
import json, sys, time
print(json.dumps({"event":"init","session_id":"sess-agy-1"}), flush=True)
line1 = sys.stdin.readline()
time.sleep(0.15)
print(json.dumps({"event":"step_update","step_update":{"conversation_id":"sess-agy-1","step_index":0,"state":"DONE","step_type":"user_input"}}), flush=True)
line2 = sys.stdin.readline()
print(json.dumps({"event":"result","session_id":"sess-agy-1","usage":{"input_tokens":5,"output_tokens":3}}), flush=True)
line3 = sys.stdin.readline()
"#
                .into(),
            ],
            environment: vec![],
            work_dir: Some(dir.clone()),
            session_id: None,
            mode: anyagents_harness::DeliveryMode::Steer,
            steer: anyagents_harness::SteerSupport::Yes,
        },
        Some(super::AgyRpcConfig {
            config_dir: dir,
            csrf_token: "test-csrf-123".into(),
            port_override: Some(port),
        }),
    )
    .unwrap();

    let mut m1 = large_message();
    m1.id = "m1".into();
    m1.text = "first".into();
    let line1 = super::message_line(&spec, &m1);
    assert!(matches!(
        harness.deliver(&m1, &line1).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    ));

    // Before `step_update` (`step_type: "user_input"`) arrives, m2 is deferred.
    let mut m2 = m1.clone();
    m2.id = "m2".into();
    m2.text = "second-steered".into();
    let line2 = super::message_line(&spec, &m2);
    assert!(!harness.input_ready());
    assert_eq!(
        harness.deliver(&m2, &line2).unwrap(),
        anyagents_harness::DeliverOutcome::Deferred
    );

    // Wait for `step_type: "user_input"` so `input_ready()` becomes true.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !harness.input_ready() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(harness.input_ready());

    // Now delivering m2 steers natively via SendUserCascadeMessage!
    assert_eq!(
        harness.deliver(&m2, &line2).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    {
        let calls = steered.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["cascadeId"], "sess-agy-1");
        assert_eq!(
            calls[0]["deliveryStrategy"],
            "MESSAGE_DELIVERY_STRATEGY_NEXT_INVOCATION"
        );
        assert!(
            calls[0]["items"][0]["text"]
                .as_str()
                .unwrap()
                .contains("anyagents-message:m2")
        );
    }

    // When backend transitions to IDLE just before stdout `result`, m3 is deferred
    // during the turn-close grace window.
    running.store(false, Ordering::SeqCst);
    let mut m3 = m1.clone();
    m3.id = "m3".into();
    m3.text = "third-after-turn".into();
    let line3 = super::message_line(&spec, &m3);
    assert_eq!(
        harness.deliver(&m3, &line3).unwrap(),
        anyagents_harness::DeliverOutcome::Deferred
    );
    assert!(!harness.input_ready());

    // If the turn remains open in IDLE past the grace window (e.g. an active
    // `schedule` background task keeps `pollResponse` alive without emitting
    // `result`), `input_ready()` recovers and `deliver()` wakes the cascade via
    // `SendUserCascadeMessage` with `plannerConfig`.
    thread::sleep(Duration::from_millis(220));
    assert!(harness.input_ready());
    assert_eq!(
        harness.deliver(&m3, &line3).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    {
        let calls = steered.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[1]["cascadeConfig"]["plannerConfig"]["modelName"],
            "gemini-4-pro-high"
        );
    }

    // A subsequent message (m4) arriving after an RPC wake must remain
    // immediately steerable (`input_ready() == true`), never deadlocking on
    // `turn_user_inputs == 0` when `finish_open` closed the previous turn.
    running.store(true, Ordering::SeqCst);
    assert!(harness.input_ready());
    let mut m4 = m1.clone();
    m4.id = "m4".into();
    m4.text = "fourth-after-rpc-wake".into();
    let line4 = super::message_line(&spec, &m4);
    assert_eq!(
        harness.deliver(&m4, &line4).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    {
        let calls = steered.lock().unwrap();
        assert_eq!(calls.len(), 3);
    }

    // Release python child to print `{"event":"result"}`.
    super::write_stdin(&harness.stdin, &json!({"release": true})).unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(3)));
    assert!(harness.wait_finished("m2", Duration::from_secs(3)));
    assert!(harness.wait_finished("m3", Duration::from_secs(3)));
    assert!(harness.input_ready());

    // Native cancel calls CancelCascadeInvocation.
    harness
        .cancel(&super::cancel_line(&spec))
        .expect("native cancel");
    {
        let calls = cancelled.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["cascadeId"], "sess-agy-1");
    }
}

#[test]
#[cfg(target_os = "linux")]
fn agy_completed_turn_with_stale_historical_quota_or_tool_error_still_finishes() {
    use std::time::Duration;
    let spec = StreamSpec::for_program("agy");
    let mut harness = pipe_harness(
        r#"
import json, sys
line = sys.stdin.readline()
print(json.dumps({"event":"step_update","step_update":{"conversation_id":"sess-agy-2","step_index":10,"state":"DONE","step_type":"user_input"}}), flush=True)
print(json.dumps({"event":"step_update","step_update":{"conversation_id":"sess-agy-2","step_index":11,"state":"ERROR","step_type":"run_command"}}), flush=True)
print(json.dumps({"event":"step_update","step_update":{"conversation_id":"sess-agy-2","step_index":12,"state":"DONE","step_type":"planner_response"}}), flush=True)
print(json.dumps({"event":"result","result":{"conversation_id":"sess-agy-2","status":"ERROR","error":"RESOURCE_EXHAUSTED (code 429): Individual quota reached. Resets in 5h12m40s.","usage":{"input_tokens":10,"output_tokens":5}}}), flush=True)
sys.stdin.read()
"#,
    );
    let mut m1 = large_message();
    m1.id = "m-done".into();
    m1.text = "hello".into();
    let line1 = super::message_line(&spec, &m1);
    assert!(matches!(
        harness.deliver(&m1, &line1).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted { .. }
    ));
    assert!(harness.wait_finished("m-done", Duration::from_secs(3)));
    assert_eq!(harness.health().quota_reset_at_ms, None);
    assert_eq!(
        harness.health().turn_state,
        anyagents_harness::TurnState::Idle
    );
}

#[test]
#[cfg(target_os = "linux")]
fn agy_quota_error_after_system_message_sets_quota_reset_and_rejects_turn() {
    use std::time::Duration;
    let spec = StreamSpec::for_program("agy");
    let mut harness = pipe_harness(
        r#"
import json, sys
line = sys.stdin.readline()
print(json.dumps({"event":"step_update","step_update":{"conversation_id":"sess-agy-3","step_index":10,"state":"DONE","step_type":"user_input"}}), flush=True)
print(json.dumps({"event":"step_update","step_update":{"conversation_id":"sess-agy-3","step_index":11,"state":"DONE","step_type":"system_message"}}), flush=True)
print(json.dumps({"event":"result","result":{"conversation_id":"sess-agy-3","status":"ERROR","error":"Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 30h59m1s.","usage":{"input_tokens":0,"output_tokens":0}}}), flush=True)
sys.stdin.read()
"#,
    );
    let mut m1 = large_message();
    m1.id = "m-quota".into();
    m1.text = "hello".into();
    let line1 = super::message_line(&spec, &m1);
    assert!(matches!(
        harness.deliver(&m1, &line1).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted { .. }
    ));
    let reset = harness
        .wait_quota(Duration::from_secs(3))
        .expect("quota reset should be recorded even after system_message");
    assert!(reset > 0);
    assert!(!harness.wait_finished("m-quota", Duration::from_millis(100)));
}

#[test]
fn bounded_stderr_withholds_partial_lines_and_decodes_invalid_utf8() {
    let mut capture = super::BoundedStderr::default();
    capture.push(b"safe diagnostic\n");
    capture.push(&vec![b'x'; super::STDERR_LIMIT - 20]);
    capture.push(b"sk-SYNTHETIC_LONG_UNTERMINATED_CREDENTIAL_0123456789");
    let masked = capture.masked(true);
    assert!(masked.starts_with("safe diagnostic\n"));
    assert!(masked.contains("stderr truncated"));
    assert!(!masked.contains("SYNTHETIC"));
    assert!(!masked.contains("sk-"));
    assert_eq!(capture.bytes.len(), super::STDERR_LIMIT);
    let mut ordinary = super::BoundedStderr::default();
    ordinary.push(b"failed \xff\n");
    assert_eq!(ordinary.masked(true), "failed \u{fffd}\n");
}

#[test]
fn partial_stderr_prefix_is_withheld_until_eof_and_uncertainty_keeps_exit_diagnostic() {
    let mut capture = super::BoundedStderr::default();
    capture.push(b"safe diagnostic\nsk-SYNTH");
    assert_eq!(capture.masked(false), "safe diagnostic\n");
    capture.push(b"ETIC_COMPLETE_CREDENTIAL_0123456789");
    assert_eq!(capture.masked(true), "safe diagnostic\n[redacted]");
    let mut safe = super::BoundedStderr::default();
    safe.push("config.toml is invalid; 測試🙂".as_bytes());
    assert_eq!(safe.masked(false), "");
    assert_eq!(safe.masked(true), "config.toml is invalid; 測試🙂");
    let mut state = super::State::new(None);
    state.unconfirmed = true;
    state.exit_fault =
        Some("harness exited with exit status: 1; stderr: failed to parse config.toml".into());
    let error = state.failure().unwrap();
    assert!(error.contains("unconfirmed steering input"));
    assert!(error.contains("exit status: 1"));
    assert!(error.contains("failed to parse config.toml"));
}

#[cfg(unix)]
fn immediate_receipt_harness() -> super::StreamHarness {
    immediate_receipt_fixture(IMMEDIATE_RECEIPT_SCRIPT, 0).unwrap()
}

#[cfg(unix)]
const IMMEDIATE_RECEIPT_SCRIPT: &str = r#"
import json,sys,time
time.sleep(int(sys.argv[1])/1000)
print(json.dumps({'type':'ready','session_id':'fixture-ready'}),flush=True)
first=None
for line in sys.stdin:
 m=json.loads(line)
 if m['type']=='cancel':
  print(json.dumps({'type':'result','request_id':first,'status':'completed'}),flush=True)
 elif m['request_id']=='refused':
  print(json.dumps({'type':'rejected','request_id':'refused','code':'fixture-refusal'}),flush=True)
 elif m['request_id']=='self-ack':
  print(json.dumps({'type':'accepted','request_id':'self-ack'}),flush=True)
 elif first is None:
  first=m['request_id']
  print(json.dumps({'type':'accepted','request_id':first}),flush=True)
 else:
  print(json.dumps({'type':'steer_ack','request_id':m['request_id'],'accepted':True,'turn_id':first}),flush=True)
"#;

#[cfg(unix)]
fn immediate_receipt_fixture(
    script: &str,
    startup_delay_ms: u64,
) -> Result<super::StreamHarness, String> {
    let harness = super::StreamHarness::launch(super::Launch {
        spec: StreamSpec {
            name: "immediate-receipt".into(),
            args: vec![],
            resume_flag: "--session",
            start_line: "user",
            cancel: "cancel",
            protocol: LineProtocol::AcknowledgedResult,
        },
        program: "/usr/bin/python3".into(),
        args: vec![
            "-u".into(),
            "-c".into(),
            script.into(),
            startup_delay_ms.to_string(),
        ],
        environment: vec![],
        work_dir: None,
        session_id: None,
        mode: anyagents_harness::DeliveryMode::Steer,
        steer: anyagents_harness::SteerSupport::Yes,
    })
    .map_err(|error| error.to_string())?;
    // Cold interpreter/module setup is not the receipt-ordering contract. Wait
    // for a frame processed by the real stdout reader before arming its 3s bound.
    harness.wait_pred(std::time::Duration::from_secs(15), |state| {
        state.session_id.as_deref() == Some("fixture-ready") || !state.alive
    });
    {
        let state = super::lock(&harness.shared);
        if !state.alive || state.session_id.as_deref() != Some("fixture-ready") {
            return Err(format!(
                "receipt fixture did not become ready: alive={}, failure={:?}, stderr={}",
                state.alive,
                state.failure(),
                state.stderr.masked(state.stderr_done)
            ));
        }
    }
    Ok(harness)
}

#[cfg(unix)]
fn await_receipt_before_post_write(
    harness: &mut super::StreamHarness,
    received: impl Fn(&super::State) -> bool + Send + 'static,
) {
    let shared = std::sync::Arc::clone(&harness.shared);
    let wake = std::sync::Arc::clone(&harness.wake);
    harness.after_write = Some(Box::new(move || {
        // The pipe write has returned, but deliver has not re-locked shared
        // state. Only the production stdout reader can satisfy this predicate.
        let state = super::lock(&shared);
        let (state, _) = wake
            .wait_timeout_while(state, std::time::Duration::from_secs(3), |state| {
                !received(state)
            })
            .unwrap();
        assert!(
            received(&state),
            "peer receipt was not processed before post-write decision: alive={}, failure={:?}, stderr={}",
            state.alive,
            state.failure(),
            state.stderr.masked(state.stderr_done)
        );
    }));
}

#[cfg(unix)]
fn receipt_message(id: &str) -> anyagents_harness::OutboundMessage {
    anyagents_harness::OutboundMessage {
        id: id.into(),
        seq: 1,
        text: id.into(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Default::default(),
    }
}

#[test]
#[cfg(unix)]
fn immediate_steer_ack_before_post_write_keeps_custody_until_terminal_receipt() {
    use anyagents_harness::DeliverOutcome::{Accepted, Deferred};
    let mut harness = immediate_receipt_harness();
    let first = receipt_message("first");
    await_receipt_before_post_write(&mut harness, |state| state.acknowledged.contains("first"));
    assert_eq!(
        harness
            .deliver(&first, &json!({"type":"user","request_id":"first"}))
            .unwrap(),
        Accepted {
            turn_finished: false
        },
        "an acknowledged initial input remains an allowed control"
    );
    let steer = receipt_message("steer");
    let line = json!({"type":"user","request_id":"steer"});
    await_receipt_before_post_write(&mut harness, |state| {
        state.acknowledged.contains("steer")
            && state
                .input_turns
                .get("steer")
                .is_some_and(|turn| turn == "first")
    });
    assert_eq!(
        harness.deliver(&steer, &line).unwrap(),
        Deferred,
        "first write cannot accept a busy steer even when its ack wins the lock"
    );
    assert_eq!(harness.deliver(&steer, &line).unwrap(), Deferred);
    assert_eq!(harness.written_ids(), ["first", "steer"]);
    harness.cancel(&json!({"type":"cancel"})).unwrap();
    assert!(harness.wait_finished("steer", std::time::Duration::from_secs(3)));
    assert_eq!(
        harness.deliver(&steer, &line).unwrap(),
        Accepted {
            turn_finished: true
        }
    );
    assert_eq!(
        harness.written_ids(),
        ["first", "steer"],
        "no replay after completion"
    );
}

#[test]
#[cfg(unix)]
fn immediate_rejection_before_post_write_is_a_typed_failure_without_replay() {
    let mut harness = immediate_receipt_harness();
    let message = receipt_message("refused");
    let line = json!({"type":"user","request_id":"refused"});
    await_receipt_before_post_write(&mut harness, |state| state.rejected.contains_key("refused"));
    for _ in 0..2 {
        let error = harness.deliver(&message, &line).unwrap_err();
        assert!(
            matches!(error, anyagents_harness::HarnessError::Failed(ref reason)
            if reason == "controller rejected input: fixture-refusal"),
            "{error:?}"
        );
    }
    assert_eq!(harness.written_ids(), ["refused"]);
}

#[test]
#[cfg(unix)]
fn immediate_self_ack_cannot_release_an_input_behind_an_open_turn() {
    use anyagents_harness::DeliverOutcome::{Accepted, Deferred};
    let mut harness = immediate_receipt_harness();
    await_receipt_before_post_write(&mut harness, |state| state.acknowledged.contains("first"));
    assert_eq!(
        harness
            .deliver(
                &receipt_message("first"),
                &json!({"type":"user","request_id":"first"})
            )
            .unwrap(),
        Accepted {
            turn_finished: false
        }
    );
    await_receipt_before_post_write(&mut harness, |state| {
        state
            .input_turns
            .get("self-ack")
            .is_some_and(|turn| turn == "self-ack")
    });
    let message = receipt_message("self-ack");
    let line = json!({"type":"user","request_id":"self-ack"});
    assert_eq!(harness.deliver(&message, &line).unwrap(), Deferred);
    assert_eq!(harness.deliver(&message, &line).unwrap(), Deferred);
    assert_eq!(harness.written_ids(), ["first", "self-ack"]);
}

#[test]
#[cfg(unix)]
fn immediate_receipt_setup_does_not_consume_the_post_write_deadline() {
    let start = std::time::Instant::now();
    // Longer than the unchanged 3s post-write bound: the old fixture failed
    // here before reaching any receipt or custody assertion.
    let mut harness = immediate_receipt_fixture(IMMEDIATE_RECEIPT_SCRIPT, 3_200).unwrap();
    assert!(start.elapsed() >= std::time::Duration::from_secs(3));
    await_receipt_before_post_write(&mut harness, |state| state.acknowledged.contains("first"));
    assert_eq!(
        harness
            .deliver(
                &receipt_message("first"),
                &json!({"type":"user","request_id":"first"})
            )
            .unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(harness.written_ids(), ["first"]);
}

#[test]
#[cfg(unix)]
fn immediate_receipt_setup_reports_peer_exit_before_readiness() {
    let error = immediate_receipt_fixture("raise RuntimeError('fixture startup failure')", 0)
        .err()
        .expect("an exited peer cannot satisfy readiness");
    assert!(error.contains("did not become ready"), "{error}");
    assert!(error.contains("fixture startup failure"), "{error}");
}

#[test]
fn cascade_run_status_requires_authoritative_running_or_idle_values() {
    use super::agy_running_status;
    for (status, running) in [
        ("CASCADE_RUN_STATUS_RUNNING", true),
        ("CASCADE_RUN_STATUS_IDLE", false),
    ] {
        assert_eq!(
            agy_running_status(
                &json!({"trajectorySummaries": {"s1": {"status": status}}}),
                "s1"
            )
            .unwrap(),
            ("s1".into(), running)
        );
    }
    for entry in [
        json!({}),
        json!({"status": null}),
        json!({"status": 1}),
        json!({"status": "unknown"}),
    ] {
        for requested in ["s1", "missing"] {
            let error =
                agy_running_status(&json!({"trajectorySummaries": {"s1": entry}}), requested)
                    .unwrap_err();
            assert!(error.contains("invalid cascade run status"));
        }
    }
    assert!(
        agy_running_status(&json!({}), "s1")
            .unwrap_err()
            .contains("trajectorySummaries")
    );
    assert_eq!(
        agy_running_status(&json!({"trajectorySummaries": {}}), "s1").unwrap(),
        ("s1".into(), false)
    );
}

#[test]
fn malformed_latest_step_cannot_clear_an_observed_quota_error() {
    use super::agy_step_quota_error;
    for step in [
        json!({}),
        json!({"type": null}),
        json!({"type": 7}),
        json!({"type": ""}),
        json!({"type": "CORTEX_STEP_TYPE_ERROR_MESSAGE"}),
    ] {
        assert!(agy_step_quota_error(&step).is_err(), "{step}");
    }
    assert!(agy_step_quota_error(&json!({"type": "CORTEX_STEP_TYPE_ERROR_MESSAGE", "status": "CORTEX_STEP_STATUS_FINISHED", "error": "RESOURCE_EXHAUSTED"})).unwrap());
    assert!(!agy_step_quota_error(&json!({"type": "CORTEX_STEP_TYPE_ERROR_MESSAGE", "status": "CORTEX_STEP_STATUS_CLEARED", "error": "RESOURCE_EXHAUSTED"})).unwrap());
    assert!(!agy_step_quota_error(&json!({"type": "CORTEX_STEP_TYPE_PLANNER_RESPONSE"})).unwrap());
}

fn agy_rpc_fixture(responses: Vec<serde_json::Value>) -> (u16, std::thread::JoinHandle<()>) {
    use std::io::{BufRead, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
            let mut length = None;
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.strip_prefix("Content-Length: ") {
                    length = Some(value.trim().parse::<usize>().unwrap());
                }
            }
            let mut body = vec![0; length.unwrap()];
            reader.read_exact(&mut body).unwrap();
            let _: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let body = response.to_string();
            write!(
                stream,
                "HTTP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    (port, server)
}

#[test]
fn malformed_run_status_is_refused_through_the_language_server_rpc() {
    for (entry, expected) in [
        (json!({"status": "CASCADE_RUN_STATUS_RUNNING"}), Some(true)),
        (json!({"status": "CASCADE_RUN_STATUS_IDLE"}), Some(false)),
        (json!({}), None),
    ] {
        let (port, server) = agy_rpc_fixture(vec![json!({"trajectorySummaries": {"s1": entry}})]);
        let outcome = super::agy_rpc_is_running(port, "test-csrf", "s1");
        server.join().unwrap();
        match expected {
            Some(running) => assert_eq!(outcome.unwrap(), ("s1".into(), running)),
            None => assert!(outcome.unwrap_err().contains("invalid cascade run status")),
        }
    }
}

#[test]
fn missing_step_type_is_unknown_not_quota_clearance_through_rpc() {
    for (step, expected) in [
        (json!({"status": "CORTEX_STEP_STATUS_FINISHED"}), None),
        (
            json!({"type": "CORTEX_STEP_TYPE_PLANNER_RESPONSE"}),
            Some(false),
        ),
        (
            json!({"type": "CORTEX_STEP_TYPE_ERROR_MESSAGE", "status": "CORTEX_STEP_STATUS_FINISHED", "error": "RESOURCE_EXHAUSTED"}),
            Some(true),
        ),
    ] {
        let (port, server) = agy_rpc_fixture(vec![
            json!({"trajectorySummaries": {"s1": {"stepCount": 3}}}),
            json!({"steps": [step]}),
        ]);
        let outcome = super::agy_rpc_is_latest_step_quota_error(port, "test-csrf", "s1");
        server.join().unwrap();
        match expected {
            Some(quota) => assert_eq!(outcome.unwrap(), quota),
            None => assert!(
                outcome
                    .unwrap_err()
                    .contains("latest cascade step missing valid type")
            ),
        }
    }
}

#[test]
#[cfg(unix)]
fn poisoned_agy_result_retires_the_session_without_acknowledging_the_turn() {
    use std::time::Duration;
    for (program, error, poisoned) in [
        (
            "agy",
            "invalid_argument (CODE 400): Request contains an invalid argument",
            true,
        ),
        ("agy", "temporary server error", false),
        (
            "omp",
            "INVALID_ARGUMENT (code 400): Request contains an invalid argument",
            false,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let pointer = dir.path().join("agy-session-id");
        std::fs::write(&pointer, "selected").unwrap();
        let spec = StreamSpec::for_program(program);
        let result = json!({"type":"result","session_id":"selected","error":{"message":error}});
        let script = format!("read line; printf '%s\\n' '{}'; exec sleep 30", result);
        let mut harness = super::StreamHarness::launch_with_agy_rpc(
            super::Launch {
                spec: spec.clone(),
                program: "/bin/sh".into(),
                args: vec!["-c".into(), script],
                environment: vec![],
                work_dir: None,
                session_id: Some("selected".into()),
                mode: anyagents_harness::DeliveryMode::Followup,
                steer: anyagents_harness::SteerSupport::Unproven,
            },
            Some(super::AgyRpcConfig {
                config_dir: dir.path().into(),
                csrf_token: "fixture".into(),
                port_override: None,
            }),
        )
        .unwrap();
        let message = anyagents_harness::OutboundMessage {
            id: "input".into(),
            seq: 1,
            text: "work".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: anyagents_harness::Context::default(),
        };
        let line = super::message_line(&spec, &message);
        assert!(matches!(
            harness.deliver(&message, &line),
            Ok(anyagents_harness::DeliverOutcome::Accepted { .. })
                | Err(anyagents_harness::HarnessError::Dead)
        ));
        assert!(harness.wait_pred(Duration::from_secs(3), |state| state.epoch > 0));
        assert_eq!(harness.health().process_alive, !poisoned);
        assert_eq!(pointer.exists(), !poisoned);
        if poisoned {
            assert_eq!(harness.session_id(), None);
            assert_eq!(harness.turns_served(), 0);
            assert!(harness.health().sign_in_fault.unwrap().contains("CODE 400"));
            let (_, persisted) = super::detect_agy_session_quota_and_error(dir.path(), i64::MAX);
            assert_eq!(persisted.as_deref(), Some(error));
        } else {
            assert_eq!(harness.session_id().as_deref(), Some("selected"));
        }
        // Drop kills this test's child even though stdout deliberately stays open.
    }
}

#[test]
#[cfg(unix)]
fn synthetic_401_claude_turn_fails_and_stops_stream_without_completing_turn() {
    use std::time::Duration;

    let synthetic = json!({
        "type": "assistant",
        "session_id": "sess-1",
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
        super::synthetic_auth_error(&synthetic)
            .unwrap()
            .contains("OAuth access token has expired")
    );
    let real_reply = json!({
        "type": "assistant",
        "message": {
            "model": "claude-opus-4-6",
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": "Quoting error: Failed to authenticate. API Error: 401 OAuth access token has expired"
            }]
        }
    });
    assert!(super::synthetic_auth_error(&real_reply).is_none());

    let spec = StreamSpec::for_program("claude");
    let result = json!({
        "type": "result",
        "subtype": "success",
        "session_id": "sess-1",
        "result": "Failed to authenticate. API Error: 401 OAuth access token has expired"
    });
    let script = format!(
        "read line; printf '%s\\n' '{}' '{}'; exec sleep 30",
        synthetic, result
    );
    let mut harness = super::StreamHarness::launch(super::Launch {
        spec: spec.clone(),
        program: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        environment: vec![],
        work_dir: None,
        session_id: Some("sess-1".into()),
        mode: anyagents_harness::DeliveryMode::Steer,
        steer: anyagents_harness::SteerSupport::Yes,
    })
    .unwrap();
    let message = anyagents_harness::OutboundMessage {
        id: "input".into(),
        seq: 1,
        text: "work".into(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: anyagents_harness::Context::default(),
    };
    let line = super::message_line(&spec, &message);
    assert!(matches!(
        harness.deliver(&message, &line),
        Ok(anyagents_harness::DeliverOutcome::Accepted { .. })
            | Err(anyagents_harness::HarnessError::Dead)
    ));
    assert!(harness.wait_pred(Duration::from_secs(3), |state| !state.alive));
    assert!(!harness.health().process_alive);
    assert_eq!(harness.turns_served(), 0);
    assert!(!harness.wait_finished("input", Duration::from_millis(10)));
    assert!(
        harness
            .health()
            .sign_in_fault
            .unwrap()
            .contains("OAuth access token has expired")
    );
}

#[test]
fn agy_rpc_discovers_running_turn_one_trajectory_and_accepts_steer_before_stdout_init() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let steered = Arc::new(Mutex::new(Vec::<String>::new()));
    let steered_srv = Arc::clone(&steered);
    let server = std::thread::spawn(move || {
        for _ in 0..4 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = vec![0u8; 8192];
            let mut total = 0usize;
            loop {
                let n = stream.read(&mut buf[total..]).unwrap_or(0);
                if n == 0 {
                    break;
                }
                total += n;
                if let Some(hdr_end) = buf[..total].windows(4).position(|w| w == b"\r\n\r\n") {
                    let hdr = String::from_utf8_lossy(&buf[..hdr_end]);
                    let content_len = hdr
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if total >= hdr_end + 4 + content_len {
                        let body_str =
                            String::from_utf8_lossy(&buf[hdr_end + 4..hdr_end + 4 + content_len])
                                .to_string();
                        let resp_json = if hdr.contains("GetAllCascadeTrajectories") {
                            serde_json::json!({
                                "trajectorySummaries": {
                                    "conv-turn-1": { "status": "CASCADE_RUN_STATUS_RUNNING" }
                                }
                            })
                        } else if hdr.contains("SendUserCascadeMessage") {
                            steered_srv.lock().unwrap().push(body_str);
                            serde_json::json!({})
                        } else {
                            serde_json::json!({})
                        };
                        let resp_bytes = resp_json.to_string();
                        let reply = format!(
                            "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                            resp_bytes.len(),
                            resp_bytes
                        );
                        let _ = stream.write_all(reply.as_bytes());
                        break;
                    }
                }
            }
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let spec = super::StreamSpec::for_program("agy");
    let mut harness = super::StreamHarness::launch_with_agy_rpc(
        super::Launch {
            spec: spec.clone(),
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "read line; exec sleep 30".into()],
            environment: vec![],
            work_dir: Some(dir.path().to_path_buf()),
            session_id: None,
            mode: anyagents_harness::DeliveryMode::Steer,
            steer: anyagents_harness::SteerSupport::Yes,
        },
        Some(super::AgyRpcConfig {
            config_dir: dir.path().to_path_buf(),
            csrf_token: "csrf-test".into(),
            port_override: Some(port),
        }),
    )
    .unwrap();

    let first = anyagents_harness::OutboundMessage {
        id: "m1".into(),
        seq: 1,
        text: "first long turn".into(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: anyagents_harness::Context::default(),
    };
    assert_eq!(
        harness
            .deliver(&first, &super::message_line(&spec, &first))
            .unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert!(harness.input_ready());

    let second = anyagents_harness::OutboundMessage {
        id: "m2".into(),
        seq: 2,
        text: "mid-turn steer before stdout init".into(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: anyagents_harness::Context::default(),
    };
    assert_eq!(
        harness
            .deliver(&second, &super::message_line(&spec, &second))
            .unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(harness.session_id().as_deref(), Some("conv-turn-1"));
    let recorded = steered.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert!(recorded[0].contains("conv-turn-1"));
    assert!(recorded[0].contains("mid-turn steer before stdout init"));
    drop(harness);
    let _ = server.join();
}
