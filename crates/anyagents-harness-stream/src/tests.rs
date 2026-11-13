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

    let omp = StreamSpec::for_program("omp");
    assert_eq!(omp.name, "anyagents-omp");
    assert_eq!(omp.protocol, LineProtocol::Result);
    assert_eq!(omp.resume_args(Some("s1")), ["--session", "s1"]);
    assert_eq!(omp.resume_args(None), Vec::<String>::new());
}

#[test]
fn the_closing_line_decides_the_turn_and_carries_its_usage() {
    let result = json!({
        "type": "result",
        "session_id": "sess-1",
        "usage": {"input_tokens": 11, "output_tokens": 7, "cache_read_tokens": 3, "cache_write_tokens": 2}
    });
    // The contract double, like any `adapter.json` program, ends a turn with
    // a `result` line.
    let effect = StreamSpec::for_program("contract").effect(&result);
    assert!(effect.finishes_turn);
    let usage = effect.usage.expect("usage on the closing line");
    assert_eq!((usage.input_tokens, usage.output_tokens), (11, 7));
    assert_eq!((usage.cache_read_tokens, usage.cache_write_tokens), (3, 2));
    assert_eq!(usage.reported_cost_micros, None);
    assert_eq!(effect.session_id.as_deref(), Some("sess-1"));
    assert_eq!(effect.text, None);

    // No line protocol ends a turn on `done`.
    for program in ["contract", "agy"] {
        let done = StreamSpec::for_program(program).effect(&json!({"type": "done"}));
        assert!(!done.finishes_turn, "{program}");
    }
}

#[test]
fn no_line_protocol_reports_a_cost_or_answer_text() {
    let result = json!({
        "type": "result",
        "cost_micros": 250_000,
        "text": "pong",
        "usage": {"input_tokens": 1, "output_tokens": 2, "cache_read_tokens": 3, "cache_write_tokens": 4}
    });
    let effect = StreamSpec::for_program("agy").effect(&result);
    assert!(effect.finishes_turn);
    assert_eq!(effect.usage.expect("usage").reported_cost_micros, None);
    assert_eq!(effect.text, None);
    let text = json!({"type": "text", "text": "pong"});
    assert_eq!(StreamSpec::for_program("omp").effect(&text).text, None);
    // Retired OMP line frames no longer close a turn.
    for frame in [
        json!({"type": "session_settled"}),
        json!({"type": "prompt_result"}),
    ] {
        assert!(!StreamSpec::for_program("omp").effect(&frame).finishes_turn);
    }
}

#[test]
fn a_rate_limit_anywhere_is_a_wait_with_its_reset_time() {
    for program in ["agy", "omp"] {
        let effect = StreamSpec::for_program(program)
            .effect(&json!({"type": "rate_limit", "resets_at_ms": 1_700_000_000_000i64}));
        assert_eq!(effect.quota_reset_at_ms, Some(1_700_000_000_000));
        assert!(!effect.finishes_turn);
    }
    let result = StreamSpec::for_program("contract").effect(&json!({"type": "result"}));
    assert_eq!(result.quota_reset_at_ms, None);
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
        delivery: crate::LINE_PER_TURN,
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

    let config = tempfile::tempdir().unwrap();
    let dir = config.path().to_path_buf();
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
            delivery: crate::LINE_INTO_TURN,
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
    // The prompt is in flight until the cascade shows its turn's first step.
    assert_eq!(
        harness.deliver(&m1, &line1).unwrap(),
        anyagents_harness::DeliverOutcome::InFlight
    );

    // The cascade is known from `init`. Before the turn's first
    // `step_update` (`step_type: "user_input"`) arrives, m2 is deferred: the
    // cascade may still be idle, and a steer would overtake the turn.
    assert!(harness.wait_pred(Duration::from_secs(3), |state| state.session_id.is_some()));
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
    assert_eq!(
        harness.deliver(&m1, &line1).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );

    // Now delivering m2 steers natively via SendUserCascadeMessage, off the
    // delivery path. The language server's `{}` queues it; it is in flight
    // until the cascade's transcript shows the turn took it.
    assert_eq!(
        taken(&mut harness, &m2, &line2, config.path(), "sess-agy-1"),
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
        settled(&mut harness, &m3, &line3),
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
        settled(&mut harness, &m3, &line3),
        anyagents_harness::DeliverOutcome::InFlight
    );
    // m3 started the cascade's next invocation, which runs.
    running.store(true, Ordering::SeqCst);
    assert_eq!(
        until_taken(&mut harness, &m3, &line3, config.path(), "sess-agy-1"),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    // The held-open turn closed when m3 started the next invocation.
    assert!(harness.wait_finished("m2", Duration::from_secs(3)));
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
        taken(&mut harness, &m4, &line4, config.path(), "sess-agy-1"),
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
fn partial_stderr_prefix_is_withheld_until_eof_and_the_exit_diagnostic_is_kept() {
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
    state.exit_fault =
        Some("harness exited with exit status: 1; stderr: failed to parse config.toml".into());
    let error = state.failure().unwrap();
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
  first=None
 elif m['request_id']=='refused':
  print(json.dumps({'type':'rejected','request_id':'refused','code':'unknown_command'}),flush=True)
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
        delivery: crate::LINE_INTO_TURN,
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
fn an_older_peer_takes_the_next_input_only_after_its_turn_ends() {
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
        "an accepted turn commits its input"
    );
    let next = receipt_message("next");
    let line = json!({"type":"user","request_id":"next"});
    assert_eq!(harness.deliver(&next, &line).unwrap(), Deferred);
    assert_eq!(harness.deliver(&next, &line).unwrap(), Deferred);
    assert_eq!(
        harness.written_ids(),
        ["first"],
        "nothing reaches a busy turn"
    );
    harness.cancel(&json!({"type":"cancel"})).unwrap();
    assert!(harness.wait_finished("first", std::time::Duration::from_secs(3)));
    assert_eq!(
        deliver_until_accepted(&mut harness, "next"),
        Accepted {
            turn_finished: false
        }
    );
    assert_eq!(harness.written_ids(), ["first", "next"], "no replay");
}

/// A refusal for good, read before the write returns, is the answer: the
/// message is rejected, the same answer comes for the same id, it is never
/// written again, and the process is not failed.
#[test]
#[cfg(unix)]
fn immediate_rejection_before_post_write_is_rejected_without_replay() {
    let mut harness = immediate_receipt_harness();
    let message = receipt_message("refused");
    let line = json!({"type":"user","request_id":"refused"});
    await_receipt_before_post_write(&mut harness, |state| state.rejected.contains_key("refused"));
    for _ in 0..2 {
        assert_eq!(
            harness.deliver(&message, &line).unwrap(),
            anyagents_harness::DeliverOutcome::Rejected {
                reason: "spiron rejected the input: unknown_command".into(),
                message_specific: false,
            }
        );
    }
    assert_eq!(harness.written_ids(), ["refused"]);
    assert!(harness.health().process_alive);
    assert_eq!(harness.proven("refused"), None);
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
                delivery: crate::LINE_PER_TURN,
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
            Ok(anyagents_harness::DeliverOutcome::Accepted { .. }
                | anyagents_harness::DeliverOutcome::InFlight)
                | Err(anyagents_harness::HarnessError::Dead)
        ));
        assert!(harness.wait_pred(Duration::from_secs(3), |state| state.epoch > 0));
        // A poisoned session's child is stopped, and is gone once its
        // stdout's end is read; any other keeps running.
        if poisoned {
            assert!(harness.wait_pred(Duration::from_secs(5), |state| !state.alive));
        }
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
        delivery: crate::LINE_INTO_TURN,
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
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let steered = Arc::new(Mutex::new(Vec::<String>::new()));
    let steered_srv = Arc::clone(&steered);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_srv = Arc::clone(&stop);
    let server = std::thread::spawn(move || {
        while !stop_srv.load(std::sync::atomic::Ordering::SeqCst) {
            let mut stream = match listener.accept() {
                Ok((stream, _)) => {
                    let _ = stream.set_nonblocking(false);
                    stream
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    continue;
                }
                Err(_) => break,
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
            delivery: crate::LINE_INTO_TURN,
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
    let first_line = super::message_line(&spec, &first);
    // Written, and in flight until the cascade shows it took it: the host
    // is woken by that, not by asking again.
    assert_eq!(
        harness.deliver(&first, &first_line).unwrap(),
        anyagents_harness::DeliverOutcome::InFlight
    );
    assert!(!harness.input_ready());

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
        taken(
            &mut harness,
            &second,
            &super::message_line(&spec, &second),
            dir.path(),
            "conv-turn-1"
        ),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(harness.session_id().as_deref(), Some("conv-turn-1"));
    // The language server reported the cascade running: the prompt is taken.
    assert_eq!(
        harness.deliver(&first, &first_line).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    let recorded = steered.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert!(recorded[0].contains("conv-turn-1"));
    assert!(recorded[0].contains("mid-turn steer before stdout init"));
    drop(harness);
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = server.join();
}

/// Offer `message` as the delivery loop does until its Antigravity steer is
/// settled: a steer is in flight until the language server has answered it,
/// and one the cascade's mailbox holds until the cascade's record shows it
/// took it. Answers `InFlight` once the mailbox holds it.
fn settled(
    harness: &mut super::StreamHarness,
    message: &anyagents_harness::OutboundMessage,
    line: &serde_json::Value,
) -> anyagents_harness::DeliverOutcome {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let outcome = harness.deliver(message, line).unwrap();
        let held = super::lock(&harness.shared)
            .agy_queued
            .iter()
            .any(|queued| queued.id == message.id);
        if outcome != anyagents_harness::DeliverOutcome::InFlight || held {
            return outcome;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the steer of {} never settled",
            message.id
        );
        harness.wait_pred(std::time::Duration::from_secs(1), |state| {
            state
                .agy_steer
                .as_ref()
                .is_none_or(|steer| steer.outcome.is_some())
        });
    }
}

/// A `USER_INPUT` row of `cascade`'s transcript: the cascade took `id`.
fn take_in_transcript(config: &std::path::Path, cascade: &str, id: &str) {
    let logs = config
        .join(".gemini/antigravity-cli/brain")
        .join(cascade)
        .join(".system_generated/logs");
    std::fs::create_dir_all(&logs).unwrap();
    let mut transcript = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(logs.join("transcript_full.jsonl"))
        .unwrap();
    let row = json!({
        "step_index": 1, "source": "USER_EXPLICIT", "type": "USER_INPUT", "status": "DONE",
        "created_at": "2026-11-13T17:00:00Z",
        "content": format!("<USER_REQUEST>\nanyagents-message:{id}\nbody\n</USER_REQUEST>"),
    });
    std::io::Write::write_all(&mut transcript, format!("{row}\n").as_bytes()).unwrap();
}

/// Steer `message`: the language server queues it and it stays in flight,
/// proving nothing, until the cascade's transcript shows the turn took it.
fn taken(
    harness: &mut super::StreamHarness,
    message: &anyagents_harness::OutboundMessage,
    line: &serde_json::Value,
    config: &std::path::Path,
    cascade: &str,
) -> anyagents_harness::DeliverOutcome {
    assert_eq!(
        settled(harness, message, line),
        anyagents_harness::DeliverOutcome::InFlight,
        "{} is not held by the cascade's mailbox",
        message.id
    );
    until_taken(harness, message, line, config, cascade)
}

/// The cascade takes `message`, which its mailbox holds: what `deliver`
/// answers once the adapter read that from its transcript.
fn until_taken(
    harness: &mut super::StreamHarness,
    message: &anyagents_harness::OutboundMessage,
    line: &serde_json::Value,
    config: &std::path::Path,
    cascade: &str,
) -> anyagents_harness::DeliverOutcome {
    assert_eq!(harness.proven(&message.id), None);
    take_in_transcript(config, cascade, &message.id);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let outcome = harness.deliver(message, line).unwrap();
        if outcome != anyagents_harness::DeliverOutcome::InFlight {
            return outcome;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the cascade's record of {} was never read",
            message.id
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

#[test]
fn an_agy_marker_is_found_whole_raw_or_as_json_escapes_it() {
    use super::agy_holds_marker;
    assert!(agy_holds_marker(
        b"\x12\x05anyagents-message:m1\nbody",
        "m1"
    ));
    assert!(agy_holds_marker(
        br#"{"content":"<USER_REQUEST>\nanyagents-message:m1\nbody"}"#,
        "m1"
    ));
    assert!(!agy_holds_marker(b"anyagents-message:m12\nbody", "m1"));
    assert!(!agy_holds_marker(b"see anyagents-message:m1", "m1"));
    assert!(!agy_holds_marker(b"", "m1"));
}

#[test]
fn agy_mailbox_bytes_decode_as_protobuf_json_writes_them() {
    use super::agy_base64;
    assert_eq!(agy_base64("aGVsbG8=").unwrap(), b"hello");
    assert_eq!(agy_base64("aGVsbG8").unwrap(), b"hello");
    assert_eq!(agy_base64("CA4gAw==").unwrap(), [0x08, 0x0e, 0x20, 0x03]);
    assert_eq!(agy_base64("+/8=").unwrap(), [0xfb, 0xff]);
    assert_eq!(agy_base64("-_8").unwrap(), [0xfb, 0xff]);
    assert!(agy_base64("not base64!").is_none());
}

/// A send that left no answer is settled from what the cascade wrote since
/// it began: a mailbox file carrying the message, or a transcript step.
#[test]
fn an_unanswered_agy_steer_is_settled_from_the_cascade_record() {
    let dir = tempfile::tempdir().unwrap();
    let rpc = super::AgyRpcConfig {
        config_dir: dir.path().into(),
        csrf_token: "csrf".into(),
        port_override: None,
    };
    let generated = dir
        .path()
        .join(".gemini/antigravity-cli/brain/cid-1/.system_generated");
    let messages = generated.join("messages");
    std::fs::create_dir_all(&messages).unwrap();
    std::fs::create_dir_all(generated.join("logs")).unwrap();
    let transcript = generated.join("logs/transcript_full.jsonl");
    std::fs::write(
        &transcript,
        "{\"type\":\"USER_INPUT\",\"content\":\"<USER_REQUEST>\\nanyagents-message:m2\\nold\"}\n",
    )
    .unwrap();
    let since = std::time::SystemTime::now();
    let from = std::fs::metadata(&transcript).unwrap().len();
    for id in ["m1", "m2"] {
        assert!(!super::agy_took(&rpc, "cid-1", id, since, from), "{id}");
    }

    // m1 queued: its text inside the base64 `CortexStep` of a mailbox file.
    std::fs::write(
        messages.join("8f1c.json"),
        json!({
            "id": "8f1c", "recipient": "cid-1", "sender": "user",
            "priority": "MESSAGE_PRIORITY_HIGH",
            "stepPayload": "CA6aAUASPmFueWFnZW50cy1tZXNzYWdlOm0xCjxtZXNzYWdlPgo8Ym9keT4KaGVsbG8KPC9ib2R5Pgo8L21lc3NhZ2U+",
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(messages.join("read.json"), r#"{"8f1c":true}"#).unwrap();
    assert!(super::agy_took(&rpc, "cid-1", "m1", since, from));
    assert!(!super::agy_took(&rpc, "cid-1", "m2", since, from));
    // A file from before the send is not its answer.
    let old = std::fs::File::options()
        .write(true)
        .open(messages.join("8f1c.json"))
        .unwrap();
    old.set_modified(since - std::time::Duration::from_secs(60))
        .unwrap();
    assert!(!super::agy_took(&rpc, "cid-1", "m1", since, from));

    // m2 taken by the cascade since: a step of the transcript past `from`.
    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(&transcript)
        .unwrap();
    std::io::Write::write_all(
        &mut log,
        b"{\"type\":\"USER_INPUT\",\"content\":\"<USER_REQUEST>\\nanyagents-message:m2\\nnew\"}\n",
    )
    .unwrap();
    assert!(super::agy_took(&rpc, "cid-1", "m2", since, from));
    // A cascade id that names more than one directory is never read.
    assert!(!super::agy_took(&rpc, "../cid-1", "m2", since, 0));
    assert!(!super::agy_took(&rpc, "", "m2", since, 0));
}

#[test]
fn the_agy_peer_is_the_language_server_this_process_started() {
    let dir = tempfile::tempdir().unwrap();
    let rpc = super::AgyRpcConfig {
        config_dir: dir.path().into(),
        csrf_token: "csrf".into(),
        port_override: None,
    };
    assert_eq!(super::agy_server_version(&rpc, Some(42)), None);
    let home = dir.path().join(".gemini/antigravity-cli");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("cli.log"),
        "I1113 17:04:37.460819      68 server.go:2531] Starting language server process with pid 4242\n\
         I1113 17:04:37.462199      68 server.go:2590] Language server version: 1.5.7\n\
         I1113 17:04:37.462677      68 server.go:1282] Language server listening on random port at 46219 for HTTP\n\
         I1113 17:05:37.460819      68 server.go:2531] Starting language server process with pid 42\n\
         I1113 17:05:37.462199      68 server.go:2590] Language server version: 1.5.8\n",
    )
    .unwrap();
    assert_eq!(
        super::agy_server_version(&rpc, Some(42)).as_deref(),
        Some("1.5.8")
    );
    assert_eq!(
        super::agy_server_version(&rpc, Some(4242)).as_deref(),
        Some("1.5.7")
    );
    // No block names the child (a wrapper's pid): the first one does.
    assert_eq!(
        super::agy_server_version(&rpc, Some(424)).as_deref(),
        Some("1.5.7")
    );
    assert_eq!(
        super::agy_server_version(&rpc, None).as_deref(),
        Some("1.5.7")
    );
}

/// What the server can have done with a request decides what the steer
/// does next: a refusal applied nothing, a request that never reached it
/// applied nothing, one left unanswered may have been applied.
#[test]
fn agy_requests_tell_a_refusal_from_no_answer_and_from_no_server() {
    use std::io::{BufRead, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        for reply in [
            Some(
                "HTTP/1.0 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n\
                 {\"code\":\"unknown\",\"message\":\"cannot send message: there is an ongoing battle\"}",
            ),
            None,
            Some("HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n{}"),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
            let mut length = 0;
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.strip_prefix("Content-Length: ") {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            if let Some(reply) = reply {
                stream.write_all(reply.as_bytes()).unwrap();
            }
        }
    });
    let send = || super::agy_rpc_exchange(port, "csrf", "SendUserCascadeMessage", &json!({}), None);
    match send() {
        Err(super::AgyRpcError::Refused(why)) => assert!(why.contains("ongoing battle"), "{why}"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(send(), Err(super::AgyRpcError::Unanswered(_))));
    assert_eq!(send().unwrap().trim(), "{}");
    server.join().unwrap();
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let closed_port = closed.local_addr().unwrap().port();
    drop(closed);
    assert!(matches!(
        super::agy_rpc_exchange(
            closed_port,
            "csrf",
            "SendUserCascadeMessage",
            &json!({}),
            None
        ),
        Err(super::AgyRpcError::Unreached(_))
    ));
}

/// Re-offer one input the way the delivery loop does until the harness takes it.
#[cfg(unix)]
fn deliver_until_accepted(
    harness: &mut super::StreamHarness,
    id: &str,
) -> anyagents_harness::DeliverOutcome {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let outcome = harness
            .deliver(
                &receipt_message(id),
                &json!({"type":"user","request_id":id}),
            )
            .unwrap();
        if matches!(outcome, anyagents_harness::DeliverOutcome::Accepted { .. })
            || std::time::Instant::now() > deadline
        {
            return outcome;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(unix)]
const CONFIRMED_TURN_SCRIPT: &str = r#"
import json,sys,time
time.sleep(int(sys.argv[1])/1000)
print(json.dumps({'type':'ready','session_id':'fixture-ready'}),flush=True)
first=None
for line in sys.stdin:
 m=json.loads(line)
 if first is None:
  first=m.get('request_id')
  print(json.dumps({'type':'accepted','request_id':first}),flush=True)
  print(json.dumps({'type':'result','request_id':first,'status':'completed'}),flush=True)
"#;

#[test]
#[cfg(unix)]
fn confirmed_turn_completion_clears_the_error_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let last_error = dir.path().join(".anyagents-last-error.json");
    std::fs::write(&last_error, b"{}").unwrap();
    let mut harness = immediate_receipt_fixture(CONFIRMED_TURN_SCRIPT, 0).unwrap();
    harness.set_state_dir(dir.path());

    assert!(matches!(
        deliver_until_accepted(&mut harness, "m1"),
        anyagents_harness::DeliverOutcome::Accepted { .. }
    ));
    assert!(
        harness.wait_pred(std::time::Duration::from_secs(10), |state| state
            .finished
            .contains("m1")),
        "the confirmed result must be processed"
    );
    assert!(
        !last_error.exists(),
        "a confirmed turn must not leave its error file"
    );
}

#[cfg(target_os = "linux")]
fn plain_message(id: &str) -> anyagents_harness::OutboundMessage {
    anyagents_harness::OutboundMessage {
        id: id.into(),
        seq: 1,
        text: format!("text of {id}"),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: anyagents_harness::Context::default(),
    }
}

#[cfg(target_os = "linux")]
/// Counts the activity watcher's calls.
fn counted_watch(harness: &super::StreamHarness) -> std::sync::mpsc::Receiver<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    let tx = std::sync::Mutex::new(tx);
    harness.watch_activity(std::sync::Arc::new(move || {
        let _ = tx.lock().unwrap().send(());
    }));
    rx
}

#[test]
#[cfg(target_os = "linux")]
fn streamed_lines_wake_the_host_only_on_transitions() {
    use std::time::Duration;
    let spec = StreamSpec::for_program("omp");
    let mut harness = super::StreamHarness::launch(super::Launch {
        spec: spec.clone(),
        program: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            r#"read line
i=0
while [ "$i" -lt 200 ]; do
    printf '%s\n' "{\"type\":\"text_delta\",\"text\":\"chunk $i\"}"
    i=$((i + 1))
    case "$i" in *0) sleep 0.01 ;; esac
done
printf '%s\n' '{"type":"result","session_id":"sess-stream","usage":{"input_tokens":1,"output_tokens":1}}'
exec sleep 30"#
                .into(),
        ],
        environment: vec![],
        work_dir: None,
        session_id: None,
        mode: anyagents_harness::DeliveryMode::Followup,
        delivery: crate::LINE_PER_TURN,
    })
    .unwrap();
    let wakes = counted_watch(&harness);
    wakes
        .recv_timeout(Duration::from_secs(5))
        .expect("the watcher wakes the host once at start");
    let message = plain_message("m1");
    assert!(matches!(
        harness
            .deliver(&message, &super::message_line(&spec, &message))
            .unwrap(),
        anyagents_harness::DeliverOutcome::Accepted { .. }
    ));
    assert!(harness.wait_finished("m1", Duration::from_secs(10)));
    // The closing line is the one transition: it ends the turn and names
    // the session. The 200 streamed lines change nothing the host reads.
    wakes
        .recv_timeout(Duration::from_secs(5))
        .expect("the turn end wakes the host");
    std::thread::sleep(Duration::from_millis(200));
    let extra = wakes.try_iter().count();
    assert!(extra <= 1, "{extra} more wakes for streamed lines");
    assert_eq!(
        harness.health().turn_state,
        anyagents_harness::TurnState::Idle
    );
    assert_eq!(harness.session_id().as_deref(), Some("sess-stream"));
}

#[test]
#[cfg(target_os = "linux")]
fn an_idle_agy_cascade_closes_its_turn_on_its_own_timer_without_polling_while_it_streams() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let polls = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&polls);
    std::thread::spawn(move || {
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
                    let head = String::from_utf8_lossy(&data[..pos]).to_ascii_lowercase();
                    let length = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if data.len() >= pos + 4 + length {
                        break;
                    }
                }
            }
            let request = String::from_utf8_lossy(&data);
            let body = if request.contains("GetAllCascadeTrajectories") {
                served.fetch_add(1, Ordering::SeqCst);
                json!({"trajectorySummaries": {"sess-idle": {"status": "CASCADE_RUN_STATUS_IDLE"}}})
                    .to_string()
            } else {
                "{}".to_string()
            };
            let _ = stream.write_all(
                format!(
                    "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let spec = StreamSpec::for_program("agy");
    // The cascade streams steps for about a second and a half, then goes
    // quiet without the closing `result`, as one held open by a background
    // task does.
    let mut harness = super::StreamHarness::launch_with_agy_rpc(
        super::Launch {
            spec: spec.clone(),
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                r#"printf '%s\n' '{"event":"init","session_id":"sess-idle"}'
read line
printf '%s\n' '{"event":"step_update","step_update":{"conversation_id":"sess-idle","step_type":"user_input"}}'
i=0
while [ "$i" -lt 30 ]; do
    sleep 0.05
    printf '%s\n' '{"event":"step_update","step_update":{"conversation_id":"sess-idle","step_type":"planner_response"}}'
    i=$((i + 1))
done
exec sleep 30"#
                    .into(),
            ],
            environment: vec![],
            work_dir: Some(dir.path().to_path_buf()),
            session_id: None,
            mode: anyagents_harness::DeliveryMode::Steer,
            delivery: crate::LINE_INTO_TURN,
        },
        Some(super::AgyRpcConfig {
            config_dir: dir.path().to_path_buf(),
            csrf_token: "csrf-idle".into(),
            port_override: Some(port),
        }),
    )
    .unwrap();
    assert!(harness.wait_pred(Duration::from_secs(5), |state| state.session_id.is_some()));
    let wakes = counted_watch(&harness);
    let message = plain_message("m1");
    let line = super::message_line(&spec, &message);
    assert_eq!(
        harness.deliver(&message, &line).unwrap(),
        anyagents_harness::DeliverOutcome::InFlight
    );
    // While steps stream, the cascade evidently runs: nobody asks.
    assert!(harness.wait_pred(Duration::from_secs(5), |state| state.turn_user_inputs > 0));
    assert_eq!(
        harness.deliver(&message, &line).unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    std::thread::sleep(Duration::from_millis(1_000));
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "no request while it streams"
    );
    // Once quiet, its own timer asks, sees it idle twice, and closes the
    // turn, waking the host: no health read drives it.
    let deadline = Instant::now() + Duration::from_secs(10);
    while harness.wait_pred(Duration::ZERO, |state| !state.open.is_empty()) {
        assert!(
            Instant::now() < deadline,
            "the idle cascade's turn never closed"
        );
        let _ = wakes.recv_timeout(Duration::from_millis(100));
    }
    assert!(harness.wait_finished("m1", Duration::ZERO));
    let asked = polls.load(Ordering::SeqCst);
    assert!(
        (2..=4).contains(&asked),
        "{asked} status requests to close one quiet turn"
    );
    assert_eq!(
        harness.health().turn_state,
        anyagents_harness::TurnState::Idle
    );
}

#[test]
fn the_agy_quota_finding_is_kept_until_a_file_it_reads_changes() {
    use std::io::Write as _;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let dir = tempfile::tempdir().unwrap();
    let logs = dir.path().join(".gemini/antigravity-cli/log");
    std::fs::create_dir_all(&logs).unwrap();
    let log = logs.join("cli-1.log");
    std::fs::write(
        &log,
        "Sending user message to conversation c1\n\
         run ended with error and no response: RESOURCE_EXHAUSTED: quota resets in 2h\n",
    )
    .unwrap();
    let mtime_ms = |path: &std::path::Path| {
        std::fs::metadata(path)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let (reset, error) = super::detect_agy_session_quota_and_error(dir.path(), now);
    let reset = reset.expect("a quota reset in the newest log");
    assert_eq!(reset, mtime_ms(&log) + 7_200_000);
    assert!(error.unwrap().contains("RESOURCE_EXHAUSTED"));
    // The kept finding still applies the current time.
    assert_eq!(
        super::detect_agy_session_quota_and_error(dir.path(), reset + 1),
        (None, None)
    );
    // A clearing newer than the log hides it.
    std::fs::write(
        dir.path().join(".anyagents-quota-cleared-ms"),
        format!("{}\n", mtime_ms(&log) + 1),
    )
    .unwrap();
    assert_eq!(
        super::detect_agy_session_quota_and_error(dir.path(), now),
        (None, None)
    );
    // The same log, appended to after the clearing, counts again.
    std::thread::sleep(Duration::from_millis(20));
    std::fs::OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(b"run ended with error and no response: overloaded\n")
        .unwrap();
    assert_eq!(
        super::detect_agy_session_quota_and_error(dir.path(), now),
        (None, Some("overloaded".to_string()))
    );
    // A persisted error newer than the clearing takes precedence.
    std::fs::write(
        dir.path().join(".anyagents-last-error.json"),
        json!({"error": "persisted", "timestamp_ms": i64::MAX - 1}).to_string(),
    )
    .unwrap();
    assert_eq!(
        super::detect_agy_session_quota_and_error(dir.path(), now),
        (None, Some("persisted".to_string()))
    );
}

#[test]
fn quota_of_parses_rfc3339_resets_at_timestamp() {
    let ev = serde_json::json!({
        "type": "result",
        "is_error": true,
        "error": "Sylphx AI failure Some(\"rate_limit_exceeded\"): Subscription quota exhausted. Your usage window resets at 2026-11-16T00:00:00Z."
    });
    assert_eq!(super::quota_of(&ev), Some(1_794_787_200_000));

    let offset_ev = serde_json::json!({
        "type": "result",
        "is_error": true,
        "error": "Quota exhausted; resets at 2026-11-15T19:00:00.250-05:00"
    });
    assert_eq!(super::quota_of(&offset_ev), Some(1_794_787_200_250));
}

/// A language server for one cascade, as `agy` 1.5.8 answers the requests
/// the adapter makes: `GetAllCascadeTrajectories` with the cascade's run
/// status, its planner config, and `SendUserCascadeMessage`, which writes the
/// message to the cascade's durable mailbox (`messages/<mail>.json`, its text
/// in a base64 `CortexStep`, and an `undelivered/<mail>` marker) and answers
/// `{}` whether or not the cascade ever takes it.
#[cfg(unix)]
fn agy_mailbox_server(
    config: std::path::PathBuf,
    cascade: &'static str,
) -> (
    u16,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::io::{BufRead as _, Read as _, Write as _};
    use std::sync::atomic::Ordering;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (sent_srv, running_srv) = (sent.clone(), running.clone());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
            let (mut head, mut length) = (String::new(), 0);
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
                if let Some(value) = line.strip_prefix("Content-Length: ") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                head.push_str(&line);
            }
            let mut body = vec![0; length];
            let _ = reader.read_exact(&mut body);
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            let answer = if head.contains("GetAllCascadeTrajectories") {
                let status = if running_srv.load(Ordering::SeqCst) {
                    "CASCADE_RUN_STATUS_RUNNING"
                } else {
                    "CASCADE_RUN_STATUS_IDLE"
                };
                json!({"trajectorySummaries": {cascade: {"status": status}}})
            } else if head.contains("GetCascadeTrajectoryGeneratorMetadata") {
                json!({"generatorMetadata": [{"plannerConfig": {"planModel": 1}}]})
            } else if head.contains("SendUserCascadeMessage") {
                let text = request["items"][0]["text"].as_str().unwrap().to_string();
                let messages = config
                    .join(".gemini/antigravity-cli/brain")
                    .join(cascade)
                    .join(".system_generated/messages");
                std::fs::create_dir_all(messages.join("undelivered")).unwrap();
                let mail = format!("mail-{}", sent_srv.lock().unwrap().len());
                let mut step = vec![0x08, 0x0e, 0x9a, 0x01];
                let user_input = [&[0x12, text.len() as u8][..], text.as_bytes()].concat();
                step.push(user_input.len() as u8);
                step.extend(user_input);
                std::fs::write(
                    messages.join(format!("{mail}.json")),
                    json!({"id": mail, "recipient": cascade, "sender": "user",
                           "timestamp": "2026-11-13T17:00:00Z",
                           "stepPayload": base64(&step)})
                    .to_string(),
                )
                .unwrap();
                std::fs::write(messages.join("undelivered").join(&mail), "").unwrap();
                sent_srv.lock().unwrap().push(text);
                json!({})
            } else {
                json!({})
            };
            let answer = answer.to_string();
            let mut stream = stream;
            let _ = write!(
                stream,
                "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{answer}",
                answer.len()
            );
        }
    });
    (port, sent, running)
}

#[cfg(unix)]
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let word = chunk.iter().enumerate().fold(0u32, |word, (at, byte)| {
            word | (u32::from(*byte) << (16 - 8 * at))
        });
        for at in 0..4 {
            if at <= chunk.len() {
                out.push(ALPHABET[((word >> (18 - 6 * at)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// An `agy` child on `cascade` whose language server is `port`: it opens a
/// turn with its first line, shows its first step, and then prints each line
/// it reads after that verbatim, so a test speaks for the cascade.
#[cfg(unix)]
fn agy_child(config: &std::path::Path, port: u16, session: Option<&str>) -> super::StreamHarness {
    let script = r#"
import json, sys
print(json.dumps({"event":"init","session_id":"conv-err"}), flush=True)
first = sys.stdin.readline()
print(json.dumps({"event":"step_update","step_update":{"conversation_id":"conv-err","step_index":0,"state":"DONE","step_type":"user_input"}}), flush=True)
for line in sys.stdin:
    message = json.loads(line)
    if "say" in message:
        print(json.dumps(message["say"]), flush=True)
"#;
    super::StreamHarness::launch_with_agy_rpc(
        super::Launch {
            spec: StreamSpec::for_program("agy"),
            program: "/usr/bin/python3".into(),
            args: vec!["-u".into(), "-c".into(), script.into()],
            environment: vec![],
            work_dir: Some(config.to_path_buf()),
            session_id: session.map(str::to_string),
            mode: anyagents_harness::DeliveryMode::Steer,
            delivery: crate::LINE_INTO_TURN,
        },
        Some(super::AgyRpcConfig {
            config_dir: config.to_path_buf(),
            csrf_token: "csrf".into(),
            port_override: Some(port),
        }),
    )
    .unwrap()
}

#[cfg(unix)]
fn agy_message(id: &str) -> anyagents_harness::OutboundMessage {
    anyagents_harness::OutboundMessage {
        id: id.into(),
        seq: 1,
        text: format!("body of {id}"),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Default::default(),
    }
}

/// The cascade's run fails while a steer waits in its mailbox. The language
/// server answered `{}`, and the cascade never took it: nothing is committed
/// and nothing is lost. A process that resumes the cascade finds the mail
/// still held there, does not write it again, and commits it once the
/// cascade's record shows it took it.
#[test]
#[cfg(unix)]
fn an_agy_cascade_error_with_queued_mail_commits_nothing_and_loses_nothing() {
    use anyagents_harness::DeliverOutcome::{Accepted, InFlight};
    use std::time::{Duration, Instant};
    let config = tempfile::tempdir().unwrap();
    let (port, sent, _) = agy_mailbox_server(config.path().to_path_buf(), "conv-err");
    let mut harness = agy_child(config.path(), port, None);
    let spec = StreamSpec::for_program("agy");
    let (m1, m2) = (agy_message("m1"), agy_message("m2"));
    let (line1, line2) = (
        super::message_line(&spec, &m1),
        super::message_line(&spec, &m2),
    );
    assert_eq!(harness.deliver(&m1, &line1).unwrap(), InFlight);
    let deadline = Instant::now() + Duration::from_secs(10);
    while harness.deliver(&m1, &line1).unwrap() == InFlight {
        assert!(Instant::now() < deadline, "the turn never started");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(settled(&mut harness, &m2, &line2), InFlight);
    assert_eq!(sent.lock().unwrap().len(), 1);
    // The run fails; the mail waits in the mailbox, untaken.
    super::write_stdin(
        &harness.stdin,
        &json!({"say": {"event": "result", "result": {"conversation_id": "conv-err",
            "status": "ERROR", "error": "API error (attempt 3): stream idle timeout"}}}),
    )
    .unwrap();
    assert!(harness.wait_finished("m1", Duration::from_secs(5)));
    let quiet_until = Instant::now() + Duration::from_millis(1_500);
    while Instant::now() < quiet_until {
        assert_eq!(harness.deliver(&m2, &line2).unwrap(), InFlight);
        assert_eq!(harness.proven("m2"), None);
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(harness.health().process_alive);
    harness.stop();
    assert_eq!(harness.proven("m2"), None);
    assert_eq!(
        harness.deliver(&m2, &line2),
        Err(anyagents_harness::HarnessError::Dead)
    );
    drop(harness);

    // The next process resumes the cascade: the mail is the cascade's to
    // take, so it is not written again, on stdin or to the language server.
    let mut next = agy_child(config.path(), port, Some("conv-err"));
    assert_eq!(next.deliver(&m2, &line2).unwrap(), InFlight);
    assert!(next.written_ids().is_empty());
    assert_eq!(sent.lock().unwrap().len(), 1);
    take_in_transcript(config.path(), "conv-err", "m2");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match next.deliver(&m2, &line2).unwrap() {
            InFlight => {}
            outcome => {
                assert_eq!(
                    outcome,
                    Accepted {
                        turn_finished: false
                    }
                );
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the cascade's record was never read"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(next.written_ids().is_empty());
    assert_eq!(sent.lock().unwrap().len(), 1);
    // What the cascade's transcript held before is answered as applied.
    let mut later = agy_child(config.path(), port, Some("conv-err"));
    drop(next);
    assert_eq!(
        later.deliver(&m2, &line2).unwrap(),
        anyagents_harness::DeliverOutcome::Duplicate
    );
    assert_eq!(
        later.proven("m2"),
        Some(anyagents_harness::Proof::Duplicate)
    );
}

/// What the cascade's record showed when its process stopped is kept: the
/// stop reads the child's stdout to its end and looks at the record once
/// more, and `proven` answers from that after the child is gone.
#[test]
#[cfg(unix)]
fn an_agy_proof_written_just_before_the_stop_is_kept() {
    use anyagents_harness::DeliverOutcome::InFlight;
    use std::time::{Duration, Instant};
    let config = tempfile::tempdir().unwrap();
    let (port, _, _) = agy_mailbox_server(config.path().to_path_buf(), "conv-err");
    let mut harness = agy_child(config.path(), port, None);
    let spec = StreamSpec::for_program("agy");
    let (m1, m2) = (agy_message("m1"), agy_message("m2"));
    let (line1, line2) = (
        super::message_line(&spec, &m1),
        super::message_line(&spec, &m2),
    );
    harness.deliver(&m1, &line1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while harness.deliver(&m1, &line1).unwrap() == InFlight {
        assert!(Instant::now() < deadline, "the turn never started");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(settled(&mut harness, &m2, &line2), InFlight);
    take_in_transcript(config.path(), "conv-err", "m2");
    harness.stop();
    assert!(!harness.health().process_alive);
    assert_eq!(
        harness.proven("m2"),
        Some(anyagents_harness::Proof::Taken {
            turn_finished: false
        })
    );
    assert_eq!(
        harness.proven("m1"),
        Some(anyagents_harness::Proof::Taken {
            turn_finished: false
        })
    );
}

/// A proof the peer wrote just before the host stops it is read before the
/// stop returns, and `proven` answers it afterwards, as `deliver` does.
#[test]
#[cfg(unix)]
fn a_proof_written_just_before_the_stop_is_answered_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let written = dir.path().join("proved");
    let script = r#"
import json, pathlib, sys, time
print(json.dumps({"type":"ready","protocol":"spiron.headless/v1","capabilities":["steer_consumed","idempotent_input"]}), flush=True)
for line in sys.stdin:
    message = json.loads(line)
    print(json.dumps({"type":"accepted","mode":"turn","request_id":message["request_id"]}), flush=True)
    pathlib.Path(sys.argv[1]).write_text("")
    time.sleep(60)
"#;
    let mut harness = super::StreamHarness::launch(super::Launch {
        spec: StreamSpec {
            name: "stop-spiron".into(),
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
            written.display().to_string(),
        ],
        environment: vec![],
        work_dir: None,
        session_id: None,
        mode: anyagents_harness::DeliveryMode::Steer,
        delivery: crate::SPIRON,
    })
    .unwrap();
    harness
        .wait_ready(std::time::Duration::from_secs(15))
        .unwrap();
    let message = receipt_message("m1");
    assert_eq!(
        harness
            .deliver(&message, &json!({"type":"user","request_id":"m1"}))
            .unwrap(),
        anyagents_harness::DeliverOutcome::InFlight
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !written.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the peer never proved it"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    // The proof is in the pipe; whether the reader has read it yet or not,
    // the stop reads it before it returns.
    harness.stop();
    assert!(!harness.health().process_alive);
    assert_eq!(
        harness.proven("m1"),
        Some(anyagents_harness::Proof::Taken {
            turn_finished: false
        })
    );
    assert_eq!(
        harness
            .deliver(&message, &json!({"type":"user","request_id":"m1"}))
            .unwrap(),
        anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
}
