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
