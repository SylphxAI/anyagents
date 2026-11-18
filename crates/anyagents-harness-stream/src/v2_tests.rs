//! A `spiron.headless/v2` connection over a Unix socket: the replay after
//! `attach` rebuilds what an earlier connection knew, and nothing it wrote is
//! written again. The peer here plays recorded frames; the real binary is
//! driven by `anyagents-harness-spiron/tests/real_spiron_v2.rs` and Spiron's
//! own conformance suite.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyagents_harness::{Context, DeliverOutcome, OutboundMessage, Proof, TurnState};
use serde_json::{Value, json};

use super::{Connect, LineProtocol, SPIRON, StreamHarness, StreamSpec};

fn spec() -> StreamSpec {
    StreamSpec {
        name: "v2-spiron".into(),
        args: vec![],
        resume_flag: "--session",
        start_line: "user",
        cancel: "cancel",
        protocol: LineProtocol::AcknowledgedResult,
    }
}

fn message(id: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.into(),
        seq: 1,
        text: "text".into(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    }
}

fn frame(seq: u64, request_id: &str, body: Value) -> String {
    let mut frame = body;
    frame["seq"] = json!(seq);
    frame["session_id"] = json!("s-1");
    frame["request_id"] = json!(request_id);
    format!("{frame}\n")
}

fn until(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn a_replay_rebuilds_the_open_turn_and_its_proofs_and_nothing_is_written_twice() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let (lines_tx, lines) = mpsc::channel::<Value>();
    let (finish_tx, finish) = mpsc::channel::<()>();
    let peer = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut out = stream;
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let attach: Value = serde_json::from_str(&line).unwrap();
        let attach_id = attach["request_id"].as_str().unwrap().to_string();
        lines_tx.send(attach).unwrap();
        // The replay: the launch's own ready, a turn an earlier connection
        // opened, a steer it acknowledged, then this connection's ready.
        let ready = json!({"type": "ready", "protocol": "spiron.headless/v2", "version": "0.1.0",
            "commit": "0123456789abcdef", "model": "sylphx/m", "context": {"window_tokens": 1000, "window_source": "catalogue"}});
        for chunk in [
            frame(5, "attach-old", ready.clone()),
            frame(6, "m1", json!({"type": "accepted", "mode": "turn"})),
            frame(7, "m1", json!({"type": "text_delta", "text": "working"})),
            frame(
                8,
                "m2",
                json!({"type": "steer_ack", "accepted": true, "turn_id": "m1"}),
            ),
            frame(9, &attach_id, ready),
        ] {
            out.write_all(chunk.as_bytes()).unwrap();
        }
        // Whatever the client writes next would be a duplicate of the replay.
        finish.recv().unwrap();
        for chunk in [
            frame(
                10,
                "m1",
                json!({"type": "steer_consumed", "steer_request_id": "m2", "turn_id": "m1", "duplicate": false}),
            ),
            frame(
                11,
                "m1",
                json!({"type": "result", "status": "completed", "is_error": false, "error": null,
                "duplicate": false, "usage": {"input_tokens": 11, "output_tokens": 2, "cached_tokens": 0, "cache_write_tokens": null},
                "cost_micros": 78, "context": null, "undelivered_steer_ids": []}),
            ),
        ] {
            out.write_all(chunk.as_bytes()).unwrap();
        }
        // Anything the client wrote after the attach, until it disconnects.
        let mut rest = Vec::new();
        let mut line = String::new();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            rest.push(serde_json::from_str::<Value>(&line).unwrap());
            line.clear();
        }
        rest
    });

    let mut harness = StreamHarness::connect(Connect {
        spec: spec(),
        program: "spiron".into(),
        socket: path.clone(),
        pid: Some(4242),
        after_seq: Some(4),
        session_id: Some("s-1".into()),
        environment: Vec::new(),
        delivery: SPIRON,
    })
    .unwrap();
    let attach = lines.recv().unwrap();
    assert_eq!(attach["type"], "attach");
    assert_eq!(attach["protocol"], "spiron.headless/v2");
    assert_eq!(attach["after_seq"], 4);
    harness.wait_ready(Duration::from_secs(10)).unwrap();
    assert_eq!(harness.attached_seq(), Some(9));
    assert_eq!(harness.process_id(), Some(4242));
    let health = harness.health();
    assert_eq!(health.turn_state, TurnState::InTurn);
    assert!(health.delivery.peer.as_deref().is_some_and(|peer| peer
        .contains("spiron.headless/v2; spiron 0.1.0+01234567; model sylphx/m; window 1000 tokens (catalogue)")));

    // What the earlier connection wrote is answered from the replay.
    assert_eq!(
        harness.proven("m1"),
        Some(Proof::Taken {
            turn_finished: false
        })
    );
    assert_eq!(
        harness
            .deliver(&message("m1"), &json!({"type": "user"}))
            .unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert_eq!(
        harness
            .deliver(&message("m2"), &json!({"type": "user"}))
            .unwrap(),
        DeliverOutcome::InFlight
    );
    assert!(harness.written_ids().is_empty());

    finish_tx.send(()).unwrap();
    until(|| harness.health().turn_state == TurnState::Idle);
    assert_eq!(
        harness.proven("m2"),
        Some(Proof::Taken {
            turn_finished: true
        })
    );
    assert_eq!(harness.turns_served(), 2);
    let usage = harness.served_usage();
    assert_eq!((usage.input_tokens, usage.output_tokens), (11, 2));
    assert_eq!(usage.reported_cost_micros, Some(78));

    // A new input is a v2 `prompt` with its request id.
    let next = harness
        .deliver(&message("m3"), &json!({"type": "user", "text": "t"}))
        .unwrap();
    assert_eq!(next, DeliverOutcome::InFlight);
    harness.stop();
    let written = peer.join().unwrap();
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!(written[0]["type"], "prompt");
    assert_eq!(written[0]["request_id"], "m3");
    assert!(written[0].get("id").is_none());
}

#[test]
fn bound_terminal_replay_rebuilds_members_without_accepted_and_context_stays_on_openers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let peer = std::thread::spawn(move || {
        let (mut out, _) = listener.accept().unwrap();
        out.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut reader = BufReader::new(out.try_clone().unwrap());
        let mut read = || {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            serde_json::from_str::<Value>(&line).unwrap()
        };
        let attach = read();
        assert_eq!(attach["after_seq"], 21);
        for chunk in [
            frame(
                22,
                "past-duplicate",
                json!({"type":"result", "status":"completed",
                "duplicate":true, "tool_context_bound":false, "final_text":"", "consumed_request_ids":[]}),
            ),
            frame(
                23,
                "past-no-error-field",
                json!({"type":"result", "status":"completed", "is_error":false,
                "duplicate":false, "tool_context_bound":true, "final_text":"incomplete envelope",
                "consumed_request_ids":["past-no-error-field"], "undelivered_steer_ids":[]}),
            ),
            frame(
                24,
                "past-error",
                json!({"type":"result", "status":"completed", "is_error":false, "error":"provider failure",
                "duplicate":false, "tool_context_bound":true, "final_text":"contradictory envelope",
                "consumed_request_ids":["past-error"], "undelivered_steer_ids":[]}),
            ),
            frame(
                25,
                "past-no-text",
                json!({"type":"result", "status":"completed", "is_error":false, "error":null,
                "duplicate":false, "tool_context_bound":true,
                "consumed_request_ids":["past-no-text"], "undelivered_steer_ids":[]}),
            ),
            frame(
                26,
                "past-opener",
                json!({"type":"result", "status":"completed", "is_error":false, "error":null,
                "duplicate":false, "tool_context_bound":true, "final_text":"replayed answer",
                "consumed_request_ids":["past-opener","past-steer"], "undelivered_steer_ids":["untaken"]}),
            ),
            frame(
                27,
                attach["request_id"].as_str().unwrap(),
                json!({"type":"ready", "protocol":"spiron.headless/v2",
                "features":["turn-tool-context-v1"]}),
            ),
        ] {
            out.write_all(chunk.as_bytes()).unwrap();
        }
        let prompt = read();
        assert_eq!(prompt["type"], "prompt");
        assert_eq!(prompt["tool_context"], "opener-scope");
        out.write_all(frame(28, "live", json!({"type":"accepted", "mode":"turn"})).as_bytes())
            .unwrap();
        let steer = read();
        assert_eq!(steer["type"], "steer");
        assert!(steer.get("tool_context").is_none());
        out.write_all(
            frame(
                29,
                "live",
                json!({"type":"result", "status":"completed", "is_error":false, "error":null,
            "duplicate":false, "tool_context_bound":false, "final_text":"unscoped answer",
            "consumed_request_ids":["live"], "undelivered_steer_ids":["later"]}),
            )
            .as_bytes(),
        )
        .unwrap();
        let mut line = String::new();
        assert_eq!(reader.read_line(&mut line).unwrap(), 0, "{line}");
    });
    let mut harness = StreamHarness::connect(Connect {
        spec: spec(),
        program: "spiron".into(),
        socket: path,
        pid: Some(4242),
        after_seq: Some(21),
        session_id: Some("s-1".into()),
        environment: Vec::new(),
        delivery: SPIRON,
    })
    .unwrap();
    harness.wait_ready(Duration::from_secs(10)).unwrap();
    assert!(harness.supports_turn_tool_context());
    assert_eq!(
        harness.proven("past-opener"),
        Some(Proof::Taken {
            turn_finished: true
        })
    );
    assert_eq!(
        harness.proven("past-steer"),
        Some(Proof::Taken {
            turn_finished: true
        })
    );
    assert_eq!(
        harness.task_turn_result("past-steer"),
        Some(("past-opener".into(), "replayed answer".into()))
    );
    for missing in [
        "untaken",
        "past-duplicate",
        "past-no-error-field",
        "past-error",
        "past-no-text",
    ] {
        assert!(harness.proven(missing).is_none());
        assert!(harness.task_turn_result(missing).is_none());
    }
    assert!(harness.written_ids().is_empty());
    harness
        .deliver(
            &message("live"),
            &json!({"type":"user", "text":"run", "tool_context":"opener-scope"}),
        )
        .unwrap();
    until(|| harness.proven("live").is_some());
    harness
        .deliver(
            &message("later"),
            &json!({"type":"user", "text":"steer", "tool_context":"not-the-opener"}),
        )
        .unwrap();
    until(|| {
        harness.proven("live")
            == Some(Proof::Taken {
                turn_finished: true,
            })
    });
    assert!(harness.task_turn_result("live").is_none());
    assert!(harness.task_turn_result("later").is_none());
    harness.stop();
    peer.join().unwrap();
}

#[test]
fn replayed_ready_does_not_enable_context_before_or_without_current_negotiation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let (allow_ready, wait_for_assertions) = mpsc::channel();
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let attach: Value = serde_json::from_str(&line).unwrap();
        for chunk in [
            frame(
                2,
                "old-attach",
                json!({"type":"ready", "protocol":"spiron.headless/v2",
                "features":["turn-tool-context-v1"]}),
            ),
            frame(
                3,
                "past",
                json!({"type":"result", "status":"completed", "is_error":false,
                "error":null, "duplicate":false, "tool_context_bound":true,
                "final_text":"historical answer", "consumed_request_ids":["past"]}),
            ),
        ] {
            stream.write_all(chunk.as_bytes()).unwrap();
        }
        wait_for_assertions
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        stream
            .write_all(
                frame(
                    4,
                    attach["request_id"].as_str().unwrap(),
                    json!({"type":"ready", "protocol":"spiron.headless/v2"}),
                )
                .as_bytes(),
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        let prompt: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(prompt["type"], "prompt");
        assert!(prompt.get("tool_context").is_none());
    });
    let mut harness = StreamHarness::connect(Connect {
        spec: spec(),
        program: "spiron".into(),
        socket: path,
        pid: None,
        after_seq: Some(1),
        session_id: Some("s-1".into()),
        environment: Vec::new(),
        delivery: SPIRON,
    })
    .unwrap();
    until(|| {
        harness.proven("past")
            == Some(Proof::Taken {
                turn_finished: true,
            })
    });
    assert!(!harness.supports_turn_tool_context());
    assert!(harness.task_turn_result("past").is_none());
    allow_ready.send(()).unwrap();
    harness.wait_ready(Duration::from_secs(10)).unwrap();
    assert!(!harness.supports_turn_tool_context());
    assert!(harness.task_turn_result("past").is_none());
    harness
        .deliver(
            &message("live"),
            &json!({"type":"user", "text":"run", "tool_context":"opener-scope"}),
        )
        .unwrap();
    peer.join().unwrap();
    harness.stop();
}

#[test]
fn a_replayed_steer_ack_marks_its_turn_in_flight_before_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let peer = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let attach: Value = serde_json::from_str(&line).unwrap();
        let attach_id = attach["request_id"].as_str().unwrap().to_string();
        let mut out = stream;
        let ready = json!({"type": "ready", "protocol": "spiron.headless/v2", "version": "0.1.0",
            "commit": "0123456789abcdef", "model": "sylphx/m", "context": {"window_tokens": 1000, "window_source": "catalogue"}});
        for chunk in [
            frame(
                1,
                "m2",
                json!({"type": "steer_ack", "accepted": true, "turn_id": "m1"}),
            ),
            frame(2, &attach_id, ready),
        ] {
            out.write_all(chunk.as_bytes()).unwrap();
        }
        let mut rest = Vec::new();
        line.clear();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            rest.push(serde_json::from_str::<Value>(&line).unwrap());
            line.clear();
        }
        rest
    });

    let mut harness = StreamHarness::connect(Connect {
        spec: spec(),
        program: "spiron".into(),
        socket: path,
        pid: Some(4242),
        after_seq: Some(0),
        session_id: Some("s-1".into()),
        environment: Vec::new(),
        delivery: SPIRON,
    })
    .unwrap();
    harness.wait_ready(Duration::from_secs(10)).unwrap();
    assert_eq!(
        harness
            .deliver(&message("m1"), &json!({"type": "prompt"}))
            .unwrap(),
        DeliverOutcome::InFlight
    );
    assert_eq!(
        harness
            .deliver(&message("m2"), &json!({"type": "steer"}))
            .unwrap(),
        DeliverOutcome::InFlight
    );
    assert!(harness.written_ids().is_empty());
    harness.stop();
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn a_replayed_quota_holds_nothing_and_a_live_one_does() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let (live_tx, live) = mpsc::channel::<()>();
    let peer = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let attach: Value = serde_json::from_str(&line).unwrap();
        let attach_id = attach["request_id"].as_str().unwrap().to_string();
        let mut out = stream;
        // An earlier turn that hit a quota hours ago, in the replay.
        let limited = json!({"type": "result", "status": "failed", "is_error": true,
            "error": "usage limit reached, resets in 5h", "duplicate": false, "usage": null,
            "cost_micros": null, "context": null, "undelivered_steer_ids": []});
        for chunk in [
            frame(3, "old", json!({"type": "accepted", "mode": "turn"})),
            frame(4, "old", limited.clone()),
            frame(
                5,
                &attach_id,
                json!({"type": "ready", "protocol": "spiron.headless/v2"}),
            ),
        ] {
            out.write_all(chunk.as_bytes()).unwrap();
        }
        live.recv().unwrap();
        // The same report from the live process holds the agent.
        for chunk in [
            frame(6, "new", json!({"type": "accepted", "mode": "turn"})),
            frame(7, "new", limited),
        ] {
            out.write_all(chunk.as_bytes()).unwrap();
        }
        let mut rest = String::new();
        while reader.read_line(&mut rest).is_ok_and(|n| n > 0) {}
    });
    let mut harness = StreamHarness::connect(Connect {
        spec: spec(),
        program: "spiron".into(),
        socket: path,
        pid: None,
        after_seq: Some(2),
        session_id: None,
        environment: Vec::new(),
        delivery: SPIRON,
    })
    .unwrap();
    harness.wait_ready(Duration::from_secs(10)).unwrap();
    assert_eq!(harness.health().quota_reset_at_ms, None);
    live_tx.send(()).unwrap();
    until(|| harness.health().quota_reset_at_ms.is_some());
    harness.stop();
    peer.join().unwrap();
}

#[test]
fn a_refused_attach_fails_the_wait() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let peer = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let attach: Value = serde_json::from_str(&line).unwrap();
        let mut out = stream;
        out.write_all(
            frame(
                1,
                attach["request_id"].as_str().unwrap(),
                json!({"type": "rejected", "code": "unsupported_protocol"}),
            )
            .as_bytes(),
        )
        .unwrap();
    });
    let harness = StreamHarness::connect(Connect {
        spec: spec(),
        program: "spiron".into(),
        socket: path,
        pid: None,
        after_seq: None,
        session_id: None,
        environment: Vec::new(),
        delivery: SPIRON,
    })
    .unwrap();
    let error = harness.wait_ready(Duration::from_secs(10)).unwrap_err();
    assert!(
        error.to_string().contains("unsupported_protocol"),
        "{error}"
    );
    peer.join().unwrap();
}

#[test]
fn an_old_or_unnamed_ready_cannot_complete_a_v2_attach() {
    for protocol in [Some("spiron.headless/v1"), None] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let peer = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let attach: Value = serde_json::from_str(&line).unwrap();
            // Even naming both old capabilities cannot admit the old protocol.
            let ready = json!({"type":"ready", "protocol":protocol,
                "capabilities":["steer_consumed","idempotent_input"]});
            stream
                .write_all(frame(1, attach["request_id"].as_str().unwrap(), ready).as_bytes())
                .unwrap();
            line.clear();
            assert_eq!(
                reader.read_line(&mut line).unwrap(),
                0,
                "no input after refusal"
            );
        });
        let mut harness = StreamHarness::connect(Connect {
            spec: spec(),
            program: "spiron".into(),
            socket: path,
            pid: None,
            after_seq: None,
            session_id: None,
            environment: Vec::new(),
            delivery: SPIRON,
        })
        .unwrap();
        let error = harness.wait_ready(Duration::from_secs(10)).unwrap_err();
        assert!(
            error.to_string().contains("unsupported_protocol"),
            "{error}"
        );
        assert!(!harness.input_ready());
        assert!(harness.written_ids().is_empty());
        harness.stop();
        peer.join().unwrap();
    }
}
