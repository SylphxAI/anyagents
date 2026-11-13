//! A peer that proves consumption and deduplicates input, driven through a
//! real pipe by the fake Spiron in `tests/fake_spiron.py`: every input is
//! written at once, and only a consumption proof answers `Accepted`.
#![cfg(unix)]

use std::path::Path;
use std::time::{Duration, Instant};

use anyagents_harness::DeliverOutcome::{self, Accepted, Deferred, Duplicate, InFlight};
use serde_json::{Value, json};

use super::{Launch, LineProtocol, StreamHarness, StreamSpec};

const FAKE_SPIRON: &str = include_str!("../tests/fake_spiron.py");

fn fake_spiron(dir: &Path, capable: bool) -> StreamHarness {
    let harness = StreamHarness::launch(Launch {
        spec: StreamSpec {
            name: "fake-spiron".into(),
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
            FAKE_SPIRON.into(),
            dir.display().to_string(),
        ],
        environment: vec![(
            "FAKE_SPIRON_CAPABLE".into(),
            if capable { "1" } else { "0" }.into(),
        )],
        work_dir: None,
        session_id: None,
        mode: anyagents_harness::DeliveryMode::Steer,
        steer: anyagents_harness::SteerSupport::Yes,
    })
    .unwrap();
    harness.wait_ready(Duration::from_secs(15)).unwrap();
    harness
}

fn message(id: &str) -> anyagents_harness::OutboundMessage {
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

fn deliver(harness: &mut StreamHarness, id: &str) -> DeliverOutcome {
    harness
        .deliver(
            &message(id),
            &json!({"type": "user", "request_id": id, "text": format!("body of {id}")}),
        )
        .unwrap()
}

#[track_caller]
fn eventually(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !done() {
        assert!(Instant::now() < deadline, "fake Spiron did not get there");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Offer `id` the way the delivery loop does until the answer is `want`.
#[track_caller]
fn until(harness: &mut StreamHarness, id: &str, want: DeliverOutcome) {
    eventually(|| deliver(harness, id) == want);
}

/// Ask the fake to act on `name`, and wait until it has.
#[track_caller]
fn act(dir: &Path, name: &str) {
    std::fs::write(dir.join(name), "").unwrap();
    eventually(|| !dir.join(name).exists());
}

/// `(launch, type, request id)` of every line the fake read.
fn wire(dir: &Path) -> Vec<(u64, String, String)> {
    lines(&dir.join("wire.jsonl"))
        .into_iter()
        .map(|line| {
            (
                line["launch"].as_u64().unwrap(),
                line["msg"]["type"].as_str().unwrap().to_string(),
                line["msg"]["request_id"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect()
}

fn lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn journaled(dir: &Path) -> Vec<String> {
    lines(&dir.join("journal.jsonl"))
        .into_iter()
        .map(|line| line["key"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn nothing_is_written_before_the_peer_says_what_it_can_do() {
    let dir = tempfile::tempdir().unwrap();
    // A peer that has not printed `ready` yet.
    let mut harness = StreamHarness::launch(Launch {
        spec: StreamSpec {
            name: "silent-spiron".into(),
            args: vec![],
            resume_flag: "--session",
            start_line: "user",
            cancel: "cancel",
            protocol: LineProtocol::AcknowledgedResult,
        },
        program: "/usr/bin/python3".into(),
        args: vec![
            "-c".into(),
            "import sys\nfor line in sys.stdin: pass".into(),
        ],
        environment: vec![],
        work_dir: None,
        session_id: None,
        mode: anyagents_harness::DeliveryMode::Steer,
        steer: anyagents_harness::SteerSupport::Yes,
    })
    .unwrap();
    assert_eq!(deliver(&mut harness, "m1"), Deferred);
    assert!(harness.written_ids().is_empty());
    assert!(!harness.input_ready());
    let error = harness.wait_ready(Duration::from_millis(50)).unwrap_err();
    assert!(error.to_string().contains("no ready frame"), "{error}");
    drop(harness);
    // An older peer announces no capabilities and keeps the older rules.
    let old = fake_spiron(dir.path(), false);
    assert!(!old.proves_consumption());
    let capable = fake_spiron(dir.path(), true);
    assert!(capable.proves_consumption());
}

#[test]
fn every_input_is_written_at_once_and_answers_accepted_only_on_its_proof() {
    let dir = tempfile::tempdir().unwrap();
    let mut harness = fake_spiron(dir.path(), true);
    harness.set_state_dir(dir.path());
    until(
        &mut harness,
        "m1",
        Accepted {
            turn_finished: false,
        },
    );
    let steers = ["m2", "m3", "m4", "m5", "m6"];
    for id in steers {
        assert_eq!(deliver(&mut harness, id), InFlight, "{id}");
    }
    eventually(|| wire(dir.path()).len() == 6);
    let written = wire(dir.path());
    assert_eq!(written[0], (1, "prompt".into(), "m1".into()));
    for (line, id) in written[1..].iter().zip(steers) {
        assert_eq!(line, &(1, "steer".to_string(), id.to_string()));
    }
    // Acknowledged on the socket is not consumed.
    eventually(|| super::lock(&harness.shared).acknowledged.len() == 6);
    for id in steers {
        assert_eq!(deliver(&mut harness, id), InFlight, "{id}");
    }
    assert!(!dir.path().join("steps.jsonl").exists());
    act(dir.path(), "step");
    // One step took the whole inbox, in order.
    let steps = lines(&dir.path().join("steps.jsonl"));
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0]["consumed"], json!(steers));
    for id in steers {
        until(
            &mut harness,
            id,
            Accepted {
                turn_finished: false,
            },
        );
    }
    act(dir.path(), "finish");
    until(
        &mut harness,
        "m6",
        Accepted {
            turn_finished: true,
        },
    );
    assert_eq!(harness.turns_served(), 6);
    assert_eq!(
        harness.health().turn_state,
        anyagents_harness::TurnState::Idle
    );
    assert_eq!(wire(dir.path()).len(), 6, "nothing was written twice");
    assert!(
        !dir.path().join("spiron-unconfirmed-steers.json").exists(),
        "a peer that proves consumption needs no hold"
    );
}

#[test]
fn a_steer_refused_by_a_closing_turn_starts_the_next_turn() {
    let dir = tempfile::tempdir().unwrap();
    let mut harness = fake_spiron(dir.path(), true);
    until(
        &mut harness,
        "m1",
        Accepted {
            turn_finished: false,
        },
    );
    act(dir.path(), "seal");
    assert!(matches!(deliver(&mut harness, "m2"), InFlight | Deferred));
    // Refused as closed (perhaps before that call returned): back in the
    // mailbox, and nothing more is steered into the closing turn.
    until(&mut harness, "m2", Deferred);
    assert_eq!(deliver(&mut harness, "m3"), Deferred);
    assert_eq!(wire(dir.path()).len(), 2);
    act(dir.path(), "finish");
    until(
        &mut harness,
        "m2",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(deliver(&mut harness, "m3"), InFlight);
    eventually(|| wire(dir.path()).len() == 4);
    let written = wire(dir.path());
    assert_eq!(written[1], (1, "steer".into(), "m2".into()));
    assert_eq!(written[2], (1, "prompt".into(), "m2".into()));
    assert_eq!(written[3], (1, "steer".into(), "m3".into()));
    assert_eq!(journaled(dir.path()), ["input:m1", "input:m2"]);
}

#[test]
fn steers_a_turn_ended_without_consuming_go_back_and_start_the_next_turn() {
    let dir = tempfile::tempdir().unwrap();
    let mut harness = fake_spiron(dir.path(), true);
    harness.set_state_dir(dir.path());
    until(
        &mut harness,
        "m1",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(deliver(&mut harness, "m2"), InFlight);
    assert_eq!(deliver(&mut harness, "m3"), InFlight);
    eventually(|| super::lock(&harness.shared).acknowledged.len() == 3);
    // The turn ends before a step: undelivered_steer_ids names both.
    act(dir.path(), "finish");
    until(
        &mut harness,
        "m1",
        Accepted {
            turn_finished: true,
        },
    );
    until(
        &mut harness,
        "m2",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(deliver(&mut harness, "m3"), InFlight);
    eventually(|| wire(dir.path()).len() == 5);
    act(dir.path(), "step");
    until(
        &mut harness,
        "m3",
        Accepted {
            turn_finished: false,
        },
    );
    let written: Vec<_> = wire(dir.path())
        .into_iter()
        .map(|(_, kind, id)| (kind, id))
        .collect();
    assert_eq!(
        written,
        [
            ("prompt".into(), "m1".into()),
            ("steer".into(), "m2".into()),
            ("steer".into(), "m3".into()),
            ("prompt".into(), "m2".into()),
            ("steer".into(), "m3".into()),
        ]
    );
    assert_eq!(journaled(dir.path()), ["input:m1", "input:m2", "input:m3"]);
    assert!(!harness.has_unconfirmed_hold());
    assert!(harness.health().sign_in_fault.is_none());
    assert!(!dir.path().join("spiron-unconfirmed-steers.json").exists());
}

#[test]
fn a_new_process_answers_an_input_the_last_one_consumed_as_a_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let mut first = fake_spiron(dir.path(), true);
    until(
        &mut first,
        "m1",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(deliver(&mut first, "m2"), InFlight);
    eventually(|| wire(dir.path()).len() == 2);
    act(dir.path(), "step");
    // The process dies before anyone records m2 as delivered.
    drop(first);
    let mut second = fake_spiron(dir.path(), true);
    // Hold the replay's turn open until a steer is admitted behind it.
    std::fs::write(dir.path().join("hold"), "").unwrap();
    assert_eq!(deliver(&mut second, "m2"), InFlight);
    assert_eq!(deliver(&mut second, "m3"), InFlight);
    eventually(|| wire(dir.path()).len() == 4);
    std::fs::remove_file(dir.path().join("hold")).unwrap();
    // The replay runs nothing, and its result hands m3 back undelivered.
    until(&mut second, "m2", Duplicate);
    until(
        &mut second,
        "m3",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(
        journaled(dir.path()),
        ["input:m1", "input:m2", "input:m3"],
        "the replayed input is journaled once"
    );
    let second_launch: Vec<_> = wire(dir.path())
        .into_iter()
        .filter(|(launch, _, _)| *launch == 2)
        .map(|(_, kind, id)| (kind, id))
        .collect();
    assert_eq!(
        second_launch,
        [
            ("prompt".into(), "m2".into()),
            ("steer".into(), "m3".into()),
            ("prompt".into(), "m3".into()),
        ]
    );
    act(dir.path(), "finish");
    until(
        &mut second,
        "m3",
        Accepted {
            turn_finished: true,
        },
    );
    assert_eq!(second.turns_served(), 1, "a duplicate ran nothing");
}

#[test]
fn steers_acknowledged_before_the_turn_is_accepted_are_consumed_after_it_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut harness = fake_spiron(dir.path(), true);
    std::fs::write(dir.path().join("hold"), "").unwrap();
    for id in ["m1", "m2", "m3"] {
        assert_eq!(deliver(&mut harness, id), InFlight, "{id}");
    }
    eventually(|| wire(dir.path()).len() == 3);
    // Both steers are acknowledged into a turn whose prompt is not journaled
    // yet: nothing is consumed.
    eventually(|| super::lock(&harness.shared).acknowledged.len() == 2);
    for id in ["m1", "m2", "m3"] {
        assert_eq!(deliver(&mut harness, id), InFlight, "{id}");
    }
    std::fs::remove_file(dir.path().join("hold")).unwrap();
    until(
        &mut harness,
        "m1",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(deliver(&mut harness, "m2"), InFlight);
    act(dir.path(), "step");
    until(
        &mut harness,
        "m2",
        Accepted {
            turn_finished: false,
        },
    );
    until(
        &mut harness,
        "m3",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(journaled(dir.path()), ["input:m1", "input:m2", "input:m3"]);
}

#[test]
fn a_turn_the_peer_could_not_start_is_offered_again_and_is_no_fault() {
    let dir = tempfile::tempdir().unwrap();
    let mut harness = fake_spiron(dir.path(), true);
    harness.set_state_dir(dir.path());
    // The session is still leased by a process that died.
    std::fs::write(dir.path().join("lease"), "").unwrap();
    std::fs::write(dir.path().join("hold"), "").unwrap();
    assert_eq!(deliver(&mut harness, "m1"), InFlight);
    assert_eq!(deliver(&mut harness, "m2"), InFlight);
    eventually(|| wire(dir.path()).len() == 2);
    std::fs::remove_file(dir.path().join("hold")).unwrap();
    // Both are refused `turn_not_started`: back in the mailbox, the process
    // is healthy, and the turn is not offered again before its backoff.
    eventually(|| super::lock(&harness.shared).in_flight.is_empty());
    let refused_at = Instant::now();
    assert_eq!(deliver(&mut harness, "m1"), Deferred);
    let health = harness.health();
    assert!(health.process_alive);
    assert!(health.sign_in_fault.is_none(), "{:?}", health.sign_in_fault);
    assert_eq!(health.turn_state, anyagents_harness::TurnState::Idle);
    assert!(!harness.has_unconfirmed_hold());
    assert_eq!(wire(dir.path()).len(), 2);
    // The backoff ends with a wake, not a poll: nothing else moves until it.
    let changes = super::lock(&harness.shared).changes;
    std::fs::remove_file(dir.path().join("lease")).unwrap();
    eventually(|| super::lock(&harness.shared).changes != changes);
    assert!(refused_at.elapsed() >= super::TURN_RETRY_FIRST / 2);
    until(
        &mut harness,
        "m1",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(deliver(&mut harness, "m2"), InFlight);
    eventually(|| wire(dir.path()).len() == 4);
    act(dir.path(), "step");
    until(
        &mut harness,
        "m2",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(journaled(dir.path()), ["input:m1", "input:m2"]);
    assert!(harness.health().sign_in_fault.is_none());
}

#[test]
fn an_older_peer_keeps_its_acknowledged_steer_deferred_and_held() {
    let dir = tempfile::tempdir().unwrap();
    let mut harness = fake_spiron(dir.path(), false);
    harness.set_state_dir(dir.path());
    until(
        &mut harness,
        "m1",
        Accepted {
            turn_finished: false,
        },
    );
    assert_eq!(deliver(&mut harness, "m2"), Deferred);
    eventually(|| super::lock(&harness.shared).acknowledged.contains("m2"));
    assert_eq!(deliver(&mut harness, "m2"), Deferred);
    assert!(dir.path().join("spiron-unconfirmed-steers.json").exists());
    act(dir.path(), "step");
    assert_eq!(deliver(&mut harness, "m2"), Deferred);
    act(dir.path(), "finish");
    until(
        &mut harness,
        "m2",
        Accepted {
            turn_finished: true,
        },
    );
    let written: Vec<_> = wire(dir.path())
        .into_iter()
        .map(|(_, kind, id)| (kind, id))
        .collect();
    assert_eq!(
        written,
        [("user".into(), "m1".into()), ("user".into(), "m2".into())]
    );
    assert!(!dir.path().join("spiron-unconfirmed-steers.json").exists());
}
