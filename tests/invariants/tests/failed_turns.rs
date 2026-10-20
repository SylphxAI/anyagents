//! A turn that ends in an error must not advance the processed cursor: the
//! message stays accepted, is readable as a failed turn, and a later successful
//! turn processes it once. The delivery loop and the supervisor share this.

use anyagents_agent::DeliveryLoop;
use anyagents_core::{Ack, Incoming, NewAgent, ReceiveOutcome, Store};
use anyagents_harness::{DeliveryMode, FakeHarness, Harness};

fn world(mode: DeliveryMode) -> (tempfile::TempDir, DeliveryLoop<FakeHarness>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    let loop_ = DeliveryLoop::start(store, agent.id, FakeHarness::new(mode)).unwrap();
    (dir, loop_)
}

fn receive(loop_: &DeliveryLoop<FakeHarness>, text: &str) -> (i64, String) {
    let agent = loop_.agent_id();
    match loop_.store().receive(&agent, Incoming::new(text)).unwrap() {
        ReceiveOutcome::New(message) => (message.seq, message.id.as_str().to_string()),
        ReceiveOutcome::Duplicate(_) => panic!("unexpected duplicate"),
    }
}

#[test]
fn a_failed_turn_leaves_the_processed_cursor_alone() {
    let (_dir, mut loop_) = world(DeliveryMode::Followup);
    let (seq, _id) = receive(&loop_, "unread");
    loop_.run_once().unwrap();
    assert_eq!(
        loop_.harness_mut().fail_turn("authentication_failed"),
        Some(seq)
    );
    loop_
        .record_failed_turn(seq, "authentication_failed")
        .unwrap();

    let agent = loop_.agent_id();
    assert_eq!(loop_.store().processed_cursor(&agent).unwrap(), 0);
    assert_eq!(loop_.store().accepted_cursor(&agent).unwrap(), seq);
    let inbox = loop_.store().inbox(&agent, None).unwrap();
    assert_eq!(inbox[0].ack, Ack::Accepted);
    assert_eq!(
        inbox[0].failed_reason.as_deref(),
        Some("authentication_failed")
    );
    let failed = loop_.store().failed_turns(&agent).unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].seq, seq);
}

#[test]
fn a_later_successful_turn_processes_the_same_message_once() {
    let (_dir, mut loop_) = world(DeliveryMode::Steer);
    let agent = loop_.agent_id();
    let (seq, _id) = receive(&loop_, "once");
    loop_.run_once().unwrap();
    assert_eq!(loop_.harness_mut().fail_turn("vendor error"), Some(seq));
    loop_.record_failed_turn(seq, "vendor error").unwrap();
    assert_eq!(loop_.store().processed_cursor(&agent).unwrap(), 0);

    // The supervisor replays the failed id into the fresh process; the same
    // message then finishes successfully exactly once.
    let message = loop_.store().failed_turns(&agent).unwrap().remove(0);
    loop_
        .harness_mut()
        .deliver(&anyagents_harness::OutboundMessage {
            id: message.id.as_str().to_string(),
            seq: message.seq,
            text: message.text.clone(),
            sender: message.sender.clone(),
            reply_to: message.reply_to.clone(),
            source_time_ms: message.source_time_ms,
        })
        .unwrap();
    // The same turn was already closed as a failure; completing it again is the
    // replay's success and does not open a second turn.
    loop_.harness_mut().complete_turn();
    loop_.mark_processed_through(seq).unwrap();

    let inbox = loop_.store().inbox(&agent, None).unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].ack, Ack::Processed);
    assert_eq!(inbox[0].failed_reason, None);
    assert!(loop_.store().failed_turns(&agent).unwrap().is_empty());
}
