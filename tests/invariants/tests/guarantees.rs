use anyagents_agent::{DeliveryLoop, Error, RunOutcome};
use anyagents_core::{Incoming, NewAgent, ReceiveOutcome, Store};
use anyagents_harness::{DeliveryMode, FakeHarness};

fn db(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("state.db")
}

fn open(dir: &tempfile::TempDir) -> Store {
    Store::open(db(dir)).unwrap()
}

struct World {
    dir: tempfile::TempDir,
    store: Store,
    agent: anyagents_core::AgentId,
}

fn world() -> World {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir);
    let agent = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap()
        .id;
    World { dir, store, agent }
}

fn receive(
    store: &Store,
    agent: &anyagents_core::AgentId,
    incoming: Incoming,
) -> anyagents_core::Message {
    match store.receive(agent, incoming).unwrap() {
        ReceiveOutcome::New(message) => message,
        ReceiveOutcome::Duplicate(_) => panic!("unexpected duplicate"),
    }
}

#[test]
fn crash_after_receive_still_delivers_once() {
    let world = world();
    let message = receive(&world.store, &world.agent, Incoming::new("kept"));
    let loop_ = DeliveryLoop::start(
        world.store,
        world.agent.clone(),
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    drop(loop_);

    let store = open(&world.dir);
    assert_eq!(store.inbox(&world.agent, None).unwrap().len(), 1);
    let mut loop_ = DeliveryLoop::start(
        store,
        world.agent.clone(),
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    loop_.run_once().unwrap();
    assert_eq!(loop_.harness().written_ids(), &[message.id.to_string()]);
    assert_eq!(loop_.harness().turns_started(), 1);
}

#[test]
fn crash_after_accept_before_cursor_dedupes_replay() {
    let world = world();
    let message = receive(&world.store, &world.agent, Incoming::new("once"));
    let mut loop_ = DeliveryLoop::start(
        world.store,
        world.agent.clone(),
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    loop_.deliver_next_without_commit().unwrap();
    assert_eq!(loop_.harness().written_ids(), &[message.id.to_string()]);
    assert_eq!(loop_.harness().turns_started(), 1);
    assert_eq!(loop_.delivered_cursor().unwrap(), 0);
    let harness = loop_.into_harness();

    let store = open(&world.dir);
    let mut loop_ = DeliveryLoop::start(store, world.agent.clone(), harness).unwrap();
    loop_.run_once().unwrap();
    assert_eq!(loop_.harness().written_ids().len(), 1);
    assert_eq!(loop_.harness().turns_started(), 1);
    assert_eq!(loop_.harness().given_ids().len(), 2);
    assert_eq!(loop_.delivered_cursor().unwrap(), message.seq);
}

#[test]
fn late_source_time_does_not_reorder_delivery() {
    let world = world();
    let mut newer = Incoming::new("newer");
    newer.source_time_ms = Some(500);
    let mut older = Incoming::new("older");
    older.source_time_ms = Some(100);
    let first = receive(&world.store, &world.agent, newer);
    let second = receive(&world.store, &world.agent, older);
    assert_eq!(second.seq, first.seq + 1);
    let mut loop_ = DeliveryLoop::start(
        world.store,
        world.agent,
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    loop_.run_once().unwrap();
    assert_eq!(
        loop_.harness().written_ids(),
        &[first.id.to_string(), second.id.to_string()]
    );
}

#[test]
fn interleaved_sources_deliver_in_receipt_order() {
    let world = world();
    let mut a = Incoming::new("a");
    a.sender = Some("one".into());
    a.source_time_ms = Some(300);
    let mut b = Incoming::new("b");
    b.sender = Some("two".into());
    b.source_time_ms = Some(100);
    let mut c = Incoming::new("c");
    c.sender = Some("one".into());
    c.source_time_ms = Some(200);
    let first = receive(&world.store, &world.agent, a);
    let second = receive(&world.store, &world.agent, b);
    let third = receive(&world.store, &world.agent, c);
    let mut loop_ = DeliveryLoop::start(
        world.store,
        world.agent,
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    loop_.run_once().unwrap();
    assert_eq!(
        loop_.harness().written_ids(),
        &[
            first.id.to_string(),
            second.id.to_string(),
            third.id.to_string()
        ]
    );
    assert_eq!((first.seq, second.seq, third.seq), (1, 2, 3));
}

#[test]
fn health_reports_the_oldest_line_the_harness_has_not_taken() {
    let world = world();
    let mut incoming = Incoming::new("waiting");
    incoming.received_at_ms = Some(1_000);
    receive(&world.store, &world.agent, incoming);
    let mut loop_ = DeliveryLoop::start(
        world.store,
        world.agent.clone(),
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    assert_eq!(
        loop_.health(6_000).unwrap().oldest_undelivered_age_ms,
        Some(5_000)
    );

    loop_.run_once().unwrap();
    assert_eq!(loop_.health(6_000).unwrap().oldest_undelivered_age_ms, None);
}

#[test]
fn quota_fault_keeps_receiving_and_reports_reset_time() {
    let world = world();
    receive(&world.store, &world.agent, Incoming::new("first"));
    let mut loop_ = DeliveryLoop::start(
        world.store,
        world.agent.clone(),
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    loop_.harness_mut().set_quota(Some(9_000_000));
    let outcome = loop_.run_once().unwrap();
    assert_eq!(
        outcome,
        RunOutcome::Quota {
            reset_at_ms: 9_000_000
        }
    );
    assert_eq!(loop_.harness().given_ids().len(), 1);
    assert_eq!(loop_.delivered_cursor().unwrap(), 0);
    assert_eq!(loop_.health(0).unwrap().quota_reset_at_ms, Some(9_000_000));

    receive(loop_.store(), &world.agent, Incoming::new("second"));
    assert_eq!(loop_.store().inbox(&world.agent, None).unwrap().len(), 2);

    loop_.harness_mut().set_quota(None);
    loop_.run_once().unwrap();
    assert_eq!(loop_.harness().written_ids().len(), 2);
    assert_eq!(loop_.harness().turns_started(), 1);
    assert_eq!(loop_.delivered_cursor().unwrap(), 2);
}

#[test]
fn second_loop_for_the_same_agent_is_refused() {
    let world = world();
    let held = DeliveryLoop::start(
        world.store,
        world.agent.clone(),
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    let err = match DeliveryLoop::start(
        open(&world.dir),
        world.agent.clone(),
        FakeHarness::new(DeliveryMode::Steer),
    ) {
        Ok(started) => {
            drop(started);
            panic!("second loop should be refused");
        }
        Err(err) => err,
    };
    assert!(err.agent_busy(), "{err}");

    let store = open(&world.dir);
    let other = store
        .create_agent(NewAgent::new("beta", "claude-code"))
        .unwrap();
    let _other =
        DeliveryLoop::start(store, other.id, FakeHarness::new(DeliveryMode::Steer)).unwrap();

    drop(held);
    DeliveryLoop::start(
        open(&world.dir),
        world.agent,
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
}

#[test]
fn killed_harness_keeps_the_message_for_a_new_process() {
    let world = world();
    let message = receive(&world.store, &world.agent, Incoming::new("still here"));
    let mut loop_ = DeliveryLoop::start(
        world.store,
        world.agent.clone(),
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    loop_.harness_mut().kill();
    let err = loop_.run_once().unwrap_err();
    assert!(matches!(err, Error::HarnessDown));
    assert_eq!(loop_.delivered_cursor().unwrap(), 0);
    drop(loop_);

    let mut loop_ = DeliveryLoop::start(
        open(&world.dir),
        world.agent,
        FakeHarness::new(DeliveryMode::Steer),
    )
    .unwrap();
    loop_.run_once().unwrap();
    assert_eq!(loop_.harness().written_ids(), &[message.id.to_string()]);
    assert_eq!(loop_.harness().turns_started(), 1);
}
