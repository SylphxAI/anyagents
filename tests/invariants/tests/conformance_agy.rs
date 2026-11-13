//! The delivery conformance suite against the Antigravity adapter, over a
//! scripted `agy` whose language server serves its Connect RPC
//! (`fakes/proto_agy.py`), and what the adapter does with a steer the
//! language server leaves unanswered, refuses, or answers late.
#![cfg(unix)]

use std::time::{Duration, Instant};

use anyagents_harness::DeliveryLevel;
use anyagents_harness_agy::AgyHarness;
use anyagents_harness_stream::StreamSpec;
use anyagents_invariants::conformance::{Fixture, Subject, World, ids};

pub struct Agy;

impl Subject for Agy {
    type Harness = AgyHarness;
    const HARNESS: &'static str = "agy";
    const PROTOCOL: &'static str = "agy";

    /// As the server starts it (`vendor.rs`): stream-json both ways, resumed
    /// by `--conversation`, the agent's config directory as `AGY_CONFIG_DIR`.
    fn launch(fixture: &Fixture, session: Option<&str>) -> AgyHarness {
        let config = fixture.dir().join("agy-config");
        std::fs::create_dir_all(&config).expect("config directory");
        let config = config.display().to_string();
        let mut spec = StreamSpec::for_program("agy");
        spec.args = [
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--dangerously-skip-permissions",
        ]
        .map(String::from)
        .to_vec();
        spec.resume_flag = "--conversation";
        AgyHarness::start_with_spec(
            spec,
            fixture.program(),
            session,
            &[("AGY_CONFIG_DIR", config.as_str())],
            Some(fixture.dir()),
        )
        .expect("agy starts")
    }

    fn session(harness: &AgyHarness) -> Option<String> {
        harness.session_id()
    }
}

mod suite {
    anyagents_invariants::conformance_suite!(super::Agy);
}

/// Have the scripted vendor act on its next request of that kind.
fn arm(fixture: &Fixture, name: &str) {
    std::fs::write(fixture.dir().join(name), "").expect("arm the vendor");
}

/// How many `SendUserCascadeMessage` requests carried `id`.
fn sends(fixture: &Fixture, id: &str) -> usize {
    std::fs::read_to_string(fixture.dir().join("rpc.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|line| {
            serde_json::from_str::<serde_json::Value>(line).is_ok_and(|row| row["id"] == id)
        })
        .count()
}

/// The prompt and one steer behind it, each seen by the model once, in order,
/// by one process.
fn assert_prompt_then(world: &World<Agy>, first: &str, second: &str) {
    let journal = world.fixture.journal();
    assert_eq!(ids(&journal), [first, second], "{journal:?}");
    assert_eq!(world.fixture.launches(), 1, "restarted");
}

#[test]
fn the_language_server_names_the_peer_and_its_level_is_mid_turn() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    let second = world.receive(1).remove(0);
    world.pump_until("the steer is committed", |w| w.cursor() == second.seq);
    let delivery = world.delivery_level();
    assert_eq!(delivery.level, DeliveryLevel::MidTurn);
    assert!(!delivery.idempotent);
    assert_eq!(delivery.peer.as_deref(), Some("agy 1.5.8"));
    world.drain(second.seq);
    assert_prompt_then(&world, first.id.as_str(), second.id.as_str());
}

/// The language server queued the steer and the connection closed before
/// its answer: the cascade's mailbox shows it queued, so it is committed and
/// never sent again.
#[test]
fn an_unanswered_steer_the_mailbox_holds_is_not_sent_again() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    arm(&world.fixture, "drop-answer");
    let second = world.receive(1).remove(0);
    world.pump_until("the queued steer is committed", |w| {
        w.cursor() == second.seq
    });
    assert!(!world.fixture.dir().join("drop-answer").exists());
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    world.drain(second.seq);
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    assert_prompt_then(&world, first.id.as_str(), second.id.as_str());
}

/// The connection closed before an answer and nothing was queued: the
/// mailbox does not hold it, so it is sent again, and the model sees it once.
#[test]
fn an_unanswered_steer_the_mailbox_lacks_is_sent_again() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    arm(&world.fixture, "drop-request");
    let second = world.receive(1).remove(0);
    world.pump_until("the resent steer is committed", |w| {
        w.cursor() == second.seq
    });
    assert!(!world.fixture.dir().join("drop-request").exists());
    assert_eq!(sends(&world.fixture, second.id.as_str()), 2);
    world.drain(second.seq);
    assert_prompt_then(&world, first.id.as_str(), second.id.as_str());
}

/// A run-status probe the language server cannot answer is asked again
/// later: no fault, no restart.
#[test]
fn a_failed_probe_costs_no_restart() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    arm(&world.fixture, "fail-probe");
    let second = world.receive(1).remove(0);
    world.pump_until("the steer is committed", |w| w.cursor() == second.seq);
    assert!(!world.fixture.dir().join("fail-probe").exists());
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    world.drain(second.seq);
    assert_prompt_then(&world, first.id.as_str(), second.id.as_str());
}

/// A cascade that refused a steer is not asked again within its turn; the
/// message opens the next turn.
#[test]
fn a_refused_steer_waits_for_the_turn_to_end() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    world.fixture.command("refuse");
    let second = world.receive(1).remove(0);
    world.pump_until("the steer reaches the vendor", |w| {
        w.fixture.wire().contains(&second.id.to_string())
    });
    world.pump_for(Duration::from_millis(800));
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    assert_eq!(world.cursor(), first.seq);
    world.fixture.command("finish");
    world.pump_until("the message opens the next turn", |w| {
        w.cursor() == second.seq
    });
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    world.drain(second.seq);
    let journal = world.fixture.journal();
    assert_eq!(journal[1].kind, "prompt", "{journal:?}");
    assert_prompt_then(&world, first.id.as_str(), second.id.as_str());
}

/// A steer whose answer takes longer than any request timeout the adapter
/// once had is waited for, not sent again.
#[test]
fn an_answer_slower_than_a_timeout_is_waited_for() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    world.fixture.set_slow(6_000);
    let second = world.receive(1).remove(0);
    world.pump_until("the steer reaches the vendor", |w| {
        w.fixture.wire().contains(&second.id.to_string())
    });
    let sent = Instant::now();
    world.pump_until("the late answer commits it", |w| w.cursor() == second.seq);
    assert!(
        sent.elapsed() >= Duration::from_secs(5),
        "{:?}",
        sent.elapsed()
    );
    world.fixture.clear_slow();
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    world.drain(second.seq);
    assert_prompt_then(&world, first.id.as_str(), second.id.as_str());
}
