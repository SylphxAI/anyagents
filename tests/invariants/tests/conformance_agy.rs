//! The delivery conformance suite against the Antigravity adapter, over a
//! scripted `agy` whose language server serves its Connect RPC
//! (`fakes/proto_agy.py`), and what the adapter does with a steer the
//! language server leaves unanswered, refuses, or answers late, and with
//! mail a cascade's mailbox holds that it never took.
#![cfg(unix)]

use std::time::{Duration, Instant};

use anyagents_core::Message;
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

/// The language server queued `message`: it is in flight, never committed,
/// until the running turn's next step takes it and the cascade's record
/// says so.
fn commit_queued(world: &mut World<Agy>, message: &Message) {
    world.pump_until("the language server queued it", |w| {
        queued(&w.fixture).contains(&message.id.to_string())
    });
    world.pump_for(Duration::from_millis(300));
    assert!(
        world.cursor() < message.seq,
        "committed before the cascade took it"
    );
    world.fixture.command("step");
    world.pump_until("the step's proof commits it", |w| w.cursor() == message.seq);
}

/// The ids of the mail a cascade's mailbox holds and has not taken: what the
/// language server answered `{}` for.
fn queued(fixture: &Fixture) -> Vec<String> {
    let brain = fixture
        .dir()
        .join("agy-config/.gemini/antigravity-cli/brain");
    let mut held = Vec::new();
    for cascade in std::fs::read_dir(brain).into_iter().flatten().flatten() {
        let undelivered = cascade
            .path()
            .join(".system_generated/messages/undelivered");
        for mail in std::fs::read_dir(undelivered)
            .into_iter()
            .flatten()
            .flatten()
        {
            let path = mail
                .path()
                .parent()
                .and_then(|dir| dir.parent())
                .map(|dir| dir.join(format!("{}.json", mail.file_name().to_string_lossy())));
            if let Some(text) = path.and_then(|path| std::fs::read_to_string(path).ok()) {
                held.push(text);
            }
        }
    }
    // The marker sits base64-encoded in the step payload: decode it with
    // the vendor's own encoding.
    held.iter()
        .filter_map(|text| serde_json::from_str::<serde_json::Value>(text).ok())
        .filter_map(|mail| mail["stepPayload"].as_str().map(str::to_string))
        .filter_map(|payload| decode(&payload))
        .filter_map(|step| {
            let text = String::from_utf8_lossy(&step).into_owned();
            let at = text.find("anyagents-message:")? + "anyagents-message:".len();
            let id = &text[at..];
            Some(id[..id.find('\n')?].to_string())
        })
        .collect()
}

fn decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0u32);
    for byte in text.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

#[test]
fn the_language_server_names_the_peer_and_its_level_is_turn_end() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    let second = world.receive(1).remove(0);
    commit_queued(&mut world, &second);
    let delivery = world.delivery_level();
    assert_eq!(delivery.level, DeliveryLevel::TurnEnd);
    assert!(delivery.idempotent);
    assert_eq!(delivery.peer.as_deref(), Some("agy 1.5.8"));
    world.drain(second.seq);
    assert_prompt_then(&world, first.id.as_str(), second.id.as_str());
}

/// The language server queued the steer and the connection closed before
/// its answer: the cascade's mailbox shows it queued, so it is never sent
/// again, and it is committed once the cascade took it.
#[test]
fn an_unanswered_steer_the_mailbox_holds_is_not_sent_again() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    arm(&world.fixture, "drop-answer");
    let second = world.receive(1).remove(0);
    commit_queued(&mut world, &second);
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
    world.pump_until("the steer is sent again", |w| {
        sends(&w.fixture, second.id.as_str()) == 2
    });
    commit_queued(&mut world, &second);
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
    commit_queued(&mut world, &second);
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
    // The turn takes it at once; the late answer still decides nothing.
    world.fixture.command("step");
    world.pump_until("the cascade's record commits it", |w| {
        w.cursor() == second.seq
    });
    assert!(
        sent.elapsed() < Duration::from_secs(5),
        "committed only on the late answer: {:?}",
        sent.elapsed()
    );
    world.pump_until("the late answer arrives", |_| {
        sent.elapsed() >= Duration::from_secs(6)
    });
    world.fixture.clear_slow();
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    world.drain(second.seq);
    assert_prompt_then(&world, first.id.as_str(), second.id.as_str());
}

/// The cascade's run fails while mail waits in its mailbox: the language
/// server queued it and answered `{}`, and the cascade never took it. It is
/// neither committed nor lost: the next message joins the mailbox behind it
/// rather than opening a turn of its own, and the invocation that message
/// starts takes both, in order.
#[test]
fn a_cascade_error_with_queued_mail_commits_nothing_and_loses_nothing() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    let second = world.receive(1).remove(0);
    world.pump_until("the language server queued it", |w| {
        queued(&w.fixture).contains(&second.id.to_string())
    });
    world.fixture.command("fail");
    world.pump_until("the failed turn ends", |w| w.harness_idle());
    world.pump_for(Duration::from_millis(1_500));
    assert_eq!(
        world.cursor(),
        first.seq,
        "committed what the cascade never took"
    );
    assert!(queued(&world.fixture).contains(&second.id.to_string()));
    assert_eq!(world.fixture.launches(), 1, "restarted");
    let third = world.receive(1).remove(0);
    world.pump_until("both are committed", |w| w.cursor() == third.seq);
    world.drain(third.seq);
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    assert_eq!(sends(&world.fixture, third.id.as_str()), 1);
    let journal = world.fixture.journal();
    assert_eq!(
        ids(&journal),
        [
            first.id.to_string(),
            second.id.to_string(),
            third.id.to_string()
        ],
        "{journal:?}"
    );
    assert_eq!(world.fixture.launches(), 1, "restarted");
}

/// The process dies while its cascade's mailbox holds mail it never took. A
/// process that resumes the cascade does not write it again: the cascade
/// takes it, and its record commits it.
#[test]
fn mail_a_resumed_cascade_still_holds_is_not_written_again() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    let second = world.receive(1).remove(0);
    world.pump_until("the language server queued it", |w| {
        queued(&w.fixture).contains(&second.id.to_string())
    });
    world.fixture.command("crash");
    world.pump_until_down();
    assert!(world.cursor() < second.seq);
    world.restart();
    world.drain(second.seq);
    assert_eq!(sends(&world.fixture, second.id.as_str()), 1);
    let journal = world.fixture.journal();
    let counts = journal
        .iter()
        .filter(|seen| seen.id == second.id.as_str())
        .count();
    assert_eq!(counts, 1, "{journal:?}");
    assert_eq!(journal[0].id, first.id.as_str());
}

/// The process dies while its cascade's mailbox holds mail it never took,
/// and the agent moves to a new cascade (the old one retired). The mail is
/// offered afresh to the new cascade and reaches the model once.
#[test]
fn a_retired_cascade_s_mail_is_offered_to_the_next_one() {
    let mut world = World::<Agy>::new();
    let first = world.prompt();
    let second = world.receive(1).remove(0);
    world.pump_until("the language server queued it", |w| {
        queued(&w.fixture).contains(&second.id.to_string())
    });
    world.fixture.command("crash");
    world.pump_until_down();
    world.delivery.retire_harness().expect("retire");
    world
        .delivery
        .replace_harness(Agy::launch(&world.fixture, None));
    world.pump_until("it is committed in the new cascade", |w| {
        w.cursor() == second.seq
    });
    world.drain(second.seq);
    let journal = world.fixture.journal();
    assert_eq!(
        ids(&journal),
        [first.id.to_string(), second.id.to_string()],
        "{journal:?}"
    );
    assert_ne!(journal[0].launch, journal[1].launch);
    // The retired cascade still holds it, unread: nothing resumes it.
    assert!(queued(&world.fixture).contains(&second.id.to_string()));
}
