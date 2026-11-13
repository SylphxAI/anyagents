//! The delivery conformance suite: one set of scenarios, run against every
//! harness adapter over a scripted vendor process that speaks that vendor's
//! protocol (`fakes/vendor.py`).
//!
//! An adapter states its level through [`HarnessHealth::delivery`]; each
//! scenario holds it to what `docs/delivery.md` promises for that level, and
//! to the rules every level shares: no message is lost, none is applied
//! twice, the vendor sees them in mailbox order, and a refusal never costs a
//! restart. The vendor's journal (what its model saw) is the evidence, the
//! mailbox cursor the commit.
//!
//! A new harness gets the suite by implementing [`Subject`] and invoking
//! [`conformance_suite!`] once, and by teaching `fakes/vendor.py` its
//! protocol.
//!
//! [`HarnessHealth::delivery`]: anyagents_harness::HarnessHealth

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyagents_agent::{DeliveryLoop, Error, RunOutcome};
use anyagents_core::{AgentId, Incoming, Message, NewAgent, ReceiveOutcome, Store};
use anyagents_harness::{Delivery, DeliveryLevel, Harness, TurnState};

const VENDOR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fakes/vendor.py");

/// How long one scenario step may take before the suite calls it stuck.
const PATIENCE: Duration = Duration::from_secs(30);

/// One scripted vendor: its state directory and the program that starts it.
pub struct Fixture {
    dir: tempfile::TempDir,
    program: PathBuf,
}

/// One input the vendor's model saw, from its journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seen {
    pub id: String,
    /// `prompt` (opened a turn) or `steer` (joined a running one at a step).
    pub kind: String,
    pub turn: Option<String>,
    pub step: u64,
    pub launch: u64,
    pub text: String,
}

/// The one wrapper every fixture's vendor program links to, written once per
/// test process. A fixture's program is a symlink to it named
/// `vendor-<protocol>` inside the fixture directory, which tells the wrapper
/// both. Writing a script per fixture raced with other test threads: a child
/// forked while the script was still open for writing makes its exec fail
/// (`ETXTBSY`); a symlink is never open for writing.
fn wrapper() -> &'static Path {
    static WRAPPER: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    WRAPPER.get_or_init(|| {
        let dir = tempfile::Builder::new()
            .prefix("anyagents-conformance-")
            .tempdir()
            .expect("wrapper directory")
            .keep();
        let path = dir.join("vendor.sh");
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\nexec /usr/bin/python3 -B -u '{VENDOR}' --fixture \"$(dirname \"$0\")\" --protocol \"$(basename \"$0\" | sed 's/^vendor-//')\" \"$@\"\n"
            ),
        )
        .expect("vendor wrapper");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                .expect("vendor wrapper mode");
        }
        path
    })
}

#[cfg(unix)]
fn link(target: &Path, at: &Path) {
    std::os::unix::fs::symlink(target, at).expect("vendor program link");
}

#[cfg(not(unix))]
fn link(target: &Path, at: &Path) {
    std::fs::copy(target, at).expect("vendor program copy");
}

impl Fixture {
    /// A fresh vendor state for `protocol`.
    pub fn new(protocol: &str) -> Self {
        let dir = tempfile::tempdir().expect("fixture directory");
        let program = dir.path().join(format!("vendor-{protocol}"));
        link(wrapper(), &program);
        Self { dir, program }
    }

    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// The executable an adapter starts as its vendor.
    pub fn program(&self) -> &Path {
        &self.program
    }

    /// Ask the vendor to act (`step`, `step-N`, `finish`, `crash`, `refuse`)
    /// and wait until it has.
    pub fn command(&self, name: &str) {
        let path = self.dir.path().join(name);
        std::fs::write(&path, "").expect("vendor command");
        eventually(&format!("the vendor acts on `{name}`"), || !path.exists());
    }

    /// Answer every input this many milliseconds late, from now on.
    pub fn set_slow(&self, millis: u64) {
        std::fs::write(self.dir.path().join("slow"), millis.to_string()).expect("slow");
    }

    pub fn clear_slow(&self) {
        let _ = std::fs::remove_file(self.dir.path().join("slow"));
    }

    fn records(&self, name: &str) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.dir.path().join(name))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).expect("vendor record"))
            .collect()
    }

    /// Every input the vendor's model saw, in order.
    pub fn journal(&self) -> Vec<Seen> {
        self.records("journal.jsonl")
            .into_iter()
            .map(|row| Seen {
                id: row["id"].as_str().unwrap_or_default().to_string(),
                kind: row["kind"].as_str().unwrap_or_default().to_string(),
                turn: row["turn"].as_str().map(str::to_string),
                step: row["step"].as_u64().unwrap_or_default(),
                launch: row["launch"].as_u64().unwrap_or_default(),
                text: row["text"].as_str().unwrap_or_default().to_string(),
            })
            .collect()
    }

    /// The id of every input that reached the vendor, as it arrived.
    pub fn wire(&self) -> Vec<String> {
        self.records("wire.jsonl")
            .into_iter()
            .map(|row| row["id"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    /// How many vendor processes have started.
    pub fn launches(&self) -> u64 {
        std::fs::read_to_string(self.dir.path().join("launches"))
            .ok()
            .and_then(|text| text.trim().parse().ok())
            .unwrap_or(0)
    }

    /// Whether the vendor has a turn running.
    pub fn in_turn(&self) -> bool {
        std::fs::read_to_string(self.dir.path().join("state.json"))
            .ok()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
            .is_some_and(|state| state["turn"].is_string())
    }
}

/// One harness adapter under test.
pub trait Subject {
    type Harness: Harness;
    /// The harness name an agent row carries.
    const HARNESS: &'static str;
    /// The protocol `fakes/vendor.py` speaks for it.
    const PROTOCOL: &'static str;
    /// Start the adapter on `fixture`'s vendor, resuming `session`.
    fn launch(fixture: &Fixture, session: Option<&str>) -> Self::Harness;
    /// The vendor session the adapter holds, to resume after a crash.
    fn session(harness: &Self::Harness) -> Option<String>;
}

/// One agent's mailbox, its delivery loop over the adapter, and the vendor.
pub struct World<S: Subject> {
    pub fixture: Fixture,
    pub delivery: DeliveryLoop<S::Harness>,
    pub agent: AgentId,
    _db: tempfile::TempDir,
    received: usize,
    served: u32,
}

impl<S: Subject> World<S> {
    pub fn new() -> Self {
        let fixture = Fixture::new(S::PROTOCOL);
        let db = tempfile::tempdir().expect("store directory");
        let store = Store::open(db.path().join("state.db")).expect("store");
        let agent = store
            .create_agent(NewAgent::new("conformance", S::HARNESS))
            .expect("agent")
            .id;
        let harness = S::launch(&fixture, None);
        let delivery = DeliveryLoop::start(store, agent.clone(), harness).expect("delivery loop");
        Self {
            fixture,
            delivery,
            agent,
            _db: db,
            received: 0,
            served: 0,
        }
    }

    pub fn delivery_level(&self) -> Delivery {
        self.delivery.harness().health().delivery
    }

    /// `count` new mailbox messages, in sequence order.
    pub fn receive(&mut self, count: usize) -> Vec<Message> {
        (0..count)
            .map(|_| {
                self.received += 1;
                let text = format!("conformance message {}", self.received);
                match self
                    .delivery
                    .store()
                    .receive(&self.agent, Incoming::new(text))
                    .expect("receive")
                {
                    ReceiveOutcome::New(message) => message,
                    ReceiveOutcome::Duplicate(_) => panic!("unexpected duplicate"),
                }
            })
            .collect()
    }

    pub fn cursor(&self) -> i64 {
        self.delivery.delivered_cursor().expect("cursor")
    }

    pub fn harness_idle(&self) -> bool {
        self.delivery.harness().health().turn_state == TurnState::Idle
    }

    pub fn alive(&self) -> bool {
        self.delivery.harness().health().process_alive
    }

    /// One supervisor pass: hand the mailbox to the harness, then record a
    /// turn the harness finished, as the supervisor does when it goes idle.
    pub fn pass(&mut self) -> Result<RunOutcome, Error> {
        let outcome = self.delivery.run_once();
        let served = self.delivery.harness().turns_served();
        if self.harness_idle() && served > self.served {
            self.delivery
                .store()
                .note_turn_completed(&self.agent)
                .expect("turn completion");
            self.served = served;
        }
        outcome
    }

    /// Passes until `done` holds. A pass that fails fails the scenario: no
    /// level may answer a delivery with a fault or a restart.
    pub fn pump_until(&mut self, what: &str, mut done: impl FnMut(&mut Self) -> bool) {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Err(err) = self.pass() {
                panic!(
                    "{}: a pass failed while waiting until {what}: {err}; vendor: {:?}",
                    S::HARNESS,
                    self.delivery.harness().health().sign_in_fault
                );
            }
            if done(self) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{}: timed out waiting until {what}; cursor {}, journal {:?}",
                S::HARNESS,
                self.cursor(),
                ids(&self.fixture.journal())
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Passes for `span`, none of which may fail.
    pub fn pump_for(&mut self, span: Duration) {
        let until = Instant::now() + span;
        self.pump_until("a quiet span passes", |_| Instant::now() >= until);
    }

    /// Passes until the harness reports its process gone.
    pub fn pump_until_down(&mut self) {
        let deadline = Instant::now() + PATIENCE;
        loop {
            match self.pass() {
                Err(Error::HarnessDown) => return,
                Err(err) => panic!("{}: unexpected failure: {err}", S::HARNESS),
                Ok(_) if !self.alive() => return,
                Ok(_) => {}
            }
            assert!(Instant::now() < deadline, "{}: never went down", S::HARNESS);
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// A new vendor process on the same session, behind the same mailbox.
    pub fn restart(&mut self) {
        let session = S::session(self.delivery.harness());
        let harness = S::launch(&self.fixture, session.as_deref());
        self.delivery.replace_harness(harness);
        self.served = 0;
    }

    /// The prompt that opens a turn: delivered, committed, and still running.
    pub fn prompt(&mut self) -> Message {
        let first = self.receive(1).remove(0);
        self.pump_until("the prompt is committed", |w| w.cursor() == first.seq);
        first
    }

    /// Drive the vendor until every message is committed and nothing runs:
    /// steps and finishes its turns, and offers a recovery note for a turn a
    /// crash cut short once the mailbox is empty, as the supervisor does.
    pub fn drain(&mut self, last: i64) {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Err(err) = self.pass() {
                panic!("{}: a pass failed while draining: {err}", S::HARNESS);
            }
            let store = self.delivery.store();
            if self.cursor() >= last
                && store.next_undelivered(&self.agent).expect("next").is_none()
                && let Some(open) = store.open_turn(&self.agent).expect("open turn")
                && self.harness_idle()
                && !self.fixture.in_turn()
                && open.recovery_count == 0
            {
                self.delivery
                    .recover_open_turn(&open)
                    .unwrap_or_else(|err| panic!("{}: recovery failed: {err}", S::HARNESS));
            }
            if self.cursor() >= last && self.harness_idle() && !self.fixture.in_turn() {
                return;
            }
            if self.fixture.in_turn() {
                self.fixture.command("step");
                self.fixture.command("finish");
            }
            assert!(
                Instant::now() < deadline,
                "{}: never drained; cursor {} of {last}, journal {:?}",
                S::HARNESS,
                self.cursor(),
                ids(&self.fixture.journal())
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl<S: Subject> Default for World<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// The ids of journal rows, in order.
pub fn ids(journal: &[Seen]) -> Vec<String> {
    journal.iter().map(|seen| seen.id.clone()).collect()
}

fn message_ids(messages: &[Message]) -> Vec<String> {
    messages.iter().map(|m| m.id.to_string()).collect()
}

/// How many times the model saw each message as input of its own.
fn applied(journal: &[Seen]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for seen in journal.iter().filter(|seen| seen.kind != "note") {
        *counts.entry(seen.id.clone()).or_insert(0) += 1;
    }
    counts
}

fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Every message reached the model once, and in mailbox order.
fn assert_once_in_order<S: Subject>(world: &World<S>, messages: &[Message]) {
    let journal = world.fixture.journal();
    let counts = applied(&journal);
    for message in messages {
        assert_eq!(
            counts.get(message.id.as_str()).copied().unwrap_or(0),
            1,
            "{}: {} reached the model {} times; journal {:?}",
            S::HARNESS,
            message.id,
            counts.get(message.id.as_str()).copied().unwrap_or(0),
            ids(&journal)
        );
    }
    let order: Vec<String> = journal
        .iter()
        .filter(|seen| messages.iter().any(|m| m.id.as_str() == seen.id))
        .map(|seen| seen.id.clone())
        .collect();
    assert_eq!(
        order,
        message_ids(messages),
        "{}: the model saw them out of order",
        S::HARNESS
    );
}

/// An idle agent's first message opens a turn and is committed.
pub fn an_idle_agent_takes_a_prompt<S: Subject>() {
    let mut world = World::<S>::new();
    let first = world.prompt();
    let journal = world.fixture.journal();
    assert_eq!(ids(&journal), [first.id.to_string()], "{}", S::HARNESS);
    assert_eq!(journal[0].kind, "prompt", "{}", S::HARNESS);
    world.fixture.command("finish");
    world.pump_until("the turn ends", |w| w.harness_idle());
    assert_eq!(world.fixture.launches(), 1);
}

/// Five messages that reach a busy agent: at `next-step` they are all
/// written at once and committed only once the running turn's next step
/// consumed them, together and in order; at `mid-turn` the transport takes
/// them at once; at `turn-end` the running turn never sees them.
pub fn five_messages_reach_a_busy_turn<S: Subject>() {
    let mut world = World::<S>::new();
    let first = world.prompt();
    let five = world.receive(5);
    let last = five.last().unwrap().seq;
    let level = world.delivery_level().level;
    match level {
        DeliveryLevel::TurnEnd => {
            world.pump_for(Duration::from_millis(300));
            assert_eq!(
                ids(&world.fixture.journal()),
                [first.id.to_string()],
                "{}: a turn-end harness put mail into a running turn",
                S::HARNESS
            );
            world.drain(last);
        }
        DeliveryLevel::MidTurn | DeliveryLevel::NextStep => {
            let expected = message_ids(&five);
            world.pump_until("all five reach the vendor", |w| {
                let wire = w.fixture.wire();
                expected.iter().all(|id| wire.contains(id))
            });
            if level == DeliveryLevel::NextStep {
                world.pump_for(Duration::from_millis(100));
                assert_eq!(
                    world.cursor(),
                    first.seq,
                    "{}: committed before the vendor proved consumption",
                    S::HARNESS
                );
            } else {
                world.pump_until("the transport took all five", |w| w.cursor() == last);
            }
            world.fixture.command("step");
            world.pump_until("all five are committed", |w| w.cursor() == last);
            let journal = world.fixture.journal();
            let steered: Vec<&Seen> = journal.iter().skip(1).collect();
            assert_eq!(
                steered
                    .iter()
                    .map(|seen| seen.id.clone())
                    .collect::<Vec<_>>(),
                expected,
                "{}: the running turn did not take the five in order",
                S::HARNESS
            );
            if level == DeliveryLevel::NextStep {
                let first_turn = journal[0].turn.clone();
                for seen in &steered {
                    assert_eq!(seen.kind, "steer", "{}", S::HARNESS);
                    assert_eq!(
                        seen.turn,
                        first_turn,
                        "{}: not the running turn",
                        S::HARNESS
                    );
                    assert_eq!(seen.step, steered[0].step, "{}: not one batch", S::HARNESS);
                }
            }
            world.drain(last);
        }
    }
    let mut all = vec![first];
    all.extend(five);
    assert_once_in_order(&world, &all);
    assert_eq!(world.fixture.launches(), 1);
}

/// Messages that arrive between steps reach the model in mailbox order.
pub fn mail_keeps_its_order_across_steps<S: Subject>() {
    let mut world = World::<S>::new();
    let mut all = vec![world.prompt()];
    for batch in [2usize, 1, 2] {
        let more = world.receive(batch);
        let wanted = message_ids(&more);
        if world.delivery_level().level != DeliveryLevel::TurnEnd {
            world.pump_until("the batch reaches the vendor", |w| {
                let wire = w.fixture.wire();
                wanted.iter().all(|id| wire.contains(id))
            });
            world.fixture.command("step");
        }
        all.extend(more);
    }
    let last = all.last().unwrap().seq;
    world.drain(last);
    assert_once_in_order(&world, &all);
}

/// The vendor dies after its step consumed two of five pending messages.
/// Nothing is lost and nothing is applied twice: at `next-step` the three
/// it never consumed reach the next process as input of their own; at
/// `mid-turn`, which committed them on acceptance, they reach it inside the
/// recovery note for the turn the crash cut short.
pub fn a_crash_after_partial_consumption_loses_nothing<S: Subject>() {
    let mut world = World::<S>::new();
    let first = world.prompt();
    let five = world.receive(5);
    let last = five.last().unwrap().seq;
    let level = world.delivery_level().level;
    if level == DeliveryLevel::TurnEnd {
        world.pump_for(Duration::from_millis(200));
        world.fixture.command("crash");
    } else {
        let expected = message_ids(&five);
        world.pump_until("all five reach the vendor", |w| {
            let wire = w.fixture.wire();
            expected.iter().all(|id| wire.contains(id))
        });
        world.fixture.command("step-2");
    }
    world.pump_until_down();
    world.restart();
    world.drain(last);
    let journal = world.fixture.journal();
    let counts = applied(&journal);
    let mut all = vec![first];
    all.extend(five);
    for (index, message) in all.iter().enumerate() {
        let raw = counts.get(message.id.as_str()).copied().unwrap_or(0);
        assert!(
            raw <= 1,
            "{}: {} applied {raw} times; journal {:?}",
            S::HARNESS,
            message.id,
            ids(&journal)
        );
        // The prompt and the two consumed before the crash went in raw.
        let consumed_before = index <= 2;
        if raw == 0 {
            assert!(
                level == DeliveryLevel::MidTurn && !consumed_before,
                "{}: {} was lost; journal {:?}",
                S::HARNESS,
                message.id,
                ids(&journal)
            );
            assert!(
                journal
                    .iter()
                    .any(|seen| seen.kind == "note" && seen.text.contains(message.id.as_str())),
                "{}: {} is in no recovery note either",
                S::HARNESS,
                message.id
            );
        }
    }
}

/// The host restarts after the vendor consumed messages but before their
/// commit was recorded. An idempotent harness recognises them when they are
/// handed over again and commits them without applying them twice.
pub fn a_host_restart_redelivers_without_applying_twice<S: Subject>() {
    let mut world = World::<S>::new();
    let first = world.prompt();
    let more = world.receive(2);
    let last = more.last().unwrap().seq;
    world.drain(last);
    if !world.delivery_level().idempotent {
        return;
    }
    world.delivery.store().rewind_for_test(&world.agent, 0);
    world.restart();
    world.drain(last);
    let mut all = vec![first];
    all.extend(more);
    assert_once_in_order(&world, &all);
}

/// A message offered again to the process that already took it (its commit
/// was lost) is not written again.
pub fn an_offer_twice_reaches_the_vendor_once<S: Subject>() {
    let mut world = World::<S>::new();
    let first = world.prompt();
    world.delivery.store().rewind_for_test(&world.agent, 0);
    world.pump_until("the prompt is committed again", |w| w.cursor() == first.seq);
    world.pump_for(Duration::from_millis(100));
    let wire = world.fixture.wire();
    assert_eq!(
        wire.iter().filter(|id| **id == first.id.as_str()).count(),
        1,
        "{}: written twice; wire {wire:?}",
        S::HARNESS
    );
    world.drain(first.seq);
    assert_once_in_order(&world, &[first]);
}

/// The vendor refuses a message that reaches its running turn. The message
/// stays in the mailbox, the process is not restarted, and the message
/// reaches the model once, later.
pub fn a_refused_message_waits_without_a_restart<S: Subject>() {
    let mut world = World::<S>::new();
    let first = world.prompt();
    world.fixture.command("refuse");
    let second = world.receive(1).remove(0);
    world.pump_for(Duration::from_millis(400));
    assert_eq!(world.fixture.launches(), 1, "{}: restarted", S::HARNESS);
    world.drain(second.seq);
    assert_once_in_order(&world, &[first, second]);
    assert_eq!(world.fixture.launches(), 1, "{}: restarted", S::HARNESS);
}

/// The vendor answers every input late. Delivery neither fails nor
/// restarts the process nor writes a message twice.
pub fn a_slow_answer_neither_fails_nor_duplicates<S: Subject>() {
    let mut world = World::<S>::new();
    world.fixture.set_slow(1_500);
    let first = world.receive(1).remove(0);
    world.pump_until("the slow prompt is committed", |w| w.cursor() == first.seq);
    let second = world.receive(1).remove(0);
    if world.delivery_level().level != DeliveryLevel::TurnEnd {
        world.pump_until("the steer reaches the vendor", |w| {
            w.fixture.wire().contains(&second.id.to_string())
        });
        world.fixture.command("step");
    }
    world.drain(second.seq);
    world.fixture.clear_slow();
    assert_once_in_order(&world, &[first, second]);
    assert_eq!(world.fixture.launches(), 1, "{}: restarted", S::HARNESS);
}

/// Every scenario as a `#[test]` for one [`Subject`].
#[macro_export]
macro_rules! conformance_suite {
    ($subject:ty) => {
        #[test]
        fn an_idle_agent_takes_a_prompt() {
            $crate::conformance::an_idle_agent_takes_a_prompt::<$subject>();
        }
        #[test]
        fn five_messages_reach_a_busy_turn() {
            $crate::conformance::five_messages_reach_a_busy_turn::<$subject>();
        }
        #[test]
        fn mail_keeps_its_order_across_steps() {
            $crate::conformance::mail_keeps_its_order_across_steps::<$subject>();
        }
        #[test]
        fn a_crash_after_partial_consumption_loses_nothing() {
            $crate::conformance::a_crash_after_partial_consumption_loses_nothing::<$subject>();
        }
        #[test]
        fn a_host_restart_redelivers_without_applying_twice() {
            $crate::conformance::a_host_restart_redelivers_without_applying_twice::<$subject>();
        }
        #[test]
        fn an_offer_twice_reaches_the_vendor_once() {
            $crate::conformance::an_offer_twice_reaches_the_vendor_once::<$subject>();
        }
        #[test]
        fn a_refused_message_waits_without_a_restart() {
            $crate::conformance::a_refused_message_waits_without_a_restart::<$subject>();
        }
        #[test]
        fn a_slow_answer_neither_fails_nor_duplicates() {
            $crate::conformance::a_slow_answer_neither_fails_nor_duplicates::<$subject>();
        }
    };
}
