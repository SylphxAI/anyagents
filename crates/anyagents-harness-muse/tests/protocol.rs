//! The Muse adapter against a scripted `muse serve` (`fake_muse`).

use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryLevel, Harness, HarnessError, OutboundMessage, TurnState,
};
use anyagents_harness_muse::{MuseHarness, command_id};
use tempfile::TempDir;

const PATIENCE: Duration = Duration::from_secs(10);

fn msg(id: &str, seq: i64) -> OutboundMessage {
    OutboundMessage {
        id: id.to_string(),
        seq,
        text: "hello".to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    }
}

/// One scripted vendor: its control directory, command table and log.
struct Vendor {
    dir: TempDir,
}

impl Vendor {
    fn new() -> Self {
        Self {
            dir: TempDir::new().unwrap(),
        }
    }

    fn start(&self, mode: &str, session: Option<&str>) -> MuseHarness {
        let log = self.dir.path().join("log");
        MuseHarness::start(
            env!("CARGO_BIN_EXE_fake_muse"),
            session,
            &[
                ("ANYAGENTS_FAKE_MODE", mode),
                ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
                ("ANYAGENTS_FAKE_MUSE_DIR", self.dir.path().to_str().unwrap()),
            ],
        )
        .expect("start")
    }

    /// Ask the vendor to act (`step`, `finish`) and wait until it has.
    fn command(&self, name: &str) {
        let path = self.dir.path().join(name);
        std::fs::write(&path, "").unwrap();
        let deadline = Instant::now() + PATIENCE;
        while path.exists() {
            assert!(
                Instant::now() < deadline,
                "the vendor never acted on {name}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("log")).unwrap_or_default()
    }

    /// How many `turn/start` requests carried `command`.
    fn writes_of(&self, command: &str) -> usize {
        self.log()
            .lines()
            .filter(|line| line.contains("\"turn/start\"") && line.contains(command))
            .count()
    }
}

/// Offer `message` the way the delivery loop does, until it answers `want`.
fn until(harness: &mut MuseHarness, message: &OutboundMessage, want: DeliverOutcome) {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let got = harness.deliver(message).expect("deliver");
        if got == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} answered {got:?}, never {want:?}",
            message.id
        );
        thread::sleep(Duration::from_millis(5));
    }
}

const CONSUMED: DeliverOutcome = DeliverOutcome::Accepted {
    turn_finished: false,
};
const FINISHED: DeliverOutcome = DeliverOutcome::Accepted {
    turn_finished: true,
};

#[test]
fn a_prompt_is_in_flight_until_its_turn_records_it_and_finished_with_the_turn() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("hold", None);
    assert_eq!(harness.session_id().as_deref(), Some("sess-from-vendor"));
    let delivery = harness.health().delivery;
    assert_eq!(delivery.level, DeliveryLevel::NextStep);
    assert!(delivery.idempotent);
    assert_eq!(delivery.peer.as_deref(), Some("muse 1.4.18-test"));
    let m1 = msg("m1", 1);
    assert!(matches!(
        harness.deliver(&m1).unwrap(),
        DeliverOutcome::InFlight | DeliverOutcome::Accepted { .. }
    ));
    until(&mut harness, &m1, CONSUMED);
    assert!(harness.in_turn());
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    vendor.command("finish");
    until(&mut harness, &m1, FINISHED);
    assert_eq!(harness.health().turn_state, TurnState::Idle);
    assert_eq!(harness.turns_served(), 1);
    let usage = harness.served_usage();
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens
        ),
        (11, 7, 3)
    );
    // Offered many times, written once, as one steer-or-start command whose
    // id is the message's own; the host held the tool approval.
    assert_eq!(harness.written_ids(), ["m1"]);
    assert_eq!(vendor.writes_of(&command_id("m1", 0)), 1);
    let log = vendor.log();
    assert_eq!(log.matches("\"ifBusy\":\"steer\"").count(), 1, "{log}");
    assert!(
        log.contains("\"id\":900"),
        "the approval was answered: {log}"
    );
}

#[test]
fn steers_wait_for_the_next_step_and_finish_with_their_turn() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("hold", None);
    let (m1, m2, m3) = (msg("m1", 1), msg("m2", 2), msg("m3", 3));
    until(&mut harness, &m1, CONSUMED);
    // The second is not written until the first is answered, so a refusal
    // can never let a later message overtake an earlier one.
    let deadline = Instant::now() + PATIENCE;
    while harness.written_ids().len() < 3 {
        for message in [&m2, &m3] {
            let got = harness.deliver(message).unwrap();
            assert!(
                matches!(got, DeliverOutcome::InFlight | DeliverOutcome::Deferred),
                "{got:?} before any step"
            );
        }
        assert!(Instant::now() < deadline, "{:?}", harness.written_ids());
        thread::sleep(Duration::from_millis(5));
    }
    thread::sleep(Duration::from_millis(100));
    assert_eq!(harness.deliver(&m2).unwrap(), DeliverOutcome::InFlight);
    assert_eq!(harness.deliver(&m3).unwrap(), DeliverOutcome::InFlight);
    vendor.command("step");
    until(&mut harness, &m2, CONSUMED);
    until(&mut harness, &m3, CONSUMED);
    assert_eq!(harness.turns_started(), 1, "a steer is not a new turn");
    vendor.command("finish");
    for message in [&m1, &m2, &m3] {
        until(&mut harness, message, FINISHED);
    }
    assert_eq!(harness.written_ids(), ["m1", "m2", "m3"]);
    assert_eq!(vendor.log().matches("\"turn/start\"").count(), 3);
}

#[test]
fn a_steer_its_turn_ended_on_finishes_in_the_successor_turn() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("hold", None);
    let (m1, m2, m3) = (msg("m1", 1), msg("m2", 2), msg("m3", 3));
    until(&mut harness, &m1, CONSUMED);
    until(&mut harness, &m2, DeliverOutcome::InFlight);
    // m3 is written only once m2's answer is in: m2 waits in the turn's inbox.
    until(&mut harness, &m3, DeliverOutcome::InFlight);
    // The turn ends before its next step: the vendor carries the steer into a
    // turn named after its command, and that turn's record is its proof. The
    // turn that ended does not finish it.
    vendor.command("finish");
    until(&mut harness, &m1, FINISHED);
    until(&mut harness, &m2, CONSUMED);
    assert!(harness.in_turn(), "the successor runs");
    assert_eq!(harness.turns_started(), 2);
    vendor.command("step");
    until(&mut harness, &m3, CONSUMED);
    vendor.command("finish");
    until(&mut harness, &m2, FINISHED);
    until(&mut harness, &m3, FINISHED);
    assert_eq!(harness.turns_served(), 2);
    assert_eq!(vendor.writes_of(&command_id("m2", 0)), 1);
    assert_eq!(vendor.writes_of(&command_id("m3", 0)), 1);
}

#[test]
fn a_rejected_steer_waits_for_its_turn_and_goes_again_under_the_next_command_id() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("refuse_steer", None);
    let (m1, m2) = (msg("m1", 1), msg("m2", 2));
    until(&mut harness, &m1, CONSUMED);
    until(&mut harness, &m2, DeliverOutcome::Deferred);
    // Deferred while the turn runs: neither written again nor a failure.
    for _ in 0..20 {
        assert_eq!(harness.deliver(&m2).unwrap(), DeliverOutcome::Deferred);
    }
    assert!(harness.health().process_alive);
    assert_eq!(harness.written_ids(), ["m1", "m2"]);
    vendor.command("finish");
    until(&mut harness, &m2, CONSUMED);
    assert_eq!(harness.written_ids(), ["m1", "m2", "m2"]);
    // The rejection is the vendor's answer to that command for good, so the
    // next write is a command of its own.
    assert_eq!(vendor.writes_of(&command_id("m2", 0)), 1);
    assert_eq!(vendor.writes_of(&command_id("m2", 1)), 1);
    assert_eq!(harness.turns_started(), 2);
}

#[test]
fn an_unknown_outcome_is_written_again_under_the_same_command_id() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("interrupt_once", None);
    let m1 = msg("m1", 1);
    until(&mut harness, &m1, CONSUMED);
    assert_eq!(harness.written_ids(), ["m1", "m1"]);
    assert_eq!(vendor.writes_of(&command_id("m1", 0)), 2);
    assert_eq!(harness.turns_started(), 1);
}

#[test]
fn a_command_an_earlier_process_wrote_is_a_duplicate() {
    let vendor = Vendor::new();
    let m1 = msg("m1", 1);
    let session = {
        let mut first = vendor.start("finish", None);
        until(&mut first, &m1, FINISHED);
        first.session_id().unwrap()
    };
    let mut second = vendor.start("finish", Some(&session));
    until(&mut second, &m1, DeliverOutcome::Duplicate);
    // The vendor replayed the first process's answer and started nothing.
    assert_eq!(second.turns_started(), 0);
    assert_eq!(second.turns_served(), 0);
    assert_eq!(vendor.writes_of(&command_id("m1", 0)), 2);
    assert_eq!(second.health().turn_state, TurnState::Idle);
}

#[test]
fn a_rate_limited_start_is_a_wait_with_its_reset_time() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("quota", None);
    let m1 = msg("m1", 1);
    until(
        &mut harness,
        &m1,
        DeliverOutcome::Quota {
            reset_at_ms: 1_700_000_000_000,
        },
    );
    assert_eq!(harness.written_ids(), ["m1"]);
    assert_eq!(harness.health().quota_reset_at_ms, Some(1_700_000_000_000));
    harness.clear_quota();
    assert_eq!(harness.health().quota_reset_at_ms, None);
}

#[test]
fn a_session_the_process_cannot_write_fails_delivery() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("unloaded", None);
    let m1 = msg("m1", 1);
    let deadline = Instant::now() + PATIENCE;
    let err = loop {
        match harness.deliver(&m1) {
            Ok(DeliverOutcome::InFlight) => {}
            Ok(other) => panic!("{other:?}"),
            Err(err) => break err,
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(5));
    };
    assert!(
        matches!(&err, HarnessError::Failed(reason) if reason.contains("-32024")),
        "{err:?}"
    );
}

#[test]
fn the_launch_is_muse_serve_and_the_first_line_is_the_id() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("hold", Some("sess-1"));
    assert_eq!(harness.session_id().as_deref(), Some("sess-1"));
    let m1 = msg("m1", 1);
    until(&mut harness, &m1, CONSUMED);
    let text = vendor.log();
    assert!(text.contains("argv:serve"), "{text}");
    assert!(text.contains("anyagents-message:m1\\nhello"), "{text}");
}

#[test]
fn a_turn_completed_before_its_start_and_answer_leaves_the_harness_idle() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("early_complete", None);
    let m1 = msg("m1", 1);
    until(&mut harness, &m1, FINISHED);
    assert!(harness.wait_until(PATIENCE, |h| h.turns_started() == 1));
    assert_eq!(harness.health().turn_state, TurnState::Idle);
    assert!(!harness.in_turn());
    assert_eq!(harness.turns_served(), 1);
}

#[test]
fn session_resume_already_in_use_retries_until_lock_releases() {
    let dir = TempDir::new().unwrap();
    let log = dir.path().join("log");
    let lock_file = dir.path().join("session.lock");
    std::fs::write(&lock_file, "locked").unwrap();
    let lock_releaser = lock_file.clone();
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(350));
        let _ = std::fs::remove_file(lock_releaser);
    });
    let harness = MuseHarness::start(
        env!("CARGO_BIN_EXE_fake_muse"),
        Some("sess-lock-1"),
        &[
            ("ANYAGENTS_FAKE_MODE", "finish"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_MUSE_LOCK_FILE", lock_file.to_str().unwrap()),
        ],
    )
    .expect("session/resume should succeed once transient lock clears");
    handle.join().unwrap();
    assert_eq!(harness.session_id().as_deref(), Some("sess-lock-1"));
}

#[test]
fn session_resume_fails_closed_when_session_lock_stays_held() {
    let dir = TempDir::new().unwrap();
    let log = dir.path().join("log");
    let lock_file = dir.path().join("session.lock");
    std::fs::write(&lock_file, "locked").unwrap();
    let err = match MuseHarness::start(
        env!("CARGO_BIN_EXE_fake_muse"),
        Some("sess-lock-held"),
        &[
            ("ANYAGENTS_FAKE_MODE", "finish"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_MUSE_LOCK_FILE", lock_file.to_str().unwrap()),
        ],
    ) {
        Ok(_) => panic!("held session lock must fail closed"),
        Err(err) => err,
    };
    let text = err.to_string().to_ascii_lowercase();
    assert!(
        text.contains("already in use"),
        "unexpected error for held lock: {err}"
    );
}
