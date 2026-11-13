//! The Muse adapter against a scripted `muse serve` (`fake_muse`).

use std::thread;
use std::time::{Duration, Instant};

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryLevel, Harness, HarnessError, OutboundMessage, Proof,
    TurnState,
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
        self.start_with(env!("CARGO_BIN_EXE_fake_muse"), mode, session, &[])
    }

    fn start_with(
        &self,
        program: &str,
        mode: &str,
        session: Option<&str>,
        extra: &[(&str, &str)],
    ) -> MuseHarness {
        let log = self.dir.path().join("log");
        let mut environment = vec![
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_MUSE_DIR", self.dir.path().to_str().unwrap()),
        ];
        environment.extend_from_slice(extra);
        MuseHarness::start(program, session, &environment).expect("start")
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
            .filter(|line| {
                line.starts_with("in:") && line.contains("\"turn/start\"") && line.contains(command)
            })
            .count()
    }

    /// The answers the vendor printed that name `command`.
    fn answers_of(&self, command: &str) -> Vec<String> {
        self.log()
            .lines()
            .filter(|line| line.starts_with("out:") && line.contains(command))
            .map(str::to_string)
            .collect()
    }
}

/// Wait until `done` holds.
fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !done() {
        assert!(Instant::now() < deadline, "never {what}");
        thread::sleep(Duration::from_millis(5));
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
    assert_eq!(delivery.peer.as_deref(), Some("muse 1.4.20-test"));
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
        log.lines()
            .any(|line| line.starts_with("in:") && line.contains("\"id\":900")),
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
fn a_refused_steer_waits_for_its_turn_and_goes_again_under_the_same_command_id() {
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
    // The queue refused it before admission and recorded nothing, so the
    // next write is the same command.
    assert_eq!(vendor.writes_of(&command_id("m2", 0)), 2);
    assert_eq!(harness.turns_started(), 2);
}

#[test]
fn a_busy_refusal_goes_again_under_the_next_id_and_every_process_walks_the_same_chain() {
    let vendor = Vendor::new();
    let (m1, m2) = (msg("m1", 1), msg("m2", 2));
    let session = {
        let mut first = vendor.start("busy_steer", None);
        until(&mut first, &m1, CONSUMED);
        // Refused at its deadline: recorded against its first id.
        until(&mut first, &m2, DeliverOutcome::Deferred);
        for _ in 0..20 {
            assert_eq!(first.deliver(&m2).unwrap(), DeliverOutcome::Deferred);
        }
        assert_eq!(vendor.writes_of(&command_id("m2", 0)), 1);
        let session = first.session_id().unwrap();
        first.stop();
        session
    };
    // A new process starts the chain at attempt 0 again: Muse replays the
    // recorded refusal, and the message goes on under attempt 1, which Muse
    // never saw. Nothing is admitted twice.
    let mut second = vendor.start("hold", Some(&session));
    until(&mut second, &m2, CONSUMED);
    assert_eq!(vendor.writes_of(&command_id("m2", 0)), 2);
    assert_eq!(vendor.writes_of(&command_id("m2", 1)), 1);
    assert_eq!(second.written_ids(), ["m2", "m2"]);
    let session = second.session_id().unwrap();
    second.stop();
    // The record holds it now, under its second id: never written again.
    let mut third = vendor.start("hold", Some(&session));
    assert_eq!(third.deliver(&m2).unwrap(), DeliverOutcome::Duplicate);
    assert!(third.written_ids().is_empty());
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
fn a_message_the_session_record_holds_is_a_duplicate_and_never_written_again() {
    let vendor = Vendor::new();
    let m1 = msg("m1", 1);
    let session = {
        let mut first = vendor.start("finish", None);
        until(&mut first, &m1, FINISHED);
        first.session_id().unwrap()
    };
    let mut second = vendor.start("finish", Some(&session));
    // Proved by the record the resume served, before anything is written.
    assert_eq!(second.proven("m1"), Some(Proof::Duplicate));
    assert_eq!(second.deliver(&m1).unwrap(), DeliverOutcome::Duplicate);
    assert_eq!(vendor.writes_of(&command_id("m1", 0)), 1);
    assert!(second.written_ids().is_empty());
    assert_eq!(second.turns_started(), 0);
    assert_eq!(second.health().turn_state, TurnState::Idle);
    assert!(
        !vendor.log().contains("excludeItems"),
        "the resume asks for the history"
    );
}

#[test]
fn a_record_over_the_history_budget_is_paged() {
    let vendor = Vendor::new();
    let messages: Vec<_> = (1..=5).map(|n| msg(&format!("m{n}"), n)).collect();
    let session = {
        let mut first = vendor.start("finish", None);
        for message in &messages {
            until(&mut first, message, FINISHED);
        }
        first.session_id().unwrap()
    };
    let mut second = vendor.start_with(
        env!("CARGO_BIN_EXE_fake_muse"),
        "finish",
        Some(&session),
        &[("ANYAGENTS_FAKE_MUSE_HISTORY", "paged")],
    );
    for message in &messages {
        assert_eq!(second.deliver(message).unwrap(), DeliverOutcome::Duplicate);
        assert_eq!(vendor.writes_of(&command_id(&message.id, 0)), 1);
    }
    let log = vendor.log();
    assert!(log.contains("\"view/page\""), "{log}");
}

#[test]
fn a_replayed_ack_without_queued_at_settles_by_its_item() {
    let vendor = Vendor::new();
    let (m1, m2) = (msg("m1", 1), msg("m2", 2));
    let session = {
        let mut first = vendor.start("hold", None);
        until(&mut first, &m1, CONSUMED);
        until(&mut first, &m2, DeliverOutcome::InFlight);
        eventually("m2 is admitted", || {
            vendor.answers_of(&command_id("m2", 0)).len() == 1
        });
        let session = first.session_id().unwrap();
        // The process goes away with m2 admitted into its turn, unconsumed.
        first.stop();
        session
    };
    let mut second = vendor.start("hold", Some(&session));
    assert_eq!(second.deliver(&m1).unwrap(), DeliverOutcome::Duplicate);
    // m2 is not in the record: written again under its own id, the vendor
    // replays the first process's ack, without `queuedAtUs`.
    until(&mut second, &m2, DeliverOutcome::InFlight);
    eventually("the replayed ack is read", || {
        vendor.answers_of(&command_id("m2", 0)).len() == 2
    });
    let answers = vendor.answers_of(&command_id("m2", 0));
    assert!(
        answers.iter().all(|answer| !answer.contains("queuedAtUs")),
        "{answers:?}"
    );
    for _ in 0..20 {
        assert_eq!(second.deliver(&m2).unwrap(), DeliverOutcome::InFlight);
    }
    // The vendor carries the waiting steer into a successor turn: its item
    // settles it.
    vendor.command("carry");
    until(&mut second, &m2, CONSUMED);
    assert_eq!(vendor.writes_of(&command_id("m2", 0)), 2);
}

#[test]
fn a_conflicting_payload_under_the_same_id_waits_for_the_first_write() {
    let vendor = Vendor::new();
    let (m1, m2) = (msg("m1", 1), msg("m2", 2));
    let session = {
        let mut first = vendor.start("hold", None);
        until(&mut first, &m1, CONSUMED);
        until(&mut first, &m2, DeliverOutcome::InFlight);
        eventually("m2 is admitted", || {
            vendor.answers_of(&command_id("m2", 0)).len() == 1
        });
        let session = first.session_id().unwrap();
        first.stop();
        session
    };
    // The same mailbox message, written by a host that composes it
    // differently: Muse refuses the id as held by another payload.
    let mut changed = m2.clone();
    changed.text = "hello, composed anew".to_string();
    let mut second = vendor.start("hold", Some(&session));
    until(&mut second, &changed, DeliverOutcome::InFlight);
    eventually("the conflict is read", || {
        vendor
            .answers_of(&command_id("m2", 0))
            .iter()
            .any(|answer| answer.contains("command_id_conflict"))
    });
    for _ in 0..20 {
        assert_eq!(second.deliver(&changed).unwrap(), DeliverOutcome::InFlight);
    }
    // The first write is the one the vendor holds, and its item settles it.
    vendor.command("carry");
    until(&mut second, &changed, CONSUMED);
    assert_eq!(second.written_ids(), ["m2"]);
    assert_eq!(vendor.writes_of(&command_id("m2", 0)), 2);
}

#[test]
fn a_refusal_for_good_is_rejected_and_answered_again_without_a_write() {
    for (mode, code) in [
        ("too_large", "-32002"),
        ("invalid_params", "-32602"),
        ("rejected", "-32030"),
    ] {
        let vendor = Vendor::new();
        let mut harness = vendor.start(mode, None);
        let m1 = msg("m1", 1);
        let deadline = Instant::now() + PATIENCE;
        let reason = loop {
            match harness.deliver(&m1).expect("no restart") {
                DeliverOutcome::InFlight => {}
                DeliverOutcome::Rejected { reason, .. } => break reason,
                other => panic!("{mode}: {other:?}"),
            }
            assert!(Instant::now() < deadline, "{mode}");
            thread::sleep(Duration::from_millis(5));
        };
        assert!(reason.contains(code), "{mode}: {reason}");
        assert!(
            !reason.contains("m1") && !reason.contains("1024"),
            "{mode}: the reason names nothing of the message: {reason}"
        );
        for _ in 0..5 {
            assert_eq!(
                harness.deliver(&m1).unwrap(),
                DeliverOutcome::Rejected {
                    message_specific: reason.contains("-32002"),
                    reason: reason.clone(),
                }
            );
        }
        assert_eq!(
            harness.proven("m1"),
            None,
            "{mode}: a refusal proves nothing"
        );
        assert_eq!(vendor.writes_of(&command_id("m1", 0)), 1, "{mode}");
        assert!(harness.health().process_alive, "{mode}");
    }
}

#[test]
fn a_proof_printed_just_before_a_stop_is_answered_after_it() {
    let vendor = Vendor::new();
    let mut harness = vendor.start("hold", None);
    let (m1, m2) = (msg("m1", 1), msg("m2", 2));
    until(&mut harness, &m1, CONSUMED);
    until(&mut harness, &m2, DeliverOutcome::InFlight);
    vendor.command("step");
    harness.stop();
    assert!(!harness.health().process_alive);
    assert_eq!(
        harness.proven("m2"),
        Some(Proof::Taken {
            turn_finished: false
        })
    );
    // A proof is answered before liveness; anything else is dead.
    assert_eq!(harness.deliver(&m2).unwrap(), CONSUMED);
    assert_eq!(harness.deliver(&msg("m3", 3)), Err(HarnessError::Dead));
}

/// A wrapper around the fake that outlives it: once the fake exits it closes
/// its stdin, so the host's next write fails, while it keeps stdout open
/// until the test lets it print one more line.
#[cfg(unix)]
fn deaf_wrapper(dir: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let path = dir.join("muse-wrapper.sh");
    let script = format!(
        "#!/bin/sh\n\
         '{fake}' \"$@\"\n\
         exec 0<&-\n\
         : > '{dir}/deaf'\n\
         while [ ! -e '{dir}/speak' ]; do sleep 0.01; done\n\
         printf '%s\\n' \"$PROOF_LINE\"\n",
        fake = env!("CARGO_BIN_EXE_fake_muse"),
        dir = dir.display(),
    );
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[cfg(unix)]
#[test]
fn a_process_is_dead_only_once_its_stdout_ended() {
    let vendor = Vendor::new();
    let wrapper_dir = TempDir::new().unwrap();
    let wrapper = deaf_wrapper(wrapper_dir.path());
    // The line the vendor prints after its stdin is gone: m2's proof.
    let proof = serde_json::json!({"jsonrpc": "2.0", "method": "item/completed",
        "params": {"sessionId": "sess-from-vendor", "item": {"kind": "userMessage",
            "turnId": command_id("m1", 0), "commandId": command_id("m2", 0), "steered": true,
            "text": "anyagents-message:m2\nhello"}}})
    .to_string();
    let mut harness = vendor.start_with(
        wrapper.to_str().unwrap(),
        "hold",
        None,
        &[("PROOF_LINE", proof.as_str())],
    );
    let (m1, m2, m3) = (msg("m1", 1), msg("m2", 2), msg("m3", 3));
    until(&mut harness, &m1, CONSUMED);
    until(&mut harness, &m2, DeliverOutcome::InFlight);
    std::fs::write(vendor.dir.path().join("exit"), "").unwrap();
    eventually("the vendor's stdin closes", || {
        wrapper_dir.path().join("deaf").exists()
    });
    // The write fails, and the process is still not dead: its stdout is
    // open, and a proof may still come.
    assert_eq!(harness.deliver(&m3).unwrap(), DeliverOutcome::Deferred);
    assert_eq!(harness.deliver(&m2).unwrap(), DeliverOutcome::InFlight);
    assert!(harness.health().process_alive);
    assert_eq!(harness.proven("m2"), None);
    std::fs::write(wrapper_dir.path().join("speak"), "").unwrap();
    eventually("the reader reaches the end", || {
        !harness.health().process_alive
    });
    assert_eq!(
        harness.proven("m2"),
        Some(Proof::Taken {
            turn_finished: false
        })
    );
    assert_eq!(harness.deliver(&m2).unwrap(), CONSUMED);
    assert_eq!(harness.deliver(&m3), Err(HarnessError::Dead));
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
