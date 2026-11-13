//! The delivery conformance suite (`anyagents_invariants::conformance`)
//! against every harness adapter that lives in a crate of its own. Grok and
//! dsh are part of the `anyagents` binary and run the same suite there.
#![cfg(unix)]

use std::time::Duration;

use anyagents_invariants::conformance::{Fixture, Subject};

/// How long a scripted vendor may take to announce itself.
const READY: Duration = Duration::from_secs(30);

mod spiron {
    use super::*;
    use anyagents_harness_spiron::{SpironHarness, SpironLaunch};

    fn launch(fixture: &Fixture, session: Option<&str>) -> SpironHarness {
        SpironHarness::start(
            SpironLaunch {
                program: fixture.program().to_path_buf(),
                args: Vec::new(),
                environment: Vec::new(),
                work_dir: fixture.dir().to_path_buf(),
                session: session.map(str::to_string),
                state_dir: fixture.dir().to_path_buf(),
            },
            READY,
        )
        .expect("spiron starts")
    }

    /// A peer whose `ready` names `steer_consumed` and `idempotent_input`.
    pub struct Spiron;

    impl Subject for Spiron {
        type Harness = SpironHarness;
        const HARNESS: &'static str = "spiron";
        const PROTOCOL: &'static str = "spiron";
        fn launch(fixture: &Fixture, session: Option<&str>) -> SpironHarness {
            launch(fixture, session)
        }
        fn session(harness: &SpironHarness) -> Option<String> {
            harness.session_id()
        }
    }

    mod capable {
        anyagents_invariants::conformance_suite!(super::Spiron);
    }

    /// A peer from before those capabilities.
    pub struct OlderSpiron;

    impl Subject for OlderSpiron {
        type Harness = SpironHarness;
        const HARNESS: &'static str = "spiron";
        const PROTOCOL: &'static str = "spiron-v0";
        fn launch(fixture: &Fixture, session: Option<&str>) -> SpironHarness {
            launch(fixture, session)
        }
        fn session(harness: &SpironHarness) -> Option<String> {
            harness.session_id()
        }
    }

    mod older {
        anyagents_invariants::conformance_suite!(super::OlderSpiron);
    }

    use anyagents_invariants::conformance::{World, ids};

    /// Have the scripted Spiron refuse the next input with `code`.
    fn refuse_next(world: &World<Spiron>, code: &str) {
        std::fs::write(world.fixture.dir().join("reject"), code).expect("arm the refusal");
    }

    /// A code no offer clears parks the message as rejected, past the
    /// cursor, with its sender told; the process is not restarted, nothing
    /// reaches the model, and the mail behind it is delivered.
    fn parked_for_good(code: &str) {
        let mut world = World::<Spiron>::new();
        let first = world.prompt();
        refuse_next(&world, code);
        let refused = world.receive(1).remove(0);
        world.pump_until("the refused message is parked", |w| {
            w.cursor() == refused.seq
        });
        let rejections = world.delivery.take_rejections();
        assert_eq!(rejections.len(), 1, "{rejections:?}");
        assert_eq!(rejections[0].message_id, refused.id.as_str());
        assert!(rejections[0].reason.contains(code), "{rejections:?}");
        let rejected = world
            .delivery
            .store()
            .rejected_messages(&world.agent)
            .expect("rejected");
        assert_eq!(rejected.len(), 1);
        let after = world.receive(1).remove(0);
        world.pump_until("the mail behind it reaches the vendor", |w| {
            w.fixture.wire().contains(&after.id.to_string())
        });
        world.fixture.command("step");
        world.pump_until("the mail behind it is committed", |w| {
            w.cursor() == after.seq
        });
        world.drain(after.seq);
        let journal = world.fixture.journal();
        assert_eq!(
            ids(&journal),
            [first.id.to_string(), after.id.to_string()],
            "{journal:?}"
        );
        assert_eq!(world.fixture.launches(), 1, "restarted");
        assert!(world.delivery.take_rejections().is_empty());
    }

    #[test]
    fn an_input_spiron_cannot_take_is_parked_without_a_restart() {
        parked_for_good("unknown_command");
    }

    #[test]
    fn a_refusal_code_the_adapter_does_not_know_is_parked_without_a_restart() {
        parked_for_good("some_later_code");
    }

    /// A turn Spiron could not start (a dead process still holds the
    /// session's lease) is offered again once the lease is gone: no restart,
    /// no fault, applied once.
    #[test]
    fn a_turn_spiron_could_not_start_is_offered_again_once_the_lease_lapses() {
        let mut world = World::<Spiron>::new();
        let lease = world.fixture.dir().join("lease");
        std::fs::write(&lease, "").expect("lease");
        let first = world.receive(1).remove(0);
        world.pump_until("the turn is refused", |w| {
            w.fixture.wire().contains(&first.id.to_string())
        });
        world.pump_for(Duration::from_millis(300));
        assert!(world.cursor() < first.seq);
        assert!(world.fixture.journal().is_empty());
        assert!(world.alive());
        std::fs::remove_file(&lease).expect("lease lapses");
        world.pump_until("the prompt is committed", |w| w.cursor() == first.seq);
        world.drain(first.seq);
        assert_eq!(ids(&world.fixture.journal()), [first.id.to_string()]);
        assert_eq!(world.fixture.launches(), 1, "restarted");
        assert!(world.delivery.take_rejections().is_empty());
    }
}
