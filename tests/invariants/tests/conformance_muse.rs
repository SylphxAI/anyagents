//! The delivery conformance suite (`anyagents_invariants::conformance`)
//! against the Muse adapter, over `fakes/proto_muse.py`.
#![cfg(unix)]

use anyagents_harness_muse::MuseHarness;
use anyagents_invariants::conformance::{Fixture, Subject};

/// `muse serve`, started the way the server starts it.
pub struct Muse;

impl Subject for Muse {
    type Harness = MuseHarness;
    const HARNESS: &'static str = "muse";
    const PROTOCOL: &'static str = "muse";

    fn launch(fixture: &Fixture, session: Option<&str>) -> MuseHarness {
        let args: Vec<String> = [
            "--provider",
            "meta",
            "--model",
            "muse-ember-1.0",
            "--disable-request-user-input",
            "--disable-sandbox",
            "--trust-workspace",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        MuseHarness::start_in_with_args(fixture.program(), &args, session, &[], Some(fixture.dir()))
            .expect("muse starts")
    }

    fn session(harness: &MuseHarness) -> Option<String> {
        harness.session_id()
    }
}

anyagents_invariants::conformance_suite!(Muse);

/// The level and the peer come from the live handshake.
#[test]
fn muse_reports_next_step_idempotent_delivery_and_its_version() {
    use anyagents_harness::{DeliveryLevel, Harness};
    let fixture = Fixture::new(Muse::PROTOCOL);
    let harness = Muse::launch(&fixture, None);
    let delivery = harness.health().delivery;
    assert_eq!(delivery.level, DeliveryLevel::NextStep);
    assert!(delivery.idempotent);
    assert_eq!(delivery.peer.as_deref(), Some("muse 1.4.20-R8619.1"));
}
