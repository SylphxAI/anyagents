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
}
