//! The delivery conformance suite against the OMP adapter (`omp --mode rpc`).
#![cfg(unix)]

use anyagents_harness_omp::OmpHarness;
use anyagents_harness_stream::StreamSpec;
use anyagents_invariants::conformance::{Fixture, Subject};

pub struct Omp;

impl Subject for Omp {
    type Harness = OmpHarness;
    const HARNESS: &'static str = "omp";
    const PROTOCOL: &'static str = "omp";

    /// As the server starts it: the catalog entry's arguments, in the
    /// agent's working directory.
    fn launch(fixture: &Fixture, session: Option<&str>) -> OmpHarness {
        let mut spec = StreamSpec::for_program("omp");
        spec.args.push("--no-extensions".to_string());
        OmpHarness::start_with_spec(spec, fixture.program(), session, &[], Some(fixture.dir()))
            .expect("omp starts")
    }

    fn session(harness: &OmpHarness) -> Option<String> {
        harness.session_id()
    }
}

anyagents_invariants::conformance_suite!(Omp);
