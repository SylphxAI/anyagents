//! The delivery conformance suite against the OpenCode adapter: a private
//! `opencode serve` per agent, driven with one-shot `opencode api` calls.
#![cfg(unix)]

use anyagents_harness_opencode::OpenCodeHarness;
use anyagents_invariants::conformance::{Fixture, Subject};

pub struct OpenCode;

impl Subject for OpenCode {
    type Harness = OpenCodeHarness;
    const HARNESS: &'static str = "opencode";
    const PROTOCOL: &'static str = "opencode";

    /// As the server starts it: the agent's config directory keeps the session
    /// id, the service port and pid, and roots the vendor's own state.
    fn launch(fixture: &Fixture, session: Option<&str>) -> OpenCodeHarness {
        let config = fixture.dir().join("config");
        std::fs::create_dir_all(&config).expect("config directory");
        OpenCodeHarness::start_in_config(
            fixture.program(),
            session,
            &[],
            Some(fixture.dir()),
            &config,
        )
        .expect("opencode starts")
    }

    fn session(harness: &OpenCodeHarness) -> Option<String> {
        harness.session_id()
    }
}

anyagents_invariants::conformance_suite!(OpenCode);
