//! The delivery conformance suite (`anyagents_invariants::conformance`)
//! against the Codex adapter, over a scripted `codex app-server`
//! (`fakes/proto_codex.py`).
#![cfg(unix)]

use anyagents_harness_codex::CodexHarness;
use anyagents_invariants::conformance::{Fixture, Subject};

/// `codex app-server` started the way an agent's process is: its state in
/// `CODEX_HOME`, in the agent's working directory.
pub struct Codex;

impl Subject for Codex {
    type Harness = CodexHarness;
    const HARNESS: &'static str = "codex";
    const PROTOCOL: &'static str = "codex";

    fn launch(fixture: &Fixture, session: Option<&str>) -> CodexHarness {
        let home = fixture.dir().display().to_string();
        CodexHarness::start_in(
            fixture.program(),
            session,
            &[("CODEX_HOME", home.as_str())],
            Some(fixture.dir()),
        )
        .expect("codex starts")
    }

    fn session(harness: &CodexHarness) -> Option<String> {
        harness.session_id()
    }
}

anyagents_invariants::conformance_suite!(Codex);
