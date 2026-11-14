//! The delivery conformance suite against the Claude Code adapter.
#![cfg(unix)]

use anyagents_harness_claude::ClaudeHarness;
use anyagents_invariants::conformance::{Fixture, Subject};

pub struct ClaudeCode;

impl Subject for ClaudeCode {
    type Harness = ClaudeHarness;
    const HARNESS: &'static str = "claude-code";
    const PROTOCOL: &'static str = "claude";

    fn launch(fixture: &Fixture, session: Option<&str>) -> ClaudeHarness {
        let config = fixture.dir().join("claude");
        let config = config.display().to_string();
        ClaudeHarness::start_in_with_args(
            fixture.program(),
            &[],
            session,
            &[("CLAUDE_CONFIG_DIR", config.as_str())],
            Some(fixture.dir()),
        )
        .expect("claude starts")
    }

    fn session(harness: &ClaudeHarness) -> Option<String> {
        harness.session_id()
    }
}

anyagents_invariants::conformance_suite!(ClaudeCode);
