//! Spec: `docs/vision.md` "First version" item 2 (any harness through one
//! adapter contract) and item 3 (AnyAgents owns the answer).
//!
//! The OpenCode harness is one catalog entry, not a copied adapter: this crate
//! is the thin name the server starts it by. Its launch shape, closing line,
//! reported cost and answer text come from `StreamSpec::for_program("opencode")`.
//!
//! Declared delivery mode is followup: a message written while a turn is open
//! waits for the next `result`. Steer support is unproven. Resume starts the
//! child again with `--session` when the session id changed or the process has
//! exited. An answer is the last `text` line after the prompt, up to and
//! including the `result` line, and does not finish the turn.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnUsage,
};
use anyagents_harness_stream::{StreamHarness, StreamSpec, cancel_line, message_line};

/// One long-lived OpenCode `serve` child.
pub struct OpenCodeHarness {
    inner: StreamHarness,
    spec: StreamSpec,
}

impl OpenCodeHarness {
    /// Start one child.
    ///
    /// `program` is the `opencode` executable, or a test double. `session_id`,
    /// when set, is passed as `--session`.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        Self::start_in(program, session_id, environment, None)
    }

    /// Start one child with its working directory set to `work_dir`.
    pub fn start_in(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        Self::start_double(program, None, session_id, environment, work_dir)
    }

    /// Start one child. `subcommand` replaces `serve` for a test double that
    /// takes its orders from a subcommand of the same executable.
    pub fn start_double(
        program: impl Into<PathBuf>,
        subcommand: Option<&str>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let environment = environment
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let mut spec = StreamSpec::for_program("opencode");
        if let Some(subcommand) = subcommand {
            spec.args = vec![subcommand.to_string()];
        }
        let args = {
            let mut args = spec.args.clone();
            if let Some(id) = session_id {
                args.push(spec.resume_flag.to_string());
                args.push(id.to_string());
            }
            args
        };
        let inner = StreamHarness::launch(anyagents_harness_stream::Launch {
            spec: spec.clone(),
            program: program.into(),
            args,
            environment,
            work_dir: work_dir.map(Path::to_path_buf),
            session_id: session_id.map(str::to_string),
            mode: DeliveryMode::Followup,
            steer: SteerSupport::Unproven,
        })?;
        Ok(Self { inner, spec })
    }

    pub fn written_ids(&self) -> Vec<String> {
        self.inner.written_ids()
    }
    pub fn turns_started(&self) -> usize {
        self.inner.turns_started()
    }
    pub fn session_id(&self) -> Option<String> {
        self.inner.session_id()
    }
    pub fn wait_finished(&self, id: &str, timeout: Duration) -> bool {
        self.inner.wait_finished(id, timeout)
    }
    pub fn wait_quota(&self, timeout: Duration) -> Option<i64> {
        self.inner.wait_quota(timeout)
    }
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        self.inner.usage_for(id)
    }
    pub fn seen(&self, id: &str) -> bool {
        self.inner.seen(id)
    }
    pub fn wait_seen(&self, id: &str, timeout: Duration) -> bool {
        self.inner.wait_seen(id, timeout)
    }
    /// Wait `quiet` with no line from the child. A quiet turn the child never
    /// closed is finished when the answer stands still.
    pub fn wait_quiet(&self, quiet: Duration, patience: Duration) -> bool {
        self.inner.quiet(quiet, patience)
    }
    /// The turn for `id` ended on quiet, not on a result line.
    pub fn closed_on_quiet(&self, id: &str) -> bool {
        self.inner.finished_quiet(id)
    }
    /// The answer text the vendor sent for `id`, when it has sent one.
    pub fn answer(&self, id: &str) -> Option<String> {
        self.inner.text_for(id)
    }
}

impl Harness for OpenCodeHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.inner
            .deliver(message, &message_line(&self.spec, message))
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.inner.resume(session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.inner.cancel(&cancel_line(&self.spec))
    }

    fn answer_for(&self, id: &str) -> Option<String> {
        self.answer(id)
    }

    fn ends_on_quiet(&self) -> Option<Duration> {
        Some(Duration::from_millis(500))
    }

    fn quiet_for(&self, _id: &str, quiet: Duration, patience: Duration) -> bool {
        self.wait_quiet(quiet, patience)
    }

    fn health(&self) -> HarnessHealth {
        self.inner.health()
    }
}
