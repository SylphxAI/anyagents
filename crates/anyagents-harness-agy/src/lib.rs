//! Spec: `docs/vision.md` "First version" item 2 (any harness through one
//! adapter contract) and the architecture head "Lazy harness setup".
//!
//! The Antigravity harness is one catalog entry, not a copied adapter: this
//! crate is the thin name the supervisor starts it by. Its launch shape, closing
//! line and usage come from `StreamSpec::for_program("agy")`.

use std::path::PathBuf;

use anyagents_harness::{DeliverOutcome, Harness, HarnessError, HarnessHealth, TurnUsage};
use anyagents_harness_stream::{StreamHarness, StreamSpec, cancel_line, message_line};

/// One long-lived Antigravity child.
pub struct AgyHarness {
    inner: StreamHarness,
    spec: StreamSpec,
}

impl AgyHarness {
    /// Start one child.
    ///
    /// `program` is the `agy` executable, or a test double. `session_id`, when
    /// set, is passed as `--resume`.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        let environment = environment
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let spec = StreamSpec::for_program("agy");
        let inner = StreamHarness::open(spec.clone(), program, session_id, environment)?;
        Ok(Self { inner, spec })
    }

    /// Start one child and resume `session_id` when the vendor takes it.
    ///
    /// The new server's step: a child starts, then the stored session is
    /// resumed on it. A vendor that says nothing and writes nothing did not
    /// take a session it was given, so a new child starts a new conversation
    /// and stderr says the resume failed. Any line means the resumed
    /// conversation ran, whatever it reported.
    pub fn start_after_restart(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        let mut harness = Self::start(program, None, environment)?;
        harness.resume_or_start(session_id, true)?;
        Ok(harness)
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

    /// The arguments the current child was started with.
    pub fn args(&self) -> Vec<String> {
        self.inner.args().to_vec()
    }

    /// Start a new child, resumed with `session`.
    ///
    /// A vendor that answers nothing to a session it was given did not take it.
    /// A caller that passes `restart` gets a child without that session
    /// instead, and stderr says why: a conversation that had to be left behind
    /// is never dropped silently. A vendor that spoke on its own was running,
    /// whatever it reported.
    pub fn resume_or_start(
        &mut self,
        session: Option<&str>,
        restart: bool,
    ) -> Result<(), HarnessError> {
        if let Some(said) = self.inner.resume_or_start(session, restart)? {
            eprintln!("{said}");
        }
        Ok(())
    }

    /// The launch just started printed nothing and its vendor wrote nothing
    /// to its inbox: it did not take what it was given.
    pub fn silent_start(&self) -> Option<bool> {
        self.inner.silent_start()
    }

    /// Whether the vendor child is running.
    pub fn is_alive_for_test(&self) -> bool {
        self.inner.health().process_alive
    }

    /// End the launch in flight as a vendor crash does: the child is killed and
    /// its reader reaches the end.
    #[cfg(feature = "double")]
    pub fn end_launch(&mut self) {
        self.inner.end_launch();
    }

    pub fn wait_finished(&self, id: &str, timeout: std::time::Duration) -> bool {
        self.inner.wait_finished(id, timeout)
    }
    pub fn wait_quota(&self, timeout: std::time::Duration) -> Option<i64> {
        self.inner.wait_quota(timeout)
    }
    pub fn seen(&self, id: &str) -> bool {
        self.inner.seen(id)
    }
    pub fn wait_seen(&self, id: &str, timeout: std::time::Duration) -> bool {
        self.inner.wait_seen(id, timeout)
    }
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        self.inner.usage_for(id)
    }
}

impl Harness for AgyHarness {
    fn deliver(
        &mut self,
        message: &anyagents_harness::OutboundMessage,
    ) -> Result<DeliverOutcome, HarnessError> {
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

    fn health(&self) -> HarnessHealth {
        self.inner.health()
    }
}
