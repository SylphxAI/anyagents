//! Spec: `docs/vision.md` "First version" item 2 (any harness through one
//! adapter contract) and the architecture head "Lazy harness setup".
//!
//! The OMP harness is one catalog entry, not a copied adapter: this crate is
//! the thin name the supervisor starts it by. Its launch shape, closing line
//! and usage come from `StreamSpec::for_program("omp")`.
//!
//! The first real session has to confirm these field names before the adapter
//! is called working: `done` closes the turn, its `session_id` is the session,
//! and `usage.input_tokens`, `usage.output_tokens`, `usage.cache_read_tokens`
//! and `usage.cache_write_tokens` are the token counts. A wait is `rate_limit`
//! with `resets_at_ms` in epoch milliseconds.

use std::path::PathBuf;
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, Harness, HarnessError, HarnessHealth, OutboundMessage, TurnUsage,
};
use anyagents_harness_stream::{StreamHarness, StreamSpec, cancel_line, message_line};

/// One long-lived OMP child. Its delivery mode is unverified.
pub struct OmpHarness {
    inner: StreamHarness,
    spec: StreamSpec,
}

impl OmpHarness {
    /// Start one child.
    ///
    /// `program` is the `omp` executable, or a test double. `session_id`, when
    /// set, is passed as `--session`.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        let environment = environment
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let spec = StreamSpec::for_program("omp");
        let inner = StreamHarness::open(spec.clone(), program, session_id, environment)?;
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
    pub fn seen(&self, id: &str) -> bool {
        self.inner.seen(id)
    }
    pub fn wait_seen(&self, id: &str, timeout: Duration) -> bool {
        self.inner.wait_seen(id, timeout)
    }
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        self.inner.usage_for(id)
    }
}

impl Harness for OmpHarness {
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

    fn health(&self) -> HarnessHealth {
        self.inner.health()
    }
}
