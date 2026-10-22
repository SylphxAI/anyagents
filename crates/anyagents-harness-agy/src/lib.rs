//! Spec: `docs/vision.md` "First version" item 2 (any harness through one
//! adapter contract) and the architecture head "Lazy harness setup".
//!
//! The Antigravity harness is one catalog entry, not a copied adapter: this
//! crate is the thin name the supervisor starts it by. Its launch shape, closing
//! line and usage come from `StreamSpec::for_program("agy")`.

use std::path::{Path, PathBuf};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, SteerSupport, TurnUsage,
};
use anyagents_harness_stream::{Launch, StreamHarness, StreamSpec, cancel_line, message_line};

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
        let spec = StreamSpec::for_program("agy");
        Self::start_with_spec(spec, program, session_id, environment, None)
    }

    /// Start one child with a custom `StreamSpec` and optional working directory.
    pub fn start_with_spec(
        spec: StreamSpec,
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let environment = environment
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let mut args = spec.args.clone();
        if let Some(id) = session_id {
            args.push(spec.resume_flag.to_string());
            args.push(id.to_string());
        }
        let (mode, steer) = if work_dir.is_some() {
            (DeliveryMode::Steer, SteerSupport::Yes)
        } else {
            (DeliveryMode::Followup, SteerSupport::Unproven)
        };
        let inner = StreamHarness::launch(Launch {
            spec: spec.clone(),
            program: program.into(),
            args,
            environment,
            work_dir: work_dir.map(Path::to_path_buf),
            session_id: session_id.map(str::to_string),
            mode,
            steer,
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

    fn input_ready(&self) -> bool {
        self.inner.input_ready()
    }

    fn health(&self) -> HarnessHealth {
        self.inner.health()
    }

    fn clear_quota(&mut self) {
        self.inner.clear_quota();
    }

    fn process_id(&self) -> Option<u32> {
        self.inner.process_id()
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        self.inner.watch_activity(notify);
    }
}
