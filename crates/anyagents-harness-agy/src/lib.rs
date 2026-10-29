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
pub use anyagents_harness_stream::detect_agy_session_quota_and_error;
use anyagents_harness_stream::{
    AgyRpcConfig, Launch, StreamHarness, StreamSpec, cancel_line, message_line,
};

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
        let environment: Vec<(String, String)> = environment
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
        let fake_mode = std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
            || environment.iter().any(|(k, _)| k == "ANYAGENTS_FAKE_MODE");
        let agy_rpc = if work_dir.is_some() && !fake_mode {
            environment
                .iter()
                .find(|(k, _)| k == "ANYAGENTS_AGENT_CONFIG" || k == "AGY_CONFIG_DIR")
                .map(|(_, v)| {
                    let csrf_token = environment
                        .iter()
                        .find(|(k, _)| k == "ANYAGENTS_AGY_CSRF_TOKEN")
                        .map(|(_, val)| val.clone())
                        .unwrap_or_else(|| {
                            let nanos = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_nanos())
                                .unwrap_or(0);
                            format!("agy-csrf-{}-{nanos}", std::process::id())
                        });
                    if !args.iter().any(|a| a == "--csrf_token") {
                        args.push("--csrf_token".to_string());
                        args.push(csrf_token.clone());
                    }
                    let port_override = environment
                        .iter()
                        .find(|(k, _)| k == "ANYAGENTS_AGY_HTTP_PORT")
                        .and_then(|(_, val)| val.parse::<u16>().ok());
                    AgyRpcConfig {
                        config_dir: PathBuf::from(v),
                        csrf_token,
                        port_override,
                    }
                })
        } else {
            None
        };
        let inner = StreamHarness::launch_with_agy_rpc(
            Launch {
                spec: spec.clone(),
                program: program.into(),
                args,
                environment,
                work_dir: work_dir.map(Path::to_path_buf),
                session_id: session_id.map(str::to_string),
                mode,
                steer,
            },
            agy_rpc,
        )?;
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

    fn turns_served(&self) -> u32 {
        self.inner.turns_served()
    }

    fn served_usage(&self) -> TurnUsage {
        self.inner.served_usage()
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        self.inner.watch_activity(notify);
    }
}
