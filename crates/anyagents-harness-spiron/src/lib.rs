//! Spiron adapter: one external `spiron --headless --json` process speaking
//! `spiron.headless/v1`, over a [`StreamHarness`].
//!
//! The process announces itself with a `ready` frame before it is given
//! anything. A peer that names `steer_consumed` and `idempotent_input` gets
//! the whole inbox at once (`prompt` when no turn is open, `steer` into the
//! running one) and proves each input consumed; it deduplicates a replay by
//! its `request_id`. A peer that names neither is given one input per turn,
//! never one while a turn runs. `docs/delivery.md` states both levels.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, Delivery, Harness, HarnessError, HarnessHealth, OutboundMessage, TurnUsage,
    compose_message,
};
use anyagents_harness_stream::{Launch, LineProtocol, StreamHarness, StreamSpec};
use serde_json::json;

/// What this adapter guarantees once a capable peer announced itself.
pub const DELIVERY: Delivery = anyagents_harness_stream::SPIRON;

/// How to start one Spiron process.
pub struct SpironLaunch {
    pub program: PathBuf,
    /// Arguments before `--session`.
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub work_dir: PathBuf,
    /// The vendor session to resume.
    pub session: Option<String>,
    /// Where the process's exit fault is recorded for the server to show.
    pub state_dir: PathBuf,
}

/// One long-lived Spiron process.
pub struct SpironHarness {
    inner: StreamHarness,
}

impl SpironHarness {
    /// Start the process and wait for its `ready` frame, which decides the
    /// custody rules every input follows. A process that exits first fails
    /// the wait at once; one that says nothing within `patience` is refused.
    pub fn start(launch: SpironLaunch, patience: Duration) -> Result<Self, HarnessError> {
        let spec = StreamSpec {
            name: "anyagents-spiron".to_string(),
            args: launch.args.clone(),
            resume_flag: "--session",
            start_line: "user",
            cancel: "cancel",
            protocol: LineProtocol::AcknowledgedResult,
        };
        let mut args = launch.args;
        if let Some(session) = &launch.session {
            args.extend(["--session".to_string(), session.clone()]);
        }
        let inner = StreamHarness::launch(Launch {
            spec,
            program: launch.program,
            args,
            environment: launch.environment,
            work_dir: Some(launch.work_dir),
            session_id: launch.session,
            mode: anyagents_harness::DeliveryMode::Steer,
            delivery: DELIVERY,
        })?;
        inner.set_state_dir(&launch.state_dir);
        inner.wait_ready(patience)?;
        Ok(Self { inner })
    }

    /// Inputs the session's journal shows an earlier process applied; each is
    /// committed without being written again.
    pub fn recognise_applied(&self, ids: impl IntoIterator<Item = String>) {
        self.inner.recognise_applied(ids);
    }

    pub fn session_id(&self) -> Option<String> {
        self.inner.session_id()
    }

    /// Whether this process proves consumption and deduplicates input.
    pub fn proves_consumption(&self) -> bool {
        self.inner.proves_consumption()
    }

    pub fn written_ids(&self) -> Vec<String> {
        self.inner.written_ids()
    }

    /// The answer text the turn `id` opened has streamed so far.
    pub fn text_for(&self, id: &str) -> Option<String> {
        self.inner.text_for(id)
    }

    /// The usage the turn `id` opened reported.
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        self.inner.usage_for(id)
    }

    pub fn program(&self) -> &Path {
        self.inner.program()
    }
}

impl Harness for SpironHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let line = json!({
            "type": "user",
            "request_id": message.id,
            "text": compose_message(&message.id, &message.context, &message.text),
        });
        self.inner.deliver(message, &line)
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.inner.resume(session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.inner.cancel(&json!({"type": "cancel"}))
    }

    fn health(&self) -> HarnessHealth {
        self.inner.health()
    }

    fn input_ready(&self) -> bool {
        self.inner.input_ready()
    }

    fn served_usage(&self) -> TurnUsage {
        self.inner.served_usage()
    }

    fn turns_served(&self) -> u32 {
        self.inner.turns_served()
    }

    fn clear_quota(&mut self) {
        self.inner.clear_quota()
    }

    fn process_id(&self) -> Option<u32> {
        self.inner.process_id()
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        self.inner.watch_activity(notify)
    }
}
