//! Spiron adapter: one external `spiron --headless --json` process, over a
//! [`StreamHarness`].
//!
//! - `spiron.headless/v2` (`docs/adr/0011-spiron-headless-v2.md`): the process
//!   runs on its own, listening on a Unix socket (`--listen`), and outlives
//!   the server. [`SpironHarness::launch_listening`] starts it and attaches;
//!   [`SpironHarness::reattach`] attaches again after a server restart with
//!   `after_seq`, and the replay rebuilds what the adapter knew. `stop` ends
//!   the process; [`SpironHarness::detach`] leaves it running.
//!
//! Inputs are journaled and deduplicated by request_id; only v2 is accepted.

pub mod cloud;
mod cloud_api;
#[cfg(test)]
mod cloud_tests;
mod stdio;

pub use cloud::{SpironCloudConfig, SpironCloudHarness, SpironCloudProvider};

use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, Harness, HarnessError, HarnessHealth, OutboundMessage, TurnUsage,
    compose_message,
};
use anyagents_harness_stream::{Connect, LineProtocol, StreamHarness, StreamSpec};
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

impl SpironLaunch {
    /// Opt this launch into the durable Spiron CLI's `--cloud` handoff mode,
    /// optionally targeting a specific cloud agent slug or ID (`--cloud-agent`).
    pub fn with_cloud(mut self, cloud_agent: Option<&str>) -> Self {
        if !self.args.iter().any(|a| a == "--cloud") {
            self.args.push("--cloud".to_string());
        }
        if let Some(target) = cloud_agent.map(str::trim).filter(|s| !s.is_empty())
            && !self
                .args
                .iter()
                .any(|a| a == "--cloud-agent" || a.starts_with("--cloud-agent="))
        {
            self.args.push("--cloud-agent".to_string());
            self.args.push(target.to_string());
        }
        self
    }

    /// Assemble CLI arguments for stdio `spiron --headless --json` without `--listen`.
    pub fn build_stdio_launch_args(&self) -> Vec<String> {
        let mut args = Vec::with_capacity(self.args.len() + 4);
        let mut iter = self.args.iter();
        while let Some(arg) = iter.next() {
            if arg == "--listen" {
                let _ = iter.next();
                continue;
            }
            if arg.starts_with("--listen=") {
                continue;
            }
            args.push(arg.clone());
        }
        if !args.iter().any(|a| a == "--headless") {
            args.insert(0, "--headless".to_string());
        }
        if !args.iter().any(|a| a == "--json") {
            args.insert(1.min(args.len()), "--json".to_string());
        }
        if let Some(session) = &self.session
            && !args
                .iter()
                .any(|a| a == "--session" || a.starts_with("--session="))
        {
            args.extend(["--session".to_string(), session.clone()]);
        }
        args
    }

    /// Assemble final CLI arguments for `spiron --headless --json --listen <socket>`
    /// without duplicating `--session` or `--listen`.
    pub fn build_launch_args(&self, socket: &Path) -> Vec<String> {
        let mut args = self.args.clone();
        if let Some(session) = &self.session
            && !args
                .iter()
                .any(|a| a == "--session" || a.starts_with("--session="))
        {
            args.extend(["--session".to_string(), session.clone()]);
        }
        if !args
            .iter()
            .any(|a| a == "--listen" || a.starts_with("--listen="))
        {
            args.extend(["--listen".to_string(), socket.display().to_string()]);
        }
        args
    }
}

/// How long a v2 process may take to end after `stop` (it cancels its turn,
/// which waits for a running tool's receipt) before its group is ended,
/// which takes its own TERM grace: together inside
/// [`anyagents_harness::STOP_DEADLINE`], which a host waits for a stop.
const STOP_GRACE: Duration = Duration::from_secs(3);

/// The longest Unix socket path (`sun_path` less its terminator) this host OS binds.
pub const MAX_SOCKET_PATH: usize = if cfg!(target_os = "linux") { 107 } else { 103 };

enum SpironBackend {
    Stream(Box<StreamHarness>),
    Stdio(Box<stdio::StdioSpiron>),
}

macro_rules! on_backend {
    ($self:expr, $m:ident $(, $arg:expr)*) => {
        match &$self.backend {
            SpironBackend::Stream(s) => s.$m($($arg),*),
            SpironBackend::Stdio(s) => s.$m($($arg),*),
        }
    };
    (mut $self:expr, $m:ident $(, $arg:expr)*) => {
        match &mut $self.backend {
            SpironBackend::Stream(s) => s.$m($($arg),*),
            SpironBackend::Stdio(s) => s.$m($($arg),*),
        }
    };
}

/// One long-lived Spiron process.
pub struct SpironHarness {
    backend: SpironBackend,
    /// A v2 process listening on its socket, which outlives the connection.
    process: Option<Listening>,
}

/// A v2 process, as this adapter ends it.
struct Listening {
    /// Ends the process's group when it was started by an earlier server.
    end: Option<Box<dyn Fn() + Send>>,
    environment: Vec<(String, String)>,
    /// Its launch record, removed when the process ends.
    record: Option<PathBuf>,
    /// Left running for the next server, or already ended.
    released: bool,
}

/// Why a v2 launch did not happen.
#[derive(Debug, PartialEq, Eq)]
pub enum ListenError {
    Failed(String),
}

/// How to attach to a v2 process an earlier server started.
pub struct Reattach {
    pub program: PathBuf,
    pub socket: PathBuf,
    pub pid: u32,
    /// The seq before the launch's first `ready`: the replay starts there.
    pub after_seq: u64,
    pub session: Option<String>,
    pub environment: Vec<(String, String)>,
    pub state_dir: PathBuf,
    /// Ends the process's group, once its identity is proven.
    pub end: Box<dyn Fn() + Send>,
    pub record: PathBuf,
}

fn spec(args: Vec<String>) -> StreamSpec {
    StreamSpec {
        name: "anyagents-spiron".to_string(),
        args,
        resume_flag: "--session",
        start_line: "prompt",
        cancel: "cancel",
        protocol: LineProtocol::AcknowledgedResult,
    }
}

/// A request id no other input of this process carries.
fn control_id(kind: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| time.as_nanos());
    format!("{kind}-{}-{nanos}", std::process::id())
}

fn exit_before_listen_error(status: std::process::ExitStatus, log: &Path) -> ListenError {
    let stderr = anyagents_harness_stream::read_bounded_masked_log(log);
    let trimmed = stderr.trim();
    if trimmed.is_empty() {
        ListenError::Failed(format!("spiron exited with {status} before it listened"))
    } else {
        ListenError::Failed(format!(
            "spiron exited with {status} before it listened: {trimmed}"
        ))
    }
}

fn wait_exited(child: &mut Child, grace: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + grace;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            _ => return None,
        }
    }
}

impl SpironHarness {
    /// Start a stdio `spiron --headless --json` process (no `--listen`).
    pub fn launch(launch: SpironLaunch) -> Result<Self, HarnessError> {
        Self::launch_with_patience(launch, Duration::from_secs(60))
    }

    /// Start a stdio `spiron --headless --json` process with an explicit startup patience.
    pub fn launch_with_patience(
        launch: SpironLaunch,
        patience: Duration,
    ) -> Result<Self, HarnessError> {
        let stdio = stdio::StdioSpiron::launch(launch, patience)?;
        Ok(Self {
            backend: SpironBackend::Stdio(Box::new(stdio)),
            process: None,
        })
    }

    /// Start a v2 process that listens on `socket` and outlives this server,
    /// and attach to it. Its output goes to `log`; launch errors fail closed.
    ///
    /// `spawned` is told the process's pid as soon as it exists, before it
    /// listens, so the host can record it: a host that dies before the
    /// attach still leaves the process found and ended by its successor.
    pub fn launch_listening(
        launch: SpironLaunch,
        socket: &Path,
        log: &Path,
        patience: Duration,
        spawned: impl FnOnce(u32) -> Result<(), String>,
    ) -> Result<Self, ListenError> {
        let failed = |error: String| ListenError::Failed(error);
        if socket.as_os_str().len() > MAX_SOCKET_PATH {
            return Err(failed(format!(
                "Unix socket path exceeds {MAX_SOCKET_PATH} bytes: {}",
                socket.display()
            )));
        }
        let args = launch.build_launch_args(socket);
        let output = std::fs::File::create(log)
            .map_err(|err| failed(format!("cannot open {}: {err}", log.display())))?;
        let errors = output
            .try_clone()
            .map_err(|err| failed(format!("cannot open {}: {err}", log.display())))?;
        // Its own process group (as every harness), stdin on /dev/null: a
        // server's group signal and a server's exit leave it running.
        let mut command = anyagents_harness::spawn_command(&launch.program, &launch.environment)
            .map_err(|err| failed(err.to_string()))?;
        command
            .args(&args)
            .current_dir(&launch.work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(output))
            .stderr(Stdio::from(errors));
        let mut child =
            anyagents_harness::spawn_child(&mut command).map_err(|err| failed(err.to_string()))?;
        if let Err(error) = spawned(child.id()) {
            anyagents_harness::kill_child(&mut child, &launch.environment);
            return Err(failed(error));
        }
        let deadline = Instant::now() + patience;
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(exit_before_listen_error(status, log));
            }
            if std::os::unix::net::UnixStream::connect(socket).is_ok() {
                break;
            }
            if Instant::now() >= deadline {
                anyagents_harness::kill_child(&mut child, &launch.environment);
                return Err(failed(format!(
                    "spiron did not listen on {} within {}s",
                    socket.display(),
                    patience.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let pid = child.id();
        let inner = StreamHarness::connect(Connect {
            spec: spec(launch.args),
            program: launch.program,
            socket: socket.to_path_buf(),
            pid: Some(pid),
            after_seq: None,
            session_id: launch.session,
            environment: launch.environment.clone(),
            delivery: DELIVERY,
        });
        let inner = match inner {
            Ok(inner) => inner,
            Err(err) => {
                if let Some(status) = wait_exited(&mut child, Duration::from_millis(250)) {
                    return Err(exit_before_listen_error(status, log));
                }
                anyagents_harness::kill_child(&mut child, &launch.environment);
                return Err(failed(err.to_string()));
            }
        };
        inner.attach_listening_child(child, log.to_path_buf());
        inner.set_state_dir(&launch.state_dir);
        let harness = Self {
            backend: SpironBackend::Stream(Box::new(inner)),
            process: Some(Listening {
                end: None,
                environment: launch.environment,
                record: None,
                released: false,
            }),
        };
        // On failure the process is ended as the harness drops.
        if let SpironBackend::Stream(ref inner) = harness.backend {
            inner
                .wait_ready(patience)
                .map_err(|err| failed(err.to_string()))?;
        }
        Ok(harness)
    }

    /// Attach to a v2 process an earlier server started, replaying every
    /// frame after `after_seq`: the adapter then knows what the earlier one
    /// knew (the open turn, every proof, the usage served).
    pub fn reattach(attach: Reattach, patience: Duration) -> Result<Self, HarnessError> {
        let inner = StreamHarness::connect(Connect {
            spec: spec(Vec::new()),
            program: attach.program,
            socket: attach.socket,
            pid: Some(attach.pid),
            after_seq: Some(attach.after_seq),
            session_id: attach.session,
            environment: attach.environment.clone(),
            delivery: DELIVERY,
        })?;
        inner.set_state_dir(&attach.state_dir);
        inner.wait_ready(patience)?;
        Ok(Self {
            backend: SpironBackend::Stream(Box::new(inner)),
            process: Some(Listening {
                end: Some(attach.end),
                environment: attach.environment,
                record: Some(attach.record),
                released: false,
            }),
        })
    }

    /// The seq before this connection's `ready`: where a later attach
    /// replays from to rebuild this adapter. `None` for stdio.
    pub fn attach_after_seq(&self) -> Option<u64> {
        self.process.as_ref()?;
        match &self.backend {
            SpironBackend::Stream(s) => s.attached_seq().map(|seq| seq.saturating_sub(1)),
            SpironBackend::Stdio(_) => None,
        }
    }

    /// Name the launch record removed when the process ends.
    pub fn set_record(&mut self, path: PathBuf) {
        if let Some(process) = &mut self.process {
            process.record = Some(path);
        }
    }

    /// The launch record of a v2 process.
    pub fn record(&self) -> Option<&Path> {
        self.process.as_ref()?.record.as_deref()
    }

    /// Whether this runs a v2 process that outlives the server.
    pub fn listening(&self) -> bool {
        self.process.is_some()
    }

    /// Close the connection and leave a v2 process running, for the next
    /// server to attach to. A stdio process ends as the harness drops.
    pub fn detach(mut self) {
        if let Some(process) = &mut self.process {
            process.released = true;
        }
        match &mut self.backend {
            SpironBackend::Stream(s) => s.stop(),
            SpironBackend::Stdio(s) => s.stop(),
        }
    }

    /// End a v2 process: `stop`, a grace to end on its own, then its group.
    fn end_listening(&mut self) {
        let (Some(process), SpironBackend::Stream(inner)) =
            (self.process.as_mut(), &mut self.backend)
        else {
            return;
        };
        if process.released {
            return;
        }
        process.released = true;
        let _ = inner.write_line(&json!({"type": "stop", "request_id": control_id("stop")}));
        inner.wait_gone(STOP_GRACE);
        if let Some(mut child) = inner.take_child() {
            anyagents_harness::kill_child(&mut child, &process.environment);
            let _ = child.wait();
        } else if let Some(end) = &process.end {
            end();
        }
        if let Some(record) = &process.record {
            let _ = std::fs::remove_file(record);
        }
    }

    pub fn recognise_applied(&self, ids: impl IntoIterator<Item = String>) {
        on_backend!(self, recognise_applied, ids)
    }

    pub fn session_id(&self) -> Option<String> {
        on_backend!(self, session_id)
    }

    pub fn proves_consumption(&self) -> bool {
        match &self.backend {
            SpironBackend::Stream(s) => s.proves_consumption(),
            SpironBackend::Stdio(_) => true,
        }
    }

    pub fn written_ids(&self) -> Vec<String> {
        on_backend!(self, written_ids)
    }

    pub fn text_for(&self, id: &str) -> Option<String> {
        on_backend!(self, text_for, id)
    }

    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        on_backend!(self, usage_for, id)
    }

    pub fn failure_for(&self, id: &str) -> Option<String> {
        match &self.backend {
            SpironBackend::Stream(_) => None,
            SpironBackend::Stdio(s) => s.failure_for(id),
        }
    }

    pub fn program(&self) -> &Path {
        on_backend!(self, program)
    }
}

impl Harness for SpironHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.deliver_with_turn_context(message, None)
    }

    fn supports_turn_tool_context(&self) -> bool {
        match &self.backend {
            SpironBackend::Stream(s) => s.supports_turn_tool_context(),
            SpironBackend::Stdio(_) => true,
        }
    }

    fn task_turn_result(&self, input_id: &str) -> Option<(String, String)> {
        on_backend!(self, task_turn_result, input_id)
    }

    fn deliver_with_turn_context(
        &mut self,
        message: &OutboundMessage,
        context: Option<&str>,
    ) -> Result<DeliverOutcome, HarnessError> {
        match &mut self.backend {
            SpironBackend::Stream(s) => {
                let mut line = json!({
                    "type": "prompt",
                    "request_id": message.id,
                    "text": compose_message(&message.id, &message.context, &message.text),
                });
                if let Some(context) = context {
                    line["tool_context"] = json!(context);
                }
                s.deliver(message, &line)
            }
            SpironBackend::Stdio(s) => s.deliver_with_turn_context(message, context),
        }
    }

    fn proven(&self, message_id: &str) -> Option<anyagents_harness::Proof> {
        on_backend!(self, proven, message_id)
    }

    fn stop(&mut self) {
        self.end_listening();
        on_backend!(mut self, stop);
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        match &mut self.backend {
            SpironBackend::Stream(s) => s.background(),
            SpironBackend::Stdio(_) => Ok(()),
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        on_backend!(mut self, resume, session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        match &mut self.backend {
            SpironBackend::Stream(s) => {
                s.cancel(&json!({"type": "cancel", "request_id": control_id("cancel")}))
            }
            SpironBackend::Stdio(s) => s.cancel(),
        }
    }

    fn health(&self) -> HarnessHealth {
        on_backend!(self, health)
    }

    fn pending_input_owner(&self) -> Option<String> {
        on_backend!(self, pending_input_owner)
    }

    fn input_ready(&self) -> bool {
        on_backend!(self, input_ready)
    }

    fn served_usage(&self) -> TurnUsage {
        on_backend!(self, served_usage)
    }

    fn turns_served(&self) -> u32 {
        on_backend!(self, turns_served)
    }

    fn clear_quota(&mut self) {
        if let SpironBackend::Stream(s) = &mut self.backend {
            s.clear_quota();
        }
    }

    fn process_id(&self) -> Option<u32> {
        on_backend!(self, process_id)
    }

    fn exit_diagnostic(&self) -> Option<String> {
        match &self.backend {
            SpironBackend::Stream(_) => None,
            SpironBackend::Stdio(s) => s.exit_diagnostic(),
        }
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        on_backend!(self, watch_activity, notify)
    }
}

impl Drop for SpironHarness {
    /// A v2 process the host neither stopped nor detached ends with the
    /// adapter, as a stdio child does.
    fn drop(&mut self) {
        self.end_listening();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_listening_rejects_oversized_socket_path_before_spawning() {
        let temp = tempfile::tempdir().unwrap();
        let oversized = temp
            .path()
            .join(format!("{}.sock", "x".repeat(MAX_SOCKET_PATH)));
        let log = temp.path().join("spiron.log");
        let launch = SpironLaunch {
            program: temp.path().join("missing-spiron"),
            args: Vec::new(),
            environment: Vec::new(),
            work_dir: temp.path().to_path_buf(),
            session: None,
            state_dir: temp.path().to_path_buf(),
        };
        let Err(ListenError::Failed(error)) = SpironHarness::launch_listening(
            launch,
            &oversized,
            &log,
            Duration::from_secs(1),
            |_| Ok(()),
        ) else {
            panic!("expected ListenError::Failed for oversized socket path");
        };
        assert!(error.contains(&MAX_SOCKET_PATH.to_string()), "{error}");
        assert!(
            !log.exists(),
            "oversized socket must fail before opening log"
        );
    }

    #[test]
    fn launch_args_support_durable_local_and_cloud_handoff_without_duplicate_flags() {
        let temp = tempfile::tempdir().unwrap();
        let sock = temp.path().join("spiron.sock");
        let launch = SpironLaunch {
            program: PathBuf::from("spiron"),
            args: vec![
                "--headless".into(),
                "--json".into(),
                "--mcp-config".into(),
                temp.path().join("spiron-mcp.json").display().to_string(),
            ],
            environment: Vec::new(),
            work_dir: temp.path().to_path_buf(),
            session: Some("sess-123".into()),
            state_dir: temp.path().to_path_buf(),
        }
        .with_cloud(Some("worker-slug"))
        .with_cloud(Some("ignored-duplicate"));

        let built = launch.build_launch_args(&sock);
        assert_eq!(built.iter().filter(|a| *a == "--cloud").count(), 1);
        assert_eq!(built.iter().filter(|a| *a == "--cloud-agent").count(), 1);
        let agent_idx = built.iter().position(|a| a == "--cloud-agent").unwrap();
        assert_eq!(built[agent_idx + 1], "worker-slug");
        assert_eq!(built.iter().filter(|a| *a == "--session").count(), 1);
        assert_eq!(built.iter().filter(|a| *a == "--listen").count(), 1);
        let stdio_args = launch.build_stdio_launch_args();
        assert!(!stdio_args.iter().any(|a| a.starts_with("--listen")));
        assert!(stdio_args.iter().any(|a| a == "--headless"));
        assert!(stdio_args.iter().any(|a| a == "--json"));
    }

    #[test]
    fn stdio_launch_runs_without_listen_and_captures_reply_usage_and_errors() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fake-spiron");
        std::fs::write(
            &script,
            "#!/bin/sh\nfor a in \"$@\"; do [ \"$a\" = \"--listen\" ] && { echo 'unexpected --listen' >&2; exit 2; }; done\n\
echo '{\"type\":\"ready\",\"protocol\":\"spiron.headless/v1\",\"session_id\":\"sp-1\"}'\n\
while IFS= read -r line; do\n\
  case \"$line\" in\n\
    *'\"type\":\"prompt\"'*) echo '{\"type\":\"accepted\",\"request_id\":\"m1\"}'; echo '{\"type\":\"text_delta\",\"request_id\":\"m1\",\"delta\":\"ok\"}'; echo '{\"type\":\"result\",\"request_id\":\"m1\",\"session_id\":\"sp-1\",\"text\":\"ok\",\"usage\":{\"input_tokens\":5,\"output_tokens\":3}}' ;;\n\
    *'\"type\":\"stop\"'*) exit 0 ;;\n\
  esac\ndone\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut h = SpironHarness::launch_with_patience(
            SpironLaunch {
                program: script,
                args: vec!["--listen".into(), "/tmp/bad.sock".into()],
                environment: Vec::new(),
                work_dir: tmp.path().to_path_buf(),
                session: None,
                state_dir: tmp.path().to_path_buf(),
            },
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(h.session_id().as_deref(), Some("sp-1"));
        let msg = OutboundMessage {
            id: "m1".into(),
            seq: 1,
            text: "hi".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: anyagents_harness::Context::default(),
        };
        let _ = h.deliver(&msg).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while h.turns_served() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(h.text_for("m1").as_deref(), Some("ok"));
        assert_eq!(h.usage_for("m1").unwrap().output_tokens, 3);
        h.stop();
    }
}
