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

/// How long a v2 process may take to end after `stop` (it cancels its turn,
/// which waits for a running tool's receipt) before its group is ended,
/// which takes its own TERM grace: together inside
/// [`anyagents_harness::STOP_DEADLINE`], which a host waits for a stop.
const STOP_GRACE: Duration = Duration::from_secs(3);

/// The longest Unix socket path (`sun_path` less its terminator) this host OS binds.
pub const MAX_SOCKET_PATH: usize = if cfg!(target_os = "linux") { 107 } else { 103 };

/// One long-lived Spiron process.
pub struct SpironHarness {
    inner: StreamHarness,
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
        let mut args = launch.args.clone();
        if let Some(session) = &launch.session {
            args.extend(["--session".to_string(), session.clone()]);
        }
        args.extend(["--listen".to_string(), socket.display().to_string()]);
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
        let harness = Self {
            inner: match inner {
                Ok(inner) => inner,
                Err(err) => {
                    if let Some(status) = wait_exited(&mut child, Duration::from_millis(250)) {
                        return Err(exit_before_listen_error(status, log));
                    }
                    anyagents_harness::kill_child(&mut child, &launch.environment);
                    return Err(failed(err.to_string()));
                }
            },
            process: Some(Listening {
                end: None,
                environment: launch.environment,
                record: None,
                released: false,
            }),
        };
        harness
            .inner
            .attach_listening_child(child, log.to_path_buf());
        harness.inner.set_state_dir(&launch.state_dir);
        // On failure the process is ended as the harness drops.
        harness
            .inner
            .wait_ready(patience)
            .map_err(|err| failed(err.to_string()))?;
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
        let harness = Self {
            inner,
            process: Some(Listening {
                end: Some(attach.end),
                environment: attach.environment,
                record: Some(attach.record),
                released: false,
            }),
        };
        harness.inner.set_state_dir(&attach.state_dir);
        harness.inner.wait_ready(patience)?;
        Ok(harness)
    }

    /// The seq before this connection's `ready`: where a later attach
    /// replays from to rebuild this adapter. `None` for stdio.
    pub fn attach_after_seq(&self) -> Option<u64> {
        self.process.as_ref()?;
        self.inner.attached_seq().map(|seq| seq.saturating_sub(1))
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
        self.inner.stop();
    }

    /// End a v2 process: `stop`, a grace to end on its own, then its group.
    fn end_listening(&mut self) {
        let Some(process) = self.process.as_mut() else {
            return;
        };
        if process.released {
            return;
        }
        process.released = true;
        let _ = self
            .inner
            .write_line(&json!({"type": "stop", "request_id": control_id("stop")}));
        self.inner.wait_gone(STOP_GRACE);
        if let Some(mut child) = self.inner.take_child() {
            anyagents_harness::kill_child(&mut child, &process.environment);
            let _ = child.wait();
        } else if let Some(end) = &process.end {
            end();
        }
        if let Some(record) = &process.record {
            let _ = std::fs::remove_file(record);
        }
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
            "type": "prompt",
            "request_id": message.id,
            "text": compose_message(&message.id, &message.context, &message.text),
        });
        self.inner.deliver(message, &line)
    }

    fn proven(&self, message_id: &str) -> Option<anyagents_harness::Proof> {
        self.inner.proven(message_id)
    }

    fn stop(&mut self) {
        self.end_listening();
        self.inner.stop();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.inner.resume(session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.inner
            .cancel(&json!({"type": "cancel", "request_id": control_id("cancel")}))
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
}
