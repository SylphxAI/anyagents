//! Builds the harness the supervisor asked for. The binary name can be overridden
//! so tests never start a real vendor. The agent's config directory is the
//! harness home; it is applied after any extra variables so those cannot point
//! the process at another home.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_harness::{
    DeliverOutcome, Harness, HarnessError, HarnessHealth, OutboundMessage, TurnUsage,
};
use anyagents_harness_agy::AgyHarness;
use anyagents_harness_claude::ClaudeHarness;
use anyagents_harness_codex::CodexHarness;
use anyagents_harness_muse::MuseHarness;
use anyagents_harness_omp::OmpHarness;
use anyagents_harness_opencode::OpenCodeHarness;
use anyagents_harness_spiron::{SpironHarness, SpironLaunch};
use anyagents_harness_stream::{Launch, StreamHarness, StreamSpec};
use anyagents_keeper::{Hosted, RemoteHarness};
use anyagents_supervisor::LaunchRequest;
use serde::{Deserialize, Serialize};

use crate::dsh::DshHarness;
use crate::grok::GrokHarness;

/// One vendor adapter over its vendor process, in the process that hosts it:
/// its keeper, or this one.
pub enum LocalAdapter {
    Claude(Box<ClaudeHarness>),
    Codex(Box<CodexHarness>),
    OpenCode(Box<OpenCodeHarness>),
    Agy(Box<AgyHarness>),
    Muse(Box<MuseHarness>),
    Omp(Box<OmpHarness>),
    Dsh(Box<DshHarness>),
    Grok(Box<GrokHarness>),
    /// The external Spiron CLI.
    Spiron(Box<SpironHarness>),
    Contract(Box<ContractHarness>),
}

/// Where vendor adapters run.
#[derive(Clone, Debug)]
pub enum HarnessHost {
    /// Each in its own keeper (`anyagents keeper`), so the harness outlives
    /// this kernel. What `anyagents serve` uses.
    Keeper(anyagents_keeper::Program),
    /// In this process, so the harness ends with it: a server embedded in a
    /// test binary (which has no keeper mode) and the one-shot offline
    /// `flush`.
    InProcess,
}

enum Host {
    Local(LocalAdapter),
    Kept(Box<RemoteHarness>),
}

/// An agent's harness as the kernel drives it: the adapter, wherever it is
/// hosted, plus the native resume pointer and host session record the kernel
/// keeps for it.
pub struct VendorHarness {
    host: Host,
    /// The canonical session file beside the agent's config; none for the
    /// contract double. Legacy `sylphx-session-id` is read once at launch.
    pointer: Option<SessionPointer>,
    /// The harness name the host session registry records the session under.
    selector: String,
}

/// The native resume pointer beside an agent's config, and the vendor session
/// this process last recorded in it and in the host session registry.
///
/// The record is written once per vendor session id, not once per health
/// read: binding the alias takes the registry lock, parses the registry and
/// verifies the memory pin, and the id only changes when the vendor starts or
/// resumes a session.
pub struct SessionPointer {
    path: PathBuf,
    recorded: std::sync::Mutex<Recorded>,
}

#[derive(Clone, PartialEq, Eq)]
enum Recorded {
    Nothing,
    Session(String),
    Retired,
}

impl From<PathBuf> for SessionPointer {
    fn from(path: PathBuf) -> Self {
        Self {
            path,
            recorded: std::sync::Mutex::new(Recorded::Nothing),
        }
    }
}

impl std::ops::Deref for SessionPointer {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for SessionPointer {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl SessionPointer {
    fn recorded(&self) -> std::sync::MutexGuard<'_, Recorded> {
        self.recorded.lock().unwrap_or_else(|err| err.into_inner())
    }
}

impl VendorHarness {
    /// An adapter hosted in this process.
    #[cfg(test)]
    pub fn local(adapter: LocalAdapter, pointer: Option<PathBuf>, selector: &str) -> Self {
        Self {
            host: Host::Local(adapter),
            pointer: pointer.map(SessionPointer::from),
            selector: selector.to_string(),
        }
    }

    /// Start the adapter `launch` describes, in a keeper or in this process.
    fn start(
        planned: Planned,
        request: &LaunchRequest,
        host: &HarnessHost,
    ) -> Result<Self, String> {
        let Planned {
            launch,
            pointer,
            selector,
        } = planned;
        let host = match host {
            HarnessHost::InProcess => Host::Local(launch.start()?),
            HarnessHost::Keeper(program) => {
                use std::os::fd::AsFd as _;
                let seat = launch
                    .environment()
                    .iter()
                    .rev()
                    .find(|(key, _)| key == anyagents_harness::SEAT_LAUNCH_ENV)
                    .map(|(_, spec)| spec.clone());
                let mut labels = std::collections::BTreeMap::new();
                labels.insert(LABEL_SELECTOR.to_string(), selector.clone());
                if let Some(pointer) = &pointer {
                    labels.insert(LABEL_POINTER.to_string(), pointer.display().to_string());
                }
                let spec = anyagents_keeper::Spec {
                    agent: request.agent.id.as_str().to_string(),
                    harness: request.agent.harness.clone(),
                    model: request.agent.model.clone(),
                    seat,
                    labels,
                    launch: serde_json::to_value(&launch).map_err(|error| error.to_string())?,
                };
                let lock = request.lock.as_deref().map(|lock| lock.as_fd());
                Host::Kept(Box::new(anyagents_keeper::spawn(
                    program,
                    &anyagents_keeper::root(&request.data_dir),
                    &spec,
                    lock,
                )?))
            }
        };
        Ok(Self {
            host,
            pointer: pointer.map(SessionPointer::from),
            selector,
        })
    }

    /// A keeper a previous kernel left, taken over: the pointer and session
    /// record follow the labels its launch gave it.
    pub(crate) fn kept(remote: RemoteHarness) -> Self {
        let labels = &remote.hello().labels;
        let selector = labels
            .get(LABEL_SELECTOR)
            .cloned()
            .unwrap_or_else(|| remote.hello().harness.clone());
        let pointer = labels.get(LABEL_POINTER).map(PathBuf::from);
        Self {
            host: Host::Kept(Box::new(remote)),
            pointer: pointer.map(SessionPointer::from),
            selector,
        }
    }

    fn adapter(&self) -> &dyn Hosted {
        match &self.host {
            Host::Local(adapter) => adapter,
            Host::Kept(remote) => remote.as_ref(),
        }
    }

    fn adapter_mut(&mut self) -> &mut dyn Hosted {
        match &mut self.host {
            Host::Local(adapter) => adapter,
            Host::Kept(remote) => remote.as_mut(),
        }
    }

    /// Whether this runs the external Spiron CLI.
    pub fn is_spiron(&self) -> bool {
        is_spiron_harness(&self.selector)
    }

    /// Leave a kept harness running for the next kernel. One hosted here
    /// ends with this process anyway.
    pub(crate) fn detach(self) {
        if let Host::Kept(remote) = self.host {
            (*remote).detach();
        }
    }

    /// Keep `note` with a kept harness for the next kernel.
    pub(crate) fn set_note(&self, note: &str) {
        if let Host::Kept(remote) = &self.host
            && let Err(error) = remote.set_note(note)
        {
            eprintln!("anyagents-keeper: cannot keep the process note: {error}");
        }
    }

    fn sync_session(&self) -> Result<(), String> {
        let Some(path) = &self.pointer else {
            return Ok(());
        };
        let adapter = self.adapter();
        if adapter.session_invalidated() {
            let mut recorded = path.recorded();
            if *recorded != Recorded::Retired {
                retire_session_pointer(path)?;
                *recorded = Recorded::Retired;
            }
            return Ok(());
        }
        let Some(id) = adapter.session_id() else {
            return Ok(());
        };
        let trimmed = id.trim();
        let mut recorded = path.recorded();
        if matches!(&*recorded, Recorded::Session(done) if done == trimmed) {
            return Ok(());
        }
        if !trimmed.is_empty() {
            let existing = match std::fs::read_to_string(path) {
                Ok(text) => Some(text),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => return Err(format!("cannot read native session pointer: {err}")),
            };
            if let Some(root) = path.parent().and_then(Path::parent)
                && let Err(error) =
                    anyagents_core::host_session::bind_vendor(root, &self.selector, trimmed)
            {
                return Err(format!("host session alias refused: {error}"));
            }
            if existing.as_deref().map(str::trim) != Some(trimmed) {
                crate::create::write_required_file(path, format!("{trimmed}\n").as_bytes(), 0o660)
                    .map_err(|err| err.to_string())?;
            }
            *recorded = Recorded::Session(trimmed.to_string());
        }
        Ok(())
    }
}

/// Labels a kernel gives a keeper, to know the session record again when it
/// takes the keeper over.
const LABEL_POINTER: &str = "session_pointer";
const LABEL_SELECTOR: &str = "session_selector";

/// What one launch starts: the adapter call, the native resume pointer and
/// the name the host session registry knows the session by.
struct Planned {
    launch: AdapterLaunch,
    pointer: Option<PathBuf>,
    selector: String,
}

/// How a host starts one vendor adapter: the call the launch path decided
/// on, as data, so a keeper can make it. Everything that reads the store or
/// writes the agent's configuration has already happened in the kernel.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "adapter", rename_all = "snake_case")]
pub enum AdapterLaunch {
    Contract {
        program: String,
        config: String,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
    },
    Claude {
        program: String,
        args: Vec<String>,
        session: Option<String>,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
        /// A cloud harness behind the Claude adapter: mail waits for the turn
        /// to end (`docs/delivery.md`).
        #[serde(default)]
        cloud: bool,
    },
    Agy {
        program: String,
        session: Option<String>,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
        model: Option<String>,
        /// The scripted double (`ANYAGENTS_FAKE_MODE`) speaks no stream-json.
        scripted: bool,
    },
    Muse {
        program: String,
        args: Vec<String>,
        session: Option<String>,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
        config_dir: PathBuf,
        scripted: bool,
    },
    Omp {
        program: String,
        args: Vec<String>,
        session: Option<String>,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
    },
    Dsh {
        program: PathBuf,
        patch: Option<PathBuf>,
        session: Option<String>,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
    },
    Grok {
        program: PathBuf,
        model: Option<String>,
        session: Option<String>,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
        exe: PathBuf,
        mcp_args: Vec<String>,
    },
    Spiron {
        program: PathBuf,
        config_dir: PathBuf,
        work_dir: PathBuf,
        agent_id: String,
        agent_name: String,
        model: Option<String>,
        session: Option<String>,
        env: Vec<(String, String)>,
    },
    OpenCode {
        program: PathBuf,
        session: Option<String>,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
        config_dir: PathBuf,
    },
    Codex {
        program: String,
        session: Option<String>,
        env: Vec<(String, String)>,
        work_dir: PathBuf,
    },
}

impl AdapterLaunch {
    fn environment(&self) -> &[(String, String)] {
        match self {
            Self::Contract { env, .. }
            | Self::Claude { env, .. }
            | Self::Agy { env, .. }
            | Self::Muse { env, .. }
            | Self::Omp { env, .. }
            | Self::Dsh { env, .. }
            | Self::Grok { env, .. }
            | Self::Spiron { env, .. }
            | Self::OpenCode { env, .. }
            | Self::Codex { env, .. } => env,
        }
    }

    /// Start the vendor process and its adapter.
    pub fn start(self) -> Result<LocalAdapter, String> {
        Ok(match self {
            Self::Contract {
                program,
                config,
                env,
                work_dir,
            } => LocalAdapter::Contract(Box::new(
                ContractHarness::start(&program, &config, &pairs(&env), Some(&work_dir))
                    .map_err(|err| err.to_string())?,
            )),
            Self::Claude {
                program,
                args,
                session,
                env,
                work_dir,
                cloud,
            } => {
                let harness =
                    start_claude(&program, &args, session.as_deref(), &env, &work_dir)?;
                LocalAdapter::Claude(Box::new(if cloud {
                    harness.with_delivery(anyagents_harness_claude::CLOUD_DELIVERY)
                } else {
                    harness
                }))
            }
            Self::Agy {
                program,
                session,
                env,
                work_dir,
                model,
                scripted,
            } => {
                let harness = if scripted {
                    AgyHarness::start(&program, session.as_deref(), &pairs(&env))
                } else {
                    let mut spec = StreamSpec::for_program("agy");
                    spec.args = vec![
                        "--input-format".to_string(),
                        "stream-json".to_string(),
                        "--output-format".to_string(),
                        "stream-json".to_string(),
                        "--dangerously-skip-permissions".to_string(),
                    ];
                    if let Some(model) = model {
                        spec.args.push("--model".to_string());
                        spec.args.push(model);
                    }
                    spec.resume_flag = "--conversation";
                    AgyHarness::start_with_spec(
                        spec,
                        &program,
                        session.as_deref(),
                        &pairs(&env),
                        Some(&work_dir),
                    )
                };
                LocalAdapter::Agy(Box::new(harness.map_err(|err| err.to_string())?))
            }
            Self::Muse {
                program,
                args,
                session,
                env,
                work_dir,
                config_dir,
                scripted,
            } => {
                let harness = MuseHarness::start_in_with_args(
                    &program,
                    &args,
                    session.as_deref(),
                    &pairs(&env),
                    Some(&work_dir),
                )
                .map_err(|err| {
                    read_muse_exit_error(&config_dir).unwrap_or_else(|| err.to_string())
                })?;
                if !scripted && !harness.health().process_alive {
                    return Err(read_muse_exit_error(&config_dir).unwrap_or_else(|| {
                        "muse serve exited during startup handshake".to_string()
                    }));
                }
                LocalAdapter::Muse(Box::new(harness))
            }
            Self::Omp {
                program,
                args,
                session,
                env,
                work_dir,
            } => {
                let mut spec = StreamSpec::for_program("omp");
                spec.args.extend(args);
                let harness = OmpHarness::start_with_spec(
                    spec,
                    program,
                    session.as_deref(),
                    &pairs(&env),
                    Some(&work_dir),
                )
                .map_err(|err| err.to_string())?;
                LocalAdapter::Omp(Box::new(harness))
            }
            Self::Dsh {
                program,
                patch,
                session,
                env,
                work_dir,
            } => LocalAdapter::Dsh(Box::new(
                DshHarness::start(
                    &program,
                    patch.as_deref(),
                    session.as_deref(),
                    &pairs(&env),
                    &work_dir,
                )
                .map_err(|err| err.to_string())?,
            )),
            Self::Grok {
                program,
                model,
                session,
                env,
                work_dir,
                exe,
                mcp_args,
            } => LocalAdapter::Grok(Box::new(
                GrokHarness::start(
                    &program,
                    model.as_deref(),
                    session.as_deref(),
                    &pairs(&env),
                    &work_dir,
                    &exe,
                    &mcp_args,
                )
                .map_err(|err| err.to_string())?,
            )),
            Self::Spiron {
                program,
                config_dir,
                work_dir,
                agent_id,
                agent_name,
                model,
                session,
                env,
            } => LocalAdapter::Spiron(Box::new(start_spiron(
                &program,
                &config_dir,
                &work_dir,
                SpironAgent {
                    id: &agent_id,
                    name: &agent_name,
                    model: model.as_deref(),
                },
                session.as_deref(),
                &env,
            )?)),
            Self::OpenCode {
                program,
                session,
                env,
                work_dir,
                config_dir,
            } => LocalAdapter::OpenCode(Box::new(
                OpenCodeHarness::start_in_config(
                    &program,
                    session.as_deref(),
                    &pairs(&env),
                    Some(&work_dir),
                    &config_dir,
                )
                .map_err(|err| {
                    read_opencode_exit_error(&config_dir).unwrap_or_else(|| err.to_string())
                })?,
            )),
            Self::Codex {
                program,
                session,
                env,
                work_dir,
            } => LocalAdapter::Codex(Box::new(start_codex(
                &program,
                session.as_deref(),
                &env,
                &work_dir,
            )?)),
        })
    }
}

/// What the adapter for `harness` guarantees for mail that reaches a busy
/// agent, before a process has announced itself (`docs/delivery.md`). A live
/// process reports its own, from its handshake.
pub(crate) fn declared_delivery(harness: &str) -> anyagents_harness::Delivery {
    match anyagents_harness::normalize_harness_name(harness) {
        "codex" => anyagents_harness_codex::DELIVERY,
        "opencode" => anyagents_harness_opencode::DELIVERY,
        "agy" => anyagents_harness_agy::DELIVERY,
        "muse" => anyagents_harness_muse::DELIVERY,
        "omp" => anyagents_harness_omp::DELIVERY,
        "dsh" => crate::dsh::DELIVERY,
        "grok" => crate::grok::DELIVERY,
        "spiron" => anyagents_harness_stream::SPIRON,
        "claude-code" => anyagents_harness_claude::DELIVERY,
        name if is_cloud_harness(name) => anyagents_harness_claude::CLOUD_DELIVERY,
        _ => anyagents_harness_stream::LINE_PER_TURN,
    }
}

/// A harness driven by a program named in the agent's config, which must obey
/// the adapter contract in `docs/architecture.md`: one JSON line on stdin per
/// message, and a `result` line on stdout when its turn ends. An agent's own
/// harness is used unless its config names one.
fn adapter_config(config_dir: &Path, harness: &str) -> Option<PathBuf> {
    let text = std::fs::read_to_string(config_dir.join("adapter.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    // Ignore the retired in-process mode marker. Explicit contract doubles
    // remain available to the credential-free server acceptance suite.
    if value.get("mode").and_then(|item| item.as_str()) == Some("sylphx-native") {
        return None;
    }
    let named = value.get("harness").and_then(|item| item.as_str())?;
    (named == harness).then(|| config_dir.join("adapter.json"))
}

/// The contract double: `anyagents harness <config>`, honouring the adapter
/// contract. It exists so a repository test proves both harnesses' wiring end to
/// end, without starting a vendor.
pub struct ContractHarness {
    inner: StreamHarness,
}

impl ContractHarness {
    fn start(
        program: &str,
        config: &str,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, anyagents_harness::HarnessError> {
        Ok(Self {
            inner: StreamHarness::launch(Launch {
                spec: anyagents_harness_stream::StreamSpec::for_program("contract"),
                program: program.into(),
                args: vec!["harness".to_string(), config.to_string()],
                environment: environment
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
                work_dir: work_dir.map(Path::to_path_buf),
                session_id: None,
                mode: anyagents_harness::DeliveryMode::Followup,
                delivery: anyagents_harness_stream::LINE_PER_TURN,
            })?,
        })
    }
}

impl anyagents_harness::Harness for ContractHarness {
    fn deliver(
        &mut self,
        message: &anyagents_harness::OutboundMessage,
    ) -> Result<DeliverOutcome, HarnessError> {
        let line = serde_json::json!({
            "type": "user",
            "text": anyagents_harness::marked_message(&message.id, &message.text)
        });
        self.inner.deliver(message, &line)
    }

    fn proven(&self, message_id: &str) -> Option<anyagents_harness::Proof> {
        self.inner.proven(message_id)
    }

    fn stop(&mut self) {
        self.inner.stop();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, _session: &str) -> Result<(), HarnessError> {
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.inner.cancel(&serde_json::json!({"type": "cancel"}))
    }

    fn health(&self) -> HarnessHealth {
        self.inner.health()
    }

    fn input_ready(&self) -> bool {
        self.inner.input_ready()
    }

    fn clear_quota(&mut self) {
        self.inner.clear_quota()
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
        self.inner.watch_activity(notify)
    }
}

pub fn child_env() -> Vec<(String, String)> {
    let mut env = Vec::new();
    for key in ["ANYAGENTS_FAKE_MODE", "ANYAGENTS_FAKE_LOG"] {
        if let Ok(value) = std::env::var(key) {
            env.push((key.to_string(), value));
        }
    }
    env
}

pub fn read_persisted_last_error(config_dir: &Path) -> Option<(Option<String>, Option<i64>)> {
    let raw = std::fs::read_to_string(config_dir.join(".anyagents-last-error.json")).ok()?;
    let val = serde_json::from_str::<serde_json::Value>(&raw).ok()?;
    let rotated_ms = std::fs::read_to_string(config_dir.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let ts = val
        .get("timestamp_ms")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    if ts <= rotated_ms {
        return None;
    }
    let err = val
        .get("error")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let reset = val.get("quota_reset_at_ms").and_then(|v| v.as_i64());
    Some((err, reset))
}

fn has_persisted_turn_error(config_dir: &Path) -> bool {
    let Ok(raw) = std::fs::read_to_string(config_dir.join(".anyagents-last-error.json")) else {
        return false;
    };
    let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return false;
    };
    val.get("consecutive_turn_errors")
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
        > 0
}

pub fn persist_last_error(config_dir: &Path, error: Option<&str>, quota_reset_at_ms: Option<i64>) {
    let path = config_dir.join(".anyagents-last-error.json");
    match error.map(str::trim).filter(|s| !s.is_empty()) {
        Some(err) => {
            let now_ms = anyagents_core::now_ms();
            let payload = serde_json::json!({
                "error": err,
                "quota_reset_at_ms": quota_reset_at_ms,
                "timestamp_ms": now_ms,
            });
            if let Err(err) = anyagents_harness::atomic_write_nonempty_file(
                &path,
                payload.to_string().as_bytes(),
                0o664,
            ) {
                eprintln!("cannot persist {}: {err}", path.display());
            }
        }
        None => {
            let _ = std::fs::remove_file(&path);
        }
    }
}

#[cfg(test)]
pub fn vendor_launch(request: LaunchRequest) -> Result<VendorHarness, String> {
    vendor_launch_authorized(request, None, &HarnessHost::InProcess)
}

/// Reuse the daemon's admitted in-memory authority; reopening SQLite alone does
/// not confer capability verification or minting authority.
pub fn vendor_launch_with_store(
    request: LaunchRequest,
    store: &anyagents_core::Store,
) -> Result<VendorHarness, String> {
    vendor_launch_authorized(request, Some(store), &HarnessHost::InProcess)
}

/// Launch with the daemon's authority, hosting the adapter where `host` says.
pub fn vendor_launch_hosted(
    request: LaunchRequest,
    store: &anyagents_core::Store,
    host: &HarnessHost,
) -> Result<VendorHarness, String> {
    vendor_launch_authorized(request, Some(store), host)
}

fn vendor_launch_authorized(
    request: LaunchRequest,
    store: Option<&anyagents_core::Store>,
    host: &HarnessHost,
) -> Result<VendorHarness, String> {
    // The supervisor owns the agent process lock. Allocate the host generation
    // before any vendor can consume its instructions, never from a late marker.
    crate::host_memory::before_launch(&request.data_dir, &request.agent.id).map_err(|error| {
        match error {
            anyagents_core::Error::Io(ref cause)
                if cause.kind() == std::io::ErrorKind::WouldBlock =>
            {
                "memory_migration_wait: waiting_on=migration.lock; retryable=true".to_owned()
            }
            _ => format!("host migration failed: {error}"),
        }
    })?;
    prepare_host_session(&request)?;
    let config_dir = request.config_dir.clone();
    let res = vendor_launch_inner(request, store, host);
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() {
        match &res {
            Ok(vh) => {
                let h = vh.health();
                if h.process_alive {
                    if let Some((_, Some(reset_ms))) = read_persisted_last_error(&config_dir)
                        && reset_ms > anyagents_core::now_ms()
                    {
                        // Keep active quota reset window intact.
                    } else if has_persisted_turn_error(&config_dir) {
                        // Keep active turn-level error intact until a turn succeeds or wake clears it.
                    } else {
                        persist_last_error(&config_dir, None, None);
                    }
                }
            }
            Err(err) => {
                persist_last_error(&config_dir, Some(err), None);
            }
        }
    }
    res
}

pub(crate) fn memory_source(agent: &anyagents_core::Agent) -> Option<&Path> {
    match agent
        .launch_env
        .iter()
        .rev()
        .find(|(key, _)| key == "ANYAGENTS_MEMORY_SOURCE")
    {
        None => Some(Path::new("memory/MEMORY.md")),
        Some((_, path)) if path.is_empty() => None,
        Some((_, path)) => Some(Path::new(path)),
    }
}

fn retire_session_pointer(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("session invalidation persistence failed: {err}")),
    }
}

fn retire_poisoned_agy_session(config_dir: &Path, now_ms: i64) -> Result<bool, String> {
    let (_, error) = anyagents_harness_agy::detect_agy_session_quota_and_error(config_dir, now_ms);
    if !error.is_some_and(|error| {
        error
            .to_ascii_lowercase()
            .contains("invalid_argument (code 400)")
    }) {
        return Ok(false);
    }
    // Preserve native conversation databases and host history; only this
    // continuation pointer is poisoned. A watermark prevents stale log replay.
    retire_session_pointer(&config_dir.join("agy-session-id"))?;
    retire_session_pointer(&config_dir.join(".anyagents-last-error.json"))?;
    anyagents_harness::atomic_write_nonempty_file(
        &config_dir.join(".anyagents-quota-cleared-ms"),
        now_ms.to_string().as_bytes(),
        0o664,
    )
    .map_err(|err| format!("cannot persist agy recovery watermark: {err}"))?;
    Ok(true)
}

fn prepare_host_session(request: &LaunchRequest) -> Result<(), String> {
    let root = request.config_dir.parent().ok_or("agent root missing")?;
    anyagents_core::host_session::recover_pending(root).map_err(|e| e.to_string())?;
    let previous = anyagents_core::host_session::active(root).map_err(|e| e.to_string())?;
    let switched = previous
        .as_ref()
        .is_some_and(|s| s.harness != request.agent.harness);
    // Adopt the legacy pointer once, without modifying it. A reset/cutover must
    // never resurrect it: an existing host pin requires a positively matching
    // legacy alias, not merely a still-present historical file.
    if is_spiron_harness(&request.agent.harness)
        && !switched
        && session_from_file(&request.config_dir, "spiron-session-id")?.is_none()
        && let Some(legacy) = session_from_file(&request.config_dir, "sylphx-session-id")?
    {
        let adopt = match &previous {
            None => true,
            Some(active) => {
                anyagents_core::host_session::bound_vendor(root, &request.agent.harness, &legacy)
                    .map_err(|e| e.to_string())?
                    .is_some_and(|bound| bound.generation == active.generation)
            }
        };
        if adopt {
            anyagents_core::learned_memory::atomic_write(
                &request.config_dir.join("spiron-session-id"),
                format!("{legacy}\n").as_bytes(),
            )
            .map_err(|e| format!("legacy session adoption failed: {e}"))?;
        }
    }
    let source = memory_source(&request.agent);
    let prepared = anyagents_core::host_session::prepare_harness_with_source(
        root,
        &request.agent.harness,
        crate::create::harness_session_filename(&request.agent.harness),
        source,
    )
    .map_err(|e| format!("host session pin failed: {e}"))?;
    if prepared.is_none() {
        anyagents_core::host_session::ensure_active_with_source(
            root,
            &request.agent.harness,
            source,
        )
        .map_err(|e| format!("host session pin failed: {e}"))?;
    }
    if let Some(file) = crate::create::harness_session_filename(&request.agent.harness)
        && let Some(id) = session_from_file(&request.config_dir, file)?
    {
        anyagents_core::host_session::bind_vendor(root, &request.agent.harness, &id)
            .map_err(|e| format!("host session alias failed: {e}"))?;
    }
    // Also render in contract-test mode, exercising the shipping launch boundary.
    crate::create::write_agent_instructions(&request.config_dir, &request.agent, None)
        .map_err(|err| err.to_string())?;
    // A session reset used to leave this marker to force a recovery note
    // ahead of new mail; nothing reads it now. Remove once no agent
    // directory carries one.
    let _ = std::fs::remove_file(request.config_dir.join(".anyagents-session-reset-recovery"));
    Ok(())
}

/// Native-memory behavior belongs to explicit operator policy. Without an
/// override, preserve the caller's supported vendor-native setting unchanged.
fn enforce_host_memory_env(harness: &str, env: &mut Vec<(String, String)>) {
    if harness == "claude-code"
        && let Some(policy) = env
            .iter()
            .find(|(key, _)| key == "ANYAGENTS_NATIVE_MEMORY_DISABLED")
            .map(|(_, value)| value.clone())
    {
        env.retain(|(key, _)| key != "CLAUDE_CODE_DISABLE_AUTO_MEMORY");
        env.push(("CLAUDE_CODE_DISABLE_AUTO_MEMORY".into(), policy));
    }
}

fn read_muse_exit_error(config_dir: &Path) -> Option<String> {
    for rel in [
        "data-home/muse/serve-exit.jsonl",
        ".local/share/muse/serve-exit.jsonl",
        "muse/serve-exit.jsonl",
    ] {
        let path = config_dir.join(rel);
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        if let Some(last_line) = text.lines().rev().find(|l| !l.trim().is_empty())
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(last_line)
            && val.pointer("/reason/kind").and_then(|v| v.as_str()) == Some("error")
            && let Some(detail) = val.pointer("/reason/detail").and_then(|v| v.as_str())
            && !detail.trim().is_empty()
        {
            return Some(format!("muse serve exit error: {}", detail.trim()));
        }
    }
    None
}

fn read_opencode_exit_error(config_dir: &Path) -> Option<String> {
    let path = config_dir.join("data-home/opencode/log/opencode.log");
    let text = std::fs::read_to_string(&path).ok()?;
    for line in text.lines().rev() {
        if line.contains("level=ERROR") {
            return Some(format!("opencode serve error: {}", line.trim()));
        }
    }
    None
}

fn vendor_launch_inner(
    mut request: LaunchRequest,
    authority: Option<&anyagents_core::Store>,
    host: &HarnessHost,
) -> Result<VendorHarness, String> {
    crate::worktree_access::ensure_launchable(&request.data_dir, &request.agent.id)
        .map_err(|error| error.to_string())?;
    // Strip any untrusted launch or authority variables from caller/stored env.
    let mut env: Vec<(String, String)> = std::mem::take(&mut request.extra_env)
        .into_iter()
        .filter(|(k, _)| {
            !matches!(
                k.as_str(),
                anyagents_harness::SEAT_LAUNCH_ENV
                    | anyagents_harness::SEAT_PEERS_ENV
                    | "ANYAGENTS_DATA"
                    | anyagents_core::STATE_DIR_ENV
                    | anyagents_core::WORKSPACES_DIR_ENV
                    | anyagents_core::SCRATCH_DIR_ENV
                    | anyagents_core::GIT_POOL_DIR_ENV
                    | "ANYAGENTS_AGENT_CONFIG"
                    | "ANYAGENTS_AGENT_UID"
                    | "ANYAGENTS_OPERATOR_TOKEN_FILE"
                    | "ANYAGENTS_CAPABILITY_KEY"
                    | "LD_PRELOAD"
                    | "LD_LIBRARY_PATH"
            )
        })
        .collect();
    // The data directory the deployment serves, so an agent's tools find the
    // one socket and one config directory that hold its capability. Never a
    // secret: the capability itself stays in a file only this agent reads.
    env.push((
        "ANYAGENTS_DATA".to_string(),
        request.data_dir.display().to_string(),
    ));
    if anyagents_core::storage_env_enabled()
        && std::env::var_os(anyagents_core::STATE_DIR_ENV).is_some()
    {
        env.push((
            anyagents_core::STATE_DIR_ENV.to_string(),
            request.data_dir.display().to_string(),
        ));
    }
    if let Some(workspaces_dir) = anyagents_core::configured_workspaces_dir() {
        env.push((
            anyagents_core::WORKSPACES_DIR_ENV.to_string(),
            workspaces_dir.display().to_string(),
        ));
    }
    if let Some(scratch_dir) = anyagents_core::configured_scratch_dir() {
        env.push((
            anyagents_core::SCRATCH_DIR_ENV.to_string(),
            scratch_dir.display().to_string(),
        ));
        if anyagents_core::storage_env_enabled()
            && let Some(test_flag) = std::env::var_os(anyagents_core::TEST_STORAGE_TIERS_ENV)
        {
            env.push((
                anyagents_core::TEST_STORAGE_TIERS_ENV.to_string(),
                test_flag.to_string_lossy().into_owned(),
            ));
        }
    }
    if let Some(git_pool_dir) = anyagents_core::configured_git_pool_dir() {
        env.push((
            anyagents_core::GIT_POOL_DIR_ENV.to_string(),
            git_pool_dir.display().to_string(),
        ));
    }
    env.push((
        "ANYAGENTS_AGENT_ID".to_string(),
        request.agent.id.as_str().to_string(),
    ));
    env.push((
        "ANYAGENTS_AGENT_NAME".to_string(),
        request.agent.name.clone(),
    ));
    // The agent's own config directory. Its tool server reads the mailbox
    // capability from here and nothing else.
    env.push((
        "ANYAGENTS_AGENT_CONFIG".to_string(),
        request.config_dir.display().to_string(),
    ));
    let root = request.data_dir.clone();
    // The agent's own config directory is the home the vendor tool reads its
    // sign-in from, and the only place AnyAgents writes into it.
    if let Some(log) = std::env::var_os("ANYAGENTS_FAKE_LOG") {
        // The server passes this to every vendor it starts; the doubles write
        // there instead of their own stderr.
        env.push((
            "ANYAGENTS_SHARED_LOG".to_string(),
            log.to_string_lossy().into_owned(),
        ));
    }
    // An agent's own launch environment: the gateway origin for the API it
    // speaks, and this agent's own token. Never a provider key.
    let mut launch_env: Vec<(String, String)> = request
        .agent
        .launch_env
        .iter()
        .filter(|(k, _)| {
            !matches!(
                k.as_str(),
                anyagents_harness::SEAT_LAUNCH_ENV
                    | anyagents_harness::SEAT_PEERS_ENV
                    | "ANYAGENTS_DATA"
                    | anyagents_core::STATE_DIR_ENV
                    | anyagents_core::WORKSPACES_DIR_ENV
                    | anyagents_core::SCRATCH_DIR_ENV
                    | anyagents_core::GIT_POOL_DIR_ENV
                    | "ANYAGENTS_AGENT_CONFIG"
                    | "ANYAGENTS_AGENT_UID"
                    | "ANYAGENTS_OPERATOR_TOKEN_FILE"
                    | "ANYAGENTS_CAPABILITY_KEY"
                    | "LD_PRELOAD"
                    | "LD_LIBRARY_PATH"
            )
        })
        .cloned()
        .collect();
    env.append(&mut launch_env);
    env.push((
        "AGY_CONFIG_DIR".to_string(),
        request.config_dir.display().to_string(),
    ));
    env.push((
        "MUSE_CONFIG_HOME".to_string(),
        request.config_dir.display().to_string(),
    ));
    let reopened;
    let store_opt = if let Some(store) = authority {
        Some(store)
    } else {
        reopened = anyagents_core::Store::open(request.data_dir.join("state.db")).ok();
        reopened.as_ref()
    };
    let seat_hint = store_opt
        .and_then(|s| crate::create::resolve_root_seat_hint(s, &request.agent))
        .or_else(|| {
            env.iter()
                .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
                .map(|(_, v)| v.clone())
        });
    let resolved_seat = store_opt
        .and_then(|s| crate::create::resolved_agent_seat_info(s, &request.agent))
        .or_else(|| crate::sandbox::existing_config_seat(&request.config_dir));
    let seat_spec = resolved_seat
        .as_ref()
        .and_then(|(user, _, _, spec_opt)| spec_opt.as_ref().map(|s| (user.clone(), s.clone())))
        .or_else(|| {
            crate::sandbox::seat_launch_spec_with_hint(
                request.agent.id.as_str(),
                seat_hint.as_deref(),
            )
        });
    if crate::sandbox::seats_configured() && seat_spec.is_none() {
        return Err(format!(
            "no admitted execution principal for {}",
            request.agent.id
        ));
    }
    if let Some((_, uid, gid, _)) = resolved_seat.as_ref() {
        crate::sandbox::prepare_config_with_seat_uid(&request.config_dir, *uid, *gid)
            .map_err(|error| error.to_string())?;
    }
    if request.agent.harness == "agy" {
        ensure_agy_steering_settings(
            &request.config_dir,
            &request.work_dir,
            &request.data_dir.join("media"),
        )
        .map_err(|err| err.to_string())?;
    }
    if std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some() {
        return Err(format!(
            "harness config verification failed for {} ({}): config write suppressed",
            request.agent.name, request.agent.harness
        ));
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() {
        let _ = crate::install::record(&root, &request.agent.harness);
        if let Some(store) = store_opt {
            crate::create::reconcile_agent_runtime_config(
                store,
                &request.data_dir,
                &request.agent,
                &request.config_dir,
                &request.work_dir,
            )
            .map_err(|e| e.to_string())?;
        } else {
            crate::create::write_agent_instructions(
                &request.config_dir,
                &request.agent,
                seat_hint.as_deref(),
            )
            .map_err(|err| err.to_string())?;
            crate::create::verify_harness_runtime_config(
                &request.config_dir,
                &request.agent.harness,
                request.agent.model.as_deref(),
            )
            .map_err(|e| e.to_string())?;
        }
    } else if let Some(store) = store_opt {
        let _ = crate::create::compute_live_session_continuity(
            store,
            &request.agent,
            &request.config_dir,
        );
    }
    if let Some((ref seat_user, ref seat_launch)) = seat_spec {
        crate::create::ensure_seat_environment(&request.config_dir, &request.work_dir)
            .map_err(|e| e.to_string())?;
        let cache_home = request.config_dir.join("cache-home");
        let _ = std::fs::create_dir_all(&cache_home);
        let tmp_dir = request.config_dir.join("tmp");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let data_home = request.config_dir.join("data-home");
        let _ = std::fs::create_dir_all(&data_home);
        let state_home = request.config_dir.join("state-home");
        let _ = std::fs::create_dir_all(&state_home);
        let xdg_config_home = if matches!(request.agent.harness.as_str(), "opencode" | "muse") {
            request.config_dir.clone()
        } else {
            let dot_config = request.config_dir.join(".config");
            let _ = std::fs::create_dir_all(&dot_config);
            dot_config
        };
        let effective_seat = resolved_seat.clone().or_else(|| {
            crate::sandbox::seat_for_agent_with_hint(
                request.agent.id.as_str(),
                seat_hint.as_deref(),
            )
            .map(|(u, uid, gid, _)| (u, uid, gid, None))
        });
        if let Some((_, uid, gid, _)) = effective_seat.as_ref() {
            let (uid, gid) = (*uid, *gid);
            crate::sandbox::share_config_with_seat_uid(&request.config_dir, uid, gid);
            let managed_work = anyagents_core::agent_work_dir(&request.data_dir, &request.agent.id);
            if request.work_dir == managed_work && !managed_work.exists() {
                let _ = crate::sandbox::ensure_seat_dir_as_owner(&managed_work, uid, gid, 0o2770);
            }
            if request.work_dir == managed_work
                && managed_work.is_dir()
                && !managed_work.join(".git").exists()
            {
                let _ = crate::sandbox::share_worktree_with_seat(
                    &managed_work,
                    uid,
                    gid,
                    &crate::worktree_access::grant_path(&request.data_dir, &request.agent.id),
                );
            }
        }
        crate::create::ensure_seat_op_config(&request.config_dir, seat_launch);
        env.push(("HOME".to_string(), request.config_dir.display().to_string()));
        env.push(("USER".to_string(), seat_user.clone()));
        env.push(("LOGNAME".to_string(), seat_user.clone()));
        env.push((
            "XDG_CONFIG_HOME".to_string(),
            xdg_config_home.display().to_string(),
        ));
        env.push(("XDG_DATA_HOME".to_string(), data_home.display().to_string()));
        env.push((
            "XDG_STATE_HOME".to_string(),
            state_home.display().to_string(),
        ));
        env.push((
            "XDG_CACHE_HOME".to_string(),
            cache_home.display().to_string(),
        ));
        env.push(("TMPDIR".to_string(), tmp_dir.display().to_string()));
        if let Some((_, uid, gid, _)) = effective_seat.as_ref()
            && let Some(scratch_root) = anyagents_core::configured_scratch_dir()
        {
            let _ = crate::sandbox::prepare_scratch_dirs_for_seat(
                &scratch_root,
                &request.agent.id,
                *uid,
                *gid,
            );
        }
        for key in ["RUSTUP_HOME", "CARGO_HOME"] {
            if let Some(value) = std::env::var_os(format!("ANYAGENTS_{key}")) {
                env.push((key.to_string(), value.to_string_lossy().into_owned()));
            }
        }
    }
    if let Some(scratch_root) = anyagents_core::configured_scratch_dir() {
        let build_root = anyagents_core::agent_scratch_build_dir(&scratch_root, &request.agent.id);
        let cargo_target = build_root.join("cargo");
        let gradle_home = build_root.join("gradle");
        let derived_data = build_root.join("derived-data");
        let scratch_tmp = anyagents_core::agent_scratch_tmp_dir(&scratch_root, &request.agent.id);
        if seat_spec.is_none() {
            for dir in [&cargo_target, &gradle_home, &derived_data, &scratch_tmp] {
                let _ = std::fs::create_dir_all(dir);
            }
        }
        env.push(("TMPDIR".to_string(), scratch_tmp.display().to_string()));
        env.push(("BUN_TMPDIR".to_string(), scratch_tmp.display().to_string()));
        env.push((
            "CARGO_TARGET_DIR".to_string(),
            cargo_target.display().to_string(),
        ));
        env.push((
            "GRADLE_USER_HOME".to_string(),
            gradle_home.display().to_string(),
        ));
        env.push((
            "DERIVED_DATA_PATH".to_string(),
            derived_data.display().to_string(),
        ));
        for key in [
            "RUSTUP_HOME",
            "CARGO_HOME",
            "SCCACHE_DIR",
            "SCCACHE_CACHE_SIZE",
            "BUN_INSTALL_CACHE_DIR",
            "UV_CACHE_DIR",
            "UV_PYTHON_INSTALL_DIR",
            "PIP_CACHE_DIR",
            "NPM_CONFIG_CACHE",
            "PLAYWRIGHT_BROWSERS_PATH",
        ] {
            if env.iter().any(|(k, _)| k == key) {
                continue;
            }
            if let Some(value) = std::env::var_os(format!("ANYAGENTS_{key}"))
                .or_else(|| std::env::var_os(key))
                .filter(|v| !v.is_empty())
            {
                env.push((key.to_string(), value.to_string_lossy().into_owned()));
            }
        }
    }
    if let Some((_, ref spec)) = seat_spec {
        env.push((anyagents_harness::SEAT_LAUNCH_ENV.to_string(), spec.clone()));
        if let Some(store) = &store_opt {
            env.push((
                anyagents_harness::SEAT_PEERS_ENV.to_owned(),
                crate::sandbox::peer_principals(store).map_err(|e| e.to_string())?,
            ));
        }
    }
    enforce_host_memory_env(&request.agent.harness, &mut env);
    let planned = plan_adapter(&request, &root, env)?;
    let vh = VendorHarness::start(planned, &request, host)?;
    vh.sync_session()?;
    Ok(vh)
}

/// Decide which adapter this agent runs and how it is started. Only what the
/// adapter needs at its start is recorded; everything else was prepared above.
fn plan_adapter(
    request: &LaunchRequest,
    root: &Path,
    mut env: Vec<(String, String)>,
) -> Result<Planned, String> {
    let scripted = std::env::var_os("ANYAGENTS_FAKE_MODE").is_some();
    let model = request
        .agent
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(str::to_string);
    let planned = |launch, pointer: Option<&str>, selector: &str| Planned {
        launch,
        pointer: pointer.map(|name| request.config_dir.join(name)),
        selector: selector.to_string(),
    };
    if let Some(config) = adapter_config(&request.config_dir, &request.agent.harness) {
        let program =
            std::env::var("ANYAGENTS_HARNESS_BIN").unwrap_or_else(|_| "anyagents-harness".into());
        return Ok(planned(
            AdapterLaunch::Contract {
                program,
                config: config.display().to_string(),
                env,
                work_dir: request.work_dir.clone(),
            },
            None,
            &request.agent.harness,
        ));
    }
    if !scripted
        && !is_cloud_harness(&request.agent.harness)
        && anyagents_tools::SandboxConfig::resolve(Some(&request.config_dir))
            .is_some_and(|cfg| cfg.remote_spawn)
    {
        let canonical = anyagents_harness::normalize_harness_name(&request.agent.harness);
        let session_file = format!("{canonical}-session-id");
        let session = session_from_file(&request.config_dir, &session_file)?.or(
            match crate::create::harness_session_filename(canonical) {
                Some(alt) if alt != session_file => session_from_file(&request.config_dir, alt)?,
                _ => None,
            },
        );
        let target_program = crate::install::program(root, canonical);
        let exe = std::env::current_exe()
            .or_else(|_| crate::data::current_exe_clean())
            .unwrap_or_else(|_| PathBuf::from("anyagents"));
        let args = vec![
            "sandbox-exec".to_string(),
            "--config".to_string(),
            request.config_dir.display().to_string(),
            "--harness".to_string(),
            canonical.to_string(),
            "--program".to_string(),
            target_program.display().to_string(),
        ];
        return Ok(planned(
            AdapterLaunch::Claude {
                program: exe.display().to_string(),
                args,
                session,
                env,
                work_dir: request.work_dir.clone(),
                cloud: false,
            },
            Some(&session_file),
            canonical,
        ));
    }
    if request.agent.harness == "agy" {
        retire_poisoned_agy_session(&request.config_dir, anyagents_core::now_ms())?;
        crate::create::clear_stale_agy_quota_error_steps(&request.config_dir);
        let program = std::env::var("ANYAGENTS_AGY_BIN").unwrap_or_else(|_| "agy".to_string());
        let session = session_from_file(&request.config_dir, "agy-session-id")?;
        Ok(planned(
            AdapterLaunch::Agy {
                program,
                session,
                env,
                work_dir: request.work_dir.clone(),
                model,
                scripted,
            },
            Some("agy-session-id"),
            "agy",
        ))
    } else if request.agent.harness == "muse" {
        env.push((
            "XDG_CONFIG_HOME".to_string(),
            request.config_dir.display().to_string(),
        ));
        env.push(("MUSE_NO_AUTO_UPDATE".to_string(), "1".to_string()));
        let program = std::env::var("ANYAGENTS_MUSE_BIN").unwrap_or_else(|_| "muse".to_string());
        let session = session_from_file(&request.config_dir, "muse-session-id")?;
        let args = if scripted {
            Vec::new()
        } else {
            let model = model.unwrap_or_else(|| "muse-wonder-1.0".to_string());
            let mut args = vec![
                "--provider".to_string(),
                "meta".to_string(),
                "--model".to_string(),
                model,
                "--disable-request-user-input".to_string(),
                "--disable-sandbox".to_string(),
                "--trust-workspace".to_string(),
                "-c".to_string(),
                "context.foreign_personal_rules=false".to_string(),
                "-c".to_string(),
                "context.foreign_personal_skills=false".to_string(),
            ];
            if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
                for override_kv in [
                    "local_session_messaging.enabled=false",
                    "tools.web_search.mode=\"off\"",
                    "tools.web_fetch.enabled=false",
                    "run.subagent_delegation_mode=\"off\"",
                    "run.workflow_trigger_mode=\"off\"",
                ] {
                    args.push("-c".to_string());
                    args.push(override_kv.to_string());
                }
            }
            args
        };
        Ok(planned(
            AdapterLaunch::Muse {
                program,
                args,
                session,
                env,
                work_dir: request.work_dir.clone(),
                config_dir: request.config_dir.clone(),
                scripted,
            },
            Some("muse-session-id"),
            "muse",
        ))
    } else if request.agent.harness == "omp" {
        let omp_agent_dir = request.config_dir.join(".omp").join("agent");
        let _ = std::fs::create_dir_all(&omp_agent_dir);
        env.push((
            "PI_CODING_AGENT_DIR".to_string(),
            omp_agent_dir.display().to_string(),
        ));
        let program = crate::install::program(root, &request.agent.harness);
        let session = session_from_file(&request.config_dir, "omp-session-id")?;
        let mut args = Vec::new();
        if let Some(model) = model {
            args.push("--model".to_string());
            args.push(model);
        }
        if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
            let policy_path =
                crate::grok::ensure_omp_policy(&omp_agent_dir).map_err(|err| err.to_string())?;
            args.push("--config".to_string());
            args.push(policy_path.display().to_string());
            args.push("--no-extensions".to_string());
        }
        let sys_prompt = request.config_dir.join("SYSTEM_PROMPT.md");
        if sys_prompt.is_file() {
            args.push("--append-system-prompt".to_string());
            args.push(sys_prompt.display().to_string());
        }
        Ok(planned(
            AdapterLaunch::Omp {
                program: program.display().to_string(),
                args,
                session,
                env,
                work_dir: request.work_dir.clone(),
            },
            Some("omp-session-id"),
            "omp",
        ))
    } else if request.agent.harness == "dsh" {
        let program = crate::install::program(root, &request.agent.harness);
        let session = session_from_file(&request.config_dir, "dsh-session-id")?;
        let patch_path = request.config_dir.join("dsh-mcp-patch.yml");
        let patch = patch_path.is_file().then_some(patch_path);
        Ok(planned(
            AdapterLaunch::Dsh {
                program,
                patch,
                session,
                env,
                work_dir: request.work_dir.clone(),
            },
            Some("dsh-session-id"),
            "dsh",
        ))
    } else if request.agent.harness == "grok" {
        let program = crate::install::program(root, &request.agent.harness);
        let session = session_from_file(&request.config_dir, "grok-session-id")?;
        let exe = crate::data::current_exe_clean().unwrap_or_else(|_| PathBuf::from("anyagents"));
        let mcp_args = vec![
            "--data".to_string(),
            request.data_dir.display().to_string(),
            "mcp".to_string(),
            "--config".to_string(),
            request.config_dir.display().to_string(),
        ];
        Ok(planned(
            AdapterLaunch::Grok {
                program,
                model: request.agent.model.clone(),
                session,
                env,
                work_dir: request.work_dir.clone(),
                exe,
                mcp_args,
            },
            Some("grok-session-id"),
            "grok",
        ))
    } else if is_cloud_harness(&request.agent.harness) {
        let canonical = anyagents_harness::normalize_harness_name(&request.agent.harness);
        let session_file = format!("{canonical}-session-id");
        let session = session_from_file(&request.config_dir, &session_file)?;
        let program = crate::install::program(root, canonical);
        let mut args = vec![
            "cloud-harness".to_string(),
            "--config".to_string(),
            request.config_dir.display().to_string(),
            "--harness".to_string(),
            canonical.to_string(),
        ];
        if let Some(model) = model {
            args.push("--model".to_string());
            args.push(model);
        }
        Ok(planned(
            AdapterLaunch::Claude {
                program: program.display().to_string(),
                args,
                session,
                env,
                work_dir: request.work_dir.clone(),
                cloud: true,
            },
            Some(&session_file),
            canonical,
        ))
    } else if is_spiron_harness(&request.agent.harness) {
        let session = session_from_file(&request.config_dir, "spiron-session-id")?;
        let program = std::env::var("ANYAGENTS_SPIRON_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|_| crate::install::program(root, "spiron"));
        Ok(planned(
            AdapterLaunch::Spiron {
                program,
                config_dir: request.config_dir.clone(),
                work_dir: request.work_dir.clone(),
                agent_id: request.agent.id.as_str().to_string(),
                agent_name: request.agent.name.clone(),
                model: request.agent.model.clone(),
                session,
                env,
            },
            Some("spiron-session-id"),
            &request.agent.harness,
        ))
    } else if request.agent.harness == "opencode" {
        // Each agent gets its own OpenCode data root. Without this every agent
        // shares ~/.local/share/opencode/opencode.db with the desk's own
        // sessions, and a reply read from it is not reliably this turn's.
        // `XDG_DATA_HOME` is the documented root the CLI honours (v2.0.29).
        let data_home = request.config_dir.join("data-home");
        std::fs::create_dir_all(&data_home).map_err(|err| err.to_string())?;
        env.push((
            "XDG_CONFIG_HOME".to_string(),
            request.config_dir.display().to_string(),
        ));
        env.push(("XDG_DATA_HOME".to_string(), data_home.display().to_string()));
        env.push((
            "XDG_STATE_HOME".to_string(),
            request.config_dir.join("state-home").display().to_string(),
        ));
        let program = crate::install::program(root, &request.agent.harness);
        // The session this agent last ran is kept beside its config, so a
        // restart resumes that exact conversation instead of starting one.
        let session = session_from_file(&request.config_dir, "opencode-session-id")?;
        // The service speaks HTTP; the adapter drives it with `opencode api`.
        Ok(planned(
            AdapterLaunch::OpenCode {
                program,
                session,
                env,
                work_dir: request.work_dir.clone(),
                config_dir: request.config_dir.clone(),
            },
            Some("opencode-session-id"),
            "opencode",
        ))
    } else if request.agent.harness == "codex" {
        env.push((
            "CODEX_HOME".to_string(),
            request.config_dir.display().to_string(),
        ));
        let session = session_from_file(&request.config_dir, "codex-session-id")?;
        let program = crate::install::program(root, &request.agent.harness);
        Ok(planned(
            AdapterLaunch::Codex {
                program: program.display().to_string(),
                session,
                env,
                work_dir: request.work_dir.clone(),
            },
            Some("codex-session-id"),
            "codex",
        ))
    } else {
        env.push((
            "CLAUDE_CONFIG_DIR".to_string(),
            request.config_dir.display().to_string(),
        ));
        let session = session_from_file(&request.config_dir, "claude-session-id")?;
        let args = if scripted {
            Vec::new()
        } else {
            let mut args = vec!["--dangerously-skip-permissions".to_string()];
            let mut effort = "medium".to_string();
            if let Some(model) = model.as_deref() {
                let (base_model, variant) = match model.split_once('#') {
                    Some((b, v)) if !b.trim().is_empty() && !v.trim().is_empty() => {
                        (b.trim(), Some(v.trim()))
                    }
                    _ => (model, None),
                };
                args.push("--model".to_string());
                args.push(base_model.to_string());
                if let Some(v) = variant {
                    effort = v.to_string();
                }
            }
            if let Some((_, env_effort)) = env.iter().find(|(k, _)| k == "CLAUDE_CODE_EFFORT_LEVEL")
                && !env_effort.trim().is_empty()
            {
                effort = env_effort.trim().to_string();
            }
            args.push("--effort".to_string());
            args.push(effort);
            args.push("--name".to_string());
            args.push(request.agent.name.clone());
            if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
                args.push("--disallowedTools".to_string());
                args.push(
                    "Task,Agent,TeamCreate,TeamDelete,SendMessage,ScheduleWakeup,Monitor,CronCreate,CronDelete,CronList,RemoteTrigger,PushNotification,WebSearch,WebFetch"
                        .to_string(),
                );
            }
            let sys_prompt = request.config_dir.join("SYSTEM_PROMPT.md");
            if sys_prompt.is_file() {
                args.push("--append-system-prompt-file".to_string());
                args.push(sys_prompt.display().to_string());
            }
            let lock_path = request.config_dir.join(".claude.json.lock");
            let _ = std::fs::remove_dir_all(&lock_path);
            let _ = std::fs::remove_file(&lock_path);
            let mcp_cfg = request.config_dir.join("mcp.json");
            let claude_cfg = request.config_dir.join(".claude.json");
            if mcp_cfg.is_file() {
                args.push("--strict-mcp-config".to_string());
                args.push("--mcp-config".to_string());
                args.push(mcp_cfg.display().to_string());
            } else if claude_cfg.is_file() {
                args.push("--mcp-config".to_string());
                args.push(claude_cfg.display().to_string());
            }
            args
        };
        let program = crate::install::program(root, &request.agent.harness);
        Ok(planned(
            AdapterLaunch::Claude {
                program: program.display().to_string(),
                args,
                session,
                env,
                work_dir: request.work_dir.clone(),
                cloud: false,
            },
            Some("claude-session-id"),
            "claude-code",
        ))
    }
}

/// Ensures `agy`'s `"queuedMessages": "send-immediately"`, `"mcp(anyagents/*)"`
/// tool permission, and `work_dir` workspace trust are active in the agent's
/// config directory and removes any legacy file-hook steering files
/// (`anyagents-steer-hook.sh`, `steer.jsonl`, and the `"anyagents-steer"` entry
/// in `hooks.json`), since `AgyHarness` steers natively via
/// `LanguageServerService/SendUserCascadeMessage`.
pub(crate) fn ensure_agy_steering_settings(
    config_dir: &Path,
    work_dir: &Path,
    media_dir: &Path,
) -> Result<(), anyagents_core::Error> {
    let dir = config_dir.join(".gemini").join("antigravity-cli");
    std::fs::create_dir_all(&dir)?;
    {
        let path = dir.join("settings.json");
        let mut root = crate::mcp_config::read_candidates(std::slice::from_ref(&path))?;
        let map = root.as_object_mut().ok_or_else(|| {
            anyagents_core::Error::Message("AGY settings is not an object".into())
        })?;
        let mut changed = false;
        if map.get("queuedMessages").and_then(|v| v.as_str()) != Some("send-immediately") {
            map.insert(
                "queuedMessages".to_string(),
                serde_json::Value::String("send-immediately".to_string()),
            );
            changed = true;
        }
        if map.get("autoMemory").and_then(|v| v.as_bool()) != Some(false) {
            map.insert("autoMemory".to_string(), serde_json::Value::Bool(false));
            changed = true;
        }
        let perms = map
            .entry("permissions".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if !perms.is_object() {
            *perms = serde_json::json!({});
            changed = true;
        }
        if let Some(perms_obj) = perms.as_object_mut() {
            let allow = perms_obj
                .entry("allow".to_string())
                .or_insert_with(|| serde_json::json!([]));
            if !allow.is_array() {
                *allow = serde_json::json!([]);
                changed = true;
            }
            let mcp_schema_grant = format!(
                "read_file({})",
                config_dir
                    .join(".gemini")
                    .join("antigravity-cli")
                    .join("mcp")
                    .display()
            );
            if let Some(allow_arr) = allow.as_array_mut() {
                for grant in ["mcp(anyagents/*)", mcp_schema_grant.as_str()] {
                    if !allow_arr.iter().any(|v| v.as_str() == Some(grant)) {
                        allow_arr.push(serde_json::Value::String(grant.to_string()));
                        changed = true;
                    }
                }
            }
            let deny = perms_obj
                .entry("deny".to_string())
                .or_insert_with(|| serde_json::json!([]));
            if !deny.is_array() {
                *deny = serde_json::json!([]);
                changed = true;
            }
            if let Some(deny_arr) = deny.as_array_mut() {
                // Keep read_file / view_file out of static permissions.deny
                // (which overrides PreToolUse decision="allow" in agy) so
                // anyagents-native-tool-guard can permit reading lazy MCP tool
                // schemas under ~/.gemini/antigravity-cli/mcp/*.json while
                // denying all workspace and host reads fail-closed.
                const AGY_LEGACY_READ_DENIES: &[&str] = &["read_file", "view_file", "read_file(*)"];
                let before_legacy = deny_arr.len();
                deny_arr.retain(|v| {
                    !v.as_str()
                        .is_some_and(|s| AGY_LEGACY_READ_DENIES.contains(&s))
                });
                if deny_arr.len() != before_legacy {
                    changed = true;
                }
                const AGY_DENIED_NATIVE_TOOLS: &[&str] = &[
                    "invoke_subagent",
                    "define_subagent",
                    "manage_subagents",
                    "send_message",
                    "schedule",
                    "search_web",
                    "read_url_content",
                    "browser_subagent",
                    "open_browser_url",
                    "run_command",
                    "manage_task",
                    "write_file",
                    "edit_file",
                    "grep_search",
                    "find_by_name",
                    "list_dir",
                    "command(*)",
                    "write_file(*)",
                    "read_url(*)",
                    "execute_url(*)",
                ];
                if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
                    for tool in AGY_DENIED_NATIVE_TOOLS {
                        if !deny_arr.iter().any(|v| v.as_str() == Some(*tool)) {
                            deny_arr.push(serde_json::Value::String((*tool).to_string()));
                            changed = true;
                        }
                    }
                } else {
                    let before = deny_arr.len();
                    deny_arr.retain(|v| {
                        !v.as_str()
                            .is_some_and(|s| AGY_DENIED_NATIVE_TOOLS.contains(&s))
                    });
                    if deny_arr.len() != before {
                        changed = true;
                    }
                }
            }
        }
        let work_dir_str = work_dir.display().to_string();
        if !work_dir_str.is_empty() {
            let trusted = map
                .entry("trustedWorkspaces".to_string())
                .or_insert_with(|| serde_json::json!([]));
            if !trusted.is_array() {
                *trusted = serde_json::json!([]);
                changed = true;
            }
            if let Some(trusted_arr) = trusted.as_array_mut()
                && !trusted_arr
                    .iter()
                    .any(|v| v.as_str() == Some(work_dir_str.as_str()))
            {
                trusted_arr.push(serde_json::Value::String(work_dir_str));
                changed = true;
            }
        }
        if changed {
            let body = serde_json::to_vec_pretty(&root)
                .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
            crate::create::write_required_file(&path, &body, 0o664)?;
        }
    }
    let hook_dir = config_dir.join(".gemini").join("config");
    std::fs::create_dir_all(&hook_dir)?;
    let _ = std::fs::remove_file(hook_dir.join("anyagents-steer-hook.sh"));
    let _ = std::fs::remove_file(hook_dir.join("steer.jsonl"));
    let hooks_path = hook_dir.join("hooks.json");
    let mut hooks_root = if hooks_path.exists() {
        crate::mcp_config::read_candidates(std::slice::from_ref(&hooks_path))?
    } else {
        serde_json::json!({})
    };
    let hooks_map = hooks_root
        .as_object_mut()
        .ok_or_else(|| anyagents_core::Error::Message("AGY hooks.json is not an object".into()))?;
    let mut hooks_changed = hooks_map.remove("anyagents-steer").is_some();
    if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
        let desired_guard = serde_json::json!({
            "enabled": true,
            "PreToolUse": [
                {
                    "matcher": "^(invoke_subagent|define_subagent|manage_subagents|send_message|schedule|search_web|read_url_content|browser_subagent|open_browser_url|browser_.*|run_command|manage_task|send_command_input|command_status|shell_exec|read_file|write_file|edit_file|view_file|view_file_outline|view_code_item|view_content_chunk|code_action|grep_search|find_by_name|find|list_dir|list_directory)$",
                    "hooks": [
                        {
                            "type": "command",
                            "command": agy_tool_guard_command(&[work_dir, media_dir]),
                            "timeout": 5
                        }
                    ]
                }
            ]
        });
        if hooks_map.get("anyagents-native-tool-guard") != Some(&desired_guard) {
            hooks_map.insert("anyagents-native-tool-guard".to_string(), desired_guard);
            hooks_changed = true;
        }
    } else if hooks_map.remove("anyagents-native-tool-guard").is_some() {
        hooks_changed = true;
    }
    if hooks_changed {
        if hooks_map.is_empty() {
            let _ = std::fs::remove_file(&hooks_path);
        } else {
            let mut body = serde_json::to_vec_pretty(&hooks_root)
                .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
            body.push(b'\n');
            crate::create::write_required_file(&hooks_path, &body, 0o664)?;
        }
    }
    Ok(())
}

/// The deny reason the guard gives the model for a native tool.
const AGY_GUARD_DENY: &str = "Native built-in tools are disabled under AnyAgents. Invoke AnyAgents MCP tools via call_mcp_tool with ServerName=\"anyagents\" (read schemas in ~/.gemini/antigravity-cli/mcp/anyagents/*.json if needed): e.g. ToolName=\"code\" with Arguments={\"code\":\"return await workspace.read({ path: \\\"...\\\" });\"} for workspace/web/anyagents operations, ToolName=\"send\" with Arguments={\"to\":[\"parent\"],\"text\":\"...\"} to reply, ToolName=\"agents\", ToolName=\"schedules\", or ToolName=\"skills\". read_file stays for an image, PDF or audio file in the workspace or the AnyAgents media directory, and for media this CLI offloaded from a tool result ([Resource offloaded to file://...]).";

/// The `PreToolUse` command that holds AGY's native tools: every one is
/// denied, except `read_file` of an MCP tool schema under
/// `~/.gemini/antigravity-cli/mcp/*.json`, and `read_file` (or `view_file`)
/// of a media file (an image, a PDF or audio, told by its bytes) whose real
/// path is in one of `roots` (the workspace and `<data>/media`) or is media
/// AGY itself offloaded under `~/.gemini/antigravity-cli/brain/
/// */.system_generated/steps/`.
///
/// AGY gives its model an MCP image only when its bytes fit the tool output
/// limit (10,240 bytes by default, `tool_config.mcp.max_output_bytes`); a
/// larger one, such as any photo `code` returns, is written to the step's
/// directory and the model is told only its path. `read_file` gives the model
/// such a file as an image, so the media a message names, and the media a
/// tool returned, stay visible. Everything else is read through `code`.
fn agy_tool_guard_command(roots: &[&Path]) -> String {
    let roots: Vec<String> = roots
        .iter()
        .map(|root| root.display().to_string())
        .collect();
    let roots = serde_json::to_string(&roots).unwrap_or_else(|_| "[]".to_string());
    let deny = serde_json::to_string(AGY_GUARD_DENY).unwrap_or_default();
    let program = format!(
        r#"import json,os,sys
deny={deny}
home=os.path.expanduser("~/.gemini/antigravity-cli")
roots=[os.path.realpath(r) for r in {roots}]
def under(p,r):
 return p.startswith(r.rstrip("/")+"/")
def media(p):
 try:
  with open(p,"rb") as f:h=f.read(12)
 except Exception:
  return False
 return h.startswith((b"\x89PNG\r\n\x1a\n",b"\xff\xd8\xff",b"GIF87a",b"GIF89a",b"%PDF-",b"OggS",b"ID3",b"fLaC")) or (h[:4]==b"RIFF" and h[8:12] in (b"WEBP",b"WAVE"))
try:
 d=json.load(sys.stdin);tc=d.get("toolCall") or {{}}
 if tc.get("name") in ("read_file","view_file"):
  p=(tc.get("args") or {{}}).get("Path") or ""
  rp=os.path.realpath(os.path.expanduser(p)) if p else ""
  mcp=os.path.realpath(os.path.join(home,"mcp"))
  steps=os.path.realpath(os.path.join(home,"brain"))
  schema=under(rp,mcp) and rp.endswith(".json")
  offloaded=under(rp,steps) and "/.system_generated/steps/" in rp
  if rp and os.path.isfile(rp) and (schema or (media(rp) and (offloaded or any(under(rp,r) for r in roots)))):
   print(json.dumps({{"decision":"allow","permissionOverrides":["read_file(*)","read_file","view_file"]}}));sys.exit(0)
except Exception:
 pass
print(json.dumps({{"decision":"deny","reason":deny}}))"#
    );
    format!(
        "python3 -c '{}' 2>/dev/null || printf '%s\\n' '{{\"decision\":\"deny\",\"reason\":\"Native built-in tools are disabled under AnyAgents; invoke AnyAgents MCP tools via call_mcp_tool with ServerName=\\\"anyagents\\\" and ToolName=\\\"code\\\" or \\\"send\\\" instead.\"}}'",
        program.replace('\'', "'\\''")
    )
}

fn start_claude(
    program: &str,
    extra_args: &[String],
    session: Option<&str>,
    env: &[(String, String)],
    work_dir: &Path,
) -> Result<ClaudeHarness, String> {
    let pairs = pairs(env);
    ClaudeHarness::start_in_with_args(program, extra_args, session, &pairs, Some(work_dir))
        .map_err(|err| err.to_string())
}

/// Legacy AnyAgents records named `sylphx` are deliberately only a selector
/// alias.  They launch the external `spiron` process and never enter a local
/// model/tool loop.
pub(crate) fn is_spiron_harness(harness: &str) -> bool {
    anyagents_harness::normalize_harness_name(harness) == "spiron"
}

pub(crate) fn is_cloud_harness(harness: &str) -> bool {
    matches!(
        anyagents_harness::normalize_harness_name(harness),
        "spiron-cloud" | "claude-cloud" | "openai-cloud" | "cursor-cloud"
    )
}

/// Who a Spiron process runs as.
pub(crate) struct SpironAgent<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub model: Option<&'a str>,
}

impl<'a> From<&'a anyagents_core::Agent> for SpironAgent<'a> {
    fn from(agent: &'a anyagents_core::Agent) -> Self {
        Self {
            id: agent.id.as_str(),
            name: &agent.name,
            model: agent.model.as_deref(),
        }
    }
}

fn start_spiron(
    program: &Path,
    config_dir: &Path,
    work_dir: &Path,
    agent: SpironAgent<'_>,
    session: Option<&str>,
    env: &[(String, String)],
) -> Result<SpironHarness, String> {
    let mcp_config = config_dir.join("spiron-mcp.json");
    let skills = config_dir.join(".agents/skills");
    let mut args = vec![
        "--headless".to_string(),
        "--json".to_string(),
        "--mcp-config".to_string(),
        mcp_config.display().to_string(),
        "--workdir".to_string(),
        work_dir.display().to_string(),
        "--skills-dir".to_string(),
        skills.display().to_string(),
        "--state-dir".to_string(),
        config_dir.display().to_string(),
        "--instructions-file".to_string(),
        config_dir.join("SYSTEM_PROMPT.md").display().to_string(),
        "--agent-id".to_string(),
        agent.id.to_string(),
        "--agent-name".to_string(),
        agent.name.to_string(),
    ];
    if let Some(model) = agent.model.map(str::trim).filter(|model| !model.is_empty()) {
        let normalized = crate::spiron::strip_coding_plan_prefix(model);
        args.extend(["--model".to_string(), normalized.to_string()]);
    }
    let mut environment = env.to_vec();
    environment.push(("HOME".to_string(), config_dir.display().to_string()));
    // Profiles are materialized into the seat config by create.rs. Do not pass
    // their coordinator selector as a Spiron TOML filename.
    environment.extend([
        (
            "SPIRON_MCP_CONFIG".to_string(),
            mcp_config.display().to_string(),
        ),
        (
            "SPIRON_WORKSPACE".to_string(),
            work_dir.display().to_string(),
        ),
        ("SPIRON_SKILLS".to_string(), skills.display().to_string()),
    ]);
    let applied = settle_legacy_spiron_hold(config_dir)?;
    // Its `ready` says whether every input may be steered at once; nothing
    // is offered before it, so mail starts on the right rules.
    let harness = SpironHarness::start(
        SpironLaunch {
            program: program.to_path_buf(),
            args,
            environment,
            work_dir: work_dir.to_path_buf(),
            session: session.map(str::to_string),
            state_dir: config_dir.to_path_buf(),
        },
        SPIRON_READY_WAIT,
    )
    .map_err(|err| err.to_string())?;
    harness.recognise_applied(applied);
    Ok(harness)
}

/// How long a Spiron process may take to announce itself before its launch
/// fails, as Codex's `initialize` handshake. An exit ends the wait at once.
const SPIRON_READY_WAIT: Duration = Duration::from_secs(60);

/// The hold a Spiron process from before `steer_consumed` could leave:
/// AnyAgents wrote it before it gave such a process input, naming input it
/// could not yet prove reached the session journal.
const LEGACY_SPIRON_HOLD: &str = "spiron-unconfirmed-steers.json";

/// Where a settled hold is kept, with the ids the journal showed applied:
/// `{"hold": <the hold>, "applied": [ids]}`. Every launch reads it again, so
/// an applied id is answered as a duplicate until it is committed, even when
/// a launch fails after the hold was settled.
const LEGACY_SPIRON_RECONCILED: &str = "spiron-unconfirmed-steers.reconciled.json";

/// Settle a hold an earlier AnyAgents left beside a Spiron agent's config. An
/// id that a user input in the agent's Spiron journal (`sessions.sqlite3`,
/// any session) carries was applied: it is returned, to be committed without
/// being written again. Any other id was never journaled and is delivered as
/// new input. The applied ids are written to the reconciled record before
/// the hold is removed, and are returned on every later launch too.
///
/// Compatibility for agent directories written before the hold was retired;
/// remove, with the reconciled records, once no agent directory carries a
/// hold and every agent's cursor has passed the ids its record names.
fn settle_legacy_spiron_hold(config_dir: &Path) -> Result<Vec<String>, String> {
    let reconciled = config_dir.join(LEGACY_SPIRON_RECONCILED);
    let mut applied: Vec<String> = std::fs::read(&reconciled)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|record| record.get("applied").cloned())
        .and_then(|ids| serde_json::from_value(ids).ok())
        .unwrap_or_default();
    let hold = config_dir.join(LEGACY_SPIRON_HOLD);
    let bytes = match std::fs::read(&hold) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(applied),
        Err(err) => return Err(format!("cannot read {}: {err}", hold.display())),
    };
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    let mut ids: Vec<String> = value
        .get("request_ids")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .chain(value.get("turn_id").and_then(serde_json::Value::as_str))
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect();
    ids.sort();
    ids.dedup();
    applied.extend(spiron_journal_holds(config_dir, &ids));
    applied.sort();
    applied.dedup();
    let record = serde_json::json!({"hold": value, "applied": applied});
    crate::create::write_required_file(&reconciled, record.to_string().as_bytes(), 0o664)
        .map_err(|err| err.to_string())?;
    std::fs::remove_file(&hold)
        .map_err(|err| format!("cannot retire {}: {err}", hold.display()))?;
    Ok(applied)
}

/// The ids among `ids` that a user input in any session of the agent's
/// Spiron journal carries: an event `{"type":"text","data":{"role":"user",
/// "content":"anyagents-message:<id>\n..."}}`. A mention anywhere else (a
/// tool result quoting the id, an assistant reply) is not one. An
/// unreadable journal names none, so that input is delivered rather than
/// lost.
fn spiron_journal_holds(config_dir: &Path, ids: &[String]) -> Vec<String> {
    let db_path = config_dir.join("sessions.sqlite3");
    if ids.is_empty() || !db_path.is_file() {
        return Vec::new();
    }
    let Some(conn) = crate::seat_tree::open_readonly_seat_sqlite(&db_path) else {
        return Vec::new();
    };
    let Ok(mut stmt) = conn.prepare(
        "SELECT 1 FROM session_entries
         WHERE json_valid(event)
           AND json_extract(event, '$.type') = 'text'
           AND json_extract(event, '$.data.role') = 'user'
           AND (json_extract(event, '$.data.content') = ?1
                OR substr(json_extract(event, '$.data.content'), 1, length(?1) + 1)
                   = ?1 || char(10))
         LIMIT 1",
    ) else {
        return Vec::new();
    };
    ids.iter()
        .filter(|id| {
            stmt.exists([format!("anyagents-message:{id}")])
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

fn start_codex(
    program: &str,
    session: Option<&str>,
    env: &[(String, String)],
    work_dir: &Path,
) -> Result<CodexHarness, String> {
    let pairs = pairs(env);
    CodexHarness::start_in(program, session, &pairs, Some(work_dir)).map_err(|err| err.to_string())
}

fn pairs(env: &[(String, String)]) -> Vec<(&str, &str)> {
    env.iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

impl Harness for LocalAdapter {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        match self {
            Self::Claude(harness) => harness.deliver(message),
            Self::Codex(harness) => harness.deliver(message),
            Self::OpenCode(harness) => harness.deliver(message),
            Self::Agy(harness) => harness.deliver(message),
            Self::Muse(harness) => harness.deliver(message),
            Self::Omp(harness) => harness.deliver(message),
            Self::Dsh(harness) => harness.deliver(message),
            Self::Grok(harness) => harness.deliver(message),
            Self::Spiron(harness) => harness.deliver(message),
            Self::Contract(harness) => harness.deliver(message),
        }
    }

    fn proven(&self, message_id: &str) -> Option<anyagents_harness::Proof> {
        match self {
            Self::Claude(harness) => harness.proven(message_id),
            Self::Codex(harness) => harness.proven(message_id),
            Self::OpenCode(harness) => harness.proven(message_id),
            Self::Agy(harness) => harness.proven(message_id),
            Self::Muse(harness) => harness.proven(message_id),
            Self::Omp(harness) => harness.proven(message_id),
            Self::Dsh(harness) => harness.proven(message_id),
            Self::Grok(harness) => harness.proven(message_id),
            Self::Spiron(harness) => harness.proven(message_id),
            Self::Contract(harness) => harness.proven(message_id),
        }
    }

    fn stop(&mut self) {
        match self {
            Self::Claude(harness) => harness.stop(),
            Self::Codex(harness) => harness.stop(),
            Self::OpenCode(harness) => harness.stop(),
            Self::Agy(harness) => harness.stop(),
            Self::Muse(harness) => harness.stop(),
            Self::Omp(harness) => harness.stop(),
            Self::Dsh(harness) => harness.stop(),
            Self::Grok(harness) => harness.stop(),
            Self::Spiron(harness) => harness.stop(),
            Self::Contract(harness) => harness.stop(),
        }
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness) => harness.background(),
            Self::Codex(harness) => harness.background(),
            Self::OpenCode(harness) => harness.background(),
            Self::Agy(harness) => harness.background(),
            Self::Muse(harness) => harness.background(),
            Self::Omp(harness) => harness.background(),
            Self::Dsh(harness) => harness.background(),
            Self::Grok(harness) => harness.background(),
            Self::Spiron(harness) => harness.background(),
            Self::Contract(harness) => harness.background(),
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness) => harness.resume(session),
            Self::Codex(harness) => harness.resume(session),
            Self::OpenCode(harness) => harness.resume(session),
            Self::Agy(harness) => harness.resume(session),
            Self::Muse(harness) => harness.resume(session),
            Self::Omp(harness) => harness.resume(session),
            Self::Dsh(harness) => harness.resume(session),
            Self::Grok(harness) => harness.resume(session),
            Self::Spiron(harness) => harness.resume(session),
            Self::Contract(harness) => harness.resume(session),
        }
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness) => harness.cancel(),
            Self::Codex(harness) => harness.cancel(),
            Self::OpenCode(harness) => harness.cancel(),
            Self::Agy(harness) => harness.cancel(),
            Self::Muse(harness) => harness.cancel(),
            Self::Omp(harness) => harness.cancel(),
            Self::Dsh(harness) => harness.cancel(),
            Self::Grok(harness) => harness.cancel(),
            Self::Spiron(harness) => harness.cancel(),
            Self::Contract(harness) => harness.cancel(),
        }
    }

    fn exit_diagnostic(&self) -> Option<String> {
        match self {
            Self::Claude(harness) => harness.exit_diagnostic(),
            _ => None,
        }
    }

    fn health(&self) -> HarnessHealth {
        match self {
            Self::Claude(harness) => harness.health(),
            Self::Codex(harness) => harness.health(),
            Self::OpenCode(harness) => harness.health(),
            Self::Agy(harness) => harness.health(),
            Self::Muse(harness) => harness.health(),
            Self::Omp(harness) => harness.health(),
            Self::Dsh(harness) => harness.health(),
            Self::Grok(harness) => harness.health(),
            Self::Spiron(harness) => harness.health(),
            Self::Contract(harness) => harness.health(),
        }
    }

    fn input_ready(&self) -> bool {
        match self {
            Self::Claude(harness) => harness.input_ready(),
            Self::Codex(harness) => harness.input_ready(),
            Self::OpenCode(harness) => harness.input_ready(),
            Self::Agy(harness) => harness.input_ready(),
            Self::Muse(harness) => harness.input_ready(),
            Self::Omp(harness) => harness.input_ready(),
            Self::Dsh(harness) => harness.input_ready(),
            Self::Grok(harness) => harness.input_ready(),
            Self::Spiron(harness) => harness.input_ready(),
            Self::Contract(harness) => harness.input_ready(),
        }
    }

    fn last_usage(&self) -> TurnUsage {
        match self {
            Self::Claude(harness) => harness.last_usage(),
            Self::Codex(harness) => harness.last_usage(),
            Self::OpenCode(harness) => harness.last_usage(),
            Self::Agy(harness) => harness.last_usage(),
            Self::Muse(harness) => harness.last_usage(),
            Self::Omp(harness) => harness.last_usage(),
            Self::Dsh(harness) => harness.last_usage(),
            Self::Grok(harness) => harness.last_usage(),
            Self::Spiron(harness) => harness.served_usage(),
            Self::Contract(harness) => harness.last_usage(),
        }
    }

    fn served_usage(&self) -> TurnUsage {
        self.last_usage()
    }

    fn turns_served(&self) -> u32 {
        match self {
            Self::Claude(harness) => harness.turns_served(),
            Self::Codex(harness) => harness.turns_served(),
            Self::OpenCode(harness) => harness.turns_served(),
            Self::Agy(harness) => harness.turns_served(),
            Self::Muse(harness) => harness.turns_served(),
            Self::Omp(harness) => harness.turns_served(),
            Self::Dsh(harness) => harness.turns_served(),
            Self::Grok(harness) => harness.turns_served(),
            Self::Spiron(harness) => harness.turns_served(),
            Self::Contract(harness) => harness.turns_served(),
        }
    }

    fn quiet_for(&self, id: &str, quiet: Duration, patience: Duration) -> bool {
        match self {
            Self::Claude(harness) => harness.quiet_for(id, quiet, patience),
            Self::Codex(harness) => harness.quiet_for(id, quiet, patience),
            Self::OpenCode(harness) => harness.quiet_for(id, quiet, patience),
            Self::Agy(harness) => harness.quiet_for(id, quiet, patience),
            Self::Muse(harness) => harness.quiet_for(id, quiet, patience),
            Self::Omp(harness) => harness.quiet_for(id, quiet, patience),
            Self::Dsh(harness) => harness.quiet_for(id, quiet, patience),
            Self::Grok(harness) => harness.quiet_for(id, quiet, patience),
            Self::Spiron(_) => false,
            Self::Contract(harness) => harness.quiet_for(id, quiet, patience),
        }
    }

    fn ends_on_quiet(&self) -> Option<Duration> {
        match self {
            Self::Claude(harness) => harness.ends_on_quiet(),
            Self::Codex(harness) => harness.ends_on_quiet(),
            Self::OpenCode(harness) => harness.ends_on_quiet(),
            Self::Agy(harness) => harness.ends_on_quiet(),
            Self::Muse(harness) => harness.ends_on_quiet(),
            Self::Omp(harness) => harness.ends_on_quiet(),
            Self::Dsh(harness) => harness.ends_on_quiet(),
            Self::Grok(harness) => harness.ends_on_quiet(),
            Self::Spiron(_) => None,
            Self::Contract(harness) => harness.ends_on_quiet(),
        }
    }

    fn clear_quota(&mut self) {
        match self {
            Self::Claude(harness) => harness.clear_quota(),
            Self::Codex(harness) => harness.clear_quota(),
            Self::OpenCode(harness) => harness.clear_quota(),
            Self::Agy(harness) => harness.clear_quota(),
            Self::Muse(harness) => harness.clear_quota(),
            Self::Omp(harness) => harness.clear_quota(),
            Self::Dsh(harness) => harness.clear_quota(),
            Self::Grok(harness) => harness.clear_quota(),
            Self::Spiron(harness) => harness.clear_quota(),
            Self::Contract(harness) => harness.clear_quota(),
        }
    }

    fn process_id(&self) -> Option<u32> {
        match self {
            Self::Claude(harness) => harness.process_id(),
            Self::Codex(harness) => harness.process_id(),
            Self::OpenCode(harness) => harness.process_id(),
            Self::Agy(harness) => harness.process_id(),
            Self::Muse(harness) => harness.process_id(),
            Self::Omp(harness) => harness.process_id(),
            Self::Dsh(harness) => harness.process_id(),
            Self::Grok(harness) => harness.process_id(),
            Self::Spiron(harness) => harness.process_id(),
            Self::Contract(harness) => harness.process_id(),
        }
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        match self {
            Self::Claude(harness) => harness.watch_activity(notify),
            Self::Codex(harness) => harness.watch_activity(notify),
            Self::OpenCode(harness) => harness.watch_activity(notify),
            Self::Agy(harness) => harness.watch_activity(notify),
            Self::Muse(harness) => harness.watch_activity(notify),
            Self::Omp(harness) => harness.watch_activity(notify),
            Self::Dsh(harness) => harness.watch_activity(notify),
            Self::Grok(harness) => harness.watch_activity(notify),
            Self::Spiron(harness) => harness.watch_activity(notify),
            Self::Contract(harness) => harness.watch_activity(notify),
        }
    }
}

impl Hosted for LocalAdapter {
    fn session_id(&self) -> Option<String> {
        match self {
            Self::Claude(harness) => harness.session_id(),
            Self::Codex(harness) => harness.session_id(),
            Self::OpenCode(harness) => harness.session_id(),
            Self::Agy(harness) => harness.session_id(),
            Self::Muse(harness) => harness.session_id(),
            Self::Omp(harness) => harness.session_id(),
            Self::Dsh(harness) => harness.session_id(),
            Self::Grok(harness) => harness.session_id(),
            Self::Spiron(harness) => harness.session_id(),
            Self::Contract(_) => None,
        }
    }

    fn session_invalidated(&self) -> bool {
        matches!(self, Self::Codex(harness) if harness.thread_invalidated())
    }
}

impl Harness for VendorHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let out = self.adapter_mut().deliver(message);
        self.sync_session().map_err(HarnessError::Failed)?;
        out
    }

    fn proven(&self, message_id: &str) -> Option<anyagents_harness::Proof> {
        self.adapter().proven(message_id)
    }

    fn stop(&mut self) {
        self.adapter_mut().stop();
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        self.adapter_mut().background()
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let out = self.adapter_mut().resume(session);
        self.sync_session().map_err(HarnessError::Failed)?;
        out
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.adapter_mut().cancel()
    }

    fn exit_diagnostic(&self) -> Option<String> {
        self.adapter().exit_diagnostic()
    }

    fn health(&self) -> HarnessHealth {
        if let Err(error) = self.sync_session() {
            eprintln!("session synchronization failed: {error}");
        }
        self.adapter().health()
    }

    fn input_ready(&self) -> bool {
        self.adapter().input_ready()
    }

    fn last_usage(&self) -> TurnUsage {
        if let Err(error) = self.sync_session() {
            eprintln!("session synchronization failed: {error}");
        }
        self.adapter().last_usage()
    }

    fn served_usage(&self) -> TurnUsage {
        self.last_usage()
    }

    fn turns_served(&self) -> u32 {
        self.adapter().turns_served()
    }

    fn quiet_for(&self, id: &str, quiet: Duration, patience: Duration) -> bool {
        self.adapter().quiet_for(id, quiet, patience)
    }

    fn ends_on_quiet(&self) -> Option<Duration> {
        self.adapter().ends_on_quiet()
    }

    fn clear_quota(&mut self) {
        self.adapter_mut().clear_quota()
    }

    fn process_id(&self) -> Option<u32> {
        self.adapter().process_id()
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        self.adapter().watch_activity(notify)
    }
}

/// The session id this agent last ran, kept beside its config.
fn session_from_file(config_dir: &Path, file_name: &str) -> Result<Option<String>, String> {
    let path = config_dir.join(file_name);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("cannot restore {}: {err}", path.display())),
    };
    let id = text.trim();
    if id.is_empty() {
        return Err(format!("empty continuation pointer at {}", path.display()));
    }
    Ok(Some(id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_native_memory_preserves_operator_choice() {
        let mut env = vec![
            ("CLAUDE_CODE_DISABLE_AUTO_MEMORY".into(), "0".into()),
            ("UNRELATED".into(), "kept".into()),
        ];
        enforce_host_memory_env("claude-code", &mut env);
        let output = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                "printf '%s:%s' \"$CLAUDE_CODE_DISABLE_AUTO_MEMORY\" \"$UNRELATED\"",
            ])
            .envs(env)
            .output()
            .unwrap();
        assert_eq!(output.stdout, b"0:kept");
    }

    // Used by the real seat and the unprivileged refusal controls below.
    // Inspect only link metadata; never read or traverse a credential target.
    const NATIVE_FIXTURE_OWNER_CLEANUP: &str = r#"
set -eu
[ "$(readlink "$1/skills")" = "$2" ]
for name in service-account-token connect-token; do
    path="$3/$name"
    [ -L "$path" ] || [ ! -e "$path" ]
done
for name in service-account-token connect-token; do
    path="$3/$name"
    if [ -L "$path" ]; then
        rm -- "$path"
    fi
done
rmdir -- "$3"
rm -- "$1/skills"
rmdir -- "$1"
"#;

    #[test]
    fn native_fixture_owner_cleanup_removes_only_expected_links() {
        for case in ["empty", "links", "regular-token", "unexpected-entry"] {
            let temp = crate::test_tempdir();
            let root = temp.path();
            let agy = root.join("config/.agy");
            let op = root.join("config/.config/op");
            let outside = root.join("outside");
            std::fs::create_dir_all(&agy).unwrap();
            std::fs::create_dir_all(&op).unwrap();
            std::fs::create_dir(&outside).unwrap();
            std::fs::write(outside.join("sentinel"), b"preserved").unwrap();
            std::os::unix::fs::symlink(&outside, agy.join("skills")).unwrap();
            match case {
                "links" => {
                    std::os::unix::fs::symlink(&outside, op.join("service-account-token")).unwrap();
                    std::os::unix::fs::symlink(root.join("absent"), op.join("connect-token"))
                        .unwrap();
                }
                "regular-token" => {
                    std::fs::write(op.join("service-account-token"), b"not a link").unwrap();
                    std::os::unix::fs::symlink(&outside, op.join("connect-token")).unwrap();
                }
                "unexpected-entry" => {
                    std::os::unix::fs::symlink(&outside, op.join("unexpected")).unwrap();
                }
                "empty" => {}
                _ => unreachable!(),
            }
            let output = std::process::Command::new("/bin/sh")
                .args(["-c", NATIVE_FIXTURE_OWNER_CLEANUP, "fixture-owner-cleanup"])
                .arg(&agy)
                .arg(&outside)
                .arg(&op)
                .output()
                .unwrap();
            assert_eq!(
                output.status.success(),
                matches!(case, "empty" | "links"),
                "{case}: {output:?}"
            );
            assert_eq!(
                std::fs::read(outside.join("sentinel")).unwrap(),
                b"preserved"
            );
            if output.status.success() {
                assert!(!agy.exists(), "{case}");
                assert!(!op.exists(), "{case}");
            } else {
                assert!(agy.join("skills").is_symlink(), "{case}");
                if case == "regular-token" {
                    assert_eq!(
                        std::fs::read(op.join("service-account-token")).unwrap(),
                        b"not a link"
                    );
                    assert!(op.join("connect-token").is_symlink());
                } else {
                    assert!(op.join("unexpected").is_symlink());
                }
            }
        }
    }

    /// Required by the Linux native-UID lane, never counted as a green skip.
    /// The executor drops DAC override so a seat-owned 0700 vendor parent really
    /// denies the daemon and exercises the owner projection fallback.
    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires the explicit privileged synthetic native-UID CI lane"]
    fn native_vendor_launch_enforces_shared_boundary_and_real_skill_mounts() {
        use anyagents_core::{NewAgent, Store};
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        assert_eq!(
            std::env::var("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").as_deref(),
            Ok("1")
        );
        assert_eq!(
            crate::sandbox::server_uid(),
            0,
            "privileged fixture executor required"
        );
        let _env = crate::server::tests::env_lock();
        let base = std::env::var_os("ANYAGENTS_NATIVE_SEAT_BASE").unwrap();
        let fixture = tempfile::Builder::new()
            .prefix("vendor-seat-")
            .tempdir_in(base)
            .unwrap();
        let fixture = std::fs::canonicalize(fixture.path()).unwrap();
        let uid = 24031;
        let gid = 24032;
        let directory = |path: &Path, mode, owner, group| {
            std::fs::create_dir_all(path).unwrap_or_else(|error| {
                panic!("create fixture directory {}: {error}", path.display())
            });
            let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
            assert_eq!(unsafe { libc::chown(c.as_ptr(), owner, group) }, 0);
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        };
        let file = |path: &Path, content: &str, mode, group| {
            std::fs::write(path, content).unwrap();
            let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
            assert_eq!(unsafe { libc::chown(c.as_ptr(), 0, group) }, 0);
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        };
        directory(&fixture, 0o711, 0, 0);
        let data = fixture.join("data");
        directory(&data, 0o711, 0, 0);
        directory(&data.join("agents"), 0o711, 0, 0);
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("test-capability-key-0123456789abcdef")
            .unwrap();
        std::fs::set_permissions(
            data.join("state.db"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        unsafe {
            std::env::set_var("ANYAGENTS_LAUNCHER", "/usr/bin/setpriv");
        }
        let agent = store
            .create_agent(NewAgent::new("native-own", "claude-code"))
            .unwrap();
        let colocated = store
            .create_agent(NewAgent::new("native-same-seat", "claude-code"))
            .unwrap();
        let config = anyagents_core::agent_config_dir(&data, &agent.id);
        let root = config.parent().unwrap();
        directory(root, 0o711, 0, 0);
        for path in [
            &config,
            &root.join("memory"),
            &root.join("skills"),
            &root.join("work"),
        ] {
            directory(path, 0o2770, 0, gid);
        }
        file(
            &config.join("mcp-capability"),
            &format!("{}\n", store.issue_capability(&agent.id).unwrap()),
            0o640,
            gid,
        );
        file(
            &root.join("memory/MEMORY.md"),
            "synthetic-memory-before\n",
            0o660,
            gid,
        );
        directory(&root.join("skills/local"), 0o2770, 0, gid);
        file(&root.join("skills/local/SKILL.md"), "# local\n", 0o660, gid);
        let colocated_root = anyagents_core::agent_config_dir(&data, &colocated.id)
            .parent()
            .unwrap()
            .to_owned();
        directory(&colocated_root, 0o711, 0, 0);
        for name in ["config", "memory", "skills"] {
            directory(&colocated_root.join(name), 0o2770, 0, gid);
            file(
                &colocated_root.join(name).join("same-seat"),
                "legitimate shared principal\n",
                0o660,
                gid,
            );
        }
        let peer = data.join("agents/foreign-seat");
        directory(&peer, 0o711, 0, 0);
        let mut protected = Vec::new();
        for name in ["config", "memory", "skills", "sessions", "transcripts"] {
            directory(&peer.join(name), 0o700, 0, 0);
            file(
                &peer.join(name).join("private"),
                "foreign-seat-private\n",
                0o600,
                0,
            );
            protected.push(peer.join(name));
            protected.push(peer.join(name).join("private"));
        }
        directory(&root.join("sessions"), 0o2770, 0, gid);
        file(
            &root.join("sessions/private"),
            "host-only
",
            0o660,
            gid,
        );
        directory(&root.join("transcripts"), 0o700, 0, 0);
        file(
            &root.join("transcripts/private"),
            "host-only
",
            0o600,
            0,
        );
        protected.push(root.join("transcripts/private"));
        for name in [
            "secrets",
            "operator-token",
            "anyagents.sock",
            "identity-boundary.json",
        ] {
            file(&data.join(name), "host-private\n", 0o600, 0);
            protected.push(data.join(name));
        }
        // The executor deliberately has no DAC override. Populate the new
        // fixture before sealing both directories, exactly as a real publisher
        // does; read-only must not mean an impossible setup that never launches.
        directory(&data.join("skills"), 0o755, 0, 0);
        directory(&data.join("skills/global"), 0o755, 0, 0);
        file(
            &data.join("skills/global/SKILL.md"),
            "# global preserved\n",
            0o444,
            0,
        );
        for path in [data.join("skills/global"), data.join("skills")] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o555)).unwrap();
        }
        protected.push(data.join("skills/global/SKILL.md"));
        // Daemon cannot traverse this exact legitimate owner-private parent.
        directory(&config.join(".agy"), 0o700, uid, gid);
        assert_eq!(
            std::fs::read_dir(config.join(".agy")).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied,
            "fixture must actually remove daemon DAC override"
        );
        let old_mask = unsafe { libc::umask(0o077) };
        crate::create::ensure_seat_skills(&config, &fixture);
        unsafe {
            libc::umask(old_mask);
        }
        assert_eq!(
            std::fs::metadata(root.join("skills-view")).unwrap().mode() & 0o777,
            0o755
        );
        assert_eq!(
            std::fs::read_dir(config.join(".agy")).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied,
            "mount fallback must not widen the vendor parent"
        );
        // The real launch sharing boundary must grant the daemon-created 0700
        // mirrors; the fixture does not pre-grant them after umask 077 mounting.
        file(
            &config.join("legacy.sqlite"),
            "legacy sqlite handoff bytes\n",
            0o600,
            gid,
        );
        // Reproduce a daemon-to-seat handoff with old private runtime trees.
        // No pre-grant: production sharing must repair every owned descendant.
        for name in ["cache-home", "tmp"] {
            directory(&config.join(name), 0o700, 0, 0);
            directory(&config.join(name).join("nested"), 0o700, 0, 0);
            directory(&config.join(name).join("nested/__pycache__"), 0o700, 0, 0);
            file(
                &config.join(name).join("nested/retained"),
                "retained-runtime-bytes\n",
                0o600,
                0,
            );
            let foreign = config.join(name).join("foreign-private");
            directory(&foreign, 0o700, uid + 1, gid + 1);
            protected.push(foreign);
            std::os::unix::fs::symlink(&peer, config.join(name).join("peer-link")).unwrap();
        }
        // A recursive group-write operation must not make a valid capability
        // permanently fail its own seat's admission.
        std::fs::set_permissions(
            config.join("mcp-capability"),
            std::fs::Permissions::from_mode(0o660),
        )
        .unwrap();
        let report = config.join("native-vendor-report");
        let probe = fixture.join("vendor-probe.sh");
        file(
            &probe,
            r#"#!/bin/sh
exec 2> "$ANYAGENTS_AGENT_CONFIG/native-vendor-trace"
set -eux
trap 'status=$?; printf "PROBE_EXIT=%s\n" "$status" >&2' 0
[ "$(id -u)" = 24031 ]; [ "$(id -g)" = 24032 ]
[ -x "$ANYAGENTS_DATA/agents" ]; [ ! -r "$ANYAGENTS_DATA/agents" ]
workdir=$(cd "$ANYAGENTS_AGENT_CONFIG/../work" && pwd)
trust=$(/usr/bin/git config --no-includes --file "$ANYAGENTS_AGENT_CONFIG/.gitconfig" --get-all safe.directory)
[ "$trust" = "$(printf '\n%s' "$workdir")" ]
for mirror in .claude .agents .agy; do
  grep -qx '# global preserved' "$ANYAGENTS_AGENT_CONFIG/$mirror/skills/global/SKILL.md"
  grep -qx '# local' "$ANYAGENTS_AGENT_CONFIG/$mirror/skills/local/SKILL.md"
done
for name in cache-home tmp; do
  runtime="$ANYAGENTS_AGENT_CONFIG/$name"
  grep -qxE '(retained-runtime-bytes|changed-runtime-bytes)' "$runtime/nested/retained"
  printf changed-runtime-bytes > "$runtime/nested/retained"
  printf created > "$runtime/nested/__pycache__/created"
  rm "$runtime/nested/__pycache__/created"
  [ ! -r "$runtime/foreign-private" ]; [ ! -x "$runtime/foreign-private" ]
  [ ! -r "$runtime/peer-link/config/private" ]
done
printf synthetic-memory-after > "$ANYAGENTS_AGENT_CONFIG/../memory/MEMORY.md"
printf authored > "$ANYAGENTS_AGENT_CONFIG/../skills/authored"
rm "$ANYAGENTS_AGENT_CONFIG/../skills/authored"
grep -qx host-only "$ANYAGENTS_AGENT_CONFIG/../sessions/private"
printf 'host-only\n' > "$ANYAGENTS_AGENT_CONFIG/../sessions/private"
printf 'created\n' > "$ANYAGENTS_AGENT_CONFIG/../sessions/created"
grep -qx created "$ANYAGENTS_AGENT_CONFIG/../sessions/created"
rm "$ANYAGENTS_AGENT_CONFIG/../sessions/created"
for path in "$ANYAGENTS_DATA/secrets" "$ANYAGENTS_DATA/operator-token" "$ANYAGENTS_DATA/agents/foreign-seat/memory/private" "$ANYAGENTS_DATA/agents/foreign-seat/sessions/private" "$ANYAGENTS_DATA/agents/foreign-seat/transcripts/private" "$ANYAGENTS_AGENT_CONFIG/../transcripts/private"; do
  if cat "$path" >/dev/null 2>&1; then exit 41; fi
  if (printf forbidden > "$path") 2>/dev/null; then exit 42; fi
done
if (printf forbidden > "$ANYAGENTS_AGENT_CONFIG/.agy/skills/global/SKILL.md") 2>/dev/null; then exit 43; fi
printf 'ACTUAL_UID=%s PRIMARY_GID=%s REAL_MOUNTS_UMASK077_OWNER_FALLBACK_OK SAME_SEAT_OK MEMORY_RW_OK SESSIONS_RW_OK PRIVATE_GLOBAL_DENIED GIT_TRUST_EXACT\n' "$(id -u)" "$(id -g)" > "$ANYAGENTS_AGENT_CONFIG/native-vendor-report.tmp"
mv "$ANYAGENTS_AGENT_CONFIG/native-vendor-report.tmp" "$ANYAGENTS_AGENT_CONFIG/native-vendor-report"
# Keep the real adapter child alive until its owner drops the harness.
cat >/dev/null
"#,
            0o555,
            0,
        );
        let snapshot = || {
            protected
                .iter()
                .map(|p| {
                    let m = std::fs::symlink_metadata(p).unwrap();
                    (
                        p.clone(),
                        m.uid(),
                        m.gid(),
                        m.mode(),
                        if m.is_file() {
                            std::fs::read(p).unwrap()
                        } else {
                            Vec::new()
                        },
                    )
                })
                .collect::<Vec<_>>()
        };
        let before = snapshot();
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
            std::env::set_var("ANYAGENTS_PRINCIPALS", serde_json::json!({
                agent.id.as_str(): {"user":"synthetic-worker", "uid":uid, "gid":gid, "socket_gid":24033},
                colocated.id.as_str(): {"user":"synthetic-worker", "uid":uid, "gid":gid, "socket_gid":24033}
            }).to_string());
            std::env::set_var("ANYAGENTS_AGENT_UID", uid.to_string());
            std::env::set_var("ANYAGENTS_AGENT_GID", gid.to_string());
            std::env::set_var("ANYAGENTS_SOCKET_GID", "24033");
            std::env::set_var("ANYAGENTS_CLAUDE_CODE_BIN", &probe);
            std::env::set_var("ANYAGENTS_LAUNCHER", "/usr/bin/setpriv");
        }
        let launch = || {
            vendor_launch_with_store(
                LaunchRequest {
                    agent: agent.clone(),
                    data_dir: data.clone(),
                    config_dir: config.clone(),
                    work_dir: root.join("work"),
                    extra_env: vec![],
                    lock: None,
                },
                &store,
            )
        };
        assert_eq!(
            crate::install::program(&data, "claude-code"),
            probe,
            "the actual resolver must select the synthetic native probe"
        );
        // The same-seat exception requires the host registry, never fixture
        // ownership alone. Keep this positive control on the production resolver.
        let peers: Vec<anyagents_harness::SeatPeer> =
            serde_json::from_str(&crate::sandbox::peer_principals(&store).unwrap()).unwrap();
        assert_eq!(peers.len(), 2);
        assert!(
            peers
                .iter()
                .any(|peer| { peer.root == colocated_root && peer.uid == uid && peer.gid == gid })
        );
        let harness = launch().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !report.is_file() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            std::fs::metadata(config.join("legacy.sqlite"))
                .unwrap()
                .uid(),
            uid
        );
        assert_eq!(
            std::fs::read(config.join("legacy.sqlite")).unwrap(),
            b"legacy sqlite handoff bytes\n"
        );
        assert_eq!(
            std::fs::metadata(config.join(".agy")).unwrap().mode() & 0o777,
            0o770,
            "config preparation must restore the admitted daemon group's access"
        );
        println!("NATIVE_VENDOR_LEGACY_SQLITE_OWNER_AND_BYTES_RETAINED");
        let evidence = std::fs::read_to_string(&report).unwrap_or_else(|error| {
            panic!(
                "native vendor report missing: {error}; process={:?}; synthetic_trace={}",
                harness.exit_diagnostic(),
                std::fs::read_to_string(config.join("native-vendor-trace"))
                    .unwrap_or_else(|e| format!("unavailable: {e}"))
            );
        });
        assert!(
            evidence.contains("REAL_MOUNTS_UMASK077_OWNER_FALLBACK_OK SAME_SEAT_OK"),
            "{evidence}"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap(),
            "synthetic-memory-after"
        );
        assert!(evidence.contains("GIT_TRUST_EXACT"), "{evidence}");
        assert_eq!(
            std::fs::metadata(config.join("mcp-capability"))
                .unwrap()
                .mode()
                & 0o777,
            0o640
        );
        for name in ["cache-home", "tmp"] {
            assert_eq!(
                std::fs::read(config.join(name).join("nested/retained")).unwrap(),
                b"changed-runtime-bytes"
            );
            for suffix in ["", "nested", "nested/__pycache__"] {
                let meta = std::fs::metadata(config.join(name).join(suffix)).unwrap();
                assert_eq!(
                    (meta.uid(), meta.gid(), meta.mode() & 0o7777),
                    (0, gid, 0o2770)
                );
            }
        }
        println!("NATIVE_VENDOR_RUNTIME_CACHE_TMP_HANDOFF_AND_CAPABILITY_NORMALIZATION_OK");
        println!("{evidence}");
        drop(harness);
        std::fs::remove_file(&report).unwrap();
        // Cover the other historical ownership branch without host DAC override.
        let capability = config.join("mcp-capability");
        let c_cap = std::ffi::CString::new(capability.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::chown(c_cap.as_ptr(), uid, gid) }, 0);
        let changed = std::process::Command::new("/usr/bin/setpriv")
            .args([
                "--reuid",
                &uid.to_string(),
                "--regid",
                &gid.to_string(),
                "--clear-groups",
                "--no-new-privs",
                "--",
                "/bin/chmod",
                "660",
            ])
            .arg(&capability)
            .status()
            .unwrap();
        assert!(changed.success());
        let seat_owned_harness = launch().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !std::fs::read_to_string(&report).is_ok_and(|text| text.contains("GIT_TRUST_EXACT"))
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            std::fs::read_to_string(&report).is_ok_and(
                |text| text.contains("REAL_MOUNTS_UMASK077_OWNER_FALLBACK_OK SAME_SEAT_OK")
            ),
            "seat-owned capability must admit the actual vendor command"
        );
        assert_eq!(
            std::fs::metadata(&capability).unwrap().mode() & 0o777,
            0o640
        );
        drop(seat_owned_harness);
        // The second command was observed through its real report before stop.
        let _ = std::fs::remove_file(&report);
        println!("NATIVE_VENDOR_SEAT_OWNED_CAPABILITY_NORMALIZED_OK");
        // A real admitted child inherits group-writable defaults even when the
        // daemon inherited 077. Explicit 0600 replacements still need the next
        // pre-read config repair; a umask cannot add permissions to such files.
        let agy_probe = fixture.join("agy-config-probe.sh");
        file(
            &agy_probe,
            r#"#!/bin/sh
set -eu
[ "$(umask)" = 0007 ]
base="$ANYAGENTS_AGENT_CONFIG/.gemini/antigravity-cli"
grep -q 'send-immediately' "$base/settings.json"
cp "$base/settings.json" "$ANYAGENTS_AGENT_CONFIG/agy-before-start.json"
mkdir -p "$base/builtin" "$base/conversations"
printf default > "$base/conversations/default"
printf '{"queuedMessages":"queued","retained":"synthetic"}' > "$base/settings.new"
chmod 600 "$base/settings.new"
mv "$base/settings.new" "$base/settings.json"
printf ready > "$ANYAGENTS_AGENT_CONFIG/agy-config-ready.tmp"
mv "$ANYAGENTS_AGENT_CONFIG/agy-config-ready.tmp" "$ANYAGENTS_AGENT_CONFIG/agy-config-ready"
cat >/dev/null
"#,
            0o555,
            0,
        );
        let mut agy_agent = agent.clone();
        agy_agent.harness = "agy".into();
        unsafe {
            std::env::set_var("ANYAGENTS_AGY_BIN", &agy_probe);
        }
        let old_mask = unsafe { libc::umask(0o077) };
        let agy_launch = vendor_launch_with_store(
            LaunchRequest {
                agent: agy_agent.clone(),
                data_dir: data.clone(),
                config_dir: config.clone(),
                work_dir: root.join("work"),
                extra_env: vec![],
                lock: None,
            },
            &store,
        );
        unsafe {
            libc::umask(old_mask);
        }
        let agy_harness = agy_launch.unwrap();
        let ready = config.join("agy-config-ready");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "admitted AGY fixture did not complete");
        drop(agy_harness);
        let vendor_dir = config.join(".gemini/antigravity-cli");
        let settings = vendor_dir.join("settings.json");
        assert_eq!(
            std::fs::metadata(vendor_dir.join("conversations"))
                .unwrap()
                .mode()
                & 0o7777,
            0o2770
        );
        assert_eq!(
            std::fs::metadata(vendor_dir.join("conversations/default"))
                .unwrap()
                .mode()
                & 0o777,
            0o660
        );
        assert_eq!(
            std::fs::read(&settings).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert!(
            ensure_agy_steering_settings(&config, &root.join("work"), &root.join("media")).is_err()
        );
        // Relaunch with the inaccessible replacement still in place. This
        // exercises actual pre-read preparation, not a helper-only repair.
        std::fs::remove_file(&ready).unwrap();
        let relaunched = vendor_launch_with_store(
            LaunchRequest {
                agent: agy_agent,
                data_dir: data.clone(),
                config_dir: config.clone(),
                work_dir: root.join("work"),
                extra_env: vec![],
                lock: None,
            },
            &store,
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "AGY relaunch refused before config repair");
        drop(relaunched);
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(config.join("agy-before-start.json")).unwrap())
                .unwrap();
        assert_eq!(value["retained"], "synthetic");
        assert_eq!(value["queuedMessages"], "send-immediately");
        crate::sandbox::prepare_config_with_seat_uid(&config, uid, gid).unwrap();
        ensure_agy_steering_settings(&config, &root.join("work"), &root.join("media")).unwrap();
        assert_eq!(std::fs::metadata(&settings).unwrap().mode() & 0o060, 0o060);
        println!("NATIVE_AGY_GROUP_DEFAULTS_AND_PRIVATE_REPLACEMENT_REPAIRED");
        for name in ["memory", "skills", "config"] {
            std::fs::set_permissions(peer.join(name), std::fs::Permissions::from_mode(0o777))
                .unwrap();
            let error = match launch() {
                Ok(_) => panic!("writable foreign {name} started vendor"),
                Err(e) => e,
            };
            assert!(
                error.contains(
                    "agent not started: boundary check failed: peer seats' private trees are denied"
                ),
                "{name}: {error}"
            );
            assert!(!report.exists(), "vendor command ran before refusal");
            std::fs::set_permissions(peer.join(name), std::fs::Permissions::from_mode(0o700))
                .unwrap();
            println!("NATIVE_VENDOR_WRITABLE_PEER_{name}_REFUSED_BEFORE_COMMAND");
        }
        // A distinct reader of a 0711 tree cannot list it but can still read a
        // known public child. Refuse search access before the actual adapter
        // starts, not merely directory read/write access in a synthetic shell.
        for name in ["memory", "skills", "config"] {
            let tree = peer.join(name);
            let known = tree.join("known-readable");
            file(&known, "synthetic-known-peer-bytes\n", 0o444, 0);
            std::fs::set_permissions(&tree, std::fs::Permissions::from_mode(0o711)).unwrap();
            let control = std::process::Command::new("/usr/bin/setpriv")
                .args([
                    "--reuid",
                    "24031",
                    "--regid",
                    "24032",
                    "--clear-groups",
                    "--no-new-privs",
                    "--",
                    "/bin/sh",
                    "-c",
                    "[ ! -r \"$1\" ] && [ ! -w \"$1\" ] && [ -x \"$1\" ] && cat \"$2\"",
                    "search-only-control",
                ])
                .arg(&tree)
                .arg(&known)
                .output()
                .unwrap();
            assert!(control.status.success(), "{name}: {control:?}");
            assert_eq!(control.stdout, b"synthetic-known-peer-bytes\n");
            let error = match launch() {
                Ok(_) => panic!("search-only foreign {name} started vendor"),
                Err(error) => error,
            };
            assert!(
                error.contains(
                    "agent not started: boundary check failed: peer seats' private trees are denied"
                ),
                "{name}: {error}"
            );
            assert!(
                !report.exists(),
                "vendor command ran before search-only refusal"
            );
            std::fs::set_permissions(&tree, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::remove_file(known).unwrap();
            println!("NATIVE_VENDOR_SEARCH_ONLY_PEER_{name}_KNOWN_READ_REFUSED_BEFORE_COMMAND");
        }
        // The workspace-session initializer runs before normal Muse adapter
        // launch, including on explicit wake. A fresh marker must not conceal
        // its exceptional process-construction route from admission.
        let muse_report = config.join("muse-initializer-processes");
        let muse_probe = fixture.join("muse-initializer.sh");
        file(
            &muse_probe,
            r#"#!/bin/sh
set -eu
[ "$(id -u)" = 24031 ]; [ "$(id -g)" = 24032 ]
[ -z "${ANYAGENTS_SEAT_LAUNCH+x}" ]; [ -z "${ANYAGENTS_SEAT_PEERS+x}" ]
printf 'started\n' >> "$ANYAGENTS_AGENT_CONFIG/muse-initializer-processes"
read -r initialize
printf '{"jsonrpc":"2.0","id":1,"result":{}}\n'
read -r initialized
read -r start
printf '{"jsonrpc":"2.0","id":2,"result":{"session":{"sessionId":"synthetic-muse-session"}}}\n'
"#,
            0o555,
            0,
        );
        let seat_environment = vec![
            (
                anyagents_harness::SEAT_LAUNCH_ENV.into(),
                format!("/usr/bin/setpriv\t{uid}\t{gid}\t{gid},24033"),
            ),
            (
                anyagents_harness::SEAT_PEERS_ENV.into(),
                crate::sandbox::peer_principals(&store).unwrap(),
            ),
        ];
        unsafe {
            std::env::remove_var("ANYAGENTS_FAKE_MODE");
            std::env::set_var("ANYAGENTS_MUSE_BIN", &muse_probe);
        }
        let initialize_muse = || {
            crate::muse_config::ensure_workspace_session(
                &data,
                agent.id.as_str(),
                &config,
                &root.join("work"),
                None,
                &seat_environment,
            )
        };
        let sid = config.join("muse-session-id");
        let marker = config.join(".anyagents-muse-workspace-session");
        for name in ["memory", "skills", "config"] {
            assert!(
                !sid.exists() && !marker.exists(),
                "must exercise a fresh Muse session"
            );
            std::fs::set_permissions(peer.join(name), std::fs::Permissions::from_mode(0o777))
                .unwrap();
            let error = initialize_muse().unwrap_err().to_string();
            assert!(
                error.contains(
                    "agent not started: boundary check failed: peer seats' private trees are denied"
                ),
                "{error}"
            );
            assert!(
                !muse_report.exists(),
                "Muse initializer started before refusal"
            );
            assert!(
                !sid.exists() && !marker.exists(),
                "refusal must not publish a session"
            );
            std::fs::set_permissions(peer.join(name), std::fs::Permissions::from_mode(0o700))
                .unwrap();
            println!("NATIVE_MUSE_FRESH_SESSION_PEER_{name}_REFUSED_ZERO_PROCESSES");
        }
        initialize_muse().unwrap();
        assert_eq!(
            std::fs::read_to_string(&sid).unwrap(),
            "synthetic-muse-session\n"
        );
        assert_eq!(
            std::fs::read_to_string(&marker).unwrap(),
            format!("synthetic-muse-session\t{}\n", root.join("work").display())
        );
        initialize_muse().unwrap();
        assert_eq!(
            std::fs::read_to_string(&muse_report).unwrap(),
            "started\n",
            "valid marker reuses exactly one admitted initializer"
        );
        println!("NATIVE_MUSE_INITIALIZER_ADMITTED_AND_MARKER_REUSED");
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }

        // Use the actual Codex adapter, not a Claude command with a different
        // label. Its active .agents view works while unused .claude projections
        // are deliberately preserved by the production mount implementation.
        let personal = fixture.join("personal-skills");
        directory(&personal, 0o755, 0, 0);
        directory(&personal.join("private"), 0o755, 0, 0);
        file(
            &personal.join("private/SKILL.md"),
            "personal preserved\n",
            0o444,
            0,
        );
        let codex_probe = fixture.join("codex-foreign-projection.sh");
        file(
            &codex_probe,
            r#"#!/bin/sh
set -eu
[ "$(id -u)" = 24031 ]; [ "$(id -g)" = 24032 ]
grep -qx '# global preserved' "$ANYAGENTS_AGENT_CONFIG/.agents/skills/global/SKILL.md"
grep -qx '# local' "$ANYAGENTS_AGENT_CONFIG/.agents/skills/local/SKILL.md"
printf 'codex-started\n' > "$ANYAGENTS_AGENT_CONFIG/codex-foreign-report"
while IFS= read -r request; do
  id=$(printf '%s\n' "$request" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  if [ -n "$id" ]; then printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"foreign-projection-fixture"}}}\n' "$id"; fi
done
"#,
            0o555,
            0,
        );
        unsafe {
            std::env::set_var("ANYAGENTS_CODEX_BIN", &codex_probe);
        }
        std::fs::rename(config.join(".claude"), config.join(".claude-host-held")).unwrap();
        for foreign_parent in [true, false] {
            if foreign_parent {
                std::os::unix::fs::symlink(&personal, config.join(".claude")).unwrap();
            } else {
                directory(&config.join(".claude"), 0o2770, 0, gid);
                std::os::unix::fs::symlink(&personal, config.join(".claude/skills")).unwrap();
            }
            crate::create::ensure_seat_skills(&config, &fixture);
            let mut codex = agent.clone();
            codex.harness = "codex".into();
            let harness = vendor_launch_with_store(
                LaunchRequest {
                    agent: codex,
                    data_dir: data.clone(),
                    config_dir: config.clone(),
                    work_dir: root.join("work"),
                    extra_env: vec![],
                    lock: None,
                },
                &store,
            )
            .unwrap();
            assert_eq!(
                std::fs::read_to_string(config.join("codex-session-id"))
                    .unwrap()
                    .trim(),
                "foreign-projection-fixture"
            );
            assert_eq!(
                std::fs::read_to_string(config.join("codex-foreign-report")).unwrap(),
                "codex-started\n"
            );
            assert_eq!(
                std::fs::read_to_string(personal.join("private/SKILL.md")).unwrap(),
                "personal preserved\n"
            );
            assert!(!personal.join("skills").exists() && !personal.join("global").exists());
            drop(harness);
            std::fs::remove_file(config.join("codex-foreign-report")).unwrap();
            if foreign_parent {
                std::fs::remove_file(config.join(".claude")).unwrap();
            } else {
                std::fs::remove_file(config.join(".claude/skills")).unwrap();
                // Launch reconciliation writes its managed instructions into
                // this fixture-owned parent. Remove only that proven output;
                // unexpected entries must still make remove_dir fail.
                let generated = config.join(".claude/CLAUDE.md");
                assert_eq!(
                    std::fs::read(&generated).unwrap(),
                    std::fs::read(config.join("CLAUDE.md")).unwrap()
                );
                std::fs::remove_file(generated).unwrap();
                std::fs::remove_dir(config.join(".claude")).unwrap();
            }
            println!(
                "NATIVE_CODEX_PRESERVED_FOREIGN_{}_LAUNCH_OK",
                if foreign_parent { "PARENT" } else { "MOUNT" }
            );
        }
        std::fs::rename(config.join(".claude-host-held"), config.join(".claude")).unwrap();
        assert_eq!(snapshot(), before, "protected bytes or permissions changed");
        println!("NATIVE_VENDOR_PROTECTED_BEFORE_EQUALS_AFTER");
        // The protected foreign-owner controls are empty leaves created only
        // by this fixture. Unlink them through the admitted writable parent;
        // never widen their private mode merely to perform teardown.
        for name in ["cache-home", "tmp"] {
            std::fs::remove_dir(config.join(name).join("foreign-private")).unwrap();
        }
        // Native launch also creates a private op leaf. Prove that the reduced-
        // DAC daemon cannot enumerate it before its actual owner removes only
        // known token links and the empty directory. Do not widen private modes
        // or recursively traverse any foreign link during fixture teardown.
        let op = config.join(".config/op");
        let metadata = std::fs::symlink_metadata(&op).unwrap();
        assert!(metadata.is_dir());
        assert_eq!((metadata.uid(), metadata.gid()), (uid, gid));
        assert_eq!(metadata.mode() & 0o7777, 0o700);
        assert_eq!(
            std::fs::read_dir(&op).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        println!("NATIVE_VENDOR_PRIVATE_OP_DENIED_BEFORE_OWNER_CLEANUP");
        let output = std::process::Command::new("/usr/bin/setpriv")
            .args([
                "--reuid",
                "24031",
                "--regid",
                "24032",
                "--clear-groups",
                "--no-new-privs",
                "--",
                "/bin/sh",
                "-c",
                NATIVE_FIXTURE_OWNER_CLEANUP,
                "fixture-owner-cleanup",
            ])
            .arg(config.join(".agy"))
            .arg(root.join("skills-view"))
            .arg(&op)
            .output()
            .unwrap();
        assert!(output.status.success(), "owner fixture cleanup: {output:?}");
        assert!(!config.join(".agy").exists());
        assert!(!op.exists());
        // Global read-only fixtures are daemon-owned, but deletion still
        // requires owner write permission after all preservation assertions.
        for path in [data.join("skills/global"), data.join("skills")] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        std::fs::remove_dir_all(&fixture).unwrap();
        assert!(!fixture.exists());
        println!("NATIVE_VENDOR_FIXTURE_REMOVED");
    }

    #[test]
    fn a_vendor_session_is_recorded_once_not_on_every_health_read() {
        use anyagents_core::host_session;
        let dir = crate::test_tempdir();
        let root = dir.path().join("agent");
        let config = root.join("config");
        std::fs::create_dir_all(&config).unwrap();
        let first = host_session::new_session(&root, "spiron").unwrap();
        let harness = SpironHarness::start(
            SpironLaunch {
                program: "/bin/sh".into(),
                args: vec![
                    "-c".into(),
                    r#"echo '{"type":"ready","protocol":"spiron.headless/v1"}'; exec sleep 30"#
                        .into(),
                ],
                environment: vec![],
                work_dir: config.clone(),
                session: Some("vendor-1".into()),
                state_dir: config.clone(),
            },
            Duration::from_secs(10),
        )
        .unwrap();
        let pointer = config.join("spiron-session-id");
        let vendor = VendorHarness::local(
            LocalAdapter::Spiron(Box::new(harness)),
            Some(pointer.clone()),
            "spiron",
        );
        vendor.sync_session().unwrap();
        assert_eq!(std::fs::read_to_string(&pointer).unwrap(), "vendor-1\n");
        assert_eq!(
            host_session::bound_vendor(&root, "spiron", "vendor-1").unwrap(),
            Some(first)
        );
        // Neither the registry nor the pointer is read again for the same
        // session: a registry that can no longer be parsed is not noticed.
        let registry = root.join("sessions/registry.json");
        let saved = std::fs::read(&registry).unwrap();
        std::fs::write(&registry, b"not json").unwrap();
        for _ in 0..3 {
            vendor.sync_session().unwrap();
            let _ = vendor.health();
        }
        assert_eq!(std::fs::read(&registry).unwrap(), b"not json");
        std::fs::write(&registry, saved).unwrap();
        assert_eq!(std::fs::read_to_string(&pointer).unwrap(), "vendor-1\n");
    }

    #[test]
    fn shipping_launch_boundary_pins_before_marker_and_refreshes_only_on_cutover() {
        use anyagents_core::{NewAgent, Store, host_session};
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let mut agent = store
            .create_agent(NewAgent::new("launch-pin", "claude-code"))
            .unwrap();
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&config).unwrap();
        let root = config.parent().unwrap();
        std::fs::create_dir_all(root.join("memory")).unwrap();
        std::fs::write(root.join("memory/MEMORY.md"), "before dispatch").unwrap();
        let request = |agent| LaunchRequest {
            agent,
            data_dir: dir.path().to_owned(),
            config_dir: config.clone(),
            work_dir: root.join("work"),
            extra_env: vec![],
            lock: None,
        };
        prepare_host_session(&request(agent.clone())).unwrap();
        let first = host_session::active(root).unwrap().unwrap();
        assert!(
            std::fs::read_to_string(config.join("CLAUDE.md"))
                .unwrap()
                .contains(&first.memory.section)
        );
        std::fs::write(root.join("memory/MEMORY.md"), "next session only").unwrap();
        std::fs::write(config.join("claude-session-id"), "late-marker").unwrap();
        prepare_host_session(&request(agent.clone())).unwrap();
        agent.model = Some("changed model".into());
        prepare_host_session(&request(agent.clone())).unwrap();
        assert_eq!(host_session::active(root).unwrap().unwrap(), first);
        assert_eq!(
            host_session::resume_vendor(root, "claude-code", "late-marker").unwrap(),
            first
        );
        agent.harness = "codex".into();
        std::fs::write(config.join("codex-session-id"), "old-codex-resume-pointer").unwrap();
        prepare_host_session(&request(agent.clone())).unwrap();
        let next = host_session::active(root).unwrap().unwrap();
        assert_ne!(next.generation, first.generation);
        assert!(next.memory.section.contains("next session only"));
        assert!(!config.join("codex-session-id").exists());
        assert!(
            std::fs::read_to_string(config.join("AGENTS.md"))
                .unwrap()
                .contains(&next.memory.section)
        );
        prepare_host_session(&request(agent)).unwrap();
        assert_eq!(host_session::active(root).unwrap().unwrap(), next);
    }

    #[test]
    fn spiron_host_pin_adopts_legacy_once_and_never_revives_it_after_reset() {
        use anyagents_core::{NewAgent, Store, host_session};
        for selector in [
            "spiron",
            "sylphx",
            "sylphx-agent",
            "sylphx_agent",
            "sylphx/default",
        ] {
            for already_pinned in [false, true] {
                let dir = crate::test_tempdir();
                let store = Store::open(dir.path().join("state.db")).unwrap();
                let agent = store
                    .create_agent(NewAgent::new("legacy-pin", selector))
                    .unwrap();
                let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
                std::fs::create_dir_all(&config).unwrap();
                let root = config.parent().unwrap();
                let legacy = b"legacy-resume\n";
                std::fs::write(config.join("sylphx-session-id"), legacy).unwrap();
                std::fs::create_dir_all(root.join("memory")).unwrap();
                std::fs::write(root.join("memory/MEMORY.md"), "original pin").unwrap();
                let prior = already_pinned.then(|| {
                    host_session::ensure_active(root, selector).unwrap();
                    host_session::bind_vendor(root, selector, "legacy-resume").unwrap()
                });
                let request = || LaunchRequest {
                    agent: agent.clone(),
                    data_dir: dir.path().to_owned(),
                    config_dir: config.clone(),
                    work_dir: root.join("work"),
                    extra_env: vec![],
                    lock: None,
                };
                prepare_host_session(&request()).unwrap();
                let first = host_session::active(root).unwrap().unwrap();
                if let Some(prior) = prior {
                    assert_eq!(first, prior);
                }
                assert_eq!(first.harness, "spiron");
                assert_eq!(
                    std::fs::read(config.join("spiron-session-id")).unwrap(),
                    legacy
                );
                assert_eq!(
                    host_session::bound_vendor(root, selector, "legacy-resume")
                        .unwrap()
                        .unwrap(),
                    first
                );
                std::fs::write(root.join("memory/MEMORY.md"), "next pin").unwrap();
                prepare_host_session(&request()).unwrap();
                assert_eq!(host_session::active(root).unwrap().unwrap(), first);
                let next = host_session::reset_session(
                    root,
                    selector,
                    crate::create::harness_session_filename(selector),
                )
                .unwrap();
                assert_ne!(next.generation, first.generation);
                assert!(next.memory.section.contains("next pin"));
                prepare_host_session(&request()).unwrap();
                assert_eq!(host_session::active(root).unwrap().unwrap(), next);
                assert!(
                    !config.join("spiron-session-id").exists(),
                    "reset resurrected legacy pointer"
                );
                assert_eq!(
                    std::fs::read(config.join("sylphx-session-id")).unwrap(),
                    legacy
                );
                assert_eq!(
                    host_session::bound_vendor(root, selector, "legacy-resume")
                        .unwrap()
                        .unwrap(),
                    first,
                    "read-only alias lookup changed the active pin"
                );
                assert_eq!(host_session::active(root).unwrap().unwrap(), next);
            }
        }
    }

    #[test]
    fn ensure_agy_steering_settings_populates_permissions_and_trusted_workspaces() {
        let dir = crate::test_tempdir();
        let config_dir = dir.path().join("config");
        let work_dir = dir.path().join("work");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();

        ensure_agy_steering_settings(&config_dir, &work_dir, &dir.path().join("media")).unwrap();

        let settings_path = config_dir
            .join(".gemini")
            .join("antigravity-cli")
            .join("settings.json");
        let text = std::fs::read_to_string(&settings_path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["queuedMessages"], "send-immediately");
        let expected_mcp_grant = format!(
            "read_file({})",
            config_dir
                .join(".gemini")
                .join("antigravity-cli")
                .join("mcp")
                .display()
        );
        assert_eq!(
            json["permissions"]["allow"],
            serde_json::json!(["mcp(anyagents/*)", expected_mcp_grant])
        );
        assert_eq!(
            json["permissions"]["deny"],
            serde_json::json!([
                "invoke_subagent",
                "define_subagent",
                "manage_subagents",
                "send_message",
                "schedule",
                "search_web",
                "read_url_content",
                "browser_subagent",
                "open_browser_url",
                "run_command",
                "manage_task",
                "write_file",
                "edit_file",
                "grep_search",
                "find_by_name",
                "list_dir",
                "command(*)",
                "write_file(*)",
                "read_url(*)",
                "execute_url(*)"
            ])
        );
        assert_eq!(
            json["trustedWorkspaces"],
            serde_json::json!([work_dir.display().to_string()])
        );
        let hooks_path = config_dir.join(".gemini").join("config").join("hooks.json");
        let hooks_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&hooks_path).unwrap()).unwrap();
        assert_eq!(
            hooks_json["anyagents-native-tool-guard"]["enabled"],
            serde_json::json!(true)
        );
        let matcher = hooks_json["anyagents-native-tool-guard"]["PreToolUse"][0]["matcher"]
            .as_str()
            .unwrap();
        for blocked in [
            "invoke_subagent",
            "define_subagent",
            "manage_subagents",
            "search_web",
            "run_command",
            "read_file",
            "write_file",
            "edit_file",
        ] {
            assert!(matcher.contains(blocked), "matcher missing {blocked}");
        }

        // Calling a second time is idempotent and does not duplicate entries.
        ensure_agy_steering_settings(&config_dir, &work_dir, &dir.path().join("media")).unwrap();
        let text2 = std::fs::read_to_string(&settings_path).unwrap();
        let json2: serde_json::Value = serde_json::from_str(&text2).unwrap();
        assert_eq!(json2, json);
        let hooks_json2: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&hooks_path).unwrap()).unwrap();
        assert_eq!(hooks_json2, hooks_json);
    }

    #[test]
    fn agy_native_tool_guard_allows_only_mcp_schema_reads_and_prunes_stale_read_deny() {
        let dir = crate::test_tempdir();
        let config_dir = dir.path().join("config");
        let work_dir = dir.path().join("work");
        let mcp_dir = config_dir
            .join(".gemini")
            .join("antigravity-cli")
            .join("mcp")
            .join("anyagents");
        std::fs::create_dir_all(&mcp_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();

        // Pre-seed legacy static read_file(*) deny to verify automatic healing.
        let settings_path = config_dir
            .join(".gemini")
            .join("antigravity-cli")
            .join("settings.json");
        std::fs::write(
            &settings_path,
            serde_json::json!({
                "permissions": {
                    "allow": ["mcp(anyagents/*)"],
                    "deny": ["read_file", "view_file", "read_file(*)", "command(*)"]
                }
            })
            .to_string(),
        )
        .unwrap();

        ensure_agy_steering_settings(&config_dir, &work_dir, &dir.path().join("media")).unwrap();
        let healed: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&settings_path).unwrap()).unwrap();
        let deny_list = healed["permissions"]["deny"].as_array().unwrap();
        for stale in ["read_file", "view_file", "read_file(*)"] {
            assert!(
                !deny_list.iter().any(|v| v.as_str() == Some(stale)),
                "stale deny {stale} must be pruned"
            );
        }

        let schema_file = mcp_dir.join("code.json");
        std::fs::write(&schema_file, r#"{"name":"code"}"#).unwrap();
        let work_secret = work_dir.join("secret.txt");
        std::fs::write(&work_secret, "workspace secret").unwrap();
        let oauth_token = config_dir
            .join(".gemini")
            .join("antigravity-cli")
            .join("antigravity-oauth-token");
        std::fs::write(&oauth_token, "token").unwrap();
        #[cfg(unix)]
        let escaped_symlink = {
            let link = mcp_dir.join("escape.json");
            std::os::unix::fs::symlink(&work_secret, &link).unwrap();
            link
        };

        let hooks_path = config_dir.join(".gemini").join("config").join("hooks.json");
        let hooks_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&hooks_path).unwrap()).unwrap();
        let cmd = hooks_json["anyagents-native-tool-guard"]["PreToolUse"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();

        let run_hook = |tool: &str, path: Option<&Path>| -> serde_json::Value {
            use std::io::Write as _;
            let payload = match path {
                Some(p) => serde_json::json!({
                    "toolCall": { "name": tool, "args": { "Path": p.display().to_string() } }
                }),
                None => serde_json::json!({
                    "toolCall": { "name": tool, "args": { "CommandLine": "ls" } }
                }),
            };
            let mut child = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(cmd)
                .env("HOME", &config_dir)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(payload.to_string().as_bytes())
                .unwrap();
            let out = child.wait_with_output().unwrap();
            assert!(out.status.success());
            serde_json::from_slice(&out.stdout).unwrap()
        };

        let allow_out = run_hook("read_file", Some(&schema_file));
        assert_eq!(allow_out["decision"], "allow");

        for blocked_path in [
            &work_secret,
            &oauth_token,
            #[cfg(unix)]
            &escaped_symlink,
        ] {
            let deny_out = run_hook("read_file", Some(blocked_path));
            assert_eq!(deny_out["decision"], "deny");
            assert!(
                deny_out["reason"]
                    .as_str()
                    .unwrap()
                    .contains("call_mcp_tool")
            );
        }

        let cmd_deny = run_hook("run_command", None);
        assert_eq!(cmd_deny["decision"], "deny");
        assert!(
            cmd_deny["reason"]
                .as_str()
                .unwrap()
                .contains("call_mcp_tool")
        );
    }
    /// The guard's answer for one native tool call, as AGY runs the hook:
    /// the call on stdin, `HOME` the agent's config directory.
    fn agy_guard(cmd: &str, home: &Path, tool: &str, path: &Path) -> serde_json::Value {
        use std::io::Write as _;
        let payload = serde_json::json!({
            "toolCall": { "name": tool, "args": { "Path": path.display().to_string() } }
        });
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(cmd)
            .env("HOME", home)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(payload.to_string().as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        serde_json::from_slice(&out.stdout).unwrap()
    }

    /// AGY gives its model an MCP image only up to 10,240 bytes and offloads
    /// any larger one to the step's directory; its native `read_file` shows
    /// the model such a file. The guard lets `read_file` (or `view_file`)
    /// open media, told by its bytes, in the workspace, in `<data>/media`,
    /// and under AGY's own offloaded steps, and nothing else.
    #[test]
    fn agy_native_read_shows_the_media_an_agent_was_given_and_nothing_else() {
        let dir = crate::test_tempdir();
        let config_dir = dir.path().join("config");
        let work_dir = dir.path().join("work");
        let media_dir = dir.path().join("media");
        let elsewhere = dir.path().join("elsewhere");
        let step =
            config_dir.join(".gemini/antigravity-cli/brain/324a320d/.system_generated/steps/3921");
        for path in [&config_dir, &work_dir, &media_dir, &elsewhere, &step] {
            std::fs::create_dir_all(path).unwrap();
        }
        ensure_agy_steering_settings(&config_dir, &work_dir, &media_dir).unwrap();
        let hooks: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(config_dir.join(".gemini/config/hooks.json")).unwrap(),
        )
        .unwrap();
        let cmd = hooks["anyagents-native-tool-guard"]["PreToolUse"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap()
            .to_string();
        let jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00";
        let write = |path: &Path, bytes: &[u8]| {
            std::fs::write(path, bytes).unwrap();
            path.to_path_buf()
        };
        let allowed = [
            write(&work_dir.join("chart.png"), b"\x89PNG\r\n\x1a\nrest"),
            write(&media_dir.join("complete_photo_a.jpg"), jpeg),
            write(&media_dir.join("complete_document_b.pdf"), b"%PDF-1.7\n"),
            write(&media_dir.join("complete_voice_c.ogg"), b"OggS\x00\x02rest"),
            write(&step.join("media_0.jpg"), jpeg),
        ];
        for path in &allowed {
            for tool in ["read_file", "view_file"] {
                assert_eq!(
                    agy_guard(&cmd, &config_dir, tool, path)["decision"],
                    "allow",
                    "{tool} {}",
                    path.display()
                );
            }
        }
        let outside = write(&elsewhere.join("photo.jpg"), jpeg);
        let mut denied = vec![
            outside.clone(),
            write(&work_dir.join("notes.txt"), b"workspace text"),
            write(&media_dir.join("fake.jpg"), b"not an image"),
            write(
                &config_dir.join(".gemini/antigravity-cli/brain/324a320d/photo.jpg"),
                jpeg,
            ),
            media_dir.join("missing.jpg"),
        ];
        #[cfg(unix)]
        {
            let link = work_dir.join("link.jpg");
            std::os::unix::fs::symlink(&outside, &link).unwrap();
            denied.push(link);
        }
        for path in &denied {
            let answer = agy_guard(&cmd, &config_dir, "read_file", path);
            assert_eq!(answer["decision"], "deny", "{}", path.display());
            assert!(
                answer["reason"]
                    .as_str()
                    .unwrap()
                    .contains("read_file stays for an image"),
                "{answer}"
            );
        }
        let other = agy_guard(&cmd, &config_dir, "run_command", &allowed[0]);
        assert_eq!(other["decision"], "deny");
    }

    #[test]
    fn poisoned_agy_session_retires_only_the_pointer_and_stale_error() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path();
        let db = config.join(".gemini/antigravity-cli/conversations/selected.db");
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        std::fs::write(&db, b"native history").unwrap();
        std::fs::write(config.join("state.db"), b"host history").unwrap();
        std::fs::write(config.join("agy-session-id"), "selected").unwrap();
        let now = anyagents_core::now_ms();
        std::fs::write(
            config.join(".anyagents-last-error.json"),
            serde_json::json!({
                "error":"INVALID_ARGUMENT (code 400): Request contains an invalid argument",
                "timestamp_ms": now - 1,
            })
            .to_string(),
        )
        .unwrap();
        assert!(retire_poisoned_agy_session(config, now).unwrap());
        assert!(!config.join("agy-session-id").exists());
        assert!(!config.join(".anyagents-last-error.json").exists());
        assert_eq!(
            std::fs::read_to_string(config.join(".anyagents-quota-cleared-ms")).unwrap(),
            now.to_string()
        );
        assert_eq!(std::fs::read(&db).unwrap(), b"native history");
        assert_eq!(
            std::fs::read(config.join("state.db")).unwrap(),
            b"host history"
        );
        assert!(!retire_poisoned_agy_session(config, now + 1).unwrap());
    }

    #[test]
    fn poison_recovery_watermark_does_not_retire_a_fresh_session_from_stale_logs() {
        let dir = tempfile::tempdir().unwrap();
        let logs = dir.path().join(".gemini/antigravity-cli/log");
        std::fs::create_dir_all(&logs).unwrap();
        std::fs::write(
            logs.join("cli-fixture.log"),
            "run ended with error and no response: INVALID_ARGUMENT (code 400): invalid argument\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("agy-session-id"), "poisoned").unwrap();
        let now = anyagents_core::now_ms() + 1;
        assert!(retire_poisoned_agy_session(dir.path(), now).unwrap());
        std::fs::write(dir.path().join("agy-session-id"), "fresh").unwrap();
        assert!(!retire_poisoned_agy_session(dir.path(), now + 1).unwrap());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("agy-session-id")).unwrap(),
            "fresh"
        );
    }

    #[test]
    fn ordinary_agy_errors_and_quota_do_not_retire_sessions() {
        for error in [
            "transient connection failure",
            "RESOURCE_EXHAUSTED: quota reached",
            "INVALID_ARGUMENT unrelated validation",
        ] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("agy-session-id"), "selected").unwrap();
            let last_error =
                serde_json::json!({ "error": error, "timestamp_ms": anyagents_core::now_ms() })
                    .to_string();
            std::fs::write(dir.path().join(".anyagents-last-error.json"), &last_error).unwrap();
            assert!(!retire_poisoned_agy_session(dir.path(), anyagents_core::now_ms()).unwrap());
            assert_eq!(
                std::fs::read_to_string(dir.path().join("agy-session-id")).unwrap(),
                "selected"
            );
            assert_eq!(
                std::fs::read_to_string(dir.path().join(".anyagents-last-error.json")).unwrap(),
                last_error
            );
            assert!(!dir.path().join(".anyagents-quota-cleared-ms").exists());
        }
    }

    #[test]
    fn continuation_pointer_absence_is_distinct_from_invalid_or_unreadable_selection() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(session_from_file(dir.path(), "session-id").unwrap(), None);
        let path = dir.path().join("session-id");
        std::fs::write(&path, "selected\n").unwrap();
        assert_eq!(
            session_from_file(dir.path(), "session-id").unwrap(),
            Some("selected".into())
        );
        for bytes in [b" \n".as_slice(), &[0xff]] {
            std::fs::write(&path, bytes).unwrap();
            assert!(session_from_file(dir.path(), "session-id").is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        retire_session_pointer(&path).unwrap();
        retire_session_pointer(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(retire_session_pointer(&path).is_err());
        assert!(session_from_file(dir.path(), "session-id").is_err());
    }

    #[test]
    fn memory_source_and_native_memory_policy_are_explicit() {
        let dir = tempfile::tempdir().unwrap();
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        let mut agent = store
            .create_agent(anyagents_core::NewAgent::new("sample", "claude-code"))
            .unwrap();
        assert_eq!(memory_source(&agent), Some(Path::new("memory/MEMORY.md")));
        agent
            .launch_env
            .push(("ANYAGENTS_MEMORY_SOURCE".into(), "".into()));
        assert_eq!(memory_source(&agent), None);
        agent
            .launch_env
            .push(("ANYAGENTS_MEMORY_SOURCE".into(), "notes/index.md".into()));
        assert_eq!(memory_source(&agent), Some(Path::new("notes/index.md")));
        let mut env = vec![("CLAUDE_CODE_DISABLE_AUTO_MEMORY".into(), "0".into())];
        enforce_host_memory_env("claude-code", &mut env);
        assert_eq!(
            env,
            vec![("CLAUDE_CODE_DISABLE_AUTO_MEMORY".into(), "0".into())]
        );
        env.push(("ANYAGENTS_NATIVE_MEMORY_DISABLED".into(), "1".into()));
        enforce_host_memory_env("claude-code", &mut env);
        assert!(env.contains(&("CLAUDE_CODE_DISABLE_AUTO_MEMORY".into(), "1".into())));
    }

    #[test]
    fn steering_settings_reject_malformed_config_and_persist_required_revision() {
        let dir = tempfile::tempdir().unwrap();
        let settings = dir.path().join(".gemini/antigravity-cli/settings.json");
        ensure_agy_steering_settings(dir.path(), dir.path(), dir.path()).unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&settings).unwrap()).unwrap();
        assert_eq!(value["queuedMessages"], "send-immediately");
        std::fs::write(&settings, "{broken").unwrap();
        assert!(ensure_agy_steering_settings(dir.path(), dir.path(), dir.path()).is_err());
        assert_eq!(std::fs::read(settings).unwrap(), b"{broken");
        let blocked = tempfile::tempdir().unwrap();
        std::fs::write(blocked.path().join(".gemini"), b"not a directory").unwrap();
        assert!(
            ensure_agy_steering_settings(blocked.path(), blocked.path(), blocked.path()).is_err()
        );
    }
}

#[cfg(test)]
#[path = "vendor/spiron_tests.rs"]
mod spiron_tests;
