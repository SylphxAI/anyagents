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
use anyagents_harness_stream::{Launch, LineProtocol, StreamHarness, StreamSpec};
use anyagents_supervisor::LaunchRequest;

use crate::dsh::DshHarness;
use crate::grok::GrokHarness;

pub enum VendorHarness {
    Claude(Box<ClaudeHarness>, SessionPointer),
    Codex(Box<CodexHarness>, SessionPointer),
    OpenCode(Box<OpenCodeHarness>, SessionPointer),
    Agy(Box<AgyHarness>, SessionPointer),
    Muse(Box<MuseHarness>, SessionPointer),
    Omp(Box<OmpHarness>, SessionPointer),
    Dsh(Box<DshHarness>, SessionPointer),
    Grok(Box<GrokHarness>, SessionPointer),
    /// The external Spiron CLI.  The pointer is the canonical session file;
    /// legacy `sylphx-session-id` is read once when this adapter starts.
    Spiron(Box<StreamHarness>, SessionPointer, String),
    Contract(Box<ContractHarness>),
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
    fn sync_session(&self) -> Result<(), String> {
        if let Self::Codex(harness, path) = self
            && harness.thread_invalidated()
        {
            let mut recorded = path.recorded();
            if *recorded != Recorded::Retired {
                retire_session_pointer(path)?;
                *recorded = Recorded::Retired;
            }
            return Ok(());
        }
        let (session, path) = match self {
            Self::Claude(harness, path) => (harness.session_id(), Some(path)),
            Self::Codex(harness, path) => (harness.session_id(), Some(path)),
            Self::Agy(harness, path) => (harness.session_id(), Some(path)),
            Self::Muse(harness, path) => (harness.session_id(), Some(path)),
            Self::Omp(harness, path) => (harness.session_id(), Some(path)),
            Self::Dsh(harness, path) => (harness.session_id(), Some(path)),
            Self::Grok(harness, path) => (harness.session_id(), Some(path)),
            Self::Spiron(harness, path, _) => (harness.session_id(), Some(path)),
            Self::OpenCode(harness, path) => (harness.session_id(), Some(path)),
            Self::Contract(_) => (None, None),
        };
        if let (Some(id), Some(path)) = (session, path) {
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
                if let Some(root) = path.parent().and_then(Path::parent) {
                    let harness = match self {
                        Self::Claude(_, path) => match path.file_name().and_then(|n| n.to_str()) {
                            Some("spiron-cloud-session-id") => "spiron-cloud",
                            Some("claude-cloud-session-id") => "claude-cloud",
                            Some("openai-cloud-session-id") => "openai-cloud",
                            Some("cursor-cloud-session-id") => "cursor-cloud",
                            Some("codex-session-id") => "codex",
                            Some("opencode-session-id") => "opencode",
                            Some("agy-session-id") => "agy",
                            Some("muse-session-id") => "muse",
                            Some("omp-session-id") => "omp",
                            Some("dsh-session-id") => "dsh",
                            Some("grok-session-id") => "grok",
                            Some("spiron-session-id") => "spiron",
                            _ => "claude-code",
                        },
                        Self::Codex(..) => "codex",
                        Self::OpenCode(..) => "opencode",
                        Self::Agy(..) => "agy",
                        Self::Muse(..) => "muse",
                        Self::Omp(..) => "omp",
                        Self::Dsh(..) => "dsh",
                        Self::Grok(..) => "grok",
                        Self::Spiron(_, _, selector) => selector.as_str(),
                        _ => unreachable!(),
                    };
                    if let Err(error) =
                        anyagents_core::host_session::bind_vendor(root, harness, trimmed)
                    {
                        return Err(format!("host session alias refused: {error}"));
                    }
                }
                if existing.as_deref().map(str::trim) != Some(trimmed) {
                    crate::create::write_required_file(
                        path,
                        format!("{trimmed}\n").as_bytes(),
                        0o660,
                    )
                    .map_err(|err| err.to_string())?;
                }
                *recorded = Recorded::Session(trimmed.to_string());
            }
        }
        Ok(())
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
                steer: anyagents_harness::SteerSupport::Unproven,
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

    fn take_reclaimed_steers(&mut self) -> Vec<String> {
        self.inner.take_reclaimed_steers()
    }

    fn has_unconfirmed_hold(&self) -> bool {
        self.inner.has_unconfirmed_hold()
    }

    fn has_unconfirmed_idle_hold(&self) -> bool {
        self.inner.has_unconfirmed_idle_hold()
    }

    fn peek_unconfirmed_request_ids(&self) -> Vec<String> {
        self.inner.peek_unconfirmed_request_ids()
    }

    fn reconcile_unconfirmed_for_recovery(
        &mut self,
    ) -> Result<Option<anyagents_harness::ReconciledUnconfirmed>, HarnessError> {
        self.inner.reconcile_unconfirmed_for_recovery()
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
    vendor_launch_authorized(request, None)
}

/// Reuse the daemon's admitted in-memory authority; reopening SQLite alone does
/// not confer capability verification or minting authority.
pub fn vendor_launch_with_store(
    request: LaunchRequest,
    store: &anyagents_core::Store,
) -> Result<VendorHarness, String> {
    vendor_launch_authorized(request, Some(store))
}

fn vendor_launch_authorized(
    request: LaunchRequest,
    store: Option<&anyagents_core::Store>,
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
    let res = vendor_launch_inner(request, store);
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
    request: LaunchRequest,
    authority: Option<&anyagents_core::Store>,
) -> Result<VendorHarness, String> {
    // Strip any untrusted launch or authority variables from caller/stored env.
    let mut env: Vec<(String, String)> = request
        .extra_env
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
    let mut store_reopen_error: Option<anyagents_core::Error> = None;
    let store_opt = if let Some(store) = authority {
        Some(store)
    } else {
        reopened = match anyagents_core::Store::open(request.data_dir.join("state.db")) {
            Ok(store) => Some(store),
            Err(error) => {
                store_reopen_error = Some(error);
                None
            }
        };
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
    // A seat launch whose state.db could not be reopened would silently
    // lose the peer projection and every store-backed decision. Fail
    // closed, naming the reopen error, rather than launching a seat
    // without it. Single-UID launches keep the previous fallback.
    if seat_spec.is_some()
        && let Some(error) = store_reopen_error
    {
        return Err(format!(
            "no seat launch without peers: cannot open state.db {}: {error}",
            request.data_dir.join("state.db").display()
        ));
    }
    // Reject missing store authority before preparing shared seat directories.
    crate::worktree_access::ensure_launchable(&request.data_dir, &request.agent.id)
        .map_err(|error| error.to_string())?;
    if let Some((_, uid, gid, _)) = resolved_seat.as_ref() {
        crate::sandbox::prepare_config_with_seat_uid(&request.config_dir, *uid, *gid)
            .map_err(|error| error.to_string())?;
    }
    if request.agent.harness == "agy" {
        ensure_agy_steering_settings(&request.config_dir, &request.work_dir)
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
    if let Some(config) = adapter_config(&request.config_dir, &request.agent.harness) {
        let program =
            std::env::var("ANYAGENTS_HARNESS_BIN").unwrap_or_else(|_| "anyagents-harness".into());
        let harness = ContractHarness::start(
            &program,
            &config.display().to_string(),
            &pairs(&env),
            Some(&request.work_dir),
        )
        .map_err(|err| err.to_string())?;
        return Ok(VendorHarness::Contract(Box::new(harness)));
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
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
        let target_program = crate::install::program(&root, canonical);
        let exe = std::env::current_exe()
            .or_else(|_| crate::data::current_exe_clean())
            .unwrap_or_else(|_| PathBuf::from("anyagents"));
        let extra_args = vec![
            "sandbox-exec".to_string(),
            "--config".to_string(),
            request.config_dir.display().to_string(),
            "--harness".to_string(),
            canonical.to_string(),
            "--program".to_string(),
            target_program.display().to_string(),
        ];
        let harness = start_claude(
            &exe.display().to_string(),
            &extra_args,
            session.as_deref(),
            &env,
            &request.work_dir,
        )?;
        let vh = VendorHarness::Claude(
            Box::new(harness),
            request.config_dir.join(&session_file).into(),
        );
        vh.sync_session()?;
        return Ok(vh);
    }
    if request.agent.harness == "agy" {
        retire_poisoned_agy_session(&request.config_dir, anyagents_core::now_ms())?;
        crate::create::clear_stale_agy_quota_error_steps(&request.config_dir);
        let program = std::env::var("ANYAGENTS_AGY_BIN").unwrap_or_else(|_| "agy".to_string());
        let session = session_from_file(&request.config_dir, "agy-session-id")?;
        let harness = if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
            AgyHarness::start(&program, session.as_deref(), &pairs(&env))
                .map_err(|err| err.to_string())?
        } else {
            let mut spec = anyagents_harness_stream::StreamSpec::for_program("agy");
            spec.args = vec![
                "--input-format".to_string(),
                "stream-json".to_string(),
                "--output-format".to_string(),
                "stream-json".to_string(),
                "--dangerously-skip-permissions".to_string(),
            ];
            if let Some(model) = request
                .agent
                .model
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
            {
                spec.args.push("--model".to_string());
                spec.args.push(model.to_string());
            }
            spec.resume_flag = "--conversation";
            AgyHarness::start_with_spec(
                spec,
                &program,
                session.as_deref(),
                &pairs(&env),
                Some(&request.work_dir),
            )
            .map_err(|err| err.to_string())?
        };
        let vh = VendorHarness::Agy(
            Box::new(harness),
            request.config_dir.join("agy-session-id").into(),
        );
        vh.sync_session()?;
        Ok(vh)
    } else if request.agent.harness == "muse" {
        env.push((
            "XDG_CONFIG_HOME".to_string(),
            request.config_dir.display().to_string(),
        ));
        env.push(("MUSE_NO_AUTO_UPDATE".to_string(), "1".to_string()));
        let program = std::env::var("ANYAGENTS_MUSE_BIN").unwrap_or_else(|_| "muse".to_string());
        let session = session_from_file(&request.config_dir, "muse-session-id")?;
        let extra_args = if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
            Vec::new()
        } else {
            let model = request
                .agent
                .model
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .unwrap_or("muse-wonder-1.0")
                .to_string();
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
        let harness = MuseHarness::start_in_with_args(
            &program,
            &extra_args,
            session.as_deref(),
            &pairs(&env),
            Some(&request.work_dir),
        )
        .map_err(|err| {
            read_muse_exit_error(&request.config_dir).unwrap_or_else(|| err.to_string())
        })?;
        if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() && !harness.health().process_alive {
            return Err(read_muse_exit_error(&request.config_dir)
                .unwrap_or_else(|| "muse serve exited during startup handshake".to_string()));
        }
        let vh = VendorHarness::Muse(
            Box::new(harness),
            request.config_dir.join("muse-session-id").into(),
        );
        vh.sync_session()?;
        Ok(vh)
    } else if request.agent.harness == "omp" {
        let omp_agent_dir = request.config_dir.join(".omp").join("agent");
        let _ = std::fs::create_dir_all(&omp_agent_dir);
        env.push((
            "PI_CODING_AGENT_DIR".to_string(),
            omp_agent_dir.display().to_string(),
        ));
        let program = crate::install::program(&root, &request.agent.harness);
        let session = session_from_file(&request.config_dir, "omp-session-id")?;
        let harness = if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
            OmpHarness::start(
                program.display().to_string(),
                session.as_deref(),
                &pairs(&env),
            )
            .map_err(|err| err.to_string())?
        } else {
            let mut spec = anyagents_harness_stream::StreamSpec::for_program("omp");
            if let Some(model) = request
                .agent
                .model
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
            {
                spec.args.push("--model".to_string());
                spec.args.push(model.to_string());
            }
            if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
                let policy_path = crate::grok::ensure_omp_policy(&omp_agent_dir)
                    .map_err(|err| err.to_string())?;
                spec.args.push("--config".to_string());
                spec.args.push(policy_path.display().to_string());
                spec.args.push("--no-extensions".to_string());
            }
            let sys_prompt = request.config_dir.join("SYSTEM_PROMPT.md");
            if sys_prompt.is_file() {
                spec.args.push("--append-system-prompt".to_string());
                spec.args.push(sys_prompt.display().to_string());
            }
            OmpHarness::start_with_spec(
                spec,
                program.display().to_string(),
                session.as_deref(),
                &pairs(&env),
                Some(&request.work_dir),
            )
            .map_err(|err| err.to_string())?
        };
        let vh = VendorHarness::Omp(
            Box::new(harness),
            request.config_dir.join("omp-session-id").into(),
        );
        vh.sync_session()?;
        Ok(vh)
    } else if request.agent.harness == "dsh" {
        let program = crate::install::program(&root, &request.agent.harness);
        let session = session_from_file(&request.config_dir, "dsh-session-id")?;
        let patch_path = request.config_dir.join("dsh-mcp-patch.yml");
        let patch = patch_path.is_file().then_some(patch_path.as_path());
        let harness = DshHarness::start(
            &program,
            patch,
            session.as_deref(),
            &pairs(&env),
            &request.work_dir,
        )
        .map_err(|err| err.to_string())?;
        let vh = VendorHarness::Dsh(
            Box::new(harness),
            request.config_dir.join("dsh-session-id").into(),
        );
        vh.sync_session()?;
        Ok(vh)
    } else if request.agent.harness == "grok" {
        let program = crate::install::program(&root, &request.agent.h