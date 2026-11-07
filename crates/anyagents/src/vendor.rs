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
use anyagents_harness_stream::{Launch, StreamHarness};
use anyagents_supervisor::LaunchRequest;

use crate::dsh::DshHarness;
use crate::grok::GrokHarness;
use crate::sylphx::SylphxHarness;

pub enum VendorHarness {
    Claude(Box<ClaudeHarness>, PathBuf),
    Codex(Box<CodexHarness>, PathBuf),
    OpenCode(Box<OpenCodeHarness>, PathBuf),
    Agy(Box<AgyHarness>, PathBuf),
    Muse(Box<MuseHarness>, PathBuf),
    Omp(Box<OmpHarness>, PathBuf),
    Dsh(Box<DshHarness>, PathBuf),
    Grok(Box<GrokHarness>, PathBuf),
    Sylphx(Box<SylphxHarness>, PathBuf),
    Contract(Box<ContractHarness>),
}

impl VendorHarness {
    fn sync_session(&self) {
        if let Self::Codex(harness, path) = self
            && harness.thread_invalidated()
        {
            let _ = std::fs::remove_file(path);
            return;
        }
        let (session, path) = match self {
            Self::Claude(harness, path) => (harness.session_id(), Some(path)),
            Self::Codex(harness, path) => (harness.session_id(), Some(path)),
            Self::Agy(harness, path) => (harness.session_id(), Some(path)),
            Self::Muse(harness, path) => (harness.session_id(), Some(path)),
            Self::Omp(harness, path) => (harness.session_id(), Some(path)),
            Self::Dsh(harness, path) => (harness.session_id(), Some(path)),
            Self::Grok(harness, path) => (harness.session_id(), Some(path)),
            Self::Sylphx(harness, path) => (harness.session_id(), Some(path)),
            Self::OpenCode(harness, path) => (harness.session_id(), Some(path)),
            Self::Contract(_) => (None, None),
        };
        if let (Some(id), Some(path)) = (session, path) {
            let trimmed = id.trim();
            if !trimmed.is_empty() {
                let existing = std::fs::read_to_string(path).ok();
                if let Some(root) = path.parent().and_then(Path::parent) {
                    let harness = match self {
                        Self::Claude(..) => "claude-code",
                        Self::Codex(..) => "codex",
                        Self::OpenCode(..) => "opencode",
                        Self::Agy(..) => "agy",
                        Self::Muse(..) => "muse",
                        Self::Omp(..) => "omp",
                        Self::Dsh(..) => "dsh",
                        Self::Grok(..) => "grok",
                        Self::Sylphx(..) => "sylphx",
                        _ => unreachable!(),
                    };
                    if let Err(error) =
                        anyagents_core::host_session::bind_vendor(root, harness, trimmed)
                    {
                        eprintln!("host session alias refused: {error}");
                        return;
                    }
                }
                if existing.as_deref().map(str::trim) != Some(trimmed) {
                    let _ = std::fs::write(path, format!("{trimmed}\n"));
                }
            }
        }
    }
}

/// A harness driven by a program named in the agent's config, which must obey
/// the adapter contract in `docs/architecture.md`: one JSON line on stdin per
/// message, and a `result` line on stdout when its turn ends. An agent's own
/// harness is used unless its config names one.
fn adapter_config(config_dir: &Path, harness: &str) -> Option<PathBuf> {
    let text = std::fs::read_to_string(config_dir.join("adapter.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    if value.get("mode").and_then(|v| v.as_str()) == Some("sylphx-native") {
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

pub fn detect_codex_session_turn_error(config_dir: &Path, min_ts_ms: i64) -> Option<String> {
    if let Some((Some(err), None)) = read_persisted_last_error(config_dir)
        && !err.trim().is_empty()
    {
        return Some(err);
    }
    let sessions_dir = config_dir.join("sessions");
    if !sessions_dir.is_dir() {
        return None;
    }
    let mut latest_file: Option<(std::time::SystemTime, PathBuf)> = None;
    if let Ok(years) = std::fs::read_dir(&sessions_dir) {
        for y in years.flatten() {
            let Ok(months) = std::fs::read_dir(y.path()) else {
                continue;
            };
            for m in months.flatten() {
                let Ok(days) = std::fs::read_dir(m.path()) else {
                    continue;
                };
                for d in days.flatten() {
                    let Ok(files) = std::fs::read_dir(d.path()) else {
                        continue;
                    };
                    for f in files.flatten() {
                        let path = f.path();
                        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                            continue;
                        };
                        if !name.starts_with("rollout-") || !name.ends_with(".jsonl") {
                            continue;
                        }
                        let Ok(meta) = f.metadata() else {
                            continue;
                        };
                        let Ok(mtime) = meta.modified() else {
                            continue;
                        };
                        if latest_file
                            .as_ref()
                            .is_none_or(|(prev_m, _)| mtime > *prev_m)
                        {
                            latest_file = Some((mtime, path));
                        }
                    }
                }
            }
        }
    }
    let (mtime, path) = latest_file?;
    let mtime_ms = mtime
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    if mtime_ms <= min_ts_ms {
        return None;
    }
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(&path).ok()?;
    let len = file.metadata().ok()?.len();
    let tail_offset = len.saturating_sub(65_536);
    if tail_offset > 0 {
        let _ = file.seek(SeekFrom::Start(tail_offset));
    }
    let mut buf = String::new();
    file.read_to_string(&mut buf).ok()?;
    let lines_iter: Box<dyn Iterator<Item = &str>> = if tail_offset > 0 {
        Box::new(buf.lines().skip(1))
    } else {
        Box::new(buf.lines())
    };
    let mut consec_err = 0u32;
    let mut last_err: Option<String> = None;
    for line in lines_iter {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        let t = val.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let payload = val.get("payload");
        let pt = payload
            .and_then(|p| p.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let role = payload
            .and_then(|p| p.get("role"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if t == "token_usage_record"
            || (t == "response_item"
                && (role == "assistant"
                    || matches!(pt, "function_call" | "custom_tool_call" | "reasoning")))
        {
            consec_err = 0;
            last_err = None;
        } else if t == "event_msg" && pt == "task_complete" {
            let err_opt = payload
                .and_then(|p| p.get("error"))
                .and_then(|e| {
                    e.get("message")
                        .and_then(|m| m.as_str())
                        .or_else(|| e.as_str())
                })
                .map(str::trim)
                .filter(|s| !s.is_empty());
            if let Some(msg) = err_opt {
                consec_err = consec_err.saturating_add(1);
                last_err = Some(msg.to_string());
            } else {
                consec_err = 0;
                last_err = None;
            }
        }
    }
    let err = last_err?;
    let lower = err.to_ascii_lowercase();
    let is_auth_401 = lower.contains("(401)")
        || lower.contains(" 401 ")
        || lower.contains("status 401")
        || lower.contains("http 401")
        || lower.contains("unauthorized")
        || lower.contains("invalid_api_key")
        || lower.contains("authentication failed");
    if is_auth_401 || consec_err >= 3 {
        Some(err)
    } else {
        None
    }
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
            let _ = std::fs::write(&path, payload.to_string());
        }
        None => {
            let _ = std::fs::remove_file(&path);
        }
    }
}

pub fn vendor_launch(request: LaunchRequest) -> Result<VendorHarness, String> {
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
    let res = vendor_launch_inner(request);
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

fn prepare_host_session(request: &LaunchRequest) -> Result<(), String> {
    let root = request.config_dir.parent().ok_or("agent root missing")?;
    anyagents_core::host_session::recover_pending(root).map_err(|e| e.to_string())?;
    let previous = anyagents_core::host_session::active(root).map_err(|e| e.to_string())?;
    let switched = previous
        .as_ref()
        .is_some_and(|s| s.harness != request.agent.harness);
    if switched {
        // Deliberate harness cutover starts a native session too. Keep its raw
        // history in place; only retire the pointer, not its transcript/state.
        if let Some(file) = crate::create::harness_session_filename(&request.agent.harness) {
            match std::fs::remove_file(request.config_dir.join(file)) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.to_string()),
            }
        }
        anyagents_core::host_session::new_session(root, &request.agent.harness)
    } else {
        anyagents_core::host_session::ensure_active(root, &request.agent.harness)
    }
    .map_err(|e| format!("host session pin failed: {e}"))?;
    if let Some(file) = crate::create::harness_session_filename(&request.agent.harness)
        && let Some(id) = session_from_file(&request.config_dir, file)
    {
        anyagents_core::host_session::bind_vendor(root, &request.agent.harness, &id)
            .map_err(|e| format!("host session alias failed: {e}"))?;
    }
    // Also render in contract-test mode, exercising the shipping launch boundary.
    crate::create::write_agent_instructions(&request.config_dir, &request.agent, None);
    Ok(())
}

/// Claude's native loader/writer would otherwise reopen a second project memory
/// source and bypass the host's immutable session pin. Official memory.md
/// (read 2026-11-07T03:04Z) documents this supported per-process opt-out.
fn enforce_host_memory_env(harness: &str, env: &mut Vec<(String, String)>) {
    if harness == "claude-code" {
        env.retain(|(key, _)| key != "CLAUDE_CODE_DISABLE_AUTO_MEMORY");
        env.push(("CLAUDE_CODE_DISABLE_AUTO_MEMORY".into(), "1".into()));
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

fn vendor_launch_inner(request: LaunchRequest) -> Result<VendorHarness, String> {
    crate::worktree_access::ensure_launchable(&request.data_dir, &request.agent.id)
        .map_err(|error| error.to_string())?;
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
    let store_opt = anyagents_core::Store::open(request.data_dir.join("state.db")).ok();
    let seat_hint = store_opt
        .as_ref()
        .and_then(|s| crate::create::resolve_root_seat_hint(s, &request.agent))
        .or_else(|| {
            env.iter()
                .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
                .map(|(_, v)| v.clone())
        });
    let resolved_seat = store_opt
        .as_ref()
        .and_then(|s| crate::create::resolved_agent_seat_info(s, &request.agent))
        .or_else(|| crate::sandbox::existing_config_seat(&request.config_dir));
    let seat_spec = resolved_seat
        .as_ref()
        .and_then(|(user, _, _, spec_opt)| spec_opt.as_ref().map(|s| (user.clone(), s.clone())))
        .or_else(|| {
            crate::sandbox::seat_launch_spec_with_hint(&request.agent.name, seat_hint.as_deref())
        });
    if request.agent.harness == "agy" {
        ensure_agy_steering_settings(&request.config_dir, &request.work_dir);
    }
    if std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some() {
        return Err(format!(
            "harness config verification failed for {} ({}): config write suppressed",
            request.agent.name, request.agent.harness
        ));
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() {
        let _ = crate::install::record(&root, &request.agent.harness);
        if let Some(ref store) = store_opt {
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
            );
            crate::create::verify_harness_runtime_config(
                &request.config_dir,
                &request.agent.harness,
                request.agent.model.as_deref(),
            )
            .map_err(|e| e.to_string())?;
        }
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
        if let Some((_, uid, gid, _)) = resolved_seat.or_else(|| {
            crate::sandbox::seat_for_agent_with_hint(&request.agent.name, seat_hint.as_deref())
                .map(|(u, uid, gid, _)| (u, uid, gid, None))
        }) {
            crate::sandbox::share_config_with_seat_uid(&request.config_dir, uid, gid);
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
        let host_home = crate::sandbox::host_home_dir();
        if let Some(ref h) = host_home
            && h.join(".rustup").is_dir()
        {
            env.push((
                "RUSTUP_HOME".to_string(),
                h.join(".rustup").display().to_string(),
            ));
        } else if Path::new("/scratch/rustup-home").is_dir() {
            env.push((
                "RUSTUP_HOME".to_string(),
                "/scratch/rustup-home".to_string(),
            ));
        }
        if let Some(ref h) = host_home
            && h.join(".cargo").is_dir()
        {
            env.push((
                "CARGO_HOME".to_string(),
                h.join(".cargo").display().to_string(),
            ));
        } else if Path::new("/scratch/cargo-registry").is_dir() {
            env.push((
                "CARGO_HOME".to_string(),
                "/scratch/cargo-registry".to_string(),
            ));
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
    if request.agent.harness == "agy" {
        crate::create::clear_stale_agy_quota_error_steps(&request.config_dir);
        let program = std::env::var("ANYAGENTS_AGY_BIN").unwrap_or_else(|_| "agy".to_string());
        let session = session_from_file(&request.config_dir, "agy-session-id");
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
        let vh = VendorHarness::Agy(Box::new(harness), request.config_dir.join("agy-session-id"));
        vh.sync_session();
        Ok(vh)
    } else if request.agent.harness == "muse" {
        env.push((
            "XDG_CONFIG_HOME".to_string(),
            request.config_dir.display().to_string(),
        ));
        env.push(("MUSE_NO_AUTO_UPDATE".to_string(), "1".to_string()));
        let program = std::env::var("ANYAGENTS_MUSE_BIN").unwrap_or_else(|_| "muse".to_string());
        let session = session_from_file(&request.config_dir, "muse-session-id");
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
            vec![
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
            ]
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
            request.config_dir.join("muse-session-id"),
        );
        vh.sync_session();
        Ok(vh)
    } else if request.agent.harness == "omp" {
        let omp_agent_dir = request.config_dir.join(".omp").join("agent");
        let _ = std::fs::create_dir_all(&omp_agent_dir);
        env.push((
            "PI_CODING_AGENT_DIR".to_string(),
            omp_agent_dir.display().to_string(),
        ));
        let program = crate::install::program(&root, &request.agent.harness);
        let session = session_from_file(&request.config_dir, "omp-session-id");
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
        let vh = VendorHarness::Omp(Box::new(harness), request.config_dir.join("omp-session-id"));
        vh.sync_session();
        Ok(vh)
    } else if request.agent.harness == "dsh" {
        let program = crate::install::program(&root, &request.agent.harness);
        let session = session_from_file(&request.config_dir, "dsh-session-id");
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
        let vh = VendorHarness::Dsh(Box::new(harness), request.config_dir.join("dsh-session-id"));
        vh.sync_session();
        Ok(vh)
    } else if request.agent.harness == "grok" {
        let program = crate::install::program(&root, &request.agent.harness);
        let session = session_from_file(&request.config_dir, "grok-session-id");
        let exe = crate::data::current_exe_clean().unwrap_or_else(|_| PathBuf::from("anyagents"));
        let mcp_args = vec![
            "--data".to_string(),
            request.data_dir.display().to_string(),
            "mcp".to_string(),
            "--config".to_string(),
            request.config_dir.display().to_string(),
        ];
        let harness = GrokHarness::start(
            &program,
            request.agent.model.as_deref(),
            session.as_deref(),
            &pairs(&env),
            &request.work_dir,
            &exe,
            &mcp_args,
        )
        .map_err(|err| err.to_string())?;
        let vh = VendorHarness::Grok(
            Box::new(harness),
            request.config_dir.join("grok-session-id"),
        );
        vh.sync_session();
        Ok(vh)
    } else if request.agent.harness == "sylphx" {
        let session = session_from_file(&request.config_dir, "sylphx-session-id");
        let exe = crate::data::current_exe_clean().unwrap_or_else(|_| PathBuf::from("anyagents"));
        let mcp_args = vec![
            "--data".to_string(),
            request.data_dir.display().to_string(),
            "mcp".to_string(),
            "--config".to_string(),
            request.config_dir.display().to_string(),
        ];
        let harness = SylphxHarness::start(
            &request.config_dir,
            &request.work_dir,
            request.agent.model.as_deref(),
            session.as_deref(),
            &pairs(&env),
            &exe,
            &mcp_args,
        )
        .map_err(|err| err.to_string())?;
        let vh = VendorHarness::Sylphx(
            Box::new(harness),
            request.config_dir.join("sylphx-session-id"),
        );
        vh.sync_session();
        Ok(vh)
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
        let program = crate::install::program(&root, &request.agent.harness);
        // The session this agent last ran is kept beside its config, so a
        // restart resumes that exact conversation instead of starting one.
        let session = session_from_file(&request.config_dir, "opencode-session-id");
        // The service speaks HTTP; the adapter drives it with `opencode api`.
        let harness = OpenCodeHarness::start_in_config(
            &program,
            session.as_deref(),
            &pairs(&env),
            Some(&request.work_dir),
            &request.config_dir,
        )
        .map_err(|err| {
            read_opencode_exit_error(&request.config_dir).unwrap_or_else(|| err.to_string())
        })?;
        let vh = VendorHarness::OpenCode(
            Box::new(harness),
            request.config_dir.join("opencode-session-id"),
        );
        vh.sync_session();
        Ok(vh)
    } else if request.agent.harness == "codex" {
        env.push((
            "CODEX_HOME".to_string(),
            request.config_dir.display().to_string(),
        ));
        let session = session_from_file(&request.config_dir, "codex-session-id");
        let program = crate::install::program(&root, &request.agent.harness);
        let harness = start_codex(
            &program.display().to_string(),
            session.as_deref(),
            &env,
            &request.work_dir,
        )?;
        let vh = VendorHarness::Codex(
            Box::new(harness),
            request.config_dir.join("codex-session-id"),
        );
        vh.sync_session();
        Ok(vh)
    } else {
        env.push((
            "CLAUDE_CONFIG_DIR".to_string(),
            request.config_dir.display().to_string(),
        ));
        let session = session_from_file(&request.config_dir, "claude-session-id");
        let extra_args = if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
            Vec::new()
        } else {
            let mut args = vec!["--dangerously-skip-permissions".to_string()];
            let mut effort = "medium".to_string();
            if let Some(model) = request
                .agent
                .model
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
            {
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
                    "Task,Agent,TeamCreate,TeamDelete,SendMessage,ScheduleWakeup,Monitor,CronCreate,CronDelete,CronList,RemoteTrigger,PushNotification"
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
        let program = crate::install::program(&root, &request.agent.harness);
        let harness = start_claude(
            &program.display().to_string(),
            &extra_args,
            session.as_deref(),
            &env,
            &request.work_dir,
        )?;
        let vh = VendorHarness::Claude(
            Box::new(harness),
            request.config_dir.join("claude-session-id"),
        );
        vh.sync_session();
        Ok(vh)
    }
}

/// Ensures `agy`'s `"queuedMessages": "send-immediately"`, `"mcp(anyagents/*)"`
/// tool permission, and `work_dir` workspace trust are active in the agent's
/// config directory and removes any legacy file-hook steering files
/// (`anyagents-steer-hook.sh`, `steer.jsonl`, and the `"anyagents-steer"` entry
/// in `hooks.json`), since `AgyHarness` steers natively via
/// `LanguageServerService/SendUserCascadeMessage`.
pub(crate) fn ensure_agy_steering_settings(config_dir: &Path, work_dir: &Path) {
    let dir = config_dir.join(".gemini").join("antigravity-cli");
    if std::fs::create_dir_all(&dir).is_ok() {
        let path = dir.join("settings.json");
        let mut map = std::fs::read_to_string(&path)
            .ok()
            .and_then(|text| {
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&text).ok()
            })
            .unwrap_or_default();
        let mut changed = false;
        if map.get("queuedMessages").and_then(|v| v.as_str()) != Some("send-immediately") {
            map.insert(
                "queuedMessages".to_string(),
                serde_json::Value::String("send-immediately".to_string()),
            );
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
            if let Some(allow_arr) = allow.as_array_mut()
                && !allow_arr
                    .iter()
                    .any(|v| v.as_str() == Some("mcp(anyagents/*)"))
            {
                allow_arr.push(serde_json::Value::String("mcp(anyagents/*)".to_string()));
                changed = true;
            }
            let deny = perms_obj
                .entry("deny".to_string())
                .or_insert_with(|| serde_json::json!([]));
            if !deny.is_array() {
                *deny = serde_json::json!([]);
                changed = true;
            }
            if let Some(deny_arr) = deny.as_array_mut() {
                if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
                    for tool in [
                        "invoke_subagent",
                        "define_subagent",
                        "manage_subagents",
                        "send_message",
                        "schedule",
                    ] {
                        if !deny_arr.iter().any(|v| v.as_str() == Some(tool)) {
                            deny_arr.push(serde_json::Value::String(tool.to_string()));
                            changed = true;
                        }
                    }
                } else {
                    let before = deny_arr.len();
                    deny_arr.retain(|v| {
                        !matches!(
                            v.as_str(),
                            Some(
                                "invoke_subagent"
                                    | "define_subagent"
                                    | "manage_subagents"
                                    | "send_message"
                                    | "schedule"
                            )
                        )
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
        if changed && let Ok(body) = serde_json::to_string_pretty(&serde_json::Value::Object(map)) {
            let _ = std::fs::write(&path, format!("{body}\n"));
        }
    }
    let hook_dir = config_dir.join(".gemini").join("config");
    let _ = std::fs::remove_file(hook_dir.join("anyagents-steer-hook.sh"));
    let _ = std::fs::remove_file(hook_dir.join("steer.jsonl"));
    let hooks_path = hook_dir.join("hooks.json");
    if let Ok(text) = std::fs::read_to_string(&hooks_path)
        && let Ok(mut hooks_map) =
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&text)
        && hooks_map.remove("anyagents-steer").is_some()
    {
        if hooks_map.is_empty() {
            let _ = std::fs::remove_file(&hooks_path);
        } else if let Ok(body) = serde_json::to_string_pretty(&serde_json::Value::Object(hooks_map))
        {
            let _ = std::fs::write(&hooks_path, format!("{body}\n"));
        }
    }
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

impl Harness for VendorHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let out = match self {
            Self::Claude(harness, _) => harness.deliver(message),
            Self::Codex(harness, _) => harness.deliver(message),
            Self::OpenCode(harness, _) => harness.deliver(message),
            Self::Agy(harness, _) => harness.deliver(message),
            Self::Muse(harness, _) => harness.deliver(message),
            Self::Omp(harness, _) => harness.deliver(message),
            Self::Dsh(harness, _) => harness.deliver(message),
            Self::Grok(harness, _) => harness.deliver(message),
            Self::Sylphx(harness, _) => harness.deliver(message),
            Self::Contract(harness) => harness.deliver(message),
        };
        self.sync_session();
        out
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness, _) => harness.background(),
            Self::Codex(harness, _) => harness.background(),
            Self::OpenCode(harness, _) => harness.background(),
            Self::Agy(harness, _) => harness.background(),
            Self::Muse(harness, _) => harness.background(),
            Self::Omp(harness, _) => harness.background(),
            Self::Dsh(harness, _) => harness.background(),
            Self::Grok(harness, _) => harness.background(),
            Self::Sylphx(harness, _) => harness.background(),
            Self::Contract(harness) => harness.background(),
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let out = match self {
            Self::Claude(harness, _) => harness.resume(session),
            Self::Codex(harness, _) => harness.resume(session),
            Self::OpenCode(harness, _) => harness.resume(session),
            Self::Agy(harness, _) => harness.resume(session),
            Self::Muse(harness, _) => harness.resume(session),
            Self::Omp(harness, _) => harness.resume(session),
            Self::Dsh(harness, _) => harness.resume(session),
            Self::Grok(harness, _) => harness.resume(session),
            Self::Sylphx(harness, _) => harness.resume(session),
            Self::Contract(harness) => harness.resume(session),
        };
        self.sync_session();
        out
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness, _) => harness.cancel(),
            Self::Codex(harness, _) => harness.cancel(),
            Self::OpenCode(harness, _) => harness.cancel(),
            Self::Agy(harness, _) => harness.cancel(),
            Self::Muse(harness, _) => harness.cancel(),
            Self::Omp(harness, _) => harness.cancel(),
            Self::Dsh(harness, _) => harness.cancel(),
            Self::Grok(harness, _) => harness.cancel(),
            Self::Sylphx(harness, _) => harness.cancel(),
            Self::Contract(harness) => harness.cancel(),
        }
    }

    fn exit_diagnostic(&self) -> Option<String> {
        match self {
            Self::Claude(harness, _) => harness.exit_diagnostic(),
            _ => None,
        }
    }

    fn health(&self) -> HarnessHealth {
        self.sync_session();
        match self {
            Self::Claude(harness, _) => harness.health(),
            Self::Codex(harness, _) => harness.health(),
            Self::OpenCode(harness, _) => harness.health(),
            Self::Agy(harness, _) => harness.health(),
            Self::Muse(harness, _) => harness.health(),
            Self::Omp(harness, _) => harness.health(),
            Self::Dsh(harness, _) => harness.health(),
            Self::Grok(harness, _) => harness.health(),
            Self::Sylphx(harness, _) => harness.health(),
            Self::Contract(harness) => harness.health(),
        }
    }

    fn input_ready(&self) -> bool {
        match self {
            Self::Claude(harness, _) => harness.input_ready(),
            Self::Codex(harness, _) => harness.input_ready(),
            Self::OpenCode(harness, _) => harness.input_ready(),
            Self::Agy(harness, _) => harness.input_ready(),
            Self::Muse(harness, _) => harness.input_ready(),
            Self::Omp(harness, _) => harness.input_ready(),
            Self::Dsh(harness, _) => harness.input_ready(),
            Self::Grok(harness, _) => harness.input_ready(),
            Self::Sylphx(harness, _) => harness.input_ready(),
            Self::Contract(harness) => harness.input_ready(),
        }
    }

    fn last_usage(&self) -> TurnUsage {
        self.sync_session();
        match self {
            Self::Claude(harness, _) => harness.last_usage(),
            Self::Codex(harness, _) => harness.last_usage(),
            Self::OpenCode(harness, _) => harness.last_usage(),
            Self::Agy(harness, _) => harness.last_usage(),
            Self::Muse(harness, _) => harness.last_usage(),
            Self::Omp(harness, _) => harness.last_usage(),
            Self::Dsh(harness, _) => harness.last_usage(),
            Self::Grok(harness, _) => harness.last_usage(),
            Self::Sylphx(harness, _) => harness.last_usage(),
            Self::Contract(harness) => harness.last_usage(),
        }
    }

    fn turns_served(&self) -> u32 {
        match self {
            Self::Claude(harness, _) => harness.turns_served(),
            Self::Codex(harness, _) => harness.turns_served(),
            Self::OpenCode(harness, _) => harness.turns_served(),
            Self::Agy(harness, _) => harness.turns_served(),
            Self::Muse(harness, _) => harness.turns_served(),
            Self::Omp(harness, _) => harness.turns_served(),
            Self::Dsh(harness, _) => harness.turns_served(),
            Self::Grok(harness, _) => harness.turns_served(),
            Self::Sylphx(harness, _) => harness.turns_served(),
            Self::Contract(harness) => harness.turns_served(),
        }
    }

    fn quiet_for(&self, id: &str, quiet: Duration, patience: Duration) -> bool {
        match self {
            Self::Claude(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Codex(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::OpenCode(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Agy(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Muse(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Omp(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Dsh(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Grok(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Sylphx(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Contract(harness) => harness.quiet_for(id, quiet, patience),
        }
    }

    fn ends_on_quiet(&self) -> Option<Duration> {
        match self {
            Self::Claude(harness, _) => harness.ends_on_quiet(),
            Self::Codex(harness, _) => harness.ends_on_quiet(),
            Self::OpenCode(harness, _) => harness.ends_on_quiet(),
            Self::Agy(harness, _) => harness.ends_on_quiet(),
            Self::Muse(harness, _) => harness.ends_on_quiet(),
            Self::Omp(harness, _) => harness.ends_on_quiet(),
            Self::Dsh(harness, _) => harness.ends_on_quiet(),
            Self::Grok(harness, _) => harness.ends_on_quiet(),
            Self::Sylphx(harness, _) => harness.ends_on_quiet(),
            Self::Contract(harness) => harness.ends_on_quiet(),
        }
    }

    fn clear_quota(&mut self) {
        match self {
            Self::Claude(harness, _) => harness.clear_quota(),
            Self::Codex(harness, _) => harness.clear_quota(),
            Self::OpenCode(harness, _) => harness.clear_quota(),
            Self::Agy(harness, _) => harness.clear_quota(),
            Self::Muse(harness, _) => harness.clear_quota(),
            Self::Omp(harness, _) => harness.clear_quota(),
            Self::Dsh(harness, _) => harness.clear_quota(),
            Self::Grok(harness, _) => harness.clear_quota(),
            Self::Sylphx(harness, _) => harness.clear_quota(),
            Self::Contract(harness) => harness.clear_quota(),
        }
    }

    fn process_id(&self) -> Option<u32> {
        match self {
            Self::Claude(harness, _) => harness.process_id(),
            Self::Codex(harness, _) => harness.process_id(),
            Self::OpenCode(harness, _) => harness.process_id(),
            Self::Agy(harness, _) => harness.process_id(),
            Self::Muse(harness, _) => harness.process_id(),
            Self::Omp(harness, _) => harness.process_id(),
            Self::Dsh(harness, _) => harness.process_id(),
            Self::Grok(harness, _) => harness.process_id(),
            Self::Sylphx(harness, _) => harness.process_id(),
            Self::Contract(harness) => harness.process_id(),
        }
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        match self {
            Self::Claude(harness, _) => harness.watch_activity(notify),
            Self::Codex(harness, _) => harness.watch_activity(notify),
            Self::OpenCode(harness, _) => harness.watch_activity(notify),
            Self::Agy(harness, _) => harness.watch_activity(notify),
            Self::Muse(harness, _) => harness.watch_activity(notify),
            Self::Omp(harness, _) => harness.watch_activity(notify),
            Self::Dsh(harness, _) => harness.watch_activity(notify),
            Self::Grok(harness, _) => harness.watch_activity(notify),
            Self::Sylphx(harness, _) => harness.watch_activity(notify),
            Self::Contract(harness) => harness.watch_activity(notify),
        }
    }
}

/// The session id this agent last ran, kept beside its config.
fn session_from_file(config_dir: &Path, file_name: &str) -> Option<String> {
    let text = std::fs::read_to_string(config_dir.join(file_name)).ok()?;
    let id = text.trim();
    (!id.is_empty()).then(|| id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_native_memory_cannot_override_host_authority() {
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
        assert_eq!(output.stdout, b"1:kept");
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
            "synthetic-capability\n",
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
        for name in ["config", "memory", "skills"] {
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
        for name in ["sessions", "transcripts"] {
            directory(&root.join(name), 0o700, 0, 0);
            file(&root.join(name).join("private"), "host-only\n", 0o600, 0);
            protected.push(root.join(name).join("private"));
        }
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
printf synthetic-memory-after > "$ANYAGENTS_AGENT_CONFIG/../memory/MEMORY.md"
printf authored > "$ANYAGENTS_AGENT_CONFIG/../skills/authored"
rm "$ANYAGENTS_AGENT_CONFIG/../skills/authored"
for path in "$ANYAGENTS_DATA/secrets" "$ANYAGENTS_DATA/operator-token" "$ANYAGENTS_DATA/agents/foreign-seat/memory/private" "$ANYAGENTS_AGENT_CONFIG/../sessions/private"; do
  if cat "$path" >/dev/null 2>&1; then exit 41; fi
  if (printf forbidden > "$path") 2>/dev/null; then exit 42; fi
done
if (printf forbidden > "$ANYAGENTS_AGENT_CONFIG/.agy/skills/global/SKILL.md") 2>/dev/null; then exit 43; fi
printf 'ACTUAL_UID=%s PRIMARY_GID=%s REAL_MOUNTS_UMASK077_OWNER_FALLBACK_OK SAME_SEAT_OK MEMORY_RW_OK PRIVATE_GLOBAL_DENIED GIT_TRUST_EXACT\n' "$(id -u)" "$(id -g)" > "$ANYAGENTS_AGENT_CONFIG/native-vendor-report"
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
            std::env::set_var("ANYAGENTS_AGENT_UID", uid.to_string());
            std::env::set_var("ANYAGENTS_AGENT_GID", gid.to_string());
            std::env::set_var("ANYAGENTS_SOCKET_GID", "24033");
            std::env::set_var("ANYAGENTS_CLAUDE_CODE_BIN", &probe);
            std::env::set_var("ANYAGENTS_LAUNCHER", "/usr/bin/setpriv");
        }
        let launch = || {
            vendor_launch(LaunchRequest {
                agent: agent.clone(),
                data_dir: data.clone(),
                config_dir: config.clone(),
                work_dir: root.join("work"),
                extra_env: vec![],
            })
        };
        assert_eq!(
            crate::install::program(&data, "claude-code"),
            probe,
            "the actual resolver must select the synthetic native probe"
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
            0o700,
            "sharing must retain the existing owner-private vendor parent"
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
        println!("{evidence}");
        drop(harness);
        std::fs::remove_file(&report).unwrap();
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
            let harness = vendor_launch(LaunchRequest {
                agent: codex,
                data_dir: data.clone(),
                config_dir: config.clone(),
                work_dir: root.join("work"),
                extra_env: vec![],
            })
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
    fn ensure_agy_steering_settings_populates_permissions_and_trusted_workspaces() {
        let dir = crate::test_tempdir();
        let config_dir = dir.path().join("config");
        let work_dir = dir.path().join("work");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();

        ensure_agy_steering_settings(&config_dir, &work_dir);

        let settings_path = config_dir
            .join(".gemini")
            .join("antigravity-cli")
            .join("settings.json");
        let text = std::fs::read_to_string(&settings_path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["queuedMessages"], "send-immediately");
        assert_eq!(
            json["permissions"]["allow"],
            serde_json::json!(["mcp(anyagents/*)"])
        );
        assert_eq!(
            json["permissions"]["deny"],
            serde_json::json!([
                "invoke_subagent",
                "define_subagent",
                "manage_subagents",
                "send_message",
                "schedule"
            ])
        );
        assert_eq!(
            json["trustedWorkspaces"],
            serde_json::json!([work_dir.display().to_string()])
        );

        // Calling a second time is idempotent and does not duplicate entries.
        ensure_agy_steering_settings(&config_dir, &work_dir);
        let text2 = std::fs::read_to_string(&settings_path).unwrap();
        let json2: serde_json::Value = serde_json::from_str(&text2).unwrap();
        assert_eq!(json2, json);
    }
}
