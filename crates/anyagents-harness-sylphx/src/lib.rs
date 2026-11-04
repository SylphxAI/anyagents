//! Native `sylphx` harness (`Sylphx Agent`) and RFC 8628 Device Flow sign-in.
//!
//! Architecture & Ecosystem Funnel:
//! 1. `anyagents sign-in sylphx` (and `anyagents sign-in <agent>`) via RFC 8628
//!    Device Authorization against `https://api.sylphx.com/v1/access/device/authorize`
//!    and `/v1/access/device/token`, saving the issued org-scoped key to
//!    `.config/sylphx/credentials.json`, `.config/opencode/sylphx.key`, and
//!    `.config/sylphx/key` (`auth` submodule).
//! 2. `SylphxHarness` (`--harness sylphx`), our native harness that executes
//!    both root agents and spawned sub-agents directly without third-party CLIs:
//!    - **Sylphx Agents BaaS mode (`SYLPHX_AGENT_ENV`)**: Connects to
//!      `https://api.sylphx.com/v1/{env}/agent_sessions` (`sylphx.agents.v1`), passing
//!      a declarative `EnvironmentSpec` (`shape: "linux-xs"`, `template: "template:agent"`,
//!      `repo: { uri, revision }`) and bridging AnyAgents coordination tools via
//!      `:submitToolResult`.
//!    - **Native Sylphx AI + Sylphx Sandboxes mode**: Multi-step tool loop against
//!      `https://api.sylphx.com/v1/responses` with local or Cloud Kata microVM Sandbox
//!      worktrees (`cloud` submodule), KV-cache pinned append-only context windows,
//!      autonomous `context_window_open` compaction, and lossless `Raw-RAG` hybrid
//!      BM25 + episode recall (`memory` submodule).

mod aci;
mod auth;
mod cloud;
mod memory;
pub mod sdk;
mod workspace;

pub mod sandbox {
    use std::path::PathBuf;

    pub fn is_isolated_test_env() -> bool {
        cfg!(test)
            || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
            || std::env::var_os("ANYAGENTS_TESTS").is_some()
            || std::env::var_os("ANYAGENTS_FAKE_LOG").is_some()
            || std::env::var_os("ANYAGENTS_HARNESS_BIN").is_some()
            || std::env::var_os("ANYAGENTS_CLAUDE_BIN").is_some()
            || std::env::var_os("ANYAGENTS_CODEX_BIN").is_some()
            || std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_some()
            || std::env::var_os("CARGO_TARGET_TMPDIR").is_some()
            || std::env::var_os("NEXTEST_RUN_ID").is_some()
    }

    pub fn host_home_dir() -> Option<PathBuf> {
        if is_isolated_test_env() {
            return None;
        }
        if let Some(explicit) = std::env::var_os("ANYAGENTS_HOST_HOME")
            && !explicit.is_empty()
        {
            let path = PathBuf::from(explicit);
            if path.is_dir() {
                return Some(path);
            }
        }
        #[cfg(unix)]
        unsafe {
            let pwd = libc::getpwuid(libc::getuid());
            if !pwd.is_null() && !(*pwd).pw_dir.is_null() {
                let dir = std::ffi::CStr::from_ptr((*pwd).pw_dir).to_string_lossy();
                if !dir.is_empty() {
                    let path = PathBuf::from(dir.into_owned());
                    if path.is_dir() {
                        return Some(path);
                    }
                }
            }
        }
        std::env::var_os("HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .filter(|p| p.is_dir())
    }
}

use aci::*;
pub use auth::*;
pub use cloud::*;
pub use memory::*;
pub use sdk::*;

use std::collections::VecDeque;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnState, TurnUsage, marked_message,
};
use serde_json::{Value, json};

#[derive(Default)]
pub(crate) struct SharedState {
    in_turn: bool,
    turns_served: u32,
    served_usage: TurnUsage,
    session_id: Option<String>,
    cloud_session_id: Option<String>,
    cloud_agent_id: Option<String>,
    dead_reason: Option<String>,
    sign_in_fault: Option<String>,
    quota_reset_at_ms: Option<i64>,
    last_activity: Option<Instant>,
    steer_queue: VecDeque<String>,
    notify: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// First-class AnyAgents harness for `Sylphx Agent` (`--harness sylphx`).
pub struct SylphxHarness {
    config_dir: PathBuf,
    work_dir: PathBuf,
    model: Option<String>,
    env: Vec<(String, String)>,
    mcp_command: PathBuf,
    mcp_args: Vec<String>,
    fake_mode: bool,
    fake_log: Option<PathBuf>,
    cancel_flag: Arc<AtomicBool>,
    state: Arc<Mutex<SharedState>>,
}

pub(crate) fn clean_exe_path(path: &Path) -> PathBuf {
    let raw = path.display().to_string();
    match raw.strip_suffix(" (deleted)") {
        Some(stripped) => PathBuf::from(stripped),
        None => path.to_path_buf(),
    }
}

impl SylphxHarness {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        config_dir: &Path,
        work_dir: &Path,
        model: Option<&str>,
        session: Option<&str>,
        env: &[(&str, &str)],
        mcp_command: &Path,
        mcp_args: &[String],
    ) -> Result<Self, HarnessError> {
        let env_owned: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let fake_mode = env_owned
            .iter()
            .any(|(k, v)| k == "ANYAGENTS_FAKE_MODE" && !v.is_empty())
            || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some();
        let fake_log = env_owned
            .iter()
            .find(|(k, _)| k == "ANYAGENTS_FAKE_LOG")
            .map(|(_, v)| PathBuf::from(v))
            .or_else(|| std::env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from));

        let initial_session = session
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                std::fs::read_to_string(config_dir.join("sylphx-session-id"))
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_else(|| format!("sylphx_ses_{}", anyagents_core::now_ms()));

        let _ = std::fs::write(
            config_dir.join("sylphx-session-id"),
            format!("{initial_session}\n"),
        );

        let state = Arc::new(Mutex::new(SharedState {
            session_id: Some(initial_session),
            ..SharedState::default()
        }));

        Ok(Self {
            config_dir: config_dir.to_path_buf(),
            work_dir: work_dir.to_path_buf(),
            model: model.map(str::to_string),
            env: env_owned,
            mcp_command: clean_exe_path(mcp_command),
            mcp_args: mcp_args.to_vec(),
            fake_mode,
            fake_log,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            state,
        })
    }

    pub fn session_id(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .session_id
            .clone()
    }

    #[cfg(test)]
    pub fn declared_capabilities() -> (DeliveryMode, SteerSupport) {
        (DeliveryMode::Steer, SteerSupport::Yes)
    }

    fn cloud_environment(&self) -> Option<String> {
        resolve_cloud_agent_environment(&self.config_dir, &self.env)
    }
}

pub fn resolve_cloud_agent_environment(
    config_dir: &Path,
    env: &[(String, String)],
) -> Option<String> {
    if matches!(
        sdk::SylphxExecutionMode::from_env(config_dir, env),
        sdk::SylphxExecutionMode::LocalOnly
    ) {
        return None;
    }
    for (k, v) in env {
        if k == "SYLPHX_AGENT_ENV" && !v.trim().is_empty() {
            let trimmed = v.trim();
            if matches!(
                trimmed.to_ascii_lowercase().as_str(),
                "local" | "none" | "off" | "false" | "0"
            ) {
                return None;
            }
            return Some(normalize_cloud_env_slug(trimmed));
        }
    }
    if let Ok(v) = std::env::var("SYLPHX_AGENT_ENV")
        && !v.trim().is_empty()
    {
        let trimmed = v.trim();
        if matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "local" | "none" | "off" | "false" | "0"
        ) {
            return None;
        }
        return Some(normalize_cloud_env_slug(trimmed));
    }
    if let Some(persisted) = load_persisted_cloud_session(config_dir)
        && !persisted.env_slug.trim().is_empty()
    {
        return Some(normalize_cloud_env_slug(&persisted.env_slug));
    }
    let ctx_path = config_dir.join(".config/sylphx/context.json");
    if let Ok(raw) = std::fs::read_to_string(ctx_path)
        && let Ok(val) = serde_json::from_str::<Value>(&raw)
        && val.get("cloud_session").and_then(Value::as_bool) == Some(true)
        && let Some(env_slug) = val
            .get("environment")
            .or_else(|| val.get("env"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
    {
        return Some(normalize_cloud_env_slug(env_slug));
    }
    if matches!(
        sdk::SylphxExecutionMode::from_env(config_dir, env),
        sdk::SylphxExecutionMode::CloudOnly
    ) && resolve_sylphx_cloud_access_key(config_dir, env).is_some()
    {
        return Some(DEFAULT_SYLPHX_SANDBOX_PARENT.to_string());
    }
    None
}

impl Harness for SylphxHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let marked = marked_message(&message.id, &message.text);
        if let Some(ref log_path) = self.fake_log {
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)
                .and_then(|mut f| writeln!(f, "sylphx deliver {}: {}", message.id, message.text));
        }

        {
            let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(ref reason) = guard.dead_reason {
                return Err(HarnessError::Failed(reason.clone()));
            }
            if let Some(reset_ms) = guard.quota_reset_at_ms {
                return Ok(DeliverOutcome::Quota {
                    reset_at_ms: reset_ms,
                });
            }
            if guard.in_turn {
                guard.steer_queue.push_back(marked.clone());
                guard.last_activity = Some(Instant::now());
                let cloud_sess = guard.cloud_session_id.clone();
                let notify = guard.notify.clone();
                drop(guard);

                // If bound to a remote Sylphx Cloud AgentSession, forward the `:steer` call immediately.
                if let (Some(env_slug), Some(sess_id)) = (self.cloud_environment(), cloud_sess)
                    && let Some(api_key) = resolve_sylphx_api_key(&self.config_dir, &self.env)
                {
                    let api_url = resolve_cloud_api_url(&self.env);
                    let sess_path = cloud_session_resource_path(&env_slug, &sess_id);
                    let steer_url = format!("{api_url}/v1/{sess_path}:steer");
                    let _ = curl_post_json(
                        &steer_url,
                        Some(&api_key),
                        &json!({
                            "message": {
                                "parts": [{ "text": marked }]
                            }
                        }),
                    );
                }
                if let Some(cb) = notify {
                    cb();
                }
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                });
            }
            guard.in_turn = true;
            guard.sign_in_fault = None;
            guard.last_activity = Some(Instant::now());
        }

        self.cancel_flag.store(false, Ordering::SeqCst);

        if self.fake_mode {
            let state = Arc::clone(&self.state);
            let cancel_flag = Arc::clone(&self.cancel_flag);
            let text_owned = message.text.clone();
            std::thread::spawn(move || {
                if text_owned.contains("sleep:") {
                    for _ in 0..20 {
                        if cancel_flag.load(Ordering::SeqCst) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(25));
                    }
                }
                let notify = {
                    let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                    g.steer_queue.clear();
                    g.in_turn = false;
                    g.turns_served = g.turns_served.saturating_add(1);
                    g.served_usage.input_tokens = g.served_usage.input_tokens.saturating_add(64);
                    g.served_usage.output_tokens = g.served_usage.output_tokens.saturating_add(32);
                    g.last_activity = Some(Instant::now());
                    g.notify.clone()
                };
                if let Some(cb) = notify {
                    cb();
                }
            });
            return Ok(DeliverOutcome::Accepted {
                turn_finished: false,
            });
        }

        let state = Arc::clone(&self.state);
        let cancel_flag = Arc::clone(&self.cancel_flag);
        let config_dir = self.config_dir.clone();
        let work_dir = self.work_dir.clone();
        let model = self.model.clone();
        let env = self.env.clone();
        let mcp_command = self.mcp_command.clone();
        let mcp_args = self.mcp_args.clone();
        let cloud_env = self.cloud_environment();

        std::thread::spawn(move || {
            let outcome = if let Some(env_slug) = cloud_env {
                match run_cloud_session_turn(
                    &state,
                    &cancel_flag,
                    &config_dir,
                    &work_dir,
                    model.as_deref(),
                    &env,
                    &mcp_command,
                    &mcp_args,
                    &env_slug,
                    &marked,
                ) {
                    Ok(usage) => Ok(usage),
                    Err(cloud_err) => {
                        if !crate::sandbox::is_isolated_test_env() {
                            Err(cloud_err)
                        } else {
                            run_native_gateway_turn(
                                &state,
                                &cancel_flag,
                                &config_dir,
                                &work_dir,
                                model.as_deref(),
                                &env,
                                &mcp_command,
                                &mcp_args,
                                &marked,
                            )
                        }
                    }
                }
            } else {
                run_native_gateway_turn(
                    &state,
                    &cancel_flag,
                    &config_dir,
                    &work_dir,
                    model.as_deref(),
                    &env,
                    &mcp_command,
                    &mcp_args,
                    &marked,
                )
            };

            let notify = {
                let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                g.in_turn = false;
                g.last_activity = Some(Instant::now());
                match outcome {
                    Ok(usage) => {
                        g.sign_in_fault = None;
                        g.turns_served = g.turns_served.saturating_add(1);
                        g.served_usage.input_tokens = g
                            .served_usage
                            .input_tokens
                            .saturating_add(usage.input_tokens);
                        g.served_usage.output_tokens = g
                            .served_usage
                            .output_tokens
                            .saturating_add(usage.output_tokens);
                        g.served_usage.cache_read_tokens = g
                            .served_usage
                            .cache_read_tokens
                            .saturating_add(usage.cache_read_tokens);
                        g.served_usage.cache_write_tokens = g
                            .served_usage
                            .cache_write_tokens
                            .saturating_add(usage.cache_write_tokens);
                        let _ = std::fs::remove_file(config_dir.join(".anyagents-last-error.json"));
                    }
                    Err(err_msg) => {
                        let consecutive = persist_turn_error(&config_dir, &err_msg);
                        let lower = err_msg.to_ascii_lowercase();
                        if lower.contains("insufficient_quota")
                            || lower.contains("quota exceeded")
                            || lower.contains("credit balance is too low")
                            || lower.contains("out of credits")
                        {
                            g.quota_reset_at_ms = Some(anyagents_core::now_ms() + 300_000);
                        } else if lower.contains("http 401")
                            || lower.contains("status 401")
                            || lower.contains("unauthorized")
                            || lower.contains("invalid_api_key")
                            || lower.contains("missing sylphx api key")
                        {
                            g.sign_in_fault = Some(err_msg.clone());
                        } else {
                            g.sign_in_fault = Some(err_msg.clone());
                            let non_retryable = lower.contains("\"retryable\":false")
                                || lower.contains("\"retryable\": false");
                            if !non_retryable && consecutive <= 3 {
                                g.dead_reason = Some(err_msg.clone());
                            }
                        }
                    }
                }
                g.notify.clone()
            };
            if let Some(cb) = notify {
                cb();
            }
        });

        Ok(DeliverOutcome::Accepted {
            turn_finished: false,
        })
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let trimmed = session.trim();
        if !trimmed.is_empty() {
            let mut g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            g.session_id = Some(trimmed.to_string());
            let _ = std::fs::write(
                self.config_dir.join("sylphx-session-id"),
                format!("{trimmed}\n"),
            );
        }
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.cancel_flag.store(true, Ordering::SeqCst);
        let (cloud_sess, notify) = {
            let mut g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            g.steer_queue.clear();
            g.in_turn = false;
            (g.cloud_session_id.clone(), g.notify.clone())
        };
        if let (Some(env_slug), Some(sess_id)) = (self.cloud_environment(), cloud_sess)
            && let Some(api_key) = resolve_sylphx_api_key(&self.config_dir, &self.env)
        {
            let api_url = std::env::var("SYLPHX_API_URL")
                .ok()
                .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string());
            let sess_path = cloud_session_resource_path(&env_slug, &sess_id);
            let interrupt_url =
                format!("{}/v1/{sess_path}:interrupt", api_url.trim_end_matches('/'));
            let _ = curl_post_json(&interrupt_url, Some(&api_key), &json!({}));
        }
        if let Some(cb) = notify {
            cb();
        }
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        HarnessHealth {
            process_alive: g.dead_reason.is_none(),
            turn_state: if g.in_turn {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: g.quota_reset_at_ms,
            sign_in_fault: g.sign_in_fault.clone(),
        }
    }

    fn input_ready(&self) -> bool {
        true
    }

    fn served_usage(&self) -> TurnUsage {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .served_usage
            .clone()
    }

    fn turns_served(&self) -> u32 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .turns_served
    }

    fn quiet_for(&self, _id: &str, quiet: Duration, _patience: Duration) -> bool {
        let g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if g.in_turn {
            return false;
        }
        g.last_activity
            .map(|t| t.elapsed() >= quiet)
            .unwrap_or(true)
    }

    fn ends_on_quiet(&self) -> Option<Duration> {
        None
    }

    fn clear_quota(&mut self) {
        let mut g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        g.quota_reset_at_ms = None;
        g.sign_in_fault = None;
    }

    fn process_id(&self) -> Option<u32> {
        Some(std::process::id())
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notify = Some(notify);
    }
}

fn persist_turn_error(config_dir: &Path, err_msg: &str) -> u64 {
    let err_path = config_dir.join(".anyagents-last-error.json");
    let prev_count = std::fs::read_to_string(&err_path)
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .and_then(|v| v.get("consecutive_turn_errors").and_then(Value::as_u64))
        .unwrap_or(0);
    let next_count = prev_count + 1;
    let doc = json!({
        "error": err_msg,
        "timestamp_ms": anyagents_core::now_ms(),
        "consecutive_turn_errors": next_count,
    });
    let _ = std::fs::write(&err_path, format!("{doc}\n"));
    next_count
}

fn cloud_session_resource_path(env_slug: &str, sess_id: &str) -> String {
    let trimmed = sess_id.trim().trim_start_matches('/');
    if trimmed.contains("/agent_sessions/") {
        trimmed.to_string()
    } else {
        let canonical_env = normalize_cloud_env_slug(env_slug);
        format!("{canonical_env}/agent_sessions/{trimmed}")
    }
}

/// Returns true only for AnyAgents multi-agent coordination / local takeover tools that
/// must be bridged to the local AnyAgents daemon via `:submitToolResult`.
///
/// Workspace execution (`bash`/`shell`, `read_file`/`write_file`/`edit_file`/`apply_patch`,
/// `grep`, `glob`, `list_dir`, `code_mode`, `sandbox`) and in-session context tools
/// (`memory_recall`, `context_budget_read`, `context_edit`, `context_window_open`,
/// `activate_skill`) are already provided natively by the Cloud Agent Runtime and Sandbox
/// (`builtin: {id: "shell"}`, `builtin: {id: "files"}`), so they are never duplicated as
/// client tools.
fn is_cloud_client_coordination_tool(name: &str) -> bool {
    matches!(
        name,
        "send"
            | "describe"
            | "archive"
            | "agents"
            | "conversation"
            | "groups"
            | "schedule"
            | "browser"
            | "goal"
    )
}

fn build_cloud_agent_client_tools() -> Vec<Value> {
    build_tool_definitions()
        .into_iter()
        .filter_map(|t| {
            let func = t.get("function")?;
            let name = func.get("name")?.as_str()?;
            if !is_cloud_client_coordination_tool(name) {
                return None;
            }
            Some(json!({
                "name": name,
                "description": func.get("description").cloned().unwrap_or(Value::String(String::new())),
                "input_schema": func.get("parameters").cloned().unwrap_or_else(|| json!({"type": "object", "properties": {}})),
                "client": {
                    "timeout": "600s"
                },
            }))
        })
        .collect()
}

fn build_cloud_agent_tools() -> Vec<Value> {
    build_cloud_agent_client_tools()
}

fn format_rfc3339_utc_plus_secs(offset_secs: u64) -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(1_793_640_000)
        .saturating_add(offset_secs);
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}Z")
}

#[allow(clippy::too_many_arguments)]
fn capture_cloud_turn_execution(
    base: &str,
    canonical_env: &str,
    api_key: &str,
    gateway_key: &str,
    repo_token: Option<&str>,
    kubeconfig_b64: Option<&str>,
    sess_path: &str,
    turn: i64,
    has_repo: bool,
) -> Result<Option<Value>, String> {
    let capture_url = format!("{base}/v1/{canonical_env}/agents:capture-credential");
    let expires_at = format_rfc3339_utc_plus_secs(10_800);
    let capture_one = |purpose: &str,
                       allowed_tools: &[&str],
                       credential: &str|
     -> Result<Option<String>, String> {
        let body = json!({
            "session": sess_path,
            "turn": turn,
            "purpose": purpose,
            "allowed_tools": allowed_tools,
            "credential": credential,
            "expires_at": expires_at,
        });
        let (st, raw) = curl_post_json(&capture_url, Some(api_key), &body)?;
        if st == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&st) {
            return Err(format!(
                "cloud capture-credential ({purpose}) HTTP {st}: {}",
                raw.trim()
            ));
        }
        let val: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        val.get("handle")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(|s| Some(s.to_string()))
            .ok_or_else(|| format!("cloud capture-credential ({purpose}) missing handle"))
    };

    let Some(runtime_handle) = capture_one("runtime", &["runtime_api"], api_key)? else {
        return Ok(None);
    };
    let Some(inference_handle) = capture_one("execution", &["inference"], gateway_key)? else {
        return Ok(None);
    };
    let mut credentials = serde_json::Map::new();
    credentials.insert(
        "inference".to_string(),
        json!({ "handle": inference_handle }),
    );
    if let Some(tok) = repo_token.filter(|s| !s.trim().is_empty()) {
        if has_repo && let Some(repo_handle) = capture_one("execution", &["repository"], tok)? {
            credentials.insert("repository".to_string(), json!({ "handle": repo_handle }));
        }
        if let Ok(Some(gh_handle)) = capture_one("execution", &["sandbox", "GH_TOKEN"], tok) {
            credentials.insert("GH_TOKEN".to_string(), json!({ "handle": gh_handle }));
        }
    }
    if !api_key.trim().is_empty()
        && let Ok(Some(sylphx_handle)) =
            capture_one("execution", &["sandbox", "SYLPHX_API_KEY"], api_key)
    {
        credentials.insert(
            "SYLPHX_API_KEY".to_string(),
            json!({ "handle": sylphx_handle }),
        );
    }
    if let Some(kc) = kubeconfig_b64.filter(|s| !s.trim().is_empty())
        && let Ok(Some(kc_handle)) = capture_one("execution", &["sandbox", "KUBECONFIG_B64"], kc)
    {
        credentials.insert("KUBECONFIG_B64".to_string(), json!({ "handle": kc_handle }));
    }
    Ok(Some(json!({
        "turn": turn,
        "runtime_credential_handle": runtime_handle,
        "credentials": Value::Object(credentials),
    })))
}

/// Runs a turn against the remote Sylphx Cloud Agents BaaS (`/v1/{env}/agent_sessions`),
/// passing a declarative `EnvironmentSpec` (`shape: "linux-s"`, `template: "template:agent"`,
/// `repo: { uri, revision }`), native cloud `builtin` tools (`shell`, `files`), and bridging
/// only AnyAgents coordination client tool calls (`send`, `agents`, `conversation`, `groups`,
/// `schedule`, `browser`, `describe`, `archive`, `goal`) via `:submitToolResult`.
#[allow(clippy::too_many_arguments)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct PersistedCloudSession {
    pub agent_id: String,
    pub session_id: String,
    pub model_id: String,
    pub env_slug: String,
    #[serde(default)]
    pub last_seq: i64,
}

pub fn load_persisted_cloud_session(config_dir: &Path) -> Option<PersistedCloudSession> {
    let raw = std::fs::read_to_string(config_dir.join("sylphx-cloud-session.json")).ok()?;
    let parsed: PersistedCloudSession = serde_json::from_str(&raw).ok()?;
    if parsed.agent_id.trim().is_empty() || parsed.session_id.trim().is_empty() {
        return None;
    }
    Some(parsed)
}

pub fn save_persisted_cloud_session(config_dir: &Path, session: &PersistedCloudSession) {
    if let Ok(raw) = serde_json::to_string_pretty(session) {
        let _ = std::fs::write(config_dir.join("sylphx-cloud-session.json"), raw);
    }
}

pub fn clear_persisted_cloud_session(config_dir: &Path) {
    let _ = std::fs::remove_file(config_dir.join("sylphx-cloud-session.json"));
}

/// Builds a bounded continuity digest from `sylphx-transcript.jsonl` when opening a new
/// Cloud `AgentSession` after prior local turns or after a model switch so the new Cloud
/// session starts with the agent's recent conversation and tool history.
pub fn build_prior_transcript_continuity_block(transcript_path: &Path) -> Option<String> {
    let items = load_transcript_input_items(transcript_path);
    if items.is_empty() {
        return None;
    }
    let tail_start = items.len().saturating_sub(16);
    let mut lines = Vec::new();
    for item in &items[tail_start..] {
        if let Some(role) = item.get("role").and_then(Value::as_str) {
            let text = if let Some(s) = item.get("content").and_then(Value::as_str) {
                s.to_string()
            } else if let Some(arr) = item.get("content").and_then(Value::as_array) {
                arr.iter()
                    .filter_map(|p| p.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                String::new()
            };
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                let bounded = truncate_tool_output_middle_utf8(trimmed.to_string(), 800, 350, 350);
                lines.push(format!("[{role}]: {bounded}"));
            }
        } else if item.get("type").and_then(Value::as_str) == Some("function_call") {
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let args = item
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or("{}");
            let bounded = truncate_tool_output_middle_utf8(args.to_string(), 300, 140, 140);
            lines.push(format!("[tool_call:{name}]: {bounded}"));
        }
    }
    if lines.is_empty() {
        return None;
    }
    Some(format!(
        "<prior_session_continuity>\n\
         The following turns occurred earlier in this agent's durable session (also searchable via `memory_recall`):\n\
         {}\n\
         </prior_session_continuity>",
        lines.join("\n")
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_cloud_session_turn(
    state: &Arc<Mutex<SharedState>>,
    cancel_flag: &Arc<AtomicBool>,
    config_dir: &Path,
    work_dir: &Path,
    model: Option<&str>,
    env: &[(String, String)],
    mcp_command: &Path,
    mcp_args: &[String],
    env_slug: &str,
    user_message: &str,
) -> Result<TurnUsage, String> {
    let api_key = resolve_sylphx_cloud_access_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx API key; run `anyagents sign-in sylphx` first".to_string()
    })?;
    let gateway_key =
        resolve_sylphx_gateway_key(config_dir, env).unwrap_or_else(|| api_key.clone());
    let repo_token = resolve_github_token(env);
    let kubeconfig_b64 = resolve_kubeconfig_b64(env);
    let base = resolve_cloud_api_url(env);
    let canonical_env = normalize_cloud_env_slug(env_slug);
    let (model_id, _) = parse_sylphx_model_and_effort(model);
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");

    let (mut agent_id, mut session_id) = {
        let g = state.lock().unwrap_or_else(PoisonError::into_inner);
        (g.cloud_agent_id.clone(), g.cloud_session_id.clone())
    };
    let mut initial_last_seq: i64 = 0;
    let mut next_turn: i64 = 1;

    if let Some(persisted) = load_persisted_cloud_session(config_dir) {
        if persisted.model_id == model_id && persisted.env_slug == canonical_env {
            if agent_id.is_none() {
                agent_id = Some(persisted.agent_id.clone());
            }
            if session_id.is_none() {
                session_id = Some(persisted.session_id.clone());
            }
            initial_last_seq = persisted.last_seq;
        } else {
            // Model or environment changed: interrupt the old cloud session and open a fresh one for the new model.
            let old_sess_path =
                cloud_session_resource_path(&persisted.env_slug, &persisted.session_id);
            let old_interrupt_url = format!("{base}/v1/{old_sess_path}:interrupt");
            let _ = curl_post_json(&old_interrupt_url, Some(&api_key), &json!({}));
            clear_persisted_cloud_session(config_dir);
            agent_id = None;
            session_id = None;
            let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
            g.cloud_agent_id = None;
            g.cloud_session_id = None;
        }
    }

    // If we are re-using an existing Cloud Session, verify it is still healthy and settle any interrupted in-flight turn.
    if let Some(ref sid) = session_id.clone() {
        let sess_path = cloud_session_resource_path(&canonical_env, sid);
        let sess_url = format!("{base}/v1/{sess_path}");
        match curl_get(&sess_url, Some(&api_key)) {
            Ok((200..=299, raw_poll)) => {
                if let Ok(val) = serde_json::from_str::<Value>(&raw_poll) {
                    let status = val
                        .get("state")
                        .or_else(|| val.get("status"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    let tc = val
                        .get("turn_count")
                        .or_else(|| val.get("turnCount"))
                        .and_then(|v| {
                            v.as_i64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                        })
                        .unwrap_or(0);
                    let s_seq = val
                        .get("last_event_sequence")
                        .or_else(|| val.get("lastEventSequence"))
                        .and_then(|v| {
                            v.as_i64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                        })
                        .unwrap_or(0);
                    initial_last_seq = initial_last_seq.max(s_seq);
                    next_turn = tc.saturating_add(1);
                    if status == "failed"
                        || status == "session_state_failed"
                        || status == "terminated"
                        || status == "errored"
                    {
                        if status.contains("running")
                            || status.contains("working")
                            || status.contains("waiting")
                            || status.contains("awaiting")
                        {
                            let interrupt_url = format!("{base}/v1/{sess_path}:interrupt");
                            let _ = curl_post_json(&interrupt_url, Some(&api_key), &json!({}));
                        }
                        clear_persisted_cloud_session(config_dir);
                        agent_id = None;
                        session_id = None;
                        initial_last_seq = 0;
                        next_turn = 1;
                        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                        g.cloud_agent_id = None;
                        g.cloud_session_id = None;
                    } else if status.contains("running")
                        || status.contains("working")
                        || status.contains("waiting")
                        || status.contains("awaiting")
                    {
                        let interrupt_url = format!("{base}/v1/{sess_path}:interrupt");
                        let _ = curl_post_json(&interrupt_url, Some(&api_key), &json!({}));
                        let mut settled = false;
                        for _ in 0..20 {
                            std::thread::sleep(Duration::from_millis(200));
                            if let Ok((200..=299, raw_after)) = curl_get(&sess_url, Some(&api_key))
                                && let Ok(val_after) = serde_json::from_str::<Value>(&raw_after)
                            {
                                let st_after = val_after
                                    .get("state")
                                    .or_else(|| val_after.get("status"))
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_ascii_lowercase();
                                let tc_after = val_after
                                    .get("turn_count")
                                    .or_else(|| val_after.get("turnCount"))
                                    .and_then(|v| {
                                        v.as_i64().or_else(|| {
                                            v.as_str().and_then(|s| s.parse::<i64>().ok())
                                        })
                                    })
                                    .unwrap_or(tc);
                                let seq_after = val_after
                                    .get("last_event_sequence")
                                    .or_else(|| val_after.get("lastEventSequence"))
                                    .and_then(|v| {
                                        v.as_i64().or_else(|| {
                                            v.as_str().and_then(|s| s.parse::<i64>().ok())
                                        })
                                    })
                                    .unwrap_or(initial_last_seq);
                                initial_last_seq = initial_last_seq.max(seq_after);
                                next_turn = tc_after.saturating_add(1);
                                if st_after == "idle"
                                    || st_after == "session_state_idle"
                                    || st_after == "completed"
                                {
                                    settled = true;
                                    break;
                                }
                            }
                        }
                        if !settled {
                            clear_persisted_cloud_session(config_dir);
                            agent_id = None;
                            session_id = None;
                            initial_last_seq = 0;
                            next_turn = 1;
                            let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                            g.cloud_agent_id = None;
                            g.cloud_session_id = None;
                        }
                    }
                }
            }
            Ok((404, _)) => {
                clear_persisted_cloud_session(config_dir);
                agent_id = None;
                session_id = None;
                initial_last_seq = 0;
                next_turn = 1;
            }
            _ => {}
        }
    }

    let light_model = env
        .iter()
        .find(|(k, v)| k == "SYLPHX_LIGHT_MODEL" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().to_string())
        .or_else(|| {
            std::env::var("SYLPHX_LIGHT_MODEL")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "google/gemini-3-flash".to_string());
    let mut system_prompt = assemble_system_prompt(config_dir, work_dir);
    if session_id.is_none()
        && let Some(prior_block) = build_prior_transcript_continuity_block(&transcript_path)
    {
        system_prompt.push_str("\n\n");
        system_prompt.push_str(&prior_block);
    }
    let environment_spec = build_cloud_environment_spec(config_dir, Some(work_dir), env);
    if let Some(repo_uri) = environment_spec
        .pointer("/repo/uri")
        .and_then(Value::as_str)
    {
        let worktree_spec =
            resolve_agent_git_worktree_spec(config_dir, Some(work_dir), env, None, None, None);
        let branch_hint = worktree_spec
            .branch
            .as_deref()
            .filter(|b| !b.is_empty())
            .map(|b| format!(" (target branch: `{b}`)"))
            .unwrap_or_default();
        system_prompt.push_str(&format!(
            "\n\n<cloud_workspace>\nYou are running as a Whole Cloud Agent inside a Cloud Sandbox container.\nYour repository (`{repo_uri}`) is checked out at `{CLOUD_WORKSPACE_DIR}`{branch_hint}.\nRun all workspace commands (`bash`, `read_file`, `write_file`, `edit_file`, `git`) inside `{CLOUD_WORKSPACE_DIR}`.\n</cloud_workspace>"
        ));
    }

    if agent_id.is_none() {
        let create_agent_url = format!("{base}/v1/{canonical_env}/agents");
        let agent_short_name = config_dir
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .unwrap_or("anyagents-sylphx");
        let spec_body = json!({
            "meta": {
                "display_name": agent_short_name,
            },
            "spec": {
                "models": {
                    "primary": model_id,
                    "light": light_model,
                },
                "instructions": system_prompt,
                "environment": environment_spec,
                "tools": build_cloud_agent_tools(),
            },
        });
        let (st, raw) = curl_post_json(&create_agent_url, Some(&api_key), &spec_body)?;
        if !(200..300).contains(&st) {
            return Err(format!("cloud create_agent HTTP {st}: {}", raw.trim()));
        }
        let val: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        let id = val
            .get("name")
            .or_else(|| val.get("id"))
            .or_else(|| val.get("uid"))
            .and_then(Value::as_str)
            .ok_or_else(|| "cloud create_agent missing name/id".to_string())?
            .to_string();
        agent_id = Some(id.clone());
        state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cloud_agent_id = Some(id);
    }

    if session_id.is_none() {
        let create_sess_url = format!("{base}/v1/{canonical_env}/agent_sessions");
        let agent_ref = agent_id.as_deref().unwrap_or_default();
        let body = json!({
            "agent": agent_ref,
            "title": "AnyAgents Session",
            "environment": environment_spec,
        });
        let (st, raw) = curl_post_json(&create_sess_url, Some(&api_key), &body)?;
        if !(200..300).contains(&st) {
            return Err(format!("cloud create_session HTTP {st}: {}", raw.trim()));
        }
        let val: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        let sid = val
            .get("name")
            .or_else(|| val.get("id"))
            .or_else(|| val.get("uid"))
            .and_then(Value::as_str)
            .ok_or_else(|| "cloud create_session missing name/id".to_string())?
            .to_string();
        session_id = Some(sid.clone());
        initial_last_seq = 0;
        next_turn = 1;
        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
        g.cloud_session_id = Some(sid.clone());
        g.session_id = Some(sid.clone());
        let _ = std::fs::write(config_dir.join("sylphx-session-id"), format!("{sid}\n"));
    }

    let aid = agent_id.unwrap_or_default();
    let mut sid = session_id.unwrap_or_default();
    save_persisted_cloud_session(
        config_dir,
        &PersistedCloudSession {
            agent_id: aid.clone(),
            session_id: sid.clone(),
            model_id: model_id.clone(),
            env_slug: canonical_env.clone(),
            last_seq: initial_last_seq,
        },
    );

    let mut sess_path = cloud_session_resource_path(&canonical_env, &sid);
    let has_repo = environment_spec.get("repo").is_some();
    let execution_opt = capture_cloud_turn_execution(
        &base,
        &canonical_env,
        &api_key,
        &gateway_key,
        repo_token.as_deref(),
        kubeconfig_b64.as_deref(),
        &sess_path,
        next_turn,
        has_repo,
    )?;
    let send_url = format!("{base}/v1/{sess_path}:send");
    let mut send_body = json!({
        "message": {
            "parts": [{ "text": user_message }]
        }
    });
    if let (Some(exec_val), Some(obj)) = (execution_opt, send_body.as_object_mut()) {
        obj.insert("execution".to_string(), exec_val);
    }
    let (mut st, mut raw) = curl_post_json(&send_url, Some(&api_key), &send_body)?;
    if matches!(st, 400 | 404 | 409 | 410)
        && (raw.contains("INVALID_STATE")
            || raw.contains("undelivered waits")
            || raw.contains("NOT_FOUND"))
    {
        let interrupt_old = format!("{base}/v1/{sess_path}:interrupt");
        let _ = curl_post_json(&interrupt_old, Some(&api_key), &json!({}));
        clear_persisted_cloud_session(config_dir);
        let create_sess_url = format!("{base}/v1/{canonical_env}/agent_sessions");
        let body = json!({
            "agent": aid,
            "title": "AnyAgents Session",
            "environment": environment_spec,
        });
        let (st_new, raw_new) = curl_post_json(&create_sess_url, Some(&api_key), &body)?;
        if (200..300).contains(&st_new)
            && let Ok(val_new) = serde_json::from_str::<Value>(&raw_new)
            && let Some(new_sid) = val_new
                .get("name")
                .or_else(|| val_new.get("id"))
                .or_else(|| val_new.get("uid"))
                .and_then(Value::as_str)
                .map(str::to_string)
        {
            sid = new_sid.clone();
            initial_last_seq = 0;
            next_turn = 1;
            {
                let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                g.cloud_session_id = Some(sid.clone());
                g.session_id = Some(sid.clone());
            }
            let _ = std::fs::write(config_dir.join("sylphx-session-id"), format!("{sid}\n"));
            save_persisted_cloud_session(
                config_dir,
                &PersistedCloudSession {
                    agent_id: aid.clone(),
                    session_id: sid.clone(),
                    model_id: model_id.clone(),
                    env_slug: canonical_env.clone(),
                    last_seq: 0,
                },
            );
            sess_path = cloud_session_resource_path(&canonical_env, &sid);
            let new_exec = capture_cloud_turn_execution(
                &base,
                &canonical_env,
                &api_key,
                &gateway_key,
                repo_token.as_deref(),
                kubeconfig_b64.as_deref(),
                &sess_path,
                1,
                has_repo,
            )?;
            let new_send_url = format!("{base}/v1/{sess_path}:send");
            let mut new_send_body = json!({
                "message": {
                    "parts": [{ "text": user_message }]
                }
            });
            if let (Some(exec_val), Some(obj)) = (new_exec, new_send_body.as_object_mut()) {
                obj.insert("execution".to_string(), exec_val);
            }
            (st, raw) = curl_post_json(&new_send_url, Some(&api_key), &new_send_body)?;
        }
    }
    if !(200..300).contains(&st) {
        return Err(format!("cloud session send HTTP {st}: {}", raw.trim()));
    }
    append_transcript_message(
        &transcript_path,
        &json!({
            "role": "user",
            "content": user_message,
        }),
    );

    let sess_url = format!("{base}/v1/{sess_path}");
    let events_url = format!("{base}/v1/{sess_path}/session_events");
    let submit_tool_url = format!("{base}/v1/{sess_path}:submitToolResult");
    let interrupt_url = format!("{base}/v1/{sess_path}:interrupt");
    let mut handled_call_ids = std::collections::HashSet::new();
    let mut last_seq: i64 = initial_last_seq;
    let baseline_seq: i64 = initial_last_seq;
    let mut sent_any_reply = false;
    let mut self_archived = false;
    let mut assistant_texts: Vec<String> = Vec::new();
    let mut turn_in_tok: u64 = 0;
    let mut turn_out_tok: u64 = 0;
    let mut turn_cache_read_tok: u64 = 0;
    let stall_timeout_ms = env
        .iter()
        .find(|(k, v)| k == "SYLPHX_CLOUD_SESSION_STALL_MS" && !v.trim().is_empty())
        .and_then(|(_, v)| v.trim().parse::<u64>().ok())
        .or_else(|| {
            std::env::var("SYLPHX_CLOUD_SESSION_STALL_MS")
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
        })
        .unwrap_or(180_000);
    let mut last_progress = Instant::now();

    while !cancel_flag.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(250));

        // Poll session_events for client tool calls and assistant messages
        let poll_events_url = if last_seq > 0 {
            format!("{events_url}?filter=sequence%20%3E%20{last_seq}")
        } else {
            events_url.clone()
        };
        if let Ok((200..=299, raw_events)) = curl_get(&poll_events_url, Some(&api_key))
            && let Ok(ev_doc) = serde_json::from_str::<Value>(&raw_events)
            && let Some(events) = ev_doc
                .get("session_events")
                .or_else(|| ev_doc.get("sessionEvents"))
                .or_else(|| ev_doc.get("events"))
                .and_then(Value::as_array)
        {
            for ev in events {
                let seq = ev
                    .get("sequence")
                    .and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                    })
                    .unwrap_or(0);
                if seq > 0 && seq <= last_seq {
                    continue;
                }
                if seq > last_seq {
                    last_seq = seq;
                    if seq > baseline_seq + 1 {
                        last_progress = Instant::now();
                    }
                }
                let ev_turn = ev
                    .get("turn")
                    .and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                    })
                    .unwrap_or(0);
                if ev_turn > 0 && ev_turn < next_turn {
                    continue;
                }

                let ev_type = ev
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_ascii_uppercase();

                if let Some(tc) = ev.get("tool_call").or_else(|| ev.get("toolCall")) {
                    last_progress = Instant::now();
                    let call_id = tc
                        .get("id")
                        .or_else(|| tc.get("call_id"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let tool_name = tc
                        .get("tool")
                        .or_else(|| tc.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let norm_fn = tool_name
                        .strip_prefix("mcp__anyagents__")
                        .unwrap_or(tool_name);
                    let is_client = is_cloud_client_coordination_tool(norm_fn)
                        && tc.get("client").and_then(Value::as_bool).unwrap_or(true);
                    if !call_id.is_empty() && is_client && handled_call_ids.insert(call_id.clone())
                    {
                        let input_val = tc
                            .get("input")
                            .or_else(|| tc.get("arguments"))
                            .cloned()
                            .unwrap_or_else(|| json!({}));
                        let parsed_args = if let Some(s) = input_val.as_str() {
                            serde_json::from_str::<Value>(s).unwrap_or_else(|_| json!({}))
                        } else {
                            input_val
                        };
                        let norm_fn = tool_name
                            .strip_prefix("mcp__anyagents__")
                            .unwrap_or(tool_name);
                        if norm_fn == "send" {
                            sent_any_reply = true;
                        }
                        if norm_fn == "archive"
                            && parsed_args
                                .get("agent")
                                .or_else(|| parsed_args.get("name"))
                                .and_then(Value::as_str)
                                .is_none_or(|s| {
                                    s.trim().is_empty() || s.eq_ignore_ascii_case("self")
                                })
                        {
                            self_archived = true;
                        }
                        append_transcript_message(
                            &transcript_path,
                            &json!({
                                "type": "function_call",
                                "call_id": call_id,
                                "name": tool_name,
                                "arguments": serde_json::to_string(&parsed_args).unwrap_or_else(|_| "{}".to_string()),
                            }),
                        );
                        let output = execute_sylphx_tool(
                            tool_name,
                            &parsed_args,
                            config_dir,
                            work_dir,
                            env,
                            mcp_command,
                            mcp_args,
                        );
                        append_transcript_message(
                            &transcript_path,
                            &json!({
                                "type": "function_call_output",
                                "call_id": call_id,
                                "output": output,
                            }),
                        );
                        let is_err = output.starts_with("error:");
                        let _ = curl_post_json(
                            &submit_tool_url,
                            Some(&api_key),
                            &json!({
                                "call_id": call_id,
                                "output": output,
                                "error": is_err,
                            }),
                        );
                        last_progress = Instant::now();
                    }
                } else if ev_type == "USAGE" {
                    let parse_u64 = |ptr: &str| -> u64 {
                        ev.pointer(ptr)
                            .and_then(|v| {
                                v.as_u64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                            })
                            .unwrap_or(0)
                    };
                    turn_in_tok = turn_in_tok.saturating_add(parse_u64("/usage/input_tokens"));
                    turn_out_tok = turn_out_tok.saturating_add(parse_u64("/usage/output_tokens"));
                    turn_cache_read_tok =
                        turn_cache_read_tok.saturating_add(parse_u64("/usage/cached_tokens"));
                } else if ev_type.contains("AGENT_MESSAGE") {
                    last_progress = Instant::now();
                    if let Some(parts) = ev.pointer("/message/parts").and_then(Value::as_array) {
                        for p in parts {
                            if let Some(t) = p
                                .get("text")
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                            {
                                assistant_texts.push(t.to_string());
                            }
                        }
                    }
                } else if ev_type.contains("RESPONSE") {
                    last_progress = Instant::now();
                    if ev.pointer("/response/is_error").and_then(Value::as_bool) == Some(true) {
                        let err_text = ev
                            .pointer("/response/text")
                            .and_then(Value::as_str)
                            .unwrap_or("cloud turn failed");
                        if err_text.contains("permanent") {
                            clear_persisted_cloud_session(config_dir);
                            let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                            g.cloud_agent_id = None;
                            g.cloud_session_id = None;
                        } else {
                            save_persisted_cloud_session(
                                config_dir,
                                &PersistedCloudSession {
                                    agent_id: aid.clone(),
                                    session_id: sid.clone(),
                                    model_id: model_id.clone(),
                                    env_slug: canonical_env.clone(),
                                    last_seq,
                                },
                            );
                        }
                        return Err(format!("cloud session turn error: {err_text}"));
                    }
                    if assistant_texts.is_empty()
                        && let Some(t) = ev
                            .pointer("/response/text")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                    {
                        assistant_texts.push(t.to_string());
                    }
                } else if ev_type == "ERROR" || ev_type == "TOOL_RESULT" || ev_type == "SANDBOX" {
                    last_progress = Instant::now();
                }
            }
        }

        if self_archived {
            break;
        }

        let (st_poll, raw_poll) = curl_get(&sess_url, Some(&api_key))?;
        if (200..300).contains(&st_poll)
            && let Ok(val) = serde_json::from_str::<Value>(&raw_poll)
        {
            let status = val
                .get("state")
                .or_else(|| val.get("status"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            if status == "idle" || status == "session_state_idle" || status == "completed" {
                let session_last_seq = val
                    .get("last_event_sequence")
                    .or_else(|| val.get("lastEventSequence"))
                    .and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                    })
                    .unwrap_or(0);
                if session_last_seq > last_seq {
                    continue;
                }
                save_persisted_cloud_session(
                    config_dir,
                    &PersistedCloudSession {
                        agent_id: aid.clone(),
                        session_id: sid.clone(),
                        model_id: model_id.clone(),
                        env_slug: canonical_env.clone(),
                        last_seq,
                    },
                );
                let content_text = assistant_texts.join("\n\n");
                if !content_text.trim().is_empty() {
                    append_transcript_message(
                        &transcript_path,
                        &json!({
                            "type": "message",
                            "role": "assistant",
                            "content": [{ "type": "output_text", "text": content_text.trim() }],
                        }),
                    );
                }
                if !sent_any_reply
                    && !content_text.trim().is_empty()
                    && let Some(reply_to) = extract_envelope_sender(user_message)
                {
                    let _ = call_anyagents_mcp_tool(
                        mcp_command,
                        mcp_args,
                        env,
                        "send",
                        &json!({
                            "to": reply_to,
                            "text": content_text.trim(),
                        }),
                    );
                }
                let in_tok = if turn_in_tok > 0 {
                    turn_in_tok
                } else {
                    val.get("total_input_tokens")
                        .or_else(|| val.pointer("/usage/input_tokens"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                };
                let out_tok = if turn_out_tok > 0 {
                    turn_out_tok
                } else {
                    val.get("total_output_tokens")
                        .or_else(|| val.pointer("/usage/output_tokens"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                };
                let cache_read_tok = if turn_cache_read_tok > 0 {
                    turn_cache_read_tok
                } else {
                    val.get("total_cache_read_tokens")
                        .or_else(|| val.pointer("/usage/input_tokens_details/cached_tokens"))
                        .or_else(|| val.pointer("/usage/cache_read_tokens"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                };
                let cache_write_tok = val
                    .get("total_cache_write_tokens")
                    .or_else(|| val.pointer("/usage/cache_write_tokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                return Ok(TurnUsage {
                    input_tokens: in_tok,
                    output_tokens: out_tok,
                    cache_read_tokens: cache_read_tok,
                    cache_write_tokens: cache_write_tok,
                    ..TurnUsage::default()
                });
            }
            if status == "failed"
                || status == "session_state_failed"
                || status == "terminated"
                || status == "errored"
            {
                clear_persisted_cloud_session(config_dir);
                return Err(format!("cloud session ended with status `{status}`"));
            }
            if last_seq <= baseline_seq + 2
                && assistant_texts.is_empty()
                && handled_call_ids.is_empty()
                && last_progress.elapsed() >= Duration::from_millis(stall_timeout_ms)
            {
                let _ = curl_post_json(&interrupt_url, Some(&api_key), &json!({}));
                return Err(format!(
                    "cloud session `{sess_path}` stalled in `{status}` for {}ms without progress",
                    stall_timeout_ms
                ));
            }
        }
    }
    if cancel_flag.load(Ordering::SeqCst) {
        let _ = curl_post_json(&interrupt_url, Some(&api_key), &json!({}));
    }
    save_persisted_cloud_session(
        config_dir,
        &PersistedCloudSession {
            agent_id: aid,
            session_id: sid,
            model_id,
            env_slug: canonical_env,
            last_seq,
        },
    );
    Ok(TurnUsage::default())
}

fn build_native_responses_request(
    model: &str,
    instructions: &str,
    input: &[Value],
    tools: &[Value],
    session_cache_key: &str,
    reasoning_effort: Option<&str>,
) -> Result<Value, String> {
    validate_responses_input(input)?;
    let mut body = json!({
        "model": model,
        "instructions": instructions,
        "input": input,
        "tools": tools,
        "prompt_cache_key": session_cache_key,
    });
    if let Some(effort) = reasoning_effort {
        body["reasoning"] = json!({ "effort": effort });
    }
    Ok(body)
}

fn native_responses_output(response: &Value) -> Result<Vec<Value>, String> {
    if response.get("error").is_none_or(Value::is_null)
        && let Some(output) = response.get("output").and_then(Value::as_array)
    {
        return Ok(output.clone());
    }
    // A successful HTTP transport is not proof of a Responses completion. Keep
    // enough envelope metadata to distinguish protocol/error responses without
    // logging model text, arguments, tool results or the full response body.
    let metadata = json!({
        "object": response.get("object").and_then(Value::as_str),
        "status": response.get("status").and_then(Value::as_str),
        "error_type": response.pointer("/error/type").and_then(Value::as_str),
        "error_code": response.pointer("/error/code").and_then(Value::as_str),
        "output_type": response.get("output").map(|v| match v {
            Value::Null => "null",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::String(_) => "string",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
        }),
    });
    Err(format!(
        "Sylphx responses API missing valid `output` array: envelope={metadata}"
    ))
}

const ATTACHED_IMAGE_MARKER_PREFIX: &str = "\n__ANYAGENTS_ATTACHED_IMAGE_DATA_URL__:";

fn image_mime_for_path(path: &str) -> Option<&'static str> {
    let lower = path.trim().to_ascii_lowercase();
    if lower.ends_with(".png") {
        Some("image/png")
    } else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        Some("image/jpeg")
    } else if lower.ends_with(".webp") {
        Some("image/webp")
    } else if lower.ends_with(".gif") {
        Some("image/gif")
    } else {
        None
    }
}

fn format_image_read_output(path: &str, mime: &str, bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return format!("error: image file `{path}` is empty (0 bytes)");
    }
    let b64 = auth::base64_encode(bytes);
    format!(
        "Loaded image `{path}` ({mime}, {} bytes) into visual context.{ATTACHED_IMAGE_MARKER_PREFIX}data:{mime};base64,{b64}",
        bytes.len()
    )
}

fn extract_attached_image_marker(output: &str) -> (String, Option<String>) {
    if let Some((clean, data_url)) = output.split_once(ATTACHED_IMAGE_MARKER_PREFIX) {
        let trimmed = data_url.trim();
        if trimmed.starts_with("data:image/") {
            return (clean.to_string(), Some(trimmed.to_string()));
        }
    }
    (output.to_string(), None)
}

fn close_dangling_function_calls(transcript_path: &Path, input_items: &mut Vec<Value>) {
    let mut answered = std::collections::HashSet::new();
    for item in input_items.iter() {
        if item.get("type").and_then(Value::as_str) == Some("function_call_output")
            && let Some(cid) = item.get("call_id").and_then(Value::as_str)
        {
            answered.insert(cid.to_string());
        }
    }
    let mut fixed = Vec::with_capacity(input_items.len());
    for item in input_items.drain(..) {
        let missing_cid = if item.get("type").and_then(Value::as_str) == Some("function_call") {
            item.get("call_id")
                .and_then(Value::as_str)
                .filter(|cid| !cid.is_empty() && !answered.contains(*cid))
                .map(ToString::to_string)
        } else {
            None
        };
        fixed.push(item);
        if let Some(cid) = missing_cid {
            answered.insert(cid.clone());
            let synthetic = json!({
                "type": "function_call_output",
                "call_id": cid,
                "output": "[tool execution interrupted before output was recorded]",
            });
            append_transcript_message(transcript_path, &synthetic);
            fixed.push(synthetic);
        }
    }
    *input_items = fixed;
}

/// Runs our native multi-step Sylphx Agent Runtime loop against Sylphx AI's
/// canonical Responses API (`https://api.sylphx.com/v1/responses`), executing
/// AnyAgents MCP tools (`agents`, `send`, `conversation`, `groups`, `schedule`,
/// `browser`, `describe`, `archive`) and workspace tools (`bash`, `read_file`,
/// `write_file`, `edit_file`) directly, and draining `steer_queue` between every
/// step so mid-turn steering takes effect immediately on the next step.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_native_gateway_turn(
    state: &Arc<Mutex<SharedState>>,
    cancel_flag: &Arc<AtomicBool>,
    config_dir: &Path,
    work_dir: &Path,
    model: Option<&str>,
    env: &[(String, String)],
    mcp_command: &Path,
    mcp_args: &[String],
    initial_user_message: &str,
) -> Result<TurnUsage, String> {
    let api_key = resolve_sylphx_gateway_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx API key; run `anyagents sign-in sylphx` first".to_string()
    })?;
    let gateway_base = env
        .iter()
        .find(|(k, _)| k == "SYLPHX_GATEWAY_URL")
        .map(|(_, v)| normalize_sylphx_gateway_url(v))
        .or_else(|| {
            std::env::var("SYLPHX_GATEWAY_URL")
                .ok()
                .map(|s| normalize_sylphx_gateway_url(&s))
        })
        .unwrap_or_else(|| DEFAULT_SYLPHX_GATEWAY_URL.to_string());
    let responses_url = format!("{gateway_base}/responses");

    let (model_id, reasoning_effort) = parse_sylphx_model_and_effort(model);
    let system_prompt = assemble_system_prompt(config_dir, work_dir);
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");

    let mut input_items = load_transcript_input_items(&transcript_path);
    close_dangling_function_calls(&transcript_path, &mut input_items);
    let stripped_body = match initial_user_message.strip_prefix("[anyagents") {
        Some(rest) => rest
            .find("]\n")
            .map(|idx| rest[idx + 2..].trim_start())
            .unwrap_or(initial_user_message),
        None => initial_user_message.trim_start(),
    };
    let is_recovery_on_unanswered_tail = stripped_body
        .starts_with("[Recovery] Your previous turn was interrupted")
        && input_items.last().is_some_and(|last| {
            last.get("role").and_then(Value::as_str) == Some("user")
                || last.get("type").and_then(Value::as_str) == Some("function_call_output")
        });
    let already_appended_by_cloud = is_recovery_on_unanswered_tail
        || input_items.last().is_some_and(|last| {
            last.get("role").and_then(Value::as_str) == Some("user")
                && last.get("content").and_then(Value::as_str) == Some(initial_user_message)
        });
    if !already_appended_by_cloud {
        let user_msg = json!({
            "role": "user",
            "content": initial_user_message,
        });
        append_transcript_message(&transcript_path, &user_msg);
        input_items.push(user_msg);
    }

    let tools = build_responses_tool_definitions();
    let session_cache_key = state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .session_id
        .clone()
        .unwrap_or_else(|| "sylphx_default_session".to_string());
    let usage_log_path = config_dir.join("sylphx-usage.jsonl");
    let mut total_in: u64 = 0;
    let mut total_out: u64 = 0;
    let mut total_cache_read: u64 = 0;
    let mut total_cache_write: u64 = 0;
    let mut sent_any_reply = false;

    let mut overflow_compacted_in_turn = false;
    for step in 0usize.. {
        if cancel_flag.load(Ordering::SeqCst) {
            break;
        }
        drain_steered_messages(state, &transcript_path, &mut input_items);
        if input_items.len() > memory::WINDOW_MAX_ITEMS
            || memory::estimate_items_tokens(&input_items) > memory::MAX_WINDOW_EST_TOKENS
        {
            input_items = load_transcript_input_items(&transcript_path);
            close_dangling_function_calls(&transcript_path, &mut input_items);
        }

        let req_body = build_native_responses_request(
            &model_id,
            &system_prompt,
            &input_items,
            &tools,
            &session_cache_key,
            reasoning_effort.as_deref(),
        )?;

        let step_t0 = Instant::now();
        let (status, raw) = curl_post_json(&responses_url, Some(&api_key), &req_body)?;
        let step_elapsed_ms = step_t0.elapsed().as_millis() as u64;
        if !(200..300).contains(&status) {
            let is_context_overflow = matches!(status, 400 | 413 | 503)
                && (raw.contains("context_length_exceeded")
                    || raw.contains("exceeds the model's context window")
                    || raw.contains("maximum context length"));
            if is_context_overflow && !overflow_compacted_in_turn {
                overflow_compacted_in_turn = true;
                let continuity = build_prior_transcript_continuity_block(&transcript_path)
                    .unwrap_or_else(|| "Prior turns preserved in raw transcript.".to_string());
                let handoff = format!(
                    "Auto-compacted after context window limit.\n{continuity}\nActive prompt:\n{}",
                    truncate_tool_output_middle_utf8(
                        initial_user_message.to_string(),
                        4_000,
                        1_800,
                        1_800
                    )
                );
                let anchor = json!({
                    "type": "context_window_open",
                    "handoff": handoff,
                    "timestamp_ms": anyagents_core::now_ms(),
                });
                append_transcript_message(&transcript_path, &anchor);
                input_items = load_transcript_input_items(&transcript_path);
                close_dangling_function_calls(&transcript_path, &mut input_items);
                continue;
            }
            return Err(format!(
                "Sylphx responses API HTTP {status}: {}",
                raw.trim()
            ));
        }
        let resp: Value = serde_json::from_str(&raw)
            .map_err(|e| format!("invalid Sylphx responses API JSON: {e}"))?;

        if let Some(usage) = resp.get("usage") {
            let step_in = usage
                .get("input_tokens")
                .or_else(|| usage.get("prompt_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let step_out = usage
                .get("output_tokens")
                .or_else(|| usage.get("completion_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let step_cache_read = usage
                .pointer("/input_tokens_details/cached_tokens")
                .or_else(|| usage.pointer("/prompt_tokens_details/cached_tokens"))
                .or_else(|| usage.get("cache_read_tokens"))
                .or_else(|| usage.get("cached_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let step_cache_write = usage
                .pointer("/input_tokens_details/cache_write_tokens")
                .or_else(|| usage.get("cache_write_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            total_in = total_in.saturating_add(step_in);
            total_out = total_out.saturating_add(step_out);
            total_cache_read = total_cache_read.saturating_add(step_cache_read);
            total_cache_write = total_cache_write.saturating_add(step_cache_write);
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&usage_log_path)
            {
                let rec = json!({
                    "step": step,
                    "input_tokens": step_in,
                    "output_tokens": step_out,
                    "cache_read_tokens": step_cache_read,
                    "cache_write_tokens": step_cache_write,
                    "elapsed_ms": step_elapsed_ms,
                });
                let _ = writeln!(f, "{rec}");
            }
        }

        if let Some(cb) = state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notify
            .clone()
        {
            cb();
        }

        let output_arr = native_responses_output(&resp)?;

        let mut function_calls = Vec::new();
        let mut assistant_texts = Vec::new();

        for item in output_arr {
            append_transcript_message(&transcript_path, &item);
            input_items.push(item.clone());
            match item.get("type").and_then(Value::as_str) {
                Some("function_call") => {
                    function_calls.push(item);
                }
                Some("message") => {
                    if let Some(parts) = item.get("content").and_then(Value::as_array) {
                        for p in parts {
                            if let Some(t) = p
                                .get("text")
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                            {
                                assistant_texts.push(t.to_string());
                            }
                        }
                    } else if let Some(t) = item
                        .get("content")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        assistant_texts.push(t.to_string());
                    }
                }
                _ => {}
            }
        }

        if !function_calls.is_empty() {
            let mut self_archived = false;
            let mut pending_window_handoff: Option<String> = None;
            let mut did_context_edit = false;
            let mut parsed_calls: Vec<(String, String, Value)> =
                Vec::with_capacity(function_calls.len());
            for fc in function_calls {
                let call_id = fc
                    .get("call_id")
                    .or_else(|| fc.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or("call_0")
                    .to_string();
                let fn_name = fc
                    .get("name")
                    .or_else(|| fc.pointer("/function/name"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let raw_args = fc
                    .get("arguments")
                    .or_else(|| fc.pointer("/function/arguments"))
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                let parsed_args: Value =
                    serde_json::from_str(raw_args).unwrap_or_else(|_| json!({}));
                let norm_fn = fn_name.strip_prefix("mcp__anyagents__").unwrap_or(&fn_name);
                if norm_fn == "send" {
                    sent_any_reply = true;
                }
                if matches!(norm_fn, "context_edit" | "context_prune") {
                    did_context_edit = true;
                }
                parsed_calls.push((call_id, fn_name, parsed_args));
            }

            if !cancel_flag.load(Ordering::SeqCst) && can_parallelize_tool_batch(&parsed_calls) {
                let outputs: Vec<String> = std::thread::scope(|s| {
                    let handles: Vec<_> = parsed_calls
                        .iter()
                        .map(|(_, fn_name, parsed_args)| {
                            s.spawn(move || {
                                execute_sylphx_tool(
                                    fn_name,
                                    parsed_args,
                                    config_dir,
                                    work_dir,
                                    env,
                                    mcp_command,
                                    mcp_args,
                                )
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|h| {
                            h.join().unwrap_or_else(|_| {
                                "error: tool worker thread panicked".to_string()
                            })
                        })
                        .collect()
                });
                for ((call_id, _, _), output) in parsed_calls.into_iter().zip(outputs) {
                    let tool_out_item = json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": output,
                    });
                    append_transcript_message(&transcript_path, &tool_out_item);
                    input_items.push(tool_out_item);
                }
            } else {
                for (call_id, fn_name, parsed_args) in parsed_calls {
                    if cancel_flag.load(Ordering::SeqCst) {
                        break;
                    }
                    let norm_fn = fn_name.strip_prefix("mcp__anyagents__").unwrap_or(&fn_name);
                    if norm_fn == "context_window_open"
                        && let Some(h) = parsed_args
                            .get("handoff")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                    {
                        pending_window_handoff = Some(h.to_string());
                    }
                    if (norm_fn == "archive"
                        && parsed_args
                            .get("agent")
                            .or_else(|| parsed_args.get("name"))
                            .and_then(Value::as_str)
                            .is_none_or(|s| s.trim().is_empty() || s.eq_ignore_ascii_case("self")))
                        || ((norm_fn == "agents" || norm_fn == "subagents")
                            && parsed_args
                                .get("action")
                                .and_then(Value::as_str)
                                .is_some_and(|a| a.eq_ignore_ascii_case("archive"))
                            && parsed_args
                                .get("name")
                                .or_else(|| parsed_args.get("agent"))
                                .and_then(Value::as_str)
                                .is_none_or(|s| {
                                    s.trim().is_empty() || s.eq_ignore_ascii_case("self")
                                }))
                    {
                        self_archived = true;
                        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                        g.served_usage.input_tokens =
                            g.served_usage.input_tokens.saturating_add(total_in);
                        g.served_usage.output_tokens =
                            g.served_usage.output_tokens.saturating_add(total_out);
                        g.served_usage.cache_read_tokens = g
                            .served_usage
                            .cache_read_tokens
                            .saturating_add(total_cache_read);
                        g.served_usage.cache_write_tokens = g
                            .served_usage
                            .cache_write_tokens
                            .saturating_add(total_cache_write);
                        total_in = 0;
                        total_out = 0;
                        total_cache_read = 0;
                        total_cache_write = 0;
                    }

                    let output = execute_sylphx_tool(
                        &fn_name,
                        &parsed_args,
                        config_dir,
                        work_dir,
                        env,
                        mcp_command,
                        mcp_args,
                    );
                    let (clean_output, attached_image_data_url) =
                        extract_attached_image_marker(&output);
                    let tool_out_item = json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": clean_output,
                    });
                    append_transcript_message(&transcript_path, &tool_out_item);
                    input_items.push(tool_out_item);
                    if let Some(data_url) = attached_image_data_url {
                        let image_item = json!({
                            "role": "user",
                            "content": [
                                {
                                    "type": "input_text",
                                    "text": format!("[Image loaded by `{fn_name}`]"),
                                },
                                {
                                    "type": "input_image",
                                    "image_url": data_url,
                                }
                            ]
                        });
                        append_transcript_message(&transcript_path, &image_item);
                        input_items.push(image_item);
                    }
                }
            }
            if let Some(handoff) = pending_window_handoff {
                let anchor = json!({
                    "type": "context_window_open",
                    "handoff": handoff,
                    "timestamp_ms": anyagents_core::now_ms(),
                });
                append_transcript_message(&transcript_path, &anchor);
                input_items = load_transcript_input_items(&transcript_path);
            } else if did_context_edit {
                input_items = load_transcript_input_items(&transcript_path);
            } else {
                maybe_emit_context_watermark_event(&transcript_path, &mut input_items);
            }
            if self_archived {
                break;
            }
            continue;
        }

        if drain_steered_messages(state, &transcript_path, &mut input_items) {
            continue;
        }

        if let Some(goal_continuation) = load_active_goal_continuation_prompt(config_dir) {
            let cont_msg = json!({
                "role": "user",
                "content": goal_continuation,
            });
            append_transcript_message(&transcript_path, &cont_msg);
            input_items.push(cont_msg);
            continue;
        }

        let content_text = assistant_texts.join("\n\n");
        if !sent_any_reply
            && !content_text.trim().is_empty()
            && let Some(reply_to) = extract_envelope_sender(initial_user_message)
        {
            let _ = call_anyagents_mcp_tool(
                mcp_command,
                mcp_args,
                env,
                "send",
                &json!({
                    "to": reply_to,
                    "text": content_text.trim(),
                }),
            );
        }
        break;
    }

    Ok(TurnUsage {
        input_tokens: total_in,
        output_tokens: total_out,
        cache_read_tokens: total_cache_read,
        cache_write_tokens: total_cache_write,
        ..TurnUsage::default()
    })
}

/// Runs `SylphxHarness` as a stdio stream adapter (`anyagents-harness harness <config>`)
/// when launched by an older `anyagents serve` process via `<config_dir>/adapter.json`.
pub fn run_stdio_adapter(
    config_path: &Path,
    config_val: &Value,
) -> Result<(), anyagents_core::Error> {
    let config_dir = config_val
        .get("config_dir")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .or_else(|| config_path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));
    let work_dir = std::env::current_dir().unwrap_or_else(|_| config_dir.clone());
    let model = config_val
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .and_then(|v| v.get("model").and_then(Value::as_str).map(str::to_string))
        });
    let mcp_command = clean_exe_path(
        &config_val
            .get("mcp_command")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .or_else(|| {
                std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
                    .ok()
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .and_then(|v| {
                        v.pointer("/mcpServers/anyagents/command")
                            .and_then(Value::as_str)
                            .map(PathBuf::from)
                    })
            })
            .unwrap_or_else(|| {
                std::env::current_exe().unwrap_or_else(|_| PathBuf::from("anyagents"))
            }),
    );
    let mcp_args: Vec<String> = config_val
        .get("mcp_args")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .or_else(|| {
            std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .and_then(|v| {
                    v.pointer("/mcpServers/anyagents/args")
                        .and_then(Value::as_array)
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|x| x.as_str().map(str::to_string))
                                .collect()
                        })
                })
        })
        .unwrap_or_default();

    let env: Vec<(String, String)> = std::env::vars().collect();
    let session_id = std::fs::read_to_string(config_dir.join("sylphx-session-id"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("sylphx_ses_{}", anyagents_core::now_ms()));
    let _ = std::fs::write(
        config_dir.join("sylphx-session-id"),
        format!("{session_id}\n"),
    );

    let state = Arc::new(Mutex::new(SharedState {
        session_id: Some(session_id.clone()),
        ..SharedState::default()
    }));
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let steered_in_turn = Arc::new(Mutex::new(0usize));

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else {
            break;
        };
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let kind = msg.get("type").and_then(Value::as_str).unwrap_or("");
        if kind == "cancel" || kind == "interrupt" {
            cancel_flag.store(true, Ordering::SeqCst);
            let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
            g.steer_queue.clear();
            g.in_turn = false;
            continue;
        }
        if kind != "user" && kind != "prompt" {
            continue;
        }
        let user_text = msg
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if user_text.trim().is_empty() {
            continue;
        }

        let already_in_turn = {
            let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
            if g.in_turn {
                g.steer_queue.push_back(user_text.clone());
                g.last_activity = Some(Instant::now());
                true
            } else {
                g.in_turn = true;
                g.last_activity = Some(Instant::now());
                false
            }
        };

        if already_in_turn {
            let mut sc = steered_in_turn
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            *sc += 1;
            continue;
        }

        cancel_flag.store(false, Ordering::SeqCst);
        *steered_in_turn
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = 0;

        let state_cl = Arc::clone(&state);
        let cancel_cl = Arc::clone(&cancel_flag);
        let steered_cl = Arc::clone(&steered_in_turn);
        let cfg_cl = config_dir.clone();
        let work_cl = work_dir.clone();
        let model_cl = model.clone();
        let env_cl = env.clone();
        let mcp_cmd_cl = mcp_command.clone();
        let mcp_args_cl = mcp_args.clone();
        let sess_cl = session_id.clone();

        std::thread::spawn(move || {
            let cloud_env = resolve_cloud_agent_environment(&cfg_cl, &env_cl);
            let outcome = if let Some(env_slug) = cloud_env {
                match run_cloud_session_turn(
                    &state_cl,
                    &cancel_cl,
                    &cfg_cl,
                    &work_cl,
                    model_cl.as_deref(),
                    &env_cl,
                    &mcp_cmd_cl,
                    &mcp_args_cl,
                    &env_slug,
                    &user_text,
                ) {
                    Ok(usage) => Ok(usage),
                    Err(cloud_err) => {
                        if !crate::sandbox::is_isolated_test_env() {
                            Err(cloud_err)
                        } else {
                            run_native_gateway_turn(
                                &state_cl,
                                &cancel_cl,
                                &cfg_cl,
                                &work_cl,
                                model_cl.as_deref(),
                                &env_cl,
                                &mcp_cmd_cl,
                                &mcp_args_cl,
                                &user_text,
                            )
                        }
                    }
                }
            } else {
                run_native_gateway_turn(
                    &state_cl,
                    &cancel_cl,
                    &cfg_cl,
                    &work_cl,
                    model_cl.as_deref(),
                    &env_cl,
                    &mcp_cmd_cl,
                    &mcp_args_cl,
                    &user_text,
                )
            };

            let extra_followups = {
                let mut g = state_cl.lock().unwrap_or_else(PoisonError::into_inner);
                g.in_turn = false;
                g.last_activity = Some(Instant::now());
                let mut sc = steered_cl.lock().unwrap_or_else(PoisonError::into_inner);
                let count = *sc;
                *sc = 0;
                count
            };

            let mut out = std::io::stdout().lock();
            match outcome {
                Ok(usage) => {
                    let _ = std::fs::remove_file(cfg_cl.join(".anyagents-last-error.json"));
                    let done_evt = json!({
                        "type": "done",
                        "session_id": sess_cl,
                        "usage": {
                            "input_tokens": usage.input_tokens,
                            "output_tokens": usage.output_tokens,
                            "cache_read_tokens": usage.cache_read_tokens,
                            "cache_write_tokens": usage.cache_write_tokens,
                        }
                    });
                    let _ = writeln!(out, "{done_evt}");
                    if extra_followups > 0 {
                        let followup_done = json!({
                            "type": "done",
                            "session_id": sess_cl,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                                "cache_read_tokens": 0,
                                "cache_write_tokens": 0,
                            }
                        });
                        let _ = writeln!(out, "{followup_done}");
                    }
                    let _ = out.flush();
                }
                Err(err_msg) => {
                    persist_turn_error(&cfg_cl, &err_msg);
                    let err_evt = json!({
                        "type": "done",
                        "session_id": sess_cl,
                        "error": err_msg,
                        "result": {
                            "status": "ERROR",
                            "error": err_msg,
                        },
                        "usage": {
                            "input_tokens": 0,
                            "output_tokens": 0,
                        }
                    });
                    let _ = writeln!(out, "{err_evt}");
                    if extra_followups > 0 {
                        let _ = writeln!(out, "{err_evt}");
                    }
                    let _ = out.flush();
                }
            }
        });
    }

    for _ in 0..2400 {
        let busy = state.lock().unwrap_or_else(PoisonError::into_inner).in_turn;
        if !busy {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn drain_steered_messages(
    state: &Arc<Mutex<SharedState>>,
    transcript_path: &Path,
    messages: &mut Vec<Value>,
) -> bool {
    let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
    if g.steer_queue.is_empty() {
        return false;
    }
    while let Some(steered) = g.steer_queue.pop_front() {
        let msg = json!({
            "role": "user",
            "content": steered,
        });
        append_transcript_message(transcript_path, &msg);
        messages.push(msg);
    }
    g.last_activity = Some(Instant::now());
    true
}

fn extract_envelope_sender(text: &str) -> Option<String> {
    let idx = text.find("from=\"")?;
    let rest = &text[idx + 6..];
    let end = rest.find('"')?;
    let sender = rest[..end].trim();
    (!sender.is_empty()).then(|| sender.to_string())
}

fn read_workspace_agents_md(work_dir: &Path) -> Option<String> {
    for dir in work_dir.ancestors() {
        let candidate = dir.join("AGENTS.md");
        if let Ok(text) = std::fs::read_to_string(&candidate)
            && !text.trim().is_empty()
        {
            return Some(text);
        }
        if dir.join(".git").exists() {
            break;
        }
    }
    None
}

fn read_shared_agents_md_for_config(config_dir: &Path) -> Option<String> {
    let mut candidates = vec![config_dir.join("AGENTS.md")];
    if let Some(data_dir) = config_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
    {
        candidates.push(data_dir.join("AGENTS.md"));
    }
    if let Some(parent) = config_dir.parent() {
        candidates.push(parent.join("AGENTS.md"));
    }
    for cand in candidates {
        if let Ok(text) = std::fs::read_to_string(&cand)
            && !text.trim().is_empty()
        {
            return Some(text);
        }
    }
    None
}

pub fn assemble_system_prompt(config_dir: &Path, work_dir: &Path) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Ok(sys) = std::fs::read_to_string(config_dir.join("SYSTEM_PROMPT.md"))
        && !sys.trim().is_empty()
    {
        parts.push(sys);
    }
    if let Some(shared_agents) = read_shared_agents_md_for_config(config_dir) {
        let trimmed = shared_agents.trim();
        if !trimmed.is_empty() && !parts.iter().any(|p| p.contains(trimmed)) {
            parts.push(shared_agents);
        }
    }
    if let Some(work_agents) = read_workspace_agents_md(work_dir) {
        let trimmed = work_agents.trim();
        if !trimmed.is_empty() && !parts.iter().any(|p| p.contains(trimmed)) {
            parts.push(work_agents);
        }
    }
    if parts.is_empty() {
        parts.push(
            "You are a Sylphx Agent running inside AnyAgents. Always use the `send` tool to reply to the sender of an incoming `<message>`.".to_string(),
        );
    }
    let skills_idx = discover_skills_index(work_dir, config_dir);
    if !skills_idx.is_empty() {
        parts.push(skills_idx);
    }
    parts.push(
        "<harness_architecture>\n\
         - Append-Only Event Stream: Incoming messages, steered updates, tool receipts, goal continuations, and watermark notices arrive as append-only events in the conversation stream so the prompt prefix stays 100% KV-cache pinned.\n\
         - Lossless Raw-RAG Recall (`memory_recall`): Every raw transcript item across the entire session (including before any `context_window_open` compaction boundary) is retained lossless on disk. Call `memory_recall` with `query` (hybrid BM25 + contiguous episode window expansion) or `range` (`transcript:<from>-<to>`) whenever you need earlier facts, exact command outputs, or causal chains.\n\
         - Autonomous Compaction (`context_budget_read` / `context_window_open`): When a `<context-event kind=\"context.watermark\">` event arrives or at a coherent milestone, call `context_window_open` with a structured `handoff` (`Goal`, `Verified Facts`, `Decisions`, `Next Action`) to start a clean window without losing raw history.\n\
         </harness_architecture>"
            .to_string(),
    );
    parts.join("\n\n")
}

fn discover_skills_dirs(work_dir: &Path, config_dir: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(custom) = std::env::var("ANYAGENTS_SKILLS_DIR") {
        let p = PathBuf::from(custom.trim());
        if p.is_dir() {
            dirs.push(p);
        }
    }
    for cand in [
        config_dir.join("skills"),
        config_dir.join(".agents/skills"),
        work_dir.join(".agents/skills"),
    ] {
        if cand.is_dir() && !dirs.contains(&cand) {
            dirs.push(cand);
        }
    }
    for ancestor in work_dir.ancestors() {
        let cand = ancestor.join(".agents/skills");
        if cand.is_dir() && !dirs.contains(&cand) {
            dirs.push(cand);
        }
    }
    if let Some(home) = crate::sandbox::host_home_dir() {
        for rel in [".agents/skills", ".gemini/antigravity-cli/builtin/skills"] {
            let p = home.join(rel);
            if p.is_dir() && !dirs.contains(&p) {
                dirs.push(p);
            }
        }
    }
    dirs
}

fn parse_skill_frontmatter(content: &str) -> Option<(String, String)> {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return None;
    }
    let rest = &trimmed[3..];
    let end = rest.find("\n---")?;
    let fm = &rest[..end];
    let mut name = None;
    let mut desc = None;
    for line in fm.lines() {
        if let Some(v) = line.strip_prefix("name:") {
            name = Some(v.trim().trim_matches('"').trim_matches('\'').to_string());
        } else if let Some(v) = line.strip_prefix("description:") {
            desc = Some(v.trim().trim_matches('"').trim_matches('\'').to_string());
        }
    }
    Some((name?, desc.unwrap_or_default()))
}

fn discover_skills_index(work_dir: &Path, config_dir: &Path) -> String {
    let mut seen = std::collections::BTreeMap::new();
    for dir in discover_skills_dirs(work_dir, config_dir) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let skill_md = entry.path().join("SKILL.md");
            if let Ok(raw) = std::fs::read_to_string(&skill_md)
                && let Some((name, desc)) = parse_skill_frontmatter(&raw)
            {
                seen.entry(name).or_insert(desc);
            }
        }
    }
    if seen.is_empty() {
        return String::new();
    }
    let mut out = String::from(
        "<skills>\nAvailable skills (call `activate_skill` with `name` to load full `SKILL.md` instructions and `references/*.md`):\n",
    );
    for (name, desc) in seen {
        let short = desc.split(". ").next().unwrap_or(&desc);
        let compact: String = short.chars().take(160).collect();
        out.push_str(&format!("- {name}: {compact}\n"));
    }
    out.push_str("</skills>");
    out
}

fn load_active_goal_continuation_prompt(config_dir: &Path) -> Option<String> {
    let goal_path = config_dir.join("sylphx-goal.json");
    let raw = std::fs::read_to_string(&goal_path).ok()?;
    let mut val: Value = serde_json::from_str(&raw).ok()?;
    if val.get("state").and_then(Value::as_str) != Some("active") {
        return None;
    }
    let continuations = val
        .get("auto_continuations")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if continuations >= 12 {
        return None;
    }
    if let Some(obj) = val.as_object_mut() {
        obj.insert("auto_continuations".to_string(), json!(continuations + 1));
    }
    let _ = std::fs::write(
        &goal_path,
        serde_json::to_string_pretty(&val).unwrap_or_default(),
    );
    Some(format!(
        "[Session Goal state] {}\nContinue executing toward the active Goal's acceptance criteria, or call `goal` with `action: \"complete\"` (or `\"pause\"` / `\"cancel\"`) once verified.",
        serde_json::to_string(&val).unwrap_or_default()
    ))
}

/// Converts `build_tool_definitions()` into the flat `/v1/responses` function tool schema
/// (`{"type": "function", "name": ..., "description": ..., "parameters": ...}`).
fn build_responses_tool_definitions() -> Vec<Value> {
    build_tool_definitions()
        .into_iter()
        .filter_map(|t| {
            let func = t.get("function")?;
            Some(json!({
                "type": "function",
                "name": func.get("name")?,
                "description": func.get("description").cloned().unwrap_or(Value::String(String::new())),
                "parameters": func.get("parameters").cloned().unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            }))
        })
        .collect()
}

fn build_tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "type": "function",
            "function": {
                "name": "send",
                "description": "Send message or perform interaction (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`) to agent(s), group(s), or channel(s).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "to": {
                            "description": "Recipient(s): string or array (`\"parent\"`, `agent:<name>`, bare name, `children`, `group:<name>`, `telegram:chat:<id>[:<topic>]`)."
                        },
                        "text": { "type": "string", "description": "Message text." },
                        "reply_to": { "type": "string", "description": "Reply reference (`anyagents:reply:msg_...`)." },
                        "action": { "type": "string", "description": "Interaction (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`)." },
                        "target": { "type": "string" },
                        "emoji": { "type": "string" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "describe",
                "description": "Read or update agent role/description (`agent`: `\"self\"` | `\"all\"` | `<name>`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string" },
                        "description": { "type": "string" },
                        "role": { "type": "string" },
                        "clear": { "type": "boolean" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "archive",
                "description": "Scale agent process to zero while preserving its permanent `agt_...` ID and session.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string", "description": "`\"self\"` (default) or sub-agent name." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "agents",
                "description": "Manage agents/sub-agents (`list`, `inspect`, `get`, `spawn`, `create`, `describe`, `steer`, `wake`, `archive`, `pause`, `delete`, `rename`, `role`, `harness`, `models`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string" },
                        "name": { "type": "string" },
                        "agent": { "type": "string" },
                        "parent": { "type": "string" },
                        "state": { "type": "string" },
                        "role": { "type": "string" },
                        "harness": { "type": "string" },
                        "model": { "type": "string" },
                        "profile": { "type": "string" },
                        "repo": { "type": "string" },
                        "branch": { "type": "string" },
                        "base": { "type": "string" },
                        "workdir": { "type": "string" },
                        "description": { "type": "string" },
                        "instructions": { "type": "string" },
                        "prompt": { "type": "string" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "conversation",
                "description": "Read or search (`query`) two-way (`in`+`out`) conversation history across agents, groups, and channels.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string" },
                        "query": { "type": "string" },
                        "with": { "type": "string" },
                        "limit": { "type": "integer" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "groups",
                "description": "Manage agent groups (`list`, `create`, `delete`, `add_member`, `remove_member`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string" },
                        "name": { "type": "string" },
                        "agent": { "type": "string" },
                        "members": { "type": "array", "items": { "type": "string" } }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "schedule",
                "description": "Schedule your own wakes (`at` or `every` with `prompt`) and external event watches (`watch` with `prompt`), or `list` and `cancel` (`id`: `<sch_...>` | `\"all\"`) your schedules. Supported `watch` specs: `github:pr:<owner>/<repo>#<num>`, `github:check:<owner>/<repo>@<ref>`, `spiron:promise:<id>`, `agent:idle:<name_or_id>`, `pid:<pid>`, `file:<path>`.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`set`, `list`, or `cancel`." },
                        "at": { "type": "string", "description": "One-off wake duration (`\"1m\"`, `\"30s\"`, `\"2h\"`) or ISO-8601 timestamp." },
                        "every": { "type": "string", "description": "Recurring wake interval (`\"1m\"`, `\"5m\"`, `\"15m\"`, `\"30m\"`, `\"1h\"`)." },
                        "watch": { "type": "string", "description": "Event watch spec (`github:pr:<owner>/<repo>#<num>`, `github:check:<owner>/<repo>@<ref>`, `spiron:promise:<id>`, `agent:idle:<name_or_id>`, `pid:<pid>`, `file:<path>`)." },
                        "prompt": { "type": "string" },
                        "id": { "type": "string" },
                        "agent": { "type": "string" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "browser",
                "description": "Drive Chromium (`goto`, `snapshot`, `click`, `fill`, `type`, `press`, `eval`, `text`, `html`, `pdf`, `screenshot`, `tabs`, `takeover`, `release_takeover`, `close`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string" },
                        "url": { "type": "string" },
                        "selector": { "type": "string" },
                        "value": { "type": "string" },
                        "script": { "type": "string" },
                        "path": { "type": "string" }
                    },
                    "required": ["action"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "code_mode",
                "description": "Programmatic Tool Calling (Python SDK: `read`, `write`, `edit`, `files`, `grep`, `sh`, `mcp`, `agents`, `send`, `conversation`, `schedule`, `recall`). Only `print(...)` enters context.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "code": { "type": "string" },
                        "timeout_ms": { "type": "integer" }
                    },
                    "required": ["code"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "bash",
                "description": "Execute bash command in workspace (Cloud Sandbox when active, automatic local fallback).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": { "type": "string" },
                        "timeout_ms": { "type": "integer" }
                    },
                    "required": ["command"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read UTF-8 file with 1-based line numbers (`[file: <path> (lines S..E of N)]`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "start_line": { "type": "integer" },
                        "end_line": { "type": "integer" }
                    },
                    "required": ["path"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "write_file",
                "description": "Write file in workspace (creates parent dirs).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "content": { "type": "string" }
                    },
                    "required": ["path", "content"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "edit_file",
                "description": "Replace `target` with `replacement` (or batch `edits: [{target, replacement}]`) in `path` with whitespace-tolerant matching.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "target": { "type": "string" },
                        "replacement": { "type": "string" },
                        "replace_all": { "type": "boolean" },
                        "edits": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "target": { "type": "string" },
                                    "replacement": { "type": "string" }
                                },
                                "required": ["target", "replacement"]
                            }
                        }
                    },
                    "required": ["path"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "apply_patch",
                "description": "Apply OpenAI V4A (`*** Begin Patch`) or unified diff (`--- a/... +++ b/...`) patch.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "patch": { "type": "string" }
                    },
                    "required": ["patch"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "grep",
                "description": "Regex/literal ripgrep search across workspace.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "pattern": { "type": "string" },
                        "path": { "type": "string" },
                        "glob": { "type": "string" }
                    },
                    "required": ["pattern"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "glob",
                "description": "Find files matching glob pattern.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "pattern": { "type": "string" },
                        "path": { "type": "string" }
                    },
                    "required": ["pattern"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "list_dir",
                "description": "List directory entries.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "activate_skill",
                "description": "Load `SKILL.md` (or `references/<reference>`) for a skill.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "reference": { "type": "string" }
                    },
                    "required": ["name"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "goal",
                "description": "Manage session Goal (`create`, `update`, `complete`, `pause`, `cancel`, `status`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string" },
                        "title": { "type": "string" },
                        "acceptance_criteria": { "type": "array", "items": { "type": "string" } },
                        "progress": { "type": "string" }
                    },
                    "required": ["action"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "memory_recall",
                "description": "Lossless Raw-RAG BM25 + range recall (`transcript:<from>-<to>`) across full append-only transcript (including pre-compaction turns).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": { "type": "string" },
                        "queries": { "type": "array", "items": { "type": "string" } },
                        "exclude": { "type": "array", "items": { "type": "string" } },
                        "include_tools": { "type": "boolean" },
                        "only_tools": { "type": "boolean" },
                        "tool_name": { "type": "string" },
                        "role": { "type": "string" },
                        "sort": { "type": "string", "enum": ["relevance", "newest", "oldest"] },
                        "range": { "type": "string" },
                        "limit": { "type": "integer" },
                        "radius": { "type": "integer" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "context_budget_read",
                "description": "Read active context window token budget and item counts.",
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "context_edit",
                "description": "Inspect, prune (`drop`/`keep_only` with automatic tool `call_id` pairing), or compress (`replace`) active context items while keeping 100% raw items on disk for `memory_recall`.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "enum": ["inspect", "edit"] },
                        "drop": { "type": "array", "items": { "type": "string" } },
                        "keep_only": { "type": "array", "items": { "type": "string" } },
                        "replace": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "range": { "type": "string" },
                                    "with": { "type": "string" }
                                },
                                "required": ["range", "with"]
                            }
                        }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "context_window_open",
                "description": "Compact active context at a milestone with a structured `handoff` (`Goal`, `Verified Facts`, `Decisions`, `Next Action`); prior raw turns stay searchable via `memory_recall`.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "handoff": { "type": "string" }
                    },
                    "required": ["handoff"]
                }
            }
        }),
    ]
}

fn execute_sylphx_tool(
    name: &str,
    args: &Value,
    config_dir: &Path,
    work_dir: &Path,
    env: &[(String, String)],
    mcp_command: &Path,
    mcp_args: &[String],
) -> String {
    let normalized = name.strip_prefix("mcp__anyagents__").unwrap_or(name);
    match normalized {
        "agents" | "subagents" | "send" | "conversation" | "search" | "groups" | "schedule"
        | "browser" | "describe" | "archive" | "wake" | "spawn" | "interact" => {
            call_anyagents_mcp_tool(mcp_command, mcp_args, env, normalized, args)
        }
        "memory_recall" | "mind_recall" => {
            execute_memory_recall(&config_dir.join("sylphx-transcript.jsonl"), args)
        }
        "context_budget_read" => {
            execute_context_budget_read(&config_dir.join("sylphx-transcript.jsonl"))
        }
        "context_edit" | "context_prune" => {
            execute_context_edit(&config_dir.join("sylphx-transcript.jsonl"), args)
        }
        "context_window_open" => {
            let Some(handoff) = args
                .get("handoff")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return "error: `context_window_open` requires a non-empty `handoff` string"
                    .to_string();
            };
            serde_json::to_string_pretty(&json!({
                "status": "window_opened",
                "handoff_chars": handoff.len(),
                "note": "New KV-cached context window anchored with `handoff`. All prior raw transcript turns remain searchable via `memory_recall`.",
            }))
            .unwrap_or_default()
        }
        "activate_skill" | "skill" => {
            let Some(skill_name) = args.get("name").and_then(Value::as_str).map(str::trim) else {
                return "error: missing `name`".to_string();
            };
            let ref_name = args
                .get("reference")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty());
            for dir in discover_skills_dirs(work_dir, config_dir) {
                let skill_dir = dir.join(skill_name);
                let skill_md = skill_dir.join("SKILL.md");
                if skill_md.is_file() {
                    if let Some(r) = ref_name {
                        let clean_r = r
                            .trim_start_matches("./")
                            .trim_start_matches("references/")
                            .trim_start_matches('/');
                        let ref_file = if clean_r.ends_with(".md") {
                            skill_dir.join("references").join(clean_r)
                        } else {
                            skill_dir.join("references").join(format!("{clean_r}.md"))
                        };
                        return std::fs::read_to_string(&ref_file).unwrap_or_else(|e| {
                            format!("error reading {}: {e}", ref_file.display())
                        });
                    }
                    let body = std::fs::read_to_string(&skill_md).unwrap_or_default();
                    let mut refs = Vec::new();
                    if let Ok(entries) = std::fs::read_dir(skill_dir.join("references")) {
                        for e in entries.flatten() {
                            if let Some(n) = e.file_name().to_str() {
                                refs.push(n.to_string());
                            }
                        }
                    }
                    refs.sort();
                    if refs.is_empty() {
                        return body;
                    }
                    return format!(
                        "{body}\n\nAvailable references (call `activate_skill` with `name: \"{skill_name}\"` and `reference`): {}",
                        refs.join(", ")
                    );
                }
            }
            format!("error: skill `{skill_name}` not found")
        }
        "goal" | "create_goal" | "update_goal" | "complete_goal" | "pause_goal" | "cancel_goal"
        | "get_goal" => {
            let action = match normalized {
                "create_goal" => "create",
                "update_goal" => "update",
                "complete_goal" => "complete",
                "pause_goal" => "pause",
                "cancel_goal" => "cancel",
                "get_goal" => "status",
                _ => args
                    .get("action")
                    .and_then(Value::as_str)
                    .unwrap_or("status")
                    .trim(),
            };
            let goal_path = config_dir.join("sylphx-goal.json");
            let mut current = std::fs::read_to_string(&goal_path)
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .unwrap_or_else(|| json!({ "state": "none" }));
            match action {
                "create" | "set" => {
                    let title = args.get("title").and_then(Value::as_str).unwrap_or("Goal");
                    let criteria = args
                        .get("acceptance_criteria")
                        .cloned()
                        .unwrap_or_else(|| json!([]));
                    current = json!({
                        "id": format!("goal_{}", anyagents_core::now_ms()),
                        "title": title,
                        "acceptance_criteria": criteria,
                        "state": "active",
                        "progress": args.get("progress").and_then(Value::as_str).unwrap_or(""),
                        "auto_continuations": 0,
                        "updated_at_ms": anyagents_core::now_ms(),
                    });
                    let _ = std::fs::write(
                        &goal_path,
                        serde_json::to_string_pretty(&current).unwrap_or_default(),
                    );
                }
                "update" => {
                    if let Some(obj) = current.as_object_mut() {
                        if let Some(t) = args.get("title").and_then(Value::as_str) {
                            obj.insert("title".to_string(), json!(t));
                        }
                        if let Some(c) = args.get("acceptance_criteria") {
                            obj.insert("acceptance_criteria".to_string(), c.clone());
                        }
                        if let Some(p) = args.get("progress").and_then(Value::as_str) {
                            obj.insert("progress".to_string(), json!(p));
                        }
                        obj.insert("auto_continuations".to_string(), json!(0));
                        obj.insert("updated_at_ms".to_string(), json!(anyagents_core::now_ms()));
                    }
                    let _ = std::fs::write(
                        &goal_path,
                        serde_json::to_string_pretty(&current).unwrap_or_default(),
                    );
                }
                "complete" | "pause" | "cancel" => {
                    let new_state = match action {
                        "complete" => "completed",
                        "pause" => "paused",
                        _ => "cancelled",
                    };
                    if let Some(obj) = current.as_object_mut() {
                        obj.insert("state".to_string(), json!(new_state));
                        if let Some(p) = args.get("progress").and_then(Value::as_str) {
                            obj.insert("progress".to_string(), json!(p));
                        }
                        obj.insert("updated_at_ms".to_string(), json!(anyagents_core::now_ms()));
                    }
                    let _ = std::fs::write(
                        &goal_path,
                        serde_json::to_string_pretty(&current).unwrap_or_default(),
                    );
                }
                _ => {}
            }
            serde_json::to_string_pretty(&current).unwrap_or_default()
        }
        "apply_patch" => {
            let Some(patch_str) = args.get("patch").and_then(Value::as_str) else {
                return "error: missing `patch`".to_string();
            };
            match apply_patch_universal(work_dir, patch_str, env) {
                Ok(msg) => msg,
                Err(e) => format!("error applying patch: {e}"),
            }
        }
        "grep" => {
            let Some(pat) = args.get("pattern").and_then(Value::as_str) else {
                return "error: missing `pattern`".to_string();
            };
            let path = args.get("path").and_then(Value::as_str).unwrap_or(".");
            let glob_arg = args
                .get("glob")
                .and_then(Value::as_str)
                .map(|g| format!(" --glob '{}'", g.replace('\'', "'\\''")))
                .unwrap_or_default();
            let escaped_pat = pat.replace('\'', "'\\''");
            let escaped_path = path.replace('\'', "'\\''");
            let script = format!(
                "rg -n --max-count 100{glob_arg} -- '{escaped_pat}' '{escaped_path}' | head -n 200"
            );
            execute_sylphx_tool(
                "bash",
                &json!({ "command": script }),
                config_dir,
                work_dir,
                env,
                mcp_command,
                mcp_args,
            )
        }
        "glob" => {
            let Some(pat) = args.get("pattern").and_then(Value::as_str) else {
                return "error: missing `pattern`".to_string();
            };
            let path = args.get("path").and_then(Value::as_str).unwrap_or(".");
            let escaped_pat = pat.replace('\'', "'\\''");
            let escaped_path = path.replace('\'', "'\\''");
            let script = format!("rg --files -g '{escaped_pat}' '{escaped_path}' | head -n 200");
            execute_sylphx_tool(
                "bash",
                &json!({ "command": script }),
                config_dir,
                work_dir,
                env,
                mcp_command,
                mcp_args,
            )
        }
        "list_dir" => {
            let path = args.get("path").and_then(Value::as_str).unwrap_or(".");
            if !workspace::delegated(env) {
                return list_dir_native(work_dir, path);
            }
            let escaped_path = path.replace('\'', "'\\''");
            let script = format!("ls -la '{escaped_path}' | head -n 200");
            execute_sylphx_tool(
                "bash",
                &json!({ "command": script }),
                config_dir,
                work_dir,
                env,
                mcp_command,
                mcp_args,
            )
        }
        "code_mode" => {
            let Some(code_str) = args.get("code").and_then(Value::as_str) else {
                return "error: missing `code`".to_string();
            };
            let timeout_ms = args
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(120_000);
            let wrapped = build_code_mode_command(code_str, config_dir, mcp_command, mcp_args);
            execute_sylphx_tool(
                "bash",
                &json!({ "command": wrapped, "timeout_ms": timeout_ms }),
                config_dir,
                work_dir,
                env,
                mcp_command,
                mcp_args,
            )
        }
        "bash" | "shell" => {
            let Some(cmd_str) = args.get("command").and_then(Value::as_str) else {
                return "error: missing `command`".to_string();
            };
            let timeout_ms = args
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(120_000);
            execute_bash_with_timeout(cmd_str, work_dir, env, timeout_ms)
        }
        "read_file" => {
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            let start_line = args.get("start_line").and_then(Value::as_u64);
            let end_line = args.get("end_line").and_then(Value::as_u64);
            let target = resolve_workspace_path(work_dir, rel);
            if let Some(mime) = image_mime_for_path(rel) {
                return match workspace::read_bytes(&target, env) {
                    Ok(bytes) => format_image_read_output(rel, mime, &bytes),
                    Err(e) => format!("error reading {}: {e}", target.display()),
                };
            }
            match workspace::read_to_string(&target, env) {
                Ok(content) => format_read_file_output(rel, &content, start_line, end_line),
                Err(e) => format!("error reading {}: {e}", target.display()),
            }
        }
        "write_file" => {
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            let Some(content) = args.get("content").and_then(Value::as_str) else {
                return "error: missing `content`".to_string();
            };
            let target = resolve_workspace_path(work_dir, rel);
            if let Some(p) = target.parent()
                && let Err(e) = workspace::create_dir_all(p, env)
            {
                return format!("error creating {}: {e}", p.display());
            }
            match workspace::write(&target, content, env) {
                Ok(()) => format!("wrote {} ({} bytes)", target.display(), content.len()),
                Err(e) => format!("error writing {}: {e}", target.display()),
            }
        }
        "edit_file" => {
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            let replace_all = args
                .get("replace_all")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mut edits_vec: Vec<(String, String)> = Vec::new();
            if let Some(arr) = args.get("edits").and_then(Value::as_array) {
                for item in arr {
                    let Some(t) = item.get("target").and_then(Value::as_str) else {
                        return "error: each entry in `edits` requires `target`".to_string();
                    };
                    let Some(r) = item.get("replacement").and_then(Value::as_str) else {
                        return "error: each entry in `edits` requires `replacement`".to_string();
                    };
                    edits_vec.push((t.to_string(), r.to_string()));
                }
            }
            if edits_vec.is_empty() {
                let Some(old_str) = args.get("target").and_then(Value::as_str) else {
                    return "error: missing `target` (or `edits` array)".to_string();
                };
                let Some(new_str) = args.get("replacement").and_then(Value::as_str) else {
                    return "error: missing `replacement`".to_string();
                };
                edits_vec.push((old_str.to_string(), new_str.to_string()));
            }
            let target = resolve_workspace_path(work_dir, rel);
            match workspace::read_to_string(&target, env) {
                Ok(existing) => match apply_edits_to_content(&existing, &edits_vec, replace_all) {
                    Ok((updated, ranges)) => match workspace::write(&target, updated, env) {
                        Ok(()) => {
                            let range_desc: Vec<String> =
                                ranges.iter().map(|(s, e)| format!("L{s}..L{e}")).collect();
                            format!("edited {} ({})", target.display(), range_desc.join(", "))
                        }
                        Err(e) => format!("error writing {}: {e}", target.display()),
                    },
                    Err(e) => format!("error editing {}: {e}", target.display()),
                },
                Err(e) => format!("error reading {}: {e}", target.display()),
            }
        }
        other => format!("error: unknown tool `{other}`"),
    }
}

fn resolve_workspace_path(work_dir: &Path, raw: &str) -> PathBuf {
    let p = Path::new(raw);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        work_dir.join(p)
    }
}

fn normalize_mcp_tool_arguments(tool_name: &str, arguments: &Value) -> Value {
    if tool_name != "schedule" {
        return arguments.clone();
    }
    let Some(mut obj) = arguments.as_object().cloned() else {
        return arguments.clone();
    };
    if obj.get("action").and_then(Value::as_str) == Some("create") {
        obj.insert("action".to_string(), json!("set"));
    }
    let kind = obj
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    if let Some(secs) = obj.get("seconds").and_then(Value::as_u64) {
        let dur_str = format!("{}s", secs.max(1));
        if kind == "every" || kind == "interval" {
            obj.entry("every".to_string())
                .or_insert_with(|| json!(dur_str));
        } else {
            obj.entry("at".to_string())
                .or_insert_with(|| json!(dur_str));
        }
    }
    if !obj.contains_key("watch")
        && let Some(wt) = obj
            .get("watch_target")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
    {
        let wk = obj
            .get("watch_kind")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let watch_spec = match wk {
            Some(k) if !wt.contains(':') => format!("{k}:{wt}"),
            _ => wt.to_string(),
        };
        obj.insert("watch".to_string(), json!(watch_spec));
    }
    Value::Object(obj)
}

fn call_anyagents_mcp_tool(
    mcp_command: &Path,
    mcp_args: &[String],
    env: &[(String, String)],
    tool_name: &str,
    arguments: &Value,
) -> String {
    let normalized_args = normalize_mcp_tool_arguments(tool_name, arguments);
    let clean_cmd = clean_exe_path(mcp_command);
    let mut cmd = Command::new(&clean_cmd);
    cmd.args(mcp_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Stderr is not part of the response. Do not let diagnostics fill a
        // pipe while we wait for the tool's stdout response.
        .stderr(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let Ok(mut child) = cmd.spawn() else {
        return format!("error: failed to spawn anyagents mcp for `{tool_name}`");
    };
    let init_req = json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "sylphx-harness", "version": "0.1.0" }
        }
    });
    let init_notif = json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    });
    let call_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": normalized_args,
        }
    });
    let mut stdin = child.stdin.take().expect("piped MCP stdin");
    let mut stdout = std::io::BufReader::new(child.stdout.take().expect("piped MCP stdout"));
    let response = (|| -> Result<Value, String> {
        for request in [init_req, init_notif, call_req] {
            writeln!(stdin, "{request}").map_err(|err| format!("writing MCP request: {err}"))?;
        }
        // EOF terminates rmcp's transport and cancels asynchronous tools. Keep
        // stdin open until our response arrives, ignoring init and notifications.
        for line in (&mut stdout).lines() {
            let line = line.map_err(|err| format!("reading MCP response: {err}"))?;
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && value.get("id").and_then(Value::as_i64) == Some(1)
            {
                return Ok(value);
            }
        }
        Err("MCP server closed stdout before tool response".to_string())
    })();
    drop(stdin);
    if response.is_err() {
        let _ = child.kill();
    }
    // Drain any trailing output after closing stdin so shutdown cannot block on
    // a full stdout pipe, then reap the server on success and failure alike.
    let _ = std::io::copy(&mut stdout, &mut std::io::sink());
    if child.wait().is_err() {
        return format!("error: failed waiting on anyagents mcp for `{tool_name}`");
    }
    let value = match response {
        Ok(value) => value,
        Err(err) => return format!("error: {err}"),
    };
    if let Some(err) = value.pointer("/error/message").and_then(Value::as_str) {
        return format!("error: {err}");
    }
    let output = value
        .pointer("/result/content/0/text")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string());
    truncate_tool_output_middle_utf8(output, 64_000, 30_000, 30_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_harness::Context;

    fn mcp_fixture_args(mode: &str) -> Vec<String> {
        vec![
            "-c".to_string(),
            include_str!("../tests/fixtures/mcp_stdio.py").to_string(),
            mode.to_string(),
        ]
    }

    #[test]
    fn mcp_stdio_waits_for_tool_response_before_closing_stdin() {
        for (mode, expected) in [
            ("success", r#"{"snapshot":"ready"}"#),
            ("error", "error: snapshot failed"),
        ] {
            let output = call_anyagents_mcp_tool(
                Path::new("python3"),
                &mcp_fixture_args(mode),
                &[],
                "browser",
                &json!({"action": "snapshot"}),
            );
            assert_eq!(output, expected, "mode: {mode}");
        }
    }

    #[test]
    fn mcp_stdio_rejects_initialize_without_tool_response() {
        let output = call_anyagents_mcp_tool(
            Path::new("python3"),
            &mcp_fixture_args("initialize_only"),
            &[],
            "browser",
            &json!({"action": "snapshot"}),
        );
        assert!(output.starts_with("error:"), "{output}");
        assert!(!output.contains("capabilities"), "{output}");
    }

    #[test]
    fn code_mode_mcp_stdio_observes_response_and_eof_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();
        for (mode, code, expected) in [
            (
                "success",
                "print(mcp('browser', action='snapshot')['snapshot'])",
                "ready",
            ),
            (
                "error",
                "try:\n    mcp('browser', action='snapshot')\nexcept RuntimeError as error:\n    print(error)",
                "snapshot failed",
            ),
            (
                "initialize_only",
                "try:\n    mcp('browser', action='snapshot')\nexcept RuntimeError as error:\n    print(error)",
                "MCP server closed stdout before tool response",
            ),
        ] {
            let command = build_code_mode_command(
                code,
                tmp.path(),
                Path::new("python3"),
                &mcp_fixture_args(mode),
            );
            let output = Command::new("sh").args(["-c", &command]).output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                String::from_utf8_lossy(&output.stdout).trim(),
                expected,
                "mode: {mode}"
            );
        }
    }

    #[test]
    fn write_sylphx_config_creates_valid_runtime_files() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        write_sylphx_config(
            cfg,
            Some("sylphx/openai/gpt-6.1-sol#xhigh"),
            Path::new("/usr/local/bin/anyagents"),
            &[
                "--data".to_string(),
                "/tmp/data".to_string(),
                "mcp".to_string(),
            ],
        )
        .unwrap();

        let agent_json: Value =
            serde_json::from_str(&std::fs::read_to_string(cfg.join("sylphx-agent.json")).unwrap())
                .unwrap();
        assert_eq!(agent_json["harness"], "sylphx");
        assert_eq!(agent_json["model"], "sylphx/openai/gpt-6.1-sol#xhigh");
        assert_eq!(
            agent_json["mcpServers"]["anyagents"]["command"],
            "/usr/local/bin/anyagents"
        );
    }

    #[test]
    fn save_and_resolve_sylphx_credentials_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let creds = SylphxCredentials {
            api_key: "sk-sylphx-test-123".to_string(),
            key_name: Some("anyagents".to_string()),
            org_id: Some("org_123".to_string()),
            org_slug: Some("sylphx".to_string()),
            api_url: Some("https://api.sylphx.com".to_string()),
            base_url: Some("https://api.sylphx.com".to_string()),
            store: Some("file".to_string()),
            orgs: Vec::new(),
            active_org: Some("org_123".to_string()),
        };
        save_sylphx_credentials(tmp.path(), &creds).unwrap();
        assert_eq!(
            read_sylphx_key_from_dir(tmp.path()).as_deref(),
            Some("sk-sylphx-test-123")
        );
        assert!(tmp.path().join(".config/opencode/sylphx.key").is_file());
        assert!(tmp.path().join(".config/sylphx/key").is_file());
    }

    #[test]
    fn sylphx_harness_fake_mode_delivers_and_steers_mid_turn() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("config");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();

        let mut harness = SylphxHarness::start(
            &cfg,
            &work,
            Some("sylphx/openai/gpt-6.1-sol#xhigh"),
            None,
            &[("ANYAGENTS_FAKE_MODE", "1")],
            Path::new("anyagents"),
            &[],
        )
        .unwrap();

        assert_eq!(
            SylphxHarness::declared_capabilities(),
            (DeliveryMode::Steer, SteerSupport::Yes)
        );
        assert!(harness.session_id().is_some());

        let first = OutboundMessage {
            id: "msg_1".to_string(),
            seq: 1,
            text: "sleep:1 please".to_string(),
            sender: Some("agent:vulcan".to_string()),
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        };
        assert_eq!(
            harness.deliver(&first).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );
        assert_eq!(harness.health().turn_state, TurnState::InTurn);

        let second = OutboundMessage {
            id: "msg_2".to_string(),
            seq: 2,
            text: "mid-turn steer message".to_string(),
            sender: Some("agent:vulcan".to_string()),
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        };
        assert_eq!(
            harness.deliver(&second).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );

        for _ in 0..40 {
            if harness.health().turn_state == TurnState::Idle {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert_eq!(harness.health().turn_state, TurnState::Idle);
        assert_eq!(harness.turns_served(), 1);
    }

    #[test]
    fn responses_api_tools_and_url_normalization_work() {
        assert_eq!(
            normalize_sylphx_gateway_url("https://api.models.sylphx.ai/v1"),
            "https://api.sylphx.com/v1"
        );
        assert_eq!(
            normalize_sylphx_gateway_url("https://api.sylphx.com/v1/"),
            "https://api.sylphx.com/v1"
        );
        let tools = build_responses_tool_definitions();
        assert!(!tools.is_empty());
        assert_eq!(tools[0]["type"], "function");
        assert!(tools[0]["name"].is_string());
        assert!(tools[0]["parameters"].is_object());
        assert!(tools[0].get("function").is_none());
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        assert!(names.contains(&"memory_recall"));
        assert!(names.contains(&"context_budget_read"));
        assert!(names.contains(&"context_window_open"));
    }

    #[test]
    fn system_prompt_stays_static_across_goal_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(cfg.join("SYSTEM_PROMPT.md"), "Static agent identity.").unwrap();

        let prompt_before = assemble_system_prompt(&cfg, &work);
        let _ = execute_sylphx_tool(
            "goal",
            &json!({
                "action": "create",
                "title": "Ship SOTA harness",
                "acceptance_criteria": ["100% KV cache prefix stability"]
            }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        let prompt_after = assemble_system_prompt(&cfg, &work);
        assert_eq!(prompt_before, prompt_after);
    }

    #[test]
    fn context_window_open_and_raw_rag_memory_recall_preserve_full_history() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        let transcript = cfg.join("sylphx-transcript.jsonl");

        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": "Deploy release v2.4.1 with token ALPHA_99182_KEY and 測試記憶檢索" }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call",
                "call_id": "call_1",
                "name": "bash",
                "arguments": "{\"command\":\"./deploy.sh --version v2.4.1\"}"
            }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "exit_code: 0\ndeployed commit sha 8f91b2c"
            }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "context_window_open",
                "handoff": "Goal: verify v2.4.1 rollout. Facts: deployed commit 8f91b2c.",
                "timestamp_ms": 1760000000000u64
            }),
        );
        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": "Now check production health" }),
        );

        // Active window only contains items from the latest `context_window_open` onward.
        let active = load_transcript_input_items(&transcript);
        assert_eq!(active.len(), 2);
        assert!(
            active[0]["content"]
                .as_str()
                .unwrap()
                .contains("context.window_opened")
        );
        assert_eq!(
            active[1]["content"].as_str(),
            Some("Now check production health")
        );

        // `memory_recall` still recalls pre-compaction items with contiguous episode context!
        let recall_raw = execute_memory_recall(
            &transcript,
            &json!({ "query": "ALPHA_99182_KEY 8f91b2c", "limit": 3, "radius": 2 }),
        );
        let recall_val: Value = serde_json::from_str(&recall_raw).unwrap();
        assert_eq!(recall_val["mode"], "query");
        assert_eq!(recall_val["total_raw_items"], 5);
        let hits = recall_val["hits"].as_array().unwrap();
        assert!(!hits.is_empty());
        let first_hit_str = serde_json::to_string(&hits[0]).unwrap();
        assert!(first_hit_str.contains("ALPHA_99182_KEY"));
        assert!(first_hit_str.contains("8f91b2c"));

        // CJK query recall works too!
        let cjk_raw = execute_memory_recall(&transcript, &json!({ "query": "記憶檢索" }));
        let cjk_val: Value = serde_json::from_str(&cjk_raw).unwrap();
        assert!(!cjk_val["hits"].as_array().unwrap().is_empty());

        // Range recall returns exact verbatim items.
        let range_raw = execute_memory_recall(&transcript, &json!({ "range": "transcript:0-2" }));
        let range_val: Value = serde_json::from_str(&range_raw).unwrap();
        assert_eq!(range_val["mode"], "range");
        assert_eq!(range_val["items"].as_array().unwrap().len(), 3);

        // Tool history filtering (`only_tools: true`, `tool_name: "bash"`) returns both the tool call and its linked output.
        let tools_raw = execute_memory_recall(
            &transcript,
            &json!({ "only_tools": true, "tool_name": "bash", "sort": "oldest" }),
        );
        let tools_val: Value = serde_json::from_str(&tools_raw).unwrap();
        let tool_hits = tools_val["hits"].as_array().unwrap();
        assert_eq!(tool_hits.len(), 2);
        assert_eq!(tool_hits[0]["tool_name"], "bash");
        assert_eq!(tool_hits[1]["tool_name"], "bash");

        // Excluding tool history (`include_tools: false`) searches only user/assistant messages.
        let msg_only_raw = execute_memory_recall(
            &transcript,
            &json!({ "queries": ["v2.4.1", "production"], "include_tools": false, "radius": 0 }),
        );
        let msg_only_val: Value = serde_json::from_str(&msg_only_raw).unwrap();
        let msg_only_str = serde_json::to_string(&msg_only_val["hits"]).unwrap();
        assert!(msg_only_str.contains("ALPHA_99182_KEY"));
        assert!(!msg_only_str.contains("[tool_call:bash]"));
    }

    #[test]
    fn missing_output_is_a_response_envelope_fault_not_input_pairing() {
        for response in [
            json!({"object": "error", "error": {"code": "upstream_fault", "type": "server_error", "message": "PRIVATE_RESPONSE"}}),
            json!({"status": "failed", "output": null}),
            json!({"output": "PRIVATE_RESPONSE"}),
            json!({"output": [], "error": {"code": "upstream_fault"}}),
        ] {
            let error = native_responses_output(&response).unwrap_err();
            assert!(error.contains("envelope="));
            assert!(!error.contains("PRIVATE_RESPONSE"));
        }
        assert!(
            native_responses_output(&json!({"output": []}))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            native_responses_output(&json!({"output": [{"type": "message", "content": "ok"}]}))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn block_aligned_stride_windowing_preserves_prefix_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("sylphx-transcript.jsonl");
        for i in 0..165 {
            append_transcript_message(
                &transcript,
                &json!({ "role": "user", "content": format!("msg_{i}") }),
            );
        }
        let w1 = load_transcript_input_items(&transcript);
        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": "msg_165" }),
        );
        let w2 = load_transcript_input_items(&transcript);
        // Item 0 must stay identical across consecutive turns between block strides!
        assert_eq!(w1[0], w2[0]);
        assert_eq!(w1[0]["content"], "msg_80");
    }

    #[test]
    fn cloud_sandbox_path_and_worktree_spec_resolution_work() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        std::fs::write(
            cfg.join(".anyagents-managed-worktree.json"),
            serde_json::to_string_pretty(&json!({
                "path": "/workspace/repository",
                "cloud_sandbox": true,
                "repo": "git@github.com:SylphxAI/anyagents.git",
                "branch": "agent/cloud-worker",
                "base": "0123456789abcdef0123456789abcdef01234567",
            }))
            .unwrap(),
        )
        .unwrap();

        write_sylphx_config(
            cfg,
            Some("sylphx/openai/gpt-6.1-sol#medium"),
            Path::new("anyagents"),
            &[],
        )
        .unwrap();

        let agent_json: Value =
            serde_json::from_str(&std::fs::read_to_string(cfg.join("sylphx-agent.json")).unwrap())
                .unwrap();
        assert_eq!(agent_json["branch"], "agent/cloud-worker");
        assert!(agent_json.get("sandbox").is_none());

        let spec = resolve_agent_git_worktree_spec(cfg, None, &[], None, None, None);
        assert_eq!(
            spec.repo_url.as_deref(),
            Some("https://github.com/SylphxAI/anyagents.git")
        );
        assert_eq!(spec.repo_slug.as_deref(), Some("SylphxAI/anyagents"));
        assert_eq!(spec.branch.as_deref(), Some("agent/cloud-worker"));
        assert_eq!(
            spec.base.as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );

        let env_spec = build_cloud_environment_spec(cfg, None, &[]);
        assert_eq!(env_spec["shape"], "linux-s");
        assert_eq!(env_spec["template"], "template:agent");
        assert!(env_spec.get("workspace_dir").is_none());
        assert_eq!(
            env_spec["repo"]["uri"],
            "https://github.com/SylphxAI/anyagents.git"
        );
        assert_eq!(
            env_spec["repo"]["revision"],
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert!(env_spec["repo"].get("slug").is_none());
        assert!(env_spec["repo"].get("branch").is_none());
        assert!(env_spec["repo"].get("base").is_none());
    }

    #[test]
    fn sota_aci_middle_truncation_fuzzy_edit_v4a_patch_and_parallel_batch_work() {
        // 1. UTF-8-safe Head + Tail middle truncation preserves both top header and bottom error summary
        let mut noisy = String::from("HEADER_START: cargo test\n");
        for i in 0..500 {
            noisy.push_str(&format!("compiling crate_{i} 測試多字節UTF8邊界安全...\n"));
        }
        noisy.push_str("FOOTER_FAILURE: test_critical_invariant FAILED at src/lib.rs:42\n");
        let truncated = truncate_tool_output_middle_utf8(noisy, 2_000, 700, 700);
        assert!(truncated.contains("HEADER_START: cargo test"));
        assert!(truncated.contains("FOOTER_FAILURE: test_critical_invariant FAILED"));
        assert!(truncated.contains("elided"));

        // 2. Line-numbered `read_file`, multi-edit + fuzzy indentation/line-number stripping in `edit_file`
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();

        let sample_rs = work.join("sample.rs");
        std::fs::write(
            &sample_rs,
            "fn alpha() {\n    let x = 1;   \n    let y = 2;\n}\n\nfn beta() {\n    let z = 10;\n}\n",
        )
        .unwrap();

        let read_out = execute_sylphx_tool(
            "read_file",
            &json!({ "path": "sample.rs" }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        assert!(read_out.contains("[file: sample.rs (lines 1..8 of 8)]"));
        assert!(read_out.contains("   1: fn alpha() {"));

        // Multi-edit: edit #1 has accidental copied line numbers (`2: ...`), edit #2 has different indentation
        let edit_out = execute_sylphx_tool(
            "edit_file",
            &json!({
                "path": "sample.rs",
                "edits": [
                    {
                        "target": "   2:     let x = 1;\n   3:     let y = 2;",
                        "replacement": "   2:     let x = 100;\n   3:     let y = 200;"
                    },
                    {
                        "target": "fn beta() {\n  let z = 10;\n}",
                        "replacement": "fn beta() {\n  let z = 999;\n}"
                    }
                ]
            }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        assert!(edit_out.starts_with("edited "), "unexpected: {edit_out}");
        let updated_rs = std::fs::read_to_string(&sample_rs).unwrap();
        assert!(updated_rs.contains("let x = 100;"));
        assert!(updated_rs.contains("let z = 999;"));

        // 3. Universal `apply_patch`: OpenAI V4A format (`*** Begin Patch`)
        let v4a_patch = "*** Begin Patch\n*** Update File: sample.rs\n@@ fn alpha @@\n fn alpha() {\n-    let x = 100;\n+    let x = 777;\n     let y = 200;\n*** Add File: added.txt\n+hello v4a\n*** End Patch";
        let patch_out = execute_sylphx_tool(
            "apply_patch",
            &json!({ "patch": v4a_patch }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        assert!(
            patch_out.contains("exit_code: 0"),
            "unexpected: {patch_out}"
        );
        assert!(
            std::fs::read_to_string(&sample_rs)
                .unwrap()
                .contains("let x = 777;")
        );
        assert_eq!(
            std::fs::read_to_string(work.join("added.txt")).unwrap(),
            "hello v4a\n"
        );

        // 4. Universal `apply_patch`: Unified diff with broken hunk header counts (`@@ -99,99 +99,99 @@`)
        let broken_unified = "--- a/sample.rs\n+++ b/sample.rs\n@@ -99,99 +99,99 @@\n fn beta() {\n-    let z = 999;\n+    let z = 42;\n }\n";
        let u_out = execute_sylphx_tool(
            "apply_patch",
            &json!({ "patch": broken_unified }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        assert!(u_out.contains("exit_code: 0"), "unexpected: {u_out}");
        assert!(
            std::fs::read_to_string(&sample_rs)
                .unwrap()
                .contains("let z = 42;")
        );

        // 5. Enforced `timeout_ms` on `bash`
        let t0 = Instant::now();
        let timeout_out = execute_sylphx_tool(
            "bash",
            &json!({ "command": "sleep 10", "timeout_ms": 500 }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        assert!(t0.elapsed() < Duration::from_secs(4));
        assert!(timeout_out.contains("timed out after 500ms"));

        // 6. Parallel tool batch classifier
        assert!(can_parallelize_tool_batch(&[
            (
                "c1".to_string(),
                "read_file".to_string(),
                json!({"path": "a.rs"})
            ),
            (
                "c2".to_string(),
                "read_file".to_string(),
                json!({"path": "b.rs"})
            ),
            (
                "c3".to_string(),
                "grep".to_string(),
                json!({"pattern": "fn"})
            ),
        ]));
        assert!(!can_parallelize_tool_batch(&[
            (
                "c1".to_string(),
                "edit_file".to_string(),
                json!({"path": "a.rs"})
            ),
            (
                "c2".to_string(),
                "edit_file".to_string(),
                json!({"path": "a.rs"})
            ),
        ]));

        // 7. Meta CLM `context_edit` (`arXiv:2609.37725`): inspect, in-place replace, and surgical drop
        //    with automatic `call_id` pair integrity while preserving 100% raw recall via `memory_recall`.
        let transcript = cfg.join("sylphx-transcript.jsonl");
        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": "Initial task prompt" }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call",
                "call_id": "call_deadend",
                "name": "grep",
                "arguments": "{\"pattern\":\"obsolete_symbol_xyz\"}"
            }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call_output",
                "call_id": "call_deadend",
                "output": "exit_code: 1\nno matches for obsolete_symbol_xyz"
            }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call",
                "call_id": "call_biglog",
                "name": "bash",
                "arguments": "{\"command\":\"cargo test\"}"
            }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call_output",
                "call_id": "call_biglog",
                "output": "exit_code: 0\n".to_string() + &"test line pass SECRET_BUILD_MARKER_777\n".repeat(100)
            }),
        );

        let inspect_raw = execute_sylphx_tool(
            "context_edit",
            &json!({ "action": "inspect" }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        let inspect_val: Value = serde_json::from_str(&inspect_raw).unwrap();
        assert_eq!(inspect_val["mode"], "inspect");
        assert_eq!(inspect_val["active_items"], 5);

        // Drop only index 1 (`call_deadend` function_call) -> index 2 (`call_deadend` output) is automatically paired and dropped too!
        // And replace index 4 (`call_biglog` huge output) with a 1-line note!
        let edit_ctx_raw = execute_sylphx_tool(
            "context_edit",
            &json!({
                "drop": ["1"],
                "replace": [{ "range": "4", "with": "cargo test 100/100 passed" }]
            }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        let edit_ctx_val: Value = serde_json::from_str(&edit_ctx_raw).unwrap();
        assert_eq!(edit_ctx_val["status"], "edited");
        assert_eq!(edit_ctx_val["active_items_before"], 5);
        assert_eq!(edit_ctx_val["active_items_after"], 3);
        assert!(edit_ctx_val["tokens_freed"].as_u64().unwrap() > 500);

        let active_after = load_transcript_input_items(&transcript);
        assert_eq!(active_after.len(), 3);
        let active_str = serde_json::to_string(&active_after).unwrap();
        assert!(!active_str.contains("call_deadend"));
        assert!(active_str.contains("cargo test 100/100 passed"));
        assert!(!active_str.contains("SECRET_BUILD_MARKER_777"));

        // Yet `memory_recall` still finds both `obsolete_symbol_xyz` and `SECRET_BUILD_MARKER_777` in the lossless raw transcript!
        let recall_dropped = execute_memory_recall(
            &transcript,
            &json!({ "query": "obsolete_symbol_xyz SECRET_BUILD_MARKER_777", "limit": 5 }),
        );
        assert!(recall_dropped.contains("obsolete_symbol_xyz"));
        assert!(recall_dropped.contains("SECRET_BUILD_MARKER_777"));

        // 8. `code_mode` (Programmatic Tool Calling / PTC) executes multi-step SDK operations in one call
        let cm_out = execute_sylphx_tool(
            "code_mode",
            &json!({
                "code": "src = read('sample.rs')\nedit('sample.rs', 'let z = 42;', 'let z = 888;')\nhits = recall('SECRET_BUILD_MARKER_777')\nprint(f'CODE_MODE_OK z_updated={\"888\" in read(\"sample.rs\")} hits={len(hits)}')"
            }),
            &cfg,
            &work,
            &[],
            Path::new("anyagents"),
            &[],
        );
        assert!(
            cm_out.contains("CODE_MODE_OK z_updated=True hits=1"),
            "unexpected: {cm_out}"
        );
    }

    #[test]
    fn assemble_system_prompt_layers_shared_and_workspace_agents_md_when_system_prompt_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let cfg = data_dir.join("agents/agt_test/config");
        let work = tmp.path().join("repo");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();

        std::fs::write(
            data_dir.join("AGENTS.md"),
            "## Two limits and operating defaults\nNo unauthorized spending and no user-data loss.\n",
        )
        .unwrap();
        std::fs::write(
            cfg.join("SYSTEM_PROMPT.md"),
            "# Agent Identity\nYou are a default Sylphx worker.\n",
        )
        .unwrap();
        std::fs::write(
            work.join("AGENTS.md"),
            "# SylphxAI/owner\nWorkspace repository instructions.\n",
        )
        .unwrap();

        let prompt = assemble_system_prompt(&cfg, &work);
        assert!(prompt.contains("# Agent Identity"));
        assert!(prompt.contains("Two limits and operating defaults"));
        assert!(prompt.contains("# SylphxAI/owner"));
        assert_eq!(
            prompt.matches("Two limits and operating defaults").count(),
            1
        );
    }

    #[test]
    fn transient_503_model_unavailable_retries_and_persistent_503_surfaces_fault() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;
        use std::sync::atomic::AtomicUsize;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_cl = Arc::clone(&hits);

        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf);
                let n = hits_cl.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    // 1st attempt: transient 503 model_unavailable
                    let body = r#"{"error":{"message":"The selected model is temporarily unavailable.","type":"server_error","code":"model_unavailable"}}"#;
                    let resp = format!(
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                } else if n == 1 {
                    // 2nd attempt (retry of turn 1): 200 OK
                    let body = r#"{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Recovered after retry"}]}],"usage":{"input_tokens":12,"output_tokens":6}}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                } else {
                    // Subsequent attempts (turn 2): persistent 503
                    let body = r#"{"error":{"message":"The selected model is temporarily unavailable.","type":"server_error","code":"model_unavailable"}}"#;
                    let resp = format!(
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
        });

        // SAFETY: test-local retry tuning
        unsafe {
            std::env::set_var("SYLPHX_RETRY_BACKOFF_MS", "5");
            std::env::set_var("SYLPHX_MAX_RETRIES", "2");
        }

        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        let gw_url = format!("http://{addr}/v1");

        let mut harness = SylphxHarness::start(
            &cfg,
            &work,
            Some("sylphx/openai/gpt-6.1-sol#medium"),
            None,
            &[
                ("SYLPHX_GATEWAY_KEY", "sk-sx-test"),
                ("SYLPHX_GATEWAY_URL", &gw_url),
            ],
            Path::new("anyagents"),
            &[],
        )
        .unwrap();

        // Turn 1: first request gets 503, retries automatically, and succeeds on 2nd request!
        let msg1 = OutboundMessage {
            id: "msg_retry_ok".to_string(),
            seq: 1,
            text: "hello retry".to_string(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        };
        harness.deliver(&msg1).unwrap();
        for _ in 0..80 {
            if harness.health().turn_state == TurnState::Idle {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(harness.turns_served(), 1);
        assert!(harness.health().sign_in_fault.is_none());
        assert!(!cfg.join(".anyagents-last-error.json").exists());

        // Turn 2: persistent 503 across all retry attempts -> surfaces `sign_in_fault` and `.anyagents-last-error.json` without advancing `turns_served`!
        let msg2 = OutboundMessage {
            id: "msg_fail".to_string(),
            seq: 2,
            text: "hello persistent 503".to_string(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        };
        harness.deliver(&msg2).unwrap();
        for _ in 0..80 {
            if harness.health().turn_state == TurnState::Idle {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(harness.turns_served(), 1);
        let fault = harness
            .health()
            .sign_in_fault
            .expect("expected sign_in_fault on persistent 503");
        assert!(fault.contains("503"), "unexpected fault: {fault}");
        assert!(
            fault.contains("model_unavailable"),
            "unexpected fault: {fault}"
        );
        assert!(cfg.join(".anyagents-last-error.json").exists());
    }

    #[test]
    fn cloud_agent_tools_separate_builtin_workspace_from_client_coordination_tools() {
        let client_tools = build_cloud_agent_client_tools();
        let client_names: Vec<&str> = client_tools
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        // Coordination & local takeover tools ARE bridged as client tools:
        for expected in [
            "send",
            "describe",
            "archive",
            "agents",
            "conversation",
            "groups",
            "schedule",
            "browser",
            "goal",
        ] {
            assert!(
                client_names.contains(&expected),
                "missing expected client coordination tool `{expected}`"
            );
        }
        // Workspace & local context tools are NEVER duplicated as client tools:
        for forbidden in [
            "bash",
            "read_file",
            "write_file",
            "edit_file",
            "apply_patch",
            "grep",
            "glob",
            "list_dir",
            "code_mode",
            "sandbox",
            "memory_recall",
            "context_budget_read",
            "context_edit",
            "context_window_open",
            "activate_skill",
        ] {
            assert!(
                !client_names.contains(&forbidden),
                "cloud client tools must not duplicate workspace/context tool `{forbidden}`"
            );
        }

        let all_cloud_tools = build_cloud_agent_tools();
        assert_eq!(all_cloud_tools.len(), client_tools.len());
        assert_eq!(all_cloud_tools[0]["client"]["timeout"], "600s");
    }

    // The fixture launcher never changes UID/groups or invokes setpriv. It only
    // verifies the trusted launch protocol and then execs as this test process.
    #[cfg(unix)]
    fn workspace_test_launcher(root: &Path) -> (Vec<(String, String)>, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let launcher = root.join("harmless-launcher");
        let log = root.join("launches");
        std::fs::write(
            &launcher,
            r#"#!/bin/bash
set -eu
[[ "$1" == --reuid && "$2" == fixture-uid ]]
[[ "$3" == --regid && "$4" == fixture-gid ]]
[[ "$5" == --groups && "$6" == fixture-groups ]]
[[ "$7" == --no-new-privs && "$8" == -- ]]
[[ -z ${ANYAGENTS_SEAT_LAUNCH+x} ]]
shift 8
printf '%s\n' "$1" >> "$WORKSPACE_TEST_LAUNCH_LOG"
if [[ ${WORKSPACE_TEST_REFUSE:-0} == 1 ]]; then
    echo 'fixture launcher refused' >&2
    exit 73
fi
exec "$@"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o700)).unwrap();
        (
            vec![
                (
                    anyagents_harness::SEAT_LAUNCH_ENV.into(),
                    format!(
                        "{}\tfixture-uid\tfixture-gid\tfixture-groups",
                        launcher.display()
                    ),
                ),
                (
                    "WORKSPACE_TEST_LAUNCH_LOG".into(),
                    log.display().to_string(),
                ),
            ],
            log,
        )
    }

    fn workspace_continuity_fixture(cfg: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        [
            (
                "sylphx-agent.json",
                "{\"model\":\"preserved\",\"profile\":\"ordinary\"}",
            ),
            ("sylphx-session-id", "same-session"),
            (
                "sylphx-transcript.jsonl",
                "accepted/UNKNOWN operation; never replay",
            ),
            (
                "sylphx-cloud-session.json",
                "{\"session_id\":\"retained-cloud\"}",
            ),
        ]
        .into_iter()
        .map(|(name, bytes)| {
            let path = cfg.join(name);
            std::fs::write(&path, bytes).unwrap();
            (path, bytes.as_bytes().to_vec())
        })
        .collect()
    }

    #[test]
    fn local_workspace_tools_use_worktree_and_inherited_uid_without_a_seat() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("selected worktree");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        let preserved = workspace_continuity_fixture(&cfg);
        let env = Vec::new();
        let tool = |name, args: Value| {
            execute_sylphx_tool(name, &args, &cfg, &work, &env, Path::new("unused-mcp"), &[])
        };
        let uid = Command::new("id").arg("-u").output().unwrap();
        let out = tool(
            "bash",
            json!({"command":"pwd -P; id -u; printf next-tool > 'created here'"}),
        );
        assert!(out.starts_with("exit_code: 0"), "{out}");
        assert!(out.contains(work.canonicalize().unwrap().to_str().unwrap()));
        assert!(out.contains(String::from_utf8_lossy(&uid.stdout).trim()));
        assert_eq!(
            std::fs::read_to_string(work.join("created here")).unwrap(),
            "next-tool"
        );
        assert!(tool("read_file", json!({"path":"created here"})).contains("next-tool"));
        for (path, bytes) in preserved {
            assert_eq!(std::fs::read(path).unwrap(), bytes);
        }
    }

    #[test]
    #[cfg(unix)]
    fn selected_local_workspace_tools_dispatch_through_trusted_launcher() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("selected worktree");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        let preserved = workspace_continuity_fixture(&cfg);
        let (env, log) = workspace_test_launcher(tmp.path());
        let tool = |name, args: Value| {
            execute_sylphx_tool(name, &args, &cfg, &work, &env, Path::new("unused-mcp"), &[])
        };
        let cases = [
            (
                "bash",
                json!({"command":"pwd -P; test -z \"${ANYAGENTS_SEAT_LAUNCH+x}\"; printf original > shell.txt"}),
                "exit_code: 0",
            ),
            (
                "write_file",
                json!({"path":"nested/' tricky $(touch forbidden).txt", "content":"alpha = 1\n測試\n"}),
                "wrote ",
            ),
            (
                "read_file",
                json!({"path":"nested/' tricky $(touch forbidden).txt", "start_line":2, "end_line":2}),
                "2: 測試",
            ),
            (
                "edit_file",
                json!({"path":"nested/' tricky $(touch forbidden).txt", "target":"alpha = 1", "replacement":"alpha = 2"}),
                "edited ",
            ),
            ("list_dir", json!({"path":"nested"}), "tricky"),
            (
                "code_mode",
                json!({"code":"write('code.txt', 'from code'); print(read('code.txt'))"}),
                "from code",
            ),
            (
                "apply_patch",
                json!({"patch":"*** Begin Patch\n*** Update File: shell.txt\n*** Move to: moved/shell.txt\n@@\n-original\n+updated\n*** Add File: added.txt\n+added\n*** End Patch"}),
                "exit_code: 0",
            ),
            (
                "apply_patch",
                json!({"patch":"*** Begin Patch\n*** Delete File: added.txt\n*** End Patch"}),
                "exit_code: 0",
            ),
            (
                "apply_patch",
                json!({"patch":"--- a/moved/shell.txt\n+++ b/moved/shell.txt\n@@ -1 +1 @@\n-updated\n+unified\n"}),
                "exit_code: 0",
            ),
            (
                "apply_patch",
                json!({"patch":"--- a/moved/shell.txt\n+++ b/moved/shell.txt\n@@ -99,99 +99,99 @@\n-unified\n+fallback\n"}),
                "exit_code: 0",
            ),
            // rg is not required on every supported test host; the launcher log
            // still proves search dispatch even when rg itself is unavailable.
            ("grep", json!({"pattern":"fallback"}), "exit_code:"),
            ("glob", json!({"pattern":"*.txt"}), "exit_code:"),
        ];
        let mut count = 0;
        for (name, args, expected) in cases {
            let out = tool(name, args);
            assert!(out.contains(expected), "{name}: {out}");
            let launches = std::fs::read_to_string(&log).unwrap();
            assert!(
                launches.lines().count() > count,
                "{name} bypassed trusted launcher"
            );
            count = launches.lines().count();
        }
        assert_eq!(
            std::fs::read_to_string(work.join("moved/shell.txt"))
                .unwrap()
                .trim(),
            "fallback"
        );
        assert!(!work.join("shell.txt").exists());
        assert!(!work.join("added.txt").exists());
        assert!(!work.join("forbidden").exists());
        assert!(
            std::fs::read_to_string(work.join("nested/' tricky $(touch forbidden).txt"))
                .unwrap()
                .starts_with("alpha = 2")
        );
        for (path, bytes) in preserved {
            assert_eq!(std::fs::read(path).unwrap(), bytes);
        }
    }

    #[test]
    #[cfg(unix)]
    fn failed_trusted_launcher_never_falls_back_or_mutates_continuity() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(work.join("retained"), "original").unwrap();
        let preserved = workspace_continuity_fixture(&cfg);
        let (mut env, log) = workspace_test_launcher(tmp.path());
        env.push(("WORKSPACE_TEST_REFUSE".into(), "1".into()));
        let tool = |name, args: Value, env: &[(String, String)]| {
            execute_sylphx_tool(name, &args, &cfg, &work, env, Path::new("unused-mcp"), &[])
        };
        for (name, args) in [
            ("bash", json!({"command":"printf forbidden > retained"})),
            ("read_file", json!({"path":"retained"})),
            (
                "write_file",
                json!({"path":"retained", "content":"forbidden"}),
            ),
            (
                "edit_file",
                json!({"path":"retained", "target":"original", "replacement":"forbidden"}),
            ),
            ("list_dir", json!({})),
            (
                "code_mode",
                json!({"code":"write('retained', 'forbidden')"}),
            ),
            (
                "apply_patch",
                json!({"patch":"*** Begin Patch\n*** Delete File: retained\n*** End Patch"}),
            ),
            (
                "apply_patch",
                json!({"patch":"--- a/retained\n+++ b/retained\n@@ -1 +1 @@\n-original\n+forbidden\n"}),
            ),
        ] {
            let out = tool(name, args, &env);
            assert!(
                out.starts_with("error") || out.starts_with("exit_code: 73"),
                "{name}: {out}"
            );
            assert!(
                !out.contains("1: original"),
                "failed launcher must not leak a server-side read"
            );
            assert_eq!(
                std::fs::read_to_string(work.join("retained")).unwrap(),
                "original"
            );
            for (path, bytes) in &preserved {
                assert_eq!(std::fs::read(path).unwrap(), *bytes);
            }
        }
        let count = std::fs::read(&log).unwrap();
        let mut unavailable = env.clone();
        unavailable
            .iter_mut()
            .find(|(k, _)| k == anyagents_harness::SEAT_LAUNCH_ENV)
            .unwrap()
            .1 = format!(
            "{}\tfixture-uid\tfixture-gid\tfixture-groups",
            tmp.path().join("no-launcher").display()
        );
        for (name, args) in [
            ("bash", json!({"command":"printf forbidden > retained"})),
            ("read_file", json!({"path":"retained"})),
            (
                "write_file",
                json!({"path":"retained", "content":"forbidden"}),
            ),
        ] {
            let out = tool(name, args, &unavailable);
            assert!(out.starts_with("error"), "{name}: {out}");
            assert_eq!(
                std::fs::read_to_string(work.join("retained")).unwrap(),
                "original"
            );
            for (path, bytes) in &preserved {
                assert_eq!(std::fs::read(path).unwrap(), *bytes);
            }
        }
        for malformed in [
            "",
            "bad\tspec",
            "\tfixture-uid\tfixture-gid\tfixture-groups",
        ] {
            let mut bad = env.clone();
            bad.iter_mut()
                .find(|(k, _)| k == anyagents_harness::SEAT_LAUNCH_ENV)
                .unwrap()
                .1 = malformed.into();
            let out = tool(
                "bash",
                json!({"command":"printf forbidden > retained"}),
                &bad,
            );
            assert!(out.contains("invalid trusted workspace launcher"), "{out}");
        }
        assert_eq!(std::fs::read(&log).unwrap(), count);
    }

    #[test]
    fn persisted_cloud_session_and_prior_transcript_continuity_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();

        let saved = PersistedCloudSession {
            agent_id: "environments/env_1/agents/agt_cloud_1".to_string(),
            session_id: "environments/env_1/agent_sessions/asess_cloud_1".to_string(),
            model_id: "openai/gpt-6.1-sol".to_string(),
            env_slug: "environments/env_1".to_string(),
            last_seq: 14,
        };
        save_persisted_cloud_session(cfg, &saved);
        let loaded = load_persisted_cloud_session(cfg).expect("persisted cloud session");
        assert_eq!(loaded.agent_id, saved.agent_id);
        assert_eq!(loaded.session_id, saved.session_id);
        assert_eq!(loaded.model_id, "openai/gpt-6.1-sol");
        assert_eq!(loaded.last_seq, 14);

        let transcript = cfg.join("sylphx-transcript.jsonl");
        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": "Check deployment status of service-alpha" }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call",
                "call_id": "call_99",
                "name": "bash",
                "arguments": "{\"command\":\"kubectl get pods\"}"
            }),
        );
        append_transcript_message(
            &transcript,
            &json!({
                "type": "message",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": "service-alpha is healthy (3/3 pods ready)." }]
            }),
        );

        let block = build_prior_transcript_continuity_block(&transcript)
            .expect("prior transcript continuity block");
        assert!(block.contains("<prior_session_continuity>"));
        assert!(block.contains("Check deployment status of service-alpha"));
        assert!(block.contains("[tool_call:bash]"));
        assert!(block.contains("service-alpha is healthy (3/3 pods ready)."));

        clear_persisted_cloud_session(cfg);
        assert!(load_persisted_cloud_session(cfg).is_none());

        let sdk_session = SylphxSdkSession::new(
            cfg,
            cfg,
            Some("sylphx/openai/gpt-6.1-sol#medium".to_string()),
            vec![("SYLPHX_EXECUTION_MODE".to_string(), "local".to_string())],
        );
        assert_eq!(sdk_session.execution_mode, SylphxExecutionMode::LocalOnly);
        assert!(NoopClientTools.tool_definitions().is_empty());
        assert!(NoopClientTools.execute("send", &json!({})).is_none());

        let receipt = sdk_session.pull_from_cloud().expect("pull from cloud");
        assert_eq!(receipt.direction, "cloud_to_local");
        assert_eq!(receipt.mode, "unchanged");
        assert!(!receipt.placement_changed);
        assert_eq!(receipt.transcript_items, 3);
    }
}
