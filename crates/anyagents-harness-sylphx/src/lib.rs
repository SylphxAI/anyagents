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
//!      `repo: { uri, revision }`) and executing entirely in the cloud with
//!      built-in shell and file tools.
//!    - **Native Sylphx AI + Sylphx Sandboxes mode**: Multi-step tool loop against
//!      `https://api.sylphx.com/v1/responses` with local or Cloud Kata microVM Sandbox
//!      worktrees (`cloud` submodule), KV-cache pinned append-only context windows,
//!      autonomous `context_window_open` compaction, and lossless `Raw-RAG` hybrid
//!      BM25 + episode recall (`memory` submodule).

mod contracts;
use contracts::*;
mod aci;
mod auth;
mod cloud;
#[cfg(test)]
mod masking_tests;
mod memory;
pub mod sdk;
mod workspace;

pub mod sandbox {
    use std::path::PathBuf;

    pub fn is_isolated_test_env() -> bool {
        isolated_test_env(cfg!(test))
    }

    fn isolated_test_env(test_build: bool) -> bool {
        test_build
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

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn claude_executable_override_does_not_select_test_isolation() {
            const PROBE: &str = "ANYAGENTS_ISOLATION_REGRESSION_PROBE";
            if let Some(expected) = std::env::var_os(PROBE) {
                // Unit binaries are test builds; exercise the exact production
                // predicate with that compile-time input false, without host reads.
                assert_eq!(isolated_test_env(false), expected == "true");
                assert!(is_isolated_test_env());
                return;
            }
            for (override_bin, explicit_test) in [(false, false), (true, false), (true, true)] {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                child
                    .env_clear()
                    .env(PROBE, explicit_test.to_string())
                    .args([
                        "--exact",
                        "sandbox::tests::claude_executable_override_does_not_select_test_isolation",
                        "--nocapture",
                    ]);
                if override_bin {
                    child.env("ANYAGENTS_CLAUDE_CODE_BIN", "/opt/vendor/claude");
                }
                if explicit_test {
                    child.env("ANYAGENTS_TESTS", "1");
                }
                let output = child.output().unwrap();
                assert!(
                    output.status.success(),
                    "override={override_bin}, explicit_test={explicit_test}: {}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
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
    SteerSupport, TurnState, TurnUsage, compose_message,
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
    env: workspace::Environment,
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
            env: env_owned.into(),
            mcp_command: clean_exe_path(mcp_command),
            mcp_args: mcp_args.to_vec(),
            fake_mode,
            fake_log,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            state,
        })
    }

    /// Supply the embedding host's authoritative peer inventory. Workspace
    /// commands refresh it immediately before admission, including mid-turn.
    pub fn set_seat_peers_resolver(
        &mut self,
        resolver: impl Fn() -> Result<String, String> + Send + Sync + 'static,
    ) {
        self.env.set_peer_resolver(Arc::new(resolver));
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

    fn cloud_environment(&self) -> Result<Option<String>, String> {
        resolve_cloud_agent_environment(&self.config_dir, &self.env)
    }
}

pub fn resolve_cloud_agent_environment(
    config_dir: &Path,
    env: &[(String, String)],
) -> Result<Option<String>, String> {
    let mode = sdk::SylphxExecutionMode::from_env(config_dir, env)?;
    if mode == sdk::SylphxExecutionMode::LocalOnly {
        return Ok(None);
    }
    // Validate selected inference settings even when placement is external-provider local.
    let settings = read_json(&config_dir.join("sylphx-agent.json"))?;
    if settings
        .as_ref()
        .and_then(|v| v.get("model"))
        .is_some_and(|v| !v.is_string())
    {
        return Err("invalid configured model".into());
    }
    if auth::is_external_chat_provider_model(None, config_dir) {
        return Ok(None);
    }
    let explicit = env
        .iter()
        .find(|(k, _)| k == "SYLPHX_AGENT_ENV")
        .map(|(_, v)| v.clone())
        .or_else(|| std::env::var("SYLPHX_AGENT_ENV").ok());
    if let Some(value) = explicit {
        return normalize_cloud_env_slug(&value).map(Some);
    }
    if let Some(persisted) = load_persisted_cloud_session(config_dir)? {
        return Ok(Some(persisted.env_slug));
    }
    if let Some(context) = read_json(&config_dir.join(".config/sylphx/context.json"))?
        && context.get("cloud_session").and_then(Value::as_bool) == Some(true)
    {
        let value = context
            .get("environment")
            .or_else(|| context.get("env"))
            .and_then(Value::as_str)
            .ok_or("cloud context missing environment")?;
        return normalize_cloud_env_slug(value).map(Some);
    }
    if mode == sdk::SylphxExecutionMode::CloudOnly {
        return Err(
            "cloud execution requires an explicit authorized environment (SYLPHX_AGENT_ENV)".into(),
        );
    }
    Ok(None)
}

pub(crate) fn is_recoverable_cloud_session_lease_error(err: &str) -> bool {
    err.contains("Paused sandbox lease could not be resumed")
        || err.contains("Pinned repository preparation failed")
        || err.contains("Sandbox lease is not ready")
        || err.contains("already been released")
        || err.contains("Existing sandbox lease could not be read")
}

fn compose_text(message: &OutboundMessage) -> String {
    let mut context = message.context.clone();
    if context.sender.is_none() {
        context.sender = message.sender.clone();
    }
    compose_message(&message.id, &context, &message.text)
}

impl Harness for SylphxHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let marked = compose_text(message);
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
                let cloud_sess = guard.cloud_session_id.clone();
                let notify = guard.notify.clone();
                if let Some(sess_id) = cloud_sess {
                    drop(guard);
                    let env_slug = self
                        .cloud_environment()
                        .map_err(HarnessError::Failed)?
                        .ok_or_else(|| {
                            HarnessError::Failed(
                                "cloud session has no environment; steer retained by caller".into(),
                            )
                        })?;
                    let api_key = resolve_sylphx_cloud_access_key(&self.config_dir, &self.env)
                        .ok_or_else(|| {
                            HarnessError::Failed("missing cloud steer credentials".into())
                        })?;
                    let base = resolve_cloud_api_url(&self.env);
                    let path = cloud_session_resource_path(&env_slug, &sess_id);
                    post_required(
                        &format!("{base}/v1/{path}:steer"),
                        &api_key,
                        &json!({"message": {"parts": [{"text": marked}]}}),
                    )
                    .map_err(HarnessError::Failed)?;
                } else {
                    guard.steer_queue.push_back(marked.clone());
                    guard.last_activity = Some(Instant::now());
                    drop(guard);
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
            let outcome = match cloud_env {
                Ok(Some(env_slug)) => run_cloud_session_turn(
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
                ),
                Err(e) => Err(e),
                Ok(None) => run_native_gateway_turn(
                    &state,
                    &cancel_flag,
                    &config_dir,
                    &work_dir,
                    model.as_deref(),
                    &env,
                    &mcp_command,
                    &mcp_args,
                    &marked,
                ),
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
                            let is_service_unavailable = lower.contains("http 500")
                                || lower.contains("http 502")
                                || lower.contains("http 503")
                                || lower.contains("http 504")
                                || lower.contains("service_unavailable")
                                || lower.contains("durable spend authorization is unavailable");
                            let non_retryable = !is_service_unavailable
                                && (lower.contains("\"retryable\":false")
                                    || lower.contains("\"retryable\": false"));
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
        let (cloud_sess, notify) = {
            let g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            (g.cloud_session_id.clone(), g.notify.clone())
        };
        if let Some(sess_id) = cloud_sess {
            let env_slug = self
                .cloud_environment()
                .map_err(HarnessError::Failed)?
                .ok_or_else(|| HarnessError::Failed("missing cloud environment".into()))?;
            let api_key =
                resolve_sylphx_cloud_access_key(&self.config_dir, &self.env).ok_or_else(|| {
                    HarnessError::Failed("missing cloud interrupt credentials".into())
                })?;
            interrupt_and_settle(
                &resolve_cloud_api_url(&self.env),
                &cloud_session_resource_path(&env_slug, &sess_id),
                &api_key,
            )
            .map_err(HarnessError::Failed)?;
        }
        self.cancel_flag.store(true, Ordering::SeqCst);
        {
            let mut g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            g.steer_queue.clear();
            g.in_turn = false;
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
        None
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
        let canonical_env = env_slug.to_string();
        format!("{canonical_env}/agent_sessions/{trimmed}")
    }
}

/// Cloud sessions execute exclusively with tools provided by the cloud runtime.
fn build_cloud_agent_tools() -> Vec<Value> {
    vec![
        json!({ "builtin": { "id": "shell" } }),
        json!({ "builtin": { "id": "files" } }),
    ]
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
        let (st, raw) = auth::post_cloud_json(&capture_url, Some(api_key), &body)?;
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
        let basic_auth = format!(
            "Basic {}",
            base64_encode(format!("x-access-token:{}", tok.trim()).as_bytes())
        );
        if has_repo
            && let Some(repo_handle) = capture_one("execution", &["repository"], &basic_auth)?
        {
            credentials.insert(
                "repository".to_string(),
                json!({
                    "handle": repo_handle,
                    "placement": "header:authorization"
                }),
            );
        }
        if let Ok(Some(gh_handle)) = capture_one("execution", &["sandbox", "GH_TOKEN"], tok.trim())
        {
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
    Ok(Some(json!({
        "turn": turn,
        "runtime_credential_handle": runtime_handle,
        "credentials": Value::Object(credentials),
    })))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct PersistedCloudSession {
    pub agent_id: String,
    pub session_id: String,
    pub model_id: String,
    pub env_slug: String,
    #[serde(default)]
    pub last_seq: i64,
}

pub fn load_persisted_cloud_session(
    config_dir: &Path,
) -> Result<Option<PersistedCloudSession>, String> {
    let Some(value) = read_json(&config_dir.join("sylphx-cloud-session.json"))? else {
        return Ok(None);
    };
    let parsed: PersistedCloudSession =
        serde_json::from_value(value).map_err(|e| format!("invalid cloud binding: {e}"))?;
    validate_cloud_binding(&parsed)?;
    Ok(Some(parsed))
}

fn validate_cloud_binding(binding: &PersistedCloudSession) -> Result<(), String> {
    if binding.agent_id.trim().is_empty()
        || binding.session_id.trim().is_empty()
        || binding.model_id.trim().is_empty()
        || binding.last_seq < 0
    {
        return Err("invalid cloud binding identity or cursor".into());
    }
    normalize_cloud_env_slug(&binding.env_slug)?;
    Ok(())
}

pub fn save_persisted_cloud_session(
    config_dir: &Path,
    session: &PersistedCloudSession,
) -> Result<(), String> {
    validate_cloud_binding(session)?;
    let raw = serde_json::to_vec_pretty(session).map_err(|e| e.to_string())?;
    write_atomic(&config_dir.join("sylphx-cloud-session.json"), &raw)
}

pub fn clear_persisted_cloud_session(config_dir: &Path) -> Result<(), String> {
    remove_optional(&config_dir.join("sylphx-cloud-session.json"))?;
    remove_optional(&config_dir.join(".sylphx-cloud-instructions-fingerprint"))
}

pub fn resolve_agent_short_name(config_dir: &Path, env: &[(String, String)]) -> String {
    if let Some(name) = env
        .iter()
        .find(|(k, v)| k == "ANYAGENTS_AGENT_NAME" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().to_string())
    {
        return name;
    }
    if let Ok(name) = std::env::var("ANYAGENTS_AGENT_NAME") {
        let trimmed = name.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Ok(sys) = std::fs::read_to_string(config_dir.join("SYSTEM_PROMPT.md")) {
        for line in sys.lines() {
            if let Some(rest) = line.trim().strip_prefix("- **Agent Name**: `")
                && let Some(end) = rest.find('`')
            {
                let parsed = rest[..end].trim();
                if !parsed.is_empty() {
                    return parsed.to_string();
                }
            }
        }
    }
    config_dir
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .unwrap_or("anyagents-sylphx")
        .to_string()
}

pub(crate) fn compute_instructions_fingerprint(
    agent_short_name: &str,
    system_prompt: &str,
) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in agent_short_name
        .as_bytes()
        .iter()
        .chain(std::iter::once(&0u8))
        .chain(system_prompt.as_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}:{:x}", system_prompt.len())
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
            if !trimmed.is_empty() && !trimmed.contains("kind=\"context.window_opened\"") {
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

/// Runs a turn entirely in the cloud, recording the final assistant output and usage.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_cloud_session_turn(
    state: &Arc<Mutex<SharedState>>,
    cancel_flag: &Arc<AtomicBool>,
    config_dir: &Path,
    work_dir: &Path,
    model: Option<&str>,
    env: &workspace::Environment,
    _mcp_command: &Path,
    _mcp_args: &[String],
    env_slug: &str,
    user_message: &str,
) -> Result<TurnUsage, String> {
    let api_key = resolve_sylphx_cloud_access_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx API key; run `anyagents sign-in sylphx` first".to_string()
    })?;
    let gateway_key =
        resolve_sylphx_gateway_key(config_dir, env).unwrap_or_else(|| api_key.clone());
    let repo_token = resolve_github_token(env);
    let base = resolve_cloud_api_url(env);
    let canonical_env = normalize_cloud_env_slug(env_slug)?;
    let (model_id, _) = parse_sylphx_model_and_effort(model);
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");
    memory::validate_transcript(&transcript_path)?;

    let (mut agent_id, mut session_id) = {
        let g = state.lock().unwrap_or_else(PoisonError::into_inner);
        (g.cloud_agent_id.clone(), g.cloud_session_id.clone())
    };
    // Retain legacy receipts for operator reconciliation without replaying host effects.
    reject_legacy_cloud_tool_receipt(config_dir)?;
    if reconcile_cloud_submission(config_dir, &base, &api_key, user_message)? {
        // The same uncertain submission has already settled remotely. Do not
        // send it again merely because its HTTP acknowledgement was lost.
        return Ok(TurnUsage::default());
    }
    let mut initial_last_seq: i64 = 0;
    let mut next_turn: i64 = 1;

    if let Some(persisted) = load_persisted_cloud_session(config_dir)? {
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
            interrupt_and_settle(&base, &old_sess_path, &api_key)?;
            clear_persisted_cloud_session(config_dir)?;
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
                let val: Value = serde_json::from_str(&raw_poll)
                    .map_err(|e| format!("invalid cloud session response: {e}"))?;
                {
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
                    let has_legacy_env = val
                        .pointer("/environment/idle_standby")
                        .and_then(Value::as_str)
                        .is_none_or(|s| s.is_empty())
                        || val.pointer("/environment/repo").is_some();
                    initial_last_seq = initial_last_seq.max(s_seq);
                    next_turn = tc.saturating_add(1);
                    if has_legacy_env
                        || status == "failed"
                        || status == "session_state_failed"
                        || status == "terminated"
                        || status == "errored"
                    {
                        if status.contains("running")
                            || status.contains("working")
                            || status.contains("waiting")
                            || status.contains("awaiting")
                        {
                            interrupt_and_settle(&base, &sess_path, &api_key)?;
                        }
                        clear_persisted_cloud_session(config_dir)?;
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
                        interrupt_and_settle(&base, &sess_path, &api_key)?;
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
                            return Err(
                                "remote turn has not settled; retaining cloud binding".into()
                            );
                        }
                    }
                }
            }
            Ok((404, _)) => {
                clear_persisted_cloud_session(config_dir)?;
                agent_id = None;
                session_id = None;
                initial_last_seq = 0;
                next_turn = 1;
            }
            Ok((status, _)) => {
                return Err(format!(
                    "cloud session read HTTP {status}; binding retained"
                ));
            }
            Err(error) => return Err(error),
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
    let agent_short_name = resolve_agent_short_name(config_dir, env);
    // Local identity, coordination instructions, skills, and tool history do not
    // belong to the cloud runtime. Only explicitly configured task context is sent.
    let initial_session_context = load_explicit_context(config_dir, work_dir)?;
    let environment_spec = build_cloud_environment_spec(config_dir, Some(work_dir), env);
    let system_prompt = format!(
        "You are a Sylphx Cloud Agent. Complete the user's task entirely in your cloud workspace using the built-in shell and file tools. Return your final answer as assistant text.\n\n\
<cloud_workspace>\nYou are running on a general-purpose Linux computer (workspace `{CLOUD_WORKSPACE_DIR}`) whose files persist across turns.\n\
Standard tools (`python3`, `node`, `git`, `gh`, `sylphx`, `curl`) are pre-installed and pre-authenticated.\n\
Clone any Git repository you need directly (`git clone` / `gh repo clone`).\n\
The VM is lightweight (`1 vCPU / 2 GiB RAM`); whenever you need to run a CPU- or memory-intensive command, execute it on an on-demand 16-vCPU / 32-GiB remote machine and sync output files back via:\n\
`sylphx build run --size large [--artifact \"<glob>\" --out .] -- <command>`\n\
</cloud_workspace>"
    );

    let current_fp = compute_instructions_fingerprint(&agent_short_name, &system_prompt);
    let fp_path = config_dir.join(".sylphx-cloud-instructions-fingerprint");
    let previous_fp = read_optional(&fp_path)?;
    if session_id.is_some() && previous_fp.as_deref().map(str::trim) != Some(current_fp.as_str()) {
        if let Some(ref sid) = session_id {
            let old_sess_path = cloud_session_resource_path(&canonical_env, sid);
            interrupt_and_settle(&base, &old_sess_path, &api_key)?;
        }
        clear_persisted_cloud_session(config_dir)?;
        agent_id = None;
        session_id = None;
        initial_last_seq = 0;
        next_turn = 1;
        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
        g.cloud_agent_id = None;
        g.cloud_session_id = None;
    }
    write_atomic(&fp_path, format!("{current_fp}\n").as_bytes())?;

    if agent_id.is_none() {
        let create_agent_url = format!("{base}/v1/{canonical_env}/agents");
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
        let (st, raw) = auth::post_cloud_json(&create_agent_url, Some(&api_key), &spec_body)?;
        if !(200..300).contains(&st) {
            return Err(format!("cloud create_agent HTTP {st}: {}", raw.trim()));
        }
        let val: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        let id = val
            .get("name")
            .or_else(|| val.get("id"))
            .or_else(|| val.get("uid"))
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
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
        let agent_ref = agent_id
            .as_deref()
            .ok_or("cloud creation missing agent identity")?;
        let body = json!({
            "agent": agent_ref,
            "title": "Sylphx Cloud Session",
            "environment": environment_spec,
        });
        let (st, raw) = auth::post_cloud_json(&create_sess_url, Some(&api_key), &body)?;
        if !(200..300).contains(&st) {
            return Err(format!("cloud create_session HTTP {st}: {}", raw.trim()));
        }
        let val: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        let sid = val
            .get("name")
            .or_else(|| val.get("id"))
            .or_else(|| val.get("uid"))
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| "cloud create_session missing name/id".to_string())?
            .to_string();
        session_id = Some(sid.clone());
        initial_last_seq = 0;
        next_turn = 1;
        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
        g.cloud_session_id = Some(sid.clone());
        g.session_id = Some(sid.clone());
        write_atomic(
            &config_dir.join("sylphx-session-id"),
            format!("{sid}\n").as_bytes(),
        )?;
    }

    let aid = agent_id.ok_or("cloud creation missing agent identity")?;
    let sid = session_id.ok_or("cloud creation missing session identity")?;
    save_persisted_cloud_session(
        config_dir,
        &PersistedCloudSession {
            agent_id: aid.clone(),
            session_id: sid.clone(),
            model_id: model_id.clone(),
            env_slug: canonical_env.clone(),
            last_seq: initial_last_seq,
        },
    )?;

    let sess_path = cloud_session_resource_path(&canonical_env, &sid);
    let has_repo = environment_spec.get("repo").is_some();
    let execution_opt = capture_cloud_turn_execution(
        &base,
        &canonical_env,
        &api_key,
        &gateway_key,
        repo_token.as_deref(),
        &sess_path,
        next_turn,
        has_repo,
    )?;
    let send_url = format!("{base}/v1/{sess_path}:send");
    let first_turn_text = if next_turn == 1 && !initial_session_context.is_empty() {
        format!("{}\n\n{user_message}", initial_session_context.trim())
    } else {
        user_message.to_string()
    };
    let mut send_body = json!({
        "message": {
            "parts": [{ "text": first_turn_text }]
        }
    });
    if let (Some(exec_val), Some(obj)) = (execution_opt, send_body.as_object_mut()) {
        obj.insert("execution".to_string(), exec_val);
    }
    append_transcript_message(
        &transcript_path,
        &json!({"role":"user", "content":user_message}),
    )?;
    let pending_cloud_turn = config_dir.join("sylphx-cloud-turn-pending.json");
    write_atomic(
        &pending_cloud_turn,
        json!({"session_path":sess_path, "message":user_message, "state":"submitting"})
            .to_string()
            .as_bytes(),
    )?;
    let (st, raw) = auth::post_cloud_json(&send_url, Some(&api_key), &send_body)?;
    if !(200..300).contains(&st) {
        return Err(format!(
            "cloud session send HTTP {st}; submission retained for reconciliation: {}",
            raw.trim()
        ));
    }
    write_atomic(
        &pending_cloud_turn,
        json!({"session_path":sess_path, "message":user_message, "state":"accepted"})
            .to_string()
            .as_bytes(),
    )?;

    let sess_url = format!("{base}/v1/{sess_path}");
    let events_url = format!("{base}/v1/{sess_path}/session_events");
    let mut last_seq: i64 = initial_last_seq;
    let baseline_seq: i64 = initial_last_seq;
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
    let mut turn_completed = false;

    while !cancel_flag.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1_200));

        // Observe cloud progress and output; tool execution stays in the cloud runtime.
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
                let seq = sequence(ev.get("sequence"), false)?;
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
                    .to_ascii_uppercase()
                    .replace('.', "_");

                if matches!(
                    ev_type.as_str(),
                    "SESSION_IDLE" | "TURN_COMPLETED" | "SESSION_COMPLETED"
                ) {
                    turn_completed = true;
                } else if ev.get("tool_call").or_else(|| ev.get("toolCall")).is_some() {
                    last_progress = Instant::now();
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
                        if err_text.contains("permanent")
                            || is_recoverable_cloud_session_lease_error(err_text)
                        {
                            clear_persisted_cloud_session(config_dir)?;
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
                            )?;
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
                    let err_candidate = if ev_type == "TOOL_RESULT" {
                        if ev.pointer("/tool_result/error").and_then(Value::as_bool) == Some(true) {
                            ev.pointer("/tool_result/output/error/message")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        } else {
                            None
                        }
                    } else {
                        Some(serde_json::to_string(ev).unwrap_or_default())
                    };
                    if let Some(ev_str) = err_candidate {
                        if !is_recoverable_cloud_session_lease_error(&ev_str) {
                            if ev_type == "ERROR" {
                                return Err(format!("cloud session turn error: {ev_str}"));
                            }
                            continue;
                        }
                        if ev_str.contains("Pinned repository preparation failed") {
                            let _ = std::fs::write(
                                config_dir.join(".anyagents-skip-pinned-repo"),
                                "1\n",
                            );
                        }
                        interrupt_and_settle(&base, &sess_path, &api_key)?;
                        clear_persisted_cloud_session(config_dir)?;
                        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                        g.cloud_agent_id = None;
                        g.cloud_session_id = None;
                        return Err(format!("cloud session turn error: {ev_str}"));
                    }
                }
            }
        }

        let session = curl_get(&sess_url, Some(&api_key))
            .ok()
            .filter(|(status, _)| (200..300).contains(status))
            .and_then(|(_, raw)| serde_json::from_str::<Value>(&raw).ok());
        if turn_completed || session.is_some() {
            let val = session.unwrap_or_else(|| json!({}));
            let status = val
                .get("state")
                .or_else(|| val.get("status"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            if matches!(
                status.as_str(),
                "failed" | "session_state_failed" | "terminated" | "errored"
            ) {
                clear_persisted_cloud_session(config_dir)?;
                return Err(format!("cloud session ended with status `{status}`"));
            }
            if turn_completed
                || matches!(status.as_str(), "idle" | "session_state_idle" | "completed")
            {
                let session_sequence = val
                    .get("last_event_sequence")
                    .or_else(|| val.get("lastEventSequence"));
                let session_last_seq = if turn_completed && session_sequence.is_none() {
                    last_seq
                } else {
                    sequence(session_sequence, true)?
                };
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
                )?;
                let content_text = assistant_texts.join("\n\n");
                if !content_text.trim().is_empty() {
                    append_transcript_message(
                        &transcript_path,
                        &json!({
                            "type": "message",
                            "role": "assistant",
                            "content": [{ "type": "output_text", "text": content_text.trim() }],
                        }),
                    )?;
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
                remove_optional(&pending_cloud_turn)?;
                return Ok(TurnUsage {
                    input_tokens: in_tok,
                    output_tokens: out_tok,
                    cache_read_tokens: cache_read_tok,
                    cache_write_tokens: cache_write_tok,
                    ..TurnUsage::default()
                });
            }
            if last_seq <= baseline_seq + 2
                && assistant_texts.is_empty()
                && last_progress.elapsed() >= Duration::from_millis(stall_timeout_ms)
            {
                interrupt_and_settle(&base, &sess_path, &api_key)?;
                return Err(format!(
                    "cloud session `{sess_path}` stalled in `{status}` for {}ms without progress",
                    stall_timeout_ms
                ));
            }
        }
    }
    if cancel_flag.load(Ordering::SeqCst) {
        interrupt_and_settle(&base, &sess_path, &api_key)?;
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
    )?;
    if cancel_flag.load(Ordering::SeqCst) {
        remove_optional(&pending_cloud_turn)?;
    }
    Ok(TurnUsage {
        input_tokens: turn_in_tok,
        output_tokens: turn_out_tok,
        cache_read_tokens: turn_cache_read_tok,
        ..TurnUsage::default()
    })
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

fn build_chat_completions_request(
    model: &str,
    instructions: &str,
    input: &[Value],
    reasoning_effort: Option<&str>,
) -> Result<Value, String> {
    validate_responses_input(input)?;
    let mut messages = Vec::with_capacity(input.len() + 1);
    if !instructions.trim().is_empty() {
        messages.push(json!({
            "role": "system",
            "content": instructions,
        }));
    }
    let mut idx = 0;
    while idx < input.len() {
        let item = &input[idx];
        if item.get("type").and_then(Value::as_str) == Some("function_call") {
            let mut tool_calls = Vec::new();
            let mut reasoning_parts = Vec::new();
            while idx < input.len()
                && input[idx].get("type").and_then(Value::as_str) == Some("function_call")
            {
                let fc = &input[idx];
                let cid = fc
                    .get("call_id")
                    .or_else(|| fc.get("id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                    .ok_or("missing tool call id")?;
                let fname = fc.get("name").and_then(Value::as_str).unwrap_or("");
                let fargs = required_text(fc, "arguments")?;
                tool_arguments(fargs)?;
                if let Some(rc) = fc
                    .get("reasoning_content")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    reasoning_parts.push(rc.to_string());
                }
                tool_calls.push(json!({
                    "id": cid,
                    "type": "function",
                    "function": {
                        "name": fname,
                        "arguments": fargs,
                    }
                }));
                idx += 1;
            }
            let mut assistant_msg = json!({
                "role": "assistant",
                "content": Value::Null,
                "tool_calls": tool_calls,
            });
            if !reasoning_parts.is_empty() {
                assistant_msg["reasoning_content"] = json!(reasoning_parts.join("\n"));
            }
            messages.push(assistant_msg);
            continue;
        }
        if item.get("type").and_then(Value::as_str) == Some("function_call_output") {
            let cid = item
                .get("call_id")
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .ok_or("missing tool call id")?;
            let out = item
                .get("output")
                .and_then(Value::as_str)
                .ok_or("invalid retained tool output")?;
            messages.push(json!({
                "role": "tool",
                "tool_call_id": cid,
                "content": out,
            }));
            idx += 1;
            continue;
        }
        if let Some(role) = item.get("role").and_then(Value::as_str) {
            let chat_role = match role {
                "developer" | "system" => "system",
                "assistant" => "assistant",
                _ => "user",
            };
            if let Some(s) = item.get("content").and_then(Value::as_str) {
                messages.push(json!({
                    "role": chat_role,
                    "content": s,
                }));
            } else if let Some(arr) = item.get("content").and_then(Value::as_array) {
                let mut parts = Vec::new();
                for p in arr {
                    let ptype = p.get("type").and_then(Value::as_str).unwrap_or("");
                    if matches!(ptype, "input_text" | "output_text" | "text")
                        && let Some(t) = p.get("text").and_then(Value::as_str)
                    {
                        parts.push(json!({ "type": "text", "text": t }));
                    } else if ptype == "input_image"
                        && let Some(u) = p.get("image_url").and_then(Value::as_str)
                    {
                        parts.push(json!({
                            "type": "image_url",
                            "image_url": { "url": u }
                        }));
                    }
                }
                if parts.len() == 1 && parts[0].get("type").and_then(Value::as_str) == Some("text")
                {
                    messages.push(json!({
                        "role": chat_role,
                        "content": parts[0].get("text").and_then(Value::as_str).unwrap_or(""),
                    }));
                } else if !parts.is_empty() {
                    messages.push(json!({
                        "role": chat_role,
                        "content": parts,
                    }));
                }
            }
        }
        idx += 1;
    }
    let mut body = json!({
        "model": model,
        "messages": messages,
        "tools": build_tool_definitions(),
    });
    if let Some(effort) = reasoning_effort {
        body["reasoning_effort"] = json!(effort);
    }
    Ok(body)
}

fn chat_completions_to_responses_envelope(raw_resp: &Value) -> Result<Value, String> {
    let Some(choices) = raw_resp.get("choices").and_then(Value::as_array) else {
        return Err(format!(
            "Chat Completions response missing `choices`: {}",
            raw_resp
        ));
    };
    let mut output = Vec::new();
    if let Some(first) = choices.first()
        && let Some(msg) = first.get("message")
    {
        let reasoning_content = msg
            .get("reasoning_content")
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|s| !s.is_empty());
        if let Some(text) = msg
            .get("content")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            output.push(json!({
                "type": "message",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": text }],
            }));
        }
        if let Some(tcs) = msg.get("tool_calls").and_then(Value::as_array) {
            for (i, tc) in tcs.iter().enumerate() {
                let cid = tc
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                    .ok_or("missing tool call id")?;
                let fname = tc
                    .pointer("/function/name")
                    .or_else(|| tc.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let fargs = tc
                    .pointer("/function/arguments")
                    .or_else(|| tc.get("arguments"))
                    .and_then(Value::as_str)
                    .ok_or("missing tool arguments")?;
                let mut fc_item = json!({
                    "type": "function_call",
                    "call_id": cid,
                    "name": fname,
                    "arguments": fargs,
                });
                if i == 0
                    && let Some(ref rc) = reasoning_content
                {
                    fc_item["reasoning_content"] = json!(rc);
                }
                output.push(fc_item);
            }
        }
    }
    Ok(json!({
        "object": "response",
        "status": "completed",
        "output": output,
        "usage": raw_resp.get("usage").cloned().unwrap_or(Value::Null),
    }))
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

fn close_dangling_function_calls(
    transcript_path: &Path,
    input_items: &mut Vec<Value>,
) -> Result<(), String> {
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
            append_transcript_message(transcript_path, &synthetic)?;
            fixed.push(synthetic);
        }
    }
    *input_items = fixed;
    Ok(())
}

/// Reconstruct unfinished inbound across failed turns/restarts. Snapshots live
/// in the existing append-only transcript, not in mailbox/config state. Raw user
/// messages after the latest snapshot cover a crash between append and snapshot.
fn load_pending_native_messages(transcript_path: &Path, active: &[Value]) -> Vec<Value> {
    let mut pending = Vec::new();
    let mut tracked = false;
    if let Ok(raw) = std::fs::read_to_string(transcript_path) {
        for line in raw.lines() {
            let Ok(item) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if item.get("type").and_then(Value::as_str) == Some("native_turn_state") {
                if let Some(items) = item.get("pending_messages").and_then(Value::as_array) {
                    pending = items.clone();
                    tracked = true;
                }
            } else if tracked && is_pending_native_message(&item) {
                pending.push(item);
            }
        }
    }
    if !tracked {
        // Migration: there is no trustworthy completed-turn boundary in legacy
        // transcripts. Preserve uncertain active inbound, never assume it done.
        pending.extend(
            active
                .iter()
                .filter(|item| is_pending_native_message(item))
                .cloned(),
        );
    }
    pending
}

fn is_pending_native_message(item: &Value) -> bool {
    item.get("role").and_then(Value::as_str) == Some("user")
        && item
            .get("content")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.trim_start().starts_with("<context-event "))
}

fn record_pending_native_messages(transcript_path: &Path, pending: &[Value]) -> Result<(), String> {
    append_transcript_message(
        transcript_path,
        &json!({
            "type": "native_turn_state", "pending_messages": pending,
            "timestamp_ms": anyagents_core::now_ms(),
        }),
    )?;
    Ok(())
}

/// Compact only between completed tool batches. Keep exact inbound messages and
/// the latest tool batch; older material remains lossless in Raw-RAG. This does
/// not touch identity, schedules, mailbox cursors, or execute any tool again.
fn compact_native_context(
    transcript_path: &Path,
    input: &[Value],
    pending_messages: &[Value],
    level: usize,
) -> Value {
    let raw_count = std::fs::read_to_string(transcript_path)
        .map(|raw| raw.lines().count())
        .unwrap_or(0);
    // Keep the last model-authored handoff once, not a recursively nested
    // digest of our own automatic handoffs on every compaction.
    let prior_handoff = std::fs::read_to_string(transcript_path)
        .ok()
        .and_then(|raw| {
            raw.lines().rev().find_map(|line| {
                let item: Value = serde_json::from_str(line).ok()?;
                if item.get("type").and_then(Value::as_str) != Some("context_window_open") {
                    return None;
                }
                item.get("prior_handoff")
                    .or_else(|| item.get("handoff"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
        })
        .unwrap_or_default();
    let mut suffix_start = None;
    // Retain the full last batch, including interleaved calls/results, rather
    // than a lone output or synthetic result. No tool is still running here.
    if let Some(last_output) = input
        .iter()
        .rposition(|item| item.get("type").and_then(Value::as_str) == Some("function_call_output"))
    {
        let mut start = last_output;
        let mut needed = std::collections::HashSet::new();
        for (idx, item) in input[..=last_output].iter().enumerate().rev() {
            match item.get("type").and_then(Value::as_str) {
                Some("function_call_output") => {
                    if let Some(cid) = item.get("call_id").and_then(Value::as_str) {
                        needed.insert(cid);
                    }
                }
                Some("function_call") => {
                    if let Some(cid) = item.get("call_id").and_then(Value::as_str) {
                        needed.remove(cid);
                    }
                    if needed.is_empty() {
                        start = idx;
                        break;
                    }
                }
                _ => {}
            }
        }
        suffix_start = Some(start);
    }
    // Pending that a model-authored handoff removed from active input still
    // precedes the retained suffix. Everything present retains its actual order:
    // a steer received after a completed tool must not move before that tool.
    let mut retained: Vec<Value> = pending_messages
        .iter()
        .filter(|item| !input.contains(item))
        .cloned()
        .collect();
    retained.extend(
        input
            .iter()
            .enumerate()
            .filter(|(idx, item)| {
                pending_messages.contains(item)
                    || (suffix_start.is_some_and(|start| *idx >= start)
                        && item.get("type").and_then(Value::as_str) != Some("reasoning"))
            })
            .map(|(_, item)| item.clone()),
    );
    if level >= 1 {
        // Results are replaceable context, not pending work. Preserve call IDs
        // and a pointer to the original raw line, never to a nested anchor copy.
        let indexed = load_transcript_indexed_items(transcript_path);
        let mut source_end = indexed.len();
        for item in retained.iter_mut().rev() {
            if item.get("type").and_then(Value::as_str) != Some("function_call_output")
                || item
                    .get("output")
                    .and_then(Value::as_str)
                    .is_none_or(|s| s.len() <= 512)
            {
                continue;
            }
            // A provider may reuse a call_id after its earlier pair completed.
            // Resolve this occurrence in the ordered active projection, whose
            // raw coordinates survive anchors and legacy tool normalization.
            let position = indexed[..source_end]
                .iter()
                .rposition(|(_, original)| original == item);
            let index = position.map(|position| {
                source_end = position;
                indexed[position].0
            });
            let range = index.map_or_else(
                || format!("transcript:0-{}", raw_count.saturating_sub(1)),
                |idx| format!("transcript:{idx}-{idx}"),
            );
            item["output"] = json!(format!(
                "[Completed tool result evicted from active context; exact raw output preserved at {range}. Use memory_recall with that range before relying on its contents. Do not replay the completed action.]"
            ));
        }
    }
    if level >= 2 {
        retained.retain(|item| pending_messages.contains(item));
    }
    let bounded = |text: String| auth::truncate_tool_output_middle_utf8(text, 2_000, 900, 900);
    let prior_handoff = if level >= 3 {
        String::new()
    } else {
        bounded(prior_handoff)
    };
    let continuity = if level >= 3 {
        String::new()
    } else {
        bounded(build_prior_transcript_continuity_block(transcript_path).unwrap_or_default())
    };
    json!({
        "type": "context_window_open",
        "handoff": format!(
            "Goal: continue the pending inbound work without replaying completed actions.\n\
             Verified Facts: pending messages are exact; completed tool state may be replaced by raw-history pointers.\n\
             Decisions: automatic pre-request compaction; no mailbox or lifecycle changes.\n\
             Next Action: reconcile the retained results, then continue the pending work.\n\
             Full prior history: transcript:0-{}. Use memory_recall before assuming older work is absent.\n\
             {prior_handoff}\n{continuity}", raw_count.saturating_sub(1)),
        "retained_items": retained,
        "prior_handoff": prior_handoff,
        "timestamp_ms": anyagents_core::now_ms(),
    })
}

/// Budget the complete serialized wire request, not bytes/4 of history alone.
/// One UTF-8 byte per token is deliberately conservative for byte-level model
/// tokenizers; framing and output get independent reserves. This also counts
/// inline images rather than silently ignoring their encoded payload.
fn request_token_upper_bound(request: &Value) -> Result<usize, String> {
    let bytes = serde_json::to_vec(request)
        .map_err(|e| format!("cannot budget model request: {e}"))?
        .len();
    let messages = request
        .get("input")
        .or_else(|| request.get("messages"))
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    Ok(bytes.saturating_add(messages.saturating_mul(32)))
}

fn prepare_budgeted_request(
    transcript_path: &Path,
    input: &mut Vec<Value>,
    pending_messages: &[Value],
    limits: &memory::ModelContextLimits,
    force_compact: bool,
    build: impl Fn(&[Value]) -> Result<Value, String>,
) -> Result<Value, String> {
    let output_reserve = (limits.window_tokens / 20)
        .max(1)
        .min(limits.max_output_tokens.unwrap_or(usize::MAX));
    let build_bounded = |items: &[Value]| -> Result<Value, String> {
        let mut body = build(items)?;
        let field = if body.get("messages").is_some() {
            "max_tokens"
        } else {
            "max_output_tokens"
        };
        body[field] = json!(output_reserve);
        Ok(body)
    };
    let mut body = build_bounded(input)?;
    let used = request_token_upper_bound(&body)?.saturating_add(output_reserve);
    let threshold = limits.window_tokens.saturating_mul(limits.compact_percent) / 100;
    let ceiling = limits.window_tokens.saturating_mul(95) / 100;
    if !force_compact && used < threshold && used <= ceiling {
        return Ok(body);
    }
    // Try progressively smaller *pure* candidates. Failed preflight must not
    // grow the raw log or change the active window on every recovery attempt.
    for level in 0..=3 {
        let anchor = compact_native_context(transcript_path, input, pending_messages, level);
        let candidate = memory::project_context_anchor(&anchor);
        body = build_bounded(&candidate)?;
        let used = request_token_upper_bound(&body)?.saturating_add(output_reserve);
        if used <= ceiling {
            append_transcript_message(transcript_path, &anchor)?;
            *input = candidate;
            return Ok(body);
        }
    }
    // Only exact pending inbound plus fixed instructions/tools remain. Refuse
    // this genuinely irreducible request before dispatch, without committing.
    let used = request_token_upper_bound(&body)?.saturating_add(output_reserve);
    Err(format!(
        "context_request_too_large: compacted current work requires {used} conservative tokens; model window {}; request not sent; raw transcript and pending work preserved",
        limits.window_tokens
    ))
}

fn is_context_overflow(status: u16, raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    matches!(status, 400 | 413 | 503)
        && [
            "context_length_exceeded",
            "exceeds the model's context window",
            "maximum context length",
            "exceeded model token limit",
        ]
        .iter()
        .any(|text| lower.contains(text))
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
    env: &workspace::Environment,
    mcp_command: &Path,
    mcp_args: &[String],
    initial_user_message: &str,
) -> Result<TurnUsage, String> {
    let endpoint = auth::resolve_upstream_endpoint(config_dir, env, model)?;
    let api_key = endpoint.api_key;
    let responses_url = endpoint.url;
    let model_id = endpoint.model_id;
    let reasoning_effort = endpoint.reasoning_effort;
    let upstream_kind = endpoint.kind;
    let mut system_prompt = assemble_system_prompt(config_dir, work_dir);
    system_prompt.push_str(&load_explicit_context(config_dir, work_dir)?);
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");
    memory::validate_transcript(&transcript_path)?;

    let mut input_items = load_transcript_input_items(&transcript_path);
    close_dangling_function_calls(&transcript_path, &mut input_items)?;
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
        || input_items.iter().rev().take(16).any(|item| {
            item.get("role").and_then(Value::as_str) == Some("user")
                && item.get("content").and_then(Value::as_str) == Some(initial_user_message)
        });
    if !already_appended_by_cloud {
        let user_msg = json!({
            "role": "user",
            "content": initial_user_message,
        });
        append_transcript_message(&transcript_path, &user_msg)?;
        input_items.push(user_msg);
    }

    let tools = build_responses_tool_definitions();
    let session_cache_key = state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .session_id
        .clone()
        .unwrap_or_else(|| {
            let short = config_dir
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("default");
            format!("sylphx_ses_{short}")
        });
    let usage_log_path = config_dir.join("sylphx-usage.jsonl");
    let mut total_in: u64 = 0;
    let mut total_out: u64 = 0;
    let mut total_cache_read: u64 = 0;
    let mut total_cache_write: u64 = 0;
    let mut unanswered_reply_target: Option<String> = extract_envelope_sender(initial_user_message);

    let limits = memory::resolve_model_context_limits(config_dir, &model_id, env);
    if limits.source == memory::ContextLimitSource::ConservativeFallback {
        let warning = json!({"role": "user", "content": format!(
            "<context-event kind=\"context.limit_fallback\" model=\"{}\" window_tokens=\"{}\">Catalog limit unavailable; using conservative offline budget. No model or harness switch.</context-event>", model_id, limits.window_tokens)});
        append_transcript_message(&transcript_path, &warning)?;
        input_items.push(warning);
    }
    let mut pending_messages = load_pending_native_messages(&transcript_path, &input_items);
    let initial = json!({"role": "user", "content": initial_user_message});
    if !pending_messages.contains(&initial) {
        pending_messages.push(initial);
    }
    record_pending_native_messages(&transcript_path, &pending_messages)?;
    let mut overflow_compacted_in_turn = false;
    let mut force_compact = false;
    for step in 0usize.. {
        if cancel_flag.load(Ordering::SeqCst) {
            break;
        }
        let before_steer = input_items.len();
        drain_steered_messages(
            state,
            &transcript_path,
            &mut input_items,
            &mut unanswered_reply_target,
        )?;
        if input_items.len() > before_steer {
            pending_messages.extend_from_slice(&input_items[before_steer..]);
            record_pending_native_messages(&transcript_path, &pending_messages)?;
        }
        let custom_ua = match &upstream_kind {
            auth::UpstreamGatewayKind::SylphxResponses => None,
            auth::UpstreamGatewayKind::OpenAiChatCompletions { user_agent, .. } => {
                user_agent.as_deref()
            }
        };
        let req_body = prepare_budgeted_request(
            &transcript_path,
            &mut input_items,
            &pending_messages,
            &limits,
            force_compact,
            |items| match &upstream_kind {
                auth::UpstreamGatewayKind::SylphxResponses => build_native_responses_request(
                    &model_id,
                    &system_prompt,
                    items,
                    &tools,
                    &session_cache_key,
                    reasoning_effort.as_deref(),
                ),
                auth::UpstreamGatewayKind::OpenAiChatCompletions { .. } => {
                    build_chat_completions_request(
                        &model_id,
                        &system_prompt,
                        items,
                        reasoning_effort.as_deref(),
                    )
                }
            },
        )?;
        force_compact = false;

        let step_t0 = Instant::now();
        let (status, raw) = auth::curl_post_json_with_headers(
            &responses_url,
            Some(&api_key),
            custom_ua,
            &req_body,
        )?;
        let step_elapsed_ms = step_t0.elapsed().as_millis() as u64;
        if !(200..300).contains(&status) {
            if is_context_overflow(status, &raw) && !overflow_compacted_in_turn {
                overflow_compacted_in_turn = true;
                force_compact = true;
                continue;
            }
            return Err(format!(
                "Sylphx responses API HTTP {status}: {}",
                raw.trim()
            ));
        }
        let raw_json: Value = serde_json::from_str(&raw)
            .map_err(|e| format!("invalid Sylphx responses API JSON: {e}"))?;
        let resp = match &upstream_kind {
            auth::UpstreamGatewayKind::SylphxResponses => raw_json,
            auth::UpstreamGatewayKind::OpenAiChatCompletions { .. } => {
                chat_completions_to_responses_envelope(&raw_json)?
            }
        };

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
            append_transcript_message(&transcript_path, &item)?;
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
            let mut batch_ids = std::collections::HashSet::new();
            for fc in function_calls {
                let call_id = fc
                    .get("call_id")
                    .or_else(|| fc.get("id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                    .ok_or("missing tool call id")?
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
                    .ok_or("missing tool arguments")?;
                let parsed_args: Value = tool_arguments(raw_args)?;
                if !batch_ids.insert(call_id.clone()) {
                    return Err("duplicate tool call id in batch".into());
                }
                validate_tool_input(&fn_name, &parsed_args)?;
                let norm_fn = fn_name.strip_prefix("mcp__anyagents__").unwrap_or(&fn_name);
                if matches!(norm_fn, "context_edit" | "context_prune")
                    || (norm_fn == "context"
                        && (parsed_args.get("action").and_then(Value::as_str) == Some("edit")
                            || parsed_args.get("edits").is_some()))
                {
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
                for ((call_id, fn_name, _), output) in parsed_calls.into_iter().zip(outputs) {
                    if matches!(
                        fn_name.trim_start_matches("mcp__anyagents__"),
                        "send" | "code"
                    ) && reply_delivered(&output, unanswered_reply_target.as_deref())
                    {
                        unanswered_reply_target = None;
                    }
                    let tool_out_item = json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": output,
                    });
                    let tool_out_item =
                        append_transcript_message(&transcript_path, &tool_out_item)?;
                    input_items.push(tool_out_item);
                }
            } else {
                for (call_id, fn_name, parsed_args) in parsed_calls {
                    if cancel_flag.load(Ordering::SeqCst) {
                        break;
                    }
                    let norm_fn = fn_name.strip_prefix("mcp__anyagents__").unwrap_or(&fn_name);
                    if (norm_fn == "context_window_open"
                        || (norm_fn == "context"
                            && (parsed_args.get("action").and_then(Value::as_str)
                                == Some("compact")
                                || parsed_args.get("handoff").is_some())))
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
                    if matches!(
                        fn_name.trim_start_matches("mcp__anyagents__"),
                        "send" | "code"
                    ) && reply_delivered(&output, unanswered_reply_target.as_deref())
                    {
                        unanswered_reply_target = None;
                    }
                    let (clean_output, attached_image_data_url) =
                        extract_attached_image_marker(&output);
                    let tool_out_item = json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": clean_output,
                    });
                    let tool_out_item =
                        append_transcript_message(&transcript_path, &tool_out_item)?;
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
                        append_transcript_message(&transcript_path, &image_item)?;
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
                append_transcript_message(&transcript_path, &anchor)?;
                input_items = load_transcript_input_items(&transcript_path);
            } else if did_context_edit {
                input_items = load_transcript_input_items(&transcript_path);
            } else {
                maybe_emit_context_watermark_event(&transcript_path, &mut input_items)?;
            }
            if self_archived {
                break;
            }
            continue;
        }

        let content_text = assistant_texts.join("\n\n");
        let trimmed_content = content_text.trim();
        if !trimmed_content.is_empty()
            && !trimmed_content.eq_ignore_ascii_case("NO_REPLY")
            && let Some(reply_to) = unanswered_reply_target.take()
        {
            let reply_receipt = call_anyagents_mcp_tool(
                mcp_command,
                mcp_args,
                env,
                "send",
                &json!({
                    "to": reply_to,
                    "text": trimmed_content,
                }),
            );
            if !reply_delivered(&reply_receipt, Some(reply_to.as_str())) {
                return Err(
                    "automatic reply delivery was not acknowledged; pending reply retained".into(),
                );
            }
        }

        let before_steer = input_items.len();
        if drain_steered_messages(
            state,
            &transcript_path,
            &mut input_items,
            &mut unanswered_reply_target,
        )? {
            pending_messages.extend_from_slice(&input_items[before_steer..]);
            record_pending_native_messages(&transcript_path, &pending_messages)?;
            continue;
        }

        if let Some(goal_continuation) = load_active_goal_continuation_prompt(config_dir)? {
            let cont_msg = json!({
                "role": "user",
                "content": goal_continuation,
            });
            append_transcript_message(&transcript_path, &cont_msg)?;
            pending_messages.push(cont_msg.clone());
            record_pending_native_messages(&transcript_path, &pending_messages)?;
            input_items.push(cont_msg);
            continue;
        }

        break;
    }

    if !cancel_flag.load(Ordering::SeqCst) {
        record_pending_native_messages(&transcript_path, &[])?;
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
    let work_dir = std::env::current_dir()?;
    let selected_settings =
        read_json(&config_dir.join("sylphx-agent.json")).map_err(anyagents_core::Error::Message)?;
    if config_val.get("model").is_some_and(|v| !v.is_string())
        || selected_settings
            .as_ref()
            .and_then(|v| v.get("model"))
            .is_some_and(|v| !v.is_string())
    {
        return Err(anyagents_core::Error::Message(
            "invalid configured model".into(),
        ));
    }
    let model = config_val
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            selected_settings
                .clone()
                .and_then(|v| v.get("model").and_then(Value::as_str).map(str::to_string))
        });
    let mcp_command = clean_exe_path(
        &config_val
            .get("mcp_command")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .or_else(|| {
                selected_settings.clone().and_then(|v| {
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
            selected_settings.clone().and_then(|v| {
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

    let env: workspace::Environment = std::env::vars().collect::<Vec<_>>().into();
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

    let transport_error = Arc::new(Mutex::new(None::<String>));
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    loop {
        if let Some(error) = transport_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            return Err(anyagents_core::Error::Message(error));
        }
        let line = match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(line) => line?,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let kind = msg.get("type").and_then(Value::as_str).unwrap_or("");
        if kind == "cancel" || kind == "interrupt" {
            cancel_flag.store(true, Ordering::SeqCst);
            // The running turn owns cancellation settlement and retained steers.
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
        let transport_cl = Arc::clone(&transport_error);

        std::thread::spawn(move || {
            let cloud_env = resolve_cloud_agent_environment(&cfg_cl, &env_cl);
            let outcome = match cloud_env {
                Ok(Some(env_slug)) => run_cloud_session_turn(
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
                ),
                Err(e) => Err(e),
                Ok(None) => run_native_gateway_turn(
                    &state_cl,
                    &cancel_cl,
                    &cfg_cl,
                    &work_cl,
                    model_cl.as_deref(),
                    &env_cl,
                    &mcp_cmd_cl,
                    &mcp_args_cl,
                    &user_text,
                ),
            };

            let extra_followups = {
                let mut g = state_cl.lock().unwrap_or_else(PoisonError::into_inner);
                g.last_activity = Some(Instant::now());
                let mut sc = steered_cl.lock().unwrap_or_else(PoisonError::into_inner);
                let count = *sc;
                *sc = 0;
                count
            };

            let terminal_result = (|| -> std::io::Result<()> {
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
                        write_terminal_frame(&mut out, &done_evt)?;
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
                            write_terminal_frame(&mut out, &followup_done)?;
                        }
                        out.flush()?;
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
                        write_terminal_frame(&mut out, &err_evt)?;
                        if extra_followups > 0 {
                            write_terminal_frame(&mut out, &err_evt)?;
                        }
                        out.flush()?;
                    }
                }
                Ok(())
            })();
            if let Err(e) = terminal_result {
                *transport_cl.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(format!("terminal output transport failed: {e}"));
            }
            state_cl
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .in_turn = false;
        });
    }

    for _ in 0..2400 {
        if let Some(error) = transport_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            return Err(anyagents_core::Error::Message(error));
        }
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
    unanswered_reply_target: &mut Option<String>,
) -> Result<bool, String> {
    let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
    if g.steer_queue.is_empty() {
        return Ok(false);
    }
    while let Some(steered) = g.steer_queue.front().cloned() {
        update_unanswered_reply_target(unanswered_reply_target, &steered);
        let msg = json!({
            "role": "user",
            "content": steered,
        });
        append_transcript_message(transcript_path, &msg)?;
        messages.push(msg);
        g.steer_queue.pop_front();
    }
    g.last_activity = Some(Instant::now());
    Ok(true)
}

fn update_unanswered_reply_target(current: &mut Option<String>, incoming_text: &str) {
    if let Some(new_target) = extract_envelope_sender(incoming_text)
        && (new_target.starts_with("telegram:")
            || current
                .as_deref()
                .is_none_or(|c| !c.starts_with("telegram:")))
    {
        *current = Some(new_target);
    }
}

fn extract_envelope_sender(text: &str) -> Option<String> {
    if let Some(idx) = text.find("answer_to=\"") {
        let rest = &text[idx + 11..];
        if let Some(end) = rest.find('"') {
            let dest = rest[..end].trim();
            if !dest.is_empty() {
                return Some(dest.to_string());
            }
        }
    }
    let idx = text.find("from=\"")?;
    let rest = &text[idx + 6..];
    let end = rest.find('"')?;
    let sender = rest[..end].trim();
    (!sender.is_empty() && !sender.starts_with("schedule:") && sender != "unknown")
        .then(|| sender.to_string())
}

fn read_workspace_agents_md(work_dir: &Path, shared_path: Option<&Path>) -> Option<String> {
    for dir in work_dir.ancestors() {
        let candidate = dir.join("AGENTS.md");
        // A shared file reached through workspace ancestry is one source, not
        // a second instruction scope. Distinct workspace files remain layered
        // even when their bytes or headings happen to match the shared file.
        if shared_path.is_some_and(|shared| {
            candidate == shared
                || candidate
                    .canonicalize()
                    .ok()
                    .zip(shared.canonicalize().ok())
                    .is_some_and(|(work, shared)| work == shared)
        }) {
            break;
        }
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

fn find_continuity_header(after: &str) -> Option<usize> {
    [
        "\n## 4. Cross-Harness Session Continuity",
        "\n## 5. Cross-Harness Session Continuity",
        "\n# Cross-Harness Continuity",
    ]
    .into_iter()
    .filter_map(|hdr| after.find(hdr))
    .min()
}

fn extract_embedded_shared_agents_section(sys: &str) -> Option<String> {
    let marker = "\n## 4. Shared Instructions (AGENTS.md)\n";
    let start = sys.find(marker)?;
    let after = &sys[start + marker.len()..];
    let body = match find_continuity_header(after) {
        Some(cont_idx) => &after[..cont_idx],
        None => after,
    };
    let trimmed = body.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn read_shared_agents_md_for_config(config_dir: &Path) -> Option<(PathBuf, String)> {
    let mut candidates = Vec::new();
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
    candidates.push(config_dir.join("AGENTS.md"));
    for cand in candidates {
        if let Ok(text) = std::fs::read_to_string(&cand) {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.starts_with("# AnyAgents Identity & Environment Awareness") {
                if let Some(extracted) = extract_embedded_shared_agents_section(trimmed) {
                    return Some((cand, extracted));
                }
                continue;
            }
            return Some((cand, text));
        }
    }
    None
}

fn strip_embedded_shared_agents_section(sys: &str) -> String {
    let marker = "\n## 4. Shared Instructions (AGENTS.md)\n";
    let Some(start) = sys.find(marker) else {
        return sys.to_string();
    };
    let prefix = sys[..start].trim_end();
    let after = &sys[start + marker.len()..];
    if let Some(cont_idx) = find_continuity_header(after) {
        let continuity = after[cont_idx..].trim_start_matches('\n');
        format!("{prefix}\n\n{continuity}")
    } else {
        prefix.to_string()
    }
}

pub fn assemble_system_prompt(config_dir: &Path, work_dir: &Path) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Ok(sys) = std::fs::read_to_string(config_dir.join("SYSTEM_PROMPT.md"))
        && !sys.trim().is_empty()
    {
        let cleaned = strip_embedded_shared_agents_section(&sys);
        if !cleaned.trim().is_empty() {
            parts.push(cleaned);
        }
    }
    let shared_agents = read_shared_agents_md_for_config(config_dir).filter(|(_, s)| {
        !s.trim().is_empty()
            && !s
                .trim_start()
                .starts_with("# AnyAgents Identity & Environment Awareness")
    });
    let work_agents = read_workspace_agents_md(
        work_dir,
        shared_agents.as_ref().map(|(path, _)| path.as_path()),
    )
    .filter(|s| !s.trim().is_empty());

    if let Some((_, shared)) = shared_agents {
        parts.push(format!(
            "## 4. Shared Instructions (AGENTS.md)\n\n{}",
            shared.trim()
        ));
    }
    if let Some(work) = work_agents {
        parts.push(format!(
            "## Workspace Instructions (AGENTS.md)\n\n{}",
            work.trim()
        ));
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
        work_dir.join("skills"),
        work_dir.join(".agents/skills"),
    ] {
        if cand.is_dir() && !dirs.contains(&cand) {
            dirs.push(cand);
        }
    }
    if let Some(data_dir) = config_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
    {
        for cand in [data_dir.join("skills"), data_dir.join(".agents/skills")] {
            if cand.is_dir() && !dirs.contains(&cand) {
                dirs.push(cand);
            }
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
    let mut desc_lines: Vec<String> = Vec::new();
    let mut in_multiline_desc = false;
    for line in fm.lines() {
        if let Some(v) = line.strip_prefix("name:") {
            in_multiline_desc = false;
            name = Some(v.trim().trim_matches('"').trim_matches('\'').to_string());
        } else if let Some(v) = line.strip_prefix("description:") {
            desc_lines.clear();
            let raw_v = v.trim();
            if matches!(raw_v, "|" | ">" | "|-" | ">-" | "") {
                in_multiline_desc = true;
            } else {
                in_multiline_desc = true;
                desc_lines.push(raw_v.trim_matches('"').trim_matches('\'').to_string());
            }
        } else if in_multiline_desc {
            if line.starts_with(' ') || line.starts_with('\t') {
                let cont = line.trim();
                if !cont.is_empty() {
                    desc_lines.push(cont.trim_matches('"').trim_matches('\'').to_string());
                }
            } else {
                in_multiline_desc = false;
            }
        }
    }
    let desc = desc_lines.join(" ");
    Some((name?, desc))
}

/// Discovers all installed skills following the `agentskills.io` 3-stage progressive
/// disclosure standard:
/// - Tier 1 (System Prompt Catalog): Full YAML frontmatter `name` + `description` (preserving
///   positive `"Use when..."` triggers and negative `"Do NOT use when..."` disambiguation
///   clauses up to the `agentskills.io` 1,024-char specification limit) with a mandatory
///   Turn-1 pre-execution activation gate.
/// - Tier 2 (On-Demand Activation): `activate_skill(name)` loads the full `SKILL.md` body and
///   lists `references/*.md`.
/// - Tier 3 (Reference Drill-Down): `activate_skill(name, reference)` loads specific
///   `references/<reference>.md` files on demand.
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
        "<skills>\n\
         You can use specialized 'skills' to help you with complex tasks. Each skill has a name and a description listed below.\n\
         If a skill seems relevant to your current task, you MUST call `activate_skill` with its `name` to read its `SKILL.md` instructions BEFORE proceeding (and pass `reference` to read any relevant `references/*.md` file).\n\n\
         Available skills:\n",
    );
    for (name, desc) in seen {
        let normalized = desc.split_whitespace().collect::<Vec<_>>().join(" ");
        let bounded: String = normalized.chars().take(1_024).collect();
        out.push_str(&format!("- {name}: {bounded}\n"));
    }
    out.push_str("</skills>");
    out
}

fn load_active_goal_continuation_prompt(config_dir: &Path) -> Result<Option<String>, String> {
    let goal_path = config_dir.join("sylphx-goal.json");
    if let Ok(raw) = std::fs::read_to_string(&goal_path)
        && raw.trim().is_empty()
    {
        let _ = remove_optional(&goal_path);
        return Ok(None);
    }
    let Some(mut val) = read_json(&goal_path)? else {
        return Ok(None);
    };
    validate_goal(&val)?;
    if val.get("state").and_then(Value::as_str) != Some("active") {
        return Ok(None);
    }
    let continuations = val
        .get("auto_continuations")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if continuations >= 12 {
        return Ok(None);
    }
    if let Some(obj) = val.as_object_mut() {
        obj.insert("auto_continuations".to_string(), json!(continuations + 1));
    }
    write_atomic(
        &goal_path,
        serde_json::to_string_pretty(&val)
            .map_err(|e| e.to_string())?
            .as_bytes(),
    )?;
    Ok(Some(format!(
        "[Session Goal state] {}\nContinue executing toward the active Goal's acceptance criteria, or call `goal` with `action: \"complete\"` (or `\"pause\"` / `\"cancel\"`) once verified.",
        serde_json::to_string(&val).unwrap_or_default()
    )))
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
                "name": "code",
                "description": "Unified AnyAgents Code Mode & Workspace/Coordination SDK. Executes an async TypeScript/JavaScript snippet in an isolated capability sandbox with `anyagents.{memory,conversation,send,agents,schedule,groups,browser,web,computer,device,media,files}` (alias `tools.anyagents`), `web.{search,fetch}` (alias `tools.web`), `computer.*` (alias `tools.computer`), `device.*` (alias `tools.device`), `media.*` (alias `tools.media`), and `workspace.{read,write,edit,multi_edit,patch,glob,grep,list,exec}` (alias `tools.workspace`). Always use `code` for file reads/edits/patches/searches, learned memory, episodic recall, web search/fetch, computer/device/media actions, agent spawning, schedules, and sending messages.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "code": {
                            "type": "string",
                            "description": "Async TypeScript / JavaScript snippet body to execute."
                        },
                        "timeout_ms": {
                            "type": "integer",
                            "description": "Optional execution timeout in milliseconds (default 30000, max 120000)."
                        }
                    },
                    "required": ["code"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "bash",
                "description": "Execute a bash command (`git`, `cargo`, `npm`, `kubectl`, `op`, etc.) in your working directory.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "Shell command string to execute."
                        },
                        "timeout_ms": {
                            "type": "integer",
                            "description": "Optional timeout in milliseconds (default 120000)."
                        }
                    },
                    "required": ["command"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "context",
                "description": "Manage your active context window, session goals, and progressive skill disclosure (`action`: `\"status\"` | `\"edit\"` | `\"compact\"` | `\"goal\"` | `\"skill\"`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["status", "edit", "compact", "goal", "skill"],
                            "description": "`\"status\"` (context budget), `\"edit\"` (replace/prune old transcript ranges), `\"compact\"` (open a fresh context window with `handoff`), `\"goal\"` (manage session goal), or `\"skill\"` (activate `SKILL.md` or `references/*.md`)."
                        },
                        "edits": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "range": { "type": "string" },
                                    "with": { "type": "string" }
                                },
                                "required": ["range", "with"]
                            }
                        },
                        "handoff": {
                            "type": "string",
                            "description": "Structured handoff (`Goal`, `Verified Facts`, `Decisions`, `Next Action`) when `action = \"compact\"`."
                        },
                        "goal_action": {
                            "type": "string",
                            "enum": ["create", "update", "complete", "pause", "cancel", "status"]
                        },
                        "title": { "type": "string" },
                        "acceptance_criteria": { "type": "array", "items": { "type": "string" } },
                        "progress": { "type": "string" },
                        "name": { "type": "string", "description": "Skill name when `action = \"skill\"`." },
                        "reference": { "type": "string", "description": "Optional reference filename under `references/` when `action = \"skill\"`." }
                    }
                }
            }
        }),
    ]
}

fn build_legacy_tool_definitions() -> Vec<Value> {
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
                        "media": { "type": "string", "description": "Local file path or HTTPS URL of an image/file/document to attach." },
                        "media_paths": { "type": "array", "items": { "type": "string" }, "description": "Multiple local file paths or HTTPS URLs to attach." },
                        "buttons": { "description": "Interactive inline buttons (array or 2D grid of `{text, callback_data, url}`)." },
                        "poll": {
                            "type": "object",
                            "description": "Native poll (`{question, options, allows_multiple_answers, is_anonymous}`).",
                            "properties": {
                                "question": { "type": "string" },
                                "options": { "type": "array", "items": { "type": "string" } },
                                "allows_multiple_answers": { "type": "boolean" },
                                "is_anonymous": { "type": "boolean" }
                            }
                        },
                        "silent": { "type": "boolean", "description": "Deliver silently without notification sound." },
                        "action": { "type": "string", "description": "Interaction (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`)." },
                        "target": { "type": "string", "description": "Target message (`anyagents:reply:msg_...`, `msg_...`, or `telegram:chat:<chat_id>:<message_id>`)." },
                        "emoji": { "type": "string", "description": "Emoji for `react` (empty string to remove reaction)." },
                        "options": { "type": "array", "items": { "type": "integer" }, "description": "0-based option indices for `vote`." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "describe",
                "description": "Read or update agent public `description` (capabilities/expertise) or private `instructions` (`SYSTEM_PROMPT.md`) (`agent`: `\"self\"` | `\"all\"` | `<name>`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string", "description": "`\"self\"` (default), `\"all\"`, or agent name/`agt_...` ID." },
                        "description": { "type": "string", "description": "Public role/capability summary." },
                        "instructions": { "type": "string", "description": "Private system prompt (`SYSTEM_PROMPT.md`)." },
                        "role": { "type": "string", "description": "Display-only role label; never selects lifecycle, scope or execution instructions." },
                        "public": { "type": "boolean", "description": "Whether agents outside this agent's parent/children tree can message it directly." },
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
                        "new_name": { "type": "string", "description": "Target name when `action` is `\"rename\"`." },
                        "parent": { "type": "string" },
                        "state": { "type": "string" },
                        "role": { "type": "string", "description": "Display-only role label; never selects lifecycle, scope or execution instructions." },
                        "harness": { "type": "string" },
                        "model": { "type": "string" },
                        "profile": { "type": "string" },
                        "repo": { "type": "string" },
                        "branch": { "type": "string" },
                        "base": { "type": "string" },
                        "workdir": { "type": "string", "description": "Workspace path or mode: worktree, private, or inherit (explicit parent sharing). Omitting both repo and workdir gives a sub-agent its own private <data>/agents/<id>/work directory." },
                        "description": { "type": "string" },
                        "instructions": { "type": "string" },
                        "public": { "type": "boolean", "description": "Cross-tree reachability (`false` default for sub-agents)." },
                        "prompt": { "type": "string" },
                        "clear": { "type": "boolean" },
                        "probe": { "type": "boolean" }
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
                        "direction": { "type": "string", "description": "`\"all\"` (default), `\"in\"`, or `\"out\"`." },
                        "before": { "type": "string", "description": "Exclusive upper bound cursor (`msg_...` ID or millisecond timestamp)." },
                        "after": { "type": "string", "description": "Exclusive lower bound cursor (`msg_...` ID or millisecond timestamp)." },
                        "limit": { "type": "integer" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "groups",
                "description": "Manage agent groups (`action`: `\"list\"`, `\"members\"`, `\"join\"`, `\"leave\"`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`\"list\"` (default), `\"members\"`, `\"join\"` (create/join group), or `\"leave\"`." },
                        "group": { "type": "string", "description": "Group name (e.g. `\"owners\"`, `\"review-team\"`)." },
                        "agent": { "type": "string", "description": "Agent name(s) or `agt_...` ID(s) to add/remove (comma-separated, defaults to self)." },
                        "agents": { "type": "array", "items": { "type": "string" }, "description": "Batch list of agent names or `agt_...` IDs to add/remove." },
                        "public": { "type": "boolean", "description": "Visibility toggle: `false` (default Private/Invite-Only) or `true` (Public/Open)." }
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
                "description": "Drive Chromium (`goto`, `snapshot`, `click`, `fill`, `type`, `press`, `eval`, `text`, `html`, `pdf`, `screenshot`, `cookies`, `set_cookies`, `tabs`, `wait`, `takeover`, `release_takeover`, `close`) with persistent per-agent profile and zero-trust 1Password `secret_ref` injection.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`goto`, `snapshot`, `click`, `fill`, `type`, `press`, `eval`, `text`, `html`, `pdf`, `screenshot`, `cookies`, `set_cookies`, `tabs`, `wait`, `takeover`, `release_takeover`, `close`." },
                        "profile": { "type": "string", "description": "Persistent browser profile name (defaults to agent name; pass lead's profile name when delegated)." },
                        "url": { "type": "string", "description": "Target URL for `goto` (or initial URL when opening a tab)." },
                        "selector": { "type": "string", "description": "Element ref (`\"@e1\"` from `snapshot`) or CSS selector." },
                        "value": { "type": "string", "description": "Input value for `fill` / `type`." },
                        "text": { "type": "string", "description": "Alias for `value` on `fill` / `type`." },
                        "secret_ref": { "type": "string", "description": "Zero-trust 1Password secret or TOTP reference (`op://<vault>/<item>/<field>` or `op-otp://<vault>/<item>`) resolved in-process by the host daemon for `fill` / `type` without exposing the secret to LLM context." },
                        "submit": { "type": "boolean", "description": "Press Enter after `fill` / `type`." },
                        "key": { "type": "string", "description": "Key name for `press` (e.g. `\"Enter\"`, `\"Tab\"`, `\"Escape\"`)." },
                        "keys": { "type": "array", "items": { "type": "string" } },
                        "hold_ms": { "type": "integer" },
                        "wait_ms": { "type": "integer", "description": "Wait duration in milliseconds." },
                        "script": { "type": "string", "description": "JavaScript expression for `eval`." },
                        "path": { "type": "string", "description": "Output file path for `screenshot` / `pdf`." },
                        "full_page": { "type": "boolean", "description": "Capture full scrollable page in `screenshot`." },
                        "tab_action": { "type": "string", "description": "`list`, `new`, `activate`, or `close` when `action` is `\"tabs\"`." },
                        "target_id": { "type": "string", "description": "Target tab ID for `tabs`." },
                        "cookies": { "type": "array", "items": { "type": "object" }, "description": "Cookies array for `set_cookies`." },
                        "reason": { "type": "string", "description": "Reason for human `takeover`." },
                        "tunnel": { "type": "boolean", "description": "Publish an HTTPS tunnel URL during `takeover`." }
                    },
                    "required": ["action"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "sandbox",
                "description": "Inspect the current workspace execution placement.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "enum": ["status"], "description": "Inspect current placement." }
                    },
                    "additionalProperties": false
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
                        "end_line": { "type": "integer" },
                        "cursor": { "type": "string", "description": "Continuation for reads of this session's raw transcript; keep path and line range unchanged." }
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
                "description": "Lossless Raw-RAG BM25 + range recall across the append-only transcript. Responses are capped at 32 KiB; repeat original arguments with next_cursor to concatenate JSON content fragments. Already-active items return pointers, not verbatim replay.",
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
                        "radius": { "type": "integer" },
                        "cursor": { "type": "string", "description": "Continuation from next_cursor; keep all original arguments unchanged." }
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

/// The sole tool-result ingress: every execution route yields complete text here.
/// Mask before any size projection, then expose only this value to native harness
/// consumers. Producers must not truncate text before returning to this boundary.
fn execute_sylphx_tool(
    name: &str,
    args: &Value,
    config_dir: &Path,
    work_dir: &Path,
    env: &workspace::Environment,
    mcp_command: &Path,
    mcp_args: &[String],
) -> String {
    if let Err(e) = validate_tool_input(name, args) {
        return format!("error: {e}");
    }
    let raw = execute_sylphx_tool_raw(name, args, config_dir, work_dir, env, mcp_command, mcp_args);
    let masked = anyagents_core::masking::mask_known_secrets(&raw).into_owned();
    let normalized = name.strip_prefix("mcp__anyagents__").unwrap_or(name);
    // Preserve the existing producer-specific observation bounds. Binary image
    // attachments and native recall retain their own structured representations.
    let limits = match normalized {
        "agents" | "subagents" | "send" | "conversation" | "search" | "groups" | "schedule"
        | "browser" | "describe" | "archive" | "wake" | "spawn" | "interact" | "memory" | "web"
        | "computer" | "device" | "media" | "files" => Some((24_000, 11_000, 11_000)),
        "code" => Some((48_000, 22_000, 22_000)),
        "bash" | "shell" | "code_mode" | "grep" | "glob" | "context" => {
            Some((30_000, 14_000, 14_000))
        }
        "list_dir" if workspace::delegated(env) => Some((30_000, 14_000, 14_000)),
        "read_file"
            if args
                .get("path")
                .and_then(Value::as_str)
                .and_then(image_mime_for_path)
                .is_none() =>
        {
            Some((64_000, 30_000, 30_000))
        }
        _ => None,
    };
    match limits {
        Some((max, head, tail)) => truncate_tool_output_middle_utf8(masked, max, head, tail),
        None => masked,
    }
}

fn execute_sylphx_tool_raw(
    name: &str,
    args: &Value,
    config_dir: &Path,
    work_dir: &Path,
    env: &workspace::Environment,
    mcp_command: &Path,
    mcp_args: &[String],
) -> String {
    let normalized = name.strip_prefix("mcp__anyagents__").unwrap_or(name);
    match normalized {
        "code" | "agents" | "subagents" | "send" | "conversation" | "search" | "groups"
        | "schedule" | "browser" | "describe" | "archive" | "wake" | "spawn" | "interact"
        | "memory" | "web" | "computer" | "device" | "media" | "files" => {
            call_anyagents_mcp_tool(mcp_command, mcp_args, env, normalized, args)
        }
        "context" => {
            let action = args
                .get("action")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| {
                    if args.get("handoff").is_some() {
                        "compact"
                    } else if args.get("edits").is_some() {
                        "edit"
                    } else if args.get("name").is_some() {
                        "skill"
                    } else if args.get("goal_action").is_some()
                        || args.get("title").is_some()
                        || args.get("acceptance_criteria").is_some()
                    {
                        "goal"
                    } else {
                        "status"
                    }
                });
            match action {
                "edit" | "prune" => execute_sylphx_tool_raw(
                    "context_edit",
                    args,
                    config_dir,
                    work_dir,
                    env,
                    mcp_command,
                    mcp_args,
                ),
                "compact" | "window_open" => execute_sylphx_tool_raw(
                    "context_window_open",
                    args,
                    config_dir,
                    work_dir,
                    env,
                    mcp_command,
                    mcp_args,
                ),
                "goal" => {
                    let mut goal_args = args.clone();
                    if let Some(ga) = args.get("goal_action").cloned()
                        && let Some(obj) = goal_args.as_object_mut()
                    {
                        obj.insert("action".to_string(), ga);
                    } else if let Some(obj) = goal_args.as_object_mut() {
                        obj.insert("action".to_string(), json!("status"));
                    }
                    execute_sylphx_tool_raw(
                        "goal",
                        &goal_args,
                        config_dir,
                        work_dir,
                        env,
                        mcp_command,
                        mcp_args,
                    )
                }
                "skill" | "activate_skill" => execute_sylphx_tool_raw(
                    "activate_skill",
                    args,
                    config_dir,
                    work_dir,
                    env,
                    mcp_command,
                    mcp_args,
                ),
                _ => execute_sylphx_tool_raw(
                    "context_budget_read",
                    args,
                    config_dir,
                    work_dir,
                    env,
                    mcp_command,
                    mcp_args,
                ),
            }
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
        "sandbox" => {
            if args.get("placement").is_some()
                || args
                    .get("action")
                    .is_some_and(|action| action.as_str() != Some("status"))
            {
                return "error: sandbox only supports `action: \"status\"`; execution placement is configured by the host".into();
            }
            let current = match resolve_cloud_agent_environment(config_dir, env) {
                Ok(Some(_)) => "cloud",
                Ok(None) => "local",
                Err(e) => return format!("error: {e}"),
            };
            serde_json::to_string_pretty(&json!({
                "status": "ok",
                "placement": current,
                "work_dir": work_dir.display().to_string(),
            }))
            .unwrap_or_default()
        }
        "activate_skill" | "skill" => {
            let Some(skill_name) = args.get("name").and_then(Value::as_str).map(str::trim) else {
                return "error: missing `name`".to_string();
            };
            if skill_name.is_empty()
                || Path::new(skill_name).components().count() != 1
                || skill_name == ".."
            {
                return "error: invalid skill name".into();
            }
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
                        if Path::new(r).is_absolute()
                            || Path::new(r)
                                .components()
                                .any(|c| c == std::path::Component::ParentDir)
                        {
                            return "error: invalid skill reference".into();
                        }
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
                    let body = match std::fs::read_to_string(&skill_md) {
                        Ok(body) => body,
                        Err(e) => return format!("error reading {}: {e}", skill_md.display()),
                    };
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
            if let Ok(raw) = std::fs::read_to_string(&goal_path)
                && raw.trim().is_empty()
            {
                let _ = remove_optional(&goal_path);
            }
            let mut current = match read_json(&goal_path) {
                Ok(Some(value)) => {
                    if let Err(e) = validate_goal(&value) {
                        return format!("error: {e}");
                    }
                    value
                }
                Ok(None) => json!({"state": "none"}),
                Err(e) => return format!("error: {e}"),
            };
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
                    if let Err(e) = write_atomic(&goal_path, current.to_string().as_bytes()) {
                        return format!("error: {e}");
                    }
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
                    if let Err(e) = write_atomic(&goal_path, current.to_string().as_bytes()) {
                        return format!("error: {e}");
                    }
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
                    if let Err(e) = write_atomic(&goal_path, current.to_string().as_bytes()) {
                        return format!("error: {e}");
                    }
                }
                "status" | "get" => {}
                _ => return format!("error: invalid goal action `{action}`"),
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
            execute_sylphx_tool_raw(
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
            execute_sylphx_tool_raw(
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
            execute_sylphx_tool_raw(
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
            execute_sylphx_tool_raw(
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
            if args.get("placement").is_some() {
                return "error: execution placement is configured by the host".into();
            }
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
            let transcript = config_dir.join("sylphx-transcript.jsonl");
            if std::fs::canonicalize(&target)
                .ok()
                .is_some_and(|path| std::fs::canonicalize(&transcript).ok().as_ref() == Some(&path))
            {
                // File reads of our own raw store must not bypass replay
                // suppression or response paging. File lines are 1-based;
                // transcript coordinates are 0-based.
                return execute_memory_recall(
                    &transcript,
                    &json!({
                        "range": format!("transcript:{}-{}", start_line.unwrap_or(1).max(1) - 1, end_line.unwrap_or(u64::MAX).saturating_sub(1)),
                        "cursor": args.get("cursor"),
                    }),
                );
            }
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
    let Some(mut obj) = arguments.as_object().cloned() else {
        return arguments.clone();
    };
    if matches!(tool_name, "agents" | "describe" | "spawn" | "subagents")
        && !obj.contains_key("instructions")
    {
        if let Some(inst) = obj.remove("instruction") {
            obj.insert("instructions".to_string(), inst);
        } else if let Some(inst) = obj.remove("system_prompt") {
            obj.insert("instructions".to_string(), inst);
        }
    }
    if tool_name != "schedule" {
        return Value::Object(obj);
    }
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
    let mut last_err = String::new();
    for attempt in 0..3 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(120 * attempt as u64));
        }
        let mut cmd = Command::new(&clean_cmd);
        cmd.args(mcp_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Stderr is not part of the response. Do not let diagnostics fill a
            // pipe while we wait for the tool's stdout response.
            .stderr(Stdio::null());
        for (k, v) in env {
            if !anyagents_harness::SERVER_ONLY_ENV.contains(&k.as_str()) {
                cmd.env(k, v);
            }
        }
        for key in anyagents_harness::SERVER_ONLY_ENV {
            cmd.env_remove(key);
        }
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(err) => {
                last_err = format!("failed to spawn anyagents mcp for `{tool_name}`: {err}");
                continue;
            }
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
        let mut saw_init_response = false;
        let response = (|| -> Result<Value, String> {
            for request in [init_req, init_notif, call_req] {
                writeln!(stdin, "{request}")
                    .map_err(|err| format!("writing MCP request: {err}"))?;
            }
            let _ = stdin.flush();
            // EOF terminates rmcp's transport and cancels asynchronous tools. Keep
            // stdin open until our response arrives, ignoring init and notifications.
            for line in (&mut stdout).lines() {
                let line = line.map_err(|err| format!("reading MCP response: {err}"))?;
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    if value.get("id").and_then(Value::as_i64) == Some(0) {
                        saw_init_response = true;
                    } else if value.get("id").and_then(Value::as_i64) == Some(1) {
                        return Ok(value);
                    }
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
        match response {
            Ok(value) => {
                if let Some(err) = value.pointer("/error/message").and_then(Value::as_str) {
                    return format!("error: {err}");
                }
                let output = value
                    .pointer("/result/content/0/text")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| value.to_string());
                return output;
            }
            Err(err) => {
                last_err = err;
                if saw_init_response {
                    break;
                }
            }
        }
    }
    format!("error: {last_err}")
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
            sender: Some("agent:worker-a".to_string()),
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
            sender: Some("agent:worker-a".to_string()),
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
        assert_eq!(names, vec!["code", "bash", "context"]);
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
            &workspace::Environment::default(),
            Path::new("anyagents"),
            &[],
        );
        let prompt_after = assemble_system_prompt(&cfg, &work);
        assert_eq!(prompt_before, prompt_after);
    }

    fn context_preflight_limits(window_tokens: usize) -> memory::ModelContextLimits {
        memory::ModelContextLimits {
            window_tokens,
            compact_percent: 85,
            max_output_tokens: None,
            source: memory::ContextLimitSource::Catalog,
        }
    }

    fn context_preflight_build(input: &[Value]) -> Result<Value, String> {
        build_native_responses_request(
            "test/model",
            "standing instructions",
            input,
            &[],
            "unchanged-session",
            None,
        )
    }

    #[test]
    fn context_preflight_compacts_before_request_and_preserves_pending_tool_state() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("sylphx-transcript.jsonl");
        let initial = json!({"role":"user", "content":"pending inbox message #123"});
        let steer = json!({"role":"user", "content":"steered update #124"});
        let call =
            json!({"type":"function_call", "call_id":"done", "name":"bash", "arguments":"{}"});
        let result = json!({"type":"function_call_output", "call_id":"done", "output":"exit_code: 0; already deployed sha123"});
        let mut input = vec![
            json!({"role":"assistant", "content":"x".repeat(30_000)}),
            initial.clone(),
            call.clone(),
            result.clone(),
            steer.clone(),
        ];
        for item in &input {
            append_transcript_message(&transcript, item).unwrap();
        }
        let raw_before = std::fs::read_to_string(&transcript).unwrap();
        let request = prepare_budgeted_request(
            &transcript,
            &mut input,
            &[initial.clone(), steer.clone()],
            &context_preflight_limits(24_000),
            false,
            context_preflight_build,
        )
        .unwrap();
        assert!(
            input[0]["content"]
                .as_str()
                .unwrap()
                .contains("context.window_opened")
        );
        for retained in [&initial, &steer, &call, &result] {
            assert_eq!(input.iter().filter(|item| *item == retained).count(), 1);
        }
        assert_eq!(&input[1..], &[initial, call, result, steer]);
        assert_eq!(input, load_transcript_input_items(&transcript));
        assert_eq!(request["prompt_cache_key"], "unchanged-session");
        assert!(request_token_upper_bound(&request).unwrap() + 1_200 < 24_000);
        assert!(
            std::fs::read_to_string(&transcript)
                .unwrap()
                .starts_with(&raw_before)
        );
        assert!(execute_memory_recall(&transcript, &json!({"query":"sha123"})).contains("sha123"));
        validate_responses_input(&input).unwrap();
    }

    // Adapted from the independent review's duplicate-completed-ID probe.
    #[test]
    fn review_reused_completed_call_id_pointer_must_recover_latest_result() {
        for legacy in [false, true] {
            for anchored in [false, true] {
                let tmp = tempfile::tempdir().unwrap();
                let transcript = tmp.path().join("sylphx-transcript.jsonl");
                let call = json!({"type":"function_call", "call_id":"reused", "name":"bash", "arguments":"{}"});
                let old = "older result A";
                let latest = "LATEST result B 中文".repeat(5_000);
                let pending = json!({"role":"user", "content":"continue"});
                for output in [old, latest.as_str()] {
                    if legacy {
                        append_transcript_message(
                            &transcript,
                            &json!({"role":"assistant", "tool_calls":[
                                {"id":"reused", "type":"function", "function":{"name":"bash", "arguments":"{}"}}
                            ]}),
                        ).unwrap();
                        append_transcript_message(
                            &transcript,
                            &json!({"role":"tool", "tool_call_id":"reused", "content":output}),
                        )
                        .unwrap();
                    } else {
                        append_transcript_message(&transcript, &call).unwrap();
                        append_transcript_message(
                            &transcript,
                            &json!({"type":"function_call_output", "call_id":"reused", "output":output}),
                        ).unwrap();
                    }
                }
                append_transcript_message(&transcript, &pending).unwrap();
                let mut input = load_transcript_input_items(&transcript);
                validate_responses_input(&input).unwrap();
                if anchored {
                    // The loader must retain each original raw position through
                    // repeated anchors, not point to an anchor's handoff text.
                    for _ in 0..2 {
                        append_transcript_message(
                            &transcript,
                            &json!({"type":"context_window_open", "handoff":"keep current work", "retained_items":input}),
                        ).unwrap();
                        input = load_transcript_input_items(&transcript);
                    }
                }
                prepare_budgeted_request(
                    &transcript,
                    &mut input,
                    &[pending],
                    &context_preflight_limits(24_000),
                    false,
                    context_preflight_build,
                )
                .unwrap();
                let pointer = input
                    .iter()
                    .find(|v| v["type"] == "function_call_output")
                    .unwrap()["output"]
                    .as_str()
                    .unwrap();
                assert!(
                    pointer.contains("transcript:3-3"),
                    "legacy={legacy}, anchored={anchored}: {pointer}"
                );
                assert!(!pointer.contains("transcript:1-1"));
                let mut args = json!({"range":"transcript:3-3"});
                let mut recovered_json = String::new();
                loop {
                    let page = execute_memory_recall(&transcript, &args);
                    assert!(page.len() <= 32_768);
                    let parsed: Value = serde_json::from_str(&page).unwrap();
                    assert_eq!(parsed["mode"], "page");
                    recovered_json.push_str(parsed["content"].as_str().unwrap());
                    let Some(cursor) = parsed["next_cursor"].as_str() else {
                        break;
                    };
                    args["cursor"] = json!(cursor);
                }
                let recovered: Value = serde_json::from_str(&recovered_json).unwrap();
                assert_eq!(recovered["items"][0]["index"], 3);
                let prefix = if legacy {
                    "[tool] "
                } else {
                    "[tool_output:bash:reused] "
                };
                assert_eq!(
                    recovered["items"][0]["content"],
                    format!("{prefix}{latest}")
                );
                validate_responses_input(&input).unwrap();
            }
        }
    }

    #[test]
    fn context_preflight_uses_active_models_catalog_window_and_offline_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("model-catalog-direct.json"), json!({"data":[
            {"id":"context-test/small", "context_length":24_000},
            {"id":"context-test/large", "limits":{"constraints":{"context_window_tokens":96_000}}}
        ]}).to_string()).unwrap();
        // A stale configured model must not override the dispatched model.
        std::fs::write(
            tmp.path().join("sylphx-agent.json"),
            r#"{"model":"wrong/model"}"#,
        )
        .unwrap();
        for (model, compact) in [("context-test/small", true), ("context-test/large", false)] {
            let limits = memory::resolve_model_context_limits(tmp.path(), model, &[]);
            assert_eq!(limits.source, memory::ContextLimitSource::Catalog);
            assert_eq!(limits.window_tokens, if compact { 24_000 } else { 96_000 });
            let transcript = tmp
                .path()
                .join(format!("{}.jsonl", if compact { "small" } else { "large" }));
            let pending = json!({"role":"user", "content":"pending"});
            let mut input = vec![
                json!({"role":"assistant", "content":"x".repeat(30_000)}),
                pending.clone(),
            ];
            for item in &input {
                append_transcript_message(&transcript, item).unwrap();
            }
            prepare_budgeted_request(
                &transcript,
                &mut input,
                &[pending],
                &limits,
                false,
                context_preflight_build,
            )
            .unwrap();
            assert_eq!(
                input[0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("context.window_opened"),
                compact
            );
        }
        let fallback = memory::resolve_model_context_limits(tmp.path(), "unknown/model", &[]);
        assert_eq!(
            fallback.source,
            memory::ContextLimitSource::ConservativeFallback
        );
        assert!(fallback.window_tokens <= memory::DEFAULT_CONTEXT_WINDOW_TOKENS);
    }

    #[test]
    fn context_preflight_pending_inbound_survives_failed_turn_restart_and_snapshot_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("sylphx-transcript.jsonl");
        let a = json!({"role":"user", "content":"unfinished inbox A"});
        let steer = json!({"role":"user", "content":"steer after failure"});
        let b = json!({"role":"user", "content":"new inbox B"});
        append_transcript_message(&transcript, &a).unwrap();
        record_pending_native_messages(&transcript, std::slice::from_ref(&a)).unwrap();
        // Failure means no completion/empty snapshot. A steer may have been
        // appended just before a crash, without its updated snapshot.
        append_transcript_message(&transcript, &steer).unwrap();
        append_transcript_message(&transcript, &b).unwrap();
        let mut input = load_transcript_input_items(&transcript);
        let pending = load_pending_native_messages(&transcript, &input);
        assert_eq!(pending, vec![a.clone(), steer.clone(), b.clone()]);
        record_pending_native_messages(&transcript, &pending).unwrap();
        prepare_budgeted_request(
            &transcript,
            &mut input,
            &pending,
            &context_preflight_limits(24_000),
            true,
            context_preflight_build,
        )
        .unwrap();
        assert_eq!(&input[1..], &[a, steer, b]);
        assert_eq!(
            load_pending_native_messages(&transcript, &load_transcript_input_items(&transcript)),
            pending
        );
        // Metadata is not sent to the provider, and success closes the pending
        // boundary so a later normal turn does not revive completed work.
        assert!(!input.iter().any(|item| item["type"] == "native_turn_state"));
        record_pending_native_messages(&transcript, &[]).unwrap();
        assert!(load_pending_native_messages(&transcript, &input).is_empty());
    }

    #[test]
    fn context_preflight_catalog_cache_is_scoped_to_gateway_and_agent() {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = tmp.path().join("model-catalog-direct.json");
        let write_catalog = |window| {
            std::fs::write(
                &catalog,
                json!({"data":[{"id":"cache-scope/model", "context_window":window}]}).to_string(),
            )
            .unwrap()
        };
        let env_a = vec![(
            "SYLPHX_GATEWAY_URL".into(),
            "https://gateway-a.invalid/v1".into(),
        )];
        let env_b = vec![(
            "SYLPHX_GATEWAY_URL".into(),
            "https://gateway-b.invalid/v1".into(),
        )];
        write_catalog(1_048_576);
        assert_eq!(
            memory::resolve_model_context_limits(tmp.path(), "cache-scope/model", &env_a)
                .window_tokens,
            1_048_576
        );
        write_catalog(8_192);
        assert_eq!(
            memory::resolve_model_context_limits(tmp.path(), "cache-scope/model", &env_b)
                .window_tokens,
            8_192
        );
        // Both entries remain cached independently.
        assert_eq!(
            memory::resolve_model_context_limits(tmp.path(), "cache-scope/model", &env_a)
                .window_tokens,
            1_048_576
        );
        let other = tempfile::tempdir().unwrap();
        std::fs::write(
            other.path().join("model-catalog-direct.json"),
            r#"{"data":[{"id":"cache-scope/model","context_window":16000}]}"#,
        )
        .unwrap();
        assert_eq!(
            memory::resolve_model_context_limits(other.path(), "cache-scope/model", &env_a)
                .window_tokens,
            16_000
        );
    }

    #[test]
    fn context_preflight_persisted_catalog_requires_matching_gateway_provenance() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(".sylphx-remote-models.json"),
            json!({
                "gateway_base_url":"https://gateway-a.invalid/v1",
                "catalog":{"data":[{"id":"disk-cache/model", "context_window":1_048_576}]}
            })
            .to_string(),
        )
        .unwrap();
        let env_a = vec![(
            "SYLPHX_GATEWAY_URL".into(),
            "https://gateway-a.invalid/v1".into(),
        )];
        let env_b = vec![(
            "SYLPHX_GATEWAY_URL".into(),
            "https://gateway-b.invalid/v1".into(),
        )];
        let a = memory::resolve_model_context_limits(tmp.path(), "disk-cache/model", &env_a);
        assert_eq!(a.window_tokens, 1_048_576);
        let b = memory::resolve_model_context_limits(tmp.path(), "disk-cache/model", &env_b);
        assert_eq!(b.source, memory::ContextLimitSource::ConservativeFallback);
        assert!(b.window_tokens <= memory::DEFAULT_CONTEXT_WINDOW_TOKENS);
    }

    // Both HTTP fakes consume a complete framed request before replying. One
    // TCP read may return just part of a header or body; closing with unread
    // bytes can reset the client and consume a scripted retry response.
    fn read_gateway_request(reader: &mut impl std::io::Read) -> std::io::Result<Value> {
        use std::io::{Error, ErrorKind};
        const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
        let mut bytes = Vec::new();
        let mut buffer = [0; 4096];
        let (header_end, length) = loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "gateway HTTP headers truncated",
                ));
            }
            bytes.extend_from_slice(&buffer[..n]);
            if bytes.len() > MAX_REQUEST_BYTES {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "gateway HTTP request too large",
                ));
            }
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                if !headers.starts_with("post /v1/responses ") {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "unexpected gateway HTTP route",
                    ));
                }
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .ok_or_else(|| {
                        Error::new(ErrorKind::InvalidData, "gateway Content-Length missing")
                    })?;
                break (end + 4, length);
            }
        };
        let total = header_end
            .checked_add(length)
            .filter(|total| *total <= MAX_REQUEST_BYTES)
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, "gateway HTTP request too large"))?;
        if bytes.len() < total {
            let received = bytes.len();
            bytes.resize(total, 0);
            reader.read_exact(&mut bytes[received..])?;
        }
        serde_json::from_slice(&bytes[header_end..total])
            .map_err(|error| Error::new(ErrorKind::InvalidData, error))
    }

    #[test]
    fn gateway_http_fixture_reads_fragmented_headers_and_large_bodies_completely() {
        use std::io::{Cursor, Read as _};
        let expected =
            json!({"input": [{"role": "user", "content": "fragmented 中 payload ".repeat(1500)}]});
        let body = serde_json::to_vec(&expected).unwrap();
        assert!(body.len() > 8192);
        let headers = format!(
            "POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let mut fragments = Cursor::new(&headers.as_bytes()[..17])
            .chain(Cursor::new(&headers.as_bytes()[17..headers.len() - 1]))
            .chain(Cursor::new(&headers.as_bytes()[headers.len() - 1..]))
            .chain(Cursor::new(&body[..13]))
            .chain(Cursor::new(&body[13..]));
        assert_eq!(read_gateway_request(&mut fragments).unwrap(), expected);
    }

    #[test]
    fn gateway_http_fixture_refuses_truncated_or_oversized_bodies() {
        let mut truncated = &b"POST /v1/responses HTTP/1.1\r\nContent-Length: 10\r\n\r\n{}"[..];
        assert_eq!(
            read_gateway_request(&mut truncated).unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        let mut oversized = &b"POST /v1/responses HTTP/1.1\r\nContent-Length: 2097152\r\n\r\n"[..];
        assert_eq!(
            read_gateway_request(&mut oversized).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn context_preflight_guards_real_native_http_dispatch() {
        use std::io::Write as _;
        use std::net::TcpListener;
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(cfg.join("model-catalog-direct.json"),
            r#"{"data":[{"id":"preflight-http/model","context_length":128000,"limits":{"max_output_tokens":2000}}]}"#).unwrap();
        let transcript = cfg.join("sylphx-transcript.jsonl");
        append_transcript_message(
            &transcript,
            &json!({"role":"assistant", "content":"old verbose history".repeat(12_000)}),
        )
        .unwrap();
        let huge_call = json!({"type":"function_call", "call_id":"completed", "name":"memory_recall", "arguments":"{}"});
        let huge_result = json!({"type":"function_call_output", "call_id":"completed", "output":"raw original 中\"".repeat(65_000)});
        append_transcript_message(&transcript, &huge_call).unwrap();
        append_transcript_message(&transcript, &huge_result).unwrap();
        append_transcript_message(
            &transcript,
            &json!({"type":"context_window_open", "handoff":"resume skills batch", "retained_items":[huge_call, huge_result]}),
        ).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let gateway = format!("http://{}/v1", listener.local_addr().unwrap());
        let transcript_at_dispatch = transcript.clone();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request = read_gateway_request(&mut stream).unwrap();
            assert!(
                request["input"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("context.window_opened")
            );
            assert!(
                std::fs::read_to_string(transcript_at_dispatch)
                    .unwrap()
                    .contains("context_window_open")
            );
            let retained_output = request["input"]
                .as_array()
                .unwrap()
                .iter()
                .find(|item| item["type"] == "function_call_output")
                .unwrap();
            assert_eq!(retained_output["call_id"], "completed");
            assert!(
                retained_output["output"]
                    .as_str()
                    .unwrap()
                    .contains("transcript:2-2")
            );
            assert!(
                !retained_output["output"]
                    .as_str()
                    .unwrap()
                    .contains("raw original")
            );
            assert_eq!(request["max_output_tokens"], 2000);
            assert!(request_token_upper_bound(&request).unwrap() + 2000 < 128_000);
            let body = r#"{"output":[],"usage":{"input_tokens":50,"output_tokens":0}}"#;
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).unwrap();
        });
        let state = Arc::new(Mutex::new(SharedState::default()));
        let cancel = Arc::new(AtomicBool::new(false));
        let env = vec![
            ("SYLPHX_GATEWAY_URL".into(), gateway),
            ("SYLPHX_GATEWAY_KEY".into(), "fixture-key".into()),
        ];
        let usage = run_native_gateway_turn(
            &state,
            &cancel,
            &cfg,
            &work,
            Some("preflight-http/model"),
            &env.clone().into(),
            Path::new("unused-mcp"),
            &[],
            "continue pending task #123",
        )
        .unwrap();
        assert_eq!(usage.input_tokens, 50);
        server.join().unwrap();
        assert!(
            load_pending_native_messages(&transcript, &load_transcript_input_items(&transcript))
                .is_empty()
        );
        // With irreducible pending input, production returns before opening the
        // HTTP connection. A listening socket proves this isn't a mock refusal.
        let refused = TcpListener::bind("127.0.0.1:0").unwrap();
        refused.set_nonblocking(true).unwrap();
        let env = vec![
            (
                "SYLPHX_GATEWAY_URL".into(),
                format!("http://{}/v1", refused.local_addr().unwrap()),
            ),
            ("SYLPHX_GATEWAY_KEY".into(), "fixture-key".into()),
        ];
        let irreducible = "irreducible pending task".repeat(20_000);
        let result = run_native_gateway_turn(
            &state,
            &cancel,
            &cfg,
            &work,
            Some("preflight-http/model"),
            &env.clone().into(),
            Path::new("unused-mcp"),
            &[],
            &irreducible,
        );
        assert!(result.unwrap_err().contains("request not sent"));
        assert_eq!(
            refused.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        // Exercise persistence through the real failed invocation, then a
        // restart/new inbound and another compaction. Completed prior inbound
        // must stay completed; the exact failed inbound must remain pending.
        let failed = json!({"role":"user", "content":irreducible});
        let next = json!({"role":"user", "content":"next short inbound"});
        append_transcript_message(&transcript, &next).unwrap();
        let mut restarted = load_transcript_input_items(&transcript);
        let pending = load_pending_native_messages(&transcript, &restarted);
        assert_eq!(pending, vec![failed.clone(), next.clone()]);
        prepare_budgeted_request(
            &transcript,
            &mut restarted,
            &pending,
            &context_preflight_limits(1_048_576),
            true,
            context_preflight_build,
        )
        .unwrap();
        // The newly enlarged fixture also retains its completed tool pair
        // when it fits. Only the failed/new inbound remains pending; completed
        // context may remain visible without being revived as pending work.
        assert!(restarted[restarted.len() - 2..] == [failed, next]);
        assert!(load_pending_native_messages(&transcript, &restarted) == pending);
        validate_responses_input(&restarted).unwrap();
    }

    #[test]
    fn context_preflight_counts_system_tools_output_and_refuses_irreducible_work_unsent() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("sylphx-transcript.jsonl");
        let pending = json!({"role":"user", "content":"must not disappear".repeat(20_000)});
        append_transcript_message(&transcript, &pending).unwrap();
        let mut input = vec![pending.clone()];
        let raw_before = std::fs::read(&transcript).unwrap();
        let input_before = input.clone();
        let result = prepare_budgeted_request(
            &transcript,
            &mut input,
            std::slice::from_ref(&pending),
            &context_preflight_limits(24_000),
            false,
            context_preflight_build,
        );
        let failure = result.unwrap_err();
        assert!(failure.contains("request not sent"));
        for _ in 0..3 {
            let repeated = prepare_budgeted_request(
                &transcript,
                &mut input,
                std::slice::from_ref(&pending),
                &context_preflight_limits(24_000),
                true,
                context_preflight_build,
            )
            .unwrap_err();
            assert_eq!(repeated, failure);
            assert_eq!(std::fs::read(&transcript).unwrap(), raw_before);
            assert_eq!(input, input_before);
        }
        assert!(input.contains(&pending));
        assert!(load_transcript_input_items(&transcript).contains(&pending));
        let body = build_native_responses_request(
            "test/model",
            &"x".repeat(50_000),
            &[],
            &[json!({"name":"big", "description":"y".repeat(50_000)})],
            "s",
            None,
        )
        .unwrap();
        assert!(request_token_upper_bound(&body).unwrap() >= 100_000);
        assert!(is_context_overflow(
            400,
            r#"{"error":{"message":"Invalid request: Your request exceeded model token limit: 1048576 (requested: 1274676)","type":"invalid_request_error"}}"#
        ));
        assert!(!is_context_overflow(429, "quota exceeded"));
    }

    #[test]
    fn native_transcript_file_reads_share_recall_cap_cursor_and_active_suppression() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        let transcript = cfg.join("sylphx-transcript.jsonl");
        let text = "raw file original 中文\n\"".repeat(4_000);
        let inbound = json!({"role":"user", "content":text});
        append_transcript_message(&transcript, &inbound).unwrap();
        append_transcript_message(
            &transcript,
            &json!({"type":"context_window_open", "handoff":"continue", "retained_items":[inbound]}),
        ).unwrap();
        let read = |args: &Value| {
            execute_sylphx_tool(
                "read_file",
                args,
                cfg,
                cfg,
                &workspace::Environment::default(),
                Path::new("unused"),
                &[],
            )
        };
        let mut args = json!({"path":"sylphx-transcript.jsonl", "start_line":1, "end_line":1});
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("sylphx-transcript.jsonl", cfg.join("raw-alias.jsonl"))
                .unwrap();
            args["path"] = json!("raw-alias.jsonl");
        }
        let active = read(&args);
        assert!(active.len() <= memory::MEMORY_RECALL_MAX_BYTES);
        assert!(active.contains("Already present in active context"));
        assert!(!active.contains("raw file original"));
        append_transcript_message(
            &transcript,
            &json!({"type":"context_window_open", "handoff":"evicted", "retained_items":[]}),
        )
        .unwrap();
        let mut combined = String::new();
        loop {
            let page = read(&args);
            assert!(page.len() <= memory::MEMORY_RECALL_MAX_BYTES);
            let parsed: Value = serde_json::from_str(&page).unwrap();
            assert_eq!(parsed["mode"], "page");
            combined.push_str(parsed["content"].as_str().unwrap());
            let Some(cursor) = parsed["next_cursor"].as_str() else {
                break;
            };
            args["cursor"] = json!(cursor);
        }
        let recovered: Value = serde_json::from_str(&combined).unwrap();
        assert!(recovered["items"][0]["content"] == format!("[user] {text}"));
        // Ordinary file reads retain their public line-numbered format.
        std::fs::write(cfg.join("plain.txt"), "unchanged normal read").unwrap();
        assert!(read(&json!({"path":"plain.txt"})).contains("   1: unchanged normal read"));
    }

    #[test]
    fn context_window_open_and_raw_rag_memory_recall_preserve_full_history() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        let transcript = cfg.join("sylphx-transcript.jsonl");

        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": "Deploy release v2.4.1 with token ALPHA_99182_KEY and 測試記憶檢索" }),
        ).unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call",
                "call_id": "call_1",
                "name": "bash",
                "arguments": "{\"command\":\"./deploy.sh --version v2.4.1\"}"
            }),
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "exit_code: 0\ndeployed commit sha 8f91b2c"
            }),
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "context_window_open",
                "handoff": "Goal: verify v2.4.1 rollout. Facts: deployed commit 8f91b2c.",
                "timestamp_ms": 1760000000000u64
            }),
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": "Now check production health" }),
        )
        .unwrap();

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
        // Exercise a real stride cut even when the configured window changes.
        let initial_items = memory::WINDOW_MAX_ITEMS + 5;
        for i in 0..initial_items {
            append_transcript_message(
                &transcript,
                &json!({ "role": "user", "content": format!("msg_{i}") }),
            )
            .unwrap();
        }
        let w1 = load_transcript_input_items(&transcript);
        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": format!("msg_{initial_items}") }),
        )
        .unwrap();
        let w2 = load_transcript_input_items(&transcript);
        // A cut occurred, it is stride-aligned, and appending one message does
        // not change the pinned prefix between consecutive stride boundaries.
        assert!(w1.len() <= memory::WINDOW_MAX_ITEMS);
        let first_index = w1[0]["content"]
            .as_str()
            .unwrap()
            .strip_prefix("msg_")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        assert!(first_index > 0);
        assert_eq!(first_index % memory::WINDOW_STRIDE_ITEMS, 0);
        assert_eq!(w1[0], w2[0]);
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
        assert_eq!(env_spec["ttl"], "1800s");
        assert_eq!(env_spec["idle_standby"], "1800s");
        assert!(env_spec.get("workspace_dir").is_none());
        assert!(env_spec.get("repo").is_none());
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
            &workspace::Environment::default(),
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
            &workspace::Environment::default(),
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
            &workspace::Environment::default(),
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
            &workspace::Environment::default(),
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
            &workspace::Environment::default(),
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
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call",
                "call_id": "call_deadend",
                "name": "grep",
                "arguments": "{\"pattern\":\"obsolete_symbol_xyz\"}"
            }),
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call_output",
                "call_id": "call_deadend",
                "output": "exit_code: 1\nno matches for obsolete_symbol_xyz"
            }),
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call",
                "call_id": "call_biglog",
                "name": "bash",
                "arguments": "{\"command\":\"cargo test\"}"
            }),
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call_output",
                "call_id": "call_biglog",
                "output": "exit_code: 0\n".to_string() + &"test line pass SECRET_BUILD_MARKER_777\n".repeat(100)
            }),
        ).unwrap();

        let inspect_raw = execute_sylphx_tool(
            "context_edit",
            &json!({ "action": "inspect" }),
            &cfg,
            &work,
            &workspace::Environment::default(),
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
            &workspace::Environment::default(),
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
            &workspace::Environment::default(),
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
            "# example/project\nWorkspace repository instructions.\n",
        )
        .unwrap();

        let prompt = assemble_system_prompt(&cfg, &work);
        assert!(prompt.contains("# Agent Identity"));
        assert!(prompt.contains("Two limits and operating defaults"));
        assert!(prompt.contains("# example/project"));
        assert_eq!(
            prompt.matches("Two limits and operating defaults").count(),
            1
        );
    }

    #[test]
    fn transient_503_model_unavailable_retries_and_persistent_503_surfaces_fault() {
        use std::io::Write as _;
        use std::net::TcpListener;
        use std::sync::atomic::AtomicUsize;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_cl = Arc::clone(&hits);

        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let _request = read_gateway_request(&mut stream).unwrap();
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

        let (activity_tx, activity_rx) = std::sync::mpsc::channel();
        harness.watch_activity(Arc::new(move || {
            let _ = activity_tx.send(());
        }));
        let wait_for_completion = |harness: &SylphxHarness| {
            // A progress ceiling, not a retry latency requirement. CI run
            // 42345118559 failed the count assertion after a 1.6 s poll limit.
            let deadline = Instant::now() + Duration::from_secs(30);
            while harness.health().turn_state != TurnState::Idle {
                assert!(
                    activity_rx
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                        .is_ok(),
                    "turn did not complete: health={:?}, HTTP requests={}",
                    harness.health(),
                    hits.load(Ordering::SeqCst),
                );
            }
        };

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
        wait_for_completion(&harness);
        assert_eq!(harness.turns_served(), 1);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
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
        wait_for_completion(&harness);
        assert_eq!(harness.turns_served(), 1);
        assert_eq!(hits.load(Ordering::SeqCst), 5);
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
    fn cloud_agent_tools_are_only_builtin_shell_and_files() {
        assert_eq!(
            build_cloud_agent_tools(),
            vec![
                json!({"builtin": {"id": "shell"}}),
                json!({"builtin": {"id": "files"}}),
            ]
        );
    }

    #[test]
    fn cloud_sessions_observe_completion_and_errors_without_host_callbacks() {
        use crate::masking_tests::{next_http_request, respond};
        use std::net::TcpListener;

        for (terminal, session_status, response_only, fails) in [
            ("", "idle", false, false),
            ("", "completed", true, false),
            ("session.idle", "running", false, false),
            ("turn.completed", "running", false, false),
            ("session.completed", "running", false, false),
            ("ERROR", "failed", false, true),
            ("RESPONSE", "failed", true, true),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let cfg = tmp.path().join("cfg");
            let work = tmp.path().join("work");
            std::fs::create_dir_all(&cfg).unwrap();
            std::fs::create_dir_all(&work).unwrap();
            std::fs::write(
                cfg.join("SYSTEM_PROMPT.md"),
                "LOCAL_ONLY_IDENTITY: use send and sandbox placement.",
            )
            .unwrap();
            std::fs::write(work.join("AGENTS.md"), "LOCAL_ONLY_INSTRUCTIONS").unwrap();
            std::fs::write(work.join("task.txt"), "Explicit task reference.").unwrap();
            std::fs::write(
                cfg.join("sylphx-agent.json"),
                r#"{"context_sources":["task.txt"]}"#,
            )
            .unwrap();
            let transcript = cfg.join("sylphx-transcript.jsonl");
            append_transcript_message(
                &transcript,
                &json!({"role":"assistant", "content":"LOCAL_ONLY_HISTORY"}),
            )
            .unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let stale_submission = terminal.is_empty() && session_status == "idle";
            if stale_submission {
                let binding = PersistedCloudSession {
                    agent_id: "old-agent".into(),
                    session_id: "old-session".into(),
                    model_id: "old-model".into(),
                    env_slug: "org/proj/dev".into(),
                    last_seq: 0,
                };
                save_persisted_cloud_session(&cfg, &binding).unwrap();
                write_atomic(&cfg.join("sylphx-cloud-turn-pending.json"), json!({
                    "session_path":cloud_session_resource_path(&binding.env_slug, &binding.session_id),
                    "message":"already settled work", "state":"submitting"
                }).to_string().as_bytes()).unwrap();
            }
            let server = std::thread::spawn(move || {
                if stale_submission {
                    let (stream, route, _) = next_http_request(&listener);
                    assert!(
                        route.starts_with("get ") && route.contains("old-session"),
                        "{route}"
                    );
                    respond(stream, json!({"state":"idle"}));
                    let (stream, route, _) = next_http_request(&listener);
                    assert!(route.contains("old-session:interrupt"), "{route}");
                    respond(stream, json!({}));
                    let (stream, route, _) = next_http_request(&listener);
                    assert!(
                        route.starts_with("get ") && route.contains("old-session"),
                        "{route}"
                    );
                    respond(stream, json!({"state":"idle"}));
                }
                let (stream, route, body) = next_http_request(&listener);
                respond(stream, json!({"name":"org/proj/dev/agents/a"}));
                assert!(
                    route.starts_with("post /v1/org/proj/dev/agents "),
                    "{route}"
                );
                assert_eq!(
                    body["spec"]["tools"],
                    json!([
                        {"builtin":{"id":"shell"}}, {"builtin":{"id":"files"}}
                    ])
                );
                let instructions = body["spec"]["instructions"].as_str().unwrap();
                for forbidden in [
                    "AnyAgents",
                    "LOCAL_ONLY_",
                    "send`",
                    "placement",
                    "memory_recall",
                    "activate_skill",
                ] {
                    assert!(!instructions.contains(forbidden), "{instructions}");
                }
                let (stream, route, _) = next_http_request(&listener);
                respond(stream, json!({"name":"org/proj/dev/agent_sessions/s"}));
                assert!(
                    route.starts_with("post /v1/org/proj/dev/agent_sessions "),
                    "{route}"
                );
                let (mut stream, route, _) = next_http_request(&listener);
                write!(
                    stream,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                )
                .unwrap();
                drop(stream);
                assert!(route.contains("/agents:capture-credential "), "{route}");
                let (stream, route, body) = next_http_request(&listener);
                respond(stream, json!({}));
                assert!(route.contains(":send "), "{route}");
                let text = body["message"]["parts"][0]["text"].as_str().unwrap();
                assert!(text.contains("Explicit task reference."));
                assert!(text.contains("Complete the cloud task."));
                assert!(!text.contains("already settled work"));
                assert!(!text.contains("LOCAL_ONLY_"));

                let calls = json!([
                    {"sequence":1,"turn":1,"type":"TOOL_CALL","tool_call":{"tool":"shell","client":false,"input":{"command":"touch cloud-callback-ran"}}},
                    {"sequence":2,"turn":1,"type":"TOOL_CALL","tool_call":{"tool":"send","client":true}},
                    {"sequence":3,"turn":1,"type":"TOOL_CALL","tool_call":{"id":"placement","tool":"sandbox","client":true,"input":{"action":"placement","placement":"local"}}},
                    {"sequence":4,"turn":1,"type":"TOOL_CALL","toolCall":{"id":"archive","tool":"archive","client":true,"input":{"agent":"self"}}}
                ]);
                let (stream, route, _) = next_http_request(&listener);
                respond(stream, json!({"session_events":calls}));
                assert!(route.contains("/session_events "), "{route}");
                let (stream, route, _) = next_http_request(&listener);
                // Idle state may be observed before its final output page is read.
                respond(
                    stream,
                    json!({"state":if terminal.is_empty() {"idle"} else {"running"},"last_event_sequence":if terminal.is_empty() {6} else {4}}),
                );
                assert!(
                    route.starts_with("get /v1/org/proj/dev/agent_sessions/s "),
                    "unexpected host callback: {route}"
                );

                let message = if response_only {
                    json!({"sequence":5,"turn":1,"type":"RESPONSE","response":{"text":"Cloud result 測試🙂","is_error":false}})
                } else {
                    json!({"sequence":5,"turn":1,"type":"AGENT_MESSAGE","message":{"parts":[{"text":"Cloud result 測試🙂"}]}})
                };
                let mut events = vec![
                    // A repeated event must not execute or duplicate output.
                    calls[3].clone(),
                    message,
                    json!({"sequence":6,"turn":1,"type":"USAGE","usage":{"input_tokens":"12","output_tokens":7,"cached_tokens":2}}),
                ];
                if !terminal.is_empty() {
                    events.push(if fails && response_only {
                        json!({"sequence":7,"turn":1,"type":"RESPONSE","response":{"is_error":true,"text":"cloud fixture failure"}})
                    } else if fails {
                        json!({"sequence":7,"turn":1,"type":"ERROR","error":{"message":"cloud fixture failure"}})
                    } else {
                        json!({"sequence":7,"turn":1,"type":terminal})
                    });
                }
                let (stream, route, _) = next_http_request(&listener);
                respond(stream, json!({"sessionEvents":events}));
                assert!(route.contains("sequence%20%3e%204"), "{route}");
                if !fails {
                    let (stream, route, _) = next_http_request(&listener);
                    respond(
                        stream,
                        json!({"state":session_status,"last_event_sequence":if terminal.is_empty() {6} else {7}}),
                    );
                    assert!(
                        route.starts_with("get /v1/org/proj/dev/agent_sessions/s "),
                        "{route}"
                    );
                }
            });
            let env = vec![
                ("SYLPHX_API_URL".into(), base),
                ("SYLPHX_API_KEY".into(), "fixture-key".into()),
            ]
            .into();
            let result = run_cloud_session_turn(
                &Arc::new(Mutex::new(SharedState::default())),
                &Arc::new(AtomicBool::new(false)),
                &cfg,
                &work,
                Some("cloud-fixture/model"),
                &env,
                Path::new("unused-mcp"),
                &[],
                "org/proj/dev",
                "Complete the cloud task.",
            );
            server.join().unwrap();
            assert!(!work.join("cloud-callback-ran").exists());
            assert!(!cfg.join("sylphx-workspace-placement.json").exists());
            assert!(!cfg.join("sylphx-cloud-tool-receipt.json").exists());
            let stored = std::fs::read_to_string(&transcript).unwrap();
            assert!(!stored.contains("function_call"));
            if fails {
                assert!(result.unwrap_err().contains("cloud fixture failure"));
            } else {
                let usage = result.unwrap();
                assert_eq!(
                    (
                        usage.input_tokens,
                        usage.output_tokens,
                        usage.cache_read_tokens
                    ),
                    (12, 7, 2)
                );
                let items = load_transcript_input_items(&transcript);
                let output = items.last().unwrap();
                assert_eq!(output["role"], "assistant");
                assert_eq!(output["content"][0]["text"], "Cloud result 測試🙂");
                assert!(!cfg.join("sylphx-cloud-turn-pending.json").exists());
                assert_eq!(
                    load_persisted_cloud_session(&cfg)
                        .unwrap()
                        .unwrap()
                        .last_seq,
                    if terminal.is_empty() { 6 } else { 7 }
                );
            }
        }
    }

    #[test]
    fn skills_index_preserves_full_agentskills_frontmatter_and_negative_triggers() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().join("work");
        let cfg = tmp.path().join("cfg");
        let skill_dir = work.join(".agents/skills/self-challenge");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::create_dir_all(&cfg).unwrap();
        let long_desc = "First-principles reasoning and disconfirmation check before non-trivial decisions. Use when designing architecture, diagnosing a root cause, or claiming an outcome is complete. Use `prove-it` for concrete verification commands; Do NOT use when answering a trivial factual lookup.";
        std::fs::write(
            skill_dir.join("SKILL.md"),
            format!("---\nname: self-challenge\ndescription: \"{long_desc}\"\n---\n# Body\n"),
        )
        .unwrap();

        let index = discover_skills_index(&work, &cfg);
        assert!(index.contains("MUST call `activate_skill`"));
        assert!(
            index.contains("Use when designing architecture"),
            "positive trigger clause must not be truncated: {index}"
        );
        assert!(
            index.contains("Do NOT use when answering a trivial factual lookup."),
            "negative disambiguation clause must not be truncated: {index}"
        );
    }

    #[test]
    fn sandbox_and_shell_tools_cannot_switch_execution_placement() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        let env: workspace::Environment = vec![
            ("SYLPHX_API_KEY".to_string(), "sylphx_sk_test".to_string()),
            ("SYLPHX_AGENT_ENV".to_string(), "org/proj/dev".to_string()),
        ]
        .into();
        let definitions = build_legacy_tool_definitions();
        let schema = &definitions
            .iter()
            .find(|tool| tool["function"]["name"] == "sandbox")
            .unwrap()["function"]["parameters"];
        assert_eq!(schema["properties"]["action"]["enum"], json!(["status"]));
        assert!(schema["properties"].get("placement").is_none());
        let bash = build_tool_definitions()
            .into_iter()
            .find(|tool| tool["function"]["name"] == "bash")
            .unwrap();
        assert!(
            bash["function"]["parameters"]["properties"]
                .get("placement")
                .is_none()
        );

        for initial_placement in ["cloud", "local"] {
            let placement_path = cfg.join("sylphx-workspace-placement.json");
            let original = json!({"placement": initial_placement}).to_string();
            std::fs::write(&placement_path, &original).unwrap();
            let status = execute_sylphx_tool(
                "sandbox",
                &json!({"action":"status"}),
                &cfg,
                &work,
                &env,
                Path::new("unused-mcp"),
                &[],
            );
            assert_eq!(
                serde_json::from_str::<Value>(&status).unwrap()["placement"],
                initial_placement
            );
            for target in ["local", "cloud"] {
                for args in [
                    json!({"action":"placement", "placement":target}),
                    json!({"action":"switch", "placement":target}),
                    json!({"action":"set", "placement":target}),
                    json!({"placement":target}),
                    json!({"action":"status", "placement":target}),
                ] {
                    for execute in [execute_sylphx_tool, execute_sylphx_tool_raw] {
                        let out = execute(
                            "sandbox",
                            &args,
                            &cfg,
                            &work,
                            &env,
                            Path::new("unused-mcp"),
                            &[],
                        );
                        assert!(out.starts_with("error:"), "{args}: {out}");
                    }
                }
                for name in ["bash", "shell"] {
                    let out = execute_sylphx_tool(
                        name,
                        &json!({"command":"touch should-not-execute", "placement":target}),
                        &cfg,
                        &work,
                        &env,
                        Path::new("unused-mcp"),
                        &[],
                    );
                    assert!(out.starts_with("error:"), "{out}");
                }
            }
            assert_eq!(std::fs::read_to_string(&placement_path).unwrap(), original);
            assert!(!work.join("should-not-execute").exists());
        }
    }

    #[test]
    fn chat_completions_roundtrip_converts_tool_calls_and_reasoning_content() {
        let input = vec![
            json!({ "role": "user", "content": "Run uname" }),
            json!({
                "type": "function_call",
                "call_id": "call_1",
                "name": "bash",
                "arguments": "{\"command\":\"uname -s\"}",
                "reasoning_content": "Need OS name"
            }),
            json!({
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "Linux\n"
            }),
        ];
        let req = build_chat_completions_request("k3", "System instructions", &input, None)
            .expect("chat completions request");
        let msgs = req["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[2]["role"], "assistant");
        assert_eq!(msgs[2]["reasoning_content"], "Need OS name");
        assert_eq!(msgs[2]["tool_calls"][0]["id"], "call_1");
        assert_eq!(msgs[3]["role"], "tool");
        assert_eq!(msgs[3]["tool_call_id"], "call_1");

        let chat_resp = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "The OS is Linux.",
                    "reasoning_content": "Verified output",
                    "tool_calls": [{
                        "id": "call_2",
                        "type": "function",
                        "function": {
                            "name": "send",
                            "arguments": "{\"to\":\"parent\",\"text\":\"Linux\"}"
                        }
                    }]
                }
            }],
            "usage": {
                "prompt_tokens": 120,
                "completion_tokens": 30
            }
        });
        let env_resp = chat_completions_to_responses_envelope(&chat_resp).unwrap();
        let output = native_responses_output(&env_resp).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0]["type"], "message");
        assert_eq!(output[1]["type"], "function_call");
        assert_eq!(output[1]["call_id"], "call_2");
        assert_eq!(output[1]["reasoning_content"], "Verified output");
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
[[ "$1" == --reuid && "$2" == 24031 ]]
[[ "$3" == --regid && "$4" == 24032 ]]
[[ "$5" == --groups && "$6" == 24032,24033 ]]
[[ "$7" == --no-new-privs && "$8" == -- ]]
[[ -z ${ANYAGENTS_SEAT_LAUNCH+x} ]]
shift 8
printf '%s\n' "$1" >> "$WORKSPACE_TEST_LAUNCH_LOG"
if [[ ${WORKSPACE_TEST_REFUSE:-0} == 1 ]]; then
    echo 'fixture launcher refused' >&2
    exit 73
fi
# This unit double proves routing only. The required native-UID fixture
# executes the admission script with real setpriv instead.
if [[ "$1" == /bin/sh && ( "${2:-}" == -c || "${2:-}" == -s ) ]]; then exit 0; fi
exec "$@"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o700)).unwrap();
        (
            vec![
                ("ANYAGENTS_DATA".into(), root.display().to_string()),
                (
                    anyagents_harness::SEAT_LAUNCH_ENV.into(),
                    format!("{}\t24031\t24032\t24032,24033", launcher.display()),
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
            execute_sylphx_tool(
                name,
                &args,
                &cfg,
                &work,
                &env.clone().into(),
                Path::new("unused-mcp"),
                &[],
            )
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
    fn already_started_harness_refreshes_same_seat_peers_for_each_workspace_command() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().canonicalize().unwrap();
        let config = data.join("agents/own/config");
        let work = data.join("work");
        for path in [
            &config,
            &work,
            &data.join("agents/own/memory"),
            &data.join("agents/own/skills"),
        ] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(config.join("mcp-capability"), "synthetic").unwrap();
        let uid = std::fs::metadata(&config)
            .unwrap()
            .uid()
            .wrapping_add(1)
            .max(1);
        let gid = uid.wrapping_add(1).max(1);
        let launcher = data.join("fixture-launcher");
        // Only process identity/procfs and GNU/BSD stat differences are mocked.
        // The complete production admission script still performs real peer
        // access tests on the actual directories; this does not prove setpriv.
        std::fs::write(
            &launcher,
            format!(
                r#"#!/bin/sh
set -eu
shift 8
if [ "$1" = /bin/sh ] && [ "$2" = -s ]; then
    prefix='id() {{ case "$1" in -u) echo {uid};; -g) echo {gid};; esac; }}
awk() {{ echo 1; }}
stat() {{ echo 640; }}
'
    script=$(cat)
    exec /bin/sh -c "$prefix$script"
fi
exec "$@"
"#
            ),
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o700)).unwrap();
        let values = vec![
            ("ANYAGENTS_DATA".into(), data.display().to_string()),
            (
                "ANYAGENTS_AGENT_CONFIG".into(),
                config.display().to_string(),
            ),
            (
                anyagents_harness::SEAT_LAUNCH_ENV.into(),
                format!("{}\t{uid}\t{gid}\t{gid}", launcher.display()),
            ),
            (anyagents_harness::SEAT_PEERS_ENV.into(), "[]".into()),
        ];
        let pairs: Vec<_> = values
            .iter()
            .map(|(k, v): &(String, String)| (k.as_str(), v.as_str()))
            .collect();
        let mut harness =
            SylphxHarness::start(&config, &work, None, None, &pairs, Path::new("unused"), &[])
                .unwrap();
        let peers = Arc::new(Mutex::new(Vec::<anyagents_harness::SeatPeer>::new()));
        let live = peers.clone();
        harness.set_seat_peers_resolver(move || {
            serde_json::to_string(&*live.lock().unwrap()).map_err(|e| e.to_string())
        });
        // The running turn retains its environment clone before child creation.
        let turn_env = harness.env.clone();
        let run = |environment: &workspace::Environment| {
            execute_sylphx_tool(
                "bash",
                &json!({"command":"printf admitted"}),
                &config,
                &work,
                environment,
                Path::new("unused"),
                &[],
            )
        };
        assert!(run(&turn_env).contains("admitted"));
        let child = data.join("agents/late-child");
        for name in ["config", "memory", "skills"] {
            let path = child.join(name);
            std::fs::create_dir_all(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o2770)).unwrap();
        }
        std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o2770)).unwrap();
        assert_eq!(std::fs::metadata(&child).unwrap().mode() & 0o7777, 0o2770);
        let stale = workspace::Environment::from(values);
        assert!(run(&stale).contains("peer seats' private trees are denied"));
        peers.lock().unwrap().push(anyagents_harness::SeatPeer {
            root: child.clone(),
            uid,
            gid,
        });
        assert!(
            run(&turn_env).contains("admitted"),
            "refresh must reach an already-running turn"
        );
        for shared_principal in [(uid + 1, gid), (uid, gid + 1)] {
            let mut list = peers.lock().unwrap();
            list[0].uid = shared_principal.0;
            list[0].gid = shared_principal.1;
            drop(list);
            assert!(run(&turn_env).contains("admitted"));
        }
        peers.lock().unwrap()[0].uid = uid + 1;
        peers.lock().unwrap()[0].gid = gid + 1;
        assert!(run(&turn_env).contains("peer seats' private trees are denied"));
        peers.lock().unwrap()[0] = anyagents_harness::SeatPeer {
            root: child,
            uid,
            gid,
        };
        assert!(run(&turn_env).contains("admitted"));
        // Directory group/mode alone cannot admit an unregistered peer.
        let foreign = data.join("agents/unregistered/memory");
        std::fs::create_dir_all(&foreign).unwrap();
        std::fs::set_permissions(&foreign, std::fs::Permissions::from_mode(0o2770)).unwrap();
        assert!(run(&turn_env).contains("peer seats' private trees are denied"));
        harness.set_seat_peers_resolver(|| Err("synthetic authority unavailable".into()));
        let refused = execute_sylphx_tool(
            "write_file",
            &json!({"path":"must-not-exist", "content":"forbidden"}),
            &config,
            &work,
            &harness.env,
            Path::new("unused"),
            &[],
        );
        assert!(
            refused.contains("synthetic authority unavailable"),
            "{refused}"
        );
        assert!(!work.join("must-not-exist").exists());
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
            execute_sylphx_tool(
                name,
                &args,
                &cfg,
                &work,
                &env.clone().into(),
                Path::new("unused-mcp"),
                &[],
            )
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
            execute_sylphx_tool(
                name,
                &args,
                &cfg,
                &work,
                &env.to_vec().into(),
                Path::new("unused-mcp"),
                &[],
            )
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
            "{}\t24031\t24032\t24032,24033",
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
        for malformed in ["", "bad\tspec", "\t24031\t24032\t24032,24033"] {
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
        save_persisted_cloud_session(cfg, &saved).unwrap();
        let loaded = load_persisted_cloud_session(cfg)
            .unwrap()
            .expect("persisted cloud session");
        assert_eq!(loaded.agent_id, saved.agent_id);
        assert_eq!(loaded.session_id, saved.session_id);
        assert_eq!(loaded.model_id, "openai/gpt-6.1-sol");
        assert_eq!(loaded.last_seq, 14);

        let transcript = cfg.join("sylphx-transcript.jsonl");
        append_transcript_message(
            &transcript,
            &json!({ "role": "user", "content": "Check deployment status of service-alpha" }),
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "function_call",
                "call_id": "call_99",
                "name": "bash",
                "arguments": "{\"command\":\"kubectl get pods\"}"
            }),
        )
        .unwrap();
        append_transcript_message(
            &transcript,
            &json!({
                "type": "message",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": "service-alpha is healthy (3/3 pods ready)." }]
            }),
        ).unwrap();

        let block = build_prior_transcript_continuity_block(&transcript)
            .expect("prior transcript continuity block");
        assert!(block.contains("<prior_session_continuity>"));
        assert!(block.contains("Check deployment status of service-alpha"));
        assert!(block.contains("[tool_call:bash]"));
        assert!(block.contains("service-alpha is healthy (3/3 pods ready)."));

        clear_persisted_cloud_session(cfg).unwrap();
        assert!(load_persisted_cloud_session(cfg).unwrap().is_none());

        let sdk_session = SylphxSdkSession::new(
            cfg,
            cfg,
            Some("sylphx/openai/gpt-6.1-sol#medium".to_string()),
            vec![("SYLPHX_EXECUTION_MODE".to_string(), "local".to_string())],
        )
        .unwrap();
        assert_eq!(sdk_session.execution_mode, SylphxExecutionMode::LocalOnly);
        assert!(NoopClientTools.tool_definitions().is_empty());
        assert!(NoopClientTools.execute("send", &json!({})).is_none());

        let receipt = sdk_session.pull_from_cloud().expect("pull from cloud");
        assert_eq!(receipt.direction, "cloud_to_local");
        assert_eq!(receipt.mode, "unchanged");
        assert!(!receipt.placement_changed);
        assert_eq!(receipt.transcript_items, 3);

        // When `sylphx-workspace-placement.json` specifies `"placement": "local"`,
        // `resolve_cloud_agent_environment` returns `None` and purges any stale
        // `sylphx-cloud-session.json` / `sylphx-sandbox-lease.json`.
        save_persisted_cloud_session(cfg, &saved).unwrap();
        std::fs::write(cfg.join("sylphx-sandbox-lease.json"), "{}").unwrap();
        std::fs::write(
            cfg.join("sylphx-workspace-placement.json"),
            r#"{"placement":"local"}"#,
        )
        .unwrap();
        assert_eq!(resolve_cloud_agent_environment(cfg, &[]).unwrap(), None);
        assert!(load_persisted_cloud_session(cfg).unwrap().is_some());
        assert!(cfg.join("sylphx-sandbox-lease.json").exists());
        assert_eq!(
            SylphxExecutionMode::from_env(cfg, &[]).unwrap(),
            SylphxExecutionMode::LocalOnly
        );
    }

    #[test]
    fn assemble_system_prompt_replaces_stale_embedded_agents_md_without_duplication() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let cfg = data_dir.join("agents/agt_test/config");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();

        let sys_doc = "# AnyAgents Identity & Environment Awareness\n\n\
                       ## 1. Your Identity (Who You Are)\n\n\
                       - **Agent Name**: `nova`\n\n\
                       ## 4. Shared Instructions (AGENTS.md)\n\n\
                       # example/project\n\n\
                       Old stale rule v1.\n\n\
                       # Cross-Harness Continuity\n\n\
                       Prior turn summary.";
        std::fs::write(cfg.join("SYSTEM_PROMPT.md"), sys_doc).unwrap();
        std::fs::write(cfg.join("AGENTS.md"), sys_doc).unwrap();
        std::fs::write(
            data_dir.join("AGENTS.md"),
            "# example/project\n\nShared rule v2.\n",
        )
        .unwrap();
        std::fs::write(
            work.join("AGENTS.md"),
            "# example/project\n\nLive workspace rule v3.\n",
        )
        .unwrap();

        let assembled = assemble_system_prompt(&cfg, &work);
        assert_eq!(
            assembled.matches("# example/project").count(),
            2,
            "must retain shared and workspace scope: {assembled}"
        );
        assert!(assembled.contains("Live workspace rule v3."));
        assert!(!assembled.contains("Old stale rule v1."));
        assert!(assembled.contains("Shared rule v2."));
        assert!(assembled.contains("# Cross-Harness Continuity"));
        assert!(assembled.contains("Prior turn summary."));

        // Also verify production `## 4. Cross-Harness Session Continuity` header is preserved
        // and `resolve_agent_short_name` extracts `nova` instead of `agt_test`.
        let sys_doc_v2 = "# AnyAgents Identity & Environment Awareness\n\n\
                          ## 1. Your Identity (Who You Are)\n\n\
                          - **Agent Name**: `nova` (addressable as `agent:nova`)\n\n\
                          ## 4. Shared Instructions (AGENTS.md)\n\n\
                          # example/project\n\n\
                          Old stale rule v1.\n\n\
                          ## 4. Cross-Harness Session Continuity (`codex` -> `sylphx`)\n\n\
                          Recent completed turn digest.";
        std::fs::write(cfg.join("SYSTEM_PROMPT.md"), sys_doc_v2).unwrap();
        let assembled_v2 = assemble_system_prompt(&cfg, &work);
        assert!(assembled_v2.contains("## 4. Cross-Harness Session Continuity"));
        assert!(assembled_v2.contains("Recent completed turn digest."));
        assert_eq!(
            resolve_agent_short_name(&cfg, &[("ANYAGENTS_AGENT_NAME".into(), "nova".into())]),
            "nova"
        );
        assert_eq!(
            resolve_agent_short_name(
                &cfg,
                &[("ANYAGENTS_AGENT_NAME".to_string(), "worker-b".to_string())]
            ),
            "worker-b"
        );

        let norm1 = normalize_mcp_tool_arguments(
            "describe",
            &json!({ "agent": "self", "instruction": "New private SOP." }),
        );
        assert_eq!(norm1["instructions"], "New private SOP.");
        let norm2 = normalize_mcp_tool_arguments(
            "agents",
            &json!({ "action": "spawn", "system_prompt": "Custom prompt." }),
        );
        assert_eq!(norm2["instructions"], "Custom prompt.");

        let defs = build_tool_definitions();
        let names: Vec<&str> = defs
            .iter()
            .filter_map(|t| t.pointer("/function/name").and_then(Value::as_str))
            .collect();
        assert_eq!(names, vec!["code", "bash", "context"]);

        let empty_env = workspace::Environment::default();
        let ctx_goal = execute_sylphx_tool(
            "context",
            &json!({
                "action": "goal",
                "goal_action": "create",
                "title": "Ship Pure Code Mode",
                "acceptance_criteria": ["CANONICAL_MCP_TOOLS = [\"code\"]"]
            }),
            &cfg,
            &work,
            &empty_env,
            Path::new("unused-mcp"),
            &[],
        );
        assert!(ctx_goal.contains("Ship Pure Code Mode"), "{ctx_goal}");

        let ctx_compact = execute_sylphx_tool(
            "context",
            &json!({
                "action": "compact",
                "handoff": "Goal: verify compact window."
            }),
            &cfg,
            &work,
            &empty_env,
            Path::new("unused-mcp"),
            &[],
        );
        assert!(ctx_compact.contains("window_opened"), "{ctx_compact}");
    }
}

fn validate_goal(value: &Value) -> Result<(), String> {
    match value.get("state").and_then(Value::as_str) {
        Some("none" | "active" | "completed" | "paused" | "cancelled") => {}
        _ => return Err("invalid goal state".into()),
    }
    if value.get("auto_continuations").is_some_and(|n| !n.is_u64()) {
        return Err("invalid goal continuation counter".into());
    }
    Ok(())
}

pub(crate) fn interrupt_and_settle(base: &str, path: &str, key: &str) -> Result<Value, String> {
    post_required(&format!("{base}/v1/{path}:interrupt"), key, &json!({}))?;
    for _ in 0..20 {
        let (status, raw) = curl_get(&format!("{base}/v1/{path}"), Some(key))?;
        if !(200..300).contains(&status) {
            return Err(format!("cloud settle HTTP {status}; binding retained"));
        }
        let value: Value =
            serde_json::from_str(&raw).map_err(|e| format!("invalid settle response: {e}"))?;
        let state = value
            .get("state")
            .or_else(|| value.get("status"))
            .and_then(Value::as_str)
            .ok_or("invalid cloud settle state; binding retained")?;
        if matches!(
            state.to_ascii_lowercase().as_str(),
            "idle" | "session_state_idle" | "completed" | "terminated" | "failed"
        ) {
            return Ok(value);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err("cloud session did not settle; binding retained".into())
}

fn validate_tool_input(name: &str, args: &Value) -> Result<(), String> {
    if !args.is_object() {
        return Err("tool arguments must be an object".into());
    }
    let name = name.strip_prefix("mcp__anyagents__").unwrap_or(name);
    if let Some(tool) = build_tool_definitions()
        .into_iter()
        .chain(build_legacy_tool_definitions())
        .find(|tool| tool.pointer("/function/name").and_then(Value::as_str) == Some(name))
    {
        validate_schema(args, &tool["function"]["parameters"], name)?;
    }
    Ok(())
}

fn reject_legacy_cloud_tool_receipt(config_dir: &Path) -> Result<(), String> {
    if read_json(&config_dir.join("sylphx-cloud-tool-receipt.json"))?.is_some() {
        return Err("legacy cloud client-tool receipt requires operator reconciliation; host tool callbacks are no longer supported".into());
    }
    Ok(())
}

fn reconcile_cloud_submission(
    config_dir: &Path,
    base: &str,
    key: &str,
    message: &str,
) -> Result<bool, String> {
    let path = config_dir.join("sylphx-cloud-turn-pending.json");
    let Some(pending) = read_json(&path)? else {
        return Ok(false);
    };
    let state = required_text(&pending, "state")?;
    if !matches!(state, "submitting" | "accepted") {
        return Err("invalid retained cloud submission state; receipt retained".into());
    }
    let session = required_text(&pending, "session_path")?;
    let binding = load_persisted_cloud_session(config_dir)?
        .ok_or("retained cloud submission has no binding")?;
    if cloud_session_resource_path(&binding.env_slug, &binding.session_id) != session {
        return Err("retained cloud submission belongs to a different session".into());
    }
    let (status, raw) = curl_get(&format!("{base}/v1/{session}"), Some(key))?;
    if !(200..300).contains(&status) {
        return Err(format!(
            "cloud submission reconciliation HTTP {status}; receipt retained"
        ));
    }
    let remote: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    let settled = remote
        .get("state")
        .or_else(|| remote.get("status"))
        .and_then(Value::as_str)
        .is_some_and(|s| {
            matches!(
                s.to_ascii_lowercase().as_str(),
                "idle"
                    | "session_state_idle"
                    | "completed"
                    | "interrupted"
                    | "terminated"
                    | "failed"
                    | "session_state_failed"
            )
        });
    if !settled {
        return Err(
            "cloud turn has an unresolved retained submission; remote session is not settled"
                .into(),
        );
    }
    // Preserve the original request and readback as an audit/recovery receipt.
    // This path only observes; it never replays a send, tool result or host effect.
    let same_message = required_text(&pending, "message")? == message;
    let digest = anyagents_core::learned_memory::sha256(pending.to_string().as_bytes());
    write_atomic(
        &config_dir.join(format!("sylphx-cloud-submission-settled-{digest}.json")),
        json!({"submission":pending, "remote":remote})
            .to_string()
            .as_bytes(),
    )?;
    remove_optional(&path)?;
    Ok(same_message)
}

fn write_terminal_frame(out: &mut impl std::io::Write, frame: &Value) -> std::io::Result<()> {
    writeln!(out, "{frame}")?;
    out.flush()
}

fn load_explicit_context(config_dir: &Path, work_dir: &Path) -> Result<String, String> {
    let mut context = String::new();
    if let Some(settings) = read_json(&config_dir.join("sylphx-agent.json"))?
        && let Some(sources) = settings.get("context_sources")
    {
        let sources = sources
            .as_array()
            .ok_or("context_sources must be an array of workspace-relative files")?;
        let workspace = work_dir
            .canonicalize()
            .map_err(|e| format!("context workspace: {e}"))?;
        for source in sources {
            let source = source
                .as_str()
                .ok_or("context source must be a path string")?;
            let relative = Path::new(source);
            if relative.is_absolute()
                || relative
                    .components()
                    .any(|c| c == std::path::Component::ParentDir)
            {
                return Err("context source must be workspace-relative".into());
            }
            let path = workspace
                .join(relative)
                .canonicalize()
                .map_err(|e| format!("context source: {e}"))?;
            if !path.starts_with(&workspace) || !path.is_file() {
                return Err("context source must be a file owned by the selected workspace".into());
            }
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("context source {}: {e}", path.display()))?;
            context.push_str(&format!(
                "\n<context_source path={source:?}>\n{text}\n</context_source>"
            ));
        }
    }
    Ok(context)
}

#[cfg(test)]
mod audit_tests;
