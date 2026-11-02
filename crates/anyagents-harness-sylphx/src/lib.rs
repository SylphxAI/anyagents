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
            mcp_command: mcp_command.to_path_buf(),
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
        for (k, v) in &self.env {
            if k == "SYLPHX_AGENT_ENV" && !v.trim().is_empty() {
                return Some(v.trim().to_string());
            }
        }
        if let Ok(v) = std::env::var("SYLPHX_AGENT_ENV")
            && !v.trim().is_empty()
        {
            return Some(v.trim().to_string());
        }
        let ctx_path = self.config_dir.join(".config/sylphx/context.json");
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
            return Some(env_slug.to_string());
        }
        None
    }
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
                        record_cloud_sandbox_fallback(&config_dir, &cloud_err);
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
                            g.dead_reason = Some(err_msg.clone());
                        }
                        persist_turn_error(&config_dir, &err_msg);
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

fn persist_turn_error(config_dir: &Path, err_msg: &str) {
    let err_path = config_dir.join(".anyagents-last-error.json");
    let prev_count = std::fs::read_to_string(&err_path)
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .and_then(|v| v.get("consecutive_turn_errors").and_then(Value::as_u64))
        .unwrap_or(0);
    let doc = json!({
        "error": err_msg,
        "timestamp_ms": anyagents_core::now_ms(),
        "consecutive_turn_errors": prev_count + 1,
    });
    let _ = std::fs::write(&err_path, format!("{doc}\n"));
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
                "client": {},
            }))
        })
        .collect()
}

fn build_cloud_agent_tools() -> Vec<Value> {
    let mut tools = vec![
        json!({
            "name": "shell",
            "builtin": { "id": "shell" }
        }),
        json!({
            "name": "files",
            "builtin": { "id": "files" }
        }),
    ];
    tools.extend(build_cloud_agent_client_tools());
    tools
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
    let base = resolve_cloud_api_url(env);
    let canonical_env = normalize_cloud_env_slug(env_slug);
    let (model_id, _) = parse_sylphx_model_and_effort(model);
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");

    let (mut agent_id, mut session_id) = {
        let g = state.lock().unwrap_or_else(PoisonError::into_inner);
        (g.cloud_agent_id.clone(), g.cloud_session_id.clone())
    };
    let mut initial_last_seq: i64 = 0;

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
                    if status == "failed"
                        || status == "session_state_failed"
                        || status == "terminated"
                        || status == "errored"
                    {
                        clear_persisted_cloud_session(config_dir);
                        session_id = None;
                        initial_last_seq = 0;
                    } else if status.contains("running")
                        || status.contains("working")
                        || status.contains("waiting")
                    {
                        let interrupt_url = format!("{base}/v1/{sess_path}:interrupt");
                        let _ = curl_post_json(&interrupt_url, Some(&api_key), &json!({}));
                    }
                }
            }
            Ok((404, _)) => {
                clear_persisted_cloud_session(config_dir);
                agent_id = None;
                session_id = None;
                initial_last_seq = 0;
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
        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
        g.cloud_session_id = Some(sid.clone());
        g.session_id = Some(sid.clone());
        let _ = std::fs::write(config_dir.join("sylphx-session-id"), format!("{sid}\n"));
    }

    let aid = agent_id.unwrap_or_default();
    let sid = session_id.unwrap_or_default();
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

    let sess_path = cloud_session_resource_path(&canonical_env, &sid);
    let send_url = format!("{base}/v1/{sess_path}:send");
    let send_body = json!({
        "message": {
            "parts": [{ "text": user_message }]
        }
    });
    let (st, raw) = curl_post_json(&send_url, Some(&api_key), &send_body)?;
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
    let stall_timeout_ms = env
        .iter()
        .find(|(k, v)| k == "SYLPHX_CLOUD_SESSION_STALL_MS" && !v.trim().is_empty())
        .and_then(|(_, v)| v.trim().parse::<u64>().ok())
        .or_else(|| {
            std::env::var("SYLPHX_CLOUD_SESSION_STALL_MS")
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
        })
        .unwrap_or(20_000);
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
                    if seq > baseline_seq + 2 {
                        last_progress = Instant::now();
                    }
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
                    let is_client = tc.get("client").and_then(Value::as_bool).unwrap_or(true);
                    if !call_id.is_empty() && is_client && handled_call_ids.insert(call_id.clone())
                    {
                        let tool_name = tc
                            .get("tool")
                            .or_else(|| tc.get("name"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
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
                } else if ev_type.contains("AGENT_MESSAGE") || ev_type.contains("RESPONSE") {
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
                    } else if let Some(t) = ev
                        .pointer("/response/text")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        assistant_texts.push(t.to_string());
                    }
                }
            }
        }

        if self_archived {
            release_persisted_sandbox_lease(config_dir);
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
                let in_tok = val
                    .get("total_input_tokens")
                    .or_else(|| val.pointer("/usage/input_tokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let out_tok = val
                    .get("total_output_tokens")
                    .or_else(|| val.pointer("/usage/output_tokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let cache_read_tok = val
                    .get("total_cache_read_tokens")
                    .or_else(|| val.pointer("/usage/input_tokens_details/cached_tokens"))
                    .or_else(|| val.pointer("/usage/cache_read_tokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
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
    let is_recovery_on_unanswered_tail = initial_user_message
        .contains("[Recovery] Your previous turn was interrupted")
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

    for step in 0..48 {
        if cancel_flag.load(Ordering::SeqCst) {
            break;
        }
        drain_steered_messages(state, &transcript_path, &mut input_items);

        let mut req_body = json!({
            "model": model_id,
            "instructions": system_prompt,
            "input": input_items,
            "tools": tools,
            "prompt_cache_key": session_cache_key,
        });
        if let Some(ref effort) = reasoning_effort
            && let Some(obj) = req_body.as_object_mut()
        {
            obj.insert("reasoning".to_string(), json!({ "effort": effort }));
        }

        let step_t0 = Instant::now();
        let (status, raw) = curl_post_json(&responses_url, Some(&api_key), &req_body)?;
        let step_elapsed_ms = step_t0.elapsed().as_millis() as u64;
        if !(200..300).contains(&status) {
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

        let Some(output_arr) = resp.get("output").and_then(Value::as_array).cloned() else {
            return Err("Sylphx responses API missing `output` array".to_string());
        };

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
                    let tool_out_item = json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": output,
                    });
                    append_transcript_message(&transcript_path, &tool_out_item);
                    input_items.push(tool_out_item);
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
                release_persisted_sandbox_lease(config_dir);
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
    let mcp_command = config_val
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
        .unwrap_or_else(|| std::env::current_exe().unwrap_or_else(|_| PathBuf::from("anyagents")));
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
            let cloud_env = env_cl
                .iter()
                .find(|(k, v)| k == "SYLPHX_AGENT_ENV" && !v.trim().is_empty())
                .map(|(_, v)| v.trim().to_string());
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
                        record_cloud_sandbox_fallback(&cfg_cl, &cloud_err);
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
                "description": "Manage durable timers, intervals, cron jobs, and watches (`list`, `create`, `pause`, `resume`, `cancel`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string" },
                        "id": { "type": "string" },
                        "kind": { "type": "string", "description": "`after`, `every`, `cron`, or `watch`." },
                        "seconds": { "type": "integer" },
                        "cron": { "type": "string" },
                        "watch_kind": { "type": "string", "description": "`command`, `process`, `file`, `http`, or `subagent`." },
                        "watch_target": { "type": "string" },
                        "prompt": { "type": "string" }
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
                "name": "sandbox",
                "description": "Manage Cloud Kata microVM Sandbox (`status`, `ensure`, `exec`, `read_file`, `write_file`, `release`, `template:agent`, `/home/user/workspace`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string" },
                        "repo": { "type": "string" },
                        "branch": { "type": "string" },
                        "base": { "type": "string" },
                        "command": { "type": "string" },
                        "path": { "type": "string" },
                        "content": { "type": "string" }
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
    let sandbox_mode = resolve_sandbox_mode(config_dir, env);
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
                        let ref_file = if r.ends_with(".md") {
                            skill_dir.join("references").join(r)
                        } else {
                            skill_dir.join("references").join(format!("{r}.md"))
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
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
            let Some(patch_str) = args.get("patch").and_then(Value::as_str) else {
                return "error: missing `patch`".to_string();
            };
            if sandbox_mode != "cloud" {
                return match apply_patch_universal(work_dir, patch_str) {
                    Ok(msg) => msg,
                    Err(e) => format!("error applying patch: {e}"),
                };
            }
            let patch_b64 = base64_encode(patch_str.as_bytes());
            let script = format!(
                "tmp_patch=$(mktemp /tmp/sylphx-patch.XXXXXX) && printf '%s' '{patch_b64}' | base64 -d > \"$tmp_patch\" && \
                 (git apply --recount -C1 --whitespace=nowarn \"$tmp_patch\" 2>/dev/null || \
                  git apply --recount -p0 -C1 --whitespace=nowarn \"$tmp_patch\" 2>/dev/null || \
                  patch -p1 --fuzz=3 --no-backup-if-mismatch < \"$tmp_patch\" 2>/dev/null || \
                  git apply --recount --whitespace=nowarn \"$tmp_patch\"); \
                 rc=$?; rm -f \"$tmp_patch\"; exit $rc"
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
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
            let path = args.get("path").and_then(Value::as_str).unwrap_or(".");
            if sandbox_mode != "cloud" {
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
        "sandbox" => {
            let action = args
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or("status")
                .trim();
            match action {
                "status" => {
                    let lease_path = config_dir.join("sylphx-sandbox-lease.json");
                    if let Ok(raw) = std::fs::read_to_string(&lease_path)
                        && let Ok(val) = serde_json::from_str::<Value>(&raw)
                    {
                        return serde_json::to_string_pretty(&json!({
                            "mode": sandbox_mode,
                            "active_lease": val,
                        }))
                        .unwrap_or_default();
                    }
                    serde_json::to_string_pretty(&json!({
                        "mode": sandbox_mode,
                        "active_lease": null,
                    }))
                    .unwrap_or_default()
                }
                "ensure" | "create" => {
                    let explicit_repo = args.get("repo").and_then(Value::as_str);
                    let explicit_branch = args.get("branch").and_then(Value::as_str);
                    let explicit_base = args.get("base").and_then(Value::as_str);
                    match ensure_cloud_sandbox_lease_with_worktree(
                        config_dir,
                        Some(work_dir),
                        env,
                        explicit_repo,
                        explicit_branch,
                        explicit_base,
                    ) {
                        Ok((lease_name, uid)) => {
                            let lease_meta = std::fs::read_to_string(
                                config_dir.join("sylphx-sandbox-lease.json"),
                            )
                            .ok()
                            .and_then(|s| serde_json::from_str::<Value>(&s).ok());
                            serde_json::to_string_pretty(&json!({
                                "status": "ready",
                                "lease": lease_name,
                                "uid": uid,
                                "image": "template:agent",
                                "shape": resolve_sandbox_shape(env),
                                "workspace_dir": CLOUD_WORKSPACE_DIR,
                                "repo": lease_meta.as_ref().and_then(|v| v.get("repo")).cloned().unwrap_or(Value::Null),
                                "branch": lease_meta.as_ref().and_then(|v| v.get("branch")).cloned().unwrap_or(Value::Null),
                            }))
                            .unwrap_or_default()
                        }
                        Err(e) => format!("error ensuring cloud sandbox lease: {e}"),
                    }
                }
                "exec" => {
                    let Some(cmd_str) = args.get("command").and_then(Value::as_str) else {
                        return "error: missing `command`".to_string();
                    };
                    match exec_in_cloud_sandbox_with_work_dir(
                        config_dir,
                        Some(work_dir),
                        env,
                        cmd_str,
                    ) {
                        Ok(out) => out,
                        Err(e) => format!("error executing in cloud sandbox: {e}"),
                    }
                }
                "read_file" => {
                    let Some(rel) = args.get("path").and_then(Value::as_str) else {
                        return "error: missing `path`".to_string();
                    };
                    match read_file_in_cloud_sandbox_with_work_dir(
                        config_dir,
                        Some(work_dir),
                        env,
                        rel,
                    ) {
                        Ok(s) => s,
                        Err(e) => format!("error reading `{rel}` in cloud sandbox: {e}"),
                    }
                }
                "write_file" => {
                    let Some(rel) = args.get("path").and_then(Value::as_str) else {
                        return "error: missing `path`".to_string();
                    };
                    let Some(content) = args.get("content").and_then(Value::as_str) else {
                        return "error: missing `content`".to_string();
                    };
                    match write_file_in_cloud_sandbox_with_work_dir(
                        config_dir,
                        Some(work_dir),
                        env,
                        rel,
                        content,
                    ) {
                        Ok(s) => s,
                        Err(e) => format!("error writing `{rel}` in cloud sandbox: {e}"),
                    }
                }
                "release" | "delete" | "stop" => {
                    release_persisted_sandbox_lease(config_dir);
                    "released cloud sandbox lease".to_string()
                }
                "teleport" | "teleport_to_cloud" | "handoff_to_cloud" => {
                    let explicit_env = args.get("env").and_then(Value::as_str);
                    let model = std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
                        .ok()
                        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                        .and_then(|v| v.get("model").and_then(Value::as_str).map(str::to_string));
                    match teleport_session_to_cloud(
                        config_dir,
                        work_dir,
                        model.as_deref(),
                        env,
                        explicit_env,
                    ) {
                        Ok(receipt) => serde_json::to_string_pretty(&receipt).unwrap_or_default(),
                        Err(e) => format!("error teleporting session to cloud: {e}"),
                    }
                }
                "pull" | "pull_from_cloud" | "fallback_to_local" => {
                    match pull_session_from_cloud(config_dir, env) {
                        Ok(receipt) => serde_json::to_string_pretty(&receipt).unwrap_or_default(),
                        Err(e) => format!("error pulling session from cloud: {e}"),
                    }
                }
                other => format!("error: unknown sandbox action `{other}`"),
            }
        }
        "code_mode" => {
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
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
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
            let Some(cmd_str) = args.get("command").and_then(Value::as_str) else {
                return "error: missing `command`".to_string();
            };
            if sandbox_mode == "cloud" {
                match exec_in_cloud_sandbox_with_work_dir(config_dir, Some(work_dir), env, cmd_str)
                {
                    Ok(out) => return out,
                    Err(e) => record_cloud_sandbox_fallback(config_dir, &e),
                }
            }
            let timeout_ms = args
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(120_000);
            execute_bash_with_timeout(cmd_str, work_dir, env, timeout_ms)
        }
        "read_file" => {
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            let start_line = args.get("start_line").and_then(Value::as_u64);
            let end_line = args.get("end_line").and_then(Value::as_u64);
            if sandbox_mode == "cloud" {
                match read_file_in_cloud_sandbox_with_work_dir(config_dir, Some(work_dir), env, rel)
                {
                    Ok(content) => {
                        return format_read_file_output(rel, &content, start_line, end_line);
                    }
                    Err(e) => record_cloud_sandbox_fallback(config_dir, &e),
                }
            }
            let target = resolve_workspace_path(work_dir, rel);
            match std::fs::read_to_string(&target) {
                Ok(content) => format_read_file_output(rel, &content, start_line, end_line),
                Err(e) => format!("error reading {}: {e}", target.display()),
            }
        }
        "write_file" => {
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            let Some(content) = args.get("content").and_then(Value::as_str) else {
                return "error: missing `content`".to_string();
            };
            if sandbox_mode == "cloud" {
                match write_file_in_cloud_sandbox_with_work_dir(
                    config_dir,
                    Some(work_dir),
                    env,
                    rel,
                    content,
                ) {
                    Ok(msg) => return msg,
                    Err(e) => record_cloud_sandbox_fallback(config_dir, &e),
                }
            }
            let target = resolve_workspace_path(work_dir, rel);
            if let Some(p) = target.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            match std::fs::write(&target, content) {
                Ok(()) => format!("wrote {} ({} bytes)", target.display(), content.len()),
                Err(e) => format!("error writing {}: {e}", target.display()),
            }
        }
        "edit_file" => {
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
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
            if sandbox_mode == "cloud" {
                match read_file_in_cloud_sandbox_with_work_dir(config_dir, Some(work_dir), env, rel)
                {
                    Ok(existing) => {
                        match apply_edits_to_content(&existing, &edits_vec, replace_all) {
                            Ok((updated, ranges)) => {
                                match write_file_in_cloud_sandbox_with_work_dir(
                                    config_dir,
                                    Some(work_dir),
                                    env,
                                    rel,
                                    &updated,
                                ) {
                                    Ok(_) => {
                                        let range_desc: Vec<String> = ranges
                                            .iter()
                                            .map(|(s, e)| format!("L{s}..L{e}"))
                                            .collect();
                                        return format!(
                                            "edited `{rel}` in cloud sandbox ({})",
                                            range_desc.join(", ")
                                        );
                                    }
                                    Err(e) => record_cloud_sandbox_fallback(config_dir, &e),
                                }
                            }
                            Err(e) => return format!("error editing `{rel}`: {e}"),
                        }
                    }
                    Err(e) => record_cloud_sandbox_fallback(config_dir, &e),
                }
            }
            let target = resolve_workspace_path(work_dir, rel);
            match std::fs::read_to_string(&target) {
                Ok(existing) => match apply_edits_to_content(&existing, &edits_vec, replace_all) {
                    Ok((updated, ranges)) => match std::fs::write(&target, updated) {
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

fn call_anyagents_mcp_tool(
    mcp_command: &Path,
    mcp_args: &[String],
    env: &[(String, String)],
    tool_name: &str,
    arguments: &Value,
) -> String {
    let mut cmd = Command::new(mcp_command);
    cmd.args(mcp_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
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
            "arguments": arguments,
        }
    });
    if let Some(mut stdin) = child.stdin.take() {
        let _ = writeln!(stdin, "{init_req}");
        let _ = writeln!(stdin, "{init_notif}");
        let _ = writeln!(stdin, "{call_req}");
    }
    let Ok(out) = child.wait_with_output() else {
        return format!("error: failed waiting on anyagents mcp for `{tool_name}`");
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines().rev() {
        if let Ok(val) = serde_json::from_str::<Value>(line)
            && val.get("id").and_then(Value::as_i64) == Some(1)
        {
            if let Some(text) = val
                .pointer("/result/content/0/text")
                .and_then(Value::as_str)
            {
                return text.to_string();
            }
            if let Some(err) = val.pointer("/error/message").and_then(Value::as_str) {
                return format!("error: {err}");
            }
        }
    }
    stdout.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_harness::Context;

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
        assert_eq!(resolve_cloud_sandbox_path("."), "/home/user/workspace");
        assert_eq!(
            resolve_cloud_sandbox_path("crates/anyagents/src/lib.rs"),
            "/home/user/workspace/crates/anyagents/src/lib.rs"
        );
        assert_eq!(
            resolve_cloud_sandbox_path("./README.md"),
            "/home/user/workspace/README.md"
        );
        assert_eq!(
            resolve_cloud_sandbox_path("/workspace/src/main.rs"),
            "/home/user/workspace/src/main.rs"
        );
        assert_eq!(
            resolve_cloud_sandbox_path("/tmp/custom.txt"),
            "/tmp/custom.txt"
        );

        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        std::fs::write(
            cfg.join(".anyagents-managed-worktree.json"),
            serde_json::to_string_pretty(&json!({
                "path": "/home/user/workspace",
                "cloud_sandbox": true,
                "repo": "git@github.com:SylphxAI/anyagents.git",
                "branch": "agent/cloud-worker",
                "base": "main",
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
        assert_eq!(agent_json["sandbox"], "cloud");
        assert_eq!(agent_json["branch"], "agent/cloud-worker");
        assert_eq!(resolve_sandbox_mode(cfg, &[]), "cloud");

        let spec = resolve_agent_git_worktree_spec(cfg, None, &[], None, None, None);
        assert_eq!(
            spec.repo_url.as_deref(),
            Some("https://github.com/SylphxAI/anyagents.git")
        );
        assert_eq!(spec.repo_slug.as_deref(), Some("SylphxAI/anyagents"));
        assert_eq!(spec.branch.as_deref(), Some("agent/cloud-worker"));
        assert_eq!(spec.base.as_deref(), Some("main"));

        let env_spec = build_cloud_environment_spec(cfg, None, &[]);
        assert_eq!(env_spec["shape"], "linux-s");
        assert_eq!(env_spec["template"], "template:agent");
        assert!(env_spec.get("workspace_dir").is_none());
        assert_eq!(
            env_spec["repo"]["uri"],
            "https://github.com/SylphxAI/anyagents.git"
        );
        assert_eq!(env_spec["repo"]["revision"], "agent/cloud-worker");
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
        assert_eq!(all_cloud_tools[0]["name"], "shell");
        assert_eq!(all_cloud_tools[0]["builtin"]["id"], "shell");
        assert_eq!(all_cloud_tools[1]["name"], "files");
        assert_eq!(all_cloud_tools[1]["builtin"]["id"], "files");
    }

    #[test]
    fn cloud_sandbox_automatically_falls_back_to_local_when_cloud_api_unavailable() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();

        // Point cloud sandbox at an unreachable port with `SYLPHX_SANDBOX=cloud`.
        let env = vec![
            ("SYLPHX_SANDBOX".to_string(), "cloud".to_string()),
            ("SYLPHX_API_KEY".to_string(), "sylphx_sk_test".to_string()),
            (
                "SYLPHX_API_URL".to_string(),
                "http://127.0.0.1:9".to_string(),
            ),
        ];
        assert_eq!(resolve_sandbox_mode(&cfg, &env), "cloud");

        // 1. `write_file` attempts cloud sandbox, fails against dead port, records fallback, and writes locally!
        let write_out = execute_sylphx_tool(
            "write_file",
            &json!({ "path": "fallback.txt", "content": "alpha = 1\n" }),
            &cfg,
            &work,
            &env,
            Path::new("anyagents"),
            &[],
        );
        assert!(write_out.starts_with("wrote "), "unexpected: {write_out}");
        assert!(cfg.join("sylphx-sandbox-fallback.json").is_file());
        assert_eq!(resolve_sandbox_mode(&cfg, &env), "local");

        // 2. `edit_file`, `read_file`, and `bash` now run locally without hitting the dead cloud endpoint!
        let edit_out = execute_sylphx_tool(
            "edit_file",
            &json!({
                "path": "fallback.txt",
                "target": "alpha = 1",
                "replacement": "alpha = 42"
            }),
            &cfg,
            &work,
            &env,
            Path::new("anyagents"),
            &[],
        );
        assert!(edit_out.starts_with("edited "), "unexpected: {edit_out}");

        let read_out = execute_sylphx_tool(
            "read_file",
            &json!({ "path": "fallback.txt" }),
            &cfg,
            &work,
            &env,
            Path::new("anyagents"),
            &[],
        );
        assert!(read_out.contains("alpha = 42"), "unexpected: {read_out}");

        let bash_out = execute_sylphx_tool(
            "bash",
            &json!({ "command": "cat fallback.txt" }),
            &cfg,
            &work,
            &env,
            Path::new("anyagents"),
            &[],
        );
        assert!(bash_out.contains("alpha = 42"), "unexpected: {bash_out}");
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
        assert_eq!(receipt.mode, "local");
        assert_eq!(receipt.transcript_items, 3);
    }
}
