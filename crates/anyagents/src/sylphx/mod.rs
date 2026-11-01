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

mod auth;
mod cloud;
mod memory;

pub use auth::*;
pub use cloud::*;
pub use memory::*;

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
struct SharedState {
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
                    let api_url = std::env::var("SYLPHX_API_URL")
                        .ok()
                        .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string());
                    let sess_path = cloud_session_resource_path(&env_slug, &sess_id);
                    let steer_url =
                        format!("{}/v1/{sess_path}:steer", api_url.trim_end_matches('/'));
                    let _ = curl_post_json(
                        &steer_url,
                        Some(&api_key),
                        &json!({
                            "content": marked,
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
                run_cloud_session_turn(
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
                )
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
                        } else if lower.contains("missing sylphx api key")
                            || lower.contains("unauthorized")
                            || lower.contains("invalid api key")
                        {
                            g.sign_in_fault = Some(err_msg.clone());
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
        format!(
            "{}/agent_sessions/{trimmed}",
            env_slug.trim().trim_matches('/')
        )
    }
}

fn build_cloud_agent_client_tools() -> Vec<Value> {
    build_tool_definitions()
        .into_iter()
        .filter_map(|t| {
            let func = t.get("function")?;
            Some(json!({
                "name": func.get("name")?,
                "description": func.get("description").cloned().unwrap_or(Value::String(String::new())),
                "input_schema": func.get("parameters").cloned().unwrap_or_else(|| json!({"type": "object", "properties": {}})),
                "client": {},
            }))
        })
        .collect()
}

/// Runs a turn against the remote Sylphx Cloud Agents BaaS (`/v1/{env}/agent_sessions`),
/// passing a declarative `EnvironmentSpec` (`shape: "linux-xs"`, `template: "template:agent"`,
/// `repo: { uri, revision }`) and executing client tool calls (`send`, `agents`, `conversation`,
/// `groups`, `schedule`, `browser`, `describe`, `archive`) via `:submitToolResult`.
#[allow(clippy::too_many_arguments)]
fn run_cloud_session_turn(
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
    let api_key = resolve_sylphx_api_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx API key; run `anyagents sign-in sylphx` first".to_string()
    })?;
    let api_url = std::env::var("SYLPHX_API_URL")
        .ok()
        .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string());
    let base = api_url.trim_end_matches('/');

    let (mut agent_id, mut session_id) = {
        let g = state.lock().unwrap_or_else(PoisonError::into_inner);
        (g.cloud_agent_id.clone(), g.cloud_session_id.clone())
    };

    let (model_id, _) = parse_sylphx_model_and_effort(model);
    let system_prompt = assemble_system_prompt(config_dir, work_dir);
    let environment_spec = build_cloud_environment_spec(config_dir, Some(work_dir), env);

    if agent_id.is_none() {
        let create_agent_url = format!("{base}/v1/{env_slug}/agents");
        let agent_short_name = config_dir
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .unwrap_or("anyagents-sylphx");
        let spec_body = json!({
            "spec": {
                "model": model_id,
                "instructions": system_prompt,
                "environment": environment_spec,
                "tools": build_cloud_agent_client_tools(),
            },
            "name": agent_short_name,
            "model": model_id,
            "system_prompt": system_prompt,
            "environment": environment_spec,
            "tools": build_tool_definitions(),
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
        let create_sess_url = format!("{base}/v1/{env_slug}/agent_sessions");
        let agent_ref = agent_id.as_deref().unwrap_or_default();
        let body = json!({
            "agent": agent_ref,
            "agent_id": agent_ref,
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
        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
        g.cloud_session_id = Some(sid.clone());
        g.session_id = Some(sid.clone());
        let _ = std::fs::write(config_dir.join("sylphx-session-id"), format!("{sid}\n"));
    }

    let sid = session_id.unwrap_or_default();
    let sess_path = cloud_session_resource_path(env_slug, &sid);
    let send_url = format!("{base}/v1/{sess_path}:send");
    let send_body = json!({
        "content": user_message,
        "message": {
            "parts": [{ "text": user_message }]
        }
    });
    let (st, raw) = curl_post_json(&send_url, Some(&api_key), &send_body)?;
    if !(200..300).contains(&st) {
        return Err(format!("cloud session send HTTP {st}: {}", raw.trim()));
    }

    let sess_url = format!("{base}/v1/{sess_path}");
    let events_url = format!("{base}/v1/{sess_path}/session_events");
    let submit_tool_url = format!("{base}/v1/{sess_path}:submitToolResult");
    let mut handled_call_ids = std::collections::HashSet::new();
    let mut last_seq: i64 = 0;
    let mut sent_any_reply = false;
    let mut self_archived = false;
    let mut assistant_texts: Vec<String> = Vec::new();

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
                }

                let ev_type = ev
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_ascii_uppercase();

                if let Some(tc) = ev.get("tool_call").or_else(|| ev.get("toolCall")) {
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
                        let output = execute_sylphx_tool(
                            tool_name,
                            &parsed_args,
                            config_dir,
                            work_dir,
                            env,
                            mcp_command,
                            mcp_args,
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
                    }
                } else if ev_type.contains("AGENT_MESSAGE") || ev_type.contains("RESPONSE") {
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
                let content_text = assistant_texts.join("\n\n");
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
                return Err(format!("cloud session ended with status `{status}`"));
            }
        }
    }
    Ok(TurnUsage::default())
}

/// Runs our native multi-step Sylphx Agent Runtime loop against Sylphx AI's
/// canonical Responses API (`https://api.sylphx.com/v1/responses`), executing
/// AnyAgents MCP tools (`agents`, `send`, `conversation`, `groups`, `schedule`,
/// `browser`, `describe`, `archive`) and workspace tools (`bash`, `read_file`,
/// `write_file`, `edit_file`) directly, and draining `steer_queue` between every
/// step so mid-turn steering takes effect immediately on the next step.
#[allow(clippy::too_many_arguments)]
fn run_native_gateway_turn(
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
    let user_msg = json!({
        "role": "user",
        "content": initial_user_message,
    });
    append_transcript_message(&transcript_path, &user_msg);
    input_items.push(user_msg);

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
            for fc in function_calls {
                if cancel_flag.load(Ordering::SeqCst) {
                    break;
                }
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
                            .is_none_or(|s| s.trim().is_empty() || s.eq_ignore_ascii_case("self")))
                {
                    self_archived = true;
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
            if let Some(handoff) = pending_window_handoff {
                let anchor = json!({
                    "type": "context_window_open",
                    "handoff": handoff,
                    "timestamp_ms": anyagents_core::now_ms(),
                });
                append_transcript_message(&transcript_path, &anchor);
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
                run_cloud_session_turn(
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
                )
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

fn assemble_system_prompt(config_dir: &Path, work_dir: &Path) -> String {
    let mut parts = Vec::new();
    if let Ok(sys) = std::fs::read_to_string(config_dir.join("SYSTEM_PROMPT.md"))
        && !sys.trim().is_empty()
    {
        parts.push(sys);
    } else if let Ok(agents_md) = std::fs::read_to_string(work_dir.join("AGENTS.md"))
        && !agents_md.trim().is_empty()
    {
        parts.push(agents_md);
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
                "description": "Send a message, media attachment, location, poll, checklist, or article, or perform an interaction (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`) to one or more agents, groups, or channels.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "to": {
                            "description": "Recipient(s): a string or array of strings (`\"parent\"` or `[\"parent\"]`, `agent:<name>`, bare name, `children`, `group:<name>`, or `telegram:chat:<id>[:<topic>]`)."
                        },
                        "text": { "type": "string", "description": "Message text or caption." },
                        "reply_to": { "type": "string", "description": "Optional reply reference (`anyagents:reply:msg_...`)." },
                        "action": { "type": "string", "description": "Optional interaction action (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`)." },
                        "target": { "type": "string", "description": "Target message for interaction actions." },
                        "emoji": { "type": "string", "description": "Emoji for `react`." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "describe",
                "description": "Read or update an agent's role & responsibility description (`agent`: `\"self\"` | `\"all\"` | `\"<agent>\"`, optional `description`, `role`, `clear`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string", "description": "Target agent (`\"self\"`, `\"all\"`, or agent name). Defaults to `\"self\"`." },
                        "description": { "type": "string", "description": "Updated role/responsibility description." },
                        "role": { "type": "string", "description": "Optional short role label." },
                        "clear": { "type": "boolean", "description": "Clear existing description." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "archive",
                "description": "Archive an agent or sub-agent (`agent`: `\"self\"` or `<agent-name>`) to scale its process to zero while preserving its permanent `agt_...` ID and session.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string", "description": "Target agent (`\"self\"` or sub-agent name). Defaults to `\"self\"`." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "agents",
                "description": "List, inspect, spawn, configure, wake, archive, pause, or delete agents and sub-agents (`list`, `inspect`, `get`, `spawn`, `create`, `describe`, `steer`, `wake`, `archive`, `pause`, `delete`, `rename`, `role`, `harness`, `models`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`list`, `inspect`, `get`, `spawn`, `create`, `describe`, `steer`, `wake`, `archive`, `pause`, `delete`, `rename`, `role`, `harness`, or `models`." },
                        "name": { "type": "string", "description": "Target agent or sub-agent name." },
                        "agent": { "type": "string", "description": "Alias for `name`." },
                        "parent": { "type": "string", "description": "Parent filter (`self`, `subagents`, `root`, or agent name)." },
                        "state": { "type": "string", "description": "State filter (`running`, `working`, `paused`, `archived`, `all`)." },
                        "role": { "type": "string", "description": "Short 2-5 word role title when spawning." },
                        "harness": { "type": "string", "description": "Harness (`sylphx`, `codex`, `claude-code`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`, or `inherit`)." },
                        "model": { "type": "string", "description": "Model ID." },
                        "profile": { "type": "string", "description": "Provider profile (`<provider>/<profile>`)." },
                        "repo": { "type": "string", "description": "Repository (`<org>/<repo>`, local path, or `inherit`)." },
                        "branch": { "type": "string" },
                        "base": { "type": "string" },
                        "workdir": { "type": "string" },
                        "description": { "type": "string", "description": "Role/responsibility description or standing instructions." },
                        "instructions": { "type": "string" },
                        "prompt": { "type": "string", "description": "Initial task prompt delivered to the spawned or woken agent." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "conversation",
                "description": "Read or search (`query`) two-way (`in` + `out`) conversation history across agents, channels, groups, and peers.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string", "description": "Agent name (`self`, `all`, or peer name)." },
                        "query": { "type": "string", "description": "Optional Hybrid RAG search query." },
                        "with": { "type": "string", "description": "Optional peer filter." },
                        "limit": { "type": "integer", "description": "Max turns to return." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "groups",
                "description": "List, create, delete, or manage agent groups (`list`, `create`, `delete`, `add_member`, `remove_member`).",
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
                "description": "List, create, pause, resume, or cancel durable timers, recurring intervals, cron schedules, and event-driven watches (`list`, `create`, `pause`, `resume`, `cancel`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`list`, `create`, `pause`, `resume`, or `cancel`." },
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
                "description": "Drive a headless Chromium browser (`goto`, `snapshot`, `click`, `fill`, `type`, `press`, `eval`, `text`, `html`, `pdf`, `screenshot`, `tabs`, `close`).",
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
                "description": "Manage or execute inside this agent's isolated Cloud Kata microVM Sandbox pod (`https://api.sylphx.com/v1/{parent}/leases`, `template:agent`, worktree at `/home/user/workspace`). Actions: `status`, `ensure`, `exec`, `read_file`, `write_file`, `release`. When `ensure` is called (or when `SYLPHX_SANDBOX=cloud`), the agent's Git repository and branch are automatically cloned and checked out inside `/home/user/workspace` with `GH_TOKEN` (injected ephemerally in memory, never written to disk) and Git author (`Kyle Tse <hi@kylet.se>`) configured, and `bash`, `read_file`, `write_file`, `edit_file`, `apply_patch`, `grep`, `glob`, and `list_dir` run inside the cloud sandbox.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`status`, `ensure`, `exec`, `read_file`, `write_file`, or `release`." },
                        "repo": { "type": "string", "description": "Optional GitHub repository (`<org>/<repo>` or HTTPS URL) to clone into `/home/user/workspace` when `action` is `ensure`." },
                        "branch": { "type": "string", "description": "Optional Git branch to checkout or create inside `/home/user/workspace` when `action` is `ensure`." },
                        "base": { "type": "string", "description": "Optional starting Git ref/SHA when creating `branch`." },
                        "command": { "type": "string", "description": "Shell command for `exec`." },
                        "path": { "type": "string", "description": "File path inside the sandbox for `read_file` or `write_file` (relative paths resolve against `/home/user/workspace`)." },
                        "content": { "type": "string", "description": "File content for `write_file`." }
                    },
                    "required": ["action"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "bash",
                "description": "Execute a bash command in the agent's working directory (or inside the isolated Cloud Kata microVM Sandbox pod when a cloud sandbox lease is active).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": { "type": "string", "description": "Shell command to run." },
                        "timeout_ms": { "type": "integer", "description": "Optional timeout in ms (default 120000)." }
                    },
                    "required": ["command"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a UTF-8 file from the workspace.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": "Relative or absolute file path." },
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
                "description": "Write contents to a file in the workspace (creating parent directories if needed).",
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
                "description": "Replace an exact target string in a file with a replacement string.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "target": { "type": "string" },
                        "replacement": { "type": "string" }
                    },
                    "required": ["path", "target", "replacement"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "apply_patch",
                "description": "Apply a unified diff patch (`git apply` / unified diff format) to the workspace.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "patch": { "type": "string", "description": "Unified diff patch text." }
                    },
                    "required": ["patch"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "grep",
                "description": "Fast regex or literal search across the workspace (`rg` / ripgrep).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "pattern": { "type": "string", "description": "Regex or literal pattern to search for." },
                        "path": { "type": "string", "description": "Optional directory or file path (defaults to `.`)." },
                        "glob": { "type": "string", "description": "Optional file glob filter (e.g. `*.rs`)." }
                    },
                    "required": ["pattern"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "glob",
                "description": "Find files matching a glob pattern in the workspace.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "pattern": { "type": "string", "description": "Glob pattern (e.g. `**/*.rs`)." },
                        "path": { "type": "string", "description": "Optional base directory." }
                    },
                    "required": ["pattern"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "list_dir",
                "description": "List entries in a workspace directory.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": "Directory path (defaults to `.`)." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "activate_skill",
                "description": "Load the full `SKILL.md` instructions (and list or read `references/*.md`) for a named skill from `.agents/skills`.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string", "description": "Skill name (e.g. `deliver-and-verify`, `self-challenge`, `prove-it`, `clear-writing`, `author-item`, `unblock`, `swarm-fuse`, `refactor`)." },
                        "reference": { "type": "string", "description": "Optional reference filename inside `references/` (e.g. `architecture.md`)." }
                    },
                    "required": ["name"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "goal",
                "description": "Manage the session's durable Goal (`create`, `update`, `complete`, `pause`, `cancel`, `status`). While a Goal is `active`, the harness automatically continues turns until the Goal's acceptance criteria are verified and `complete` is called.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`create`, `update`, `complete`, `pause`, `cancel`, or `status`." },
                        "title": { "type": "string", "description": "Goal title." },
                        "acceptance_criteria": { "type": "array", "items": { "type": "string" }, "description": "Observable acceptance test cases." },
                        "progress": { "type": "string", "description": "Progress notes or verification evidence." }
                    },
                    "required": ["action"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "memory_recall",
                "description": "Zero-Embedding Lossless `Raw-RAG` lexical + structured recall over the full append-only session transcript (including all turns prior to any `context_window_open` compaction boundary). Supports single/multi-query BM25 (`query`, `queries`), negative terms (`exclude`), tool history toggles (`include_tools`, `only_tools`, `tool_name`), `role` filter, ordering (`sort`: `\"relevance\"` | `\"newest\"` | `\"oldest\"`), exact range coordinates (`range`: `transcript:<from>-<to>`), and contiguous episode window expansion (`radius`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "Primary search query (supports English, code identifiers, and CJK / Cantonese terms)." },
                        "queries": { "type": "array", "items": { "type": "string" }, "description": "Optional additional search queries / synonyms OR-combined with `query`." },
                        "exclude": { "type": "array", "items": { "type": "string" }, "description": "Optional negative substrings — items containing any of these terms are excluded." },
                        "include_tools": { "type": "boolean", "description": "Whether to include tool call/output history (`true` by default). Set `false` to search only user/assistant messages." },
                        "only_tools": { "type": "boolean", "description": "When `true`, searches ONLY tool call and tool output items." },
                        "tool_name": { "type": "string", "description": "Optional tool name filter (e.g. `\"bash\"`, `\"edit_file\"`, `\"send\"`). Matches both the tool call and its output." },
                        "role": { "type": "string", "description": "Optional role filter (`\"user\"`, `\"assistant\"`, `\"tool\"`)." },
                        "sort": { "type": "string", "enum": ["relevance", "newest", "oldest"], "description": "Result ordering (`\"relevance\"` default, `\"newest\"`, or `\"oldest\"`)." },
                        "range": { "type": "string", "description": "Exact item range coordinate (`transcript:<from>-<to>` or `<from>-<to>`) returned by a previous `memory_recall` hit." },
                        "limit": { "type": "integer", "description": "Maximum episode windows or filtered items to return (default 5, max 50)." },
                        "radius": { "type": "integer", "description": "Contiguous neighbor items to include around each matched window to preserve intact `(Event -> Action -> Receipt)` causal chains (default 2, max 10)." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "context_budget_read",
                "description": "Read the active context window's token budget, item count, total persisted raw transcript count, and whether a handoff (`context_window_open`) is due.",
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "context_window_open",
                "description": "Autonomously compact the active context window at a coherent task milestone by writing a structured `handoff` (`Goal`, `Verified Facts`, `Decisions`, `Next Action`) and opening a fresh KV-cached window. 100% of prior raw transcript turns remain on disk and searchable via `memory_recall`.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "handoff": {
                            "type": "string",
                            "description": "Structured handoff anchoring the new context window: current goal, verified facts/SHAs/paths, key decisions, and immediate next step."
                        }
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
            let Some(patch_str) = args.get("patch").and_then(Value::as_str) else {
                return "error: missing `patch`".to_string();
            };
            let patch_b64 = base64_encode(patch_str.as_bytes());
            let script =
                format!("printf '%s' '{patch_b64}' | base64 -d | git apply --whitespace=nowarn -");
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
            let path = args.get("path").and_then(Value::as_str).unwrap_or(".");
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
                                "shape": "linux-xs",
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
                other => format!("error: unknown sandbox action `{other}`"),
            }
        }
        "bash" | "shell" => {
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
            let Some(cmd_str) = args.get("command").and_then(Value::as_str) else {
                return "error: missing `command`".to_string();
            };
            if sandbox_mode == "cloud" {
                return match exec_in_cloud_sandbox_with_work_dir(
                    config_dir,
                    Some(work_dir),
                    env,
                    cmd_str,
                ) {
                    Ok(out) => out,
                    Err(e) => format!("error executing in cloud sandbox: {e}"),
                };
            }
            let mut cmd = Command::new("bash");
            cmd.arg("-c")
                .arg(cmd_str)
                .current_dir(work_dir)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            for (k, v) in env {
                cmd.env(k, v);
            }
            match cmd.output() {
                Ok(out) => {
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let code = out.status.code().unwrap_or(-1);
                    let mut combined = format!("exit_code: {code}\n");
                    if !stdout.is_empty() {
                        combined.push_str(&stdout);
                    }
                    if !stderr.is_empty() {
                        if !combined.ends_with('\n') {
                            combined.push('\n');
                        }
                        combined.push_str("stderr:\n");
                        combined.push_str(&stderr);
                    }
                    if combined.len() > 32_000 {
                        combined.truncate(32_000);
                        combined.push_str("\n...[truncated]");
                    }
                    combined
                }
                Err(e) => format!("error executing bash: {e}"),
            }
        }
        "read_file" => {
            if sandbox_mode == "none" {
                return "error: sandbox is disabled (`SYLPHX_SANDBOX=none`)".to_string();
            }
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            if sandbox_mode == "cloud" {
                return match read_file_in_cloud_sandbox_with_work_dir(
                    config_dir,
                    Some(work_dir),
                    env,
                    rel,
                ) {
                    Ok(content) => {
                        let start = args
                            .get("start_line")
                            .and_then(Value::as_u64)
                            .unwrap_or(1)
                            .max(1) as usize;
                        let lines: Vec<&str> = content.lines().collect();
                        let end = args
                            .get("end_line")
                            .and_then(Value::as_u64)
                            .map(|n| n as usize)
                            .unwrap_or(lines.len())
                            .min(lines.len());
                        if start > lines.len() {
                            String::new()
                        } else {
                            lines[start - 1..end].join("\n")
                        }
                    }
                    Err(e) => format!("error reading `{rel}` in cloud sandbox: {e}"),
                };
            }
            let target = resolve_workspace_path(work_dir, rel);
            match std::fs::read_to_string(&target) {
                Ok(content) => {
                    let start = args
                        .get("start_line")
                        .and_then(Value::as_u64)
                        .unwrap_or(1)
                        .max(1) as usize;
                    let lines: Vec<&str> = content.lines().collect();
                    let end = args
                        .get("end_line")
                        .and_then(Value::as_u64)
                        .map(|n| n as usize)
                        .unwrap_or(lines.len())
                        .min(lines.len());
                    if start > lines.len() {
                        return String::new();
                    }
                    lines[start - 1..end].join("\n")
                }
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
                return match write_file_in_cloud_sandbox_with_work_dir(
                    config_dir,
                    Some(work_dir),
                    env,
                    rel,
                    content,
                ) {
                    Ok(msg) => msg,
                    Err(e) => format!("error writing `{rel}` in cloud sandbox: {e}"),
                };
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
            let Some(old_str) = args.get("target").and_then(Value::as_str) else {
                return "error: missing `target`".to_string();
            };
            let Some(new_str) = args.get("replacement").and_then(Value::as_str) else {
                return "error: missing `replacement`".to_string();
            };
            if sandbox_mode == "cloud" {
                return match read_file_in_cloud_sandbox_with_work_dir(
                    config_dir,
                    Some(work_dir),
                    env,
                    rel,
                ) {
                    Ok(existing) => {
                        if !existing.contains(old_str) {
                            return format!("error: target string not found in `{rel}`");
                        }
                        let updated = existing.replace(old_str, new_str);
                        match write_file_in_cloud_sandbox_with_work_dir(
                            config_dir,
                            Some(work_dir),
                            env,
                            rel,
                            &updated,
                        ) {
                            Ok(_) => format!("edited `{rel}` in cloud sandbox"),
                            Err(e) => format!("error writing `{rel}` in cloud sandbox: {e}"),
                        }
                    }
                    Err(e) => format!("error reading `{rel}` in cloud sandbox: {e}"),
                };
            }
            let target = resolve_workspace_path(work_dir, rel);
            match std::fs::read_to_string(&target) {
                Ok(existing) => {
                    if !existing.contains(old_str) {
                        return format!("error: target string not found in {}", target.display());
                    }
                    let updated = existing.replace(old_str, new_str);
                    match std::fs::write(&target, updated) {
                        Ok(()) => format!("edited {}", target.display()),
                        Err(e) => format!("error writing {}: {e}", target.display()),
                    }
                }
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
    fn create_agent_and_subagent_with_sylphx_harness_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let store = anyagents_core::Store::open(data.join("state.db")).unwrap();

        let root = crate::create::create(
            &store,
            &data,
            crate::create::CreateRequest {
                name: "sylphx-lead".to_string(),
                role: Some("Sylphx Lead".to_string()),
                harness: "sylphx-agent".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: None,
                instructions: Some("Lead instructions".to_string()),
                parent_id: None,
            },
        )
        .unwrap();
        assert_eq!(root.harness, "sylphx");
        let root_cfg = anyagents_core::agent_config_dir(&data, &root.id);
        assert!(root_cfg.join("sylphx-agent.json").is_file());

        let sub = crate::create::create(
            &store,
            &data,
            crate::create::CreateRequest {
                name: "sylphx-worker-1".to_string(),
                role: Some("Sylphx Sub-Agent".to_string()),
                harness: "sylphx".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#medium".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: None,
                instructions: Some("Worker instructions".to_string()),
                parent_id: Some(root.id.clone()),
            },
        )
        .unwrap();
        assert_eq!(sub.harness, "sylphx");
        assert_eq!(sub.parent_id.as_ref(), Some(&root.id));
        let sub_cfg = anyagents_core::agent_config_dir(&data, &sub.id);
        assert!(sub_cfg.join("sylphx-agent.json").is_file());
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
        assert_eq!(env_spec["shape"], "linux-xs");
        assert_eq!(env_spec["template"], "template:agent");
        assert_eq!(
            env_spec["repo"]["uri"],
            "https://github.com/SylphxAI/anyagents.git"
        );
        assert_eq!(env_spec["repo"]["revision"], "agent/cloud-worker");
    }
}
