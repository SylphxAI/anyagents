//! Sylphx Core Agent SDK (`sylphx::sdk`).
//!
//! First-principles boundary:
//! - **Core SDK (`SylphxSdkSession`, `SylphxExecutionMode`, `SylphxClientToolHandler`)**:
//!   Owns the standalone Agent Loop, append-only `sylphx-transcript.jsonl` event log,
//!   lossless Raw-RAG `memory_recall`, `context_window_open`, workspace ACI tools, and
//!   seamless Local <-> Cloud Session Teleportation (`teleport_to_cloud` / `pull_from_cloud`).
//! - **Zero Backdoor Surface Integration**:
//!   - Standalone CLI / TUI or third-party Rust apps use `SylphxSdkSession` with
//!     `NoopClientTools` (only core workspace, memory, and sandbox tools).
//!   - Multi-agent orchestrators like AnyAgents plug in coordination tools (`send`,
//!     `agents`, `conversation`, `groups`, `schedule`, `browser`) through the public
//!     `SylphxClientToolHandler` trait without privileged backdoors.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

use crate::{
    PersistedCloudSession, SharedState, append_transcript_message, assemble_system_prompt,
    build_cloud_agent_tools, build_cloud_environment_spec, build_prior_transcript_continuity_block,
    clear_persisted_cloud_session, cloud_session_resource_path, compute_instructions_fingerprint,
    curl_get, load_persisted_cloud_session, load_transcript_input_items, normalize_cloud_env_slug,
    parse_sylphx_model_and_effort, resolve_agent_short_name, resolve_cloud_api_url,
    resolve_sylphx_cloud_access_key, run_cloud_session_turn, run_native_gateway_turn,
    save_persisted_cloud_session,
};
use anyagents_harness::TurnUsage;
use std::io::{BufRead, Write};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

/// Execution topology for a `SylphxSdkSession`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SylphxExecutionMode {
    /// Prefer the explicitly configured Cloud environment; use local only when
    /// no Cloud environment is selected. Never replay an uncertain remote effect.
    AutoCloudFirst,
    /// Run the multi-step agent loop strictly on the local machine (`/v1/responses`).
    LocalOnly,
    /// Run strictly on Sylphx Cloud (`agent_sessions`).
    CloudOnly,
}

impl SylphxExecutionMode {
    pub fn from_env(config_dir: &Path, env: &[(String, String)]) -> Result<Self, String> {
        let settings = crate::contracts::read_json(&config_dir.join("sylphx-agent.json"))?;
        let placement =
            crate::contracts::read_json(&config_dir.join("sylphx-workspace-placement.json"))?;
        let configured = settings.as_ref().and_then(|v| v.get("execution_mode"));
        let placed = placement.as_ref().and_then(|v| v.get("placement"));
        if placement.is_some() && placed.is_none() {
            return Err("placement setting is missing".into());
        }
        if configured.is_some_and(|v| !v.is_string()) || placed.is_some_and(|v| !v.is_string()) {
            return Err("execution mode/placement must be a string".into());
        }
        let explicit = env
            .iter()
            .find(|(k, _)| k == "SYLPHX_EXECUTION_MODE")
            .map(|(_, v)| v.clone())
            .or_else(|| std::env::var("SYLPHX_EXECUTION_MODE").ok());
        // Persistent placement owns routing, independent of SDK session transfer.
        let mode = placed
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(explicit)
            .or_else(|| configured.and_then(Value::as_str).map(str::to_string));
        match mode
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("auto_cloud_first" | "auto" | "cloud") => Ok(Self::AutoCloudFirst),
            Some("local" | "local_only" | "native") => Ok(Self::LocalOnly),
            Some("cloud_only" | "strict_cloud") => Ok(Self::CloudOnly),
            Some(other) => Err(format!("invalid execution mode/placement: {other}")),
        }
    }
}

/// Pluggable client-tool extension point so external orchestrators (like AnyAgents) or
/// custom host applications can attach domain tools without coupling the Core SDK.
pub trait SylphxClientToolHandler: Send + Sync {
    /// Client tool declarations (`{"name": ..., "description": ..., "parameters": ...}`).
    fn tool_definitions(&self) -> Vec<Value>;

    /// Executes a client tool if owned by this handler, returning `Some(output)`.
    /// Returns `None` if `tool_name` is a built-in Core SDK workspace/memory tool.
    fn execute(&self, tool_name: &str, args: &Value) -> Option<String>;
}

/// Standalone client-tool handler with zero orchestrator tools (pure Core SDK mode).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopClientTools;

impl SylphxClientToolHandler for NoopClientTools {
    fn tool_definitions(&self) -> Vec<Value> {
        Vec::new()
    }

    fn execute(&self, _tool_name: &str, _args: &Value) -> Option<String> {
        None
    }
}

/// Result of teleporting a local session to Sylphx Cloud or pulling a Cloud session back to local.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTeleportReceipt {
    pub direction: String,
    pub mode: String,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
    pub env_slug: Option<String>,
    pub synced_events: usize,
    pub transcript_items: usize,
    /// Session transfer is not persistent workspace routing configuration.
    pub placement_changed: bool,
}

/// Teleports a local `SylphxSdkSession` (`sylphx-transcript.jsonl` + git workspace state)
/// to Sylphx Cloud (`/v1/{env}/agent_sessions`) so execution continues seamlessly in the cloud.
pub fn teleport_session_to_cloud(
    config_dir: &Path,
    work_dir: &Path,
    model: Option<&str>,
    env: &[(String, String)],
    explicit_env_slug: Option<&str>,
) -> Result<SessionTeleportReceipt, String> {
    if load_persisted_cloud_session(config_dir)?.is_some() {
        return Err(
            "teleport requires reconciling/pulling the retained cloud session first".into(),
        );
    }
    let api_key = resolve_sylphx_cloud_access_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx API key; run `anyagents sign-in sylphx` first".to_string()
    })?;
    let base = resolve_cloud_api_url(env);
    let env_slug = explicit_env_slug
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            env.iter()
                .find(|(k, v)| k == "SYLPHX_AGENT_ENV" && !v.trim().is_empty())
                .map(|(_, v)| v.trim().to_string())
        })
        .or_else(|| {
            std::env::var("SYLPHX_AGENT_ENV")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .ok_or("teleport requires an explicit authorized cloud environment (SYLPHX_AGENT_ENV)")?;
    let canonical_env = normalize_cloud_env_slug(&env_slug)?;
    let (model_id, _) = parse_sylphx_model_and_effort(model);
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");
    crate::memory::validate_transcript(&transcript_path)?;
    let transcript_items = load_transcript_input_items(&transcript_path).len();

    let mut system_prompt = assemble_system_prompt(config_dir, work_dir);
    system_prompt.push_str(&crate::load_explicit_context(config_dir, work_dir)?);
    if let Some(prior_block) = build_prior_transcript_continuity_block(&transcript_path) {
        system_prompt.push_str("\n\n");
        system_prompt.push_str(&prior_block);
    }
    let environment_spec = build_cloud_environment_spec(config_dir, Some(work_dir), env);

    let create_agent_url = format!("{base}/v1/{canonical_env}/agents");
    let agent_short_name = resolve_agent_short_name(config_dir, env);
    let current_fp = compute_instructions_fingerprint(&agent_short_name, &system_prompt);
    let spec_body = json!({
        "meta": {
            "display_name": agent_short_name,
        },
        "spec": {
            "models": {
                "primary": model_id,
                "light": "google/gemini-3-flash",
            },
            "instructions": system_prompt,
            "environment": environment_spec,
            "tools": build_cloud_agent_tools(),
        },
    });
    let (st_agent, raw_agent) =
        crate::auth::post_cloud_json(&create_agent_url, Some(&api_key), &spec_body)?;
    if !(200..300).contains(&st_agent) {
        return Err(format!(
            "teleport_to_cloud create_agent HTTP {st_agent}: {}",
            raw_agent.trim()
        ));
    }
    let agent_val: Value = serde_json::from_str(&raw_agent).map_err(|e| e.to_string())?;
    let agent_id = agent_val
        .get("name")
        .or_else(|| agent_val.get("id"))
        .or_else(|| agent_val.get("uid"))
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| "teleport_to_cloud create_agent missing name/id".to_string())?
        .to_string();

    let create_sess_url = format!("{base}/v1/{canonical_env}/agent_sessions");
    let sess_body = json!({
        "agent": agent_id,
        "title": "Teleported Sylphx SDK Session",
        "environment": environment_spec,
    });
    let (st_sess, raw_sess) =
        crate::auth::post_cloud_json(&create_sess_url, Some(&api_key), &sess_body)?;
    if !(200..300).contains(&st_sess) {
        return Err(format!(
            "teleport_to_cloud create_session HTTP {st_sess}: {}",
            raw_sess.trim()
        ));
    }
    let sess_val: Value = serde_json::from_str(&raw_sess).map_err(|e| e.to_string())?;
    let session_id = sess_val
        .get("name")
        .or_else(|| sess_val.get("id"))
        .or_else(|| sess_val.get("uid"))
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| "teleport_to_cloud create_session missing name/id".to_string())?
        .to_string();

    save_persisted_cloud_session(
        config_dir,
        &PersistedCloudSession {
            agent_id: agent_id.clone(),
            session_id: session_id.clone(),
            model_id,
            env_slug: canonical_env.clone(),
            last_seq: 0,
        },
    )?;
    crate::contracts::write_atomic(
        &config_dir.join(".sylphx-cloud-instructions-fingerprint"),
        format!("{current_fp}\n").as_bytes(),
    )?;
    crate::contracts::write_atomic(
        &config_dir.join("sylphx-session-id"),
        format!("{session_id}\n").as_bytes(),
    )?;
    let _ = std::fs::remove_file(config_dir.join("sylphx-cloud-session-fallback.json"));
    let _ = std::fs::remove_file(config_dir.join("sylphx-sandbox-fallback.json"));

    Ok(SessionTeleportReceipt {
        direction: "local_to_cloud".to_string(),
        mode: "cloud".to_string(),
        agent_id: Some(agent_id),
        session_id: Some(session_id),
        env_slug: Some(canonical_env),
        synced_events: transcript_items,
        transcript_items,
        placement_changed: false,
    })
}

/// Pulls any unsynced `session_events` from the active Sylphx Cloud `AgentSession`
/// into local `sylphx-transcript.jsonl`, interrupts any in-flight cloud turn, and
/// clears the retained cloud-session metadata. This does not select persistent
/// workspace placement; that independent routing record remains unchanged.
pub fn pull_session_from_cloud(
    config_dir: &Path,
    env: &[(String, String)],
) -> Result<SessionTeleportReceipt, String> {
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");
    crate::memory::validate_transcript(&transcript_path)?;
    let mut synced_events = 0usize;
    let mut prev_agent_id = None;
    let mut prev_session_id = None;
    let mut prev_env_slug = None;

    if let Some(mut persisted) = load_persisted_cloud_session(config_dir)? {
        prev_agent_id = Some(persisted.agent_id.clone());
        prev_session_id = Some(persisted.session_id.clone());
        prev_env_slug = Some(persisted.env_slug.clone());
        let api_key = resolve_sylphx_cloud_access_key(config_dir, env)
            .ok_or("cloud pull requires credentials; binding retained")?;
        let base = resolve_cloud_api_url(env);
        let sess_path = cloud_session_resource_path(&persisted.env_slug, &persisted.session_id);
        let settled = crate::interrupt_and_settle(&base, &sess_path, &api_key)?;
        let terminal_seq = crate::contracts::sequence(
            settled
                .get("last_event_sequence")
                .or_else(|| settled.get("lastEventSequence")),
            true,
        )?;
        let starting_seq = persisted.last_seq;
        let mut page_token = String::new();
        let mut seen_tokens = std::collections::HashSet::new();
        loop {
            let url = format!(
                "{base}/v1/{sess_path}/session_events?filter=sequence%20%3E%20{starting_seq}{page_token}"
            );
            let (status, raw) = curl_get(&url, Some(&api_key))?;
            if !(200..300).contains(&status) {
                return Err(format!("cloud pull events HTTP {status}; binding retained"));
            }
            let doc: Value = serde_json::from_str(&raw)
                .map_err(|e| format!("invalid cloud pull events: {e}"))?;
            let events = doc
                .get("session_events")
                .or_else(|| doc.get("sessionEvents"))
                .or_else(|| doc.get("events"))
                .and_then(Value::as_array)
                .ok_or("cloud pull missing events array")?;
            // Validate the full page before retaining any of it.
            for event in events {
                crate::contracts::sequence(event.get("sequence"), false)?;
            }
            for event in events {
                let seq = crate::contracts::sequence(event.get("sequence"), false)?;
                if seq <= persisted.last_seq {
                    continue;
                }
                // A retained identity lets a retry reconcile append-before-cursor failure.
                let existing =
                    crate::contracts::read_optional(&transcript_path)?.unwrap_or_default();
                let duplicate = existing
                    .lines()
                    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                    .any(|item| {
                        item.get("cloud_session").and_then(Value::as_str)
                            == Some(&persisted.session_id)
                            && item.get("cloud_sequence").and_then(Value::as_i64) == Some(seq)
                    });
                if !duplicate {
                    append_transcript_message(
                        &transcript_path,
                        &json!({
                            "type": "cloud_session_event", "cloud_session": persisted.session_id,
                            "cloud_sequence": seq, "event": event,
                        }),
                    )?;
                    synced_events += 1;
                }
                persisted.last_seq = seq;
                save_persisted_cloud_session(config_dir, &persisted)?;
            }
            let next = doc
                .get("next_page_token")
                .or_else(|| doc.get("nextPageToken"));
            if let Some(token) = next.and_then(Value::as_str).filter(|s| !s.is_empty()) {
                if !seen_tokens.insert(token.to_string()) {
                    return Err("cloud pull repeated pagination token; binding retained".into());
                }
                let encoded: String = token.bytes().map(|byte| format!("%{byte:02X}")).collect();
                page_token = format!("&page_token={encoded}");
            } else {
                break;
            }
        }
        if persisted.last_seq < terminal_seq {
            return Err(
                "cloud pull did not reach the settled event watermark; binding retained".into(),
            );
        }
        clear_persisted_cloud_session(config_dir)?;
        crate::contracts::remove_optional(&config_dir.join("sylphx-cloud-turn-pending.json"))?;
    }

    let transcript_items = load_transcript_input_items(&transcript_path).len();
    Ok(SessionTeleportReceipt {
        direction: "cloud_to_local".to_string(),
        mode: "unchanged".to_string(),
        agent_id: prev_agent_id,
        session_id: prev_session_id,
        env_slug: prev_env_slug,
        synced_events,
        transcript_items,
        placement_changed: false,
    })
}

/// High-level standalone session handle for embedding the Sylphx Agent Loop in a CLI,
/// TUI, or host Rust application without AnyAgents server dependencies.
pub struct SylphxSdkSession {
    pub config_dir: PathBuf,
    pub work_dir: PathBuf,
    pub model: Option<String>,
    pub execution_mode: SylphxExecutionMode,
    pub env: Vec<(String, String)>,
}

impl SylphxSdkSession {
    pub fn new(
        config_dir: impl Into<PathBuf>,
        work_dir: impl Into<PathBuf>,
        model: Option<String>,
        env: Vec<(String, String)>,
    ) -> Result<Self, String> {
        let config_dir = config_dir.into();
        let work_dir = work_dir.into();
        let execution_mode = SylphxExecutionMode::from_env(&config_dir, &env)?;
        Ok(Self {
            config_dir,
            work_dir,
            model,
            execution_mode,
            env,
        })
    }

    pub fn teleport_to_cloud(
        &self,
        explicit_env_slug: Option<&str>,
    ) -> Result<SessionTeleportReceipt, String> {
        teleport_session_to_cloud(
            &self.config_dir,
            &self.work_dir,
            self.model.as_deref(),
            &self.env,
            explicit_env_slug,
        )
    }

    pub fn pull_from_cloud(&self) -> Result<SessionTeleportReceipt, String> {
        pull_session_from_cloud(&self.config_dir, &self.env)
    }

    /// Executes a complete multi-step Agent Turn using this SDK session's configured
    /// `SylphxExecutionMode` (`AutoCloudFirst`, `LocalOnly`, or `CloudOnly`).
    pub fn run_turn(&self, user_prompt: &str) -> Result<TurnUsage, String> {
        let state = Arc::new(Mutex::new(SharedState::default()));
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let exe = crate::clean_exe_path(
            &std::env::current_exe().unwrap_or_else(|_| PathBuf::from("anyagents")),
        );
        let environment = self.env.clone().into();
        // A submitted/unknown cloud effect must not be repeated locally or on a new session.
        let selected = if self.execution_mode == SylphxExecutionMode::LocalOnly {
            None
        } else {
            crate::resolve_cloud_agent_environment(&self.config_dir, &self.env)?
        };
        if self.execution_mode == SylphxExecutionMode::CloudOnly && selected.is_none() {
            return Err("cloud execution requires an explicit authorized environment".into());
        }
        if let Some(env_slug) = selected {
            run_cloud_session_turn(
                &state,
                &cancel_flag,
                &self.config_dir,
                &self.work_dir,
                self.model.as_deref(),
                &environment,
                &exe,
                &[],
                &env_slug,
                user_prompt,
            )
        } else {
            run_native_gateway_turn(
                &state,
                &cancel_flag,
                &self.config_dir,
                &self.work_dir,
                self.model.as_deref(),
                &environment,
                &exe,
                &[],
                user_prompt,
            )
        }
    }

    /// Runs an interactive terminal REPL / CLI session directly on top of the Core SDK,
    /// supporting `/cloud` (teleport session to Sylphx Cloud), `/local` (pull session back
    /// to local), `/status`, and `/exit` without requiring an AnyAgents daemon.
    pub fn run_interactive_repl(&mut self) -> Result<(), String> {
        let stdin = std::io::stdin();
        let mut stdout = std::io::stdout();
        loop {
            write!(stdout, "sylphx ({:?})> ", self.execution_mode).map_err(|e| e.to_string())?;
            stdout.flush().map_err(|e| e.to_string())?;
            let mut line = String::new();
            let n = stdin
                .lock()
                .read_line(&mut line)
                .map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match trimmed {
                "/exit" | "/quit" => break,
                "/cloud" | "/teleport" => {
                    let receipt = self.teleport_to_cloud(None)?;
                    self.execution_mode = SylphxExecutionMode::AutoCloudFirst;
                    writeln!(
                        stdout,
                        "[teleported to cloud session: {:?}]",
                        receipt.session_id
                    )
                    .map_err(|e| e.to_string())?;
                    continue;
                }
                "/local" | "/pull" => {
                    let receipt = self.pull_from_cloud()?;
                    self.execution_mode = SylphxExecutionMode::LocalOnly;
                    writeln!(
                        stdout,
                        "[pulled {} events to local transcript ({} total items)]",
                        receipt.synced_events, receipt.transcript_items
                    )
                    .map_err(|e| e.to_string())?;
                    continue;
                }
                _ => {}
            }
            let usage = self.run_turn(trimmed)?;
            writeln!(
                stdout,
                "[turn complete: in={} out={} cache_read={}]",
                usage.input_tokens, usage.output_tokens, usage.cache_read_tokens
            )
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
