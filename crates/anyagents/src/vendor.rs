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

pub enum VendorHarness {
    Claude(Box<ClaudeHarness>, PathBuf),
    Codex(Box<CodexHarness>, PathBuf),
    OpenCode(Box<OpenCodeHarness>),
    Agy(Box<AgyHarness>, PathBuf),
    Muse(Box<MuseHarness>, PathBuf),
    Omp(Box<OmpHarness>, PathBuf),
    Dsh(Box<DshHarness>, PathBuf),
    Grok(Box<GrokHarness>, PathBuf),
    Contract(Box<ContractHarness>),
}

impl VendorHarness {
    fn sync_session(&self) {
        let (session, path) = match self {
            Self::Claude(harness, path) => (harness.session_id(), Some(path)),
            Self::Codex(harness, path) => (harness.session_id(), Some(path)),
            Self::Agy(harness, path) => (harness.session_id(), Some(path)),
            Self::Muse(harness, path) => (harness.session_id(), Some(path)),
            Self::Omp(harness, path) => (harness.session_id(), Some(path)),
            Self::Dsh(harness, path) => (harness.session_id(), Some(path)),
            Self::Grok(harness, path) => (harness.session_id(), Some(path)),
            Self::OpenCode(_) | Self::Contract(_) => (None, None),
        };
        if let (Some(id), Some(path)) = (session, path) {
            let trimmed = id.trim();
            if !trimmed.is_empty() {
                let existing = std::fs::read_to_string(path).ok();
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

pub fn vendor_launch(request: LaunchRequest) -> Result<VendorHarness, String> {
    // Strip any untrusted launch or authority variables from caller/stored env.
    let mut env: Vec<(String, String)> = request
        .extra_env
        .into_iter()
        .filter(|(k, _)| {
            !matches!(
                k.as_str(),
                anyagents_harness::SEAT_LAUNCH_ENV
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
    let parent_name = env
        .iter()
        .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
        .map(|(_, v)| v.clone());
    let seat_spec =
        crate::sandbox::seat_launch_spec_with_hint(&request.agent.name, parent_name.as_deref());
    if request.agent.harness == "agy" {
        ensure_agy_steering_settings(&request.config_dir);
    }
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() {
        let _ = crate::install::record(&root, &request.agent.harness);
        crate::create::write_agent_instructions(
            &request.config_dir,
            &request.agent,
            parent_name.as_deref(),
        );
    }
    if let Some((ref seat_user, ref seat_launch)) = seat_spec {
        crate::create::ensure_seat_environment(&request.config_dir);
        if let Ok(store) = anyagents_core::Store::open(request.data_dir.join("state.db")) {
            crate::create::seed_harness_credentials(
                &store,
                &request.data_dir,
                &request.agent,
                &request.config_dir,
            );
            if request.agent.harness == "opencode"
                && let Ok(exe) = std::env::current_exe()
            {
                let mcp_args = vec![
                    "--data".to_string(),
                    request.data_dir.display().to_string(),
                    "mcp".to_string(),
                    "--config".to_string(),
                    request.config_dir.display().to_string(),
                ];
                let _ = crate::opencode_config::write(
                    &request.config_dir,
                    request.agent.model.as_deref(),
                    &exe,
                    &mcp_args,
                );
            } else if request.agent.harness == "codex"
                && let Ok(exe) = std::env::current_exe()
            {
                let mcp_args = vec![
                    "--data".to_string(),
                    request.data_dir.display().to_string(),
                    "mcp".to_string(),
                    "--config".to_string(),
                    request.config_dir.display().to_string(),
                ];
                let _ = crate::mcp_config::write_codex_with_model(
                    &request.config_dir.join("config.toml"),
                    request.agent.model.as_deref(),
                    &exe,
                    &mcp_args,
                );
            } else if request.agent.harness == "omp"
                && let Ok(exe) = std::env::current_exe()
            {
                let mcp_args = vec![
                    "--data".to_string(),
                    request.data_dir.display().to_string(),
                    "mcp".to_string(),
                    "--config".to_string(),
                    request.config_dir.display().to_string(),
                ];
                let _ = crate::grok::write_omp_mcp_config(&request.config_dir, &exe, &mcp_args);
            } else if request.agent.harness == "dsh"
                && let Ok(exe) = std::env::current_exe()
            {
                let mcp_args = vec![
                    "--data".to_string(),
                    request.data_dir.display().to_string(),
                    "mcp".to_string(),
                    "--config".to_string(),
                    request.config_dir.display().to_string(),
                ];
                let _ = crate::dsh::write_dsh_mcp_patch(
                    &request.config_dir,
                    request.agent.model.as_deref(),
                    &exe,
                    &mcp_args,
                );
            } else if request.agent.harness == "grok"
                && let Ok(exe) = std::env::current_exe()
            {
                let mcp_args = vec![
                    "--data".to_string(),
                    request.data_dir.display().to_string(),
                    "mcp".to_string(),
                    "--config".to_string(),
                    request.config_dir.display().to_string(),
                ];
                let _ = crate::grok::write_grok_config(
                    &request.config_dir,
                    request.agent.model.as_deref(),
                    &exe,
                    &mcp_args,
                );
            } else if request.agent.harness == "muse"
                && let Ok(exe) = std::env::current_exe()
            {
                let mcp_args = vec![
                    "--data".to_string(),
                    request.data_dir.display().to_string(),
                    "mcp".to_string(),
                    "--config".to_string(),
                    request.config_dir.display().to_string(),
                ];
                let _ = crate::muse_config::write(
                    &request.config_dir,
                    request.agent.model.as_deref(),
                    &exe,
                    &mcp_args,
                );
            }
        }
        let cache_home = request.config_dir.join("cache-home");
        let _ = std::fs::create_dir_all(&cache_home);
        let tmp_dir = request.config_dir.join("tmp");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let _ = std::fs::create_dir_all(request.config_dir.join("data-home"));
        let _ = std::fs::create_dir_all(request.config_dir.join("state-home"));
        if let Some((_, _, gid, _)) =
            crate::sandbox::seat_for_agent_with_hint(&request.agent.name, parent_name.as_deref())
        {
            crate::sandbox::share_config_with_seat(&request.config_dir, gid);
        }
        crate::create::ensure_seat_op_config(&request.config_dir, seat_launch);
        env.push(("HOME".to_string(), request.config_dir.display().to_string()));
        env.push(("USER".to_string(), seat_user.clone()));
        env.push(("LOGNAME".to_string(), seat_user.clone()));
        env.push((
            "XDG_CACHE_HOME".to_string(),
            cache_home.display().to_string(),
        ));
        env.push(("TMPDIR".to_string(), tmp_dir.display().to_string()));
    }
    if let Some((_, ref spec)) = seat_spec {
        env.push((anyagents_harness::SEAT_LAUNCH_ENV.to_string(), spec.clone()));
    }
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
        .map_err(|err| err.to_string())?;
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
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("anyagents"));
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
        .map_err(|err| err.to_string())?;
        Ok(VendorHarness::OpenCode(Box::new(harness)))
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
            if let Some(model) = request
                .agent
                .model
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
            {
                args.push("--model".to_string());
                args.push(model.to_string());
            }
            args.push("--effort".to_string());
            args.push("medium".to_string());
            args.push("--name".to_string());
            args.push(request.agent.name.clone());
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

/// Ensures `agy`'s `"queuedMessages": "send-immediately"` setting is active in
/// the agent's config directory and removes any legacy file-hook steering
/// files (`anyagents-steer-hook.sh`, `steer.jsonl`, and the `"anyagents-steer"`
/// entry in `hooks.json`), since `AgyHarness` steers natively via
/// `LanguageServerService/SendUserCascadeMessage`.
fn ensure_agy_steering_settings(config_dir: &Path) {
    let dir = config_dir.join(".gemini").join("antigravity-cli");
    if std::fs::create_dir_all(&dir).is_ok() {
        let path = dir.join("settings.json");
        let mut map = std::fs::read_to_string(&path)
            .ok()
            .and_then(|text| {
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&text).ok()
            })
            .unwrap_or_default();
        if map.get("queuedMessages").and_then(|v| v.as_str()) != Some("send-immediately") {
            map.insert(
                "queuedMessages".to_string(),
                serde_json::Value::String("send-immediately".to_string()),
            );
            if let Ok(body) = serde_json::to_string_pretty(&serde_json::Value::Object(map)) {
                let _ = std::fs::write(&path, format!("{body}\n"));
            }
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
            Self::OpenCode(harness) => harness.deliver(message),
            Self::Agy(harness, _) => harness.deliver(message),
            Self::Muse(harness, _) => harness.deliver(message),
            Self::Omp(harness, _) => harness.deliver(message),
            Self::Dsh(harness, _) => harness.deliver(message),
            Self::Grok(harness, _) => harness.deliver(message),
            Self::Contract(harness) => harness.deliver(message),
        };
        self.sync_session();
        out
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness, _) => harness.background(),
            Self::Codex(harness, _) => harness.background(),
            Self::OpenCode(harness) => harness.background(),
            Self::Agy(harness, _) => harness.background(),
            Self::Muse(harness, _) => harness.background(),
            Self::Omp(harness, _) => harness.background(),
            Self::Dsh(harness, _) => harness.background(),
            Self::Grok(harness, _) => harness.background(),
            Self::Contract(harness) => harness.background(),
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let out = match self {
            Self::Claude(harness, _) => harness.resume(session),
            Self::Codex(harness, _) => harness.resume(session),
            Self::OpenCode(harness) => harness.resume(session),
            Self::Agy(harness, _) => harness.resume(session),
            Self::Muse(harness, _) => harness.resume(session),
            Self::Omp(harness, _) => harness.resume(session),
            Self::Dsh(harness, _) => harness.resume(session),
            Self::Grok(harness, _) => harness.resume(session),
            Self::Contract(harness) => harness.resume(session),
        };
        self.sync_session();
        out
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness, _) => harness.cancel(),
            Self::Codex(harness, _) => harness.cancel(),
            Self::OpenCode(harness) => harness.cancel(),
            Self::Agy(harness, _) => harness.cancel(),
            Self::Muse(harness, _) => harness.cancel(),
            Self::Omp(harness, _) => harness.cancel(),
            Self::Dsh(harness, _) => harness.cancel(),
            Self::Grok(harness, _) => harness.cancel(),
            Self::Contract(harness) => harness.cancel(),
        }
    }

    fn health(&self) -> HarnessHealth {
        self.sync_session();
        match self {
            Self::Claude(harness, _) => harness.health(),
            Self::Codex(harness, _) => harness.health(),
            Self::OpenCode(harness) => harness.health(),
            Self::Agy(harness, _) => harness.health(),
            Self::Muse(harness, _) => harness.health(),
            Self::Omp(harness, _) => harness.health(),
            Self::Dsh(harness, _) => harness.health(),
            Self::Grok(harness, _) => harness.health(),
            Self::Contract(harness) => harness.health(),
        }
    }

    fn input_ready(&self) -> bool {
        match self {
            Self::Claude(harness, _) => harness.input_ready(),
            Self::Codex(harness, _) => harness.input_ready(),
            Self::OpenCode(harness) => harness.input_ready(),
            Self::Agy(harness, _) => harness.input_ready(),
            Self::Muse(harness, _) => harness.input_ready(),
            Self::Omp(harness, _) => harness.input_ready(),
            Self::Dsh(harness, _) => harness.input_ready(),
            Self::Grok(harness, _) => harness.input_ready(),
            Self::Contract(harness) => harness.input_ready(),
        }
    }

    fn last_usage(&self) -> TurnUsage {
        self.sync_session();
        match self {
            Self::Claude(harness, _) => harness.last_usage(),
            Self::Codex(harness, _) => harness.last_usage(),
            Self::OpenCode(harness) => harness.last_usage(),
            Self::Agy(harness, _) => harness.last_usage(),
            Self::Muse(harness, _) => harness.last_usage(),
            Self::Omp(harness, _) => harness.last_usage(),
            Self::Dsh(harness, _) => harness.last_usage(),
            Self::Grok(harness, _) => harness.last_usage(),
            Self::Contract(harness) => harness.last_usage(),
        }
    }

    fn quiet_for(&self, id: &str, quiet: Duration, patience: Duration) -> bool {
        match self {
            Self::Claude(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Codex(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::OpenCode(harness) => harness.quiet_for(id, quiet, patience),
            Self::Agy(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Muse(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Omp(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Dsh(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Grok(harness, _) => harness.quiet_for(id, quiet, patience),
            Self::Contract(harness) => harness.quiet_for(id, quiet, patience),
        }
    }

    fn ends_on_quiet(&self) -> Option<Duration> {
        match self {
            Self::Claude(harness, _) => harness.ends_on_quiet(),
            Self::Codex(harness, _) => harness.ends_on_quiet(),
            Self::OpenCode(harness) => harness.ends_on_quiet(),
            Self::Agy(harness, _) => harness.ends_on_quiet(),
            Self::Muse(harness, _) => harness.ends_on_quiet(),
            Self::Omp(harness, _) => harness.ends_on_quiet(),
            Self::Dsh(harness, _) => harness.ends_on_quiet(),
            Self::Grok(harness, _) => harness.ends_on_quiet(),
            Self::Contract(harness) => harness.ends_on_quiet(),
        }
    }

    fn clear_quota(&mut self) {
        match self {
            Self::Claude(harness, _) => harness.clear_quota(),
            Self::Codex(harness, _) => harness.clear_quota(),
            Self::OpenCode(harness) => harness.clear_quota(),
            Self::Agy(harness, _) => harness.clear_quota(),
            Self::Muse(harness, _) => harness.clear_quota(),
            Self::Omp(harness, _) => harness.clear_quota(),
            Self::Dsh(harness, _) => harness.clear_quota(),
            Self::Grok(harness, _) => harness.clear_quota(),
            Self::Contract(harness) => harness.clear_quota(),
        }
    }

    fn process_id(&self) -> Option<u32> {
        match self {
            Self::Claude(harness, _) => harness.process_id(),
            Self::Codex(harness, _) => harness.process_id(),
            Self::OpenCode(harness) => harness.process_id(),
            Self::Agy(harness, _) => harness.process_id(),
            Self::Muse(harness, _) => harness.process_id(),
            Self::Omp(harness, _) => harness.process_id(),
            Self::Dsh(harness, _) => harness.process_id(),
            Self::Grok(harness, _) => harness.process_id(),
            Self::Contract(harness) => harness.process_id(),
        }
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        match self {
            Self::Claude(harness, _) => harness.watch_activity(notify),
            Self::Codex(harness, _) => harness.watch_activity(notify),
            Self::OpenCode(harness) => harness.watch_activity(notify),
            Self::Agy(harness, _) => harness.watch_activity(notify),
            Self::Muse(harness, _) => harness.watch_activity(notify),
            Self::Omp(harness, _) => harness.watch_activity(notify),
            Self::Dsh(harness, _) => harness.watch_activity(notify),
            Self::Grok(harness, _) => harness.watch_activity(notify),
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
