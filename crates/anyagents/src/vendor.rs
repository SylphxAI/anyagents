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
use anyagents_harness_opencode::OpenCodeHarness;
use anyagents_harness_stream::{Launch, StreamHarness};
use anyagents_supervisor::LaunchRequest;

pub enum VendorHarness {
    Claude(Box<ClaudeHarness>),
    Codex(Box<CodexHarness>),
    OpenCode(Box<OpenCodeHarness>),
    Agy(Box<AgyHarness>),
    Muse(Box<MuseHarness>),
    Contract(Box<ContractHarness>),
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
    let mut env = request.extra_env;
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
    let mut launch_env = request.agent.launch_env.clone();
    env.append(&mut launch_env);
    env.push((
        "AGY_CONFIG_DIR".to_string(),
        request.config_dir.display().to_string(),
    ));
    env.push((
        "MUSE_CONFIG_HOME".to_string(),
        request.config_dir.display().to_string(),
    ));
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
        // The agent's own config directory carries the steering hook, so a
        // message reaches the running turn instead of waiting for the next one.
        let harness =
            AgyHarness::start(&program, None, &pairs(&env)).map_err(|err| err.to_string())?;
        Ok(VendorHarness::Agy(Box::new(harness)))
    } else if request.agent.harness == "muse" {
        let program = std::env::var("ANYAGENTS_MUSE_BIN").unwrap_or_else(|_| "muse".to_string());
        let harness = MuseHarness::start_in(&program, None, &pairs(&env), Some(&request.work_dir))
            .map_err(|err| err.to_string())?;
        Ok(VendorHarness::Muse(Box::new(harness)))
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
        let session = opencode_session(&request.config_dir);
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
        let program = crate::install::program(&root, &request.agent.harness);
        let harness = start_codex(&program.display().to_string(), &env, &request.work_dir)?;
        Ok(VendorHarness::Codex(Box::new(harness)))
    } else {
        env.push((
            "CLAUDE_CONFIG_DIR".to_string(),
            request.config_dir.display().to_string(),
        ));
        let program = crate::install::program(&root, &request.agent.harness);
        let harness = start_claude(&program.display().to_string(), &env, &request.work_dir)?;
        Ok(VendorHarness::Claude(Box::new(harness)))
    }
}

fn start_claude(
    program: &str,
    env: &[(String, String)],
    work_dir: &Path,
) -> Result<ClaudeHarness, String> {
    let pairs = pairs(env);
    ClaudeHarness::start_in(program, None, &pairs, Some(work_dir)).map_err(|err| err.to_string())
}

fn start_codex(
    program: &str,
    env: &[(String, String)],
    work_dir: &Path,
) -> Result<CodexHarness, String> {
    let pairs = pairs(env);
    CodexHarness::start_in(program, None, &pairs, Some(work_dir)).map_err(|err| err.to_string())
}

fn pairs(env: &[(String, String)]) -> Vec<(&str, &str)> {
    env.iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

impl Harness for VendorHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        match self {
            Self::Claude(harness) => harness.deliver(message),
            Self::Codex(harness) => harness.deliver(message),
            Self::OpenCode(harness) => harness.deliver(message),
            Self::Agy(harness) => harness.deliver(message),
            Self::Muse(harness) => harness.deliver(message),
            Self::Contract(harness) => harness.deliver(message),
        }
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness) => harness.background(),
            Self::Codex(harness) => harness.background(),
            Self::OpenCode(harness) => harness.background(),
            Self::Agy(harness) => harness.background(),
            Self::Muse(harness) => harness.background(),
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
            Self::Contract(harness) => harness.cancel(),
        }
    }

    fn health(&self) -> HarnessHealth {
        match self {
            Self::Claude(harness) => harness.health(),
            Self::Codex(harness) => harness.health(),
            Self::OpenCode(harness) => harness.health(),
            Self::Agy(harness) => harness.health(),
            Self::Muse(harness) => harness.health(),
            Self::Contract(harness) => harness.health(),
        }
    }

    fn last_usage(&self) -> TurnUsage {
        match self {
            Self::Claude(harness) => harness.last_usage(),
            Self::Codex(harness) => harness.last_usage(),
            Self::OpenCode(harness) => harness.last_usage(),
            Self::Agy(harness) => harness.last_usage(),
            Self::Muse(harness) => harness.last_usage(),
            Self::Contract(harness) => harness.last_usage(),
        }
    }

    fn quiet_for(&self, id: &str, quiet: Duration, patience: Duration) -> bool {
        match self {
            Self::Claude(harness) => harness.quiet_for(id, quiet, patience),
            Self::Codex(harness) => harness.quiet_for(id, quiet, patience),
            Self::OpenCode(harness) => harness.quiet_for(id, quiet, patience),
            Self::Agy(harness) => harness.quiet_for(id, quiet, patience),
            Self::Muse(harness) => harness.quiet_for(id, quiet, patience),
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
            Self::Contract(harness) => harness.watch_activity(notify),
        }
    }
}

/// The OpenCode session id this agent last ran, kept beside its config.
fn opencode_session(config_dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(config_dir.join("opencode-session-id")).ok()?;
    let id = text.trim();
    (!id.is_empty()).then(|| id.to_string())
}
