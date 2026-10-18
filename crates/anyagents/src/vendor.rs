//! Builds the harness the supervisor asked for. The binary name can be overridden
//! so tests never start a real vendor. The agent's config directory is the
//! harness home; it is applied after any extra variables so those cannot point
//! the process at another home.

use std::path::Path;

use anyagents_harness::{
    DeliverOutcome, Harness, HarnessError, HarnessHealth, OutboundMessage, TurnUsage,
};
use anyagents_harness_claude::ClaudeHarness;
use anyagents_harness_codex::CodexHarness;
use anyagents_supervisor::LaunchRequest;

pub enum VendorHarness {
    Claude(ClaudeHarness),
    Codex(CodexHarness),
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
    if request.agent.harness == "codex" {
        env.push((
            "CODEX_HOME".to_string(),
            request.config_dir.display().to_string(),
        ));
        let program = std::env::var("ANYAGENTS_CODEX_BIN").unwrap_or_else(|_| "codex".to_string());
        let harness = start_codex(&program, &env, &request.work_dir)?;
        Ok(VendorHarness::Codex(harness))
    } else {
        env.push((
            "CLAUDE_CONFIG_DIR".to_string(),
            request.config_dir.display().to_string(),
        ));
        let program =
            std::env::var("ANYAGENTS_CLAUDE_BIN").unwrap_or_else(|_| "claude".to_string());
        let harness = start_claude(&program, &env, &request.work_dir)?;
        Ok(VendorHarness::Claude(harness))
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
        }
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness) => harness.background(),
            Self::Codex(harness) => harness.background(),
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness) => harness.resume(session),
            Self::Codex(harness) => harness.resume(session),
        }
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        match self {
            Self::Claude(harness) => harness.cancel(),
            Self::Codex(harness) => harness.cancel(),
        }
    }

    fn health(&self) -> HarnessHealth {
        match self {
            Self::Claude(harness) => harness.health(),
            Self::Codex(harness) => harness.health(),
        }
    }

    fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        match self {
            Self::Claude(harness) => harness.usage_for(id),
            Self::Codex(harness) => harness.usage_for(id),
        }
    }

    fn clear_quota(&mut self) {
        match self {
            Self::Claude(harness) => harness.clear_quota(),
            Self::Codex(harness) => harness.clear_quota(),
        }
    }

    fn process_id(&self) -> Option<u32> {
        match self {
            Self::Claude(harness) => harness.process_id(),
            Self::Codex(harness) => harness.process_id(),
        }
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        match self {
            Self::Claude(harness) => harness.watch_activity(notify),
            Self::Codex(harness) => harness.watch_activity(notify),
        }
    }
}
