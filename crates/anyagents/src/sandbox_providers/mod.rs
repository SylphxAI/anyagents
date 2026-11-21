//! Orchestrator-side pluggable sandbox provider fleet (`LocalHost`, `Docker`, `E2B`,
//! `Modal`, `Daytona`, `Cloudflare`, `GenericHttpOci`, and native `Sylphx` leases) and
//! `ANYAGENTS_SANDBOX_*` environment compatibility layered over Master `agent_tools`.
#![allow(dead_code)]

use std::path::Path;

pub(crate) mod image_version;
pub(crate) mod lifecycle;
pub mod providers;
pub mod sylphx;

#[cfg(test)]
mod tests;

pub use sylphx::{
    SandboxMode, SylphxProviderConfig, destroy_sandbox_for_config_dir,
    release_sandbox_for_config_dir, renew_lease_for_config_dir, serverless_enabled_from_env,
};

pub(crate) const DEFAULT_EXEC_TIMEOUT_MS: u64 = 30_000;

pub(crate) fn url_encode_query_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 8);
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn anyagents_remote_spawn_flag() -> bool {
    std::env::var("ANYAGENTS_SANDBOX_REMOTE_SPAWN")
        .or_else(|_| std::env::var("AGENT_SANDBOX_REMOTE_SPAWN"))
        .ok()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true"))
}

/// Load `<config_dir>/sandbox.json`, honoring both `ANYAGENTS_SANDBOX_REMOTE_SPAWN`
/// and `AGENT_SANDBOX_REMOTE_SPAWN`.
pub fn sandbox_config_from_config_dir(config_dir: &Path) -> Option<agent_tools::SandboxConfig> {
    let mut cfg = agent_tools::SandboxConfig::from_config_dir(config_dir)?;
    if anyagents_remote_spawn_flag() {
        cfg.remote_spawn = true;
    }
    Some(cfg)
}

/// Resolve [`agent_tools::SandboxConfig`] from `<config_dir>/sandbox.json`,
/// `ANYAGENTS_SANDBOX_*`, or `AGENT_SANDBOX_*`.
pub fn resolve_sandbox_config(config_dir: Option<&Path>) -> Option<agent_tools::SandboxConfig> {
    if let Some(dir) = config_dir
        && let Some(cfg) = sandbox_config_from_config_dir(dir)
    {
        return Some(cfg);
    }
    if let Some(cfg) = sandbox_config_from_anyagents_env() {
        return Some(cfg);
    }
    agent_tools::SandboxConfig::from_env()
}

fn sandbox_config_from_anyagents_env() -> Option<agent_tools::SandboxConfig> {
    let endpoint = std::env::var("ANYAGENTS_SANDBOX_API_URL")
        .ok()?
        .trim()
        .trim_end_matches('/')
        .to_string();
    let sandbox_id = std::env::var("ANYAGENTS_SANDBOX_ID")
        .ok()?
        .trim()
        .to_string();
    if endpoint.is_empty() || sandbox_id.is_empty() {
        return None;
    }
    let token = std::env::var("ANYAGENTS_SANDBOX_TOKEN")
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    let workdir = std::env::var("ANYAGENTS_SANDBOX_WORKDIR")
        .ok()
        .map(|w| w.trim().to_string())
        .filter(|w| !w.is_empty())
        .unwrap_or_else(|| "/workspace".to_string());
    Some(agent_tools::SandboxConfig {
        endpoint,
        sandbox_id,
        token,
        workdir: workdir.trim_end_matches('/').to_string(),
        remote_spawn: anyagents_remote_spawn_flag(),
    })
}

/// Resolve [`agent_tools::SandboxProviderConfig`] from `<data_dir>/sandbox-provider.json`,
/// `ANYAGENTS_SANDBOX_PROVIDER_*`, or `AGENT_SANDBOX_PROVIDER_*`.
pub fn resolve_sandbox_provider_config(
    data_dir: Option<&Path>,
) -> Option<agent_tools::SandboxProviderConfig> {
    if let Some(dir) = data_dir
        && let Some(mut cfg) = agent_tools::SandboxProviderConfig::from_data_dir(dir)
    {
        if anyagents_remote_spawn_flag() {
            cfg.remote_spawn = true;
        }
        return Some(cfg);
    }
    if let Some(cfg) = sandbox_provider_config_from_anyagents_env() {
        return Some(cfg);
    }
    agent_tools::SandboxProviderConfig::from_env()
}

fn sandbox_provider_config_from_anyagents_env() -> Option<agent_tools::SandboxProviderConfig> {
    let endpoint = std::env::var("ANYAGENTS_SANDBOX_PROVIDER_URL")
        .ok()?
        .trim()
        .trim_end_matches('/')
        .to_string();
    if endpoint.is_empty() {
        return None;
    }
    let token = std::env::var("ANYAGENTS_SANDBOX_PROVIDER_TOKEN")
        .or_else(|_| std::env::var("ANYAGENTS_SANDBOX_TOKEN"))
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    let default_workdir = std::env::var("ANYAGENTS_SANDBOX_WORKDIR")
        .ok()
        .map(|w| w.trim().trim_end_matches('/').to_string())
        .filter(|w| !w.is_empty())
        .unwrap_or_else(|| "/workspace".to_string());
    Some(agent_tools::SandboxProviderConfig {
        endpoint,
        token,
        default_workdir,
        remote_spawn: anyagents_remote_spawn_flag(),
    })
}
