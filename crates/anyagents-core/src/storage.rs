//! Storage-tier path resolution for separating control-plane state, pure source
//! workspaces, shared bare Git object pools, and disposable compute scratch.
//!
//! When unset, AnyAgents uses its single-root layout under `{data}` with zero
//! configuration. Deployments with multi-tier storage volumes can configure:
//! - `ANYAGENTS_STATE_DIR`: control-plane state (`state.db`, capabilities, configs)
//! - `ANYAGENTS_WORKSPACES_DIR`: managed per-agent workspaces (`{workspaces}/{id}`)
//! - `ANYAGENTS_SCRATCH_DIR`: disposable per-agent build/tmp outputs (`{scratch}/{builds,tmp}/{id}`)
//! - `ANYAGENTS_GIT_POOL_DIR`: shared bare Git object pool (`{pool}/{org}/{repo}.git`)

use std::path::{Path, PathBuf};

use crate::AgentId;

pub const STATE_DIR_ENV: &str = "ANYAGENTS_STATE_DIR";
pub const WORKSPACES_DIR_ENV: &str = "ANYAGENTS_WORKSPACES_DIR";
pub const SCRATCH_DIR_ENV: &str = "ANYAGENTS_SCRATCH_DIR";
pub const GIT_POOL_DIR_ENV: &str = "ANYAGENTS_GIT_POOL_DIR";
pub const TEST_STORAGE_TIERS_ENV: &str = "ANYAGENTS_TEST_STORAGE_TIERS";

/// Host storage-tier environment variables are ignored inside isolated unit and
/// integration test harnesses unless the test explicitly opts in via
/// `ANYAGENTS_TEST_STORAGE_TIERS=1`.
pub fn storage_env_enabled() -> bool {
    storage_env_enabled_for(cfg!(test))
}

fn storage_env_enabled_for(is_unit_test: bool) -> bool {
    let isolated = is_unit_test || std::env::var_os("ANYAGENTS_TESTS").is_some();
    !isolated || std::env::var_os(TEST_STORAGE_TIERS_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

fn non_empty_path_env(name: &str) -> Option<PathBuf> {
    let value = std::env::var_os(name)?;
    let trimmed = value.to_string_lossy();
    let trimmed = trimmed.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(PathBuf::from(trimmed))
}

/// Configured control-plane state directory (`ANYAGENTS_STATE_DIR` when tier
/// resolution is active, falling back to `ANYAGENTS_DATA`).
pub fn configured_state_dir() -> Option<PathBuf> {
    if storage_env_enabled()
        && let Some(state_dir) = non_empty_path_env(STATE_DIR_ENV)
    {
        return Some(state_dir);
    }
    non_empty_path_env("ANYAGENTS_DATA")
}

/// Configured root directory for managed per-agent workspaces (`{workspaces}/{id}`).
pub fn configured_workspaces_dir() -> Option<PathBuf> {
    if !storage_env_enabled() {
        return None;
    }
    non_empty_path_env(WORKSPACES_DIR_ENV)
}

/// Configured root directory for disposable per-agent build & temporary trees.
pub fn configured_scratch_dir() -> Option<PathBuf> {
    if !storage_env_enabled() {
        return None;
    }
    non_empty_path_env(SCRATCH_DIR_ENV)
}

/// Configured root directory for shared bare Git object pools (`{pool}/{org}/{repo}.git`).
pub fn configured_git_pool_dir() -> Option<PathBuf> {
    if !storage_env_enabled() {
        return None;
    }
    non_empty_path_env(GIT_POOL_DIR_ENV)
}

/// `{scratch}/builds/{id}`.
pub fn agent_scratch_build_dir(scratch_dir: &Path, id: &AgentId) -> PathBuf {
    scratch_dir.join("builds").join(id.as_str())
}

/// `{scratch}/tmp/{id}`.
pub fn agent_scratch_tmp_dir(scratch_dir: &Path, id: &AgentId) -> PathBuf {
    scratch_dir.join("tmp").join(id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_paths_scope_by_agent_id() {
        let id = AgentId::generate();
        let scratch = Path::new("/var/tmp/scratch-tier");
        assert_eq!(
            agent_scratch_build_dir(scratch, &id),
            PathBuf::from(format!("/var/tmp/scratch-tier/builds/{id}"))
        );
        assert_eq!(
            agent_scratch_tmp_dir(scratch, &id),
            PathBuf::from(format!("/var/tmp/scratch-tier/tmp/{id}"))
        );
    }
}
