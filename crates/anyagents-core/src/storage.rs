//! Storage-tier path resolution for separating control-plane state, pure source
//! workspaces, shared bare Git object pools, and disposable compute scratch.
//!
//! When unset, AnyAgents uses its single-root layout under `{data}` with zero
//! configuration. Deployments with multi-tier storage volumes can configure:
//! - `ANYAGENTS_STATE_DIR`: control-plane state (`state.db`, capabilities, configs)
//! - `ANYAGENTS_WORKSPACES_DIR`: managed per-agent workspaces (`{workspaces}/{id}`)
//! - `ANYAGENTS_SCRATCH_DIR`: each agent's own storage root on local disk
//!   (`{scratch}/agents/{id}`), holding its disposable `build/` and `tmp/`
//! - `ANYAGENTS_GIT_POOL_DIR`: the read-only Git object pool agents' own repositories
//!   borrow from (`{pool}/{org}/{repo}.git`; `{data}/git-pool` when unset)
//!
//! Source and other state that cannot be rebuilt stay on the workspaces and
//! state tiers. Everything under an agent's scratch root can be rebuilt, so the
//! server may evict its `build/` when the scratch budget is exceeded and removes
//! the whole root when the agent is archived.

use std::path::{Path, PathBuf};

use crate::AgentId;

pub const STATE_DIR_ENV: &str = "ANYAGENTS_STATE_DIR";
pub const WORKSPACES_DIR_ENV: &str = "ANYAGENTS_WORKSPACES_DIR";
pub const SCRATCH_DIR_ENV: &str = "ANYAGENTS_SCRATCH_DIR";
pub const GIT_POOL_DIR_ENV: &str = "ANYAGENTS_GIT_POOL_DIR";
pub const TEST_STORAGE_TIERS_ENV: &str = "ANYAGENTS_TEST_STORAGE_TIERS";
/// Total bytes of build output all agents may keep under `{scratch}/agents`
/// (and the legacy `{scratch}/builds`). A size such as `300G`, `300GiB` or a
/// byte count; `0` turns enforcement off. Default [`DEFAULT_SCRATCH_BUILD_BUDGET`].
pub const SCRATCH_BUILD_BUDGET_ENV: &str = "ANYAGENTS_SCRATCH_BUILD_BUDGET";
/// Where an archived agent's unpushed or uncommitted work is saved before its
/// checkout is deleted. Default `{data}/rescue`.
pub const RESCUE_DIR_ENV: &str = "ANYAGENTS_RESCUE_DIR";
/// Exported to every harness: the agent's own build directory, so build tools
/// and host wrappers (the desk's Cargo wrapper) put output under its root.
pub const AGENT_BUILD_DIR_ENV: &str = "ANYAGENTS_AGENT_BUILD_DIR";
/// 300 GiB.
pub const DEFAULT_SCRATCH_BUILD_BUDGET: u64 = 300 << 30;

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

/// Configured root directory for the Git object pool (`{pool}/{org}/{repo}.git`).
/// Unset, the checkout code uses `{data}/git-pool`.
pub fn configured_git_pool_dir() -> Option<PathBuf> {
    if !storage_env_enabled() {
        return None;
    }
    non_empty_path_env(GIT_POOL_DIR_ENV)
}

/// `{scratch}/agents/{id}`: the agent's own storage root on the scratch tier.
/// It holds only output that can be rebuilt, and nothing else writes there.
pub fn agent_scratch_root(scratch_dir: &Path, id: &AgentId) -> PathBuf {
    scratch_dir.join("agents").join(id.as_str())
}

/// `{scratch}/agents/{id}/build`: Cargo, Gradle and Xcode output.
pub fn agent_scratch_build_dir(scratch_dir: &Path, id: &AgentId) -> PathBuf {
    agent_scratch_root(scratch_dir, id).join("build")
}

/// `{scratch}/agents/{id}/tmp`: the agent's `TMPDIR`.
pub fn agent_scratch_tmp_dir(scratch_dir: &Path, id: &AgentId) -> PathBuf {
    agent_scratch_root(scratch_dir, id).join("tmp")
}

/// The layout before agent roots: `{scratch}/builds/{id}` and
/// `{scratch}/tmp/{id}`. A harness started by an older server keeps writing
/// there until it is restarted, so cleanup and the budget still cover them.
pub fn legacy_agent_scratch_dirs(scratch_dir: &Path, id: &AgentId) -> [PathBuf; 2] {
    [
        scratch_dir.join("builds").join(id.as_str()),
        scratch_dir.join("tmp").join(id.as_str()),
    ]
}

/// Every scratch directory that belongs to this agent, current and legacy.
pub fn agent_scratch_dirs(scratch_dir: &Path, id: &AgentId) -> [PathBuf; 3] {
    let [builds, tmp] = legacy_agent_scratch_dirs(scratch_dir, id);
    [agent_scratch_root(scratch_dir, id), builds, tmp]
}

/// The configured scratch build budget in bytes; `None` when enforcement is
/// off (no scratch tier, or a budget of `0`).
pub fn configured_scratch_build_budget() -> Option<u64> {
    configured_scratch_dir()?;
    let raw = std::env::var(SCRATCH_BUILD_BUDGET_ENV).unwrap_or_default();
    let budget = if raw.trim().is_empty() {
        DEFAULT_SCRATCH_BUILD_BUDGET
    } else {
        parse_size(&raw).unwrap_or_else(|| {
            eprintln!(
                "anyagents-storage: {SCRATCH_BUILD_BUDGET_ENV}={raw:?} is not a size; using {} GiB",
                DEFAULT_SCRATCH_BUILD_BUDGET >> 30
            );
            DEFAULT_SCRATCH_BUILD_BUDGET
        })
    };
    (budget > 0).then_some(budget)
}

/// `300G`, `300GiB`, `300 GB`, `1.5T`, `512M` or a plain byte count. Units are
/// binary, as `du -h` prints them.
pub fn parse_size(raw: &str) -> Option<u64> {
    let text = raw.trim();
    let split = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: f64 = number.parse().ok()?;
    let shift = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "k" | "kb" | "kib" => 10,
        "m" | "mb" | "mib" => 20,
        "g" | "gb" | "gib" => 30,
        "t" | "tb" | "tib" => 40,
        _ => return None,
    };
    if !number.is_finite() || number < 0.0 {
        return None;
    }
    Some((number * (1u64 << shift) as f64) as u64)
}

/// The rescue area: `ANYAGENTS_RESCUE_DIR`, else `{data}/rescue`.
pub fn rescue_dir(data_dir: &Path) -> PathBuf {
    if storage_env_enabled()
        && let Some(dir) = non_empty_path_env(RESCUE_DIR_ENV)
    {
        return dir;
    }
    data_dir.join("rescue")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_paths_share_one_root_per_agent() {
        let id = AgentId::generate();
        let scratch = Path::new("/var/tmp/scratch-tier");
        let root = PathBuf::from(format!("/var/tmp/scratch-tier/agents/{id}"));
        assert_eq!(agent_scratch_root(scratch, &id), root);
        assert_eq!(agent_scratch_build_dir(scratch, &id), root.join("build"));
        assert_eq!(agent_scratch_tmp_dir(scratch, &id), root.join("tmp"));
        assert_eq!(
            agent_scratch_dirs(scratch, &id),
            [
                root,
                PathBuf::from(format!("/var/tmp/scratch-tier/builds/{id}")),
                PathBuf::from(format!("/var/tmp/scratch-tier/tmp/{id}")),
            ]
        );
    }

    #[test]
    fn sizes_parse_in_binary_units() {
        assert_eq!(parse_size("300G"), Some(300 << 30));
        assert_eq!(parse_size(" 300 GiB "), Some(300 << 30));
        assert_eq!(parse_size("1.5T"), Some(3 << 39));
        assert_eq!(parse_size("512m"), Some(512 << 20));
        assert_eq!(parse_size("4096"), Some(4096));
        assert_eq!(parse_size("0"), Some(0));
        assert_eq!(parse_size("lots"), None);
        assert_eq!(parse_size("3 parsecs"), None);
        assert_eq!(parse_size("-1G"), None);
    }
}
