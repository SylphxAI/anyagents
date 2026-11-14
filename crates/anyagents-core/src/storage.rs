//! Storage-tier path resolution for separating control-plane state, pure source
//! workspaces, shared bare Git object pools, and disposable compute scratch.
//!
//! When unset, AnyAgents uses its single-root layout under `{data}` with zero
//! configuration. Deployments with multi-tier storage volumes can configure:
//! - `ANYAGENTS_STATE_DIR`: control-plane state (`state.db`, capabilities, configs)
//! - `ANYAGENTS_WORKSPACES_DIR`: managed per-agent workspaces (`{workspaces}/{id}`)
//! - `ANYAGENTS_SCRATCH_DIR`: disposable per-agent build/tmp outputs (`{scratch}/{builds,tmp}/{id}`)
//! - `ANYAGENTS_GIT_POOL_DIR`: the read-only Git object pool agents' own repositories
//!   borrow from (`{pool}/{org}/{repo}.git`; `{data}/git-pool` when unset)
//!
//! Source and other state that cannot be rebuilt belong on the workspaces and
//! state tiers. The server removes an agent's scratch directories when it is
//! archived, and evicts verifiable build output from them (and only that) when
//! the scratch build budget is exceeded.

use std::path::{Path, PathBuf};

use crate::AgentId;

pub const STATE_DIR_ENV: &str = "ANYAGENTS_STATE_DIR";
pub const WORKSPACES_DIR_ENV: &str = "ANYAGENTS_WORKSPACES_DIR";
pub const SCRATCH_DIR_ENV: &str = "ANYAGENTS_SCRATCH_DIR";
pub const GIT_POOL_DIR_ENV: &str = "ANYAGENTS_GIT_POOL_DIR";
pub const TEST_STORAGE_TIERS_ENV: &str = "ANYAGENTS_TEST_STORAGE_TIERS";
/// Total bytes of build output all agents may keep under `{scratch}/builds`. A size such as `300G`, `300GiB` or a
/// byte count; `0` turns enforcement off. Default [`DEFAULT_SCRATCH_BUILD_BUDGET`].
pub const SCRATCH_BUILD_BUDGET_ENV: &str = "ANYAGENTS_SCRATCH_BUILD_BUDGET";
/// `0` makes the server enforce the scratch build budget. Unset (or anything
/// else) it only logs what it would evict: a deployment turns enforcement on
/// once it has read a dry run.
pub const SCRATCH_BUILD_BUDGET_DRY_RUN_ENV: &str = "ANYAGENTS_SCRATCH_BUILD_BUDGET_DRY_RUN";
/// Build output on the scratch volume that no agent owns but that shares the
/// budget: `:`-separated directories whose entries are each one evictable
/// build directory. The last path component may end in `*`
/// (`/scratch/build-targets/cargo-*`).
pub const SCRATCH_BUILD_EXTRA_DIRS_ENV: &str = "ANYAGENTS_SCRATCH_BUILD_EXTRA_DIRS";
/// Directories, in the same form, that [`SCRATCH_BUILD_EXTRA_DIRS_ENV`] must
/// not cover: build output a host keeps on purpose (the desk janitor's
/// `retained_cache_users`).
pub const SCRATCH_BUILD_EXTRA_EXCLUDE_ENV: &str = "ANYAGENTS_SCRATCH_BUILD_EXTRA_EXCLUDE";
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

/// `{scratch}/builds/{id}`: the agent's build root. Cargo, Gradle and Xcode
/// output, and the private tool caches a seat gets (`cache/<tool>`), live
/// here. It stays where it always was: moving it would rebuild every agent's
/// output beside the old copy on a volume that cannot grow.
pub fn agent_scratch_build_dir(scratch_dir: &Path, id: &AgentId) -> PathBuf {
    scratch_dir.join("builds").join(id.as_str())
}

/// `{scratch}/tmp/{id}`: the agent's `TMPDIR`.
pub fn agent_scratch_tmp_dir(scratch_dir: &Path, id: &AgentId) -> PathBuf {
    scratch_dir.join("tmp").join(id.as_str())
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

/// Whether the budget only logs what it would evict (the default).
pub fn scratch_build_budget_dry_run() -> bool {
    std::env::var(SCRATCH_BUILD_BUDGET_DRY_RUN_ENV)
        .map(|value| !matches!(value.trim(), "0" | "false" | "off" | "no"))
        .unwrap_or(true)
}

/// The directories [`SCRATCH_BUILD_EXTRA_DIRS_ENV`] names, with a trailing
/// `*` in the last component expanded to the directories that exist now,
/// less those [`SCRATCH_BUILD_EXTRA_EXCLUDE_ENV`] names.
pub fn configured_scratch_build_extra_dirs() -> Vec<PathBuf> {
    let Ok(raw) = std::env::var(SCRATCH_BUILD_EXTRA_DIRS_ENV) else {
        return Vec::new();
    };
    let exclude = std::env::var(SCRATCH_BUILD_EXTRA_EXCLUDE_ENV).unwrap_or_default();
    extra_dirs_without(&raw, &exclude)
}

fn extra_dirs_without(raw: &str, exclude: &str) -> Vec<PathBuf> {
    let excluded = expand_extra_dirs(exclude);
    expand_extra_dirs(raw)
        .into_iter()
        .filter(|dir| !excluded.contains(dir))
        .collect()
}

fn expand_extra_dirs(raw: &str) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    for entry in raw.split(':').map(str::trim).filter(|e| !e.is_empty()) {
        let path = Path::new(entry);
        if !path.is_absolute() {
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        match (name.strip_suffix('*'), path.parent()) {
            (Some(prefix), Some(parent)) if !prefix.contains(['*', '?', '[']) => {
                let Ok(entries) = std::fs::read_dir(parent) else {
                    continue;
                };
                let mut found: Vec<PathBuf> = entries
                    .flatten()
                    .filter(|e| e.file_type().is_ok_and(|kind| kind.is_dir()))
                    .filter(|e| {
                        e.file_name()
                            .to_str()
                            .is_some_and(|n| n.starts_with(prefix) && !n.starts_with('.'))
                    })
                    .map(|e| e.path())
                    .collect();
                found.sort();
                dirs.extend(found);
            }
            (None, _) if path.is_dir() => dirs.push(path.to_path_buf()),
            _ => {}
        }
    }
    dirs
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

    #[test]
    fn extra_dirs_expand_a_trailing_star_and_skip_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for name in [
            "cargo-kyle",
            "cargo-seat-sage",
            "sccache-kyle",
            ".cargo-hidden",
        ] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        std::fs::write(root.join("cargo-file"), b"not a directory").unwrap();
        let raw = format!(
            "{}/cargo-*:relative/cargo-*:{}/missing:{}/sccache-kyle",
            root.display(),
            root.display(),
            root.display()
        );
        assert_eq!(
            expand_extra_dirs(&raw),
            [
                root.join("cargo-kyle"),
                root.join("cargo-seat-sage"),
                root.join("sccache-kyle")
            ]
        );
    }

    /// The desk janitor retains `kyle`'s caches: an excluded directory is
    /// never covered, however the include list is written.
    #[test]
    fn excluded_extra_dirs_are_never_covered() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for name in ["cargo-kyle", "cargo-seat-sage"] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        let include = format!("{}/cargo-*", root.display());
        let exclude = format!(
            "{}/cargo-kyle:{}/cargo-gone",
            root.display(),
            root.display()
        );
        assert_eq!(
            extra_dirs_without(&include, &exclude),
            [root.join("cargo-seat-sage")]
        );
        assert_eq!(
            extra_dirs_without(&include, ""),
            [root.join("cargo-kyle"), root.join("cargo-seat-sage")]
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
