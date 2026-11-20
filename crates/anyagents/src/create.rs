//! `agents.create`. The agent row, its directories, and its MCP registration
//! are one step. The harness config is written only under the agent's own
//! config directory.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, Store};

mod archetypes;
mod credentials;
mod model_defaults;
mod profile_identity;
mod profile_rotation;
mod profiles;
mod quota;
mod quota_probe;
mod runtime;
mod signin;
mod spawn;
mod workspace;

#[cfg(test)]
mod archetype_tests;
#[cfg(test)]
mod runtime_config_tests;
#[cfg(test)]
mod tests;

pub use spawn::{CreateRequest, create, format_agent_display};

pub(crate) use archetypes::{
    ArchetypeSource, ArchetypeTemplate, DEFAULT_AGENT_ROLE, SubagentDefaultSpec,
    SubagentDefaultsConfig, ancestor_archetype_workdirs, apply_archetype_defaults,
    backfill_historical_agent_roles, discover_available_archetype_slugs,
    discover_available_archetype_templates, effective_agent_instructions,
    hydrate_roles_from_store_if_postgres, load_subagent_defaults,
    mark_instructions_rotated_if_changed, normalize_or_validate_agent_role,
    parse_archetype_markdown, resolve_archetype_template_with_source, resolve_subagent_defaults,
    save_subagent_defaults, valid_template_stem,
};
pub(crate) use credentials::{
    propagate_profile_files_to_bound_agents, seed_harness_credentials, seeded_profile_name,
    sync_fresher_bound_agent_credentials, sync_fresher_bound_agent_credentials_in,
    write_profile_marker,
};
pub(crate) use model_defaults::persist_spawned_agent_config_to_store;
pub(crate) use profile_identity::{extract_profile_account_identity, update_profile_json_identity};
pub(crate) use profile_rotation::{force_refresh_provider_profiles, try_rotate_exhausted_profile};
pub(crate) use profiles::{
    ProviderProfileEntry, QuotaWindow, bound_profile_name, bound_profile_of_provider,
    compatible_harnesses_for, ensure_harness_runs_profile, ensure_profile_compatible_and_mapped,
    hydrate_profiles_from_store_if_postgres, list_harness_profiles, list_provider_profiles_fast,
    list_provider_profiles_fresh, set_provider_profile,
    sync_and_list_provider_profiles_with_options, upstream_provider_of,
};
#[cfg(test)]
pub(crate) use quota::format_openai_usage_summary;
pub(crate) use quota::{
    clear_stale_agy_quota_error_steps, enrich_profiles_usage_with_options,
    parse_quota_windows_from_usage, parse_remaining_pct_from_usage, probe_profile_models_catalog,
    probe_profile_remaining_pct, record_profile_quota_reset,
};
pub(crate) use quota_probe::{probe_one_profile_usage, probe_one_profile_usage_with_options};
#[cfg(test)]
pub(crate) use runtime::set_agent_harness;
pub(crate) use runtime::{
    agent_session_filename, compute_live_session_continuity, configure_agent_runtime,
    configure_agent_runtime_with_options, ensure_agy_mcp_config, harness_session_filename,
    maybe_notify_parent_on_error, poke_agent_wake_socket, poke_agent_wake_socket_mail,
    reconcile_agent_runtime_config, reconcile_agent_runtime_config_for_launch,
    reconcile_agent_runtime_config_for_live, reset_agent_session_locked, resolve_root_seat_hint,
    resolved_agent_seat_info, set_agent_harness_with_options, verify_harness_runtime_config,
};
pub(crate) use signin::{
    SigninStaging, deduplicate_same_account_profiles, finalize_agent_signin,
    resolve_and_commit_staged_signin,
};
pub(crate) use spawn::create_operator_root;
pub(crate) use workspace::{
    chmod_seat_file_readable, ensure_seat_environment, ensure_seat_op_config,
    extract_file_tilde_refs, sync_agent_description_files, sync_agent_description_files_for_live,
    sync_all_agent_instructions, write_agent_instructions,
    write_agent_instructions_with_continuity_and_store, write_required_file,
    write_seat_shared_file,
};

#[cfg(test)]
pub(crate) use archetypes::{
    BUILTIN_GENERAL_ROLE_BODY, INSTRUCTIONS_ROTATED_MARKER, resolve_archetype_template,
};
#[cfg(all(test, unix))]
pub(crate) use archetypes::{archetype_search_roots, resolve_archetype_template_with_ancestors};
#[cfg(test)]
pub(crate) use profiles::default_kind_and_harnesses;
#[cfg(test)]
pub(crate) use runtime::{agy_mcp_registered, mcp_server_args};
#[cfg(all(test, unix))]
pub(crate) use workspace::ensure_seat_skills;
#[cfg(test)]
pub(crate) use workspace::write_agent_instructions_with_continuity;

// Preserved `crate::create::*` API surface for items currently referenced only within
// their owning submodules.
#[allow(unused_imports)]
pub(crate) use archetypes::{
    archetype_search_roots_with_ancestors, discover_available_archetype_slugs_with_ancestors,
    has_configured_archetype_roots_with_ancestors, resolve_role_template_for_agent,
};
#[allow(unused_imports)]
pub(crate) use credentials::sync_anthropic_agent_credentials_from_profile;
#[allow(unused_imports)]
pub(crate) use profiles::profile_compatible_harnesses;
#[allow(unused_imports)]
pub(crate) use quota::USAGE_LIMITED_PREFIX;
#[allow(unused_imports)]
pub(crate) use runtime::{build_harness_continuity_summary, ensure_agy_mcp_registration};
#[allow(unused_imports)]
pub(crate) use signin::finalize_profile_signin;
#[allow(unused_imports)]
pub(crate) use workspace::{data_dir_from_config, read_shared_agents_md};

fn is_git_repo_dir(path: &Path) -> bool {
    std::process::Command::new("git")
        .args([
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "--git-dir"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn provision_cloud_sandbox_worktree_marker(
    store: &Store,
    agent: &Agent,
    config: &Path,
    repo_raw: &str,
    branch_opt: Option<&str>,
    base_opt: Option<&str>,
) -> Result<(), Error> {
    let branch = branch_opt
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("agent/{}", agent.name));
    let mut resolved_repo = repo_raw.trim().to_string();
    let mut initial_head = base_opt
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_default();

    if matches!(
        resolved_repo.to_ascii_lowercase().as_str(),
        "inherit" | "parent" | "worktree" | "branch" | "isolated"
    ) {
        if let Some(parent_wd) = agent
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok())
            .and_then(|p| p.workdir)
            .map(PathBuf::from)
            .filter(|p| p.is_dir())
        {
            if let Ok(out) = std::process::Command::new("git")
                .args([
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "core.hooksPath=/dev/null",
                ])
                .arg("-C")
                .arg(&parent_wd)
                .args(["config", "--get", "remote.origin.url"])
                .output()
                && out.status.success()
            {
                let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if !url.is_empty() {
                    resolved_repo = url;
                }
            }
            if initial_head.is_empty()
                && let Ok(out) = std::process::Command::new("git")
                    .args([
                        "-c",
                        "core.fsmonitor=false",
                        "-c",
                        "core.hooksPath=/dev/null",
                    ])
                    .arg("-C")
                    .arg(&parent_wd)
                    .args(["rev-parse", "HEAD"])
                    .output()
                && out.status.success()
            {
                initial_head = String::from_utf8_lossy(&out.stdout).trim().to_string();
            }
        }
        if matches!(
            resolved_repo.to_ascii_lowercase().as_str(),
            "inherit" | "parent" | "worktree" | "branch" | "isolated"
        ) && let Some(pid) = agent.parent_id.as_ref()
            && let Some(agents_dir) = config.parent().and_then(|p| p.parent())
        {
            let parent_marker = agents_dir
                .join(pid.as_str())
                .join("config/.anyagents-managed-worktree.json");
            if let Ok(raw) = std::fs::read_to_string(parent_marker)
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            {
                if let Some(r) = val
                    .get("repo")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    resolved_repo = r.to_string();
                }
                if initial_head.is_empty()
                    && let Some(b) = val
                        .get("base")
                        .or_else(|| val.get("initial_head"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                {
                    initial_head = b.to_string();
                }
            }
        }
        if matches!(
            resolved_repo.to_ascii_lowercase().as_str(),
            "inherit" | "parent" | "worktree" | "branch" | "isolated"
        ) {
            resolved_repo.clear();
        }
    } else if Path::new(&resolved_repo).is_dir() {
        let local_repo = Path::new(&resolved_repo);
        if let Ok(out) = std::process::Command::new("git")
            .args([
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.hooksPath=/dev/null",
            ])
            .arg("-C")
            .arg(local_repo)
            .args(["config", "--get", "remote.origin.url"])
            .output()
            && out.status.success()
        {
            let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !url.is_empty() {
                resolved_repo = url;
            }
        }
    }

    let payload = serde_json::json!({
        "path": crate::spiron::CLOUD_WORKSPACE_DIR,
        "cloud_sandbox": true,
        "repo": resolved_repo,
        "branch": branch,
        "base": initial_head,
        "initial_head": initial_head,
    });
    let bytes =
        serde_json::to_vec_pretty(&payload).map_err(|err| Error::Message(err.to_string()))?;
    write_required_file(
        &config.join(".anyagents-managed-worktree.json"),
        &bytes,
        0o664,
    )?;
    if store.is_postgres() {
        let _ = store.blob_store().put_agent_file(
            &agent.id,
            "config",
            ".anyagents-managed-worktree.json",
            &bytes,
            false,
        );
        if !resolved_repo.is_empty() {
            let repo_spec_bytes = serde_json::to_vec_pretty(&serde_json::json!({
                "repo": resolved_repo,
                "branch": branch,
                "base": initial_head,
            }))
            .unwrap_or_else(|_| bytes.clone());
            let _ = store.blob_store().put_agent_file(
                &agent.id,
                "config",
                "repo-spec.json",
                &repo_spec_bytes,
                false,
            );
        }
    }
    Ok(())
}

fn record_managed_worktree_marker(
    config: &Path,
    target_wt: &Path,
    repo_raw: &str,
    branch: Option<&str>,
) -> Result<(), Error> {
    let initial_head = std::process::Command::new("git")
        .args([
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .arg("-C")
        .arg(target_wt)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let effective_repo = if matches!(
        repo_raw.trim().to_ascii_lowercase().as_str(),
        "inherit" | "parent" | "worktree" | "branch" | "isolated" | "explicit-worktree"
    ) || Path::new(repo_raw).is_dir()
    {
        std::process::Command::new("git")
            .args([
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.hooksPath=/dev/null",
            ])
            .arg("-C")
            .arg(target_wt)
            .args(["config", "--get", "remote.origin.url"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| repo_raw.to_string())
    } else {
        repo_raw.to_string()
    };
    let payload = serde_json::json!({
        "path": target_wt.display().to_string(),
        "repo": effective_repo,
        "branch": branch.unwrap_or(""),
        "initial_head": initial_head,
    });
    let bytes =
        serde_json::to_vec_pretty(&payload).map_err(|err| Error::Message(err.to_string()))?;
    write_required_file(
        &config.join(".anyagents-managed-worktree.json"),
        &bytes,
        0o664,
    )
}

/// Fast-forward the checkout that holds the shared `skills` and `AGENTS.md`.
/// One `ls-remote` names origin's `main`; while it is the commit this process
/// last pulled, the checkout is neither scanned nor pulled.
#[cfg(unix)]
pub(crate) fn sync_canonical_skills(data_dir: &Path) {
    use std::collections::BTreeMap;
    use std::sync::{Mutex, PoisonError};
    static PULLED: Mutex<BTreeMap<PathBuf, String>> = Mutex::new(BTreeMap::new());
    let git = |repo_root: &Path| {
        let mut command = std::process::Command::new("git");
        command
            .args([
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.hooksPath=/dev/null",
            ])
            .arg("-C")
            .arg(repo_root);
        command
    };
    let repo_roots: std::collections::BTreeSet<PathBuf> = ["skills", "AGENTS.md"]
        .into_iter()
        .filter_map(|rel| std::fs::canonicalize(data_dir.join(rel)).ok())
        .filter_map(|resolved| resolved.parent().map(Path::to_path_buf))
        .filter(|repo_root| repo_root.join(".git").exists())
        .collect();
    for repo_root in repo_roots {
        let remote_main = git(&repo_root)
            .args(["ls-remote", "--quiet", "origin", "refs/heads/main"])
            .stderr(std::process::Stdio::null())
            .output()
            .ok()
            .filter(|out| out.status.success())
            .and_then(|out| {
                String::from_utf8_lossy(&out.stdout)
                    .split_whitespace()
                    .next()
                    .map(str::to_string)
            });
        let pulled = || PULLED.lock().unwrap_or_else(PoisonError::into_inner);
        if remote_main.is_some() && pulled().get(&repo_root) == remote_main.as_ref() {
            continue;
        }
        let is_clean = git(&repo_root)
            .args(["status", "--porcelain"])
            .output()
            .ok()
            .is_some_and(|o| o.status.success() && o.stdout.is_empty());
        if !is_clean {
            continue;
        }
        let fast_forwarded = git(&repo_root)
            .args(["pull", "--ff-only", "--quiet", "origin", "main"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        if fast_forwarded && let Some(commit) = remote_main {
            pulled().insert(repo_root, commit);
        }
    }
}
