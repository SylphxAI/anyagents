//! Agent creation, private directory lifecycle, rollback, and managed worktree provisioning.

use std::path::{Path, PathBuf};

use anyagents_core::{
    Agent, AgentId, Error, ExecutionRuntime, NewAgent, Store, agent_config_dir, agent_work_dir,
};
use anyagents_harness::{normalize_harness_name, resolve_harness_and_runtime};

use crate::install;

use super::archetypes::resolve_role_template_with_ancestors;
use super::{
    DEFAULT_AGENT_ROLE, ancestor_archetype_workdirs, ensure_harness_runs_profile,
    ensure_profile_compatible_and_mapped, is_git_repo_dir, provision_cloud_sandbox_worktree_marker,
    reconcile_agent_runtime_config, record_managed_worktree_marker, resolved_agent_seat_info,
    upstream_provider_of, write_profile_marker,
};

#[derive(Default)]
pub struct CreateRequest {
    pub name: String,
    pub role: Option<String>,
    pub harness: String,
    pub runtime: Option<String>,
    pub budget_cap_micros: Option<u64>,
    pub model: Option<String>,
    pub profile: Option<String>,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub base: Option<String>,
    pub workdir: Option<PathBuf>,
    /// The working directory an owner agent gets when none is named.
    pub workdir_default: Option<PathBuf>,
    pub description: Option<String>,
    pub instructions: Option<String>,
    pub parent_id: Option<AgentId>,
}

pub fn format_agent_display(name: &str, role: Option<&str>) -> String {
    match role.map(str::trim).filter(|s| !s.is_empty()) {
        Some(r) => format!("{name} · {r}"),
        None => name.to_string(),
    }
}

/// New config roots are private before credentials or a seat grant exist.
/// Parent directories are created first with umask-governed traversable
/// permissions; only the target directory itself is restricted to `0o700`,
/// leaving an existing root's admitted group/mode unchanged.
pub(super) fn create_private_config_dir(config: &Path) -> std::io::Result<()> {
    if let Some(parent) = config.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(config) {
        Ok(()) => Ok(()),
        Err(err)
            if err.kind() == std::io::ErrorKind::AlreadyExists
                && std::fs::symlink_metadata(config).is_ok_and(|m| m.is_dir()) =>
        {
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// What one creation attempt made on disk, so that a failed attempt removes
/// exactly that and nothing that existed before it.
#[derive(Default)]
pub(super) struct MadeDirs {
    /// `<data>/agents/<id>`, which only this attempt's row names, and whether
    /// this attempt created it.
    root: Option<(PathBuf, bool)>,
    /// Work directories and their parents this attempt created, oldest
    /// first. Rollback removes each only while it is empty.
    dirs: Vec<PathBuf>,
}

/// Unix permission bits of `path` itself, never of a symlink target.
#[cfg(unix)]
pub(super) fn own_mode(path: &Path) -> std::io::Result<Option<u32>> {
    use std::os::unix::fs::PermissionsExt as _;
    let meta = std::fs::symlink_metadata(path)?;
    Ok((meta.is_dir() && !meta.file_type().is_symlink())
        .then(|| meta.permissions().mode() & 0o7777))
}

#[cfg(unix)]
fn set_own_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

/// An agent tree is private from its first byte: the root is made `0o700`
/// before anything is written into it, and opened for traversal only by
/// [`open_agent_root`] once its config is private. A root that already exists
/// can only have been made for this new id by a writer racing the creation; it
/// is closed the same way and kept by rollback.
pub(super) fn create_private_agent_root(root: &Path, config: &Path) -> std::io::Result<bool> {
    if let Some(parent) = root.parent()
        && !parent.as_os_str().is_empty()
    {
        // `<data>/agents` is the shared inventory every seat enumerates.
        std::fs::create_dir_all(parent)?;
    }
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(root) {
        Ok(()) => Ok(true),
        Err(err)
            if err.kind() == std::io::ErrorKind::AlreadyExists
                && std::fs::symlink_metadata(root)
                    .is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink()) =>
        {
            #[cfg(unix)]
            {
                for dir in [root, config] {
                    match own_mode(dir) {
                        Ok(Some(mode)) if mode & 0o077 != 0 => set_own_mode(dir, mode & 0o7700)?,
                        Ok(_) => {}
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => return Err(err),
                    }
                }
            }
            #[cfg(not(unix))]
            let _ = config;
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

/// The seat reaches its config and a legacy managed workdir through the agent
/// root. Open the root for traversal only after its config is private, with
/// the same bits a umask-created root had.
pub(super) fn open_agent_root(root: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    if let Some(mode) = own_mode(root)?
        && mode & 0o011 == 0
    {
        set_own_mode(root, (mode | 0o755) & 0o7777)?;
    }
    #[cfg(not(unix))]
    let _ = root;
    Ok(())
}

/// `create_dir_all` that records every level it made, so a failed creation
/// can remove those levels and no directory that existed before it.
pub(super) fn create_dir_all_recorded(
    path: &Path,
    made: &std::cell::RefCell<MadeDirs>,
) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut cursor = Some(path);
    while let Some(dir) = cursor.filter(|dir| !dir.as_os_str().is_empty()) {
        match std::fs::symlink_metadata(dir) {
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                missing.push(dir.to_path_buf());
                cursor = dir.parent();
            }
            Err(err) => return Err(err),
        }
    }
    for dir in missing.into_iter().rev() {
        match std::fs::create_dir(&dir) {
            Ok(()) => made.borrow_mut().dirs.push(dir),
            // Another writer made it first; it is not this attempt's.
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && dir.is_dir() => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

pub fn create(store: &Store, data: &Path, request: CreateRequest) -> Result<Agent, Error> {
    let data = std::path::absolute(data)?;
    let checkouts = crate::repo::Checkouts::configured(&data);
    create_with_seat(store, &data, request, &checkouts, None)
}

#[cfg(test)]
pub(super) fn create_with_consumer(
    store: &Store,
    data: &Path,
    request: CreateRequest,
    consumer: &crate::repo::Checkouts,
) -> Result<Agent, Error> {
    create_with_seat(store, data, request, consumer, None)
}

pub(crate) fn create_operator_root(
    store: &Store,
    data: &Path,
    request: CreateRequest,
    seat: crate::sandbox::RootSeatRequest,
) -> Result<Agent, Error> {
    let data = std::path::absolute(data)?;
    let checkouts = crate::repo::Checkouts::configured(&data);
    create_with_seat(store, &data, request, &checkouts, Some(seat))
}

fn create_with_seat(
    store: &Store,
    data: &Path,
    request: CreateRequest,
    consumer: &crate::repo::Checkouts,
    root_seat: Option<crate::sandbox::RootSeatRequest>,
) -> Result<Agent, Error> {
    create_with_seat_and_sandbox_mode(
        store,
        data,
        request,
        consumer,
        root_seat,
        crate::sandbox_providers::SandboxMode::from_env(),
    )
}

pub(super) fn create_with_seat_and_sandbox_mode(
    store: &Store,
    data: &Path,
    request: CreateRequest,
    consumer: &crate::repo::Checkouts,
    root_seat: Option<crate::sandbox::RootSeatRequest>,
    sandbox_mode: crate::sandbox_providers::SandboxMode,
) -> Result<Agent, Error> {
    let default_runtime =
        if sandbox_mode.is_remote_required() || crate::server::serverless::is_serverless_mode() {
            ExecutionRuntime::Sandbox { provider: None }
        } else {
            ExecutionRuntime::Local
        };
    if request.harness.trim().is_empty() {
        return Err(Error::Message(
            "no harness selected; set a harness in the request, role template, or deployment default"
                .into(),
        ));
    }
    let resolved = resolve_harness_and_runtime(
        &request.harness,
        request.runtime.as_deref(),
        default_runtime,
    )
    .map_err(|err| Error::Message(err.to_string()))?;
    if sandbox_mode.is_remote_required() && resolved.runtime == ExecutionRuntime::Local {
        return Err(Error::Message(
            "runtime `local` is refused when ANYAGENTS_SANDBOX_MODE=remote_required; use `sandbox` or `cloud`"
                .into(),
        ));
    }
    let normalized_harness = resolved.harness.clone();
    // A harness is installed only if an adapter can start it.
    let adapter_selector = resolved.legacy_harness_selector();
    install::adapter(&normalized_harness).or_else(|_| install::adapter(&adapter_selector))?;
    let mut new = NewAgent::new(request.name, normalized_harness.clone())
        .with_runtime(resolved.runtime.clone());
    if let Some(cap) = request.budget_cap_micros {
        new = new.with_budget_micros(cap);
    }
    new.role = Some(
        request
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_AGENT_ROLE)
            .to_string(),
    );
    new.model = request
        .model
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty());
    new.description = request
        .description
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let template_description = new.role.as_deref().and_then(|role| {
        let parent = request
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok());
        let ancestor_wds = ancestor_archetype_workdirs(Some(store), data, parent.as_ref()).ok()?;
        resolve_role_template_with_ancestors(role, request.workdir.as_deref(), &ancestor_wds, data)
            .ok()
            .flatten()
            .and_then(|(_, tpl, _)| tpl.description)
    });
    new.instructions = request
        .instructions
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            new.description
                .as_deref()
                .filter(|desc| Some(*desc) != template_description.as_deref())
                .map(str::to_string)
        });
    new.parent_id = request.parent_id;
    // Refused before the row exists: a creation rolled back later costs a
    // delete of everything keyed by the agent.
    ensure_harness_runs_profile(
        data,
        &normalized_harness,
        request.profile.as_deref(),
        new.model.as_deref(),
    )?;
    let agent = store.create_agent(new)?;
    let data = std::path::absolute(data)?;
    // A checkout the consumer produced for this creation; rollback removes it.
    let provisioned: std::cell::RefCell<Option<PathBuf>> = std::cell::RefCell::new(None);
    // Every directory this call made; rollback removes exactly these.
    let made: std::cell::RefCell<MadeDirs> = std::cell::RefCell::new(MadeDirs::default());
    let mut claimed_root_seat = false;
    let finish = (|| -> Result<Agent, Error> {
        if root_seat.is_none() && !sandbox_mode.is_remote_required() {
            crate::sandbox::bind_principal(store, &agent, None)?;
        }
        // Nothing is installed until an agent needs it: this is that moment. A
        // failure is the reason the agent's first turn cannot start, reported here
        // and never retried in a loop.
        let install_target = if crate::remote::is_agent_cloud(&agent, Some(&data), None) {
            crate::remote::agent_cloud_selector(&agent)
        } else {
            agent.harness.clone()
        };
        if !cfg!(test) && !sandbox_mode.is_remote_required() {
            install::record(&data, &install_target).map_err(Error::Message)?;
        }
        // The gateway origin and this agent's own token, kept so a restart starts
        // the same agent the same way.
        let mut pairs = crate::gateway::agent_env(&data, &agent.id);
        let parent_name = agent
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok())
            .map(|p| p.name);
        if let Some(ref pname) = parent_name {
            pairs.push(("ANYAGENTS_PARENT_NAME".to_string(), pname.clone()));
        }
        store.set_agent_launch_env(&agent.id, &pairs)?;
        let managed = agent_work_dir(&data, &agent.id);
        let config = agent_config_dir(&data, &agent.id);
        let root = config
            .parent()
            .ok_or_else(|| Error::Message("agent config has no root".into()))?
            .to_path_buf();
        let made_root = create_private_agent_root(&root, &config)?;
        made.borrow_mut().root = Some((root.clone(), made_root));
        create_private_config_dir(&config)?;
        let _ = std::fs::create_dir_all(config.join("tmp"));
        if let Some(prof) = request
            .profile
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let (prov, prof_name) = match prof.split_once('/') {
                Some((pv, pn)) => (pv.trim().to_string(), pn.trim().to_string()),
                None => (
                    upstream_provider_of(&agent.harness, agent.model.as_deref()),
                    prof.to_string(),
                ),
            };
            ensure_profile_compatible_and_mapped(
                &data,
                &prov,
                &prof_name,
                normalize_harness_name(&agent.harness),
                agent.model.as_deref(),
                Some(&config),
            )?;
            let normalized = format!("{prov}/{prof_name}");
            write_profile_marker(&config, ".anyagents-profile", &format!("{normalized}\n"))?;
        }
        let wd_str = request
            .workdir
            .as_ref()
            .and_then(|p| p.to_str())
            .map(str::trim)
            .unwrap_or("");
        let wd_lower = wd_str.to_ascii_lowercase();
        let wd_is_cloud_keyword = matches!(
            wd_lower.as_str(),
            "cloud" | "cloud-worktree" | "cloud-agent"
        );
        let wd_is_worktree_keyword = matches!(
            wd_lower.as_str(),
            "worktree" | "branch" | "isolated" | "cloud-worktree"
        );
        let wd_is_private_keyword = matches!(
            wd_lower.as_str(),
            "remote" | "private" | "sandbox" | "self" | "cloud" | "cloud-agent"
        );
        let wd_is_inherit_keyword = matches!(wd_lower.as_str(), "inherit" | "parent");
        let parent_has_git_workdir = agent
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok())
            .and_then(|p| p.workdir)
            .map(PathBuf::from)
            .is_some_and(|pwd| pwd.is_dir() && is_git_repo_dir(&pwd));
        let effective_repo = request
            .repo
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .or(if wd_is_worktree_keyword && parent_has_git_workdir {
                Some("inherit")
            } else {
                None
            });
        let explicit_workdir = if wd_str.is_empty()
            || wd_is_worktree_keyword
            || wd_is_private_keyword
            || wd_is_inherit_keyword
        {
            None
        } else {
            request.workdir.clone()
        };
        if let Some(path) = &request.workdir
            && wd_str.is_empty()
        {
            return Err(Error::WorkdirPermission {
                path: path.display().to_string(),
                message: "working directory must not be empty".into(),
            });
        }
        if let Some(path) = &explicit_workdir {
            crate::sandbox::validate_workdir_target(path)?;
        }
        let is_cloud_agent_mode = wd_is_cloud_keyword
            || pairs
                .iter()
                .any(|(k, v)| k == "SYLPHX_AGENT_ENV" && !v.trim().is_empty())
            || std::env::var("SYLPHX_AGENT_ENV")
                .ok()
                .is_some_and(|v| !v.trim().is_empty());
        if wd_is_cloud_keyword && !pairs.iter().any(|(k, _)| k == "SYLPHX_AGENT_ENV") {
            let env_slug = std::fs::read_to_string(config.join(".config/sylphx/context.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|v| {
                    v.get("environment")
                        .or_else(|| v.get("env"))
                        .and_then(|s| s.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                })
                .unwrap_or_else(|| "default".to_string());
            pairs.push(("SYLPHX_AGENT_ENV".to_string(), env_slug));
        }
        let effective_cloud = crate::remote::is_agent_cloud(&agent, Some(&data), None);
        if sandbox_mode.is_remote_required()
            && !effective_cloud
            && !crate::sandbox_exec::has_remote_sandbox_provider(Some(&data))
            && crate::sandbox_providers::SylphxProviderConfig::resolve(Some(&data))
                .map_err(Error::Message)?
                .is_none()
            && crate::sandbox_providers::resolve_sandbox_provider_config(Some(&data)).is_none()
        {
            return Err(Error::Message(
                "ANYAGENTS_SANDBOX_MODE=remote_required requires a remote sandbox provider (`ANYAGENTS_SANDBOX_PROVIDER` or `ANYAGENTS_SYLPHX_PARENT`)"
                    .into(),
            ));
        }
        let workdir_created =
            explicit_workdir.as_ref().is_some_and(|p| !p.exists()) || effective_repo.is_some();
        let managed_abs = std::path::absolute(&managed)?;
        // Cloud harnesses run only a small HTTP launcher here, never a local checkout.
        // Explicit filesystem paths and explicit inheritance keep their local semantics.
        let remote_placement = (agent.runtime.is_cloud()
            || crate::vendor::is_cloud_harness(&agent.harness)
            || sandbox_mode.is_remote_required())
            && explicit_workdir.is_none()
            && !wd_is_inherit_keyword;
        // Record the allocation before any checkout or cloud provisioning can
        // block. This does not admit the agent to the runtime.
        if !remote_placement
            && (effective_repo.is_some()
                || wd_is_private_keyword
                || wd_is_worktree_keyword
                || (explicit_workdir.is_none()
                    && !wd_is_inherit_keyword
                    && agent.parent_id.is_some()))
        {
            let target = match &explicit_workdir {
                Some(path) => std::path::absolute(path)?,
                None => managed_abs.clone(),
            };
            store.set_agent_workdir_if_missing(&agent.id, &target.display().to_string())?;
        }
        let cwd = if remote_placement {
            provision_cloud_sandbox_worktree_marker(
                store,
                &agent,
                &config,
                effective_repo.unwrap_or(""),
                request.branch.as_deref(),
                request.base.as_deref(),
            )?;
            if sandbox_mode.is_remote_required() {
                PathBuf::from(crate::sandbox_providers::sylphx::DEFAULT_SANDBOX_WORKDIR)
            } else {
                let launcher = root.join("launcher");
                create_dir_all_recorded(&launcher, &made)?;
                launcher
            }
        } else if let Some(repo_raw) = effective_repo {
            if (sandbox_mode.is_remote_required())
                || (crate::vendor::is_spiron_harness(&agent.harness)
                    && is_cloud_agent_mode
                    && explicit_workdir.is_none())
            {
                provision_cloud_sandbox_worktree_marker(
                    store,
                    &agent,
                    &config,
                    repo_raw,
                    request.branch.as_deref(),
                    request.base.as_deref(),
                )?;
                managed_abs.clone()
            } else {
                let target_wt = match &explicit_workdir {
                    Some(path) => std::path::absolute(path)?,
                    None => managed_abs.clone(),
                };
                provision_managed_worktree(
                    consumer,
                    store,
                    &agent,
                    &config,
                    repo_raw,
                    request.branch.as_deref(),
                    request.base.as_deref(),
                    &target_wt,
                    parent_name.as_deref(),
                    &provisioned,
                    &made,
                )?;
                target_wt
            }
        } else if wd_is_private_keyword || wd_is_worktree_keyword {
            if sandbox_mode.is_remote_required()
                || (crate::vendor::is_spiron_harness(&agent.harness) && is_cloud_agent_mode)
            {
                provision_cloud_sandbox_worktree_marker(
                    store,
                    &agent,
                    &config,
                    "",
                    request.branch.as_deref(),
                    request.base.as_deref(),
                )?;
            }
            managed_abs.clone()
        } else if wd_is_inherit_keyword {
            let inherited_parent_wd = agent
                .parent_id
                .as_ref()
                .and_then(|pid| store.agent(pid).ok())
                .and_then(|p| p.workdir)
                .map(PathBuf::from)
                .filter(|p| p.is_dir());
            let parent_wd = inherited_parent_wd.ok_or_else(|| Error::WorkdirPermission {
                path: wd_str.into(),
                message: "parent has no accessible workdir to inherit".into(),
            })?;
            std::path::absolute(&parent_wd)?
        } else {
            match &explicit_workdir {
                Some(path) => {
                    create_dir_all_recorded(path, &made).map_err(|error| {
                        Error::WorkdirPermission {
                            path: path.display().to_string(),
                            message: error.to_string(),
                        }
                    })?;
                    let abs = std::path::absolute(path)?;
                    if agent.parent_id.is_some()
                        && abs.join(".git").is_file()
                        && !config.join(".anyagents-managed-worktree.json").exists()
                    {
                        record_managed_worktree_marker(
                            &config,
                            &abs,
                            "explicit-worktree",
                            request.branch.as_deref(),
                        )?;
                    }
                    abs
                }
                None => {
                    if agent.parent_id.is_some() {
                        if sandbox_mode.is_remote_required()
                            || (crate::vendor::is_spiron_harness(&agent.harness)
                                && is_cloud_agent_mode)
                        {
                            provision_cloud_sandbox_worktree_marker(
                                store,
                                &agent,
                                &config,
                                "",
                                request.branch.as_deref(),
                                request.base.as_deref(),
                            )?;
                        }
                        managed_abs.clone()
                    } else {
                        match &request.workdir_default {
                            Some(shared) => {
                                create_dir_all_recorded(shared, &made)?;
                                std::path::absolute(shared)?
                            }
                            None => managed_abs.clone(),
                        }
                    }
                }
            }
        };
        store.set_agent_workdir_if_missing(&agent.id, &cwd.display().to_string())?;
        if cwd == managed_abs && !managed_abs.exists() {
            create_dir_all_recorded(&managed_abs, &made)?;
        }
        // Reserve a named seat after workdir validation and before opening the tree to it.
        if !sandbox_mode.is_remote_required() && root_seat.is_some() {
            claimed_root_seat = crate::sandbox::bind_principal(store, &agent, root_seat.as_ref())?;
        }
        // The config is private; the seat may now traverse to its own tree.
        open_agent_root(&root)?;
        let (uid, gid, launch) = if sandbox_mode.is_remote_required() {
            (
                crate::sandbox::server_uid(),
                crate::sandbox::server_gid(),
                None,
            )
        } else {
            match resolved_agent_seat_info(store, &agent) {
                Some((_, uid, gid, launch)) => (uid, gid, launch),
                None if claimed_root_seat => {
                    return Err(Error::PermissionDenied(
                        "newly admitted seat could not be resolved for execution".into(),
                    ));
                }
                None => (
                    crate::sandbox::server_uid(),
                    crate::sandbox::server_gid(),
                    None,
                ),
            }
        };
        if !sandbox_mode.is_remote_required() {
            if remote_placement || workdir_created || cwd == managed_abs {
                crate::sandbox::share_worktree_with_seat(&cwd, gid)?;
            }
            crate::repo::verify(&cwd, uid, gid, launch.as_deref())?;
        }
        crate::operator::mint(store, &agent.id)?;
        store.set_agent_launch_env(&agent.id, &pairs)?;
        if !remote_placement
            && !effective_cloud
            && crate::sandbox_providers::sandbox_config_from_config_dir(&config).is_none()
            && !crate::sandbox_exec::has_remote_sandbox_provider(Some(&data))
            && crate::sandbox_providers::SylphxProviderConfig::resolve(Some(&data))
                .map_err(Error::Message)?
                .is_none()
            && let Some(provider) =
                crate::sandbox_providers::resolve_sandbox_provider_config(Some(&data))
            && let Ok(sbx) = agent_tools::SandboxProvider::provision(
                &provider,
                agent.id.as_str(),
                &agent.name,
                effective_repo,
            )
        {
            let _ = sbx.write_to_config_dir(&config);
        }
        let mut pre_running = agent.clone();
        pre_running.workdir = Some(cwd.display().to_string());
        reconcile_agent_runtime_config(store, &data, &pre_running, &config, &cwd)?;
        super::persist_spawned_agent_config_to_store(store, &agent.id, &config);
        let running = store.set_agent_running(&agent.id, &cwd.display().to_string())?;
        Ok(running)
    })();
    if let Err(error) = finish {
        return Err(roll_back_failed_creation(
            store,
            &agent,
            provisioned.into_inner().as_deref(),
            &made.into_inner(),
            claimed_root_seat,
            error,
        ));
    }
    finish
}

/// Undo a creation that failed after its name was reserved. The same name must
/// always be retryable. The row, the checkout this attempt made and the
/// directories it made are removed; a row that cannot be deleted releases its
/// name instead. Whatever stays behind is closed to seats first, so a kept
/// tree never fails another seat's peer-boundary check.
fn roll_back_failed_creation(
    store: &Store,
    agent: &Agent,
    provisioned: Option<&Path>,
    made: &MadeDirs,
    claimed_root_seat: bool,
    error: Error,
) -> Error {
    #[cfg(unix)]
    if let Some((root, _)) = &made.root
        && let Ok(Some(mode)) = own_mode(root)
        && mode & 0o077 != 0
        && let Err(close) = set_own_mode(root, mode & 0o7700)
    {
        eprintln!(
            "warning: cannot close partial directory {} of failed creation {}: {close}",
            root.display(),
            agent.id
        );
    }
    let mut kept_checkout = None;
    if let Some(checkout) = provisioned
        && let Err(cleanup) = crate::repo::discard(checkout)
    {
        eprintln!(
            "warning: cannot remove checkout {} of failed creation {}: {cleanup}",
            checkout.display(),
            agent.id
        );
        kept_checkout = Some(checkout);
    }
    if let Err(delete) = store.delete_agent(&agent.id) {
        eprintln!(
            "warning: cannot delete failed creation of {}: {delete}",
            agent.name
        );
        if let Err(release) = store.release_unadmitted_agent(&agent.id) {
            eprintln!(
                "warning: failed creation of {} kept its name: {release}",
                agent.name
            );
        }
        return error;
    }
    remove_made_dirs(made, kept_checkout, &agent.id);
    if claimed_root_seat && let Err(cleanup) = crate::sandbox::rollback_root_seat(store, &agent.id)
    {
        eprintln!(
            "warning: could not roll back the seat grant for {}: {cleanup}",
            agent.id
        );
    }
    error
}

/// Remove, newest first, exactly the directories one failed creation made.
/// Work directories go only while empty, so no content this attempt did not
/// write is ever removed. The agent root this attempt made holds only what it
/// wrote, unless a checkout it could not remove lives inside; then it stays.
pub(super) fn remove_made_dirs(made: &MadeDirs, kept_checkout: Option<&Path>, id: &AgentId) {
    for dir in made.dirs.iter().rev() {
        match std::fs::remove_dir(dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => eprintln!(
                "warning: keeping directory {} of failed creation {id}: {err}",
                dir.display()
            ),
        }
    }
    let Some((root, true)) = &made.root else {
        return;
    };
    if kept_checkout.is_some_and(|checkout| checkout.starts_with(root)) {
        eprintln!(
            "warning: keeping directory {} of failed creation {id}: it holds a checkout",
            root.display()
        );
        return;
    }
    if let Err(remove) = std::fs::remove_dir_all(root)
        && remove.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!("warning: cannot remove partial directory of failed creation {id}: {remove}");
    }
}

#[allow(clippy::too_many_arguments)]
fn provision_managed_worktree(
    consumer: &crate::repo::Checkouts,
    store: &Store,
    agent: &Agent,
    config: &Path,
    repo_raw: &str,
    branch_opt: Option<&str>,
    base_opt: Option<&str>,
    target_wt: &Path,
    _parent_name: Option<&str>,
    provisioned: &std::cell::RefCell<Option<PathBuf>>,
    made: &std::cell::RefCell<MadeDirs>,
) -> Result<(), Error> {
    if target_wt.exists() {
        let is_empty = std::fs::read_dir(target_wt)
            .ok()
            .is_some_and(|mut d| d.next().is_none());
        if is_empty {
            let _ = std::fs::remove_dir(target_wt);
        } else {
            return Err(Error::Message(format!(
                "target workdir `{}` already exists and is not empty",
                target_wt.display()
            )));
        }
    }
    if let Some(parent_dir) = target_wt.parent() {
        create_dir_all_recorded(parent_dir, made)?;
    }
    let branch = branch_opt
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("agent/{}", agent.name));
    let base = base_opt.map(str::trim).filter(|s| !s.is_empty());

    if matches!(
        repo_raw.to_ascii_lowercase().as_str(),
        "inherit" | "parent" | "worktree" | "branch" | "isolated"
    ) {
        let parent_wd = agent
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok())
            .and_then(|p| p.workdir)
            .map(PathBuf::from)
            .ok_or_else(|| {
                Error::Message(
                    "repo=\"inherit\" requires a parent agent with an active git workdir".into(),
                )
            })?;
        consumer.local(&parent_wd, target_wt, &branch, base)?;
    } else if Path::new(repo_raw).is_dir() && is_git_repo_dir(Path::new(repo_raw)) {
        consumer.local(Path::new(repo_raw), target_wt, &branch, base)?;
    } else {
        consumer.remote(repo_raw, target_wt, &branch, base)?;
    }
    // The target was absent or empty above: this checkout belongs to us.
    *provisioned.borrow_mut() = Some(target_wt.to_path_buf());

    record_managed_worktree_marker(config, target_wt, repo_raw, Some(&branch))?;
    Ok(())
}
