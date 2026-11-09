//! `agents.create`. The agent row, its directories, and its MCP registration
//! are one step. The harness config is written only under the agent's own
//! config directory.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, AgentId, Error, NewAgent, Store, agent_config_dir, agent_work_dir};
use anyagents_harness::normalize_harness_name;

use crate::{install, mcp_config, muse_config, opencode_config};

pub struct CreateRequest {
    pub name: String,
    pub role: Option<String>,
    pub harness: String,
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
/// DirBuilder leaves an existing root's admitted group/mode unchanged.
fn create_private_config_dir(config: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(config)
}

pub fn create(store: &Store, data: &Path, request: CreateRequest) -> Result<Agent, Error> {
    create_with_consumer(
        store,
        data,
        request,
        &crate::worktree::Consumer::configured(),
    )
}

fn create_with_consumer(
    store: &Store,
    data: &Path,
    request: CreateRequest,
    consumer: &crate::worktree::Consumer,
) -> Result<Agent, Error> {
    create_with_seat(store, data, request, consumer, None)
}

type WorkdirSeat = (String, u32, u32, Option<String>);

fn create_with_seat(
    store: &Store,
    data: &Path,
    request: CreateRequest,
    consumer: &crate::worktree::Consumer,
    seat_override: Option<WorkdirSeat>,
) -> Result<Agent, Error> {
    let normalized_harness = normalize_harness_name(&request.harness).to_string();
    // A harness is installed only if an adapter can start it.
    install::adapter(&normalized_harness)?;
    let mut new = NewAgent::new(request.name, normalized_harness.clone());
    new.role = request.role;
    new.model = request
        .model
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .or_else(|| {
            (normalized_harness == "spiron")
                .then(|| crate::spiron::DEFAULT_SPIRON_MODEL.to_string())
        });
    new.description = request
        .description
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    new.instructions = request
        .instructions
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| new.description.clone());
    new.parent_id = request.parent_id;
    let agent = store.create_agent(new)?;
    let data = std::path::absolute(data)?;
    let finish = (|| -> Result<Agent, Error> {
        crate::sandbox::bind_principal(store, &agent)?;
        // Nothing is installed until an agent needs it: this is that moment. A
        // failure is the reason the agent's first turn cannot start, reported here
        // and never retried in a loop.
        if !cfg!(test) {
            install::record(&data, &agent.harness).map_err(Error::Message)?;
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
        std::fs::create_dir_all(&managed)?;
        let config = agent_config_dir(&data, &agent.id);
        create_private_config_dir(&config)?;
        let _ = std::fs::create_dir_all(config.join("tmp"));
        if let Some(prof) = request
            .profile
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let normalized = if prof.contains('/') {
                prof.to_string()
            } else {
                format!(
                    "{}/{prof}",
                    upstream_provider_of(&agent.harness, agent.model.as_deref())
                )
            };
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
            "private" | "sandbox" | "self" | "cloud" | "cloud-agent"
        );
        let wd_is_inherit_keyword = matches!(wd_lower.as_str(), "inherit" | "parent");
        let effective_repo = request
            .repo
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .or(if wd_is_worktree_keyword {
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
        let is_cloud_agent_mode = wd_is_cloud_keyword
            || pairs
                .iter()
                .any(|(k, v)| k == "SYLPHX_AGENT_ENV" && !v.trim().is_empty())
            || std::env::var("SYLPHX_AGENT_ENV")
                .ok()
                .is_some_and(|v| !v.trim().is_empty());
        if wd_is_cloud_keyword && !pairs.iter().any(|(k, _)| k == "SYLPHX_AGENT_ENV") {
            pairs.push(("SYLPHX_AGENT_ENV".to_string(), "default".to_string()));
        }
        let workdir_created =
            explicit_workdir.as_ref().is_some_and(|p| !p.exists()) || effective_repo.is_some();
        let managed_abs = std::path::absolute(&managed)?;
        let cwd = if let Some(repo_raw) = effective_repo {
            if crate::vendor::is_spiron_harness(&agent.harness)
                && is_cloud_agent_mode
                && explicit_workdir.is_none()
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
                )?;
                target_wt
            }
        } else if wd_is_private_keyword {
            if crate::vendor::is_spiron_harness(&agent.harness) && is_cloud_agent_mode {
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
                    std::fs::create_dir_all(path).map_err(|error| Error::WorkdirPermission {
                        path: path.display().to_string(),
                        message: error.to_string(),
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
                        if crate::vendor::is_spiron_harness(&agent.harness) && is_cloud_agent_mode {
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
                                std::fs::create_dir_all(shared)?;
                                std::path::absolute(shared)?
                            }
                            None => managed_abs.clone(),
                        }
                    }
                }
            }
        };
        if let Some((_, uid, gid, launch)) = seat_override
            .clone()
            .or_else(|| resolved_agent_seat_info(store, &agent))
        {
            if workdir_created || cwd == managed_abs {
                crate::sandbox::share_worktree_with_seat(
                    &cwd,
                    uid,
                    gid,
                    &crate::worktree_access::grant_path(&data, &agent.id),
                )?;
            }
            crate::worktree_access::verify(&cwd, uid, gid, launch.as_deref())?;
        } else {
            if workdir_created || cwd == managed_abs {
                crate::sandbox::share_worktree_with_seat(
                    &cwd,
                    crate::sandbox::server_uid(),
                    crate::sandbox::server_gid(),
                    &crate::worktree_access::grant_path(&data, &agent.id),
                )?;
            }
            crate::worktree_access::verify(
                &cwd,
                crate::sandbox::server_uid(),
                crate::sandbox::server_gid(),
                None,
            )?;
        }
        crate::operator::mint(store, &agent.id)?;
        store.set_agent_launch_env(&agent.id, &pairs)?;
        let mut pre_running = agent.clone();
        pre_running.workdir = Some(cwd.display().to_string());
        reconcile_agent_runtime_config(store, &data, &pre_running, &config, &cwd)?;
        let running = store.set_agent_running(&agent.id, &cwd.display().to_string())?;
        Ok(running)
    })();
    if finish.is_err() {
        // Keep the row and recovery journal if revocation fails; otherwise no
        // later maintenance could find and remove a partial admission's grant.
        if let Err(error) = crate::worktree_access::revoke_recorded(&data, &agent.id) {
            let _ = store.set_agent_state(&agent.id, "archived");
            return Err(error);
        }
        let _ = store.delete_agent(&agent.id);
        let _ = std::fs::remove_dir_all(data.join("agents").join(agent.id.as_str()));
    }
    finish
}

#[allow(clippy::too_many_arguments)]
fn provision_managed_worktree(
    consumer: &crate::worktree::Consumer,
    store: &Store,
    agent: &Agent,
    config: &Path,
    repo_raw: &str,
    branch_opt: Option<&str>,
    base_opt: Option<&str>,
    target_wt: &Path,
    _parent_name: Option<&str>,
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
        std::fs::create_dir_all(parent_dir)?;
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

    // Identity belongs to this checkout, not A's common config. Replacing the
    // common file as the server would drop its owner and existing named ACLs.
    crate::worktree_access::set_worktree_identity(target_wt)?;

    record_managed_worktree_marker(config, target_wt, repo_raw, Some(&branch))?;
    Ok(())
}

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

pub(crate) fn sync_all_agent_instructions(store: &Store, data: &Path) -> Result<(), Error> {
    let agents = store.list_agents()?;
    for agent in &agents {
        if agent.state == "archived" {
            continue;
        }
        let cfg = agent_config_dir(data, &agent.id);
        if !cfg.is_dir() {
            continue;
        }
        sync_agent_description_files(store, agent, &cfg)?;
    }
    Ok(())
}

pub(crate) fn write_agent_instructions(
    config: &Path,
    agent: &Agent,
    parent_name: Option<&str>,
) -> Result<(), Error> {
    let cached_bridge = std::fs::read_to_string(config.join(".anyagents-session-bridge.json"))
        .ok()
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .and_then(|v| {
            let h = v.get("harness").and_then(|x| x.as_str())?;
            if h != agent.harness {
                return None;
            }
            v.get("summary")
                .and_then(|x| x.as_str())
                .map(str::to_string)
        });
    write_agent_instructions_with_continuity(config, agent, parent_name, cached_bridge.as_deref())
}

fn lock_instruction_mirrors(config: &Path) -> std::io::Result<std::fs::File> {
    let root = config
        .parent()
        .ok_or_else(|| std::io::Error::other("agent root missing"))?;
    let sessions = root.join("sessions");
    create_private_config_dir(&sessions)?;
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(sessions.join("instructions.lock"))?;
    file.lock()?;
    Ok(file)
}

pub(crate) fn write_agent_instructions_with_continuity(
    config: &Path,
    agent: &Agent,
    parent_name: Option<&str>,
    continuity: Option<&str>,
) -> Result<(), Error> {
    // Launch preparation and description/config updates can render concurrently,
    // including from different server processes. Hold a host-owned lock through
    // all mirrors and their readbacks so writers cannot replace each other's
    // verified output or leave mirrors from different snapshots.
    let _instructions_lock = lock_instruction_mirrors(config)?;
    let _ = std::fs::remove_file(config.join(".anyagents-harness-continuity.md"));
    let mut doc = format!(
        "# AnyAgents Identity & Environment Awareness\n\n\
         ## 1. Your Identity (Who You Are)\n\n\
         - **Agent Name**: `{name}` (addressable as `agent:{name}`)\n\
         - **Agent ID**: `{id}`\n",
        name = agent.name,
        id = agent.id,
    );
    if let Some(role) = agent
        .role
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        doc.push_str(&format!("- **Role Label**: `{role}`\n"));
    }
    if let Some(pub_desc) = agent.public_description() {
        doc.push_str(&format!(
            "- **Public Description (`description` — visible to all peers & group rosters)**: {pub_desc}\n"
        ));
    }
    if let Some(parent) = parent_name {
        doc.push_str(&format!(
            "- **Relationship**: Sub-agent of `agent:{parent}` (addressable as `\"parent\"` or `\"agent:{parent}\"`).\n"
        ));
    } else {
        doc.push_str("- **Role**: Root Agent\n");
    }

    doc.push_str("- **Lifecycle**: Role labels do not set lifecycle policy; follow your instructions. Any agent may call `agents` with `action: \"archive\"`. Idle processes release after mailbox/child/background work drains; identity and session persist.\n");

    if let Some(custom) = agent
        .instructions
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        doc.push_str("\n## 2. Role, Responsibilities & Private Instructions (`instructions`)\n\n");
        doc.push_str(custom);
        doc.push('\n');
    }

    doc.push_str(
        "\n## 3. Hierarchy, Topology & Communication Rules\n\
         \n\
         - `description` is a public routing card; `instructions` are private to you and ancestors. Supervision is downward-only.\n\
         - Live rosters arrive via `<topology event=\"initial|delta\" ... />`; inline `<message ...>` routing carries `sender_rel`, `sender_role`, `reply_to`, `answer_to`.\n\
         - `scope`: \"isolated\" = parent/children/bound channels; \"private\" (sub-agent default) adds shared groups; \"siblings\" adds siblings; \"public\" = workspace-wide. In group chats only the matching domain agent replies; otherwise `NO_REPLY`.\n\
         - Stdout never reaches peers/parents/channels. Always call `send` to reply/report (`to`, `reply_to: \"anyagents:reply:msg_...\"`, `text`, etc.); use the envelope's `answer_to`.\n\
         - Never poll an inbox: messages, sub-agent completions and watches wake you reactively.\n\
         - Vendor sub-agent/timer tools are disabled by default. Use `code` (`anyagents.*`, `workspace.*`, `web.*`, `computer.*`) or `agents` for lifecycle: isolated `<data>/agents/<id>/work` by default, or `repo` worktrees; `archive` reaps clean/pushed worktrees, preserves dirty/unpushed ones. Use `memory` for durable facts, `conversation` for history/search, `groups` for groups, `schedule` for wakes/event watches.\n\
         - `browser` provides persistent Chromium; fill secrets with `secret_ref: \"op://...\"` / `\"op-otp://...\"` without reading them. On `captcha_detected: true`, use `action: \"takeover\"` and send the takeover URL to the user.\n",
    );
    if let Some(parent) = parent_name {
        doc.push_str(&format!(
            "- Direct parent: `agent:{parent}`; report via `send` to `parent`.\n"
        ));
    }

    // A configured harness switch rotates before rendering. Ordinary updates
    // consume the persisted pin without opening live learned-memory files.
    if let Some(root) = config.parent() {
        if let Some((_, policy)) = agent
            .launch_env
            .iter()
            .find(|(key, _)| key == "ANYAGENTS_MEMORY_GUIDANCE")
            && !policy.trim().is_empty()
        {
            doc.push_str("\n## Memory policy\n\n");
            doc.push_str(policy);
            doc.push('\n');
        }
        match anyagents_core::host_session::prepare_harness_with_source(
            root,
            &agent.harness,
            harness_session_filename(&agent.harness),
            crate::vendor::memory_source(agent),
        ) {
            Ok(Some(session)) => {
                doc.push('\n');
                doc.push_str(&session.memory.section);
                doc.push('\n');
            }
            Ok(None) => (), // First dispatch pins and rerenders before launch.
            Err(error) => {
                return Err(Error::Message(format!(
                    "invalid host memory pin for {}: {error}",
                    agent.id
                )));
            }
        }
    }

    // Spiron assembles the shared layer independently. Other harnesses load
    // their global AGENTS.md / CLAUDE.md / GEMINI.md instead, so those files
    // must retain the shared layer even though SYSTEM_PROMPT.md stays compact.
    let mut native_doc = doc.clone();
    if let Some(shared) = read_shared_agents_md(config) {
        native_doc.push_str("\n## 4. Shared Instructions (AGENTS.md)\n\n");
        native_doc.push_str(&shared);
        native_doc.push('\n');
    }
    if let Some(cont) = continuity.map(str::trim).filter(|s| !s.is_empty()) {
        // Reserve each file's own shared-layer budget before adding continuity.
        // Never truncate company instructions to make room for history.
        const MAX_DOC_BYTES: usize = 23_200;
        for rendered in [&mut doc, &mut native_doc] {
            let remaining = MAX_DOC_BYTES.saturating_sub(rendered.len() + 2);
            if remaining >= 256 {
                rendered.push('\n');
                if cont.len() <= remaining {
                    rendered.push_str(cont);
                } else {
                    let mut cut = remaining;
                    while cut > 0 && !cont.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    if let Some(nl) = cont[..cut].rfind('\n')
                        && nl > 0
                    {
                        cut = nl;
                    }
                    rendered.push_str(&cont[..cut]);
                }
                rendered.push('\n');
            }
        }
    }

    let sys_prompt_path = config.join("SYSTEM_PROMPT.md");
    let prompt_changed = std::fs::read(&sys_prompt_path).ok().as_deref() != Some(doc.as_bytes())
        || std::fs::read(config.join("AGENTS.md")).ok().as_deref() != Some(native_doc.as_bytes());
    if prompt_changed && config.join("sylphx-cloud-session.json").is_file() {
        let _ = write_seat_shared_file(
            &config.join(".sylphx-cloud-instructions-fingerprint"),
            b"stale\n",
            0o664,
        );
    }

    let paths = [
        (sys_prompt_path, &doc),
        (config.join("AGENTS.md"), &native_doc),
        (config.join("opencode/AGENTS.md"), &native_doc),
        (config.join(".config/opencode/AGENTS.md"), &native_doc),
        (config.join("CLAUDE.md"), &native_doc),
        (config.join(".claude/CLAUDE.md"), &native_doc),
        (config.join(".gemini/config/GEMINI.md"), &native_doc),
        (config.join("muse/AGENTS.md"), &native_doc),
        (config.join(".config/muse/AGENTS.md"), &native_doc),
        (config.join(".omp/agent/APPEND_SYSTEM.md"), &native_doc),
        (config.join(".omp/agent/AGENTS.md"), &native_doc),
        (config.join(".dsh/AGENTS.md"), &native_doc),
        (config.join(".grok/AGENTS.md"), &native_doc),
    ];
    for (path, contents) in &paths {
        if std::fs::read(path).ok().as_deref() == Some(contents.as_bytes()) {
            continue;
        }
        write_required_file(path, contents.as_bytes(), 0o664)?;
    }
    // agy identifies GEMINI.md as "user_global", but rules by file path: even
    // symlinks inject the prompt twice. Keep only the canonical global rule and
    // remove both legacy aliases on every refresh of existing configurations.
    let gemini_cfg = config.join(".gemini").join("config");
    let _ = std::fs::remove_file(gemini_cfg.join("AGENTS.md"));
    let _ = std::fs::remove_file(gemini_cfg.join("rules").join("AGENTS.md"));
    Ok(())
}

/// Fast, non-intrusive instruction sync used by `describe()`: updates the
/// agent's system prompt files (`AGENTS.md`, `CLAUDE.md`, `SYSTEM_PROMPT.md`,
/// `config.toml` developer instructions) without walking or re-chowning the
/// `config/` or `work/` directory trees, and guarantees `config`'s existing
/// group and execute bits remain intact.
pub(crate) fn sync_agent_description_files(
    store: &Store,
    agent: &Agent,
    config: &Path,
) -> Result<(), Error> {
    #[cfg(unix)]
    let pre_existing_mode = {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(config)
            .ok()
            .map(|m| m.permissions().mode() & 0o7777)
    };
    create_private_config_dir(config)?;
    #[cfg(unix)]
    if let Some(base_mode) = pre_existing_mode {
        use std::os::unix::fs::PermissionsExt as _;
        let group_exec = if (base_mode & 0o0060) != 0 { 0o0010 } else { 0 };
        let other_exec = if (base_mode & 0o0006) != 0 { 0o0001 } else { 0 };
        let safe_mode = (base_mode | 0o0700 | group_exec | other_exec) & 0o7777;
        if safe_mode != base_mode {
            std::fs::set_permissions(config, std::fs::Permissions::from_mode(safe_mode))?;
        }
    }
    let parent_name = agent
        .parent_id
        .as_ref()
        .and_then(|pid| store.agent(pid).ok())
        .map(|p| p.name)
        .or_else(|| {
            agent
                .launch_env
                .iter()
                .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
                .map(|(_, v)| v.clone())
        });
    let continuity = compute_live_session_continuity(store, agent, config);
    write_agent_instructions_with_continuity(
        config,
        agent,
        parent_name.as_deref(),
        continuity.as_deref(),
    )?;
    if agent.harness == "codex" {
        let codex_toml = config.join("config.toml");
        if codex_toml.is_file() {
            mcp_config::sync_codex_developer_instructions(&codex_toml, continuity.as_deref())?;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let base_mode = pre_existing_mode.unwrap_or(0o0700);
        let group_exec = if (base_mode & 0o0060) != 0 { 0o0010 } else { 0 };
        let other_exec = if (base_mode & 0o0006) != 0 { 0o0001 } else { 0 };
        let safe_mode = (base_mode | 0o0700 | group_exec | other_exec) & 0o7777;
        if let Ok(after_meta) = std::fs::metadata(config)
            && (after_meta.permissions().mode() & 0o7777) != safe_mode
        {
            std::fs::set_permissions(config, std::fs::Permissions::from_mode(safe_mode))?;
        }
    }
    Ok(())
}

pub(crate) fn extract_file_tilde_refs(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(idx) = rest.find("{file:~/") {
        let after = &rest[idx + "{file:~/".len()..];
        if let Some(end) = after.find('}') {
            let rel = after[..end].trim();
            if !rel.is_empty() && !rel.contains("..") && !out.iter().any(|existing| existing == rel)
            {
                out.push(rel.to_string());
            }
            rest = &after[end + 1..];
        } else {
            break;
        }
    }
    out
}

pub(crate) fn ensure_seat_environment(target_config: &Path, workdir: &Path) -> Result<(), Error> {
    for subdir in ["tmp", "data-home", "cache-home", "state-home"] {
        let dir = target_config.join(subdir);
        let _ = std::fs::create_dir_all(&dir);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777));
        }
    }
    crate::worktree_access::configure_seat_git(target_config, workdir)?;
    #[cfg(unix)]
    {
        let shared: Vec<String> = std::env::var("ANYAGENTS_SHARED_HOME_DIRS")
            .ok()
            .map(|raw| {
                serde_json::from_str(&raw)
                    .map_err(|err| Error::Message(format!("ANYAGENTS_SHARED_HOME_DIRS: {err}")))
            })
            .transpose()?
            .unwrap_or_default();
        let host_home = crate::sandbox::host_home_dir();
        for shared_dir in shared {
            let rel = Path::new(&shared_dir);
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err(Error::Message(
                    "shared home paths must be relative and cannot escape the home".into(),
                ));
            }
            let home = host_home.as_ref().ok_or_else(|| {
                Error::Message("configured shared home paths require an available host home".into())
            })?;
            let src = home.join(rel);
            let dst = target_config.join(rel);
            if !dst.exists() {
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::os::unix::fs::symlink(&src, &dst)?;
            }
        }
        for (rel, key) in [
            (".rustup", "ANYAGENTS_RUSTUP_HOME"),
            (".cargo", "ANYAGENTS_CARGO_HOME"),
        ] {
            if let Some(src) = std::env::var_os(key)
                && !target_config.join(rel).exists()
            {
                std::os::unix::fs::symlink(PathBuf::from(src), target_config.join(rel))?;
            }
        }
        if let Some(host_home) = host_home {
            let host_local_opt = host_home.join(".local/opt");
            let dst_local_opt = target_config.join(".local/opt");
            if host_local_opt.exists() && !dst_local_opt.exists() {
                if let Some(parent) = dst_local_opt.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::os::unix::fs::symlink(&host_local_opt, &dst_local_opt);
            }
            ensure_seat_skills(target_config, &host_home);
        }
    }
    Ok(())
}

pub(crate) fn data_dir_from_config(config: &Path) -> Option<PathBuf> {
    let parent1 = config.parent()?;
    let parent2 = parent1.parent()?;
    if parent2.file_name().and_then(|s| s.to_str()) == Some("agents") {
        parent2.parent().map(Path::to_path_buf)
    } else {
        Some(parent1.to_path_buf())
    }
}

pub(crate) fn read_shared_agents_md(config: &Path) -> Option<String> {
    let data_dir = data_dir_from_config(config)?;
    std::fs::read_to_string(data_dir.join("AGENTS.md"))
        .ok()
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

#[cfg(unix)]
pub(crate) fn sync_canonical_skills(data_dir: &Path) {
    for rel in ["skills", "AGENTS.md"] {
        let target_path = data_dir.join(rel);
        if let Ok(resolved) = std::fs::canonicalize(&target_path)
            && let Some(repo_root) = resolved.parent()
            && repo_root.join(".git").exists()
        {
            let is_clean = std::process::Command::new("git")
                .args([
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "core.hooksPath=/dev/null",
                ])
                .arg("-C")
                .arg(repo_root)
                .args(["status", "--porcelain"])
                .output()
                .ok()
                .is_some_and(|o| o.status.success() && o.stdout.is_empty());
            if is_clean {
                let _ = std::process::Command::new("git")
                    .args([
                        "-c",
                        "core.fsmonitor=false",
                        "-c",
                        "core.hooksPath=/dev/null",
                    ])
                    .arg("-C")
                    .arg(repo_root)
                    .args(["pull", "--ff-only", "--quiet", "origin", "main"])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        }
    }
}

#[cfg(unix)]
pub(crate) fn ensure_seat_skills(target_config: &Path, _host_home: &Path) {
    // An absent shared library must not disable the agent's own overlay.
    let Some(data) = data_dir_from_config(target_config) else {
        return;
    };
    let skills_root = data.join("skills");
    if let Some(root) = target_config.parent()
        && let Err(error) = anyagents_core::host_skills::mount_with_fallback(
            root,
            &skills_root,
            target_config,
            |target, view, global, overlay| {
                install_skill_projection_as_owner(target_config, target, view, global, overlay)
            },
        )
    {
        eprintln!(
            "skill overlay mount refused for {}: {error}",
            target_config.display()
        );
    }
}

/// Only a refused mirror is delegated, and all effects run as the existing
/// parent owner. No daemon chmod, broad home grant or foreign-link replacement.
#[cfg(unix)]
fn install_skill_projection_as_owner(
    config: &Path,
    target: &Path,
    view: &Path,
    global: &Path,
    overlay: &Path,
) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let relative = target.strip_prefix(config).map_err(std::io::Error::other)?;
    if relative
        .components()
        .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(std::io::Error::other("invalid skill projection path"));
    }
    let owner = target
        .parent()
        .into_iter()
        .flat_map(Path::ancestors)
        .take_while(|p| p.starts_with(config))
        .find_map(|p| std::fs::symlink_metadata(p).ok().filter(|m| m.is_dir()));
    let owner = owner.ok_or_else(|| std::io::Error::other("skill projection owner missing"))?;
    // UID and primary GID are independent OS identities.
    let (eff_uid, eff_gid) = (owner.uid(), owner.gid());
    if eff_uid == crate::sandbox::server_uid() {
        return Err(std::io::Error::other(
            "skill projection is not owned by another seat",
        ));
    }
    let launcher = crate::sandbox::descriptor_launcher()
        .ok_or_else(|| std::io::Error::other("skill projection launcher missing"))?;
    let groups = match crate::sandbox::sock_gid_for() {
        Some(sg) if sg != eff_gid && sg != 0 => format!("{eff_gid},{sg}"),
        _ => eff_gid.to_string(),
    };
    let output = std::process::Command::new(launcher).args([
        "--reuid", &eff_uid.to_string(), "--regid", &eff_gid.to_string(),
        "--groups", &groups, "--no-new-privs", "--", "/bin/sh", "-c",
        r#"set -eu
config=$1; relative=$2; view=$3; global=$4; overlay=$5
# Every existing component is inspected by its actual owner, not the daemon
# which was denied traversal. Foreign parent links are preserved untouched.
parent=$config
old_ifs=$IFS; IFS=/; set -- $relative; IFS=$old_ifs
while [ "$#" -gt 1 ]; do
  parent=$parent/$1; shift
  if [ -L "$parent" ]; then exit 0; fi
  mkdir -p "$parent"
done
target=$parent/$1
if [ -L "$target" ]; then
  old=$(readlink "$target")
  if [ "$old" = "$view" ]; then exit 0; fi
  if [ "$old" != "$global" ]; then exit 0; fi
  temp=$parent/.skills-owner-$$
  trap 'rm -f "$temp"' EXIT HUP INT TERM
  ln -s "$view" "$temp"
  mv -fT "$temp" "$target"
elif [ -d "$target" ]; then
  for item in "$target"/*; do
    [ -L "$item" ] || continue
    old=$(readlink "$item"); source_parent=${old%/*}
    if { [ "$source_parent" = "$global" ] || [ "$source_parent" = "$overlay" ]; } && [ ! -e "$view/${item##*/}" ]; then rm "$item"; fi
  done
  for item in "$view"/*; do
    [ -L "$item" ] || continue
    link=$target/${item##*/}; source=$(readlink "$item")
    if [ -L "$link" ]; then
      old=$(readlink "$link"); source_parent=${old%/*}
      if [ "$source_parent" != "$global" ] && [ "$source_parent" != "$overlay" ]; then continue; fi
      if [ "$old" = "$source" ]; then continue; fi
      rm "$link"
    elif [ -e "$link" ]; then continue; fi
    ln -s "$source" "$link"
  done
elif [ ! -e "$target" ]; then ln -s "$view" "$target"; fi
"#, "skill-projection-owner",
    ]).args([config.as_os_str(), relative.as_os_str(), view.as_os_str(), global.as_os_str(), overlay.as_os_str()])
        .stdin(std::process::Stdio::null()).output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "owner skill projection refused: {}",
            output.status
        )))
    }
}

pub(crate) fn ensure_seat_op_config(target_config: &Path, seat_launch: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let mut parts = seat_launch.split('\t');
        let (Some(launcher), Some(uid), Some(gid), Some(groups)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return;
        };
        let Ok(target_uid) = uid.parse::<u32>() else {
            return;
        };
        let Ok(target_gid) = gid.parse::<u32>() else {
            return;
        };
        let dot_config = target_config.join(".config");
        let _ = std::fs::create_dir_all(&dot_config);
        let _ = std::fs::set_permissions(&dot_config, std::fs::Permissions::from_mode(0o2770));
        let op_dir = dot_config.join("op");
        if let Ok(meta) = std::fs::symlink_metadata(&op_dir)
            && (meta.uid() != target_uid || meta.gid() != target_gid)
        {
            let me = crate::sandbox::server_uid();
            if meta.uid() == me {
                let _ = std::fs::remove_dir_all(&op_dir);
            } else {
                let old_uid = meta.uid().to_string();
                let old_gid = meta.gid().to_string();
                let _ = std::process::Command::new(launcher)
                    .args([
                        "--reuid",
                        &old_uid,
                        "--regid",
                        &old_gid,
                        "--groups",
                        &old_gid,
                        "--no-new-privs",
                        "--",
                        "rm",
                        "-rf",
                    ])
                    .arg(&op_dir)
                    .stderr(std::process::Stdio::null())
                    .status();
                let _ = std::fs::remove_dir_all(&op_dir);
            }
        }
        let op_token_src = crate::sandbox::host_home_dir()
            .map(|h| h.join(".config/op-seat/service-account-token"))
            .filter(|p| p.exists())
            .unwrap_or_default();
        let connect_token_src = crate::sandbox::host_home_dir()
            .map(|h| h.join(".config/op/connect-token"))
            .filter(|p| p.exists())
            .unwrap_or_default();
        let _ = std::process::Command::new(launcher)
            .args([
                "--reuid",
                uid,
                "--regid",
                gid,
                "--groups",
                groups,
                "--no-new-privs",
                "--",
                "sh",
                "-c",
                "cd \"$1\" && mkdir -p .config/op && chmod 00700 .config/op && if [ -n \"$2\" ]; then ln -sfn \"$2\" .config/op/service-account-token; fi && if [ -n \"$3\" ]; then ln -sfn \"$3\" .config/op/connect-token; fi",
                "sh",
            ])
            .arg(target_config)
            .arg(op_token_src.as_os_str())
            .arg(connect_token_src.as_os_str())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

pub(crate) fn chmod_seat_file_readable(path: &Path) {
    #[cfg(unix)]
    if !crate::sandbox::is_isolated_test_env()
        && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
        && let Ok(meta) = std::fs::metadata(path)
    {
        use std::os::unix::fs::MetadataExt as _;
        let uid = meta.uid();
        let gid = meta.gid();
        if uid != 0
            && let (Some(parent), Some(fname)) = (path.parent(), path.file_name())
        {
            let uid_s = uid.to_string();
            let gid_s = gid.to_string();
            let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
                .current_dir(parent)
                .args([
                    "--reuid",
                    &uid_s,
                    "--regid",
                    &gid_s,
                    "--groups",
                    &gid_s,
                    "--no-new-privs",
                    "--",
                    "/bin/chmod",
                    "0666",
                ])
                .arg(fname)
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

/// Atomically replace only after a complete write, sync and read-back. A failed
/// destination write never removes the old file or the caller's source.
pub(crate) fn write_required_file(dst: &Path, bytes: &[u8], mode: u32) -> Result<(), Error> {
    use std::io::Write as _;
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dst.with_file_name(format!(
        ".{}.{}.{}.new",
        dst.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id(),
        seq
    ));
    let result = (|| -> Result<(), Error> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        if std::fs::read(&tmp)? != bytes {
            return Err(Error::Message(format!(
                "write verification failed: {}",
                dst.display()
            )));
        }
        std::fs::rename(&tmp, dst)?;
        if let Some(parent) = dst.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        if std::fs::read(dst)? != bytes {
            return Err(Error::Message(format!(
                "write verification failed: {}",
                dst.display()
            )));
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

pub(crate) fn write_seat_shared_file(dst: &Path, bytes: &[u8], mode: u32) -> bool {
    match write_required_file(dst, bytes, mode) {
        Ok(()) => true,
        Err(err) => {
            eprintln!("cannot persist {}: {err}", dst.display());
            false
        }
    }
}

pub(crate) fn write_profile_marker(
    config_dir: &Path,
    marker_file: &str,
    content: &str,
) -> Result<(), Error> {
    write_required_file(&config_dir.join(marker_file), content.as_bytes(), 0o664)
}

pub(crate) fn seeded_profile_name(config_dir: &Path) -> Option<String> {
    std::fs::read_to_string(config_dir.join(".anyagents-seeded-profile"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn persist_refreshed_credentials(root: &Path, rels: &[&str], bytes: &[u8]) -> Result<(), Error> {
    let recovery = root.join(".anyagents-pending-credentials.json");
    write_required_file(&recovery, bytes, 0o600)?;
    for rel in rels {
        write_required_file(&root.join(rel), bytes, 0o600)?;
    }
    std::fs::remove_file(recovery)?;
    Ok(())
}

fn post_anthropic_oauth_refresh(http: &ureq::Agent, req_body: &str) -> Option<serde_json::Value> {
    let custom_url = std::env::var("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let urls: Vec<&str> = match custom_url.as_deref() {
        Some(u) => vec![u],
        None => vec![
            "https://api.anthropic.com/v1/oauth/token",
            "https://platform.claude.com/v1/oauth/token",
        ],
    };
    for url in urls {
        if let Ok(rresp) = http
            .post(url)
            .header("content-type", "application/json")
            .header("user-agent", "claude-code/2.1.310")
            .send(req_body.as_bytes())
            && let Ok(rbody) = rresp.into_body().read_to_string()
            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
            && rval
                .get("access_token")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        {
            return Some(rval);
        }
    }
    None
}

fn seat_sanitized_anthropic_credentials(src_bytes: &[u8], now_ms: i64) -> Vec<u8> {
    if crate::sandbox::is_isolated_test_env()
        && std::env::var_os("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL").is_none()
    {
        return src_bytes.to_vec();
    }
    let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(src_bytes) else {
        return src_bytes.to_vec();
    };
    if let Some(oauth) = val
        .get_mut("claudeAiOauth")
        .and_then(serde_json::Value::as_object_mut)
    {
        let exp = oauth
            .get("expiresAt")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        oauth.insert("refreshToken".to_string(), serde_json::Value::Null);
        if exp <= now_ms {
            oauth.insert(
                "accessToken".to_string(),
                serde_json::Value::String(String::new()),
            );
            oauth.insert("expiresAt".to_string(), serde_json::Value::Number(0.into()));
        }
        if let Ok(out) = serde_json::to_vec(&val) {
            return out;
        }
    }
    src_bytes.to_vec()
}

fn refresh_anthropic_profile_oauth_if_due(data: &Path, profile_root: &Path) -> Result<(), Error> {
    if crate::sandbox::is_isolated_test_env()
        && std::env::var_os("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL").is_none()
    {
        return Ok(());
    }
    let cred_path = profile_root.join(".credentials.json");
    let sub_cred_path = profile_root.join(".claude/.credentials.json");
    let Ok(cred_text) =
        std::fs::read_to_string(&cred_path).or_else(|_| std::fs::read_to_string(&sub_cred_path))
    else {
        return Ok(());
    };
    let Ok(mut cred) = serde_json::from_str::<serde_json::Value>(&cred_text) else {
        return Ok(());
    };
    let now_ms = anyagents_core::now_ms();
    let expires_at = cred
        .pointer("/claudeAiOauth/expiresAt")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(i64::MAX);
    if expires_at > now_ms.saturating_add(45 * 60 * 1_000) {
        return Ok(());
    }
    let attempt_path = profile_root.join(".anyagents-oauth-refresh-attempt-ms");
    let last_attempt_ms = std::fs::read_to_string(&attempt_path)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let min_retry_ms = if expires_at <= now_ms.saturating_add(30 * 60 * 1_000) {
        60_000
    } else {
        300_000
    };
    if now_ms.saturating_sub(last_attempt_ms) < min_retry_ms {
        return Ok(());
    }
    let Some(rtok) = cred
        .pointer("/claudeAiOauth/refreshToken")
        .and_then(serde_json::Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    else {
        return Ok(());
    };
    let _ = std::fs::write(&attempt_path, format!("{now_ms}\n"));
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(8)))
        .build()
        .new_agent();
    let req_body = serde_json::json!({
        "grant_type": "refresh_token",
        "refresh_token": rtok,
        "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
    })
    .to_string();
    let Some(rval) = post_anthropic_oauth_refresh(&http, &req_body) else {
        return Ok(());
    };
    let Some(new_acc) = rval
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    else {
        return Ok(());
    };
    if let Some(oauth) = cred
        .get_mut("claudeAiOauth")
        .and_then(serde_json::Value::as_object_mut)
    {
        oauth.insert(
            "accessToken".to_string(),
            serde_json::Value::String(new_acc),
        );
        if let Some(new_ref) = rval
            .get("refresh_token")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            oauth.insert(
                "refreshToken".to_string(),
                serde_json::Value::String(new_ref.to_string()),
            );
        }
        if let Some(exp_in) = rval.get("expires_in").and_then(serde_json::Value::as_i64) {
            oauth.insert(
                "expiresAt".to_string(),
                serde_json::Value::Number((now_ms + exp_in * 1_000).into()),
            );
        }
        if let Ok(updated) = serde_json::to_string(&cred) {
            if let Err(err) = persist_refreshed_credentials(
                profile_root,
                &[".credentials.json", ".claude/.credentials.json"],
                updated.as_bytes(),
            ) {
                let _ = std::fs::remove_file(&attempt_path);
                return Err(err);
            }
            let _ = std::fs::remove_file(profile_root.join(".anyagents-usage-backoff-until-ms"));
            if let Some(profile_name) = profile_root.file_name().and_then(|n| n.to_str())
                && let Err(err) = propagate_profile_files_to_bound_agents(
                    data,
                    "anthropic",
                    profile_name,
                    &[".credentials.json", ".claude/.credentials.json"],
                )
            {
                let _ = std::fs::remove_file(&attempt_path);
                return Err(err);
            }
        }
    }
    Ok(())
}

fn heal_mcp_config_file(path: &Path) -> bool {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    let needs_heal = match std::fs::read(path) {
        Ok(bytes) => {
            bytes.is_empty()
                || !serde_json::from_slice::<serde_json::Value>(&bytes)
                    .is_ok_and(|val| val.is_object())
        }
        Err(_) => false,
    };
    if needs_heal {
        write_seat_shared_file(path, b"{\"mcpServers\":{}}\n", 0o660)
    } else {
        false
    }
}

pub(crate) fn ensure_agy_mcp_config(profile_roots: &[PathBuf], target_config: &Path) {
    for profile_root in profile_roots {
        let _ = heal_mcp_config_file(&profile_root.join(".gemini/config/mcp_config.json"));
        let _ = heal_mcp_config_file(&profile_root.join("mcp_config.json"));
    }
    let _ = heal_mcp_config_file(&target_config.join(".gemini/config/mcp_config.json"));
    let _ = heal_mcp_config_file(&target_config.join("mcp_config.json"));
}

pub(crate) fn sync_anthropic_agent_credentials_from_profile(
    store: Option<&Store>,
    data: &Path,
    agent: &Agent,
    target_config: &Path,
    profile_root: &Path,
) {
    if !target_config.is_dir() || !profile_root.is_dir() {
        return;
    }
    let _ = refresh_anthropic_profile_oauth_if_due(data, profile_root);
    let src = if profile_root.join(".credentials.json").is_file() {
        profile_root.join(".credentials.json")
    } else if profile_root.join(".claude/.credentials.json").is_file() {
        profile_root.join(".claude/.credentials.json")
    } else {
        return;
    };
    let Ok(raw_src_bytes) = std::fs::read(&src) else {
        return;
    };
    let now_ms = anyagents_core::now_ms();
    let Some(src_exp) = json_oauth_expires_at(&raw_src_bytes) else {
        return;
    };
    if src_exp <= now_ms {
        return;
    }
    let sanitized = seat_sanitized_anthropic_credentials(&raw_src_bytes, now_ms);
    let mut rewrote_any = false;
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        let dst = target_config.join(rel);
        let needs_rewrite = match std::fs::read(&dst) {
            Ok(dst_bytes) => {
                if dst_bytes == sanitized {
                    false
                } else {
                    let dst_exp = json_oauth_expires_at(&dst_bytes).unwrap_or(0);
                    dst_bytes.is_empty()
                        || dst_exp <= now_ms.saturating_add(30 * 60 * 1_000)
                        || src_exp > dst_exp
                }
            }
            Err(_) => !dst.is_dir(),
        };
        if needs_rewrite && write_seat_shared_file(&dst, &sanitized, 0o660) {
            rewrote_any = true;
        }
    }
    if rewrote_any && let Some(s) = store {
        if let Some((_, uid, gid, _)) = resolved_agent_seat_info(s, agent) {
            crate::sandbox::share_config_with_seat_uid(target_config, uid, gid);
        }
        if !s.has_open_turn(&agent.id).unwrap_or(false) {
            let _ = write_profile_marker(
                target_config,
                ".anyagents-profile-rotated-ms",
                &format!("{now_ms}\n"),
            );
            let _ = std::fs::remove_file(target_config.join(".anyagents-last-error.json"));
            poke_agent_wake_socket(data, &agent.id);
        }
    }
}

pub(crate) fn seed_harness_credentials(
    store: &Store,
    data: &Path,
    agent: &Agent,
    target_config: &Path,
) -> Result<(), Error> {
    let mut all_agents = store.list_agents()?;
    if all_agents.len() > 32 {
        all_agents.retain(|a| a.state != "archived");
    }
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let static_rel_paths: &[&str] = match normalize_harness_name(&agent.harness) {
        "claude-code" => &[".credentials.json", ".claude/.credentials.json"],
        "codex" if provider == "openai" => &[
            "auth.json",
            ".codex/auth.json",
            "models_cache.json",
            "model-catalog-direct.json",
            ".config/sylphx/key",
            ".config/opencode/sylphx.key",
            ".config/opencode/opencode.json",
        ],
        "codex" => &[
            "models_cache.json",
            "model-catalog-direct.json",
            ".config/sylphx/key",
            ".config/opencode/sylphx.key",
            ".config/opencode/opencode.json",
        ],
        "opencode" => &[
            "opencode/opencode.json",
            ".config/opencode/opencode.json",
            "opencode.json",
            "data-home/opencode/auth.json",
            ".local/share/opencode/auth.json",
        ],
        "agy" => &[
            ".gemini/antigravity-cli/antigravity-oauth-token",
            ".gemini/antigravity-cli/antigravity_state.pbtxt",
            ".gemini/antigravity-cli/jetski_state.pbtxt",
            ".gemini/config/config.json",
            ".gemini/config/.migrated",
        ],
        "muse" => &[
            "settings.json",
            "muse/settings.json",
            "muse/trust.json",
            "muse/auth.json",
            ".config/muse/settings.json",
            ".config/muse/trust.json",
            ".config/muse/auth.json",
            ".local/share/muse/auth.json",
        ],
        "omp" => &[
            ".omp/agent/agent.db",
            ".omp/agent/models.yml",
            ".omp/agent/models.db",
            "agent.db",
            "models.yml",
            "models.db",
        ],
        "dsh" => &[
            ".dsh/.credentials.yaml",
            ".dsh/settings.yaml",
            ".dsh/.anonymous-user-id",
            ".dsh/profiles/headless/cordis.yml",
        ],
        "grok" => &[
            ".grok/auth.json",
            ".grok/models_cache.json",
            ".grok/settings_cache.json",
        ],
        "spiron" => &[
            ".config/sylphx/credentials.json",
            ".config/sylphx/context.json",
            ".config/sylphx/key",
            ".config/opencode/sylphx.key",
            ".config/opencode/kimi-code-plan.key",
            ".config/opencode/kimi.key",
            ".config/opencode/qwen-token-plan.key",
            ".config/opencode/qwen-coding-plan.key",
            ".config/opencode/glm-code-plan.key",
            ".config/opencode/minimax-code-plan.key",
            ".config/opencode/openrouter.key",
            ".config/opencode/opencode.json",
        ],
        _ => &[],
    };
    let mut rel_paths: Vec<String> = static_rel_paths.iter().map(|s| (*s).to_string()).collect();

    let profile_roots = select_or_bind_profile(data, &all_agents, agent, target_config);
    let current_bound = bound_profile_name(target_config);
    let last_seeded = seeded_profile_name(target_config);
    let explicit_non_default_profile = current_bound.as_ref().is_some_and(|b| {
        let prof = b.split_once('/').map(|(_, p)| p).unwrap_or(b.as_str());
        !prof.is_empty() && prof != "default"
    });
    let profile_changed = current_bound.is_some()
        && (current_bound != last_seeded
            || (last_seeded.is_none() && explicit_non_default_profile));

    if agent.harness == "codex" && provider != "openai" {
        for stale_rel in ["auth.json", ".codex/auth.json"] {
            let stale_path = target_config.join(stale_rel);
            if stale_path.exists() {
                let _ = std::fs::remove_file(&stale_path);
            }
        }
    }

    if let (Some(bound), Some(primary_profile_root)) =
        (current_bound.clone(), profile_roots.first().cloned())
    {
        let harvest_peers = all_agents.len() <= 32;
        for other in &all_agents {
            if !harvest_peers && other.id != agent.id {
                continue;
            }
            let other_cfg = agent_config_dir(data, &other.id);
            if bound_profile_name(&other_cfg).as_deref() == Some(bound.as_str()) {
                for rel in [
                    ".credentials.json",
                    ".claude/.credentials.json",
                    "auth.json",
                    ".codex/auth.json",
                    "models_cache.json",
                    "model-catalog-direct.json",
                    ".gemini/antigravity-cli/antigravity-oauth-token",
                ] {
                    if matches!(rel, "auth.json" | ".codex/auth.json")
                        && !bound.starts_with("openai/")
                    {
                        continue;
                    }
                    let other_src = other_cfg.join(rel);
                    if other_src.is_file() {
                        let _ =
                            copy_seat_file_if_newer(&other_src, &primary_profile_root.join(rel));
                    }
                }
            }
        }
        if provider == "anthropic" {
            refresh_anthropic_profile_oauth_if_due(data, &primary_profile_root)?;
        }
    }
    ensure_agy_mcp_config(&profile_roots, target_config);

    for profile_root in &profile_roots {
        for candidate in [
            ".config/opencode/opencode.json",
            "opencode/opencode.json",
            "opencode.json",
        ] {
            if let Ok(text) = std::fs::read_to_string(profile_root.join(candidate)) {
                for r in extract_file_tilde_refs(&text) {
                    if !rel_paths.contains(&r) {
                        rel_paths.push(r);
                    }
                }
            }
        }
    }
    for rel in &rel_paths {
        let dst = target_config.join(rel);
        let is_anthropic_cred = matches!(
            rel.as_str(),
            ".credentials.json" | ".claude/.credentials.json"
        );
        let is_auth_cred = rel.ends_with(".key")
            || is_anthropic_cred
            || matches!(
                rel.as_str(),
                ".gemini/antigravity-cli/antigravity-oauth-token"
                    | ".gemini/antigravity-cli/antigravity_state.pbtxt"
                    | ".gemini/antigravity-cli/jetski_state.pbtxt"
                    | "auth.json"
                    | ".codex/auth.json"
                    | "muse/auth.json"
                    | ".config/muse/auth.json"
                    | ".grok/auth.json"
                    | ".dsh/.credentials.yaml"
                    | ".omp/agent/agent.db"
                    | "agent.db"
            );
        let dst_non_empty = dst.metadata().is_ok_and(|m| m.len() > 0);
        if dst_non_empty && !is_auth_cred {
            continue;
        }
        for profile_root in &profile_roots {
            let mut src = profile_root.join(rel);
            if !src.is_file() {
                let fallback = match rel.as_str() {
                    ".claude/.credentials.json" => Some(".credentials.json"),
                    ".codex/auth.json" => Some("auth.json"),
                    ".config/muse/auth.json" => Some("muse/auth.json"),
                    _ => None,
                };
                if let Some(fb) = fallback {
                    src = profile_root.join(fb);
                }
            }
            if src.is_file() {
                let Ok(raw_src_bytes) = std::fs::read(&src) else {
                    continue;
                };
                let now_ms = anyagents_core::now_ms();
                let src_bytes = if is_anthropic_cred {
                    seat_sanitized_anthropic_credentials(&raw_src_bytes, now_ms)
                } else {
                    raw_src_bytes
                };
                if dst_non_empty {
                    if std::fs::read(&dst).is_err() {
                        chmod_seat_file_readable(&dst);
                    }
                    if let Ok(dst_bytes) = std::fs::read(&dst) {
                        if src_bytes == dst_bytes {
                            break;
                        }
                        if !profile_changed && !is_anthropic_cred {
                            let different_identity = matches!(
                                (
                                    json_oauth_identity(&src_bytes),
                                    json_oauth_identity(&dst_bytes)
                                ),
                                (Some(s_id), Some(d_id)) if s_id != d_id
                            );
                            let src_exp = json_oauth_expires_at(&src_bytes);
                            let dst_exp = json_oauth_expires_at(&dst_bytes);
                            let newer_oauth = different_identity
                                || matches!(
                                    (src_exp, dst_exp),
                                    (Some(s_exp), Some(d_exp)) if s_exp > d_exp
                                )
                                || (src_exp.is_some_and(|s_exp| s_exp > now_ms)
                                    && dst_exp.is_none());
                            if !newer_oauth {
                                break;
                            }
                        }
                    } else if !profile_changed && !is_anthropic_cred {
                        break;
                    }
                }
                write_required_file(&dst, &src_bytes, 0o660)?;
                break;
            }
        }
    }
    if let Some(ref bound) = current_bound {
        write_profile_marker(
            target_config,
            ".anyagents-seeded-profile",
            &format!("{bound}\n"),
        )?;
        if profile_changed {
            write_profile_marker(
                target_config,
                ".anyagents-profile-rotated-ms",
                &format!("{}\n", anyagents_core::now_ms()),
            )?;
            let _ = std::fs::remove_file(target_config.join(".anyagents-last-error.json"));
        }
    }
    Ok(())
}

/// One Upstream Provider Profile in the server's centralized profile pool
/// (`<data>/profiles/<provider>/<profile>`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProviderProfileEntry {
    pub provider: String,
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    pub kind: String,
    pub compatible_harnesses: Vec<String>,
    pub assigned_agents: Vec<String>,
    #[serde(default)]
    pub subagent_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<String>,
}

/// Caller holds the lifecycle lock and has re-read management authority.
/// Process admission is checked before touching any host pin or native pointer.
pub(crate) fn reset_agent_session_locked(
    store: &Store,
    agent: &Agent,
) -> Result<serde_json::Value, Error> {
    if agent.state == "running" {
        return Err(Error::AgentBusy);
    }
    let _process = store.try_lock_agent(&agent.id)?;
    let root = store.data_dir().join("agents").join(agent.id.as_str());
    let session = anyagents_core::host_session::reset_session_with_source(
        &root,
        &agent.harness,
        harness_session_filename(&agent.harness),
        crate::vendor::memory_source(agent),
    )?;
    Ok(serde_json::json!({
        "agent_id": agent.id, "name": agent.name, "state": agent.state,
        "generation": session.generation, "harness": session.harness,
        "memory_sha256": session.memory.sha256,
    }))
}

pub(crate) fn harness_session_filename(harness: &str) -> Option<&'static str> {
    match harness {
        "codex" => Some("codex-session-id"),
        "opencode" => Some("opencode-session-id"),
        "claude-code" => Some("claude-session-id"),
        "agy" => Some("agy-session-id"),
        "muse" => Some("muse-session-id"),
        "omp" => Some("omp-session-id"),
        "dsh" => Some("dsh-session-id"),
        "grok" => Some("grok-session-id"),
        selector if crate::vendor::is_spiron_harness(selector) => Some("spiron-session-id"),
        _ => None,
    }
}

pub(crate) fn build_harness_continuity_summary(
    store: &Store,
    agent: &Agent,
    previous_harness: Option<&str>,
    target_harness: &str,
) -> Option<String> {
    let turns = store.conversation(&agent.id, None, None, Some(10)).ok()?;
    let last_out_idx = turns.iter().rposition(|t| t.direction == "out")?;
    let completed_turns = &turns[..=last_out_idx];
    if completed_turns.is_empty() {
        return None;
    }
    let header_pair = match previous_harness.filter(|p| !p.is_empty() && *p != target_harness) {
        Some(prev) => format!("`{prev}` -> `{target_harness}`"),
        None => format!("`state.db` -> `{target_harness}`"),
    };
    let mut out = format!(
        "## 4. Cross-Harness Session Continuity ({header_pair})\n\n\
         - This AnyAgents identity (`{}` / `{}`) is running on harness `{target_harness}` while retaining its AnyAgents ID, role description, model (`{}`), workdir (`{}`), mailbox cursor, and external execution custody.\n\
         - **Do NOT replay or repeat already-completed `[OUT]` turns or external side effects** listed below; your mailbox cursor (`delivered_seq`) is already advanced past all prior delivered messages.\n\
         - **Do NOT reopen paused, archived, or restricted product work** merely because the harness session started; act only on the new incoming `<message>` envelope delivered in the current turn.\n\
         - Your full historical two-way transcript in `state.db` remains queryable on demand via `conversation({{ \"agent\": \"self\", \"limit\": 30 }})`.\n\n\
         ### Recent Completed Two-Way Turns (`state.db`)\n\n",
        agent.name,
        agent.id.as_str(),
        agent.model.as_deref().unwrap_or("default"),
        agent.workdir.as_deref().unwrap_or("."),
    );
    for turn in completed_turns {
        let dir_tag = if turn.direction == "out" {
            "OUT (already sent/completed)"
        } else {
            "IN (already delivered)"
        };
        let peer = if turn.direction == "out" {
            turn.to.as_str()
        } else {
            turn.from.as_str()
        };
        let snippet: String = turn
            .text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let truncated: String = if snippet.chars().count() > 360 {
            let head: String = snippet.chars().take(360).collect();
            format!("{head}...")
        } else {
            snippet
        };
        out.push_str(&format!(
            "- `[{dir_tag}]` `{}` (`ts={}`, peer=`{peer}`): {}\n",
            turn.id, turn.timestamp_ms, truncated
        ));
    }
    Some(out)
}

/// Declaratively computes cross-harness session continuity from `state.db` (`SSOT`)
/// whenever an identity with prior history in `state.db` starts a cold session on
/// `agent.harness` or switches back after turns on another harness, while keeping
/// the bridge static across subsequent turns of the same active harness session
/// so LLM prompt-prefix KV caches stay warm.
pub(crate) fn compute_live_session_continuity(
    store: &Store,
    agent: &Agent,
    config: &Path,
) -> Option<String> {
    let _ = std::fs::remove_file(config.join(".anyagents-harness-continuity.md"));
    let bridge_cache_path = config.join(".anyagents-session-bridge.json");
    let recovery_cache_path = config.join(".anyagents-session-recovery.md");
    let turns = store.conversation(&agent.id, None, None, Some(10)).ok()?;
    if turns.is_empty() {
        let _ = std::fs::remove_file(&bridge_cache_path);
        let _ = std::fs::remove_file(&recovery_cache_path);
        return None;
    }
    if let Some(recovery_summary) =
        build_harness_continuity_summary(store, agent, None, &agent.harness)
    {
        let _ = write_seat_shared_file(&recovery_cache_path, recovery_summary.as_bytes(), 0o664);
    } else {
        let _ = std::fs::remove_file(&recovery_cache_path);
    }

    let active_sess_file = harness_session_filename(&agent.harness).map(|f| config.join(f));
    let active_sess_mtime = active_sess_file.as_ref().and_then(|p| {
        let text = std::fs::read_to_string(p).ok()?;
        if text.trim().is_empty() {
            return None;
        }
        std::fs::metadata(p).ok()?.modified().ok()
    });

    let mut newest_other: Option<(&str, std::time::SystemTime)> = None;
    for candidate in [
        "codex",
        "opencode",
        "claude-code",
        "agy",
        "muse",
        "omp",
        "dsh",
        "grok",
        "spiron",
    ] {
        if candidate == normalize_harness_name(&agent.harness) {
            continue;
        }
        if let Some(fname) = harness_session_filename(candidate) {
            let p = config.join(fname);
            if let Ok(text) = std::fs::read_to_string(&p)
                && !text.trim().is_empty()
                && let Ok(meta) = std::fs::metadata(&p)
                && let Ok(mtime) = meta.modified()
                && newest_other.is_none_or(|(_, prev_t)| mtime > prev_t)
            {
                newest_other = Some((candidate, mtime));
            }
        }
    }

    let other_is_newer = match (active_sess_mtime, newest_other) {
        (Some(act_t), Some((_, oth_t))) => oth_t > act_t,
        (None, _) => true,
        (Some(_), None) => false,
    };

    if other_is_newer {
        if active_sess_mtime.is_some()
            && let Some(ref p) = active_sess_file
        {
            let _ = std::fs::remove_file(p);
        }
        let prev_harness = newest_other.map(|(h, _)| h);
        let summary = build_harness_continuity_summary(store, agent, prev_harness, &agent.harness)?;
        let payload = serde_json::json!({
            "harness": agent.harness,
            "previous_harness": prev_harness,
            "summary": summary,
        });
        if let Ok(bytes) = serde_json::to_vec_pretty(&payload) {
            let _ = write_seat_shared_file(&bridge_cache_path, &bytes, 0o664);
        }
        return Some(summary);
    }

    if let Ok(text) = std::fs::read_to_string(&bridge_cache_path)
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
        && val.get("harness").and_then(|v| v.as_str()) == Some(agent.harness.as_str())
        && let Some(cached) = val
            .get("summary")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
    {
        return Some(cached.to_string());
    }

    if let Some((prev_h, _)) = newest_other {
        let summary = build_harness_continuity_summary(store, agent, Some(prev_h), &agent.harness)?;
        let payload = serde_json::json!({
            "harness": agent.harness,
            "previous_harness": prev_h,
            "summary": summary,
        });
        if let Ok(bytes) = serde_json::to_vec_pretty(&payload) {
            let _ = write_seat_shared_file(&bridge_cache_path, &bytes, 0o664);
        }
        return Some(summary);
    }

    None
}

/// Walks `parent_id` in `state.db` up to the root ancestor so multi-hop sub-agents
/// inherit the root ancestor's hint consistently.
pub(crate) fn resolve_root_seat_hint(store: &Store, agent: &Agent) -> Option<String> {
    let mut curr = agent.parent_id.clone();
    let mut hops = 0usize;
    let mut last_name = None;
    while let Some(pid) = curr {
        if hops >= 16 {
            break;
        }
        hops += 1;
        let Ok(parent) = store.agent(&pid) else {
            break;
        };
        last_name = Some(parent.name);
        curr = parent.parent_id;
    }
    last_name.or_else(|| {
        agent
            .launch_env
            .iter()
            .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
            .map(|(_, v)| v.clone())
    })
}

/// Resolve only the operator-admitted principal keyed by this immutable agent ID.
/// Display names, ancestry, and historical filesystem ownership grant no authority.
pub(crate) fn resolved_agent_seat_info(
    store: &Store,
    agent: &Agent,
) -> Option<(String, u32, u32, Option<String>)> {
    let _ = store;
    let seat_hint: Option<String> = None;
    if let Some((user, spec)) =
        crate::sandbox::seat_launch_spec_with_hint(agent.id.as_str(), seat_hint.as_deref())
    {
        let mut parts = spec.split('\t');
        let (_launcher, Some(uid_s), Some(gid_s)) = (parts.next(), parts.next(), parts.next())
        else {
            return None;
        };
        if let (Ok(uid), Ok(gid)) = (uid_s.parse::<u32>(), gid_s.parse::<u32>()) {
            return Some((user, uid, gid, Some(spec)));
        }
    }
    let (user, uid, gid, _) =
        crate::sandbox::seat_for_agent_with_hint(agent.id.as_str(), seat_hint.as_deref())?;
    Some((user, uid, gid, None))
}

/// Canonical, idempotent runtime reconciliation for an agent: resolves the
/// authoritative parent from `state.db`, seeds credentials and model catalogs,
/// ensures the mailbox capability, computes cross-harness continuity from
/// `state.db` when needed, writes all instruction surfaces and the active
/// harness's MCP/model config, and shares permissions with the agent's OS seat.
pub(crate) fn reconcile_agent_runtime_config(
    store: &Store,
    data: &Path,
    agent: &Agent,
    config: &Path,
    work_dir: &Path,
) -> Result<Option<String>, Error> {
    #[cfg(unix)]
    let pre_existing_mode = {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(config)
            .ok()
            .map(|m| m.permissions().mode() & 0o7777)
    };
    create_private_config_dir(config)?;
    if let Some((_, uid, gid, _)) = resolved_agent_seat_info(store, agent) {
        crate::sandbox::prepare_config_with_seat_uid(config, uid, gid)?;
    }
    #[cfg(unix)]
    if let Some(base_mode) = pre_existing_mode {
        use std::os::unix::fs::PermissionsExt as _;
        let group_exec = if (base_mode & 0o0060) != 0 { 0o0010 } else { 0 };
        let other_exec = if (base_mode & 0o0006) != 0 { 0o0001 } else { 0 };
        let safe_mode = (base_mode | 0o0700 | group_exec | other_exec) & 0o7777;
        if safe_mode != base_mode {
            let _ = std::fs::set_permissions(config, std::fs::Permissions::from_mode(safe_mode));
        }
    }
    let parent_name = agent
        .parent_id
        .as_ref()
        .and_then(|pid| store.agent(pid).ok())
        .map(|p| p.name)
        .or_else(|| {
            agent
                .launch_env
                .iter()
                .find(|(k, _)| k == "ANYAGENTS_PARENT_NAME")
                .map(|(_, v)| v.clone())
        });
    ensure_seat_environment(config, work_dir)?;
    seed_harness_credentials(store, data, agent, config)?;
    if !config.join("mcp-capability").is_file()
        && let Err(mint_error) = crate::operator::ensure(store, &agent.id)
    {
        let mut repaired = false;
        #[cfg(unix)]
        if let Some(op_tok) = crate::operator::operator_token(data)
            && let Ok(mut stream) =
                std::os::unix::net::UnixStream::connect(data.join("anyagents.sock"))
        {
            use std::io::{Read as _, Write as _};
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));
            let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(2)));
            let req = format!(
                "POST /v1/operator/agents/{}/capability HTTP/1.1\r\nHost: localhost\r\n{}: {op_tok}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}",
                agent.id,
                crate::operator::HEADER
            );
            stream.write_all(req.as_bytes())?;
            let mut buf = String::new();
            stream.read_to_string(&mut buf)?;
            if !buf.starts_with("HTTP/1.1 200 ") && !buf.starts_with("HTTP/1.1 201 ") {
                return Err(Error::Message("capability repair refused".into()));
            }
            repaired = true;
        }
        if !repaired {
            return Err(mint_error);
        }
    }
    let capability = std::fs::read_to_string(config.join("mcp-capability"))?;
    crate::operator::verify_capability(store, &agent.id, capability.trim())?;
    if crate::sandbox::seats_configured() && resolved_agent_seat_info(store, agent).is_none() {
        return Err(Error::Message(format!(
            "no admitted execution principal for {}",
            agent.id
        )));
    }

    let continuity = compute_live_session_continuity(store, agent, config);
    write_agent_instructions_with_continuity(
        config,
        agent,
        parent_name.as_deref(),
        continuity.as_deref(),
    )?;

    let suppress_config_write = std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some()
        || data.join(".anyagents-suppress-config-write").exists();
    if !suppress_config_write && let Ok(command) = crate::data::current_exe_clean() {
        let args = vec![
            "--data".to_string(),
            data.display().to_string(),
            "mcp".to_string(),
            "--config".to_string(),
            config.display().to_string(),
        ];
        for (other_harness, stale_file) in [
            ("opencode", "opencode.json"),
            ("codex", "config.toml"),
            ("muse", "settings.json"),
            ("claude-code", ".claude.json"),
            ("claude-code", "mcp.json"),
        ] {
            if agent.harness != other_harness {
                let _ = std::fs::remove_file(config.join(stale_file));
            }
        }
        match normalize_harness_name(&agent.harness) {
            "codex" => {
                let codex_toml = config.join("config.toml");
                mcp_config::write_codex_with_model(
                    &codex_toml,
                    agent.model.as_deref(),
                    &command,
                    &args,
                )?;
                mcp_config::sync_codex_developer_instructions(&codex_toml, continuity.as_deref())?;
            }
            "opencode" => {
                opencode_config::write(config, agent.model.as_deref(), &command, &args)?;
            }
            "agy" => {
                mcp_config::write_claude(
                    &config.join(".gemini/config/mcp_config.json"),
                    &command,
                    &args,
                )?;
                crate::vendor::ensure_agy_steering_settings(config, work_dir)?;
            }
            "muse" => {
                muse_config::write(config, agent.model.as_deref(), &command, &args)?;
                muse_config::point_vendor_home(config)?;
            }
            "omp" => {
                crate::grok::write_omp_mcp_config(config, &command, &args)?;
            }
            "dsh" => {
                crate::dsh::write_dsh_mcp_patch(config, agent.model.as_deref(), &command, &args)?;
            }
            "grok" => {
                crate::grok::write_grok_config(config, agent.model.as_deref(), &command, &args)?;
            }
            "spiron" => {
                crate::spiron::write_spiron_config(
                    config,
                    agent.model.as_deref(),
                    &command,
                    &args,
                )?;
            }
            _ => {
                mcp_config::sanitize_claude_settings(config)?;
                mcp_config::write_claude(&config.join(".claude.json"), &command, &args)?;
                mcp_config::write_claude(&config.join("mcp.json"), &command, &args)?;
            }
        }
    }

    if let Some((_, uid, gid, spec_opt)) = resolved_agent_seat_info(store, agent) {
        if let Some(ref spec) = spec_opt {
            ensure_seat_op_config(config, spec);
        }
        crate::sandbox::share_config_with_seat_uid(config, uid, gid);
        // Workdir admission belongs to create, not config-only reconciliation:
        // an archived agent may already have had its managed workdir reaped.
        if agent.harness == "muse"
            && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
            && !crate::sandbox::is_isolated_test_env()
        {
            let mut seat_environment = Vec::new();
            if let Some(spec) = spec_opt {
                seat_environment.push((anyagents_harness::SEAT_LAUNCH_ENV.into(), spec));
                seat_environment.push((
                    anyagents_harness::SEAT_PEERS_ENV.into(),
                    crate::sandbox::peer_principals(store)?,
                ));
            }
            muse_config::ensure_workspace_session(
                data,
                agent.id.as_str(),
                config,
                work_dir,
                agent.model.as_deref(),
                &seat_environment,
            )?;
        }
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let base_mode = pre_existing_mode.unwrap_or(0o0700);
            let group_exec = if (base_mode & 0o0060) != 0 { 0o0010 } else { 0 };
            let other_exec = if (base_mode & 0o0006) != 0 { 0o0001 } else { 0 };
            let safe_mode = (base_mode | 0o0700 | group_exec | other_exec) & 0o7777;
            let _ = std::fs::set_permissions(config, std::fs::Permissions::from_mode(safe_mode));
        }
    }
    verify_harness_runtime_config(config, &agent.harness, agent.model.as_deref())?;
    Ok(continuity)
}

/// Verifies that the active harness's runtime configuration file exists on disk
/// and contains the required AnyAgents MCP server, model, and provider bindings
/// before any harness process is allowed to start.
pub(crate) fn verify_harness_runtime_config(
    config: &Path,
    harness: &str,
    model: Option<&str>,
) -> Result<(), Error> {
    if std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some() {
        return Err(Error::Message(format!(
            "harness runtime config verification failed for `{harness}`: config write suppressed"
        )));
    }
    let has_model = model.map(str::trim).is_some_and(|m| !m.is_empty());
    match harness {
        "codex" => {
            let path = config.join("config.toml");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("codex runtime config missing: {}", path.display()))
            })?;
            if !text.contains("[mcp_servers.anyagents]") || !text.contains("command =") {
                return Err(Error::Message(
                    "codex config.toml missing [mcp_servers.anyagents]".into(),
                ));
            }
            if has_model
                && !text.lines().any(|l| {
                    let t = l.trim_start();
                    t.starts_with("model =") || t.starts_with("model=")
                })
            {
                return Err(Error::Message("codex config.toml missing model".into()));
            }
            let profile_is_sylphx = std::fs::read_to_string(config.join(".anyagents-profile"))
                .ok()
                .is_some_and(|s| {
                    let t = s.trim();
                    t == "sylphx" || t.starts_with("sylphx/")
                });
            let model_is_sylphx = model
                .map(str::trim)
                .is_some_and(|m| m.starts_with("sylphx/"));
            if (profile_is_sylphx || model_is_sylphx)
                && (!text.contains("model_provider = \"sylphx\"")
                    || !text.contains("[model_providers.sylphx]"))
            {
                return Err(Error::Message(
                    "codex config.toml missing sylphx model_provider".into(),
                ));
            }
        }
        "opencode" => {
            for rel in ["opencode/opencode.json", ".config/opencode/opencode.json"] {
                let path = config.join(rel);
                let bytes = std::fs::read(&path).map_err(|_| {
                    Error::Message(format!(
                        "opencode runtime config missing: {}",
                        path.display()
                    ))
                })?;
                let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::Message(format!(
                        "opencode runtime config invalid JSON: {}",
                        path.display()
                    ))
                })?;
                if val.pointer("/mcp/anyagents/command").is_none() {
                    return Err(Error::Message(format!(
                        "opencode config missing mcp.anyagents: {}",
                        path.display()
                    )));
                }
                if has_model
                    && val
                        .get("model")
                        .and_then(|v| v.as_str())
                        .is_none_or(|s| s.trim().is_empty())
                {
                    return Err(Error::Message(format!(
                        "opencode config missing model: {}",
                        path.display()
                    )));
                }
            }
        }
        "muse" => {
            for rel in [
                "settings.json",
                "muse/settings.json",
                ".config/muse/settings.json",
            ] {
                let path = config.join(rel);
                let bytes = std::fs::read(&path).map_err(|_| {
                    Error::Message(format!("muse runtime config missing: {}", path.display()))
                })?;
                let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::Message(format!(
                        "muse runtime config invalid JSON: {}",
                        path.display()
                    ))
                })?;
                if val.get("schema_version").is_none()
                    || val.pointer("/mcpServers/anyagents/command").is_none()
                {
                    return Err(Error::Message(format!(
                        "muse settings.json missing schema_version or mcpServers.anyagents: {}",
                        path.display()
                    )));
                }
            }
        }
        "agy" => {
            let path = config.join(".gemini/config/mcp_config.json");
            let bytes = std::fs::read(&path).map_err(|_| {
                Error::Message(format!("agy runtime config missing: {}", path.display()))
            })?;
            let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                Error::Message(format!(
                    "agy runtime config invalid JSON: {}",
                    path.display()
                ))
            })?;
            if val.pointer("/mcpServers/anyagents/command").is_none() {
                return Err(Error::Message(
                    "agy mcp_config.json missing mcpServers.anyagents".into(),
                ));
            }
        }
        "omp" => {
            let path = config.join(".omp/agent/mcp.json");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("omp runtime config missing: {}", path.display()))
            })?;
            if !text.contains("\"anyagents\"") {
                return Err(Error::Message("omp mcp.json missing anyagents".into()));
            }
        }
        "dsh" => {
            let path = config.join("dsh-mcp-patch.yml");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("dsh runtime config missing: {}", path.display()))
            })?;
            if !text.contains("anyagents") {
                return Err(Error::Message("dsh-mcp-patch.yml missing anyagents".into()));
            }
        }
        "grok" => {
            let path = config.join(".grok/config.toml");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("grok runtime config missing: {}", path.display()))
            })?;
            if !text.contains("[mcp_servers.anyagents]") {
                return Err(Error::Message(
                    "grok config.toml missing [mcp_servers.anyagents]".into(),
                ));
            }
        }
        selector if crate::vendor::is_spiron_harness(selector) => {
            let path = config.join("spiron-mcp.json");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("spiron runtime config missing: {}", path.display()))
            })?;
            if !text.contains("\"anyagents\"") {
                return Err(Error::Message(
                    "spiron-mcp.json missing anyagents MCP server".into(),
                ));
            }
        }
        _ => {
            for rel in ["mcp.json", ".claude.json"] {
                let path = config.join(rel);
                let bytes = std::fs::read(&path).map_err(|_| {
                    Error::Message(format!(
                        "claude-code runtime config missing: {}",
                        path.display()
                    ))
                })?;
                let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::Message(format!(
                        "claude-code runtime config invalid JSON: {}",
                        path.display()
                    ))
                })?;
                if val.pointer("/mcpServers/anyagents/command").is_none() {
                    return Err(Error::Message(format!(
                        "claude-code {} missing mcpServers.anyagents",
                        path.display()
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Notifies a sub-agent's parent once when the sub-agent enters `turn_state == "errored"`.
/// Uses both a marker file in `config` (`.anyagents-notified-parent-error.json`) and
/// `Store::receive`'s ACID `source_key` deduplication so the parent never receives
/// duplicate alerts for the same error episode.
pub(crate) fn maybe_notify_parent_on_error(
    store: &Store,
    agent: &Agent,
    config: &Path,
    err_msg: &str,
) {
    let Some(ref parent_id) = agent.parent_id else {
        return;
    };
    let trimmed = err_msg.trim();
    if trimmed.is_empty() || crate::host_memory::is_migration_wait(trimmed) {
        return;
    }
    let rotated_ms = std::fs::read_to_string(config.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let marker_path = config.join(".anyagents-notified-parent-error.json");
    let source_key = format!(
        "subagent-error:{}:{rotated_ms}:{trimmed}",
        agent.id.as_str()
    );
    if let Ok(existing) = std::fs::read_to_string(&marker_path)
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&existing)
        && val.get("source_key").and_then(|v| v.as_str()) == Some(source_key.as_str())
    {
        return;
    }
    let marker = serde_json::json!({
        "source_key": source_key,
        "error": trimmed,
        "timestamp_ms": anyagents_core::now_ms(),
    });
    let _ = std::fs::write(&marker_path, marker.to_string());
    let text = format!(
        "[Sub-Agent Error] Sub-agent `{}` (`{}`) entered `turn_state: \"errored\"` on harness `{}`: {}",
        agent.name,
        agent.id.as_str(),
        agent.harness,
        trimmed
    );
    if let Ok(anyagents_core::ReceiveOutcome::New(_)) = store.receive(
        parent_id,
        anyagents_core::Incoming {
            text,
            sender: Some(format!("agent:{}", agent.name)),
            source_time_ms: None,
            source_key: Some(source_key),
            reply_to: None,
            quote: None,
            origin: None,
            received_at_ms: None,
        },
    ) {
        poke_agent_wake_socket_mail(store.data_dir(), parent_id);
    }
}

pub(crate) fn poke_agent_wake_socket_mail(data: &Path, agent_id: &AgentId) {
    #[cfg(unix)]
    {
        use std::os::unix::net::UnixDatagram;
        let sock_path = data.join("wake.sock");
        if sock_path.exists()
            && let Ok(sock) = UnixDatagram::unbound()
        {
            let msg = format!("m{}", agent_id.as_str());
            let _ = sock.send_to(msg.as_bytes(), &sock_path);
        }
    }
}

/// Declaratively update an existing agent's runtime configuration (`harness`, `model`, `profile`)
/// in `state.db` without deleting or recreating its identity, killing an in-progress turn,
/// replaying already-delivered messages, or reopening paused scope.
pub(crate) fn configure_agent_runtime(
    store: &Store,
    data: &Path,
    actor: &str,
    name: &str,
    harness: Option<&str>,
    model: Option<&str>,
    profile: Option<&str>,
) -> Result<serde_json::Value, Error> {
    let existing = store.agent_by_name(name)?;
    let previous_harness = existing.harness.clone();
    let resolved_model = model
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| existing.model.clone());
    let config = agent_config_dir(data, &existing.id);
    let target_harness = match harness.map(str::trim) {
        Some("") => return Err(Error::Message("harness is empty".into())),
        Some(h) => normalize_harness_name(h).to_string(),
        None => normalize_harness_name(&existing.harness).to_string(),
    };
    crate::install::adapter(&target_harness)?;
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && !crate::sandbox::is_isolated_test_env()
        && actor != "system:profile-rebalance"
    {
        crate::install::record(data, &target_harness).map_err(Error::Message)?;
    }
    let current_bound = bound_profile_name(&config);

    let (provider, profile_name) =
        if let Some(explicit_prof) = profile.map(str::trim).filter(|s| !s.is_empty()) {
            if let Some((p, n)) = explicit_prof.split_once('/') {
                (p.trim().to_string(), n.trim().to_string())
            } else {
                (
                    upstream_provider_of(&target_harness, resolved_model.as_deref()),
                    explicit_prof.to_string(),
                )
            }
        } else if let Some(ref bound) = current_bound {
            if let Some((p, n)) = bound.split_once('/') {
                let target_prov = upstream_provider_of(&target_harness, resolved_model.as_deref());
                if model.is_some() && p.trim() != target_prov {
                    (target_prov, "default".to_string())
                } else {
                    (p.trim().to_string(), n.trim().to_string())
                }
            } else {
                (
                    upstream_provider_of(&target_harness, resolved_model.as_deref()),
                    bound.trim().to_string(),
                )
            }
        } else {
            (
                upstream_provider_of(&target_harness, resolved_model.as_deref()),
                "default".to_string(),
            )
        };

    let valid_token = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    };
    if !valid_token(&provider) || !valid_token(&profile_name) {
        return Err(Error::Message(format!(
            "invalid provider profile `{provider}/{profile_name}`"
        )));
    }
    let (_, default_compat) = default_kind_and_harnesses(&provider);
    let profile_dir = data.join("profiles").join(&provider).join(&profile_name);
    let compatible_harnesses = if profile_dir.join("profile.json").is_file() {
        std::fs::read_to_string(profile_dir.join("profile.json"))
            .ok()
            .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
            .and_then(|v| {
                v.get("compatible_harnesses")
                    .and_then(|a| a.as_array())
                    .cloned()
            })
            .map(|arr| {
                let mut v: Vec<String> = arr
                    .into_iter()
                    .filter_map(|x| x.as_str().map(normalize_harness_name).map(str::to_string))
                    .collect();
                for d in &default_compat {
                    if !v.iter().any(|x| x == d) {
                        v.push(d.clone());
                    }
                }
                v
            })
            .filter(|v| !v.is_empty())
            .unwrap_or(default_compat)
    } else {
        default_compat
    };
    if !compatible_harnesses.iter().any(|h| h == &target_harness) {
        return Err(Error::Message(format!(
            "harness `{target_harness}` is not compatible with provider profile `{provider}/{profile_name}` (compatible harnesses: {})",
            compatible_harnesses.join(", ")
        )));
    }
    if data.join("profiles").is_dir() && !profile_dir.is_dir() {
        return Err(Error::Message(format!(
            "provider profile not found: `{provider}/{profile_name}`"
        )));
    }

    let open_turn = store.has_open_turn(&existing.id).unwrap_or(false);
    let lock_busy = matches!(store.try_lock_agent(&existing.id), Err(Error::AgentBusy));
    let in_progress_turn = existing.state == "running" && (open_turn || lock_busy);

    let audit_action = if harness.is_some() {
        "agents.harness"
    } else {
        "agents.model"
    };
    store.record_admin_audit(actor, audit_action, &existing.name)?;
    if existing.harness != target_harness {
        store.set_agent_harness(&existing.id, &target_harness)?;
    }
    if let Some(ref m) = resolved_model
        && existing.model.as_deref() != Some(m.as_str())
    {
        store.set_agent_model(&existing.id, m)?;
    }
    let updated = store.agent(&existing.id)?;

    create_private_config_dir(&config)?;
    let normalized_profile = format!("{provider}/{profile_name}");
    if profile.is_some() && profile_dir.is_dir() {
        fn copy_files(source: &Path, target: &Path) -> Result<(), Error> {
            std::fs::create_dir_all(target)?;
            for entry in std::fs::read_dir(source)? {
                let entry = entry?;
                let fname = entry.file_name();
                let fname_s = fname.to_string_lossy();
                if matches!(
                    fname_s.as_ref(),
                    "annotations"
                        | "bin"
                        | "brain"
                        | "builtin"
                        | "cache"
                        | "cache-home"
                        | "conversations"
                        | "crashes"
                        | "implicit"
                        | "log"
                        | "presence"
                        | "updater"
                        | "projects"
                        | "sessions"
                        | "session-env"
                        | "shell-snapshots"
                        | "backups"
                        | "cli.log"
                        | "conversation_summaries.db"
                        | "conversation_summaries.db-shm"
                        | "conversation_summaries.db-wal"
                ) {
                    continue;
                }
                let kind = entry.file_type()?;
                let target = target.join(&fname);
                if kind.is_dir() {
                    copy_files(&entry.path(), &target)?;
                } else if kind.is_file() && entry.file_name() != "profile.json" {
                    let bytes = std::fs::read(entry.path())?;
                    write_required_file(&target, &bytes, 0o660)?;
                }
            }
            Ok(())
        }
        copy_files(&profile_dir, &config)?;
        write_profile_marker(
            &config,
            ".anyagents-profile-rotated-ms",
            &format!("{}\n", anyagents_core::now_ms()),
        )?;
    }
    write_profile_marker(
        &config,
        ".anyagents-profile-rotated-ms",
        &format!("{}\n", anyagents_core::now_ms()),
    )?;
    write_profile_marker(
        &config,
        ".anyagents-profile",
        &format!("{normalized_profile}\n"),
    )?;
    let _ = std::fs::remove_file(config.join(".anyagents-last-error.json"));
    if updated.harness == "agy" {
        clear_stale_agy_quota_error_steps(&config);
    }

    let work_dir = updated
        .workdir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| agent_work_dir(data, &updated.id));
    reconcile_agent_runtime_config(store, data, &updated, &config, &work_dir)?;
    poke_agent_wake_socket(data, &updated.id);

    let process = if lock_busy { "alive" } else { "down" };
    let turn_state = if in_progress_turn { "working" } else { "idle" };
    let cutover_status = if in_progress_turn {
        "pending_active_turn"
    } else if lock_busy && previous_harness == target_harness {
        "running"
    } else {
        "ready_on_next_wake"
    };

    Ok(serde_json::json!({
        "id": updated.id.as_str(),
        "name": updated.name,
        "agent": updated.name,
        "parent": updated.parent_id.as_ref().map(|p| p.as_str()),
        "previous_harness": previous_harness,
        "harness": updated.harness,
        "model": updated.model,
        "profile": normalized_profile,
        "workdir": updated.workdir,
        "state": updated.state,
        "process": process,
        "turn_state": turn_state,
        "in_progress_turn": in_progress_turn,
        "cutover_status": cutover_status,
        "description_preserved": updated.instructions.is_some(),
    }))
}

pub(crate) fn set_agent_harness(
    store: &Store,
    data: &Path,
    actor: &str,
    name: &str,
    harness: &str,
    model: Option<&str>,
    profile: Option<&str>,
) -> Result<serde_json::Value, Error> {
    if harness.trim().is_empty() {
        return Err(Error::Message("harness is empty".into()));
    }
    configure_agent_runtime(store, data, actor, name, Some(harness), model, profile)
}

/// Create or update a named provider profile in `<data>/profiles/<provider>/<profile>`.
pub(crate) fn set_provider_profile(
    data: &Path,
    provider_profile: &str,
    key: &str,
) -> Result<(String, String), Error> {
    let (provider, profile) = provider_profile.split_once('/').ok_or_else(|| {
        Error::Message("expected <provider>/<profile> (e.g. provider/example)".into())
    })?;
    let valid = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    };
    if !valid(provider) || !valid(profile) {
        return Err(Error::Message("invalid provider or profile name".into()));
    }
    let trimmed = key.trim();
    if trimmed.is_empty() || trimmed.contains(char::is_whitespace) {
        return Err(Error::Message(
            "a nonempty single-line key is required".into(),
        ));
    }
    let dir = data.join("profiles").join(provider).join(profile);
    ensure_provider_profile_meta(&dir, provider, None);
    let default_dir = data.join("profiles").join(provider).join("default");
    if default_dir != dir {
        for rel in [".config/opencode/opencode.json", "opencode.json"] {
            if default_dir.join(rel).is_file() && !dir.join(rel).exists() {
                if let Some(p) = dir.join(rel).parent() {
                    let _ = std::fs::create_dir_all(p);
                }
                let _ = std::fs::copy(default_dir.join(rel), dir.join(rel));
            }
        }
    }
    if provider == "google-antigravity" {
        let tok_dir = dir.join(".gemini").join("antigravity-cli");
        std::fs::create_dir_all(&tok_dir)?;
        let tok_path = tok_dir.join("antigravity-oauth-token");
        std::fs::write(&tok_path, format!("{trimmed}\n"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&tok_path, std::fs::Permissions::from_mode(0o600));
        }
        let _ = std::fs::remove_file(dir.join(".config/opencode/google.key"));
        propagate_profile_files_to_bound_agents(
            data,
            provider,
            profile,
            &[".gemini/antigravity-cli/antigravity-oauth-token"],
        )?;
        return Ok((provider.to_string(), profile.to_string()));
    }
    let key_filenames: &[&str] = match provider {
        "kimi-code-plan" => &["kimi-code-plan.key", "kimi.key"],
        "qwen-token-plan" => &["qwen-coding-plan.key", "qwen.key"],
        "glm-code-plan" => &["glm-code-plan.key", "glm.key"],
        "minimax-code-plan" => &["minimax-code-plan.key", "minimax.key"],
        "sylphx" => &["sylphx.key"],
        "openrouter" => &["openrouter.key"],
        "deepseek" => &["deepseek.key"],
        "google-gemini" => &["gemini.key"],
        other => {
            let prefix = other.split('-').next().unwrap_or(other);
            let fname = format!("{prefix}.key");
            write_profile_key_file(&dir, &fname, trimmed)?;
            let rel = format!(".config/opencode/{fname}");
            propagate_profile_files_to_bound_agents(data, provider, profile, &[rel.as_str()])?;
            return Ok((provider.to_string(), profile.to_string()));
        }
    };
    let mut rels = Vec::new();
    for fname in key_filenames {
        write_profile_key_file(&dir, fname, trimmed)?;
        rels.push(format!(".config/opencode/{fname}"));
    }
    let rel_refs: Vec<&str> = rels.iter().map(String::as_str).collect();
    propagate_profile_files_to_bound_agents(data, provider, profile, &rel_refs)?;
    Ok((provider.to_string(), profile.to_string()))
}

fn write_profile_key_file(dir: &Path, filename: &str, key: &str) -> Result<(), Error> {
    let key_dir = dir.join(".config").join("opencode");
    std::fs::create_dir_all(&key_dir)?;
    let key_path = key_dir.join(filename);
    std::fs::write(&key_path, format!("{key}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

pub(crate) fn propagate_profile_files_to_bound_agents(
    data: &Path,
    provider: &str,
    profile: &str,
    rel_paths: &[&str],
) -> Result<(), Error> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    let target_tag = format!("{provider}/{profile}");
    let store_opt = Store::open(data.join("state.db")).ok();
    let all_agents = store_opt
        .as_ref()
        .and_then(|s| s.list_agents().ok())
        .unwrap_or_default();
    let active_agents: Vec<&Agent> = all_agents
        .iter()
        .filter(|a| a.state != "archived")
        .collect();
    for agent_rec in active_agents {
        let cfg = agent_config_dir(data, &agent_rec.id);
        let dir_id = agent_rec.id.as_str();
        if !cfg.is_dir() {
            continue;
        }
        let bound = bound_profile_name(&cfg);
        let matches_target = match bound.as_deref() {
            Some(b) if b == target_tag => true,
            Some(b) if b == profile => {
                upstream_provider_of(&agent_rec.harness, agent_rec.model.as_deref()) == provider
            }
            None if profile == "default" => {
                upstream_provider_of(&agent_rec.harness, agent_rec.model.as_deref()) == provider
            }
            _ => false,
        };
        if !matches_target {
            continue;
        }
        let mut installed = 0usize;
        for rel in rel_paths {
            let mut src = prof_dir.join(rel);
            if !src.is_file() {
                let fb = match *rel {
                    ".claude/.credentials.json" => Some(".credentials.json"),
                    ".codex/auth.json" => Some("auth.json"),
                    ".config/muse/auth.json" => Some("muse/auth.json"),
                    _ => None,
                };
                if let Some(f) = fb {
                    src = prof_dir.join(f);
                }
            }
            let raw_bytes = match std::fs::read(&src) {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue, // Optional aliases may be absent.
                Err(err) => return Err(err.into()),
            };
            {
                let bytes = if matches!(*rel, ".credentials.json" | ".claude/.credentials.json") {
                    seat_sanitized_anthropic_credentials(&raw_bytes, anyagents_core::now_ms())
                } else {
                    raw_bytes
                };
                if bytes.is_empty() {
                    return Err(Error::Message(format!(
                        "empty credential source for {dir_id}: {rel}"
                    )));
                }
                if let Err(err) = write_required_file(&cfg.join(rel), &bytes, 0o660) {
                    // No seeded/rotation marker or wake is published for this failed agent.
                    eprintln!("credential propagation failed for {dir_id}: {err}");
                    return Err(err);
                }
                installed += 1;
            }
        }
        if !rel_paths.is_empty() && installed == 0 {
            return Err(Error::Message(format!(
                "no profile credentials installed for {dir_id}"
            )));
        }
        write_profile_marker(
            &cfg,
            ".anyagents-seeded-profile",
            &format!("{target_tag}\n"),
        )?;
        write_profile_marker(
            &cfg,
            ".anyagents-profile-rotated-ms",
            &format!("{}\n", anyagents_core::now_ms()),
        )?;
        let _ = std::fs::remove_file(cfg.join(".anyagents-last-error.json"));
        if let Some(s) = store_opt.as_ref() {
            if let Some((_, uid, gid, _)) = resolved_agent_seat_info(s, agent_rec) {
                crate::sandbox::share_config_with_seat_uid(&cfg, uid, gid);
            }
            poke_agent_wake_socket(data, &agent_rec.id);
        }
    }
    Ok(())
}

pub(crate) fn update_profile_json_identity(
    prof_dir: &Path,
    provider: &str,
    email: Option<&str>,
    account_id: Option<&str>,
) {
    ensure_provider_profile_meta(prof_dir, provider, None);
    let meta_path = prof_dir.join("profile.json");
    let mut val = std::fs::read_to_string(&meta_path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let Some(obj) = val.as_object_mut() else {
        return;
    };
    let mut changed = false;
    if let Some(em) = email.map(str::trim).filter(|s| !s.is_empty())
        && obj.get("account").and_then(serde_json::Value::as_str) != Some(em)
    {
        obj.insert(
            "account".to_string(),
            serde_json::Value::String(em.to_string()),
        );
        changed = true;
    }
    if let Some(aid) = account_id.map(str::trim).filter(|s| !s.is_empty())
        && obj.get("account_id").and_then(serde_json::Value::as_str) != Some(aid)
    {
        obj.insert(
            "account_id".to_string(),
            serde_json::Value::String(aid.to_string()),
        );
        changed = true;
    }
    if changed && let Ok(text) = serde_json::to_string_pretty(&val) {
        let _ = std::fs::write(&meta_path, text);
    }
}

fn extract_jwt_email(jwt: &str) -> Option<String> {
    let payload_b64 = jwt.split('.').nth(1)?;
    let decoded = decode_b64url(payload_b64)?;
    let val: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    val.get("email")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            val.pointer("/https:~1~1api.openai.com~1profile/email")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Extracts `(email, account_id)` for a provider profile directory from its
/// vendor credential files or `profile.json`.
pub(crate) fn extract_profile_account_identity(
    prof_dir: &Path,
    provider: &str,
) -> (Option<String>, Option<String>) {
    let mut email: Option<String> = None;
    let mut account_id: Option<String> = None;

    match provider {
        "anthropic" => {
            for rel in [".credentials.json", ".claude/.credentials.json"] {
                if let Ok(text) = std::fs::read_to_string(prof_dir.join(rel))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                {
                    if email.is_none() {
                        email = val
                            .pointer("/claudeAiOauth/email")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                    if account_id.is_none() {
                        account_id = val
                            .pointer("/claudeAiOauth/accountUuid")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                }
            }
            for rel in [".claude.json", ".claude/.claude.json"] {
                if let Ok(text) = std::fs::read_to_string(prof_dir.join(rel))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                {
                    if email.is_none() {
                        email = val
                            .pointer("/oauthAccount/emailAddress")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                    if account_id.is_none() {
                        account_id = val
                            .pointer("/oauthAccount/accountUuid")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                }
            }
        }
        "openai" => {
            for rel in ["auth.json", ".codex/auth.json"] {
                if let Ok(text) = std::fs::read_to_string(prof_dir.join(rel))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                {
                    if email.is_none() {
                        email = val
                            .get("id_token_email")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string)
                            .or_else(|| {
                                val.pointer("/tokens/id_token")
                                    .and_then(serde_json::Value::as_str)
                                    .and_then(extract_jwt_email)
                            });
                    }
                    if account_id.is_none() {
                        account_id = val
                            .pointer("/tokens/account_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                }
            }
        }
        "google-antigravity" => {
            let tok_path = prof_dir.join(".gemini/antigravity-cli/antigravity-oauth-token");
            if let Ok(text) = std::fs::read_to_string(tok_path)
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
            {
                email = val
                    .get("email")
                    .or_else(|| val.get("id_token_email"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                account_id = val
                    .get("account_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
            }
        }
        "muse" => {
            for rel in ["muse/auth.json", ".config/muse/auth.json"] {
                if let Ok(text) = std::fs::read_to_string(prof_dir.join(rel))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                    && email.is_none()
                {
                    email = val
                        .pointer("/providers/meta/user_email")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string);
                }
            }
        }
        "xai" => {
            if let Ok(text) = std::fs::read_to_string(prof_dir.join(".grok/auth.json"))
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                && let Some(obj) = val.as_object()
            {
                for entry in obj.values() {
                    if email.is_none() {
                        email = entry
                            .get("email")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                    if account_id.is_none() {
                        account_id = entry
                            .get("user_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                }
            }
        }
        "cursor" => {
            for rel in ["agent.db", ".omp/agent/agent.db"] {
                let db_path = prof_dir.join(rel);
                if db_path.is_file()
                    && let Ok(conn) = rusqlite::Connection::open_with_flags(
                        &db_path,
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                    )
                    && let Ok(id_key) = conn.query_row(
                        "SELECT identity_key FROM auth_credentials WHERE provider = 'cursor' ORDER BY id DESC LIMIT 1",
                        [],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    && let Some(raw_id) = id_key
                {
                    let cleaned = raw_id
                        .strip_prefix("account:")
                        .unwrap_or(&raw_id)
                        .split('|')
                        .next_back()
                        .unwrap_or(&raw_id)
                        .trim()
                        .to_string();
                    if !cleaned.is_empty() {
                        if cleaned.contains('@') {
                            email = Some(cleaned);
                        } else {
                            account_id = Some(cleaned);
                        }
                        break;
                    }
                }
            }
        }
        _ => {}
    }

    if let Ok(raw) = std::fs::read_to_string(prof_dir.join("profile.json"))
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
    {
        if email.is_none() {
            email = val
                .get("account")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
        }
        if account_id.is_none() {
            account_id = val
                .get("account_id")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
        }
    }

    (email, account_id)
}

fn sanitize_profile_slug(raw: &str) -> String {
    let base = raw.split('@').next().unwrap_or(raw).trim();
    let mut slug = String::new();
    for ch in base.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            slug.push(ch.to_ascii_lowercase());
        } else if ch == '.' && !slug.ends_with('-') && !slug.is_empty() {
            slug.push('-');
        }
    }
    let slug = slug.trim_matches('-').trim_matches('_');
    if slug.is_empty() || slug == "default" {
        "acct".to_string()
    } else {
        slug.to_string()
    }
}

fn copy_dir_recursive_overwrite(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        let dst_path = dst.join(entry.file_name());
        if ft.is_dir() {
            copy_dir_recursive_overwrite(&entry.path(), &dst_path)?;
        } else if ft.is_file() {
            let bytes = std::fs::read(entry.path())?;
            write_required_file(&dst_path, &bytes, 0o600).map_err(std::io::Error::other)?;
        } else {
            return Err(std::io::Error::other(
                "staged credentials must be regular files or directories",
            ));
        }
    }
    Ok(())
}

fn extract_profile_dedup_key(prof_dir: &Path, provider: &str) -> Option<String> {
    let (email, account_id) = extract_profile_account_identity(prof_dir, provider);
    if let Some(em) = email {
        return Some(format!("email:{}", em.to_ascii_lowercase()));
    }
    if let Some(id) = account_id {
        return Some(format!("id:{id}"));
    }
    match provider {
        "deepseek" => read_first_non_empty_file(prof_dir, &[".config/opencode/deepseek.key"])
            .or_else(|| read_dsh_yaml_ref(prof_dir, "DEEPSEEK_API_KEY"))
            .map(|k| format!("key:{k}")),
        "kimi-code-plan" => read_first_non_empty_file(
            prof_dir,
            &[
                ".config/opencode/kimi-code-plan.key",
                ".config/opencode/kimi.key",
            ],
        )
        .map(|k| format!("key:{k}")),
        "glm-code-plan" => read_first_non_empty_file(
            prof_dir,
            &[
                ".config/opencode/glm-code-plan.key",
                ".config/opencode/glm.key",
            ],
        )
        .map(|k| format!("key:{k}")),
        "minimax-code-plan" => read_first_non_empty_file(
            prof_dir,
            &[
                ".config/opencode/minimax-code-plan.key",
                ".config/opencode/minimax.key",
            ],
        )
        .map(|k| format!("key:{k}")),
        "openrouter" => read_first_non_empty_file(prof_dir, &[".config/opencode/openrouter.key"])
            .or_else(|| read_dsh_yaml_ref(prof_dir, "OPENROUTER_API_KEY"))
            .map(|k| format!("key:{k}")),
        "google-gemini" => {
            read_first_non_empty_file(prof_dir, &["gemini-api-key", ".config/opencode/gemini.key"])
                .or_else(|| read_dsh_yaml_ref(prof_dir, "GEMINI_API_KEY"))
                .map(|k| format!("key:{k}"))
        }
        _ => None,
    }
}

/// Enforces the `1 Email / Account = 1 Profile` invariant across all providers under
/// `<data>/profiles/<provider>`. If two or more profiles under the same provider share
/// the exact same email, account ID, or API key, merges them into a single canonical
/// profile (preferring `default` when present), updates any bound agents' `.anyagents-profile`
/// and `.anyagents-seeded-profile` markers, and removes the duplicate profile directory.
pub(crate) fn deduplicate_same_account_profiles(data: &Path) {
    let profiles_root = data.join("profiles");
    let Ok(providers) = std::fs::read_dir(&profiles_root) else {
        return;
    };
    for prov_entry in providers.flatten() {
        let prov_path = prov_entry.path();
        if !prov_path.is_dir() {
            continue;
        }
        let provider = prov_entry.file_name().to_string_lossy().into_owned();
        if provider.starts_with('.') {
            continue;
        }
        let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
            continue;
        };
        let mut profs: Vec<(String, PathBuf)> = prof_dirs
            .flatten()
            .filter_map(|e| {
                let p = e.path();
                if !p.is_dir() {
                    return None;
                }
                let name = e.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') {
                    return None;
                }
                Some((name, p))
            })
            .collect();
        if profs.len() <= 1 {
            continue;
        }
        profs.sort_by(|a, b| {
            (a.0 != "default")
                .cmp(&(b.0 != "default"))
                .then_with(|| a.0.cmp(&b.0))
        });

        let mut canonical_by_key: std::collections::BTreeMap<String, (String, PathBuf)> =
            std::collections::BTreeMap::new();
        let mut merged_into: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();

        for (prof_name, prof_path) in profs {
            let (em, aid) = extract_profile_account_identity(&prof_path, &provider);
            let dedup_key = extract_profile_dedup_key(&prof_path, &provider);
            let mut matched_canon: Option<(String, PathBuf)> = None;
            if let Some(ref e) = em {
                let k = format!("email:{}", e.to_ascii_lowercase());
                if let Some(c) = canonical_by_key.get(&k) {
                    matched_canon = Some(c.clone());
                }
            }
            if matched_canon.is_none()
                && let Some(ref id) = aid
            {
                let k = format!("id:{id}");
                if let Some(c) = canonical_by_key.get(&k) {
                    matched_canon = Some(c.clone());
                }
            }
            if matched_canon.is_none()
                && let Some(ref dk) = dedup_key
                && let Some(c) = canonical_by_key.get(dk)
            {
                matched_canon = Some(c.clone());
            }

            if let Some((canon_name, canon_path)) = matched_canon {
                let rels: &[&str] = match provider.as_str() {
                    "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
                    "openai" => &["auth.json", ".codex/auth.json"],
                    "google-antigravity" => &[
                        ".gemini/antigravity-cli/antigravity-oauth-token",
                        ".gemini/antigravity-cli/antigravity_state.pbtxt",
                        ".gemini/config/config.json",
                    ],
                    "muse" => &["muse/auth.json", ".config/muse/auth.json"],
                    "xai" => &[".grok/auth.json"],
                    _ => &[],
                };
                let mut complete = !rels.is_empty();
                for rel in rels {
                    let src = prof_path.join(rel);
                    if src.is_file() {
                        let dst = canon_path.join(rel);
                        let copied = copy_seat_file_if_newer(&src, &dst);
                        complete &= copied
                            || std::fs::read(&src)
                                .ok()
                                .zip(std::fs::read(&dst).ok())
                                .is_some_and(|(a, b)| a == b);
                    }
                }
                if !complete {
                    continue;
                }
                // Retain every source byte in an excluded recovery directory before rebinding.
                let recovery = prof_path
                    .with_file_name(format!(".merged-{prof_name}-{}", anyagents_core::now_ms()));
                if recovery.exists() || std::fs::rename(&prof_path, &recovery).is_err() {
                    continue;
                }
                merged_into.insert(prof_name, canon_name);
            } else {
                if let Some(e) = em {
                    canonical_by_key.insert(
                        format!("email:{}", e.to_ascii_lowercase()),
                        (prof_name.clone(), prof_path.clone()),
                    );
                }
                if let Some(id) = aid {
                    canonical_by_key
                        .insert(format!("id:{id}"), (prof_name.clone(), prof_path.clone()));
                }
                if let Some(dk) = dedup_key {
                    canonical_by_key.insert(dk, (prof_name, prof_path));
                }
            }
        }

        if !merged_into.is_empty()
            && let Ok(agents_dir) = std::fs::read_dir(data.join("agents"))
        {
            for entry in agents_dir.flatten() {
                let cfg = entry.path().join("config");
                if !cfg.is_dir() {
                    continue;
                }
                for marker in [".anyagents-profile", ".anyagents-seeded-profile"] {
                    if let Ok(raw) = std::fs::read_to_string(cfg.join(marker)) {
                        let trimmed = raw.trim();
                        let (m_prov, m_prof) = trimmed
                            .split_once('/')
                            .unwrap_or((provider.as_str(), trimmed));
                        if m_prov == provider
                            && let Some(canon_prof) = merged_into.get(m_prof)
                        {
                            write_profile_marker(
                                &cfg,
                                marker,
                                &format!("{provider}/{canon_prof}\n"),
                            )
                            .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
                        }
                    }
                }
            }
        }
    }
}

/// Resolves the target profile for an OAuth sign-in staged at `staging_dir` under
/// strict `1 Email / Account = 1 Profile` and `1 Agent = 1 Assigned Profile` rules:
/// 1. If an existing profile under `provider` already holds the same `account_id` or
///    `email`, updates that single profile in-place (never creating a duplicate profile
///    for the same account).
/// 2. If the signed-in account is new to `provider`:
///    - Uses `explicit_profile` if specified and not already occupied by a different account;
///    - Otherwise uses `default` if `default` does not yet hold a different account;
///    - Otherwise creates a new profile named after the account's email handle.
fn has_credential_material(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.iter().any(|(key, value)| {
        (matches!(
            key.as_str(),
            "access_token"
                | "accessToken"
                | "refresh_token"
                | "refreshToken"
                | "api_key"
                | "apiKey"
                | "OPENAI_API_KEY"
                | "key"
                | "token"
        ) && value.as_str().is_some_and(|text| !text.trim().is_empty()))
            || has_credential_material(value)
    })
}

fn validate_staged_credentials(stage: &Path, provider: &str) -> Result<(), Error> {
    let candidates: &[&str] = match provider {
        "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
        "openai" => &["auth.json", ".codex/auth.json"],
        "google-antigravity" => &[".gemini/antigravity-cli/antigravity-oauth-token"],
        "muse" => &["muse/auth.json", ".config/muse/auth.json"],
        "xai" => &[".grok/auth.json"],
        _ => {
            return Err(Error::Message(format!(
                "unsupported sign-in provider: {provider}"
            )));
        }
    };
    let mut found = false;
    for rel in candidates {
        let bytes = match std::fs::read(stage.join(rel)) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if bytes.is_empty() {
            return Err(Error::Message(format!("empty staged credential: {rel}")));
        }
        // AGY also accepts an operator-provided plaintext refresh token. Other
        // providers use JSON credential stores; metadata alone is not a login.
        let plaintext_agy_token = provider == "google-antigravity"
            && std::str::from_utf8(&bytes).is_ok_and(|text| {
                let token = text.trim();
                !token.is_empty()
                    && !token.contains(char::is_whitespace)
                    && !token.starts_with(['{', '[', '"'])
            });
        if !plaintext_agy_token {
            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|err| Error::Message(format!("invalid staged credential {rel}: {err}")))?;
            if !has_credential_material(&value) {
                return Err(Error::Message(format!(
                    "no usable credential material in staged file: {rel}"
                )));
            }
        }
        found = true;
    }
    if !found {
        return Err(Error::Message("no staged sign-in credentials".into()));
    }
    Ok(())
}

pub(crate) fn resolve_and_commit_staged_signin(
    data: &Path,
    provider: &str,
    staging_dir: &Path,
    explicit_profile: Option<&str>,
) -> Result<(String, PathBuf, Option<String>), Error> {
    validate_staged_credentials(staging_dir, provider)?;
    deduplicate_same_account_profiles(data);
    if provider == "anthropic" {
        let root_cred = staging_dir.join(".credentials.json");
        let sub_cred = staging_dir.join(".claude/.credentials.json");
        let root_mt = std::fs::metadata(&root_cred)
            .and_then(|m| m.modified())
            .ok();
        let sub_mt = std::fs::metadata(&sub_cred).and_then(|m| m.modified()).ok();
        if sub_mt > root_mt
            && let Ok(bytes) = std::fs::read(&sub_cred)
        {
            write_required_file(&root_cred, &bytes, 0o600)?;
        } else if root_mt.is_some()
            && let Ok(bytes) = std::fs::read(&root_cred)
        {
            let _ = std::fs::create_dir_all(staging_dir.join(".claude"));
            write_required_file(&sub_cred, &bytes, 0o600)?;
        }
        let (claude_email, claude_uuid) = extract_profile_account_identity(staging_dir, provider);
        if let Ok(cred_text) = std::fs::read_to_string(&root_cred)
            && let Ok(mut cred) = serde_json::from_str::<serde_json::Value>(&cred_text)
        {
            if let Some(oauth) = cred
                .get_mut("claudeAiOauth")
                .and_then(serde_json::Value::as_object_mut)
            {
                if let Some(ref em) = claude_email {
                    oauth.insert("email".to_string(), serde_json::Value::String(em.clone()));
                }
                if let Some(ref u) = claude_uuid {
                    oauth.insert(
                        "accountUuid".to_string(),
                        serde_json::Value::String(u.clone()),
                    );
                }
            }
            if let Some(tok) = cred
                .pointer("/claudeAiOauth/accessToken")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
            {
                let http = ureq::Agent::config_builder()
                    .timeout_global(Some(std::time::Duration::from_secs(6)))
                    .build()
                    .new_agent();
                let _ = sync_anthropic_profile_metadata(
                    &http,
                    data,
                    staging_dir,
                    ".staging",
                    &tok,
                    &mut cred,
                    true,
                );
            }
            if let Ok(updated) = serde_json::to_string(&cred) {
                let _ = std::fs::create_dir_all(staging_dir.join(".claude"));
                write_required_file(&root_cred, updated.as_bytes(), 0o600)?;
                write_required_file(&sub_cred, updated.as_bytes(), 0o600)?;
            }
        }
    } else if provider == "google-antigravity"
        && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none()
    {
        let tok_path = staging_dir.join(".gemini/antigravity-cli/antigravity-oauth-token");
        if !tok_path.is_file() {
            let _ = std::fs::remove_dir_all(staging_dir);
            return Err(Error::Message(
                "agy sign-in did not complete: no OAuth token was written".into(),
            ));
        }
        if let Ok(text) = std::fs::read_to_string(&tok_path)
            && let Ok(mut val) = serde_json::from_str::<serde_json::Value>(&text)
        {
            let has_email = val
                .get("email")
                .or_else(|| val.get("id_token_email"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| !s.trim().is_empty());
            if !has_email
                && let Some(access_tok) = val
                    .pointer("/token/access_token")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
            {
                let http = ureq::Agent::config_builder()
                    .timeout_global(Some(std::time::Duration::from_secs(6)))
                    .build()
                    .new_agent();
                if let Ok(mut resp) = http
                    .get("https://www.googleapis.com/oauth2/v2/userinfo")
                    .header("Authorization", &format!("Bearer {access_tok}"))
                    .call()
                    && let Ok(body) = resp.body_mut().read_to_string()
                    && let Ok(info) = serde_json::from_str::<serde_json::Value>(&body)
                    && let Some(obj) = val.as_object_mut()
                {
                    if let Some(em) = info
                        .get("email")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        obj.insert(
                            "id_token_email".to_string(),
                            serde_json::Value::String(em.to_string()),
                        );
                        obj.insert(
                            "email".to_string(),
                            serde_json::Value::String(em.to_string()),
                        );
                    }
                    if let Some(id) = info
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        obj.insert(
                            "account_id".to_string(),
                            serde_json::Value::String(id.to_string()),
                        );
                    }
                    if let Ok(updated) = serde_json::to_string(&val) {
                        let _ = std::fs::write(&tok_path, updated);
                    }
                }
            }
        }
    }

    let (staged_email, staged_id) = extract_profile_account_identity(staging_dir, provider);
    let existing = list_harness_profiles(data, provider);
    struct ExistingProf {
        name: String,
        email: Option<String>,
        acct_id: Option<String>,
        has_creds: bool,
    }
    let mut profs = Vec::new();
    for (name, dir) in &existing {
        let (em, aid) = extract_profile_account_identity(dir, provider);
        let has_creds = em.is_some()
            || aid.is_some()
            || dir.join(".credentials.json").is_file()
            || dir.join("auth.json").is_file()
            || dir
                .join(".gemini/antigravity-cli/antigravity-oauth-token")
                .is_file();
        profs.push(ExistingProf {
            name: name.clone(),
            email: em,
            acct_id: aid,
            has_creds,
        });
    }

    let matches_staged = |p: &ExistingProf| -> bool {
        if let (Some(s_id), Some(p_id)) = (staged_id.as_deref(), p.acct_id.as_deref())
            && s_id == p_id
        {
            return true;
        }
        if let (Some(s_em), Some(p_em)) = (staged_email.as_deref(), p.email.as_deref())
            && s_em.eq_ignore_ascii_case(p_em)
        {
            return true;
        }
        false
    };

    let chosen_profile = if let Some(existing_match) = profs.iter().find(|p| matches_staged(p)) {
        // 1 Email / Account = 1 Profile: if this account already exists under `provider`,
        // always update its existing profile rather than creating a second profile with the same email.
        existing_match.name.clone()
    } else if let Some(exp) = explicit_profile.map(str::trim).filter(|s| !s.is_empty()) {
        exp.to_string()
    } else if profs.is_empty()
        || profs
            .iter()
            .find(|p| p.name == "default")
            .is_none_or(|p| !p.has_creds)
    {
        "default".to_string()
    } else if let Some(empty) = profs.iter().find(|p| !p.has_creds) {
        empty.name.clone()
    } else {
        let base_slug = staged_email
            .as_deref()
            .map(sanitize_profile_slug)
            .or_else(|| staged_id.as_deref().map(sanitize_profile_slug))
            .unwrap_or_else(|| "acct".to_string());
        let mut candidate = base_slug.clone();
        let mut seq = 2usize;
        while profs.iter().any(|p| p.name == candidate) {
            candidate = format!("{base_slug}-{seq}");
            seq += 1;
        }
        candidate
    };

    let final_dir = data.join("profiles").join(provider).join(&chosen_profile);
    if staging_dir != final_dir {
        copy_dir_recursive_overwrite(staging_dir, &final_dir)?;
        std::fs::remove_dir_all(staging_dir)?;
    }
    finalize_profile_signin(data, provider, &chosen_profile)?;
    deduplicate_same_account_profiles(data);
    let (final_email, final_id) = extract_profile_account_identity(&final_dir, provider);
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none()
        && data.join("state.db").is_file()
        && let Ok(store) = Store::open(data.join("state.db"))
    {
        let _ = sync_and_list_provider_profiles_with_options(&store, data, true);
    }
    Ok((chosen_profile, final_dir, final_email.or(final_id)))
}

pub(crate) fn finalize_profile_signin(
    data: &Path,
    provider: &str,
    profile: &str,
) -> Result<(), Error> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    ensure_provider_profile_meta(&prof_dir, provider, None);
    let _ = std::fs::remove_file(prof_dir.join(".anyagents-oauth-refresh-attempt-ms"));
    let _ = std::fs::remove_file(prof_dir.join(".anyagents-profile-meta-sync-ms"));
    let _ = std::fs::remove_file(prof_dir.join(".anyagents-usage-backoff-until-ms"));
    let _ = std::fs::remove_file(prof_dir.join(".anyagents-quota-reset-ms"));
    let (pre_email, pre_id) = extract_profile_account_identity(&prof_dir, provider);
    update_profile_json_identity(&prof_dir, provider, pre_email.as_deref(), pre_id.as_deref());
    match provider {
        "anthropic" => {
            let root_cred = prof_dir.join(".credentials.json");
            let sub_cred = prof_dir.join(".claude/.credentials.json");
            let root_mt = std::fs::metadata(&root_cred)
                .and_then(|m| m.modified())
                .ok();
            let sub_mt = std::fs::metadata(&sub_cred).and_then(|m| m.modified()).ok();
            if sub_mt > root_mt
                && let Ok(bytes) = std::fs::read(&sub_cred)
            {
                write_required_file(&root_cred, &bytes, 0o600)?;
            } else if root_mt.is_some()
                && let Ok(bytes) = std::fs::read(&root_cred)
            {
                let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
                write_required_file(&sub_cred, &bytes, 0o600)?;
            }
            if let Ok(cred_text) = std::fs::read_to_string(&root_cred)
                && let Ok(mut cred) = serde_json::from_str::<serde_json::Value>(&cred_text)
            {
                if let Some(oauth) = cred
                    .get_mut("claudeAiOauth")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    if let Some(ref em) = pre_email {
                        oauth.insert("email".to_string(), serde_json::Value::String(em.clone()));
                    }
                    if let Some(ref u) = pre_id {
                        oauth.insert(
                            "accountUuid".to_string(),
                            serde_json::Value::String(u.clone()),
                        );
                    }
                }
                if let Ok(updated) = serde_json::to_string(&cred) {
                    let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
                    write_required_file(&root_cred, updated.as_bytes(), 0o600)?;
                    write_required_file(&sub_cred, updated.as_bytes(), 0o600)?;
                }
                if let Some(tok) = cred
                    .pointer("/claudeAiOauth/accessToken")
                    .and_then(serde_json::Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                {
                    let http = ureq::Agent::config_builder()
                        .timeout_global(Some(std::time::Duration::from_secs(6)))
                        .build()
                        .new_agent();
                    let _ = sync_anthropic_profile_metadata(
                        &http, data, &prof_dir, profile, &tok, &mut cred, true,
                    );
                }
            }
            propagate_profile_files_to_bound_agents(
                data,
                "anthropic",
                profile,
                &[".credentials.json", ".claude/.credentials.json"],
            )?;
        }
        "openai" => {
            let root_auth = prof_dir.join("auth.json");
            let sub_auth = prof_dir.join(".codex/auth.json");
            let root_mt = std::fs::metadata(&root_auth)
                .and_then(|m| m.modified())
                .ok();
            let sub_mt = std::fs::metadata(&sub_auth).and_then(|m| m.modified()).ok();
            if sub_mt > root_mt
                && let Ok(bytes) = std::fs::read(&sub_auth)
            {
                write_required_file(&root_auth, &bytes, 0o600)?;
            } else if root_mt.is_some()
                && let Ok(bytes) = std::fs::read(&root_auth)
            {
                let _ = std::fs::create_dir_all(prof_dir.join(".codex"));
                write_required_file(&sub_auth, &bytes, 0o600)?;
            }
            propagate_profile_files_to_bound_agents(
                data,
                "openai",
                profile,
                &["auth.json", ".codex/auth.json"],
            )?;
        }
        "google-antigravity" => {
            propagate_profile_files_to_bound_agents(
                data,
                "google-antigravity",
                profile,
                &[
                    ".gemini/antigravity-cli/antigravity-oauth-token",
                    ".gemini/antigravity-cli/antigravity_state.pbtxt",
                    ".gemini/config/config.json",
                ],
            )?;
        }
        "muse" => {
            propagate_profile_files_to_bound_agents(
                data,
                "muse",
                profile,
                &["muse/auth.json", ".config/muse/auth.json"],
            )?;
        }
        "xai" => {
            propagate_profile_files_to_bound_agents(data, "xai", profile, &[".grok/auth.json"])?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn finalize_agent_signin(data: &Path, agent: &Agent) -> Result<(), Error> {
    let cfg = agent_config_dir(data, &agent.id);
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let stage = data.join("profiles").join(&provider).join(format!(
        ".signin-staging-agent-{}-{}",
        agent.id.as_str(),
        anyagents_core::now_ms()
    ));
    std::fs::create_dir_all(&stage)?;
    let rels: &[&str] = match provider.as_str() {
        "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
        "openai" => &["auth.json", ".codex/auth.json"],
        "google-antigravity" => &[
            ".gemini/antigravity-cli/antigravity-oauth-token",
            ".gemini/antigravity-cli/antigravity_state.pbtxt",
            ".gemini/config/config.json",
        ],
        "muse" => &["muse/auth.json", ".config/muse/auth.json"],
        "xai" => &[".grok/auth.json"],
        _ => &[],
    };
    let mut best_src: Option<(std::time::SystemTime, Vec<u8>)> = None;
    for rel in rels {
        let src = cfg.join(rel);
        if std::fs::read(&src).is_err() {
            chmod_seat_file_readable(&src);
        }
        if let Ok(meta) = std::fs::metadata(&src)
            && meta.is_file()
            && meta.len() > 0
            && let Ok(bytes) = std::fs::read(&src)
        {
            let mt = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
            if matches!(provider.as_str(), "anthropic" | "openai") {
                if best_src.as_ref().is_none_or(|(bmt, _)| mt > *bmt) {
                    best_src = Some((mt, bytes));
                }
            } else {
                let dst = stage.join(rel);
                if let Some(p) = dst.parent() {
                    std::fs::create_dir_all(p)?;
                }
                write_required_file(&dst, &bytes, 0o600)?;
            }
        }
    }
    if let Some((_, bytes)) = best_src {
        for rel in rels {
            let dst = stage.join(rel);
            if let Some(p) = dst.parent() {
                std::fs::create_dir_all(p)?;
            }
            write_required_file(&dst, &bytes, 0o600)?;
        }
    }
    if !rels.iter().any(|rel| stage.join(rel).is_file()) {
        return Err(Error::Message("no staged sign-in credentials".into()));
    }
    let (resolved_prof, _, _) = resolve_and_commit_staged_signin(data, &provider, &stage, None)?;
    let normalized = format!("{provider}/{resolved_prof}\n");
    write_profile_marker(&cfg, ".anyagents-profile", &normalized)?;
    write_profile_marker(&cfg, ".anyagents-seeded-profile", &normalized)?;
    finalize_profile_signin(data, &provider, &resolved_prof)?;
    Ok(())
}

/// Resolves the upstream provider an agent draws its credential profile from.
///
/// An agent's `model` prefix (`sylphx/...`, `openrouter/...`, `qwen-token-plan/...`,
/// `kimi-code-plan/...`, `glm-code-plan/...`, `minimax-code-plan/...`) takes
/// precedence because any harness (`opencode`, `omp`, `claude-code`, `codex`)
/// can run against that upstream provider; otherwise defaults to the native
/// subscription provider of the agent's harness.
pub(crate) fn upstream_provider_of(harness: &str, model: Option<&str>) -> String {
    if let Some(m) = model.map(str::trim).filter(|s| !s.is_empty())
        && let Some((prefix, _)) = m.split_once('/')
    {
        match prefix {
            "sylphx" | "openrouter" | "qwen-token-plan" | "kimi-code-plan" | "glm-code-plan"
            | "minimax-code-plan" | "anthropic" | "openai" | "xai" | "deepseek" => {
                return prefix.to_string();
            }
            _ => {}
        }
    }
    match normalize_harness_name(harness) {
        "claude-code" => "anthropic".to_string(),
        "codex" => "openai".to_string(),
        "agy" => "google-antigravity".to_string(),
        "muse" => "muse".to_string(),
        "dsh" => "deepseek".to_string(),
        "grok" => "xai".to_string(),
        "spiron" | "opencode" | "omp" => "sylphx".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn default_kind_and_harnesses(provider: &str) -> (&'static str, Vec<String>) {
    let harnesses = |list: &[&str]| list.iter().map(|s| (*s).to_string()).collect();
    match provider {
        "sylphx" => (
            "api-key",
            harnesses(&[
                "spiron",
                "opencode",
                "omp",
                "claude-code",
                "codex",
                "muse",
                "grok",
                "dsh",
            ]),
        ),
        "openrouter" | "opengateway" | "google-gemini" => (
            "api-key",
            harnesses(&[
                "opencode",
                "omp",
                "claude-code",
                "codex",
                "muse",
                "grok",
                "dsh",
            ]),
        ),
        "qwen-token-plan" | "kimi-code-plan" | "glm-code-plan" | "minimax-code-plan" => (
            "coding-plan",
            harnesses(&["spiron", "opencode", "omp", "claude-code", "codex", "dsh"]),
        ),
        "anthropic" => (
            "subscription",
            harnesses(&["claude-code", "opencode", "omp", "muse"]),
        ),
        "openai" => (
            "subscription",
            harnesses(&["codex", "opencode", "omp", "muse"]),
        ),
        "google-antigravity" => ("subscription", harnesses(&["agy", "opencode", "omp"])),
        "muse" => ("subscription", harnesses(&["muse", "omp"])),
        "deepseek" => ("subscription", harnesses(&["dsh", "opencode", "omp"])),
        "xai" => ("subscription", harnesses(&["grok", "opencode", "omp"])),
        "cursor" => ("subscription", harnesses(&["omp", "opencode"])),
        _ => (
            "api-key",
            harnesses(&["opencode", "omp", "claude-code", "codex"]),
        ),
    }
}

fn copy_host_file(src: &Path, dst: &Path) -> bool {
    if !src.is_file() {
        return false;
    }
    if dst.is_file() {
        return true;
    }
    if let Some(p) = dst.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    std::fs::copy(src, dst).is_ok()
}

fn json_oauth_expires_at(bytes: &[u8]) -> Option<i64> {
    let val: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let tok = val
        .pointer("/claudeAiOauth/accessToken")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    if tok.is_empty() {
        return None;
    }
    val.pointer("/claudeAiOauth/expiresAt")
        .and_then(serde_json::Value::as_i64)
}

fn json_oauth_has_refresh_token(bytes: &[u8]) -> bool {
    let Ok(val) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    val.pointer("/claudeAiOauth/refreshToken")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| !s.trim().is_empty())
}

fn json_oauth_identity(bytes: &[u8]) -> Option<String> {
    let val: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    for ptr in [
        "/claudeAiOauth/accountUuid",
        "/claudeAiOauth/email",
        "/id_token_email",
        "/email",
        "/account_id",
        "/tokens/account_id",
        "/token/refresh_token",
    ] {
        if let Some(s) = val
            .pointer(ptr)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(s.to_string());
        }
    }
    None
}

fn merge_preserved_oauth_metadata(src_bytes: &[u8], dst_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut src_val: serde_json::Value = serde_json::from_slice(src_bytes).ok()?;
    let dst_val: serde_json::Value = serde_json::from_slice(dst_bytes).ok()?;
    let mut changed = false;
    if let (Some(dst_oauth), Some(src_oauth)) = (
        dst_val
            .get("claudeAiOauth")
            .and_then(serde_json::Value::as_object),
        src_val
            .get_mut("claudeAiOauth")
            .and_then(serde_json::Value::as_object_mut),
    ) {
        for key in ["accountUuid", "email"] {
            if !src_oauth.contains_key(key)
                && let Some(v) = dst_oauth.get(key)
                && v.as_str().is_some_and(|s| !s.trim().is_empty())
            {
                src_oauth.insert(key.to_string(), v.clone());
                changed = true;
            }
        }
    }
    if let (Some(dst_obj), Some(src_obj)) = (dst_val.as_object(), src_val.as_object_mut()) {
        for key in ["id_token_email", "email", "account_id"] {
            if !src_obj.contains_key(key)
                && let Some(v) = dst_obj.get(key)
                && v.as_str().is_some_and(|s| !s.trim().is_empty())
            {
                src_obj.insert(key.to_string(), v.clone());
                changed = true;
            }
        }
    }
    changed.then(|| serde_json::to_vec(&src_val).ok()).flatten()
}

fn copy_seat_file_if_newer(src: &Path, dst: &Path) -> bool {
    let Ok(src_meta) = std::fs::metadata(src) else {
        return false;
    };
    if !src_meta.is_file() || src_meta.len() == 0 {
        return false;
    }
    if std::fs::read(src).is_err() {
        chmod_seat_file_readable(src);
    }
    let Ok(mut src_bytes) = std::fs::read(src) else {
        return false;
    };
    if src_bytes.is_empty() {
        return false;
    }
    if src.file_name().is_some_and(|n| n == ".credentials.json")
        && (json_oauth_expires_at(&src_bytes).is_none()
            || !json_oauth_has_refresh_token(&src_bytes))
    {
        return false;
    }
    if src
        .file_name()
        .is_some_and(|n| n == "antigravity-oauth-token")
    {
        let valid_agy_tok = serde_json::from_slice::<serde_json::Value>(&src_bytes)
            .ok()
            .is_some_and(|v| {
                v.pointer("/token/refresh_token")
                    .or_else(|| v.get("refresh_token"))
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|s| !s.trim().is_empty())
            });
        if !valid_agy_tok {
            return false;
        }
    }
    if let Ok(dst_meta) = std::fs::metadata(dst)
        && dst_meta.is_file()
        && dst_meta.len() > 0
    {
        if let Ok(dst_bytes) = std::fs::read(dst) {
            if src_bytes == dst_bytes {
                return false;
            }
            if let (Some(src_id), Some(dst_id)) = (
                json_oauth_identity(&src_bytes),
                json_oauth_identity(&dst_bytes),
            ) && src_id != dst_id
            {
                return false;
            }
            if let (Some(src_exp), Some(dst_exp)) = (
                json_oauth_expires_at(&src_bytes),
                json_oauth_expires_at(&dst_bytes),
            ) {
                if src_exp <= dst_exp {
                    return false;
                }
            } else if src_meta.modified().ok() <= dst_meta.modified().ok() {
                return false;
            }
            if let Some(merged) = merge_preserved_oauth_metadata(&src_bytes, &dst_bytes) {
                src_bytes = merged;
            }
        } else if src_meta.modified().ok() <= dst_meta.modified().ok() {
            return false;
        }
    }
    if let Some(p) = dst.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    if std::fs::write(dst, &src_bytes).is_err() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(dst, std::fs::Permissions::from_mode(0o600));
    }
    true
}

fn ensure_provider_profile_meta(dir: &Path, provider: &str, kind_override: Option<&str>) {
    let _ = std::fs::create_dir_all(dir);
    let meta_path = dir.join("profile.json");
    if !meta_path.exists() {
        let (default_kind, harnesses) = default_kind_and_harnesses(provider);
        let kind = kind_override.unwrap_or(default_kind);
        let meta = serde_json::json!({
            "provider": provider,
            "kind": kind,
            "compatible_harnesses": harnesses,
        });
        if let Ok(text) = serde_json::to_string_pretty(&meta) {
            let _ = std::fs::write(&meta_path, text);
        }
    }
}

/// Discovers and imports existing upstream provider credentials from active seats
/// and host config into `<data>/profiles/<provider>/<profile>`, and returns all
/// upstream provider profiles sorted by provider and profile name.
pub(crate) fn sync_and_list_provider_profiles(
    store: &Store,
    data: &Path,
) -> Vec<ProviderProfileEntry> {
    sync_and_list_provider_profiles_inner(store, data, true, true)
}

pub(crate) fn list_provider_profiles_fast(store: &Store, data: &Path) -> Vec<ProviderProfileEntry> {
    sync_and_list_provider_profiles_inner(store, data, true, false)
}

pub(crate) fn sync_and_list_provider_profiles_with_options(
    store: &Store,
    data: &Path,
    probe_usage: bool,
) -> Vec<ProviderProfileEntry> {
    sync_and_list_provider_profiles_inner(store, data, probe_usage, probe_usage)
}

fn sync_and_list_provider_profiles_inner(
    store: &Store,
    data: &Path,
    probe_usage: bool,
    rebalance: bool,
) -> Vec<ProviderProfileEntry> {
    let agents = store.list_agents().unwrap_or_default();
    let profiles_root = data.join("profiles");
    let _ = std::fs::create_dir_all(&profiles_root);

    let now_ms = anyagents_core::now_ms();
    let seed_stamp = profiles_root.join(".last-agent-cred-sync-ms");
    let skip_heavy_seed = !rebalance
        && !crate::sandbox::is_isolated_test_env()
        && std::fs::read_to_string(&seed_stamp)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .is_some_and(|last_ms| now_ms.saturating_sub(last_ms) < 30_000);

    if !skip_heavy_seed {
        // Migrate any legacy harness-named empty/default directories if they match a harness name
        // rather than a provider name and have no `profile.json`.
        for legacy_harness in [
            "claude-code",
            "codex",
            "opencode",
            "agy",
            "omp",
            "dsh",
            "grok",
            "opengateway",
        ] {
            let legacy_dir = profiles_root.join(legacy_harness);
            if legacy_dir.is_dir() && !legacy_dir.join("default").join("profile.json").exists() {
                let _ = std::fs::remove_dir_all(&legacy_dir);
            }
        }

        // Seed upstream provider profiles from existing seats on this server so pre-existing
        // logins (Subscriptions, Coding Plans, API Keys) are registered in the Master Pool.
        for agent in &agents {
            if agent.state == "archived" {
                continue;
            }
            let cfg = agent_config_dir(data, &agent.id);
            let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
            let bound = bound_profile_name(&cfg);
            let seeded = seeded_profile_name(&cfg);
            if seeded.is_some() && bound.is_some() && seeded != bound {
                continue;
            }
            let prof_name = bound
                .and_then(|b| {
                    let p = b.split_once('/').map(|(_, p)| p.to_string()).unwrap_or(b);
                    (!p.is_empty()).then_some(p)
                })
                .unwrap_or_else(|| "default".to_string());
            let profile_dir = profiles_root.join(&provider).join(&prof_name);
            match provider.as_str() {
                "anthropic" => {
                    let rels = [".credentials.json", ".claude/.credentials.json"];
                    for src_rel in rels {
                        let src = cfg.join(src_rel);
                        if src.is_file() {
                            ensure_provider_profile_meta(&profile_dir, "anthropic", None);
                            for dst_rel in rels {
                                let _ = copy_seat_file_if_newer(&src, &profile_dir.join(dst_rel));
                            }
                        }
                    }
                }
                "openai" => {
                    let rels = ["auth.json", ".codex/auth.json"];
                    for src_rel in rels {
                        let src = cfg.join(src_rel);
                        if src.is_file() {
                            ensure_provider_profile_meta(&profile_dir, "openai", None);
                            for dst_rel in rels {
                                let _ = copy_seat_file_if_newer(&src, &profile_dir.join(dst_rel));
                            }
                        }
                    }
                }
                "google-antigravity" => {
                    for rel in [
                        ".gemini/antigravity-cli/antigravity-oauth-token",
                        ".gemini/antigravity-cli/antigravity_state.pbtxt",
                        ".gemini/config/config.json",
                    ] {
                        let src = cfg.join(rel);
                        if src.is_file() {
                            ensure_provider_profile_meta(&profile_dir, "google-antigravity", None);
                            let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                        }
                    }
                }
                "muse" => {
                    for rel in ["muse/auth.json", ".config/muse/auth.json"] {
                        let src = cfg.join(rel);
                        if src.is_file() {
                            ensure_provider_profile_meta(&profile_dir, "muse", None);
                            let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                        }
                    }
                }
                "sylphx" | "qwen-token-plan" | "kimi-code-plan" | "glm-code-plan"
                | "minimax-code-plan" => {
                    let kind = if provider == "sylphx" {
                        "api-key"
                    } else {
                        "coding-plan"
                    };
                    let opencode_rel = ".config/opencode/opencode.json";
                    let src_json = cfg.join(opencode_rel);
                    if src_json.is_file() {
                        ensure_provider_profile_meta(&profile_dir, &provider, Some(kind));
                        let dst_json = profile_dir.join(opencode_rel);
                        if !dst_json.is_file()
                            && let Ok(text) = std::fs::read_to_string(&src_json)
                            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                            && let Some(prov_block) = val.get("provider")
                        {
                            if let Some(p) = dst_json.parent() {
                                let _ = std::fs::create_dir_all(p);
                            }
                            let clean = serde_json::json!({
                                "$schema": "https://opencode.ai/config.json",
                                "provider": prov_block,
                            });
                            if let Ok(clean_bytes) = serde_json::to_vec_pretty(&clean) {
                                let _ = std::fs::write(&dst_json, clean_bytes);
                            }
                        }
                        if let Ok(text) = std::fs::read_to_string(&src_json) {
                            for rel in extract_file_tilde_refs(&text) {
                                let _ = copy_host_file(&cfg.join(&rel), &profile_dir.join(&rel));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        deduplicate_same_account_profiles(data);
    }

    let mut entries: Vec<ProviderProfileEntry> = Vec::new();
    if let Ok(providers) = std::fs::read_dir(&profiles_root) {
        for prov_entry in providers.flatten() {
            let prov_path = prov_entry.path();
            if !prov_path.is_dir() {
                continue;
            }
            let provider = prov_entry.file_name().to_string_lossy().into_owned();
            if provider.starts_with('.') {
                continue;
            }
            let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
                continue;
            };
            for prof_entry in prof_dirs.flatten() {
                let prof_path = prof_entry.path();
                if !prof_path.is_dir() {
                    continue;
                }
                let profile = prof_entry.file_name().to_string_lossy().into_owned();
                if profile.starts_with('.') {
                    continue;
                }
                let (default_kind, default_harnesses) = default_kind_and_harnesses(&provider);
                let mut kind = default_kind.to_string();
                let mut compatible_harnesses = default_harnesses;
                if let Ok(raw) = std::fs::read_to_string(prof_path.join("profile.json"))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
                {
                    if let Some(k) = val.get("kind").and_then(serde_json::Value::as_str) {
                        kind = k.to_string();
                    }
                    if let Some(arr) = val
                        .get("compatible_harnesses")
                        .and_then(serde_json::Value::as_array)
                    {
                        let parsed: Vec<String> = arr
                            .iter()
                            .filter_map(|v| {
                                v.as_str().map(normalize_harness_name).map(str::to_string)
                            })
                            .collect();
                        if !parsed.is_empty() {
                            compatible_harnesses = parsed;
                        }
                    }
                }
                if matches!(
                    provider.as_str(),
                    "sylphx"
                        | "kimi-code-plan"
                        | "qwen-token-plan"
                        | "glm-code-plan"
                        | "minimax-code-plan"
                        | "openrouter"
                ) && !compatible_harnesses.iter().any(|h| h == "spiron")
                {
                    compatible_harnesses.insert(0, "spiron".to_string());
                }
                let mut seen = std::collections::HashSet::new();
                compatible_harnesses.retain(|h| seen.insert(h.clone()));
                let (acct_email, acct_id) = extract_profile_account_identity(&prof_path, &provider);
                entries.push(ProviderProfileEntry {
                    provider: provider.clone(),
                    profile,
                    account: acct_email.or(acct_id),
                    kind,
                    compatible_harnesses,
                    assigned_agents: Vec::new(),
                    subagent_count: 0,
                    usage: None,
                });
            }
        }
    }

    entries.sort_by(|a, b| (&a.provider, &a.profile).cmp(&(&b.provider, &b.profile)));
    if probe_usage {
        enrich_profiles_usage(data, &mut entries);
    }

    if rebalance
        && !crate::sandbox::is_isolated_test_env()
        && let Ok(store) = Store::open(data.join("state.db"))
    {
        rebalance_provider_profiles(&store, data, &agents, &entries);
    }

    // Bind active agents to their resolved `<provider>/<profile>`.
    let mut ordered_agents: Vec<&Agent> = agents.iter().filter(|a| a.state != "archived").collect();
    ordered_agents.sort_by_key(|a| a.parent_id.is_some());
    for agent in ordered_agents {
        let is_root = agent.parent_id.is_none();
        let cfg = agent_config_dir(data, &agent.id);
        let expected_provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
        let bound = bound_profile_name(&cfg);
        let (bound_prov, bound_prof) = match bound.as_deref() {
            Some(s) if s.contains('/') => {
                let (p, n) = s.split_once('/').unwrap_or((&expected_provider, "default"));
                (p.to_string(), n.to_string())
            }
            Some(s) if !s.is_empty() => (expected_provider.clone(), s.to_string()),
            _ => (expected_provider.clone(), "default".to_string()),
        };
        let normalized = format!("{bound_prov}/{bound_prof}");
        if bound.as_deref() != Some(normalized.as_str()) {
            write_profile_marker(&cfg, ".anyagents-profile", &format!("{normalized}\n"))
                .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
        }
        let target_idx = entries
            .iter()
            .position(|e| e.provider == bound_prov && e.profile == bound_prof)
            .or_else(|| entries.iter().position(|e| e.provider == expected_provider));
        if let Some(idx) = target_idx
            && let Some(entry) = entries.get_mut(idx)
        {
            if is_root {
                entry.assigned_agents.push(agent.name.clone());
            } else {
                entry.subagent_count += 1;
            }
        }
    }

    for entry in &mut entries {
        entry.assigned_agents.sort();
    }
    entries
}

/// Unknown, malformed and out-of-range observations carry no quota authority.
fn observed_remaining_pct(value: &serde_json::Value) -> Option<i64> {
    let used = value.as_f64()?;
    (used.is_finite() && (0.0..=100.0).contains(&used)).then(|| (100.0 - used).round() as i64)
}
fn observed_fraction_pct(value: &serde_json::Value) -> Option<i64> {
    let left = value.as_f64()?;
    (left.is_finite() && (0.0..=1.0).contains(&left)).then(|| (left * 100.0).round() as i64)
}
fn observed_window_min(first: Option<i64>, second: Option<i64>) -> Option<i64> {
    Some(first?.min(second?))
}
fn observed_agy_tier(value: &serde_json::Value) -> Option<String> {
    let id = value
        .pointer("/paidTier/id")
        .or_else(|| value.pointer("/currentTier/id"))?
        .as_str()?;
    if id.contains("ultra") {
        Some("ultra".into())
    } else if id.contains("pro") {
        Some("pro".into())
    } else if id.contains("free") {
        Some("free".into())
    } else {
        None
    }
}

fn parse_effective_rebalance_pct(usage: &str) -> Option<i64> {
    let min_pct = parse_remaining_pct_from_usage(usage, None)?;
    if min_pct <= 0 {
        return Some(0);
    }
    let extract_after = |label: &str| -> Option<i64> {
        let idx = usage.find(label)?;
        let rest = &usage[idx + label.len()..];
        let pct_idx = rest.find("% left")?;
        rest[..pct_idx].trim().parse::<i64>().ok()
    };
    if let (Some(p5), Some(p7)) = (extract_after("5h:"), extract_after("7d:"))
        && p5 >= 20
        && p7 >= 10
    {
        return Some((p5 + p7) / 2);
    }
    Some(min_pct)
}

fn rebalance_provider_profiles(
    store: &Store,
    data: &Path,
    agents: &[Agent],
    entries: &[ProviderProfileEntry],
) {
    let mut providers: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for e in entries {
        if e.kind == "subscription" {
            providers.insert(e.provider.clone());
        }
    }
    for provider in providers {
        let mut prof_rem: std::collections::BTreeMap<String, i64> =
            std::collections::BTreeMap::new();
        for e in entries.iter().filter(|e| e.provider == provider) {
            if let Some(ref u) = e.usage {
                if provider == "google-antigravity" && !u.contains("(ultra)") {
                    continue;
                }
                if let Some(pct) = parse_effective_rebalance_pct(u) {
                    prof_rem.insert(e.profile.clone(), pct);
                }
            }
        }
        if prof_rem.len() < 2 {
            continue;
        }
        let mut bound: std::collections::BTreeMap<String, Vec<String>> =
            std::collections::BTreeMap::new();
        for p in prof_rem.keys() {
            bound.insert(p.clone(), Vec::new());
        }
        let mut active_agents: Vec<&Agent> =
            agents.iter().filter(|a| a.state != "archived").collect();
        active_agents.sort_by(|a, b| a.name.cmp(&b.name));
        for a in active_agents {
            let exp_prov = upstream_provider_of(&a.harness, a.model.as_deref());
            if exp_prov != provider {
                continue;
            }
            let cfg = agent_config_dir(data, &a.id);
            let prof_name = bound_profile_name(&cfg)
                .and_then(|b| {
                    let p = b.split_once('/').map(|(_, p)| p.to_string()).unwrap_or(b);
                    (!p.is_empty()).then_some(p)
                })
                .unwrap_or_else(|| "default".to_string());
            bound.entry(prof_name).or_default().push(a.name.clone());
        }
        // Working seats still contribute load, but automatic maintenance must
        // not rewrite their credentials or request a process rotation mid-turn.
        // Read the durable turn state afresh rather than trusting the census.
        let can_move = |name: &str| {
            store
                .agent_by_name(name)
                .is_ok_and(|agent| store.has_open_turn(&agent.id).is_ok_and(|open| !open))
        };
        // Step 1: evacuate exhausted profiles (0% left) onto any profile with remaining quota (> 0% left).
        let prof_keys: Vec<String> = bound.keys().cloned().collect();
        for src_prof in &prof_keys {
            let Some(src_pct) = prof_rem.get(src_prof).copied() else {
                continue;
            };
            if src_pct > 0 {
                continue;
            }
            let to_move = bound.get(src_prof).cloned().unwrap_or_default();
            for agent_name in to_move {
                if !can_move(&agent_name) {
                    continue;
                }
                let mut best_dst: Option<(String, i64, i64)> = None;
                for (cand_prof, &cand_pct) in &prof_rem {
                    if cand_prof == src_prof || cand_pct <= 0 {
                        continue;
                    }
                    let cnt = bound.get(cand_prof).map(|v| v.len() as i64).unwrap_or(0);
                    let score = (cand_pct * 100) / (cnt + 1);
                    if best_dst
                        .as_ref()
                        .is_none_or(|(_, bs, br)| (score, cand_pct) > (*bs, *br))
                    {
                        best_dst = Some((cand_prof.clone(), score, cand_pct));
                    }
                }
                if let Some((dst_prof, _, _)) = best_dst {
                    let target = format!("{provider}/{dst_prof}");
                    if configure_agent_runtime(
                        store,
                        data,
                        "system:profile-rebalance",
                        &agent_name,
                        None,
                        None,
                        Some(&target),
                    )
                    .is_ok()
                    {
                        if let Some(v) = bound.get_mut(src_prof) {
                            v.retain(|n| n != &agent_name);
                        }
                        bound.entry(dst_prof).or_default().push(agent_name);
                    }
                }
            }
        }
        // Step 2: balance overloaded profiles onto healthy profiles so agent counts stay even
        // (differing by at most 1 across active profiles with healthy quota) and higher-quota
        // profiles carry the remainder.
        for _ in 0..16 {
            let mut best_move: Option<(String, String, i64)> = None;
            for (src_prof, src_agents) in &bound {
                let src_len = src_agents.len() as i64;
                if src_len < 2 || !src_agents.iter().any(|name| can_move(name)) {
                    continue;
                }
                let Some(src_pct) = prof_rem.get(src_prof).copied() else {
                    continue;
                };
                let src_share = (src_pct * 100) / src_len;
                for (dst_prof, &dst_pct) in &prof_rem {
                    if dst_prof == src_prof || dst_pct < 25 {
                        continue;
                    }
                    let dst_len = bound.get(dst_prof).map(|v| v.len() as i64).unwrap_or(0);
                    let new_dst_share = (dst_pct * 100) / (dst_len + 1);
                    let share_gain = new_dst_share - src_share;
                    let count_gap = src_len - dst_len;
                    let should_move = if count_gap >= 2 {
                        true
                    } else if count_gap == 1 {
                        dst_pct > src_pct + 15
                    } else {
                        share_gain >= 2500
                    };
                    if should_move {
                        let priority = count_gap * 10_000 + share_gain;
                        if best_move
                            .as_ref()
                            .is_none_or(|(_, _, best_pri)| priority > *best_pri)
                        {
                            best_move = Some((src_prof.clone(), dst_prof.clone(), priority));
                        }
                    }
                }
            }
            let Some((src_prof, dst_prof, _)) = best_move else {
                break;
            };
            let Some(agent_name) = bound
                .get(&src_prof)
                .and_then(|agents| agents.iter().rev().find(|name| can_move(name)).cloned())
            else {
                continue;
            };
            let target = format!("{provider}/{dst_prof}");
            if configure_agent_runtime(
                store,
                data,
                "system:profile-rebalance",
                &agent_name,
                None,
                None,
                Some(&target),
            )
            .is_ok()
            {
                if let Some(v) = bound.get_mut(&src_prof) {
                    v.retain(|n| n != &agent_name);
                }
                bound.entry(dst_prof).or_default().push(agent_name);
            } else {
                break;
            }
        }
    }
}

pub(crate) fn record_profile_quota_reset(data: &Path, agent: &Agent, reset_at_ms: i64) {
    let now_ms = anyagents_core::now_ms();
    if reset_at_ms <= now_ms {
        return;
    }
    let cfg = agent_config_dir(data, &agent.id);
    let rotated_ms = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let cleared_ms = std::fs::read_to_string(cfg.join(".anyagents-quota-cleared-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
        .max(rotated_ms);
    if cleared_ms > 0 && now_ms.saturating_sub(cleared_ms) < 45_000 {
        return;
    }
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let prof_name = bound_profile_name(&cfg)
        .and_then(|b| {
            let p = b.split_once('/').map(|(_, p)| p.to_string()).unwrap_or(b);
            (!p.is_empty()).then_some(p)
        })
        .unwrap_or_else(|| "default".to_string());
    let profile_dir = data.join("profiles").join(&provider).join(&prof_name);
    if profile_dir.is_dir() {
        let marker = profile_dir.join(".anyagents-quota-reset-ms");
        let existing = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0);
        if existing != reset_at_ms {
            let _ = std::fs::write(&marker, format!("{reset_at_ms}\n"));
        }
    }
}

fn parse_reset_eta_secs(eta: &str) -> Option<i64> {
    let mut total = 0i64;
    let mut num = 0i64;
    let mut has_digit = false;
    let mut matched_any = false;
    for ch in eta.trim().chars() {
        if let Some(d) = ch.to_digit(10) {
            num = num.saturating_mul(10).saturating_add(d as i64);
            has_digit = true;
        } else {
            if !has_digit {
                return None;
            }
            let mult = match ch {
                'd' => 86_400,
                'h' => 3_600,
                'm' => 60,
                's' => 1,
                _ => return None,
            };
            total = total.saturating_add(num.saturating_mul(mult));
            num = 0;
            has_digit = false;
            matched_any = true;
        }
    }
    if has_digit || !matched_any {
        return None;
    }
    Some(total)
}

fn advance_cached_usage_etas(cached: &str, elapsed_secs: i64) -> String {
    if elapsed_secs <= 0 || cached.is_empty() {
        return cached.to_string();
    }
    let (body, tier_suffix) = if cached.ends_with(')')
        && let Some(idx) = cached.rfind(" (")
    {
        (&cached[..idx], &cached[idx..])
    } else {
        (cached, "")
    };
    if body.is_empty() {
        return cached.to_string();
    }
    let mut updated_parts = Vec::new();
    for part in body.split(" · ") {
        if let Some((prefix, eta_str)) = part.split_once(" ↻")
            && let Some(orig_secs) = parse_reset_eta_secs(eta_str)
        {
            let rem_secs = orig_secs - elapsed_secs;
            if rem_secs > 0 {
                if let Some(new_eta) = format_reset_eta_secs(rem_secs) {
                    updated_parts.push(format!("{prefix} ↻{new_eta}"));
                } else {
                    updated_parts.push(prefix.to_string());
                }
            } else if let Some((win, rest)) = prefix.split_once(": ")
                && rest.ends_with("% left")
            {
                updated_parts.push(format!("{win}: 100% left"));
            } else {
                updated_parts.push(prefix.to_string());
            }
        } else {
            updated_parts.push(part.to_string());
        }
    }
    format!("{}{tier_suffix}", updated_parts.join(" · "))
}

fn overlay_active_quota_reset_on_cached_usage(
    prof_dir: &Path,
    last_usage_path: &Path,
    cached: &str,
    now_ms: i64,
) -> String {
    let usage_mtime_ms = std::fs::metadata(last_usage_path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let elapsed_secs = if usage_mtime_ms > 0 {
        now_ms.saturating_sub(usage_mtime_ms) / 1_000
    } else {
        0
    };
    let cached_advanced = advance_cached_usage_etas(cached, elapsed_secs);
    let reset_marker = prof_dir.join(".anyagents-quota-reset-ms");
    let raw_reset_ms = std::fs::read_to_string(&reset_marker)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok());
    if raw_reset_ms.is_some_and(|ms| ms <= now_ms) {
        let _ = std::fs::remove_file(&reset_marker);
    }
    let Some(reset_ms) = raw_reset_ms.filter(|&ms| ms > now_ms) else {
        return cached_advanced;
    };
    let reset_mtime_ms = std::fs::metadata(&reset_marker)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(now_ms);
    if usage_mtime_ms > reset_mtime_ms && now_ms.saturating_sub(usage_mtime_ms) < 60_000 {
        return cached_advanced;
    }
    let eta = format_reset_eta_epoch_ms(reset_ms)
        .map(|e| format!(" ↻{e}"))
        .unwrap_or_default();
    let zero_5h = format!("5h: 0% left{eta}");
    if cached_advanced.starts_with("5h:") {
        if let Some((_, rest)) = cached_advanced.split_once(" · ") {
            return format!("{zero_5h} · {rest}");
        }
        let tier_suffix = cached_advanced
            .rfind(" (")
            .map(|idx| &cached_advanced[idx..])
            .unwrap_or_default();
        return format!("{zero_5h}{tier_suffix}");
    }
    if cached_advanced.is_empty() {
        zero_5h
    } else if cached_advanced.starts_with('(') {
        format!("{zero_5h} {cached_advanced}")
    } else {
        format!("{zero_5h} · {cached_advanced}")
    }
}

pub(crate) fn sync_fresher_bound_agent_credentials(data: &Path) {
    let profiles_root = data.join("profiles");
    let now_ms = anyagents_core::now_ms();
    let sync_stamp = profiles_root.join(".last-agent-cred-sync-ms");
    if !crate::sandbox::is_isolated_test_env()
        && let Ok(raw) = std::fs::read_to_string(&sync_stamp)
        && let Ok(last_ms) = raw.trim().parse::<i64>()
        && now_ms.saturating_sub(last_ms) < 15_000
    {
        return;
    }
    let _ = std::fs::write(&sync_stamp, format!("{now_ms}\n"));
    if let Ok(entries) = std::fs::read_dir(profiles_root.join("google-antigravity")) {
        for entry in entries.flatten() {
            let prof_path = entry.path();
            if prof_path.is_dir() {
                let _ = heal_mcp_config_file(&prof_path.join(".gemini/config/mcp_config.json"));
                let _ = heal_mcp_config_file(&prof_path.join("mcp_config.json"));
            }
        }
    }
    if let Ok(store) = Store::open(data.join("state.db"))
        && let Ok(agents) = store.list_agents()
    {
        for agent in &agents {
            if agent.state == "archived" {
                continue;
            }
            let cfg = agent_config_dir(data, &agent.id);
            if agent.harness == "claude-code"
                && let Some(reset_ms) =
                    anyagents_harness_claude::detect_session_quota_reset_ms(&cfg, now_ms)
            {
                record_profile_quota_reset(data, agent, reset_ms);
            }
            let bound = bound_profile_name(&cfg);
            let seeded = seeded_profile_name(&cfg);
            if seeded.is_some() && bound.is_some() && seeded != bound {
                continue;
            }
            let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
            let prof_name = bound
                .and_then(|b| {
                    let p = b.split_once('/').map(|(_, p)| p.to_string()).unwrap_or(b);
                    (!p.is_empty()).then_some(p)
                })
                .unwrap_or_else(|| "default".to_string());
            let profile_dir = profiles_root.join(&provider).join(&prof_name);
            ensure_agy_mcp_config(std::slice::from_ref(&profile_dir), &cfg);
            let rels: &[&str] = match provider.as_str() {
                "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
                "openai" => &["auth.json", ".codex/auth.json"],
                "google-antigravity" => &[".gemini/antigravity-cli/antigravity-oauth-token"],
                _ => &[],
            };
            let mut updated_any = false;
            for src_rel in rels {
                let src = cfg.join(src_rel);
                if src.is_file() {
                    for dst_rel in rels {
                        if copy_seat_file_if_newer(&src, &profile_dir.join(dst_rel)) {
                            updated_any = true;
                        }
                    }
                }
            }
            if updated_any {
                propagate_profile_files_to_bound_agents(data, &provider, &prof_name, rels)
                    .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
            } else if provider == "anthropic" {
                sync_anthropic_agent_credentials_from_profile(
                    Some(&store),
                    data,
                    agent,
                    &cfg,
                    &profile_dir,
                );
            }
        }
    }
}

/// Syncs any fresher OAuth credentials from active seats back into
/// `<data>/profiles/<provider>/<profile>` and probes native upstream usage,
/// quota, or balance for each profile in parallel.
pub(crate) fn enrich_profiles_usage(data: &Path, entries: &mut [ProviderProfileEntry]) {
    if crate::sandbox::is_isolated_test_env() || entries.is_empty() {
        return;
    }
    sync_fresher_bound_agent_credentials(data);

    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for (idx, entry) in entries.iter().enumerate() {
            if entry.usage.is_some() {
                continue;
            }
            let provider = entry.provider.clone();
            let profile = entry.profile.clone();
            handles
                .push(s.spawn(move || (idx, probe_one_profile_usage(data, &provider, &profile))));
        }
        for handle in handles {
            if let Ok((idx, Some(usage))) = handle.join()
                && let Some(entry) = entries.get_mut(idx)
            {
                entry.usage = Some(usage);
            }
        }
    });
    for entry in entries.iter_mut() {
        if entry.account.is_none() {
            let prof_dir = data
                .join("profiles")
                .join(&entry.provider)
                .join(&entry.profile);
            let (em, id) = extract_profile_account_identity(&prof_dir, &entry.provider);
            entry.account = em.or(id);
        }
    }
}

fn read_first_non_empty_file(base: &Path, rels: &[&str]) -> Option<String> {
    for rel in rels {
        if let Ok(text) = std::fs::read_to_string(base.join(rel)) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn read_dsh_yaml_ref(base: &Path, key_name: &str) -> Option<String> {
    let text = std::fs::read_to_string(base.join(".dsh/.credentials.yaml")).ok()?;
    let prefix = format!("{key_name}:");
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix(&prefix) {
            let val = rest.trim().trim_matches('"').trim_matches('\'');
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

fn extract_agy_oauth_clients() -> Vec<(String, String)> {
    static AGY_CLIENTS: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    AGY_CLIENTS
        .get_or_init(|| {
            let mut candidates = Vec::new();
            let cache_path = crate::sandbox::host_home_dir()
                .map(|h| h.join(".cache/anyagents-agy-oauth-clients.json"));
            if let Some(home) = crate::sandbox::host_home_dir() {
                candidates.push(home.join(".local/bin/agy"));
            }
            candidates.push(PathBuf::from("/usr/local/bin/agy"));
            let Some(agy_bin) = candidates.into_iter().find(|p| p.is_file()) else {
                return Vec::new();
            };
            let bin_meta = std::fs::metadata(&agy_bin).ok();
            let bin_size = bin_meta.as_ref().map(|m| m.len()).unwrap_or(0);
            if let Some(ref cp) = cache_path
                && let Ok(raw) = std::fs::read_to_string(cp)
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
                && val.get("bin_size").and_then(|v| v.as_u64()) == Some(bin_size)
                && let Some(arr) = val.get("clients").and_then(|v| v.as_array())
            {
                let parsed: Vec<(String, String)> = arr
                    .iter()
                    .filter_map(|item| {
                        Some((
                            item.get(0)?.as_str()?.to_string(),
                            item.get(1)?.as_str()?.to_string(),
                        ))
                    })
                    .collect();
                if !parsed.is_empty() {
                    return parsed;
                }
            }
            let Ok(bytes) = std::fs::read(&agy_bin) else {
                return Vec::new();
            };
            let cid_needle =
                b"1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com";
            if !bytes.windows(cid_needle.len()).any(|w| w == cid_needle) {
                return Vec::new();
            }
            let Ok(cid) = String::from_utf8(cid_needle.to_vec()) else {
                return Vec::new();
            };
            let sec_prefix = b"GOCSPX-";
            let mut out = Vec::new();
            for idx in 0..bytes.len().saturating_sub(35) {
                if &bytes[idx..idx + 7] == sec_prefix {
                    let candidate = &bytes[idx..idx + 35];
                    if candidate[7..]
                        .iter()
                        .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_')
                        && let Ok(secret) = String::from_utf8(candidate.to_vec())
                        && !out.iter().any(|(_, s)| s == &secret)
                    {
                        out.push((cid.clone(), secret));
                    }
                }
            }
            if let Some(ref cp) = cache_path {
                if let Some(parent) = cp.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let cache_obj = serde_json::json!({
                    "bin_size": bin_size,
                    "clients": out,
                });
                let _ = std::fs::write(cp, cache_obj.to_string());
            }
            out
        })
        .clone()
}

fn extract_cursor_oauth_from_db(prof_dir: &Path) -> Option<(String, String)> {
    let db_path = ["agent.db", ".omp/agent/agent.db"]
        .into_iter()
        .map(|r| prof_dir.join(r))
        .find(|p| p.is_file())?;
    if let Ok(conn) =
        rusqlite::Connection::open_with_flags(&db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        && let Ok(data_str) = conn.query_row(
            "SELECT data FROM auth_credentials WHERE provider = 'cursor' ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&data_str)
        && let Some(access) = val
            .get("access")
            .and_then(serde_json::Value::as_str)
            .map(|s| s.trim().to_string())
        && !access.is_empty()
    {
        let refresh = val
            .get("refresh")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        return Some((access, refresh));
    }
    let bytes = std::fs::read(db_path).ok()?;
    for needle in [
        &b"{\"access\":\"eyJ"[..],
        &b"{\"type\":\"oauth\",\"refresh\":\""[..],
    ] {
        for idx in (0..bytes.len().saturating_sub(needle.len())).rev() {
            if &bytes[idx..idx + needle.len()] == needle {
                let slice = &bytes[idx..bytes.len().min(idx + 2048)];
                let Some(end) = slice.iter().position(|b| *b == b'}') else {
                    continue;
                };
                let Ok(json_str) = std::str::from_utf8(&slice[..=end]) else {
                    continue;
                };
                let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) else {
                    continue;
                };
                let Some(access) = val
                    .get("access")
                    .and_then(serde_json::Value::as_str)
                    .map(|s| s.trim().to_string())
                else {
                    continue;
                };
                let refresh = val
                    .get("refresh")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if access.starts_with("eyJ") {
                    return Some((access, refresh));
                }
            }
        }
    }
    None
}

fn decode_b64url(input: &str) -> Option<Vec<u8>> {
    let val_of = |b: u8| -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    };
    let clean: Vec<u8> = input
        .bytes()
        .filter(|b| *b != b'=' && !b.is_ascii_whitespace())
        .collect();
    let mut out = Vec::with_capacity(clean.len() * 3 / 4);
    for chunk in clean.chunks(4) {
        let b0 = val_of(*chunk.first()?)?;
        let b1 = val_of(*chunk.get(1)?)?;
        out.push((b0 << 2) | (b1 >> 4));
        if let Some(&c2) = chunk.get(2) {
            let b2 = val_of(c2)?;
            out.push((b1 << 4) | (b2 >> 2));
            if let Some(&c3) = chunk.get(3) {
                let b3 = val_of(c3)?;
                out.push((b2 << 6) | b3);
            }
        }
    }
    Some(out)
}

fn extract_jwt_sub_tail(jwt: &str) -> Option<String> {
    let payload_b64 = jwt.split('.').nth(1)?;
    let decoded = decode_b64url(payload_b64)?;
    let val: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    let sub = val.get("sub")?.as_str()?;
    Some(sub.split('|').next_back().unwrap_or(sub).to_string())
}

fn anthropic_tier_label(sub_type: Option<&str>, rate_tier: Option<&str>) -> Option<&'static str> {
    match rate_tier.unwrap_or("") {
        "default_claude_max_20x" => return Some("max 20x"),
        "default_claude_max_5x" => return Some("max 5x"),
        _ => {}
    }
    match sub_type.unwrap_or("") {
        "max" | "claude_max" => Some("max"),
        "pro" | "claude_pro" => Some("pro"),
        "team" | "claude_team" => Some("team"),
        "enterprise" | "claude_enterprise" => Some("enterprise"),
        _ => None,
    }
}

fn sync_anthropic_profile_metadata(
    http: &ureq::Agent,
    data: &Path,
    prof_dir: &Path,
    profile: &str,
    token: &str,
    cred: &mut serde_json::Value,
    force: bool,
) -> Option<&'static str> {
    let existing_sub = cred
        .pointer("/claudeAiOauth/subscriptionType")
        .and_then(serde_json::Value::as_str);
    let existing_tier = cred
        .pointer("/claudeAiOauth/rateLimitTier")
        .and_then(serde_json::Value::as_str);
    let existing_email = cred
        .pointer("/claudeAiOauth/email")
        .and_then(serde_json::Value::as_str);
    let now_ms = anyagents_core::now_ms();
    let meta_sync_path = prof_dir.join(".anyagents-profile-meta-sync-ms");
    let last_sync_ms = std::fs::read_to_string(&meta_sync_path)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let mut changed = false;
    if (force
        || existing_sub.is_none()
        || existing_tier.is_none()
        || existing_email.is_none()
        || now_ms.saturating_sub(last_sync_ms) >= 300_000)
        && let Ok(resp) = http
            .get("https://api.anthropic.com/api/oauth/profile")
            .header("authorization", &format!("Bearer {token}"))
            .header("anthropic-beta", "oauth-2025-04-20")
            .header("cache-control", "no-cache")
            .header("user-agent", "claude-code/2.1.310")
            .call()
        && let Ok(body) = resp.into_body().read_to_string()
        && let Ok(pval) = serde_json::from_str::<serde_json::Value>(&body)
    {
        let _ = std::fs::write(&meta_sync_path, format!("{now_ms}\n"));
        let org_type = pval
            .pointer("/organization/organization_type")
            .and_then(serde_json::Value::as_str);
        let mapped_sub = match org_type {
            Some("claude_max") => Some("max"),
            Some("claude_pro") => Some("pro"),
            Some("claude_team") => Some("team"),
            Some("claude_enterprise") => Some("enterprise"),
            _ => None,
        };
        let live_tier = pval
            .pointer("/organization/rate_limit_tier")
            .and_then(serde_json::Value::as_str);
        let acct_uuid = pval
            .pointer("/account/uuid")
            .and_then(serde_json::Value::as_str);
        let acct_email = pval
            .pointer("/account/email")
            .and_then(serde_json::Value::as_str);
        if !profile.starts_with('.') {
            update_profile_json_identity(prof_dir, "anthropic", acct_email, acct_uuid);
        }
        if let Some(oauth) = cred
            .get_mut("claudeAiOauth")
            .and_then(serde_json::Value::as_object_mut)
        {
            if let Some(em) = acct_email
                && oauth.get("email").and_then(serde_json::Value::as_str) != Some(em)
            {
                oauth.insert(
                    "email".to_string(),
                    serde_json::Value::String(em.to_string()),
                );
                changed = true;
            }
            if let Some(au) = acct_uuid
                && oauth.get("accountUuid").and_then(serde_json::Value::as_str) != Some(au)
            {
                oauth.insert(
                    "accountUuid".to_string(),
                    serde_json::Value::String(au.to_string()),
                );
                changed = true;
            }
            if let Some(ms) = mapped_sub
                && oauth
                    .get("subscriptionType")
                    .and_then(serde_json::Value::as_str)
                    != Some(ms)
            {
                oauth.insert(
                    "subscriptionType".to_string(),
                    serde_json::Value::String(ms.to_string()),
                );
                oauth.insert(
                    "subscriptionTypeSource".to_string(),
                    serde_json::Value::String("profile".to_string()),
                );
                changed = true;
            }
            if let Some(lt) = live_tier
                && oauth
                    .get("rateLimitTier")
                    .and_then(serde_json::Value::as_str)
                    != Some(lt)
            {
                oauth.insert(
                    "rateLimitTier".to_string(),
                    serde_json::Value::String(lt.to_string()),
                );
                changed = true;
            }
        }
    }
    if (changed || !prof_dir.join(".claude/.credentials.json").is_file())
        && let Ok(updated) = serde_json::to_string(cred)
    {
        let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
        let _ = std::fs::write(prof_dir.join(".credentials.json"), &updated);
        let _ = std::fs::write(prof_dir.join(".claude/.credentials.json"), &updated);
        if !profile.starts_with('.') {
            propagate_profile_files_to_bound_agents(
                data,
                "anthropic",
                profile,
                &[".credentials.json", ".claude/.credentials.json"],
            )
            .map_err(|err| eprintln!("runtime persistence failed: {err}"))
            .ok()?;
        }
    }
    let sub_type = cred
        .pointer("/claudeAiOauth/subscriptionType")
        .and_then(serde_json::Value::as_str);
    let rate_tier = cred
        .pointer("/claudeAiOauth/rateLimitTier")
        .and_then(serde_json::Value::as_str);
    anthropic_tier_label(sub_type, rate_tier)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m_adj = if m > 2 {
        (m - 3) as u64
    } else {
        (m + 9) as u64
    };
    let doy = (153 * m_adj + 2) / 5 + (d as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + (doe as i64) - 719_468
}

fn parse_iso_utc_epoch_secs(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.len() < 19 {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: i64 = s.get(5..7)?.parse().ok()?;
    let day: i64 = s.get(8..10)?.parse().ok()?;
    let hour: i64 = s.get(11..13)?.parse().ok()?;
    let min: i64 = s.get(14..16)?.parse().ok()?;
    let sec: i64 = s.get(17..19)?.parse().ok()?;
    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + hour * 3_600 + min * 60 + sec)
}

fn format_reset_eta_secs(rem_secs: i64) -> Option<String> {
    if rem_secs <= 0 {
        return None;
    }
    if rem_secs < 3_600 {
        let m = ((rem_secs + 59) / 60).max(1);
        return Some(format!("{m}m"));
    }
    if rem_secs < 86_400 {
        let h = rem_secs / 3_600;
        let m = (rem_secs % 3_600) / 60;
        return if m == 0 {
            Some(format!("{h}h"))
        } else {
            Some(format!("{h}h{m:02}m"))
        };
    }
    let d = rem_secs / 86_400;
    let h = (rem_secs % 86_400) / 3_600;
    if h == 0 {
        Some(format!("{d}d"))
    } else {
        Some(format!("{d}d{h:02}h"))
    }
}

fn format_reset_eta_iso(iso_str: &str) -> Option<String> {
    let target_secs = parse_iso_utc_epoch_secs(iso_str)?;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    format_reset_eta_secs(target_secs - now_secs)
}

fn format_reset_eta_epoch_ms(target_ms: i64) -> Option<String> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    format_reset_eta_secs((target_ms - now_ms) / 1_000)
}

fn format_anthropic_usage_summary(
    val: &serde_json::Value,
    tier_badge: Option<&str>,
) -> Option<String> {
    let tier_suffix = tier_badge.map(|t| format!(" ({t})")).unwrap_or_default();
    let limits = val.get("limits").and_then(serde_json::Value::as_array);

    let find_limit = |want_kind: &str, want_group: &str| -> Option<(f64, Option<&str>)> {
        let arr = limits?;
        for item in arr {
            if item.get("is_active").and_then(serde_json::Value::as_bool) == Some(false) {
                continue;
            }
            let kind = item
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let group = item
                .get("group")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let scope_null = item.get("scope").is_none_or(serde_json::Value::is_null);
            if kind == want_kind || (group == want_group && scope_null) {
                let pct = item.get("percent").and_then(serde_json::Value::as_f64)?;
                let resets = item.get("resets_at").and_then(serde_json::Value::as_str);
                return Some((pct, resets));
            }
        }
        None
    };

    let mut parts = Vec::new();

    let window_5h = val
        .pointer("/five_hour/utilization")
        .and_then(serde_json::Value::as_f64)
        .map(|u| {
            (
                u,
                val.pointer("/five_hour/resets_at")
                    .and_then(serde_json::Value::as_str),
            )
        })
        .or_else(|| find_limit("five_hour_all", "five_hour"));

    if let Some((u5, resets_5h)) = window_5h {
        let rem_5h = (100.0 - u5).clamp(0.0, 100.0).round() as i64;
        let eta_5h = (rem_5h < 100)
            .then(|| resets_5h.and_then(format_reset_eta_iso))
            .flatten()
            .map(|e| format!(" ↻{e}"))
            .unwrap_or_default();
        parts.push(format!("5h: {rem_5h}% left{eta_5h}"));
    }

    let window_7d = val
        .pointer("/seven_day/utilization")
        .and_then(serde_json::Value::as_f64)
        .map(|u| {
            (
                u,
                val.pointer("/seven_day/resets_at")
                    .and_then(serde_json::Value::as_str),
            )
        })
        .or_else(|| find_limit("weekly_all", "weekly"));

    if let Some((u7, resets_7d)) = window_7d {
        let rem_7d = (100.0 - u7).clamp(0.0, 100.0).round() as i64;
        let eta_7d = (rem_7d < 100)
            .then(|| resets_7d.and_then(format_reset_eta_iso))
            .flatten()
            .map(|e| format!(" ↻{e}"))
            .unwrap_or_default();
        parts.push(format!("7d: {rem_7d}% left{eta_7d}"));
    }

    if let Some(u_opus) = val
        .pointer("/seven_day_opus/utilization")
        .and_then(serde_json::Value::as_f64)
    {
        let rem_opus = (100.0 - u_opus).clamp(0.0, 100.0).round() as i64;
        let eta_opus = (rem_opus < 100)
            .then(|| {
                val.pointer("/seven_day_opus/resets_at")
                    .and_then(serde_json::Value::as_str)
                    .and_then(format_reset_eta_iso)
            })
            .flatten()
            .map(|e| format!(" ↻{e}"))
            .unwrap_or_default();
        parts.push(format!("opus: {rem_opus}% left{eta_opus}"));
    }

    if let Some(minor) = val
        .pointer("/spend/balance/amount_minor")
        .and_then(serde_json::Value::as_i64)
    {
        let exp = val
            .pointer("/spend/balance/exponent")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(2) as i32;
        let dollars = (minor as f64) / 10f64.powi(exp);
        parts.push(format!("${dollars:.2} left"));
    } else if let Some(rem_d) = val
        .pointer("/seven_day/remaining_dollars")
        .and_then(serde_json::Value::as_f64)
    {
        parts.push(format!("${rem_d:.2} left"));
    } else if val
        .pointer("/extra_usage/is_enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && let Some(limit) = val
            .pointer("/extra_usage/monthly_limit")
            .and_then(serde_json::Value::as_f64)
    {
        let used = val
            .pointer("/extra_usage/used_credits")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        let exp = val
            .pointer("/extra_usage/decimal_places")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(2) as i32;
        let scale = if limit > 1000.0 && exp == 2 {
            100.0
        } else {
            1.0
        };
        let rem_d = ((limit - used) / scale).max(0.0);
        parts.push(format!("extra: ${rem_d:.2} left"));
    }

    if parts.is_empty() {
        return None;
    }
    Some(format!("{}{tier_suffix}", parts.join(" · ")))
}

pub(crate) fn probe_one_profile_usage(
    data: &Path,
    provider: &str,
    profile: &str,
) -> Option<String> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    let last_usage_path = prof_dir.join(".anyagents-last-usage.txt");
    let now_ms = anyagents_core::now_ms();
    if provider != "anthropic"
        && let Ok(meta) = std::fs::metadata(&last_usage_path)
        && let Ok(mtime) = meta.modified()
        && let Ok(dur) = mtime.duration_since(std::time::UNIX_EPOCH)
        && now_ms.saturating_sub(dur.as_millis() as i64) < 120_000
        && let Ok(cached) = std::fs::read_to_string(&last_usage_path)
    {
        let trimmed = cached.trim();
        if trimmed == "__NONE__" {
            return None;
        }
        if !trimmed.is_empty() {
            return Some(overlay_active_quota_reset_on_cached_usage(
                &prof_dir,
                &last_usage_path,
                trimmed,
                now_ms,
            ));
        }
    }

    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(3)))
        .build()
        .new_agent();

    let result = (|| -> Option<String> {
        match provider {
            "anthropic" => {
                let cred_text = std::fs::read_to_string(prof_dir.join(".credentials.json"))
                    .or_else(|_| {
                        std::fs::read_to_string(prof_dir.join(".claude/.credentials.json"))
                    })
                    .ok()?;
                let mut cred: serde_json::Value = serde_json::from_str(&cred_text).ok()?;
                let mut token = cred
                    .pointer("/claudeAiOauth/accessToken")?
                    .as_str()?
                    .trim()
                    .to_string();
                if token.is_empty() {
                    return None;
                }
                let fetch_usage = |tok: &str| -> Result<serde_json::Value, u16> {
                    match http
                        .get("https://api.anthropic.com/api/oauth/usage")
                        .header("authorization", &format!("Bearer {tok}"))
                        .header("anthropic-beta", "oauth-2025-04-20")
                        .header("cache-control", "no-cache")
                        .header("user-agent", "claude-code/2.1.310")
                        .call()
                    {
                        Ok(resp) => {
                            let body = resp.into_body().read_to_string().map_err(|_| 0u16)?;
                            serde_json::from_str(&body).map_err(|_| 0u16)
                        }
                        Err(ureq::Error::StatusCode(code)) => Err(code),
                        Err(_) => Err(0),
                    }
                };
                let last_usage_path = prof_dir.join(".anyagents-last-usage.txt");
                let backoff_path = prof_dir.join(".anyagents-usage-backoff-until-ms");
                let now_ms = anyagents_core::now_ms();
                let expires_at = cred
                    .pointer("/claudeAiOauth/expiresAt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(i64::MAX);
                let fallback_cached_usage =
                    |cred_val: &serde_json::Value, oauth_expired: bool| -> Option<String> {
                        let raw = if let Ok(cached) = std::fs::read_to_string(&last_usage_path)
                            && !cached.trim().is_empty()
                        {
                            overlay_active_quota_reset_on_cached_usage(
                                &prof_dir,
                                &last_usage_path,
                                cached.trim(),
                                now_ms,
                            )
                        } else {
                            let tier = cred_val
                                .pointer("/claudeAiOauth/rateLimitTier")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("");
                            let sub = cred_val
                                .pointer("/claudeAiOauth/subscriptionType")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("");
                            let badge = if tier.contains("20x") {
                                "max 20x"
                            } else if tier.contains("5x") {
                                "max 5x"
                            } else {
                                sub
                            };
                            let base = if badge.is_empty() {
                                String::new()
                            } else {
                                format!("({badge})")
                            };
                            overlay_active_quota_reset_on_cached_usage(
                                &prof_dir,
                                &last_usage_path,
                                &base,
                                now_ms,
                            )
                        };
                        if oauth_expired {
                            let tail = raw.strip_prefix("5h: 100% left · ").unwrap_or(raw.as_str());
                            if tail.is_empty() {
                                return Some("oauth expired".to_string());
                            }
                            return Some(format!("oauth expired · {tail}"));
                        }
                        (!raw.is_empty()).then_some(raw)
                    };
                if expires_at > now_ms
                    && let Ok(meta) = std::fs::metadata(&last_usage_path)
                    && let Ok(mtime) = meta.modified()
                    && let Ok(dur) = mtime.duration_since(std::time::UNIX_EPOCH)
                    && now_ms.saturating_sub(dur.as_millis() as i64) < 60_000
                    && let Some(cached) = fallback_cached_usage(&cred, false)
                {
                    return Some(cached);
                }
                if let Some(backoff_until) = std::fs::read_to_string(&backoff_path)
                    .ok()
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    && backoff_until > now_ms
                {
                    return fallback_cached_usage(&cred, expires_at <= now_ms);
                }
                let expiring_soon = now_ms.saturating_add(45 * 60 * 1000) >= expires_at;
                let initial_usage = if expiring_soon {
                    Err(401)
                } else {
                    fetch_usage(&token)
                };
                let val = match initial_usage {
                    Ok(v) => v,
                    Err(code) if code != 401 => {
                        if code == 429 {
                            let _ =
                                std::fs::write(&backoff_path, format!("{}\n", now_ms + 180_000));
                        }
                        return fallback_cached_usage(&cred, expires_at <= now_ms);
                    }
                    Err(_) => {
                        let attempt_path = prof_dir.join(".anyagents-oauth-refresh-attempt-ms");
                        let last_attempt_ms = std::fs::read_to_string(&attempt_path)
                            .ok()
                            .and_then(|s| s.trim().parse::<i64>().ok())
                            .unwrap_or(0);
                        if now_ms.saturating_sub(last_attempt_ms) < 300_000 {
                            if expires_at <= now_ms {
                                return fallback_cached_usage(&cred, true);
                            }
                            match fetch_usage(&token) {
                                Ok(v) => v,
                                Err(code) => {
                                    if code == 429 {
                                        let _ = std::fs::write(
                                            &backoff_path,
                                            format!("{}\n", now_ms + 180_000),
                                        );
                                    }
                                    return fallback_cached_usage(&cred, false);
                                }
                            }
                        } else {
                            let rtok = cred
                                .pointer("/claudeAiOauth/refreshToken")
                                .and_then(serde_json::Value::as_str)?
                                .trim()
                                .to_string();
                            if rtok.is_empty() {
                                return fallback_cached_usage(&cred, expires_at <= now_ms);
                            }
                            let _ = std::fs::write(&attempt_path, format!("{now_ms}\n"));
                            let req_body = serde_json::json!({
                                "grant_type": "refresh_token",
                                "refresh_token": rtok,
                                "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
                            })
                            .to_string();
                            let Some(rval) = post_anthropic_oauth_refresh(&http, &req_body) else {
                                return fallback_cached_usage(&cred, expires_at <= now_ms);
                            };
                            let Some(new_acc) = rval
                                .get("access_token")
                                .and_then(|v| v.as_str())
                                .map(|s| s.trim().to_string())
                            else {
                                return fallback_cached_usage(&cred, expires_at <= now_ms);
                            };
                            if let Some(oauth) = cred
                                .get_mut("claudeAiOauth")
                                .and_then(serde_json::Value::as_object_mut)
                            {
                                oauth.insert(
                                    "accessToken".to_string(),
                                    serde_json::Value::String(new_acc.clone()),
                                );
                                if let Some(new_ref) = rval
                                    .get("refresh_token")
                                    .and_then(serde_json::Value::as_str)
                                {
                                    oauth.insert(
                                        "refreshToken".to_string(),
                                        serde_json::Value::String(new_ref.to_string()),
                                    );
                                }
                                if let Some(exp_in) =
                                    rval.get("expires_in").and_then(serde_json::Value::as_i64)
                                {
                                    oauth.insert(
                                        "expiresAt".to_string(),
                                        serde_json::Value::Number((now_ms + exp_in * 1000).into()),
                                    );
                                }
                                if let Ok(updated) = serde_json::to_string(&cred) {
                                    persist_refreshed_credentials(
                                        &prof_dir,
                                        &[".credentials.json", ".claude/.credentials.json"],
                                        updated.as_bytes(),
                                    )
                                    .map_err(|err| {
                                        eprintln!("credential refresh persistence failed: {err}")
                                    })
                                    .ok()?;
                                    propagate_profile_files_to_bound_agents(
                                        data,
                                        provider,
                                        profile,
                                        &[".credentials.json", ".claude/.credentials.json"],
                                    )
                                    .map_err(|err| eprintln!("runtime persistence failed: {err}"))
                                    .ok()?;
                                }
                            }
                            token = new_acc;
                            match fetch_usage(&token) {
                                Ok(v) => v,
                                Err(_) => return fallback_cached_usage(&cred, false),
                            }
                        }
                    }
                };
                let tier_badge = sync_anthropic_profile_metadata(
                    &http, data, &prof_dir, profile, &token, &mut cred, false,
                );
                let formatted = format_anthropic_usage_summary(&val, tier_badge);
                if let Some(ref s) = formatted {
                    let _ = std::fs::write(&last_usage_path, format!("{s}\n"));
                    let _ = std::fs::remove_file(&backoff_path);
                    if parse_remaining_pct_from_usage(s, None).is_some_and(|p| p > 0) {
                        let _ = std::fs::remove_file(prof_dir.join(".anyagents-quota-reset-ms"));
                    }
                }
                formatted
            }
            "openai" => {
                let auth_text = std::fs::read_to_string(prof_dir.join("auth.json"))
                    .or_else(|_| std::fs::read_to_string(prof_dir.join(".codex/auth.json")))
                    .ok()?;
                let mut auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
                let token = auth
                    .pointer("/tokens/access_token")?
                    .as_str()?
                    .trim()
                    .to_string();
                if token.is_empty() {
                    return None;
                }
                let acc_id = auth
                    .pointer("/tokens/account_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let (oa_em, oa_id) = extract_profile_account_identity(&prof_dir, "openai");
                update_profile_json_identity(
                    &prof_dir,
                    "openai",
                    oa_em.as_deref(),
                    oa_id
                        .as_deref()
                        .or((!acc_id.is_empty()).then_some(acc_id.as_str())),
                );
                let fetch_wham = |tok: &str| -> Option<serde_json::Value> {
                    let mut req = http
                        .get("https://chatgpt.com/backend-api/wham/usage")
                        .header("authorization", &format!("Bearer {tok}"))
                        .header("user-agent", "codex-cli/0.164.0");
                    if !acc_id.is_empty() {
                        req = req.header("chatgpt-account-id", &acc_id);
                    }
                    let resp = req.call().ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let val = match fetch_wham(&token) {
                    Some(v) => v,
                    None => {
                        let rtok = auth
                            .pointer("/tokens/refresh_token")
                            .and_then(serde_json::Value::as_str)?
                            .trim()
                            .to_string();
                        if rtok.is_empty() {
                            return None;
                        }
                        let req_body = serde_json::json!({
                            "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
                            "grant_type": "refresh_token",
                            "refresh_token": rtok,
                        })
                        .to_string();
                        let rresp = http
                            .post("https://auth.openai.com/oauth/token")
                            .header("content-type", "application/json")
                            .send(req_body.as_bytes())
                            .ok()?;
                        let rbody = rresp.into_body().read_to_string().ok()?;
                        let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                        let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
                        if let Some(tokens_obj) = auth
                            .get_mut("tokens")
                            .and_then(serde_json::Value::as_object_mut)
                        {
                            tokens_obj.insert(
                                "access_token".to_string(),
                                serde_json::Value::String(new_acc.clone()),
                            );
                            if let Some(new_ref) = rval
                                .get("refresh_token")
                                .and_then(serde_json::Value::as_str)
                            {
                                tokens_obj.insert(
                                    "refresh_token".to_string(),
                                    serde_json::Value::String(new_ref.to_string()),
                                );
                            }
                            if let Ok(updated) = serde_json::to_string_pretty(&auth) {
                                persist_refreshed_credentials(
                                    &prof_dir,
                                    &["auth.json", ".codex/auth.json"],
                                    updated.as_bytes(),
                                )
                                .map_err(|err| {
                                    eprintln!("credential refresh persistence failed: {err}")
                                })
                                .ok()?;
                                propagate_profile_files_to_bound_agents(
                                    data,
                                    provider,
                                    profile,
                                    &["auth.json", ".codex/auth.json"],
                                )
                                .map_err(|err| eprintln!("runtime persistence failed: {err}"))
                                .ok()?;
                            }
                        }
                        fetch_wham(&new_acc)?
                    }
                };
                let used = val
                    .pointer("/rate_limit/primary_window/used_percent")
                    .and_then(serde_json::Value::as_f64)?;
                let rem = (100.0 - used).clamp(0.0, 100.0).round() as i64;
                let eta = (rem < 100)
                    .then(|| {
                        val.pointer("/rate_limit/primary_window/reset_after_seconds")
                            .and_then(serde_json::Value::as_i64)
                            .and_then(format_reset_eta_secs)
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                match val.get("plan_type").and_then(serde_json::Value::as_str) {
                    Some(plan) if !plan.is_empty() => {
                        Some(format!("7d: {rem}% left{eta} ({plan})"))
                    }
                    _ => Some(format!("7d: {rem}% left{eta}")),
                }
            }
            "google-antigravity" => {
                let tok_rel = ".gemini/antigravity-cli/antigravity-oauth-token";
                let tok_path = prof_dir.join(tok_rel);
                let tok_text = std::fs::read_to_string(&tok_path).ok()?;
                let mut tok_val: serde_json::Value = serde_json::from_str(&tok_text).ok()?;
                let token = tok_val
                    .pointer("/token/access_token")
                    .or_else(|| tok_val.get("access_token"))?
                    .as_str()?
                    .trim()
                    .to_string();
                if token.is_empty() {
                    return None;
                }
                let fetch_summary = |tok: &str| -> Option<serde_json::Value> {
                    let resp = http
                    .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:retrieveUserQuotaSummary")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("content-type", "application/json")
                    .header("user-agent", "antigravity/3.0.0")
                    .send(b"{}")
                    .ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let fetch_models = |tok: &str| -> Option<serde_json::Value> {
                    let resp = http
                    .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("content-type", "application/json")
                    .header("user-agent", "antigravity")
                    .send(b"{}")
                    .ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let mut active_token = token;
                let mut summary_val = fetch_summary(&active_token);
                if summary_val.is_none() {
                    let attempt_path = prof_dir.join(".anyagents-oauth-refresh-attempt-ms");
                    let last_attempt_ms = std::fs::read_to_string(&attempt_path)
                        .ok()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        .unwrap_or(0);
                    if now_ms.saturating_sub(last_attempt_ms) < 300_000 {
                        let _ = std::fs::write(&last_usage_path, "__NONE__\n");
                        return None;
                    }
                    let _ = std::fs::write(&attempt_path, format!("{now_ms}\n"));
                    let rtok = tok_val
                        .pointer("/token/refresh_token")
                        .or_else(|| tok_val.get("refresh_token"))
                        .and_then(serde_json::Value::as_str)?
                        .trim()
                        .to_string();
                    if rtok.is_empty() {
                        let _ = std::fs::write(&last_usage_path, "__NONE__\n");
                        return None;
                    }
                    let mut refreshed_acc = None;
                    for (cid, csec) in extract_agy_oauth_clients() {
                        let form_body = format!(
                            "client_id={cid}&client_secret={csec}&refresh_token={rtok}&grant_type=refresh_token"
                        );
                        if let Ok(rresp) = http
                            .post("https://oauth2.googleapis.com/token")
                            .header("content-type", "application/x-www-form-urlencoded")
                            .send(form_body.as_bytes())
                            && let Ok(rbody) = rresp.into_body().read_to_string()
                            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
                            && let Some(acc) = rval
                                .get("access_token")
                                .and_then(serde_json::Value::as_str)
                                .map(|s| s.trim().to_string())
                            && !acc.is_empty()
                        {
                            refreshed_acc = Some(acc);
                            break;
                        }
                    }
                    let Some(new_acc) = refreshed_acc else {
                        let _ = std::fs::write(&last_usage_path, "__NONE__\n");
                        return None;
                    };
                    if let Some(tok_obj) = tok_val
                        .get_mut("token")
                        .and_then(serde_json::Value::as_object_mut)
                    {
                        tok_obj.insert(
                            "access_token".to_string(),
                            serde_json::Value::String(new_acc.clone()),
                        );
                    } else if let Some(root_obj) = tok_val.as_object_mut() {
                        root_obj.insert(
                            "access_token".to_string(),
                            serde_json::Value::String(new_acc.clone()),
                        );
                    }
                    if let Ok(updated) = serde_json::to_string_pretty(&tok_val) {
                        let _ = std::fs::write(&tok_path, &updated);
                        propagate_profile_files_to_bound_agents(
                            data,
                            provider,
                            profile,
                            &[tok_rel],
                        )
                        .map_err(|err| eprintln!("runtime persistence failed: {err}"))
                        .ok()?;
                    }
                    active_token = new_acc;
                    summary_val = fetch_summary(&active_token);
                }
                if extract_profile_account_identity(&prof_dir, "google-antigravity")
                    .0
                    .is_none()
                    && let Ok(uresp) = http
                        .get("https://www.googleapis.com/oauth2/v2/userinfo")
                        .header("authorization", &format!("Bearer {active_token}"))
                        .call()
                    && let Ok(ubody) = uresp.into_body().read_to_string()
                    && let Ok(uval) = serde_json::from_str::<serde_json::Value>(&ubody)
                {
                    let em = uval.get("email").and_then(serde_json::Value::as_str);
                    let uid = uval.get("id").and_then(serde_json::Value::as_str);
                    update_profile_json_identity(&prof_dir, "google-antigravity", em, uid);
                }
                if let Some(ref sv) = summary_val
                    && let Some(groups) = sv.get("groups").and_then(serde_json::Value::as_array)
                {
                    let mut gem_5h: Option<(i64, Option<String>)> = None;
                    let mut gem_7d: Option<(i64, Option<String>)> = None;
                    let mut opus_5h: Option<i64> = None;
                    let mut opus_7d: Option<i64> = None;
                    for g in groups {
                        let Some(buckets) = g.get("buckets").and_then(serde_json::Value::as_array)
                        else {
                            continue;
                        };
                        for b in buckets {
                            let bid = b
                                .get("bucketId")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("");
                            let Some(pct) =
                                b.get("remainingFraction").and_then(observed_fraction_pct)
                            else {
                                continue;
                            };
                            let eta = (pct < 100)
                                .then(|| {
                                    b.get("resetTime")
                                        .and_then(serde_json::Value::as_str)
                                        .and_then(format_reset_eta_iso)
                                })
                                .flatten();
                            match bid {
                                "gemini-5h" => gem_5h = Some((pct, eta)),
                                "gemini-weekly" => gem_7d = Some((pct, eta)),
                                "3p-5h" => opus_5h = Some(pct),
                                "3p-weekly" => opus_7d = Some(pct),
                                _ => {}
                            }
                        }
                    }
                    if let (Some((g5, g5_eta)), Some((mut g7, mut g7_eta))) = (gem_5h, gem_7d) {
                        let now_ms = anyagents_core::now_ms();
                        let tier_cache_path = prof_dir.join(".anyagents-agy-tier.txt");
                        let tier_fresh = std::fs::metadata(&tier_cache_path)
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .is_some_and(|d| now_ms.saturating_sub(d.as_millis() as i64) < 600_000)
                            && prof_dir.join(".anyagents-profile-meta-sync-ms").exists();
                        let tier = if tier_fresh
                            && let Ok(cached_tier) = std::fs::read_to_string(&tier_cache_path)
                            && !cached_tier.trim().is_empty()
                            && cached_tier.trim() != "free"
                        {
                            cached_tier.trim().to_string()
                        } else {
                            let resolved_tier = http
                            .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:loadCodeAssist")
                            .header("authorization", &format!("Bearer {active_token}"))
                            .header("content-type", "application/json")
                            .header("user-agent", "antigravity/3.0.0")
                            .send(b"{}")
                            .ok()
                            .and_then(|r| r.into_body().read_to_string().ok())
                            .and_then(|b| serde_json::from_str::<serde_json::Value>(&b).ok())
                            .as_ref().and_then(observed_agy_tier)
                            .filter(|tier| !tier.is_empty())?;
                            let _ = std::fs::write(&tier_cache_path, format!("{resolved_tier}\n"));
                            let _ = std::fs::write(
                                prof_dir.join(".anyagents-profile-meta-sync-ms"),
                                format!("{now_ms}\n"),
                            );
                            resolved_tier
                        };
                        let reset_marker = prof_dir.join(".anyagents-quota-reset-ms");
                        if tier == "ultra" && g5.min(g7) > 0 {
                            let _ = std::fs::remove_file(&reset_marker);
                        } else if let Ok(raw_reset) = std::fs::read_to_string(&reset_marker)
                            && let Ok(reset_ms) = raw_reset.trim().parse::<i64>()
                        {
                            if reset_ms <= now_ms {
                                let _ = std::fs::remove_file(&reset_marker);
                            } else {
                                g7 = 0;
                                g7_eta =
                                    format_reset_eta_secs((reset_ms - now_ms) / 1_000).or(g7_eta);
                            }
                        }
                        let e5 = g5_eta.map(|e| format!(" ↻{e}")).unwrap_or_default();
                        let e7 = g7_eta.map(|e| format!(" ↻{e}")).unwrap_or_default();
                        let opus_min = observed_window_min(opus_5h, opus_7d)?;
                        return Some(format!(
                            "5h: {g5}% left{e5} · 7d: {g7}% left{e7} · opus: {opus_min}% left ({tier})"
                        ));
                    }
                }
                let val = fetch_models(&active_token)?;
                let models = val.get("models")?.as_object()?;
                let frac_of = |names: &[&str]| -> Option<i64> {
                    for n in names {
                        if let Some(m) = models.get(*n)
                            && let Some(qi) = m.get("quotaInfo")
                        {
                            let f = qi
                                .get("remainingFraction")
                                .and_then(serde_json::Value::as_f64)?;
                            return Some((f * 100.0).clamp(0.0, 100.0).round() as i64);
                        }
                    }
                    None
                };
                let gem = frac_of(&[
                    "gemini-4-argon-high",
                    "gemini-3.1-pro-high",
                    "gemini-2.5-pro",
                    "gemini-3-flash",
                ])?;
                Some(format!("5h: {gem}% left · 7d: {gem}% left"))
            }
            "kimi-code-plan" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &[
                        ".config/opencode/kimi-code-plan.key",
                        ".config/opencode/kimi.key",
                    ],
                )?;
                let resp = http
                    .get("https://api.kimi.com/coding/v1/usages")
                    .header("authorization", &format!("Bearer {key}"))
                    .header("user-agent", "claude-code/1.0")
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let parse_num = |ptr: &str| -> Option<f64> {
                    let v = val.pointer(ptr)?;
                    v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok())
                };
                let pct_5h = if let Some(ratio) = parse_num("/usages/limit_5h/used_ratio") {
                    ((1.0 - ratio) * 100.0).clamp(0.0, 100.0).round() as i64
                } else {
                    let l5 = parse_num("/limits/0/detail/limit")?.max(1.0);
                    let r5 = parse_num("/limits/0/detail/remaining")?;
                    (r5 / l5 * 100.0).clamp(0.0, 100.0).round() as i64
                };
                let pct_mo = if let Some(ratio) = parse_num("/usages/limit_month_total/used_ratio")
                {
                    ((1.0 - ratio) * 100.0).clamp(0.0, 100.0).round() as i64
                } else {
                    let lm = parse_num("/usage/limit")?.max(1.0);
                    let rm = parse_num("/usage/remaining")?;
                    (rm / lm * 100.0).clamp(0.0, 100.0).round() as i64
                };
                let eta_5h = (pct_5h < 100)
                    .then(|| {
                        val.pointer("/usages/limit_5h/reset_time")
                            .or_else(|| val.pointer("/limits/0/detail/resetTime"))
                            .and_then(serde_json::Value::as_str)
                            .and_then(format_reset_eta_iso)
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                let eta_mo = (pct_mo < 100)
                    .then(|| {
                        val.pointer("/usages/limit_month_total/reset_time")
                            .and_then(serde_json::Value::as_str)
                            .and_then(format_reset_eta_iso)
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                Some(format!(
                    "5h: {pct_5h}% left{eta_5h} · mo: {pct_mo}% left{eta_mo}"
                ))
            }
            "minimax-code-plan" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &[
                        ".config/opencode/minimax-code-plan.key",
                        ".config/opencode/minimax.key",
                    ],
                )?;
                let resp = http
                    .get("https://api.minimax.io/v1/api/openplatform/coding_plan/remains")
                    .header("authorization", &format!("Bearer {key}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let m0 = val.pointer("/model_remains/0")?;
                let pct_5h = if let Some(p) = m0
                    .get("current_interval_remaining_percent")
                    .and_then(serde_json::Value::as_f64)
                {
                    p.clamp(0.0, 100.0).round() as i64
                } else {
                    let tot_5h = m0
                        .get("current_interval_total_count")
                        .and_then(serde_json::Value::as_f64)?
                        .max(1.0);
                    let rem_5h = m0
                        .get("current_interval_usage_count")
                        .and_then(serde_json::Value::as_f64)?;
                    (rem_5h / tot_5h * 100.0).clamp(0.0, 100.0).round() as i64
                };
                let pct_7d = if let Some(p) = m0
                    .get("current_weekly_remaining_percent")
                    .and_then(serde_json::Value::as_f64)
                {
                    p.clamp(0.0, 100.0).round() as i64
                } else {
                    let tot_7d = m0
                        .get("current_weekly_total_count")
                        .and_then(serde_json::Value::as_f64)?
                        .max(1.0);
                    let rem_7d = m0
                        .get("current_weekly_usage_count")
                        .and_then(serde_json::Value::as_f64)?;
                    (rem_7d / tot_7d * 100.0).clamp(0.0, 100.0).round() as i64
                };
                let eta_5h = (pct_5h < 100)
                    .then(|| {
                        m0.get("remains_time")
                            .and_then(serde_json::Value::as_i64)
                            .and_then(|ms| format_reset_eta_secs(ms / 1_000))
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                let eta_7d = (pct_7d < 100)
                    .then(|| {
                        m0.get("weekly_remains_time")
                            .and_then(serde_json::Value::as_i64)
                            .and_then(|ms| format_reset_eta_secs(ms / 1_000))
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                Some(format!(
                    "5h: {pct_5h}% left{eta_5h} · 7d: {pct_7d}% left{eta_7d}"
                ))
            }
            "glm-code-plan" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &[
                        ".config/opencode/glm-code-plan.key",
                        ".config/opencode/glm.key",
                    ],
                )?;
                let resp = http
                    .get("https://api.z.ai/api/monitor/usage/quota/limit")
                    .header("authorization", &key)
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let limits = val.pointer("/data/limits")?.as_array()?;
                let mut rem_5h = None;
                let mut eta_5h = None;
                let mut rem_7d = None;
                let mut eta_7d = None;
                for item in limits {
                    if item.get("type").and_then(serde_json::Value::as_str) != Some("TOKENS_LIMIT")
                    {
                        continue;
                    }
                    let unit = item.get("unit").and_then(serde_json::Value::as_i64);
                    let rem = item.get("percentage").and_then(observed_remaining_pct)?;
                    let eta = (rem < 100)
                        .then(|| {
                            item.get("nextResetTime")
                                .and_then(serde_json::Value::as_i64)
                                .and_then(format_reset_eta_epoch_ms)
                        })
                        .flatten();
                    if unit == Some(3) {
                        rem_5h = Some(rem);
                        eta_5h = eta;
                    } else if unit == Some(6) {
                        rem_7d = Some(rem);
                        eta_7d = eta;
                    }
                }
                let r5 = rem_5h?;
                let r7 = rem_7d?;
                let e5 = eta_5h.map(|e| format!(" ↻{e}")).unwrap_or_default();
                let e7 = eta_7d.map(|e| format!(" ↻{e}")).unwrap_or_default();
                Some(format!("5h: {r5}% left{e5} · 7d: {r7}% left{e7}"))
            }
            "deepseek" => {
                let key = read_first_non_empty_file(&prof_dir, &[".config/opencode/deepseek.key"])
                    .or_else(|| read_dsh_yaml_ref(&prof_dir, "DEEPSEEK_API_KEY"))?;
                let resp = http
                    .get("https://api.deepseek.com/user/balance")
                    .header("authorization", &format!("Bearer {key}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let infos = val.get("balance_infos")?.as_array()?;
                let mut best: Option<(f64, String)> = None;
                for info in infos {
                    let bal = info
                        .get("total_balance")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|s| s.parse::<f64>().ok())?;
                    let cur = info
                        .get("currency")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("CNY")
                        .to_string();
                    if best.as_ref().is_none_or(|(b, _)| bal > *b) {
                        best = Some((bal, cur));
                    }
                }
                let (bal, cur) = best?;
                let sym = if cur == "CNY" { "¥" } else { "$" };
                Some(format!("{sym}{bal:.2} {cur}"))
            }
            "openrouter" => {
                let key =
                    read_first_non_empty_file(&prof_dir, &[".config/opencode/openrouter.key"])
                        .or_else(|| read_dsh_yaml_ref(&prof_dir, "OPENROUTER_API_KEY"))?;
                let resp = http
                    .get("https://openrouter.ai/api/v1/credits")
                    .header("authorization", &format!("Bearer {key}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let total = val
                    .pointer("/data/total_credits")
                    .and_then(serde_json::Value::as_f64)?;
                let used = val
                    .pointer("/data/total_usage")
                    .and_then(serde_json::Value::as_f64)?;
                let rem = (total - used).max(0.0);
                Some(format!("${rem:.2} left (${used:.0} / ${total:.0})"))
            }
            "cursor" => {
                let (mut access, refresh) = extract_cursor_oauth_from_db(&prof_dir)?;
                let fetch_summary = |tok: &str| -> Option<serde_json::Value> {
                    let uid = extract_jwt_sub_tail(tok)?;
                    let resp = http
                        .get("https://cursor.com/api/usage-summary")
                        .header("authorization", &format!("Bearer {tok}"))
                        .header(
                            "cookie",
                            &format!("WorkosCursorSessionToken={uid}%3A%3A{tok}"),
                        )
                        .call()
                        .ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let val = match fetch_summary(&access) {
                    Some(v) => v,
                    None if !refresh.is_empty() => {
                        let cursor_cid = concat!("KbZUR41c", "Y7W6zRSd", "pSUJ7I7m", "LYBKOCmB");
                        let req_body = serde_json::json!({
                            "grant_type": "refresh_token",
                            "client_id": cursor_cid,
                            "refresh_token": refresh,
                        })
                        .to_string();
                        let rresp = http
                            .post("https://api2.cursor.sh/oauth/token")
                            .header("content-type", "application/json")
                            .send(req_body.as_bytes())
                            .ok()?;
                        let rbody = rresp.into_body().read_to_string().ok()?;
                        let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                        access = rval.get("access_token")?.as_str()?.trim().to_string();
                        fetch_summary(&access)?
                    }
                    None => return None,
                };
                let plan = val.pointer("/individualUsage/plan")?;
                let limit_cents = plan.get("limit").and_then(serde_json::Value::as_f64)?;
                let rem_cents = plan.get("remaining").and_then(serde_json::Value::as_f64)?;
                let rem_pct = plan
                    .get("totalPercentUsed")
                    .and_then(observed_remaining_pct)?;
                let tier = val
                    .get("membershipType")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                if limit_cents > 0.0 {
                    let rem_usd = (rem_cents / 100.0).round() as i64;
                    let lim_usd = (limit_cents / 100.0).round() as i64;
                    Some(format!(
                        "mo: {rem_pct}% left (${rem_usd}/${lim_usd} {tier})"
                    ))
                } else {
                    Some(format!("mo: {rem_pct}% left ({tier})"))
                }
            }
            "muse" => {
                let auth_text = std::fs::read_to_string(prof_dir.join("muse/auth.json"))
                    .or_else(|_| std::fs::read_to_string(prof_dir.join(".config/muse/auth.json")))
                    .ok()?;
                let auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
                let token = auth
                    .pointer("/providers/meta/access_token")?
                    .as_str()?
                    .trim();
                if token.is_empty() {
                    return None;
                }
                let resp = http
                    .post("https://api.meta.ai/muse-code/key")
                    .header("authorization", &format!("Bearer {token}"))
                    .header("x-api-version", "1.0.0")
                    .header("content-type", "application/json")
                    .send(b"{}")
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                if val
                    .get("is_subs_active")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
                {
                    return None;
                }
                let tier = val
                    .get("subs_tier_name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("active");
                let short_tier = tier.strip_prefix("Muse Code ").unwrap_or(tier);
                let rem_pct = val
                    .pointer("/subs_usage/window/used_percent")
                    .or_else(|| val.pointer("/subs_usage/used_percent"))
                    .and_then(observed_remaining_pct)?;
                Some(format!("5h: {rem_pct}% left ({short_tier})"))
            }
            "xai" => {
                let auth_rel = ".grok/auth.json";
                let auth_path = prof_dir.join(auth_rel);
                let auth_text = std::fs::read_to_string(&auth_path).ok()?;
                let mut root_auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
                let entry_key = if root_auth.get("key").is_some() {
                    None
                } else {
                    root_auth
                        .as_object()?
                        .iter()
                        .find(|(_, v)| v.get("key").is_some())
                        .map(|(k, _)| k.clone())
                };
                let auth_entry = match &entry_key {
                    Some(k) => root_auth.get(k)?,
                    None => &root_auth,
                };
                let key = auth_entry.get("key")?.as_str()?.trim().to_string();
                if key.is_empty() {
                    return None;
                }
                let rtok = auth_entry
                    .get("refresh_token")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let issuer = auth_entry
                    .get("oidc_issuer")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("https://auth.x.ai")
                    .trim_end_matches('/')
                    .to_string();
                let cid = auth_entry
                    .get("oidc_client_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let fetch_billing = |tok: &str| -> Option<serde_json::Value> {
                    let resp = http
                        .get("https://cli-chat-proxy.grok.com/v1/billing?format=credits")
                        .header("authorization", &format!("Bearer {tok}"))
                        .header("x-xai-token-auth", "xai-grok-cli")
                        .call()
                        .ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let val = match fetch_billing(&key) {
                    Some(v) => v,
                    None => {
                        if rtok.is_empty() || cid.is_empty() {
                            return None;
                        }
                        let form_body = format!(
                            "grant_type=refresh_token&refresh_token={rtok}&client_id={cid}"
                        );
                        let rresp = http
                            .post(&format!("{issuer}/oauth2/token"))
                            .header("content-type", "application/x-www-form-urlencoded")
                            .send(form_body.as_bytes())
                            .ok()?;
                        let rbody = rresp.into_body().read_to_string().ok()?;
                        let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                        let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
                        let target_obj = match &entry_key {
                            Some(k) => root_auth
                                .get_mut(k)
                                .and_then(serde_json::Value::as_object_mut),
                            None => root_auth.as_object_mut(),
                        };
                        if let Some(obj) = target_obj {
                            obj.insert(
                                "key".to_string(),
                                serde_json::Value::String(new_acc.clone()),
                            );
                            if let Some(new_ref) = rval
                                .get("refresh_token")
                                .and_then(serde_json::Value::as_str)
                            {
                                obj.insert(
                                    "refresh_token".to_string(),
                                    serde_json::Value::String(new_ref.to_string()),
                                );
                            }
                            if let Ok(updated) = serde_json::to_string_pretty(&root_auth) {
                                persist_refreshed_credentials(
                                    &prof_dir,
                                    &[auth_rel],
                                    updated.as_bytes(),
                                )
                                .map_err(|err| {
                                    eprintln!("credential refresh persistence failed: {err}")
                                })
                                .ok()?;
                                propagate_profile_files_to_bound_agents(
                                    data,
                                    provider,
                                    profile,
                                    &[auth_rel],
                                )
                                .map_err(|err| eprintln!("runtime persistence failed: {err}"))
                                .ok()?;
                            }
                        }
                        fetch_billing(&new_acc)?
                    }
                };
                let used = val
                    .pointer("/config/creditUsagePercent")
                    .or_else(|| val.get("creditUsagePercent"))
                    .and_then(serde_json::Value::as_f64)?;
                let rem = (100.0 - used).clamp(0.0, 100.0).round() as i64;
                let eta = (rem < 100)
                    .then(|| {
                        val.pointer("/config/currentPeriod/end")
                            .or_else(|| val.pointer("/currentPeriod/end"))
                            .and_then(serde_json::Value::as_str)
                            .and_then(format_reset_eta_iso)
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                Some(format!("7d: {rem}% left{eta}"))
            }
            "qwen-token-plan" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &[
                        ".config/opencode/qwen-coding-plan.key",
                        ".config/opencode/qwen.key",
                    ],
                )?;
                let resp = http
                    .get("https://token-plan.maas.qwencloudapi.com/compatible-mode/v1/models")
                    .header("authorization", &format!("Bearer {key}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let count = val.get("data")?.as_array()?.len();
                Some(format!("active ({count} models)"))
            }
            "sylphx" => {
                let resp = http.get("https://api.sylphx.com/health").call().ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                if val.get("status").and_then(serde_json::Value::as_str) == Some("ok") {
                    Some("online (gateway-edge)".to_string())
                } else {
                    None
                }
            }
            "google-gemini" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &["gemini-api-key", ".config/opencode/gemini.key"],
                )
                .or_else(|| read_dsh_yaml_ref(&prof_dir, "GEMINI_API_KEY"))?;
                let resp = http
                .get(&format!(
                    "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1&key={key}"
                ))
                .call()
                .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                if val.get("models").is_some() {
                    Some("active (api-key)".to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    })();
    if provider != "anthropic" {
        if let Some(ref s) = result {
            let _ = std::fs::write(&last_usage_path, format!("{s}\n"));
        } else {
            let _ = std::fs::write(&last_usage_path, "__NONE__\n");
        }
    }
    result
}

pub(crate) fn parse_remaining_pct_from_usage(usage: &str, model: Option<&str>) -> Option<i64> {
    if usage.starts_with("oauth expired") {
        return Some(0);
    }
    let extract_after = |label: &str| -> Option<i64> {
        let idx = usage.find(label)?;
        let rest = &usage[idx + label.len()..];
        let pct_idx = rest.find("% left")?;
        rest[..pct_idx].trim().parse::<i64>().ok()
    };
    let wants_opus = model.is_some_and(|m| {
        let lower = m.to_ascii_lowercase();
        lower.contains("opus") || lower.contains("claude")
    });
    if wants_opus && usage.contains("opus:") {
        return extract_after("opus:");
    }
    if usage.contains("gemini:") {
        return extract_after("gemini:");
    }
    let mut min_pct: Option<i64> = None;
    for prefix in ["5h:", "7d:", "mo:"] {
        if let Some(p) = extract_after(prefix) {
            min_pct = Some(min_pct.map_or(p, |cur| cur.min(p)));
        }
    }
    min_pct
}

pub(crate) fn probe_profile_remaining_pct(
    data: &Path,
    provider: &str,
    profile: &str,
    model: Option<&str>,
) -> Option<i64> {
    sync_fresher_bound_agent_credentials(data);
    let usage = probe_one_profile_usage(data, provider, profile)?;
    let pct = parse_remaining_pct_from_usage(&usage, model)?;
    if provider == "anthropic" && pct > 0 {
        let prof_dir = data.join("profiles").join(provider).join(profile);
        let now_ms = anyagents_core::now_ms();
        let fresh_live_read = std::fs::metadata(prof_dir.join(".anyagents-last-usage.txt"))
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .is_some_and(|d| now_ms.saturating_sub(d.as_millis() as i64) < 65_000);
        if !fresh_live_read {
            return None;
        }
    }
    Some(pct)
}

pub(crate) fn poke_agent_wake_socket(data: &Path, agent_id: &AgentId) {
    #[cfg(unix)]
    {
        use std::os::unix::net::UnixDatagram;
        let sock_path = data.join("wake.sock");
        if sock_path.exists()
            && let Ok(sock) = UnixDatagram::unbound()
        {
            let msg = format!("a{}", agent_id.as_str());
            let _ = sock.send_to(msg.as_bytes(), &sock_path);
        }
    }
}

pub(crate) fn force_refresh_provider_profiles(
    data: &Path,
    target: Option<&str>,
) -> Result<(), Error> {
    let filter = target.map(str::trim).filter(|s| {
        !s.is_empty() && !s.eq_ignore_ascii_case("refresh") && !s.eq_ignore_ascii_case("reset")
    });
    let _ = std::fs::remove_file(data.join("profiles/.last-agent-cred-sync-ms"));
    sync_fresher_bound_agent_credentials(data);
    let now_ms = anyagents_core::now_ms();
    if let Ok(store) = Store::open(data.join("state.db"))
        && let Ok(agents) = store.list_agents()
    {
        for agent in agents {
            if agent.state == "archived" {
                continue;
            }
            let cfg = agent_config_dir(data, &agent.id);
            let _ = std::fs::remove_file(cfg.join(".anyagents-last-quota-check-ms"));
        }
    }
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(4)))
        .build()
        .new_agent();
    let profiles_root = data.join("profiles");
    let Ok(providers) = std::fs::read_dir(&profiles_root) else {
        return Ok(());
    };
    for prov_entry in providers.flatten() {
        let prov_path = prov_entry.path();
        if !prov_path.is_dir() {
            continue;
        }
        let provider = prov_entry.file_name().to_string_lossy().into_owned();
        let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
            continue;
        };
        for prof_entry in prof_dirs.flatten() {
            let prof_dir = prof_entry.path();
            if !prof_dir.is_dir() {
                continue;
            }
            let profile = prof_entry.file_name().to_string_lossy().into_owned();
            let full_name = format!("{provider}/{profile}");
            if let Some(f) = filter
                && f != provider
                && f != profile
                && f != full_name
            {
                continue;
            }
            let _ = std::fs::remove_file(prof_dir.join(".anyagents-profile-meta-sync-ms"));
            let _ = std::fs::remove_file(prof_dir.join(".anyagents-last-usage.txt"));
            let _ = std::fs::remove_file(prof_dir.join(".anyagents-oauth-refresh-attempt-ms"));
            match provider.as_str() {
                "anthropic" => {
                    let Ok(cred_text) = std::fs::read_to_string(prof_dir.join(".credentials.json"))
                        .or_else(|_| {
                            std::fs::read_to_string(prof_dir.join(".claude/.credentials.json"))
                        })
                    else {
                        continue;
                    };
                    let Ok(mut cred) = serde_json::from_str::<serde_json::Value>(&cred_text) else {
                        continue;
                    };
                    let mut tok = cred
                        .pointer("/claudeAiOauth/accessToken")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let rtok = cred
                        .pointer("/claudeAiOauth/refreshToken")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let expires_at = cred
                        .pointer("/claudeAiOauth/expiresAt")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or(0);
                    let tok_valid = !tok.is_empty()
                        && expires_at > now_ms + 300_000
                        && http
                            .get("https://api.anthropic.com/api/oauth/usage")
                            .header("authorization", &format!("Bearer {tok}"))
                            .header("anthropic-beta", "oauth-2025-04-20")
                            .header("cache-control", "no-cache")
                            .header("user-agent", "claude-code/2.1.310")
                            .call()
                            .is_ok();
                    let last_attempt_ms = std::fs::read_to_string(
                        prof_dir.join(".anyagents-oauth-refresh-attempt-ms"),
                    )
                    .ok()
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .unwrap_or(0);
                    let mut refreshed_token = false;
                    if !tok_valid
                        && !rtok.is_empty()
                        && now_ms.saturating_sub(last_attempt_ms) >= 300_000
                    {
                        let _ = std::fs::write(
                            prof_dir.join(".anyagents-oauth-refresh-attempt-ms"),
                            format!("{now_ms}\n"),
                        );
                        let req_body = serde_json::json!({
                            "grant_type": "refresh_token",
                            "refresh_token": rtok,
                            "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
                        })
                        .to_string();
                        if let Some(rval) = post_anthropic_oauth_refresh(&http, &req_body)
                            && let Some(new_acc) = rval
                                .get("access_token")
                                .and_then(serde_json::Value::as_str)
                                .map(|s| s.trim().to_string())
                            && !new_acc.is_empty()
                        {
                            tok = new_acc.clone();
                            refreshed_token = true;
                            if let Some(oauth) = cred
                                .get_mut("claudeAiOauth")
                                .and_then(serde_json::Value::as_object_mut)
                            {
                                oauth.insert(
                                    "accessToken".to_string(),
                                    serde_json::Value::String(new_acc),
                                );
                                if let Some(new_ref) = rval
                                    .get("refresh_token")
                                    .and_then(serde_json::Value::as_str)
                                {
                                    oauth.insert(
                                        "refreshToken".to_string(),
                                        serde_json::Value::String(new_ref.to_string()),
                                    );
                                }
                                if let Some(exp_in) =
                                    rval.get("expires_in").and_then(serde_json::Value::as_i64)
                                {
                                    oauth.insert(
                                        "expiresAt".to_string(),
                                        serde_json::Value::Number((now_ms + exp_in * 1000).into()),
                                    );
                                }
                            }
                        }
                    }
                    if !tok.is_empty() {
                        let _ = sync_anthropic_profile_metadata(
                            &http, data, &prof_dir, &profile, &tok, &mut cred, true,
                        );
                        if refreshed_token && let Ok(updated) = serde_json::to_string(&cred) {
                            persist_refreshed_credentials(
                                &prof_dir,
                                &[".credentials.json", ".claude/.credentials.json"],
                                updated.as_bytes(),
                            )?;
                            propagate_profile_files_to_bound_agents(
                                data,
                                "anthropic",
                                &profile,
                                &[".credentials.json", ".claude/.credentials.json"],
                            )?;
                        }
                    }
                }
                "openai" => {
                    let Ok(auth_text) = std::fs::read_to_string(prof_dir.join("auth.json"))
                        .or_else(|_| std::fs::read_to_string(prof_dir.join(".codex/auth.json")))
                    else {
                        continue;
                    };
                    let Ok(mut auth) = serde_json::from_str::<serde_json::Value>(&auth_text) else {
                        continue;
                    };
                    let rtok = auth
                        .pointer("/tokens/refresh_token")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if !rtok.is_empty() {
                        let req_body = serde_json::json!({
                            "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
                            "grant_type": "refresh_token",
                            "refresh_token": rtok,
                        })
                        .to_string();
                        if let Ok(rresp) = http
                            .post("https://auth.openai.com/oauth/token")
                            .header("content-type", "application/json")
                            .send(req_body.as_bytes())
                            && let Ok(rbody) = rresp.into_body().read_to_string()
                            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
                            && let Some(new_acc) = rval
                                .get("access_token")
                                .and_then(serde_json::Value::as_str)
                                .map(|s| s.trim().to_string())
                            && !new_acc.is_empty()
                            && let Some(tokens_obj) = auth
                                .get_mut("tokens")
                                .and_then(serde_json::Value::as_object_mut)
                        {
                            tokens_obj.insert(
                                "access_token".to_string(),
                                serde_json::Value::String(new_acc),
                            );
                            if let Some(new_ref) = rval
                                .get("refresh_token")
                                .and_then(serde_json::Value::as_str)
                            {
                                tokens_obj.insert(
                                    "refresh_token".to_string(),
                                    serde_json::Value::String(new_ref.to_string()),
                                );
                            }
                            if let Ok(updated) = serde_json::to_string_pretty(&auth) {
                                persist_refreshed_credentials(
                                    &prof_dir,
                                    &["auth.json", ".codex/auth.json"],
                                    updated.as_bytes(),
                                )?;
                                propagate_profile_files_to_bound_agents(
                                    data,
                                    "openai",
                                    &profile,
                                    &["auth.json", ".codex/auth.json"],
                                )?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Clears historical `CORTEX_STEP_TYPE_ERROR_MESSAGE` (`step_type = 17`) rows
/// in `<config_dir>/.gemini/antigravity-cli/conversations/*.db` so `agy` does not
/// re-emit stale `"status":"ERROR","error":"...Resets in..."` result lines at the
/// end of subsequent healthy turns via `StepIndicesByType(17)`.
pub(crate) fn clear_stale_agy_quota_error_steps(config_dir: &Path) {
    let conv_dir = config_dir
        .join(".gemini")
        .join("antigravity-cli")
        .join("conversations");
    if !conv_dir.is_dir() {
        return;
    }
    #[cfg(unix)]
    if !crate::sandbox::is_isolated_test_env()
        && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
        && let Ok(conv_meta) = std::fs::metadata(&conv_dir)
    {
        use std::os::unix::fs::MetadataExt as _;
        let (my_uid, my_gid) = std::fs::metadata("/proc/self")
            .map(|m| (m.uid(), m.gid()))
            .unwrap_or((conv_meta.uid(), conv_meta.gid()));
        let mut seat_uid_gid: Option<(u32, u32)> = None;
        if conv_meta.uid() != my_uid {
            seat_uid_gid = Some((conv_meta.uid(), conv_meta.gid()));
        } else if conv_meta.gid() != my_gid {
            seat_uid_gid = Some((conv_meta.gid(), conv_meta.gid()));
        }
        if let Ok(entries) = std::fs::read_dir(&conv_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("db")
                    && let Ok(m) = entry.metadata()
                    && m.uid() != my_uid
                {
                    seat_uid_gid = Some((m.uid(), m.gid()));
                    break;
                }
            }
        }
        if let Some((target_uid, target_gid)) = seat_uid_gid {
            let sock_gid = config_dir
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| std::fs::metadata(p).ok())
                .map(|m| m.gid())
                .unwrap_or(my_gid);
            let uid_s = target_uid.to_string();
            let gid_s = target_gid.to_string();
            let groups_s = format!("{target_gid},{sock_gid},{my_gid}");
            let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
                .args([
                    "--reuid",
                    &uid_s,
                    "--regid",
                    &gid_s,
                    "--groups",
                    &groups_s,
                    "--no-new-privs",
                    "--",
                    "python3",
                    "-c",
                    "import glob, os, sqlite3, sys\nfor p in glob.glob(os.path.join(sys.argv[1], '*.db')):\n    try:\n        c = sqlite3.connect(p, timeout=0.5)\n        if c.execute('SELECT 1 FROM steps WHERE step_type = 17 LIMIT 1').fetchone():\n            c.execute('UPDATE steps SET step_type = 0, status = 5, error_details = NULL, step_payload = ? WHERE step_type = 17', (b' \\x05*\\x03\\xd2\\x01\\x00',))\n            c.commit()\n        c.close()\n    except Exception:\n        pass\n",
                ])
                .arg(&conv_dir)
                .stderr(std::process::Stdio::null())
                .status();
            return;
        }
    }
    let Ok(entries) = std::fs::read_dir(&conv_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }
        if let Ok(conn) = rusqlite::Connection::open(&path) {
            let _ = conn.busy_timeout(std::time::Duration::from_millis(500));
            let _ = conn.execute(
                "UPDATE steps SET step_type = 0, status = 5, error_details = NULL, step_payload = X'20052a03d20100' WHERE step_type = 17",
                [],
            );
        }
    }
}

/// Probes the exact model catalog and per-model quota availability for upstream
/// provider profiles (especially `google-antigravity/<profile>`).
pub(crate) fn probe_profile_models_catalog(data: &Path, target: Option<&str>) -> serde_json::Value {
    let filter = target.map(str::trim).filter(|s| {
        !s.is_empty() && !s.eq_ignore_ascii_case("models") && !s.eq_ignore_ascii_case("all")
    });
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(6)))
        .build()
        .new_agent();

    let mut agy_cli_models = Vec::new();
    let agy_bin = crate::sandbox::host_home_dir()
        .map(|h| h.join(".local/bin/agy"))
        .filter(|p| p.is_file())
        .unwrap_or_else(|| PathBuf::from("agy"));
    if let Ok(out) = std::process::Command::new(&agy_bin).arg("models").output()
        && out.status.success()
    {
        let stdout = String::from_utf8_lossy(&out.stdout);
        for line in stdout.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("Available models:") {
                continue;
            }
            let (left, right) = trimmed.split_once("  ").unwrap_or((trimmed, ""));
            let id = left.trim();
            if id.is_empty() {
                continue;
            }
            let is_default = right.contains("(default)");
            let label = right.replace("(default)", "").trim().to_string();
            agy_cli_models.push(serde_json::json!({
                "id": id,
                "label": label,
                "default": is_default,
            }));
        }
    }

    let mut profiles_out = Vec::new();
    let profiles_root = data.join("profiles");
    if let Ok(providers) = std::fs::read_dir(&profiles_root) {
        let mut prov_list: Vec<_> = providers.flatten().collect();
        prov_list.sort_by_key(|e| e.file_name());
        for prov_entry in prov_list {
            let prov_path = prov_entry.path();
            if !prov_path.is_dir() {
                continue;
            }
            let provider = prov_entry.file_name().to_string_lossy().into_owned();
            if filter.is_none() && provider != "google-antigravity" {
                continue;
            }
            let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
                continue;
            };
            let mut prof_list: Vec<_> = prof_dirs.flatten().collect();
            prof_list.sort_by_key(|e| e.file_name());
            for prof_entry in prof_list {
                let prof_dir = prof_entry.path();
                if !prof_dir.is_dir() {
                    continue;
                }
                let profile = prof_entry.file_name().to_string_lossy().into_owned();
                let full_name = format!("{provider}/{profile}");
                if let Some(f) = filter
                    && f != provider
                    && f != profile
                    && f != full_name
                    && !(f == "agy" && provider == "google-antigravity")
                {
                    continue;
                }
                if provider == "google-antigravity" {
                    let _ = probe_one_profile_usage(data, &provider, &profile);
                    let tok_path = prof_dir.join(".gemini/antigravity-cli/antigravity-oauth-token");
                    let mut backend_models = Vec::new();
                    if let Ok(tok_text) = std::fs::read_to_string(&tok_path)
                        && let Ok(tok_val) = serde_json::from_str::<serde_json::Value>(&tok_text)
                        && let Some(tok) = tok_val
                            .pointer("/token/access_token")
                            .or_else(|| tok_val.get("access_token"))
                            .and_then(|v| v.as_str())
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                        && let Ok(resp) = http
                            .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels")
                            .header("authorization", &format!("Bearer {tok}"))
                            .header("content-type", "application/json")
                            .header("user-agent", "antigravity")
                            .send(b"{}")
                        && let Ok(body) = resp.into_body().read_to_string()
                        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&body)
                        && let Some(models_obj) = val.get("models").and_then(|m| m.as_object())
                    {
                        for (model_id, m) in models_obj {
                            let qi = m.get("quotaInfo");
                            let rem_pct = qi
                                .and_then(|q| q.get("remainingFraction"))
                                .and_then(|v| v.as_f64())
                                .map(|f| (f * 100.0).clamp(0.0, 100.0).round() as i64)
                                .unwrap_or(0);
                            let reset_time = qi
                                .and_then(|q| q.get("resetTime"))
                                .and_then(|v| v.as_str());
                            backend_models.push(serde_json::json!({
                                "id": model_id,
                                "display_name": m.get("displayName").and_then(|v| v.as_str()),
                                "remaining_pct": rem_pct,
                                "reset_time": reset_time,
                            }));
                        }
                    }
                    profiles_out.push(serde_json::json!({
                        "profile": full_name,
                        "provider": provider,
                        "cli_models": agy_cli_models,
                        "backend_models": backend_models,
                    }));
                }
            }
        }
    }

    serde_json::json!({
        "agy_cli_models": agy_cli_models,
        "profiles": profiles_out,
    })
}

/// Checks if `agent`'s currently bound profile has recovered quota (`> 5% left`
/// after a plan upgrade or vendor mid-window reset) OR is exhausted (`0% left`)
/// while another profile under the same upstream provider has healthy quota (`>= 20%`).
/// Rate-limited by `.anyagents-last-quota-check-ms` (120s cooldown) when called
/// during status reconciliation so background checks never spam vendor APIs.
pub(crate) fn try_rotate_exhausted_profile(
    store: &Store,
    data: &Path,
    agent: &Agent,
    has_active_quota_hold: bool,
) -> Option<String> {
    if crate::sandbox::is_isolated_test_env() {
        return None;
    }
    let cfg = agent_config_dir(data, &agent.id);
    let now_ms = anyagents_core::now_ms();
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let current_prof = bound_profile_name(&cfg)
        .and_then(|b| {
            let p = b.split_once('/').map(|(_, p)| p.to_string()).unwrap_or(b);
            (!p.is_empty()).then_some(p)
        })
        .unwrap_or_else(|| "default".to_string());
    let prof_dir = data.join("profiles").join(&provider).join(&current_prof);
    let rotated_ms = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let cleared_ms = std::fs::read_to_string(cfg.join(".anyagents-quota-cleared-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
        .max(rotated_ms);
    if has_active_quota_hold {
        let last_check = std::fs::read_to_string(cfg.join(".anyagents-last-quota-check-ms"))
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0);
        if now_ms.saturating_sub(last_check) < 120_000 {
            if cleared_ms >= last_check && now_ms.saturating_sub(cleared_ms) < 120_000 {
                return Some(format!("{provider}/{current_prof}"));
            }
            return None;
        }
        write_profile_marker(
            &cfg,
            ".anyagents-last-quota-check-ms",
            &format!("{now_ms}\n"),
        )
        .map_err(|err| eprintln!("runtime persistence failed: {err}"))
        .ok()?;
    }
    let post_clear_error = std::fs::read_to_string(cfg.join(".anyagents-last-error.json"))
        .ok()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .is_some_and(|val| {
            let ts = val
                .get("timestamp_ms")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            let q = val
                .get("quota_reset_at_ms")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            cleared_ms > 0 && ts > cleared_ms && q > now_ms && now_ms.saturating_sub(ts) < 900_000
        });
    let cur_pct = if post_clear_error {
        Some(0)
    } else {
        probe_profile_remaining_pct(data, &provider, &current_prof, agent.model.as_deref())
    };
    if has_active_quota_hold && cur_pct.is_some_and(|p| p > 0) {
        write_profile_marker(&cfg, ".anyagents-quota-cleared-ms", &format!("{now_ms}\n"))
            .map_err(|err| eprintln!("runtime persistence failed: {err}"))
            .ok()?;
        let _ = std::fs::remove_file(cfg.join(".anyagents-last-error.json"));
        let _ = std::fs::remove_file(prof_dir.join(".anyagents-quota-reset-ms"));
        if agent.harness == "agy" {
            clear_stale_agy_quota_error_steps(&cfg);
        }
        poke_agent_wake_socket(data, &agent.id);
        return Some(format!("{provider}/{current_prof}"));
    }
    if has_active_quota_hold && cur_pct.is_some_and(|pct| pct <= 0) {
        let bound_counts = count_bound_profile_agents(
            data,
            &store.list_agents().unwrap_or_default(),
            &provider,
            &agent.id,
        );
        let mut best: Option<(String, i64, i64)> = None;
        for (cand_prof, _) in list_harness_profiles(data, &provider) {
            if cand_prof == current_prof {
                continue;
            }
            if let Some(rem_pct) =
                probe_profile_remaining_pct(data, &provider, &cand_prof, agent.model.as_deref())
                && rem_pct > 0
            {
                let cnt = bound_counts.get(&cand_prof).copied().unwrap_or(0);
                let score = (rem_pct * 100) / (cnt + 1);
                if best.as_ref().is_none_or(|(_, best_score, best_rem)| {
                    (score, rem_pct) > (*best_score, *best_rem)
                }) {
                    best = Some((cand_prof, score, rem_pct));
                }
            }
        }
        if let Some((best_prof, _, _)) = best {
            let target_prof = format!("{provider}/{best_prof}");
            if configure_agent_runtime(
                store,
                data,
                "system:quota-rotate",
                &agent.name,
                None,
                None,
                Some(&target_prof),
            )
            .is_ok()
            {
                return Some(target_prof);
            }
        }
    }
    None
}

fn count_bound_profile_agents(
    data: &Path,
    agents: &[Agent],
    provider: &str,
    excluded_agent: &AgentId,
) -> std::collections::HashMap<String, i64> {
    let mut counts = std::collections::HashMap::new();
    for agent in agents {
        if agent.state != "archived" && &agent.id != excluded_agent {
            let cfg = agent_config_dir(data, &agent.id);
            if let Some(bound) = bound_profile_name(&cfg) {
                let (bound_provider, profile) = bound
                    .split_once('/')
                    .map(|(p, n)| (p.to_string(), n.to_string()))
                    .unwrap_or_else(|| {
                        (
                            upstream_provider_of(&agent.harness, agent.model.as_deref()),
                            bound.clone(),
                        )
                    });
                if bound_provider == provider {
                    *counts.entry(profile).or_insert(0) += 1;
                }
            }
        }
    }
    counts
}

/// Lists all available Master Profiles for `provider_or_harness` under `<data>/profiles/<provider>/<profile>`.
pub(crate) fn list_harness_profiles(
    data: &Path,
    provider_or_harness: &str,
) -> Vec<(String, PathBuf)> {
    let provider = match provider_or_harness {
        "claude-code" | "codex" | "agy" | "muse" | "dsh" | "grok" | "opencode" | "omp" => {
            upstream_provider_of(provider_or_harness, None)
        }
        other => other.to_string(),
    };
    let mut out: Vec<(String, PathBuf)> = Vec::new();
    for base in [
        data.join("profiles").join(&provider),
        data.join("profiles").join(provider_or_harness),
        data.join("config").join(&provider),
        data.join("config").join(provider_or_harness),
    ] {
        let Ok(entries) = std::fs::read_dir(&base) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            if !out.iter().any(|(n, _)| n == &name) {
                out.push((name, path));
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Returns the profile marker name bound to an agent's config directory, if any.
pub(crate) fn bound_profile_name(config_dir: &Path) -> Option<String> {
    std::fs::read_to_string(config_dir.join(".anyagents-profile"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Selects (or reuses the already-bound) Upstream Provider Profile for `agent`
/// from `<data>/profiles/<provider>/<profile>` with zero automatic load-balancing
/// ("分流"): an agent uses its explicitly assigned `.anyagents-profile` (defaulting
/// deterministically to `<provider>/default`).
fn select_or_bind_profile(
    data: &Path,
    _all_agents: &[Agent],
    agent: &Agent,
    target_config: &Path,
) -> Vec<PathBuf> {
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let available = list_harness_profiles(data, &provider);
    if let Some(existing) = bound_profile_name(target_config) {
        let prof_name = existing
            .split_once('/')
            .map(|(_, p)| p)
            .unwrap_or(existing.as_str());
        if let Some((_, dir)) = available.iter().find(|(n, _)| n == prof_name) {
            return vec![dir.clone()];
        }
        let exact_dir = data.join("profiles").join(&provider).join(prof_name);
        if exact_dir.is_dir() {
            return vec![exact_dir];
        }
    }
    let chosen_name = if available.iter().any(|(n, _)| n == "default") || available.is_empty() {
        "default".to_string()
    } else {
        available[0].0.clone()
    };
    write_profile_marker(
        target_config,
        ".anyagents-profile",
        &format!("{provider}/{chosen_name}\n"),
    )
    .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
    vec![data.join("profiles").join(&provider).join(chosen_name)]
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct SubagentDefaultSpec {
    /// `"inherit"` or `"preset"`.
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SubagentDefaultsConfig {
    pub default: SubagentDefaultSpec,
    #[serde(default)]
    pub agents: std::collections::BTreeMap<String, SubagentDefaultSpec>,
}

impl Default for SubagentDefaultsConfig {
    fn default() -> Self {
        Self {
            default: SubagentDefaultSpec {
                mode: "inherit".to_string(),
                harness: None,
                model: None,
                profile: None,
            },
            agents: std::collections::BTreeMap::new(),
        }
    }
}

pub(crate) fn load_subagent_defaults(data: &Path) -> SubagentDefaultsConfig {
    let path = data.join("subagent-defaults.json");
    let mut cfg = std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| serde_json::from_str::<SubagentDefaultsConfig>(&text).ok())
        .unwrap_or_default();
    normalize_subagent_harnesses(&mut cfg);
    cfg
}

fn normalize_subagent_harnesses(cfg: &mut SubagentDefaultsConfig) {
    for spec in std::iter::once(&mut cfg.default).chain(cfg.agents.values_mut()) {
        if let Some(harness) = spec.harness.as_mut() {
            *harness = normalize_harness_name(harness).to_string();
        }
    }
}

pub(crate) fn save_subagent_defaults(
    data: &Path,
    cfg: &SubagentDefaultsConfig,
) -> Result<(), Error> {
    let path = data.join("subagent-defaults.json");
    let mut cfg = cfg.clone();
    normalize_subagent_harnesses(&mut cfg);
    let text = serde_json::to_string_pretty(&cfg).map_err(|err| Error::Message(err.to_string()))?;
    std::fs::write(path, format!("{text}\n"))?;
    Ok(())
}

/// Parsed archetype template loaded from `<archetype>.md` or `<archetype>/agent.md`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ArchetypeTemplate {
    pub description: Option<String>,
    pub scope: Option<String>,
    pub workdir: Option<String>,
    pub harness: Option<String>,
    pub model: Option<String>,
    pub role: Option<String>,
    pub profile: Option<String>,
    pub body: String,
}

/// Parse optional YAML frontmatter (`description`, `scope`, `workdir`, `harness`, `model`,
/// `role`, `profile`) and markdown body from an archetype `.md` file.
///
/// Templates are configuration, rather than best-effort prose.  In particular, once a
/// document starts with a frontmatter delimiter, an incomplete or malformed delimiter must
/// not turn the rest of the file into an instruction body by accident.
pub(crate) fn parse_archetype_markdown(raw: &str) -> Result<ArchetypeTemplate, Error> {
    let mut tpl = ArchetypeTemplate::default();
    let normalized = raw.replace("\r\n", "\n");
    let trimmed_start = normalized.trim_start_matches('\u{feff}');
    let body = if let Some(rest) = trimmed_start.strip_prefix("---\n") {
        let mut offset = 0;
        let mut closing = None;
        for line in rest.split_inclusive('\n') {
            if line.trim_end_matches('\n') == "---" {
                closing = Some((offset, offset + line.len()));
                break;
            }
            offset += line.len();
        }
        let (start, end) = closing.ok_or_else(|| {
            Error::Message("archetype template has unclosed YAML frontmatter".to_string())
        })?;
        let frontmatter = &rest[..start];
        let invalid_yaml = |err: serde_yaml_ng::Error| {
            // YAML errors can quote the input. Do not reflect private template
            // contents into a validation response.
            Error::Message(format!(
                "archetype template has malformed YAML frontmatter{}",
                err.location()
                    .map(|loc| format!(" at line {}", loc.line()))
                    .unwrap_or_default()
            ))
        };
        let fields: std::collections::BTreeMap<String, String> = if frontmatter
            .lines()
            .all(|line| line.trim().is_empty() || line.trim().starts_with('#'))
        {
            std::collections::BTreeMap::new()
        } else {
            // Value's mapping parser also rejects duplicate keys; a direct
            // BTreeMap deserializer would silently overwrite them.
            let mapping: serde_yaml_ng::Value =
                serde_yaml_ng::from_str(frontmatter).map_err(invalid_yaml)?;
            serde_yaml_ng::from_value(mapping).map_err(invalid_yaml)?
        };
        if fields.is_empty() && rest[end..].trim().is_empty() {
            return Err(Error::Message(
                "archetype template frontmatter requires fields or a markdown body".to_string(),
            ));
        }
        for (key, value) in fields {
            let value = value.trim();
            if key.trim().is_empty() || value.is_empty() {
                return Err(Error::Message(
                    "archetype template has an empty YAML key or value".to_string(),
                ));
            }
            match key.to_ascii_lowercase().as_str() {
                "description" | "summary" => tpl.description = Some(value.to_string()),
                "scope" => tpl.scope = Some(anyagents_core::parse_agent_scope(value)?.to_string()),
                "workdir" | "workspace" => tpl.workdir = Some(value.to_string()),
                "harness" => tpl.harness = Some(normalize_harness_name(value).to_string()),
                "model" => tpl.model = Some(value.to_string()),
                "role" => tpl.role = Some(value.to_string()),
                "profile" => tpl.profile = Some(value.to_string()),
                "name" | "archetype" => {}
                _ => {
                    return Err(Error::Message(format!(
                        "unknown archetype template frontmatter key {key:?}"
                    )));
                }
            }
        }
        &rest[end..]
    } else {
        trimmed_start
    };
    tpl.body = body.trim().to_string();
    Ok(tpl)
}

/// Resolve an archetype template by name across:
/// 1. `<explicit_workdir>/.agents/agents/<archetype>.md` (or `<archetype>/agent.md`)
/// 2. `<parent_workdir>/.agents/agents/<archetype>.md` (or `<archetype>/agent.md`)
/// 3. `<data_dir>/.agents/agents/<archetype>.md` (then `<data_dir>/agents-templates/`, `<data_dir>/archetypes/`)
/// 4. `<HOME>/.agents/agents/<archetype>.md` (and `owner_workdir()/.agents/agents/`)
pub(crate) fn resolve_archetype_template(
    archetype: &str,
    explicit_workdir: Option<&Path>,
    parent_workdir: Option<&Path>,
    data_dir: &Path,
) -> Result<Option<ArchetypeTemplate>, Error> {
    let slug = archetype.trim();
    if !valid_template_stem(slug) {
        return Err(Error::Message(format!(
            "invalid archetype template name {archetype:?}; expected a single filename stem"
        )));
    }

    let mut search_roots: Vec<(PathBuf, PathBuf)> = Vec::new();
    if let Some(wd) = explicit_workdir {
        let s = wd.to_string_lossy();
        if !matches!(s.trim(), "" | "worktree" | "branch" | "private" | "inherit") {
            search_roots.push((wd.to_path_buf(), PathBuf::from(".agents/agents")));
        }
    }
    if let Some(pwd) = parent_workdir {
        search_roots.push((pwd.to_path_buf(), PathBuf::from(".agents/agents")));
    }
    search_roots.push((data_dir.to_path_buf(), PathBuf::from(".agents/agents")));
    search_roots.push((data_dir.to_path_buf(), PathBuf::from("agents-templates")));
    search_roots.push((data_dir.to_path_buf(), PathBuf::from("archetypes")));
    #[cfg(not(test))]
    {
        if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
            search_roots.push((home, PathBuf::from(".agents/agents")));
        }
        if let Some(owd) = crate::server::owner_workdir() {
            search_roots.push((owd, PathBuf::from(".agents/agents")));
        }
    }

    let candidates = [
        format!("{slug}.md"),
        format!("{}/agent.md", slug),
        format!("{}/AGENT.md", slug),
    ];

    for (base, root) in search_roots {
        for rel in &candidates {
            if let Some(raw) = read_archetype_file(&base, &root.join(rel))? {
                return parse_archetype_markdown(&raw).map(Some);
            }
        }
    }
    Ok(None)
}

/// Read a template without ever following a symlink in the candidate path.  The
/// check is intentionally performed for every lookup, including implicit role
/// lookup, so a hostile `.agents/agents` cannot be bypassed by falling through to
/// another search root.
fn read_archetype_file(base: &Path, relative: &Path) -> Result<Option<String>, Error> {
    let inspect = |component: &Path| -> Result<bool, Error> {
        let metadata = match std::fs::symlink_metadata(component) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                return Err(Error::Message(format!(
                    "cannot inspect archetype template path {}: {err}",
                    component.display()
                )));
            }
        };
        if metadata.file_type().is_symlink() {
            return Err(Error::Message(format!(
                "archetype template path contains a symlink: {}",
                component.display()
            )));
        }
        Ok(true)
    };
    if !inspect(base)? {
        return Ok(None);
    }
    // Ancestors above the authorized base can contain OS aliases (e.g. macOS
    // /var -> /private/var). Resolve those once, then refuse every symlink from
    // the base through the template file itself.
    let canonical_base = std::fs::canonicalize(base).map_err(|err| {
        Error::Message(format!(
            "cannot resolve archetype base {}: {err}",
            base.display()
        ))
    })?;
    let mut component = canonical_base.clone();
    for part in relative.components() {
        component.push(part);
        if !inspect(&component)? {
            return Ok(None);
        }
    }
    let path = canonical_base.join(relative);
    let path = path.as_path();

    let template_dir = path.parent().ok_or_else(|| {
        Error::Message(format!(
            "archetype template has no containing directory: {}",
            path.display()
        ))
    })?;
    let canonical_dir = std::fs::canonicalize(template_dir).map_err(|err| {
        Error::Message(format!(
            "cannot resolve archetype template directory {}: {err}",
            template_dir.display()
        ))
    })?;
    let canonical_file = std::fs::canonicalize(path).map_err(|err| {
        Error::Message(format!(
            "cannot resolve archetype template {}: {err}",
            path.display()
        ))
    })?;
    if canonical_file.parent() != Some(canonical_dir.as_path()) {
        return Err(Error::Message(format!(
            "archetype template escapes its authorized directory: {}",
            path.display()
        )));
    }

    let file = open_archetype_nofollow(path).map_err(|err| {
        Error::Message(format!(
            "cannot open archetype template {} without following symlinks: {err}",
            path.display()
        ))
    })?;
    if !file.metadata()?.is_file() {
        return Err(Error::Message(format!(
            "archetype template is not a regular file: {}",
            path.display()
        )));
    }
    let mut raw = String::new();
    use std::io::Read;
    std::io::BufReader::new(file)
        .read_to_string(&mut raw)
        .map_err(|err| {
            Error::Message(format!(
                "cannot read archetype template {}: {err}",
                path.display()
            ))
        })?;
    Ok(Some(raw))
}

// Pin each parent directory by descriptor. O_NOFOLLOW on the final file alone
// would still allow an intermediate directory to be swapped for a symlink after
// symlink_metadata has checked it.
#[cfg(unix)]
fn open_archetype_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Component;

    let mut directory = std::fs::File::open(if path.is_absolute() { "/" } else { "." })?;
    let mut parts = path
        .components()
        .filter(|part| !matches!(part, Component::RootDir))
        .peekable();
    while let Some(part) = parts.next() {
        let name = std::ffi::CString::new(part.as_os_str().as_bytes())?;
        let mut flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
        if parts.peek().is_some() {
            flags |= libc::O_DIRECTORY;
        }
        // SAFETY: name is a NUL-terminated path component and the borrowed
        // directory descriptor is live. A successful descriptor is owned once.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: openat returned a new descriptor and ownership is transferred.
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        if parts.peek().is_none() {
            return Ok(file);
        }
        directory = file;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "empty template path",
    ))
}

#[cfg(not(unix))]
fn open_archetype_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

/// Apply archetype template defaults (`.agents/agents/<archetype>.md`) to a `CreateAgentRequest`
/// in-place before agent creation. When `archetype` is omitted, also checks whether `role`
/// matches a template slug (e.g. `role: "code-reviewer"` when `.agents/agents/code-reviewer.md`
/// exists) while keeping `role` methodology-neutral when no template matches.
pub(crate) fn apply_archetype_defaults(
    data_dir: &Path,
    parent: Option<&Agent>,
    body: &mut crate::api::CreateAgentRequest,
) -> Result<(), Error> {
    let explicit_wd = body
        .workdir
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);
    let parent_wd = parent.and_then(|p| p.workdir.as_deref()).map(Path::new);
    let tpl = if let Some(arch) = body
        .archetype
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        match resolve_archetype_template(arch, explicit_wd.as_deref(), parent_wd, data_dir)? {
            Some(template) => Some(template),
            None => {
                return Err(Error::Message(format!(
                    "unknown archetype template {arch:?}"
                )));
            }
        }
    } else if let Some(role) = body
        .role
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let slugified = role.to_ascii_lowercase().replace(' ', "-");
        let direct = if valid_template_stem(role) {
            resolve_archetype_template(role, explicit_wd.as_deref(), parent_wd, data_dir)?
        } else {
            None
        };
        let resolved = if direct.is_some() {
            direct
        } else if valid_template_stem(&slugified) {
            resolve_archetype_template(&slugified, explicit_wd.as_deref(), parent_wd, data_dir)?
        } else {
            None
        };
        if resolved.is_some() {
            resolved
        } else if let Some(fallback_slug) = fallback_archetype_slug_from_role(role) {
            resolve_archetype_template(fallback_slug, explicit_wd.as_deref(), parent_wd, data_dir)?
        } else {
            // A display label is not required to be a template stem.
            None
        }
    } else {
        None
    };
    let Some(tpl) = tpl else {
        return Ok(());
    };

    if body
        .description
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && let Some(desc) = tpl.description
    {
        body.description = Some(desc);
    }
    if body
        .scope
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && body.public.is_none()
        && let Some(scope) = tpl.scope
    {
        body.scope = Some(scope);
    }
    if body
        .workdir
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && body
            .repo
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
        && let Some(wd) = tpl.workdir
    {
        body.workdir = Some(wd);
    }
    if body.harness.as_deref().is_none_or(|h| h.trim().is_empty())
        && let Some(h) = tpl.harness
    {
        body.harness = Some(h);
    }
    if body
        .model
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && let Some(m) = tpl.model
    {
        body.model = Some(m);
    }
    if body
        .profile
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && let Some(p) = tpl.profile
    {
        body.profile = Some(p);
    }
    if body
        .role
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
        && let Some(r) = tpl.role
    {
        body.role = Some(r);
    }
    if !tpl.body.is_empty() {
        body.instructions = match body
            .instructions
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(existing) => Some(format!("{}\n\n{}", tpl.body, existing)),
            None => Some(tpl.body),
        };
    }
    Ok(())
}

fn fallback_archetype_slug_from_role(role: &str) -> Option<&'static str> {
    let lower = role.to_ascii_lowercase();
    if lower.contains("director") || role.contains("總監") || role.contains("總管") {
        Some("director")
    } else if lower.contains("manager") || role.contains("主管") {
        Some("manager")
    } else if lower.contains("assistant") || role.contains("專屬助理") {
        Some("assistant")
    } else if lower.contains("worker") || role.contains("專員") {
        Some("worker")
    } else {
        None
    }
}

fn valid_template_stem(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
}

/// Resolve `(harness, model, profile)` for a newly created agent or sub-agent.
pub(crate) fn resolve_subagent_defaults(
    data: &Path,
    parent: Option<&Agent>,
    raw_harness: &str,
    raw_model: Option<String>,
    raw_profile: Option<String>,
) -> (String, Option<String>, Option<String>) {
    let h_trim = normalize_harness_name(raw_harness);
    let m_trim = raw_model
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let p_trim = raw_profile
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let Some(parent_agent) = parent else {
        let harness = if h_trim.is_empty() || h_trim.eq_ignore_ascii_case("inherit") {
            "claude-code".to_string()
        } else {
            h_trim.to_string()
        };
        let model = m_trim
            .filter(|m| !m.eq_ignore_ascii_case("inherit"))
            .map(str::to_string)
            .or_else(|| {
                (harness == "spiron").then(|| crate::spiron::DEFAULT_SPIRON_MODEL.to_string())
            });
        let profile = p_trim
            .filter(|p| !p.eq_ignore_ascii_case("inherit"))
            .map(str::to_string);
        return (harness, model, profile);
    };

    let cfg = load_subagent_defaults(data);
    let spec = cfg.agents.get(&parent_agent.name).unwrap_or(&cfg.default);
    let parent_cfg_dir = agent_config_dir(data, &parent_agent.id);
    let parent_bound_profile = bound_profile_name(&parent_cfg_dir);

    let inherit_harness = || {
        if parent_agent.harness == "agy" {
            "spiron".to_string()
        } else {
            normalize_harness_name(&parent_agent.harness).to_string()
        }
    };

    let explicit_inherit = h_trim.eq_ignore_ascii_case("inherit")
        || m_trim.is_some_and(|m| m.eq_ignore_ascii_case("inherit"))
        || p_trim.is_some_and(|p| p.eq_ignore_ascii_case("inherit"));

    let use_inherit = explicit_inherit || spec.mode.eq_ignore_ascii_case("inherit");

    let resolved_harness = if !h_trim.is_empty() && !h_trim.eq_ignore_ascii_case("inherit") {
        h_trim.to_string()
    } else if h_trim.eq_ignore_ascii_case("inherit") || use_inherit {
        inherit_harness()
    } else {
        spec.harness
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("inherit"))
            .map(str::to_string)
            .unwrap_or_else(inherit_harness)
    };

    let resolved_model = if let Some(m) = m_trim {
        if m.eq_ignore_ascii_case("inherit") {
            parent_agent.model.clone()
        } else {
            Some(m.to_string())
        }
    } else if use_inherit {
        if resolved_harness == parent_agent.harness {
            parent_agent.model.clone()
        } else {
            None
        }
    } else if h_trim.is_empty() || spec.harness.as_deref() == Some(resolved_harness.as_str()) {
        spec.model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(|m| {
                if m.eq_ignore_ascii_case("inherit") {
                    parent_agent.model.clone()
                } else {
                    Some(m.to_string())
                }
            })
    } else {
        None
    }
    .or_else(|| {
        (resolved_harness == "spiron").then(|| {
            spec.model
                .as_deref()
                .map(str::trim)
                .filter(|m| m.starts_with("sylphx/"))
                .map(str::to_string)
                .unwrap_or_else(|| crate::spiron::DEFAULT_SPIRON_MODEL.to_string())
        })
    });

    let target_provider = upstream_provider_of(&resolved_harness, resolved_model.as_deref());
    let inherit_profile_if_compatible = || {
        parent_bound_profile.as_ref().and_then(|prof| {
            let prov = prof
                .split_once('/')
                .map(|(p, _)| p)
                .unwrap_or(prof.as_str());
            (prov == target_provider).then(|| prof.clone())
        })
    };

    let resolved_profile = if let Some(p) = p_trim {
        if p.eq_ignore_ascii_case("inherit") {
            inherit_profile_if_compatible()
        } else {
            Some(p.to_string())
        }
    } else if use_inherit {
        inherit_profile_if_compatible()
    } else {
        spec.profile
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(|p| {
                if p.eq_ignore_ascii_case("inherit") {
                    inherit_profile_if_compatible()
                } else {
                    let prov = p
                        .split_once('/')
                        .map(|(pr, _)| pr)
                        .unwrap_or(&target_provider);
                    (prov == target_provider).then(|| p.to_string())
                }
            })
            .or_else(inherit_profile_if_compatible)
    };

    (resolved_harness, resolved_model, resolved_profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_harness_selectors_create_and_switch_to_spiron_without_renaming_gateway() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let model = crate::spiron::DEFAULT_SPIRON_MODEL;
        for (index, legacy) in ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default"]
            .into_iter()
            .enumerate()
        {
            let (harness, resolved_model, profile) =
                resolve_subagent_defaults(data, None, legacy, None, Some("sylphx/default".into()));
            assert_eq!(harness, "spiron");
            assert_eq!(resolved_model.as_deref(), Some(model));
            assert_eq!(profile.as_deref(), Some("sylphx/default"));
            let created = create(
                &store,
                data,
                CreateRequest {
                    name: format!("legacy-{index}"),
                    role: None,
                    harness: legacy.into(),
                    model: None,
                    profile: Some("sylphx/default".into()),
                    repo: None,
                    branch: None,
                    base: None,
                    workdir: None,
                    workdir_default: None,
                    description: None,
                    instructions: None,
                    parent_id: None,
                },
            )
            .unwrap();
            assert_eq!(created.harness, "spiron");
            assert_eq!(created.model.as_deref(), Some(model));
            assert_eq!(upstream_provider_of(legacy, None), "sylphx");
            let switched =
                set_agent_harness(&store, data, "operator", &created.name, legacy, None, None)
                    .unwrap();
            assert_eq!(switched["harness"], "spiron");
            assert_eq!(switched["model"], model);
            assert_eq!(switched["profile"], "sylphx/default");
        }
    }

    #[test]
    fn legacy_profile_compatibility_lists_expose_only_canonical_harnesses() {
        let dir = crate::test_tempdir();
        let profile = dir.path().join("profiles/sylphx/default");
        std::fs::create_dir_all(&profile).unwrap();
        std::fs::write(profile.join("profile.json"), serde_json::json!({
            "compatible_harnesses": ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default", "spiron", "codex"]
        }).to_string()).unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let profiles = sync_and_list_provider_profiles_inner(&store, dir.path(), false, false);
        let entry = profiles
            .iter()
            .find(|entry| entry.provider == "sylphx")
            .unwrap();
        assert_eq!(entry.compatible_harnesses, ["spiron", "codex"]);
        for provider in [
            "sylphx",
            "qwen-token-plan",
            "kimi-code-plan",
            "glm-code-plan",
            "minimax-code-plan",
        ] {
            let (_, harnesses) = default_kind_and_harnesses(provider);
            assert_eq!(harnesses.iter().filter(|h| *h == "spiron").count(), 1);
            assert!(!harnesses.iter().any(|h| h.starts_with("sylphx")));
        }
    }

    #[test]
    fn legacy_subagent_defaults_are_canonical_but_provider_profiles_are_preserved() {
        let dir = crate::test_tempdir();
        std::fs::write(dir.path().join("subagent-defaults.json"), serde_json::json!({
            "default": {"mode": "custom", "harness": "sylphx/default", "model": "sylphx/openai/gpt-6.1-sol#medium", "profile": "sylphx/default"},
            "agents": {"parent": {"mode": "custom", "harness": "sylphx-agent", "profile": "sylphx/default"}}
        }).to_string()).unwrap();
        let cfg = load_subagent_defaults(dir.path());
        assert_eq!(cfg.default.harness.as_deref(), Some("spiron"));
        assert_eq!(cfg.agents["parent"].harness.as_deref(), Some("spiron"));
        assert_eq!(cfg.default.profile.as_deref(), Some("sylphx/default"));
        assert_eq!(
            cfg.default.model.as_deref(),
            Some(crate::spiron::DEFAULT_SPIRON_MODEL)
        );
        save_subagent_defaults(dir.path(), &cfg).unwrap();
        let saved: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("subagent-defaults.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(saved["default"]["harness"], "spiron");
        assert_eq!(saved["default"]["profile"], "sylphx/default");
    }

    fn rebalance_test_profiles(data: &Path, busy_pct: i64) -> Vec<ProviderProfileEntry> {
        [("busy", busy_pct), ("healthy", 80)]
            .into_iter()
            .map(|(profile, pct)| {
                ensure_provider_profile_meta(
                    &data.join("profiles/openai").join(profile),
                    "openai",
                    None,
                );
                ProviderProfileEntry {
                    provider: "openai".to_string(),
                    profile: profile.to_string(),
                    account: None,
                    kind: "subscription".to_string(),
                    compatible_harnesses: vec!["codex".to_string()],
                    assigned_agents: Vec::new(),
                    subagent_count: 0,
                    usage: Some(format!("5h: {pct}% left · 7d: {pct}% left")),
                }
            })
            .collect()
    }

    fn rebalance_test_agent(store: &Store, name: &str, parent: Option<&Agent>) -> Agent {
        let mut new = NewAgent::new(name, "codex");
        new.parent_id = parent.map(|a| a.id.clone());
        let agent = store.create_agent(new).unwrap();
        let cfg = agent_config_dir(store.data_dir(), &agent.id);
        std::fs::create_dir_all(&cfg).unwrap();
        write_profile_marker(&cfg, ".anyagents-profile", "openai/busy\n").unwrap();
        agent
    }

    #[cfg(unix)]
    #[test]
    fn new_config_roots_are_private_and_existing_seat_grants_are_preserved() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = crate::test_tempdir();
        let config = dir.path().join("created/config");
        create_private_config_dir(&config).unwrap();
        assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o777, 0o700);
        std::fs::write(config.join("credential"), "retained credential").unwrap();
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o2770)).unwrap();
        create_private_config_dir(&config).unwrap();
        assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o7777, 0o2770);
        assert_eq!(
            std::fs::read_to_string(config.join("credential")).unwrap(),
            "retained credential"
        );
        let blocked = dir.path().join("not-a-directory");
        std::fs::write(&blocked, "preserved").unwrap();
        assert!(create_private_config_dir(&blocked).is_err());
        assert_eq!(std::fs::read_to_string(blocked).unwrap(), "preserved");
    }

    #[test]
    fn rebalance_provider_profiles_balances_overloaded_roots_and_nested_subagents() {
        for depth in 0..=2 {
            let tmp = tempfile::tempdir().unwrap();
            let data = tmp.path();
            let store = Store::open(data.join("state.db")).unwrap();
            store
                .hold_capability_key("0123456789abcdef0123456789abcdef")
                .unwrap();
            let entries = rebalance_test_profiles(data, 50);
            let mut parent = None;
            for level in 0..depth {
                let mut new = NewAgent::new(format!("ancestor-{level}"), "claude-code");
                new.parent_id = parent.as_ref().map(|a: &Agent| a.id.clone());
                parent = Some(store.create_agent(new).unwrap());
            }
            let stay = rebalance_test_agent(&store, "a-stay", parent.as_ref());
            let moved = rebalance_test_agent(&store, "z-move", parent.as_ref());

            rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

            assert_eq!(
                bound_profile_name(&agent_config_dir(data, &stay.id)).as_deref(),
                Some("openai/busy"),
                "depth {depth}"
            );
            assert_eq!(
                bound_profile_name(&agent_config_dir(data, &moved.id)).as_deref(),
                Some("openai/healthy"),
                "depth {depth}"
            );
            let after = store.agent(&moved.id).unwrap();
            assert_eq!(after.parent_id, moved.parent_id);
            assert_eq!(after.harness, moved.harness);
            assert_eq!(after.model, moved.model);
            assert_eq!(after.state, moved.state);
        }
    }

    #[test]
    fn rebalance_provider_profiles_evacuates_exhausted_roots_and_subagents() {
        for is_child in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let data = tmp.path();
            let store = Store::open(data.join("state.db")).unwrap();
            store
                .hold_capability_key("0123456789abcdef0123456789abcdef")
                .unwrap();
            let entries = rebalance_test_profiles(data, 0);
            let parent = store
                .create_agent(NewAgent::new("parent", "claude-code"))
                .unwrap();
            let agent = rebalance_test_agent(&store, "worker", is_child.then_some(&parent));

            rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

            assert_eq!(
                bound_profile_name(&agent_config_dir(data, &agent.id)).as_deref(),
                Some("openai/healthy"),
                "is_child={is_child}"
            );
        }
    }

    #[test]
    fn rebalance_provider_profiles_defers_open_turns_but_counts_their_load() {
        for busy_pct in [0, 50] {
            for is_child in [false, true] {
                let tmp = tempfile::tempdir().unwrap();
                let data = tmp.path();
                let store = Store::open(data.join("state.db")).unwrap();
                store
                    .hold_capability_key("0123456789abcdef0123456789abcdef")
                    .unwrap();
                let entries = rebalance_test_profiles(data, busy_pct);
                let parent = store
                    .create_agent(NewAgent::new("parent", "claude-code"))
                    .unwrap();
                let idle = rebalance_test_agent(&store, "a-idle", is_child.then_some(&parent));
                let working =
                    rebalance_test_agent(&store, "z-working", is_child.then_some(&parent));
                for agent in [&idle, &working] {
                    store
                        .set_agent_running(&agent.id, &data.join(&agent.name).display().to_string())
                        .unwrap();
                }
                store
                    .receive(&working.id, anyagents_core::Incoming::new("active turn"))
                    .unwrap();
                let message = store.next_undelivered(&working.id).unwrap().unwrap();
                store.note_delivered(&working.id, message.seq).unwrap();
                assert!(store.has_open_turn(&working.id).unwrap());
                let cfg = agent_config_dir(data, &working.id);
                write_profile_marker(&cfg, ".anyagents-profile-rotated-ms", "123\n").unwrap();

                rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

                assert_eq!(
                    bound_profile_name(&cfg).as_deref(),
                    Some("openai/busy"),
                    "busy_pct={busy_pct}, is_child={is_child}"
                );
                assert_eq!(
                    std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms")).unwrap(),
                    "123\n"
                );
                assert!(store.has_open_turn(&working.id).unwrap());
                assert_eq!(store.delivered_cursor(&working.id).unwrap(), message.seq);
                // The working seat still contributes load. An idle seat moves
                // even though the alphabetically last seat cannot be moved.
                assert_eq!(
                    bound_profile_name(&agent_config_dir(data, &idle.id)).as_deref(),
                    Some("openai/healthy")
                );

                if busy_pct == 0 {
                    store.note_turn_completed(&working.id).unwrap();
                    rebalance_provider_profiles(
                        &store,
                        data,
                        &store.list_agents().unwrap(),
                        &entries,
                    );
                    assert_eq!(bound_profile_name(&cfg).as_deref(), Some("openai/healthy"));
                    assert_ne!(
                        std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms")).unwrap(),
                        "123\n"
                    );
                }
            }
        }
    }

    #[test]
    fn rebalance_provider_profiles_preserves_archived_and_other_provider_agents() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let entries = rebalance_test_profiles(data, 0);
        let root = rebalance_test_agent(&store, "archived-root", None);
        let child = rebalance_test_agent(&store, "archived-child", Some(&root));
        for agent in [&root, &child] {
            store.set_agent_state(&agent.id, "archived").unwrap();
        }
        let foreign = rebalance_test_agent(&store, "foreign-provider", Some(&root));
        store
            .set_agent_model(&foreign.id, "sylphx/openai/gpt-6.1-sol#xhigh")
            .unwrap();

        rebalance_provider_profiles(&store, data, &store.list_agents().unwrap(), &entries);

        for agent in [&root, &child, &foreign] {
            assert_eq!(
                bound_profile_name(&agent_config_dir(data, &agent.id)).as_deref(),
                Some("openai/busy"),
                "{}",
                agent.name
            );
        }
    }

    #[test]
    fn quota_rotation_counts_subagent_load_and_preserves_exclusions() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let root = rebalance_test_agent(&store, "root", None);
        let rotating = rebalance_test_agent(&store, "rotating", Some(&root));
        let child = rebalance_test_agent(&store, "child", Some(&root));
        let nested = rebalance_test_agent(&store, "nested", Some(&child));
        // Legacy unqualified bindings still use the agent's upstream provider.
        write_profile_marker(
            &agent_config_dir(data, &nested.id),
            ".anyagents-profile",
            "busy\n",
        )
        .unwrap();
        let archived = rebalance_test_agent(&store, "archived", Some(&root));
        store.set_agent_state(&archived.id, "archived").unwrap();
        let foreign = rebalance_test_agent(&store, "foreign", Some(&root));
        write_profile_marker(
            &agent_config_dir(data, &foreign.id),
            ".anyagents-profile",
            "anthropic/busy\n",
        )
        .unwrap();
        let missing = rebalance_test_agent(&store, "missing", Some(&root));
        std::fs::remove_file(agent_config_dir(data, &missing.id).join(".anyagents-profile"))
            .unwrap();

        let counts =
            count_bound_profile_agents(data, &store.list_agents().unwrap(), "openai", &rotating.id);

        assert_eq!(
            counts,
            std::collections::HashMap::from([("busy".to_string(), 3)])
        );
    }

    #[test]
    #[cfg(unix)]
    fn seat_skill_wrapper_mounts_overlay_without_shared_library() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("agents/agent-id/config");
        let skill = dir.path().join("agents/agent-id/skills/private/SKILL.md");
        std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(&skill, "---\nname: private\n---\nexact overlay").unwrap();
        ensure_seat_skills(&config, dir.path());
        assert_eq!(
            std::fs::read(config.join(".claude/skills/private/SKILL.md")).unwrap(),
            std::fs::read(skill).unwrap()
        );
        assert!(!dir.path().join("skills").exists());
    }

    #[test]
    fn rendered_whole_memory_block_respects_caps_separate_from_fixed_file_policy() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let mut agent = store
            .create_agent(anyagents_core::NewAgent::new(
                "caps-rendered",
                "claude-code",
            ))
            .unwrap();
        agent.launch_env.push((
            "ANYAGENTS_MEMORY_GUIDANCE".into(),
            "Operator memory policy".into(),
        ));
        let config = agent_config_dir(dir.path(), &agent.id);
        let root = config.parent().unwrap();
        std::fs::create_dir_all(root.join("memory")).unwrap();
        std::fs::write(root.join("memory/MEMORY.md"), "繁體長記憶".repeat(3000)).unwrap();
        for harness in ["claude-code", "spiron"] {
            agent.harness = harness.into();
            let pin = anyagents_core::host_session::new_session(root, harness).unwrap();
            write_agent_instructions(&config, &agent, None).unwrap();
            for name in ["AGENTS.md", "CLAUDE.md", "SYSTEM_PROMPT.md"] {
                let doc = std::fs::read_to_string(config.join(name)).unwrap();
                // Find full delimiters at line boundaries, not their mention in
                // the independent fixed operating instructions.
                let start = doc.find("\n<learned_memory>\n").unwrap() + 1;
                let end = doc[start..].find("</learned_memory>\n").unwrap()
                    + start
                    + "</learned_memory>\n".len();
                let block = &doc[start..end];
                assert_eq!(block, pin.memory.section);
                assert!(block.len() <= 4096, "{name}: {} bytes", block.len());
                assert!(block.lines().count() <= 150);
                assert!(block.contains("Memory truncated"));
                assert!(!block.contains("fixed; not learned memory"));
                assert!(doc[..start].contains("Operator memory policy"));
            }
        }
    }

    #[test]
    fn instruction_mirrors_share_one_pinned_snapshot_across_harnesses() {
        use anyagents_core::host_session;
        // Resume-pointer reads reject symlinked ancestors, including macOS /var.
        let tmp = crate::test_tempdir();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let mut agent = store
            .create_agent(anyagents_core::NewAgent::new(
                "memory-renderer",
                "claude-code",
            ))
            .unwrap();
        let config = agent_config_dir(data, &agent.id);
        let root = config.parent().unwrap();
        std::fs::create_dir_all(root.join("memory")).unwrap();
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(root.join("memory/MEMORY.md"), "PINNED_FIRST_FACT 繁體\n").unwrap();
        let first = host_session::new_session(root, &agent.harness).unwrap();
        write_agent_instructions(&config, &agent, None).unwrap();
        let paths = [
            "AGENTS.md",
            "CLAUDE.md",
            "SYSTEM_PROMPT.md",
            ".claude/CLAUDE.md",
            ".config/opencode/AGENTS.md",
            ".gemini/config/GEMINI.md",
        ];
        for path in paths {
            let text = std::fs::read_to_string(config.join(path)).unwrap();
            assert!(text.contains(&first.memory.section), "{path}");
        }
        std::fs::write(root.join("memory/MEMORY.md"), "EDITED_SECOND_FACT\n").unwrap();
        agent.description = Some("ordinary describe update".into());
        write_agent_instructions(&config, &agent, None).unwrap();
        let rerender = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
        assert!(rerender.contains("PINNED_FIRST_FACT"));
        assert!(!rerender.contains("EDITED_SECOND_FACT"));
        // External CLI consumption of this mirror is exercised by the real-pipe
        // Spiron launch/wake probe; AnyAgents no longer assembles model prompts.
        agent.harness = "spiron".into();
        write_agent_instructions(&config, &agent, None).unwrap();
        let switched = host_session::active(root).unwrap().unwrap();
        assert_eq!(switched.harness, "spiron");
        assert_ne!(switched.generation, first.generation);
        for path in paths {
            let text = std::fs::read_to_string(config.join(path)).unwrap();
            assert!(text.contains(&switched.memory.section));
            assert!(!text.contains("PINNED_FIRST_FACT"));
            assert!(text.contains("EDITED_SECOND_FACT"));
        }
        std::fs::remove_file(root.join("memory/MEMORY.md")).unwrap();
        write_agent_instructions(&config, &agent, None).unwrap();
        assert_eq!(host_session::active(root).unwrap().unwrap(), switched);
    }
    #[test]
    fn parse_remaining_pct_from_usage_handles_agy_5h_and_7d_windows() {
        let usage = "gemini: 84% left (5h: 100%, 7d: 84%) · opus: 0% left (5h: 0%, 7d: 0%)";
        assert_eq!(
            parse_remaining_pct_from_usage(usage, Some("gemini-4-argon-high")),
            Some(84)
        );
        assert_eq!(
            parse_remaining_pct_from_usage(usage, Some("claude-opus-5-5")),
            Some(0)
        );
    }

    #[test]
    fn clear_stale_agy_quota_error_steps_resets_step_type_17_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let conv_dir = tmp
            .path()
            .join(".gemini")
            .join("antigravity-cli")
            .join("conversations");
        std::fs::create_dir_all(&conv_dir).unwrap();
        let db_path = conv_dir.join("test-conv.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute(
            "CREATE TABLE steps (idx integer PRIMARY KEY, step_type integer NOT NULL DEFAULT 0, status integer NOT NULL DEFAULT 0, error_details blob, step_payload blob)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (idx, step_type, status, error_details, step_payload) VALUES (10, 17, 5, X'0102', X'08112005')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (idx, step_type, status, error_details, step_payload) VALUES (11, 15, 5, NULL, X'080f2005')",
            [],
        )
        .unwrap();
        drop(conn);

        clear_stale_agy_quota_error_steps(tmp.path());

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let (st, status, ed, sp): (i64, i64, Option<Vec<u8>>, Vec<u8>) = conn
            .query_row(
                "SELECT step_type, status, error_details, step_payload FROM steps WHERE idx = 10",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(st, 0);
        assert_eq!(status, 5);
        assert!(ed.is_none());
        assert_eq!(sp, b" \x05*\x03\xd2\x01\x00");
    }

    #[test]
    fn set_agent_harness_preserves_identity_history_and_refuses_incompatible() {
        let tmp = crate::test_tempdir();
        let data = tmp.path();
        let owner_workdir = data.join("owner-workdir");
        std::fs::create_dir(&owner_workdir).unwrap();
        std::fs::create_dir_all(data.join("profiles/sylphx/default")).unwrap();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent {
                name: "sample-parent".to_string(),
                role: None,
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#xhigh".to_string()),
                workdir: Some(owner_workdir.display().to_string()),
                description: Some("Root coordinator".to_string()),
                instructions: Some("Root coordinator".to_string()),
                parent_id: None,
            })
            .unwrap();
        let owner = store
            .create_agent(anyagents_core::NewAgent {
                name: "owner-anymd".to_string(),
                role: None,
                harness: "opencode".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#xhigh".to_string()),
                workdir: Some(owner_workdir.display().to_string()),
                description: Some("Product Owner for AnyMD. Paused scope.".to_string()),
                instructions: Some("Product Owner for AnyMD. Paused scope.".to_string()),
                parent_id: Some(parent.id.clone()),
            })
            .unwrap();
        let cfg = agent_config_dir(data, &owner.id);
        std::fs::create_dir_all(&cfg).unwrap();
        write_profile_marker(&cfg, ".anyagents-profile", "sylphx/default\n").unwrap();
        let root = cfg.parent().unwrap();
        let initial_session =
            anyagents_core::host_session::ensure_active(root, "opencode").unwrap();
        anyagents_core::host_session::bind_vendor(root, "opencode", "ses_original").unwrap();
        let mut inc = anyagents_core::Incoming::new("Initial owner task");
        inc.sender = Some("agent:sample-parent".to_string());
        let _ = store.receive(&owner.id, inc).unwrap();
        let _ = store.record_outbound(
            &owner.id,
            "agent:sample-parent",
            None,
            "Completed initial AnyMD audit",
        );
        let _ = store.set_agent_state(&owner.id, "archived").unwrap();

        let bad_harness = set_agent_harness(
            &store,
            data,
            "operator",
            "owner-anymd",
            "unknown-harness",
            None,
            None,
        );
        assert!(bad_harness.is_err());

        let incompat =
            set_agent_harness(&store, data, "operator", "owner-anymd", "agy", None, None);
        assert!(incompat.is_err());
        assert_eq!(store.agent(&owner.id).unwrap().harness, "opencode");

        let receipt =
            set_agent_harness(&store, data, "operator", "owner-anymd", "codex", None, None)
                .unwrap();
        assert_eq!(receipt["id"], owner.id.as_str());
        assert_eq!(receipt["name"], "owner-anymd");
        assert_eq!(receipt["previous_harness"], "opencode");
        assert_eq!(receipt["harness"], "codex");
        assert_eq!(receipt["model"], "sylphx/openai/gpt-6.1-sol#xhigh");
        assert_eq!(receipt["profile"], "sylphx/default");
        assert_eq!(receipt["state"], "archived");
        assert_eq!(receipt["cutover_status"], "ready_on_next_wake");
        let switched_session = anyagents_core::host_session::active(root).unwrap().unwrap();
        assert_eq!(switched_session.harness, "codex");
        assert_ne!(switched_session.generation, initial_session.generation);

        let after = store.agent(&owner.id).unwrap();
        assert_eq!(after.id, owner.id);
        assert_eq!(after.harness, "codex");
        assert_eq!(
            after.model.as_deref(),
            Some("sylphx/openai/gpt-6.1-sol#xhigh")
        );
        assert_eq!(after.state, "archived");
        assert_eq!(
            after.instructions.as_deref(),
            Some("Product Owner for AnyMD. Paused scope.")
        );
        let toml_text = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
        assert!(toml_text.contains("model = \"gpt-6.1-sol\""));
        assert!(toml_text.contains("model_reasoning_effort = \"xhigh\""));
        assert!(toml_text.contains("model_provider = \"sylphx\""));
        assert!(toml_text.contains("developer_instructions = "));
        assert!(toml_text.contains("Completed initial AnyMD audit"));
        assert!(!cfg.join(".anyagents-harness-continuity.md").exists());

        // Simulate an older opencode session vs a newer codex session + new state.db turn,
        // then switch back to opencode and verify declarative round-trip continuity.
        std::fs::write(cfg.join("opencode-session-id"), "ses_old_opencode\n").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(cfg.join("codex-session-id"), "thread_new_codex\n").unwrap();
        let _ = store.record_outbound(
            &owner.id,
            "agent:sample-parent",
            None,
            "Completed second turn on Codex",
        );
        let roundtrip = configure_agent_runtime(
            &store,
            data,
            "operator",
            "owner-anymd",
            Some("opencode"),
            None,
            None,
        )
        .unwrap();
        assert_eq!(roundtrip["harness"], "opencode");
        let roundtrip_session = anyagents_core::host_session::active(root).unwrap().unwrap();
        assert_eq!(roundtrip_session.harness, "opencode");
        assert_ne!(roundtrip_session.generation, switched_session.generation);
        // The outgoing generation's pin and alias remain resumable.
        assert_eq!(
            anyagents_core::host_session::resume_vendor(root, "opencode", "ses_original").unwrap(),
            initial_session
        );
        assert!(!cfg.join("opencode-session-id").exists());
        let agents_md = std::fs::read_to_string(cfg.join("AGENTS.md")).unwrap();
        assert!(agents_md.contains("Completed second turn on Codex"));

        // Verify model-only update preserves bound profile without requiring --profile.
        let model_only = configure_agent_runtime(
            &store,
            data,
            "operator",
            "owner-anymd",
            Some("codex"),
            Some("sylphx/openai/gpt-5.4-mini#medium"),
            None,
        )
        .unwrap();
        assert_eq!(model_only["harness"], "codex");
        assert_eq!(model_only["model"], "sylphx/openai/gpt-5.4-mini#medium");
        assert_eq!(model_only["profile"], "sylphx/default");
        let toml_after = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
        assert!(toml_after.contains("model = \"gpt-5.4-mini\""));
        assert!(toml_after.contains("model_reasoning_effort = \"medium\""));

        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
            std::fs::set_permissions(&cfg, std::fs::Permissions::from_mode(0o0700)).unwrap();
            let orig_gid = std::fs::metadata(&cfg).unwrap().gid();
            let updated_desc = store
                .set_agent_instructions(&owner.id, Some("Updated description via describe sync."))
                .unwrap();
            let work_dir = agent_work_dir(data, &owner.id);
            sync_agent_description_files(&store, &updated_desc, &cfg).unwrap();
            let meta_after = std::fs::metadata(&cfg).unwrap();
            assert_eq!(meta_after.permissions().mode() & 0o7777, 0o0700);
            assert_eq!(meta_after.gid(), orig_gid);

            // Even if config dir had its execute bit stripped (0600), prompt sync heals it back to 0700.
            std::fs::set_permissions(&cfg, std::fs::Permissions::from_mode(0o0600)).unwrap();
            sync_agent_description_files(&store, &updated_desc, &cfg).unwrap();
            let healed_mode = std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o7777;
            assert_eq!(healed_mode & 0o0700, 0o0700);

            reconcile_agent_runtime_config(&store, data, &updated_desc, &cfg, &work_dir).unwrap();
            let healed_reconcile = std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o7777;
            assert_eq!(healed_reconcile & 0o0700, 0o0700);
        }
    }

    #[test]
    fn spawn_workspace_default_is_private_and_explicit_modes_are_preserved() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path();
        let db = data.join("state.db");
        let store = Store::open(&db).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let parent_workdir = data.join("parent-worktree");
        std::fs::create_dir_all(&parent_workdir).unwrap();
        std::fs::write(parent_workdir.join("parent-only.txt"), "preserve me").unwrap();
        let parent = store
            .create_agent(NewAgent::new("parent", "codex"))
            .unwrap();
        let parent = store
            .set_agent_running(&parent.id, parent_workdir.to_str().unwrap())
            .unwrap();
        let custom_workdir = data.join("explicit-child-path");
        let mut stored_workdirs = vec![(parent.id.clone(), parent.workdir.clone())];
        let mut private_workdirs = Vec::new();

        for (name, workdir) in [
            ("default-first", None),
            ("default-second", None),
            ("private-child", Some(PathBuf::from("private"))),
            ("inherited-child", Some(PathBuf::from("inherit"))),
            ("explicit-child", Some(custom_workdir.clone())),
        ] {
            let child = create(
                &store,
                data,
                CreateRequest {
                    name: name.to_string(),
                    role: None,
                    harness: "codex".to_string(),
                    model: Some("test-model".to_string()),
                    profile: None,
                    repo: None,
                    branch: None,
                    base: None,
                    workdir,
                    // A root override must never become a child default.
                    workdir_default: Some(parent_workdir.clone()),
                    description: None,
                    instructions: None,
                    parent_id: Some(parent.id.clone()),
                },
            )
            .unwrap();
            let actual = PathBuf::from(child.workdir.as_deref().unwrap());
            let expected = match name {
                "inherited-child" => parent_workdir.clone(),
                "explicit-child" => custom_workdir.clone(),
                _ => agent_work_dir(data, &child.id),
            };
            assert_eq!(actual, expected, "workspace selection for {name}");
            assert!(actual.is_dir());
            if name.starts_with("default-") || name == "private-child" {
                assert_ne!(actual, parent_workdir);
                assert!(!actual.join("parent-only.txt").exists());
                assert_eq!(std::fs::read_dir(&actual).unwrap().count(), 0);
                assert!(!private_workdirs.contains(&actual));
                private_workdirs.push(actual);
            }
            stored_workdirs.push((child.id.clone(), child.workdir));
            for (id, expected) in &stored_workdirs {
                assert_eq!(&store.agent(id).unwrap().workdir, expected);
            }
        }
        assert_eq!(
            std::fs::read_to_string(parent_workdir.join("parent-only.txt")).unwrap(),
            "preserve me"
        );
        drop(store);
        let reopened = Store::open(&db).unwrap();
        for (id, expected) in stored_workdirs {
            assert_eq!(reopened.agent(&id).unwrap().workdir, expected);
        }
    }

    #[test]
    fn role_label_does_not_select_execution_or_lifecycle_prompt() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let mut new = NewAgent::new("worker", "codex");
        new.instructions = Some("Follow the project's explicit task instructions.".into());
        let mut agent = store.create_agent(new).unwrap();
        let config = dir.path().join("prompt");
        std::fs::create_dir_all(&config).unwrap();
        for parent in [None, Some("parent")] {
            agent.role = None;
            write_agent_instructions(&config, &agent, parent).unwrap();
            let plain = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
            agent.role = Some("Researcher".into());
            write_agent_instructions(&config, &agent, parent).unwrap();
            let labelled = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
            assert_eq!(
                labelled.replace("- **Role Label**: `Researcher`\n", ""),
                plain
            );
            assert!(plain.contains("Follow the project's explicit task instructions."));
            assert!(plain.contains("Any agent may call"));
            for unwanted in [
                "permanent domain owner",
                "do not** archive yourself",
                "Standing Domain",
                "Hub · Mind · Runner",
            ] {
                assert!(
                    !labelled.contains(unwanted),
                    "unexpected lifecycle instruction: {unwanted}"
                );
            }
        }
    }

    #[test]
    fn spawn_refuses_when_config_write_is_suppressed_or_invalid() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();

        let empty_cfg = data.join("empty-codex-cfg");
        std::fs::create_dir_all(&empty_cfg).unwrap();
        let err =
            verify_harness_runtime_config(&empty_cfg, "codex", Some("sylphx/openai/gpt-5.4#xhigh"))
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("codex runtime config missing") && err.contains("config.toml"),
            "unexpected err: {err}"
        );

        // Write a partial config missing [mcp_servers.anyagents]
        std::fs::write(empty_cfg.join("config.toml"), "model = \"gpt-5.4\"\n").unwrap();
        let err2 =
            verify_harness_runtime_config(&empty_cfg, "codex", Some("sylphx/openai/gpt-5.4#xhigh"))
                .unwrap_err()
                .to_string();
        assert!(
            err2.contains("[mcp_servers.anyagents]"),
            "unexpected err: {err2}"
        );

        std::fs::write(data.join(".anyagents-suppress-config-write"), "1").unwrap();
        let res = create(
            &store,
            data,
            CreateRequest {
                name: "broken-spawn-child".to_string(),
                role: Some("Worker".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: Some(data.to_path_buf()),
                description: None,
                instructions: Some("Should never start".to_string()),
                parent_id: None,
            },
        );
        let _ = std::fs::remove_file(data.join(".anyagents-suppress-config-write"));
        assert!(
            res.is_err(),
            "expected spawn to fail when config write suppressed"
        );
        assert!(
            store.agent_by_name("broken-spawn-child").is_err(),
            "refused spawn must not leave a running agent row behind"
        );
    }

    #[test]
    fn failing_turn_shows_errored_and_notifies_parent_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();

        let parent = create(
            &store,
            data,
            CreateRequest {
                name: "parent-helper".to_string(),
                role: Some("Owner".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: Some(data.to_path_buf()),
                description: None,
                instructions: Some("Parent".to_string()),
                parent_id: None,
            },
        )
        .unwrap();

        let child = create(
            &store,
            data,
            CreateRequest {
                name: "helper-goal-settlement".to_string(),
                role: Some("Worker".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: Some(data.to_path_buf()),
                description: None,
                instructions: Some("Child".to_string()),
                parent_id: Some(parent.id.clone()),
            },
        )
        .unwrap();

        let child_cfg = anyagents_core::agent_config_dir(data, &child.id);
        let sess_dir = child_cfg.join("sessions/2026/10/29");
        std::fs::create_dir_all(&sess_dir).unwrap();
        let rollout_path = sess_dir.join("rollout-2026-10-29T18-51-32-test.jsonl");
        let rollout_line = serde_json::json!({
            "timestamp": "2026-10-29T18:51:40.000Z",
            "ordinal": 4,
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "root_turn_id": "t1",
                "turn_id": "t1",
                "last_agent_message": null,
                "error": {
                    "message": "workspace routing discovery unauthorized (401)"
                }
            }
        });
        std::fs::write(&rollout_path, format!("{rollout_line}\n")).unwrap();
        let rotated_at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        std::fs::write(
            child_cfg.join(".anyagents-profile-rotated-ms"),
            "1700000000000\n",
        )
        .unwrap();
        let rollout = std::fs::File::options()
            .write(true)
            .open(&rollout_path)
            .unwrap();
        rollout.set_modified(rotated_at).unwrap();

        let health = anyagents_supervisor::HealthBoard::default();
        let (delivery_mode, steer_support) = anyagents_supervisor::declared(&child.harness);
        health.publish(
            &child.id,
            anyagents_supervisor::LiveState {
                process_alive: true,
                turn_state: anyagents_harness::TurnState::InTurn,
                delivery_mode,
                steer_support,
                quota_reset_at_ms: None,
                restart_at_ms: None,
                pid: None,
                live_harness: Some(child.harness.clone()),
                live_model: child.model.clone(),
                process_started_at_ms: Some(anyagents_core::now_ms()),
                last_error: None,
            },
        );
        let mut views;

        // A rollout at the rotation boundary is stale, even if it contains an
        // error. Do not let wall-clock or filesystem precision decide freshness.
        views = vec![crate::server::agent_view(&store, &health, &child).unwrap()];
        assert_eq!(views[0].turn_state, "working");
        assert!(views[0].last_error.is_none());
        assert!(store.next_undelivered(&parent.id).unwrap().is_none());

        rollout
            .set_modified(rotated_at + std::time::Duration::from_secs(1))
            .unwrap();
        views = vec![crate::server::agent_view(&store, &health, &child).unwrap()];
        assert_eq!(views[0].turn_state, "errored");
        assert_eq!(
            views[0].last_error.as_deref(),
            Some("workspace routing discovery unauthorized (401)")
        );
        assert_eq!(views[0].block_reason.as_deref(), Some("sign_in_required"));

        let first_msg = store
            .next_undelivered(&parent.id)
            .unwrap()
            .expect("parent notified once");
        assert!(first_msg.text.contains("[Sub-Agent Error]"));
        assert!(
            first_msg
                .text
                .contains("workspace routing discovery unauthorized (401)")
        );
        store.note_delivered(&parent.id, first_msg.seq).unwrap();

        // Second check must not re-notify parent for the same error.
        let _ = crate::server::agent_view(&store, &health, &child).unwrap();
        assert!(
            store.next_undelivered(&parent.id).unwrap().is_none(),
            "parent should only be notified once per error"
        );
    }

    fn worktree_request(name: &str, repo: Option<String>) -> CreateRequest {
        CreateRequest {
            name: name.into(),
            role: None,
            description: None,
            harness: "codex".into(),
            model: None,
            profile: None,
            repo,
            branch: None,
            base: None,
            workdir: None,
            workdir_default: None,
            instructions: None,
            parent_id: None,
        }
    }

    #[test]
    fn explicit_workdir_is_honoured_without_default_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        std::fs::create_dir(&data).unwrap();
        let f = crate::worktree::tests::Fixture::new(&data);
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let explicit = dir.path().join("explicit");
        std::fs::create_dir(&explicit).unwrap();
        let mut request = worktree_request("explicit", None);
        request.workdir = Some(explicit.clone());
        let agent = create_with_consumer(&store, &data, request, &f.consumer).unwrap();
        assert_eq!(agent.workdir.as_deref(), explicit.to_str());
        assert!(!f.log.exists());
    }

    #[test]
    fn explicit_unwritable_workdir_is_a_typed_refusal_without_repair() {
        if crate::sandbox::server_uid() == 0 {
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        std::fs::create_dir(&data).unwrap();
        let f = crate::worktree::tests::Fixture::new(&data);
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let explicit = dir.path().join("explicit");
        std::fs::create_dir(&explicit).unwrap();
        let blocked = explicit.join("blocked");
        std::fs::write(&blocked, "preserve").unwrap();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o444)).unwrap();
        let mut request = worktree_request("blocked", None);
        request.workdir = Some(explicit.clone());
        let err = create_with_consumer(&store, &data, request, &f.consumer).unwrap_err();
        assert!(matches!(err, Error::WorkdirPermission { .. }), "{err}");
        assert!(err.to_string().contains(blocked.to_str().unwrap()), "{err}");
        assert!(store.agent_by_name("blocked").is_err());
        assert_eq!(std::fs::read_to_string(&blocked).unwrap(), "preserve");
        assert_eq!(
            std::fs::metadata(&blocked).unwrap().permissions().mode() & 0o777,
            0o444
        );
    }

    #[test]
    fn missing_inherited_workdir_is_refused_without_default_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let mut request = worktree_request("missing-inherit", None);
        request.workdir = Some("inherit".into());
        let err = create_with_consumer(&store, dir.path(), request, &f.consumer).unwrap_err();
        assert!(matches!(err, Error::WorkdirPermission { .. }), "{err}");
        assert!(
            err.to_string().contains("parent has no accessible workdir"),
            "{err}"
        );
        assert!(store.agent_by_name("missing-inherit").is_err());
        assert!(!f.log.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn privileged_cross_uid_spawn_can_create_commit_and_status() {
        use std::os::unix::{fs::PermissionsExt, process::ExitStatusExt};
        use std::process::Command;
        let caps = std::fs::read_to_string("/proc/self/status").unwrap();
        let effective = caps
            .lines()
            .find_map(|l| l.strip_prefix("CapEff:\t"))
            .and_then(|n| u64::from_str_radix(n, 16).ok())
            .unwrap_or(0);
        let required = (1 << 0) | (1 << 6) | (1 << 7); // CHOWN, SETGID, SETUID
        if effective & required != required || crate::sandbox::server_uid() != 0 {
            assert!(
                std::env::var_os("ANYAGENTS_REQUIRE_PRIVILEGED_WORKTREE_TEST").is_none(),
                "requires privilege: CAP_CHOWN, CAP_SETGID and CAP_SETUID"
            );
            eprintln!(
                "requires privilege: CAP_CHOWN, CAP_SETGID and CAP_SETUID; A→B worktree acceptance not executed"
            );
            return;
        }
        if std::env::var_os("ANYAGENTS_PRIVILEGED_WORKER").is_none() {
            let out = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "create::tests::privileged_cross_uid_spawn_can_create_commit_and_status",
                    "--nocapture",
                ])
                .env("ANYAGENTS_PRIVILEGED_WORKER", "1")
                // This proof selects a private identity independently of A's
                // common config; an unset override deliberately preserves A's.
                .env("ANYAGENTS_GIT_AUTHOR_NAME", "Example Author")
                .env("ANYAGENTS_GIT_AUTHOR_EMAIL", "author@example.test")
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "safe.directory")
                .env("GIT_CONFIG_VALUE_0", "*")
                .output()
                .unwrap();
            eprintln!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            assert!(out.status.success(), "privileged worktree worker failed");
            return;
        }
        unsafe { std::env::set_var("ANYAGENTS_LAUNCHER", "/usr/bin/setpriv") };
        if let Ok(data) = std::env::var("ANYAGENTS_INSPECTION_ABORT_DATA") {
            let id = anyagents_core::AgentId::parse(
                &std::env::var("ANYAGENTS_INSPECTION_ABORT_ID").unwrap(),
            )
            .unwrap();
            let checkout =
                PathBuf::from(std::env::var("ANYAGENTS_INSPECTION_ABORT_CHECKOUT").unwrap());
            let record =
                crate::worktree_access::RecordedWorktree::load(Path::new(&data), &id, &checkout)
                    .unwrap();
            let _: std::io::Result<()> = record.inspect(|| {
                let status = record.command()?.args(["status", "--porcelain"]).output()?;
                assert!(
                    status.status.success(),
                    "{}",
                    String::from_utf8_lossy(&status.stderr)
                );
                // No Drop or unwind cleanup may run before restart recovery.
                unsafe { libc::raise(libc::SIGKILL) };
                std::process::abort();
            });
            panic!("inspection did not reach its abort barrier");
        }
        struct Users(Vec<String>);
        impl Drop for Users {
            fn drop(&mut self) {
                for user in &self.0 {
                    let _ = Command::new("userdel").arg(user).status();
                }
            }
        }
        let mut users = Users(Vec::new());
        let mut ids = Vec::new();
        for suffix in ["a", "b", "c"] {
            let name = format!("aa-wt-{}-{suffix}", std::process::id());
            assert!(
                Command::new("useradd")
                    .args(["--no-create-home", "--user-group", &name])
                    .status()
                    .unwrap()
                    .success()
            );
            users.0.push(name.clone());
            let id = |flag: &str| {
                String::from_utf8(
                    Command::new("id")
                        .args([flag, &name])
                        .output()
                        .unwrap()
                        .stdout,
                )
                .unwrap()
                .trim()
                .parse::<u32>()
                .unwrap()
            };
            ids.push((id("-u"), id("-g")));
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let data = dir.path().join("data");
        std::fs::create_dir(&data).unwrap();
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o755)).unwrap();
        let f = crate::worktree::tests::Fixture::new(&data);
        let (a_uid, a_gid) = ids[0];
        let (b_uid, b_gid) = ids[1];
        // Match the consumer's already-initialized shared repository. Sparse
        // checkout must write only its private config, not initialize A's.
        crate::worktree::tests::git(&f.source, &["config", "extensions.worktreeConfig", "true"]);
        crate::worktree::tests::git(&f.source, &["config", "user.name", "Original local owner"]);
        crate::worktree::tests::git(
            &f.source,
            &["config", "user.email", "original@example.invalid"],
        );
        std::fs::set_permissions(
            f.source.join(".git/config"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert!(
            Command::new("chown")
                .args(["-R", &format!("{a_uid}:{a_gid}")])
                .arg(&data)
                .status()
                .unwrap()
                .success()
        );
        // A's config must survive byte-for-byte. The new seat gets its own
        // identity, never a common rewrite.
        let common_config = f.source.join(".git/config");
        let config_before = std::fs::read(&common_config).unwrap();
        let config_inode =
            std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&common_config).unwrap());
        let authority = data.join("consumer-authority");
        std::fs::rename(data.join("consumer"), &authority).unwrap();
        crate::worktree::tests::write_executable(
            &data.join("consumer"),
            &format!(
                "#!/bin/sh\nexec /usr/bin/setpriv --reuid {a_uid} --regid {a_gid} --clear-groups -- env HOME=/nonexistent XDG_CONFIG_HOME=/nonexistent GIT_CONFIG_GLOBAL=/dev/null {:?} \"$@\"\n",
                authority
            ),
        );
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let parent = store
            .create_agent(NewAgent::new("parent-a", "codex"))
            .unwrap();
        store
            .set_agent_running(&parent.id, f.source.to_str().unwrap())
            .unwrap();
        let target = dir.path().join("checkout");
        // The consumer executes as A and must be able to create the checkout.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let mut request = worktree_request("child-b", Some("inherit".into()));
        request.parent_id = Some(parent.id.clone());
        request.workdir = Some(target.clone());
        let agent = create_with_seat(
            &store,
            &data,
            request,
            &f.consumer,
            Some((users.0[1].clone(), b_uid, b_gid, None)),
        )
        .unwrap();
        assert_eq!(agent.workdir.as_deref(), target.to_str());
        use std::os::unix::fs::MetadataExt;
        let config_after = std::fs::metadata(&common_config).unwrap();
        assert_eq!(config_after.uid(), a_uid);
        assert_eq!(config_after.ino(), config_inode);
        assert_eq!(config_after.mode() & 0o707, 0o600);
        assert_eq!(std::fs::read(&common_config).unwrap(), config_before);
        let readable = Command::new("/usr/bin/setpriv")
            .args([
                "--reuid",
                &a_uid.to_string(),
                "--regid",
                &a_gid.to_string(),
                "--clear-groups",
                "--",
                "cat",
            ])
            .arg(&common_config)
            .output()
            .unwrap();
        assert!(readable.status.success(), "A must still read its config");
        assert_eq!(readable.stdout, config_before);
        let common = crate::worktree::tests::git(
            &target,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        );
        let objects = PathBuf::from(common).join("objects");
        let new_object_dir = (0..256)
            .map(|n| objects.join(format!("{n:02x}")))
            .find(|p| !p.exists())
            .unwrap();
        let b_config = agent_config_dir(&data, &agent.id);
        crate::sandbox::share_config_with_seat_uid(&b_config, b_uid, b_gid);
        let push_remote = dir.path().join("seat-push.git");
        let out = Command::new("/usr/bin/setpriv")
            .env_remove("GIT_CONFIG_COUNT")
            .env_remove("GIT_AUTHOR_NAME").env_remove("GIT_AUTHOR_EMAIL")
            .env_remove("GIT_COMMITTER_NAME").env_remove("GIT_COMMITTER_EMAIL")
            .env("HOME", &b_config)
            .env("XDG_CONFIG_HOME", "/nonexistent")
            .env("GIT_CONFIG_GLOBAL", b_config.join(".gitconfig"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args(["--reuid", &b_uid.to_string(), "--regid", &b_gid.to_string(), "--clear-groups", "--", "/bin/sh", "-c",
                r#"set -eu; mkdir "$2"; cd "$1"; printf child > src/child; git add src/child; git commit -m 'Child commit'; git status --porcelain; git init --bare "$3"; git push "$3" HEAD:refs/heads/seat-proof"#, "child-seat"])
            .arg(&target).arg(&new_object_dir).arg(&push_remote).output().unwrap();
        assert!(
            out.status.success(),
            "B create/commit/status: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(target.join("src/child")).unwrap(),
            "child"
        );
        assert_eq!(
            crate::worktree::tests::git(&target, &["log", "-1", "--format=%an <%ae>%n%cn <%ce>"]),
            "Example Author <author@example.test>\nExample Author <author@example.test>"
        );
        let common = objects.parent().unwrap();
        let private = PathBuf::from(crate::worktree::tests::git(
            &target,
            &["rev-parse", "--path-format=absolute", "--git-dir"],
        ));
        let run_as = |uid: u32, gid: u32, script: &str, paths: &[&Path]| {
            let mut command = Command::new("/usr/bin/setpriv");
            command
                // Keep probes in the shared fixture root; these seats cannot
                // traverse the CI checkout's ancestors.
                .current_dir(dir.path())
                .env("HOME", "/nonexistent")
                .env("XDG_CONFIG_HOME", "/nonexistent")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .args([
                    "--reuid",
                    &uid.to_string(),
                    "--regid",
                    &gid.to_string(),
                    "--clear-groups",
                    "--",
                    "/bin/sh",
                    "-c",
                    script,
                    "seat-proof",
                ]);
            for path in paths {
                command.arg(path);
            }
            let out = command.output().unwrap();
            assert!(
                out.status.success(),
                "uid={uid}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        // Each forbidden operation must fail as B, including rename/unlink (a
        // read-only file alone would not stop either in a writable directory).
        run_as(
            b_uid,
            b_gid,
            r#"
set -eu
mkdir "$1/evil-hooks"
printf '#!/bin/sh\nprintf compromised > "%s"\n' "$1/hook-executed" > "$1/evil-hooks/pre-commit"
chmod +x "$1/evil-hooks/pre-commit"
if printf evil >> "$2/config"; then exit 11; fi
if printf evil >> "$2/hooks/pre-commit"; then exit 12; fi
if printf evil > "$2/hooks/child-hook"; then exit 13; fi
if printf evil > "$2/info/child-info"; then exit 14; fi
if git -c safe.directory="$1" -C "$1" config --worktree core.hooksPath "$1/evil-hooks"; then exit 15; fi
if printf evil >> "$3/config.worktree"; then exit 16; fi
if rm "$3/config.worktree"; then exit 17; fi
printf evil > "$3/child-config"
if mv "$3/child-config" "$3/config.worktree"; then exit 18; fi
if printf evil >> "$3/commondir"; then exit 19; fi
test -f "$2/logs/HEAD"
if printf evil >> "$2/logs/HEAD"; then exit 27; fi
if rm "$2/logs/HEAD"; then exit 28; fi
"#,
            &[&target, common, &private],
        );
        assert!(!target.join("hook-executed").exists());
        // B's ACL grant and new Git objects must not revoke sibling A.
        assert_eq!(std::fs::metadata(&new_object_dir).unwrap().uid(), b_uid);
        let out = Command::new("/usr/bin/setpriv")
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "core.hooksPath")
            .env("GIT_CONFIG_VALUE_0", target.join("evil-hooks"))
            .env("HOME", "/nonexistent")
            .env("XDG_CONFIG_HOME", "/nonexistent")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .args(["--reuid", &a_uid.to_string(), "--regid", &a_gid.to_string(), "--clear-groups", "--", "/bin/sh", "-c",
                "set -eu; : > \"$2/sibling-write\"; rm \"$2/sibling-write\"; cd \"$1\"; printf sibling > src/sibling; git -c safe.directory=\"$1\" add src/sibling; git -c safe.directory=\"$1\" -c user.name='Example Author' -c user.email=author@example.test -c core.hooksPath=\"$3/hooks\" commit -m 'Sibling commit' ; git -c safe.directory=\"$1\" status --porcelain", "sibling-seat"])
            .arg(&f.source).arg(&new_object_dir).arg(objects.parent().unwrap()).output().unwrap();
        assert!(
            out.status.success(),
            "A still creates/commits/status after B: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        // Existing explicit workdirs are checked as B without rewriting them.
        let mut explicit = worktree_request("explicit-b", None);
        explicit.workdir = Some(target.clone());
        let accepted = create_with_seat(
            &store,
            &data,
            explicit,
            &f.consumer,
            Some((users.0[1].clone(), b_uid, b_gid, None)),
        )
        .unwrap();
        assert_eq!(accepted.workdir.as_deref(), target.to_str());
        let blocked = target.join("src/parent-only");
        std::fs::write(&blocked, "preserve").unwrap();
        assert!(
            Command::new("chown")
                .arg(format!("{a_uid}:{a_gid}"))
                .arg(&blocked)
                .status()
                .unwrap()
                .success()
        );
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut explicit = worktree_request("refused-b", None);
        explicit.workdir = Some(target.clone());
        let err = create_with_seat(
            &store,
            &data,
            explicit,
            &f.consumer,
            Some((users.0[1].clone(), b_uid, b_gid, None)),
        )
        .unwrap_err();
        assert!(matches!(err, Error::WorkdirPermission { .. }), "{err}");
        assert!(err.to_string().contains(blocked.to_str().unwrap()), "{err}");
        assert!(store.agent_by_name("refused-b").is_err());
        assert_eq!(
            std::fs::metadata(&blocked).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(std::fs::read_to_string(&blocked).unwrap(), "preserve");
        // End this refusal scenario before the independent wake positive.
        // Restore must not widen access to a later parent-only checkout file.
        std::fs::remove_file(&blocked).unwrap();
        assert!(
            !target.join("hook-executed").exists(),
            "parent must ignore B's planted hook"
        );
        let (c_uid, c_gid) = ids[2];
        let mut request = worktree_request("child-c", Some("ExampleOrg/demo".into()));
        request.workdir = Some(dir.path().join("checkout-c"));
        request.parent_id = Some(agent.id.clone());
        let c = create_with_seat(
            &store,
            &data,
            request,
            &f.consumer,
            Some((users.0[2].clone(), c_uid, c_gid, None)),
        )
        .unwrap();
        run_as(
            c_uid,
            c_gid,
            r#"set -eu; test -x "$1"; if cat "$2/HEAD"; then exit 21; fi; if cat "$2/index"; then exit 22; fi"#,
            &[common, &private],
        );
        let c_private = PathBuf::from(crate::worktree::tests::git(
            Path::new(c.workdir.as_deref().unwrap()),
            &["rev-parse", "--path-format=absolute", "--git-dir"],
        ));
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; if cat "$1/HEAD"; then exit 23; fi"#,
            &[&c_private],
        );
        // Parent config/remote/push-u lock+rename must preserve both active
        // readers immediately. No launch, wake or inspecting read may repair
        // the config between the parent's write and these real-UID probes.
        let parent_push = dir.path().join("parent-push.git");
        for edit in [
            r#"git -c safe.directory="$1" -C "$1" config test.replacement inherited"#,
            r#"git -c safe.directory="$1" -C "$1" remote add refresh-proof /nonexistent/refresh-proof"#,
            r#"set -eu; git init --bare "$2"; git -c safe.directory="$1" -C "$1" push -u "$2" HEAD:refs/heads/parent-proof"#,
        ] {
            // push -u can replace config twice. Keep the old inode allocated
            // until comparison so a later replacement cannot reuse its number.
            let before = std::fs::File::open(&common_config).unwrap();
            run_as(a_uid, a_gid, edit, &[&f.source, &parent_push]);
            assert_ne!(
                std::fs::metadata(&common_config).unwrap().ino(),
                before.metadata().unwrap().ino(),
                "parent config rewrite did not replace the pinned inode: {edit}"
            );
            drop(before);
            for (uid, gid, checkout, sibling) in [
                (b_uid, b_gid, target.as_path(), &c_private),
                (
                    c_uid,
                    c_gid,
                    Path::new(c.workdir.as_deref().unwrap()),
                    &private,
                ),
            ] {
                run_as(
                    uid,
                    gid,
                    r#"set -eu; test -r "$2/config"; git -c safe.directory="$1" -C "$1" status --porcelain; if printf evil >> "$2/config"; then exit 41; fi; if printf evil > "$2/anyagents-config/config.lock"; then exit 45; fi; if rm "$2/config"; then exit 46; fi; if printf evil >> "$2/hooks/pre-commit"; then exit 42; fi; if printf evil > "$2/info/refresh-leak"; then exit 43; fi; if cat "$3/index"; then exit 44; fi"#,
                    &[checkout, common, sibling],
                );
            }
            assert_eq!(std::fs::metadata(&common_config).unwrap().uid(), a_uid);
            run_as(
                a_uid,
                a_gid,
                r#"set -eu; test -r "$1"; test -w "$1""#,
                &[&common_config],
            );
        }
        // A third, unrecorded principal cannot acquire the config's read grant.
        run_as(
            65534,
            65534,
            r#"if cat "$1/config"; then exit 47; fi"#,
            &[common],
        );
        // Vesta's same-size clean-filter payload: merely disabling hooks and
        // fsmonitor does not stop `status` executing a configured clean driver.
        let original_dotgit = std::fs::read(target.join(".git")).unwrap();
        let forged = dir.path().join("child-controlled.git");
        let seed = dir.path().join("filter-seed");
        let filter = dir.path().join("clean-filter");
        let filter_marker = dir.path().join("filter-executed");
        run_as(
            b_uid,
            b_gid,
            r#"
set -eu
umask 022
git -C "$5/.." init -b main --separate-git-dir="$2" "$1"
mkdir "$1/src"
printf 'included\n' > "$1/src/keep"
printf 'src/keep filter=x\n' > "$1/.gitattributes"
git -C "$1" add .
git -C "$1" -c user.name='Fixture' -c user.email=fixture@example.invalid commit -m 'Synthetic filter baseline'
printf '#!/bin/sh\nid -u >> "%s"\ncat\n' "$4" > "$3"
chmod +x "$3"
git --git-dir="$2" config filter.x.clean "$3"
git --git-dir="$2" config filter.x.required true
git --git-dir="$2" config core.worktree "$5"
# The protected A-owned pointer is not writable by B. The attack replaces
# its directory entry using B's checkout-directory rights instead.
test ! -w "$5/.git"
rm "$5/.git"
printf 'gitdir: %s\n' "$2" > "$5/.git"
printf 'src/keep filter=x\n' > "$5/.gitattributes"
printf 'changed!\n' > "$5/src/keep"
printf 'changed!\n' > "$1/src/keep"
# Positive exploit control: this seat owns the planted gitdir and must run it.
git -c safe.directory="$5" -C "$5" status --porcelain
"#,
            &[&seed, &forged, &filter, &filter_marker, &target],
        );
        let executions = std::fs::read_to_string(&filter_marker).unwrap();
        assert!(!executions.is_empty());
        assert!(executions.lines().all(|line| line == b_uid.to_string()));
        std::fs::remove_file(&filter_marker).unwrap();
        let recorded =
            crate::worktree_access::RecordedWorktree::load(&data, &agent.id, &target).unwrap();
        let pinned = recorded
            .command()
            .unwrap()
            .args(["status", "--porcelain"])
            .output()
            .unwrap();
        assert!(
            pinned.status.success(),
            "trusted cleanup status: {}",
            String::from_utf8_lossy(&pinned.stderr)
        );
        assert!(!crate::server::is_git_worktree_safe_to_reap(
            &data, &agent.id, &target
        ));
        assert!(
            !filter_marker.exists(),
            "server cleanup followed B's .git pointer"
        );
        // The A and C seat homes trust only their own recorded roots, not B's.
        let a_config = agent_config_dir(&data, &parent.id);
        std::fs::create_dir_all(&a_config).unwrap();
        ensure_seat_environment(&a_config, &f.source).unwrap();
        crate::sandbox::share_config_with_seat_uid(&a_config, a_uid, a_gid);
        for (uid, gid, config) in [
            (a_uid, a_gid, &a_config),
            (c_uid, c_gid, &agent_config_dir(&data, &c.id)),
        ] {
            crate::sandbox::share_config_with_seat_uid(config, uid, gid);
            let out = Command::new("/usr/bin/setpriv")
                .env_remove("GIT_CONFIG_COUNT")
                .env("HOME", config)
                .env("XDG_CONFIG_HOME", "/nonexistent")
                .env("GIT_CONFIG_GLOBAL", config.join(".gitconfig"))
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .args([
                    "--reuid",
                    &uid.to_string(),
                    "--regid",
                    &gid.to_string(),
                    "--clear-groups",
                    "--",
                    "git",
                    "-C",
                ])
                .arg(&seed)
                .args(["status", "--porcelain"])
                .output()
                .unwrap();
            assert!(!out.status.success());
            assert!(
                String::from_utf8_lossy(&out.stderr).contains("dubious ownership"),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            assert!(
                !filter_marker.exists(),
                "foreign seat executed B's clean filter"
            );
        }
        std::fs::write(target.join(".git"), original_dotgit).unwrap();
        std::fs::write(target.join("src/keep"), "included\n").unwrap();
        std::fs::remove_file(target.join(".gitattributes")).unwrap();
        // A trusted filter definition is still executable when B selects it
        // through checkout or index attributes. It may run only with B's rights.
        let parent_only = dir.path().join("parent-only");
        std::fs::create_dir(&parent_only).unwrap();
        std::fs::set_permissions(&parent_only, std::fs::Permissions::from_mode(0o700)).unwrap();
        let parent_marker = parent_only.join("filter-ran");
        std::fs::write(
            &filter,
            format!(
                "#!/bin/sh\nid -u >> '{}'\nprintf hit > '{}' 2>/dev/null || :\ncat\n",
                filter_marker.display(),
                parent_marker.display()
            ),
        )
        .unwrap();
        run_as(
            a_uid,
            a_gid,
            r#"set -eu; git -c safe.directory="$1" -C "$1" config filter.trusted.clean "$2"; git -c safe.directory="$1" -C "$1" config filter.trusted.required true"#,
            &[&f.source, &filter],
        );
        crate::worktree_access::ensure_launchable(&data, &agent.id).unwrap();
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; printf 'src/keep filter=trusted\n' > "$1/.gitattributes"; printf 'changed!\n' > "$1/src/keep"; git -c safe.directory="$1" -C "$1" add .gitattributes"#,
            &[&target],
        );
        std::fs::remove_file(&filter_marker).ok();
        crate::worktree_access::revoke_recorded(&data, &agent.id).unwrap();
        assert!(!crate::server::is_git_worktree_safe_to_reap(
            &data, &agent.id, &target
        ));
        let executions = std::fs::read_to_string(&filter_marker).unwrap();
        assert!(!executions.is_empty(), "trusted filter was not exercised");
        assert!(
            executions.lines().all(|line| line == b_uid.to_string()),
            "filter ran as another UID: {executions}"
        );
        assert!(
            !parent_marker.exists(),
            "cleanup filter retained parent rights"
        );
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; if cat "$1/index"; then exit 31; fi; if mkdir "$2/revoked-check"; then exit 32; fi"#,
            &[&private, &objects],
        );
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "create::tests::privileged_cross_uid_spawn_can_create_commit_and_status",
                "--nocapture",
            ])
            .env("ANYAGENTS_INSPECTION_ABORT_DATA", &data)
            .env("ANYAGENTS_INSPECTION_ABORT_ID", agent.id.as_str())
            .env("ANYAGENTS_INSPECTION_ABORT_CHECKOUT", &target)
            .output()
            .unwrap();
        assert_eq!(
            status.status.signal(),
            Some(libc::SIGKILL),
            "abort worker: {}{}",
            String::from_utf8_lossy(&status.stdout),
            String::from_utf8_lossy(&status.stderr)
        );
        let receipt = || -> serde_json::Value {
            serde_json::from_slice(
                &std::fs::read(crate::worktree_access::grant_path(&data, &agent.id)).unwrap(),
            )
            .unwrap()
        };
        assert_eq!(receipt()["active"], false);
        assert_eq!(receipt()["inspection"], true);
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; test -r "$1/index"; if printf evil >> "$2/../config"; then exit 33; fi; if mkdir "$2/read-only-check"; then exit 34; fi"#,
            &[&private, &objects],
        );
        crate::worktree_access::recover_inspections(&data).unwrap();
        assert_eq!(receipt()["inspection"], false);
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; if cat "$1/index"; then exit 35; fi; if mkdir "$2/recovery-check"; then exit 36; fi"#,
            &[&private, &objects],
        );
        assert!(!crate::server::is_git_worktree_safe_to_reap(
            &data, &agent.id, &target
        ));
        assert!(
            target.join("src/child").is_file(),
            "recovery lost the dirty checkout"
        );
        assert_eq!(receipt()["inspection"], false);
        assert!(!parent_marker.exists());
        crate::worktree_access::restore_recorded(&data, &agent.id).unwrap();
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; git -c safe.directory="$1" -C "$1" reset HEAD -- .gitattributes; rm "$1/.gitattributes"; printf 'included\n' > "$1/src/keep""#,
            &[&target],
        );
        // Keep both the trusted config and driver unchanged through the checks;
        // remove them only after the owner-UID and interrupted-check proofs.
        run_as(
            a_uid,
            a_gid,
            r#"git -c safe.directory="$1" -C "$1" config --remove-section filter.trusted"#,
            &[&f.source],
        );
        // Concurrent agents commonly share one OS seat. B2 admission and B1
        // archive must not revoke either live workspace's effective bindings.
        let b2_target = dir.path().join("checkout-b2");
        let mut request = worktree_request("child-b2", Some("ExampleOrg/demo".into()));
        request.workdir = Some(b2_target.clone());
        let b2 = create_with_seat(
            &store,
            &data,
            request,
            &f.consumer,
            Some((users.0[1].clone(), b_uid, b_gid, None)),
        )
        .unwrap();
        crate::worktree_access::verify(&target, b_uid, b_gid, None).unwrap();
        crate::worktree_access::verify(&b2_target, b_uid, b_gid, None).unwrap();
        // Archive a dirty external checkout: preserve its bytes but revoke the
        // common-store grant before any clean-worktree deletion decision.
        store.set_agent_state(&agent.id, "archived").unwrap();
        assert!(crate::server::reap_subagent_ephemeral_dirs(
            &data, &agent.id
        ));
        assert!(target.join("src/child").is_file());
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; if cat "$1/HEAD"; then exit 24; fi; mkdir "$2/same-seat-live"; rmdir "$2/same-seat-live""#,
            &[&private, &objects],
        );
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; cd "$1"; printf survivor > src/survivor; git -c safe.directory="$1" add src/survivor; git -c safe.directory="$1" -c user.name='Example Author' -c user.email=author@example.test commit -m 'Survivor commit'; git -c safe.directory="$1" status --porcelain"#,
            &[&b2_target],
        );
        let receipt: serde_json::Value = serde_json::from_slice(
            &std::fs::read(crate::worktree_access::grant_path(&data, &agent.id)).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["active"], false);
        // A deliberate wake restores only the original allowlisted metadata.
        crate::worktree_access::restore_recorded(&data, &agent.id).unwrap();
        crate::worktree_access::verify(&target, b_uid, b_gid, None).unwrap();
        crate::worktree_access::revoke_recorded(&data, &agent.id).unwrap();
        crate::worktree_access::verify(&b2_target, b_uid, b_gid, None).unwrap();
        store.set_agent_state(&b2.id, "archived").unwrap();
        assert!(crate::server::reap_subagent_ephemeral_dirs(&data, &b2.id));
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; if mkdir "$1/revoked"; then exit 25; fi"#,
            &[&objects],
        );
        // C still works after B's revocation (separate principals, same store).
        crate::worktree_access::verify(
            Path::new(c.workdir.as_deref().unwrap()),
            c_uid,
            c_gid,
            None,
        )
        .unwrap();
        store.set_agent_state(&c.id, "archived").unwrap();
        assert!(crate::server::reap_subagent_ephemeral_dirs(&data, &c.id));
        // An admission refusal AFTER sharing must revoke, read back, and only
        // then discard the child row and its private state.
        let mut request = worktree_request("rollback-b", Some("ExampleOrg/demo".into()));
        request.workdir = Some(dir.path().join("rollback-b"));
        let err = create_with_seat(
            &store,
            &data,
            request,
            &f.consumer,
            Some((users.0[1].clone(), b_uid, b_gid, Some("invalid".into()))),
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid seat launcher"), "{err}");
        assert!(store.agent_by_name("rollback-b").is_err());
        run_as(
            b_uid,
            b_gid,
            r#"set -eu; if mkdir "$1/rollback-leak"; then exit 26; fi"#,
            &[&objects],
        );
        // A real child seat can take flock(LOCK_EX) through a read-only HEAD
        // descriptor. Inspection cleanup must fail closed without waiting for
        // that child; the fresh-server matrix proves both startup entrypoints.
        let mut request = worktree_request("locked-b", Some("ExampleOrg/demo".into()));
        request.workdir = Some(dir.path().join("locked-checkout"));
        let locked = create_with_seat(
            &store,
            &data,
            request,
            &f.consumer,
            Some((users.0[1].clone(), b_uid, b_gid, None)),
        )
        .unwrap();
        let locked_checkout = Path::new(locked.workdir.as_deref().unwrap());
        let record =
            crate::worktree_access::RecordedWorktree::load(&data, &locked.id, locked_checkout)
                .unwrap();
        crate::worktree_access::revoke_recorded(&data, &locked.id).unwrap();
        let ready = dir.path().join("seat-flock-ready");
        struct Locker(std::process::Child);
        impl Drop for Locker {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut locker = None;
        let started = std::time::Instant::now();
        let inspection = record.inspect(|| {
            locker = Some(Locker(Command::new("/usr/bin/setpriv")
                .current_dir(dir.path())
                .args(["--reuid", &b_uid.to_string(), "--regid", &b_gid.to_string(), "--clear-groups", "--", "/bin/sh", "-c",
                    r#"exec 9< "$1/HEAD"; /usr/bin/flock -n -x 9 || exit; : > "$2"; read ignored"#, "seat-flock"])
                .arg(record.private()).arg(&ready)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .spawn()?));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while !ready.exists() {
                assert!(locker.as_mut().unwrap().0.try_wait()?.is_none()
                    && std::time::Instant::now() < deadline, "seat did not acquire the read-only HEAD flock");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Ok(())
        });
        assert!(inspection.is_err());
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "seat flock blocked inspection recovery"
        );
        let receipt: serde_json::Value = serde_json::from_slice(
            &std::fs::read(crate::worktree_access::grant_path(&data, &locked.id)).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["quarantined"], true);
        assert!(crate::worktree_access::ensure_launchable(&data, &locked.id).is_err());
        drop(locker);
        eprintln!(
            "A→B→C worktree acceptance EXECUTED: parent uid={a_uid}, child uid={b_uid}, third uid={c_uid}; writes, protected config/hooks/info/config.worktree/shared logs/HEAD, sticky rename denial, sibling metadata denial, same-seat B1/B2 survival, archive/reap/rollback revocation, parent hook pin, private commit identity, exact seat trust/status/commit/push, planted-filter cleanup/foreign-seat denial, trusted-config filter owner-UID execution, config/remote/push-u mid-session inherited readers without launch/wake, SIGKILL inspection-grant recovery, and read-only HEAD-flock bounded quarantine passed"
        );
    }

    #[test]
    fn consumer_refusal_rolls_back_spawn_and_private_never_calls_consumer() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let fixture = crate::worktree::tests::Fixture::new(data);
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        std::fs::write(&fixture.fail, "worktree").unwrap();
        let err = create_with_consumer(
            &store,
            data,
            worktree_request("refused", Some("ExampleOrg/demo".into())),
            &fixture.consumer,
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeProvisioning { ref message, .. }
            if message.contains("injected policy refusal")),
            "{err}"
        );
        assert!(matches!(
            store.agent_by_name("refused"),
            Err(Error::AgentNotFound(_))
        ));
        assert_eq!(std::fs::read_dir(data.join("agents")).unwrap().count(), 0);
        let before = std::fs::read_to_string(&fixture.log).unwrap();
        let mut request = worktree_request("private", None);
        request.workdir = Some("private".into());
        let private = create_with_consumer(&store, data, request, &fixture.consumer).unwrap();
        let wd = PathBuf::from(private.workdir.unwrap());
        assert!(wd.is_dir());
        assert!(!wd.join(".git").exists());
        assert_eq!(std::fs::read_dir(wd).unwrap().count(), 0);
        assert_eq!(std::fs::read_to_string(&fixture.log).unwrap(), before);
    }

    #[test]
    fn existing_git_target_is_not_adopted_or_modified() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let fixture = crate::worktree::tests::Fixture::new(data);
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let before = crate::worktree::tests::git(&fixture.source, &["rev-parse", "HEAD"]);
        let mut request = worktree_request("existing", Some("ExampleOrg/demo".into()));
        request.workdir = Some(fixture.source.clone());
        let err = create_with_consumer(&store, data, request, &fixture.consumer).unwrap_err();
        assert!(
            matches!(err, Error::Message(ref message) if message.contains("already exists and is not empty")),
            "{err}"
        );
        assert_eq!(
            crate::worktree::tests::git(&fixture.source, &["rev-parse", "HEAD"]),
            before
        );
        assert!(fixture.source.join("excluded/large").is_file());
        assert!(!fixture.log.exists());
        assert!(matches!(
            store.agent_by_name("existing"),
            Err(Error::AgentNotFound(_))
        ));
    }

    #[test]
    fn local_repo_spawn_uses_consumer_and_refuses_unpushed_head() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let fixture = crate::worktree::tests::Fixture::new(data);
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let repo = fixture.source.display().to_string();
        let agent = create_with_consumer(
            &store,
            data,
            worktree_request("local", Some(repo.clone())),
            &fixture.consumer,
        )
        .unwrap();
        fixture.assert_checkout(Path::new(agent.workdir.as_deref().unwrap()));
        let inherited_workdir = Path::new(agent.workdir.as_deref().unwrap());
        crate::worktree::tests::git(
            inherited_workdir,
            &["commit", "--allow-empty", "-m", "Unpublished parent"],
        );
        let mut inherit = worktree_request("unpublished-parent", Some("inherit".into()));
        inherit.parent_id = Some(agent.id.clone());
        let err = create_with_consumer(&store, data, inherit, &fixture.consumer).unwrap_err();
        assert!(
            matches!(err, Error::WorktreeSourceUnpublished { ref message, .. }
            if message.contains("push HEAD first")),
            "{err}"
        );
        assert!(matches!(
            store.agent_by_name("unpublished-parent"),
            Err(Error::AgentNotFound(_))
        ));
        crate::worktree::tests::git(
            &fixture.source,
            &["commit", "--allow-empty", "-m", "Unpublished"],
        );
        let before = std::fs::read_to_string(&fixture.log).unwrap();
        let err = create_with_consumer(
            &store,
            data,
            worktree_request("unpublished", Some(repo)),
            &fixture.consumer,
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeSourceUnpublished { ref message, .. }
            if message.contains("push HEAD first")),
            "{err}"
        );
        assert!(matches!(
            store.agent_by_name("unpublished"),
            Err(Error::AgentNotFound(_))
        ));
        assert_eq!(std::fs::read_to_string(&fixture.log).unwrap(), before);
    }

    #[test]
    fn shared_agents_md_and_managed_repo_worktree_lifecycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();

        // 1. Seed shared company AGENTS.md at <data>/AGENTS.md
        std::fs::write(
            data.join("AGENTS.md"),
            "# ExampleOrg Shared Constitution\nAlways reason from first principles.\n",
        )
        .unwrap();

        let fixture = crate::worktree::tests::Fixture::new(data);
        let create = |store: &Store, data: &Path, request: CreateRequest| {
            create_with_consumer(store, data, request, &fixture.consumer)
        };

        // 3. Create L0 Root Agent
        let root_wd = data.join("root-workspace");
        std::fs::create_dir_all(&root_wd).unwrap();
        let root = create(
            &store,
            data,
            CreateRequest {
                name: "root-argon".to_string(),
                role: Some("Owner".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: Some(root_wd.clone()),
                workdir_default: Some(root_wd.clone()),
                description: None,
                instructions: Some("Root owner role description.".to_string()),
                parent_id: None,
            },
        )
        .unwrap();

        // 4. Create L1 Sub-Agent with declarative `repo: "ExampleOrg/demo"` (provisions isolated git worktree)
        let l1 = create(
            &store,
            data,
            CreateRequest {
                name: "l1-feature-worker".to_string(),
                role: Some("Feature Worker".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: Some("ExampleOrg/demo".to_string()),
                branch: Some("feat/l1-work".to_string()),
                base: Some("main".to_string()),
                workdir: None,
                workdir_default: Some(root_wd.clone()),
                description: None,
                instructions: Some("L1 worker role description.".to_string()),
                parent_id: Some(root.id.clone()),
            },
        )
        .unwrap();

        let l1_wt = PathBuf::from(l1.workdir.as_deref().unwrap());
        assert!(l1_wt.join(".git").exists(), "L1 should have a git worktree");
        assert!(l1_wt.join("README.md").is_file());
        fixture.assert_checkout(&l1_wt);

        // 5. Create L2 Nested Sub-Agent with `repo: "inherit"` (branches from L1's git repo)
        let l2 = create(
            &store,
            data,
            CreateRequest {
                name: "l2-nested-worker".to_string(),
                role: Some("Nested Worker".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: Some("inherit".to_string()),
                branch: None,
                base: None,
                workdir: None,
                workdir_default: Some(root_wd.clone()),
                description: None,
                instructions: Some("L2 nested worker role description.".to_string()),
                parent_id: Some(l1.id.clone()),
            },
        )
        .unwrap();
        let l2_wt = PathBuf::from(l2.workdir.as_deref().unwrap());
        assert_ne!(l2_wt, l1_wt);
        assert!(
            l2_wt.join(".git").exists(),
            "L2 should have its own isolated git worktree"
        );
        assert!(l2_wt.join("README.md").is_file());
        fixture.assert_checkout(&l2_wt);
        assert_eq!(
            crate::worktree::tests::git(&l1_wt, &["rev-parse", "HEAD"]),
            crate::worktree::tests::git(&l2_wt, &["rev-parse", "HEAD"]),
        );
        assert_eq!(
            std::fs::read_to_string(&fixture.log)
                .unwrap()
                .lines()
                .filter(|line| line.starts_with("worktree "))
                .count(),
            2
        );

        // 6. Spiron identity stays compact at every depth, while other
        // harness entrypoints retain the shared company instructions.
        for (ag, expected_desc) in [
            (&root, "Root owner role description."),
            (&l1, "L1 worker role description."),
            (&l2, "L2 nested worker role description."),
        ] {
            let cfg = anyagents_core::agent_config_dir(data, &ag.id);
            let prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
            let native_prompt = std::fs::read_to_string(cfg.join("AGENTS.md")).unwrap();
            assert!(native_prompt.starts_with(&prompt));
            assert_eq!(
                native_prompt
                    .matches("# ExampleOrg Shared Constitution")
                    .count(),
                1
            );
            assert!(prompt.len() < 3_500, "{}: {} bytes", ag.name, prompt.len());
            assert!(
                prompt.contains(expected_desc),
                "missing role description for {}",
                ag.name
            );
            assert!(
                !prompt.contains("## 4. Shared Instructions (AGENTS.md)")
                    && !prompt.contains("# ExampleOrg Shared Constitution"),
                "shared AGENTS.md must be loaded by the harness for {}",
                ag.name
            );
            if let Some(parent_id) = ag.parent_id.as_ref() {
                let parent = store.agent(parent_id).unwrap();
                assert!(prompt.contains(&format!("Direct parent: `agent:{}`", parent.name)));
            }
            let toml = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
            assert!(toml.contains("project_doc_max_bytes = 131072"));
        }

        // 7. Safe Worktree GC:
        //    - L2's worktree is clean and unmodified -> reaping L2 removes `l2_wt` cleanly.
        crate::server::reap_subagent_ephemeral_dirs(data, &l2.id);
        assert!(
            !l2_wt.exists(),
            "clean unmodified worktree should be reaped without recreating empty work/ dir"
        );

        //    - Dirty L1's worktree with an uncommitted file -> reaping L1 MUST preserve `l1_wt`!
        std::fs::write(l1_wt.join("uncommitted.txt"), "work in progress\n").unwrap();
        crate::server::reap_subagent_ephemeral_dirs(data, &l1.id);
        assert!(
            l1_wt.join("uncommitted.txt").is_file(),
            "dirty worktree must NEVER be deleted on archive"
        );
    }

    #[test]
    fn anthropic_usage_formats_weekly_only_when_five_hour_is_null() {
        let payload = serde_json::json!({
            "five_hour": null,
            "seven_day": {
                "utilization": 75.0,
                "resets_at": "2099-11-05T14:59:59.917112+00:00",
                "limit_dollars": null,
                "used_dollars": null,
                "remaining_dollars": null,
                "locked_reason": null
            },
            "limits": [
                {
                    "kind": "weekly_all",
                    "group": "weekly",
                    "percent": 75,
                    "severity": "warning",
                    "resets_at": "2099-11-05T14:59:59.917112+00:00",
                    "scope": null,
                    "is_active": true
                }
            ],
            "spend": {
                "used": { "amount_minor": 0, "currency": "USD", "exponent": 2 },
                "limit": null,
                "percent": 0,
                "enabled": false,
                "balance": null
            }
        });
        let formatted = format_anthropic_usage_summary(&payload, Some("max 20x")).unwrap();
        assert!(formatted.starts_with("7d: 25% left ↻"), "got: {formatted}");
        assert!(formatted.ends_with(" (max 20x)"), "got: {formatted}");
    }

    #[test]
    fn anthropic_usage_formats_both_windows_and_spend_balance() {
        let payload = serde_json::json!({
            "five_hour": {
                "utilization": 10.0,
                "resets_at": "2099-11-01T02:00:00Z"
            },
            "seven_day": {
                "utilization": 40.0,
                "resets_at": "2099-11-05T15:00:00Z"
            },
            "spend": {
                "balance": { "amount_minor": 4250, "currency": "USD", "exponent": 2 }
            }
        });
        let formatted = format_anthropic_usage_summary(&payload, Some("max 5x")).unwrap();
        assert!(formatted.contains("5h: 90% left ↻"), "got: {formatted}");
        assert!(formatted.contains(" · 7d: 60% left ↻"), "got: {formatted}");
        assert!(
            formatted.contains(" · $42.50 left (max 5x)"),
            "got: {formatted}"
        );
    }

    #[test]
    fn anthropic_profiles_never_cross_contaminate_credentials() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let def_dir = data.join("profiles/anthropic/default");
        let secondary_dir = data.join("profiles/anthropic/secondary");
        std::fs::create_dir_all(&def_dir).unwrap();
        std::fs::create_dir_all(&secondary_dir).unwrap();

        let now_ms = anyagents_core::now_ms();
        let def_cred = serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "mock-oat-default-fresh",
                "refreshToken": "mock-ort-default-refresh",
                "expiresAt": now_ms + 3_600_000,
                "accountUuid": "acct-default-uuid"
            }
        });
        let secondary_cred = serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "mock-oat-secondary-expired",
                "refreshToken": "mock-ort-secondary-refresh",
                "expiresAt": now_ms - 60_000,
                "accountUuid": "acct-secondary-uuid"
            }
        });
        std::fs::write(def_dir.join(".credentials.json"), def_cred.to_string()).unwrap();
        std::fs::write(
            secondary_dir.join(".credentials.json"),
            secondary_cred.to_string(),
        )
        .unwrap();

        sync_fresher_bound_agent_credentials(data);

        let after_secondary: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(secondary_dir.join(".credentials.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            after_secondary
                .pointer("/claudeAiOauth/refreshToken")
                .and_then(serde_json::Value::as_str),
            Some("mock-ort-secondary-refresh"),
            "secondary profile must never be overwritten by default profile credentials"
        );
    }

    #[test]
    fn signin_deduplicates_same_email_and_enforces_one_email_one_profile() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let def_dir = data.join("profiles/anthropic/default");
        let secondary_dir = data.join("profiles/anthropic/secondary");
        let agent_cfg = data.join("agents/secondary-id/config");
        std::fs::create_dir_all(&def_dir).unwrap();
        std::fs::create_dir_all(&secondary_dir).unwrap();
        std::fs::create_dir_all(&agent_cfg).unwrap();
        std::fs::write(
            agent_cfg.join(".anyagents-profile"),
            "anthropic/secondary\n",
        )
        .unwrap();

        let dup_cred = serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "",
                "refreshToken": "rtok-account-one",
                "accountUuid": "uuid-account-one",
                "email": "account-one@example.test"
            }
        });
        std::fs::write(def_dir.join(".credentials.json"), dup_cred.to_string()).unwrap();
        std::fs::write(
            secondary_dir.join(".credentials.json"),
            dup_cred.to_string(),
        )
        .unwrap();

        // Deduplicating same-email profiles merges `secondary` into `default` and updates bound agents.
        deduplicate_same_account_profiles(data);
        assert!(def_dir.is_dir());
        assert!(
            !secondary_dir.exists(),
            "duplicate profile for the same email must be removed"
        );
        assert_eq!(
            bound_profile_name(&agent_cfg).as_deref(),
            Some("anthropic/default")
        );

        // Signing in a new account (`account-two@example.test`) without `--profile`
        // creates its own distinct profile (`account-two`) without touching `default`.
        let stage1 = data.join("profiles/anthropic/.signin-staging-test1");
        std::fs::create_dir_all(&stage1).unwrap();
        std::fs::write(
            stage1.join(".credentials.json"),
            serde_json::json!({
                "claudeAiOauth": {
                    "accessToken": "",
                    "refreshToken": "rtok-account-two",
                    "accountUuid": "uuid-account-two",
                    "email": "account-two@example.test"
                }
            })
            .to_string(),
        )
        .unwrap();

        let (prof1, second_dir, acct1) =
            resolve_and_commit_staged_signin(data, "anthropic", &stage1, None).unwrap();
        assert_eq!(prof1, "account-two");
        assert_eq!(acct1.as_deref(), Some("account-two@example.test"));
        assert_eq!(
            extract_profile_account_identity(&def_dir, "anthropic")
                .0
                .as_deref(),
            Some("account-one@example.test")
        );
        assert_eq!(
            extract_profile_account_identity(&second_dir, "anthropic")
                .0
                .as_deref(),
            Some("account-two@example.test")
        );

        // Re-signing in `account-one@example.test` (even with an explicit `--profile extra`)
        // updates its existing `default` profile rather than creating a duplicate email profile.
        let stage2 = data.join("profiles/anthropic/.signin-staging-test2");
        std::fs::create_dir_all(&stage2).unwrap();
        std::fs::write(
            stage2.join(".credentials.json"),
            serde_json::json!({
                "claudeAiOauth": {
                    "accessToken": "",
                    "refreshToken": "rtok-account-one-2",
                    "accountUuid": "uuid-account-one",
                    "email": "account-one@example.test"
                }
            })
            .to_string(),
        )
        .unwrap();
        let (prof2, _, acct2) =
            resolve_and_commit_staged_signin(data, "anthropic", &stage2, Some("extra")).unwrap();
        assert_eq!(prof2, "default");
        assert_eq!(acct2.as_deref(), Some("account-one@example.test"));
        assert!(!data.join("profiles/anthropic/extra").exists());
    }

    #[test]
    fn overlay_active_quota_reset_replaces_stale_cached_5h_window() {
        let tmp = tempfile::tempdir().unwrap();
        let prof_dir = tmp.path();
        let usage_path = prof_dir.join(".anyagents-last-usage.txt");
        let cached = "5h: 49% left ↻3h01m · 7d: 63% left ↻2d09h (max 20x)";
        std::fs::write(&usage_path, format!("{cached}\n")).unwrap();
        let now_ms = anyagents_core::now_ms();
        let reset_ms = now_ms + 95 * 60 * 1_000;
        std::fs::write(
            prof_dir.join(".anyagents-quota-reset-ms"),
            format!("{reset_ms}\n"),
        )
        .unwrap();

        let overlaid =
            overlay_active_quota_reset_on_cached_usage(prof_dir, &usage_path, cached, now_ms);
        assert!(
            overlaid.starts_with("5h: 0% left ↻1h"),
            "expected 5h: 0% left with live reset ETA, got: {overlaid}"
        );
        assert!(
            overlaid.ends_with(" · 7d: 63% left ↻2d09h (max 20x)"),
            "expected 7d tail preserved, got: {overlaid}"
        );
        assert_eq!(parse_remaining_pct_from_usage(&overlaid, None), Some(0));
    }

    #[test]
    fn copy_seat_file_if_newer_syncs_rotated_refresh_token_and_preserves_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let prof_cred = tmp.path().join("profile-credentials.json");
        let seat_cred = tmp.path().join(".credentials.json");
        let now_ms = anyagents_core::now_ms();
        std::fs::write(
            &prof_cred,
            serde_json::json!({
                "claudeAiOauth": {
                    "accessToken": "old-access",
                    "refreshToken": "old-refresh-token",
                    "expiresAt": now_ms - 10_000,
                    "email": "account-two@example.test",
                    "accountUuid": "uuid-account-two"
                }
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &seat_cred,
            serde_json::json!({
                "claudeAiOauth": {
                    "accessToken": "new-access",
                    "refreshToken": "rotated-refresh-token",
                    "expiresAt": now_ms + 3_600_000
                }
            })
            .to_string(),
        )
        .unwrap();

        assert!(copy_seat_file_if_newer(&seat_cred, &prof_cred));
        let synced: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&prof_cred).unwrap()).unwrap();
        assert_eq!(
            synced
                .pointer("/claudeAiOauth/accessToken")
                .and_then(serde_json::Value::as_str),
            Some("new-access")
        );
        assert_eq!(
            synced
                .pointer("/claudeAiOauth/refreshToken")
                .and_then(serde_json::Value::as_str),
            Some("rotated-refresh-token")
        );
        assert_eq!(
            synced
                .pointer("/claudeAiOauth/email")
                .and_then(serde_json::Value::as_str),
            Some("account-two@example.test")
        );
        assert_eq!(
            synced
                .pointer("/claudeAiOauth/accountUuid")
                .and_then(serde_json::Value::as_str),
            Some("uuid-account-two")
        );
    }

    #[test]
    fn shared_agents_md_reaches_native_aliases_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let shared_path = data.join("AGENTS.md");
        let work = data.join("workspace");
        std::fs::create_dir_all(work.join(".git")).unwrap();
        std::fs::write(
            work.join("AGENTS.md"),
            "# Repository rules\nLOCAL_ONLY_RULE\n",
        )
        .unwrap();
        let shared = "# Example/shared-instructions\nSHARED_ONLY_RULE 繁體\n";

        for harness in ["agy", "claude-code", "codex", "spiron"] {
            std::fs::write(&shared_path, shared).unwrap();
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(harness, harness))
                .unwrap();
            let config = agent_config_dir(data, &agent.id);
            std::fs::create_dir_all(&config).unwrap();
            write_agent_instructions_with_continuity(&config, &agent, None, None).unwrap();

            let compact = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
            assert!(!compact.contains("SHARED_ONLY_RULE"), "{harness}");
            assert!(!compact.contains("## 4. Shared Instructions (AGENTS.md)"));
            for path in [
                "AGENTS.md",
                "CLAUDE.md",
                ".claude/CLAUDE.md",
                ".gemini/config/GEMINI.md",
            ] {
                let native = std::fs::read_to_string(config.join(path)).unwrap();
                assert!(native.starts_with(&compact), "{harness}: {path}");
                assert!(native.contains(shared.trim()), "{harness}: {path}");
                assert_eq!(
                    native.matches("SHARED_ONLY_RULE").count(),
                    1,
                    "{harness}: {path}"
                );
                assert_eq!(
                    native
                        .matches("## 4. Shared Instructions (AGENTS.md)")
                        .count(),
                    1,
                    "{harness}: {path}"
                );
            }
            assert!(!config.join(".gemini/config/AGENTS.md").exists());
            assert!(!config.join(".gemini/config/rules/AGENTS.md").exists());
        }
    }

    #[test]
    fn create_compact_prompt_removes_legacy_gemini_rules_and_bounds_continuity() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();

        let shared_constitution = format!(
            "# Example/shared-instructions\n\n{}",
            "Rule line for shared company instructions.\n".repeat(160)
        );
        std::fs::write(data.join("AGENTS.md"), &shared_constitution).unwrap();

        let agent = create(
            &store,
            data,
            CreateRequest {
                name: "sample-agent".to_string(),
                role: Some("Growth Mind".to_string()),
                harness: "agy".to_string(),
                model: Some("claude-opus-4-6-thinking#max".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: Some(data.to_path_buf()),
                description: Some("Public Layer 1 charter for Hermes.".to_string()),
                instructions: Some("Private Layer 2 execution SOP for Hermes.".to_string()),
                parent_id: None,
            },
        )
        .unwrap();

        assert_eq!(
            agent.public_description().as_deref(),
            Some("Public Layer 1 charter for Hermes.")
        );
        assert_eq!(
            agent.instructions.as_deref(),
            Some("Private Layer 2 execution SOP for Hermes.")
        );

        let cfg = anyagents_core::agent_config_dir(data, &agent.id);
        let base_prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
        assert!(base_prompt.len() < 3_500, "{} bytes", base_prompt.len());
        assert!(!base_prompt.contains("## 4. Shared Instructions (AGENTS.md)"));
        assert!(!base_prompt.contains(&shared_constitution));
        assert!(!base_prompt.contains("Cross-Harness Session Continuity"));
        assert!(!base_prompt.contains("\n<learned_memory>\n"));
        assert!(!base_prompt.contains("## Memory policy"));
        assert!(base_prompt.contains("## 3. Hierarchy, Topology & Communication Rules"));
        assert!(!base_prompt.contains("Runtime Awareness & the 6 Canonical MCP Tools"));
        assert!(!base_prompt.contains("## 2.1"));

        let gemini_md = cfg.join(".gemini/config/GEMINI.md");
        let gemini_agents = cfg.join(".gemini/config/AGENTS.md");
        let gemini_rules_agents = cfg.join(".gemini/config/rules/AGENTS.md");
        let base_native_prompt = std::fs::read_to_string(&gemini_md).unwrap();
        assert!(base_native_prompt.starts_with(&base_prompt));
        assert!(base_native_prompt.contains(shared_constitution.trim()));
        assert_eq!(
            base_native_prompt
                .matches("# Example/shared-instructions")
                .count(),
            1
        );
        assert!(!gemini_agents.exists());
        assert!(!gemini_rules_agents.exists());

        // Refresh cleans up old copies without disturbing unrelated rules.
        std::fs::create_dir_all(gemini_rules_agents.parent().unwrap()).unwrap();
        std::fs::write(&gemini_agents, &base_prompt).unwrap();
        std::fs::write(&gemini_rules_agents, &base_prompt).unwrap();
        let other_rule = cfg.join(".gemini/config/rules/custom.md");
        std::fs::write(&other_rule, "Keep this caller-owned rule.\n").unwrap();
        write_agent_instructions_with_continuity(&cfg, &agent, None, None).unwrap();
        assert!(!gemini_agents.exists());
        assert!(!gemini_rules_agents.exists());
        assert_eq!(
            std::fs::read_to_string(&gemini_md).unwrap(),
            base_native_prompt
        );
        std::fs::write(cfg.join("sylphx-cloud-session.json"), "{}").unwrap();

        // The previous symlink deduplication did not work: agy identifies the
        // global rule as user_global and scanned rules by file path.
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("GEMINI.md", &gemini_agents).unwrap();
            std::os::unix::fs::symlink("../GEMINI.md", &gemini_rules_agents).unwrap();
        }

        let huge_continuity = format!(
            "## 4. Cross-Harness Session Continuity (`claude-code` -> `agy`)\n\n{}",
            "- `[OUT (already sent/completed)]` `msg_1`: Completed verification 繁體.\n"
                .repeat(1500)
        );
        write_agent_instructions_with_continuity(&cfg, &agent, None, Some(&huge_continuity))
            .unwrap();

        let sys_prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
        assert!(sys_prompt.contains("- **Agent Name**: `sample-agent`"));
        assert!(sys_prompt.contains("- **Role Label**: `Growth Mind`"));
        assert!(sys_prompt.contains("Public Layer 1 charter for Hermes."));
        assert!(sys_prompt.contains("Private Layer 2 execution SOP for Hermes."));
        assert!(!sys_prompt.contains("## 4. Shared Instructions (AGENTS.md)"));
        assert!(!sys_prompt.contains(&shared_constitution));
        assert!(sys_prompt.starts_with(&base_prompt));
        assert!(sys_prompt.len() < base_prompt.len() + huge_continuity.len());
        assert!(sys_prompt.contains("## 4. Cross-Harness Session Continuity"));
        assert!(
            sys_prompt.len() < 24_000,
            "prompt must stay under agy's 24,000-byte per-file cap, got {} bytes",
            sys_prompt.len()
        );
        assert_eq!(
            std::fs::read_to_string(cfg.join(".sylphx-cloud-instructions-fingerprint")).unwrap(),
            "stale\n"
        );

        let native_prompt = std::fs::read_to_string(&gemini_md).unwrap();
        assert!(native_prompt.starts_with(&base_native_prompt));
        assert!(native_prompt.contains(shared_constitution.trim()));
        assert_eq!(
            native_prompt
                .matches("# Example/shared-instructions")
                .count(),
            1
        );
        assert!(native_prompt.contains("## 4. Cross-Harness Session Continuity"));
        assert!(
            native_prompt.len() < 24_000,
            "{} bytes",
            native_prompt.len()
        );
        assert!(!gemini_agents.exists());
        assert!(!gemini_rules_agents.exists());
        // exists() alone also accepts dangling symlinks; neither alias may remain.
        assert!(std::fs::symlink_metadata(&gemini_agents).is_err());
        assert!(std::fs::symlink_metadata(&gemini_rules_agents).is_err());
        assert_eq!(
            std::fs::read_to_string(&other_rule).unwrap(),
            "Keep this caller-owned rule.\n"
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("missing.md", &gemini_agents).unwrap();
            std::os::unix::fs::symlink("missing.md", &gemini_rules_agents).unwrap();
        }
        write_agent_instructions_with_continuity(&cfg, &agent, None, Some(&huge_continuity))
            .unwrap();
        assert!(std::fs::symlink_metadata(&gemini_agents).is_err());
        assert!(std::fs::symlink_metadata(&gemini_rules_agents).is_err());
        assert_eq!(std::fs::read_to_string(&gemini_md).unwrap(), native_prompt);
    }
}

#[cfg(test)]
mod archetype_tests {
    use super::*;

    #[test]
    fn data_archetypes_accept_name_metadata_and_prefer_shipped_templates() {
        let dir = tempfile::tempdir().unwrap();
        for root in [".agents/agents", "agents-templates", "archetypes"] {
            std::fs::create_dir_all(dir.path().join(root)).unwrap();
        }
        for slug in ["worker", "reviewer", "mind"] {
            std::fs::write(
                dir.path().join(format!(".agents/agents/{slug}.md")),
                format!(
                    "---\nname: {slug}\narchetype: {slug}\nscope: private\n---\nShipped {slug}."
                ),
            )
            .unwrap();
            for fallback in ["agents-templates", "archetypes"] {
                std::fs::write(
                    dir.path().join(format!("{fallback}/{slug}.md")),
                    "Fallback.",
                )
                .unwrap();
            }
            let template = resolve_archetype_template(slug, None, None, dir.path())
                .unwrap()
                .unwrap();
            assert_eq!(template.body, format!("Shipped {slug}."));
            assert_eq!(template.scope.as_deref(), Some("private"));
            std::fs::remove_file(dir.path().join(format!(".agents/agents/{slug}.md"))).unwrap();
            assert_eq!(
                resolve_archetype_template(slug, None, None, dir.path())
                    .unwrap()
                    .unwrap()
                    .body,
                "Fallback."
            );
        }
        assert!(
            parse_archetype_markdown("---\nname: worker\nunknown: rejected\n---\nBody.").is_err()
        );
    }

    #[test]
    fn composite_role_labels_resolve_fallback_archetype_templates() {
        let dir = tempfile::tempdir().unwrap();
        let templates_dir = dir.path().join(".agents/agents");
        std::fs::create_dir_all(&templates_dir).unwrap();
        for (slug, scope, body_text) in [
            ("director", "public", "Director template body."),
            ("manager", "siblings", "Manager template body."),
            ("assistant", "isolated", "Assistant template body."),
            ("worker", "private", "Worker template body."),
        ] {
            std::fs::write(
                templates_dir.join(format!("{slug}.md")),
                format!(
                    "---\nname: {slug}\nscope: {scope}\ndescription: Default {slug}\n---\n{body_text}"
                ),
            )
            .unwrap();
        }

        for (role_label, expected_scope, expected_body) in [
            (
                "工作總監 (Work Director)",
                "public",
                "Director template body.",
            ),
            ("項目總管", "public", "Director template body."),
            (
                "商業化與數值經濟主管 (Monetization & Economy Manager)",
                "siblings",
                "Manager template body.",
            ),
            ("外部聯絡專屬助理", "isolated", "Assistant template body."),
            ("專員 / Worker", "private", "Worker template body."),
        ] {
            let mut req: crate::api::CreateAgentRequest =
                serde_json::from_value(serde_json::json!({
                    "name": "sample-agent",
                    "role": role_label,
                    "instructions": "Custom assignment instructions."
                }))
                .unwrap();
            apply_archetype_defaults(dir.path(), None, &mut req).unwrap();
            assert_eq!(req.role.as_deref(), Some(role_label));
            assert_eq!(req.scope.as_deref(), Some(expected_scope));
            assert_eq!(
                req.instructions.as_deref(),
                Some(format!("{expected_body}\n\nCustom assignment instructions.").as_str())
            );
        }
    }

    #[test]
    fn archetype_frontmatter_requires_valid_yaml_and_complete_delimiters() {
        for raw in [
            "---\ndescription: broken",
            "---\ndescription: broken\n---not-a-delimiter\nbody",
            "---\ndescription missing colon\n---\nbody",
            "---\ndescription: \"unclosed\n---\nbody",
            "---\ndescription: [unclosed\n---\nbody",
            "---\ndescription: {unclosed\n---\nbody",
            "---\n: missing key\n---\nbody",
            "---\ndescription:\n---\nbody",
            "---\ndescription: ''\n---\nbody",
            "---\ndescription: first\ndescription: second\n---\nbody",
            "---\n---",
            "---\n# comment only\n---\n",
        ] {
            let error = parse_archetype_markdown(raw).unwrap_err().to_string();
            assert!(error.contains("archetype template"), "{error}");
        }

        for raw in [
            "---\ndescription: 'Reviewer: #1'\n---\nReview this change.",
            "---\r\ndescription: 'Reviewer: #1'\r\n---\r\nReview this change.",
        ] {
            let template = parse_archetype_markdown(raw).unwrap();
            assert_eq!(template.description.as_deref(), Some("Reviewer: #1"));
            assert_eq!(template.body, "Review this change.");
        }
        let fields_only = parse_archetype_markdown("---\nrole: reviewer\n---").unwrap();
        assert_eq!(fields_only.role.as_deref(), Some("reviewer"));
        assert!(fields_only.body.is_empty());
        assert_eq!(
            parse_archetype_markdown("---\n---\nBody.").unwrap().body,
            "Body."
        );
        assert_eq!(
            parse_archetype_markdown("Plain markdown.").unwrap().body,
            "Plain markdown."
        );
    }

    #[test]
    fn archetype_frontmatter_rejects_unknown_keys_and_invalid_scope() {
        for (raw, expected) in [
            (
                "---\nscpoe: isolated\n---\nUNKNOWN_KEY_BODY",
                "unknown archetype template frontmatter key \"scpoe\"",
            ),
            (
                "---\nscope: invalid-review-scope\n---\nINVALID_SCOPE_BODY",
                "invalid scope `invalid-review-scope`",
            ),
        ] {
            let error = parse_archetype_markdown(raw).unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
        }
        let template = parse_archetype_markdown("---\nscope: team\n---\nBody.").unwrap();
        assert_eq!(template.scope.as_deref(), Some("siblings"));
    }

    #[cfg(unix)]
    #[test]
    fn archetype_nofollow_opens_refuse_symlinks_but_allow_os_aliases_above_base() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let real = root.join("real");
        let templates = real.join("work/.agents/agents");
        std::fs::create_dir_all(&templates).unwrap();
        std::fs::write(templates.join("writer.md"), "Trusted template.").unwrap();
        let alias = root.join("alias");
        symlink(&real, &alias).unwrap();
        let template =
            resolve_archetype_template("writer", Some(&alias.join("work")), None, dir.path())
                .unwrap()
                .unwrap();
        assert_eq!(template.body, "Trusted template.");

        // These use the descriptor-only open directly, so the race-resistant
        // layer is independently covered rather than masked by metadata checks.
        symlink(templates.join("writer.md"), templates.join("escape.md")).unwrap();
        assert_eq!(
            open_archetype_nofollow(&templates.join("escape.md"))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ELOOP)
        );
        symlink(&templates, real.join("redirected")).unwrap();
        let error = open_archetype_nofollow(&real.join("redirected/writer.md")).unwrap_err();
        assert!(
            matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)),
            "{error}"
        );
    }
}

#[cfg(test)]
mod runtime_config_tests {
    use super::*;

    #[test]
    fn required_writes_markers_and_refresh_recovery_report_destination_failures() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("required");
        write_required_file(&path, b"first", 0o600).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"first");
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(write_required_file(&path, b"second", 0o600).is_err());
        assert!(path.is_dir());
        write_profile_marker(dir.path(), ".anyagents-profile", "provider/one\n").unwrap();
        assert_eq!(
            bound_profile_name(dir.path()).as_deref(),
            Some("provider/one")
        );
        std::fs::create_dir(dir.path().join("blocked-marker")).unwrap();
        assert!(write_profile_marker(dir.path(), "blocked-marker", "provider/two\n").is_err());
        std::fs::create_dir(dir.path().join(".credentials.json")).unwrap();
        assert!(
            persist_refreshed_credentials(dir.path(), &[".credentials.json"], b"replacement-token")
                .is_err()
        );
        assert_eq!(
            std::fs::read(dir.path().join(".anyagents-pending-credentials.json")).unwrap(),
            b"replacement-token"
        );
        std::fs::remove_dir(dir.path().join(".credentials.json")).unwrap();
        persist_refreshed_credentials(dir.path(), &[".credentials.json"], b"replacement-token")
            .unwrap();
        assert!(
            !dir.path()
                .join(".anyagents-pending-credentials.json")
                .exists()
        );
    }

    #[test]
    fn concurrent_instruction_renders_publish_one_complete_set_of_mirrors() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new(
                "concurrent-render",
                "claude-code",
            ))
            .unwrap();
        let config = agent_config_dir(dir.path(), &agent.id);
        let barrier = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            for writer in 0..4 {
                let mut agent = agent.clone();
                agent.description = Some(format!("CONCURRENT_RENDER_{writer}"));
                let config = &config;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..8 {
                        write_agent_instructions(config, &agent, None).unwrap();
                    }
                });
            }
        });
        let system = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
        let last = (0..4)
            .map(|writer| format!("CONCURRENT_RENDER_{writer}"))
            .find(|marker| system.contains(marker))
            .unwrap();
        for name in [
            "AGENTS.md",
            "CLAUDE.md",
            ".claude/CLAUDE.md",
            "opencode/AGENTS.md",
            ".config/opencode/AGENTS.md",
            ".gemini/config/GEMINI.md",
            "muse/AGENTS.md",
            ".config/muse/AGENTS.md",
            ".omp/agent/APPEND_SYSTEM.md",
            ".omp/agent/AGENTS.md",
            ".dsh/AGENTS.md",
            ".grok/AGENTS.md",
        ] {
            let mirror = std::fs::read_to_string(config.join(name)).unwrap();
            assert!(mirror.contains(&last), "{name} contains another render");
        }
    }

    #[test]
    fn prompt_and_cloud_marker_failures_are_not_acknowledged_as_synced() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let mut agent = store
            .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
            .unwrap();
        let config = agent_config_dir(dir.path(), &agent.id);
        write_agent_instructions(&config, &agent, None).unwrap();
        let text = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md")).unwrap();
        assert!(!text.contains("## Memory policy"));
        agent.launch_env.push((
            "ANYAGENTS_MEMORY_GUIDANCE".into(),
            "Use the operator-selected facts source.".into(),
        ));
        write_agent_instructions(&config, &agent, None).unwrap();
        assert!(
            std::fs::read_to_string(config.join("SYSTEM_PROMPT.md"))
                .unwrap()
                .contains("Use the operator-selected facts source.")
        );
        std::fs::remove_file(config.join("SYSTEM_PROMPT.md")).unwrap();
        std::fs::create_dir(config.join("SYSTEM_PROMPT.md")).unwrap();
        assert!(sync_agent_description_files(&store, &agent, &config).is_err());
        provision_cloud_sandbox_worktree_marker(
            &store,
            &agent,
            &config,
            "https://git.example.test/team/repo",
            Some("feature"),
            Some("main"),
        )
        .unwrap();
        let marker = config.join(".anyagents-managed-worktree.json");
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
        assert_eq!(value["branch"], "feature");
        std::fs::remove_file(&marker).unwrap();
        std::fs::create_dir(&marker).unwrap();
        assert!(
            provision_cloud_sandbox_worktree_marker(
                &store,
                &agent,
                &config,
                "example/repo",
                None,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn staged_signin_copy_failure_preserves_source_and_missing_credentials_refuse() {
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        std::fs::create_dir(&stage).unwrap();
        assert!(
            resolve_and_commit_staged_signin(dir.path(), "openai", &stage, Some("target")).is_err()
        );
        assert!(stage.is_dir());
        let bytes = br#"{"tokens":{"access_token":"fixture-only"}}"#;
        std::fs::write(stage.join("auth.json"), bytes).unwrap();
        let final_dir = dir.path().join("profiles/openai/target");
        std::fs::create_dir_all(final_dir.join("auth.json")).unwrap();
        assert!(
            resolve_and_commit_staged_signin(dir.path(), "openai", &stage, Some("target")).is_err()
        );
        assert_eq!(std::fs::read(stage.join("auth.json")).unwrap(), bytes);
        std::fs::remove_dir(final_dir.join("auth.json")).unwrap();
        let (name, destination, _) =
            resolve_and_commit_staged_signin(dir.path(), "openai", &stage, Some("target")).unwrap();
        assert_eq!(name, "target");
        assert_eq!(std::fs::read(destination.join("auth.json")).unwrap(), bytes);
        assert!(!stage.exists());
    }

    #[test]
    fn credential_propagation_does_not_seed_or_rotate_a_failed_destination() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
            .unwrap();
        let cfg = agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&cfg).unwrap();
        write_profile_marker(&cfg, ".anyagents-profile", "openai/target\n").unwrap();
        let prof = dir.path().join("profiles/openai/target");
        std::fs::create_dir_all(&prof).unwrap();
        std::fs::write(
            prof.join("auth.json"),
            br#"{"tokens":{"access_token":"fixture-only"}}"#,
        )
        .unwrap();
        std::fs::create_dir(cfg.join("auth.json")).unwrap();
        assert!(
            propagate_profile_files_to_bound_agents(dir.path(), "openai", "target", &["auth.json"])
                .is_err()
        );
        assert!(!cfg.join(".anyagents-seeded-profile").exists());
        assert!(!cfg.join(".anyagents-profile-rotated-ms").exists());
        std::fs::remove_dir(cfg.join("auth.json")).unwrap();
        propagate_profile_files_to_bound_agents(dir.path(), "openai", "target", &["auth.json"])
            .unwrap();
        assert_eq!(seeded_profile_name(&cfg).as_deref(), Some("openai/target"));
        assert!(prof.join("auth.json").is_file());
    }

    #[test]
    fn unknown_or_malformed_quota_observations_never_fabricate_remaining_quota() {
        for value in [
            serde_json::json!(null),
            serde_json::json!("0"),
            serde_json::json!({}),
            serde_json::json!(-1),
            serde_json::json!(101),
        ] {
            assert!(observed_remaining_pct(&value).is_none());
        }
        assert_eq!(observed_remaining_pct(&serde_json::json!(25)), Some(75));
        assert_eq!(observed_remaining_pct(&serde_json::json!(100)), Some(0));
        assert_eq!(observed_fraction_pct(&serde_json::json!(0.7)), Some(70));
        assert!(observed_fraction_pct(&serde_json::json!(null)).is_none());
        assert_eq!(
            observed_agy_tier(&serde_json::json!({"paidTier":{"id":"ultra"}})).as_deref(),
            Some("ultra")
        );
        for value in [
            serde_json::json!({}),
            serde_json::json!({"paidTier":{"id":17}}),
            serde_json::json!({"currentTier":{"id":"unknown"}}),
        ] {
            assert!(observed_agy_tier(&value).is_none());
        }
        assert_eq!(observed_window_min(Some(70), Some(80)), Some(70));
        assert!(observed_window_min(None, Some(80)).is_none());
        assert!(observed_window_min(Some(70), None).is_none());
    }

    #[test]
    fn display_formatting_does_not_reserve_company_style_name_prefixes() {
        assert_eq!(
            format_agent_display("owner-example", Some("Editor")),
            "owner-example · Editor"
        );
        assert_eq!(
            format_agent_display("example", Some("Editor")),
            "example · Editor"
        );
        assert_eq!(format_agent_display("owner-example", None), "owner-example");
    }
    #[test]
    fn failed_profile_cutover_keeps_binding_and_source_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
            .unwrap();
        let cfg = agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&cfg).unwrap();
        write_profile_marker(&cfg, ".anyagents-profile", "openai/previous\n").unwrap();
        let prof = dir.path().join("profiles/openai/target");
        std::fs::create_dir_all(&prof).unwrap();
        std::fs::write(prof.join("auth.json"), br#"{"account_id":"fixture-only"}"#).unwrap();
        std::fs::create_dir(cfg.join("auth.json")).unwrap();
        assert!(
            configure_agent_runtime(
                &store,
                dir.path(),
                "operator",
                &agent.name,
                None,
                None,
                Some("openai/target")
            )
            .is_err()
        );
        assert_eq!(bound_profile_name(&cfg).as_deref(), Some("openai/previous"));
        assert!(!cfg.join(".anyagents-profile-rotated-ms").exists());
        assert_eq!(
            std::fs::read(prof.join("auth.json")).unwrap(),
            br#"{"account_id":"fixture-only"}"#
        );
    }

    #[test]
    fn deduplication_retains_failed_source_and_preserves_successful_merge_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let provider = dir.path().join("profiles/openai");
        let canonical = provider.join("default");
        let duplicate = provider.join("duplicate");
        for path in [&canonical, &duplicate] {
            std::fs::create_dir_all(path).unwrap();
            std::fs::write(
                path.join("profile.json"),
                br#"{"account":"sample@example.test"}"#,
            )
            .unwrap();
        }
        let bytes = br#"{"account_id":"same-account","token":"newer-fixture"}"#;
        std::fs::write(duplicate.join("auth.json"), bytes).unwrap();
        std::fs::create_dir(canonical.join("auth.json")).unwrap();
        deduplicate_same_account_profiles(dir.path());
        assert_eq!(std::fs::read(duplicate.join("auth.json")).unwrap(), bytes);
        std::fs::remove_dir(canonical.join("auth.json")).unwrap();
        deduplicate_same_account_profiles(dir.path());
        assert_eq!(std::fs::read(canonical.join("auth.json")).unwrap(), bytes);
        assert!(!duplicate.exists());
        let recovery = std::fs::read_dir(provider)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(".merged-duplicate-")
            })
            .unwrap();
        assert_eq!(std::fs::read(recovery.join("auth.json")).unwrap(), bytes);
    }

    #[test]
    fn newer_signin_alias_is_retained_when_root_alias_cannot_commit() {
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        std::fs::create_dir_all(stage.join(".claude")).unwrap();
        std::fs::create_dir(stage.join(".credentials.json")).unwrap();
        let bytes = br#"{"claudeAiOauth":{"refreshToken":"newer-fixture"}}"#;
        std::fs::write(stage.join(".claude/.credentials.json"), bytes).unwrap();
        assert!(
            resolve_and_commit_staged_signin(dir.path(), "anthropic", &stage, Some("target"))
                .is_err()
        );
        assert_eq!(
            std::fs::read(stage.join(".claude/.credentials.json")).unwrap(),
            bytes
        );
        assert!(!dir.path().join("profiles/anthropic/target").exists());
    }

    #[test]
    fn staged_credential_validation_accepts_each_provider_and_refuses_non_credentials() {
        for (provider, rel, bytes) in [
            (
                "anthropic",
                ".credentials.json",
                br#"{"claudeAiOauth":{"refreshToken":"fixture-token"}}"#.as_slice(),
            ),
            (
                "openai",
                "auth.json",
                br#"{"OPENAI_API_KEY":"fixture-token"}"#,
            ),
            (
                "google-antigravity",
                ".gemini/antigravity-cli/antigravity-oauth-token",
                br#"{"access_token":"fixture-token"}"#,
            ),
            (
                "google-antigravity",
                ".gemini/antigravity-cli/antigravity-oauth-token",
                b"fixture-refresh-token\n",
            ),
            ("muse", "muse/auth.json", br#"{"token":"fixture-token"}"#),
            ("xai", ".grok/auth.json", br#"{"apiKey":"fixture-token"}"#),
        ] {
            let dir = tempfile::tempdir().unwrap();
            assert!(validate_staged_credentials(dir.path(), provider).is_err());
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, bytes).unwrap();
            validate_staged_credentials(dir.path(), provider).unwrap();
            for invalid in [
                b"{}".as_slice(),
                b"{invalid",
                br#"{"account_id":"metadata-only"}"#,
                br#"{"access_token":" "}"#,
            ] {
                std::fs::write(&path, invalid).unwrap();
                assert!(validate_staged_credentials(dir.path(), provider).is_err());
                assert_eq!(std::fs::read(&path).unwrap(), invalid);
            }
        }
    }

    #[test]
    fn finalization_without_usable_credentials_never_marks_agent_seeded() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("sample", "codex"))
            .unwrap();
        let cfg = agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&cfg).unwrap();
        assert!(finalize_agent_signin(dir.path(), &agent).is_err());
        assert!(!cfg.join(".anyagents-seeded-profile").exists());
        std::fs::write(cfg.join("auth.json"), b"{malformed").unwrap();
        assert!(finalize_agent_signin(dir.path(), &agent).is_err());
        assert_eq!(std::fs::read(cfg.join("auth.json")).unwrap(), b"{malformed");
        assert!(!cfg.join(".anyagents-seeded-profile").exists());
        for invalid in [
            br#"{"account_id":"metadata-only"}"#.as_slice(),
            br#"{"tokens":{"access_token":" "}}"#,
        ] {
            std::fs::write(cfg.join("auth.json"), invalid).unwrap();
            assert!(finalize_agent_signin(dir.path(), &agent).is_err());
            assert_eq!(std::fs::read(cfg.join("auth.json")).unwrap(), invalid);
            assert!(!cfg.join(".anyagents-seeded-profile").exists());
        }
    }

    #[test]
    fn rebalance_does_not_move_unknown_profiles_when_other_profiles_are_measured() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let agents: Vec<_> = (0..3)
            .map(|n| {
                store
                    .create_agent(anyagents_core::NewAgent::new(
                        format!("sample-{n}"),
                        "codex",
                    ))
                    .unwrap()
            })
            .collect();
        for agent in &agents {
            let cfg = agent_config_dir(dir.path(), &agent.id);
            write_profile_marker(&cfg, ".anyagents-profile", "openai/unknown\n").unwrap();
        }
        let entry = |profile: &str, usage: Option<&str>| ProviderProfileEntry {
            provider: "openai".into(),
            profile: profile.into(),
            kind: "subscription".into(),
            usage: usage.map(str::to_string),
            account: None,
            compatible_harnesses: vec!["codex".into()],
            assigned_agents: vec![],
            subagent_count: 0,
        };
        let entries = vec![
            entry("healthy-one", Some("5h: 80% left")),
            entry("healthy-two", Some("5h: 90% left")),
            entry("unknown", None),
        ];
        rebalance_provider_profiles(&store, dir.path(), &agents, &entries);
        for agent in &agents {
            assert_eq!(
                bound_profile_name(&agent_config_dir(dir.path(), &agent.id)).as_deref(),
                Some("openai/unknown")
            );
        }
    }
    #[test]
    fn capability_admission_rejects_foreign_identity_and_accepts_minted_agent_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let selected = store
            .create_agent(anyagents_core::NewAgent::new("selected", "codex"))
            .unwrap();
        let foreign = store
            .create_agent(anyagents_core::NewAgent::new("foreign", "codex"))
            .unwrap();
        crate::operator::ensure(&store, &foreign.id).unwrap();
        let config = agent_config_dir(dir.path(), &selected.id);
        std::fs::create_dir_all(&config).unwrap();
        let foreign_token =
            std::fs::read(agent_config_dir(dir.path(), &foreign.id).join("mcp-capability"))
                .unwrap();
        std::fs::write(config.join("mcp-capability"), foreign_token).unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir(&work).unwrap();
        let error = reconcile_agent_runtime_config(&store, dir.path(), &selected, &config, &work)
            .unwrap_err();
        assert!(error.to_string().contains("another agent"), "{error}");
        std::fs::remove_file(config.join("mcp-capability")).unwrap();
        reconcile_agent_runtime_config(&store, dir.path(), &selected, &config, &work).unwrap();
        let token = std::fs::read_to_string(config.join("mcp-capability")).unwrap();
        assert_eq!(
            store.authenticate_capability(token.trim()).unwrap(),
            selected.id
        );
    }

    #[test]
    fn shared_home_mounts_and_toolchain_caches_require_explicit_configuration() {
        // A mutex cannot protect unrelated runtime readers of these variables.
        // Keep both valid overrides and refusal inputs in an isolated process.
        const WORKER: &str = "ANYAGENTS_SHARED_HOME_TEST";
        if std::env::var_os(WORKER).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "create::runtime_config_tests::shared_home_mounts_and_toolchain_caches_require_explicit_configuration",
                    "--nocapture",
                ])
                .env(WORKER, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
            return;
        }
        let keys = [
            "ANYAGENTS_SHARED_HOME_DIRS",
            "ANYAGENTS_RUSTUP_HOME",
            "ANYAGENTS_CARGO_HOME",
        ];
        let old = keys.map(std::env::var_os);
        unsafe {
            for key in keys {
                std::env::remove_var(key);
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("config");
        ensure_seat_environment(&cfg, dir.path()).unwrap();
        assert!(!cfg.join(".rustup").exists());
        assert!(!cfg.join(".cargo").exists());
        assert!(!cfg.join(".local/share/desk-claude").exists());
        let cache = dir.path().join("selected-cache");
        std::fs::create_dir(&cache).unwrap();
        unsafe {
            std::env::set_var(keys[1], &cache);
        }
        ensure_seat_environment(&cfg, dir.path()).unwrap();
        assert_eq!(std::fs::read_link(cfg.join(".rustup")).unwrap(), cache);
        unsafe {
            std::env::set_var(keys[0], r#"["../escape"]"#);
        }
        assert!(ensure_seat_environment(&cfg, dir.path()).is_err());
        unsafe {
            for (key, value) in keys.into_iter().zip(old) {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    #[test]
    fn zero_byte_and_corrupted_mcp_configs_are_self_healed_across_profile_and_agent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("agy-sample", "agy"))
            .unwrap();
        let profile = dir.path().join("profiles/google-antigravity/default");
        std::fs::create_dir_all(profile.join(".gemini/config")).unwrap();
        std::fs::create_dir_all(profile.join(".gemini/antigravity-cli")).unwrap();
        std::fs::write(
            profile.join(".gemini/antigravity-cli/antigravity-oauth-token"),
            br#"{"access_token":"fixture-token"}"#,
        )
        .unwrap();
        let profile_mcp = profile.join(".gemini/config/mcp_config.json");
        std::fs::write(&profile_mcp, b"").unwrap();

        let cfg = agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(cfg.join(".gemini/config")).unwrap();
        write_profile_marker(&cfg, ".anyagents-profile", "google-antigravity/default\n").unwrap();
        let agent_mcp = cfg.join(".gemini/config/mcp_config.json");
        std::fs::write(&agent_mcp, b"").unwrap();

        seed_harness_credentials(&store, dir.path(), &agent, &cfg).unwrap();
        let parsed_profile: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&profile_mcp).unwrap()).unwrap();
        assert!(parsed_profile.get("mcpServers").unwrap().is_object());
        let parsed_agent: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&agent_mcp).unwrap()).unwrap();
        assert!(parsed_agent.get("mcpServers").unwrap().is_object());

        // Truncate again and verify reconcile_agent_runtime_config self-heals
        // 0-byte or truncated JSON and writes the agent's MCP capability config.
        std::fs::write(&profile_mcp, b"{truncated").unwrap();
        std::fs::write(&agent_mcp, b"").unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        reconcile_agent_runtime_config(&store, dir.path(), &agent, &cfg, &work).unwrap();
        let healed_profile: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&profile_mcp).unwrap()).unwrap();
        assert!(healed_profile.get("mcpServers").unwrap().is_object());
        let healed_agent: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&agent_mcp).unwrap()).unwrap();
        assert!(
            healed_agent
                .pointer("/mcpServers/anyagents/command")
                .and_then(serde_json::Value::as_str)
                .is_some()
        );
    }

    #[test]
    fn mid_session_anthropic_oauth_refresh_updates_profile_and_sanitized_agent_credentials() {
        const WORKER: &str = "ANYAGENTS_ANTHROPIC_REFRESH_TEST";
        if std::env::var_os(WORKER).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "create::runtime_config_tests::mid_session_anthropic_oauth_refresh_updates_profile_and_sanitized_agent_credentials",
                    "--nocapture",
                ])
                .env(WORKER, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
            return;
        }

        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let body = r#"{"access_token":"fresh-access-token","refresh_token":"fresh-refresh-token","expires_in":28800}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
            }
        });

        let token_url = format!("http://{addr}/v1/oauth/token");
        unsafe {
            std::env::set_var("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL", &token_url);
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new(
                "claude-sample",
                "claude-code",
            ))
            .unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        // Expiring in 10 minutes (< 30 minute proactive refresh threshold).
        let expiring_soon = now_ms + 10 * 60 * 1000;
        let profile = dir.path().join("profiles/anthropic/default");
        std::fs::create_dir_all(profile.join(".claude")).unwrap();
        let initial_profile_creds = serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "old-access-token",
                "refreshToken": "old-refresh-token",
                "expiresAt": expiring_soon,
                "subscriptionType": "max"
            }
        });
        let raw_initial = serde_json::to_vec(&initial_profile_creds).unwrap();
        std::fs::write(profile.join(".credentials.json"), &raw_initial).unwrap();
        std::fs::write(profile.join(".claude/.credentials.json"), &raw_initial).unwrap();

        let cfg = agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(cfg.join(".claude")).unwrap();
        write_profile_marker(&cfg, ".anyagents-profile", "anthropic/default\n").unwrap();
        let stale_agent_creds = serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "old-access-token",
                "refreshToken": null,
                "expiresAt": expiring_soon,
                "subscriptionType": "max"
            }
        });
        let raw_stale = serde_json::to_vec(&stale_agent_creds).unwrap();
        std::fs::write(cfg.join(".credentials.json"), &raw_stale).unwrap();
        std::fs::write(cfg.join(".claude/.credentials.json"), &raw_stale).unwrap();

        sync_fresher_bound_agent_credentials(dir.path());
        server.join().unwrap();

        for rel in [".credentials.json", ".claude/.credentials.json"] {
            let prof_val: serde_json::Value =
                serde_json::from_slice(&std::fs::read(profile.join(rel)).unwrap()).unwrap();
            assert_eq!(
                prof_val
                    .pointer("/claudeAiOauth/accessToken")
                    .and_then(serde_json::Value::as_str),
                Some("fresh-access-token")
            );
            assert_eq!(
                prof_val
                    .pointer("/claudeAiOauth/refreshToken")
                    .and_then(serde_json::Value::as_str),
                Some("fresh-refresh-token")
            );

            let agent_val: serde_json::Value =
                serde_json::from_slice(&std::fs::read(cfg.join(rel)).unwrap()).unwrap();
            assert_eq!(
                agent_val
                    .pointer("/claudeAiOauth/accessToken")
                    .and_then(serde_json::Value::as_str),
                Some("fresh-access-token")
            );
            assert!(
                agent_val
                    .pointer("/claudeAiOauth/refreshToken")
                    .is_some_and(serde_json::Value::is_null),
                "seat credentials must strip refreshToken: {agent_val}"
            );
            assert!(
                agent_val
                    .pointer("/claudeAiOauth/expiresAt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap()
                    > expiring_soon
            );
        }

        unsafe {
            std::env::remove_var("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL");
        }
    }
}
