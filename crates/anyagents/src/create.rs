//! `agents.create`. The agent row, its directories, and its MCP registration
//! are one step. The harness config is written only under the agent's own
//! config directory.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, AgentId, Error, NewAgent, Store, agent_config_dir, agent_work_dir};

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
    pub instructions: Option<String>,
    pub parent_id: Option<AgentId>,
}

pub fn format_agent_display(name: &str, role: Option<&str>) -> String {
    if name.starts_with("owner-") {
        return name.to_string();
    }
    match role.map(str::trim).filter(|s| !s.is_empty()) {
        Some(r) => format!("{name} · {r}"),
        None => name.to_string(),
    }
}

pub fn create(store: &Store, data: &Path, request: CreateRequest) -> Result<Agent, Error> {
    let normalized_harness = match request.harness.trim() {
        "sylphx-agent" | "sylphx_agent" => "sylphx".to_string(),
        other => other.to_string(),
    };
    // A harness is installed only if an adapter can start it.
    install::adapter(&normalized_harness)?;
    let mut new = NewAgent::new(request.name, normalized_harness.clone());
    new.role = request.role;
    new.model = request
        .model
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .or_else(|| {
            (normalized_harness == "sylphx")
                .then(|| crate::sylphx::DEFAULT_SYLPHX_MODEL.to_string())
        });
    new.instructions = request.instructions;
    new.parent_id = request.parent_id;
    let agent = store.create_agent(new)?;
    let data = std::path::absolute(data)?;
    let finish = (|| -> Result<Agent, Error> {
        // Nothing is installed until an agent needs it: this is that moment. A
        // failure is the reason the agent's first turn cannot start, reported here
        // and never retried in a loop.
        install::record(&data, &agent.harness).map_err(Error::Message)?;
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
        std::fs::create_dir_all(&config)?;
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
            write_profile_marker(&config, ".anyagents-profile", &format!("{normalized}\n"));
        }
        let wd_str = request
            .workdir
            .as_ref()
            .and_then(|p| p.to_str())
            .map(str::trim)
            .unwrap_or("");
        let wd_is_worktree_keyword = matches!(
            wd_str.to_ascii_lowercase().as_str(),
            "worktree" | "branch" | "isolated"
        );
        let wd_is_private_keyword = matches!(
            wd_str.to_ascii_lowercase().as_str(),
            "private" | "sandbox" | "self"
        );
        let wd_is_inherit_keyword = matches!(
            wd_str.to_ascii_lowercase().as_str(),
            "inherit" | "parent"
        );
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
        let workdir_created =
            explicit_workdir.as_ref().is_some_and(|p| !p.exists()) || effective_repo.is_some();
        let managed_abs = std::path::absolute(&managed)?;
        let cwd = if let Some(repo_raw) = effective_repo {
            let target_wt = match &explicit_workdir {
                Some(path) => std::path::absolute(path)?,
                None => managed_abs.clone(),
            };
            provision_managed_worktree(
                store,
                &data,
                &agent,
                &config,
                repo_raw,
                request.branch.as_deref(),
                request.base.as_deref(),
                &target_wt,
                parent_name.as_deref(),
            )?;
            target_wt
        } else if wd_is_private_keyword {
            managed_abs.clone()
        } else if wd_is_inherit_keyword {
            let inherited_parent_wd = agent
                .parent_id
                .as_ref()
                .and_then(|pid| store.agent(pid).ok())
                .and_then(|p| p.workdir)
                .map(PathBuf::from)
                .filter(|p| p.is_dir());
            if let Some(parent_wd) = inherited_parent_wd {
                std::path::absolute(&parent_wd)?
            } else {
                managed_abs.clone()
            }
        } else {
            match &explicit_workdir {
                Some(path) => {
                    std::fs::create_dir_all(path)?;
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
                        );
                    }
                    abs
                }
                None => {
                    if agent.parent_id.is_some() {
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
        let _ = crate::operator::mint(store, &agent.id);
        store.set_agent_launch_env(&agent.id, &pairs)?;
        let mut pre_running = agent.clone();
        pre_running.workdir = Some(cwd.display().to_string());
        reconcile_agent_runtime_config(store, &data, &pre_running, &config, &cwd)?;
        let running = store.set_agent_running(&agent.id, &cwd.display().to_string())?;
        let should_share_cwd = (workdir_created
            || cwd == managed_abs
            || explicit_workdir.is_some())
            && cwd != Path::new("/scratch/wt/owners-main")
            && cwd != Path::new("/scratch/wt");
        if should_share_cwd
            && let Some((_, _, gid, _)) = resolved_agent_seat_info(store, &running)
        {
            crate::sandbox::share_worktree_with_seat(&cwd, gid);
        }
        Ok(running)
    })();
    if finish.is_err() {
        let _ = store.delete_agent(&agent.id);
        let _ = std::fs::remove_dir_all(data.join("agents").join(agent.id.as_str()));
    }
    finish
}

pub(crate) fn provision_managed_worktree(
    store: &Store,
    data: &Path,
    agent: &Agent,
    config: &Path,
    repo_raw: &str,
    branch_opt: Option<&str>,
    base_opt: Option<&str>,
    target_wt: &Path,
    parent_name: Option<&str>,
) -> Result<(), Error> {
    if target_wt.exists() {
        if target_wt.join(".git").exists() {
            record_managed_worktree_marker(config, target_wt, repo_raw, branch_opt);
            return Ok(());
        }
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
        add_git_worktree_from_local_repo(&parent_wd, target_wt, &branch, base)?;
    } else if Path::new(repo_raw).is_dir() && is_git_repo_dir(Path::new(repo_raw)) {
        add_git_worktree_from_local_repo(Path::new(repo_raw), target_wt, &branch, base)?;
    } else if repo_raw.contains('/') && !repo_raw.contains("..") {
        let desk_hub_bin = Path::new("/home/kyle/.local/bin/desk-git-hub");
        let hub_fallback = Path::new("/workspace-data/desk-git-hubs")
            .join(repo_raw.replacen('/', "-", 1));
        let data_hub_fallback = data.join("git-hubs").join(repo_raw);
        let data_repos_hub = data.join("repos").join(
            repo_raw
                .trim_start_matches("https://github.com/")
                .trim_end_matches(".git"),
        );
        if desk_hub_bin.is_file()
            && !crate::sandbox::is_isolated_test_env()
            && !repo_raw.starts_with("http://")
            && !repo_raw.starts_with("https://")
        {
            if let Some(b) = base
                && !b.starts_with("origin/")
                && b != "HEAD"
            {
                let _ = std::process::Command::new(desk_hub_bin)
                    .args(["fetch", repo_raw, b])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
            }
            let mut cmd = std::process::Command::new(desk_hub_bin);
            cmd.args(["worktree", repo_raw]).arg(target_wt).arg(&branch);
            if let Some(b) = base {
                let start_ref = if b.starts_with("origin/") || b == "HEAD" {
                    b.to_string()
                } else {
                    format!("origin/{b}")
                };
                cmd.arg(start_ref);
            }
            let out = cmd.output()?;
            if !out.status.success() {
                if hub_fallback.is_dir() && is_git_repo_dir(&hub_fallback) {
                    add_git_worktree_from_local_repo(
                        &hub_fallback,
                        target_wt,
                        &branch,
                        Some(base.unwrap_or("origin/HEAD")),
                    )?;
                } else {
                    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
                    return Err(Error::Message(format!(
                        "failed to provision worktree for `{repo_raw}`: {stderr}"
                    )));
                }
            }
        } else if data_hub_fallback.is_dir() && is_git_repo_dir(&data_hub_fallback) {
            add_git_worktree_from_local_repo(&data_hub_fallback, target_wt, &branch, base)?;
        } else if data_repos_hub.is_dir() && is_git_repo_dir(&data_repos_hub) {
            add_git_worktree_from_local_repo(&data_repos_hub, target_wt, &branch, base)?;
        } else if hub_fallback.is_dir() && is_git_repo_dir(&hub_fallback) {
            add_git_worktree_from_local_repo(
                &hub_fallback,
                target_wt,
                &branch,
                Some(base.unwrap_or("origin/HEAD")),
            )?;
        } else if !crate::sandbox::is_isolated_test_env() {
            let remote_url = if repo_raw.starts_with("http://")
                || repo_raw.starts_with("https://")
                || repo_raw.starts_with("git@")
            {
                repo_raw.to_string()
            } else {
                format!("https://github.com/{repo_raw}.git")
            };
            if let Some(parent_hub) = data_repos_hub.parent() {
                std::fs::create_dir_all(parent_hub)?;
            }
            let clone_out = std::process::Command::new("git")
                .args(["clone", "--no-checkout", &remote_url])
                .arg(&data_repos_hub)
                .output()?;
            if !clone_out.status.success() {
                let stderr = String::from_utf8_lossy(&clone_out.stderr).trim().to_string();
                return Err(Error::Message(format!(
                    "git clone for `{repo_raw}` failed: {stderr}"
                )));
            }
            add_git_worktree_from_local_repo(
                &data_repos_hub,
                target_wt,
                &branch,
                Some(base.unwrap_or("origin/HEAD")),
            )?;
        } else {
            return Err(Error::Message(format!(
                "repository `{repo_raw}` not found locally"
            )));
        }
    } else {
        return Err(Error::Message(format!(
            "invalid `repo` value `{repo_raw}`: pass `<org>/<repo>`, `\"inherit\"`, or a local git repository path"
        )));
    }

    let _ = std::process::Command::new("git")
        .arg("-C")
        .arg(target_wt)
        .args(["config", "--local", "user.name", "Kyle Tse"])
        .status();
    let _ = std::process::Command::new("git")
        .arg("-C")
        .arg(target_wt)
        .args(["config", "--local", "user.email", "hi@kylet.se"])
        .status();

    #[cfg(unix)]
    if let Some((_, _, gid, _)) = resolved_agent_seat_info(store, agent).or_else(|| {
        crate::sandbox::seat_for_agent_with_hint(&agent.name, parent_name)
            .map(|(u, uid, gid, _)| (u, uid, gid, None))
    }) {
        crate::sandbox::share_worktree_with_seat(target_wt, gid);
    }

    record_managed_worktree_marker(config, target_wt, repo_raw, Some(&branch));
    Ok(())
}

fn is_git_repo_dir(path: &Path) -> bool {
    std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "--git-dir"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn add_git_worktree_from_local_repo(
    source_repo: &Path,
    target_wt: &Path,
    branch: &str,
    base: Option<&str>,
) -> Result<(), Error> {
    let start_ref = base.unwrap_or("HEAD");
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(source_repo)
        .args(["worktree", "add", "-B", branch])
        .arg(target_wt)
        .arg(start_ref)
        .output()?;
    if out.status.success() {
        return Ok(());
    }
    let fallback = std::process::Command::new("git")
        .arg("-C")
        .arg(source_repo)
        .args(["worktree", "add", "--detach"])
        .arg(target_wt)
        .arg(start_ref)
        .output()?;
    if fallback.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    Err(Error::Message(format!(
        "git worktree add from `{}` failed: {stderr}",
        source_repo.display()
    )))
}

fn record_managed_worktree_marker(
    config: &Path,
    target_wt: &Path,
    repo_raw: &str,
    branch: Option<&str>,
) {
    let initial_head = std::process::Command::new("git")
        .arg("-C")
        .arg(target_wt)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let payload = serde_json::json!({
        "path": target_wt.display().to_string(),
        "repo": repo_raw,
        "branch": branch.unwrap_or(""),
        "initial_head": initial_head,
    });
    if let Ok(bytes) = serde_json::to_vec_pretty(&payload) {
        let _ = write_seat_shared_file(
            &config.join(".anyagents-managed-worktree.json"),
            &bytes,
            0o664,
        );
    }
}

pub(crate) fn sync_all_agent_instructions(store: &Store, data: &Path) {
    let Ok(agents) = store.list_agents() else {
        return;
    };
    #[cfg(unix)]
    let host_home = crate::sandbox::host_home_dir();
    for agent in &agents {
        let cfg = agent_config_dir(data, &agent.id);
        if !cfg.is_dir() {
            continue;
        }
        #[cfg(unix)]
        if let Some(ref hh) = host_home {
            ensure_seat_skills(&cfg, hh);
        }
        let work_dir = agent
            .workdir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| agent_work_dir(data, &agent.id));
        let _ = reconcile_agent_runtime_config(store, data, agent, &cfg, &work_dir);
    }
}

pub(crate) fn write_agent_instructions(config: &Path, agent: &Agent, parent_name: Option<&str>) {
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
    write_agent_instructions_with_continuity(config, agent, parent_name, cached_bridge.as_deref());
}

pub(crate) fn write_agent_instructions_with_continuity(
    config: &Path,
    agent: &Agent,
    parent_name: Option<&str>,
    continuity: Option<&str>,
) {
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
    if let Some(parent) = parent_name {
        doc.push_str(&format!(
            "- **Role**: Sub-Agent spawned by `agent:{parent}` (addressable as `\"parent\"` or `\"agent:{parent}\"`). Stay `running` across as many turns as needed until your assigned goal is completely finished and verified; once you have sent your final report to `\"parent\"` via `send`, call `agents({{ \"action\": \"archive\" }})` to retire yourself.\n"
        ));
    } else {
        doc.push_str("- **Role**: Root Agent\n");
    }

    if let Some(custom) = agent
        .instructions
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        doc.push_str("\n## 2. Role, Responsibilities & Standing Description\n\n");
        doc.push_str(custom);
        doc.push('\n');
    }

    doc.push_str(
        "\n## 3. Runtime Awareness & the 6 Canonical MCP Tools (`agents`, `send`, `conversation`, `groups`, `schedule`, `browser`)\n\n\
         - **Cache-Stable Architecture (Static System Prompt + Tail Events & On-Demand Tools)**:\n\
           - This System Prompt is **strictly static** so your LLM prompt prefix KV cache is never invalidated when groups, channels, peers, or sub-agents change.\n\
           - Live routing context arrives **inline per message** at the tail of your context inside the `<message ...>` XML envelope (`id`, `from`, `sender_name`, `to`, `channel`, `chat`, `chat_title`, `chat_type`, `topic`, `event`, `in_reply_to`, `mentions`, `reply_to`, `answer_to`).\n\
           - Always call your native **AnyAgents MCP tools** (`agents`, `send`, `conversation`, `groups`, `schedule`, `browser`) directly (or via `await tools.anyagents.<tool>({...})` inside `execute` in OpenCode Code Mode) — they are schema-typed, zero-overlap, and automatically authenticated as your seat.\n\
         - **1. Unified Agent & Sub-Agent Manager (`agents` MCP Tool — Native Harness Sub-Agents Disabled by Default)**:\n\
           - Built-in harness sub-agent tools (`Task`/`Agent`/`task`/`invoke_subagent`) are **disabled by default** so sub-agents are never trapped inside a single vendor CLI. Always use `agents` for all peer, role, and sub-agent operations:\n\
             - **List Peers or Sub-Agents**: `agents({ \"action\": \"list\" })` (all agents), `agents({ \"action\": \"list\", \"parent\": \"self\" })` (your sub-agents + summary counts), `agents({ \"action\": \"list\", \"parent\": \"root\" })` (root agents), with optional `state`: `\"running\"` | `\"working\"` | `\"paused\"` | `\"archived\"` | `\"all\"`.\n\
             - **Inspect Status & Recent Turns**: `agents({ \"action\": \"inspect\", \"name\": \"<name-or-id>\", \"limit\": 10 })` (live status, schedules, and recent conversation turns in one call).\n\
             - **Spawn a Sub-Agent**: `agents({ \"action\": \"spawn\", \"name\": \"<optional-name>\", \"repo\": \"<org>/<repo>\" | \"inherit\", \"branch\": \"<optional-branch>\", \"base\": \"<optional-base>\", \"workdir\": \"<optional-path-or-mode>\", \"description\": \"...\", \"prompt\": \"...\" })`. When `harness`/`model`/`profile` are omitted, AnyAgents automatically uses the configured sub-agent default (`opencode` + `sylphx/openai/gpt-6.1-sol#xhigh` + `sylphx/default`, or pass `harness: \"inherit\"` to inherit your own harness/model/profile).\n\
             - **Read or Update Role Description (`describe`)**: `agents({ \"action\": \"describe\", \"name\": \"self\" })` (read your own role), `agents({ \"action\": \"describe\", \"name\": \"self\", \"description\": \"...\" })` (update your own role/mission description; syncs your private system prompt and group roster), `agents({ \"action\": \"describe\", \"name\": \"all\" })` (read every peer's role & responsibilities).\n\
             - **Steer & Lifecycle Control**: `agents({ \"action\": \"steer\", \"name\": \"<name-or-id>\", \"prompt\": \"...\" })`, `agents({ \"action\": \"wake\" | \"pause\" | \"archive\" | \"delete\", \"name\": \"<name-or-id>\" })` (omit `name` on `\"archive\"` to archive yourself).\n\
             - **Admin / Configuration**: `agents({ \"action\": \"create\" | \"rename\" | \"role\" | \"harness\" | \"models\", ... })`.\n\
         - **Per-Agent Workspace (`workdir` / `repo`), Managed Git Worktrees & Shared Context Architecture**:\n\
           - **Every Agent Owns Its Own Workspace**: Working directories and Git worktrees belong to each individual agent (`<data>/agents/<id>/work` or a chosen path), never a single system-wide directory. Multi-level sub-agent trees (Root -> L1 -> L2+) are supported at any depth.\n\
           - **Choosing a Sub-Agent's Workspace (`repo` & `workdir` on `agents` `action = \"spawn\"`)**:\n\
             - **Target a Specific Repository (`repo: \"<org>/<repo>\"` or git path/URL)**: Automatically provisions an isolated Git worktree for that agent at `<data>/agents/<id>/work` on branch `agent/<name>` (or custom `branch` from `base`).\n\
             - **Isolated Branch of Your Current Repository (`repo: \"inherit\"` or `workdir: \"worktree\"`)**: Branches a new isolated Git worktree at `<data>/agents/<id>/work` from your current Git repo and `HEAD` — use this whenever spawning parallel coding sub-agents on the same repo so they never collide on `.git/index.lock` or uncommitted edits.\n\
             - **Private Empty Sandbox (`workdir: \"private\"`)**: Gives the agent its own clean directory at `<data>/agents/<id>/work`.\n\
             - **Default When `repo` and `workdir` Are Omitted**: A sub-agent inherits its parent's `workdir` (`\"inherit\"`) so it immediately works in the same directory/repo as its parent; a root agent defaults to its own isolated `<data>/agents/<id>/work`.\n\
           - **Universal Shared `AGENTS.md` & `Skills` + Repo-Local Layering**: Global instructions (`<data>/AGENTS.md`) and global `skills` (`<data>/skills`) are mounted inside every agent's `config/` home directory, so switching `repo` or `workdir` never loses shared instructions or skills; meanwhile, any repo-specific `<workdir>/AGENTS.md` or `.agents/skills` in the target repo is layered on top automatically.\n\
           - **Safe Automatic Worktree Lifecycle**: While a sub-agent is `idle` between turns, its Git worktree is preserved so its parent can inspect or `steer` it. When `archive` is called, AnyAgents checks if the managed worktree is clean (`git status --porcelain` empty) and has no unpushed commits: if clean/pushed, it runs `git worktree remove` and deletes the directory automatically; if uncommitted or unpushed work remains, it preserves the worktree on disk so zero code is ever lost.\n\
         - **2. Outbound Messages, Rich Media & Message Interactions (`send` MCP Tool)**:\n\
           - Plain text printed only to your local session/stdout is **never** delivered to other agents, your parent, groups, or channels. Whenever a message or task asks you to reply, answer, or report back, you **must** call `send` (`to`: e.g. `[\"parent\"]`, `[\"sage\"]`, `[\"group:owners\"]`, or `answer_to` / `from`; `reply_to`: the `anyagents:reply:msg_...` token from `<message>`; `text`, plus optional `media`, `location`, `poll`, `checklist`, `article`).\n\
           - **Reactions, Poll Votes & Edits**: Pass `action`: `\"react\"` (with `emoji`, e.g. `\"👍\"`, `\"🔥\"`), `\"vote\"` (with `options`, e.g. `[\"1\"]`), `\"check\"` / `\"uncheck\"`, `\"edit\"`, `\"delete\"`, `\"pin\"`, `\"unpin\"`, or `\"stop_poll\"` along with `target` (or `reply_to`: `anyagents:reply:msg_...`) on `send`.\n\
         - **3. Memory, History & Hybrid RAG Search (`conversation` MCP Tool)**:\n\
           - Incoming messages are pushed automatically to your session — never poll an inbox. Pass `query` (`conversation({ \"query\": \"...\", \"agent\": \"all\", \"limit\": 20 })`) to run Hybrid RAG search (dense vector embedding `voyageai/voyage-4-large` + SQLite FTS5 BM25) across all agents' historical and live conversations without loading the full history. Omit `query` (`conversation({ \"agent\": \"sage\", \"limit\": 50 })`) to read recent chronological two-way (`in` + `out`) turns.\n\
         - **4. Internal Multi-Agent Groups (`groups` MCP Tool)**:\n\
           - `groups({ \"action\": \"list\" })` (returns `members` + full `roster` with each member's `description`), `groups({ \"action\": \"join\" | \"leave\", \"group\": \"<name>\" })`.\n\
         - **5. Self-Wakes & Event Watches (`schedule` MCP Tool — Native Harness Timers/Monitors Disabled by Default)**:\n\
           - Schedule your own one-off (`at`) or recurring (`every`) wakes (`schedule({ \"every\": \"15m\", \"prompt\": \"...\" })`, `schedule({ \"at\": \"1m\", \"prompt\": \"...\" })`) or external/local event watches (`schedule({ \"watch\": \"github:pr:<owner>/<repo>#<num>\" | \"github:check:<owner>/<repo>@<ref>\" | \"spiron:promise:<id>\" | \"agent:idle:<name>\" | \"pid:<pid>\" | \"file:<path>\", \"prompt\": \"...\" })`), list them (`schedule({ \"action\": \"list\" })`), or cancel them (`schedule({ \"action\": \"cancel\", \"id\": \"<sch_...>\" | \"all\" })`). Wakes fire only when you are idle and not quota-blocked, and a missed wake never double-fires.\n\
         - **6. Native SOTA Anti-Detect Browser & Persistent Browser Profiles (`browser` MCP Tool)**:\n\
           - Runs real headed Chromium on a virtual X11 display (`xvfb-headed`) with deterministic per-profile anti-detect fingerprinting (WebGL vendor/renderer, Client Hints `Sec-CH-UA`, `navigator.webdriver = false`, WebRTC UDP leak protection, Canvas seed), hardware-trusted CDP input (`event.isTrusted === true`), and persistent `--user-data-dir` login profiles.\n\
           - **Zero-Knowledge Credential & 2FA OTP Fill**: Never read a plaintext password into your LLM context to type it into a browser. Pass a 1Password or credential reference directly in `text` or `secret_ref` (`browser({ \"action\": \"fill\", \"selector\": \"@e2\", \"secret_ref\": \"op://Vault/Item/password\" })` or `\"op-otp://Vault/Item\"` for live 6-digit 2FA TOTP, or `\"secret://<ref>\"`); the daemon resolves and types it via hardware CDP in memory without exposing the secret to LLM logs.\n\
           - Actions: `browser({ \"action\": \"open\", \"url\": \"https://...\", \"profile\": \"<optional-profile>\" })` (launches on demand and returns visible text + `@e1`, `@e2` interactive element refs), `browser({ \"action\": \"snapshot\" })`, `browser({ \"action\": \"click\", \"selector\": \"@e1\" })`, `browser({ \"action\": \"fill\", \"selector\": \"@e2\", \"text\": \"...\", \"submit\": true })`, `browser({ \"action\": \"press\", \"key\": \"Enter\" })`, `browser({ \"action\": \"eval\", \"script\": \"...\" })`, `browser({ \"action\": \"screenshot\" })` (returns PNG path ready to inspect or send via `send` media), `browser({ \"action\": \"cookies\" })`, `browser({ \"action\": \"tabs\" })`, `browser({ \"action\": \"profiles\" })`, and `browser({ \"action\": \"close\" })` (scales the browser process to zero while keeping the profile logged in on disk).\n",
    );

    if let Some(shared) = read_shared_agents_md(config) {
        doc.push_str("\n## 4. Shared Instructions (AGENTS.md)\n\n");
        doc.push_str(&shared);
        doc.push('\n');
    }

    if let Some(cont) = continuity.map(str::trim).filter(|s| !s.is_empty()) {
        doc.push('\n');
        doc.push_str(cont);
        doc.push('\n');
    }

    let paths = [
        config.join("SYSTEM_PROMPT.md"),
        config.join("AGENTS.md"),
        config.join("opencode").join("AGENTS.md"),
        config.join(".config").join("opencode").join("AGENTS.md"),
        config.join("CLAUDE.md"),
        config.join(".claude").join("CLAUDE.md"),
        config.join(".gemini").join("config").join("GEMINI.md"),
        config.join(".gemini").join("config").join("AGENTS.md"),
        config
            .join(".gemini")
            .join("config")
            .join("rules")
            .join("AGENTS.md"),
        config.join("muse").join("AGENTS.md"),
        config.join(".config").join("muse").join("AGENTS.md"),
        config.join(".omp").join("agent").join("APPEND_SYSTEM.md"),
        config.join(".omp").join("agent").join("AGENTS.md"),
        config.join(".dsh").join("AGENTS.md"),
        config.join(".grok").join("AGENTS.md"),
    ];
    for path in &paths {
        if std::fs::read(path).ok().as_deref() == Some(doc.as_bytes()) {
            continue;
        }
        let _ = write_seat_shared_file(path, doc.as_bytes(), 0o664);
    }
}

/// Fast, non-intrusive instruction sync used by `describe()`: updates the
/// agent's system prompt files (`AGENTS.md`, `CLAUDE.md`, `SYSTEM_PROMPT.md`,
/// `config.toml` developer instructions) without walking or re-chowning the
/// `config/` or `work/` directory trees, and guarantees `config`'s existing
/// group and execute bits remain intact.
pub(crate) fn sync_agent_description_files(store: &Store, agent: &Agent, config: &Path) {
    #[cfg(unix)]
    let pre_existing_mode = {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(config)
            .ok()
            .map(|m| m.permissions().mode() & 0o7777)
    };
    let _ = std::fs::create_dir_all(config);
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
    let continuity = compute_live_session_continuity(store, agent, config);
    write_agent_instructions_with_continuity(
        config,
        agent,
        parent_name.as_deref(),
        continuity.as_deref(),
    );
    if agent.harness == "codex" {
        let codex_toml = config.join("config.toml");
        if codex_toml.is_file() {
            let _ = mcp_config::sync_codex_developer_instructions(
                &codex_toml,
                continuity.as_deref(),
            );
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
            let _ = std::fs::set_permissions(config, std::fs::Permissions::from_mode(safe_mode));
        }
    }
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

pub(crate) fn ensure_seat_environment(target_config: &Path) {
    for subdir in ["tmp", "data-home", "cache-home", "state-home"] {
        let _ = std::fs::create_dir_all(target_config.join(subdir));
    }
    let gitconfig = target_config.join(".gitconfig");
    if !gitconfig.exists() {
        let _ = std::fs::write(&gitconfig, "[safe]\n\tdirectory = *\n");
    }
    #[cfg(unix)]
    if let Some(host_home) = crate::sandbox::host_home_dir() {
        let host_local = host_home.join(".local");
        if host_local.is_dir() {
            let local_dir = target_config.join(".local");
            let _ = std::fs::create_dir_all(&local_dir);
            let opt_link = local_dir.join("opt");
            let host_opt = host_local.join("opt");
            if host_opt.is_dir() && !opt_link.exists() {
                let _ = std::os::unix::fs::symlink(&host_opt, &opt_link);
            }
            let bin_dir = local_dir.join("bin");
            let _ = std::fs::create_dir_all(&bin_dir);
            let host_bin = host_local.join("bin");
            if let Ok(entries) = std::fs::read_dir(&host_bin) {
                for entry in entries.flatten() {
                    let src = entry.path();
                    let dst = bin_dir.join(entry.file_name());
                    if src.exists() && !dst.exists() {
                        let _ = std::os::unix::fs::symlink(&src, &dst);
                    }
                }
            }
        }
        for shared_dir in [".kube", ".config/gh"] {
            let src = host_home.join(shared_dir);
            let dst = target_config.join(shared_dir);
            if src.exists() && !dst.exists() {
                if let Some(parent) = dst.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::os::unix::fs::symlink(&src, &dst);
            }
        }
        for (rel, shared_fallback) in [
            (".rustup", "/scratch/rustup-home"),
            (".cargo", "/scratch/cargo-registry"),
        ] {
            let dst = target_config.join(rel);
            if !dst.exists() {
                let h = host_home.join(rel);
                let found_shared = if h.is_dir() {
                    Some(h)
                } else {
                    let fallback = Path::new(shared_fallback);
                    fallback.is_dir().then(|| fallback.to_path_buf())
                };
                if let Some(src) = found_shared {
                    let _ = std::os::unix::fs::symlink(&src, &dst);
                }
            }
        }
        ensure_seat_skills(target_config, &host_home);
    }
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
    let path = data_dir.join("AGENTS.md");
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(unix)]
pub(crate) fn sync_canonical_skills(data_dir: &Path) {
    let data_skills = data_dir.join("skills");
    if let Ok(skills_dir) = std::fs::canonicalize(&data_skills)
        && let Some(repo_root) = skills_dir.parent()
        && repo_root.join(".git").exists()
    {
        let is_clean = std::process::Command::new("git")
            .arg("-C")
            .arg(repo_root)
            .args(["status", "--porcelain"])
            .output()
            .ok()
            .is_some_and(|o| o.status.success() && o.stdout.is_empty());
        if is_clean {
            let _ = std::process::Command::new("git")
                .arg("-C")
                .arg(repo_root)
                .args(["pull", "--ff-only", "--quiet", "origin", "main"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

#[cfg(unix)]
pub(crate) fn ensure_seat_skills(target_config: &Path, _host_home: &Path) {
    let data_skills = data_dir_from_config(target_config)
        .map(|d| d.join("skills"))
        .filter(|p| p.is_dir())
        .or_else(|| {
            target_config
                .parent()
                .and_then(|p| p.parent())
                .map(|d| d.join("skills"))
                .filter(|p| p.is_dir())
        });
    let Some(skills_root) = data_skills else {
        return;
    };
    for rel in [
        "skills",
        ".agents/skills",
        ".claude/skills",
        ".codex/skills",
        "opencode/skills",
        ".config/opencode/skills",
        ".gemini/config/skills",
        "muse/skills",
        ".config/muse/skills",
        ".omp/agent/skills",
        ".dsh/skills",
        ".grok/skills",
    ] {
        let dst = target_config.join(rel);
        if let Ok(meta) = std::fs::symlink_metadata(&dst) {
            if meta.file_type().is_symlink()
                && dst.exists()
                && std::fs::read_link(&dst).ok().as_deref() == Some(skills_root.as_path())
            {
                continue;
            }
            if meta.file_type().is_dir() {
                let _ = std::fs::remove_dir_all(&dst);
            } else {
                let _ = std::fs::remove_file(&dst);
            }
        }
        if let Some(parent) = dst.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if std::os::unix::fs::symlink(&skills_root, &dst).is_err()
            && !crate::sandbox::is_isolated_test_env()
            && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
            && let Some(anc_meta) = dst
                .parent()
                .and_then(|p| p.ancestors().find(|a| a.exists()))
                .and_then(|a| std::fs::metadata(a).ok())
        {
            use std::os::unix::fs::MetadataExt as _;
            let uid = anc_meta.uid();
            let gid = anc_meta.gid();
            if uid != 0
                && let Some(parent) = Path::new(rel).parent()
            {
                let rel_parent = if parent.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    parent
                };
                let uid_s = uid.to_string();
                let gid_s = gid.to_string();
                let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
                    .current_dir(target_config)
                    .args([
                        "--reuid",
                        &uid_s,
                        "--regid",
                        &gid_s,
                        "--groups",
                        &gid_s,
                        "--no-new-privs",
                        "--",
                        "sh",
                        "-c",
                        "mkdir -p \"$1\" && chmod a+rx \"$1\" && rm -rf \"$2\" && ln -sfn \"$3\" \"$2\"",
                        "sh",
                    ])
                    .arg(rel_parent)
                    .arg(rel)
                    .arg(&skills_root)
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        }
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

pub(crate) fn write_seat_shared_file(dst: &Path, bytes: &[u8], mode: u32) -> bool {
    if let Some(parent) = dst.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if std::fs::write(dst, bytes).is_err() {
        chmod_seat_file_readable(dst);
        if std::fs::write(dst, bytes).is_err() {
            let _ = std::fs::remove_file(dst);
            if std::fs::write(dst, bytes).is_err() {
                return false;
            }
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(dst, std::fs::Permissions::from_mode(mode));
    }
    true
}

pub(crate) fn write_profile_marker(config_dir: &Path, marker_file: &str, content: &str) {
    let _ = write_seat_shared_file(&config_dir.join(marker_file), content.as_bytes(), 0o664);
}

pub(crate) fn seeded_profile_name(config_dir: &Path) -> Option<String> {
    std::fs::read_to_string(config_dir.join(".anyagents-seeded-profile"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub(crate) fn seed_harness_credentials(
    store: &Store,
    data: &Path,
    agent: &Agent,
    target_config: &Path,
) {
    let Ok(all_agents) = store.list_agents() else {
        return;
    };
    let static_rel_paths: &[&str] = match agent.harness.as_str() {
        "claude-code" => &[".credentials.json", ".claude/.credentials.json"],
        "codex" => &[
            "auth.json",
            ".codex/auth.json",
            "models_cache.json",
            "model-catalog-direct.json",
            ".config/desk/sylphx-ai/key",
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
        "sylphx" => &[
            ".config/sylphx/credentials.json",
            ".config/sylphx/context.json",
            ".config/desk/sylphx-ai/key",
            ".config/opencode/sylphx.key",
            ".config/opencode/opencode.json",
            "sylphx-agent.json",
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

    if let (Some(bound), Some(primary_profile_root)) =
        (current_bound.as_deref(), profile_roots.first())
    {
        for other in &all_agents {
            if other.id == agent.id {
                continue;
            }
            let other_cfg = agent_config_dir(data, &other.id);
            if bound_profile_name(&other_cfg).as_deref() == Some(bound) {
                for rel in [
                    ".credentials.json",
                    ".claude/.credentials.json",
                    "auth.json",
                    ".codex/auth.json",
                    "models_cache.json",
                    "model-catalog-direct.json",
                    ".gemini/antigravity-cli/antigravity-oauth-token",
                ] {
                    let other_src = other_cfg.join(rel);
                    if other_src.is_file() {
                        let _ =
                            copy_seat_file_if_newer(&other_src, &primary_profile_root.join(rel));
                    }
                }
            }
        }
    }

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
        let is_auth_cred = rel.ends_with(".key")
            || matches!(
                rel.as_str(),
                ".gemini/antigravity-cli/antigravity-oauth-token"
                    | ".gemini/antigravity-cli/antigravity_state.pbtxt"
                    | ".gemini/antigravity-cli/jetski_state.pbtxt"
                    | ".credentials.json"
                    | ".claude/.credentials.json"
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
                let Ok(src_bytes) = std::fs::read(&src) else {
                    continue;
                };
                if dst_non_empty {
                    if std::fs::read(&dst).is_err() {
                        chmod_seat_file_readable(&dst);
                    }
                    if let Ok(dst_bytes) = std::fs::read(&dst) {
                        if src_bytes == dst_bytes {
                            break;
                        }
                        if !profile_changed {
                            let newer_oauth = matches!(
                                (
                                    json_oauth_expires_at(&src_bytes),
                                    json_oauth_expires_at(&dst_bytes)
                                ),
                                (Some(s_exp), Some(d_exp)) if s_exp > d_exp
                            ) || (json_oauth_expires_at(&src_bytes).is_some()
                                && json_oauth_expires_at(&dst_bytes).is_none());
                            if !newer_oauth {
                                break;
                            }
                        }
                    } else if !profile_changed {
                        break;
                    }
                }
                if write_seat_shared_file(&dst, &src_bytes, 0o660) {
                    break;
                }
            }
        }
    }
    if let Some(ref bound) = current_bound {
        write_profile_marker(
            target_config,
            ".anyagents-seeded-profile",
            &format!("{bound}\n"),
        );
    }
}

/// One Upstream Provider Profile in the server's centralized profile pool
/// (`<data>/profiles/<provider>/<profile>`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProviderProfileEntry {
    pub provider: String,
    pub profile: String,
    pub kind: String,
    pub compatible_harnesses: Vec<String>,
    pub assigned_agents: Vec<String>,
    #[serde(default)]
    pub subagent_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<String>,
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
        "sylphx" => Some("sylphx-session-id"),
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
    if turns.is_empty() {
        return None;
    }
    let header_pair = match previous_harness.filter(|p| !p.is_empty() && *p != target_harness) {
        Some(prev) => format!("`{prev}` -> `{target_harness}`"),
        None => format!("`state.db` -> `{target_harness}`"),
    };
    let mut out = format!(
        "## 4. Cross-Harness Session Continuity ({header_pair})\n\n\
         - This AnyAgents identity (`{}` / `{}`) is running on harness `{target_harness}` while retaining its AnyAgents ID, role description, model (`{}`), workdir (`{}`), mailbox cursor, and external Spiron execution custody.\n\
         - **Do NOT replay or repeat already-completed `[OUT]` turns or external side effects** listed below; your mailbox cursor (`delivered_seq`) is already advanced past all prior delivered messages.\n\
         - **Do NOT reopen paused, archived, or restricted product work** merely because the harness session started; act only on the new incoming `<message>` envelope delivered in the current turn.\n\
         - Your full historical two-way transcript in `state.db` remains queryable on demand via `conversation({{ \"agent\": \"self\", \"limit\": 30 }})`.\n\n\
         ### Recent Completed Two-Way Turns (`state.db`)\n\n",
        agent.name,
        agent.id.as_str(),
        agent.model.as_deref().unwrap_or("default"),
        agent.workdir.as_deref().unwrap_or("."),
    );
    for turn in turns {
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
    let turns = store.conversation(&agent.id, None, None, Some(10)).ok()?;
    if turns.is_empty() {
        let _ = std::fs::remove_file(&bridge_cache_path);
        return None;
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
        "sylphx",
    ] {
        if candidate == agent.harness {
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
/// inherit the root ancestor's hint consistently across `serve-admin` and `serve`.
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

/// Resolve an agent's authoritative `(seat_user, uid, gid, seat_launch_spec)`
/// using the existing `seat-*` group on `<data>/agents/<id>/config` when present,
/// and otherwise the root-parent hint and `seat_launch_spec_with_hint` lookup.
pub(crate) fn resolved_agent_seat_info(
    store: &Store,
    agent: &Agent,
) -> Option<(String, u32, u32, Option<String>)> {
    let cfg = agent_config_dir(store.data_dir(), &agent.id);
    if let Some(existing) = crate::sandbox::existing_config_seat(&cfg) {
        return Some(existing);
    }
    let seat_hint = resolve_root_seat_hint(store, agent);
    if let Some((user, spec)) =
        crate::sandbox::seat_launch_spec_with_hint(&agent.name, seat_hint.as_deref())
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
        crate::sandbox::seat_for_agent_with_hint(&agent.name, seat_hint.as_deref())?;
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
    std::fs::create_dir_all(config)?;
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
    ensure_seat_environment(config);
    seed_harness_credentials(store, data, agent, config);
    if !config.join("mcp-capability").is_file()
        && crate::operator::ensure(store, &agent.id).is_err()
    {
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
                agent.name,
                crate::operator::HEADER
            );
            let _ = stream.write_all(req.as_bytes());
            let mut buf = String::new();
            let _ = stream.read_to_string(&mut buf);
        }
    }

    let continuity = compute_live_session_continuity(store, agent, config);
    write_agent_instructions_with_continuity(
        config,
        agent,
        parent_name.as_deref(),
        continuity.as_deref(),
    );

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
        match agent.harness.as_str() {
            "codex" => {
                let codex_toml = config.join("config.toml");
                mcp_config::write_codex_with_model(
                    &codex_toml,
                    agent.model.as_deref(),
                    &command,
                    &args,
                )?;
                let _ = mcp_config::sync_codex_developer_instructions(
                    &codex_toml,
                    continuity.as_deref(),
                );
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
                crate::vendor::ensure_agy_steering_settings(config, work_dir);
            }
            "muse" => {
                muse_config::write(config, agent.model.as_deref(), &command, &args)?;
                let _ = muse_config::point_vendor_home(config);
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
            "sylphx" => {
                crate::sylphx::write_sylphx_config(
                    config,
                    agent.model.as_deref(),
                    &command,
                    &args,
                )?;
            }
            _ => {
                let _ = mcp_config::sanitize_claude_settings(config);
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
        if work_dir != Path::new("/scratch/wt/owners-main")
            && work_dir != Path::new("/scratch/wt")
            && (agent.parent_id.is_some()
                || work_dir.starts_with("/scratch/tmp")
                || work_dir.starts_with(data))
        {
            crate::sandbox::share_worktree_with_seat(work_dir, gid);
        }
        if agent.harness == "muse"
            && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
            && !crate::sandbox::is_isolated_test_env()
        {
            muse_config::ensure_workspace_session(
                data,
                agent.id.as_str(),
                config,
                work_dir,
                agent.model.as_deref(),
                spec_opt.as_deref(),
            );
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
                Error::Message(format!(
                    "codex runtime config missing: {}",
                    path.display()
                ))
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
                    Error::Message(format!("opencode runtime config missing: {}", path.display()))
                })?;
                let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::Message(format!("opencode runtime config invalid JSON: {}", path.display()))
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
            for rel in ["settings.json", "muse/settings.json", ".config/muse/settings.json"] {
                let path = config.join(rel);
                let bytes = std::fs::read(&path).map_err(|_| {
                    Error::Message(format!("muse runtime config missing: {}", path.display()))
                })?;
                let val: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    Error::Message(format!("muse runtime config invalid JSON: {}", path.display()))
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
                Error::Message(format!("agy runtime config invalid JSON: {}", path.display()))
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
                return Err(Error::Message(
                    "dsh-mcp-patch.yml missing anyagents".into(),
                ));
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
        "sylphx" => {
            let path = config.join("sylphx-agent.json");
            let text = std::fs::read_to_string(&path).map_err(|_| {
                Error::Message(format!("sylphx runtime config missing: {}", path.display()))
            })?;
            if !text.contains("\"anyagents\"") {
                return Err(Error::Message(
                    "sylphx-agent.json missing anyagents MCP server".into(),
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
    if trimmed.is_empty() {
        return;
    }
    let rotated_ms = std::fs::read_to_string(config.join(".anyagents-profile-rotated-ms"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let marker_path = config.join(".anyagents-notified-parent-error.json");
    let source_key = format!("subagent-error:{}:{rotated_ms}:{trimmed}", agent.id.as_str());
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
    let target_harness = match harness.map(str::trim) {
        Some("") => return Err(Error::Message("harness is empty".into())),
        Some("sylphx-agent" | "sylphx_agent") => "sylphx".to_string(),
        Some(h) => h.to_string(),
        None => existing.harness.clone(),
    };
    let _adapter = crate::install::adapter(&target_harness)?;
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none() && !crate::sandbox::is_isolated_test_env()
    {
        crate::install::record(data, &target_harness).map_err(Error::Message)?;
    }
    let resolved_model = model
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| existing.model.clone());
    let config = agent_config_dir(data, &existing.id);
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
                    .filter_map(|x| x.as_str().map(str::to_string))
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

    std::fs::create_dir_all(&config)?;
    let normalized_profile = format!("{provider}/{profile_name}");
    if profile.is_some() && profile_dir.is_dir() {
        fn copy_files(source: &Path, target: &Path) -> Result<(), Error> {
            std::fs::create_dir_all(target)?;
            for entry in std::fs::read_dir(source)? {
                let entry = entry?;
                let kind = entry.file_type()?;
                let target = target.join(entry.file_name());
                if kind.is_dir() {
                    copy_files(&entry.path(), &target)?;
                } else if kind.is_file()
                    && entry.file_name() != "profile.json"
                    && let Ok(bytes) = std::fs::read(entry.path())
                {
                    let _ = write_seat_shared_file(&target, &bytes, 0o660);
                }
            }
            Ok(())
        }
        let _ = copy_files(&profile_dir, &config);
        write_profile_marker(
            &config,
            ".anyagents-profile-rotated-ms",
            &format!("{}\n", anyagents_core::now_ms()),
        );
    }
    write_profile_marker(
        &config,
        ".anyagents-profile-rotated-ms",
        &format!("{}\n", anyagents_core::now_ms()),
    );
    write_profile_marker(
        &config,
        ".anyagents-profile",
        &format!("{normalized_profile}\n"),
    );
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
        Error::Message("expected <provider>/<profile> (e.g. kimi-code-plan/rhea)".into())
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
        );
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
            propagate_profile_files_to_bound_agents(data, provider, profile, &[rel.as_str()]);
            return Ok((provider.to_string(), profile.to_string()));
        }
    };
    let mut rels = Vec::new();
    for fname in key_filenames {
        write_profile_key_file(&dir, fname, trimmed)?;
        rels.push(format!(".config/opencode/{fname}"));
    }
    let rel_refs: Vec<&str> = rels.iter().map(String::as_str).collect();
    propagate_profile_files_to_bound_agents(data, provider, profile, &rel_refs);
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
) {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    let target_tag = format!("{provider}/{profile}");
    let store_opt = Store::open(data.join("state.db")).ok();
    let all_agents = store_opt
        .as_ref()
        .and_then(|s| s.list_agents().ok())
        .unwrap_or_default();
    let Ok(agents_dir) = std::fs::read_dir(data.join("agents")) else {
        return;
    };
    for entry in agents_dir.flatten() {
        let cfg = entry.path().join("config");
        if !cfg.is_dir() {
            continue;
        }
        let dir_id = entry.file_name().to_string_lossy().into_owned();
        let agent_rec = all_agents.iter().find(|a| a.id.as_str() == dir_id);
        let bound = bound_profile_name(&cfg);
        let matches_target = match bound.as_deref() {
            Some(b) if b == target_tag => true,
            Some(b) if b == profile => agent_rec
                .is_some_and(|a| upstream_provider_of(&a.harness, a.model.as_deref()) == provider),
            None if profile == "default" => agent_rec
                .is_some_and(|a| upstream_provider_of(&a.harness, a.model.as_deref()) == provider),
            _ => false,
        };
        if !matches_target {
            continue;
        }
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
            if let Ok(bytes) = std::fs::read(&src) {
                let _ = write_seat_shared_file(&cfg.join(rel), &bytes, 0o660);
            }
        }
        write_profile_marker(
            &cfg,
            ".anyagents-seeded-profile",
            &format!("{target_tag}\n"),
        );
        if let (Some(s), Some(a)) = (store_opt.as_ref(), agent_rec)
            && let Some((_, uid, gid, _)) = resolved_agent_seat_info(s, a)
        {
            crate::sandbox::share_config_with_seat_uid(&cfg, uid, gid);
        }
    }
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
    match harness {
        "claude-code" => "anthropic".to_string(),
        "codex" => "openai".to_string(),
        "agy" => "google-antigravity".to_string(),
        "muse" => "muse".to_string(),
        "dsh" => "deepseek".to_string(),
        "grok" => "xai".to_string(),
        "sylphx" | "opencode" | "omp" => "sylphx".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn default_kind_and_harnesses(provider: &str) -> (&'static str, Vec<String>) {
    let harnesses = |list: &[&str]| list.iter().map(|s| (*s).to_string()).collect();
    match provider {
        "sylphx" => (
            "api-key",
            harnesses(&[
                "sylphx",
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
            harnesses(&["opencode", "omp", "claude-code", "codex", "dsh"]),
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

fn json_oauth_identity(bytes: &[u8]) -> Option<String> {
    let val: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    for ptr in [
        "/id_token_email",
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
    let Ok(src_bytes) = std::fs::read(src) else {
        return false;
    };
    if src_bytes.is_empty() {
        return false;
    }
    if src.file_name().is_some_and(|n| n == ".credentials.json")
        && json_oauth_expires_at(&src_bytes).is_none()
    {
        return false;
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
    sync_and_list_provider_profiles_with_options(store, data, true)
}

pub(crate) fn sync_and_list_provider_profiles_with_options(
    store: &Store,
    data: &Path,
    probe_usage: bool,
) -> Vec<ProviderProfileEntry> {
    let agents = store.list_agents().unwrap_or_default();
    let profiles_root = data.join("profiles");
    let _ = std::fs::create_dir_all(&profiles_root);

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
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect();
                        if !parsed.is_empty() {
                            compatible_harnesses = parsed;
                        }
                    }
                }
                if provider == "sylphx" && !compatible_harnesses.iter().any(|h| h == "sylphx") {
                    compatible_harnesses.insert(0, "sylphx".to_string());
                }
                entries.push(ProviderProfileEntry {
                    provider: provider.clone(),
                    profile,
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

    // Bind active agents to their resolved `<provider>/<profile>`, restore affinity
    // (`<provider>/<agent.name>`) when a dedicated profile recovers quota (`>= 10%`),
    // auto-failover any agent bound to an exhausted (`0% left`) profile when another
    // profile under the same upstream provider has healthy quota (`>= 20% left`), and
    // auto-clear any stale session quota hold when the bound profile now has `> 0%` left.
    let active_names: std::collections::BTreeSet<String> = agents
        .iter()
        .filter(|a| a.state == "running")
        .map(|a| a.name.clone())
        .collect();
    let mut ordered_agents: Vec<&Agent> =
        agents.iter().filter(|a| a.state != "archived").collect();
    ordered_agents.sort_by_key(|a| a.parent_id.is_some());
    for agent in ordered_agents {
        let is_root = agent.parent_id.is_none();
        let cfg = agent_config_dir(data, &agent.id);
        let expected_provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
        let bound = bound_profile_name(&cfg);
        let (bound_prov, mut bound_prof) = match bound.as_deref() {
            Some(s) if s.contains('/') => {
                let (p, n) = s.split_once('/').unwrap_or((&expected_provider, "default"));
                (p.to_string(), n.to_string())
            }
            Some(s) if !s.is_empty() => (expected_provider.clone(), s.to_string()),
            _ => (expected_provider.clone(), "default".to_string()),
        };
        let has_dedicated = entries
            .iter()
            .any(|e| e.provider == expected_provider && e.profile == agent.name);
        if is_root && bound_prov == expected_provider && bound_prof != agent.name && has_dedicated {
            let affinity_pct = entries
                .iter()
                .find(|e| e.provider == expected_provider && e.profile == agent.name)
                .and_then(|e| e.usage.as_deref())
                .and_then(|u| parse_remaining_pct_from_usage(u, agent.model.as_deref()));
            let current_occupied = entries
                .iter()
                .find(|e| e.provider == bound_prov && e.profile == bound_prof)
                .is_some_and(|e| !e.assigned_agents.is_empty());
            if affinity_pct.is_some_and(|p| p >= 10) || current_occupied {
                rotate_agent_to_profile(store, data, agent, &expected_provider, &agent.name);
                bound_prof = agent.name.clone();
            }
        }
        let cur_pct = entries
            .iter()
            .find(|e| e.provider == bound_prov && e.profile == bound_prof)
            .and_then(|e| e.usage.as_deref())
            .and_then(|u| parse_remaining_pct_from_usage(u, agent.model.as_deref()));
        if agent.state == "running" && cur_pct == Some(0) {
            if let Some((best_alt, _)) = entries
                .iter()
                .filter(|e| {
                    e.provider == bound_prov
                        && e.profile != bound_prof
                        && e.assigned_agents.is_empty()
                        && !active_names.contains(&e.profile)
                })
                .filter_map(|e| {
                    let pct = parse_remaining_pct_from_usage(
                        e.usage.as_deref()?,
                        agent.model.as_deref(),
                    )?;
                    (pct >= 20).then_some((e.profile.clone(), pct))
                })
                .max_by_key(|(_, pct)| *pct)
            {
                rotate_agent_to_profile(store, data, agent, &bound_prov, &best_alt);
                bound_prof = best_alt;
            }
        }
        let normalized = format!("{bound_prov}/{bound_prof}");
        if bound.as_deref() != Some(normalized.as_str()) {
            write_profile_marker(&cfg, ".anyagents-profile", &format!("{normalized}\n"));
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
        entry.assigned_agents.sort_by(|a, b| {
            a.starts_with("owner-")
                .cmp(&b.starts_with("owner-"))
                .then_with(|| a.cmp(b))
        });
    }
    entries
}

pub(crate) fn sync_fresher_bound_agent_credentials(data: &Path) {
    let profiles_root = data.join("profiles");
    if let Ok(store) = Store::open(data.join("state.db"))
        && let Ok(agents) = store.list_agents()
    {
        for agent in &agents {
            let cfg = agent_config_dir(data, &agent.id);
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
                propagate_profile_files_to_bound_agents(data, &provider, &prof_name, rels);
            }
        }
    }
    let anth_root = profiles_root.join("anthropic");
    if let Ok(entries) = std::fs::read_dir(&anth_root) {
        let now_ms = anyagents_core::now_ms();
        let mut profs: Vec<(String, PathBuf, i64, Option<String>, Vec<u8>)> = Vec::new();
        for entry in entries.flatten() {
            let p = entry.path();
            if !p.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let bytes = std::fs::read(p.join(".credentials.json"))
                .or_else(|_| std::fs::read(p.join(".claude/.credentials.json")))
                .unwrap_or_default();
            let exp = json_oauth_expires_at(&bytes).unwrap_or(0);
            let acct = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|v| {
                    v.pointer("/claudeAiOauth/accountUuid")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                });
            profs.push((name, p, exp, acct, bytes));
        }
        if let Some((_, _, best_exp, best_acct, best_bytes)) = profs
            .iter()
            .filter(|(_, _, exp, _, bytes)| *exp > now_ms && !bytes.is_empty())
            .max_by_key(|(_, _, exp, _, _)| *exp)
            .cloned()
        {
            for (name, dir, exp, acct, _) in &profs {
                let same_acct = match (&best_acct, acct) {
                    (Some(a), Some(b)) => a == b,
                    _ => true,
                };
                if *exp < best_exp && (*exp <= now_ms || same_acct) {
                    let _ = std::fs::create_dir_all(dir.join(".claude"));
                    let _ = std::fs::write(dir.join(".credentials.json"), &best_bytes);
                    let _ = std::fs::write(dir.join(".claude/.credentials.json"), &best_bytes);
                    propagate_profile_files_to_bound_agents(
                        data,
                        "anthropic",
                        name,
                        &[".credentials.json", ".claude/.credentials.json"],
                    );
                }
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

fn extract_agy_oauth_client() -> Option<(String, String)> {
    let agy_bin = ["/home/kyle/.local/bin/agy", "/usr/local/bin/agy"]
        .into_iter()
        .map(Path::new)
        .find(|p| p.is_file())?;
    let bytes = std::fs::read(agy_bin).ok()?;
    let cid_needle = b"1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com";
    if !bytes.windows(cid_needle.len()).any(|w| w == cid_needle) {
        return None;
    }
    let sec_prefix = b"GOCSPX-";
    for idx in 0..bytes.len().saturating_sub(35) {
        if &bytes[idx..idx + 7] == sec_prefix {
            let candidate = &bytes[idx..idx + 35];
            if candidate[7..]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_')
            {
                let secret = String::from_utf8(candidate.to_vec()).ok()?;
                return Some((String::from_utf8(cid_needle.to_vec()).ok()?, secret));
            }
        }
    }
    None
}

fn extract_cursor_oauth_from_db(prof_dir: &Path) -> Option<(String, String)> {
    let db_path = ["agent.db", ".omp/agent/agent.db"]
        .into_iter()
        .map(|r| prof_dir.join(r))
        .find(|p| p.is_file())?;
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
        if let Some(oauth) = cred
            .get_mut("claudeAiOauth")
            .and_then(serde_json::Value::as_object_mut)
        {
            if let Some(au) = acct_uuid
                && oauth
                    .get("accountUuid")
                    .and_then(serde_json::Value::as_str)
                    != Some(au)
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
    if changed || !prof_dir.join(".claude/.credentials.json").is_file() {
        if let Ok(updated) = serde_json::to_string(cred) {
            let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
            let _ = std::fs::write(prof_dir.join(".credentials.json"), &updated);
            let _ = std::fs::write(prof_dir.join(".claude/.credentials.json"), &updated);
            propagate_profile_files_to_bound_agents(
                data,
                "anthropic",
                profile,
                &[".credentials.json", ".claude/.credentials.json"],
            );
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
    let m_adj = if m > 2 { (m - 3) as u64 } else { (m + 9) as u64 };
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

pub(crate) fn probe_one_profile_usage(
    data: &Path,
    provider: &str,
    profile: &str,
) -> Option<String> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(4)))
        .build()
        .new_agent();

    match provider {
        "anthropic" => {
            let cred_text = std::fs::read_to_string(prof_dir.join(".credentials.json"))
                .or_else(|_| std::fs::read_to_string(prof_dir.join(".claude/.credentials.json")))
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
            let fetch_usage = |tok: &str| -> Option<serde_json::Value> {
                let resp = http
                    .get("https://api.anthropic.com/api/oauth/usage")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("anthropic-beta", "oauth-2025-04-20")
                    .header("cache-control", "no-cache")
                    .header("user-agent", "claude-code/2.1.310")
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                serde_json::from_str(&body).ok()
            };
            let now_ms = anyagents_core::now_ms();
            let expires_at = cred
                .pointer("/claudeAiOauth/expiresAt")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(i64::MAX);
            let expiring_soon = now_ms.saturating_add(30 * 60 * 1000) >= expires_at;
            let initial_usage = if expiring_soon {
                None
            } else {
                fetch_usage(&token)
            };
            let val = match initial_usage {
                Some(v) => v,
                None => {
                    let attempt_path = prof_dir.join(".anyagents-oauth-refresh-attempt-ms");
                    let last_attempt_ms = std::fs::read_to_string(&attempt_path)
                        .ok()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        .unwrap_or(0);
                    if now_ms.saturating_sub(last_attempt_ms) < 300_000 {
                        fetch_usage(&token)?
                    } else {
                    let rtok = cred
                        .pointer("/claudeAiOauth/refreshToken")
                        .and_then(serde_json::Value::as_str)?
                        .trim()
                        .to_string();
                    if rtok.is_empty() {
                        return None;
                    }
                    let _ = std::fs::write(&attempt_path, format!("{now_ms}\n"));
                    let req_body = serde_json::json!({
                        "grant_type": "refresh_token",
                        "refresh_token": rtok,
                        "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
                    })
                    .to_string();
                    let rresp = http
                        .post("https://platform.claude.com/v1/oauth/token")
                        .header("content-type", "application/json")
                        .header("user-agent", "claude-code/2.1.310")
                        .send(req_body.as_bytes())
                        .ok()?;
                    let rbody = rresp.into_body().read_to_string().ok()?;
                    let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                    let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
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
                            let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
                            let _ = std::fs::write(prof_dir.join(".credentials.json"), &updated);
                            let _ = std::fs::write(
                                prof_dir.join(".claude/.credentials.json"),
                                &updated,
                            );
                            propagate_profile_files_to_bound_agents(
                                data,
                                provider,
                                profile,
                                &[".credentials.json", ".claude/.credentials.json"],
                            );
                        }
                    }
                        token = new_acc;
                        fetch_usage(&token)?
                    }
                }
            };
            let tier_badge = sync_anthropic_profile_metadata(
                &http, data, &prof_dir, profile, &token, &mut cred, false,
            );
            let tier_suffix = tier_badge.map(|t| format!(" ({t})")).unwrap_or_default();
            let u5 = val
                .pointer("/five_hour/utilization")
                .and_then(serde_json::Value::as_f64)?;
            let u7 = val
                .pointer("/seven_day/utilization")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem_5h = (100.0 - u5).clamp(0.0, 100.0).round() as i64;
            let rem_7d = (100.0 - u7).clamp(0.0, 100.0).round() as i64;
            let eta_5h = (rem_5h < 100)
                .then(|| {
                    val.pointer("/five_hour/resets_at")
                        .and_then(serde_json::Value::as_str)
                        .and_then(format_reset_eta_iso)
                })
                .flatten()
                .map(|e| format!(" ↻{e}"))
                .unwrap_or_default();
            let eta_7d = (rem_7d < 100)
                .then(|| {
                    val.pointer("/seven_day/resets_at")
                        .and_then(serde_json::Value::as_str)
                        .and_then(format_reset_eta_iso)
                })
                .flatten()
                .map(|e| format!(" ↻{e}"))
                .unwrap_or_default();
            Some(format!(
                "5h: {rem_5h}% left{eta_5h} · 7d: {rem_7d}% left{eta_7d}{tier_suffix}"
            ))
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
                            let _ = std::fs::create_dir_all(prof_dir.join(".codex"));
                            let _ = std::fs::write(prof_dir.join("auth.json"), &updated);
                            let _ = std::fs::write(prof_dir.join(".codex/auth.json"), &updated);
                            propagate_profile_files_to_bound_agents(
                                data,
                                provider,
                                profile,
                                &["auth.json", ".codex/auth.json"],
                            );
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
                Some(plan) if !plan.is_empty() => Some(format!("7d: {rem}% left{eta} ({plan})")),
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
                let rtok = tok_val
                    .pointer("/token/refresh_token")
                    .or_else(|| tok_val.get("refresh_token"))
                    .and_then(serde_json::Value::as_str)?
                    .trim()
                    .to_string();
                if rtok.is_empty() {
                    return None;
                }
                let (cid, csec) = extract_agy_oauth_client()?;
                let form_body = format!(
                    "client_id={cid}&client_secret={csec}&refresh_token={rtok}&grant_type=refresh_token"
                );
                let rresp = http
                    .post("https://oauth2.googleapis.com/token")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .send(form_body.as_bytes())
                    .ok()?;
                let rbody = rresp.into_body().read_to_string().ok()?;
                let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
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
                    propagate_profile_files_to_bound_agents(data, provider, profile, &[tok_rel]);
                }
                active_token = new_acc;
                summary_val = fetch_summary(&active_token);
            }
            if let Some(ref sv) = summary_val
                && let Some(groups) = sv.get("groups").and_then(serde_json::Value::as_array)
            {
                let mut gem_5h: Option<(i64, Option<String>)> = None;
                let mut gem_7d: Option<(i64, Option<String>)> = None;
                let mut opus_5h: Option<(i64, Option<String>)> = None;
                let mut opus_7d: Option<(i64, Option<String>)> = None;
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
                        let frac = b
                            .get("remainingFraction")
                            .and_then(serde_json::Value::as_f64)
                            .unwrap_or(0.0);
                        let pct = (frac * 100.0).clamp(0.0, 100.0).round() as i64;
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
                            "3p-5h" => opus_5h = Some((pct, eta)),
                            "3p-weekly" => opus_7d = Some((pct, eta)),
                            _ => {}
                        }
                    }
                }
                if let (
                    Some((g5, g5_eta)),
                    Some((g7, g7_eta)),
                    Some((o5, o5_eta)),
                    Some((o7, o7_eta)),
                ) = (gem_5h, gem_7d, opus_5h, opus_7d)
                {
                    let fmt_pool =
                        |label: &str, p5: i64, e5: Option<String>, p7: i64, e7: Option<String>| {
                            let p = p5.min(p7);
                            let mut parts = Vec::new();
                            if p5 < 100 {
                                let es = e5.map(|e| format!(" ↻{e}")).unwrap_or_default();
                                if p5 == p && p5 < p7 {
                                    parts.push(format!("5h{es}"));
                                } else {
                                    parts.push(format!("5h {p5}%{es}"));
                                }
                            }
                            if p7 < 100 {
                                let es = e7.map(|e| format!(" ↻{e}")).unwrap_or_default();
                                if p7 == p {
                                    parts.push(format!("7d{es}"));
                                } else {
                                    parts.push(format!("7d {p7}%{es}"));
                                }
                            }
                            if parts.is_empty() {
                                format!("{label}: {p}% left")
                            } else {
                                format!("{label}: {p}% left ({})", parts.join(", "))
                            }
                        };
                    let gem_s = fmt_pool("gemini", g5, g5_eta, g7, g7_eta);
                    let opus_s = fmt_pool("opus", o5, o5_eta, o7, o7_eta);
                    return Some(format!("{gem_s} · {opus_s}"));
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
                            .and_then(serde_json::Value::as_f64)
                            .unwrap_or(0.0);
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
            let opus = frac_of(&[
                "claude-opus-5-5-thinking",
                "claude-opus-5-5-high",
                "claude-opus-5-5-medium",
                "claude-opus-4-6-thinking",
            ])?;
            Some(format!("gemini: {gem}% left · opus: {opus}% left"))
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
                let r5 = parse_num("/limits/0/detail/remaining").unwrap_or(0.0);
                (r5 / l5 * 100.0).clamp(0.0, 100.0).round() as i64
            };
            let pct_mo = if let Some(ratio) = parse_num("/usages/limit_month_total/used_ratio") {
                ((1.0 - ratio) * 100.0).clamp(0.0, 100.0).round() as i64
            } else {
                let lm = parse_num("/usage/limit")?.max(1.0);
                let rm = parse_num("/usage/remaining").unwrap_or(0.0);
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
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
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
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
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
                if item.get("type").and_then(serde_json::Value::as_str) != Some("TOKENS_LIMIT") {
                    continue;
                }
                let unit = item.get("unit").and_then(serde_json::Value::as_i64);
                let pct = item
                    .get("percentage")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                let rem = (100 - pct).clamp(0, 100);
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
            let r7 = rem_7d.unwrap_or(100);
            let e5 = eta_5h.map(|e| format!(" ↻{e}")).unwrap_or_default();
            let e7 = eta_7d.map(|e| format!(" ↻{e}")).unwrap_or_default();
            Some(format!("5h: {r5}% left{e5} · 7d: {r7}% left{e7}"))
        }
        "deepseek" => {
            let api_dir = data.join("profiles/deepseek/api");
            let key = read_first_non_empty_file(&prof_dir, &[".config/opencode/deepseek.key"])
                .or_else(|| read_dsh_yaml_ref(&prof_dir, "DEEPSEEK_API_KEY"))
                .or_else(|| read_first_non_empty_file(&api_dir, &[".config/opencode/deepseek.key"]))
                .or_else(|| read_dsh_yaml_ref(&api_dir, "DEEPSEEK_API_KEY"))?;
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
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
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
            let key = read_first_non_empty_file(&prof_dir, &[".config/opencode/openrouter.key"])
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
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
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
            let limit_cents = plan
                .get("limit")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem_cents = plan
                .get("remaining")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let used_pct = plan
                .get("totalPercentUsed")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem_pct = (100.0 - used_pct).clamp(0.0, 100.0).round() as i64;
            let tier = val
                .get("membershipType")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("pro");
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
            let used_pct = val
                .pointer("/subs_usage/window/used_percent")
                .or_else(|| val.pointer("/subs_usage/used_percent"))
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            let rem_pct = (100.0 - used_pct).clamp(0.0, 100.0).round() as i64;
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
                    let form_body =
                        format!("grant_type=refresh_token&refresh_token={rtok}&client_id={cid}");
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
                            let _ = std::fs::write(&auth_path, &updated);
                            propagate_profile_files_to_bound_agents(
                                data,
                                provider,
                                profile,
                                &[auth_rel],
                            );
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
            let _key = crate::sylphx::read_sylphx_key_from_dir(&prof_dir)?;
            let resp = http
                .get("https://api.sylphx.com/health")
                .call()
                .ok()?;
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
}

pub(crate) fn parse_remaining_pct_from_usage(usage: &str, model: Option<&str>) -> Option<i64> {
    let extract_after = |label: &str| -> Option<i64> {
        let idx = usage.find(label)?;
        let rest = &usage[idx + label.len()..];
        let pct_idx = rest.find("% left")?;
        rest[..pct_idx].trim().parse::<i64>().ok()
    };
    if usage.contains("gemini:") && usage.contains("opus:") {
        let wants_opus = model
            .map(|m| {
                let lower = m.to_ascii_lowercase();
                lower.contains("opus") || lower.contains("claude")
            })
            .unwrap_or(false);
        if wants_opus {
            return extract_after("opus:");
        }
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
    parse_remaining_pct_from_usage(&usage, model)
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

pub(crate) fn force_refresh_provider_profiles(data: &Path, target: Option<&str>) {
    let filter = target.map(str::trim).filter(|s| {
        !s.is_empty() && !s.eq_ignore_ascii_case("refresh") && !s.eq_ignore_ascii_case("reset")
    });
    sync_fresher_bound_agent_credentials(data);
    let now_ms = anyagents_core::now_ms();
    if let Ok(agents_dir) = std::fs::read_dir(data.join("agents")) {
        for entry in agents_dir.flatten() {
            let cfg = entry.path().join("config");
            let _ = std::fs::remove_file(cfg.join(".anyagents-last-quota-check-ms"));
        }
    }
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(6)))
        .build()
        .new_agent();
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
            let _ = std::fs::remove_file(prof_dir.join(".anyagents-oauth-refresh-attempt-ms"));
            let _ = std::fs::remove_file(prof_dir.join(".anyagents-profile-meta-sync-ms"));
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
                    let mut refreshed_token = false;
                    if !tok_valid && !rtok.is_empty() {
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
                        if let Ok(rresp) = http
                            .post("https://platform.claude.com/v1/oauth/token")
                            .header("content-type", "application/json")
                            .header("user-agent", "claude-code/2.1.310")
                            .send(req_body.as_bytes())
                            && let Ok(rbody) = rresp.into_body().read_to_string()
                            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
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
                        if refreshed_token
                            && let Ok(updated) = serde_json::to_string(&cred)
                        {
                            let _ = std::fs::create_dir_all(prof_dir.join(".claude"));
                            let _ = std::fs::write(prof_dir.join(".credentials.json"), &updated);
                            let _ = std::fs::write(
                                prof_dir.join(".claude/.credentials.json"),
                                &updated,
                            );
                            propagate_profile_files_to_bound_agents(
                                data,
                                "anthropic",
                                &profile,
                                &[".credentials.json", ".claude/.credentials.json"],
                            );
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
                                let _ = std::fs::create_dir_all(prof_dir.join(".codex"));
                                let _ = std::fs::write(prof_dir.join("auth.json"), &updated);
                                let _ = std::fs::write(prof_dir.join(".codex/auth.json"), &updated);
                                propagate_profile_files_to_bound_agents(
                                    data,
                                    "openai",
                                    &profile,
                                    &["auth.json", ".codex/auth.json"],
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
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
        if conv_meta.uid() != my_uid {
            let sock_gid = config_dir
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| std::fs::metadata(p).ok())
                .map(|m| m.gid())
                .unwrap_or(my_gid);
            let uid_s = conv_meta.uid().to_string();
            let gid_s = conv_meta.gid().to_string();
            let groups_s = format!("{},{},{}", conv_meta.gid(), sock_gid, my_gid);
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

fn restart_agent_harness_process(store: &Store, agent: &Agent) {
    #[cfg(unix)]
    {
        if agent.state == "running"
            && !crate::sandbox::is_isolated_test_env()
            && Path::new("/usr/local/bin/anyagents-setpriv").is_file()
            && let Some((_, uid, gid, _)) = resolved_agent_seat_info(store, agent)
        {
            let uid_s = uid.to_string();
            let gid_s = gid.to_string();
            let proc_pat = match agent.harness.as_str() {
                "claude-code" => "claude",
                "agy" => "agy",
                "codex" => "codex",
                _ => "",
            };
            if !proc_pat.is_empty() {
                let _ = std::process::Command::new("/usr/local/bin/anyagents-setpriv")
                    .args([
                        "--reuid",
                        &uid_s,
                        "--regid",
                        &gid_s,
                        "--groups",
                        &gid_s,
                        "--no-new-privs",
                        "--",
                        "/usr/bin/pkill",
                        "-TERM",
                        "-u",
                        &uid_s,
                        "-x",
                        proc_pat,
                    ])
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (store, agent);
    }
}

pub(crate) fn rotate_agent_to_profile(
    store: &Store,
    data: &Path,
    agent: &Agent,
    provider: &str,
    new_profile: &str,
) {
    let cfg = agent_config_dir(data, &agent.id);
    let normalized = format!("{provider}/{new_profile}\n");
    write_profile_marker(&cfg, ".anyagents-profile", &normalized);
    write_profile_marker(
        &cfg,
        ".anyagents-profile-rotated-ms",
        &format!("{}\n", anyagents_core::now_ms()),
    );
    let _ = std::fs::remove_file(cfg.join(".anyagents-last-error.json"));
    seed_harness_credentials(store, data, agent, &cfg);
    if let Some((_, uid, gid, _)) = resolved_agent_seat_info(store, agent) {
        crate::sandbox::share_config_with_seat_uid(&cfg, uid, gid);
    }
    if !store.has_open_turn(&agent.id).unwrap_or(false) {
        restart_agent_harness_process(store, agent);
    }
    if agent.harness == "agy" {
        clear_stale_agy_quota_error_steps(&cfg);
    }
    poke_agent_wake_socket(data, &agent.id);
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
    let agy_bin = if Path::new("/home/kyle/.local/bin/agy").is_file() {
        "/home/kyle/.local/bin/agy"
    } else {
        "agy"
    };
    if let Ok(out) = std::process::Command::new(agy_bin).arg("models").output()
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
    if has_active_quota_hold {
        let last_check = std::fs::read_to_string(cfg.join(".anyagents-last-quota-check-ms"))
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .unwrap_or(0);
        if now_ms.saturating_sub(last_check) < 120_000 {
            let rotated_ms = std::fs::read_to_string(cfg.join(".anyagents-profile-rotated-ms"))
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .unwrap_or(0);
            let cleared_ms = std::fs::read_to_string(cfg.join(".anyagents-quota-cleared-ms"))
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .unwrap_or(0)
                .max(rotated_ms);
            if cleared_ms >= last_check && now_ms.saturating_sub(cleared_ms) < 120_000 {
                return Some(format!("{provider}/{current_prof}"));
            }
            return None;
        }
        write_profile_marker(
            &cfg,
            ".anyagents-last-quota-check-ms",
            &format!("{now_ms}\n"),
        );
    }
    let cur_pct =
        probe_profile_remaining_pct(data, &provider, &current_prof, agent.model.as_deref());
    if has_active_quota_hold && cur_pct.is_some_and(|p| p > 5) {
        write_profile_marker(
            &cfg,
            ".anyagents-quota-cleared-ms",
            &format!("{now_ms}\n"),
        );
        if agent.harness == "agy" {
            clear_stale_agy_quota_error_steps(&cfg);
        }
        poke_agent_wake_socket(data, &agent.id);
        return Some(format!("{provider}/{current_prof}"));
    }
    let available = list_harness_profiles(data, &provider);
    if available.len() <= 1 {
        return None;
    }
    let is_exhausted =
        cur_pct == Some(0) || (has_active_quota_hold && cur_pct.is_none_or(|p| p <= 5));
    if !is_exhausted {
        return None;
    }
    let all_agents = store.list_agents().unwrap_or_default();
    let mut counts: std::collections::BTreeMap<String, usize> = available
        .iter()
        .map(|(name, _)| (name.clone(), 0usize))
        .collect();
    for other in &all_agents {
        if other.id != agent.id
            && other.state == "running"
            && upstream_provider_of(&other.harness, other.model.as_deref()) == provider
        {
            if let Some(c) = counts.get_mut(&other.name) {
                *c += 1;
            }
            let other_cfg = agent_config_dir(data, &other.id);
            if let Some(p) = bound_profile_name(&other_cfg) {
                let prof_name = p.split_once('/').map(|(_, n)| n).unwrap_or(p.as_str());
                if let Some(c) = counts.get_mut(prof_name) {
                    *c += 1;
                }
            }
        }
    }
    let best_alt = available
        .iter()
        .filter(|(name, _)| name != &current_prof && counts.get(name).copied().unwrap_or(0) == 0)
        .filter_map(|(name, _)| {
            let pct = probe_profile_remaining_pct(data, &provider, name, agent.model.as_deref())?;
            (pct >= 20).then_some((name.clone(), pct))
        })
        .max_by_key(|(_, pct)| *pct)
        .map(|(name, _)| name)?;
    rotate_agent_to_profile(store, data, agent, &provider, &best_alt);
    Some(format!("{provider}/{best_alt}"))
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
/// from `<data>/profiles/<provider>/<profile>` using quota-aware least-loaded
/// assignment across agents sharing that upstream provider, and persists
/// `.anyagents-profile` (`<provider>/<profile>`) in `target_config`.
fn select_or_bind_profile(
    data: &Path,
    all_agents: &[Agent],
    agent: &Agent,
    target_config: &Path,
) -> Vec<PathBuf> {
    let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
    let available = list_harness_profiles(data, &provider);
    let mut roots = Vec::new();
    if let Some(existing) = bound_profile_name(target_config) {
        let prof_name = existing
            .split_once('/')
            .map(|(_, p)| p)
            .unwrap_or(existing.as_str());
        if let Some((_, dir)) = available.iter().find(|(n, _)| n == prof_name) {
            roots.push(dir.clone());
        } else {
            roots.push(data.join("profiles").join(&provider).join(prof_name));
        }
        if prof_name != "default" {
            return roots;
        }
    } else if !available.is_empty() {
        let mut counts: std::collections::BTreeMap<String, usize> = available
            .iter()
            .map(|(name, _)| (name.clone(), 0usize))
            .collect();
        for other in all_agents {
            if other.id != agent.id
                && other.state != "archived"
                && upstream_provider_of(&other.harness, other.model.as_deref()) == provider
            {
                let other_cfg = agent_config_dir(data, &other.id);
                if let Some(p) = bound_profile_name(&other_cfg) {
                    let prof_name = p.split_once('/').map(|(_, n)| n).unwrap_or(p.as_str());
                    if let Some(c) = counts.get_mut(prof_name) {
                        *c += 1;
                    }
                }
            }
        }
        let probe_enabled = !crate::sandbox::is_isolated_test_env() && available.len() > 1;
        let scored: Vec<(&(String, PathBuf), bool, bool, usize, i64)> = available
            .iter()
            .map(|item| {
                let pct = if probe_enabled {
                    probe_profile_remaining_pct(data, &provider, &item.0, agent.model.as_deref())
                        .unwrap_or(100)
                } else {
                    100
                };
                let assigned = counts.get(&item.0).copied().unwrap_or(0);
                (item, pct > 0, pct >= 20, assigned, pct)
            })
            .collect();
        let best_tier = scored
            .iter()
            .map(|(_, has_quota, healthy, assigned, _)| {
                (*has_quota, *healthy, std::cmp::Reverse(*assigned))
            })
            .max()
            .unwrap_or((true, true, std::cmp::Reverse(0)));
        let candidates: Vec<&(String, PathBuf)> = scored
            .iter()
            .filter(|(_, has_quota, healthy, assigned, _)| {
                (*has_quota, *healthy, std::cmp::Reverse(*assigned)) == best_tier
            })
            .map(|(item, _, _, _, _)| *item)
            .collect();
        let hash = agent.name.bytes().fold(0usize, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(b as usize)
        });
        let chosen = candidates[hash % candidates.len()];
        write_profile_marker(
            target_config,
            ".anyagents-profile",
            &format!("{provider}/{}\n", chosen.0),
        );
        roots.push(chosen.1.clone());
    } else {
        write_profile_marker(
            target_config,
            ".anyagents-profile",
            &format!("{provider}/default\n"),
        );
    }
    roots.push(data.join("profiles").join(&provider).join("default"));
    roots.push(data.join("profiles").join(&agent.harness).join("default"));
    roots.push(data.join("config").join(&provider).join("default"));
    roots
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
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| serde_json::from_str::<SubagentDefaultsConfig>(&text).ok())
        .unwrap_or_default()
}

pub(crate) fn save_subagent_defaults(
    data: &Path,
    cfg: &SubagentDefaultsConfig,
) -> Result<(), Error> {
    let path = data.join("subagent-defaults.json");
    let text = serde_json::to_string_pretty(cfg).map_err(|err| Error::Message(err.to_string()))?;
    std::fs::write(path, format!("{text}\n"))?;
    Ok(())
}

/// Resolve `(harness, model, profile)` for a newly created agent or sub-agent.
pub(crate) fn resolve_subagent_defaults(
    data: &Path,
    parent: Option<&Agent>,
    raw_harness: &str,
    raw_model: Option<String>,
    raw_profile: Option<String>,
) -> (String, Option<String>, Option<String>) {
    let h_trim = raw_harness.trim();
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
                matches!(harness.as_str(), "sylphx" | "sylphx-agent" | "sylphx_agent")
                    .then(|| crate::sylphx::DEFAULT_SYLPHX_MODEL.to_string())
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
            "claude-code".to_string()
        } else {
            parent_agent.harness.clone()
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
        matches!(
            resolved_harness.as_str(),
            "sylphx" | "sylphx-agent" | "sylphx_agent"
        )
        .then(|| {
            spec.model
                .as_deref()
                .map(str::trim)
                .filter(|m| m.starts_with("sylphx/"))
                .map(str::to_string)
                .unwrap_or_else(|| crate::sylphx::DEFAULT_SYLPHX_MODEL.to_string())
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
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        std::fs::create_dir_all(data.join("profiles/sylphx/default")).unwrap();
        let store = Store::open(data.join("state.db")).unwrap();
        let parent = store
            .create_agent(anyagents_core::NewAgent {
                name: "prism".to_string(),
                role: None,
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#xhigh".to_string()),
                workdir: Some("/scratch/wt/owners-main".to_string()),
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
                workdir: Some("/scratch/wt/owners-main".to_string()),
                instructions: Some("Product Owner for AnyMD. Paused scope.".to_string()),
                parent_id: Some(parent.id.clone()),
            })
            .unwrap();
        let cfg = agent_config_dir(data, &owner.id);
        std::fs::create_dir_all(&cfg).unwrap();
        write_profile_marker(&cfg, ".anyagents-profile", "sylphx/default\n");
        let mut inc = anyagents_core::Incoming::new("Initial owner task");
        inc.sender = Some("agent:prism".to_string());
        let _ = store.receive(&owner.id, inc).unwrap();
        let _ = store.record_outbound(
            &owner.id,
            "agent:prism",
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
            "agent:prism",
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
            sync_agent_description_files(&store, &updated_desc, &cfg);
            let meta_after = std::fs::metadata(&cfg).unwrap();
            assert_eq!(meta_after.permissions().mode() & 0o7777, 0o0700);
            assert_eq!(meta_after.gid(), orig_gid);

            // Even if config dir had its execute bit stripped (0600), prompt sync heals it back to 0700.
            std::fs::set_permissions(&cfg, std::fs::Permissions::from_mode(0o0600)).unwrap();
            sync_agent_description_files(&store, &updated_desc, &cfg);
            let healed_mode = std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o7777;
            assert_eq!(healed_mode & 0o0700, 0o0700);

            reconcile_agent_runtime_config(&store, data, &updated_desc, &cfg, &work_dir).unwrap();
            let healed_reconcile = std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o7777;
            assert_eq!(healed_reconcile & 0o0700, 0o0700);
        }
    }

    #[test]
    fn spawn_refuses_when_config_write_is_suppressed_or_invalid() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();

        let empty_cfg = data.join("empty-codex-cfg");
        std::fs::create_dir_all(&empty_cfg).unwrap();
        let err = verify_harness_runtime_config(
            &empty_cfg,
            "codex",
            Some("sylphx/openai/gpt-5.4#xhigh"),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("codex runtime config missing") && err.contains("config.toml"),
            "unexpected err: {err}"
        );

        // Write a partial config missing [mcp_servers.anyagents]
        std::fs::write(empty_cfg.join("config.toml"), "model = \"gpt-5.4\"\n").unwrap();
        let err2 = verify_harness_runtime_config(
            &empty_cfg,
            "codex",
            Some("sylphx/openai/gpt-5.4#xhigh"),
        )
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
                instructions: Some("Should never start".to_string()),
                parent_id: None,
            },
        );
        let _ = std::fs::remove_file(data.join(".anyagents-suppress-config-write"));
        assert!(res.is_err(), "expected spawn to fail when config write suppressed");
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

        let parent = create(
            &store,
            data,
            CreateRequest {
                name: "parent-helios".to_string(),
                role: Some("Owner".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: Some(data.to_path_buf()),
                instructions: Some("Parent".to_string()),
                parent_id: None,
            },
        )
        .unwrap();

        let child = create(
            &store,
            data,
            CreateRequest {
                name: "helios-goal-settlement".to_string(),
                role: Some("Worker".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: Some(data.to_path_buf()),
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

        let mut views = vec![crate::api::AgentView {
            id: child.id.to_string(),
            name: child.name.clone(),
            role: child.role.clone(),
            aliases: Vec::new(),
            parent: Some(parent.id.to_string()),
            harness: child.harness.clone(),
            model: child.model.clone(),
            live_harness: Some(child.harness.clone()),
            live_model: child.model.clone(),
            process_started_at_ms: Some(anyagents_core::now_ms()),
            pending_switch: false,
            workdir: child.workdir.clone(),
            state: "running".to_string(),
            process: "alive".to_string(),
            turn_state: "working".to_string(),
            delivery_mode: "steer".to_string(),
            steer_support: "yes".to_string(),
            quota_reset_at_ms: None,
            block_reason: None,
            last_error: None,
            oldest_undelivered_age_ms: None,
            install: "installed".to_string(),
            description: child.instructions.clone(),
            seat_user: None,
            seat_uid: None,
            seat_gid: None,
            schedules: Vec::new(),
        }];

        crate::agent::reconcile_and_enrich_views(data, &mut views);
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
        assert!(first_msg.text.contains("workspace routing discovery unauthorized (401)"));
        store.note_delivered(&parent.id, first_msg.seq).unwrap();

        // Second check must not re-notify parent for the same error.
        crate::agent::reconcile_and_enrich_views(data, &mut views);
        assert!(
            store.next_undelivered(&parent.id).unwrap().is_none(),
            "parent should only be notified once per error"
        );
    }

    #[test]
    fn shared_agents_md_and_managed_repo_worktree_lifecycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();

        // 1. Seed shared company AGENTS.md at <data>/AGENTS.md
        std::fs::write(
            data.join("AGENTS.md"),
            "# SylphxAI Shared Constitution\nAlways reason from first principles.\n",
        )
        .unwrap();

        // 2. Seed a local Git repository to test declarative `repo` worktree provisioning
        let hub_repo = data.join("git-hubs/SylphxAI/demo");
        std::fs::create_dir_all(&hub_repo).unwrap();
        assert!(
            std::process::Command::new("git")
                .arg("-C")
                .arg(&hub_repo)
                .args(["init", "-b", "main"])
                .status()
                .unwrap()
                .success()
        );
        std::process::Command::new("git")
            .arg("-C")
            .arg(&hub_repo)
            .args(["config", "user.name", "Kyle Tse"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .arg("-C")
            .arg(&hub_repo)
            .args(["config", "user.email", "hi@kylet.se"])
            .status()
            .unwrap();
        std::fs::write(hub_repo.join("README.md"), "# Demo Repo\n").unwrap();
        std::process::Command::new("git")
            .arg("-C")
            .arg(&hub_repo)
            .args(["add", "README.md"])
            .status()
            .unwrap();
        assert!(
            std::process::Command::new("git")
                .arg("-C")
                .arg(&hub_repo)
                .args(["commit", "-m", "Initial commit"])
                .status()
                .unwrap()
                .success()
        );

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
                instructions: Some("Root owner role description.".to_string()),
                parent_id: None,
            },
        )
        .unwrap();

        // 4. Create L1 Sub-Agent with declarative `repo: "SylphxAI/demo"` (provisions isolated git worktree)
        let l1 = create(
            &store,
            data,
            CreateRequest {
                name: "l1-feature-worker".to_string(),
                role: Some("Feature Worker".to_string()),
                harness: "codex".to_string(),
                model: Some("sylphx/openai/gpt-5.4#xhigh".to_string()),
                profile: None,
                repo: Some("SylphxAI/demo".to_string()),
                branch: Some("feat/l1-work".to_string()),
                base: Some("main".to_string()),
                workdir: None,
                workdir_default: Some(root_wd.clone()),
                instructions: Some("L1 worker role description.".to_string()),
                parent_id: Some(root.id.clone()),
            },
        )
        .unwrap();

        let l1_wt = PathBuf::from(l1.workdir.as_deref().unwrap());
        assert!(l1_wt.join(".git").exists(), "L1 should have a git worktree");
        assert!(l1_wt.join("README.md").is_file());

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
                instructions: Some("L2 nested worker role description.".to_string()),
                parent_id: Some(l1.id.clone()),
            },
        )
        .unwrap();
        let l2_wt = PathBuf::from(l2.workdir.as_deref().unwrap());
        assert_ne!(l2_wt, l1_wt);
        assert!(l2_wt.join(".git").exists(), "L2 should have its own isolated git worktree");
        assert!(l2_wt.join("README.md").is_file());

        // 6. Verify L0, L1, and L2 all receive the shared AGENTS.md in their config/AGENTS.md
        // AND their own role description in `## 2. Role, Responsibilities & Standing Description`
        for (ag, expected_desc) in [
            (&root, "Root owner role description."),
            (&l1, "L1 worker role description."),
            (&l2, "L2 nested worker role description."),
        ] {
            let cfg = anyagents_core::agent_config_dir(data, &ag.id);
            let prompt = std::fs::read_to_string(cfg.join("AGENTS.md")).unwrap();
            assert!(
                prompt.contains(expected_desc),
                "missing role description for {}",
                ag.name
            );
            assert!(
                prompt.contains("## 4. Shared Instructions (AGENTS.md)")
                    && prompt.contains("# SylphxAI Shared Constitution"),
                "missing shared AGENTS.md for {}",
                ag.name
            );
            let toml = std::fs::read_to_string(cfg.join("config.toml")).unwrap();
            assert!(toml.contains("project_doc_max_bytes = 131072"));
        }

        // 7. Safe Worktree GC:
        //    - L2's worktree is clean and unmodified -> reaping L2 removes `l2_wt` cleanly.
        crate::server::reap_subagent_ephemeral_dirs(data, &l2.id);
        assert!(
            !l2_wt.exists(),
            "clean unmodified worktree should be reaped on archive"
        );

        //    - Dirty L1's worktree with an uncommitted file -> reaping L1 MUST preserve `l1_wt`!
        std::fs::write(l1_wt.join("uncommitted.txt"), "work in progress\n").unwrap();
        crate::server::reap_subagent_ephemeral_dirs(data, &l1.id);
        assert!(
            l1_wt.join("uncommitted.txt").is_file(),
            "dirty worktree must NEVER be deleted on archive"
        );
    }
}
