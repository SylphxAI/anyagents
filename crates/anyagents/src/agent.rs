//! `agents` and `groups`: the operator's view and changes, through the socket
//! the server owns when the boundary is in place, and through the store on a
//! single-user machine.

use std::path::Path;

use anyagents_core::{Agent, AgentId, Error, Store};

use crate::api::{AgentView, AgentsResponse, GroupActionRequest, GroupView, GroupsResponse};
use crate::client;
use crate::server;

use crate::AgentsCommand;

/// A failure from `usage`: the store's, or the server's answer.
pub enum UsageError {
    Store(Error),
    Refused(String),
}

impl From<Error> for UsageError {
    fn from(err: Error) -> Self {
        Self::Store(err)
    }
}

pub fn agents(data: &Path, action: AgentsCommand) -> Result<(), Error> {
    match action {
        AgentsCommand::GrantAdmin { agent } => set_admin(data, &agent, true)?,
        AgentsCommand::RevokeAdmin { agent } => set_admin(data, &agent, false)?,
        AgentsCommand::RevokeRemote { agent, session } => {
            revoke_remote_cmd(data, &agent, session.as_deref())?
        }
        AgentsCommand::Rename { agent, new_name } => rename_cmd(data, &agent, &new_name)?,
        AgentsCommand::Role { agent, role, clear } => {
            role_cmd(data, &agent, role.as_deref(), clear)?
        }
        AgentsCommand::Create {
            name,
            role,
            harness,
            model,
            profile,
            repo,
            branch,
            base,
            workdir,
            description,
            instructions,
            parent,
            prompt,
        } => {
            let body = serde_json::json!({
                "name": name,
                "role": role,
                "harness": harness,
                "model": model,
                "profile": profile,
                "repo": repo,
                "branch": branch,
                "base": base,
                "workdir": workdir.map(|path| path.display().to_string()),
                "description": description,
                "instructions": instructions,
                "parent": parent,
                "prompt": prompt,
            });
            let view = create(data, body).map_err(Error::Message)?;
            println!("{}\t{}", view.id, view.name);
        }
        AgentsCommand::Archive { name } => {
            let view = set_state(data, &name, "archived")?;
            println!("{}\t{}\t{}", view.id, view.name, view.state);
        }
        AgentsCommand::Pause { name } => {
            let view = set_state(data, &name, "paused")?;
            println!("{}\t{}\t{}", view.id, view.name, view.state);
        }
        AgentsCommand::Resume { name, prompt } => resume_cmd(data, &name, prompt)?,
        AgentsCommand::Inspect { agent, limit, json } => {
            crate::inspect_cmd(data, agent, limit, json)?
        }
        AgentsCommand::Inbox(args) => crate::channel::inbox(data, args)?,
        AgentsCommand::Logs(args) => crate::channel::conversation(data, args)?,
        AgentsCommand::Send { to, text, from } => crate::channel::send_with_options(
            data,
            crate::channel::SendOptions {
                to,
                text,
                from,
                source_key: None,
                source_time_ms: None,
                reply_to: None,
                quote: None,
                media: None,
                location: None,
                poll: None,
                checklist: None,
                article: None,
            },
        )?,
        AgentsCommand::Describe {
            agent,
            description,
            instructions,
            role,
            clear,
        } => describe_cmd(data, agent, description, instructions, role, clear)?,
        AgentsCommand::ResetSession { name } => {
            reset_session_cmd(data, &name)?;
        }
        AgentsCommand::Delete { name } => {
            delete_cmd(data, &name)?;
        }
        AgentsCommand::List {
            state,
            parent,
            tree,
            depth,
            wide,
        } => {
            list_filtered(
                data,
                state.as_deref(),
                parent.as_deref(),
                false,
                tree,
                depth,
                wide,
            )?;
        }
        AgentsCommand::Instructions {
            name,
            instructions,
            clear,
        } => {
            describe_cmd(data, Some(name), None, instructions, None, clear)?;
        }
        AgentsCommand::Harness {
            name,
            harness,
            harness_flag,
            model,
            profile,
        } => {
            let target_harness = harness
                .as_deref()
                .or(harness_flag.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    Error::Message(
                        "usage: anyagents agents harness <agent> <harness> [--model ...] [--profile ...]"
                            .into(),
                    )
                })?;
            harness_cmd(
                data,
                &name,
                target_harness,
                model.as_deref(),
                profile.as_deref(),
            )?;
        }
    }
    Ok(())
}

pub fn rename_cmd(data: &Path, agent: &str, new_name: &str) -> Result<(), Error> {
    let clean_agent = agent.trim().strip_prefix("agent:").unwrap_or(agent.trim());
    let clean_new = new_name
        .trim()
        .strip_prefix("agent:")
        .unwrap_or(new_name.trim());
    let body = serde_json::json!({
        "name": clean_new,
    });
    if client::can_call_server(data) {
        match client::call::<AgentView>(
            data,
            "POST",
            &format!("/v1/agents/{clean_agent}/rename"),
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        ) {
            Ok(Some(view)) => {
                println!(
                    "{}\t{}\t{}",
                    view.id,
                    view.name,
                    if view.aliases.is_empty() {
                        "-".to_string()
                    } else {
                        view.aliases.join(",")
                    }
                );
                return Ok(());
            }
            Ok(None) => return Err(Error::Message("the server answered nothing".into())),
            Err(err) if err.contains("404") && client::agent_identity().is_none() => {}
            Err(err) => return Err(Error::Message(err)),
        }
    }
    if client::agent_identity().is_some() {
        return Err(Error::Message(
            "administrative authority required (agent session cannot bypass server boundary)"
                .into(),
        ));
    }
    let store = open_with_key(data)?;
    let old = store.agent_by_name(clean_agent)?;
    crate::operator::ensure(&store, &old.id)?;
    store.record_admin_audit("operator", "agents.rename", &old.name)?;
    let updated = store.rename_agent(&old.id, clean_new)?;
    if old.name != updated.name {
        let browsers_dir = data.join("browsers");
        let old_browser = browsers_dir.join(&old.name);
        let new_browser = browsers_dir.join(&updated.name);
        if old_browser.is_dir()
            && !new_browser.exists()
            && std::fs::rename(&old_browser, &new_browser).is_ok()
        {
            #[cfg(unix)]
            {
                let _ = std::os::unix::fs::symlink(&updated.name, &old_browser);
            }
        }
    }
    let cfg = anyagents_core::agent_config_dir(data, &updated.id);
    let work_dir = updated
        .workdir
        .as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| anyagents_core::agent_work_dir(data, &updated.id));
    crate::create::reconcile_agent_runtime_config(&store, data, &updated, &cfg, &work_dir)?;
    crate::telegram::sync_agent_telegram_bot_name(&store, data, &updated);
    let aliases = store.agent_aliases(&updated.id).unwrap_or_default();
    println!(
        "{}\t{}\t{}",
        updated.id,
        updated.name,
        if aliases.is_empty() {
            "-".to_string()
        } else {
            aliases.join(",")
        }
    );
    Ok(())
}

pub fn role_cmd(data: &Path, agent: &str, role: Option<&str>, clear: bool) -> Result<(), Error> {
    let clean_agent = agent.trim().strip_prefix("agent:").unwrap_or(agent.trim());
    let body = serde_json::json!({
        "role": role,
        "clear": clear,
    });
    if client::can_call_server(data) {
        match client::call::<AgentView>(
            data,
            "POST",
            &format!("/v1/agents/{clean_agent}/role"),
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        ) {
            Ok(Some(view)) => {
                println!(
                    "{}\t{}\t{}",
                    view.id,
                    crate::create::format_agent_display(&view.name, view.role.as_deref()),
                    view.role.as_deref().unwrap_or("-")
                );
                return Ok(());
            }
            Ok(None) => return Err(Error::Message("the server answered nothing".into())),
            Err(err) if err.contains("404") && client::agent_identity().is_none() => {}
            Err(err) => return Err(Error::Message(err)),
        }
    }
    if client::agent_identity().is_some() {
        return Err(Error::Message(
            "administrative authority required (agent session cannot bypass server boundary)"
                .into(),
        ));
    }
    let store = open_with_key(data)?;
    let found = store.agent_by_name(clean_agent)?;
    crate::operator::ensure(&store, &found.id)?;
    store.record_admin_audit("operator", "agents.role", &found.name)?;
    let new_role = if clear {
        None
    } else if let Some(raw) = role.map(str::trim).filter(|s| !s.is_empty()) {
        let parent_wd = found
            .parent_id
            .as_ref()
            .and_then(|pid| store.agent(pid).ok())
            .and_then(|p| p.workdir);
        Some(crate::create::normalize_or_validate_agent_role(
            raw,
            found.workdir.as_deref().map(std::path::Path::new),
            parent_wd.as_deref().map(std::path::Path::new),
            data,
        )?)
    } else {
        None
    };
    let updated = store.set_agent_role(&found.id, new_role.as_deref())?;
    let cfg = anyagents_core::agent_config_dir(data, &updated.id);
    let work_dir = updated
        .workdir
        .as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| anyagents_core::agent_work_dir(data, &updated.id));
    crate::create::reconcile_agent_runtime_config(&store, data, &updated, &cfg, &work_dir)?;
    crate::telegram::sync_agent_telegram_bot_name(&store, data, &updated);
    println!(
        "{}\t{}\t{}",
        updated.id,
        crate::create::format_agent_display(&updated.name, updated.role.as_deref()),
        updated.role.as_deref().unwrap_or("-")
    );
    Ok(())
}

pub fn harness_cmd(
    data: &Path,
    agent: &str,
    harness: &str,
    model: Option<&str>,
    profile: Option<&str>,
) -> Result<(), Error> {
    let clean_agent = agent.trim().strip_prefix("agent:").unwrap_or(agent.trim());
    let body = serde_json::json!({
        "harness": harness,
        "model": model,
        "profile": profile,
    });
    if client::can_call_server(data) {
        match client::call::<serde_json::Value>(
            data,
            "POST",
            &format!("/v1/agents/{clean_agent}/harness"),
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        ) {
            Ok(result) => {
                if client::agent_identity().is_none()
                    && let Ok(store) = open(data)
                {
                    let _ = crate::create::set_agent_harness(
                        &store,
                        data,
                        "operator",
                        clean_agent,
                        harness,
                        model,
                        profile,
                    );
                }
                println!(
                    "{}",
                    result.ok_or_else(|| Error::Message("the server answered nothing".into()))?
                );
                return Ok(());
            }
            Err(err) if err.contains("404") && client::agent_identity().is_none() => {}
            Err(err) => return Err(Error::Message(err)),
        }
    }
    if client::agent_identity().is_some() {
        return Err(Error::Message(
            "administrative authority required (agent session cannot bypass server boundary)"
                .into(),
        ));
    }
    let store = open(data)?;
    let out = crate::create::set_agent_harness(
        &store,
        data,
        "operator",
        clean_agent,
        harness,
        model,
        profile,
    )?;
    println!("{out}");
    Ok(())
}

pub fn describe_cmd(
    data: &Path,
    agent_or_text: Option<String>,
    description: Option<String>,
    instructions: Option<String>,
    role: Option<String>,
    clear: bool,
) -> Result<(), Error> {
    let caller_id = client::agent_identity().map(|id| id.to_string());
    let (target_name, new_desc) = match (agent_or_text, description) {
        (Some(a), Some(d)) => (a, Some(d)),
        (Some(a), None) => {
            if caller_id.is_some()
                && role.is_none()
                && instructions.is_none()
                && (a.contains(' ') || a.len() > 28)
                && !clear
            {
                ("self".to_string(), Some(a))
            } else {
                (a, None)
            }
        }
        (None, Some(d)) => (caller_id.unwrap_or_else(|| "self".to_string()), Some(d)),
        (None, None) => {
            if instructions.is_some() || role.is_some() || clear {
                (caller_id.unwrap_or_else(|| "self".to_string()), None)
            } else {
                (caller_id.unwrap_or_else(|| "all".to_string()), None)
            }
        }
    };
    if client::can_call_server(data) {
        if target_name.eq_ignore_ascii_case("all")
            && new_desc.is_none()
            && instructions.is_none()
            && role.is_none()
            && !clear
        {
            if let Ok(store) = Store::open(data.join("state.db")) {
                crate::create::sync_all_agent_instructions(&store, data)?;
            }
            let listed = client::call::<AgentsResponse>(
                data,
                "GET",
                "/v1/agents",
                None,
                &client::authority(data),
            )
            .map_err(Error::Message)?;
            if let Some(listed) = listed {
                for a in listed.agents {
                    if a.state == "archived" && a.parent.is_some() {
                        continue;
                    }
                    println!(
                        "{}\t{}\t{}\t{}",
                        crate::create::format_agent_display(&a.name, a.role.as_deref()),
                        a.harness,
                        a.state,
                        a.description.as_deref().unwrap_or("-")
                    );
                }
                return Ok(());
            }
        }
        if new_desc.is_some() || instructions.is_some() || role.is_some() || clear {
            let body = serde_json::json!({
                "description": new_desc,
                "instructions": instructions,
                "role": role,
                "clear": clear,
            });
            let answered = client::call::<serde_json::Value>(
                data,
                "POST",
                &format!("/v1/agents/{target_name}/description"),
                Some(body.to_string().into_bytes()),
                &client::authority(data),
            )
            .map_err(Error::Message)?;
            if let Some(val) = answered {
                let n = val
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&target_name);
                let r = val.get("role").and_then(|v| v.as_str());
                let d = val
                    .get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-");
                println!(
                    "updated\t{}\t{d}",
                    crate::create::format_agent_display(n, r)
                );
                return Ok(());
            }
        } else {
            let answered = client::call::<serde_json::Value>(
                data,
                "GET",
                &format!("/v1/agents/{target_name}/description"),
                None,
                &client::authority(data),
            )
            .map_err(Error::Message)?;
            if let Some(val) = answered {
                let n = val
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&target_name);
                let r = val.get("role").and_then(|v| v.as_str());
                let d = val
                    .get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-");
                println!("{}\t{d}", crate::create::format_agent_display(n, r));
                return Ok(());
            }
        }
    }
    let store = Store::open(data.join("state.db"))?;
    if target_name == "all" {
        crate::create::sync_all_agent_instructions(&store, data)?;
        println!("synced\tall");
        return Ok(());
    }
    let mut rec = store.agent_by_name(&target_name)?;
    if let Some(ref r) = role {
        let new_r = if r.trim().is_empty() {
            None
        } else {
            let parent_wd = rec
                .parent_id
                .as_ref()
                .and_then(|pid| store.agent(pid).ok())
                .and_then(|p| p.workdir);
            Some(crate::create::normalize_or_validate_agent_role(
                r.trim(),
                rec.workdir.as_deref().map(std::path::Path::new),
                parent_wd.as_deref().map(std::path::Path::new),
                data,
            )?)
        };
        rec = store.set_agent_role(&rec.id, new_r.as_deref())?;
        crate::telegram::sync_agent_telegram_bot_name(&store, data, &rec);
    }
    if clear {
        rec = store.set_agent_description(&rec.id, None)?;
        rec = store.set_agent_instructions(&rec.id, None)?;
    } else {
        if let Some(ref text) = new_desc {
            rec = store.set_agent_description(&rec.id, Some(text))?;
            if instructions.is_none() && rec.instructions.is_none() {
                rec = store.set_agent_instructions(&rec.id, Some(text))?;
            }
            crate::telegram::sync_agent_telegram_bot_name(&store, data, &rec);
        }
        if let Some(ref inst) = instructions {
            let trimmed = inst.trim();
            rec = store.set_agent_instructions(
                &rec.id,
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                },
            )?;
        }
    }
    if rec.parent_id.is_none() {
        crate::create::sync_all_agent_instructions(&store, data)?;
    } else {
        let cfg = anyagents_core::agent_config_dir(data, &rec.id);
        crate::create::sync_agent_description_files(&store, &rec, &cfg)?;
    }
    let pub_desc = rec.public_description();
    if new_desc.is_some() || instructions.is_some() || role.is_some() || clear {
        println!(
            "updated\t{}\t{}",
            crate::create::format_agent_display(&rec.name, rec.role.as_deref()),
            pub_desc.as_deref().unwrap_or("-")
        );
    } else {
        println!(
            "{}\t{}",
            crate::create::format_agent_display(&rec.name, rec.role.as_deref()),
            pub_desc.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn spawn(
    data: &Path,
    name: Option<String>,
    harness: Option<String>,
    model: Option<String>,
    profile: Option<String>,
    repo: Option<String>,
    branch: Option<String>,
    base: Option<String>,
    workdir: Option<std::path::PathBuf>,
    description: Option<String>,
    instructions: Option<String>,
    parent: Option<String>,
    prompt: Option<String>,
) -> Result<(), Error> {
    let resolved_name = match name.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
        Some(n) => n,
        None => {
            let prefix = parent
                .as_deref()
                .or_else(|| client::agent_identity().map(|_| "sub"))
                .unwrap_or("worker");
            let raw = AgentId::generate().to_string();
            let short = raw
                .strip_prefix("agt_")
                .unwrap_or(&raw)
                .chars()
                .take(8)
                .collect::<String>()
                .to_ascii_lowercase();
            format!("{prefix}-sub-{short}")
        }
    };
    let body = serde_json::json!({
        "name": resolved_name,
        "harness": harness.unwrap_or_default(),
        "model": model,
        "profile": profile,
        "repo": repo,
        "branch": branch,
        "base": base,
        "workdir": workdir.map(|path| path.display().to_string()),
        "description": description,
        "instructions": instructions,
        "parent": parent,
        "prompt": prompt,
    });
    let view = create(data, body).map_err(Error::Message)?;
    println!(
        "{}\t{}\t{}\t{}\t{}",
        view.id,
        view.name,
        view.parent.as_deref().unwrap_or("-"),
        view.harness,
        view.state
    );
    Ok(())
}

pub fn archive_cmd(data: &Path, name: Option<String>) -> Result<(), Error> {
    let target = match name.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
        Some(n) => n,
        None => client::agent_identity()
            .map(|id| id.to_string())
            .ok_or_else(|| Error::Message("archive needs an agent name or id".into()))?,
    };
    let view = set_state(data, &target, "archived")?;
    println!("{}\t{}\t{}", view.id, view.name, view.state);
    Ok(())
}

pub fn pause_cmd(data: &Path, name: Option<String>) -> Result<(), Error> {
    let target = match name.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
        Some(n) => n,
        None => client::agent_identity()
            .map(|id| id.to_string())
            .ok_or_else(|| Error::Message("pause needs an agent name or id".into()))?,
    };
    let view = set_state(data, &target, "paused")?;
    println!("{}\t{}\t{}", view.id, view.name, view.state);
    Ok(())
}

pub fn delete_cmd(data: &Path, name: &str) -> Result<(), Error> {
    if client::can_call_server(data) {
        let answered = client::call::<serde_json::Value>(
            data,
            "DELETE",
            &format!("/v1/agents/{name}"),
            None,
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        if let Some(val) = answered {
            let id = val.get("id").and_then(|v| v.as_str()).unwrap_or("-");
            let deleted_name = val.get("name").and_then(|v| v.as_str()).unwrap_or(name);
            println!("{id}\t{deleted_name}\tdeleted");
            return Ok(());
        }
        return Err(Error::Message("the server answered nothing".to_string()));
    }
    let store = open(data)?;
    let found = store.agent_by_name(name)?;
    let _lifecycle = store.lock_agent_lifecycle(&found.id)?;
    let found = store.agent(&found.id)?;
    let deleted = store.delete_agent(&found.id)?;
    let agent_dir = data.join("agents").join(deleted.id.as_str());
    let _ = std::fs::remove_dir_all(&agent_dir);
    println!("{}\t{}\tdeleted", deleted.id, deleted.name);
    Ok(())
}

pub fn reset_session_cmd(data: &Path, name: &str) -> Result<(), Error> {
    let result = if client::can_call_server(data) {
        client::call::<serde_json::Value>(
            data,
            "POST",
            &format!("/v1/agents/{name}/session/reset"),
            Some(b"{}".to_vec()),
            &client::authority(data),
        )
        .map_err(Error::Message)?
        .ok_or_else(|| Error::Message("the server answered nothing".into()))?
    } else {
        if client::agent_identity().is_some() {
            return Err(Error::Message(
                "agent session reset requires the authenticated server; offline database fallback is disabled".into(),
            ));
        }
        let store = open(data)?;
        let found = store.agent_by_name(name)?;
        let _lifecycle = store.try_lock_agent_lifecycle(&found.id)?;
        let found = store.agent(&found.id)?;
        crate::create::reset_agent_session_locked(&store, &found)?
    };
    println!("{result}");
    Ok(())
}

pub fn resume_cmd(data: &Path, name: &str, prompt: Option<String>) -> Result<(), Error> {
    let view = set_state(data, name, "running")?;
    if let Some(text) = prompt.filter(|s| !s.trim().is_empty()) {
        crate::channel::send_simple(data, &view.name, text)?;
    }
    println!("{}\t{}\t{}", view.id, view.name, view.state);
    Ok(())
}

pub fn peers_with_options(
    data: &Path,
    state: Option<&str>,
    parent: Option<&str>,
    tree: bool,
    depth: Option<usize>,
    wide: bool,
) -> Result<(), Error> {
    list_filtered(data, state, parent, true, tree, depth, wide)
}

fn set_state(data: &Path, name: &str, state: &str) -> Result<AgentView, Error> {
    let verb = match state {
        "archived" => "archive",
        "paused" => "pause",
        _ => "wake",
    };
    if client::can_call_server(data) {
        let answered = client::call::<AgentView>(
            data,
            "POST",
            &format!("/v1/agents/{name}/{verb}"),
            Some(b"{}".to_vec()),
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        let view =
            answered.ok_or_else(|| Error::Message("the server answered nothing".to_string()))?;
        return Ok(view);
    }
    let store = open_with_key(data)?;
    let found = store.agent_by_name(name)?;
    let _lifecycle = store.lock_agent_lifecycle(&found.id)?;
    let found = store.agent(&found.id)?;
    if state == "running" {
        crate::operator::ensure(&store, &found.id)?;
        let cfg = anyagents_core::agent_config_dir(data, &found.id);
        let work_dir = found
            .workdir
            .as_deref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| anyagents_core::agent_work_dir(data, &found.id));
        std::fs::create_dir_all(&work_dir)?;
        if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
            || std::env::var_os("ANYAGENTS_SUPPRESS_CONFIG_WRITE").is_some()
        {
            crate::create::reconcile_agent_runtime_config(&store, data, &found, &cfg, &work_dir)?;
        }
        let _ = std::fs::remove_file(cfg.join(".anyagents-last-error.json"));
        let _ = std::fs::remove_file(cfg.join(".anyagents-notified-parent-error.json"));
        let now = anyagents_core::now_ms();
        crate::create::write_profile_marker(
            &cfg,
            ".anyagents-quota-cleared-ms",
            &format!("{now}\n"),
        )?;
        if !store.has_open_turn(&found.id).unwrap_or(false) {
            crate::create::write_profile_marker(
                &cfg,
                ".anyagents-profile-rotated-ms",
                &format!("{now}\n"),
            )?;
        }
    }
    if state == "running" {
        crate::worktree_access::restore_recorded(data, &found.id)?;
        server::clear_explicitly_archived(data, &found.id);
    }
    let agent = store.set_agent_state(&found.id, state)?;
    if state == "running" {
        crate::create::poke_agent_wake_socket(data, &agent.id);
    } else if state == "archived" {
        let archived = server::mark_explicitly_archived_locked(&store, data, &agent)?;
        // The offline path has no daemon maintenance worker. Keep the parent's
        // lifecycle lock, taking descendant locks only downward as above.
        for id in archived {
            if id == agent.id {
                server::reap_archived_agent_locked(&store, data, &id);
            } else if let Ok(_child_lifecycle) = store.try_lock_agent_lifecycle(&id) {
                server::reap_archived_agent_locked(&store, data, &id);
            }
        }
    }
    let lag = store.lag(&agent.id, anyagents_core::now_ms())?;
    let aliases = store.agent_aliases(&agent.id).unwrap_or_default();
    let seat_info = crate::create::resolved_agent_seat_info(&store, &agent);
    let public_desc = agent.public_description();
    let scope_val = agent.scope().to_string();
    let lifecycle_val = agent.lifecycle().to_string();
    let delivery: crate::api::DeliveryView =
        (&crate::vendor::declared_delivery(&agent.harness)).into();
    Ok(AgentView {
        has_open_turn: false,
        open_turn_age_ms: None,
        profile: None,
        id: agent.id.to_string(),
        name: agent.name,
        role: agent.role,
        aliases,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        scope: scope_val,
        lifecycle: lifecycle_val,
        relationship: None,
        can_message: None,
        harness: agent.harness,
        model: agent.model,
        live_harness: None,
        live_model: None,
        process_started_at_ms: None,
        pending_switch: false,
        workdir: agent.workdir,
        state: agent.state,
        process: "-".to_string(),
        turn_state: "idle".to_string(),
        delivery_mode: delivery.legacy_mode(),
        steer_support: delivery.legacy_steer(),
        delivery,
        quota_reset_at_ms: None,
        block_reason: None,
        last_error: None,
        oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
        install: "-".to_string(),
        description: public_desc,
        instructions: agent.instructions,
        seat_user: seat_info.as_ref().map(|(u, _, _, _)| u.clone()),
        seat_uid: seat_info.as_ref().map(|(_, uid, _, _)| *uid),
        seat_gid: seat_info.as_ref().map(|(_, _, gid, _)| *gid),
        schedules: crate::schedule::list_agent_schedules(&store, &agent.id, false)
            .map_err(Error::Message)?,
        turn_errors: None,
        unsettled_outbound: Vec::new(),
    })
}

/// `agents create`. The server owns it when it is there, so one process makes
/// the agent, mints its capability and starts it.
fn create(data: &Path, body: serde_json::Value) -> Result<AgentView, String> {
    if client::can_call_server(data) {
        match client::call::<AgentView>(
            data,
            "POST",
            "/v1/agents",
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        ) {
            Ok(answered) => {
                return answered.ok_or_else(|| "the server answered nothing".to_string());
            }
            Err(err)
                if body.get("parent").and_then(|v| v.as_str()) == Some("root")
                    && err.contains("404") => {}
            Err(err) => return Err(err),
        }
    }
    let mut parsed: crate::api::CreateAgentRequest =
        serde_json::from_value(body).map_err(|err| err.to_string())?;
    let store = open_with_key(data).map_err(|err| err.to_string())?;
    let parent_agent = match parsed.parent.as_deref() {
        Some("root" | "") => None,
        Some(parent_ref) => Some(
            store
                .agent_by_name(parent_ref)
                .map_err(|err| err.to_string())?,
        ),
        None => None,
    };
    crate::create::apply_archetype_defaults(Some(&store), data, parent_agent.as_ref(), &mut parsed)
        .map_err(|err| err.to_string())?;
    if let Some(scope) = parsed.scope.as_deref() {
        anyagents_core::parse_agent_scope(scope).map_err(|err| err.to_string())?;
    }
    if let Some(lifecycle) = parsed.lifecycle.as_deref() {
        anyagents_core::parse_agent_lifecycle(lifecycle).map_err(|err| err.to_string())?;
    }
    let parent_id = parent_agent.as_ref().map(|p| p.id.clone());
    let (harness, model, profile) = crate::create::resolve_subagent_defaults(
        data,
        parent_agent.as_ref(),
        parsed.harness.as_deref().unwrap_or("claude-code"),
        parsed.model,
        parsed.profile,
    );
    if crate::vendor::is_cloud_harness(&harness) {
        crate::remote::require_public_url(data)?;
    }
    let mut agent = crate::create::create(
        &store,
        data,
        crate::create::CreateRequest {
            name: parsed.name,
            role: parsed.role,
            harness,
            model,
            profile,
            repo: parsed.repo,
            branch: parsed.branch,
            base: parsed.base,
            workdir: parsed.workdir.map(std::path::PathBuf::from),
            workdir_default: server::owner_workdir(),
            description: parsed.description,
            instructions: parsed.instructions,
            parent_id,
        },
    )
    .map_err(|err| err.to_string())?;
    if let Some(ref scope_val) = parsed.scope {
        agent = store
            .set_agent_scope_as(&agent.id, scope_val, "operator")
            .map_err(|err| err.to_string())?;
        let cfg = anyagents_core::agent_config_dir(data, &agent.id);
        crate::create::sync_agent_description_files(&store, &agent, &cfg)
            .map_err(|error| error.to_string())?;
    } else if let Some(pub_val) = parsed.public {
        agent = store
            .set_agent_public_as(&agent.id, pub_val, "operator")
            .map_err(|err| err.to_string())?;
        let cfg = anyagents_core::agent_config_dir(data, &agent.id);
        crate::create::sync_agent_description_files(&store, &agent, &cfg)
            .map_err(|error| error.to_string())?;
    }
    if let Some(ref lifecycle_val) = parsed.lifecycle {
        agent = store
            .set_agent_lifecycle_as(&agent.id, lifecycle_val, "operator")
            .map_err(|err| err.to_string())?;
    }
    // A create is a new instance: its capability is minted so its harness
    // starts with one.
    crate::operator::mint(&store, &agent.id).map_err(|err| err.to_string())?;
    if let Some(prompt) = parsed
        .prompt
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        store
            .receive(&agent.id, anyagents_core::Incoming::new(prompt))
            .map_err(|e| e.to_string())?;
    }
    let lag = store
        .lag(&agent.id, anyagents_core::now_ms())
        .map_err(|err| err.to_string())?;
    let aliases = store.agent_aliases(&agent.id).unwrap_or_default();
    let seat_info = crate::create::resolved_agent_seat_info(&store, &agent);
    let public_desc = agent.public_description();
    let scope_val = agent.scope().to_string();
    let lifecycle_val = agent.lifecycle().to_string();
    let delivery: crate::api::DeliveryView =
        (&crate::vendor::declared_delivery(&agent.harness)).into();
    Ok(AgentView {
        has_open_turn: false,
        open_turn_age_ms: None,
        profile: None,
        id: agent.id.to_string(),
        name: agent.name,
        role: agent.role,
        aliases,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        scope: scope_val,
        lifecycle: lifecycle_val,
        relationship: None,
        can_message: None,
        harness: agent.harness,
        model: agent.model,
        live_harness: None,
        live_model: None,
        process_started_at_ms: None,
        pending_switch: false,
        workdir: agent.workdir,
        state: agent.state,
        process: "-".to_string(),
        turn_state: "idle".to_string(),
        delivery_mode: delivery.legacy_mode(),
        steer_support: delivery.legacy_steer(),
        delivery,
        quota_reset_at_ms: None,
        block_reason: None,
        last_error: None,
        oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
        install: "-".to_string(),
        description: public_desc,
        instructions: agent.instructions,
        seat_user: seat_info.as_ref().map(|(u, _, _, _)| u.clone()),
        seat_uid: seat_info.as_ref().map(|(_, uid, _, _)| *uid),
        seat_gid: seat_info.as_ref().map(|(_, _, gid, _)| *gid),
        schedules: crate::schedule::list_agent_schedules(&store, &agent.id, false)?,
        turn_errors: None,
        unsettled_outbound: Vec::new(),
    })
}

/// End an agent's remote sessions: through the server when it runs, else in
/// the store directly, which every server process reads on the next call.
fn revoke_remote_cmd(data: &Path, agent: &str, session: Option<&str>) -> Result<(), Error> {
    if client::can_call_server(data) {
        let body = serde_json::json!({ "session_id": session });
        let answered = client::call::<serde_json::Value>(
            data,
            "POST",
            &format!("/v1/operator/agents/{agent}/remote-sessions/revoke"),
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        )
        .map_err(Error::Message)?
        .ok_or_else(|| Error::Message("the server answered nothing".into()))?;
        println!("{answered}");
        return Ok(());
    }
    let store = open(data)?;
    let rec = store.agent_by_name(agent)?;
    store.record_admin_audit("operator", "agents.revoke_remote_sessions", &rec.name)?;
    let revoked = crate::remote::revoke(&store, &rec.id, session)?;
    println!(
        "{}",
        serde_json::json!({ "agent": rec.name, "session_id": session, "revoked": revoked })
    );
    Ok(())
}

fn set_admin(data: &Path, agent: &str, admin: bool) -> Result<(), Error> {
    let body = serde_json::json!({"admin": admin});
    match client::call::<serde_json::Value>(
        data,
        "POST",
        &format!("/v1/agents/{agent}/admin"),
        Some(body.to_string().into_bytes()),
        &client::authority(data),
    ) {
        Ok(result) => {
            println!(
                "{}",
                result.ok_or_else(|| Error::Message("the server answered nothing".into()))?
            );
            return Ok(());
        }
        Err(err) if err.contains("404") && client::agent_identity().is_none() => {}
        Err(err) => return Err(Error::Message(err)),
    }
    let store = open(data)?;
    let rec = store.agent_by_name(agent)?;
    store.record_admin_audit(
        "operator",
        if admin {
            "agents.grant_admin"
        } else {
            "agents.revoke_admin"
        },
        &rec.name,
    )?;
    store.set_agent_admin(&rec.id, admin)?;
    println!("{}", serde_json::json!({"agent": rec.name, "admin": admin}));
    Ok(())
}

fn apply_parent_filter_for_views(
    agents: &mut Vec<AgentView>,
    parent_filter: Option<&str>,
    pretty: bool,
) -> bool {
    let Some(pf) = parent_filter.map(str::trim).filter(|s| !s.is_empty()) else {
        return false;
    };
    let cleaned = pf.strip_prefix("agent:").unwrap_or(pf);
    if cleaned.eq_ignore_ascii_case("root") || cleaned.eq_ignore_ascii_case("none") {
        if !pretty {
            agents.retain(|a| a.parent.is_none());
        }
        return false;
    }
    if cleaned.eq_ignore_ascii_case("subagents") || cleaned.eq_ignore_ascii_case("all") {
        if !pretty {
            agents.retain(|a| a.parent.is_some());
        }
        return true;
    }
    let wanted_id = if cleaned.eq_ignore_ascii_case("self") {
        client::agent_identity().map(|id| id.to_string())
    } else {
        agents
            .iter()
            .find(|a| {
                a.name.eq_ignore_ascii_case(cleaned)
                    || a.id == cleaned
                    || a.aliases.iter().any(|al| al.eq_ignore_ascii_case(cleaned))
            })
            .map(|a| a.id.clone())
            .or_else(|| Some(cleaned.to_string()))
    };
    if let Some(pid) = wanted_id {
        if pretty {
            let mut keep_ids = std::collections::HashSet::new();
            keep_ids.insert(pid.clone());
            loop {
                let mut added = false;
                for a in agents.iter() {
                    if let Some(ref p) = a.parent
                        && keep_ids.contains(p)
                        && keep_ids.insert(a.id.clone())
                    {
                        added = true;
                    }
                }
                if !added {
                    break;
                }
            }
            agents.retain(|a| keep_ids.contains(&a.id));
            for a in agents.iter_mut() {
                if a.id == pid {
                    a.parent = None;
                }
            }
        } else {
            agents.retain(|a| a.parent.as_deref() == Some(pid.as_str()));
        }
    }
    false
}

/// `agents list` / `peers`: the server's view when it is there, else the stored rows.
fn list_filtered(
    data: &Path,
    state_filter: Option<&str>,
    parent_filter: Option<&str>,
    show_parent: bool,
    tree: bool,
    depth: Option<usize>,
    wide: bool,
) -> Result<(), Error> {
    let pretty = crate::ui::is_pretty();
    let is_root_only_filter = parent_filter
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|pf| pf.strip_prefix("agent:").unwrap_or(pf))
        .is_some_and(|c| c.eq_ignore_ascii_case("root") || c.eq_ignore_ascii_case("none"));
    let has_specific_or_subagents_parent = parent_filter
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .is_some()
        && !is_root_only_filter;
    let effective_max_depth = if is_root_only_filter && !tree && depth.is_none() {
        Some(0)
    } else if let Some(d) = depth {
        Some(d)
    } else if tree || has_specific_or_subagents_parent {
        None
    } else {
        Some(0)
    };

    let query = server::ListAgentsQuery {
        state: state_filter.map(str::to_string),
        // Parent filtering below preserves the CLI's subtree/depth semantics.
        parent: None,
        name: None,
    };
    let path = state_filter
        .map(|s| format!("/v1/agents?state={s}"))
        .unwrap_or_else(|| "/v1/agents".into());
    let mut listed = match client::get::<AgentsResponse>(data, &path).map_err(Error::Message)? {
        Some(listed) => listed,
        None => offline_roster(data, &query)?,
    };
    let only_with_subagents =
        apply_parent_filter_for_views(&mut listed.agents, parent_filter, pretty);
    let include_archived = state_filter.is_some_and(|s| matches!(s, "all" | "archived"));
    if pretty {
        crate::ui::print_agents_table_with_options(
            &listed.agents,
            None,
            crate::ui::AgentsTableOptions {
                include_archived,
                archived_count: Some(listed.archived_count),
                max_depth: effective_max_depth,
                only_with_subagents,
                wide,
            },
        );
    } else {
        for agent in listed
            .agents
            .iter()
            .filter(|a| include_archived || a.state != "archived")
        {
            if show_parent {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    agent.id,
                    agent.name,
                    agent.parent.as_deref().unwrap_or("-"),
                    agent.harness,
                    agent.model.as_deref().unwrap_or("-"),
                    agent.display_state(),
                    agent.process,
                    show_optional(agent.oldest_undelivered_age_ms)
                );
            } else {
                print_agent(AgentLine {
                    id: &agent.id,
                    name: &agent.name,
                    harness: &agent.harness,
                    state: agent.display_state(),
                    process: &agent.process,
                    delivery: agent.delivery.label(),
                    undelivered: agent.oldest_undelivered_age_ms,
                    quota: agent.quota_reset_at_ms,
                });
            }
        }
    }
    Ok(())
}

pub fn offline_roster(
    data: &Path,
    query: &server::ListAgentsQuery,
) -> Result<AgentsResponse, Error> {
    if client::agent_identity().is_some() {
        return Err(Error::Message(
            "agent roster reads require the authenticated server".into(),
        ));
    }
    let store = open(data)?;
    let (agents, archived_count) = store.roster_agents(query.state.as_deref())?;
    server::roster_views(
        &store,
        &anyagents_supervisor::HealthBoard::default(),
        &agents.iter().collect::<Vec<_>>(),
        archived_count,
        query,
    )
    .map_err(|e| Error::Message(e.message))
}

pub fn view_named(data: &Path, target: &str) -> Result<AgentView, Error> {
    if let Some(view) = client::get::<AgentView>(
        data,
        &format!("/v1/agents/{}", crate::channel::urlencoding_simple(target)),
    )
    .map_err(Error::Message)?
    {
        return Ok(view);
    }
    if client::agent_identity().is_some() {
        return Err(Error::Message(
            "agent reads require the authenticated server".into(),
        ));
    }
    let store = open(data)?;
    let agent = store.agent_by_name(target)?;
    server::agent_view(
        &store,
        &anyagents_supervisor::HealthBoard::default(),
        &agent,
    )
    .map_err(|e| Error::Message(e.message))
}

/// The store, holding a capability key: the one this process was given, else a
/// fresh one. A command that creates an agent before its server exists mints a
/// capability the server will not recognise, so that server mints its own the
/// next time it starts; this path is the single-user route only.
pub fn open_with_key(data: &Path) -> Result<Store, Error> {
    let store = open(data)?;
    let key = match std::env::var("ANYAGENTS_CAPABILITY_KEY") {
        Ok(key) if key.len() >= 32 => key,
        _ => {
            use std::io::Read as _;
            let mut bytes = [0u8; 32];
            std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }
    };
    store.hold_capability_key(&key)?;
    Ok(store)
}

/// The store. Where the boundary is in place this is the operator's own store,
/// as the server keeps it; only an operator token can be running this command
/// there. An agent's tools never open it: they use the socket.
pub fn open(data: &Path) -> Result<Store, Error> {
    Store::open(data.join("state.db"))
}

/// One agent by name, from the socket when the server is there.
pub fn named(data: &Path, agent: &str) -> Result<Agent, Error> {
    if !client::can_call_server(data) && client::agent_identity().is_none() {
        return open(data)?.agent_by_name(agent);
    }
    let found = view_named(data, agent)?;
    Ok(Agent {
        is_admin: false,
        id: AgentId::parse(&found.id)?,
        name: found.name,
        role: found.role,
        parent_id: found.parent.and_then(|p| AgentId::parse(&p).ok()),
        harness: found.harness,
        model: found.model,
        workdir: found.workdir,
        description: found.description,
        instructions: found.instructions,
        state: found.state,
        created_at_ms: 0,
        launch_env: Vec::new(),
    })
}

pub fn list_groups(data: &Path) -> Result<Vec<(String, Vec<String>)>, Error> {
    if client::can_call_server(data) {
        let resp = client::call::<GroupsResponse>(
            data,
            "GET",
            "/v1/groups",
            None,
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        if let Some(resp) = resp {
            return Ok(resp
                .groups
                .into_iter()
                .map(|g| (g.name, g.members))
                .collect());
        }
    }
    open(data)?.list_groups()
}

pub fn join_group(data: &Path, group: &str, agent_id: &AgentId) -> Result<(), Error> {
    if client::can_call_server(data) {
        let body = serde_json::to_vec(&GroupActionRequest {
            action: "join".to_string(),
            group: group.to_string(),
            agent: Some(agent_id.to_string()),
            public: None,
        })
        .map_err(|err| Error::Message(err.to_string()))?;
        let _ = client::call::<GroupView>(
            data,
            "POST",
            "/v1/groups",
            Some(body),
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        return Ok(());
    }
    if client::agent_identity().is_some() {
        return Err(Error::Message("agent group mutations require the authenticated server; offline database fallback is disabled".into()));
    }
    open(data)?.join_group(group, agent_id)
}

pub fn leave_group(data: &Path, group: &str, agent_id: &AgentId) -> Result<(), Error> {
    if client::can_call_server(data) {
        let body = serde_json::to_vec(&GroupActionRequest {
            action: "leave".to_string(),
            group: group.to_string(),
            agent: Some(agent_id.to_string()),
            public: None,
        })
        .map_err(|err| Error::Message(err.to_string()))?;
        let _ = client::call::<GroupView>(
            data,
            "POST",
            "/v1/groups",
            Some(body),
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        return Ok(());
    }
    if client::agent_identity().is_some() {
        return Err(Error::Message("agent group mutations require the authenticated server; offline database fallback is disabled".into()));
    }
    open(data)?.leave_group(group, agent_id)
}

/// The agents in a group.
pub fn group_members(data: &Path, group: &str) -> Result<Vec<String>, Error> {
    if client::can_call_server(data) {
        let body = serde_json::to_vec(&GroupActionRequest {
            action: "members".to_string(),
            group: group.to_string(),
            agent: None,
            public: None,
        })
        .map_err(|err| Error::Message(err.to_string()))?;
        let resp = client::call::<GroupView>(
            data,
            "POST",
            "/v1/groups",
            Some(body),
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        if let Some(resp) = resp {
            return Ok(resp.members);
        }
    }
    open(data)?.group_members(group)
}

struct AgentLine<'a> {
    id: &'a str,
    name: &'a str,
    harness: &'a str,
    state: &'a str,
    process: &'a str,
    delivery: String,
    undelivered: Option<i64>,
    quota: Option<i64>,
}

fn print_agent(agent: AgentLine<'_>) {
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        agent.id,
        agent.name,
        agent.harness,
        agent.state,
        agent.process,
        agent.delivery,
        show_optional(agent.undelivered),
        show_optional(agent.quota),
    );
}

fn show_optional(value: Option<i64>) -> String {
    value
        .map(|number| number.to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Hand every line the running server may have left to the harness, through the
/// vendor's own input API. A line the harness already took is not written again:
/// the adapter remembers the ids in the session it resumed.
/// List an agent's parked messages, or put one back in its mailbox.
pub fn requeue(
    data: &Path,
    name: &str,
    message: Option<&str>,
) -> Result<(), anyagents_core::Error> {
    let store = open(data)?;
    let agent = store.agent_by_name(name)?;
    let Some(message) = message else {
        for parked in store.rejected_messages(&agent.id)? {
            println!(
                "{}\t{}\t{}\t{}",
                parked.message_id,
                parked.seq,
                parked.sender.as_deref().unwrap_or("-"),
                parked.reason
            );
        }
        return Ok(());
    };
    let requeued = store.requeue_rejected(&agent.id, message)?;
    println!(
        "{}\trequeued\t{message}\t{}",
        agent.name,
        requeued.id.as_str()
    );
    Ok(())
}

pub fn flush(data: &Path, name: &str) -> Result<(), anyagents_core::Error> {
    let store = open(data)?;
    let agents = if name == "all" {
        store.list_agents()?
    } else {
        vec![store.agent_by_name(name)?]
    };
    for agent in agents {
        if agent.state != "running" {
            continue;
        }
        // One vendor process per agent: a harness a server or a keeper holds
        // takes its own mail; this one-shot process would be a second.
        let _process = match store.try_lock_agent(&agent.id) {
            Ok(lock) => lock,
            Err(anyagents_core::Error::AgentBusy) => {
                println!("{}	skipped	its harness process is live", agent.name);
                continue;
            }
            Err(error) => return Err(error),
        };
        let config_dir = anyagents_core::agent_config_dir(data, &agent.id);
        let work_dir = agent
            .workdir
            .as_deref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| anyagents_core::agent_work_dir(data, &agent.id));
        let request = anyagents_supervisor::LaunchRequest {
            agent: agent.clone(),
            data_dir: data.to_path_buf(),
            config_dir,
            work_dir,
            extra_env: Vec::new(),
            lock: None,
        };
        let mut harness = match crate::vendor::vendor_launch_with_store(request, &store) {
            Ok(harness) => harness,
            Err(reason) => {
                println!("{}\tskipped\t{reason}", agent.name);
                continue;
            }
        };
        let mut flushed = 0u32;
        let mut failure = None;
        // The delivery loop's breaker: the same refusal twice is a vendor that
        // refuses everything, so nothing more is parked.
        let mut last_rejection: Option<String> = None;
        loop {
            let message = store.next_undelivered(&agent.id)?;
            let Some(message) = message else {
                break;
            };
            let outbound = anyagents_harness::OutboundMessage {
                id: message.id.as_str().to_string(),
                seq: message.seq,
                text: message.text.clone(),
                sender: message.sender.clone(),
                reply_to: message.reply_to.clone(),
                source_time_ms: message.source_time_ms,
                context: message.context.clone(),
            };
            // Only a message the harness provably took is delivered. A line
            // still in flight, deferred or quota-held stays in the mailbox for
            // the next delivery, which idempotent input makes safe to repeat.
            match anyagents_harness::Harness::deliver(&mut harness, &outbound) {
                Ok(
                    anyagents_harness::DeliverOutcome::Accepted { .. }
                    | anyagents_harness::DeliverOutcome::Duplicate,
                ) => {
                    store.note_delivered(&agent.id, message.seq)?;
                    flushed += 1;
                    last_rejection = None;
                }
                Ok(anyagents_harness::DeliverOutcome::InFlight) => {
                    failure = Some("not yet consumed by the harness; left queued".into());
                    break;
                }
                Ok(anyagents_harness::DeliverOutcome::Deferred) => {
                    failure = Some("harness deferred the message; left queued".into());
                    break;
                }
                Ok(anyagents_harness::DeliverOutcome::Quota { reset_at_ms }) => {
                    failure = Some(format!("quota until {reset_at_ms}; left queued"));
                    break;
                }
                // Refused for good: parked past the cursor, its sender told.
                Ok(anyagents_harness::DeliverOutcome::Rejected {
                    reason,
                    message_specific,
                }) => {
                    if !message_specific && last_rejection.as_deref() == Some(reason.as_str()) {
                        failure = Some(format!(
                            "the harness refuses message after message ({reason}); left queued"
                        ));
                        break;
                    }
                    store.reject_message(&agent.id, message.seq, &reason)?;
                    println!(
                        "{}\trejected\t{}\t{reason}",
                        agent.name,
                        message.id.as_str()
                    );
                    if !message_specific {
                        last_rejection = Some(reason);
                    }
                }
                Err(err) => {
                    failure = Some(err.to_string());
                    break;
                }
            }
        }
        let cursor = store.delivered_cursor(&agent.id)?;
        match failure {
            Some(reason) => println!("{}\tstopped\t{cursor}\t{flushed}\t{reason}", agent.name),
            None => println!("{}\tdelivered\t{cursor}\t{flushed}", agent.name),
        }
    }
    Ok(())
}

/// Parameters for the structured usage query.
#[derive(Debug, Default, Clone)]
pub struct UsageParams {
    pub agent: Option<String>,
    pub group_by: Option<String>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub days: Option<i64>,
    pub include_descendants: Option<bool>,
}

/// Recorded usage, summed. Prefers the daemon socket (`GET /v1/usage`) so an
/// isolated seat process never has to open the owner-only store file; only
/// when the daemon is unavailable does it fall back to a read-only direct
/// open of the store, keeping the historical output unchanged.
pub fn usage(data: &Path, params: &UsageParams) -> Result<(), UsageError> {
    use anyagents_core::{UsageReport, sum_usage};
    use std::collections::HashMap;

    // Normalize the legacy `--by day` spelling onto `group_by`.
    let group_by = params
        .group_by
        .as_deref()
        .map(str::trim)
        .filter(|g| !g.is_empty());
    let by_day = match group_by {
        None | Some("agent") => false,
        Some("day") => true,
        Some(other) => {
            return Err(UsageError::Refused(format!(
                "unknown usage grouping: {other}"
            )));
        }
    };

    // 1) Socket-first: the daemon resolves the caller from its credential
    // and enforces the causal authorization boundary server-side.
    match crate::client::get::<UsageReport>(data, &usage_path("/v1/usage", params)) {
        Ok(Some(report)) => {
            print_report(&report);
            return Ok(());
        }
        Ok(None) => {} // daemon unavailable: fall through to the read-only open
        Err(err) => return Err(UsageError::Refused(err)),
    }

    // 2) Fallback: read-only direct open of the store (historical behavior).
    // Only the legacy dimensions are renderable offline; window/subtree
    // filters require the daemon.
    let store = open(data)?;
    let agent = match params.agent.as_deref() {
        Some(name) => Some(store.agent_by_name(name)?),
        None => None,
    };
    let records = store.list_usage(agent.as_ref().map(|agent| &agent.id))?;
    let names: HashMap<String, String> = store
        .list_agents()?
        .into_iter()
        .map(|agent| (agent.id.to_string(), agent.name))
        .collect();
    let totals = sum_usage(&records, by_day);
    if crate::ui::is_pretty() {
        if totals.is_empty() {
            println!("  {}", crate::ui::dim("No usage records found."));
            return Ok(());
        }
        println!("\n  {}\n", crate::ui::bold_cyan("Token Usage"));
        let mut table = crate::ui::Table::new(&[
            ("AGENT", false),
            ("DAY", false),
            ("INPUT", true),
            ("OUTPUT", true),
            ("CACHE READ", true),
            ("CACHE WRITE", true),
            ("COST", true),
        ]);
        for total in totals {
            let name = names
                .get(total.agent_id.as_str())
                .map(String::as_str)
                .unwrap_or_else(|| total.agent_id.as_str());
            let day = total.day.as_deref().unwrap_or("all-time");
            let cost = match total.reported_cost_micros {
                Some(micros) => crate::ui::green(&format!("${:.4}", micros as f64 / 1_000_000.0)),
                None => crate::ui::dim("-"),
            };
            table.add_row(vec![
                crate::ui::bold(name),
                crate::ui::dim(day),
                crate::ui::format_tokens(total.input_tokens),
                crate::ui::format_tokens(total.output_tokens),
                crate::ui::format_tokens(total.cache_read_tokens),
                crate::ui::format_tokens(total.cache_write_tokens),
                cost,
            ]);
        }
        table.print();
        println!();
        return Ok(());
    }
    for total in totals {
        let name = names
            .get(total.agent_id.as_str())
            .map(String::as_str)
            .unwrap_or_else(|| total.agent_id.as_str());
        let day = total.day.as_deref().unwrap_or("-");
        let cost = match total.reported_cost_micros {
            Some(micros) => micros.to_string(),
            None => "-".to_string(),
        };
        println!(
            "{name}\t{day}\t{}\t{}\t{}\t{}\t{cost}",
            total.input_tokens,
            total.output_tokens,
            total.cache_read_tokens,
            total.cache_write_tokens,
        );
    }
    Ok(())
}

/// `route` with the target, window and grouping of `params` as its query.
fn usage_path(route: &str, params: &UsageParams) -> String {
    let group_by = params
        .group_by
        .as_deref()
        .map(str::trim)
        .filter(|g| !g.is_empty());
    let mut qs: Vec<(&str, String)> = Vec::new();
    if let Some(agent) = params.agent.as_deref().filter(|a| !a.trim().is_empty()) {
        qs.push(("agent", agent.to_string()));
    }
    if let Some(v) = params.include_descendants {
        qs.push(("include_descendants", v.to_string()));
    }
    if let Some(v) = params.since_ms {
        qs.push(("since_ms", v.to_string()));
    }
    if let Some(v) = params.until_ms {
        qs.push(("until_ms", v.to_string()));
    }
    if let Some(v) = params.days {
        qs.push(("days", v.to_string()));
    }
    if let Some(g) = group_by {
        qs.push(("group_by", g.to_string()));
    }
    if qs.is_empty() {
        return route.to_string();
    }
    let q = qs
        .iter()
        .map(|(k, v)| format!("{k}={}", crate::mcp::urlencoding_simple_pub(v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{route}?{q}")
}

/// Outcomes, recovery and cost per successful outcome (ADR 0008). Prefers
/// the daemon socket (`GET /v1/outcomes`), which applies the caller's
/// boundary; only the operator falls back to a read of the store when the
/// daemon is unavailable.
pub fn outcomes(data: &Path, params: &UsageParams) -> Result<(), UsageError> {
    use anyagents_core::{OutcomeGroupBy, OutcomeReport, UsageFilter, UsageGroupBy};
    let report =
        match crate::client::get::<OutcomeReport>(data, &usage_path("/v1/outcomes", params)) {
            Ok(Some(report)) => report,
            // An agent reads only through the server, which applies its boundary.
            Ok(None) if client::agent_identity().is_some() => {
                return Err(UsageError::Refused(
                    "the outcome report needs the running server".to_string(),
                ));
            }
            Ok(None) => {
                let store = open(data)?;
                let agents = store.list_agents()?;
                let ids = anyagents_core::resolve_usage_agents(
                    None,
                    &agents,
                    params.agent.as_deref(),
                    params.include_descendants,
                )
                .map_err(|err| UsageError::Refused(err.to_string()))?;
                let group_by = OutcomeGroupBy::parse(params.group_by.as_deref().unwrap_or(""))
                    .map_err(UsageError::Refused)?;
                let now = anyagents_core::now_ms();
                let (since, until) = UsageFilter {
                    since_ms: params.since_ms,
                    until_ms: params.until_ms,
                    days: params.days,
                    group_by: UsageGroupBy::Agent,
                }
                .window(now)
                .map_err(UsageError::Refused)?;
                store.outcome_report(&ids, since, until.unwrap_or(now), group_by)?
            }
            Err(err) => return Err(UsageError::Refused(err)),
        };
    print_outcomes(&report);
    Ok(())
}

/// The grouping a report row stands for, as one label.
fn outcome_label(group: &anyagents_core::OutcomeGroup) -> String {
    let parts: Vec<&str> = [
        group.harness.as_deref(),
        group.model.as_deref(),
        group.role.as_deref(),
        group.profile.as_deref(),
        group.agent_name.as_deref(),
        group.skill.as_deref(),
    ]
    .into_iter()
    .flatten()
    .collect();
    if parts.is_empty() {
        "-".to_string()
    } else {
        parts.join("/")
    }
}

fn print_outcomes(report: &anyagents_core::OutcomeReport) {
    let money = |micros: Option<i64>| match micros {
        Some(micros) => format!("${:.4}", micros as f64 / 1_000_000.0),
        None => "-".to_string(),
    };
    let cost = |cost: &anyagents_core::OutcomeCost| {
        if cost.turns_with_cost == 0 {
            "-".to_string()
        } else if cost.turns_with_cost == cost.turns {
            money(Some(cost.reported_cost_micros))
        } else {
            format!(
                "{} ({}/{} turns)",
                money(Some(cost.reported_cost_micros)),
                cost.turns_with_cost,
                cost.turns
            )
        }
    };
    let minutes = |ms: Option<i64>| ms.map_or("-".to_string(), |ms| format!("{}m", ms / 60_000));
    if crate::ui::is_pretty() {
        if report.groups.is_empty() {
            println!("  {}", crate::ui::dim("No outcomes recorded."));
        } else {
            println!("\n  {}\n", crate::ui::bold_cyan("Outcomes"));
            let mut table = crate::ui::Table::new(&[
                ("GROUP", false),
                ("AGENTS", true),
                ("SUCCEEDED", true),
                ("FAILED", true),
                ("ABANDONED", true),
                ("RECOVERED", true),
                ("COST", true),
                ("COST/SUCCESS", true),
                ("TO SUCCESS", true),
            ]);
            for group in &report.groups {
                table.add_row(vec![
                    crate::ui::bold(&outcome_label(group)),
                    group.agents.to_string(),
                    crate::ui::green(&group.succeeded.to_string()),
                    group.failed.to_string(),
                    group.abandoned.to_string(),
                    format!("{}/{}", group.recovered, group.reworked),
                    cost(&group.cost),
                    money(group.cost_per_success_micros),
                    minutes(group.median_ms_to_success),
                ]);
            }
            table.print();
        }
        println!(
            "  {} {}\n",
            crate::ui::dim("Unattributed usage:"),
            cost(&report.unattributed)
        );
        return;
    }
    for group in &report.groups {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            outcome_label(group),
            group.agents,
            group.succeeded,
            group.failed,
            group.abandoned,
            group.reworked,
            group.recovered,
            group.cost.turns,
            group.cost.turns_with_cost,
            group.cost.reported_cost_micros,
            group
                .cost_per_success_micros
                .map_or("-".to_string(), |micros| micros.to_string()),
            group
                .median_ms_to_success
                .map_or("-".to_string(), |ms| ms.to_string()),
        );
    }
    println!(
        "unattributed\t\t\t\t\t\t\t{}\t{}\t{}\t\t",
        report.unattributed.turns,
        report.unattributed.turns_with_cost,
        report.unattributed.reported_cost_micros
    );
}

/// Record a verdict on `agent`'s work through the server, which checks that
/// the caller is its ancestor, a root admin or the operator.
pub fn verdict_cmd(
    data: &Path,
    agent: &str,
    verdict: &str,
    reason: Option<&str>,
) -> Result<(), Error> {
    let clean_agent = agent.trim().strip_prefix("agent:").unwrap_or(agent.trim());
    if !client::can_call_server(data) {
        return Err(Error::Message(
            "recording a verdict needs the running server".into(),
        ));
    }
    let body = serde_json::json!({ "verdict": verdict, "reason": reason });
    match client::call::<anyagents_core::Outcome>(
        data,
        "POST",
        &format!("/v1/agents/{clean_agent}/verdict"),
        Some(body.to_string().into_bytes()),
        &client::authority(data),
    ) {
        Ok(Some(outcome)) => {
            println!(
                "{}\t{clean_agent}\t{}\t{}",
                outcome.id,
                outcome.kind.as_str(),
                outcome.actor
            );
            Ok(())
        }
        Ok(None) => Err(Error::Message("the server answered nothing".into())),
        Err(err) => Err(Error::Message(err)),
    }
}

/// Render a socket `UsageReport` in the historical table/TSV shape.
fn print_report(report: &anyagents_core::UsageReport) {
    let grouped = !matches!(report.group_by, anyagents_core::UsageGroupBy::Agent);
    if crate::ui::is_pretty() {
        if report.buckets.is_empty() {
            println!("  {}", crate::ui::dim("No usage records found."));
            return;
        }
        println!("\n  {}\n", crate::ui::bold_cyan("Token Usage"));
        let mut table = crate::ui::Table::new(&[
            ("AGENT", false),
            ("DAY", false),
            ("MODEL", false),
            ("HARNESS", false),
            ("INPUT", true),
            ("OUTPUT", true),
            ("CACHE READ", true),
            ("CACHE WRITE", true),
            ("COST", true),
        ]);
        for b in &report.buckets {
            let name = b
                .agent_name
                .as_deref()
                .unwrap_or_else(|| b.agent_id.as_str());
            let dim = b
                .day
                .as_deref()
                .or(b.model.as_deref())
                .or(b.harness.as_deref())
                .unwrap_or("all-time");
            let _ = grouped;
            let cost = match b.totals.reported_cost_micros {
                Some(micros) => crate::ui::green(&format!("${:.4}", micros as f64 / 1_000_000.0)),
                None => crate::ui::dim("-"),
            };
            table.add_row(vec![
                crate::ui::bold(name),
                crate::ui::dim(dim),
                crate::ui::dim(b.model.as_deref().unwrap_or("-")),
                crate::ui::dim(b.harness.as_deref().unwrap_or("-")),
                crate::ui::format_tokens(b.totals.input_tokens),
                crate::ui::format_tokens(b.totals.output_tokens),
                crate::ui::format_tokens(b.totals.cache_read_tokens),
                crate::ui::format_tokens(b.totals.cache_write_tokens),
                cost,
            ]);
        }
        table.print();
        println!();
        return;
    }
    for b in &report.buckets {
        let name = b
            .agent_name
            .as_deref()
            .unwrap_or_else(|| b.agent_id.as_str());
        let dim = b
            .day
            .as_deref()
            .or(b.model.as_deref())
            .or(b.harness.as_deref())
            .unwrap_or("-");
        let cost = match b.totals.reported_cost_micros {
            Some(micros) => micros.to_string(),
            None => "-".to_string(),
        };
        println!(
            "{name}\t{dim}\t{}\t{}\t{}\t{}\t{cost}",
            b.totals.input_tokens,
            b.totals.output_tokens,
            b.totals.cache_read_tokens,
            b.totals.cache_write_tokens,
        );
    }
}

#[cfg(test)]
mod archetype_tests {
    use super::*;

    #[test]
    fn archetype_offline_validation_refuses_before_persistence() {
        let _env = crate::server::tests::env_lock();
        let dir = tempfile::tempdir().unwrap();
        let templates = dir.path().join("archetypes");
        std::fs::create_dir_all(&templates).unwrap();
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
            std::fs::write(templates.join("broken.md"), raw).unwrap();
            let error = create(
                dir.path(),
                serde_json::json!({
                    "name": "rejected-template-agent", "archetype": "broken",
                }),
            )
            .unwrap_err();
            assert!(error.contains(expected), "{error}");
        }
        let error = create(
            dir.path(),
            serde_json::json!({
                "name": "rejected-template-agent", "scope": "invalid-request-scope",
            }),
        )
        .unwrap_err();
        assert!(
            error.contains("invalid scope `invalid-request-scope`"),
            "{error}"
        );
        assert!(open(dir.path()).unwrap().list_agents().unwrap().is_empty());
        assert!(!dir.path().join("agents").exists());
    }
}
