//! `agents` and `groups`: the operator's view and changes, through the socket
//! the server owns when the boundary is in place, and through the store on a
//! single-user machine.

use std::path::Path;

use anyagents_core::{Agent, AgentId, Error, Store};
use anyagents_supervisor::{declared, mode_name, steer_name};

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
        AgentsCommand::Create {
            name,
            harness,
            model,
            profile,
            workdir,
            instructions,
            parent,
            prompt,
        } => {
            let body = serde_json::json!({
                "name": name,
                "harness": harness,
                "model": model,
                "profile": profile,
                "workdir": workdir.map(|path| path.display().to_string()),
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
        AgentsCommand::Wake { name } => {
            let view = set_state(data, &name, "running")?;
            println!("{}\t{}\t{}", view.id, view.name, view.state);
        }
        AgentsCommand::Delete { name } => {
            delete_cmd(data, &name)?;
        }
        AgentsCommand::List { state, parent } => {
            list_filtered(data, state.as_deref(), parent.as_deref(), false)?
        }
        AgentsCommand::Instructions {
            name,
            instructions,
            clear,
        } => {
            describe_cmd(data, Some(name), instructions, clear)?;
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
    clear: bool,
) -> Result<(), Error> {
    let caller_id = client::agent_identity().map(|id| id.to_string());
    let (target_name, new_desc) = match (agent_or_text, description) {
        (Some(a), Some(d)) => (a, Some(d)),
        (Some(a), None) => {
            if caller_id.is_some() && (a.contains(' ') || a.len() > 28) && !clear {
                ("self".to_string(), Some(a))
            } else {
                (a, None)
            }
        }
        (None, Some(d)) => (caller_id.unwrap_or_else(|| "self".to_string()), Some(d)),
        (None, None) => (caller_id.unwrap_or_else(|| "all".to_string()), None),
    };
    if client::can_call_server(data) {
        if target_name.eq_ignore_ascii_case("all") && new_desc.is_none() && !clear {
            if let Ok(store) = Store::open(data.join("state.db")) {
                crate::create::sync_all_agent_instructions(&store, data);
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
                        a.name,
                        a.harness,
                        a.state,
                        a.description.as_deref().unwrap_or("-")
                    );
                }
                return Ok(());
            }
        }
        if new_desc.is_some() || clear {
            let body = serde_json::json!({
                "description": new_desc,
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
                let d = val
                    .get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-");
                println!("updated\t{n}\t{d}");
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
                let d = val
                    .get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-");
                println!("{n}\t{d}");
                return Ok(());
            }
        }
    }
    let store = Store::open(data.join("state.db"))?;
    if target_name == "all" {
        crate::create::sync_all_agent_instructions(&store, data);
        println!("synced\tall");
        return Ok(());
    }
    let mut rec = store.agent_by_name(&target_name)?;
    if clear {
        rec = store.set_agent_instructions(&rec.id, None)?;
    } else if let Some(ref text) = new_desc {
        rec = store.set_agent_instructions(&rec.id, Some(text))?;
    }
    let cfg = anyagents_core::agent_config_dir(data, &rec.id);
    let work_dir = rec
        .workdir
        .as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| anyagents_core::agent_work_dir(data, &rec.id));
    let _ = crate::create::reconcile_agent_runtime_config(&store, data, &rec, &cfg, &work_dir);
    if new_desc.is_some() || clear {
        println!(
            "updated\t{}\t{}",
            rec.name,
            rec.instructions.as_deref().unwrap_or("-")
        );
    } else {
        println!(
            "{}\t{}",
            rec.name,
            rec.instructions.as_deref().unwrap_or("-")
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
    workdir: Option<std::path::PathBuf>,
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
        "workdir": workdir.map(|path| path.display().to_string()),
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
    let deleted = store.delete_agent(&found.id)?;
    let agent_dir = data.join("agents").join(deleted.id.as_str());
    let _ = std::fs::remove_dir_all(&agent_dir);
    println!("{}\t{}\tdeleted", deleted.id, deleted.name);
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

pub fn peers(data: &Path, state: Option<&str>, parent: Option<&str>) -> Result<(), Error> {
    list_filtered(data, state, parent, true)
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
        if state == "archived"
            && view.parent.is_some()
            && let Ok(id) = AgentId::parse(&view.id)
        {
            server::reap_subagent_ephemeral_dirs(data, &id);
        }
        return Ok(view);
    }
    let store = open(data)?;
    let found = store.agent_by_name(name)?;
    let agent = store.set_agent_state(&found.id, state)?;
    if state == "archived" && found.parent_id.is_some() {
        server::reap_subagent_ephemeral_dirs(data, &found.id);
    }
    let lag = store.lag(&agent.id, anyagents_core::now_ms())?;
    let (mode, steer) = declared(&agent.harness);
    let seat_info = crate::create::resolved_agent_seat_info(&store, &agent);
    Ok(AgentView {
        id: agent.id.to_string(),
        name: agent.name,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        harness: agent.harness,
        model: agent.model,
        workdir: agent.workdir,
        state: agent.state,
        process: "-".to_string(),
        turn_state: "idle".to_string(),
        delivery_mode: mode_name(mode).to_string(),
        steer_support: steer_name(steer).to_string(),
        quota_reset_at_ms: None,
        oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
        install: "-".to_string(),
        description: agent.instructions,
        seat_user: seat_info.as_ref().map(|(u, _, _, _)| u.clone()),
        seat_uid: seat_info.as_ref().map(|(_, uid, _, _)| *uid),
        seat_gid: seat_info.as_ref().map(|(_, _, gid, _)| *gid),
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
    let parsed: crate::api::CreateAgentRequest =
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
    let parent_id = parent_agent.as_ref().map(|p| p.id.clone());
    let (harness, model, profile) = crate::create::resolve_subagent_defaults(
        data,
        parent_agent.as_ref(),
        &parsed.harness,
        parsed.model,
        parsed.profile,
    );
    let agent = crate::create::create(
        &store,
        data,
        crate::create::CreateRequest {
            name: parsed.name,
            harness,
            model,
            profile,
            workdir: parsed.workdir.map(std::path::PathBuf::from),
            workdir_default: server::owner_workdir(),
            instructions: parsed.instructions,
            parent_id,
        },
    )
    .map_err(|err| err.to_string())?;
    // A create is a new instance: its capability is minted so its harness
    // starts with one.
    crate::operator::mint(&store, &agent.id).map_err(|err| err.to_string())?;
    if let Some(prompt) = parsed
        .prompt
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let _ = store.receive(&agent.id, anyagents_core::Incoming::new(prompt));
    }
    let lag = store
        .lag(&agent.id, anyagents_core::now_ms())
        .map_err(|err| err.to_string())?;
    let (mode, steer) = declared(&agent.harness);
    let seat_info = crate::create::resolved_agent_seat_info(&store, &agent);
    Ok(AgentView {
        id: agent.id.to_string(),
        name: agent.name,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        harness: agent.harness,
        model: agent.model,
        workdir: agent.workdir,
        state: agent.state,
        process: "-".to_string(),
        turn_state: "idle".to_string(),
        delivery_mode: mode_name(mode).to_string(),
        steer_support: steer_name(steer).to_string(),
        quota_reset_at_ms: None,
        oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
        install: "-".to_string(),
        description: agent.instructions,
        seat_user: seat_info.as_ref().map(|(u, _, _, _)| u.clone()),
        seat_uid: seat_info.as_ref().map(|(_, uid, _, _)| *uid),
        seat_gid: seat_info.as_ref().map(|(_, _, gid, _)| *gid),
    })
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
    store.set_agent_admin(&rec.id, admin)?;
    let _ = store.record_admin_audit(
        "operator",
        if admin {
            "agents.grant_admin"
        } else {
            "agents.revoke_admin"
        },
        &rec.name,
    );
    println!("{}", serde_json::json!({"agent": rec.name, "admin": admin}));
    Ok(())
}

fn is_agy_turn_settled_on_disk(config_dir: &Path, opened_at_ms: i64, now_ms: i64) -> bool {
    if now_ms.saturating_sub(opened_at_ms) < 45_000 {
        return false;
    }
    let conv_dir = config_dir
        .join(".gemini")
        .join("antigravity-cli")
        .join("conversations");
    let Ok(entries) = std::fs::read_dir(&conv_dir) else {
        return false;
    };
    let mut latest_ms: Option<i64> = None;
    for entry in entries.flatten() {
        if let Ok(meta) = entry.metadata()
            && let Ok(modified) = meta.modified()
            && let Ok(dur) = modified.duration_since(std::time::UNIX_EPOCH)
        {
            let ms = i64::try_from(dur.as_millis()).unwrap_or(0);
            latest_ms = Some(latest_ms.map_or(ms, |prev| prev.max(ms)));
        }
    }
    latest_ms.is_some_and(|ms| now_ms.saturating_sub(ms) > 45_000)
}

/// Enrich `AgentView` rows from the authoritative SQLite `turn_recovery` table
/// (`has_open_turn`).
pub fn reconcile_and_enrich_views(data: &Path, views: &mut [AgentView]) {
    let Ok(store) = open(data) else {
        return;
    };
    let now = anyagents_core::now_ms();
    for view in views.iter_mut() {
        let Ok(id) = AgentId::parse(&view.id) else {
            continue;
        };
        if (view.seat_user.is_none() || view.seat_uid.is_none() || view.seat_gid.is_none())
            && let Ok(agent_rec) = store.agent(&id)
            && let Some((u, uid, gid, _)) = crate::create::resolved_agent_seat_info(&store, &agent_rec)
        {
            view.seat_user = Some(u);
            view.seat_uid = Some(uid);
            view.seat_gid = Some(gid);
        }
        let cfg = anyagents_core::agent_config_dir(data, &id);
        if view.quota_reset_at_ms.is_none() && view.harness == "claude-code" {
            view.quota_reset_at_ms =
                anyagents_harness_claude::detect_session_quota_reset_ms(&cfg, now);
        }
        if view.quota_reset_at_ms.is_some_and(|ms| ms > now)
            && let Ok(agent_rec) = store.agent(&id)
            && crate::create::try_rotate_exhausted_profile(&store, data, &agent_rec, true).is_some()
        {
            view.quota_reset_at_ms = None;
        }
        if view.state == "running" && view.process == "alive" {
            let mut open_turn = store.has_open_turn(&id).unwrap_or(false);
            if open_turn && view.harness == "agy" && view.oldest_undelivered_age_ms.is_none() {
                let opened_at_ms = store
                    .open_turn(&id)
                    .ok()
                    .flatten()
                    .map(|o| o.opened_at_ms)
                    .unwrap_or(0);
                if is_agy_turn_settled_on_disk(&cfg, opened_at_ms, now) {
                    let _ = store.note_turn_completed(&id);
                    open_turn = false;
                    view.turn_state = "idle".to_string();
                }
            }
            if view.quota_reset_at_ms.is_some_and(|ms| ms > now) {
                view.turn_state = "idle".to_string();
            } else if open_turn && view.quota_reset_at_ms.is_none() {
                view.turn_state = "working".to_string();
            } else if view.turn_state.is_empty() {
                view.turn_state = "idle".to_string();
            }
        } else if view.turn_state.is_empty() {
            view.turn_state = "idle".to_string();
        }
    }
}

/// `agents list` / `peers`: the server's view when it is there, else the stored rows.
fn list_filtered(
    data: &Path,
    state_filter: Option<&str>,
    parent_filter: Option<&str>,
    show_parent: bool,
) -> Result<(), Error> {
    if client::can_call_server(data) {
        let mut params = Vec::new();
        if let Some(s) = state_filter.map(str::trim).filter(|s| !s.is_empty()) {
            params.push(format!("state={s}"));
        }
        if let Some(p) = parent_filter.map(str::trim).filter(|s| !s.is_empty()) {
            params.push(format!("parent={p}"));
        }
        let path = if params.is_empty() {
            "/v1/agents".to_string()
        } else {
            format!("/v1/agents?{}", params.join("&"))
        };
        let listed =
            client::call::<AgentsResponse>(data, "GET", &path, None, &client::authority(data))
                .map_err(Error::Message)?;
        if let Some(mut listed) = listed {
            reconcile_and_enrich_views(data, &mut listed.agents);
            if let Some(sf) = state_filter
                .map(str::trim)
                .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
            {
                listed.agents.retain(|a| a.state.eq_ignore_ascii_case(sf));
            }
            if let Some(pf) = parent_filter.map(str::trim).filter(|s| !s.is_empty()) {
                let cleaned = pf.strip_prefix("agent:").unwrap_or(pf);
                if cleaned.eq_ignore_ascii_case("root") || cleaned.eq_ignore_ascii_case("none") {
                    listed.agents.retain(|a| a.parent.is_none());
                } else if cleaned.eq_ignore_ascii_case("subagents")
                    || cleaned.eq_ignore_ascii_case("all")
                {
                    listed.agents.retain(|a| a.parent.is_some());
                } else {
                    let wanted_id = if cleaned.eq_ignore_ascii_case("self") {
                        client::agent_identity().map(|id| id.to_string())
                    } else {
                        listed
                            .agents
                            .iter()
                            .find(|a| a.name.eq_ignore_ascii_case(cleaned) || a.id == cleaned)
                            .map(|a| a.id.clone())
                            .or_else(|| Some(cleaned.to_string()))
                    };
                    if let Some(pid) = wanted_id {
                        listed
                            .agents
                            .retain(|a| a.parent.as_deref() == Some(pid.as_str()));
                    }
                }
            }
            let include_archived = state_filter.is_some_and(|sf| {
                let sf = sf.trim();
                sf.eq_ignore_ascii_case("all") || sf.eq_ignore_ascii_case("archived")
            });
            if crate::ui::is_pretty() {
                crate::ui::print_agents_table(&listed.agents, None, include_archived);
                return Ok(());
            }
            for agent in listed.agents {
                if show_parent {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        agent.id,
                        agent.name,
                        agent.parent.as_deref().unwrap_or("-"),
                        agent.harness,
                        agent.model.as_deref().unwrap_or("-"),
                        agent.state,
                        agent.process,
                        show_optional(agent.oldest_undelivered_age_ms),
                    );
                } else {
                    print_agent(AgentLine {
                        id: &agent.id,
                        name: &agent.name,
                        harness: &agent.harness,
                        state: &agent.state,
                        process: &agent.process,
                        mode: &agent.delivery_mode,
                        steer: &agent.steer_support,
                        undelivered: agent.oldest_undelivered_age_ms,
                        quota: agent.quota_reset_at_ms,
                    });
                }
            }
            return Ok(());
        }
    }
    let store = open(data)?;
    let now = anyagents_core::now_ms();
    let mut fallback_views = Vec::new();
    for agent in store.list_agents()? {
        if let Some(sf) = state_filter
            && sf != "all"
            && agent.state != sf
        {
            continue;
        }
        let lag = store.lag(&agent.id, now)?;
        let (mode, steer) = declared(&agent.harness);
        let seat_info = crate::create::resolved_agent_seat_info(&store, &agent);
        if crate::ui::is_pretty() {
            fallback_views.push(AgentView {
                id: agent.id.to_string(),
                name: agent.name,
                parent: agent.parent_id.as_ref().map(ToString::to_string),
                harness: agent.harness,
                model: agent.model,
                workdir: agent.workdir,
                state: agent.state,
                process: "down".to_string(),
                turn_state: "idle".to_string(),
                delivery_mode: mode_name(mode).to_string(),
                steer_support: steer_name(steer).to_string(),
                quota_reset_at_ms: None,
                oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
                install: "-".to_string(),
                description: agent.instructions,
                seat_user: seat_info.as_ref().map(|(u, _, _, _)| u.clone()),
                seat_uid: seat_info.as_ref().map(|(_, uid, _, _)| *uid),
                seat_gid: seat_info.as_ref().map(|(_, _, gid, _)| *gid),
            });
            continue;
        }
        if show_parent {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\tdown\t{}",
                agent.id,
                agent.name,
                agent
                    .parent_id
                    .as_ref()
                    .map(ToString::to_string)
                    .as_deref()
                    .unwrap_or("-"),
                agent.harness,
                agent.model.as_deref().unwrap_or("-"),
                agent.state,
                show_optional(lag.oldest_undelivered_age_ms),
            );
        } else {
            print_agent(AgentLine {
                id: agent.id.as_str(),
                name: &agent.name,
                harness: &agent.harness,
                state: &agent.state,
                process: "down",
                mode: mode_name(mode),
                steer: steer_name(steer),
                undelivered: lag.oldest_undelivered_age_ms,
                quota: None,
            });
        }
    }
    if crate::ui::is_pretty() {
        let include_archived = state_filter.is_some_and(|sf| {
            let sf = sf.trim();
            sf.eq_ignore_ascii_case("all") || sf.eq_ignore_ascii_case("archived")
        });
        crate::ui::print_agents_table(&fallback_views, None, include_archived);
    }
    Ok(())
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
    if client::can_call_server(data) {
        let listed = client::call::<AgentsResponse>(
            data,
            "GET",
            "/v1/agents",
            None,
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        if let Some(listed) = listed
            && let Some(found) = listed
                .agents
                .into_iter()
                .find(|row| row.name == agent || row.id == agent)
        {
            return Ok(Agent {
                is_admin: false,
                id: AgentId::parse(&found.id)?,
                name: found.name,
                parent_id: found.parent.and_then(|p| AgentId::parse(&p).ok()),
                harness: found.harness,
                model: found.model,
                workdir: found.workdir,
                instructions: found.description,
                state: found.state,
                created_at_ms: 0,
                launch_env: Vec::new(),
            });
        }
    }
    open(data)?.agent_by_name(agent)
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
    open(data)?.join_group(group, agent_id)
}

pub fn leave_group(data: &Path, group: &str, agent_id: &AgentId) -> Result<(), Error> {
    if client::can_call_server(data) {
        let body = serde_json::to_vec(&GroupActionRequest {
            action: "leave".to_string(),
            group: group.to_string(),
            agent: Some(agent_id.to_string()),
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
    open(data)?.leave_group(group, agent_id)
}

/// The agents in a group.
pub fn group_members(data: &Path, group: &str) -> Result<Vec<String>, Error> {
    if client::can_call_server(data) {
        let body = serde_json::to_vec(&GroupActionRequest {
            action: "members".to_string(),
            group: group.to_string(),
            agent: None,
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
    mode: &'a str,
    steer: &'a str,
    undelivered: Option<i64>,
    quota: Option<i64>,
}

fn print_agent(agent: AgentLine<'_>) {
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        agent.id,
        agent.name,
        agent.harness,
        agent.state,
        agent.process,
        agent.mode,
        agent.steer,
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
        };
        let mut harness = match crate::vendor::vendor_launch(request) {
            Ok(harness) => harness,
            Err(reason) => {
                println!("{}\tskipped\t{reason}", agent.name);
                continue;
            }
        };
        let mut flushed = 0u32;
        let mut failure = None;
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
                context: anyagents_harness::Context::default(),
            };
            match anyagents_harness::Harness::deliver(&mut harness, &outbound) {
                Ok(_) => {
                    store.note_delivered(&agent.id, message.seq)?;
                    flushed += 1;
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

/// Recorded usage, summed. The socket has no usage route yet, so this reads the
/// stored rows.
pub fn usage(data: &Path, agent: Option<&str>, by: Option<&str>) -> Result<(), UsageError> {
    use anyagents_core::sum_usage;
    use std::collections::HashMap;
    let by_day = match by {
        None => false,
        Some("day") => true,
        Some(other) => {
            return Err(UsageError::Refused(format!(
                "unknown usage grouping: {other}"
            )));
        }
    };
    let store = open(data)?;
    let agent = match agent {
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
