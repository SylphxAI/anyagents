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
            workdir,
            instructions,
            parent,
            prompt,
        } => {
            let body = serde_json::json!({
                "name": name,
                "harness": harness,
                "model": model,
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
        AgentsCommand::Wake { name } => {
            let view = set_state(data, &name, "running")?;
            println!("{}\t{}\t{}", view.id, view.name, view.state);
        }
        AgentsCommand::List { state, parent } => {
            list_filtered(data, state.as_deref(), parent.as_deref(), false)?
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn spawn(
    data: &Path,
    name: Option<String>,
    harness: Option<String>,
    model: Option<String>,
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
    let verb = if state == "archived" {
        "archive"
    } else {
        "wake"
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
        return answered.ok_or_else(|| Error::Message("the server answered nothing".to_string()));
    }
    let store = open(data)?;
    let found = store.agent_by_name(name)?;
    let agent = store.set_agent_state(&found.id, state)?;
    let lag = store.lag(&agent.id, anyagents_core::now_ms())?;
    let (mode, steer) = declared(&agent.harness);
    Ok(AgentView {
        id: agent.id.to_string(),
        name: agent.name,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        harness: agent.harness,
        model: agent.model,
        workdir: agent.workdir,
        state: agent.state,
        process: "-".to_string(),
        delivery_mode: mode_name(mode).to_string(),
        steer_support: steer_name(steer).to_string(),
        quota_reset_at_ms: None,
        oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
        install: "-".to_string(),
    })
}

/// `agents create`. The server owns it when it is there, so one process makes
/// the agent, mints its capability and starts it.
fn create(data: &Path, body: serde_json::Value) -> Result<AgentView, String> {
    if server::socket_path(data).exists() || client::agent_identity().is_some() {
        let answered = client::call::<AgentView>(
            data,
            "POST",
            "/v1/agents",
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        )?;
        return answered.ok_or_else(|| "the server answered nothing".to_string());
    }
    let parsed: crate::api::CreateAgentRequest =
        serde_json::from_value(body).map_err(|err| err.to_string())?;
    let store = open_with_key(data).map_err(|err| err.to_string())?;
    let parent_id = match parsed.parent.as_deref() {
        Some("root" | "") => None,
        Some(parent_ref) => Some(
            store
                .agent_by_name(parent_ref)
                .map_err(|err| err.to_string())?
                .id,
        ),
        None => None,
    };
    let harness = if parsed.harness.trim().is_empty() {
        "claude-code".to_string()
    } else {
        parsed.harness
    };
    let agent = crate::create::create(
        &store,
        data,
        crate::create::CreateRequest {
            name: parsed.name,
            harness,
            model: parsed.model,
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
    Ok(AgentView {
        id: agent.id.to_string(),
        name: agent.name,
        parent: agent.parent_id.as_ref().map(ToString::to_string),
        harness: agent.harness,
        model: agent.model,
        workdir: agent.workdir,
        state: agent.state,
        process: "-".to_string(),
        delivery_mode: mode_name(mode).to_string(),
        steer_support: steer_name(steer).to_string(),
        quota_reset_at_ms: None,
        oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
        install: "-".to_string(),
    })
}

fn set_admin(data: &Path, agent: &str, admin: bool) -> Result<(), Error> {
    let body = serde_json::json!({"admin": admin});
    let result = client::call::<serde_json::Value>(
        data,
        "POST",
        &format!("/v1/agents/{agent}/admin"),
        Some(body.to_string().into_bytes()),
        &client::authority(data),
    )
    .map_err(Error::Message)?;
    println!(
        "{}",
        result.ok_or_else(|| Error::Message("the server answered nothing".into()))?
    );
    Ok(())
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
    for agent in store.list_agents()? {
        if let Some(sf) = state_filter
            && sf != "all"
            && agent.state != sf
        {
            continue;
        }
        let lag = store.lag(&agent.id, now)?;
        let (mode, steer) = declared(&agent.harness);
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
                instructions: None,
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
    for total in sum_usage(&records, by_day) {
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
