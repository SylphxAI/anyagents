//! `agents` and `groups`: the operator's view and changes, through the socket
//! the server owns when the boundary is in place, and through the store on a
//! single-user machine.

use std::path::Path;

use anyagents_core::{Agent, AgentId, Error, Store};
use anyagents_supervisor::{declared, mode_name, steer_name};

use crate::api::{AgentView, AgentsResponse};
use crate::client::{self, Authority};
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
        AgentsCommand::Create {
            name,
            harness,
            model,
            workdir,
            instructions,
        } => {
            let body = serde_json::json!({
                "name": name,
                "harness": harness,
                "model": model,
                "workdir": workdir.map(|path| path.display().to_string()),
                "instructions": instructions,
            });
            let view = create(data, body).map_err(Error::Message)?;
            println!("{}\t{}", view.id, view.name);
        }
        AgentsCommand::List => list(data)?,
    }
    Ok(())
}

/// `agents create`. The server owns it when it is there, so one process makes
/// the agent, mints its capability and starts it.
fn create(data: &Path, body: serde_json::Value) -> Result<AgentView, String> {
    if server::socket_path(data).exists() {
        let answered = client::call::<AgentView>(
            data,
            "POST",
            "/v1/agents",
            Some(body.to_string().into_bytes()),
            &client::authority(),
        )?;
        return answered.ok_or_else(|| "the server answered nothing".to_string());
    }
    let parsed: crate::api::CreateAgentRequest =
        serde_json::from_value(body).map_err(|err| err.to_string())?;
    let store = open_with_key(data).map_err(|err| err.to_string())?;
    let agent = crate::create::create(
        &store,
        data,
        crate::create::CreateRequest {
            name: parsed.name,
            harness: parsed.harness,
            model: parsed.model,
            workdir: parsed.workdir.map(std::path::PathBuf::from),
            workdir_default: server::owner_workdir(),
            instructions: parsed.instructions,
        },
    )
    .map_err(|err| err.to_string())?;
    // A create is a new instance: its capability is minted so its harness
    // starts with one.
    crate::operator::mint(&store, &agent.id).map_err(|err| err.to_string())?;
    let lag = store
        .lag(&agent.id, anyagents_core::now_ms())
        .map_err(|err| err.to_string())?;
    let (mode, steer) = declared(&agent.harness);
    Ok(AgentView {
        id: agent.id.to_string(),
        name: agent.name,
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

/// `agents list`: the server's view when it is there, else the stored rows.
fn list(data: &Path) -> Result<(), Error> {
    if server::socket_path(data).exists() && crate::operator::operator_token().is_some() {
        let listed = client::call::<AgentsResponse>(
            data,
            "GET",
            "/v1/agents",
            None,
            &client::authority(),
        )
        .map_err(Error::Message)?;
        if let Some(listed) = listed {
            for agent in listed.agents {
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
            return Ok(());
        }
    }
    let store = open(data)?;
    let now = anyagents_core::now_ms();
    for agent in store.list_agents()? {
        let lag = store.lag(&agent.id, now)?;
        let (mode, steer) = declared(&agent.harness);
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
    if server::socket_path(data).exists() && crate::operator::operator_token().is_some() {
        let listed = client::call::<AgentsResponse>(
            data,
            "GET",
            "/v1/agents",
            None,
            &client::authority(),
        )
        .map_err(Error::Message)?;
        if let Some(listed) = listed
            && let Some(found) = listed.agents.into_iter().find(|row| row.name == agent)
        {
            return Ok(Agent {
                id: AgentId::parse(&found.id)?,
                name: found.name,
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

pub fn join_group(data: &Path, group: &str, agent_id: &AgentId) -> Result<(), Error> {
    open(data)?.join_group(group, agent_id)
}

pub fn leave_group(data: &Path, group: &str, agent_id: &AgentId) -> Result<(), Error> {
    open(data)?.leave_group(group, agent_id)
}

/// The agents in a group. The socket has no group route yet, so this reads the
/// stored rows; a deployment with the boundary keeps its database and its
/// operator on the same side.
pub fn group_members(data: &Path, group: &str) -> Result<Vec<String>, Error> {
    let store = open(data);
    match store {
        Ok(store) => store.group_members(group),
        Err(Error::Message(_)) => {
            let listed = client::call::<AgentsResponse>(
                data,
                "GET",
                "/v1/operator/groups/{group}/members",
                None,
                &Authority::Operator,
            )
            .map_err(Error::Message)?;
            Ok(listed
                .map(|listed| listed.agents.into_iter().map(|agent| agent.name).collect())
                .unwrap_or_default())
        }
        Err(other) => Err(other),
    }
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
