//! `agents.create`. The agent row, its directories, and its MCP registration
//! are one step. The harness config is written only under the agent's own
//! config directory.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, NewAgent, Store, agent_config_dir, agent_work_dir};

use crate::{install, mcp_config, muse_config, opencode_config};

pub struct CreateRequest {
    pub name: String,
    pub harness: String,
    pub model: Option<String>,
    pub workdir: Option<PathBuf>,
    /// The working directory an owner agent gets when none is named.
    pub workdir_default: Option<PathBuf>,
    pub instructions: Option<String>,
}

pub fn create(store: &Store, data: &Path, request: CreateRequest) -> Result<Agent, Error> {
    // A harness is installed only if an adapter can start it.
    install::adapter(&request.harness)?;
    let mut new = NewAgent::new(request.name, request.harness);
    new.model = request.model;
    new.instructions = request.instructions;
    let agent = store.create_agent(new)?;
    let data = std::path::absolute(data)?;
    // Nothing is installed until an agent needs it: this is that moment. A
    // failure is the reason the agent's first turn cannot start, reported here
    // and never retried in a loop.
    install::record(&data, &agent.harness).map_err(Error::Message)?;
    // The gateway origin and this agent's own token, kept so a restart starts
    // the same agent the same way.
    let pairs = crate::gateway::agent_env(&data, &agent.id);
    store.set_agent_launch_env(&agent.id, &pairs)?;
    let managed = agent_work_dir(&data, &agent.id);
    std::fs::create_dir_all(&managed)?;
    let config = agent_config_dir(&data, &agent.id);
    std::fs::create_dir_all(&config)?;
    let cwd = match &request.workdir {
        Some(path) => {
            std::fs::create_dir_all(path)?;
            std::path::absolute(path)?
        }
        // Every owner agent hosted by AnyAgents works in one shared checkout,
        // so it loads the same company rules and skills as the owner sessions
        // the desk has always run. A caller can still name its own directory.
        None => match &request.workdir_default {
            Some(shared) => {
                std::fs::create_dir_all(shared)?;
                std::path::absolute(shared)?
            }
            None => std::path::absolute(&managed)?,
        },
    };
    let command = std::env::current_exe()?;
    // The tool server is pointed at the agent's own config directory, where its
    // capability is; a name on a command line is never an identity.
    let args = vec![
        "--data".to_string(),
        data.display().to_string(),
        "mcp".to_string(),
        "--config".to_string(),
        config.display().to_string(),
    ];
    if agent.harness == "codex" {
        mcp_config::write_codex(&config.join("config.toml"), &command, &args)?;
    } else if agent.harness == "opencode" {
        opencode_config::write(&config, agent.model.as_deref(), &command, &args)?;
    } else if agent.harness == "agy" {
        // The Antigravity CLI writes its own config here; the MCP server needs
        // the CLI's own `mcp add` consent flow, so the agent starts without it.
    } else if agent.harness == "muse" {
        muse_config::write(&config, agent.model.as_deref(), &command, &args)?;
        muse_config::point_vendor_home(&config)?;
    } else {
        mcp_config::write_claude(&config.join(".claude.json"), &command, &args)?;
    }
    let running = store.set_agent_running(&agent.id, &cwd.display().to_string())?;
    store.set_agent_launch_env(&agent.id, &pairs)?;
    Ok(running)
}
