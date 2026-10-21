//! `agents.create`. The agent row, its directories, and its MCP registration
//! are one step. The harness config is written only under the agent's own
//! config directory.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, NewAgent, Store, agent_config_dir, agent_work_dir};

use crate::{agy_config, install, mcp_config, muse_config, opencode_config};

/// The executable script the Antigravity hook runs: this binary's hook
/// command, for this agent, in this data directory.
fn agy_hook_script(command: &Path, args: &[String], agent: &str) -> String {
    // `--data <path> mcp --agent <name>`: the data directory follows `--data`.
    let data = args
        .iter()
        .position(|arg| arg == "--data")
        .and_then(|at| args.get(at + 1))
        .cloned()
        .unwrap_or_default();
    format!(
        "#!/bin/sh\nexec {} --data {} agy-hook --agent {}\n",
        shell_quote(&command.display().to_string()),
        shell_quote(&data),
        shell_quote(agent)
    )
}

fn shell_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

pub struct CreateRequest {
    pub name: String,
    pub harness: String,
    pub model: Option<String>,
    pub workdir: Option<PathBuf>,
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
        None => std::path::absolute(&managed)?,
    };
    let command = std::env::current_exe()?;
    let args = vec![
        "--data".to_string(),
        data.display().to_string(),
        "mcp".to_string(),
        "--agent".to_string(),
        agent.name.clone(),
    ];
    if agent.harness == "codex" {
        mcp_config::write_codex(&config.join("config.toml"), &command, &args)?;
    } else if agent.harness == "opencode" {
        opencode_config::write(&config, agent.model.as_deref(), &command, &args)?;
    } else if agent.harness == "agy" {
        // The steering hook is the one thing this client can write. The MCP
        // server needs the CLI's own `mcp add` consent flow, so the agent
        // starts without it rather than failing to start.
        let hook = agy_config::hook_path(&config);
        std::fs::write(&hook, agy_hook_script(&command, &args, &agent.name))?;
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700))?;
        }
        // The adapter registers it where `agy` reads it, and serves its queue.
        anyagents_harness_agy::write_settings(&config, &hook)
            .map_err(|err| Error::Message(err.to_string()))?;
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
