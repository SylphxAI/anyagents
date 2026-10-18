//! Command line, local server, and per-agent MCP entry point.

use std::path::PathBuf;
use std::process::ExitCode;

use std::collections::HashMap;
use std::path::Path;

use anyagents_core::{Address, Incoming, ReceiveOutcome, Store, sum_usage};
use anyagents_supervisor::{declared, mode_name, steer_name};
use clap::{Parser, Subcommand};

mod api;
mod create;
mod mcp;
mod mcp_config;
mod server;
mod vendor;

#[derive(Parser)]
#[command(
    name = "anyagents",
    version,
    about = "Mailbox and delivery for long-lived coding agents",
    arg_required_else_help = true
)]
struct Cli {
    /// Directory that holds the database.
    #[arg(long, global = true, default_value = ".anyagents")]
    data: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the server.
    Serve,
    /// Create and list agents.
    Agents {
        #[command(subcommand)]
        action: AgentsCommand,
    },
    /// Append a message to an agent's mailbox.
    Send {
        /// `agent:<name>`, a bare name, `group:<name>`, or a telegram target.
        to: String,
        text: String,
        #[arg(long)]
        source_key: Option<String>,
        #[arg(long)]
        source_time_ms: Option<i64>,
        #[arg(long)]
        reply_to: Option<String>,
        #[arg(long)]
        from: Option<String>,
    },
    /// Read an agent's mailbox.
    Inbox {
        agent: String,
        /// Exclusive sequence cursor. Omit to read from the start.
        #[arg(long)]
        since: Option<i64>,
    },
    /// List recorded usage.
    Usage {
        #[arg(long)]
        agent: Option<String>,
        /// Group sums. Only `day` is supported.
        #[arg(long)]
        by: Option<String>,
    },
    /// Speak MCP for one agent.
    Mcp {
        #[arg(long)]
        agent: String,
    },
}

#[derive(Subcommand)]
enum AgentsCommand {
    /// Record an agent. The harness is a catalog key.
    Create {
        name: String,
        #[arg(long, default_value = "claude-code")]
        harness: String,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        workdir: Option<PathBuf>,
        #[arg(long)]
        instructions: Option<String>,
    },
    /// List agents.
    List,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), anyagents_core::Error> {
    match cli.command {
        Command::Serve => server::run(&cli.data),
        Command::Mcp { agent } => mcp::run(&cli.data, &agent),
        Command::Agents { action } => agents(&cli.data, action),
        Command::Send {
            to,
            text,
            source_key,
            source_time_ms,
            reply_to,
            from,
        } => send(
            &cli.data,
            &to,
            text,
            source_key,
            source_time_ms,
            reply_to,
            from,
        ),
        Command::Inbox { agent, since } => inbox(&cli.data, &agent, since),
        Command::Usage { agent, by } => usage(&cli.data, agent.as_deref(), by.as_deref()),
    }
}

fn open(data: &std::path::Path) -> Result<Store, anyagents_core::Error> {
    Store::open(data.join("state.db"))
}

fn agents(data: &Path, action: AgentsCommand) -> Result<(), anyagents_core::Error> {
    match action {
        AgentsCommand::Create {
            name,
            harness,
            model,
            workdir,
            instructions,
        } => {
            let store = open(data)?;
            let agent = create::create(
                &store,
                data,
                create::CreateRequest {
                    name,
                    harness,
                    model,
                    workdir,
                    instructions,
                },
            )?;
            println!("{}\t{}", agent.id, agent.name);
        }
        AgentsCommand::List => list_agents(data)?,
    }
    Ok(())
}

fn list_agents(data: &Path) -> Result<(), anyagents_core::Error> {
    let socket = server::socket_path(data);
    if let Ok(listed) = mcp::get_json_sync::<api::AgentsResponse>(&socket, "GET", "/v1/agents") {
        for agent in listed.agents {
            print_agent(AgentLine {
                id: &agent.id,
                name: &agent.name,
                harness: &agent.harness,
                state: &agent.state,
                process: &agent.process,
                mode: &agent.delivery_mode,
                steer: &agent.steer_support,
                unaccepted: agent.oldest_unaccepted_age_ms,
                unprocessed: agent.oldest_unprocessed_age_ms,
                quota: agent.quota_reset_at_ms,
            });
        }
        return Ok(());
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
            unaccepted: lag.oldest_unaccepted_age_ms,
            unprocessed: lag.oldest_unprocessed_age_ms,
            quota: None,
        });
    }
    Ok(())
}

struct AgentLine<'a> {
    id: &'a str,
    name: &'a str,
    harness: &'a str,
    state: &'a str,
    process: &'a str,
    mode: &'a str,
    steer: &'a str,
    unaccepted: Option<i64>,
    unprocessed: Option<i64>,
    quota: Option<i64>,
}

fn print_agent(agent: AgentLine<'_>) {
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        agent.id,
        agent.name,
        agent.harness,
        agent.state,
        agent.process,
        agent.mode,
        agent.steer,
        show_optional(agent.unaccepted),
        show_optional(agent.unprocessed),
        show_optional(agent.quota),
    );
}

fn show_optional(value: Option<i64>) -> String {
    value
        .map(|number| number.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn send(
    data: &std::path::Path,
    to: &str,
    text: String,
    source_key: Option<String>,
    source_time_ms: Option<i64>,
    reply_to: Option<String>,
    from: Option<String>,
) -> Result<(), anyagents_core::Error> {
    let address = Address::parse(to)?;
    let name = match address {
        Address::Agent(name) => name,
        Address::Group(_) | Address::TelegramChat(_) | Address::TelegramUser(_) => {
            return Err(anyagents_core::Error::NotImplemented(
                "send to a group or a telegram target".into(),
            ));
        }
    };
    let store = open(data)?;
    let agent = store.agent_by_name(&name)?;
    let outcome = store.receive(
        &agent.id,
        Incoming {
            text,
            sender: from,
            source_time_ms,
            source_key,
            reply_to,
            received_at_ms: None,
        },
    )?;
    match outcome {
        ReceiveOutcome::New(message) => println!("{}\t{}", message.id, message.seq),
        ReceiveOutcome::Duplicate(message) => {
            println!("{}\t{}\tduplicate", message.id, message.seq);
        }
    }
    Ok(())
}

fn inbox(
    data: &std::path::Path,
    agent: &str,
    since: Option<i64>,
) -> Result<(), anyagents_core::Error> {
    let store = open(data)?;
    let agent = store.agent_by_name(agent)?;
    for message in store.inbox(&agent.id, since)? {
        println!(
            "{}\t{}\t{}\t{}",
            message.seq, message.ack, message.received_at_ms, message.text
        );
    }
    Ok(())
}

fn usage(data: &Path, agent: Option<&str>, by: Option<&str>) -> Result<(), anyagents_core::Error> {
    let by_day = match by {
        None => false,
        Some("day") => true,
        Some(other) => {
            return Err(anyagents_core::Error::Message(format!(
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
