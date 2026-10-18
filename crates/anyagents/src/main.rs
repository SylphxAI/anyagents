//! Command line for the mailbox. The server and the MCP entry point come later;
//! until then those commands say so and exit non-zero.

use std::path::PathBuf;
use std::process::ExitCode;

use anyagents_core::{Address, Incoming, NewAgent, ReceiveOutcome, Store};
use clap::{Parser, Subcommand};

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
        Command::Serve => Err(anyagents_core::Error::NotImplemented("serve".into())),
        Command::Mcp { .. } => Err(anyagents_core::Error::NotImplemented("mcp".into())),
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
        Command::Usage { agent } => usage(&cli.data, agent.as_deref()),
    }
}

fn open(data: &std::path::Path) -> Result<Store, anyagents_core::Error> {
    Store::open(data.join("state.db"))
}

fn agents(data: &std::path::Path, action: AgentsCommand) -> Result<(), anyagents_core::Error> {
    let store = open(data)?;
    match action {
        AgentsCommand::Create {
            name,
            harness,
            model,
        } => {
            let mut new = NewAgent::new(name, harness);
            new.model = model;
            let agent = store.create_agent(new)?;
            println!("{}\t{}", agent.id, agent.name);
        }
        AgentsCommand::List => {
            for agent in store.list_agents()? {
                println!(
                    "{}\t{}\t{}\t{}",
                    agent.id, agent.name, agent.harness, agent.state
                );
            }
        }
    }
    Ok(())
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

fn usage(data: &std::path::Path, agent: Option<&str>) -> Result<(), anyagents_core::Error> {
    let store = open(data)?;
    let agent = match agent {
        Some(name) => Some(store.agent_by_name(name)?),
        None => None,
    };
    for record in store.list_usage(agent.as_ref().map(|agent| &agent.id))? {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            record.id,
            record.agent_id,
            record.harness,
            record.model.unwrap_or_default(),
            record.input_tokens,
            record.output_tokens,
            record.billing_mode,
            record.day
        );
    }
    Ok(())
}
