//! The main command: parse, then run one verb.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod agent;
mod api;
mod auth;
mod channel;
mod client;
mod create;
mod data;
mod fake_opencode;
mod gateway;
mod harness_double;
mod install;
mod mcp;
mod mcp_config;
mod muse_config;
mod opencode_config;
mod operator;
mod reply;
mod sandbox;
mod server;
mod signin;
mod telegram;
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
    /// Sign an agent's harness in through the vendor's own command.
    SignIn {
        /// The agent name.
        agent: String,
    },
    /// Harness catalog: what can be signed in, and what installs it.
    Harnesses {
        #[command(subcommand)]
        action: HarnessesCommand,
    },
    /// Create and list agents.
    Agents {
        #[command(subcommand)]
        action: AgentsCommand,
    },
    /// Store the provider key an agent's model runs on (API key mode).
    Model {
        /// The agent name.
        agent: String,
        /// The provider origin, for example https://api.deepseek.com.
        #[arg(long = "base-url")]
        base_url: String,
        /// The model id the provider serves, for example deepseek-chat.
        #[arg(long)]
        model: String,
        /// A file holding the key, on its own line. The key is never an argument.
        #[arg(long = "key-file")]
        key_file: PathBuf,
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
        /// The agent this message is from. An agent's own route always sends as
        /// that agent.
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
    /// Speak MCP for one agent. Its capability comes from its config
    /// directory, never from a name.
    Mcp {
        /// The agent's own config directory. The server names it to every
        /// harness it starts, so a harness that omits it is read from there.
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Start one agent command inside the deployment's OS boundary, or refuse
    /// and name the admission it needs. Never starts it unisolated.
    AgentRun {
        /// The agent's own config directory.
        #[arg(long)]
        config: Option<PathBuf>,
        /// The command, after `--`.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// The deployment's boundary contract: what an agent may read, and what it
    /// may write.
    Boundary,
    /// Test double for a streaming vendor. It prints text and no result line,
    /// and records its argv and stdin when `ANYAGENTS_FAKE_LOG` names a file.
    #[command(hide = true)]
    FakeOpencode,
    /// Test double for a harness: obeys the adapter contract for the harness
    /// named in `<config>`, an agent's `adapter.json`.
    #[command(hide = true)]
    Harness {
        /// The `adapter.json` the agent was created with.
        config: PathBuf,
    },
    /// Put one agent's mailbox into its harness now, if the running server has
    /// not. Reads the store directly, so it also covers a server that was just
    /// upgraded and is still inside its restart backoff.
    Flush {
        /// The agent name, or `all`.
        agent: String,
    },
    /// Record a channel. The secret is copied into the credential store.
    Channels {
        #[command(subcommand)]
        action: ChannelsCommand,
    },
    /// Named groups of agents, and who is in them.
    Groups {
        #[command(subcommand)]
        action: GroupsCommand,
    },
}

#[derive(Subcommand)]
enum GroupsCommand {
    /// Add an agent to a named group, creating the group if it is new.
    Join { group: String, agent: String },
    /// Take an agent out of a named group.
    Leave { group: String, agent: String },
    /// List a named group's members, one per line.
    Members { group: String },
}

#[derive(Subcommand)]
enum HarnessesCommand {
    /// List the harnesses this client can sign in. Prints no credential.
    List,
}

#[derive(Subcommand)]
enum ChannelsCommand {
    /// Copy a token and record the channel that uses it.
    Add {
        /// Channel kind. Only `telegram` is supported.
        kind: String,
        #[arg(long)]
        agent: String,
        /// File holding the bot token. The token is not an argument.
        #[arg(long)]
        token_file: PathBuf,
    },
    /// List channels. No secret is printed.
    List,
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
        Command::SignIn { agent } => sign_in(&cli.data, &agent),
        Command::Harnesses { action } => match action {
            HarnessesCommand::List => {
                list_harnesses();
                Ok(())
            }
        },
        Command::Mcp { config } => mcp::run(&cli.data, config.as_deref()),
        Command::AgentRun { config, command } => {
            sandbox::run(&cli.data, config.as_deref(), &command)
        }
        Command::Boundary => print_boundary(&cli.data),
        Command::FakeOpencode => fake_opencode::run(),
        Command::Harness { config } => harness_double::run(&config),
        Command::Agents { action } => agent::agents(&cli.data, action),
        Command::Flush { agent } => agent::flush(&cli.data, &agent),
        Command::Model {
            agent,
            base_url,
            model,
            key_file,
        } => store_key(&cli.data, &agent, &base_url, &model, &key_file),
        Command::Send {
            to,
            text,
            source_key,
            source_time_ms,
            reply_to,
            from,
        } => channel::send(
            &cli.data,
            &to,
            text,
            source_key,
            source_time_ms,
            reply_to,
            from,
        ),
        Command::Inbox { agent, since } => channel::inbox(&cli.data, &agent, since),
        Command::Usage { agent, by } => agent::usage(&cli.data, agent.as_deref(), by.as_deref())
            .map_err(|err| match err {
                agent::UsageError::Store(err) => err,
                agent::UsageError::Refused(text) => anyagents_core::Error::Message(text),
            }),
        Command::Channels { action } => channel::channels(&cli.data, action),
        Command::Groups { action } => groups(&cli.data, action),
    }
}

/// Print the boundary contract this deployment wrote, or say it has none.
fn print_boundary(data: &Path) -> Result<(), anyagents_core::Error> {
    let boundary = sandbox::boundary_of(data)?;
    let text = serde_json::to_string_pretty(&boundary)
        .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
    println!("{text}");
    Ok(())
}

/// Print the catalog: id, the variable that points the vendor at its private
/// config directory, and the command that installs it. Never a credential.
fn list_harnesses() {
    for entry in anyagents_installer::builtin_catalog() {
        println!(
            "{}\t{}\t{}",
            entry.id,
            signin::config_variable(&entry.id).unwrap_or("-"),
            signin::install_hint(&entry.id).unwrap_or_else(|| "-".to_string())
        );
    }
}

/// Store an agent's provider key. The key is read from a file, kept 0600 under
/// the agent's directory, and never printed. The next turn then runs through the
/// server's gateway on that provider.
fn store_key(
    data: &Path,
    agent: &str,
    base_url: &str,
    model: &str,
    key_file: &Path,
) -> Result<(), anyagents_core::Error> {
    let raw = std::fs::read_to_string(key_file)?;
    let key = raw.trim();
    if key.is_empty() {
        return Err(anyagents_core::Error::Message(format!(
            "{} holds no key",
            key_file.display()
        )));
    }
    if key.contains(char::is_whitespace) {
        return Err(anyagents_core::Error::Message(format!(
            "{} does not hold one key on its own line",
            key_file.display()
        )));
    }
    let recorded = agent::named(data, agent)?;
    let data = std::path::absolute(data)?;
    gateway::store_key(&data, &recorded, base_url, model, key)?;
    println!("key stored\t{}\t{}", recorded.name, model);
    Ok(())
}

/// The installer's sign-in, run against the agent's own config directory. This
/// command reads no socket, so it works before the server is up and while it is
/// restarting a harness that is signed out.
fn sign_in(data: &Path, agent: &str) -> Result<(), anyagents_core::Error> {
    let mut runner = signin::ProcessRunner;
    let mut sink = signin::TerminalSink;
    let outcome = signin::sign_in_agent(data, agent, &mut runner, &mut sink)?;
    let harness = agent::named(data, agent)?.harness;
    if !outcome.signed_in {
        let hint = signin::install_hint(&harness).unwrap_or_else(|| "-".to_string());
        return Err(anyagents_core::Error::Message(format!(
            "{agent} is not signed in: the vendor command finished without reporting a sign-in (install: {hint})"
        )));
    }
    println!(
        "signed in\t{agent}\t{harness}\t{}",
        outcome.account.as_deref().unwrap_or("-")
    );
    // A sign-in is a new instance: the server gives the agent a fresh
    // capability and retires the last one.
    let _ = client::call::<serde_json::Value>(
        data,
        "POST",
        &format!("/v1/operator/agents/{agent}/capability"),
        Some(b"{}".to_vec()),
        &client::Authority::Operator,
    );
    Ok(())
}

fn groups(data: &Path, action: GroupsCommand) -> Result<(), anyagents_core::Error> {
    match action {
        GroupsCommand::Join { group, agent } => {
            let agent = agent::named(data, &agent)?;
            agent::join_group(data, &group, &agent.id)?;
            println!("{group}\t{}", agent.name);
        }
        GroupsCommand::Leave { group, agent } => {
            let agent = agent::named(data, &agent)?;
            agent::leave_group(data, &group, &agent.id)?;
            println!("{group}\t{}", agent.name);
        }
        GroupsCommand::Members { group } => {
            for name in agent::group_members(data, &group)? {
                println!("{name}");
            }
        }
    }
    Ok(())
}
