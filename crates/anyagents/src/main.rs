//! Command line, local server, and per-agent MCP entry point.

use std::path::PathBuf;
use std::process::ExitCode;

use std::collections::HashMap;
use std::path::Path;

use anyagents_core::{
    Address, CredentialStore, FileCredentialStore, Incoming, NewChannel, ReceiveOutcome, Store,
    sum_usage,
};
use anyagents_supervisor::{declared, mode_name, steer_name};
use clap::{Parser, Subcommand};

mod api;
mod create;
mod fake_opencode;
mod gateway;
mod harness_double;
mod install;
mod mcp;
mod mcp_config;
mod muse_config;
mod opencode_config;
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
    /// Body of the Antigravity steering hook: reads the CLI's event on stdin and
    /// writes the hook's answer on stdout. Reads the mailbox, prints no secret.
    #[command(hide = true)]
    AgyHook {
        #[arg(long)]
        agent: String,
        /// The conversation the CLI reports; an event from any other is ignored.
        #[arg(long, default_value = "")]
        conversation: String,
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
        Command::Mcp { agent } => mcp::run(&cli.data, &agent),
        Command::FakeOpencode => fake_opencode::run(),
        Command::Harness { config } => harness_double::run(&config),
        Command::AgyHook {
            agent,
            conversation,
        } => {
            agy_hook(&cli.data, &agent, &conversation);
            Ok(())
        }
        Command::Agents { action } => agents(&cli.data, action),
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
        Command::Channels { action } => channels(&cli.data, action),
        Command::Groups { action } => groups(&cli.data, action),
    }
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
    let store = open(data)?;
    let recorded = store.agent_by_name(agent)?;
    let data = std::path::absolute(data)?;
    gateway::store_key(&data, &recorded, base_url, model, key)?;
    println!("key stored\t{}\t{}", recorded.name, model);
    Ok(())
}

/// The Antigravity steering hook. The CLI runs it before every model call; it
/// answers with the mailbox messages the server has not written to this turn.
///
/// Everything it can fail to read answers `{}`, so a hook never breaks a turn.
/// The server is reached through its socket only; the store is not opened here.
fn agy_hook(data: &Path, agent: &str, conversation: &str) {
    use std::io::{Read, Write};
    let mut stdout = std::io::stdout().lock();
    let mut event = String::new();
    let _ = std::io::stdin().lock().read_to_string(&mut event);
    let event: serde_json::Value = serde_json::from_str(&event).unwrap_or(serde_json::Value::Null);
    if event
        .get("invocationNum")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
        == 0
    {
        let _ = writeln!(stdout, "{{}}");
        return;
    }
    if !conversation.is_empty()
        && event
            .get("conversationId")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| id != conversation)
    {
        let _ = writeln!(stdout, "{{}}");
        return;
    }
    let before = match mcp::get_json_sync::<api::InboxResponse>(
        &server::socket_path(data),
        "GET",
        &format!("/v1/agents/{agent}/inbox"),
    ) {
        Ok(value) => value,
        Err(_) => {
            let _ = writeln!(stdout, "{{}}");
            return;
        }
    };
    // The mailbox is in sequence order, oldest first. Only what the harness has
    // not taken counts: a message the server wrote is the turn's own prompt, and
    // the server moves its cursor when it writes it.
    let lines: Vec<api::MessageView> = before
        .messages
        .into_iter()
        .filter(|line| !line.accepted)
        .collect();
    if lines.is_empty() {
        let _ = writeln!(stdout, "{{}}");
        return;
    }
    let mut used =
        String::from("New messages arrived while you were working (delivered mid-turn):");
    for line in lines.iter().take(40) {
        used.push_str("\n\n");
        used.push_str(&anyagents_harness::marked_message(&line.id, &line.text));
    }
    let answer = serde_json::json!({"injectSteps": [{"userMessage": used}]});
    let _ = writeln!(stdout, "{answer}");
}

/// The installer's sign-in, run against the agent's own config directory. This
/// command reads no socket, so it works before the server is up and while it is
/// restarting a harness that is signed out.
fn sign_in(data: &Path, agent: &str) -> Result<(), anyagents_core::Error> {
    let mut runner = signin::ProcessRunner;
    let mut sink = signin::TerminalSink;
    let outcome = signin::sign_in_agent(data, agent, &mut runner, &mut sink)?;
    let harness = open(data)?.agent_by_name(agent)?.harness;
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
    Ok(())
}

fn channels(data: &Path, action: ChannelsCommand) -> Result<(), anyagents_core::Error> {
    match action {
        ChannelsCommand::Add {
            kind,
            agent,
            token_file,
        } => add_channel(data, &kind, &agent, &token_file),
        ChannelsCommand::List => list_channels(data),
    }
}

fn add_channel(
    data: &Path,
    kind: &str,
    agent_name: &str,
    token_file: &Path,
) -> Result<(), anyagents_core::Error> {
    if kind != "telegram" {
        return Err(anyagents_core::Error::Message(format!(
            "unsupported channel kind: {kind}"
        )));
    }
    let token = std::fs::read(token_file)?;
    let token = String::from_utf8(token)
        .map_err(|_| anyagents_core::Error::Message("token file is not text".into()))?;
    let token = token.trim();
    if token.is_empty() || token.chars().any(|ch| ch.is_whitespace() || ch == '/') {
        return Err(anyagents_core::Error::Message(
            "token is empty or contains whitespace or a slash".into(),
        ));
    }
    let store = open(data)?;
    let agent = store.agent_by_name(agent_name)?;
    let channel = store.create_channel(NewChannel {
        kind: kind.to_string(),
        identity: None,
        config: "{}".to_string(),
        credential_ref: None,
    })?;
    let credential_ref = format!("channels/{}", channel.id);
    FileCredentialStore::new(data).write(&credential_ref, token.as_bytes())?;
    store.set_credential_ref(&channel.id, &credential_ref)?;
    store.attach_channel(&channel.id, &agent.id)?;
    println!("{}\t{}", channel.id, channel.kind);
    Ok(())
}

fn groups(data: &Path, action: GroupsCommand) -> Result<(), anyagents_core::Error> {
    let store = open(data)?;
    match action {
        GroupsCommand::Join { group, agent } => {
            let agent = store.agent_by_name(&agent)?;
            store.join_group(&group, &agent.id)?;
            println!("{group}\t{}", agent.name);
        }
        GroupsCommand::Leave { group, agent } => {
            let agent = store.agent_by_name(&agent)?;
            store.leave_group(&group, &agent.id)?;
            println!("{group}\t{}", agent.name);
        }
        GroupsCommand::Members { group } => {
            for name in store.group_members(&group)? {
                println!("{name}");
            }
        }
    }
    Ok(())
}

fn list_channels(data: &Path) -> Result<(), anyagents_core::Error> {
    let store = open(data)?;
    for channel in store.list_channels()? {
        let names: Vec<String> = channel
            .agents
            .iter()
            .filter_map(|id| store.agent(id).ok().map(|agent| agent.name))
            .collect();
        println!(
            "{}\t{}\t{}\t{}",
            channel.id,
            channel.kind,
            channel.credential_ref.as_deref().unwrap_or("-"),
            names.join(",")
        );
    }
    Ok(())
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
                    workdir_default: server::owner_workdir(),
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
        Address::Group(room) => {
            let store = open(data)?;
            let delivered = store.receive_room(
                &room,
                Incoming {
                    text,
                    sender: from,
                    source_time_ms,
                    source_key,
                    reply_to,
                    received_at_ms: None,
                },
            )?;
            if delivered.is_empty() {
                return Err(anyagents_core::Error::Message(format!(
                    "room {room} has no agent to deliver to"
                )));
            }
            // One line per recipient: the message id and its sequence in that
            // agent's mailbox, so a caller can read each one back.
            for (agent, outcome) in delivered {
                let message = match outcome {
                    ReceiveOutcome::New(message) => message,
                    ReceiveOutcome::Duplicate(message) => message,
                };
                println!("{}\t{}\t{}", agent.name, message.id, message.seq);
            }
            return Ok(());
        }
        Address::TelegramChat { chat, thread } => {
            return send_telegram(
                data,
                &chat,
                thread.as_deref(),
                &text,
                reply_to.as_deref(),
                None,
                from.as_deref(),
            );
        }
        Address::TelegramUser { user, message } => {
            return send_telegram(
                data,
                &user,
                None,
                &text,
                reply_to.as_deref(),
                message.as_deref(),
                from.as_deref(),
            );
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

#[allow(clippy::too_many_arguments)]
fn send_telegram(
    data: &Path,
    chat: &str,
    thread: Option<&str>,
    text: &str,
    reply_to: Option<&str>,
    address_reply: Option<&str>,
    from: Option<&str>,
) -> Result<(), anyagents_core::Error> {
    let chat_id: i64 = chat
        .parse()
        .map_err(|_| anyagents_core::Error::Message("telegram chat id is not a number".into()))?;
    let thread_id = match thread {
        None => None,
        Some(raw) => Some(parse_id(raw, "telegram topic id")?),
    };
    let reply_to = match reply_to.or(address_reply) {
        None => None,
        Some(raw) => Some(parse_id(raw, "reply_to is not a telegram message id")?),
    };
    let store = open(data)?;
    let message_id = telegram::post_text(&store, data, from, chat_id, text, reply_to, thread_id)?;
    println!("{message_id}");
    Ok(())
}

fn parse_id(raw: &str, what: &'static str) -> Result<i64, anyagents_core::Error> {
    raw.parse()
        .map_err(|_| anyagents_core::Error::Message(what.into()))
}

fn inbox(
    data: &std::path::Path,
    agent: &str,
    since: Option<i64>,
) -> Result<(), anyagents_core::Error> {
    let store = open(data)?;
    let agent = store.agent_by_name(agent)?;
    for message in store.inbox(&agent.id, since)? {
        let ack = match &message.failed_reason {
            Some(reason) => format!("failed ({reason})"),
            None => message.ack.to_string(),
        };
        println!(
            "{}\t{}\t{}\t{}",
            message.seq, ack, message.received_at_ms, message.text
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
