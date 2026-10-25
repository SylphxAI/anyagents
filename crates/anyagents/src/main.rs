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
mod dsh;
mod fake_opencode;
mod gateway;
mod grok;
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
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run the server.
    Serve,
    /// Inspect server health, boundary/token state, installed harnesses, master profiles, and agents.
    Status {
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Sign an agent's harness or a server master profile in through the vendor's own command.
    SignIn {
        /// Agent name or harness ID (`claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`).
        agent: String,
        /// Optional master profile name (e.g. `acct-1`, `acct-2`, `default`).
        #[arg(long)]
        profile: Option<String>,
    },
    /// Harness catalog: what can be signed in, what installs it, and master profiles.
    Harnesses {
        #[command(subcommand)]
        action: HarnessesCommand,
    },
    /// Upstream provider profile pool (Subscriptions, Coding Plans, and API Keys) shared across harnesses.
    Profiles {
        /// Optional provider or harness filter (e.g. `sylphx`, `anthropic`, `openai`, `google-antigravity`, `qwen-token-plan`).
        filter: Option<String>,
    },
    /// Create and list agents.
    Agents {
        #[command(subcommand)]
        action: AgentsCommand,
    },
    /// List all agents and sub-agents with their parent, harness, model, state, and process health.
    Peers {
        /// Optional state filter: `running`, `archived`, or `all`.
        #[arg(long)]
        state: Option<String>,
        /// Optional parent filter: agent name, `agt_...` id, `self`, or `root`.
        #[arg(long)]
        parent: Option<String>,
    },
    /// Spawn a new agent or sub-agent and optionally deliver an initial task prompt.
    Spawn {
        /// Optional agent name (`[a-z0-9][a-z0-9_-]*`). Auto-generated when omitted.
        name: Option<String>,
        #[arg(long)]
        harness: Option<String>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        workdir: Option<PathBuf>,
        #[arg(long)]
        instructions: Option<String>,
        #[arg(long)]
        parent: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
    },
    /// Archive an agent (`state = archived`), scaling its process to zero while keeping its session.
    Archive {
        /// Agent name or `agt_...` id (defaults to this agent when run inside an agent).
        name: Option<String>,
    },
    /// Wake an archived agent (`state = running`) and resume its session.
    Wake {
        /// Agent name or `agt_...` id.
        name: String,
    },
    /// Resume an archived agent (`state = running`) and optionally send a task prompt.
    Resume {
        /// Agent name or `agt_...` id.
        name: String,
        /// Optional task/message to deliver immediately after waking.
        #[arg(long)]
        prompt: Option<String>,
    },
    /// Store the provider key an agent's model runs on (API key mode).
    Model {
        /// The agent name.
        agent: String,
        /// The provider origin, for example https://api.deepseek.com.
        #[arg(long = "base-url")]
        base_url: Option<String>,
        /// The model id the provider serves, for example deepseek-chat.
        #[arg(long)]
        model: String,
        /// A file holding the key, on its own line. The key is never an argument.
        #[arg(long = "key-file")]
        key_file: Option<PathBuf>,
        /// Existing provider/profile to bind instead of an API key.
        #[arg(long)]
        profile: Option<String>,
    },
    /// Send a message, media file, GPS location/venue, or poll to one or more agents, groups, or channels.
    Send {
        /// Target(s): `agent:<name>`, bare name, comma-separated names (`sage,forge`), `parent`, `children`, `group:<name>`, `all`, or `telegram:chat:<id>[:<topic>]`.
        to: String,
        /// Message text or caption (optional when sending media, location, or poll).
        #[arg(default_value = "")]
        text: String,
        #[arg(long)]
        source_key: Option<String>,
        #[arg(long)]
        source_time_ms: Option<i64>,
        #[arg(long)]
        reply_to: Option<String>,
        /// The agent or operator this message is from.
        #[arg(long)]
        from: Option<String>,
        /// Optional quoted excerpt from the message being replied to.
        #[arg(long)]
        quote: Option<String>,
        /// Attach a photo by local file path, URL, or file_id.
        #[arg(long)]
        photo: Option<String>,
        /// Attach a video by local file path, URL, or file_id.
        #[arg(long)]
        video: Option<String>,
        /// Attach an audio file by local file path, URL, or file_id.
        #[arg(long)]
        audio: Option<String>,
        /// Attach a music track by local file path, URL, or file_id.
        #[arg(long)]
        music: Option<String>,
        /// Optional music/audio track title.
        #[arg(long = "music-title")]
        music_title: Option<String>,
        /// Optional music/audio artist or performer name.
        #[arg(long = "music-performer")]
        music_performer: Option<String>,
        /// Attach a voice note by local file path, URL, or file_id.
        #[arg(long)]
        voice: Option<String>,
        /// Attach a document by local file path, URL, or file_id.
        #[arg(long)]
        document: Option<String>,
        /// Explicit media kind (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`).
        #[arg(long = "media-kind")]
        media_kind: Option<String>,
        /// Explicit media source path, URL, or file_id.
        #[arg(long)]
        media: Option<String>,
        /// GPS latitude in degrees.
        #[arg(long)]
        lat: Option<f64>,
        /// GPS longitude in degrees.
        #[arg(long)]
        lon: Option<f64>,
        /// Optional venue title for GPS location.
        #[arg(long = "venue-title")]
        venue_title: Option<String>,
        /// Optional venue address for GPS location.
        #[arg(long = "venue-address")]
        venue_address: Option<String>,
        /// Interactive poll question.
        #[arg(long = "poll-question")]
        poll_question: Option<String>,
        /// Interactive poll option (specify 2 to 10 times).
        #[arg(long = "poll-option")]
        poll_options: Vec<String>,
        /// Whether the poll is anonymous.
        #[arg(long = "poll-anonymous")]
        poll_anonymous: Option<bool>,
        /// Whether the poll allows multiple answers.
        #[arg(long = "poll-multiple")]
        poll_multiple: Option<bool>,
        /// Interactive checklist title.
        #[arg(long = "checklist-title")]
        checklist_title: Option<String>,
        /// Interactive checklist task item (specify 1 to 30 times).
        #[arg(long = "checklist-task")]
        checklist_tasks: Vec<String>,
        /// Article title (published as a Telegraph Instant View article when `--article-content` is given).
        #[arg(long = "article-title")]
        article_title: Option<String>,
        /// Article Markdown/text content.
        #[arg(long = "article-content")]
        article_content: Option<String>,
        /// Optional existing article URL.
        #[arg(long = "article-url")]
        article_url: Option<String>,
        /// Optional article author name.
        #[arg(long = "article-author")]
        article_author: Option<String>,
    },
    /// Perform an IM channel or message interaction (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`).
    Interact {
        /// Action: `react`, `vote`, `edit`, `delete`, `pin`, `unpin`, or `stop_poll`.
        action: String,
        /// Target message: `anyagents:reply:msg_...`, `msg_...`, or `telegram:chat:<chat_id>:<message_id>`.
        target: String,
        /// Emoji reaction for `react` (e.g. `👍`, `🔥`, `❤️`).
        #[arg(long)]
        emoji: Option<String>,
        /// Poll option(s) for `vote` (1-based index `1`, `2` or option text).
        #[arg(long = "option")]
        options: Vec<String>,
        /// New text for `edit` or comment for `vote`.
        #[arg(long)]
        text: Option<String>,
        /// Acting agent name (required when called as operator).
        #[arg(long)]
        from: Option<String>,
    },
    /// Read an agent's incoming mailbox.
    Inbox {
        /// Agent name or `agt_...` id (defaults to this agent when run inside an agent).
        agent: Option<String>,
        /// Exclusive sequence cursor. Omit to read from the start.
        #[arg(long)]
        since: Option<i64>,
        /// Maximum number of messages to return.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Read any agent's full two-way (`in` + `out`) conversation across users, channels, groups, and peers.
    Conversation {
        /// Agent name or `agt_...` id (defaults to this agent when run inside an agent).
        agent: Option<String>,
        /// Optional peer filter (e.g. `alice`, `agent:beta`, `group:crew`, `telegram:chat:-1001234567890`).
        #[arg(long)]
        with: Option<String>,
        /// Exclusive timestamp cursor in milliseconds.
        #[arg(long = "since-ms")]
        since_ms: Option<i64>,
        /// Maximum number of turns to return (default 50).
        #[arg(long)]
        limit: Option<usize>,
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
    Join {
        group: String,
        /// Agent name or `agt_...` id (defaults to this agent when run inside an agent).
        agent: Option<String>,
    },
    /// Take an agent out of a named group.
    Leave {
        group: String,
        /// Agent name or `agt_...` id (defaults to this agent when run inside an agent).
        agent: Option<String>,
    },
    /// List a named group's members, one per line.
    Members { group: String },
    /// List all named groups and their members.
    List,
}

#[derive(Subcommand)]
enum HarnessesCommand {
    /// List the harnesses this client can sign in. Prints no credential.
    List,
    /// List master account profiles stored on the server for each harness.
    Profiles {
        /// Optional harness ID filter.
        harness: Option<String>,
    },
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
    /// Grant administrative authority to a root agent (operator only).
    GrantAdmin { agent: String },
    /// Revoke administrative authority immediately (operator only).
    RevokeAdmin { agent: String },
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
        #[arg(long)]
        parent: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
    },
    /// Archive an agent (`state = archived`), scaling its process to zero while keeping its session.
    Archive { name: String },
    /// Wake and resume an archived agent (`state = running`).
    Wake { name: String },
    /// List agents.
    List {
        /// Optional state filter: `running`, `archived`, or `all`.
        #[arg(long)]
        state: Option<String>,
        /// Optional parent filter: agent name, `agt_...` id, `self`, or `root`.
        #[arg(long)]
        parent: Option<String>,
    },
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

fn resolve_data_dir(raw: PathBuf, _is_serve: bool) -> PathBuf {
    if raw == Path::new(".anyagents")
        && !raw.exists()
        && let Some(env_dir) = data::data_dir()
    {
        return env_dir;
    }
    raw
}

fn run(cli: Cli) -> Result<(), anyagents_core::Error> {
    let data = resolve_data_dir(cli.data, matches!(cli.command, Command::Serve));
    match cli.command {
        Command::Serve => server::run(&data),
        Command::Status { json } => status_cmd(&data, json),
        Command::SignIn { agent, profile } => sign_in(&data, &agent, profile.as_deref()),
        Command::Harnesses { action } => match action {
            HarnessesCommand::List => {
                list_harnesses();
                Ok(())
            }
            HarnessesCommand::Profiles { harness } => list_profiles_cmd(&data, harness.as_deref()),
        },
        Command::Profiles { filter } => list_profiles_cmd(&data, filter.as_deref()),
        Command::Mcp { config } => mcp::run(&data, config.as_deref()),
        Command::AgentRun { config, command } => sandbox::run(&data, config.as_deref(), &command),
        Command::Boundary => print_boundary(&data),
        Command::FakeOpencode => fake_opencode::run(),
        Command::Harness { config } => harness_double::run(&config),
        Command::Agents { action } => agent::agents(&data, action),
        Command::Peers { state, parent } => {
            agent::peers(&data, state.as_deref(), parent.as_deref())
        }
        Command::Spawn {
            name,
            harness,
            model,
            workdir,
            instructions,
            parent,
            prompt,
        } => agent::spawn(
            &data,
            name,
            harness,
            model,
            workdir,
            instructions,
            parent,
            prompt,
        ),
        Command::Archive { name } => agent::archive_cmd(&data, name),
        Command::Wake { name } => agent::resume_cmd(&data, &name, None),
        Command::Resume { name, prompt } => agent::resume_cmd(&data, &name, prompt),
        Command::Flush { agent } => agent::flush(&data, &agent),
        Command::Model {
            agent,
            base_url,
            model,
            key_file,
            profile,
        } => model_cmd(
            &data,
            &agent,
            base_url.as_deref(),
            &model,
            key_file.as_deref(),
            profile.as_deref(),
        ),
        Command::Send {
            to,
            text,
            source_key,
            source_time_ms,
            reply_to,
            from,
            quote,
            photo,
            video,
            audio,
            music,
            music_title,
            music_performer,
            voice,
            document,
            media_kind,
            media,
            lat,
            lon,
            venue_title,
            venue_address,
            poll_question,
            poll_options,
            poll_anonymous,
            poll_multiple,
            checklist_title,
            checklist_tasks,
            article_title,
            article_content,
            article_url,
            article_author,
        } => {
            let resolved_media = if let (Some(kind), Some(source)) = (media_kind, media) {
                Some(api::SendMedia {
                    kind,
                    source,
                    title: music_title,
                    performer: music_performer,
                })
            } else if let Some(source) = photo {
                Some(api::SendMedia {
                    kind: "photo".to_string(),
                    source,
                    title: None,
                    performer: None,
                })
            } else if let Some(source) = video {
                Some(api::SendMedia {
                    kind: "video".to_string(),
                    source,
                    title: None,
                    performer: None,
                })
            } else if let Some(source) = music {
                Some(api::SendMedia {
                    kind: "music".to_string(),
                    source,
                    title: music_title,
                    performer: music_performer,
                })
            } else if let Some(source) = audio {
                Some(api::SendMedia {
                    kind: "audio".to_string(),
                    source,
                    title: music_title,
                    performer: music_performer,
                })
            } else if let Some(source) = voice {
                Some(api::SendMedia {
                    kind: "voice".to_string(),
                    source,
                    title: None,
                    performer: None,
                })
            } else {
                document.map(|source| api::SendMedia {
                    kind: "document".to_string(),
                    source,
                    title: None,
                    performer: None,
                })
            };
            let resolved_location = match (lat, lon) {
                (Some(latitude), Some(longitude)) => Some(api::SendLocation {
                    latitude,
                    longitude,
                    title: venue_title,
                    address: venue_address,
                }),
                _ => None,
            };
            let resolved_poll = poll_question.map(|question| api::SendPoll {
                question,
                options: poll_options,
                is_anonymous: poll_anonymous,
                allows_multiple_answers: poll_multiple,
            });
            let resolved_checklist = checklist_title.map(|title| api::SendChecklist {
                title,
                tasks: checklist_tasks,
                business_connection_id: None,
            });
            let resolved_article = article_title.map(|title| api::SendArticle {
                title,
                content: article_content,
                url: article_url,
                author_name: article_author,
            });
            channel::send_with_options(
                &data,
                channel::SendOptions {
                    to,
                    text,
                    source_key,
                    source_time_ms,
                    reply_to,
                    from,
                    quote,
                    media: resolved_media,
                    location: resolved_location,
                    poll: resolved_poll,
                    checklist: resolved_checklist,
                    article: resolved_article,
                },
            )
        }
        Command::Interact {
            action,
            target,
            emoji,
            options,
            text,
            from,
        } => interact_cmd(&data, action, target, emoji, options, text, from),
        Command::Inbox {
            agent,
            since,
            limit,
        } => channel::inbox(&data, agent.as_deref(), since, limit),
        Command::Conversation {
            agent,
            with,
            since_ms,
            limit,
        } => channel::conversation(&data, agent.as_deref(), with.as_deref(), since_ms, limit),
        Command::Usage { agent, by } => agent::usage(&data, agent.as_deref(), by.as_deref())
            .map_err(|err| match err {
                agent::UsageError::Store(err) => err,
                agent::UsageError::Refused(text) => anyagents_core::Error::Message(text),
            }),
        Command::Channels { action } => channel::channels(&data, action),
        Command::Groups { action } => groups(&data, action),
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

fn model_cmd(
    data: &Path,
    agent: &str,
    base_url: Option<&str>,
    model: &str,
    key_file: Option<&Path>,
    profile: Option<&str>,
) -> Result<(), anyagents_core::Error> {
    use anyagents_core::Error;
    if profile.is_some() && (key_file.is_some() || base_url.is_some()) {
        return Err(Error::Message(
            "profile cannot be combined with a key or base URL".into(),
        ));
    }
    if client::can_call_server(data) || client::agent_identity().is_some() {
        let key = key_file.map(std::fs::read_to_string).transpose()?;
        let body = serde_json::json!({"model": model, "base_url": base_url, "key": key, "profile": profile});
        let result = client::call::<serde_json::Value>(
            data,
            "POST",
            &format!("/v1/agents/{agent}/model"),
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        )
        .map_err(Error::Message)?;
        println!(
            "{}",
            result.ok_or_else(|| Error::Message("the server answered nothing".into()))?
        );
        return Ok(());
    }
    if let Some(profile) = profile {
        let store = agent::open(data)?;
        return create::bind_profile(&store, &store.agent_by_name(agent)?, profile, model);
    }
    store_key(
        data,
        agent,
        base_url.ok_or_else(|| Error::Message("base URL is required".into()))?,
        model,
        key_file.ok_or_else(|| Error::Message("key file is required".into()))?,
    )
}

/// The installer's sign-in, run against either a server master profile
/// (`<data>/profiles/<harness>/<profile>`) when `target` is a harness catalog
/// ID or `profile` is specified, or against a recorded agent's own config directory.
fn sign_in(data: &Path, target: &str, profile: Option<&str>) -> Result<(), anyagents_core::Error> {
    let mut runner = signin::ProcessRunner;
    let mut sink = signin::TerminalSink;
    if signin::signable(target) {
        let prof = profile
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("default");
        let provider = create::upstream_provider_of(target, None);
        let dir = data.join("profiles").join(&provider).join(prof);
        std::fs::create_dir_all(&dir)?;
        let outcome = anyagents_installer::sign_in_at(
            &anyagents_installer::builtin_catalog(),
            &dir,
            target,
            &mut runner,
            &mut sink,
        )
        .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
        println!(
            "signed in\tprofile:{provider}/{prof}\t{}\t{}",
            dir.display(),
            outcome.account.as_deref().unwrap_or("-")
        );
        return Ok(());
    }
    if let Some(prof) = profile.map(str::trim).filter(|s| !s.is_empty()) {
        let rec = agent::named(data, target)?;
        let cfg = anyagents_core::agent_config_dir(data, &rec.id);
        std::fs::create_dir_all(&cfg)?;
        let _ = std::fs::write(cfg.join(".anyagents-profile"), format!("{prof}\n"));
    }
    let outcome = signin::sign_in_agent(data, target, &mut runner, &mut sink)?;
    let harness = agent::named(data, target)?.harness;
    if !outcome.signed_in {
        let hint = signin::install_hint(&harness).unwrap_or_else(|| "-".to_string());
        return Err(anyagents_core::Error::Message(format!(
            "{target} is not signed in: the vendor command finished without reporting a sign-in (install: {hint})"
        )));
    }
    println!(
        "signed in\t{target}\t{harness}\t{}",
        outcome.account.as_deref().unwrap_or("-")
    );
    // A sign-in is a new instance: the server gives the agent a fresh
    // capability and retires the last one.
    let _ = client::call::<serde_json::Value>(
        data,
        "POST",
        &format!("/v1/operator/agents/{target}/capability"),
        Some(b"{}".to_vec()),
        &client::Authority::Operator,
    );
    Ok(())
}

fn list_profiles_cmd(data: &Path, filter: Option<&str>) -> Result<(), anyagents_core::Error> {
    let entries = if client::can_call_server(data) || client::agent_identity().is_some() {
        client::call::<Vec<create::ProviderProfileEntry>>(
            data,
            "GET",
            "/v1/profiles",
            None,
            &client::authority(data),
        )
        .map_err(anyagents_core::Error::Message)?
        .ok_or_else(|| anyagents_core::Error::Message("the server answered nothing".into()))?
    } else {
        let store = anyagents_core::Store::open(data.join("state.db"))?;
        create::sync_and_list_provider_profiles(&store, data)
    };
    println!(
        "{:<20}\t{:<10}\t{:<13}\t{:<44}\tAGENTS",
        "PROVIDER", "PROFILE", "KIND", "COMPATIBLE HARNESSES"
    );
    for e in entries {
        if let Some(f) = filter {
            let f_trimmed = f.trim();
            if e.provider != f_trimmed && !e.compatible_harnesses.iter().any(|h| h == f_trimmed) {
                continue;
            }
        }
        let harnesses = e.compatible_harnesses.join(",");
        let agents = if e.assigned_agents.is_empty() {
            "-".to_string()
        } else {
            e.assigned_agents.join(",")
        };
        println!(
            "{:<20}\t{:<10}\t{:<13}\t{:<44}\t{}",
            e.provider, e.profile, e.kind, harnesses, agents
        );
    }
    Ok(())
}

fn interact_cmd(
    data: &Path,
    action: String,
    target: String,
    emoji: Option<String>,
    options: Vec<String>,
    text: Option<String>,
    from: Option<String>,
) -> Result<(), anyagents_core::Error> {
    let authority = client::authority(data);
    let sender = match from.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
        Some(s) => Some(if s.starts_with("agent:") {
            s
        } else {
            format!("agent:{s}")
        }),
        None => client::agent_identity().map(|id| format!("agent:{id}")),
    };
    let body = api::InteractRequest {
        sender,
        action,
        target,
        emoji,
        options,
        text,
    };
    let payload =
        serde_json::to_vec(&body).map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
    let resp: api::InteractResponse =
        client::call(data, "POST", "/v1/interact", Some(payload), &authority)
            .map_err(anyagents_core::Error::Message)?
            .ok_or_else(|| anyagents_core::Error::Message("empty interact response".into()))?;
    println!(
        "{}\t{}\t{}",
        resp.action,
        resp.target,
        resp.detail.as_deref().unwrap_or("ok")
    );
    Ok(())
}

fn status_cmd(data: &Path, json_out: bool) -> Result<(), anyagents_core::Error> {
    let sock = server::socket_path(data);
    let authority = client::authority(data);
    let live_agents =
        client::call::<api::AgentsResponse>(data, "GET", "/v1/agents", None, &authority)
            .ok()
            .flatten();
    let server_up = live_agents.is_some();
    let webhook_addr = std::fs::read_to_string(data.join("webhook.addr"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let boundary = sandbox::boundary_of(data).ok();
    let has_operator_token = operator::operator_token(data).is_some();

    let provider_profiles = anyagents_core::Store::open(data.join("state.db"))
        .ok()
        .map(|s| create::sync_and_list_provider_profiles(&s, data))
        .unwrap_or_default();

    let mut harness_rows = Vec::new();
    for entry in anyagents_installer::builtin_catalog() {
        let bin_path = install::program(data, &entry.id);
        let present = bin_path.is_file() || anyagents_installer::machine_has(&entry.id);
        let (mode, steer) = anyagents_supervisor::declared(&entry.id);
        let profiles: Vec<String> = provider_profiles
            .iter()
            .filter(|p| p.compatible_harnesses.iter().any(|h| h == &entry.id))
            .map(|p| format!("{}/{}", p.provider, p.profile))
            .collect();
        harness_rows.push(serde_json::json!({
            "id": entry.id,
            "installed": present,
            "binary": bin_path.display().to_string(),
            "delivery_mode": anyagents_supervisor::mode_name(mode),
            "steer_support": anyagents_supervisor::steer_name(steer),
            "profiles": profiles,
        }));
    }

    let agents_list = match live_agents {
        Some(ref resp) => resp.agents.clone(),
        None => {
            let mut fallback = Vec::new();
            if let Ok(store) = anyagents_core::Store::open(data.join("state.db"))
                && let Ok(rows) = store.list_agents()
            {
                for a in rows {
                    let (mode, steer) = anyagents_supervisor::declared(&a.harness);
                    fallback.push(api::AgentView {
                        id: a.id.to_string(),
                        name: a.name,
                        parent: a.parent_id.map(|p| p.to_string()),
                        harness: a.harness,
                        model: a.model,
                        workdir: a.workdir,
                        state: a.state,
                        process: "offline".to_string(),
                        delivery_mode: anyagents_supervisor::mode_name(mode).to_string(),
                        steer_support: anyagents_supervisor::steer_name(steer).to_string(),
                        quota_reset_at_ms: None,
                        oldest_undelivered_age_ms: None,
                        install: "recorded".to_string(),
                    });
                }
            }
            fallback
        }
    };

    if json_out {
        let doc = serde_json::json!({
            "server": if server_up { "running" } else { "stopped" },
            "socket": sock.display().to_string(),
            "webhook_addr": webhook_addr,
            "operator_token_present": has_operator_token,
            "boundary": boundary,
            "harnesses": harness_rows,
            "agents": agents_list,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&doc)
                .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
        );
        return Ok(());
    }

    println!(
        "server\t{}\tsocket={}\twebhook={}\toperator_token={}",
        if server_up { "running" } else { "stopped" },
        sock.display(),
        webhook_addr.as_deref().unwrap_or("-"),
        if has_operator_token {
            "ready"
        } else {
            "missing"
        }
    );
    println!("harnesses:");
    for h in &harness_rows {
        let id = h["id"].as_str().unwrap_or("-");
        let inst = if h["installed"].as_bool() == Some(true) {
            "ready"
        } else {
            "on-demand"
        };
        let mode = h["delivery_mode"].as_str().unwrap_or("-");
        let steer = h["steer_support"].as_str().unwrap_or("-");
        let profs = h["profiles"]
            .as_array()
            .map(|arr| {
                let names: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
                if names.is_empty() {
                    "auto".to_string()
                } else {
                    names.join(",")
                }
            })
            .unwrap_or_else(|| "auto".to_string());
        println!("  {id}\t{inst}\tdelivery={mode}\tsteer={steer}\tprofiles={profs}");
    }
    let running_count = agents_list.iter().filter(|a| a.state == "running").count();
    let archived_count = agents_list.iter().filter(|a| a.state == "archived").count();
    println!(
        "agents\ttotal={}\trunning={running_count}\tarchived={archived_count}",
        agents_list.len()
    );
    for a in &agents_list {
        let aid = anyagents_core::AgentId::parse(&a.id).ok();
        let prof = aid
            .as_ref()
            .and_then(|id| create::bound_profile_name(&anyagents_core::agent_config_dir(data, id)))
            .unwrap_or_else(|| "default".to_string());
        println!(
            "  {}\t{}\tharness={}\tprofile={prof}\tstate={}\tprocess={}\tmode={}",
            a.name, a.id, a.harness, a.state, a.process, a.delivery_mode
        );
    }
    Ok(())
}

fn groups(data: &Path, action: GroupsCommand) -> Result<(), anyagents_core::Error> {
    match action {
        GroupsCommand::Join { group, agent } => {
            let who = match agent
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
            {
                Some(a) => a,
                None => client::agent_identity()
                    .map(|id| id.to_string())
                    .ok_or_else(|| {
                        anyagents_core::Error::Message("groups join needs an agent name".into())
                    })?,
            };
            let agent = agent::named(data, &who)?;
            agent::join_group(data, &group, &agent.id)?;
            println!("{group}\t{}", agent.name);
        }
        GroupsCommand::Leave { group, agent } => {
            let who = match agent
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
            {
                Some(a) => a,
                None => client::agent_identity()
                    .map(|id| id.to_string())
                    .ok_or_else(|| {
                        anyagents_core::Error::Message("groups leave needs an agent name".into())
                    })?,
            };
            let agent = agent::named(data, &who)?;
            agent::leave_group(data, &group, &agent.id)?;
            println!("{group}\t{}", agent.name);
        }
        GroupsCommand::Members { group } => {
            for name in agent::group_members(data, &group)? {
                println!("{name}");
            }
        }
        GroupsCommand::List => {
            for (group, members) in agent::list_groups(data)? {
                println!("{group}\t{}", members.join(","));
            }
        }
    }
    Ok(())
}
