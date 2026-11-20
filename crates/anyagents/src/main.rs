//! The main command: parse, then run one verb.

#[cfg(all(target_os = "linux", not(target_env = "musl")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyagents_core::Error;
use clap::{Args, Parser, Subcommand};

mod agent;
mod agent_file;
mod api;
mod auth;
mod auth_device;
mod channel;
mod client;
mod control_cli;
mod create;
mod data;
mod fake_opencode;
mod gateway;
mod harness_double;
mod host_memory;
mod identity;
mod install;
mod keeper;
mod mcp;
mod mcp_config;
mod memory_service;
mod muse_config;
mod opencode_config;
mod operator;
mod outbound;
mod remote;
mod reply;
mod repo;
mod runtime_cli;
mod sandbox;
mod sandbox_exec;
mod sandbox_providers;
mod schedule;
mod seat_tree;
mod server;
mod serverless_cloud;
mod signin;
mod spiron;
mod telegram;
mod timer;
mod ui;
mod vendor;

#[derive(Parser)]
#[command(
    name = "anyagents",
    version,
    about = "Manage agents, accounts, and automations"
)]
struct Cli {
    /// Directory that holds the database.
    #[arg(long, global = true, default_value = ".anyagents")]
    data: PathBuf,
    /// External PostgreSQL database URL (`postgres://...`); also reads `ANYAGENTS_DATABASE_URL`.
    #[arg(long = "database-url", global = true)]
    database_url: Option<String>,
    /// Show storage paths and additional diagnostic metadata.
    #[arg(long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run the server.
    Serve {
        /// TCP port for the HTTP listener (overrides `ANYAGENTS_PORT` and `PORT`).
        #[arg(long)]
        port: Option<u16>,
    },
    /// See server and fleet health (also the default).
    #[command(display_order = 1)]
    Status {
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Authenticate via RFC 8628 OIDC Device Flow (`auth login|status|logout`) or manage provider keys.
    #[command(display_order = 3)]
    Auth {
        #[command(subcommand)]
        action: Option<control_cli::AuthCommand>,
    },
    /// Sign an agent's harness or a server master profile in through the vendor's own command.
    #[command(hide = true, aliases = ["signin", "login"])]
    SignIn {
        /// Agent name, `<provider>/<profile>`, or harness ID (`spiron`, `claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`).
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
    /// Manage upstream provider profiles and 5h + Weekly quotas (`list [--fresh]`, `native-signin`, `set`).
    Profiles(control_cli::ProfilesCliArgs),
    /// Manage write-only deployment secrets (`list`, `set`, `grant`, `revoke`, `delete`).
    Secrets(control_cli::SecretsCliArgs),
    /// List or manage agents.
    #[command(display_order = 2, visible_alias = "agent")]
    Agents {
        #[command(subcommand)]
        action: Option<AgentsCommand>,
    },
    /// List agents with optional state and tree filters.
    #[command(name = "ls", aliases = ["peers", "ps"], display_order = 5)]
    Peers {
        /// Optional state filter: `running`, `archived`, or `all`.
        #[arg(short = 's', long)]
        state: Option<String>,
        /// Optional parent filter: agent name, `agt_...` id, `self`, or `root`.
        #[arg(short = 'p', long)]
        parent: Option<String>,
        /// Expand full multi-level sub-agent tree (`L1 -> L2 -> L3+`).
        #[arg(short = 't', long)]
        tree: bool,
        /// Maximum sub-agent tree depth to expand (`0` = roots only, `1` = L1, `2` = L2, ...).
        #[arg(short = 'd', long)]
        depth: Option<usize>,
        /// Show wide table including `agt_...` IDs without terminal truncation.
        #[arg(short = 'w', long)]
        wide: bool,
    },
    /// List sub-agents or configure default sub-agent harness/model/profile (`anyagents subagents default ...`).
    Subagents {
        /// Optional action (`default`, `list`) or parent agent name (`self`, `all`, `<agent>`).
        parent: Option<String>,
        /// Optional state filter (`running`, `paused`, `archived`, or `all`).
        #[arg(short = 's', long)]
        state: Option<String>,
        /// Expand full multi-level sub-agent tree (`L1 -> L2 -> L3+`).
        #[arg(short = 't', long)]
        tree: bool,
        /// Maximum sub-agent tree depth to expand (`0` = roots only, `1` = L1, `2` = L2, ...).
        #[arg(short = 'd', long)]
        depth: Option<usize>,
        /// Show wide table including `agt_...` IDs without terminal truncation.
        #[arg(short = 'w', long)]
        wide: bool,
        /// Clear the deployment fallback (legacy alias for --clear).
        #[arg(long)]
        inherit: bool,
        /// Set the deployment fallback harness.
        #[arg(long)]
        harness: Option<String>,
        /// Set the deployment fallback model.
        #[arg(long)]
        model: Option<String>,
        /// Set the deployment fallback profile.
        #[arg(long)]
        profile: Option<String>,
        /// Legacy parent name, supported only with --clear to delete a retired override.
        #[arg(long)]
        agent: Option<String>,
        /// Clear the deployment fallback or delete a retired per-agent override.
        #[arg(long)]
        clear: bool,
    },
    /// Spawn a new agent or sub-agent and optionally deliver an initial task prompt.
    Spawn(runtime_cli::SpawnCliArgs),
    /// Archive an agent (`state = archived`), scaling its process to zero while keeping its session.
    Archive {
        /// Agent name or `agt_...` id (defaults to this agent when run inside an agent).
        name: Option<String>,
    },
    /// Pause an agent (`state = paused`), stopping its process, channel polling, and group wakes until resumed.
    Pause {
        /// Agent name or `agt_...` id (defaults to this agent when run inside an agent).
        name: Option<String>,
    },
    /// Wake an archived or paused agent (`state = running`) and resume its session.
    Wake {
        /// Agent name or `agt_...` id.
        name: String,
        /// Optional task/message to deliver immediately after waking.
        #[arg(long)]
        prompt: Option<String>,
    },
    /// Resume an archived or paused agent (`state = running`) and optionally send a task prompt.
    Resume {
        /// Agent name or `agt_...` id.
        #[arg(required_unless_present = "faulted")]
        name: Option<String>,
        /// Optional task/message to deliver immediately after waking.
        #[arg(long)]
        prompt: Option<String>,
        /// Resume every agent parked as `faulted` (its own session's errors
        /// paused it), once its cause is fixed. Operator only.
        #[arg(long, conflicts_with_all = ["name", "prompt"])]
        faulted: bool,
    },
    /// Permanently delete an agent and its on-disk directory.
    Delete {
        /// Agent name or `agt_...` id.
        name: String,
    },
    /// Store the provider key an agent's model runs on (API key mode), bind a named provider profile, or switch its harness.
    Model {
        /// The agent name.
        agent: String,
        /// Optional positional model ID (or pass `--model <id>`).
        target_model: Option<String>,
        /// The provider origin, for example https://api.deepseek.com.
        #[arg(long = "base-url")]
        base_url: Option<String>,
        /// The model id the provider serves, for example deepseek-chat.
        #[arg(long)]
        model: Option<String>,
        /// Optional target harness ID (`codex`, `opencode`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`).
        #[arg(long)]
        harness: Option<String>,
        /// A file holding the key, on its own line. The key is never an argument.
        #[arg(long = "key-file")]
        key_file: Option<PathBuf>,
        /// Existing provider/profile to bind instead of an API key.
        #[arg(long)]
        profile: Option<String>,
    },
    /// Send a message, media file, GPS location/venue, or poll to one or more agents, groups, or channels.
    Send(Box<SendArgs>),
    /// Perform an IM channel or message interaction (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`).
    #[command(hide = true)]
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
    #[command(visible_alias = "mail")]
    Inbox(InboxArgs),
    /// Read, search, or follow an agent's full conversation.
    #[command(name = "logs", aliases = ["conversation", "messages", "msg", "conv", "history", "tail"], display_order = 4)]
    Conversation(ConversationArgs),
    /// Audited erasure (conversation.redact): ancestors/admin only; target must be quiescent.
    #[command(hide = true)]
    Redact {
        agent: String,
        /// Inbound/outbound/tool id or transcript:<zero-based-line>.
        #[arg(long, required_unless_present = "seq", conflicts_with = "seq")]
        message_id: Option<String>,
        /// Mailbox sequence (not a transcript line).
        #[arg(long)]
        seq: Option<i64>,
        /// End-exclusive span start; omit the span to redact the whole body.
        #[arg(long, requires_all = ["end", "unit"])]
        start: Option<usize>,
        #[arg(long, requires_all = ["start", "unit"])]
        end: Option<usize>,
        #[arg(long, value_parser = ["bytes", "chars"], requires_all = ["start", "end"])]
        unit: Option<String>,
        /// Audit reason; must not contain the sensitive text.
        #[arg(long)]
        reason: String,
    },
    /// Zero-Embedding Lexical + Structured search across historical and live conversations.
    Search(SearchArgs),
    /// List recorded usage (`--group-by agent|day|model|harness|profile`).
    Usage(control_cli::UsageCliArgs),
    /// Speak MCP for one agent. Its capability comes from its config
    /// directory, never from a name.
    #[command(hide = true)]
    Mcp {
        /// The agent's own config directory. The server names it to every
        /// harness it starts, so a harness that omits it is read from there.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Ignored legacy MCP tool profile flag retained for config backward compatibility.
        #[arg(long = "profile", hide = true)]
        _profile: Option<String>,
    },
    /// Start one agent command inside the deployment's OS boundary, or refuse
    /// and name the admission it needs. Never starts it unisolated.
    #[command(hide = true)]
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
    #[command(hide = true)]
    Boundary,
    /// The keepers that hold harness processes across server restarts.
    Keepers {
        #[command(subcommand)]
        action: KeepersCommand,
    },
    /// Test double for a streaming vendor. It prints text and no result line,
    /// and records its argv and stdin when `ANYAGENTS_FAKE_LOG` names a file.
    #[command(hide = true)]
    FakeOpencode,
    /// Switch an existing agent's harness while preserving its identity, model, profile, workdir, history, and lifecycle state (or run the hidden test double when given a single `adapter.json` path).
    #[command(hide = true)]
    Harness {
        /// Agent name (`owner-anymd`) or `agt_...` ID (or `adapter.json` path for the test double).
        config: PathBuf,
        /// Target harness ID (`codex`, `opencode`, `claude-code`, `agy`, `spiron`, `muse`, `omp`).
        harness: Option<String>,
        /// Optional target harness ID flag (`--harness codex`).
        #[arg(long = "harness")]
        harness_flag: Option<String>,
        /// Optional model override (retains existing model when omitted).
        #[arg(long)]
        model: Option<String>,
        /// Optional `<provider>/<profile>` override (retains existing bound profile when omitted).
        #[arg(long)]
        profile: Option<String>,
    },
    /// Put one agent's mailbox into its harness now, if the running server has
    /// not. Reads the store directly, so it also covers a server that was just
    /// upgraded and is still inside its restart backoff.
    #[command(hide = true)]
    Flush {
        /// The agent name, or `all`.
        agent: String,
    },
    /// List the messages an agent's harness refused for good, or put one
    /// back in its mailbox as a new message.
    Requeue {
        /// The agent name.
        agent: String,
        /// A parked message id (`msg_...`). Without it, list the parked ones.
        message: Option<String>,
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
    /// View or update an agent's role & responsibility description (`anyagents describe [agent] [description]`).
    Describe {
        /// Target agent name (`self`, `all`, `reviewer`, `planner`, ...) or description text when updating `self`.
        agent: Option<String>,
        /// New role/responsibility description (syncs into the agent's private system prompt and group roster).
        description: Option<String>,
        /// Optional detailed private standing instructions (`instructions` / `SYSTEM_PROMPT.md`).
        #[arg(long, visible_alias = "instruction", visible_alias = "system-prompt")]
        instructions: Option<String>,
        /// Optional display-only role label; no effect on lifecycle or scope.
        #[arg(long)]
        role: Option<String>,
        /// Clear custom description.
        #[arg(long)]
        clear: bool,
    },
    /// Rename an agent in place while preserving its `agt_...` ID, history, mailbox, grants, seat, and Telegram binding, and keeping the old name as a live alias.
    Rename {
        /// Current agent name or `agt_...` ID.
        agent: String,
        /// New agent name (`[a-z0-9][a-z0-9_-]*`).
        new_name: String,
    },
    /// Set or clear an agent's short role label (`anyagents role <agent> <role>`).
    Role {
        /// Target agent name or `agt_...` ID.
        agent: String,
        /// Display-only role label; no effect on lifecycle or scope.
        role: Option<String>,
        /// Clear the role label.
        #[arg(long)]
        clear: bool,
    },
    /// List, create, or cancel scheduled wakes.
    #[command(name = "schedules", aliases = ["schedule", "cron"], display_order = 6)]
    Schedule(Box<ScheduleArgs>),
    /// List visible fleet automations (event watches and timed wakes).
    Automations {
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Sent messages whose delivery could not be confirmed (a timeout, a
    /// dropped connection, a post cut short). None is resent automatically:
    /// list them, send one again, or settle it without sending. Operator only.
    Outbound {
        #[command(subcommand)]
        action: Option<OutboundCommand>,
        /// Output machine-readable JSON.
        #[arg(long, global = true)]
        json: bool,
    },
    /// Inspect an agent's live state, schedules, quota/block status, and recent conversation turns.
    Inspect {
        /// Agent name or `agt_...` ID (defaults to `self` inside an agent).
        agent: Option<String>,
        /// Number of recent turns to include (default 10).
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: usize,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Run a local CLI harness inside a remote sandbox (`POST /v1/sandboxes/{id}/exec`) speaking `stream-json`.
    #[command(name = "sandbox-exec", hide = true)]
    SandboxExec(sandbox_exec::SandboxExecArgs),
    /// Dense retrieval and reranking over raw history, as its own process.
    #[command(name = "memory-service")]
    MemoryService(memory_service::MemoryServiceArgs),
    /// Database maintenance and PostgreSQL migration commands.
    Db {
        #[command(subcommand)]
        action: Box<DbCommand>,
    },
}

#[derive(Args)]
struct SendArgs {
    /// Target(s): `agent:<name>`, bare name, comma-separated names (`planner,builder`), `parent`, `children`, `group:<name>`, `all`, `operator`, or `telegram:chat:<id>[:<topic>]`.
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
}

#[derive(Args)]
struct ScheduleArgs {
    /// List visible schedules across all agents (read-only).
    #[arg(long, conflicts_with_all = ["agent", "at", "every", "watch", "prompt", "cancel"])]
    all: bool,
    /// Optional target agent name or `agt_...` id (defaults to this agent when run inside an agent).
    agent: Option<String>,
    /// One-off wake time (`1m`, `30s`, `2026-10-29T16:30:00Z`).
    #[arg(long)]
    at: Option<String>,
    /// Recurring wake interval (`1m`, `5m`, `15m`, `1h`).
    #[arg(long)]
    every: Option<String>,
    /// Event watch (`webhook:<topic>`, `event:<topic>`, `agent:idle:<name_or_id>`, `pid:<pid>`, `file:<path>`).
    #[arg(long)]
    watch: Option<String>,
    /// Prompt delivered when the schedule or event watch fires.
    #[arg(long)]
    prompt: Option<String>,
    /// Cancel schedule by `sch_...` ID or `all`.
    #[arg(long)]
    cancel: Option<String>,
    /// Output machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Subcommand)]
enum DbCommand {
    /// Migrate local SQLite databases (`state.db`, `memory-index.db`, `host-history-index-v1.db`) into PostgreSQL.
    #[command(name = "migrate-to-postgres")]
    MigrateToPostgres {
        /// Source AnyAgents data directory containing SQLite databases (defaults to `--data`).
        #[arg(long)]
        source: Option<PathBuf>,
        /// Target PostgreSQL URL (defaults to `--database-url` / `ANYAGENTS_DATABASE_URL`).
        #[arg(long)]
        target: Option<String>,
        /// Skip copying derived `memory-index.db` and `host-history-index-v1.db` search indexes.
        #[arg(long)]
        skip_index: bool,
        /// Rows per batched PostgreSQL UNNEST insert transaction.
        #[arg(long, default_value_t = 2500)]
        batch_size: usize,
        /// Align archived agent runtime states, clear legacy fault markers, and close unclosed turns.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        reconcile_stale_runtime: bool,
        /// Skip re-streaming tables whose PostgreSQL row count already matches or exceeds SQLite.
        #[arg(long, default_value_t = false)]
        resume: bool,
        /// Output machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum OutboundCommand {
    /// List every unconfirmed send (the default).
    List,
    /// Send one again, as its agent and to its destination. The record
    /// closes with the new receipt only when that send succeeds.
    Retry {
        /// The key `list` shows.
        key: String,
    },
    /// Settle one without sending it: the recipient already has it, or it is
    /// no longer wanted.
    Discard {
        /// The key `list` shows.
        key: String,
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
enum KeepersCommand {
    /// List every keeper: its agent, its harness process and the binary it runs.
    List,
    /// End keepers and their harness processes, and detached harness processes
    /// (v2 Spiron): one agent's, or all of them. A running server starts a
    /// running agent's harness again; use this before rolling back to a binary
    /// without keepers or without detached harnesses.
    Stop {
        /// Agent name or `agt_...` id.
        agent: Option<String>,
        /// Every keeper.
        #[arg(long, conflicts_with = "agent")]
        all: bool,
        /// Also end a keeper that does not answer while its process runs
        /// (its socket is gone, or it is wedged), by signal, once its
        /// recorded identity proves the process is that keeper.
        #[arg(long)]
        force: bool,
    },
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
    /// Check and update installed harnesses to the latest upstream version.
    Update {
        /// Optional harness ID filter (updates all when omitted).
        harness: Option<String>,
    },
}

#[derive(Subcommand)]
enum ChannelsCommand {
    /// Bind a Telegram bot token, Managed Bot ID (`getManagedBotToken`), or request a new Managed Bot for an agent.
    Add {
        /// Channel kind. Only `telegram` is supported.
        #[arg(default_value = "telegram")]
        kind: String,
        #[arg(long)]
        agent: String,
        /// File holding the bot token. The token is not an argument.
        #[arg(long)]
        token_file: Option<PathBuf>,
        /// Telegram bot user ID for a Managed Bot (`getManagedBotToken`).
        #[arg(long = "bot-id")]
        bot_id: Option<i64>,
        /// Suggested Telegram `@username` (or existing spare bot `@username`).
        #[arg(long = "username", alias = "bot")]
        bot_username: Option<String>,
        /// Optional manager agent or bot name when requesting/fetching a Managed Bot.
        #[arg(long)]
        manager: Option<String>,
    },
    /// Generate a Telegram Bot API 9.6 `https://t.me/newbot/<manager>/<bot>?name=...` creation link and register pending auto-binding for `--agent`.
    #[command(visible_alias = "new-bot", alias = "request_bot")]
    RequestBot {
        #[arg(long)]
        agent: String,
        /// Suggested Telegram `@username` for the new bot.
        #[arg(long = "username", alias = "bot")]
        bot_username: Option<String>,
        /// Optional manager agent or bot name.
        #[arg(long)]
        manager: Option<String>,
    },
    /// List channels. No secret is printed.
    List,
}

/// Options shared by message browsing and lexical search.
#[derive(Args, Clone, Debug, Default)]
struct HistoryOptions {
    #[arg(long)]
    with: Option<String>,
    #[arg(long)]
    scope: Option<String>,
    #[arg(long, alias = "from")]
    sender: Option<String>,
    #[arg(long = "in", conflicts_with_all = ["outbound", "direction"])]
    inbound: bool,
    #[arg(long = "out", conflicts_with_all = ["inbound", "direction"])]
    outbound: bool,
    #[arg(long, value_parser = ["in", "out", "all"])]
    direction: Option<String>,
    #[arg(long, alias = "include-tools")]
    tools: bool,
    #[arg(long)]
    only_tools: bool,
    #[arg(long)]
    tool: Option<String>,
    /// Relative duration (10m, 2h, 7d), RFC3339 timestamp, or epoch milliseconds.
    #[arg(long, alias = "since-ms", value_parser = parse_time)]
    since: Option<i64>,
    #[arg(long, alias = "until-ms", value_parser = parse_time)]
    until: Option<i64>,
    #[arg(long)]
    from_seq: Option<i64>,
    #[arg(long)]
    to_seq: Option<i64>,
    /// Number of entries (default: 20).
    #[arg(short = 'n', long, conflicts_with = "all")]
    limit: Option<usize>,
    /// Fetch every matching entry.
    #[arg(long)]
    all: bool,
    #[arg(long, value_parser = ["oldest", "newest", "relevance"])]
    sort: Option<String>,
    #[arg(long)]
    radius: Option<usize>,
    #[arg(long)]
    exclude: Option<String>,
    #[arg(long)]
    full: bool,
    #[arg(long)]
    json: bool,
    #[arg(short = 'f', long, conflicts_with = "until")]
    follow: bool,
}

#[derive(Args, Clone, Debug)]
struct ConversationArgs {
    agent: Option<String>,
    /// Optional search text; --query is also accepted.
    #[arg(conflicts_with = "query")]
    text: Option<String>,
    #[arg(short = 'q', long)]
    query: Option<String>,
    #[command(flatten)]
    options: HistoryOptions,
}

#[derive(Args, Clone, Debug)]
struct SearchArgs {
    query: String,
    #[arg(long)]
    agent: Option<String>,
    #[command(flatten)]
    options: HistoryOptions,
}

#[derive(Args, Clone, Debug)]
struct InboxArgs {
    agent: Option<String>,
    /// Read the operator's inbox: what agents reported with `send operator`.
    /// Only the operator may.
    #[arg(long, conflicts_with_all = ["agent", "unread", "sender"])]
    operator: bool,
    /// Exclusive mailbox sequence cursor.
    #[arg(long)]
    since: Option<i64>,
    /// Number of messages (default: 20).
    #[arg(short = 'n', long, conflicts_with = "all")]
    limit: Option<usize>,
    #[arg(long)]
    all: bool,
    #[arg(long, alias = "undelivered")]
    unread: bool,
    #[arg(long, alias = "from")]
    sender: Option<String>,
    #[arg(long)]
    full: bool,
    #[arg(long)]
    json: bool,
    #[arg(short = 'f', long)]
    follow: bool,
}

fn parse_time(raw: &str) -> Result<i64, String> {
    parse_time_at(raw, anyagents_core::now_ms())
}

fn parse_time_at(raw: &str, now: i64) -> Result<i64, String> {
    if let Ok(ms) = raw.parse::<i64>() {
        return Ok(ms);
    }
    if let Some((amount, multiplier)) = [
        ("ms", 1_i64),
        ("s", 1_000),
        ("m", 60_000),
        ("h", 3_600_000),
        ("d", 86_400_000),
    ]
    .into_iter()
    .find_map(|(unit, scale)| raw.strip_suffix(unit).map(|n| (n, scale)))
    {
        let duration = amount
            .parse::<i64>()
            .ok()
            .filter(|n| *n >= 0)
            .and_then(|n| n.checked_mul(multiplier));
        return duration
            .and_then(|d| now.checked_sub(d))
            .ok_or_else(|| format!("invalid duration: {raw}"));
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|t| t.timestamp_millis())
        .map_err(|_| {
            format!(
                "invalid time `{raw}`; use 10m, 2h, 7d, an RFC3339 timestamp, or epoch milliseconds"
            )
        })
}

#[derive(Subcommand)]
enum AgentsCommand {
    /// Inspect one agent and its recent conversation.
    Inspect {
        agent: Option<String>,
        #[arg(short = 'n', long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
    /// Read the incoming mailbox.
    Inbox(InboxArgs),
    /// Read, search, or follow conversation history.
    #[command(visible_aliases = ["conversation", "messages", "msg"], alias = "tail")]
    Logs(ConversationArgs),
    /// Send a text message.
    Send {
        to: String,
        text: String,
        #[arg(long)]
        from: Option<String>,
    },
    /// Resume an archived or paused agent.
    #[command(visible_alias = "wake")]
    Resume {
        name: String,
        #[arg(long)]
        prompt: Option<String>,
    },
    /// View or update the agent description.
    Describe {
        agent: Option<String>,
        description: Option<String>,
        #[arg(long)]
        instructions: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long)]
        clear: bool,
    },
    /// End an agent's remote (cloud) sessions now: every token of each stops authenticating on its next call (operator only).
    RevokeRemote {
        /// Agent name or `agt_...` ID.
        agent: String,
        /// Only this remote session (`rs_...`); all of the agent's sessions when omitted.
        #[arg(long)]
        session: Option<String>,
    },
    /// Rename an agent in place while preserving its `agt_...` ID, history, mailbox, grants, seat, and Telegram binding, and keeping the old name as a live alias.
    Rename {
        /// Current agent name or `agt_...` ID.
        agent: String,
        /// New agent name (`[a-z0-9][a-z0-9_-]*`).
        new_name: String,
    },
    /// Set or clear an agent's short role label (`anyagents agents role <agent> <role>`).
    Role {
        /// Target agent name or `agt_...` ID.
        agent: String,
        /// Display-only role label; no effect on lifecycle or scope.
        role: Option<String>,
        /// Clear the role label.
        #[arg(long)]
        clear: bool,
    },
    /// Record an agent. The harness is a catalog key.
    Create {
        name: String,
        /// Optional short role label (`Planner`, `Design`, `Support`, ...).
        #[arg(long)]
        role: Option<String>,
        #[arg(long, default_value = "")]
        harness: String,
        #[command(flatten)]
        runtime_args: runtime_cli::RuntimeCliArgs,
        #[arg(long)]
        model: Option<String>,
        /// Optional upstream provider account profile (e.g. `acct-a`, `acct-b`, `anthropic/acct-b`).
        #[arg(long)]
        profile: Option<String>,
        /// Optional repository (`<org>/<repo>`, local repo path, or `inherit`) to provision a Git repository of the agent's own.
        #[arg(long)]
        repo: Option<String>,
        /// Optional Git branch name for the managed checkout when `--repo` is set (defaults to `agent/<name>`).
        #[arg(long)]
        branch: Option<String>,
        /// Optional starting Git ref/branch when `--repo` is set (defaults to `origin/HEAD`).
        #[arg(long)]
        base: Option<String>,
        #[arg(long)]
        workdir: Option<PathBuf>,
        #[arg(long)]
        description: Option<String>,
        #[arg(long, visible_alias = "instruction", visible_alias = "system-prompt")]
        instructions: Option<String>,
        #[arg(long)]
        parent: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
    },
    /// Archive an agent (`state = archived`), scaling its process to zero while keeping its session.
    Archive { name: String },
    /// Pause an agent (`state = paused`), stopping its process, channel polling, and group wakes until resumed.
    Pause { name: String },
    /// Wake and resume an archived or paused agent (`state = running`).
    /// Start a new logical session for a quiescent agent; retain all old history and pins.
    #[command(alias = "new-session")]
    ResetSession { name: String },
    /// Permanently delete an agent and its on-disk directory.
    Delete { name: String },
    /// List agents.
    List {
        /// Optional state filter: `running`, `paused`, `archived`, or `all`.
        #[arg(short = 's', long)]
        state: Option<String>,
        /// Optional parent filter: agent name, `agt_...` id, `self`, or `root`.
        #[arg(short = 'p', long)]
        parent: Option<String>,
        /// Expand full multi-level sub-agent tree (`L1 -> L2 -> L3+`).
        #[arg(short = 't', long)]
        tree: bool,
        /// Maximum sub-agent tree depth to expand (`0` = roots only, `1` = L1, `2` = L2, ...).
        #[arg(short = 'd', long)]
        depth: Option<usize>,
        /// Show wide table including `agt_...` IDs without terminal truncation.
        #[arg(short = 'w', long)]
        wide: bool,
    },
    /// View or update an agent's standing role instructions and refresh its injected system prompt.
    Instructions {
        /// Agent name or `agt_...` id (or `all` to refresh every agent's system prompt).
        name: String,
        /// New standing role instructions for this agent (omit to view current).
        instructions: Option<String>,
        /// Clear custom standing instructions.
        #[arg(long)]
        clear: bool,
    },
    /// Switch an existing agent's harness in place while preserving its identity, model, profile, workdir, history, and lifecycle state.
    Harness {
        /// Agent name or `agt_...` id.
        name: String,
        /// Target harness ID (`codex`, `opencode`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`).
        harness: Option<String>,
        /// Target harness ID flag (`--harness codex`).
        #[arg(long = "harness")]
        harness_flag: Option<String>,
        #[command(flatten)]
        runtime_args: runtime_cli::RuntimeCliArgs,
        /// Optional model override (retains existing model when omitted).
        #[arg(long)]
        model: Option<String>,
        /// Optional `<provider>/<profile>` override (retains existing bound profile when omitted).
        #[arg(long)]
        profile: Option<String>,
    },
}

fn main() -> ExitCode {
    // Rust ignores SIGPIPE by default. Restore normal Unix pipeline behavior
    // before any output so a closed reader exits quietly instead of panicking.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    let argv: Vec<std::ffi::OsString> = std::env::args_os().collect();
    if argv
        .first()
        .and_then(|arg| Path::new(arg).file_stem())
        .is_some_and(|stem| stem == "anyagents-guest")
    {
        return anyagents_guest::run_cli_exit_code();
    }
    if argv.get(1).is_some_and(|arg| arg == "internal") {
        return internal(&argv[2..]);
    }
    sandbox::hydrate_operator_groups_if_needed();
    let raw_argv: Vec<String> = std::env::args().collect();
    if let Some(code) = keeper::try_run(&raw_argv) {
        return ExitCode::from(u8::try_from(code).unwrap_or(1));
    }
    if let Some(sbx_args) = sandbox_exec::try_parse_from_raw_argv(&raw_argv) {
        return match sandbox_exec::run(sbx_args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                ui::error(&err.to_string());
                ExitCode::FAILURE
            }
        };
    }
    let cli = Cli::parse();
    ui::set_verbose(cli.verbose);
    // The daemon also writes child stdin pipes and must handle their closure as
    // an I/O error, not terminate the entire service. Only operator output uses
    // the default pipeline disposition installed above.
    #[cfg(unix)]
    if matches!(cli.command, Some(Command::Serve { .. })) {
        unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        }
    }
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            ui::error(&err.to_string());
            ExitCode::FAILURE
        }
    }
}

/// Helpers AnyAgents runs as itself, never commands for an operator. They
/// start before any deployment setup: no data directory, no group hydration,
/// no inherited state beyond what the caller passed.
fn internal(args: &[std::ffi::OsString]) -> ExitCode {
    match args.first().and_then(|arg| arg.to_str()) {
        Some("admission-probe") => repo::admission_probe_main(&args[1..]),
        Some("seat-file") => seat_tree::seat_file_main(&args[1..]),
        _ => {
            eprintln!("anyagents internal: unknown helper");
            ExitCode::from(2)
        }
    }
}

fn resolve_data_dir(raw: PathBuf, _is_serve: bool) -> PathBuf {
    if raw == Path::new(".anyagents")
        && (!raw.exists()
            || (!raw.join("anyagents.sock").exists() && !crate::sandbox::is_isolated_test_env()))
        && let Some(env_dir) = data::data_dir()
    {
        return env_dir;
    }
    raw
}

fn run(cli: Cli) -> Result<(), anyagents_core::Error> {
    anyagents_core::db::set_cli_database_url(cli.database_url.clone());
    let is_serve = matches!(cli.command, Some(Command::Serve { .. }));
    let data = resolve_data_dir(cli.data, is_serve);
    sandbox::load_principal_registry(&data)?;
    // Helpers a command runs use the deployment's manifest, as the server's do.
    if !is_serve {
        anyagents_toolchain::init_cli(&data, sandbox::host_home_dir());
    }
    match cli.command.unwrap_or(Command::Status { json: false }) {
        Command::Serve { port: Some(port) } => server::run_with_port(&data, Some(port)),
        Command::Serve { port: None } => server::run(&data),
        Command::Status { json } => status_cmd(&data, json),
        Command::Auth { action } => {
            control_cli::run_auth_cmd(&data, action, sign_in, list_harnesses)
        }
        Command::SignIn { agent, profile } => sign_in(&data, &agent, profile.as_deref()),
        Command::Harnesses { action } => match action {
            HarnessesCommand::List => {
                list_harnesses(&data);
                Ok(())
            }
            HarnessesCommand::Profiles { harness } => {
                control_cli::profiles_list_cmd(&data, harness.as_deref(), false, None, false)
            }
            HarnessesCommand::Update { harness } => update_harnesses_cmd(&data, harness.as_deref()),
        },
        Command::Profiles(args) => control_cli::run_profiles_cmd(&data, args),
        Command::Secrets(args) => control_cli::run_secrets_cmd(&data, args),
        Command::Mcp {
            config,
            _profile: _,
        } => mcp::run(&data, config.as_deref()),
        Command::AgentRun { config, command } => sandbox::run(&data, config.as_deref(), &command),
        Command::Boundary => print_boundary(&data),
        Command::Keepers { action } => match action {
            KeepersCommand::List => keeper::list(&data),
            KeepersCommand::Stop { agent, all, force } => {
                if agent.is_none() && !all {
                    return Err(anyagents_core::Error::Message(
                        "name an agent, or pass --all".into(),
                    ));
                }
                keeper::stop(&data, agent.as_deref(), force)
            }
        },
        Command::FakeOpencode => fake_opencode::run(),
        Command::Harness {
            config,
            harness,
            harness_flag,
            model,
            profile,
        } => {
            if let Some(target_harness) = harness
                .as_deref()
                .or(harness_flag.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                agent::harness_cmd(
                    &data,
                    &config.to_string_lossy(),
                    Some(target_harness),
                    None,
                    None,
                    model.as_deref(),
                    profile.as_deref(),
                )
            } else {
                harness_double::run(&config)
            }
        }
        Command::Agents {
            action: Some(action),
        } => agent::agents(&data, action),
        Command::Agents { action: None } => {
            agent::peers_with_options(&data, None, None, false, None, false)
        }
        Command::Peers {
            state,
            parent,
            tree,
            depth,
            wide,
        } => agent::peers_with_options(
            &data,
            state.as_deref(),
            parent.as_deref(),
            tree,
            depth,
            wide,
        ),
        Command::Subagents {
            parent,
            state,
            tree,
            depth,
            wide,
            inherit,
            harness,
            model,
            profile,
            agent,
            clear,
        } => {
            if parent
                .as_deref()
                .is_some_and(|p| p.trim().eq_ignore_ascii_case("default"))
                || inherit
                || harness.is_some()
                || model.is_some()
                || profile.is_some()
                || clear
            {
                subagents_default_cmd(
                    &data,
                    inherit,
                    harness.as_deref(),
                    model.as_deref(),
                    profile.as_deref(),
                    agent.as_deref(),
                    clear,
                )
            } else {
                let cleaned_parent = parent.filter(|p| !p.trim().eq_ignore_ascii_case("list"));
                let parent_filter = cleaned_parent.or_else(|| {
                    if std::env::var("ANYAGENTS_AGENT_ID").is_ok()
                        || std::env::var("ANYAGENTS_AGENT_NAME").is_ok()
                    {
                        Some("self".to_owned())
                    } else {
                        Some("subagents".to_owned())
                    }
                });
                agent::peers_with_options(
                    &data,
                    state.as_deref(),
                    parent_filter.as_deref(),
                    tree,
                    depth,
                    wide,
                )
            }
        }
        Command::Spawn(args) => agent::spawn(&data, args),
        Command::Archive { name } => agent::archive_cmd(&data, name),
        Command::Pause { name } => agent::pause_cmd(&data, name),
        Command::Wake { name, prompt } => agent::resume_cmd(&data, &name, prompt),
        Command::Resume { faulted: true, .. } => agent::resume_faulted(&data),
        Command::Resume { name, prompt, .. } => {
            agent::resume_cmd(&data, name.as_deref().unwrap_or_default(), prompt)
        }
        Command::Delete { name } => agent::delete_cmd(&data, &name),
        Command::Flush { agent } => agent::flush(&data, &agent),
        Command::Requeue { agent, message } => agent::requeue(&data, &agent, message.as_deref()),
        Command::Model {
            agent,
            target_model,
            base_url,
            model,
            harness,
            key_file,
            profile,
        } => {
            let resolved_model = model.or(target_model);
            model_cmd(
                &data,
                &agent,
                base_url.as_deref(),
                resolved_model.as_deref(),
                harness.as_deref(),
                key_file.as_deref(),
                profile.as_deref(),
            )
        }
        Command::Send(send_args) => {
            let SendArgs {
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
            } = *send_args;
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
        Command::Inbox(args) => channel::inbox(&data, args),
        Command::Conversation(args) => channel::conversation(&data, args),
        Command::Redact {
            agent,
            message_id,
            seq,
            start,
            end,
            unit,
            reason,
        } => {
            let span = match (start, end, unit) {
                (Some(start), Some(end), Some(unit)) => {
                    Some(api::RedactionSpan { start, end, unit }.try_into()?)
                }
                (None, None, None) => None,
                _ => return Err(Error::Message("span needs start, end and unit".into())),
            };
            let request = anyagents_core::redaction::Request {
                message_id,
                seq,
                span,
                reason,
            };
            let body = serde_json::to_vec(&request).map_err(|e| Error::Message(e.to_string()))?;
            let path = format!("/v1/agents/{agent}/conversation/redact");
            let receipt: anyagents_core::redaction::Receipt =
                client::call(&data, "POST", &path, Some(body), &client::authority(&data))
                    .map_err(Error::Message)?
                    .ok_or_else(|| {
                        Error::Message(
                            "redaction requires a running admin API; no offline bypass".into(),
                        )
                    })?;
            println!(
                "{}",
                serde_json::to_string_pretty(&receipt)
                    .map_err(|e| Error::Message(e.to_string()))?
            );
            Ok(())
        }
        Command::Search(args) => channel::conversation(
            &data,
            ConversationArgs {
                agent: Some(args.agent.unwrap_or_else(|| "all".into())),
                text: None,
                query: Some(args.query),
                options: args.options,
            },
        ),
        Command::Usage(args) => control_cli::run_usage_cmd(&data, args),
        Command::Channels { action } => channel::channels(&data, action),
        Command::Groups { action } => groups(&data, action),
        Command::Describe {
            agent,
            description,
            instructions,
            role,
            clear,
        } => agent::describe_cmd(&data, agent, description, instructions, role, clear),
        Command::Rename { agent, new_name } => agent::rename_cmd(&data, &agent, &new_name),
        Command::Role { agent, role, clear } => {
            agent::role_cmd(&data, &agent, role.as_deref(), clear)
        }
        Command::Schedule(schedule_args) => {
            let ScheduleArgs {
                all,
                agent,
                at,
                every,
                watch,
                prompt,
                cancel,
                json,
            } = *schedule_args;
            schedule_cmd(&data, agent, at, every, watch, prompt, cancel, json, all)
        }
        Command::Automations { json } => {
            schedule_cmd(&data, None, None, None, None, None, None, json, true)
        }
        Command::Inspect { agent, limit, json } => inspect_cmd(&data, agent, limit, json),
        Command::Outbound { action, json } => match action.unwrap_or(OutboundCommand::List) {
            OutboundCommand::List => outbound::list(&data, json),
            OutboundCommand::Retry { key } => outbound::retry(&data, &key, json),
            OutboundCommand::Discard { key } => outbound::discard(&data, &key, json),
        },
        Command::SandboxExec(args) => sandbox_exec::run(args),
        Command::MemoryService(args) => memory_service::run(&data, args),
        Command::Db { action } => db_cmd(&data, *action),
    }
}

fn db_cmd(data: &Path, action: DbCommand) -> Result<(), anyagents_core::Error> {
    match action {
        DbCommand::MigrateToPostgres {
            source,
            target,
            skip_index,
            batch_size,
            reconcile_stale_runtime,
            resume,
            json,
        } => {
            let source_dir = source.unwrap_or_else(|| data.to_path_buf());
            let target_url = target
                .or_else(|| anyagents_core::db::configured_database_url_for_data_dir(&source_dir))
                .ok_or_else(|| {
                    anyagents_core::Error::Message(
                        "missing --target PostgreSQL URL (or --database-url / ANYAGENTS_DATABASE_URL)"
                            .into(),
                    )
                })?;
            let opts = anyagents_core::db::MigrateToPostgresOptions {
                source_dir,
                target_url,
                skip_index,
                batch_size,
                reconcile_stale_runtime,
                resume,
                master_key: anyagents_core::MasterKey::from_env()?,
            };
            let report = anyagents_core::db::migrate_sqlite_dir_to_postgres(&opts)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).map_err(|e| {
                        anyagents_core::Error::Message(format!("json encode failed: {e}"))
                    })?
                );
            } else {
                let total_rows: i64 = report.tables.iter().map(|t| t.pg_rows_after).sum();
                println!(
                    "Migrated {} tables ({total_rows} PG rows; agent_files={}, deployment_blobs={}, gateway_routes={}, sandbox_bindings={}, skipped_secrets={}) in {} ms (reconciled: archived_runtime={}, legacy_faults={}, stale_turns={})",
                    report.tables.len(),
                    report.agent_files_migrated,
                    report.deployment_blobs_migrated,
                    report.gateway_routes_migrated,
                    report.sandbox_bindings_migrated,
                    report.skipped_secret_files,
                    report.elapsed_ms,
                    report.reconciled_archived_runtime,
                    report.reconciled_legacy_faults,
                    report.reconciled_stale_turns,
                );
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn schedule_cmd(
    data: &Path,
    agent: Option<String>,
    at: Option<String>,
    every: Option<String>,
    watch: Option<String>,
    prompt: Option<String>,
    cancel: Option<String>,
    json: bool,
    all: bool,
) -> Result<(), anyagents_core::Error> {
    let target = if all {
        Some("all".to_string())
    } else {
        agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
            .or_else(|| client::agent_identity().map(|id| id.to_string()))
    };
    let action = if cancel.is_some() {
        "cancel"
    } else if at.is_some() || every.is_some() || watch.is_some() {
        "set"
    } else {
        "list"
    };
    let fleet = target.as_deref() == Some("all");
    if fleet && (action != "list" || prompt.is_some()) {
        return Err(anyagents_core::Error::Message(
            "fleet automations are read-only; name one agent to change a schedule".into(),
        ));
    }
    let body = serde_json::json!({
        "action": action,
        "at": at,
        "every": every,
        "watch": watch,
        "prompt": prompt,
        "id": cancel,
        "agent": target,
    });
    if client::can_call_server(data) {
        let path = match target.as_deref() {
            Some("all") => "/v1/schedules".to_string(),
            Some(t) => format!("/v1/agents/{t}/schedules"),
            None => "/v1/schedules".to_string(),
        };
        let bytes = serde_json::to_vec(&body)
            .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
        let response = client::call::<serde_json::Value>(
            data,
            "POST",
            &path,
            Some(bytes),
            &client::authority(data),
        );
        let response = if fleet {
            response.map_err(anyagents_core::Error::Message)?
        } else {
            response.ok().flatten()
        };
        if let Some(val) = response {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&val)
                        .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
                );
            } else if ui::is_pretty() {
                ui::schedules(&val);
            } else if let Some(s) = val.get("schedule") {
                println!(
                    "{}\t{}\t{}\t{}",
                    s.get("id").and_then(|v| v.as_str()).unwrap_or("-"),
                    s.get("kind").and_then(|v| v.as_str()).unwrap_or("-"),
                    s.get("spec").and_then(|v| v.as_str()).unwrap_or("-"),
                    s.get("prompt").and_then(|v| v.as_str()).unwrap_or("-")
                );
            } else if let Some(arr) = val.get("schedules").and_then(|v| v.as_array()) {
                for s in arr {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        s.get("id").and_then(|v| v.as_str()).unwrap_or("-"),
                        s.get("kind").and_then(|v| v.as_str()).unwrap_or("-"),
                        s.get("spec").and_then(|v| v.as_str()).unwrap_or("-"),
                        s.get("state").and_then(|v| v.as_str()).unwrap_or("-"),
                        s.get("prompt").and_then(|v| v.as_str()).unwrap_or("-")
                    );
                }
            } else {
                println!("{val}");
            }
            return Ok(());
        }
    }
    if fleet {
        return Err(anyagents_core::Error::Message(
            "fleet automations require a running server".into(),
        ));
    }
    let store = anyagents_core::Store::open(data.join("state.db"))?;
    let target_name = target.ok_or_else(|| {
        anyagents_core::Error::Message("schedule requires an agent name or id".into())
    })?;
    let rec = store.agent_by_name(&target_name)?;
    match action {
        "cancel" => {
            let cid = cancel.unwrap_or_default();
            let count = schedule::cancel_agent_schedule(&store, &rec.id, &cid)
                .map_err(anyagents_core::Error::Message)?;
            let out = serde_json::json!({
                "agent_id": rec.id.to_string(),
                "agent": rec.name,
                "cancelled": count,
                "schedules": schedule::list_agent_schedules(&store, &rec.id, false).map_err(anyagents_core::Error::Message)?,
            });
            if !json && ui::is_pretty() {
                ui::schedules(&out);
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&out)
                        .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
                );
            }
        }
        "set" => {
            let prompt_text = prompt.unwrap_or_default();
            let health = anyagents_supervisor::HealthBoard::default();
            let created = schedule::create_agent_schedule(
                &store,
                &health,
                &rec,
                at.as_deref(),
                every.as_deref(),
                watch.as_deref(),
                &prompt_text,
            )
            .map_err(anyagents_core::Error::Message)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "agent_id": rec.id.to_string(),
                        "agent": rec.name,
                        "schedule": created,
                    }))
                    .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
                );
            } else if ui::is_pretty() {
                ui::schedules(&serde_json::json!({"schedule": created}));
            } else {
                println!(
                    "{}\t{}\t{}\t{}",
                    created.id, created.kind, created.spec, created.prompt
                );
            }
        }
        _ => {
            let list = schedule::list_agent_schedules(&store, &rec.id, false)
                .map_err(anyagents_core::Error::Message)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "agent_id": rec.id.to_string(),
                        "agent": rec.name,
                        "schedules": list,
                    }))
                    .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
                );
            } else if ui::is_pretty() {
                ui::schedules(&serde_json::json!({"schedules": list}));
            } else {
                for s in list {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        s.id, s.kind, s.spec, s.state, s.prompt
                    );
                }
            }
        }
    }
    Ok(())
}

fn inspect_cmd(data: &Path, agent: Option<String>, limit: usize, json: bool) -> Result<(), Error> {
    let target = agent
        .or_else(|| client::agent_identity().map(|id| id.to_string()))
        .ok_or_else(|| Error::Message("inspect requires an agent name or id".into()))?;
    let view = agent::view_named(data, &target)?;
    let turns = channel::read_conversation(
        data,
        &view.id,
        None,
        &HistoryOptions {
            limit: Some(limit),
            ..Default::default()
        },
    )?
    .turns;
    if json || !ui::is_pretty() {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "agent": view, "schedules": view.schedules, "recent_turns": turns,
            }))
            .map_err(|e| Error::Message(e.to_string()))?
        );
    } else {
        ui::value(
            &view.name,
            &serde_json::json!({
                "id": view.id,
                "state": view.state,
                "process": view.process,
                "turn": view.turn_state,
                "lag_ms": view.oldest_undelivered_age_ms,
                "block": view.block_reason,
                "harness": view.harness,
                "model": view.model,
                "profile": view.profile,
                "workdir": view.workdir,
                "seat": view.seat_user,
                "open_turn": view.has_open_turn,
                "delivery": view.delivery,
                "schedules": view.schedules,
                "last_error": view.last_error,
                "turn_errors": view.turn_errors,
                "unsettled_outbound": view.unsettled_outbound,
            }),
        );
        channel::emit_turns(&turns, true, false)?;
    }
    Ok(())
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
fn list_harnesses(data: &Path) {
    let catalog = anyagents_installer::Catalog::load(data)
        .unwrap_or_else(|_| anyagents_installer::Catalog::builtin());
    if ui::is_pretty() {
        println!("\n  {}\n", ui::bold_cyan("Supported Harnesses"));
        let mut table = ui::Table::new(&[
            ("HARNESS", false),
            ("VERSION", false),
            ("CONFIG ENV", false),
            ("INSTALL SOURCE", false),
        ]);
        for entry in catalog.harnesses() {
            let ver = install::detect_version(data, &entry.id);
            let ver_cell = if ver == "-" {
                ui::dim("-")
            } else {
                ui::green(&ver)
            };
            table.add_row(vec![
                ui::bold(&ui::cyan(&entry.id)),
                ver_cell,
                ui::dim(signin::config_variable(&entry.id).unwrap_or("-")),
                entry.source.label(),
            ]);
        }
        table.print();
        println!();
        return;
    }
    for entry in catalog.harnesses() {
        println!(
            "{}\t{}\t{}",
            entry.id,
            signin::config_variable(&entry.id).unwrap_or("-"),
            entry.source.label()
        );
    }
}

fn update_harnesses_cmd(data: &Path, target: Option<&str>) -> Result<(), anyagents_core::Error> {
    let catalog = anyagents_installer::Catalog::load(data)
        .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
    let targets: Vec<String> = match target.map(str::trim).filter(|s| !s.is_empty()) {
        Some(t) => {
            let normalized = anyagents_harness::normalize_harness_name(t);
            if catalog.get(normalized).is_none() {
                return Err(anyagents_core::Error::Message(
                    crate::install::unknown_harness_message(
                        t,
                        catalog.harnesses().iter().map(|entry| entry.id.as_str()),
                    ),
                ));
            }
            vec![normalized.to_string()]
        }
        None => catalog
            .harnesses()
            .iter()
            .chain(catalog.runtimes())
            .filter(|entry| {
                entry.source.is_managed()
                    && !matches!(
                        entry.source,
                        anyagents_installer::source::Source::Unconfigured
                    )
            })
            .map(|entry| entry.id.clone())
            .collect(),
    };

    let mut rows = Vec::new();
    for id in targets {
        match install::update_harness(data, &id) {
            Ok((before, after, updated)) => {
                rows.push((id, before, after, updated, None));
            }
            Err(err) => {
                let cur = install::detect_version(data, &id);
                rows.push((id, cur.clone(), cur, false, Some(err)));
            }
        }
    }

    if ui::is_pretty() {
        println!("\n  {}\n", ui::bold_cyan("Harness Updates"));
        let mut table = ui::Table::new(&[
            ("HARNESS", false),
            ("BEFORE", false),
            ("CURRENT", false),
            ("STATUS", false),
        ]);
        for (id, before, after, updated, err) in rows {
            let status = if let Some(e) = err {
                ui::yellow(&format!("! {e}"))
            } else if updated {
                ui::bold_green("↑ updated")
            } else if after == "-" {
                ui::dim("on-demand")
            } else {
                ui::green("✓ up-to-date")
            };
            table.add_row(vec![
                ui::bold(&ui::cyan(&id)),
                ui::dim(&before),
                if updated {
                    ui::bold_green(&after)
                } else {
                    after
                },
                status,
            ]);
        }
        table.print();
        println!();
        return Ok(());
    }

    for (id, before, after, updated, err) in rows {
        let state = if let Some(e) = err {
            format!("error:{e}")
        } else if updated {
            "updated".to_string()
        } else {
            "up-to-date".to_string()
        };
        println!("{id}\t{before}\t{after}\t{state}");
    }
    Ok(())
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
    if ui::is_pretty() {
        ui::success(&format!("Key stored for {} · {model}", recorded.name));
    } else {
        println!("key stored\t{}\t{}", recorded.name, model);
    }
    Ok(())
}

fn model_cmd(
    data: &Path,
    agent: &str,
    base_url: Option<&str>,
    model: Option<&str>,
    harness: Option<&str>,
    key_file: Option<&Path>,
    profile: Option<&str>,
) -> Result<(), anyagents_core::Error> {
    use anyagents_core::Error;
    if profile.is_some() && (key_file.is_some() || base_url.is_some()) {
        return Err(Error::Message(
            "profile cannot be combined with a key or base URL".into(),
        ));
    }
    if let Some(target_harness) = harness.map(str::trim).filter(|s| !s.is_empty())
        && key_file.is_none()
        && base_url.is_none()
    {
        return agent::harness_cmd(
            data,
            agent,
            Some(target_harness),
            None,
            None,
            model,
            profile,
        );
    }
    let clean_model = model.map(str::trim).filter(|s| !s.is_empty());
    let clean_profile = profile.map(str::trim).filter(|s| !s.is_empty());
    if key_file.is_none() && base_url.is_none() && clean_model.is_none() && clean_profile.is_none()
    {
        return Err(Error::Message("model is required".into()));
    }
    if client::can_call_server(data) {
        let key = key_file.map(std::fs::read_to_string).transpose()?;
        let body = serde_json::json!({
            "model": clean_model,
            "base_url": base_url,
            "key": key,
            "profile": clean_profile,
        });
        match client::call::<serde_json::Value>(
            data,
            "POST",
            &format!("/v1/agents/{agent}/model"),
            Some(body.to_string().into_bytes()),
            &client::authority(data),
        ) {
            Ok(result) => {
                let result =
                    result.ok_or_else(|| Error::Message("the server answered nothing".into()))?;
                if ui::is_pretty() {
                    ui::value("Model updated", &result);
                } else {
                    println!("{result}");
                }
                return Ok(());
            }
            Err(err) if err.contains("404") => {}
            Err(err) => return Err(Error::Message(err)),
        }
    }
    if key_file.is_none() && base_url.is_none() {
        if client::agent_identity().is_some() {
            return Err(Error::Message(
                "administrative authority required (agent session cannot bypass server boundary)"
                    .into(),
            ));
        }
        let store = agent::open(data)?;
        let out = create::configure_agent_runtime(
            &store,
            data,
            "operator",
            agent,
            None,
            clean_model,
            clean_profile,
        )?;
        if ui::is_pretty() {
            ui::value("Model updated", &out);
        } else {
            println!("{out}");
        }
        return Ok(());
    }
    let model = clean_model.ok_or_else(|| Error::Message("model is required".into()))?;
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
    if ui::is_pretty() {
        ui::heading(&format!("Sign in · {target}"));
        ui::note("Follow the provider's sign-in steps. Input is masked; Ctrl+C cancels.");
    }
    let mut runner = signin::ProcessRunner;
    let mut sink = signin::TerminalSink;
    let raw_target = target.trim();
    let (split_target, split_prof) = match raw_target.split_once('/') {
        Some((lhs, rhs)) if !lhs.trim().is_empty() && !rhs.trim().is_empty() => {
            (lhs.trim(), Some(rhs.trim()))
        }
        _ => (raw_target, None),
    };
    let effective_profile = profile
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or(split_prof);
    let target = match anyagents_harness::normalize_harness_name(split_target) {
        "anthropic" => "claude-code",
        "openai" => "codex",
        "google-antigravity" => "agy",
        "deepseek" => "dsh",
        "xai" => "grok",
        other => other,
    };
    if signin::signable(target) {
        let explicit_prof = effective_profile
            .and_then(|p| p.split_once('/').map(|(_, r)| r.trim()).or(Some(p)))
            .filter(|s| !s.is_empty());
        let provider = create::upstream_provider_of(target, None);
        // Empty staging is removed on return; unique credentials survive failures.
        let staging =
            create::SigninStaging::create(data, &provider, &anyagents_core::now_ms().to_string())?;
        let staging_dir = staging.path();
        let use_native_agy_pkce = target == "agy"
            && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
            && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none();
        let outcome = if use_native_agy_pkce {
            signin::google_antigravity_pkce_sign_in(staging_dir, &mut sink)?
        } else {
            anyagents_installer::sign_in_at(
                &anyagents_installer::builtin_catalog(),
                staging_dir,
                target,
                &mut runner,
                &mut sink,
            )
            .map_err(|err| anyagents_core::Error::Message(err.to_string()))?
        };
        let (resolved_prof, final_dir, resolved_acct) =
            create::resolve_and_commit_staged_signin(data, &provider, staging_dir, explicit_prof)?;
        if ui::is_pretty() {
            match resolved_acct
                .as_deref()
                .or(outcome.account.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty() && *s != "-")
            {
                Some(account) => ui::success(&format!(
                    "Signed in · {provider}/{resolved_prof} · {account}"
                )),
                None => ui::success(&format!("Signed in · {provider}/{resolved_prof}")),
            }
            if ui::is_verbose() {
                ui::note(&format!("Profile directory: {}", final_dir.display()));
            }
        } else {
            println!(
                "signed in\tprofile:{provider}/{resolved_prof}\t{}\t{}",
                final_dir.display(),
                resolved_acct
                    .as_deref()
                    .or(outcome.account.as_deref())
                    .unwrap_or("-")
            );
        }
        return Ok(());
    }
    if let Some(prof) = effective_profile {
        let rec = agent::named(data, target)?;
        let cfg = anyagents_core::agent_config_dir(data, &rec.id);
        std::fs::create_dir_all(&cfg)?;
        let expected_prov = create::upstream_provider_of(&rec.harness, rec.model.as_deref());
        let (prov, prof_name) = prof.split_once('/').unwrap_or((&expected_prov, prof));
        let normalized = format!("{prov}/{prof_name}");
        create::write_profile_marker(&cfg, ".anyagents-profile", &format!("{normalized}\n"))?;
        let prof_dir = data.join("profiles").join(prov).join(prof_name);
        let has_stored_creds = std::fs::read_dir(&prof_dir)
            .ok()
            .is_some_and(|entries| entries.flatten().any(|e| e.file_name() != "profile.json"));
        if has_stored_creds && let Ok(store) = anyagents_core::Store::open(data.join("state.db")) {
            create::seed_harness_credentials(&store, data, &rec, &cfg)?;
            if let Some((_, uid, gid, _)) = crate::sandbox::seat_for_agent(rec.id.as_str()) {
                crate::sandbox::share_config_with_seat_uid(&cfg, uid, gid);
            }
            if ui::is_pretty() {
                ui::success(&format!("Signed in · {target} · profile {normalized}"));
                if ui::is_verbose() {
                    ui::note(&format!("Harness: {} · Profile: {normalized}", rec.harness));
                }
            } else {
                println!("signed in\t{target}\t{}\tprofile:{normalized}", rec.harness);
            }
            return Ok(());
        }
    }
    let rec = agent::named(data, target)?;
    let outcome = if anyagents_harness::normalize_harness_name(&rec.harness) == "agy"
        && std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
        && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none()
    {
        let root = std::path::absolute(data)?;
        let cfg = anyagents_core::agent_config_dir(&root, &rec.id);
        std::fs::create_dir_all(&cfg)?;
        std::fs::create_dir_all(anyagents_core::agent_work_dir(&root, &rec.id))?;
        signin::google_antigravity_pkce_sign_in(&cfg, &mut sink)?
    } else {
        signin::sign_in_agent(data, target, &mut runner, &mut sink)?
    };
    let harness = rec.harness.clone();
    if !outcome.signed_in {
        let hint = signin::install_hint(&harness).unwrap_or_else(|| "-".to_string());
        return Err(anyagents_core::Error::Message(format!(
            "{target} is not signed in: the vendor command finished without reporting a sign-in (install: {hint})"
        )));
    }
    create::finalize_agent_signin(data, &rec)?;
    // A sign-in is a new instance: the server gives the agent a fresh
    // capability and retires the last one.
    if server::socket_path(data).exists() {
        client::call::<serde_json::Value>(
            data,
            "POST",
            &format!("/v1/operator/agents/{target}/capability"),
            Some(b"{}".to_vec()),
            &client::Authority::Operator,
        )
        .map_err(anyagents_core::Error::Message)?
        .ok_or_else(|| {
            anyagents_core::Error::Message("capability rotation returned no receipt".into())
        })?;
    } else {
        // An offline instance uses the same durable capability rotation mechanism.
        let store = agent::open_with_key(data)?;
        operator::mint(&store, &rec.id)?;
    }
    if ui::is_pretty() {
        match outcome
            .account
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && *s != "-")
        {
            Some(account) => ui::success(&format!("Signed in · {target} · {harness} · {account}")),
            None => ui::success(&format!("Signed in · {target} · {harness}")),
        }
    } else {
        println!(
            "signed in\t{target}\t{harness}\t{}",
            outcome.account.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

fn subagents_default_cmd(
    data: &Path,
    inherit: bool,
    harness: Option<&str>,
    model: Option<&str>,
    profile: Option<&str>,
    agent: Option<&str>,
    clear: bool,
) -> Result<(), anyagents_core::Error> {
    use anyagents_core::Error;
    let clear = clear || inherit;
    let is_write = harness.is_some() || model.is_some() || profile.is_some() || clear;
    if is_write && !clear && agent.is_some_and(|name| !name.trim().is_empty()) {
        return Err(Error::Message(
            "per-agent defaults are retired; use role template frontmatter or set the global deployment default".into(),
        ));
    }
    let cfg = if is_write {
        let mode = if clear {
            "inherit".to_string()
        } else {
            "preset".to_string()
        };
        if client::can_call_server(data) {
            let body = serde_json::json!({
                "agent": agent,
                "clear": clear,
                "mode": mode,
                "harness": harness,
                "model": model,
                "profile": profile,
            });
            match client::call::<create::SubagentDefaultsConfig>(
                data,
                "POST",
                "/v1/subagent-defaults",
                Some(body.to_string().into_bytes()),
                &client::authority(data),
            ) {
                Ok(Some(updated)) => updated,
                Ok(None) => {
                    return Err(Error::Message("the server answered nothing".into()));
                }
                Err(err) if err.contains("404") => {
                    let mut c = create::load_subagent_defaults(data);
                    apply_local_subagent_default(
                        &mut c, clear, &mode, harness, model, profile, agent,
                    );
                    create::save_subagent_defaults(data, &c)?;
                    c
                }
                Err(err) => return Err(Error::Message(err)),
            }
        } else {
            let mut c = create::load_subagent_defaults(data);
            apply_local_subagent_default(&mut c, clear, &mode, harness, model, profile, agent);
            create::save_subagent_defaults(data, &c)?;
            c
        }
    } else if client::can_call_server(data) {
        match client::call::<create::SubagentDefaultsConfig>(
            data,
            "GET",
            "/v1/subagent-defaults",
            None,
            &client::authority(data),
        ) {
            Ok(Some(c)) => c,
            Ok(None) | Err(_) => create::load_subagent_defaults(data),
        }
    } else {
        create::load_subagent_defaults(data)
    };

    if ui::is_pretty() {
        println!("\n  {}\n", ui::bold_cyan("Sub-Agent Defaults"));
        let mut table = ui::Table::new(&[
            ("SCOPE", false),
            ("STATUS", false),
            ("HARNESS", false),
            ("MODEL", false),
            ("PROFILE", false),
        ]);
        let fmt_row = |scope: &str, status: &str, s: &create::SubagentDefaultSpec| {
            vec![
                ui::bold(scope),
                status.to_string(),
                s.harness.clone().unwrap_or_default(),
                s.model.clone().unwrap_or_default(),
                s.profile.clone().unwrap_or_default(),
            ]
        };
        table.add_row(fmt_row("global (*)", "deployment fallback", &cfg.default));
        for (ag, s) in &cfg.agents {
            table.add_row(fmt_row(ag, "retired (ignored)", s));
        }
        table.print();
        println!();
        return Ok(());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&cfg).map_err(|e| Error::Message(e.to_string()))?
    );
    Ok(())
}

fn apply_local_subagent_default(
    cfg: &mut create::SubagentDefaultsConfig,
    clear: bool,
    mode: &str,
    harness: Option<&str>,
    model: Option<&str>,
    profile: Option<&str>,
    agent: Option<&str>,
) {
    if clear {
        if let Some(ag) = agent {
            cfg.agents.remove(ag.trim());
        } else {
            cfg.default = create::SubagentDefaultSpec {
                mode: "inherit".to_string(),
                harness: None,
                model: None,
                profile: None,
            };
        }
        return;
    }
    let spec = create::SubagentDefaultSpec {
        mode: mode.to_string(),
        harness: harness
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(anyagents_harness::normalize_harness_name)
            .map(str::to_string),
        model: model
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        profile: profile
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    };
    cfg.default = spec;
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
        bot: None,
    };
    let payload =
        serde_json::to_vec(&body).map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
    let resp: api::InteractResponse =
        client::call(data, "POST", "/v1/interact", Some(payload), &authority)
            .map_err(anyagents_core::Error::Message)?
            .ok_or_else(|| anyagents_core::Error::Message("empty interact response".into()))?;
    if ui::is_pretty() {
        ui::success(&format!("{} · {}", resp.action, resp.target));
        if let Some(detail) = resp.detail.as_deref() {
            ui::note(detail);
        }
    } else {
        println!(
            "{}\t{}\t{}",
            resp.action,
            resp.target,
            resp.detail.as_deref().unwrap_or("ok")
        );
    }
    Ok(())
}

fn status_cmd(data: &Path, json_out: bool) -> Result<(), anyagents_core::Error> {
    let sock = server::socket_path(data);
    let live_agents = client::get::<api::AgentsResponse>(data, "/v1/agents")
        .map_err(anyagents_core::Error::Message)?;
    let server_up = live_agents.is_some();
    let webhook_addr = std::fs::read_to_string(data.join("webhook.addr"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let public_addr = std::fs::read_to_string(data.join("public.addr"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let boundary = sandbox::boundary_of(data).ok();
    let has_operator_token = operator::operator_token(data).is_some();

    let provider_profiles = anyagents_core::Store::open(data.join("state.db"))
        .ok()
        .map(|s| create::sync_and_list_provider_profiles_with_options(&s, data, false))
        .unwrap_or_default();

    let catalog = anyagents_installer::Catalog::load(data)
        .unwrap_or_else(|_| anyagents_installer::Catalog::builtin());
    let mut harness_rows = Vec::new();
    for entry in catalog.harnesses() {
        let bin_path = install::program(data, &entry.id);
        let present = install::is_healthy(data, &entry.id) || bin_path.is_file();
        let version = install::recorded_version(data, &entry.id);
        let delivery = api::DeliveryView::from(&vendor::declared_delivery(&entry.id));
        let profiles: Vec<String> = provider_profiles
            .iter()
            .filter(|p| p.compatible_harnesses.iter().any(|h| h == &entry.id))
            .map(|p| format!("{}/{}", p.provider, p.profile))
            .collect();
        harness_rows.push(serde_json::json!({
            "id": entry.id,
            "installed": present,
            "version": version,
            "binary": bin_path.display().to_string(),
            "delivery": delivery,
            "profiles": profiles,
        }));
    }

    let listed = match live_agents {
        Some(resp) => resp,
        None => agent::offline_roster(data, &server::ListAgentsQuery::default())?,
    };
    let archived_count = listed.archived_count;
    let agents_list = listed.agents;
    // What needs the operator: agents parked after errored turns, and sends
    // whose delivery is unconfirmed (held, never resent automatically).
    let faulted: Vec<&api::AgentView> = agents_list
        .iter()
        .filter(|agent| agent.display_state() == "faulted")
        .collect();
    let unsettled: Vec<&api::UnsettledOutboundView> = agents_list
        .iter()
        .flat_map(|agent| agent.unsettled_outbound.iter())
        .collect();

    if json_out {
        let doc = serde_json::json!({
            "server": if server_up { "running" } else { "stopped" },
            "socket": sock.display().to_string(),
            "webhook_addr": webhook_addr,
            "public_addr": public_addr,
            "operator_token_present": has_operator_token,
            "boundary": boundary,
            "harnesses": harness_rows,
            "agents": agents_list,
            "archived_count": archived_count,
            "attention": {
                "faulted": faulted.iter().map(|agent| serde_json::json!({
                    "id": agent.id,
                    "name": agent.name,
                    "parent": agent.parent,
                    "turn_errors": agent.turn_errors,
                })).collect::<Vec<_>>(),
                "unsettled_outbound": unsettled,
            },
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&doc)
                .map_err(|e| anyagents_core::Error::Message(e.to_string()))?
        );
        return Ok(());
    }

    if ui::is_pretty() {
        let badge = if server_up {
            ui::bold_green("● running")
        } else {
            ui::red("○ stopped")
        };
        // Older daemons may not expose identity. Don't substitute the CLI's version.
        let version = if server_up {
            client::get::<serde_json::Value>(data, "/v1/webhooks/healthz")
                .ok()
                .flatten()
                .and_then(|identity| identity.get("version")?.as_str().map(str::to_owned))
                .map(|version| format!(" · {version}"))
                .unwrap_or_default()
        } else {
            String::new()
        };
        println!("\n  {}  {badge}{version}", ui::bold_cyan("Server"));
        if ui::is_verbose() {
            println!("  Socket: {}", sock.display());
            if let Some(addr) = webhook_addr.as_deref() {
                println!("  Webhook: {addr}");
            }
            if let Some(addr) = public_addr.as_deref() {
                println!("  Public listener: {addr}");
            }
            println!(
                "  Operator token: {}",
                if has_operator_token {
                    "ready"
                } else {
                    "missing"
                }
            );
        }
        let now = anyagents_core::now_ms();
        let attention = agents_list
            .iter()
            .filter(|agent| ui::agent_attention(agent, now).is_some());
        for (i, agent) in attention.enumerate() {
            if i == 0 {
                println!("\n  {}", ui::bold_cyan("Attention"));
            }
            println!(
                "  {}  {}",
                ui::bold(&agent.name),
                ui::format_status_with_alerts(agent, now)
            );
        }
        ui::print_agents_table_with_options(
            &agents_list,
            None,
            ui::AgentsTableOptions {
                title: "Departments",
                ..ui::AgentsTableOptions::default()
            },
        );
        if !unsettled.is_empty() {
            println!(
                "  {} unconfirmed sends · see anyagents outbound",
                unsettled.len()
            );
        }
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
        let delivery = delivery_label(&h["delivery"]);
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
        println!("  {id}\t{inst}\tdelivery={delivery}\tprofiles={profs}");
    }
    for agent in &faulted {
        println!(
            "attention\tfaulted\t{}\t{}\t{}",
            agent.name,
            agent.id,
            agent
                .turn_errors
                .as_ref()
                .and_then(|errors| errors.last_error.as_deref())
                .unwrap_or("-")
        );
    }
    for held in &unsettled {
        println!(
            "attention\tunconfirmed_send\t{}\t{}\t{}\t{}",
            held.key,
            held.agent_id,
            held.destination,
            held.error.as_deref().unwrap_or("-")
        );
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
            "  {}\t{}\tharness={}\tprofile={prof}\tstate={}\tprocess={}\tdelivery={}",
            a.name,
            a.id,
            a.harness,
            a.state,
            a.process,
            a.delivery.label()
        );
    }
    Ok(())
}

/// The delivery column of one harness row: its level, `+idempotent` when a
/// replay is recognised.
fn delivery_label(value: &serde_json::Value) -> String {
    serde_json::from_value::<api::DeliveryView>(value.clone())
        .map(|delivery| delivery.label())
        .unwrap_or_else(|_| "-".to_string())
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
            if ui::is_pretty() {
                ui::success(&format!("{} joined {group}", agent.name));
            } else {
                println!("{group}\t{}", agent.name);
            }
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
            if ui::is_pretty() {
                ui::success(&format!("{} left {group}", agent.name));
            } else {
                println!("{group}\t{}", agent.name);
            }
        }
        GroupsCommand::Members { group } => {
            let members = agent::group_members(data, &group)?;
            if ui::is_pretty() {
                let role_map: std::collections::HashMap<String, Option<String>> =
                    anyagents_core::Store::open(data.join("state.db"))
                        .ok()
                        .and_then(|s| s.list_agents().ok())
                        .unwrap_or_default()
                        .into_iter()
                        .map(|a| (a.name, a.role))
                        .collect();
                println!(
                    "\n  {}  {} members\n",
                    ui::bold_cyan(&format!("Group · {group}")),
                    ui::bold(&members.len().to_string())
                );
                for name in members {
                    let role = role_map.get(&name).and_then(|r| r.as_deref());
                    let display = match role.map(str::trim).filter(|s| !s.is_empty()) {
                        Some(r) => format!("{} {}", ui::bold(&name), ui::dim(&format!("· {r}"))),
                        None => ui::bold(&name),
                    };
                    println!("  • {display}");
                }
                println!();
                return Ok(());
            }
            for name in members {
                println!("{name}");
            }
        }
        GroupsCommand::List => {
            let list = agent::list_groups(data)?;
            if ui::is_pretty() {
                if list.is_empty() {
                    println!("  {}", ui::dim("No groups found."));
                    return Ok(());
                }
                let role_map: std::collections::HashMap<String, Option<String>> =
                    anyagents_core::Store::open(data.join("state.db"))
                        .ok()
                        .and_then(|s| s.list_agents().ok())
                        .unwrap_or_default()
                        .into_iter()
                        .map(|a| (a.name, a.role))
                        .collect();
                println!(
                    "\n  {}  {} total\n",
                    ui::bold_cyan("Groups"),
                    ui::bold(&list.len().to_string())
                );
                let mut table =
                    ui::Table::new(&[("GROUP", false), ("COUNT", true), ("MEMBERS", false)]);
                for (group, members) in list {
                    let formatted_members: Vec<String> = members
                        .iter()
                        .map(|m| {
                            let r = role_map.get(m).and_then(|v| v.as_deref());
                            create::format_agent_display(m, r)
                        })
                        .collect();
                    table.add_row(vec![
                        ui::bold(&ui::cyan(&group)),
                        ui::dim(&members.len().to_string()),
                        formatted_members.join(", "),
                    ]);
                }
                table.print();
                println!();
                return Ok(());
            }
            for (group, members) in list {
                println!("{group}\t{}", members.join(","));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) use cli_query_tests::test_tempdir;

#[cfg(test)]
mod cli_query_tests {
    use super::*;
    use clap::CommandFactory;

    /// Physical fixture roots keep system aliases (for example a temporary-root
    /// symlink) outside the boundary. Product readers still refuse all symlinks.
    pub(crate) fn test_tempdir() -> tempfile::TempDir {
        tempfile::tempdir_in(
            std::env::temp_dir()
                .canonicalize()
                .expect("physical temporary root"),
        )
        .expect("owned temporary fixture")
    }

    #[test]
    fn internal_commands_are_hidden_but_direct_help_still_parses() {
        let help = Cli::command().render_help().to_string();
        for command in [
            "agent-run",
            "fake-opencode",
            "sandbox-exec",
            "boundary",
            "flush",
            "harness",
            "mcp",
            "interact",
            "redact",
        ] {
            assert!(
                !help
                    .lines()
                    .any(|line| line.split_whitespace().next() == Some(command)),
                "{command}"
            );
            let Err(err) = Cli::try_parse_from(["anyagents", command, "--help"]) else {
                panic!("expected help for {command}");
            };
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp, "{command}");
        }
    }

    #[test]
    fn command_tree_and_message_aliases_parse() {
        Cli::command().debug_assert();
        for alias in [
            "conversation",
            "logs",
            "messages",
            "msg",
            "tail",
            "conv",
            "history",
        ] {
            assert!(matches!(
                Cli::try_parse_from(["anyagents", alias, "vulcan", "-n", "10"])
                    .unwrap()
                    .command,
                Some(Command::Conversation(_))
            ));
        }
        assert!(matches!(
            Cli::try_parse_from(["anyagents", "mail", "vulcan", "--unread"])
                .unwrap()
                .command,
            Some(Command::Inbox(_))
        ));
        for alias in ["ls", "ps"] {
            assert!(matches!(
                Cli::try_parse_from(["anyagents", alias]).unwrap().command,
                Some(Command::Peers { .. })
            ));
        }
        // `resume` takes a name, or `--faulted` for every parked agent.
        let faulted = Cli::try_parse_from(["anyagents", "resume", "--faulted"]).unwrap();
        assert!(matches!(
            faulted.command,
            Some(Command::Resume {
                faulted: true,
                name: None,
                ..
            })
        ));
        assert!(Cli::try_parse_from(["anyagents", "resume"]).is_err());
        assert!(Cli::try_parse_from(["anyagents", "resume", "vulcan", "--faulted"]).is_err());
        let inspected = Cli::try_parse_from(["anyagents", "inspect", "vulcan", "-n", "5"]).unwrap();
        assert!(matches!(
            inspected.command,
            Some(Command::Inspect { limit: 5, .. })
        ));
        for subcommand in [
            "logs",
            "messages",
            "conversation",
            "inbox",
            "inspect",
            "describe",
            "resume",
            "wake",
        ] {
            Cli::try_parse_from(["anyagents", "agent", subcommand, "vulcan"]).unwrap();
        }
        let cli = Cli::try_parse_from([
            "anyagents",
            "logs",
            "vulcan",
            "--sender",
            "alice",
            "--in",
            "--only-tools",
            "--tool",
            "bash",
            "--since",
            "10m",
            "--until",
            "2026-11-09T00:00:00Z",
            "--from-seq",
            "3",
            "--to-seq",
            "8",
            "--sort",
            "oldest",
            "--radius",
            "2",
            "--exclude",
            "noise",
            "--full",
            "--json",
        ])
        .unwrap();
        let Some(Command::Conversation(args)) = cli.command else {
            panic!()
        };
        assert!(args.options.inbound && args.options.full && args.options.json);
        assert_eq!(args.options.from_seq, Some(3));
        assert!(Cli::try_parse_from(["anyagents", "logs", "vulcan", "--in", "--out"]).is_err());
        assert!(Cli::try_parse_from(["anyagents", "mail", "vulcan", "--all", "-n", "2"]).is_err());
    }

    #[test]
    fn relative_and_absolute_time_parsing() {
        let now = 1_800_000_000_000;
        assert_eq!(parse_time_at("10m", now).unwrap(), now - 600_000);
        assert_eq!(parse_time_at("2h", now).unwrap(), now - 7_200_000);
        assert_eq!(parse_time_at("30s", now).unwrap(), now - 30_000);
        assert_eq!(parse_time_at("7d", now).unwrap(), now - 604_800_000);
        assert_eq!(
            parse_time_at("2026-11-09T00:00:00Z", now).unwrap(),
            1_794_182_400_000
        );
        assert_eq!(
            parse_time_at("2026-11-09T02:00:00+02:00", now).unwrap(),
            1_794_182_400_000
        );
        assert_eq!(
            parse_time_at("1794182400000", now).unwrap(),
            1_794_182_400_000
        );
        for invalid in [
            "nope",
            "-2h",
            "9999999999999999999d",
            "2026-13-01T00:00:00Z",
        ] {
            assert!(parse_time_at(invalid, now).is_err(), "{invalid}");
        }
    }
}
