//! MCP stdio server bound to one agent.
//!
//! An agent's tools reach the server through the one mailbox capability the
//! server minted for that agent's session. The capability is read from a file
//! in the agent's own config directory, which the CLI is told once as an
//! argument; it is never an argument itself, never a tool argument and never
//! printed. The server decides whose mailbox a call reads and who a message is
//! from, so a name the model writes can only choose a destination.

use std::path::{Path, PathBuf};

use anyagents_core::{AgentId, Error};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{ServiceExt, tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::api::{
    AgentView, AgentsResponse, ConversationResponse, CreateAgentRequest, GroupActionRequest,
    GroupView, GroupsResponse, InteractRequest, InteractResponse, SendArticle, SendChecklist,
    SendLocation, SendMedia, SendPoll, SendRequest, SendResponse,
};
use crate::operator::CAPABILITY_FILE;
use crate::server;

/// The agent whose tools run here: the id and name the server resolved from
/// the capability, never a name the caller wrote.
#[derive(Clone, Debug)]
pub struct AgentRef {
    pub id: AgentId,
    pub name: String,
    pub workdir: Option<PathBuf>,
}

/// Run the tool server for the agent whose config directory is `config`. The
/// server names the directory it started this agent with as
/// `ANYAGENTS_AGENT_CONFIG`; a command line that omits it is read from there,
/// and from nowhere else.
pub fn run(data: &Path, config: Option<&Path>) -> Result<(), Error> {
    let owned;
    let config = match config {
        Some(config) => config,
        None => {
            owned = std::env::var_os("ANYAGENTS_AGENT_CONFIG")
                .map(PathBuf::from)
                .ok_or_else(|| {
                    Error::Message(
                        "the tool server was given no config directory and its environment names none"
                            .into(),
                    )
                })?;
            &owned
        }
    };
    let socket = server::socket_path(data);
    let capability = capability_of(config)?;
    let config_buf = config.to_path_buf();
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let client = Client {
            socket,
            config: config_buf,
            capability,
        };
        let who = client.whoami().await.map_err(Error::Message)?;
        // A harness names the agent id it was given; the server already
        // answered with the agent this session acts as, and the two may not
        // differ.
        if let Ok(named) = std::env::var("ANYAGENTS_AGENT_ID")
            && named.trim() != who.id.as_str()
        {
            return Err(Error::Message(
                "this config directory belongs to another agent".into(),
            ));
        }
        let service = AgentMcp { agent: who, client }
            .serve(rmcp::transport::io::stdio())
            .await
            .map_err(|err| Error::Message(err.to_string()))?;
        service
            .waiting()
            .await
            .map_err(|err| Error::Message(err.to_string()))?;
        Ok(())
    })
}

/// The capability an agent's harness reads: a file only that agent's own
/// config directory holds.
fn capability_of(config: &Path) -> Result<String, Error> {
    let path = config.join(CAPABILITY_FILE);
    let text = std::fs::read_to_string(&path).map_err(|err| {
        Error::Message(format!(
            "this agent has no mailbox capability at {}: {err}",
            path.display()
        ))
    })?;
    let capability = text.trim().to_string();
    if capability.is_empty() {
        return Err(Error::Message(
            "this agent's mailbox capability is empty".into(),
        ));
    }
    Ok(capability)
}

#[derive(Clone)]
struct Client {
    socket: PathBuf,
    config: PathBuf,
    capability: String,
}

impl Client {
    fn current_capability(&self) -> String {
        capability_of(&self.config).unwrap_or_else(|_| self.capability.clone())
    }

    /// The agent this capability authenticates, as the server resolved it.
    async fn whoami(&self) -> Result<AgentRef, String> {
        let named = crate::client::agent_identity().map(|id| id.to_string());
        let cap = self.current_capability();
        let who: serde_json::Value = request_naming(
            &self.socket,
            "GET",
            "/v1/whoami",
            None,
            Some(&cap),
            None,
            named.as_deref(),
        )
        .await
        .and_then(|(status, bytes)| {
            if status.is_success() {
                serde_json::from_slice(&bytes).map_err(|err| err.to_string())
            } else {
                Err(error_message(&bytes))
            }
        })?;
        let id = who
            .get("agent_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "the server named no agent".to_string())?;
        let name = who
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "the server named no agent".to_string())?;
        let workdir = who
            .get("workdir")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        Ok(AgentRef {
            id: AgentId::parse(id).map_err(|err| err.to_string())?,
            name: name.to_string(),
            workdir,
        })
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, String> {
        let method = if body.is_some() { "POST" } else { "GET" };
        self.request_method(method, path, body).await
    }

    async fn request_method<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, String> {
        let named = crate::client::agent_identity().map(|id| id.to_string());
        let cap = self.current_capability();
        let (status, bytes) = request_naming(
            &self.socket,
            method,
            path,
            body,
            Some(&cap),
            None,
            named.as_deref(),
        )
        .await?;
        if !status.is_success() {
            return Err(error_message(&bytes));
        }
        serde_json::from_slice(&bytes).map_err(|err| err.to_string())
    }

    async fn get_agents(&self) -> Result<AgentsResponse, String> {
        self.get("/v1/agents", None).await
    }
}

#[derive(Clone)]
struct AgentMcp {
    agent: AgentRef,
    client: Client,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SendArgs {
    /// Destinations: agent names or IDs (`planner`, `agent:planner`, `agt_...`, `parent`,
    /// `children`), internal groups (`group:owners`, `group:<name>`, `group:all`, `all`),
    /// or external channel targets (`telegram:chat:<chat_id>[:<topic_id>]`,
    /// `telegram:user:<user_id>[:<message_id>]`). An authored send addressed to
    /// exactly one archived agent by name or `agt_...` ID wakes its session.
    /// Broadcasts, channel updates and system events do not. May be
    /// empty (`[]`) when `reply_to` names a mailbox reference (`anyagents:reply:<msg_id>`)
    /// or when performing a message interaction (`action`: `"react"` | `"vote"` | `"edit"` | `"delete"` | `"pin"` | `"unpin"` | `"stop_poll"`).
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    to: Vec<String>,
    /// Message text or caption (Markdown is automatically rendered as rich HTML on Telegram).
    /// Also used as new message text when `action = "edit"` or optional comment when `action = "vote"`.
    #[serde(default)]
    text: String,
    /// Optional reply or interaction target: `anyagents:reply:<msg_id>` from the message header
    /// (answers or reacts in the same chat/topic/agent), or a plain destination address.
    #[serde(default)]
    reply_to: Option<String>,
    /// Optional quoted substring or message text to attach as a quote.
    #[serde(default)]
    quote: Option<String>,
    /// Optional media attachment (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`)
    /// by local file path, `https://` URL, or Telegram `file_id` (plus optional `title` and `performer` for `music`/`audio`).
    #[serde(default)]
    media: Option<SendMedia>,
    /// Optional GPS coordinates or venue (`latitude`, `longitude`, optional `title` and `address`).
    #[serde(default)]
    location: Option<SendLocation>,
    /// Optional interactive poll (`question`, `options`, optional `is_anonymous` and `allows_multiple_answers`).
    #[serde(default)]
    poll: Option<SendPoll>,
    /// Optional interactive checklist (`title`, `tasks`, optional `business_connection_id`).
    #[serde(default)]
    checklist: Option<SendChecklist>,
    /// Optional rich Instant View article (`title`, `content` or `url`, optional `author_name`).
    #[serde(default)]
    article: Option<SendArticle>,
    /// Optional message interaction action (`"send"` default, or `"react"`, `"vote"`, `"check"`, `"uncheck"`,
    /// `"edit"`, `"delete"`, `"pin"`, `"unpin"`, `"stop_poll"` on an existing message identified by `target` or `reply_to`).
    #[serde(default)]
    action: Option<String>,
    /// Target message for interactions (`anyagents:reply:msg_...`, `msg_...`, or `telegram:chat:<chat_id>:<message_id>`).
    /// Falls back to `reply_to` if omitted.
    #[serde(default)]
    target: Option<String>,
    /// Emoji reaction when `action = "react"` (e.g. `"👍"`, `"🔥"`, `"❤️"`, `"🎉"`, or `""` to clear).
    #[serde(default)]
    emoji: Option<String>,
    /// Selected poll option(s) when `action = "vote"` (1-based option numbers `"1"`, `"2"` or option text)
    /// or checklist item numbers when `action = "check" | "uncheck"`.
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    options: Vec<String>,
    /// Names this send. Send it again with the same value only when an earlier
    /// attempt got no answer.
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct ConversationArgs {
    /// Opaque indexed result ID for exact raw readback, never a file path.
    #[serde(default)]
    raw_id: Option<String>,
    /// Expected whole-event SHA-256 from the result's `raw.sha256`; required with raw_id.
    #[serde(default)]
    raw_sha256: Option<String>,
    /// Byte offset relative to the selected whole raw event/document (default 0).
    #[serde(default)]
    raw_offset: Option<u64>,
    /// Requested bytes 1..=65536 (default 65536), clipped at the event end.
    #[serde(default)]
    raw_length: Option<u64>,
    /// Search both sources (default), only learned memory, or only transcripts.
    #[serde(default)]
    scope: Option<String>,
    /// Optional memory topic slug (e.g. `"architecture-decisions"`, `"billing-rules"`) to write,
    /// upsert, or delete in `<agent_root>/memory/<topic>.md` and automatically rebuild `MEMORY.md`
    /// without needing host paths. When set, `scope` defaults to `"memory"` and `agent` defaults to `self`.
    #[serde(default)]
    topic: Option<String>,
    /// Optional 1-line description / trigger phrase for `topic` when upserting learned memory.
    #[serde(default)]
    description: Option<String>,
    /// Markdown body to write into `<agent_root>/memory/<topic>.md` when upserting learned memory.
    #[serde(default, alias = "body", alias = "text")]
    content: Option<String>,
    /// When `true` with `topic`, deletes `<agent_root>/memory/<topic>.md` and rebuilds `MEMORY.md`.
    #[serde(default)]
    delete: Option<bool>,
    /// Optional lexical + structured search query (`Raw-RAG`: SQLite FTS5 BM25 with hybrid
    /// ASCII identifier + CJK bigram tokenization, exact phrase boost, and substring fallback).
    /// When set, searches authorized historical and live turns without loading the
    /// full history. Omit `query` to read recent chronological turns or filter by tool/range.
    #[serde(default, alias = "q", alias = "search")]
    query: Option<String>,
    /// Optional array (or comma-separated list) of additional search queries / synonyms
    /// OR-combined with `query` in a single turn.
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    queries: Vec<String>,
    /// Optional negative terms — any turn containing any of these substrings is excluded.
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    exclude: Vec<String>,
    /// Agent name (`planner`, `builder`, `reviewer`, `"all"`) or `agt_...` ID whose complete
    /// two-way (`in` + `out`) conversation to read or search. Defaults to `"all"` when
    /// searching, or this agent when reading recent chronological turns. Non-admin
    /// callers can read only self and descendants; `all` means that authorized set.
    #[serde(default)]
    agent: Option<String>,
    /// Optional peer filter (e.g. `alice`, `telegram:user:42`, `telegram:chat:-1001234567890`,
    /// `group:owners`, or `agent:beta`).
    #[serde(default)]
    with: Option<String>,
    /// Optional direction filter (`"in"`, `"out"`, or `"all"`).
    #[serde(default)]
    direction: Option<String>,
    /// Optional sender substring filter.
    #[serde(default)]
    sender: Option<String>,
    /// Whether to include tool/system history (`true` by default). Set `false` to search
    /// only human/agent conversation messages without tool/schedule/sub-agent events.
    #[serde(default)]
    include_tools: Option<bool>,
    /// When `true`, searches ONLY tool/system history (`kind = "tool"`).
    #[serde(default)]
    only_tools: Option<bool>,
    /// Optional tool name filter (e.g. `"browser"`, `"agents"`, `"schedule"`, `"send"`).
    #[serde(default)]
    tool_name: Option<String>,
    /// Optional lower timestamp bound in milliseconds (`timestamp_ms > since_ms`).
    #[serde(default)]
    since_ms: Option<i64>,
    /// Optional upper timestamp bound in milliseconds (`timestamp_ms <= until_ms`).
    #[serde(default)]
    until_ms: Option<i64>,
    /// Optional sequence range lower bound (`seq >= from_seq`).
    #[serde(default)]
    from_seq: Option<i64>,
    /// Optional sequence range upper bound (`seq <= to_seq`).
    #[serde(default)]
    to_seq: Option<i64>,
    /// Optional sequence range shorthand (`"10-25"` or `"seq:10-25"`).
    #[serde(default)]
    range: Option<String>,
    /// Result ordering: `"relevance"` (default for search), `"newest"` (`timestamp_ms DESC`),
    /// or `"oldest"` (`timestamp_ms ASC`).
    #[serde(default)]
    sort: Option<String>,
    /// Contiguous neighbor turns (`0..=10`) to include around each matched hit so causal
    /// `(Trigger -> Action -> Outcome)` chains stay intact.
    #[serde(default)]
    radius: Option<usize>,
    /// Optional maximum number of turns to return (defaults to 20 when searching,
    /// or 50 for chronological history).
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct AgentsArgs {
    /// Action to perform:
    /// - `"list"` (default: list agents and sub-agents with optional `state` and `parent` filters)
    /// - `"inspect"` / `"status"` (inspect an agent or sub-agent's live status, schedules, and recent conversation turns)
    /// - `"spawn"` (spawn a new sub-agent under this agent, with optional `repo` / `branch` / `base` / `workdir` for an isolated Git worktree)
    /// - `"create"` (admin-only: create a standalone root agent with no parent)
    /// - `"describe"` (read or update an agent's role & responsibility `description` / `instructions` and short `role` label; defaults `name` to `"self"`)
    /// - `"steer"` / `"send"` (send/steer a follow-up `prompt` into an agent or sub-agent, waking it if archived)
    /// - `"wake"` / `"resume"` (wake an archived or paused agent, with optional `prompt`)
    /// - `"reset_session"` / `"new_session"` (new session for a quiescent managed agent; preserves old pins and raw state)
    /// - `"pause"` (pause an agent)
    /// - `"archive"` (any agent, including roots and labelled children, may self-archive; preserves browser profiles; auto-reaps clean/pushed managed Git worktrees while preserving dirty ones)
    /// - `"delete"` / `"kill"` (permanently delete a sub-agent or agent)
    /// - `"rename"` (admin-only: rename an agent in place while keeping its `agt_...` ID and old name as an alias)
    /// - `"role"` (set or clear an agent's short role label)
    /// - `"harness"` (declaratively update an agent's harness/model/profile)
    /// - `"models"` (list upstream provider profile model catalogs)
    #[serde(default)]
    action: Option<String>,
    /// Target body id when action = "redact"; `transcript:<zero-based-line>` addresses
    /// a Sylphx output/content string in config/sylphx-transcript.jsonl.
    #[serde(default)]
    message_id: Option<String>,
    /// Mailbox sequence instead of message_id when action = "redact".
    #[serde(default)]
    seq: Option<i64>,
    /// Optional end-exclusive decoded-text span. Omit to tombstone the whole body.
    #[serde(default)]
    span: Option<crate::api::RedactionSpan>,
    /// Required audit reason for redact. Never repeat sensitive text in this field.
    #[serde(default)]
    reason: Option<String>,
    /// Optional state filter for `list`: `"running"`, `"working"`, `"errored"`, `"idle"`, `"paused"`, `"archived"`, or `"all"` (default `"all"`).
    #[serde(default)]
    state: Option<String>,
    /// Optional parent filter for `list`: `"self"` / `"children"` (direct sub-agents of this agent),
    /// `"subagents"` (all sub-agents across all parents), `"root"` (root agents with no parent), or an agent name/`agt_...` ID.
    #[serde(default)]
    parent: Option<String>,
    /// Optional case-insensitive multi-token search query for `list`: every whitespace-separated token must match `name`, `id`, `role`, `description`, `workdir`, `harness`, `model`, or `aliases`.
    #[serde(default, alias = "search", alias = "filter", alias = "q")]
    query: Option<String>,
    /// Target agent name or `agt_...` ID (`"self"`, `"all"`, or a peer/sub-agent name; optional on `spawn` where a `<caller>-sub-<id>` name is auto-generated if omitted).
    #[serde(
        default,
        alias = "agent",
        alias = "id",
        alias = "agent_id",
        alias = "target"
    )]
    name: Option<String>,
    /// New agent name when `action = "rename"`.
    #[serde(default, alias = "to")]
    new_name: Option<String>,
    /// Optional archetype template name (`.agents/agents/<archetype>.md`, e.g. `"writer"`, `"reviewer"`, `"mind"`) when `action` is `"spawn"` or `"create"`.
    #[serde(default, alias = "type", alias = "template", alias = "preset")]
    archetype: Option<String>,
    /// Optional display-only role label (for example, `"Researcher"`); never selects lifecycle or scope.
    /// when `action` is `"spawn"`, `"create"`, `"describe"`, or `"role"`.
    #[serde(default)]
    role: Option<String>,
    /// Set `true` to clear the description or role label when `action` is `"describe"` or `"role"`.
    #[serde(default)]
    clear: Option<bool>,
    /// Harness catalog ID when `action` is `"spawn"`, `"create"`, or `"harness"` (`sylphx`, `opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`, or `"inherit"`).
    #[serde(default)]
    harness: Option<String>,
    /// Optional model override when `action` is `"spawn"`, `"create"`, or `"harness"`.
    #[serde(default)]
    model: Option<String>,
    /// Optional upstream provider account profile when `action` is `"spawn"`, `"create"`, or `"harness"`.
    #[serde(default)]
    profile: Option<String>,
    /// Optional repository (`"<org>/<repo>"`, local repo path, or `"inherit"` to branch from the parent's repo) when `action` is `"spawn"` or `"create"` to automatically provision an isolated, lifecycle-managed Git worktree at `<data>/agents/<id>/work`.
    #[serde(default)]
    repo: Option<String>,
    /// Optional Git branch name for the managed worktree when `repo` is provided (defaults to `agent/<name>`).
    #[serde(default)]
    branch: Option<String>,
    /// Optional starting Git ref/branch when `repo` is provided (defaults to `origin/HEAD` or parent `HEAD`).
    #[serde(default)]
    base: Option<String>,
    /// Optional workspace path or mode: `"worktree"` / `"branch"` (isolated Git worktree from the parent repo), `"private"` (clean isolated `<data>/agents/<id>/work`), or `"inherit"` (explicit parent directory sharing). When both `repo` and `workdir` are omitted, a sub-agent gets its own private directory. Root creation may use the operator-configured working directory.
    #[serde(default)]
    workdir: Option<String>,
    /// Concise public role/mission summary (`description`, Layer 1 — visible to all peers and group rosters) when `action` is `"spawn"`, `"create"`, or `"describe"`.
    #[serde(default)]
    description: Option<String>,
    /// Detailed private standing instructions (`instructions`, Layer 2 — injected only into the agent's own private system prompt) when `action` is `"spawn"`, `"create"`, or `"describe"`.
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    instructions: Option<String>,
    /// Internal communication & visibility scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`) when `action` is `"spawn"`, `"create"`, or `"describe"`.
    #[serde(default)]
    scope: Option<String>,
    /// Whether this sub-agent accepts external messages (`true`) from agents outside its direct parent/child lineage (and allows outside agents to invite it to virtual groups). Defaults to `false` (`private`) when spawning a sub-agent.
    #[serde(default, alias = "allow_external")]
    public: Option<bool>,
    /// Optional internal virtual group name(s) (comma-separated, e.g. `"auth-migration"`) to atomically join both the parent and the spawned sub-agent into upon creation.
    #[serde(default)]
    group: Option<String>,
    /// Task prompt or follow-up message when `action` is `"spawn"`, `"create"`, `"steer"`, `"send"`, or `"wake"`.
    #[serde(default, alias = "text", alias = "message")]
    prompt: Option<String>,
    /// Suggested Telegram `@username` (or existing spare bot `@username`) when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(
        default,
        alias = "bot",
        alias = "suggested_username",
        alias = "username"
    )]
    bot_username: Option<String>,
    /// Telegram bot user ID (`getManagedBotToken`) when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(default, alias = "managed_bot_id", alias = "user_id")]
    bot_id: Option<i64>,
    /// Optional raw Telegram bot HTTP API token when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(default, alias = "bot_token")]
    token: Option<String>,
    /// Zero-based position in the deterministically sorted list. Continue with
    /// `next_cursor` and the same query, parent and state; this is not a snapshot.
    #[serde(default, alias = "offset")]
    cursor: Option<usize>,
    /// Page size for list (default 100, clamped to 1..=100), or recent turns for inspect (default 10).
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SpawnArgs {
    /// Optional short agent name (`[a-z0-9][a-z0-9_-]*`). If omitted, a unique
    /// `<caller>-sub-<id>` name is generated automatically.
    #[serde(
        default,
        alias = "agent",
        alias = "id",
        alias = "agent_id",
        alias = "target"
    )]
    name: Option<String>,
    /// Optional archetype template name (`.agents/agents/<archetype>.md`, e.g. `"writer"`, `"reviewer"`, `"mind"`).
    #[serde(default, alias = "type", alias = "template", alias = "preset")]
    archetype: Option<String>,
    /// Optional display-only role label; no effect on lifecycle, scope or prompts.
    #[serde(default)]
    role: Option<String>,
    /// Harness catalog ID (`opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`).
    /// Defaults to the caller's own harness (or `claude-code` when the caller runs on `agy`).
    #[serde(default)]
    harness: Option<String>,
    /// Optional model override (e.g. `sylphx/openai/gpt-6.1-sol#xhigh`, `claude-opus-5-5#high`).
    #[serde(default)]
    model: Option<String>,
    /// Optional upstream provider account profile (e.g. `acct-a`, `openai/default`, `sylphx/default`).
    #[serde(default)]
    profile: Option<String>,
    /// Optional repository (`"<org>/<repo>"`, local repo path, or `"inherit"` to branch from the parent's repo) to automatically provision an isolated, lifecycle-managed Git worktree as the sub-agent's `workdir`.
    #[serde(default)]
    repo: Option<String>,
    /// Optional Git branch name to create in the managed worktree when `repo` is provided (defaults to `agent/<name>`).
    #[serde(default)]
    branch: Option<String>,
    /// Optional starting Git ref/branch for the managed worktree when `repo` is provided (defaults to `origin/HEAD` or parent `HEAD`).
    #[serde(default)]
    base: Option<String>,
    /// Optional workspace path or mode: `"worktree"` / `"branch"` (isolated Git worktree from the parent repo), `"private"` (clean isolated `<data>/agents/<id>/work`), or `"inherit"` (explicit parent directory sharing). When both `repo` and `workdir` are omitted, a sub-agent gets its own private directory. Root creation may use the operator-configured working directory.
    #[serde(default)]
    workdir: Option<String>,
    /// Optional concise public description (`description`, Layer 1) for the spawned agent.
    #[serde(default)]
    description: Option<String>,
    /// Optional detailed private standing instructions (`instructions`, Layer 2) for the spawned agent.
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    instructions: Option<String>,
    /// Optional internal communication & visibility scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`).
    #[serde(default)]
    scope: Option<String>,
    /// Whether this sub-agent accepts external messages (`true`) from agents outside its direct parent/child lineage. Defaults to `false` (`private`).
    #[serde(default, alias = "allow_external")]
    public: Option<bool>,
    /// Optional internal virtual group name(s) (comma-separated, e.g. `"auth-migration"`) to atomically join both the parent and the spawned sub-agent into upon creation.
    #[serde(default)]
    group: Option<String>,
    /// Optional initial task/message to deliver immediately to the spawned agent.
    #[serde(default)]
    prompt: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct DescribeArgs {
    /// Target agent name (`"self"`, `"reviewer"`, `"planner"`, `"all"`, or `agt_...` ID). Defaults to `"self"`.
    #[serde(default, alias = "name", alias = "target")]
    agent: Option<String>,
    /// Concise public role/responsibility summary (`description`, Layer 1 — visible to all peers and group rosters).
    /// Omit to read the current description.
    #[serde(default, alias = "text")]
    description: Option<String>,
    /// Detailed private standing instructions (`instructions`, Layer 2 — injected only into the agent's own private system prompt).
    #[serde(default, alias = "instruction", alias = "system_prompt")]
    instructions: Option<String>,
    /// Optional display-only role label; no effect on lifecycle, scope or prompts.
    #[serde(default)]
    role: Option<String>,
    /// Optional internal communication & visibility scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`).
    #[serde(default)]
    scope: Option<String>,
    /// Optional toggle (`true` or `false`) for whether a sub-agent accepts external messages from agents outside its direct parent/child lineage.
    #[serde(default, alias = "allow_external")]
    public: Option<bool>,
    /// Set `true` to clear the agent's custom description/instructions.
    #[serde(default)]
    clear: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SubagentsArgs {
    /// Action to perform: `"list"` (default: list sub-agents and their live status),
    /// `"inspect"` / `"status"` (inspect a sub-agent's live status + recent conversation turns),
    /// `"spawn"` (create and start a new cross-harness sub-agent),
    /// `"steer"` / `"send"` (send/steer a follow-up prompt into a sub-agent, waking it if archived),
    /// `"wake"` / `"resume"` (wake an archived or paused sub-agent, with optional `prompt`),
    /// `"pause"` (pause a sub-agent), `"archive"` (archive a sub-agent to scale to zero),
    /// or `"delete"` / `"kill"` (permanently delete a sub-agent).
    #[serde(default)]
    action: Option<String>,
    /// Target sub-agent name or `agt_...` ID (for `inspect`, `steer`, `wake`, `pause`, `archive`, `delete`, or `name` for `spawn`).
    #[serde(
        default,
        alias = "id",
        alias = "name",
        alias = "agent_id",
        alias = "target"
    )]
    agent: Option<String>,
    /// Optional state filter for `list`: `"running"`, `"paused"`, `"archived"`, or `"all"` (default `"all"`).
    #[serde(default)]
    state: Option<String>,
    /// Optional parent filter for `list`: `"self"` (default: this agent's sub-agents), `"all"` (all sub-agents across all parents), or a parent agent name/ID.
    #[serde(default)]
    parent: Option<String>,
    /// Optional case-insensitive multi-token search query for `list`.
    #[serde(default, alias = "search", alias = "filter", alias = "q")]
    query: Option<String>,
    /// Optional archetype template name (`.agents/agents/<archetype>.md`, e.g. `"writer"`, `"reviewer"`, `"mind"`) when `action = "spawn"`.
    #[serde(default, alias = "type", alias = "template", alias = "preset")]
    archetype: Option<String>,
    /// Harness catalog ID when `action = "spawn"` (`opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`).
    #[serde(default)]
    harness: Option<String>,
    /// Optional model override when `action = "spawn"`.
    #[serde(default)]
    model: Option<String>,
    /// Optional account profile when `action = "spawn"`.
    #[serde(default)]
    profile: Option<String>,
    /// Optional repository (`"<org>/<repo>"`, local repo path, or `"inherit"`) when `action = "spawn"` to automatically provision an isolated, lifecycle-managed Git worktree.
    #[serde(default)]
    repo: Option<String>,
    /// Optional Git branch name for the managed worktree when `action = "spawn"` and `repo` is set (defaults to `agent/<name>`).
    #[serde(default)]
    branch: Option<String>,
    /// Optional starting Git ref/branch when `action = "spawn"` and `repo` is set (defaults to `origin/HEAD` or parent `HEAD`).
    #[serde(default)]
    base: Option<String>,
    /// Optional workspace path or mode: `"worktree"` / `"branch"` (isolated Git worktree from the parent repo), `"private"` (clean isolated `<data>/agents/<id>/work`), or `"inherit"` (explicit parent directory sharing). When both `repo` and `workdir` are omitted, a sub-agent gets its own private directory. Root creation may use the operator-configured working directory.
    #[serde(default)]
    workdir: Option<String>,
    /// Optional concise public description when `action = "spawn"`.
    #[serde(default)]
    description: Option<String>,
    /// Optional private system/role instructions when `action = "spawn"`.
    #[serde(
        default,
        alias = "role",
        alias = "instruction",
        alias = "system_prompt"
    )]
    instructions: Option<String>,
    /// Optional internal communication & visibility scope (`"isolated"` | `"private"` | `"siblings"` | `"public"`).
    #[serde(default)]
    scope: Option<String>,
    /// Whether this sub-agent accepts external messages (`true`) from agents outside its direct parent/child lineage. Defaults to `false` (`private`).
    #[serde(default, alias = "allow_external")]
    public: Option<bool>,
    /// Optional internal virtual group name(s) (comma-separated) to atomically join both the parent and the spawned sub-agent into upon creation.
    #[serde(default)]
    group: Option<String>,
    /// Task prompt or follow-up message when `action` is `"spawn"`, `"steer"`, `"send"`, `"wake"`, or `"resume"`.
    #[serde(default, alias = "text", alias = "message")]
    prompt: Option<String>,
    /// Suggested Telegram `@username` (or existing spare bot `@username`) when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(
        default,
        alias = "bot",
        alias = "suggested_username",
        alias = "username"
    )]
    bot_username: Option<String>,
    /// Telegram bot user ID (`getManagedBotToken`) when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(default, alias = "managed_bot_id", alias = "user_id")]
    bot_id: Option<i64>,
    /// Optional raw Telegram bot HTTP API token when `action = "telegram_bot"` / `"bind_telegram"` / `"channel"`.
    #[serde(default, alias = "bot_token")]
    token: Option<String>,
    /// Zero-based position in the deterministically sorted list. Continue with
    /// `next_cursor` and the same query, parent and state; this is not a snapshot.
    #[serde(default, alias = "offset")]
    cursor: Option<usize>,
    /// Page size for list (default 100, clamped to 1..=100), or recent turns for inspect (default 10).
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct ArchiveArgs {
    /// Agent name or `agt_...` ID to archive (scales process to zero while keeping
    /// its permanent `agt_...` ID, mailbox, and session on disk so an intentional
    /// direct `send` or `wake` call resumes it). Defaults to this agent itself when omitted.
    #[serde(
        default,
        alias = "id",
        alias = "name",
        alias = "agent_id",
        alias = "target"
    )]
    agent: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct WakeArgs {
    /// Agent name or `agt_...` ID to wake and resume from `archived` or `paused` state.
    #[serde(alias = "id", alias = "name", alias = "agent_id", alias = "target")]
    agent: String,
    /// Optional follow-up prompt/message to deliver immediately after waking.
    #[serde(default, alias = "text", alias = "message")]
    prompt: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct GroupsArgs {
    /// Action: `"list"` (list all groups and members), `"members"` (list one group's
    /// members), `"join"` (create/join an internal group), or `"leave"` (leave a group).
    #[serde(default)]
    action: Option<String>,
    /// Group name (e.g. `"owners"`, `"review-team"`). Required for `"join"`, `"leave"`, `"members"`.
    #[serde(default, alias = "name")]
    group: Option<String>,
    /// Optional agent name(s) or `agt_...` ID(s) to add/remove (comma-separated, defaults to this agent). When creating a new group and adding another agent, the creator is automatically joined too.
    #[serde(default)]
    agent: Option<String>,
    /// Optional list of agent names or `agt_...` IDs to add/remove in a single batch call.
    #[serde(
        default,
        alias = "members",
        deserialize_with = "crate::api::deserialize_string_or_vec"
    )]
    agents: Vec<String>,
    /// Optional group visibility toggle: `false` (default = Private / Invite-Only group; participation requires membership or an invitation) or `true` (Public / Open group).
    #[serde(default, alias = "allow_external")]
    public: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct InteractArgs {
    /// Channel/message action: `"react"` (set/clear emoji reaction on a message),
    /// `"vote"` (cast a ballot on a poll), `"edit"` (edit message text),
    /// `"delete"` (delete a message), `"pin"` (pin a message), `"unpin"` (unpin a message),
    /// or `"stop_poll"` (close an active poll).
    action: String,
    /// Target message: `anyagents:reply:msg_...` from the message header, `msg_...`,
    /// or `telegram:chat:<chat_id>:<message_id>`.
    target: String,
    /// Emoji reaction when `action = "react"` (e.g. `"👍"`, `"🔥"`, `"❤️"`, `"🎉"`, or `""` to clear).
    #[serde(default)]
    emoji: Option<String>,
    /// Selected poll option(s) when `action = "vote"` (1-based option numbers `"1"`, `"2"` or option text).
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    options: Vec<String>,
    /// New message text when `action = "edit"`, or optional rationale/comment when `action = "vote"`.
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct ScheduleArgs {
    /// Action: `"set"` (create a one-off `at`, recurring `every`, or event `watch` wake),
    /// `"list"` (list active schedules), or `"cancel"` (cancel a schedule by `id` or `"all"`).
    /// Inferred automatically when `at`, `every`, `watch`, or `id` is provided.
    #[serde(default)]
    action: Option<String>,
    /// One-off wake time: relative duration (`"1m"`, `"30s"`, `"2h"`) or ISO-8601 / Unix timestamp (`"2026-10-29T16:30:00Z"`).
    #[serde(default)]
    at: Option<String>,
    /// Recurring wake interval (`"1m"`, `"5m"`, `"15m"`, `"30m"`, `"1h"`). Fires only when idle and not quota-blocked; a missed wake never double-fires.
    #[serde(default)]
    every: Option<String>,
    /// Event watch specification:
    /// - `"github:pr:<owner>/<repo>#<num>"` (GitHub PR reaching a terminal state: merged/closed or checks terminal)
    /// - `"github:check:<owner>/<repo>@<ref>"` (GitHub check runs reaching a terminal state)
    /// - `"spiron:promise:<id>"` (Spiron promise state change pulled from the public API)
    /// - `"agent:idle:<name_or_id>"` (AnyAgents agent transitioning to idle)
    /// - `"pid:<pid>"` / `"process:<pid>"` (local background process PID exiting)
    /// - `"file:<path>"` / `"path:<path>"` (local file/directory created or modified)
    #[serde(default)]
    watch: Option<String>,
    /// Prompt delivered to the agent when the schedule or event watch fires.
    #[serde(default, alias = "text", alias = "message")]
    prompt: Option<String>,
    /// Schedule ID (`sch_...`) or `"all"` when `action = "cancel"`.
    #[serde(default, alias = "schedule_id")]
    id: Option<String>,
    /// Optional target agent name or `agt_...` ID (defaults to this agent; owners/parents can also inspect or manage a peer/child's schedules).
    #[serde(default, alias = "name", alias = "target")]
    agent: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct MemoryArgs {
    /// Learned-memory operation to perform:
    /// - `"list"` (default when no `topic`, `query`, `content`, or `delete` is passed): Read `MEMORY.md` and list all topic entries.
    /// - `"read"` (inferred when `topic` is provided without `content` or `delete`): Read `<agent_root>/memory/<topic>.md` in O(1).
    /// - `"write"` / `"upsert"` (inferred when `content` is provided): Create or update `<agent_root>/memory/<topic>.md` and rebuild `MEMORY.md`.
    /// - `"delete"` (inferred when `delete: true` is provided): Delete `<agent_root>/memory/<topic>.md` and rebuild `MEMORY.md`.
    /// - `"search"` (inferred when `query` is provided): Search the FTS5 learned-memory index.
    #[serde(default)]
    action: Option<String>,
    /// Learned-memory topic slug (`<agent_root>/memory/<topic>.md`, e.g. `"deployment-runbook"`).
    #[serde(default, alias = "slug", alias = "name")]
    topic: Option<String>,
    /// 1-line trigger description indexed in `MEMORY.md` when `action = "write"` / `"upsert"`.
    #[serde(default, alias = "summary", alias = "trigger")]
    description: Option<String>,
    /// Markdown body to write into `<agent_root>/memory/<topic>.md` when `action = "write"` / `"upsert"`.
    #[serde(default, alias = "body", alias = "text", alias = "markdown")]
    content: Option<String>,
    /// Search query when `action = "search"`.
    #[serde(default, alias = "q", alias = "search")]
    query: Option<String>,
    /// When `true`, deletes `<agent_root>/memory/<topic>.md` and rebuilds `MEMORY.md`.
    #[serde(default)]
    delete: Option<bool>,
    /// Target agent name or `agt_...` ID (defaults to this agent itself).
    #[serde(default, alias = "target")]
    agent: Option<String>,
    /// Maximum number of results when `action = "search"` or `"list"` (default 20).
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
struct FileEditSpec {
    /// Target string in the file to replace (supports exact, CRLF-normalized, trailing-whitespace-trimmed, and indentation-tolerant matching).
    #[serde(
        default,
        alias = "old_str",
        alias = "old_string",
        alias = "search",
        alias = "find"
    )]
    target: String,
    /// Replacement string to substitute in place of `target`.
    #[serde(default, alias = "new_str", alias = "new_string", alias = "replace")]
    replacement: String,
    /// Optional per-edit override for whether multiple occurrences of `target` should all be replaced (`true` by default).
    #[serde(default)]
    replace_all: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct FilesArgs {
    /// File/workspace operation to perform:
    /// - `"read"`: Read a file with optional 1-based `start_line`, `end_line`, `byte_offset`, and `max_bytes` (default 46080).
    /// - `"write"`: Create, overwrite, or append (`append: true`) a file, automatically creating parent directories and returning `{ path, bytes_written, sha256 }`.
    /// - `"edit"` / `"multi_edit"`: Apply a `target` -> `replacement` edit (or an atomic array of `edits: [{ target, replacement }]`) with 4-stage cascade matching, returning `{ path, replacements_made, lines_added, lines_removed, sha256 }`.
    /// - `"patch"`: Apply an OpenAI V4A patch (`*** Begin Patch` / `*** Update File:` / `*** Add File:` / `*** Delete File:`) or standard unified diff (`--- a/...` / `+++ b/...`).
    /// - `"list"`: List directory entries (`name`, `path`, `kind`: `"file" | "dir" | "symlink"`, `bytes`) sorted deterministically.
    /// - `"glob"`: Fast glob file listing (`pattern`, optional `path`, `max_results` default 200), skipping `.git`/`target`/`node_modules` and respecting `.gitignore` by default.
    /// - `"grep"`: Fast regex/literal content search (`query`, optional `path`, `glob`, `max_results` default 100, `context_lines`), returning `{ matches: [{ path, line, text }] }`.
    #[serde(default)]
    action: Option<String>,
    /// File or directory path (relative paths are resolved against the agent's `workdir`).
    #[serde(
        default,
        alias = "file",
        alias = "file_path",
        alias = "filepath",
        alias = "dir",
        alias = "directory"
    )]
    path: Option<String>,
    /// 1-based inclusive start line when `action = "read"`.
    #[serde(default, alias = "from_line", alias = "offset_line")]
    start_line: Option<usize>,
    /// 1-based inclusive end line when `action = "read"`.
    #[serde(default, alias = "to_line", alias = "limit_line")]
    end_line: Option<usize>,
    /// Byte offset into the selected line window when `action = "read"` (default 0).
    #[serde(default, alias = "offset")]
    byte_offset: Option<usize>,
    /// Maximum bytes to read when `action = "read"` (default 46080, max 262144).
    #[serde(default)]
    max_bytes: Option<usize>,
    /// File content to write when `action = "write"`.
    #[serde(default, alias = "text", alias = "body")]
    content: Option<String>,
    /// Whether to allow overwriting an existing file when `action = "write"` (`true` by default; set `false` to fail if the file already exists).
    #[serde(default)]
    overwrite: Option<bool>,
    /// Whether to append `content` to the end of the file when `action = "write"` (`false` by default).
    #[serde(default)]
    append: Option<bool>,
    /// Target string to replace when `action = "edit"`.
    #[serde(default, alias = "old_str", alias = "old_string")]
    target: Option<String>,
    /// Replacement string when `action = "edit"`.
    #[serde(default, alias = "new_str", alias = "new_string", alias = "replace")]
    replacement: Option<String>,
    /// Array of `{ target, replacement }` edits applied atomically when `action = "edit"` or `"multi_edit"`.
    #[serde(default)]
    edits: Vec<FileEditSpec>,
    /// Whether multiple occurrences of `target` should all be replaced (`true` by default; set `false` to require a unique match).
    #[serde(default)]
    replace_all: Option<bool>,
    /// OpenAI V4A patch (`*** Begin Patch` ...) or unified diff (`--- a/...` / `+++ b/...`) when `action = "patch"`.
    #[serde(default, alias = "diff")]
    patch: Option<String>,
    /// Glob pattern when `action = "glob"` (e.g. `"**/*.rs"`, `"src/**/*.ts"`).
    #[serde(default)]
    pattern: Option<String>,
    /// Optional file filter glob when `action = "grep"` (or alias for `pattern` when `action = "glob"`).
    #[serde(default)]
    glob: Option<String>,
    /// Search regex or literal string when `action = "grep"`.
    #[serde(default, alias = "q", alias = "search", alias = "regex")]
    query: Option<String>,
    /// Maximum number of results to return (`200` default for `glob`, `100` default for `grep`, `500` default for `list`).
    #[serde(default, alias = "limit")]
    max_results: Option<usize>,
    /// Number of context lines (`0..=10`) before and after each match when `action = "grep"`.
    #[serde(default, alias = "context")]
    context_lines: Option<usize>,
    /// Case-insensitive matching when `action = "grep"` (`false` by default).
    #[serde(default, alias = "ignore_case")]
    case_insensitive: Option<bool>,
    /// Force literal substring matching when `action = "grep"` (`false` by default).
    #[serde(default)]
    literal: Option<bool>,
    /// Include hidden dotfiles/directories during `glob`, `grep`, or `list` (`false` by default for `glob`/`grep`, `true` by default for `list`).
    #[serde(default)]
    include_hidden: Option<bool>,
    /// Include `.git`, `target`, `node_modules`, and `.gitignore`-matched paths during `glob` or `grep` (`false` by default).
    #[serde(default)]
    include_ignored: Option<bool>,
    /// Shell command string (`bash -c <cmd>`) when `action = "exec"`.
    #[serde(default, alias = "command")]
    cmd: Option<String>,
    /// Optional working directory (relative to the agent's `workdir` or absolute) when `action = "exec"`.
    #[serde(default, alias = "workdir")]
    cwd: Option<String>,
    /// Optional execution timeout in milliseconds when `action = "exec"` (default `30000`, max `120000`).
    #[serde(default, alias = "timeout")]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct DynamicObjectArgs {
    /// Action name to execute on the target capability (`computer`, `device`, or `media`).
    #[serde(default)]
    action: Option<String>,
    /// Additional action-specific fields.
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

impl DynamicObjectArgs {
    fn from_value(val: serde_json::Value) -> Result<Self, String> {
        serde_json::from_value(val).map_err(|e| format!("invalid object arguments: {e}"))
    }

    fn into_value(self) -> serde_json::Value {
        let mut map = self.extra;
        if let Some(act) = self.action {
            map.insert("action".to_string(), serde_json::Value::String(act));
        }
        serde_json::Value::Object(map)
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct CodeArgs {
    /// Async TypeScript / JavaScript snippet body to execute in the AnyAgents capability sandbox.
    /// Has access to `anyagents.{memory,conversation,send,agents,schedule,groups,browser,files}` (and `tools.anyagents.*`)
    /// plus `workspace.{read,write,edit,multi_edit,patch,glob,grep,list}` (and `tools.workspace.*`) and `console.{log,warn,error}`.
    /// Ambient `process.env`, `require`, `child_process`, and raw `fs` are isolated and unavailable.
    #[serde(alias = "script", alias = "source")]
    code: String,
    /// Optional execution timeout in milliseconds (default `30000`, max `120000`).
    #[serde(default, alias = "timeout")]
    timeout_ms: Option<u64>,
}

fn generate_subagent_name(parent_name: &str) -> String {
    let raw = AgentId::generate().to_string();
    let short = raw
        .strip_prefix("agt_")
        .unwrap_or(&raw)
        .chars()
        .take(8)
        .collect::<String>()
        .to_ascii_lowercase();
    format!("{parent_name}-sub-{short}")
}

fn send_turn_result(response: SendResponse, closing: bool) -> Result<serde_json::Value, String> {
    let mut result = serde_json::to_value(response).map_err(|e| e.to_string())?;
    if closing {
        result["status"] = serde_json::json!("turn_complete_no_reply_sent");
        result["turn_complete_hint"] = serde_json::json!(
            "No message was sent. End this turn immediately. Do not call send or any other tool again."
        );
    } else {
        result["turn_complete_hint"] = serde_json::json!(
            "The send request is complete. If your work is finished, end this turn without calling send again for a closing token."
        );
    }
    Ok(result)
}

#[tool_router]
impl AgentMcp {
    #[tool(
        description = "Unified outbound communication and message interaction tool: send a message, media file (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`), GPS `location`/venue, interactive `poll`, `checklist`, or Instant View `article` to one or multiple agents (`planner`, `agt_...`, `parent`, `children`), internal groups (`group:<name>`, `group:all`, `all`), or external channel targets (`telegram:chat:<id>[:<topic>]`, `telegram:user:<id>`); OR perform a message interaction (`action`: `\"react\"` | `\"vote\"` | `\"check\"` | `\"uncheck\"` | `\"edit\"` | `\"delete\"` | `\"pin\"` | `\"unpin\"` | `\"stop_poll\"`) on a target message (`target` or `reply_to`: `anyagents:reply:msg_...` or `telegram:chat:<chat_id>:<message_id>`)."
    )]
    async fn send(
        &self,
        Parameters(args): Parameters<SendArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let inferred_interact = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("send"))
            .map(str::to_ascii_lowercase)
            .or_else(|| {
                if args.emoji.is_some() && args.to.is_empty() && args.text.trim().is_empty() {
                    Some("react".to_string())
                } else {
                    None
                }
            });
        if let Some(interact_action) = inferred_interact {
            let target = args
                .target
                .or(args.reply_to)
                .or_else(|| args.to.into_iter().next())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    format!("`send` with `action = \"{interact_action}\"` requires `target` or `reply_to`")
                })?;
            let text_opt = if args.text.trim().is_empty() {
                None
            } else {
                Some(args.text)
            };
            let res = self
                .interact(Parameters(InteractArgs {
                    action: interact_action,
                    target,
                    emoji: args.emoji,
                    options: args.options,
                    text: text_opt,
                }))
                .await?
                .0;
            return Ok(rmcp::handler::server::wrapper::Json(
                serde_json::to_value(res).map_err(|e| e.to_string())?,
            ));
        }

        let quote =
            args.quote
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|t| anyagents_core::Quote {
                    text: t.to_string(),
                    ..anyagents_core::Quote::default()
                });
        let body = SendRequest {
            // The server sends as this agent whatever this says, and refuses a
            // body that names anyone else.
            sender: Some(format!("agent:{}", self.agent.name)),
            to: args.to,
            text: args.text,
            reply_to: args.reply_to,
            quote,
            media: args.media,
            location: args.location,
            poll: args.poll,
            checklist: args.checklist,
            article: args.article,
            idempotency_key: args.idempotency_key,
        };
        let closing = body.is_turn_closing_sentinel();
        let response: SendResponse = self
            .client
            .get(
                "/v1/send",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(send_turn_result(
            response, closing,
        )?))
    }

    #[tool(
        description = "Unified two-way (`in` + `out`) conversation reader, host memory search (`scope`: all|memory|transcripts; default all), and zero-friction learned-memory topic reader/writer (`topic`, `description`, `content`, `delete`). Exact bounded raw readback uses `raw_id` and `raw_sha256` from a result plus optional `raw_offset`/`raw_length` (bytes, max65536); it returns exact `raw_bytes`, full raw coordinates and the requested window. Paths are never accepted. Native historical events without source time return timestamp_ms:null (CLI: unknown); missing time is never inferred from file mtime, and timestamp bounds exclude unknown times. Lexical + Structured History Search (`query`, `queries`, `exclude`, `include_tools`, `only_tools`, `tool_name`, `direction`, `sender`, `since_ms`, `until_ms`, `from_seq`, `to_seq`, `range`, `sort`, `radius`, `limit`). To read a learned-memory topic directly in O(1), pass `topic: \"<slug>\"` (without `content` or `delete`). To write or update a durable learned-memory topic without host file paths, pass `topic: \"<slug>\"`, `description: \"<1-line trigger>\"`, and `content: \"<markdown>\"` (or `delete: true` to remove a stale topic); AnyAgents writes `<agent_root>/memory/<topic>.md`, syncs the host index immediately, and rebuilds `MEMORY.md` automatically."
    )]
    async fn conversation(
        &self,
        Parameters(args): Parameters<ConversationArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<ConversationResponse>, String> {
        let clean_q = args
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let (range_from, range_to) = parse_sequence_range(args.range.as_deref())?;
        let from_seq = args.from_seq.or(range_from);
        let to_seq = args.to_seq.or(range_to);
        let has_topic_mutation = args
            .topic
            .as_deref()
            .map(str::trim)
            .is_some_and(|s| !s.is_empty());
        let is_search_mode = !has_topic_mutation
            && (args.raw_id.is_some()
                || clean_q.is_some()
                || !args.queries.is_empty()
                || !args.exclude.is_empty()
                || args.only_tools == Some(true)
                || args.tool_name.is_some());
        let default_agent = if is_search_mode {
            "all".to_string()
        } else {
            match crate::client::agent_identity() {
                Some(id) => id.to_string(),
                None => self.agent.id.to_string(),
            }
        };
        let target = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
            .unwrap_or(default_agent);
        let mut query = Vec::new();
        if let Some(id) = args.raw_id {
            query.push(format!("raw_id={}", urlencoding_simple(&id)));
        }
        if let Some(digest) = args.raw_sha256 {
            query.push(format!("raw_sha256={}", urlencoding_simple(&digest)));
        }
        if let Some(offset) = args.raw_offset {
            query.push(format!("raw_offset={offset}"));
        }
        if let Some(length) = args.raw_length {
            query.push(format!("raw_length={length}"));
        }
        if let Some(scope) = args.scope {
            if !["all", "memory", "transcripts"].contains(&scope.as_str()) {
                return Err("scope must be all, memory or transcripts".into());
            }
            query.push(format!("scope={scope}"));
        } else if has_topic_mutation {
            query.push("scope=memory".to_string());
        }
        if let Some(topic) = args
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("topic={}", urlencoding_simple(topic)));
        }
        if let Some(desc) = args
            .description
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("description={}", urlencoding_simple(desc)));
        }
        if let Some(content) = args.content.as_deref() {
            query.push(format!("content={}", urlencoding_simple(content)));
        }
        if let Some(del) = args.delete {
            query.push(format!("delete={del}"));
        }
        if let Some(q) = clean_q {
            query.push(format!("query={}", urlencoding_simple(q)));
        }
        if !args.queries.is_empty() {
            query.push(format!(
                "queries={}",
                urlencoding_simple(&args.queries.join("|"))
            ));
        }
        if !args.exclude.is_empty() {
            query.push(format!(
                "exclude={}",
                urlencoding_simple(&args.exclude.join("|"))
            ));
        }
        if let Some(with_peer) = args
            .with
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("with={}", urlencoding_simple(with_peer)));
        }
        if let Some(dir) = args
            .direction
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("direction={}", urlencoding_simple(dir)));
        }
        if let Some(sender) = args
            .sender
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("sender={}", urlencoding_simple(sender)));
        }
        if let Some(inc) = args.include_tools {
            query.push(format!("include_tools={inc}"));
        }
        if let Some(only) = args.only_tools {
            query.push(format!("only_tools={only}"));
        }
        if let Some(tname) = args
            .tool_name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("tool_name={}", urlencoding_simple(tname)));
        }
        if let Some(since_ms) = args.since_ms {
            query.push(format!("since_ms={since_ms}"));
        }
        if let Some(until_ms) = args.until_ms {
            query.push(format!("until_ms={until_ms}"));
        }
        if let Some(fs) = from_seq {
            query.push(format!("from_seq={fs}"));
        }
        if let Some(ts) = to_seq {
            query.push(format!("to_seq={ts}"));
        }
        if let Some(sort) = args
            .sort
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("sort={}", urlencoding_simple(sort)));
        }
        if let Some(rad) = args.radius {
            query.push(format!("radius={rad}"));
        }
        let limit = args.limit.unwrap_or(if is_search_mode { 20 } else { 50 });
        query.push(format!("limit={limit}"));
        let path = format!("/v1/agents/{target}/conversation?{}", query.join("&"));
        let response = self.client.get(&path, None).await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Zero-Embedding Lossless Lexical + Structured History Search across authorized conversations and tool history (`query`, `queries`, `exclude`, `agent`, `with`, `direction`, `sender`, `include_tools`, `only_tools`, `tool_name`, `since_ms`, `until_ms`, `from_seq`, `to_seq`, `range`, `sort`, `radius`, `limit`). Combines SQLite FTS5 BM25 (ASCII identifiers + CJK bigrams), exact phrase boost, and contiguous episode context expansion."
    )]
    async fn search(
        &self,
        Parameters(args): Parameters<ConversationArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<ConversationResponse>, String> {
        self.conversation(Parameters(args)).await
    }

    #[tool(
        description = "Unified agent & cross-harness sub-agent manager (`action`: `list` | `inspect` | `spawn` | `create` | `describe` | `redact` | `steer` | `wake` | `pause` | `archive` | `delete` | `rename` | `role` | `harness` | `models`, supporting multi-level sub-agent trees). Use `list` (default, with optional `parent`: `\"self\"` | `\"subagents\"` | `\"root\"` and `state`: `\"running\"` | `\"working\"` | `\"paused\"` | `\"archived\"` | `\"all\"`) to inspect agents and sub-agents; use `inspect` (`name`: `\"<agent-or-id>\"`, `limit`: `10`) to view live status, schedules, and recent turns; use `spawn` (`name`, `repo`: `\"<org>/<repo>\"` | `\"inherit\"`, `branch`, `base`, `workdir`: path | `\"worktree\"` | `\"private\"` | `\"inherit\"`, `description`, `prompt`, `harness`, `model`, `profile`) to launch a sub-agent with an isolated managed Git worktree; use `describe` (`name`: `\"self\"` | `\"all\"` | `\"<agent>\"`, `description`: `\"...\"`) to read or update role descriptions; use `steer`, `wake`, `pause`, `archive` (auto-reaps clean/pushed managed worktrees while preserving dirty ones), or `delete` to control lifecycle; use `redact` (`name`, `message_id` or `seq`, optional `span`: {`unit`: `bytes` | `chars`, `start`, `end`}, `reason`) for audited erasure with permission `conversation.redact` (ancestors/admin only, quiescent target); or use `create`, `rename`, `role`, `harness`, `models`."
    )]
    async fn agents(
        &self,
        Parameters(args): Parameters<AgentsArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| {
                if args.new_name.is_some() {
                    "rename".to_string()
                } else if args.instructions.is_some()
                    && args.prompt.is_none()
                    && args.harness.is_none()
                    && args.repo.is_none()
                {
                    "describe".to_string()
                } else if args.prompt.is_some()
                    && (args.repo.is_some() || args.workdir.is_some() || args.branch.is_some())
                {
                    "spawn".to_string()
                } else {
                    "list".to_string()
                }
            });
        match action.as_str() {
            "redact" => {
                let target = args
                    .name
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| "redact requires a target agent".to_string())?;
                let request = anyagents_core::redaction::Request {
                    message_id: args.message_id,
                    seq: args.seq,
                    span: args
                        .span
                        .map(TryInto::try_into)
                        .transpose()
                        .map_err(|e: Error| e.to_string())?,
                    reason: args.reason.ok_or_else(|| {
                        "redact requires a reason (without sensitive text)".to_string()
                    })?,
                };
                let path = format!(
                    "/v1/agents/{}/conversation/redact",
                    urlencoding_simple(target.strip_prefix("agent:").unwrap_or(target))
                );
                let response = self
                    .client
                    .get(
                        &path,
                        Some(serde_json::to_vec(&request).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(response))
            }
            "list" | "ls" => {
                let mut response: AgentsResponse = self.client.get_agents().await?;
                let state_trimmed = args
                    .state
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                match state_trimmed {
                    Some(state_filter) if !state_filter.eq_ignore_ascii_case("all") => {
                        response.agents.retain(|a| {
                            a.state.eq_ignore_ascii_case(state_filter)
                                || a.turn_state.eq_ignore_ascii_case(state_filter)
                        });
                    }
                    Some(_) => {}
                    None => {
                        response.agents.retain(|a| a.state != "archived");
                    }
                }
                if let Some(parent_filter) = args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    let cleaned = parent_filter
                        .strip_prefix("agent:")
                        .unwrap_or(parent_filter);
                    if cleaned.eq_ignore_ascii_case("root") || cleaned.eq_ignore_ascii_case("none")
                    {
                        response.agents.retain(|a| a.parent.is_none());
                    } else if cleaned.eq_ignore_ascii_case("subagents")
                        || cleaned.eq_ignore_ascii_case("all_subagents")
                    {
                        response.agents.retain(|a| a.parent.is_some());
                    } else if cleaned.eq_ignore_ascii_case("siblings")
                        || cleaned.eq_ignore_ascii_case("sibling")
                        || cleaned.eq_ignore_ascii_case("peers")
                        || cleaned.eq_ignore_ascii_case("peer")
                    {
                        response.agents.retain(|a| {
                            matches!(a.relationship.as_deref(), Some("sibling" | "peer"))
                        });
                    } else if cleaned.eq_ignore_ascii_case("parent") {
                        response
                            .agents
                            .retain(|a| a.relationship.as_deref() == Some("parent"));
                    } else if !cleaned.eq_ignore_ascii_case("all") {
                        let wanted_id = if cleaned.eq_ignore_ascii_case("self")
                            || cleaned.eq_ignore_ascii_case("children")
                            || cleaned.eq_ignore_ascii_case(&self.agent.name)
                            || cleaned == self.agent.id.as_str()
                        {
                            Some(self.agent.id.to_string())
                        } else {
                            response
                                .agents
                                .iter()
                                .find(|a| {
                                    a.name.eq_ignore_ascii_case(cleaned)
                                        || a.id == cleaned
                                        || a.aliases
                                            .iter()
                                            .any(|al| al.eq_ignore_ascii_case(cleaned))
                                })
                                .map(|a| a.id.clone())
                                .or_else(|| Some(cleaned.to_string()))
                        };
                        if let Some(wid) = wanted_id {
                            response
                                .agents
                                .retain(|a| a.parent.as_deref() == Some(wid.as_str()));
                        }
                    }
                }
                let summary = filter_and_summarize_agent_list(
                    &mut response.agents,
                    args.query.as_deref(),
                    args.limit,
                    args.cursor,
                );
                for a in &mut response.agents {
                    a.instructions = None;
                }
                let mut val = serde_json::to_value(&response).map_err(|e| e.to_string())?;
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("next_cursor".to_string(), summary["next_cursor"].clone());
                    obj.insert("summary".to_string(), summary);
                }
                Ok(rmcp::handler::server::wrapper::Json(val))
            }
            "spawn" => {
                let spawned = self
                    .spawn(Parameters(SpawnArgs {
                        name: args.name,
                        archetype: args.archetype,
                        role: args.role,
                        harness: args.harness,
                        model: args.model,
                        profile: args.profile,
                        repo: args.repo,
                        branch: args.branch,
                        base: args.base,
                        workdir: args.workdir,
                        description: args.description,
                        instructions: args.instructions,
                        scope: args.scope,
                        public: args.public,
                        group: args.group,
                        prompt: args.prompt,
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(spawned).map_err(|e| e.to_string())?,
                ))
            }
            "create" => {
                let name = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents create` requires `name`".to_string())?
                    .to_string();
                let harness = args
                    .harness
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("")
                    .to_string();
                let parent_val = match args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    Some(p) if p.eq_ignore_ascii_case("self") => Some(self.agent.id.to_string()),
                    Some(p) => Some(p.to_string()),
                    None => Some("root".to_string()),
                };
                let body = CreateAgentRequest {
                    name,
                    archetype: args.archetype,
                    role: args.role,
                    harness: Some(harness),
                    model: args.model,
                    profile: args.profile,
                    repo: args.repo,
                    branch: args.branch,
                    base: args.base,
                    workdir: args.workdir,
                    description: args.description,
                    instructions: args.instructions,
                    parent: parent_val,
                    scope: args.scope,
                    public: args.public,
                    group: args.group,
                    prompt: args.prompt,
                };
                let created: AgentView = self
                    .client
                    .get(
                        "/v1/agents",
                        Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(created).map_err(|e| e.to_string())?,
                ))
            }
            "describe" => {
                self.describe(Parameters(DescribeArgs {
                    agent: args.name,
                    description: args.description,
                    instructions: args.instructions,
                    role: args.role,
                    scope: args.scope,
                    public: args.public,
                    clear: args.clear,
                }))
                .await
            }
            "steer" | "send" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents steer` requires `name` (or `agent`)".to_string())?;
                let text = args
                    .prompt
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents steer` requires `prompt`".to_string())?;
                self.send(Parameters(SendArgs {
                    to: vec![target.to_string()],
                    text: text.to_string(),
                    ..SendArgs::default()
                }))
                .await
            }
            "wake" | "resume" => {
                let target = args
                    .name
                    .ok_or_else(|| "`agents wake` requires `name` (or `agent`)".to_string())?;
                let woke = self
                    .wake(Parameters(WakeArgs {
                        agent: target,
                        prompt: args.prompt,
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(woke).map_err(|e| e.to_string())?,
                ))
            }
            "reset_session" | "new_session" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        "`agents reset_session` requires `name` (or `agent`)".to_string()
                    })?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let result: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/session/reset"),
                        Some(b"{}".to_vec()),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(result))
            }
            "pause" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents pause` requires `name` (or `agent`)".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let view: AgentView = self
                    .client
                    .get(&format!("/v1/agents/{clean}/pause"), Some(b"{}".to_vec()))
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(view).map_err(|e| e.to_string())?,
                ))
            }
            "archive" => {
                let view = self
                    .archive(Parameters(ArchiveArgs { agent: args.name }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(view).map_err(|e| e.to_string())?,
                ))
            }
            "delete" | "kill" | "remove" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents delete` requires `name` (or `agent`)".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let deleted: serde_json::Value = self
                    .client
                    .request_method("DELETE", &format!("/v1/agents/{clean}"), None)
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(deleted))
            }
            "rename" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents rename` requires `name`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let new_name = args
                    .new_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents rename` requires `new_name`".to_string())?;
                let clean_new = new_name.strip_prefix("agent:").unwrap_or(new_name);
                let body = serde_json::json!({
                    "name": clean_new,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/rename"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "role" | "set_role" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents role` requires `name`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let body = serde_json::json!({
                    "role": args.role,
                    "clear": args.clear.unwrap_or(false),
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/role"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "harness" | "set_harness" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents harness` requires `name`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let harness = args
                    .harness
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents harness` requires `harness`".to_string())?;
                let body = serde_json::json!({
                    "harness": harness,
                    "model": args.model,
                    "profile": args.profile,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/harness"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "transfer" | "reparent" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents transfer` requires `name` (or `agent`)".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let new_parent = args
                    .parent
                    .as_deref()
                    .or(args.new_name.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let body = serde_json::json!({
                    "parent": new_parent,
                    "reason": args.prompt,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/transfer"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "inspect" | "status" | "get" | "info" | "show" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("self");
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let resp: AgentsResponse = self.client.get_agents().await?;
                let view = resp
                    .agents
                    .into_iter()
                    .find(|a| {
                        (clean.eq_ignore_ascii_case("self") && a.id == self.agent.id.as_str())
                            || a.name.eq_ignore_ascii_case(clean)
                            || a.id == clean
                            || a.aliases.iter().any(|al| al.eq_ignore_ascii_case(clean))
                    })
                    .ok_or_else(|| format!("agent `{clean}` not found"))?;
                let limit = args.limit.unwrap_or(10);
                let conv_path = format!("/v1/agents/{}/conversation?limit={limit}", view.id);
                let conv: ConversationResponse =
                    self.client
                        .get(&conv_path, None)
                        .await
                        .unwrap_or(ConversationResponse {
                            agent_id: view.id.clone(),
                            agent: view.name.clone(),
                            warnings: Vec::new(),
                            turns: Vec::new(),
                        });
                let schedules = view.schedules.clone();
                Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                    "agent": view,
                    "subagent": view,
                    "schedules": schedules,
                    "recent_turns": conv.turns,
                })))
            }
            "models" | "catalog" | "profiles" => {
                let filter = args
                    .profile
                    .as_deref()
                    .or(args.harness.as_deref())
                    .or(args.name.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let path = match filter {
                    Some(f) => format!("/v1/profiles/models?filter={}", urlencoding_simple(f)),
                    None => "/v1/profiles/models".to_string(),
                };
                let catalogs: serde_json::Value = self.client.get(&path, None).await?;
                Ok(rmcp::handler::server::wrapper::Json(catalogs))
            }
            "telegram_bot" | "bind_telegram" | "channel" | "request_bot" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        "`agents telegram_bot` requires `name` (target sub-agent name or ID)"
                            .to_string()
                    })?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let body = serde_json::json!({
                    "kind": "telegram",
                    "agent": clean,
                    "token": args.token,
                    "bot_id": args.bot_id,
                    "bot_username": args.bot_username,
                    "manager": args.parent,
                });
                let res: serde_json::Value = self
                    .client
                    .get(
                        "/v1/channels",
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(res))
            }
            "channels" | "list_channels" => {
                let res: serde_json::Value = self.client.get("/v1/channels", None).await?;
                Ok(rmcp::handler::server::wrapper::Json(res))
            }
            other => Err(format!(
                "unknown agents action `{other}`; expected `list`, `inspect`, `spawn`, `create`, `describe`, `steer`, `wake`, `pause`, `archive`, `delete`, `rename`, `role`, `harness`, `transfer`, `telegram_bot`, `channels`, or `models`"
            )),
        }
    }

    #[tool(
        description = "Spawn a new sub-agent with this agent as its `parent` (supports multi-level sub-agent trees). Pass `repo` (e.g. `\"example-org/workspace\"` or `\"inherit\"` to branch from your current repo, with optional `branch` and `base`) to automatically provision an isolated, lifecycle-managed Git worktree at `<data>/agents/<id>/work` (cleanly reaped on `archive` when pushed, or preserved if dirty). If both `repo` and `workdir` are omitted, defaults to its own isolated `<data>/agents/<id>/work`. Every sub-agent automatically receives the shared `AGENTS.md` and top-level `skills` in its config home."
    )]
    async fn spawn(
        &self,
        Parameters(args): Parameters<SpawnArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<AgentView>, String> {
        let name = args
            .name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| generate_subagent_name(&self.agent.name));
        let harness = args
            .harness
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("")
            .to_string();
        let body = CreateAgentRequest {
            name,
            archetype: args.archetype,
            role: args.role,
            harness: Some(harness),
            model: args.model,
            profile: args.profile,
            repo: args.repo,
            branch: args.branch,
            base: args.base,
            workdir: args.workdir,
            description: args.description,
            instructions: args.instructions,
            parent: Some(self.agent.id.to_string()),
            scope: args.scope,
            public: args.public,
            group: args.group,
            prompt: args.prompt,
        };
        let response = self
            .client
            .get(
                "/v1/agents",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Unified cross-harness sub-agent manager (`action`: `list` | `inspect` | `spawn` | `steer` | `wake` | `pause` | `archive` | `delete`, supporting multi-level sub-agent trees). Use `list` (default) to view your sub-agents and their live status (`running`, `paused`, `archived`, `turn_state`); use `inspect` with `agent` to view live status AND recent conversation turns; use `spawn` to launch a sub-agent on any harness (`opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`), model, profile, and workspace (`repo`: `\"<org>/<repo>\"` or `\"inherit\"` for an isolated Git worktree at `<data>/agents/<id>/work`; `workdir`: path, `\"worktree\"`, `\"private\"`, or `\"inherit\"` for explicit parent sharing; omitting both `repo` and `workdir` gives a private directory); use `steer` to send follow-up instructions; use `pause`, `archive` (auto-reaps clean/pushed managed worktrees while preserving dirty ones), `wake`, or `delete` to control its lifecycle."
    )]
    async fn subagents(
        &self,
        Parameters(args): Parameters<SubagentsArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("list")
            .to_ascii_lowercase();
        match action.as_str() {
            "list" | "ls" => {
                let parent_arg = args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("self");
                let mut resp: AgentsResponse = self.client.get_agents().await?;
                if parent_arg.eq_ignore_ascii_case("all") {
                    resp.agents.retain(|a| a.parent.is_some());
                } else {
                    let cleaned = parent_arg.strip_prefix("agent:").unwrap_or(parent_arg);
                    let wanted_id = if cleaned.eq_ignore_ascii_case("self")
                        || cleaned.eq_ignore_ascii_case(&self.agent.name)
                        || cleaned == self.agent.id.as_str()
                    {
                        self.agent.id.to_string()
                    } else {
                        resp.agents
                            .iter()
                            .find(|a| {
                                a.name.eq_ignore_ascii_case(cleaned)
                                    || a.id == cleaned
                                    || a.aliases.iter().any(|al| al.eq_ignore_ascii_case(cleaned))
                            })
                            .map(|a| a.id.clone())
                            .unwrap_or_else(|| cleaned.to_string())
                    };
                    resp.agents
                        .retain(|a| a.parent.as_deref() == Some(wanted_id.as_str()));
                }
                if let Some(st) = args
                    .state
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
                {
                    resp.agents.retain(|a| {
                        a.state.eq_ignore_ascii_case(st) || a.turn_state.eq_ignore_ascii_case(st)
                    });
                }
                let summary = filter_and_summarize_agent_list(
                    &mut resp.agents,
                    args.query.as_deref(),
                    args.limit,
                    args.cursor,
                );
                for a in &mut resp.agents {
                    a.instructions = None;
                }
                Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                    "parent": self.agent.name,
                    "next_cursor": summary["next_cursor"],
                    "summary": summary,
                    "subagents": resp.agents,
                })))
            }
            "inspect" | "status" | "get" | "info" | "show" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("self");
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let resp: AgentsResponse = self.client.get_agents().await?;
                let view = resp
                    .agents
                    .into_iter()
                    .find(|a| {
                        (clean.eq_ignore_ascii_case("self") && a.id == self.agent.id.as_str())
                            || a.name.eq_ignore_ascii_case(clean)
                            || a.id == clean
                            || a.aliases.iter().any(|al| al.eq_ignore_ascii_case(clean))
                    })
                    .ok_or_else(|| format!("sub-agent `{clean}` not found"))?;
                let limit = args.limit.unwrap_or(10);
                let conv_path = format!("/v1/agents/{}/conversation?limit={limit}", view.id);
                let conv: ConversationResponse =
                    self.client
                        .get(&conv_path, None)
                        .await
                        .unwrap_or(ConversationResponse {
                            agent_id: view.id.clone(),
                            agent: view.name.clone(),
                            warnings: Vec::new(),
                            turns: Vec::new(),
                        });
                let schedules = view.schedules.clone();
                Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                    "subagent": view,
                    "schedules": schedules,
                    "recent_turns": conv.turns,
                })))
            }
            "spawn" | "create" => {
                let spawned = self
                    .spawn(Parameters(SpawnArgs {
                        name: args.agent,
                        archetype: args.archetype,
                        role: None,
                        harness: args.harness,
                        model: args.model,
                        profile: args.profile,
                        repo: args.repo,
                        branch: args.branch,
                        base: args.base,
                        workdir: args.workdir,
                        description: args.description,
                        instructions: args.instructions,
                        scope: args.scope,
                        public: args.public,
                        group: args.group,
                        prompt: args.prompt,
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(spawned).map_err(|e| e.to_string())?,
                ))
            }
            "steer" | "send" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents steer` requires `agent`".to_string())?;
                let text = args
                    .prompt
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents steer` requires `prompt`".to_string())?;
                let sent = self
                    .send(Parameters(SendArgs {
                        to: vec![target.to_string()],
                        text: text.to_string(),
                        ..SendArgs::default()
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(sent).map_err(|e| e.to_string())?,
                ))
            }
            "wake" | "resume" => {
                let target = args
                    .agent
                    .ok_or_else(|| "`subagents wake` requires `agent`".to_string())?;
                let woke = self
                    .wake(Parameters(WakeArgs {
                        agent: target,
                        prompt: args.prompt,
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(woke).map_err(|e| e.to_string())?,
                ))
            }
            "pause" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents pause` requires `agent`".to_string())?
                    .strip_prefix("agent:")
                    .unwrap_or_default()
                    .to_string();
                let clean = if target.is_empty() {
                    args.agent.unwrap()
                } else {
                    target
                };
                let view: AgentView = self
                    .client
                    .get(&format!("/v1/agents/{clean}/pause"), Some(b"{}".to_vec()))
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(view).map_err(|e| e.to_string())?,
                ))
            }
            "archive" => {
                let view = self
                    .archive(Parameters(ArchiveArgs { agent: args.agent }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(view).map_err(|e| e.to_string())?,
                ))
            }
            "harness" | "set_harness" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents harness` requires `agent`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let harness = args
                    .harness
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents harness` requires `harness`".to_string())?;
                let body = serde_json::json!({
                    "harness": harness,
                    "model": args.model,
                    "profile": args.profile,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/harness"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "transfer" | "reparent" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents transfer` requires `agent`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let new_parent = args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let body = serde_json::json!({
                    "parent": new_parent,
                    "reason": args.prompt,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/transfer"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "delete" | "kill" | "remove" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents delete` requires `agent`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let deleted: serde_json::Value = self
                    .client
                    .request_method("DELETE", &format!("/v1/agents/{clean}"), None)
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(deleted))
            }
            "telegram_bot" | "bind_telegram" | "channel" | "request_bot" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        "`subagents telegram_bot` requires `agent` (target sub-agent name or ID)"
                            .to_string()
                    })?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let body = serde_json::json!({
                    "kind": "telegram",
                    "agent": clean,
                    "token": args.token,
                    "bot_id": args.bot_id,
                    "bot_username": args.bot_username,
                    "manager": args.parent,
                });
                let res: serde_json::Value = self
                    .client
                    .get(
                        "/v1/channels",
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(res))
            }
            other => Err(format!(
                "unknown subagents action `{other}`; expected `list`, `inspect`, `spawn`, `steer`, `wake`, `pause`, `archive`, `harness`, `transfer`, `telegram_bot`, or `delete`"
            )),
        }
    }

    #[tool(
        description = "Archive an agent (`state = archived`), stopping its OS process while preserving its permanent `agt_...` ID, mailbox, and session so an intentional direct `send` or `wake` call resumes it. For sub-agents with a managed Git worktree, automatically runs `git worktree remove` if the worktree is clean and has no unpushed commits, or preserves the worktree on disk if uncommitted/unpushed work remains."
    )]
    async fn archive(
        &self,
        Parameters(args): Parameters<ArchiveArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<AgentView>, String> {
        let target = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("self"))
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
            .filter(|s| !s.eq_ignore_ascii_case("self"))
            .unwrap_or_else(|| self.agent.id.to_string());
        let response = self
            .client
            .get(
                &format!("/v1/agents/{target}/archive"),
                Some(b"{}".to_vec()),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Wake and resume an archived or paused agent by its `agt_...` ID or name, optionally delivering a follow-up `prompt`."
    )]
    async fn wake(
        &self,
        Parameters(args): Parameters<WakeArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<AgentView>, String> {
        let target = args
            .agent
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(args.agent.trim())
            .to_string();
        let response: AgentView = self
            .client
            .get(&format!("/v1/agents/{target}/wake"), Some(b"{}".to_vec()))
            .await?;
        if let Some(prompt) = args
            .prompt
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let _ = self
                .send(Parameters(SendArgs {
                    to: vec![target],
                    text: prompt.to_string(),
                    ..SendArgs::default()
                }))
                .await?;
        }
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Create, join, leave, or list internal agent groups (`action`: `list` | `members` | `join` | `leave`). Agents can form internal groups without any external channel or human."
    )]
    async fn groups(
        &self,
        Parameters(args): Parameters<GroupsArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<GroupsResponse>, String> {
        let action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("list");
        if action == "list" {
            let response = self.client.get("/v1/groups", None).await?;
            return Ok(rmcp::handler::server::wrapper::Json(response));
        }
        let group = args
            .group
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("groups {action} requires a `group` name"))?
            .to_string();
        let combined_agent = {
            let mut list = Vec::new();
            if let Some(a) = args
                .agent
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                list.push(a.to_string());
            }
            for item in args.agents {
                let trimmed = item.trim();
                if !trimmed.is_empty() {
                    list.push(trimmed.to_string());
                }
            }
            if list.is_empty() {
                None
            } else {
                Some(list.join(","))
            }
        };
        let body = GroupActionRequest {
            action: action.to_string(),
            group,
            agent: combined_agent,
            public: args.public,
        };
        let one: GroupView = self
            .client
            .get(
                "/v1/groups",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(GroupsResponse {
            groups: vec![one],
        }))
    }

    #[tool(
        description = "Perform a rich IM channel or message interaction (`action`: `react` | `vote` | `edit` | `delete` | `pin` | `unpin` | `stop_poll`) on a target message (`anyagents:reply:msg_...` from the message header or `telegram:chat:<chat_id>:<message_id>`)."
    )]
    async fn interact(
        &self,
        Parameters(args): Parameters<InteractArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<InteractResponse>, String> {
        let body = InteractRequest {
            sender: Some(format!("agent:{}", self.agent.name)),
            action: args.action,
            target: args.target,
            emoji: args.emoji,
            options: args.options,
            text: args.text,
        };
        let response = self
            .client
            .get(
                "/v1/interact",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Read or update an agent's role & responsibility description (`description` / `instructions`). Any agent can update its own description (`agent: \"self\"`, `description: \"...\"`) whenever its mission or focus evolves, and Owner Admins or Parent agents can update other agents' descriptions. Updating `description` automatically syncs the agent's private `SYSTEM_PROMPT.md` / `AGENTS.md` and makes the role visible to all peers in `agents` and `groups`."
    )]
    async fn describe(
        &self,
        Parameters(args): Parameters<DescribeArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let target = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
            .unwrap_or_else(|| self.agent.name.clone());
        let resolved_target = if target.eq_ignore_ascii_case("self") {
            self.agent.name.clone()
        } else {
            target
        };
        if resolved_target.eq_ignore_ascii_case("all")
            && args.description.is_none()
            && args.instructions.is_none()
            && args.role.is_none()
            && args.scope.is_none()
            && args.public.is_none()
            && !args.clear.unwrap_or(false)
        {
            let resp: AgentsResponse = self.client.get_agents().await?;
            let roster: Vec<serde_json::Value> = resp
                .agents
                .into_iter()
                .filter(|a| a.state != "archived" || a.parent.is_none())
                .map(|a| {
                    serde_json::json!({
                        "id": a.id,
                        "name": a.name,
                        "role": a.role,
                        "aliases": a.aliases,
                        "parent": a.parent,
                        "scope": a.scope,
                        "relationship": a.relationship,
                        "can_message": a.can_message,
                        "harness": a.harness,
                        "model": a.model,
                        "state": a.state,
                        "description": a.description,
                    })
                })
                .collect();
            return Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                "agents": roster,
            })));
        }
        if args.description.is_some()
            || args.instructions.is_some()
            || args.role.is_some()
            || args.scope.is_some()
            || args.public.is_some()
            || args.clear.unwrap_or(false)
        {
            let body = serde_json::json!({
                "description": args.description,
                "instructions": args.instructions,
                "role": args.role,
                "scope": args.scope,
                "public": args.public,
                "clear": args.clear.unwrap_or(false),
            });
            let updated: serde_json::Value = self
                .client
                .get(
                    &format!("/v1/agents/{resolved_target}/description"),
                    Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                )
                .await?;
            return Ok(rmcp::handler::server::wrapper::Json(updated));
        }
        let current: serde_json::Value = self
            .client
            .get(&format!("/v1/agents/{resolved_target}/description"), None)
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(current))
    }

    #[tool(
        description = "Native SOTA anti-detect browser with persistent login profiles, interactive human CAPTCHA takeover, and scale-to-zero lifecycle (`action`: `open` | `snapshot` | `click` | `fill` | `press` | `hold` | `key_down` | `key_up` | `eval` | `screenshot` | `cookies` | `tabs` | `takeover` | `takeover_status` | `wait_for_user` | `release_takeover` | `status` | `profiles` | `close`). Runs real headed Chromium on a virtual X11 display (`xvfb-headed`) with deterministic per-profile WebGL/Client-Hints/Canvas stealth fingerprinting and persistent `--user-data-dir` login state. Pass `action: \"open\"` with `url` to navigate and receive interactive `@e1`, `@e2` refs; pass `action: \"click\"` or `\"fill\"` with `selector: \"@e1\"`; pass `action: \"press\"` or `\"hold\"` with `key` (or `keys`: `[\"w\", \"d\"]`) and optional `hold_ms` (e.g. `500`) to hold trusted keyboard input (`isTrusted: true`, W3C `KeyboardEvent.code`) across rendered animation frames and release cleanly on completion; when a CAPTCHA or 2FA challenge (`captcha_detected: true`) requires human assistance, pass `action: \"takeover\"` with optional `reason` to launch a live remote interactive browser takeover session (`takeover_url`) and send `takeover_url` (plus `screenshot_path` via `send` media) to the user; pass `action: \"close\"` when finished to scale the browser process to zero while keeping the profile logged in."
    )]
    async fn browser(
        &self,
        Parameters(args): Parameters<crate::browser::BrowserRequest>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let response: serde_json::Value = self
            .client
            .get(
                "/v1/browser",
                Some(serde_json::to_vec(&args).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Schedule your own wakes (`at` or `every` with `prompt`) and external event watches (`watch` with `prompt`), or `list` and `cancel` (`id`: `<sch_...>` | `\"all\"`) your schedules. Supported `watch` specs: `github:pr:<owner>/<repo>#<num>` (PR or checks terminal), `github:check:<owner>/<repo>@<ref>` (check runs terminal), `spiron:promise:<id>` (Spiron promise state change pulled from the public API), and `agent:idle:<name_or_id>` (AnyAgents agent going idle). Wakes fire only when you are idle and not quota-blocked, and a missed wake never fires twice."
    )]
    async fn schedule(
        &self,
        Parameters(args): Parameters<ScheduleArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let target_opt = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("self"))
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string());
        let path = match target_opt.as_deref() {
            Some(target) => format!("/v1/agents/{target}/schedules"),
            None => "/v1/schedules".to_string(),
        };
        let body = serde_json::json!({
            "action": args.action,
            "at": args.at,
            "every": args.every,
            "watch": args.watch,
            "prompt": args.prompt,
            "id": args.id,
            "agent": target_opt,
        });
        let response: serde_json::Value = self
            .client
            .get(
                &path,
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "First-class learned-memory manager (`action`: `\"list\"` | `\"read\"` | `\"write\"` | `\"upsert\"` | `\"delete\"` | `\"search\"`). Inferred automatically when `action` is omitted: `delete: true` -> `\"delete\"`, `content` -> `\"write\"`, `query` -> `\"search\"`, `topic` -> `\"read\"`, else `\"list\"`. Writes `<agent_root>/memory/<topic>.md`, syncs the FTS5 memory index, and rebuilds `MEMORY.md` (<150 lines / 4 KB)."
    )]
    async fn memory(
        &self,
        Parameters(args): Parameters<MemoryArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let val = self.execute_memory_tool(args).await?;
        Ok(rmcp::handler::server::wrapper::Json(val))
    }

    #[tool(
        description = "Multi-provider policy-aware web search and URL extraction tool (`action`: `\"search\"` | `\"fetch\"`; `engine`: `\"auto\"` | `\"parallel\"` | `\"exa\"` | `\"tavily\"` | `\"firecrawl\"` | `\"perplexity\"` | `\"brave\"` | `\"anysearch\"` | `\"direct\"`). Supports `allowed_domains`, `blocked_domains`, `country`, `max_results`, `max_chars`, `mode`, and `external_web_access` with bounded automatic failover."
    )]
    async fn web(
        &self,
        Parameters(args): Parameters<crate::web::WebRequest>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let data_dir = self.client.socket.parent().map(Path::to_path_buf);
        let local_has_keys = !crate::web::load_web_keys(data_dir.as_deref()).is_empty();
        let server_available = self.client.socket.exists();
        if !local_has_keys && server_available {
            let body = serde_json::to_vec(&args).map_err(|e| e.to_string())?;
            if let Ok(val) = self
                .client
                .get::<serde_json::Value>("/v1/web", Some(body))
                .await
            {
                return Ok(rmcp::handler::server::wrapper::Json(val));
            }
        }
        let val = tokio::task::spawn_blocking(move || {
            crate::web::execute_web_request(data_dir.as_deref(), args)
        })
        .await
        .map_err(|e| format!("web task join error: {e}"))??;
        Ok(rmcp::handler::server::wrapper::Json(val))
    }

    #[tool(
        description = "Desktop GUI Computer Use tool supporting Semantic Accessibility Snapshot Mode (`action: \"snapshot\"`, returning `@<snapshot_id>:e<N>` refs with Web Wrapper Skipping and `STALE_REF` guard), Window/Clipboard management (`list_windows`, `focus_window`, `clipboard_get`, `clipboard_set`), and Visual Pixel Mode (`screenshot`, `cursor_position`, `move`, `click`, `mouse_down`, `mouse_up`, `drag`, `scroll`, `type`, `key`, `hold_key`, `wait`, `zoom`, or Anthropic/OpenAI `action`)."
    )]
    async fn computer(
        &self,
        Parameters(args): Parameters<DynamicObjectArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let data_dir = self.client.socket.parent().map(Path::to_path_buf);
        let val = crate::computer::execute_computer_request(
            data_dir.as_deref(),
            &self.client.config,
            args.into_value(),
        )
        .await?;
        Ok(rmcp::handler::server::wrapper::Json(val))
    }

    #[tool(
        description = "Android / iOS Mobile Device Control tool (`action`: `\"screenshot\"` | `\"tap\"` | `\"swipe\"` | `\"type\"` | `\"press\"` | `\"install_app\"` | `\"launch_app\"` | `\"terminate_app\"` | `\"open_url\"` | `\"rotate\"`)."
    )]
    async fn device(
        &self,
        Parameters(args): Parameters<DynamicObjectArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let work_dir = self.resolve_workdir();
        let val = crate::computer::execute_device_request(&work_dir, args.into_value()).await?;
        Ok(rmcp::handler::server::wrapper::Json(val))
    }

    #[tool(
        description = "Multimodal Media tool (`action`: `\"image_generate\"` | `\"speech\"` | `\"transcribe\"`) backed by the Sylphx AI Gateway (`/v1/images/generations`, `/v1/audio/speech`, `/v1/audio/transcriptions`)."
    )]
    async fn media(
        &self,
        Parameters(args): Parameters<DynamicObjectArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let data_dir = self.client.socket.parent().map(Path::to_path_buf);
        let work_dir = self.resolve_workdir();
        let val = crate::computer::execute_media_request(
            data_dir.as_deref(),
            &self.client.config,
            &work_dir,
            args.into_value(),
        )
        .await?;
        Ok(rmcp::handler::server::wrapper::Json(val))
    }

    #[tool(
        description = "Unified cross-harness file, edit, patch, list, glob, grep, and command exec tool (`action`: `\"read\"` | `\"write\"` | `\"edit\"` | `\"multi_edit\"` | `\"patch\"` | `\"list\"` | `\"glob\"` | `\"grep\"` | `\"exec\"`), resolving relative paths deterministically against your `workdir`."
    )]
    async fn files(
        &self,
        Parameters(args): Parameters<FilesArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let base_dir = self.resolve_workdir();
        let val = tokio::task::spawn_blocking(move || execute_files_tool(&base_dir, args))
            .await
            .map_err(|e| format!("files task join error: {e}"))??;
        Ok(rmcp::handler::server::wrapper::Json(val))
    }

    #[tool(
        description = "Unified Cross-Harness Code Mode / Programmatic Tool Calling (`code`, optional `timeout_ms` default 30000, max 120000). Executes an async TypeScript / JavaScript snippet in an isolated capability sandbox (`process.env`, `require`, `child_process`, and raw `fs` are unavailable) with `anyagents.{memory,conversation,send,agents,schedule,groups,browser,web,computer,device,media,files}` (alias `tools.anyagents`), `web.{search,fetch}` (alias `tools.web`), `computer.{snapshot,list_windows,focus_window,clipboard_get,clipboard_set,screenshot,cursor_position,move,click,mouse_down,mouse_up,drag,scroll,type,key,hold_key,wait,zoom,action}` (alias `tools.computer`), `device.{screenshot,tap,swipe,type,press,install_app,launch_app,terminate_app,open_url,rotate}` (alias `tools.device`), `media.{image_generate,speech,transcribe}` (alias `tools.media`), and `workspace.{read,write,edit,multi_edit,patch,glob,grep,list,exec}` (alias `tools.workspace`). Compose parallel multi-tool queries with `await Promise.all([...])`, filter/aggregate intermediate data in memory, log diagnostics via `console.log`, and `return` only the compact result `{ result, logs, duration_ms }`."
    )]
    async fn code(
        &self,
        Parameters(args): Parameters<CodeArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let val = self.execute_code_tool(args).await?;
        Ok(rmcp::handler::server::wrapper::Json(val))
    }
}

pub const CANONICAL_MCP_TOOLS: &[&str] = &["code"];

#[rmcp::tool_handler(router = Self::tool_router())]
impl rmcp::ServerHandler for AgentMcp {
    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::ListToolsResult, rmcp::ErrorData> {
        let supports_cache_hints = context
            .protocol_version()
            .is_some_and(|version| version >= rmcp::model::ProtocolVersion::V_2026_07_28);
        let tools = Self::tool_router()
            .list_all()
            .into_iter()
            .filter(|t| CANONICAL_MCP_TOOLS.contains(&t.name.as_ref()))
            .collect();
        Ok(rmcp::model::ListToolsResult {
            result_type: Some(rmcp::model::ResultType::COMPLETE),
            tools,
            meta: None,
            next_cursor: None,
            ttl_ms: supports_cache_hints.then_some(0),
            cache_scope: supports_cache_hints.then_some(rmcp::model::CacheScope::Public),
        })
    }
}

fn urlencoding_simple(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'@') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

const DEFAULT_READ_MAX_BYTES: usize = 46_080;
const HARD_READ_MAX_BYTES: usize = 262_144;
const DEFAULT_GLOB_MAX_RESULTS: usize = 200;
const HARD_GLOB_MAX_RESULTS: usize = 5_000;
const DEFAULT_GREP_MAX_RESULTS: usize = 100;
const HARD_GREP_MAX_RESULTS: usize = 2_000;
const DEFAULT_LIST_MAX_RESULTS: usize = 500;
const HARD_LIST_MAX_RESULTS: usize = 5_000;
const DEFAULT_CODE_TIMEOUT_MS: u64 = 30_000;
const MAX_CODE_TIMEOUT_MS: u64 = 120_000;
const MAX_CODE_RESULT_BYTES: usize = 48 * 1024;
const MAX_CODE_TOOL_CALLS: usize = 256;

const DEFAULT_IGNORED_DIRS: &[&str] = &[
    ".git",
    "target",
    "node_modules",
    ".next",
    "dist",
    ".venv",
    "__pycache__",
];

const CODE_SANDBOX_WORKER_JS: &str = r#"
const Module = require("module");
const vm = require("vm");
const readline = require("readline");
const stripTS = typeof Bun !== "undefined" && Bun.Transpiler
  ? (src) => new Bun.Transpiler({ loader: "ts" }).transformSync(src)
  : (src) => Module.stripTypeScriptTypes(src);

const stdoutWrite = process.stdout.write.bind(process.stdout);
const exitProcess = process.exit.bind(process);
const outerSetTimeout = setTimeout;
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });

for (const k of Object.keys(process.env)) {
  try { delete process.env[k]; } catch {}
}
try {
  Object.defineProperty(process, "env", {
    value: Object.freeze(Object.create(null)),
    writable: false,
    configurable: false,
  });
} catch {}
Module._load = () => {
  throw new Error("require/import is disabled in AnyAgents code sandbox; use tools.anyagents.*");
};
try { delete globalThis.process; } catch {}
try { delete globalThis.require; } catch {}
try { delete globalThis.Bun; } catch {}
try { delete globalThis.Deno; } catch {}
try { delete globalThis.fetch; } catch {}
try { delete globalThis.WebSocket; } catch {}

const pending = new Map();
let nextId = 1;
const logs = [];
const MAX_LOGS = 200;
const MAX_LOG_LEN = 4096;

function hostLogRaw(level, text) {
  if (logs.length >= MAX_LOGS) return;
  const prefix = level === "log" ? "" : `[${level}] `;
  const msg = prefix + String(text);
  logs.push(msg.length > MAX_LOG_LEN ? msg.slice(0, MAX_LOG_LEN) + "...(truncated)" : msg);
}

function hostCallRaw(tool, reqJson, onOk, onErr) {
  const id = nextId++;
  let args = {};
  try { args = JSON.parse(reqJson); } catch {}
  pending.set(id, { onOk, onErr });
  stdoutWrite(JSON.stringify({ type: "call", id, tool: String(tool), args }) + "\n");
}

function hostSleepRaw(ms, cb) {
  const delay = Math.max(0, Math.min(120000, Number(ms) || 0));
  outerSetTimeout(() => {
    try { cb(); } catch {}
  }, delay);
}

function compileUserCode(rawCode, ctx) {
  const trimmed = String(rawCode ?? "").trim();
  if (!trimmed) {
    return vm.runInContext("(async () => null)", ctx);
  }
  if (!/\breturn\b/.test(trimmed)) {
    try {
      const exprBody = trimmed.replace(/;+\s*$/, "");
      const exprTs = "var __fn = async (tools: any, anyagents: any, workspace: any, web: any, console: any) => {\n  \"use strict\";\n  return (\n" + exprBody + "\n  );\n};";
      const exprJs = stripTS(exprTs) + "\n;__fn;";
      return vm.runInContext(exprJs, ctx);
    } catch {}
  }
  const stmtTs = "var __fn = async (tools: any, anyagents: any, workspace: any, web: any, console: any) => {\n  \"use strict\";\n" + rawCode + "\n};";
  const stmtJs = stripTS(stmtTs) + "\n;__fn;";
  return vm.runInContext(stmtJs, ctx);
}

rl.on("line", async (line) => {
  if (!line.trim()) return;
  let msg;
  try { msg = JSON.parse(line); } catch { return; }
  if (msg.type === "reply") {
    const p = pending.get(msg.id);
    if (p) {
      pending.delete(msg.id);
      if (msg.ok) {
        p.onOk(JSON.stringify(msg.result ?? null));
      } else {
        p.onErr(String(msg.error || "tool call failed"));
      }
    }
    return;
  }
  if (msg.type === "start") {
    try {
      const ctx = vm.createContext(Object.create(null), {
        codeGeneration: { strings: false, wasm: false },
      });
      const bootstrap = vm.runInContext(`(function(hostCallRaw, hostLogRaw, hostSleepRaw) {
        "use strict";
        const callTool = (tool, req) => new Promise((resolve, reject) => {
          hostCallRaw(String(tool), JSON.stringify(req ?? {}), (resJson) => {
            try {
              const parsed = JSON.parse(resJson);
              if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
                if ("results" in parsed && !("hits" in parsed)) {
                  Object.defineProperty(parsed, "hits", {
                    get() { return this.results; },
                    enumerable: false,
                    configurable: true,
                  });
                }
                if ("content" in parsed && !("text" in parsed)) {
                  Object.defineProperty(parsed, "text", {
                    get() { return this.content; },
                    enumerable: false,
                    configurable: true,
                  });
                }
              }
              resolve(parsed);
            } catch (e) { reject(e); }
          }, (errStr) => {
            reject(new Error(String(errStr)));
          });
        });
        const fmt = (v) => {
          if (typeof v === "string") return v;
          try { return JSON.stringify(v); } catch { return String(v); }
        };
        const log = (lvl, args) => hostLogRaw(lvl, args.map(fmt).join(" "));
        const memoryFn = (req = {}) => callTool("memory", typeof req === "string" ? { action: "read", topic: req } : req);
        memoryFn.read = (topicOrObj = {}) => callTool("memory", typeof topicOrObj === "string" ? { action: "read", topic: topicOrObj } : { ...topicOrObj, action: "read" });
        memoryFn.write = (opts = {}) => callTool("memory", { ...opts, action: "write" });
        memoryFn.upsert = (opts = {}) => callTool("memory", { ...opts, action: "write" });
        memoryFn.delete = (topicOrObj = {}) => callTool("memory", typeof topicOrObj === "string" ? { action: "delete", topic: topicOrObj, delete: true } : { ...topicOrObj, action: "delete", delete: true });
        memoryFn.list = (opts = {}) => callTool("memory", { ...(typeof opts === "string" ? { agent: opts } : opts), action: "list" });
        memoryFn.search = (queryOrObj = {}) => callTool("memory", typeof queryOrObj === "string" ? { action: "search", query: queryOrObj } : { ...queryOrObj, action: "search" });
        memoryFn.recall = (queryOrObj = {}) => callTool("memory", typeof queryOrObj === "string" ? { action: "recall", query: queryOrObj } : { ...queryOrObj, action: "recall" });
        memoryFn.status = (opts = {}) => callTool("memory", { ...(typeof opts === "string" ? { agent: opts } : opts), action: "status" });
        memoryFn.consolidate = (opts = {}) => callTool("memory", { ...opts, action: "consolidate" });
        Object.freeze(memoryFn);

        const webFn = (req = {}) => callTool("web", typeof req === "string"
          ? (/^https?:/i.test(req.trim()) ? { action: "fetch", url: req } : { action: "search", query: req })
          : req);
        webFn.search = (queryOrObj = {}) => callTool("web", typeof queryOrObj === "string"
          ? { action: "search", query: queryOrObj }
          : { ...queryOrObj, action: "search" });
        webFn.fetch = (urlOrObj = {}) => callTool("web", typeof urlOrObj === "string"
          ? { action: "fetch", url: urlOrObj }
          : { ...urlOrObj, action: "fetch" });
        Object.freeze(webFn);

        const computerFn = (req = {}) => callTool("computer", typeof req === "string" ? { action: req } : req);
        computerFn.snapshot = (opts = {}) => callTool("computer", { ...opts, action: "snapshot" });
        computerFn.list_windows = (opts = {}) => callTool("computer", { ...opts, action: "list_windows" });
        computerFn.focus_window = (opts = {}) => callTool("computer", typeof opts === "string" ? { action: "focus_window", window_id: opts } : { ...opts, action: "focus_window" });
        computerFn.clipboard_get = (opts = {}) => callTool("computer", { ...opts, action: "clipboard_get" });
        computerFn.clipboard_set = (textOrObj = {}) => callTool("computer", typeof textOrObj === "string" ? { action: "clipboard_set", text: textOrObj } : { ...textOrObj, action: "clipboard_set" });
        computerFn.screenshot = (opts = {}) => callTool("computer", { ...opts, action: "screenshot" });
        computerFn.cursor_position = (opts = {}) => callTool("computer", { ...opts, action: "cursor_position" });
        computerFn.move = (opts = {}) => callTool("computer", { ...opts, action: "move" });
        computerFn.click = (opts = {}) => callTool("computer", typeof opts === "string" ? { action: "click", ref: opts } : { ...opts, action: "click" });
        computerFn.mouse_down = (opts = {}) => callTool("computer", { ...opts, action: "mouse_down" });
        computerFn.mouse_up = (opts = {}) => callTool("computer", { ...opts, action: "mouse_up" });
        computerFn.drag = (opts = {}) => callTool("computer", { ...opts, action: "drag" });
        computerFn.scroll = (opts = {}) => callTool("computer", { ...opts, action: "scroll" });
        computerFn.type = (textOrObj = {}) => callTool("computer", typeof textOrObj === "string" ? { action: "type", text: textOrObj } : { ...textOrObj, action: "type" });
        computerFn.key = (keyOrObj = {}) => callTool("computer", typeof keyOrObj === "string" ? { action: "key", key: keyOrObj } : { ...keyOrObj, action: "key" });
        computerFn.hold_key = (opts = {}) => callTool("computer", { ...opts, action: "hold_key" });
        computerFn.wait = (msOrObj = {}) => callTool("computer", typeof msOrObj === "number" ? { action: "wait", duration_ms: msOrObj } : { ...msOrObj, action: "wait" });
        computerFn.zoom = (opts = {}) => callTool("computer", { ...opts, action: "zoom" });
        computerFn.action = (actionJson = {}) => callTool("computer", actionJson);
        Object.freeze(computerFn);

        const deviceFn = (req = {}) => callTool("device", typeof req === "string" ? { action: req } : req);
        deviceFn.screenshot = (opts = {}) => callTool("device", { ...opts, action: "screenshot" });
        deviceFn.tap = (opts = {}) => callTool("device", { ...opts, action: "tap" });
        deviceFn.swipe = (opts = {}) => callTool("device", { ...opts, action: "swipe" });
        deviceFn.type = (textOrObj = {}) => callTool("device", typeof textOrObj === "string" ? { action: "type", text: textOrObj } : { ...textOrObj, action: "type" });
        deviceFn.press = (btnOrObj = {}) => callTool("device", typeof btnOrObj === "string" ? { action: "press", button: btnOrObj } : { ...btnOrObj, action: "press" });
        deviceFn.install_app = (apkOrObj = {}) => callTool("device", typeof apkOrObj === "string" ? { action: "install_app", apk_path: apkOrObj } : { ...apkOrObj, action: "install_app" });
        deviceFn.launch_app = (pkgOrObj = {}) => callTool("device", typeof pkgOrObj === "string" ? { action: "launch_app", package: pkgOrObj } : { ...pkgOrObj, action: "launch_app" });
        deviceFn.terminate_app = (pkgOrObj = {}) => callTool("device", typeof pkgOrObj === "string" ? { action: "terminate_app", package: pkgOrObj } : { ...pkgOrObj, action: "terminate_app" });
        deviceFn.open_url = (urlOrObj = {}) => callTool("device", typeof urlOrObj === "string" ? { action: "open_url", url: urlOrObj } : { ...urlOrObj, action: "open_url" });
        deviceFn.rotate = (orientOrObj = {}) => callTool("device", typeof orientOrObj === "string" ? { action: "rotate", orientation: orientOrObj } : { ...orientOrObj, action: "rotate" });
        Object.freeze(deviceFn);

        const mediaFn = (req = {}) => callTool("media", req);
        mediaFn.image_generate = (promptOrObj = {}) => callTool("media", typeof promptOrObj === "string" ? { action: "image_generate", prompt: promptOrObj } : { ...promptOrObj, action: "image_generate" });
        mediaFn.speech = (inputOrObj = {}) => callTool("media", typeof inputOrObj === "string" ? { action: "speech", input: inputOrObj } : { ...inputOrObj, action: "speech" });
        mediaFn.transcribe = (pathOrObj = {}) => callTool("media", typeof pathOrObj === "string" ? { action: "transcribe", file_path: pathOrObj } : { ...pathOrObj, action: "transcribe" });
        Object.freeze(mediaFn);

        const anyagents = Object.freeze({
          memory: memoryFn,
          web: webFn,
          computer: computerFn,
          device: deviceFn,
          media: mediaFn,
          agents: (req = {}) => callTool("agents", req),
          send: (req = {}) => callTool("send", req),
          conversation: (req = {}) => callTool("conversation", req),
          search: (req = {}) => callTool("conversation", req),
          groups: (req = {}) => callTool("groups", req),
          schedule: (req = {}) => callTool("schedule", req),
          browser: (req = {}) => callTool("browser", req),
          files: (req = {}) => callTool("files", req),
          subagents: (req = {}) => callTool("subagents", req),
          spawn: (req = {}) => callTool("spawn", req),
          describe: (req = {}) => callTool("describe", req),
          archive: (req = {}) => callTool("archive", req),
          wake: (req = {}) => callTool("wake", req),
          interact: (req = {}) => callTool("interact", req),
        });
        const workspace = Object.freeze({
          read: (opts = {}) => callTool("files", { ...(typeof opts === "string" ? { path: opts } : opts), action: "read" }),
          write: (opts = {}, content) => callTool("files", typeof opts === "string" ? { path: opts, content: content ?? "", action: "write" } : { ...opts, action: "write" }),
          edit: (opts = {}) => callTool("files", { ...opts, action: "edit" }),
          multi_edit: (opts = {}) => callTool("files", { ...opts, action: "multi_edit" }),
          patch: (opts = {}) => callTool("files", { ...(typeof opts === "string" ? { patch: opts } : opts), action: "patch" }),
          glob: (opts = {}) => callTool("files", { ...(typeof opts === "string" ? { pattern: opts } : opts), action: "glob" }),
          grep: (opts = {}) => callTool("files", { ...(typeof opts === "string" ? { query: opts } : opts), action: "grep" }),
          list: (opts = {}) => callTool("files", { ...(typeof opts === "string" ? { path: opts } : opts), action: "list" }),
          exec: (opts = {}) => callTool("files", { ...(typeof opts === "string" ? { cmd: opts } : opts), action: "exec" }),
        });
        const tools = Object.freeze({ anyagents, workspace, memory: memoryFn, web: webFn, computer: computerFn, device: deviceFn, media: mediaFn });
        const console = Object.freeze({
          log: (...a) => log("log", a),
          info: (...a) => log("info", a),
          warn: (...a) => log("warn", a),
          error: (...a) => log("error", a),
          debug: (...a) => log("debug", a),
        });
        const setTimeout = (cb, ms = 0) => {
          hostSleepRaw(Number(ms) || 0, () => {
            if (typeof cb === "function") cb();
          });
        };
        globalThis.tools = tools;
        globalThis.anyagents = anyagents;
        globalThis.workspace = workspace;
        globalThis.memory = memoryFn;
        globalThis.web = webFn;
        globalThis.computer = computerFn;
        globalThis.device = deviceFn;
        globalThis.media = mediaFn;
        globalThis.console = console;
        globalThis.setTimeout = setTimeout;
        return Object.freeze({ tools, anyagents, workspace, memory: memoryFn, web: webFn, computer: computerFn, device: deviceFn, media: mediaFn, console });
      })`, ctx);
      const env = bootstrap(hostCallRaw, hostLogRaw, hostSleepRaw);
      const fn = compileUserCode(msg.code, ctx);
      try {
        Object.defineProperty(Function.prototype, "constructor", { value: undefined, writable: false, configurable: false });
        Object.defineProperty((async () => {}).constructor.prototype, "constructor", { value: undefined, writable: false, configurable: false });
      } catch {}
      const result = await fn(env.tools, env.anyagents, env.workspace, env.web, env.console);
      let safeResult = result === undefined ? null : result;
      let doneLine;
      try {
        doneLine = JSON.stringify({ type: "done", ok: true, result: safeResult, logs }) + "\n";
      } catch {
        doneLine = JSON.stringify({ type: "done", ok: true, result: String(safeResult), logs }) + "\n";
      }
      stdoutWrite(doneLine, () => exitProcess(0));
    } catch (err) {
      const errMsg = err && err.stack ? String(err.stack) : String(err);
      stdoutWrite(JSON.stringify({ type: "done", ok: false, error: errMsg, logs }) + "\n", () => exitProcess(0));
    }
  }
});
"#;

struct CodeChildGuard {
    child: Option<std::process::Child>,
}

impl Drop for CodeChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            {
                let pgid = child.id() as i32;
                if pgid > 0 {
                    unsafe {
                        let _ = libc::kill(-pgid, libc::SIGKILL);
                    }
                }
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

static CACHED_JS_RUNTIME: std::sync::OnceLock<Option<(PathBuf, bool)>> = std::sync::OnceLock::new();

fn find_js_runtime() -> Option<(PathBuf, bool)> {
    CACHED_JS_RUNTIME
        .get_or_init(|| {
            let mut dirs = Vec::new();
            if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
                dirs.push(home.join(".local/opt/node/bin"));
                dirs.push(home.join(".local/opt/bun/bin"));
                if let Some(host_home) = crate::sandbox::host_home_dir() {
                    dirs.push(host_home.join(".local/opt/node/bin"));
                    dirs.push(host_home.join(".local/opt/bun/bin"));
                }
                dirs.push(home.join(".local/bin"));
                dirs.push(home.join(".bun/bin"));
            } else if let Some(host_home) = crate::sandbox::host_home_dir() {
                dirs.push(host_home.join(".local/opt/node/bin"));
                dirs.push(host_home.join(".local/opt/bun/bin"));
            }
            if let Some(path_os) = std::env::var_os("PATH") {
                let path_dirs: Vec<PathBuf> = std::env::split_paths(&path_os).collect();
                for dir in &path_dirs {
                    if dir.ends_with(".local/bin")
                        && let Some(local_root) = dir.parent()
                    {
                        dirs.push(local_root.join("opt/node/bin"));
                        dirs.push(local_root.join("opt/bun/bin"));
                    }
                }
                dirs.extend(path_dirs);
            }
            dirs.push(PathBuf::from("/usr/local/bin"));
            dirs.push(PathBuf::from("/usr/bin"));
            dirs.push(PathBuf::from("/bin"));

            for (bin_name, is_node) in [("node", true), ("bun", false)] {
                for dir in &dirs {
                    let candidate = dir.join(bin_name);
                    if candidate.is_file() {
                        return Some((candidate, is_node));
                    }
                }
            }
            None
        })
        .clone()
}

impl AgentMcp {
    fn resolve_workdir(&self) -> PathBuf {
        if let Some(ref wd) = self.agent.workdir
            && !wd.as_os_str().is_empty()
        {
            return wd.clone();
        }
        if let Some(agent_root) = self.client.config.parent() {
            let sibling_work = agent_root.join("work");
            if sibling_work.is_dir() {
                return sibling_work;
            }
        }
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    }

    async fn execute_memory_tool(&self, args: MemoryArgs) -> Result<serde_json::Value, String> {
        let raw_topic = args
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let raw_query = args
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let is_index_topic = raw_topic.as_deref().is_some_and(|t| {
            t.eq_ignore_ascii_case("memory")
                || t.eq_ignore_ascii_case("memory.md")
                || t.eq_ignore_ascii_case("index")
        });

        let mut action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| {
                if args.delete == Some(true) {
                    "delete".to_string()
                } else if args.content.is_some() {
                    "write".to_string()
                } else if raw_query.is_some() {
                    "search".to_string()
                } else if raw_topic.is_some() && !is_index_topic {
                    "read".to_string()
                } else {
                    "list".to_string()
                }
            });
        if (action == "read" || action == "get") && (raw_topic.is_none() || is_index_topic) {
            action = "list".to_string();
        }

        let target_arg = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string());
        let is_self_target = target_arg.as_deref().is_none_or(|t| {
            t.eq_ignore_ascii_case("self")
                || t.eq_ignore_ascii_case(&self.agent.name)
                || t == self.agent.id.as_str()
        });
        let local_agent_root = self.client.config.parent().map(Path::to_path_buf);

        match action.as_str() {
            "list" | "ls" | "index" => {
                let mut index_text = String::new();
                let mut topics_val = Vec::new();
                let mut turns_val = Vec::new();

                if is_self_target && let Some(ref root) = local_agent_root {
                    if let Ok(topics) = anyagents_core::learned_memory::list_memory_topics(root) {
                        if let Some(lim) = args.limit {
                            topics_val = topics
                                .into_iter()
                                .take(lim.max(1))
                                .filter_map(|t| serde_json::to_value(t).ok())
                                .collect();
                        } else {
                            topics_val = topics
                                .into_iter()
                                .filter_map(|t| serde_json::to_value(t).ok())
                                .collect();
                        }
                    }
                    if let Ok(txt) = std::fs::read_to_string(root.join("memory/MEMORY.md")) {
                        index_text = txt;
                    }
                }

                if (index_text.is_empty() || !is_self_target)
                    && let Ok(resp) = self
                        .conversation(Parameters(ConversationArgs {
                            scope: Some("memory".into()),
                            topic: Some("MEMORY".into()),
                            agent: target_arg.clone(),
                            ..ConversationArgs::default()
                        }))
                        .await
                {
                    if let Some(first) = resp.0.turns.first()
                        && index_text.is_empty()
                    {
                        index_text = first.text.clone();
                    }
                    turns_val = resp
                        .0
                        .turns
                        .into_iter()
                        .filter_map(|t| serde_json::to_value(t).ok())
                        .collect();
                }
                if index_text.is_empty() {
                    index_text = format!(
                        "# {} — Learned Memory Index\n- No topic files recorded yet.\n",
                        target_arg.as_deref().unwrap_or(&self.agent.name)
                    );
                }
                let count = topics_val.len();
                Ok(serde_json::json!({
                    "action": "list",
                    "agent": target_arg.unwrap_or_else(|| self.agent.name.clone()),
                    "index": index_text,
                    "topics": topics_val,
                    "count": count,
                    "turns": turns_val,
                }))
            }
            "read" | "get" => {
                let topic =
                    raw_topic.ok_or_else(|| "`memory` read requires `topic`".to_string())?;
                let slug = anyagents_core::learned_memory::sanitize_topic_slug(&topic)
                    .map_err(|e| e.to_string())?;
                let filename = format!("{slug}.md");

                match self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("memory".into()),
                        topic: Some(slug.clone()),
                        agent: target_arg.clone(),
                        ..ConversationArgs::default()
                    }))
                    .await
                {
                    Ok(resp) => {
                        let content = resp
                            .0
                            .turns
                            .first()
                            .map(|t| t.text.clone())
                            .unwrap_or_default();
                        let (_, description) =
                            anyagents_core::learned_memory::parse_topic_frontmatter(
                                &slug, &content,
                            );
                        Ok(serde_json::json!({
                            "action": "read",
                            "agent": resp.0.agent,
                            "topic": slug,
                            "filename": filename,
                            "description": description,
                            "content": content,
                            "turns": resp.0.turns,
                        }))
                    }
                    Err(err) => {
                        if is_self_target && let Some(ref root) = local_agent_root {
                            let path = root.join("memory").join(&filename);
                            if let Ok(content) = std::fs::read_to_string(&path) {
                                let (_, description) =
                                    anyagents_core::learned_memory::parse_topic_frontmatter(
                                        &slug, &content,
                                    );
                                return Ok(serde_json::json!({
                                    "action": "read",
                                    "agent": self.agent.name,
                                    "topic": slug,
                                    "filename": filename,
                                    "description": description,
                                    "content": content,
                                    "turns": [{
                                        "id": format!("memory:{}:{}", self.agent.id, slug),
                                        "text": content,
                                        "source": "memory",
                                        "direction": "in",
                                        "from": format!("agent:{}", self.agent.name),
                                        "to": format!("agent:{}", self.agent.name),
                                        "reply_to": filename,
                                    }],
                                }));
                            }
                        }
                        Err(err)
                    }
                }
            }
            "write" | "upsert" | "set" => {
                let topic =
                    raw_topic.ok_or_else(|| "`memory` write requires `topic`".to_string())?;
                let slug = anyagents_core::learned_memory::sanitize_topic_slug(&topic)
                    .map_err(|e| e.to_string())?;
                let content = args.content.unwrap_or_default();

                match self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("memory".into()),
                        topic: Some(slug.clone()),
                        description: args.description.clone(),
                        content: Some(content.clone()),
                        agent: target_arg.clone(),
                        ..ConversationArgs::default()
                    }))
                    .await
                {
                    Ok(resp) => {
                        let written_text = resp
                            .0
                            .turns
                            .first()
                            .map(|t| t.text.clone())
                            .unwrap_or_default();
                        let (_, description) =
                            anyagents_core::learned_memory::parse_topic_frontmatter(
                                &slug,
                                &written_text,
                            );
                        let sha256 = sha256_hex(written_text.as_bytes());
                        Ok(serde_json::json!({
                            "action": "write",
                            "agent": resp.0.agent,
                            "topic": slug,
                            "filename": format!("{slug}.md"),
                            "description": description,
                            "content": written_text,
                            "sha256": sha256,
                            "turns": resp.0.turns,
                        }))
                    }
                    Err(err) => {
                        if is_self_target && let Some(ref root) = local_agent_root {
                            let (path, entry, index_md) =
                                anyagents_core::learned_memory::upsert_memory_topic(
                                    root,
                                    &self.agent.name,
                                    None,
                                    &slug,
                                    args.description.as_deref(),
                                    &content,
                                    anyagents_core::now_ms(),
                                )
                                .map_err(|e| e.to_string())?;
                            let written_text = std::fs::read_to_string(&path).unwrap_or_default();
                            let sha256 = sha256_hex(written_text.as_bytes());
                            return Ok(serde_json::json!({
                                "action": "write",
                                "agent": self.agent.name,
                                "topic": entry.slug,
                                "filename": entry.filename,
                                "description": entry.description,
                                "content": written_text,
                                "index": index_md,
                                "sha256": sha256,
                                "turns": [{
                                    "id": format!("memory:{}:{}", self.agent.id, slug),
                                    "text": written_text,
                                    "source": "memory",
                                    "direction": "in",
                                    "from": format!("agent:{}", self.agent.name),
                                    "to": format!("agent:{}", self.agent.name),
                                    "reply_to": format!("{slug}.md"),
                                }],
                            }));
                        }
                        Err(err)
                    }
                }
            }
            "delete" | "remove" | "rm" => {
                let topic =
                    raw_topic.ok_or_else(|| "`memory` delete requires `topic`".to_string())?;
                let slug = anyagents_core::learned_memory::sanitize_topic_slug(&topic)
                    .map_err(|e| e.to_string())?;

                match self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("memory".into()),
                        topic: Some(slug.clone()),
                        delete: Some(true),
                        agent: target_arg.clone(),
                        ..ConversationArgs::default()
                    }))
                    .await
                {
                    Ok(resp) => {
                        let first_text = resp
                            .0
                            .turns
                            .first()
                            .map(|t| t.text.clone())
                            .unwrap_or_default();
                        let deleted = first_text.starts_with("deleted=true");
                        let index = first_text
                            .split_once('\n')
                            .map(|(_, rest)| rest.to_string())
                            .unwrap_or_default();
                        Ok(serde_json::json!({
                            "action": "delete",
                            "agent": resp.0.agent,
                            "topic": slug,
                            "deleted": deleted,
                            "index": index,
                            "turns": resp.0.turns,
                        }))
                    }
                    Err(err) => {
                        if is_self_target && let Some(ref root) = local_agent_root {
                            let (removed, index) =
                                anyagents_core::learned_memory::delete_memory_topic(
                                    root,
                                    &self.agent.name,
                                    None,
                                    &slug,
                                )
                                .map_err(|e| e.to_string())?;
                            return Ok(serde_json::json!({
                                "action": "delete",
                                "agent": self.agent.name,
                                "topic": slug,
                                "deleted": removed,
                                "index": index,
                                "turns": [{
                                    "id": format!("memory:{}:deleted:{}", self.agent.id, slug),
                                    "text": format!("deleted={removed}\n{index}"),
                                    "source": "memory",
                                    "direction": "in",
                                    "from": format!("agent:{}", self.agent.name),
                                    "to": format!("agent:{}", self.agent.name),
                                }],
                            }));
                        }
                        Err(err)
                    }
                }
            }
            "search" | "query" | "find" => {
                let q = raw_query
                    .or(raw_topic)
                    .ok_or_else(|| "`memory` search requires `query`".to_string())?;
                match self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("memory".into()),
                        query: Some(q.clone()),
                        agent: target_arg.clone(),
                        limit: args.limit,
                        ..ConversationArgs::default()
                    }))
                    .await
                {
                    Ok(resp) => {
                        let count = resp.0.turns.len();
                        Ok(serde_json::json!({
                            "action": "search",
                            "agent": resp.0.agent,
                            "query": q,
                            "turns": resp.0.turns,
                            "matches": resp.0.turns,
                            "count": count,
                        }))
                    }
                    Err(err) => {
                        if is_self_target
                            && let Some(ref root) = local_agent_root
                            && let Ok(topics) =
                                anyagents_core::learned_memory::list_memory_topics(root)
                        {
                            let tokens: Vec<String> = q
                                .to_ascii_lowercase()
                                .split_whitespace()
                                .filter(|t| !t.is_empty())
                                .map(str::to_string)
                                .collect();
                            let limit = args.limit.unwrap_or(20).max(1);
                            let mut hits = Vec::new();
                            for entry in topics {
                                let path = root.join("memory").join(&entry.filename);
                                let text = std::fs::read_to_string(&path).unwrap_or_default();
                                let hay = format!("{} {} {}", entry.slug, entry.description, text)
                                    .to_ascii_lowercase();
                                if tokens.iter().all(|tok| hay.contains(tok)) {
                                    hits.push(serde_json::json!({
                                        "id": format!("memory:{}:{}", self.agent.id, entry.slug),
                                        "topic": entry.slug,
                                        "filename": entry.filename,
                                        "description": entry.description,
                                        "text": text,
                                        "source": "memory",
                                        "direction": "in",
                                        "from": format!("agent:{}", self.agent.name),
                                        "to": format!("agent:{}", self.agent.name),
                                        "reply_to": entry.filename,
                                        "timestamp_ms": entry.updated_at_ms,
                                    }));
                                    if hits.len() >= limit {
                                        break;
                                    }
                                }
                            }
                            let count = hits.len();
                            return Ok(serde_json::json!({
                                "action": "search",
                                "agent": self.agent.name,
                                "query": q,
                                "turns": hits,
                                "matches": hits,
                                "count": count,
                            }));
                        }
                        Err(err)
                    }
                }
            }
            "status" => {
                if let Some(ref root) = local_agent_root {
                    let topics = anyagents_core::learned_memory::list_memory_topics(root)
                        .unwrap_or_default();
                    let sleep_state = anyagents_core::learned_memory::read_sleep_state(root);
                    let index_text =
                        std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap_or_default();
                    return Ok(serde_json::json!({
                        "action": "status",
                        "agent": self.agent.name,
                        "topic_count": topics.len(),
                        "index_bytes": index_text.len(),
                        "index_lines": index_text.lines().count(),
                        "last_consolidated_ms": sleep_state.last_consolidated_ms,
                        "consolidated_topics": sleep_state.consolidated_topics,
                    }));
                }
                Err("local memory root is not available for `memory.status`".to_string())
            }
            "recall" | "history" => {
                let q = raw_query
                    .or(raw_topic)
                    .ok_or_else(|| "`memory` recall requires `query`".to_string())?;
                let resp = self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("all".into()),
                        query: Some(q.clone()),
                        agent: target_arg.clone(),
                        limit: args.limit,
                        ..ConversationArgs::default()
                    }))
                    .await?;
                let count = resp.0.turns.len();
                Ok(serde_json::json!({
                    "action": "recall",
                    "agent": resp.0.agent,
                    "query": q,
                    "turns": resp.0.turns,
                    "matches": resp.0.turns,
                    "count": count,
                }))
            }
            "consolidate" | "sleep" => {
                if let Some(ref root) = local_agent_root {
                    let index_md = anyagents_core::learned_memory::rebuild_memory_index(
                        root,
                        &self.agent.name,
                        None,
                    )
                    .map_err(|e| e.to_string())?;
                    let topics = anyagents_core::learned_memory::list_memory_topics(root)
                        .unwrap_or_default();
                    return Ok(serde_json::json!({
                        "action": "consolidate",
                        "agent": self.agent.name,
                        "topic_count": topics.len(),
                        "index": index_md,
                    }));
                }
                Err("local memory root is not available for `memory.consolidate`".to_string())
            }
            other => Err(format!(
                "unknown `memory` action `{other}`; expected `list`, `read`, `write`, `delete`, `search`, `recall`, `status`, or `consolidate`"
            )),
        }
    }

    async fn dispatch_code_tool(
        &self,
        tool: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let norm = tool
            .trim()
            .strip_prefix("mcp__anyagents__")
            .unwrap_or(tool.trim());
        match norm {
            "memory" => {
                let parsed: MemoryArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `memory` arguments: {e}"))?;
                Ok(self.memory(Parameters(parsed)).await?.0)
            }
            "agents" => {
                let parsed: AgentsArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `agents` arguments: {e}"))?;
                Ok(self.agents(Parameters(parsed)).await?.0)
            }
            "send" => {
                let parsed: SendArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `send` arguments: {e}"))?;
                Ok(self.send(Parameters(parsed)).await?.0)
            }
            "conversation" | "search" => {
                let parsed: ConversationArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `conversation` arguments: {e}"))?;
                let res = self.conversation(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "groups" => {
                let parsed: GroupsArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `groups` arguments: {e}"))?;
                let res = self.groups(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "schedule" => {
                let parsed: ScheduleArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `schedule` arguments: {e}"))?;
                Ok(self.schedule(Parameters(parsed)).await?.0)
            }
            "browser" => {
                let parsed: crate::browser::BrowserRequest = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `browser` arguments: {e}"))?;
                Ok(self.browser(Parameters(parsed)).await?.0)
            }
            "web" | "web_search" | "web_fetch" => {
                let mut parsed: crate::web::WebRequest = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `web` arguments: {e}"))?;
                if parsed.action.is_none() {
                    if norm == "web_search" {
                        parsed.action = Some("search".to_string());
                    } else if norm == "web_fetch" {
                        parsed.action = Some("fetch".to_string());
                    }
                }
                Ok(self.web(Parameters(parsed)).await?.0)
            }
            "computer" => Ok(self
                .computer(Parameters(DynamicObjectArgs::from_value(args)?))
                .await?
                .0),
            "device" => Ok(self
                .device(Parameters(DynamicObjectArgs::from_value(args)?))
                .await?
                .0),
            "media" => Ok(self
                .media(Parameters(DynamicObjectArgs::from_value(args)?))
                .await?
                .0),
            "files" => {
                let parsed: FilesArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `files` arguments: {e}"))?;
                Ok(self.files(Parameters(parsed)).await?.0)
            }
            "subagents" => {
                let parsed: SubagentsArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `subagents` arguments: {e}"))?;
                Ok(self.subagents(Parameters(parsed)).await?.0)
            }
            "spawn" => {
                let parsed: SpawnArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `spawn` arguments: {e}"))?;
                let res = self.spawn(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "describe" => {
                let parsed: DescribeArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `describe` arguments: {e}"))?;
                Ok(self.describe(Parameters(parsed)).await?.0)
            }
            "archive" => {
                let parsed: ArchiveArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `archive` arguments: {e}"))?;
                let res = self.archive(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "wake" => {
                let parsed: WakeArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `wake` arguments: {e}"))?;
                let res = self.wake(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "interact" => {
                let parsed: InteractArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `interact` arguments: {e}"))?;
                let res = self.interact(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            other => Err(format!(
                "unknown AnyAgents tool `{other}` in code mode; expected one of `memory`, `web`, `agents`, `send`, `conversation`, `groups`, `schedule`, `browser`, `files`"
            )),
        }
    }

    async fn execute_code_tool(&self, args: CodeArgs) -> Result<serde_json::Value, String> {
        use std::io::{BufRead, BufReader, Write};
        use std::process::{Command, Stdio};
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};

        let timeout_ms = args
            .timeout_ms
            .unwrap_or(DEFAULT_CODE_TIMEOUT_MS)
            .clamp(100, MAX_CODE_TIMEOUT_MS);
        let (runtime_bin, is_node) = find_js_runtime().ok_or_else(|| {
            "neither `node` nor `bun` was found on this machine to execute `code` mode".to_string()
        })?;

        let started = Instant::now();
        let mut cmd = Command::new(&runtime_bin);
        cmd.env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .env("DESK_NODE_SHIM", "off")
            .env("NODE_NO_WARNINGS", "1");
        if let Some(home) = std::env::var_os("HOME") {
            cmd.env("HOME", home);
        }
        if let Some(host_home) = std::env::var_os("ANYAGENTS_HOST_HOME") {
            cmd.env("ANYAGENTS_HOST_HOME", &host_home);
        }
        if let Some(node_dir) = std::env::var_os("DESK_NODE_DEFAULT_DIR") {
            cmd.env("DESK_NODE_DEFAULT_DIR", node_dir);
        } else if let Some(host_home) = crate::sandbox::host_home_dir() {
            let opt_node = host_home.join(".local/opt/node/bin");
            if opt_node.is_dir() {
                cmd.env("DESK_NODE_DEFAULT_DIR", opt_node);
            }
        }
        if is_node {
            cmd.arg("--no-warnings");
        }
        cmd.arg("-e")
            .arg(CODE_SANDBOX_WORKER_JS)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            cmd.process_group(0);
        }

        let mut child = cmd.spawn().map_err(|e| {
            format!(
                "failed to spawn code sandbox ({}): {e}",
                runtime_bin.display()
            )
        })?;
        let mut child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| "code sandbox stdin missing".to_string())?;
        let child_stdout = child
            .stdout
            .take()
            .ok_or_else(|| "code sandbox stdout missing".to_string())?;
        let child_stderr = child
            .stderr
            .take()
            .ok_or_else(|| "code sandbox stderr missing".to_string())?;
        let _guard = CodeChildGuard { child: Some(child) };

        let start_payload = serde_json::json!({
            "type": "start",
            "code": args.code,
        });
        let mut start_line = serde_json::to_vec(&start_payload).map_err(|e| e.to_string())?;
        start_line.push(b'\n');
        child_stdin
            .write_all(&start_line)
            .and_then(|()| child_stdin.flush())
            .map_err(|e| format!("failed to send code to sandbox: {e}"))?;

        let shared_stdin = Arc::new(Mutex::new(child_stdin));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();

        std::thread::spawn(move || {
            let reader = BufReader::new(child_stdout);
            for line in reader.lines().map_while(Result::ok) {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed)
                    && tx.send(val).is_err()
                {
                    break;
                }
            }
        });

        let stderr_handle = std::thread::spawn(move || {
            use std::io::Read;
            let mut buf = Vec::new();
            let mut take = child_stderr.take(16 * 1024);
            let _ = take.read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).into_owned()
        });

        // Receipts come from the actual host dispatch, never snippet return
        // values, source text or logs, which can contain invented deliveries.
        let deliveries = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let mut in_flight = tokio::task::JoinSet::new();
        let mut call_count = 0usize;

        let loop_res = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
            while let Some(msg) = rx.recv().await {
                let msg_type = msg.get("type").and_then(serde_json::Value::as_str).unwrap_or("");
                match msg_type {
                    "call" => {
                        call_count += 1;
                        let id = msg.get("id").and_then(serde_json::Value::as_u64).unwrap_or(0);
                        if call_count > MAX_CODE_TOOL_CALLS {
                            let reply = serde_json::json!({
                                "type": "reply",
                                "id": id,
                                "ok": false,
                                "error": format!("exceeded maximum of {MAX_CODE_TOOL_CALLS} tool calls per `code` turn"),
                            });
                            if let Ok(mut guard) = shared_stdin.lock() {
                                let _ = writeln!(guard, "{reply}");
                                let _ = guard.flush();
                            }
                            continue;
                        }
                        let tool_name = msg
                            .get("tool")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let tool_args = msg
                            .get("args")
                            .cloned()
                            .unwrap_or_else(|| serde_json::json!({}));
                        let mcp = self.clone();
                        let stdin_clone = Arc::clone(&shared_stdin);
                        let deliveries = Arc::clone(&deliveries);
                        in_flight.spawn(async move {
                            let res = mcp.dispatch_code_tool(&tool_name, tool_args).await;
                            if tool_name.trim().trim_start_matches("mcp__anyagents__") == "send"
                                && let Ok(val) = &res
                                && let Some(items) = val.get("deliveries").and_then(serde_json::Value::as_array)
                            {
                                deliveries.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .extend(items.iter().cloned());
                            }
                            let reply = match res {
                                Ok(val) => serde_json::json!({
                                    "type": "reply",
                                    "id": id,
                                    "ok": true,
                                    "result": val,
                                }),
                                Err(err) => serde_json::json!({
                                    "type": "reply",
                                    "id": id,
                                    "ok": false,
                                    "error": err,
                                }),
                            };
                            if let Ok(mut guard) = stdin_clone.lock() {
                                let _ = writeln!(guard, "{reply}");
                                let _ = guard.flush();
                            }
                        });
                    }
                    "done" => {
                        return Ok(msg);
                    }
                    _ => {}
                }
            }
            Err("code sandbox exited before returning a result".to_string())
        })
        .await;

        in_flight.abort_all();
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(timeout_ms);

        let done_msg = match loop_res {
            Ok(Ok(msg)) => msg,
            Ok(Err(err)) => {
                let stderr_text = stderr_handle.join().unwrap_or_default();
                let stderr_trimmed = stderr_text.trim();
                if stderr_trimmed.is_empty() {
                    return Err(err);
                }
                return Err(format!("{err}: {stderr_trimmed}"));
            }
            Err(_) => {
                return Err(format!(
                    "`code` execution timed out after {timeout_ms}ms and was terminated"
                ));
            }
        };

        let logs: Vec<String> = done_msg
            .get("logs")
            .and_then(serde_json::Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();

        let ok = done_msg
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if !ok {
            let err = done_msg
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown code execution error");
            if logs.is_empty() {
                return Err(format!("code execution error: {err}"));
            }
            return Err(format!(
                "code execution error: {err}\nlogs:\n{}",
                logs.join("\n")
            ));
        }

        let deliveries = deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let raw_result = done_msg
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let serialized = serde_json::to_string(&raw_result).unwrap_or_default();
        if serialized.len() > MAX_CODE_RESULT_BYTES {
            let head_end = floor_char_boundary_str(&serialized, 36 * 1024);
            let tail_start =
                ceil_char_boundary_str(&serialized, serialized.len().saturating_sub(8 * 1024));
            let preview = format!(
                "{}\n...[result truncated: {} bytes exceeded {} byte cap; filter or map fields inside `code` before returning]...\n{}",
                &serialized[..head_end],
                serialized.len(),
                MAX_CODE_RESULT_BYTES,
                &serialized[tail_start..]
            );
            return Ok(serde_json::json!({
                "result": preview,
                "deliveries": deliveries,
                "truncated": true,
                "original_bytes": serialized.len(),
                "max_bytes": MAX_CODE_RESULT_BYTES,
                "logs": logs,
                "duration_ms": duration_ms,
            }));
        }

        Ok(serde_json::json!({
            "result": raw_result,
            "deliveries": deliveries,
            "truncated": false,
            "logs": logs,
            "duration_ms": duration_ms,
        }))
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn floor_char_boundary_str(s: &str, index: usize) -> usize {
    if index >= s.len() {
        return s.len();
    }
    let mut i = index;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn ceil_char_boundary_str(s: &str, index: usize) -> usize {
    if index >= s.len() {
        return s.len();
    }
    let mut i = index;
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                    out.pop();
                } else if !out.is_absolute() {
                    out.push("..");
                }
            }
            Component::Normal(c) => out.push(c),
        }
    }
    if out.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        out
    }
}

fn resolve_workspace_path(
    base: &Path,
    raw: Option<&str>,
    required: bool,
) -> Result<PathBuf, String> {
    let Some(raw_str) = raw else {
        if required {
            return Err("`path` is required for this `files` action".to_string());
        }
        return Ok(normalize_lexical_path(base));
    };
    if raw_str.contains('\0') {
        return Err("`path` must not contain null bytes".to_string());
    }
    let trimmed = raw_str.trim();
    if trimmed.is_empty() {
        if required {
            return Err("`path` must not be empty".to_string());
        }
        return Ok(normalize_lexical_path(base));
    }
    let candidate = Path::new(trimmed);
    let joined = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        base.join(candidate)
    };
    Ok(normalize_lexical_path(&joined))
}

fn execute_files_tool(base_dir: &Path, args: FilesArgs) -> Result<serde_json::Value, String> {
    let action = args
        .action
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| {
            if args.cmd.is_some() {
                "exec".to_string()
            } else if args.patch.is_some() {
                "patch".to_string()
            } else if !args.edits.is_empty() {
                "multi_edit".to_string()
            } else if args.target.is_some() {
                "edit".to_string()
            } else if args.content.is_some() {
                "write".to_string()
            } else if args.query.is_some() {
                "grep".to_string()
            } else if args.pattern.is_some() || args.glob.is_some() {
                "glob".to_string()
            } else {
                "read".to_string()
            }
        });

    match action.as_str() {
        "read" | "cat" | "view" => {
            let resolved = resolve_workspace_path(base_dir, args.path.as_deref(), true)?;
            let meta = std::fs::metadata(&resolved)
                .map_err(|e| format!("cannot read {}: {e}", resolved.display()))?;
            if meta.is_dir() {
                return Err(format!(
                    "{} is a directory; use `action: \"list\"` or `action: \"glob\"` to list files",
                    resolved.display()
                ));
            }
            let full_bytes = std::fs::read(&resolved)
                .map_err(|e| format!("cannot read {}: {e}", resolved.display()))?;
            let sha256 = sha256_hex(&full_bytes);
            let total_bytes = full_bytes.len();
            let text = String::from_utf8_lossy(&full_bytes);
            let all_lines: Vec<&str> = text.lines().collect();
            let total_lines = all_lines.len();

            let max_bytes = args
                .max_bytes
                .unwrap_or(DEFAULT_READ_MAX_BYTES)
                .clamp(1, HARD_READ_MAX_BYTES);
            let byte_offset = args.byte_offset.unwrap_or(0);

            if total_lines == 0 {
                return Ok(serde_json::json!({
                    "action": "read",
                    "path": resolved.display().to_string(),
                    "content": "",
                    "numbered": "",
                    "start_line": 0,
                    "end_line": 0,
                    "total_lines": 0,
                    "bytes_read": 0,
                    "total_bytes": total_bytes,
                    "byte_offset": byte_offset,
                    "truncated": false,
                    "sha256": sha256,
                }));
            }

            let start_line = args.start_line.unwrap_or(1).max(1);
            if start_line > total_lines {
                return Ok(serde_json::json!({
                    "action": "read",
                    "path": resolved.display().to_string(),
                    "content": "",
                    "numbered": "",
                    "start_line": start_line,
                    "end_line": total_lines,
                    "total_lines": total_lines,
                    "bytes_read": 0,
                    "total_bytes": total_bytes,
                    "byte_offset": byte_offset,
                    "truncated": false,
                    "sha256": sha256,
                }));
            }
            let requested_end = args
                .end_line
                .unwrap_or(total_lines)
                .min(total_lines)
                .max(start_line);
            let selected_lines = &all_lines[start_line - 1..requested_end];
            let joined = selected_lines.join("\n");

            let start_byte = floor_char_boundary_str(&joined, byte_offset.min(joined.len()));
            let after_offset = &joined[start_byte..];
            let end_byte = floor_char_boundary_str(after_offset, max_bytes);
            let sliced = &after_offset[..end_byte];
            let truncated = end_byte < after_offset.len() || requested_end < total_lines;

            let offset_lines = joined[..start_byte].bytes().filter(|&b| b == b'\n').count();
            let first_line_no = start_line + offset_lines;
            let sliced_lines: Vec<&str> = sliced.lines().collect();
            let actual_end_line = if sliced_lines.is_empty() {
                first_line_no
            } else {
                first_line_no + sliced_lines.len() - 1
            };
            let mut numbered = String::with_capacity(sliced.len() + sliced_lines.len() * 8);
            for (idx, line) in sliced_lines.iter().enumerate() {
                if idx > 0 {
                    numbered.push('\n');
                }
                numbered.push_str(&format!("{}: {line}", first_line_no + idx));
            }

            Ok(serde_json::json!({
                "action": "read",
                "path": resolved.display().to_string(),
                "content": sliced,
                "numbered": numbered,
                "start_line": first_line_no,
                "end_line": actual_end_line,
                "total_lines": total_lines,
                "bytes_read": sliced.len(),
                "total_bytes": total_bytes,
                "byte_offset": start_byte,
                "truncated": truncated,
                "sha256": sha256,
            }))
        }
        "write" | "create" => {
            let resolved = resolve_workspace_path(base_dir, args.path.as_deref(), true)?;
            let content = args.content.unwrap_or_default();
            let append = args.append.unwrap_or(false);
            let overwrite = args.overwrite.unwrap_or(true);
            if !append && !overwrite && resolved.exists() {
                return Err(format!(
                    "file {} already exists and `overwrite` is false",
                    resolved.display()
                ));
            }
            if let Some(parent) = resolved.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent).map_err(|e| {
                    format!("cannot create parent directory {}: {e}", parent.display())
                })?;
            }
            if append {
                use std::io::Write as _;
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&resolved)
                    .map_err(|e| format!("cannot open {} for append: {e}", resolved.display()))?;
                f.write_all(content.as_bytes())
                    .map_err(|e| format!("cannot append to {}: {e}", resolved.display()))?;
            } else {
                std::fs::write(&resolved, content.as_bytes())
                    .map_err(|e| format!("cannot write {}: {e}", resolved.display()))?;
            }
            let final_bytes = std::fs::read(&resolved)
                .map_err(|e| format!("cannot read back {}: {e}", resolved.display()))?;
            let sha256 = sha256_hex(&final_bytes);
            Ok(serde_json::json!({
                "action": "write",
                "path": resolved.display().to_string(),
                "bytes_written": content.len(),
                "total_bytes": final_bytes.len(),
                "append": append,
                "sha256": sha256,
            }))
        }
        "edit" | "multi_edit" | "replace" => {
            let resolved = resolve_workspace_path(base_dir, args.path.as_deref(), true)?;
            let mut specs = Vec::new();
            if let Some(target) = args.target {
                specs.push(FileEditSpec {
                    target,
                    replacement: args.replacement.unwrap_or_default(),
                    replace_all: args.replace_all,
                });
            }
            specs.extend(args.edits);
            if specs.is_empty() {
                return Err(
                    "`files` edit requires `target` (and optional `replacement`) or non-empty `edits`"
                        .to_string(),
                );
            }
            let existing = std::fs::read_to_string(&resolved)
                .map_err(|e| format!("cannot read {} for edit: {e}", resolved.display()))?;
            let (updated, replacements_made, lines_added, lines_removed) = apply_atomic_edits(
                &existing,
                &specs,
                args.replace_all.unwrap_or(true),
                &resolved,
            )?;
            std::fs::write(&resolved, updated.as_bytes())
                .map_err(|e| format!("cannot write edited file {}: {e}", resolved.display()))?;
            let sha256 = sha256_hex(updated.as_bytes());
            Ok(serde_json::json!({
                "action": if action == "multi_edit" { "multi_edit" } else { "edit" },
                "path": resolved.display().to_string(),
                "replacements_made": replacements_made,
                "lines_added": lines_added,
                "lines_removed": lines_removed,
                "sha256": sha256,
            }))
        }
        "patch" | "apply_patch" => {
            let patch_text = args
                .patch
                .or(args.content)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`files` patch requires non-empty `patch`".to_string())?;
            apply_workspace_patch(base_dir, &patch_text)
        }
        "list" | "ls" | "list_dir" => {
            let dir_path = resolve_workspace_path(base_dir, args.path.as_deref(), false)?;
            let rd = std::fs::read_dir(&dir_path)
                .map_err(|e| format!("cannot list directory {}: {e}", dir_path.display()))?;
            let include_hidden = args.include_hidden.unwrap_or(true);
            let max_results = args
                .max_results
                .unwrap_or(DEFAULT_LIST_MAX_RESULTS)
                .clamp(1, HARD_LIST_MAX_RESULTS);

            let mut raw_entries: Vec<(String, serde_json::Value)> = Vec::new();
            for entry_res in rd {
                let Ok(entry) = entry_res else {
                    continue;
                };
                let name = entry.file_name().to_string_lossy().into_owned();
                if !include_hidden && name.starts_with('.') {
                    continue;
                }
                let entry_path = entry.path();
                let Ok(sym_meta) = std::fs::symlink_metadata(&entry_path) else {
                    continue;
                };
                let ft = sym_meta.file_type();
                let (kind, bytes) = if ft.is_symlink() {
                    let target_len = std::fs::metadata(&entry_path)
                        .ok()
                        .filter(|m| m.is_file())
                        .map_or(0, |m| m.len());
                    ("symlink", target_len)
                } else if ft.is_dir() {
                    ("dir", 0)
                } else {
                    ("file", sym_meta.len())
                };
                let rel_path = entry_path
                    .strip_prefix(base_dir)
                    .ok()
                    .filter(|p| !p.as_os_str().is_empty())
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| entry_path.display().to_string());
                raw_entries.push((
                    name.clone(),
                    serde_json::json!({
                        "name": name,
                        "path": rel_path,
                        "kind": kind,
                        "bytes": bytes,
                    }),
                ));
            }
            raw_entries.sort_by(|a, b| a.0.cmp(&b.0));
            let truncated = raw_entries.len() > max_results;
            let entries: Vec<serde_json::Value> = raw_entries
                .into_iter()
                .take(max_results)
                .map(|(_, v)| v)
                .collect();
            let count = entries.len();
            Ok(serde_json::json!({
                "action": "list",
                "path": dir_path.display().to_string(),
                "entries": entries,
                "count": count,
                "truncated": truncated,
            }))
        }
        "glob" => {
            let mut raw_pattern = args
                .pattern
                .or(args.glob)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            let mut raw_path = args
                .path
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            if raw_pattern.is_none()
                && let Some(ref p) = raw_path
                && (p.contains('*') || p.contains('?') || p.contains('[') || p.contains('{'))
            {
                raw_pattern = Some(p.clone());
                raw_path = None;
            }
            let search_root = resolve_workspace_path(base_dir, raw_path.as_deref(), false)?;
            if !search_root.exists() {
                return Err(format!(
                    "glob base directory {} does not exist",
                    search_root.display()
                ));
            }
            let pattern = raw_pattern.unwrap_or_else(|| "**/*".to_string());
            let clean_pat = pattern.strip_prefix("./").unwrap_or(&pattern);
            let max_results = args
                .max_results
                .unwrap_or(DEFAULT_GLOB_MAX_RESULTS)
                .clamp(1, HARD_GLOB_MAX_RESULTS);
            let include_hidden = args
                .include_hidden
                .unwrap_or_else(|| clean_pat.starts_with('.') || clean_pat.contains("/."));
            let include_ignored = args.include_ignored.unwrap_or(false)
                || DEFAULT_IGNORED_DIRS.iter().any(|d| clean_pat.contains(d));

            let mut candidate_files = Vec::new();
            let gitignore_patterns = if include_ignored {
                Vec::new()
            } else {
                read_gitignore_patterns(&search_root)
            };
            collect_workspace_files(
                &search_root,
                &search_root,
                include_hidden,
                include_ignored,
                &gitignore_patterns,
                &mut candidate_files,
            );
            candidate_files.sort();

            let mut matched = Vec::new();
            let mut truncated = false;
            let pat_has_slash = clean_pat.contains('/');
            for rel in candidate_files {
                let file_name = Path::new(&rel)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&rel);
                let is_match = glob_matches(clean_pat, &rel)
                    || (!pat_has_slash && glob_matches(clean_pat, file_name));
                if is_match {
                    if matched.len() >= max_results {
                        truncated = true;
                        break;
                    }
                    matched.push(rel);
                }
            }
            let count = matched.len();
            Ok(serde_json::json!({
                "action": "glob",
                "base_dir": search_root.display().to_string(),
                "pattern": pattern,
                "files": matched,
                "matches": matched,
                "count": count,
                "truncated": truncated,
            }))
        }
        "grep" | "search" => {
            let query = args
                .query
                .or(args.pattern)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`files` grep requires a non-empty `query`".to_string())?;
            let target_path = resolve_workspace_path(base_dir, args.path.as_deref(), false)?;
            if !target_path.exists() {
                return Err(format!(
                    "grep target path {} does not exist",
                    target_path.display()
                ));
            }
            let max_results = args
                .max_results
                .unwrap_or(DEFAULT_GREP_MAX_RESULTS)
                .clamp(1, HARD_GREP_MAX_RESULTS);
            let context_lines = args.context_lines.unwrap_or(0).min(10);
            let matcher = GrepMatcher::compile(
                &query,
                args.case_insensitive.unwrap_or(false),
                args.literal.unwrap_or(false),
            );
            let file_glob = args
                .glob
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.strip_prefix("./").unwrap_or(s));

            let files_to_search: Vec<(PathBuf, String)> = if target_path.is_file() {
                let display = target_path
                    .strip_prefix(base_dir)
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|_| target_path.display().to_string());
                vec![(target_path, display)]
            } else {
                let include_hidden = args.include_hidden.unwrap_or(false);
                let include_ignored = args.include_ignored.unwrap_or(false);
                let gitignore_patterns = if include_ignored {
                    Vec::new()
                } else {
                    read_gitignore_patterns(&target_path)
                };
                let mut rels = Vec::new();
                collect_workspace_files(
                    &target_path,
                    &target_path,
                    include_hidden,
                    include_ignored,
                    &gitignore_patterns,
                    &mut rels,
                );
                rels.sort();
                rels.into_iter()
                    .filter(|rel| {
                        if let Some(g) = file_glob {
                            let fname = Path::new(rel)
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or(rel);
                            glob_matches(g, rel) || (!g.contains('/') && glob_matches(g, fname))
                        } else {
                            true
                        }
                    })
                    .map(|rel| {
                        let abs = target_path.join(&rel);
                        let disp = abs
                            .strip_prefix(base_dir)
                            .map(|p| p.display().to_string())
                            .unwrap_or(rel);
                        (abs, disp)
                    })
                    .collect()
            };

            let mut matches = Vec::new();
            let mut truncated = false;
            'file_loop: for (abs_path, disp_path) in files_to_search {
                let Ok(bytes) = std::fs::read(&abs_path) else {
                    continue;
                };
                let probe_len = bytes.len().min(8192);
                if bytes[..probe_len].contains(&0) {
                    continue;
                }
                let text = String::from_utf8_lossy(&bytes);
                let lines: Vec<&str> = text.lines().collect();
                for (idx, line) in lines.iter().enumerate() {
                    if matcher.is_match(line) {
                        if matches.len() >= max_results {
                            truncated = true;
                            break 'file_loop;
                        }
                        let mut hit = serde_json::json!({
                            "path": disp_path,
                            "line": idx + 1,
                            "text": line,
                        });
                        if context_lines > 0
                            && let Some(obj) = hit.as_object_mut()
                        {
                            let before_start = idx.saturating_sub(context_lines);
                            let after_end = (idx + 1 + context_lines).min(lines.len());
                            let before: Vec<&str> = lines[before_start..idx].to_vec();
                            let after: Vec<&str> = lines[idx + 1..after_end].to_vec();
                            obj.insert("context_before".to_string(), serde_json::json!(before));
                            obj.insert("context_after".to_string(), serde_json::json!(after));
                        }
                        matches.push(hit);
                    }
                }
            }
            let count = matches.len();
            Ok(serde_json::json!({
                "action": "grep",
                "query": query,
                "matches": matches,
                "count": count,
                "truncated": truncated,
            }))
        }
        "exec" | "bash" | "sh" | "command" => {
            if std::env::var("ANYAGENTS_DISABLE_WORKSPACE_EXEC")
                .ok()
                .is_some_and(|v| {
                    matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
                })
            {
                return Err(
                    "workspace.exec is disabled for this agent (ANYAGENTS_DISABLE_WORKSPACE_EXEC=1); delegate command execution to a subagent".to_string(),
                );
            }
            let cmd_str = args
                .cmd
                .as_deref()
                .or(args.content.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`workspace.exec` requires a non-empty `cmd`".to_string())?;
            let raw_cwd = args.cwd.as_deref().or(args.path.as_deref());
            let resolved_cwd = resolve_workspace_path(base_dir, raw_cwd, false)?;
            if !resolved_cwd.is_dir() {
                return Err(format!(
                    "workspace.exec cwd {} does not exist or is not a directory",
                    resolved_cwd.display()
                ));
            }
            let timeout_ms = args
                .timeout_ms
                .unwrap_or(DEFAULT_CODE_TIMEOUT_MS)
                .clamp(100, MAX_CODE_TIMEOUT_MS);
            let started = std::time::Instant::now();
            let mut cmd = std::process::Command::new("/bin/bash");
            cmd.arg("-c")
                .arg(cmd_str)
                .current_dir(&resolved_cwd)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt as _;
                cmd.process_group(0);
            }
            let mut child = cmd
                .spawn()
                .map_err(|e| format!("failed to spawn bash for workspace.exec: {e}"))?;
            let stdout_pipe = child
                .stdout
                .take()
                .ok_or_else(|| "workspace.exec stdout missing".to_string())?;
            let stderr_pipe = child
                .stderr
                .take()
                .ok_or_else(|| "workspace.exec stderr missing".to_string())?;
            let stdout_handle = std::thread::spawn(move || {
                use std::io::Read as _;
                let mut buf = Vec::new();
                let mut take = stdout_pipe.take(64 * 1024);
                let _ = take.read_to_end(&mut buf);
                String::from_utf8_lossy(&buf).into_owned()
            });
            let stderr_handle = std::thread::spawn(move || {
                use std::io::Read as _;
                let mut buf = Vec::new();
                let mut take = stderr_pipe.take(64 * 1024);
                let _ = take.read_to_end(&mut buf);
                String::from_utf8_lossy(&buf).into_owned()
            });
            let mut guard = CodeChildGuard { child: Some(child) };
            let deadline = started + std::time::Duration::from_millis(timeout_ms);
            let status_opt = loop {
                if let Some(ref mut c) = guard.child {
                    match c.try_wait() {
                        Ok(Some(st)) => break Some(st),
                        Ok(None) => {
                            if std::time::Instant::now() >= deadline {
                                break None;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        Err(e) => return Err(format!("workspace.exec wait failed: {e}")),
                    }
                } else {
                    break None;
                }
            };
            drop(guard);
            let stdout = stdout_handle.join().unwrap_or_default();
            let stderr = stderr_handle.join().unwrap_or_default();
            let duration_ms = started.elapsed().as_millis() as u64;
            let Some(status) = status_opt else {
                return Err(format!(
                    "workspace.exec timed out after {timeout_ms}ms (stdout: {}, stderr: {})",
                    stdout.trim(),
                    stderr.trim()
                ));
            };
            let exit_code = status.code().unwrap_or(-1);
            Ok(serde_json::json!({
                "action": "exec",
                "cmd": cmd_str,
                "cwd": resolved_cwd.display().to_string(),
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "duration_ms": duration_ms,
            }))
        }
        other => Err(format!(
            "unknown `files` action `{other}`; expected `read`, `write`, `edit`, `multi_edit`, `patch`, `list`, `glob`, `grep`, or `exec`"
        )),
    }
}

fn apply_atomic_edits(
    existing: &str,
    specs: &[FileEditSpec],
    default_replace_all: bool,
    path: &Path,
) -> Result<(String, usize, usize, usize), String> {
    let mut working = existing.to_string();
    let mut total_replacements = 0usize;
    let mut total_lines_added = 0usize;
    let mut total_lines_removed = 0usize;

    for (idx, spec) in specs.iter().enumerate() {
        if spec.target.is_empty() {
            return Err(format!(
                "edit #{} in {}: `target` must not be empty",
                idx + 1,
                path.display()
            ));
        }
        let replace_all = spec.replace_all.unwrap_or(default_replace_all);
        let tgt_lines = spec
            .target
            .lines()
            .count()
            .max(usize::from(!spec.target.is_empty()));
        let rep_lines = spec
            .replacement
            .lines()
            .count()
            .max(usize::from(!spec.replacement.is_empty()));

        match apply_single_edit_cascade(&working, &spec.target, &spec.replacement, replace_all) {
            Ok((next, count)) => {
                working = next;
                total_replacements += count;
                total_lines_removed += count * tgt_lines;
                total_lines_added += count * rep_lines;
            }
            Err(reason) => {
                let hint = find_closest_lines(&working, &spec.target);
                return Err(format!(
                    "edit #{} in {}: {reason}{hint}",
                    idx + 1,
                    path.display()
                ));
            }
        }
    }

    Ok((
        working,
        total_replacements,
        total_lines_added,
        total_lines_removed,
    ))
}

fn apply_single_edit_cascade(
    content: &str,
    target: &str,
    replacement: &str,
    replace_all: bool,
) -> Result<(String, usize), String> {
    // Stage 1: Exact match
    let exact_matches: Vec<usize> = content.match_indices(target).map(|(pos, _)| pos).collect();
    if exact_matches.len() == 1 || (exact_matches.len() > 1 && replace_all) {
        let count = if replace_all { exact_matches.len() } else { 1 };
        let updated = if replace_all {
            content.replace(target, replacement)
        } else {
            content.replacen(target, replacement, 1)
        };
        return Ok((updated, count));
    } else if exact_matches.len() > 1 {
        let lines: Vec<String> = exact_matches
            .iter()
            .take(8)
            .map(|&pos| (content[..pos].bytes().filter(|&b| b == b'\n').count() + 1).to_string())
            .collect();
        return Err(format!(
            "`target` matched {} times (at lines {}); include more context or set `replace_all: true`",
            exact_matches.len(),
            lines.join(", ")
        ));
    }

    // Stage 2: CRLF/LF normalized match
    let uses_crlf = content.contains("\r\n");
    let norm_content = content.replace("\r\n", "\n");
    let norm_target = target.replace("\r\n", "\n");
    let norm_replacement = replacement.replace("\r\n", "\n");
    if norm_content != content || norm_target != target {
        let crlf_matches: Vec<usize> = norm_content
            .match_indices(&norm_target)
            .map(|(pos, _)| pos)
            .collect();
        if crlf_matches.len() == 1 || (crlf_matches.len() > 1 && replace_all) {
            let count = if replace_all { crlf_matches.len() } else { 1 };
            let updated_lf = if replace_all {
                norm_content.replace(&norm_target, &norm_replacement)
            } else {
                norm_content.replacen(&norm_target, &norm_replacement, 1)
            };
            let final_str = if uses_crlf {
                updated_lf.replace('\n', "\r\n")
            } else {
                updated_lf
            };
            return Ok((final_str, count));
        } else if crlf_matches.len() > 1 {
            let lines: Vec<String> = crlf_matches
                .iter()
                .take(8)
                .map(|&pos| {
                    (norm_content[..pos].bytes().filter(|&b| b == b'\n').count() + 1).to_string()
                })
                .collect();
            return Err(format!(
                "`target` matched {} times (at lines {}); include more context or set `replace_all: true`",
                crlf_matches.len(),
                lines.join(", ")
            ));
        }
    }

    let doc_lines: Vec<&str> = norm_content.split('\n').collect();
    let target_lines: Vec<&str> = norm_target.split('\n').collect();
    if target_lines.is_empty() || target_lines.len() > doc_lines.len() {
        return Err("`target` not found.".to_string());
    }

    // Stage 3: Line-end whitespace-trimmed (`trim_end()`) match
    let mut trim_end_hits = Vec::new();
    for i in 0..=(doc_lines.len() - target_lines.len()) {
        if (0..target_lines.len())
            .all(|j| doc_lines[i + j].trim_end() == target_lines[j].trim_end())
        {
            trim_end_hits.push(i);
        }
    }
    if trim_end_hits.len() == 1 {
        let i = trim_end_hits[0];
        let repl_lines: Vec<&str> = norm_replacement.split('\n').collect();
        let mut new_doc: Vec<String> = Vec::with_capacity(doc_lines.len() + repl_lines.len());
        new_doc.extend(doc_lines[..i].iter().map(|s| (*s).to_string()));
        new_doc.extend(repl_lines.iter().map(|s| (*s).to_string()));
        new_doc.extend(
            doc_lines[i + target_lines.len()..]
                .iter()
                .map(|s| (*s).to_string()),
        );
        let joined = if uses_crlf {
            new_doc.join("\r\n")
        } else {
            new_doc.join("\n")
        };
        return Ok((joined, 1));
    } else if trim_end_hits.len() > 1 {
        let lines: Vec<String> = trim_end_hits
            .iter()
            .take(8)
            .map(|&i| (i + 1).to_string())
            .collect();
        return Err(format!(
            "`target` matched {} times (at lines {}); include more surrounding context lines",
            trim_end_hits.len(),
            lines.join(", ")
        ));
    }

    // Stage 4: Indentation-tolerant (`trim()`) match preserving relative indentation
    let has_substantive_line = target_lines.iter().any(|l| l.trim().len() >= 3);
    if has_substantive_line {
        let mut trim_hits = Vec::new();
        for i in 0..=(doc_lines.len() - target_lines.len()) {
            if (0..target_lines.len()).all(|j| doc_lines[i + j].trim() == target_lines[j].trim()) {
                trim_hits.push(i);
            }
        }
        if trim_hits.len() == 1 {
            let i = trim_hits[0];
            let mut indent_delta: isize = 0;
            for j in 0..target_lines.len() {
                if !target_lines[j].trim().is_empty() && !doc_lines[i + j].trim().is_empty() {
                    let doc_indent = doc_lines[i + j].len() - doc_lines[i + j].trim_start().len();
                    let tgt_indent = target_lines[j].len() - target_lines[j].trim_start().len();
                    indent_delta = doc_indent as isize - tgt_indent as isize;
                    break;
                }
            }
            let repl_lines: Vec<String> = norm_replacement
                .split('\n')
                .map(|line| {
                    if line.trim().is_empty() || indent_delta == 0 {
                        line.to_string()
                    } else if indent_delta > 0 {
                        format!("{}{}", " ".repeat(indent_delta as usize), line)
                    } else {
                        let remove = (-indent_delta) as usize;
                        let leading = line.len() - line.trim_start_matches(' ').len();
                        line[remove.min(leading)..].to_string()
                    }
                })
                .collect();
            let mut new_doc: Vec<String> = Vec::with_capacity(doc_lines.len() + repl_lines.len());
            new_doc.extend(doc_lines[..i].iter().map(|s| (*s).to_string()));
            new_doc.extend(repl_lines);
            new_doc.extend(
                doc_lines[i + target_lines.len()..]
                    .iter()
                    .map(|s| (*s).to_string()),
            );
            let joined = if uses_crlf {
                new_doc.join("\r\n")
            } else {
                new_doc.join("\n")
            };
            return Ok((joined, 1));
        } else if trim_hits.len() > 1 {
            let lines: Vec<String> = trim_hits
                .iter()
                .take(8)
                .map(|&i| (i + 1).to_string())
                .collect();
            return Err(format!(
                "`target` matched {} times by indentation-tolerant comparison (at lines {}); include more context lines",
                trim_hits.len(),
                lines.join(", ")
            ));
        }
    }

    Err("`target` not found.".to_string())
}

fn is_v4a_patch(text: &str) -> bool {
    text.lines().any(|l| {
        let t = l.trim();
        t == "*** Begin Patch"
            || t.starts_with("*** Update File:")
            || t.starts_with("*** Add File:")
            || t.starts_with("*** Delete File:")
    })
}

fn resolve_patch_rel_path(base_dir: &Path, raw: &str) -> Result<PathBuf, String> {
    let clean = raw
        .trim()
        .strip_prefix("a/")
        .or_else(|| raw.trim().strip_prefix("b/"))
        .unwrap_or(raw.trim());
    resolve_workspace_path(base_dir, Some(clean), true)
}

fn apply_workspace_patch(base_dir: &Path, patch_text: &str) -> Result<serde_json::Value, String> {
    let normalized = patch_text.replace("\r\n", "\n");
    let lines: Vec<&str> = normalized.lines().collect();
    let mut operations = Vec::new();
    let mut files = Vec::new();

    if is_v4a_patch(&normalized) {
        let mut i = 0usize;
        while i < lines.len() {
            let line = lines[i].trim();
            if line.is_empty()
                || line == "*** Begin Patch"
                || line == "*** End Patch"
                || line.starts_with("```")
            {
                i += 1;
                continue;
            }
            if let Some(rel) = line.strip_prefix("*** Add File:").map(str::trim) {
                i += 1;
                let mut body_lines = Vec::new();
                while i < lines.len() && !lines[i].trim().starts_with("*** ") {
                    let raw_l = lines[i];
                    if let Some(stripped) = raw_l.strip_prefix('+') {
                        body_lines.push(stripped);
                    } else {
                        body_lines.push(raw_l);
                    }
                    i += 1;
                }
                let target = resolve_patch_rel_path(base_dir, rel)?;
                if let Some(p) = target.parent()
                    && !p.as_os_str().is_empty()
                {
                    std::fs::create_dir_all(p).map_err(|e| {
                        format!("failed creating parent directory {}: {e}", p.display())
                    })?;
                }
                let mut content = body_lines.join("\n");
                if !content.is_empty() && !content.ends_with('\n') {
                    content.push('\n');
                }
                std::fs::write(&target, content.as_bytes())
                    .map_err(|e| format!("failed writing `{rel}`: {e}"))?;
                let sha256 = sha256_hex(content.as_bytes());
                files.push(rel.to_string());
                operations.push(serde_json::json!({
                    "op": "add",
                    "path": rel,
                    "resolved_path": target.display().to_string(),
                    "sha256": sha256,
                }));
                continue;
            }
            if let Some(rel) = line.strip_prefix("*** Delete File:").map(str::trim) {
                i += 1;
                while i < lines.len() && !lines[i].trim().starts_with("*** ") {
                    i += 1;
                }
                let target = resolve_patch_rel_path(base_dir, rel)?;
                if target.exists() {
                    std::fs::remove_file(&target)
                        .map_err(|e| format!("failed deleting `{rel}`: {e}"))?;
                }
                files.push(rel.to_string());
                operations.push(serde_json::json!({
                    "op": "delete",
                    "path": rel,
                    "resolved_path": target.display().to_string(),
                }));
                continue;
            }
            if let Some(rel) = line.strip_prefix("*** Update File:").map(str::trim) {
                i += 1;
                let mut move_to: Option<String> = None;
                if i < lines.len()
                    && let Some(mv) = lines[i].trim().strip_prefix("*** Move to:").map(str::trim)
                {
                    move_to = Some(mv.to_string());
                    i += 1;
                }
                let mut hunk_lines = Vec::new();
                while i < lines.len() && !lines[i].trim().starts_with("*** ") {
                    if !lines[i].trim().starts_with("```") {
                        hunk_lines.push(lines[i]);
                    }
                    i += 1;
                }
                let target = resolve_patch_rel_path(base_dir, rel)?;
                let existing = std::fs::read_to_string(&target)
                    .map_err(|e| format!("failed reading `{rel}`: {e}"))?;
                let updated = apply_patch_context_hunks(&existing, &hunk_lines)
                    .map_err(|e| format!("failed patching `{rel}`: {e}"))?;
                let sha256 = sha256_hex(updated.as_bytes());
                if let Some(dest_rel) = move_to {
                    let dest = resolve_patch_rel_path(base_dir, &dest_rel)?;
                    if let Some(p) = dest.parent()
                        && !p.as_os_str().is_empty()
                    {
                        std::fs::create_dir_all(p).map_err(|e| {
                            format!("failed creating parent directory {}: {e}", p.display())
                        })?;
                    }
                    std::fs::write(&dest, updated.as_bytes())
                        .map_err(|e| format!("failed writing `{dest_rel}`: {e}"))?;
                    if dest != target && target.exists() {
                        let _ = std::fs::remove_file(&target);
                    }
                    files.push(dest_rel.clone());
                    operations.push(serde_json::json!({
                        "op": "update",
                        "path": rel,
                        "moved_to": dest_rel,
                        "resolved_path": dest.display().to_string(),
                        "sha256": sha256,
                    }));
                } else {
                    std::fs::write(&target, updated.as_bytes())
                        .map_err(|e| format!("failed writing `{rel}`: {e}"))?;
                    files.push(rel.to_string());
                    operations.push(serde_json::json!({
                        "op": "update",
                        "path": rel,
                        "resolved_path": target.display().to_string(),
                        "sha256": sha256,
                    }));
                }
                continue;
            }
            i += 1;
        }
    } else {
        let mut i = 0usize;
        while i < lines.len() {
            let line = lines[i];
            if line.starts_with("--- ") && i + 1 < lines.len() && lines[i + 1].starts_with("+++ ") {
                let old_header = line
                    .strip_prefix("--- ")
                    .unwrap_or("")
                    .split('\t')
                    .next()
                    .unwrap_or("")
                    .trim();
                let new_header = lines[i + 1]
                    .strip_prefix("+++ ")
                    .unwrap_or("")
                    .split('\t')
                    .next()
                    .unwrap_or("")
                    .trim();
                i += 2;
                let mut hunk_lines = Vec::new();
                while i < lines.len()
                    && !lines[i].starts_with("diff --git ")
                    && !(lines[i].starts_with("--- ")
                        && i + 1 < lines.len()
                        && lines[i + 1].starts_with("+++ "))
                {
                    hunk_lines.push(lines[i]);
                    i += 1;
                }
                if new_header == "/dev/null" {
                    let target = resolve_patch_rel_path(base_dir, old_header)?;
                    if target.exists() {
                        std::fs::remove_file(&target)
                            .map_err(|e| format!("failed deleting `{old_header}`: {e}"))?;
                    }
                    let clean_rel = old_header.strip_prefix("a/").unwrap_or(old_header);
                    files.push(clean_rel.to_string());
                    operations.push(serde_json::json!({
                        "op": "delete",
                        "path": clean_rel,
                        "resolved_path": target.display().to_string(),
                    }));
                } else if old_header == "/dev/null" {
                    let target = resolve_patch_rel_path(base_dir, new_header)?;
                    if let Some(p) = target.parent()
                        && !p.as_os_str().is_empty()
                    {
                        std::fs::create_dir_all(p).map_err(|e| {
                            format!("failed creating parent directory {}: {e}", p.display())
                        })?;
                    }
                    let mut added = Vec::new();
                    for hl in &hunk_lines {
                        if hl.starts_with("@@") || hl.starts_with("\\ No newline") {
                            continue;
                        }
                        if let Some(rest) = hl.strip_prefix('+') {
                            added.push(rest);
                        } else if let Some(rest) = hl.strip_prefix(' ') {
                            added.push(rest);
                        }
                    }
                    let mut content = added.join("\n");
                    if !content.is_empty() {
                        content.push('\n');
                    }
                    std::fs::write(&target, content.as_bytes())
                        .map_err(|e| format!("failed writing `{new_header}`: {e}"))?;
                    let sha256 = sha256_hex(content.as_bytes());
                    let clean_rel = new_header.strip_prefix("b/").unwrap_or(new_header);
                    files.push(clean_rel.to_string());
                    operations.push(serde_json::json!({
                        "op": "add",
                        "path": clean_rel,
                        "resolved_path": target.display().to_string(),
                        "sha256": sha256,
                    }));
                } else {
                    let rel = if !new_header.is_empty() {
                        new_header
                    } else {
                        old_header
                    };
                    let target = resolve_patch_rel_path(base_dir, rel)?;
                    let existing = std::fs::read_to_string(&target)
                        .map_err(|e| format!("failed reading `{rel}`: {e}"))?;
                    let updated = apply_patch_context_hunks(&existing, &hunk_lines)
                        .map_err(|e| format!("failed applying hunk to `{rel}`: {e}"))?;
                    std::fs::write(&target, updated.as_bytes())
                        .map_err(|e| format!("failed writing `{rel}`: {e}"))?;
                    let sha256 = sha256_hex(updated.as_bytes());
                    let clean_rel = rel
                        .strip_prefix("b/")
                        .or_else(|| rel.strip_prefix("a/"))
                        .unwrap_or(rel);
                    files.push(clean_rel.to_string());
                    operations.push(serde_json::json!({
                        "op": "update",
                        "path": clean_rel,
                        "resolved_path": target.display().to_string(),
                        "sha256": sha256,
                    }));
                }
                continue;
            }
            i += 1;
        }
    }

    if operations.is_empty() {
        return Err(
            "patch failed: no valid V4A (`*** Begin Patch` / `*** Update File:`) or unified diff (`--- a/...` / `+++ b/...`) hunks found"
                .to_string(),
        );
    }
    let count = operations.len();
    Ok(serde_json::json!({
        "action": "patch",
        "files": files,
        "operations": operations,
        "count": count,
    }))
}

fn apply_patch_context_hunks(existing: &str, hunk_lines: &[&str]) -> Result<String, String> {
    let had_trailing_newline = existing.ends_with('\n');
    let mut doc_lines: Vec<String> = existing
        .replace("\r\n", "\n")
        .lines()
        .map(String::from)
        .collect();

    let mut hunks: Vec<Vec<&str>> = Vec::new();
    let mut current_hunk: Vec<&str> = Vec::new();
    for &line in hunk_lines {
        if line.starts_with("@@") {
            if !current_hunk.is_empty() {
                hunks.push(std::mem::take(&mut current_hunk));
            }
            continue;
        }
        if line.starts_with("\\ No newline") {
            continue;
        }
        current_hunk.push(line);
    }
    if !current_hunk.is_empty() {
        hunks.push(current_hunk);
    }

    let mut search_cursor = 0usize;
    for (hunk_idx, hunk) in hunks.iter().enumerate() {
        let mut old_block: Vec<&str> = Vec::new();
        let mut new_block: Vec<String> = Vec::new();
        for &hl in hunk {
            if let Some(rem) = hl.strip_prefix('-') {
                old_block.push(rem);
            } else if let Some(add) = hl.strip_prefix('+') {
                new_block.push(add.to_string());
            } else if let Some(ctx) = hl.strip_prefix(' ') {
                old_block.push(ctx);
                new_block.push(ctx.to_string());
            } else {
                old_block.push(hl);
                new_block.push(hl.to_string());
            }
        }

        if old_block.is_empty() {
            let pos = search_cursor.min(doc_lines.len());
            doc_lines.splice(pos..pos, new_block);
            continue;
        }

        if let Some(pos) = find_patch_subslice_lines(&doc_lines, &old_block, search_cursor) {
            let new_len = new_block.len();
            doc_lines.splice(pos..pos + old_block.len(), new_block);
            search_cursor = pos + new_len;
        } else {
            return Err(format!(
                "hunk #{} context did not match target file around line `{}`",
                hunk_idx + 1,
                old_block.first().unwrap_or(&"")
            ));
        }
    }

    let mut out = doc_lines.join("\n");
    if had_trailing_newline && !out.ends_with('\n') {
        out.push('\n');
    }
    Ok(out)
}

fn find_patch_subslice_lines(doc: &[String], needle: &[&str], cursor: usize) -> Option<usize> {
    if needle.len() > doc.len() {
        return None;
    }
    let max_start = doc.len() - needle.len();
    let start_pos = cursor.min(max_start);

    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j] == needle[j]) {
            return Some(idx);
        }
    }
    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j].trim_end() == needle[j].trim_end()) {
            return Some(idx);
        }
    }
    for idx in (start_pos..=max_start).chain(0..start_pos) {
        if (0..needle.len()).all(|j| doc[idx + j].trim() == needle[j].trim()) {
            return Some(idx);
        }
    }
    None
}

fn find_closest_lines(content: &str, target: &str) -> String {
    let first_non_empty = target
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    if first_non_empty.is_empty() {
        return String::new();
    }
    let needle_lower = first_non_empty.to_ascii_lowercase();
    let tokens: Vec<&str> = needle_lower
        .split_whitespace()
        .filter(|t| t.len() >= 3)
        .take(4)
        .collect();

    let mut candidates = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let lower = line.to_ascii_lowercase();
        let hit = lower.contains(&needle_lower)
            || (!tokens.is_empty() && tokens.iter().all(|t| lower.contains(t)));
        if hit {
            candidates.push(format!("  {}: {}", idx + 1, line));
            if candidates.len() >= 5 {
                break;
            }
        }
    }
    if candidates.is_empty() {
        let preview: Vec<String> = content
            .lines()
            .take(5)
            .enumerate()
            .map(|(i, l)| format!("  {}: {l}", i + 1))
            .collect();
        if preview.is_empty() {
            return " (file is empty)".to_string();
        }
        return format!("\nFirst lines of file:\n{}", preview.join("\n"));
    }
    format!("\nClosest lines in file:\n{}", candidates.join("\n"))
}

fn read_gitignore_patterns(root: &Path) -> Vec<String> {
    let path = root.join(".gitignore");
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with('!'))
        .map(|l| l.trim_matches('/').to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

fn is_gitignored(rel_path: &str, file_name: &str, patterns: &[String]) -> bool {
    for pat in patterns {
        if pat == file_name || pat == rel_path {
            return true;
        }
        if glob_matches(pat, rel_path) || (!pat.contains('/') && glob_matches(pat, file_name)) {
            return true;
        }
    }
    false
}

fn collect_workspace_files(
    root: &Path,
    current: &Path,
    include_hidden: bool,
    include_ignored: bool,
    gitignore_patterns: &[String],
    out: &mut Vec<String>,
) {
    let Ok(rd) = std::fs::read_dir(current) else {
        return;
    };
    let mut entries: Vec<std::fs::DirEntry> = rd.flatten().collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let name_os = entry.file_name();
        let Some(name) = name_os.to_str() else {
            continue;
        };
        if !include_hidden && name.starts_with('.') {
            continue;
        }
        if !include_ignored && DEFAULT_IGNORED_DIRS.contains(&name) {
            continue;
        }
        let path = entry.path();
        let Ok(rel) = path.strip_prefix(root) else {
            continue;
        };
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        if !include_ignored && is_gitignored(&rel_str, name, gitignore_patterns) {
            continue;
        }
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if ft.is_dir() {
            collect_workspace_files(
                root,
                &path,
                include_hidden,
                include_ignored,
                gitignore_patterns,
                out,
            );
        } else if ft.is_file() {
            out.push(rel_str);
        }
    }
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    if let Some(open) = pattern.find('{')
        && let Some(close_rel) = pattern[open + 1..].find('}')
    {
        let close = open + 1 + close_rel;
        let prefix = &pattern[..open];
        let suffix = &pattern[close + 1..];
        for alt in pattern[open + 1..close].split(',') {
            let expanded = format!("{prefix}{alt}{suffix}");
            if glob_matches(&expanded, text) {
                return true;
            }
        }
        return false;
    }
    let pat_chars: Vec<char> = pattern.chars().collect();
    let txt_chars: Vec<char> = text.chars().collect();
    glob_match_chars(&pat_chars, 0, &txt_chars, 0)
}

fn glob_match_chars(pat: &[char], mut pi: usize, txt: &[char], mut ti: usize) -> bool {
    while pi < pat.len() {
        if pi + 1 < pat.len() && pat[pi] == '*' && pat[pi + 1] == '*' {
            pi += 2;
            if pi < pat.len() && pat[pi] == '/' {
                pi += 1;
                if glob_match_chars(pat, pi, txt, ti) {
                    return true;
                }
                for idx in ti..txt.len() {
                    if txt[idx] == '/' && glob_match_chars(pat, pi, txt, idx + 1) {
                        return true;
                    }
                }
                return false;
            }
            if pi == pat.len() {
                return true;
            }
            for idx in ti..=txt.len() {
                if glob_match_chars(pat, pi, txt, idx) {
                    return true;
                }
            }
            return false;
        }
        match pat[pi] {
            '*' => {
                pi += 1;
                for idx in ti..=txt.len() {
                    if idx > ti && txt[idx - 1] == '/' {
                        break;
                    }
                    if glob_match_chars(pat, pi, txt, idx) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                if ti >= txt.len() || txt[ti] == '/' {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
            '[' => {
                if ti >= txt.len() || txt[ti] == '/' {
                    return false;
                }
                if let Some((matched, next_pi)) = match_char_class(pat, pi + 1, txt[ti], false) {
                    if !matched {
                        return false;
                    }
                    pi = next_pi;
                    ti += 1;
                } else {
                    if pat[pi] != txt[ti] {
                        return false;
                    }
                    pi += 1;
                    ti += 1;
                }
            }
            c => {
                if ti >= txt.len() || txt[ti] != c {
                    return false;
                }
                pi += 1;
                ti += 1;
            }
        }
    }
    ti == txt.len()
}

fn match_char_class(
    pat: &[char],
    mut idx: usize,
    ch: char,
    case_insensitive: bool,
) -> Option<(bool, usize)> {
    if idx >= pat.len() {
        return None;
    }
    let negated = if pat[idx] == '!' || pat[idx] == '^' {
        idx += 1;
        true
    } else {
        false
    };
    let target = if case_insensitive {
        ch.to_ascii_lowercase()
    } else {
        ch
    };
    let mut found = false;
    let mut closed = false;
    while idx < pat.len() {
        if pat[idx] == ']' {
            closed = true;
            idx += 1;
            break;
        }
        let mut start = pat[idx];
        if start == '\\' && idx + 1 < pat.len() {
            idx += 1;
            start = pat[idx];
        }
        if idx + 2 < pat.len() && pat[idx + 1] == '-' && pat[idx + 2] != ']' {
            let end = pat[idx + 2];
            let (s, e) = if case_insensitive {
                (start.to_ascii_lowercase(), end.to_ascii_lowercase())
            } else {
                (start, end)
            };
            if target >= s && target <= e {
                found = true;
            }
            idx += 3;
        } else {
            let s = if case_insensitive {
                start.to_ascii_lowercase()
            } else {
                start
            };
            if target == s {
                found = true;
            }
            idx += 1;
        }
    }
    if !closed {
        return None;
    }
    Some((if negated { !found } else { found }, idx))
}

enum GrepMatcher {
    Literal {
        needle: String,
        case_insensitive: bool,
    },
    Regex {
        node: RegexNode,
        case_insensitive: bool,
    },
}

#[derive(Clone)]
enum RegexNode {
    Alt(Vec<RegexSeq>),
}

#[derive(Clone)]
struct RegexSeq {
    pieces: Vec<RegexPiece>,
}

#[derive(Clone)]
struct RegexPiece {
    atom: RegexAtom,
    min: usize,
    max: Option<usize>,
}

#[derive(Clone)]
enum RegexAtom {
    AnchorStart,
    AnchorEnd,
    WordBoundary,
    AnyChar,
    Literal(char),
    Digit(bool),
    Word(bool),
    Space(bool),
    Class {
        negated: bool,
        ranges: Vec<(char, char)>,
    },
    Group(Box<RegexNode>),
}

impl GrepMatcher {
    fn compile(query: &str, mut case_insensitive: bool, force_literal: bool) -> Self {
        let mut pat = query;
        if let Some(stripped) = pat.strip_prefix("(?i)") {
            case_insensitive = true;
            pat = stripped;
        }
        if force_literal {
            return Self::Literal {
                needle: if case_insensitive {
                    pat.to_ascii_lowercase()
                } else {
                    pat.to_string()
                },
                case_insensitive,
            };
        }
        let chars: Vec<char> = pat.chars().collect();
        let mut pos = 0;
        if let Some(node) = parse_regex_alt(&chars, &mut pos, false, case_insensitive)
            && pos == chars.len()
        {
            return Self::Regex {
                node,
                case_insensitive,
            };
        }
        Self::Literal {
            needle: if case_insensitive {
                pat.to_ascii_lowercase()
            } else {
                pat.to_string()
            },
            case_insensitive,
        }
    }

    fn is_match(&self, line: &str) -> bool {
        match self {
            Self::Literal {
                needle,
                case_insensitive,
            } => {
                if *case_insensitive {
                    line.to_ascii_lowercase().contains(needle)
                } else {
                    line.contains(needle)
                }
            }
            Self::Regex {
                node,
                case_insensitive,
            } => {
                let chars: Vec<char> = line.chars().collect();
                let mut steps = 0usize;
                for start in 0..=chars.len() {
                    if eval_regex_node(node, &chars, start, *case_insensitive, &mut steps).is_some()
                    {
                        return true;
                    }
                    if steps > 50_000 {
                        break;
                    }
                }
                false
            }
        }
    }
}

fn parse_regex_alt(
    chars: &[char],
    pos: &mut usize,
    in_group: bool,
    case_insensitive: bool,
) -> Option<RegexNode> {
    let mut branches = Vec::new();
    loop {
        branches.push(parse_regex_seq(chars, pos, in_group, case_insensitive)?);
        if *pos < chars.len() && chars[*pos] == '|' {
            *pos += 1;
        } else {
            break;
        }
    }
    Some(RegexNode::Alt(branches))
}

fn parse_regex_seq(
    chars: &[char],
    pos: &mut usize,
    in_group: bool,
    case_insensitive: bool,
) -> Option<RegexSeq> {
    let mut pieces = Vec::new();
    while *pos < chars.len() {
        let ch = chars[*pos];
        if ch == '|' || (in_group && ch == ')') {
            break;
        }
        if ch == ')' && !in_group {
            return None;
        }
        let atom = match ch {
            '^' => {
                *pos += 1;
                RegexAtom::AnchorStart
            }
            '$' => {
                *pos += 1;
                RegexAtom::AnchorEnd
            }
            '.' => {
                *pos += 1;
                RegexAtom::AnyChar
            }
            '(' => {
                *pos += 1;
                if *pos + 1 < chars.len() && chars[*pos] == '?' && chars[*pos + 1] == ':' {
                    *pos += 2;
                }
                let inner = parse_regex_alt(chars, pos, true, case_insensitive)?;
                if *pos >= chars.len() || chars[*pos] != ')' {
                    return None;
                }
                *pos += 1;
                RegexAtom::Group(Box::new(inner))
            }
            '[' => {
                *pos += 1;
                parse_regex_class(chars, pos, case_insensitive)?
            }
            '\\' => {
                *pos += 1;
                if *pos >= chars.len() {
                    return None;
                }
                let esc = chars[*pos];
                *pos += 1;
                match esc {
                    'd' => RegexAtom::Digit(true),
                    'D' => RegexAtom::Digit(false),
                    'w' => RegexAtom::Word(true),
                    'W' => RegexAtom::Word(false),
                    's' => RegexAtom::Space(true),
                    'S' => RegexAtom::Space(false),
                    'b' => RegexAtom::WordBoundary,
                    'n' => RegexAtom::Literal('\n'),
                    'r' => RegexAtom::Literal('\r'),
                    't' => RegexAtom::Literal('\t'),
                    other => RegexAtom::Literal(if case_insensitive {
                        other.to_ascii_lowercase()
                    } else {
                        other
                    }),
                }
            }
            '*' | '+' | '?' => return None,
            other => {
                *pos += 1;
                RegexAtom::Literal(if case_insensitive {
                    other.to_ascii_lowercase()
                } else {
                    other
                })
            }
        };
        let (min, max) = if *pos < chars.len() {
            match chars[*pos] {
                '*' => {
                    *pos += 1;
                    if *pos < chars.len() && chars[*pos] == '?' {
                        *pos += 1;
                    }
                    (0, None)
                }
                '+' => {
                    *pos += 1;
                    if *pos < chars.len() && chars[*pos] == '?' {
                        *pos += 1;
                    }
                    (1, None)
                }
                '?' => {
                    *pos += 1;
                    if *pos < chars.len() && chars[*pos] == '?' {
                        *pos += 1;
                    }
                    (0, Some(1))
                }
                '{' => {
                    if let Some((m, mx, next_p)) = parse_bounded_rep(chars, *pos + 1) {
                        *pos = next_p;
                        (m, mx)
                    } else {
                        (1, Some(1))
                    }
                }
                _ => (1, Some(1)),
            }
        } else {
            (1, Some(1))
        };
        pieces.push(RegexPiece { atom, min, max });
    }
    Some(RegexSeq { pieces })
}

fn parse_bounded_rep(chars: &[char], mut idx: usize) -> Option<(usize, Option<usize>, usize)> {
    let start = idx;
    while idx < chars.len() && chars[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == start || idx >= chars.len() {
        return None;
    }
    let min: usize = chars[start..idx].iter().collect::<String>().parse().ok()?;
    if chars[idx] == '}' {
        return Some((min, Some(min), idx + 1));
    }
    if chars[idx] != ',' {
        return None;
    }
    idx += 1;
    let max_start = idx;
    while idx < chars.len() && chars[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx >= chars.len() || chars[idx] != '}' {
        return None;
    }
    let max = if idx == max_start {
        None
    } else {
        Some(
            chars[max_start..idx]
                .iter()
                .collect::<String>()
                .parse::<usize>()
                .ok()?,
        )
    };
    Some((min, max, idx + 1))
}

fn parse_regex_class(chars: &[char], pos: &mut usize, case_insensitive: bool) -> Option<RegexAtom> {
    if *pos >= chars.len() {
        return None;
    }
    let negated = if chars[*pos] == '^' {
        *pos += 1;
        true
    } else {
        false
    };
    let mut ranges = Vec::new();
    let mut closed = false;
    while *pos < chars.len() {
        if chars[*pos] == ']' && !ranges.is_empty() {
            closed = true;
            *pos += 1;
            break;
        }
        let mut c = chars[*pos];
        *pos += 1;
        if c == '\\' {
            if *pos >= chars.len() {
                return None;
            }
            let esc = chars[*pos];
            *pos += 1;
            match esc {
                'd' => {
                    ranges.push(('0', '9'));
                    continue;
                }
                's' => {
                    for sp in [' ', '\t', '\n', '\r'] {
                        ranges.push((sp, sp));
                    }
                    continue;
                }
                'w' => {
                    ranges.push(('a', 'z'));
                    ranges.push(('A', 'Z'));
                    ranges.push(('0', '9'));
                    ranges.push(('_', '_'));
                    continue;
                }
                other => c = other,
            }
        }
        if *pos + 1 < chars.len() && chars[*pos] == '-' && chars[*pos + 1] != ']' {
            *pos += 1;
            let mut end = chars[*pos];
            *pos += 1;
            if end == '\\' && *pos < chars.len() {
                end = chars[*pos];
                *pos += 1;
            }
            let (s, e) = if case_insensitive {
                (c.to_ascii_lowercase(), end.to_ascii_lowercase())
            } else {
                (c, end)
            };
            ranges.push((s, e));
        } else {
            let s = if case_insensitive {
                c.to_ascii_lowercase()
            } else {
                c
            };
            ranges.push((s, s));
        }
    }
    if !closed {
        return None;
    }
    Some(RegexAtom::Class { negated, ranges })
}

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn eval_regex_node(
    node: &RegexNode,
    chars: &[char],
    idx: usize,
    case_insensitive: bool,
    steps: &mut usize,
) -> Option<usize> {
    let RegexNode::Alt(branches) = node;
    for seq in branches {
        if let Some(end) = eval_regex_seq(&seq.pieces, chars, idx, case_insensitive, steps) {
            return Some(end);
        }
    }
    None
}

fn eval_regex_seq(
    pieces: &[RegexPiece],
    chars: &[char],
    idx: usize,
    case_insensitive: bool,
    steps: &mut usize,
) -> Option<usize> {
    *steps += 1;
    if *steps > 50_000 {
        return None;
    }
    let Some((first, rest)) = pieces.split_first() else {
        return Some(idx);
    };
    let max_rep = first.max.unwrap_or(chars.len().saturating_sub(idx) + 1);
    let mut positions = vec![idx];
    let mut cur = idx;
    for _ in 0..max_rep {
        if let Some(next_idx) = eval_regex_atom(&first.atom, chars, cur, case_insensitive, steps) {
            if next_idx == cur && positions.len() > first.min {
                break;
            }
            positions.push(next_idx);
            cur = next_idx;
        } else {
            break;
        }
    }
    if positions.len() <= first.min {
        return None;
    }
    for &pos in positions[first.min..].iter().rev() {
        if let Some(end) = eval_regex_seq(rest, chars, pos, case_insensitive, steps) {
            return Some(end);
        }
    }
    None
}

fn eval_regex_atom(
    atom: &RegexAtom,
    chars: &[char],
    idx: usize,
    case_insensitive: bool,
    steps: &mut usize,
) -> Option<usize> {
    match atom {
        RegexAtom::AnchorStart => (idx == 0).then_some(idx),
        RegexAtom::AnchorEnd => (idx == chars.len()).then_some(idx),
        RegexAtom::WordBoundary => {
            let prev_w = idx > 0 && is_word_char(chars[idx - 1]);
            let next_w = idx < chars.len() && is_word_char(chars[idx]);
            (prev_w != next_w).then_some(idx)
        }
        RegexAtom::AnyChar => (idx < chars.len() && chars[idx] != '\n').then_some(idx + 1),
        RegexAtom::Literal(expected) => {
            if idx >= chars.len() {
                return None;
            }
            let actual = if case_insensitive {
                chars[idx].to_ascii_lowercase()
            } else {
                chars[idx]
            };
            (actual == *expected).then_some(idx + 1)
        }
        RegexAtom::Digit(want) => {
            if idx >= chars.len() {
                return None;
            }
            (chars[idx].is_ascii_digit() == *want).then_some(idx + 1)
        }
        RegexAtom::Word(want) => {
            if idx >= chars.len() {
                return None;
            }
            (is_word_char(chars[idx]) == *want).then_some(idx + 1)
        }
        RegexAtom::Space(want) => {
            if idx >= chars.len() {
                return None;
            }
            (chars[idx].is_ascii_whitespace() == *want).then_some(idx + 1)
        }
        RegexAtom::Class { negated, ranges } => {
            if idx >= chars.len() {
                return None;
            }
            let c = if case_insensitive {
                chars[idx].to_ascii_lowercase()
            } else {
                chars[idx]
            };
            let hit = ranges.iter().any(|&(s, e)| c >= s && c <= e);
            let ok = if *negated { !hit } else { hit };
            ok.then_some(idx + 1)
        }
        RegexAtom::Group(inner) => eval_regex_node(inner, chars, idx, case_insensitive, steps),
    }
}

fn agent_matches_query(agent: &AgentView, query: &str) -> bool {
    let tokens: Vec<String> = query
        .split_whitespace()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    if tokens.is_empty() {
        return true;
    }
    let name = agent.name.to_ascii_lowercase();
    let id = agent.id.to_ascii_lowercase();
    let role = agent.role.as_deref().unwrap_or("").to_ascii_lowercase();
    let desc = agent
        .description
        .as_deref()
        .unwrap_or("")
        .to_ascii_lowercase();
    let workdir = agent.workdir.as_deref().unwrap_or("").to_ascii_lowercase();
    let harness = agent.harness.to_ascii_lowercase();
    let model = agent.model.as_deref().unwrap_or("").to_ascii_lowercase();
    let aliases: Vec<String> = agent
        .aliases
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();

    tokens.iter().all(|tok| {
        name.contains(tok)
            || id.contains(tok)
            || role.contains(tok)
            || desc.contains(tok)
            || workdir.contains(tok)
            || harness.contains(tok)
            || model.contains(tok)
            || aliases.iter().any(|al| al.contains(tok))
    })
}

fn agent_list_priority(agent: &AgentView) -> u8 {
    if agent.turn_state == "errored" || (agent.state == "running" && agent.turn_state == "working")
    {
        0
    } else if agent.state == "running" {
        1
    } else if agent.state == "idle" {
        2
    } else if agent.state == "paused" {
        3
    } else {
        4
    }
}

const DEFAULT_LIST_LIMIT: usize = 100;
const MAX_LIST_LIMIT: usize = 100;

pub(crate) fn filter_and_summarize_agent_list(
    agents: &mut Vec<AgentView>,
    query: Option<&str>,
    limit: Option<usize>,
    cursor: Option<usize>,
) -> serde_json::Value {
    // The caller has already applied visibility, parent and state scope. Query
    // and pagination change the slice, not this population's health counts.
    let total = agents.len();
    let running = agents.iter().filter(|a| a.state == "running").count();
    let working = agents
        .iter()
        .filter(|a| a.state == "running" && a.turn_state == "working")
        .count();
    let errored = agents.iter().filter(|a| a.turn_state == "errored").count();
    let scaled_to_zero = agents.iter().filter(|a| a.state == "idle").count();
    let running_idle = agents
        .iter()
        .filter(|a| a.state == "running" && a.turn_state != "working" && a.turn_state != "errored")
        .count();
    let idle = running_idle + scaled_to_zero;
    let paused = agents.iter().filter(|a| a.state == "paused").count();
    let archived = agents.iter().filter(|a| a.state == "archived").count();

    if let Some(q) = query.map(str::trim).filter(|s| !s.is_empty()) {
        agents.retain(|a| agent_matches_query(a, q));
    }
    let matched = agents.len();
    // One total order at every page size: priority, name, then immutable ID.
    // Do not discard archived matches: continuation must reach the full population.
    agents.sort_by(|a, b| {
        agent_list_priority(a)
            .cmp(&agent_list_priority(b))
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.id.cmp(&b.id))
    });
    let cap = limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT);
    let start = cursor.unwrap_or(0).min(matched);
    let end = start.saturating_add(cap).min(matched);
    let next_cursor = (end < matched).then_some(end);
    agents.drain(..start);
    agents.truncate(cap);

    serde_json::json!({
        "total": total,
        "matched": matched,
        "returned": agents.len(),
        "next_cursor": next_cursor,
        "running": running,
        "working": working,
        "errored": errored,
        "idle": idle,
        "scaled_to_zero": scaled_to_zero,
        "paused": paused,
        "archived": archived,
    })
}

fn error_message(bytes: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned())
}

/// One call on the socket: the status code and the body.
pub(crate) async fn request(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    capability: Option<&str>,
    operator: Option<&str>,
) -> Result<(hyper::StatusCode, Vec<u8>), String> {
    request_naming(socket, method, path, body, capability, operator, None).await
}

/// One call on the socket, with the agent id this tool server was given. The id
/// is what a deployment without the boundary uses to name its one route; with
/// the boundary only the capability decides, and an operator token alone is
/// accepted beside it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn request_naming(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    capability: Option<&str>,
    operator: Option<&str>,
    agent_id: Option<&str>,
) -> Result<(hyper::StatusCode, Vec<u8>), String> {
    let payload = body.unwrap_or_default();
    let mut builder = hyper::Request::builder()
        .method(method)
        .uri(format!("http://localhost{path}"))
        .header(hyper::header::HOST, "localhost")
        .header(hyper::header::CONTENT_TYPE, "application/json");
    if let Some(token) = capability {
        builder = builder.header(crate::operator::AGENT_HEADER, token);
    }
    if let Some(token) = operator {
        builder = builder.header(crate::operator::HEADER, token);
    }
    if let Some(id) = agent_id {
        builder = builder.header(crate::auth::AGENT_ID_HEADER, id);
    }
    let request = builder
        .body(Full::new(Bytes::from(payload)))
        .map_err(|err| err.to_string())?;
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .map_err(|err| format!("cannot reach the server: {err}"))?;
    let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|err| err.to_string())?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = sender
        .send_request(request)
        .await
        .map_err(|err| err.to_string())?;
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .map_err(|err| err.to_string())?
        .to_bytes()
        .to_vec();
    Ok((status, bytes))
}

/// Parse an explicit bounded read without turning malformed bounds into absence.
fn parse_sequence_range(range: Option<&str>) -> Result<(Option<i64>, Option<i64>), String> {
    let Some(range) = range.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok((None, None));
    };
    let range = range.strip_prefix("seq:").unwrap_or(range).trim();
    let parse = |raw: &str| -> Result<i64, String> {
        let value = raw
            .trim()
            .parse::<i64>()
            .map_err(|_| "range must be seq:<start>-<end> or seq:<number>".to_string())?;
        if value < 0 {
            return Err("sequence bounds must be nonnegative".into());
        }
        Ok(value)
    };
    let (start, end) = match range.split_once('-') {
        Some((start, end)) => (parse(start)?, parse(end)?),
        None => {
            let value = parse(range)?;
            (value, value)
        }
    };
    if start > end {
        return Err("sequence range start must not exceed end".into());
    }
    Ok((Some(start), Some(end)))
}

#[cfg(test)]
mod spawn_args_tests {
    #[tokio::test]
    async fn mcp_and_cli_use_only_primary_socket_and_preserve_its_errors() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let primary = tokio::net::UnixListener::bind(&socket).unwrap();
        let admin = tokio::net::UnixListener::bind(dir.path().join("admin.sock")).unwrap();
        let admin_calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&admin_calls);
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, status, body) = tokio::select! {
                    accepted = primary.accept() => (accepted.unwrap().0, "500 Internal Server Error", r#"{"error":"primary refused"}"#),
                    accepted = admin.accept() => {
                        observed.fetch_add(1, Ordering::SeqCst);
                        (accepted.unwrap().0, "200 OK", r#"{"agents":[]}"#)
                    }
                };
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                }
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let client = Client {
            socket,
            config: dir.path().join("config"),
            capability: "test-capability".into(),
        };
        assert_eq!(
            client
                .request_method::<serde_json::Value>("POST", "/v1/agents", None)
                .await
                .unwrap_err(),
            "primary refused"
        );
        assert_eq!(client.get_agents().await.err().unwrap(), "primary refused");
        let data = dir.path().to_owned();
        let error = tokio::task::spawn_blocking(move || {
            crate::client::call::<serde_json::Value>(
                &data,
                "POST",
                "/v1/agents",
                None,
                &crate::client::Authority::None,
            )
            .unwrap_err()
        })
        .await
        .unwrap();
        assert_eq!(error, "primary refused");
        assert_eq!(admin_calls.load(Ordering::SeqCst), 0);
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn send_turn_result_ends_closing_tokens_without_an_interrupt_failure() {
        let closed = send_turn_result(
            SendResponse {
                deliveries: vec![],
                skipped: vec![],
            },
            true,
        )
        .unwrap();
        assert_eq!(closed["status"], "turn_complete_no_reply_sent");
        assert!(
            closed["turn_complete_hint"]
                .as_str()
                .unwrap()
                .contains("Do not call send or any other tool again")
        );
        let normal = send_turn_result(
            SendResponse {
                deliveries: vec![],
                skipped: vec![],
            },
            false,
        )
        .unwrap();
        assert!(normal.get("status").is_none());
        assert!(
            normal["turn_complete_hint"]
                .as_str()
                .unwrap()
                .contains("without calling send again")
        );
        assert_eq!(normal["deliveries"], json!([]));
    }

    #[test]
    fn send_closing_sentinels_are_exact_text_only_tokens() {
        for text in ["NO_REPLY", " \nNO_REPLY。 \n"] {
            assert!(
                SendRequest {
                    text: text.into(),
                    ..Default::default()
                }
                .is_turn_closing_sentinel(),
                "{text:?}"
            );
        }
        for text in [
            " End. ",
            "end",
            "停止。",
            "停止",
            "No more.",
            "NO MORE",
            "。",
            "Done.",
            "done",
            "終了。",
            "Final!",
            "done...",
            "",
            " ",
            "End of report.",
            "Done. The build passed.",
            "NO_REPLY is a token",
            "Final version",
            "Stopping",
            "!",
            ".",
        ] {
            assert!(
                !SendRequest {
                    text: text.into(),
                    ..Default::default()
                }
                .is_turn_closing_sentinel(),
                "{text:?}"
            );
        }
        for attachment in [
            json!({"media":{"kind":"photo","source":"fixture.png"}}),
            json!({"location":{"latitude":1.0,"longitude":2.0}}),
            json!({"poll":{"question":"End?","options":["yes","no"]}}),
            json!({"checklist":{"title":"End","tasks":["finish"]}}),
            json!({"article":{"title":"End","content":"report"}}),
            json!({"quote":{"text":"real quoted content"}}),
        ] {
            let mut value = attachment;
            value["text"] = json!("NO_REPLY");
            let request: SendRequest = serde_json::from_value(value).unwrap();
            assert!(!request.is_turn_closing_sentinel());
        }
    }

    #[test]
    fn explicit_sequence_range_refuses_invalid_bounds() {
        for range in [
            "seq:bad-bad",
            "seq:1-bad",
            "seq:bad-2",
            "seq:5-2",
            "seq:-1",
            "seq:1-2-3",
            "seq:9223372036854775808",
        ] {
            assert!(super::parse_sequence_range(Some(range)).is_err(), "{range}");
        }
        assert_eq!(
            super::parse_sequence_range(Some("seq:1-2")).unwrap(),
            (Some(1), Some(2))
        );
        assert_eq!(
            super::parse_sequence_range(Some("3")).unwrap(),
            (Some(3), Some(3))
        );
        assert_eq!(super::parse_sequence_range(None).unwrap(), (None, None));
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn spawn_routes_accept_separate_description_and_instructions() {
        let input = json!({
            "action": "spawn",
            "name": "worker",
            "description": "Public role",
            "instructions": "Private instructions"
        });
        let agents: AgentsArgs = serde_json::from_value(input.clone()).unwrap();
        let spawn: SpawnArgs = serde_json::from_value(input.clone()).unwrap();
        let subagents: SubagentsArgs = serde_json::from_value(input).unwrap();
        for (description, instructions) in [
            (agents.description, agents.instructions),
            (spawn.description, spawn.instructions),
            (subagents.description, subagents.instructions),
        ] {
            assert_eq!(description.as_deref(), Some("Public role"));
            assert_eq!(instructions.as_deref(), Some("Private instructions"));
        }
    }

    #[test]
    fn subagents_preserves_description_without_synthesizing_instructions() {
        let args: SubagentsArgs = serde_json::from_value(json!({
            "action": "spawn", "description": "Public role"
        }))
        .unwrap();
        assert_eq!(args.description.as_deref(), Some("Public role"));
        assert!(args.instructions.is_none());
    }

    fn dummy_agent_view(
        id: &str,
        name: &str,
        state: &str,
        turn_state: &str,
        description: Option<&str>,
    ) -> AgentView {
        AgentView {
            id: id.to_string(),
            name: name.to_string(),
            role: None,
            aliases: Vec::new(),
            parent: Some("agt_parent".to_string()),
            scope: "private".to_string(),
            relationship: Some("child".to_string()),
            can_message: Some(true),
            harness: "opencode".to_string(),
            model: Some("gpt-5.4-mini".to_string()),
            live_harness: None,
            live_model: None,
            process_started_at_ms: None,
            pending_switch: false,
            workdir: Some(format!("/data/agents/{id}/work")),
            state: state.to_string(),
            process: "none".to_string(),
            turn_state: turn_state.to_string(),
            delivery_mode: "turn".to_string(),
            steer_support: "send".to_string(),
            quota_reset_at_ms: None,
            block_reason: None,
            last_error: None,
            oldest_undelivered_age_ms: None,
            install: "installed".to_string(),
            description: description.map(str::to_string),
            instructions: None,
            seat_user: None,
            seat_uid: None,
            seat_gid: None,
            schedules: Vec::new(),
        }
    }

    #[test]
    fn list_query_and_pagination_preserve_summary_and_prioritize_working_agents() {
        let mut agents = Vec::new();
        for i in 0..120 {
            agents.push(dummy_agent_view(
                &format!("agt_idle_{i}"),
                &format!("idle-sub-{i}"),
                "idle",
                "idle",
                Some("Idle worker"),
            ));
        }
        agents.push(dummy_agent_view(
            "agt_work_1",
            "active-writer",
            "running",
            "working",
            Some("Implementing archetype parser"),
        ));
        agents.push(dummy_agent_view(
            "agt_err_1",
            "broken-runner",
            "running",
            "errored",
            Some("OOM failure"),
        ));
        agents.push(dummy_agent_view(
            "agt_run_idle_1",
            "standing-mind",
            "running",
            "idle",
            Some("Domain mind"),
        ));

        let mut default_capped = agents.clone();
        let summary = filter_and_summarize_agent_list(&mut default_capped, None, None, None);
        assert_eq!(summary["total"], 123);
        assert_eq!(summary["matched"], 123);
        assert_eq!(summary["returned"], 100);
        assert_eq!(summary["running"], 3);
        assert_eq!(summary["working"], 1);
        assert_eq!(summary["errored"], 1);
        assert_eq!(summary["scaled_to_zero"], 120);
        assert_eq!(summary["idle"], 121);
        assert_eq!(default_capped.len(), 100);
        assert!(default_capped.iter().any(|a| a.name == "active-writer"));
        assert!(default_capped.iter().any(|a| a.name == "broken-runner"));
        assert!(default_capped.iter().any(|a| a.name == "standing-mind"));

        let mut queried = agents.clone();
        let q_summary =
            filter_and_summarize_agent_list(&mut queried, Some("archetype active"), Some(10), None);
        assert_eq!(q_summary["total"], 123);
        assert_eq!(q_summary["matched"], 1);
        assert_eq!(q_summary["idle"], 121);
        assert_eq!(q_summary["running"], 3);
        assert_eq!(q_summary["returned"], 1);
        assert_eq!(queried[0].name, "active-writer");

        let mut limited = agents.clone();
        let limited_summary =
            filter_and_summarize_agent_list(&mut limited, Some("idle-sub"), Some(2), None);
        assert_eq!(limited_summary["total"], 123);
        assert_eq!(limited_summary["matched"], 120);
        assert_eq!(limited_summary["returned"], 2);
        assert_eq!(limited_summary["running"], 3);
        assert_eq!(limited_summary["idle"], 121);
        assert_eq!(limited.len(), 2);
        assert!(
            limited
                .iter()
                .all(|agent| agent.name.starts_with("idle-sub-"))
        );

        let mut unmatched = agents.clone();
        let no_matches =
            filter_and_summarize_agent_list(&mut unmatched, Some("no-such-agent"), Some(1), None);
        assert_eq!(no_matches["total"], 123);
        assert_eq!(no_matches["matched"], 0);
        assert_eq!(no_matches["returned"], 0);
        assert_eq!(no_matches["running"], 3);
        assert!(unmatched.is_empty());
    }

    #[test]
    fn list_query_preserves_archived_and_paused_population_counts() {
        let mut agents = vec![dummy_agent_view(
            "paused",
            "paused-worker",
            "paused",
            "idle",
            None,
        )];
        for i in 0..30 {
            agents.push(dummy_agent_view(
                &format!("archive-{i}"),
                &format!("archived-worker-{i}"),
                "archived",
                "idle",
                None,
            ));
        }
        // There are 30 archived agents in scope but only 11 match this query.
        // Pagination must preserve the full archived population.
        let summary =
            filter_and_summarize_agent_list(&mut agents, Some("archived-worker-1"), Some(2), None);
        assert_eq!(summary["total"], 31);
        assert_eq!(summary["archived"], 30);
        assert_eq!(summary["paused"], 1);
        assert_eq!(summary["matched"], 11);
        assert_eq!(summary["returned"], 2);
        assert_eq!(agents.len(), 2);
        assert!(
            agents
                .iter()
                .all(|agent| agent.name.starts_with("archived-worker-1"))
        );
    }

    #[test]
    fn list_order_is_identical_on_both_sides_of_limit_boundary() {
        let agents = vec![
            dummy_agent_view("paused", "a-paused", "paused", "idle", None),
            dummy_agent_view("running", "z-running", "running", "working", None),
        ];
        for limit in [1, 2, 100, usize::MAX] {
            let mut page = agents.clone();
            filter_and_summarize_agent_list(&mut page, None, Some(limit), None);
            assert_eq!(page[0].id, "running");
        }
        let mut tied = vec![
            dummy_agent_view("z", "same", "idle", "idle", None),
            dummy_agent_view("a", "same", "idle", "idle", None),
        ];
        filter_and_summarize_agent_list(&mut tied, None, None, None);
        assert_eq!(tied[0].id, "a");
    }

    #[test]
    fn list_pages_clamp_limits_and_reach_all_10002_agents_including_archives() {
        let agents: Vec<_> = (0..10002)
            .rev()
            .map(|i| {
                dummy_agent_view(
                    &format!("id-{i:05}"),
                    &format!("population-{i:05}"),
                    if i < 25 { "archived" } else { "idle" },
                    "idle",
                    None,
                )
            })
            .collect();
        let mut cursor = None;
        let mut seen = std::collections::HashSet::new();
        let mut pages = 0;
        loop {
            let mut page = agents.clone();
            let summary = filter_and_summarize_agent_list(
                &mut page,
                Some("population"),
                Some(usize::MAX),
                cursor,
            );
            assert_eq!(summary["total"], 10002);
            assert_eq!(summary["matched"], 10002);
            assert_eq!(summary["archived"], 25);
            assert_eq!(summary["idle"], 9977);
            assert!(page.len() <= MAX_LIST_LIMIT);
            for agent in page {
                assert!(seen.insert(agent.id), "duplicate page member");
            }
            pages += 1;
            cursor = summary["next_cursor"].as_u64().map(|v| v as usize);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(pages, 101);
        assert_eq!(seen.len(), 10002);
        let mut zero = agents.clone();
        let summary = filter_and_summarize_agent_list(&mut zero, None, Some(0), None);
        assert_eq!(zero.len(), 1);
        assert_eq!(summary["next_cursor"], 1);
        let mut beyond = agents;
        let summary = filter_and_summarize_agent_list(&mut beyond, None, None, Some(usize::MAX));
        assert!(beyond.is_empty());
        assert!(summary["next_cursor"].is_null());
    }

    #[tokio::test]
    async fn list_public_mcp_tools_expose_bounded_continuation_and_population_summary() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let parent_id = AgentId::generate();
        let mut population: Vec<_> = (0..103)
            .rev()
            .map(|i| {
                dummy_agent_view(
                    &format!("id-{i:03}"),
                    &format!("worker-{i:03}"),
                    "idle",
                    "idle",
                    None,
                )
            })
            .collect();
        population.push(dummy_agent_view(
            "paused", "a-paused", "paused", "idle", None,
        ));
        population.push(dummy_agent_view(
            "working",
            "z-running",
            "running",
            "working",
            None,
        ));
        for a in &mut population {
            a.instructions = Some("PRIVATE_PROMPT".into());
            a.parent = Some(parent_id.to_string());
        }
        let body = serde_json::to_vec(&AgentsResponse {
            agents: population,
            gateway: None,
        })
        .unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..8 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let byte = stream.read_u8().await.unwrap();
                    request.push(byte);
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let request_line = std::str::from_utf8(&request)
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap();
                let mut parts = request_line.split_whitespace();
                assert_eq!(parts.next(), Some("GET"));
                let uri: hyper::Uri = parts.next().unwrap().parse().unwrap();
                assert_eq!(uri.path(), "/v1/agents");
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let mcp = AgentMcp {
            agent: AgentRef {
                id: parent_id,
                name: "parent".into(),
                workdir: None,
            },
            client: Client {
                socket,
                config: dir.path().join("config"),
                capability: "synthetic-test-capability".into(),
            },
        };
        for legacy in [false, true] {
            for (arguments, expected_len, next, first) in [
                (json!({"limit": 1000000}), 100, Some(100), "z-running"),
                (
                    json!({"limit": 1000000, "offset": 100}),
                    5,
                    None,
                    "worker-099",
                ),
                (json!({"limit": 1}), 1, Some(1), "z-running"),
                (json!({"limit": 2}), 2, Some(2), "z-running"),
            ] {
                let mut args = arguments;
                args["action"] = json!("list");
                args["state"] = json!("all");
                let result = if legacy {
                    mcp.subagents(Parameters(serde_json::from_value(args).unwrap()))
                        .await
                        .unwrap()
                        .0
                } else {
                    mcp.agents(Parameters(serde_json::from_value(args).unwrap()))
                        .await
                        .unwrap()
                        .0
                };
                let rows = result[if legacy { "subagents" } else { "agents" }]
                    .as_array()
                    .unwrap();
                assert_eq!(rows.len(), expected_len);
                assert_eq!(rows[0]["name"], first);
                assert!(rows.iter().all(|a| a["instructions"].is_null()));
                assert_eq!(result["next_cursor"], json!(next));
                assert_eq!(result["summary"]["total"], 105);
                assert_eq!(result["summary"]["matched"], 105);
                assert_eq!(result["summary"]["working"], 1);
                assert_eq!(result["summary"]["paused"], 1);
            }
        }
        server.await.unwrap();
    }

    #[test]
    fn mcp_and_api_accept_singular_instruction_and_system_prompt_aliases() {
        for key in ["instruction", "system_prompt"] {
            let input = json!({
                "action": "spawn",
                "name": "worker",
                "description": "Public summary",
                key: "Private standing SOP"
            });
            let agents: AgentsArgs = serde_json::from_value(input.clone()).unwrap();
            let spawn: SpawnArgs = serde_json::from_value(input.clone()).unwrap();
            let describe: DescribeArgs = serde_json::from_value(input.clone()).unwrap();
            let subagents: SubagentsArgs = serde_json::from_value(input.clone()).unwrap();
            let api_req: crate::api::CreateAgentRequest = serde_json::from_value(input).unwrap();
            assert_eq!(agents.instructions.as_deref(), Some("Private standing SOP"));
            assert_eq!(spawn.instructions.as_deref(), Some("Private standing SOP"));
            assert_eq!(
                describe.instructions.as_deref(),
                Some("Private standing SOP")
            );
            assert_eq!(
                subagents.instructions.as_deref(),
                Some("Private standing SOP")
            );
            assert_eq!(
                api_req.instructions.as_deref(),
                Some("Private standing SOP")
            );
        }
    }

    #[tokio::test]
    async fn files_tool_supports_read_write_atomic_multi_edit_glob_and_grep() {
        let dir = tempfile::tempdir().unwrap();
        let workdir = dir.path().join("work");
        std::fs::create_dir_all(&workdir).unwrap();
        std::fs::write(workdir.join(".gitignore"), "ignored_dir\n*.log\n").unwrap();
        std::fs::create_dir_all(workdir.join("target")).unwrap();
        std::fs::write(workdir.join("target/skip.rs"), "fn skipped() {}\n").unwrap();
        std::fs::create_dir_all(workdir.join("ignored_dir")).unwrap();
        std::fs::write(workdir.join("ignored_dir/skip2.rs"), "fn skipped2() {}\n").unwrap();

        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "tester".into(),
                workdir: Some(workdir.clone()),
            },
            client: Client {
                socket: dir.path().join("unused.sock"),
                config: dir.path().join("config"),
                capability: "test-cap".into(),
            },
        };

        // 1. Write a nested file (auto-creating parent directories).
        let write_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "write",
                    "path": "src/engine.rs",
                    "content": "pub fn alpha() -> i32 {\n    10\n}\npub fn beta() -> i32 {\n    20\n}\n"
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(write_res["action"], "write");
        assert_eq!(write_res["bytes_written"], 65);
        let initial_sha = write_res["sha256"].as_str().unwrap().to_string();
        assert_eq!(initial_sha.len(), 64);

        // 2. Append to the file.
        let append_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "write",
                    "path": "src/engine.rs",
                    "content": "pub fn gamma() -> i32 {\n    30\n}\n",
                    "append": true
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(append_res["append"], true);
        assert_ne!(append_res["sha256"].as_str().unwrap(), initial_sha);

        // 3. Read line range.
        let read_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "read",
                    "path": "src/engine.rs",
                    "start_line": 4,
                    "end_line": 6
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(read_res["start_line"], 4);
        assert_eq!(read_res["end_line"], 6);
        assert_eq!(read_res["total_lines"], 9);
        assert_eq!(read_res["content"], "pub fn beta() -> i32 {\n    20\n}");
        assert!(
            read_res["numbered"]
                .as_str()
                .unwrap()
                .contains("4: pub fn beta()")
        );

        // 4. Atomic multi_edit: if second edit fails, first edit must NOT be written to disk.
        let failed_edit = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "multi_edit",
                    "path": "src/engine.rs",
                    "edits": [
                        { "target": "    10", "replacement": "    100" },
                        { "target": "pub fn beta() -> u64", "replacement": "pub fn beta() -> i64" }
                    ]
                }))
                .unwrap(),
            ))
            .await;
        let err_msg = match failed_edit {
            Err(msg) => msg,
            Ok(_) => panic!("expected missing target edit to fail"),
        };
        assert!(err_msg.contains("target` not found"), "{err_msg}");
        assert!(err_msg.contains("4: pub fn beta() -> i32"), "{err_msg}");
        let unchanged = std::fs::read_to_string(workdir.join("src/engine.rs")).unwrap();
        assert!(unchanged.contains("    10\n"));

        // 5. Valid multi_edit succeeds atomically.
        let edit_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "multi_edit",
                    "path": "src/engine.rs",
                    "edits": [
                        { "target": "    10", "replacement": "    100" },
                        { "target": "    20", "replacement": "    200\n    + 1" }
                    ]
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(edit_res["replacements_made"], 2);
        assert_eq!(edit_res["lines_removed"], 2);
        assert_eq!(edit_res["lines_added"], 3);

        // 6. Glob respects .gitignore and skips target/.git by default.
        let glob_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "glob",
                    "pattern": "**/*.rs"
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        assert_eq!(glob_res["files"], json!(["src/engine.rs"]));

        // 7. Grep with regex and context_lines.
        let grep_res = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "grep",
                    "query": r"pub fn (beta|gamma)\(\)",
                    "glob": "*.rs",
                    "context_lines": 1
                }))
                .unwrap(),
            ))
            .await
            .unwrap()
            .0;
        let hits = grep_res["matches"].as_array().unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["path"], "src/engine.rs");
        assert_eq!(hits[0]["line"], 4);
        assert_eq!(hits[0]["context_after"], json!(["    200"]));
        assert_eq!(hits[1]["line"], 8);

        // 8. Null bytes in path are rejected.
        let null_err = mcp
            .files(Parameters(
                serde_json::from_value(json!({
                    "action": "read",
                    "path": "bad\0path.rs"
                }))
                .unwrap(),
            ))
            .await;
        assert!(null_err.is_err());
    }

    #[tokio::test]
    async fn code_delivery_receipts_are_host_observed_not_snippet_authored() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("proof.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mcp = AgentMcp {
            agent: AgentRef {
                id: AgentId::generate(),
                name: "receipt-worker".into(),
                workdir: Some(dir.path().into()),
            },
            client: Client {
                socket,
                config: dir.path().join("config"),
                capability: "synthetic-receipt-capability".into(),
            },
        };
        let fabricated = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                console.log('send(');
                try { await anyagents.send({to: 123}); } catch {}
                return {deliveries: [{to:'agent:parent', id:'invented'}]};
            "#
                .into(),
                timeout_ms: Some(15000),
            }))
            .await
            .unwrap()
            .0;
        assert_eq!(fabricated["result"]["deliveries"][0]["id"], "invented");
        assert_eq!(fabricated["deliveries"], json!([]));
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                request.push(stream.read_u8().await.unwrap());
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let header = String::from_utf8(request).unwrap();
            let mut request_line = header.lines().next().unwrap().split_whitespace();
            assert_eq!(request_line.next(), Some("POST"));
            let uri: hyper::Uri = request_line.next().unwrap().parse().unwrap();
            assert_eq!(uri.path(), "/v1/send");
            let len: usize = header
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|n| n.trim().parse().unwrap())
                })
                .unwrap();
            let mut body = vec![0; len];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["text"],
                "real delivery"
            );
            let body = json!({"deliveries":[{"to":"agent:parent","id":"msg-observed","seq":1,"duplicate":false}]}).to_string();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(body.as_bytes()).await.unwrap();
        });
        let observed = mcp
            .code(Parameters(CodeArgs {
                code:
                    "await anyagents.send({to:'agent:parent', text:'real delivery'}); return null;"
                        .into(),
                timeout_ms: Some(15000),
            }))
            .await
            .unwrap()
            .0;
        assert_eq!(observed["result"], serde_json::Value::Null);
        assert_eq!(observed["deliveries"][0]["id"], "msg-observed");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn code_tool_executes_typescript_parallel_rpc_and_blocks_process_and_child_process() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let workdir = dir.path().join("work");
        std::fs::create_dir_all(&workdir).unwrap();
        std::fs::write(
            workdir.join("notes.txt"),
            "alpha-line\nbeta-target-line\ngamma-line\n",
        )
        .unwrap();

        let socket = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let parent_id = AgentId::generate();
        let population = vec![
            dummy_agent_view("agt_1", "alpha-worker", "running", "working", Some("First")),
            dummy_agent_view("agt_2", "beta-worker", "idle", "idle", Some("Second")),
        ];
        let body = serde_json::to_vec(&AgentsResponse {
            agents: population,
            gateway: None,
        })
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let byte = stream.read_u8().await.unwrap();
                request.push(byte);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
        });

        let mcp = AgentMcp {
            agent: AgentRef {
                id: parent_id,
                name: "parent".into(),
                workdir: Some(workdir.clone()),
            },
            client: Client {
                socket,
                config: dir.path().join("config"),
                capability: "synthetic-test-capability".into(),
            },
        };

        // 1. Parallel Promise.all across `tools.anyagents.agents` and `tools.anyagents.files` / `tools.workspace` with TypeScript syntax.
        let code_res = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                    interface Summary {
                        activeAgents: string[];
                        matchedLines: number[];
                        editedSha: string;
                    }
                    const [roster, grepRes, editRes] = await Promise.all([
                        tools.anyagents.agents({ action: "list", state: "all" }),
                        tools.workspace.grep({ query: "beta-target", path: "notes.txt" }),
                        tools.workspace.write({ path: "computed.txt", content: "ok\n" }),
                    ]);
                    console.log("parallel calls done", roster.agents.length);
                    const out: Summary = {
                        activeAgents: roster.agents
                            .filter((a: any) => a.state === "running")
                            .map((a: any) => a.name),
                        matchedLines: grepRes.matches.map((m: any) => m.line),
                        editedSha: editRes.sha256,
                    };
                    return out;
                "#
                .to_string(),
                timeout_ms: Some(15_000),
            }))
            .await
            .unwrap()
            .0;

        assert_eq!(code_res["truncated"], false);
        assert_eq!(code_res["result"]["activeAgents"], json!(["alpha-worker"]));
        assert_eq!(code_res["result"]["matchedLines"], json!([2]));
        assert_eq!(code_res["logs"], json!(["parallel calls done 2"]));
        assert!(workdir.join("computed.txt").is_file());
        server.await.unwrap();

        // 2. Verify capability sandbox blocks `process`, `process.env`, `require`, `child_process`, `fs`, `fetch`, and Function constructor escapes.
        let sandbox_probe = mcp
            .code(Parameters(CodeArgs {
                code: r#"
                    let constructorEscapeBlocked = false;
                    try {
                        (tools as any).constructor.constructor("return process")();
                    } catch {
                        constructorEscapeBlocked = true;
                    }
                    let fnEscapeBlocked = false;
                    try {
                        tools.anyagents.files.constructor("return process")();
                    } catch {
                        fnEscapeBlocked = true;
                    }
                    return {
                        typeofProcess: typeof (globalThis as any).process,
                        typeofRequire: typeof (globalThis as any).require,
                        typeofChildProcess: typeof (globalThis as any).child_process,
                        typeofFs: typeof (globalThis as any).fs,
                        typeofFetch: typeof (globalThis as any).fetch,
                        typeofBun: typeof (globalThis as any).Bun,
                        constructorEscapeBlocked,
                        fnEscapeBlocked,
                    };
                "#
                .to_string(),
                timeout_ms: Some(10_000),
            }))
            .await
            .unwrap()
            .0;

        assert_eq!(
            sandbox_probe["result"],
            json!({
                "typeofProcess": "undefined",
                "typeofRequire": "undefined",
                "typeofChildProcess": "undefined",
                "typeofFs": "undefined",
                "typeofFetch": "undefined",
                "typeofBun": "undefined",
                "constructorEscapeBlocked": true,
                "fnEscapeBlocked": true,
            })
        );

        // 3. Direct attempt to call `require("child_process")` or read `process.env` throws an error.
        for forbidden_snippet in [
            r#"return require("child_process").execSync("id").toString();"#,
            r#"return process.env.PATH;"#,
        ] {
            let res = mcp
                .code(Parameters(CodeArgs {
                    code: forbidden_snippet.to_string(),
                    timeout_ms: Some(5_000),
                }))
                .await;
            assert!(
                res.is_err(),
                "expected `{forbidden_snippet}` to fail in sandbox"
            );
        }

        // 4. Oversized return payloads (>48 KB) are safely truncated with `truncated: true`.
        let huge_res = mcp
            .code(Parameters(CodeArgs {
                code: r#"return "x".repeat(60000);"#.to_string(),
                timeout_ms: Some(10_000),
            }))
            .await
            .unwrap()
            .0;
        assert_eq!(huge_res["truncated"], true);
        assert!(huge_res["original_bytes"].as_u64().unwrap() >= 60_000);
        assert!(
            huge_res["result"]
                .as_str()
                .unwrap()
                .contains("result truncated:")
        );

        // 5. Pure Code Mode exposes only `code` in CANONICAL_MCP_TOOLS, and `anyagents.memory` + `workspace.{list,patch,edit}` work end-to-end inside `code`.
        assert_eq!(CANONICAL_MCP_TOOLS, &["code"]);
        std::fs::write(
            workdir.join("indent.rs"),
            "fn outer() {\n        let value = 41;\n        println!(\"{value}\");\n}\n",
        )
        .unwrap();

        let evo_res = mcp
            .code(Parameters(CodeArgs {
                code: r##"
                    await anyagents.memory.write({
                        topic: "build-invariants",
                        description: "Verified build and deployment invariants",
                        content: "# Build Invariants\n\nAlways verify live receipts.\n",
                    });
                    const readTopic = await anyagents.memory.read("build-invariants");
                    const listedMem = await anyagents.memory.list();
                    const searchedMem = await anyagents.memory.search("receipts");
                    const deletedMem = await anyagents.memory.delete("build-invariants");

                    // Indentation-tolerant edit (target has 0 spaces, file has 8 spaces)
                    await workspace.edit({
                        path: "indent.rs",
                        target: "let value = 41;\nprintln!(\"{value}\");",
                        replacement: "let value = 42;\nprintln!(\"answer={value}\");",
                    });

                    // V4A patch (Add + Update)
                    await workspace.patch([
                        "*** Begin Patch",
                        "*** Add File: src/patched.txt",
                        "+line one",
                        "+line two",
                        "*** Update File: src/patched.txt",
                        "@@",
                        " line one",
                        "-line two",
                        "+line two updated",
                        "*** End Patch",
                    ].join("\n"));

                    // Unified diff patch
                    await workspace.patch([
                        "--- a/src/patched.txt",
                        "+++ b/src/patched.txt",
                        "@@ -1,2 +1,3 @@",
                        " line one",
                        " line two updated",
                        "+line three unified",
                    ].join("\n"));

                    const dirList = await workspace.list("src");
                    const patchedRead = await workspace.read("src/patched.txt");
                    const indentRead = await workspace.read("indent.rs");
                    const execRes = await tools.workspace.exec({ cmd: "printf 'hello-%s' exec", cwd: "src" });
                    const hasWebHelpers = typeof tools.web.search === "function"
                        && typeof tools.web.fetch === "function"
                        && typeof web.search === "function"
                        && typeof anyagents.web.fetch === "function";

                    return {
                        topicSlug: readTopic.topic,
                        hasFact: readTopic.content.includes("Always verify live receipts."),
                        listedCount: listedMem.count,
                        searchCount: searchedMem.count,
                        deleted: deletedMem.deleted,
                        dirNames: dirList.entries.map((e: any) => e.name),
                        patchedContent: patchedRead.content,
                        indentContent: indentRead.content,
                        execExit: execRes.exit_code,
                        execStdout: execRes.stdout,
                        hasWebHelpers,
                    };
                "##
                .to_string(),
                timeout_ms: Some(15_000),
            }))
            .await
            .unwrap()
            .0;

        assert_eq!(evo_res["result"]["topicSlug"], "build-invariants");
        assert_eq!(evo_res["result"]["hasFact"], true);
        assert_eq!(evo_res["result"]["listedCount"], 1);
        assert_eq!(evo_res["result"]["searchCount"], 1);
        assert_eq!(evo_res["result"]["deleted"], true);
        assert_eq!(evo_res["result"]["dirNames"], json!(["patched.txt"]));
        assert_eq!(
            evo_res["result"]["patchedContent"],
            "line one\nline two updated\nline three unified"
        );
        assert_eq!(
            evo_res["result"]["indentContent"],
            "fn outer() {\n        let value = 42;\n        println!(\"answer={value}\");\n}"
        );
        assert_eq!(evo_res["result"]["execExit"], 0);
        assert_eq!(evo_res["result"]["execStdout"], "hello-exec");
        assert_eq!(evo_res["result"]["hasWebHelpers"], true);
    }
}
