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
        Ok(AgentRef {
            id: AgentId::parse(id).map_err(|err| err.to_string())?,
            name: name.to_string(),
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
}

struct AgentMcp {
    agent: AgentRef,
    client: Client,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SendArgs {
    /// Destinations: agent names or IDs (`sage`, `agent:sage`, `agt_...`, `parent`,
    /// `children`), internal groups (`group:owners`, `group:<name>`, `group:all`, `all`),
    /// or external channel targets (`telegram:chat:<chat_id>[:<topic_id>]`,
    /// `telegram:user:<user_id>[:<message_id>]`). Sending to an archived agent's
    /// name or `agt_...` ID automatically wakes and resumes its session. May be
    /// empty (`[]`) when `reply_to` names a mailbox reference (`anyagents:reply:<msg_id>`).
    #[serde(default, deserialize_with = "crate::api::deserialize_string_or_vec")]
    to: Vec<String>,
    /// Message text or caption (Markdown is automatically rendered as rich HTML on Telegram).
    /// May be omitted when sending `media`, `location`, or `poll`.
    #[serde(default)]
    text: String,
    /// Optional reply target: `anyagents:reply:<msg_id>` from the message header
    /// (answers in the same chat/topic/agent), or a plain destination address.
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
    /// Names this send. Send it again with the same value only when an earlier
    /// attempt got no answer.
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct ConversationArgs {
    /// Optional semantic + keyword search query (Hybrid RAG: dense vector embedding
    /// `voyageai/voyage-4-large` + SQLite FTS5 trigram BM25 with Reciprocal Rank Fusion).
    /// When set, searches across all historical and live turns without loading the
    /// full history. Omit `query` to read recent chronological turns.
    #[serde(default, alias = "q", alias = "search")]
    query: Option<String>,
    /// Agent name (`sage`, `argon`, `prism`, `"all"`) or `agt_...` ID whose complete
    /// two-way (`in` + `out`) conversation to read or search. Defaults to `"all"` when
    /// `query` is provided (searching across all agents), or this agent when reading
    /// recent chronological turns.
    #[serde(default)]
    agent: Option<String>,
    /// Optional peer filter (e.g. `alice`, `telegram:user:42`, `telegram:chat:-1001234567890`,
    /// `group:owners`, or `agent:beta`).
    #[serde(default)]
    with: Option<String>,
    /// Optional exclusive timestamp cursor in milliseconds (`timestamp_ms > since_ms`).
    #[serde(default)]
    since_ms: Option<i64>,
    /// Optional maximum number of turns to return (defaults to 20 when `query` is set,
    /// or 50 for chronological history).
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct AgentsArgs {
    /// Optional state filter: `"running"`, `"archived"`, or `"all"` (default `"all"`).
    #[serde(default)]
    state: Option<String>,
    /// Optional parent filter: agent name, `agt_...` ID, `"self"` (children of this agent),
    /// or `"root"` (persistent root agents with no parent).
    #[serde(default)]
    parent: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SpawnArgs {
    /// Optional short agent name (`[a-z0-9][a-z0-9_-]*`). If omitted, a unique
    /// `<caller>-sub-<id>` name is generated automatically.
    #[serde(default)]
    name: Option<String>,
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
    /// Optional working directory path.
    #[serde(default)]
    workdir: Option<String>,
    /// Optional system/role description or standing instructions for the spawned agent.
    #[serde(default, alias = "description", alias = "role")]
    instructions: Option<String>,
    /// Optional initial task/message to deliver immediately to the spawned agent.
    #[serde(default)]
    prompt: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct DescribeArgs {
    /// Target agent name (`"self"`, `"xenon"`, `"sage"`, `"all"`, or `agt_...` ID). Defaults to `"self"`.
    #[serde(default, alias = "name", alias = "target")]
    agent: Option<String>,
    /// New role/responsibility description to set (syncs into the agent's private system prompt and group roster).
    /// Omit to read the current description.
    #[serde(default, alias = "instructions", alias = "role", alias = "text")]
    description: Option<String>,
    /// Set `true` to clear the agent's custom description.
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
    /// Harness catalog ID when `action = "spawn"` (`opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`).
    #[serde(default)]
    harness: Option<String>,
    /// Optional model override when `action = "spawn"`.
    #[serde(default)]
    model: Option<String>,
    /// Optional account profile when `action = "spawn"`.
    #[serde(default)]
    profile: Option<String>,
    /// Optional working directory when `action = "spawn"`.
    #[serde(default)]
    workdir: Option<String>,
    /// Optional system/role instructions when `action = "spawn"`.
    #[serde(default, alias = "description", alias = "role")]
    instructions: Option<String>,
    /// Task prompt or follow-up message when `action` is `"spawn"`, `"steer"`, `"send"`, `"wake"`, or `"resume"`.
    #[serde(default, alias = "text", alias = "message")]
    prompt: Option<String>,
    /// Number of recent conversation turns to include when `action = "inspect"` (default 10).
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct ArchiveArgs {
    /// Agent name or `agt_...` ID to archive (scales process to zero while keeping
    /// its permanent `agt_...` ID, mailbox, and session on disk so any future message
    /// or `wake` call resumes it). Defaults to this agent itself when omitted.
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
    #[serde(default)]
    group: Option<String>,
    /// Optional agent name or `agt_...` ID to add/remove (defaults to this agent).
    #[serde(default)]
    agent: Option<String>,
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

#[tool_router(server_handler)]
impl AgentMcp {
    #[tool(
        description = "Send a message, media file (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`), GPS `location`/venue, interactive `poll`, `checklist`, or Instant View `article` to one or multiple agents (`sage`, `agt_...`, `parent`, `children`), internal groups (`group:<name>`, `group:all`, `all`), or external channel targets (`telegram:chat:<id>[:<topic>]`, `telegram:user:<id>`)."
    )]
    async fn send(
        &self,
        Parameters(args): Parameters<SendArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<SendResponse>, String> {
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
        let response = self
            .client
            .get(
                "/v1/send",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Unified two-way (`in` + `out`) conversation reader and Hybrid RAG semantic search (`query`). Live incoming messages are pushed automatically to your session, so you never need to poll an inbox. Pass `query` to run Hybrid RAG search (dense vector embedding + BM25 keyword search) across all agents' historical memory (`agent` defaults to `\"all\"`), or omit `query` to read recent chronological two-way turns of yourself or any peer (`agent`, `with`, `since_ms`, `limit`)."
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
        let default_agent = if clean_q.is_some() {
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
        if let Some(q) = clean_q {
            query.push(format!("query={}", urlencoding_simple(q)));
        }
        if let Some(with_peer) = args
            .with
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("with={}", urlencoding_simple(with_peer)));
        }
        if let Some(since_ms) = args.since_ms {
            query.push(format!("since_ms={since_ms}"));
        }
        let limit = args
            .limit
            .unwrap_or(if clean_q.is_some() { 20 } else { 50 });
        query.push(format!("limit={limit}"));
        let path = format!("/v1/agents/{target}/conversation?{}", query.join("&"));
        let response = self.client.get(&path, None).await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Hybrid RAG semantic + keyword search across all agents' historical and live conversations (`query`, optional `agent`, `with`, `since_ms`, `limit`). Uses locked `voyageai/voyage-4-large` dense embeddings fused with SQLite FTS5 trigram BM25."
    )]
    async fn search(
        &self,
        Parameters(args): Parameters<ConversationArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<ConversationResponse>, String> {
        self.conversation(Parameters(args)).await
    }

    #[tool(
        description = "List agents (persistent and sub-agents, running or archived) with their permanent `agt_...` ID, name, parent, harness, state, and lag. Supports optional `state` (`running` | `archived` | `all`) and `parent` (`self` | `root` | `<name/id>`) filters."
    )]
    async fn agents(
        &self,
        Parameters(args): Parameters<AgentsArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<AgentsResponse>, String> {
        let mut response: AgentsResponse = self.client.get("/v1/agents", None).await?;
        if let Some(state_filter) = args
            .state
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
        {
            response
                .agents
                .retain(|a| a.state.eq_ignore_ascii_case(state_filter));
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
            if cleaned.eq_ignore_ascii_case("root") || cleaned.eq_ignore_ascii_case("none") {
                response.agents.retain(|a| a.parent.is_none());
            } else {
                let wanted_id = if cleaned.eq_ignore_ascii_case("self")
                    || cleaned.eq_ignore_ascii_case(&self.agent.name)
                    || cleaned == self.agent.id.as_str()
                {
                    Some(self.agent.id.to_string())
                } else {
                    response
                        .agents
                        .iter()
                        .find(|a| a.name.eq_ignore_ascii_case(cleaned) || a.id == cleaned)
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
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Spawn a new sub-agent with this agent as its `parent`. Returns its permanent `agt_...` ID and name. The sub-agent automatically scales to zero (`state = archived`) once its turn finishes and its mailbox is drained, and automatically wakes/resumes whenever a new message is sent to its ID/name."
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
            harness,
            model: args.model,
            profile: args.profile,
            workdir: args.workdir,
            instructions: args.instructions,
            parent: Some(self.agent.id.to_string()),
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
        description = "Unified cross-harness sub-agent manager (`action`: `list` | `inspect` | `spawn` | `steer` | `wake` | `pause` | `archive` | `delete`). Use `list` (default) to view your sub-agents and their live status (`running`, `paused`, `archived`, `turn_state`); use `inspect` with `agent` to view a sub-agent's live status AND its recent conversation turns; use `spawn` to launch a sub-agent on any harness (`opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `dsh`, `grok`), model, profile, and workdir; use `steer` to send follow-up instructions; use `pause`, `archive`, `wake`, or `delete` to control its lifecycle."
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
                let mut resp: AgentsResponse = self.client.get("/v1/agents", None).await?;
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
                            .find(|a| a.name.eq_ignore_ascii_case(cleaned) || a.id == cleaned)
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
                    resp.agents.retain(|a| a.state.eq_ignore_ascii_case(st));
                }
                let running = resp.agents.iter().filter(|a| a.state == "running").count();
                let working = resp
                    .agents
                    .iter()
                    .filter(|a| a.state == "running" && a.turn_state == "working")
                    .count();
                let paused = resp.agents.iter().filter(|a| a.state == "paused").count();
                let archived = resp.agents.iter().filter(|a| a.state == "archived").count();
                Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                    "parent": self.agent.name,
                    "summary": {
                        "total": resp.agents.len(),
                        "running": running,
                        "working": working,
                        "idle": running.saturating_sub(working),
                        "paused": paused,
                        "archived": archived,
                    },
                    "subagents": resp.agents,
                })))
            }
            "inspect" | "status" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        "`subagents inspect` requires `agent` (name or agt_... id)".to_string()
                    })?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let resp: AgentsResponse = self.client.get("/v1/agents", None).await?;
                let view = resp
                    .agents
                    .into_iter()
                    .find(|a| a.name.eq_ignore_ascii_case(clean) || a.id == clean)
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
                            turns: Vec::new(),
                        });
                Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                    "subagent": view,
                    "recent_turns": conv.turns,
                })))
            }
            "spawn" | "create" => {
                let spawned = self
                    .spawn(Parameters(SpawnArgs {
                        name: args.agent,
                        harness: args.harness,
                        model: args.model,
                        profile: args.profile,
                        workdir: args.workdir,
                        instructions: args.instructions,
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
            other => Err(format!(
                "unknown subagents action `{other}`; expected `list`, `inspect`, `spawn`, `steer`, `wake`, `pause`, `archive`, or `delete`"
            )),
        }
    }

    #[tool(
        description = "Archive an agent (`state = archived`), stopping its OS process while preserving its permanent `agt_...` ID, mailbox, and session so any future message or `wake` call resumes it."
    )]
    async fn archive(
        &self,
        Parameters(args): Parameters<ArchiveArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<AgentView>, String> {
        let target = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
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
        let body = GroupActionRequest {
            action: action.to_string(),
            group,
            agent: args.agent,
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
            && !args.clear.unwrap_or(false)
        {
            let resp: AgentsResponse = self.client.get("/v1/agents", None).await?;
            let roster: Vec<serde_json::Value> = resp
                .agents
                .into_iter()
                .filter(|a| a.state != "archived" || a.parent.is_none())
                .map(|a| {
                    serde_json::json!({
                        "id": a.id,
                        "name": a.name,
                        "parent": a.parent,
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
        if args.description.is_some() || args.clear.unwrap_or(false) {
            let body = serde_json::json!({
                "description": args.description,
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
        description = "Native SOTA anti-detect browser with persistent login profiles and scale-to-zero lifecycle (`action`: `open` | `snapshot` | `click` | `fill` | `press` | `eval` | `screenshot` | `cookies` | `tabs` | `status` | `profiles` | `close`). Runs real headed Chromium on a virtual X11 display (`xvfb-headed`) with deterministic per-profile WebGL/Client-Hints/Canvas stealth fingerprinting and persistent `--user-data-dir` login state. Pass `action: \"open\"` with `url` to navigate and receive interactive `@e1`, `@e2` refs; pass `action: \"click\"` or `\"fill\"` with `selector: \"@e1\"`; pass `action: \"close\"` when finished to scale the browser process to zero while keeping the profile logged in."
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
