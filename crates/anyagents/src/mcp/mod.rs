//! MCP stdio server bound to one agent.
//!
//! An agent's tools reach the server through the one mailbox capability the
//! server minted for that agent's session. The capability is read from a file
//! in the agent's own config directory, which the CLI is told once as an
//! argument; it is never an argument itself, never a tool argument and never
//! printed. The server decides whose mailbox a call reads and who a message is
//! from, so a name the model writes can only choose a destination.

use std::path::{Path, PathBuf};
use std::sync::Arc;

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

mod code_children;
pub(crate) mod code_contract;
#[cfg(test)]
mod contract_tests;

pub(crate) use code_contract::CodeLimits;

mod agent_list;
mod args;
mod client;
mod code_mode;
mod files;
mod frame_cap;
mod media;
mod profile;
#[cfg(test)]
mod tests;
mod tools;

pub(crate) use agent_list::*;
use args::*;
#[cfg(test)]
pub(crate) use client::request;
use client::{Call, SERVED_HARNESS, as_call, current_call};
pub(crate) use client::{
    Client, HTTP_MCP_HEADER, RECONNECT_WITHIN, TASK_TURN_CONTEXT_HEADER, is_task_turn_context,
    request_waiting,
};
use code_mode::*;
use files::*;
pub(crate) use frame_cap::FrameArtifactStdout;
#[cfg(test)]
pub(crate) use frame_cap::MCP_TEXT_SPILL_BYTES;
use frame_cap::{agent_spill_dir, artifact_info, contains_media, spill_file};
pub use media::MultimediaJson;
use media::*;
pub use profile::McpProfile;
#[cfg(test)]
pub use profile::{CANONICAL_MCP_TOOLS, COORDINATION_MCP_TOOLS, REMOTE_MCP_TOOLS};

/// The agent whose tools run here: the id and name the server resolved from
/// the capability, never a name the caller wrote.
#[derive(Clone, Debug)]
pub struct AgentRef {
    pub id: AgentId,
    pub name: String,
    pub workdir: Option<PathBuf>,
}

/// Cancellation for the startup identity call. The startup `whoami` is a
/// daemon RPC, not a stdio frame, so the MCP transport is not running yet and
/// cannot observe the harness going away. A daemon that never answers would
/// otherwise leave the tool server hung forever, holding a process slot and
/// its memory. This watches the two signals that the harness is gone: its
/// stdin writer closing, and its process exiting.
#[cfg(unix)]
type HarnessGone = tokio::sync::oneshot::Receiver<()>;
#[cfg(not(unix))]
type HarnessGone = std::future::Pending<()>;

#[cfg(unix)]
fn harness_gone() -> (
    HarnessGone,
    Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::oneshot;

    let (gone_tx, gone_rx) = oneshot::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let parent = unsafe { libc::getppid() };
    std::thread::Builder::new()
        .name("mcp-startup-watch".into())
        .spawn(move || {
            let mut gone_tx = Some(gone_tx);
            loop {
                if thread_stop.load(Ordering::Relaxed) {
                    break;
                }
                // Request POLLIN so BSD poll reports POLLHUP too; poll never
                // consumes bytes, so an initialize frame buffered by the
                // harness stays for the stdio transport to read after startup.
                let mut stdin = libc::pollfd {
                    fd: libc::STDIN_FILENO,
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: poll receives one live pollfd for the call's duration.
                let ready = unsafe { libc::poll(&mut stdin, 1, 100) };
                let stdin_closed = ready > 0
                    && stdin.revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0;
                if stdin_closed || unsafe { libc::getppid() } != parent {
                    if let Some(tx) = gone_tx.take() {
                        let _ = tx.send(());
                    }
                    break;
                }
            }
        })
        .expect("spawn mcp startup watch");
    (gone_rx, Some(stop))
}

#[cfg(not(unix))]
fn harness_gone() -> (
    HarnessGone,
    Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) {
    (std::future::pending(), None)
}

/// Run the tool server for the agent whose config directory is `config`. The
/// server names the directory it started this agent with as
/// `ANYAGENTS_AGENT_CONFIG`; a command line that omits it is read from there,
/// and from nowhere else.
pub fn run(data: &Path, config: Option<&Path>, profile: Option<McpProfile>) -> Result<(), Error> {
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
    tool_server_runtime()?.block_on(async {
        let client = Client::new(socket, config_buf.clone(), capability);
        let (gone, stop_watch) = harness_gone();
        let who = tokio::select! {
            who = client.whoami_with_harness() => who,
            // The harness closed stdin or exited during startup; nothing is
            // left to serve, so leave cleanly instead of hanging on the RPC.
            _ = gone => return Ok(()),
        };
        if let Some(stop) = stop_watch {
            use std::sync::atomic::Ordering;
            stop.store(true, Ordering::Relaxed);
        }
        let (who, harness, limits) = who.map_err(Error::Message)?;
        // A call waits out a server restart only while its harness still
        // waits for the answer.
        let _ = SERVED_HARNESS.set(harness.clone());
        // The limits come from the server's environment only; a server that
        // names none leaves the defaults.
        CodeLimits::serve(limits.unwrap_or_default());
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
        let resolved_profile = McpProfile::resolve_stdio(profile, harness.as_deref(), &config_buf);
        let spill_dir = agent_spill_dir(&config_buf, &who.id);
        let service = AgentMcp {
            agent: who,
            client,
            profile: resolved_profile,
        }
        .serve({
            let (stdin, stdout) = rmcp::transport::io::stdio();
            (
                stdin,
                FrameArtifactStdout::with_spill_dir(stdout, spill_dir),
            )
        })
        .await
        .map_err(|err| Error::Message(err.to_string()))?;
        service
            .waiting()
            .await
            .map_err(|err| Error::Message(err.to_string()))?;
        Ok(())
    })
}

/// The runtime one agent's tool server runs on. Every agent has its own tool
/// server, so its fixed cost is paid once per agent: one thread drives every
/// call, and blocking work (file tools, local web fetches, stdio reads) goes
/// to tokio's on-demand blocking pool, whose idle threads exit by themselves.
/// A worker per CPU in each of a hundred tool servers is thousands of idle
/// threads on a shared machine.
fn tool_server_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// The capability an agent's harness reads: a file only that agent's own
/// config directory holds.
pub(crate) fn capability_of(config: &Path) -> Result<String, Error> {
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
pub(crate) struct AgentMcp {
    pub(crate) agent: AgentRef,
    pub(crate) client: Client,
    pub(crate) profile: McpProfile,
}

/// The `schedules` tool's contract, with the exact shape of each action.
const SCHEDULES_TOOL_DESCRIPTION: &str = "List authorized fleet automations with {action: list, agent: all}; fleet reads include owner names and never permit mutation. Wake yourself later or on an event. Create: `{action: \"create\", prompt, <one trigger>}` with `at` (\"20m\" or an ISO time), `every` (\"15m\") or `watch`: `github:pr:<owner>/<repo>#<num>` (PR or its checks terminal), `github:check:<owner>/<repo>@<ref>`, `spiron:promise:<id>`, `agent:idle:<name_or_id>`. `action: \"at\"|\"every\"|\"watch\"` with that field also creates. One trigger per schedule; make two calls for a watch plus a deadline. `{action: \"list\"}`; `{action: \"cancel\", id: \"sch_...\"|\"all\"}`. A fired schedule enters your mailbox like any message (steered into a running turn, held while you are paused or quota-blocked) and never queues a second wake while its last one is undelivered.";

/// The `code` tool's contract: language, scope, limits and every callable
/// name, compact because every agent turn pays for it.
/// `tool_definitions_with_roles` rewrites the limits to the configured ones.
const CODE_TOOL_DESCRIPTION: &str = "Run a TypeScript snippet (plain JS works; NOT Python). `code` is the body of an async function: `await` calls, `return` a JSON value; you get `{result, logs, duration_ms}`. No `import`, `require`, `process`, `Buffer`, `fs` or `fetch`; use the globals below. `timeout_ms`: default 30000, max 600000. An inner `workspace.exec` gets the remaining time, or its own `timeout_ms` (the call stretches to fit, up to the max).\n\
workspace: read(path|{path, start_line?, end_line?}), write(path, content), edit({path, target, replacement}), multi_edit({path, edits: [{target, replacement}]}), patch(text), glob(pattern|{pattern, path?}), grep(query|{query, path?, glob?}), list(path?), exec(cmd|{cmd, cwd?, timeout_ms?}) -> {exit_code, stdout, stderr}.\n\
anyagents: send, inbox, agents, schedule, conversation, groups, browser, skills, interact; each takes its MCP tool's object, e.g. `anyagents.schedule({action: \"create\", watch: \"github:pr:o/r#1\", prompt: \"...\"})`. memory: read(topic), write({topic, description, content}), search(query), list(). web: search(query), fetch(url). computer, device, media: `<ns>.<action>(opts)`. All also under `tools.`. For `media.video_generate({prompt, model?})`, set the outer `code` `timeout_ms` above 300000 when the configured ceiling allows it.\n\
`console.log` goes to `logs`. Use `Promise.all` for independent calls. Run other languages from a file you `workspace.write`, not a quoted one-liner. Reading images, PDF, audio or video, and screenshots, attach the media.";

const CODE_TOOL_LIMITS_TEXT: &str = "default 30000, max 600000";

/// The `code` description with the limits this server enforces and what the
/// agent's workspace backend (`WorkspaceBackend::kind`) does with an exec's
/// `timeout_ms`.
fn code_tool_description(limits: CodeLimits, backend: &str) -> String {
    let mut limits_text = format!("default {}, max {}", limits.default_ms, limits.max_ms);
    match backend {
        "local" => {}
        "docker" => limits_text.push_str(
            "; `workspace.exec` runs in your container, which does not stop a command at its `timeout_ms`",
        ),
        _ => limits_text.push_str(&format!(
            "; `workspace.exec` runs in your remote sandbox, which stops a command at {}",
            agent_tools::REMOTE_EXEC_MAX_TIMEOUT_MS
        )),
    }
    CODE_TOOL_DESCRIPTION.replace(CODE_TOOL_LIMITS_TEXT, &limits_text)
}

/// Remove prose keys (`description`, `title`) from a JSON Schema tree. Field
/// prose duplicates the tool-level description and is the main driver of
/// `tools/list` frame size; keeping the structural schema (types, required
/// fields, enums) preserves how clients build calls.
fn strip_schema_prose(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.remove("description");
            map.remove("title");
            for nested in map.values_mut() {
                strip_schema_prose(nested);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                strip_schema_prose(item);
            }
        }
        _ => {}
    }
}

/// Compact one tool definition for `tools/list`: schema prose is dropped so
/// the single-line JSON-RPC response stays below the 64 KiB stdio scanner
/// budget that stdio clients use. The tool description itself is untouched.
fn compact_tool_definition(mut tool: rmcp::model::Tool) -> rmcp::model::Tool {
    let mut input = serde_json::to_value(&*tool.input_schema).unwrap_or(serde_json::Value::Null);
    strip_schema_prose(&mut input);
    if let Some(object) = input.as_object() {
        tool.input_schema = std::sync::Arc::new(object.clone());
    }
    if let Some(output) = &tool.output_schema {
        let mut output = serde_json::to_value(&**output).unwrap_or(serde_json::Value::Null);
        strip_schema_prose(&mut output);
        if let Some(object) = output.as_object() {
            tool.output_schema = Some(std::sync::Arc::new(object.clone()));
        }
    }
    tool
}

pub fn tool_definitions(profile: McpProfile) -> Vec<rmcp::model::Tool> {
    let allowed = profile.exposed_tools();
    let all = AgentMcp::tool_router().list_all();
    allowed
        .iter()
        .filter_map(|wanted| all.iter().find(|t| t.name.as_ref() == *wanted).cloned())
        .map(compact_tool_definition)
        .collect()
}

/// `tool_definitions` plus the role-template catalog discovered under
/// `.agents/agents/`. The catalog is appended to the tool-level description of
/// every exposed tool with a `role` argument, because schema prose is stripped
/// by `compact_tool_definition`.
pub fn tool_definitions_with_roles(
    profile: McpProfile,
    workdir: Option<&Path>,
    data_dir: Option<&Path>,
) -> Vec<rmcp::model::Tool> {
    let mut tools = tool_definitions(profile);
    let Some(dd) = data_dir else {
        return tools;
    };
    let templates = crate::create::discover_available_archetype_templates(workdir, None, dd);
    if templates.is_empty() {
        return tools;
    }
    let role_catalog = templates
        .iter()
        .map(|(slug, desc)| match desc {
            Some(d) => format!("`{slug}` ({d})"),
            None => format!("`{slug}`"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let append_note =
        format!(" Available roles (`general` is the built-in default): {role_catalog}.");
    for tool in &mut tools {
        let has_role_arg = tool
            .input_schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|props| props.contains_key("role"));
        if !has_role_arg {
            continue;
        }
        let mut desc = tool.description.as_deref().unwrap_or_default().to_string();
        if !desc.contains("Available roles (") {
            desc.push_str(&append_note);
            tool.description = Some(desc.into());
        }
    }
    tools
}

impl rmcp::ServerHandler for AgentMcp {
    fn get_info(&self) -> rmcp::model::ServerConfig {
        rmcp::model::ServerConfig::new(
            rmcp::model::ServerCapabilities::builder()
                .enable_tools()
                .build(),
        )
        .with_server_info(rmcp::model::Implementation::from_build_env())
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::ListToolsResult, rmcp::ErrorData> {
        let supports_cache_hints = context
            .protocol_version()
            .is_some_and(|version| version >= rmcp::model::ProtocolVersion::V_2026_07_28);
        let tools = self.listed_tools();
        Ok(rmcp::model::ListToolsResult {
            result_type: Some(rmcp::model::ResultType::COMPLETE),
            tools,
            meta: None,
            next_cursor: None,
            ttl_ms: supports_cache_hints.then_some(0),
            cache_scope: supports_cache_hints.then_some(rmcp::model::CacheScope::Public),
        })
    }

    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, rmcp::ErrorData> {
        if !self.profile.allows_tool(request.name.as_ref()) {
            return Err(rmcp::ErrorData::invalid_params(
                format!(
                    "tool `{}` is not available under the `{}` MCP profile",
                    request.name,
                    self.profile.as_str()
                ),
                None,
            ));
        }
        let ct = context.ct.clone();
        // rmcp moves params._meta into RequestContext. Scope the opaque
        // opener credential to this dispatch, including nested code calls.
        let mut scoped = self.clone();
        if !scoped.client.via_http {
            scoped.client.turn_context = context
                .meta
                .get("io.spiron/turn-context")
                .and_then(serde_json::Value::as_str)
                .filter(|token| is_task_turn_context(token))
                .map(str::to_owned);
        }
        let tcc = rmcp::handler::server::tool::ToolCallContext::new(&scoped, request, context);
        as_call(
            Call::new(Arc::new(move || ct.is_cancelled())),
            Self::tool_router().call(tcc),
        )
        .await
    }

    fn get_tool(&self, name: &str) -> Option<rmcp::model::Tool> {
        if !self.profile.allows_tool(name) {
            return None;
        }
        Self::tool_router().get(name).cloned()
    }
}

pub(crate) fn urlencoding_simple_pub(s: &str) -> String {
    urlencoding_simple(s)
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
