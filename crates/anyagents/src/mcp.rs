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

use crate::api::{AgentsResponse, InboxResponse, SendRequest, SendResponse};
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
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let client = Client { socket, capability };
        let who = client.whoami().await.map_err(Error::Message)?;
        // A harness names the agent id it was given; the server already
        // answered with the agent this session acts as, and the two may not
        // differ.
        if let Ok(named) = std::env::var("ANYAGENTS_AGENT_ID") {
            if named.trim() != who.id.as_str() {
                return Err(Error::Message(
                    "this config directory belongs to another agent".into(),
                ));
            }
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
    capability: String,
}

impl Client {
    /// The agent this capability authenticates, as the server resolved it.
    async fn whoami(&self) -> Result<AgentRef, String> {
        let named = crate::client::agent_identity().map(|id| id.to_string());
        let who: serde_json::Value = request_naming(
            &self.socket,
            "GET",
            "/v1/whoami",
            None,
            Some(&self.capability),
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
        let named = crate::client::agent_identity().map(|id| id.to_string());
        let (status, bytes) = request_naming(
            &self.socket,
            method,
            path,
            body,
            Some(&self.capability),
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

#[derive(Debug, Deserialize, JsonSchema)]
struct SendArgs {
    /// Agent names or `agent:<name>` addresses. There is no default.
    to: Vec<String>,
    text: String,
    #[serde(default)]
    reply_to: Option<String>,
    /// Names this send. Send it again with the same value only when an earlier
    /// attempt got no answer.
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct InboxArgs {
    /// Exclusive sequence cursor. Omit to read from the start.
    #[serde(default)]
    since: Option<i64>,
}

#[tool_router(server_handler)]
impl AgentMcp {
    #[tool(description = "Send text to each address as this agent.")]
    async fn send(
        &self,
        Parameters(args): Parameters<SendArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<SendResponse>, String> {
        let body = SendRequest {
            // The server sends as this agent whatever this says, and refuses a
            // body that names anyone else.
            sender: Some(format!("agent:{}", self.agent.name)),
            to: args.to,
            text: args.text,
            reply_to: args.reply_to,
            quote: None,
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

    #[tool(description = "Read this agent's own mailbox.")]
    async fn inbox(
        &self,
        Parameters(args): Parameters<InboxArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<InboxResponse>, String> {
        // The server answers with its own agent id, so this path names the
        // mailbox the capability owns; on a deployment without the boundary the
        // route resolves it the same way.
        let agent = match crate::client::agent_identity() {
            Some(id) => id,
            None => self.agent.id.clone(),
        };
        let path = match args.since {
            Some(since) => format!("/v1/agents/{agent}/inbox?since={since}"),
            None => format!("/v1/agents/{agent}/inbox"),
        };
        let response = self.client.get(&path, None).await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(description = "List the agents on this server, with harness, state, and lag.")]
    async fn agents(&self) -> Result<rmcp::handler::server::wrapper::Json<AgentsResponse>, String> {
        let response = self.client.get("/v1/agents", None).await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }
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
