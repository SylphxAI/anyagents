//! MCP stdio server bound to one agent. Tools call the local unix-socket API.

use std::path::Path;

use anyagents_core::Error;
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
use crate::server;

pub fn run(data: &Path, agent: &str) -> Result<(), Error> {
    let socket = server::socket_path(data);
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let listed = get_json::<AgentsResponse>(&socket, "GET", "/v1/agents", None)
            .await
            .map_err(Error::Message)?;
        if !listed.agents.iter().any(|row| row.name == agent) {
            return Err(Error::AgentNotFound(agent.to_string()));
        }
        let service = AgentMcp {
            agent: agent.to_string(),
            socket,
        }
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

struct AgentMcp {
    agent: String,
    socket: std::path::PathBuf,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SendArgs {
    /// Agent names or `agent:<name>` addresses. There is no default.
    to: Vec<String>,
    text: String,
    #[serde(default)]
    reply_to: Option<String>,
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
            sender: Some(format!("agent:{}", self.agent)),
            to: args.to,
            text: args.text,
            reply_to: args.reply_to,
        };
        let response = get_json(
            &self.socket,
            "POST",
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
        let path = match args.since {
            Some(since) => format!(
                "/v1/agents/{}/inbox?since={since}",
                path_segment(&self.agent)
            ),
            None => format!("/v1/agents/{}/inbox", path_segment(&self.agent)),
        };
        let response = get_json(&self.socket, "GET", &path, None).await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(description = "List the agents on this server, with harness, state, and lag.")]
    async fn agents(&self) -> Result<rmcp::handler::server::wrapper::Json<AgentsResponse>, String> {
        let response = get_json(&self.socket, "GET", "/v1/agents", None).await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }
}

fn path_segment(raw: &str) -> String {
    let mut out = String::new();
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

async fn get_json<T: DeserializeOwned>(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<T, String> {
    let payload = body.unwrap_or_default();
    let request = hyper::Request::builder()
        .method(method)
        .uri(format!("http://localhost{path}"))
        .header(hyper::header::HOST, "localhost")
        .header(hyper::header::CONTENT_TYPE, "application/json")
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
        .to_bytes();
    if !status.is_success() {
        let message = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned());
        return Err(message);
    }
    serde_json::from_slice(&bytes).map_err(|err| err.to_string())
}
