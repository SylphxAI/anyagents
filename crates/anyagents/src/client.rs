//! The Unix-socket client the command line uses.
//!
//! A command presents the operator token when the environment names one; a
//! command that runs inside an agent presents that agent's capability, read from
//! its own config directory. Nothing else is an authority.

use std::path::Path;
use std::time::Duration;

use anyagents_core::AgentId;

use crate::operator::CAPABILITY_FILE;
use crate::server;

/// How long one call may take. Creating an agent installs a harness, which can
/// take a minute; everything else answers at once.
const TIMEOUT: Duration = Duration::from_secs(180);

/// What this client presents.
#[derive(Clone, Debug)]
pub(crate) enum Authority {
    Operator,
    Agent(AgentId),
    /// No authority: only a deployment without the boundary answers.
    None,
}

/// The authority this process may present: the agent's own capability when it
/// runs inside an agent, else the operator's token.
pub(crate) fn authority(data: &Path) -> Authority {
    // Inside an agent its own capability is the only authority it has; a
    // command run by the operator presents the operator's token.
    if crate::operator::operator_token(data).is_some() {
        return Authority::Operator;
    }
    if let Some(id) = agent_identity() {
        return Authority::Agent(id);
    }
    Authority::None
}

/// The agent whose config directory this process runs with, from the interface
/// the deployment exports to an agent's harness. An id, never a name.
pub(crate) fn agent_identity() -> Option<AgentId> {
    let raw = std::env::var("ANYAGENTS_AGENT_ID").ok()?;
    AgentId::parse(raw.trim()).ok()
}

fn capability(id: &AgentId) -> Option<String> {
    let data = crate::data::data_dir()?;
    let path = anyagents_core::agent_config_dir(&data, id).join(CAPABILITY_FILE);
    let text = std::fs::read_to_string(path).ok()?;
    let token = text.trim().to_string();
    (!token.is_empty()).then_some(token)
}

/// One call, answered as JSON.
pub(crate) fn call<T: serde::de::DeserializeOwned>(
    data: &Path,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    authority: &Authority,
) -> Result<Option<T>, String> {
    let (agent_token, operator_token) = match authority {
        Authority::Operator => (None, crate::operator::operator_token(data)),
        Authority::Agent(id) => (capability(id), None),
        Authority::None => (None, None),
    };
    // A boundary refuses a call that has no credential rather than answering
    // with nothing, so a client notices instead of reading an empty deployment.
    let socket = server::socket_path(data);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    let answer = runtime.block_on(async {
        tokio::time::timeout(
            TIMEOUT,
            crate::mcp::request(
                &socket,
                method,
                path,
                body,
                agent_token.as_deref(),
                operator_token.as_deref(),
            ),
        )
        .await
        .map_err(|_| "the server did not answer".to_string())?
    })?;
    let (status, bytes) = answer;
    if !status.is_success() {
        return Err(error_message(&bytes));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|err| err.to_string())
}

/// The error a refused call reported, without a secret or a presented token.
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
