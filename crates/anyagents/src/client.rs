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
    if std::env::var_os("ANYAGENTS_OPERATOR_TOKEN_FILE").is_none()
        && let Some(id) = agent_identity()
    {
        return Authority::Agent(id);
    }
    if crate::operator::operator_token(data).is_some() {
        return Authority::Operator;
    }
    if let Some(id) = agent_identity() {
        return Authority::Agent(id);
    }
    Authority::None
}

pub(crate) fn can_call_server(data: &Path) -> bool {
    server::socket_path(data).exists() && !matches!(authority(data), Authority::None)
}

/// The agent whose config directory this process runs with, from the interface
/// the deployment exports to an agent's harness. An id, never a name.
pub(crate) fn agent_identity() -> Option<AgentId> {
    let raw = std::env::var("ANYAGENTS_AGENT_ID").ok()?;
    AgentId::parse(raw.trim()).ok()
}

fn capability(data: &Path, id: &AgentId) -> Option<String> {
    let dir = std::env::var_os("ANYAGENTS_AGENT_CONFIG")
        .filter(|v| !v.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let root = crate::data::data_dir().unwrap_or_else(|| data.to_path_buf());
            anyagents_core::agent_config_dir(&root, id)
        });
    let path = dir.join(CAPABILITY_FILE);
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
        Authority::Agent(id) => (capability(data, id), None),
        Authority::None => (None, None),
    };
    // A boundary refuses a call that has no credential rather than answering
    // with nothing, so a client notices instead of reading an empty deployment.
    let socket = server::socket_path(data);
    let admin_socket = data.join("admin.sock");
    let prefer_admin = admin_socket.exists()
        && ((method == "POST" && path == "/v1/agents")
            || path.ends_with("/harness")
            || path.ends_with("/model")
            || path.ends_with("/rename")
            || path.ends_with("/role")
            || path.ends_with("/description")
            || path.ends_with("/wake")
            || path.ends_with("/pause")
            || path.ends_with("/archive")
            || path.starts_with("/v1/subagent-defaults")
            || path.starts_with("/v1/profiles")
            || path.starts_with("/v1/schedules")
            || path.starts_with("/v1/browser")
            || path.starts_with("/v1/send")
            || path.contains("/schedules")
            || path.contains("/conversation"));
    let (primary_sock, secondary_sock) = if prefer_admin {
        (&admin_socket, Some(&socket))
    } else {
        (&socket, admin_socket.exists().then_some(&admin_socket))
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    let body_clone = body.clone();
    let answer = runtime.block_on(async {
        tokio::time::timeout(
            TIMEOUT,
            crate::mcp::request(
                primary_sock,
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
    let (mut status, mut bytes) = answer;
    if !status.is_success()
        && let Some(sec_sock) = secondary_sock
        && let Ok(Ok(fallback)) = runtime.block_on(async {
            tokio::time::timeout(
                TIMEOUT,
                crate::mcp::request(
                    sec_sock,
                    method,
                    path,
                    body_clone,
                    agent_token.as_deref(),
                    operator_token.as_deref(),
                ),
            )
            .await
        })
        && (fallback.0.is_success() || status.as_u16() == 404)
    {
        (status, bytes) = fallback;
    }
    if !status.is_success() {
        let msg = error_message(&bytes);
        return Err(if msg.is_empty() {
            format!("http {}", status.as_u16())
        } else {
            msg
        });
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
