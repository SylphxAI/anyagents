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

/// How long the server may take to answer one call once it is connected (a
/// wait for a restarting server is not counted). Creating an agent installs
/// a harness, which can take a minute; everything else answers at once.
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

/// This command goes through the server. An operator's does when the server
/// is up; otherwise it uses the store it owns. An authenticated agent's always
/// does: it has no store of its own, and a socket missing for a moment is a
/// server restarting, which its call waits out ([`restart_wait`]) before it
/// refuses; an agent process with neither socket nor capability was never
/// admitted by a server and falls through to each command's offline refusal.
pub(crate) fn can_call_server(data: &Path) -> bool {
    match authority(data) {
        Authority::Agent(id) => {
            server::socket_path(data).exists() || capability(data, &id).is_some()
        }
        Authority::Operator => server::socket_path(data).exists(),
        Authority::None => false,
    }
}

/// A command sending through the server whatever its authority: the server's
/// socket is there, or it runs as an agent (see [`can_call_server`]).
pub(crate) fn sends_through_server(data: &Path) -> bool {
    server::socket_path(data).exists() || matches!(authority(data), Authority::Agent(_))
}

/// The agent whose config directory this process runs with, from the interface
/// the deployment exports to an agent's harness. An id, never a name.
pub(crate) fn agent_identity() -> Option<AgentId> {
    let raw = std::env::var("ANYAGENTS_AGENT_ID").ok()?;
    AgentId::parse(raw.trim()).ok()
}

/// Read through the authenticated server when available. Only a host operator
/// may use the caller's offline read path after a transport failure; HTTP and
/// decoding errors must remain errors, never an authorization bypass.
pub(crate) fn get<T: serde::de::DeserializeOwned>(
    data: &Path,
    path: &str,
) -> Result<Option<T>, String> {
    if !can_call_server(data) {
        return Ok(None);
    }
    let authority = authority(data);
    match call_inner(data, "GET", path, None, &authority) {
        Err(err) if may_read_offline(&err, agent_identity().is_some()) => Ok(None),
        // The agent waited and no server came back (its socket is still
        // gone): answer as when there is none, so each command gives its own
        // refusal ("... requires the authenticated server") rather than a
        // transport error.
        Err(CallError::Absent(_)) => Ok(None),
        result => result.map_err(CallError::message),
    }
}

enum CallError {
    Transport(String),
    /// An agent's call found no server, even after waiting for one: nothing
    /// was sent.
    Absent(String),
    Response(String),
}

impl CallError {
    fn message(self) -> String {
        match self {
            Self::Transport(message) | Self::Absent(message) | Self::Response(message) => message,
        }
    }
}

fn may_read_offline(error: &CallError, is_agent: bool) -> bool {
    !is_agent && matches!(error, CallError::Transport(_))
}

/// How long one command waits for a server that is restarting (its socket
/// missing or refusing, so nothing was sent). An operator's read does not
/// wait: it is answered from the store at once. Everything else (an agent's
/// command, any change) waits the restart out.
///
/// `ANYAGENTS_RECONNECT_WAIT_MS` replaces the wait (an operator's choice, or
/// a test of something other than restarts), up to ten minutes.
fn restart_wait(
    method: &str,
    authority: &Authority,
    has_agent_capability: bool,
) -> std::time::Duration {
    if method.eq_ignore_ascii_case("GET") && matches!(authority, Authority::Operator) {
        return std::time::Duration::ZERO;
    }
    if matches!(authority, Authority::Agent(_)) && !has_agent_capability {
        return std::time::Duration::ZERO;
    }
    std::env::var("ANYAGENTS_RECONNECT_WAIT_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|ms| std::time::Duration::from_millis(ms).min(std::time::Duration::from_secs(600)))
        .unwrap_or(crate::mcp::RECONNECT_WITHIN)
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
    call_inner(data, method, path, body, authority).map_err(CallError::message)
}

fn call_inner<T: serde::de::DeserializeOwned>(
    data: &Path,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    authority: &Authority,
) -> Result<Option<T>, CallError> {
    let (agent_token, operator_token) = match authority {
        Authority::Operator => (None, crate::operator::operator_token(data)),
        Authority::Agent(id) => (capability(data, id), None),
        Authority::None => (None, None),
    };
    // A boundary refuses a call that has no credential rather than answering
    // with nothing, so a client notices instead of reading an empty deployment.
    let socket = server::socket_path(data);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| CallError::Response(err.to_string()))?;
    let within = restart_wait(method, authority, agent_token.is_some());
    let (status, bytes) = runtime
        .block_on(crate::mcp::request_waiting(
            &socket,
            method,
            path,
            body,
            agent_token.as_deref(),
            operator_token.as_deref(),
            within,
            TIMEOUT,
        ))
        .map_err(|error| {
            if matches!(authority, Authority::Agent(_)) && !socket.exists() {
                CallError::Absent(format!(
                    "this command requires the authenticated server, and none came back within \
                     {}s ({error})",
                    within.as_secs()
                ))
            } else {
                CallError::Transport(error)
            }
        })?;
    if !status.is_success() {
        let msg = error_message(&bytes);
        return Err(CallError::Response(if msg.is_empty() {
            format!("http {}", status.as_u16())
        } else {
            msg
        }));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|err| CallError::Response(err.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_reads_require_operator_and_transport_failure() {
        for error in [
            "the server did not answer",
            "cannot reach the server: Connection refused (os error 111)",
            "cannot reach the server: connection closed before message completed",
            "cannot reach the server: error reading a body from connection",
        ] {
            let failure = CallError::Transport(error.into());
            assert!(may_read_offline(&failure, false), "{error}");
            assert!(!may_read_offline(&failure, true), "{error}");
        }
        for error in [
            "forbidden",
            "http 403",
            "primary refused",
            "invalid JSON",
            "the server did not answer",
            "cannot reach the server: forbidden",
        ] {
            let failure = CallError::Response(error.into());
            assert!(!may_read_offline(&failure, false), "{error}");
            assert!(!may_read_offline(&failure, true), "{error}");
        }
    }
}
