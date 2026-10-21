//! Which principal made one API call, decided before any route runs.
//!
//! Two authorities exist and never stand in for each other.
//!
//! * **The operator** administers the deployment: creates agents, signs a
//!   harness in, sets a channel secret, mints an agent's capability. It presents
//!   a token the server keeps only a digest of, and only a process whose real
//!   user is the server's own user is read as the operator.
//! * **An agent instance** acts as exactly one agent, through the mailbox
//!   capability the server minted for it. The agent id comes from the stored
//!   capability, never from a name, an environment, a path or a session id the
//!   caller wrote. A replaced session's capability stops working at once.
//!
//! One deployment runs without that boundary: a server started with no operator
//! token and no agent route is a single-user machine, and there an
//! unauthenticated call acts as the agent its request names. The server says so
//! once at start.

use std::path::Path;

use anyagents_core::{AgentId, Store};
use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::operator;
use crate::server::AppState;

/// Who is calling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    /// The deployment's operator.
    Operator,
    /// One agent instance, authenticated for exactly this agent.
    Agent { agent_id: AgentId, name: String },
}

impl Principal {
    pub fn is_operator(&self) -> bool {
        matches!(self, Self::Operator)
    }
}

/// A call that is authenticated but not allowed to do this.
#[derive(Debug)]
pub enum AuthError {
    /// Nothing was presented, or what was presented is not valid.
    Unauthenticated(String),
    /// Authenticated, and not allowed to do this.
    Forbidden(String),
}

impl AuthError {
    pub fn message(&self) -> &str {
        match self {
            Self::Unauthenticated(text) => text,
            Self::Forbidden(text) => text,
        }
    }

    pub fn status(&self) -> axum::http::StatusCode {
        match self {
            Self::Unauthenticated(_) => axum::http::StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => axum::http::StatusCode::FORBIDDEN,
        }
    }
}

/// How this server authenticates.
#[derive(Clone)]
pub enum Mode {
    /// The identity boundary: a capability, or the operator token.
    Confined { operator_digest: Option<String> },
    /// The single-user machine, for this data directory only.
    Trusted { data_dir: std::path::PathBuf },
}

/// A one-way digest under a fixed server-side label. The operator token has
/// enough entropy that this is enough to keep it out of every file.
fn digest(label: &str, token: &str) -> String {
    let mut bytes = Vec::with_capacity(label.len() + token.len() + 1);
    bytes.extend_from_slice(label.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(token.as_bytes());
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in &bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

/// The digest a presented operator token must match.
pub fn operator_digest(token: &str) -> String {
    digest("anyagents-operator", token)
}

fn same_dir(left: &Path, right: &Path) -> bool {
    matches!(
        (std::path::absolute(left).ok(), std::path::absolute(right).ok()),
        (Some(a), Some(b)) if a == b
    )
}

impl Mode {
    /// This process's mode, from its environment.
    pub fn from_env() -> Self {
        if let Some(token) = operator::operator_token() {
            return Self::Confined {
                operator_digest: Some(operator_digest(&token)),
            };
        }
        match std::env::var_os("ANYAGENTS_AGENT_ROUTE") {
            Some(route) if !route.is_empty() => Self::Trusted {
                data_dir: std::path::PathBuf::from(route),
            },
            _ => Self::Confined {
                operator_digest: None,
            },
        }
    }

    pub fn is_confined(&self) -> bool {
        matches!(self, Self::Confined { .. })
    }

    /// Whether this deployment trusts its own machine's agent route.
    pub fn is_trusted(&self) -> bool {
        matches!(self, Self::Trusted { .. })
    }

    /// Decide who is calling, before any route runs.
    pub fn principal(&self, store: &Store, parts: &Parts) -> Result<Principal, AuthError> {
        let agent_token = header(parts, operator::AGENT_HEADER);
        let operator_token = header(parts, operator::HEADER);
        // One request carries one authority, never two.
        if agent_token.is_some() && operator_token.is_some() {
            return Err(AuthError::Unauthenticated(
                "one authority per call".to_string(),
            ));
        }
        if let Some(token) = operator_token {
            let expected = match self {
                Self::Confined {
                    operator_digest: Some(expected),
                } => expected,
                _ => {
                    return Err(AuthError::Unauthenticated(
                        "this server has no operator".to_string(),
                    ));
                }
            };
            if digest("anyagents-operator", &token) != *expected {
                return Err(AuthError::Unauthenticated(
                    "the operator token is not valid".to_string(),
                ));
            }
            return Ok(Principal::Operator);
        }
        if let Some(token) = agent_token {
            // A machine without the boundary knows no capabilities: its route
            // is the agent the tool server reads from its own config directory,
            // which the server resolves to an existing agent and nothing else.
            if self.is_trusted()
                && let Some(id) = header(parts, AGENT_ID_HEADER)
            {
                return agent_named(store, &id);
            }
            return agent_for_capability(store, &token);
        }
        match self {
            Self::Trusted { data_dir } if same_dir(data_dir, store.data_dir()) => {
                let named = parts
                    .extensions
                    .get::<RouteAgent>()
                    .map(|route| route.0.clone())
                    .or_else(|| header(parts, AGENT_ID_HEADER));
                match named {
                    Some(route) => agent_named(store, &route),
                    None => Err(AuthError::Unauthenticated(
                        "this call names no agent".to_string(),
                    )),
                }
            }
            _ => Err(AuthError::Unauthenticated(
                "the session is not authenticated".to_string(),
            )),
        }
    }
}

/// The agent one capability authenticates, read from the store.
pub fn agent_for_capability(store: &Store, token: &str) -> Result<Principal, AuthError> {
    let agent_id = store
        .authenticate_capability(token)
        .map_err(|_| AuthError::Unauthenticated("the session is not authenticated".into()))?;
    let name = store
        .agent(&agent_id)
        .map_err(|_| AuthError::Unauthenticated("the agent is gone".into()))?
        .name;
    Ok(Principal::Agent { agent_id, name })
}

/// The agent a route names: a name or an id, resolved in the store.
fn agent_named(store: &Store, route: &str) -> Result<Principal, AuthError> {
    if let Ok(id) = AgentId::parse(route.trim()) {
        if let Ok(agent) = store.agent(&id) {
            return Ok(Principal::Agent {
                agent_id: agent.id,
                name: agent.name,
            });
        }
    }
    agent_by_name(store, route)
}

/// The agent a name resolves to, read from the store.
pub fn agent_by_name(store: &Store, name: &str) -> Result<Principal, AuthError> {
    let agent = store
        .agent_by_name(name)
        .map_err(|_| AuthError::Unauthenticated(format!("agent not found: {name}")))?;
    Ok(Principal::Agent {
        agent_id: agent.id,
        name: agent.name,
    })
}

/// The agent id a tool server was given at start. On a deployment without the
/// boundary it names the one route; with the boundary it is a no-op header,
/// and only the operator token may follow it.
pub const AGENT_ID_HEADER: &str = "x-anyagents-agent-id";

/// The agent name a request's body or path names, put in place by the route
/// layer so the single-user route can find it. On the identity boundary it is
/// ignored.
#[derive(Clone, Debug)]
pub struct RouteAgent(pub String);

fn header(parts: &Parts, name: &'static str) -> Option<String> {
    parts
        .headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

impl FromRequestParts<AppState> for Principal {
    type Rejection = AuthRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        state
            .auth
            .principal(&state.store, parts)
            .map_err(AuthRejection)
    }
}

/// The rejection an unauthenticated or forbidden call gets. The message never
/// echoes what was presented.
pub struct AuthRejection(pub AuthError);

impl axum::response::IntoResponse for AuthRejection {
    fn into_response(self) -> axum::response::Response {
        let body = axum::Json(serde_json::json!({ "error": self.0.message() }));
        (self.0.status(), body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_and_its_digest_differ_and_a_different_token_does_not_match() {
        let digest = operator_digest("0123456789abcdef0123456789abcdef");
        assert!(!digest.contains("0123456789abcdef0123456789abcdef"));
        assert_eq!(digest.len(), 16);
        assert_ne!(digest, operator_digest("0123456789abcdef0123456789abcdee"));
    }

    #[test]
    fn the_mode_never_trusts_a_directory_it_was_not_given() {
        let mode = Mode::Trusted {
            data_dir: std::path::PathBuf::from("/tmp/one"),
        };
        assert!(!mode.is_confined());
        assert!(!same_dir(Path::new("/tmp/one/"), Path::new("/tmp/two")));
        assert!(same_dir(Path::new("/tmp/one"), Path::new("/tmp/./one")));
    }
}
