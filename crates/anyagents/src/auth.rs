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
#[allow(dead_code)]
pub enum AuthError {
    /// Nothing was presented, or what was presented is not valid.
    Unauthenticated(String),
    /// Authenticated, and not allowed to do this.
    Forbidden(String),
    /// The decision itself failed; nothing was decided about the caller.
    Internal(String),
}

impl AuthError {
    pub fn message(&self) -> &str {
        match self {
            Self::Unauthenticated(text) => text,
            Self::Forbidden(text) => text,
            Self::Internal(text) => text,
        }
    }

    pub fn status(&self) -> axum::http::StatusCode {
        match self {
            Self::Unauthenticated(_) => axum::http::StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => axum::http::StatusCode::FORBIDDEN,
            Self::Internal(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
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
    let left = std::fs::canonicalize(left).or_else(|_| std::path::absolute(left));
    let right = std::fs::canonicalize(right).or_else(|_| std::path::absolute(right));
    matches!((left.ok(), right.ok()), (Some(a), Some(b)) if a == b)
}

impl Mode {
    /// This process's mode, from its environment and the data directory it
    /// serves.
    pub fn from_env(data: &std::path::Path) -> Self {
        if let Some(token) = operator::operator_token(data) {
            return Self::Confined {
                operator_digest: Some(operator_digest(&token)),
            };
        }
        if crate::server::serverless::is_serverless_mode()
            || crate::server::admin_auth::is_oidc_configured()
        {
            return Self::Confined {
                operator_digest: None,
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

    /// Whether an operator token digest is configured for confined API auth.
    pub fn has_operator(&self) -> bool {
        matches!(
            self,
            Self::Confined {
                operator_digest: Some(_)
            }
        )
    }

    /// Whether this deployment trusts its own machine's agent route.
    pub fn is_trusted(&self) -> bool {
        matches!(self, Self::Trusted { .. })
    }

    /// Decide who is calling, before any route runs, from what the request
    /// presented. A capability or a route name is resolved in the store, so the
    /// extractor runs this on the blocking pool; only the operator token, a pure
    /// digest comparison, is decided in place.
    pub fn principal(&self, store: &Store, presented: &Presented) -> Result<Principal, AuthError> {
        if let Some(decided) = self.principal_without_store(presented) {
            return decided;
        }
        if let Some(token) = presented.agent_token.as_deref() {
            if token.starts_with(anyagents_core::REMOTE_SESSION_TOKEN_PREFIX) {
                if !presented.allows_remote_session {
                    return Err(AuthError::Unauthenticated(
                        "the session is not authenticated".into(),
                    ));
                }
                return agent_for_capability(store, token);
            }
            // A machine without the boundary knows no capabilities: its route
            // is the agent the tool server reads from its own config directory,
            // which the server resolves to an existing agent and nothing else.
            if self.is_trusted()
                && !crate::server::serverless::is_serverless_mode()
                && !crate::server::admin_auth::is_oidc_configured()
                && let Some(id) = presented.agent_id.as_deref()
            {
                return agent_named(store, id);
            }
            return agent_for_capability(store, token);
        }
        if let Some(bearer) = presented.bearer_token.as_deref()
            && anyagents_core::is_remote_session_token(bearer)
        {
            if !presented.allows_remote_session {
                return Err(AuthError::Unauthenticated(
                    "the session is not authenticated".into(),
                ));
            }
            return agent_for_capability(store, bearer);
        }
        match self {
            Self::Trusted { data_dir }
                if same_dir(data_dir, store.data_dir())
                    && !crate::server::serverless::is_serverless_mode()
                    && !crate::server::admin_auth::is_oidc_configured() =>
            {
                match presented
                    .route_agent
                    .as_deref()
                    .or(presented.agent_id.as_deref())
                {
                    Some(route) => agent_named(store, route),
                    None if presented.admin_csrf => Ok(Principal::Operator),
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

    /// The answer when it needs no store and no file: two authorities, the
    /// operator token checked against its digest, or a stateless HMAC-verified
    /// admin OIDC session cookie. `None` when the store decides.
    fn principal_without_store(
        &self,
        presented: &Presented,
    ) -> Option<Result<Principal, AuthError>> {
        // One request carries one authority, never two.
        let authorities = usize::from(presented.agent_token.is_some())
            + usize::from(presented.operator_token.is_some())
            + usize::from(presented.admin_session.is_some())
            + usize::from(presented.bearer_token.is_some());
        if authorities > 1 {
            return Some(Err(AuthError::Unauthenticated(
                "one authority per call".to_string(),
            )));
        }
        if let Some(token) = presented.operator_token.as_deref() {
            let expected = match self {
                Self::Confined {
                    operator_digest: Some(expected),
                } => expected,
                _ => {
                    return Some(Err(AuthError::Unauthenticated(
                        "this server has no operator".to_string(),
                    )));
                }
            };
            if digest("anyagents-operator", token) != *expected {
                return Some(Err(AuthError::Unauthenticated(
                    "the operator token is not valid".to_string(),
                )));
            }
            return Some(Ok(Principal::Operator));
        }
        if let Some(cookie_val) = presented.admin_session.as_deref() {
            if presented.is_state_changing && !presented.admin_csrf {
                return Some(Err(AuthError::Forbidden(
                    "missing required x-anyagents-admin-csrf header".to_string(),
                )));
            }
            return match crate::server::admin_auth::verify_session_cookie_from_env(cookie_val) {
                Ok(_session) => Some(Ok(Principal::Operator)),
                Err(why) => Some(Err(AuthError::Unauthenticated(why))),
            };
        }
        None
    }
}

/// What one request presented, copied out of its head so the decision can run
/// on the blocking pool while the request's async worker serves other calls.
#[derive(Clone, Debug, Default)]
pub struct Presented {
    agent_token: Option<String>,
    operator_token: Option<String>,
    admin_session: Option<String>,
    bearer_token: Option<String>,
    admin_csrf: bool,
    is_state_changing: bool,
    allows_remote_session: bool,
    agent_id: Option<String>,
    route_agent: Option<String>,
}

/// Strict takeover tunnel session identifier validation (`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`).
/// Rejects path traversal, slashes, dots, whitespace, and non-cookie-safe characters.
pub(crate) fn is_valid_takeover_session_id(seg: &str) -> bool {
    let bytes = seg.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// Whether a request method and path are explicitly admitted for remote session
/// tokens (`aars_*`): sandbox browser-profile sync, media retrieval, and
/// outbound takeover reverse-tunnel registration only. Every other internal
/// coordinator API route refuses remote session tokens fail-closed.
fn allows_remote_session_path(method: &axum::http::Method, path: &str) -> bool {
    if path.contains("..") || path.contains('\\') {
        return false;
    }
    let is_profile_archive = path
        .strip_prefix("/v1/browser-profiles/")
        .and_then(|rest| rest.strip_suffix("/archive"))
        .is_some_and(|seg| !seg.is_empty() && !seg.contains('/'));
    let is_takeover_tunnel = path
        .strip_prefix("/v1/takeover/")
        .and_then(|rest| rest.strip_suffix("/tunnel"))
        .is_some_and(is_valid_takeover_session_id);
    match *method {
        axum::http::Method::GET => {
            path == "/v1/browser-profiles"
                || is_profile_archive
                || is_takeover_tunnel
                || path.strip_prefix("/v1/media/").is_some_and(|rest| {
                    let mut parts = rest.split('/');
                    let first = parts.next().is_some_and(|s| !s.is_empty());
                    let second = match parts.next() {
                        None => true,
                        Some(s) => !s.is_empty(),
                    };
                    first && second && parts.next().is_none()
                })
        }
        axum::http::Method::PUT => is_profile_archive,
        _ => false,
    }
}

fn bearer_header(parts: &Parts) -> Option<String> {
    let raw = header(parts, "authorization")?;
    let (scheme, token) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let trimmed = token.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let a = left.as_bytes();
    let b = right.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

impl Presented {
    pub(crate) fn is_oidc(&self) -> bool {
        self.admin_session.is_some()
            || self
                .bearer_token
                .as_deref()
                .is_some_and(|token| !anyagents_core::is_remote_session_token(token))
    }

    pub fn of(parts: &Parts) -> Self {
        let agent_token = header(parts, operator::AGENT_HEADER);
        let operator_token = header(parts, operator::HEADER);
        let admin_session = crate::server::admin_auth::extract_session_cookie(&parts.headers);
        let mut bearer_token = bearer_header(parts);

        // Backward-compatible dedup for legacy clients that send the exact same
        // agent token in both `x-anyagents-agent` and `Authorization: Bearer`.
        // Only collapse when no operator/admin authority is present; mismatched
        // or mixed authorities continue to fail closed with 401.
        if operator_token.is_none()
            && admin_session.is_none()
            && let (Some(agent), Some(bearer)) = (agent_token.as_deref(), bearer_token.as_deref())
            && constant_time_eq(agent, bearer)
        {
            bearer_token = None;
        }

        Self {
            agent_token,
            operator_token,
            admin_session,
            bearer_token,
            admin_csrf: header(parts, crate::server::admin_auth::ADMIN_CSRF_HEADER).as_deref()
                == Some("1"),
            is_state_changing: matches!(
                parts.method,
                axum::http::Method::POST
                    | axum::http::Method::PUT
                    | axum::http::Method::PATCH
                    | axum::http::Method::DELETE
            ),
            allows_remote_session: allows_remote_session_path(&parts.method, parts.uri.path()),
            agent_id: header(parts, AGENT_ID_HEADER),
            route_agent: parts
                .extensions
                .get::<RouteAgent>()
                .map(|route| route.0.clone()),
        }
    }
}

/// Run one authentication step that reads the store or a file on the blocking
/// pool, so a slow disk holds this request and never an async worker.
pub async fn off_worker<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, AuthError> + Send + 'static,
) -> Result<T, AuthRejection> {
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result.map_err(AuthRejection),
        Err(err) => Err(AuthRejection(AuthError::Internal(format!(
            "the caller could not be resolved: {err}"
        )))),
    }
}

/// The agent one capability or remote session token authenticates, read from
/// the store on every call so a rotated, expired, or revoked credential stops
/// working on the next call, whichever process committed the change.
pub fn agent_for_capability(store: &Store, token: &str) -> Result<Principal, AuthError> {
    if token.starts_with(anyagents_core::REMOTE_SESSION_TOKEN_PREFIX) {
        return agent_for_remote_session(store, token);
    }
    if let Ok(Some((agent_id, name))) = store.capability_holder(token) {
        let name = name.ok_or_else(|| AuthError::Unauthenticated("the agent is gone".into()))?;
        return Ok(Principal::Agent { agent_id, name });
    }
    #[cfg(unix)]
    if !store.holds_capability_key()
        && let Some(principal) = verify_capability_via_primary_socket(store.data_dir(), token)
    {
        return Ok(principal);
    }
    Err(AuthError::Unauthenticated(
        "the session is not authenticated".into(),
    ))
}

fn agent_for_remote_session(store: &Store, token: &str) -> Result<Principal, AuthError> {
    let unauthenticated =
        || AuthError::Unauthenticated("invalid, expired or revoked remote session token".into());
    let map_auth_err = |err: anyagents_core::Error| match err {
        anyagents_core::Error::Db(_)
        | anyagents_core::Error::Postgres(_)
        | anyagents_core::Error::Io(_) => AuthError::Internal(
            "temporarily unable to verify remote session token; retry shortly".into(),
        ),
        _ => unauthenticated(),
    };
    if !anyagents_core::is_remote_session_token(token) {
        return Err(unauthenticated());
    }
    let grant = store
        .authenticate_remote_session(token)
        .map_err(map_auth_err)?;
    let agent = store.agent(&grant.agent_id).map_err(map_auth_err)?;
    let config_dir = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
    if !crate::remote::runs_off_host_in(store, &agent, &config_dir) {
        return Err(unauthenticated());
    }
    Ok(Principal::Agent {
        agent_id: agent.id,
        name: agent.name,
    })
}

#[cfg(unix)]
fn verify_capability_via_primary_socket(data_dir: &Path, token: &str) -> Option<Principal> {
    use std::io::{Read as _, Write as _};
    use std::os::unix::net::UnixStream;
    if token.contains('\r') || token.contains('\n') {
        return None;
    }
    let sock = data_dir.join("anyagents.sock");
    let mut stream = UnixStream::connect(sock).ok()?;
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(2)));
    let req = format!(
        "GET /v1/whoami HTTP/1.1\r\nHost: localhost\r\n{}: {token}\r\nConnection: close\r\n\r\n",
        operator::AGENT_HEADER
    );
    stream.write_all(req.as_bytes()).ok()?;
    let mut resp = String::new();
    stream.read_to_string(&mut resp).ok()?;
    let (head, body) = resp.split_once("\r\n\r\n")?;
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return None;
    }
    let json_start = body.find('{')?;
    let json_end = body.rfind('}')?;
    let val: serde_json::Value = serde_json::from_str(&body[json_start..=json_end]).ok()?;
    let id_str = val.get("agent_id")?.as_str()?;
    let name = val.get("name")?.as_str()?.to_string();
    let agent_id = AgentId::parse(id_str).ok()?;
    Some(Principal::Agent { agent_id, name })
}

/// The agent a route names: a name or an id, resolved in the store.
fn agent_named(store: &Store, route: &str) -> Result<Principal, AuthError> {
    if let Ok(id) = AgentId::parse(route.trim())
        && let Ok(agent) = store.agent(&id)
    {
        return Ok(Principal::Agent {
            agent_id: agent.id,
            name: agent.name,
        });
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

/// Authenticated principal injected directly into an in-process Axum router
/// call (for example HTTP MCP after verifying a remote session token). External
/// HTTP and Unix-socket requests never carry this extension.
#[derive(Clone, Debug)]
pub(crate) struct InjectedPrincipal(pub Principal);

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
        if let Some(injected) = parts.extensions.get::<InjectedPrincipal>() {
            return Ok(injected.0.clone());
        }
        let presented = Presented::of(parts);
        if let Some(decided) = state.auth.principal_without_store(&presented) {
            return decided.map_err(AuthRejection);
        }
        if let Some(bearer) = presented.bearer_token.as_deref()
            && !anyagents_core::is_remote_session_token(bearer)
        {
            return crate::server::oidc_bearer::authenticate_bearer_token(
                state.store.data_dir(),
                bearer,
            )
            .await
            .map_err(AuthRejection);
        }
        let is_remote = presented
            .agent_token
            .as_deref()
            .or(presented.bearer_token.as_deref())
            .is_some_and(anyagents_core::is_remote_session_token);
        let auth = state.auth.clone();
        let store = std::sync::Arc::clone(&state.store);
        let principal = off_worker(move || auth.principal(&store, &presented)).await?;
        if is_remote && matches!(principal, Principal::Agent { .. }) {
            // Preserve the same host-isolation guards (`refuse_host_media_over_http`,
            // `refuse_host_placement_over_http`, no host wake/revival) on direct
            // remote-session HTTP calls as on HTTP MCP calls.
            parts.headers.insert(
                axum::http::header::HeaderName::from_static(crate::mcp::HTTP_MCP_HEADER),
                axum::http::HeaderValue::from_static("1"),
            );
        }
        Ok(principal)
    }
}

/// The rejection an unauthenticated or forbidden call gets. The message never
/// echoes what was presented.
#[derive(Debug)]
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
    fn daemon_rejects_revoked_capability_without_calling_its_own_socket() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store.hold_capability_key(&"a".repeat(64)).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("auth-reader", "test"))
            .unwrap();
        let revoked = store.issue_capability(&agent.id).unwrap();
        store.issue_capability(&agent.id).unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind(dir.path().join("anyagents.sock")).unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(agent_for_capability(&store, &revoked).is_err());
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }

    #[test]
    fn a_capability_rotated_by_another_process_stops_working_on_the_next_call() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let server = Store::open(&path).unwrap();
        server.hold_capability_key(&"a".repeat(64)).unwrap();
        let agent = server
            .create_agent(anyagents_core::NewAgent::new("rotated", "test"))
            .unwrap();
        let first = server.issue_capability(&agent.id).unwrap();
        assert_eq!(
            agent_for_capability(&server, &first).unwrap(),
            Principal::Agent {
                agent_id: agent.id.clone(),
                name: "rotated".into(),
            }
        );
        // Another process holding the key mints a new capability and later
        // retires every one; the server sees each change on its next call.
        let other = Store::open(&path).unwrap();
        other.hold_capability_key(&"a".repeat(64)).unwrap();
        let second = other.issue_capability(&agent.id).unwrap();
        assert!(agent_for_capability(&server, &first).is_err());
        other.rename_agent(&agent.id, "renamed").unwrap();
        assert_eq!(
            agent_for_capability(&server, &second).unwrap(),
            Principal::Agent {
                agent_id: agent.id.clone(),
                name: "renamed".into(),
            }
        );
        other.retire_capabilities(&agent.id).unwrap();
        assert!(agent_for_capability(&server, &second).is_err());
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

    struct EnvRestore(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl EnvRestore {
        fn capture(keys: &[&'static str]) -> Self {
            Self(keys.iter().map(|&k| (k, std::env::var_os(k))).collect())
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (k, v) in &self.0 {
                match v {
                    Some(val) => unsafe { std::env::set_var(k, val) },
                    None => unsafe { std::env::remove_var(k) },
                }
            }
        }
    }

    #[test]
    fn admin_session_cookie_auth_enforces_csrf_single_authority_and_allowlist() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_ADMIN_OIDC_ISSUER",
            "ANYAGENTS_ADMIN_OIDC_CLIENT_ID",
            "ANYAGENTS_ADMIN_EMAILS",
            "ANYAGENTS_ADMIN_SESSION_SECRET",
            "ANYAGENTS_MASTER_KEY",
            "ANYAGENTS_OPERATOR_TOKEN",
            "ANYAGENTS_SERVERLESS",
        ]);

        unsafe {
            std::env::set_var("ANYAGENTS_ADMIN_OIDC_ISSUER", "https://idp.example.com");
            std::env::set_var("ANYAGENTS_ADMIN_OIDC_CLIENT_ID", "console-client");
            std::env::set_var("ANYAGENTS_ADMIN_EMAILS", "admin@example.com");
            std::env::set_var(
                "ANYAGENTS_ADMIN_SESSION_SECRET",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            );
            std::env::set_var("ANYAGENTS_SERVERLESS", "true");
        }

        let key = crate::server::admin_auth::session_signing_key_from_env().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let cookie_val = crate::server::admin_auth::sign_admin_session(
            &key,
            &crate::server::admin_auth::AdminSession {
                email: "admin@example.com".to_string(),
                name: Some("Admin".to_string()),
                sub: "sub-admin".to_string(),
                iat: now,
                exp: now + 3600,
            },
        )
        .unwrap();
        let cookie_hdr = format!(
            "{}={cookie_val}",
            crate::server::admin_auth::ADMIN_SESSION_COOKIE
        );
        let mode = Mode::Confined {
            operator_digest: Some(operator_digest("op-secret-0123456789")),
        };

        // 1. GET with valid admin cookie authenticates as Principal::Operator.
        let get_req = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/agents")
            .header(axum::http::header::COOKIE, &cookie_hdr)
            .body(())
            .unwrap();
        let (get_parts, ()) = get_req.into_parts();
        let get_presented = Presented::of(&get_parts);
        assert_eq!(
            mode.principal_without_store(&get_presented)
                .unwrap()
                .unwrap(),
            Principal::Operator
        );

        // 2. POST with valid admin cookie + CSRF header authenticates as Principal::Operator.
        let post_ok_req = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/agents")
            .header(axum::http::header::COOKIE, &cookie_hdr)
            .header(crate::server::admin_auth::ADMIN_CSRF_HEADER, "1")
            .body(())
            .unwrap();
        let (post_ok_parts, ()) = post_ok_req.into_parts();
        let post_ok_presented = Presented::of(&post_ok_parts);
        assert_eq!(
            mode.principal_without_store(&post_ok_presented)
                .unwrap()
                .unwrap(),
            Principal::Operator
        );

        // 3. POST/PUT/PATCH/DELETE with valid admin cookie WITHOUT CSRF header is rejected (403).
        for method in ["POST", "PUT", "PATCH", "DELETE"] {
            let mut_req = axum::http::Request::builder()
                .method(method)
                .uri("/v1/agents")
                .header(axum::http::header::COOKIE, &cookie_hdr)
                .body(())
                .unwrap();
            let (mut_parts, ()) = mut_req.into_parts();
            let mut_presented = Presented::of(&mut_parts);
            let err = mode
                .principal_without_store(&mut_presented)
                .unwrap()
                .unwrap_err();
            assert_eq!(err.status(), axum::http::StatusCode::FORBIDDEN, "{method}");
        }

        // 4. Dual authority (agent capability + admin cookie, or operator token + admin cookie) fails closed.
        let dual_agent = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/agents")
            .header(axum::http::header::COOKIE, &cookie_hdr)
            .header(operator::AGENT_HEADER, "agent-cap-token")
            .body(())
            .unwrap();
        let (dual_agent_parts, ()) = dual_agent.into_parts();
        let err = mode
            .principal_without_store(&Presented::of(&dual_agent_parts))
            .unwrap()
            .unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(err.message(), "one authority per call");

        let dual_op = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/agents")
            .header(axum::http::header::COOKIE, &cookie_hdr)
            .header(operator::HEADER, "op-secret-0123456789")
            .body(())
            .unwrap();
        let (dual_op_parts, ()) = dual_op.into_parts();
        let err = mode
            .principal_without_store(&Presented::of(&dual_op_parts))
            .unwrap()
            .unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(err.message(), "one authority per call");

        // 5. Revoking email from ANYAGENTS_ADMIN_EMAILS immediately invalidates the cookie.
        unsafe {
            std::env::set_var("ANYAGENTS_ADMIN_EMAILS", "other@example.com");
        }
        assert!(
            mode.principal_without_store(&get_presented)
                .unwrap()
                .is_err()
        );

        // 6. Unconfigured OIDC fails closed even with a previously signed cookie.
        unsafe {
            std::env::remove_var("ANYAGENTS_ADMIN_OIDC_ISSUER");
        }
        assert!(
            mode.principal_without_store(&get_presented)
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn remote_session_token_auth_dedup_and_fail_closed_boundaries() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_SANDBOX_MODE",
            "ANYAGENTS_SANDBOX_PROVIDER",
            "ANYAGENTS_SANDBOX_ENDPOINT",
            "ANYAGENTS_SANDBOX_API_KEY",
            "E2B_API_KEY",
            "MODAL_TOKEN_ID",
            "DAYTONA_API_KEY",
            "SPRITES_TOKEN",
            "ANYAGENTS_ADMIN_OIDC_ISSUER",
            "ANYAGENTS_ADMIN_OIDC_CLIENT_ID",
            "ANYAGENTS_ADMIN_EMAILS",
            "ANYAGENTS_ADMIN_SESSION_SECRET",
            "ANYAGENTS_OPERATOR_TOKEN",
        ]);
        for key in [
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_SANDBOX_MODE",
            "ANYAGENTS_SANDBOX_PROVIDER",
            "ANYAGENTS_SANDBOX_ENDPOINT",
            "ANYAGENTS_SANDBOX_API_KEY",
            "E2B_API_KEY",
            "MODAL_TOKEN_ID",
            "DAYTONA_API_KEY",
            "SPRITES_TOKEN",
            "ANYAGENTS_ADMIN_OIDC_ISSUER",
            "ANYAGENTS_ADMIN_OIDC_CLIENT_ID",
            "ANYAGENTS_ADMIN_EMAILS",
            "ANYAGENTS_ADMIN_SESSION_SECRET",
        ] {
            unsafe {
                std::env::remove_var(key);
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let store = std::sync::Arc::new(Store::open(&db_path).unwrap());
        let off_host = store
            .create_agent(
                anyagents_core::NewAgent::new("sandbox-agent", "claude-code")
                    .with_runtime(anyagents_core::ExecutionRuntime::Sandbox { provider: None }),
            )
            .unwrap();
        let local_agent = store
            .create_agent(
                anyagents_core::NewAgent::new("local-agent", "claude-code")
                    .with_runtime(anyagents_core::ExecutionRuntime::Local),
            )
            .unwrap();
        let off_host_session = store.begin_remote_session(&off_host.id, 60_000).unwrap();
        let local_session = store.begin_remote_session(&local_agent.id, 60_000).unwrap();
        let tok = off_host_session.token;
        let local_tok = local_session.token;

        struct NoopRunner;

        impl anyagents_installer::CommandRunner for NoopRunner {
            fn run(
                &mut self,
                _program: &str,
                _args: &[String],
                _dir: &Path,
                _env: &[(&str, &str)],
                _on_line: &mut dyn anyagents_installer::LineSink,
            ) -> Result<(), anyagents_installer::InstallError> {
                Ok(())
            }
        }

        let expected = Principal::Agent {
            agent_id: off_host.id.clone(),
            name: off_host.name.clone(),
        };
        let state = AppState {
            store: std::sync::Arc::clone(&store),
            health: anyagents_supervisor::HealthBoard::default(),
            sign_in: std::sync::Arc::new(std::sync::Mutex::new(NoopRunner)),
            auth: Mode::Confined {
                operator_digest: Some(operator_digest("op-secret-0123456789")),
            },
            identity: std::sync::Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend: std::sync::Arc::new(crate::timer::NoopTimerBackend),
        };

        // 1. Single `x-anyagents-agent` remote session token authenticates and sets host-isolation header.
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(operator::AGENT_HEADER, &tok)
            .body(())
            .unwrap()
            .into_parts();
        let p = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap();
        assert_eq!(p, expected);
        assert!(!p.is_operator());
        assert_eq!(
            parts
                .headers
                .get(crate::mcp::HTTP_MCP_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );

        // 2. Legacy matching `x-anyagents-agent` + `Authorization: Bearer` deduplicates to single authority.
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(operator::AGENT_HEADER, &tok)
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {tok}"))
            .body(())
            .unwrap()
            .into_parts();
        let p = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap();
        assert_eq!(p, expected);
        assert_eq!(
            parts
                .headers
                .get(crate::mcp::HTTP_MCP_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );

        // 3. Pure `Authorization: Bearer <remote-token>` routes to remote session auth (not OIDC).
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {tok}"))
            .body(())
            .unwrap()
            .into_parts();
        let p = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap();
        assert_eq!(p, expected);
        assert_eq!(
            parts
                .headers
                .get(crate::mcp::HTTP_MCP_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );

        // 4. Distinct dual authority (`x-anyagents-agent: A` + `Authorization: Bearer B`) fails closed (401).
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(operator::AGENT_HEADER, &tok)
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {local_tok}"),
            )
            .body(())
            .unwrap()
            .into_parts();
        let err = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap_err();
        assert_eq!(err.0.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(err.0.message(), "one authority per call");

        // 5. Matching agent+bearer combined with operator token or admin cookie still fails closed (401).
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(operator::AGENT_HEADER, &tok)
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {tok}"))
            .header(operator::HEADER, "op-secret-0123456789")
            .body(())
            .unwrap()
            .into_parts();
        let err = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap_err();
        assert_eq!(err.0.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(err.0.message(), "one authority per call");

        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(operator::AGENT_HEADER, &tok)
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {tok}"))
            .header(
                axum::http::header::COOKIE,
                format!(
                    "{}=fake-cookie",
                    crate::server::admin_auth::ADMIN_SESSION_COOKIE
                ),
            )
            .body(())
            .unwrap()
            .into_parts();
        let err = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap_err();
        assert_eq!(err.0.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(err.0.message(), "one authority per call");

        // 6. Non-off-host (local) agent remote session token fails closed (401).
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(operator::AGENT_HEADER, &local_tok)
            .body(())
            .unwrap()
            .into_parts();
        let err = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap_err();
        assert_eq!(err.0.status(), axum::http::StatusCode::UNAUTHORIZED);

        // 7. Malformed `aars_*` token fails closed (401) and non-remote Bearer still hits OIDC (401).
        for bad_hdr in [
            (operator::AGENT_HEADER, "aars_not_hex_or_short"),
            ("authorization", "Bearer not-a-remote-token"),
        ] {
            let (mut parts, ()) = axum::http::Request::builder()
                .method("GET")
                .uri("/v1/browser-profiles")
                .header(bad_hdr.0, bad_hdr.1)
                .body(())
                .unwrap()
                .into_parts();
            let err = Principal::from_request_parts(&mut parts, &state)
                .await
                .unwrap_err();
            assert_eq!(err.0.status(), axum::http::StatusCode::UNAUTHORIZED);
        }

        // 8. Takeover reverse-tunnel (`GET /v1/takeover/{session_id}/tunnel`) admits valid
        //    session IDs and rejects traversal, nested paths, and non-GET methods fail-closed.
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/takeover/tk_0123456789abcdef/tunnel")
            .header(operator::AGENT_HEADER, &tok)
            .body(())
            .unwrap()
            .into_parts();
        let p = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap();
        assert_eq!(p, expected);

        // 9. Internal API routes (outside browser-profiles, media, and takeover tunnel) refuse
        //    remote session tokens (401).
        for (method, uri) in [
            ("GET", "/"),
            ("GET", "/v1/agents"),
            ("POST", "/v1/send"),
            ("GET", "/v1/secrets"),
            ("DELETE", "/v1/browser-profiles/team-site"),
            ("PUT", "/v1/agents/sandbox-agent/browser-profiles"),
            ("GET", "/v1/browser-profiles/../secrets"),
            ("POST", "/v1/takeover/tk_0123456789abcdef/tunnel"),
            ("PUT", "/v1/takeover/tk_0123456789abcdef/tunnel"),
            ("DELETE", "/v1/takeover/tk_0123456789abcdef/tunnel"),
            ("GET", "/v1/takeover/../secrets/tunnel"),
            ("GET", "/v1/takeover/a/b/tunnel"),
            ("GET", "/v1/takeover//tunnel"),
            ("GET", "/v1/takeover/a%2Fb/tunnel"),
            ("GET", "/v1/takeover/tk_0123456789abcdef/other"),
        ] {
            let (mut parts, ()) = axum::http::Request::builder()
                .method(method)
                .uri(uri)
                .header(operator::AGENT_HEADER, &tok)
                .body(())
                .unwrap()
                .into_parts();
            let err = Principal::from_request_parts(&mut parts, &state)
                .await
                .unwrap_err();
            assert_eq!(
                err.0.status(),
                axum::http::StatusCode::UNAUTHORIZED,
                "{method} {uri}"
            );
        }

        // 9. Even in Mode::Trusted, a remote session token ignores spoofed `x-anyagents-agent-id`.
        let trusted = Mode::Trusted {
            data_dir: dir.path().to_path_buf(),
        };
        let (parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(operator::AGENT_HEADER, &tok)
            .header(AGENT_ID_HEADER, local_agent.id.as_str())
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(
            trusted.principal(&store, &Presented::of(&parts)).unwrap(),
            expected
        );

        // 10. Revoking the remote session immediately invalidates it (401).
        store.revoke_remote_sessions(&off_host.id, None).unwrap();
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(operator::AGENT_HEADER, &tok)
            .body(())
            .unwrap()
            .into_parts();
        let err = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap_err();
        assert_eq!(err.0.status(), axum::http::StatusCode::UNAUTHORIZED);

        // 10. Expired remote session token immediately fails closed (401).
        let fresh = store.begin_remote_session(&off_host.id, 60_000).unwrap();
        rusqlite::Connection::open(&db_path)
            .unwrap()
            .execute(
                "UPDATE remote_session_tokens SET expires_at_ms = 1 WHERE session_id = ?1",
                [fresh.session_id.as_str()],
            )
            .unwrap();
        let (mut parts, ()) = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/browser-profiles")
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {}", fresh.token),
            )
            .body(())
            .unwrap()
            .into_parts();
        let err = Principal::from_request_parts(&mut parts, &state)
            .await
            .unwrap_err();
        assert_eq!(err.0.status(), axum::http::StatusCode::UNAUTHORIZED);
    }
}
