//! Shared server state, error responses, request guards, and placement helpers.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyagents_core::{Agent, Error, Store};
use anyagents_installer::{CommandRunner, InstallError, LineSink};
use anyagents_supervisor::HealthBoard;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::api::SendRequest;
use crate::auth::{self, Principal};
use crate::identity::ServingIdentity;
use crate::vendor;

use super::{in_flight, profiles};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<Store>,
    pub(crate) health: HealthBoard,
    pub(crate) sign_in: Arc<Mutex<dyn CommandRunner + Send>>,
    pub(crate) auth: auth::Mode,
    pub(crate) identity: Arc<ServingIdentity>,
    /// The last provider-profile listing, refreshed in the background.
    pub(crate) profiles: Arc<profiles::ProfileSnapshots>,
    /// Event-driven timer backend (local Tokio timers or external webhook/delayed-run scheduler).
    pub(crate) timer_backend: Arc<dyn crate::timer::TimerBackend>,
}

/// Resolve the logical workspace directory inside a remote sandbox VM.
///
/// NEVER returns a coordinator-host path (such as `<data>/agents/<id>/launcher`
/// or `<data>/agents/<id>/work`). Prefers:
/// 1. Persisted `agent_sandbox_bindings.remote_workdir`
/// 2. Explicit `ANYAGENTS_SANDBOX_WORKDIR` / `ANYAGENTS_REMOTE_WORKDIR` in `agent.launch_env`
/// 3. Process env `ANYAGENTS_SANDBOX_WORKDIR` (when host storage env is active)
/// 4. Default `/home/user/agent-data/workspace`.
pub(crate) fn resolve_remote_sandbox_workdir(store: &Store, agent: &Agent) -> String {
    let is_host_local = |raw: &str| -> bool {
        let trimmed = raw.trim();
        if trimmed.is_empty() || !trimmed.starts_with('/') {
            return true;
        }
        let path = Path::new(trimmed);
        if trimmed.ends_with("/launcher")
            || trimmed.contains("/agents/")
            || trimmed.starts_with("/var/lib/anyagents")
            || trimmed.starts_with("/tmp/")
            || trimmed == "/tmp"
        {
            return true;
        }
        let data_dir = store.data_dir();
        if !data_dir.as_os_str().is_empty() && path.starts_with(data_dir) {
            return true;
        }
        if let Some(ws) = anyagents_core::configured_workspaces_dir()
            && path.starts_with(ws)
        {
            return true;
        }
        let temp_dir = std::env::temp_dir();
        !temp_dir.as_os_str().is_empty() && path.starts_with(&temp_dir)
    };

    if let Ok(Some(binding)) = store.sandbox_binding(&agent.id)
        && !is_host_local(&binding.remote_workdir)
    {
        return binding.remote_workdir;
    }
    for key in ["ANYAGENTS_SANDBOX_WORKDIR", "ANYAGENTS_REMOTE_WORKDIR"] {
        if let Some((_, val)) = agent.launch_env.iter().rev().find(|(k, _)| k == key)
            && !is_host_local(val)
        {
            return val.trim().to_string();
        }
    }
    if anyagents_core::storage_env_enabled()
        && let Ok(val) = std::env::var("ANYAGENTS_SANDBOX_WORKDIR")
        && !is_host_local(&val)
    {
        return val.trim().to_string();
    }
    anyagents_core::DEFAULT_REMOTE_SANDBOX_WORKDIR.to_string()
}

/// Only the operator administers the deployment.
pub(super) fn require_operator(principal: &Principal) -> Result<(), ApiError> {
    if principal.is_operator() {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "an agent session cannot administer the server",
        ))
    }
}

/// Only the deployment operator holds cross-tree administrative authority.
pub(super) fn require_operator_audited(
    store: &Store,
    principal: &Principal,
    action: &str,
    target: &str,
) -> Result<String, ApiError> {
    if !principal.is_operator() {
        return Err(ApiError::forbidden("administrative authority required"));
    }
    store
        .record_admin_audit("operator", action, target)
        .map_err(ApiError::from_store)?;
    Ok("operator".to_string())
}

/// Whether this call was made by the tool server behind the HTTP MCP route.
/// That tool server runs inside the server process as the server's own user,
/// so a call it makes may not start or read anything on the host.
pub(super) fn via_http_mcp(headers: &axum::http::HeaderMap) -> bool {
    headers.contains_key(crate::mcp::HTTP_MCP_HEADER)
}

/// Whether an agent's harness runs off this host (including PostgreSQL sandbox bindings).
pub(super) fn runs_off_host(store: &Store, agent: &Agent) -> bool {
    crate::remote::runs_off_host_in(
        store,
        agent,
        &anyagents_core::agent_config_dir(store.data_dir(), &agent.id),
    )
}

/// Over HTTP MCP an agent is created on, or moved to, an off-host runtime (`cloud` or
/// `sandbox`) only: a `local` runtime would run on this host on the call's behalf.
pub(super) fn refuse_host_placement_over_http(
    via_http: bool,
    data_dir: &Path,
    harness: &str,
    explicit_runtime: Option<&str>,
) -> Result<(), ApiError> {
    if !via_http {
        return Ok(());
    }
    let remote_required = crate::sandbox_providers::SandboxMode::from_env().is_remote_required();
    let default_runtime = if remote_required || super::serverless::is_serverless_mode() {
        anyagents_core::ExecutionRuntime::Sandbox { provider: None }
    } else {
        anyagents_core::ExecutionRuntime::Local
    };
    let resolved =
        anyagents_harness::resolve_harness_and_runtime(harness, explicit_runtime, default_runtime)
            .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err.to_string()))?;
    let explicitly_local = explicit_runtime
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| anyagents_core::ExecutionRuntime::parse(s).ok())
        .is_some_and(|rt| rt == anyagents_core::ExecutionRuntime::Local)
        || anyagents_harness::decompose_harness_selector(harness)
            .ok()
            .and_then(|(_, rt)| rt)
            .is_some_and(|rt| rt == anyagents_core::ExecutionRuntime::Local);
    let remote_capable = !explicitly_local
        && (resolved.runtime.is_off_host()
            || vendor::is_effective_cloud_harness(&resolved.harness, Some(data_dir), None)
            || remote_required);
    if !remote_capable {
        return Err(ApiError::forbidden(format!(
            "a call over HTTP MCP places agents only on a cloud harness (`claude-cloud`, \
             `openai-cloud`, `cursor-cloud`, `spiron-cloud`, or `--runtime cloud`/`sandbox`); \
             `{harness}` would run on this host"
        )));
    }
    Ok(())
}

/// Over HTTP MCP an attachment is an HTTP(S) URL or a private media reference.
/// A local path or a bare Telegram file ID must never be interpreted on the host.
pub(super) fn refuse_host_media_over_http(body: &SendRequest) -> Result<(), ApiError> {
    if let Some(media) = &body.media {
        require_remote_media_source(&media.source)?;
    }
    Ok(())
}

pub(super) fn require_remote_media_source(source: &str) -> Result<(), ApiError> {
    use super::private_media::{UPLOAD_HELP, source_id};
    let source = source.trim();
    if source_id(source)?.is_some()
        || source.starts_with("http://")
        || source.starts_with("https://")
    {
        return Ok(());
    }
    Err(ApiError::forbidden(format!(
        "HTTP MCP accepts HTTP(S) URLs or private media references, not host paths or Telegram file IDs. {UPLOAD_HELP}"
    )))
}

/// A harness that runs off this host can only call back through
/// `public_url`; refuse to create or move an agent onto one without it, before
/// anything is written.
pub(super) fn require_remote_callback(
    data_dir: &Path,
    harness: &str,
    explicit_runtime: Option<&str>,
) -> Result<(), ApiError> {
    let default_runtime = if crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
        || super::serverless::is_serverless_mode()
    {
        anyagents_core::ExecutionRuntime::Sandbox { provider: None }
    } else {
        anyagents_core::ExecutionRuntime::Local
    };
    let is_cloud =
        anyagents_harness::resolve_harness_and_runtime(harness, explicit_runtime, default_runtime)
            .map(|r| r.runtime.is_cloud())
            .unwrap_or_else(|_| vendor::is_cloud_harness(harness));
    if is_cloud {
        crate::remote::require_public_url(data_dir)
            .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?;
    }
    Ok(())
}

/// Require `public_url` whenever explicitly switching an agent to an off-host runtime (`cloud` or `sandbox`).
pub(super) fn require_remote_callback_for_runtime(
    data_dir: &Path,
    runtime: &anyagents_core::ExecutionRuntime,
) -> Result<(), ApiError> {
    if runtime.is_off_host() {
        crate::remote::require_public_url(data_dir)
            .map_err(|why| ApiError::new(StatusCode::BAD_REQUEST, why))?;
    }
    Ok(())
}

pub(super) fn is_self_or_ancestor(
    store: &Store,
    caller_id: &anyagents_core::AgentId,
    target: &anyagents_core::Agent,
) -> bool {
    if caller_id == &target.id {
        return true;
    }
    let mut cursor = target.parent_id.clone();
    let mut depth = 0usize;
    while let Some(pid) = cursor {
        if &pid == caller_id {
            return true;
        }
        cursor = store.agent(&pid).ok().and_then(|a| a.parent_id);
        depth += 1;
        if depth > 16 {
            break;
        }
    }
    false
}

/// An unrestricted reader's scope: every agent, archived ones included.
pub(super) fn every_agent_id(store: &Store) -> Result<Vec<String>, ApiError> {
    Ok(store
        .agent_ids()
        .map_err(ApiError::from_store)?
        .iter()
        .map(ToString::to_string)
        .collect())
}

/// Resolve the caller's conversation visibility once, before selecting a
/// recent or Raw-RAG read path. Non-admin agents see only themselves and their
/// descendants; `all` is an alias for exactly that set. Administrative reads
/// are intentionally unrestricted and leave an audit row.
pub(super) fn conversation_visibility(
    store: &Store,
    principal: &Principal,
    requested: &str,
) -> Result<(String, Option<Vec<String>>), ApiError> {
    let requested = requested.trim();
    match principal {
        Principal::Operator => {
            store
                .record_admin_audit("operator", "conversation.read", requested)
                .map_err(ApiError::from_store)?;
            Ok((requested.to_string(), None))
        }
        Principal::Agent { agent_id, .. } => {
            let caller = store.agent(agent_id).map_err(ApiError::from_store)?;
            if requested.eq_ignore_ascii_case("self") {
                return Ok((caller.name, Some(vec![caller.id.to_string()])));
            }
            let mut visible_ids = store
                .descendant_ids(&caller.id)
                .map_err(ApiError::from_store)?;
            visible_ids.insert(caller.id.clone());
            if requested.eq_ignore_ascii_case("all") || requested == "*" {
                let visible_agent_ids = visible_ids.iter().map(ToString::to_string).collect();
                return Ok(("all".to_string(), Some(visible_agent_ids)));
            }

            let target = store
                .agent_by_name(requested)
                .map_err(ApiError::from_store)?;
            if !visible_ids.contains(&target.id) {
                return Err(ApiError::forbidden(format!(
                    "agent `{}` cannot read private conversation history of `{}`; your own messages with it are in your history: `conversation({{with: \"{}\"}})`",
                    caller.name, target.name, target.name
                )));
            }
            Ok((target.name, Some(vec![target.id.to_string()])))
        }
    }
}

/// One line of a sign-in, as the client reads it.
#[derive(serde::Serialize)]
pub(super) struct SignInLine {
    pub(super) kind: &'static str,
    pub(super) text: String,
}

/// Runs the vendor command for real. Each line it prints reaches the sink
/// before the command returns.
pub(super) struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        let env_pairs: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let mut command = anyagents_harness::spawn_command(Path::new(program), &env_pairs)
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        command
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        if let Some(stdout) = child.stdout.take() {
            for line in BufReader::new(stdout).lines() {
                let line = line.map_err(|err| InstallError::Failed(err.to_string()))?;
                on_line.line(&line);
            }
        }
        let status = child
            .wait()
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        if status.success() {
            Ok(())
        } else {
            Err(InstallError::Failed(format!("exit {status}")))
        }
    }
}

pub(super) async fn spawn_store<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    // The call counts toward a stop until this work returns, even if the
    // call's future is dropped first (`in_flight`).
    let call = in_flight::carry();
    match tokio::task::spawn_blocking(move || {
        let _call = call;
        work()
    })
    .await
    {
        Ok(result) => result,
        Err(err) => Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )),
    }
}

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(super) status: StatusCode,
    pub(crate) message: String,
}

impl ApiError {
    pub(super) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub(super) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    pub(super) fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, message)
    }

    pub(super) fn from_store(err: Error) -> Self {
        let status = match &err {
            Error::PermissionDenied(_) => StatusCode::FORBIDDEN,
            Error::AgentBusy => StatusCode::CONFLICT,
            Error::AgentNotFound(_) => StatusCode::NOT_FOUND,
            Error::GroupForbidden { .. } => StatusCode::FORBIDDEN,
            Error::AgentExists(_) => StatusCode::CONFLICT,
            Error::InvalidAddress(_)
            | Error::Message(_)
            | Error::WorktreeSourceUnpublished { .. } => StatusCode::BAD_REQUEST,
            Error::WorktreeProvisioning { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Error::WorkdirPermission { .. } => StatusCode::FORBIDDEN,
            Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::new(status, err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

/// The working directory every owner agent shares, when the deployment sets one.
/// `ANYAGENTS_OWNER_WORKDIR` names it; without it an agent keeps its own
/// managed directory.
pub fn owner_workdir() -> Option<std::path::PathBuf> {
    std::env::var_os("ANYAGENTS_OWNER_WORKDIR").map(std::path::PathBuf::from)
}
