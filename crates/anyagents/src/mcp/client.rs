//! HTTP/UDS client link, capability auth, and reconnect/restart waiting.

use super::*;

pub(crate) const TASK_TURN_CONTEXT_HEADER: &str = "x-anyagents-task-turn-context";

pub(crate) fn is_task_turn_context(token: &str) -> bool {
    token.len() == 32 && token.bytes().all(|c| c.is_ascii_hexdigit())
}

#[derive(Clone)]
pub(crate) struct Client {
    pub(crate) socket: PathBuf,
    pub(crate) config: PathBuf,
    /// The capability read at start, used while the file cannot be read.
    pub(crate) capability: String,
    /// Set for the tool server the HTTP MCP route runs inside the server
    /// process: every call it makes carries [`HTTP_MCP_HEADER`], so routes
    /// that would start or read something on the host refuse it.
    pub(super) via_http: bool,
    /// A mounted stdio call's opener context, scoped to this dispatch clone.
    pub(super) turn_context: Option<String>,
    /// The router of the server process that owns this HTTP MCP tool server:
    /// calls go straight through it instead of dialing the external socket,
    /// which closes at the start of a stop while admitted calls still drain.
    in_process: Option<axum::Router>,
    /// Verified principal injected directly into `in_process` router calls so
    /// HTTP MCP never reads pod-local `mcp-capability` or calls `operator::ensure()`.
    in_process_principal: Option<crate::auth::Principal>,
    /// What every clone of this client shares: idle connections to the
    /// server and the capability file as last read.
    link: Arc<ServerLink>,
}

/// Connections kept open between calls, at most this many while idle. A
/// burst of parallel calls opens what it needs; the rest close after it.
pub(super) const MAX_IDLE_CONNECTIONS: usize = 4;

#[derive(Default)]
pub(super) struct ServerLink {
    idle: std::sync::Mutex<Vec<http1::SendRequest<Full<Bytes>>>>,
    capability: std::sync::Mutex<Option<(CapabilityStamp, String)>>,
}

/// The identity of one version of the capability file. A rotation writes a
/// new file and renames it into place, which changes the inode; an in-place
/// rewrite changes the size or a timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CapabilityStamp {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

impl CapabilityStamp {
    pub(super) fn of(meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt as _;
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            len: meta.len(),
            mtime: (meta.mtime(), meta.mtime_nsec()),
            ctime: (meta.ctime(), meta.ctime_nsec()),
        }
    }
}

pub(super) fn locked<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Client {
    pub(crate) fn new(socket: PathBuf, config: PathBuf, capability: String) -> Self {
        Self {
            socket,
            config,
            capability,
            via_http: false,
            turn_context: None,
            in_process: None,
            in_process_principal: None,
            link: Arc::default(),
        }
    }

    /// The client of a tool server that answers MCP over HTTP.
    #[cfg(test)]
    pub(super) fn over_http(mut self) -> Self {
        self.via_http = true;
        self
    }

    /// The client of an HTTP MCP tool server running inside `router`'s own
    /// server process with a directly injected authenticated principal.
    pub(super) fn over_http_router(
        mut self,
        router: axum::Router,
        principal: crate::auth::Principal,
    ) -> Self {
        self.via_http = true;
        self.in_process = Some(router);
        self.in_process_principal = Some(principal);
        self
    }

    /// The capability in the agent's config directory now. The server may
    /// rotate it while this process runs, so every call presents what the
    /// file holds; the file is read again only when its identity changed.
    pub(super) fn current_capability(&self) -> String {
        let Ok(meta) = std::fs::metadata(self.config.join(CAPABILITY_FILE)) else {
            return self.capability.clone();
        };
        let stamp = CapabilityStamp::of(&meta);
        let mut cached = locked(&self.link.capability);
        if let Some((seen, token)) = cached.as_ref()
            && *seen == stamp
        {
            return token.clone();
        }
        match capability_of(&self.config) {
            Ok(token) => {
                *cached = Some((stamp, token.clone()));
                token
            }
            Err(_) => self.capability.clone(),
        }
    }

    /// One call on a kept-alive connection to the server. A connection the
    /// server closed (it restarted, or it is stopping) is dropped and the
    /// call goes on a new one. A request hyper hands back unsent goes again,
    /// on any connection; one the server may have received is repeated only
    /// when it reads (`GET`), and otherwise ends in [`interrupted_call`], so
    /// nothing that changes state is ever applied twice.
    pub(super) async fn send(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        capability: &str,
        agent_id: Option<&str>,
    ) -> Result<(hyper::StatusCode, Vec<u8>), String> {
        let payload = body.unwrap_or_default();
        let cap_header = if self.in_process_principal.is_some() {
            None
        } else {
            Some(capability)
        };
        let fresh = || {
            let mut request = build_request(
                method,
                path,
                Some(payload.clone()),
                cap_header,
                None,
                agent_id,
                self.via_http,
            )?;
            if let Some(principal) = &self.in_process_principal {
                request
                    .extensions_mut()
                    .insert(crate::auth::InjectedPrincipal(principal.clone()));
            }
            if path == "/v1/send"
                && let Some(context) = self.turn_context.as_deref()
            {
                let mut value = hyper::header::HeaderValue::from_str(context)
                    .map_err(|_| "invalid task turn context".to_string())?;
                value.set_sensitive(true);
                request
                    .headers_mut()
                    .insert(TASK_TURN_CONTEXT_HEADER, value);
            }
            Ok::<_, String>(request)
        };
        if let Some(router) = &self.in_process {
            use hyper::service::Service as _;
            if call_cancelled() {
                return Err("the tool call was cancelled".into());
            }
            let service = hyper_util::service::TowerToHyperService::new(router.clone());
            let Ok(response) = service.call(fresh()?).await;
            let status = response.status();
            let collected = response
                .into_body()
                .collect()
                .await
                .map_err(|error| error.to_string())?;
            return Ok((status, collected.to_bytes().to_vec()));
        }
        let mut request = fresh()?;
        let mut reads_repeated = false;
        let wait = self.restart_wait();
        loop {
            let (mut sender, reused) = match self.idle_connection().await {
                Some(sender) => (sender, true),
                // A tool call made while the server restarts waits for it,
                // within what is left of the call's own wait.
                None => (connect_waiting(&self.socket, &wait).await?, false),
            };
            // A call its harness gave up on while it waited is not sent.
            if call_cancelled() {
                return Err("the tool call was cancelled".into());
            }
            let response = match sender.try_send_request(request).await {
                Ok(response) => response,
                Err(mut error) => {
                    if let Some(unsent) = error.take_message() {
                        // Nothing reached the server: the next connection
                        // (to this server or the next one) takes it. A new
                        // connection closed unused is a server stopping, so
                        // that pause counts against the call's wait.
                        request = unsent;
                        if !reused {
                            pause_for_restart(&wait, &error.into_error().to_string()).await?;
                        }
                        continue;
                    }
                    let error = error.into_error().to_string();
                    match after_lost_connection(method, &mut reads_repeated) {
                        Some(()) => {
                            request = fresh()?;
                            continue;
                        }
                        None => return Err(interrupted_call(method, path, &error)),
                    }
                }
            };
            let status = response.status();
            let bytes = match response.into_body().collect().await {
                Ok(collected) => collected.to_bytes().to_vec(),
                Err(error) => match after_lost_connection(method, &mut reads_repeated) {
                    Some(()) => {
                        request = fresh()?;
                        continue;
                    }
                    None => return Err(interrupted_call(method, path, &error.to_string())),
                },
            };
            let mut idle = locked(&self.link.idle);
            if idle.len() < MAX_IDLE_CONNECTIONS {
                idle.push(sender);
            }
            return Ok((status, bytes));
        }
    }

    /// How long this client's calls wait for a server that is not listening:
    /// one wait for the whole call, so a call outside any tool call's scope
    /// still ends. A tool server inside the server process (over HTTP) never
    /// waits: the socket it calls is its own process's, so a refusal means
    /// that server is stopping, no other server will answer this process, and
    /// waiting would only hold the stop open.
    pub(super) fn restart_wait(&self) -> SocketWait {
        if self.via_http {
            SocketWait::new(std::time::Duration::ZERO)
        } else {
            call_wait()
        }
    }

    /// An idle connection that can take a request now, if one is open.
    pub(super) async fn idle_connection(&self) -> Option<http1::SendRequest<Full<Bytes>>> {
        loop {
            let mut sender = locked(&self.link.idle).pop()?;
            if sender.ready().await.is_ok() {
                return Some(sender);
            }
        }
    }

    /// The agent this capability authenticates, as the server resolved it.
    pub(super) async fn whoami_with_harness(&self) -> Result<(AgentRef, Option<String>), String> {
        let named = crate::client::agent_identity().map(|id| id.to_string());
        let cap = self.current_capability();
        let who: serde_json::Value = self
            .send("GET", "/v1/whoami", None, &cap, named.as_deref())
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
        let workdir = who
            .get("workdir")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let harness = who
            .get("harness")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        Ok((
            AgentRef {
                id: AgentId::parse(id).map_err(|err| err.to_string())?,
                name: name.to_string(),
                workdir,
            },
            harness,
        ))
    }

    pub(super) async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, String> {
        let method = if body.is_some() { "POST" } else { "GET" };
        self.request_method(method, path, body).await
    }

    pub(super) async fn request_method<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, String> {
        let named = crate::client::agent_identity().map(|id| id.to_string());
        let cap = self.current_capability();
        let (status, bytes) = self
            .send(method, path, body, &cap, named.as_deref())
            .await?;
        if !status.is_success() {
            return Err(error_message(&bytes));
        }
        serde_json::from_slice(&bytes).map_err(|err| err.to_string())
    }

    pub(super) async fn get_agents(&self) -> Result<AgentsResponse, String> {
        self.get_agents_for_state(None).await
    }

    pub(super) async fn get_agents_for_state(
        &self,
        state: Option<&str>,
    ) -> Result<AgentsResponse, String> {
        // The daemon defaults to live rows; turn-state filters stay local.
        let path = match state.map(str::trim) {
            Some(s) if s.eq_ignore_ascii_case("archived") => "/v1/agents?state=archived",
            Some(s) if s.eq_ignore_ascii_case("all") => "/v1/agents?state=all",
            _ => "/v1/agents",
        };
        self.get(path, None).await
    }

    pub(super) async fn get_agent(&self, target: &str) -> Result<crate::api::AgentView, String> {
        let encoded = super::urlencoding_simple(target);
        self.get(&format!("/v1/agents/{encoded}"), None).await
    }

    /// Resolve a target agent for `inspect` / `status`. Checks the live roster
    /// first and falls back to `GET /v1/agents/{id_or_name}` so recently
    /// archived agents remain inspectable by ID, name, or alias.
    pub(super) async fn resolve_agent_for_inspect(
        &self,
        self_id: &str,
        clean: &str,
        label: &str,
    ) -> Result<crate::api::AgentView, String> {
        let resp: AgentsResponse = self.get_agents().await?;
        if let Some(view) = resp.agents.into_iter().find(|a| {
            (clean.eq_ignore_ascii_case("self") && a.id == self_id)
                || a.name.eq_ignore_ascii_case(clean)
                || a.id == clean
                || a.aliases.iter().any(|al| al.eq_ignore_ascii_case(clean))
        }) {
            return Ok(view);
        }
        let lookup = if clean.eq_ignore_ascii_case("self") {
            self_id
        } else {
            clean
        };
        self.get_agent(lookup)
            .await
            .map_err(|_| format!("{label} `{clean}` not found"))
    }
}

pub(super) fn error_message(bytes: &[u8]) -> String {
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

/// One call on its own connection to the socket, without waiting for a
/// server that is not listening: the status code and the body. Commands use
/// [`request_waiting`]; a tool server keeps its connections open between
/// calls (see [`Client`]).
#[cfg(test)]
pub(crate) async fn request(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    capability: Option<&str>,
    operator: Option<&str>,
) -> Result<(hyper::StatusCode, Vec<u8>), String> {
    request_waiting(
        socket,
        method,
        path,
        body,
        capability,
        operator,
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(180),
    )
    .await
}

/// What one exchange on a connection came to.
pub(super) enum Exchange {
    Answered(hyper::StatusCode, Vec<u8>),
    /// Handed back unsent: nothing reached the server.
    Unsent(String),
    /// Lost after the request went out: it may have applied.
    Lost(String),
}

/// [`request`], waiting up to `within` for a server that is restarting (its
/// socket missing or refusing: nothing was sent). A connection lost after the
/// request went out is retried once for a read; any other call ends in
/// [`interrupted_call`], since it may have applied.
///
/// `answer_within` bounds the server's answer once a connection is open; it
/// never counts the wait for a server, so a long wait does not eat the time
/// the call itself needs.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn request_waiting(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    capability: Option<&str>,
    operator: Option<&str>,
    within: std::time::Duration,
    answer_within: std::time::Duration,
) -> Result<(hyper::StatusCode, Vec<u8>), String> {
    let payload = body.unwrap_or_default();
    let wait = SocketWait::new(within);
    let mut repeated = false;
    loop {
        let request = build_request(
            method,
            path,
            Some(payload.clone()),
            capability,
            operator,
            None,
            false,
        )?;
        let mut sender = connect_waiting(socket, &wait).await?;
        let exchange = async {
            match sender.try_send_request(request).await {
                Err(mut error) => {
                    let unsent = error.take_message().is_some();
                    let error = error.into_error().to_string();
                    if unsent {
                        Exchange::Unsent(error)
                    } else {
                        Exchange::Lost(error)
                    }
                }
                Ok(response) => {
                    let status = response.status();
                    match response.into_body().collect().await {
                        Ok(collected) => Exchange::Answered(status, collected.to_bytes().to_vec()),
                        Err(error) => Exchange::Lost(error.to_string()),
                    }
                }
            }
        };
        let Ok(exchange) = tokio::time::timeout(answer_within, exchange).await else {
            let waited = format!(
                "the server did not answer within {}s",
                answer_within.as_secs()
            );
            return Err(if method.eq_ignore_ascii_case("GET") {
                waited
            } else {
                format!(
                    "{waited} ({method} {path}); it may or may not have applied. Check with a \
                     read of what it changes before trying it again"
                )
            });
        };
        match exchange {
            Exchange::Answered(status, bytes) => return Ok((status, bytes)),
            Exchange::Unsent(error) => pause_for_restart(&wait, &error).await?,
            Exchange::Lost(error) => {
                if after_lost_connection(method, &mut repeated).is_none() {
                    return Err(interrupted_call(method, path, &error));
                }
            }
        }
    }
}

/// One request to the server's API, with whichever credentials the caller
/// presents. The agent id is what a deployment without the boundary uses to
/// name its one route; with the boundary only the capability decides, and an
/// operator token alone is accepted beside it.
/// Marks a call made by the tool server behind the HTTP MCP route. It can only
/// narrow what a call may do, so a caller that omits it gains nothing: the
/// tool server that runs as the server's own user always sets it.
pub(crate) const HTTP_MCP_HEADER: &str = "x-anyagents-via-http-mcp";

pub(super) fn build_request(
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    capability: Option<&str>,
    operator: Option<&str>,
    agent_id: Option<&str>,
    via_http: bool,
) -> Result<hyper::Request<Full<Bytes>>, String> {
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
    if via_http {
        builder = builder.header(HTTP_MCP_HEADER, "1");
    }
    builder
        .body(Full::new(Bytes::from(payload)))
        .map_err(|err| err.to_string())
}

/// How long a call waits for the server's socket to come back, at most.
/// The harness outlives a server restart or upgrade (its keeper holds it),
/// and for that moment nothing listens: the socket is missing or refuses.
/// A refused or missing socket means nothing was sent, so only the
/// connection is tried again; no request is ever sent twice.
///
/// A restart is bounded on the server's side: a stopping server lets its
/// running mutations finish (seconds normally, at most a minute) and exits
/// within `server::STOP_WITHIN`, and a new server binds its socket within
/// about a second of starting. Two minutes covers that with room for a slow start (a
/// migration), and is further cut to what the harness allows
/// ([`harness_patience`]).
pub(super) const RESTART_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

/// The wait of a one-shot command an agent runs in its shell (`anyagents
/// send ...`): shell tools time out sooner than MCP calls, so it stays short.
/// An operator's read does not wait; it falls back to the store at once.
pub(crate) const RECONNECT_WITHIN: std::time::Duration = std::time::Duration::from_secs(45);

/// How long the harness waits for one tool call before giving up on it, where
/// that is known: a call is never sent after its harness gave up, so the
/// socket wait ends [`PATIENCE_MARGIN`] before this, counted from the call's
/// start. Codex: 60 s (`tool_timeout_sec`). agy: 3 min (its MCP calls end in
/// "timed out after 3m0s"). Claude Code (stdio: about 28 h, 30 min idle),
/// OpenCode (12 h) and Spiron (no stdio call deadline) wait longer than any
/// restart. Any other harness gets the shortest known, Codex's. A harness
/// that cancels the call (`notifications/cancelled`) ends the wait at once.
pub(super) fn harness_patience(harness: Option<&str>) -> Option<std::time::Duration> {
    match harness.map(str::trim).unwrap_or_default() {
        "claude-code" | "opencode" | "spiron" => None,
        "agy" => Some(std::time::Duration::from_secs(180)),
        _ => Some(std::time::Duration::from_secs(60)),
    }
}

/// Room left between the end of a socket wait and the harness's deadline: the
/// request must still be answered.
pub(super) const PATIENCE_MARGIN: std::time::Duration = std::time::Duration::from_secs(15);

/// The harness this tool server serves, as the server named it at start.
pub(super) static SERVED_HARNESS: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

/// One tool call as the requests it makes see it: whether its harness
/// cancelled it, and the time it has spent waiting for the server's socket,
/// shared by every request the call (and everything it spawns) makes.
#[derive(Clone)]
pub(super) struct Call {
    cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
    wait: SocketWait,
}

impl Call {
    /// A call `cancelled` reports on, starting now, with the whole wait
    /// still ahead: up to [`RESTART_WAIT`], ending before its harness would
    /// give up on it.
    pub(super) fn new(cancelled: Arc<dyn Fn() -> bool + Send + Sync>) -> Self {
        let harness = SERVED_HARNESS.get().and_then(Option::as_deref);
        let until = harness_patience(harness)
            .map(|patience| tokio::time::Instant::now() + patience.saturating_sub(PATIENCE_MARGIN));
        Self {
            cancelled,
            wait: SocketWait::new(RESTART_WAIT).until(until),
        }
    }

    #[cfg(test)]
    pub(super) fn waiting_at_most(
        cancelled: Arc<dyn Fn() -> bool + Send + Sync>,
        budget: std::time::Duration,
    ) -> Self {
        Self {
            cancelled,
            wait: SocketWait::new(budget),
        }
    }
}

/// The time spent waiting for the server's socket, against a budget: only
/// the time some request actually waits counts (requests waiting at once
/// count it once), so a call that has run long still has its whole wait —
/// unless its harness's deadline (`until`) comes first.
#[derive(Clone)]
pub(super) struct SocketWait {
    budget: std::time::Duration,
    until: Option<tokio::time::Instant>,
    spent: Arc<std::sync::Mutex<Spent>>,
}

#[derive(Default)]
pub(super) struct Spent {
    before: std::time::Duration,
    waiters: usize,
    since: Option<tokio::time::Instant>,
}

impl SocketWait {
    pub(super) fn new(budget: std::time::Duration) -> Self {
        Self {
            budget,
            until: None,
            spent: Arc::default(),
        }
    }

    /// The same wait, ending by `until` whatever is left of the budget.
    pub(super) fn until(mut self, until: Option<tokio::time::Instant>) -> Self {
        self.until = until;
        self
    }

    pub(super) fn spent(&self) -> std::sync::MutexGuard<'_, Spent> {
        self.spent.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// What is left of the budget, and before the deadline.
    pub(super) fn left(&self) -> std::time::Duration {
        let spent = self.spent();
        let running = spent
            .since
            .map_or(std::time::Duration::ZERO, |since| since.elapsed());
        let left = self.budget.saturating_sub(spent.before + running);
        match self.until {
            Some(until) => left.min(until.saturating_duration_since(tokio::time::Instant::now())),
            None => left,
        }
    }

    /// One request starts waiting; the wait counts until the guard drops.
    pub(super) fn begin(&self) -> Waiting<'_> {
        let mut spent = self.spent();
        if spent.waiters == 0 {
            spent.since = Some(tokio::time::Instant::now());
        }
        spent.waiters += 1;
        Waiting(self)
    }
}

/// A request waiting for the socket.
pub(super) struct Waiting<'a>(&'a SocketWait);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let mut spent = self.0.spent();
        spent.waiters -= 1;
        if spent.waiters == 0
            && let Some(since) = spent.since.take()
        {
            spent.before += since.elapsed();
        }
    }
}

tokio::task_local! {
    /// The tool call this task serves.
    static CALL: Call;
}

/// The current tool call was cancelled by its harness.
pub(super) fn call_cancelled() -> bool {
    CALL.try_with(|call| (call.cancelled)()).unwrap_or(false)
}

/// The current tool call's wait for the server's socket; a request outside
/// any call has the whole wait to itself.
pub(super) fn call_wait() -> SocketWait {
    CALL.try_with(|call| call.wait.clone())
        .unwrap_or_else(|_| SocketWait::new(RECONNECT_WITHIN))
}

/// Run `body` as `call`, so the requests it makes are neither connected nor
/// sent once its harness cancelled it, and wait no longer than it may.
pub(super) async fn as_call<F: std::future::Future>(call: Call, body: F) -> F::Output {
    CALL.scope(call, body).await
}

/// How often a missing or refusing socket is tried again.
pub(super) const RECONNECT_PROBE: std::time::Duration = std::time::Duration::from_millis(250);

/// A connect error that means no server listens right now (between one
/// server's stop and the next one's start): the socket file is missing, it
/// refuses, or its backlog is full. Nothing reached a server.
pub(super) fn server_restarting(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::WouldBlock
    )
}

/// A new connection the server closed before taking the request: it is
/// stopping. Wait a probe's length, counted against the call's wait.
pub(super) async fn pause_for_restart(wait: &SocketWait, error: &str) -> Result<(), String> {
    if wait.left().is_zero() {
        return Err(format!(
            "cannot reach the server: {error}. The call was not sent; the server is \
             restarting, so try it again shortly"
        ));
    }
    let _waiting = wait.begin();
    tokio::time::sleep(RECONNECT_PROBE.min(wait.left())).await;
    Ok(())
}

/// After a connection was lost with a request on it: a read is sent once
/// more (reading twice changes nothing); anything else is not.
pub(super) fn after_lost_connection(method: &str, repeated: &mut bool) -> Option<()> {
    if method.eq_ignore_ascii_case("GET") && !*repeated {
        *repeated = true;
        return Some(());
    }
    None
}

/// The error for a call whose connection the server closed after it may have
/// received it: the server restarted (or stopped) mid-call, so the change may
/// or may not have been made, and repeating it blindly could make it twice.
pub(crate) fn interrupted_call(method: &str, path: &str, error: &str) -> String {
    let check = match path.split('?').next().unwrap_or(path) {
        "/v1/send" => {
            "read your recent messages with `conversation` (or send again with the same \
             `idempotency_key`, which never delivers twice)"
        }
        p if p.ends_with("/schedules") => "`schedules` with `action: \"list\"`",
        "/v1/agents" => "`agents` list (a spawn shows up there once it applied)",
        p if p.starts_with("/v1/agents/") => "`agents` inspect on that agent",
        p if p.starts_with("/v1/groups") => "`groups` list",
        _ => "a read of what it changes",
    };
    format!(
        "the server restarted during this call ({method} {path}: {error}); it may or may not \
         have applied. Check with {check} before trying it again"
    )
}

/// [`connect_waiting`] for a test, waiting up to `within` while nothing
/// listens on `socket`.
#[cfg(test)]
pub(super) async fn connect_within(
    socket: &Path,
    within: std::time::Duration,
) -> Result<http1::SendRequest<Full<Bytes>>, String> {
    connect_waiting(socket, &SocketWait::new(within)).await
}

/// A new HTTP/1 connection to the server's socket, waiting within what is
/// left of `wait` while nothing listens on it (an error at once with no wait
/// left). Its driver runs as a task until the server closes it or the last
/// sender is dropped.
pub(super) async fn connect_waiting(
    socket: &Path,
    wait: &SocketWait,
) -> Result<http1::SendRequest<Full<Bytes>>, String> {
    let mut waiting = None;
    let stream = loop {
        if call_cancelled() {
            return Err("the tool call was cancelled".into());
        }
        match tokio::net::UnixStream::connect(socket).await {
            Ok(stream) => break stream,
            Err(err) if server_restarting(&err) && !wait.left().is_zero() => {
                waiting.get_or_insert_with(|| {
                    eprintln!(
                        "anyagents: the server is not listening ({err}); nothing was sent, \
                         waiting up to {}s for it to come back",
                        wait.left().as_secs()
                    );
                    (wait.begin(), tokio::time::Instant::now())
                });
                tokio::time::sleep(RECONNECT_PROBE.min(wait.left())).await;
            }
            Err(err) if server_restarting(&err) => {
                return Err(format!(
                    "cannot reach the server: {err}. The call was not sent; the server is \
                     restarting or down, so try it again shortly"
                ));
            }
            Err(err) => return Err(format!("cannot reach the server: {err}")),
        }
    };
    if let Some((_, since)) = waiting.take() {
        eprintln!(
            "anyagents: the server is back after {:.1}s",
            since.elapsed().as_secs_f64()
        );
    }
    let (sender, connection) = http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|err| err.to_string())?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(sender)
}

#[cfg(test)]
mod reconnect_tests {
    use std::sync::Arc;
    /// A tool call made while the server restarts waits for its socket to
    /// come back, and its request is sent once, on the new server.
    #[tokio::test]
    async fn a_call_made_while_the_server_restarts_reaches_the_next_server() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let listening = socket.clone();
        let server = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
            let listener = tokio::net::UnixListener::bind(&listening).unwrap();
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut seen = Vec::new();
            let mut buf = [0u8; 1024];
            while !String::from_utf8_lossy(&seen).contains("\r\n\r\n") {
                let read = stream.read(&mut buf).await.unwrap();
                seen.extend_from_slice(&buf[..read]);
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .unwrap();
            // A second request on another connection would block here.
            drop(listener);
            seen
        });
        let mut sender = super::connect_within(&socket, std::time::Duration::from_secs(20))
            .await
            .unwrap();
        let request =
            super::build_request("GET", "/v1/health", None, None, None, None, false).unwrap();
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), hyper::StatusCode::OK);
        let seen = server.await.unwrap();
        let seen = String::from_utf8_lossy(&seen);
        assert!(
            seen.starts_with("GET ") && seen.contains("/v1/health"),
            "{seen}"
        );
    }

    /// A call its harness cancels while it waits for the server is never
    /// carried out: the next server never sees it.
    #[tokio::test]
    async fn a_call_cancelled_while_it_waits_never_reaches_the_next_server() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let cancelled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&cancelled);
        let client = super::Client::new(
            socket.clone(),
            dir.path().join("config"),
            "synthetic-test-capability".into(),
        );
        let call = tokio::spawn(super::as_call(
            super::Call::new(Arc::new(move || flag.load(Ordering::SeqCst))),
            async move {
                client
                    .send("POST", "/v1/send", Some(b"{}".to_vec()), "cap", None)
                    .await
            },
        ));
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        cancelled.store(true, Ordering::SeqCst);
        // The harness gave up; then the next server comes up.
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let error = call
            .await
            .unwrap()
            .expect_err("a cancelled call was carried out");
        assert!(error.contains("cancelled"), "{error}");
        // It may have connected in the instant before it saw the cancel, but
        // it sends nothing on that connection.
        if let Ok(accepted) =
            tokio::time::timeout(std::time::Duration::from_millis(800), listener.accept()).await
        {
            use tokio::io::AsyncReadExt as _;
            let (mut stream, _) = accepted.unwrap();
            let mut seen = Vec::new();
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                stream.read_to_end(&mut seen),
            )
            .await;
            assert!(
                seen.is_empty(),
                "the cancelled call reached the next server: {}",
                String::from_utf8_lossy(&seen)
            );
        }
    }

    /// The wait is the call's, not each request's: requests a call makes one
    /// after another while the server is gone share it.
    #[tokio::test]
    async fn requests_in_one_call_share_its_wait() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let client = super::Client::new(
            socket,
            dir.path().join("config"),
            "synthetic-test-capability".into(),
        );
        let started = std::time::Instant::now();
        let call = super::Call::waiting_at_most(
            Arc::new(|| false),
            std::time::Duration::from_millis(1_000),
        );
        let failures = super::as_call(call, async move {
            let mut failures = 0;
            for _ in 0..3 {
                if client
                    .send("GET", "/v1/whoami", None, "cap", None)
                    .await
                    .is_err()
                {
                    failures += 1;
                }
            }
            failures
        })
        .await;
        assert_eq!(failures, 3);
        assert!(
            started.elapsed() < std::time::Duration::from_millis(1_900),
            "each request waited on its own: {:?}",
            started.elapsed()
        );
    }

    /// Only time spent waiting for the socket counts against a call's wait:
    /// a call that has run long still waits the whole time for a server
    /// that restarts.
    #[tokio::test]
    async fn a_call_that_ran_long_still_waits_for_a_restarting_server() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let client = super::Client::new(
            socket.clone(),
            dir.path().join("config"),
            "synthetic-test-capability".into(),
        );
        let call = super::Call::waiting_at_most(
            Arc::new(|| false),
            std::time::Duration::from_millis(1_000),
        );
        let answer = tokio::spawn(super::as_call(call, async move {
            // Longer than the whole wait, at work before the server goes.
            tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
            client.send("GET", "/v1/whoami", None, "cap", None).await
        }));
        // The server comes back within the wait, after the call's work.
        tokio::time::sleep(std::time::Duration::from_millis(2_000)).await;
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut seen = Vec::new();
        let mut buf = [0u8; 1024];
        while !String::from_utf8_lossy(&seen).contains("\r\n\r\n") {
            let read = stream.read(&mut buf).await.unwrap();
            seen.extend_from_slice(&buf[..read]);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            .await
            .unwrap();
        let answer = answer.await.unwrap();
        assert!(answer.is_ok(), "{answer:?}");
    }

    /// Past the wait, a server that is still gone is an error, as before.
    #[tokio::test]
    async fn a_server_still_gone_after_the_wait_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let started = std::time::Instant::now();
        let error = super::connect_within(&socket, std::time::Duration::from_millis(600))
            .await
            .expect_err("a missing server was reached");
        assert!(error.starts_with("cannot reach the server"), "{error}");
        assert!(started.elapsed() >= std::time::Duration::from_millis(600));
    }

    /// A missing, refusing or full socket means nothing was sent, so the
    /// connection is tried again; a lost connection repeats only a read, once.
    #[test]
    fn what_a_restart_lets_a_call_try_again() {
        use std::io::{Error, ErrorKind};
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::ConnectionRefused,
            ErrorKind::WouldBlock,
        ] {
            assert!(super::server_restarting(&Error::from(kind)), "{kind:?}");
        }
        for kind in [ErrorKind::PermissionDenied, ErrorKind::InvalidInput] {
            assert!(!super::server_restarting(&Error::from(kind)), "{kind:?}");
        }
        let mut repeated = false;
        assert!(super::after_lost_connection("GET", &mut repeated).is_some());
        assert!(super::after_lost_connection("GET", &mut repeated).is_none());
        for method in ["POST", "PUT", "DELETE"] {
            let mut repeated = false;
            assert!(super::after_lost_connection(method, &mut repeated).is_none());
        }
        for (path, check) in [
            ("/v1/send", "idempotency_key"),
            ("/v1/schedules", "schedules"),
            ("/v1/agents/worker/schedules", "schedules"),
            ("/v1/agents", "agents` list"),
            ("/v1/agents/worker/archive", "agents` inspect"),
        ] {
            let error = super::interrupted_call("POST", path, "connection closed");
            assert!(error.contains("may or may not have applied"), "{error}");
            assert!(error.contains(check), "{path}: {error}");
        }
    }

    /// The wait ends before the harness would give up on the call.
    #[test]
    fn a_call_waits_only_while_its_harness_still_waits() {
        use std::time::Duration;
        assert_eq!(
            super::harness_patience(Some("codex")),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            super::harness_patience(Some("agy")),
            Some(Duration::from_secs(180))
        );
        assert_eq!(super::harness_patience(Some("spiron")), None);
        assert_eq!(super::harness_patience(Some("claude-code")), None);
        assert_eq!(
            super::harness_patience(Some("something-new")),
            Some(Duration::from_secs(60))
        );
        assert_eq!(super::harness_patience(None), Some(Duration::from_secs(60)));
        let soon = tokio::time::Instant::now() + Duration::from_millis(200);
        let wait = super::SocketWait::new(Duration::from_secs(120)).until(Some(soon));
        assert!(wait.left() <= Duration::from_millis(200));
        let open = super::SocketWait::new(Duration::from_secs(120));
        assert!(open.left() > Duration::from_secs(119));
    }

    /// Read one HTTP request's head from `stream`.
    async fn read_head(stream: &mut tokio::net::UnixStream) -> String {
        use tokio::io::AsyncReadExt;
        let mut seen = Vec::new();
        let mut buf = [0u8; 1024];
        while !String::from_utf8_lossy(&seen).contains("\r\n\r\n") {
            let read = stream.read(&mut buf).await.unwrap();
            if read == 0 {
                break;
            }
            seen.extend_from_slice(&buf[..read]);
        }
        String::from_utf8_lossy(&seen).into_owned()
    }

    /// A command waits while the socket is missing (between one server and
    /// the next) and is answered by the server that comes up.
    #[tokio::test]
    async fn a_command_waits_while_the_socket_is_missing() {
        use tokio::io::AsyncWriteExt;
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let path = socket.clone();
        let call = tokio::spawn(async move {
            super::request_waiting(
                &path,
                "POST",
                "/v1/send",
                Some(b"{}".to_vec()),
                Some("cap"),
                None,
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(10),
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        assert!(
            !call.is_finished(),
            "the command gave up while the socket was missing"
        );
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let head = read_head(&mut stream).await;
        assert!(
            head.starts_with("POST ") && head.contains("/v1/send"),
            "{head}"
        );
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            .await
            .unwrap();
        let (status, body) = call.await.unwrap().unwrap();
        assert_eq!(status, hyper::StatusCode::OK);
        assert_eq!(body, b"ok");
    }

    /// A change whose connection is lost after it was sent is not sent again:
    /// the caller is told it may or may not have applied. A read is sent once
    /// more and answered.
    #[tokio::test]
    async fn a_lost_connection_repeats_only_a_read() {
        use tokio::io::AsyncWriteExt;
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let mut heads = Vec::new();
            // The first request of each call: read it, then drop the
            // connection unanswered, as a server stopping mid-call does.
            for answer in [false, false, true] {
                let accepted =
                    tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept())
                        .await;
                let Ok(Ok((mut stream, _))) = accepted else {
                    break;
                };
                heads.push(read_head(&mut stream).await);
                if answer {
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                        .await
                        .unwrap();
                }
            }
            heads
        });
        let error = super::request_waiting(
            &socket,
            "POST",
            "/v1/schedules",
            Some(b"{}".to_vec()),
            Some("cap"),
            None,
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(5),
        )
        .await
        .expect_err("a change was answered by a dropped connection");
        assert!(error.contains("may or may not have applied"), "{error}");
        let (status, body) = super::request_waiting(
            &socket,
            "GET",
            "/v1/schedules",
            None,
            Some("cap"),
            None,
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(status, hyper::StatusCode::OK);
        assert_eq!(body, b"ok");
        let heads = server.await.unwrap();
        let posts = heads.iter().filter(|h| h.starts_with("POST ")).count();
        let gets = heads.iter().filter(|h| h.starts_with("GET ")).count();
        assert_eq!((posts, gets), (1, 2), "{heads:?}");
    }

    /// The tool server inside the server process (MCP over HTTP) calls its
    /// own process's socket: a refusal there is that server stopping, so the
    /// call fails at once instead of holding the stop open while it waits.
    #[tokio::test]
    async fn a_tool_server_inside_the_server_never_waits_for_its_own_socket() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        // Bound and closed: the file stays and every connection is refused,
        // as for a server whose listener closed at its stop.
        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
        let client = super::Client::new(socket.clone(), dir.path().into(), "cap".into());
        let started = std::time::Instant::now();
        let error = client
            .clone()
            .over_http()
            .send("POST", "/v1/send", Some(b"{}".to_vec()), "cap", None)
            .await
            .expect_err("a refusing socket answered");
        assert!(error.contains("cannot reach the server"), "{error}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "waited {:?}",
            started.elapsed()
        );
        // A server that takes each connection and closes it unused (one
        // stopping) ends the call as well: one wait for the whole call.
        let _ = std::fs::remove_file(&socket);
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let closer = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                drop(stream);
            }
        });
        let started = std::time::Instant::now();
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client
                .over_http()
                .send("POST", "/v1/send", Some(b"{}".to_vec()), "cap", None),
        )
        .await
        .expect("the call kept retrying a server that closes every connection")
        .expect_err("a closing server answered");
        assert!(
            error.contains("not sent") || error.contains("restarted during this call"),
            "{error}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        closer.abort();
    }

    /// The bound on the server's answer does not count the wait for the
    /// server: a call that waited out a restart still has its whole answer
    /// time.
    #[tokio::test]
    async fn a_restart_wait_does_not_eat_the_answer_time() {
        use tokio::io::AsyncWriteExt;
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let listening = socket.clone();
        let server = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
            let listener = tokio::net::UnixListener::bind(&listening).unwrap();
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_head(&mut stream).await;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .unwrap();
            // A second server that never answers.
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_head(&mut stream).await;
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        });
        let (status, body) = super::request_waiting(
            &socket,
            "POST",
            "/v1/send",
            Some(b"{}".to_vec()),
            Some("cap"),
            None,
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(1),
        )
        .await
        .expect("the wait for the server was counted against the answer");
        assert_eq!(status, hyper::StatusCode::OK);
        assert_eq!(body, b"ok");
        let started = std::time::Instant::now();
        let error = super::request_waiting(
            &socket,
            "POST",
            "/v1/send",
            Some(b"{}".to_vec()),
            Some("cap"),
            None,
            std::time::Duration::from_secs(10),
            std::time::Duration::from_millis(400),
        )
        .await
        .expect_err("a server that never answers answered");
        assert!(error.starts_with("the server did not answer"), "{error}");
        assert!(error.contains("may or may not have applied"), "{error}");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        server.abort();
    }

    /// Outside a `CALL` scope (such as stdio startup `whoami_with_harness`),
    /// `Client::send` hoists a single `SocketWait` before its retry loop so a
    /// retry shares what is left of `RECONNECT_WITHIN` instead of starting a
    /// fresh 45 s wait on every iteration.
    #[tokio::test(start_paused = true)]
    async fn client_send_outside_a_call_scope_bounds_its_wait_across_retries() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let listening = socket.clone();
        let server = tokio::spawn(async move {
            // Spend 30 s of the 45 s budget waiting for the first server.
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let listener = tokio::net::UnixListener::bind(&listening).unwrap();
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_head(&mut stream).await;
            // The server restarts again mid-read: unlink and drop without answering.
            let _ = std::fs::remove_file(&listening);
            drop(listener);
            drop(stream);
        });
        let client = super::Client::new(socket, dir.path().into(), "cap".into());
        let started = tokio::time::Instant::now();
        let error = client
            .send("GET", "/v1/whoami", None, "cap", None)
            .await
            .expect_err("an unanswered retry succeeded");
        assert!(error.contains("not sent"), "{error}");
        let elapsed = started.elapsed();
        // Shared wait: 30 s + remaining 15 s = 45 s (not 30 s + fresh 45 s = 75 s).
        assert!(
            elapsed >= super::RECONNECT_WITHIN
                && elapsed < super::RECONNECT_WITHIN + std::time::Duration::from_secs(5),
            "elapsed {elapsed:?}"
        );
        server.abort();
    }
}

#[cfg(test)]
mod client_link_tests {
    use super::*;
    use axum::serve::ListenerExt as _;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    /// A stand-in for the server: counts the connections it accepted and
    /// records the capability each call presented.
    struct Recorder {
        accepted: Arc<AtomicUsize>,
        presented: Arc<Mutex<Vec<String>>>,
    }

    /// Serve on `socket` from whichever runtime is current.
    fn serve(socket: &Path) -> (Recorder, impl std::future::Future<Output = ()> + use<>) {
        let accepted = Arc::new(AtomicUsize::new(0));
        let presented = Arc::new(Mutex::new(Vec::new()));
        let counted = Arc::clone(&accepted);
        let listener = tokio::net::UnixListener::bind(socket)
            .unwrap()
            .tap_io(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
            });
        let recorded = Arc::clone(&presented);
        let app = axum::Router::new().fallback(move |headers: axum::http::HeaderMap| {
            let recorded = Arc::clone(&recorded);
            async move {
                if let Some(token) = headers
                    .get(crate::operator::AGENT_HEADER)
                    .and_then(|value| value.to_str().ok())
                {
                    recorded.lock().unwrap().push(token.to_string());
                }
                axum::Json(serde_json::json!({"ok": true}))
            }
        });
        let server = async move {
            let _ = axum::serve(listener, app).await;
        };
        (
            Recorder {
                accepted,
                presented,
            },
            server,
        )
    }

    async fn call(client: &Client) -> serde_json::Value {
        client
            .request_method("GET", "/v1/whoami", None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn sequential_calls_share_one_connection_and_parallel_calls_open_what_they_need() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let (recorder, server) = serve(&socket);
        let server = tokio::spawn(server);
        let client = Client::new(socket, dir.path().join("config"), "cap".into());
        for _ in 0..5 {
            assert_eq!(call(&client).await["ok"], true);
        }
        assert_eq!(recorder.accepted.load(Ordering::SeqCst), 1);

        let calls = (0..8).map(|_| call(&client));
        for answer in futures::future::join_all(calls).await {
            assert_eq!(answer["ok"], true);
        }
        let opened = recorder.accepted.load(Ordering::SeqCst);
        assert!((1..=9).contains(&opened), "opened {opened}");
        assert!(locked(&client.link.idle).len() <= MAX_IDLE_CONNECTIONS);
        // The connections kept after the burst serve the next calls.
        call(&client).await;
        assert_eq!(recorder.accepted.load(Ordering::SeqCst), opened);
        server.abort();
    }

    #[tokio::test]
    async fn a_restarted_server_is_reached_on_a_new_connection() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let client = Client::new(socket.clone(), dir.path().join("config"), "cap".into());

        // The first server runs on its own runtime, so stopping that runtime
        // closes its connections the way a server process exit does.
        let first = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (before, server) = {
            let _entered = first.enter();
            serve(&socket)
        };
        first.spawn(server);
        call(&client).await;
        assert_eq!(before.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(locked(&client.link.idle).len(), 1);
        first.shutdown_background();

        let deadline = Instant::now() + Duration::from_secs(5);
        while !locked(&client.link.idle)
            .iter()
            .all(|sender| sender.is_closed())
        {
            assert!(
                Instant::now() < deadline,
                "the closed connection was never seen"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        std::fs::remove_file(&socket).unwrap();
        let (after, server) = serve(&socket);
        let server = tokio::spawn(server);
        assert_eq!(call(&client).await["ok"], true);
        assert_eq!(after.accepted.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn every_call_presents_the_capability_the_file_holds_now() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("anyagents.sock");
        let config = dir.path().join("config");
        std::fs::create_dir_all(&config).unwrap();
        let file = config.join(CAPABILITY_FILE);
        std::fs::write(&file, "first\n").unwrap();
        let (recorder, server) = serve(&socket);
        let server = tokio::spawn(server);
        let client = Client::new(socket, config.clone(), "at-start".into());

        call(&client).await;
        call(&client).await;
        // A rotation writes a new file and renames it into place.
        let staged = config.join("mcp-capability.new");
        std::fs::write(&staged, "second\n").unwrap();
        std::fs::rename(&staged, &file).unwrap();
        call(&client).await;
        // A rewrite in place that changes the size is seen too.
        std::fs::write(&file, "third-and-longer\n").unwrap();
        call(&client).await;
        // With no readable file the capability read at start is presented.
        std::fs::remove_file(&file).unwrap();
        call(&client).await;
        assert_eq!(
            *recorder.presented.lock().unwrap(),
            ["first", "first", "second", "third-and-longer", "at-start"]
        );
        server.abort();
    }
}
