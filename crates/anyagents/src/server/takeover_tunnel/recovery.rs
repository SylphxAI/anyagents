//! Viewer recovery is a separate operation from viewing or reconnecting a tunnel.

use agent_tools::sha256_hex;
use anyagents_core::{Agent, AgentId, Error, Incoming, TakeoverLink, now_ms};
use axum::http::Method;
use axum::response::Html;

use super::*;
use crate::server::{ApiError, admin_auth, console_html, spawn_store};

const RECOVERY_WINDOW_MS: i64 = 120_000;
const NOT_FOUND: &str = "takeover session not found";

#[derive(Clone, Copy, Default)]
pub(super) struct ConsoleAvailable(pub bool);

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/takeover/{session_id}/_reopen", get(status).post(reopen))
        .route(
            "/assets/takeover-recovery.js",
            get(|| async { javascript(include_str!("recovery.js")) }),
        )
}

pub(super) async fn remember_link(
    store: &Arc<Store>,
    owner: &TunnelOwner,
    session_id: &str,
    token: &str,
    pin: Option<&LeasePin>,
) {
    let TunnelOwner::Agent(agent) = owner else {
        return;
    };
    let (store, agent, sid, hash) = (
        Arc::clone(store),
        agent.clone(),
        session_id.to_owned(),
        sha256_hex(token.as_bytes()),
    );
    let expiry = pin.map(LeasePin::expires_ms);
    // A recovery-store fault must not take the existing viewer transport down.
    if !matches!(
        tokio::task::spawn_blocking(
            move || store.remember_takeover_link(&sid, &hash, &agent, expiry)
        )
        .await,
        Ok(Ok(()))
    ) {
        eprintln!("Could not record takeover link for recovery");
    }
}

pub(super) async fn end_link(store: Option<Arc<Store>>, session_id: &str, token: &str) {
    let Some(store) = store else { return };
    let (sid, hash) = (session_id.to_owned(), sha256_hex(token.as_bytes()));
    if !matches!(
        tokio::task::spawn_blocking(move || store.end_takeover_link(&sid, &hash)).await,
        Ok(Ok(()))
    ) {
        eprintln!("Could not retire takeover link for recovery");
    }
}

#[derive(Clone, Copy)]
enum Retired {
    Expired,
    Ended,
}

pub(super) struct Link {
    pub record: TakeoverLink,
    pub token: String,
    retired: Option<Retired>,
}

fn link_token(sid: &str, headers: &HeaderMap, uri: &Uri) -> Option<String> {
    let from_query = uri.query().and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes())
            .find(|(key, _)| key == "token")
            .map(|(_, token)| token.into_owned())
    });
    from_query
        .or_else(|| admin_auth::extract_cookie(headers, &format!("{COOKIE_PREFIX}{sid}")))
        .filter(|token| is_valid_takeover_token(token))
}

pub(super) async fn lookup(
    state: &AppState,
    sid: &str,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<Option<Link>, ApiError> {
    if !is_valid_takeover_session_id(sid) {
        return Ok(None);
    }
    let Some(token) = link_token(sid, headers, uri) else {
        return Ok(None);
    };
    let (store, sid) = (state.store.clone(), sid.to_owned());
    let hash = sha256_hex(token.as_bytes());
    spawn_store(move || {
        store
            .takeover_link(&sid, &hash)
            .map(|record| {
                record.map(|record| {
                    let retired = if record
                        .ended_ms
                        .is_some_and(|ended| record.expires_ms.is_none_or(|expiry| ended < expiry))
                    {
                        Some(Retired::Ended)
                    } else if record.expires_ms.is_some_and(|expiry| expiry <= now_ms()) {
                        Some(Retired::Expired)
                    } else {
                        None
                    };
                    Link {
                        record,
                        token,
                        retired,
                    }
                })
            })
            .map_err(|_| unavailable())
    })
    .await
}

pub(super) fn unavailable() -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "Could not verify this takeover link. Try again shortly.",
    )
}

fn prefers_html(headers: &HeaderMap) -> bool {
    let mut html: f32 = 0.0;
    let mut json: f32 = 0.0;
    for value in headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
    {
        for part in value.split(',') {
            let mut segments = part.split(';').map(str::trim);
            let media = segments.next().unwrap_or_default().to_ascii_lowercase();
            let q = segments
                .filter_map(|s| s.split_once('='))
                .find(|(k, _)| k.trim().eq_ignore_ascii_case("q"))
                .map_or(1.0, |(_, q)| {
                    q.trim()
                        .parse::<f32>()
                        .ok()
                        .filter(|n| (0.0..=1.0).contains(n))
                        .unwrap_or(0.0)
                });
            if media == "text/html" {
                html = html.max(q);
            }
            if media == "application/json" || media.ends_with("+json") {
                json = json.max(q);
            }
        }
    }
    html > 0.0 && html >= json
}

pub(super) async fn viewer_error(
    state: &AppState,
    sid: &str,
    method: &Method,
    headers: &HeaderMap,
    uri: &Uri,
    error: (StatusCode, &'static str),
    console: ConsoleAvailable,
) -> Response {
    if *method != Method::GET
        || !prefers_html(headers)
        || !matches!(
            error.0,
            StatusCode::UNAUTHORIZED | StatusCode::NOT_FOUND | StatusCode::GONE
        )
    {
        return no_store(json_error_response(error.0, error.1));
    }
    let link = match lookup(state, sid, headers, uri).await {
        Ok(link) => link,
        Err(_) => {
            return page(
                StatusCode::SERVICE_UNAVAILABLE,
                "Unavailable",
                "Couldn't verify this link",
                "Try opening the link again in a moment.",
                "",
                "",
            );
        }
    };
    let Some(link) = link else {
        return page(
            StatusCode::NOT_FOUND,
            "Invalid link",
            "Takeover not found",
            "Check that you opened the full link, or ask the agent for a new takeover link.",
            "",
            "",
        );
    };
    let Some(retired) = link.retired else {
        return page(
            StatusCode::SERVICE_UNAVAILABLE,
            "Unavailable",
            "Desktop not connected",
            "This link is known, but the desktop isn’t available here. Try again shortly.",
            "",
            "",
        );
    };
    let (badge, title, description) = match retired {
        Retired::Expired => (
            "Expired",
            "This takeover expired",
            "The desktop link reached its time limit. Reopen it to start a new takeover for the same agent.",
        ),
        Retired::Ended => (
            "Ended",
            "This takeover ended",
            "This link no longer controls the desktop. Reopen it to request a new takeover for the same agent.",
        ),
    };
    let signed_in = admin_auth::extract_session_cookie(headers)
        .is_some_and(|raw| admin_auth::verify_session_cookie_from_env(&raw).is_ok());
    let mut agent = String::new();
    let action = if signed_in {
        let (store, id) = (state.store.clone(), link.record.agent_id);
        if let Ok(Ok(row)) = tokio::task::spawn_blocking(move || store.agent(&id)).await {
            agent = format!(
                "<dl><dt>Agent</dt><dd>{}</dd></dl>",
                console_html::html_escape(&row.name)
            );
        }
        let login = console.0 && admin_auth::is_oidc_configured();
        format!(
            r#"<button id="reopen" data-url="/takeover/{sid}/_reopen" data-login="{login}" type="button">Reset &amp; Reopen Takeover</button><noscript><p>Enable JavaScript to reopen this takeover.</p></noscript>"#
        )
    } else if console.0 && admin_auth::is_oidc_configured() {
        let next: String = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("next", &format!("/takeover/{sid}/"))
            .finish();
        format!(
            r#"<a id="reopen-sign-in" class="button" href="/admin/auth/login?{next}">Sign in to reopen</a>"#
        )
    } else {
        "<p>Ask a signed-in operator for a new link.</p>".into()
    };
    let mut response = page(StatusCode::GONE, badge, title, description, &agent, &action);
    // Keep the old credential scoped to this path across OIDC; never put it
    // in the login URL, rendered markup or recovery wake message.
    let secure = if forwarded_proto(headers) == Some("https")
        || serverless::resolve_public_url(state.store.data_dir())
            .is_some_and(|url| url.starts_with("https://"))
    {
        "; Secure"
    } else {
        ""
    };
    if let Ok(cookie) = HeaderValue::from_str(&format!(
        "{COOKIE_PREFIX}{sid}={}; Path=/takeover/{sid}/; HttpOnly; SameSite=Lax; Max-Age=3600{secure}",
        link.token
    )) {
        response.headers_mut().append(header::SET_COOKIE, cookie);
    }
    response
}

pub(super) fn page(
    status: StatusCode,
    badge: &str,
    title: &str,
    description: &str,
    agent: &str,
    action: &str,
) -> Response {
    // A single replacement pass so escaped agent names cannot become template directives.
    let template = include_str!("recovery.html");
    let body = template
        .split("<!--slot-->")
        .zip(["", badge, title, description, agent, action, ""])
        .fold(String::new(), |mut html, (part, before)| {
            html.push_str(before);
            html.push_str(part);
            html
        });
    let mut response = no_store((status, Html(body)).into_response());
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(
        "default-src 'none'; style-src 'unsafe-inline'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

pub(super) fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept, Cookie"));
    response.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

async fn authorized_link(
    state: &AppState,
    sid: &str,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<AgentId, ApiError> {
    let Some(link) = lookup(state, sid, headers, uri).await? else {
        return Err(ApiError::new(StatusCode::NOT_FOUND, NOT_FOUND));
    };
    if link.retired.is_none() {
        return Err(ApiError::conflict(
            "This takeover has not ended. Open the original link to reconnect.",
        ));
    }
    Ok(link.record.agent_id)
}

fn agent_available(store: &Store, id: &AgentId) -> Result<Agent, ApiError> {
    let agent = store.agent(id).map_err(|e| match e {
        Error::AgentNotFound(_) => ApiError::new(
            StatusCode::NOT_FOUND,
            "This agent was deleted or no longer exists.",
        ),
        _ => unavailable(),
    })?;
    if crate::server::is_explicitly_archived(store.data_dir(), &agent) || agent.state == "archived"
    {
        return Err(ApiError::conflict(
            "This agent is archived. Restore it before reopening a takeover.",
        ));
    }
    if !matches!(agent.state.as_str(), "idle" | "running" | "created") {
        return Err(ApiError::conflict(
            "This agent is stopped or paused. Start it before reopening a takeover.",
        ));
    }
    Ok(agent)
}

async fn live_url(state: &AppState, id: &AgentId, original: &str) -> Option<String> {
    let registry = registry_for_store(&state.store);
    let mut candidates: Vec<_> = registry
        .sessions
        .read()
        .await
        .iter()
        .filter(|(sid, session)| {
            sid.as_str() != original
                && matches!(&session.owner, TunnelOwner::Agent(owner) if owner == id)
        })
        .map(|(sid, session)| (sid.clone(), session.clone()))
        .collect();
    candidates.sort_unstable_by_key(|(_, entry)| std::cmp::Reverse(entry.created));
    for (sid, entry) in candidates {
        if *entry.returned.borrow() {
            continue;
        }
        if entry
            .conn
            .read()
            .await
            .tx
            .as_ref()
            .is_none_or(mpsc::Sender::is_closed)
        {
            continue;
        }
        if let Some(pin) = &entry.pin
            && pin.check().await.is_err()
        {
            continue;
        }
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("token", &entry.token)
            .finish();
        // Never forward the new credential to a host derived from guest registration headers.
        return Some(format!("/takeover/{sid}/?{query}"));
    }
    None
}

// Use the exact existing operator authority, but disallow the local trusted
// fallback, which can infer Operator from the CSRF header alone.
fn require_recovery_operator(
    state: &AppState,
    principal: Principal,
    headers: &HeaderMap,
    post: bool,
) -> Result<(), ApiError> {
    crate::server::require_operator(&principal)?;
    if state.auth.is_trusted()
        && !admin_auth::extract_session_cookie(headers)
            .is_some_and(|raw| admin_auth::verify_session_cookie_from_env(&raw).is_ok())
    {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "Sign in as an operator to reopen this takeover.",
        ));
    }
    if post
        && headers
            .get(admin_auth::ADMIN_CSRF_HEADER)
            .is_none_or(|v| v != "1")
    {
        return Err(ApiError::forbidden("Missing required CSRF header."));
    }
    Ok(())
}

fn progress(url: Option<String>) -> Response {
    let status = if url.is_some() {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    (status, axum::Json(serde_json::json!({ "url": url }))).into_response()
}

async fn status(
    principal: Result<Principal, crate::auth::AuthRejection>,
    State(state): State<AppState>,
    AxumPath(sid): AxumPath<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let result: Result<_, ApiError> = async {
        require_recovery_operator(&state, principal.map_err(auth_error)?, &headers, false)?;
        let agent = authorized_link(&state, &sid, &headers, &uri).await?;
        let (store, id) = (state.store.clone(), agent.clone());
        spawn_store(move || agent_available(&store, &id)).await?;
        Ok(progress(live_url(&state, &agent, &sid).await))
    }
    .await;
    no_store(result.into_response())
}

fn auth_error(crate::auth::AuthRejection(err): crate::auth::AuthRejection) -> ApiError {
    ApiError::new(err.status(), err.message())
}

async fn reopen(
    principal: Result<Principal, crate::auth::AuthRejection>,
    State(state): State<AppState>,
    AxumPath(sid): AxumPath<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let result: Result<_, ApiError> = async {
        require_recovery_operator(&state, principal.map_err(auth_error)?, &headers, true)?;
        let agent = authorized_link(&state, &sid, &headers, &uri).await?;
        let (store, id) = (state.store.clone(), agent.clone());
        spawn_store(move || agent_available(&store, &id)).await?;
        if let Some(url) = live_url(&state, &agent, &sid).await { return Ok(progress(Some(url))); }
        enqueue(&state, agent.clone(), |store, id| {
            let bucket = now_ms().div_euclid(RECOVERY_WINDOW_MS);
            for previous in [bucket, bucket - 1] {
                let existing = store.message_by_source_key(id, &format!("takeover-reopen:{previous}")).map_err(|_| unavailable())?;
                if existing.is_some_and(|msg| now_ms().saturating_sub(msg.received_at_ms) < RECOVERY_WINDOW_MS) { return Ok(None); }
            }
            Ok(Some((format!("takeover-reopen:{bucket}"),
                "[Browser takeover reopen] An operator requested a new desktop takeover. Call browser_takeover to open a fresh remote desktop link; do not wait for a chat reply.".into())))
        }).await?;
        wake(&state, agent);
        Ok(progress(None))
    }.await;
    no_store(result.into_response())
}

pub(super) async fn enqueue(
    state: &AppState,
    agent: AgentId,
    message: impl FnOnce(&Store, &AgentId) -> Result<Option<(String, String)>, ApiError>
    + Send
    + 'static,
) -> Result<(), ApiError> {
    let store = state.store.clone();
    spawn_store(move || {
        let id = agent;
        let _guard = store.lock_agent_lifecycle(&id).map_err(|_| unavailable())?;
        agent_available(&store, &id)?;
        let Some((key, text)) = message(&store, &id)? else {
            return Ok(());
        };
        let mut incoming = Incoming::new(text);
        incoming.sender = Some("operator".into());
        incoming.source_key = Some(key);
        store.receive(&id, incoming).map_err(|_| unavailable())?;
        Ok(())
    })
    .await
}

// Mailbox admission above is durable. Retry the wake even after a duplicate
// receipt so a failed dispatch attempt cannot strand an otherwise idle owner.
pub(super) fn wake(state: &AppState, agent: AgentId) {
    crate::create::poke_agent_wake_socket(state.store.data_dir(), &agent);
    crate::create::poke_agent_wake_socket_mail(state.store.data_dir(), &agent);
    if serverless::is_serverless_mode() {
        let store = state.store.clone();
        let timer = state.timer_backend.clone();
        let call = crate::server::in_flight::carry();
        tokio::spawn(async move {
            let _call = call;
            if let Err(err) =
                serverless::dispatch_next_mailbox_message_with_timer(store, &agent, Some(timer))
                    .await
            {
                eprintln!("Could not dispatch takeover wake: {err}");
            }
        });
    }
}

// Both scripts are static, same-origin, and contain no credentials.
pub(super) fn javascript(script: &'static str) -> Response {
    no_store(
        (
            [
                (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            ],
            script,
        )
            .into_response(),
    )
}
