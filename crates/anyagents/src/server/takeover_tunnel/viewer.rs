//! Coordinator-only viewer controls. The workstation retains its existing
//! responsibility for the RFB input gate and local desktop lifecycle.

use agent_tools::sha256_hex;
use axum::body::to_bytes;
use axum::http::Method;

use super::*;
use crate::server::{ApiError, spawn_store};

const ACTION_HEADER: &str = "x-anyagents-takeover-action";
const SCRIPT_TAG: &str = r#"<script type="module" src="app.js"></script>"#;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/takeover/{session_id}/_handback",
            axum::routing::post(handback),
        )
        .route("/takeover/{session_id}/_returned", get(returned))
        .route(
            "/assets/takeover-viewer.js",
            get(|| async { recovery::javascript(include_str!("viewer.js")) }),
        )
}

// The proxy already limits non-SSE guest responses. Only decorate the known
// workstation document, not its scripts, assets, or any arbitrary HTML viewer.
pub(super) async fn add_controls(response: Response) -> Response {
    if response.status() != StatusCode::OK
        || !response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(';').next() == Some("text/html"))
        || response.headers().contains_key(header::CONTENT_ENCODING)
    {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let Ok(bytes) = to_bytes(body, MAX_HTTP_RESP_BYTES).await else {
        return json_error_response(StatusCode::BAD_GATEWAY, "could not read the takeover page");
    };
    if let Ok(html) = std::str::from_utf8(&bytes)
        && html.contains(SCRIPT_TAG)
    {
        let html = html.replacen(
            SCRIPT_TAG,
            concat!(
                r#"<script type="module" src="app.js"></script>"#,
                r#"<script defer src="/assets/takeover-viewer.js"></script>"#
            ),
            1,
        );
        for field in [header::CONTENT_LENGTH, header::ETAG, header::LAST_MODIFIED] {
            parts.headers.remove(field);
        }
        return recovery::no_store(Response::from_parts(parts, Body::from(html)));
    }
    Response::from_parts(parts, Body::from(bytes))
}

fn handback_key(session_id: &str, token: &str) -> String {
    format!(
        "takeover-handback:{session_id}:{}",
        sha256_hex(token.as_bytes())
    )
}

async fn handback(
    State(state): State<AppState>,
    AxumPath(sid): AxumPath<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if headers.get(ACTION_HEADER).is_none_or(|v| v != "handback") {
        return recovery::no_store(json_error_response(
            StatusCode::FORBIDDEN,
            "Missing required takeover action header.",
        ));
    }
    let result = match resolve_and_auth_viewer(&state, &sid, &headers, &uri).await {
        Ok((registry, entry, _)) if check_viewer_origin(&headers, &entry) => {
            // Once accepted, a dropped browser request must not cancel the wake
            // between release and durable mailbox admission.
            let call = super::super::in_flight::carry();
            tokio::spawn(async move {
                let _call = call;
                finish(&state, &sid, registry, entry).await
            })
            .await
            .unwrap_or_else(|_| Err(recovery::unavailable()))
        }
        Ok(_) => Err(ApiError::forbidden("Cross-origin takeover request denied.")),
        Err(error) => receipt(&state, &sid, &headers, &uri, error).await,
    };
    recovery::no_store(match result {
        Ok(()) => axum::Json(serde_json::json!({ "handed_back": true })).into_response(),
        Err(err) => err.into_response(),
    })
}

async fn receipt(
    state: &AppState,
    sid: &str,
    headers: &HeaderMap,
    uri: &Uri,
    error: (StatusCode, &'static str),
) -> Result<(), ApiError> {
    let Some(link) = recovery::lookup(state, sid, headers, uri).await? else {
        return Err(ApiError::new(error.0, error.1));
    };
    let store = state.store.clone();
    let key = handback_key(sid, &link.token);
    spawn_store(move || {
        if store
            .message_by_source_key(&link.record.agent_id, &key)
            .map_err(|_| recovery::unavailable())?
            .is_some()
        {
            Ok(())
        } else {
            Err(ApiError::new(error.0, error.1))
        }
    })
    .await
}

async fn finish(
    state: &AppState,
    sid: &str,
    registry: Arc<TakeoverRegistry>,
    entry: Arc<TakeoverSession>,
) -> Result<(), ApiError> {
    // /take and /release must not race. The old viewer token stops authorizing
    // mutations when the upstream gate has acknowledged release.
    let mut released = entry.control.lock().await;
    if !*released {
        let response = proxy_viewer_http(
            registry,
            entry.clone(),
            Method::POST.to_string(),
            "/release".into(),
            None,
            Vec::new(),
            Body::empty(),
            None,
        )
        .await;
        if !response.status().is_success() {
            return Err(ApiError::new(
                response.status(),
                "Could not confirm desktop handback. Try again.",
            ));
        }
        let body = tokio::time::timeout(HTTP_HEAD_TIMEOUT, to_bytes(response.into_body(), 8192))
            .await
            .ok()
            .and_then(Result::ok)
            .and_then(|body| serde_json::from_slice::<serde_json::Value>(&body).ok());
        if body.as_ref().and_then(|value| value["holder"].as_str()) != Some("agent") {
            return Err(ApiError::new(
                StatusCode::BAD_GATEWAY,
                "The desktop did not confirm handback. Try again.",
            ));
        }
        *released = true;
    }
    if let TunnelOwner::Agent(agent) = &entry.owner {
        let key = handback_key(sid, &entry.token);
        recovery::enqueue(state, agent.clone(), move |_, _| Ok(Some((key,
            "[Browser handback] The operator handed the desktop back to you. Resume the task you were working on.".into(),
        )))).await?;
    }
    // Only signal terminal closure after the workstation advanced its gate and
    // the wake was admitted. This lets the guest distinguish handback from TTL.
    if let Some(pin) = &entry.pin {
        pin.end().await;
    }
    recovery::end_link(entry.store.upgrade(), sid, &entry.token).await;
    entry.returned.send_replace(true);
    if let TunnelOwner::Agent(agent) = &entry.owner {
        recovery::wake(state, agent.clone());
    }
    Ok(())
}

async fn returned(
    State(state): State<AppState>,
    AxumPath(sid): AxumPath<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    match receipt(
        &state,
        &sid,
        &headers,
        &uri,
        (StatusCode::NOT_FOUND, "takeover session not found"),
    )
    .await
    {
        Ok(()) => recovery::page(
            StatusCode::OK,
            "Handed back",
            "The agent is back in control",
            "The agent has been notified and can resume work. You can close this tab.",
            "",
            "",
        ),
        Err(error) => recovery::no_store(error.into_response()),
    }
}
