//! Bounded reattachment of viewers whose authenticated pin outlived the registry.

use super::*;
use crate::sandbox_exec::takeover_pin::{self, Rejection};

pub(super) type ViewerError = (StatusCode, &'static str);
pub(super) const NOT_FOUND: ViewerError = (StatusCode::NOT_FOUND, "takeover session not found");
pub(super) const GONE: ViewerError = (StatusCode::GONE, "takeover lease expired");
pub(super) const UNAVAILABLE: ViewerError = (
    StatusCode::SERVICE_UNAVAILABLE,
    "cannot verify takeover lease",
);
pub(super) const RECONNECTING: ViewerError =
    (StatusCode::SERVICE_UNAVAILABLE, "takeover reconnecting");

// Also check for revocation by another process (which cannot notify this registry).
const RECHECK_INTERVAL: Duration = Duration::from_millis(500);

pub(super) async fn wait(
    registry: &TakeoverRegistry,
    store: &Arc<Store>,
    sid: &str,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<Arc<TakeoverSession>, ViewerError> {
    let tokens: Vec<_> = viewer_credentials(sid, headers, uri)
        .map(|(token, _)| token)
        .filter(|t| is_valid_takeover_token(t))
        .take(4)
        .map(|t| t.into_owned())
        .collect();
    if tokens.is_empty() {
        return Err(NOT_FOUND);
    }
    let store = Arc::clone(store);
    let lookup_sid = sid.to_owned();
    let lookup = move |tokens: Vec<String>| {
        let (store, sid) = (Arc::clone(&store), lookup_sid.clone());
        tokio::task::spawn_blocking(move || {
            for token in tokens {
                match takeover_pin::viewer_deadline(&store, &sid, &token) {
                    Ok(expiry) => return Ok((token, expiry)),
                    Err(Rejection::Unauthorized) => continue,
                    Err(Rejection::Expired) => return Err(GONE),
                    Err(Rejection::Ended | Rejection::Occupied) => return Err(NOT_FOUND),
                    Err(Rejection::Store) => return Err(UNAVAILABLE),
                }
            }
            Err(NOT_FOUND)
        })
    };
    let deadline = tokio::time::Instant::now() + RECONNECT_WAIT_CAP;
    let mut candidates = tokens;
    loop {
        let inserted = registry.inserted.notified();
        tokio::pin!(inserted);
        inserted.as_mut().enable();
        // No tunnel wait before the durable check succeeds, and never wait on an
        // unreadable pin. Reauthorize before returning a retryable timeout.
        let verify = lookup(candidates);
        let (token, expiry) = tokio::time::timeout_at(deadline + Duration::from_secs(1), verify)
            .await
            .map_err(|_| UNAVAILABLE)?
            .map_err(|_| UNAVAILABLE)??;
        if let Some(entry) = registry.get_active(sid).await {
            return Ok(entry); // The caller authenticates against this entry too.
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(RECONNECTING);
        }
        let remaining = expiry.saturating_sub(anyagents_core::now_ms()).max(0) as u64;
        let next = deadline.min(now + RECHECK_INTERVAL.min(Duration::from_millis(remaining)));
        let _ = tokio::time::timeout_at(next, inserted).await;
        candidates = vec![token];
    }
}

pub(super) fn retry_response(html: bool) -> Response {
    let mut response = if html {
        recovery::page(
            StatusCode::SERVICE_UNAVAILABLE,
            "Reconnecting",
            "Reconnecting to desktop…",
            "This page will retry automatically.",
            "",
            r#"<script defer src="/assets/takeover-retry.js"></script>"#,
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(
                serde_json::json!({"error": "takeover reconnecting", "retry_after_secs": 1}),
            ),
        )
            .into_response()
    };
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    recovery::no_store(response)
}
