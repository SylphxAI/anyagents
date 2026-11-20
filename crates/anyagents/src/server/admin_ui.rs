//! Embedded Admin Console (`/admin`) static SPA and operator overview endpoints.
//!
//! Serves zero-build HTML/CSS/JS directly from the binary with strict security
//! headers (`Content-Security-Policy`, `X-Frame-Options: DENY`,
//! `X-Content-Type-Options: nosniff`) on both local (`build_router`) and Pure
//! Serverless (`build_webhook_router`) listeners.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use serde_json::{Map, Value, json};

use crate::auth::Principal;

use super::{ApiError, AppState, admin_auth, require_operator, serverless, spawn_store};

const INDEX_HTML: &str = include_str!("../../assets/admin/index.html");
const APP_JS: &str = include_str!("../../assets/admin/app.js");
const SECRETS_JS: &str = include_str!("../../assets/admin/secrets.js");
const STYLES_CSS: &str = include_str!("../../assets/admin/styles.css");

const ADMIN_CSP: &str =
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'";

fn static_asset_response(content_type: &'static str, body: &'static str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .header(CACHE_CONTROL, "no-store")
        .header(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static(ADMIN_CSP),
        )
        .header(
            HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("DENY"),
        )
        .header(
            HeaderName::from_static("x-content-type-options"),
            HeaderValue::from_static("nosniff"),
        )
        .header(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("no-referrer"),
        )
        .body(Body::from(body))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

pub(crate) async fn index_route() -> Response {
    static_asset_response("text/html; charset=utf-8", INDEX_HTML)
}

pub(crate) async fn app_js_route() -> Response {
    static_asset_response("application/javascript; charset=utf-8", APP_JS)
}

pub(crate) async fn secrets_js_route() -> Response {
    static_asset_response("application/javascript; charset=utf-8", SECRETS_JS)
}

pub(crate) async fn styles_css_route() -> Response {
    static_asset_response("text/css; charset=utf-8", STYLES_CSS)
}

/// `GET /admin/api/overview`: operator-only summary supplementing `/v1/agents` with
/// per-agent `unread_count`, `sandbox_lease`, and serverless tick status.
pub(crate) async fn overview_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_operator(&principal)?;
    let store = Arc::clone(&state.store);
    spawn_store(move || {
        let agents = store.list_agents().map_err(ApiError::from_store)?;
        let rts = store.remote_turn_store();
        let mut agents_meta = Map::new();
        let mut active_remote_turns = Vec::new();

        for agent in &agents {
            let unread_count = if agent.state == "archived" {
                0
            } else {
                store
                    .list_undelivered(&agent.id, 100)
                    .map(|msgs| msgs.len())
                    .unwrap_or(0)
            };
            let sandbox_lease = store.sandbox_binding(&agent.id).ok().flatten().map(|b| {
                json!({
                    "provider": b.provider,
                    "lease_state": b.lease_state,
                    "lease_id": b.lease_id,
                    "volume_id": b.volume_id,
                    "lease_expires_at_ms": b.lease_expires_at_ms,
                    "idle_since_ms": b.idle_since_ms,
                    "last_heartbeat_ms": b.last_heartbeat_ms,
                })
            });
            agents_meta.insert(
                agent.id.as_str().to_string(),
                json!({
                    "unread_count": unread_count,
                    "sandbox_lease": sandbox_lease,
                }),
            );

            if agent.state != "archived"
                && let Ok(turns) = rts.list_active_for_agent(&agent.id)
            {
                for turn in turns {
                    active_remote_turns.push(json!({
                        "turn_id": turn.turn_id,
                        "agent_id": turn.agent_id.as_str(),
                        "agent_name": agent.name,
                        "harness_kind": turn.harness_kind,
                        "provider_kind": turn.provider_kind,
                        "status": turn.status,
                        "created_at_ms": turn.created_at_ms,
                        "last_event_at_ms": turn.last_event_at_ms,
                        "next_poll_at_ms": turn.next_poll_at_ms,
                    }));
                }
            }
        }

        let active_sandboxes_count = store
            .sandbox_binding_store()
            .list_active_ready()
            .map(|v| v.len())
            .unwrap_or(0);
        let sandbox_provider = std::env::var("ANYAGENTS_SANDBOX_PROVIDER")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let tick_token_configured = serverless::configured_tick_token().is_some();
        let tick_jwks_configured = std::env::var("ANYAGENTS_TICK_JWKS_URL")
            .ok()
            .is_some_and(|s| !s.trim().is_empty());

        Ok(Json(json!({
            "serverless": serverless::is_serverless_mode(),
            "public_url": serverless::resolve_public_url(store.data_dir()),
            "sandbox_provider": sandbox_provider,
            "tick_token_configured": tick_token_configured,
            "tick_jwks_configured": tick_jwks_configured,
            "active_sandboxes_count": active_sandboxes_count,
            "agents_meta": Value::Object(agents_meta),
            "active_remote_turns": active_remote_turns,
        })))
    })
    .await
}

/// `POST /admin/api/tick`: operator-authenticated manual trigger for one bounded
/// serverless maintenance & schedule tick pass.
pub(crate) async fn trigger_tick_route(
    principal: Principal,
    State(state): State<AppState>,
) -> Result<Json<serverless::TickSummary>, ApiError> {
    require_operator(&principal)?;
    let summary = serverless::run_tick_pass(Arc::clone(&state.store), state.health.clone()).await?;
    Ok(Json(summary))
}

/// Mount all `/admin` SPA, OIDC auth, and operator overview routes on a router.
pub(crate) fn mount_admin_routes(router: Router<AppState>) -> Router<AppState> {
    router
        .route("/admin", get(index_route))
        .route("/admin/", get(index_route))
        .route("/admin/assets/app.js", get(app_js_route))
        .route("/admin/assets/secrets.js", get(secrets_js_route))
        .route("/admin/assets/styles.css", get(styles_css_route))
        .route("/admin/auth/login", get(admin_auth::login_route))
        .route("/admin/auth/callback", get(admin_auth::callback_route))
        .route("/admin/auth/logout", post(admin_auth::logout_route))
        .route("/admin/auth/me", get(admin_auth::me_route))
        .route("/admin/api/overview", get(overview_route))
        .route("/admin/api/tick", post(trigger_tick_route))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::Store;
    use anyagents_supervisor::HealthBoard;
    use axum::http::Request;
    use http_body_util::BodyExt as _;
    use std::sync::Mutex;
    use tower::ServiceExt as _;

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

    fn make_state(store: Arc<Store>, auth: crate::auth::Mode) -> AppState {
        AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(super::super::ProcessRunner)),
            auth,
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn admin_console_serves_csp_html_and_fails_closed_on_api_routes() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_OPERATOR_TOKEN",
            "ANYAGENTS_OPERATOR_TOKEN_FILE",
            "ANYAGENTS_ADMIN_OIDC_ISSUER",
            "ANYAGENTS_ADMIN_OIDC_CLIENT_ID",
            "ANYAGENTS_ADMIN_EMAILS",
            "ANYAGENTS_ADMIN_SESSION_SECRET",
            "ANYAGENTS_PUBLIC_URL",
        ]);

        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "true");
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN");
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
            std::env::set_var("ANYAGENTS_ADMIN_OIDC_ISSUER", "https://idp.example.com");
            std::env::set_var("ANYAGENTS_ADMIN_OIDC_CLIENT_ID", "console-client");
            std::env::set_var("ANYAGENTS_ADMIN_EMAILS", "ops@example.com");
            std::env::set_var(
                "ANYAGENTS_ADMIN_SESSION_SECRET",
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            );
            std::env::set_var("ANYAGENTS_PUBLIC_URL", "https://console.example.com");
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let auth = crate::auth::Mode::from_env(dir.path());
        assert!(auth.is_confined());
        let app = super::super::build_webhook_router(make_state(Arc::clone(&store), auth));

        // 1. GET /admin returns 200 HTML with strict CSP, X-Frame-Options, nosniff.
        let html_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(html_resp.status(), StatusCode::OK);
        assert_eq!(
            html_resp
                .headers()
                .get("content-security-policy")
                .and_then(|v| v.to_str().ok()),
            Some(ADMIN_CSP)
        );
        assert_eq!(
            html_resp
                .headers()
                .get("x-frame-options")
                .and_then(|v| v.to_str().ok()),
            Some("DENY")
        );
        assert_eq!(
            html_resp
                .headers()
                .get("x-content-type-options")
                .and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );
        let html_body = html_resp.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&html_body).contains("AnyAgents Admin Console"));

        // 2. GET /v1/agents and GET /admin/api/overview fail closed (401) without cookie/token.
        let unauth_agents = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth_agents.status(), StatusCode::UNAUTHORIZED);

        let unauth_overview = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/api/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth_overview.status(), StatusCode::UNAUTHORIZED);

        // 3. GET /admin/auth/me reports unauthenticated OIDC mode without leaking secrets.
        let me_unauth = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/auth/me")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(me_unauth.status(), StatusCode::OK);
        let me_json: Value =
            serde_json::from_slice(&me_unauth.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(me_json["authenticated"], json!(false));
        assert_eq!(me_json["mode"], json!("oidc"));
        assert_eq!(me_json["serverless"], json!(true));

        // 4. With valid signed session cookie, GET /v1/agents & /admin/api/overview return 200.
        let key = admin_auth::session_signing_key_from_env().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let cookie_val = admin_auth::sign_admin_session(
            &key,
            &admin_auth::AdminSession {
                email: "ops@example.com".to_string(),
                name: Some("Ops".to_string()),
                sub: "sub-ops".to_string(),
                iat: now,
                exp: now + 3600,
            },
        )
        .unwrap();
        let cookie_hdr = format!("{}={cookie_val}", admin_auth::ADMIN_SESSION_COOKIE);

        let authed_agents = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .header(axum::http::header::COOKIE, &cookie_hdr)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authed_agents.status(), StatusCode::OK);

        let authed_overview = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/api/overview")
                    .header(axum::http::header::COOKIE, &cookie_hdr)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authed_overview.status(), StatusCode::OK);

        // 5. POST /admin/api/tick rejects missing CSRF header (403), accepts with CSRF header (200).
        let no_csrf_tick = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/tick")
                    .header(axum::http::header::COOKIE, &cookie_hdr)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_csrf_tick.status(), StatusCode::FORBIDDEN);

        let ok_tick = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/tick")
                    .header(axum::http::header::COOKIE, &cookie_hdr)
                    .header(admin_auth::ADMIN_CSRF_HEADER, "1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_tick.status(), StatusCode::OK);

        // 6. POST /admin/auth/logout requires CSRF header and clears the session cookie.
        let logout_no_csrf = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/auth/logout")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logout_no_csrf.status(), StatusCode::FORBIDDEN);

        let logout_ok = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/auth/logout")
                    .header("x-requested-with", "fetch")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logout_ok.status(), StatusCode::OK);
        let set_cookie = logout_ok
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(set_cookie.contains("anyagents_admin_session=;"));
        assert!(set_cookie.contains("Max-Age=0"));
        assert!(set_cookie.contains("Secure"));
    }
}
