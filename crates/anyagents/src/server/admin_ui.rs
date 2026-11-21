//! Embedded AnyAgents Web Control Plane Console (`/`) static SPA, `/admin`
//! compatibility redirect, OIDC auth route mounting, and operator overview endpoints.
//!
//! Serves modular zero-build HTML/CSS/JS assets directly from `src/server/console/`
//! with strict security headers (`Content-Security-Policy`, `X-Frame-Options: DENY`,
//! `X-Content-Type-Options: nosniff`) on both local (`build_router`) and Pure
//! Serverless (`build_webhook_router`) listeners.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CACHE_CONTROL, LOCATION};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Map, Value, json};

use crate::auth::Principal;

use super::{
    ApiError, AppState, admin_auth, console_html, ops, require_operator, serverless, spawn_store,
};

const INDEX_HTML: &str = include_str!("console/index.html");
const CONSOLE_CSS: &str = include_str!("console/console.css");
const CONSOLE_JS: &str = include_str!("console/console.js");

fn static_asset_response(content_type: &'static str, body: &'static str) -> Response {
    console_html::console_response(StatusCode::OK, content_type, body.to_string())
}

/// `GET /`: serves the Web Control Plane UI.
///
/// - When `Accept` explicitly requests JSON only (e.g. automated API probes),
///   returns the `{"ok": true}` readiness probe.
/// - When OIDC is configured and the caller does not carry a valid operator
///   session cookie, serves the standalone AnyAgents Sign-In page (`signin.html`).
/// - When authenticated or in single-user local mode, serves the Web Control
///   Plane UI (`index.html`).
pub(crate) async fn root_route(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if console_html::accepts_json_only(&headers) {
        return ops::public_healthz().await.into_response();
    }
    if admin_auth::is_oidc_configured() {
        let authenticated = admin_auth::extract_session_cookie(&headers)
            .as_deref()
            .and_then(|raw| admin_auth::verify_session_cookie_from_env(raw).ok())
            .is_some();
        if !authenticated {
            let coordinator_url =
                serverless::resolve_public_url(state.store.data_dir()).or_else(|| {
                    console_html::request_host_header(&headers).map(|host| format!("http://{host}"))
                });
            return console_html::render_signin_response(coordinator_url.as_deref());
        }
    }
    static_asset_response("text/html; charset=utf-8", INDEX_HTML)
}

/// `GET /admin` and `GET /admin/`: clean `302 Found` redirect to `/`.
pub(crate) async fn redirect_to_root_route() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header(LOCATION, "/")
        .header(CACHE_CONTROL, "no-store")
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

pub(crate) async fn console_css_route() -> Response {
    static_asset_response("text/css; charset=utf-8", CONSOLE_CSS)
}

pub(crate) async fn console_js_route() -> Response {
    static_asset_response("application/javascript; charset=utf-8", CONSOLE_JS)
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
        let agents = store.agent_summaries(true).map_err(ApiError::from_store)?;
        let (mut metadata, active_sandboxes_count) = store.agent_overview_metadata(&agents);
        let mut agents_meta = Map::new();
        let mut active_remote_turns = Vec::new();

        for agent in agents {
            let meta = metadata.remove(&agent.id).unwrap_or_default();
            let archived = agent.state == "archived";
            agents_meta.insert(
                agent.id.as_str().to_string(),
                json!({
                    "unread_count": if archived { 0 } else { meta.unread_count },
                    "sandbox_lease": meta.sandbox_lease,
                }),
            );

            if !archived {
                for turn in meta.active_remote_turns {
                    active_remote_turns.push(json!({
                        "turn_id": turn.turn_id,
                        "agent_id": agent.id.as_str(),
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

        let sandbox_provider = std::env::var("ANYAGENTS_SANDBOX_PROVIDER")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let timer_token_configured = super::timer_fire::configured_timer_token().is_some();
        let timer_jwks_configured = super::timer_fire::configured_timer_jwks_url().is_some();
        let timer_scheduler_configured =
            super::timer_fire::configured_timer_scheduler_url().is_some();

        Ok(Json(json!({
            "serverless": serverless::is_serverless_mode(),
            "public_url": serverless::resolve_public_url(store.data_dir()),
            "sandbox_provider": sandbox_provider,
            "timer_token_configured": timer_token_configured,
            "timer_jwks_configured": timer_jwks_configured,
            "timer_scheduler_configured": timer_scheduler_configured,
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

/// Mount the root Web Control Plane UI (`/`), `/admin` compatibility redirects,
/// modular static console assets, OIDC auth routes (`/admin/auth/*` + `/auth/*`),
/// and operator overview endpoints on a router.
pub(crate) fn mount_admin_routes(router: Router<AppState>) -> Router<AppState> {
    router
        .route("/", get(root_route))
        .route("/admin", get(redirect_to_root_route))
        .route("/admin/", get(redirect_to_root_route))
        .route("/assets/console.css", get(console_css_route))
        .route("/assets/console.js", get(console_js_route))
        // Legacy URLs route to the same generated React entry point.
        .route("/assets/profiles.js", get(console_js_route))
        .route("/assets/secrets.js", get(console_js_route))
        .route("/assets/signin.js", get(console_js_route))
        .route("/admin/assets/styles.css", get(console_css_route))
        .route("/admin/assets/app.js", get(console_js_route))
        .route("/admin/assets/secrets.js", get(console_js_route))
        .route("/admin/auth/login", get(admin_auth::login_route))
        .route("/admin/auth/callback", get(admin_auth::callback_route))
        .route("/admin/auth/logout", post(admin_auth::logout_route))
        .route("/admin/auth/me", get(admin_auth::me_route))
        .route("/auth/login", get(admin_auth::login_route))
        .route("/auth/callback", get(admin_auth::callback_route))
        .route("/auth/logout", post(admin_auth::logout_route))
        .route("/auth/me", get(admin_auth::me_route))
        .route("/admin/api/overview", get(overview_route))
        .route("/admin/api/tick", post(trigger_tick_route))
        .merge(super::versions::router())
        .merge(super::runtime_issues::admin_router())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::Store;
    use anyagents_supervisor::HealthBoard;
    use axum::http::Request;
    use axum::http::header::{ACCEPT, COOKIE, HOST, SET_COOKIE};
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
            timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        }
    }

    fn oidc_env_keys() -> &'static [&'static str] {
        &[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_OPERATOR_TOKEN",
            "ANYAGENTS_OPERATOR_TOKEN_FILE",
            "ANYAGENTS_ADMIN_OIDC_ISSUER",
            "ANYAGENTS_ADMIN_OIDC_CLIENT_ID",
            "ANYAGENTS_ADMIN_EMAILS",
            "ANYAGENTS_ADMIN_SESSION_SECRET",
            "ANYAGENTS_PUBLIC_URL",
        ]
    }

    fn configure_test_oidc_env() {
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
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn versions_routes_require_operator_and_csrf_and_persist_policy() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(oidc_env_keys());
        configure_test_oidc_env();
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        store
            .create_agent(anyagents_core::NewAgent::new("fixture", "codex"))
            .unwrap();
        let auth = crate::auth::Mode::from_env(dir.path());
        let app = super::super::build_webhook_router(make_state(store, auth));
        let cookie = valid_session_cookie_header();
        let request =
            |method: &str, path: &str, payload: &'static str, authenticated: bool, csrf: bool| {
                let mut req = Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json");
                if authenticated {
                    req = req.header(COOKIE, &cookie);
                }
                if csrf {
                    req = req.header(admin_auth::ADMIN_CSRF_HEADER, "1");
                }
                app.clone().oneshot(req.body(Body::from(payload)).unwrap())
            };
        let versions = "/admin/api/versions";
        let policy = "/admin/api/versions/policy";
        let drain = "/admin/api/agents/fixture/drain-upgrade";
        assert_eq!(
            request("GET", versions, "", false, false)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            request("POST", drain, "", true, false)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            request(
                "PATCH",
                policy,
                r#"{"auto_update_policy":"auto"}"#,
                true,
                false
            )
            .await
            .unwrap()
            .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            request(
                "PATCH",
                policy,
                r#"{"auto_update_policy":"unknown"}"#,
                true,
                true
            )
            .await
            .unwrap()
            .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        for value in ["auto", "manual", "pinned"] {
            let update = Request::builder()
                .method("PATCH")
                .uri(policy)
                .header(COOKIE, &cookie)
                .header(admin_auth::ADMIN_CSRF_HEADER, "1")
                .header("content-type", "application/json")
                .body(Body::from(json!({"auto_update_policy":value}).to_string()))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(update).await.unwrap().status(),
                StatusCode::OK
            );
            let inventory = request("GET", versions, "", true, false).await.unwrap();
            assert_eq!(inventory.status(), StatusCode::OK);
            let data: Value =
                serde_json::from_slice(&inventory.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(data["coordinator"]["auto_update_policy"], value);
            assert_eq!(
                data["summary"],
                json!({"total":1,"up_to_date":0,"outdated":0,"draining":0,"cold":1})
            );
            assert_eq!(data["agents"][0]["upgrade_status"], "cold_uses_latest");
        }
        let drained = request("POST", drain, "", true, true).await.unwrap();
        assert_eq!(drained.status(), StatusCode::OK);
        let data: Value =
            serde_json::from_slice(&drained.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(
            data,
            json!({"agent":"fixture", "action":"cold_next_turn_latest"})
        );
    }

    fn valid_session_cookie_header() -> String {
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
        format!("{}={cookie_val}", admin_auth::ADMIN_SESSION_COOKIE)
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn admin_console_serves_csp_html_and_fails_closed_on_api_routes() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(oidc_env_keys());
        configure_test_oidc_env();

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let auth = crate::auth::Mode::from_env(dir.path());
        assert!(auth.is_confined());
        let app = super::super::build_webhook_router(make_state(Arc::clone(&store), auth));

        // 1. GET /admin and GET /admin/ redirect 302 -> /.
        for legacy in ["/admin", "/admin/"] {
            let redir = app
                .clone()
                .oneshot(Request::builder().uri(legacy).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(redir.status(), StatusCode::FOUND, "{legacy}");
            assert_eq!(
                redir.headers().get(LOCATION).and_then(|v| v.to_str().ok()),
                Some("/"),
                "{legacy}"
            );
        }

        // 2. GET / when OIDC is enabled & caller is unauthenticated serves the Sign-In page
        //    with strict CSP, X-Frame-Options, nosniff, 1-click OIDC link, and CLI snippet.
        let signin_resp = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(signin_resp.status(), StatusCode::OK);
        assert_eq!(
            signin_resp
                .headers()
                .get("content-security-policy")
                .and_then(|v| v.to_str().ok()),
            Some(console_html::CONSOLE_CSP)
        );
        assert_eq!(
            signin_resp
                .headers()
                .get("x-frame-options")
                .and_then(|v| v.to_str().ok()),
            Some("DENY")
        );
        assert_eq!(
            signin_resp
                .headers()
                .get("x-content-type-options")
                .and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );
        let signin_body = signin_resp.into_body().collect().await.unwrap().to_bytes();
        let signin_html = String::from_utf8_lossy(&signin_body);
        assert!(signin_html.contains("Sign in with OIDC"));
        assert!(signin_html.contains(r#"href="/admin/auth/login""#));
        assert!(signin_html.contains("anyagents auth login --url https://console.example.com"));
        assert!(!signin_html.contains("<script>"));

        // 3. Health probes (/healthz, /readyz, /v1/webhooks/healthz) remain strictly JSON,
        //    and explicit Accept: application/json on / also returns readiness JSON.
        for path in ["/healthz", "/readyz", "/v1/webhooks/healthz"] {
            let probe = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(probe.status(), StatusCode::OK, "{path}");
            let body: Value =
                serde_json::from_slice(&probe.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(body, json!({ "ok": true }), "{path}");
        }
        let json_root = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(ACCEPT, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json_root.status(), StatusCode::OK);
        let json_root_body: Value =
            serde_json::from_slice(&json_root.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(json_root_body, json!({ "ok": true }));

        // 4. GET /v1/agents and GET /admin/api/overview fail closed (401) without cookie/token.
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

        // 5. GET /admin/auth/me and /auth/me report unauthenticated OIDC mode.
        for me_path in ["/admin/auth/me", "/auth/me"] {
            let me_unauth = app
                .clone()
                .oneshot(Request::builder().uri(me_path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(me_unauth.status(), StatusCode::OK, "{me_path}");
            let me_json: Value =
                serde_json::from_slice(&me_unauth.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(me_json["authenticated"], json!(false));
            assert_eq!(me_json["mode"], json!("oidc"));
            assert_eq!(me_json["serverless"], json!(true));
        }

        // 6. With valid signed session cookie, GET / serves the full Control Plane UI
        //    and /v1/agents & /admin/api/overview return 200.
        let cookie_hdr = valid_session_cookie_header();

        let authed_root = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(COOKIE, &cookie_hdr)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authed_root.status(), StatusCode::OK);
        let root_html_bytes = authed_root.into_body().collect().await.unwrap().to_bytes();
        let root_html = String::from_utf8_lossy(&root_html_bytes);
        assert!(root_html.contains("AnyAgents Console"));
        assert!(root_html.contains(r#"id="console-root""#));
        assert!(root_html.contains(r#"src="/assets/secrets.js""#));
        assert!(!root_html.contains("<script>"));

        let authed_agents = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .header(COOKIE, &cookie_hdr)
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
                    .header(COOKIE, &cookie_hdr)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authed_overview.status(), StatusCode::OK);

        // 7. POST /admin/api/tick rejects missing CSRF header (403), accepts with CSRF header (200).
        let no_csrf_tick = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/tick")
                    .header(COOKIE, &cookie_hdr)
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
                    .header(COOKIE, &cookie_hdr)
                    .header(admin_auth::ADMIN_CSRF_HEADER, "1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_tick.status(), StatusCode::OK);

        // 8. POST /admin/auth/logout requires CSRF header and clears the session cookie.
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
            .get(SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(set_cookie.contains("anyagents_admin_session=;"));
        assert!(set_cookie.contains("Max-Age=0"));
        assert!(set_cookie.contains("Secure"));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn local_single_user_mode_serves_console_ui_at_root() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(oidc_env_keys());
        for key in oidc_env_keys() {
            unsafe { std::env::remove_var(key) };
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let app = super::super::build_router(make_state(
            Arc::clone(&store),
            crate::auth::Mode::Trusted {
                data_dir: dir.path().to_path_buf(),
            },
        ));

        let resp = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = String::from_utf8_lossy(&resp.into_body().collect().await.unwrap().to_bytes())
            .to_string();
        assert!(body.contains("AnyAgents Console"));
        assert!(body.contains(r#"data-tab="fleet""#));

        // Modular assets all return 200 with strict CSP.
        for asset in [
            "/assets/console.css",
            "/assets/console.js",
            "/assets/profiles.js",
            "/assets/secrets.js",
            "/assets/signin.js",
            "/admin/assets/styles.css",
            "/admin/assets/app.js",
            "/admin/assets/secrets.js",
        ] {
            let r = app
                .clone()
                .oneshot(Request::builder().uri(asset).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::OK, "{asset}");
            assert!(
                r.headers().contains_key("content-security-policy"),
                "{asset}"
            );
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn oidc_login_redirects_non_canonical_host_before_minting_cookie() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(oidc_env_keys());
        configure_test_oidc_env();

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let auth = crate::auth::Mode::from_env(dir.path());
        let app = super::super::build_webhook_router(make_state(Arc::clone(&store), auth));

        // 1. Alias Host header -> 302 to canonical ANYAGENTS_PUBLIC_URL preserving safe `next`,
        //    with ZERO Set-Cookie header minted on the non-canonical host.
        let alias_host = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/auth/login?next=%2F%3Ftab%3Dsecrets")
                    .header(HOST, "alias.example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(alias_host.status(), StatusCode::FOUND);
        assert_eq!(
            alias_host
                .headers()
                .get(LOCATION)
                .and_then(|v| v.to_str().ok()),
            Some("https://console.example.com/admin/auth/login?next=%2F%3Ftab%3Dsecrets")
        );
        assert!(
            alias_host.headers().get(SET_COOKIE).is_none(),
            "must never mint pre-auth cookie on non-canonical host"
        );

        // 2. Alias X-Forwarded-Host header (even if Host matches or via /auth/login alias) ->
        //    302 to canonical /admin/auth/login, dropping unsafe open-redirect `next`.
        let alias_xfh = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/auth/login?next=//evil.example/steal")
                    .header(HOST, "console.example.com")
                    .header("x-forwarded-host", "custom-alias.example.org")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(alias_xfh.status(), StatusCode::FOUND);
        assert_eq!(
            alias_xfh
                .headers()
                .get(LOCATION)
                .and_then(|v| v.to_str().ok()),
            Some("https://console.example.com/admin/auth/login")
        );
        assert!(alias_xfh.headers().get(SET_COOKIE).is_none());
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn oidc_callback_renders_graceful_html_recovery_or_json_for_missing_and_expired_cookie() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(oidc_env_keys());
        configure_test_oidc_env();

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let auth = crate::auth::Mode::from_env(dir.path());
        let app = super::super::build_webhook_router(make_state(Arc::clone(&store), auth));

        // 1. Missing pre-auth cookie from a human browser -> 401 HTML recovery page with
        //    Cache-Control: no-store and 1-click "Sign in again" link to canonical /admin/auth/login.
        let browser_missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/auth/callback?code=abc123&state=st456")
                    .header(ACCEPT, "text/html,application/xhtml+xml,*/*;q=0.8")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(browser_missing.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            browser_missing
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
        assert!(
            browser_missing
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .starts_with("text/html")
        );
        let html_bytes = browser_missing
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let html = String::from_utf8_lossy(&html_bytes);
        assert!(html.contains("AnyAgents"));
        assert!(html.contains("Sign in again"));
        assert!(html.contains(r#"href="https://console.example.com/admin/auth/login""#));
        assert!(!html.contains("<script>"));

        // 2. Missing pre-auth cookie with explicit Accept: application/json -> structured JSON 401.
        let api_missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/auth/callback?code=abc123&state=st456")
                    .header(ACCEPT, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(api_missing.status(), StatusCode::UNAUTHORIZED);
        let api_json: Value =
            serde_json::from_slice(&api_missing.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(
            api_json["error"],
            json!("missing OIDC pre-auth state cookie")
        );

        // 3. Expired pre-auth cookie -> 401 HTML recovery page for browsers, JSON for API clients.
        let key = admin_auth::session_signing_key_from_env().unwrap();
        let expired_payload = json!({
            "state": "st456",
            "nonce": "nn789",
            "code_verifier": "cv000",
            "redirect_uri": "https://console.example.com/admin/auth/callback",
            "iat": 1_600_000_000i64,
            "exp": 1_600_000_600i64,
        });
        let signed_expired = admin_auth::sign_cookie_payload(
            &key,
            b"anyagents-admin-oidc-preauth-v1:",
            &expired_payload,
        )
        .unwrap();
        let expired_cookie_hdr = format!("{}={signed_expired}", admin_auth::ADMIN_OIDC_COOKIE);

        let browser_expired = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/auth/callback?code=abc123&state=st456")
                    .header(COOKIE, &expired_cookie_hdr)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(browser_expired.status(), StatusCode::UNAUTHORIZED);
        let expired_html = String::from_utf8_lossy(
            &browser_expired
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
        )
        .to_string();
        assert!(expired_html.contains("Your sign-in session expired before returning"));
        assert!(expired_html.contains("Sign in again"));
        assert!(expired_html.contains(r#"href="https://console.example.com/admin/auth/login""#));

        let api_expired = app
            .oneshot(
                Request::builder()
                    .uri("/auth/callback?code=abc123&state=st456")
                    .header(COOKIE, &expired_cookie_hdr)
                    .header(ACCEPT, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(api_expired.status(), StatusCode::UNAUTHORIZED);
        let api_expired_json: Value =
            serde_json::from_slice(&api_expired.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(
            api_expired_json["error"]
                .as_str()
                .unwrap_or("")
                .contains("expired")
        );
    }
}
