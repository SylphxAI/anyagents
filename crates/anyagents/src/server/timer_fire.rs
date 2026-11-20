//! Event-driven single-schedule fire endpoints (`POST /v1/internal/fire/{id}`
//! and `POST /v1/internal/fire`) with fail-closed Ed25519 JWKS and shared-token
//! authentication.

use std::collections::HashMap;
use std::io::Read as _;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_core::{AgentId, constant_time_eq, now_ms, verify_hmac_sha256_signature};
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use ring::signature::{ED25519, UnparsedPublicKey};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::types::{ApiError, AppState, spawn_store};

const JWKS_CACHE_TTL: Duration = Duration::from_secs(300);
const JWS_CLOCK_SKEW_SECS: i64 = 60;

#[derive(Clone, Debug)]
struct Ed25519Jwk {
    kid: Option<String>,
    public_key_bytes: [u8; 32],
}

#[derive(Clone, Debug)]
struct CachedJwks {
    fetched_at: Instant,
    keys: Vec<Ed25519Jwk>,
}

static JWKS_CACHE: LazyLock<Mutex<HashMap<String, CachedJwks>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
pub(crate) fn clear_jwks_cache() {
    if let Ok(mut guard) = JWKS_CACHE.lock() {
        guard.clear();
    }
}

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/internal/fire/{id}", post(fire_schedule_by_path_route))
        .route("/v1/internal/fire", post(fire_schedule_by_body_route))
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn env_primary_or_alias(primary: &str, alias: &str) -> Option<String> {
    env_non_empty(primary).or_else(|| env_non_empty(alias))
}

/// Configured shared token for timer fire callbacks (`ANYAGENTS_TIMER_TOKEN`
/// or legacy alias `ANYAGENTS_TICK_TOKEN`).
pub(crate) fn configured_timer_token() -> Option<String> {
    env_primary_or_alias("ANYAGENTS_TIMER_TOKEN", "ANYAGENTS_TICK_TOKEN")
}

/// Configured Ed25519 JWKS URL for timer fire callbacks (`ANYAGENTS_TIMER_JWKS_URL`
/// or legacy alias `ANYAGENTS_TICK_JWKS_URL`).
pub(crate) fn configured_timer_jwks_url() -> Option<String> {
    env_primary_or_alias("ANYAGENTS_TIMER_JWKS_URL", "ANYAGENTS_TICK_JWKS_URL")
}

/// Configured external timer scheduler URL (`ANYAGENTS_TIMER_SCHEDULER_URL`).
pub(crate) fn configured_timer_scheduler_url() -> Option<String> {
    env_non_empty(crate::timer::external::ENV_TIMER_SCHEDULER_URL)
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let val = headers.get("authorization")?.to_str().ok()?.trim();
    val.strip_prefix("Bearer ")
        .or_else(|| val.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn parse_jwks_ed25519_keys(raw: &[u8]) -> Result<Vec<Ed25519Jwk>, String> {
    let doc: Value = serde_json::from_slice(raw).map_err(|e| format!("invalid JWKS JSON: {e}"))?;
    let keys = doc
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| "JWKS missing `keys` array".to_string())?;
    let mut parsed = Vec::new();
    for entry in keys {
        let kty = entry.get("kty").and_then(Value::as_str).unwrap_or("");
        let crv = entry.get("crv").and_then(Value::as_str).unwrap_or("");
        if kty != "OKP" || crv != "Ed25519" {
            continue;
        }
        if let Some(key_use) = entry.get("use").and_then(Value::as_str)
            && key_use != "sig"
        {
            continue;
        }
        if let Some(alg) = entry.get("alg").and_then(Value::as_str)
            && alg != "EdDSA"
        {
            continue;
        }
        let Some(x_b64) = entry.get("x").and_then(Value::as_str) else {
            continue;
        };
        let Ok(decoded) = URL_SAFE_NO_PAD.decode(x_b64.trim()) else {
            continue;
        };
        if decoded.len() != 32 {
            continue;
        }
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&decoded);
        let kid = entry
            .get("kid")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        parsed.push(Ed25519Jwk {
            kid,
            public_key_bytes: pk,
        });
    }
    Ok(parsed)
}

async fn fetch_jwks_keys(jwks_url: &str, force_refresh: bool) -> Result<Vec<Ed25519Jwk>, String> {
    if !force_refresh {
        let guard = JWKS_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached) = guard.get(jwks_url)
            && cached.fetched_at.elapsed() < JWKS_CACHE_TTL
        {
            return Ok(cached.keys.clone());
        }
    }

    let url = jwks_url.to_string();
    let keys = tokio::task::spawn_blocking(move || -> Result<Vec<Ed25519Jwk>, String> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(5)))
            .http_status_as_error(false)
            .build()
            .into();
        let mut resp = agent
            .get(&url)
            .header("accept", "application/json")
            .call()
            .map_err(|e| format!("JWKS fetch failed ({url}): {e}"))?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(format!("JWKS endpoint returned HTTP {status}"));
        }
        let mut raw = Vec::new();
        let _ = resp
            .body_mut()
            .as_reader()
            .take(512 * 1024)
            .read_to_end(&mut raw);
        parse_jwks_ed25519_keys(&raw)
    })
    .await
    .map_err(|e| format!("JWKS fetch join error: {e}"))??;

    let mut guard = JWKS_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    guard.insert(
        jwks_url.to_string(),
        CachedJwks {
            fetched_at: Instant::now(),
            keys: keys.clone(),
        },
    );
    Ok(keys)
}

fn extract_timer_jws(headers: &HeaderMap) -> Option<String> {
    if let Some(hdr_name) = env_primary_or_alias(
        "ANYAGENTS_TIMER_SIGNATURE_HEADER",
        "ANYAGENTS_TICK_SIGNATURE_HEADER",
    ) {
        let val = headers
            .get(hdr_name.as_str())
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())?;
        let cleaned = val
            .strip_prefix("Bearer ")
            .or_else(|| val.strip_prefix("bearer "))
            .map(str::trim)
            .unwrap_or(val);
        return (!cleaned.is_empty()).then(|| cleaned.to_string());
    }
    let tok = extract_bearer_token(headers)?;
    let parts: Vec<&str> = tok.split('.').collect();
    if parts.len() == 3 && parts.iter().all(|p| !p.is_empty()) {
        Some(tok)
    } else {
        None
    }
}

async fn verify_timer_jws_jwks(
    jwks_url: &str,
    headers: &HeaderMap,
    raw_body: &[u8],
) -> Result<(), String> {
    let jws = extract_timer_jws(headers)
        .ok_or_else(|| "missing timer JWS signature header".to_string())?;
    let parts: Vec<&str> = jws.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err("malformed compact JWS".to_string());
    }
    let (header_b64, payload_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|_| "invalid JWS header base64url".to_string())?;
    let header_json: Value =
        serde_json::from_slice(&header_bytes).map_err(|_| "invalid JWS header JSON".to_string())?;
    if header_json.get("alg").and_then(Value::as_str) != Some("EdDSA") {
        return Err("JWS header alg must be EdDSA".to_string());
    }
    let kid = header_json
        .get("kid")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let sig_bytes = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|_| "invalid JWS signature base64url".to_string())?;
    if sig_bytes.len() != 64 {
        return Err("invalid EdDSA signature length".to_string());
    }
    let signing_input = format!("{header_b64}.{payload_b64}");

    let mut keys = fetch_jwks_keys(jwks_url, false).await?;
    let mut candidates: Vec<&Ed25519Jwk> = match kid {
        Some(want_kid) => keys
            .iter()
            .filter(|k| k.kid.as_deref() == Some(want_kid))
            .collect(),
        None => keys.iter().collect(),
    };
    if candidates.is_empty() {
        keys = fetch_jwks_keys(jwks_url, true).await?;
        candidates = match kid {
            Some(want_kid) => keys
                .iter()
                .filter(|k| k.kid.as_deref() == Some(want_kid))
                .collect(),
            None => keys.iter().collect(),
        };
    }
    if candidates.is_empty() {
        return Err("no matching Ed25519 JWK found for kid".to_string());
    }

    let sig_valid = candidates.iter().any(|jwk| {
        UnparsedPublicKey::new(&ED25519, &jwk.public_key_bytes)
            .verify(signing_input.as_bytes(), &sig_bytes)
            .is_ok()
    });
    if !sig_valid {
        return Err("EdDSA signature verification failed".to_string());
    }

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| "invalid JWS payload base64url".to_string())?;
    let claims: Value = serde_json::from_slice(&payload_bytes)
        .map_err(|_| "invalid JWS payload JSON".to_string())?;
    let claims_obj = claims
        .as_object()
        .ok_or_else(|| "JWS payload must be a JSON object".to_string())?;

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let exp = claims_obj
        .get("exp")
        .and_then(Value::as_i64)
        .ok_or_else(|| "JWS missing numeric `exp` claim".to_string())?;
    if now_secs > exp.saturating_add(JWS_CLOCK_SKEW_SECS) {
        return Err("JWS token expired".to_string());
    }
    if let Some(nbf) = claims_obj.get("nbf").and_then(Value::as_i64)
        && now_secs.saturating_add(JWS_CLOCK_SKEW_SECS) < nbf
    {
        return Err("JWS token not yet valid".to_string());
    }

    if let Some(expected_iss) = env_primary_or_alias("ANYAGENTS_TIMER_ISS", "ANYAGENTS_TICK_ISS") {
        let got_iss = claims_obj.get("iss").and_then(Value::as_str).unwrap_or("");
        if got_iss != expected_iss {
            return Err("JWS `iss` claim mismatch".to_string());
        }
    }
    if let Some(expected_aud) = env_primary_or_alias("ANYAGENTS_TIMER_AUD", "ANYAGENTS_TICK_AUD") {
        let aud_ok = match claims_obj.get("aud") {
            Some(Value::String(s)) => s == &expected_aud,
            Some(Value::Array(arr)) => arr
                .iter()
                .any(|v| v.as_str() == Some(expected_aud.as_str())),
            _ => false,
        };
        if !aud_ok {
            return Err("JWS `aud` claim mismatch".to_string());
        }
    }
    if let Some(expected_sub) = env_primary_or_alias("ANYAGENTS_TIMER_SUB", "ANYAGENTS_TICK_SUB") {
        let got_sub = claims_obj.get("sub").and_then(Value::as_str).unwrap_or("");
        if got_sub != expected_sub {
            return Err("JWS `sub` claim mismatch".to_string());
        }
    }

    if let Some(bh_val) = claims_obj.get("bh") {
        let Some(bh_str) = bh_val.as_str() else {
            return Err("JWS `bh` claim must be a string".to_string());
        };
        let expected_bh = URL_SAFE_NO_PAD.encode(Sha256::digest(raw_body));
        if !constant_time_eq(expected_bh.as_bytes(), bh_str.as_bytes()) {
            return Err("JWS `bh` body hash mismatch".to_string());
        }
    }

    Ok(())
}

fn verify_timer_shared_token(expected: &str, headers: &HeaderMap, raw_body: &[u8]) -> bool {
    if let Some(tok) = extract_bearer_token(headers)
        && constant_time_eq(expected.as_bytes(), tok.as_bytes())
    {
        return true;
    }
    for hdr in ["x-anyagents-timer-token", "x-anyagents-tick-token"] {
        if let Some(hdr_tok) = headers
            .get(hdr)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            && constant_time_eq(expected.as_bytes(), hdr_tok.as_bytes())
        {
            return true;
        }
    }
    for hdr in ["x-anyagents-timer-signature", "x-anyagents-tick-signature"] {
        if let Some(sig) = headers.get(hdr).and_then(|v| v.to_str().ok())
            && verify_hmac_sha256_signature(expected.as_bytes(), raw_body, sig)
        {
            return true;
        }
    }
    false
}

async fn authenticate_timer_fire_request(
    headers: &HeaderMap,
    raw_body: &[u8],
) -> Result<(), ApiError> {
    let token = configured_timer_token();
    let jwks_url = configured_timer_jwks_url();

    if token.is_none() && jwks_url.is_none() {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "timer fire endpoint disabled: configure ANYAGENTS_TIMER_JWKS_URL or ANYAGENTS_TIMER_TOKEN",
        ));
    }

    if let Some(ref expected) = token
        && verify_timer_shared_token(expected, headers, raw_body)
    {
        return Ok(());
    }

    if let Some(ref url) = jwks_url
        && verify_timer_jws_jwks(url, headers, raw_body).await.is_ok()
    {
        return Ok(());
    }

    Err(ApiError::new(
        StatusCode::UNAUTHORIZED,
        "invalid timer fire authentication credentials",
    ))
}

fn parse_fire_payload(raw_body: &[u8]) -> (Option<String>, Option<i64>) {
    if raw_body.is_empty() {
        return (None, None);
    }
    let Ok(val) = serde_json::from_slice::<Value>(raw_body) else {
        return (None, None);
    };
    let input = val.get("input");
    let schedule_id = input
        .and_then(|v| v.get("schedule_id"))
        .or_else(|| val.get("schedule_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let intended_at_ms = input
        .and_then(|v| v.get("intended_at_ms"))
        .or_else(|| val.get("intended_at_ms"))
        .and_then(Value::as_i64);
    (schedule_id, intended_at_ms)
}

async fn execute_fire(
    state: AppState,
    schedule_id: String,
    intended_at_ms: Option<i64>,
) -> Result<impl IntoResponse, ApiError> {
    if super::serverless_maintenance::is_system_timer_id(&schedule_id) {
        let result = super::serverless_maintenance::fire_system_timer(
            Arc::clone(&state.store),
            Arc::clone(&state.timer_backend),
            &schedule_id,
        )
        .await
        .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err))?;
        return Ok(Json(result));
    }

    let store = Arc::clone(&state.store);
    let health = state.health.clone();
    let timer_backend = Arc::clone(&state.timer_backend);
    let result = spawn_store(move || {
        crate::timer::fire_schedule_by_id(
            &store,
            &health,
            timer_backend.as_ref(),
            &schedule_id,
            intended_at_ms,
            now_ms(),
        )
        .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err))
    })
    .await?;

    if result.fired
        && super::serverless::is_serverless_mode()
        && let Some(ref aid_str) = result.agent_id
        && let Ok(aid) = AgentId::parse(aid_str)
    {
        let _ = super::serverless::dispatch_next_mailbox_message_with_timer(
            Arc::clone(&state.store),
            &aid,
            Some(Arc::clone(&state.timer_backend)),
        )
        .await;
    }

    Ok(Json(result))
}

/// `POST /v1/internal/fire/{id}`: authenticate via Ed25519 JWKS or shared token,
/// then fire strictly schedule `{id}` without waking unrelated schedules.
pub(crate) async fn fire_schedule_by_path_route(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    authenticate_timer_fire_request(&headers, &body).await?;
    let (_, intended_at_ms) = parse_fire_payload(&body);
    execute_fire(state, id, intended_at_ms).await
}

/// `POST /v1/internal/fire`: fixed-handler-URI variant accepting `input.schedule_id`
/// or top-level `schedule_id` in the signed JSON body.
pub(crate) async fn fire_schedule_by_body_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    authenticate_timer_fire_request(&headers, &body).await?;
    let (schedule_id, intended_at_ms) = parse_fire_payload(&body);
    let Some(id) = schedule_id else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "missing schedule_id in request body",
        ));
    };
    execute_fire(state, id, intended_at_ms).await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyagents_core::{NewAgent, NewSchedule, Store};
    use anyagents_supervisor::HealthBoard;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt as _;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    use serde_json::json;
    use tower::ServiceExt as _;

    use super::*;
    use crate::timer::TimerBackend;

    #[derive(Default)]
    struct RecordingTimerBackend {
        scheduled: Mutex<Vec<(String, i64)>>,
        cancelled: Mutex<Vec<(String, Option<String>)>>,
    }

    impl TimerBackend for RecordingTimerBackend {
        fn schedule_one_shot(
            &self,
            schedule_id: &str,
            fire_at_ms: i64,
        ) -> Result<Option<String>, String> {
            self.scheduled
                .lock()
                .unwrap()
                .push((schedule_id.to_string(), fire_at_ms));
            Ok(Some(format!("ext-{schedule_id}-{fire_at_ms}")))
        }

        fn cancel(&self, schedule_id: &str, external_run_id: Option<&str>) -> Result<(), String> {
            self.cancelled
                .lock()
                .unwrap()
                .push((schedule_id.to_string(), external_run_id.map(str::to_string)));
            Ok(())
        }
    }

    fn jwks_doc_for(keys: &[(&str, &Ed25519KeyPair)]) -> Value {
        let entries: Vec<Value> = keys
            .iter()
            .map(|(kid, kp)| {
                json!({
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "use": "sig",
                    "alg": "EdDSA",
                    "kid": kid,
                    "x": URL_SAFE_NO_PAD.encode(kp.public_key().as_ref()),
                })
            })
            .collect();
        json!({ "keys": entries })
    }

    fn sign_ed25519_jws(kp: &Ed25519KeyPair, kid: &str, claims: &Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({ "alg": "EdDSA", "typ": "JWT", "kid": kid })).unwrap(),
        );
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{header}.{payload}");
        let sig = kp.sign(signing_input.as_bytes());
        format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(sig.as_ref()))
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

    fn test_state(
        store: Arc<Store>,
        data_dir: &std::path::Path,
        timer_backend: Arc<dyn TimerBackend>,
    ) -> AppState {
        AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(super::super::ProcessRunner)),
            auth: crate::auth::Mode::Trusted {
                data_dir: data_dir.to_path_buf(),
            },
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn internal_fire_enforces_ed25519_jwks_fires_strictly_one_schedule_and_retires_tick() {
        let _env = crate::server::tests::env_lock();
        clear_jwks_cache();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_TIMER_TOKEN",
            "ANYAGENTS_TIMER_JWKS_URL",
            "ANYAGENTS_TIMER_SIGNATURE_HEADER",
            "ANYAGENTS_TIMER_ISS",
            "ANYAGENTS_TIMER_AUD",
            "ANYAGENTS_TIMER_SUB",
            "ANYAGENTS_TICK_TOKEN",
            "ANYAGENTS_TICK_JWKS_URL",
            "ANYAGENTS_TICK_SIGNATURE_HEADER",
            "ANYAGENTS_TICK_ISS",
            "ANYAGENTS_TICK_AUD",
            "ANYAGENTS_TICK_SUB",
        ]);
        for k in [
            "ANYAGENTS_TIMER_TOKEN",
            "ANYAGENTS_TIMER_JWKS_URL",
            "ANYAGENTS_TIMER_SIGNATURE_HEADER",
            "ANYAGENTS_TIMER_ISS",
            "ANYAGENTS_TIMER_AUD",
            "ANYAGENTS_TIMER_SUB",
            "ANYAGENTS_TICK_TOKEN",
            "ANYAGENTS_TICK_JWKS_URL",
            "ANYAGENTS_TICK_SIGNATURE_HEADER",
            "ANYAGENTS_TICK_ISS",
            "ANYAGENTS_TICK_AUD",
            "ANYAGENTS_TICK_SUB",
        ] {
            unsafe { std::env::remove_var(k) };
        }

        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let recorder = Arc::new(RecordingTimerBackend::default());
        let app = super::super::build_public_router(test_state(
            Arc::clone(&store),
            dir.path(),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        ));

        // 0. Legacy `/v1/internal/tick` is retired (404 Not Found).
        let legacy_tick = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/tick")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(legacy_tick.status(), StatusCode::NOT_FOUND);

        // 1. Fail-closed 503 when neither JWKS nor token is configured.
        let unconfigured = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire/sch_none")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unconfigured.status(), StatusCode::SERVICE_UNAVAILABLE);

        // 2. Start local JWKS server with Ed25519 key rotation support.
        let rng = SystemRandom::new();
        let pkcs8_1 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp1 = Ed25519KeyPair::from_pkcs8(pkcs8_1.as_ref()).unwrap();
        let pkcs8_2 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp2 = Ed25519KeyPair::from_pkcs8(pkcs8_2.as_ref()).unwrap();

        let jwks_state = Arc::new(Mutex::new(jwks_doc_for(&[("k1", &kp1)])));
        let jwks_srv_state = Arc::clone(&jwks_state);
        let jwks_app = axum::Router::new().route(
            "/.well-known/jwks.json",
            axum::routing::get(move || {
                let st = Arc::clone(&jwks_srv_state);
                async move { Json(st.lock().unwrap().clone()) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let jwks_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, jwks_app).await;
        });

        unsafe {
            std::env::set_var(
                "ANYAGENTS_TIMER_JWKS_URL",
                format!("http://{jwks_addr}/.well-known/jwks.json"),
            );
            std::env::set_var("ANYAGENTS_TIMER_SIGNATURE_HEADER", "X-Timer-Signature");
            std::env::set_var("ANYAGENTS_TIMER_ISS", "https://timers.example.test");
            std::env::set_var(
                "ANYAGENTS_TIMER_AUD",
                "https://coord.example.test/v1/internal/fire",
            );
        }

        // Create two independent agents and three schedules:
        // - one-shot `at` on agent A
        // - recurring `every` on agent A
        // - unrelated due `at` on idle agent B (MUST NOT wake!)
        let agent_a = store
            .create_agent(NewAgent::new("agent-a", "codex"))
            .unwrap();
        let agent_b = store
            .create_agent(NewAgent::new("agent-b", "codex"))
            .unwrap();
        store.set_agent_running(&agent_a.id, "/work").unwrap();
        store.set_agent_running(&agent_b.id, "/work").unwrap();

        let now = now_ms();
        let oneshot = store
            .create_schedule(NewSchedule {
                agent_id: agent_a.id.clone(),
                kind: "at".into(),
                spec: "5s".into(),
                prompt: "exact one-shot".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now - 500),
                watch_baseline: None,
            })
            .unwrap();
        let recurring = store
            .create_schedule(NewSchedule {
                agent_id: agent_a.id.clone(),
                kind: "every".into(),
                spec: "10s".into(),
                prompt: "recurring 10s".into(),
                interval_ms: Some(10_000),
                next_fire_at_ms: Some(now - 200),
                watch_baseline: None,
            })
            .unwrap();
        let unrelated = store
            .create_schedule(NewSchedule {
                agent_id: agent_b.id.clone(),
                kind: "at".into(),
                spec: "1s".into(),
                prompt: "idle tenant schedule".into(),
                interval_ms: None,
                next_fire_at_ms: Some(now - 1_000),
                watch_baseline: None,
            })
            .unwrap();

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // 3. Missing auth / wrong aud / tampered body rejected fail-closed (401).
        let missing_auth = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{}", oneshot.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

        let bad_aud_jws = sign_ed25519_jws(
            &kp1,
            "k1",
            &json!({
                "iss": "https://timers.example.test",
                "aud": "https://wrong.example.test",
                "exp": now_secs + 300,
            }),
        );
        let bad_aud_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{}", oneshot.id))
                    .header("x-timer-signature", bad_aud_jws)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_aud_resp.status(), StatusCode::UNAUTHORIZED);

        let body_oneshot = serde_json::to_vec(&json!({
            "intended_at_ms": oneshot.next_fire_at_ms.unwrap(),
        }))
        .unwrap();
        let bh_oneshot = URL_SAFE_NO_PAD.encode(Sha256::digest(&body_oneshot));
        let valid_oneshot_jws = sign_ed25519_jws(
            &kp1,
            "k1",
            &json!({
                "iss": "https://timers.example.test",
                "aud": "https://coord.example.test/v1/internal/fire",
                "exp": now_secs + 300,
                "bh": bh_oneshot,
            }),
        );
        let tampered_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{}", oneshot.id))
                    .header("x-timer-signature", valid_oneshot_jws.clone())
                    .body(Body::from(&b"{\"intended_at_ms\":0}"[..]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tampered_resp.status(), StatusCode::UNAUTHORIZED);

        // 4. Valid Ed25519 JWS fires ONLY `oneshot` without waking `unrelated`!
        let ok_oneshot = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{}", oneshot.id))
                    .header("x-timer-signature", valid_oneshot_jws.clone())
                    .body(Body::from(body_oneshot.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_oneshot.status(), StatusCode::OK);
        let oneshot_json: Value =
            serde_json::from_slice(&ok_oneshot.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(oneshot_json["fired"], true);
        assert_eq!(oneshot_json["schedule_id"], oneshot.id);
        assert!(oneshot_json.get("next_run_ms").is_none());
        assert_eq!(
            store.get_schedule(&oneshot.id).unwrap().unwrap().state,
            "fired"
        );
        // Idle agent B's due schedule was NOT scanned or woken!
        assert!(store.inbox(&agent_b.id, None).unwrap().is_empty());
        assert_eq!(
            store.get_schedule(&unrelated.id).unwrap().unwrap().state,
            "active"
        );
        assert!(recorder.scheduled.lock().unwrap().is_empty());

        // 5. Replaying the already-fired one-shot returns idempotent `{ fired: false }`.
        let replay = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{}", oneshot.id))
                    .header("x-timer-signature", valid_oneshot_jws)
                    .body(Body::from(body_oneshot))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        let replay_json: Value =
            serde_json::from_slice(&replay.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(replay_json["fired"], false);
        assert_eq!(replay_json["reason"], "not_found_or_inactive");

        // 6. Fixed-URI `POST /v1/internal/fire` with rotated key `k2` fires recurring schedule
        //    and re-arms next `run_at` on `TimerBackend`!
        *jwks_state.lock().unwrap() = jwks_doc_for(&[("k1", &kp1), ("k2", &kp2)]);
        let fixed_body = serde_json::to_vec(&json!({
            "input": {
                "schedule_id": recurring.id,
                "intended_at_ms": recurring.next_fire_at_ms.unwrap(),
            }
        }))
        .unwrap();
        let bh_fixed = URL_SAFE_NO_PAD.encode(Sha256::digest(&fixed_body));
        let rotated_jws = sign_ed25519_jws(
            &kp2,
            "k2",
            &json!({
                "iss": "https://timers.example.test",
                "aud": "https://coord.example.test/v1/internal/fire",
                "exp": now_secs + 300,
                "bh": bh_fixed,
            }),
        );
        let rec_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/internal/fire")
                    .header("content-type", "application/json")
                    .header("x-timer-signature", rotated_jws)
                    .body(Body::from(fixed_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rec_resp.status(), StatusCode::OK);
        let rec_json: Value =
            serde_json::from_slice(&rec_resp.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(rec_json["fired"], true);
        let next_run = rec_json["next_run_ms"].as_i64().unwrap();
        assert!(next_run > now);
        let recorded = recorder.scheduled.lock().unwrap().clone();
        assert_eq!(recorded, vec![(recurring.id.clone(), next_run)]);
        let updated_rec = store.get_schedule(&recurring.id).unwrap().unwrap();
        assert_eq!(updated_rec.state, "active");
        assert_eq!(updated_rec.next_fire_at_ms, Some(next_run));
        assert_eq!(
            updated_rec.external_timer_id.as_deref(),
            Some(format!("ext-{}-{next_run}", recurring.id).as_str())
        );

        // 7. Cancelled schedule returns idempotent no-op.
        store
            .cancel_schedule(Some(&agent_b.id), &unrelated.id)
            .unwrap();
        let cancel_jws = sign_ed25519_jws(
            &kp2,
            "k2",
            &json!({
                "iss": "https://timers.example.test",
                "aud": "https://coord.example.test/v1/internal/fire",
                "exp": now_secs + 300,
            }),
        );
        let cancelled_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/internal/fire/{}", unrelated.id))
                    .header("x-timer-signature", cancel_jws)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancelled_resp.status(), StatusCode::OK);
        let cancelled_json: Value = serde_json::from_slice(
            &cancelled_resp
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
        )
        .unwrap();
        assert_eq!(cancelled_json["fired"], false);
        assert_eq!(cancelled_json["reason"], "not_found_or_inactive");
        assert!(store.inbox(&agent_b.id, None).unwrap().is_empty());

        clear_jwks_cache();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn schedule_crud_and_agent_archive_arm_and_cancel_timer_backend() {
        let _env = crate::server::tests::env_lock();
        let _restore =
            EnvRestore::capture(&["ANYAGENTS_OPERATOR_TOKEN", "ANYAGENTS_OPERATOR_TOKEN_FILE"]);
        unsafe {
            std::env::set_var("ANYAGENTS_OPERATOR_TOKEN", "crud-timer-op-secret");
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        }
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("crud-timer-agent", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        let recorder = Arc::new(RecordingTimerBackend::default());
        let mut state = test_state(
            Arc::clone(&store),
            dir.path(),
            Arc::clone(&recorder) as Arc<dyn TimerBackend>,
        );
        state.auth = crate::auth::Mode::from_env(dir.path());
        let app = super::super::build_router(state);

        // 1. Create a 10s schedule via HTTP CRUD -> arms TimerBackend & persists external_timer_id.
        let create_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/agents/crud-timer-agent/schedules")
                    .header("content-type", "application/json")
                    .header(crate::operator::HEADER, "crud-timer-op-secret")
                    .body(Body::from(
                        json!({
                            "action": "create",
                            "at": "10s",
                            "prompt": "ping in 10s"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::OK);
        let create_json: Value =
            serde_json::from_slice(&create_resp.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let sched_id = create_json["schedule"]["id"].as_str().unwrap().to_string();
        let persisted = store.get_schedule(&sched_id).unwrap().unwrap();
        let ext_id_1 = persisted.external_timer_id.clone().unwrap();
        assert!(ext_id_1.starts_with(&format!("ext-{sched_id}-")));
        assert_eq!(recorder.scheduled.lock().unwrap().len(), 1);

        // 2. Cancel via HTTP CRUD -> cancels TimerBackend with persisted external_timer_id.
        let cancel_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/agents/crud-timer-agent/schedules")
                    .header("content-type", "application/json")
                    .header(crate::operator::HEADER, "crud-timer-op-secret")
                    .body(Body::from(
                        json!({
                            "action": "cancel",
                            "id": sched_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel_resp.status(), StatusCode::OK);
        assert_eq!(
            recorder.cancelled.lock().unwrap().as_slice(),
            &[(sched_id.clone(), Some(ext_id_1))]
        );

        // 3. Create another schedule, then archive the owning agent -> cancels TimerBackend!
        let create_resp2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/agents/crud-timer-agent/schedules")
                    .header("content-type", "application/json")
                    .header(crate::operator::HEADER, "crud-timer-op-secret")
                    .body(Body::from(
                        json!({
                            "action": "create",
                            "every": "30s",
                            "prompt": "pulse"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_resp2.status(), StatusCode::OK);
        let create_json2: Value =
            serde_json::from_slice(&create_resp2.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let sched_id2 = create_json2["schedule"]["id"].as_str().unwrap().to_string();
        let ext_id_2 = store
            .get_schedule(&sched_id2)
            .unwrap()
            .unwrap()
            .external_timer_id
            .unwrap();

        let archive_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/agents/crud-timer-agent/archive")
                    .header(crate::operator::HEADER, "crud-timer-op-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(archive_resp.status(), StatusCode::OK);
        let cancelled_calls = recorder.cancelled.lock().unwrap().clone();
        assert!(
            cancelled_calls.contains(&(sched_id2, Some(ext_id_2))),
            "expected archive to cancel timer backend, got {cancelled_calls:?}"
        );
    }
}
