//! Fail-closed OIDC/OAuth2 Bearer JWT authentication (RFC 8707 `resource`
//! audience scoping) and RFC 9728 Protected Resource Metadata discovery for the
//! unified Control-Plane REST API.
//!
//! Shares the exact same OIDC discovery, JWKS cache, signature verification
//! algorithms (`ES256`, `RS256`, `EdDSA`), `email_verified == true` rule, and
//! `ANYAGENTS_ADMIN_EMAILS` allowlist as browser `/admin` cookie auth
//! ([`super::admin_auth`]).

use std::path::Path;

use axum::extract::State;
use axum::http::header::CACHE_CONTROL;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::signature::{
    ECDSA_P256_SHA256_FIXED, ED25519, RSA_PKCS1_2048_8192_SHA256, RsaPublicKeyComponents,
    UnparsedPublicKey,
};
use serde_json::{Value, json};

use crate::auth::{AuthError, Principal};

use super::admin_auth::{
    AdminAuthzError, AdminJwk, CLOCK_SKEW_SECS, IdTokenVerifyError, JwkKey, OidcConfig,
    VerifiedIdToken, authorize_admin_identity, env_non_empty, fetch_discovery, fetch_jwks,
    fetch_userinfo, unix_now_secs,
};
use super::{AppState, serverless};

const OIDC_RESOURCE_ENV: &str = "ANYAGENTS_ADMIN_OIDC_RESOURCE";
const OIDC_AUDIENCE_ENV: &str = "ANYAGENTS_ADMIN_OIDC_AUDIENCE";
const OIDC_DEVICE_CLIENT_ID_ENV: &str = "ANYAGENTS_ADMIN_OIDC_DEVICE_CLIENT_ID";

/// Public discovery router exposing RFC 9728 OAuth 2.0 Protected Resource Metadata
/// so CLI clients (`anyagents auth login --url <coordinator>`) auto-discover the
/// coordinator's OIDC issuer, client ID, and RFC 8707 `resource` URI.
pub(super) fn discovery_router() -> Router<AppState> {
    Router::new().route(
        "/.well-known/oauth-protected-resource",
        get(protected_resource_metadata_route),
    )
}

fn strip_leading_zeros(bytes: &[u8]) -> Vec<u8> {
    let first_non_zero = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len());
    bytes[first_non_zero..].to_vec()
}

/// Parse an RFC 7517 JWKS JSON document into verifiable `ES256`, `RS256`, and `EdDSA` keys.
pub(crate) fn parse_jwks(raw: &[u8]) -> Result<Vec<AdminJwk>, String> {
    let doc: Value = serde_json::from_slice(raw).map_err(|e| format!("invalid JWKS JSON: {e}"))?;
    let keys = doc
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| "JWKS missing `keys` array".to_string())?;

    let mut parsed = Vec::new();
    for entry in keys {
        if let Some(key_use) = entry.get("use").and_then(Value::as_str)
            && key_use != "sig"
        {
            continue;
        }
        let kid = entry
            .get("kid")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let alg = entry
            .get("alg")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let kty = entry.get("kty").and_then(Value::as_str).unwrap_or("");
        match kty {
            "EC" => {
                if entry.get("crv").and_then(Value::as_str) != Some("P-256") {
                    continue;
                }
                if let Some(ref a) = alg
                    && a != "ES256"
                {
                    continue;
                }
                let Some(x_b64) = entry.get("x").and_then(Value::as_str) else {
                    continue;
                };
                let Some(y_b64) = entry.get("y").and_then(Value::as_str) else {
                    continue;
                };
                let (Ok(x), Ok(y)) = (
                    URL_SAFE_NO_PAD.decode(x_b64.trim()),
                    URL_SAFE_NO_PAD.decode(y_b64.trim()),
                ) else {
                    continue;
                };
                if x.len() != 32 || y.len() != 32 {
                    continue;
                }
                let mut uncompressed = [0u8; 65];
                uncompressed[0] = 0x04;
                uncompressed[1..33].copy_from_slice(&x);
                uncompressed[33..65].copy_from_slice(&y);
                parsed.push(AdminJwk {
                    kid,
                    alg,
                    key: JwkKey::EcP256 { uncompressed },
                });
            }
            "RSA" => {
                if let Some(ref a) = alg
                    && a != "RS256"
                {
                    continue;
                }
                let Some(n_b64) = entry.get("n").and_then(Value::as_str) else {
                    continue;
                };
                let Some(e_b64) = entry.get("e").and_then(Value::as_str) else {
                    continue;
                };
                let (Ok(n_raw), Ok(e_raw)) = (
                    URL_SAFE_NO_PAD.decode(n_b64.trim()),
                    URL_SAFE_NO_PAD.decode(e_b64.trim()),
                ) else {
                    continue;
                };
                let n = strip_leading_zeros(&n_raw);
                let e = strip_leading_zeros(&e_raw);
                if n.is_empty() || e.is_empty() {
                    continue;
                }
                parsed.push(AdminJwk {
                    kid,
                    alg,
                    key: JwkKey::Rsa { n, e },
                });
            }
            "OKP" => {
                if entry.get("crv").and_then(Value::as_str) != Some("Ed25519") {
                    continue;
                }
                if let Some(ref a) = alg
                    && a != "EdDSA"
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
                let mut public_key = [0u8; 32];
                public_key.copy_from_slice(&decoded);
                parsed.push(AdminJwk {
                    kid,
                    alg,
                    key: JwkKey::Ed25519 { public_key },
                });
            }
            _ => {}
        }
    }
    Ok(parsed)
}

/// Normalize an RFC 8707 resource URI for canonical exact matching.
pub(crate) fn normalize_resource_uri(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_string()
}

/// Resolve the expected RFC 8707 `resource` / `aud` URI for Bearer tokens.
///
/// Precedence:
/// 1. `ANYAGENTS_ADMIN_OIDC_RESOURCE`
/// 2. `ANYAGENTS_ADMIN_OIDC_AUDIENCE`
/// 3. Resolved coordinator public URL (`ANYAGENTS_PUBLIC_URL`, `ANYAGENTS_WEBHOOK_URL`,
///    or `[server] public_url` in `config.toml`).
pub(crate) fn expected_resource_audience(data_dir: &Path) -> Option<String> {
    let raw = env_non_empty(OIDC_RESOURCE_ENV)
        .or_else(|| env_non_empty(OIDC_AUDIENCE_ENV))
        .or_else(|| serverless::resolve_public_url(data_dir))?;
    let normalized = normalize_resource_uri(&raw);
    (!normalized.is_empty()).then_some(normalized)
}

/// Preferred public client ID advertised for RFC 8628 Device Authorization Grant.
pub(crate) fn device_client_id(cfg: &OidcConfig) -> String {
    env_non_empty(OIDC_DEVICE_CLIENT_ID_ENV).unwrap_or_else(|| cfg.client_id.clone())
}

/// All OIDC client IDs authorized to mint tokens for this coordinator.
pub(crate) fn allowed_client_ids(cfg: &OidcConfig) -> Vec<String> {
    let mut ids = vec![cfg.client_id.clone()];
    if let Some(dev_cid) = env_non_empty(OIDC_DEVICE_CLIENT_ID_ENV)
        && !ids.iter().any(|existing| existing == &dev_cid)
    {
        ids.push(dev_cid);
    }
    ids
}

/// `GET /.well-known/oauth-protected-resource`: RFC 9728 metadata (never exposes
/// secrets or admin email allowlists).
async fn protected_resource_metadata_route(State(state): State<AppState>) -> impl IntoResponse {
    let Some(cfg) = OidcConfig::from_env() else {
        return (
            StatusCode::NOT_FOUND,
            [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
            Json(json!({ "error": "OIDC authentication is not configured" })),
        );
    };
    let resource = expected_resource_audience(state.store.data_dir());
    let body = json!({
        "resource": resource,
        "authorization_servers": [cfg.issuer.clone()],
        "issuer": cfg.issuer,
        "client_id": device_client_id(&cfg),
        "scopes_supported": ["openid", "email", "profile"],
        "bearer_methods_supported": ["header"],
    });
    (
        StatusCode::OK,
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(body),
    )
}

/// Verify an OIDC/OAuth2 Bearer JWT against `jwks`, issuer, timestamps, and
/// RFC 8707 `resource` audience scoping (pure, zero network I/O).
///
/// Strictly accepts `ES256`, `RS256`, and `EdDSA` only (rejecting `alg: none`
/// and symmetric HMAC algorithms).
pub(crate) fn verify_bearer_jwt_claims(
    token: &str,
    jwks: &[AdminJwk],
    expected_issuer: &str,
    expected_resource: Option<&str>,
    allowed_clients: &[String],
    now_secs: i64,
) -> Result<VerifiedIdToken, IdTokenVerifyError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err(IdTokenVerifyError::Invalid(
            "malformed compact bearer JWT".to_string(),
        ));
    }
    let (header_b64, payload_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid bearer JWT header base64url".into()))?;
    let header_json: Value = serde_json::from_slice(&header_bytes)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid bearer JWT header JSON".into()))?;
    let alg = header_json
        .get("alg")
        .and_then(Value::as_str)
        .ok_or_else(|| IdTokenVerifyError::Invalid("bearer JWT header missing alg".into()))?;
    if !matches!(alg, "ES256" | "RS256" | "EdDSA") {
        return Err(IdTokenVerifyError::Invalid(format!(
            "unsupported bearer JWT alg `{alg}`"
        )));
    }
    let kid = header_json
        .get("kid")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let sig_bytes = URL_SAFE_NO_PAD.decode(sig_b64).map_err(|_| {
        IdTokenVerifyError::Invalid("invalid bearer JWT signature base64url".into())
    })?;

    let candidates: Vec<&AdminJwk> = jwks
        .iter()
        .filter(|jwk| {
            let kid_ok = match kid {
                Some(want) => jwk.kid.as_deref() == Some(want),
                None => true,
            };
            let alg_ok = jwk.alg.as_deref().is_none_or(|a| a == alg);
            let kty_ok = matches!(
                (alg, &jwk.key),
                ("ES256", JwkKey::EcP256 { .. })
                    | ("RS256", JwkKey::Rsa { .. })
                    | ("EdDSA", JwkKey::Ed25519 { .. })
            );
            kid_ok && alg_ok && kty_ok
        })
        .collect();

    if candidates.is_empty() {
        return Err(IdTokenVerifyError::UnknownKid);
    }

    let signing_input = format!("{header_b64}.{payload_b64}");
    let sig_valid = candidates.iter().any(|jwk| match (alg, &jwk.key) {
        ("ES256", JwkKey::EcP256 { uncompressed }) => {
            sig_bytes.len() == 64
                && UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, uncompressed.as_slice())
                    .verify(signing_input.as_bytes(), &sig_bytes)
                    .is_ok()
        }
        ("RS256", JwkKey::Rsa { n, e }) => RsaPublicKeyComponents { n, e }
            .verify(
                &RSA_PKCS1_2048_8192_SHA256,
                signing_input.as_bytes(),
                &sig_bytes,
            )
            .is_ok(),
        ("EdDSA", JwkKey::Ed25519 { public_key }) => {
            sig_bytes.len() == 64
                && UnparsedPublicKey::new(&ED25519, public_key.as_slice())
                    .verify(signing_input.as_bytes(), &sig_bytes)
                    .is_ok()
        }
        _ => false,
    });
    if !sig_valid {
        return Err(IdTokenVerifyError::Invalid(
            "bearer JWT signature verification failed".to_string(),
        ));
    }

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid bearer JWT payload base64url".into()))?;
    let claims: Value = serde_json::from_slice(&payload_bytes)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid bearer JWT payload JSON".into()))?;
    let obj = claims.as_object().ok_or_else(|| {
        IdTokenVerifyError::Invalid("bearer JWT payload must be an object".into())
    })?;

    let iss = obj.get("iss").and_then(Value::as_str).unwrap_or("");
    if iss.trim_end_matches('/') != expected_issuer.trim_end_matches('/') {
        return Err(IdTokenVerifyError::Invalid(
            "bearer JWT iss mismatch".to_string(),
        ));
    }

    let aud_values: Vec<&str> = match obj.get("aud") {
        Some(Value::String(s)) => vec![s.as_str()],
        Some(Value::Array(arr)) => arr.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };
    if aud_values.is_empty() {
        return Err(IdTokenVerifyError::Invalid(
            "bearer JWT missing aud claim".to_string(),
        ));
    }

    if let Some(expected_res) = expected_resource.map(normalize_resource_uri) {
        let resource_matched = aud_values
            .iter()
            .any(|aud| normalize_resource_uri(aud) == expected_res);
        if !resource_matched {
            return Err(IdTokenVerifyError::Invalid(
                "bearer JWT aud does not match configured resource".to_string(),
            ));
        }
    } else {
        let client_matched = aud_values
            .iter()
            .any(|aud| allowed_clients.iter().any(|cid| cid == aud));
        if !client_matched {
            return Err(IdTokenVerifyError::Invalid(
                "bearer JWT aud does not include an allowed client_id".to_string(),
            ));
        }
    }

    if let Some(azp) = obj.get("azp").and_then(Value::as_str)
        && !allowed_clients.iter().any(|cid| cid == azp)
    {
        return Err(IdTokenVerifyError::Invalid(
            "bearer JWT azp mismatch".to_string(),
        ));
    }

    let exp = obj
        .get("exp")
        .and_then(Value::as_i64)
        .ok_or_else(|| IdTokenVerifyError::Invalid("bearer JWT missing numeric exp".into()))?;
    if exp <= now_secs.saturating_sub(CLOCK_SKEW_SECS) {
        return Err(IdTokenVerifyError::Invalid(
            "bearer JWT expired".to_string(),
        ));
    }

    let iat = obj
        .get("iat")
        .and_then(Value::as_i64)
        .ok_or_else(|| IdTokenVerifyError::Invalid("bearer JWT missing numeric iat".into()))?;
    if iat > now_secs.saturating_add(CLOCK_SKEW_SECS) {
        return Err(IdTokenVerifyError::Invalid(
            "bearer JWT iat is in the future".to_string(),
        ));
    }

    if let Some(nbf) = obj.get("nbf").and_then(Value::as_i64)
        && nbf > now_secs.saturating_add(CLOCK_SKEW_SECS)
    {
        return Err(IdTokenVerifyError::Invalid(
            "bearer JWT not yet valid".to_string(),
        ));
    }

    let sub = obj
        .get("sub")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| IdTokenVerifyError::Invalid("bearer JWT missing sub".into()))?
        .to_string();

    let email = obj
        .get("email")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let email_verified = match obj.get("email_verified") {
        Some(Value::Bool(b)) => Some(*b),
        None | Some(Value::Null) => None,
        Some(_) => Some(false),
    };
    let name = obj
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    Ok(VerifiedIdToken {
        sub,
        email,
        email_verified,
        name,
    })
}

/// Authenticate an `Authorization: Bearer <jwt>` credential as `Principal::Operator`
/// against the configured OIDC issuer, JWKS, RFC 8707 resource audience, and
/// `ANYAGENTS_ADMIN_EMAILS`.
pub(crate) async fn authenticate_bearer_token(
    data_dir: &Path,
    raw_token: &str,
) -> Result<Principal, AuthError> {
    let cfg = OidcConfig::from_env().ok_or_else(|| {
        AuthError::Unauthenticated("OIDC bearer authentication is not configured".to_string())
    })?;
    let expected_resource = expected_resource_audience(data_dir);
    let clients = allowed_client_ids(&cfg);

    let discovery = fetch_discovery(&cfg.issuer)
        .await
        .map_err(AuthError::Unauthenticated)?;
    let jwks = fetch_jwks(&discovery.jwks_uri, false)
        .await
        .map_err(AuthError::Unauthenticated)?;

    let verified = match verify_bearer_jwt_claims(
        raw_token,
        &jwks,
        &cfg.issuer,
        expected_resource.as_deref(),
        &clients,
        unix_now_secs(),
    ) {
        Ok(claims) => claims,
        Err(IdTokenVerifyError::UnknownKid) => {
            let refreshed = fetch_jwks(&discovery.jwks_uri, true)
                .await
                .map_err(AuthError::Unauthenticated)?;
            verify_bearer_jwt_claims(
                raw_token,
                &refreshed,
                &cfg.issuer,
                expected_resource.as_deref(),
                &clients,
                unix_now_secs(),
            )
            .map_err(|e| AuthError::Unauthenticated(e.to_string()))?
        }
        Err(IdTokenVerifyError::Invalid(msg)) => {
            return Err(AuthError::Unauthenticated(msg));
        }
    };

    let needs_userinfo = verified.email_verified != Some(false)
        && (verified.email.is_none() || verified.email_verified.is_none());
    let userinfo = if needs_userinfo && let Some(ui_url) = discovery.userinfo_endpoint.as_deref() {
        fetch_userinfo(ui_url, raw_token).await.ok()
    } else {
        None
    };

    authorize_admin_identity(&verified, userinfo.as_ref(), &cfg.allowed_emails).map_err(|err| {
        match err {
            AdminAuthzError::Unauthenticated(msg) => AuthError::Unauthenticated(msg),
            AdminAuthzError::Forbidden(msg) => AuthError::Forbidden(msg),
        }
    })?;

    Ok(Principal::Operator)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex};

    use anyagents_core::Store;
    use anyagents_supervisor::HealthBoard;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt as _;
    use ring::rand::SystemRandom;
    use ring::signature::{
        ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, Ed25519KeyPair, KeyPair as _,
    };
    use tower::ServiceExt as _;

    use super::*;
    use crate::server::admin_auth::parse_jwks;

    struct EnvRestore(Vec<(&'static str, Option<OsString>)>);

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

    fn sign_eddsa(kp: &Ed25519KeyPair, kid: &str, claims: &Value) -> String {
        let header = json!({ "alg": "EdDSA", "typ": "JWT", "kid": kid });
        let h = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
        let p = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let input = format!("{h}.{p}");
        let sig = kp.sign(input.as_bytes());
        format!("{input}.{}", URL_SAFE_NO_PAD.encode(sig.as_ref()))
    }

    fn sign_es256(kp: &EcdsaKeyPair, rng: &SystemRandom, kid: &str, claims: &Value) -> String {
        let header = json!({ "alg": "ES256", "typ": "JWT", "kid": kid });
        let h = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
        let p = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let input = format!("{h}.{p}");
        let sig = kp.sign(rng, input.as_bytes()).unwrap();
        format!("{input}.{}", URL_SAFE_NO_PAD.encode(sig.as_ref()))
    }

    fn eddsa_jwk_json(kp: &Ed25519KeyPair, kid: &str) -> Value {
        json!({
            "keys": [{
                "kty": "OKP",
                "crv": "Ed25519",
                "use": "sig",
                "alg": "EdDSA",
                "kid": kid,
                "x": URL_SAFE_NO_PAD.encode(kp.public_key().as_ref()),
            }]
        })
    }

    fn es256_jwk_json(kp: &EcdsaKeyPair, kid: &str) -> Value {
        let pub_bytes = kp.public_key().as_ref();
        assert_eq!(pub_bytes.len(), 65);
        assert_eq!(pub_bytes[0], 0x04);
        json!({
            "keys": [{
                "kty": "EC",
                "crv": "P-256",
                "use": "sig",
                "alg": "ES256",
                "kid": kid,
                "x": URL_SAFE_NO_PAD.encode(&pub_bytes[1..33]),
                "y": URL_SAFE_NO_PAD.encode(&pub_bytes[33..65]),
            }]
        })
    }

    #[test]
    fn rfc8707_resource_audience_acceptance_and_mismatch_rejection() {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let kp = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .unwrap();
        let jwks =
            parse_jwks(&serde_json::to_vec(&es256_jwk_json(&kp, "k-es256")).unwrap()).unwrap();

        let now = 1_750_000_000i64;
        let issuer = "https://idp.example.com";
        let resource = "https://coord.example.com";
        let clients = vec!["cli-device-client".to_string()];
        let allowed_emails: BTreeSet<String> =
            ["ops@example.com".to_string()].into_iter().collect();

        // 1. Exact resource URI match (with trailing slash normalization) succeeds.
        let valid_jwt = sign_es256(
            &kp,
            &rng,
            "k-es256",
            &json!({
                "iss": issuer,
                "sub": "user-1",
                "aud": "https://coord.example.com/",
                "azp": "cli-device-client",
                "email": "Ops@Example.com",
                "email_verified": true,
                "iat": now - 10,
                "exp": now + 600,
            }),
        );
        let verified =
            verify_bearer_jwt_claims(&valid_jwt, &jwks, issuer, Some(resource), &clients, now)
                .expect("matching RFC 8707 resource aud must succeed");
        let authz = authorize_admin_identity(&verified, None, &allowed_emails).unwrap();
        assert_eq!(authz.email, "ops@example.com");

        // 2. Array aud containing configured resource succeeds.
        let array_aud_jwt = sign_es256(
            &kp,
            &rng,
            "k-es256",
            &json!({
                "iss": issuer,
                "sub": "user-1",
                "aud": ["https://other.example.com", "https://coord.example.com"],
                "email": "ops@example.com",
                "email_verified": true,
                "iat": now - 10,
                "exp": now + 600,
            }),
        );
        assert!(
            verify_bearer_jwt_claims(&array_aud_jwt, &jwks, issuer, Some(resource), &clients, now,)
                .is_ok()
        );

        // 3. Mismatched resource URI MUST fail closed!
        let wrong_res_jwt = sign_es256(
            &kp,
            &rng,
            "k-es256",
            &json!({
                "iss": issuer,
                "sub": "user-1",
                "aud": "https://evil-or-other.example.com",
                "email": "ops@example.com",
                "email_verified": true,
                "iat": now - 10,
                "exp": now + 600,
            }),
        );
        let err =
            verify_bearer_jwt_claims(&wrong_res_jwt, &jwks, issuer, Some(resource), &clients, now)
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("aud does not match configured resource"),
            "unexpected error: {err}"
        );

        // 4. Client-id-only aud MUST fail closed when a resource URI is configured!
        let client_only_jwt = sign_es256(
            &kp,
            &rng,
            "k-es256",
            &json!({
                "iss": issuer,
                "sub": "user-1",
                "aud": "cli-device-client",
                "email": "ops@example.com",
                "email_verified": true,
                "iat": now - 10,
                "exp": now + 600,
            }),
        );
        assert!(
            verify_bearer_jwt_claims(
                &client_only_jwt,
                &jwks,
                issuer,
                Some(resource),
                &clients,
                now,
            )
            .is_err()
        );

        // 5. alg:none and unverified emails fail closed.
        let none_header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let none_payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "iss": issuer,
                "sub": "user-1",
                "aud": resource,
                "email": "ops@example.com",
                "email_verified": true,
                "iat": now,
                "exp": now + 600,
            }))
            .unwrap(),
        );
        let none_jwt = format!("{none_header}.{none_payload}.sig");
        assert!(
            verify_bearer_jwt_claims(&none_jwt, &jwks, issuer, Some(resource), &clients, now)
                .is_err()
        );

        let unverified_jwt = sign_es256(
            &kp,
            &rng,
            "k-es256",
            &json!({
                "iss": issuer,
                "sub": "user-1",
                "aud": resource,
                "email": "ops@example.com",
                "email_verified": false,
                "iat": now - 10,
                "exp": now + 600,
            }),
        );
        let parsed_unverified = verify_bearer_jwt_claims(
            &unverified_jwt,
            &jwks,
            issuer,
            Some(resource),
            &clients,
            now,
        )
        .unwrap();
        assert!(matches!(
            authorize_admin_identity(&parsed_unverified, None, &allowed_emails),
            Err(AdminAuthzError::Unauthenticated(_))
        ));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn end_to_end_oidc_bearer_and_rfc9728_discovery_on_control_plane_router() {
        let _env = crate::server::tests::env_lock();
        let _restore = EnvRestore::capture(&[
            "ANYAGENTS_SERVERLESS",
            "ANYAGENTS_OPERATOR_TOKEN",
            "ANYAGENTS_OPERATOR_TOKEN_FILE",
            "ANYAGENTS_ADMIN_OIDC_ISSUER",
            "ANYAGENTS_ADMIN_OIDC_CLIENT_ID",
            "ANYAGENTS_ADMIN_OIDC_DEVICE_CLIENT_ID",
            "ANYAGENTS_ADMIN_OIDC_RESOURCE",
            "ANYAGENTS_ADMIN_EMAILS",
            "ANYAGENTS_PUBLIC_URL",
        ]);

        // Start a local mock OIDC IdP serving discovery + JWKS.
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let jwks_doc = eddsa_jwk_json(&kp, "k-eddsa-1");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer = format!("http://{addr}");
        let issuer_for_server = issuer.clone();
        let jwks_for_server = jwks_doc.clone();
        let idp_app = Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(move || {
                    let iss = issuer_for_server.clone();
                    async move {
                        Json(json!({
                            "issuer": iss,
                            "authorization_endpoint": format!("{iss}/authorize"),
                            "token_endpoint": format!("{iss}/token"),
                            "device_authorization_endpoint": format!("{iss}/device"),
                            "jwks_uri": format!("{iss}/jwks"),
                        }))
                    }
                }),
            )
            .route(
                "/jwks",
                get(move || {
                    let doc = jwks_for_server.clone();
                    async move { Json(doc) }
                }),
            );
        let idp_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, idp_app).await;
        });

        let coord_url = "https://control.example.com";
        unsafe {
            std::env::set_var("ANYAGENTS_SERVERLESS", "true");
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN");
            std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
            std::env::set_var("ANYAGENTS_ADMIN_OIDC_ISSUER", &issuer);
            std::env::set_var("ANYAGENTS_ADMIN_OIDC_CLIENT_ID", "web-console");
            std::env::set_var("ANYAGENTS_ADMIN_OIDC_DEVICE_CLIENT_ID", "cli-device");
            std::env::remove_var("ANYAGENTS_ADMIN_OIDC_RESOURCE");
            std::env::set_var("ANYAGENTS_ADMIN_EMAILS", "admin@example.com");
            std::env::set_var("ANYAGENTS_PUBLIC_URL", coord_url);
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let auth = crate::auth::Mode::from_env(dir.path());
        let state = AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(crate::server::types::ProcessRunner)),
            auth,
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
            timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        };
        let app = crate::server::build_webhook_router(state);

        // 1. RFC 9728 discovery advertises resource, issuer, and device client_id without secrets.
        let disc_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-protected-resource")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disc_resp.status(), StatusCode::OK);
        let disc_json: Value =
            serde_json::from_slice(&disc_resp.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(disc_json["resource"], json!(coord_url));
        assert_eq!(disc_json["issuer"], json!(issuer));
        assert_eq!(disc_json["client_id"], json!("cli-device"));

        let now = unix_now_secs();
        // 2. Valid EdDSA Bearer JWT scoped to ANYAGENTS_PUBLIC_URL authenticates as Principal::Operator.
        let ok_jwt = sign_eddsa(
            &kp,
            "k-eddsa-1",
            &json!({
                "iss": issuer,
                "sub": "sub-1",
                "aud": coord_url,
                "azp": "cli-device",
                "email": "admin@example.com",
                "email_verified": true,
                "iat": now - 5,
                "exp": now + 600,
            }),
        );
        let ok_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .header("authorization", format!("Bearer {ok_jwt}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_resp.status(), StatusCode::OK);

        // 3. Bearer JWT with mismatched RFC 8707 resource is rejected (401).
        let bad_aud_jwt = sign_eddsa(
            &kp,
            "k-eddsa-1",
            &json!({
                "iss": issuer,
                "sub": "sub-1",
                "aud": "https://wrong-coordinator.example.com",
                "email": "admin@example.com",
                "email_verified": true,
                "iat": now - 5,
                "exp": now + 600,
            }),
        );
        let bad_aud_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .header("authorization", format!("Bearer {bad_aud_jwt}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_aud_resp.status(), StatusCode::UNAUTHORIZED);

        // 4. Bearer JWT for non-allowlisted email is rejected (403).
        let wrong_email_jwt = sign_eddsa(
            &kp,
            "k-eddsa-1",
            &json!({
                "iss": issuer,
                "sub": "sub-2",
                "aud": coord_url,
                "email": "intruder@example.com",
                "email_verified": true,
                "iat": now - 5,
                "exp": now + 600,
            }),
        );
        let wrong_email_resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/agents")
                    .header("authorization", format!("Bearer {wrong_email_jwt}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_email_resp.status(), StatusCode::FORBIDDEN);

        idp_handle.abort();
    }
}
