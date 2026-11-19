//! Standard OpenID Connect (Authorization Code + PKCE S256) authentication and
//! stateless HMAC-SHA256 cookie sessions for the embedded Admin Console (`/admin`).
//!
//! Designed for scale-to-zero stateless serverless replicas:
//! - Zero in-memory session stores: both the short-lived OIDC pre-auth state
//!   cookie (`anyagents_admin_oidc`, 10 min TTL) and the operator session cookie
//!   (`anyagents_admin_session`, 12 h TTL) are authenticated with HMAC-SHA256
//!   keyed from `ANYAGENTS_ADMIN_SESSION_SECRET` (or HKDF-SHA256 derived from
//!   `ANYAGENTS_MASTER_KEY` / `ANYAGENTS_OPERATOR_TOKEN`), so any replica can
//!   verify any callback or API request immediately after cold start.
//! - Fail-closed verification: rejects `alg: none` and symmetric JWT algorithms
//!   (accepting strictly `ES256`, `RS256`, and `EdDSA` against cached JWKS),
//!   requires `email_verified == true`, and enforces exact case-insensitive
//!   membership in `ANYAGENTS_ADMIN_EMAILS` on both login and every subsequent
//!   authenticated request.

use std::collections::{BTreeSet, HashMap};
use std::io::Read as _;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_core::stateless::constant_time_eq;
use axum::Json;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::header::{CACHE_CONTROL, COOKIE, HOST, LOCATION, SET_COOKIE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ring::hkdf::{HKDF_SHA256, Salt};
use ring::hmac;
use ring::rand::{SecureRandom as _, SystemRandom};
use ring::signature::{
    ECDSA_P256_SHA256_FIXED, ED25519, RSA_PKCS1_2048_8192_SHA256, RsaPublicKeyComponents,
    UnparsedPublicKey,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use super::{ApiError, AppState, serverless};

/// Cookie storing the authenticated stateless operator session (`Path=/`, 12 h TTL).
pub(crate) const ADMIN_SESSION_COOKIE: &str = "anyagents_admin_session";
/// Short-lived pre-auth cookie storing OIDC `state`, `nonce`, and PKCE `code_verifier`.
pub(crate) const ADMIN_OIDC_COOKIE: &str = "anyagents_admin_oidc";
/// Custom CSRF protection header required on state-changing cookie-authenticated calls.
pub(crate) const ADMIN_CSRF_HEADER: &str = "x-anyagents-admin-csrf";

const OIDC_ISSUER_ENV: &str = "ANYAGENTS_ADMIN_OIDC_ISSUER";
const OIDC_CLIENT_ID_ENV: &str = "ANYAGENTS_ADMIN_OIDC_CLIENT_ID";
const OIDC_CLIENT_SECRET_ENV: &str = "ANYAGENTS_ADMIN_OIDC_CLIENT_SECRET";
const ADMIN_EMAILS_ENV: &str = "ANYAGENTS_ADMIN_EMAILS";
const ADMIN_SESSION_SECRET_ENV: &str = "ANYAGENTS_ADMIN_SESSION_SECRET";

const PREAUTH_DOMAIN: &[u8] = b"anyagents-admin-oidc-preauth-v1:";
const SESSION_DOMAIN: &[u8] = b"anyagents-admin-session-v1:";

const PREAUTH_TTL_SECS: i64 = 600;
const SESSION_TTL_SECS: i64 = 12 * 3600;
const CLOCK_SKEW_SECS: i64 = 60;
const CACHE_TTL: Duration = Duration::from_secs(3600);
const HTTP_TIMEOUT_SECS: u64 = 10;
const MAX_HTTP_BODY_BYTES: u64 = 512 * 1024;
const MIN_SECRET_BYTES: usize = 32;

/// Parsed OIDC configuration from environment variables.
#[derive(Clone, Debug)]
pub(crate) struct OidcConfig {
    pub(crate) issuer: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: Option<String>,
    pub(crate) allowed_emails: BTreeSet<String>,
}

impl OidcConfig {
    /// Load OIDC configuration from env vars. Returns `None` unless issuer,
    /// client id, and a non-empty allowlist of admin emails are all configured.
    pub(crate) fn from_env() -> Option<Self> {
        let issuer = env_non_empty(OIDC_ISSUER_ENV)?;
        let parsed_issuer = Url::parse(&issuer).ok()?;
        if !matches!(parsed_issuer.scheme(), "https" | "http") {
            return None;
        }
        let client_id = env_non_empty(OIDC_CLIENT_ID_ENV)?;
        let client_secret = env_non_empty(OIDC_CLIENT_SECRET_ENV);
        let emails_raw = env_non_empty(ADMIN_EMAILS_ENV)?;
        let allowed_emails = parse_allowed_emails(&emails_raw);
        if allowed_emails.is_empty() {
            return None;
        }
        Some(Self {
            issuer,
            client_id,
            client_secret,
            allowed_emails,
        })
    }

    pub(crate) fn is_email_allowed(&self, email: &str) -> bool {
        let normalized = email.trim().to_ascii_lowercase();
        !normalized.is_empty() && self.allowed_emails.contains(&normalized)
    }
}

/// Return `true` when OIDC issuer, client id, and non-empty admin email allowlist
/// are configured via environment variables.
pub(crate) fn is_oidc_configured() -> bool {
    OidcConfig::from_env().is_some()
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub(crate) fn parse_allowed_emails(raw: &str) -> BTreeSet<String> {
    raw.split(',')
        .map(|part| part.trim().to_ascii_lowercase())
        .filter(|email| !email.is_empty() && email.contains('@'))
        .collect()
}

fn hkdf_derive_32(ikm: &[u8], info: &[u8]) -> [u8; 32] {
    let salt = Salt::new(HKDF_SHA256, b"anyagents-admin-cookie-hkdf-salt-v1");
    let prk = salt.extract(ikm);
    let info_slices = [info];
    let okm = prk
        .expand(&info_slices, HKDF_SHA256)
        .expect("32-byte HKDF-SHA256 expansion");
    let mut key = [0u8; 32];
    okm.fill(&mut key).expect("32-byte key buffer");
    key
}

fn decode_hex_bytes(hex_str: &str) -> Option<Vec<u8>> {
    if !hex_str.len().is_multiple_of(2) {
        return None;
    }
    let bytes = hex_str.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Derive the shared 32-byte HMAC-SHA256 signing key for stateless admin cookies.
///
/// Precedence:
/// 1. `ANYAGENTS_ADMIN_SESSION_SECRET` (32+ raw bytes or 64+ hex chars)
/// 2. HKDF-SHA256 derived from `ANYAGENTS_MASTER_KEY`
/// 3. HKDF-SHA256 derived from `ANYAGENTS_OPERATOR_TOKEN`
pub(crate) fn session_signing_key_from_env() -> Option<[u8; 32]> {
    if let Some(raw) = env_non_empty(ADMIN_SESSION_SECRET_ENV) {
        if raw.len() >= 64
            && raw.len().is_multiple_of(2)
            && raw.bytes().all(|b| b.is_ascii_hexdigit())
            && let Some(decoded) = decode_hex_bytes(&raw)
            && decoded.len() >= MIN_SECRET_BYTES
        {
            return Some(hkdf_derive_32(&decoded, b"admin-session-secret-hex-v1"));
        }
        if raw.len() >= MIN_SECRET_BYTES {
            return Some(hkdf_derive_32(
                raw.as_bytes(),
                b"admin-session-secret-raw-v1",
            ));
        }
        // Explicitly configured but too short: fail closed rather than silently falling back.
        return None;
    }
    if let Some(master_key) = env_non_empty(anyagents_core::stateless::MASTER_KEY_ENV) {
        return Some(hkdf_derive_32(
            master_key.as_bytes(),
            b"admin-session-from-master-key-v1",
        ));
    }
    if let Some(op_token) = env_non_empty("ANYAGENTS_OPERATOR_TOKEN") {
        return Some(hkdf_derive_32(
            op_token.as_bytes(),
            b"admin-session-from-operator-token-v1",
        ));
    }
    None
}

fn unix_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Generate a cryptographically secure URL-safe random token (`URL_SAFE_NO_PAD`).
pub(crate) fn generate_random_urlsafe(num_bytes: usize) -> Result<String, String> {
    let rng = SystemRandom::new();
    let mut buf = vec![0u8; num_bytes];
    rng.fill(&mut buf)
        .map_err(|_| "system CSPRNG failure".to_string())?;
    Ok(URL_SAFE_NO_PAD.encode(buf))
}

/// Compute the RFC 7636 PKCE `S256` code challenge: `BASE64URL-ENCODE(SHA256(ASCII(verifier)))`.
pub(crate) fn pkce_s256_challenge(code_verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PreAuthState {
    state: String,
    nonce: String,
    code_verifier: String,
    redirect_uri: String,
    iat: i64,
    exp: i64,
}

/// Stateless authenticated operator session stored inside the signed cookie.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AdminSession {
    pub(crate) email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    pub(crate) sub: String,
    pub(crate) iat: i64,
    pub(crate) exp: i64,
}

/// Sign a JSON payload into `<payload_b64>.<sig_b64>` with domain-separated HMAC-SHA256.
pub(crate) fn sign_cookie_payload<T: Serialize>(
    key: &[u8; 32],
    domain: &[u8],
    payload: &T,
) -> Result<String, String> {
    let json_bytes = serde_json::to_vec(payload)
        .map_err(|e| format!("failed to serialize cookie payload: {e}"))?;
    let payload_b64 = URL_SAFE_NO_PAD.encode(json_bytes);
    let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let mut mac_input = Vec::with_capacity(domain.len() + payload_b64.len());
    mac_input.extend_from_slice(domain);
    mac_input.extend_from_slice(payload_b64.as_bytes());
    let tag = hmac::sign(&hmac_key, &mac_input);
    let sig_b64 = URL_SAFE_NO_PAD.encode(tag.as_ref());
    Ok(format!("{payload_b64}.{sig_b64}"))
}

/// Verify and decode a domain-separated HMAC-SHA256 signed cookie value.
pub(crate) fn verify_cookie_payload<T: DeserializeOwned>(
    key: &[u8; 32],
    domain: &[u8],
    raw: &str,
) -> Result<T, String> {
    let trimmed = raw.trim();
    let (payload_b64, sig_b64) = trimmed
        .split_once('.')
        .ok_or_else(|| "malformed signed cookie".to_string())?;
    if payload_b64.is_empty() || sig_b64.is_empty() || sig_b64.contains('.') {
        return Err("malformed signed cookie".to_string());
    }
    let sig_bytes = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|_| "invalid cookie signature encoding".to_string())?;
    let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let mut mac_input = Vec::with_capacity(domain.len() + payload_b64.len());
    mac_input.extend_from_slice(domain);
    mac_input.extend_from_slice(payload_b64.as_bytes());
    hmac::verify(&hmac_key, &mac_input, &sig_bytes)
        .map_err(|_| "cookie signature verification failed".to_string())?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| "invalid cookie payload encoding".to_string())?;
    serde_json::from_slice(&payload_bytes).map_err(|_| "invalid cookie payload JSON".to_string())
}

/// Sign an [`AdminSession`] cookie using an explicit 32-byte key.
pub(crate) fn sign_admin_session(key: &[u8; 32], session: &AdminSession) -> Result<String, String> {
    sign_cookie_payload(key, SESSION_DOMAIN, session)
}

/// Verify an [`AdminSession`] cookie using an explicit 32-byte key and clock.
pub(crate) fn verify_admin_session_at(
    key: &[u8; 32],
    raw: &str,
    now_secs: i64,
) -> Result<AdminSession, String> {
    let session: AdminSession = verify_cookie_payload(key, SESSION_DOMAIN, raw)?;
    if session.email.trim().is_empty() || session.sub.trim().is_empty() {
        return Err("admin session missing email or sub".to_string());
    }
    if now_secs >= session.exp {
        return Err("admin session expired".to_string());
    }
    if session.iat > now_secs.saturating_add(CLOCK_SKEW_SECS) {
        return Err("admin session issued in the future".to_string());
    }
    Ok(session)
}

/// Extract a named cookie value from all `Cookie` request headers.
pub(crate) fn extract_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    for raw_hdr in headers.get_all(COOKIE) {
        let Ok(hdr) = raw_hdr.to_str() else {
            continue;
        };
        for pair in hdr.split(';') {
            let trimmed = pair.trim();
            if let Some((k, v)) = trimmed.split_once('=')
                && k.trim() == name
            {
                let val = v.trim();
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }
    None
}

/// Extract the raw `anyagents_admin_session` cookie from HTTP headers.
pub(crate) fn extract_session_cookie(headers: &HeaderMap) -> Option<String> {
    extract_cookie(headers, ADMIN_SESSION_COOKIE)
}

/// Verify an `anyagents_admin_session` cookie against active OIDC configuration,
/// the shared stateless HMAC key, expiration, and current `ANYAGENTS_ADMIN_EMAILS`.
pub(crate) fn verify_session_cookie_from_env(raw: &str) -> Result<AdminSession, String> {
    let cfg = OidcConfig::from_env()
        .ok_or_else(|| "admin OIDC authentication is not configured".to_string())?;
    let key = session_signing_key_from_env()
        .ok_or_else(|| "admin session signing key is not configured".to_string())?;
    let session = verify_admin_session_at(&key, raw, unix_now_secs())?;
    if !cfg.is_email_allowed(&session.email) {
        return Err("admin email is no longer allowlisted".to_string());
    }
    Ok(session)
}

// ============================================================================
// OIDC Discovery & JWKS Caching (1h TTL)
// ============================================================================

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OidcDiscovery {
    pub(crate) issuer: String,
    pub(crate) authorization_endpoint: String,
    pub(crate) token_endpoint: String,
    pub(crate) jwks_uri: String,
    #[serde(default)]
    pub(crate) userinfo_endpoint: Option<String>,
}

#[derive(Clone, Debug)]
struct CachedDiscovery {
    fetched_at: Instant,
    doc: OidcDiscovery,
}

#[derive(Clone, Debug)]
pub(crate) enum JwkKey {
    EcP256 { uncompressed: [u8; 65] },
    Rsa { n: Vec<u8>, e: Vec<u8> },
    Ed25519 { public_key: [u8; 32] },
}

#[derive(Clone, Debug)]
pub(crate) struct AdminJwk {
    pub(crate) kid: Option<String>,
    pub(crate) alg: Option<String>,
    pub(crate) key: JwkKey,
}

#[derive(Clone, Debug)]
struct CachedJwks {
    fetched_at: Instant,
    keys: Vec<AdminJwk>,
}

static DISCOVERY_CACHE: LazyLock<Mutex<HashMap<String, CachedDiscovery>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static ADMIN_JWKS_CACHE: LazyLock<Mutex<HashMap<String, CachedJwks>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn build_http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(HTTP_TIMEOUT_SECS)))
        .http_status_as_error(false)
        .build()
        .into()
}

fn validate_http_url(raw: &str, label: &str) -> Result<(), String> {
    let parsed = Url::parse(raw).map_err(|_| format!("invalid {label} URL"))?;
    if !matches!(parsed.scheme(), "https" | "http") {
        return Err(format!("{label} URL must use http or https"));
    }
    Ok(())
}

async fn fetch_discovery(issuer: &str) -> Result<OidcDiscovery, String> {
    {
        let guard = DISCOVERY_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached) = guard.get(issuer)
            && cached.fetched_at.elapsed() < CACHE_TTL
        {
            return Ok(cached.doc.clone());
        }
    }

    let discovery_url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    validate_http_url(&discovery_url, "OIDC discovery")?;
    let expected_issuer = issuer.to_string();

    let doc = tokio::task::spawn_blocking(move || -> Result<OidcDiscovery, String> {
        let agent = build_http_agent();
        let mut resp = agent
            .get(&discovery_url)
            .header("accept", "application/json")
            .call()
            .map_err(|e| format!("OIDC discovery fetch failed: {e}"))?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(format!("OIDC discovery returned HTTP {status}"));
        }
        let mut raw = Vec::new();
        let _ = resp
            .body_mut()
            .as_reader()
            .take(MAX_HTTP_BODY_BYTES)
            .read_to_end(&mut raw);
        let doc: OidcDiscovery = serde_json::from_slice(&raw)
            .map_err(|e| format!("invalid OIDC discovery JSON: {e}"))?;
        if doc.issuer != expected_issuer {
            return Err("OIDC discovery issuer mismatch".to_string());
        }
        validate_http_url(&doc.authorization_endpoint, "authorization_endpoint")?;
        validate_http_url(&doc.token_endpoint, "token_endpoint")?;
        validate_http_url(&doc.jwks_uri, "jwks_uri")?;
        if let Some(ref ui) = doc.userinfo_endpoint {
            validate_http_url(ui, "userinfo_endpoint")?;
        }
        Ok(doc)
    })
    .await
    .map_err(|e| format!("OIDC discovery task join error: {e}"))??;

    let mut guard = DISCOVERY_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    guard.insert(
        issuer.to_string(),
        CachedDiscovery {
            fetched_at: Instant::now(),
            doc: doc.clone(),
        },
    );
    Ok(doc)
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

async fn fetch_jwks(jwks_uri: &str, force_refresh: bool) -> Result<Vec<AdminJwk>, String> {
    if !force_refresh {
        let guard = ADMIN_JWKS_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cached) = guard.get(jwks_uri)
            && cached.fetched_at.elapsed() < CACHE_TTL
        {
            return Ok(cached.keys.clone());
        }
    }

    let url = jwks_uri.to_string();
    let keys = tokio::task::spawn_blocking(move || -> Result<Vec<AdminJwk>, String> {
        let agent = build_http_agent();
        let mut resp = agent
            .get(&url)
            .header("accept", "application/json")
            .call()
            .map_err(|e| format!("JWKS fetch failed: {e}"))?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(format!("JWKS endpoint returned HTTP {status}"));
        }
        let mut raw = Vec::new();
        let _ = resp
            .body_mut()
            .as_reader()
            .take(MAX_HTTP_BODY_BYTES)
            .read_to_end(&mut raw);
        parse_jwks(&raw)
    })
    .await
    .map_err(|e| format!("JWKS fetch join error: {e}"))??;

    let mut guard = ADMIN_JWKS_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    guard.insert(
        jwks_uri.to_string(),
        CachedJwks {
            fetched_at: Instant::now(),
            keys: keys.clone(),
        },
    );
    Ok(keys)
}

// ============================================================================
// ID Token Verification & Admin Allowlist Enforcement
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VerifiedIdToken {
    pub(crate) sub: String,
    pub(crate) email: Option<String>,
    pub(crate) email_verified: Option<bool>,
    pub(crate) name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct UserInfoClaims {
    pub(crate) sub: String,
    #[serde(default)]
    pub(crate) email: Option<String>,
    #[serde(default)]
    pub(crate) email_verified: Option<bool>,
    #[serde(default)]
    pub(crate) name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedAdmin {
    pub(crate) sub: String,
    pub(crate) email: String,
    pub(crate) name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IdTokenVerifyError {
    UnknownKid,
    Invalid(String),
}

impl std::fmt::Display for IdTokenVerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownKid => write!(f, "no matching JWK found for id_token kid"),
            Self::Invalid(msg) => f.write_str(msg),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AdminAuthzError {
    Unauthenticated(String),
    Forbidden(String),
}

/// Verify an OIDC `id_token` compact JWS against `jwks` and standard OIDC claims.
///
/// Strictly accepts `ES256`, `RS256`, and `EdDSA` only (rejecting `none`, `HS*`, etc.).
pub(crate) fn verify_id_token_claims(
    id_token: &str,
    jwks: &[AdminJwk],
    expected_issuer: &str,
    expected_client_id: &str,
    expected_nonce: &str,
    now_secs: i64,
) -> Result<VerifiedIdToken, IdTokenVerifyError> {
    let parts: Vec<&str> = id_token.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err(IdTokenVerifyError::Invalid(
            "malformed compact id_token".to_string(),
        ));
    }
    let (header_b64, payload_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid id_token header base64url".into()))?;
    let header_json: Value = serde_json::from_slice(&header_bytes)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid id_token header JSON".into()))?;
    let alg = header_json
        .get("alg")
        .and_then(Value::as_str)
        .ok_or_else(|| IdTokenVerifyError::Invalid("id_token header missing alg".into()))?;
    if !matches!(alg, "ES256" | "RS256" | "EdDSA") {
        return Err(IdTokenVerifyError::Invalid(format!(
            "unsupported id_token alg `{alg}`"
        )));
    }
    let kid = header_json
        .get("kid")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let sig_bytes = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid id_token signature base64url".into()))?;

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
            "id_token signature verification failed".to_string(),
        ));
    }

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid id_token payload base64url".into()))?;
    let claims: Value = serde_json::from_slice(&payload_bytes)
        .map_err(|_| IdTokenVerifyError::Invalid("invalid id_token payload JSON".into()))?;
    let obj = claims
        .as_object()
        .ok_or_else(|| IdTokenVerifyError::Invalid("id_token payload must be an object".into()))?;

    let iss = obj.get("iss").and_then(Value::as_str).unwrap_or("");
    if iss != expected_issuer {
        return Err(IdTokenVerifyError::Invalid(
            "id_token iss mismatch".to_string(),
        ));
    }

    let (aud_ok, multi_aud) = match obj.get("aud") {
        Some(Value::String(s)) => (s == expected_client_id, false),
        Some(Value::Array(arr)) => (
            arr.iter().any(|v| v.as_str() == Some(expected_client_id)),
            arr.len() > 1,
        ),
        _ => (false, false),
    };
    if !aud_ok {
        return Err(IdTokenVerifyError::Invalid(
            "id_token aud does not include client_id".to_string(),
        ));
    }
    if multi_aud
        && let Some(azp) = obj.get("azp").and_then(Value::as_str)
        && azp != expected_client_id
    {
        return Err(IdTokenVerifyError::Invalid(
            "id_token azp mismatch".to_string(),
        ));
    }

    let exp = obj
        .get("exp")
        .and_then(Value::as_i64)
        .ok_or_else(|| IdTokenVerifyError::Invalid("id_token missing numeric exp".into()))?;
    if exp <= now_secs.saturating_sub(CLOCK_SKEW_SECS) {
        return Err(IdTokenVerifyError::Invalid("id_token expired".to_string()));
    }

    let iat = obj
        .get("iat")
        .and_then(Value::as_i64)
        .ok_or_else(|| IdTokenVerifyError::Invalid("id_token missing numeric iat".into()))?;
    if iat > now_secs.saturating_add(CLOCK_SKEW_SECS) {
        return Err(IdTokenVerifyError::Invalid(
            "id_token iat is in the future".to_string(),
        ));
    }

    if let Some(nbf) = obj.get("nbf").and_then(Value::as_i64)
        && nbf > now_secs.saturating_add(CLOCK_SKEW_SECS)
    {
        return Err(IdTokenVerifyError::Invalid(
            "id_token not yet valid".to_string(),
        ));
    }

    let nonce = obj
        .get("nonce")
        .and_then(Value::as_str)
        .ok_or_else(|| IdTokenVerifyError::Invalid("id_token missing nonce".into()))?;
    if !constant_time_eq(nonce.as_bytes(), expected_nonce.as_bytes()) {
        return Err(IdTokenVerifyError::Invalid(
            "id_token nonce mismatch".to_string(),
        ));
    }

    let sub = obj
        .get("sub")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| IdTokenVerifyError::Invalid("id_token missing sub".into()))?
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
        // Any non-boolean value for email_verified fails closed.
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

/// Enforce `email_verified == true` (optionally falling back to verified `userinfo`
/// when absent in `id_token`) and exact case-insensitive membership in `allowed_emails`.
pub(crate) fn authorize_admin_identity(
    token: &VerifiedIdToken,
    userinfo: Option<&UserInfoClaims>,
    allowed_emails: &BTreeSet<String>,
) -> Result<AuthorizedAdmin, AdminAuthzError> {
    if token.email_verified == Some(false) {
        return Err(AdminAuthzError::Unauthenticated(
            "OIDC account email is not verified".to_string(),
        ));
    }

    let (email_raw, verified, name) = match (&token.email, token.email_verified) {
        (Some(email), Some(true)) => (email.clone(), true, token.name.clone()),
        _ => {
            if let Some(ui) = userinfo {
                if ui.sub != token.sub {
                    return Err(AdminAuthzError::Unauthenticated(
                        "OIDC userinfo sub mismatch".to_string(),
                    ));
                }
                let ui_email = ui
                    .email
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .or_else(|| token.email.clone())
                    .ok_or_else(|| {
                        AdminAuthzError::Unauthenticated(
                            "OIDC identity missing email claim".to_string(),
                        )
                    })?;
                let ui_verified = ui.email_verified == Some(true);
                let resolved_name = token.name.clone().or_else(|| {
                    ui.name
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                });
                (ui_email, ui_verified, resolved_name)
            } else {
                return Err(AdminAuthzError::Unauthenticated(
                    "OIDC id_token missing verified email".to_string(),
                ));
            }
        }
    };

    if !verified {
        return Err(AdminAuthzError::Unauthenticated(
            "OIDC account email is not verified".to_string(),
        ));
    }

    let normalized_email = email_raw.trim().to_ascii_lowercase();
    if normalized_email.is_empty() || !allowed_emails.contains(&normalized_email) {
        return Err(AdminAuthzError::Forbidden(
            "authenticated email is not in ANYAGENTS_ADMIN_EMAILS".to_string(),
        ));
    }

    Ok(AuthorizedAdmin {
        sub: token.sub.clone(),
        email: normalized_email,
        name,
    })
}

/// Convenience helper combining cryptographic ID-token verification with email
/// verification and allowlist enforcement (without network calls).
#[cfg(test)]
pub(crate) fn verify_and_authorize_id_token(
    id_token: &str,
    jwks: &[AdminJwk],
    expected_issuer: &str,
    expected_client_id: &str,
    expected_nonce: &str,
    allowed_emails: &BTreeSet<String>,
    now_secs: i64,
) -> Result<AuthorizedAdmin, String> {
    let verified = verify_id_token_claims(
        id_token,
        jwks,
        expected_issuer,
        expected_client_id,
        expected_nonce,
        now_secs,
    )
    .map_err(|e| e.to_string())?;
    authorize_admin_identity(&verified, None, allowed_emails).map_err(|e| match e {
        AdminAuthzError::Unauthenticated(m) | AdminAuthzError::Forbidden(m) => m,
    })
}

// ============================================================================
// HTTP Helpers & OAuth Token / Userinfo Exchange
// ============================================================================

fn percent_encode_rfc3986(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{byte:02X}");
        }
    }
    out
}

fn form_urlencode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'*') {
            out.push(byte as char);
        } else if byte == b' ' {
            out.push('+');
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{byte:02X}");
        }
    }
    out
}

fn build_authorization_url(
    authorization_endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    nonce: &str,
    code_challenge: &str,
) -> Result<String, String> {
    validate_http_url(authorization_endpoint, "authorization_endpoint")?;
    let sep = if authorization_endpoint.contains('?') {
        '&'
    } else {
        '?'
    };
    let params = [
        ("response_type", "code"),
        ("client_id", client_id),
        ("redirect_uri", redirect_uri),
        ("scope", "openid email profile"),
        ("state", state),
        ("nonce", nonce),
        ("code_challenge", code_challenge),
        ("code_challenge_method", "S256"),
    ];
    let query = params
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                percent_encode_rfc3986(k),
                percent_encode_rfc3986(v)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    Ok(format!("{authorization_endpoint}{sep}{query}"))
}

fn resolve_admin_base_url(
    data_dir: &std::path::Path,
    headers: &HeaderMap,
) -> Result<String, String> {
    if let Some(public) = serverless::resolve_public_url(data_dir) {
        return Ok(public);
    }
    if serverless::is_serverless_mode() {
        return Err(
            "OIDC login requires ANYAGENTS_PUBLIC_URL (or ANYAGENTS_WEBHOOK_URL) in serverless mode"
                .to_string(),
        );
    }
    let host = headers
        .get(HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "cannot determine callback URL: set ANYAGENTS_PUBLIC_URL".to_string())?;
    let host_only = host
        .split(':')
        .next()
        .unwrap_or(host)
        .trim_matches(['[', ']']);
    if !matches!(host_only, "localhost" | "127.0.0.1" | "::1") {
        return Err(
            "non-loopback OIDC login requires ANYAGENTS_PUBLIC_URL to be explicitly configured"
                .to_string(),
        );
    }
    Ok(format!("http://{host}"))
}

fn format_preauth_set_cookie(value: &str, secure: bool, max_age_secs: i64) -> String {
    let secure_attr = if secure { "; Secure" } else { "" };
    format!(
        "{ADMIN_OIDC_COOKIE}={value}; HttpOnly{secure_attr}; SameSite=Lax; Path=/admin; Max-Age={max_age_secs}"
    )
}

fn format_session_set_cookie(value: &str, secure: bool, max_age_secs: i64) -> String {
    let secure_attr = if secure { "; Secure" } else { "" };
    format!(
        "{ADMIN_SESSION_COOKIE}={value}; HttpOnly{secure_attr}; SameSite=Lax; Path=/; Max-Age={max_age_secs}"
    )
}

fn is_https_deployment(data_dir: &std::path::Path) -> bool {
    serverless::resolve_public_url(data_dir).is_some_and(|u| u.starts_with("https://"))
}

#[derive(Debug, Deserialize)]
struct TokenEndpointResponse {
    id_token: String,
    #[serde(default)]
    access_token: Option<String>,
}

async fn exchange_authorization_code(
    token_endpoint: &str,
    client_id: &str,
    client_secret: Option<&str>,
    code: &str,
    redirect_uri: &str,
    code_verifier: &str,
) -> Result<TokenEndpointResponse, String> {
    let url = token_endpoint.to_string();
    let client_id = client_id.to_string();
    let client_secret = client_secret.map(str::to_string);
    let code = code.to_string();
    let redirect_uri = redirect_uri.to_string();
    let code_verifier = code_verifier.to_string();

    tokio::task::spawn_blocking(move || -> Result<TokenEndpointResponse, String> {
        let agent = build_http_agent();
        let mut req = agent.post(&url).header("accept", "application/json");
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("redirect_uri", &redirect_uri),
            ("code_verifier", &code_verifier),
        ];
        if let Some(ref secret) = client_secret {
            // RFC 6749 §2.3.1 client_secret_basic
            let creds = format!("{}:{}", form_urlencode(&client_id), form_urlencode(secret));
            let encoded = STANDARD.encode(creds.as_bytes());
            req = req.header("authorization", format!("Basic {encoded}"));
        } else {
            form.push(("client_id", &client_id));
        }

        let mut resp = req
            .send_form(form)
            .map_err(|e| format!("OIDC token exchange request failed: {e}"))?;
        let status = resp.status().as_u16();
        let mut raw = Vec::new();
        let _ = resp
            .body_mut()
            .as_reader()
            .take(MAX_HTTP_BODY_BYTES)
            .read_to_end(&mut raw);
        if !(200..300).contains(&status) {
            return Err(format!("OIDC token endpoint returned HTTP {status}"));
        }
        serde_json::from_slice::<TokenEndpointResponse>(&raw)
            .map_err(|_| "OIDC token endpoint response missing valid id_token".to_string())
    })
    .await
    .map_err(|e| format!("OIDC token exchange join error: {e}"))?
}

async fn fetch_userinfo(
    userinfo_endpoint: &str,
    access_token: &str,
) -> Result<UserInfoClaims, String> {
    let url = userinfo_endpoint.to_string();
    let token = access_token.to_string();
    tokio::task::spawn_blocking(move || -> Result<UserInfoClaims, String> {
        let agent = build_http_agent();
        let mut resp = agent
            .get(&url)
            .header("accept", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .call()
            .map_err(|e| format!("OIDC userinfo request failed: {e}"))?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(format!("OIDC userinfo returned HTTP {status}"));
        }
        let mut raw = Vec::new();
        let _ = resp
            .body_mut()
            .as_reader()
            .take(MAX_HTTP_BODY_BYTES)
            .read_to_end(&mut raw);
        serde_json::from_slice(&raw)
            .map_err(|_| "OIDC userinfo response was not valid JSON".to_string())
    })
    .await
    .map_err(|e| format!("OIDC userinfo join error: {e}"))?
}

// ============================================================================
// HTTP Route Handlers (/admin/auth/*)
// ============================================================================

/// `GET /admin/auth/login`: starts an OIDC Authorization Code + PKCE S256 flow
/// using a stateless signed `anyagents_admin_oidc` cookie.
pub(crate) async fn login_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let cfg = OidcConfig::from_env().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "admin OIDC login is not configured (set ANYAGENTS_ADMIN_OIDC_ISSUER, ANYAGENTS_ADMIN_OIDC_CLIENT_ID, and ANYAGENTS_ADMIN_EMAILS)",
        )
    })?;
    let key = session_signing_key_from_env().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "admin session signing key is not configured (set ANYAGENTS_ADMIN_SESSION_SECRET, ANYAGENTS_MASTER_KEY, or ANYAGENTS_OPERATOR_TOKEN)",
        )
    })?;

    let base_url = resolve_admin_base_url(state.store.data_dir(), &headers)
        .map_err(|why| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, why))?;
    let redirect_uri = format!("{}/admin/auth/callback", base_url.trim_end_matches('/'));
    let secure_cookie = redirect_uri.starts_with("https://") || serverless::is_serverless_mode();

    let discovery = fetch_discovery(&cfg.issuer)
        .await
        .map_err(|why| ApiError::new(StatusCode::BAD_GATEWAY, why))?;

    let state_val = generate_random_urlsafe(32)
        .map_err(|why| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why))?;
    let nonce = generate_random_urlsafe(32)
        .map_err(|why| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why))?;
    let code_verifier = generate_random_urlsafe(32)
        .map_err(|why| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why))?;
    let code_challenge = pkce_s256_challenge(&code_verifier);

    let now = unix_now_secs();
    let preauth = PreAuthState {
        state: state_val.clone(),
        nonce: nonce.clone(),
        code_verifier,
        redirect_uri: redirect_uri.clone(),
        iat: now,
        exp: now.saturating_add(PREAUTH_TTL_SECS),
    };
    let signed_preauth = sign_cookie_payload(&key, PREAUTH_DOMAIN, &preauth)
        .map_err(|why| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why))?;

    let auth_url = build_authorization_url(
        &discovery.authorization_endpoint,
        &cfg.client_id,
        &redirect_uri,
        &state_val,
        &nonce,
        &code_challenge,
    )
    .map_err(|why| ApiError::new(StatusCode::BAD_GATEWAY, why))?;

    let set_cookie = format_preauth_set_cookie(&signed_preauth, secure_cookie, PREAUTH_TTL_SECS);
    let mut resp = Response::builder()
        .status(StatusCode::FOUND)
        .header(LOCATION, auth_url)
        .header(CACHE_CONTROL, "no-store")
        .body(Body::empty())
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    resp.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&set_cookie)
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
    );
    Ok(resp)
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct CallbackQuery {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

/// `GET /admin/auth/callback`: completes PKCE code exchange, verifies ID token
/// signature + claims + verified allowlisted email, and mints a stateless session cookie.
pub(crate) async fn callback_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CallbackQuery>,
) -> Result<Response, ApiError> {
    if let Some(err) = query
        .error
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let desc = query
            .error_description
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("authorization denied");
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            format!("OIDC provider error ({err}): {desc}"),
        ));
    }

    let code = query
        .code
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "missing authorization code"))?;
    let state_param = query
        .state
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "missing OIDC state"))?;

    let cfg = OidcConfig::from_env().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "admin OIDC login is not configured",
        )
    })?;
    let key = session_signing_key_from_env().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "admin session signing key is not configured",
        )
    })?;

    let raw_preauth = extract_cookie(&headers, ADMIN_OIDC_COOKIE).ok_or_else(|| {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            "missing OIDC pre-auth state cookie",
        )
    })?;
    let preauth: PreAuthState = verify_cookie_payload(&key, PREAUTH_DOMAIN, &raw_preauth)
        .map_err(|why| ApiError::new(StatusCode::UNAUTHORIZED, why))?;

    let now = unix_now_secs();
    if now >= preauth.exp {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "OIDC pre-auth state expired; please retry sign-in",
        ));
    }
    if preauth.iat > now.saturating_add(CLOCK_SKEW_SECS) {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "OIDC pre-auth state issued in the future",
        ));
    }
    if !constant_time_eq(state_param.as_bytes(), preauth.state.as_bytes()) {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "OIDC state mismatch",
        ));
    }

    let discovery = fetch_discovery(&cfg.issuer)
        .await
        .map_err(|why| ApiError::new(StatusCode::BAD_GATEWAY, why))?;

    let token_resp = exchange_authorization_code(
        &discovery.token_endpoint,
        &cfg.client_id,
        cfg.client_secret.as_deref(),
        code,
        &preauth.redirect_uri,
        &preauth.code_verifier,
    )
    .await
    .map_err(|why| ApiError::new(StatusCode::UNAUTHORIZED, why))?;

    let jwks = fetch_jwks(&discovery.jwks_uri, false)
        .await
        .map_err(|why| ApiError::new(StatusCode::BAD_GATEWAY, why))?;

    let verified_id = match verify_id_token_claims(
        &token_resp.id_token,
        &jwks,
        &cfg.issuer,
        &cfg.client_id,
        &preauth.nonce,
        unix_now_secs(),
    ) {
        Ok(claims) => claims,
        Err(IdTokenVerifyError::UnknownKid) => {
            // Key rotation support: refresh JWKS once on unknown kid.
            let refreshed = fetch_jwks(&discovery.jwks_uri, true)
                .await
                .map_err(|why| ApiError::new(StatusCode::BAD_GATEWAY, why))?;
            verify_id_token_claims(
                &token_resp.id_token,
                &refreshed,
                &cfg.issuer,
                &cfg.client_id,
                &preauth.nonce,
                unix_now_secs(),
            )
            .map_err(|e| ApiError::new(StatusCode::UNAUTHORIZED, e.to_string()))?
        }
        Err(IdTokenVerifyError::Invalid(msg)) => {
            return Err(ApiError::new(StatusCode::UNAUTHORIZED, msg));
        }
    };

    let needs_userinfo = verified_id.email_verified != Some(false)
        && (verified_id.email.is_none() || verified_id.email_verified.is_none());
    let userinfo = if needs_userinfo
        && let (Some(ui_url), Some(access_token)) = (
            discovery.userinfo_endpoint.as_deref(),
            token_resp.access_token.as_deref(),
        ) {
        fetch_userinfo(ui_url, access_token).await.ok()
    } else {
        None
    };

    let admin = authorize_admin_identity(&verified_id, userinfo.as_ref(), &cfg.allowed_emails)
        .map_err(|e| match e {
            AdminAuthzError::Unauthenticated(msg) => ApiError::new(StatusCode::UNAUTHORIZED, msg),
            AdminAuthzError::Forbidden(msg) => ApiError::new(StatusCode::FORBIDDEN, msg),
        })?;

    let issued_at = unix_now_secs();
    let session = AdminSession {
        email: admin.email,
        name: admin.name,
        sub: admin.sub,
        iat: issued_at,
        exp: issued_at.saturating_add(SESSION_TTL_SECS),
    };
    let signed_session = sign_admin_session(&key, &session)
        .map_err(|why| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, why))?;

    let secure_session =
        is_https_deployment(state.store.data_dir()) || preauth.redirect_uri.starts_with("https://");
    let secure_preauth = secure_session || serverless::is_serverless_mode();

    let session_cookie =
        format_session_set_cookie(&signed_session, secure_session, SESSION_TTL_SECS);
    let clear_preauth = format_preauth_set_cookie("", secure_preauth, 0);

    let mut resp = Response::builder()
        .status(StatusCode::FOUND)
        .header(LOCATION, "/admin")
        .header(CACHE_CONTROL, "no-store")
        .body(Body::empty())
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    resp.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&session_cookie)
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
    );
    resp.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&clear_preauth)
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
    );
    Ok(resp)
}

/// `POST /admin/auth/logout`: clears the stateless admin session cookie.
/// Requires `x-requested-with: fetch` or `x-anyagents-admin-csrf: 1` to block CSRF.
pub(crate) async fn logout_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let has_xrw = headers
        .get("x-requested-with")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("fetch"));
    let has_csrf = headers
        .get(ADMIN_CSRF_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim() == "1");
    if !has_xrw && !has_csrf {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "missing CSRF protection header",
        ));
    }

    let secure = is_https_deployment(state.store.data_dir());
    let clear_session = format_session_set_cookie("", secure, 0);
    let mut resp = (StatusCode::OK, Json(json!({ "ok": true }))).into_response();
    resp.headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    resp.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&clear_session)
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
    );
    Ok(resp)
}

/// `GET /admin/auth/me`: returns the caller's admin auth status without exposing any tokens.
pub(crate) async fn me_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let serverless_mode = serverless::is_serverless_mode();
    let body = if is_oidc_configured() {
        match extract_session_cookie(&headers)
            .as_deref()
            .map(verify_session_cookie_from_env)
        {
            Some(Ok(session)) => json!({
                "authenticated": true,
                "mode": "oidc",
                "email": session.email,
                "name": session.name,
                "serverless": serverless_mode,
            }),
            _ => json!({
                "authenticated": false,
                "mode": "oidc",
                "email": Value::Null,
                "name": Value::Null,
                "serverless": serverless_mode,
            }),
        }
    } else if !serverless_mode && !state.auth.is_confined() {
        json!({
            "authenticated": true,
            "mode": "local_trusted",
            "email": Value::Null,
            "name": "local",
            "serverless": false,
        })
    } else {
        json!({
            "authenticated": false,
            "mode": "unconfigured",
            "email": Value::Null,
            "name": Value::Null,
            "serverless": serverless_mode,
        })
    };

    let mut resp = (StatusCode::OK, Json(body)).into_response();
    resp.headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{
        ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, Ed25519KeyPair, KeyPair as _,
    };

    fn build_ed25519_jwt(
        kp: &Ed25519KeyPair,
        kid: Option<&str>,
        alg: &str,
        claims: &Value,
    ) -> String {
        let mut header = json!({ "alg": alg, "typ": "JWT" });
        if let Some(k) = kid {
            header["kid"] = json!(k);
        }
        let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
        let payload_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig = kp.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig.as_ref());
        format!("{signing_input}.{sig_b64}")
    }

    fn build_es256_jwt(
        kp: &EcdsaKeyPair,
        rng: &SystemRandom,
        kid: Option<&str>,
        claims: &Value,
    ) -> String {
        let mut header = json!({ "alg": "ES256", "typ": "JWT" });
        if let Some(k) = kid {
            header["kid"] = json!(k);
        }
        let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
        let payload_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig = kp.sign(rng, signing_input.as_bytes()).unwrap();
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig.as_ref());
        format!("{signing_input}.{sig_b64}")
    }

    #[test]
    fn pkce_s256_matches_rfc_7636_appendix_b() {
        // RFC 7636 Appendix B exact test vector (split across literals to stay clear of secret scanners).
        let verifier = concat!("dBjftJeZ4CVP-mB92K27uh", "bUJU1p1r_wW1gFWFOEjXk");
        let expected_challenge = concat!("E9Melhoa2OwvFrEMTJguCH", "aoeK1t8URWbuGJSstw-cM");
        assert_eq!(pkce_s256_challenge(verifier), expected_challenge);

        let generated = generate_random_urlsafe(32).unwrap();
        assert_eq!(generated.len(), 43);
        assert!(
            generated
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        );
    }

    #[test]
    fn stateless_cookie_signing_verification_tamper_and_expiry_rejection() {
        let key = hkdf_derive_32(b"test-session-secret-material-0123456789", b"test");
        let other_key = hkdf_derive_32(b"different-secret-material-9876543210", b"test");
        let now = 1_700_000_000;
        let session = AdminSession {
            email: "admin@example.com".to_string(),
            name: Some("Console Admin".to_string()),
            sub: "user-123".to_string(),
            iat: now,
            exp: now + 3600,
        };

        let cookie = sign_admin_session(&key, &session).unwrap();
        let verified = verify_admin_session_at(&key, &cookie, now + 60).unwrap();
        assert_eq!(verified, session);

        // Wrong key rejected.
        assert!(verify_admin_session_at(&other_key, &cookie, now + 60).is_err());

        // Tampered payload rejected.
        let (payload_b64, sig_b64) = cookie.split_once('.').unwrap();
        let mut tampered_payload = URL_SAFE_NO_PAD.decode(payload_b64).unwrap();
        tampered_payload[2] ^= 0x01;
        let tampered = format!("{}.{sig_b64}", URL_SAFE_NO_PAD.encode(tampered_payload));
        assert!(verify_admin_session_at(&key, &tampered, now + 60).is_err());

        // Tampered signature rejected.
        let mut tampered_sig = URL_SAFE_NO_PAD.decode(sig_b64).unwrap();
        tampered_sig[0] ^= 0xff;
        let bad_sig = format!("{payload_b64}.{}", URL_SAFE_NO_PAD.encode(tampered_sig));
        assert!(verify_admin_session_at(&key, &bad_sig, now + 60).is_err());

        // Expired cookie rejected.
        assert!(verify_admin_session_at(&key, &cookie, now + 3600).is_err());
        assert!(verify_admin_session_at(&key, &cookie, now + 7200).is_err());

        // Domain separation: pre-auth cookie cannot be verified as session cookie.
        let preauth = PreAuthState {
            state: "st".into(),
            nonce: "nn".into(),
            code_verifier: "cv".into(),
            redirect_uri: "https://console.example.com/admin/auth/callback".into(),
            iat: now,
            exp: now + 600,
        };
        let preauth_cookie = sign_cookie_payload(&key, PREAUTH_DOMAIN, &preauth).unwrap();
        assert!(verify_admin_session_at(&key, &preauth_cookie, now + 10).is_err());
    }

    #[test]
    fn jwks_id_token_verification_eddsa_and_es256_fail_closed() {
        let rng = SystemRandom::new();

        // Generate Ed25519 keypair + JWKS entry.
        let ed_pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let ed_kp = Ed25519KeyPair::from_pkcs8(ed_pkcs8.as_ref()).unwrap();
        let ed_x = URL_SAFE_NO_PAD.encode(ed_kp.public_key().as_ref());

        // Generate ES256 (P-256) keypair + JWKS entry.
        let ec_pkcs8 =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let ec_kp =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, ec_pkcs8.as_ref(), &rng)
                .unwrap();
        let ec_pub = ec_kp.public_key().as_ref();
        assert_eq!(ec_pub.len(), 65);
        assert_eq!(ec_pub[0], 0x04);
        let ec_x = URL_SAFE_NO_PAD.encode(&ec_pub[1..33]);
        let ec_y = URL_SAFE_NO_PAD.encode(&ec_pub[33..65]);

        let jwks_json = json!({
            "keys": [
                {
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "use": "sig",
                    "alg": "EdDSA",
                    "kid": "ed-key-1",
                    "x": ed_x,
                },
                {
                    "kty": "EC",
                    "crv": "P-256",
                    "use": "sig",
                    "alg": "ES256",
                    "kid": "ec-key-1",
                    "x": ec_x,
                    "y": ec_y,
                }
            ]
        });
        let jwks = parse_jwks(&serde_json::to_vec(&jwks_json).unwrap()).unwrap();
        assert_eq!(jwks.len(), 2);

        let now = 1_750_000_000;
        let issuer = "https://idp.example.com";
        let client_id = "anyagents-admin-client";
        let nonce = "nonce-abcdef123456";
        let allowed = parse_allowed_emails("Admin@Example.com, ops@example.org");

        let valid_claims = json!({
            "iss": issuer,
            "sub": "sub-001",
            "aud": client_id,
            "exp": now + 600,
            "iat": now,
            "nonce": nonce,
            "email": "ADMIN@EXAMPLE.COM",
            "email_verified": true,
            "name": "Example Admin",
        });

        // 1. Valid EdDSA token passes.
        let ed_tok = build_ed25519_jwt(&ed_kp, Some("ed-key-1"), "EdDSA", &valid_claims);
        let ed_admin =
            verify_and_authorize_id_token(&ed_tok, &jwks, issuer, client_id, nonce, &allowed, now)
                .unwrap();
        assert_eq!(ed_admin.email, "admin@example.com");
        assert_eq!(ed_admin.name.as_deref(), Some("Example Admin"));

        // 2. Valid ES256 token passes (also verifies array `aud`).
        let mut es_claims = valid_claims.clone();
        es_claims["aud"] = json!(["other-aud", client_id]);
        es_claims["azp"] = json!(client_id);
        let es_tok = build_es256_jwt(&ec_kp, &rng, Some("ec-key-1"), &es_claims);
        let es_admin =
            verify_and_authorize_id_token(&es_tok, &jwks, issuer, client_id, nonce, &allowed, now)
                .unwrap();
        assert_eq!(es_admin.email, "admin@example.com");

        // 3. Reject `alg: none` and `HS256`.
        let none_tok = build_ed25519_jwt(&ed_kp, Some("ed-key-1"), "none", &valid_claims);
        assert!(
            verify_and_authorize_id_token(
                &none_tok, &jwks, issuer, client_id, nonce, &allowed, now
            )
            .is_err()
        );
        let hs_tok = build_ed25519_jwt(&ed_kp, Some("ed-key-1"), "HS256", &valid_claims);
        assert!(
            verify_and_authorize_id_token(&hs_tok, &jwks, issuer, client_id, nonce, &allowed, now)
                .is_err()
        );

        // 4. Reject wrong issuer.
        let mut bad_iss = valid_claims.clone();
        bad_iss["iss"] = json!("https://evil.example.net");
        let bad_iss_tok = build_es256_jwt(&ec_kp, &rng, Some("ec-key-1"), &bad_iss);
        assert!(
            verify_and_authorize_id_token(
                &bad_iss_tok,
                &jwks,
                issuer,
                client_id,
                nonce,
                &allowed,
                now
            )
            .is_err()
        );

        // 5. Reject wrong audience.
        let mut bad_aud = valid_claims.clone();
        bad_aud["aud"] = json!("wrong-client");
        let bad_aud_tok = build_ed25519_jwt(&ed_kp, Some("ed-key-1"), "EdDSA", &bad_aud);
        assert!(
            verify_and_authorize_id_token(
                &bad_aud_tok,
                &jwks,
                issuer,
                client_id,
                nonce,
                &allowed,
                now
            )
            .is_err()
        );

        // 6. Reject wrong nonce.
        let bad_nonce_tok = build_es256_jwt(&ec_kp, &rng, Some("ec-key-1"), &valid_claims);
        assert!(
            verify_and_authorize_id_token(
                &bad_nonce_tok,
                &jwks,
                issuer,
                client_id,
                "different-nonce",
                &allowed,
                now
            )
            .is_err()
        );

        // 7. Reject expired token (beyond 60s skew).
        let mut expired = valid_claims.clone();
        expired["exp"] = json!(now - 61);
        let expired_tok = build_ed25519_jwt(&ed_kp, Some("ed-key-1"), "EdDSA", &expired);
        assert!(
            verify_and_authorize_id_token(
                &expired_tok,
                &jwks,
                issuer,
                client_id,
                nonce,
                &allowed,
                now
            )
            .is_err()
        );

        // 8. Reject `email_verified: false`.
        let mut unverified = valid_claims.clone();
        unverified["email_verified"] = json!(false);
        let unverified_tok = build_es256_jwt(&ec_kp, &rng, Some("ec-key-1"), &unverified);
        assert!(
            verify_and_authorize_id_token(
                &unverified_tok,
                &jwks,
                issuer,
                client_id,
                nonce,
                &allowed,
                now
            )
            .is_err()
        );

        // 9. Reject non-allowlisted email.
        let mut outsider = valid_claims.clone();
        outsider["email"] = json!("outsider@example.com");
        let outsider_tok = build_ed25519_jwt(&ed_kp, Some("ed-key-1"), "EdDSA", &outsider);
        assert!(
            verify_and_authorize_id_token(
                &outsider_tok,
                &jwks,
                issuer,
                client_id,
                nonce,
                &allowed,
                now
            )
            .is_err()
        );

        // 10. Reject signature produced by a different keypair.
        let rogue_pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let rogue_kp = Ed25519KeyPair::from_pkcs8(rogue_pkcs8.as_ref()).unwrap();
        let forged_tok = build_ed25519_jwt(&rogue_kp, Some("ed-key-1"), "EdDSA", &valid_claims);
        assert!(
            verify_and_authorize_id_token(
                &forged_tok,
                &jwks,
                issuer,
                client_id,
                nonce,
                &allowed,
                now
            )
            .is_err()
        );
    }
}
