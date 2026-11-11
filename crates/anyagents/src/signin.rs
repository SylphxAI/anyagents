//! `harnesses sign-in` and `sign-in`. The installer's vendor sign-in is run for
//! the config directory of a recorded agent, so the credential lands in the same
//! store the agent's harness is started with.

use std::io::{BufRead, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use anyagents_core::{Error, Store, agent_config_dir, agent_work_dir};
use anyagents_installer::{
    CommandRunner, InstallError, InstallMethod, LineSink, SignInOutcome, SignInPrompt, SignInSink,
    builtin_catalog, config_dir_variable,
};
use sha2::{Digest, Sha256};

/// Interactive timeout for Google OAuth2 PKCE sign-in (15 minutes).
pub const AGY_SIGNIN_TIMEOUT: Duration = Duration::from_secs(15 * 60);

pub const AGY_OAUTH_AUTHORIZE_URL: &str = "https://accounts.google.com/o/oauth2/auth";
pub const AGY_OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
pub const AGY_OAUTH_USERINFO_URL: &str = "https://www.googleapis.com/oauth2/v2/userinfo";
pub const AGY_OAUTH_CLIENT_ID: &str =
    "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com";
pub const AGY_OAUTH_REDIRECT_URI: &str = "https://antigravity.google/oauth-callback";
pub const AGY_OAUTH_SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/cclog",
    "https://www.googleapis.com/auth/experimentsandconfigs",
    "https://www.googleapis.com/auth/aicode",
    "openid",
];

const AGY_ONBOARDING_PBTXT: &str = "post_onboarding: {\n  completed_steps: POST_ONBOARDING_STEP_TYPE_MANAGER_WELCOME\n  completed_steps: POST_ONBOARDING_STEP_TYPE_USAGE_MODE\n  completed_steps: POST_ONBOARDING_STEP_TYPE_AGENT_CONFIGURATION\n  completed_steps: POST_ONBOARDING_STEP_TYPE_ADD_WORKSPACE\n}\nagent_onboarding_completed: AGENT_ONBOARDING_STATE_COMPLETED\nmigrations: { key: 3 value: MIGRATION_STATUS_COMPLETED }\nmigrations: { key: 4 value: MIGRATION_STATUS_COMPLETED }\nmigrations: { key: 5 value: MIGRATION_STATUS_COMPLETED }\n";

/// Which harnesses this client can sign in.
pub fn signable(harness: &str) -> bool {
    let harness = anyagents_harness::normalize_harness_name(harness);
    builtin_catalog().iter().any(|entry| entry.id == harness)
}

/// The variable that points `harness` at its own config directory.
pub fn config_variable(harness: &str) -> Option<&'static str> {
    config_dir_variable(harness)
}

/// How the vendor tool is installed. Printed so the client knows what is missing
/// when the program is not on the machine.
pub fn install_hint(harness: &str) -> Option<String> {
    let harness = anyagents_harness::normalize_harness_name(harness);
    builtin_catalog()
        .into_iter()
        .find(|entry| entry.id == harness)
        .map(|entry| match entry.install {
            InstallMethod::Npm { package } => format!("npm install -g {package}"),
            InstallMethod::VendorScript { url } => format!("curl -fsSL {url} | sh"),
            InstallMethod::ReleaseBinary { url, .. } => url,
            // A tab separates the printed columns; keep the hint one word.
            InstallMethod::Vendored => "machine-copy".to_string(),
        })
}

/// Run the vendor sign-in for the agent's own config directory.
pub fn sign_in_agent(
    data: &Path,
    agent_name: &str,
    runner: &mut dyn CommandRunner,
    sink: &mut dyn SignInSink,
) -> Result<SignInOutcome, Error> {
    let store = Store::open(data.join("state.db"))?;
    let agent = store.agent_by_name(agent_name)?;
    // Both the child cwd and vendor HOME/config variables must name the
    // same directory even when the CLI received `--data .`.
    let root = std::path::absolute(store.data_dir())?;
    if !signable(&agent.harness) {
        return Err(Error::NotImplemented(format!(
            "sign-in for harness {}",
            agent.harness
        )));
    }
    let dir = agent_config_dir(&root, &agent.id);
    std::fs::create_dir_all(&dir)?;
    // The vendor command needs a working directory; the supervisor would create
    // it before the first launch, and an agent that was never launched has none.
    std::fs::create_dir_all(agent_work_dir(&root, &agent.id))?;
    anyagents_installer::sign_in_at(&builtin_catalog(), &dir, &agent.harness, runner, sink).map_err(
        |err| match err {
            InstallError::UnknownHarness(id) => {
                Error::NotImplemented(format!("sign-in for harness {id}"))
            }
            other => Error::Message(other.to_string()),
        },
    )
}

fn base64url_nopad(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 0x3f) as usize] as char);
        }
    }
    out
}

fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    let trimmed = input.trim().trim_end_matches('=');
    let mut buf = Vec::with_capacity(trimmed.len() * 3 / 4 + 3);
    let mut acc = 0u32;
    let mut bits = 0u8;
    for ch in trimmed.bytes() {
        let val = match ch {
            b'A'..=b'Z' => ch - b'A',
            b'a'..=b'z' => ch - b'a' + 26,
            b'0'..=b'9' => ch - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            _ => return None,
        };
        acc = (acc << 6) | (val as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            buf.push(((acc >> bits) & 0xff) as u8);
        }
    }
    Some(buf)
}

fn fill_os_random(dest: &mut [u8]) -> Result<(), Error> {
    if let Ok(mut urandom) = std::fs::File::open("/dev/urandom")
        && urandom.read_exact(dest).is_ok()
    {
        return Ok(());
    }
    Err(Error::Message(
        "failed to read cryptographic randomness from /dev/urandom".into(),
    ))
}

fn url_encode_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 16);
    for b in raw.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'+' => {
                out.push(b' ');
                idx += 1;
            }
            b'%' if idx + 2 < bytes.len() => {
                let hi = (bytes[idx + 1] as char).to_digit(16);
                let lo = (bytes[idx + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push(((h << 4) | l) as u8);
                    idx += 3;
                } else {
                    out.push(bytes[idx]);
                    idx += 1;
                }
            }
            other => {
                out.push(other);
                idx += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Compute RFC 7636 S256 PKCE `code_challenge` for `code_verifier`.
pub fn pkce_s256_challenge(code_verifier: &str) -> String {
    let digest = Sha256::digest(code_verifier.as_bytes());
    base64url_nopad(&digest)
}

/// Build the Google OAuth2 authorization URL for `google-antigravity` (`agy`).
pub fn build_google_antigravity_authorize_url(
    authorize_endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    code_challenge: &str,
) -> String {
    let scopes = AGY_OAUTH_SCOPES
        .iter()
        .map(|s| url_encode_component(s))
        .collect::<Vec<_>>()
        .join("+");
    format!(
        "{authorize_endpoint}?access_type=offline&client_id={}&code_challenge={}&code_challenge_method=S256&prompt=consent&redirect_uri={}&response_type=code&scope={scopes}&state={}",
        url_encode_component(client_id),
        url_encode_component(code_challenge),
        url_encode_component(redirect_uri),
        url_encode_component(state),
    )
}

/// Parse either a bare Google OAuth authorization code or a full/partial callback URL
/// (`https://antigravity.google/oauth-callback?state=...&code=...`), validating `state`
/// fail-closed whenever present.
pub fn parse_google_oauth_input(raw: &str, expected_state: &str) -> Result<String, Error> {
    let trimmed = raw
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == '`');
    if trimmed.is_empty() {
        return Err(Error::Message(
            "authorization code or callback URL is empty".into(),
        ));
    }
    let without_fragment = trimmed.split('#').next().unwrap_or(trimmed).trim();
    let query_opt = if let Some((_, q)) = without_fragment.split_once('?') {
        Some(q)
    } else if without_fragment.contains("code=") || without_fragment.contains("error=") {
        Some(without_fragment)
    } else {
        None
    };

    if let Some(query) = query_opt {
        let mut found_code: Option<String> = None;
        let mut found_state: Option<String> = None;
        let mut found_error: Option<String> = None;
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let key = percent_decode(k);
            let val = percent_decode(v);
            match key.as_str() {
                "code" if !val.trim().is_empty() => found_code = Some(val.trim().to_string()),
                "state" if !val.trim().is_empty() => found_state = Some(val.trim().to_string()),
                "error" if !val.trim().is_empty() => found_error = Some(val.trim().to_string()),
                _ => {}
            }
        }
        if let Some(err) = found_error {
            return Err(Error::Message(format!(
                "Google OAuth returned error `{err}`"
            )));
        }
        if let Some(st) = found_state
            && !expected_state.is_empty()
            && st != expected_state
        {
            return Err(Error::Message(
                "Google OAuth callback state mismatch (paste the URL from the latest sign-in prompt)"
                    .into(),
            ));
        }
        if let Some(code) = found_code {
            return Ok(code);
        }
        return Err(Error::Message(
            "callback URL does not contain a non-empty `code` query parameter".into(),
        ));
    }

    if without_fragment.starts_with("http://") || without_fragment.starts_with("https://") {
        return Err(Error::Message(
            "callback URL is missing `?code=...` query parameters".into(),
        ));
    }

    let decoded = if without_fragment.contains('%') {
        percent_decode(without_fragment)
    } else {
        without_fragment.to_string()
    };
    let clean = decoded.trim();
    if clean.is_empty() || clean.chars().any(char::is_whitespace) {
        return Err(Error::Message("invalid authorization code format".into()));
    }
    Ok(clean.to_string())
}

fn decode_jwt_identity_fallback(id_token: &str) -> (Option<String>, Option<String>) {
    let mut parts = id_token.split('.');
    let _header = parts.next();
    let Some(payload_b64) = parts.next() else {
        return (None, None);
    };
    let Some(bytes) = base64url_decode(payload_b64) else {
        return (None, None);
    };
    let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return (None, None);
    };
    let email = val
        .get("email")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    let sub = val
        .get("sub")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    (email, sub)
}

#[derive(Clone, Debug)]
pub struct GooglePkceEndpoints {
    pub authorize_url: String,
    pub token_url: String,
    pub userinfo_url: String,
    pub redirect_uri: String,
    pub timeout: Duration,
}

impl Default for GooglePkceEndpoints {
    fn default() -> Self {
        Self {
            authorize_url: AGY_OAUTH_AUTHORIZE_URL.to_string(),
            token_url: AGY_OAUTH_TOKEN_URL.to_string(),
            userinfo_url: AGY_OAUTH_USERINFO_URL.to_string(),
            redirect_uri: AGY_OAUTH_REDIRECT_URI.to_string(),
            timeout: AGY_SIGNIN_TIMEOUT,
        }
    }
}

/// Native Rust Google OAuth2 PKCE interactive sign-in for `google-antigravity` (`agy`).
pub fn google_antigravity_pkce_sign_in(
    target_home_dir: &Path,
    sink: &mut dyn SignInSink,
) -> Result<SignInOutcome, Error> {
    let (tx, rx) = mpsc::channel::<std::io::Result<String>>();
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = tx.send(Ok(String::new()));
                    break;
                }
                Ok(_) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let _ = tx.send(Ok(line));
                    break;
                }
                Err(err) => {
                    let _ = tx.send(Err(err));
                    break;
                }
            }
        }
    });

    let endpoints = GooglePkceEndpoints::default();
    let mut verifier_bytes = [0u8; 32];
    fill_os_random(&mut verifier_bytes)?;
    let mut state_bytes = [0u8; 16];
    fill_os_random(&mut state_bytes)?;
    let code_verifier = base64url_nopad(&verifier_bytes);
    let code_challenge = pkce_s256_challenge(&code_verifier);
    let state = base64url_nopad(&state_bytes);
    let auth_url = build_google_antigravity_authorize_url(
        &endpoints.authorize_url,
        AGY_OAUTH_CLIENT_ID,
        &endpoints.redirect_uri,
        &state,
        &code_challenge,
    );
    let prompts = vec![
        SignInPrompt::Link(auth_url),
        SignInPrompt::Prompt(
            "Paste the authorization code or full callback URL (waiting up to 15m):".to_string(),
        ),
    ];
    for p in &prompts {
        sink.prompt(p);
    }
    let _ = std::io::stdout().flush();

    let (ctx, crx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = ctx.send(crate::create::extract_agy_oauth_clients());
    });

    let raw_input = match rx.recv_timeout(endpoints.timeout) {
        Ok(Ok(line)) if !line.trim().is_empty() => line,
        Ok(Ok(_)) => {
            return Err(Error::Message(
                "sign-in aborted: standard input closed before receiving an authorization code"
                    .into(),
            ));
        }
        Ok(Err(err)) => {
            return Err(Error::Message(format!(
                "sign-in failed reading authorization code: {err}"
            )));
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            return Err(Error::Message(format!(
                "sign-in timed out after {}s waiting for authorization code",
                endpoints.timeout.as_secs()
            )));
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            return Err(Error::Message(
                "sign-in aborted: input channel closed before receiving an authorization code"
                    .into(),
            ));
        }
    };

    let code = parse_google_oauth_input(&raw_input, &state)?;
    let mut clients = crx
        .recv_timeout(Duration::from_secs(30))
        .unwrap_or_default();
    if let Ok(env_secret) = std::env::var("ANYAGENTS_AGY_CLIENT_SECRET")
        && !env_secret.trim().is_empty()
    {
        clients.insert(
            0,
            (
                AGY_OAUTH_CLIENT_ID.to_string(),
                env_secret.trim().to_string(),
            ),
        );
    }
    let mut outcome = exchange_and_persist_google_pkce(
        target_home_dir,
        &endpoints,
        &clients,
        &code,
        &code_verifier,
    )?;
    outcome.prompts = prompts;
    sink.done(&outcome);
    Ok(outcome)
}

/// Testable Google OAuth2 PKCE flow with injected input channel, endpoints, and client list.
#[cfg(test)]
pub fn google_antigravity_pkce_sign_in_with(
    target_home_dir: &Path,
    endpoints: &GooglePkceEndpoints,
    clients: &[(String, String)],
    input_rx: mpsc::Receiver<String>,
    sink: &mut dyn SignInSink,
) -> Result<SignInOutcome, Error> {
    let mut verifier_bytes = [0u8; 32];
    fill_os_random(&mut verifier_bytes)?;
    let mut state_bytes = [0u8; 16];
    fill_os_random(&mut state_bytes)?;
    let code_verifier = base64url_nopad(&verifier_bytes);
    let code_challenge = pkce_s256_challenge(&code_verifier);
    let state = base64url_nopad(&state_bytes);
    let client_id = clients
        .first()
        .map(|(cid, _)| cid.as_str())
        .unwrap_or(AGY_OAUTH_CLIENT_ID);
    let auth_url = build_google_antigravity_authorize_url(
        &endpoints.authorize_url,
        client_id,
        &endpoints.redirect_uri,
        &state,
        &code_challenge,
    );
    let prompts = vec![
        SignInPrompt::Link(auth_url),
        SignInPrompt::Prompt(
            "Paste the authorization code or full callback URL (waiting up to 15m):".to_string(),
        ),
    ];
    for p in &prompts {
        sink.prompt(p);
    }

    let deadline = std::time::Instant::now() + endpoints.timeout;
    let raw_input = loop {
        let now = std::time::Instant::now();
        if now >= deadline {
            return Err(Error::Message(format!(
                "sign-in timed out after {}s waiting for authorization code",
                endpoints.timeout.as_secs()
            )));
        }
        match input_rx.recv_timeout(deadline - now) {
            Ok(line) if line.trim().is_empty() => continue,
            Ok(line) => break line,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(Error::Message(format!(
                    "sign-in timed out after {}s waiting for authorization code",
                    endpoints.timeout.as_secs()
                )));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(Error::Message(
                    "sign-in aborted: standard input closed before receiving an authorization code"
                        .into(),
                ));
            }
        }
    };

    let code = parse_google_oauth_input(&raw_input, &state)?;
    let mut outcome = exchange_and_persist_google_pkce(
        target_home_dir,
        endpoints,
        clients,
        &code,
        &code_verifier,
    )?;
    outcome.prompts = prompts;
    sink.done(&outcome);
    Ok(outcome)
}

fn exchange_and_persist_google_pkce(
    target_home_dir: &Path,
    endpoints: &GooglePkceEndpoints,
    clients: &[(String, String)],
    code: &str,
    code_verifier: &str,
) -> Result<SignInOutcome, Error> {
    if clients.is_empty() {
        return Err(Error::Message(
            "agy OAuth client credentials unavailable: ensure `agy` is installed on PATH or ~/.local/bin/agy"
                .into(),
        ));
    }
    let http = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(30)))
        .build()
        .new_agent();

    let mut token_json: Option<serde_json::Value> = None;
    let mut last_err = String::from("no OAuth client candidate succeeded");
    for (cid, csec) in clients {
        let form_body = format!(
            "client_id={}&client_secret={}&code={}&code_verifier={}&grant_type=authorization_code&redirect_uri={}",
            url_encode_component(cid),
            url_encode_component(csec),
            url_encode_component(code),
            url_encode_component(code_verifier),
            url_encode_component(&endpoints.redirect_uri),
        );
        let resp = http
            .post(&endpoints.token_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .send(form_body.as_bytes())
            .map_err(|err| Error::Message(format!("Google OAuth token exchange failed: {err}")))?;
        let status = resp.status().as_u16();
        let body_str = resp
            .into_body()
            .read_to_string()
            .map_err(|err| Error::Message(format!("failed reading token response: {err}")))?;
        let parsed: serde_json::Value = serde_json::from_str(&body_str).unwrap_or_default();
        if (200..300).contains(&status) {
            token_json = Some(parsed);
            break;
        }
        let err_code = parsed
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("http_error");
        let err_desc = parsed
            .get("error_description")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status == 401 || err_code == "invalid_client" || err_code == "unauthorized_client" {
            last_err = format!("OAuth client rejected ({err_code})");
            continue;
        }
        return Err(Error::Message(if err_desc.is_empty() {
            format!("Google OAuth token exchange failed ({status}): {err_code}")
        } else {
            format!("Google OAuth token exchange failed ({status}): {err_code} ({err_desc})")
        }));
    }

    let token_val = token_json.ok_or_else(|| Error::Message(last_err))?;
    let access_token = token_val
        .get("access_token")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Message("Google OAuth response missing `access_token`".into()))?;
    let refresh_token = token_val
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::Message(
                "Google OAuth response missing `refresh_token` (re-approve with prompt=consent)"
                    .into(),
            )
        })?;
    let expires_in = token_val
        .get("expires_in")
        .and_then(|v| v.as_i64())
        .unwrap_or(3599)
        .max(60);
    let token_type = token_val
        .get("token_type")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("Bearer");
    let (jwt_email, jwt_sub) = token_val
        .get("id_token")
        .and_then(|v| v.as_str())
        .map(decode_jwt_identity_fallback)
        .unwrap_or((None, None));

    let mut email = jwt_email;
    let mut account_id = jwt_sub;
    if let Ok(resp) = http
        .get(&endpoints.userinfo_url)
        .header("Authorization", &format!("Bearer {access_token}"))
        .header("Accept", "application/json")
        .call()
    {
        let status = resp.status().as_u16();
        if (200..300).contains(&status)
            && let Ok(body) = resp.into_body().read_to_string()
            && let Ok(info) = serde_json::from_str::<serde_json::Value>(&body)
        {
            if let Some(e) = info
                .get("email")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                email = Some(e.to_string());
            }
            if let Some(id) = info
                .get("id")
                .or_else(|| info.get("sub"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                account_id = Some(id.to_string());
            }
        }
    }

    let email = email.ok_or_else(|| {
        Error::Message("failed to resolve Google account email from userinfo or id_token".into())
    })?;
    let account_id = account_id.unwrap_or_default();
    let expiry = (chrono::Utc::now() + chrono::Duration::seconds(expires_in))
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string();

    let agy_dir = target_home_dir.join(".gemini/antigravity-cli");
    std::fs::create_dir_all(&agy_dir)?;
    let stored = serde_json::json!({
        "auth_method": "consumer",
        "id_token_email": email,
        "email": email,
        "account_id": account_id,
        "token": {
            "access_token": access_token,
            "expires_in": expires_in,
            "expiry": expiry,
            "refresh_token": refresh_token,
            "token_type": token_type,
        }
    });
    let mut token_bytes =
        serde_json::to_vec_pretty(&stored).map_err(|err| Error::Message(err.to_string()))?;
    token_bytes.push(b'\n');
    anyagents_harness::atomic_write_nonempty_file(
        &agy_dir.join("antigravity-oauth-token"),
        &token_bytes,
        0o600,
    )
    .map_err(|err| Error::Message(format!("persist antigravity-oauth-token: {err}")))?;

    for name in ["jetski_state.pbtxt", "antigravity_state.pbtxt"] {
        let state_path = agy_dir.join(name);
        if !state_path.exists() {
            anyagents_harness::atomic_write_nonempty_file(
                &state_path,
                AGY_ONBOARDING_PBTXT.as_bytes(),
                0o600,
            )
            .map_err(|err| Error::Message(format!("persist {name}: {err}")))?;
        }
    }

    Ok(SignInOutcome {
        signed_in: true,
        account: Some(email),
        prompts: Vec::new(),
    })
}

/// Runs the vendor command for real. Each line it prints reaches the sink before
/// the command returns.
pub struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        let resolved_program: std::path::PathBuf = if std::env::var_os("ANYAGENTS_FAKE_MODE")
            .is_none()
            && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none()
        {
            let harness = match program {
                "claude" => "claude-code",
                other => other,
            };
            dir.ancestors()
                .find(|a| a.join("harnesses").is_dir())
                .map(|root| crate::install::program(root, harness))
                .filter(|p| p.is_file())
                .unwrap_or_else(|| std::path::PathBuf::from(program))
        } else {
            std::path::PathBuf::from(program)
        };
        let mut command = Command::new(&resolved_program);
        command
            .args(args)
            .current_dir(dir)
            .stdin(Stdio::inherit())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if std::env::var_os("DISPLAY").is_none()
            && std::env::var_os("WAYLAND_DISPLAY").is_none()
            && std::env::var_os("BROWSER").is_none()
        {
            command.env("BROWSER", "/bin/true");
        }
        if std::env::var_os("ANYAGENTS_FAKE_MODE").is_none()
            && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none()
        {
            command.env("HOME", dir);
        }
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command.spawn().map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                InstallError::Failed(format!("{program} is not installed"))
            } else {
                InstallError::Failed(err.to_string())
            }
        })?;
        if let Some(mut stdout) = child.stdout.take() {
            let pass_through = std::io::IsTerminal::is_terminal(&std::io::stdout())
                && std::env::var_os("ANYAGENTS_FAKE_VENDOR_MODE").is_none();
            let mut buf = [0u8; 4096];
            let mut pending = Vec::new();
            loop {
                let n = stdout
                    .read(&mut buf)
                    .map_err(|err| InstallError::Failed(err.to_string()))?;
                if n == 0 {
                    if !pending.is_empty() {
                        let line = String::from_utf8_lossy(&pending).to_string();
                        let trimmed = line.trim_end_matches(['\r', '\n']);
                        if !pass_through
                            || trimmed.starts_with("LINK ")
                            || trimmed.starts_with("DEVICE ")
                            || trimmed.starts_with("PROMPT ")
                            || trimmed.starts_with("SIGNED_IN ")
                        {
                            on_line.line(trimmed);
                        } else {
                            let mut out = std::io::stdout().lock();
                            let _ = out.write_all(&pending);
                            let _ = out.flush();
                        }
                    }
                    break;
                }
                pending.extend_from_slice(&buf[..n]);
                while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
                    let line_bytes: Vec<u8> = pending.drain(..=pos).collect();
                    let line = String::from_utf8_lossy(&line_bytes);
                    let trimmed = line.trim_end_matches(['\r', '\n']);
                    if !pass_through
                        || trimmed.starts_with("LINK ")
                        || trimmed.starts_with("DEVICE ")
                        || trimmed.starts_with("PROMPT ")
                        || trimmed.starts_with("SIGNED_IN ")
                    {
                        on_line.line(trimmed);
                    } else {
                        let mut out = std::io::stdout().lock();
                        let _ = out.write_all(&line_bytes);
                        let _ = out.flush();
                    }
                }
                if pass_through && !pending.is_empty() {
                    let partial = String::from_utf8_lossy(&pending);
                    if !partial.starts_with("LINK")
                        && !partial.starts_with("DEVICE")
                        && !partial.starts_with("PROMPT")
                        && !partial.starts_with("SIGNED_IN")
                    {
                        let mut out = std::io::stdout().lock();
                        let _ = out.write_all(&pending);
                        let _ = out.flush();
                        pending.clear();
                    }
                }
            }
        }
        let status = child
            .wait()
            .map_err(|err| InstallError::Failed(err.to_string()))?;
        if status.success() {
            Ok(())
        } else {
            Err(InstallError::Failed(format!(
                "vendor command exited {status}"
            )))
        }
    }
}

/// Prints each prompt as it arrives. The outcome is printed by the caller so it
/// is the last line, even for a sign-in that never asks for anything.
pub struct TerminalSink;

impl SignInSink for TerminalSink {
    fn prompt(&mut self, prompt: &SignInPrompt) {
        let mut out = std::io::stdout().lock();
        let _ = match prompt {
            SignInPrompt::Link(url) => writeln!(out, "open {url}"),
            SignInPrompt::DeviceCode(code) => writeln!(out, "enter code {code}"),
            SignInPrompt::Prompt(text) => writeln!(out, "{text}"),
        };
        let _ = out.flush();
    }

    fn done(&mut self, _: &SignInOutcome) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::NewAgent;
    use anyagents_installer::ScriptedRunner;
    use std::net::TcpListener;

    struct Collect(Vec<SignInPrompt>);

    impl SignInSink for Collect {
        fn prompt(&mut self, prompt: &SignInPrompt) {
            self.0.push(prompt.clone());
        }
        fn done(&mut self, _: &SignInOutcome) {}
    }

    fn agent(data: &Path, name: &str, harness: &str) -> anyagents_core::Agent {
        let store = Store::open(data.join("state.db")).unwrap();
        store.create_agent(NewAgent::new(name, harness)).unwrap()
    }

    #[test]
    fn sign_in_uses_the_agents_own_config_directory() {
        let data = tempfile::tempdir().unwrap();
        let agent = agent(data.path(), "ada", "claude-code");
        let mut runner = ScriptedRunner::default();
        runner.answer(
            "claude",
            "LINK https://auth.example/device\nSIGNED_IN ada@example\n",
        );
        let mut sink = Collect(Vec::new());
        let outcome = sign_in_agent(data.path(), "ada", &mut runner, &mut sink).unwrap();
        assert!(outcome.signed_in);
        assert_eq!(outcome.account.as_deref(), Some("ada@example"));
        assert_eq!(sink.0.len(), 1);
        let expected = agent_config_dir(data.path(), &agent.id);
        assert_eq!(runner.calls.len(), 1);
        let call = &runner.calls[0];
        assert_eq!(call.program, "claude");
        assert_eq!(call.args, vec!["auth".to_string(), "login".to_string()]);
        assert_eq!(call.dir, expected);
        assert!(
            call.env
                .iter()
                .any(|(key, value)| key == "CLAUDE_CONFIG_DIR" && Path::new(value) == expected)
        );
        assert_eq!(
            call.env
                .iter()
                .find(|(key, _)| key == "CLAUDE_CONFIG_DIR")
                .map(|(_, value)| Path::new(value)),
            Some(expected.as_path())
        );
    }

    #[test]
    fn sign_in_for_codex_points_it_at_its_own_home() {
        let data = tempfile::tempdir().unwrap();
        let agent = agent(data.path(), "bea", "codex");
        let mut runner = ScriptedRunner::default();
        runner.answer("codex", "SIGNED_IN bea@example\n");
        let mut sink = Collect(Vec::new());
        sign_in_agent(data.path(), "bea", &mut runner, &mut sink).unwrap();
        let call = &runner.calls[0];
        assert!(call.env.iter().any(|(key, value)| key == "CODEX_HOME"
            && Path::new(value) == agent_config_dir(data.path(), &agent.id)));
    }

    #[test]
    fn a_failed_vendor_command_is_an_error_and_keeps_the_prompts() {
        struct Failing;
        impl CommandRunner for Failing {
            fn run(
                &mut self,
                _: &str,
                _: &[String],
                _: &Path,
                _: &[(&str, &str)],
                on_line: &mut dyn LineSink,
            ) -> Result<(), InstallError> {
                on_line.line("LINK https://auth.example/device");
                Err(InstallError::Failed("exit 1".to_string()))
            }
        }
        let data = tempfile::tempdir().unwrap();
        agent(data.path(), "cy", "claude-code");
        let mut sink = Collect(Vec::new());
        let err = sign_in_agent(data.path(), "cy", &mut Failing, &mut sink).unwrap_err();
        assert!(err.to_string().contains("exit 1"), "{err}");
        assert_eq!(sink.0.len(), 1);
    }

    #[test]
    fn an_unknown_agent_is_not_found() {
        let data = tempfile::tempdir().unwrap();
        let mut runner = ScriptedRunner::default();
        let mut sink = Collect(Vec::new());
        let err = sign_in_agent(data.path(), "nobody", &mut runner, &mut sink).unwrap_err();
        assert!(matches!(err, Error::AgentNotFound(_)), "{err}");
        assert!(runner.calls.is_empty());
    }

    #[test]
    fn google_pkce_timeout_is_fifteen_minutes_and_s256_matches_rfc7636() {
        assert_eq!(AGY_SIGNIN_TIMEOUT, Duration::from_secs(900));
        assert_eq!(
            pkce_s256_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn parse_google_oauth_input_accepts_bare_code_and_callback_urls_and_rejects_state_mismatch() {
        assert_eq!(
            parse_google_oauth_input("4/0AeanS0bbareCode_123", "st-1").unwrap(),
            "4/0AeanS0bbareCode_123"
        );
        assert_eq!(
            parse_google_oauth_input("4%2F0AeanS0bEncoded_456", "st-1").unwrap(),
            "4/0AeanS0bEncoded_456"
        );
        let full_url = "https://antigravity.google/oauth-callback?state=st-1&code=4%2F0AfJohXn_abc123&scope=email+profile#";
        assert_eq!(
            parse_google_oauth_input(full_url, "st-1").unwrap(),
            "4/0AfJohXn_abc123"
        );
        let partial_url = "oauth-callback?code=4%2F0ApartiaL&state=st-1";
        assert_eq!(
            parse_google_oauth_input(partial_url, "st-1").unwrap(),
            "4/0ApartiaL"
        );
        let mismatch = parse_google_oauth_input(
            "https://antigravity.google/oauth-callback?state=wrong&code=4/0Axyz",
            "st-1",
        )
        .unwrap_err();
        assert!(
            mismatch.to_string().contains("state mismatch"),
            "{mismatch}"
        );
        let denied = parse_google_oauth_input(
            "https://antigravity.google/oauth-callback?error=access_denied&state=st-1",
            "st-1",
        )
        .unwrap_err();
        assert!(denied.to_string().contains("access_denied"), "{denied}");
    }

    #[test]
    fn google_pkce_flow_skips_invalid_client_candidates_persists_token_and_commits_profile() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for step in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap();
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let (status, body) = match step {
                    0 => {
                        assert!(req.contains("client_secret=wrong-secret"));
                        (
                            "401 Unauthorized",
                            r#"{"error":"invalid_client","error_description":"Unauthorized"}"#,
                        )
                    }
                    1 => {
                        assert!(req.contains("client_secret=valid-secret"));
                        assert!(req.contains("code_verifier="));
                        assert!(req.contains("code=4%2F0AvalidCode_789"));
                        (
                            "200 OK",
                            r#"{"access_token":"ya29.mock_access","refresh_token":"1//mock_refresh","expires_in":3599,"token_type":"Bearer"}"#,
                        )
                    }
                    _ => {
                        assert!(
                            req.to_ascii_lowercase()
                                .contains("authorization: bearer ya29.mock_access")
                        );
                        (
                            "200 OK",
                            r#"{"id":"109876543210","email":"alice.dev@example.com","verified_email":true}"#,
                        )
                    }
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(resp.as_bytes()).unwrap();
            }
        });

        let data = tempfile::tempdir().unwrap();
        let staging = data
            .path()
            .join("profiles/google-antigravity/.signin-staging-test");
        std::fs::create_dir_all(&staging).unwrap();
        let endpoints = GooglePkceEndpoints {
            authorize_url: format!("http://{addr}/auth"),
            token_url: format!("http://{addr}/token"),
            userinfo_url: format!("http://{addr}/userinfo"),
            redirect_uri: AGY_OAUTH_REDIRECT_URI.to_string(),
            timeout: Duration::from_secs(5),
        };
        let clients = vec![
            (AGY_OAUTH_CLIENT_ID.to_string(), "wrong-secret".to_string()),
            (AGY_OAUTH_CLIENT_ID.to_string(), "valid-secret".to_string()),
        ];

        let (tx, rx) = mpsc::channel();
        tx.send("\n".to_string()).unwrap();
        tx.send("https://antigravity.google/oauth-callback?code=4%2F0AvalidCode_789".to_string())
            .unwrap();

        let mut sink = Collect(Vec::new());
        let t0 = Instant::now();
        let outcome =
            google_antigravity_pkce_sign_in_with(&staging, &endpoints, &clients, rx, &mut sink)
                .unwrap();
        assert!(t0.elapsed() < Duration::from_secs(2));
        assert!(outcome.signed_in);
        assert_eq!(outcome.account.as_deref(), Some("alice.dev@example.com"));
        assert!(matches!(
            sink.0.first(),
            Some(SignInPrompt::Link(url)) if url.contains("code_challenge_method=S256")
        ));

        let (prof_name, final_dir, acct) = crate::create::resolve_and_commit_staged_signin(
            data.path(),
            "google-antigravity",
            &staging,
            None,
        )
        .unwrap();
        assert_eq!(prof_name, "alice.dev");
        assert_eq!(acct.as_deref(), Some("alice.dev@example.com"));
        assert!(!staging.exists(), "staging directory must be cleaned up");
        let saved: serde_json::Value = serde_json::from_slice(
            &std::fs::read(final_dir.join(".gemini/antigravity-cli/antigravity-oauth-token"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(saved["email"], "alice.dev@example.com");
        assert_eq!(saved["account_id"], "109876543210");
        assert_eq!(saved["token"]["access_token"], "ya29.mock_access");
        assert_eq!(saved["token"]["refresh_token"], "1//mock_refresh");
        server.join().unwrap();
    }

    #[test]
    fn google_pkce_flow_enforces_timeout_when_no_code_arrives() {
        let staging = tempfile::tempdir().unwrap();
        let endpoints = GooglePkceEndpoints {
            timeout: Duration::from_millis(60),
            ..GooglePkceEndpoints::default()
        };
        let clients = vec![(AGY_OAUTH_CLIENT_ID.to_string(), "sec".to_string())];
        let (_tx, rx) = mpsc::channel::<String>();
        let mut sink = Collect(Vec::new());
        let err = google_antigravity_pkce_sign_in_with(
            staging.path(),
            &endpoints,
            &clients,
            rx,
            &mut sink,
        )
        .unwrap_err();
        assert!(err.to_string().contains("timed out"), "{err}");
    }
}
