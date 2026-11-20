//! Pure-OSS RFC 8628 OAuth 2.0 Device Authorization Grant with RFC 8707
//! `resource` audience scoping and caller-private token storage.
//!
//! Tokens are stored strictly inside the caller's own state directory
//! (`$XDG_STATE_HOME/anyagents/control-auth.json` or
//! `~/.local/state/anyagents/control-auth.json`, `0700` dir / `0600` file) and
//! never inside a repository, workspace, or shared coordinator data directory,
//! so child agent sandboxes inherit zero administrative credentials.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_core::Error;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

const CONTROL_AUTH_FILE_ENV: &str = "ANYAGENTS_CONTROL_AUTH_FILE";
const CONTROL_URL_ENV: &str = "ANYAGENTS_CONTROL_URL";
const DEFAULT_SCOPE: &str = "openid email profile";
const DEVICE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";
const HTTP_TIMEOUT_SECS: u64 = 15;
const MAX_HTTP_BODY_BYTES: u64 = 512 * 1024;
const DEFAULT_EXPIRES_IN_SECS: u64 = 600;
const DEFAULT_POLL_INTERVAL_SECS: u64 = 5;
const SLOW_DOWN_INCREMENT_SECS: u64 = 5;

/// Caller-private control-plane OIDC/OAuth2 credential persisted in
/// `$XDG_STATE_HOME/anyagents/control-auth.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredControlAuth {
    pub(crate) coordinator_url: String,
    pub(crate) resource: String,
    pub(crate) issuer: String,
    pub(crate) client_id: String,
    pub(crate) access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) id_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) token_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at_unix: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) email: Option<String>,
    pub(crate) obtained_at_unix: i64,
}

impl StoredControlAuth {
    /// Bearer token presented in `Authorization: Bearer <token>`.
    pub(crate) fn bearer_token(&self) -> Option<&str> {
        let access = self.access_token.trim();
        if !access.is_empty() {
            return Some(access);
        }
        self.id_token
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Whether the stored token is expired as of `now_unix`.
    pub(crate) fn is_expired_at(&self, now_unix: i64) -> bool {
        self.expires_at_unix.is_some_and(|exp| now_unix >= exp)
    }
}

/// Parameters for `anyagents auth login`.
#[derive(Clone, Debug, Default)]
pub(crate) struct DeviceLoginOptions {
    pub(crate) url: Option<String>,
    pub(crate) issuer: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) resource: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ProtectedResourceMetadata {
    #[serde(default)]
    resource: Option<String>,
    #[serde(default)]
    issuer: Option<String>,
    #[serde(default)]
    authorization_servers: Vec<String>,
    #[serde(default)]
    client_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct OidcDeviceDiscovery {
    issuer: String,
    #[serde(default)]
    device_authorization_endpoint: Option<String>,
    token_endpoint: String,
}

/// Parsed RFC 8628 device authorization response.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DeviceAuthResponse {
    pub(crate) device_code: String,
    pub(crate) user_code: String,
    #[serde(alias = "verification_url")]
    pub(crate) verification_uri: String,
    #[serde(default)]
    pub(crate) verification_uri_complete: Option<String>,
    #[serde(default)]
    pub(crate) expires_in: Option<u64>,
    #[serde(default)]
    pub(crate) interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DeviceTokenSuccess {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct DeviceTokenErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

fn unix_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn normalize_url(raw: &str, label: &str) -> Result<String, Error> {
    let trimmed = raw.trim();
    let parsed = Url::parse(trimmed)
        .map_err(|err| Error::Message(format!("invalid {label} URL `{trimmed}`: {err}")))?;
    if !matches!(parsed.scheme(), "https" | "http") {
        return Err(Error::Message(format!(
            "{label} URL must use http or https"
        )));
    }
    Ok(trimmed.trim_end_matches('/').to_string())
}

/// Resolve the caller-private state file path (`$XDG_STATE_HOME/anyagents/control-auth.json`
/// or `~/.local/state/anyagents/control-auth.json`). Never resolves inside a
/// repository, workspace, or coordinator data directory.
pub(crate) fn control_auth_file_path() -> Result<PathBuf, Error> {
    if let Some(explicit) = non_empty_env(CONTROL_AUTH_FILE_ENV) {
        return Ok(PathBuf::from(explicit));
    }
    if let Some(xdg) = non_empty_env("XDG_STATE_HOME") {
        return Ok(PathBuf::from(xdg)
            .join("anyagents")
            .join("control-auth.json"));
    }
    if let Some(home) = non_empty_env("HOME") {
        return Ok(PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("anyagents")
            .join("control-auth.json"));
    }
    Err(Error::Message(
        "cannot determine caller state directory: set XDG_STATE_HOME or HOME".into(),
    ))
}

/// Persist `auth` atomically to `path` with `0700` parent directory and `0600` file permissions.
pub(crate) fn save_control_auth_at(path: &Path, auth: &StoredControlAuth) -> Result<(), Error> {
    let parent = path.parent().ok_or_else(|| {
        Error::Message(format!(
            "invalid control auth path (no parent): {}",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }

    let bytes = serde_json::to_vec_pretty(auth)
        .map_err(|err| Error::Message(format!("failed to serialize control auth: {err}")))?;
    let tmp = parent.join(format!(
        ".control-auth.tmp.{}.{}",
        std::process::id(),
        unix_now_secs()
    ));

    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(&bytes)?;
        f.write_all(b"\n")?;
        f.sync_all()?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let mut payload = bytes;
        payload.push(b'\n');
        std::fs::write(&tmp, payload)?;
    }

    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Load the caller's saved control-plane OIDC token, if present.
pub(crate) fn load_control_auth_at(path: &Path) -> Result<Option<StoredControlAuth>, Error> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let parsed: StoredControlAuth = serde_json::from_slice(&bytes).map_err(|err| {
                Error::Message(format!(
                    "corrupt control auth file {}: {err}",
                    path.display()
                ))
            })?;
            Ok(Some(parsed))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Load from the default caller-private state path.
pub(crate) fn load_control_auth() -> Result<Option<StoredControlAuth>, Error> {
    let path = control_auth_file_path()?;
    load_control_auth_at(&path)
}

fn build_http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(HTTP_TIMEOUT_SECS)))
        .http_status_as_error(false)
        .build()
        .into()
}

fn read_limited_body(resp: &mut ureq::http::Response<ureq::Body>) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = resp
        .body_mut()
        .as_reader()
        .take(MAX_HTTP_BODY_BYTES)
        .read_to_end(&mut buf);
    buf
}

fn decode_jwt_email_hint(jwt: &str) -> Option<String> {
    let mut parts = jwt.split('.');
    let _header = parts.next()?;
    let payload_b64 = parts.next()?;
    let _sig = parts.next()?;
    let bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let val: Value = serde_json::from_slice(&bytes).ok()?;
    val.get("email")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn fetch_coordinator_metadata(
    agent: &ureq::Agent,
    coordinator_url: &str,
) -> Result<ProtectedResourceMetadata, Error> {
    let url = format!("{coordinator_url}/.well-known/oauth-protected-resource");
    let mut resp = agent
        .get(&url)
        .header("accept", "application/json")
        .call()
        .map_err(|err| {
            Error::Message(format!("coordinator OIDC discovery failed ({url}): {err}"))
        })?;
    let status = resp.status().as_u16();
    let body = read_limited_body(&mut resp);
    if !(200..300).contains(&status) {
        return Err(Error::Message(format!(
            "coordinator OIDC discovery returned HTTP {status} ({url}); pass --issuer and --client-id explicitly if the coordinator does not advertise protected-resource metadata"
        )));
    }
    serde_json::from_slice(&body)
        .map_err(|err| Error::Message(format!("invalid coordinator OIDC metadata JSON: {err}")))
}

fn fetch_oidc_discovery(agent: &ureq::Agent, issuer: &str) -> Result<OidcDeviceDiscovery, Error> {
    let url = format!("{issuer}/.well-known/openid-configuration");
    let mut resp = agent
        .get(&url)
        .header("accept", "application/json")
        .call()
        .map_err(|err| Error::Message(format!("OIDC discovery failed ({url}): {err}")))?;
    let status = resp.status().as_u16();
    let body = read_limited_body(&mut resp);
    if !(200..300).contains(&status) {
        return Err(Error::Message(format!(
            "OIDC discovery returned HTTP {status} ({url})"
        )));
    }
    let doc: OidcDeviceDiscovery = serde_json::from_slice(&body)
        .map_err(|err| Error::Message(format!("invalid OIDC discovery JSON: {err}")))?;
    let discovered_issuer = normalize_url(&doc.issuer, "discovered issuer")?;
    if discovered_issuer != issuer {
        return Err(Error::Message(format!(
            "OIDC discovery issuer mismatch (expected `{issuer}`, got `{discovered_issuer}`)"
        )));
    }
    let _ = normalize_url(&doc.token_endpoint, "token_endpoint")?;
    if let Some(ref dev_ep) = doc.device_authorization_endpoint {
        let _ = normalize_url(dev_ep, "device_authorization_endpoint")?;
    }
    Ok(doc)
}

/// Execute RFC 8628 Device Authorization Grant with RFC 8707 `resource` audience
/// scoping, calling `on_prompt` once the verification URI and user code arrive.
pub(crate) fn execute_device_flow_with_timing(
    opts: &DeviceLoginOptions,
    save_path: &Path,
    min_poll_interval: Duration,
    slow_down_step: Duration,
    mut on_prompt: impl FnMut(&DeviceAuthResponse),
) -> Result<StoredControlAuth, Error> {
    let agent = build_http_agent();

    let raw_url = opts
        .url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| non_empty_env(CONTROL_URL_ENV));
    let coordinator_url = match raw_url {
        Some(u) => Some(normalize_url(&u, "coordinator")?),
        None => None,
    };

    let mut issuer = opts
        .issuer
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let mut client_id = opts
        .client_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let mut resource = opts
        .resource
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    if (issuer.is_none() || client_id.is_none() || resource.is_none())
        && let Some(ref coord) = coordinator_url
    {
        let meta = fetch_coordinator_metadata(&agent, coord)?;
        if issuer.is_none() {
            issuer = meta
                .issuer
                .filter(|s| !s.trim().is_empty())
                .or_else(|| meta.authorization_servers.into_iter().next());
        }
        if client_id.is_none() {
            client_id = meta.client_id.filter(|s| !s.trim().is_empty());
        }
        if resource.is_none() {
            resource = meta.resource.filter(|s| !s.trim().is_empty());
        }
    }

    let issuer = normalize_url(
        issuer.as_deref().ok_or_else(|| {
            Error::Message(
                "missing OIDC issuer: pass --url <coordinator_url> or --issuer <oidc_issuer>"
                    .into(),
            )
        })?,
        "OIDC issuer",
    )?;
    let client_id = client_id.ok_or_else(|| {
        Error::Message(
            "missing OAuth client_id: pass --url <coordinator_url> or --client-id <id>".into(),
        )
    })?;
    let effective_resource = match resource.or_else(|| coordinator_url.clone()) {
        Some(res) => normalize_url(&res, "resource")?,
        None => String::new(),
    };
    let effective_coordinator_url = coordinator_url
        .or_else(|| (!effective_resource.is_empty()).then(|| effective_resource.clone()))
        .unwrap_or_else(|| issuer.clone());

    let discovery = fetch_oidc_discovery(&agent, &issuer)?;
    let device_endpoint = discovery.device_authorization_endpoint.ok_or_else(|| {
        Error::Message(format!(
            "OIDC provider `{issuer}` does not advertise RFC 8628 `device_authorization_endpoint`"
        ))
    })?;

    // 1. RFC 8628 §3.1 Device Authorization Request (with RFC 8707 `resource`).
    let mut dev_form: Vec<(&str, &str)> =
        vec![("client_id", client_id.as_str()), ("scope", DEFAULT_SCOPE)];
    if !effective_resource.is_empty() {
        dev_form.push(("resource", effective_resource.as_str()));
    }
    let mut dev_resp = agent
        .post(&device_endpoint)
        .header("accept", "application/json")
        .send_form(dev_form)
        .map_err(|err| {
            Error::Message(format!(
                "RFC 8628 device authorization request failed ({device_endpoint}): {err}"
            ))
        })?;
    let dev_status = dev_resp.status().as_u16();
    let dev_body = read_limited_body(&mut dev_resp);
    if !(200..300).contains(&dev_status) {
        let err_detail = serde_json::from_slice::<DeviceTokenErrorBody>(&dev_body)
            .ok()
            .and_then(|e| e.error_description.or(e.error))
            .unwrap_or_else(|| format!("HTTP {dev_status}"));
        return Err(Error::Message(format!(
            "device authorization request rejected: {err_detail}"
        )));
    }
    let device_auth: DeviceAuthResponse = serde_json::from_slice(&dev_body).map_err(|err| {
        Error::Message(format!(
            "invalid RFC 8628 device authorization JSON response: {err}"
        ))
    })?;
    let _ = normalize_url(&device_auth.verification_uri, "verification_uri")?;
    if let Some(ref complete) = device_auth.verification_uri_complete {
        let _ = normalize_url(complete, "verification_uri_complete")?;
    }

    on_prompt(&device_auth);

    // 2. RFC 8628 §3.4 Device Access Token Polling Loop.
    let expires_in_secs = device_auth
        .expires_in
        .unwrap_or(DEFAULT_EXPIRES_IN_SECS)
        .max(1);
    let deadline = Instant::now() + Duration::from_secs(expires_in_secs);
    let mut poll_interval = Duration::from_secs(
        device_auth
            .interval
            .unwrap_or(DEFAULT_POLL_INTERVAL_SECS)
            .max(1),
    )
    .min(Duration::from_secs(60));
    if min_poll_interval < poll_interval && device_auth.interval == Some(0) {
        poll_interval = min_poll_interval;
    }

    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(Error::Message(
                "device authorization code expired before approval; please run `anyagents auth login` again"
                    .into(),
            ));
        }
        let sleep_for = poll_interval.min(deadline.saturating_duration_since(now));
        if !sleep_for.is_zero() {
            std::thread::sleep(sleep_for);
        }

        let mut token_form: Vec<(&str, &str)> = vec![
            ("grant_type", DEVICE_GRANT_TYPE),
            ("device_code", device_auth.device_code.as_str()),
            ("client_id", client_id.as_str()),
        ];
        if !effective_resource.is_empty() {
            token_form.push(("resource", effective_resource.as_str()));
        }

        let mut tok_resp = agent
            .post(&discovery.token_endpoint)
            .header("accept", "application/json")
            .send_form(token_form)
            .map_err(|err| {
                Error::Message(format!(
                    "OIDC token polling request failed ({}): {err}",
                    discovery.token_endpoint
                ))
            })?;
        let tok_status = tok_resp.status().as_u16();
        let tok_body = read_limited_body(&mut tok_resp);

        if (200..300).contains(&tok_status) {
            let success: DeviceTokenSuccess = serde_json::from_slice(&tok_body).map_err(|err| {
                Error::Message(format!("invalid OIDC token JSON response: {err}"))
            })?;
            let obtained_at = unix_now_secs();
            let expires_at = success
                .expires_in
                .filter(|&s| s > 0)
                .map(|s| obtained_at.saturating_add(s));
            let email = decode_jwt_email_hint(&success.access_token)
                .or_else(|| success.id_token.as_deref().and_then(decode_jwt_email_hint));
            let stored = StoredControlAuth {
                coordinator_url: effective_coordinator_url,
                resource: effective_resource,
                issuer,
                client_id,
                access_token: success.access_token,
                id_token: success.id_token,
                refresh_token: success.refresh_token,
                token_type: success.token_type,
                scope: success.scope,
                expires_at_unix: expires_at,
                email,
                obtained_at_unix: obtained_at,
            };
            save_control_auth_at(save_path, &stored)?;
            return Ok(stored);
        }

        let err_body = serde_json::from_slice::<DeviceTokenErrorBody>(&tok_body).ok();
        match err_body
            .as_ref()
            .and_then(|e| e.error.as_deref())
            .unwrap_or("")
        {
            "authorization_pending" => continue,
            "slow_down" => {
                poll_interval = (poll_interval + slow_down_step).min(Duration::from_secs(60));
                continue;
            }
            "expired_token" => {
                return Err(Error::Message(
                    "device authorization code expired; please run `anyagents auth login` again"
                        .into(),
                ));
            }
            "access_denied" => {
                return Err(Error::Message(
                    "authorization denied by user during device sign-in".into(),
                ));
            }
            other => {
                let desc = err_body
                    .as_ref()
                    .and_then(|e| e.error_description.as_deref())
                    .filter(|s| !s.is_empty());
                let detail = match (other.is_empty(), desc) {
                    (false, Some(d)) => format!("{other}: {d}"),
                    (false, None) => other.to_string(),
                    (true, Some(d)) => d.to_string(),
                    (true, None) => format!("HTTP {tok_status}"),
                };
                return Err(Error::Message(format!(
                    "OIDC device token exchange failed: {detail}"
                )));
            }
        }
    }
}

/// Interactive `anyagents auth login` command using RFC 8628 Device Flow.
pub(crate) fn login_cmd(opts: &DeviceLoginOptions) -> Result<(), Error> {
    let path = control_auth_file_path()?;
    let stored = execute_device_flow_with_timing(
        opts,
        &path,
        Duration::from_secs(1),
        Duration::from_secs(SLOW_DOWN_INCREMENT_SECS),
        |prompt| {
            if let Some(ref complete) = prompt.verification_uri_complete {
                eprintln!("\n  Approve sign-in (1-tap link):");
                eprintln!("    {complete}\n");
                eprintln!(
                    "  Or open {} and enter code: {}\n",
                    prompt.verification_uri, prompt.user_code
                );
            } else {
                eprintln!(
                    "\n  Open {} and enter code: {}\n",
                    prompt.verification_uri, prompt.user_code
                );
            }
            eprintln!("  Waiting for authorization...");
        },
    )?;

    let acct = stored.email.as_deref().unwrap_or("authenticated");
    if crate::ui::is_pretty() {
        crate::ui::success(&format!("Signed in to {} ({acct})", stored.coordinator_url));
        crate::ui::note(&format!("Credentials saved to {}", path.display()));
    } else {
        println!(
            "authenticated\t{}\t{}\t{}",
            stored.coordinator_url,
            acct,
            path.display()
        );
    }
    Ok(())
}

/// `anyagents auth status`: display caller's control-plane OIDC auth state
/// without ever printing secret token values.
pub(crate) fn status_cmd(json_out: bool) -> Result<(), Error> {
    let path = control_auth_file_path()?;
    let loaded = load_control_auth_at(&path)?;
    let now = unix_now_secs();
    if json_out {
        let payload = match loaded {
            Some(ref auth) => serde_json::json!({
                "authenticated": !auth.is_expired_at(now),
                "expired": auth.is_expired_at(now),
                "coordinator_url": auth.coordinator_url,
                "resource": auth.resource,
                "issuer": auth.issuer,
                "client_id": auth.client_id,
                "email": auth.email,
                "expires_at_unix": auth.expires_at_unix,
                "state_file": path.display().to_string(),
            }),
            None => serde_json::json!({
                "authenticated": false,
                "state_file": path.display().to_string(),
            }),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&payload).unwrap_or_default()
        );
        return Ok(());
    }

    match loaded {
        Some(auth) => {
            let expired = auth.is_expired_at(now);
            let status_label = if expired { "expired" } else { "authenticated" };
            if crate::ui::is_pretty() {
                if expired {
                    crate::ui::note(&format!(
                        "Control-plane auth expired for {} ({})",
                        auth.coordinator_url,
                        auth.email.as_deref().unwrap_or("-")
                    ));
                } else {
                    crate::ui::success(&format!(
                        "Authenticated to {} ({})",
                        auth.coordinator_url,
                        auth.email.as_deref().unwrap_or("-")
                    ));
                }
                crate::ui::note(&format!("Issuer: {}", auth.issuer));
                if !auth.resource.is_empty() {
                    crate::ui::note(&format!("Resource: {}", auth.resource));
                }
                crate::ui::note(&format!("State file: {}", path.display()));
            } else {
                println!(
                    "{status_label}\t{}\t{}\t{}",
                    auth.coordinator_url,
                    auth.email.as_deref().unwrap_or("-"),
                    path.display()
                );
            }
        }
        None => {
            if crate::ui::is_pretty() {
                crate::ui::note(
                    "Not signed in to any remote coordinator (`anyagents auth login --url <url>`).",
                );
            } else {
                println!("unauthenticated\t-\t-\t{}", path.display());
            }
        }
    }
    Ok(())
}

/// `anyagents auth logout`: remove the caller's saved control-plane OIDC token.
pub(crate) fn logout_cmd() -> Result<(), Error> {
    let path = control_auth_file_path()?;
    let removed = match std::fs::remove_file(&path) {
        Ok(()) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => return Err(err.into()),
    };
    if crate::ui::is_pretty() {
        if removed {
            crate::ui::success(&format!(
                "Removed control-plane credentials at {}",
                path.display()
            ));
        } else {
            crate::ui::note("Already signed out (no control-plane credentials found).");
        }
    } else {
        println!(
            "{}\t{}",
            if removed {
                "logged_out"
            } else {
                "already_signed_out"
            },
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use super::*;

    fn spawn_mock_http_server(
        handler: impl Fn(String, String, String) -> (u16, String) + Send + Sync + 'static,
    ) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let base = format!("http://{addr}");
        let handler = Arc::new(handler);
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut req_line = String::new();
                if reader.read_line(&mut req_line).unwrap_or(0) == 0 {
                    continue;
                }
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("").to_string();
                let mut content_len = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        break;
                    }
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        break;
                    }
                    if let Some((k, v)) = trimmed.split_once(':')
                        && k.trim().eq_ignore_ascii_case("content-length")
                    {
                        content_len = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body_buf = vec![0u8; content_len];
                if content_len > 0 {
                    let _ = reader.read_exact(&mut body_buf);
                }
                let body_str = String::from_utf8_lossy(&body_buf).to_string();
                let (status, resp_body) = handler(method, path, body_str);
                let reason = match status {
                    200 => "OK",
                    400 => "Bad Request",
                    _ => "Error",
                };
                let mut stream = stream;
                let resp = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                    resp_body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        (base, handle)
    }

    fn fake_jwt_with_email(email: &str) -> String {
        let h = URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","typ":"JWT"}"#);
        let p = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({
                "sub": "u-1",
                "email": email,
                "email_verified": true,
            }))
            .unwrap(),
        );
        format!("{h}.{p}.sig")
    }

    #[test]
    fn rfc8628_device_flow_discovers_sends_rfc8707_resource_polls_and_saves_private_0600() {
        let seen_device_body = Arc::new(Mutex::new(String::new()));
        let seen_token_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let dev_cap = Arc::clone(&seen_device_body);
        let tok_cap = Arc::clone(&seen_token_bodies);
        let polls = Arc::clone(&poll_count);
        let base_slot = Arc::new(Mutex::new(String::new()));
        let base_for_srv = Arc::clone(&base_slot);

        let (base, _handle) = spawn_mock_http_server(move |method, path, body| {
            let base = base_for_srv.lock().unwrap().clone();
            match (method.as_str(), path.as_str()) {
                ("GET", "/.well-known/oauth-protected-resource") => (
                    200,
                    serde_json::json!({
                        "resource": base,
                        "authorization_servers": [base],
                        "issuer": base,
                        "client_id": "anyagents-cli-public",
                    })
                    .to_string(),
                ),
                ("GET", "/.well-known/openid-configuration") => (
                    200,
                    serde_json::json!({
                        "issuer": base,
                        "device_authorization_endpoint": format!("{base}/oauth/device"),
                        "token_endpoint": format!("{base}/oauth/token"),
                    })
                    .to_string(),
                ),
                ("POST", "/oauth/device") => {
                    *dev_cap.lock().unwrap() = body;
                    (
                        200,
                        serde_json::json!({
                            "device_code": "dev-code-123",
                            "user_code": "ABCD-EFGH",
                            "verification_uri": format!("{base}/verify"),
                            "verification_uri_complete": format!("{base}/verify?user_code=ABCD-EFGH"),
                            "expires_in": 120,
                            "interval": 0
                        })
                        .to_string(),
                    )
                }
                ("POST", "/oauth/token") => {
                    tok_cap.lock().unwrap().push(body);
                    let n = polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if n == 0 {
                        (
                            400,
                            serde_json::json!({ "error": "authorization_pending" }).to_string(),
                        )
                    } else if n == 1 {
                        (400, serde_json::json!({ "error": "slow_down" }).to_string())
                    } else {
                        (
                            200,
                            serde_json::json!({
                                "access_token": fake_jwt_with_email("admin@example.com"),
                                "token_type": "Bearer",
                                "scope": "openid email profile",
                                "expires_in": 3600
                            })
                            .to_string(),
                        )
                    }
                }
                _ => (404, r#"{"error":"not found"}"#.to_string()),
            }
        });
        *base_slot.lock().unwrap() = base.clone();

        let dir = tempfile::tempdir().unwrap();
        let save_path = dir
            .path()
            .join("state")
            .join("anyagents")
            .join("control-auth.json");
        let mut prompted_complete = None;

        let saved = execute_device_flow_with_timing(
            &DeviceLoginOptions {
                url: Some(base.clone()),
                issuer: None,
                client_id: None,
                resource: None,
            },
            &save_path,
            Duration::from_millis(5),
            Duration::from_millis(5),
            |p| {
                prompted_complete = p.verification_uri_complete.clone();
            },
        )
        .expect("RFC 8628 device flow should complete after pending + slow_down");

        assert_eq!(
            prompted_complete.as_deref(),
            Some(format!("{base}/verify?user_code=ABCD-EFGH").as_str())
        );
        assert_eq!(saved.coordinator_url, base);
        assert_eq!(saved.resource, base);
        assert_eq!(saved.client_id, "anyagents-cli-public");
        assert_eq!(saved.email.as_deref(), Some("admin@example.com"));
        assert_eq!(poll_count.load(std::sync::atomic::Ordering::SeqCst), 3);

        // Verify RFC 8707 `resource` and `scope` were sent on the device request.
        let dev_form = seen_device_body.lock().unwrap().clone();
        assert!(
            dev_form.contains("client_id=anyagents-cli-public"),
            "{dev_form}"
        );
        assert!(
            dev_form.contains("scope=openid+email+profile"),
            "{dev_form}"
        );
        assert!(dev_form.contains("resource=http"), "{dev_form}");

        // Verify RFC 8628 grant_type + RFC 8707 resource were sent on token polls.
        let tok_forms = seen_token_bodies.lock().unwrap().clone();
        assert_eq!(tok_forms.len(), 3);
        assert!(tok_forms[0].contains("device_code=dev-code-123"));
        assert!(tok_forms[0].contains("resource=http"));

        // Verify caller-private permissions (`0700` dir / `0600` file) on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let dir_mode = std::fs::metadata(save_path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            let file_mode = std::fs::metadata(&save_path).unwrap().permissions().mode() & 0o777;
            assert_eq!(dir_mode, 0o700, "state dir must be 0700");
            assert_eq!(file_mode, 0o600, "control-auth.json must be 0600");
        }

        let reloaded = load_control_auth_at(&save_path).unwrap().unwrap();
        assert_eq!(reloaded, saved);
    }

    #[test]
    fn rfc8628_device_flow_handles_access_denied_and_expired_token() {
        for terminal_err in ["access_denied", "expired_token"] {
            let err_code = terminal_err.to_string();
            let base_slot = Arc::new(Mutex::new(String::new()));
            let base_for_srv = Arc::clone(&base_slot);

            let (base, _handle) = spawn_mock_http_server(move |method, path, _body| {
                let base = base_for_srv.lock().unwrap().clone();
                match (method.as_str(), path.as_str()) {
                    ("GET", "/.well-known/openid-configuration") => (
                        200,
                        serde_json::json!({
                            "issuer": base,
                            "device_authorization_endpoint": format!("{base}/device"),
                            "token_endpoint": format!("{base}/token"),
                        })
                        .to_string(),
                    ),
                    ("POST", "/device") => (
                        200,
                        serde_json::json!({
                            "device_code": "dc-1",
                            "user_code": "WXYZ-1234",
                            "verification_uri": format!("{base}/verify"),
                            "expires_in": 60,
                            "interval": 0
                        })
                        .to_string(),
                    ),
                    ("POST", "/token") => {
                        (400, serde_json::json!({ "error": err_code }).to_string())
                    }
                    _ => (404, "{}".to_string()),
                }
            });
            *base_slot.lock().unwrap() = base.clone();

            let dir = tempfile::tempdir().unwrap();
            let save_path = dir.path().join("control-auth.json");
            let err = execute_device_flow_with_timing(
                &DeviceLoginOptions {
                    url: Some("https://coord.example.com".into()),
                    issuer: Some(base),
                    client_id: Some("cid".into()),
                    resource: Some("https://coord.example.com".into()),
                },
                &save_path,
                Duration::from_millis(2),
                Duration::from_millis(2),
                |_| {},
            )
            .unwrap_err();
            assert!(!save_path.exists());
            if terminal_err == "access_denied" {
                assert!(err.to_string().contains("denied"), "{err}");
            } else {
                assert!(err.to_string().contains("expired"), "{err}");
            }
        }
    }
}
