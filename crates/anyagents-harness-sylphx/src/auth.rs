//! RFC 8628 Device Flow authentication, credential discovery, and Sylphx configuration.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyagents_installer::{InstallError, LineSink};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[cfg(test)]
mod transport_tests;

pub const DEFAULT_SYLPHX_API_URL: &str = "https://api.sylphx.com";
pub const DEFAULT_SYLPHX_GATEWAY_URL: &str = "https://api.sylphx.com/v1";
pub const DEFAULT_SYLPHX_MODEL: &str = "sylphx/openai/gpt-6.1-sol#medium";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SylphxOrgCredential {
    pub id: String,
    pub slug: String,
    pub key_name: String,
    pub store: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SylphxCredentials {
    pub api_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orgs: Vec<SylphxOrgCredential>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_org: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeviceAuthorizeResponse {
    device_code: String,
    user_code: String,
    #[allow(dead_code)]
    verification_uri: String,
    verification_uri_complete: String,
    expires_in: u64,
    interval: u64,
}

fn sylphx_host_label() -> String {
    let raw = std::env::var("SYLPHX_DEVICE_NAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "anyagents".to_string());
    let mut out = String::new();
    let mut gap = false;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
            if gap && !out.is_empty() && !out.ends_with('-') && c != '-' {
                out.push('-');
            }
            gap = false;
            out.push(c);
        } else {
            gap = true;
        }
    }
    let label: String = out.chars().take(64).collect();
    if label.is_empty() {
        "anyagents".to_string()
    } else {
        label
    }
}

/// Runs RFC 8628 Device Flow against `https://api.sylphx.com` and persists the
/// resulting org-scoped key(s) under `config_dir`.
pub fn run_device_flow_login(
    config_dir: &Path,
    on_line: &mut dyn LineSink,
) -> Result<(), InstallError> {
    let api_url = std::env::var("SYLPHX_API_URL")
        .or_else(|_| std::env::var("SYLPHX_BASE_URL"))
        .ok()
        .map(|s| {
            s.trim()
                .trim_end_matches('/')
                .trim_end_matches("/v1")
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string());

    let authorize_url = format!("{api_url}/v1/access/device/authorize");
    let auth_body = json!({
        "client": "sylphx-cli",
        "host": sylphx_host_label(),
    });
    let (status, raw) = curl_post_json(&authorize_url, None, &auth_body)
        .map_err(|e| InstallError::Failed(format!("Sylphx device authorize failed: {e}")))?;
    if !(200..300).contains(&status) {
        return Err(InstallError::Failed(format!(
            "Sylphx device authorize HTTP {status}: {}",
            raw.trim()
        )));
    }
    let device: DeviceAuthorizeResponse = serde_json::from_str(&raw).map_err(|e| {
        InstallError::Failed(format!("invalid Sylphx device authorize response: {e}"))
    })?;

    on_line.line(&format!("LINK {}", device.verification_uri_complete));
    on_line.line(&format!("DEVICE {}", device.user_code));

    let token_url = format!("{api_url}/v1/access/device/token");
    let mut interval_secs = device.interval.max(2);
    let deadline = Instant::now() + Duration::from_secs(device.expires_in.max(60));

    loop {
        if Instant::now() >= deadline {
            return Err(InstallError::Failed(
                "Sylphx device authorization timed out".to_string(),
            ));
        }
        std::thread::sleep(Duration::from_secs(interval_secs));
        let poll_body = json!({ "device_code": device.device_code });
        let Ok((poll_status, poll_raw)) = curl_post_json(&token_url, None, &poll_body) else {
            continue;
        };
        if poll_status == 429 || (500..600).contains(&poll_status) {
            continue;
        }

        if (200..300).contains(&poll_status) {
            let v: Value = serde_json::from_str(&poll_raw).map_err(|e| {
                InstallError::Failed(format!("invalid Sylphx device token response: {e}"))
            })?;
            let mut orgs: Vec<SylphxOrgCredential> = Vec::new();
            if let Some(arr) = v.get("keys").and_then(Value::as_array) {
                for item in arr {
                    let Some(k) = item
                        .get("api_key")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    else {
                        continue;
                    };
                    let org_id = item
                        .pointer("/org/id")
                        .and_then(Value::as_str)
                        .unwrap_or("sylphx")
                        .to_string();
                    let org_slug = item
                        .pointer("/org/slug")
                        .and_then(Value::as_str)
                        .unwrap_or(&org_id)
                        .to_string();
                    let key_name = item
                        .pointer("/key/name")
                        .and_then(Value::as_str)
                        .unwrap_or("device")
                        .to_string();
                    orgs.push(SylphxOrgCredential {
                        id: org_id,
                        slug: org_slug,
                        key_name,
                        store: "file".to_string(),
                        api_key: Some(k.to_string()),
                    });
                }
            }
            let primary = orgs
                .iter()
                .find(|o| o.slug == "sylphx" || o.id == "sylphx")
                .or_else(|| orgs.first())
                .cloned();
            let (api_key, key_name, org_id, org_slug) = if let Some(p) = primary {
                (
                    p.api_key.unwrap_or_default(),
                    Some(p.key_name),
                    Some(p.id),
                    Some(p.slug),
                )
            } else {
                let k = v
                    .get("api_key")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        InstallError::Failed(
                            "Sylphx device token response contained no api_key".to_string(),
                        )
                    })?
                    .to_string();
                let kn = v
                    .get("key_name")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let oid = v.get("org_id").and_then(Value::as_str).map(str::to_string);
                let oslug = v
                    .get("org_slug")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                (k, kn, oid, oslug)
            };
            let creds = SylphxCredentials {
                api_key,
                key_name,
                org_id: org_id.clone(),
                org_slug: org_slug.clone(),
                api_url: Some(api_url.clone()),
                base_url: Some(api_url),
                store: Some("file".to_string()),
                orgs,
                active_org: org_id,
            };
            save_sylphx_credentials(config_dir, &creds)
                .map_err(|e| InstallError::Failed(format!("failed to save Sylphx key: {e}")))?;
            let account = creds
                .org_slug
                .as_deref()
                .or(creds.org_id.as_deref())
                .or(creds.key_name.as_deref())
                .unwrap_or("sylphx");
            on_line.line(&format!("SIGNED_IN {account}"));
            return Ok(());
        }

        let err_code = serde_json::from_str::<Value>(&poll_raw)
            .ok()
            .and_then(|v| {
                v.get("error")
                    .and_then(|e| {
                        e.as_str()
                            .map(str::to_string)
                            .or_else(|| e.get("code").and_then(Value::as_str).map(str::to_string))
                            .or_else(|| {
                                e.get("message").and_then(Value::as_str).map(str::to_string)
                            })
                    })
                    .or_else(|| v.get("code").and_then(Value::as_str).map(str::to_string))
            })
            .unwrap_or_default();

        if poll_status == 428 || err_code.contains("authorization_pending") {
            continue;
        }
        if err_code.contains("slow_down") {
            interval_secs += 5;
            continue;
        }
        return Err(InstallError::Failed(format!(
            "Sylphx device authorization failed (HTTP {poll_status}): {}",
            if err_code.is_empty() {
                poll_raw.trim()
            } else {
                &err_code
            }
        )));
    }
}

/// Writes the Sylphx credentials to `.config/sylphx/credentials.json`,
/// `.config/opencode/sylphx.key`, and `.config/sylphx/key` under `base_dir`.
///
/// Preserves any existing project-environment gateway key (`sk-sx-...`) in
/// `.config/opencode/sylphx.key` and `.config/sylphx/key` when `creds.api_key`
/// is an org-wide device flow key (`sylphx_sk_...`).
pub fn save_sylphx_credentials(
    base_dir: &Path,
    creds: &SylphxCredentials,
) -> Result<(), std::io::Error> {
    let sylphx_dir = base_dir.join(".config/sylphx");
    std::fs::create_dir_all(&sylphx_dir)?;
    let creds_path = sylphx_dir.join("credentials.json");
    let body = serde_json::to_string_pretty(creds).map_err(std::io::Error::other)?;
    write_secret_file(&creds_path, &format!("{body}\n"))?;

    let key_trimmed = creds.api_key.trim();
    if !key_trimmed.is_empty() {
        let fallback_gw_key = if key_trimmed.starts_with("sylphx_sk_") {
            crate::sandbox::host_home_dir()
                .and_then(|h| read_sylphx_gateway_key_from_dir(&h))
                .filter(|k| k.starts_with("sk-sx-"))
                .unwrap_or_else(|| key_trimmed.to_string())
        } else {
            key_trimmed.to_string()
        };
        for rel in [".config/opencode/sylphx.key", ".config/sylphx/key"] {
            let p = base_dir.join(rel);
            let existing_is_project_key = std::fs::read_to_string(&p)
                .ok()
                .map(|s| s.trim().starts_with("sk-sx-"))
                .unwrap_or(false);
            if existing_is_project_key && key_trimmed.starts_with("sylphx_sk_") {
                continue;
            }
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent)?;
            }
            write_secret_file(&p, &format!("{fallback_gw_key}\n"))?;
        }
    }
    Ok(())
}

fn write_secret_file(path: &Path, content: &str) -> Result<(), std::io::Error> {
    std::fs::write(path, content)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Writes `<config_dir>/sylphx-agent.json`, `<config_dir>/adapter.json`
/// (`"mode": "sylphx-native"`), and keeps `.config/opencode/opencode.json`
/// in sync for an agent running on `--harness sylphx`.
pub fn write_sylphx_config(
    config_dir: &Path,
    model: Option<&str>,
    mcp_command: &Path,
    mcp_args: &[String],
) -> Result<(), anyagents_core::Error> {
    std::fs::create_dir_all(config_dir)?;
    let resolved_model = model
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SYLPHX_MODEL);
    let gateway_url = std::env::var("SYLPHX_GATEWAY_URL")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_SYLPHX_GATEWAY_URL.to_string());
    let cloud_api_url = std::env::var("SYLPHX_API_URL")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string());

    let mut command_vec = vec![mcp_command.display().to_string()];
    command_vec.extend(mcp_args.iter().cloned());

    let existing_agent_json = std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok());
    let worktree_marker =
        std::fs::read_to_string(config_dir.join(".anyagents-managed-worktree.json"))
            .ok()
            .and_then(|s| serde_json::from_str::<Value>(&s).ok());

    let mut doc = json!({
        "schema_version": 1,
        "harness": "sylphx",
        "model": resolved_model,
        "gateway_base_url": gateway_url,
        "cloud_api_url": cloud_api_url,
        "mcpServers": {
            "anyagents": {
                "command": mcp_command.display().to_string(),
                "args": mcp_args,
            }
        },
        "mcp": {
            "anyagents": {
                "type": "local",
                "command": command_vec,
                "enabled": true,
            }
        }
    });

    if let Some(obj) = doc.as_object_mut() {
        for key in ["sandbox", "repo", "branch", "base"] {
            if let Some(val) = worktree_marker
                .as_ref()
                .and_then(|m| m.get(key))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    existing_agent_json
                        .as_ref()
                        .and_then(|e| e.get(key))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                })
            {
                obj.insert(key.to_string(), json!(val));
            }
        }
        if !obj.contains_key("repo") {
            let wt = crate::cloud::resolve_agent_git_worktree_spec(
                config_dir,
                None,
                &[],
                None,
                None,
                None,
            );
            if let Some(slug) = wt.repo_slug {
                obj.insert("repo".to_string(), json!(slug));
            }
            if !obj.contains_key("branch")
                && let Some(br) = wt.branch
            {
                obj.insert("branch".to_string(), json!(br));
            }
            if !obj.contains_key("base")
                && let Some(base) = wt.base
            {
                obj.insert("base".to_string(), json!(base));
            }
        }
        let marker_cloud = worktree_marker
            .as_ref()
            .and_then(|m| m.get("cloud_sandbox"))
            .and_then(Value::as_bool);
        if marker_cloud == Some(true)
            || (!obj.contains_key("sandbox")
                && marker_cloud != Some(false)
                && !crate::sandbox::is_isolated_test_env()
                && crate::cloud::resolve_sylphx_cloud_access_key(config_dir, &[]).is_some())
        {
            obj.insert("sandbox".to_string(), json!("cloud"));
        }
    }
    let text = serde_json::to_string_pretty(&doc)
        .map_err(|e| anyagents_core::Error::Message(e.to_string()))?;
    std::fs::write(config_dir.join("sylphx-agent.json"), format!("{text}\n"))?;

    let adapter_doc = json!({
        "harness": "sylphx",
        "mode": "sylphx-native",
        "config_dir": config_dir.display().to_string(),
        "model": resolved_model,
        "mcp_command": mcp_command.display().to_string(),
        "mcp_args": mcp_args,
    });
    if let Ok(adapter_text) = serde_json::to_string_pretty(&adapter_doc) {
        let _ = std::fs::write(config_dir.join("adapter.json"), format!("{adapter_text}\n"));
    }

    Ok(())
}

/// Resolves the Sylphx Cloud Platform API key (`https://api.sylphx.com/v1/...`),
/// prioritizing `.config/sylphx/credentials.json` (`sylphx_sk_...`).
pub fn resolve_sylphx_api_key(config_dir: &Path, env: &[(String, String)]) -> Option<String> {
    for (k, v) in env {
        if (k == "SYLPHX_API_KEY" || k == "SYLPHX_AI_KEY") && !v.trim().is_empty() {
            return Some(v.trim().to_string());
        }
    }
    for k in ["SYLPHX_API_KEY", "SYLPHX_AI_KEY"] {
        if let Ok(v) = std::env::var(k)
            && !v.trim().is_empty()
        {
            return Some(v.trim().to_string());
        }
    }
    if let Some(key) = read_sylphx_key_from_dir(config_dir) {
        return Some(key);
    }
    if let Some(data_dir) = config_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        && let Some(key) = read_sylphx_key_from_dir(&data_dir.join("profiles/sylphx/default"))
    {
        return Some(key);
    }
    if let Some(home) = crate::sandbox::host_home_dir()
        && let Some(key) = read_sylphx_key_from_dir(&home)
    {
        return Some(key);
    }
    None
}

/// Resolves the Sylphx AI Gateway key (`https://api.sylphx.com/v1/responses`),
/// prioritizing project-environment keys (`sk-sx-...` in `.config/sylphx/key` or
/// `.config/opencode/sylphx.key`) before falling back to `.config/sylphx/credentials.json`.
pub fn resolve_sylphx_gateway_key(config_dir: &Path, env: &[(String, String)]) -> Option<String> {
    for (k, v) in env {
        if (k == "SYLPHX_AI_KEY" || k == "SYLPHX_API_KEY" || k == "SYLPHX_GATEWAY_KEY")
            && !v.trim().is_empty()
        {
            return Some(v.trim().to_string());
        }
    }
    for k in ["SYLPHX_AI_KEY", "SYLPHX_API_KEY", "SYLPHX_GATEWAY_KEY"] {
        if let Ok(v) = std::env::var(k)
            && !v.trim().is_empty()
        {
            return Some(v.trim().to_string());
        }
    }
    let mut candidate_dirs = vec![config_dir.to_path_buf()];
    if let Some(data_dir) = config_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
    {
        candidate_dirs.push(data_dir.join("profiles/sylphx/default"));
    }
    if let Some(home) = crate::sandbox::host_home_dir() {
        candidate_dirs.push(home);
    }
    for dir in &candidate_dirs {
        if let Some(k) = read_sylphx_gateway_key_from_dir(dir)
            && k.starts_with("sk-sx-")
        {
            return Some(k);
        }
    }
    for dir in &candidate_dirs {
        if let Some(k) = read_sylphx_gateway_key_from_dir(dir) {
            return Some(k);
        }
    }
    resolve_sylphx_api_key(config_dir, env)
}

pub fn read_sylphx_gateway_key_from_dir(base: &Path) -> Option<String> {
    for rel in [
        ".config/sylphx/key",
        ".config/opencode/sylphx.key",
        "sylphx.key",
    ] {
        if let Ok(raw) = std::fs::read_to_string(base.join(rel)) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

pub fn read_sylphx_key_from_dir(base: &Path) -> Option<String> {
    let creds_path = base.join(".config/sylphx/credentials.json");
    if let Ok(raw) = std::fs::read_to_string(&creds_path)
        && let Ok(val) = serde_json::from_str::<Value>(&raw)
    {
        if let Some(k) = val
            .get("api_key")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(k.to_string());
        }
        if let Some(orgs) = val.get("orgs").and_then(Value::as_array) {
            let active = val.get("active_org").and_then(Value::as_str);
            if let Some(act) = active
                && let Some(k) = orgs
                    .iter()
                    .find(|o| {
                        o.get("id").and_then(Value::as_str) == Some(act)
                            || o.get("slug").and_then(Value::as_str) == Some(act)
                    })
                    .and_then(|o| o.get("api_key"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
            {
                return Some(k.to_string());
            }
            if let Some(k) = orgs.iter().find_map(|o| {
                o.get("api_key")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
            }) {
                return Some(k.to_string());
            }
        }
    }
    for rel in [
        ".config/sylphx/key",
        ".config/opencode/sylphx.key",
        "sylphx.key",
    ] {
        if let Ok(raw) = std::fs::read_to_string(base.join(rel)) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

/// Splits a model string such as `"sylphx/openai/gpt-6.1-sol#xhigh"` into
/// `(upstream_model_id, optional_reasoning_effort)`.
pub fn parse_sylphx_model_and_effort(raw: Option<&str>) -> (String, Option<String>) {
    let m = raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SYLPHX_MODEL);
    let (base, effort) = match m.split_once('#') {
        Some((b, e)) if !b.trim().is_empty() && !e.trim().is_empty() => {
            (b.trim(), Some(e.trim().to_string()))
        }
        _ => (m, None),
    };
    let stripped = base.strip_prefix("sylphx/").unwrap_or(base).to_string();
    (stripped, effort)
}

/// Normalizes legacy Sylphx AI hostnames (`api.models.sylphx.ai`, `models.sylphx.ai`,
/// `ai.sylphx.com`, `api.sylphx.ai`) to the canonical `https://api.sylphx.com/v1`.
pub fn normalize_sylphx_gateway_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty()
        || trimmed.contains("models.sylphx.ai")
        || trimmed.contains("api.sylphx.ai")
        || trimmed.contains("://ai.sylphx.com")
    {
        return DEFAULT_SYLPHX_GATEWAY_URL.to_string();
    }
    if trimmed == "https://api.sylphx.com" {
        return DEFAULT_SYLPHX_GATEWAY_URL.to_string();
    }
    trimmed.to_string()
}

fn shared_http_agent() -> &'static ureq::Agent {
    static AGENT: std::sync::OnceLock<ureq::Agent> = std::sync::OnceLock::new();
    AGENT.get_or_init(|| {
        ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(180)))
            .build()
            .new_agent()
    })
}

pub(super) fn is_transient_gateway_failure(status: u16, body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    if lower.contains("\"retryable\":false") || lower.contains("\"retryable\": false") {
        return false;
    }
    let is_hard_quota = lower.contains("insufficient_quota")
        || lower.contains("credit balance is too low")
        || lower.contains("out of credits")
        || lower.contains("quota exceeded");
    if matches!(status, 408 | 409 | 425 | 500 | 502 | 503 | 504 | 520..=529) {
        return true;
    }
    if status == 429 {
        return !is_hard_quota;
    }
    if !(200..300).contains(&status) {
        return lower.contains("model_unavailable")
            || lower.contains("temporarily unavailable")
            || lower.contains("overloaded")
            || lower.contains("bad gateway")
            || lower.contains("service unavailable")
            || lower.contains("gateway timeout");
    }
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return true;
    }
    match serde_json::from_str::<Value>(trimmed) {
        Err(_) => true,
        Ok(val) => {
            let has_valid_output = val.get("error").is_none_or(Value::is_null)
                && val.get("output").is_some_and(Value::is_array);
            if has_valid_output || is_hard_quota {
                return false;
            }
            let has_error_or_failed = !val.get("error").is_none_or(Value::is_null)
                || val.get("status").and_then(Value::as_str) == Some("failed");
            has_error_or_failed
                && (lower.contains("model_unavailable")
                    || lower.contains("included_capacity_unavailable")
                    || lower.contains("temporarily unavailable")
                    || lower.contains("overloaded")
                    || lower.contains("server_error")
                    || lower.contains("api_error")
                    || lower.contains("rate_limit")
                    || lower.contains("\"retryable\":true")
                    || lower.contains("\"retryable\": true")
                    || lower.contains("bad gateway")
                    || lower.contains("service unavailable")
                    || lower.contains("gateway timeout"))
        }
    }
}

fn gateway_retry_policy() -> (u32, u64) {
    let max_retries = std::env::var("SYLPHX_MAX_RETRIES")
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(8);
    let base_backoff_ms = std::env::var("SYLPHX_RETRY_BACKOFF_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(1_000);
    (max_retries, base_backoff_ms)
}

fn parse_retry_after_ms(body: &str) -> Option<u64> {
    let val = serde_json::from_str::<Value>(body.trim()).ok()?;
    if let Some(ms) = val
        .get("retry_after_ms")
        .or_else(|| val.pointer("/error/retry_after_ms"))
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
        })
    {
        return Some(ms.min(30_000));
    }
    let secs = val
        .get("error")
        .and_then(|e| e.get("retry_after_seconds"))
        .and_then(Value::as_u64)
        .or_else(|| val.get("retry_after_seconds").and_then(Value::as_u64))?;
    Some(secs.min(30).saturating_mul(1_000))
}

fn curl_post_json_once(
    url: &str,
    bearer_token: Option<&str>,
    payload: &str,
) -> Result<(u16, String), String> {
    let mut req = shared_http_agent()
        .post(url)
        .header("content-type", "application/json");
    if let Some(tok) = bearer_token {
        req = req.header("authorization", &format!("Bearer {tok}"));
    }
    if let Ok(resp) = req.send(payload.as_bytes()) {
        let code = resp.status().as_u16();
        if let Ok(text) = resp.into_body().read_to_string() {
            return Ok((code, text));
        }
    }

    curl_post_json_fallback(url, bearer_token, payload)
}

fn curl_post_json_fallback(
    url: &str,
    bearer_token: Option<&str>,
    payload: &str,
) -> Result<(u16, String), String> {
    let mut cmd = Command::new("curl");
    cmd.args([
        "-sS",
        "-X",
        "POST",
        "--max-time",
        "210",
        "-H",
        "Content-Type: application/json",
        "-w",
        "\n%{http_code}",
    ]);
    if let Some(tok) = bearer_token {
        cmd.arg("-H").arg(format!("Authorization: Bearer {tok}"));
    }
    // Conversation bodies can exceed the OS limit for one argv entry. Send
    // the exact serialized bytes through stdin instead of the command line.
    cmd.arg("--data-binary").arg("@-").arg(url);
    run_json_post_command(cmd, payload)
}

fn run_json_post_command(mut cmd: Command, payload: &str) -> Result<(u16, String), String> {
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| e.to_string())?;
    let written = child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::other("HTTP request stdin missing"))
        .and_then(|mut stdin| stdin.write_all(payload.as_bytes()));
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    if !out.status.success() && out.stdout.is_empty() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    let response = parse_curl_status_and_body(&String::from_utf8_lossy(&out.stdout));
    if let Err(error) = written {
        // A client may close stdin after receiving an early HTTP refusal. Its
        // actual response takes precedence over our write-side BrokenPipe.
        if response
            .as_ref()
            .is_ok_and(|(status, _)| (100..600).contains(status))
        {
            return response;
        }
        return Err(format!("writing HTTP request body: {error}"));
    }
    response
}

pub(super) fn curl_post_json(
    url: &str,
    bearer_token: Option<&str>,
    body: &Value,
) -> Result<(u16, String), String> {
    let payload = serde_json::to_string(body).map_err(|e| e.to_string())?;
    let (max_retries, base_backoff_ms) = gateway_retry_policy();
    let jitter_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
        ^ (std::process::id() as u64);
    let mut attempt = 0u32;
    loop {
        match curl_post_json_once(url, bearer_token, &payload) {
            Ok((code, text)) => {
                if attempt < max_retries && is_transient_gateway_failure(code, &text) {
                    let exp_ms = base_backoff_ms.saturating_mul(1u64 << attempt.min(4));
                    let retry_after_ms = parse_retry_after_ms(&text).unwrap_or(0);
                    let jitter_ms =
                        if base_backoff_ms >= 500 {
                            (jitter_seed.wrapping_add(attempt as u64 * 313)) % 750
                        } else {
                            0
                        };
                    let delay_ms = exp_ms.max(retry_after_ms).saturating_add(jitter_ms);
                    std::thread::sleep(Duration::from_millis(delay_ms));
                    attempt += 1;
                    continue;
                }
                return Ok((code, text));
            }
            Err(err) => {
                if attempt < max_retries {
                    let delay_ms = base_backoff_ms.saturating_mul(1u64 << attempt.min(4));
                    std::thread::sleep(Duration::from_millis(delay_ms));
                    attempt += 1;
                    continue;
                }
                return Err(err);
            }
        }
    }
}

pub(super) fn curl_get(url: &str, bearer_token: Option<&str>) -> Result<(u16, String), String> {
    let mut req = shared_http_agent().get(url);
    if let Some(tok) = bearer_token {
        req = req.header("authorization", &format!("Bearer {tok}"));
    }
    if let Ok(resp) = req.call() {
        let code = resp.status().as_u16();
        if let Ok(text) = resp.into_body().read_to_string() {
            return Ok((code, text));
        }
    }

    let mut cmd = Command::new("curl");
    cmd.args(["-sS", "--max-time", "30", "-w", "\n%{http_code}"]);
    if let Some(tok) = bearer_token {
        cmd.arg("-H").arg(format!("Authorization: Bearer {tok}"));
    }
    cmd.arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = cmd.output().map_err(|e| e.to_string())?;
    if !out.status.success() && out.stdout.is_empty() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    parse_curl_status_and_body(&String::from_utf8_lossy(&out.stdout))
}

/// UTF-8-safe Head + Tail middle-out observation truncation (`arXiv:2508.21433`,
/// NeurIPS DL4Code 2025): preserves both the command/header at the top AND the
/// compiler/test failure trace and exit summary at the bottom while eliding
/// repetitive middle build/log lines.
pub(super) fn truncate_tool_output_middle_utf8(
    text: String,
    max_bytes: usize,
    head_bytes: usize,
    tail_bytes: usize,
) -> String {
    if text.len() <= max_bytes || head_bytes + tail_bytes >= text.len() {
        return text;
    }
    let mut head_end = head_bytes.min(text.len());
    while head_end > 0 && !text.is_char_boundary(head_end) {
        head_end -= 1;
    }
    if let Some(nl) = text[..head_end].rfind('\n')
        && head_end.saturating_sub(nl) <= 512
    {
        head_end = nl;
    }

    let mut tail_start = text.len().saturating_sub(tail_bytes);
    while tail_start < text.len() && !text.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    if let Some(nl) = text[tail_start..].find('\n')
        && nl <= 512
        && tail_start + nl + 1 < text.len()
    {
        tail_start += nl + 1;
    }

    if head_end >= tail_start {
        return text;
    }
    let elided = &text[head_end..tail_start];
    let elided_lines = elided.lines().count();
    let elided_bytes = elided.len();
    format!(
        "{}\n\n... [elided {elided_lines} lines ({elided_bytes} bytes) from middle of output; head and tail preserved] ...\n\n{}",
        &text[..head_end],
        &text[tail_start..]
    )
}

fn parse_curl_status_and_body(raw: &str) -> Result<(u16, String), String> {
    let trimmed = raw.trim_end();
    let Some(idx) = trimmed.rfind('\n') else {
        let code = trimmed.trim().parse::<u16>().unwrap_or(0);
        return Ok((code, String::new()));
    };
    let body = trimmed[..idx].to_string();
    let code_str = trimmed[idx + 1..].trim();
    let code = code_str
        .parse::<u16>()
        .map_err(|e| format!("invalid HTTP status `{code_str}`: {e}"))?;
    Ok((code, body))
}

pub(super) fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub(super) fn base64_decode(input: &str) -> Vec<u8> {
    fn val(b: u8) -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u8;
    for &b in input.as_bytes() {
        if b == b'=' {
            break;
        }
        if let Some(v) = val(b) {
            buf = (buf << 6) | (v as u32);
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push(((buf >> bits) & 0xff) as u8);
            }
        }
    }
    out
}
