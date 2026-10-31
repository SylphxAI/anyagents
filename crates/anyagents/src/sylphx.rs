//! Native `sylphx` harness (`Sylphx Agent`) and RFC 8628 Device Flow sign-in.
//!
//! Supports:
//! 1. `anyagents sign-in sylphx` (and `anyagents sign-in <agent>`) via RFC 8628
//!    Device Authorization against `https://api.sylphx.com/v1/access/device/authorize`
//!    and `/v1/access/device/token`, saving the issued org-scoped key to
//!    `.config/sylphx/credentials.json`, `.config/opencode/sylphx.key`, and
//!    `.config/desk/sylphx-ai/key`.
//! 2. `SylphxHarness` (`--harness sylphx`), our own native harness that executes
//!    both root agents and spawned sub-agents directly without third-party CLIs:
//!    - Cloud Session mode against `https://api.sylphx.com/v1/{env}/agent_sessions`
//!      (`:send`, `:steer`, `:interrupt`, `/session_events`) when `SYLPHX_AGENT_ENV`
//!      is set.
//!    - Native Sylphx Agent Runtime multi-step tool loop against Sylphx AI
//!      (`https://api.sylphx.com/v1/responses`) with direct execution
//!      of all AnyAgents coordination tools (`agents`, `send`, `conversation`,
//!      `groups`, `schedule`, `browser`, `describe`, `archive`) and workspace tools
//!      (`bash`, `read_file`, `write_file`, `edit_file`), plus immediate mid-turn
//!      steering between tool steps.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnState, TurnUsage, marked_message,
};
use anyagents_installer::{InstallError, LineSink};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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
                let kn = v.get("key_name").and_then(Value::as_str).map(str::to_string);
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
/// `.config/opencode/sylphx.key`, and `.config/desk/sylphx-ai/key` under `base_dir`.
///
/// Preserves any existing project-environment gateway key (`sk-sx-...`) in
/// `.config/opencode/sylphx.key` and `.config/desk/sylphx-ai/key` when `creds.api_key`
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
        for rel in [".config/opencode/sylphx.key", ".config/desk/sylphx-ai/key"] {
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

    let doc = json!({
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

    let _ = crate::opencode_config::write(config_dir, Some(resolved_model), mcp_command, mcp_args);
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
/// prioritizing project-environment keys (`sk-sx-...` in `.config/desk/sylphx-ai/key` or
/// `.config/opencode/sylphx.key`) before falling back to `.config/sylphx/credentials.json`.
pub fn resolve_sylphx_gateway_key(config_dir: &Path, env: &[(String, String)]) -> Option<String> {
    for (k, v) in env {
        if (k == "SYLPHX_AI_KEY" || k == "SYLPHX_API_KEY") && !v.trim().is_empty() {
            return Some(v.trim().to_string());
        }
    }
    for k in ["SYLPHX_AI_KEY", "SYLPHX_API_KEY"] {
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
        ".config/desk/sylphx-ai/key",
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
            if let Some(k) = orgs
                .iter()
                .find_map(|o| {
                    o.get("api_key")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                })
            {
                return Some(k.to_string());
            }
        }
    }
    for rel in [
        ".config/desk/sylphx-ai/key",
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

#[derive(Default)]
struct SharedState {
    in_turn: bool,
    turns_served: u32,
    served_usage: TurnUsage,
    session_id: Option<String>,
    cloud_session_id: Option<String>,
    cloud_agent_id: Option<String>,
    dead_reason: Option<String>,
    sign_in_fault: Option<String>,
    quota_reset_at_ms: Option<i64>,
    last_activity: Option<Instant>,
    steer_queue: VecDeque<String>,
    notify: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// First-class AnyAgents harness for `Sylphx Agent` (`--harness sylphx`).
pub struct SylphxHarness {
    config_dir: PathBuf,
    work_dir: PathBuf,
    model: Option<String>,
    env: Vec<(String, String)>,
    mcp_command: PathBuf,
    mcp_args: Vec<String>,
    fake_mode: bool,
    fake_log: Option<PathBuf>,
    cancel_flag: Arc<AtomicBool>,
    state: Arc<Mutex<SharedState>>,
}

impl SylphxHarness {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        config_dir: &Path,
        work_dir: &Path,
        model: Option<&str>,
        session: Option<&str>,
        env: &[(&str, &str)],
        mcp_command: &Path,
        mcp_args: &[String],
    ) -> Result<Self, HarnessError> {
        let env_owned: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let fake_mode = env_owned
            .iter()
            .any(|(k, v)| k == "ANYAGENTS_FAKE_MODE" && !v.is_empty())
            || std::env::var_os("ANYAGENTS_FAKE_MODE").is_some();
        let fake_log = env_owned
            .iter()
            .find(|(k, _)| k == "ANYAGENTS_FAKE_LOG")
            .map(|(_, v)| PathBuf::from(v))
            .or_else(|| std::env::var_os("ANYAGENTS_FAKE_LOG").map(PathBuf::from));

        let initial_session = session
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                std::fs::read_to_string(config_dir.join("sylphx-session-id"))
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_else(|| format!("sylphx_ses_{}", anyagents_core::now_ms()));

        let _ = std::fs::write(
            config_dir.join("sylphx-session-id"),
            format!("{initial_session}\n"),
        );

        let state = Arc::new(Mutex::new(SharedState {
            session_id: Some(initial_session),
            ..SharedState::default()
        }));

        Ok(Self {
            config_dir: config_dir.to_path_buf(),
            work_dir: work_dir.to_path_buf(),
            model: model.map(str::to_string),
            env: env_owned,
            mcp_command: mcp_command.to_path_buf(),
            mcp_args: mcp_args.to_vec(),
            fake_mode,
            fake_log,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            state,
        })
    }

    pub fn session_id(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .session_id
            .clone()
    }

    #[cfg(test)]
    pub fn declared_capabilities() -> (DeliveryMode, SteerSupport) {
        (DeliveryMode::Steer, SteerSupport::Yes)
    }

    fn cloud_environment(&self) -> Option<String> {
        for (k, v) in &self.env {
            if k == "SYLPHX_AGENT_ENV" && !v.trim().is_empty() {
                return Some(v.trim().to_string());
            }
        }
        if let Ok(v) = std::env::var("SYLPHX_AGENT_ENV")
            && !v.trim().is_empty()
        {
            return Some(v.trim().to_string());
        }
        let ctx_path = self.config_dir.join(".config/sylphx/context.json");
        if let Ok(raw) = std::fs::read_to_string(ctx_path)
            && let Ok(val) = serde_json::from_str::<Value>(&raw)
            && val.get("cloud_session").and_then(Value::as_bool) == Some(true)
            && let Some(env_slug) = val
                .get("environment")
                .or_else(|| val.get("env"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            return Some(env_slug.to_string());
        }
        None
    }
}

impl Harness for SylphxHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let marked = marked_message(&message.id, &message.text);
        if let Some(ref log_path) = self.fake_log {
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)
                .and_then(|mut f| writeln!(f, "sylphx deliver {}: {}", message.id, message.text));
        }

        {
            let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(ref reason) = guard.dead_reason {
                return Err(HarnessError::Failed(reason.clone()));
            }
            if let Some(reset_ms) = guard.quota_reset_at_ms {
                return Ok(DeliverOutcome::Quota {
                    reset_at_ms: reset_ms,
                });
            }
            if guard.in_turn {
                guard.steer_queue.push_back(marked.clone());
                guard.last_activity = Some(Instant::now());
                let cloud_sess = guard.cloud_session_id.clone();
                let notify = guard.notify.clone();
                drop(guard);

                // If bound to a remote Sylphx Cloud AgentSession, forward the `:steer` call immediately.
                if let (Some(env_slug), Some(sess_id)) = (self.cloud_environment(), cloud_sess)
                    && let Some(api_key) = resolve_sylphx_api_key(&self.config_dir, &self.env)
                {
                    let api_url = std::env::var("SYLPHX_API_URL")
                        .ok()
                        .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string());
                    let steer_url = format!(
                        "{}/v1/{}/agent_sessions/{}:steer",
                        api_url.trim_end_matches('/'),
                        env_slug,
                        sess_id
                    );
                    let _ = curl_post_json(
                        &steer_url,
                        Some(&api_key),
                        &json!({ "content": marked }),
                    );
                }
                if let Some(cb) = notify {
                    cb();
                }
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                });
            }
            guard.in_turn = true;
            guard.last_activity = Some(Instant::now());
        }

        self.cancel_flag.store(false, Ordering::SeqCst);

        if self.fake_mode {
            let state = Arc::clone(&self.state);
            let cancel_flag = Arc::clone(&self.cancel_flag);
            let text_owned = message.text.clone();
            std::thread::spawn(move || {
                if text_owned.contains("sleep:") {
                    for _ in 0..20 {
                        if cancel_flag.load(Ordering::SeqCst) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(25));
                    }
                }
                let notify = {
                    let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                    g.steer_queue.clear();
                    g.in_turn = false;
                    g.turns_served = g.turns_served.saturating_add(1);
                    g.served_usage.input_tokens = g.served_usage.input_tokens.saturating_add(64);
                    g.served_usage.output_tokens = g.served_usage.output_tokens.saturating_add(32);
                    g.last_activity = Some(Instant::now());
                    g.notify.clone()
                };
                if let Some(cb) = notify {
                    cb();
                }
            });
            return Ok(DeliverOutcome::Accepted {
                turn_finished: false,
            });
        }

        let state = Arc::clone(&self.state);
        let cancel_flag = Arc::clone(&self.cancel_flag);
        let config_dir = self.config_dir.clone();
        let work_dir = self.work_dir.clone();
        let model = self.model.clone();
        let env = self.env.clone();
        let mcp_command = self.mcp_command.clone();
        let mcp_args = self.mcp_args.clone();
        let cloud_env = self.cloud_environment();

        std::thread::spawn(move || {
            let outcome = if let Some(env_slug) = cloud_env {
                run_cloud_session_turn(
                    &state,
                    &cancel_flag,
                    &config_dir,
                    &work_dir,
                    model.as_deref(),
                    &env,
                    &mcp_command,
                    &mcp_args,
                    &env_slug,
                    &marked,
                )
            } else {
                run_native_gateway_turn(
                    &state,
                    &cancel_flag,
                    &config_dir,
                    &work_dir,
                    model.as_deref(),
                    &env,
                    &mcp_command,
                    &mcp_args,
                    &marked,
                )
            };

            let notify = {
                let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
                g.in_turn = false;
                g.last_activity = Some(Instant::now());
                match outcome {
                    Ok(usage) => {
                        g.turns_served = g.turns_served.saturating_add(1);
                        g.served_usage.input_tokens =
                            g.served_usage.input_tokens.saturating_add(usage.input_tokens);
                        g.served_usage.output_tokens = g
                            .served_usage
                            .output_tokens
                            .saturating_add(usage.output_tokens);
                        let _ = std::fs::remove_file(config_dir.join(".anyagents-last-error.json"));
                    }
                    Err(err_msg) => {
                        let lower = err_msg.to_ascii_lowercase();
                        if lower.contains("insufficient_quota")
                            || lower.contains("quota exceeded")
                            || lower.contains("credit balance is too low")
                            || lower.contains("out of credits")
                        {
                            g.quota_reset_at_ms = Some(anyagents_core::now_ms() + 300_000);
                        } else if lower.contains("missing sylphx api key")
                            || lower.contains("unauthorized")
                            || lower.contains("invalid api key")
                        {
                            g.sign_in_fault = Some(err_msg.clone());
                        }
                        persist_turn_error(&config_dir, &err_msg);
                    }
                }
                g.notify.clone()
            };
            if let Some(cb) = notify {
                cb();
            }
        });

        Ok(DeliverOutcome::Accepted {
            turn_finished: false,
        })
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let trimmed = session.trim();
        if !trimmed.is_empty() {
            let mut g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            g.session_id = Some(trimmed.to_string());
            let _ = std::fs::write(
                self.config_dir.join("sylphx-session-id"),
                format!("{trimmed}\n"),
            );
        }
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.cancel_flag.store(true, Ordering::SeqCst);
        let (cloud_sess, notify) = {
            let mut g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            g.steer_queue.clear();
            g.in_turn = false;
            (g.cloud_session_id.clone(), g.notify.clone())
        };
        if let (Some(env_slug), Some(sess_id)) = (self.cloud_environment(), cloud_sess)
            && let Some(api_key) = resolve_sylphx_api_key(&self.config_dir, &self.env)
        {
            let api_url = std::env::var("SYLPHX_API_URL")
                .ok()
                .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string());
            let interrupt_url = format!(
                "{}/v1/{}/agent_sessions/{}:interrupt",
                api_url.trim_end_matches('/'),
                env_slug,
                sess_id
            );
            let _ = curl_post_json(&interrupt_url, Some(&api_key), &json!({}));
        }
        if let Some(cb) = notify {
            cb();
        }
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        HarnessHealth {
            process_alive: g.dead_reason.is_none(),
            turn_state: if g.in_turn {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: g.quota_reset_at_ms,
            sign_in_fault: g.sign_in_fault.clone(),
        }
    }

    fn input_ready(&self) -> bool {
        true
    }

    fn served_usage(&self) -> TurnUsage {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .served_usage
            .clone()
    }

    fn turns_served(&self) -> u32 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .turns_served
    }

    fn quiet_for(&self, _id: &str, quiet: Duration, _patience: Duration) -> bool {
        let g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if g.in_turn {
            return false;
        }
        g.last_activity
            .map(|t| t.elapsed() >= quiet)
            .unwrap_or(true)
    }

    fn ends_on_quiet(&self) -> Option<Duration> {
        None
    }

    fn clear_quota(&mut self) {
        let mut g = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        g.quota_reset_at_ms = None;
        g.sign_in_fault = None;
    }

    fn process_id(&self) -> Option<u32> {
        Some(std::process::id())
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notify = Some(notify);
    }
}

fn persist_turn_error(config_dir: &Path, err_msg: &str) {
    let err_path = config_dir.join(".anyagents-last-error.json");
    let prev_count = std::fs::read_to_string(&err_path)
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .and_then(|v| v.get("consecutive_turn_errors").and_then(Value::as_u64))
        .unwrap_or(0);
    let doc = json!({
        "error": err_msg,
        "timestamp_ms": anyagents_core::now_ms(),
        "consecutive_turn_errors": prev_count + 1,
    });
    let _ = std::fs::write(&err_path, format!("{doc}\n"));
}

/// Runs a turn against the remote Sylphx Cloud Agents service (`/v1/{env}/agent_sessions`).
#[allow(clippy::too_many_arguments)]
fn run_cloud_session_turn(
    state: &Arc<Mutex<SharedState>>,
    cancel_flag: &Arc<AtomicBool>,
    config_dir: &Path,
    work_dir: &Path,
    model: Option<&str>,
    env: &[(String, String)],
    mcp_command: &Path,
    mcp_args: &[String],
    env_slug: &str,
    user_message: &str,
) -> Result<TurnUsage, String> {
    let api_key = resolve_sylphx_api_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx API key; run `anyagents sign-in sylphx` first".to_string()
    })?;
    let api_url = std::env::var("SYLPHX_API_URL")
        .ok()
        .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string());
    let base = api_url.trim_end_matches('/');

    let (mut agent_id, mut session_id) = {
        let g = state.lock().unwrap_or_else(PoisonError::into_inner);
        (g.cloud_agent_id.clone(), g.cloud_session_id.clone())
    };

    let (model_id, _) = parse_sylphx_model_and_effort(model);
    let system_prompt = assemble_system_prompt(config_dir, work_dir);

    if agent_id.is_none() {
        let create_agent_url = format!("{base}/v1/{env_slug}/agents");
        let body = json!({
            "name": config_dir
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("anyagents-sylphx"),
            "model": model_id,
            "system_prompt": system_prompt,
            "tools": build_tool_definitions(),
        });
        let (st, raw) = curl_post_json(&create_agent_url, Some(&api_key), &body)?;
        if !(200..300).contains(&st) {
            return Err(format!("cloud create_agent HTTP {st}: {}", raw.trim()));
        }
        let val: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        let id = val
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| "cloud create_agent missing id".to_string())?
            .to_string();
        agent_id = Some(id.clone());
        state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cloud_agent_id = Some(id);
    }

    if session_id.is_none() {
        let create_sess_url = format!("{base}/v1/{env_slug}/agent_sessions");
        let body = json!({
            "agent_id": agent_id.as_deref().unwrap_or_default(),
            "title": "AnyAgents Session",
        });
        let (st, raw) = curl_post_json(&create_sess_url, Some(&api_key), &body)?;
        if !(200..300).contains(&st) {
            return Err(format!("cloud create_session HTTP {st}: {}", raw.trim()));
        }
        let val: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        let sid = val
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| "cloud create_session missing id".to_string())?
            .to_string();
        session_id = Some(sid.clone());
        let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
        g.cloud_session_id = Some(sid.clone());
        g.session_id = Some(sid.clone());
        let _ = std::fs::write(config_dir.join("sylphx-session-id"), format!("{sid}\n"));
    }

    let sid = session_id.unwrap_or_default();
    let send_url = format!("{base}/v1/{env_slug}/agent_sessions/{sid}:send");
    let (st, raw) = curl_post_json(&send_url, Some(&api_key), &json!({ "content": user_message }))?;
    if !(200..300).contains(&st) {
        return Err(format!("cloud session send HTTP {st}: {}", raw.trim()));
    }

    let sess_url = format!("{base}/v1/{env_slug}/agent_sessions/{sid}");
    while !cancel_flag.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(250));
        let (st_poll, raw_poll) = curl_get(&sess_url, Some(&api_key))?;
        if (200..300).contains(&st_poll)
            && let Ok(val) = serde_json::from_str::<Value>(&raw_poll)
        {
            let status = val.get("status").and_then(Value::as_str).unwrap_or("");
            if status == "idle" || status == "completed" {
                let in_tok = val
                    .get("total_input_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let out_tok = val
                    .get("total_output_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                return Ok(TurnUsage {
                    input_tokens: in_tok,
                    output_tokens: out_tok,
                    ..TurnUsage::default()
                });
            }
            if status == "failed" || status == "terminated" || status == "errored" {
                return Err(format!("cloud session ended with status `{status}`"));
            }
        }
        let _ = (mcp_command, mcp_args);
    }
    Ok(TurnUsage::default())
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

/// Runs our native multi-step Sylphx Agent Runtime loop against Sylphx AI's
/// canonical Responses API (`https://api.sylphx.com/v1/responses`), executing
/// AnyAgents MCP tools (`agents`, `send`, `conversation`, `groups`, `schedule`,
/// `browser`, `describe`, `archive`) and workspace tools (`bash`, `read_file`,
/// `write_file`, `edit_file`) directly, and draining `steer_queue` between every
/// step so mid-turn steering takes effect immediately on the next step.
#[allow(clippy::too_many_arguments)]
fn run_native_gateway_turn(
    state: &Arc<Mutex<SharedState>>,
    cancel_flag: &Arc<AtomicBool>,
    config_dir: &Path,
    work_dir: &Path,
    model: Option<&str>,
    env: &[(String, String)],
    mcp_command: &Path,
    mcp_args: &[String],
    initial_user_message: &str,
) -> Result<TurnUsage, String> {
    let api_key = resolve_sylphx_gateway_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx API key; run `anyagents sign-in sylphx` first".to_string()
    })?;
    let gateway_base = env
        .iter()
        .find(|(k, _)| k == "SYLPHX_GATEWAY_URL")
        .map(|(_, v)| normalize_sylphx_gateway_url(v))
        .or_else(|| {
            std::env::var("SYLPHX_GATEWAY_URL")
                .ok()
                .map(|s| normalize_sylphx_gateway_url(&s))
        })
        .unwrap_or_else(|| DEFAULT_SYLPHX_GATEWAY_URL.to_string());
    let responses_url = format!("{gateway_base}/responses");

    let (model_id, reasoning_effort) = parse_sylphx_model_and_effort(model);
    let system_prompt = assemble_system_prompt(config_dir, work_dir);
    let transcript_path = config_dir.join("sylphx-transcript.jsonl");

    let mut input_items = load_transcript_input_items(&transcript_path);
    let user_msg = json!({
        "role": "user",
        "content": initial_user_message,
    });
    append_transcript_message(&transcript_path, &user_msg);
    input_items.push(user_msg);

    let tools = build_responses_tool_definitions();
    let mut total_in: u64 = 0;
    let mut total_out: u64 = 0;
    let mut sent_any_reply = false;

    for _step in 0..48 {
        if cancel_flag.load(Ordering::SeqCst) {
            break;
        }
        drain_steered_messages(state, &transcript_path, &mut input_items);

        let mut req_body = json!({
            "model": model_id,
            "instructions": system_prompt,
            "input": input_items,
            "tools": tools,
        });
        if let Some(ref effort) = reasoning_effort
            && let Some(obj) = req_body.as_object_mut()
        {
            obj.insert("reasoning".to_string(), json!({ "effort": effort }));
        }

        let (status, raw) = curl_post_json(&responses_url, Some(&api_key), &req_body)?;
        if !(200..300).contains(&status) {
            return Err(format!("Sylphx responses API HTTP {status}: {}", raw.trim()));
        }
        let resp: Value = serde_json::from_str(&raw)
            .map_err(|e| format!("invalid Sylphx responses API JSON: {e}"))?;

        if let Some(usage) = resp.get("usage") {
            total_in = total_in.saturating_add(
                usage
                    .get("input_tokens")
                    .or_else(|| usage.get("prompt_tokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
            total_out = total_out.saturating_add(
                usage
                    .get("output_tokens")
                    .or_else(|| usage.get("completion_tokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
        }

        if let Some(cb) = state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notify
            .clone()
        {
            cb();
        }

        let Some(output_arr) = resp.get("output").and_then(Value::as_array).cloned() else {
            return Err("Sylphx responses API missing `output` array".to_string());
        };

        let mut function_calls = Vec::new();
        let mut assistant_texts = Vec::new();

        for item in output_arr {
            append_transcript_message(&transcript_path, &item);
            input_items.push(item.clone());
            match item.get("type").and_then(Value::as_str) {
                Some("function_call") => {
                    function_calls.push(item);
                }
                Some("message") => {
                    if let Some(parts) = item.get("content").and_then(Value::as_array) {
                        for p in parts {
                            if let Some(t) = p
                                .get("text")
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                            {
                                assistant_texts.push(t.to_string());
                            }
                        }
                    } else if let Some(t) = item
                        .get("content")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        assistant_texts.push(t.to_string());
                    }
                }
                _ => {}
            }
        }

        if !function_calls.is_empty() {
            let mut self_archived = false;
            for fc in function_calls {
                if cancel_flag.load(Ordering::SeqCst) {
                    break;
                }
                let call_id = fc
                    .get("call_id")
                    .or_else(|| fc.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or("call_0")
                    .to_string();
                let fn_name = fc
                    .get("name")
                    .or_else(|| fc.pointer("/function/name"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let raw_args = fc
                    .get("arguments")
                    .or_else(|| fc.pointer("/function/arguments"))
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                let parsed_args: Value =
                    serde_json::from_str(raw_args).unwrap_or_else(|_| json!({}));

                let norm_fn = fn_name.strip_prefix("mcp__anyagents__").unwrap_or(&fn_name);
                if norm_fn == "send" {
                    sent_any_reply = true;
                }
                if (norm_fn == "archive"
                    && parsed_args
                        .get("agent")
                        .or_else(|| parsed_args.get("name"))
                        .and_then(Value::as_str)
                        .is_none_or(|s| s.trim().is_empty() || s.eq_ignore_ascii_case("self")))
                    || ((norm_fn == "agents" || norm_fn == "subagents")
                        && parsed_args
                            .get("action")
                            .and_then(Value::as_str)
                            .is_some_and(|a| a.eq_ignore_ascii_case("archive"))
                        && parsed_args
                            .get("name")
                            .or_else(|| parsed_args.get("agent"))
                            .and_then(Value::as_str)
                            .is_none_or(|s| s.trim().is_empty() || s.eq_ignore_ascii_case("self")))
                {
                    self_archived = true;
                }

                let output = execute_sylphx_tool(
                    &fn_name,
                    &parsed_args,
                    work_dir,
                    env,
                    mcp_command,
                    mcp_args,
                );
                let tool_out_item = json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": output,
                });
                append_transcript_message(&transcript_path, &tool_out_item);
                input_items.push(tool_out_item);
            }
            if self_archived {
                break;
            }
            continue;
        }

        if drain_steered_messages(state, &transcript_path, &mut input_items) {
            continue;
        }

        let content_text = assistant_texts.join("\n\n");
        if !sent_any_reply
            && !content_text.trim().is_empty()
            && let Some(reply_to) = extract_envelope_sender(initial_user_message)
        {
            let _ = call_anyagents_mcp_tool(
                mcp_command,
                mcp_args,
                env,
                "send",
                &json!({
                    "to": reply_to,
                    "text": content_text.trim(),
                }),
            );
        }
        break;
    }

    Ok(TurnUsage {
        input_tokens: total_in,
        output_tokens: total_out,
        ..TurnUsage::default()
    })
}

/// Runs `SylphxHarness` as a stdio stream adapter (`anyagents-harness harness <config>`)
/// when launched by an older `anyagents serve` process via `<config_dir>/adapter.json`.
pub fn run_stdio_adapter(
    config_path: &Path,
    config_val: &Value,
) -> Result<(), anyagents_core::Error> {
    let config_dir = config_val
        .get("config_dir")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .or_else(|| config_path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));
    let work_dir = std::env::current_dir().unwrap_or_else(|_| config_dir.clone());
    let model = config_val
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .and_then(|v| v.get("model").and_then(Value::as_str).map(str::to_string))
        });
    let mcp_command = config_val
        .get("mcp_command")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .or_else(|| {
            std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .and_then(|v| {
                    v.pointer("/mcpServers/anyagents/command")
                        .and_then(Value::as_str)
                        .map(PathBuf::from)
                })
        })
        .unwrap_or_else(|| {
            std::env::current_exe().unwrap_or_else(|_| PathBuf::from("anyagents"))
        });
    let mcp_args: Vec<String> = config_val
        .get("mcp_args")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .or_else(|| {
            std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .and_then(|v| {
                    v.pointer("/mcpServers/anyagents/args")
                        .and_then(Value::as_array)
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|x| x.as_str().map(str::to_string))
                                .collect()
                        })
                })
        })
        .unwrap_or_default();

    let env: Vec<(String, String)> = std::env::vars().collect();
    let session_id = std::fs::read_to_string(config_dir.join("sylphx-session-id"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("sylphx_ses_{}", anyagents_core::now_ms()));
    let _ = std::fs::write(
        config_dir.join("sylphx-session-id"),
        format!("{session_id}\n"),
    );

    let state = Arc::new(Mutex::new(SharedState {
        session_id: Some(session_id.clone()),
        ..SharedState::default()
    }));
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let steered_in_turn = Arc::new(Mutex::new(0usize));

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else {
            break;
        };
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let kind = msg.get("type").and_then(Value::as_str).unwrap_or("");
        if kind == "cancel" || kind == "interrupt" {
            cancel_flag.store(true, Ordering::SeqCst);
            let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
            g.steer_queue.clear();
            g.in_turn = false;
            continue;
        }
        if kind != "user" && kind != "prompt" {
            continue;
        }
        let user_text = msg
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if user_text.trim().is_empty() {
            continue;
        }

        let already_in_turn = {
            let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
            if g.in_turn {
                g.steer_queue.push_back(user_text.clone());
                g.last_activity = Some(Instant::now());
                true
            } else {
                g.in_turn = true;
                g.last_activity = Some(Instant::now());
                false
            }
        };

        if already_in_turn {
            let mut sc = steered_in_turn.lock().unwrap_or_else(PoisonError::into_inner);
            *sc += 1;
            continue;
        }

        cancel_flag.store(false, Ordering::SeqCst);
        *steered_in_turn.lock().unwrap_or_else(PoisonError::into_inner) = 0;

        let state_cl = Arc::clone(&state);
        let cancel_cl = Arc::clone(&cancel_flag);
        let steered_cl = Arc::clone(&steered_in_turn);
        let cfg_cl = config_dir.clone();
        let work_cl = work_dir.clone();
        let model_cl = model.clone();
        let env_cl = env.clone();
        let mcp_cmd_cl = mcp_command.clone();
        let mcp_args_cl = mcp_args.clone();
        let sess_cl = session_id.clone();

        std::thread::spawn(move || {
            let cloud_env = env_cl
                .iter()
                .find(|(k, v)| k == "SYLPHX_AGENT_ENV" && !v.trim().is_empty())
                .map(|(_, v)| v.trim().to_string());
            let outcome = if let Some(env_slug) = cloud_env {
                run_cloud_session_turn(
                    &state_cl,
                    &cancel_cl,
                    &cfg_cl,
                    &work_cl,
                    model_cl.as_deref(),
                    &env_cl,
                    &mcp_cmd_cl,
                    &mcp_args_cl,
                    &env_slug,
                    &user_text,
                )
            } else {
                run_native_gateway_turn(
                    &state_cl,
                    &cancel_cl,
                    &cfg_cl,
                    &work_cl,
                    model_cl.as_deref(),
                    &env_cl,
                    &mcp_cmd_cl,
                    &mcp_args_cl,
                    &user_text,
                )
            };

            let extra_followups = {
                let mut g = state_cl.lock().unwrap_or_else(PoisonError::into_inner);
                g.in_turn = false;
                g.last_activity = Some(Instant::now());
                let mut sc = steered_cl.lock().unwrap_or_else(PoisonError::into_inner);
                let count = *sc;
                *sc = 0;
                count
            };

            let mut out = std::io::stdout().lock();
            match outcome {
                Ok(usage) => {
                    let _ = std::fs::remove_file(cfg_cl.join(".anyagents-last-error.json"));
                    let done_evt = json!({
                        "type": "done",
                        "session_id": sess_cl,
                        "usage": {
                            "input_tokens": usage.input_tokens,
                            "output_tokens": usage.output_tokens,
                        }
                    });
                    let _ = writeln!(out, "{done_evt}");
                    if extra_followups > 0 {
                        let followup_done = json!({
                            "type": "done",
                            "session_id": sess_cl,
                            "usage": {
                                "input_tokens": 0,
                                "output_tokens": 0,
                            }
                        });
                        let _ = writeln!(out, "{followup_done}");
                    }
                    let _ = out.flush();
                }
                Err(err_msg) => {
                    persist_turn_error(&cfg_cl, &err_msg);
                    let err_evt = json!({
                        "type": "done",
                        "session_id": sess_cl,
                        "error": err_msg,
                        "usage": {
                            "input_tokens": 0,
                            "output_tokens": 0,
                        }
                    });
                    let _ = writeln!(out, "{err_evt}");
                    if extra_followups > 0 {
                        let _ = writeln!(out, "{err_evt}");
                    }
                    let _ = out.flush();
                }
            }
        });
    }

    for _ in 0..2400 {
        let busy = state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .in_turn;
        if !busy {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn drain_steered_messages(
    state: &Arc<Mutex<SharedState>>,
    transcript_path: &Path,
    messages: &mut Vec<Value>,
) -> bool {
    let mut g = state.lock().unwrap_or_else(PoisonError::into_inner);
    if g.steer_queue.is_empty() {
        return false;
    }
    while let Some(steered) = g.steer_queue.pop_front() {
        let msg = json!({
            "role": "user",
            "content": steered,
        });
        append_transcript_message(transcript_path, &msg);
        messages.push(msg);
    }
    g.last_activity = Some(Instant::now());
    true
}

fn extract_envelope_sender(text: &str) -> Option<String> {
    let idx = text.find("from=\"")?;
    let rest = &text[idx + 6..];
    let end = rest.find('"')?;
    let sender = rest[..end].trim();
    (!sender.is_empty()).then(|| sender.to_string())
}

fn assemble_system_prompt(config_dir: &Path, work_dir: &Path) -> String {
    let mut parts = Vec::new();
    if let Ok(sys) = std::fs::read_to_string(config_dir.join("SYSTEM_PROMPT.md"))
        && !sys.trim().is_empty()
    {
        parts.push(sys);
    } else if let Ok(agents_md) = std::fs::read_to_string(work_dir.join("AGENTS.md"))
        && !agents_md.trim().is_empty()
    {
        parts.push(agents_md);
    }
    if parts.is_empty() {
        "You are a Sylphx Agent running inside AnyAgents. Always use the `send` tool to reply to the sender of an incoming `<message>`.".to_string()
    } else {
        parts.join("\n\n")
    }
}

/// Loads persisted transcript items for `/v1/responses` (`input`), automatically
/// converting any legacy Chat Completions entries (`tool_calls` / `role: "tool"`)
/// into Responses API items (`function_call` / `function_call_output`).
fn load_transcript_input_items(transcript_path: &Path) -> Vec<Value> {
    let Ok(file) = std::fs::File::open(transcript_path) else {
        return Vec::new();
    };
    let mut history = Vec::new();
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(m) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if m.get("type").is_some() {
            history.push(m);
            continue;
        }
        match m.get("role").and_then(Value::as_str) {
            Some("user") => {
                history.push(m);
            }
            Some("assistant") => {
                if let Some(s) = m
                    .get("content")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    history.push(json!({
                        "role": "assistant",
                        "content": s,
                    }));
                }
                if let Some(tcs) = m.get("tool_calls").and_then(Value::as_array) {
                    for tc in tcs {
                        if let (Some(cid), Some(fname)) = (
                            tc.get("id").and_then(Value::as_str),
                            tc.pointer("/function/name").and_then(Value::as_str),
                        ) {
                            let fargs = tc
                                .pointer("/function/arguments")
                                .and_then(Value::as_str)
                                .unwrap_or("{}");
                            history.push(json!({
                                "type": "function_call",
                                "call_id": cid,
                                "name": fname,
                                "arguments": fargs,
                            }));
                        }
                    }
                }
            }
            Some("tool") => {
                if let Some(cid) = m.get("tool_call_id").and_then(Value::as_str) {
                    let out_str = m.get("content").and_then(Value::as_str).unwrap_or("");
                    history.push(json!({
                        "type": "function_call_output",
                        "call_id": cid,
                        "output": out_str,
                    }));
                }
            }
            _ => {}
        }
    }
    if history.len() > 80 {
        let split = history.len() - 80;
        history.drain(0..split);
        while history.first().is_some_and(|item| {
            item.get("type").and_then(Value::as_str) == Some("function_call_output")
                || item.get("type").and_then(Value::as_str) == Some("reasoning")
        }) {
            history.remove(0);
        }
    }
    history
}

fn append_transcript_message(transcript_path: &Path, msg: &Value) {
    if let Ok(line) = serde_json::to_string(msg)
        && let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(transcript_path)
    {
        let _ = writeln!(f, "{line}");
    }
}

/// Converts `build_tool_definitions()` into the flat `/v1/responses` function tool schema
/// (`{"type": "function", "name": ..., "description": ..., "parameters": ...}`).
fn build_responses_tool_definitions() -> Vec<Value> {
    build_tool_definitions()
        .into_iter()
        .filter_map(|t| {
            let func = t.get("function")?;
            Some(json!({
                "type": "function",
                "name": func.get("name")?,
                "description": func.get("description").cloned().unwrap_or(Value::String(String::new())),
                "parameters": func.get("parameters").cloned().unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            }))
        })
        .collect()
}

fn build_tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "type": "function",
            "function": {
                "name": "send",
                "description": "Send a message, media attachment, location, poll, checklist, or article, or perform an interaction (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`) to one or more agents, groups, or channels.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "to": {
                            "description": "Recipient(s): a string or array of strings (`\"parent\"` or `[\"parent\"]`, `agent:<name>`, bare name, `children`, `group:<name>`, or `telegram:chat:<id>[:<topic>]`)."
                        },
                        "text": { "type": "string", "description": "Message text or caption." },
                        "reply_to": { "type": "string", "description": "Optional reply reference (`anyagents:reply:msg_...`)." },
                        "action": { "type": "string", "description": "Optional interaction action (`react`, `vote`, `edit`, `delete`, `pin`, `unpin`, `stop_poll`)." },
                        "target": { "type": "string", "description": "Target message for interaction actions." },
                        "emoji": { "type": "string", "description": "Emoji for `react`." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "describe",
                "description": "Read or update an agent's role & responsibility description (`agent`: `\"self\"` | `\"all\"` | `\"<agent>\"`, optional `description`, `role`, `clear`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string", "description": "Target agent (`\"self\"`, `\"all\"`, or agent name). Defaults to `\"self\"`." },
                        "description": { "type": "string", "description": "Updated role/responsibility description." },
                        "role": { "type": "string", "description": "Optional short role label." },
                        "clear": { "type": "boolean", "description": "Clear existing description." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "archive",
                "description": "Archive an agent or sub-agent (`agent`: `\"self\"` or `<agent-name>`) to scale its process to zero while preserving its permanent `agt_...` ID and session.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string", "description": "Target agent (`\"self\"` or sub-agent name). Defaults to `\"self\"`." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "agents",
                "description": "List, inspect, spawn, configure, wake, archive, pause, or delete agents and sub-agents (`list`, `inspect`, `get`, `spawn`, `create`, `describe`, `steer`, `wake`, `archive`, `pause`, `delete`, `rename`, `role`, `harness`, `models`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`list`, `inspect`, `get`, `spawn`, `create`, `describe`, `steer`, `wake`, `archive`, `pause`, `delete`, `rename`, `role`, `harness`, or `models`." },
                        "name": { "type": "string", "description": "Target agent or sub-agent name." },
                        "agent": { "type": "string", "description": "Alias for `name`." },
                        "parent": { "type": "string", "description": "Parent filter (`self`, `subagents`, `root`, or agent name)." },
                        "state": { "type": "string", "description": "State filter (`running`, `working`, `paused`, `archived`, `all`)." },
                        "role": { "type": "string", "description": "Short 2-5 word role title when spawning." },
                        "harness": { "type": "string", "description": "Harness (`sylphx`, `codex`, `claude-code`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`, or `inherit`)." },
                        "model": { "type": "string", "description": "Model ID." },
                        "profile": { "type": "string", "description": "Provider profile (`<provider>/<profile>`)." },
                        "repo": { "type": "string", "description": "Repository (`<org>/<repo>`, local path, or `inherit`)." },
                        "branch": { "type": "string" },
                        "base": { "type": "string" },
                        "workdir": { "type": "string" },
                        "description": { "type": "string", "description": "Role/responsibility description or standing instructions." },
                        "instructions": { "type": "string" },
                        "prompt": { "type": "string", "description": "Initial task prompt delivered to the spawned or woken agent." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "conversation",
                "description": "Read or search (`query`) two-way (`in` + `out`) conversation history across agents, channels, groups, and peers.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string", "description": "Agent name (`self`, `all`, or peer name)." },
                        "query": { "type": "string", "description": "Optional Hybrid RAG search query." },
                        "with": { "type": "string", "description": "Optional peer filter." },
                        "limit": { "type": "integer", "description": "Max turns to return." }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "groups",
                "description": "List, create, delete, or manage agent groups (`list`, `create`, `delete`, `add_member`, `remove_member`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string" },
                        "name": { "type": "string" },
                        "agent": { "type": "string" },
                        "members": { "type": "array", "items": { "type": "string" } }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "schedule",
                "description": "List, create, pause, resume, or cancel durable timers, recurring intervals, cron schedules, and event-driven watches (`list`, `create`, `pause`, `resume`, `cancel`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string", "description": "`list`, `create`, `pause`, `resume`, or `cancel`." },
                        "id": { "type": "string" },
                        "kind": { "type": "string", "description": "`after`, `every`, `cron`, or `watch`." },
                        "seconds": { "type": "integer" },
                        "cron": { "type": "string" },
                        "watch_kind": { "type": "string", "description": "`command`, `process`, `file`, `http`, or `subagent`." },
                        "watch_target": { "type": "string" },
                        "prompt": { "type": "string" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "browser",
                "description": "Drive a headless Chromium browser (`goto`, `snapshot`, `click`, `fill`, `type`, `press`, `eval`, `text`, `html`, `pdf`, `screenshot`, `tabs`, `close`).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": { "type": "string" },
                        "url": { "type": "string" },
                        "selector": { "type": "string" },
                        "value": { "type": "string" },
                        "script": { "type": "string" },
                        "path": { "type": "string" }
                    },
                    "required": ["action"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "bash",
                "description": "Execute a bash command in the agent's working directory.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": { "type": "string", "description": "Shell command to run." },
                        "timeout_ms": { "type": "integer", "description": "Optional timeout in ms (default 120000)." }
                    },
                    "required": ["command"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a UTF-8 file from the workspace.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": "Relative or absolute file path." },
                        "start_line": { "type": "integer" },
                        "end_line": { "type": "integer" }
                    },
                    "required": ["path"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "write_file",
                "description": "Write contents to a file in the workspace (creating parent directories if needed).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "content": { "type": "string" }
                    },
                    "required": ["path", "content"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "edit_file",
                "description": "Replace an exact target string in a file with a replacement string.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "target": { "type": "string" },
                        "replacement": { "type": "string" }
                    },
                    "required": ["path", "target", "replacement"]
                }
            }
        }),
    ]
}

fn execute_sylphx_tool(
    name: &str,
    args: &Value,
    work_dir: &Path,
    env: &[(String, String)],
    mcp_command: &Path,
    mcp_args: &[String],
) -> String {
    let normalized = name.strip_prefix("mcp__anyagents__").unwrap_or(name);
    match normalized {
        "agents" | "subagents" | "send" | "conversation" | "search" | "groups" | "schedule"
        | "browser" | "describe" | "archive" | "wake" | "spawn" | "interact" => {
            call_anyagents_mcp_tool(mcp_command, mcp_args, env, normalized, args)
        }
        "bash" | "shell" => {
            let Some(cmd_str) = args.get("command").and_then(Value::as_str) else {
                return "error: missing `command`".to_string();
            };
            let mut cmd = Command::new("bash");
            cmd.arg("-c")
                .arg(cmd_str)
                .current_dir(work_dir)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            for (k, v) in env {
                cmd.env(k, v);
            }
            match cmd.output() {
                Ok(out) => {
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let code = out.status.code().unwrap_or(-1);
                    let mut combined = format!("exit_code: {code}\n");
                    if !stdout.is_empty() {
                        combined.push_str(&stdout);
                    }
                    if !stderr.is_empty() {
                        if !combined.ends_with('\n') {
                            combined.push('\n');
                        }
                        combined.push_str("stderr:\n");
                        combined.push_str(&stderr);
                    }
                    if combined.len() > 32_000 {
                        combined.truncate(32_000);
                        combined.push_str("\n...[truncated]");
                    }
                    combined
                }
                Err(e) => format!("error executing bash: {e}"),
            }
        }
        "read_file" => {
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            let target = resolve_workspace_path(work_dir, rel);
            match std::fs::read_to_string(&target) {
                Ok(content) => {
                    let start = args
                        .get("start_line")
                        .and_then(Value::as_u64)
                        .unwrap_or(1)
                        .max(1) as usize;
                    let lines: Vec<&str> = content.lines().collect();
                    let end = args
                        .get("end_line")
                        .and_then(Value::as_u64)
                        .map(|n| n as usize)
                        .unwrap_or(lines.len())
                        .min(lines.len());
                    if start > lines.len() {
                        return String::new();
                    }
                    lines[start - 1..end].join("\n")
                }
                Err(e) => format!("error reading {}: {e}", target.display()),
            }
        }
        "write_file" => {
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            let Some(content) = args.get("content").and_then(Value::as_str) else {
                return "error: missing `content`".to_string();
            };
            let target = resolve_workspace_path(work_dir, rel);
            if let Some(p) = target.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            match std::fs::write(&target, content) {
                Ok(()) => format!("wrote {} ({} bytes)", target.display(), content.len()),
                Err(e) => format!("error writing {}: {e}", target.display()),
            }
        }
        "edit_file" => {
            let Some(rel) = args.get("path").and_then(Value::as_str) else {
                return "error: missing `path`".to_string();
            };
            let Some(old_str) = args.get("target").and_then(Value::as_str) else {
                return "error: missing `target`".to_string();
            };
            let Some(new_str) = args.get("replacement").and_then(Value::as_str) else {
                return "error: missing `replacement`".to_string();
            };
            let target = resolve_workspace_path(work_dir, rel);
            match std::fs::read_to_string(&target) {
                Ok(existing) => {
                    if !existing.contains(old_str) {
                        return format!("error: target string not found in {}", target.display());
                    }
                    let updated = existing.replace(old_str, new_str);
                    match std::fs::write(&target, updated) {
                        Ok(()) => format!("edited {}", target.display()),
                        Err(e) => format!("error writing {}: {e}", target.display()),
                    }
                }
                Err(e) => format!("error reading {}: {e}", target.display()),
            }
        }
        other => format!("error: unknown tool `{other}`"),
    }
}

fn resolve_workspace_path(work_dir: &Path, raw: &str) -> PathBuf {
    let p = Path::new(raw);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        work_dir.join(p)
    }
}

fn call_anyagents_mcp_tool(
    mcp_command: &Path,
    mcp_args: &[String],
    env: &[(String, String)],
    tool_name: &str,
    arguments: &Value,
) -> String {
    let mut cmd = Command::new(mcp_command);
    cmd.args(mcp_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let Ok(mut child) = cmd.spawn() else {
        return format!("error: failed to spawn anyagents mcp for `{tool_name}`");
    };
    let init_req = json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "sylphx-harness", "version": "0.1.0" }
        }
    });
    let init_notif = json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    });
    let call_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments,
        }
    });
    if let Some(mut stdin) = child.stdin.take() {
        let _ = writeln!(stdin, "{init_req}");
        let _ = writeln!(stdin, "{init_notif}");
        let _ = writeln!(stdin, "{call_req}");
    }
    let Ok(out) = child.wait_with_output() else {
        return format!("error: failed waiting on anyagents mcp for `{tool_name}`");
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines().rev() {
        if let Ok(val) = serde_json::from_str::<Value>(line)
            && val.get("id").and_then(Value::as_i64) == Some(1)
        {
            if let Some(text) = val
                .pointer("/result/content/0/text")
                .and_then(Value::as_str)
            {
                return text.to_string();
            }
            if let Some(err) = val.pointer("/error/message").and_then(Value::as_str) {
                return format!("error: {err}");
            }
        }
    }
    stdout.trim().to_string()
}

fn curl_post_json(
    url: &str,
    bearer_token: Option<&str>,
    body: &Value,
) -> Result<(u16, String), String> {
    let payload = serde_json::to_string(body).map_err(|e| e.to_string())?;
    let mut cmd = Command::new("curl");
    cmd.args([
        "-sS",
        "-X",
        "POST",
        "--max-time",
        "120",
        "-H",
        "Content-Type: application/json",
        "-w",
        "\n%{http_code}",
    ]);
    if let Some(tok) = bearer_token {
        cmd.arg("-H").arg(format!("Authorization: Bearer {tok}"));
    }
    cmd.arg("-d")
        .arg(&payload)
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = cmd.output().map_err(|e| e.to_string())?;
    if !out.status.success() && out.stdout.is_empty() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    parse_curl_status_and_body(&String::from_utf8_lossy(&out.stdout))
}

fn curl_get(url: &str, bearer_token: Option<&str>) -> Result<(u16, String), String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_harness::Context;

    #[test]
    fn write_sylphx_config_creates_valid_runtime_files() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        write_sylphx_config(
            cfg,
            Some("sylphx/openai/gpt-6.1-sol#xhigh"),
            Path::new("/usr/local/bin/anyagents"),
            &[
                "--data".to_string(),
                "/tmp/data".to_string(),
                "mcp".to_string(),
            ],
        )
        .unwrap();

        let agent_json: Value = serde_json::from_str(
            &std::fs::read_to_string(cfg.join("sylphx-agent.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(agent_json["harness"], "sylphx");
        assert_eq!(agent_json["model"], "sylphx/openai/gpt-6.1-sol#xhigh");
        assert_eq!(
            agent_json["mcpServers"]["anyagents"]["command"],
            "/usr/local/bin/anyagents"
        );
    }

    #[test]
    fn save_and_resolve_sylphx_credentials_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let creds = SylphxCredentials {
            api_key: "sk-sylphx-test-123".to_string(),
            key_name: Some("anyagents".to_string()),
            org_id: Some("org_123".to_string()),
            org_slug: Some("sylphx".to_string()),
            api_url: Some("https://api.sylphx.com".to_string()),
            base_url: Some("https://api.sylphx.com".to_string()),
            store: Some("file".to_string()),
            orgs: Vec::new(),
            active_org: Some("org_123".to_string()),
        };
        save_sylphx_credentials(tmp.path(), &creds).unwrap();
        assert_eq!(
            read_sylphx_key_from_dir(tmp.path()).as_deref(),
            Some("sk-sylphx-test-123")
        );
        assert!(tmp.path().join(".config/opencode/sylphx.key").is_file());
        assert!(tmp.path().join(".config/desk/sylphx-ai/key").is_file());
    }

    #[test]
    fn sylphx_harness_fake_mode_delivers_and_steers_mid_turn() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("config");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();

        let mut harness = SylphxHarness::start(
            &cfg,
            &work,
            Some("sylphx/openai/gpt-6.1-sol#xhigh"),
            None,
            &[("ANYAGENTS_FAKE_MODE", "1")],
            Path::new("anyagents"),
            &[],
        )
        .unwrap();

        assert_eq!(
            SylphxHarness::declared_capabilities(),
            (DeliveryMode::Steer, SteerSupport::Yes)
        );
        assert!(harness.session_id().is_some());

        let first = OutboundMessage {
            id: "msg_1".to_string(),
            seq: 1,
            text: "sleep:1 please".to_string(),
            sender: Some("agent:vulcan".to_string()),
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        };
        assert_eq!(
            harness.deliver(&first).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );
        assert_eq!(harness.health().turn_state, TurnState::InTurn);

        let second = OutboundMessage {
            id: "msg_2".to_string(),
            seq: 2,
            text: "mid-turn steer message".to_string(),
            sender: Some("agent:vulcan".to_string()),
            reply_to: None,
            source_time_ms: None,
            context: Context::default(),
        };
        assert_eq!(
            harness.deliver(&second).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );

        for _ in 0..40 {
            if harness.health().turn_state == TurnState::Idle {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert_eq!(harness.health().turn_state, TurnState::Idle);
        assert_eq!(harness.turns_served(), 1);
    }

    #[test]
    fn create_agent_and_subagent_with_sylphx_harness_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let store = anyagents_core::Store::open(data.join("state.db")).unwrap();

        let root = crate::create::create(
            &store,
            &data,
            crate::create::CreateRequest {
                name: "sylphx-lead".to_string(),
                role: Some("Sylphx Lead".to_string()),
                harness: "sylphx-agent".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: None,
                instructions: Some("Lead instructions".to_string()),
                parent_id: None,
            },
        )
        .unwrap();
        assert_eq!(root.harness, "sylphx");
        let root_cfg = anyagents_core::agent_config_dir(&data, &root.id);
        assert!(root_cfg.join("sylphx-agent.json").is_file());

        let sub = crate::create::create(
            &store,
            &data,
            crate::create::CreateRequest {
                name: "sylphx-worker-1".to_string(),
                role: Some("Sylphx Sub-Agent".to_string()),
                harness: "sylphx".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#medium".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: None,
                instructions: Some("Worker instructions".to_string()),
                parent_id: Some(root.id.clone()),
            },
        )
        .unwrap();
        assert_eq!(sub.harness, "sylphx");
        assert_eq!(sub.parent_id.as_ref(), Some(&root.id));
        let sub_cfg = anyagents_core::agent_config_dir(&data, &sub.id);
        assert!(sub_cfg.join("sylphx-agent.json").is_file());
    }

    #[test]
    fn responses_api_tools_and_url_normalization_work() {
        assert_eq!(
            normalize_sylphx_gateway_url("https://api.models.sylphx.ai/v1"),
            "https://api.sylphx.com/v1"
        );
        assert_eq!(
            normalize_sylphx_gateway_url("https://api.sylphx.com/v1/"),
            "https://api.sylphx.com/v1"
        );
        let tools = build_responses_tool_definitions();
        assert!(!tools.is_empty());
        assert_eq!(tools[0]["type"], "function");
        assert!(tools[0]["name"].is_string());
        assert!(tools[0]["parameters"].is_object());
        assert!(tools[0].get("function").is_none());
    }
}
