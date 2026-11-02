//! Cloud Kata microVM Sandbox (`Sylphx Sandboxes` `/v1/{parent}/leases`) and declarative
//! `EnvironmentSpec` (`Sylphx Agents` BaaS) worktree management.
//!
//! Security Invariant:
//! - GitHub credentials (`GH_TOKEN` / `GITHUB_TOKEN`) are **never** written to disk inside the
//!   sandbox (`~/.git-credentials` and `~/.anyagents_cloud_env` are never created and any legacy
//!   files are purged). Instead, `git` uses an in-memory credential helper (`!f() { ... }`) that
//!   reads `$GH_TOKEN` from the ephemeral `:exec` process environment when needed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde_json::{Value, json};

use super::auth::{
    DEFAULT_SYLPHX_API_URL, base64_decode, base64_encode, curl_get, curl_post_json,
    resolve_sylphx_api_key, truncate_tool_output_middle_utf8,
};

pub const DEFAULT_SYLPHX_SANDBOX_PARENT: &str = "orgs/org_fs01afbdccgn8xdk1ghj5y47w0vtph/projects/prj_fs01nh48z2cee2ccqddykqeys3rvc4/envs/env_fs014mk148yf5ngawpe4zcey9rvq89";
pub const DEFAULT_SYLPHX_SANDBOX_SHAPE: &str = "linux-s";
pub const DEFAULT_SYLPHX_SANDBOX_IDLE_TIMEOUT: &str = "1800s";
pub const CLOUD_WORKSPACE_DIR: &str = "/home/user/workspace";
const CLOUD_EXEC_ZERO_SENTINEL: i64 = 200;

fn config_dir_lease_lock(config_dir: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let map = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard
        .entry(config_dir.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn next_exec_idempotency_key(prefix: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!(
        "{prefix}_{}_{}_{n}",
        std::process::id(),
        anyagents_core::now_ms()
    )
}

/// Wraps a guest shell script so an exit code of `0` is emitted over `envd` protojson
/// as `CLOUD_EXEC_ZERO_SENTINEL` (`200`), avoiding proto3 default-scalar omission of
/// `"exitCode": 0` in `sandboxes-api`.
pub(super) fn wrap_cloud_exec_command_for_zero_exit(script: &str) -> String {
    format!(
        "( {script}\n); __sylphx_rc=$?; if [ \"$__sylphx_rc\" -eq 0 ]; then exit {CLOUD_EXEC_ZERO_SENTINEL}; else exit \"$__sylphx_rc\"; fi"
    )
}

pub(super) fn normalize_cloud_exec_exit_code(raw_code: i64) -> i64 {
    if raw_code == CLOUD_EXEC_ZERO_SENTINEL {
        0
    } else {
        raw_code
    }
}

#[derive(Debug, Clone, Default)]
pub struct CloudWorktreeSpec {
    pub repo_url: Option<String>,
    pub repo_slug: Option<String>,
    pub branch: Option<String>,
    pub base: Option<String>,
}

pub fn normalize_cloud_env_slug(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches('/');
    match trimmed {
        ""
        | "default"
        | "prod"
        | "sylphx"
        | "sylphx/default/prod"
        | "orgs/sylphx/projects/default/envs/prod"
        | "organizations/sylphx/projects/default/environments/prod" => {
            DEFAULT_SYLPHX_SANDBOX_PARENT.to_string()
        }
        other => other.to_string(),
    }
}

pub(super) fn resolve_cloud_api_url(env: &[(String, String)]) -> String {
    env.iter()
        .find(|(k, v)| k == "SYLPHX_API_URL" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().trim_end_matches('/').to_string())
        .or_else(|| {
            std::env::var("SYLPHX_API_URL")
                .ok()
                .map(|s| s.trim().trim_end_matches('/').to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| DEFAULT_SYLPHX_API_URL.to_string())
}

pub(super) fn resolve_sandbox_shape(env: &[(String, String)]) -> String {
    env.iter()
        .find(|(k, v)| k == "SYLPHX_SANDBOX_SHAPE" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().to_string())
        .or_else(|| {
            std::env::var("SYLPHX_SANDBOX_SHAPE")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| DEFAULT_SYLPHX_SANDBOX_SHAPE.to_string())
}

pub(super) fn resolve_sandbox_idle_timeout(env: &[(String, String)]) -> String {
    env.iter()
        .find(|(k, v)| k == "SYLPHX_SANDBOX_IDLE_TIMEOUT" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().to_string())
        .or_else(|| {
            std::env::var("SYLPHX_SANDBOX_IDLE_TIMEOUT")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| DEFAULT_SYLPHX_SANDBOX_IDLE_TIMEOUT.to_string())
}

/// Builds a declarative `sylphx.agents.v1.EnvironmentSpec` JSON object for `Sylphx Agents`
/// BaaS (`POST /v1/{env}/agents` and `POST /v1/{env}/agent_sessions`) so the cloud runtime
/// provisions a Kata microVM sandbox (`template:agent`, `shape: "linux-s"` warm pool) with
/// the agent's Git repository and branch/revision mounted at `/home/user/workspace`.
///
/// Emits strictly valid `sylphx.agents.v1.EnvironmentSpec` and `RepositorySpec` protobuf
/// fields (`shape`, `template`, `repo: { uri, revision }`) so strict AIP proto validation
/// never rejects with `400 UNKNOWN_FIELD`.
pub fn build_cloud_environment_spec(
    config_dir: &Path,
    work_dir: Option<&Path>,
    env: &[(String, String)],
) -> Value {
    let wt = resolve_agent_git_worktree_spec(config_dir, work_dir, env, None, None, None);
    let shape = resolve_sandbox_shape(env);
    let mut spec = json!({
        "shape": shape,
        "template": "template:agent",
        "ttl": "1800s",
    });
    if let Some(uri) = wt.repo_url
        && let Some(obj) = spec.as_object_mut()
    {
        let mut repo_obj = serde_json::Map::new();
        repo_obj.insert("uri".to_string(), json!(uri));
        if let Some(rev) = wt.branch.or(wt.base) {
            repo_obj.insert("revision".to_string(), json!(rev));
        }
        obj.insert("repo".to_string(), Value::Object(repo_obj));
    }
    spec
}

fn normalize_github_repo_url_and_slug(raw: &str) -> Option<(String, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "inherit" | "parent" | "worktree" | "branch" | "isolated" | "explicit-worktree"
        )
    {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix("git@github.com:") {
        let slug = rest.trim_end_matches(".git").trim_matches('/');
        if slug.contains('/') {
            return Some((format!("https://github.com/{slug}.git"), slug.to_string()));
        }
    }
    if let Some(rest) = trimmed
        .strip_prefix("https://github.com/")
        .or_else(|| trimmed.strip_prefix("http://github.com/"))
    {
        let slug = rest.trim_end_matches(".git").trim_matches('/');
        if slug.contains('/') {
            return Some((format!("https://github.com/{slug}.git"), slug.to_string()));
        }
    }
    if trimmed.starts_with("https://") || trimmed.starts_with("http://") {
        return Some((trimmed.to_string(), trimmed.to_string()));
    }
    let p = Path::new(trimmed);
    if p.is_dir()
        && let Some(origin) = git_origin_url(p)
    {
        return normalize_github_repo_url_and_slug(&origin);
    }
    let parts: Vec<&str> = trimmed.trim_matches('/').split('/').collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() && !trimmed.contains("..") {
        let slug = format!("{}/{}", parts[0], parts[1].trim_end_matches(".git"));
        return Some((format!("https://github.com/{slug}.git"), slug));
    }
    None
}

fn git_origin_url(dir: &Path) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["config", "--get", "remote.origin.url"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then_some(s)
}

fn git_current_branch(dir: &Path) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty() && s != "HEAD").then_some(s)
}

fn git_current_head_sha(dir: &Path) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "HEAD"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then_some(s)
}

pub fn resolve_agent_git_worktree_spec(
    config_dir: &Path,
    work_dir: Option<&Path>,
    env: &[(String, String)],
    explicit_repo: Option<&str>,
    explicit_branch: Option<&str>,
    explicit_base: Option<&str>,
) -> CloudWorktreeSpec {
    let marker = std::fs::read_to_string(config_dir.join(".anyagents-managed-worktree.json"))
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok());
    let agent_json = std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok());

    let env_lookup = |key: &str| -> Option<String> {
        env.iter()
            .find(|(k, v)| k == key && !v.trim().is_empty())
            .map(|(_, v)| v.trim().to_string())
            .or_else(|| {
                std::env::var(key)
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
    };

    let raw_repo = explicit_repo
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| env_lookup("ANYAGENTS_REPO"))
        .or_else(|| {
            marker
                .as_ref()
                .and_then(|m| m.get("repo"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .or_else(|| {
            agent_json
                .as_ref()
                .and_then(|m| m.get("repo"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        });

    let mut candidate_git_dirs: Vec<PathBuf> = Vec::new();
    if let Some(wd) = work_dir {
        candidate_git_dirs.push(wd.to_path_buf());
    }
    if let Some(p) = marker
        .as_ref()
        .and_then(|m| m.get("path"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
    {
        candidate_git_dirs.push(p);
    }
    if let Some(parent) = config_dir.parent() {
        candidate_git_dirs.push(parent.join("work"));
    }

    let (repo_url, repo_slug) = raw_repo
        .as_deref()
        .and_then(normalize_github_repo_url_and_slug)
        .or_else(|| {
            for dir in &candidate_git_dirs {
                if dir.is_dir()
                    && let Some(origin) = git_origin_url(dir)
                    && let Some(pair) = normalize_github_repo_url_and_slug(&origin)
                {
                    return Some(pair);
                }
            }
            None
        })
        .map(|(u, s)| (Some(u), Some(s)))
        .unwrap_or((None, None));

    let branch = explicit_branch
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| env_lookup("ANYAGENTS_BRANCH"))
        .or_else(|| {
            marker
                .as_ref()
                .and_then(|m| m.get("branch"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .or_else(|| {
            agent_json
                .as_ref()
                .and_then(|m| m.get("branch"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .or_else(|| {
            for dir in &candidate_git_dirs {
                if dir.is_dir()
                    && let Some(b) = git_current_branch(dir)
                {
                    return Some(b);
                }
            }
            None
        });

    let base = explicit_base
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| env_lookup("ANYAGENTS_BASE"))
        .or_else(|| {
            marker
                .as_ref()
                .and_then(|m| m.get("base").or_else(|| m.get("initial_head")))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .or_else(|| {
            agent_json
                .as_ref()
                .and_then(|m| m.get("base"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .or_else(|| {
            for dir in &candidate_git_dirs {
                if dir.is_dir()
                    && let Some(sha) = git_current_head_sha(dir)
                {
                    return Some(sha);
                }
            }
            None
        });

    CloudWorktreeSpec {
        repo_url,
        repo_slug,
        branch,
        base,
    }
}

pub(super) fn resolve_github_token(env: &[(String, String)]) -> Option<String> {
    for (k, v) in env {
        if (k == "GH_TOKEN" || k == "GITHUB_TOKEN") && !v.trim().is_empty() {
            return Some(v.trim().to_string());
        }
    }
    for k in ["GH_TOKEN", "GITHUB_TOKEN"] {
        if let Ok(v) = std::env::var(k)
            && !v.trim().is_empty()
        {
            return Some(v.trim().to_string());
        }
    }
    if !crate::sandbox::is_isolated_test_env() {
        static CACHED_GH_TOKEN: OnceLock<Mutex<Option<(i64, String)>>> = OnceLock::new();
        let cache = CACHED_GH_TOKEN.get_or_init(|| Mutex::new(None));
        let now = anyagents_core::now_ms();
        if let Ok(guard) = cache.lock()
            && let Some((ts, ref tok)) = *guard
            && now.saturating_sub(ts) < 300_000
        {
            return Some(tok.clone());
        }
        if let Ok(out) = Command::new("gh")
            .args(["auth", "token"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            && out.status.success()
        {
            let tok = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !tok.is_empty() {
                if let Ok(mut guard) = cache.lock() {
                    *guard = Some((now, tok.clone()));
                }
                return Some(tok);
            }
        }
    }
    None
}

pub fn resolve_cloud_sandbox_path(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == "./" || trimmed == "/workspace" {
        return CLOUD_WORKSPACE_DIR.to_string();
    }
    if let Some(rest) = trimmed.strip_prefix("/workspace/") {
        return format!("{CLOUD_WORKSPACE_DIR}/{}", rest.trim_start_matches('/'));
    }
    if trimmed.starts_with('/') {
        return trimmed.to_string();
    }
    let clean = trimmed.strip_prefix("./").unwrap_or(trimmed);
    format!("{CLOUD_WORKSPACE_DIR}/{clean}")
}

fn parse_retry_after_ms_from_reason(reason: &str) -> Option<i64> {
    let idx = reason.find('{')?;
    let val = serde_json::from_str::<Value>(&reason[idx..]).ok()?;
    val.get("retry_after_ms")
        .or_else(|| val.get("retryAfterMs"))
        .or_else(|| val.pointer("/error/retry_after_ms"))
        .and_then(Value::as_i64)
        .filter(|&ms| ms > 0)
}

fn deterministic_jitter_ms(config_dir: &Path, max_jitter_ms: i64) -> i64 {
    if max_jitter_ms <= 0 {
        return 0;
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in config_dir.to_string_lossy().as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % (max_jitter_ms as u64)) as i64
}

pub(super) fn record_cloud_sandbox_fallback(config_dir: &Path, reason: &str) {
    let fallback_path = config_dir.join("sylphx-sandbox-fallback.json");
    let prev_failures = std::fs::read_to_string(&fallback_path)
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .and_then(|v| v.get("consecutive_failures").and_then(Value::as_u64))
        .unwrap_or(0);
    let consecutive_failures = prev_failures.saturating_add(1);
    let base_cooldown_ms = std::env::var("SYLPHX_SANDBOX_FALLBACK_COOLDOWN_MS")
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(60_000);
    let shift = consecutive_failures.saturating_sub(1).min(4) as u32;
    let exp_cooldown_ms = base_cooldown_ms.saturating_mul(1i64 << shift).min(900_000);
    let retry_after_ms = parse_retry_after_ms_from_reason(reason).unwrap_or(0);
    let jitter_ms = if base_cooldown_ms >= 5_000 {
        deterministic_jitter_ms(config_dir, 15_000)
    } else {
        0
    };
    let cooldown_ms = exp_cooldown_ms
        .max(retry_after_ms)
        .saturating_add(jitter_ms);
    let now = anyagents_core::now_ms();
    let doc = json!({
        "fallback_to": "local",
        "reason": reason,
        "timestamp_ms": now,
        "consecutive_failures": consecutive_failures,
        "cooldown_ms": cooldown_ms,
        "next_retry_at_ms": now.saturating_add(cooldown_ms),
    });
    let _ = std::fs::write(&fallback_path, format!("{doc}\n"));
}

pub(super) fn clear_cloud_sandbox_fallback(config_dir: &Path) {
    let _ = std::fs::remove_file(config_dir.join("sylphx-sandbox-fallback.json"));
}

pub(super) fn is_cloud_sandbox_in_fallback_cooldown(config_dir: &Path) -> bool {
    let fallback_path = config_dir.join("sylphx-sandbox-fallback.json");
    let Ok(raw) = std::fs::read_to_string(&fallback_path) else {
        return false;
    };
    let Ok(val) = serde_json::from_str::<Value>(&raw) else {
        return false;
    };
    let Some(ts) = val.get("timestamp_ms").and_then(Value::as_i64) else {
        return false;
    };
    let now = anyagents_core::now_ms();
    if let Ok(explicit_env_ms) = std::env::var("SYLPHX_SANDBOX_FALLBACK_COOLDOWN_MS")
        && let Ok(cooldown_ms) = explicit_env_ms.trim().parse::<i64>()
    {
        return now.saturating_sub(ts) <= cooldown_ms;
    }
    if let Some(next_retry_at_ms) = val.get("next_retry_at_ms").and_then(Value::as_i64) {
        return now <= next_retry_at_ms;
    }
    let cooldown_ms = val
        .get("cooldown_ms")
        .and_then(Value::as_i64)
        .unwrap_or(60_000);
    now.saturating_sub(ts) <= cooldown_ms
}

pub(super) fn resolve_sandbox_mode(config_dir: &Path, env: &[(String, String)]) -> String {
    let raw = env
        .iter()
        .find(|(k, v)| k == "SYLPHX_SANDBOX" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().to_ascii_lowercase())
        .or_else(|| {
            std::env::var("SYLPHX_SANDBOX")
                .ok()
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
        })
        .or_else(|| {
            std::fs::read_to_string(config_dir.join("sylphx-agent.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .and_then(|v| {
                    v.get("sandbox")
                        .and_then(Value::as_str)
                        .map(|s| s.trim().to_ascii_lowercase())
                })
        })
        .or_else(|| {
            std::fs::read_to_string(config_dir.join(".anyagents-managed-worktree.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .and_then(|v| {
                    (v.get("cloud_sandbox").and_then(Value::as_bool) == Some(true))
                        .then(|| "cloud".to_string())
                })
        });
    match raw.as_deref() {
        Some("none" | "stateless" | "off" | "false" | "0") => "none".to_string(),
        Some("local" | "host") => "local".to_string(),
        Some("cloud" | "pod" | "kata" | "microvm" | "sandbox" | "auto") => {
            if is_cloud_sandbox_in_fallback_cooldown(config_dir) {
                "local".to_string()
            } else {
                "cloud".to_string()
            }
        }
        _ => {
            if is_cloud_sandbox_in_fallback_cooldown(config_dir) {
                "local".to_string()
            } else if config_dir.join("sylphx-sandbox-lease.json").is_file() {
                "cloud".to_string()
            } else if !crate::sandbox::is_isolated_test_env()
                && resolve_sylphx_cloud_access_key(config_dir, env).is_some()
            {
                "cloud".to_string()
            } else {
                "local".to_string()
            }
        }
    }
}

pub fn resolve_sylphx_cloud_access_key(
    config_dir: &Path,
    env: &[(String, String)],
) -> Option<String> {
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
        let creds_path = dir.join(".config/sylphx/credentials.json");
        if let Ok(raw) = std::fs::read_to_string(&creds_path)
            && let Ok(val) = serde_json::from_str::<Value>(&raw)
            && let Some(k) = val
                .get("api_key")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            return Some(k.to_string());
        }
    }
    resolve_sylphx_api_key(config_dir, env)
}

#[allow(dead_code)]
pub fn ensure_cloud_sandbox_lease(
    config_dir: &Path,
    env: &[(String, String)],
) -> Result<(String, String), String> {
    ensure_cloud_sandbox_lease_with_worktree(config_dir, None, env, None, None, None)
}

fn is_terminal_lease_state(state: &str) -> bool {
    matches!(
        state,
        "refused"
            | "ended"
            | "failed"
            | "expired"
            | "released"
            | "lease_state_refused"
            | "lease_state_ended"
            | "lease_state_failed"
            | "lease_state_expired"
            | "lease_state_released"
    )
}

fn is_ready_lease_state(state: &str) -> bool {
    matches!(state, "ready" | "lease_state_ready")
}

fn poll_lease_until_ready(
    api_url: &str,
    access_key: &str,
    lease_name: &str,
    initial_val: Value,
) -> Result<Value, String> {
    let mut lease_val = initial_val;
    let initial_state = lease_val
        .pointer("/status/state")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    if is_ready_lease_state(&initial_state) {
        return Ok(lease_val);
    }
    if is_terminal_lease_state(&initial_state) {
        return Err(format!(
            "Cloud Sandbox Lease `{lease_name}` entered state `{initial_state}`"
        ));
    }
    let get_url = format!("{api_url}/v1/{lease_name}");
    for _ in 0..45 {
        std::thread::sleep(Duration::from_secs(1));
        if let Ok((200..=299, body)) = curl_get(&get_url, Some(access_key))
            && let Ok(live) = serde_json::from_str::<Value>(&body)
        {
            let st = live
                .pointer("/status/state")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            lease_val = live;
            if is_ready_lease_state(&st) {
                return Ok(lease_val);
            }
            if is_terminal_lease_state(&st) {
                return Err(format!(
                    "Cloud Sandbox Lease `{lease_name}` entered state `{st}`"
                ));
            }
        }
    }
    Err(format!(
        "Cloud Sandbox Lease `{lease_name}` did not become ready within 45s"
    ))
}

fn persist_lease_metadata(
    lease_file: &Path,
    lease_val: &mut Value,
    wt_spec: &CloudWorktreeSpec,
    bootstrapped: bool,
) {
    if let Some(obj) = lease_val.as_object_mut() {
        obj.insert("workspace_bootstrapped".to_string(), json!(bootstrapped));
        obj.insert("workspace_dir".to_string(), json!(CLOUD_WORKSPACE_DIR));
        if let Some(ref slug) = wt_spec.repo_slug {
            obj.insert("repo".to_string(), json!(slug));
        }
        if let Some(ref br) = wt_spec.branch {
            obj.insert("branch".to_string(), json!(br));
        }
    }
    if let Ok(pretty) = serde_json::to_string_pretty(lease_val) {
        let _ = std::fs::write(lease_file, format!("{pretty}\n"));
    }
}

pub fn ensure_cloud_sandbox_lease_with_worktree(
    config_dir: &Path,
    work_dir: Option<&Path>,
    env: &[(String, String)],
    explicit_repo: Option<&str>,
    explicit_branch: Option<&str>,
    explicit_base: Option<&str>,
) -> Result<(String, String), String> {
    let lock = config_dir_lease_lock(config_dir);
    let _guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let access_key = resolve_sylphx_cloud_access_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx Cloud access key; run `anyagents sign-in sylphx` first".to_string()
    })?;
    let api_url = resolve_cloud_api_url(env);
    let shape = resolve_sandbox_shape(env);
    let idle_timeout = resolve_sandbox_idle_timeout(env);
    let wt_spec = resolve_agent_git_worktree_spec(
        config_dir,
        work_dir,
        env,
        explicit_repo,
        explicit_branch,
        explicit_base,
    );
    let lease_file = config_dir.join("sylphx-sandbox-lease.json");

    if let Ok(raw) = std::fs::read_to_string(&lease_file)
        && let Ok(persisted) = serde_json::from_str::<Value>(&raw)
        && let Some(name) = persisted
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    {
        let get_url = format!("{api_url}/v1/{name}");
        match curl_get(&get_url, Some(&access_key)) {
            Ok((200..=299, body)) => {
                if let Ok(live) = serde_json::from_str::<Value>(&body) {
                    let state = live
                        .pointer("/status/state")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    if is_terminal_lease_state(&state) {
                        let _ = std::fs::remove_file(&lease_file);
                    } else {
                        let mut ready_live =
                            match poll_lease_until_ready(&api_url, &access_key, &name, live) {
                                Ok(v) => v,
                                Err(e) => {
                                    if e.contains("entered state") {
                                        let _ = std::fs::remove_file(&lease_file);
                                    }
                                    return Err(e);
                                }
                            };
                        let uid = ready_live
                            .get("uid")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let prev_repo = persisted
                            .get("repo")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        let prev_branch = persisted
                            .get("branch")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        let already_bootstrapped = persisted
                            .get("workspace_bootstrapped")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        let needs_bootstrap = !already_bootstrapped
                            || (wt_spec.repo_slug.is_some() && wt_spec.repo_slug != prev_repo)
                            || (wt_spec.branch.is_some() && wt_spec.branch != prev_branch);
                        if needs_bootstrap {
                            bootstrap_cloud_sandbox_worktree(
                                &api_url,
                                &access_key,
                                &name,
                                &wt_spec,
                                env,
                            )?;
                            persist_lease_metadata(&lease_file, &mut ready_live, &wt_spec, true);
                        }
                        clear_cloud_sandbox_fallback(config_dir);
                        return Ok((name, uid));
                    }
                }
            }
            Ok((404, _)) => {
                let _ = std::fs::remove_file(&lease_file);
            }
            Ok((status, body)) => {
                return Err(format!("GetLease HTTP {status}: {}", body.trim()));
            }
            Err(e) => {
                return Err(format!("GetLease failed: {e}"));
            }
        }
    }

    let raw_parent = env
        .iter()
        .find(|(k, v)| k == "SYLPHX_SANDBOX_PARENT" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().to_string())
        .or_else(|| {
            std::env::var("SYLPHX_SANDBOX_PARENT")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| DEFAULT_SYLPHX_SANDBOX_PARENT.to_string());
    let parent = normalize_cloud_env_slug(&raw_parent);

    let create_url = format!("{api_url}/v1/{parent}/leases");
    let req_body = json!({
        "spec": {
            "shape": shape,
            "kind": "general",
            "image": "template:agent",
            "network": {
                "egress": "allow"
            },
            "ttl": "1800s",
            "idle_timeout": idle_timeout,
        }
    });
    let (status, raw) = curl_post_json(&create_url, Some(&access_key), &req_body)?;
    if !(200..300).contains(&status) {
        return Err(format!("CreateLease HTTP {status}: {}", raw.trim()));
    }
    let mut lease_val: Value =
        serde_json::from_str(&raw).map_err(|e| format!("invalid Lease JSON: {e}"))?;
    let lease_name = lease_val
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "CreateLease response missing `name`".to_string())?
        .to_string();
    let uid = lease_val
        .get("uid")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    // Persist immediately after CreateLease succeeds (`workspace_bootstrapped: false`)
    // so any failure during readiness polling or worktree bootstrap still tracks the
    // allocated lease rather than leaking an orphan VM on the next call.
    persist_lease_metadata(&lease_file, &mut lease_val, &wt_spec, false);

    let mut ready_val = match poll_lease_until_ready(&api_url, &access_key, &lease_name, lease_val)
    {
        Ok(v) => v,
        Err(e) => {
            if e.contains("entered state") {
                let _ = std::fs::remove_file(&lease_file);
            }
            return Err(e);
        }
    };

    bootstrap_cloud_sandbox_worktree(&api_url, &access_key, &lease_name, &wt_spec, env)?;
    persist_lease_metadata(&lease_file, &mut ready_val, &wt_spec, true);
    clear_cloud_sandbox_fallback(config_dir);
    Ok((lease_name, uid))
}

fn post_exec_with_idempotency_retry(
    exec_url: &str,
    access_key: &str,
    payload: &Value,
) -> Result<(u16, String), String> {
    for attempt in 0..60 {
        let (status, raw) = curl_post_json(exec_url, Some(access_key), payload)?;
        if status == 409 && raw.contains("IDEMPOTENCY_IN_PROGRESS") && attempt < 59 {
            std::thread::sleep(Duration::from_secs(2));
            continue;
        }
        if matches!(status, 502..=504) && attempt < 5 {
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }
        return Ok((status, raw));
    }
    curl_post_json(exec_url, Some(access_key), payload)
}

fn bootstrap_cloud_sandbox_worktree(
    api_url: &str,
    access_key: &str,
    lease_name: &str,
    wt_spec: &CloudWorktreeSpec,
    env: &[(String, String)],
) -> Result<(), String> {
    let exec_url = format!("{api_url}/v1/{lease_name}:exec");
    let mut exec_env = serde_json::Map::new();
    exec_env.insert(
        "ANYAGENTS_WORKSPACE".to_string(),
        json!(CLOUD_WORKSPACE_DIR),
    );
    if let Some(tok) = resolve_github_token(env) {
        exec_env.insert("GH_TOKEN".to_string(), json!(tok));
        exec_env.insert("GITHUB_TOKEN".to_string(), json!(tok));
    }
    if let Some(ref url) = wt_spec.repo_url {
        exec_env.insert("ANYAGENTS_CLONE_URL".to_string(), json!(url));
    }
    if let Some(ref slug) = wt_spec.repo_slug {
        exec_env.insert("ANYAGENTS_REPO_SLUG".to_string(), json!(slug));
    }
    if let Some(ref br) = wt_spec.branch {
        exec_env.insert("ANYAGENTS_BRANCH".to_string(), json!(br));
    }
    if let Some(ref base) = wt_spec.base {
        exec_env.insert("ANYAGENTS_BASE_REF".to_string(), json!(base));
    }

    // Security invariant: never persist GH_TOKEN to ~/.git-credentials or ~/.anyagents_cloud_env on disk.
    // Instead, configure an in-memory git credential helper that reads $GH_TOKEN from the ephemeral
    // `:exec` process environment only when git requests credentials.
    let bootstrap_script = r#"set -e
mkdir -p "$ANYAGENTS_WORKSPACE" "$HOME/.local/bin"
cat > "$HOME/.local/bin/gh" << 'EOF_GH'
#!/usr/bin/env bash
set -e
if [ -x /usr/bin/gh ]; then
  exec /usr/bin/gh "$@"
fi
GH_BIN="$HOME/.local/bin/gh-bin"
if [ ! -x "$GH_BIN" ]; then
  tmp_dir=$(mktemp -d /tmp/gh-dl.XXXXXX)
  if curl -fsSL "https://github.com/cli/cli/releases/download/v2.67.0/gh_2.67.0_linux_amd64.tar.gz" | tar -xz -C "$tmp_dir" 2>/dev/null; then
    mv "$tmp_dir"/gh_*_linux_amd64/bin/gh "$GH_BIN.$$"
    chmod 755 "$GH_BIN.$$"
    mv -f "$GH_BIN.$$" "$GH_BIN"
  fi
  rm -rf "$tmp_dir"
fi
if [ -x "$GH_BIN" ]; then
  exec "$GH_BIN" "$@"
fi
echo "gh: failed to bootstrap github cli" >&2
exit 1
EOF_GH
chmod 755 "$HOME/.local/bin/gh"
exec 9>/tmp/sylphx-bootstrap.lock
flock -w 180 9 || true
rm -f "$HOME/.git-credentials" "$HOME/.anyagents_cloud_env"
git config --global user.name "Kyle Tse"
git config --global user.email "hi@kylet.se"
git config --global init.defaultBranch main
git config --global --add safe.directory "$ANYAGENTS_WORKSPACE"
git config --global credential.helper '!f() { [ "$1" = get ] && [ -n "$GH_TOKEN" ] && printf "username=x-access-token\npassword=%s\n" "$GH_TOKEN"; }; f'
if [ -f "$ANYAGENTS_WORKSPACE/.git/index.lock" ] && ! pgrep -x git >/dev/null 2>&1; then
  rm -f "$ANYAGENTS_WORKSPACE/.git/index.lock"
fi
if [ -n "$ANYAGENTS_CLONE_URL" ] && [ ! -d "$ANYAGENTS_WORKSPACE/.git" ]; then
  for _try in 1 2 3; do
    if git clone --depth 50 --no-single-branch --filter=blob:none "$ANYAGENTS_CLONE_URL" "$ANYAGENTS_WORKSPACE"; then
      break
    fi
    rm -rf "$ANYAGENTS_WORKSPACE"
    mkdir -p "$ANYAGENTS_WORKSPACE"
    sleep 2
  done
fi
if [ -d "$ANYAGENTS_WORKSPACE/.git" ]; then
  cd "$ANYAGENTS_WORKSPACE"
  rm -f .git/index.lock
  git config --local user.name "Kyle Tse"
  git config --local user.email "hi@kylet.se"
  if [ -n "$ANYAGENTS_BASE_REF" ]; then
    git fetch origin "$ANYAGENTS_BASE_REF" >/dev/null 2>&1 || true
  fi
  if [ -n "$ANYAGENTS_BRANCH" ] && [ "$ANYAGENTS_BRANCH" != "HEAD" ]; then
    git fetch origin "$ANYAGENTS_BRANCH" >/dev/null 2>&1 || true
    if git show-ref --verify --quiet "refs/heads/$ANYAGENTS_BRANCH"; then
      git checkout "$ANYAGENTS_BRANCH"
    elif git show-ref --verify --quiet "refs/remotes/origin/$ANYAGENTS_BRANCH"; then
      git checkout -B "$ANYAGENTS_BRANCH" "refs/remotes/origin/$ANYAGENTS_BRANCH"
    elif [ -n "$ANYAGENTS_BASE_REF" ] && git rev-parse --verify --quiet "$ANYAGENTS_BASE_REF^{commit}" >/dev/null; then
      git checkout -B "$ANYAGENTS_BRANCH" "$ANYAGENTS_BASE_REF"
    else
      git checkout -b "$ANYAGENTS_BRANCH"
    fi
  fi
fi
"#;

    let wrapped_bootstrap = wrap_cloud_exec_command_for_zero_exit(bootstrap_script);
    let mut idempotency_key = next_exec_idempotency_key("bootstrap");
    let (mut status, mut raw) = post_exec_with_idempotency_retry(
        &exec_url,
        access_key,
        &json!({
            "idempotency_key": idempotency_key,
            "command": ["bash", "-lc", wrapped_bootstrap],
            "env": exec_env,
            "timeout": "180s",
        }),
    )?;
    if status == 409 && raw.contains("EXEC_OUTCOME_UNKNOWN") {
        std::thread::sleep(Duration::from_secs(2));
        idempotency_key = next_exec_idempotency_key("bootstrap_retry");
        (status, raw) = post_exec_with_idempotency_retry(
            &exec_url,
            access_key,
            &json!({
                "idempotency_key": idempotency_key,
                "command": ["bash", "-lc", wrapped_bootstrap],
                "env": exec_env,
                "timeout": "180s",
            }),
        )?;
    }
    if !(200..300).contains(&status) {
        return Err(format!("Bootstrap ExecLease HTTP {status}: {}", raw.trim()));
    }
    let val: Value =
        serde_json::from_str(&raw).map_err(|e| format!("invalid ExecLease JSON: {e}"))?;
    let raw_code = val.get("exit_code").and_then(Value::as_i64).unwrap_or(0);
    let code = normalize_cloud_exec_exit_code(raw_code);
    if code != 0 {
        let stderr_b64 = val.get("stderr").and_then(Value::as_str).unwrap_or("");
        let stderr = String::from_utf8_lossy(&base64_decode(stderr_b64)).into_owned();
        return Err(format!(
            "cloud sandbox worktree bootstrap exited with {code}: {}",
            stderr.trim()
        ));
    }
    Ok(())
}

pub fn release_persisted_sandbox_lease(config_dir: &Path) {
    let lock = config_dir_lease_lock(config_dir);
    let _guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let lease_file = config_dir.join("sylphx-sandbox-lease.json");
    let Ok(raw) = std::fs::read_to_string(&lease_file) else {
        return;
    };
    let Ok(val) = serde_json::from_str::<Value>(&raw) else {
        let _ = std::fs::remove_file(&lease_file);
        return;
    };
    let Some(lease_name) = val
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        let _ = std::fs::remove_file(&lease_file);
        return;
    };
    let Some(access_key) = resolve_sylphx_cloud_access_key(config_dir, &[]) else {
        return;
    };
    let api_url = resolve_cloud_api_url(&[]);
    let release_url = format!("{api_url}/v1/{lease_name}:release");
    if let Ok((status, _)) = curl_post_json(&release_url, Some(&access_key), &json!({}))
        && ((200..300).contains(&status) || status == 404)
    {
        let _ = std::fs::remove_file(&lease_file);
    }
}

#[allow(dead_code)]
pub(super) fn exec_in_cloud_sandbox(
    config_dir: &Path,
    env: &[(String, String)],
    cmd_str: &str,
) -> Result<String, String> {
    exec_in_cloud_sandbox_with_work_dir(config_dir, None, env, cmd_str)
}

pub(super) fn exec_in_cloud_sandbox_with_work_dir(
    config_dir: &Path,
    work_dir: Option<&Path>,
    env: &[(String, String)],
    cmd_str: &str,
) -> Result<String, String> {
    let (lease_name, _) =
        ensure_cloud_sandbox_lease_with_worktree(config_dir, work_dir, env, None, None, None)?;
    let access_key = resolve_sylphx_cloud_access_key(config_dir, env)
        .ok_or_else(|| "missing Sylphx Cloud access key".to_string())?;
    let api_url = resolve_cloud_api_url(env);
    let exec_url = format!("{api_url}/v1/{lease_name}:exec");
    let mut exec_env = serde_json::Map::new();
    if let Some(tok) = resolve_github_token(env) {
        exec_env.insert("GH_TOKEN".to_string(), json!(tok));
        exec_env.insert("GITHUB_TOKEN".to_string(), json!(tok));
    }
    let inner_cmd = format!(
        "export PATH=\"$HOME/.local/bin:$PATH\"; mkdir -p \"{CLOUD_WORKSPACE_DIR}\" && cd \"{CLOUD_WORKSPACE_DIR}\" && {cmd_str}"
    );
    let wrapped_cmd = wrap_cloud_exec_command_for_zero_exit(&inner_cmd);
    let idempotency_key = next_exec_idempotency_key("exec");
    let (status, raw) = post_exec_with_idempotency_retry(
        &exec_url,
        &access_key,
        &json!({
            "idempotency_key": idempotency_key,
            "command": ["bash", "-lc", wrapped_cmd],
            "working_directory": CLOUD_WORKSPACE_DIR,
            "env": exec_env,
            "timeout": "180s",
        }),
    )?;
    if status == 409 && raw.contains("EXEC_OUTCOME_UNKNOWN") {
        return Ok(
            "exit_code: 125\nstderr:\ncloud sandbox command stream interrupted (EXEC_OUTCOME_UNKNOWN); the command was already dispatched in /home/user/workspace — check workspace state (e.g. git status / ls) before retrying.\n"
                .to_string(),
        );
    }
    if !(200..300).contains(&status) {
        return Err(format!("ExecLease HTTP {status}: {}", raw.trim()));
    }
    let val: Value =
        serde_json::from_str(&raw).map_err(|e| format!("invalid ExecLease JSON: {e}"))?;
    let raw_code = val.get("exit_code").and_then(Value::as_i64).unwrap_or(0);
    let code = normalize_cloud_exec_exit_code(raw_code);
    let stdout_b64 = val.get("stdout").and_then(Value::as_str).unwrap_or("");
    let stderr_b64 = val.get("stderr").and_then(Value::as_str).unwrap_or("");
    let stdout = String::from_utf8_lossy(&base64_decode(stdout_b64)).into_owned();
    let stderr = String::from_utf8_lossy(&base64_decode(stderr_b64)).into_owned();
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
    Ok(truncate_tool_output_middle_utf8(
        combined, 30_000, 14_000, 14_000,
    ))
}

#[allow(dead_code)]
pub(super) fn read_file_in_cloud_sandbox(
    config_dir: &Path,
    env: &[(String, String)],
    path: &str,
) -> Result<String, String> {
    read_file_in_cloud_sandbox_with_work_dir(config_dir, None, env, path)
}

pub(super) fn read_file_in_cloud_sandbox_with_work_dir(
    config_dir: &Path,
    work_dir: Option<&Path>,
    env: &[(String, String)],
    path: &str,
) -> Result<String, String> {
    let (lease_name, _) =
        ensure_cloud_sandbox_lease_with_worktree(config_dir, work_dir, env, None, None, None)?;
    let access_key = resolve_sylphx_cloud_access_key(config_dir, env)
        .ok_or_else(|| "missing Sylphx Cloud access key".to_string())?;
    let api_url = resolve_cloud_api_url(env);
    let guest_path = resolve_cloud_sandbox_path(path);
    let url = format!("{api_url}/v1/{lease_name}:readFile");
    let (status, raw) = curl_post_json(&url, Some(&access_key), &json!({ "path": guest_path }))?;
    if !(200..300).contains(&status) {
        return Err(format!("ReadFile HTTP {status}: {}", raw.trim()));
    }
    let val: Value =
        serde_json::from_str(&raw).map_err(|e| format!("invalid ReadFile JSON: {e}"))?;
    let content_b64 = val.get("content").and_then(Value::as_str).unwrap_or("");
    Ok(String::from_utf8_lossy(&base64_decode(content_b64)).into_owned())
}

#[allow(dead_code)]
pub(super) fn write_file_in_cloud_sandbox(
    config_dir: &Path,
    env: &[(String, String)],
    path: &str,
    content: &str,
) -> Result<String, String> {
    write_file_in_cloud_sandbox_with_work_dir(config_dir, None, env, path, content)
}

pub(super) fn write_file_in_cloud_sandbox_with_work_dir(
    config_dir: &Path,
    work_dir: Option<&Path>,
    env: &[(String, String)],
    path: &str,
    content: &str,
) -> Result<String, String> {
    let (lease_name, _) =
        ensure_cloud_sandbox_lease_with_worktree(config_dir, work_dir, env, None, None, None)?;
    let access_key = resolve_sylphx_cloud_access_key(config_dir, env)
        .ok_or_else(|| "missing Sylphx Cloud access key".to_string())?;
    let api_url = resolve_cloud_api_url(env);
    let guest_path = resolve_cloud_sandbox_path(path);
    let url = format!("{api_url}/v1/{lease_name}:writeFile");
    let (status, raw) = curl_post_json(
        &url,
        Some(&access_key),
        &json!({
            "path": guest_path,
            "content": base64_encode(content.as_bytes()),
        }),
    )?;
    if !(200..300).contains(&status) {
        return Err(format!("WriteFile HTTP {status}: {}", raw.trim()));
    }
    Ok(format!(
        "wrote `{guest_path}` ({} bytes) in cloud sandbox `{lease_name}`",
        content.len()
    ))
}
