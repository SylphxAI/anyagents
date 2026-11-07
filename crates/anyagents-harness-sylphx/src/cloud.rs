//! Declarative `EnvironmentSpec` (`Sylphx Cloud Agents` `/v1/{env}/agents` and
//! `/v1/{env}/agent_sessions`) and Git repository/worktree resolution for
//! Whole-Agent-on-Cloud execution.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};

use serde_json::{Value, json};

use super::auth::{DEFAULT_SYLPHX_API_URL, resolve_sylphx_api_key};

pub const DEFAULT_SYLPHX_SANDBOX_PARENT: &str = "orgs/org_fs01afbdccgn8xdk1ghj5y47w0vtph/projects/prj_fs01nh48z2cee2ccqddykqeys3rvc4/envs/env_fs014mk148yf5ngawpe4zcey9rvq89";
pub const DEFAULT_SYLPHX_SANDBOX_SHAPE: &str = "linux-s";
pub const CLOUD_WORKSPACE_DIR: &str = "/home/user";

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

/// Builds a declarative `sylphx.agents.v1.EnvironmentSpec` JSON object for `Sylphx Cloud Agents`
/// (`POST /v1/{env}/agents` and `POST /v1/{env}/agent_sessions`) so the cloud runtime provisions
/// a persistent, general-purpose Cloud Agent computer (`template:agent`, `shape: "linux-s"`,
/// `idle_standby: "1800s"`) managed natively by `Sylphx Cloud`.
pub fn build_cloud_environment_spec(
    _config_dir: &Path,
    _work_dir: Option<&Path>,
    env: &[(String, String)],
) -> Value {
    let shape = resolve_sandbox_shape(env);
    json!({
        "shape": shape,
        "template": "template:agent",
        "ttl": "1800s",
        "idle_standby": "1800s",
    })
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
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
        ])
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
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
        ])
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
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
        ])
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
