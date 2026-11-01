//! Cloud Kata microVM Sandbox (`Sylphx Sandboxes` `/v1/{parent}/leases`) and declarative
//! `EnvironmentSpec` (`Sylphx Agents` BaaS) worktree management.
//!
//! Security Invariant:
//! - GitHub credentials (`GH_TOKEN` / `GITHUB_TOKEN`) are **never** written to disk inside the
//!   sandbox (`~/.git-credentials` and `~/.anyagents_cloud_env` are never created and any legacy
//!   files are purged). Instead, `git` uses an in-memory credential helper (`!f() { ... }`) that
//!   reads `$GH_TOKEN` from the ephemeral `:exec` process environment when needed.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{Value, json};

use super::auth::{
    DEFAULT_SYLPHX_API_URL, base64_decode, base64_encode, curl_get, curl_post_json,
    resolve_sylphx_api_key,
};

const DEFAULT_SYLPHX_SANDBOX_PARENT: &str = "orgs/org_fs01afbdccgn8xdk1ghj5y47w0vtph/projects/prj_fs01nh48z2cee2ccqddykqeys3rvc4/envs/env_fs014mk148yf5ngawpe4zcey9rvq89";
pub const CLOUD_WORKSPACE_DIR: &str = "/home/user/workspace";

#[derive(Debug, Clone, Default)]
pub struct CloudWorktreeSpec {
    pub repo_url: Option<String>,
    pub repo_slug: Option<String>,
    pub branch: Option<String>,
    pub base: Option<String>,
}

/// Builds a declarative `sylphx.agents.v1.EnvironmentSpec` JSON object for `Sylphx Agents`
/// BaaS (`POST /v1/{env}/agents` and `POST /v1/{env}/agent_sessions`) so the cloud runtime
/// provisions a Kata microVM sandbox (`template:agent`) with the agent's Git repository and
/// branch/revision mounted at `/home/user/workspace`.
pub fn build_cloud_environment_spec(
    config_dir: &Path,
    work_dir: Option<&Path>,
    env: &[(String, String)],
) -> Value {
    let wt = resolve_agent_git_worktree_spec(config_dir, work_dir, env, None, None, None);
    let mut spec = json!({
        "shape": "linux-xs",
        "template": "template:agent",
        "workspace_dir": CLOUD_WORKSPACE_DIR,
    });
    if let Some(uri) = wt.repo_url
        && let Some(obj) = spec.as_object_mut()
    {
        let mut repo_obj = serde_json::Map::new();
        repo_obj.insert("uri".to_string(), json!(uri));
        if let Some(slug) = wt.repo_slug {
            repo_obj.insert("slug".to_string(), json!(slug));
        }
        if let Some(rev) = wt.branch.clone().or(wt.base.clone()) {
            repo_obj.insert("revision".to_string(), json!(rev));
        }
        if let Some(br) = wt.branch {
            repo_obj.insert("branch".to_string(), json!(br));
        }
        if let Some(base) = wt.base {
            repo_obj.insert("base".to_string(), json!(base));
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

fn resolve_github_token(env: &[(String, String)]) -> Option<String> {
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
    if !crate::sandbox::is_isolated_test_env()
        && let Ok(out) = Command::new("gh")
            .args(["auth", "token"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
        && out.status.success()
    {
        let tok = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !tok.is_empty() {
            return Some(tok);
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
        Some("cloud" | "pod" | "kata" | "microvm" | "sandbox") => "cloud".to_string(),
        Some("local" | "host") => "local".to_string(),
        _ => {
            if config_dir.join("sylphx-sandbox-lease.json").is_file() {
                "cloud".to_string()
            } else {
                "local".to_string()
            }
        }
    }
}

pub(super) fn resolve_sylphx_cloud_access_key(
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

pub fn ensure_cloud_sandbox_lease_with_worktree(
    config_dir: &Path,
    work_dir: Option<&Path>,
    env: &[(String, String)],
    explicit_repo: Option<&str>,
    explicit_branch: Option<&str>,
    explicit_base: Option<&str>,
) -> Result<(String, String), String> {
    let access_key = resolve_sylphx_cloud_access_key(config_dir, env).ok_or_else(|| {
        "missing Sylphx Cloud access key; run `anyagents sign-in sylphx` first".to_string()
    })?;
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
        && let Ok(mut persisted) = serde_json::from_str::<Value>(&raw)
        && let Some(name) = persisted
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string)
    {
        let get_url = format!("{DEFAULT_SYLPHX_API_URL}/v1/{name}");
        if let Ok((200..=299, body)) = curl_get(&get_url, Some(&access_key))
            && let Ok(live) = serde_json::from_str::<Value>(&body)
        {
            let state = live
                .pointer("/status/state")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            if state == "ready" || state == "lease_state_ready" {
                let uid = live
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
                    bootstrap_cloud_sandbox_worktree(&access_key, &name, &wt_spec, env)?;
                    if let Some(obj) = persisted.as_object_mut() {
                        obj.insert("workspace_bootstrapped".to_string(), json!(true));
                        obj.insert("workspace_dir".to_string(), json!(CLOUD_WORKSPACE_DIR));
                        if let Some(ref slug) = wt_spec.repo_slug {
                            obj.insert("repo".to_string(), json!(slug));
                        }
                        if let Some(ref br) = wt_spec.branch {
                            obj.insert("branch".to_string(), json!(br));
                        }
                    }
                    if let Ok(pretty) = serde_json::to_string_pretty(&persisted) {
                        let _ = std::fs::write(&lease_file, format!("{pretty}\n"));
                    }
                }
                return Ok((name, uid));
            }
        }
    }

    let parent = env
        .iter()
        .find(|(k, v)| k == "SYLPHX_SANDBOX_PARENT" && !v.trim().is_empty())
        .map(|(_, v)| v.trim().to_string())
        .or_else(|| {
            std::env::var("SYLPHX_SANDBOX_PARENT")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| DEFAULT_SYLPHX_SANDBOX_PARENT.to_string());

    let create_url = format!("{DEFAULT_SYLPHX_API_URL}/v1/{parent}/leases");
    let req_body = json!({
        "spec": {
            "shape": "linux-xs",
            "kind": "general",
            "image": "template:agent",
            "network": {
                "egress": "allow"
            },
            "ttl": "1800s",
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

    // Poll until status.state == "ready" if not immediately ready
    let mut ready = lease_val
        .pointer("/status/state")
        .and_then(Value::as_str)
        .is_some_and(|s| {
            s.eq_ignore_ascii_case("ready") || s.eq_ignore_ascii_case("lease_state_ready")
        });
    if !ready {
        let get_url = format!("{DEFAULT_SYLPHX_API_URL}/v1/{lease_name}");
        for _ in 0..45 {
            std::thread::sleep(Duration::from_secs(1));
            if let Ok((200..=299, body)) = curl_get(&get_url, Some(&access_key))
                && let Ok(live) = serde_json::from_str::<Value>(&body)
            {
                let st = live
                    .pointer("/status/state")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_ascii_lowercase();
                lease_val = live;
                if st == "ready" || st == "lease_state_ready" {
                    ready = true;
                    break;
                }
                if st == "refused" || st == "ended" || st == "failed" {
                    return Err(format!(
                        "Cloud Sandbox Lease `{lease_name}` entered state `{st}`"
                    ));
                }
            }
        }
    }
    if !ready {
        return Err(format!(
            "Cloud Sandbox Lease `{lease_name}` did not become ready within 45s"
        ));
    }

    bootstrap_cloud_sandbox_worktree(&access_key, &lease_name, &wt_spec, env)?;

    if let Some(obj) = lease_val.as_object_mut() {
        obj.insert("workspace_bootstrapped".to_string(), json!(true));
        obj.insert("workspace_dir".to_string(), json!(CLOUD_WORKSPACE_DIR));
        if let Some(ref slug) = wt_spec.repo_slug {
            obj.insert("repo".to_string(), json!(slug));
        }
        if let Some(ref br) = wt_spec.branch {
            obj.insert("branch".to_string(), json!(br));
        }
    }
    if let Ok(pretty) = serde_json::to_string_pretty(&lease_val) {
        let _ = std::fs::write(&lease_file, format!("{pretty}\n"));
    }
    Ok((lease_name, uid))
}

fn bootstrap_cloud_sandbox_worktree(
    access_key: &str,
    lease_name: &str,
    wt_spec: &CloudWorktreeSpec,
    env: &[(String, String)],
) -> Result<(), String> {
    let exec_url = format!("{DEFAULT_SYLPHX_API_URL}/v1/{lease_name}:exec");
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
rm -f "$HOME/.git-credentials" "$HOME/.anyagents_cloud_env"
git config --global user.name "Kyle Tse"
git config --global user.email "hi@kylet.se"
git config --global init.defaultBranch main
git config --global --add safe.directory "$ANYAGENTS_WORKSPACE"
git config --global credential.helper '!f() { [ "$1" = get ] && [ -n "$GH_TOKEN" ] && printf "username=x-access-token\npassword=%s\n" "$GH_TOKEN"; }; f'
if [ -n "$ANYAGENTS_CLONE_URL" ] && [ ! -d "$ANYAGENTS_WORKSPACE/.git" ]; then
  git clone --filter=blob:none "$ANYAGENTS_CLONE_URL" "$ANYAGENTS_WORKSPACE"
fi
if [ -d "$ANYAGENTS_WORKSPACE/.git" ]; then
  cd "$ANYAGENTS_WORKSPACE"
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

    let (status, raw) = curl_post_json(
        &exec_url,
        Some(access_key),
        &json!({
            "command": ["bash", "-lc", bootstrap_script],
            "env": exec_env,
            "timeout": "180s",
        }),
    )?;
    if !(200..300).contains(&status) {
        return Err(format!("Bootstrap ExecLease HTTP {status}: {}", raw.trim()));
    }
    let val: Value =
        serde_json::from_str(&raw).map_err(|e| format!("invalid ExecLease JSON: {e}"))?;
    let code = val.get("exit_code").and_then(Value::as_i64).unwrap_or(0);
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
    let lease_file = config_dir.join("sylphx-sandbox-lease.json");
    let Ok(raw) = std::fs::read_to_string(&lease_file) else {
        return;
    };
    let _ = std::fs::remove_file(&lease_file);
    let Ok(val) = serde_json::from_str::<Value>(&raw) else {
        return;
    };
    let Some(lease_name) = val.get("name").and_then(Value::as_str) else {
        return;
    };
    let Some(access_key) = resolve_sylphx_cloud_access_key(config_dir, &[]) else {
        return;
    };
    let release_url = format!("{DEFAULT_SYLPHX_API_URL}/v1/{lease_name}:release");
    let _ = curl_post_json(&release_url, Some(&access_key), &json!({}));
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
    let exec_url = format!("{DEFAULT_SYLPHX_API_URL}/v1/{lease_name}:exec");
    let mut exec_env = serde_json::Map::new();
    let needs_git_auth = cmd_str.contains("git") || cmd_str.contains("gh");
    if needs_git_auth && let Some(tok) = resolve_github_token(env) {
        exec_env.insert("GH_TOKEN".to_string(), json!(tok));
        exec_env.insert("GITHUB_TOKEN".to_string(), json!(tok));
    }
    let wrapped_cmd = format!(
        "export PATH=\"$HOME/.local/bin:$PATH\"; mkdir -p \"{CLOUD_WORKSPACE_DIR}\" && cd \"{CLOUD_WORKSPACE_DIR}\" && {cmd_str}"
    );
    let (status, raw) = curl_post_json(
        &exec_url,
        Some(&access_key),
        &json!({
            "command": ["bash", "-lc", wrapped_cmd],
            "working_directory": CLOUD_WORKSPACE_DIR,
            "env": exec_env,
            "timeout": "120s",
        }),
    )?;
    if !(200..300).contains(&status) {
        return Err(format!("ExecLease HTTP {status}: {}", raw.trim()));
    }
    let val: Value =
        serde_json::from_str(&raw).map_err(|e| format!("invalid ExecLease JSON: {e}"))?;
    let code = val.get("exit_code").and_then(Value::as_i64).unwrap_or(0);
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
    if combined.len() > 32_000 {
        combined.truncate(32_000);
        combined.push_str("\n...[truncated]");
    }
    Ok(combined)
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
    let guest_path = resolve_cloud_sandbox_path(path);
    let url = format!("{DEFAULT_SYLPHX_API_URL}/v1/{lease_name}:readFile");
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
    let guest_path = resolve_cloud_sandbox_path(path);
    let url = format!("{DEFAULT_SYLPHX_API_URL}/v1/{lease_name}:writeFile");
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
