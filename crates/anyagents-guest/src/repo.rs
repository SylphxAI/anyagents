use std::io::Read as _;
use std::net::ToSocketAddrs as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const DEFAULT_GIT_CLONE_TIMEOUT_SECS: u64 = 120;
const DNS_WAIT_MAX_ATTEMPTS: usize = 5;
const MAX_DNS_CLONE_RETRIES: usize = 2;

use super::{
    GuestRepoSpec, REPO_SPEC_FILE, load_guest_config, resolve_default_state_dir, spawn_command,
};

pub(crate) fn load_repo_spec(state_dir: &Path) -> Option<GuestRepoSpec> {
    let raw = std::fs::read_to_string(state_dir.join(REPO_SPEC_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Normalize GitHub `Org/Repo` shorthand to `https://github.com/Org/Repo.git`,
/// leaving full URLs, SSH remotes, and existing local paths untouched.
pub(crate) fn normalize_repo_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.contains("://")
        || trimmed.contains('@')
        || trimmed.starts_with('/')
        || trimmed.starts_with('.')
        || Path::new(trimmed).exists()
    {
        return trimmed.to_string();
    }
    if let Some((owner, repo)) = trimmed.split_once('/')
        && !repo.contains('/')
        && is_valid_github_segment(owner)
        && is_valid_github_segment(repo)
    {
        let clean = trimmed.trim_end_matches(".git");
        return format!("https://github.com/{clean}.git");
    }
    trimmed.to_string()
}

fn is_valid_github_segment(seg: &str) -> bool {
    !seg.is_empty()
        && seg != "."
        && seg != ".."
        && seg
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

pub(crate) fn is_github_https_url(repo: &str) -> bool {
    repo.starts_with("https://github.com/") || repo.starts_with("https://www.github.com/")
}

/// Redact tokens and embedded URL credentials before logging or surfacing errors.
pub(crate) fn redact_secrets(text: &str, token: Option<&str>) -> String {
    let mut out = text.to_string();
    if let Some(tok) = token.map(str::trim).filter(|t| !t.is_empty()) {
        out = out.replace(tok, "[REDACTED]");
    }
    for scheme in ["https://", "http://"] {
        out = redact_url_userinfo(&out, scheme);
    }
    for prefix in ["github_pat_", "ghp_", "gho_", "ghs_", "ghu_"] {
        out = redact_prefixed_tokens(&out, prefix);
    }
    out
}

fn redact_url_userinfo(input: &str, scheme: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find(scheme) {
        out.push_str(&rest[..pos + scheme.len()]);
        let after = &rest[pos + scheme.len()..];
        let end = after
            .find(|c: char| c.is_whitespace() || matches!(c, '\'' | '"' | ')' | ']' | '>' | ','))
            .unwrap_or(after.len());
        let authority_and_path = &after[..end];
        let host_boundary = authority_and_path
            .find('/')
            .unwrap_or(authority_and_path.len());
        if let Some(at_idx) = authority_and_path[..host_boundary].rfind('@') {
            out.push_str("[REDACTED]@");
            out.push_str(&authority_and_path[at_idx + 1..]);
        } else {
            out.push_str(authority_and_path);
        }
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

fn redact_prefixed_tokens(input: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find(prefix) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + prefix.len()..];
        let tok_len = after
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
            .count();
        if tok_len >= 8 {
            out.push_str("[REDACTED]");
            rest = &after[tok_len..];
        } else {
            out.push_str(prefix);
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

/// Build in-memory git environment variables for non-interactive cloning and
/// optional GitHub HTTPS authentication via `GIT_CONFIG_COUNT`.
///
/// Resets any inherited `credential.helper` first (`credential.helper=`) before
/// installing a scoped GitHub helper reading `$ANYAGENTS_GIT_TOKEN` from memory,
/// never writing plaintext tokens into `.git/config` or process argv.
pub(crate) fn git_credential_env_pairs(
    repo: &str,
    gh_token: Option<&str>,
) -> Vec<(String, String)> {
    let mut env = vec![
        ("GIT_TERMINAL_PROMPT".to_string(), "0".to_string()),
        ("GIT_ASKPASS".to_string(), "/bin/true".to_string()),
        ("GIT_HTTP_LOW_SPEED_LIMIT".to_string(), "1000".to_string()),
        ("GIT_HTTP_LOW_SPEED_TIME".to_string(), "30".to_string()),
    ];
    if let Some(tok) = gh_token.map(str::trim).filter(|t| !t.is_empty())
        && is_github_https_url(repo)
    {
        env.push(("ANYAGENTS_GIT_TOKEN".to_string(), tok.to_string()));
        env.push(("GIT_CONFIG_COUNT".to_string(), "2".to_string()));
        env.push((
            "GIT_CONFIG_KEY_0".to_string(),
            "credential.helper".to_string(),
        ));
        env.push(("GIT_CONFIG_VALUE_0".to_string(), String::new()));
        env.push((
            "GIT_CONFIG_KEY_1".to_string(),
            "credential.https://github.com.helper".to_string(),
        ));
        env.push((
            "GIT_CONFIG_VALUE_1".to_string(),
            "!f() { echo username=x-access-token; echo password=\"$ANYAGENTS_GIT_TOKEN\"; }; f"
                .to_string(),
        ));
    }
    env
}

/// Configure a git command with non-interactive prompting and optional in-memory
/// GitHub HTTPS token auth via `GIT_CONFIG_COUNT`.
fn configure_git_command(
    git_bin: &Path,
    repo: &str,
    gh_token: Option<&str>,
) -> Result<std::process::Command, String> {
    let mut cmd = spawn_command(git_bin, &[]).map_err(|e| format!("git spawn: {e}"))?;
    for (k, v) in git_credential_env_pairs(repo, gh_token) {
        cmd.env(k, v);
    }
    Ok(cmd)
}

fn find_token_in_pairs(pairs: &[(String, String)]) -> Option<String> {
    for key in ["GH_TOKEN", "GITHUB_TOKEN"] {
        if let Some((_, val)) = pairs.iter().rev().find(|(k, _)| k == key) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn resolve_github_token(work_dir: &Path) -> Option<String> {
    if let Some(tok) = std::env::var("GH_TOKEN")
        .ok()
        .or_else(|| std::env::var("GITHUB_TOKEN").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return Some(tok);
    }
    let mut state_candidates = vec![resolve_default_state_dir(None)];
    if let Some(parent) = work_dir.parent() {
        state_candidates.push(parent.join("state"));
    }
    for state_dir in state_candidates {
        if let Some(cfg) = load_guest_config(&state_dir)
            && let Some(tok) = find_token_in_pairs(&cfg.env)
        {
            return Some(tok);
        }
    }
    None
}

fn resolve_gh_binary() -> Option<PathBuf> {
    if let Ok(resolved) = anyagents_toolchain::tool("gh", "anyagents-guest.repo") {
        return Some(resolved.path);
    }
    for candidate in [
        "/usr/bin/gh",
        "/usr/local/bin/gh",
        "/home/user/.local/bin/gh",
    ] {
        let path = Path::new(candidate);
        if path.is_file() {
            return Some(path.to_path_buf());
        }
    }
    None
}

/// Non-interactively run `gh auth setup-git` once for `state_dir` when a GitHub
/// token is present and `gh` is installed, ignoring errors cleanly.
pub(crate) fn ensure_gh_auth_setup_git(state_dir: &Path, gh_token: Option<&str>) {
    sanitize_stale_credential_helpers(state_dir);
    let Some(tok) = gh_token.map(str::trim).filter(|t| !t.is_empty()) else {
        return;
    };
    let gitconfig = state_dir.join(".gitconfig");
    if let Ok(existing) = std::fs::read_to_string(&gitconfig)
        && existing.contains("gh auth git-credential")
    {
        return;
    }
    let Some(gh_bin) = resolve_gh_binary() else {
        return;
    };
    if let Ok(mut cmd) = spawn_command(&gh_bin, &[]) {
        let _ = cmd
            .args(["auth", "setup-git"])
            .env("HOME", state_dir)
            .env("GH_TOKEN", tok)
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Strip stale `helper = ...` lines in `<state_dir>/.gitconfig` referencing
/// nonexistent absolute binary paths (e.g. host paths from another machine).
pub(crate) fn sanitize_stale_credential_helpers(state_dir: &Path) {
    let path = state_dir.join(".gitconfig");
    let Ok(raw) = std::fs::read_to_string(&path) else {
        return;
    };
    let mut changed = false;
    let kept: Vec<&str> = raw
        .lines()
        .filter(|line| {
            let t = line.trim();
            if let Some(val) = t
                .strip_prefix("helper")
                .and_then(|r| r.trim_start().strip_prefix('='))
            {
                let cmd = val.trim().trim_start_matches('!').trim().trim_matches('"');
                let bin = cmd.split_whitespace().next().unwrap_or("");
                if bin.starts_with('/') && !Path::new(bin).exists() {
                    changed = true;
                    return false;
                }
            }
            true
        })
        .collect();
    if changed {
        let mut out = kept.join("\n");
        if !out.is_empty() {
            out.push('\n');
        }
        let _ = anyagents_harness::atomic_write_file(&path, out.as_bytes(), 0o644);
    }
}

/// Ensure `[user] name` and `email` are persisted in `<state_dir>/.gitconfig`
/// from `GIT_AUTHOR_NAME`/`GIT_AUTHOR_EMAIL` (or `ANYAGENTS_SANDBOX_ENV_*`).
pub(crate) fn ensure_git_user_identity(state_dir: &Path, env: &[(String, String)]) {
    let lookup = |keys: &[&str]| -> Option<String> {
        for k in keys {
            if let Some((_, v)) = env.iter().rev().find(|(ek, _)| ek == k)
                && !v.trim().is_empty()
            {
                return Some(v.trim().to_string());
            }
            if let Ok(v) = std::env::var(k)
                && !v.trim().is_empty()
            {
                return Some(v.trim().to_string());
            }
        }
        None
    };
    let name = lookup(&[
        "GIT_AUTHOR_NAME",
        "GIT_COMMITTER_NAME",
        "ANYAGENTS_SANDBOX_ENV_GIT_AUTHOR_NAME",
    ]);
    let email = lookup(&[
        "GIT_AUTHOR_EMAIL",
        "GIT_COMMITTER_EMAIL",
        "ANYAGENTS_SANDBOX_ENV_GIT_AUTHOR_EMAIL",
    ]);
    if name.is_none() && email.is_none() {
        return;
    }
    let git_bin = anyagents_toolchain::tool("git", "anyagents-guest.repo")
        .map(|r| r.path)
        .unwrap_or_else(|_| PathBuf::from("/usr/bin/git"));
    for (key, val) in [("user.name", name), ("user.email", email)] {
        if let Some(v) = val
            && let Ok(mut cmd) = spawn_command(&git_bin, &[])
        {
            let _ = cmd
                .args(["config", "--global", key, &v])
                .env("HOME", state_dir)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

pub(crate) fn ensure_gh_auth_setup_git_for_env(state_dir: &Path, env: &[(String, String)]) {
    sanitize_stale_credential_helpers(state_dir);
    ensure_git_user_identity(state_dir, env);
    let tok = find_token_in_pairs(env).or_else(|| {
        std::env::var("GH_TOKEN")
            .ok()
            .or_else(|| std::env::var("GITHUB_TOKEN").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    });
    ensure_gh_auth_setup_git(state_dir, tok.as_deref());
}

fn clone_timeout() -> Duration {
    let secs = std::env::var("ANYAGENTS_GIT_CLONE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(DEFAULT_GIT_CLONE_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

fn run_git_with_timeout(cmd: &mut Command, timeout: Duration) -> Result<Output, String> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| e.to_string())?;
    let mut out_pipe = child.stdout.take();
    let mut err_pipe = child.stderr.take();
    let out_t = std::thread::spawn(move || {
        let mut b = Vec::new();
        if let Some(ref mut p) = out_pipe {
            let _ = p.read_to_end(&mut b);
        }
        b
    });
    let err_t = std::thread::spawn(move || {
        let mut b = Vec::new();
        if let Some(ref mut p) = err_pipe {
            let _ = p.read_to_end(&mut b);
        }
        b
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait().map_err(|e| e.to_string())? {
            Some(st) => break st,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                let stderr = String::from_utf8_lossy(&err_t.join().unwrap_or_default())
                    .trim()
                    .to_string();
                let _ = out_t.join();
                return Err(if stderr.is_empty() {
                    format!("git clone timed out after {}s", timeout.as_secs())
                } else {
                    format!("git clone timed out after {}s: {stderr}", timeout.as_secs())
                });
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    Ok(Output {
        status,
        stdout: out_t.join().unwrap_or_default(),
        stderr: err_t.join().unwrap_or_default(),
    })
}

fn is_missing_remote_branch_error(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    (lower.contains("remote branch") && lower.contains("not found"))
        || lower.contains("not found in upstream")
}

/// Extract the bare hostname from an `http://` or `https://` repository URL,
/// stripping any embedded `user:pass@` userinfo and `:port`.
pub(crate) fn extract_http_repo_host(repo: &str) -> Option<String> {
    let rest = repo
        .trim()
        .strip_prefix("https://")
        .or_else(|| repo.trim().strip_prefix("http://"))?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, h)| h)
        .unwrap_or(authority);
    let host = if let Some(bracketed) = host_port.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or("")
    } else {
        host_port.split(':').next().unwrap_or("")
    }
    .trim();
    (!host.is_empty()).then(|| host.to_string())
}

fn is_transient_dns_error(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    lower.contains("could not resolve host")
        || lower.contains("temporary failure in name resolution")
}

fn is_filter_unsupported_error(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    lower.contains("filtering not recognized")
        || lower.contains("does not support filter")
        || lower.contains("invalid filter-spec")
        || lower.contains("uploadpack.allowfilter")
        || lower.contains("unsupported capability")
}

/// Bounded cold-boot DNS readiness check (linear backoff capped ~10s total for 5 attempts).
fn wait_for_dns_with<F>(max_attempts: usize, base_backoff: Duration, mut resolve: F) -> bool
where
    F: FnMut() -> bool,
{
    for attempt in 1..=max_attempts.max(1) {
        if resolve() {
            return true;
        }
        if attempt < max_attempts {
            std::thread::sleep(base_backoff.saturating_mul(attempt as u32));
        }
    }
    false
}

fn wait_for_dns_host(host: &str) -> bool {
    wait_for_dns_with(DNS_WAIT_MAX_ATTEMPTS, Duration::from_secs(1), || {
        (host, 443)
            .to_socket_addrs()
            .is_ok_and(|mut it| it.next().is_some())
    })
}

struct CloneContext<'a> {
    git_bin: &'a Path,
    clone_cwd: &'a Path,
    work_dir: &'a Path,
    repo: &'a str,
    gh_token: Option<&'a str>,
    shallow: bool,
    timeout: Duration,
}

fn prepare_clean_clone_dir(work_dir: &Path) {
    if work_dir.exists() && !work_dir.join(".git").exists() {
        let _ = std::fs::remove_dir_all(work_dir);
    }
    if let Some(parent) = work_dir.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::create_dir_all(work_dir);
}

fn run_clone_attempt(
    ctx: &CloneContext<'_>,
    branch: Option<&str>,
    mut use_filter: bool,
) -> Result<Output, String> {
    let mut dns_retries = 0usize;
    loop {
        let mut cmd = configure_git_command(ctx.git_bin, ctx.repo, ctx.gh_token)?;
        cmd.current_dir(ctx.clone_cwd).arg("clone");
        if ctx.shallow {
            if use_filter {
                cmd.arg("--filter=blob:none");
            }
            cmd.args(["--depth", "1", "--single-branch"]);
        }
        if let Some(b) = branch {
            cmd.arg("--branch").arg(b);
        }
        cmd.arg(ctx.repo).arg(ctx.work_dir);
        let out = run_git_with_timeout(&mut cmd, ctx.timeout)?;
        if out.status.success() {
            return Ok(out);
        }
        let stderr = String::from_utf8_lossy(&out.stderr);
        if ctx.shallow && use_filter && is_filter_unsupported_error(&stderr) {
            use_filter = false;
            prepare_clean_clone_dir(ctx.work_dir);
            continue;
        }
        if is_transient_dns_error(&stderr) && dns_retries < MAX_DNS_CLONE_RETRIES {
            dns_retries += 1;
            std::thread::sleep(Duration::from_secs((dns_retries as u64) * 2));
            prepare_clean_clone_dir(ctx.work_dir);
            continue;
        }
        return Ok(out);
    }
}

fn format_clone_error(repo: &str, detail: &str, gh_token: Option<&str>) -> String {
    let safe_repo = redact_secrets(repo, gh_token);
    let safe_detail = redact_secrets(detail.trim(), gh_token);
    format!("repo clone failed for {safe_repo}: {safe_detail}")
}

/// Ensure the repository described by `spec` is checked out inside `work_dir`
/// when no `.git` metadata exists yet.
pub(crate) fn ensure_repo_checked_out(
    work_dir: &Path,
    spec: &GuestRepoSpec,
    git_override: Option<&Path>,
) -> Result<(), String> {
    let raw_repo = spec.repo.trim();
    if raw_repo.is_empty() || work_dir.join(".git").exists() {
        return Ok(());
    }
    let repo = normalize_repo_url(raw_repo);
    let git_bin = match git_override {
        Some(p) => p.to_path_buf(),
        None => anyagents_toolchain::tool("git", "anyagents-guest.repo")
            .map(|r| r.path)
            .unwrap_or_else(|_| PathBuf::from("/usr/bin/git")),
    };
    let gh_token = resolve_github_token(work_dir);
    let state_dir = resolve_default_state_dir(None);
    ensure_gh_auth_setup_git(&state_dir, gh_token.as_deref());

    let wanted_branch = spec
        .branch
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let base_branch = spec
        .base
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    // Always run `git clone` from the parent directory (or `/`), never from `work_dir`
    // itself: when a first `git clone --branch <missing>` fails or cleans up `work_dir`,
    // inheriting `work_dir` as CWD unlinks the child's CWD inode and causes the fallback
    // clone to abort with `fatal: Unable to read current working directory`.
    let clone_cwd = work_dir
        .parent()
        .filter(|p| p.is_dir())
        .unwrap_or_else(|| Path::new("/"));

    let timeout = clone_timeout();
    let shallow = std::env::var("ANYAGENTS_GIT_CLONE_FULL").ok().as_deref() != Some("1");
    let http_host = extract_http_repo_host(&repo);
    if let Some(ref host) = http_host {
        let _ = wait_for_dns_host(host);
    }
    let use_filter = shallow
        && http_host.is_some()
        && std::env::var("ANYAGENTS_GIT_CLONE_NO_FILTER")
            .ok()
            .as_deref()
            != Some("1");
    let ctx = CloneContext {
        git_bin: &git_bin,
        clone_cwd,
        work_dir,
        repo: &repo,
        gh_token: gh_token.as_deref(),
        shallow,
        timeout,
    };
    let out = run_clone_attempt(&ctx, wanted_branch.or(base_branch), use_filter)
        .map_err(|e| format_clone_error(&repo, &e, gh_token.as_deref()))?;
    let mut create_local_branch: Option<&str> = None;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if let Some(branch) = wanted_branch
            && is_missing_remote_branch_error(&stderr)
        {
            prepare_clean_clone_dir(work_dir);
            let fb_out = run_clone_attempt(&ctx, base_branch, use_filter)
                .map_err(|e| format_clone_error(&repo, &e, gh_token.as_deref()))?;
            if !fb_out.status.success() {
                return Err(format_clone_error(
                    &repo,
                    &String::from_utf8_lossy(&fb_out.stderr),
                    gh_token.as_deref(),
                ));
            }
            create_local_branch = Some(branch);
        } else {
            return Err(format_clone_error(&repo, &stderr, gh_token.as_deref()));
        }
    }
    // If our own process CWD was unlinked when `work_dir` was recreated during clone,
    // re-anchor it to the freshly cloned `work_dir` so downstream commands inherit a valid CWD.
    if std::env::current_dir().is_err() {
        let _ = std::env::set_current_dir(work_dir);
    }
    if let Some(branch) = create_local_branch {
        let mut co = spawn_command(&git_bin, &[]).map_err(|e| format!("git spawn: {e}"))?;
        co.current_dir(work_dir).args(["checkout", "-B", branch]);
        let co_out = co.output().map_err(|e| {
            format_clone_error(
                &repo,
                &format!("git checkout -B `{branch}`: {e}"),
                gh_token.as_deref(),
            )
        })?;
        if !co_out.status.success() {
            return Err(format_clone_error(
                &repo,
                &format!(
                    "git checkout -B `{branch}`: {}",
                    String::from_utf8_lossy(&co_out.stderr).trim()
                ),
                gh_token.as_deref(),
            ));
        }
    } else if let Some(base) = base_branch
        && wanted_branch.is_none()
    {
        let mut co = spawn_command(&git_bin, &[]).map_err(|e| format!("git spawn: {e}"))?;
        co.current_dir(work_dir).args(["checkout", base]);
        let co_out = co.output().map_err(|e| {
            format_clone_error(
                &repo,
                &format!("git checkout `{base}`: {e}"),
                gh_token.as_deref(),
            )
        })?;
        if !co_out.status.success() {
            return Err(format_clone_error(
                &repo,
                &format!(
                    "git checkout `{base}`: {}",
                    String::from_utf8_lossy(&co_out.stderr).trim()
                ),
                gh_token.as_deref(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn normalize_repo_url_expands_github_shorthand_and_preserves_urls() {
        assert_eq!(
            normalize_repo_url("Org/PrivateRepo"),
            "https://github.com/Org/PrivateRepo.git"
        );
        assert_eq!(
            normalize_repo_url("Org/PrivateRepo.git"),
            "https://github.com/Org/PrivateRepo.git"
        );
        assert_eq!(
            normalize_repo_url("https://github.com/Org/PrivateRepo.git"),
            "https://github.com/Org/PrivateRepo.git"
        );
        assert_eq!(
            normalize_repo_url("git@github.com:Org/PrivateRepo.git"),
            "git@github.com:Org/PrivateRepo.git"
        );
        assert_eq!(normalize_repo_url("/tmp/local-repo"), "/tmp/local-repo");
    }

    #[test]
    fn git_credential_env_resets_helper_and_uses_in_memory_config_for_github() {
        let synthetic_token = format!("{}_{}", "ghp", "syntheticUnitTestValue12345");
        let env: BTreeMap<String, String> = git_credential_env_pairs(
            "https://github.com/Org/PrivateRepo.git",
            Some(&synthetic_token),
        )
        .into_iter()
        .collect();
        assert_eq!(
            env.get("GIT_TERMINAL_PROMPT").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            env.get("GIT_ASKPASS").map(String::as_str),
            Some("/bin/true")
        );
        assert_eq!(env.get("GIT_CONFIG_COUNT").map(String::as_str), Some("2"));
        assert_eq!(
            env.get("GIT_CONFIG_KEY_0").map(String::as_str),
            Some("credential.helper")
        );
        assert_eq!(env.get("GIT_CONFIG_VALUE_0").map(String::as_str), Some(""));
        assert_eq!(
            env.get("GIT_CONFIG_KEY_1").map(String::as_str),
            Some("credential.https://github.com.helper")
        );
        assert!(
            env.get("GIT_CONFIG_VALUE_1")
                .is_some_and(|v| v.contains("ANYAGENTS_GIT_TOKEN") && !v.contains(&synthetic_token))
        );
        assert_eq!(
            env.get("ANYAGENTS_GIT_TOKEN").map(String::as_str),
            Some(synthetic_token.as_str())
        );

        // Non-GitHub remotes must never receive GitHub token config.
        let other: BTreeMap<String, String> = git_credential_env_pairs(
            "https://gitlab.example.invalid/org/repo.git",
            Some(&synthetic_token),
        )
        .into_iter()
        .collect();
        assert!(!other.contains_key("ANYAGENTS_GIT_TOKEN"));
        assert!(!other.contains_key("GIT_CONFIG_COUNT"));
    }

    #[test]
    fn redact_secrets_strips_tokens_and_embedded_credentials() {
        let tok = format!("{}_{}", "ghp", "syntheticRedactionCheck9876");
        let pat = format!("{}_{}", "github_pat", "syntheticPatCheck123456");
        let url_with_tok = format!("https://x-access-token:{tok}@github.com/Org/PrivateRepo.git");
        let url_with_pw = format!(
            "https://{}:{}@github.com/Org/PrivateRepo.git",
            "user", "synthetic_pw_123"
        );
        let stderr =
            format!("fatal: Authentication failed for '{url_with_pw}' using {tok} and {pat}");
        let err = format_clone_error(&url_with_tok, &stderr, Some(&tok));
        assert!(err.starts_with("repo clone failed for "), "{err}");
        assert!(!err.contains(&tok), "raw token leaked: {err}");
        assert!(
            !err.contains("synthetic_pw_123"),
            "URL password leaked: {err}"
        );
        assert!(!err.contains(&pat), "PAT leaked: {err}");
        assert!(err.contains("[REDACTED]"), "{err}");
    }

    #[test]
    fn ensure_repo_checked_out_fails_closed_immediately_with_redacted_error() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("workspace");
        std::fs::create_dir_all(&work).unwrap();
        let fake_git = dir.path().join("fake-git");
        let leak_token = format!("{}_{}", "ghp", "syntheticLeakCheck99999");
        let script = format!(
            "#!/bin/sh\necho \"fatal: Authentication failed for 'https://x-access-token:{leak_token}@github.com/Org/PrivateRepo.git'\" >&2\nexit 128\n"
        );
        std::fs::write(&fake_git, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&fake_git, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let spec = GuestRepoSpec {
            repo: "Org/PrivateRepo".to_string(),
            branch: Some("main".to_string()),
            base: None,
        };
        let err = ensure_repo_checked_out(&work, &spec, Some(&fake_git))
            .expect_err("auth failure must fail closed immediately");
        assert!(
            err.starts_with("repo clone failed for https://github.com/Org/PrivateRepo.git:"),
            "{err}"
        );
        assert!(!err.contains(&leak_token), "token must be redacted: {err}");
        assert!(err.contains("[REDACTED]"), "{err}");
    }

    #[test]
    fn ensure_repo_checked_out_creates_local_branch_when_remote_branch_missing() {
        let dir = tempfile::tempdir().unwrap();
        let origin = dir.path().join("origin.git");
        let seed = dir.path().join("seed");
        let work = dir.path().join("workspace");
        let run_git = |cwd: &Path, args: &[&str]| {
            let out = std::process::Command::new("git")
                .current_dir(cwd)
                .args(args)
                .env("GIT_AUTHOR_NAME", "Test")
                .env("GIT_AUTHOR_EMAIL", "test@example.com")
                .env("GIT_COMMITTER_NAME", "Test")
                .env("GIT_COMMITTER_EMAIL", "test@example.com")
                .output()
                .expect("git invocation");
            assert!(
                out.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        std::fs::create_dir_all(&origin).unwrap();
        std::fs::create_dir_all(&seed).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        run_git(&origin, &["init", "--bare", "--initial-branch=main"]);
        run_git(&seed, &["init", "--initial-branch=main"]);
        std::fs::write(seed.join("README.md"), "hello\n").unwrap();
        run_git(&seed, &["add", "README.md"]);
        run_git(&seed, &["commit", "-m", "init"]);
        run_git(
            &seed,
            &["remote", "add", "origin", origin.to_str().unwrap()],
        );
        run_git(&seed, &["push", "-u", "origin", "main"]);

        let spec = GuestRepoSpec {
            repo: origin.to_str().unwrap().to_string(),
            branch: Some("agent/fresh-local-branch".to_string()),
            base: Some("main".to_string()),
        };
        ensure_repo_checked_out(&work, &spec, Some(Path::new("git")))
            .expect("missing remote branch should fall back to base and create local branch");
        assert_eq!(
            run_git(&work, &["rev-parse", "--abbrev-ref", "HEAD"]),
            "agent/fresh-local-branch"
        );
        assert_eq!(
            std::fs::read_to_string(work.join("README.md")).unwrap(),
            "hello\n"
        );
    }

    #[test]
    fn sanitizes_stale_helpers_sets_git_identity_and_enforces_clone_timeout() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path();
        std::fs::write(
            state.join(".gitconfig"),
            "[credential \"https://github.com\"]\n\thelper = !/nonexistent/bin/gh auth git-credential\n\thelper = !/bin/sh -c true\n",
        )
        .unwrap();
        let env = vec![
            ("GIT_AUTHOR_NAME".into(), "OSS Bot".into()),
            ("GIT_AUTHOR_EMAIL".into(), "bot@example.invalid".into()),
        ];
        sanitize_stale_credential_helpers(state);
        ensure_git_user_identity(state, &env);
        let cfg = std::fs::read_to_string(state.join(".gitconfig")).unwrap();
        assert!(!cfg.contains("/nonexistent/bin/gh"), "{cfg}");
        assert!(
            cfg.contains("OSS Bot") && cfg.contains("bot@example.invalid"),
            "{cfg}"
        );

        let mut slow = Command::new("/bin/sh");
        slow.args(["-c", "sleep 5"]);
        let err = run_git_with_timeout(&mut slow, Duration::from_millis(120)).unwrap_err();
        assert!(err.contains("timed out"), "{err}");
    }

    #[test]
    fn extracts_http_repo_host_and_detects_dns_and_filter_errors() {
        assert_eq!(
            extract_http_repo_host("https://github.com/Org/Repo.git").as_deref(),
            Some("github.com")
        );
        assert_eq!(
            extract_http_repo_host("https://x-access-token:secret@github.com:443/Org/Repo.git")
                .as_deref(),
            Some("github.com")
        );
        assert_eq!(
            extract_http_repo_host("http://git.example.invalid:8080/team/app").as_deref(),
            Some("git.example.invalid")
        );
        assert_eq!(
            extract_http_repo_host("https://[::1]:8443/org/repo").as_deref(),
            Some("::1")
        );
        assert_eq!(extract_http_repo_host("/tmp/local-bare.git"), None);
        assert_eq!(extract_http_repo_host("git@github.com:Org/Repo.git"), None);

        assert!(is_transient_dns_error(
            "fatal: unable to access 'https://github.com/a/b.git/': Could not resolve host: github.com"
        ));
        assert!(is_transient_dns_error(
            "Temporary failure in name resolution"
        ));
        assert!(!is_transient_dns_error("Repository not found"));

        for msg in [
            "warning: filtering not recognized by server, ignoring",
            "fatal: server does not support filter",
            "fatal: invalid filter-spec 'blob:none'",
            "error: uploadpack.allowFilter is not enabled",
            "fatal: unsupported capability: filter",
        ] {
            assert!(is_filter_unsupported_error(msg), "{msg}");
        }
        assert!(!is_filter_unsupported_error(
            "Permission denied (publickey)"
        ));

        let mut tries = 0usize;
        assert!(wait_for_dns_with(5, Duration::from_millis(5), || {
            tries += 1;
            tries >= 3
        }));
        assert_eq!(tries, 3);
        let mut exhausted = 0usize;
        assert!(!wait_for_dns_with(3, Duration::from_millis(2), || {
            exhausted += 1;
            false
        }));
        assert_eq!(exhausted, 3);
    }

    #[cfg(unix)]
    #[test]
    fn partial_filter_clone_falls_back_cleanly_when_server_rejects_filter() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let fake_git = tmp.path().join("fake-git");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(
            &fake_git,
            "#!/bin/sh\nfor a in \"$@\"; do\n  if [ \"$a\" = \"--filter=blob:none\" ]; then\n    echo 'fatal: server does not support filter' >&2\n    exit 128\n  fi\ndone\neval \"last=\\$$#\"\nmkdir -p \"$last/.git\"\nexit 0\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake_git, std::fs::Permissions::from_mode(0o755)).unwrap();

        let ctx = CloneContext {
            git_bin: &fake_git,
            clone_cwd: tmp.path(),
            work_dir: &work,
            repo: "https://example.invalid/org/repo.git",
            gh_token: None,
            shallow: true,
            timeout: Duration::from_secs(5),
        };
        let out = run_clone_attempt(&ctx, None, true).unwrap();
        assert!(
            out.status.success(),
            "expected fallback clone without --filter=blob:none"
        );
        assert!(work.join(".git").is_dir());
    }
}
