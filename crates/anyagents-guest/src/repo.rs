use std::path::{Path, PathBuf};
use std::process::Stdio;

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

pub(crate) fn ensure_gh_auth_setup_git_for_env(state_dir: &Path, env: &[(String, String)]) {
    let tok = find_token_in_pairs(env).or_else(|| {
        std::env::var("GH_TOKEN")
            .ok()
            .or_else(|| std::env::var("GITHUB_TOKEN").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    });
    ensure_gh_auth_setup_git(state_dir, tok.as_deref());
}

fn is_missing_remote_branch_error(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    (lower.contains("remote branch") && lower.contains("not found"))
        || lower.contains("not found in upstream")
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

    let mut cmd = configure_git_command(&git_bin, &repo, gh_token.as_deref())?;
    cmd.current_dir(clone_cwd).arg("clone");
    if let Some(branch) = wanted_branch {
        cmd.arg("--branch").arg(branch);
    }
    cmd.arg(&repo).arg(work_dir);
    let out = cmd
        .output()
        .map_err(|e| format_clone_error(&repo, &e.to_string(), gh_token.as_deref()))?;
    let mut create_local_branch: Option<&str> = None;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if let Some(branch) = wanted_branch
            && is_missing_remote_branch_error(&stderr)
        {
            // Agent branch does not exist on the remote yet (common for fresh
            // agent worktree branches). Clone the base/default branch and create
            // the local branch cleanly.
            if work_dir.exists() && !work_dir.join(".git").exists() {
                let _ = std::fs::remove_dir_all(work_dir);
                let _ = std::fs::create_dir_all(work_dir);
            }
            let mut fallback = configure_git_command(&git_bin, &repo, gh_token.as_deref())?;
            fallback.current_dir(clone_cwd).arg("clone");
            if let Some(base) = base_branch {
                fallback.arg("--branch").arg(base);
            }
            fallback.arg(&repo).arg(work_dir);
            let fb_out = fallback
                .output()
                .map_err(|e| format_clone_error(&repo, &e.to_string(), gh_token.as_deref()))?;
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
}
