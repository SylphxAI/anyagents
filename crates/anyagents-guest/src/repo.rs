use std::path::{Path, PathBuf};

use super::{GuestRepoSpec, REPO_SPEC_FILE, spawn_command};

pub(crate) fn load_repo_spec(state_dir: &Path) -> Option<GuestRepoSpec> {
    let raw = std::fs::read_to_string(state_dir.join(REPO_SPEC_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Configure a git command with non-interactive prompting and optional
/// GitHub HTTPS token auth via `GH_TOKEN`/`GITHUB_TOKEN`.
fn configure_git_command(
    git_bin: &Path,
    repo: &str,
    gh_token: Option<&str>,
) -> Result<std::process::Command, String> {
    let mut cmd = spawn_command(git_bin, &[]).map_err(|e| format!("git spawn: {e}"))?;
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    if let Some(tok) = gh_token
        && (repo.starts_with("https://github.com/") || repo.starts_with("https://www.github.com/"))
    {
        cmd.env("ANYAGENTS_GIT_TOKEN", tok);
        cmd.arg("-c").arg(
            "credential.helper=!f() { echo username=x-access-token; echo password=\"$ANYAGENTS_GIT_TOKEN\"; }; f",
        );
    }
    Ok(cmd)
}

fn is_missing_remote_branch_error(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    (lower.contains("remote branch") && lower.contains("not found"))
        || lower.contains("not found in upstream")
}

/// Ensure the repository described by `spec` is checked out inside `work_dir`
/// when no `.git` metadata exists yet.
pub(crate) fn ensure_repo_checked_out(
    work_dir: &Path,
    spec: &GuestRepoSpec,
    git_override: Option<&Path>,
) -> Result<(), String> {
    let repo = spec.repo.trim();
    if repo.is_empty() || work_dir.join(".git").exists() {
        return Ok(());
    }
    let git_bin = match git_override {
        Some(p) => p.to_path_buf(),
        None => anyagents_toolchain::tool("git", "anyagents-guest.repo")
            .map(|r| r.path)
            .unwrap_or_else(|_| PathBuf::from("/usr/bin/git")),
    };
    let gh_token = std::env::var("GH_TOKEN")
        .ok()
        .or_else(|| std::env::var("GITHUB_TOKEN").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
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

    let mut cmd = configure_git_command(&git_bin, repo, gh_token.as_deref())?;
    cmd.arg("clone");
    if let Some(branch) = wanted_branch {
        cmd.arg("--branch").arg(branch);
    }
    cmd.arg(repo).arg(work_dir);
    let out = cmd.output().map_err(|e| format!("git clone failed: {e}"))?;
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
            let mut fallback = configure_git_command(&git_bin, repo, gh_token.as_deref())?;
            fallback.arg("clone");
            if let Some(base) = base_branch {
                fallback.arg("--branch").arg(base);
            }
            fallback.arg(repo).arg(work_dir);
            let fb_out = fallback
                .output()
                .map_err(|e| format!("git clone fallback failed: {e}"))?;
            if !fb_out.status.success() {
                return Err(format!(
                    "git clone `{repo}` failed: {}",
                    String::from_utf8_lossy(&fb_out.stderr).trim()
                ));
            }
            create_local_branch = Some(branch);
        } else {
            return Err(format!("git clone `{repo}` failed: {}", stderr.trim()));
        }
    }
    if let Some(branch) = create_local_branch {
        let mut co = spawn_command(&git_bin, &[]).map_err(|e| format!("git spawn: {e}"))?;
        co.current_dir(work_dir).args(["checkout", "-B", branch]);
        let co_out = co
            .output()
            .map_err(|e| format!("git checkout -B `{branch}` failed: {e}"))?;
        if !co_out.status.success() {
            return Err(format!(
                "git checkout -B `{branch}` failed: {}",
                String::from_utf8_lossy(&co_out.stderr).trim()
            ));
        }
    } else if let Some(base) = base_branch
        && wanted_branch.is_none()
    {
        let mut co = spawn_command(&git_bin, &[]).map_err(|e| format!("git spawn: {e}"))?;
        co.current_dir(work_dir).args(["checkout", base]);
        let co_out = co
            .output()
            .map_err(|e| format!("git checkout `{base}` failed: {e}"))?;
        if !co_out.status.success() {
            return Err(format!(
                "git checkout `{base}` failed: {}",
                String::from_utf8_lossy(&co_out.stderr).trim()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
