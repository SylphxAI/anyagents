//! External checkout provider contract. The explicitly configured executable accepts
//! `fetch <source> <ref>` and `worktree <source> <target> <branch> [base]`.
//! No host-specific provider or install location is selected implicitly.
use std::path::{Path, PathBuf};
use std::process::Command;

use anyagents_core::Error;

pub(crate) struct Consumer {
    executable: PathBuf,
    git_pool_dir: Option<PathBuf>,
}

impl Consumer {
    pub(crate) fn configured() -> Self {
        Self {
            executable: anyagents_toolchain::tool("checkout-provider", "checkout")
                .map(|tool| tool.path)
                .unwrap_or_default(),
            git_pool_dir: anyagents_core::configured_git_pool_dir(),
        }
    }

    fn run(&self, repo: &str, args: &[&std::ffi::OsStr]) -> Result<(), Error> {
        if self.executable.as_os_str().is_empty() {
            return Err(Error::WorktreeProvisioning {
                repo: repo.into(),
                message: "configure ANYAGENTS_CHECKOUT_PROVIDER_BIN with a checkout provider or ANYAGENTS_GIT_POOL_DIR with a bare Git pool"
                    .into(),
            });
        }
        let mut cmd = Command::new(&self.executable);
        cmd.args(args);
        if let Some(pool) = &self.git_pool_dir {
            cmd.env(anyagents_core::GIT_POOL_DIR_ENV, pool);
        }
        let out = cmd.output().map_err(|error| Error::WorktreeProvisioning {
            repo: repo.into(),
            message: format!("{}: {error}", self.executable.display()),
        })?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let stdout = String::from_utf8_lossy(&out.stdout);
            return Err(Error::WorktreeProvisioning {
                repo: repo.into(),
                message: format!(
                    "{}: {}",
                    out.status,
                    if stderr.trim().is_empty() {
                        stdout.trim()
                    } else {
                        stderr.trim()
                    }
                ),
            });
        }
        Ok(())
    }

    fn fetch(&self, repo: &str, base: &str) -> Result<(), Error> {
        if self.executable.as_os_str().is_empty()
            && let Some(pool_root) = &self.git_pool_dir
        {
            return pool_fetch(pool_root, repo, base);
        }
        self.run(repo, &["fetch".as_ref(), repo.as_ref(), base.as_ref()])
    }

    fn checkout(
        &self,
        repo: &str,
        target: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), Error> {
        if self.executable.as_os_str().is_empty()
            && let Some(pool_root) = &self.git_pool_dir
        {
            return pool_checkout(pool_root, repo, target, branch, base);
        }
        let mut args = vec![
            "worktree".as_ref(),
            repo.as_ref(),
            target.as_os_str(),
            branch.as_ref(),
        ];
        if let Some(base) = base {
            args.push(base.as_ref());
        }
        self.run(repo, &args)
    }

    pub(crate) fn remote(
        &self,
        raw: &str,
        target: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), Error> {
        let repo = github_repo(raw).unwrap_or_else(|| raw.to_string());
        if repo.trim().is_empty() {
            return Err(Error::WorktreeSourceUnpublished {
                source_repo: raw.into(),
                message: "repository source is empty".into(),
            });
        }
        let start = base.map(|base| {
            if base == "HEAD"
                || base.starts_with("origin/")
                || is_commit_id(base)
                || base.starts_with("refs/tags/")
            {
                base.to_string()
            } else if let Some(branch) = base.strip_prefix("refs/heads/") {
                format!("origin/{branch}")
            } else {
                format!("origin/{base}")
            }
        });
        if let Some(base) = base.filter(|b| *b != "HEAD" && *b != "origin/HEAD" && !is_commit_id(b))
        {
            self.fetch(&repo, base.strip_prefix("origin/").unwrap_or(base))?;
        }
        self.checkout(&repo, target, branch, start.as_deref())
    }

    pub(crate) fn local(
        &self,
        source: &Path,
        target: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), Error> {
        crate::worktree_access::validate_git_config(source)?;
        let refuse = |message: String| Error::WorktreeSourceUnpublished {
            source_repo: source.display().to_string(),
            message,
        };
        let git_tool =
            anyagents_toolchain::tool("git", "checkout").map_err(|e| refuse(e.to_string()))?;
        let git = |args: &[&str]| -> Result<String, Error> {
            let out = git_tool
                .command()
                .args([
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "core.hooksPath=/dev/null",
                ])
                .arg("-C")
                .arg(source)
                .args(args)
                .output()
                .map_err(|e| refuse(e.to_string()))?;
            if !out.status.success() {
                return Err(refuse(
                    String::from_utf8_lossy(&out.stderr).trim().to_string(),
                ));
            }
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
        };
        let mut remotes: Vec<_> = git(&["remote"])?.lines().map(str::to_string).collect();
        remotes.sort_by_key(|name| (name != "origin", name.clone()));
        let (remote, repo) = remotes
            .into_iter()
            .find_map(|name| {
                let url = git(&["config", "--get", &format!("remote.{name}.url")]).ok()?;
                (!url.trim().is_empty()).then(|| (name, github_remote_repo(&url).unwrap_or(url)))
            })
            .ok_or_else(|| refuse("no remote is configured".into()))?;
        let head = git(&["rev-parse", "--verify", "HEAD^{commit}"])?;
        let selected = git(&[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{}^{{commit}}", base.unwrap_or("HEAD")),
        ])?;
        // Read live advertised refs, not potentially stale origin/* tracking refs.
        let refs = git(&["ls-remote", "--heads", "--tags", &remote])?;
        let reachable_ref = |commit: &str| -> Option<String> {
            refs.lines()
                .filter_map(|line| line.split_once('\t'))
                .find_map(|(tip, name)| {
                    if name.ends_with("^{}") {
                        return None;
                    }
                    let reachable = tip == commit
                        || git_tool
                            .command()
                            .args([
                                "-c",
                                "core.fsmonitor=false",
                                "-c",
                                "core.hooksPath=/dev/null",
                            ])
                            .arg("-C")
                            .arg(source)
                            .args(["merge-base", "--is-ancestor", commit, tip])
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null())
                            .status()
                            .is_ok_and(|s| s.success());
                    reachable.then(|| name.to_string())
                })
        };
        reachable_ref(&head).ok_or_else(|| refuse("HEAD is not reachable from the remote's advertised refs; push HEAD first (fetch the remote if local history is stale)".into()))?;
        let reference = reachable_ref(&selected).ok_or_else(|| {
            refuse(
                "requested base is not reachable from the remote's advertised refs; push it first"
                    .into(),
            )
        })?;
        // A published feature commit may not be in the hub's default branch yet.
        self.fetch(&repo, &reference)?;
        self.checkout(&repo, target, branch, Some(&selected))
    }
}

fn is_safe_slug_part(part: &str) -> bool {
    !part.is_empty()
        && part != "."
        && !part.contains("..")
        && part
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn resolve_pool_slot(pool_root: &Path, raw: &str) -> Result<(PathBuf, PathBuf, String), Error> {
    let fail = |message: String| Error::WorktreeProvisioning {
        repo: raw.into(),
        message,
    };
    let trimmed = raw.trim().trim_end_matches('/');
    let (org, name, remote_url) = if let Some(slug) = github_repo(trimmed) {
        let (org, name) = slug
            .split_once('/')
            .ok_or_else(|| fail("invalid repository slug".into()))?;
        let remote_url = if trimmed.starts_with("https://")
            || trimmed.starts_with("http://")
            || trimmed.starts_with("git@")
            || trimmed.starts_with("ssh://")
            || trimmed.starts_with("file://")
        {
            trimmed.to_string()
        } else {
            let base = std::env::var("ANYAGENTS_GIT_REMOTE_BASE")
                .ok()
                .map(|s| s.trim().trim_end_matches('/').to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "https://github.com".to_string());
            format!("{base}/{slug}.git")
        };
        (org.to_string(), name.to_string(), remote_url)
    } else if trimmed.starts_with("file://") || Path::new(trimmed).is_absolute() {
        let path_part = trimmed.strip_prefix("file://").unwrap_or(trimmed);
        let comps: Vec<&str> = Path::new(path_part)
            .components()
            .filter_map(|c| match c {
                std::path::Component::Normal(s) => s.to_str(),
                _ => None,
            })
            .collect();
        if comps.is_empty() {
            return Err(fail("invalid local repository source".into()));
        }
        let raw_name = comps[comps.len() - 1];
        let name = raw_name.strip_suffix(".git").unwrap_or(raw_name);
        let org = if comps.len() >= 2 {
            comps[comps.len() - 2]
        } else {
            "local"
        };
        if !is_safe_slug_part(org) || !is_safe_slug_part(name) {
            return Err(fail("unsafe repository path component".into()));
        }
        (org.to_string(), name.to_string(), trimmed.to_string())
    } else {
        return Err(fail(format!(
            "unsupported repository identifier for bare pool: {trimmed}"
        )));
    };
    let pool_repo = pool_root.join(&org).join(format!("{name}.git"));
    let lock_file = pool_root.join(".locks").join(format!("{org}-{name}.lock"));
    Ok((pool_repo, lock_file, remote_url))
}

fn lock_pool_slot(repo: &str, lock_path: &Path) -> Result<std::fs::File, Error> {
    use std::os::fd::AsRawFd as _;
    let fail = |e: std::io::Error| Error::WorktreeProvisioning {
        repo: repo.into(),
        message: format!("pool lock {}: {e}", lock_path.display()),
    };
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent).map_err(fail)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .map_err(fail)?;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(fail(std::io::Error::last_os_error()));
    }
    Ok(file)
}

fn run_git_pool(
    repo: &str,
    dir: Option<&Path>,
    args: &[&std::ffi::OsStr],
) -> Result<String, Error> {
    let mut cmd = anyagents_toolchain::tool("git", "checkout")
        .map_err(|e| Error::WorktreeProvisioning {
            repo: repo.into(),
            message: e.to_string(),
        })?
        .command();
    cmd.args([
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "fetch.writeCommitGraph=false",
    ]);
    if let Some(d) = dir {
        cmd.arg("-C").arg(d);
    }
    cmd.args(args);
    let out = cmd.output().map_err(|e| Error::WorktreeProvisioning {
        repo: repo.into(),
        message: format!("git: {e}"),
    })?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        return Err(Error::WorktreeProvisioning {
            repo: repo.into(),
            message: format!(
                "{}: {}",
                out.status,
                if stderr.trim().is_empty() {
                    stdout.trim()
                } else {
                    stderr.trim()
                }
            ),
        });
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn clamp_pool_permissions(pool_repo: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        // Per-seat repositories read pool objects through
        // `objects/info/alternates`. With no seat write access anywhere in the
        // pool, world read/traverse is enough for seats and no per-seat pool
        // ACL grant is needed. Legacy linked privates stay under worktrees/,
        // which keeps no access for other principals.
        for (dir, mode) in [
            (pool_repo.to_path_buf(), 0o755),
            (pool_repo.join("worktrees"), 0o750),
        ] {
            if let Ok(meta) = std::fs::symlink_metadata(&dir)
                && meta.is_dir()
                && !meta.file_type().is_symlink()
            {
                let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode));
            }
        }
        for file in [pool_repo.join("config"), pool_repo.join("config.worktree")] {
            if let Ok(meta) = std::fs::symlink_metadata(&file)
                && meta.is_file()
                && !meta.file_type().is_symlink()
            {
                let _ = std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640));
            }
        }
        if let Ok(entries) = std::fs::read_dir(pool_repo.join("worktrees")) {
            for entry in entries.flatten() {
                let wt_meta = entry.path();
                if wt_meta.is_dir() {
                    let _ =
                        std::fs::set_permissions(&wt_meta, std::fs::Permissions::from_mode(0o750));
                    for name in ["commondir", "config.worktree"] {
                        let f = wt_meta.join(name);
                        if let Ok(m) = std::fs::symlink_metadata(&f)
                            && m.is_file()
                            && !m.file_type().is_symlink()
                        {
                            let _ = std::fs::set_permissions(
                                &f,
                                std::fs::Permissions::from_mode(0o640),
                            );
                        }
                    }
                }
            }
        }
    }
}

fn ensure_pool_repo(repo: &str, pool_repo: &Path, remote_url: &str) -> Result<(), Error> {
    if !pool_repo.join("HEAD").is_file() {
        if let Some(parent) = pool_repo.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::WorktreeProvisioning {
                repo: repo.into(),
                message: format!("{}: {e}", parent.display()),
            })?;
        }
        run_git_pool(
            repo,
            None,
            &[
                "clone".as_ref(),
                "--bare".as_ref(),
                "--reference-if-able".as_ref(),
                pool_repo.as_os_str(),
                remote_url.as_ref(),
                pool_repo.as_os_str(),
            ],
        )?;
    }
    let worktrees_dir = pool_repo.join("worktrees");
    let _ = std::fs::create_dir_all(&worktrees_dir);
    for (k, v) in [
        ("remote.origin.url", remote_url),
        ("remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"),
        ("extensions.worktreeConfig", "true"),
        ("fetch.prune", "false"),
        ("gc.auto", "0"),
        ("gc.pruneExpire", "never"),
    ] {
        // When managed Git config symlink is already active, write through the real config path.
        let cfg_file = std::fs::canonicalize(pool_repo.join("config"))
            .unwrap_or_else(|_| pool_repo.join("config"));
        run_git_pool(
            repo,
            None,
            &[
                "config".as_ref(),
                "--file".as_ref(),
                cfg_file.as_os_str(),
                k.as_ref(),
                v.as_ref(),
            ],
        )?;
    }
    clamp_pool_permissions(pool_repo);
    Ok(())
}

fn pool_fetch(pool_root: &Path, repo: &str, base: &str) -> Result<(), Error> {
    let (pool_repo, lock_path, remote_url) = resolve_pool_slot(pool_root, repo)?;
    let _lock = lock_pool_slot(repo, &lock_path)?;
    ensure_pool_repo(repo, &pool_repo, &remote_url)?;
    let spec = if let Some(tag) = base.strip_prefix("refs/tags/") {
        format!("+refs/tags/{tag}:refs/tags/{tag}")
    } else if let Some(branch) = base.strip_prefix("refs/heads/") {
        format!("+refs/heads/{branch}:refs/remotes/origin/{branch}")
    } else {
        let branch = base.strip_prefix("origin/").unwrap_or(base);
        format!("+refs/heads/{branch}:refs/remotes/origin/{branch}")
    };
    run_git_pool(
        repo,
        Some(&pool_repo),
        &[
            "fetch".as_ref(),
            "--no-tags".as_ref(),
            "--no-auto-maintenance".as_ref(),
            "origin".as_ref(),
            spec.as_ref(),
        ],
    )?;
    clamp_pool_permissions(&pool_repo);
    Ok(())
}

/// Provision a per-seat repository over the shared object pool. The pool is
/// a read-only object store: the seat references pool objects through
/// `objects/info/alternates` and keeps every object it creates in its own
/// gitdir, so the pool only ever grows from server-side fetches. The seat's
/// refs, packed-refs, config and index are exclusively its own; no seat
/// writes reach shared state.
fn pool_checkout(
    pool_root: &Path,
    repo: &str,
    target: &Path,
    branch: &str,
    base: Option<&str>,
) -> Result<(), Error> {
    let (pool_repo, lock_path, remote_url) = resolve_pool_slot(pool_root, repo)?;
    let _lock = lock_pool_slot(repo, &lock_path)?;
    ensure_pool_repo(repo, &pool_repo, &remote_url)?;
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::WorktreeProvisioning {
            repo: repo.into(),
            message: format!("{}: {e}", parent.display()),
        })?;
    }
    let started_empty = !target.exists()
        || std::fs::read_dir(target).is_ok_and(|mut entries| entries.next().is_none());
    let provision =
        || -> Result<(), Error> {
            // `--reference` records the pool objects as alternates instead of
            // copying them: the seat reads upstream history without duplicating
            // it, and maintenance in the seat can never write pool objects.
            run_git_pool(
                repo,
                None,
                &[
                    "clone".as_ref(),
                    "--reference".as_ref(),
                    pool_repo.as_os_str(),
                    "--no-checkout".as_ref(),
                    remote_url.as_ref(),
                    target.as_os_str(),
                ],
            )?;
            let start = match base {
                Some(b) if b != "HEAD" && b != "origin/HEAD" => b.to_string(),
                _ => {
                    let sym = run_git_pool(
                        repo,
                        Some(target),
                        &["symbolic-ref".as_ref(), "refs/remotes/origin/HEAD".as_ref()],
                    )?;
                    if let Some(name) = sym.strip_prefix("refs/remotes/origin/") {
                        format!("origin/{name}")
                    } else {
                        "HEAD".to_string()
                    }
                }
            };
            // The clone already materialized the default local branch; reset it
            // to the requested start like `worktree add -b` would.
            if branch.trim().is_empty() {
                run_git_pool(
                    repo,
                    Some(target),
                    &["checkout".as_ref(), "--detach".as_ref(), start.as_ref()],
                )?;
            } else {
                run_git_pool(
                    repo,
                    Some(target),
                    &[
                        "checkout".as_ref(),
                        "-B".as_ref(),
                        branch.as_ref(),
                        start.as_ref(),
                    ],
                )?;
            }
            // `config --worktree` writes only the seat's own config.worktree.
            // Auto-maintenance stays off so retention is explicit.
            run_git_pool(
                repo,
                Some(target),
                &[
                    "config".as_ref(),
                    "extensions.worktreeConfig".as_ref(),
                    "true".as_ref(),
                ],
            )?;
            run_git_pool(
                repo,
                Some(target),
                &["config".as_ref(), "gc.auto".as_ref(), "0".as_ref()],
            )?;
            let gitdir = target.join(".git");
            let alternates = std::fs::read_to_string(gitdir.join("objects/info/alternates"))
                .map_err(|e| Error::WorktreeProvisioning {
                    repo: repo.into(),
                    message: format!("{}: {e}", gitdir.join("objects/info/alternates").display()),
                })?;
            let expected = pool_repo.join("objects");
            let actual = alternates
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty() && !line.starts_with('#'))
                .map(PathBuf::from);
            if actual
                .as_ref()
                .and_then(|raw| std::fs::canonicalize(raw).ok())
                != std::fs::canonicalize(&expected).ok()
            {
                return Err(Error::WorktreeProvisioning {
                    repo: repo.into(),
                    message: format!(
                        "seat alternates {}: expected object pool {}",
                        actual
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "<none>".to_string()),
                        expected.display()
                    ),
                });
            }
            Ok(())
        };
    let result = provision();
    // A partial checkout must stay retryable: remove whatever this
    // provisioning created. The pool never gains seat objects, so nothing
    // here needs reviving elsewhere.
    if result.is_err() && started_empty {
        let _ = std::fs::remove_dir_all(target);
    }
    result?;
    clamp_pool_permissions(&pool_repo);
    Ok(())
}

fn is_commit_id(raw: &str) -> bool {
    matches!(raw.len(), 40 | 64) && raw.bytes().all(|b| b.is_ascii_hexdigit())
}

fn github_remote_repo(raw: &str) -> Option<String> {
    [
        "https://github.com/",
        "http://github.com/",
        "git@github.com:",
        "ssh://git@github.com/",
    ]
    .iter()
    .any(|prefix| raw.starts_with(prefix))
    .then(|| github_repo(raw))
    .flatten()
}

fn github_repo(raw: &str) -> Option<String> {
    let raw = raw.trim().trim_end_matches('/');
    let slug = raw
        .strip_prefix("https://github.com/")
        .or_else(|| raw.strip_prefix("http://github.com/"))
        .or_else(|| raw.strip_prefix("git@github.com:"))
        .or_else(|| raw.strip_prefix("ssh://git@github.com/"))
        .unwrap_or(raw)
        .trim_end_matches(".git");
    let parts: Vec<_> = slug.split('/').collect();
    (parts.len() == 2
        && parts.iter().all(|part| {
            !part.is_empty()
                && !part.contains("..")
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        }))
    .then(|| slug.to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    pub(crate) struct Fixture {
        pub consumer: Consumer,
        pub source: PathBuf,
        pub log: PathBuf,
        pub fail: PathBuf,
    }

    pub(crate) fn git(source: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(source)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().into()
    }

    /// The test parent must never hold a writable executable descriptor: an
    /// unrelated parallel fork can inherit it until exec and cause ETXTBSY even
    /// after std::fs::write returns. A waited writer has no surviving children.
    pub(crate) fn write_executable(path: &Path, body: &str) {
        assert!(
            Command::new("/bin/sh")
                .args(["-c", "printf '%s' \"$2\" > \"$1\"", "fixture-writer"])
                .arg(path)
                .arg(body)
                .status()
                .unwrap()
                .success()
        );
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    impl Fixture {
        pub(crate) fn new(dir: &Path) -> Self {
            let source = dir.join("source");
            let remote = dir.join("remote.git");
            let executable = dir.join("consumer");
            let log = dir.join("consumer.log");
            let fail = dir.join("fail");
            std::fs::create_dir_all(source.join("src")).unwrap();
            std::fs::create_dir_all(source.join("excluded")).unwrap();
            git(&source, &["init", "-b", "main"]);
            git(&source, &["config", "user.name", "Example Author"]);
            git(&source, &["config", "user.email", "author@example.test"]);
            std::fs::write(source.join("README.md"), "# Demo Repo\n").unwrap();
            std::fs::write(source.join("src/keep"), "included\n").unwrap();
            std::fs::write(source.join("excluded/large"), "excluded\n").unwrap();
            git(&source, &["add", "."]);
            git(&source, &["commit", "-m", "Initial commit"]);
            git(
                &source,
                &[
                    "clone",
                    "--bare",
                    "--no-hardlinks",
                    ".",
                    remote.to_str().unwrap(),
                ],
            );
            git(
                &source,
                &[
                    "remote",
                    "add",
                    "origin",
                    "https://github.com/ExampleOrg/demo.git",
                ],
            );
            git(
                &source,
                &[
                    "config",
                    &format!("url.file://{}.insteadOf", remote.display()),
                    "https://github.com/ExampleOrg/demo.git",
                ],
            );
            git(&source, &["update-ref", "refs/remotes/origin/main", "HEAD"]);
            // The authority fixture deliberately owns sparse checkout and hooks;
            // the consumer must not reimplement either policy or recover on failure.
            write_executable(
                &executable,
                &format!(
                    r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> {log:?}
if [ -f {fail:?} ] && [ "$(cat {fail:?})" = "$1" ]; then
  echo 'checkout-provider: injected policy refusal' >&2
  exit 42
fi
case "$1" in
  fetch) exit 0 ;;
  worktree)
    git -C {source:?} worktree add -b "$4" "$3" "${{5:-HEAD}}" >&2
    git -C "$3" sparse-checkout set src
    hooks=$(git -C "$3" rev-parse --git-path hooks)
    mkdir -p "$hooks"
    printf '#!/bin/sh\nexit 0\n' > "$hooks/pre-commit"
    printf '#!/bin/sh\nexit 0\n' > "$hooks/pre-push"
    chmod +x "$hooks/pre-commit" "$hooks/pre-push"
    ;;
  *) exit 2 ;;
esac
"#
                ),
            );
            Self {
                consumer: Consumer {
                    executable,
                    git_pool_dir: None,
                },
                source,
                log,
                fail,
            }
        }

        pub(crate) fn assert_checkout(&self, target: &Path) {
            assert_eq!(
                git(target, &["config", "--get", "core.sparseCheckout"]),
                "true"
            );
            assert!(target.join("src/keep").is_file());
            assert!(!target.join("excluded/large").exists());
            for name in ["pre-commit", "pre-push"] {
                let hook = PathBuf::from(git(
                    target,
                    &["rev-parse", "--git-path", &format!("hooks/{name}")],
                ));
                assert_ne!(
                    std::fs::metadata(hook).unwrap().permissions().mode() & 0o111,
                    0
                );
            }
        }
    }

    #[test]
    fn remote_and_local_checkouts_use_consumer_policy() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        let remote = dir.path().join("remote-work");
        f.consumer
            .remote(
                "https://github.com/ExampleOrg/demo.git",
                &remote,
                "remote-work",
                Some("main"),
            )
            .unwrap();
        f.assert_checkout(&remote);
        let local = dir.path().join("local-work");
        f.consumer
            .local(&f.source, &local, "local-work", None)
            .unwrap();
        f.assert_checkout(&local);
        assert_eq!(
            git(&local, &["rev-parse", "HEAD"]),
            git(&f.source, &["rev-parse", "HEAD"])
        );
        let log = std::fs::read_to_string(&f.log).unwrap();
        assert!(log.contains("fetch ExampleOrg/demo main"));
        assert!(log.contains("fetch ExampleOrg/demo refs/heads/main"));
        assert_eq!(
            log.lines().filter(|l| l.starts_with("worktree ")).count(),
            2
        );
    }

    #[test]
    fn unpushed_head_and_stale_tracking_refs_are_refused_before_consumer() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        git(&f.source, &["commit", "--allow-empty", "-m", "Not pushed"]);
        git(
            &f.source,
            &["update-ref", "refs/remotes/origin/main", "HEAD"],
        );
        let target = dir.path().join("work");
        let err = f
            .consumer
            .local(&f.source, &target, "work", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeSourceUnpublished { ref message, .. } if message.contains("push HEAD first")),
            "{err}"
        );
        assert!(!target.exists());
        assert!(!f.log.exists());
        // Publishing the same commit makes the previously refused request valid.
        git(&f.source, &["push", "origin", "HEAD:main"]);
        f.consumer.local(&f.source, &target, "work", None).unwrap();
        f.assert_checkout(&target);
    }

    #[test]
    fn missing_remote_refuses_before_consumer() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        git(&f.source, &["remote", "remove", "origin"]);
        let err = f
            .consumer
            .local(&f.source, &dir.path().join("work"), "work", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeSourceUnpublished { ref message, .. } if message.contains("no remote")),
            "{err}"
        );
        assert!(!f.log.exists());
    }

    #[test]
    fn fetch_and_checkout_failures_keep_tool_message_without_fallback() {
        for operation in ["fetch", "worktree"] {
            let dir = tempfile::tempdir().unwrap();
            let f = Fixture::new(dir.path());
            std::fs::write(&f.fail, operation).unwrap();
            let target = dir.path().join("work");
            let err = f
                .consumer
                .remote("ExampleOrg/demo", &target, "work", Some("main"))
                .unwrap_err();
            assert!(
                matches!(err, Error::WorktreeProvisioning { ref message, .. } if message.contains("injected policy refusal") && message.contains("42")),
                "{err}"
            );
            assert!(!target.exists());
            let log = std::fs::read_to_string(&f.log).unwrap();
            assert_eq!(
                log.lines().count(),
                if operation == "fetch" { 1 } else { 2 }
            );
        }
    }

    #[test]
    fn github_urls_are_normalized_but_other_hosts_and_local_remotes_are_rejected() {
        for raw in [
            "https://github.com/ExampleOrg/demo.git",
            "git@github.com:ExampleOrg/demo.git",
            "ssh://git@github.com/ExampleOrg/demo",
        ] {
            assert_eq!(github_remote_repo(raw).as_deref(), Some("ExampleOrg/demo"));
        }
        for raw in [
            "ExampleOrg/demo",
            "../demo",
            "/tmp/demo",
            "https://example.com/ExampleOrg/demo",
            "https://github.com/ExampleOrg/../demo",
        ] {
            assert!(github_remote_repo(raw).is_none(), "{raw}");
        }
    }

    #[test]
    fn published_ancestor_and_non_origin_github_remote_are_supported() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        let selected = git(&f.source, &["rev-parse", "HEAD"]);
        git(
            &f.source,
            &["commit", "--allow-empty", "-m", "Published descendant"],
        );
        git(&f.source, &["push", "origin", "HEAD:main"]);
        git(&f.source, &["checkout", "--detach", &selected]);
        git(&f.source, &["remote", "rename", "origin", "upstream"]);
        let target = dir.path().join("work");
        f.consumer.local(&f.source, &target, "work", None).unwrap();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), selected);
        f.assert_checkout(&target);
    }

    #[test]
    fn absent_consumer_does_not_enable_a_non_desk_mode() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        let consumer = Consumer {
            executable: dir.path().join("absent-consumer"),
            git_pool_dir: None,
        };
        let target = dir.path().join("work");
        let err = consumer
            .local(&f.source, &target, "work", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeProvisioning { ref message, .. } if message.contains("absent-consumer")),
            "{err}"
        );
        assert!(!target.exists());
    }
    #[test]
    fn unconfigured_checkout_provider_refuses_and_generic_remote_sources_are_forwarded() {
        let dir = tempfile::tempdir().unwrap();
        let unconfigured = Consumer {
            executable: PathBuf::new(),
            git_pool_dir: None,
        };
        assert!(
            unconfigured
                .remote(
                    "https://git.example.test/team/repo.git",
                    &dir.path().join("target"),
                    "branch",
                    None
                )
                .is_err()
        );
        let executable = dir.path().join("provider");
        let receipt = dir.path().join("args");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n",
                receipt.display()
            ),
        );
        let configured = Consumer {
            executable,
            git_pool_dir: None,
        };
        configured
            .remote(
                "https://git.example.test/team/repo.git",
                &dir.path().join("target"),
                "branch",
                Some("main"),
            )
            .unwrap();
        let args = std::fs::read_to_string(receipt).unwrap();
        assert!(args.contains("https://git.example.test/team/repo.git"));
        assert!(args.contains("worktree"));
    }

    /// Measurement for the engine-owned pool as a multi-seat store: the server
    /// (root here) owns the pool; a distinct unprivileged seat performs the
    /// ordinary workflow in its admitted checkout. Prints a JSON receipt of
    /// every operation's outcome. It asserts only what admission guarantees:
    /// the pool passes the shared trust predicate and the seat can commit.
    #[cfg(target_os = "linux")]
    #[test]
    fn privileged_pool_seat_workflow_receipt() {
        use std::os::unix::fs::PermissionsExt;
        if unsafe { libc::geteuid() } != 0 {
            assert!(
                std::env::var_os("ANYAGENTS_REQUIRE_PRIVILEGED_WORKTREE_TEST").is_none(),
                "requires root to create a distinct seat user"
            );
            eprintln!("requires root; pool seat workflow receipt not produced");
            return;
        }
        struct User(String);
        impl Drop for User {
            fn drop(&mut self) {
                let _ = Command::new("userdel").arg(&self.0).status();
            }
        }
        let name = format!("aa-pool-{}", std::process::id());
        assert!(
            Command::new("useradd")
                .args(["--no-create-home", "--user-group", &name])
                .status()
                .unwrap()
                .success()
        );
        let user = User(name);
        let id = |flag: &str| -> u32 {
            String::from_utf8(
                Command::new("id")
                    .args([flag, &user.0])
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap()
            .trim()
            .parse()
            .unwrap()
        };
        let (uid, gid) = (id("-u"), id("-g"));

        let dir = tempfile::tempdir_in("/var/tmp").unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let seed = dir.path().join("seed");
        let origin = dir.path().join("origin.git");
        std::fs::create_dir_all(&seed).unwrap();
        git(&seed, &["init", "-q", "-b", "main"]);
        for args in [
            &[
                "-c",
                "user.name=A",
                "-c",
                "user.email=a@example.invalid",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "base",
            ][..],
            &["branch", "stale"][..],
            &["branch", "packed-local"][..],
        ] {
            git(&seed, args);
        }
        git(
            &seed,
            &["clone", "-q", "--bare", ".", origin.to_str().unwrap()],
        );
        // The seat may push to its remote; the pool itself stays server-owned.
        assert!(
            Command::new("chown")
                .args(["-R", &format!("{uid}:{gid}")])
                .arg(&origin)
                .status()
                .unwrap()
                .success()
        );

        let pool_root = dir.path().join("pool");
        let consumer = Consumer {
            executable: PathBuf::new(),
            git_pool_dir: Some(pool_root.clone()),
        };
        let work = dir.path().join("work");
        let source = format!("file://{}", origin.display());
        consumer.remote(&source, &work, "agent/seat", None).unwrap();
        let common = PathBuf::from(git(
            &work,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        ));
        // The bare pool already holds `packed-local` and `stale` as branches;
        // fetch tracking refs too, then make every ref exist only packed.
        git(
            &common,
            &[
                "fetch",
                "-q",
                "origin",
                "+refs/heads/*:refs/remotes/origin/*",
            ],
        );
        git(&common, &["pack-refs", "--all"]);

        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let agent = anyagents_core::AgentId::generate();
        let journal = crate::worktree_access::grant_path(&data, &agent);
        crate::worktree_access::share(&work, uid, gid, &journal).unwrap();
        crate::worktree_access::RecordedWorktree::load(&data, &agent, &work)
            .expect("recorded grant")
            .validate()
            .expect("the pool passes the launch trust predicate");
        crate::worktree_access::verify(
            &work,
            uid,
            gid,
            Some(&format!("/usr/bin/setpriv\t{uid}\t{gid}\t{gid}")),
        )
        .expect("seat admission probe");

        let seat = |args: &[&str]| -> (i32, String) {
            let out = Command::new("/usr/bin/setpriv")
                .args([
                    "--reuid",
                    &uid.to_string(),
                    "--regid",
                    &gid.to_string(),
                    "--groups",
                    &gid.to_string(),
                    "--no-new-privs",
                    "--",
                ])
                .args([
                    "env",
                    "HOME=/nonexistent",
                    "GIT_CONFIG_GLOBAL=/dev/null",
                    "GIT_CONFIG_SYSTEM=/dev/null",
                    "XDG_CONFIG_HOME=/nonexistent",
                ])
                .args([
                    "git",
                    "-c",
                    "safe.directory=*",
                    "-c",
                    "user.name=Seat",
                    "-c",
                    "user.email=seat@example.invalid",
                    "-C",
                ])
                .arg(&work)
                .args(args)
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&out.stderr);
            let first = stderr
                .lines()
                .find(|l| l.starts_with("fatal"))
                .or_else(|| stderr.lines().find(|l| l.starts_with("error")))
                .unwrap_or("")
                .to_string();
            (out.status.code().unwrap_or(-1), first)
        };
        let mut receipt = Vec::new();
        let mut record = |op: &str, result: (i32, String)| {
            receipt.push(serde_json::json!({"op": op, "exit": result.0, "message": result.1}));
            result.0
        };
        let commit = record(
            "commit on new branch",
            seat(&["commit", "-q", "--allow-empty", "-m", "seat work"]),
        );
        record(
            "push -u",
            seat(&["push", "-q", "-u", "origin", "HEAD:refs/heads/agent/seat"]),
        );
        record(
            "upstream recorded in config",
            seat(&["config", "--get", "branch.agent/seat.remote"]),
        );
        record(
            "set-upstream-to",
            seat(&["branch", "--set-upstream-to=origin/main"]),
        );
        record(
            "worktree-scoped config write",
            seat(&[
                "config",
                "--worktree",
                "branch.agent/seat.description",
                "seat",
            ]),
        );
        git(&origin, &["branch", "-D", "stale"]);
        record(
            "fetch --prune (packed tracking ref)",
            seat(&["fetch", "-q", "--prune", "origin"]),
        );
        record("create loose branch", seat(&["branch", "tmp-loose"]));
        record("branch -d loose", seat(&["branch", "-d", "tmp-loose"]));
        record("branch -D packed", seat(&["branch", "-D", "packed-local"]));
        record("gc --auto", seat(&["gc", "--auto"]));
        record("worktree prune", seat(&["worktree", "prune"]));
        record("status", seat(&["status", "--porcelain"]));
        eprintln!(
            "POOL_SEAT_WORKFLOW_RECEIPT {}",
            serde_json::to_string(&receipt).unwrap()
        );
        assert_eq!(commit, 0, "seat cannot commit in an admitted pool checkout");
    }
    /// A pool-backed consumer for tests: bare pool under `pool_dir`,
    /// upstream reached over file:// from a `remotes_root/ExampleOrg/demo.git`
    /// bare repository seeded by the caller.
    pub(crate) fn seed_bare_remote(dir: &Path) -> PathBuf {
        let seed = dir.join("seed");
        std::fs::create_dir_all(&seed).unwrap();
        git(&seed, &["init", "-b", "main"]);
        git(&seed, &["config", "user.name", "Example Author"]);
        git(&seed, &["config", "user.email", "author@example.test"]);
        std::fs::write(seed.join("README.md"), "# Demo\n").unwrap();
        git(&seed, &["add", "."]);
        git(&seed, &["commit", "-m", "Initial commit"]);
        git(&seed, &["branch", "stale"]);
        git(&seed, &["branch", "packed-local"]);
        let remote = dir.join("remotes/ExampleOrg/demo.git");
        git(
            &seed,
            &[
                "clone",
                "--bare",
                "--no-hardlinks",
                ".",
                remote.to_str().unwrap(),
            ],
        );
        remote
    }

    pub(crate) fn pool_consumer(pool_dir: &Path) -> Consumer {
        Consumer {
            executable: PathBuf::new(),
            git_pool_dir: Some(pool_dir.to_path_buf()),
        }
    }

    /// Object accounting for a bare pool: loose plus packed objects. Seat
    /// writes must never change this number.
    pub(crate) fn pool_object_count(pool_repo: &Path) -> u64 {
        let report = git(pool_repo, &["count-objects", "-v"]);
        report
            .lines()
            .filter_map(|line| {
                let (key, value) = line.split_once(": ")?;
                (key == "count" || key == "in-pack").then(|| value.trim().parse::<u64>().ok())
            })
            .flatten()
            .sum()
    }

    /// Pool-backed checkouts are per-seat repositories, not linked
    /// worktrees: the seat gitdir is a real directory, refs and objects are
    /// exclusively the seat's, and the pool only ever gains objects from
    /// server-side fetches.
    #[test]
    fn pool_checkout_is_a_seat_repository_that_never_writes_the_pool() {
        let dir = tempfile::tempdir().unwrap();
        let remote = seed_bare_remote(dir.path());
        let consumer = pool_consumer(&dir.path().join("pool"));
        let target = dir.path().join("seat");
        consumer
            .remote(
                &format!("file://{}", remote.display()),
                &target,
                "agent/seat",
                Some("main"),
            )
            .unwrap();
        let pool_repo = dir.path().join("pool").join("ExampleOrg/demo.git");
        assert!(pool_repo.join("HEAD").is_file());
        // A real gitdir, never a linked-worktree pointer.
        assert!(target.join(".git").is_dir() && !target.join(".git").is_symlink());
        assert_eq!(
            git(&target, &["rev-parse", "--git-common-dir"]),
            git(&target, &["rev-parse", "--git-dir"])
        );
        let alternates =
            std::fs::read_to_string(target.join(".git/objects/info/alternates")).unwrap();
        assert!(
            alternates.contains(&format!("{}/objects\n", pool_repo.display())),
            "alternates: {alternates}"
        );
        assert_eq!(git(&target, &["branch", "--show-current"]), "agent/seat");
        assert!(target.join("README.md").is_file());
        let rate = {
            let before = pool_object_count(&pool_repo);
            assert!(before > 0, "pool holds upstream history");
            git(&target, &["config", "user.name", "Seat Author"]);
            git(&target, &["config", "user.email", "seat@example.test"]);
            git(&target, &["commit", "--allow-empty", "-m", "Seat work"]);
            git(&target, &["branch", "tmp-loose"]);
            git(&target, &["branch", "-d", "tmp-loose"]);
            git(&target, &["pack-refs", "--all"]);
            git(&target, &["gc"]);
            assert_eq!(
                pool_object_count(&pool_repo),
                before,
                "seat commits and gc must not add objects to the pool"
            );
            // Seat objects land in the seat gitdir.
            let seat_objects = pool_object_count(target.join(".git").as_path());
            assert!(seat_objects > 0, "seat gitdir holds its own objects");
            before
        };
        assert_eq!(pool_object_count(&pool_repo), rate);
    }
}
