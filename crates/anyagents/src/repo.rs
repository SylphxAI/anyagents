//! One repository per agent (ADR 0010 D5, ADR 0006 decision 11).
//!
//! An agent's checkout is its own Git repository: its config, refs, index and
//! hooks belong to it alone, so it needs ordinary permissions on its own tree
//! and nothing else. Objects come from a read-only pool through
//! `objects/info/alternates`, so a new checkout costs a checkout, not a
//! download. Only the control plane fetches into a pool, and a pool is never
//! pruned: pruning a reference repository can corrupt the clones that borrow
//! from it (git-clone(1)).
//!
//! The pool is AnyAgents' own bare repository per remote, under
//! `ANYAGENTS_GIT_POOL_DIR` (default `<data>/git-pool`). A deployment may
//! instead name a checkout provider (`ANYAGENTS_CHECKOUT_PROVIDER_BIN`) that
//! accepts `fetch <source> <ref>` and `worktree <source> <target> <branch>
//! [base]`. A provider that makes a linked worktree of its own store has its
//! result adopted into an own repository, and its store becomes the pool.
use std::path::{Path, PathBuf};
use std::process::Command;

use anyagents_core::Error;

/// The pool's directory under the data directory when none is configured.
const POOL_DIR: &str = "git-pool";

/// Where an in-place conversion builds the new Git directory, inside the
/// checkout, before it is exchanged with the old `.git` pointer.
const ADOPT_STAGING: &str = ".git.anyagents-adopt";

fn refusal(path: &Path, message: impl std::fmt::Display) -> Error {
    Error::WorkdirPermission {
        path: path.display().to_string(),
        message: message.to_string(),
    }
}

fn provisioning(repo: &str, message: impl std::fmt::Display) -> Error {
    Error::WorktreeProvisioning {
        repo: repo.into(),
        message: message.to_string(),
    }
}

/// How this server makes checkouts.
pub(crate) struct Checkouts {
    provider: Option<PathBuf>,
    pool_root: PathBuf,
    remote_base: String,
    author: (Option<String>, Option<String>),
}

impl Checkouts {
    pub(crate) fn configured(data: &Path) -> Self {
        Self {
            provider: anyagents_toolchain::tool("checkout-provider", "checkout")
                .ok()
                .map(|tool| tool.path)
                .filter(|path| !path.as_os_str().is_empty()),
            pool_root: anyagents_core::configured_git_pool_dir()
                .unwrap_or_else(|| data.join(POOL_DIR)),
            remote_base: remote_base(),
            author: (
                std::env::var("ANYAGENTS_GIT_AUTHOR_NAME").ok(),
                std::env::var("ANYAGENTS_GIT_AUTHOR_EMAIL").ok(),
            ),
        }
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
        let base_branch = base
            .filter(|b| *b != "HEAD" && *b != "origin/HEAD" && !is_commit_id(b))
            .filter(|b| !b.starts_with("refs/tags/"))
            .map(|b| {
                b.strip_prefix("origin/")
                    .or_else(|| b.strip_prefix("refs/heads/"))
                    .unwrap_or(b)
            });
        if let Some(base) = base.filter(|b| *b != "HEAD" && *b != "origin/HEAD" && !is_commit_id(b))
        {
            self.fetch(&repo, base.strip_prefix("origin/").unwrap_or(base))?;
        }
        self.checkout(&repo, target, branch, start.as_deref(), base_branch)
    }

    /// Branch from a local repository (`repo: "inherit"` or a path). Its HEAD
    /// and the requested base must already be published. Only plain reads run
    /// inside the source, which an agent may own: the remote is asked from
    /// outside it, so the source's configuration never selects a program.
    pub(crate) fn local(
        &self,
        source: &Path,
        target: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), Error> {
        let refuse = |message: String| Error::WorktreeSourceUnpublished {
            source_repo: source.display().to_string(),
            message,
        };
        let git_tool =
            anyagents_toolchain::tool("git", "checkout").map_err(|e| refuse(e.to_string()))?;
        let read = |args: &[&str]| -> Result<std::process::Output, Error> {
            git_tool
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
                .stdin(std::process::Stdio::null())
                .output()
                .map_err(|e| refuse(e.to_string()))
        };
        let git = |args: &[&str]| -> Result<String, Error> {
            let out = read(args)?;
            if !out.status.success() {
                return Err(refuse(
                    String::from_utf8_lossy(&out.stderr).trim().to_string(),
                ));
            }
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
        };
        let mut remotes: Vec<_> = git(&["remote"])?.lines().map(str::to_string).collect();
        remotes.sort_by_key(|name| (name != "origin", name.clone()));
        let repo = remotes
            .into_iter()
            .find_map(|name| {
                let url = git(&["config", "--get", &format!("remote.{name}.url")]).ok()?;
                (!url.trim().is_empty()).then(|| github_remote_repo(&url).unwrap_or(url))
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
        let refs = run_git(
            &repo,
            None,
            &[
                "ls-remote".as_ref(),
                "--heads".as_ref(),
                "--tags".as_ref(),
                self.remote_url(&repo).as_ref(),
            ],
        )
        .map_err(|e| refuse(e.to_string()))?;
        let reachable_ref = |commit: &str| -> Option<String> {
            refs.lines()
                .filter_map(|line| line.split_once('\t'))
                .find_map(|(tip, name)| {
                    if name.ends_with("^{}") {
                        return None;
                    }
                    let reachable = tip == commit
                        || read(&["merge-base", "--is-ancestor", commit, tip])
                            .is_ok_and(|out| out.status.success());
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
        // A published feature commit may not be in the pool's default branch yet.
        self.fetch(&repo, &reference)?;
        let base_branch = reference.strip_prefix("refs/heads/");
        self.checkout(&repo, target, branch, Some(&selected), base_branch)
    }

    fn fetch(&self, repo: &str, base: &str) -> Result<(), Error> {
        match &self.provider {
            Some(provider) => run_provider(
                provider,
                repo,
                &["fetch".as_ref(), repo.as_ref(), base.as_ref()],
            ),
            None => self.pool_fetch(repo, base),
        }
    }

    fn checkout(
        &self,
        repo: &str,
        target: &Path,
        branch: &str,
        base: Option<&str>,
        base_branch: Option<&str>,
    ) -> Result<(), Error> {
        let author = self.author(target)?;
        match &self.provider {
            None => self.pool_clone(repo, target, branch, base)?,
            Some(provider) => {
                let mut args: Vec<&std::ffi::OsStr> = vec![
                    "worktree".as_ref(),
                    repo.as_ref(),
                    target.as_os_str(),
                    branch.as_ref(),
                ];
                if let Some(base) = base {
                    args.push(base.as_ref());
                }
                run_provider(provider, repo, &args)?;
                // A linked worktree of the provider's store becomes an own
                // repository; the store is its pool from now on.
                if let Adoption::Declined(reason) = adopt_from(target, None, base_branch, false) {
                    if let Err(error) = discard(target) {
                        eprintln!(
                            "warning: cannot remove unadopted checkout {}: {error}",
                            target.display()
                        );
                    }
                    return Err(provisioning(
                        repo,
                        format!("the provider's checkout is not an own repository: {reason}"),
                    ));
                }
            }
        }
        // A caller-configured commit identity belongs to this repository alone.
        if let Some((name, email)) = author {
            for (key, value) in [("user.name", name), ("user.email", email)] {
                if let Err(error) = run_git(
                    repo,
                    Some(target),
                    &["config".as_ref(), key.as_ref(), value.as_ref()],
                ) {
                    let _ = discard(target);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn author(&self, target: &Path) -> Result<Option<(&str, &str)>, Error> {
        match &self.author {
            (None, None) => Ok(None),
            (Some(name), Some(email)) => Ok(Some((name, email))),
            _ => Err(refusal(target, "configure both Git author name and email")),
        }
    }

    /// The URL a repository slug names, as the pool fetches it.
    fn remote_url(&self, repo: &str) -> String {
        match github_repo(repo) {
            Some(slug) if !repo.contains("://") && !repo.starts_with("git@") => {
                format!("{}/{slug}.git", self.remote_base)
            }
            _ => repo.to_string(),
        }
    }

    fn pool_slot(&self, raw: &str) -> Result<(PathBuf, PathBuf, String), Error> {
        let trimmed = raw.trim().trim_end_matches('/');
        let (org, name, remote_url) = if let Some(slug) = github_repo(trimmed) {
            let (org, name) = slug
                .split_once('/')
                .ok_or_else(|| provisioning(raw, "invalid repository slug"))?;
            let remote_url = if trimmed.starts_with("https://")
                || trimmed.starts_with("http://")
                || trimmed.starts_with("git@")
                || trimmed.starts_with("ssh://")
                || trimmed.starts_with("file://")
            {
                trimmed.to_string()
            } else {
                self.remote_url(&slug)
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
            let Some(raw_name) = comps.last() else {
                return Err(provisioning(raw, "invalid local repository source"));
            };
            let name = raw_name.strip_suffix(".git").unwrap_or(raw_name);
            let org = if comps.len() >= 2 {
                comps[comps.len() - 2]
            } else {
                "local"
            };
            if !is_safe_slug_part(org) || !is_safe_slug_part(name) {
                return Err(provisioning(raw, "unsafe repository path component"));
            }
            (org.to_string(), name.to_string(), trimmed.to_string())
        } else {
            return Err(provisioning(
                raw,
                format!("unsupported repository identifier for the pool: {trimmed}"),
            ));
        };
        let pool = self.pool_root.join(&org).join(format!("{name}.git"));
        let lock = self
            .pool_root
            .join(".locks")
            .join(format!("{org}-{name}.lock"));
        Ok((pool, lock, remote_url))
    }

    fn pool_fetch(&self, repo: &str, base: &str) -> Result<(), Error> {
        let (pool, lock, remote_url) = self.pool_slot(repo)?;
        let _lock = lock_pool(repo, &lock)?;
        ensure_pool(repo, &pool, &remote_url)?;
        let spec = if let Some(tag) = base.strip_prefix("refs/tags/") {
            format!("+refs/tags/{tag}:refs/tags/{tag}")
        } else {
            let branch = base
                .strip_prefix("refs/heads/")
                .or_else(|| base.strip_prefix("origin/"))
                .unwrap_or(base);
            format!("+refs/heads/{branch}:refs/remotes/origin/{branch}")
        };
        fetch_into_pool(repo, &pool, &spec)
    }

    /// A new repository at `target` whose objects come from the pool. The pool
    /// lock is held only while the pool is read; the repository itself is made
    /// outside it, because nothing ever removes a pool object.
    fn pool_clone(
        &self,
        repo: &str,
        target: &Path,
        branch: &str,
        base: Option<&str>,
    ) -> Result<(), Error> {
        let (pool, lock, remote_url) = self.pool_slot(repo)?;
        let (commit, tracking, default) = {
            let _lock = lock_pool(repo, &lock)?;
            ensure_pool(repo, &pool, &remote_url)?;
            let default = pool_default_branch(repo, &pool)?;
            let start = match base {
                Some(b) if b != "HEAD" && b != "origin/HEAD" => b.to_string(),
                _ => default
                    .as_ref()
                    .map_or_else(|| "HEAD".to_string(), |d| format!("origin/{d}")),
            };
            let commit = run_git(
                repo,
                Some(&pool),
                &[
                    "rev-parse".as_ref(),
                    "--verify".as_ref(),
                    "--end-of-options".as_ref(),
                    format!("{start}^{{commit}}").as_ref(),
                ],
            )?;
            // The repository starts with the remote-tracking refs it was made
            // from: the default branch, and the base when it names a branch or tag.
            let mut tracking = Vec::new();
            let names = [
                default.as_ref().map(|d| format!("refs/remotes/origin/{d}")),
                {
                    if let Some(b) = start.strip_prefix("origin/") {
                        Some(format!("refs/remotes/origin/{b}"))
                    } else {
                        start.starts_with("refs/tags/").then(|| start.clone())
                    }
                },
            ];
            for name in names.into_iter().flatten() {
                if tracking.iter().any(|(known, _)| *known == name) {
                    continue;
                }
                if let Ok(oid) = run_git(
                    repo,
                    Some(&pool),
                    &[
                        "rev-parse".as_ref(),
                        "--verify".as_ref(),
                        "--end-of-options".as_ref(),
                        name.as_ref(),
                    ],
                ) {
                    tracking.push((name, oid));
                }
            }
            (commit, tracking, default)
        };
        let started_empty = !target.exists()
            || std::fs::read_dir(target).is_ok_and(|mut entries| entries.next().is_none());
        if !started_empty {
            return Err(provisioning(
                repo,
                format!("{} already exists and is not empty", target.display()),
            ));
        }
        let made = (|| -> Result<(), Error> {
            let format = run_git(
                repo,
                Some(&pool),
                &["rev-parse".as_ref(), "--show-object-format".as_ref()],
            )?;
            run_git(
                repo,
                None,
                &[
                    "init".as_ref(),
                    "--quiet".as_ref(),
                    "--template=".as_ref(),
                    format!("--object-format={format}").as_ref(),
                    target.as_os_str(),
                ],
            )?;
            let objects = std::fs::canonicalize(pool.join("objects"))
                .map_err(|e| provisioning(repo, format!("{}: {e}", pool.display())))?;
            let alternates = target.join(".git/objects/info/alternates");
            std::fs::create_dir_all(alternates.parent().unwrap_or(target))
                .and_then(|()| std::fs::write(&alternates, format!("{}\n", objects.display())))
                .map_err(|e| provisioning(repo, format!("{}: {e}", alternates.display())))?;
            for (key, value) in [
                ("remote.origin.url", remote_url.as_str()),
                ("remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"),
            ] {
                run_git(
                    repo,
                    Some(target),
                    &["config".as_ref(), key.as_ref(), value.as_ref()],
                )?;
            }
            let updates: String = tracking
                .iter()
                .map(|(name, oid)| format!("create {name} {oid}\n"))
                .collect();
            run_git_input(
                repo,
                Some(target),
                &["update-ref".as_ref(), "--stdin".as_ref()],
                &updates,
            )?;
            if let Some(default) = &default {
                run_git(
                    repo,
                    Some(target),
                    &[
                        "symbolic-ref".as_ref(),
                        "refs/remotes/origin/HEAD".as_ref(),
                        format!("refs/remotes/origin/{default}").as_ref(),
                    ],
                )?;
            }
            if branch.trim().is_empty() {
                run_git(
                    repo,
                    Some(target),
                    &[
                        "checkout".as_ref(),
                        "--quiet".as_ref(),
                        "--detach".as_ref(),
                        commit.as_ref(),
                    ],
                )?;
            } else {
                run_git(
                    repo,
                    Some(target),
                    &[
                        "checkout".as_ref(),
                        "--quiet".as_ref(),
                        "--no-track".as_ref(),
                        "-b".as_ref(),
                        branch.as_ref(),
                        commit.as_ref(),
                    ],
                )?;
            }
            Ok(())
        })();
        if made.is_err() {
            // A partial repository must stay retryable; nothing here touched the pool.
            let _ = std::fs::remove_dir_all(target);
        }
        made
    }
}

/// Where a GitHub slug is fetched from (`ANYAGENTS_GIT_REMOTE_BASE`).
fn remote_base() -> String {
    std::env::var("ANYAGENTS_GIT_REMOTE_BASE")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "https://github.com".to_string())
}

/// What a checkout's `origin` advertises now, as `(object, ref)` for its
/// branches, tags and notes: the remote's own answer, not the checkout's
/// remote-tracking refs. The URL is read as `seat`; the server asks with its
/// own credentials, from outside the checkout, and only a GitHub repository
/// or a local path. `None` when it cannot ask or gets no answer.
pub(crate) fn published(
    checkout: &Path,
    seat: Option<(u32, u32)>,
) -> Option<Vec<(String, String)>> {
    let out = inspect_command(seat, &checkout.join(".git"), None)
        .ok()?
        .args(["ls-remote", "--get-url", "origin"])
        .stdin(std::process::Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let url = match github_remote_repo(&url) {
        Some(slug) => format!("{}/{slug}.git", remote_base()),
        None if url.starts_with("file://") || Path::new(&url).is_absolute() => url,
        None => return None,
    };
    let listed = run_git(
        "origin",
        None,
        &[
            "-c".as_ref(),
            "http.lowSpeedLimit=1000".as_ref(),
            "-c".as_ref(),
            "http.lowSpeedTime=60".as_ref(),
            "ls-remote".as_ref(),
            url.as_ref(),
            "refs/heads/*".as_ref(),
            "refs/tags/*".as_ref(),
            "refs/notes/*".as_ref(),
        ],
    )
    .ok()?;
    Some(
        listed
            .lines()
            .filter_map(|line| line.split_once('\t'))
            .map(|(oid, name)| (oid.to_string(), name.to_string()))
            .collect(),
    )
}

fn run_provider(provider: &Path, repo: &str, args: &[&std::ffi::OsStr]) -> Result<(), Error> {
    let out = Command::new(provider)
        .args(args)
        .output()
        .map_err(|error| provisioning(repo, format!("{}: {error}", provider.display())))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        return Err(provisioning(
            repo,
            format!(
                "{}: {}",
                out.status,
                if stderr.trim().is_empty() {
                    stdout.trim()
                } else {
                    stderr.trim()
                }
            ),
        ));
    }
    Ok(())
}

fn is_safe_slug_part(part: &str) -> bool {
    !part.is_empty()
        && part != "."
        && !part.contains("..")
        && part
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn lock_pool(repo: &str, lock_path: &Path) -> Result<std::fs::File, Error> {
    use std::os::fd::AsRawFd as _;
    let fail =
        |e: std::io::Error| provisioning(repo, format!("pool lock {}: {e}", lock_path.display()));
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

/// Git as the server, for the pool and for repositories it is making. Outside
/// a repository it runs from `/`, never from whatever the server's own
/// directory holds.
fn git_command(repo: &str, dir: Option<&Path>) -> Result<Command, Error> {
    let mut cmd = anyagents_toolchain::tool("git", "checkout")
        .map_err(|e| provisioning(repo, e))?
        .command();
    cmd.args([
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "fetch.writeCommitGraph=false",
    ]);
    match dir {
        Some(d) => {
            cmd.arg("-C").arg(d);
        }
        None => {
            cmd.current_dir("/");
        }
    }
    Ok(cmd)
}

fn git_result(repo: &str, out: std::io::Result<std::process::Output>) -> Result<String, Error> {
    let out = out.map_err(|e| provisioning(repo, format!("git: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stdout = String::from_utf8_lossy(&out.stdout);
        return Err(provisioning(
            repo,
            format!(
                "{}: {}",
                out.status,
                if stderr.trim().is_empty() {
                    stdout.trim()
                } else {
                    stderr.trim()
                }
            ),
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn run_git(repo: &str, dir: Option<&Path>, args: &[&std::ffi::OsStr]) -> Result<String, Error> {
    let mut cmd = git_command(repo, dir)?;
    git_result(
        repo,
        cmd.args(args).stdin(std::process::Stdio::null()).output(),
    )
}

fn run_git_input(
    repo: &str,
    dir: Option<&Path>,
    args: &[&std::ffi::OsStr],
    input: &str,
) -> Result<String, Error> {
    let mut cmd = git_command(repo, dir)?;
    git_result(repo, output_with_input(cmd.args(args), input))
}

pub(crate) fn output_with_input(
    cmd: &mut Command,
    input: &str,
) -> std::io::Result<std::process::Output> {
    use std::io::Write as _;
    let mut child = cmd
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let input = input.to_string();
    // Write from another thread: a large input must not deadlock against a
    // child that fills its own output pipe first.
    let writer = std::thread::spawn(move || stdin.write_all(input.as_bytes()));
    let out = child.wait_with_output()?;
    writer
        .join()
        .map_err(|_| std::io::Error::other("stdin writer panicked"))??;
    Ok(out)
}

/// A pool is a bare repository that only the control plane writes. It is
/// readable by every agent, which borrows its objects, and it is never pruned.
fn ensure_pool(repo: &str, pool: &Path, remote_url: &str) -> Result<(), Error> {
    if !pool.join("HEAD").is_file() {
        if let Some(parent) = pool.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| provisioning(repo, format!("{}: {e}", parent.display())))?;
        }
        run_git(
            repo,
            None,
            &[
                "init".as_ref(),
                "--quiet".as_ref(),
                "--bare".as_ref(),
                "--template=".as_ref(),
                "--shared=0644".as_ref(),
                pool.as_os_str(),
            ],
        )?;
    }
    for (key, value) in [
        ("remote.origin.url", remote_url),
        ("remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"),
        ("core.sharedRepository", "0644"),
        ("fetch.prune", "false"),
        ("gc.auto", "0"),
        ("gc.pruneExpire", "never"),
        ("gc.cruftPacks", "true"),
    ] {
        run_git(
            repo,
            None,
            &[
                "config".as_ref(),
                "--file".as_ref(),
                pool.join("config").as_os_str(),
                key.as_ref(),
                value.as_ref(),
            ],
        )?;
    }
    Ok(())
}

fn fetch_into_pool(repo: &str, pool: &Path, spec: &str) -> Result<(), Error> {
    run_git(
        repo,
        Some(pool),
        &[
            "fetch".as_ref(),
            "--quiet".as_ref(),
            "--no-tags".as_ref(),
            "--no-prune".as_ref(),
            "--no-auto-maintenance".as_ref(),
            "origin".as_ref(),
            spec.as_ref(),
        ],
    )
    .map(drop)
}

/// The remote's default branch, fetched into the pool. `None` when the remote
/// advertises none (an empty repository).
fn pool_default_branch(repo: &str, pool: &Path) -> Result<Option<String>, Error> {
    if let Ok(sym) = run_git(
        repo,
        Some(pool),
        &["symbolic-ref".as_ref(), "refs/remotes/origin/HEAD".as_ref()],
    ) && let Some(branch) = sym.strip_prefix("refs/remotes/origin/")
    {
        let spec = format!("+refs/heads/{branch}:refs/remotes/origin/{branch}");
        // A stale default is still a default; a failed refresh is not fatal.
        let _ = fetch_into_pool(repo, pool, &spec);
        return Ok(Some(branch.to_string()));
    }
    let advertised = run_git(
        repo,
        Some(pool),
        &[
            "ls-remote".as_ref(),
            "--symref".as_ref(),
            "origin".as_ref(),
            "HEAD".as_ref(),
        ],
    )?;
    for line in advertised.lines() {
        if let Some((target, name)) = line.split_once('\t')
            && name == "HEAD"
            && let Some(branch) = target.strip_prefix("ref: refs/heads/")
        {
            fetch_into_pool(
                repo,
                pool,
                &format!("+refs/heads/{branch}:refs/remotes/origin/{branch}"),
            )?;
            run_git(
                repo,
                Some(pool),
                &[
                    "symbolic-ref".as_ref(),
                    "refs/remotes/origin/HEAD".as_ref(),
                    format!("refs/remotes/origin/{branch}").as_ref(),
                ],
            )?;
            return Ok(Some(branch.to_string()));
        }
    }
    Ok(None)
}

/// What [`adopt`] made of a checkout.
pub(crate) enum Adoption {
    /// Already an own repository.
    Own,
    /// A linked worktree, now an own repository.
    Converted,
    /// Left exactly as it was, for this reason.
    Declined(String),
}

/// Convert a linked worktree of a shared store into an own repository, in
/// place. Nothing in the working tree is touched.
///
/// - The store must never prune (`gc.pruneExpire=never`, `gc.cruftPacks=true`):
///   the new repository borrows its objects.
/// - The new Git directory is built beside the `.git` pointer: the store's
///   configuration without other checkouts' branches, this checkout's
///   `config.worktree`, and fetch refspecs covering its own branch and its
///   base (so a plain `git fetch origin` refreshes them); the same HEAD,
///   index, reflogs, sparse patterns, excludes, hooks, remote-tracking refs
///   and tags.
/// - Objects only this checkout reaches (unpushed and reflog-only commits,
///   staged blobs) are packed into it.
/// - A keep ref in the store (`refs/anyagents/keep/<checkout>`) holds every
///   commit the new repository's refs reach until it is reaped, so even an
///   explicit prune of the store cannot take them.
/// - The conversion stands only if `git status` reads the same before and
///   after. Then the new directory is synced to disk and exchanged with the
///   pointer in one rename, and the store forgets the registration. A crash
///   after the exchange is finished by the next call.
///
/// An operation in progress, a locked worktree, or a registration that does
/// not name this checkout declines and changes nothing. `seat` is the
/// identity Git reads the checkout as.
pub(crate) fn adopt(checkout: &Path, seat: Option<(u32, u32)>) -> Adoption {
    adopt_from(checkout, seat, None, false)
}

/// [`adopt`], with the remote branch the checkout was made from, and (for
/// tests) a crash right after the exchange.
fn adopt_from(
    checkout: &Path,
    seat: Option<(u32, u32)>,
    base: Option<&str>,
    crash_after_exchange: bool,
) -> Adoption {
    let dotgit = checkout.join(".git");
    let staging = checkout.join(ADOPT_STAGING);
    match std::fs::symlink_metadata(&dotgit) {
        Ok(meta) if meta.is_dir() => {
            finish_interrupted(checkout, &staging);
            return Adoption::Own;
        }
        Ok(meta) if meta.is_file() => {}
        Ok(_) => return Adoption::Declined(".git is neither a directory nor a file".into()),
        Err(error) => return Adoption::Declined(format!("{}: {error}", dotgit.display())),
    }
    let linked = match Linked::read(checkout, &dotgit) {
        Ok(linked) => linked,
        Err(reason) => return Adoption::Declined(reason),
    };
    if let Some(busy) = linked.busy() {
        return Adoption::Declined(busy);
    }
    if let Err(reason) = linked.store_keeps_objects() {
        return Adoption::Declined(reason);
    }
    // A build an earlier attempt did not finish.
    remove_staging(&staging);
    if let Err(reason) = linked.build(checkout, &staging, seat, base) {
        remove_staging(&staging);
        return Adoption::Declined(reason);
    }
    let keep = match linked.keep(checkout, &staging) {
        Ok(keep) => keep,
        Err(reason) => {
            remove_staging(&staging);
            return Adoption::Declined(reason);
        }
    };
    if let Err(error) = sync_tree(&staging).and_then(|()| exchange(&dotgit, &staging)) {
        remove_staging(&staging);
        let _ = store_git(&linked.common, None, &["update-ref", "-d", &keep]);
        return Adoption::Declined(format!("cannot exchange {}: {error}", dotgit.display()));
    }
    let _ = sync_dir(checkout);
    if crash_after_exchange {
        return Adoption::Converted;
    }
    linked.release();
    remove_staging(&staging);
    Adoption::Converted
}

/// After an exchange, the staging path holds the old `.git` pointer until
/// the store has forgotten the registration. Finish that, then clear it.
fn finish_interrupted(checkout: &Path, staging: &Path) {
    if std::fs::symlink_metadata(staging).is_ok_and(|meta| meta.is_file())
        && let Ok(linked) = Linked::read(checkout, staging)
    {
        linked.release();
    }
    remove_staging(staging);
}

/// Remove a checkout made for a creation that failed, and its registration
/// in a shared store if it is still a linked worktree.
pub(crate) fn discard(checkout: &Path) -> std::io::Result<()> {
    let dotgit = checkout.join(".git");
    if std::fs::symlink_metadata(&dotgit).is_ok_and(|m| m.is_file()) {
        Linked::read(checkout, &dotgit)
            .map_err(std::io::Error::other)?
            .release();
    }
    match std::fs::remove_dir_all(checkout) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// The store stops keeping a converted repository's borrowed objects. Called
/// when that repository is removed; a no-op for any other checkout.
pub(crate) fn forget(checkout: &Path) {
    let Ok(name) = keep_ref(checkout) else {
        return;
    };
    let Ok(alternates) = std::fs::read_to_string(checkout.join(".git/objects/info/alternates"))
    else {
        return;
    };
    for line in alternates.lines() {
        if let Some(store) = Path::new(line.trim()).parent()
            && store.join("HEAD").is_file()
        {
            let _ = store_git(store, None, &["update-ref", "-d", &name]);
        }
    }
}

/// The store ref that keeps a converted checkout's borrowed objects, named
/// from the checkout's canonical path.
fn keep_ref(checkout: &Path) -> Result<String, String> {
    let path =
        std::fs::canonicalize(checkout).map_err(|e| format!("{}: {e}", checkout.display()))?;
    let mut cmd = hardened_git(None).map_err(|e| e.to_string())?;
    cmd.args(["hash-object", "--stdin"]);
    let out =
        output_with_input(&mut cmd, &path.display().to_string()).map_err(|e| e.to_string())?;
    let hex = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if !out.status.success() || hex.is_empty() {
        return Err(format!(
            "cannot name the keep ref of {}",
            checkout.display()
        ));
    }
    Ok(format!("refs/anyagents/keep/{hex}"))
}

fn remove_staging(staging: &Path) {
    match std::fs::symlink_metadata(staging) {
        Ok(meta) if meta.is_dir() => {
            let _ = std::fs::remove_dir_all(staging);
        }
        Ok(_) => {
            let _ = std::fs::remove_file(staging);
        }
        Err(_) => {}
    }
}

/// Every file and directory of a tree on disk before it is exchanged in.
fn sync_tree(root: &Path) -> std::io::Result<()> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                std::fs::File::open(entry.path())?.sync_all()?;
            }
        }
        sync_dir(&dir)?;
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Exchange two paths in one step, so a crash leaves either the old `.git`
/// or the new one, never neither.
fn exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    let ca = std::ffi::CString::new(a.as_os_str().as_bytes())?;
    let cb = std::ffi::CString::new(b.as_os_str().as_bytes())?;
    #[cfg(target_os = "linux")]
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            ca.as_ptr(),
            libc::AT_FDCWD,
            cb.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    #[cfg(target_os = "macos")]
    let rc = unsafe { libc::renamex_np(ca.as_ptr(), cb.as_ptr(), libc::RENAME_SWAP) };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let rc = {
        let _ = (&ca, &cb);
        unsafe { *libc::__error() = libc::ENOSYS };
        -1
    };
    if rc == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if !matches!(
        error.raw_os_error(),
        Some(libc::EINVAL | libc::ENOSYS | libc::ENOTSUP)
    ) {
        return Err(error);
    }
    // A filesystem without an atomic exchange: two renames, undone on failure.
    let aside = a.with_file_name(format!("{ADOPT_STAGING}-old"));
    std::fs::rename(a, &aside)?;
    if let Err(error) = std::fs::rename(b, a) {
        std::fs::rename(&aside, a)?;
        return Err(error);
    }
    std::fs::rename(&aside, b)
}

enum Head {
    Branch(String, String),
    Detached(String),
}

/// A checkout's registration in a shared store, read from a `.git` pointer
/// and accepted only when the registration names this checkout back.
struct Linked {
    private: PathBuf,
    common: PathBuf,
    head: Head,
}

impl Linked {
    fn read(checkout: &Path, pointer_file: &Path) -> Result<Self, String> {
        use std::io::Read as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut text = String::new();
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(pointer_file)
            .and_then(|file| file.take(8193).read_to_string(&mut text))
            .map_err(|e| format!("{}: {e}", pointer_file.display()))?;
        let pointer = text
            .trim()
            .strip_prefix("gitdir: ")
            .filter(|_| text.len() <= 8192)
            .ok_or_else(|| format!("{} is not a gitdir pointer", pointer_file.display()))?;
        let canonical = |path: PathBuf| {
            std::fs::canonicalize(&path).map_err(|e| format!("{}: {e}", path.display()))
        };
        let private = canonical(checkout.join(pointer))?;
        let line = |name: &str| {
            std::fs::read_to_string(private.join(name))
                .map(|text| text.trim().to_string())
                .map_err(|e| format!("{}: {e}", private.join(name).display()))
        };
        if canonical(private.join(line("gitdir")?))? != canonical(checkout.join(".git"))? {
            return Err(format!(
                "{} is registered to another checkout",
                private.display()
            ));
        }
        let common = canonical(private.join(line("commondir")?))?;
        if private.parent() != Some(common.join("worktrees").as_path()) {
            return Err(format!(
                "{} is not a registered worktree",
                private.display()
            ));
        }
        let head = line("HEAD")?;
        let head = match head.strip_prefix("ref: ") {
            Some(name) => {
                let oid = store_git(&common, None, &["rev-parse", "--verify", "--quiet", name])
                    .map_err(|e| format!("{name}: {e}"))?;
                Head::Branch(name.to_string(), oid)
            }
            None if is_commit_id(&head) => Head::Detached(head),
            None => return Err(format!("unreadable HEAD in {}", private.display())),
        };
        Ok(Self {
            private,
            common,
            head,
        })
    }

    /// An operation in progress, or a worktree its owner locked.
    fn busy(&self) -> Option<String> {
        [
            "MERGE_HEAD",
            "CHERRY_PICK_HEAD",
            "REVERT_HEAD",
            "BISECT_LOG",
            "rebase-merge",
            "rebase-apply",
            "sequencer",
            "index.lock",
            "HEAD.lock",
            "modules",
            "locked",
        ]
        .into_iter()
        .find(|name| std::fs::symlink_metadata(self.private.join(name)).is_ok())
        .map(|name| format!("{name} in {}", self.private.display()))
    }

    /// The store will lend objects for good only if it never prunes.
    fn store_keeps_objects(&self) -> Result<(), String> {
        let get = |args: &[&str]| store_git(&self.common, None, args).unwrap_or_default();
        let prune = get(&["config", "--get", "gc.pruneExpire"]);
        let cruft = get(&["config", "--bool", "--get", "gc.cruftPacks"]);
        if prune != "never" || cruft != "true" {
            return Err(format!(
                "{} may prune objects a converted repository borrows: set gc.pruneExpire=never \
                 and gc.cruftPacks=true on it first",
                self.common.display()
            ));
        }
        Ok(())
    }

    fn branch(&self) -> Option<&str> {
        match &self.head {
            Head::Branch(name, _) => name.strip_prefix("refs/heads/"),
            Head::Detached(_) => None,
        }
    }

    fn oid(&self) -> &str {
        match &self.head {
            Head::Branch(_, oid) | Head::Detached(oid) => oid,
        }
    }

    fn build(
        &self,
        checkout: &Path,
        staging: &Path,
        seat: Option<(u32, u32)>,
        base: Option<&str>,
    ) -> Result<(), String> {
        let before = status(seat, &self.private, checkout)?;
        let (c, s) = (&self.common, staging);
        let format = store_git(c, None, &["rev-parse", "--show-object-format"])?;
        plain_git(&[
            "init".as_ref(),
            "--quiet".as_ref(),
            "--bare".as_ref(),
            "--template=".as_ref(),
            format!("--object-format={format}").as_ref(),
            s.as_os_str(),
        ])?;
        for (key, value) in [("core.bare", "false"), ("core.logAllRefUpdates", "true")] {
            store_git(s, None, &["config", key, value])?;
        }
        let objects = std::fs::canonicalize(c.join("objects"))
            .map_err(|e| format!("{}: {e}", c.join("objects").display()))?;
        write_file(
            &s.join("objects/info/alternates"),
            format!("{}\n", objects.display()).as_bytes(),
        )?;
        self.copy_config(s, base)?;
        self.copy_refs(s)?;
        for (from, to) in [
            (self.private.join("index"), s.join("index")),
            (self.private.join("logs/HEAD"), s.join("logs/HEAD")),
            (
                self.private.join("info/sparse-checkout"),
                s.join("info/sparse-checkout"),
            ),
            (c.join("info/exclude"), s.join("info/exclude")),
            (c.join("info/attributes"), s.join("info/attributes")),
            (c.join("shallow"), s.join("shallow")),
        ] {
            copy_if_present(&from, &to)?;
        }
        if let Head::Branch(name, _) = &self.head {
            copy_if_present(&c.join("logs").join(name), &s.join("logs").join(name))?;
        }
        // A split index keeps its shared part beside the index.
        if let Ok(entries) = std::fs::read_dir(&self.private) {
            for entry in entries.flatten() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("sharedindex.")
                {
                    copy_if_present(&entry.path(), &s.join(entry.file_name()))?;
                }
            }
        }
        copy_hooks(&c.join("hooks"), &s.join("hooks"))?;
        // Objects only this checkout reaches leave the store's keeping:
        // unpushed and reflog-only commits, and blobs only the index names.
        let mine = store_git(
            s,
            Some(checkout),
            &[
                "rev-list",
                "--objects",
                "--indexed-objects",
                "--reflog",
                "HEAD",
                "--not",
                "--remotes",
                "--tags",
            ],
        )?;
        if !mine.trim().is_empty() {
            let mut cmd = store_command(s, None).map_err(|e| e.to_string())?;
            cmd.args(["pack-objects", "--quiet"])
                .arg(s.join("objects/pack/pack"));
            let out =
                output_with_input(&mut cmd, &format!("{mine}\n")).map_err(|e| e.to_string())?;
            if !out.status.success() {
                return Err(format!(
                    "cannot copy this checkout's own objects: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
        }
        if let Some((_, gid)) = seat {
            share_tree(s, gid, false, &|_| {})
                .and_then(|()| close_to_others(s))
                .map_err(|e| format!("{}: {e}", s.display()))?;
        }
        let staged = format!("?? {ADOPT_STAGING}/");
        let after: Vec<String> = status(seat, s, checkout)?
            .into_iter()
            .filter(|entry| *entry != staged)
            .collect();
        if after != before {
            return Err(format!(
                "git status differs after conversion: {before:?} became {after:?}"
            ));
        }
        let head = store_git(s, None, &["rev-parse", "--verify", "HEAD"])?;
        if head != self.oid() {
            return Err(format!("HEAD {} became {head}", self.oid()));
        }
        Ok(())
    }

    /// The store's configuration, without the settings of other checkouts'
    /// branches, then this checkout's own `config.worktree` over it, and a
    /// fetch refspec for this checkout's branch and base, so a plain
    /// `git fetch origin` refreshes both.
    fn copy_config(&self, staging: &Path, base: Option<&str>) -> Result<(), String> {
        let list = |file: &Path| -> Result<Vec<(String, String)>, String> {
            if !file.exists() {
                return Ok(Vec::new());
            }
            let raw = plain_git(&[
                "config".as_ref(),
                "--includes".as_ref(),
                "--file".as_ref(),
                file.as_os_str(),
                "--list".as_ref(),
                "-z".as_ref(),
            ])?;
            Ok(raw
                .split('\0')
                .filter(|entry| !entry.is_empty())
                .map(|entry| match entry.split_once('\n') {
                    Some((key, value)) => (key.to_string(), value.to_string()),
                    None => (entry.to_string(), "true".to_string()),
                })
                .collect())
        };
        let common_config = self.common.join("config");
        let mut pairs = list(&common_config)?;
        let worktree_config = pairs.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case("extensions.worktreeconfig")
                && matches!(
                    value.to_ascii_lowercase().as_str(),
                    "true" | "yes" | "on" | "1"
                )
        });
        if worktree_config {
            pairs.extend(list(&self.private.join("config.worktree"))?);
        }
        let branch = self.branch();
        let target = staging.join("config");
        let mut fetch = Vec::new();
        let mut origin = false;
        let mut upstream = None;
        for (key, value) in pairs {
            let (section, rest) = key.split_once('.').unwrap_or((&key, ""));
            let (subsection, variable) = rest.rsplit_once('.').unwrap_or(("", rest));
            let section = section.to_ascii_lowercase();
            if section == "remote" && subsection == "origin" {
                origin |= variable == "url";
                if variable == "fetch" {
                    fetch.push(value.clone());
                }
            }
            if section == "branch" && Some(subsection) == branch && variable == "merge" {
                upstream = value.strip_prefix("refs/heads/").map(str::to_string);
            }
            let keep = match section.as_str() {
                "core" => !matches!(variable, "bare" | "worktree" | "repositoryformatversion"),
                "extensions" | "include" | "includeif" | "safe" => false,
                "branch" => Some(subsection) == branch,
                _ => true,
            };
            if keep {
                plain_git(&[
                    "config".as_ref(),
                    "--file".as_ref(),
                    target.as_os_str(),
                    "--add".as_ref(),
                    key.as_ref(),
                    value.as_ref(),
                ])?;
            }
        }
        if !origin || fetch.iter().any(|spec| spec.contains("refs/heads/*")) {
            return Ok(());
        }
        // A glob, never an exact refspec: a branch not pushed yet, or since
        // deleted upstream, would make every `git fetch origin` fail.
        let wanted = [
            branch.map(str::to_string),
            base.map(str::to_string),
            upstream,
        ];
        for name in wanted.into_iter().flatten() {
            let exact = format!("+refs/heads/{name}:refs/remotes/origin/{name}");
            let glob = format!("+refs/heads/{name}*:refs/remotes/origin/{name}*");
            if fetch.contains(&exact) || fetch.contains(&glob) || !valid_branch(&name) {
                continue;
            }
            plain_git(&[
                "config".as_ref(),
                "--file".as_ref(),
                target.as_os_str(),
                "--add".as_ref(),
                "remote.origin.fetch".as_ref(),
                glob.as_ref(),
            ])?;
            fetch.push(glob);
        }
        Ok(())
    }

    /// This checkout's HEAD and branch, and the store's remote-tracking refs
    /// and tags. Other checkouts' branches stay in the store.
    fn copy_refs(&self, staging: &Path) -> Result<(), String> {
        let listed = store_git(
            &self.common,
            None,
            &[
                "for-each-ref",
                "--format=%(refname)%00%(objectname)%00%(symref)",
                "refs/remotes",
                "refs/tags",
            ],
        )?;
        let mut updates = String::new();
        let mut symbolic = Vec::new();
        for line in listed.lines() {
            let mut fields = line.split('\0');
            let (Some(name), Some(oid), symref) = (fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            match symref.filter(|s| !s.is_empty()) {
                Some(target) => symbolic.push((name.to_string(), target.to_string())),
                None => updates.push_str(&format!("create {name} {oid}\n")),
            }
        }
        if let Head::Branch(name, oid) = &self.head {
            updates.push_str(&format!("create {name} {oid}\n"));
        }
        let mut cmd = store_command(staging, None).map_err(|e| e.to_string())?;
        cmd.args(["update-ref", "--stdin"]);
        let out = output_with_input(&mut cmd, &updates).map_err(|e| e.to_string())?;
        if !out.status.success() {
            return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
        }
        for (name, target) in symbolic {
            store_git(staging, None, &["symbolic-ref", &name, &target])?;
        }
        match &self.head {
            Head::Branch(name, _) => store_git(staging, None, &["symbolic-ref", "HEAD", name]),
            Head::Detached(oid) => {
                store_git(staging, None, &["update-ref", "--no-deref", "HEAD", oid])
            }
        }
        .map(drop)
    }

    /// Point the store's keep ref for this checkout at one commit whose
    /// parents are every commit the new repository's refs reach today, so the
    /// store holds them however its own refs move. Returns the ref.
    fn keep(&self, checkout: &Path, staging: &Path) -> Result<String, String> {
        let listed = store_git(
            staging,
            None,
            &[
                "for-each-ref",
                "--format=%(objecttype) %(objectname) %(*objecttype) %(*objectname)",
            ],
        )?;
        let mut parents = vec![self.oid().to_string()];
        for line in listed.lines() {
            let fields: Vec<&str> = line.split(' ').collect();
            let commit = match fields.as_slice() {
                ["commit", oid, ..] => Some(*oid),
                ["tag", _, "commit", peeled] => Some(*peeled),
                _ => None,
            };
            if let Some(oid) = commit
                && !parents.iter().any(|known| known == oid)
            {
                parents.push(oid.to_string());
            }
        }
        let mut tree = store_command(&self.common, None).map_err(|e| e.to_string())?;
        tree.arg("mktree");
        let tree = output_with_input(&mut tree, "").map_err(|e| e.to_string())?;
        let tree = String::from_utf8_lossy(&tree.stdout).trim().to_string();
        let message = format!(
            "anyagents: keep what {} borrows from this store",
            checkout.display()
        );
        let mut args = vec![
            "-c",
            "user.name=AnyAgents",
            "-c",
            "user.email=anyagents@localhost",
            "commit-tree",
            &tree,
            "-m",
            &message,
        ];
        for parent in &parents {
            args.extend(["-p", parent.as_str()]);
        }
        let commit = store_git(&self.common, None, &args)?;
        let name = keep_ref(checkout)?;
        store_git(&self.common, None, &["update-ref", &name, &commit])?;
        Ok(name)
    }

    /// The store forgets this checkout: its registration goes, and its branch
    /// too when another ref still holds every commit and no other checkout
    /// has it out. A failure here only leaves the store fuller.
    fn release(&self) {
        if let Err(error) = std::fs::remove_dir_all(&self.private) {
            eprintln!(
                "warning: cannot remove worktree registration {}: {error}",
                self.private.display()
            );
        }
        let Head::Branch(name, oid) = &self.head else {
            return;
        };
        let checked_out = std::fs::read_dir(self.common.join("worktrees"))
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path().join("HEAD"))
            .chain(std::iter::once(self.common.join("HEAD")))
            .any(|head| {
                std::fs::read_to_string(head)
                    .is_ok_and(|text| text.trim() == format!("ref: {name}"))
            });
        if checked_out {
            return;
        }
        let contained = store_git(
            &self.common,
            None,
            &["for-each-ref", "--format=%(refname)", "--contains", oid],
        )
        .is_ok_and(|refs| refs.lines().any(|other| other != name));
        if contained
            && let Err(error) = store_git(&self.common, None, &["update-ref", "-d", name, oid])
        {
            eprintln!(
                "warning: cannot release branch {name} in {}: {error}",
                self.common.display()
            );
        }
    }
}

/// The store's hooks, made this repository's own. A linked hook stays a
/// link, to the same absolute target.
fn copy_hooks(from: &Path, to: &Path) -> Result<(), String> {
    let Ok(entries) = std::fs::read_dir(from) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".sample") {
            continue;
        }
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_symlink() {
            let target = std::fs::read_link(entry.path())
                .map_err(|e| format!("{}: {e}", entry.path().display()))?;
            let target = if target.is_absolute() {
                target
            } else {
                from.join(target)
            };
            std::fs::create_dir_all(to).map_err(|e| format!("{}: {e}", to.display()))?;
            std::os::unix::fs::symlink(&target, to.join(&name))
                .map_err(|e| format!("{}: {e}", to.join(&name).display()))?;
        } else if kind.is_file() {
            copy_if_present(&entry.path(), &to.join(&name))?;
        }
    }
    Ok(())
}

/// A branch name Git would accept, checked without running anything.
fn valid_branch(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with(['-', '/', '.'])
        && !name.ends_with(['/', '.'])
        && !name.ends_with(".lock")
        && !name.contains("..")
        && !name.contains("//")
        && !name.contains("@{")
        && name
            .bytes()
            .all(|b| b > b' ' && b != 0x7f && !b"~^:?*[\\".contains(&b))
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    std::fs::write(path, bytes).map_err(|e| format!("{}: {e}", path.display()))
}

/// Copy a regular file with its modification time. The index needs it:
/// Git rechecks an entry's content only while the entry is no older than the
/// index file, so a newer copy would hide a same-size edit made within the
/// same timestamp.
fn copy_if_present(from: &Path, to: &Path) -> Result<(), String> {
    let modified = match std::fs::symlink_metadata(from) {
        Ok(meta) if meta.is_file() => meta.modified(),
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("{}: {error}", from.display())),
    }
    .map_err(|e| format!("{}: {e}", from.display()))?;
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    std::fs::copy(from, to)
        .and_then(|_| std::fs::OpenOptions::new().write(true).open(to))
        .and_then(|file| file.set_modified(modified))
        .map_err(|e| format!("{} -> {}: {e}", from.display(), to.display()))
}

/// `git status` of a checkout read through `git_dir`, as `seat`.
fn status(
    seat: Option<(u32, u32)>,
    git_dir: &Path,
    checkout: &Path,
) -> Result<Vec<String>, String> {
    let out = inspect_command(seat, git_dir, Some(checkout))
        .and_then(|mut cmd| {
            cmd.args(["status", "--porcelain=v1", "-z", "--untracked-files=normal"])
                .stdin(std::process::Stdio::null())
                .output()
        })
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!(
            "git status: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .split('\0')
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect())
}

fn store_command(git_dir: &Path, work_tree: Option<&Path>) -> std::io::Result<Command> {
    inspect_command(None, git_dir, work_tree)
}

fn store_git(git_dir: &Path, work_tree: Option<&Path>, args: &[&str]) -> Result<String, String> {
    let out = store_command(git_dir, work_tree)
        .and_then(|mut cmd| cmd.args(args).stdin(std::process::Stdio::null()).output())
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!(
            "git {}: {}",
            args.first().copied().unwrap_or_default(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Git that reads a repository an agent may control: run as that agent's
/// `seat` when it is not this process, with no inherited environment, no
/// user or system configuration, no hooks and no fsmonitor, and the Git
/// directory and working tree named explicitly, never discovered.
pub(crate) fn inspect_command(
    seat: Option<(u32, u32)>,
    git_dir: &Path,
    work_tree: Option<&Path>,
) -> std::io::Result<Command> {
    let mut cmd = hardened_git(seat)?;
    cmd.arg("--git-dir").arg(git_dir);
    if let Some(work_tree) = work_tree {
        cmd.arg("--work-tree").arg(work_tree);
    }
    Ok(cmd)
}

/// Hardened Git outside any repository: it runs from `/`, which is none.
fn plain_git(args: &[&std::ffi::OsStr]) -> Result<String, String> {
    let out = hardened_git(None)
        .and_then(|mut cmd| cmd.args(args).stdin(std::process::Stdio::null()).output())
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!(
            "git {}: {}",
            args.first()
                .map(|a| a.to_string_lossy())
                .unwrap_or_default(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn hardened_git(seat: Option<(u32, u32)>) -> std::io::Result<Command> {
    let git = anyagents_toolchain::tool("git", "checkout").map_err(std::io::Error::other)?;
    // SAFETY: geteuid and getegid take no arguments and cannot fail.
    let (euid, egid) = unsafe { (libc::geteuid(), libc::getegid()) };
    let other = seat.filter(|&(uid, gid)| euid != uid || egid != gid);
    let mut cmd = match other {
        Some((uid, gid)) => {
            let mut cmd = Command::new(crate::sandbox::launcher());
            cmd.args([
                "--reuid",
                &uid.to_string(),
                "--regid",
                &gid.to_string(),
                "--groups",
                &gid.to_string(),
                "--no-new-privs",
                "--",
            ]);
            cmd.arg(git.path());
            cmd
        }
        None => git.command(),
    };
    cmd.env_clear()
        .current_dir("/")
        .env("PATH", anyagents_toolchain::global().exec_path("repo"))
        .env("HOME", "/nonexistent")
        .env("XDG_CONFIG_HOME", "/nonexistent")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .args([
            "--no-optional-locks",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
        ]);
    Ok(cmd)
}

/// Give the seat's group the checkout: its own `.git` included when it is a
/// repository, only the working tree when `.git` points elsewhere.
pub(crate) fn share(path: &Path, gid: u32) -> Result<(), Error> {
    let own = std::fs::symlink_metadata(path.join(".git")).is_ok_and(|meta| meta.is_dir());
    share_tree(path, gid, !own, &|_| {}).map_err(|e| refusal(path, e))?;
    if own {
        close_to_others(&path.join(".git")).map_err(|e| refusal(path, e))?;
    }
    Ok(())
}

/// An unreadable object owned by another principal is bound with `O_PATH`.
/// Such a descriptor names the exact inode but cannot be read or changed.
#[cfg(unix)]
fn path_only(file: &std::fs::File) -> bool {
    use std::os::fd::AsRawFd;
    #[cfg(target_os = "linux")]
    {
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        flags >= 0 && flags & libc::O_PATH != 0
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file.as_raw_fd();
        false
    }
}

/// Resolve every component without canonicalizing a planted symlink first.
fn open_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    if !path.is_absolute() {
        return Err(std::io::Error::other("metadata path is not absolute"));
    }
    let mut file = std::fs::File::open("/")?;
    for component in path.components() {
        match component {
            std::path::Component::RootDir => {}
            std::path::Component::Normal(name) => {
                let name = std::ffi::CString::new(name.as_encoded_bytes())?;
                let fd = unsafe {
                    libc::openat(
                        file.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                    )
                };
                if fd < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                file = unsafe { std::fs::File::from_raw_fd(fd) };
            }
            _ => return Err(std::io::Error::other("non-normal metadata path")),
        }
    }
    Ok(file)
}

#[cfg(target_os = "linux")]
fn descriptor_privilege_launcher() -> Option<PathBuf> {
    if !crate::sandbox::is_isolated_test_env()
        || std::env::var_os("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").is_some()
    {
        crate::sandbox::descriptor_launcher()
    } else {
        None
    }
}

#[cfg(unix)]
fn descriptor_fchown(
    file: &std::fs::File,
    uid: Option<u32>,
    gid: Option<u32>,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe {
        libc::fchown(
            file.as_raw_fd(),
            uid.unwrap_or(u32::MAX),
            gid.unwrap_or(u32::MAX),
        )
    } == 0
    {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    #[cfg(target_os = "linux")]
    if matches!(err.raw_os_error(), Some(libc::EPERM | libc::EACCES))
        && let Some(launcher) = descriptor_privilege_launcher()
    {
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        if !meta.is_dir() && (!meta.is_file() || meta.nlink() != 1) {
            return Err(err);
        }
        let owner_spec = match (uid, gid) {
            (Some(u), Some(g)) => format!("{u}:{g}"),
            (Some(u), None) => u.to_string(),
            (None, Some(g)) => format!(":{g}"),
            (None, None) => return Ok(()),
        };
        let status = std::process::Command::new(launcher)
            .args([
                "--reuid",
                "0",
                "--regid",
                "0",
                "--clear-groups",
                "--no-new-privs",
                "--",
                "/bin/chown",
                &owner_spec,
                "/dev/fd/0",
            ])
            .stdin(file.try_clone()?)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;
        if status.success() {
            return Ok(());
        }
    }
    Err(err)
}

#[cfg(unix)]
fn descriptor_fchmod(file: &std::fs::File, mode: libc::mode_t) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::fchmod(file.as_raw_fd(), mode) } == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    #[cfg(target_os = "linux")]
    if matches!(err.raw_os_error(), Some(libc::EPERM | libc::EACCES))
        && let Some(launcher) = descriptor_privilege_launcher()
    {
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        if !meta.is_dir() && (!meta.is_file() || meta.nlink() != 1) {
            return Err(err);
        }
        let mode_arg = format!("{:04o}", mode & 0o7777);
        let status = std::process::Command::new(&launcher)
            .args([
                "--reuid",
                &meta.uid().to_string(),
                "--regid",
                &meta.gid().to_string(),
                "--groups",
                &meta.gid().to_string(),
                "--no-new-privs",
                "--",
                "/bin/chmod",
                &mode_arg,
                "/dev/fd/0",
            ])
            .stdin(file.try_clone()?)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;
        if status.success() && (file.metadata()?.mode() as libc::mode_t & 0o7777) == (mode & 0o7777)
        {
            return Ok(());
        }
        let root_status = std::process::Command::new(launcher)
            .args([
                "--reuid",
                "0",
                "--regid",
                "0",
                "--clear-groups",
                "--no-new-privs",
                "--",
                "/bin/chmod",
                &mode_arg,
                "/dev/fd/0",
            ])
            .stdin(file.try_clone()?)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;
        if root_status.success() {
            return Ok(());
        }
    }
    Err(err)
}

#[cfg(unix)]
fn unreadable_foreign(meta: &std::fs::Metadata) -> std::io::Error {
    use std::os::unix::fs::MetadataExt;
    std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        format!(
            "object owned by uid {} is unreadable by the server",
            meta.uid()
        ),
    )
}

/// Give `gid` read and write on a tree: that group, `g+rw`, and setgid
/// directories. Only opened objects change, never a symlink's target, and a
/// hard-linked file is refused. With `skip_git`, entries named `.git` are left
/// alone.
#[cfg(unix)]
fn share_tree(
    path: &Path,
    gid: u32,
    skip_git: bool,
    before_open: &dyn Fn(&Path),
) -> std::io::Result<()> {
    walk_tree(path, skip_git, before_open, &|file, path| {
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        if path_only(file) {
            return Err(unreadable_foreign(&meta));
        }
        let mode = meta.mode() & 0o7777;
        let desired = if meta.is_dir() {
            mode | 0o2770
        } else {
            mode | 0o0660
        };
        if !meta.is_dir() && meta.nlink() > 1 && (meta.gid() != gid || mode != desired) {
            return Err(std::io::Error::other(format!(
                "{}: refusing permission changes to a hard-linked file",
                path.display()
            )));
        }
        if meta.gid() != gid {
            descriptor_fchown(file, None, Some(gid)).map_err(|e| {
                std::io::Error::other(format!("{}: chgrp to {gid}: {e}", path.display()))
            })?;
        }
        if mode != desired {
            descriptor_fchmod(file, desired as libc::mode_t)?;
        }
        let after = file.metadata()?;
        if after.gid() != gid || after.mode() & 0o7777 != desired {
            return Err(std::io::Error::other("permission readback differs"));
        }
        Ok(())
    })
}

#[cfg(unix)]
fn walk_tree(
    path: &Path,
    skip_git: bool,
    before_open: &dyn Fn(&Path),
    apply: &dyn Fn(&std::fs::File, &Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    // Resolve trusted host aliases (e.g. macOS /var -> /private/var) once.
    let resolved = std::fs::canonicalize(path)?;
    let file = open_nofollow(&resolved)?;
    fn visit(
        file: &std::fs::File,
        path: &Path,
        skip_git: bool,
        before_open: &dyn Fn(&Path),
        apply: &dyn Fn(&std::fs::File, &Path) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        let fd = file.as_raw_fd();
        apply(file, path)?;
        if !meta.is_dir() {
            return Ok(());
        }
        // fdopendir owns only the duplicate; closing it does not close file.
        // Close-on-exec, so a process started meanwhile never inherits it.
        let raw = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        if raw < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let dir = unsafe { libc::fdopendir(raw) };
        if dir.is_null() {
            unsafe { libc::close(raw) };
            return Err(std::io::Error::last_os_error());
        }
        struct Directory(*mut libc::DIR);
        impl Drop for Directory {
            fn drop(&mut self) {
                unsafe { libc::closedir(self.0) };
            }
        }
        let dir = Directory(dir);
        loop {
            #[cfg(target_os = "linux")]
            let errno = unsafe { libc::__errno_location() };
            #[cfg(not(target_os = "linux"))]
            let errno = unsafe { libc::__error() };
            unsafe {
                *errno = 0;
            }
            let entry = unsafe { libc::readdir(dir.0) };
            if entry.is_null() {
                if unsafe { *errno } != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                break;
            }
            let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }
            use std::os::unix::ffi::OsStrExt;
            let child_path = path.join(std::ffi::OsStr::from_bytes(name.to_bytes()));
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            if unsafe {
                libc::fstatat(
                    fd,
                    name.as_ptr(),
                    stat.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            let stat = unsafe { stat.assume_init() };
            let kind = stat.st_mode & libc::S_IFMT;
            if kind == libc::S_IFLNK || (skip_git && name.to_bytes() == b".git") {
                continue;
            }
            if kind != libc::S_IFDIR && kind != libc::S_IFREG {
                return Err(std::io::Error::other(format!(
                    "{}: unsupported file type",
                    child_path.display()
                )));
            }
            let flags = libc::O_RDONLY
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK
                | if kind == libc::S_IFDIR {
                    libc::O_DIRECTORY
                } else {
                    0
                };
            before_open(&child_path);
            #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
            let mut child = unsafe { libc::openat(fd, name.as_ptr(), flags) };
            // Each failed open's errno is captured before any later syscall
            // (including ignored repair attempts) can overwrite it.
            #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
            let mut open_error = (child < 0).then(std::io::Error::last_os_error);
            #[cfg(target_os = "linux")]
            if open_error
                .as_ref()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied)
                && (kind == libc::S_IFDIR || stat.st_nlink == 1)
                && descriptor_privilege_launcher().is_some()
            {
                let path_fd = unsafe {
                    libc::openat(
                        fd,
                        name.as_ptr(),
                        libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                    )
                };
                if path_fd >= 0 {
                    let bound = unsafe { std::fs::File::from_raw_fd(path_fd) };
                    #[allow(
                        clippy::unnecessary_cast,
                        reason = "stat fields differ across Unix targets"
                    )]
                    let (stat_dev, stat_ino) = (stat.st_dev as u64, stat.st_ino as u64);
                    if let Ok(bmeta) = bound.metadata()
                        && bmeta.dev() == stat_dev
                        && bmeta.ino() == stat_ino
                        && (bmeta.is_dir() || (bmeta.is_file() && bmeta.nlink() == 1))
                    {
                        let min_mode = ((bmeta.mode() & 0o7777)
                            | if bmeta.is_dir() { 0o770 } else { 0o660 })
                            as libc::mode_t;
                        let _ = descriptor_fchmod(&bound, min_mode);
                        child = unsafe { libc::openat(fd, name.as_ptr(), flags) };
                        open_error = (child < 0).then(std::io::Error::last_os_error);
                        if open_error
                            .as_ref()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied)
                        {
                            let orig_uid = bmeta.uid();
                            let srv_uid = crate::sandbox::server_uid();
                            if orig_uid != srv_uid
                                && descriptor_fchown(&bound, Some(srv_uid), None).is_ok()
                            {
                                child = unsafe { libc::openat(fd, name.as_ptr(), flags) };
                                open_error = (child < 0).then(std::io::Error::last_os_error);
                                let _ = descriptor_fchown(&bound, Some(orig_uid), None);
                            }
                        }
                    }
                }
            }
            if child < 0 {
                let error = open_error
                    .take()
                    .unwrap_or_else(|| std::io::Error::other("open failed"));
                // Another principal's unreadable regular file (e.g. a 0600 pack
                // marker another seat wrote) is bound read-only by inode so the
                // callback can inspect its ACL. Directories are never bound
                // this way: their entries could not be enumerated.
                #[cfg(target_os = "linux")]
                if error.raw_os_error() == Some(libc::EACCES)
                    && kind == libc::S_IFREG
                    && stat.st_uid != crate::sandbox::server_uid()
                {
                    child = unsafe {
                        libc::openat(
                            fd,
                            name.as_ptr(),
                            libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                        )
                    };
                }
                if child < 0 {
                    return Err(error);
                }
            }
            let child = unsafe { std::fs::File::from_raw_fd(child) };
            let opened = child.metadata()?;
            #[allow(
                clippy::unnecessary_cast,
                reason = "stat fields differ across Unix targets"
            )]
            if opened.dev() != stat.st_dev as u64 || opened.ino() != stat.st_ino as u64 {
                return Err(std::io::Error::other(format!(
                    "{}: object replaced during sharing",
                    child_path.display()
                )));
            }
            visit(&child, &child_path, skip_git, before_open, apply)?;
        }
        Ok(())
    }
    visit(&file, path, skip_git, before_open, apply)
}

/// Run admission as the seat that will execute the agent, not as its parent.
/// The probe is this binary's own `internal admission-probe`, so it depends on
/// no host tool but Git, resolved through the toolchain: a host image that
/// wraps `find`, `sh` or `test` cannot break it. A probe as the server's own
/// identity runs in this process.
pub(crate) fn verify(path: &Path, uid: u32, gid: u32, launch: Option<&str>) -> Result<(), Error> {
    let env = probe_env();
    let git = probe_git(&env[0].1).map_err(|e| refusal(path, e))?;
    let seat = |message: String| refusal(path, format!("seat uid {uid}, gid {gid}: {message}"));
    if unsafe { libc::geteuid() } == uid && unsafe { libc::getegid() } == gid {
        return admission_probe(path, &git, Some(&env)).map_err(seat);
    }
    let (program, args) = probe_program().map_err(|e| refusal(path, e))?;
    let mut command = if let Some(spec) = launch {
        let parts: Vec<_> = spec.split('\t').collect();
        if parts.len() != 4 {
            return Err(refusal(path, "invalid seat launcher"));
        }
        let mut command = std::process::Command::new(parts[0]);
        command.args([
            "--reuid",
            parts[1],
            "--regid",
            parts[2],
            "--groups",
            parts[3],
            "--no-new-privs",
            "--",
        ]);
        command.arg(&program);
        command
    } else {
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new(&program);
        // Clear supplementary grants before changing uid. These changes must
        // all run in pre_exec: CommandExt::uid would drop privilege before a
        // pre_exec setgroups call.
        unsafe {
            command.pre_exec(move || {
                if libc::setgroups(1, &gid) != 0 || libc::setgid(gid) != 0 || libc::setuid(uid) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        command
    };
    // The seat's process sees only what the probe needs, never the server's
    // environment (secrets included).
    command
        .args(args)
        .arg("--git")
        .arg(&git)
        .arg("--")
        .arg(path)
        .env_clear()
        .envs(env)
        .current_dir("/")
        .stdin(std::process::Stdio::null());
    // The seat itself executes this binary. A seat that cannot is a
    // deployment fault to name, never a reason to run Rust code after fork.
    let unexecutable = |detail: String| {
        refusal(
            path,
            format!(
                "seat uid {uid} cannot execute {}: {detail}; install AnyAgents where seat \
                 users can execute it (every ancestor directory traversable)",
                program.display()
            ),
        )
    };
    let out = match command.output() {
        Ok(out) => out,
        Err(error) if exec_failure(&error) => return Err(unexecutable(error.to_string())),
        Err(error) => return Err(refusal(path, error)),
    };
    // A launcher (setpriv) that cannot exec the probe exits 126 or 127; the
    // probe itself only ever exits 0, 1 or 2.
    if launch.is_some() && matches!(out.status.code(), Some(126 | 127)) {
        return Err(unexecutable(
            String::from_utf8_lossy(&out.stderr).trim().to_string(),
        ));
    }
    if !out.status.success() {
        return Err(seat(format!(
            "{}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}

/// An `exec` refusal of the program itself, as opposed to the identity
/// change before it (`EPERM` from setgroups/setgid/setuid).
fn exec_failure(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EACCES | libc::ENOENT | libc::ENOEXEC | libc::ENOTDIR | libc::ELOOP)
    )
}

/// The environment of the probe and of the Git it runs: the toolchain's helper
/// `PATH`, and no user or system Git configuration.
fn probe_env() -> Vec<(&'static str, std::ffi::OsString)> {
    vec![
        (
            "PATH",
            anyagents_toolchain::global().exec_path("repo.admission"),
        ),
        ("HOME", "/nonexistent".into()),
        ("XDG_CONFIG_HOME", "/nonexistent".into()),
        ("GIT_CONFIG_GLOBAL", "/dev/null".into()),
        ("GIT_CONFIG_SYSTEM", "/dev/null".into()),
    ]
}

/// The Git the probe runs: the toolchain's declared `git` when it declares
/// one, otherwise the first `git` on the toolchain's helper `PATH`, which is
/// the one the shell probe ran. Never a wrapper earlier on the server's own
/// inherited `PATH`: the desk puts its guard scripts there.
fn probe_git(helper_path: &std::ffi::OsStr) -> std::io::Result<PathBuf> {
    if let Ok(tool) = anyagents_toolchain::tool("git", "worktree admission")
        && !matches!(tool.provenance, anyagents_toolchain::Provenance::Legacy)
    {
        return Ok(tool.path);
    }
    std::env::split_paths(helper_path)
        .map(|dir| dir.join("git"))
        .find(|candidate| candidate.is_file() && may(candidate, libc::X_OK))
        .ok_or_else(|| std::io::Error::other("no git on the toolchain's helper PATH"))
}

/// This binary, asked to run the probe.
#[cfg(not(test))]
fn probe_program() -> std::io::Result<(PathBuf, Vec<&'static str>)> {
    Ok((
        crate::data::current_exe_clean()?,
        vec!["internal", "admission-probe"],
    ))
}

/// Under `cargo test` this binary is the test harness: its probe worker test
/// takes the probe's arguments instead.
#[cfg(test)]
fn probe_program() -> std::io::Result<(PathBuf, Vec<&'static str>)> {
    Ok((
        seat_executable_test_harness()?,
        vec![
            "--exact",
            "repo::tests::admission_probe_worker",
            "--nocapture",
            "--",
        ],
    ))
}

/// The test harness, for a seat to execute in place of this binary. A seat
/// must be able to execute it, as it must execute the server binary, but a
/// build tree usually sits below a home directory no other uid can traverse.
/// The harness therefore runs from one seat-executable copy in a fresh 0755
/// directory under the system temporary directory.
#[cfg(test)]
pub(crate) fn seat_executable_test_harness() -> std::io::Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    static COPY: std::sync::OnceLock<Result<PathBuf, String>> = std::sync::OnceLock::new();
    let copy = COPY.get_or_init(|| {
        let copy = || -> std::io::Result<PathBuf> {
            let harness = std::env::current_exe()?;
            let directory = std::env::temp_dir().join(format!(
                "anyagents-probe-harness-{}-{}",
                std::process::id(),
                anyagents_core::AgentId::generate()
            ));
            std::fs::create_dir(&directory)?;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755))?;
            let copy = directory.join("anyagents-test-harness");
            // A link or a waited `cp`: this process never holds a writable
            // descriptor of an executable a parallel fork could inherit.
            if std::fs::hard_link(&harness, &copy).is_err() {
                let status = std::process::Command::new("/bin/cp")
                    .arg("--")
                    .arg(&harness)
                    .arg(&copy)
                    .status()?;
                if !status.success() {
                    return Err(std::io::Error::other("cannot copy the test harness"));
                }
            }
            std::fs::set_permissions(&copy, std::fs::Permissions::from_mode(0o755))?;
            Ok(copy)
        };
        copy().map_err(|error| error.to_string())
    });
    copy.clone().map_err(std::io::Error::other)
}

/// `anyagents internal admission-probe --git <git> -- <path>`, run as the
/// seat: [`admission_probe`] with the environment its parent gave it. Exits
/// 1 with the reason on stderr, 2 on a malformed call.
pub(crate) fn admission_probe_main(args: &[std::ffi::OsString]) -> std::process::ExitCode {
    let [flag, git, separator, path] = args else {
        eprintln!("usage: anyagents internal admission-probe --git <git> -- <path>");
        return std::process::ExitCode::from(2);
    };
    if flag != "--git" || separator != "--" {
        eprintln!("usage: anyagents internal admission-probe --git <git> -- <path>");
        return std::process::ExitCode::from(2);
    }
    match admission_probe(Path::new(path), Path::new(git), None) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// What the seat must be able to do, checked as the seat: write every file
/// and directory of its working tree (its own `.git` entry aside) and
/// traverse every directory, create a lock file in the tree, in its Git
/// directory and in its `objects/` and `refs/`, and run `git status`.
/// `env`: the environment for Git, or `None` to inherit this process's.
fn admission_probe(
    path: &Path,
    git: &Path,
    env: Option<&[(&'static str, std::ffi::OsString)]>,
) -> Result<(), String> {
    check_tree(path)?;
    probe_lock(path)?;
    match std::fs::symlink_metadata(path.join(".git")) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("{}: {error}", path.join(".git").display())),
    }
    let git = |args: &[&str]| -> Result<std::process::Output, String> {
        let mut command = std::process::Command::new(git);
        if let Some(env) = env {
            command.env_clear().envs(env.iter().cloned());
        }
        let mut safe = std::ffi::OsString::from("safe.directory=");
        safe.push(path);
        let out = command
            .args([
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.hooksPath=/dev/null",
            ])
            .arg("-c")
            .arg(safe)
            .arg("-C")
            .arg(path)
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .map_err(|error| format!("{}: {error}", git.display()))?;
        if out.status.success() {
            Ok(out)
        } else {
            Err(format!(
                "git {}: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        }
    };
    let dir = |flag: &str| -> Result<PathBuf, String> {
        let out = git(&["rev-parse", "--path-format=absolute", flag])?;
        Ok(PathBuf::from(String::from_utf8_lossy(&out.stdout).trim()))
    };
    let private = dir("--git-dir")?;
    let common = dir("--git-common-dir")?;
    for directory in [private, common.join("objects"), common.join("refs")] {
        probe_lock(&directory)?;
    }
    git(&["status", "--porcelain"]).map(drop)
}

/// Every regular file and directory under `root`, symlinks never followed,
/// must be writable, and every directory traversable and listable.
fn check_tree(root: &Path) -> Result<(), String> {
    let refuse = |path: &Path| format!("{}: seat cannot write/traverse", path.display());
    let meta = std::fs::symlink_metadata(root).map_err(|e| format!("{}: {e}", root.display()))?;
    let mut pending = Vec::new();
    if meta.is_dir() {
        pending.push(root.to_path_buf());
    } else if meta.is_file() && !may(root, libc::W_OK) {
        return Err(refuse(root));
    }
    while let Some(directory) = pending.pop() {
        if !may(&directory, libc::W_OK | libc::X_OK) {
            return Err(refuse(&directory));
        }
        let entries = std::fs::read_dir(&directory)
            .map_err(|e| format!("{}: seat cannot list: {e}", directory.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("{}: {e}", directory.display()))?;
            let child = entry.path();
            if directory == root && entry.file_name() == ".git" {
                continue;
            }
            let kind = entry
                .file_type()
                .map_err(|e| format!("{}: {e}", child.display()))?;
            if kind.is_dir() {
                pending.push(child);
            } else if kind.is_file() && !may(&child, libc::W_OK) {
                return Err(refuse(&child));
            }
        }
    }
    Ok(())
}

/// The kernel's own answer for this process's effective identity.
fn may(path: &Path, mode: libc::c_int) -> bool {
    let Ok(name) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else {
        return false;
    };
    unsafe { libc::faccessat(libc::AT_FDCWD, name.as_ptr(), mode, libc::AT_EACCESS) == 0 }
}

/// Create and remove a uniquely named file, as Git's lock files need.
fn probe_lock(directory: &Path) -> Result<(), String> {
    let probe = directory.join(format!(
        ".anyagents-write-check-{}-{}",
        std::process::id(),
        anyagents_core::AgentId::generate()
    ));
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .map_err(|e| format!("{}: seat cannot create a lock: {e}", directory.display()))?;
    std::fs::remove_file(&probe)
        .map_err(|e| format!("{}: seat cannot remove its lock: {e}", directory.display()))
}

/// Replace the legacy wildcard with a reset plus the exact checkout. Preserve
/// other caller settings; bind the final replacement to the opened config dir,
/// never a seat-replaceable .gitconfig symlink or hard-link target.
pub(crate) fn configure_seat_git(config: &Path, workdir: &Path) -> Result<(), Error> {
    use std::io::{Read, Write};
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    };
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(config)?;
    let mut bytes = Vec::new();
    for attempt in 0..8 {
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                c".gitconfig".as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd >= 0 {
            let mut old = unsafe { std::fs::File::from_raw_fd(fd) };
            let meta = old.metadata()?;
            if meta.is_file() && meta.nlink() == 0 && attempt < 7 {
                // Another thread atomically replaced `.gitconfig` via `renameat`
                // between `openat` and `fstat`; reopen the current inode.
                continue;
            }
            if !meta.is_file() || meta.nlink() != 1 || meta.len() > 1024 * 1024 {
                return Err(refusal(config, "unsafe or oversized .gitconfig"));
            }
            old.read_to_end(&mut bytes)?;
            break;
        } else if std::io::Error::last_os_error().kind() != std::io::ErrorKind::NotFound {
            return Err(refusal(config, std::io::Error::last_os_error()));
        } else {
            break;
        }
    }
    let path_env = anyagents_toolchain::global().exec_path("repo");
    let git_tool = anyagents_toolchain::tool("git", "checkout").map_err(|e| refusal(config, e))?;
    // Edits are staged in the agent's root, which the server owns and the
    // seat cannot write, never in the seat-writable config directory.
    let staging = std::fs::canonicalize(
        config
            .parent()
            .ok_or_else(|| refusal(config, "missing agent root"))?,
    )?;
    // Even `git config --file` discovers the caller's repository first. The
    // daemon may not be able to read that checkout under its reduced identity;
    // an unrelated malformed config must not govern this file-only operation.
    let git = || {
        let mut command = git_tool.command();
        command.current_dir(&staging).env("PATH", &path_env);
        for key in [
            "GIT_DIR",
            "GIT_COMMON_DIR",
            "GIT_WORK_TREE",
            "GIT_CONFIG_PARAMETERS",
            "GIT_CONFIG_COUNT",
        ] {
            command.env_remove(key);
        }
        // Explicit non-repository metadata also works for data roots containing
        // ':', which cannot be represented in GIT_CEILING_DIRECTORIES on Unix.
        command.env("GIT_DIR", "/dev/null");
        command
    };
    let temporary = staging.join(format!(
        "gitconfig-{}.tmp",
        anyagents_core::AgentId::generate()
    ));
    let result = (|| -> Result<(), Error> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        let edit = |args: &[&str]| -> Result<std::process::Output, Error> {
            Ok(git()
                .args(["config", "--no-includes", "--file"])
                .arg(&temporary)
                .args(args)
                .output()?)
        };
        let unset = edit(&["--unset-all", "safe.directory"])?;
        if !unset.status.success() && unset.status.code() != Some(5) {
            return Err(refusal(config, "cannot narrow safe.directory"));
        }
        let root = std::path::absolute(workdir)?;
        if root.as_os_str().as_encoded_bytes().contains(&b'*') {
            return Err(refusal(
                workdir,
                "exact safe.directory paths cannot contain wildcard characters",
            ));
        }
        // Generate the suffix with Git's own parser/quoting, then append it
        // after all preserved include sections. Editing an existing [safe]
        // section in place could let a later include re-enable its wildcard.
        let suffix = temporary.with_extension("safe");
        let suffix_result = (|| -> Result<Vec<u8>, Error> {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&suffix)?;
            for root in [String::new(), root.display().to_string()] {
                let out = git()
                    .args(["config", "--no-includes", "--file"])
                    .arg(&suffix)
                    .args(["--add", "safe.directory", &root])
                    .output()?;
                if !out.status.success() {
                    return Err(refusal(config, "cannot record exact safe.directory"));
                }
            }
            Ok(std::fs::read(&suffix)?)
        })();
        let _ = std::fs::remove_file(&suffix);
        let mut bytes = std::fs::read(&temporary)?;
        bytes.push(b'\n');
        bytes.extend_from_slice(&suffix_result?);
        let name = std::ffi::CString::new(format!(
            ".gitconfig-{}",
            anyagents_core::AgentId::generate()
        ))
        .map_err(|e| refusal(config, e))?;
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o640,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        descriptor_fchown(&file, None, Some(directory.metadata()?.gid()))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        if unsafe {
            libc::renameat(
                directory.as_raw_fd(),
                name.as_ptr(),
                directory.as_raw_fd(),
                c".gitconfig".as_ptr(),
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        directory.sync_all()?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&temporary);
    result
}

/// No other principal enters a repository's Git directory: its config and
/// refs are its owner's and its seat's alone.
fn close_to_others(git_dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let file = open_nofollow(&std::fs::canonicalize(git_dir)?)?;
    let mode = file.metadata()?.mode() & 0o7777;
    if mode & 0o007 != 0 {
        descriptor_fchmod(&file, (mode & !0o007) as libc::mode_t)?;
    }
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
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    pub(crate) fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
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

    /// Checkouts with no provider: AnyAgents' own pool under `dir/pool`, and
    /// remotes under `dir/remotes`.
    pub(crate) fn pool_checkouts(dir: &Path) -> Checkouts {
        Checkouts {
            provider: None,
            pool_root: dir.join("pool"),
            remote_base: format!("file://{}", dir.join("remotes").display()),
            author: (None, None),
        }
    }

    /// An upstream `remotes/ExampleOrg/demo.git` with `main` and two files.
    pub(crate) fn seed_remote(dir: &Path) -> PathBuf {
        let seed = dir.join("seed");
        std::fs::create_dir_all(seed.join("src")).unwrap();
        std::fs::create_dir_all(seed.join("other")).unwrap();
        git(&seed, &["init", "-q", "-b", "main"]);
        std::fs::write(seed.join("README.md"), "# Demo\n").unwrap();
        std::fs::write(seed.join("src/keep"), "included\n").unwrap();
        std::fs::write(seed.join("other/skip"), "outside the sparse cone\n").unwrap();
        git(&seed, &["add", "."]);
        git(
            &seed,
            &[
                "-c",
                "user.name=Seed",
                "-c",
                "user.email=seed@example.test",
                "commit",
                "-q",
                "-m",
                "Initial commit",
            ],
        );
        let remote = dir.join("remotes/ExampleOrg/demo.git");
        std::fs::create_dir_all(remote.parent().unwrap()).unwrap();
        git(
            &seed,
            &[
                "clone",
                "-q",
                "--bare",
                "--no-hardlinks",
                ".",
                remote.to_str().unwrap(),
            ],
        );
        remote
    }

    /// Loose plus packed objects of a repository.
    pub(crate) fn object_count(repo: &Path) -> u64 {
        git(repo, &["count-objects", "-v"])
            .lines()
            .filter_map(|line| {
                let (key, value) = line.split_once(": ")?;
                (key == "count" || key == "in-pack").then(|| value.trim().parse::<u64>().ok())
            })
            .flatten()
            .sum()
    }

    fn committer(dir: &Path, args: &[&str]) -> String {
        let mut all = vec![
            "-c",
            "user.name=Agent",
            "-c",
            "user.email=agent@example.test",
        ];
        all.extend_from_slice(args);
        git(dir, &all)
    }

    /// A deployment's checkout provider that makes linked worktrees of its
    /// own store (`source`), with its own sparse-checkout and hook policy.
    pub(crate) struct Fixture {
        pub checkouts: Checkouts,
        pub source: PathBuf,
        pub log: PathBuf,
        pub fail: PathBuf,
    }

    impl Fixture {
        pub(crate) fn new(dir: &Path) -> Self {
            let source = dir.join("source");
            let remote = dir.join("remotes/ExampleOrg/demo.git");
            let executable = dir.join("provider");
            let log = dir.join("provider.log");
            let fail = dir.join("fail");
            std::fs::create_dir_all(source.join("src")).unwrap();
            std::fs::create_dir_all(source.join("excluded")).unwrap();
            std::fs::create_dir_all(remote.parent().unwrap()).unwrap();
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
            // A store that lends objects never prunes them.
            git(&source, &["config", "gc.pruneExpire", "never"]);
            git(&source, &["config", "gc.cruftPacks", "true"]);
            // The provider owns sparse checkout and hooks; AnyAgents adopts
            // its result and must neither reimplement nor lose that policy.
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
    if [ -f {fail:?} ] && [ "$(cat {fail:?})" = merge ]; then
      git -C "$3" rev-parse HEAD > "$(git -C "$3" rev-parse --git-dir)/MERGE_HEAD"
    fi
    ;;
  *) exit 2 ;;
esac
"#
                ),
            );
            Self {
                checkouts: Checkouts {
                    provider: Some(executable),
                    ..pool_checkouts(dir)
                },
                source,
                log,
                fail,
            }
        }

        pub(crate) fn assert_checkout(&self, target: &Path) {
            assert!(
                target.join(".git").is_dir(),
                "{} is not an own repository",
                target.display()
            );
            assert_eq!(
                git(target, &["rev-parse", "--git-common-dir"]),
                git(target, &["rev-parse", "--git-dir"])
            );
            assert_eq!(
                git(target, &["config", "--get", "core.sparseCheckout"]),
                "true"
            );
            assert!(target.join("src/keep").is_file());
            assert!(!target.join("excluded/large").exists());
            for name in ["pre-commit", "pre-push"] {
                let hook = target.join(git(
                    target,
                    &["rev-parse", "--git-path", &format!("hooks/{name}")],
                ));
                assert!(hook.starts_with(target.join(".git")), "{}", hook.display());
                assert_ne!(
                    std::fs::metadata(hook).unwrap().permissions().mode() & 0o111,
                    0
                );
            }
            assert!(
                !git(&self.source, &["worktree", "list", "--porcelain"])
                    .contains(target.to_str().unwrap()),
                "the provider's store still registers {}",
                target.display()
            );
        }
    }

    #[test]
    fn remote_and_local_checkouts_keep_the_provider_policy_in_own_repositories() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        let remote = dir.path().join("remote-work");
        f.checkouts
            .remote(
                "https://github.com/ExampleOrg/demo.git",
                &remote,
                "remote-work",
                Some("main"),
            )
            .unwrap();
        f.assert_checkout(&remote);
        let local = dir.path().join("local-work");
        f.checkouts
            .local(&f.source, &local, "local-work", None)
            .unwrap();
        f.assert_checkout(&local);
        assert_eq!(
            git(&local, &["rev-parse", "HEAD"]),
            git(&f.source, &["rev-parse", "HEAD"])
        );
        // Each branch was released from the store, which keeps what each
        // repository borrows under one keep ref apiece instead.
        let branches = git(&f.source, &["branch", "--format=%(refname:short)"]);
        assert!(!branches.contains("remote-work") && !branches.contains("local-work"));
        let keeps = git(
            &f.source,
            &["for-each-ref", "--format=%(refname)", "refs/anyagents/keep"],
        );
        assert_eq!(keeps.lines().count(), 2, "{keeps}");
        for target in [&remote, &local] {
            assert!(keeps.contains(&keep_ref(target).unwrap()));
        }
        let log = std::fs::read_to_string(&f.log).unwrap();
        assert!(log.contains("fetch ExampleOrg/demo main"));
        assert!(log.contains("fetch ExampleOrg/demo refs/heads/main"));
        assert_eq!(
            log.lines().filter(|l| l.starts_with("worktree ")).count(),
            2
        );
    }

    #[test]
    fn unpushed_head_and_stale_tracking_refs_are_refused_before_the_provider() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        git(&f.source, &["commit", "--allow-empty", "-m", "Not pushed"]);
        git(
            &f.source,
            &["update-ref", "refs/remotes/origin/main", "HEAD"],
        );
        let target = dir.path().join("work");
        let err = f
            .checkouts
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
        f.checkouts.local(&f.source, &target, "work", None).unwrap();
        f.assert_checkout(&target);
    }

    #[test]
    fn missing_remote_refuses_before_the_provider() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        git(&f.source, &["remote", "remove", "origin"]);
        let err = f
            .checkouts
            .local(&f.source, &dir.path().join("work"), "work", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeSourceUnpublished { ref message, .. } if message.contains("no remote")),
            "{err}"
        );
        assert!(!f.log.exists());
    }

    #[test]
    fn provider_failures_keep_its_message_without_fallback() {
        for operation in ["fetch", "worktree"] {
            let dir = tempfile::tempdir().unwrap();
            let f = Fixture::new(dir.path());
            std::fs::write(&f.fail, operation).unwrap();
            let target = dir.path().join("work");
            let err = f
                .checkouts
                .remote("ExampleOrg/demo", &target, "work", Some("main"))
                .unwrap_err();
            assert!(
                matches!(err, Error::WorktreeProvisioning { ref message, .. } if message.contains("injected policy refusal") && message.contains("42")),
                "{err}"
            );
            assert!(!target.exists());
            assert!(!dir.path().join("pool").exists(), "fell back to the pool");
            let log = std::fs::read_to_string(&f.log).unwrap();
            assert_eq!(
                log.lines().count(),
                if operation == "fetch" { 1 } else { 2 }
            );
        }
    }

    /// A provider result that cannot be adopted is removed with its
    /// registration, so the same branch can be provisioned again.
    #[test]
    fn an_unadoptable_provider_checkout_is_discarded_and_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let f = Fixture::new(dir.path());
        std::fs::write(&f.fail, "merge").unwrap();
        let target = dir.path().join("work");
        let err = f
            .checkouts
            .remote("ExampleOrg/demo", &target, "agent/retry", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeProvisioning { ref message, .. } if message.contains("MERGE_HEAD")),
            "{err}"
        );
        assert!(!target.exists());
        assert!(!git(&f.source, &["branch"]).contains("agent/retry"));
        std::fs::remove_file(&f.fail).unwrap();
        f.checkouts
            .remote("ExampleOrg/demo", &target, "agent/retry", None)
            .unwrap();
        f.assert_checkout(&target);
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
        f.checkouts.local(&f.source, &target, "work", None).unwrap();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), selected);
        f.assert_checkout(&target);
    }

    #[test]
    fn an_absent_provider_is_named_and_never_replaced_by_the_pool() {
        let dir = tempfile::tempdir().unwrap();
        seed_remote(dir.path());
        let checkouts = Checkouts {
            provider: Some(dir.path().join("absent-provider")),
            ..pool_checkouts(dir.path())
        };
        let target = dir.path().join("work");
        let err = checkouts
            .remote("ExampleOrg/demo", &target, "work", None)
            .unwrap_err();
        assert!(
            matches!(err, Error::WorktreeProvisioning { ref message, .. } if message.contains("absent-provider")),
            "{err}"
        );
        assert!(!target.exists());
        assert!(!dir.path().join("pool").exists());
    }

    #[test]
    fn generic_remote_sources_are_forwarded_to_the_provider() {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("provider");
        let receipt = dir.path().join("args");
        // A provider that already makes own repositories needs no adoption.
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n[ \"$1\" = worktree ] && git init -q \"$3\"\nexit 0\n",
                receipt.display()
            ),
        );
        let checkouts = Checkouts {
            provider: Some(executable),
            ..pool_checkouts(dir.path())
        };
        let target = dir.path().join("target");
        checkouts
            .remote(
                "https://git.example.test/team/repo.git",
                &target,
                "branch",
                Some("main"),
            )
            .unwrap();
        let args = std::fs::read_to_string(receipt).unwrap();
        assert!(args.contains("fetch https://git.example.test/team/repo.git main"));
        assert!(args.contains("worktree https://git.example.test/team/repo.git"));
        assert!(target.join(".git").is_dir());
        // The pool refuses what it cannot name.
        assert!(
            pool_checkouts(dir.path())
                .remote(
                    "https://git.example.test/team/repo.git",
                    &dir.path().join("other"),
                    "branch",
                    None
                )
                .is_err()
        );
    }

    /// The default path: a repository of its own over AnyAgents' pool. Work in
    /// it (commit, branch, pack, gc) never adds to the pool, and a push
    /// reaches the remote.
    #[test]
    fn a_pool_checkout_is_an_own_repository_that_never_writes_the_pool() {
        let dir = tempfile::tempdir().unwrap();
        let remote = seed_remote(dir.path());
        let checkouts = pool_checkouts(dir.path());
        let target = dir.path().join("agent");
        checkouts
            .remote("ExampleOrg/demo", &target, "agent/one", None)
            .unwrap();
        let pool = dir.path().join("pool/ExampleOrg/demo.git");
        assert!(pool.join("HEAD").is_file());
        assert!(target.join(".git").is_dir() && !target.join(".git").is_symlink());
        assert_eq!(
            git(&target, &["rev-parse", "--git-common-dir"]),
            git(&target, &["rev-parse", "--git-dir"])
        );
        assert_eq!(
            std::fs::read_to_string(target.join(".git/objects/info/alternates")).unwrap(),
            format!(
                "{}\n",
                std::fs::canonicalize(pool.join("objects"))
                    .unwrap()
                    .display()
            )
        );
        assert_eq!(object_count(&target), 0, "a checkout copies no objects");
        assert_eq!(git(&target, &["branch", "--show-current"]), "agent/one");
        assert_eq!(
            git(&target, &["rev-parse", "HEAD"]),
            git(&remote, &["rev-parse", "main"])
        );
        assert_eq!(
            git(&target, &["symbolic-ref", "refs/remotes/origin/HEAD"]),
            "refs/remotes/origin/main"
        );
        assert_eq!(git(&pool, &["config", "gc.pruneExpire"]), "never");
        let pool_objects = object_count(&pool);
        let pool_refs = git(&pool, &["for-each-ref"]);
        std::fs::write(target.join("src/keep"), "changed\n").unwrap();
        committer(&target, &["commit", "-qam", "Agent work"]);
        git(&target, &["branch", "scratch"]);
        git(&target, &["pack-refs", "--all"]);
        git(&target, &["gc", "-q"]);
        assert_eq!(object_count(&pool), pool_objects, "the pool gained objects");
        assert_eq!(
            git(&pool, &["for-each-ref"]),
            pool_refs,
            "the pool's refs moved"
        );
        assert!(object_count(&target) > 0, "new objects are the agent's own");
        git(&target, &["push", "-q", "origin", "agent/one"]);
        assert_eq!(
            git(&remote, &["rev-parse", "agent/one"]),
            git(&target, &["rev-parse", "HEAD"])
        );
        assert_eq!(
            git(
                &target,
                &["rev-list", "HEAD", "--not", "--remotes", "--count"]
            ),
            "0"
        );
        git(&target, &["fsck", "--connectivity-only", "--no-progress"]);
    }

    /// Two agents on one repository share only the pool's objects: neither
    /// sees the other's branches or configuration, and nothing either does
    /// reaches the pool's refs.
    #[test]
    fn two_agents_on_one_repository_share_no_refs_or_config() {
        let dir = tempfile::tempdir().unwrap();
        seed_remote(dir.path());
        let checkouts = pool_checkouts(dir.path());
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        checkouts
            .remote("ExampleOrg/demo", &a, "agent/a", None)
            .unwrap();
        checkouts
            .remote("ExampleOrg/demo", &b, "agent/b", None)
            .unwrap();
        git(&a, &["config", "user.name", "Agent A"]);
        git(&a, &["config", "core.hooksPath", "/nonexistent/a-hooks"]);
        committer(&a, &["commit", "-q", "--allow-empty", "-m", "A only"]);
        git(&a, &["branch", "a-private"]);
        let refs = |dir: &Path| git(dir, &["for-each-ref", "--format=%(refname)"]);
        assert!(!refs(&b).contains("agent/a") && !refs(&b).contains("a-private"));
        assert!(!refs(&a).contains("agent/b"));
        assert!(
            git(&b, &["config", "--list", "--show-origin"])
                .lines()
                .all(|line| { !line.contains("Agent A") && !line.contains("a-hooks") })
        );
        let pool = dir.path().join("pool/ExampleOrg/demo.git");
        assert!(
            refs(&pool)
                .lines()
                .all(|name| name.starts_with("refs/remotes/origin/")),
            "{}",
            refs(&pool)
        );
        let head_a = git(&a, &["rev-parse", "HEAD"]);
        let missing = Command::new("git")
            .arg("-C")
            .arg(&b)
            .args(["cat-file", "-e", &head_a])
            .status()
            .unwrap();
        assert!(!missing.success(), "B can read A's unpushed commit");
        // What one agent publishes, the other fetches from the remote.
        git(&a, &["push", "-q", "origin", "agent/a"]);
        git(&b, &["fetch", "-q", "origin", "agent/a"]);
        assert_eq!(git(&b, &["rev-parse", "origin/agent/a"]), head_a);
    }

    #[test]
    fn author_identity_is_the_repository_s_own_and_must_be_complete() {
        let dir = tempfile::tempdir().unwrap();
        seed_remote(dir.path());
        let partial = Checkouts {
            author: (Some("Selected Author".into()), None),
            ..pool_checkouts(dir.path())
        };
        assert!(
            partial
                .remote("ExampleOrg/demo", &dir.path().join("partial"), "p", None)
                .is_err()
        );
        assert!(!dir.path().join("partial").exists());
        let complete = Checkouts {
            author: (
                Some("Selected Author".into()),
                Some("selected@example.test".into()),
            ),
            ..pool_checkouts(dir.path())
        };
        let target = dir.path().join("complete");
        complete
            .remote("ExampleOrg/demo", &target, "c", None)
            .unwrap();
        assert_eq!(
            git(&target, &["config", "--local", "--get", "user.name"]),
            "Selected Author"
        );
        assert_eq!(
            git(&target, &["config", "--local", "--get", "user.email"]),
            "selected@example.test"
        );
    }

    /// Adopt, failing with the reason when it declines.
    fn converted(checkout: &Path) {
        match adopt(checkout, None) {
            Adoption::Converted => {}
            Adoption::Own => panic!("{} was already an own repository", checkout.display()),
            Adoption::Declined(reason) => panic!("declined: {reason}"),
        }
    }

    /// A store with a linked worktree that holds every kind of local work.
    struct Legacy {
        hub: PathBuf,
        work: PathBuf,
        sibling: PathBuf,
        private: PathBuf,
    }

    fn legacy(dir: &Path) -> Legacy {
        let remote = seed_remote(dir);
        let hub = dir.join("hub");
        git(
            dir,
            &[
                "clone",
                "-q",
                remote.to_str().unwrap(),
                hub.to_str().unwrap(),
            ],
        );
        git(&hub, &["config", "user.name", "Store Owner"]);
        git(&hub, &["config", "branch.someone-else.remote", "origin"]);
        git(&hub, &["config", "gc.pruneExpire", "never"]);
        git(&hub, &["config", "gc.cruftPacks", "true"]);
        std::fs::write(hub.join(".git/info/exclude"), "*.log\n").unwrap();
        write_executable(&hub.join(".git/hooks/pre-push"), "#!/bin/sh\nexit 0\n");
        let work = dir.join("work");
        let sibling = dir.join("sibling");
        git(
            &hub,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "agent/w",
                work.to_str().unwrap(),
            ],
        );
        git(
            &hub,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "agent/sibling",
                sibling.to_str().unwrap(),
            ],
        );
        git(&work, &["sparse-checkout", "set", "src"]);
        // A commit only the reflog still names: abandoned with a reset.
        committer(&work, &["commit", "-q", "--allow-empty", "-m", "Abandoned"]);
        git(&work, &["reset", "-q", "--hard", "HEAD~1"]);
        committer(&work, &["commit", "-q", "--allow-empty", "-m", "Unpushed"]);
        std::fs::write(work.join("src/staged"), "staged only\n").unwrap();
        git(&work, &["add", "src/staged"]);
        std::fs::write(work.join("src/keep"), "modified\n").unwrap();
        std::fs::write(work.join("src/untracked"), "untracked\n").unwrap();
        std::fs::write(work.join("src/ignored.log"), "ignored\n").unwrap();
        let private = PathBuf::from(git(
            &work,
            &["rev-parse", "--path-format=absolute", "--git-dir"],
        ));
        Legacy {
            hub,
            work,
            sibling,
            private,
        }
    }

    fn tree_bytes(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        let mut out = std::collections::BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.file_name().is_some_and(|n| n == ".git") {
                    continue;
                }
                if path.is_dir() {
                    pending.push(path);
                } else {
                    out.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        std::fs::read(&path).unwrap(),
                    );
                }
            }
        }
        out
    }

    /// Migration of an existing worktree: converted in place, every kind of
    /// local work intact, the store forgetting only this checkout. Its unique
    /// objects survive even a store that is pruned afterwards.
    #[test]
    fn a_linked_worktree_converts_in_place_without_losing_work() {
        let dir = tempfile::tempdir().unwrap();
        let l = legacy(dir.path());
        let status = |dir: &Path| git(dir, &["status", "--porcelain", "--untracked-files=all"]);
        let before_status = status(&l.work);
        let before_head = git(&l.work, &["rev-parse", "HEAD"]);
        let before_files = tree_bytes(&l.work);
        let before_sibling = git(&l.sibling, &["rev-parse", "HEAD"]);
        converted(&l.work);
        assert!(l.work.join(".git").is_dir());
        assert!(!l.work.join(ADOPT_STAGING).exists());
        assert!(
            !l.private.exists(),
            "the store still registers the checkout"
        );
        assert_eq!(status(&l.work), before_status);
        assert_eq!(git(&l.work, &["rev-parse", "HEAD"]), before_head);
        assert_eq!(git(&l.work, &["branch", "--show-current"]), "agent/w");
        assert_eq!(
            tree_bytes(&l.work),
            before_files,
            "the working tree changed"
        );
        assert_eq!(git(&l.work, &["config", "core.sparseCheckout"]), "true");
        assert!(!l.work.join("other").exists(), "sparse patterns were lost");
        assert_eq!(git(&l.work, &["config", "user.name"]), "Store Owner");
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&l.work)
                .args(["config", "--get", "branch.someone-else.remote"])
                .output()
                .unwrap()
                .stdout
                .is_empty(),
            "another checkout's branch settings came along"
        );
        assert!(l.work.join(".git/hooks/pre-push").is_file());
        assert_eq!(
            git(&l.work, &["diff", "--cached", "--name-only"]),
            "src/staged"
        );
        // The sibling is untouched. The store keeps what the new repository
        // borrows under its keep ref, so the branch is released.
        assert_eq!(git(&l.sibling, &["rev-parse", "HEAD"]), before_sibling);
        assert!(!git(&l.hub, &["branch"]).contains("agent/w"));
        let keep = keep_ref(&l.work).unwrap();
        assert!(
            git(
                &l.hub,
                &["merge-base", "--is-ancestor", &before_head, &keep]
            )
            .is_empty(),
            "the keep ref does not hold HEAD"
        );
        // Unpushed, staged and reflog-only work is the repository's own:
        // a store pruning everything but its remote-tracking refs cannot
        // take it.
        let abandoned = git(&l.work, &["log", "-g", "--format=%H %s", "HEAD"])
            .lines()
            .find_map(|line| line.strip_suffix(" Abandoned").map(str::to_string))
            .expect("the reflog keeps the abandoned commit");
        forget(&l.work);
        assert!(git(&l.hub, &["for-each-ref", &keep]).is_empty());
        git(&l.hub, &["reflog", "expire", "--expire=now", "--all"]);
        git(&l.hub, &["gc", "-q", "--prune=now"]);
        git(&l.work, &["fsck", "--connectivity-only", "--no-progress"]);
        assert_eq!(git(&l.work, &["rev-parse", "HEAD"]), before_head);
        assert_eq!(git(&l.work, &["show", ":src/staged"]), "staged only");
        git(
            &l.work,
            &["cat-file", "-e", &format!("{abandoned}^{{commit}}")],
        );
        assert!(matches!(adopt(&l.work, None), Adoption::Own));
    }

    /// The review's repro: a converted checkout borrows its base from the
    /// store. Upstream rewrites the base, the store refetches, expires its
    /// reflogs and prunes everything unreachable now; the keep ref still
    /// holds the base, so the repository stays whole and can push. Its fetch
    /// refspec covers the base, so a plain `git fetch origin` sees the rewrite.
    #[test]
    fn a_converted_repository_survives_its_base_moving_and_the_store_pruning() {
        let dir = tempfile::tempdir().unwrap();
        let remote = seed_remote(dir.path());
        let seed = dir.path().join("seed");
        git(&seed, &["checkout", "-q", "-b", "feat"]);
        std::fs::write(seed.join("f"), "feat\n").unwrap();
        git(&seed, &["add", "f"]);
        committer(&seed, &["commit", "-qm", "feat v1"]);
        git(&seed, &["push", "-q", remote.to_str().unwrap(), "feat"]);
        let hub = dir.path().join("hub");
        git(
            dir.path(),
            &[
                "clone",
                "-q",
                "--single-branch",
                "-b",
                "main",
                remote.to_str().unwrap(),
                hub.to_str().unwrap(),
            ],
        );
        git(
            &hub,
            &[
                "fetch",
                "-q",
                "origin",
                "+refs/heads/feat:refs/remotes/origin/feat",
            ],
        );
        let work = dir.path().join("work");
        git(
            &hub,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "agent/w",
                work.to_str().unwrap(),
                "origin/feat",
            ],
        );
        // A store that may prune is refused, and nothing changes.
        assert!(
            matches!(adopt(&work, None), Adoption::Declined(ref why) if why.contains("gc.pruneExpire"))
        );
        assert!(work.join(".git").is_file());
        git(&hub, &["config", "gc.pruneExpire", "never"]);
        git(&hub, &["config", "gc.cruftPacks", "true"]);
        // As at a provider spawn, which knows the base it asked for.
        match adopt_from(&work, None, Some("feat"), false) {
            Adoption::Converted => {}
            Adoption::Declined(reason) => panic!("declined: {reason}"),
            Adoption::Own => panic!("already own"),
        }
        std::fs::write(work.join("mine"), "mine\n").unwrap();
        git(&work, &["add", "mine"]);
        committer(&work, &["commit", "-qm", "unpushed"]);
        let v1 = git(&work, &["rev-parse", "origin/feat"]);
        // Upstream rewrites feat; the store refetches, its reflog goes, and it
        // prunes now, overriding its own never.
        committer(&seed, &["commit", "-q", "--amend", "-m", "feat v2"]);
        git(&seed, &["push", "-qf", remote.to_str().unwrap(), "feat"]);
        git(
            &hub,
            &[
                "fetch",
                "-q",
                "origin",
                "+refs/heads/feat:refs/remotes/origin/feat",
            ],
        );
        git(
            &hub,
            &["reflog", "expire", "--expire-unreachable=now", "--all"],
        );
        git(&hub, &["gc", "-q", "--prune=now"]);
        git(&work, &["fsck", "--connectivity-only", "--no-progress"]);
        git(&work, &["log", "--oneline"]);
        git(&work, &["push", "-q", remote.to_str().unwrap(), "agent/w"]);
        // A plain fetch refreshes the base.
        git(&work, &["fetch", "-q", "origin"]);
        let v2 = git(&work, &["rev-parse", "origin/feat"]);
        assert_ne!(v1, v2);
        assert_eq!(v2, git(&remote, &["rev-parse", "feat"]));
        let specs = git(&work, &["config", "--get-all", "remote.origin.fetch"]);
        assert!(specs.contains("refs/heads/feat*"), "{specs}");
        assert!(specs.contains("refs/heads/agent/w*"), "{specs}");
    }

    /// A conversion that crashed after the exchange is finished by the next
    /// call: the store forgets the registration and the old pointer goes.
    #[test]
    fn an_interrupted_conversion_is_finished_on_the_next_call() {
        let dir = tempfile::tempdir().unwrap();
        let l = legacy(dir.path());
        let head = git(&l.work, &["rev-parse", "HEAD"]);
        assert!(matches!(
            adopt_from(&l.work, None, None, true),
            Adoption::Converted
        ));
        assert!(l.work.join(".git").is_dir());
        assert!(
            l.work.join(ADOPT_STAGING).is_file(),
            "no pointer to finish from"
        );
        assert!(l.private.exists());
        assert!(matches!(adopt(&l.work, None), Adoption::Own));
        assert!(
            !l.private.exists(),
            "the store still registers the checkout"
        );
        assert!(!l.work.join(ADOPT_STAGING).exists());
        assert!(!git(&l.hub, &["worktree", "list"]).contains(l.work.to_str().unwrap()));
        assert_eq!(git(&l.work, &["rev-parse", "HEAD"]), head);
        // A forged leftover pointer at another registration releases nothing.
        std::fs::copy(l.sibling.join(".git"), l.work.join(ADOPT_STAGING)).unwrap();
        assert!(matches!(adopt(&l.work, None), Adoption::Own));
        assert!(l.sibling.join(".git").is_file());
        assert!(git(&l.hub, &["worktree", "list"]).contains(l.sibling.to_str().unwrap()));
        assert!(!l.work.join(ADOPT_STAGING).exists());
    }

    /// A staging directory an earlier attempt left behind is cleared, and the
    /// conversion goes ahead.
    #[test]
    fn a_leftover_staging_directory_is_cleared_on_retry() {
        let dir = tempfile::tempdir().unwrap();
        let l = legacy(dir.path());
        let staging = l.work.join(ADOPT_STAGING);
        std::fs::create_dir_all(staging.join("objects/info")).unwrap();
        std::fs::write(staging.join("HEAD"), "junk\n").unwrap();
        std::fs::write(staging.join("objects/info/alternates"), "/nonexistent\n").unwrap();
        converted(&l.work);
        assert!(!staging.exists());
        git(&l.work, &["fsck", "--connectivity-only", "--no-progress"]);
    }

    /// A worktree its owner locked is left alone.
    #[test]
    fn a_locked_worktree_is_not_converted() {
        let dir = tempfile::tempdir().unwrap();
        let l = legacy(dir.path());
        git(&l.hub, &["worktree", "lock", l.work.to_str().unwrap()]);
        assert!(
            matches!(adopt(&l.work, None), Adoption::Declined(ref why) if why.contains("locked"))
        );
        assert!(l.work.join(".git").is_file());
        assert!(l.private.join("locked").exists());
    }

    /// A store hook that is a link stays a link to the same file, absolute
    /// or relative, and still runs.
    #[test]
    fn linked_hooks_are_carried_over_as_links() {
        let dir = tempfile::tempdir().unwrap();
        let l = legacy(dir.path());
        let script = dir.path().join("shared-hook");
        write_executable(&script, "#!/bin/sh\nexit 0\n");
        let hooks = l.hub.join(".git/hooks");
        std::os::unix::fs::symlink(&script, hooks.join("pre-commit")).unwrap();
        write_executable(&hooks.join("real-commit-msg"), "#!/bin/sh\nexit 0\n");
        std::os::unix::fs::symlink("real-commit-msg", hooks.join("commit-msg")).unwrap();
        converted(&l.work);
        let own = l.work.join(".git/hooks");
        assert_eq!(std::fs::read_link(own.join("pre-commit")).unwrap(), script);
        // The same file, however the platform spells its temporary root
        // (macOS: /var is /private/var).
        let target = std::fs::read_link(own.join("commit-msg")).unwrap();
        assert!(target.is_absolute(), "{}", target.display());
        assert_eq!(
            std::fs::canonicalize(target).unwrap(),
            std::fs::canonicalize(hooks.join("real-commit-msg")).unwrap()
        );
        committer(
            &l.work,
            &["commit", "-q", "--allow-empty", "-m", "hooks run"],
        );
    }

    /// Syncing a staging tree reaches every file and directory and never
    /// follows a link out of it.
    #[test]
    fn syncing_a_tree_skips_links() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("a/b")).unwrap();
        std::fs::write(dir.path().join("a/b/file"), "x").unwrap();
        std::os::unix::fs::symlink("/nonexistent", dir.path().join("a/dangling")).unwrap();
        sync_tree(dir.path()).unwrap();
    }

    /// A same-size edit made within the index's own timestamp is visible to
    /// Git only because the entry is racily clean. The converted index keeps
    /// that timestamp, so the edit stays visible instead of reading as clean.
    #[test]
    fn a_racily_clean_edit_stays_visible_after_conversion() {
        let dir = tempfile::tempdir().unwrap();
        let l = legacy(dir.path());
        // Only timestamps decide, as on a filesystem without usable ctime.
        git(&l.hub, &["config", "core.trustctime", "false"]);
        let debug = git(&l.work, &["ls-files", "--debug", "src/keep"]);
        let (sec, nsec) = debug
            .lines()
            .find_map(|line| line.trim().strip_prefix("mtime: "))
            .and_then(|stamp| stamp.split_once(':'))
            .map(|(s, n)| (s.parse::<u64>().unwrap(), n.parse::<u32>().unwrap()))
            .unwrap();
        let stamp = std::time::UNIX_EPOCH + std::time::Duration::new(sec, nsec);
        for path in [l.work.join("src/keep"), l.private.join("index")] {
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(stamp)
                .unwrap();
        }
        let status = |dir: &Path| git(dir, &["--no-optional-locks", "status", "--porcelain"]);
        assert!(
            status(&l.work)
                .lines()
                .any(|line| line.trim() == "M src/keep")
        );
        converted(&l.work);
        assert!(
            status(&l.work)
                .lines()
                .any(|line| line.trim() == "M src/keep"),
            "the edit reads as clean"
        );
    }

    /// A checkout in the middle of an operation is left exactly as it is.
    #[test]
    fn a_worktree_with_an_operation_in_progress_is_not_converted() {
        let dir = tempfile::tempdir().unwrap();
        let l = legacy(dir.path());
        std::fs::write(
            l.private.join("MERGE_HEAD"),
            git(&l.work, &["rev-parse", "HEAD"]),
        )
        .unwrap();
        let pointer = std::fs::read(l.work.join(".git")).unwrap();
        assert!(
            matches!(adopt(&l.work, None), Adoption::Declined(ref why) if why.contains("MERGE_HEAD"))
        );
        assert_eq!(std::fs::read(l.work.join(".git")).unwrap(), pointer);
        assert!(l.private.join("index").is_file());
        assert!(!l.work.join(ADOPT_STAGING).exists());
        // A pointer at another checkout's registration is refused too.
        std::fs::remove_file(l.private.join("MERGE_HEAD")).unwrap();
        let sibling_pointer = std::fs::read(l.sibling.join(".git")).unwrap();
        std::fs::write(l.work.join(".git"), &sibling_pointer).unwrap();
        assert!(
            matches!(adopt(&l.work, None), Adoption::Declined(ref why) if why.contains("registered to another checkout"))
        );
        assert!(l.sibling.join(".git").is_file());
    }

    /// Discarding a failed creation's linked checkout removes only its
    /// registration, and its branch only when no commit would be lost.
    #[test]
    fn discard_never_deletes_unique_commits_or_other_registrations() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        git(&source, &["init", "-q", "-b", "main"]);
        committer(&source, &["commit", "-q", "--allow-empty", "-m", "base"]);
        let untouched = dir.path().join("untouched");
        let worked = dir.path().join("worked");
        let sibling = dir.path().join("sibling");
        for (path, branch) in [
            (&untouched, "agent/untouched"),
            (&worked, "agent/worked"),
            (&sibling, "agent/sibling"),
        ] {
            git(
                &source,
                &[
                    "worktree",
                    "add",
                    "-q",
                    "-b",
                    branch,
                    path.to_str().unwrap(),
                ],
            );
        }
        committer(&worked, &["commit", "-q", "--allow-empty", "-m", "unique"]);
        discard(&untouched).unwrap();
        discard(&worked).unwrap();
        assert!(!untouched.exists() && !worked.exists());
        let branches = git(&source, &["branch", "--format=%(refname:short)"]);
        assert!(
            !branches.lines().any(|b| b == "agent/untouched"),
            "{branches}"
        );
        assert!(
            branches.lines().any(|b| b == "agent/worked"),
            "unique work lost: {branches}"
        );
        let worktrees = git(&source, &["worktree", "list", "--porcelain"]);
        assert!(worktrees.contains(sibling.to_str().unwrap()));
        assert!(!worktrees.contains(untouched.to_str().unwrap()));
        assert!(sibling.join(".git").exists());
    }

    #[test]
    fn sharing_an_own_repository_closes_its_git_directory_to_others() {
        let dir = tempfile::tempdir().unwrap();
        seed_remote(dir.path());
        let target = dir.path().join("agent");
        pool_checkouts(dir.path())
            .remote("ExampleOrg/demo", &target, "agent/one", None)
            .unwrap();
        std::fs::set_permissions(target.join(".git"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        share(&target, crate::sandbox::server_gid()).unwrap();
        let mode = std::fs::metadata(target.join(".git")).unwrap().mode();
        assert_eq!(mode & 0o007, 0, "{mode:o}");
        assert_eq!(mode & 0o070, 0o070, "{mode:o}");
        let config = std::fs::metadata(target.join(".git/config"))
            .unwrap()
            .mode();
        assert_eq!(config & 0o060, 0o060, "{config:o}");
    }

    /// Two seats, two uids, one repository: each commits and pushes in its
    /// own checkout as itself, and neither can read the other's Git
    /// directory. Requires root (the CI privileged lane runs it).
    #[cfg(target_os = "linux")]
    #[test]
    fn privileged_seats_own_their_repositories() {
        if unsafe { libc::geteuid() } != 0 {
            assert!(
                std::env::var_os("ANYAGENTS_REQUIRE_PRIVILEGED_WORKTREE_TEST").is_none(),
                "requires root to run two seat uids"
            );
            eprintln!("requires root; two-seat repository proof not executed");
            return;
        }
        let dir = tempfile::tempdir_in("/var/tmp").unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let remote = seed_remote(dir.path());
        git(&remote, &["config", "core.sharedRepository", "0666"]);
        assert!(
            Command::new("chmod")
                .args(["-R", "a+rwX"])
                .arg(&remote)
                .status()
                .unwrap()
                .success()
        );
        let checkouts = pool_checkouts(dir.path());
        let pool = dir.path().join("pool/ExampleOrg/demo.git");
        let seats = [(24051_u32, 24051_u32), (24052, 24052)];
        let mut work = Vec::new();
        for (index, (uid, gid)) in seats.into_iter().enumerate() {
            let target = dir.path().join(format!("seat-{index}"));
            checkouts
                .remote("ExampleOrg/demo", &target, &format!("agent/{index}"), None)
                .unwrap();
            share(&target, gid).unwrap();
            verify(
                &target,
                uid,
                gid,
                Some(&format!("/usr/bin/setpriv\t{uid}\t{gid}\t{gid}")),
            )
            .expect("seat admission probe");
            work.push((target, uid, gid));
        }
        let pool_objects = object_count(&pool);
        // The bare remote stands in for a host the seats push to; its
        // receive-pack reads only global configuration.
        let seat_config = dir.path().join("seat.gitconfig");
        std::fs::write(
            &seat_config,
            format!("[safe]\n\tdirectory = {}\n", remote.display()),
        )
        .unwrap();
        std::fs::set_permissions(&seat_config, std::fs::Permissions::from_mode(0o644)).unwrap();
        let as_seat = |uid: u32, gid: u32, dir: &Path, args: &[&str]| {
            Command::new("/usr/bin/setpriv")
                .args([
                    "--reuid",
                    &uid.to_string(),
                    "--regid",
                    &gid.to_string(),
                    "--groups",
                    &gid.to_string(),
                    "--no-new-privs",
                    "--",
                    "env",
                    "HOME=/nonexistent",
                    &format!("GIT_CONFIG_GLOBAL={}", seat_config.display()),
                    "GIT_CONFIG_SYSTEM=/dev/null",
                    "git",
                ])
                .arg("-c")
                .arg(format!("safe.directory={}", dir.display()))
                .args(["-c", "user.name=Seat", "-c", "user.email=seat@example.test"])
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap()
        };
        for (index, (target, uid, gid)) in work.iter().enumerate() {
            std::fs::write(target.join("src/keep"), format!("seat {index}\n")).unwrap();
            for args in [
                &["commit", "-qam", "seat work"][..],
                &["branch", "seat-private"][..],
                &["config", "seat.marker", "own"][..],
                &["push", "-q", "origin", &format!("agent/{index}")][..],
            ] {
                let out = as_seat(*uid, *gid, target, args);
                assert!(
                    out.status.success(),
                    "seat {index} {args:?}: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
        }
        let (a, _, _) = &work[0];
        let (_, b_uid, b_gid) = &work[1];
        let peek = as_seat(*b_uid, *b_gid, a, &["for-each-ref"]);
        assert!(!peek.status.success(), "seat 1 read seat 0's refs");
        let config = Command::new("/usr/bin/setpriv")
            .args([
                "--reuid",
                &b_uid.to_string(),
                "--regid",
                &b_gid.to_string(),
                "--groups",
                &b_gid.to_string(),
                "--",
                "cat",
            ])
            .arg(a.join(".git/config"))
            .output()
            .unwrap();
        assert!(!config.status.success(), "seat 1 read seat 0's config");
        assert_eq!(object_count(&pool), pool_objects, "a seat wrote the pool");
        for (index, _) in work.iter().enumerate() {
            assert!(!git(&remote, &["branch", "--list", &format!("agent/{index}")]).is_empty());
        }
    }

    #[test]
    fn seat_git_configuration_ignores_ambient_repository() {
        const FIXTURE: &str = "ANYAGENTS_SEAT_GIT_FIXTURE";
        if let Some(root) = std::env::var_os(FIXTURE) {
            let root = PathBuf::from(root);
            let config = root.join("data/agents/synthetic/config");
            let workdir = root.join("synthetic-work");
            crate::create::ensure_seat_environment(&config, &workdir).unwrap();
            let output = std::process::Command::new("git")
                .current_dir(&config)
                .env("GIT_DIR", "/dev/null")
                .env_remove("GIT_CONFIG_PARAMETERS")
                .env_remove("GIT_CONFIG_COUNT")
                .args(["config", "--no-includes", "--file"])
                .arg(config.join(".gitconfig"))
                .args(["--get-all", "safe.directory"])
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            assert_eq!(
                output.stdout,
                format!("\n{}\n", workdir.display()).as_bytes()
            );
            assert!(
                std::fs::read_to_string(config.join(".gitconfig"))
                    .unwrap()
                    .contains("確認")
            );
            let invalid = b"[broken\n";
            std::fs::write(config.join(".gitconfig"), invalid).unwrap();
            let error = crate::create::ensure_seat_environment(&config, &workdir).unwrap_err();
            assert!(
                error.to_string().contains("cannot narrow safe.directory"),
                "{error}"
            );
            assert_eq!(std::fs::read(config.join(".gitconfig")).unwrap(), invalid);
            return;
        }
        let dir = tempfile::Builder::new()
            .prefix("seat-git:parser-")
            .tempdir()
            .unwrap();
        let initialized = std::process::Command::new("git")
            .args(["init", "-q"])
            .arg(dir.path())
            .output()
            .unwrap();
        assert!(initialized.status.success(), "{initialized:?}");
        std::fs::write(dir.path().join(".git/config"), "[ambient-broken\n").unwrap();
        let config = dir.path().join("data/agents/synthetic/config");
        std::fs::create_dir_all(&config).unwrap();
        for routing in [
            None,
            Some(("GIT_DIR", dir.path().join(".git").into_os_string())),
            Some(("GIT_CONFIG_PARAMETERS", "invalid".into())),
            Some(("GIT_CONFIG_COUNT", "invalid".into())),
        ] {
            std::fs::write(
                config.join(".gitconfig"),
                "[safe]\n directory = *\n[alias]\n check = 確認\n",
            )
            .unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            // Isolate cwd and environment in the child instead of mutating
            // process-global inputs of concurrently running tests.
            for (key, _) in std::env::vars_os() {
                if key.as_encoded_bytes().starts_with(b"ANYAGENTS_") {
                    child.env_remove(key);
                }
            }
            if let Some((key, value)) = &routing {
                child.env(key, value);
            }
            let output = child
                .current_dir(dir.path())
                .env("ANYAGENTS_TESTS", "1")
                .env(FIXTURE, dir.path())
                .args([
                    "--exact",
                    "repo::tests::seat_git_configuration_ignores_ambient_repository",
                    "--nocapture",
                ])
                .output()
                .unwrap();
            assert!(output.status.success(), "routing={routing:?}: {output:?}");
        }
    }

    #[test]
    fn seat_trust_is_exact_resets_late_includes_and_preserves_other_settings() {
        let dir = tempfile::tempdir().unwrap();
        let id = anyagents_core::AgentId::generate();
        let config = anyagents_core::agent_config_dir(dir.path(), &id);
        std::fs::create_dir_all(&config).unwrap();
        let included = dir.path().join("included.gitconfig");
        std::fs::write(&included, "[safe]\n directory = *\n").unwrap();
        std::fs::write(config.join(".gitconfig"), format!("[safe]\n directory = *\n[alias]\n check = \"確認 status --short\"\n[include]\n path = {}\n", included.display())).unwrap();
        let workdir = dir.path().join("checkout with \"quote\"");
        for root in [&workdir, &dir.path().join("changed-root")] {
            configure_seat_git(&config, root).unwrap();
            let read = |key| {
                let out = std::process::Command::new("git")
                    .args(["config", "--includes", "--file"])
                    .arg(config.join(".gitconfig"))
                    .args(["--get-all", key])
                    .output()
                    .unwrap();
                assert!(
                    out.status.success(),
                    "{}",
                    String::from_utf8_lossy(&out.stderr)
                );
                String::from_utf8(out.stdout).unwrap()
            };
            assert_eq!(read("safe.directory"), format!("*\n\n{}\n", root.display()));
            assert_eq!(read("alias.check"), "確認 status --short\n");
        }
        let before = std::fs::read(config.join(".gitconfig")).unwrap();
        let err = configure_seat_git(&config, &dir.path().join("literal-*")).unwrap_err();
        assert!(err.to_string().contains("wildcard characters"), "{err}");
        assert_eq!(std::fs::read(config.join(".gitconfig")).unwrap(), before);
        let outside = dir.path().join("outside");
        std::fs::write(&outside, "keep").unwrap();
        std::fs::remove_file(config.join(".gitconfig")).unwrap();
        std::os::unix::fs::symlink(&outside, config.join(".gitconfig")).unwrap();
        assert!(configure_seat_git(&config, &workdir).is_err());
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "keep");
        std::fs::remove_file(config.join(".gitconfig")).unwrap();
        std::fs::hard_link(&outside, config.join(".gitconfig")).unwrap();
        let err = configure_seat_git(&config, &workdir).unwrap_err();
        assert!(err.to_string().contains("unsafe or oversized"), "{err}");
        assert_eq!(std::fs::read_to_string(outside).unwrap(), "keep");
    }

    #[test]
    fn sharing_uses_the_requested_supplementary_group() {
        let dir = tempfile::tempdir().unwrap();
        let gid = unsafe {
            let count = libc::getgroups(0, std::ptr::null_mut());
            let mut groups = vec![0; count.max(0) as usize];
            if count > 0 {
                libc::getgroups(count, groups.as_mut_ptr());
            }
            groups
                .into_iter()
                .find(|g| *g != crate::sandbox::server_gid())
                .unwrap_or(crate::sandbox::server_gid())
        };
        std::fs::write(dir.path().join("file"), "data").unwrap();
        share(dir.path(), gid).unwrap();
        assert_eq!(
            std::fs::metadata(dir.path().join("file")).unwrap().gid(),
            gid
        );
    }

    #[test]
    fn sharing_refuses_unavailable_group_instead_of_sock_fallback() {
        if crate::sandbox::server_uid() == 0 {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let err = share(dir.path(), u32::MAX - 1).unwrap_err();
        assert!(matches!(err, Error::WorkdirPermission { .. }));
        assert!(err.to_string().contains("chgrp"), "{err}");
    }

    #[test]
    fn replacement_between_enumeration_and_open_cannot_change_outside_file() {
        let dir = tempfile::tempdir().unwrap();
        let outside = dir.path().join("outside");
        std::fs::write(&outside, "keep").unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o600)).unwrap();
        let tree = dir.path().join("tree");
        std::fs::create_dir(&tree).unwrap();
        let victim = tree.join("victim");
        std::fs::write(&victim, "replace").unwrap();
        let err = share_tree(&tree, crate::sandbox::server_gid(), true, &|path| {
            if path == victim {
                std::fs::remove_file(path).unwrap();
                std::os::unix::fs::symlink(&outside, path).unwrap();
            }
        })
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ELOOP));
        assert_eq!(std::fs::metadata(outside).unwrap().mode() & 0o777, 0o600);
    }

    #[test]
    fn sharing_never_follows_outside_symlinks_or_changes_hardlinks() {
        let dir = tempfile::tempdir().unwrap();
        let outside = dir.path().join("outside");
        std::fs::write(&outside, "keep").unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o600)).unwrap();
        let tree = dir.path().join("tree");
        std::fs::create_dir(&tree).unwrap();
        std::os::unix::fs::symlink(&outside, tree.join("link")).unwrap();
        share(&tree, crate::sandbox::server_gid()).unwrap();
        assert_eq!(std::fs::metadata(&outside).unwrap().mode() & 0o777, 0o600);
        std::fs::hard_link(&outside, tree.join("hardlink")).unwrap();
        let err = share(&tree, crate::sandbox::server_gid()).unwrap_err();
        assert!(err.to_string().contains("hard-linked"), "{err}");
        assert_eq!(std::fs::metadata(&outside).unwrap().mode() & 0o777, 0o600);
    }

    /// The probe as another uid, under `cargo test`: the test harness stands
    /// in for this binary's `internal admission-probe`.
    #[test]
    fn admission_probe_worker() {
        let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
        let Some(start) = args.iter().position(|arg| arg == "--git") else {
            return;
        };
        let code = match admission_probe_main(&args[start..]) {
            code if code == std::process::ExitCode::SUCCESS => 0,
            _ => 1,
        };
        std::process::exit(code);
    }

    /// A real Git binary. The test `PATH` may start with an operator wrapper
    /// (the desk's `git` is a shell script), which cannot run on an empty
    /// `PATH`.
    fn system_git() -> PathBuf {
        ["/usr/bin/git", "/usr/local/bin/git", "/bin/git"]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| path.is_file())
            .unwrap_or_else(|| {
                anyagents_toolchain::tool("git", "test")
                    .unwrap()
                    .path()
                    .to_path_buf()
            })
    }

    /// Admission depends on no host tool but Git: with nothing on `PATH` (a
    /// host image may wrap or remove `find`, `sh` and `test`), the probe
    /// still decides, and its refusals name the object that failed.
    #[test]
    fn the_admission_probe_needs_no_host_tool_but_git() {
        let dir = crate::test_tempdir();
        let tree = dir.path().join("tree");
        std::fs::create_dir_all(tree.join("src/nested")).unwrap();
        std::fs::write(tree.join("src/nested/file"), "x").unwrap();
        std::os::unix::fs::symlink("/nonexistent", tree.join("dangling")).unwrap();
        git(&tree, &["init", "-q", "-b", "main"]);
        let bare: Vec<(&'static str, std::ffi::OsString)> = vec![
            ("PATH", "/nonexistent".into()),
            ("HOME", "/nonexistent".into()),
            ("GIT_CONFIG_GLOBAL", "/dev/null".into()),
            ("GIT_CONFIG_SYSTEM", "/dev/null".into()),
        ];
        admission_probe(&tree, &system_git(), Some(&bare)).unwrap();
        let left: Vec<_> = std::fs::read_dir(&tree)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| n.to_string_lossy().starts_with(".anyagents-write-check"))
            .collect();
        assert!(left.is_empty(), "probe left {left:?}");
        if crate::sandbox::server_uid() == 0 {
            return; // root may write everything
        }
        let file = tree.join("src/nested/file");
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();
        let error = admission_probe(&tree, &system_git(), Some(&bare)).unwrap_err();
        assert!(
            error.contains(file.to_str().unwrap()) && error.contains("cannot write"),
            "{error}"
        );
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        let refs = tree.join(".git/refs");
        std::fs::set_permissions(&refs, std::fs::Permissions::from_mode(0o555)).unwrap();
        let error = admission_probe(&tree, &system_git(), Some(&bare)).unwrap_err();
        std::fs::set_permissions(&refs, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            error.contains(refs.to_str().unwrap()) && error.contains("cannot create a lock"),
            "{error}"
        );
        // A helper process (the path a seat's probe takes) reports through
        // its exit status, with the reason on stderr.
        let (program, args) = probe_program().unwrap();
        let run = |path: &Path| {
            std::process::Command::new(&program)
                .args(&args)
                .arg("--git")
                .arg(system_git())
                .arg("--")
                .arg(path)
                .env_clear()
                .envs(bare.iter().cloned())
                .output()
                .unwrap()
        };
        assert!(run(&tree).status.success());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();
        let refused = run(&tree);
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(refused.status.code(), Some(1));
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains(file.to_str().unwrap()),
            "{}",
            String::from_utf8_lossy(&refused.stderr)
        );
    }

    /// The seat itself executes the server binary. A seat that cannot (the
    /// binary or an ancestor directory is closed to it) is named with the
    /// fix, never reported as a bare permission error.
    #[test]
    fn a_seat_that_cannot_execute_the_probe_is_named() {
        let dir = crate::test_tempdir();
        let launcher = dir.path().join("launcher");
        write_executable(
            &launcher,
            "#!/bin/sh\necho 'setpriv: failed to execute probe: Permission denied' >&2\nexit 126\n",
        );
        let uid = crate::sandbox::server_uid().wrapping_add(1);
        let gid = crate::sandbox::server_gid();
        let spec = format!("{}\t{uid}\t{gid}\t{gid}", launcher.display());
        let error = verify(dir.path(), uid, gid, Some(&spec))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!("seat uid {uid} cannot execute")),
            "{error}"
        );
        assert!(
            error.contains("install AnyAgents where seat users can execute it"),
            "{error}"
        );
        assert!(error.contains("Permission denied"), "{error}");
        assert!(exec_failure(&std::io::Error::from_raw_os_error(
            libc::EACCES
        )));
        assert!(!exec_failure(&std::io::Error::from_raw_os_error(
            libc::EPERM
        )));
    }

    /// The probe's Git is the toolchain's: a declared `git`, else the first on
    /// the helper `PATH`. A `git` that resolves first on the server's own
    /// inherited `PATH` (the desk's guard wrapper sits there) is never used.
    #[test]
    fn the_probe_runs_the_helper_path_git_not_an_inherited_wrapper() {
        let dir = crate::test_tempdir();
        let helper = dir.path().join("helper-bin");
        std::fs::create_dir(&helper).unwrap();
        write_executable(&helper.join("git"), "#!/bin/sh\nexit 0\n");
        let path = std::env::join_paths([&helper]).unwrap();
        assert_eq!(probe_git(&path).unwrap(), helper.join("git"));
        assert!(probe_git(std::ffi::OsStr::new("/nonexistent")).is_err());
    }
}
