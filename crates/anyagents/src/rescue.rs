//! Reclaiming an archived agent's work directory without losing work.
//!
//! Every checkout is the agent's own repository ([`crate::repo`], ADR 0010
//! D5); a linked worktree from before that is converted with
//! [`crate::repo::adopt`] first. Then, one checkout at a time:
//!
//! - one that holds nothing a remote lacks is deleted. It needs a clean
//!   status, and every commit that HEAD, a branch, the stash, a tag, a note or
//!   a reflog reaches must be on a remote-tracking ref, with no tag the pool
//!   lacks. Both this module's check and
//!   [`crate::server::is_git_worktree_safe_to_reap`] must agree;
//! - any other is first saved to the rescue area, as a `git bundle` of all
//!   of that (reflog-only commits under `refs/anyagents-rescue/`) and a tar
//!   of its uncommitted files, and then deleted;
//! - files outside any checkout are saved as one tar, without build output,
//!   tool homes or caches;
//! - a checkout that cannot be converted or read, or whose rescue fails, is
//!   kept where it is, its build output removed, and the operator is told.
//!
//! A clone with an object store of its own is bundled with `--all`, so it
//! restores on its own. A repository that borrows objects from the read-only
//! pool (`objects/info/alternates`) is bundled down to what the remotes
//! already have: its own unpushed commits, which [`crate::repo::adopt`] copied
//! into it. Bundling all of its refs would copy the pool's whole history once
//! per checkout.
//!
//! Git reads run through [`crate::repo::inspect_command`] as the agent's seat,
//! and tar runs as the seat too, so configuration an agent wrote into its own
//! repository never runs with the server's rights.
//!
//! A rescue lands in `{rescue}/{agent id}/{time}/` with a `manifest.json`
//! that says what each file is and how to restore it. Nothing is deleted
//! before the rescue is complete on disk.

use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyagents_core::AgentId;

/// Directory names that only hold build output. They are never rescued.
pub(crate) const BUILD_OUTPUT: &[&str] = &["target", "node_modules", ".next", ".turbo", "dist"];

/// Tool homes and caches agents leave beside their checkouts. Outside a
/// checkout they are never rescued either: they hold downloads, not work.
const CACHES: &[&str] = &[
    ".bun",
    ".bun-cache",
    ".cache",
    ".cargo",
    ".gradle",
    ".npm",
    ".pnpm-store",
    ".rustup",
    ".tmp",
    ".venv",
    "__pycache__",
    "cargo-cache",
    "cargo-home",
    "tmp",
    "venv",
];

fn disposable(name: &std::ffi::OsStr) -> bool {
    BUILD_OUTPUT.iter().chain(CACHES).any(|dir| name == *dir)
}

/// The rescue area keeps at least this much free space.
const MIN_FREE_BYTES: u64 = 2 << 30;

/// Where the rescue keeps the reflog-only commits of a checkout it saves, so
/// its bundle can name them.
const RESCUE_REFS: &str = "refs/anyagents-rescue";

/// Uncommitted paths that never make a checkout worth saving: what AnyAgents
/// and the harnesses write into a checkout, and build output. The same list
/// as [`crate::server::is_git_worktree_safe_to_reap`].
fn incidental(path: &str) -> bool {
    let path = path.trim_end_matches('/');
    matches!(
        path,
        "CLAUDE.md" | "AGENTS.md" | "GEMINI.md" | ".claude" | ".gemini" | "node_modules" | "target"
    ) || path.starts_with(".desk-worktree")
        || path.starts_with(".claude/")
        || path.starts_with(".gemini/")
        || path.starts_with("node_modules/")
        || path.starts_with("target/")
}

/// Runs tar, and plain Git outside any repository.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Runner {
    /// `(uid, gid)` through the privileged launcher, or `None` to run as the
    /// server itself.
    seat: Option<(u32, u32)>,
}

impl Runner {
    /// As `seat`, unless that is this process already.
    fn as_seat(seat: Option<(u32, u32)>) -> Self {
        // SAFETY: geteuid and getegid take no arguments and cannot fail.
        let (euid, egid) = unsafe { (libc::geteuid(), libc::getegid()) };
        Self {
            seat: seat.filter(|&(uid, gid)| euid != uid || egid != gid),
        }
    }

    fn command(&self, program: &Path) -> Command {
        let mut cmd = match self.seat {
            None => Command::new(program),
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
                cmd.arg(program);
                cmd
            }
        };
        cmd.env_clear()
            .current_dir("/")
            .env(
                "PATH",
                anyagents_toolchain::global().exec_path("archive rescue"),
            )
            .env("HOME", "/nonexistent")
            .env("XDG_CONFIG_HOME", "/nonexistent")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("LC_ALL", "C")
            .stdin(Stdio::null());
        cmd
    }

    fn tar(&self, dir: &Path) -> Result<Command, String> {
        let tar = anyagents_toolchain::tool("tar", "archive rescue").map_err(|e| e.to_string())?;
        let mut cmd = self.command(tar.path());
        cmd.arg("-C").arg(dir);
        Ok(cmd)
    }
}

/// Git reading `checkout`, its own `.git` named explicitly.
fn git_in(checkout: &Path, seat: Option<(u32, u32)>) -> Result<Command, String> {
    crate::repo::inspect_command(seat, &checkout.join(".git"), Some(checkout))
        .map_err(|error| error.to_string())
}

fn first_line(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
        .chars()
        .take(300)
        .collect()
}

fn output(mut cmd: Command, args: &[&str]) -> Result<std::process::Output, String> {
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("{}: {error}", args.first().unwrap_or(&"")))
}

fn stdout(cmd: Command, args: &[&str]) -> Result<Vec<u8>, String> {
    let out = output(cmd, args)?;
    if !out.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.first().unwrap_or(&""),
            first_line(&out.stderr)
        ));
    }
    Ok(out.stdout)
}

/// One `git status --porcelain -z` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Change {
    pub(crate) code: [u8; 2],
    pub(crate) path: Vec<u8>,
}

impl Change {
    fn path_text(&self) -> String {
        String::from_utf8_lossy(&self.path).into_owned()
    }

    /// The file is still in the working tree, so it can be archived.
    fn present(&self) -> bool {
        self.code[1] != b'D' && !(self.code[0] == b'D' && self.code[1] == b' ')
    }
}

fn parse_status(raw: &[u8]) -> Vec<Change> {
    let mut changes = Vec::new();
    let mut fields = raw.split(|byte| *byte == 0).filter(|f| !f.is_empty());
    while let Some(field) = fields.next() {
        if field.len() < 4 {
            continue;
        }
        let code = [field[0], field[1]];
        changes.push(Change {
            code,
            path: field[3..].to_vec(),
        });
        // A rename or copy is followed by its source path.
        if matches!(code[0], b'R' | b'C') {
            fields.next();
        }
    }
    changes
}

/// What a checkout holds that its remotes do not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RepoState {
    /// The checked-out commit (`None` for a repository with no commits).
    pub(crate) head: Option<String>,
    /// The checked-out branch, `refs/heads/...`, if any.
    pub(crate) branch: Option<String>,
    /// `remote.origin.url`, to restore a checkout from.
    pub(crate) origin: Option<String>,
    /// It borrows objects from a pool (`objects/info/alternates`).
    pub(crate) borrows: bool,
    /// Commits of HEAD and the local branches that no remote-tracking ref has.
    pub(crate) unpushed: u64,
    /// Further commits no remote-tracking ref has that only the stash, notes,
    /// tags or the reflogs reach.
    pub(crate) hidden: u64,
    /// `refs/stash` exists.
    pub(crate) stash: bool,
    /// Tags the pool does not have (every tag of a clone with its own objects).
    pub(crate) local_tags: Vec<String>,
    /// Reflog entries no ref reaches: dropped stashes, amended or reset commits.
    pub(crate) reflog_only: Vec<String>,
    pub(crate) changes: Vec<Change>,
    status: Vec<u8>,
    refs: Vec<u8>,
}

impl RepoState {
    /// Deleting this checkout as it is would lose something no remote has.
    pub(crate) fn owes_rescue(&self) -> bool {
        self.unpushed > 0
            || self.hidden > 0
            || self.stash
            || !self.local_tags.is_empty()
            || !self.reflog_only.is_empty()
            || self.changes.iter().any(|c| !incidental(&c.path_text()))
    }
}

fn count(out: Vec<u8>) -> Result<u64, String> {
    String::from_utf8_lossy(&out)
        .trim()
        .parse::<u64>()
        .map_err(|_| "git rev-list printed no count".to_string())
}

fn lines(out: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(out)
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect()
}

/// `(object, refname)` of every tag in the Git directory `git_dir`.
fn tags(git_dir: &Path, seat: Option<(u32, u32)>) -> Result<Vec<(String, String)>, String> {
    let cmd = crate::repo::inspect_command(seat, git_dir, None).map_err(|e| e.to_string())?;
    let out = stdout(
        cmd,
        &[
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            "refs/tags",
        ],
    )?;
    Ok(lines(&out)
        .into_iter()
        .filter_map(|line| {
            line.split_once(' ')
                .map(|(oid, name)| (oid.to_string(), name.to_string()))
        })
        .collect())
}

/// The pool a repository borrows objects from: the Git directory around the
/// first `objects/info/alternates` entry.
fn pool_of(checkout: &Path) -> Option<PathBuf> {
    let text = std::fs::read_to_string(checkout.join(".git/objects/info/alternates")).ok()?;
    let objects = text.lines().map(str::trim).find(|l| !l.is_empty())?;
    let objects = Path::new(objects);
    let objects = if objects.is_absolute() {
        objects.to_path_buf()
    } else {
        checkout.join(".git/objects").join(objects)
    };
    objects.parent().map(Path::to_path_buf)
}

/// Read an own repository's state: everything in it no remote-tracking ref
/// reaches, and its uncommitted files.
pub(crate) fn inspect(checkout: &Path, seat: Option<(u32, u32)>) -> Result<RepoState, String> {
    if !std::fs::symlink_metadata(checkout.join(".git")).is_ok_and(|meta| meta.is_dir()) {
        return Err("it is not a repository of its own".into());
    }
    let git = || git_in(checkout, seat);
    let status = stdout(
        git()?,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=normal",
            "--ignore-submodules=none",
        ],
    )?;
    let head = output(git()?, &["rev-parse", "--verify", "-q", "HEAD^{commit}"])?;
    let head = head
        .status
        .success()
        .then(|| String::from_utf8_lossy(&head.stdout).trim().to_string())
        .filter(|sha| !sha.is_empty());
    let stash = output(git()?, &["rev-parse", "--verify", "-q", "refs/stash"])?
        .status
        .success();
    let unpushed = if head.is_some() {
        count(stdout(
            git()?,
            &[
                "rev-list",
                "--count",
                "HEAD",
                "--branches",
                "--not",
                "--remotes",
            ],
        )?)?
    } else {
        0
    };
    // What only the stash, notes, tags or the reflogs reach.
    let mut args = vec!["rev-list", "--count"];
    if stash {
        args.push("refs/stash");
    }
    args.extend([
        "--tags",
        "--glob=refs/notes/*",
        "--reflog",
        "--not",
        "--remotes",
        "--branches",
    ]);
    if head.is_some() {
        args.push("HEAD");
    }
    let hidden = count(stdout(git()?, &args)?)?;
    // `--stdin` (empty) lets a repository with no reflog at all list nothing
    // instead of failing for want of a revision.
    let tips = lines(&stdout(
        git()?,
        &["rev-list", "--no-walk", "--reflog", "--stdin"],
    )?);
    let unreached: std::collections::BTreeSet<String> = lines(&stdout(
        git()?,
        &["rev-list", "--reflog", "--not", "--all"],
    )?)
    .into_iter()
    .collect();
    let mut reflog_only: Vec<String> = tips
        .into_iter()
        .filter(|tip| unreached.contains(tip))
        .collect();
    reflog_only.sort();
    reflog_only.dedup();
    let own = tags(&checkout.join(".git"), seat)?;
    let local_tags = match pool_of(checkout) {
        Some(pool) => {
            let pooled: std::collections::BTreeSet<(String, String)> =
                tags(&pool, seat).unwrap_or_default().into_iter().collect();
            own.into_iter()
                .filter(|tag| !pooled.contains(tag))
                .map(|(_, name)| name)
                .collect()
        }
        // A clone with its own objects: no record of which tags came from
        // its remote, so every tag is kept.
        None => own.into_iter().map(|(_, name)| name).collect(),
    };
    let text = |args: &[&str]| -> Result<Option<String>, String> {
        Ok(output(git()?, args)
            .ok()
            .filter(|out| out.status.success())
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .filter(|value| !value.is_empty()))
    };
    let branch =
        text(&["symbolic-ref", "-q", "HEAD"])?.filter(|name| name.starts_with("refs/heads/"));
    let origin = text(&["config", "--get", "remote.origin.url"])?;
    let refs = stdout(
        git()?,
        &[
            "for-each-ref",
            "--format=%(objectname) %(objecttype) %(refname)",
        ],
    )?;
    Ok(RepoState {
        head,
        branch,
        origin,
        borrows: checkout
            .join(".git/objects/info/alternates")
            .symlink_metadata()
            .is_ok(),
        unpushed,
        hidden,
        stash,
        local_tags,
        reflog_only,
        changes: parse_status(&status),
        status,
        refs,
    })
}

/// What was saved for one checkout.
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SavedRepo {
    /// The checkout, relative to the work directory (`.` for the work directory itself).
    pub(crate) path: String,
    pub(crate) head: Option<String>,
    pub(crate) branch: Option<String>,
    pub(crate) origin: Option<String>,
    /// It borrowed from a pool: its bundle holds only commits no remote has.
    pub(crate) borrowed_objects: bool,
    pub(crate) unpushed_commits: u64,
    /// Commits only the stash, notes, tags or reflogs reach.
    pub(crate) hidden_commits: u64,
    pub(crate) stash: bool,
    pub(crate) local_tags: Vec<String>,
    /// Reflog-only commits, kept in the bundle as `refs/anyagents-rescue/reflog-<n>`.
    pub(crate) reflog_only: Vec<String>,
    /// Every ref it had, `<object> <type> <name>` per line.
    pub(crate) refs: String,
    pub(crate) uncommitted_paths: usize,
    pub(crate) bundle: Option<String>,
    pub(crate) bundle_refs: Vec<String>,
    pub(crate) uncommitted_tar: Option<String>,
    pub(crate) status: String,
    pub(crate) bytes: u64,
}

type OsStrLike = std::ffi::OsStr;

fn os(text: &str) -> &OsStrLike {
    OsStrLike::new(text)
}

fn write_to(
    path: &Path,
    mut cmd: Command,
    args: &[&OsStrLike],
    input: Option<&[u8]>,
) -> Result<u64, String> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("cannot create {}: {error}", path.display()))?;
    cmd.args(args)
        .stdout(Stdio::from(file.try_clone().map_err(|e| e.to_string())?))
        .stderr(Stdio::piped());
    cmd.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    let mut child = cmd.spawn().map_err(|error| error.to_string())?;
    if let Some(input) = input {
        let mut stdin = child.stdin.take().ok_or("no stdin")?;
        stdin.write_all(input).map_err(|error| error.to_string())?;
    }
    let out = child
        .wait_with_output()
        .map_err(|error| error.to_string())?;
    if !out.status.success() {
        return Err(first_line(&out.stderr));
    }
    file.sync_all().map_err(|error| error.to_string())?;
    Ok(file.metadata().map_err(|e| e.to_string())?.len())
}

/// Save one checkout into `out` under the file name prefix `slug`.
fn save_repo(
    checkout: &Path,
    seat: Option<(u32, u32)>,
    state: &RepoState,
    rel: &str,
    out: &Path,
    slug: &str,
) -> Result<SavedRepo, String> {
    let mut saved = SavedRepo {
        path: rel.to_string(),
        head: state.head.clone(),
        branch: state.branch.clone(),
        origin: state.origin.clone(),
        borrowed_objects: state.borrows,
        unpushed_commits: state.unpushed,
        hidden_commits: state.hidden,
        stash: state.stash,
        local_tags: state.local_tags.clone(),
        reflog_only: state.reflog_only.clone(),
        refs: format!("{slug}.refs"),
        uncommitted_paths: state.changes.len(),
        bundle: None,
        bundle_refs: Vec::new(),
        uncommitted_tar: None,
        status: format!("{slug}.status"),
        bytes: 0,
    };
    let status_text: Vec<u8> = state
        .status
        .iter()
        .map(|byte| if *byte == 0 { b'\n' } else { *byte })
        .collect();
    std::fs::write(out.join(&saved.status), &status_text).map_err(|error| error.to_string())?;
    std::fs::write(out.join(&saved.refs), &state.refs).map_err(|error| error.to_string())?;
    // Commits only a reflog reaches get a ref, so the bundle can carry them.
    for (n, tip) in state.reflog_only.iter().enumerate() {
        let name = format!("{RESCUE_REFS}/reflog-{n}");
        stdout(git_in(checkout, seat)?, &["update-ref", &name, tip])?;
    }
    // A clone with its own objects carries every ref, so it restores on its
    // own. A repository that borrows from the pool carries what the remotes
    // do not have; the pool and the remote hold the rest.
    let mut refs: Vec<String> = Vec::new();
    if !state.borrows {
        refs.push("--all".into());
    } else if state.unpushed > 0
        || state.hidden > 0
        || state.stash
        || !state.local_tags.is_empty()
        || !state.reflog_only.is_empty()
    {
        refs.push("--branches".into());
        if state.head.is_some() {
            refs.push("HEAD".into());
        }
        if state.stash {
            refs.push("refs/stash".into());
        }
        refs.extend(state.local_tags.iter().cloned());
        refs.push("--glob=refs/notes/*".into());
        refs.push(format!("--glob={RESCUE_REFS}/*"));
        refs.extend(["--not".into(), "--remotes".into()]);
    }
    let has_objects = state.head.is_some() || state.stash || !state.local_tags.is_empty();
    if !refs.is_empty() && has_objects {
        let file = out.join(format!("{slug}.bundle"));
        let mut args: Vec<&OsStrLike> = vec![os("bundle"), os("create"), os("-")];
        args.extend(refs.iter().map(|r| os(r)));
        match write_to(&file, git_in(checkout, seat)?, &args, None) {
            Ok(bytes) => {
                saved.bytes += bytes;
                // The bundle must name its refs when read back on its own.
                let listed = bundle_heads(&file)?;
                if listed.is_empty() {
                    return Err("git bundle wrote no refs".into());
                }
                saved.bundle = Some(format!("{slug}.bundle"));
                saved.bundle_refs = listed;
            }
            // Only refs whose commits a remote already has (a lightweight
            // tag on a pushed commit): `{slug}.refs` names them.
            Err(error) if error.contains("empty bundle") => {
                let _ = std::fs::remove_file(&file);
            }
            Err(error) => return Err(format!("git bundle failed: {error}")),
        }
    }
    let mut names = Vec::new();
    for change in state.changes.iter().filter(|c| c.present()) {
        names.extend_from_slice(&change.path);
        names.push(0);
    }
    if !names.is_empty() {
        let file = out.join(format!("{slug}.uncommitted.tar"));
        let mut args: Vec<String> = BUILD_OUTPUT
            .iter()
            .map(|dir| format!("--exclude={dir}"))
            .collect();
        args.extend(["--null", "-T", "-", "-cf", "-"].map(String::from));
        let args: Vec<&OsStrLike> = args.iter().map(|a| os(a)).collect();
        saved.bytes += write_to(
            &file,
            Runner::as_seat(seat).tar(checkout)?,
            &args,
            Some(&names),
        )
        .map_err(|error| format!("tar of uncommitted files failed: {error}"))?;
        saved.uncommitted_tar = Some(format!("{slug}.uncommitted.tar"));
    }
    Ok(saved)
}

/// The refs a bundle file lists, read with no repository and no user config.
fn bundle_heads(file: &Path) -> Result<Vec<String>, String> {
    let git = anyagents_toolchain::tool("git", "archive rescue").map_err(|e| e.to_string())?;
    let mut cmd = Runner::as_seat(None).command(git.path());
    cmd.arg("bundle").arg("list-heads").arg(file);
    let out = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| error.to_string())?;
    if !out.status.success() {
        return Err(format!(
            "git bundle list-heads failed: {}",
            first_line(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| line.split_once(' ').map(|(_, name)| name.to_string()))
        .collect())
}

/// A file name for `rel` (a path inside the work directory).
fn slug(rel: &str, taken: &mut std::collections::BTreeSet<String>) -> String {
    let base: String = if rel == "." {
        "work".to_string()
    } else {
        rel.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    let base = base.trim_start_matches('.').to_string();
    let base = if base.is_empty() { "repo".into() } else { base };
    let mut name = base.clone();
    let mut n = 1;
    while !taken.insert(name.clone()) {
        n += 1;
        name = format!("{base}-{n}");
    }
    name
}

/// What reclaiming one work directory did.
#[derive(Debug, Default)]
pub(crate) struct Outcome {
    /// Every removal it attempted succeeded.
    pub(crate) complete: bool,
    /// Checkouts or files left in place, and why.
    pub(crate) kept: Vec<(PathBuf, String)>,
    /// The rescue directory written, if any.
    pub(crate) rescued: Option<PathBuf>,
    /// Checkouts deleted with nothing to save.
    pub(crate) clean: usize,
}

/// The checkouts directly in `work` (or `work` itself), and its other
/// top-level entries that are not build output, tool homes or caches.
fn survey(work: &Path) -> std::io::Result<(Vec<PathBuf>, Vec<std::ffi::OsString>)> {
    if work.join(".git").symlink_metadata().is_ok() {
        return Ok((vec![work.to_path_buf()], Vec::new()));
    }
    let mut repos = Vec::new();
    let mut loose = Vec::new();
    for entry in std::fs::read_dir(work)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let is_dir = entry.file_type()?.is_dir();
        if is_dir && path.join(".git").symlink_metadata().is_ok() {
            repos.push(path);
        } else if is_dir && disposable(&name) {
            continue;
        } else {
            loose.push(name);
        }
    }
    repos.sort();
    loose.sort();
    Ok((repos, loose))
}

/// The rescue area has room for another rescue.
fn rescue_has_room(dir: &Path) -> Result<(), String> {
    let probe = dir
        .ancestors()
        .find(|p| p.exists())
        .unwrap_or(Path::new("/"));
    let c_path =
        std::ffi::CString::new(probe.as_os_str().as_encoded_bytes()).map_err(|e| e.to_string())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: c_path is NUL-terminated and stat is written by the call.
    if unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(format!(
            "cannot read free space at {}: {}",
            probe.display(),
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: statvfs returned 0, so it filled the struct.
    let stat = unsafe { stat.assume_init() };
    #[allow(clippy::unnecessary_cast)]
    let (free, size) = (
        stat.f_bavail as u64 * stat.f_frsize as u64,
        stat.f_blocks as u64 * stat.f_frsize as u64,
    );
    if free < MIN_FREE_BYTES.max(size / 20) {
        return Err(format!(
            "the rescue area {} has only {} MiB free",
            probe.display(),
            free >> 20
        ));
    }
    Ok(())
}

/// Reclaims one archived agent's managed work directory.
pub(crate) struct Reclaim<'a> {
    pub(crate) data_dir: &'a Path,
    pub(crate) agent_id: &'a AgentId,
    pub(crate) agent_name: Option<&'a str>,
    pub(crate) generation: &'a [u8],
    /// `(uid, gid, socket gid)` of the agent's seat, for removals.
    pub(crate) seat_ids: Option<(u32, u32, u32)>,
}

enum Plan {
    Delete(PathBuf),
    Save(PathBuf, Box<RepoState>),
}

impl Reclaim<'_> {
    fn seat(&self) -> Option<(u32, u32)> {
        self.seat_ids.map(|(uid, gid, _)| (uid, gid))
    }

    /// What to do with one checkout: convert a linked worktree, then delete
    /// it when it is safe to, else save it. `Err` keeps it.
    fn plan(&self, checkout: PathBuf) -> Result<Plan, (PathBuf, String)> {
        let seat = self.seat();
        if std::fs::symlink_metadata(checkout.join(".git")).is_ok_and(|meta| meta.is_file())
            && let crate::repo::Adoption::Declined(reason) = crate::repo::adopt(&checkout, seat)
        {
            return Err((
                checkout,
                format!("not converted to its own repository: {reason}"),
            ));
        }
        // Deleted as it is only when the clean check and this one both find
        // nothing a remote lacks; anything either sees is saved first.
        match inspect(&checkout, seat) {
            Ok(state)
                if !state.owes_rescue()
                    && crate::server::is_git_worktree_safe_to_reap(&checkout, seat) =>
            {
                Ok(Plan::Delete(checkout))
            }
            Ok(state) => Ok(Plan::Save(checkout, Box::new(state))),
            Err(reason) => Err((checkout, reason)),
        }
    }

    /// Reclaim `work`: one checkout, or a directory of checkouts and files.
    pub(crate) fn work_dir(&self, work: &Path) -> Outcome {
        let mut outcome = Outcome {
            complete: true,
            ..Outcome::default()
        };
        let (repos, loose) = match survey(work) {
            Ok(found) => found,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return outcome,
            Err(error) => {
                outcome
                    .kept
                    .push((work.to_path_buf(), format!("cannot list it: {error}")));
                return outcome;
            }
        };
        let mut plans = Vec::new();
        for checkout in repos {
            match self.plan(checkout) {
                Ok(plan) => plans.push(plan),
                Err((checkout, reason)) => {
                    outcome.complete &=
                        crate::server::reap_build_outputs_in_worktree(&checkout, self.seat_ids);
                    outcome.kept.push((checkout, reason));
                }
            }
        }
        let owes = plans.iter().any(|p| matches!(p, Plan::Save(..))) || !loose.is_empty();
        let mut loose_saved = loose.is_empty();
        if owes {
            match self.save(work, &plans, &loose) {
                Ok(dir) => {
                    outcome.rescued = Some(dir);
                    loose_saved = true;
                }
                Err(reason) => {
                    // Keep everything that needed saving; still delete what
                    // was clean.
                    plans.retain(|plan| match plan {
                        Plan::Save(checkout, _) => {
                            outcome.complete &= crate::server::reap_build_outputs_in_worktree(
                                checkout,
                                self.seat_ids,
                            );
                            outcome
                                .kept
                                .push((checkout.clone(), format!("rescue failed: {reason}")));
                            false
                        }
                        Plan::Delete(_) => true,
                    });
                    if !loose.is_empty() {
                        outcome.kept.push((
                            work.to_path_buf(),
                            format!("files outside a checkout; rescue failed: {reason}"),
                        ));
                    }
                }
            }
        }
        for plan in &plans {
            let (Plan::Delete(checkout) | Plan::Save(checkout, _)) = plan;
            if matches!(plan, Plan::Delete(_)) {
                outcome.clean += 1;
            }
            // The store's keep ref for a converted repository goes with it;
            // a rescued one's bundle carries everything the remotes lack.
            crate::repo::forget(checkout);
            outcome.complete &= crate::server::remove_path_with_seat(checkout, self.seat_ids);
        }
        if outcome.kept.is_empty() && loose_saved {
            outcome.complete &= crate::server::remove_path_with_seat(work, self.seat_ids);
        } else if work.join(".git").symlink_metadata().is_err() {
            for dir in BUILD_OUTPUT {
                outcome.complete &=
                    crate::server::remove_path_with_seat(&work.join(dir), self.seat_ids);
            }
        }
        outcome
    }

    /// Write every owed checkout and the loose files into a new rescue
    /// directory, and return it once it is complete on disk.
    fn save(
        &self,
        work: &Path,
        plans: &[Plan],
        loose: &[std::ffi::OsString],
    ) -> Result<PathBuf, String> {
        let base = anyagents_core::rescue_dir(self.data_dir).join(self.agent_id.as_str());
        std::fs::create_dir_all(&base).map_err(|error| error.to_string())?;
        if let Some(top) = base.parent() {
            let _ = std::fs::set_permissions(top, std::fs::Permissions::from_mode(0o700));
        }
        rescue_has_room(&base)?;
        let stamp = anyagents_core::now_ms();
        let partial = base.join(format!(".partial-{stamp}"));
        let done = base.join(stamp.to_string());
        std::fs::create_dir(&partial).map_err(|error| error.to_string())?;
        let result = self
            .save_into(work, plans, loose, &partial)
            .and_then(|manifest| {
                let bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
                anyagents_harness::atomic_write_nonempty_file(
                    &partial.join("manifest.json"),
                    &bytes,
                    0o600,
                )
                .map_err(|error| error.to_string())?;
                std::fs::rename(&partial, &done).map_err(|error| error.to_string())?;
                if let Ok(dir) = std::fs::File::open(&base) {
                    let _ = dir.sync_all();
                }
                Ok(done.clone())
            });
        if result.is_err() {
            let _ = std::fs::remove_dir_all(&partial);
        }
        result
    }

    fn save_into(
        &self,
        work: &Path,
        plans: &[Plan],
        loose: &[std::ffi::OsString],
        out: &Path,
    ) -> Result<serde_json::Value, String> {
        let mut taken = std::collections::BTreeSet::new();
        let mut repos = Vec::new();
        for plan in plans {
            let Plan::Save(checkout, state) = plan else {
                continue;
            };
            let rel = checkout
                .strip_prefix(work)
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
                .filter(|p| !p.is_empty())
                .unwrap_or_else(|| ".".to_string());
            let name = slug(&rel, &mut taken);
            let saved = save_repo(checkout, self.seat(), state, &rel, out, &name)
                .map_err(|error| format!("{rel}: {error}"))?;
            repos.push(saved);
        }
        let mut loose_tar = None;
        if !loose.is_empty() {
            let mut names = Vec::new();
            for name in loose {
                names.extend_from_slice(name.as_encoded_bytes());
                names.push(0);
            }
            let mut args: Vec<String> = BUILD_OUTPUT
                .iter()
                .chain(CACHES)
                .map(|dir| format!("--exclude={dir}"))
                .collect();
            args.extend(["--null", "-T", "-", "-cf", "-"].map(String::from));
            let args: Vec<&OsStrLike> = args.iter().map(|a| os(a)).collect();
            let file = out.join("loose.tar");
            write_to(
                &file,
                Runner::as_seat(self.seat()).tar(work)?,
                &args,
                Some(&names),
            )
            .map_err(|error| format!("tar of files outside a checkout failed: {error}"))?;
            loose_tar = Some("loose.tar");
        }
        Ok(serde_json::json!({
            "agent_id": self.agent_id.as_str(),
            "agent_name": self.agent_name,
            "archive_generation": String::from_utf8_lossy(self.generation).trim(),
            "work_dir": work,
            "rescued_at_ms": anyagents_core::now_ms(),
            "checkouts": repos,
            "loose_tar": loose_tar,
            "restore": "A clone with its own objects: git clone <name>.bundle <dir>. One that borrowed from a pool: clone its origin, git fetch <name>.bundle if there is one, and check out its head. Then tar -xf <name>.uncommitted.tar -C <dir>. loose.tar holds the files that sat outside any checkout.",
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args([
                "-c",
                "user.name=Example Author",
                "-c",
                "user.email=author@example.org",
                "-c",
                "init.defaultBranch=main",
                "-C",
            ])
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// A bare origin and a clone of it with one pushed commit.
    fn published(root: &Path, name: &str) -> PathBuf {
        let origin = root.join(format!("{name}.git"));
        std::fs::create_dir_all(&origin).unwrap();
        git(&origin, &["init", "-q", "--bare"]);
        let clone = root.join("work").join(name);
        std::fs::create_dir_all(&clone).unwrap();
        git(&clone, &["init", "-q"]);
        git(
            &clone,
            &["remote", "add", "origin", origin.to_str().unwrap()],
        );
        std::fs::write(clone.join("README.md"), b"published\n").unwrap();
        std::fs::write(clone.join(".gitignore"), b"/target/\n").unwrap();
        git(&clone, &["add", "README.md", ".gitignore"]);
        git(&clone, &["commit", "-qm", "Published"]);
        git(&clone, &["push", "-q", "origin", "HEAD:refs/heads/main"]);
        git(&clone, &["fetch", "-q", "origin"]);
        clone
    }

    fn reclaim(data: &Path, id: &AgentId) -> Reclaim<'static> {
        Reclaim {
            data_dir: Box::leak(data.to_path_buf().into_boxed_path()),
            agent_id: Box::leak(Box::new(id.clone())),
            agent_name: Some("worker"),
            generation: b"1:gen\n",
            seat_ids: None,
        }
    }

    fn manifest(rescued: &Path) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(rescued.join("manifest.json")).unwrap()).unwrap()
    }

    fn listing(tar: &Path) -> String {
        let out = Command::new("tar").arg("-tf").arg(tar).output().unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    #[test]
    fn status_parsing_skips_rename_sources_and_spots_deletions() {
        let raw = b"R  new.rs\0old.rs\0 M src/lib.rs\0?? notes.md\0 D gone.rs\0D  staged-gone.rs\0";
        let changes = parse_status(raw);
        let paths: Vec<_> = changes.iter().map(Change::path_text).collect();
        assert_eq!(
            paths,
            [
                "new.rs",
                "src/lib.rs",
                "notes.md",
                "gone.rs",
                "staged-gone.rs"
            ]
        );
        let present: Vec<_> = changes
            .iter()
            .filter(|c| c.present())
            .map(Change::path_text)
            .collect();
        assert_eq!(present, ["new.rs", "src/lib.rs", "notes.md"]);
    }

    #[test]
    fn clean_checkouts_are_deleted_and_owed_ones_are_saved_then_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let id = AgentId::generate();
        let clean = published(dir.path(), "clean");
        std::fs::create_dir_all(clean.join("target/debug")).unwrap();
        std::fs::write(clean.join("CLAUDE.md"), b"instructions\n").unwrap();
        let ahead = published(dir.path(), "ahead");
        std::fs::write(ahead.join("feature.rs"), b"fn unpushed() {}\n").unwrap();
        git(&ahead, &["add", "feature.rs"]);
        git(&ahead, &["commit", "-qm", "Unpushed feature"]);
        let unpushed_head = git(&ahead, &["rev-parse", "HEAD"]);
        let dirty = published(dir.path(), "dirty");
        std::fs::write(dirty.join("README.md"), b"edited\n").unwrap();
        std::fs::create_dir_all(dirty.join("drafts/target")).unwrap();
        std::fs::write(dirty.join("drafts/plan.md"), b"untracked draft\n").unwrap();
        std::fs::write(dirty.join("drafts/target/out"), b"built").unwrap();
        std::fs::create_dir_all(dirty.join("target/debug")).unwrap();
        std::fs::write(dirty.join("target/debug/huge"), b"build output").unwrap();
        let work = dir.path().join("work");
        std::fs::write(work.join("report.md"), b"loose report\n").unwrap();
        std::fs::create_dir_all(work.join("node_modules/pkg")).unwrap();
        std::fs::create_dir_all(work.join(".bun-cache/pkg")).unwrap();
        std::fs::write(work.join(".bun-cache/pkg/blob"), b"downloaded").unwrap();
        std::fs::create_dir_all(work.join("notes/target")).unwrap();
        std::fs::write(work.join("notes/plan.md"), b"plan\n").unwrap();
        std::fs::write(work.join("notes/target/out"), b"built").unwrap();

        let outcome = reclaim(&data, &id).work_dir(&work);
        assert!(outcome.complete, "{outcome:?}");
        assert!(outcome.kept.is_empty(), "{:?}", outcome.kept);
        assert_eq!(outcome.clean, 1);
        assert!(!work.exists(), "the reclaimed work directory remains");
        let rescued = outcome.rescued.expect("owed work was rescued");
        assert!(rescued.starts_with(data.join("rescue").join(id.as_str())));
        let manifest = manifest(&rescued);
        assert_eq!(
            manifest["checkouts"].as_array().unwrap().len(),
            2,
            "{manifest:#}"
        );
        assert_eq!(manifest["loose_tar"], "loose.tar");

        // The unpushed commit comes back from its bundle.
        let restored = dir.path().join("restored-ahead");
        let out = Command::new("git")
            .args(["clone", "-q"])
            .arg(rescued.join("ahead.bundle"))
            .arg(&restored)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(git(&restored, &["rev-parse", "HEAD"]), unpushed_head);
        assert!(restored.join("feature.rs").exists());

        // The uncommitted edits come back from the tar, without build output.
        let saved = listing(&rescued.join("dirty.uncommitted.tar"));
        assert!(
            saved.contains("README.md") && saved.contains("drafts/plan.md"),
            "{saved}"
        );
        assert!(!saved.contains("target"), "{saved}");
        let loose = listing(&rescued.join("loose.tar"));
        assert!(
            loose.contains("report.md") && loose.contains("notes/plan.md"),
            "{loose}"
        );
        assert!(!loose.contains("node_modules"), "{loose}");
        assert!(!loose.contains(".bun-cache"), "{loose}");
        assert!(!loose.contains("notes/target"), "{loose}");
        assert!(!loose.contains("clean"), "{loose}");
    }

    #[test]
    fn a_repository_borrowing_from_a_pool_bundles_only_what_no_remote_has() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let id = AgentId::generate();
        // The pool: a clone that already holds the published history.
        let pool = published(&dir.path().join("pool"), "repo");
        let origin = dir.path().join("pool/repo.git");
        let work = dir.path().join("work");
        let make = |name: &str| {
            let checkout = work.join(name);
            let out = Command::new("git")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .args(["clone", "-q", "--reference"])
                .arg(&pool)
                .arg(&origin)
                .arg(&checkout)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            assert!(checkout.join(".git/objects/info/alternates").is_file());
            checkout
        };
        let ahead = make("ahead");
        std::fs::write(ahead.join("feature.rs"), b"fn feature() {}\n").unwrap();
        git(&ahead, &["add", "feature.rs"]);
        git(&ahead, &["commit", "-qm", "Unpushed feature"]);
        let unpushed_head = git(&ahead, &["rev-parse", "HEAD"]);
        let dirty = make("dirty");
        std::fs::write(dirty.join("README.md"), b"edited\n").unwrap();
        let pushed_head = git(&dirty, &["rev-parse", "HEAD"]);

        let outcome = reclaim(&data, &id).work_dir(&work);
        assert!(outcome.complete && outcome.kept.is_empty(), "{outcome:?}");
        assert!(!work.exists());
        let rescued = outcome.rescued.unwrap();
        let manifest = manifest(&rescued);
        let entry = |path: &str| {
            manifest["checkouts"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["path"] == path)
                .unwrap()
                .clone()
        };
        // The unpushed commit is in a bundle that needs the remote's history.
        let ahead_entry = entry("ahead");
        assert_eq!(ahead_entry["borrowed_objects"], true);
        assert_eq!(ahead_entry["head"], unpushed_head.as_str());
        let verify = Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .args(["-C", pool.to_str().unwrap(), "bundle", "verify", "-q"])
            .arg(rescued.join("ahead.bundle"))
            .output()
            .unwrap();
        assert!(
            verify.status.success(),
            "{}",
            String::from_utf8_lossy(&verify.stderr)
        );
        let heads = bundle_heads(&rescued.join("ahead.bundle")).unwrap();
        assert!(heads.contains(&"refs/heads/main".to_string()), "{heads:?}");
        // A pushed HEAD needs no bundle: its commit and the tar restore it.
        let dirty_entry = entry("dirty");
        assert_eq!(dirty_entry["head"], pushed_head.as_str());
        assert!(dirty_entry["bundle"].is_null());
        assert!(rescued.join("dirty.uncommitted.tar").is_file());
        assert!(
            dirty_entry["origin"]
                .as_str()
                .unwrap()
                .ends_with("repo.git")
        );
    }

    #[test]
    fn a_linked_worktree_is_converted_before_it_is_judged() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let id = AgentId::generate();
        let shared = published(&dir.path().join("shared"), "repo");
        // A store adoption may borrow from never prunes (#278).
        git(&shared, &["config", "gc.pruneExpire", "never"]);
        git(&shared, &["config", "gc.cruftPacks", "true"]);
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let ahead = work.join("ahead");
        git(
            &shared,
            &[
                "worktree",
                "add",
                "-q",
                ahead.to_str().unwrap(),
                "-b",
                "feature",
            ],
        );
        std::fs::write(ahead.join("feature.rs"), b"fn feature() {}\n").unwrap();
        git(&ahead, &["add", "feature.rs"]);
        git(&ahead, &["commit", "-qm", "Unpushed feature"]);
        let unpushed_head = git(&ahead, &["rev-parse", "HEAD"]);

        let outcome = reclaim(&data, &id).work_dir(&work);
        assert!(outcome.complete && outcome.kept.is_empty(), "{outcome:?}");
        assert!(!work.exists());
        let rescued = outcome.rescued.unwrap();
        let heads = bundle_heads(&rescued.join("ahead.bundle")).unwrap();
        assert!(
            heads.contains(&"refs/heads/feature".to_string()),
            "{heads:?}"
        );
        let restored = dir.path().join("restored");
        git(
            &shared,
            &[
                "clone",
                "-q",
                "--no-checkout",
                ".",
                restored.to_str().unwrap(),
            ],
        );
        git(
            &restored,
            &[
                "fetch",
                "-q",
                rescued.join("ahead.bundle").to_str().unwrap(),
                "refs/heads/feature",
            ],
        );
        assert_eq!(git(&restored, &["rev-parse", "FETCH_HEAD"]), unpushed_head);
        // The shared store forgot the converted checkout.
        assert!(!git(&shared, &["worktree", "list"]).contains("ahead"));
    }

    #[test]
    fn stash_tags_notes_and_reflog_only_commits_are_rescued() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let id = AgentId::generate();
        let work = published(dir.path(), "repo");
        let pushed = git(&work, &["rev-parse", "HEAD"]);
        // Each of these alone keeps a pushed, clean checkout from being
        // deleted as it is.
        git(&work, &["tag", "light"]);
        git(&work, &["tag", "-a", "release", "-m", "a local release"]);
        git(&work, &["notes", "add", "-m", "a note", "HEAD"]);
        git(
            &work,
            &["commit", "-q", "--allow-empty", "-m", "Reset away"],
        );
        let reset_away = git(&work, &["rev-parse", "HEAD"]);
        git(&work, &["reset", "-q", "--hard", &pushed]);
        std::fs::write(work.join("README.md"), b"stashed edit\n").unwrap();
        git(&work, &["stash", "-q"]);
        assert!(git(&work, &["status", "--porcelain"]).is_empty());
        let state = inspect(&work, None).unwrap();
        assert!(state.owes_rescue(), "{state:?}");
        assert!(state.stash);
        assert_eq!(state.local_tags, ["refs/tags/light", "refs/tags/release"]);
        assert!(state.reflog_only.contains(&reset_away), "{state:?}");

        let outcome = reclaim(&data, &id).work_dir(&work);
        assert!(outcome.complete && outcome.kept.is_empty(), "{outcome:?}");
        assert!(!work.exists());
        let rescued = outcome.rescued.unwrap();
        let heads = bundle_heads(&rescued.join("work.bundle")).unwrap();
        for name in [
            "refs/stash",
            "refs/tags/light",
            "refs/tags/release",
            "refs/notes/commits",
            "refs/anyagents-rescue/reflog-0",
        ] {
            assert!(
                heads.contains(&name.to_string()),
                "{name} missing: {heads:?}"
            );
        }
        // The stashed edit and the reset-away commit come back.
        let restored = dir.path().join("restored");
        std::fs::create_dir_all(&restored).unwrap();
        git(&restored, &["init", "-q", "--bare"]);
        git(
            &restored,
            &[
                "fetch",
                "-q",
                rescued.join("work.bundle").to_str().unwrap(),
                "+refs/*:refs/*",
            ],
        );
        assert_eq!(
            git(&restored, &["show", "refs/stash:README.md"]),
            "stashed edit"
        );
        git(
            &restored,
            &["cat-file", "-e", &format!("{reset_away}^{{commit}}")],
        );
        let manifest = manifest(&rescued);
        assert_eq!(manifest["checkouts"][0]["stash"], true);
        assert!(rescued.join("work.refs").is_file());
    }

    #[test]
    fn a_checkout_with_no_remote_or_no_commit_is_saved() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let id = AgentId::generate();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-q"]);
        std::fs::write(work.join("source.rs"), b"fn local() {}\n").unwrap();
        git(&work, &["add", "source.rs"]);
        git(&work, &["commit", "-qm", "Never pushed"]);
        let outcome = reclaim(&data, &id).work_dir(&work);
        assert!(outcome.complete && outcome.kept.is_empty(), "{outcome:?}");
        let rescued = outcome.rescued.unwrap();
        assert!(rescued.join("work.bundle").is_file());
        assert!(!work.exists());

        let unborn = dir.path().join("unborn");
        std::fs::create_dir_all(&unborn).unwrap();
        git(&unborn, &["init", "-q"]);
        std::fs::write(unborn.join("first.rs"), b"fn first() {}\n").unwrap();
        let outcome = reclaim(&data, &id).work_dir(&unborn);
        assert!(outcome.complete && outcome.kept.is_empty(), "{outcome:?}");
        let rescued = outcome.rescued.unwrap();
        assert!(!rescued.join("work.bundle").exists(), "no commit to bundle");
        assert!(rescued.join("work.uncommitted.tar").is_file());
        assert!(!unborn.exists());
    }

    #[test]
    fn an_unreadable_checkout_is_kept_with_only_its_build_output_removed() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let id = AgentId::generate();
        let work = dir.path().join("work");
        let broken = work.join("broken");
        std::fs::create_dir_all(broken.join(".git")).unwrap();
        std::fs::create_dir_all(broken.join("target/debug")).unwrap();
        std::fs::write(broken.join("source.rs"), b"fn unknown() {}\n").unwrap();
        // A pointer that names no registered worktree is not converted.
        let pointer = work.join("pointer");
        std::fs::create_dir_all(&pointer).unwrap();
        std::fs::write(pointer.join(".git"), b"gitdir: /nonexistent/broken\n").unwrap();
        std::fs::write(pointer.join("source.rs"), b"fn kept() {}\n").unwrap();
        let outcome = reclaim(&data, &id).work_dir(&work);
        let kept: Vec<_> = outcome.kept.iter().map(|(path, _)| path.clone()).collect();
        assert_eq!(kept, [broken.clone(), pointer.clone()], "{outcome:?}");
        assert!(outcome.rescued.is_none());
        assert!(broken.join("source.rs").exists());
        assert!(pointer.join("source.rs").exists());
        assert!(!broken.join("target").exists());
    }

    #[test]
    fn a_failed_rescue_deletes_nothing_that_needed_it() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let id = AgentId::generate();
        let ahead = published(dir.path(), "ahead");
        git(&ahead, &["commit", "-q", "--allow-empty", "-m", "Unpushed"]);
        let clean = published(dir.path(), "clean");
        // The rescue area cannot be created: a file sits where it would go.
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("rescue"), b"not a directory").unwrap();
        let work = dir.path().join("work");
        let outcome = reclaim(&data, &id).work_dir(&work);
        assert!(outcome.rescued.is_none());
        assert_eq!(outcome.kept.len(), 1, "{outcome:?}");
        assert!(ahead.join(".git").exists(), "unpushed work was deleted");
        assert!(!clean.exists(), "the clean checkout is still reclaimed");
    }
}
