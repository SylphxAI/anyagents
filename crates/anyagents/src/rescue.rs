//! Reclaiming an archived agent's work directory without losing work.
//!
//! When an agent is archived its managed work directory is reclaimed, one
//! checkout at a time:
//!
//! - a Git checkout with nothing unpushed and nothing uncommitted is deleted;
//! - a checkout with unpushed commits or uncommitted files is first saved to the
//!   rescue area, as a `git bundle` and a tar of the uncommitted files, and
//!   then deleted;
//! - files outside any checkout are saved as one tar, without build output,
//!   tool homes or caches;
//! - a checkout whose state cannot be read is kept where it is, its build output
//!   removed, and the operator is told.
//!
//! Git and tar run as the owner of the checkout (the agent's seat) whenever that
//! is not the server, so configuration an agent wrote into its own repository
//! never runs with the server's rights.
//!
//! A rescue lands in `{rescue}/{agent id}/{time}/` with a `manifest.json`
//! that says what each file is and how to restore it. Nothing is deleted
//! before the rescue is complete on disk.

use std::io::Write as _;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
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

/// Uncommitted paths that never make a checkout worth rescuing: what
/// AnyAgents and the harnesses write into a checkout, and build output.
fn incidental(path: &str) -> bool {
    let path = path.trim_end_matches('/');
    matches!(
        path,
        "CLAUDE.md" | "AGENTS.md" | "GEMINI.md" | ".claude" | ".gemini"
    ) || path.starts_with(".desk-worktree")
        || path.starts_with(".claude/")
        || path.starts_with(".gemini/")
        || BUILD_OUTPUT
            .iter()
            .any(|dir| path == *dir || path.starts_with(&format!("{dir}/")))
}

/// Who runs Git and tar on a checkout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Runner {
    /// `(uid, gid, groups)` through the privileged launcher, or `None` to run
    /// as the server itself.
    user: Option<(u32, u32, String)>,
}

impl Runner {
    /// The server itself.
    pub(crate) fn server() -> Self {
        Self { user: None }
    }

    /// Whoever owns `path`: the server when it owns it, else that uid through
    /// the launcher. A root-owned path, or a foreign one without a launcher,
    /// is refused.
    pub(crate) fn owner_of(path: &Path, seat_ids: Option<(u32, u32, u32)>) -> Result<Self, String> {
        let meta = std::fs::symlink_metadata(path)
            .map_err(|error| format!("cannot read its owner: {error}"))?;
        let uid = meta.uid();
        if uid == crate::sandbox::server_uid() {
            return Ok(Self::server());
        }
        if uid == 0 {
            return Err("it is owned by root".into());
        }
        let launcher = crate::sandbox::launcher();
        if !launcher.is_file() {
            return Err(format!(
                "it is owned by uid {uid} and {} is not installed to act as it",
                launcher.display()
            ));
        }
        let seat = seat_ids.filter(|(seat_uid, _, _)| *seat_uid == uid);
        let gid = seat.map_or(meta.gid(), |(_, gid, _)| gid);
        let groups = match seat {
            Some((_, gid, socket)) if socket != gid && socket != 0 => format!("{gid},{socket}"),
            _ => gid.to_string(),
        };
        Ok(Self {
            user: Some((uid, gid, groups)),
        })
    }

    fn command(&self, program: &Path) -> Command {
        let mut cmd = match &self.user {
            None => Command::new(program),
            Some((uid, gid, groups)) => {
                let mut cmd = Command::new(crate::sandbox::launcher());
                cmd.args([
                    "--reuid",
                    &uid.to_string(),
                    "--regid",
                    &gid.to_string(),
                    "--groups",
                    groups,
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
            .env("GIT_ATTR_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("LC_ALL", "C")
            .stdin(Stdio::null());
        cmd
    }

    /// Git in `repo`, with the repository's hooks, fsmonitor and the user's
    /// configuration turned off and no optional lock taken.
    pub(crate) fn git(&self, repo: &Path) -> Result<Command, String> {
        let git = anyagents_toolchain::tool("git", "archive rescue").map_err(|e| e.to_string())?;
        let mut cmd = self.command(git.path());
        cmd.args([
            "--no-optional-locks",
            "-c",
            "safe.directory=*",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
            "-C",
        ])
        .arg(repo);
        Ok(cmd)
    }

    fn tar(&self, dir: &Path) -> Result<Command, String> {
        let tar = anyagents_toolchain::tool("tar", "archive rescue").map_err(|e| e.to_string())?;
        let mut cmd = self.command(tar.path());
        cmd.arg("-C").arg(dir);
        Ok(cmd)
    }
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
    /// A linked worktree of a shared repository, not a clone of its own.
    pub(crate) linked: bool,
    /// The checked-out branch, `refs/heads/...`, if any.
    pub(crate) branch: Option<String>,
    /// `remote.origin.url`, to restore a checkout from.
    pub(crate) origin: Option<String>,
    /// Commits no remote-tracking ref has: on HEAD or any local branch of a
    /// clone of its own; on HEAD alone of a linked worktree, whose branches
    /// live on in the shared repository when the checkout is deleted.
    pub(crate) unpushed: u64,
    pub(crate) changes: Vec<Change>,
    status: Vec<u8>,
}

impl RepoState {
    /// Uncommitted changes that are worth keeping.
    pub(crate) fn dirty(&self) -> impl Iterator<Item = &Change> {
        self.changes.iter().filter(|c| !incidental(&c.path_text()))
    }

    /// Deleting this checkout would lose something.
    pub(crate) fn owes_rescue(&self) -> bool {
        self.unpushed > 0 || self.dirty().next().is_some()
    }
}

/// Read a checkout's state through `git`, which returns a Git command already
/// pointed at the checkout.
pub(crate) fn inspect(git: &dyn Fn() -> Result<Command, String>) -> Result<RepoState, String> {
    let dirs = stdout(
        git()?,
        &[
            "rev-parse",
            "--path-format=absolute",
            "--git-dir",
            "--git-common-dir",
        ],
    )?;
    let dirs = String::from_utf8_lossy(&dirs);
    let mut lines = dirs.lines();
    let (git_dir, common_dir) = (lines.next().unwrap_or(""), lines.next().unwrap_or(""));
    if git_dir.is_empty() || common_dir.is_empty() {
        return Err("git rev-parse named no repository".into());
    }
    let linked = Path::new(git_dir) != Path::new(common_dir);
    let status = stdout(
        git()?,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ],
    )?;
    let head = output(git()?, &["rev-parse", "--verify", "-q", "HEAD^{commit}"])?;
    let head = head
        .status
        .success()
        .then(|| String::from_utf8_lossy(&head.stdout).trim().to_string())
        .filter(|sha| !sha.is_empty());
    let unpushed = if head.is_some() {
        let mut args = vec!["rev-list", "--count", "HEAD"];
        if !linked {
            args.push("--branches");
        }
        args.extend(["--not", "--remotes"]);
        let count = stdout(git()?, &args)?;
        String::from_utf8_lossy(&count)
            .trim()
            .parse::<u64>()
            .map_err(|_| "git rev-list printed no count".to_string())?
    } else {
        0
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
    Ok(RepoState {
        head,
        linked,
        branch,
        origin,
        unpushed,
        changes: parse_status(&status),
        status,
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
    /// A linked worktree: its bundle holds only commits no remote has.
    pub(crate) linked: bool,
    pub(crate) unpushed_commits: u64,
    pub(crate) uncommitted_paths: usize,
    pub(crate) bundle: Option<String>,
    pub(crate) bundle_refs: Vec<String>,
    pub(crate) uncommitted_tar: Option<String>,
    pub(crate) status: String,
    pub(crate) bytes: u64,
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
    if input.is_some() {
        cmd.stdin(Stdio::piped());
    }
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

type OsStrLike = std::ffi::OsStr;

fn os(text: &str) -> &OsStrLike {
    OsStrLike::new(text)
}

/// Save one checkout into `out` under the file name prefix `slug`.
pub(crate) fn save_repo(
    git: &dyn Fn() -> Result<Command, String>,
    tar: &Runner,
    checkout: &Path,
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
        linked: state.linked,
        unpushed_commits: state.unpushed,
        uncommitted_paths: state.dirty().count(),
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
    // A clone of its own carries every ref, so it restores on its own. A
    // linked worktree shares its history and branches with every other
    // worktree of its repository, which this does not delete: bundling all of
    // it would copy the whole shared repository once per checkout. It
    // carries HEAD (and its branch) down to what the remotes already have,
    // and nothing when HEAD is pushed.
    let refs: Option<Vec<String>> = match (&state.head, state.linked) {
        (None, _) => None,
        (Some(_), false) => Some(vec!["--all".to_string()]),
        (Some(_), true) if state.unpushed == 0 => None,
        (Some(_), true) => Some(
            std::iter::once("HEAD".to_string())
                .chain(state.branch.clone())
                .chain(["--not".to_string(), "--remotes".to_string()])
                .collect(),
        ),
    };
    if let Some(refs) = refs {
        let file = out.join(format!("{slug}.bundle"));
        let mut args: Vec<&OsStrLike> = vec![os("bundle"), os("create"), os("-")];
        args.extend(refs.iter().map(|r| os(r)));
        saved.bytes += write_to(&file, git()?, &args, None)
            .map_err(|error| format!("git bundle failed: {error}"))?;
        // The bundle must name its refs when read back on its own.
        let listed = bundle_heads(&file)?;
        if listed.is_empty() {
            return Err("git bundle wrote no refs".into());
        }
        saved.bundle = Some(format!("{slug}.bundle"));
        saved.bundle_refs = listed;
    }
    let mut names = Vec::new();
    for change in state.changes.iter().filter(|c| c.present()) {
        names.extend_from_slice(&change.path);
        names.push(0);
    }
    if !names.is_empty() {
        let file = out.join(format!("{slug}.uncommitted.tar"));
        let args = [os("--null"), os("-T"), os("-"), os("-cf"), os("-")];
        saved.bytes += write_to(&file, tar.tar(checkout)?, &args, Some(&names))
            .map_err(|error| format!("tar of uncommitted files failed: {error}"))?;
        saved.uncommitted_tar = Some(format!("{slug}.uncommitted.tar"));
    }
    Ok(saved)
}

/// The refs a bundle file lists, read with no repository and no user config.
fn bundle_heads(file: &Path) -> Result<Vec<String>, String> {
    let git = anyagents_toolchain::tool("git", "archive rescue").map_err(|e| e.to_string())?;
    let mut cmd = Runner::server().command(git.path());
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

/// One checkout inside the work directory and how to reach its Git.
pub(crate) struct Checkout {
    pub(crate) path: PathBuf,
    /// The journal AnyAgents recorded when it created this checkout.
    pub(crate) recorded: Option<crate::worktree_access::RecordedWorktree>,
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
/// top-level entries that are not build output.
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
    let free = stat.f_bavail as u64 * stat.f_frsize as u64;
    #[allow(clippy::unnecessary_cast)]
    let size = stat.f_blocks as u64 * stat.f_frsize as u64;
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
    pub(crate) seat_ids: Option<(u32, u32, u32)>,
}

enum Plan {
    Delete(Checkout),
    Save(Checkout, RepoState, Runner),
}

impl Reclaim<'_> {
    fn state_of(&self, checkout: &Checkout) -> Result<(RepoState, Runner), String> {
        let tar = Runner::owner_of(&checkout.path, self.seat_ids)?;
        match &checkout.recorded {
            Some(record) => {
                if !record.checkout_pointer_matches() {
                    return Err("its .git pointer is missing or changed".into());
                }
                let state = record
                    .inspect(|| {
                        inspect(&|| record.command().map_err(|e| e.to_string()))
                            .map_err(std::io::Error::other)
                    })
                    .map_err(|error| error.to_string())?;
                Ok((state, tar))
            }
            None => {
                let state = inspect(&|| tar.git(&checkout.path))?;
                Ok((state, tar))
            }
        }
    }

    /// Reclaim `work`, finding each checkout's journal with `recorded`.
    pub(crate) fn work_dir(
        &self,
        work: &Path,
        recorded: &dyn Fn(&Path) -> Option<crate::worktree_access::RecordedWorktree>,
    ) -> Outcome {
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
        for path in repos {
            let checkout = Checkout {
                recorded: recorded(&path),
                path,
            };
            match self.state_of(&checkout) {
                Ok((state, _)) if !state.owes_rescue() => plans.push(Plan::Delete(checkout)),
                Ok((state, runner)) => plans.push(Plan::Save(checkout, state, runner)),
                Err(reason) => {
                    outcome.complete &= crate::server::reap_build_outputs_in_worktree(
                        &checkout.path,
                        self.seat_ids,
                    );
                    outcome.kept.push((checkout.path, reason));
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
                        Plan::Save(checkout, ..) => {
                            outcome.complete &= crate::server::reap_build_outputs_in_worktree(
                                &checkout.path,
                                self.seat_ids,
                            );
                            outcome
                                .kept
                                .push((checkout.path.clone(), format!("rescue failed: {reason}")));
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
            let (Plan::Delete(checkout) | Plan::Save(checkout, ..)) = plan;
            if matches!(plan, Plan::Delete(_)) {
                outcome.clean += 1;
            }
            outcome.complete &= self.delete_checkout(checkout);
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

    fn delete_checkout(&self, checkout: &Checkout) -> bool {
        let mut complete = crate::server::remove_path_with_seat(&checkout.path, self.seat_ids);
        // A recorded worktree's private Git directory goes with it; only the
        // journal's validated path, never one read back from the checkout.
        if let Some(record) = &checkout.recorded {
            if record.validate().is_ok() {
                complete &= crate::server::remove_path_with_seat(record.private(), self.seat_ids);
            } else {
                complete = false;
            }
        }
        complete
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
            let Plan::Save(checkout, state, runner) = plan else {
                continue;
            };
            let rel = checkout
                .path
                .strip_prefix(work)
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
                .filter(|p| !p.is_empty())
                .unwrap_or_else(|| ".".to_string());
            let name = slug(&rel, &mut taken);
            let saved = match &checkout.recorded {
                Some(record) => record
                    .inspect(|| {
                        save_repo(
                            &|| record.command().map_err(|e| e.to_string()),
                            runner,
                            &checkout.path,
                            state,
                            &rel,
                            out,
                            &name,
                        )
                        .map_err(std::io::Error::other)
                    })
                    .map_err(|error| error.to_string()),
                None => save_repo(
                    &|| runner.git(&checkout.path),
                    runner,
                    &checkout.path,
                    state,
                    &rel,
                    out,
                    &name,
                ),
            }
            .map_err(|error| format!("{rel}: {error}"))?;
            repos.push(saved);
        }
        let mut loose_tar = None;
        if !loose.is_empty() {
            let runner = Runner::owner_of(work, self.seat_ids)?;
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
            write_to(&file, runner.tar(work)?, &args, Some(&names))
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
            "restore": "A clone of its own: git clone <name>.bundle <dir>. A linked worktree: clone its origin, git fetch <name>.bundle HEAD if there is one, and check out its head. Then tar -xf <name>.uncommitted.tar -C <dir>. loose.tar holds the files that sat outside any checkout.",
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
        assert!(incidental("CLAUDE.md"));
        assert!(incidental("target/debug/x"));
        assert!(incidental(".claude/settings.json"));
        assert!(!incidental("src/target.rs"));
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
        std::fs::write(dirty.join("draft.md"), b"untracked draft\n").unwrap();
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

        let outcome = reclaim(&data, &id).work_dir(&work, &|_| None);
        assert!(outcome.complete, "{outcome:?}");
        assert!(outcome.kept.is_empty(), "{:?}", outcome.kept);
        assert_eq!(outcome.clean, 1);
        assert!(!work.exists(), "the reclaimed work directory remains");
        let rescued = outcome.rescued.expect("owed work was rescued");
        assert!(rescued.starts_with(data.join("rescue").join(id.as_str())));
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(rescued.join("manifest.json")).unwrap()).unwrap();
        let checkouts = manifest["checkouts"].as_array().unwrap();
        assert_eq!(checkouts.len(), 2, "{manifest:#}");
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
        let listing = Command::new("tar")
            .arg("-tf")
            .arg(rescued.join("dirty.uncommitted.tar"))
            .output()
            .unwrap();
        let listing = String::from_utf8_lossy(&listing.stdout);
        assert!(
            listing.contains("README.md") && listing.contains("draft.md"),
            "{listing}"
        );
        assert!(!listing.contains("target"), "{listing}");
        let loose = Command::new("tar")
            .arg("-tf")
            .arg(rescued.join("loose.tar"))
            .output()
            .unwrap();
        let loose = String::from_utf8_lossy(&loose.stdout);
        assert!(loose.contains("report.md"), "{loose}");
        assert!(loose.contains("notes/plan.md"), "{loose}");
        assert!(!loose.contains("node_modules"), "{loose}");
        assert!(!loose.contains(".bun-cache"), "{loose}");
        assert!(!loose.contains("notes/target"), "{loose}");
        assert!(!loose.contains("clean"), "{loose}");
    }

    #[test]
    fn a_linked_worktree_bundles_only_what_no_remote_has() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let id = AgentId::generate();
        // The shared repository lives outside the work directory.
        let shared = published(&dir.path().join("shared"), "repo");
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let ahead = work.join("ahead");
        let dirty = work.join("dirty");
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
        git(
            &shared,
            &[
                "worktree",
                "add",
                "-q",
                dirty.to_str().unwrap(),
                "-b",
                "drafts",
            ],
        );
        std::fs::write(ahead.join("feature.rs"), b"fn feature() {}\n").unwrap();
        git(&ahead, &["add", "feature.rs"]);
        git(&ahead, &["commit", "-qm", "Unpushed feature"]);
        let unpushed_head = git(&ahead, &["rev-parse", "HEAD"]);
        std::fs::write(dirty.join("README.md"), b"edited\n").unwrap();
        let pushed_head = git(&dirty, &["rev-parse", "HEAD"]);

        let outcome = reclaim(&data, &id).work_dir(&work, &|_| None);
        assert!(outcome.complete && outcome.kept.is_empty(), "{outcome:?}");
        assert!(!work.exists());
        let rescued = outcome.rescued.unwrap();
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(rescued.join("manifest.json")).unwrap()).unwrap();
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
        assert_eq!(ahead_entry["linked"], true);
        assert_eq!(ahead_entry["head"], unpushed_head.as_str());
        let verify = Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .args(["-C", shared.to_str().unwrap(), "bundle", "verify", "-q"])
            .arg(rescued.join("ahead.bundle"))
            .output()
            .unwrap();
        assert!(
            verify.status.success(),
            "{}",
            String::from_utf8_lossy(&verify.stderr)
        );
        let heads = bundle_heads(&rescued.join("ahead.bundle")).unwrap();
        assert!(
            heads.contains(&"refs/heads/feature".to_string()),
            "{heads:?}"
        );
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
        // Deleting the checkouts leaves the shared repository's branches.
        assert_eq!(
            git(&shared, &["rev-parse", "refs/heads/feature"]),
            unpushed_head
        );
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
        let outcome = reclaim(&data, &id).work_dir(&work, &|_| None);
        assert!(outcome.complete && outcome.kept.is_empty(), "{outcome:?}");
        let rescued = outcome.rescued.unwrap();
        assert!(rescued.join("work.bundle").is_file());
        assert!(!work.exists());

        let unborn = dir.path().join("unborn");
        std::fs::create_dir_all(&unborn).unwrap();
        git(&unborn, &["init", "-q"]);
        std::fs::write(unborn.join("first.rs"), b"fn first() {}\n").unwrap();
        let outcome = reclaim(&data, &id).work_dir(&unborn, &|_| None);
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
        let outcome = reclaim(&data, &id).work_dir(&work, &|_| None);
        assert_eq!(outcome.kept.len(), 1, "{outcome:?}");
        assert_eq!(outcome.kept[0].0, broken);
        assert!(outcome.rescued.is_none());
        assert!(broken.join("source.rs").exists());
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
        let outcome = reclaim(&data, &id).work_dir(&work, &|_| None);
        assert!(outcome.rescued.is_none());
        assert_eq!(outcome.kept.len(), 1, "{outcome:?}");
        assert!(ahead.join(".git").exists(), "unpushed work was deleted");
        assert!(!clean.exists(), "the clean checkout is still reclaimed");
    }
}
