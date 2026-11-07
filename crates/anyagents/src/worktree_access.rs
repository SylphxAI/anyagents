//! Worktree sharing and admission, including both kinds of Git metadata.
use std::path::{Path, PathBuf};

use anyagents_core::Error;

fn refusal(path: &Path, message: impl std::fmt::Display) -> Error {
    Error::WorkdirPermission {
        path: path.display().to_string(),
        message: message.to_string(),
    }
}

fn git_dirs(path: &Path) -> Result<Vec<PathBuf>, Error> {
    if !path.join(".git").exists() {
        return Ok(Vec::new());
    }
    let mut dirs = Vec::new();
    for flag in ["--git-dir", "--git-common-dir"] {
        let out = std::process::Command::new("git")
            .args([
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.hooksPath=/dev/null",
                "-C",
            ])
            .arg(path)
            .args(["rev-parse", "--path-format=absolute", flag])
            .output()
            .map_err(|e| refusal(path, e))?;
        if !out.status.success() {
            return Err(refusal(path, String::from_utf8_lossy(&out.stderr)));
        }
        let dir =
            std::fs::canonicalize(PathBuf::from(String::from_utf8_lossy(&out.stdout).trim()))?;
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }
    Ok(dirs)
}

/// The consumer enables worktreeConfig for sparse checkout. Refuse rather than
/// let --worktree silently fall back to rewriting the common configuration.
pub(crate) fn set_worktree_identity(path: &Path) -> Result<(), Error> {
    let dirs = git_dirs(path)?;
    let (Some(private), Some(common)) = (dirs.first(), dirs.last()) else {
        return Err(refusal(path, "consumer did not produce Git metadata"));
    };
    if private == common {
        return Err(refusal(path, "identity requires a linked worktree"));
    }
    validate_config_files(private, common)?;
    let extension = std::process::Command::new("git")
        .args(["config", "--no-includes", "--file"])
        .arg(common.join("config"))
        .args(["--bool", "--get", "extensions.worktreeConfig"])
        .output()?;
    if !extension.status.success() || extension.stdout != b"true\n" {
        return Err(refusal(
            path,
            "consumer must enable extensions.worktreeConfig",
        ));
    }
    // No agent has been admitted yet. This parent-controlled private file will
    // receive the read-only ACL/sticky protection before the seat can run.
    let config = private.join("config.worktree");
    if config.symlink_metadata().is_ok_and(|m| !m.is_file()) {
        return Err(refusal(&config, "worktree config is not a regular file"));
    }
    for (key, value) in [("user.name", "Kyle Tse"), ("user.email", "hi@kylet.se")] {
        let out = std::process::Command::new("git")
            .args(["config", "--no-includes", "--file"])
            .arg(&config)
            .args([key, value])
            .output()?;
        if !out.status.success() {
            return Err(refusal(&config, "cannot set worktree commit identity"));
        }
    }
    Ok(())
}

/// Includes can redirect otherwise protected configuration to child-writable
/// files. Reject the graph at admission and before local-source Git reads.
pub(crate) fn validate_git_config(path: &Path) -> Result<(), Error> {
    let dirs = git_dirs(path)?;
    if let Some(common) = dirs.last() {
        validate_config_files(&dirs[0], common)?;
    }
    Ok(())
}

fn validate_config_files(private: &Path, common: &Path) -> Result<(), Error> {
    use std::os::unix::fs::OpenOptionsExt;
    for path in [
        common.join("config"),
        common.join("config.worktree"),
        private.join("config.worktree"),
    ] {
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
        {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(refusal(&path, e)),
        };
        if !file.metadata()?.is_file() {
            return Err(refusal(&path, "Git configuration is not a regular file"));
        }
        // --file - and --no-includes inspect only the opened input. Never
        // follow the include or print config values in a diagnostic.
        let out = std::process::Command::new("git")
            .args([
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "config",
                "--no-includes",
                "--file",
                "-",
                "--name-only",
                "--get-regexp",
                "^(include\\.path|includeif\\..*\\.path)$",
            ])
            .stdin(file)
            .output()
            .map_err(|e| refusal(&path, e))?;
        match out.status.code() {
            Some(1) => {}
            Some(0) => {
                return Err(refusal(
                    &path,
                    "Git include/includeIf is unsupported for shared worktrees",
                ));
            }
            _ => {
                return Err(refusal(
                    &path,
                    "cannot validate Git configuration without includes",
                ));
            }
        }
    }
    Ok(())
}

/// Replace the legacy wildcard with a reset plus exact admitted roots. Preserve
/// other caller settings; bind the final replacement to the opened config dir,
/// never a seat-replaceable .gitconfig symlink or hard-link target.
pub(crate) fn configure_seat_git(config: &Path, workdir: &Path) -> Result<(), Error> {
    use std::io::{Read, Write};
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    };
    let data = crate::create::data_dir_from_config(config)
        .ok_or_else(|| refusal(config, "missing agent data root"))?;
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(config)?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c".gitconfig".as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    let mut bytes = Vec::new();
    if fd >= 0 {
        let mut old = unsafe { std::fs::File::from_raw_fd(fd) };
        let meta = old.metadata()?;
        if !meta.is_file() || meta.nlink() != 1 || meta.len() > 1024 * 1024 {
            return Err(refusal(config, "unsafe or oversized .gitconfig"));
        }
        old.read_to_end(&mut bytes)?;
    } else if std::io::Error::last_os_error().kind() != std::io::ErrorKind::NotFound {
        return Err(refusal(config, std::io::Error::last_os_error()));
    }
    // Harness fixtures can deliberately omit system tools from PATH. Preserve
    // their configured precedence, then include the OS Git used for parsing.
    let mut paths: Vec<_> = std::env::var_os("PATH")
        .map(|value| std::env::split_paths(&value).collect())
        .unwrap_or_default();
    paths.extend([PathBuf::from("/usr/bin"), PathBuf::from("/bin")]);
    let path_env = std::env::join_paths(paths).map_err(|e| refusal(config, e))?;
    let staging = data.join("worktree-grants");
    std::fs::create_dir_all(&staging)?;
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
            Ok(std::process::Command::new("git")
                .env("PATH", &path_env)
                .args(["config", "--no-includes", "--file"])
                .arg(&temporary)
                .args(args)
                .output()?)
        };
        let unset = edit(&["--unset-all", "safe.directory"])?;
        if !unset.status.success() && unset.status.code() != Some(5) {
            return Err(refusal(config, "cannot narrow safe.directory"));
        }
        let mut roots = vec![std::path::absolute(workdir)?];
        if let Some(id) = config
            .parent()
            .and_then(Path::file_name)
            .and_then(|s| s.to_str())
        {
            let journal = data.join("worktree-grants").join(format!("{id}.json"));
            if let Ok(raw) = std::fs::read(journal) {
                let grant: Grant = serde_json::from_slice(&raw).map_err(|e| refusal(config, e))?;
                if grant.active && grant.checkout == roots[0] {
                    roots.push(grant.common);
                }
            }
        }
        if roots
            .iter()
            .any(|root| root.as_os_str().as_encoded_bytes().contains(&b'*'))
        {
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
            for root in
                std::iter::once(String::new()).chain(roots.iter().map(|p| p.display().to_string()))
            {
                let out = std::process::Command::new("git")
                    .env("PATH", &path_env)
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
        if unsafe { libc::fchown(file.as_raw_fd(), u32::MAX, directory.metadata()?.gid()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
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

/// The journal lives under the server-owned data directory, outside the seat's
/// config/worktree. Persist it before granting anything so partial admission can
/// always revoke, even if the checkout or its .git pointer later disappears.
#[derive(serde::Serialize, serde::Deserialize)]
struct Grant {
    common: PathBuf,
    gid: u32,
    uid: u32,
    private: PathBuf,
    checkout: PathBuf,
    active: bool,
}

pub(crate) fn grant_path(data: &Path, id: &anyagents_core::AgentId) -> PathBuf {
    data.join("worktree-grants").join(format!("{id}.json"))
}

// st_mode's group bits are the POSIX ACL mask, not the owning-group entry.
// A preserved named-owner rw grant can make that mask writable while every
// foreign principal remains read-only. Inspect effective entries instead.
fn writable_by_untrusted(path: &Path, owners: &[u32]) -> std::io::Result<bool> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let meta = file.metadata()?;
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let mut bytes = vec![0u8; 65536];
        let len = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                c"system.posix_acl_access".as_ptr(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
            )
        };
        if len >= 0 {
            bytes.truncate(len as usize);
            if bytes.len() < 4
                || bytes[..4] != 2u32.to_le_bytes()
                || !(bytes.len() - 4).is_multiple_of(8)
            {
                return Err(std::io::Error::other("invalid metadata ACL"));
            }
            let entries: Vec<_> = bytes[4..]
                .as_chunks::<8>()
                .0
                .iter()
                .map(|b| {
                    (
                        u16::from_le_bytes([b[0], b[1]]),
                        u16::from_le_bytes([b[2], b[3]]),
                        u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
                    )
                })
                .collect();
            let mask = entries.iter().find(|e| e.0 == 16).map_or(7, |e| e.1);
            return Ok(entries.iter().any(|(tag, perm, uid)| match tag {
                2 => perm & mask & 2 != 0 && !owners.contains(uid),
                4 | 8 => perm & mask & 2 != 0,
                32 => perm & 2 != 0,
                _ => false,
            }));
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::ENODATA) {
            return Err(err);
        }
    }
    Ok(meta.mode() & 0o022 != 0)
}

/// The checkout's .git file is seat-writable. Maintenance may only use this
/// server-owned record, including after revocation; it must never rediscover a
/// gitdir through the checkout or the seat-owned marker.
pub(crate) struct RecordedWorktree(Grant);

impl RecordedWorktree {
    pub(crate) fn load(data: &Path, id: &anyagents_core::AgentId, checkout: &Path) -> Option<Self> {
        let bytes = std::fs::read(grant_path(data, id)).ok()?;
        let grant: Grant = serde_json::from_slice(&bytes).ok()?;
        (grant.checkout == checkout).then_some(Self(grant))
    }

    pub(crate) fn validate(&self) -> std::io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let g = &self.0;
        if g.private.parent() != Some(g.common.join("worktrees").as_path()) {
            return Err(std::io::Error::other("unrecorded private gitdir layout"));
        }
        let owner = std::fs::symlink_metadata(&g.private)?.uid();
        if owner == g.uid && owner != crate::sandbox::server_uid() {
            return Err(std::io::Error::other("seat-owned private gitdir"));
        }
        for path in [&g.common, &g.common.join("worktrees"), &g.private] {
            let meta = std::fs::symlink_metadata(path)?;
            if !meta.is_dir()
                || meta.uid() != owner
                || std::fs::canonicalize(path)? != *path
                || path != &g.private
                    && writable_by_untrusted(path, &[owner, crate::sandbox::server_uid()])?
            {
                return Err(std::io::Error::other("untrusted Git metadata directory"));
            }
        }
        for path in [
            g.common.join("config"),
            g.common.join("config.worktree"),
            g.private.join("config.worktree"),
            g.private.join("commondir"),
        ] {
            let meta = match std::fs::symlink_metadata(&path) {
                Err(e)
                    if path == g.common.join("config.worktree")
                        && e.kind() == std::io::ErrorKind::NotFound =>
                {
                    continue;
                }
                other => other?,
            };
            if !meta.is_file()
                || meta.nlink() != 1
                || writable_by_untrusted(&path, &[owner, crate::sandbox::server_uid()])?
                || ![owner, crate::sandbox::server_uid()].contains(&meta.uid())
            {
                return Err(std::io::Error::other(
                    "untrusted Git configuration or common-dir pointer",
                ));
            }
        }
        if std::fs::read_to_string(g.private.join("commondir"))?.trim() != "../.." {
            return Err(std::io::Error::other("unrecorded common-dir pointer"));
        }
        validate_config_files(&g.private, &g.common).map_err(std::io::Error::other)
    }

    pub(crate) fn command(&self) -> std::io::Result<std::process::Command> {
        self.validate()?;
        let mut cmd = std::process::Command::new("git");
        cmd.env_remove("GIT_CONFIG_COUNT")
            .env_remove("GIT_CONFIG_PARAMETERS")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR")
            .env_remove("GIT_INDEX_FILE")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_ATTR_NOSYSTEM", "1")
            .args([
                "--no-optional-locks",
                "-c",
                "safe.directory=",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.attributesFile=/dev/null",
            ])
            .arg("--git-dir")
            .arg(&self.0.private)
            .arg("--work-tree")
            .arg(&self.0.checkout);
        Ok(cmd)
    }

    pub(crate) fn private(&self) -> &Path {
        &self.0.private
    }
}

pub(crate) fn revoke_recorded(data: &Path, id: &anyagents_core::AgentId) -> Result<(), Error> {
    let journal = grant_path(data, id);
    let _lock = lock_journals(&journal).map_err(|e| refusal(&journal, e))?;
    let bytes = match std::fs::read(&journal) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(refusal(&journal, e)),
    };
    let mut grant: Grant = serde_json::from_slice(&bytes).map_err(|e| refusal(&journal, e))?;
    if !grant.active {
        return Ok(());
    }
    let retained = other_active_grants(&journal, &grant).map_err(|e| refusal(&journal, e))?;
    revoke_common(&grant.common, grant.uid, grant.gid, &retained)
        .map_err(|e| refusal(&grant.common, e))?;
    grant.active = false;
    write_grant(&journal, &grant).map_err(|e| refusal(&journal, e))
}

fn write_grant(journal: &Path, grant: &Grant) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::create_dir_all(journal.parent().unwrap())?;
    let temporary = journal.with_extension(format!("{}.tmp", anyagents_core::AgentId::generate()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(grant)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, journal)?;
    std::fs::File::open(journal.parent().unwrap())?.sync_all()
}

fn lock_journals(journal: &Path) -> std::io::Result<std::fs::File> {
    use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};
    let directory = journal.parent().unwrap();
    std::fs::create_dir_all(directory)?;
    std::fs::File::open(directory.parent().unwrap())?.sync_all()?;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(directory.join(".lock"))?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(lock) // closing the descriptor releases the lock, including on refusal
}

fn other_active_grants(journal: &Path, grant: &Grant) -> std::io::Result<Vec<Grant>> {
    let mut retained = Vec::new();
    for entry in std::fs::read_dir(journal.parent().unwrap())? {
        let path = entry?.path();
        if path == journal || path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let other: Grant = serde_json::from_slice(&std::fs::read(path)?)?;
        if other.active && other.common == grant.common {
            retained.push(other);
        }
    }
    Ok(retained)
}

fn retained_path(common: &Path, path: &Path, grant: &Grant) -> bool {
    path == common
        || path == common.join("worktrees")
        || path == common.join("logs")
        || path == common.join("config")
        || path == common.join("HEAD")
        || ["objects", "refs", "logs/refs", "packed-refs"]
            .iter()
            .any(|name| path.starts_with(common.join(name)))
        || path.starts_with(&grant.private)
}

fn revoke_common(common: &Path, uid: u32, gid: u32, retained: &[Grant]) -> std::io::Result<()> {
    walk_tree(common, false, &|_| {}, &|file, path| {
        // Group and named-user bindings have independent lifetimes. Preserve
        // either only where another active journal still needs that binding.
        let keep_group = retained
            .iter()
            .any(|g| g.gid == gid && retained_path(common, path, g));
        let keep_user = retained
            .iter()
            .any(|g| g.uid == uid && retained_path(common, path, g));
        if keep_group && keep_user {
            return Ok(());
        }
        if keep_group {
            // The reserved ID cannot be a POSIX named group. This removes only
            // the expired named-user binding when a different UID shares a GID.
            return update_metadata_acl(file, u32::MAX, None, false, (!keep_user).then_some(uid));
        }
        update_metadata_acl(file, gid, None, false, (!keep_user).then_some(uid))
    })
}

/// Change only opened objects, never a symlink's target. Metadata sharing is an
/// allowlist: the common store and worktrees/ are traversal-only, never writable
/// or default-ACL parents. Only shared content and this seat's private gitdir
/// receive write grants; sibling metadata, config, hooks and info stay protected.
pub(crate) fn share(path: &Path, uid: u32, gid: u32, journal: &Path) -> Result<(), Error> {
    let dirs = git_dirs(path)?;
    share_tree(path, gid).map_err(|e| refusal(path, e))?;
    let Some(common) = dirs.last() else {
        return Ok(());
    };
    let private = &dirs[0];
    // A single-principal ordinary checkout needs no cross-seat metadata grant.
    // Cross-seat sharing is supported only for linked worktrees.
    if private == common {
        if uid == crate::sandbox::server_uid() && gid == crate::sandbox::server_gid() {
            return Ok(());
        }
        return Err(refusal(
            path,
            "cross-seat sharing requires a linked Git worktree",
        ));
    }
    if private.parent() != Some(common.join("worktrees").as_path()) {
        return Err(refusal(
            path,
            "private gitdir is not a direct common/worktrees child",
        ));
    }
    share_known(path, private, common, uid, gid, journal)
}

/// A direct wake can restore a retained dirty checkout's grant, but only from
/// the server's recorded metadata paths, never a child-editable .git pointer.
pub(crate) fn restore_recorded(data: &Path, id: &anyagents_core::AgentId) -> Result<(), Error> {
    let journal = grant_path(data, id);
    let _lock = lock_journals(&journal).map_err(|e| refusal(&journal, e))?;
    let bytes = match std::fs::read(&journal) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(refusal(&journal, e)),
    };
    let grant: Grant = serde_json::from_slice(&bytes).map_err(|e| refusal(&journal, e))?;
    if grant.active || !grant.checkout.join(".git").exists() {
        return Ok(());
    }
    share_known_locked(
        &grant.checkout,
        &grant.private,
        &grant.common,
        grant.uid,
        grant.gid,
        &journal,
    )
}

fn share_known(
    path: &Path,
    private: &Path,
    common: &Path,
    uid: u32,
    gid: u32,
    journal: &Path,
) -> Result<(), Error> {
    let _lock = lock_journals(journal).map_err(|e| refusal(journal, e))?;
    share_known_locked(path, private, common, uid, gid, journal)
}

fn share_known_locked(
    path: &Path,
    private: &Path,
    common: &Path,
    uid: u32,
    gid: u32,
    journal: &Path,
) -> Result<(), Error> {
    validate_config_files(private, common)?;
    let grant = Grant {
        common: common.to_path_buf(),
        gid,
        uid,
        private: private.to_path_buf(),
        checkout: path.to_path_buf(),
        active: true,
    };
    let retained = other_active_grants(journal, &grant).map_err(|e| refusal(journal, e))?;
    write_grant(journal, &grant).map_err(|e| refusal(journal, e))?;
    // Clear any legacy broad grants/defaults for this seat before the allowlist.
    revoke_common(common, uid, gid, &retained).map_err(|e| refusal(common, e))?;
    for root in [
        common.to_path_buf(),
        common.join("worktrees"),
        common.join("logs"),
    ]
    .into_iter()
    .filter(|root| root.exists())
    {
        with_directory(&root, &|file| {
            restrict_others(file)?;
            clear_default_acl(file)?;
            update_metadata_acl(file, gid, Some(1), false, None)
        })
        .map_err(|e| refusal(&root, e))?;
    }
    // The main worktree's logs/HEAD is the publisher's independent baseline,
    // not shared content. Only branch/tag reflogs belong in the write set.
    for name in ["objects", "refs", "logs/refs", "packed-refs"] {
        let root = common.join(name);
        if root.exists() {
            walk_tree(&root, false, &|_| {}, &|file, _| {
                grant_metadata_acl(file, gid)
            })
            .map_err(|e| refusal(&root, e))?;
        }
    }
    // Config is readable but never writable. A common 0600 config must still
    // work without granting write access or exposing it to unrelated seats.
    for name in ["config", "HEAD"] {
        let root = common.join(name);
        if root.exists() {
            walk_tree(&root, false, &|_| {}, &|file, _| {
                update_metadata_acl(file, gid, Some(4), false, None)
            })
            .map_err(|e| refusal(&root, e))?;
        }
    }
    protect_private_gitdir(private, uid, gid).map_err(|e| refusal(private, e))?;
    Ok(())
}

#[cfg(unix)]
fn with_directory(
    path: &Path,
    apply: &dyn Fn(&std::fs::File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    // walk_tree binds its callback to no-follow descriptors. Do not descend.
    walk_tree_depth(path, false, &|_| {}, &|f, _| apply(f), false)
}

#[cfg(target_os = "linux")]
fn clear_default_acl(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::fremovexattr(file.as_raw_fd(), c"system.posix_acl_default".as_ptr()) } != 0
        && std::io::Error::last_os_error().raw_os_error() != Some(libc::ENODATA)
    {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            c"system.posix_acl_default".as_ptr(),
            std::ptr::null_mut(),
            0,
        )
    } >= 0
        || std::io::Error::last_os_error().raw_os_error() != Some(libc::ENODATA)
    {
        return Err(std::io::Error::other(
            "default ACL removal readback differs",
        ));
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn clear_default_acl(_file: &std::fs::File) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_others(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    let meta = file.metadata()?;
    if !meta.is_dir() && meta.nlink() != 1 {
        return Err(std::io::Error::other(
            "refusing permission changes to a hard-linked file",
        ));
    }
    let mode = meta.mode() & 0o7777 & !0o007;
    if unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if file.metadata()?.mode() & 7 != 0 {
        return Err(std::io::Error::other("other-permission readback differs"));
    }
    Ok(())
}

#[cfg(unix)]
fn protect_private_gitdir(private: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    use std::os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    };
    // Sticky + a parent-owned sentinel prevents unlink/rename as well as direct
    // writes. Merely chmod-ing config.worktree read-only would be insufficient.
    let config = private.join("config.worktree");
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&config)?;
    if f.metadata()?.nlink() != 1
        || f.metadata()?.uid() == uid && uid != crate::sandbox::server_uid()
    {
        return Err(std::io::Error::other(
            "untrusted config.worktree owner or links",
        ));
    }
    update_metadata_acl(&f, gid, Some(4), false, None)?;
    walk_tree(private, false, &|_| {}, &|file, path| {
        restrict_others(file)?;
        if path == config
            || path.parent() == Some(private)
                && matches!(
                    path.file_name().and_then(|n| n.to_str()),
                    Some("commondir" | "gitdir")
                )
        {
            return update_metadata_acl(file, gid, Some(4), false, None);
        }
        grant_metadata_acl(file, gid)?;
        let meta = file.metadata()?;
        if path == private {
            if meta.uid() == uid && uid != crate::sandbox::server_uid() {
                return Err(std::io::Error::other(
                    "private gitdir must remain parent-owned",
                ));
            }
            let sticky_mode = ((meta.mode() & 0o7777) | 0o1000) as libc::mode_t;
            if unsafe { libc::fchmod(file.as_raw_fd(), sticky_mode) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if file.metadata()?.mode() & 0o1000 == 0 {
                return Err(std::io::Error::other("sticky readback differs"));
            }
        } else if path.parent() == Some(private)
            && !meta.is_dir()
            && !matches!(
                path.file_name().and_then(|n| n.to_str()),
                Some("commondir" | "gitdir")
            )
        {
            // Git replaces HEAD/index through lock+rename. They must belong to
            // B in a sticky parent; A retains its additive named-user ACL.
            if meta.uid() != uid && unsafe { libc::fchown(file.as_raw_fd(), uid, u32::MAX) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if file.metadata()?.uid() != uid {
                return Err(std::io::Error::other(
                    "private metadata owner readback differs",
                ));
            }
        }
        Ok(())
    })
}

#[cfg(unix)]
fn share_tree(path: &Path, gid: u32) -> std::io::Result<()> {
    share_tree_with_hook(path, gid, false, &|_| {})
}

#[cfg(unix)]
fn share_tree_with_hook(
    path: &Path,
    gid: u32,
    preserve_group_grants: bool,
    before_open: &dyn Fn(&Path),
) -> std::io::Result<()> {
    walk_tree(path, !preserve_group_grants, before_open, &|file, path| {
        if preserve_group_grants {
            return grant_metadata_acl(file, gid);
        }
        use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
        let meta = file.metadata()?;
        let fd = file.as_raw_fd();
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
        if meta.gid() != gid && unsafe { libc::fchown(fd, u32::MAX, gid) } != 0 {
            return Err(std::io::Error::other(format!(
                "{}: chgrp to {gid}: {}",
                path.display(),
                std::io::Error::last_os_error()
            )));
        }
        if mode != desired && unsafe { libc::fchmod(fd, desired as libc::mode_t) } != 0 {
            return Err(std::io::Error::last_os_error());
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
    walk_tree_depth(path, skip_git, before_open, apply, true)
}

#[cfg(unix)]
fn walk_tree_depth(
    path: &Path,
    skip_git: bool,
    before_open: &dyn Fn(&Path),
    apply: &dyn Fn(&std::fs::File, &Path) -> std::io::Result<()>,
    descend: bool,
) -> std::io::Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd};
    let canonical = std::fs::canonicalize(path)?;
    let mut file = std::fs::File::open("/")?;
    for component in canonical.components() {
        if let std::path::Component::Normal(name) = component {
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
    }
    fn visit(
        file: &std::fs::File,
        path: &Path,
        skip_git: bool,
        before_open: &dyn Fn(&Path),
        apply: &dyn Fn(&std::fs::File, &Path) -> std::io::Result<()>,
        descend: bool,
    ) -> std::io::Result<()> {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        let fd = file.as_raw_fd();
        apply(file, path)?;
        if !meta.is_dir() || !descend {
            return Ok(());
        }
        // fdopendir owns only the duplicate; closing it does not close file.
        let raw = unsafe { libc::dup(fd) };
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
            if kind == libc::S_IFLNK {
                continue;
            }
            if skip_git && name.to_bytes() == b".git" {
                // The common store gets additive ACLs, never a group replacement.
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
            let child = unsafe { libc::openat(fd, name.as_ptr(), flags) };
            if child < 0 {
                return Err(std::io::Error::last_os_error());
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
            visit(&child, &child_path, skip_git, before_open, apply, descend)?;
        }
        Ok(())
    }
    visit(&file, path, skip_git, before_open, apply, descend)
}

#[cfg(not(unix))]
fn share_tree(_path: &Path, _gid: u32) -> std::io::Result<()> {
    Ok(())
}

// Linux POSIX ACL xattrs are a versioned sequence of (tag, permissions, id).
// The descriptor-based API keeps every mutation bound to the opened object.
#[cfg(target_os = "linux")]
fn grant_metadata_acl(file: &std::fs::File, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata()?;
    update_metadata_acl(
        file,
        gid,
        Some(if meta.is_dir() || meta.mode() & 0o111 != 0 {
            7
        } else {
            6
        }),
        meta.is_dir(),
        None,
    )
}

#[cfg(target_os = "linux")]
fn update_metadata_acl(
    file: &std::fs::File,
    gid: u32,
    permissions: Option<u16>,
    inherit: bool,
    revoke_uid: Option<u32>,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    // Serialize read/modify/write ACL grants across simultaneous spawns.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    struct Unlock(i32);
    impl Drop for Unlock {
        fn drop(&mut self) {
            unsafe { libc::flock(self.0, libc::LOCK_UN) };
        }
    }
    let _unlock = Unlock(file.as_raw_fd());
    let meta = file.metadata()?;
    if permissions.is_some() && !meta.is_dir() && meta.nlink() > 1 {
        return Err(std::io::Error::other(
            "refusing ACL changes to a hard-linked file",
        ));
    }
    let mode = meta.mode();
    let base = vec![
        (1u16, ((mode >> 6) & 7) as u16, u32::MAX),
        (4, ((mode >> 3) & 7) as u16, u32::MAX),
        (32, (mode & 7) as u16, u32::MAX),
    ];
    let access_name = c"system.posix_acl_access";
    let default_name = c"system.posix_acl_default";
    fn read(
        file: &std::fs::File,
        name: &std::ffi::CStr,
    ) -> std::io::Result<Option<Vec<(u16, u16, u32)>>> {
        use std::os::fd::AsRawFd;
        let mut bytes = vec![0u8; 65536];
        let len = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                name.as_ptr(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
            )
        };
        if len < 0 {
            let err = std::io::Error::last_os_error();
            return if err.raw_os_error() == Some(libc::ENODATA) {
                Ok(None)
            } else {
                Err(err)
            };
        }
        bytes.truncate(len as usize);
        if bytes.len() < 4
            || !(bytes.len() - 4).is_multiple_of(8)
            || bytes[..4] != 2u32.to_le_bytes()
        {
            return Err(std::io::Error::other("invalid POSIX ACL xattr"));
        }
        Ok(Some(
            bytes[4..]
                .as_chunks::<8>()
                .0
                .iter()
                .map(|b| {
                    (
                        u16::from_le_bytes([b[0], b[1]]),
                        u16::from_le_bytes([b[2], b[3]]),
                        u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
                    )
                })
                .collect(),
        ))
    }
    let old_access = read(file, access_name)?;
    let old_default = if meta.is_dir() {
        read(file, default_name)?
    } else {
        None
    };
    if permissions.is_none()
        && !old_access
            .iter()
            .chain(old_default.iter())
            .flatten()
            .any(|e| e.0 == 8 && e.2 == gid || e.0 == 2 && Some(e.2) == revoke_uid)
    {
        return Ok(());
    }
    if !meta.is_dir() && meta.nlink() > 1 {
        return Err(std::io::Error::other(
            "refusing ACL changes to a hard-linked file",
        ));
    }
    let access = old_access.unwrap_or(base);
    let mut acls = vec![(access_name, access.clone())];
    if meta.is_dir() {
        if inherit {
            acls.push((default_name, read(file, default_name)?.unwrap_or(access)));
        } else if let Some(existing) = read(file, default_name)? {
            acls.push((default_name, existing));
        }
    }
    for (name, mut entries) in acls {
        let old_mask = entries.iter().find(|e| e.0 == 16).map(|e| e.1).unwrap_or(7);
        // Preserve effective existing grants, including entries restricted by
        // an old mask, instead of accidentally unmasking them for another seat.
        for entry in &mut entries {
            if matches!(entry.0, 2 | 4 | 8) {
                entry.1 &= old_mask;
            }
        }
        entries.retain(|e| !(e.0 == 8 && e.2 == gid || e.0 == 2 && Some(e.2) == revoke_uid));
        if let Some(desired) = permissions.filter(|_| name == access_name || inherit) {
            entries.push((8, desired, gid));
            // On B-created descendants, A retains its previous effective access.
            let owner_permissions = entries.iter().find(|e| e.0 == 1).map(|e| e.1).unwrap_or(0);
            if let Some(entry) = entries.iter_mut().find(|e| e.0 == 2 && e.2 == meta.uid()) {
                entry.1 |= owner_permissions;
            } else {
                entries.push((2, owner_permissions, meta.uid()));
            }
        }
        entries.retain(|e| e.0 != 16);
        let mask = entries
            .iter()
            .filter(|e| matches!(e.0, 2 | 4 | 8))
            .fold(0, |m, e| m | e.1);
        entries.push((16, mask, u32::MAX));
        entries.sort_unstable_by_key(|e| (e.0, e.2));
        let mut bytes = 2u32.to_le_bytes().to_vec();
        for (tag, perm, id) in &entries {
            bytes.extend(tag.to_le_bytes());
            bytes.extend(perm.to_le_bytes());
            bytes.extend(id.to_le_bytes());
        }
        if unsafe {
            libc::fsetxattr(
                file.as_raw_fd(),
                name.as_ptr(),
                bytes.as_ptr().cast(),
                bytes.len(),
                0,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error());
        }
        if read(file, name)?.as_ref() != Some(&entries) {
            return Err(std::io::Error::other("POSIX ACL readback differs"));
        }
    }
    let after = file.metadata()?;
    if after.uid() != meta.uid() || after.gid() != meta.gid() {
        return Err(std::io::Error::other(
            "Git metadata ownership changed during ACL grant",
        ));
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn grant_metadata_acl(file: &std::fs::File, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata()?;
    // Same-principal non-Linux hosts need no new cross-seat grant.
    let same_principal =
        meta.uid() == unsafe { libc::geteuid() } && gid == unsafe { libc::getegid() };
    if (meta.gid() == gid && meta.mode() & 0o070 == 0o070) || same_principal {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "cross-seat Git metadata requires Linux POSIX ACL support",
        ))
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn update_metadata_acl(
    file: &std::fs::File,
    gid: u32,
    permissions: Option<u16>,
    _inherit: bool,
    _revoke_uid: Option<u32>,
) -> std::io::Result<()> {
    if permissions.is_none() {
        Ok(())
    } else {
        grant_metadata_acl(file, gid)
    }
}

/// Run admission as the seat that will execute the agent, not as its parent.
/// Unique probes verify lock-directory writes without touching an active index.
pub(crate) fn verify(path: &Path, uid: u32, gid: u32, launch: Option<&str>) -> Result<(), Error> {
    let mut command = std::process::Command::new("/bin/sh");
    #[cfg(unix)]
    if unsafe { libc::geteuid() } != uid || unsafe { libc::getegid() } != gid {
        if let Some(spec) = launch {
            let parts: Vec<_> = spec.split('\t').collect();
            if parts.len() != 4 {
                return Err(refusal(path, "invalid seat launcher"));
            }
            command = std::process::Command::new(parts[0]);
            command.args([
                "--reuid",
                parts[1],
                "--regid",
                parts[2],
                "--groups",
                parts[3],
                "--no-new-privs",
                "--",
                "/bin/sh",
            ]);
        } else {
            use std::os::unix::process::CommandExt;
            // Clear supplementary grants before changing uid. These changes
            // must all run in pre_exec: CommandExt::uid would drop privilege
            // before a pre_exec setgroups call.
            unsafe {
                command.pre_exec(move || {
                    if libc::setgroups(1, &gid) != 0
                        || libc::setgid(gid) != 0
                        || libc::setuid(uid) != 0
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
    }
    let script = r#"
set -eu
check_tree() {
  find "$1" -path "$1/.git" -prune -o \( -type f -o -type d \) -exec sh -c '
    for p do
      if [ ! -w "$p" ] || { [ -d "$p" ] && [ ! -x "$p" ]; }; then
        printf "%s: seat cannot write/traverse\n" "$p" >&2; exit 1
      fi
    done
  ' sh {} +
}
probe() {
  p="$1/.anyagents-write-check-$$"
  (set -C; : > "$p") || { printf "%s: seat cannot create a lock\n" "$1" >&2; exit 1; }
  rm -- "$p"
}
check_tree "$1"
probe "$1"
if [ -e "$1/.git" ] || [ -L "$1/.git" ]; then
  private=$(git -c safe.directory="$1" -C "$1" rev-parse --path-format=absolute --git-dir)
  common=$(git -c safe.directory="$1" -C "$1" rev-parse --path-format=absolute --git-common-dir)
  for d in "$private" "$common/objects" "$common/refs"; do
    probe "$d"
  done
  git -c safe.directory="$1" -C "$1" status --porcelain >/dev/null
fi
"#;
    // Vendor fixtures and deployments may intentionally narrow PATH. Keep their
    // overrides first, then include the OS utilities used by this admission.
    let mut paths: Vec<_> = std::env::var_os("PATH")
        .map(|value| std::env::split_paths(&value).collect())
        .unwrap_or_default();
    paths.extend([PathBuf::from("/usr/bin"), PathBuf::from("/bin")]);
    let path_env = std::env::join_paths(paths).map_err(|e| refusal(path, e))?;
    let out = command
        .env("PATH", path_env)
        .env("HOME", "/nonexistent")
        .env("XDG_CONFIG_HOME", "/nonexistent")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .args(["-c", script, "workdir-admission"])
        .arg(path)
        .output()
        .map_err(|e| refusal(path, e))?;
    if !out.status.success() {
        return Err(refusal(
            path,
            format!(
                "seat uid {uid}, gid {gid}: {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    #[test]
    fn private_identity_preserves_common_config_and_requires_extension() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("SylphxAI/demo", &target, "child", None)
            .unwrap();
        let common_config = f.source.join(".git/config");
        let before = std::fs::read(&common_config).unwrap();
        let inode = std::fs::metadata(&common_config).unwrap().ino();
        set_worktree_identity(&target).unwrap();
        assert_eq!(std::fs::read(&common_config).unwrap(), before);
        assert_eq!(std::fs::metadata(&common_config).unwrap().ino(), inode);
        for (key, expected) in [("user.name", "Kyle Tse"), ("user.email", "hi@kylet.se")] {
            assert_eq!(
                crate::worktree::tests::git(&target, &["config", "--worktree", "--get", key]),
                expected
            );
        }
        crate::worktree::tests::git(
            &target,
            &["config", "--local", "extensions.worktreeConfig", "false"],
        );
        let before = std::fs::read(&common_config).unwrap();
        let err = set_worktree_identity(&target).unwrap_err();
        assert!(
            err.to_string().contains("extensions.worktreeConfig"),
            "{err}"
        );
        assert_eq!(std::fs::read(common_config).unwrap(), before);
    }

    #[test]
    fn recorded_status_ignores_planted_gitdir_and_rejects_untrusted_config() {
        use crate::worktree::tests::git;
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("SylphxAI/demo", &target, "child", None)
            .unwrap();
        set_worktree_identity(&target).unwrap();
        let id = anyagents_core::AgentId::generate();
        share(
            &target,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
            &grant_path(dir.path(), &id),
        )
        .unwrap();
        let recorded = RecordedWorktree::load(dir.path(), &id, &target).unwrap();
        let status = || {
            recorded
                .command()
                .unwrap()
                .args(["status", "--porcelain"])
                .output()
                .unwrap()
        };
        let baseline = status();
        assert!(
            baseline.status.success(),
            "{}",
            String::from_utf8_lossy(&baseline.stderr)
        );
        assert!(baseline.stdout.is_empty());
        let planted = dir.path().join("planted");
        std::fs::create_dir(&planted).unwrap();
        git(&planted, &["init", "-b", "main"]);
        std::fs::write(planted.join("payload"), "included\n").unwrap();
        std::fs::write(planted.join(".gitattributes"), "payload filter=x\n").unwrap();
        git(&planted, &["add", "."]);
        git(
            &planted,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-m",
                "Synthetic baseline",
            ],
        );
        let marker = dir.path().join("filter-ran");
        git(
            &planted,
            &[
                "config",
                "filter.x.clean",
                &format!("printf hit >> '{}'; cat", marker.display()),
            ],
        );
        git(
            &planted,
            &["config", "core.worktree", target.to_str().unwrap()],
        );
        std::fs::write(
            target.join(".git"),
            format!("gitdir: {}\n", planted.join(".git").display()),
        )
        .unwrap();
        std::fs::write(target.join("payload"), "changed!\n").unwrap();
        std::fs::write(target.join(".gitattributes"), "payload filter=x\n").unwrap();
        git(&target, &["status", "--porcelain"]);
        assert!(marker.exists(), "payload positive control did not execute");
        std::fs::remove_file(&marker).unwrap();
        let safe = status();
        assert!(
            safe.status.success(),
            "{}",
            String::from_utf8_lossy(&safe.stderr)
        );
        assert!(!safe.stdout.is_empty());
        assert!(!marker.exists(), "recorded status followed planted gitdir");
        let config = recorded.private().join("config.worktree");
        let previous = std::fs::read(&config).unwrap();
        std::fs::write(&config, b"[include]\npath = /nonexistent/foreign\n").unwrap();
        let err = recorded.command().expect_err("include must refuse");
        assert!(err.to_string().contains("include/includeIf"), "{err}");
        std::fs::write(&config, previous).unwrap();
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(recorded.command().is_err(), "writable config must refuse");
        assert!(!marker.exists());
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
        let common = dir.path().join("common");
        write_grant(
            &grant_path(dir.path(), &id),
            &Grant {
                common: common.clone(),
                private: common.join("worktrees/child"),
                checkout: workdir.clone(),
                uid: crate::sandbox::server_uid(),
                gid: crate::sandbox::server_gid(),
                active: true,
            },
        )
        .unwrap();
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
            let expected = if root == &workdir {
                format!("*\n\n{}\n{}\n", root.display(), common.display())
            } else {
                format!("*\n\n{}\n", root.display())
            };
            assert_eq!(read("safe.directory"), expected);
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
    fn sharing_covers_worktree_gitdir_and_common_objects_and_refs() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("SylphxAI/demo", &target, "child", None)
            .unwrap();
        share(
            &target,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
            &dir.path().join("grant.json"),
        )
        .unwrap();
        let meta = std::fs::metadata(&target).unwrap();
        assert_eq!(meta.gid(), crate::sandbox::server_gid());
        assert_eq!(meta.permissions().mode() & 0o2070, 0o2070);
        #[cfg(target_os = "linux")]
        {
            let common = git_dirs(&target).unwrap().pop().unwrap();
            for path in [common.join("objects"), common.join("refs")] {
                assert_ne!(std::fs::metadata(path).unwrap().mode() & 0o020, 0);
            }
        }
        verify(
            &target,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
            None,
        )
        .unwrap();
        crate::worktree::tests::git(
            &target,
            &["commit", "--allow-empty", "-m", "Writable child"],
        );
        crate::worktree::tests::git(&target, &["status", "--porcelain"]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn metadata_acl_is_additive_preserves_masks_and_inherits_both_seats() {
        use std::os::fd::AsRawFd;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("metadata");
        std::fs::create_dir(&root).unwrap();
        let before = std::fs::metadata(&root).unwrap();
        let file = std::fs::File::open(&root).unwrap();
        let old_gid = 123450;
        let new_gid = 123451;
        let mut acl = 2u32.to_le_bytes().to_vec();
        for (tag, perms, id) in [
            (1u16, 7u16, u32::MAX),
            (4, 7, u32::MAX),
            (8, 7, old_gid),
            (16, 5, u32::MAX),
            (32, 0, u32::MAX),
        ] {
            acl.extend(tag.to_le_bytes());
            acl.extend(perms.to_le_bytes());
            acl.extend(id.to_le_bytes());
        }
        assert_eq!(
            unsafe {
                libc::fsetxattr(
                    file.as_raw_fd(),
                    c"system.posix_acl_access".as_ptr(),
                    acl.as_ptr().cast(),
                    acl.len(),
                    0,
                )
            },
            0
        );
        grant_metadata_acl(&file, new_gid).unwrap();
        fn entries(path: &Path, name: &std::ffi::CStr) -> Vec<(u16, u16, u32)> {
            let file = std::fs::File::open(path).unwrap();
            let mut bytes = [0u8; 1024];
            let n = unsafe {
                libc::fgetxattr(
                    file.as_raw_fd(),
                    name.as_ptr(),
                    bytes.as_mut_ptr().cast(),
                    bytes.len(),
                )
            };
            assert!(n > 4, "{}", std::io::Error::last_os_error());
            bytes[4..n as usize]
                .as_chunks::<8>()
                .0
                .iter()
                .map(|b| {
                    (
                        u16::from_le_bytes(b[..2].try_into().unwrap()),
                        u16::from_le_bytes(b[2..4].try_into().unwrap()),
                        u32::from_le_bytes(b[4..].try_into().unwrap()),
                    )
                })
                .collect()
        }
        let after = std::fs::metadata(&root).unwrap();
        assert_eq!((after.uid(), after.gid()), (before.uid(), before.gid()));
        for name in [c"system.posix_acl_access", c"system.posix_acl_default"] {
            let acl = entries(&root, name);
            assert!(acl.contains(&(4, 5, u32::MAX)), "{acl:?}");
            assert!(acl.contains(&(8, 5, old_gid)), "{acl:?}");
            assert!(acl.contains(&(8, 7, new_gid)), "{acl:?}");
            assert!(acl.contains(&(2, 7, before.uid())), "{acl:?}");
        }
        let inherited = root.join("new-objects");
        std::fs::create_dir(&inherited).unwrap();
        let acl = entries(&inherited, c"system.posix_acl_access");
        assert!(acl.contains(&(8, 5, old_gid)), "{acl:?}");
        assert!(acl.contains(&(8, 7, new_gid)), "{acl:?}");
        assert!(acl.contains(&(2, 7, before.uid())), "{acl:?}");
        // A later third grant keeps B as well as A instead of replacing B.
        grant_metadata_acl(&file, new_gid + 1).unwrap();
        let acl = entries(&root, c"system.posix_acl_default");
        assert!(acl.contains(&(8, 5, old_gid)), "{acl:?}");
        assert!(acl.contains(&(8, 7, new_gid)), "{acl:?}");
        assert!(acl.contains(&(8, 7, new_gid + 1)), "{acl:?}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn journalled_allowlist_has_no_ancestor_defaults_and_revokes_every_entry() {
        use std::os::fd::AsRawFd;
        fn acl(path: &Path, name: &std::ffi::CStr) -> Vec<(u16, u16, u32)> {
            let file = std::fs::File::open(path).unwrap();
            let mut bytes = [0u8; 65536];
            let len = unsafe {
                libc::fgetxattr(
                    file.as_raw_fd(),
                    name.as_ptr(),
                    bytes.as_mut_ptr().cast(),
                    bytes.len(),
                )
            };
            if len < 0 {
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::ENODATA)
                );
                return Vec::new();
            }
            bytes[4..len as usize]
                .as_chunks::<8>()
                .0
                .iter()
                .map(|b| {
                    (
                        u16::from_le_bytes(b[..2].try_into().unwrap()),
                        u16::from_le_bytes(b[2..4].try_into().unwrap()),
                        u32::from_le_bytes(b[4..].try_into().unwrap()),
                    )
                })
                .collect()
        }
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("SylphxAI/demo", &target, "child", None)
            .unwrap();
        let dirs = git_dirs(&target).unwrap();
        let private = &dirs[0];
        let common = dirs.last().unwrap();
        let uid = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid().wrapping_add(100000);
        let id = anyagents_core::AgentId::generate();
        let journal = grant_path(dir.path(), &id);
        share_known(&target, private, common, uid, gid, &journal).unwrap();
        for root in [
            common.clone(),
            common.join("worktrees"),
            common.join("logs"),
        ] {
            assert!(acl(&root, c"system.posix_acl_default").is_empty());
            assert!(acl(&root, c"system.posix_acl_access").contains(&(8, 1, gid)));
        }
        for config in [
            common.join("config"),
            private.join("config.worktree"),
            private.join("commondir"),
        ] {
            assert!(
                acl(&config, c"system.posix_acl_access").contains(&(8, 4, gid)),
                "{}",
                config.display()
            );
        }
        assert!(
            !acl(&common.join("hooks/pre-commit"), c"system.posix_acl_access")
                .iter()
                .any(|e| e.0 == 8 && e.2 == gid)
        );
        assert!(
            !acl(&common.join("logs/HEAD"), c"system.posix_acl_access")
                .iter()
                .any(|e| e.0 == 8 && e.2 == gid)
        );
        assert!(acl(&common.join("logs/refs"), c"system.posix_acl_access").contains(&(8, 7, gid)));
        let sibling = dir.path().join("sibling");
        f.consumer
            .remote("SylphxAI/demo", &sibling, "sibling", None)
            .unwrap();
        let sibling_dirs = git_dirs(&sibling).unwrap();
        share_known(
            &sibling,
            &sibling_dirs[0],
            common,
            uid,
            gid + 1,
            &grant_path(dir.path(), &anyagents_core::AgentId::generate()),
        )
        .unwrap();
        assert!(
            !acl(&sibling_dirs[0], c"system.posix_acl_access")
                .iter()
                .any(|e| e.0 == 8 && e.2 == gid)
        );
        revoke_recorded(dir.path(), &id).unwrap();
        walk_tree(common, false, &|_| {}, &|_, path| {
            for name in [c"system.posix_acl_access", c"system.posix_acl_default"] {
                assert!(
                    !acl(path, name).iter().any(|e| e.0 == 8 && e.2 == gid),
                    "{}",
                    path.display()
                );
            }
            Ok(())
        })
        .unwrap();
        restore_recorded(dir.path(), &id).unwrap();
        assert!(acl(private, c"system.posix_acl_access").contains(&(8, 7, gid)));
        revoke_recorded(dir.path(), &id).unwrap();
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn same_seat_journals_preserve_survivor_and_revoke_last_binding() {
        use std::os::fd::AsRawFd;
        let group_permission = |path: &Path, gid: u32| {
            let file = std::fs::File::open(path).unwrap();
            let mut bytes = [0u8; 65536];
            let len = unsafe {
                libc::fgetxattr(
                    file.as_raw_fd(),
                    c"system.posix_acl_access".as_ptr(),
                    bytes.as_mut_ptr().cast(),
                    bytes.len(),
                )
            };
            if len < 0 {
                return None;
            }
            bytes[4..len as usize]
                .as_chunks::<8>()
                .0
                .iter()
                .find_map(|b| {
                    (u16::from_le_bytes(b[..2].try_into().unwrap()) == 8
                        && u32::from_le_bytes(b[4..].try_into().unwrap()) == gid)
                        .then(|| u16::from_le_bytes(b[2..4].try_into().unwrap()))
                })
        };
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let uid = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid().wrapping_add(100000);
        let mut records = Vec::new();
        for name in ["b1", "b2"] {
            let target = dir.path().join(name);
            f.consumer
                .remote("SylphxAI/demo", &target, name, None)
                .unwrap();
            let dirs = git_dirs(&target).unwrap();
            let id = anyagents_core::AgentId::generate();
            share_known(
                &target,
                &dirs[0],
                dirs.last().unwrap(),
                uid,
                gid,
                &grant_path(dir.path(), &id),
            )
            .unwrap();
            records.push((id, dirs));
        }
        let (b1, first) = &records[0];
        let (b2, second) = &records[1];
        let objects = first.last().unwrap().join("objects");
        assert_eq!(group_permission(&first[0], gid), Some(7));
        assert_eq!(group_permission(&second[0], gid), Some(7));
        let config = first.last().unwrap().join("config");
        assert_eq!(group_permission(&config, gid), Some(4));
        revoke_recorded(dir.path(), b1).unwrap();
        assert_eq!(group_permission(&first[0], gid), None);
        assert_eq!(group_permission(&second[0], gid), Some(7));
        assert_eq!(group_permission(&objects, gid), Some(7));
        assert_eq!(group_permission(&config, gid), Some(4));
        revoke_recorded(dir.path(), b2).unwrap();
        assert_eq!(group_permission(&second[0], gid), None);
        assert_eq!(group_permission(&objects, gid), None);
        assert_eq!(group_permission(&config, gid), None);
    }

    #[test]
    fn shared_config_refuses_includes_without_following_them() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("SylphxAI/demo", &target, "child", None)
            .unwrap();
        let dirs = git_dirs(&target).unwrap();
        let common = dirs.last().unwrap();
        let config = common.join("config");
        let original = std::fs::read(&config).unwrap();
        validate_git_config(&target).unwrap();
        for header in ["include", "includeIf \"gitdir:**\""] {
            let mut bytes = original.clone();
            bytes.extend_from_slice(
                format!("\n[{header}]\npath = /nonexistent/child-controlled\n").as_bytes(),
            );
            std::fs::write(&config, bytes).unwrap();
            let err = validate_git_config(&target).unwrap_err();
            assert!(err.to_string().contains("include/includeIf"), "{err}");
        }
        std::fs::write(&config, original).unwrap();
        std::fs::write(
            dirs[0].join("config.worktree"),
            "[include]\npath = /nonexistent/child-controlled\n",
        )
        .unwrap();
        let err = validate_git_config(&target).unwrap_err();
        assert!(err.to_string().contains("include/includeIf"), "{err}");
    }

    #[test]
    fn admission_refuses_broken_or_unwritable_git_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let uid = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid();
        std::os::unix::fs::symlink("missing-gitdir", dir.path().join(".git")).unwrap();
        let err = verify(dir.path(), uid, gid, None).unwrap_err();
        assert!(matches!(err, Error::WorkdirPermission { .. }), "{err}");
        std::fs::remove_file(dir.path().join(".git")).unwrap();
        crate::worktree::tests::git(dir.path(), &["init", "-b", "main"]);
        let objects = dir.path().join(".git/objects");
        std::fs::set_permissions(&objects, std::fs::Permissions::from_mode(0o555)).unwrap();
        if uid != 0 {
            let err = verify(dir.path(), uid, gid, None).unwrap_err();
            assert!(matches!(err, Error::WorkdirPermission { .. }), "{err}");
            assert!(err.to_string().contains(objects.to_str().unwrap()), "{err}");
        }
        std::fs::set_permissions(&objects, std::fs::Permissions::from_mode(0o755)).unwrap();
        verify(dir.path(), uid, gid, None).unwrap();
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
        share(
            dir.path(),
            crate::sandbox::server_uid(),
            gid,
            &dir.path().join("grant.json"),
        )
        .unwrap();
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
        let err = share(
            dir.path(),
            crate::sandbox::server_uid(),
            u32::MAX - 1,
            &dir.path().join("grant.json"),
        )
        .unwrap_err();
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
        let err = share_tree_with_hook(&tree, crate::sandbox::server_gid(), false, &|path| {
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
        share(
            &tree,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
            &dir.path().join("grant.json"),
        )
        .unwrap();
        assert_eq!(std::fs::metadata(&outside).unwrap().mode() & 0o777, 0o600);
        std::fs::hard_link(&outside, tree.join("hardlink")).unwrap();
        let err = share(
            &tree,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
            &dir.path().join("grant.json"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("hard-linked"), "{err}");
        assert_eq!(std::fs::metadata(&outside).unwrap().mode() & 0o777, 0o600);
    }
}
