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
        // Per-seat repositories carry their own identity: the pool never
        // touches them, so no consumer extension is required. Record the
        // worktree-config extension and the identity in the seat's own
        // config.worktree.
        validate_config_files(private, common)?;
        let config = common.join("config");
        let extension = std::process::Command::new("git")
            .args(["config", "--no-includes", "--file"])
            .arg(&config)
            .args(["extensions.worktreeConfig", "true"])
            .output()?;
        if !extension.status.success() {
            return Err(refusal(path, "cannot enable extensions.worktreeConfig"));
        }
    } else {
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
    }
    // No agent has been admitted yet. This parent-controlled private file will
    // receive the read-only ACL/sticky protection before the seat can run.
    let config = private.join("config.worktree");
    if config.symlink_metadata().is_ok_and(|m| !m.is_file()) {
        return Err(refusal(&config, "worktree config is not a regular file"));
    }
    let name = std::env::var("ANYAGENTS_GIT_AUTHOR_NAME").ok();
    let email = std::env::var("ANYAGENTS_GIT_AUTHOR_EMAIL").ok();
    if name.is_some() != email.is_some() {
        return Err(refusal(path, "configure both Git author name and email"));
    }
    for (key, value) in [("user.name", name), ("user.email", email)] {
        let Some(value) = value else {
            continue;
        };
        if value.trim().is_empty() {
            return Err(refusal(path, "Git author identity cannot be empty"));
        }
        let out = std::process::Command::new("git")
            .args(["config", "--no-includes", "--file"])
            .arg(&config)
            .args([key, &value])
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
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    // A directory shared as both private and common is a per-seat
    // repository: git has no separate worktree storage to distinguish.
    let mut paths = vec![common.join("config")];
    if private != common {
        paths.push(common.join("config.worktree"));
        paths.push(private.join("config.worktree"));
    } else {
        paths.push(common.join("config.worktree"));
    }
    for path in paths {
        // Per-seat config.worktree is opened with O_NOFOLLOW directly,
        // never through the managed symlink; only the legacy common config
        // goes through the pinned managed open. A managed config is a
        // symlink even when git reports one directory, so route any
        // symlinked config through the pinned open. A per-seat config is
        // seat-writable by design, so its writes stay allowed and its
        // contents are refused below instead of its writability.
        let is_symlink =
            std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_symlink());
        let pinned = path == common.join("config") && (private != common || is_symlink);
        let per_seat_config = path == common.join("config") && private == common && !is_symlink;
        let mut file = match if pinned {
            open_common_config(common, None)
        } else {
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)
        } {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(refusal(&path, e)),
        };
        let meta = file.metadata()?;
        if !meta.is_file() || per_seat_config && meta.nlink() != 1 || meta.len() > 1024 * 1024 {
            return Err(refusal(&path, "Git configuration is not a regular file"));
        }
        use std::io::Read;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|e| refusal(&path, e))?;
        // --file - and --no-includes inspect only the opened input. Never
        // follow the include or print config values in a diagnostic.
        use std::io::Write;
        let run_check = |args: &[&str]| -> Result<std::process::Output, Error> {
            let mut child = std::process::Command::new("git")
                .args([
                    "-c",
                    "core.hooksPath=/dev/null",
                    "-c",
                    "core.fsmonitor=false",
                    "config",
                    "--no-includes",
                    "--file",
                    "-",
                ])
                .args(args)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| refusal(&path, e))?;
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(&bytes);
            }
            child.wait_with_output().map_err(|e| refusal(&path, e))
        };
        let inspect = run_check(&[
            "--name-only",
            "--get-regexp",
            "^(include\\.path|includeif\\..*\\.path)$",
        ])?;
        match inspect.status.code() {
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
        // A seat-writable config can name an executable fsmonitor. The
        // server reads this config as its own uid, so refuse the key
        // outright: with or without includes, fsmonitor is never inert
        // once git runs status or diff. Every server-side git invocation
        // passes core.fsmonitor=false regardless.
        let fsmonitor = run_check(&["--get", "core.fsmonitor"])?;
        match fsmonitor.status.code() {
            Some(1) => {}
            Some(0) => {
                let value = String::from_utf8_lossy(&fsmonitor.stdout);
                if value.trim() != "false" && !value.trim().is_empty() {
                    return Err(refusal(
                        &path,
                        "core.fsmonitor is refused for server-read Git configuration",
                    ));
                }
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
    // Harness fixtures can deliberately omit system tools from PATH. Preserve
    // their configured precedence, then include the OS Git used for parsing.
    let mut paths: Vec<_> = std::env::var_os("PATH")
        .map(|value| std::env::split_paths(&value).collect())
        .unwrap_or_default();
    paths.extend([PathBuf::from("/usr/bin"), PathBuf::from("/bin")]);
    let path_env = std::env::join_paths(paths).map_err(|e| refusal(config, e))?;
    let staging = data.join("worktree-grants");
    std::fs::create_dir_all(&staging)?;
    let staging = std::fs::canonicalize(staging)?;
    // Even `git config --file` discovers the caller's repository first. The
    // daemon may not be able to read that checkout under its reduced identity;
    // an unrelated malformed config must not govern this file-only operation.
    let git = || {
        let mut command = std::process::Command::new("git");
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

/// The journal lives under the server-owned data directory, outside the seat's
/// config/worktree. Persist it before granting anything so partial admission can
/// always revoke, even if the checkout or its .git pointer later disappears.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Grant {
    common: PathBuf,
    gid: u32,
    uid: u32,
    private: PathBuf,
    checkout: PathBuf,
    active: bool,
    /// Shared object-pool repository for per-seat repositories. `None` keeps
    /// the legacy linked-worktree admission path for checkouts provisioned
    /// before this field existed; nothing converts them automatically.
    #[serde(default)]
    pool: Option<PathBuf>,
    #[serde(default)]
    inspection: bool,
    #[serde(default)]
    inspection_paths: Option<Vec<InspectionPath>>,
    #[serde(default)]
    quarantined: bool,
    #[serde(default)]
    config: Option<ManagedConfig>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct InspectionPath {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

// Git resolves a config symlink before creating config.lock. Keep inheritance
// confined to this single-purpose directory, never the mixed common gitdir.
const CONFIG_DIRECTORY: &str = "anyagents-config";

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ManagedConfig {
    directory: InspectionPath,
    link: InspectionPath,
    // Retained during interrupted adoption: quarantine preserves both the old
    // pathname and the new hard link, including the original config bytes.
    adoption_source: Option<InspectionPath>,
}

fn identity(path: &Path) -> std::io::Result<InspectionPath> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(path)?;
    Ok(InspectionPath {
        path: path.to_path_buf(),
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

fn open_common_config(
    common: &Path,
    pinned: Option<&ManagedConfig>,
) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::MetadataExt;
    let path = common.join("config");
    let link = std::fs::symlink_metadata(&path)?;
    let owners = [
        open_nofollow(common)?.metadata()?.uid(),
        crate::sandbox::server_uid(),
    ];
    let target = if link.file_type().is_symlink() {
        let relative = Path::new(CONFIG_DIRECTORY).join("config");
        if std::fs::read_link(&path)? != relative
            || !owners.contains(&link.uid())
            || link.nlink() != 1
        {
            return Err(std::io::Error::other("untrusted managed Git config link"));
        }
        let directory = common.join(CONFIG_DIRECTORY);
        let opened = open_nofollow(&directory)?;
        let meta = opened.metadata()?;
        if !meta.is_dir()
            || !owners.contains(&meta.uid())
            || writable_file_by_untrusted(&opened, &owners)?
        {
            return Err(std::io::Error::other(
                "untrusted managed Git config directory",
            ));
        }
        if let Some(record) = pinned
            && (record.directory.path != directory
                || record.link.path != path
                || record.directory.dev != meta.dev()
                || record.directory.ino != meta.ino()
                || record.link.dev != link.dev()
                || record.link.ino != link.ino()
                || record.adoption_source.is_some())
        {
            return Err(std::io::Error::other("managed Git config identity changed"));
        }
        directory.join("config")
    } else {
        if pinned.is_some() {
            return Err(std::io::Error::other("managed Git config link replaced"));
        }
        path
    };
    let file = open_nofollow(&target)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.nlink() != 1
        || !owners.contains(&meta.uid())
        || writable_file_by_untrusted(&file, &owners)?
    {
        return Err(std::io::Error::other("untrusted Git configuration"));
    }
    Ok(file)
}

// Serialize ACL/default transitions with Git's own lock+rename as well as
// other lifecycle calls. A pre-existing writer is a bounded refusal, never a
// stale lock to remove; otherwise an in-flight old lock could resurrect access.
struct ConfigLock {
    _file: std::fs::File,
    path: PathBuf,
}

impl Drop for ConfigLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn lock_config(path: PathBuf) -> std::io::Result<ConfigLock> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)?;
    Ok(ConfigLock { _file: file, path })
}

/// Caller holds the journal lock. Adoption also takes Git's original config.lock
/// before touching the config. The old inode is hard-linked into the dedicated
/// directory, then a symlink is renamed over the original pathname atomically.
/// A durable quarantined intent precedes the hard link; interruption never loses
/// config bytes or silently admits a partly migrated checkout.
fn manage_config(grant: &mut Grant, journal: &Path, retained: &[Grant]) -> std::io::Result<()> {
    manage_config_with_hook(grant, journal, retained, &|| Ok(()))
}

fn manage_config_with_hook(
    grant: &mut Grant,
    journal: &Path,
    retained: &[Grant],
    before_publish: &dyn Fn() -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    let common = &grant.common;
    let path = common.join("config");
    let mut target_lock = None;
    if grant.config.is_none() {
        if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err(std::io::Error::other("unrecorded managed Git config"));
        }
        let _source_lock = lock_config(common.join("config.lock"))?;
        struct Remove(PathBuf);
        impl Drop for Remove {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let source = open_common_config(common, None)?;
        let meta = source.metadata()?;
        let directory = common.join(CONFIG_DIRECTORY);
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let dir = open_nofollow(&directory)?;
        clear_default_acl(&dir)?;
        target_lock = Some(lock_config(directory.join("config.lock"))?);
        dir.set_permissions(std::fs::Permissions::from_mode(0o700))?;
        descriptor_fchown(&dir, Some(meta.uid()), Some(meta.gid()))?;
        let temporary = common.join(format!(
            ".config-link-{}",
            anyagents_core::AgentId::generate()
        ));
        std::os::unix::fs::symlink(Path::new(CONFIG_DIRECTORY).join("config"), &temporary)?;
        let _remove_temporary = Remove(temporary.clone());
        grant.config = Some(ManagedConfig {
            directory: identity(&directory)?,
            link: identity(&temporary)?,
            adoption_source: Some(identity(&path)?),
        });
        grant.quarantined = true;
        write_grant(journal, grant)?;
        let current = std::fs::symlink_metadata(&path)?;
        if current.dev() != meta.dev() || current.ino() != meta.ino() || current.nlink() != 1 {
            return Err(std::io::Error::other("Git config changed during adoption"));
        }
        // Common and destination directories are protected from every reader;
        // neither path traverses a writable seat-controlled component.
        std::fs::hard_link(&path, directory.join("config"))?;
        source.sync_all()?;
        dir.sync_all()?;
        before_publish()?;
        std::fs::rename(&temporary, &path)?;
        open_nofollow(common)?.sync_all()?;
        let record = grant.config.as_mut().unwrap();
        record.link.path = path;
        record.adoption_source = None;
        grant.quarantined = false;
        write_grant(journal, grant)?;
    }
    let _target_lock = match target_lock {
        Some(lock) => lock,
        None => lock_config(common.join(CONFIG_DIRECTORY).join("config.lock"))?,
    };
    let file = open_common_config(common, grant.config.as_ref())?;
    let directory = grant.config.as_ref().unwrap().directory.open()?;
    let mut readers: Vec<_> = std::iter::once(&*grant)
        .chain(retained.iter())
        .filter(|g| g.active && !g.quarantined && !g.inspection)
        .map(|g| g.gid)
        .collect();
    // Relocation must never narrow who could already read the config: Git
    // checkouts the engine did not admit (made by an external provider) read
    // it through the common directory's own group. Read only, never write.
    readers.extend(store_group_reader(common)?);
    for gid in &readers {
        update_metadata_acl(&directory, *gid, Some(5), false, None)?;
        update_metadata_acl(&file, *gid, Some(4), false, None)?;
    }
    config_default_readers(&directory, &readers)?;
    Ok(())
}

/// The common directory's owning group, when it may list and traverse the
/// store (and so could read a config file placed there before relocation).
fn store_group_reader(common: &Path) -> std::io::Result<Option<u32>> {
    use std::os::unix::fs::MetadataExt;
    let directory = open_nofollow(common)?;
    let meta = directory.metadata()?;
    #[cfg(target_os = "linux")]
    let group_permissions = {
        let mut bytes = vec![0u8; 65536];
        let len = read_xattr(&directory, c"system.posix_acl_access", &mut bytes);
        if len >= 0 {
            bytes.truncate(len as usize);
            if bytes.len() < 4
                || bytes[..4] != 2u32.to_le_bytes()
                || !(bytes.len() - 4).is_multiple_of(8)
            {
                return Err(std::io::Error::other("invalid common directory ACL"));
            }
            let entries: Vec<_> = bytes[4..]
                .as_chunks::<8>()
                .0
                .iter()
                .map(|b| {
                    (
                        u16::from_le_bytes([b[0], b[1]]),
                        u16::from_le_bytes([b[2], b[3]]),
                    )
                })
                .collect();
            let mask = entries.iter().find(|e| e.0 == 16).map_or(7, |e| e.1);
            entries.iter().find(|e| e.0 == 4).map_or(0, |e| e.1 & mask)
        } else {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ENODATA) {
                return Err(error);
            }
            ((meta.mode() >> 3) & 7) as u16
        }
    };
    #[cfg(not(target_os = "linux"))]
    let group_permissions = ((meta.mode() >> 3) & 7) as u16;
    Ok((group_permissions & 5 == 5).then_some(meta.gid()))
}

/// Recompute the managed config's reader set without admitting anything: the
/// store group plus every still-active grant. Runs after a revocation and at
/// startup so the last revocation never leaves provider checkouts unreadable.
fn refresh_store_config_readers(journal: &Path, grant: &Grant) -> std::io::Result<()> {
    if grant.config.is_none() || grant.quarantined || grant.inspection {
        return Ok(());
    }
    // `manage_config` never adopts here: the record is already pinned.
    let mut current = grant.clone();
    let retained = other_active_grants(journal, &current)?;
    manage_config(&mut current, journal, &retained)
}

#[cfg(target_os = "linux")]
fn config_default_readers(directory: &std::fs::File, readers: &[u32]) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut groups = readers.to_vec();
    groups.sort_unstable();
    groups.dedup();
    // All future files are parent-owned rw, owning group/outsiders have no
    // access, and only recorded reader groups inherit read (never write).
    let mut entries = vec![(1u16, 6u16, u32::MAX), (4, 0, u32::MAX)];
    entries.extend(groups.into_iter().map(|gid| (8, 4, gid)));
    entries.extend([(16, 4, u32::MAX), (32, 0, u32::MAX)]);
    let mut bytes = 2u32.to_le_bytes().to_vec();
    for (tag, perm, id) in entries {
        bytes.extend(tag.to_le_bytes());
        bytes.extend(perm.to_le_bytes());
        bytes.extend(id.to_le_bytes());
    }
    let name = c"system.posix_acl_default";
    descriptor_fsetxattr(directory, name, &bytes)?;
    let mut readback = vec![0u8; bytes.len() + 1];
    let len = unsafe {
        libc::fgetxattr(
            directory.as_raw_fd(),
            name.as_ptr(),
            readback.as_mut_ptr().cast(),
            readback.len(),
        )
    };
    if len != bytes.len() as isize || readback[..bytes.len()] != bytes {
        return Err(std::io::Error::other("config default ACL readback differs"));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn config_default_readers(_directory: &std::fs::File, _readers: &[u32]) -> std::io::Result<()> {
    Ok(()) // cross-UID admission is already refused without Linux ACLs
}

pub(crate) fn grant_path(data: &Path, id: &anyagents_core::AgentId) -> PathBuf {
    data.join("worktree-grants").join(format!("{id}.json"))
}

/// The supervisor's automatic restart and explicit wakes share this refusal.
/// A quarantined checkout must not run even if its lifecycle row says running.
pub(crate) fn ensure_launchable(data: &Path, id: &anyagents_core::AgentId) -> Result<(), Error> {
    let journal = grant_path(data, id);
    let _lock = lock_journals(&journal).map_err(|error| refusal(&journal, error))?;
    let bytes = match std::fs::read(&journal) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(refusal(&journal, error)),
    };
    let grant: Grant = serde_json::from_slice(&bytes).map_err(|error| refusal(&journal, error))?;
    if grant.quarantined || grant.inspection {
        return Err(refusal(
            &journal,
            "worktree is quarantined or has incomplete ACL recovery",
        ));
    }
    if grant.active {
        RecordedWorktree {
            grant,
            journal: journal.clone(),
        }
        .refresh_config_readers()
        .map_err(|error| refusal(&journal, error))?;
    }
    Ok(())
}

/// Quarantine preserves the whole checkout, including nominal build outputs.
/// An unreadable or malformed recovery record is uncertainty, not permission
/// to delete. Only an absent journal establishes that no recovery is pending.
pub(crate) fn reaping_blocked(data: &Path, id: &anyagents_core::AgentId) -> bool {
    match std::fs::read(grant_path(data, id)) {
        Ok(bytes) => serde_json::from_slice::<Grant>(&bytes)
            .map_or(true, |grant| grant.quarantined || grant.inspection),
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
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

/// Read an extended attribute of the opened inode. `O_PATH` descriptors do not
/// support f*xattr, so they are read through their own /proc descriptor link,
/// which stays bound to the same inode and never re-resolves a pathname.
#[cfg(target_os = "linux")]
fn read_xattr(file: &std::fs::File, name: &std::ffi::CStr, buffer: &mut [u8]) -> isize {
    use std::os::fd::AsRawFd;
    if path_only(file) {
        let Ok(link) = std::ffi::CString::new(format!("/proc/self/fd/{}", file.as_raw_fd())) else {
            return -1;
        };
        return unsafe {
            libc::getxattr(
                link.as_ptr(),
                name.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
    }
    unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
        )
    }
}

/// Only an object's owner (or a fully privileged server) can change its mode
/// or ACL. Anything else is read-only evidence for the server.
#[cfg(unix)]
fn server_may_change(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    let server = crate::sandbox::server_uid();
    server == 0 || meta.uid() == server
}

// st_mode's group bits are the POSIX ACL mask, not the owning-group entry.
// A preserved named-owner rw grant can make that mask writable while every
// foreign principal remains read-only. Inspect effective entries instead.
fn writable_by_untrusted(path: &Path, owners: &[u32]) -> std::io::Result<bool> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    writable_file_by_untrusted(&file, owners)
}

fn writable_file_by_untrusted(file: &std::fs::File, owners: &[u32]) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    #[cfg(not(target_os = "linux"))]
    let _ = owners; // Named-user POSIX ACL classification below is Linux-only.
    let meta = file.metadata()?;
    #[cfg(target_os = "linux")]
    {
        let mut bytes = vec![0u8; 65536];
        let len = read_xattr(file, c"system.posix_acl_access", &mut bytes);
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

/// Launch and admission share this exact predicate: the common store and its
/// worktrees/ directory belong to the private gitdir's (non-seat) owner and no
/// other principal can write them. Admission must never accept a layout that
/// every later launch refuses. Returns the trusted metadata owner.
fn trusted_metadata_layout(common: &Path, private: &Path, seat_uid: u32) -> std::io::Result<u32> {
    use std::os::unix::fs::MetadataExt;
    if private.parent() != Some(common.join("worktrees").as_path()) {
        return Err(std::io::Error::other("unrecorded private gitdir layout"));
    }
    let owner = std::fs::symlink_metadata(private)?.uid();
    if owner == seat_uid && owner != crate::sandbox::server_uid() {
        return Err(std::io::Error::other("seat-owned private gitdir"));
    }
    let trusted = [owner, crate::sandbox::server_uid()];
    for path in [common, &common.join("worktrees"), private] {
        let meta = std::fs::symlink_metadata(path)?;
        let problem = if !meta.is_dir() {
            Some("is not a directory".to_string())
        } else if meta.uid() != owner {
            Some(format!("is owned by uid {}, not uid {owner}", meta.uid()))
        } else if std::fs::canonicalize(path)? != *path {
            Some("is not a canonical path".to_string())
        } else if path != private && writable_by_untrusted(path, &trusted)? {
            Some(format!(
                "is writable by principals other than uid {owner} (mode {:04o}{})",
                meta.mode() & 0o7777,
                if has_access_acl(path)? {
                    " plus POSIX ACL"
                } else {
                    ""
                }
            ))
        } else {
            None
        };
        if let Some(problem) = problem {
            return Err(std::io::Error::other(format!(
                "untrusted Git metadata directory {}: {problem}",
                path.display()
            )));
        }
    }
    Ok(owner)
}

#[cfg(target_os = "linux")]
fn has_access_acl(path: &Path) -> std::io::Result<bool> {
    let file = open_nofollow(path)?;
    let mut bytes = vec![0u8; 65536];
    if read_xattr(&file, c"system.posix_acl_access", &mut bytes) >= 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ENODATA) {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(not(target_os = "linux"))]
fn has_access_acl(_path: &Path) -> std::io::Result<bool> {
    Ok(false)
}

/// The checkout's .git file is seat-writable. Maintenance may only use this
/// server-owned record, including after revocation; it must never rediscover a
/// gitdir through the checkout or the seat-owned marker.
pub(crate) struct RecordedWorktree {
    grant: Grant,
    journal: PathBuf,
}

impl RecordedWorktree {
    pub(crate) fn load(data: &Path, id: &anyagents_core::AgentId, checkout: &Path) -> Option<Self> {
        let bytes = std::fs::read(grant_path(data, id)).ok()?;
        let grant: Grant = serde_json::from_slice(&bytes).ok()?;
        (grant.checkout == checkout).then_some(Self {
            grant,
            journal: grant_path(data, id),
        })
    }

    pub(crate) fn validate(&self) -> std::io::Result<()> {
        if self.grant.pool.is_some() {
            return self.validate_seat();
        }
        use std::os::unix::fs::MetadataExt;
        let g = &self.grant;
        let current: Grant = serde_json::from_slice(&std::fs::read(&self.journal)?)?;
        if current.quarantined {
            return Err(std::io::Error::other(
                "worktree is quarantined after incomplete ACL recovery",
            ));
        }
        let owner = trusted_metadata_layout(&g.common, &g.private, g.uid)?;
        open_common_config(&g.common, current.config.as_ref())?;
        for path in [
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

    /// Check a per-seat repository against its journal. The seat gitdir is
    /// exclusive to this seat and seat-writable by design, so there is no
    /// shared config to protect: the journal plus the config deny-list are
    /// the trust boundary. Everything the server reads as its own uid stays
    /// mechanically inert, and a hostile fsmonitor never validates.
    fn validate_seat(&self) -> std::io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let g = &self.grant;
        let Some(pool) = g.pool.as_ref() else {
            return Err(std::io::Error::other("per-seat grant lacks a pool"));
        };
        let current: Grant = serde_json::from_slice(&std::fs::read(&self.journal)?)?;
        if current.quarantined {
            return Err(std::io::Error::other(
                "worktree is quarantined after incomplete ACL recovery",
            ));
        }
        if current.pool.as_ref() != Some(pool) {
            return Err(std::io::Error::other("per-seat pool changed"));
        }
        if g.private != g.common || g.private != g.checkout.join(".git") {
            return Err(std::io::Error::other("unrecorded seat gitdir layout"));
        }
        let git_meta = std::fs::symlink_metadata(&g.private)?;
        if !git_meta.is_dir()
            || git_meta.file_type().is_symlink()
            || git_meta.mode() & 0o002 != 0
            || std::fs::canonicalize(&g.private)? != g.private
        {
            return Err(std::io::Error::other("untrusted seat gitdir"));
        }
        let alternates = std::fs::read_to_string(g.private.join("objects/info/alternates"))?;
        let line = alternates
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .ok_or_else(|| std::io::Error::other("seat repository lacks pool alternates"))?;
        let objects = if Path::new(line).is_absolute() {
            PathBuf::from(line)
        } else {
            g.private.join("objects").join(line)
        };
        if std::fs::canonicalize(objects)? != std::fs::canonicalize(pool.join("objects"))? {
            return Err(std::io::Error::other("seat pool alternates changed"));
        }
        if std::fs::read_to_string(pool.join("HEAD")).is_err() {
            return Err(std::io::Error::other("recorded object pool is missing"));
        }
        validate_config_files(&g.private, &g.common).map_err(std::io::Error::other)
    }

    /// Old journals are adopted once under the lifecycle lock. After adoption,
    /// Git's own lock+rename inherits reader ACLs immediately, without a wake.
    fn refresh_config_readers(&self) -> std::io::Result<()> {
        self.validate()?;
        if self.grant.pool.is_some() {
            // Per-seat configuration is exclusively the seat's; there are
            // no shared config readers to refresh.
            return Ok(());
        }
        let mut current: Grant = serde_json::from_slice(&std::fs::read(&self.journal)?)?;
        let retained = other_active_grants(&self.journal, &current)?;
        manage_config(&mut current, &self.journal, &retained)
    }

    /// Archive revokes all metadata access before reaping. Temporarily grant
    /// only read/traverse, never write or inheritance, while the owner's Git
    /// inspects its recorded checkout. Serialize ACL changes, not the Git
    /// process: a slow filter must not block other agents' lifecycle work.
    pub(crate) fn inspect<T>(
        &self,
        read: impl FnOnce() -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        let lock = lock_journals(&self.journal)?;
        self.validate()?;
        let g = &self.grant;
        let mut current: Grant = serde_json::from_slice(&std::fs::read(&self.journal)?)?;
        if current.common != g.common
            || current.private != g.private
            || current.checkout != g.checkout
            || current.uid != g.uid
            || current.gid != g.gid
        {
            return Err(std::io::Error::other("worktree journal changed"));
        }
        if current.inspection {
            return Err(std::io::Error::other(
                "worktree has incomplete ACL recovery",
            ));
        }
        if current.active || g.uid == crate::sandbox::server_uid() {
            if current.active {
                self.refresh_config_readers()?;
            }
            drop(lock);
            return read();
        }
        let retained = other_active_grants(&self.journal, g)?;
        let files = std::cell::RefCell::new(Vec::new());
        walk_tree(&g.common, false, &|_| {}, &|file, path| {
            if !retained_path(&g.common, path, g) {
                return Ok(());
            }
            let permissions = if [
                g.common.clone(),
                g.common.join("worktrees"),
                g.common.join("logs"),
            ]
            .contains(&path.to_path_buf())
            {
                1
            } else if file.metadata()?.is_dir() {
                5
            } else {
                4
            };
            // An active same-seat sibling may already have rw access here.
            // Do not replace its stronger grant with the inspection grant.
            if retained
                .iter()
                .any(|other| other.gid == g.gid && retained_path(&g.common, path, other))
            {
                return Ok(());
            }
            use std::os::unix::fs::MetadataExt;
            let meta = file.metadata()?;
            // The server cannot grant on another principal's object; such a
            // grant is skipped, so it needs no inspection intent or revocation.
            if path_only(file) || !server_may_change(&meta) {
                return Ok(());
            }
            files.borrow_mut().push((
                InspectionPath {
                    path: path.to_path_buf(),
                    dev: meta.dev(),
                    ino: meta.ino(),
                },
                permissions,
            ));
            Ok(())
        })?;
        let files = files.into_inner();
        current.inspection_paths = Some(files.iter().map(|(target, _)| target.clone()).collect());
        current.inspection = true;
        write_grant(&self.journal, &current)?;
        let granted = files.iter().try_for_each(|(target, permissions)| {
            update_metadata_acl(&target.open()?, g.gid, Some(*permissions), false, None)
        });
        drop(lock);
        let result = granted.and_then(|()| read());
        // Re-read active bindings after Git: another seat may have been
        // admitted meanwhile. Also revoke partial grants on setup failure.
        let _lock = lock_journals(&self.journal)?;
        let mut current: Grant = serde_json::from_slice(&std::fs::read(&self.journal)?)?;
        let retained = other_active_grants(&self.journal, g)?;
        if !current.active
            && let Err(error) = revoke_inspection(&current, &retained)
        {
            current.quarantined = true;
            write_grant(&self.journal, &current)?;
            return Err(error);
        }
        current.inspection_paths = None;
        current.inspection = false;
        write_grant(&self.journal, &current)?;
        result
    }

    pub(crate) fn command(&self) -> std::io::Result<std::process::Command> {
        self.validate()?;
        let g = &self.grant;
        let mut cmd = if unsafe { libc::geteuid() } == g.uid && unsafe { libc::getegid() } == g.gid
        {
            std::process::Command::new("git")
        } else {
            // Attributes (including index attributes) can select any driver in
            // trusted config. Pinning metadata alone cannot make parent-UID Git
            // safe: every worktree-reading command runs with the journaled UID.
            let mut cmd = std::process::Command::new(crate::sandbox::launcher());
            cmd.args([
                "--reuid",
                &g.uid.to_string(),
                "--regid",
                &g.gid.to_string(),
                "--groups",
                &g.gid.to_string(),
                "--no-new-privs",
                "--",
                "git",
            ]);
            cmd
        };
        cmd.env_clear()
            .current_dir("/")
            .env(
                "PATH",
                std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
            )
            .env("HOME", "/nonexistent")
            .env("XDG_CONFIG_HOME", "/nonexistent")
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
            .arg(&self.grant.private)
            .arg("--work-tree")
            .arg(&self.grant.checkout);
        Ok(cmd)
    }

    pub(crate) fn checkout_pointer_matches(&self) -> bool {
        if self.grant.pool.is_some() {
            let git = self.grant.checkout.join(".git");
            return std::fs::symlink_metadata(&git).is_ok_and(|meta| {
                meta.is_dir()
                    && !meta.file_type().is_symlink()
                    && std::fs::canonicalize(&git).is_ok_and(|path| path == self.grant.private)
            });
        }
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;
        let path = self.grant.checkout.join(".git");
        let Ok(file) = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
        else {
            return false;
        };
        if !file.metadata().is_ok_and(|meta| meta.is_file()) {
            return false;
        }
        let mut text = String::new();
        if file.take(8193).read_to_string(&mut text).is_err() || text.len() > 8192 {
            return false;
        }
        let Some(pointer) = text.trim().strip_prefix("gitdir: ") else {
            return false;
        };
        let pointer = self.grant.checkout.join(pointer);
        std::fs::canonicalize(pointer).is_ok_and(|path| path == self.grant.private)
    }

    pub(crate) fn private(&self) -> &Path {
        &self.grant.private
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
    if !grant.active && !grant.inspection {
        return Ok(());
    }
    if grant.pool.is_some() {
        revoke_seat(&grant.common, grant.uid, grant.gid).map_err(|e| refusal(&grant.common, e))?;
    } else {
        let retained = other_active_grants(&journal, &grant).map_err(|e| refusal(&journal, e))?;
        revoke_common(&grant.common, grant.uid, grant.gid, &retained, None)
            .map_err(|e| refusal(&grant.common, e))?;
    }
    grant.active = false;
    grant.inspection = false;
    write_grant(&journal, &grant).map_err(|e| refusal(&journal, e))?;
    // The revocation itself is complete and durable. A busy config lock only
    // delays the reader refresh to the next admission or server start.
    if let Err(error) = refresh_store_config_readers(&journal, &grant) {
        eprintln!(
            "warning: managed Git config readers of {} not refreshed: {error}",
            grant.common.display()
        );
    }
    Ok(())
}

/// Run before either server mode starts consumers. An inspection intent is
/// durable before its first ACL change, so even SIGKILL leaves enough evidence
/// to remove temporary read access and read the resulting ACLs back.
pub(crate) fn recover_inspections(data: &Path) -> Result<(), Error> {
    let directory = data.join("worktree-grants");
    match std::fs::read_dir(&directory) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    }
    let _lock = lock_journals(&directory.join("recovery.json"))?;
    for entry in std::fs::read_dir(&directory)? {
        let journal = entry?.path();
        if journal.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let recover = || -> std::io::Result<()> {
            let mut grant: Grant = serde_json::from_slice(&std::fs::read(&journal)?)?;
            if !grant.inspection {
                return Ok(());
            }
            let result = if grant.active {
                Err(std::io::Error::other("active grant has an open inspection"))
            } else {
                other_active_grants(&journal, &grant)
                    .and_then(|retained| revoke_inspection(&grant, &retained))
            };
            if result.is_err() {
                grant.quarantined = true;
            } else {
                grant.inspection = false;
                grant.inspection_paths = None;
            }
            write_grant(&journal, &grant)?;
            result
        };
        if let Err(error) = recover() {
            eprintln!(
                "warning: quarantined worktree journal {}: {error}",
                journal.display()
            );
        }
    }
    // Repair stores whose managed config lost every reader when their last
    // grant was revoked: one refresh per common store with a pinned record.
    let mut refreshed: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(&directory)? {
        let journal = entry?.path();
        if journal.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let Ok(grant) = std::fs::read(&journal).and_then(|bytes| {
            serde_json::from_slice::<Grant>(&bytes).map_err(std::io::Error::from)
        }) else {
            continue;
        };
        if grant.config.is_none()
            || grant.quarantined
            || grant.inspection
            || refreshed.contains(&grant.common)
        {
            continue;
        }
        match refresh_store_config_readers(&journal, &grant) {
            Ok(()) => refreshed.push(grant.common.clone()),
            Err(error) => eprintln!(
                "warning: managed Git config readers of {} not refreshed: {error}",
                grant.common.display()
            ),
        }
    }
    Ok(())
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

// Common configuration outlives any individual checkout. A clean reaper may
// already have removed a retired journal's private gitdir; validate only the
// recorded common directory/link identity here, independently of ACL membership.
fn recorded_config(journal: &Path, common: &Path) -> std::io::Result<Option<ManagedConfig>> {
    let mut config = None;
    for entry in std::fs::read_dir(journal.parent().unwrap())? {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let grant: Grant = serde_json::from_slice(&std::fs::read(&path)?)?;
        if grant.common != common {
            continue;
        }
        if path == journal && (grant.quarantined || grant.inspection) {
            return Err(std::io::Error::other(
                "worktree has incomplete ACL recovery",
            ));
        }
        if grant.quarantined || grant.inspection {
            continue;
        }
        if let Some(record) = grant.config {
            open_common_config(common, Some(&record))?;
            config = Some(record);
        }
    }
    Ok(config)
}

fn retained_path(common: &Path, path: &Path, grant: &Grant) -> bool {
    path == common
        || path == common.join("worktrees")
        || path == common.join("logs")
        || path == common.join("config")
        || path.starts_with(common.join(CONFIG_DIRECTORY))
        || path == common.join("HEAD")
        || ["objects", "refs", "logs/refs", "packed-refs"]
            .iter()
            .any(|name| path.starts_with(common.join(name)))
        || path.starts_with(&grant.private)
}

/// Shared content the admission re-grants read/write. Revoking it first would
/// only be undone, and is impossible on objects other principals wrote there.
fn regranted_content(common: &Path, path: &Path, grant: &Grant) -> bool {
    ["objects", "refs", "logs/refs", "packed-refs"]
        .iter()
        .any(|name| path.starts_with(common.join(name)))
        || path.starts_with(&grant.private)
}

fn revoke_binding(
    file: &std::fs::File,
    path: &Path,
    common: &Path,
    uid: u32,
    gid: u32,
    retained: &[Grant],
    regrant: Option<&Grant>,
) -> std::io::Result<()> {
    let regranted =
        regrant.is_some_and(|g| g.uid == uid && g.gid == gid && regranted_content(common, path, g));
    let keep_group = regranted
        || retained
            .iter()
            .any(|g| g.gid == gid && retained_path(common, path, g));
    let keep_user = regranted
        || retained
            .iter()
            .any(|g| g.uid == uid && retained_path(common, path, g));
    if keep_group && keep_user {
        return Ok(());
    }
    update_metadata_acl(
        file,
        if keep_group { u32::MAX } else { gid },
        None,
        false,
        (!keep_user).then_some(uid),
    )
}

fn revoke_common(
    common: &Path,
    uid: u32,
    gid: u32,
    retained: &[Grant],
    regrant: Option<&Grant>,
) -> std::io::Result<()> {
    let _config_lock = match std::fs::symlink_metadata(common.join("config")) {
        Ok(meta) => {
            open_common_config(common, None)?;
            Some(lock_config(if meta.file_type().is_symlink() {
                common.join(CONFIG_DIRECTORY).join("config.lock")
            } else {
                common.join("config.lock")
            })?)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    walk_tree(common, false, &|_| {}, &|file, path| {
        revoke_binding(file, path, common, uid, gid, retained, regrant)
    })
}

/// Revoke a per-seat repository's exclusive grants. The seat gitdir belongs
/// to no other seat, so nothing is retained and no shared config lock is
/// needed. The pool is world-readable by mode and is never granted.
fn revoke_seat(common: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    walk_tree(common, false, &|_| {}, &|file, path| {
        revoke_binding(file, path, common, uid, gid, &[], None)
    })
}

/// New inspection intents name every opened object before the first ACL write.
/// Legacy intents require a best-effort no-follow walk; hostile entries never
/// prevent revocation of the remaining regular files or another worktree.
fn revoke_inspection(grant: &Grant, retained: &[Grant]) -> std::io::Result<()> {
    let issues = std::cell::RefCell::new(Vec::new());
    if let Some(paths) = &grant.inspection_paths {
        for target in paths {
            let revoke = || -> std::io::Result<()> {
                if !target.path.starts_with(&grant.common)
                    || !retained_path(&grant.common, &target.path, grant)
                {
                    return Err(std::io::Error::other(
                        "inspection target outside journaled grant set",
                    ));
                }
                let file = target.open()?;
                revoke_binding(
                    &file,
                    &target.path,
                    &grant.common,
                    grant.uid,
                    grant.gid,
                    retained,
                    None,
                )
            };
            if let Err(error) = revoke() {
                issues
                    .borrow_mut()
                    .push(format!("{}: {error}", target.path.display()));
            }
        }
    } else if let Err(error) = walk_tree_policy(
        &grant.common,
        false,
        &|_| {},
        &|file, path| {
            revoke_binding(
                file,
                path,
                &grant.common,
                grant.uid,
                grant.gid,
                retained,
                None,
            )
        },
        true,
        Some(&issues),
    ) {
        issues.borrow_mut().push(error.to_string());
    }
    let issues = issues.into_inner();
    if issues.is_empty() {
        Ok(())
    } else {
        Err(std::io::Error::other(issues.join("; ")))
    }
}

impl InspectionPath {
    fn open(&self) -> std::io::Result<std::fs::File> {
        use std::os::unix::fs::MetadataExt;
        let file = open_nofollow(&self.path)?;
        let meta = file.metadata()?;
        if (!meta.is_file() && !meta.is_dir()) || meta.dev() != self.dev || meta.ino() != self.ino {
            return Err(std::io::Error::other(
                "inspection target type or identity changed",
            ));
        }
        Ok(file)
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
    // A single-principal ordinary checkout needs no cross-seat metadata
    // grant. Cross-seat sharing is supported for linked worktrees and for
    // per-seat repositories over the pool.
    if private == common {
        if let Some(pool) = derive_pool(private) {
            return share_seat(path, private, &pool, uid, gid, journal);
        }
        if uid == crate::sandbox::server_uid() && gid == crate::sandbox::server_gid() {
            return Ok(());
        }
        return Err(refusal(
            path,
            "cross-seat sharing requires a linked Git worktree or a pool-backed per-seat repository",
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
    if grant.quarantined || grant.inspection {
        return Err(refusal(
            &journal,
            "worktree is quarantined after incomplete ACL recovery",
        ));
    }
    if grant.active {
        return RecordedWorktree {
            grant,
            journal: journal.clone(),
        }
        .refresh_config_readers()
        .map_err(|error| refusal(&journal, error));
    }
    if !grant.checkout.join(".git").exists() {
        return Ok(());
    }
    if let Some(pool) = grant.pool.clone() {
        return share_seat_locked(
            &grant.checkout.clone(),
            &grant.private.clone(),
            &pool,
            grant.uid,
            grant.gid,
            &journal,
        );
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
    // Fail closed before the first journal or ACL write: a store that launch
    // would refuse must not be admitted, granted, or left needing revocation.
    trusted_metadata_layout(common, private, uid).map_err(|e| refusal(common, e))?;
    let mut grant = Grant {
        common: common.to_path_buf(),
        gid,
        uid,
        private: private.to_path_buf(),
        checkout: path.to_path_buf(),
        active: true,
        pool: None,
        inspection: false,
        inspection_paths: None,
        quarantined: false,
        config: None,
    };
    let retained = other_active_grants(journal, &grant).map_err(|e| refusal(journal, e))?;
    // Provenance survives the last reader's retirement. Inactive journals may
    // witness only the exact managed objects, never restore their old grants.
    grant.config = recorded_config(journal, common).map_err(|e| refusal(journal, e))?;
    write_grant(journal, &grant).map_err(|e| refusal(journal, e))?;
    // Branch reflogs are in the write allowlist, but a fresh bare store has no
    // logs/refs and the seat cannot create it through traversal-only logs/.
    // The store's owner creates the allowlisted directory (never following a
    // planted symlink: an existing entry is opened no-follow below).
    for name in ["logs", "logs/refs"] {
        match std::fs::create_dir(common.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(refusal(&common.join(name), e)),
        }
    }
    // Clear any legacy broad grants/defaults for this seat before the allowlist.
    revoke_common(common, uid, gid, &retained, Some(&grant)).map_err(|e| refusal(common, e))?;
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
    // Only the dedicated config directory has read-only inheritance. The
    // mixed common directory, hooks/info and siblings never receive defaults.
    manage_config(&mut grant, journal, &retained).map_err(|e| refusal(common, e))?;
    let head = common.join("HEAD");
    if head.exists() {
        walk_tree(&head, false, &|_| {}, &|file, _| {
            update_metadata_acl(file, gid, Some(4), false, None)
        })
        .map_err(|e| refusal(&head, e))?;
    }
    protect_private_gitdir(private, uid, gid).map_err(|e| refusal(private, e))?;
    Ok(())
}

/// Read the object pool a per-seat repository references through its
/// alternates. Provisioning wrote this file seconds before admission, but
/// every later use rechecks the canonical path against the journal.
fn derive_pool(private: &Path) -> Option<PathBuf> {
    let alternates = std::fs::read_to_string(private.join("objects/info/alternates")).ok()?;
    let line = alternates
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))?;
    let objects = if Path::new(line).is_absolute() {
        PathBuf::from(line)
    } else {
        private.join("objects").join(line)
    };
    let pool = std::fs::canonicalize(objects.parent()?).ok()?;
    (pool != private && pool.join("HEAD").is_file()).then_some(pool)
}

fn share_seat(
    path: &Path,
    private: &Path,
    pool: &Path,
    uid: u32,
    gid: u32,
    journal: &Path,
) -> Result<(), Error> {
    let _lock = lock_journals(journal).map_err(|e| refusal(journal, e))?;
    share_seat_locked(path, private, pool, uid, gid, journal)
}

fn share_seat_locked(
    path: &Path,
    private: &Path,
    pool: &Path,
    uid: u32,
    gid: u32,
    journal: &Path,
) -> Result<(), Error> {
    validate_config_files(private, private)?;
    let grant = Grant {
        common: private.to_path_buf(),
        gid,
        uid,
        private: private.to_path_buf(),
        checkout: path.to_path_buf(),
        active: true,
        pool: Some(pool.to_path_buf()),
        inspection: false,
        inspection_paths: None,
        quarantined: false,
        config: None,
    };
    write_grant(journal, &grant).map_err(|e| refusal(journal, e))?;
    // Clear any stale grants for this seat before granting. The seat gitdir
    // is exclusive to this seat, so nothing else retains access here.
    revoke_seat(private, uid, gid).map_err(|e| refusal(private, e))?;
    // The seat writes its own objects, refs, packed-refs, config, index and
    // HEAD through ordinary lock+rename. Grants cover the whole tree, but
    // never the pool: seats read pool objects, only the server fetches.
    walk_tree(private, false, &|_| {}, &|file, _| {
        restrict_others(file)?;
        grant_metadata_acl(file, gid)
    })
    .map_err(|e| refusal(private, e))?;
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

#[cfg(target_os = "linux")]
fn descriptor_fsetxattr(
    file: &std::fs::File,
    name: &std::ffi::CStr,
    bytes: &[u8],
) -> std::io::Result<()> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    if unsafe {
        libc::fsetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            bytes.as_ptr().cast(),
            bytes.len(),
            0,
        )
    } == 0
    {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if matches!(err.raw_os_error(), Some(libc::EPERM | libc::EACCES)) {
        let meta = file.metadata()?;
        let orig_uid = meta.uid();
        let srv_uid = crate::sandbox::server_uid();
        if orig_uid != srv_uid && (meta.is_dir() || (meta.is_file() && meta.nlink() == 1)) {
            descriptor_fchown(file, Some(srv_uid), None)?;
            let res = if unsafe {
                libc::fsetxattr(
                    file.as_raw_fd(),
                    name.as_ptr(),
                    bytes.as_ptr().cast(),
                    bytes.len(),
                    0,
                )
            } == 0
            {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            };
            let restore = descriptor_fchown(file, Some(orig_uid), None);
            return res.and(restore);
        }
    }
    Err(err)
}

#[cfg(target_os = "linux")]
fn descriptor_fremovexattr(file: &std::fs::File, name: &std::ffi::CStr) -> std::io::Result<()> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    if unsafe { libc::fremovexattr(file.as_raw_fd(), name.as_ptr()) } == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ENODATA) {
        return Ok(());
    }
    if matches!(err.raw_os_error(), Some(libc::EPERM | libc::EACCES)) {
        let meta = file.metadata()?;
        let orig_uid = meta.uid();
        let srv_uid = crate::sandbox::server_uid();
        if orig_uid != srv_uid && (meta.is_dir() || (meta.is_file() && meta.nlink() == 1)) {
            descriptor_fchown(file, Some(srv_uid), None)?;
            let res = if unsafe { libc::fremovexattr(file.as_raw_fd(), name.as_ptr()) } == 0
                || std::io::Error::last_os_error().raw_os_error() == Some(libc::ENODATA)
            {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            };
            let restore = descriptor_fchown(file, Some(orig_uid), None);
            return res.and(restore);
        }
    }
    Err(err)
}

#[cfg(target_os = "linux")]
fn clear_default_acl(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    descriptor_fremovexattr(file, c"system.posix_acl_default")?;
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

#[cfg(unix)]
fn restrict_others(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata()?;
    // Already restricted: nothing to change, including on objects the server
    // does not own (e.g. HEAD/index deliberately handed to the seat).
    if meta.mode() & 0o007 == 0 {
        return Ok(());
    }
    if path_only(file) {
        return Err(unreadable_foreign(&meta));
    }
    if !meta.is_dir() && meta.nlink() != 1 {
        return Err(std::io::Error::other(
            "refusing permission changes to a hard-linked file",
        ));
    }
    let mode = meta.mode() & 0o7777 & !0o007;
    descriptor_fchmod(file, mode as libc::mode_t)?;
    if file.metadata()?.mode() & 7 != 0 {
        return Err(std::io::Error::other("other-permission readback differs"));
    }
    Ok(())
}

#[cfg(unix)]
fn protect_private_gitdir(private: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
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
        if path_only(file) {
            return Err(unreadable_foreign(&file.metadata()?));
        }
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
            descriptor_fchmod(file, sticky_mode)?;
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
            if meta.uid() != uid {
                descriptor_fchown(file, Some(uid), None)?;
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
    walk_tree_policy(path, skip_git, before_open, apply, descend, None)
}

#[cfg(unix)]
fn walk_tree_policy(
    path: &Path,
    skip_git: bool,
    before_open: &dyn Fn(&Path),
    apply: &dyn Fn(&std::fs::File, &Path) -> std::io::Result<()>,
    descend: bool,
    issues: Option<&std::cell::RefCell<Vec<String>>>,
) -> std::io::Result<()> {
    // Ordinary callers historically resolve trusted host aliases (e.g. macOS
    // /var -> /private/var). Recovery must never resolve a replacement alias.
    let resolved = if issues.is_none() {
        std::fs::canonicalize(path)?
    } else {
        path.to_path_buf()
    };
    let file = open_nofollow(&resolved)?;
    fn record(
        issues: Option<&std::cell::RefCell<Vec<String>>>,
        path: &Path,
        error: std::io::Error,
    ) -> std::io::Result<()> {
        if let Some(issues) = issues {
            issues
                .borrow_mut()
                .push(format!("{}: {error}", path.display()));
            Ok(())
        } else {
            Err(error)
        }
    }
    fn visit(
        file: &std::fs::File,
        path: &Path,
        skip_git: bool,
        before_open: &dyn Fn(&Path),
        apply: &dyn Fn(&std::fs::File, &Path) -> std::io::Result<()>,
        descend: bool,
        issues: Option<&std::cell::RefCell<Vec<String>>>,
    ) -> std::io::Result<()> {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        let fd = file.as_raw_fd();
        if let Err(error) = apply(file, path) {
            record(issues, path, error)?;
        }
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
                record(issues, &child_path, std::io::Error::last_os_error())?;
                continue;
            }
            let stat = unsafe { stat.assume_init() };
            let kind = stat.st_mode & libc::S_IFMT;
            if kind == libc::S_IFLNK {
                if issues.is_some()
                    && !(child_path == path.join("config")
                        && std::fs::read_link(&child_path).is_ok_and(|target| {
                            target == Path::new(CONFIG_DIRECTORY).join("config")
                        })
                        && open_common_config(path, None).is_ok())
                {
                    record(
                        issues,
                        &child_path,
                        std::io::Error::other("symlink skipped during ACL recovery"),
                    )?;
                }
                continue;
            }
            if skip_git && name.to_bytes() == b".git" {
                // The common store gets additive ACLs, never a group replacement.
                continue;
            }
            if kind != libc::S_IFDIR && kind != libc::S_IFREG {
                record(
                    issues,
                    &child_path,
                    std::io::Error::other("unsupported file type skipped"),
                )?;
                continue;
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
                    record(issues, &child_path, error)?;
                    continue;
                }
            }
            let child = unsafe { std::fs::File::from_raw_fd(child) };
            let opened = child.metadata()?;
            #[allow(
                clippy::unnecessary_cast,
                reason = "stat fields differ across Unix targets"
            )]
            if opened.dev() != stat.st_dev as u64 || opened.ino() != stat.st_ino as u64 {
                record(
                    issues,
                    &child_path,
                    std::io::Error::other("object replaced during sharing"),
                )?;
                continue;
            }
            if let Err(error) = visit(
                &child,
                &child_path,
                skip_git,
                before_open,
                apply,
                descend,
                issues,
            ) {
                record(issues, &child_path, error)?;
            }
        }
        Ok(())
    }
    visit(&file, path, skip_git, before_open, apply, descend, issues)
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
    let access_name = c"system.posix_acl_access";
    let default_name = c"system.posix_acl_default";
    let owner_meta = file.metadata()?;
    if path_only(file) || !server_may_change(&owner_meta) {
        // Another principal's object (e.g. a pack another seat wrote into a
        // shared store) cannot be changed by the server. Adding access is
        // skipped: the seat-side admission probe proves the access it needs.
        // Removing or narrowing existing access is impossible, so it refuses.
        // Compare effective access (entry & mask). Inherited execute on a
        // regular file grants nothing a data file's reader could use.
        let relevant = if owner_meta.is_dir() { 7 } else { 6 };
        let narrows = |entries: Option<Vec<(u16, u16, u32)>>| {
            let entries = entries.unwrap_or_default();
            let mask = entries.iter().find(|e| e.0 == 16).map_or(7, |e| e.1);
            entries.iter().any(|&(tag, perm, id)| {
                tag == 8
                    && id == gid
                    && permissions.is_none_or(|p| perm & mask & relevant & !p != 0)
                    || tag == 2 && Some(id) == revoke_uid
            })
        };
        let default = if owner_meta.is_dir() && (inherit || permissions.is_none()) {
            read(file, default_name)?
        } else {
            None
        };
        if narrows(read(file, access_name)?) || narrows(default) {
            return Err(std::io::Error::other(format!(
                "cannot narrow an ACL entry on an object owned by uid {}",
                owner_meta.uid()
            )));
        }
        return Ok(());
    }
    // Serialize read/modify/write ACL grants across simultaneous spawns.
    // A seat can flock even a read-only metadata descriptor. Never let that
    // advisory lock hold startup recovery or another seat's lifecycle hostage.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
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
    fn read(
        file: &std::fs::File,
        name: &std::ffi::CStr,
    ) -> std::io::Result<Option<Vec<(u16, u16, u32)>>> {
        let mut bytes = vec![0u8; 65536];
        let len = read_xattr(file, name, &mut bytes);
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
            if desired & 2 != 0 {
                let owner_permissions = entries.iter().find(|e| e.0 == 1).map(|e| e.1).unwrap_or(0);
                if let Some(entry) = entries.iter_mut().find(|e| e.0 == 2 && e.2 == meta.uid()) {
                    entry.1 |= owner_permissions;
                } else {
                    entries.push((2, owner_permissions, meta.uid()));
                }
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
        descriptor_fsetxattr(file, name, &bytes)?;
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
  private=$(git -c safe.directory="$1" -c core.fsmonitor=false -c core.hooksPath=/dev/null -C "$1" rev-parse --path-format=absolute --git-dir)
  common=$(git -c safe.directory="$1" -c core.fsmonitor=false -c core.hooksPath=/dev/null -C "$1" rev-parse --path-format=absolute --git-common-dir)
  for d in "$private" "$common/objects" "$common/refs"; do
    probe "$d"
  done
  git -c safe.directory="$1" -c core.fsmonitor=false -c core.hooksPath=/dev/null -C "$1" status --porcelain >/dev/null
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

    #[cfg(target_os = "linux")]
    #[test]
    fn exact_inspection_recovery_ignores_unrelated_special_entries() {
        use std::os::fd::AsRawFd;
        let dir = tempfile::tempdir().unwrap();
        let common = dir.path().join("common");
        std::fs::create_dir(&common).unwrap();
        let path = common.join("HEAD");
        std::fs::write(&path, "recorded content").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let gid = crate::sandbox::server_gid().wrapping_add(100000);
        update_metadata_acl(&file, gid, Some(4), false, None).unwrap();
        let meta = file.metadata().unwrap();
        let outside = dir.path().join("outside");
        std::fs::write(&outside, "outside content").unwrap();
        std::fs::hard_link(&outside, common.join("unrelated-hardlink")).unwrap();
        std::os::unix::fs::symlink(&outside, common.join("unrelated-symlink")).unwrap();
        let fifo =
            std::ffi::CString::new(common.join("unrelated-fifo").as_os_str().as_encoded_bytes())
                .unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        let id = anyagents_core::AgentId::generate();
        let journal = grant_path(dir.path(), &id);
        write_grant(
            &journal,
            &Grant {
                common: common.clone(),
                private: common.join("worktrees/child"),
                checkout: dir.path().join("checkout"),
                uid: crate::sandbox::server_uid(),
                gid,
                active: false,
                pool: None,
                inspection: true,
                quarantined: false,
                config: None,
                inspection_paths: Some(vec![InspectionPath {
                    path,
                    dev: meta.dev(),
                    ino: meta.ino(),
                }]),
            },
        )
        .unwrap();
        recover_inspections(dir.path()).unwrap();
        let grant: Grant = serde_json::from_slice(&std::fs::read(journal).unwrap()).unwrap();
        assert!(!grant.inspection && !grant.quarantined && grant.inspection_paths.is_none());
        let mut bytes = [0u8; 65536];
        let len = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                c"system.posix_acl_access".as_ptr(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
            )
        };
        if len >= 0 {
            assert!(
                !bytes[4..len as usize]
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .any(|entry| {
                        u16::from_le_bytes(entry[..2].try_into().unwrap()) == 8
                            && u32::from_le_bytes(entry[4..].try_into().unwrap()) == gid
                    })
            );
        } else {
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::ENODATA)
            );
        }
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside content");
        assert_eq!(std::fs::metadata(&outside).unwrap().nlink(), 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn recovery_server_worker() {
        let Some(data) = std::env::var_os("ANYAGENTS_RECOVERY_WORKER_DATA") else {
            return;
        };
        crate::server::run(Path::new(&data)).unwrap();
    }

    #[cfg(target_os = "linux")]
    fn recovery_quarantines_hostile_entry(kind: &str) {
        use std::io::{Read, Write};
        use std::os::{fd::AsRawFd, unix::net::UnixStream};
        fn acl(path: &Path) -> Vec<u8> {
            let file = std::fs::File::open(path).unwrap();
            let mut bytes = vec![0; 65536];
            let len = unsafe {
                libc::fgetxattr(
                    file.as_raw_fd(),
                    c"system.posix_acl_access".as_ptr(),
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
            bytes.truncate(len as usize);
            bytes
        }
        fn has_group(path: &Path, gid: u32) -> bool {
            acl(path)
                .get(4..)
                .unwrap_or_default()
                .as_chunks::<8>()
                .0
                .iter()
                .any(|e| {
                    u16::from_le_bytes(e[..2].try_into().unwrap()) == 8
                        && u32::from_le_bytes(e[4..].try_into().unwrap()) == gid
                })
        }
        fn target(path: &Path, gid: u32) -> InspectionPath {
            std::fs::write(path, "inspection content\n").unwrap();
            let file = std::fs::File::open(path).unwrap();
            update_metadata_acl(&file, gid, Some(4), false, None).unwrap();
            let meta = file.metadata().unwrap();
            InspectionPath {
                path: path.to_path_buf(),
                dev: meta.dev(),
                ino: meta.ino(),
            }
        }
        struct Server(std::process::Child);
        impl Drop for Server {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        // Exercise both the deployed pre-upgrade journal and the exact-object
        // journal. A malicious entry must not abort either server entrypoint.
        for exact in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let f = crate::worktree::tests::Fixture::new(dir.path());
            let uid = crate::sandbox::server_uid();
            let gid = crate::sandbox::server_gid().wrapping_add(100000);
            let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
            let bad_agent = store
                .create_agent(anyagents_core::NewAgent::new("bad-seat", "codex"))
                .unwrap();
            let good_agent = store
                .create_agent(anyagents_core::NewAgent::new("good-seat", "codex"))
                .unwrap();
            store.set_agent_state(&bad_agent.id, "paused").unwrap();
            store.set_agent_state(&good_agent.id, "paused").unwrap();
            let bad = bad_agent.id.clone();
            let good = good_agent.id.clone();
            let checkout = anyagents_core::agent_work_dir(dir.path(), &bad);
            let survivor = dir.path().join("good");
            for (id, path, group, branch) in [
                (&bad, &checkout, gid, "bad"),
                (&good, &survivor, gid + 1, "good"),
            ] {
                f.consumer
                    .remote("ExampleOrg/demo", path, branch, None)
                    .unwrap();
                let dirs = git_dirs(path).unwrap();
                share_known(
                    path,
                    &dirs[0],
                    dirs.last().unwrap(),
                    uid,
                    group,
                    &grant_path(dir.path(), id),
                )
                .unwrap();
            }
            let bad_record = RecordedWorktree::load(dir.path(), &bad, &checkout).unwrap();
            bad_record.validate().unwrap(); // baseline reaches the actual guard
            revoke_recorded(dir.path(), &bad).unwrap();
            let journal = grant_path(dir.path(), &bad);
            let mut grant: Grant =
                serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
            let common = grant.common.clone();
            let hostile = if kind == "flock" {
                bad_record.private().join("HEAD")
            } else {
                common.join("objects/inspection-hostile")
            };
            let after = common.join("objects/inspection-after");
            let paths = vec![target(&hostile, gid), target(&after, gid)];
            grant.inspection = true;
            grant.inspection_paths = exact.then_some(paths);
            write_grant(&journal, &grant).unwrap();
            let outside = dir.path().join("outside");
            let mut held_lock = None;
            match kind {
                "flock" => {
                    // flock(2) permits an exclusive lock on a read-only fd.
                    let file = std::fs::File::open(&hostile).unwrap();
                    assert_eq!(
                        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                        0
                    );
                    held_lock = Some(file);
                    std::fs::write(&outside, "outside content\n").unwrap();
                }
                "fifo" => {
                    std::fs::remove_file(&hostile).unwrap();
                    let name =
                        std::ffi::CString::new(hostile.as_os_str().as_encoded_bytes()).unwrap();
                    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
                    std::fs::write(&outside, "outside content\n").unwrap();
                }
                "hardlink" => {
                    std::fs::hard_link(&hostile, &outside).unwrap();
                }
                "symlink" => {
                    std::fs::remove_file(&hostile).unwrap();
                    std::fs::write(&outside, "outside content\n").unwrap();
                    std::os::unix::fs::symlink(&outside, &hostile).unwrap();
                }
                _ => unreachable!(),
            }
            let build_output = checkout.join("target/debug/preserve-after-quarantine");
            std::fs::create_dir_all(build_output.parent().unwrap()).unwrap();
            std::fs::write(&build_output, "unique build evidence").unwrap();
            let outside_bytes = std::fs::read(&outside).unwrap();
            let outside_acl = acl(&outside);
            let survivor_dirs = git_dirs(&survivor).unwrap();
            let survivor_acl = acl(&survivor_dirs[0]);
            let objects_acl = acl(&common.join("objects"));
            assert!(has_group(&after, gid));
            // A second interrupted inspection in an independent common store
            // must also be recovered, regardless of journal enumeration order.
            let other = dir.path().join("other-common");
            std::fs::create_dir(&other).unwrap();
            let other_target = target(&other.join("HEAD"), gid + 2);
            let other_id = anyagents_core::AgentId::generate();
            let other_journal = grant_path(dir.path(), &other_id);
            write_grant(
                &other_journal,
                &Grant {
                    common: other.clone(),
                    private: other.join("worktrees/other"),
                    checkout: dir.path().join("other-checkout"),
                    uid,
                    gid: gid + 2,
                    active: false,
                    pool: None,
                    inspection: true,
                    inspection_paths: Some(vec![other_target]),
                    quarantined: false,
                    config: None,
                },
            )
            .unwrap();
            let log = dir.path().join("server.log");
            let command_env = std::env::vars_os()
                .filter(|(key, _)| !key.as_encoded_bytes().starts_with(b"ANYAGENTS_"));
            let mut server = Server(
                std::process::Command::new(std::env::current_exe().unwrap())
                    .env_clear()
                    .envs(command_env)
                    .env("ANYAGENTS_RECOVERY_WORKER_DATA", dir.path())
                    .env("TOKIO_WORKER_THREADS", "2")
                    .args([
                        "--exact",
                        "worktree_access::tests::recovery_server_worker",
                        "--nocapture",
                    ])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::fs::File::create(&log).unwrap())
                    .spawn()
                    .unwrap(),
            );
            let socket = crate::server::socket_path(dir.path());
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut stream = loop {
                if let Ok(stream) = UnixStream::connect(&socket) {
                    break stream;
                }
                assert!(
                    server.0.try_wait().unwrap().is_none() && std::time::Instant::now() < deadline,
                    "{kind}, exact={exact}: {}",
                    std::fs::read_to_string(&log).unwrap()
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            stream.write_all(format!("GET /v1/whoami HTTP/1.1\r\nHost: local\r\n{}: {}\r\nConnection: close\r\n\r\n", crate::auth::AGENT_ID_HEADER, good).as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert!(
                response.starts_with("HTTP/1.1 200") && response.contains(good.as_str()),
                "{response}"
            );
            let recovered: Grant =
                serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
            assert!(recovered.quarantined && recovered.inspection && !recovered.active);
            assert!(
                bad_record
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("quarantined")
            );
            assert!(
                restore_recorded(dir.path(), &bad)
                    .unwrap_err()
                    .to_string()
                    .contains("quarantined")
            );
            let launch = crate::vendor::vendor_launch(anyagents_supervisor::LaunchRequest {
                agent: bad_agent.clone(),
                data_dir: dir.path().to_path_buf(),
                config_dir: anyagents_core::agent_config_dir(dir.path(), &bad),
                work_dir: checkout.clone(),
                extra_env: Vec::new(),
            });
            let error = match launch {
                Err(error) => error,
                Ok(_) => panic!("quarantined worktree launched"),
            };
            assert!(error.contains("quarantined"), "{error}");
            ensure_launchable(dir.path(), &good).unwrap();
            assert!(
                !has_group(&after, gid),
                "a hostile entry stopped later revocation"
            );
            assert_eq!(acl(&survivor_dirs[0]), survivor_acl);
            assert_eq!(acl(&common.join("objects")), objects_acl);
            RecordedWorktree::load(dir.path(), &good, &survivor)
                .unwrap()
                .validate()
                .unwrap();
            assert!(!has_group(&other.join("HEAD"), gid + 2));
            let other_grant: Grant =
                serde_json::from_slice(&std::fs::read(&other_journal).unwrap()).unwrap();
            assert!(!other_grant.inspection && !other_grant.quarantined);
            assert_eq!(std::fs::read(&outside).unwrap(), outside_bytes);
            assert_eq!(
                acl(&outside),
                outside_acl,
                "outside link target ACL changed"
            );
            assert!(
                std::fs::read_to_string(&log)
                    .unwrap()
                    .contains("quarantined worktree journal")
            );
            // Even after releasing a transient lock, quarantine is not cleared
            // by reaping: no build output or recovery evidence may be removed.
            drop(held_lock);
            assert!(!crate::server::reap_subagent_ephemeral_dirs(
                dir.path(),
                &bad
            ));
            assert_eq!(
                std::fs::read(&build_output).unwrap(),
                b"unique build evidence"
            );
            let retained: Grant =
                serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
            assert!(retained.quarantined && retained.inspection);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn locked_head_inspection_recovery_starts_server_and_preserves_build_outputs() {
        recovery_quarantines_hostile_entry("flock");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fifo_inspection_recovery_quarantines_only_its_worktree() {
        recovery_quarantines_hostile_entry("fifo");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn hardlink_inspection_recovery_quarantines_only_its_worktree() {
        recovery_quarantines_hostile_entry("hardlink");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn symlink_inspection_recovery_quarantines_only_its_worktree() {
        recovery_quarantines_hostile_entry("symlink");
    }

    // Author overrides are process-global: a lock cannot protect readers in
    // other modules. Each identity scenario gets its own environment instead.
    fn identity_test_worker(name: &str) -> bool {
        let name = format!("worktree_access::tests::{name}");
        if std::env::var("ANYAGENTS_WORKTREE_IDENTITY_TEST").as_deref() == Ok(name.as_str()) {
            return false;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &name, "--nocapture"])
            .env("ANYAGENTS_WORKTREE_IDENTITY_TEST", &name)
            .env_remove("ANYAGENTS_GIT_AUTHOR_NAME")
            .env_remove("ANYAGENTS_GIT_AUTHOR_EMAIL")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        true
    }

    #[test]
    fn private_identity_preserves_common_config_and_requires_extension() {
        if identity_test_worker("private_identity_preserves_common_config_and_requires_extension") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("ExampleOrg/demo", &target, "child", None)
            .unwrap();
        let common_config = f.source.join(".git/config");
        let before = std::fs::read(&common_config).unwrap();
        let inode = std::fs::metadata(&common_config).unwrap().ino();
        set_worktree_identity(&target).unwrap();
        assert_eq!(std::fs::read(&common_config).unwrap(), before);
        assert_eq!(std::fs::metadata(&common_config).unwrap().ino(), inode);
        for (key, expected) in [
            ("user.name", "Example Author"),
            ("user.email", "author@example.test"),
        ] {
            assert_eq!(
                crate::worktree::tests::git(&target, &["config", "--get", key]),
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
            .remote("ExampleOrg/demo", &target, "child", None)
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
                    "worktree_access::tests::seat_git_configuration_ignores_ambient_repository",
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
                pool: None,
                inspection: false,
                inspection_paths: None,
                quarantined: false,
                config: None,
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
            .remote("ExampleOrg/demo", &target, "child", None)
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
    fn managed_config_adoption_preserves_original_and_pins_both_objects() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("ExampleOrg/demo", &target, "child", None)
            .unwrap();
        let dirs = git_dirs(&target).unwrap();
        let common = dirs.last().unwrap();
        let config = common.join("config");
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
        let original = std::fs::read(&config).unwrap();
        let meta = std::fs::metadata(&config).unwrap();
        let id = anyagents_core::AgentId::generate();
        let journal = grant_path(dir.path(), &id);
        let uid = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid().wrapping_add(100000);
        std::fs::write(common.join("config.lock"), b"another Git writer").unwrap();
        let err = share_known(&target, &dirs[0], common, uid, gid, &journal).unwrap_err();
        assert!(err.to_string().contains("exists"), "{err}");
        assert_eq!(std::fs::read(&config).unwrap(), original);
        assert_eq!(std::fs::metadata(&config).unwrap().ino(), meta.ino());
        assert!(!common.join(CONFIG_DIRECTORY).exists());
        assert_eq!(
            std::fs::read(common.join("config.lock")).unwrap(),
            b"another Git writer"
        );
        std::fs::remove_file(common.join("config.lock")).unwrap();
        share_known(&target, &dirs[0], common, uid, gid, &journal).unwrap();
        assert_eq!(std::fs::read(&config).unwrap(), original);
        let after = std::fs::metadata(&config).unwrap();
        assert_eq!(
            (
                after.dev(),
                after.ino(),
                after.uid(),
                after.gid(),
                after.nlink()
            ),
            (meta.dev(), meta.ino(), meta.uid(), meta.gid(), 1)
        );
        ensure_launchable(dir.path(), &id).unwrap();
        let managed = common.join(CONFIG_DIRECTORY);
        let saved = common.join("config-directory-saved");
        std::fs::rename(&managed, &saved).unwrap();
        std::fs::create_dir(&managed).unwrap();
        std::fs::set_permissions(&managed, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(managed.join("config"), &original).unwrap();
        let replacement = std::fs::metadata(managed.join("config")).unwrap();
        let err = ensure_launchable(dir.path(), &id).unwrap_err();
        assert!(err.to_string().contains("identity changed"), "{err}");
        assert_eq!(
            std::fs::metadata(managed.join("config")).unwrap().mode(),
            replacement.mode()
        );
        std::fs::remove_dir_all(&managed).unwrap();
        std::fs::rename(&saved, &managed).unwrap();
        let saved_link = common.join("config-link-saved");
        std::fs::rename(&config, &saved_link).unwrap();
        std::os::unix::fs::symlink(Path::new(CONFIG_DIRECTORY).join("config"), &config).unwrap();
        let err = ensure_launchable(dir.path(), &id).unwrap_err();
        assert!(err.to_string().contains("identity changed"), "{err}");
        std::fs::remove_file(&config).unwrap();
        std::fs::rename(saved_link, config).unwrap();
        ensure_launchable(dir.path(), &id).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn interrupted_config_adoption_is_journalled_and_preserves_both_source_links() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("ExampleOrg/demo", &target, "child", None)
            .unwrap();
        let dirs = git_dirs(&target).unwrap();
        let common = dirs.last().unwrap();
        let config = common.join("config");
        let original = std::fs::read(&config).unwrap();
        let original_inode = std::fs::metadata(&config).unwrap().ino();
        let id = anyagents_core::AgentId::generate();
        let journal = grant_path(dir.path(), &id);
        let mut grant = Grant {
            common: common.clone(),
            private: dirs[0].clone(),
            checkout: target.clone(),
            uid: crate::sandbox::server_uid(),
            gid: crate::sandbox::server_gid(),
            active: true,
            pool: None,
            inspection: false,
            inspection_paths: None,
            quarantined: false,
            config: None,
        };
        let lock = lock_journals(&journal).unwrap();
        let err = manage_config_with_hook(&mut grant, &journal, &[], &|| {
            Err(std::io::Error::other(
                "injected config adoption interruption",
            ))
        })
        .unwrap_err();
        assert_eq!(err.to_string(), "injected config adoption interruption");
        drop(lock);
        let pending: Grant = serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
        assert!(pending.quarantined && pending.config.unwrap().adoption_source.is_some());
        for path in [&config, &common.join(CONFIG_DIRECTORY).join("config")] {
            assert_eq!(std::fs::read(path).unwrap(), original);
            let meta = std::fs::metadata(path).unwrap();
            assert_eq!((meta.ino(), meta.nlink()), (original_inode, 2));
        }
        assert!(
            ensure_launchable(dir.path(), &id)
                .unwrap_err()
                .to_string()
                .contains("quarantined")
        );
        assert!(reaping_blocked(dir.path(), &id));
        assert!(!common.join("config.lock").exists());
    }

    #[test]
    fn admission_recovers_config_provenance_after_the_last_reader_retires() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let first = dir.path().join("first");
        f.consumer
            .remote("ExampleOrg/demo", &first, "first", None)
            .unwrap();
        let id = anyagents_core::AgentId::generate();
        let journal = grant_path(dir.path(), &id);
        let uid = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid();
        share(&first, uid, gid, &journal).unwrap();
        let dirs = git_dirs(&first).unwrap();
        let common = dirs.last().unwrap();
        let retired: Grant = serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
        let config = retired.config.unwrap();
        revoke_recorded(dir.path(), &id).unwrap();
        std::fs::remove_dir_all(&dirs[0]).unwrap();
        assert!(
            other_active_grants(
                &journal,
                &serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap()
            )
            .unwrap()
            .is_empty()
        );

        let second = dir.path().join("second");
        f.consumer
            .remote("ExampleOrg/demo", &second, "second", None)
            .unwrap();
        let second_id = anyagents_core::AgentId::generate();
        share(&second, uid, gid, &grant_path(dir.path(), &second_id)).unwrap();
        let current: Grant =
            serde_json::from_slice(&std::fs::read(grant_path(dir.path(), &second_id)).unwrap())
                .unwrap();
        assert_eq!(current.config.unwrap().directory.ino, config.directory.ino);
        let retired: Grant = serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
        assert!(
            !retired.active,
            "provenance recovery revived a retired grant"
        );
        revoke_recorded(dir.path(), &second_id).unwrap();

        // A same-target replacement is still a different, unrecorded symlink.
        // Retain the original link so its inode cannot be recycled for the fake.
        let link = common.join("config");
        let saved = common.join("config.saved");
        std::fs::rename(&link, &saved).unwrap();
        std::os::unix::fs::symlink(Path::new(CONFIG_DIRECTORY).join("config"), &link).unwrap();
        let error = share(&second, uid, gid, &grant_path(dir.path(), &second_id)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("managed Git config identity changed"),
            "{error}"
        );
        let current: Grant =
            serde_json::from_slice(&std::fs::read(grant_path(dir.path(), &second_id)).unwrap())
                .unwrap();
        assert!(!current.active, "identity refusal revived a retired grant");
        std::fs::remove_file(link).unwrap();
        std::fs::rename(saved, common.join("config")).unwrap();
        assert!(recorded_config(&journal, common).unwrap().is_some());
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
            .remote("ExampleOrg/demo", &target, "child", None)
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
            .remote("ExampleOrg/demo", &sibling, "sibling", None)
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
        // Real Git config lock+rename inherits readers immediately. These
        // assertions deliberately precede every lifecycle or inspection call.
        let config = common.join("config");
        let managed = common.join(CONFIG_DIRECTORY);
        assert_eq!(
            std::fs::read_link(&config).unwrap(),
            Path::new(CONFIG_DIRECTORY).join("config")
        );
        let config_link = std::fs::symlink_metadata(&config).unwrap().ino();
        for edit in ["first", "second", "third"] {
            let before = std::fs::metadata(&config).unwrap().ino();
            crate::worktree::tests::git(&f.source, &["config", "test.replacement", edit]);
            assert_ne!(std::fs::metadata(&config).unwrap().ino(), before);
            assert_eq!(
                std::fs::symlink_metadata(&config).unwrap().ino(),
                config_link
            );
            for reader in [gid, gid + 1] {
                assert!(acl(&config, c"system.posix_acl_access").contains(&(8, 4, reader)));
                assert!(acl(&managed, c"system.posix_acl_default").contains(&(8, 4, reader)));
            }
            for root in [
                common.clone(),
                common.join("worktrees"),
                common.join("logs"),
            ] {
                assert!(acl(&root, c"system.posix_acl_default").is_empty());
            }
            for protected in [
                common.join("hooks/pre-commit"),
                sibling_dirs[0].join("index"),
            ] {
                assert!(
                    !acl(&protected, c"system.posix_acl_access")
                        .iter()
                        .any(|e| e.0 == 8 && e.2 == gid)
                );
            }
        }
        // No refresh may follow an outside symlink or change a hard-linked
        // replacement. The original outside bytes, owner, mode and ACL survive.
        let saved = common.join("config.saved");
        std::fs::rename(&config, &saved).unwrap();
        let outside = dir.path().join("outside-config");
        std::fs::write(&outside, b"[core]\nrepositoryformatversion = 0\n").unwrap();
        let outside_meta = std::fs::metadata(&outside).unwrap();
        let outside_acl = acl(&outside, c"system.posix_acl_access");
        for hardlink in [false, true] {
            if hardlink {
                std::fs::hard_link(&outside, &config).unwrap();
            } else {
                std::os::unix::fs::symlink(&outside, &config).unwrap();
            }
            assert!(ensure_launchable(dir.path(), &id).is_err());
            assert!(restore_recorded(dir.path(), &id).is_err());
            assert!(
                RecordedWorktree::load(dir.path(), &id, &target)
                    .unwrap()
                    .inspect(|| Ok(()))
                    .is_err()
            );
            let after = std::fs::metadata(&outside).unwrap();
            assert_eq!(
                (after.uid(), after.gid(), after.mode()),
                (outside_meta.uid(), outside_meta.gid(), outside_meta.mode())
            );
            assert_eq!(acl(&outside, c"system.posix_acl_access"), outside_acl);
            assert_eq!(
                std::fs::read(&outside).unwrap(),
                b"[core]\nrepositoryformatversion = 0\n"
            );
            std::fs::remove_file(&config).unwrap();
        }
        std::fs::rename(saved, &config).unwrap();
        revoke_recorded(dir.path(), &id).unwrap();
        crate::worktree::tests::git(&f.source, &["config", "test.after-revocation", "survivor"]);
        assert!(
            !acl(&config, c"system.posix_acl_access")
                .iter()
                .any(|e| e.0 == 8 && e.2 == gid)
        );
        assert!(acl(&config, c"system.posix_acl_access").contains(&(8, 4, gid + 1)));
        assert!(
            !acl(&managed, c"system.posix_acl_default")
                .iter()
                .any(|e| e.0 == 8 && e.2 == gid)
        );
        assert!(acl(&managed, c"system.posix_acl_default").contains(&(8, 4, gid + 1)));
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
                .remote("ExampleOrg/demo", &target, name, None)
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
        crate::worktree::tests::git(&f.source, &["config", "test.survivor", "active"]);
        assert_eq!(group_permission(&config, gid), Some(4));
        let writer_lock = first
            .last()
            .unwrap()
            .join(CONFIG_DIRECTORY)
            .join("config.lock");
        std::fs::write(&writer_lock, b"in-flight Git writer").unwrap();
        let error = revoke_recorded(dir.path(), b2).unwrap_err();
        assert!(error.to_string().contains("exists"), "{error}");
        assert_eq!(
            std::fs::read(&writer_lock).unwrap(),
            b"in-flight Git writer"
        );
        assert_eq!(group_permission(&config, gid), Some(4));
        let still_active: Grant =
            serde_json::from_slice(&std::fs::read(grant_path(dir.path(), b2)).unwrap()).unwrap();
        assert!(still_active.active);
        std::fs::remove_file(writer_lock).unwrap();
        revoke_recorded(dir.path(), b2).unwrap();
        assert_eq!(group_permission(&second[0], gid), None);
        assert_eq!(group_permission(&objects, gid), None);
        assert_eq!(group_permission(&config, gid), None);
        crate::worktree::tests::git(&f.source, &["config", "test.survivor", "retired"]);
        assert_eq!(group_permission(&config, gid), None);
    }

    #[test]
    fn shared_config_refuses_includes_without_following_them() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("ExampleOrg/demo", &target, "child", None)
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

    #[cfg(target_os = "linux")]
    fn acl_entries(path: &Path, name: &std::ffi::CStr) -> Vec<(u16, u16, u32)> {
        let file = std::fs::File::open(path).unwrap();
        let mut bytes = vec![0u8; 65536];
        let len = read_xattr(&file, name, &mut bytes);
        if len < 0 {
            return Vec::new();
        }
        bytes.truncate(len as usize);
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
            .collect()
    }

    /// An object the server cannot open or change (another principal's
    /// unreadable file in a shared store) is bound by inode with O_PATH. Its
    /// ACL is still read exactly; additive grants are skipped and only a
    /// revocation or narrowing that is actually needed refuses.
    #[cfg(target_os = "linux")]
    #[test]
    fn unchangeable_objects_skip_grants_and_refuse_only_needed_revocations() {
        use std::os::fd::FromRawFd;
        if crate::sandbox::server_uid() == 0 {
            return; // a fully privileged server can change every object
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pack.promisor");
        std::fs::write(&path, b"").unwrap();
        let granted = 61_234;
        update_metadata_acl(
            &std::fs::File::open(&path).unwrap(),
            granted,
            Some(6),
            false,
            None,
        )
        .unwrap();
        let before = acl_entries(&path, c"system.posix_acl_access");
        // Owner class denied (so the owner cannot open it) while the ACL mask
        // keeps the named entry effective.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o060)).unwrap();
        assert_eq!(
            std::fs::File::open(&path).unwrap_err().raw_os_error(),
            Some(libc::EACCES)
        );
        let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        let fd = unsafe {
            libc::open(
                name.as_ptr(),
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        assert!(fd >= 0);
        let bound = unsafe { std::fs::File::from_raw_fd(fd) };
        assert!(path_only(&bound));
        // Adding (or re-adding equal) access is skipped without mutation.
        update_metadata_acl(&bound, granted, Some(6), false, None).unwrap();
        update_metadata_acl(&bound, granted, Some(7), false, None).unwrap();
        update_metadata_acl(&bound, 61_235, Some(6), false, None).unwrap();
        // A gid without an entry needs no revocation.
        update_metadata_acl(&bound, 61_235, None, false, None).unwrap();
        // Narrowing or revoking an existing entry is impossible: refuse.
        let narrow = update_metadata_acl(&bound, granted, Some(4), false, None).unwrap_err();
        assert!(narrow.to_string().contains("cannot narrow"), "{narrow}");
        let revoke = update_metadata_acl(&bound, granted, None, false, None).unwrap_err();
        assert!(revoke.to_string().contains("cannot narrow"), "{revoke}");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut after = acl_entries(&path, c"system.posix_acl_access");
        let mut expected = before.clone();
        // chmod rewrote only the owner/mask/other classes; named entries stand.
        after.retain(|e| e.0 == 8);
        expected.retain(|e| e.0 == 8);
        assert_eq!(after, expected, "an unchangeable object was modified");

        // A readable object owned by another principal behaves the same way.
        let foreign = std::fs::File::open("/etc/passwd").unwrap();
        assert!(!server_may_change(&foreign.metadata().unwrap()));
        update_metadata_acl(&foreign, granted, Some(6), false, None).unwrap();
        update_metadata_acl(&foreign, granted, None, false, None).unwrap();
    }

    /// Admission applies launch's trust predicate before writing anything: a
    /// common store writable by another principal is refused with the path and
    /// mode, and leaves no journal or grant behind.
    #[test]
    fn admission_refuses_an_untrusted_store_before_any_grant() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let work = dir.path().join("work");
        f.consumer
            .remote("ExampleOrg/demo", &work, "work", None)
            .unwrap();
        let common = git_dirs(&work).unwrap().last().unwrap().clone();
        let mode = std::fs::metadata(&common).unwrap().permissions().mode();
        std::fs::set_permissions(&common, std::fs::Permissions::from_mode(mode | 0o020)).unwrap();
        let id = anyagents_core::AgentId::generate();
        let journal = grant_path(dir.path(), &id);
        let uid = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid();
        let error = share(&work, uid, gid, &journal).unwrap_err();
        let text = error.to_string();
        assert!(matches!(error, Error::WorkdirPermission { .. }), "{text}");
        assert!(text.contains("untrusted Git metadata directory"), "{text}");
        assert!(text.contains(common.to_str().unwrap()), "{text}");
        assert!(text.contains("mode"), "{text}");
        assert!(!journal.exists(), "refused admission wrote a grant journal");
        assert!(
            std::fs::symlink_metadata(common.join("config"))
                .unwrap()
                .is_file(),
            "refused admission relocated the store's config"
        );
        // The same store is admitted once it is trustworthy.
        std::fs::set_permissions(&common, std::fs::Permissions::from_mode(mode)).unwrap();
        share(&work, uid, gid, &journal).unwrap();
        RecordedWorktree::load(dir.path(), &id, &work)
            .unwrap()
            .validate()
            .unwrap();
    }

    /// Relocating the config never narrows the store's existing read audience:
    /// checkouts the engine never admitted keep reading it through the common
    /// directory's group after the last grant is revoked, nobody but the owner
    /// can write it, and startup repairs a store that lost its readers.
    #[cfg(target_os = "linux")]
    #[test]
    fn managed_config_stays_readable_by_the_store_group_after_the_last_revocation() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let work = dir.path().join("work");
        f.consumer
            .remote("ExampleOrg/demo", &work, "work", None)
            .unwrap();
        let common = git_dirs(&work).unwrap().last().unwrap().clone();
        let mode = std::fs::metadata(&common).unwrap().permissions().mode();
        std::fs::set_permissions(&common, std::fs::Permissions::from_mode(mode | 0o050)).unwrap();
        let store_group = std::fs::metadata(&common).unwrap().gid();
        let id = anyagents_core::AgentId::generate();
        let uid = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid();
        share(&work, uid, gid, &grant_path(dir.path(), &id)).unwrap();
        revoke_recorded(dir.path(), &id).unwrap();

        let directory = common.join(CONFIG_DIRECTORY);
        let config = directory.join("config");
        let readable = |path: &Path, perm: u16| {
            acl_entries(path, c"system.posix_acl_access").contains(&(8, perm, store_group))
        };
        assert!(
            readable(&directory, 5),
            "{:?}",
            acl_entries(&directory, c"system.posix_acl_access")
        );
        assert!(
            readable(&config, 4),
            "{:?}",
            acl_entries(&config, c"system.posix_acl_access")
        );
        assert!(
            acl_entries(&directory, c"system.posix_acl_default").contains(&(8, 4, store_group)),
            "Git's lock+rename replacement must inherit the store reader"
        );
        let owners = [uid];
        assert!(!writable_file_by_untrusted(&open_nofollow(&directory).unwrap(), &owners).unwrap());
        assert!(!writable_file_by_untrusted(&open_nofollow(&config).unwrap(), &owners).unwrap());

        // Simulate the regression: every reader stripped from an adopted store.
        for path in [&directory, &config] {
            update_metadata_acl(
                &open_nofollow(path).unwrap(),
                store_group,
                None,
                false,
                None,
            )
            .unwrap();
            assert!(!readable(path, if path == &directory { 5 } else { 4 }));
        }
        config_default_readers(&open_nofollow(&directory).unwrap(), &[]).unwrap();
        recover_inspections(dir.path()).unwrap();
        assert!(readable(&directory, 5));
        assert!(readable(&config, 4));
        assert!(
            acl_entries(&directory, c"system.posix_acl_default").contains(&(8, 4, store_group))
        );

        // A store whose group cannot traverse it gains no store reader.
        assert_eq!(
            {
                std::fs::set_permissions(&common, std::fs::Permissions::from_mode(mode & !0o070))
                    .unwrap();
                store_group_reader(&common).unwrap()
            },
            None
        );
        std::fs::set_permissions(&common, std::fs::Permissions::from_mode(mode)).unwrap();
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
    #[test]
    fn explicit_author_configuration_is_local_and_partial_configuration_refuses() {
        if identity_test_worker(
            "explicit_author_configuration_is_local_and_partial_configuration_refuses",
        ) {
            return;
        }
        let _env = crate::server::tests::env_lock();
        let keys = ["ANYAGENTS_GIT_AUTHOR_NAME", "ANYAGENTS_GIT_AUTHOR_EMAIL"];
        let old = keys.map(std::env::var_os);
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("ExampleOrg/demo", &target, "child", None)
            .unwrap();
        let before = std::fs::read(f.source.join(".git/config")).unwrap();
        unsafe {
            std::env::set_var(keys[0], "Selected Author");
            std::env::remove_var(keys[1]);
        }
        assert!(set_worktree_identity(&target).is_err());
        assert_eq!(std::fs::read(f.source.join(".git/config")).unwrap(), before);
        unsafe {
            std::env::set_var(keys[1], "selected@example.test");
        }
        set_worktree_identity(&target).unwrap();
        assert_eq!(
            crate::worktree::tests::git(&target, &["config", "--worktree", "--get", "user.name"]),
            "Selected Author"
        );
        assert_eq!(
            crate::worktree::tests::git(&target, &["config", "--worktree", "--get", "user.email"]),
            "selected@example.test"
        );
        assert_eq!(std::fs::read(f.source.join(".git/config")).unwrap(), before);
        unsafe {
            for (key, value) in keys.into_iter().zip(old) {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    /// Provision a pool-backed per-seat repository and admit it as the
    /// server's own principal (the single-uid route). Returns the temp
    /// root, the seat checkout, the bare remote and the recorded agent id.
    fn pool_seat_checkout() -> (tempfile::TempDir, PathBuf, PathBuf, anyagents_core::AgentId) {
        let dir = tempfile::tempdir().unwrap();
        let remote = crate::worktree::tests::seed_bare_remote(dir.path());
        let consumer = crate::worktree::tests::pool_consumer(&dir.path().join("pool"));
        let target = dir.path().join("seat");
        consumer
            .remote(
                &format!("file://{}", remote.display()),
                &target,
                "agent/seat",
                Some("main"),
            )
            .unwrap();
        let id = anyagents_core::AgentId::generate();
        share(
            &target,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
            &grant_path(dir.path(), &id),
        )
        .unwrap();
        (dir, target, remote, id)
    }

    /// Every operation in the seat receipt exits zero through the recorded
    /// launch command, and none of them add objects to the pool.
    #[test]
    fn per_seat_receipt_ops_exit_zero_through_the_recorded_command() {
        let (dir, target, _remote, agent) = pool_seat_checkout();
        let pool_repo = dir.path().join("pool/ExampleOrg/demo.git");
        let before = crate::worktree::tests::pool_object_count(&pool_repo);
        let record = RecordedWorktree::load(dir.path(), &agent, &target).unwrap();
        record.validate().unwrap();
        let run = |args: &[&str]| -> std::process::Output {
            record.command().unwrap().args(args).output().unwrap()
        };
        let assert_ok = |args: &[&str]| {
            let out = run(args);
            assert!(
                out.status.success(),
                "{args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        assert_ok(&[
            "-c",
            "user.name=Seat Author",
            "-c",
            "user.email=seat@example.test",
            "commit",
            "--allow-empty",
            "-m",
            "Seat work",
        ]);
        assert_ok(&["push", "-q", "-u", "origin", "HEAD:refs/heads/agent/seat"]);
        assert_eq!(
            String::from_utf8_lossy(&run(&["config", "--get", "branch.agent/seat.remote"]).stdout)
                .trim(),
            "origin"
        );
        assert_ok(&["branch", "--set-upstream-to=origin/main"]);
        assert_ok(&["config", "--worktree", "seat.k", "v"]);
        assert_eq!(
            String::from_utf8_lossy(&run(&["config", "--worktree", "--get", "seat.k"]).stdout)
                .trim(),
            "v"
        );
        assert_ok(&["branch", "packed-local", "origin/packed-local"]);
        assert_ok(&["pack-refs", "--all"]);
        crate::worktree::tests::git(&_remote, &["branch", "-D", "stale"]);
        assert_ok(&["fetch", "-q", "--prune", "origin"]);
        assert!(
            !run(&["show-ref", "--verify", "refs/remotes/origin/stale"])
                .status
                .success(),
            "pruned tracking ref must be gone"
        );
        assert_ok(&["branch", "-D", "packed-local"]);
        assert_ok(&["gc"]);
        assert_ok(&["status", "--porcelain"]);
        assert_eq!(
            crate::worktree::tests::pool_object_count(&pool_repo),
            before,
            "receipt must not add objects to the pool"
        );
        revoke_recorded(dir.path(), &agent).unwrap();
        assert_eq!(
            crate::worktree::tests::pool_object_count(&pool_repo),
            before
        );
    }

    /// A seat-owned config naming an executable fsmonitor (and a hooks
    /// directory) is refused by server-side validation and never executed:
    /// no marker file appears and launch reads stay refused.
    #[test]
    fn hostile_fsmonitor_and_hook_are_refused_and_never_executed() {
        let (dir, target, _remote, agent) = pool_seat_checkout();
        let evil = dir.path().join("evil");
        std::fs::write(
            &evil,
            format!("#!/bin/sh\ntouch {}/pwned\n", dir.path().display()),
        )
        .unwrap();
        std::fs::set_permissions(&evil, std::fs::Permissions::from_mode(0o755)).unwrap();
        let hooks = dir.path().join("hooks");
        std::fs::create_dir(&hooks).unwrap();
        std::fs::copy(&evil, hooks.join("post-checkout")).unwrap();
        std::fs::set_permissions(
            hooks.join("post-checkout"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        crate::worktree::tests::git(
            &target,
            &["config", "core.fsmonitor", &evil.display().to_string()],
        );
        crate::worktree::tests::git(
            &target,
            &["config", "core.hooksPath", &hooks.display().to_string()],
        );
        let err = validate_git_config(&target).unwrap_err();
        assert!(err.to_string().contains("fsmonitor"), "{err}");
        let record = RecordedWorktree::load(dir.path(), &agent, &target).unwrap();
        assert!(record.validate().is_err());
        assert!(record.command().is_err());
        assert!(!dir.path().join("pwned").exists(), "fsmonitor must not run");
        // A server-uid status read is refused by validation itself, so the
        // hook path is not reachable through server commands either.
        assert!(record.command().is_err());
        assert!(!dir.path().join("pwned").exists());
        // The seat can repair its own config; launch works again without
        // running the hook. `checkout` would run post-checkout if server
        // commands honored the seat's hooksPath.
        crate::worktree::tests::git(&target, &["config", "--unset", "core.fsmonitor"]);
        record.validate().unwrap();
        let out = record
            .command()
            .unwrap()
            .args(["checkout", "--detach", "HEAD"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!dir.path().join("pwned").exists(), "hook must not run");
    }
}
