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
    let git = anyagents_toolchain::tool("git", "checkout").map_err(|e| refusal(path, e))?;
    let mut dirs = Vec::new();
    for flag in ["--git-dir", "--git-common-dir"] {
        let out = git
            .command()
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
    let git = anyagents_toolchain::tool("git", "checkout").map_err(|e| refusal(path, e))?;
    if private == common {
        // Per-seat repositories carry their own identity: the pool never
        // touches them, so no consumer extension is required. Record the
        // worktree-config extension and the identity in the seat's own
        // config.worktree.
        validate_config_files(private, common)?;
        let config = common.join("config");
        let extension = anyagents_toolchain::tool("git", "checkout")
                .map_err(|e| refusal(&path, e))?
                .command()
            .args(["config", "--no-includes", "--file"])
            .arg(&config)
            .args(["extensions.worktreeConfig", "true"])
            .output()?;
        if !extension.status.success() {
            return Err(refusal(path, "cannot enable extensions.worktreeConfig"));
        }
    } else {
        validate_config_files(private, common)?;
        let extension = anyagents_toolchain::tool("git", "checkout")
                .map_err(|e| refusal(&path, e))?
                .command()
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
        let out = git
            .command()
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
            let mut child = anyagents_toolchain::tool("git", "checkout")
                .map_err(|e| refusal(&path, e))?
                .command()
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
    let path_env = anyagents_toolchain::global().exec_path("worktree_access");
    let git_tool = anyagents_toolchain::tool("git", "checkout").map_err(|e| refusal(config, e))?;
    let staging = data.join("worktree-grants");
    std::fs::create_dir_all(&staging)?;
    let staging = std::fs::canonicalize(staging)?;
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
        let git = g.checkout.join(".git");
        let gitdir = std::fs::canonicalize(&git).unwrap_or(git);
        if g.private != g.common || gitdir != g.private {
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