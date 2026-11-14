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
    if private == common {
        return Err(refusal(path, "identity requires a linked worktree"));
    }
    validate_config_files(private, common)?;
    let git = anyagents_toolchain::tool("git", "checkout").map_err(|e| refusal(path, e))?;
    let extension = git
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
    use std::os::unix::fs::OpenOptionsExt;
    for path in [
        common.join("config"),
        common.join("config.worktree"),
        private.join("config.worktree"),
    ] {
        // Only an absent pathname is skipped, never a managed link whose
        // directory or file has gone missing.
        if std::fs::symlink_metadata(&path)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            continue;
        }
        let file = if path == common.join("config") {
            match open_common_config(common) {
                Ok(CommonConfig::Plain(file) | CommonConfig::Linked(file)) => file,
                Ok(CommonConfig::Managed { file, .. }) => file,
                Err(e) => return Err(refusal(&path, e)),
            }
        } else {
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)
                .map_err(|e| refusal(&path, e))?
        };
        if !file.metadata()?.is_file() {
            return Err(refusal(&path, "Git configuration is not a regular file"));
        }
        // --file - and --no-includes inspect only the opened input. Never
        // follow the include or print config values in a diagnostic.
        let out = anyagents_toolchain::tool("git", "checkout")
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
/// A journal records one agent's grant and nothing about the shared store: the
/// managed config witnesses itself (see [`open_common_config`]), so a settled
/// journal can be removed without losing any provenance.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Grant {
    common: PathBuf,
    gid: u32,
    uid: u32,
    private: PathBuf,
    checkout: PathBuf,
    active: bool,
    #[serde(default)]
    inspection: bool,
    #[serde(default)]
    inspection_paths: Option<Vec<InspectionPath>>,
    #[serde(default)]
    quarantined: bool,
    /// Why the journal is quarantined, for the operator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl Grant {
    /// A grant to the server's own uid and group (a seatless agent): the agent
    /// runs as the server itself, so there is no boundary to open and no
    /// access to take away. Revoking it would strip the server's own named
    /// entries from objects seats created.
    fn is_server_principal(&self) -> bool {
        self.uid == crate::sandbox::server_uid() && self.gid == crate::sandbox::server_gid()
    }

    fn quarantine(&mut self, reason: impl std::fmt::Display) {
        self.quarantined = true;
        self.reason = Some(reason.to_string());
    }
}

/// An object recorded before a temporary inspection grant. Its inode is
/// compared on the common store's current file system: device numbers change
/// when a volume is attached again (this desk's hub volume went from device
/// 65008 to 64784 between two pod starts), inode numbers do not.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct InspectionPath {
    path: PathBuf,
    ino: u64,
}

// Git resolves a config symlink before creating config.lock. Keep inheritance
// confined to this single-purpose directory, never the mixed common gitdir.
const CONFIG_DIRECTORY: &str = "anyagents-config";
// Adoption stages its symlink under one fixed name, so an interrupted
// adoption leaves at most one, which the next adoption replaces.
const CONFIG_LINK_STAGE: &str = ".anyagents-config.link";

/// A hub's common `config` as the hub itself shows it. No journal is
/// consulted: a managed config is recognized by its exact shape and by the
/// server's ownership of the link, the directory and the file, which only the
/// server (or root) can produce. Journals, whole data directories and device
/// numbers have all been lost or changed on a live desk; none of that can make
/// a hub unusable now.
enum CommonConfig {
    /// An ordinary file owned by the store's owner or the server.
    Plain(std::fs::File),
    /// The server's interrupted adoption: its own file is also hard-linked as
    /// `anyagents-config/config`. The next adoption finishes it.
    Linked(std::fs::File),
    /// `config -> anyagents-config/config`, all three the server's own.
    Managed {
        file: std::fs::File,
        directory: std::fs::File,
    },
}

impl CommonConfig {
    /// The lock Git (and every lifecycle transition) takes to replace it.
    fn lock_path(&self, common: &Path) -> PathBuf {
        match self {
            Self::Managed { .. } => common.join(CONFIG_DIRECTORY).join("config.lock"),
            Self::Plain(_) | Self::Linked(_) => common.join("config.lock"),
        }
    }
}

fn open_common_config(common: &Path) -> std::io::Result<CommonConfig> {
    open_common_config_as(common, crate::sandbox::server_uid())
}

/// [`open_common_config`] for a given server uid, so the ownership rule is
/// testable without a second principal. The link witnesses the relocation:
/// only the server makes it. The directory and the file belong to the server
/// or to the store's owner, whose own Git keeps rewriting its configuration
/// through the link (lock and rename inside the directory). Nobody else may
/// write any of them, a trusted metadata group included.
fn open_common_config_as(common: &Path, server: u32) -> std::io::Result<CommonConfig> {
    use std::os::unix::fs::MetadataExt;
    let path = common.join("config");
    let link = std::fs::symlink_metadata(&path)?;
    let owners = [open_nofollow(common)?.metadata()?.uid(), server];
    if link.file_type().is_symlink() {
        if std::fs::read_link(&path)? != Path::new(CONFIG_DIRECTORY).join("config") {
            return Err(std::io::Error::other(format!(
                "{} is a symlink, but not the managed link to {CONFIG_DIRECTORY}/config",
                path.display()
            )));
        }
        if link.uid() != server || link.nlink() != 1 {
            return Err(std::io::Error::other(format!(
                "managed Git config link {} belongs to uid {}, not the server (uid {server}): \
                 only the server's own relocation is trusted",
                path.display(),
                link.uid()
            )));
        }
        let directory = managed_directory(common, &owners).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                std::io::Error::other(format!(
                    "managed Git config link {} has no {CONFIG_DIRECTORY}/ directory",
                    path.display()
                ))
            } else {
                error
            }
        })?;
        let file = open_at_nofollow(&directory, c"config")?;
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.nlink() != 1
            || !owners.contains(&meta.uid())
            || writable_by_anyone_else(&file, &owners)?
        {
            return Err(std::io::Error::other(format!(
                "untrusted managed Git config {}/config: it must be a single-link regular \
                 file owned by the server (uid {server}) or the store's owner (uid {}) that \
                 no one else can write",
                common.join(CONFIG_DIRECTORY).display(),
                owners[0]
            )));
        }
        return Ok(CommonConfig::Managed { file, directory });
    }
    let file = open_nofollow(&path)?;
    let meta = file.metadata()?;
    // Only an interrupted relocation explains a second link.
    if meta.is_file()
        && meta.nlink() == 2
        && owners.contains(&meta.uid())
        && let Ok(directory) = managed_directory(common, &owners)
        && let Ok(linked) = open_at_nofollow(&directory, c"config").and_then(|f| f.metadata())
        && linked.dev() == meta.dev()
        && linked.ino() == meta.ino()
        && !writable_by_anyone_else(&file, &owners)?
    {
        return Ok(CommonConfig::Linked(file));
    }
    if !meta.is_file() || meta.nlink() != 1 || !owners.contains(&meta.uid()) {
        return Err(std::io::Error::other("untrusted Git configuration"));
    }
    if writable_by_anyone_else(&file, &owners)? {
        return Err(std::io::Error::other(format!(
            "Git configuration {} is writable by someone other than its owner (mode {:04o}): \
             any seat in its group could rewrite it, and a trusted metadata group is no \
             exception for configuration. Its owner repairs it with `chmod go-w {}`",
            path.display(),
            meta.mode() & 0o7777,
            path.display()
        )));
    }
    Ok(CommonConfig::Plain(file))
}

/// Configuration and its directory admit exactly their owners as writers. A
/// deployment's trusted metadata groups (`ANYAGENTS_TRUSTED_METADATA_GIDS`)
/// are no exception: every seat carries such a group, so a group-writable
/// file could have been written by any of them, and a server-owned one moved
/// into place by any seat that can rename in the common directory.
fn writable_by_anyone_else(file: &std::fs::File, owners: &[u32]) -> std::io::Result<bool> {
    writable_file_by_untrusted_with(file, owners, &[])
}

/// The dedicated directory: owned by the server or the store's owner
/// (`owners`), writable by no one else, holding nothing but the config and
/// Git's lock for it.
fn managed_directory(common: &Path, owners: &[u32]) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::MetadataExt;
    let path = common.join(CONFIG_DIRECTORY);
    let directory = open_nofollow(&path)?;
    let meta = directory.metadata()?;
    if !meta.is_dir()
        || !owners.contains(&meta.uid())
        || writable_by_anyone_else(&directory, owners)?
    {
        return Err(std::io::Error::other(format!(
            "untrusted managed Git config directory {}: it must be a directory owned by the \
             server or the store's owner (uids {owners:?}) that no one else can write",
            path.display()
        )));
    }
    if let Some(stray) = entry_names(&directory)?
        .into_iter()
        .find(|name| name != "config" && name != "config.lock")
    {
        return Err(std::io::Error::other(format!(
            "managed Git config directory {} holds an unexpected entry {stray:?}",
            path.display()
        )));
    }
    Ok(directory)
}

fn open_at_nofollow(
    directory: &std::fs::File,
    name: &std::ffi::CStr,
) -> std::io::Result<std::fs::File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

/// The names in an opened directory, read through that descriptor.
fn entry_names(directory: &std::fs::File) -> std::io::Result<Vec<std::ffi::OsString>> {
    use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};
    // fdopendir owns only the duplicate; closing it does not close `directory`.
    let raw = unsafe { libc::dup(directory.as_raw_fd()) };
    if raw < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let dir = unsafe { libc::fdopendir(raw) };
    if dir.is_null() {
        let error = std::io::Error::last_os_error();
        unsafe { libc::close(raw) };
        return Err(error);
    }
    struct Directory(*mut libc::DIR);
    impl Drop for Directory {
        fn drop(&mut self) {
            unsafe { libc::closedir(self.0) };
        }
    }
    let dir = Directory(dir);
    // The duplicate shares the file offset: read from the start.
    unsafe { libc::rewinddir(dir.0) };
    let mut names = Vec::new();
    loop {
        #[cfg(target_os = "linux")]
        let errno = unsafe { libc::__errno_location() };
        #[cfg(not(target_os = "linux"))]
        let errno = unsafe { libc::__error() };
        unsafe { *errno = 0 };
        let entry = unsafe { libc::readdir(dir.0) };
        if entry.is_null() {
            if unsafe { *errno } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            return Ok(names);
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.push(std::ffi::OsStr::from_bytes(name).to_os_string());
        }
    }
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
        .open(&path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                std::io::Error::new(
                    error.kind(),
                    format!(
                        "{} exists: a Git process is writing this config; if none is \
                         running, the lock is stale and its owner must remove it",
                        path.display()
                    ),
                )
            } else {
                error
            }
        })?;
    Ok(ConfigLock { _file: file, path })
}

/// Caller holds the journal lock. Relocate the server's own plain config into
/// the dedicated directory (or finish an interrupted relocation), then give
/// `readers` read access to it. A config another principal owns is left in
/// place: only the server's own objects can witness a relocation.
fn manage_config(common: &Path, readers: &[u32]) -> std::io::Result<()> {
    let relocated = relocate_config(common, &|| Ok(()))?;
    if !matches!(open_common_config(common)?, CommonConfig::Managed { .. }) {
        return Ok(()); // not the server's to relocate
    }
    let _lock = match relocated {
        Some(lock) => lock,
        None => lock_config(common.join(CONFIG_DIRECTORY).join("config.lock"))?,
    };
    let CommonConfig::Managed { file, directory } = open_common_config(common)? else {
        return Err(std::io::Error::other(
            "managed Git config replaced while locked",
        ));
    };
    // Relocation must never narrow who could already read the config: Git
    // checkouts the engine did not admit (made by an external provider) read
    // it through the common directory's own group. Read only, never write.
    let mut readers = readers.to_vec();
    readers.extend(store_group_reader(common)?);
    for gid in &readers {
        update_metadata_acl(&directory, *gid, Some(5), false, None)?;
        update_metadata_acl(&file, *gid, Some(4), false, None)?;
    }
    config_default_readers(&directory, &readers)
}

/// Move the store's plain config into `anyagents-config/` behind an exact
/// symlink the server makes, or finish an interrupted move. The directory and
/// the file stay with the config's owner, whose Git keeps writing through the
/// link. Git's `config.lock` on the
/// original path is held throughout, and the new name's lock from before the
/// link is published: the caller keeps that one (the returned lock) while it
/// sets the readers, so no Git write through the link precedes them. Every
/// step leaves a state [`open_common_config`] accepts (plain, linked, or
/// managed), so an interruption never loses bytes or strands the hub; the
/// next call resumes. `None`: nothing was moved now.
fn relocate_config(
    common: &Path,
    before_publish: &dyn Fn() -> std::io::Result<()>,
) -> std::io::Result<Option<ConfigLock>> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    let owners = [
        open_nofollow(common)?.metadata()?.uid(),
        crate::sandbox::server_uid(),
    ];
    if matches!(open_common_config(common)?, CommonConfig::Managed { .. }) {
        return Ok(None);
    }
    let _source_lock = lock_config(common.join("config.lock"))?;
    // Classify under Git's lock: a writer may have replaced the file.
    let source = match open_common_config(common)? {
        CommonConfig::Managed { .. } => return Ok(None),
        CommonConfig::Plain(file) | CommonConfig::Linked(file) => file,
    };
    let meta = source.metadata()?;
    let path = common.join(CONFIG_DIRECTORY);
    let created = match std::fs::DirBuilder::new().mode(0o700).create(&path) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => return Err(error),
    };
    let directory = managed_directory(common, &owners)?;
    let same = |file: &std::fs::File| {
        file.metadata()
            .is_ok_and(|m| m.is_file() && m.dev() == meta.dev() && m.ino() == meta.ino())
    };
    // An existing directory is used only as this relocation's own interrupted
    // state: still empty, or already holding this very inode. Anything else
    // (a directory moved in from elsewhere) is refused, never filled.
    if !created {
        let entries = entry_names(&directory)?;
        let interrupted = entries.is_empty()
            || entries == ["config"]
                && open_at_nofollow(&directory, c"config").is_ok_and(|f| same(&f));
        if !interrupted {
            return Err(std::io::Error::other(format!(
                "{} exists and is not an interrupted relocation of {}; move it aside to \
                 relocate the config",
                path.display(),
                common.join("config").display()
            )));
        }
    }
    // Owner-only, as created (keeping a set-group-ID bit inherited from the
    // common directory); readers get named ACL entries below.
    let directory_mode = directory.metadata()?.mode() & 0o7777;
    if directory_mode & 0o7777 != (directory_mode & 0o2000) | 0o700 {
        descriptor_fchmod(
            &directory,
            ((directory_mode & 0o2000) | 0o700) as libc::mode_t,
        )?;
    }
    clear_default_acl(&directory)?;
    // The config's owner (the store's owner or the server) owns the directory:
    // its Git creates config.lock there and renames it over the config.
    let directory_meta = directory.metadata()?;
    if directory_meta.uid() != meta.uid() || directory_meta.gid() != meta.gid() {
        descriptor_fchown(&directory, Some(meta.uid()), Some(meta.gid()))?;
    }
    let target_lock = lock_config(path.join("config.lock"))?;
    match open_at_nofollow(&directory, c"config") {
        Ok(linked) if same(&linked) => {}
        Ok(_) => {
            return Err(std::io::Error::other(format!(
                "{} already holds a different config; move it aside to relocate {}",
                path.join("config").display(),
                common.join("config").display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::hard_link(common.join("config"), path.join("config"))?;
            if !open_at_nofollow(&directory, c"config").is_ok_and(|f| same(&f)) {
                let _ = std::fs::remove_file(path.join("config"));
                return Err(std::io::Error::other("Git config changed during adoption"));
            }
        }
        Err(error) => return Err(error),
    }
    // Only its owner writes the relocated config: no group or other write,
    // whatever mode Git or an ACL mask gave the original. Readers get named
    // ACL entries afterwards, so the mask set here never hides them.
    let mode = source.metadata()?.mode() & 0o7777;
    if mode & 0o022 != 0 {
        descriptor_fchmod(&source, (mode & !0o022) as libc::mode_t)?;
    }
    source.sync_all()?;
    directory.sync_all()?;
    before_publish()?;
    let stage = common.join(CONFIG_LINK_STAGE);
    match std::fs::symlink_metadata(&stage) {
        Ok(meta) if meta.file_type().is_symlink() => std::fs::remove_file(&stage)?,
        Ok(_) => {
            return Err(std::io::Error::other(format!(
                "{} exists and is not a symlink",
                stage.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    std::os::unix::fs::symlink(Path::new(CONFIG_DIRECTORY).join("config"), &stage)?;
    std::fs::rename(&stage, common.join("config"))?;
    open_nofollow(common)?.sync_all()?;
    Ok(Some(target_lock))
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
            if !no_acl(&error) {
                return Err(error);
            }
            ((meta.mode() >> 3) & 7) as u16
        }
    };
    #[cfg(not(target_os = "linux"))]
    let group_permissions = ((meta.mode() >> 3) & 7) as u16;
    Ok((group_permissions & 5 == 5).then_some(meta.gid()))
}

/// The seat groups that read a store's managed config: every active,
/// settled grant on it, except the server's own.
fn config_readers(journal: &Path, common: &Path) -> std::io::Result<Vec<u32>> {
    let mut readers: Vec<u32> = grants_on(journal, common)?
        .into_iter()
        .filter(|g| g.active && !g.quarantined && !g.inspection && !g.is_server_principal())
        .map(|g| g.gid)
        .collect();
    readers.sort_unstable();
    readers.dedup();
    Ok(readers)
}

/// Recompute a managed config's readers without relocating anything: the
/// store group plus every still-active grant. Runs after a revocation and at
/// startup so the last revocation never leaves provider checkouts unreadable.
fn refresh_store_config_readers(journal: &Path, common: &Path) -> std::io::Result<()> {
    match open_common_config(common) {
        Ok(CommonConfig::Managed { .. }) => {}
        Ok(_) => return Ok(()),
        // A store removed from disk has no readers left to keep.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    manage_config(common, &config_readers(journal, common)?)
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

/// Why a journal refuses launch and reaping, in the operator's terms.
fn quarantine_message(grant: &Grant) -> String {
    match (&grant.reason, grant.quarantined) {
        (Some(reason), true) => format!("worktree is quarantined: {reason}"),
        (None, true) => "worktree is quarantined after incomplete ACL recovery".into(),
        (_, false) => "worktree has incomplete ACL recovery".into(),
    }
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
        return Err(refusal(&journal, quarantine_message(&grant)));
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

/// No ACL xattr: none set (`ENODATA`), or a file system without POSIX ACL
/// support (`EOPNOTSUPP`), where the mode bits are the whole policy.
#[cfg(target_os = "linux")]
fn no_acl(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::ENODATA | libc::EOPNOTSUPP))
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

/// Groups a deployment declares as one trust domain with the server for
/// shared Git metadata (`ANYAGENTS_TRUSTED_METADATA_GIDS`, comma-separated
/// numeric gids or group names). Empty by default, so a group-writable store is
/// untrusted unless the deployment says otherwise: on a desk where every seat
/// shares one set of Git hubs by design, those seats already trust each other's
/// writes to that metadata, and refusing it only blocks every checkout.
fn trusted_metadata_gids() -> &'static [u32] {
    static GIDS: std::sync::OnceLock<Vec<u32>> = std::sync::OnceLock::new();
    GIDS.get_or_init(|| {
        std::env::var("ANYAGENTS_TRUSTED_METADATA_GIDS")
            .ok()
            .map(|raw| parse_trusted_gids(&raw))
            .unwrap_or_default()
    })
}

fn parse_trusted_gids(raw: &str) -> Vec<u32> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .filter_map(|item| {
            item.parse::<u32>().ok().or_else(|| {
                let name = std::ffi::CString::new(item).ok()?;
                // SAFETY: getgrnam reads a NUL-terminated name and returns a
                // pointer into static storage that is read before any other call.
                let entry = unsafe { libc::getgrnam(name.as_ptr()) };
                (!entry.is_null()).then(|| unsafe { (*entry).gr_gid })
            })
        })
        .collect()
}

fn writable_file_by_untrusted(file: &std::fs::File, owners: &[u32]) -> std::io::Result<bool> {
    writable_file_by_untrusted_with(file, owners, trusted_metadata_gids())
}

fn writable_file_by_untrusted_with(
    file: &std::fs::File,
    owners: &[u32],
    trusted_gids: &[u32],
) -> std::io::Result<bool> {
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
            return Ok(entries.iter().any(|(tag, perm, id)| match tag {
                2 => perm & mask & 2 != 0 && !owners.contains(id),
                4 => perm & mask & 2 != 0 && !trusted_gids.contains(&meta.gid()),
                8 => perm & mask & 2 != 0 && !trusted_gids.contains(id),
                32 => perm & 2 != 0,
                _ => false,
            }));
        }
        let err = std::io::Error::last_os_error();
        if !no_acl(&err) {
            return Err(err);
        }
    }
    Ok(meta.mode() & 0o002 != 0
        || (meta.mode() & 0o020 != 0 && !trusted_gids.contains(&meta.gid())))
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
                "is writable by principals other than uid {owner} (mode {:04o}{}); a \
                 deployment whose seats share this store by design names that group in \
                 ANYAGENTS_TRUSTED_METADATA_GIDS",
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

/// Shared Git metadata that everyone may write is never admitted: any local
/// user could rewrite the store's objects, refs, history, hooks or excludes,
/// and a default ACL would hand the same write to every object a seat adds
/// later. Admission checks these names first, then every object of the store
/// during its own walk (see [`revoke_common`]). The refusal names the path
/// and the owner's repair.
fn refuse_world_writable(common: &Path) -> std::io::Result<()> {
    for name in [
        "",
        "worktrees",
        "logs",
        "logs/refs",
        "objects",
        "refs",
        "packed-refs",
        "HEAD",
        "hooks",
        "info",
    ] {
        let path = if name.is_empty() {
            common.to_path_buf()
        } else {
            common.join(name)
        };
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_symlink() => continue, // never followed
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        }
        let label = if name.is_empty() {
            "the common Git directory"
        } else {
            name
        };
        refuse_world_writable_object(&open_nofollow(&path)?, &path, Some(label))?;
    }
    Ok(())
}

/// One opened object everyone may write, by mode or by a directory's default
/// ACL `other` entry, is refused with its path and the owner's repair.
fn refuse_world_writable_object(
    file: &std::fs::File,
    path: &Path,
    label: Option<&str>,
) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata()?;
    let access = meta.mode() & 0o002 != 0;
    let default = meta.is_dir() && default_acl_lets_everyone_write(file)?;
    if !access && !default {
        return Ok(());
    }
    let chmod = if meta.is_dir() {
        format!("`chmod -R o-w {}`", path.display())
    } else {
        format!("`chmod o-w {}`", path.display())
    };
    let (what, repair) = match (access, default) {
        (true, false) => (format!("mode {:04o}", meta.mode() & 0o7777), chmod),
        (false, _) => (
            "its default ACL gives every new entry write for everyone (other::w)".to_string(),
            format!(
                "`setfacl -d -m o::rx` on {} and each directory under it",
                path.display()
            ),
        ),
        (true, true) => (
            format!(
                "mode {:04o}, and its default ACL gives every new entry write for everyone",
                meta.mode() & 0o7777
            ),
            format!("{chmod}, then `setfacl -d -m o::rx` on it and each directory under it"),
        ),
    };
    let named = match label {
        Some(label) => format!("{label} ({})", path.display()),
        None => path.display().to_string(),
    };
    Err(std::io::Error::other(format!(
        "{named} is world-writable ({what}): any local user could rewrite this repository's \
         Git metadata. Its owner (root, for objects a seat owns) repairs it with {repair}"
    )))
}

#[cfg(target_os = "linux")]
fn default_acl_lets_everyone_write(file: &std::fs::File) -> std::io::Result<bool> {
    let mut bytes = vec![0u8; 65536];
    let len = read_xattr(file, c"system.posix_acl_default", &mut bytes);
    if len < 0 {
        let error = std::io::Error::last_os_error();
        return if no_acl(&error) {
            Ok(false)
        } else {
            Err(error)
        };
    }
    bytes.truncate(len as usize);
    if bytes.len() < 4 || bytes[..4] != 2u32.to_le_bytes() || !(bytes.len() - 4).is_multiple_of(8) {
        return Err(std::io::Error::other("invalid default ACL"));
    }
    Ok(bytes[4..].as_chunks::<8>().0.iter().any(|e| {
        u16::from_le_bytes([e[0], e[1]]) == 32 && u16::from_le_bytes([e[2], e[3]]) & 2 != 0
    }))
}

#[cfg(not(target_os = "linux"))]
fn default_acl_lets_everyone_write(_file: &std::fs::File) -> std::io::Result<bool> {
    Ok(false)
}

#[cfg(target_os = "linux")]
fn has_access_acl(path: &Path) -> std::io::Result<bool> {
    let file = open_nofollow(path)?;
    let mut bytes = vec![0u8; 65536];
    if read_xattr(&file, c"system.posix_acl_access", &mut bytes) >= 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if no_acl(&error) {
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
        use std::os::unix::fs::MetadataExt;
        let g = &self.grant;
        let current: Grant = serde_json::from_slice(&std::fs::read(&self.journal)?)?;
        if current.quarantined {
            return Err(std::io::Error::other(quarantine_message(&current)));
        }
        let owner = trusted_metadata_layout(&g.common, &g.private, g.uid)?;
        open_common_config(&g.common)?;
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

    /// A store admitted before relocation is relocated once under the
    /// lifecycle lock. After that, Git's own lock+rename inherits reader ACLs
    /// immediately, without a wake. The server's own grant reads as the owner.
    fn refresh_config_readers(&self) -> std::io::Result<()> {
        self.validate()?;
        if self.grant.is_server_principal() {
            return Ok(());
        }
        manage_config(
            &self.grant.common,
            &config_readers(&self.journal, &self.grant.common)?,
        )
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
            return Err(std::io::Error::other(quarantine_message(&current)));
        }
        if current.active || g.uid == crate::sandbox::server_uid() {
            if current.active {
                self.refresh_config_readers()?;
            }
            drop(lock);
            return read();
        }
        let retained = other_active_grants(&self.journal, g)?;
        use std::os::unix::fs::MetadataExt;
        let store_dev = open_nofollow(&g.common)?.metadata()?.dev();
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
            let meta = file.metadata()?;
            // The server cannot grant on another principal's object; such a
            // grant is skipped, so it needs no inspection intent or revocation.
            // Nothing on another file system is ever granted.
            if path_only(file) || !server_may_change(&meta) || meta.dev() != store_dev {
                return Ok(());
            }
            files.borrow_mut().push((
                InspectionPath {
                    path: path.to_path_buf(),
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
            update_metadata_acl(
                &target.open(store_dev)?,
                g.gid,
                Some(*permissions),
                false,
                None,
            )
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
            current.quarantine(format!(
                "temporary cleanup read access could not be revoked: {error}"
            ));
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
        let git = anyagents_toolchain::tool("git", "checkout").map_err(std::io::Error::other)?;
        let g = &self.grant;
        let mut cmd = if unsafe { libc::geteuid() } == g.uid && unsafe { libc::getegid() } == g.gid
        {
            git.command()
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
            ]);
            cmd.arg(git.path());
            cmd
        };
        cmd.env_clear()
            .current_dir("/")
            .env(
                "PATH",
                anyagents_toolchain::global().exec_path("worktree_access"),
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
    revoke_locked(&journal)
}

/// Caller holds the journal lock.
fn revoke_locked(journal: &Path) -> Result<(), Error> {
    let bytes = match std::fs::read(journal) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(refusal(journal, e)),
    };
    let mut grant: Grant = serde_json::from_slice(&bytes).map_err(|e| refusal(journal, e))?;
    if !grant.active && !grant.inspection {
        return Ok(());
    }
    // The server's own grant opened nothing, so there is nothing to take back.
    if !grant.is_server_principal() {
        let retained = other_active_grants(journal, &grant).map_err(|e| refusal(journal, e))?;
        revoke_common(&grant.common, grant.uid, grant.gid, &retained, None)
            .map_err(|e| refusal(&grant.common, e))?;
    }
    grant.active = false;
    grant.inspection = false;
    write_grant(journal, &grant).map_err(|e| refusal(journal, e))?;
    // The revocation itself is complete and durable. A busy config lock only
    // delays the reader refresh to the next admission or server start.
    if let Err(error) = refresh_store_config_readers(journal, &grant.common) {
        eprintln!(
            "warning: managed Git config readers of {} not refreshed: {error}",
            grant.common.display()
        );
    }
    Ok(())
}

/// Keep a journal whose grant could not be revoked, marked so that nothing
/// launches, reaps or retries it until an operator has looked: `reason` says
/// what failed. An unreadable journal already blocks both.
pub(crate) fn quarantine_recorded(
    data: &Path,
    id: &anyagents_core::AgentId,
    reason: impl std::fmt::Display,
) -> Result<(), Error> {
    let journal = grant_path(data, id);
    let _lock = lock_journals(&journal).map_err(|e| refusal(&journal, e))?;
    let bytes = match std::fs::read(&journal) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(refusal(&journal, e)),
    };
    let Ok(mut grant) = serde_json::from_slice::<Grant>(&bytes) else {
        return Ok(());
    };
    grant.quarantine(reason);
    write_grant(&journal, &grant).map_err(|e| refusal(&journal, e))
}

/// Remove a journal that witnesses nothing any more: its grant is revoked, no
/// inspection or quarantine is pending, and its checkout is gone, so no
/// cleanup can still need the record. Returns whether it was removed.
pub(crate) fn retire_settled(data: &Path, id: &anyagents_core::AgentId) -> Result<bool, Error> {
    let journal = grant_path(data, id);
    let _lock = lock_journals(&journal).map_err(|e| refusal(&journal, e))?;
    retire_locked(&journal).map_err(|e| refusal(&journal, e))
}

/// Caller holds the journal lock. A checkout that still exists keeps its
/// journal even without a `.git` pointer: cleanup must never mistake a
/// journaled checkout for a disposable directory.
fn retire_locked(journal: &Path) -> std::io::Result<bool> {
    let grant: Grant = match std::fs::read(journal) {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    let checkout_gone = std::fs::symlink_metadata(&grant.checkout)
        .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound);
    if grant.active || grant.inspection || grant.quarantined || !checkout_gone {
        return Ok(false);
    }
    std::fs::remove_file(journal)?;
    std::fs::File::open(journal.parent().unwrap())?.sync_all()?;
    Ok(true)
}

/// What became of the agent a grant journal belongs to.
pub(crate) enum JournalOwner {
    /// Its row exists and was admitted: its lifecycle owns the journal.
    Live,
    /// Its row is gone. Its checkout may hold work and is never removed here.
    Deleted,
    /// A creation that failed before admission: it never ran, so its
    /// checkout is only what that creation provisioned.
    FailedCreation,
}

/// Once per server start, from the first maintenance pass: bring every
/// journal whose agent can never run again to a final state. A grant it still
/// holds is revoked. A failed creation's checkout is removed as its rollback
/// should have (see [`remove_failed_checkout`]). The journal is then removed
/// once its checkout is gone. Whatever cannot be revoked or removed
/// quarantines the journal with the reason. Nothing here is retried later.
/// Every quarantined or unreadable journal is listed, one line each, so an
/// operator sees what waits for them. Only server-owned journals in a
/// server-owned directory that no one else can write are acted on.
/// `protected`: whether the directory with this (device, inode) is, or
/// holds, an admitted agent's workdir.
pub(crate) fn settle_grant_journals(
    data: &Path,
    owner: &dyn Fn(&anyagents_core::AgentId) -> JournalOwner,
    protected: &dyn Fn(u64, u64) -> bool,
) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    let server = crate::sandbox::server_uid();
    let directory = data.join("worktree-grants");
    let listed = match std::fs::symlink_metadata(&directory) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
        Ok(_) => open_nofollow(&directory)?,
    };
    let meta = listed.metadata()?;
    if !meta.is_dir() || meta.uid() != server || writable_by_anyone_else(&listed, &[server])? {
        return Err(refusal(
            &directory,
            format!(
                "the grant journal directory must be owned by the server (uid {server}) and \
                 writable by no one else; journals are not settled"
            ),
        ));
    }
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<_>>()?;
    entries.sort();
    let mut retired = 0usize;
    for path in entries {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(id) = name
            .strip_suffix(".json")
            .and_then(|id| anyagents_core::AgentId::parse(id).ok())
        else {
            continue;
        };
        let lock = lock_journals(&path)?;
        let file = match open_nofollow(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                eprintln!(
                    "worktree grant journal {} is unreadable ({error})",
                    path.display()
                );
                continue;
            }
        };
        let journal_meta = file.metadata()?;
        if !journal_meta.is_file()
            || journal_meta.uid() != server
            || journal_meta.nlink() != 1
            || writable_by_anyone_else(&file, &[server])?
        {
            eprintln!(
                "worktree grant journal {} is not a single-link file only the server (uid \
                 {server}) owns and writes; it is not acted on",
                path.display()
            );
            continue;
        }
        let grant = match std::io::read_to_string(&file)
            .and_then(|text| serde_json::from_str::<Grant>(&text).map_err(Into::into))
        {
            Ok(grant) => grant,
            Err(error) => {
                eprintln!(
                    "worktree grant journal {} is unreadable ({error}); it blocks launch and \
                     cleanup of {id} until an operator inspects and removes it",
                    path.display()
                );
                continue;
            }
        };
        if grant.quarantined || grant.inspection {
            eprintln!(
                "worktree grant journal {}: {}; repair the checkout {} and its Git metadata, \
                 then remove the journal",
                path.display(),
                quarantine_message(&grant),
                grant.checkout.display()
            );
            continue;
        }
        let failed_creation = match owner(&id) {
            JournalOwner::Live => continue,
            JournalOwner::Deleted => false,
            JournalOwner::FailedCreation => true,
        };
        let mut settled = if grant.active {
            revoke_locked(&path).map_err(|e| format!("the grant of {id} could not be revoked: {e}"))
        } else {
            Ok(())
        };
        if settled.is_ok() && failed_creation {
            settled = grants_except(&path)
                .map_err(|e| e.to_string())
                .and_then(|others| remove_failed_checkout(&grant, &others, protected, lock));
        } else {
            drop(lock);
        }
        let _lock = lock_journals(&path)?;
        if let Err(reason) = settled {
            let mut current: Grant =
                serde_json::from_slice(&std::fs::read(&path)?).map_err(std::io::Error::from)?;
            current.quarantine(&reason);
            write_grant(&path, &current)?;
            eprintln!(
                "worktree grant journal {}: {}",
                path.display(),
                quarantine_message(&current)
            );
            continue;
        }
        match retire_locked(&path) {
            Ok(true) => retired += 1,
            Ok(false) => {}
            Err(error) => eprintln!(
                "warning: worktree grant journal {} not removed: {error}",
                path.display()
            ),
        }
    }
    // Staging files of an interrupted journal or seat Git write: no writer
    // can still own one from an earlier server start.
    for entry in std::fs::read_dir(&directory)? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if (name.ends_with(".tmp") || name.ends_with(".safe"))
            && std::fs::symlink_metadata(&path).is_ok_and(|m| {
                m.is_file()
                    && m.modified().is_ok_and(|t| {
                        t.elapsed()
                            .is_ok_and(|age| age > std::time::Duration::from_secs(3600))
                    })
            })
        {
            let _ = std::fs::remove_file(&path);
        }
    }
    if retired > 0 {
        eprintln!("removed {retired} settled worktree grant journals of agents that can never run");
    }
    Ok(())
}

/// Every other journal in `journal`'s directory.
fn grants_except(journal: &Path) -> std::io::Result<Vec<Grant>> {
    let mut grants = Vec::new();
    for entry in std::fs::read_dir(journal.parent().unwrap())? {
        let path = entry?.path();
        if path == journal || path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        grants.push(serde_json::from_slice(&std::fs::read(path)?)?);
    }
    Ok(grants)
}

/// Remove a failed creation's checkout and its registration, the two
/// directories Git keeps for a linked worktree, as cleanup does: no Git
/// process runs, so nothing in the seat-writable checkout (a `.git` pointer,
/// attributes, a config the seat could have written) is executed with the
/// server's rights. Every step holds descriptors. Both directories are opened
/// component by component from `/` without following a symlink; the `.git`
/// pointer and the registration's `gitdir` back link are read through those
/// descriptors and must name each other by inode, not by text; the trees are
/// removed relative to the held descriptors, never following a symlink and
/// never leaving the checkout's file system. Never removed: a directory that
/// is (or holds) an admitted agent's workdir, by inode, or one another journal
/// records. The branch stays: its name may be in use by a later agent, and an
/// unused branch costs nothing. The journal lock is released while the trees
/// are removed.
fn remove_failed_checkout(
    grant: &Grant,
    others: &[Grant],
    protected: &dyn Fn(u64, u64) -> bool,
    lock: std::fs::File,
) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let checkout = &grant.checkout;
    let overlaps = |a: &Path, b: &Path| a.starts_with(b) || b.starts_with(a);
    if let Some(other) = others.iter().find(|other| {
        overlaps(&other.checkout, checkout)
            || overlaps(&other.private, &grant.private)
            || overlaps(&other.private, checkout)
    }) {
        return Err(format!(
            "the checkout {} of a failed creation is also recorded for {}; it is kept",
            checkout.display(),
            other.checkout.display()
        ));
    }
    let checkout_dir = match open_directory_nofollow(checkout) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "the checkout {} of a failed creation does not resolve to a directory \
                 without following a symlink ({error}); it is kept",
                checkout.display()
            ));
        }
    };
    let checkout_meta = checkout_dir.metadata().map_err(|e| e.to_string())?;
    if protected(checkout_meta.dev(), checkout_meta.ino()) {
        return Err(format!(
            "the checkout {} of a failed creation is (or holds) an admitted agent's workdir; \
             it is kept",
            checkout.display()
        ));
    }
    let kept = |why: String| {
        format!(
            "the checkout {} and its registration {} do not name each other ({why}); both \
             are kept",
            checkout.display(),
            grant.private.display()
        )
    };
    if grant.private.parent() != Some(grant.common.join("worktrees").as_path()) {
        return Err(kept(
            "the registration is not under the store's worktrees/".into(),
        ));
    }
    let private_dir =
        open_directory_nofollow(&grant.private).map_err(|e| kept(format!("registration: {e}")))?;
    let private_meta = private_dir.metadata().map_err(|e| e.to_string())?;
    // The checkout's pointer names this registration, by inode.
    let dotgit =
        open_at_nofollow(&checkout_dir, c".git").map_err(|e| kept(format!(".git: {e}")))?;
    let dotgit_meta = dotgit.metadata().map_err(|e| e.to_string())?;
    if !dotgit_meta.is_file() {
        return Err(kept(".git is not a file".into()));
    }
    let pointer = read_small(&dotgit).map_err(|e| kept(format!(".git: {e}")))?;
    let Some(named) = pointer.trim().strip_prefix("gitdir: ") else {
        return Err(kept(".git holds no gitdir pointer".into()));
    };
    let named = checkout.join(named);
    let pointed = open_directory_nofollow(&named)
        .and_then(|directory| directory.metadata())
        .map_err(|e| kept(format!("{}: {e}", named.display())))?;
    if (pointed.dev(), pointed.ino()) != (private_meta.dev(), private_meta.ino()) {
        return Err(kept(".git points at another directory".into()));
    }
    // The registration's back link names this checkout's .git, by inode.
    let back = open_at_nofollow(&private_dir, c"gitdir")
        .and_then(|file| read_small(&file))
        .map_err(|e| kept(format!("gitdir: {e}")))?;
    let back = Path::new(back.trim());
    let linked = back
        .parent()
        .ok_or_else(|| std::io::Error::other("no parent"))
        .and_then(open_directory_nofollow)
        .and_then(|parent| {
            let name = back
                .file_name()
                .ok_or_else(|| std::io::Error::other("no file name"))?;
            open_at_nofollow(&parent, &std::ffi::CString::new(name.as_encoded_bytes())?)?.metadata()
        })
        .map_err(|e| kept(format!("{}: {e}", back.display())))?;
    if (linked.dev(), linked.ino()) != (dotgit_meta.dev(), dotgit_meta.ino()) {
        return Err(kept("the registration names another .git".into()));
    }
    drop(lock);
    remove_directory(checkout, &checkout_dir).map_err(|e| {
        format!(
            "the checkout {} could not be removed: {e}",
            checkout.display()
        )
    })?;
    remove_directory(&grant.private, &private_dir).map_err(|e| {
        format!(
            "the registration {} could not be removed: {e}",
            grant.private.display()
        )
    })
}

/// A directory opened component by component from `/`, no symlink followed.
fn open_directory_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    let directory = open_nofollow(path)?;
    if !directory.metadata()?.is_dir() {
        return Err(std::io::Error::other("not a directory"));
    }
    Ok(directory)
}

/// At most 8 KiB of a small text file, through its descriptor.
fn read_small(file: &std::fs::File) -> std::io::Result<String> {
    use std::io::Read;
    let mut text = String::new();
    file.take(8193).read_to_string(&mut text)?;
    if text.len() > 8192 {
        return Err(std::io::Error::other("file too large"));
    }
    Ok(text)
}

/// Remove the directory `path`, already opened as `directory`: empty it
/// through the descriptor, then remove its name from its parent, opened
/// without following a symlink, only while that name is still this inode.
fn remove_directory(path: &Path, directory: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    let meta = directory.metadata()?;
    empty_directory_at(directory, meta.dev())?;
    let parent = open_directory_nofollow(
        path.parent()
            .ok_or_else(|| std::io::Error::other("no parent directory"))?,
    )?;
    let name = std::ffi::CString::new(
        path.file_name()
            .ok_or_else(|| std::io::Error::other("no directory name"))?
            .as_encoded_bytes(),
    )?;
    let current = stat_at(&parent, &name)?;
    #[allow(
        clippy::unnecessary_cast,
        reason = "stat fields differ across Unix targets"
    )]
    if (current.st_dev as u64, current.st_ino as u64) != (meta.dev(), meta.ino()) {
        return Err(std::io::Error::other(
            "the directory was replaced while being removed",
        ));
    }
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Remove everything inside an opened directory, relative to descriptors:
/// a symlink is removed itself, never followed, and a directory on another
/// file system (`dev`) stops the removal.
fn empty_directory_at(directory: &std::fs::File, dev: u64) -> std::io::Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};
    for name in entry_names(directory)? {
        let c_name = std::ffi::CString::new(name.as_bytes())?;
        let stat = stat_at(directory, &c_name)?;
        let flags = if stat.st_mode & libc::S_IFMT == libc::S_IFDIR {
            #[allow(
                clippy::unnecessary_cast,
                reason = "stat fields differ across Unix targets"
            )]
            if stat.st_dev as u64 != dev {
                return Err(std::io::Error::other(format!(
                    "{name:?} is another file system mounted inside; it is kept"
                )));
            }
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    c_name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            let child = unsafe { std::fs::File::from_raw_fd(fd) };
            let opened = child.metadata()?;
            #[allow(
                clippy::unnecessary_cast,
                reason = "stat fields differ across Unix targets"
            )]
            if (opened.dev(), opened.ino()) != (stat.st_dev as u64, stat.st_ino as u64) {
                return Err(std::io::Error::other(format!(
                    "{name:?} was replaced while being removed"
                )));
            }
            empty_directory_at(&child, dev)?;
            libc::AT_REMOVEDIR
        } else {
            0
        };
        if unsafe { libc::unlinkat(directory.as_raw_fd(), c_name.as_ptr(), flags) } != 0 {
            let error = std::io::Error::last_os_error();
            return Err(std::io::Error::new(
                error.kind(),
                format!("{name:?}: {error}"),
            ));
        }
    }
    Ok(())
}

/// `fstatat` of `name` in an opened directory, without following a symlink.
fn stat_at(directory: &std::fs::File, name: &std::ffi::CStr) -> std::io::Result<libc::stat> {
    use std::os::fd::AsRawFd;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { stat.assume_init() })
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
            match &result {
                Err(error) => grant.quarantine(format!(
                    "temporary cleanup read access could not be revoked at startup: {error}"
                )),
                Ok(()) => {
                    grant.inspection = false;
                    grant.inspection_paths = None;
                }
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
    // grant was revoked: one refresh per store a settled journal names.
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
        if grant.quarantined || grant.inspection || refreshed.contains(&grant.common) {
            continue;
        }
        refreshed.push(grant.common.clone());
        match refresh_store_config_readers(&journal, &grant.common) {
            Ok(()) => {}
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

/// Every journal on `common`, read from the journal's own directory.
fn grants_on(journal: &Path, common: &Path) -> std::io::Result<Vec<Grant>> {
    let mut grants = Vec::new();
    for entry in std::fs::read_dir(journal.parent().unwrap())? {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let grant: Grant = serde_json::from_slice(&std::fs::read(path)?)?;
        if grant.common == common {
            grants.push(grant);
        }
    }
    Ok(grants)
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
    if seat_owns(&file.metadata()?, uid) {
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

/// An object the seat itself owns stays under its control whatever its ACL
/// says: an owner can always grant itself access again. The files a seat
/// writes into its own private gitdir inherit its group entry, and the server
/// cannot change another principal's ACL, so revocation leaves them as they
/// are instead of refusing the whole archive or delete.
fn seat_owns(meta: &std::fs::Metadata, seat_uid: u32) -> bool {
    use std::os::unix::fs::MetadataExt;
    meta.uid() == seat_uid && !server_may_change(meta)
}

fn revoke_common(
    common: &Path,
    uid: u32,
    gid: u32,
    retained: &[Grant],
    regrant: Option<&Grant>,
) -> std::io::Result<()> {
    let _config_lock = match open_common_config(common) {
        Ok(config) => Some(lock_config(config.lock_path(common))?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    walk_tree(common, false, &|_| {}, &|file, path| {
        // Admission (the only caller with a regrant) refuses any object of
        // the store everyone may write; revocation never stops for it.
        if regrant.is_some() {
            refuse_world_writable_object(file, path, None)?;
        }
        revoke_binding(file, path, common, uid, gid, retained, regrant)
    })
}

/// New inspection intents name every opened object before the first ACL write.
/// Legacy intents require a best-effort no-follow walk; hostile entries never
/// prevent revocation of the remaining regular files or another worktree.
fn revoke_inspection(grant: &Grant, retained: &[Grant]) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let issues = std::cell::RefCell::new(Vec::new());
    if let Some(paths) = &grant.inspection_paths {
        let store_dev = open_nofollow(&grant.common)?.metadata()?.dev();
        for target in paths {
            let revoke = || -> std::io::Result<()> {
                if !target.path.starts_with(&grant.common)
                    || !retained_path(&grant.common, &target.path, grant)
                {
                    return Err(std::io::Error::other(
                        "inspection target outside journaled grant set",
                    ));
                }
                let file = target.open(store_dev)?;
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
    /// The recorded object, if `path` still names that inode on `store_dev`,
    /// the common store's file system as it is mounted now.
    fn open(&self, store_dev: u64) -> std::io::Result<std::fs::File> {
        use std::os::unix::fs::MetadataExt;
        let file = open_nofollow(&self.path)?;
        let meta = file.metadata()?;
        if (!meta.is_file() && !meta.is_dir()) || meta.dev() != store_dev || meta.ino() != self.ino
        {
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
    if grant.quarantined || grant.inspection {
        return Err(refusal(&journal, quarantine_message(&grant)));
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
    // Fail closed before the first journal or ACL write: a store that launch
    // would refuse must not be admitted, granted, or left needing revocation.
    refuse_world_writable(common).map_err(|e| refusal(common, e))?;
    validate_config_files(private, common)?;
    trusted_metadata_layout(common, private, uid).map_err(|e| refusal(common, e))?;
    // A journal awaiting recovery is never overwritten by a new grant.
    match std::fs::read(journal) {
        Ok(bytes) => {
            let pending = serde_json::from_slice::<Grant>(&bytes).map_or_else(
                |e| Some(e.to_string()),
                |g| (g.quarantined || g.inspection).then(|| quarantine_message(&g)),
            );
            if let Some(pending) = pending {
                return Err(refusal(journal, pending));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(refusal(journal, e)),
    }
    let grant = Grant {
        common: common.to_path_buf(),
        gid,
        uid,
        private: private.to_path_buf(),
        checkout: path.to_path_buf(),
        active: true,
        inspection: false,
        inspection_paths: None,
        quarantined: false,
        reason: None,
    };
    let retained = other_active_grants(journal, &grant).map_err(|e| refusal(journal, e))?;
    write_grant(journal, &grant).map_err(|e| refusal(journal, e))?;
    // A seatless agent runs as the server: it already holds the server's
    // access, so nothing is granted now and nothing needs revoking later. The
    // journal still records its metadata for cleanup.
    if grant.is_server_principal() {
        return Ok(());
    }
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
    manage_config(
        common,
        &config_readers(journal, common).map_err(|e| refusal(journal, e))?,
    )
    .map_err(|e| refusal(common, e))?;
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

/// Every descriptor operation that changes an object needs a real open
/// descriptor. On an `O_PATH` binding the kernel answers `EBADF`, which once
/// surfaced as an endless "Bad file descriptor (os error 9)" revocation
/// failure; name the object's owner instead.
#[cfg(unix)]
fn changeable(file: &std::fs::File) -> std::io::Result<()> {
    if path_only(file) {
        return Err(unreadable_foreign(&file.metadata()?));
    }
    Ok(())
}

#[cfg(unix)]
fn descriptor_fchown(
    file: &std::fs::File,
    uid: Option<u32>,
    gid: Option<u32>,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    changeable(file)?;
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
    changeable(file)?;
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
    changeable(file)?;
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
    changeable(file)?;
    if unsafe { libc::fremovexattr(file.as_raw_fd(), name.as_ptr()) } == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if no_acl(&err) {
        return Ok(());
    }
    if matches!(err.raw_os_error(), Some(libc::EPERM | libc::EACCES)) {
        let meta = file.metadata()?;
        let orig_uid = meta.uid();
        let srv_uid = crate::sandbox::server_uid();
        if orig_uid != srv_uid && (meta.is_dir() || (meta.is_file() && meta.nlink() == 1)) {
            descriptor_fchown(file, Some(srv_uid), None)?;
            let res = if unsafe { libc::fremovexattr(file.as_raw_fd(), name.as_ptr()) } == 0
                || no_acl(&std::io::Error::last_os_error())
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
        || !no_acl(&std::io::Error::last_os_error())
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
        } else if error.raw_os_error().is_some() {
            // A bare errno names no object; the operator needs the path.
            Err(std::io::Error::new(
                error.kind(),
                format!("{}: {error}", path.display()),
            ))
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
                // The managed config link (and adoption's staged copy of it)
                // is the server's own; any other symlink is reported.
                let managed_link = std::fs::read_link(&child_path)
                    .is_ok_and(|target| target == Path::new(CONFIG_DIRECTORY).join("config"));
                if issues.is_some()
                    && !(managed_link
                        && (child_path == path.join(CONFIG_LINK_STAGE)
                            || child_path == path.join("config")
                                && open_common_config(path).is_ok()))
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
            if child < 0 {
                // Captured before any later syscall can overwrite errno.
                let error = std::io::Error::last_os_error();
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
            return if no_acl(&err) { Ok(None) } else { Err(err) };
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
            anyagents_toolchain::global().exec_path("worktree_access.admission"),
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
/// takes the probe's arguments instead. A seat must be able to execute it, as
/// it must execute the server binary, but a build tree usually sits below a
/// home directory no other uid can traverse. The harness therefore runs from
/// one seat-executable copy in a fresh 0755 directory under the system
/// temporary directory.
#[cfg(test)]
fn probe_program() -> std::io::Result<(PathBuf, Vec<&'static str>)> {
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
    Ok((
        copy.clone().map_err(std::io::Error::other)?,
        vec![
            "--exact",
            "worktree_access::tests::admission_probe_worker",
            "--nocapture",
            "--",
        ],
    ))
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
/// traverse every directory, create a lock file in the tree, in its private
/// gitdir and in the shared `objects/` and `refs/`, and run `git status`.
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
                inspection: true,
                quarantined: false,
                reason: None,
                inspection_paths: Some(vec![InspectionPath {
                    path,
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
                    inspection: true,
                    inspection_paths: Some(vec![other_target]),
                    quarantined: false,
                    reason: None,
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
                inspection: false,
                inspection_paths: None,
                quarantined: false,
                reason: None,
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

    /// A seatless agent runs as the server. Its `repo:` worktree is created,
    /// admitted, archived and retired end to end without a single change to
    /// the shared store: there is no boundary to open, so there is nothing to
    /// grant and nothing to revoke (revoking would strip the server's own
    /// entries from objects seats created). The journal still records the
    /// metadata, so cleanup works as for any agent.
    #[test]
    fn a_seatless_agent_shares_and_revokes_without_touching_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("ExampleOrg/demo", &target, "child", None)
            .unwrap();
        let common = git_dirs(&target).unwrap().pop().unwrap();
        let snapshot = || {
            let entries = std::cell::RefCell::new(Vec::new());
            walk_tree(&common, false, &|_| {}, &|file, path| {
                let meta = file.metadata()?;
                #[cfg(target_os = "linux")]
                let acl = {
                    let mut bytes = [0u8; 256];
                    let len = read_xattr(file, c"system.posix_acl_access", &mut bytes);
                    (len > 0).then(|| bytes[..len as usize].to_vec())
                };
                #[cfg(not(target_os = "linux"))]
                let acl: Option<Vec<u8>> = None;
                entries.borrow_mut().push((
                    path.to_path_buf(),
                    meta.mode(),
                    meta.uid(),
                    meta.gid(),
                    acl,
                ));
                Ok(())
            })
            .unwrap();
            entries.into_inner()
        };
        let before = snapshot();
        let (uid, gid) = (crate::sandbox::server_uid(), crate::sandbox::server_gid());
        let id = anyagents_core::AgentId::generate();
        share(&target, uid, gid, &grant_path(dir.path(), &id)).unwrap();
        let meta = std::fs::metadata(&target).unwrap();
        assert_eq!(meta.gid(), gid);
        assert_eq!(meta.permissions().mode() & 0o2070, 0o2070);
        assert_eq!(
            snapshot(),
            before,
            "the server's own grant changed the store"
        );
        assert!(
            std::fs::symlink_metadata(common.join("config"))
                .unwrap()
                .is_file()
        );
        verify(&target, uid, gid, None).unwrap();
        crate::worktree::tests::git(&target, &["commit", "--allow-empty", "-m", "Seatless"]);
        crate::worktree::tests::git(&target, &["status", "--porcelain"]);
        ensure_launchable(dir.path(), &id).unwrap();
        let record = RecordedWorktree::load(dir.path(), &id, &target).unwrap();
        assert!(record.inspect(|| Ok(true)).unwrap());
        // Archive: the grant ends without a walk of the store.
        let before = snapshot();
        revoke_recorded(dir.path(), &id).unwrap();
        assert_eq!(
            snapshot(),
            before,
            "revoking the server's own grant changed the store"
        );
        let grant: Grant =
            serde_json::from_slice(&std::fs::read(grant_path(dir.path(), &id)).unwrap()).unwrap();
        assert!(!grant.active && !grant.quarantined);
        // Wake restores it, delete retires it once the checkout is gone.
        restore_recorded(dir.path(), &id).unwrap();
        revoke_recorded(dir.path(), &id).unwrap();
        assert!(
            !retire_settled(dir.path(), &id).unwrap(),
            "checkout still exists"
        );
        std::fs::remove_dir_all(&target).unwrap();
        assert!(retire_settled(dir.path(), &id).unwrap());
        assert!(!grant_path(dir.path(), &id).exists());
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

    /// A common Git directory with an ordinary config, made by the test user,
    /// who is the server here.
    fn plain_common(dir: &Path) -> PathBuf {
        let repo = dir.join("hub");
        std::fs::create_dir(&repo).unwrap();
        crate::worktree::tests::git(&repo, &["init", "-q", "-b", "main"]);
        let common = std::fs::canonicalize(repo.join(".git")).unwrap();
        std::fs::set_permissions(&common, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(
            common.join("config"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        common
    }

    fn is_managed(common: &Path) -> bool {
        matches!(open_common_config(common), Ok(CommonConfig::Managed { .. }))
    }

    /// The live outage: six hubs had a managed config and no journal to
    /// witness it, so every new grant was refused forever. Provenance now
    /// lives in the hub: the server's ownership of the link, the directory
    /// and the file is the witness, and no journal, data directory or device
    /// number is involved.
    #[test]
    fn a_relocated_config_is_witnessed_by_the_hub_without_any_journal() {
        let dir = crate::test_tempdir();
        let common = plain_common(dir.path());
        let config = common.join("config");
        let original = std::fs::read(&config).unwrap();
        let before = std::fs::metadata(&config).unwrap();
        assert!(matches!(
            open_common_config(&common).unwrap(),
            CommonConfig::Plain(_)
        ));
        let lock = relocate_config(&common, &|| Ok(())).unwrap();
        assert!(lock.is_some(), "a relocation returns its held lock");
        assert!(common.join(CONFIG_DIRECTORY).join("config.lock").exists());
        drop(lock);
        assert!(!common.join(CONFIG_DIRECTORY).join("config.lock").exists());
        assert!(!common.join("config.lock").exists());
        assert_eq!(
            std::fs::read_link(&config).unwrap(),
            Path::new(CONFIG_DIRECTORY).join("config")
        );
        let moved = std::fs::metadata(common.join(CONFIG_DIRECTORY).join("config")).unwrap();
        assert_eq!(
            (moved.ino(), moved.uid(), moved.nlink()),
            (before.ino(), before.uid(), 1)
        );
        assert_eq!(std::fs::read(&config).unwrap(), original);
        // No journal exists anywhere, yet the hub is recognized and Git
        // reads its configuration through the link.
        assert!(is_managed(&common));
        assert!(!dir.path().join("worktree-grants").exists());
        validate_config_files(&common.join("worktrees/none"), &common).unwrap();
        crate::worktree::tests::git(common.parent().unwrap(), &["config", "core.bare"]);
        // Relocating again is a no-op, and a Git rewrite keeps the witness.
        assert!(relocate_config(&common, &|| Ok(())).unwrap().is_none());
        crate::worktree::tests::git(
            common.parent().unwrap(),
            &["config", "anyagents.test", "rewritten"],
        );
        assert!(is_managed(&common));
        // Only the server's own objects witness it: the same hub seen by a
        // server running as anyone else is refused, never adopted.
        let other = crate::sandbox::server_uid().wrapping_add(1);
        let error = open_common_config_as(&common, other)
            .err()
            .expect("another principal's relocation is not trusted");
        assert!(error.to_string().contains("not the server"), "{error}");
    }

    /// Every interruption point of a relocation leaves a state the hub
    /// itself explains; the next attempt finishes it without a journal,
    /// without quarantine and without losing a byte.
    #[test]
    fn an_interrupted_relocation_resumes_from_the_hub_state() {
        let dir = crate::test_tempdir();
        let common = plain_common(dir.path());
        let config = common.join("config");
        let original = std::fs::read(&config).unwrap();
        let inode = std::fs::metadata(&config).unwrap().ino();
        let error = relocate_config(&common, &|| {
            Err(std::io::Error::other(
                "injected interruption before publish",
            ))
        })
        .err()
        .expect("the hook interrupts");
        assert_eq!(error.to_string(), "injected interruption before publish");
        for path in [&config, &common.join(CONFIG_DIRECTORY).join("config")] {
            let meta = std::fs::symlink_metadata(path).unwrap();
            assert_eq!((meta.ino(), meta.nlink()), (inode, 2));
            assert_eq!(std::fs::read(path).unwrap(), original);
        }
        assert!(!common.join("config.lock").exists());
        // Both names, one inode: readable as an ordinary config meanwhile.
        assert!(matches!(
            open_common_config(&common).unwrap(),
            CommonConfig::Linked(_)
        ));
        validate_config_files(&common.join("worktrees/none"), &common).unwrap();
        // A crash between staging the link and renaming it leaves the staged
        // symlink; the next attempt replaces it.
        std::os::unix::fs::symlink(
            Path::new(CONFIG_DIRECTORY).join("config"),
            common.join(CONFIG_LINK_STAGE),
        )
        .unwrap();
        drop(relocate_config(&common, &|| Ok(())).unwrap());
        assert!(is_managed(&common));
        assert!(!common.join(CONFIG_LINK_STAGE).exists());
        let moved = std::fs::metadata(common.join(CONFIG_DIRECTORY).join("config")).unwrap();
        assert_eq!((moved.ino(), moved.nlink()), (inode, 1));
        assert_eq!(std::fs::read(&config).unwrap(), original);

        // Interrupted right after creating the directory: it is reused.
        let fresh = crate::test_tempdir();
        let common = plain_common(fresh.path());
        std::fs::create_dir(common.join(CONFIG_DIRECTORY)).unwrap();
        std::fs::set_permissions(
            common.join(CONFIG_DIRECTORY),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        drop(relocate_config(&common, &|| Ok(())).unwrap());
        assert!(is_managed(&common));
    }

    /// A planted "managed" config is never adopted, whatever it imitates,
    /// and a planted directory never receives the real config.
    #[test]
    fn planted_managed_configs_are_refused() {
        let dir = crate::test_tempdir();
        let outside = dir.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(
            outside.join("config"),
            "[core]\n\tfsmonitor = /tmp/payload\n",
        )
        .unwrap();
        type Plant = fn(&Path, &Path);
        let cases: [(&str, Plant, &str); 7] = [
            (
                "wrong link target",
                |common, outside| {
                    std::fs::remove_file(common.join("config")).unwrap();
                    std::os::unix::fs::symlink(outside.join("config"), common.join("config"))
                        .unwrap();
                },
                "not the managed link",
            ),
            (
                "directory is a symlink",
                |common, outside| {
                    std::fs::remove_dir_all(common.join(CONFIG_DIRECTORY)).unwrap();
                    std::os::unix::fs::symlink(outside, common.join(CONFIG_DIRECTORY)).unwrap();
                },
                "",
            ),
            (
                "file is a symlink",
                |common, outside| {
                    let file = common.join(CONFIG_DIRECTORY).join("config");
                    std::fs::remove_file(&file).unwrap();
                    std::os::unix::fs::symlink(outside.join("config"), file).unwrap();
                },
                "",
            ),
            (
                "file has a second link",
                |common, outside| {
                    std::fs::hard_link(
                        common.join(CONFIG_DIRECTORY).join("config"),
                        outside.join("second-link"),
                    )
                    .unwrap();
                },
                "single-link regular file",
            ),
            (
                "unexpected entry",
                |common, _| {
                    std::fs::write(common.join(CONFIG_DIRECTORY).join("hooks"), "").unwrap();
                },
                "unexpected entry",
            ),
            (
                "world-writable directory",
                |common, _| {
                    std::fs::set_permissions(
                        common.join(CONFIG_DIRECTORY),
                        std::fs::Permissions::from_mode(0o777),
                    )
                    .unwrap();
                },
                "no one else can write",
            ),
            (
                "world-writable file",
                |common, _| {
                    std::fs::set_permissions(
                        common.join(CONFIG_DIRECTORY).join("config"),
                        std::fs::Permissions::from_mode(0o666),
                    )
                    .unwrap();
                },
                "no one else can write",
            ),
        ];
        for (name, plant, expected) in cases {
            let case = tempfile::tempdir_in(dir.path()).unwrap();
            let common = plain_common(case.path());
            drop(relocate_config(&common, &|| Ok(())).unwrap());
            assert!(is_managed(&common), "{name}: baseline");
            plant(&common, &outside);
            let error = open_common_config(&common)
                .err()
                .unwrap_or_else(|| panic!("{name}: planted config accepted"));
            assert!(error.to_string().contains(expected), "{name}: {error}");
            assert!(
                validate_config_files(&common.join("worktrees/none"), &common).is_err(),
                "{name}: admission accepted it"
            );
            assert!(relocate_config(&common, &|| Ok(())).is_err(), "{name}");
            let _ = std::fs::remove_file(outside.join("second-link"));
        }
        // A planted directory beside an ordinary config is not filled.
        let case = tempfile::tempdir_in(dir.path()).unwrap();
        let common = plain_common(case.path());
        std::fs::create_dir(common.join(CONFIG_DIRECTORY)).unwrap();
        std::fs::set_permissions(
            common.join(CONFIG_DIRECTORY),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        std::fs::write(common.join(CONFIG_DIRECTORY).join("config"), "planted").unwrap();
        let error = relocate_config(&common, &|| Ok(())).err().unwrap();
        assert!(
            error.to_string().contains("not an interrupted relocation"),
            "{error}"
        );
        assert!(
            std::fs::symlink_metadata(common.join("config"))
                .unwrap()
                .is_file()
        );
        assert_eq!(
            std::fs::read(common.join(CONFIG_DIRECTORY).join("config")).unwrap(),
            b"planted"
        );
    }

    /// The whole admission path with real ACLs: a managed config made by an
    /// earlier admission keeps working after every journal is gone, and a
    /// journal never records anything about the store.
    #[cfg(target_os = "linux")]
    #[test]
    fn admission_survives_lost_journals_and_records_no_store_provenance() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let first = dir.path().join("first");
        f.consumer
            .remote("ExampleOrg/demo", &first, "first", None)
            .unwrap();
        let dirs = git_dirs(&first).unwrap();
        let common = dirs.last().unwrap().clone();
        let config = common.join("config");
        let original = std::fs::read(&config).unwrap();
        let uid = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid().wrapping_add(100000);
        let id = anyagents_core::AgentId::generate();
        let journal = grant_path(dir.path(), &id);
        std::fs::write(common.join("config.lock"), b"another Git writer").unwrap();
        let err = share_known(&first, &dirs[0], &common, uid, gid, &journal).unwrap_err();
        assert!(err.to_string().contains("exists"), "{err}");
        assert!(!common.join(CONFIG_DIRECTORY).exists());
        std::fs::remove_file(common.join("config.lock")).unwrap();
        revoke_recorded(dir.path(), &id).unwrap();
        share_known(&first, &dirs[0], &common, uid, gid, &journal).unwrap();
        assert!(is_managed(&common));
        assert_eq!(std::fs::read(&config).unwrap(), original);
        let recorded = std::fs::read_to_string(&journal).unwrap();
        assert!(!recorded.contains("anyagents-config"), "{recorded}");
        ensure_launchable(dir.path(), &id).unwrap();
        // Every journal is lost (a data directory moved without them).
        std::fs::remove_dir_all(dir.path().join("worktree-grants")).unwrap();
        let second = dir.path().join("second");
        f.consumer
            .remote("ExampleOrg/demo", &second, "second", None)
            .unwrap();
        let second_id = anyagents_core::AgentId::generate();
        let second_dirs = git_dirs(&second).unwrap();
        share_known(
            &second,
            &second_dirs[0],
            &common,
            uid,
            gid + 1,
            &grant_path(dir.path(), &second_id),
        )
        .unwrap();
        assert!(is_managed(&common));
        ensure_launchable(dir.path(), &second_id).unwrap();
        revoke_recorded(dir.path(), &second_id).unwrap();
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
        // A seat's group: the server's own grant relocates nothing. Only the
        // metadata grant is under test; a checkout chgrp to a group the test
        // user is not in needs root.
        let gid = crate::sandbox::server_gid().wrapping_add(100000);
        let dirs = git_dirs(&work).unwrap();
        share_known(
            &work,
            &dirs[0],
            dirs.last().unwrap(),
            uid,
            gid,
            &grant_path(dir.path(), &id),
        )
        .unwrap();
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
        let message = err.to_string();
        assert!(
            message.contains(&format!("os error {}", libc::ELOOP))
                && message.contains(victim.to_str().unwrap()),
            "{message}"
        );
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

    /// A group a deployment declares trusted may write shared Git metadata;
    /// any other group writer, and every world writer, stays untrusted.
    #[cfg(unix)]
    #[test]
    fn group_writable_metadata_is_trusted_only_for_declared_groups() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = crate::test_tempdir();
        let store = dir.path().join("store");
        std::fs::create_dir(&store).unwrap();
        std::fs::set_permissions(&store, std::fs::Permissions::from_mode(0o2775)).unwrap();
        let gid = std::fs::metadata(&store).unwrap().gid();
        let uid = std::fs::metadata(&store).unwrap().uid();
        let open = || open_nofollow(&store).unwrap();
        assert!(writable_file_by_untrusted_with(&open(), &[uid], &[]).unwrap());
        assert!(!writable_file_by_untrusted_with(&open(), &[uid], &[gid]).unwrap());
        assert!(writable_file_by_untrusted_with(&open(), &[uid], &[gid.wrapping_add(1)]).unwrap());
        std::fs::set_permissions(&store, std::fs::Permissions::from_mode(0o2777)).unwrap();
        assert!(writable_file_by_untrusted_with(&open(), &[uid], &[gid]).unwrap());
        assert_eq!(
            parse_trusted_gids(" 2100, ,abc-no-such-group-xyz ,7"),
            vec![2100, 7]
        );
    }

    /// Revoking a seat skips only the objects that seat owns and the server
    /// cannot change. The server's own objects are still revoked as before.
    #[cfg(unix)]
    #[test]
    fn revocation_skips_only_objects_the_revoked_seat_owns() {
        use std::os::unix::fs::MetadataExt;
        let dir = crate::test_tempdir();
        let own = std::fs::metadata(dir.path()).unwrap();
        let server = crate::sandbox::server_uid();
        assert!(!seat_owns(&own, own.uid()), "server-owned stays revocable");
        assert!(!seat_owns(&own, own.uid().wrapping_add(1)));
        let foreign = std::fs::metadata("/").unwrap();
        if server != 0 && foreign.uid() != server {
            assert!(seat_owns(&foreign, foreign.uid()));
            assert!(!seat_owns(&foreign, foreign.uid().wrapping_add(1)));
        }
    }

    /// The deployed revocation loop logged "Bad file descriptor (os error 9)"
    /// for every retry. Its source: an object the server cannot open is bound
    /// with `O_PATH`, and the kernel refuses fchmod/fchown/f*xattr on such a
    /// descriptor with EBADF. The old walk tried exactly that as a "repair"
    /// and then reported the last errno. No change helper may ever reach the
    /// kernel with a path-only descriptor; it names the object's owner instead.
    #[cfg(target_os = "linux")]
    #[test]
    fn path_only_bindings_are_never_changed_through_their_descriptor() {
        use std::os::fd::{AsRawFd, FromRawFd};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pack.keep");
        std::fs::write(&path, b"").unwrap();
        let before = std::fs::metadata(&path).unwrap().mode();
        let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        let fd = unsafe {
            libc::open(
                name.as_ptr(),
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        assert!(fd >= 0, "{}", std::io::Error::last_os_error());
        let bound = unsafe { std::fs::File::from_raw_fd(fd) };
        assert!(path_only(&bound));
        // What the old repair branch received from the kernel.
        assert_eq!(unsafe { libc::fchmod(bound.as_raw_fd(), 0o600) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EBADF)
        );
        for result in [
            descriptor_fchmod(&bound, 0o600),
            descriptor_fchown(&bound, None, None),
            descriptor_fsetxattr(&bound, c"user.anyagents-test", b"1"),
            descriptor_fremovexattr(&bound, c"user.anyagents-test"),
        ] {
            let error = result.unwrap_err();
            assert_ne!(error.raw_os_error(), Some(libc::EBADF), "{error}");
            assert!(
                error.to_string().contains("unreadable by the server"),
                "{error}"
            );
        }
        // Reading a path-only binding's ACL still works, through /proc.
        let mut bytes = [0u8; 64];
        let len = read_xattr(&bound, c"system.posix_acl_access", &mut bytes);
        assert!(
            len >= 0 || no_acl(&std::io::Error::last_os_error()),
            "{}",
            std::io::Error::last_os_error()
        );
        assert_eq!(std::fs::metadata(&path).unwrap().mode(), before);
    }

    /// A tree walk that cannot open an object reports the object's own path
    /// and the real errno, never a later syscall's.
    #[test]
    fn walk_failures_name_the_object_and_keep_the_open_errno() {
        if crate::sandbox::server_uid() == 0 {
            return; // root opens everything
        }
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let locked = root.join("objects/ab");
        std::fs::create_dir_all(&locked).unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let error = walk_tree(&root, false, &|_| {}, &|_, _| Ok(())).unwrap_err();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::PermissionDenied,
            "{error}"
        );
        let message = error.to_string();
        assert!(message.contains(locked.to_str().unwrap()), "{message}");
        assert!(message.contains("os error 13"), "{message}");
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
        crate::worktree::tests::git(&tree, &["init", "-q", "-b", "main"]);
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
        crate::worktree::tests::write_executable(
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
        crate::worktree::tests::write_executable(&helper.join("git"), "#!/bin/sh\nexit 0\n");
        let path = std::env::join_paths([&helper]).unwrap();
        assert_eq!(probe_git(&path).unwrap(), helper.join("git"));
        assert!(probe_git(std::ffi::OsStr::new("/nonexistent")).is_err());
    }

    /// keel's shared store had `objects/` and `refs/` at mode 2777 with a
    /// default ACL `other::rwx`. Admission names that and its repair instead
    /// of granting on top of it.
    #[test]
    fn admission_names_world_writable_metadata_and_its_repair() {
        let dir = crate::test_tempdir();
        let common = plain_common(dir.path());
        refuse_world_writable(&common).unwrap();
        for name in ["objects", "refs", "hooks", "info", ""] {
            let path = if name.is_empty() {
                common.clone()
            } else {
                common.join(name)
            };
            // Git's template may not provide hooks/ or info/.
            std::fs::create_dir_all(&path).unwrap();
            let mode = std::fs::metadata(&path).unwrap().mode() & 0o7777;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode | 0o002)).unwrap();
            let error = refuse_world_writable(&common).unwrap_err().to_string();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(error.contains("is world-writable"), "{error}");
            assert!(error.contains(path.to_str().unwrap()), "{error}");
            assert!(error.contains("chmod -R o-w"), "{error}");
            if !name.is_empty() {
                assert!(error.starts_with(&format!("{name} (")), "{error}");
            }
        }
        refuse_world_writable(&common).unwrap();
    }

    /// Below the top-level names, admission's own walk of the store refuses
    /// any object everyone may write (cloud had one world-writable private
    /// gitdir). Revocation is never stopped by it.
    #[test]
    fn admission_refuses_a_world_writable_object_anywhere_in_the_store() {
        let dir = crate::test_tempdir();
        let common = plain_common(dir.path());
        let deep = common.join("worktrees/other-agent");
        std::fs::create_dir_all(&deep).unwrap();
        std::fs::set_permissions(&deep, std::fs::Permissions::from_mode(0o777)).unwrap();
        refuse_world_writable(&common).unwrap();
        let (uid, gid) = (
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid().wrapping_add(100000),
        );
        let grant = Grant {
            common: common.clone(),
            private: common.join("worktrees/child"),
            checkout: dir.path().join("child"),
            uid,
            gid,
            active: true,
            inspection: false,
            inspection_paths: None,
            quarantined: false,
            reason: None,
        };
        let error = revoke_common(&common, uid, gid, &[], Some(&grant))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(deep.to_str().unwrap()) && error.contains("is world-writable"),
            "{error}"
        );
        assert!(error.contains("chmod -R o-w"), "{error}");
        revoke_common(&common, uid, gid, &[], None).unwrap();
    }

    /// The review's attack: a seat in the trusted store group writes config
    /// text into a server-owned group-writable file (`.git/description` is
    /// 0664 on the desk) and renames it onto `config`, or swaps a
    /// group-writable server-owned directory in as `anyagents-config`.
    /// Configuration admits exactly its owners as writers, trusted group or
    /// not, so neither is accepted, relocated or managed.
    #[test]
    fn group_writable_config_objects_are_never_trusted() {
        let dir = crate::test_tempdir();
        let common = plain_common(dir.path());
        let config = common.join("config");
        let original = std::fs::read(&config).unwrap();
        let description = common.join("description");
        std::fs::write(&description, "[core]\n\tfsmonitor = /tmp/payload\n").unwrap();
        std::fs::set_permissions(&description, std::fs::Permissions::from_mode(0o664)).unwrap();
        std::fs::rename(&config, common.join("config.saved")).unwrap();
        std::fs::rename(&description, &config).unwrap();
        let error = open_common_config(&common).err().unwrap().to_string();
        assert!(
            error.contains("writable by someone other than its owner")
                && error.contains("chmod go-w"),
            "{error}"
        );
        assert!(relocate_config(&common, &|| Ok(())).is_err());
        assert!(!common.join(CONFIG_DIRECTORY).exists());
        assert!(validate_config_files(&common.join("worktrees/none"), &common).is_err());
        // The store's own config, not group-writable, relocates; the managed
        // file carries no group or other write.
        std::fs::remove_file(&config).unwrap();
        std::fs::rename(common.join("config.saved"), &config).unwrap();
        drop(relocate_config(&common, &|| Ok(())).unwrap());
        assert!(is_managed(&common));
        let managed = common.join(CONFIG_DIRECTORY).join("config");
        assert_eq!(std::fs::metadata(&managed).unwrap().mode() & 0o022, 0);
        assert_eq!(std::fs::read(&managed).unwrap(), original);
        // A group-writable server-owned directory swapped in is refused.
        let swapped = common.join("hooks-like");
        std::fs::create_dir(&swapped).unwrap();
        std::fs::write(swapped.join("config"), "[core]\n\thooksPath = /tmp\n").unwrap();
        std::fs::set_permissions(&swapped, std::fs::Permissions::from_mode(0o775)).unwrap();
        std::fs::rename(common.join(CONFIG_DIRECTORY), common.join("managed.saved")).unwrap();
        std::fs::rename(&swapped, common.join(CONFIG_DIRECTORY)).unwrap();
        let error = open_common_config(&common).err().unwrap().to_string();
        assert!(error.contains("no one else can write"), "{error}");
    }

    /// The same, for a default ACL alone: every object a seat adds later
    /// would be world-writable.
    #[cfg(target_os = "linux")]
    #[test]
    fn admission_names_a_world_writable_default_acl() {
        let dir = crate::test_tempdir();
        let common = plain_common(dir.path());
        let objects = open_nofollow(&common.join("objects")).unwrap();
        let mut acl = 2u32.to_le_bytes().to_vec();
        for (tag, perm) in [(1u16, 7u16), (4, 5), (32, 7)] {
            acl.extend(tag.to_le_bytes());
            acl.extend(perm.to_le_bytes());
            acl.extend(u32::MAX.to_le_bytes());
        }
        if let Err(error) = descriptor_fsetxattr(&objects, c"system.posix_acl_default", &acl) {
            assert_eq!(error.raw_os_error(), Some(libc::EOPNOTSUPP), "{error}");
            return; // no POSIX ACLs on this file system
        }
        let error = refuse_world_writable(&common).unwrap_err().to_string();
        assert!(error.contains("default ACL"), "{error}");
        assert!(error.contains("setfacl -d -m o::rx"), "{error}");
    }

    fn journal_of(data: &Path, id: &anyagents_core::AgentId, checkout: &Path, active: bool) {
        write_grant(
            &grant_path(data, id),
            &Grant {
                common: data.join("missing-hub/.git"),
                private: data.join("missing-hub/.git/worktrees/x"),
                checkout: checkout.to_path_buf(),
                uid: crate::sandbox::server_uid(),
                gid: crate::sandbox::server_gid(),
                active,
                inspection: false,
                inspection_paths: None,
                quarantined: false,
                reason: None,
            },
        )
        .unwrap();
    }

    /// A linked-worktree layout without Git: the checkout's `.git` pointer
    /// and the registration's `gitdir` back link name each other.
    fn registered_checkout(data: &Path, name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let common = data.join("hub/.git");
        let private = common.join("worktrees").join(name);
        let checkout = data.join(name);
        std::fs::create_dir_all(&private).unwrap();
        std::fs::create_dir_all(checkout.join("src")).unwrap();
        std::fs::write(checkout.join("src/file"), "provisioned").unwrap();
        std::fs::write(
            checkout.join(".git"),
            format!("gitdir: {}\n", private.display()),
        )
        .unwrap();
        std::fs::write(
            private.join("gitdir"),
            format!("{}\n", checkout.join(".git").display()),
        )
        .unwrap();
        (common, private, checkout)
    }

    fn failed_journal(data: &Path, id: &anyagents_core::AgentId, name: &str) -> PathBuf {
        let (common, private, checkout) = registered_checkout(data, name);
        write_grant(
            &grant_path(data, id),
            &Grant {
                common,
                private,
                checkout: checkout.clone(),
                uid: crate::sandbox::server_uid(),
                gid: crate::sandbox::server_gid(),
                active: false,
                inspection: false,
                inspection_paths: None,
                quarantined: false,
                reason: None,
            },
        )
        .unwrap();
        checkout
    }

    fn reason_of(data: &Path, id: &anyagents_core::AgentId) -> String {
        let grant: Grant =
            serde_json::from_slice(&std::fs::read(grant_path(data, id)).unwrap()).unwrap();
        assert!(grant.quarantined, "not quarantined");
        grant.reason.unwrap()
    }

    /// A journal left by an agent that can never run again ends in one pass:
    /// removed once nothing remains to witness (a failed creation's checkout
    /// and registration removed first), kept while a deleted agent's checkout
    /// exists, quarantined with the reason when its grant or checkout cannot
    /// be undone. A live agent's journal is never touched, and a later pass
    /// retries nothing.
    #[test]
    fn startup_settles_orphaned_journals_once() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        let id = || anyagents_core::AgentId::generate();
        let (gone, kept, live, held, failed, corrupt) = (id(), id(), id(), id(), id(), id());
        std::fs::create_dir(data.join("deleted-agent-work")).unwrap();
        journal_of(data, &gone, &data.join("removed-checkout"), true);
        journal_of(data, &kept, &data.join("deleted-agent-work"), false);
        journal_of(data, &live, &data.join("removed-checkout"), false);
        let failed_checkout = failed_journal(data, &failed, "failed-work");
        // A seat grant on a store that no longer exists cannot be revoked.
        let mut grant: Grant =
            serde_json::from_slice(&std::fs::read(grant_path(data, &gone)).unwrap()).unwrap();
        grant.gid = grant.gid.wrapping_add(100000);
        write_grant(&grant_path(data, &held), &grant).unwrap();
        let staging = data.join("worktree-grants/gitconfig-x.tmp");
        let fresh_staging = data.join("worktree-grants/gitconfig-y.safe");
        std::fs::write(&fresh_staging, b"").unwrap();
        std::fs::File::create(&staging)
            .unwrap()
            .set_modified(std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
        let owner = |agent: &anyagents_core::AgentId| {
            if agent == &live {
                JournalOwner::Live
            } else if agent == &failed {
                JournalOwner::FailedCreation
            } else {
                JournalOwner::Deleted
            }
        };
        let unprotected = |_: u64, _: u64| false;
        settle_grant_journals(data, &owner, &unprotected).unwrap();
        // The server's own grant needs no revocation: removed outright.
        assert!(!grant_path(data, &gone).exists());
        assert!(
            grant_path(data, &kept).exists(),
            "a checkout keeps its journal"
        );
        assert!(data.join("deleted-agent-work").exists());
        assert!(
            grant_path(data, &live).exists(),
            "a live agent's journal moved"
        );
        assert!(!grant_path(data, &failed).exists());
        assert!(!failed_checkout.exists());
        assert!(!data.join("hub/.git/worktrees/failed-work").exists());
        assert!(!staging.exists());
        assert!(fresh_staging.exists(), "a staging file in use was removed");
        let reason = reason_of(data, &held);
        assert!(reason.contains("could not be revoked"), "{reason}");
        assert!(reaping_blocked(data, &held));
        assert!(
            ensure_launchable(data, &held)
                .unwrap_err()
                .to_string()
                .contains(&reason)
        );
        // A later pass changes nothing: no retry, no new state. An unreadable
        // journal is listed and left for an operator.
        std::fs::write(grant_path(data, &corrupt), b"not a journal").unwrap();
        let before = std::fs::read(grant_path(data, &held)).unwrap();
        settle_grant_journals(data, &owner, &unprotected).unwrap();
        assert_eq!(std::fs::read(grant_path(data, &held)).unwrap(), before);
        assert_eq!(
            std::fs::read(grant_path(data, &corrupt)).unwrap(),
            b"not a journal"
        );
        // Once a deleted agent's checkout is gone, its journal goes too.
        std::fs::remove_dir(data.join("deleted-agent-work")).unwrap();
        settle_grant_journals(data, &owner, &unprotected).unwrap();
        assert!(!grant_path(data, &kept).exists());
    }

    /// A failed creation's checkout is removed only from the journal's own
    /// record, never through anything in the checkout, and never when an
    /// admitted agent or another journal names it, or when the checkout and
    /// its registration no longer name each other. Every refusal keeps both
    /// trees and quarantines the journal with the reason. Journals the
    /// server alone does not own and write are not acted on at all.
    #[test]
    fn settling_a_failed_creation_removes_only_its_own_checkout() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        let id = || anyagents_core::AgentId::generate();
        let (swapped, named, shared, sharer, loose) = (id(), id(), id(), id(), id());
        let swapped_checkout = failed_journal(data, &swapped, "swapped");
        // The seat-writable pointer now names another registration.
        let elsewhere = data.join("hub/.git/worktrees/elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(
            swapped_checkout.join(".git"),
            format!("gitdir: {}\n", elsewhere.display()),
        )
        .unwrap();
        let named_checkout = failed_journal(data, &named, "named");
        let shared_checkout = failed_journal(data, &shared, "shared");
        journal_of(data, &sharer, &shared_checkout, false);
        let loose_checkout = failed_journal(data, &loose, "loose");
        std::fs::set_permissions(
            grant_path(data, &loose),
            std::fs::Permissions::from_mode(0o666),
        )
        .unwrap();
        let owner = |agent: &anyagents_core::AgentId| {
            if agent == &sharer {
                JournalOwner::Live
            } else {
                JournalOwner::FailedCreation
            }
        };
        // An admitted workdir inside the checkout, named through a symlinked
        // alias: protection compares inodes, not text.
        let alias = data.join("alias");
        std::os::unix::fs::symlink(&named_checkout, &alias).unwrap();
        let live_workdir = std::fs::canonicalize(alias.join("src")).unwrap();
        let protected_ids: std::collections::HashSet<(u64, u64)> = live_workdir
            .ancestors()
            .filter_map(|dir| std::fs::metadata(dir).ok())
            .map(|meta| (meta.dev(), meta.ino()))
            .collect();
        let protected = |dev: u64, ino: u64| protected_ids.contains(&(dev, ino));
        settle_grant_journals(data, &owner, &protected).unwrap();
        for (agent, checkout, expected) in [
            (&swapped, &swapped_checkout, "points at another directory"),
            (&named, &named_checkout, "admitted agent's workdir"),
            (&shared, &shared_checkout, "also recorded"),
        ] {
            let reason = reason_of(data, agent);
            assert!(reason.contains(expected), "{reason}");
            assert_eq!(
                std::fs::read_to_string(checkout.join("src/file")).unwrap(),
                "provisioned"
            );
        }
        assert!(elsewhere.exists());
        assert!(data.join("hub/.git/worktrees/swapped").exists());
        // A journal others can write is left exactly as it is.
        assert!(loose_checkout.join("src/file").exists());
        let untouched: Grant =
            serde_json::from_slice(&std::fs::read(grant_path(data, &loose)).unwrap()).unwrap();
        assert!(!untouched.quarantined);
        // A journal directory others can write settles nothing.
        let journals = data.join("worktree-grants");
        std::fs::set_permissions(&journals, std::fs::Permissions::from_mode(0o777)).unwrap();
        let error = settle_grant_journals(data, &owner, &protected).unwrap_err();
        std::fs::set_permissions(&journals, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            error.to_string().contains("writable by no one else"),
            "{error}"
        );
    }

    /// The review's attack on the settle pass: after a failed creation, a
    /// component of its checkout path is swapped for a symlink to a directory
    /// the server must keep, with a `.git` planted there that names the
    /// registration. Nothing is followed and nothing outside is removed; the
    /// journal is quarantined. A symlink inside a real checkout is removed
    /// itself, never followed.
    #[test]
    fn settling_never_follows_a_swapped_path_or_a_symlink_inside() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        let id = || anyagents_core::AgentId::generate();
        let (swapped, honest) = (id(), id());
        std::fs::create_dir_all(data.join("wt/x")).unwrap();
        let (common, private, _) = registered_checkout(data, "victim");
        // The journal records wt/x/agent; move it aside and plant a symlink.
        let checkout = data.join("wt/x/agent");
        std::fs::rename(data.join("victim"), &checkout).unwrap();
        std::fs::write(
            private.join("gitdir"),
            format!("{}\n", checkout.join(".git").display()),
        )
        .unwrap();
        write_grant(
            &grant_path(data, &swapped),
            &Grant {
                common: common.clone(),
                private: private.clone(),
                checkout: checkout.clone(),
                uid: crate::sandbox::server_uid(),
                gid: crate::sandbox::server_gid(),
                active: false,
                inspection: false,
                inspection_paths: None,
                quarantined: false,
                reason: None,
            },
        )
        .unwrap();
        let precious = data.join("precious");
        std::fs::create_dir_all(precious.join("agent")).unwrap();
        std::fs::write(precious.join("agent/state.db"), "keep me").unwrap();
        std::fs::write(
            precious.join("agent/.git"),
            format!("gitdir: {}\n", private.display()),
        )
        .unwrap();
        std::fs::rename(data.join("wt/x"), data.join("wt/x.real")).unwrap();
        std::os::unix::fs::symlink(&precious, data.join("wt/x")).unwrap();
        // A failed creation whose checkout holds a symlink to the outside.
        let honest_checkout = failed_journal(data, &honest, "honest");
        let outside = data.join("outside-target");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("file"), "outside").unwrap();
        std::os::unix::fs::symlink(&outside, honest_checkout.join("src/link")).unwrap();
        let owner = |_: &anyagents_core::AgentId| JournalOwner::FailedCreation;
        settle_grant_journals(data, &owner, &|_, _| false).unwrap();
        assert_eq!(
            std::fs::read_to_string(precious.join("agent/state.db")).unwrap(),
            "keep me"
        );
        assert!(data.join("wt/x.real/agent/src/file").exists());
        assert!(private.exists());
        let reason = reason_of(data, &swapped);
        assert!(reason.contains("without following a symlink"), "{reason}");
        assert!(!honest_checkout.exists());
        assert_eq!(
            std::fs::read_to_string(outside.join("file")).unwrap(),
            "outside"
        );
    }

    /// A journal written before this release still reads: its store
    /// provenance and inspection device numbers are ignored.
    #[test]
    fn journals_from_before_the_hub_witness_still_read() {
        let legacy = r#"{"common":"/hub/.git","gid":2104,"uid":2104,
            "private":"/hub/.git/worktrees/checkout5","checkout":"/w/agt",
            "active":false,"inspection":false,"inspection_paths":[
            {"path":"/hub/.git/HEAD","dev":65008,"ino":7}],"quarantined":false,
            "config":{"directory":{"path":"/hub/.git/anyagents-config","dev":65008,"ino":1},
            "link":{"path":"/hub/.git/config","dev":65008,"ino":2},"adoption_source":null}}"#;
        let grant: Grant = serde_json::from_str(legacy).unwrap();
        assert_eq!(grant.inspection_paths.unwrap()[0].ino, 7);
        assert!(grant.reason.is_none());
    }

    /// Three principals with real ACLs, as the privileged cross-seat CI job
    /// has them, without adding accounts: the server (root here), the store's
    /// owner A and a seat B, as numeric ids. A's own 0600 config is relocated
    /// behind a server-made link; the directory and file stay A's, so A keeps
    /// rewriting it through the link, and B reads every version and can write
    /// none. Run it as root: `sudo env TMPDIR=<dir on an ACL file system>
    /// <test binary> --exact worktree_access::tests::privileged_store_owner_keeps_writing_its_relocated_config`.
    #[cfg(target_os = "linux")]
    #[test]
    fn privileged_store_owner_keeps_writing_its_relocated_config() {
        use std::process::Command;
        if unsafe { libc::geteuid() } != 0 {
            assert!(
                std::env::var_os("ANYAGENTS_REQUIRE_PRIVILEGED_WORKTREE_TEST").is_none(),
                "requires root"
            );
            return;
        }
        let (a, b) = (3_999_901u32, 3_999_902u32);
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let source = root.join("source");
        let checkout = root.join("checkout");
        std::fs::create_dir(&source).unwrap();
        let git = crate::worktree::tests::git;
        git(&source, &["init", "-q", "-b", "main"]);
        git(&source, &["config", "user.name", "Store owner"]);
        git(&source, &["config", "user.email", "owner@example.invalid"]);
        git(&source, &["config", "extensions.worktreeConfig", "true"]);
        git(&source, &["commit", "-q", "--allow-empty", "-m", "base"]);
        git(
            &source,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "child",
                checkout.to_str().unwrap(),
            ],
        );
        let common = root.join("source/.git");
        let config = common.join("config");
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
        for tree in [&source, &checkout] {
            assert!(
                Command::new("chown")
                    .args(["-R", &format!("{a}:{a}")])
                    .arg(tree)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let before = std::fs::metadata(&config).unwrap();
        let private = common.join("worktrees/checkout");
        let journal = grant_path(&root.join("data"), &anyagents_core::AgentId::generate());
        share_known(&checkout, &private, &common, b, b, &journal).unwrap();

        // The server made the link; the directory and the file stay A's, and
        // nobody but A writes either.
        let link = std::fs::symlink_metadata(&config).unwrap();
        assert!(link.file_type().is_symlink() && link.uid() == 0);
        let directory = std::fs::metadata(common.join(CONFIG_DIRECTORY)).unwrap();
        let file = std::fs::metadata(common.join(CONFIG_DIRECTORY).join("config")).unwrap();
        assert_eq!(
            (directory.uid(), file.uid(), file.ino()),
            (a, a, before.ino())
        );
        assert_eq!((directory.mode() & 0o022, file.mode() & 0o022), (0, 0));
        assert!(is_managed(&common));

        let as_principal = |uid: u32, args: &[&str]| {
            Command::new("/usr/bin/setpriv")
                .args([
                    "--reuid",
                    &uid.to_string(),
                    "--regid",
                    &uid.to_string(),
                    "--clear-groups",
                    "--",
                    "/usr/bin/env",
                    "-i",
                    "PATH=/usr/bin:/bin",
                    "HOME=/nonexistent",
                    "GIT_CONFIG_GLOBAL=/dev/null",
                    "GIT_CONFIG_SYSTEM=/dev/null",
                    "git",
                    "-c",
                    "safe.directory=*",
                ])
                .args(args)
                .output()
                .unwrap()
        };
        let read = |uid: u32| {
            let out = as_principal(
                uid,
                &[
                    "-C",
                    checkout.to_str().unwrap(),
                    "config",
                    "--get",
                    "user.name",
                ],
            );
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8(out.stdout).unwrap().trim().to_string()
        };
        assert_eq!(read(b), "Store owner");
        // A rewrites through the link (lock and rename in its directory).
        let out = as_principal(
            a,
            &[
                "-C",
                source.to_str().unwrap(),
                "config",
                "user.name",
                "Rewritten",
            ],
        );
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            std::fs::symlink_metadata(&config)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(read(b), "Rewritten");
        assert!(is_managed(&common));
        // B never writes it.
        let out = as_principal(
            b,
            &[
                "-C",
                checkout.to_str().unwrap(),
                "config",
                "user.name",
                "Seat",
            ],
        );
        assert!(!out.status.success());
        assert_eq!(read(b), "Rewritten");
        let out = as_principal(
            b,
            &[
                "-C",
                checkout.to_str().unwrap(),
                "rev-parse",
                "--path-format=absolute",
                "--git-dir",
            ],
        );
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
