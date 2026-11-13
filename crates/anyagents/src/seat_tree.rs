//! Descriptor-bound sharing of mutable config and host overlays. Names select
//! candidates only; metadata, recursion and effects use the opened inode.
use std::{
    ffi::{CString, OsStr},
    fs::File,
    io,
    os::fd::{AsRawFd, FromRawFd},
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Component, Path},
};

fn open_child(parent: &File, name: &OsStr, directory: bool) -> io::Result<File> {
    let name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
    let flags = libc::O_RDONLY
        | libc::O_NOFOLLOW
        | libc::O_NONBLOCK
        | libc::O_CLOEXEC
        | if directory { libc::O_DIRECTORY } else { 0 };
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn open_root(path: &Path) -> io::Result<File> {
    let absolute = std::path::absolute(path)?;
    let mut directory = File::open("/")?;
    for part in absolute.components() {
        match part {
            Component::RootDir => (),
            Component::Normal(name) => directory = open_child(&directory, name, true)?,
            _ => return Err(io::Error::other("unsupported seat path component")),
        }
    }
    Ok(directory)
}

fn names(directory: &File) -> io::Result<Vec<std::ffi::OsString>> {
    // fdopendir owns a duplicate; neither enumeration nor recursion reopens a
    // pathname that the seat can replace while the grant is in progress.
    let fd = unsafe { libc::dup(directory.as_raw_fd()) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let stream = unsafe { libc::fdopendir(fd) };
    if stream.is_null() {
        unsafe {
            libc::close(fd);
        }
        return Err(io::Error::last_os_error());
    }
    struct Stream(*mut libc::DIR);
    impl Drop for Stream {
        fn drop(&mut self) {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    let stream = Stream(stream);
    let mut out = Vec::new();
    loop {
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            break;
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            out.push(OsStr::from_bytes(name).to_owned());
        }
    }
    Ok(out)
}

fn grant(file: &File, uid: u32, gid: u32, capability: bool, daemon_access: bool) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_dir() && (!meta.is_file() || meta.nlink() != 1) {
        return Err(io::Error::other(
            "seat sharing requires a directory or single-link regular file",
        ));
    }
    if !daemon_access && meta.uid() == uid && meta.is_dir() && meta.mode() & 0o700 == 0o700 {
        return Ok(());
    }
    if !daemon_access
        && meta.uid() == uid
        && !meta.is_dir()
        && !capability
        && meta.mode() & 0o600 == 0o600
    {
        return Ok(());
    }
    let mode: libc::mode_t = if meta.is_dir() {
        0o2770
    } else if capability {
        0o640
    } else if meta.mode() & 0o100 != 0 {
        0o770
    } else {
        0o660
    };
    #[allow(
        clippy::unnecessary_cast,
        reason = "MetadataExt::mode is u32 while libc::mode_t is u16 on macOS"
    )]
    if meta.gid() == gid && (meta.mode() as libc::mode_t & 0o7777) == mode {
        return Ok(());
    }
    let fd = file.as_raw_fd();
    let group_ok = meta.gid() == gid || unsafe { libc::fchown(fd, u32::MAX, gid) } == 0;
    let mode_ok = unsafe { libc::fchmod(fd, mode) } == 0;
    if group_ok && mode_ok {
        return Ok(());
    }
    // Retain the configured owner's existing ability to repair a shared group,
    // but pass only the opened object, never its replaceable original path.
    if (!super::sandbox::is_isolated_test_env()
        || std::env::var_os("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").is_some())
        && let Some(launcher) = super::sandbox::descriptor_launcher()
    {
        let result = std::process::Command::new(launcher)
            .args([
                "--reuid",
                &meta.uid().to_string(),
                "--regid",
                &meta.gid().to_string(),
                "--groups",
                &format!("{},{gid}", meta.gid()),
                "--no-new-privs",
                "--",
                "/bin/sh",
                "-c",
                "chgrp \"$1\" /dev/fd/0 && chmod \"$2\" /dev/fd/0",
                "seat-inode",
                &gid.to_string(),
                &format!("{mode:o}"),
            ])
            .stdin(file.try_clone()?)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;
        if result.success() {
            return Ok(());
        }
    }
    // Seat-owned objects with the intended effective grant need no mutation.
    let after = file.metadata()?;
    if !daemon_access
        && after.uid() == uid
        && after.gid() == gid
        && after.mode() & 0o700 == 0o700
        && meta.is_dir()
    {
        return Ok(());
    }
    Err(io::Error::other("cannot grant the opened seat inode"))
}

/// O_PATH binds even an unreadable inode before delegating its mode repair.
/// Reopening /dev/fd/N uses that same opened object, never the mutable name.
#[cfg(target_os = "linux")]
fn inaccessible_child(
    parent: &File,
    name: &OsStr,
    uid: u32,
    gid: u32,
    expected: &libc::stat,
    access: ShareAccess,
) -> io::Result<Option<File>> {
    let name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let bound = unsafe { File::from_raw_fd(fd) };
    let meta = bound.metadata()?;
    if meta.dev() != expected.st_dev || meta.ino() != expected.st_ino {
        return Err(io::Error::other(
            "seat tree inode changed before owner repair",
        ));
    }
    if access.runtime_tree && meta.uid() != super::sandbox::server_uid() && meta.uid() != uid {
        return Ok(None);
    }
    if !access.daemon_access
        && meta.uid() == uid
        && ((meta.is_dir() && meta.mode() & 0o700 == 0o700)
            || (!meta.is_dir()
                && name.as_bytes() != b"mcp-capability"
                && meta.mode() & 0o600 == 0o600))
    {
        // The existing seat-private inode already has its owner's required
        // grant. Do not widen it just to reopen as the daemon.
        return Ok(None);
    }
    grant(
        &bound,
        uid,
        gid,
        name.as_bytes() == b"mcp-capability",
        access.daemon_access,
    )?;
    Ok(Some(File::open(format!("/dev/fd/{fd}"))?))
}
#[cfg(not(target_os = "linux"))]
fn inaccessible_child(
    _parent: &File,
    _name: &OsStr,
    _uid: u32,
    _gid: u32,
    _expected: &libc::stat,
    _access: ShareAccess,
) -> io::Result<Option<File>> {
    Err(io::Error::from(io::ErrorKind::PermissionDenied))
}

#[cfg(target_os = "linux")]
fn migrate_open_file(
    parent: &File,
    name: &OsStr,
    source: &File,
    uid: u32,
    gid: u32,
) -> io::Result<()> {
    use std::io::{Seek, SeekFrom};
    use std::os::unix::process::CommandExt as _;
    let before = source.metadata()?;
    if before.uid() == uid {
        return Ok(());
    }
    if unsafe { libc::fchown(source.as_raw_fd(), uid, gid) } == 0 {
        return Ok(());
    }
    let launcher = super::sandbox::descriptor_launcher()
        .ok_or_else(|| io::Error::other("seat migration launcher missing"))?;
    let nonce = format!(
        "{}-{}-{}",
        std::process::id(),
        anyagents_core::now_ms(),
        before.ino()
    );
    let temp = std::ffi::OsString::from(format!(".seat-copy-{nonce}"));
    let backup = std::ffi::OsString::from(format!(".seat-before-{nonce}"));
    let fd = parent.as_raw_fd();
    struct TempCopyCleanup<'a> {
        parent: &'a File,
        temp: &'a OsStr,
        launcher: &'a Path,
        uid: u32,
        gid: u32,
        armed: bool,
    }
    impl Drop for TempCopyCleanup<'_> {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            if let Ok(c_temp) = CString::new(self.temp.as_bytes()) {
                let rc = unsafe { libc::unlinkat(self.parent.as_raw_fd(), c_temp.as_ptr(), 0) };
                if rc == 0 || io::Error::last_os_error().kind() == io::ErrorKind::NotFound {
                    return;
                }
            }
            let dir_fd = self.parent.as_raw_fd();
            let mut rm = std::process::Command::new(self.launcher);
            rm.args([
                "--reuid",
                &self.uid.to_string(),
                "--regid",
                &self.gid.to_string(),
                "--groups",
                &self.gid.to_string(),
                "--no-new-privs",
                "--",
                "/bin/rm",
                "-f",
                "--",
            ])
            .arg(Path::new(".").join(self.temp))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
            unsafe {
                rm.pre_exec(move || {
                    if libc::fchdir(dir_fd) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let _ = rm.status();
        }
    }
    let mut cleanup = TempCopyCleanup {
        parent,
        temp: &temp,
        launcher: &launcher,
        uid,
        gid,
        armed: true,
    };
    let mut command = std::process::Command::new(&launcher);
    command
        .args([
            "--reuid",
            &uid.to_string(),
            "--regid",
            &gid.to_string(),
            "--groups",
            &gid.to_string(),
            "--no-new-privs",
            "--",
            "/bin/sh",
            "-c",
            "set -euC; umask 007; cat > \"./$1\"",
            "seat-copy",
        ])
        .arg(&temp)
        .stdin(source.try_clone()?)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    // Bind working directory directly to the opened parent descriptor via fchdir
    // before privilege drop, avoiding /dev/fd/3 (/proc/self/fd/3) traversal when
    // setresuid sets PR_SET_DUMPABLE=0 (mode 0500 root:root on /proc/self/fd).
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(fd) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut original = source.try_clone()?;
    original.seek(SeekFrom::Start(0))?;
    if !command.status()?.success() {
        return Err(io::Error::other(
            "seat inode copy refused; original retained",
        ));
    }
    original.seek(SeekFrom::Start(0))?;
    // The new inode belongs to the seat, which may not share a daemon group.
    // Read back under that exact owner rather than widening it for inspection.
    let mut verify = std::process::Command::new(&launcher);
    verify
        .args([
            "--reuid",
            &uid.to_string(),
            "--regid",
            &gid.to_string(),
            "--groups",
            &gid.to_string(),
            "--no-new-privs",
            "--",
            "/bin/sh",
            "-c",
            "set -eu; cmp -s - \"./$1\"; sync -f \"./$1\"",
            "seat-readback",
        ])
        .arg(&temp)
        .stdin(source.try_clone()?)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    unsafe {
        verify.pre_exec(move || {
            if libc::fchdir(fd) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    if !verify.status()?.success() {
        return Err(io::Error::other(
            "seat copy readback differs; original retained",
        ));
    }
    let after = source.metadata()?;
    if (
        before.dev(),
        before.ino(),
        before.len(),
        before.mtime(),
        before.mtime_nsec(),
    ) != (
        after.dev(),
        after.ino(),
        after.len(),
        after.mtime(),
        after.mtime_nsec(),
    ) {
        return Err(io::Error::other(
            "seat source changed during copy; both copies retained",
        ));
    }
    let rename = |from: &OsStr, to: &OsStr| -> io::Result<()> {
        let from = CString::new(from.as_bytes()).map_err(io::Error::other)?;
        let to = CString::new(to.as_bytes()).map_err(io::Error::other)?;
        if unsafe {
            libc::renameat2(
                parent.as_raw_fd(),
                from.as_ptr(),
                parent.as_raw_fd(),
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        } == 0
        {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    };
    // Capture the old name without overwriting anything. Keep its complete bytes
    // as a recovery object; a changed name is restored only when still absent.
    rename(name, &backup)?;
    let retained = open_child(parent, &backup, false)?.metadata()?;
    if (retained.dev(), retained.ino()) != (before.dev(), before.ino()) {
        let _ = rename(&backup, name);
        return Err(io::Error::other(
            "seat source name changed; recovery copies retained",
        ));
    }
    if let Err(err) = rename(&temp, name) {
        let _ = rename(&backup, name);
        return Err(err);
    }
    cleanup.armed = false;
    parent.sync_all()?;
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn migrate_open_file(
    _parent: &File,
    _name: &OsStr,
    _source: &File,
    _uid: u32,
    _gid: u32,
) -> io::Result<()> {
    Err(io::Error::other("managed UID file handoff requires Linux"))
}

#[derive(Clone, Copy, Default)]
struct ShareAccess {
    daemon_access: bool,
    runtime_tree: bool,
}

pub(super) fn share(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
    share_open(
        open_root(path)?,
        path,
        uid,
        gid,
        ShareAccess::default(),
        &mut |_, _| {},
    )
}

/// Config is jointly maintained by the daemon and its admitted principal.
/// Vendor-created private modes must be repaired before any daemon config read.
pub(super) fn share_config(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
    let directory = match open_root(path) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            let absolute = std::path::absolute(path)?;
            let parent = open_root(
                absolute
                    .parent()
                    .ok_or_else(|| io::Error::other("config parent missing"))?,
            )?;
            let name = absolute
                .file_name()
                .ok_or_else(|| io::Error::other("config name missing"))?;
            let c_name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            if unsafe {
                libc::fstatat(
                    parent.as_raw_fd(),
                    c_name.as_ptr(),
                    stat.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
            inaccessible_child(
                &parent,
                name,
                uid,
                gid,
                &unsafe { stat.assume_init() },
                ShareAccess {
                    daemon_access: true,
                    runtime_tree: false,
                },
            )?
            .ok_or_else(|| io::Error::other("config root repair did not grant daemon access"))?
        }
        Err(error) => return Err(error),
    };
    share_open(
        directory,
        path,
        uid,
        gid,
        ShareAccess {
            daemon_access: true,
            runtime_tree: false,
        },
        &mut |_, _| {},
    )
}

fn tighten_private_inode(
    file: &File,
    meta: &std::fs::Metadata,
    uid: u32,
    gid: u32,
    mode: libc::mode_t,
) -> io::Result<()> {
    if meta.uid() != uid {
        return Ok(());
    }
    #[allow(
        clippy::unnecessary_cast,
        reason = "MetadataExt::mode is u32 while libc::mode_t is u16 on macOS"
    )]
    if (meta.mode() as libc::mode_t & 0o7777) == mode {
        return Ok(());
    }
    if unsafe { libc::fchmod(file.as_raw_fd(), mode) } == 0 {
        return Ok(());
    }
    if (!super::sandbox::is_isolated_test_env()
        || std::env::var_os("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").is_some())
        && let Some(launcher) = super::sandbox::descriptor_launcher()
    {
        let status = std::process::Command::new(launcher)
            .args([
                "--reuid",
                &meta.uid().to_string(),
                "--regid",
                &meta.gid().to_string(),
                "--groups",
                &format!("{},{gid}", meta.gid()),
                "--no-new-privs",
                "--",
                "/bin/sh",
                "-c",
                "chmod $1 /dev/fd/0",
                "seat-inode",
                &format!("{mode:o}"),
            ])
            .stdin(file.try_clone()?)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?;
        if status.success() {
            return Ok(());
        }
    }
    Ok(())
}

fn tighten_private_op_dir(parent: &File, name: &OsStr, uid: u32, gid: u32) -> io::Result<()> {
    match open_child(parent, name, true) {
        Ok(op_dir) => {
            let meta = op_dir.metadata()?;
            if !meta.is_dir() {
                return Ok(());
            }
            for child_name in names(&op_dir).unwrap_or_default() {
                if let Ok(child) = open_child(&op_dir, &child_name, false)
                    && let Ok(cmeta) = child.metadata()
                    && cmeta.is_file()
                    && cmeta.nlink() == 1
                {
                    let _ = tighten_private_inode(&child, &cmeta, uid, gid, 0o600);
                } else {
                    #[cfg(target_os = "linux")]
                    if let Ok(c_child) = CString::new(child_name.as_bytes()) {
                        let fd = unsafe {
                            libc::openat(
                                op_dir.as_raw_fd(),
                                c_child.as_ptr(),
                                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                            )
                        };
                        if fd >= 0 {
                            let bound = unsafe { File::from_raw_fd(fd) };
                            if let Ok(cmeta) = bound.metadata()
                                && cmeta.is_file()
                                && cmeta.nlink() == 1
                            {
                                let _ = tighten_private_inode(&bound, &cmeta, uid, gid, 0o600);
                            }
                        }
                    }
                }
            }
            let _ = tighten_private_inode(&op_dir, &meta, uid, gid, 0o700);
        }
        #[cfg(target_os = "linux")]
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            let c_name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
            let fd = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    c_name.as_ptr(),
                    libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd >= 0 {
                let bound = unsafe { File::from_raw_fd(fd) };
                if let Ok(meta) = bound.metadata()
                    && meta.is_dir()
                    && meta.uid() == uid
                    && (!super::sandbox::is_isolated_test_env()
                        || std::env::var_os("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").is_some())
                    && let Some(launcher) = super::sandbox::descriptor_launcher()
                {
                    let _ = std::process::Command::new(launcher)
                        .args([
                            "--reuid",
                            &meta.uid().to_string(),
                            "--regid",
                            &meta.gid().to_string(),
                            "--groups",
                            &format!("{},{gid}", meta.gid()),
                            "--no-new-privs",
                            "--",
                            "/bin/sh",
                            "-c",
                            "chmod 700 /dev/fd/0 && if [ -f /dev/fd/0/config ] && [ ! -L /dev/fd/0/config ]; then chmod 600 /dev/fd/0/config; fi",
                        ])
                        .stdin(bound.try_clone()?)
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status();
                }
            }
        }
        Err(_) => {}
    }
    Ok(())
}

fn share_open(
    directory: File,
    path: &Path,
    uid: u32,
    gid: u32,
    access: ShareAccess,
    before_open: &mut impl FnMut(&Path, &OsStr),
) -> io::Result<()> {
    grant(&directory, uid, gid, false, access.daemon_access)?;
    for name in names(&directory)? {
        let child_path = path.join(&name);
        let text = name.to_string_lossy();
        if text.starts_with(".seat-") {
            if text.starts_with(".seat-copy-")
                && let Ok(c_name) = CString::new(name.as_bytes())
            {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                if unsafe {
                    libc::fstatat(
                        directory.as_raw_fd(),
                        c_name.as_ptr(),
                        stat.as_mut_ptr(),
                        libc::AT_SYMLINK_NOFOLLOW,
                    )
                } == 0
                {
                    let stat = unsafe { stat.assume_init() };
                    let now_secs = (anyagents_core::now_ms() / 1000).max(0);
                    if (stat.st_mode & libc::S_IFMT) == libc::S_IFREG
                        && now_secs.saturating_sub(stat.st_mtime.max(0)) >= 60
                    {
                        let _ =
                            unsafe { libc::unlinkat(directory.as_raw_fd(), c_name.as_ptr(), 0) };
                    }
                }
            }
            continue;
        }
        if !access.runtime_tree
            && (matches!(
                text.as_ref(),
                ".ssh"
                    | ".gnupg"
                    | "node_modules"
                    | ".bun"
                    | ".turbo"
                    | ".next"
                    | "target"
                    | ".git"
                    | "data-home"
                    | "state-home"
                    | ".cargo"
                    | ".rustup"
                    | "brain"
                    | ".system_generated"
                    | "plugins"
                    | "sessions"
                    | "shell-snapshots"
                    | "telemetry"
                    | "statsig"
                    | "__pycache__"
            ))
        {
            continue;
        }
        if !access.runtime_tree
            && text == "op"
            && (path.ends_with(".config") || path.ends_with("config-home"))
        {
            let _ = tighten_private_op_dir(&directory, &name, uid, gid);
            continue;
        }
        let c_name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe {
            libc::fstatat(
                directory.as_raw_fd(),
                c_name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::NotFound {
                continue;
            }
            return Err(err);
        }
        let stat = unsafe { stat.assume_init() };
        // Runtime scratch/cache trees need the same admitted seat grant after
        // an execution-identity handoff. Do not mutate a foreign owner's tree.
        let access = ShareAccess {
            runtime_tree: access.runtime_tree || matches!(text.as_ref(), "cache-home" | "tmp"),
            ..access
        };
        if access.runtime_tree && stat.st_uid != super::sandbox::server_uid() && stat.st_uid != uid
        {
            continue;
        }
        let kind = stat.st_mode & libc::S_IFMT;
        if kind == libc::S_IFLNK || kind == libc::S_IFSOCK || kind == libc::S_IFIFO {
            continue;
        }
        if kind != libc::S_IFDIR && kind != libc::S_IFREG {
            return Err(io::Error::other("unsupported seat tree object"));
        }
        before_open(path, &name);
        let child = match open_child(&directory, &name, kind == libc::S_IFDIR) {
            Ok(child) => child,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                match inaccessible_child(&directory, &name, uid, gid, &stat, access) {
                    Ok(Some(child)) => child,
                    Ok(None) => continue,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                }
            }
            Err(error) => return Err(error),
        };
        let opened = child.metadata()?;
        #[allow(
            clippy::unnecessary_cast,
            reason = "libc dev_t differs between Linux and macOS"
        )]
        let same_inode = opened.dev() == stat.st_dev as u64 && opened.ino() == stat.st_ino as u64;
        if !same_inode {
            return Err(io::Error::other(
                "seat tree inode changed during enumeration",
            ));
        }
        if access.runtime_tree
            && opened.uid() != super::sandbox::server_uid()
            && opened.uid() != uid
        {
            continue;
        }
        if kind == libc::S_IFDIR {
            match share_open(child, &child_path, uid, gid, access, before_open) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            }
        } else {
            // Runtime caches (`cache-home`, `tmp`) are append-only and
            // legitimately hardlinked inside the same tree (uv's archive
            // layout hardlinks wheels across archive dirs). Refusing every
            // hardlink breaks any seat whose tooling populated its cache;
            // the seat already owns these addresses. Only the grant is
            // skipped, and only for hardlinked cache files: single-link
            // cache files still get read/write treatment for their mode.
            let needs_migration = !text.starts_with(".seat-")
                && (text.starts_with("opencode.db")
                    || text == "serve-exit.jsonl"
                    || text.contains(".sqlite"));
            if access.runtime_tree
                && !needs_migration
                && child.metadata().is_ok_and(|meta| meta.nlink() > 1)
            {
                continue;
            }
            grant(
                &child,
                uid,
                gid,
                text == "mcp-capability",
                access.daemon_access,
            )?;
            if needs_migration && child.metadata()?.uid() != uid {
                match migrate_open_file(&directory, &name, &child, uid, gid) {
                    Ok(()) => (),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                }
            }
        }
    }
    Ok(())
}

/// Read-only handle to a seat-owned SQLite database that never creates or mutates
/// `-wal`, `-shm`, or `-journal` sidecars in the seat directory (preventing the
/// daemon from claiming ownership of WAL/SHM sidecars away from the seat user).
struct PrivateSnapshotDir(std::path::PathBuf);

impl PrivateSnapshotDir {
    fn create() -> Option<Self> {
        use std::os::unix::fs::DirBuilderExt as _;
        let base = std::env::temp_dir();
        let pid = std::process::id();
        for seq in 0..64u32 {
            let candidate = base.join(format!(
                "anyagents-ro-sqlite-{pid}-{}-{seq}",
                anyagents_core::now_ms()
            ));
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700);
            if builder.create(&candidate).is_ok() {
                return Some(Self(candidate));
            }
        }
        None
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for PrivateSnapshotDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) struct ReadOnlySeatDb {
    conn: rusqlite::Connection,
    _snapshot: Option<PrivateSnapshotDir>,
}

impl std::ops::Deref for ReadOnlySeatDb {
    type Target = rusqlite::Connection;
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

fn sqlite_immutable_uri(path: &Path) -> Option<String> {
    let abs = std::path::absolute(path).ok()?;
    let raw = abs.to_str()?;
    let mut out = String::with_capacity(raw.len() + 24);
    out.push_str("file:");
    if !raw.starts_with('/') {
        out.push('/');
    }
    for b in raw.bytes() {
        match b {
            b'%' => out.push_str("%25"),
            b'?' => out.push_str("%3f"),
            b'#' => out.push_str("%23"),
            b' ' => out.push_str("%20"),
            _ => out.push(b as char),
        }
    }
    out.push_str("?immutable=1");
    Some(out)
}

pub(crate) fn open_readonly_seat_sqlite(db_path: &Path) -> Option<ReadOnlySeatDb> {
    if !db_path.is_file() {
        return None;
    }
    let mut wal_os = db_path.as_os_str().to_os_string();
    wal_os.push("-wal");
    let wal_path = std::path::PathBuf::from(wal_os);
    let has_nonempty_wal = std::fs::metadata(&wal_path)
        .map(|m| m.is_file() && m.len() > 0)
        .unwrap_or(false);
    if !has_nonempty_wal && let Some(uri) = sqlite_immutable_uri(db_path) {
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_URI;
        if let Ok(conn) = rusqlite::Connection::open_with_flags(uri, flags) {
            let _ = conn.busy_timeout(std::time::Duration::from_millis(500));
            return Some(ReadOnlySeatDb {
                conn,
                _snapshot: None,
            });
        }
    }
    let tmp = PrivateSnapshotDir::create()?;
    let snap_db = tmp.path().join("snapshot.sqlite3");
    std::fs::copy(db_path, &snap_db).ok()?;
    if has_nonempty_wal {
        let _ = std::fs::copy(&wal_path, tmp.path().join("snapshot.sqlite3-wal"));
    }
    let conn = rusqlite::Connection::open(&snap_db).ok()?;
    let _ = conn.busy_timeout(std::time::Duration::from_millis(500));
    let _ = conn.pragma_update(None, "query_only", "ON");
    Some(ReadOnlySeatDb {
        conn,
        _snapshot: Some(tmp),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_cache_hardlinks_are_tolerated_but_top_level_links_are_refused() {
        let temp = crate::test_tempdir();
        let own = temp.path().join("config");
        let me = super::super::sandbox::server_uid();
        let seat_uid = if me == 42001 { 42002 } else { 42001 };
        let gid = super::super::sandbox::server_gid();
        // uv-style archive layout: one inode, two names, inside the cache.
        let cached = own.join("cache-home/uvcache/archive-v0/pkg/file.py");
        std::fs::create_dir_all(cached.parent().unwrap()).unwrap();
        std::fs::write(&cached, "cached").unwrap();
        std::fs::create_dir_all(own.join("cache-home/uvcache/pkg")).unwrap();
        std::fs::hard_link(&cached, own.join("cache-home/uvcache/pkg/link.py")).unwrap();
        share(&own, seat_uid, gid).unwrap();
        // A hardlinked file outside the runtime tree stays refused.
        let top = own.join("top.py");
        std::fs::write(&top, "top").unwrap();
        std::fs::hard_link(&top, own.join("again.py")).unwrap();
        let err = share(&own, seat_uid, gid).unwrap_err();
        assert!(err.to_string().contains("single-link"), "{err}");
    }

    #[test]
    fn capability_group_write_is_normalized_for_current_and_distinct_seat_owners() {
        use std::os::unix::fs::PermissionsExt;
        let temp = crate::test_tempdir();
        let own = temp.path().join("config");
        std::fs::create_dir(&own).unwrap();
        let cap = own.join(crate::operator::CAPABILITY_FILE);
        std::fs::write(&cap, "synthetic-capability").unwrap();
        let me = super::super::sandbox::server_uid();
        let gid = super::super::sandbox::server_gid();
        for seat_uid in [me, if me == 42001 { 42002 } else { 42001 }] {
            for mode in [0o600, 0o660, 0o664] {
                std::fs::set_permissions(&cap, std::fs::Permissions::from_mode(mode)).unwrap();
                share(&own, seat_uid, gid).unwrap();
                let meta = cap.metadata().unwrap();
                assert_eq!(meta.mode() & 0o7777, 0o640);
                assert_eq!(meta.gid(), gid);
                assert_eq!(
                    std::fs::read_to_string(&cap).unwrap(),
                    "synthetic-capability"
                );
            }
        }
    }

    #[test]
    fn server_owned_cache_and_tmp_descendants_receive_the_admitted_group_grant() {
        use std::os::unix::fs::PermissionsExt;
        let temp = crate::test_tempdir();
        let own = temp.path().join("config");
        let me = super::super::sandbox::server_uid();
        let seat_uid = if me == 42001 { 42002 } else { 42001 };
        let gid = super::super::sandbox::server_gid();
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o700)).unwrap();
        let before = outside.metadata().unwrap();
        for name in ["cache-home", "tmp"] {
            let nested = own.join(name).join("nested/deep");
            std::fs::create_dir_all(&nested).unwrap();
            std::fs::write(nested.join("retained"), "preserved").unwrap();
            for p in [
                own.join(name),
                own.join(name).join("nested"),
                nested.clone(),
            ] {
                std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
            std::os::unix::fs::symlink(&outside, own.join(name).join("foreign-link")).unwrap();
        }
        share(&own, seat_uid, gid).unwrap();
        for name in ["cache-home", "tmp"] {
            for suffix in ["", "nested", "nested/deep"] {
                let meta = own.join(name).join(suffix).metadata().unwrap();
                assert_eq!(meta.gid(), gid);
                assert_eq!(meta.mode() & 0o7777, 0o2770);
            }
            assert_eq!(
                std::fs::read_to_string(own.join(name).join("nested/deep/retained")).unwrap(),
                "preserved"
            );
        }
        let after = outside.metadata().unwrap();
        assert_eq!(
            (before.dev(), before.ino(), before.mode(), before.gid()),
            (after.dev(), after.ino(), after.mode(), after.gid())
        );
    }

    #[test]
    fn cache_directory_swap_refuses_without_changing_the_outside_inode() {
        use std::os::unix::fs::PermissionsExt;
        let temp = crate::test_tempdir();
        let config = temp.path().join("config");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(config.join("cache-home/nested")).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o700)).unwrap();
        let before = outside.metadata().unwrap();
        let mut swapped = false;
        let result = share_open(
            open_root(&config).unwrap(),
            &config,
            if super::super::sandbox::server_uid() == 42001 {
                42002
            } else {
                42001
            },
            super::super::sandbox::server_gid(),
            ShareAccess::default(),
            &mut |parent, name| {
                if parent.ends_with("cache-home") && name == "nested" {
                    std::fs::rename(parent.join(name), parent.join("held")).unwrap();
                    std::os::unix::fs::symlink(&outside, parent.join(name)).unwrap();
                    swapped = true;
                }
            },
        );
        assert!(swapped);
        assert!(result.is_err());
        let after = outside.metadata().unwrap();
        assert_eq!(
            (before.dev(), before.ino(), before.mode(), before.gid()),
            (after.dev(), after.ino(), after.mode(), after.gid())
        );
    }

    #[test]
    fn config_grants_group_access_to_private_vendor_objects_but_not_capability_writes() {
        use std::os::unix::fs::PermissionsExt;
        let temp = crate::test_tempdir();
        let config = temp.path().join("config");
        let vendor = config.join(".gemini/antigravity-cli");
        std::fs::create_dir_all(vendor.join("builtin")).unwrap();
        std::fs::create_dir_all(vendor.join("conversations")).unwrap();
        for path in [
            &config,
            &vendor,
            &vendor.join("builtin"),
            &vendor.join("conversations"),
        ] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let settings = vendor.join("settings.json");
        std::fs::write(&settings, b"{\"retained\":true}").unwrap();
        std::fs::set_permissions(&settings, std::fs::Permissions::from_mode(0o600)).unwrap();
        let capability = config.join("mcp-capability");
        std::fs::write(&capability, b"synthetic-capability").unwrap();
        share_config(
            &config,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
        )
        .unwrap();
        for path in [
            &config,
            &vendor,
            &vendor.join("builtin"),
            &vendor.join("conversations"),
        ] {
            assert_eq!(std::fs::metadata(path).unwrap().mode() & 0o7777, 0o2770);
        }
        assert_eq!(std::fs::metadata(&settings).unwrap().mode() & 0o777, 0o660);
        assert_eq!(std::fs::metadata(capability).unwrap().mode() & 0o777, 0o640);
        assert_eq!(std::fs::read(settings).unwrap(), b"{\"retained\":true}");

        let op_dir = config.join(".config/op");
        std::fs::create_dir_all(&op_dir).unwrap();
        let op_cfg = op_dir.join("config");
        std::fs::write(&op_cfg, b"{}").unwrap();
        std::fs::set_permissions(&op_dir, std::fs::Permissions::from_mode(0o2770)).unwrap();
        std::fs::set_permissions(&op_cfg, std::fs::Permissions::from_mode(0o660)).unwrap();
        share_config(
            &config,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
        )
        .unwrap();
        assert_eq!(std::fs::metadata(&op_dir).unwrap().mode() & 0o7777, 0o700);
        assert_eq!(std::fs::metadata(&op_cfg).unwrap().mode() & 0o777, 0o600);
    }

    #[test]
    fn config_sharing_never_changes_symlink_targets_or_hardlinked_private_files() {
        use std::os::unix::fs::PermissionsExt;
        let temp = crate::test_tempdir();
        let config = temp.path().join("config");
        std::fs::create_dir(&config).unwrap();
        let private = temp.path().join("private");
        std::fs::write(&private, b"preserve").unwrap();
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::os::unix::fs::symlink(&private, config.join("symlink")).unwrap();
        share_config(
            &config,
            crate::sandbox::server_uid(),
            crate::sandbox::server_gid(),
        )
        .unwrap();
        std::fs::hard_link(&private, config.join("hardlink")).unwrap();
        assert!(
            share_config(
                &config,
                crate::sandbox::server_uid(),
                crate::sandbox::server_gid()
            )
            .is_err()
        );
        assert_eq!(std::fs::metadata(private).unwrap().mode() & 0o777, 0o600);
    }

    #[test]
    fn planted_directory_to_symlink_swap_preserves_peer_inode_and_bytes() {
        let temp = crate::test_tempdir();
        let own = temp.path().join("own");
        let peer = temp.path().join("peer");
        std::fs::create_dir_all(own.join("nested")).unwrap();
        std::fs::create_dir(&peer).unwrap();
        std::fs::write(peer.join("private"), b"protected peer bytes").unwrap();
        let before = std::fs::metadata(&peer).unwrap();
        let file_before = std::fs::metadata(peer.join("private")).unwrap();
        let mut swapped = false;
        let result = share_open(
            open_root(&own).unwrap(),
            &own,
            super::super::sandbox::server_uid(),
            super::super::sandbox::server_gid(),
            ShareAccess {
                daemon_access: true,
                runtime_tree: false,
            },
            &mut |parent, name| {
                if name == "nested" {
                    std::fs::rename(parent.join(name), parent.join("held")).unwrap();
                    std::os::unix::fs::symlink(&peer, parent.join(name)).unwrap();
                    swapped = true;
                }
            },
        );
        assert!(swapped);
        assert!(matches!(
            result.unwrap_err().raw_os_error(),
            Some(libc::ELOOP | libc::ENOTDIR)
        ));
        let after = std::fs::metadata(&peer).unwrap();
        let file_after = std::fs::metadata(peer.join("private")).unwrap();
        assert_eq!(
            (
                before.dev(),
                before.ino(),
                before.uid(),
                before.gid(),
                before.mode()
            ),
            (
                after.dev(),
                after.ino(),
                after.uid(),
                after.gid(),
                after.mode()
            )
        );
        assert_eq!(
            (file_before.uid(), file_before.gid(), file_before.mode()),
            (file_after.uid(), file_after.gid(), file_after.mode())
        );
        assert_eq!(
            std::fs::read(peer.join("private")).unwrap(),
            b"protected peer bytes"
        );
    }

    #[test]
    fn open_readonly_seat_sqlite_never_creates_wal_or_shm_sidecars() {
        let temp = crate::test_tempdir();
        let cfg = temp.path().join("config");
        std::fs::create_dir_all(&cfg).unwrap();
        let db_path = cfg.join("sessions.sqlite3");

        {
            let writer = rusqlite::Connection::open(&db_path).unwrap();
            let _: String = writer
                .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
                .unwrap();
            writer
                .execute_batch(
                    "CREATE TABLE session_entries(session_id TEXT NOT NULL, idx INTEGER NOT NULL, event TEXT NOT NULL);
                     INSERT INTO session_entries VALUES('s1', 0, 'anyagents-message:msg_1');",
                )
                .unwrap();
        }
        assert!(!cfg.join("sessions.sqlite3-wal").exists());
        assert!(!cfg.join("sessions.sqlite3-shm").exists());

        {
            let ro = open_readonly_seat_sqlite(&db_path).expect("open WAL db immutable");
            let ev: String = ro
                .query_row(
                    "SELECT event FROM session_entries WHERE session_id = 's1'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(ev, "anyagents-message:msg_1");
            assert!(
                !cfg.join("sessions.sqlite3-wal").exists(),
                "daemon read-only query must never create -wal sidecar in seat config"
            );
            assert!(
                !cfg.join("sessions.sqlite3-shm").exists(),
                "daemon read-only query must never create -shm sidecar in seat config"
            );
        }

        let crash_cfg = temp.path().join("crash-config");
        std::fs::create_dir_all(&crash_cfg).unwrap();
        let writer = rusqlite::Connection::open(&db_path).unwrap();
        let _ = writer.pragma_update(None, "wal_autocheckpoint", 0);
        writer
            .execute(
                "INSERT INTO session_entries VALUES('s1', 1, 'anyagents-message:msg_2')",
                [],
            )
            .unwrap();
        assert!(
            std::fs::metadata(cfg.join("sessions.sqlite3-wal"))
                .unwrap()
                .len()
                > 0
        );
        std::fs::copy(&db_path, crash_cfg.join("sessions.sqlite3")).unwrap();
        std::fs::copy(
            cfg.join("sessions.sqlite3-wal"),
            crash_cfg.join("sessions.sqlite3-wal"),
        )
        .unwrap();
        drop(writer);

        let ro2 = open_readonly_seat_sqlite(&crash_cfg.join("sessions.sqlite3"))
            .expect("open snapshot with uncheckpointed WAL");
        let count: i64 = ro2
            .query_row(
                "SELECT COUNT(*) FROM session_entries WHERE session_id = 's1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 2,
            "must replay uncheckpointed WAL frames via private snapshot"
        );
        assert!(
            !crash_cfg.join("sessions.sqlite3-shm").exists(),
            "snapshot read must never create daemon-owned -shm in crash_cfg"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn migrate_open_file_uses_fchdir_without_dev_fd_3_and_unlinks_temp_on_failure() {
        use std::os::unix::fs::PermissionsExt;
        let _lock = crate::server::tests::env_lock();
        let temp = crate::test_tempdir();
        let launcher = temp.path().join("fake-seat-launcher.sh");
        std::fs::write(
            &launcher,
            r#"#!/usr/bin/env python3
import os, subprocess, sys
args = sys.argv[1:]
sep = args.index("--")
cmd = args[sep + 1:]
joined = " ".join(cmd)
if "/dev/fd/3" in joined:
    sys.exit(97)
mode = os.environ.get("FAKE_LAUNCHER_MODE", "ok")
if mode == "fail_copy_after_touch" and len(cmd) > 3 and cmd[3] == "seat-copy":
    open("./" + cmd[4], "wb").close()
    sys.exit(41)
if mode == "fail_verify" and len(cmd) > 3 and cmd[3] == "seat-readback":
    sys.exit(42)
os.execvp(cmd[0], cmd)
"#,
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cfg = temp.path().join("config");
        std::fs::create_dir_all(&cfg).unwrap();
        let shm = cfg.join("sessions.sqlite3-shm");
        std::fs::write(&shm, vec![0xABu8; 32768]).unwrap();
        let me = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid();
        let other_uid = if me == 42001 { 42002 } else { 42001 };

        struct EnvReset;
        impl Drop for EnvReset {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("ANYAGENTS_LAUNCHER");
                    std::env::remove_var("FAKE_LAUNCHER_MODE");
                }
            }
        }
        let _reset = EnvReset;
        unsafe {
            std::env::set_var("ANYAGENTS_LAUNCHER", &launcher);
            std::env::set_var("FAKE_LAUNCHER_MODE", "fail_copy_after_touch");
        }
        let parent = open_root(&cfg).unwrap();
        let child = open_child(&parent, OsStr::new("sessions.sqlite3-shm"), false).unwrap();
        let err = migrate_open_file(
            &parent,
            OsStr::new("sessions.sqlite3-shm"),
            &child,
            other_uid,
            gid,
        )
        .unwrap_err();
        assert!(err.to_string().contains("seat inode copy refused"));
        let leftovers: Vec<_> = std::fs::read_dir(&cfg)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".seat-copy-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "failed copy must unlink 0-byte .seat-copy-* temp file: {leftovers:?}"
        );

        unsafe {
            std::env::set_var("FAKE_LAUNCHER_MODE", "fail_verify");
        }
        let err2 = migrate_open_file(
            &parent,
            OsStr::new("sessions.sqlite3-shm"),
            &child,
            other_uid,
            gid,
        )
        .unwrap_err();
        assert!(err2.to_string().contains("seat copy readback differs"));
        let leftovers2: Vec<_> = std::fs::read_dir(&cfg)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".seat-copy-"))
            .collect();
        assert!(
            leftovers2.is_empty(),
            "failed readback must unlink .seat-copy-* temp file: {leftovers2:?}"
        );

        unsafe {
            std::env::set_var("FAKE_LAUNCHER_MODE", "ok");
        }
        migrate_open_file(
            &parent,
            OsStr::new("sessions.sqlite3-shm"),
            &child,
            other_uid,
            gid,
        )
        .expect("fchdir ./$1 copy + verify + atomic swap must succeed without /dev/fd/3");
        assert_eq!(std::fs::read(&shm).unwrap().len(), 32768);

        let stale_orphan = cfg.join(".seat-copy-67774-1000-99");
        std::fs::write(&stale_orphan, b"").unwrap();
        let c_stale = CString::new(stale_orphan.as_os_str().as_bytes()).unwrap();
        let old_ts = [
            libc::timespec {
                tv_sec: 1_700_000_000,
                tv_nsec: 0,
            },
            libc::timespec {
                tv_sec: 1_700_000_000,
                tv_nsec: 0,
            },
        ];
        unsafe {
            libc::utimensat(libc::AT_FDCWD, c_stale.as_ptr(), old_ts.as_ptr(), 0);
            std::env::remove_var("ANYAGENTS_LAUNCHER");
            std::env::remove_var("FAKE_LAUNCHER_MODE");
        }
        share_config(&cfg, me, gid).unwrap();
        assert!(
            !stale_orphan.exists(),
            "share_config must reap stale .seat-copy-* orphan"
        );
    }
}
