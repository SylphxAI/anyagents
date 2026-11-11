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
            "set -euC; umask 007; target=\"/dev/fd/3/$1\"; [ -d /dev/fd/3 ] || target=\"./$1\"; cat > \"$target\"",
            "seat-copy",
        ])
        .arg(&temp)
        .stdin(source.try_clone()?)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let fd = parent.as_raw_fd();
    // Only async-signal-safe syscalls occur between fork and exec. `fchdir(fd)`
    // (plus descriptor 3 when not stripped by `close_range(3, ~0, 0)`) binds
    // the exact destination directory inode across privilege drop and renames.
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(fd) < 0 || libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0
            {
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
            "set -eu; target=\"/dev/fd/3/$1\"; [ -d /dev/fd/3 ] || target=\"./$1\"; cmp -s - \"$target\"; sync -f \"$target\"",
            "seat-readback",
        ])
        .arg(&temp)
        .stdin(source.try_clone()?)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    unsafe {
        verify.pre_exec(move || {
            if libc::fchdir(fd) < 0 || libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0
            {
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
    rename(&temp, name)?;
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
            grant(
                &child,
                uid,
                gid,
                text == "mcp-capability",
                access.daemon_access,
            )?;
            if !text.starts_with(".seat-")
                && (text.starts_with("opencode.db")
                    || text == "serve-exit.jsonl"
                    || text.contains(".sqlite"))
                && child.metadata()?.uid() != uid
            {
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
