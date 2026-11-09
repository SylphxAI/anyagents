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

fn grant(file: &File, uid: u32, gid: u32, capability: bool) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_dir() && (!meta.is_file() || meta.nlink() != 1) {
        return Err(io::Error::other(
            "seat sharing requires a directory or single-link regular file",
        ));
    }
    if meta.uid() == uid && meta.is_dir() && meta.mode() & 0o700 == 0o700 {
        return Ok(());
    }
    if meta.uid() == uid && !meta.is_dir() && !capability && meta.mode() & 0o600 == 0o600 {
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
    if after.uid() == uid && after.gid() == gid && after.mode() & 0o700 == 0o700 && meta.is_dir() {
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
    if meta.uid() == uid
        && ((meta.is_dir() && meta.mode() & 0o700 == 0o700)
            || (!meta.is_dir()
                && name.as_bytes() != b"mcp-capability"
                && meta.mode() & 0o600 == 0o600))
    {
        // The existing seat-private inode already has its owner's required
        // grant. Do not widen it just to reopen as the daemon.
        return Ok(None);
    }
    grant(&bound, uid, gid, name.as_bytes() == b"mcp-capability")?;
    Ok(Some(File::open(format!("/dev/fd/{fd}"))?))
}
#[cfg(not(target_os = "linux"))]
fn inaccessible_child(
    _parent: &File,
    _name: &OsStr,
    _uid: u32,
    _gid: u32,
    _expected: &libc::stat,
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
            "set -euC; umask 007; cat > \"/dev/fd/3/$1\"",
            "seat-copy",
        ])
        .arg(&temp)
        .stdin(source.try_clone()?)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let fd = parent.as_raw_fd();
    // Only async-signal-safe syscalls occur between fork and exec. Descriptor 3
    // binds the destination directory across privilege drop and path renames.
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
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
            "set -eu; cmp -s - \"/dev/fd/3/$1\"; sync -f \"/dev/fd/3/$1\"",
            "seat-readback",
        ])
        .arg(&temp)
        .stdin(source.try_clone()?)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    unsafe {
        verify.pre_exec(move || {
            if libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
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

pub(super) fn share(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
    share_open(open_root(path)?, path, uid, gid, false, &mut |_, _| {})
}

fn share_open(
    directory: File,
    path: &Path,
    uid: u32,
    gid: u32,
    owner_only: bool,
    before_open: &mut impl FnMut(&Path, &OsStr),
) -> io::Result<()> {
    grant(&directory, uid, gid, false)?;
    for name in names(&directory)? {
        let child_path = path.join(&name);
        let text = name.to_string_lossy();
        if text.starts_with(".seat-") {
            continue;
        }
        if matches!(
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
        ) || (text == "op" && path.ends_with(".config"))
        {
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
        let owner_only = owner_only || matches!(text.as_ref(), "cache-home" | "tmp");
        if owner_only && stat.st_uid != super::sandbox::server_uid() && stat.st_uid != uid {
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
                match inaccessible_child(&directory, &name, uid, gid, &stat) {
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
        if kind == libc::S_IFDIR {
            match share_open(child, &child_path, uid, gid, owner_only, before_open) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            }
        } else {
            grant(&child, uid, gid, text == "mcp-capability")?;
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
            std::fs::set_permissions(&cap, std::fs::Permissions::from_mode(0o660)).unwrap();
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
            false,
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
