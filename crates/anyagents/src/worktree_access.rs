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
            .arg("-C")
            .arg(path)
            .args(["rev-parse", "--path-format=absolute", flag])
            .output()
            .map_err(|e| refusal(path, e))?;
        if !out.status.success() {
            return Err(refusal(path, String::from_utf8_lossy(&out.stderr)));
        }
        let dir = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }
    Ok(dirs)
}

/// Change only opened objects, never a symlink's target. Hard-linked files that
/// need changes are refused rather than changing an object outside this tree.
pub(crate) fn share(path: &Path, gid: u32) -> Result<(), Error> {
    let dirs = git_dirs(path)?;
    share_tree(path, gid).map_err(|e| refusal(path, e))?;
    for root in &dirs {
        // The common store can serve other agents. Never replace a distinct
        // group's existing grant just to make the newest child writable.
        share_tree_with_hook(root, gid, true, &|_| {}).map_err(|e| refusal(root, e))?;
    }
    Ok(())
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
                    libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC,
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
        gid: u32,
        preserve_group_grants: bool,
        before_open: &dyn Fn(&Path),
    ) -> std::io::Result<()> {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::fs::MetadataExt;
        let meta = file.metadata()?;
        let fd = file.as_raw_fd();
        let mode = meta.mode() & 0o7777;
        if preserve_group_grants {
            grant_metadata_acl(file, gid).map_err(|e| {
                std::io::Error::other(format!(
                    "{}: Git metadata ACL for group {gid}: {e}",
                    path.display()
                ))
            })?;
        } else {
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
            // SAFETY: fd is owned and remains open throughout changes/readback.
            if meta.gid() != gid && unsafe { libc::fchown(fd, u32::MAX, gid) } != 0 {
                return Err(std::io::Error::other(format!(
                    "{}: chgrp to {gid}: {}",
                    path.display(),
                    std::io::Error::last_os_error()
                )));
            }
            if mode != desired && unsafe { libc::fchmod(fd, desired as libc::mode_t) } != 0 {
                return Err(std::io::Error::other(format!(
                    "{}: chmod to {desired:o}: {}",
                    path.display(),
                    std::io::Error::last_os_error()
                )));
            }
            let after = file.metadata()?;
            if after.gid() != gid || after.mode() & 0o7777 != desired {
                return Err(std::io::Error::other(format!(
                    "{}: permission readback differs",
                    path.display()
                )));
            }
        }
        if !meta.is_dir() {
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
            if !preserve_group_grants && kind == libc::S_IFDIR && name.to_bytes() == b".git" {
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
            visit(&child, &child_path, gid, preserve_group_grants, before_open)?;
        }
        Ok(())
    }
    visit(&file, path, gid, preserve_group_grants, before_open)
}

#[cfg(not(unix))]
fn share_tree(_path: &Path, _gid: u32) -> std::io::Result<()> {
    Ok(())
}

// Linux POSIX ACL xattrs are a versioned sequence of (tag, permissions, id).
// The descriptor-based API keeps every mutation bound to the opened object.
#[cfg(target_os = "linux")]
fn grant_metadata_acl(file: &std::fs::File, gid: u32) -> std::io::Result<()> {
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
    if !meta.is_dir() && meta.nlink() > 1 {
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
    let access = read(file, access_name)?.unwrap_or(base);
    let mut acls = vec![(access_name, access.clone())];
    if meta.is_dir() {
        acls.push((default_name, read(file, default_name)?.unwrap_or(access)));
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
        let desired = if meta.is_dir() || mode & 0o111 != 0 {
            7
        } else {
            6
        };
        if let Some(entry) = entries.iter_mut().find(|e| e.0 == 8 && e.2 == gid) {
            entry.1 |= desired;
        } else {
            entries.push((8, desired, gid));
        }
        // A new object may be owned by B. Retain the previous owner A as
        // a named user too, so default ACLs preserve A on B's descendants.
        let owner_permissions = entries.iter().find(|e| e.0 == 1).map(|e| e.1).unwrap_or(0);
        if let Some(entry) = entries.iter_mut().find(|e| e.0 == 2 && e.2 == meta.uid()) {
            entry.1 |= owner_permissions;
        } else {
            entries.push((2, owner_permissions, meta.uid()));
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
  for flag in --git-dir --git-common-dir; do
    d=$(git -c safe.directory="$1" -C "$1" rev-parse --path-format=absolute "$flag")
    # Git objects/packs are intentionally immutable; commits require writable
    # directories (objects/refs/logs/index lock), not rewriting old objects.
    find "$d" -type d -exec sh -c '
      for p do
        if [ ! -w "$p" ] || [ ! -x "$p" ]; then
          printf "%s: seat cannot write/traverse Git metadata\n" "$p" >&2; exit 1
        fi
      done
    ' sh {} +
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
    fn sharing_covers_worktree_gitdir_and_common_objects_and_refs() {
        let dir = tempfile::tempdir().unwrap();
        let f = crate::worktree::tests::Fixture::new(dir.path());
        let target = dir.path().join("child");
        f.consumer
            .remote("SylphxAI/demo", &target, "child", None)
            .unwrap();
        share(&target, crate::sandbox::server_gid()).unwrap();
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
        share(&tree, crate::sandbox::server_gid()).unwrap();
        assert_eq!(std::fs::metadata(&outside).unwrap().mode() & 0o777, 0o600);
        std::fs::hard_link(&outside, tree.join("hardlink")).unwrap();
        let err = share(&tree, crate::sandbox::server_gid()).unwrap_err();
        assert!(err.to_string().contains("hard-linked"), "{err}");
        assert_eq!(std::fs::metadata(&outside).unwrap().mode() & 0o777, 0o600);
    }
}
