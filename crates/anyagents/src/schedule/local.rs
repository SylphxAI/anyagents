//! Kernel-backed hints for local watch predicates.
//!
//! Construct before the initial predicate evaluation, inside a Tokio runtime with
//! I/O enabled. Hints are not predicate results: evaluate again after `changed`.
//! Unsupported registrations are explicit so the caller can retain its fallback
//! cadence. Supported registrations never use a timer or periodic filesystem probe.
//! Linux paths require a known-local filesystem and no symlink or `..` components;
//! unsupported paths, platforms, PIDs and unavailable reactors stay on fallback.

use std::collections::HashSet;
use std::future::poll_fn;
use std::task::{Context, Poll};

use super::WatchTarget;

/// Owns local kernel registrations. Dropping this value closes every descriptor.
pub(super) struct LocalEvents {
    registrations: Vec<(WatchTarget, Registration)>,
    fallback: HashSet<WatchTarget>,
    ready: HashSet<WatchTarget>,
}

impl LocalEvents {
    /// Register first, then evaluate the initial predicate to close the check/wait race.
    pub(super) fn new(targets: impl IntoIterator<Item = WatchTarget>) -> Self {
        let mut events = Self {
            registrations: Vec::new(),
            fallback: HashSet::new(),
            ready: HashSet::new(),
        };
        let mut seen = HashSet::new();
        for target in targets {
            if !matches!(
                target,
                WatchTarget::PathChanged { .. } | WatchTarget::ProcessExit { .. }
            ) || !seen.insert(target.clone())
            {
                continue;
            }
            match Registration::new(&target) {
                Ok(Some(registration)) => events.registrations.push((target, registration)),
                // A process that disappeared during registration needs evaluation now.
                Ok(None) => {
                    events.ready.insert(target);
                }
                Err(_) => {
                    events.fallback.insert(target);
                }
            }
        }
        events
    }

    pub(super) fn fallback(&self) -> &HashSet<WatchTarget> {
        &self.fallback
    }

    /// Cancellation-safe: registrations and consumed hints remain owned by self.
    /// With no supported registrations or queued hints, remains pending forever.
    pub(super) async fn changed(&mut self) -> HashSet<WatchTarget> {
        poll_fn(|cx| self.poll_changed(cx)).await
    }

    fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<HashSet<WatchTarget>> {
        let mut index = 0;
        while index < self.registrations.len() {
            let result = self.registrations[index].1.poll_changed(cx);
            match result {
                Poll::Pending => index += 1,
                Poll::Ready(Ok(continuing)) => {
                    self.ready.insert(self.registrations[index].0.clone());
                    if continuing {
                        index += 1;
                    } else {
                        self.registrations.swap_remove(index);
                    }
                }
                Poll::Ready(Err(_)) => {
                    let (target, _) = self.registrations.swap_remove(index);
                    self.ready.insert(target.clone());
                    self.fallback.insert(target);
                }
            }
        }
        if self.ready.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(std::mem::take(&mut self.ready))
        }
    }
}

#[cfg(target_os = "linux")]
use linux::Registration;

#[cfg(not(target_os = "linux"))]
struct Registration;

#[cfg(not(target_os = "linux"))]
impl Registration {
    fn new(_: &WatchTarget) -> std::io::Result<Option<Self>> {
        Err(std::io::ErrorKind::Unsupported.into())
    }

    fn poll_changed(&mut self, _: &mut Context<'_>) -> Poll<std::io::Result<bool>> {
        Poll::Pending
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::{HashMap, HashSet};
    use std::ffi::{CString, OsString};
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Component, Path, PathBuf};
    use std::task::{Context, Poll, ready};

    use tokio::io::Interest;
    use tokio::io::unix::AsyncFd;

    use super::WatchTarget;

    pub(super) enum Registration {
        Path(PathWatch),
        Process(AsyncFd<OwnedFd>),
    }

    impl Registration {
        pub(super) fn new(target: &WatchTarget) -> io::Result<Option<Self>> {
            if tokio::runtime::Handle::try_current().is_err() {
                return Err(io::ErrorKind::Unsupported.into());
            }
            match target {
                WatchTarget::PathChanged { path } => {
                    Ok(Some(Self::Path(PathWatch::new(Path::new(path))?)))
                }
                WatchTarget::ProcessExit { pid } => {
                    let pid = i32::try_from(*pid)
                        .ok()
                        .filter(|pid| *pid > 0)
                        .ok_or(io::ErrorKind::InvalidInput)?;
                    // SAFETY: pidfd_open takes only scalar arguments and returns a new fd.
                    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0u32) };
                    if fd < 0 {
                        let error = io::Error::last_os_error();
                        if error.raw_os_error() == Some(libc::ESRCH) {
                            return Ok(None);
                        }
                        return Err(error);
                    }
                    // SAFETY: a successful pidfd_open returns an exclusively owned fd.
                    let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
                    Ok(Some(Self::Process(async_fd(fd)?)))
                }
                _ => Err(io::ErrorKind::InvalidInput.into()),
            }
        }

        #[cfg(test)]
        pub(super) fn raw_fd(&self) -> i32 {
            match self {
                Self::Path(path) => path.fd.as_raw_fd(),
                Self::Process(fd) => fd.as_raw_fd(),
            }
        }

        // true: keep the rearmed registration; false: a one-shot process exited.
        pub(super) fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
            match self {
                Self::Path(path) => path.poll_changed(cx).map_ok(|()| true),
                Self::Process(fd) => {
                    let _guard = ready!(fd.poll_read_ready(cx))?;
                    Poll::Ready(Ok(false))
                }
            }
        }
    }

    #[derive(Default)]
    struct InterestNames {
        target: bool,
        children: HashSet<OsString>,
    }

    pub(super) struct PathWatch {
        path: PathBuf,
        fd: AsyncFd<OwnedFd>,
        interests: HashMap<i32, InterestNames>,
    }

    const MASK: u32 = libc::IN_MODIFY
        | libc::IN_ATTRIB
        | libc::IN_CLOSE_WRITE
        | libc::IN_CREATE
        | libc::IN_DELETE
        | libc::IN_MOVED_FROM
        | libc::IN_MOVED_TO
        | libc::IN_DELETE_SELF
        | libc::IN_MOVE_SELF
        | libc::IN_UNMOUNT;
    const SELF_EVENTS: u32 = libc::IN_DELETE_SELF
        | libc::IN_MOVE_SELF
        | libc::IN_UNMOUNT
        | libc::IN_IGNORED
        | libc::IN_ATTRIB;

    fn async_fd(fd: OwnedFd) -> io::Result<AsyncFd<OwnedFd>> {
        // Tokio has no non-panicking reactor-availability query. Catch only this
        // registration boundary so an entered runtime with I/O disabled falls
        // back too. Moving OwnedFd into the closure closes it on unwinding.
        std::panic::catch_unwind(|| {
            // SAFETY: OwnedFd keeps a stable, valid descriptor open until AsyncFd
            // deregisters it on drop. No code closes or replaces the raw fd.
            unsafe { AsyncFd::register_with_interest(fd, Interest::READABLE) }
        })
        .map_err(|_| io::Error::from(io::ErrorKind::Unsupported))?
        .map_err(Into::into)
    }

    // inotify can register successfully on filesystems that do not emit reliable
    // local changes (notably procfs and network/FUSE mounts). Fail closed to the
    // caller's existing probe cadence unless this is a known local filesystem.
    fn check_filesystem(path: &CString) -> io::Result<()> {
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: path is NUL-terminated and stat points to writable statfs storage.
        if unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful statfs initialized the entire result.
        let kind = unsafe { stat.assume_init() }.f_type as u64;
        match kind {
            // ext2/3/4, XFS, Btrfs, tmpfs, overlayfs, ramfs, F2FS.
            0xef53 | 0x58465342 | 0x9123683e | 0x01021994 | 0x794c7630 | 0x858458f6
            | 0xf2f52010 => Ok(()),
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("filesystem {kind:#x} does not guarantee local notifications"),
            )),
        }
    }

    impl PathWatch {
        fn new(path: &Path) -> io::Result<Self> {
            // Capture relative paths now; changing cwd later must not retarget a watch.
            if path.as_os_str().is_empty() || path.components().any(|p| p == Component::ParentDir) {
                return Err(io::ErrorKind::Unsupported.into());
            }
            let path = if path.is_absolute() {
                path.to_owned()
            } else {
                std::env::current_dir()?.join(path)
            };
            // SAFETY: inotify_init1 takes flags and returns a new descriptor.
            let raw = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
            if raw < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: this successful inotify_init1 descriptor is exclusively owned.
            let fd = unsafe { OwnedFd::from_raw_fd(raw) };
            let mut interests = HashMap::<i32, InterestNames>::new();
            let mut deepest = None;
            let prefixes: Vec<_> = path.ancestors().collect();
            // Watch root first, then descend. A rename/create during registration
            // queues a hint in an already-installed parent, before initial evaluation.
            for index in (0..prefixes.len()).rev() {
                let prefix = prefixes[index];
                let name = CString::new(prefix.as_os_str().as_bytes())
                    .map_err(|_| io::ErrorKind::InvalidInput)?;
                // Following a symlink would also require watching its referent's
                // entire ancestry. Keep those predicates on the explicit fallback
                // path, including existing links and links created during rearm.
                if std::fs::symlink_metadata(prefix).is_ok_and(|m| m.file_type().is_symlink()) {
                    return Err(io::ErrorKind::Unsupported.into());
                }
                // SAFETY: name is NUL-terminated and fd is an owned inotify instance.
                // IN_MASK_ADD preserves masks when different paths alias one inode.
                let wd = unsafe {
                    libc::inotify_add_watch(fd.as_raw_fd(), name.as_ptr(), MASK | libc::IN_MASK_ADD)
                };
                if wd < 0 {
                    let error = io::Error::last_os_error();
                    match error.raw_os_error() {
                        Some(libc::ENOENT | libc::ENOTDIR) => {
                            // A dangling link may point outside the watched ancestry.
                            // Do not claim creation coverage that inotify cannot give.
                            if std::fs::symlink_metadata(prefix)
                                .is_ok_and(|m| m.file_type().is_symlink())
                            {
                                return Err(io::ErrorKind::Unsupported.into());
                            }
                            break;
                        }
                        _ => return Err(error),
                    }
                }
                deepest = Some(name);
                let interest = interests.entry(wd).or_default();
                if index == 0 {
                    interest.target = true;
                } else {
                    let child = prefixes[index - 1]
                        .file_name()
                        .ok_or(io::ErrorKind::Unsupported)?;
                    interest.children.insert(child.to_owned());
                }
            }
            let deepest = deepest.ok_or(io::ErrorKind::Unsupported)?;
            check_filesystem(&deepest)?;
            Ok(Self {
                path,
                fd: async_fd(fd)?,
                interests,
            })
        }

        fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut guard = ready!(self.fd.poll_read_ready(cx))?;
            let mut buffer = [0u8; 16 * 1024];
            let result = guard.try_io(|fd| {
                // SAFETY: buffer is writable for its full length; fd is alive and
                // nonblocking. No pointers are retained by read.
                let length =
                    unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
                if length < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(length as usize)
                }
            });
            let length = match result {
                Err(_) => return Poll::Pending, // WouldBlock clears Tokio readiness.
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Ok(result) => result?,
            };
            drop(guard);
            if length == 0 {
                return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
            }
            if self.relevant(&buffer[..length])? {
                // Arm the new fd before dropping the old one and before evaluation.
                // This also repairs watches invalidated by rename/delete/replacement.
                *self = Self::new(&self.path)?;
                Poll::Ready(Ok(()))
            } else {
                // Bound each poll to one buffer, even under unrelated-event floods.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }

        fn relevant(&self, mut bytes: &[u8]) -> io::Result<bool> {
            let header = std::mem::size_of::<libc::inotify_event>();
            let mut relevant = false;
            while !bytes.is_empty() {
                if bytes.len() < header {
                    return Err(io::ErrorKind::InvalidData.into());
                }
                // SAFETY: header bytes are present. read_unaligned handles a byte
                // buffer's alignment; inotify_event contains only integer fields.
                let event = unsafe {
                    std::ptr::read_unaligned(bytes.as_ptr().cast::<libc::inotify_event>())
                };
                let end = header
                    .checked_add(event.len as usize)
                    .filter(|end| *end <= bytes.len())
                    .ok_or(io::ErrorKind::InvalidData)?;
                let name = &bytes[header..end];
                let name = &name[..name.iter().position(|b| *b == 0).unwrap_or(name.len())];
                relevant |= event.mask & libc::IN_Q_OVERFLOW != 0
                    || self.interests.get(&event.wd).is_some_and(|interest| {
                        interest.target
                            || event.mask & SELF_EVENTS != 0
                            || interest
                                .children
                                .contains(std::ffi::OsStr::from_bytes(name))
                    });
                bytes = &bytes[end..];
            }
            Ok(relevant)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::Path;
    use std::pin::pin;
    use std::task::{Context, Waker};

    use super::*;

    fn target(path: &Path) -> WatchTarget {
        WatchTarget::PathChanged {
            path: path.to_string_lossy().into_owned(),
        }
    }

    fn assert_pending(events: &mut LocalEvents) {
        let mut changed = pin!(events.changed());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(changed.as_mut().poll(&mut cx).is_pending());
    }

    #[test]
    fn no_runtime_and_invalid_targets_fall_back() {
        let local = WatchTarget::ProcessExit { pid: 0 };
        let remote = WatchTarget::AgentIdle {
            target: "other".into(),
        };
        let mut events = LocalEvents::new([local.clone(), local.clone(), remote]);
        assert_eq!(events.fallback(), &HashSet::from([local]));
        assert_pending(&mut events);
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn non_linux_explicitly_falls_back() {
        let target = target(Path::new("/"));
        let mut events = LocalEvents::new([target.clone()]);
        assert_eq!(events.fallback(), &HashSet::from([target]));
        assert_pending(&mut events);
    }

    #[cfg(target_os = "linux")]
    mod linux {
        use std::io::Write;
        use std::process::{Child, Command, Stdio};
        use std::time::Duration;

        use super::*;

        async fn changed(events: &mut LocalEvents, target: &WatchTarget) {
            let ready = tokio::time::timeout(Duration::from_secs(5), events.changed())
                .await
                .expect("kernel event not delivered");
            assert_eq!(ready, HashSet::from([target.clone()]));
            assert!(events.fallback().is_empty(), "unexpected fallback");
        }

        fn registered(target: WatchTarget) -> LocalEvents {
            let events = LocalEvents::new([target.clone(), target.clone()]);
            assert!(
                events.fallback().is_empty(),
                "test requires local inotify/pidfd support: {:?}",
                Registration::new(&target).err()
            );
            assert_eq!(
                events.registrations.len(),
                1,
                "duplicate targets must share a registration"
            );
            events
        }

        #[test]
        fn no_io_runtime_falls_back_without_unwinding() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                let target = WatchTarget::ProcessExit {
                    pid: std::process::id(),
                };
                let mut events = LocalEvents::new([target.clone()]);
                assert_eq!(events.fallback(), &HashSet::from([target]));
                assert_pending(&mut events);
            });
        }

        #[tokio::test]
        async fn file_modify_atomic_replace_delete_and_recreate() {
            let dir = tempfile::tempdir_in("/dev/shm")
                .expect("Linux kernel-watch tests require local tmpfs /dev/shm");
            let path = dir.path().join("watched");
            std::fs::write(&path, "initial").unwrap();
            let target = target(&path);
            let mut events = registered(target.clone());
            // Mutation before the first wait must remain queued.
            std::fs::write(&path, "modified").unwrap();
            changed(&mut events, &target).await;
            let replacement = dir.path().join("replacement");
            std::fs::write(&replacement, "replacement").unwrap();
            std::fs::rename(replacement, &path).unwrap();
            changed(&mut events, &target).await;
            std::fs::write(&path, "new inode modified").unwrap();
            changed(&mut events, &target).await;
            std::fs::remove_file(&path).unwrap();
            changed(&mut events, &target).await;
            std::fs::write(&path, "recreated").unwrap();
            changed(&mut events, &target).await;
            std::fs::write(&path, "still watched").unwrap();
            changed(&mut events, &target).await;
        }

        #[tokio::test]
        async fn missing_ancestors_and_parent_replacement_rearm() {
            let dir = tempfile::tempdir_in("/dev/shm")
                .expect("Linux kernel-watch tests require local tmpfs /dev/shm");
            let parent = dir.path().join("parent");
            let nested = parent.join("nested");
            let path = nested.join("file");
            let target = target(&path);
            let mut events = registered(target.clone());
            std::fs::create_dir(&parent).unwrap();
            changed(&mut events, &target).await;
            std::fs::create_dir(&nested).unwrap();
            changed(&mut events, &target).await;
            std::fs::write(&path, "created").unwrap();
            changed(&mut events, &target).await;
            std::fs::rename(&parent, dir.path().join("old-parent")).unwrap();
            changed(&mut events, &target).await;
            std::fs::create_dir_all(&nested).unwrap();
            std::fs::write(&path, "new parent").unwrap();
            changed(&mut events, &target).await;
            std::fs::write(&path, "new file modified").unwrap();
            changed(&mut events, &target).await;
        }

        #[tokio::test]
        async fn unrelated_siblings_do_not_wake_and_cancelled_wait_is_safe() {
            let dir = tempfile::tempdir_in("/dev/shm")
                .expect("Linux kernel-watch tests require local tmpfs /dev/shm");
            let path = dir.path().join("watched");
            std::fs::write(&path, "initial").unwrap();
            let target = target(&path);
            let mut events = registered(target.clone());
            // Poll and cancel before the event: no registration is lost.
            assert_pending(&mut events);
            std::fs::write(dir.path().join("unrelated"), "sibling").unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(30), events.changed())
                    .await
                    .is_err()
            );
            std::fs::write(path, "modified after cancellation").unwrap();
            changed(&mut events, &target).await;
        }

        #[tokio::test]
        async fn multiple_targets_only_return_affected_hints() {
            let dir = tempfile::tempdir_in("/dev/shm")
                .expect("Linux kernel-watch tests require local tmpfs /dev/shm");
            let first = dir.path().join("first");
            let second = dir.path().join("second");
            std::fs::write(&first, "initial").unwrap();
            std::fs::write(&second, "initial").unwrap();
            let first_target = target(&first);
            let second_target = target(&second);
            let mut events = LocalEvents::new([first_target.clone(), second_target.clone()]);
            assert!(events.fallback().is_empty());
            assert_eq!(events.registrations.len(), 2);
            std::fs::write(&first, "changed first").unwrap();
            changed(&mut events, &first_target).await;
            std::fs::write(&second, "changed second").unwrap();
            changed(&mut events, &second_target).await;
        }

        struct ChildGuard(Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        fn child() -> ChildGuard {
            ChildGuard(
                Command::new("sh")
                    .args(["-c", "read line"])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            )
        }

        #[tokio::test]
        async fn real_child_exit_is_one_shot_and_already_gone_is_ready() {
            let mut child = child();
            let target = WatchTarget::ProcessExit { pid: child.0.id() };
            let mut events = registered(target.clone());
            assert_pending(&mut events);
            child.0.stdin.take().unwrap().write_all(b"exit\n").unwrap();
            changed(&mut events, &target).await;
            assert!(events.registrations.is_empty());
            assert_pending(&mut events);
            child.0.wait().unwrap();
            let mut gone = LocalEvents::new([target.clone()]);
            assert!(gone.registrations.is_empty());
            changed(&mut gone, &target).await;
            assert_pending(&mut gone);
        }

        #[tokio::test]
        async fn unsupported_paths_and_pids_are_explicit_fallback() {
            let dir = tempfile::tempdir_in("/dev/shm")
                .expect("Linux kernel-watch tests require local tmpfs /dev/shm");
            let link = dir.path().join("dangling");
            std::os::unix::fs::symlink(dir.path().join("absent"), &link).unwrap();
            let referent = dir.path().join("referent");
            std::fs::write(&referent, "existing").unwrap();
            let existing_link = dir.path().join("existing-link");
            std::os::unix::fs::symlink(&referent, &existing_link).unwrap();
            let targets = [
                target(&link),
                target(&existing_link),
                target(Path::new("/proc/self/stat")),
                target(Path::new("/tmp/../unsupported")),
                WatchTarget::PathChanged {
                    path: "bad\0path".into(),
                },
                WatchTarget::ProcessExit { pid: 0 },
                WatchTarget::ProcessExit { pid: u32::MAX },
            ];
            let mut events = LocalEvents::new(targets.clone());
            assert_eq!(events.fallback(), &HashSet::from(targets));
            assert_pending(&mut events);
        }

        #[tokio::test]
        async fn rearm_failure_returns_target_and_moves_it_to_fallback() {
            let dir = tempfile::tempdir_in("/dev/shm")
                .expect("Linux kernel-watch tests require local tmpfs /dev/shm");
            let path = dir.path().join("watched");
            std::fs::write(&path, "initial").unwrap();
            let target = target(&path);
            let mut events = registered(target.clone());
            std::fs::remove_file(&path).unwrap();
            std::os::unix::fs::symlink(dir.path().join("missing"), &path).unwrap();
            let ready = tokio::time::timeout(Duration::from_secs(5), events.changed())
                .await
                .unwrap();
            assert_eq!(ready, HashSet::from([target.clone()]));
            assert_eq!(events.fallback(), &HashSet::from([target]));
            assert!(events.registrations.is_empty());
            assert_pending(&mut events);
        }

        #[tokio::test]
        async fn descriptors_close_on_drop() {
            // Run the close checks in a separate one-test process, so no other test
            // can allocate the same fd between drop and F_GETFD.
            const MARKER: &str = "ANYAGENTS_LOCAL_FD_TEST";
            if std::env::var_os(MARKER).is_none() {
                let status = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "schedule::local::tests::linux::descriptors_close_on_drop",
                        "--test-threads=1",
                    ])
                    .env(MARKER, "1")
                    .status()
                    .unwrap();
                assert!(status.success());
                return;
            }
            let dir = tempfile::tempdir_in("/dev/shm")
                .expect("Linux kernel-watch tests require local tmpfs /dev/shm");
            let mut child = child();
            let path = dir.path().join("watched");
            let path_target = target(&path);
            std::fs::write(&path, "initial").unwrap();
            let mut events = LocalEvents::new([
                path_target.clone(),
                WatchTarget::ProcessExit { pid: child.0.id() },
            ]);
            assert!(events.fallback().is_empty());
            assert_eq!(events.registrations.len(), 2);
            let descriptor_count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
            let baseline = descriptor_count();
            for iteration in 0..32 {
                let replacement = dir.path().join("replacement");
                std::fs::write(&replacement, iteration.to_string()).unwrap();
                std::fs::rename(replacement, &path).unwrap();
                changed(&mut events, &path_target).await;
                assert_eq!(descriptor_count(), baseline, "rearm must not leak fds");
            }
            let fds: Vec<_> = events
                .registrations
                .iter()
                .map(|(_, r)| r.raw_fd())
                .collect();
            drop(events);
            for fd in fds {
                // SAFETY: F_GETFD only inspects this integer; it never dereferences memory.
                assert_eq!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EBADF)
                );
            }
            child.0.stdin.take();
        }
    }
}
