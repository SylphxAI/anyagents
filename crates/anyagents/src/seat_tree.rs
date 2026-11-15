//! Descriptor-bound sharing of mutable config and host overlays. Names select
//! candidates only; metadata, recursion and effects use the opened inode.
use std::{
    ffi::{CString, OsStr, OsString},
    fs::File,
    io,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd},
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Component, Path, PathBuf},
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

/// A private copy remains staged until its mailbox row is committed. Failed
/// delivery (or a racing duplicate) removes it through the held directory.
pub(crate) struct InboundMedia {
    path: PathBuf,
    directory: File,
    name: CString,
    retained: bool,
}

impl InboundMedia {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn retain(mut self) {
        self.retained = true;
    }
}

impl Drop for InboundMedia {
    fn drop(&mut self) {
        if !self.retained {
            unsafe {
                libc::unlinkat(self.directory.as_raw_fd(), self.name.as_ptr(), 0);
            }
        }
    }
}

/// Daemon-owned inbound copies. Neither the recipient nor its supplementary
/// groups can replace a directory or file; only the admitted uid can read it.
pub(crate) fn persist_inbound_media(
    data: &Path,
    recipient: &anyagents_core::AgentId,
    uid: Option<u32>,
    name: &str,
    bytes: &[u8],
) -> io::Result<InboundMedia> {
    use std::io::Write;
    use std::os::fd::AsFd;
    let data_fd = open_root(data)?;
    let root = private_media_dir(&data_fd, OsStr::new("inbound-media"), None, true)?;
    let dir = private_media_dir(&root, OsStr::new(recipient.as_str()), uid, false)?;
    // Bound the generated component even for a NAME_MAX source basename.
    let extension = Path::new(name)
        .extension()
        .and_then(OsStr::to_str)
        .filter(|s| s.len() <= 16 && s.bytes().all(|b| b.is_ascii_alphanumeric()));
    let name = match extension {
        Some(ext) => format!("{}.{ext}", anyagents_core::MessageId::generate()),
        None => anyagents_core::MessageId::generate().to_string(),
    };
    // Only the basename from MediaUpload is used; defense in depth for callers.
    if name.contains('/') || name.contains('\\') || name.contains('\0') {
        return Err(io::Error::other("invalid inbound media filename"));
    }
    let c_name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            c_name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut file = unsafe { File::from_raw_fd(fd) };
    let result = (|| {
        clear_private_acl(file.as_fd())?;
        file.write_all(bytes)?;
        file.sync_all()?;
        inbound_grant(&file, uid, false)?;
        file.sync_all()?;
        dir.sync_all()
    })();
    if let Err(error) = result {
        unsafe {
            libc::unlinkat(dir.as_raw_fd(), c_name.as_ptr(), 0);
        }
        return Err(error);
    }
    Ok(InboundMedia {
        path: data
            .join("inbound-media")
            .join(recipient.as_str())
            .join(name),
        directory: dir,
        name: c_name,
        retained: false,
    })
}

fn private_media_dir(
    parent: &File,
    name: &OsStr,
    uid: Option<u32>,
    traverse: bool,
) -> io::Result<File> {
    use std::os::fd::AsFd;
    let c_name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
    if unsafe { libc::mkdirat(parent.as_raw_fd(), c_name.as_ptr(), 0o700) } != 0
        && io::Error::last_os_error().kind() != io::ErrorKind::AlreadyExists
    {
        return Err(io::Error::last_os_error());
    }
    let dir = open_child(parent, name, true)?;
    if dir.metadata()?.uid() != super::sandbox::server_uid() {
        return Err(io::Error::other(
            "inbound media directory is not daemon-owned",
        ));
    }
    clear_private_acl(dir.as_fd())?;
    if unsafe { libc::fchmod(dir.as_raw_fd(), if traverse { 0o711 } else { 0o700 }) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if !traverse {
        inbound_grant(&dir, uid, true)?;
    }
    Ok(dir)
}

fn inbound_grant(file: &File, uid: Option<u32>, directory: bool) -> io::Result<()> {
    let Some(uid) = uid else {
        return Ok(());
    };
    #[cfg(target_os = "linux")]
    {
        let mut acl = 2u32.to_le_bytes().to_vec();
        for (tag, perm, id) in [
            (1u16, if directory { 7u16 } else { 6 }, u32::MAX),
            (2, if directory { 5 } else { 4 }, uid),
            (4, 0, u32::MAX), // never grant the daemon's or socket's group
            (16, if directory { 5 } else { 4 }, u32::MAX),
            (32, 0, u32::MAX),
        ] {
            acl.extend_from_slice(&tag.to_le_bytes());
            acl.extend_from_slice(&perm.to_le_bytes());
            acl.extend_from_slice(&id.to_le_bytes());
        }
        if unsafe {
            libc::fsetxattr(
                file.as_raw_fd(),
                c"system.posix_acl_access".as_ptr(),
                acl.as_ptr().cast(),
                acl.len(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut actual = vec![0u8; acl.len()];
        let length = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                c"system.posix_acl_access".as_ptr(),
                actual.as_mut_ptr().cast(),
                actual.len(),
            )
        };
        if length != acl.len() as isize || actual != acl {
            return Err(io::Error::other("inbound media ACL read-back mismatch"));
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, uid, directory);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "private seat media grants require Linux ACLs",
        ))
    }
}

fn names(directory: &File) -> io::Result<Vec<std::ffi::OsString>> {
    // fdopendir owns a duplicate; neither enumeration nor recursion reopens a
    // pathname that the seat can replace while the grant is in progress.
    // Close-on-exec, so a process started meanwhile never inherits it.
    let fd = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
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

// These commands read stdin from an opened inode (never a pipe) and discard
// stdout. Drain stderr while the child runs, retaining only a bounded prefix;
// waiting first or stopping at the cap could deadlock a noisy launcher.
const SEAT_STDERR_LIMIT: u64 = 4096;

fn run_seat_command(command: &mut std::process::Command, stage: &str) -> io::Result<()> {
    use std::io::Read;
    let mut child = command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| io::Error::new(error.kind(), format!("{stage} spawn failed: {error}")))?;
    let mut stderr = child.stderr.take().expect("piped seat stderr");
    let mut prefix = Vec::new();
    let read_result = (&mut stderr)
        .take(SEAT_STDERR_LIMIT + 1)
        .read_to_end(&mut prefix)
        .and_then(|_| io::copy(&mut stderr, &mut io::sink()).map(|_| ()));
    // Close even on a read error before waiting, so the child cannot block on
    // an undrained pipe. Always reap the child, including failed diagnostics.
    drop(stderr);
    let status = child
        .wait()
        .map_err(|error| io::Error::new(error.kind(), format!("{stage} wait failed: {error}")))?;
    read_result.map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("{stage} stderr read failed ({status}): {error}"),
        )
    })?;
    if status.success() {
        return Ok(());
    }
    let truncated = prefix.len() > SEAT_STDERR_LIMIT as usize;
    prefix.truncate(SEAT_STDERR_LIMIT as usize);
    // Debug escaping keeps control characters/newlines out of host log records.
    let stderr = String::from_utf8_lossy(&prefix);
    Err(io::Error::other(format!(
        "{stage} failed ({status}); stderr={stderr:?}{}",
        if truncated { " [truncated]" } else { "" }
    )))
}

/// The only `PATH` a seat command sees. The helper resolves nothing on it; it
/// is here for a launcher that wants one.
const SEAT_PATH: &str = "/usr/bin:/bin";

/// This binary, asked to run its `internal seat-file` helper.
#[cfg(not(test))]
fn seat_file_program() -> io::Result<(PathBuf, Vec<&'static str>)> {
    Ok((
        crate::data::current_exe_clean()?,
        vec!["internal", "seat-file"],
    ))
}

/// Under `cargo test` this binary is the test harness: its worker test takes
/// the helper's arguments instead.
#[cfg(test)]
fn seat_file_program() -> io::Result<(PathBuf, Vec<&'static str>)> {
    Ok((
        crate::repo::seat_executable_test_harness()?,
        vec![
            "--exact",
            "seat_tree::tests::seat_file_worker",
            "--nocapture",
            "--",
            "seat-file",
        ],
    ))
}

/// A command that runs this binary's `internal seat-file` helper as `uid`,
/// `gid` and `groups` through `launcher`; the caller appends the operation.
/// The seat runs no host tool, so a wrapper earlier on the daemon's `PATH`
/// (the desk's `cat` once was one the seat could not run) cannot break it,
/// and it sees a fixed environment, never the daemon's.
fn seat_file_command(
    launcher: &Path,
    uid: u32,
    gid: u32,
    groups: &str,
) -> io::Result<std::process::Command> {
    let (program, helper) = seat_file_program()?;
    let mut command = std::process::Command::new(launcher);
    command
        .args([
            "--reuid",
            &uid.to_string(),
            "--regid",
            &gid.to_string(),
            "--groups",
            groups,
            "--no-new-privs",
            "--",
        ])
        .arg(program)
        .args(helper)
        .env_clear()
        .env("PATH", SEAT_PATH);
    Ok(command)
}

/// `anyagents internal seat-file <operation>`: one file change the daemon
/// needs made as a seat, run as that seat by [`seat_file_command`]. Each
/// operation acts on standard input (an opened inode, never a pipe) or on one
/// name in the working directory (an opened parent, entered with `fchdir`
/// before the launcher runs). Exits 1 with the reason on stderr, 2 on a
/// malformed call.
///
/// - `inode <mode> [<gid>]`: give standard input's inode group `gid`, then
///   octal `mode`.
/// - `op-dir`: strip access/default ACLs, mode 0700 on standard input's
///   directory and 0600 on each single-link regular file; reject other entries.
/// - `config-acl <seat> <daemon> <mode> <inherit>`: install exact principal
///   ACLs on the input inode, with a default ACL when `inherit` is `1`.
/// - `copy <name>`: create `./<name>` (0660, never over an existing name),
///   copy standard input into it and fsync it.
/// - `readback <name>`: succeed only when `./<name>` holds exactly the bytes
///   left on standard input.
/// - `unlink <name>`: remove `./<name>` if it is there.
/// - `open <path>`: open absolute `<path>` read-only as the seat, never
///   through a final symbolic link, and hand the descriptor back over
///   standard input (a Unix socket) when it is a regular file.
/// - `create <path>`: the same for writing, creating or truncating it.
/// - `kill <pid>`: SIGKILL one of the seat's own processes (a helper the
///   daemon gave up on and may not signal itself).
pub(crate) fn seat_file_main(args: &[OsString]) -> std::process::ExitCode {
    let usage = || {
        eprintln!(
            "usage: anyagents internal seat-file inode <mode> [<gid>] | op-dir | \
             copy <name> | readback <name> | unlink <name> | open <path> | create <path> | \
             kill <pid> | config-acl <seat> <daemon> <mode> <inherit>"
        );
        std::process::ExitCode::from(2)
    };
    // Standard input stays open for this process's life; borrowing never
    // closes it.
    let input = unsafe { BorrowedFd::borrow_raw(libc::STDIN_FILENO) };
    let operation = args.first().and_then(|arg| arg.to_str());
    let result = match (operation, &args[args.len().min(1)..]) {
        (Some("inode"), [mode]) => match parse_mode(mode) {
            Some(mode) => set_inode(input, None, mode),
            None => return usage(),
        },
        (Some("inode"), [mode, gid]) => {
            match (parse_mode(mode), gid.to_str().and_then(|g| g.parse().ok())) {
                (Some(mode), Some(gid)) => set_inode(input, Some(gid), mode),
                _ => return usage(),
            }
        }
        (Some("config-acl"), [uid, daemon, mode, inherit]) => {
            match (
                uid.to_str().and_then(|v| v.parse().ok()),
                daemon.to_str().and_then(|v| v.parse().ok()),
                parse_mode(mode),
                inherit.to_str(),
            ) {
                (Some(uid), Some(daemon), Some(mode), Some(inherit @ ("0" | "1"))) =>
                {
                    #[allow(clippy::unnecessary_cast, reason = "mode_t differs across platforms")]
                    config_acl(input, uid, daemon, mode as u32, inherit == "1")
                }
                _ => return usage(),
            }
        }
        (Some("op-dir"), []) => tighten_op_dir(input),
        (Some(operation @ ("copy" | "readback" | "unlink")), [name]) => {
            let Some(name) = single_name(name) else {
                return usage();
            };
            match operation {
                "copy" => copy_into(input, name),
                "readback" => read_back(input, name),
                _ => match std::fs::remove_file(Path::new(".").join(name)) {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                    other => other,
                },
            }
        }
        (Some("kill"), [pid]) => match pid.to_str().and_then(|p| p.parse::<libc::pid_t>().ok()) {
            // Only this seat's own process can be signalled from here.
            Some(pid) if pid > 1 => {
                if unsafe { libc::kill(pid, libc::SIGKILL) } == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            }
            _ => return usage(),
        },
        (Some(operation @ ("open" | "create")), [path]) if Path::new(path).is_absolute() => {
            open_regular_file(Path::new(path), operation == "create")
                .and_then(|file| hand_back(input, &file))
        }
        _ => return usage(),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("seat-file {}: {error}", operation.unwrap_or_default());
            std::process::ExitCode::FAILURE
        }
    }
}

fn parse_mode(mode: &OsStr) -> Option<libc::mode_t> {
    libc::mode_t::from_str_radix(mode.to_str()?, 8)
        .ok()
        .filter(|mode| *mode <= 0o7777)
}

/// `name` when it names an entry of the working directory itself.
fn single_name(name: &OsStr) -> Option<&OsStr> {
    let bytes = name.as_bytes();
    (!bytes.is_empty() && !bytes.contains(&b'/') && bytes != b"." && bytes != b"..").then_some(name)
}

/// Group `gid` (when given), then `mode`, on the inode `fd` refers to, which
/// may be an `O_PATH` descriptor.
fn set_inode(fd: BorrowedFd<'_>, gid: Option<libc::gid_t>, mode: libc::mode_t) -> io::Result<()> {
    let raw = fd.as_raw_fd();
    if let Some(gid) = gid {
        // AT_EMPTY_PATH reaches an O_PATH descriptor too, which fchown refuses.
        #[cfg(target_os = "linux")]
        let changed =
            unsafe { libc::fchownat(raw, c"".as_ptr(), u32::MAX, gid, libc::AT_EMPTY_PATH) };
        #[cfg(not(target_os = "linux"))]
        let changed = unsafe { libc::fchown(raw, u32::MAX, gid) };
        if changed != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    if unsafe { libc::fchmod(raw, mode) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() != Some(libc::EBADF) {
        return Err(error);
    }
    // An O_PATH descriptor refuses fchmod; change the inode it names, as
    // `chmod /dev/fd/0` did.
    let fds = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    let path = CString::new(format!("{fds}/{raw}")).map_err(io::Error::other)?;
    if unsafe { libc::chmod(path.as_ptr(), mode) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// chmod alone leaves a default ACL behind: future private files would inherit
/// named readers. Remove both ACLs through the pinned descriptor first.
#[cfg(target_os = "linux")]
fn clear_private_acl(fd: BorrowedFd<'_>) -> io::Result<()> {
    let path =
        CString::new(format!("/proc/self/fd/{}", fd.as_raw_fd())).map_err(io::Error::other)?;
    for name in [c"system.posix_acl_default", c"system.posix_acl_access"] {
        if unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) } != 0 {
            let error = io::Error::last_os_error();
            if !matches!(error.raw_os_error(), Some(libc::ENODATA | libc::ENOTSUP)) {
                return Err(error);
            }
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn clear_private_acl(_fd: BorrowedFd<'_>) -> io::Result<()> {
    Ok(())
}

fn tighten_op_dir(dir: BorrowedFd<'_>) -> io::Result<()> {
    use std::os::fd::AsFd as _;
    clear_private_acl(dir)?;
    set_inode(dir, None, 0o700)?;
    // Owner helpers receive O_PATH descriptors for initially inaccessible dirs.
    #[cfg(target_os = "linux")]
    let directory = File::open(format!("/proc/self/fd/{}", dir.as_raw_fd()))?;
    #[cfg(not(target_os = "linux"))]
    let directory = File::from(dir.try_clone_to_owned()?);
    for name in names(&directory)? {
        let file = open_child(&directory, &name, false)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(io::Error::other(
                "private config requires single-link regular files",
            ));
        }
        clear_private_acl(file.as_fd())?;
        set_inode(file.as_fd(), None, 0o600)?;
    }
    Ok(())
}

fn copy_into(source: BorrowedFd<'_>, name: &OsStr) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt as _;
    unsafe {
        libc::umask(0o007);
    }
    let mut copy = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o660)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(Path::new(".").join(name))?;
    io::copy(&mut File::from(source.try_clone_to_owned()?), &mut copy)?;
    copy.sync_all()
}

fn read_back(source: BorrowedFd<'_>, name: &OsStr) -> io::Result<()> {
    use std::io::Read as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    fn fill(reader: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
        let mut filled = 0;
        while filled < buffer.len() {
            match reader.read(&mut buffer[filled..]) {
                Ok(0) => break,
                Ok(read) => filled += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => (),
                Err(error) => return Err(error),
            }
        }
        Ok(filled)
    }
    let mut copy = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(Path::new(".").join(name))?;
    let mut source = File::from(source.try_clone_to_owned()?);
    let (mut expected, mut actual) = (vec![0; 64 * 1024], vec![0; 64 * 1024]);
    loop {
        let want = fill(&mut source, &mut expected)?;
        let have = fill(&mut copy, &mut actual)?;
        if want != have || expected[..want] != actual[..have] {
            return Err(io::Error::other("copy differs from its source"));
        }
        if want == 0 {
            return Ok(());
        }
    }
}

/// `path` opened by this process's own identity: read-only, or for writing
/// (created 0644 or truncated) when `create`. A final symbolic link, a FIFO,
/// a device or a directory is refused; the kernel checks every directory
/// above it against this identity.
pub(crate) fn open_regular_file(path: &Path, create: bool) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut options = std::fs::OpenOptions::new();
    if create {
        options.write(true).create(true).truncate(true).mode(0o644);
    } else {
        options.read(true);
    }
    // O_NONBLOCK: opening a FIFO never waits for a peer.
    let file = options
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_NOCTTY | libc::O_CLOEXEC)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("not a regular file"));
    }
    Ok(file)
}

/// Send `file`'s descriptor to the daemon on `socket` (standard input).
fn hand_back(socket: BorrowedFd<'_>, file: &File) -> io::Result<()> {
    // Borrowed: standard input stays this process's to close.
    let stream = std::mem::ManuallyDrop::new(unsafe {
        std::os::unix::net::UnixStream::from_raw_fd(socket.as_raw_fd())
    });
    anyagents_keeper::wire::send_frame(&stream, &"opened", &[file.as_raw_fd()])
}

/// The descriptor came from a process running as the seat; take nothing
/// but a regular file from it.
fn regular_file(file: File, stage: &str) -> io::Result<File> {
    if !file.metadata()?.is_file() {
        return Err(io::Error::other(format!("{stage}: not a regular file")));
    }
    Ok(file)
}

/// `child`'s exit status if it exits within `grace`, polling, never
/// blocking past it.
fn exit_within(
    child: &mut std::process::Child,
    grace: std::time::Duration,
) -> io::Result<Option<std::process::ExitStatus>> {
    let deadline = std::time::Instant::now() + grace;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if std::time::Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// At most `limit` bytes of `pipe`, read until its writers close it or
/// `deadline` passes, polling a non-blocking descriptor.
fn read_until(
    pipe: std::process::ChildStderr,
    limit: usize,
    deadline: std::time::Duration,
) -> Vec<u8> {
    let fd = pipe.as_raw_fd();
    // SAFETY: fcntl on a descriptor `pipe` owns.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
    let end = std::time::Instant::now() + deadline;
    let mut out = Vec::new();
    let mut chunk = [0u8; 1024];
    while out.len() < limit {
        let left = end.saturating_duration_since(std::time::Instant::now());
        let mut poll = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = left.as_millis().min(i32::MAX as u128) as libc::c_int;
        // SAFETY: one pollfd for the duration of the call.
        let ready = unsafe { libc::poll(&mut poll, 1, millis) };
        if ready < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        if ready == 0 {
            break;
        }
        // SAFETY: a buffer of the length passed.
        let read = unsafe { libc::read(fd, chunk.as_mut_ptr().cast(), chunk.len()) };
        if read <= 0 {
            if read < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        let take = (read as usize).min(limit - out.len());
        out.extend_from_slice(&chunk[..take]);
    }
    out
}

/// Seat helpers one uid may leave with the reaper before its next open is
/// refused.
const SEAT_REAPER_PER_UID: usize = 8;

fn reaped_for(uid: u32) -> usize {
    SEAT_REAPER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|(owner, _)| *owner == uid)
        .count()
}

/// Kill a seat helper that did not exit, and hand it to the reaper. The
/// daemon may not signal another uid's process, so a kill that is refused
/// is asked of the seat itself (`seat-file kill <pid>`); that request is
/// reaped the same way. The child stays unreaped until then, so its pid
/// cannot be reused under the kill.
fn stop_seat_child(
    mut child: std::process::Child,
    launcher: &Path,
    uid: u32,
    gid: u32,
    groups: &str,
) {
    if child.kill().is_err()
        && let Ok(mut kill) = seat_file_command(launcher, uid, gid, groups)
    {
        kill.arg("kill")
            .arg(child.id().to_string())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if let Ok(kill) = kill.spawn() {
            reap_later(uid, kill);
        }
    }
    reap_later(uid, child);
}

/// Children a request thread will not wait for, reaped by one background
/// thread as they exit.
static SEAT_REAPER: std::sync::Mutex<Vec<(u32, std::process::Child)>> =
    std::sync::Mutex::new(Vec::new());

fn reap_later(uid: u32, child: std::process::Child) {
    static STARTED: std::sync::Once = std::sync::Once::new();
    SEAT_REAPER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((uid, child));
    STARTED.call_once(|| {
        let _ = std::thread::Builder::new()
            .name("seat-reaper".into())
            .spawn(|| {
                loop {
                    SEAT_REAPER
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .retain_mut(|(_, child)| !matches!(child.try_wait(), Ok(Some(_)) | Err(_)));
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
            });
    });
}

/// How long the daemon waits for a seat to open one file.
const SEAT_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Open absolute `path` as seat `uid` (`gid`, `groups`) through `launcher`
/// and return the regular file the seat itself opened: read-only, or for
/// writing when `create`. The daemon never resolves the name; what the seat
/// cannot open, this cannot return (ADR 0010 D3).
pub(crate) fn open_as_seat(
    launcher: &Path,
    uid: u32,
    gid: u32,
    groups: &str,
    path: &Path,
    create: bool,
) -> io::Result<File> {
    open_as_seat_within(launcher, uid, gid, groups, path, create, SEAT_OPEN_TIMEOUT)
}

/// How long a seat helper that answered may take to exit before it is
/// stopped and left to the reaper.
const SEAT_EXIT_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

fn open_as_seat_within(
    launcher: &Path,
    uid: u32,
    gid: u32,
    groups: &str,
    path: &Path,
    create: bool,
    timeout: std::time::Duration,
) -> io::Result<File> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a seat opens an absolute path only",
        ));
    }
    let stage = if create { "seat-create" } else { "seat-open" };
    // A seat whose helpers keep not exiting gets no more: what the reaper
    // holds for one uid stays bounded.
    if reaped_for(uid) >= SEAT_REAPER_PER_UID {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("{stage}: this seat's earlier helpers have not exited"),
        ));
    }
    let (ours, theirs) = std::os::unix::net::UnixStream::pair()?;
    ours.set_read_timeout(Some(timeout))?;
    // The command, and with it this process's copy of the seat's end, is
    // dropped at spawn, so a seat that exits without answering reads as EOF.
    let mut child = {
        let mut command = seat_file_command(launcher, uid, gid, groups)?;
        command
            .arg(if create { "create" } else { "open" })
            .arg(path)
            .stdin(std::process::Stdio::from(std::os::fd::OwnedFd::from(
                theirs,
            )))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped());
        command.spawn().map_err(|error| {
            io::Error::new(error.kind(), format!("{stage} spawn failed: {error}"))
        })?
    };
    let stderr = child.stderr.take().expect("piped seat stderr");
    let mut reader = anyagents_keeper::wire::FrameReader::new(&ours);
    let answer = reader.next_frame();
    let mut fds = reader.take_fds();
    // The helper runs as the seat, which may stop or hang it; this thread
    // never waits on it unbounded. One that does not exit promptly is
    // killed (as the seat, when this process may not signal it) and left to
    // the reaper.
    let grace = if answer.is_err() {
        std::time::Duration::ZERO
    } else {
        SEAT_EXIT_GRACE
    };
    let Some(status) = exit_within(&mut child, grace)? else {
        stop_seat_child(child, launcher, uid, gid, groups);
        return match (answer, fds.len()) {
            (Ok(Some(frame)), 1) if frame == br#""opened""# => {
                regular_file(File::from(fds.remove(0)), stage)
            }
            (Err(error), _) => Err(io::Error::new(
                error.kind(),
                format!("{stage}: the seat did not answer in time: {error}"),
            )),
            _ => Err(io::Error::other(format!(
                "{stage}: the seat did not exit after answering without one opened file"
            ))),
        };
    };
    // The helper has exited. Its reason is what its stderr holds now, read
    // without a thread and never past a deadline: a pipe the seat keeps
    // open (a process it forked) cannot hold this one.
    let prefix = read_until(stderr, SEAT_STDERR_LIMIT as usize, SEAT_EXIT_GRACE);
    if !status.success() {
        let stderr = String::from_utf8_lossy(&prefix);
        // The helper's own reason, without its prefix; Debug escaping keeps
        // control characters out of the caller's message.
        let reason = stderr
            .trim()
            .strip_prefix(&format!(
                "seat-file {}: ",
                if create { "create" } else { "open" }
            ))
            .unwrap_or(stderr.trim());
        return Err(io::Error::new(
            if prefix.is_empty() {
                io::ErrorKind::Other
            } else {
                io::ErrorKind::PermissionDenied
            },
            format!("{stage} failed ({status}): {reason:?}"),
        ));
    }
    match (answer, fds.len()) {
        (Ok(Some(frame)), 1) if frame == br#""opened""# => {
            regular_file(File::from(fds.remove(0)), stage)
        }
        (Err(error), _) => Err(io::Error::new(
            error.kind(),
            format!("{stage} answer: {error}"),
        )),
        _ => Err(io::Error::other(format!(
            "{stage}: the seat answered without one opened file"
        ))),
    }
}

/// Install a Linux POSIX ACL on the bound inode, not its replaceable pathname.
/// Named users are retained even when equal to today's owner: inherited files
/// may be created by either principal. No unrelated group or other gets access.
#[cfg(target_os = "linux")]
fn config_acl(
    fd: BorrowedFd<'_>,
    uid: u32,
    daemon: u32,
    mode: u32,
    inherit: bool,
) -> io::Result<()> {
    let file = File::from(fd.try_clone_to_owned()?);
    let meta = file.metadata()?;
    if !meta.is_dir() && (!meta.is_file() || meta.nlink() != 1) {
        return Err(io::Error::other(
            "config ACL requires a directory or single-link file",
        ));
    }
    let seat = ((mode >> 3) & 7) as u16;
    let owner = ((mode >> 6) & 7) as u16;
    let mut users = std::collections::BTreeMap::new();
    users.insert(uid, seat);
    // A capability is read-only for the seat; its daemon owner already has
    // write authority and a named writer must not widen the ACL mask.
    users.insert(daemon, if seat & 2 != 0 { owner } else { seat });
    let mut bytes = 2u32.to_le_bytes().to_vec();
    let mut entry = |tag: u16, perm: u16, id: u32| {
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&perm.to_le_bytes());
        bytes.extend_from_slice(&id.to_le_bytes());
    };
    entry(1, owner, u32::MAX);
    for (id, perm) in users {
        entry(2, perm, id);
    }
    entry(4, seat, u32::MAX);
    entry(16, seat, u32::MAX);
    entry(32, 0, u32::MAX);
    // setxattr through procfs also handles O_PATH; the open descriptor pins
    // the object across rename/symlink swaps, and the type was checked above.
    let path =
        CString::new(format!("/proc/self/fd/{}", fd.as_raw_fd())).map_err(io::Error::other)?;
    for name in [c"system.posix_acl_access", c"system.posix_acl_default"]
        .into_iter()
        .take(if inherit { 2 } else { 1 })
    {
        // An admitted non-owner may already have the exact grant. Avoid an
        // unnecessary owner-helper launch (or EPERM) for an idempotent repair.
        let mut existing = vec![0u8; bytes.len()];
        let length = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                existing.as_mut_ptr().cast(),
                existing.len(),
            )
        };
        if length == bytes.len() as isize && existing == bytes {
            continue;
        }
        if unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                bytes.as_ptr().cast(),
                bytes.len(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut actual = vec![0u8; bytes.len()];
        let length = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                actual.as_mut_ptr().cast(),
                actual.len(),
            )
        };
        if length < 0 {
            return Err(io::Error::last_os_error());
        }
        if length as usize != bytes.len() || actual != bytes {
            return Err(io::Error::other("config ACL read-back mismatch"));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn config_acl(
    _fd: BorrowedFd<'_>,
    _uid: u32,
    _daemon: u32,
    _mode: u32,
    _inherit: bool,
) -> io::Result<()> {
    Ok(())
}

fn grant_config_acl(file: &File, uid: u32, gid: u32, mode: u32) -> io::Result<()> {
    use std::os::fd::AsFd as _;
    let daemon = super::sandbox::server_uid();
    let meta = file.metadata()?;
    let inherit = meta.is_dir();
    match config_acl(file.as_fd(), uid, daemon, mode, inherit) {
        Ok(()) => Ok(()),
        Err(error) => {
            if super::sandbox::is_isolated_test_env()
                && std::env::var_os("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").is_none()
            {
                return Err(error);
            }
            let Some(launcher) = super::sandbox::descriptor_launcher() else {
                return Err(error);
            };
            let mut command = seat_file_command(
                &launcher,
                meta.uid(),
                meta.gid(),
                &format!("{},{gid}", meta.gid()),
            )?;
            command
                .args([
                    "config-acl",
                    &uid.to_string(),
                    &daemon.to_string(),
                    &format!("{mode:o}"),
                    if inherit { "1" } else { "0" },
                ])
                .stdin(file.try_clone()?);
            run_seat_command(&mut command, "seat-config-acl")
        }
    }
}

#[allow(
    clippy::unnecessary_cast,
    reason = "mode_t is u16 on macOS and u32 on Linux"
)]
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
        return if daemon_access {
            grant_config_acl(file, uid, gid, mode as u32)
        } else {
            Ok(())
        };
    }
    let fd = file.as_raw_fd();
    let group_ok = meta.gid() == gid || unsafe { libc::fchown(fd, u32::MAX, gid) } == 0;
    let mode_ok = unsafe { libc::fchmod(fd, mode) } == 0;
    if group_ok && mode_ok {
        return if daemon_access {
            grant_config_acl(file, uid, gid, mode as u32)
        } else {
            Ok(())
        };
    }
    let mut repair_error = None;
    // Retain the configured owner's existing ability to repair a shared group,
    // but pass only the opened object, never its replaceable original path.
    if (!super::sandbox::is_isolated_test_env()
        || std::env::var_os("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").is_some())
        && let Some(launcher) = super::sandbox::descriptor_launcher()
    {
        let mut command = seat_file_command(
            &launcher,
            meta.uid(),
            meta.gid(),
            &format!("{},{gid}", meta.gid()),
        )?;
        command
            .args(["inode", &format!("{mode:o}"), &gid.to_string()])
            .stdin(file.try_clone()?);
        match run_seat_command(&mut command, "seat-inode") {
            Ok(()) => {
                return if daemon_access {
                    grant_config_acl(file, uid, gid, mode as u32)
                } else {
                    Ok(())
                };
            }
            Err(error) => repair_error = Some(error),
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
    Err(match repair_error {
        Some(error) => io::Error::new(
            error.kind(),
            format!("cannot grant the opened seat inode: {error}"),
        ),
        None => io::Error::other("cannot grant the opened seat inode"),
    })
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
            let Ok(mut rm) =
                seat_file_command(self.launcher, self.uid, self.gid, &self.gid.to_string())
            else {
                return;
            };
            rm.arg("unlink")
                .arg(self.temp)
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
    let mut command = seat_file_command(&launcher, uid, gid, &gid.to_string())?;
    command.arg("copy").arg(&temp).stdin(source.try_clone()?);
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
    run_seat_command(&mut command, "seat-copy").map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("seat inode copy refused; original retained: {error}"),
        )
    })?;
    original.seek(SeekFrom::Start(0))?;
    // The new inode belongs to the seat, which may not share a daemon group.
    // Read back under that exact owner rather than widening it for inspection.
    let mut verify = seat_file_command(&launcher, uid, gid, &gid.to_string())?;
    verify.arg("readback").arg(&temp).stdin(source.try_clone()?);
    unsafe {
        verify.pre_exec(move || {
            if libc::fchdir(fd) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    run_seat_command(&mut verify, "seat-readback").map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("seat copy readback differs; original retained: {error}"),
        )
    })?;
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
    brain_tree: bool,
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
            let stat = unsafe { stat.assume_init() };
            if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
                return Err(io::Error::other("config root must be a directory"));
            }
            inaccessible_child(
                &parent,
                name,
                uid,
                gid,
                &stat,
                ShareAccess {
                    daemon_access: true,
                    runtime_tree: false,
                    brain_tree: false,
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
            brain_tree: false,
        },
        &mut |_, _| {},
    )
}

fn tighten_private_op_dir(directory: &File, name: &OsStr, uid: u32, gid: u32) -> io::Result<()> {
    use std::os::fd::AsFd as _;
    let op_dir = match open_child(directory, name, true) {
        Ok(file) => file,
        #[cfg(target_os = "linux")]
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            let name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            unsafe { File::from_raw_fd(fd) }
        }
        Err(error) => return Err(error),
    };
    let meta = op_dir.metadata()?;
    // A foreign owner's private directory must never be changed by this grant.
    if meta.uid() != uid && meta.uid() != super::sandbox::server_uid() {
        return Err(io::Error::other("private config has an unrelated owner"));
    }
    match tighten_op_dir(op_dir.as_fd()) {
        Ok(()) => Ok(()),
        Err(error) => {
            if super::sandbox::is_isolated_test_env()
                && std::env::var_os("ANYAGENTS_REQUIRE_NATIVE_SEAT_TEST").is_none()
            {
                return Err(error);
            }
            let Some(launcher) = super::sandbox::descriptor_launcher() else {
                return Err(error);
            };
            let mut command = seat_file_command(
                &launcher,
                meta.uid(),
                meta.gid(),
                &format!("{},{gid}", meta.gid()),
            )?;
            command.arg("op-dir").stdin(op_dir);
            run_seat_command(&mut command, "seat-private-config")
        }
    }
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
            && !(access.daemon_access
                && (text == "brain" || (access.brain_tree && text == ".system_generated")))
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
            tighten_private_op_dir(&directory, &name, uid, gid)?;
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
            runtime_tree: access.runtime_tree
                || matches!(
                    text.as_ref(),
                    "cache-home" | "tmp" | "data-home" | "state-home"
                ),
            brain_tree: access.brain_tree || (access.daemon_access && text == "brain"),
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

    #[cfg(target_os = "linux")]
    #[test]
    fn config_acl_inherits_explicit_principals_into_runtime_output() {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        fn acl(path: &Path, name: &std::ffi::CStr) -> Vec<u8> {
            let path = CString::new(path.as_os_str().as_bytes()).unwrap();
            let mut bytes = vec![0u8; 256];
            let size = unsafe {
                libc::getxattr(
                    path.as_ptr(),
                    name.as_ptr(),
                    bytes.as_mut_ptr().cast(),
                    bytes.len(),
                )
            };
            assert!(size > 0, "ACL read failed: {}", io::Error::last_os_error());
            bytes.truncate(size as usize);
            bytes
        }
        fn named(bytes: &[u8], uid: u32) -> Option<u16> {
            bytes[4..].as_chunks::<8>().0.iter().find_map(|entry| {
                let tag = u16::from_le_bytes(entry[..2].try_into().unwrap());
                let id = u32::from_le_bytes(entry[4..].try_into().unwrap());
                (tag == 2 && id == uid).then(|| u16::from_le_bytes(entry[2..4].try_into().unwrap()))
            })
        }
        let temp = crate::test_tempdir();
        let config = temp.path().join("config");
        let brain = config.join(".gemini/antigravity-cli/brain/task");
        std::fs::create_dir_all(&brain).unwrap();
        let executable = config.join(".local/opt/node/bin/node");
        std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
        std::fs::write(&executable, b"#!/bin/sh").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let daemon = crate::sandbox::server_uid();
        let seat = if daemon == 12345 { 12346 } else { 12345 };
        share_config(&config, seat, crate::sandbox::server_gid()).unwrap();
        assert_eq!(
            named(&acl(&executable, c"system.posix_acl_access"), daemon),
            Some(7)
        );
        assert_eq!(
            named(&acl(&executable, c"system.posix_acl_access"), seat),
            Some(7)
        );
        assert_eq!(
            named(&acl(&brain, c"system.posix_acl_default"), daemon),
            Some(7)
        );
        let output = brain.join("output.txt");
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .open(&output)
            .unwrap();
        let inherited = acl(&output, c"system.posix_acl_access");
        assert_eq!(named(&inherited, daemon), Some(7));
        assert_eq!(named(&inherited, seat), Some(7));
        assert_eq!(std::fs::metadata(&output).unwrap().mode() & 0o777, 0o660);
        assert_eq!(std::fs::metadata(&config).unwrap().mode() & 0o007, 0);
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
                brain_tree: false,
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

    #[test]
    fn seat_command_bounds_and_drains_stderr_and_reports_stage_status() {
        use std::process::{Command, Stdio};
        for stage in ["seat-inode", "seat-copy", "seat-readback"] {
            let error = run_seat_command(
                Command::new("/bin/sh")
                    .args(["-c", "printf 'synthetic\\n\\033failure\\377' >&2; exit 43"])
                    .stdin(Stdio::null()),
                stage,
            )
            .unwrap_err()
            .to_string();
            assert!(
                error.contains(&format!("{stage} failed (exit status: 43)")),
                "{error}"
            );
            assert!(error.contains("synthetic\\n"), "{error}");
            assert!(!error.contains('\n') && !error.contains('\x1b'), "{error}");
        }
        // Exceeds a pipe buffer, so a wait-before-read or cap-without-drain
        // implementation hangs instead of observing the child's exit status.
        for code in [0, 44] {
            let result = run_seat_command(
                Command::new("/bin/sh")
                    .args(["-c", &format!("head -c 1048576 /dev/zero >&2; exit {code}")])
                    .stdin(Stdio::null()),
                "seat-copy",
            );
            if code == 0 {
                result.unwrap();
            } else {
                let error = result.unwrap_err().to_string();
                assert!(error.contains("exit status: 44"));
                assert!(error.ends_with(" [truncated]"));
                assert!(error.len() < 4 * SEAT_STDERR_LIMIT as usize + 100);
            }
        }
    }

    #[test]
    fn seat_command_reports_spawn_failure_and_signal() {
        use std::process::{Command, Stdio};
        let dir = tempfile::tempdir().unwrap();
        let error = run_seat_command(
            Command::new(dir.path().join("missing-launcher")).stdin(Stdio::null()),
            "seat-copy",
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.to_string().contains("seat-copy spawn failed"));
        let error = run_seat_command(
            Command::new("/bin/sh")
                .args(["-c", "kill -TERM $$"])
                .stdin(Stdio::null()),
            "seat-readback",
        )
        .unwrap_err();
        assert!(error.to_string().contains("seat-readback failed (signal:"));
    }

    /// The helper as a seat, under `cargo test`: the test harness stands in
    /// for this binary's `internal seat-file`.
    #[test]
    fn seat_file_worker() {
        let args: Vec<OsString> = std::env::args_os().collect();
        let Some(start) = args.iter().position(|arg| arg == "seat-file") else {
            return;
        };
        let code = seat_file_main(&args[start + 1..]);
        std::process::exit(if code == std::process::ExitCode::SUCCESS {
            0
        } else if code == std::process::ExitCode::from(2) {
            2
        } else {
            1
        });
    }

    /// A stand-in for setpriv that runs the command as the same user. It
    /// records the environment it was given in `launcher-env`, fails the stage
    /// named by a `mode` file beside it, and puts a `hostile-bin` directory
    /// beside it first on the command's `PATH` when there is one.
    #[cfg(target_os = "linux")]
    fn fake_seat_launcher(dir: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let launcher = dir.join("fake-seat-launcher.sh");
        std::fs::write(
            &launcher,
            r#"#!/bin/sh
here=${0%/*}
export -p > "$here/launcher-env"
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do shift; done
shift
case "$*" in *"/dev/fd/3"*) exit 97 ;; esac
mode=ok
if [ -f "$here/mode" ]; then read -r mode < "$here/mode"; fi
op=
for arg do
  case $arg in copy|readback|unlink|inode|op-dir|config-acl) op=$arg ;; esac
  name=$arg
done
if [ "$mode" = fail_copy_after_touch ] && [ "$op" = copy ]; then
  : > "./$name"
  echo "synthetic copy denied" >&2
  exit 41
fi
if [ "$mode" = fail_verify ] && [ "$op" = readback ]; then
  echo "synthetic readback denied" >&2
  exit 42
fi
if [ -d "$here/hostile-bin" ]; then PATH="$here/hostile-bin:$PATH"; export PATH; fi
exec "$@"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
        launcher
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn migrate_open_file_uses_fchdir_without_dev_fd_3_and_unlinks_temp_on_failure() {
        let _lock = crate::server::tests::env_lock();
        let temp = crate::test_tempdir();
        let launcher = fake_seat_launcher(temp.path());
        let mode = temp.path().join("mode");

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
                }
            }
        }
        let _reset = EnvReset;
        unsafe {
            std::env::set_var("ANYAGENTS_LAUNCHER", &launcher);
        }
        std::fs::write(&mode, "fail_copy_after_touch\n").unwrap();
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
        assert!(
            err.to_string()
                .contains("seat-copy failed (exit status: 41)")
        );
        assert!(err.to_string().contains("synthetic copy denied"));
        assert_eq!(std::fs::read(&shm).unwrap(), vec![0xABu8; 32768]);
        assert_eq!(
            std::fs::metadata(&shm).unwrap().ino(),
            child.metadata().unwrap().ino()
        );
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

        std::fs::write(&mode, "fail_verify\n").unwrap();
        let err2 = migrate_open_file(
            &parent,
            OsStr::new("sessions.sqlite3-shm"),
            &child,
            other_uid,
            gid,
        )
        .unwrap_err();
        assert!(err2.to_string().contains("seat copy readback differs"));
        assert!(
            err2.to_string()
                .contains("seat-readback failed (exit status: 42)")
        );
        assert!(err2.to_string().contains("synthetic readback denied"));
        assert_eq!(std::fs::read(&shm).unwrap(), vec![0xABu8; 32768]);
        assert_eq!(
            std::fs::metadata(&shm).unwrap().ino(),
            child.metadata().unwrap().ino()
        );
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

        std::fs::write(&mode, "ok\n").unwrap();
        migrate_open_file(
            &parent,
            OsStr::new("sessions.sqlite3-shm"),
            &child,
            other_uid,
            gid,
        )
        .expect("fchdir copy + verify + atomic swap must succeed without /dev/fd/3");
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
        }
        share_config(&cfg, me, gid).unwrap();
        assert!(
            !stale_orphan.exists(),
            "share_config must reap stale .seat-copy-* orphan"
        );
    }

    /// Every seat command runs this binary's own helper with a fixed
    /// environment. A `PATH` whose `cat`, `cmp`, `sync`, `chmod`, `chgrp`,
    /// `rm` and `sh` all fail (as the desk's guard wrappers did for a seat
    /// that could not read them) changes nothing, and the seat never sees
    /// the daemon's environment.
    #[cfg(target_os = "linux")]
    #[test]
    fn seat_file_commands_ignore_a_hostile_path_and_the_daemon_environment() {
        use std::os::unix::fs::PermissionsExt;
        let _lock = crate::server::tests::env_lock();
        let temp = crate::test_tempdir();
        let launcher = fake_seat_launcher(temp.path());
        let hostile = temp.path().join("hostile-bin");
        std::fs::create_dir(&hostile).unwrap();
        for tool in ["cat", "cmp", "sync", "chmod", "chgrp", "rm", "sh"] {
            let path = hostile.join(tool);
            std::fs::write(&path, "#!/bin/sh\necho \"hostile $0\" >&2\nexit 1\n").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        struct EnvReset;
        impl Drop for EnvReset {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("ANYAGENTS_LAUNCHER");
                    std::env::remove_var("ANYAGENTS_SEAT_TEST_DAEMON_ONLY");
                }
            }
        }
        let _reset = EnvReset;
        unsafe {
            std::env::set_var("ANYAGENTS_LAUNCHER", &launcher);
            std::env::set_var("ANYAGENTS_SEAT_TEST_DAEMON_ONLY", "daemon-secret");
        }
        let me = crate::sandbox::server_uid();
        let gid = crate::sandbox::server_gid();
        let other_uid = if me == 42001 { 42002 } else { 42001 };

        // The migration that stopped eight agents: copy, read back, swap.
        let cfg = temp.path().join("config");
        std::fs::create_dir(&cfg).unwrap();
        let db = cfg.join("opencode.db");
        let bytes: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&db, &bytes).unwrap();
        let parent = open_root(&cfg).unwrap();
        let child = open_child(&parent, OsStr::new("opencode.db"), false).unwrap();
        migrate_open_file(&parent, OsStr::new("opencode.db"), &child, other_uid, gid)
            .expect("migration must not depend on PATH");
        assert_eq!(std::fs::read(&db).unwrap(), bytes);
        assert_ne!(
            std::fs::metadata(&db).unwrap().ino(),
            child.metadata().unwrap().ino()
        );
        let seen = std::fs::read_to_string(temp.path().join("launcher-env")).unwrap();
        assert!(seen.contains(SEAT_PATH), "{seen}");
        assert!(!seen.contains("daemon-secret"), "{seen}");
        assert!(!seen.contains("ANYAGENTS_LAUNCHER"), "{seen}");

        let run = |args: &[&str], stdin: File| {
            let mut command = seat_file_command(&launcher, me, gid, &gid.to_string()).unwrap();
            command.args(args).stdin(stdin);
            unsafe {
                use std::os::unix::process::CommandExt as _;
                let dir = parent.as_raw_fd();
                command.pre_exec(move || {
                    if libc::fchdir(dir) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            run_seat_command(&mut command, "seat-test")
        };
        // A read-back that differs is refused by the helper itself.
        let other = temp.path().join("other");
        std::fs::write(&other, b"different").unwrap();
        std::fs::write(cfg.join(".seat-copy-check"), b"different").unwrap();
        run(
            &["readback", ".seat-copy-check"],
            File::open(&other).unwrap(),
        )
        .unwrap();
        std::fs::write(&other, b"differenT").unwrap();
        let error = run(
            &["readback", ".seat-copy-check"],
            File::open(&other).unwrap(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("copy differs from its source"), "{error}");
        run(&["unlink", ".seat-copy-check"], File::open(&other).unwrap()).unwrap();
        assert!(!cfg.join(".seat-copy-check").exists());
        // Group and mode through an O_PATH descriptor, as `grant` passes one.
        let opath = |path: &Path| {
            let name = CString::new(path.as_os_str().as_bytes()).unwrap();
            let fd = unsafe {
                libc::open(
                    name.as_ptr(),
                    libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            assert!(fd >= 0);
            unsafe { File::from_raw_fd(fd) }
        };
        run(&["inode", "640", &gid.to_string()], opath(&other)).unwrap();
        assert_eq!(std::fs::metadata(&other).unwrap().mode() & 0o7777, 0o640);
        let op = temp.path().join("op");
        std::fs::create_dir(&op).unwrap();
        std::fs::write(op.join("config"), b"x").unwrap();
        std::fs::set_permissions(&op, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(op.join("config"), std::fs::Permissions::from_mode(0o644))
            .unwrap();
        run(&["op-dir"], opath(&op)).unwrap();
        assert_eq!(std::fs::metadata(&op).unwrap().mode() & 0o7777, 0o700);
        assert_eq!(
            std::fs::metadata(op.join("config")).unwrap().mode() & 0o7777,
            0o600
        );
        let error = run(&["copy", "../escape"], File::open(&other).unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("exit status: 2"), "{error}");
    }

    /// A launcher that records its arguments in `launcher-args` and runs the
    /// command as this same user.
    #[cfg(target_os = "linux")]
    fn recording_launcher(dir: &Path) -> PathBuf {
        let launcher = dir.join("recording-launcher.sh");
        crate::repo::tests::write_executable(
            &launcher,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"${0%/*}/launcher-args\"\n\
             while [ \"$#\" -gt 0 ] && [ \"$1\" != \"--\" ]; do shift; done\nshift\nexec \"$@\"\n",
        );
        launcher
    }

    /// `open` and `create` hand back only what the seat itself opened: a
    /// regular file, never through a final link, by absolute path, with the
    /// seat's own uid, gid and groups on the launcher's command line.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_seat_opens_a_file_itself_and_hands_back_the_descriptor() {
        use std::io::{Read as _, Write as _};
        let temp = crate::test_tempdir();
        let launcher = recording_launcher(temp.path());
        let open = |path: &Path, create: bool| {
            open_as_seat(&launcher, 42001, 42002, "42002,42003", path, create)
        };
        let photo = temp.path().join("photo.png");
        std::fs::write(&photo, b"png bytes").unwrap();
        let mut bytes = Vec::new();
        open(&photo, false)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, b"png bytes");
        let args = std::fs::read_to_string(temp.path().join("launcher-args")).unwrap();
        let args: Vec<_> = args.lines().collect();
        for pair in [
            ["--reuid", "42001"],
            ["--regid", "42002"],
            ["--groups", "42002,42003"],
        ] {
            assert!(args.windows(2).any(|w| w == pair), "{args:?}");
        }
        assert!(args.contains(&"--no-new-privs"), "{args:?}");
        assert_eq!(args[args.len() - 2..], ["open", photo.to_str().unwrap()]);

        let secret = temp.path().join("operator-token");
        std::fs::write(&secret, b"operator-secret").unwrap();
        std::os::unix::fs::symlink(&secret, temp.path().join("link")).unwrap();
        let fifo = temp.path().join("fifo");
        let c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
        let error = open(&temp.path().join("link"), false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("seat-open failed"), "{error}");
        assert!(error.contains("symbolic links"), "{error}");
        let error = open(&fifo, false).unwrap_err().to_string();
        assert!(error.contains("not a regular file"), "{error}");
        assert!(open(temp.path(), false).is_err());
        assert!(open(&temp.path().join("missing"), false).is_err());
        let error = open(Path::new("photo.png"), false).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let out = temp.path().join("out.png");
        open(&out, true).unwrap().write_all(b"written").unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"written");
        assert!(open(&temp.path().join("link"), true).is_err());
        assert_eq!(std::fs::read(&secret).unwrap(), b"operator-secret");
    }

    /// A seat that answers without a descriptor, or whose launcher fails,
    /// yields an error, never a file.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_seat_that_hands_back_nothing_opens_nothing() {
        let temp = crate::test_tempdir();
        let launcher = temp.path().join("silent.sh");
        crate::repo::tests::write_executable(&launcher, "#!/bin/sh\nexit 0\n");
        let file = temp.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let error = open_as_seat(&launcher, 42001, 42002, "42002", &file, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("without one opened file"), "{error}");
        let launcher = temp.path().join("denying.sh");
        crate::repo::tests::write_executable(&launcher, "#!/bin/sh\necho denied >&2\nexit 3\n");
        let error = open_as_seat(&launcher, 42001, 42002, "42002", &file, false)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("exit status: 3") && error.contains("denied"),
            "{error}"
        );
    }

    /// A seat that stops its own helper (or never answers) holds no daemon
    /// thread: the open fails at its timeout, the helper is killed and left
    /// to the reaper, and nothing waits on it.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_stopped_seat_helper_never_pins_the_caller() {
        let temp = crate::test_tempdir();
        let launcher = temp.path().join("stopping.sh");
        crate::repo::tests::write_executable(&launcher, "#!/bin/sh\nkill -STOP $$\nexit 0\n");
        let file = temp.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let started = std::time::Instant::now();
        let error = open_as_seat_within(
            &launcher,
            42001,
            42002,
            "42002",
            &file,
            false,
            std::time::Duration::from_millis(500),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("did not answer in time"), "{error}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        // The reaper collects the killed helper.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !SEAT_REAPER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
        {
            assert!(std::time::Instant::now() < deadline, "helper never reaped");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// A helper whose stderr a process it forked keeps open still yields
    /// its reason within the grace, and a seat's helpers held by the reaper
    /// are bounded: past the bound its next open is refused.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_held_stderr_and_a_full_reaper_never_pin_the_caller() {
        let temp = crate::test_tempdir();
        let launcher = temp.path().join("holding.sh");
        crate::repo::tests::write_executable(
            &launcher,
            "#!/bin/sh\n/bin/sleep 30 </dev/null >/dev/null &\necho denied >&2\nexit 3\n",
        );
        let file = temp.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let started = std::time::Instant::now();
        let error = open_as_seat(&launcher, 42011, 42012, "42012", &file, false)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("exit status: 3") && error.contains("denied"),
            "{error}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );

        let uid = 42021;
        let mut sleepers = Vec::new();
        for _ in 0..SEAT_REAPER_PER_UID {
            let child = std::process::Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .unwrap();
            sleepers.push(child.id());
            reap_later(uid, child);
        }
        let error = open_as_seat(&launcher, uid, 42012, "42012", &file, false).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock, "{error}");
        for pid in sleepers {
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while reaped_for(uid) > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "sleepers never reaped"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// `kill <pid>` signals one of the seat's own processes, and nothing
    /// that is not a pid.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_seat_kills_its_own_helper_on_request() {
        use std::os::unix::process::ExitStatusExt as _;
        let temp = crate::test_tempdir();
        let launcher = recording_launcher(temp.path());
        let mut sleeper = std::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let kill = |pid: &str| {
            let mut command = seat_file_command(&launcher, 42001, 42002, "42002").unwrap();
            command
                .args(["kill", pid])
                .stdin(std::process::Stdio::null());
            run_seat_command(&mut command, "seat-kill")
        };
        kill(&sleeper.id().to_string()).unwrap();
        assert_eq!(sleeper.wait().unwrap().signal(), Some(libc::SIGKILL));
        for bad in ["1", "0", "-1", "self"] {
            let error = kill(bad).unwrap_err().to_string();
            assert!(error.contains("exit status: 2"), "{bad}: {error}");
        }
    }

    /// With a real seat (root drops to a fixture uid through `setpriv`): the
    /// seat cannot open the operator's token, a link to it, or create beside
    /// it, and opens what it may. Required on the privileged Linux lane.
    #[cfg(target_os = "linux")]
    #[test]
    fn privileged_seat_open_refuses_what_the_seat_cannot_read() {
        use std::io::Read as _;
        use std::os::unix::fs::{PermissionsExt as _, symlink};
        let caps = std::fs::read_to_string("/proc/self/status").unwrap();
        let effective = caps
            .lines()
            .find_map(|l| l.strip_prefix("CapEff:\t"))
            .and_then(|n| u64::from_str_radix(n, 16).ok())
            .unwrap_or(0);
        let required = (1 << 0) | (1 << 6) | (1 << 7); // CHOWN, SETGID, SETUID
        if effective & required != required
            || crate::sandbox::server_uid() != 0
            || !Path::new("/usr/bin/setpriv").is_file()
        {
            assert!(
                std::env::var_os("ANYAGENTS_REQUIRE_PRIVILEGED_WORKTREE_TEST").is_none(),
                "requires root with CAP_CHOWN, CAP_SETGID, CAP_SETUID and /usr/bin/setpriv"
            );
            eprintln!("requires privilege; seat open acceptance not executed");
            return;
        }
        let (uid, gid) = (24061u32, 24062u32);
        let base = tempfile::Builder::new()
            .prefix("seat-open-")
            .tempdir_in("/var/tmp")
            .unwrap();
        let root = std::fs::canonicalize(base.path()).unwrap();
        let chown = |path: &Path, u: u32, g: u32| {
            let c = CString::new(path.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::lchown(c.as_ptr(), u, g) }, 0);
        };
        let mode = |path: &Path, m: u32| {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(m)).unwrap();
        };
        mode(&root, 0o711);
        // The server's secrets: root's own, as the operator token is kyle's.
        let secrets = root.join("secrets");
        std::fs::create_dir(&secrets).unwrap();
        mode(&secrets, 0o711);
        let token = secrets.join("operator-token");
        std::fs::write(&token, b"operator-secret").unwrap();
        mode(&token, 0o600);
        let public = root.join("public.png");
        std::fs::write(&public, b"public").unwrap();
        mode(&public, 0o644);
        // The seat's own directory, with a link to the token in it.
        let own = root.join("seat");
        std::fs::create_dir(&own).unwrap();
        chown(&own, uid, gid);
        mode(&own, 0o700);
        let mine = own.join("mine.png");
        std::fs::write(&mine, b"mine").unwrap();
        chown(&mine, uid, gid);
        mode(&mine, 0o600);
        symlink(&token, own.join("link")).unwrap();
        chown(&own.join("link"), uid, gid);
        let launcher = Path::new("/usr/bin/setpriv");
        let open = |path: &Path, create: bool| {
            open_as_seat(launcher, uid, gid, &gid.to_string(), path, create)
        };
        let read = |path: &Path| {
            let mut bytes = Vec::new();
            open(path, false)?.read_to_end(&mut bytes)?;
            Ok::<_, io::Error>(bytes)
        };
        assert_eq!(read(&public).unwrap(), b"public");
        assert_eq!(read(&mine).unwrap(), b"mine");
        let error = read(&token).unwrap_err().to_string();
        assert!(error.contains("Permission denied"), "{error}");
        let error = read(&own.join("link")).unwrap_err().to_string();
        assert!(error.contains("symbolic links"), "{error}");
        assert!(open(&secrets.join("planted"), true).is_err());
        assert!(!secrets.join("planted").exists());
        let created = own.join("out.png");
        drop(open(&created, true).unwrap());
        assert_eq!(std::fs::metadata(&created).unwrap().uid(), uid);
        assert_eq!(std::fs::read(&token).unwrap(), b"operator-secret");
        // The same refusal through the authority a send uses.
        let _lock = crate::server::tests::env_lock();
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                unsafe { std::env::remove_var("ANYAGENTS_LAUNCHER") };
            }
        }
        let _reset = Reset;
        unsafe { std::env::set_var("ANYAGENTS_LAUNCHER", launcher) };
        let seat = crate::agent_file::FileAuthority::Seat {
            uid,
            gid,
            groups: gid.to_string(),
        };
        let why = seat
            .read(&token, crate::agent_file::MEDIA_UPLOAD_LIMIT)
            .unwrap_err();
        assert!(why.contains("Permission denied"), "{why}");
        assert_eq!(
            seat.read(&public, crate::agent_file::MEDIA_UPLOAD_LIMIT)
                .unwrap(),
            b"public"
        );
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "seat_config_acl_tests.rs"]
mod privileged_config_tests;
