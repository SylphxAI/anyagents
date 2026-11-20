//! Host files the server opens because an agent named them: a local media
//! path to upload.
//!
//! ADR 0010 D3: what the server does on an agent's behalf never uses the
//! server's own access. A seat agent's file is opened by that seat (the
//! `internal seat-file` helper hands back the descriptor it opened); an agent
//! that shares the server's uid reaches only files beneath its own
//! directories, resolved with `openat2(RESOLVE_BENEATH)`; only the operator,
//! and a CLI acting as its own user, open with their own access. No name is
//! checked as the server and then opened, so neither a symbolic link nor a
//! rename between check and open can move the result.

use std::fs::File;
use std::io::{self, Read as _};
use std::path::{Component, Path, PathBuf};

use anyagents_core::{Agent, Store};

/// The largest local file a send uploads: Telegram's own Bot API limit.
pub(crate) const MEDIA_UPLOAD_LIMIT: u64 = 50 * 1024 * 1024;

/// Whose access opens a host file a caller named.
#[derive(Clone, Debug)]
pub(crate) enum FileAuthority {
    /// The operator, or a CLI as its own user: that caller's own access.
    Own,
    /// An agent in its own OS seat: the seat opens the file.
    Seat { uid: u32, gid: u32, groups: String },
    /// An agent sharing the server's uid: only beneath its own directories.
    Beneath(Beneath),
    /// An agent that may open nothing on this host, and why.
    Refused(String),
}

/// The directories a seatless agent's files are resolved beneath.
#[derive(Clone, Debug, Default)]
pub(crate) struct Beneath {
    /// Its own: read and write.
    own: Vec<PathBuf>,
    /// Read-only roots: shared channel media and recipient-private copies.
    shared: Vec<PathBuf>,
}

impl FileAuthority {
    /// The authority of the agent `id`.
    pub(crate) fn of_agent_id(store: &Store, id: &anyagents_core::AgentId) -> Self {
        match store.agent(id) {
            Ok(agent) => Self::for_agent(store.data_dir(), &agent),
            Err(_) => Self::Refused("the calling agent no longer exists".into()),
        }
    }

    /// An agent's authority: its seat when the operator admitted one, its own
    /// directories when this host runs every agent as the server's uid, and
    /// nothing when seats are configured but this agent has none.
    pub(crate) fn for_agent(data: &Path, agent: &Agent) -> Self {
        if let Some((_, uid, gid, socket_gid)) = crate::sandbox::seat_for_agent(agent.id.as_str()) {
            return Self::Seat {
                uid,
                gid,
                groups: format!("{gid},{socket_gid}"),
            };
        }
        if crate::sandbox::seats_configured() {
            return Self::Refused(format!(
                "agent `{}` has no seat on this host, so the server opens no host file for it",
                agent.name
            ));
        }
        let mut own = Vec::new();
        if let Some(workdir) = agent.workdir.as_deref().filter(|w| !w.trim().is_empty()) {
            own.push(PathBuf::from(workdir));
        }
        own.push(anyagents_core::agent_work_dir(data, &agent.id));
        own.push(data.join("agents").join(agent.id.as_str()));
        if let Some(scratch) = anyagents_core::configured_scratch_dir() {
            own.push(anyagents_core::agent_scratch_build_dir(&scratch, &agent.id));
            own.push(anyagents_core::agent_scratch_tmp_dir(&scratch, &agent.id));
        }
        Self::Beneath(Beneath {
            own,
            shared: vec![
                data.join(anyagents_harness::media::MEDIA_DIR),
                data.join("inbound-media").join(agent.id.as_str()),
            ],
        })
    }

    /// The bytes of the regular file `path`, at most `limit` of them.
    pub(crate) fn read(&self, path: &Path, limit: u64) -> Result<Vec<u8>, String> {
        let shown = path.display();
        let file = self
            .open(path)
            .map_err(|error| format!("cannot read {shown}: {error}"))?;
        let size = file
            .metadata()
            .map_err(|error| format!("cannot read {shown}: {error}"))?
            .len();
        if size > limit {
            return Err(format!(
                "{shown} is {size} bytes; the limit is {limit} bytes"
            ));
        }
        let mut bytes = Vec::with_capacity(size as usize);
        file.take(limit + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("cannot read {shown}: {error}"))?;
        if bytes.len() as u64 > limit {
            return Err(format!(
                "{shown} grew past the {limit}-byte limit while read"
            ));
        }
        Ok(bytes)
    }

    fn open(&self, path: &Path) -> io::Result<File> {
        match self {
            Self::Own => open_own(path),
            Self::Refused(why) => Err(io::Error::new(io::ErrorKind::PermissionDenied, why.clone())),
            Self::Seat { .. } | Self::Beneath(_) if !path.is_absolute() => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "an agent's file path must be absolute",
            )),
            Self::Seat { uid, gid, groups } => {
                let launcher = crate::sandbox::descriptor_launcher().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "the seat launcher is not installed, so no file is opened as the seat",
                    )
                })?;
                crate::seat_tree::open_as_seat(&launcher, *uid, *gid, groups, path, false)
            }
            Self::Beneath(beneath) => beneath.open(path),
        }
    }
}

/// The caller's own access, following links as its own open would.
fn open_own(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    let file = options
        .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY | libc::O_CLOEXEC)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("not a regular file"));
    }
    Ok(file)
}

impl Beneath {
    fn open(&self, path: &Path) -> io::Result<File> {
        let candidates = self.own.iter().chain(self.shared.iter());
        for root in candidates {
            // A root is the server's own choice, so it may be named through a
            // link; the agent's part below it never leaves it.
            for root in [Some(root.to_path_buf()), std::fs::canonicalize(root).ok()]
                .into_iter()
                .flatten()
            {
                let Ok(relative) = path.strip_prefix(&root) else {
                    continue;
                };
                match File::open(&root) {
                    Ok(directory) => return open_beneath(&directory, relative),
                    // Roots nest (its directory holds its work directory).
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "outside this agent's own directories",
        ))
    }
}

/// `relative` opened beneath `directory`: no `..`, absolute link or magic
/// link may leave it, and the final component is never a link.
fn open_beneath(directory: &File, relative: &Path) -> io::Result<File> {
    if relative.as_os_str().is_empty() {
        return Err(io::Error::other("not a regular file"));
    }
    let flags =
        libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_NOCTTY | libc::O_CLOEXEC | libc::O_RDONLY;
    let file = open_beneath_raw(directory, relative, flags)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("not a regular file"));
    }
    Ok(file)
}

#[cfg(target_os = "linux")]
fn open_beneath_raw(directory: &File, relative: &Path, flags: libc::c_int) -> io::Result<File> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;
    #[repr(C)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }
    let name = std::ffi::CString::new(relative.as_os_str().as_bytes()).map_err(io::Error::other)?;
    let how = OpenHow {
        flags: flags as u64,
        mode: if flags & libc::O_CREAT != 0 { 0o644 } else { 0 },
        resolve: libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS,
    };
    // SAFETY: a valid directory descriptor, a NUL-terminated name and an
    // `open_how` of the size passed.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            directory.as_raw_fd(),
            name.as_ptr(),
            &how as *const OpenHow,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd >= 0 {
        // SAFETY: openat2 returned a new descriptor this process owns.
        return Ok(unsafe { File::from_raw_fd(fd as libc::c_int) });
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        // A kernel before 5.6: walk it one component at a time instead.
        Some(libc::ENOSYS) => open_by_components(directory, relative, flags),
        Some(libc::EXDEV) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "the path leaves this agent's own directories",
        )),
        _ => Err(error),
    }
}

#[cfg(not(target_os = "linux"))]
fn open_beneath_raw(directory: &File, relative: &Path, flags: libc::c_int) -> io::Result<File> {
    open_by_components(directory, relative, flags)
}

/// `relative` one component at a time, refusing `..` and every link.
fn open_by_components(directory: &File, relative: &Path, flags: libc::c_int) -> io::Result<File> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;
    let parts: Vec<_> = relative.components().collect();
    let mut current = directory.try_clone()?;
    for (index, part) in parts.iter().enumerate() {
        let Component::Normal(name) = part else {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the path leaves this agent's own directories",
            ));
        };
        let last = index + 1 == parts.len();
        let name = std::ffi::CString::new(name.as_bytes()).map_err(io::Error::other)?;
        let open_flags = if last {
            flags
        } else {
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
        };
        // SAFETY: a valid directory descriptor and a NUL-terminated name.
        let fd = unsafe {
            libc::openat(
                current.as_raw_fd(),
                name.as_ptr(),
                open_flags,
                0o644 as libc::c_uint,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openat returned a new descriptor this process owns.
        current = unsafe { File::from_raw_fd(fd) };
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn beneath(own: &Path, shared: &Path) -> FileAuthority {
        FileAuthority::Beneath(Beneath {
            own: vec![own.to_path_buf()],
            shared: vec![shared.to_path_buf()],
        })
    }

    /// A seatless agent reads its own files and inbound media, and nothing else
    /// the server could: not the operator token, not through a link, not
    /// through `..`.
    #[test]
    fn a_seatless_agent_reads_only_beneath_its_own_directories() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        let work = data.join("agents/a1/work");
        let media = data.join("media");
        for dir in [&work, &media, &data.join("secrets")] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let token = data.join("secrets/operator-token");
        std::fs::write(&token, b"operator-secret").unwrap();
        std::fs::write(work.join("photo.png"), b"png").unwrap();
        std::fs::write(media.join("in.jpg"), b"jpg").unwrap();
        let files = beneath(&work, &media);

        assert_eq!(files.read(&work.join("photo.png"), 100).unwrap(), b"png");
        assert_eq!(files.read(&media.join("in.jpg"), 100).unwrap(), b"jpg");
        for refused in [
            token.clone(),
            work.join("../../../secrets/operator-token"),
            PathBuf::from("photo.png"),
        ] {
            assert!(files.read(&refused, 100).is_err(), "{}", refused.display());
        }
        // A link to the token, as the final component or as a directory.
        symlink(&token, work.join("token-link")).unwrap();
        symlink(data.join("secrets"), work.join("secrets-link")).unwrap();
        let error = files.read(&work.join("token-link"), 100).unwrap_err();
        assert!(error.contains("token-link"), "{error}");
        files
            .read(&work.join("secrets-link/operator-token"), 100)
            .unwrap_err();
        // A FIFO is not a file, and opening one never waits.
        let fifo = work.join("fifo");
        let c = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
        assert!(files.read(&fifo, 100).unwrap_err().contains("regular file"));
        // The size limit holds.
        assert!(
            files
                .read(&work.join("photo.png"), 2)
                .unwrap_err()
                .contains("limit")
        );
    }

    #[test]
    fn the_component_walk_refuses_every_link_and_parent() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        std::fs::create_dir_all(root.join("sub")).unwrap();
        std::fs::write(root.join("sub/file"), b"ok").unwrap();
        std::fs::write(temp.path().join("outside"), b"secret").unwrap();
        symlink(temp.path().join("outside"), root.join("link")).unwrap();
        symlink("sub", root.join("dirlink")).unwrap();
        let dir = File::open(&root).unwrap();
        let flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        let mut ok = String::new();
        open_by_components(&dir, Path::new("sub/file"), flags)
            .unwrap()
            .read_to_string(&mut ok)
            .unwrap();
        assert_eq!(ok, "ok");
        for refused in ["link", "dirlink/file", "sub/../../outside"] {
            open_by_components(&dir, Path::new(refused), flags).unwrap_err();
        }
    }

    #[test]
    fn a_refused_agent_opens_nothing_and_the_operator_opens_its_own() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let refused = FileAuthority::Refused("no seat".into());
        assert!(refused.read(&file, 10).unwrap_err().contains("no seat"));
        assert_eq!(FileAuthority::Own.read(&file, 10).unwrap(), b"x");
        // The operator follows its own links, but still reads files only.
        symlink(&file, temp.path().join("link")).unwrap();
        assert_eq!(
            FileAuthority::Own
                .read(&temp.path().join("link"), 10)
                .unwrap(),
            b"x"
        );
        assert!(FileAuthority::Own.read(temp.path(), 10).is_err());
    }
}
