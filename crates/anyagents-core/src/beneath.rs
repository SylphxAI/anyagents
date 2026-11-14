//! Files below a trusted directory, reached without following any link.
//!
//! An agent's `memory/`, `config/` and `skills/` directories are writable by
//! its OS seat, and the server opens files in them as its own user. A link
//! the seat planted there must never carry a server read or write outside
//! (ADR 0010 D3). Every operation here works from an opened directory
//! descriptor, opens each component with `O_NOFOLLOW`, and never resolves a
//! name twice, so neither a link nor a rename between check and use moves it.
//! Only the starting directory, which the caller chose, is opened by path.

use std::ffi::{CString, OsStr, OsString};
use std::fs::{File, Metadata};
use std::io::{self, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Component, Path};

/// An opened directory.
#[derive(Debug)]
pub struct Dir(File);

/// What a directory entry is, read without following it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    File,
    Dir,
    /// A link, a socket, a device or anything else: never entered or read.
    Other,
}

fn c_name(name: &OsStr) -> io::Result<CString> {
    if name.is_empty() || name == "." || name == ".." || name.as_bytes().contains(&b'/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a single directory entry name",
        ));
    }
    CString::new(name.as_bytes()).map_err(io::Error::other)
}

fn check(fd: libc::c_int) -> io::Result<File> {
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: a descriptor the call just returned and nothing else owns.
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

/// A link where a directory or file was expected reads as this error.
fn not_plain(error: io::Error) -> io::Error {
    match error.raw_os_error() {
        Some(libc::ELOOP) | Some(libc::ENOTDIR) => io::Error::new(
            io::ErrorKind::InvalidData,
            "a link or a non-directory where a plain directory or file was expected",
        ),
        _ => error,
    }
}

impl Dir {
    /// `path` itself, which the caller chose: links in it are followed.
    pub fn open(path: &Path) -> io::Result<Self> {
        let name = CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other)?;
        // SAFETY: a NUL-terminated path.
        check(unsafe {
            libc::open(
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        })
        .map(Self)
    }

    /// Absolute `path` with no link at any component, as
    /// [`crate::raw_segment::open_readonly`] opens a file.
    pub fn open_no_follow(path: &Path) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "an absolute directory path is required",
            ));
        }
        Self::open(Path::new("/"))?.dir(path.strip_prefix("/").unwrap_or(path))
    }

    /// The directory `relative` below this one, every component a plain
    /// directory and never a link.
    pub fn dir(&self, relative: &Path) -> io::Result<Self> {
        let mut current = Self(self.0.try_clone()?);
        for part in relative.components() {
            match part {
                Component::Normal(name) => {
                    let name = c_name(name)?;
                    // SAFETY: a directory descriptor and a NUL-terminated name.
                    current = check(unsafe {
                        libc::openat(
                            current.0.as_raw_fd(),
                            name.as_ptr(),
                            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                        )
                    })
                    .map(Self)
                    .map_err(not_plain)?;
                }
                Component::CurDir => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "a relative path below the directory is required",
                    ));
                }
            }
        }
        Ok(current)
    }

    /// The directory `name`, created with `mode` when it is absent; an
    /// existing link there is refused, never followed.
    pub fn create_dir(&self, name: &OsStr, mode: libc::mode_t) -> io::Result<Self> {
        let c = c_name(name)?;
        // SAFETY: a directory descriptor and a NUL-terminated name.
        if unsafe { libc::mkdirat(self.0.as_raw_fd(), c.as_ptr(), mode) } != 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }
        self.dir(Path::new(name))
    }

    /// The regular file `name`, read-only. A link, a FIFO or a device is
    /// refused, and opening one never waits.
    pub fn open_file(&self, name: &OsStr) -> io::Result<File> {
        let c = c_name(name)?;
        // SAFETY: a directory descriptor and a NUL-terminated name.
        let file = check(unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                c.as_ptr(),
                libc::O_RDONLY
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK
                    | libc::O_NOCTTY
                    | libc::O_CLOEXEC,
            )
        })
        .map_err(not_plain)?;
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a regular file",
            ));
        }
        Ok(file)
    }

    /// The bytes of the regular file `name`.
    pub fn read(&self, name: &OsStr) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        io::Read::read_to_end(&mut self.open_file(name)?, &mut bytes)?;
        Ok(bytes)
    }

    /// What `name` is, without following it; `None` when it is absent.
    pub fn kind(&self, name: &OsStr) -> io::Result<Option<Kind>> {
        let c = c_name(name)?;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: a directory descriptor, a NUL-terminated name and room for
        // one stat.
        if unsafe {
            libc::fstatat(
                self.0.as_raw_fd(),
                c.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(error)
            };
        }
        // SAFETY: fstatat filled it.
        let mode = unsafe { stat.assume_init() }.st_mode & libc::S_IFMT;
        Ok(Some(if mode == libc::S_IFREG {
            Kind::File
        } else if mode == libc::S_IFDIR {
            Kind::Dir
        } else {
            Kind::Other
        }))
    }

    /// Every entry but `.` and `..`, with what each is, read from this
    /// descriptor: a name swapped for a link is listed as [`Kind::Other`].
    pub fn entries(&self) -> io::Result<Vec<(OsString, Kind)>> {
        // fdopendir owns a duplicate, so this descriptor stays open.
        // SAFETY: fcntl on a descriptor this value owns.
        let fd = unsafe { libc::fcntl(self.0.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a new descriptor fdopendir takes ownership of.
        let stream = unsafe { libc::fdopendir(fd) };
        if stream.is_null() {
            let error = io::Error::last_os_error();
            // SAFETY: fdopendir failed, so the duplicate is still this one's.
            unsafe { libc::close(fd) };
            return Err(error);
        }
        struct Stream(*mut libc::DIR);
        impl Drop for Stream {
            fn drop(&mut self) {
                // SAFETY: a stream fdopendir returned, closed once.
                unsafe { libc::closedir(self.0) };
            }
        }
        let stream = Stream(stream);
        // SAFETY: rewind a stream this function owns.
        unsafe { libc::rewinddir(stream.0) };
        let mut names = Vec::new();
        loop {
            // SAFETY: a live stream; the entry is copied out at once.
            let entry = unsafe { libc::readdir(stream.0) };
            if entry.is_null() {
                break;
            }
            // SAFETY: readdir returned a valid entry with a NUL-terminated name.
            let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if name != b"." && name != b".." {
                names.push(OsStr::from_bytes(name).to_owned());
            }
        }
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            if let Some(kind) = self.kind(&name)? {
                out.push((name, kind));
            }
        }
        Ok(out)
    }

    /// Replace `name` with `bytes`: a new file beside it, synced, renamed over
    /// it (a link there is replaced, never followed), then the directory
    /// synced. A failure leaves `name` as it was.
    pub fn write_atomic(&self, name: &OsStr, bytes: &[u8]) -> io::Result<()> {
        let target = c_name(name)?;
        let temp_name = OsString::from(format!(".memory-{}.tmp", uuid::Uuid::now_v7()));
        let temp = c_name(&temp_name)?;
        // SAFETY: a directory descriptor and a NUL-terminated name.
        let mut out = check(unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                temp.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o666 as libc::c_uint,
            )
        })?;
        let result = out
            .write_all(bytes)
            .and_then(|()| out.sync_all())
            .and_then(|()| {
                // SAFETY: one directory descriptor and two NUL-terminated names.
                if unsafe {
                    libc::renameat(
                        self.0.as_raw_fd(),
                        temp.as_ptr(),
                        self.0.as_raw_fd(),
                        target.as_ptr(),
                    )
                } != 0
                {
                    return Err(io::Error::last_os_error());
                }
                self.0.sync_all()
            });
        if result.is_err() {
            // SAFETY: a directory descriptor and a NUL-terminated name.
            unsafe { libc::unlinkat(self.0.as_raw_fd(), temp.as_ptr(), 0) };
        }
        result
    }

    /// Remove `name` when it is a regular file; `false` when it is absent or
    /// anything else, which is left alone.
    pub fn remove_file(&self, name: &OsStr) -> io::Result<bool> {
        if self.kind(name)? != Some(Kind::File) {
            return Ok(false);
        }
        let c = c_name(name)?;
        // SAFETY: a directory descriptor and a NUL-terminated name. unlinkat
        // removes the entry itself, never what a link names.
        if unsafe { libc::unlinkat(self.0.as_raw_fd(), c.as_ptr(), 0) } != 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error)
            };
        }
        Ok(true)
    }

    /// This directory's own metadata.
    pub fn metadata(&self) -> io::Result<Metadata> {
        self.0.metadata()
    }

    /// Change this directory's own mode.
    pub fn set_mode(&self, mode: u32) -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        self.0
            .set_permissions(std::fs::Permissions::from_mode(mode))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn nothing_below_the_root_is_reached_through_a_link() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.join("dir")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(root.join("dir/file"), b"inside").unwrap();
        std::fs::write(outside.join("secret"), b"secret").unwrap();
        symlink(outside.join("secret"), root.join("dir/file-link")).unwrap();
        symlink(&outside, root.join("dir-link")).unwrap();
        let dir = Dir::open(&root).unwrap();
        assert_eq!(
            dir.dir(Path::new("dir"))
                .unwrap()
                .read(OsStr::new("file"))
                .unwrap(),
            b"inside"
        );
        let inner = dir.dir(Path::new("dir")).unwrap();
        assert_eq!(
            inner.read(OsStr::new("file-link")).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(dir.dir(Path::new("dir-link")).is_err());
        assert!(dir.create_dir(OsStr::new("dir-link"), 0o700).is_err());
        assert!(dir.dir(Path::new("../outside")).is_err());
        for bad in ["", ".", "..", "a/b"] {
            assert!(inner.read(OsStr::new(bad)).is_err(), "{bad}");
        }
        let mut entries = inner.entries().unwrap();
        entries.sort();
        assert_eq!(
            entries,
            [
                (OsString::from("file"), Kind::File),
                (OsString::from("file-link"), Kind::Other),
            ]
        );
        // Writing over a link replaces the link; removing one leaves it.
        assert!(!inner.remove_file(OsStr::new("file-link")).unwrap());
        inner.write_atomic(OsStr::new("file-link"), b"new").unwrap();
        assert_eq!(std::fs::read(outside.join("secret")).unwrap(), b"secret");
        assert_eq!(inner.read(OsStr::new("file-link")).unwrap(), b"new");
        assert!(inner.remove_file(OsStr::new("file")).unwrap());
        assert!(!root.join("dir/file").exists());
        assert!(Dir::open_no_follow(&root.join("dir-link")).is_err());
    }
}
