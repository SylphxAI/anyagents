//! The only way server code touches the trees an agent's seat can write.
//!
//! `<data>/agents/<id>/{config,memory,skills,sessions,work}` are group-owned
//! by the agent's seat, so the seat can put a link, a FIFO or a forged file
//! anywhere below them; the agent root itself is the server's. The server
//! runs as its own user, so a path-based read, write, rename or truncate it
//! makes there can be steered anywhere that user reaches (ADR 0010 D3).
//! [`AgentTree`] holds the opened agent root and reaches everything below it
//! through [`Dir`]: every component opened with `O_NOFOLLOW`, files read
//! with a size limit, written by an `O_EXCL` temporary renamed into place.
//!
//! `crates/anyagents/tests/agent_tree_fs.rs` keeps raw `std::fs` out of the
//! modules that work on these trees.

use std::ffi::OsStr;
use std::io;
use std::path::{Component, Path, PathBuf};

use crate::beneath::Dir;

/// The default cap on one file read from an agent tree.
pub const READ_LIMIT: u64 = 4 * 1024 * 1024;

/// One agent's root directory, opened.
#[derive(Debug)]
pub struct AgentTree {
    root: Dir,
    path: PathBuf,
}

/// `relative` as its directory part and final name; refuses an empty path,
/// `..`, and an absolute path.
fn split(relative: &Path) -> io::Result<(&Path, &OsStr)> {
    if relative.is_absolute()
        || relative
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "an agent tree path is relative and stays below the root",
        ));
    }
    let name = relative.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "an agent tree path names a file",
        )
    })?;
    Ok((relative.parent().unwrap_or(Path::new("")), name))
}

impl AgentTree {
    /// `<data>/agents/<id>`.
    pub fn of(data_dir: &Path, id: &crate::AgentId) -> io::Result<Self> {
        Self::open(&data_dir.join("agents").join(id.as_str()))
    }

    /// The agent root at `path`, which the server chose: opened as named.
    /// Everything below it is reached without following a link.
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            root: Dir::open(path)?,
            path: path.to_path_buf(),
        })
    }

    /// The root's path, for messages and for handing to a harness.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The root itself.
    pub fn root(&self) -> &Dir {
        &self.root
    }

    /// The directory `relative` below the root, every component a plain
    /// directory.
    pub fn dir(&self, relative: &Path) -> io::Result<Dir> {
        self.root.dir(relative)
    }

    /// The directory `relative`, created with `mode` where missing.
    pub fn create_dir_all(&self, relative: &Path, mode: libc::mode_t) -> io::Result<Dir> {
        self.root.create_dir_all(relative, mode)
    }

    /// The regular file `relative`, at most `limit` bytes.
    pub fn read(&self, relative: &Path, limit: u64) -> io::Result<Vec<u8>> {
        let (parent, name) = split(relative)?;
        self.root.dir(parent)?.read(name, limit)
    }

    /// [`Self::read`], with an absent file (or directory above it) as `None`.
    pub fn read_optional(&self, relative: &Path, limit: u64) -> io::Result<Option<Vec<u8>>> {
        match self.read(relative, limit) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Replace the file `relative` with `bytes` at exactly `mode`, creating
    /// the directories above it with `dir_mode`.
    pub fn write(
        &self,
        relative: &Path,
        bytes: &[u8],
        mode: u32,
        dir_mode: libc::mode_t,
    ) -> io::Result<()> {
        let (parent, name) = split(relative)?;
        self.root
            .create_dir_all(parent, dir_mode)?
            .write_atomic_mode(name, bytes, mode)
    }

    /// Remove the regular file `relative`; a link or anything else is left.
    pub fn remove_file(&self, relative: &Path) -> io::Result<bool> {
        let (parent, name) = split(relative)?;
        match self.root.dir(parent) {
            Ok(dir) => dir.remove_file(name),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn a_tree_reaches_nothing_outside_its_root() {
        let temp = crate::test_tempdir();
        let root = temp.path().join("agent");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(root.join("config")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret"), b"secret").unwrap();
        symlink(outside.join("secret"), root.join("config/linked")).unwrap();
        symlink(&outside, root.join("config/dir-link")).unwrap();
        let tree = AgentTree::open(&root).unwrap();
        tree.write(Path::new("config/a/b.json"), b"{}", 0o640, 0o770)
            .unwrap();
        assert_eq!(tree.read(Path::new("config/a/b.json"), 16).unwrap(), b"{}");
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            std::fs::metadata(root.join("config/a/b.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
        for refused in [
            "config/linked",
            "config/dir-link/secret",
            "../outside/secret",
            "/etc/hostname",
        ] {
            assert!(tree.read(Path::new(refused), 64).is_err(), "{refused}");
        }
        assert!(
            tree.write(Path::new("config/dir-link/planted"), b"x", 0o600, 0o700)
                .is_err()
        );
        assert!(!tree.remove_file(Path::new("config/linked")).unwrap());
        assert_eq!(
            tree.read_optional(Path::new("config/absent"), 64).unwrap(),
            None
        );
        assert_eq!(std::fs::read(outside.join("secret")).unwrap(), b"secret");
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
    }
}
