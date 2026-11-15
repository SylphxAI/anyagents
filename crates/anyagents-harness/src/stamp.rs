//! A file's identity and version as metadata alone shows them, so a reader
//! can keep what it derived from a file until the file changes.

use std::path::Path;
use std::time::SystemTime;

/// Inode, size and modification time. Equal stamps mean the file was neither
/// replaced nor rewritten nor appended to, as far as the file system records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileStamp {
    ino: u64,
    len: u64,
    modified: Option<SystemTime>,
}

impl FileStamp {
    /// The stamp of `path`, following symlinks; `None` when it is missing or
    /// unreadable.
    pub fn of(path: &Path) -> Option<Self> {
        std::fs::metadata(path)
            .ok()
            .map(|meta| Self::from_metadata(&meta))
    }

    pub fn from_metadata(meta: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        let ino = std::os::unix::fs::MetadataExt::ino(meta);
        #[cfg(not(unix))]
        let ino = 0;
        Self {
            ino,
            len: meta.len(),
            modified: meta.modified().ok(),
        }
    }

    pub fn modified(&self) -> Option<SystemTime> {
        self.modified
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stamp_changes_with_an_append_or_a_replacement_and_not_otherwise() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        assert_eq!(FileStamp::of(&path), None);
        std::fs::write(&path, b"one\n").unwrap();
        let first = FileStamp::of(&path).unwrap();
        assert_eq!(FileStamp::of(&path), Some(first));
        use std::io::Write as _;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"two\n")
            .unwrap();
        let appended = FileStamp::of(&path).unwrap();
        assert_ne!(appended, first);
        let other = dir.path().join("other");
        std::fs::write(&other, b"one\ntwo\n").unwrap();
        std::fs::rename(&other, &path).unwrap();
        assert_ne!(FileStamp::of(&path).unwrap(), appended);
    }
}
