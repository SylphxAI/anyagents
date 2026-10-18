//! Secrets live in a file, never in the database.
//!
//! A [`Secret`] has no `Debug` and no `Display`, so a log of it prints nothing
//! of the value. The file store writes mode 0600 and refuses to read a file
//! that the group or anyone else can read.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use crate::error::Error;

/// A secret value. It cannot be printed or debugged.
pub struct Secret(Vec<u8>);

impl Secret {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_str(&self) -> Result<&str, Error> {
        std::str::from_utf8(&self.0).map_err(|_| Error::Message("secret is not text".into()))
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Reads one secret by the reference a record holds.
pub trait CredentialStore {
    fn read(&self, credential_ref: &str) -> Result<Secret, Error>;
    fn write(&self, credential_ref: &str, value: &[u8]) -> Result<(), Error>;
}

/// Files under `<root>/secrets/<ref>`. A ref may not climb out of that directory.
pub struct FileCredentialStore {
    root: PathBuf,
}

impl FileCredentialStore {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            root: data_dir.join("secrets"),
        }
    }

    fn path(&self, credential_ref: &str) -> Result<PathBuf, Error> {
        if credential_ref.is_empty()
            || credential_ref
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(Error::Message("credential ref is not a plain path".into()));
        }
        Ok(self.root.join(credential_ref))
    }
}

impl CredentialStore for FileCredentialStore {
    fn read(&self, credential_ref: &str) -> Result<Secret, Error> {
        let path = self.path(credential_ref)?;
        let meta = fs::metadata(&path)?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(Error::Message(
                "credential file is readable by the group or others".into(),
            ));
        }
        Ok(Secret(fs::read(path)?))
    }

    fn write(&self, credential_ref: &str, value: &[u8]) -> Result<(), Error> {
        let path = self.path(credential_ref)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(value)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_is_written_0600_and_refused_when_wider() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileCredentialStore::new(dir.path());
        store.write("channels/one", b"tok-1").unwrap();
        let secret = store.read("channels/one").unwrap();
        assert_eq!(secret.as_str().unwrap(), "tok-1");
        let path = dir.path().join("secrets/channels/one");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(store.read("channels/one").is_err());
    }

    #[test]
    fn a_ref_that_climbs_out_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileCredentialStore::new(dir.path());
        assert!(store.write("../outside", b"no").is_err());
        assert!(store.read("a/../../b").is_err());
    }
}
