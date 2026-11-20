//! Secrets live in mode `0600` files locally and, in PostgreSQL mode, also
//! write through to `deployment_blobs(namespace = "secrets")`.
//!
//! A [`Secret`] has no `Debug` and no `Display`, so a log of it prints nothing
//! of the value. The file store writes mode 0600 and refuses to read a file
//! that the group or anyone else can read.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::error::Error;

/// A secret value. It cannot be printed or debugged.
pub struct Secret(Vec<u8>);

impl Secret {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

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
        anyagents_harness::atomic_write_nonempty_file(&path, value, 0o600)?;
        Ok(())
    }
}

/// Store-backed [`CredentialStore`] that keeps a local `0600` cache under
/// `<data_dir>/secrets/<ref>` and, when backed by PostgreSQL, durably writes
/// through to and reads fallback secrets from `deployment_blobs(namespace = "secrets")`.
pub struct StoreCredentialStore<'a> {
    store: &'a crate::Store,
    local: FileCredentialStore,
}

impl<'a> StoreCredentialStore<'a> {
    pub fn new(store: &'a crate::Store) -> Self {
        Self::with_data_dir(store, store.data_dir())
    }

    pub fn with_data_dir(store: &'a crate::Store, data_dir: &Path) -> Self {
        Self {
            store,
            local: FileCredentialStore::new(data_dir),
        }
    }
}

fn read_with_blob_fallback(
    local: &FileCredentialStore,
    load_blobs: impl FnOnce() -> Result<Option<Box<dyn crate::StateBlobStore>>, Error>,
    credential_ref: &str,
) -> Result<Secret, Error> {
    match local.read(credential_ref) {
        Ok(secret) => Ok(secret),
        Err(Error::Io(io_err)) if io_err.kind() == std::io::ErrorKind::NotFound => {
            if let Some(blobs) = load_blobs()?
                && let Some(blob) = blobs.get_deployment_blob("secrets", credential_ref)?
            {
                if !blob.content.is_empty() {
                    let _ = local.write(credential_ref, &blob.content);
                }
                return Ok(Secret::from_bytes(blob.content));
            }
            Err(Error::Io(io_err))
        }
        Err(err) => Err(err),
    }
}

fn write_through_to_blobs(
    local: &FileCredentialStore,
    blobs: Option<(&dyn crate::StateBlobStore, bool)>,
    credential_ref: &str,
    value: &[u8],
) -> Result<(), Error> {
    local.write(credential_ref, value)?;
    if let Some((blobs, encrypted)) = blobs {
        blobs.put_deployment_blob("secrets", credential_ref, value, encrypted)?;
    }
    Ok(())
}

impl CredentialStore for StoreCredentialStore<'_> {
    fn read(&self, credential_ref: &str) -> Result<Secret, Error> {
        read_with_blob_fallback(
            &self.local,
            || {
                if !self.store.is_postgres() {
                    return Ok(None);
                }
                let master_key = crate::MasterKey::from_env()?;
                Ok(Some(self.store.blob_store_with_key(master_key)))
            },
            credential_ref,
        )
    }

    fn write(&self, credential_ref: &str, value: &[u8]) -> Result<(), Error> {
        let _ = self.local.path(credential_ref)?;
        if self.store.is_postgres() {
            let master_key = crate::MasterKey::from_env()?;
            let encrypted = master_key.is_some();
            let blobs = self.store.blob_store_with_key(master_key);
            write_through_to_blobs(
                &self.local,
                Some((blobs.as_ref(), encrypted)),
                credential_ref,
                value,
            )
        } else {
            write_through_to_blobs(&self.local, None, credential_ref, value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FsStateBlobStore, StateBlobStore as _, Store};

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

    #[test]
    fn store_credential_store_matches_file_store_in_sqlite_mode() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let creds = StoreCredentialStore::new(&store);
        creds.write("channels/bot-1", b"111:secret-tok").unwrap();
        let loaded = creds.read("channels/bot-1").unwrap();
        assert_eq!(loaded.as_str().unwrap(), "111:secret-tok");

        let path = dir.path().join("secrets/channels/bot-1");
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(creds.read("channels/bot-1").is_err());
        assert!(creds.write("../escape", b"nope").is_err());
        assert!(creds.read("../escape").is_err());

        let missing = creds.read("channels/missing");
        assert!(matches!(
            missing,
            Err(Error::Io(ref err)) if err.kind() == std::io::ErrorKind::NotFound
        ));
    }

    #[test]
    fn blob_write_through_and_ephemeral_pod_fallback_cache_0600() {
        let pod_a = crate::test_tempdir();
        let pod_b = crate::test_tempdir();
        let remote_blobs_dir = crate::test_tempdir();

        let local_a = FileCredentialStore::new(pod_a.path());
        let remote = FsStateBlobStore::new(remote_blobs_dir.path());

        // Pod A writes through to local 0600 disk and durable deployment_blobs(secrets).
        write_through_to_blobs(
            &local_a,
            Some((&remote, true)),
            "channels/chn_01abc",
            b"777:durable-bot-token",
        )
        .unwrap();
        assert!(pod_a.path().join("secrets/channels/chn_01abc").is_file());
        let stored_blob = remote
            .get_deployment_blob("secrets", "channels/chn_01abc")
            .unwrap()
            .expect("persisted to deployment_blobs(secrets)");
        assert_eq!(stored_blob.content, b"777:durable-bot-token");

        // Pod B starts with an empty ephemeral disk, falls back to deployment_blobs,
        // and warms its local 0600 cache.
        let local_b = FileCredentialStore::new(pod_b.path());
        let remote_root = remote_blobs_dir.path().to_path_buf();
        let open_remote = || -> Result<Option<Box<dyn crate::StateBlobStore>>, Error> {
            let store: Box<dyn crate::StateBlobStore> =
                Box::new(FsStateBlobStore::new(&remote_root));
            Ok(Some(store))
        };
        let loaded = read_with_blob_fallback(&local_b, open_remote, "channels/chn_01abc").unwrap();
        assert_eq!(loaded.as_str().unwrap(), "777:durable-bot-token");

        let cached_path = pod_b.path().join("secrets/channels/chn_01abc");
        assert!(cached_path.is_file());
        let mode = fs::metadata(&cached_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        // Subsequent read hits local cache without consulting durable blobs.
        let cached = read_with_blob_fallback(
            &local_b,
            || panic!("cache hit should not query durable blob store"),
            "channels/chn_01abc",
        )
        .unwrap();
        assert_eq!(cached.as_str().unwrap(), "777:durable-bot-token");

        // Missing credential in both local disk and durable blobs returns NotFound.
        let missing = read_with_blob_fallback(&local_b, open_remote, "channels/does-not-exist");
        assert!(matches!(
            missing,
            Err(Error::Io(ref err)) if err.kind() == std::io::ErrorKind::NotFound
        ));

        // Wider local permissions still fail closed and never silently fall back.
        fs::set_permissions(&cached_path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_with_blob_fallback(&local_b, open_remote, "channels/chn_01abc").is_err());
    }
}
