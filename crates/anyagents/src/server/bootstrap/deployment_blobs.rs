//! Materialize PostgreSQL-backed deployment state before opening serving routes.

use std::collections::{BTreeSet, HashSet};
use std::fs::{self, DirBuilder, File, Permissions};
use std::io;
use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
use std::path::Path;

use anyagents_core::stateless::{BlobEntry, StateBlobStore, validate_blob_path};
use anyagents_core::{Error, MasterKey, Store};

// Disk I/O bound: a worker waiting on fsync doesn't consume a CPU. Bound open
// files, stacks and temporary read-back buffers independently of fleet size.
const MAX_WRITERS: usize = 32;

pub(super) fn hydrate_on_start(store: &Store, data: &Path) -> Result<usize, Error> {
    let blobs = store.blob_store_with_key(MasterKey::from_env()?);
    let hydrated = hydrate(blobs.as_ref(), data)?;
    if hydrated > 0 {
        eprintln!("anyagents-blobs: hydrated {hydrated} deployment blob(s) from postgres");
    }
    Ok(hydrated)
}

fn hydrate(blobs: &dyn StateBlobStore, data: &Path) -> Result<usize, Error> {
    let mut hydrated = 0;
    create_secret_dir(&data.join("secrets"))?;
    // Preserve namespace precedence (e.g. a profile wins over a config blob
    // whose path starts with `profiles/`). Keep one namespace's payloads in RAM.
    for ns in ["config", "secrets", "profiles", "skills", "roles"] {
        let root = if ns == "config" {
            data.to_owned()
        } else {
            data.join(ns)
        };
        let entries = blobs.get_deployment_blobs_all(ns)?;
        hydrated += materialize(data, &root, &entries, ns == "secrets")?;
    }
    Ok(hydrated)
}

fn create_secret_dir(path: &Path) -> io::Result<()> {
    DirBuilder::new().recursive(true).mode(0o700).create(path)?;
    fs::set_permissions(path, Permissions::from_mode(0o700))
}

fn materialize(
    data: &Path,
    root: &Path,
    entries: &[BlobEntry],
    secret: bool,
) -> Result<usize, Error> {
    let mut dirs = BTreeSet::new();
    let mut seen = HashSet::new();
    let mut duplicate = false;
    let mut files = Vec::new();
    for entry in entries {
        let Ok(path) = validate_blob_path(&entry.path) else {
            continue;
        };
        let dest = root.join(path);
        for dir in dest.ancestors().skip(1) {
            dirs.insert(dir.to_owned());
            if dir == data {
                break;
            }
        }
        duplicate |= !seen.insert(dest.clone());
        let mode = if secret || entry.encrypted {
            0o600
        } else {
            0o644
        };
        files.push((dest, entry.content.as_slice(), mode));
    }
    // A path sorts before its descendants; make each directory once instead
    // of creating/chmod'ing the same hierarchy on every file write.
    for dir in &dirs {
        if secret && dir.starts_with(root) {
            create_secret_dir(dir)?;
        } else {
            fs::create_dir_all(dir)?;
        }
    }
    let write = |(dest, bytes, mode): &(std::path::PathBuf, &[u8], u32)| -> Result<(), Error> {
        anyagents_harness::atomic_write_file_defer_parent_sync(dest, bytes, *mode)?;
        Ok(())
    };
    let write_all = |batch: &[_]| batch.iter().try_for_each(write);
    // Legacy rows may normalize to the same path; keep their order so the
    // final rename matches the serial hydration result.
    let result = if files.len() <= 1 || duplicate {
        write_all(&files)
    } else {
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            let mut result = Ok(());
            for chunk in files.chunks(files.len().div_ceil(MAX_WRITERS)) {
                match std::thread::Builder::new().spawn_scoped(scope, || write_all(chunk)) {
                    Ok(worker) => workers.push(worker),
                    Err(err) => {
                        result = Err(err.into());
                        break;
                    }
                }
            }
            for worker in workers {
                let completed = worker.join().unwrap_or_else(|_| {
                    Err(Error::Message("deployment blob writer panicked".into()))
                });
                result = result.and(completed);
            }
            result
        })
    };
    // Flush once per touched directory, including newly created ancestors,
    // deepest first. Always flush successful siblings before returning a
    // write error. If interrupted before this barrier the durable PG copy
    // can replay the batch; any visible file was individually synced first.
    let mut durable = Ok(());
    for dir in dirs.iter().rev() {
        durable = durable.and(File::open(dir).and_then(|file| file.sync_all()));
    }
    result?;
    durable?;
    Ok(files.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_blob_hydration_preserves_existing_file_on_rename_error() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::create_dir(root.join("not-a-file")).unwrap();
        fs::write(root.join("not-a-file/retained"), b"keep").unwrap();
        let entries = [BlobEntry {
            namespace: "secrets".into(),
            path: "not-a-file".into(),
            content: vec![],
            sha256: String::new(),
            encrypted: true,
            version: 1,
            updated_at_ms: 0,
        }];
        assert!(materialize(root, root, &entries, true).is_err());
        assert_eq!(fs::read(root.join("not-a-file/retained")).unwrap(), b"keep");
        assert_eq!(fs::read_dir(root).unwrap().count(), 1);
    }

    #[test]
    fn deployment_blob_hydration_round_trips_664_postgres_files() {
        let Ok(url) = std::env::var(anyagents_core::db::TEST_POSTGRES_URL_ENV) else {
            eprintln!("skipping PostgreSQL hydration regression: test URL is unset");
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open_postgres(&url, dir.path()).unwrap();
        let key = MasterKey::from_bytes(b"disposable hydration test key").unwrap();
        let blobs = store.blob_store_with_key(Some(key));
        let tag = anyagents_core::AgentId::generate();
        let mut fixtures = Vec::new();
        for i in 0..664 {
            let ns = ["config", "secrets", "profiles", "skills", "roles"][i % 5];
            let path = format!("{tag}/{}/file-{i}", i % 9);
            let content = if i % 7 == 0 {
                vec![]
            } else {
                [0, 255, (i % 256) as u8].repeat(1024)
            };
            let encrypted = ns == "secrets" || i % 11 == 0;
            let dest = if ns == "config" {
                dir.path().join(&path)
            } else {
                dir.path().join(ns).join(&path)
            };
            blobs
                .put_deployment_blob(ns, &path, &content, encrypted)
                .unwrap();
            fixtures.push((ns, path, dest, content, encrypted));
        }
        // Include replacement of a nonempty file by an authoritative empty blob.
        let first = &fixtures[0].2;
        fs::create_dir_all(first.parent().unwrap()).unwrap();
        fs::write(first, b"superseded").unwrap();

        let started = std::time::Instant::now();
        let hydrated = hydrate(blobs.as_ref(), dir.path());
        let elapsed = started.elapsed();
        for (ns, path, ..) in &fixtures {
            blobs.delete_deployment_blob(ns, path).unwrap();
        }
        assert!(hydrated.unwrap() >= fixtures.len());
        eprintln!("664-file PostgreSQL read + atomic materialization: {elapsed:?}");
        for (ns, _, dest, content, encrypted) in fixtures {
            assert_eq!(fs::read(&dest).unwrap(), content);
            assert_eq!(
                fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
                if encrypted { 0o600 } else { 0o644 }
            );
            if ns == "secrets" {
                assert_eq!(
                    fs::metadata(dest.parent().unwrap())
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o700
                );
            }
        }
    }
}
