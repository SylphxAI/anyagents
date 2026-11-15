//! Crash- and `ENOSPC`-safe atomic file persistence.
//!
//! Never opens `dst` with `O_TRUNC`: writes a sibling temporary file in `dst`'s
//! parent directory via `create_new`, sets exact permissions, flushes and
//! `sync_all()`s the temporary file, verifies complete read-back (and non-empty
//! payload when required), atomically replaces `dst` via `std::fs::rename`, and
//! syncs the parent directory entry. On any error before `rename`, unlinks the
//! temporary file and leaves any pre-existing `dst` byte-for-byte intact.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Atomically persist `bytes` to `dst` with Unix mode `mode`, allowing empty payloads.
pub fn atomic_write_file(dst: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
    atomic_write_with(dst, bytes, mode, false, |file, data| {
        file.write_all(data)?;
        file.sync_all()
    })
}

/// Atomically persist non-empty `bytes` to `dst` with Unix mode `mode`.
///
/// Fails closed with `ErrorKind::InvalidInput` if `bytes` is empty so auth tokens,
/// credentials, profile metadata, and state JSON files can never be truncated to `0` bytes.
pub fn atomic_write_nonempty_file(dst: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
    atomic_write_with(dst, bytes, mode, true, |file, data| {
        file.write_all(data)?;
        file.sync_all()
    })
}

/// Atomically persist non-empty `bytes` to `dst` inside an existing parent directory.
///
/// Does NOT call `create_dir_all` on `dst.parent()`, so writing state holds or
/// markers into a retired/offline `state_dir` fails closed with `NotFound`.
pub fn atomic_write_in_existing_dir(dst: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
    atomic_write_inner(dst, bytes, mode, true, false, |file, data| {
        file.write_all(data)?;
        file.sync_all()
    })
}

/// Core atomic writer with an injectable `write_fn` so tests can deterministically
/// inject mid-write `ENOSPC` / partial-write failures after temporary inode creation.
pub fn atomic_write_with<F>(
    dst: &Path,
    bytes: &[u8],
    mode: u32,
    require_nonempty: bool,
    write_fn: F,
) -> io::Result<()>
where
    F: FnOnce(&mut File, &[u8]) -> io::Result<()>,
{
    atomic_write_inner(dst, bytes, mode, require_nonempty, true, write_fn)
}

fn atomic_write_inner<F>(
    dst: &Path,
    bytes: &[u8],
    mode: u32,
    require_nonempty: bool,
    create_parents: bool,
    write_fn: F,
) -> io::Result<()>
where
    F: FnOnce(&mut File, &[u8]) -> io::Result<()>,
{
    if require_nonempty && bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to persist empty file: {}", dst.display()),
        ));
    }
    let Some(file_name) = dst.file_name().filter(|n| !n.is_empty()) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid target file path: {}", dst.display()),
        ));
    };
    let parent = match dst.parent() {
        Some(p) if !p.as_os_str().is_empty() => {
            if create_parents {
                fs::create_dir_all(p)?;
            }
            p
        }
        _ => Path::new("."),
    };
    let seq = NEXT_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(
        ".{}.{}.{}.anyagents-tmp",
        file_name.to_string_lossy(),
        std::process::id(),
        seq
    ));

    let result = (|| -> io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(mode);
        }
        let mut file = options.open(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(fs::Permissions::from_mode(mode))?;
        }
        write_fn(&mut file, bytes)?;
        file.sync_all()?;
        drop(file);

        let written = fs::read(&tmp)?;
        if (require_nonempty && written.is_empty()) || written != bytes {
            return Err(io::Error::other(format!(
                "atomic write verification failed before commit: {}",
                dst.display()
            )));
        }

        // The synced temporary file was verified above; `rename` commits it
        // whole. A concurrent writer's later rename may replace it at once,
        // so `dst` is not read back: its bytes may rightly be another commit.
        fs::rename(&tmp, dst)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enospc_and_partial_write_preserve_existing_credential_file_and_clean_temp() {
        let dir = tempfile::tempdir().unwrap();
        let tok_path = dir
            .path()
            .join(".gemini/antigravity-cli/antigravity-oauth-token");
        let original = br#"{"token":{"access_token":"ya29.initial","refresh_token":"1//reusable-refresh"},"email":"user@example.com"}"#;
        atomic_write_nonempty_file(&tok_path, original, 0o600).unwrap();
        assert_eq!(fs::read(&tok_path).unwrap(), original);

        let updated = br#"{"token":{"access_token":"ya29.updated","refresh_token":"1//reusable-refresh"},"email":"user@example.com"}"#;

        // 1. Mid-write ENOSPC after partial write into the sibling temp file.
        let err = atomic_write_with(&tok_path, updated, 0o600, true, |file, data| {
            file.write_all(&data[..24])?;
            Err(io::Error::from_raw_os_error(28)) // ENOSPC
        })
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(28));
        assert_eq!(fs::read(&tok_path).unwrap(), original);

        // Ensure zero orphan temp files remain in the directory.
        let entries: Vec<_> = fs::read_dir(tok_path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["antigravity-oauth-token"]);

        // 2. Partial write without OS error still fails closed on read-back verification.
        let err = atomic_write_with(&tok_path, updated, 0o600, true, |file, data| {
            file.write_all(&data[..16])
        })
        .unwrap_err();
        assert!(err.to_string().contains("verification failed"));
        assert_eq!(fs::read(&tok_path).unwrap(), original);

        // 3. Empty payload is refused before touching disk.
        let err = atomic_write_nonempty_file(&tok_path, b"", 0o600).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(fs::read(&tok_path).unwrap(), original);
    }

    /// Profile markers are rewritten by concurrent writers (switch, wake,
    /// rotation, the periodic seat pass) while readers parse them. A reader
    /// sees one writer's whole value, never an empty or partial file, and no
    /// temporary file is left behind.
    #[test]
    fn concurrent_marker_writers_never_expose_empty_or_partial_content() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".anyagents-profile-rotated-ms");
        atomic_write_in_existing_dir(&marker, b"1000000000000\n", 0o664).unwrap();
        let values: Vec<Vec<u8>> = (0..8)
            .map(|w| format!("{}\n", 1_800_000_000_000_u64 + w).into_bytes())
            .collect();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        std::thread::scope(|scope| {
            let mut readers = Vec::new();
            for _ in 0..4 {
                let (marker, stop, values) = (&marker, stop.clone(), &values);
                readers.push(scope.spawn(move || {
                    let mut reads = 0_u64;
                    // One more read after the writers finish, so every reader
                    // also checks the final committed value.
                    loop {
                        let done = stop.load(std::sync::atomic::Ordering::Acquire);
                        let seen = fs::read(marker).unwrap();
                        assert!(
                            seen == b"1000000000000\n" || values.contains(&seen),
                            "reader saw {seen:?}"
                        );
                        reads += 1;
                        if done {
                            return reads;
                        }
                    }
                }));
            }
            let writers: Vec<_> = values
                .iter()
                .map(|value| {
                    let marker = &marker;
                    scope.spawn(move || {
                        for _ in 0..200 {
                            atomic_write_in_existing_dir(marker, value, 0o664).unwrap();
                        }
                    })
                })
                .collect();
            for writer in writers {
                writer.join().unwrap();
            }
            stop.store(true, std::sync::atomic::Ordering::Release);
            for reader in readers {
                assert!(reader.join().unwrap() > 0);
            }
        });
        assert!(values.contains(&fs::read(&marker).unwrap()));
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        assert_eq!(
            atomic_write_in_existing_dir(&marker, b"", 0o664)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    #[cfg(unix)]
    fn unwritable_parent_directory_preserves_writable_target_file() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("locked");
        fs::create_dir_all(&parent).unwrap();
        let cred = parent.join("auth.json");
        let original = br#"{"refresh_token":"keep-intact"}"#;
        atomic_write_nonempty_file(&cred, original, 0o600).unwrap();

        // Even though `cred` itself is 0o600 (so `std::fs::write` with `O_TRUNC` would truncate it!),
        // `atomic_write_nonempty_file` creates a sibling temp file in `parent` and fails closed.
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();
        let unprivileged = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(parent.join(".root-probe"))
            .is_err();
        let res = atomic_write_nonempty_file(&cred, br#"{"refresh_token":"new"}"#, 0o600);
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let _ = fs::remove_file(parent.join(".root-probe"));
        if unprivileged {
            assert!(res.is_err());
            assert_eq!(fs::read(&cred).unwrap(), original);
        }
    }
}
