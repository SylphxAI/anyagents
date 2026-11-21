//! Own one clone transaction per guest state directory. The private journal, not
//! a directory-name prefix in the workspace, authorizes recovery and cleanup.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub(super) struct Slot {
    journal: PathBuf,
    lock: File,
}

impl Drop for Slot {
    fn drop(&mut self) {
        // A concurrently forked child can transiently hold the file until exec.
        // Release our advisory lock explicitly rather than waiting for its last fd.
        let _ = self.lock.unlock();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Identity {
    // The device number can change when a persistent volume is remounted.
    inode: u64,
}

impl From<&fs::Metadata> for Identity {
    fn from(meta: &fs::Metadata) -> Self {
        Self { inode: meta.ino() }
    }
}

#[derive(Deserialize, Serialize)]
pub(super) struct Attempt {
    workspace: Vec<u8>,
    id: [u8; 16],
    in_place: bool,
    directory: Option<Identity>,
    // A mount root cannot be replaced. Record the identity of each top-level
    // entry before moving any of them. The final .git rename is the commit point.
    install: Option<Vec<(Vec<u8>, Identity)>>,
}

impl Slot {
    pub(super) fn lock(state: &Path) -> io::Result<Self> {
        let lock = File::options()
            .create(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(state.join("clone.lock"))?;
        if !lock.metadata()?.is_file() {
            return Err(io::Error::other("clone lock is not a regular file"));
        }
        // Keep the lock inode stable, including after a successful clone.
        lock.try_lock()?;
        Ok(Self {
            journal: state.join("clone-attempt.json"),
            lock,
        })
    }

    fn save(&self, attempt: &Attempt) -> io::Result<()> {
        let bytes = serde_json::to_vec(attempt)?;
        anyagents_harness::atomic_write_in_existing_dir(&self.journal, &bytes, 0o600)
    }

    pub(super) fn load(&self, work: &Path) -> io::Result<Option<Attempt>> {
        match fs::read(&self.journal) {
            Ok(bytes) => {
                let attempt: Attempt = serde_json::from_slice(&bytes)?;
                if attempt.workspace != work.as_os_str().as_bytes()
                    || (!attempt.in_place && work.parent().is_none())
                {
                    return Err(io::Error::other(
                        "clone journal belongs to another workspace",
                    ));
                }
                Ok(Some(attempt))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(super) fn begin(&self, work: &Path, in_place: bool) -> io::Result<Attempt> {
        let mut attempt = Attempt {
            workspace: work.as_os_str().as_bytes().to_vec(),
            id: *uuid::Uuid::new_v4().as_bytes(),
            in_place,
            directory: None,
            install: None,
        };
        let root = attempt.root(work);
        if fs::symlink_metadata(&root).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "clone staging path exists",
            ));
        }
        self.save(&attempt)?;
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        attempt.directory = Some((&fs::symlink_metadata(root)?).into());
        self.save(&attempt)?;
        Ok(attempt)
    }

    pub(super) fn cleanup(&self, work: &Path, attempt: &Attempt) -> io::Result<()> {
        let root = attempt.root(work);
        if attempt.check_root(&root)? {
            if attempt.directory.is_some() {
                fs::remove_dir_all(&root)?;
            } else {
                // No clone can have started before its staging inode was recorded.
                fs::remove_dir(&root)?;
            }
        }
        fs::remove_file(&self.journal)
    }
}

impl Attempt {
    fn root(&self, work: &Path) -> PathBuf {
        let parent = if self.in_place {
            work
        } else {
            work.parent().expect("sibling staging needs a parent")
        };
        parent.join(format!(
            ".clone-staging-{}",
            uuid::Uuid::from_bytes(self.id)
        ))
    }

    fn check_root(&self, root: &Path) -> io::Result<bool> {
        match fs::symlink_metadata(root) {
            Ok(meta) if meta.is_dir() && self.directory.is_none_or(|id| id == (&meta).into()) => {
                Ok(true)
            }
            Ok(_) => Err(io::Error::other("clone staging directory was replaced")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(super) fn checkout(&self, work: &Path) -> PathBuf {
        self.root(work).join("checkout")
    }

    pub(super) fn is_installing(&self) -> bool {
        self.install.is_some()
    }

    pub(super) fn publish(&mut self, slot: &Slot, work: &Path) -> io::Result<()> {
        let root = self.root(work);
        let checkout = self.checkout(work);
        require_empty(work, self.in_place.then(|| root.file_name().unwrap()))?;
        if !self.in_place {
            // POSIX will only replace an empty directory, never a populated one.
            return fs::rename(checkout, work);
        }
        self.record_install(slot, &checkout)?;
        self.resume_install(work)
    }

    fn record_install(&mut self, slot: &Slot, checkout: &Path) -> io::Result<()> {
        self.install = Some(entries(checkout)?.into_iter().collect());
        slot.save(self)
    }

    pub(super) fn resume_install(&self, work: &Path) -> io::Result<()> {
        self.install_entries(work, |_| Ok(()))
    }

    fn install_entries(
        &self,
        work: &Path,
        mut after_move: impl FnMut(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let root = self.root(work);
        if !self.check_root(&root)? {
            return Err(io::Error::other(
                "clone staging directory is missing during publication",
            ));
        }
        let checkout = self.checkout(work);
        if !fs::symlink_metadata(&checkout)?.is_dir() {
            return Err(io::Error::other("staged checkout was replaced"));
        }
        let staged = entries(&checkout)?;
        let mut installed = entries(work)?;
        installed.remove(root.file_name().unwrap().as_bytes());
        let expected = self.install.as_ref().expect("publication journal missing");
        if !staged.contains_key(b".git".as_slice())
            || staged.len() + installed.len() != expected.len()
            || expected
                .iter()
                .any(|(name, id)| match (staged.get(name), installed.get(name)) {
                    (Some(actual), None) | (None, Some(actual)) => actual != id,
                    _ => true,
                })
        {
            return Err(io::Error::other(
                "workspace changed during clone publication; contents preserved",
            ));
        }
        for name in staged.keys().filter(|name| name.as_slice() != b".git") {
            let name = OsString::from_vec(name.clone());
            let target = work.join(&name);
            move_entry(&checkout.join(&name), &target)?;
            after_move(&target)?;
        }
        move_entry(&checkout.join(".git"), &work.join(".git"))
    }
}

fn entries(dir: &Path) -> io::Result<BTreeMap<Vec<u8>, Identity>> {
    fs::read_dir(dir)?
        .map(|entry| {
            let entry = entry?;
            Ok((
                entry.file_name().into_vec(),
                (&fs::symlink_metadata(entry.path())?).into(),
            ))
        })
        .collect()
}

pub(super) fn require_empty(work: &Path, except: Option<&OsStr>) -> io::Result<()> {
    for entry in fs::read_dir(work)? {
        if Some(entry?.file_name().as_os_str()) != except {
            return Err(io::Error::other(
                "workspace is not empty; contents preserved",
            ));
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn move_entry(source: &Path, target: &Path) -> io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    renameat_with(CWD, source, CWD, target, RenameFlags::NOREPLACE)?;
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn move_entry(_source: &Path, _target: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "exclusive rename unavailable",
    ))
}

pub(super) fn needs_in_place(work: &Path) -> io::Result<bool> {
    let Some(parent) = work.parent() else {
        return Ok(true);
    };
    if fs::metadata(work)?.dev() != fs::metadata(parent)?.dev() {
        return Ok(true);
    }
    // A bind mount can have the same device number as its parent. If mount-root
    // identification is unavailable on Linux, stay inside the target volume.
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{AtFlags, CWD, StatxAttributes, StatxFlags, statx};
        Ok(statx(CWD, work, AtFlags::empty(), StatxFlags::empty())
            .ok()
            .and_then(|s| {
                s.stx_attributes_mask
                    .contains(StatxAttributes::MOUNT_ROOT)
                    .then(|| s.stx_attributes.contains(StatxAttributes::MOUNT_ROOT))
            })
            .unwrap_or(true))
    }
    #[cfg(not(target_os = "linux"))]
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guest::{GuestRepoSpec, ensure_repo_checked_out};
    use std::process::Command;

    #[test]
    fn in_place_install_resumes_at_git_metadata_commit_point() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let work = root.join("work");
        fs::create_dir(&work).unwrap();
        let slot = Slot::lock(&root).unwrap();
        let mut attempt = slot.begin(&work, true).unwrap();
        let checkout = attempt.checkout(&work);
        fs::create_dir(&checkout).unwrap();
        let git = |args: &[&str]| {
            let out = Command::new("git")
                .current_dir(&checkout)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        git(&["init", "--initial-branch=main"]);
        fs::write(checkout.join("a"), b"a").unwrap();
        fs::write(checkout.join("b"), b"b").unwrap();
        git(&["add", "."]);
        git(&[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-m",
            "initial",
        ]);
        let inode = fs::metadata(&work).unwrap().ino();
        attempt.record_install(&slot, &checkout).unwrap();
        let err = attempt
            .install_entries(&work, |_| Err(io::Error::other("interrupted")))
            .unwrap_err();
        assert_eq!(err.to_string(), "interrupted");
        assert!(work.join("a").exists());
        assert!(!work.join("b").exists());
        assert!(!work.join(".git").exists());
        drop(slot);

        // A file arriving while the guest is down must never be overwritten.
        fs::write(work.join("b"), b"user data").unwrap();
        let spec = GuestRepoSpec {
            repo: "/unused".into(),
            branch: Some("main".into()),
            base: None,
        };
        let boot = || ensure_repo_checked_out(&work, &root, &spec, Some(Path::new("git")));
        assert!(
            boot()
                .unwrap_err()
                .contains("workspace changed during clone publication")
        );
        assert_eq!(fs::read(work.join("b")).unwrap(), b"user data");
        assert!(!work.join(".git").exists());
        fs::remove_file(work.join("b")).unwrap();
        boot().unwrap();
        assert_eq!(fs::metadata(&work).unwrap().ino(), inode);
        assert_eq!(fs::read(work.join("a")).unwrap(), b"a");
        assert_eq!(fs::read(work.join("b")).unwrap(), b"b");
        assert!(work.join(".git/HEAD").is_file());
        assert!(!attempt.root(&work).exists());
        assert!(!root.join("clone-attempt.json").exists());
    }
}
