//! Process and lifecycle locks for agents (flock and PostgreSQL coordinator leases).

use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::agents::agent_lock_path;
use super::{Store, WRITE_RETRY_BUDGET, registry};
use crate::error::Error;
use crate::id::AgentId;

/// Held for one vendor loop or lifecycle mutation. Drop explicitly releases the
/// flock even while a fork-inherited descriptor remains open.
///
/// A process lock can be shared with the keeper that owns the agent's harness
/// process (see `docs/adr/0007-process-architecture.md`): every copy is a
/// descriptor of the same open file description, so the lock is held while
/// any copy, in any process, is open. Only the descriptor that took the lock
/// (or adopted it from a keeper) unlocks on drop, and not once
/// [`AgentLock::retain`] said the keeper keeps holding it after this process.
#[derive(Debug)]
enum AgentLockInner {
    Flock(File),
    PgLease {
        guard: Arc<crate::stateless::CoordinatorLeaseGuard>,
        /// Read-only descriptor supporting `AsFd` / `share()` compatibility for
        /// callers that expect an OS fd handle.
        fd: File,
    },
}

#[derive(Debug)]
pub struct AgentLock {
    inner: AgentLockInner,
    /// This descriptor took the lock (or adopted it), so its drop releases it.
    owner: bool,
    /// Cleared by [`AgentLock::retain`] on any copy: no copy in this process
    /// releases the lock, because a keeper holds it on after this process.
    release: Arc<std::sync::atomic::AtomicBool>,
}

impl AgentLock {
    fn taken(file: File) -> Self {
        Self {
            inner: AgentLockInner::Flock(file),
            owner: true,
            release: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    fn from_pg_lease(guard: crate::stateless::CoordinatorLeaseGuard) -> std::io::Result<Self> {
        let fd = OpenOptions::new().read(true).open("/dev/null")?;
        Ok(Self {
            inner: AgentLockInner::PgLease {
                guard: Arc::new(guard),
                fd,
            },
            owner: true,
            release: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        })
    }

    /// Another descriptor of this lock, for a keeper process to inherit or
    /// for a later launch to hand on. The copy never unlocks on drop.
    pub fn share(&self) -> std::io::Result<AgentLock> {
        let inner = match &self.inner {
            AgentLockInner::Flock(file) => AgentLockInner::Flock(file.try_clone()?),
            AgentLockInner::PgLease { guard, fd } => AgentLockInner::PgLease {
                guard: Arc::clone(guard),
                fd: fd.try_clone()?,
            },
        };
        Ok(Self {
            inner,
            owner: false,
            release: Arc::clone(&self.release),
        })
    }

    /// The lock a keeper passed back on reattach: the same open file
    /// description it holds. This process now owns releasing it, unless it
    /// retains it again.
    pub fn adopt(fd: std::os::fd::OwnedFd) -> AgentLock {
        Self::taken(File::from(fd))
    }

    /// A keeper keeps holding this lock after this process lets go: no copy
    /// in this process releases it on drop; each only closes its descriptor.
    pub fn retain(&self) {
        self.release.store(false, Ordering::SeqCst);
    }

    /// Fencing token when backed by a distributed PostgreSQL coordinator lease.
    pub fn fencing_token(&self) -> Option<i64> {
        match &self.inner {
            AgentLockInner::Flock(_) => None,
            AgentLockInner::PgLease { guard, .. } => Some(guard.lease().fencing_token),
        }
    }

    #[cfg(test)]
    pub(super) fn flock_file(&self) -> &File {
        match &self.inner {
            AgentLockInner::Flock(file) => file,
            AgentLockInner::PgLease { fd, .. } => fd,
        }
    }
}

impl std::os::fd::AsFd for AgentLock {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        match &self.inner {
            AgentLockInner::Flock(file) => file.as_fd(),
            AgentLockInner::PgLease { fd, .. } => fd.as_fd(),
        }
    }
}

impl Drop for AgentLock {
    fn drop(&mut self) {
        if !self.owner {
            return;
        }
        let should_release = self.release.load(Ordering::SeqCst);
        match &self.inner {
            AgentLockInner::Flock(file) => {
                if !should_release {
                    return;
                }
                // Closing only our descriptor leaves flock held by a concurrent fork's
                // inherited copy until exec. Release the open-file-description lock
                // explicitly so the next turn and busy probe see the intended lifetime.
                let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
            }
            AgentLockInner::PgLease { guard, .. } => {
                if should_release {
                    guard.release_now();
                } else {
                    guard.detach_without_release();
                }
            }
        }
    }
}

impl Store {
    /// Exclusive lock for this agent's vendor process. A second caller gets [`Error::AgentBusy`].
    pub fn try_lock_agent(&self, agent_id: &AgentId) -> Result<AgentLock, Error> {
        if let registry::Backend::Postgres(pg) = &self.backend {
            let backend = crate::stateless::PgCoordinatorLockBackend::new(
                pg.handle.pool.clone(),
                self.replica_id.clone(),
            );
            let key = format!("agent:proc:{}", agent_id.as_str());
            let Some(guard) =
                backend.try_acquire_guard(&key, crate::stateless::DEFAULT_LEASE_TTL)?
            else {
                return Err(Error::AgentBusy);
            };
            return AgentLock::from_pg_lease(guard).map_err(Error::Io);
        }
        let path = agent_lock_path(&self.data_dir, agent_id);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        // flock, not a POSIX record lock: two descriptors in one process must exclude
        // each other, which is what a second loop in the same test process needs.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == ErrorKind::WouldBlock || err.raw_os_error() == Some(libc::EAGAIN) {
                return Err(Error::AgentBusy);
            }
            return Err(Error::Io(err));
        }
        Ok(AgentLock::taken(file))
    }

    /// Serialize lifecycle state and archive-marker changes across API/server
    /// processes. This is separate from the vendor-process lock: an archived
    /// or live process must never prevent its lifecycle from being changed.
    /// Keep the returned guard alive through preparation and the store commit.
    pub fn lock_agent_lifecycle(&self, agent_id: &AgentId) -> Result<AgentLock, Error> {
        self.agent_lifecycle_lock(agent_id, false)
    }

    /// Maintenance must skip a busy lifecycle rather than delay unrelated mail.
    pub fn try_lock_agent_lifecycle(&self, agent_id: &AgentId) -> Result<AgentLock, Error> {
        self.agent_lifecycle_lock(agent_id, true)
    }

    fn agent_lifecycle_lock(
        &self,
        agent_id: &AgentId,
        nonblocking: bool,
    ) -> Result<AgentLock, Error> {
        if let registry::Backend::Postgres(pg) = &self.backend {
            debug_assert!(
                nonblocking || !registry::holds_store_writer(),
                "a lifecycle lock is waited for while this thread holds the Store writer"
            );
            let backend = crate::stateless::PgCoordinatorLockBackend::new(
                pg.handle.pool.clone(),
                self.replica_id.clone(),
            );
            let key = format!("agent:life:{}", agent_id.as_str());
            let guard = if nonblocking {
                match backend.try_acquire_guard(&key, crate::stateless::DEFAULT_LEASE_TTL)? {
                    Some(g) => g,
                    None => return Err(Error::AgentBusy),
                }
            } else {
                backend.acquire_guard_blocking(
                    &key,
                    crate::stateless::DEFAULT_LEASE_TTL,
                    WRITE_RETRY_BUDGET,
                )?
            };
            return AgentLock::from_pg_lease(guard).map_err(Error::Io);
        }
        let dir = self.data_dir.join("locks");
        std::fs::create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(format!("{}.lifecycle", agent_id.as_str())))?;
        // Lock order: lifecycle before the Store writer. Waiting here while
        // holding the writer stalls every write of the process for as long
        // as the lifecycle's holder keeps it (and deadlocks if that holder
        // waits for the writer). A non-blocking attempt never waits.
        debug_assert!(
            nonblocking || !registry::holds_store_writer(),
            "a lifecycle lock is waited for while this thread holds the Store writer"
        );
        let operation = libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 };
        loop {
            let rc = unsafe { libc::flock(file.as_raw_fd(), operation) };
            if rc == 0 {
                return Ok(AgentLock::taken(file));
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == ErrorKind::WouldBlock {
                return Err(Error::AgentBusy);
            }
            if error.kind() != ErrorKind::Interrupted {
                return Err(Error::Io(error));
            }
        }
    }
}
