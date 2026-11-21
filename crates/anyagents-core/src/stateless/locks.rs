//! Distributed coordinator locks & singleton leader leases.
//!
//! - [`PgCoordinatorLockBackend`] coordinates process locks (`agent:proc:<id>`),
//!   lifecycle locks (`agent:life:<id>`), and singleton maintenance/sweeper
//!   elections (`maint:singleton`, `maint:unadmitted_names`, `sched:singleton`)
//!   atomically in `public.coordinator_leases` with monotonic fencing tokens and
//!   automatic background lease renewal.
//! - [`LocalFlockLockBackend`] wraps single-host `flock(2)` files under
//!   `<data_dir>/locks/` so SQLite mode stays zero-config and unchanged.

use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::io::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use sqlx::PgPool;

use crate::db::{acquire_pg, block_on_pg, pg_runtime};
use crate::error::Error;
use crate::migrate::now_ms;

/// Default TTL for distributed coordinator leases (renewed every `ttl / 3`).
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

/// One acquired coordinator lease row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorLease {
    pub lease_key: String,
    pub owner_replica_id: String,
    pub fencing_token: i64,
    pub acquired_at_ms: i64,
    pub heartbeat_at_ms: i64,
    pub expires_at_ms: i64,
}

/// Distributed lock & leader-election backend.
pub trait CoordinatorLockBackend: Send + Sync {
    fn replica_id(&self) -> &str;

    /// Non-blocking lease acquisition. Returns `Ok(None)` if held by another owner and unexpired.
    fn try_acquire(
        &self,
        lease_key: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinatorLease>, Error>;

    /// Extend an existing lease owned by `(owner_replica_id, fencing_token)`.
    /// Returns `Ok(None)` if the lease expired or was fenced out.
    fn renew(
        &self,
        lease: &CoordinatorLease,
        ttl: Duration,
    ) -> Result<Option<CoordinatorLease>, Error>;

    /// Release an owned lease if `(owner_replica_id, fencing_token)` still matches.
    fn release(&self, lease: &CoordinatorLease) -> Result<bool, Error>;

    /// Inspect current lease row (even if expired).
    fn inspect(&self, lease_key: &str) -> Result<Option<CoordinatorLease>, Error>;
}

/// PostgreSQL implementation backed by `public.coordinator_leases`.
#[derive(Clone)]
pub struct PgCoordinatorLockBackend {
    pool: PgPool,
    replica_id: String,
}

impl PgCoordinatorLockBackend {
    pub fn new(pool: PgPool, replica_id: impl Into<String>) -> Self {
        Self {
            pool,
            replica_id: replica_id.into(),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Acquire a lease and attach an automatic background heartbeat renewer + release-on-drop guard.
    pub fn try_acquire_guard(
        &self,
        lease_key: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinatorLeaseGuard>, Error> {
        let Some(lease) = self.try_acquire(lease_key, ttl)? else {
            return Ok(None);
        };
        Ok(Some(CoordinatorLeaseGuard::start(self.clone(), lease, ttl)))
    }

    /// Blocking acquisition with bounded backoff polling (for lifecycle locks).
    pub fn acquire_guard_blocking(
        &self,
        lease_key: &str,
        ttl: Duration,
        timeout: Duration,
    ) -> Result<CoordinatorLeaseGuard, Error> {
        let deadline = Instant::now() + timeout;
        let mut delay = Duration::from_millis(10);
        loop {
            if let Some(guard) = self.try_acquire_guard(lease_key, ttl)? {
                return Ok(guard);
            }
            if Instant::now() >= deadline {
                return Err(Error::AgentBusy);
            }
            std::thread::sleep(delay);
            delay = (delay * 2).min(Duration::from_millis(100));
        }
    }
}

impl CoordinatorLockBackend for PgCoordinatorLockBackend {
    fn replica_id(&self) -> &str {
        &self.replica_id
    }

    fn try_acquire(
        &self,
        lease_key: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinatorLease>, Error> {
        let now = now_ms();
        let ttl_ms = i64::try_from(ttl.as_millis().max(1)).unwrap_or(i64::MAX);
        let expires_at = now.saturating_add(ttl_ms);
        let pool = self.pool.clone();
        let key = lease_key.to_string();
        let owner = self.replica_id.clone();

        let row: Option<(String, String, i64, i64, i64, i64)> = block_on_pg(async move {
            let mut conn = acquire_pg(&pool).await?;
            sqlx::query_as(
                "INSERT INTO public.coordinator_leases
                    (lease_key, owner_replica_id, fencing_token, acquired_at_ms, heartbeat_at_ms, expires_at_ms)
                 VALUES ($1, $2, 1, $3, $3, $4)
                 ON CONFLICT (lease_key) DO UPDATE
                 SET owner_replica_id = EXCLUDED.owner_replica_id,
                     fencing_token    = public.coordinator_leases.fencing_token + 1,
                     acquired_at_ms   = EXCLUDED.acquired_at_ms,
                     heartbeat_at_ms  = EXCLUDED.heartbeat_at_ms,
                     expires_at_ms    = EXCLUDED.expires_at_ms
                 WHERE public.coordinator_leases.expires_at_ms <= $3
                 RETURNING lease_key, owner_replica_id, fencing_token, acquired_at_ms, heartbeat_at_ms, expires_at_ms",
            )
            .bind(&key)
            .bind(&owner)
            .bind(now)
            .bind(expires_at)
            .fetch_optional(&mut *conn)
            .await
            .map_err(Error::from)
        })?;
        Ok(row.map(row_to_lease))
    }

    fn renew(
        &self,
        lease: &CoordinatorLease,
        ttl: Duration,
    ) -> Result<Option<CoordinatorLease>, Error> {
        let now = now_ms();
        let ttl_ms = i64::try_from(ttl.as_millis().max(1)).unwrap_or(i64::MAX);
        let expires_at = now.saturating_add(ttl_ms);
        let pool = self.pool.clone();
        let key = lease.lease_key.clone();
        let owner = lease.owner_replica_id.clone();
        let token = lease.fencing_token;

        let row: Option<(String, String, i64, i64, i64, i64)> = block_on_pg(async move {
            let mut conn = acquire_pg(&pool).await?;
            sqlx::query_as(
                "UPDATE public.coordinator_leases
                 SET heartbeat_at_ms = $4,
                     expires_at_ms   = $5
                 WHERE lease_key = $1
                   AND owner_replica_id = $2
                   AND fencing_token = $3
                   AND expires_at_ms > $4
                 RETURNING lease_key, owner_replica_id, fencing_token, acquired_at_ms, heartbeat_at_ms, expires_at_ms",
            )
            .bind(&key)
            .bind(&owner)
            .bind(token)
            .bind(now)
            .bind(expires_at)
            .fetch_optional(&mut *conn)
            .await
            .map_err(Error::from)
        })?;
        Ok(row.map(row_to_lease))
    }

    fn release(&self, lease: &CoordinatorLease) -> Result<bool, Error> {
        let now = now_ms();
        let pool = self.pool.clone();
        let key = lease.lease_key.clone();
        let owner = lease.owner_replica_id.clone();
        let token = lease.fencing_token;
        // Preserve the row with `expires_at_ms = 0` rather than deleting it so
        // `fencing_token` advances strictly monotonically across clean releases
        // as well as crash/expiry handovers.
        let affected = block_on_pg(async move {
            let mut conn = acquire_pg(&pool).await?;
            sqlx::query(
                "UPDATE public.coordinator_leases
                 SET expires_at_ms = 0,
                     heartbeat_at_ms = $4
                 WHERE lease_key = $1
                   AND owner_replica_id = $2
                   AND fencing_token = $3",
            )
            .bind(&key)
            .bind(&owner)
            .bind(token)
            .bind(now)
            .execute(&mut *conn)
            .await
            .map(|r| r.rows_affected())
            .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn inspect(&self, lease_key: &str) -> Result<Option<CoordinatorLease>, Error> {
        let now = now_ms();
        let pool = self.pool.clone();
        let key = lease_key.to_string();
        let row: Option<(String, String, i64, i64, i64, i64)> = block_on_pg(async move {
            let mut conn = acquire_pg(&pool).await?;
            sqlx::query_as(
                "SELECT lease_key, owner_replica_id, fencing_token, acquired_at_ms, heartbeat_at_ms, expires_at_ms
                 FROM public.coordinator_leases
                 WHERE lease_key = $1
                   AND expires_at_ms > $2",
            )
            .bind(&key)
            .bind(now)
            .fetch_optional(&mut *conn)
            .await
            .map_err(Error::from)
        })?;
        Ok(row.map(row_to_lease))
    }
}

fn row_to_lease(
    (lease_key, owner_replica_id, fencing_token, acquired_at_ms, heartbeat_at_ms, expires_at_ms): (
        String,
        String,
        i64,
        i64,
        i64,
        i64,
    ),
) -> CoordinatorLease {
    CoordinatorLease {
        lease_key,
        owner_replica_id,
        fencing_token,
        acquired_at_ms,
        heartbeat_at_ms,
        expires_at_ms,
    }
}

/// RAII guard for a held PostgreSQL coordinator lease.
///
/// Automatically renews the lease every `ttl / 3` on the shared PG runtime and
/// releases the row cleanly on drop (unless retained).
pub struct CoordinatorLeaseGuard {
    backend: PgCoordinatorLockBackend,
    lease: CoordinatorLease,
    stop: tokio::sync::watch::Sender<bool>,
    released: AtomicBool,
}

impl std::fmt::Debug for CoordinatorLeaseGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorLeaseGuard")
            .field("lease_key", &self.lease.lease_key)
            .field("owner_replica_id", &self.lease.owner_replica_id)
            .field("fencing_token", &self.lease.fencing_token)
            .finish()
    }
}

impl CoordinatorLeaseGuard {
    fn start(backend: PgCoordinatorLockBackend, lease: CoordinatorLease, ttl: Duration) -> Self {
        let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
        let renew_every = (ttl / 3).max(Duration::from_millis(50));
        let pool = backend.pool.clone();
        let lease_key = lease.lease_key.clone();
        let owner = lease.owner_replica_id.clone();
        let token = lease.fencing_token;
        let ttl_ms = i64::try_from(ttl.as_millis().max(1)).unwrap_or(i64::MAX);

        pg_runtime().spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stop_rx.wait_for(|stopped| *stopped) => break,
                    _ = tokio::time::sleep(renew_every) => {}
                }
                if *stop_rx.borrow() {
                    break;
                }
                let now = now_ms();
                let expires_at = now.saturating_add(ttl_ms);
                let Ok(mut conn) = acquire_pg(&pool).await else {
                    continue;
                };
                let updated = sqlx::query(
                    "UPDATE public.coordinator_leases
                     SET heartbeat_at_ms = $4,
                         expires_at_ms   = $5
                     WHERE lease_key = $1
                       AND owner_replica_id = $2
                       AND fencing_token = $3
                       AND expires_at_ms > $4",
                )
                .bind(&lease_key)
                .bind(&owner)
                .bind(token)
                .bind(now)
                .bind(expires_at)
                .execute(&mut *conn)
                .await;
                if matches!(updated, Ok(res) if res.rows_affected() == 0) {
                    // Lease was released or fenced out; stop renewing.
                    break;
                }
            }
        });

        Self {
            backend,
            lease,
            stop,
            released: AtomicBool::new(false),
        }
    }

    pub fn lease(&self) -> &CoordinatorLease {
        &self.lease
    }

    /// Release the lease early; subsequent calls and `Drop` become no-ops.
    pub fn release_now(&self) {
        if self.released.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.stop.send(true);
        let _ = self.backend.release(&self.lease);
    }

    /// Stop background renewal without deleting the row (keeper retention semantics).
    pub fn detach_without_release(&self) {
        self.released.store(true, Ordering::SeqCst);
        let _ = self.stop.send(true);
    }
}

impl Drop for CoordinatorLeaseGuard {
    fn drop(&mut self) {
        self.release_now();
    }
}

/// Local single-host `flock` lock backend for zero-config SQLite mode.
#[derive(Clone, Debug)]
pub struct LocalFlockLockBackend {
    locks_dir: PathBuf,
    replica_id: String,
    held: Arc<std::sync::Mutex<std::collections::HashMap<String, (i64, File)>>>,
}

impl LocalFlockLockBackend {
    pub fn new(data_dir: impl AsRef<Path>, replica_id: impl Into<String>) -> Self {
        Self {
            locks_dir: data_dir.as_ref().join("locks"),
            replica_id: replica_id.into(),
            held: Arc::default(),
        }
    }

    fn lock_path(&self, lease_key: &str) -> PathBuf {
        let safe: String = lease_key
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.locks_dir.join(format!("{safe}.lease"))
    }
}

impl CoordinatorLockBackend for LocalFlockLockBackend {
    fn replica_id(&self) -> &str {
        &self.replica_id
    }

    fn try_acquire(
        &self,
        lease_key: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinatorLease>, Error> {
        std::fs::create_dir_all(&self.locks_dir)?;
        let path = self.lock_path(lease_key);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == ErrorKind::WouldBlock || err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(None);
            }
            return Err(Error::Io(err));
        }
        let now = now_ms();
        let ttl_ms = i64::try_from(ttl.as_millis().max(1)).unwrap_or(i64::MAX);
        let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        let token = held.get(lease_key).map(|(t, _)| t + 1).unwrap_or(1);
        held.insert(lease_key.to_string(), (token, file));
        Ok(Some(CoordinatorLease {
            lease_key: lease_key.to_string(),
            owner_replica_id: self.replica_id.clone(),
            fencing_token: token,
            acquired_at_ms: now,
            heartbeat_at_ms: now,
            expires_at_ms: now.saturating_add(ttl_ms),
        }))
    }

    fn renew(
        &self,
        lease: &CoordinatorLease,
        ttl: Duration,
    ) -> Result<Option<CoordinatorLease>, Error> {
        let held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        let Some((token, _)) = held.get(&lease.lease_key) else {
            return Ok(None);
        };
        if *token != lease.fencing_token || lease.owner_replica_id != self.replica_id {
            return Ok(None);
        }
        let now = now_ms();
        let ttl_ms = i64::try_from(ttl.as_millis().max(1)).unwrap_or(i64::MAX);
        Ok(Some(CoordinatorLease {
            heartbeat_at_ms: now,
            expires_at_ms: now.saturating_add(ttl_ms),
            ..lease.clone()
        }))
    }

    fn release(&self, lease: &CoordinatorLease) -> Result<bool, Error> {
        let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((token, _)) = held.get(&lease.lease_key)
            && *token == lease.fencing_token
            && lease.owner_replica_id == self.replica_id
        {
            if let Some((_, file)) = held.remove(&lease.lease_key) {
                let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn inspect(&self, lease_key: &str) -> Result<Option<CoordinatorLease>, Error> {
        let held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        Ok(held.get(lease_key).map(|(token, _)| CoordinatorLease {
            lease_key: lease_key.to_string(),
            owner_replica_id: self.replica_id.clone(),
            fencing_token: *token,
            acquired_at_ms: 0,
            heartbeat_at_ms: now_ms(),
            expires_at_ms: i64::MAX,
        }))
    }
}
