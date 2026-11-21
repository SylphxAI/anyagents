//! PostgreSQL pool policy and checkout diagnostics.

use std::str::FromStr;
use std::time::{Duration, Instant};

use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection as _, PgPool, Postgres};

use super::{
    PG_ACQUIRE_TIMEOUT_SECS_ENV, PG_MAX_CONNECTIONS_ENV, PG_STATEMENT_CACHE_ENV, block_on_pg,
    non_empty_trimmed_env, redact_database_url,
};
use crate::error::Error;

const SLOW_ACQUIRE: Duration = Duration::from_millis(500);

fn pg_connect_options(url: &str) -> Result<PgConnectOptions, Error> {
    let opts = PgConnectOptions::from_str(url).map_err(|err| {
        Error::Message(format!(
            "invalid postgres url {}: {err}",
            redact_database_url(url)
        ))
    })?;
    // Default to 0 (unnamed extended queries) so transaction-mode PgBouncer
    // endpoints never fail on prepared statement collisions.
    let stmt_cache = non_empty_trimmed_env(PG_STATEMENT_CACHE_ENV)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    Ok(opts
        .application_name("anyagents")
        .statement_cache_capacity(stmt_cache))
}

fn pg_pool_options(explicit: Option<u32>, env_max: &str, env_timeout: &str) -> PgPoolOptions {
    // Each replica has its own pool. Leave room for other replicas and direct connections.
    let max_conns = explicit
        .or_else(|| env_max.trim().parse().ok())
        .unwrap_or(32)
        .clamp(2, 64);
    // Bound checkout/connection setup, not statement execution.
    let timeout_secs = match env_timeout.trim().parse::<u64>() {
        Ok(secs) if secs > 0 => secs.min(30),
        _ => 3,
    };
    PgPoolOptions::new()
        .max_connections(max_conns)
        .acquire_timeout(Duration::from_secs(timeout_secs))
        .idle_timeout(Some(Duration::from_secs(300)))
        .max_lifetime(Some(Duration::from_secs(1800)))
        // SQLx verifies/drains the connection on return. Re-probe cold idle
        // connections only; pinging before every checkout adds a remote RTT.
        .test_before_acquire(false)
        .before_acquire(|conn, meta| {
            Box::pin(async move {
                if meta.idle_for >= Duration::from_secs(5) {
                    conn.ping().await?;
                }
                Ok(true)
            })
        })
}

pub(crate) fn build_pg_pool(url: &str, max_connections: Option<u32>) -> Result<PgPool, Error> {
    let opts = pg_connect_options(url)?;
    let env_max = std::env::var(PG_MAX_CONNECTIONS_ENV).unwrap_or_default();
    let env_timeout = std::env::var(PG_ACQUIRE_TIMEOUT_SECS_ENV).unwrap_or_default();
    let pool = pg_pool_options(max_connections, &env_max, &env_timeout);
    block_on_pg(pool.connect_with(opts)).map_err(Error::from)
}

/// Time checkout separately from statement/transaction execution.
pub(crate) async fn acquire_pg(pool: &PgPool) -> Result<PoolConnection<Postgres>, sqlx::Error> {
    let start = Instant::now();
    let result = pool.acquire().await;
    let elapsed = start.elapsed();
    let timed_out = matches!(&result, Err(sqlx::Error::PoolTimedOut));
    if timed_out || elapsed > SLOW_ACQUIRE {
        let size = pool.size();
        let idle = pool.num_idle();
        let (idle, active) = checkout_counts(size, idle);
        eprintln!(
            "WARN postgres_pool_checkout elapsed_ms={} pool_size={size} idle_connections={idle} active_connections={active} max_connections={} timed_out={timed_out}",
            elapsed.as_millis(),
            pool.options().get_max_connections(),
        );
    }
    result
}

fn checkout_counts(size: u32, idle: usize) -> (String, String) {
    // SQLx samples size and idle independently. A checkout can even race a
    // return between its permit release and idle-counter increment. Don't
    // report a wrapped idle count or infer activity from an invalid sample.
    (size as usize).checked_sub(idle).map_or_else(
        || ("unknown".into(), "unknown".into()),
        |active| (idle.to_string(), active.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::TEST_POSTGRES_URL_ENV;

    #[test]
    fn checkout_counts_reject_inconsistent_samples() {
        assert_eq!(checkout_counts(4, 1), ("1".into(), "3".into()));
        assert_eq!(checkout_counts(4, 0), ("0".into(), "4".into()));
        for idle in [5, usize::MAX] {
            assert_eq!(
                checkout_counts(4, idle),
                ("unknown".into(), "unknown".into())
            );
        }
    }

    #[test]
    fn pg_pool_defaults_leave_headroom_and_bound_waiting() {
        let options = pg_pool_options(None, "", "");
        assert_eq!(options.get_max_connections(), 32);
        assert_eq!(options.get_min_connections(), 0);
        assert_eq!(options.get_acquire_timeout(), Duration::from_secs(3));
        assert_eq!(options.get_idle_timeout(), Some(Duration::from_secs(300)));
        assert_eq!(options.get_max_lifetime(), Some(Duration::from_secs(1800)));
    }

    #[test]
    fn pg_pool_connection_limit_preserves_override_precedence_and_bounds() {
        for (explicit, env, expected) in [
            (None, "  24\n", 24),
            (Some(2), "24", 2),
            (Some(4), "24", 4),
            (Some(0), "24", 2),
            (None, "0", 2),
            (None, "1", 2),
            (None, "1024", 64),
            (None, "4294967296", 32),
            (None, "-5", 32),
            (None, "invalid", 32),
        ] {
            assert_eq!(
                pg_pool_options(explicit, env, "").get_max_connections(),
                expected
            );
        }
    }

    #[test]
    fn pg_pool_acquire_timeout_accepts_only_bounded_positive_seconds() {
        for (env, expected) in [
            ("  1\n", 1),
            ("12", 12),
            ("30", 30),
            ("120", 30),
            (" ", 3),
            ("0", 3),
            ("-1", 3),
            ("1.5", 3),
            ("18446744073709551616", 3),
        ] {
            let options = pg_pool_options(Some(2), "", env);
            assert_eq!(options.get_acquire_timeout(), Duration::from_secs(expected));
        }
    }

    #[test]
    fn saturated_pg_pool_times_out_and_recovers() {
        let Some(url) = non_empty_trimmed_env(TEST_POSTGRES_URL_ENV) else {
            eprintln!("skipping pool saturation test: {TEST_POSTGRES_URL_ENV} not set");
            return;
        };
        block_on_pg(async {
            let pool = pg_pool_options(Some(2), "", "1")
                .connect_with(pg_connect_options(&url).unwrap())
                .await
                .unwrap();
            let first = acquire_pg(&pool).await.unwrap();
            let _second = acquire_pg(&pool).await.unwrap();
            let err = tokio::time::timeout(Duration::from_secs(5), acquire_pg(&pool))
                .await
                .expect("the pool must time out before the test deadline")
                .unwrap_err();
            assert!(matches!(err, sqlx::Error::PoolTimedOut));
            assert!(Error::from(err).is_transient_lock_error());

            drop(first);
            let result: i32 = sqlx::query_scalar("SELECT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(result, 1);
        });
    }
}
