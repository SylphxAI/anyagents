//! Pacing of vendor requests.
//!
//! * [`RequestBudget`]: one token bucket per vendor account, shared by every
//!   remote session of every agent that calls with that account, so a fleet
//!   of remote agents never exceeds what one organisation may send, and a
//!   `retry-after` one call was told pauses all of them.
//! * [`backoff_ms`]: how long one agent waits before it offers mail again
//!   after the vendor did not take it: exponential, with jitter.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Requests per second one account may send, sustained.
pub const DEFAULT_RATE: f64 = 10.0;
/// Requests one account may send at once after a quiet spell.
pub const DEFAULT_BURST: f64 = 20.0;

struct Bucket {
    tokens: f64,
    refilled: Instant,
    paused_until: Option<Instant>,
}

/// A token bucket shared by every caller of one vendor account.
pub struct RequestBudget {
    rate: f64,
    burst: f64,
    bucket: Mutex<Bucket>,
}

impl RequestBudget {
    pub fn new(rate: f64, burst: f64) -> Self {
        Self {
            rate: rate.max(0.001),
            burst: burst.max(1.0),
            bucket: Mutex::new(Bucket {
                tokens: burst.max(1.0),
                refilled: Instant::now(),
                paused_until: None,
            }),
        }
    }

    /// The one budget of `account` in this process (a digest of its
    /// credential, never the credential), made on first use.
    pub fn shared(account: &str) -> Arc<Self> {
        static BUDGETS: OnceLock<Mutex<HashMap<String, Arc<RequestBudget>>>> = OnceLock::new();
        let mut budgets = BUDGETS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        Arc::clone(
            budgets
                .entry(account.to_string())
                .or_insert_with(|| Arc::new(Self::new(DEFAULT_RATE, DEFAULT_BURST))),
        )
    }

    /// How long the caller must wait for a request now; `None` takes one.
    fn try_take(&self, now: Instant) -> Option<Duration> {
        let mut bucket = self.bucket.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(until) = bucket.paused_until {
            if now < until {
                return Some(until - now);
            }
            bucket.paused_until = None;
        }
        let elapsed = now.saturating_duration_since(bucket.refilled).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.burst);
        bucket.refilled = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            return None;
        }
        Some(Duration::from_secs_f64((1.0 - bucket.tokens) / self.rate))
    }

    /// Wait for the account's turn to send one request.
    pub async fn acquire(&self) {
        while let Some(wait) = self.try_take(Instant::now()) {
            tokio::time::sleep(wait).await;
        }
    }

    /// The vendor asked this account to wait: every caller does.
    pub fn pause_for(&self, delay: Duration) {
        let until = Instant::now() + delay;
        let mut bucket = self.bucket.lock().unwrap_or_else(|err| err.into_inner());
        if bucket.paused_until.is_none_or(|current| current < until) {
            bucket.paused_until = Some(until);
        }
    }
}

/// The wait before attempt `attempt` (1-based) to offer mail again: 1 s
/// doubling to 5 minutes, spread by up to ±20%, never less than what the
/// vendor asked for.
pub fn backoff_ms(attempt: u32, retry_after_ms: Option<u64>) -> u64 {
    let shift = attempt.saturating_sub(1).min(16);
    let base = 1_000u64.saturating_mul(1 << shift).min(300_000);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let percent = u64::from(nanos % 41); // 0..=40, i.e. -20%..=+20%
    let jittered = base.saturating_mul(80 + percent) / 100;
    jittered.max(retry_after_ms.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bucket_paces_after_its_burst_and_a_pause_holds_every_caller() {
        let budget = RequestBudget::new(10.0, 3.0);
        let now = Instant::now();
        for _ in 0..3 {
            assert_eq!(budget.try_take(now), None);
        }
        let wait = budget.try_take(now).unwrap();
        assert!(wait > Duration::from_millis(50) && wait <= Duration::from_millis(100));
        // Refilled at the rate.
        assert_eq!(budget.try_take(now + Duration::from_millis(150)), None);

        budget.pause_for(Duration::from_secs(2));
        let held = budget.try_take(Instant::now()).unwrap();
        assert!(held > Duration::from_millis(1_500), "{held:?}");
        assert!(Arc::ptr_eq(
            &RequestBudget::shared("a"),
            &RequestBudget::shared("a")
        ));
        assert!(!Arc::ptr_eq(
            &RequestBudget::shared("a"),
            &RequestBudget::shared("b")
        ));
    }

    #[test]
    fn backoff_doubles_with_jitter_caps_and_honours_retry_after() {
        for (attempt, low, high) in [(1, 800, 1_200), (2, 1_600, 2_400), (30, 240_000, 360_000)] {
            let wait = backoff_ms(attempt, None);
            assert!((low..=high).contains(&wait), "{attempt}: {wait}");
        }
        assert_eq!(backoff_ms(1, Some(30_000)), 30_000);
    }
}
