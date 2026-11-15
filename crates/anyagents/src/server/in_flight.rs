//! The mutating calls a stopping server still owes an answer.
//!
//! Dropping a call's future does not stop its work: a handler's store and file
//! work runs on a blocking-pool thread ([`super::spawn_store`]) that finishes
//! whatever happens to the future. What ends that work is the process exiting,
//! and a mutation cut there can be half done across its steps (a spawn
//! admitted with no prompt, a delete with its row gone and its files kept).
//! So every call that is not a read holds a token for as long as its work
//! runs, carried into the blocking thread, and the stop path waits for the
//! count to reach zero, up to a bound, before the process exits.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::http::Method;
use axum::middleware::Next;
use axum::response::Response;

/// Counts mutating calls whose work is still running.
#[derive(Default)]
pub(crate) struct Tracker {
    running: AtomicUsize,
}

/// One mutating call's claim on the stop: held by the call's future and by the
/// blocking work it starts; the call counts until the last holder lets go.
pub(crate) struct Token {
    tracker: &'static Tracker,
}

impl Drop for Token {
    fn drop(&mut self) {
        self.tracker.running.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Tracker {
    pub(crate) const fn new() -> Self {
        Self {
            running: AtomicUsize::new(0),
        }
    }

    pub(crate) fn begin(&'static self) -> Arc<Token> {
        self.running.fetch_add(1, Ordering::SeqCst);
        Arc::new(Token { tracker: self })
    }

    pub(crate) fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    /// Wait until no mutating call is running, or `within` has passed.
    /// Returns how many were still running.
    pub(crate) async fn settle(&self, within: Duration) -> usize {
        let until = Instant::now() + within;
        loop {
            let running = self.running();
            if running == 0 || Instant::now() >= until {
                return running;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// The server's mutating calls.
pub(crate) static MUTATIONS: Tracker = Tracker::new();

tokio::task_local! {
    static CURRENT: Arc<Token>;
}

/// Router middleware: a call that is not a read counts until its work ends.
pub(crate) async fn track(request: Request, next: Next) -> Response {
    if matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        return next.run(request).await;
    }
    CURRENT.scope(MUTATIONS.begin(), next.run(request)).await
}

/// The current call's token, to move into blocking work it starts, so the call
/// keeps counting after its future is dropped and until that work returns.
pub(crate) fn carry() -> Option<Arc<Token>> {
    CURRENT.try_with(Arc::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_TRACKER: Tracker = Tracker::new();

    #[tokio::test]
    async fn blocking_work_keeps_its_call_counted_after_the_future_is_dropped() {
        let token = TEST_TRACKER.begin();
        let carried = CURRENT
            .scope(token, async { carry().expect("inside a call") })
            .await;
        // The future is gone; the blocking work it started still holds the
        // call, and the stop waits for it.
        assert_eq!(TEST_TRACKER.running(), 1);
        assert_eq!(TEST_TRACKER.settle(Duration::from_millis(100)).await, 1);
        let work = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            drop(carried);
        });
        assert_eq!(TEST_TRACKER.settle(Duration::from_secs(5)).await, 0);
        work.join().unwrap();
        assert!(carry().is_none(), "outside a call nothing is carried");
    }
}
