//! The Telegram poll runs on its own thread, one long poll at a time. Dropping
//! the poller asks the thread to stop, and `stop` waits for the call in hand, so
//! the server never needs a timed sleep to shut down.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use anyagents_core::Store;

use super::channel::{LONG_POLL_S, poll_once};

/// A running poller. Dropping it waits for the thread to stop.
pub struct Poller {
    stop: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Poller {
    pub fn start(store: Arc<Store>, data_dir: std::path::PathBuf) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("anyagents-telegram".into())
            .spawn(move || {
                while !flag.load(Ordering::SeqCst) {
                    let dir = &data_dir;
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        poll_once(&store, dir, LONG_POLL_S)
                    }));
                    // A failed poll waits a moment; a good one asks again at once.
                    if !matches!(outcome, Ok(Ok(()))) {
                        sleep_a_moment(&flag);
                    }
                }
            })
            .map_err(|err| err.to_string())?;
        Ok(Self {
            stop,
            handle: Mutex::new(Some(handle)),
        })
    }

    /// Ask the thread to stop and wait for it. Called at server shutdown.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let handle = lock(&self.handle).take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.stop();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn sleep_a_moment(flag: &AtomicBool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if flag.load(Ordering::SeqCst) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
