//! Shared by the supervisor's integration tests: a supervisor that does not
//! stop fails its test within a bound instead of hanging the run.

#![allow(dead_code)]

use std::sync::{Arc, Mutex, atomic::AtomicBool};
use std::time::Duration;

use anyagents_supervisor::{Clock, ManualClock};

/// How long a supervisor may take to stop.
pub const STOP_WITHIN: Duration = Duration::from_secs(30);

/// Run `stop` (a supervisor's drop or `retain_processes`), failing the test
/// when it has not returned within [`STOP_WITHIN`]. While the test is already
/// failing, a supervisor that does not stop is reported and left behind.
pub fn within(what: &str, stop: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        stop();
        let _ = done.send(());
    });
    if finished.recv_timeout(STOP_WITHIN).is_ok() {
        return;
    }
    if std::thread::panicking() {
        eprintln!("never within {STOP_WITHIN:?}: {what}");
    } else {
        panic!("never within {STOP_WITHIN:?}: {what}");
    }
}

/// Stop `supervisor` by dropping it, within [`STOP_WITHIN`].
pub fn stop<T: Send + 'static>(supervisor: T) {
    within("the supervisor stops", move || drop(supervisor));
}

/// A [`ManualClock`] that records every wait it is asked for, and which of
/// them still block a worker. A test reads the deadline a worker actually
/// installed, and moves time only to a timer one waits on: time moved before
/// a worker reads the clock shifts the timer it is about to install past the
/// new time, and nothing wakes it.
pub struct RecordingClock {
    inner: ManualClock,
    waits: Mutex<Vec<(i64, Option<i64>)>>,
    blocked: Mutex<Vec<i64>>,
}

impl RecordingClock {
    pub fn new(inner: ManualClock) -> Arc<Self> {
        Arc::new(Self {
            inner,
            waits: Mutex::new(Vec::new()),
            blocked: Mutex::new(Vec::new()),
        })
    }

    /// Every wait so far: the time it was asked at and its deadline.
    pub fn waits(&self) -> Vec<(i64, Option<i64>)> {
        self.waits.lock().unwrap().clone()
    }

    /// Move time to the nearest timer a worker is blocked on, when it falls
    /// after now and no later than `limit`. `true` when time moved.
    pub fn fire_next_timer(&self, limit: i64) -> bool {
        let now = self.inner.now_ms();
        let next = self
            .blocked
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|at| (now + 1..=limit).contains(at))
            .min();
        next.map(|at| self.inner.set_ms(at)).is_some()
    }
}

impl Clock for RecordingClock {
    fn now_ms(&self) -> i64 {
        self.inner.now_ms()
    }
    fn wait(&self, deadline: Option<i64>, woken: &Arc<AtomicBool>) -> bool {
        self.waits.lock().unwrap().push((self.now_ms(), deadline));
        if let Some(at) = deadline {
            self.blocked.lock().unwrap().push(at);
        }
        let flagged = self.inner.wait(deadline, woken);
        if let Some(at) = deadline {
            let mut blocked = self.blocked.lock().unwrap();
            if let Some(index) = blocked.iter().position(|blocked| *blocked == at) {
                blocked.swap_remove(index);
            }
        }
        flagged
    }
    fn poke(&self) {
        self.inner.poke();
    }
}
