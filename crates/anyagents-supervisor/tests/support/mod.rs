//! Shared by the supervisor's integration tests: a supervisor that does not
//! stop fails its test within a bound instead of hanging the run.

#![allow(dead_code)]

use std::time::Duration;

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
