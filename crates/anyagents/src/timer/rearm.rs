//! Retry timer submissions without retrying a committed mailbox claim.

use std::time::{Duration, Instant};

use anyagents_core::Store;

use super::TimerBackend;

pub(super) fn rearm_after_claim(
    store: &Store,
    backend: &dyn TimerBackend,
    id: &str,
    due: i64,
    now: i64,
) -> bool {
    match arm_slot(store, backend, id, Some(due), due, due, now) {
        Ok(()) => true,
        Err(err) => {
            eprintln!("Schedule {id} could not be re-armed for {due}: {err}");
            false
        }
    }
}

pub(super) fn arm_slot(
    store: &Store,
    backend: &dyn TimerBackend,
    id: &str,
    expected_due: Option<i64>,
    delivery_at: i64,
    intended_at: i64,
    now: i64,
) -> Result<(), String> {
    if backend.is_noop() {
        return Ok(());
    }
    let start = Instant::now();
    let mut fire_at = delivery_at;
    let mut delays = [Duration::from_millis(200), Duration::from_millis(400)].into_iter();
    loop {
        if !store
            .get_schedule(id)
            .map_err(|err| err.to_string())?
            .is_some_and(|row| row.state == "active" && row.next_fire_at_ms == expected_due)
        {
            return Ok(());
        }
        // A scheduler may reject a past timestamp. Keep retries byte-for-byte
        // identical while possible, otherwise move only the delivery time.
        let current = now.saturating_add(start.elapsed().as_millis() as i64);
        if backend.is_remote() && fire_at <= current {
            fire_at = current.saturating_add(1_000);
        }
        match backend.schedule_slot(id, fire_at, expected_due, intended_at) {
            Ok(Some(receipt)) => {
                if !store
                    .set_schedule_external_timer_id_for_slot(id, expected_due, &receipt)
                    .map_err(|err| format!("persist timer receipt: {err}"))?
                {
                    backend.cancel(id, Some(&receipt))?;
                }
                return Ok(());
            }
            Ok(None) => return Ok(()),
            Err(err) => match delays.next() {
                Some(delay) => std::thread::sleep(delay),
                None => return Err(err),
            },
        }
    }
}
