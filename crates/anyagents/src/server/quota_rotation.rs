//! Profile rotation for agents a view shows quota-held.
//!
//! Recording a profile's reset and rotating an exhausted profile read vendor
//! usage APIs and sync credentials, so they never run inside a request. A view
//! that sees an active quota hold queues the agent here, and one worker thread
//! does the rotation the view used to do inline. The trigger is unchanged; only
//! the request no longer waits for it.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyagents_core::{AgentId, Store};

#[derive(Default)]
struct Queue {
    pending: Mutex<HashMap<AgentId, i64>>,
    ready: Condvar,
}

static QUEUE: OnceLock<Queue> = OnceLock::new();

/// Ask the worker to record `reset_ms` and consider rotating this agent's
/// profile. A no-op where no worker runs (offline reads, tests).
pub(crate) fn queue(agent_id: &AgentId, reset_ms: i64) {
    let Some(queue) = QUEUE.get() else {
        return;
    };
    queue
        .pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(agent_id.clone(), reset_ms);
    queue.ready.notify_one();
}

/// Start the worker once per process. It sleeps until a view queues an agent.
pub(crate) fn start(store: Arc<Store>) {
    if QUEUE.set(Queue::default()).is_err() {
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("anyagents-quota-rotation".into())
        .spawn(move || {
            let mut handled = HashMap::new();
            loop {
                for (agent_id, reset_ms) in take() {
                    if due(&handled, &agent_id, reset_ms, Instant::now()) {
                        rotate(&store, &agent_id, reset_ms);
                        handled.insert(agent_id, (reset_ms, Instant::now()));
                    }
                }
            }
        });
    if let Err(err) = spawned {
        eprintln!("quota rotation worker did not start: {err}");
    }
}

/// Every view of a quota-held agent queues it again, but the same hold needs
/// handling once per [`RECHECK`] (the rotation's own cooldown), or at once
/// when its reset moves. Without this gate each view re-read the agent's
/// markers and rewrote its profile's reset file.
const RECHECK: Duration = Duration::from_secs(120);

fn due(
    handled: &HashMap<AgentId, (i64, Instant)>,
    agent_id: &AgentId,
    reset_ms: i64,
    now: Instant,
) -> bool {
    handled.get(agent_id).is_none_or(|&(seen, at)| {
        seen != reset_ms || now.saturating_duration_since(at) >= RECHECK
    })
}

fn take() -> HashMap<AgentId, i64> {
    let queue = QUEUE.get().expect("queue is set before the worker starts");
    let mut pending = queue
        .pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while pending.is_empty() {
        pending = queue
            .ready
            .wait(pending)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    std::mem::take(&mut *pending)
}

fn rotate(store: &Store, agent_id: &AgentId, reset_ms: i64) {
    let Ok(agent) = store.agent(agent_id) else {
        return;
    };
    if agent.state == "archived" {
        return;
    }
    crate::create::record_profile_quota_reset(store.data_dir(), &agent, reset_ms);
    let _ = crate::create::try_rotate_exhausted_profile(store, store.data_dir(), &agent, true);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hold_is_handled_once_per_recheck_or_when_its_reset_moves() {
        let id = AgentId::generate();
        let start = Instant::now();
        let mut handled = HashMap::new();
        assert!(due(&handled, &id, 1_000, start), "a new hold is handled");
        handled.insert(id.clone(), (1_000, start));
        assert!(!due(&handled, &id, 1_000, start + Duration::from_secs(119)));
        assert!(due(&handled, &id, 2_000, start + Duration::from_secs(1)), "a moved reset");
        assert!(due(&handled, &id, 1_000, start + RECHECK), "the cooldown passed");
        assert!(due(&handled, &AgentId::generate(), 1_000, start), "another agent");
    }
}
