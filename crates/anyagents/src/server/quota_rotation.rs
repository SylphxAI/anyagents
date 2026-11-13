//! Profile rotation for agents a view shows quota-held.
//!
//! Recording a profile's reset and rotating an exhausted profile read vendor
//! usage APIs and sync credentials, so they never run inside a request. A view
//! that sees an active quota hold queues the agent here, and one worker thread
//! does the rotation the view used to do inline. The trigger is unchanged; only
//! the request no longer waits for it.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

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
            loop {
                for (agent_id, reset_ms) in take() {
                    rotate(&store, &agent_id, reset_ms);
                }
            }
        });
    if let Err(err) = spawned {
        eprintln!("quota rotation worker did not start: {err}");
    }
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
