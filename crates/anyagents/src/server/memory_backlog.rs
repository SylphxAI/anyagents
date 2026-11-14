//! Agents whose sleep-memory consolidation may have work.
//!
//! Every live agent is in the backlog when the daemon starts. After that an
//! agent joins when the store wakes it (new mail, a state change) or an API
//! write changes its memory, and leaves once a pass finds its consolidation
//! caught up with its mailbox. A turn always begins with one of those wakes, so
//! the maintenance worker visits only agents that did something, and an idle
//! fleet costs it no per-agent work.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

use anyagents_core::{AgentId, Store, Wake};

#[derive(Default)]
pub(crate) struct MemoryBacklog {
    agents: Mutex<HashSet<AgentId>>,
}

/// One backlog per data directory this process serves.
static BACKLOGS: LazyLock<Mutex<HashMap<PathBuf, Arc<MemoryBacklog>>>> =
    LazyLock::new(Mutex::default);

fn backlog_for(data_dir: &Path, create: bool) -> Option<Arc<MemoryBacklog>> {
    let mut backlogs = BACKLOGS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if create {
        return Some(Arc::clone(
            backlogs.entry(data_dir.to_path_buf()).or_default(),
        ));
    }
    backlogs.get(data_dir).cloned()
}

impl MemoryBacklog {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<AgentId>> {
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn mark(&self, agent_id: &AgentId) {
        self.lock().insert(agent_id.clone());
    }

    pub(crate) fn pending(&self) -> Vec<AgentId> {
        let mut pending: Vec<_> = self.lock().iter().cloned().collect();
        pending.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        pending
    }

    pub(crate) fn settle(&self, agent_id: &AgentId) {
        self.lock().remove(agent_id);
    }
}

/// The daemon's backlog: seeded with the live fleet and fed by store wakes.
pub(crate) fn start(store: &Store) -> Arc<MemoryBacklog> {
    let backlog = backlog_for(store.data_dir(), true).expect("created on demand");
    for agent in store.list_live_agents().unwrap_or_default() {
        backlog.mark(&agent.id);
    }
    let wakes = store.subscribe();
    let fed = Arc::clone(&backlog);
    let spawned = std::thread::Builder::new()
        .name("anyagents-memory-wakes".into())
        .spawn(move || {
            for wake in wakes {
                match wake {
                    Wake::Mail { agent_id } | Wake::Agent { agent_id } => fed.mark(&agent_id),
                }
            }
        });
    if let Err(err) = spawned {
        eprintln!("memory backlog wake listener did not start: {err}");
    }
    backlog
}

/// An API write changed this agent's memory. A no-op where no daemon serves
/// `data_dir`.
pub(crate) fn mark(data_dir: &Path, agent_id: &AgentId) {
    if let Some(backlog) = backlog_for(data_dir, false) {
        backlog.mark(agent_id);
    }
}
