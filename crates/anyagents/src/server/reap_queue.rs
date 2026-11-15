//! Archived sub-agents whose workspace cleanup is still owed.
//!
//! An archive enqueues the agent, and the maintenance worker visits only queued
//! agents that are due. A cleanup that fails waits out a doubling backoff
//! instead of being retried every minute. The queue is one small file per agent
//! under `<data>/reap-pending/`, so it survives restarts and offline CLI
//! archives; `.anyagents-reaped` in the agent's config stays the record of a
//! finished cleanup.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use anyagents_core::{AgentId, Store};

const DIR: &str = "reap-pending";
const FIRST_RETRY_MS: i64 = 60_000;
const MAX_RETRY_MS: i64 = 24 * 3_600_000;

#[derive(Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Entry {
    attempts: u32,
    next_at_ms: i64,
}

fn entry_path(data: &Path, id: &AgentId) -> PathBuf {
    data.join(DIR).join(id.as_str())
}

fn write(data: &Path, id: &AgentId, entry: &Entry) {
    let path = entry_path(data, id);
    let written = std::fs::create_dir_all(data.join(DIR)).and_then(|()| {
        anyagents_harness::atomic_write_nonempty_file(
            &path,
            serde_json::to_string(entry)
                .expect("an entry serializes")
                .as_bytes(),
            0o600,
        )
    });
    if let Err(err) = written {
        eprintln!("cannot persist {}: {err}", path.display());
    }
}

/// Owe a cleanup for this archive, due at once.
pub(super) fn enqueue(data: &Path, id: &AgentId) {
    write(data, id, &Entry::default());
}

/// Nothing is owed: the cleanup finished, or the agent is no longer archived.
pub(super) fn dequeue(data: &Path, id: &AgentId) {
    let _ = std::fs::remove_file(entry_path(data, id));
}

/// The cleanup failed: try again after twice the last wait.
pub(super) fn note_failure(data: &Path, id: &AgentId, now_ms: i64) {
    let attempts = std::fs::read(entry_path(data, id))
        .ok()
        .and_then(|raw| serde_json::from_slice::<Entry>(&raw).ok())
        .map_or(0, |entry| entry.attempts)
        .saturating_add(1);
    write(
        data,
        id,
        &Entry {
            attempts,
            next_at_ms: now_ms.saturating_add(retry_delay_ms(attempts)),
        },
    );
}

fn retry_delay_ms(attempts: u32) -> i64 {
    FIRST_RETRY_MS
        .saturating_mul(1i64 << attempts.saturating_sub(1).min(20))
        .min(MAX_RETRY_MS)
}

/// Queued agents whose cleanup is due at `now_ms`. An unreadable entry is due.
pub(super) fn due(data: &Path, now_ms: i64) -> Vec<AgentId> {
    let Ok(entries) = std::fs::read_dir(data.join(DIR)) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let id = AgentId::parse(entry.file_name().to_str()?).ok()?;
            let due = std::fs::read(entry.path())
                .ok()
                .and_then(|raw| serde_json::from_slice::<Entry>(&raw).ok())
                .is_none_or(|entry| entry.next_at_ms <= now_ms);
            due.then_some(id)
        })
        .collect()
}

/// Enqueue every archived sub-agent the queue does not hold and whose current
/// archive has no finished cleanup: archives an earlier binary, or a writer
/// that bypassed the archive path, left unqueued. It runs on the first pass of
/// each process for a data directory, so its cost is per start, not per pass.
/// A queued agent keeps its backoff.
pub(super) fn backfill(store: &Store, data: &Path) {
    static DONE: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());
    let done = || DONE.lock().unwrap_or_else(PoisonError::into_inner);
    if !done().insert(data.to_path_buf()) {
        return;
    }
    let Ok(archived) = store.archived_subagent_ids() else {
        done().remove(data);
        return;
    };
    for agent_id in archived {
        if entry_path(data, &agent_id).exists() {
            continue;
        }
        let cfg = anyagents_core::agent_config_dir(data, &agent_id);
        let generation = std::fs::read(cfg.join(".anyagents-archived")).unwrap_or_default();
        let finished = !generation.is_empty()
            && std::fs::read(cfg.join(".anyagents-reaped")).is_ok_and(|done| done == generation);
        if !finished {
            enqueue(data, &agent_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_back_off_doubling_to_a_day() {
        let dir = tempfile::tempdir().unwrap();
        let id = AgentId::generate();
        enqueue(dir.path(), &id);
        assert_eq!(due(dir.path(), 0), std::slice::from_ref(&id));
        note_failure(dir.path(), &id, 1_000);
        assert!(due(dir.path(), 1_000 + 59_999).is_empty());
        assert_eq!(due(dir.path(), 1_000 + 60_000), std::slice::from_ref(&id));
        note_failure(dir.path(), &id, 100_000);
        assert!(due(dir.path(), 100_000 + 119_999).is_empty());
        assert_eq!(
            due(dir.path(), 100_000 + 120_000),
            std::slice::from_ref(&id)
        );
        assert_eq!(retry_delay_ms(30), MAX_RETRY_MS);
        // A new archive owes a fresh cleanup at once.
        enqueue(dir.path(), &id);
        assert_eq!(due(dir.path(), 0), std::slice::from_ref(&id));
        dequeue(dir.path(), &id);
        assert!(due(dir.path(), i64::MAX).is_empty());
    }
}
