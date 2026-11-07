//! Daemon-only host memory boundaries. Never invoked by the seat-side MCP
//! process and never changes native raw-writer ownership in PR1.
use anyagents_core::{
    AgentId, Error, Store,
    host_migration::{AgentOwnership, HostMigration},
};
use std::{collections::HashSet, fs, path::Path};

/// The caller owns this agent's process lock and has not started a vendor yet.
/// The global migration lock serializes receipt publication across agents.
pub(crate) fn before_launch(data: &Path, agent: &AgentId) -> Result<(), Error> {
    before_launch_from(data, agent, Path::new("/scratch/tmp/owners"))
}
fn before_launch_from(data: &Path, agent: &AgentId, owners_pages: &Path) -> Result<(), Error> {
    let store = Store::open(data.join("state.db"))?;
    store.agent(agent)?; // no caller-selected or filename-guessed ownership
    let registry = store.list_agents()?;
    let owners = registry
        .into_iter()
        .map(|a| {
            let mut aliases = store.agent_aliases(&a.id)?;
            aliases.push(a.name);
            Ok(AgentOwnership { id: a.id, aliases })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let directory = data.join("host-memory-migration");
    fs::create_dir_all(&directory)?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("migration.lock"))?;
    lock.lock()?;
    let mut migration = HostMigration::open(data)?;
    migration.discover(&owners, owners_pages)?;
    let stopped = HashSet::from([agent.to_string()]);
    migration.stage_selected(&stopped, Some(&stopped))?;
    Ok(())
}

/// Deploy/startup migration inventories every registered identity, including
/// archived identities that will never launch. Process locks, not SQL state,
/// establish quiescence; live or busy identities are explicitly deferred.
pub(crate) fn on_start(store: &Store) -> Result<(), Error> {
    on_start_from(store, Path::new("/scratch/tmp/owners"))
}
fn on_start_from(store: &Store, owners_pages: &Path) -> Result<(), Error> {
    let directory = store.data_dir().join("host-memory-migration");
    fs::create_dir_all(&directory)?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("migration.lock"))?;
    lock.lock()?;
    let registry = store.list_agents()?;
    let mut stopped = HashSet::new();
    let mut deferred = Vec::new();
    let mut guards = Vec::new();
    let mut owners = Vec::new();
    for agent in &registry {
        let mut aliases = store.agent_aliases(&agent.id)?;
        aliases.push(agent.name.clone());
        owners.push(AgentOwnership {
            id: agent.id.clone(),
            aliases,
        });
        if let Ok(lifecycle) = store.try_lock_agent_lifecycle(&agent.id)
            && let Ok(process) = store.try_lock_agent(&agent.id)
        {
            stopped.insert(agent.id.to_string());
            guards.push((lifecycle, process));
        } else {
            deferred.push(agent.id.to_string());
        }
    }
    let mut migration = HostMigration::open(store.data_dir())?;
    migration.discover(&owners, owners_pages)?;
    migration.stage(&stopped)?;
    let mut statuses = std::collections::BTreeMap::<String, usize>::new();
    for entry in &migration.manifest.entries {
        *statuses.entry(format!("{:?}", entry.status)).or_default() += 1;
    }
    let mut stopped: Vec<_> = stopped.into_iter().collect();
    stopped.sort();
    deferred.sort();
    let receipt = serde_json::json!({"version": 1, "timestamp_ms": anyagents_core::now_ms(),
        "population_count": registry.len(), "stopped_agents": stopped,
        "deferred_active_or_busy_agents": deferred, "entry_counts": statuses,
        "manifest_sha256": anyagents_core::learned_memory::sha256(&fs::read(directory.join("manifest-v1.json"))?)});
    anyagents_core::learned_memory::atomic_write(
        &directory.join("population-v1.json"),
        &serde_json::to_vec_pretty(&receipt).map_err(std::io::Error::other)?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{
        NewAgent,
        host_migration::{MigrationKind, MigrationStatus},
        learned_memory::sha256,
    };
    #[test]
    fn deploy_population_migrates_archived_and_defers_active_writer() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let archived = store
            .create_agent(NewAgent::new("never-launches", "claude-code"))
            .unwrap();
        store.set_agent_state(&archived.id, "archived").unwrap();
        let active = store
            .create_agent(NewAgent::new("active-writer", "claude-code"))
            .unwrap();
        for agent in [&archived, &active] {
            let old = dir
                .path()
                .join("agents")
                .join(agent.id.as_str())
                .join("config/projects/known/memory");
            fs::create_dir_all(&old).unwrap();
            fs::write(old.join("MEMORY.md"), format!("exact {}", agent.name)).unwrap();
        }
        let _live = store.try_lock_agent(&active.id).unwrap();
        on_start_from(&store, &dir.path().join("owners")).unwrap();
        let root = |id: &AgentId| dir.path().join("agents").join(id.as_str());
        assert_eq!(
            fs::read(root(&archived.id).join("memory/MEMORY.md")).unwrap(),
            b"exact never-launches"
        );
        assert!(
            !root(&archived.id)
                .join("config/projects/known/memory/MEMORY.md")
                .exists()
        );
        assert_eq!(
            fs::read(root(&active.id).join("config/projects/known/memory/MEMORY.md")).unwrap(),
            b"exact active-writer"
        );
        assert!(!root(&active.id).join("memory/MEMORY.md").exists());
        let receipt: serde_json::Value = serde_json::from_slice(
            &fs::read(dir.path().join("host-memory-migration/population-v1.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["population_count"], 2);
        assert_eq!(
            receipt["stopped_agents"],
            serde_json::json!([archived.id.to_string()])
        );
        assert_eq!(
            receipt["deferred_active_or_busy_agents"],
            serde_json::json!([active.id.to_string()])
        );
    }

    #[test]
    fn daemon_launch_migrates_facts_stages_state_and_never_hands_off_native_raw() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("known-owner", "sylphx"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        let config = root.join("config");
        let old = config.join(".claude/projects/known-project/memory");
        fs::create_dir_all(&old).unwrap();
        let bytes = b"---\ntype: reference\n---\n[Fact](fact.md)\n";
        fs::write(old.join("MEMORY.md"), bytes).unwrap();
        fs::write(old.join("fact.md"), b"exact fact bytes\n").unwrap();
        let raw = b"{\"role\":\"tool\",\"output\":\"native history\"}\n";
        fs::write(config.join("sylphx-transcript.jsonl"), raw).unwrap();
        fs::write(config.join("sylphx-goal.json"), b"{\"status\":\"active\"}").unwrap();
        before_launch_from(dir.path(), &agent.id, &dir.path().join("owners")).unwrap();
        assert_eq!(fs::read(root.join("memory/MEMORY.md")).unwrap(), bytes);
        assert_eq!(
            fs::read(root.join("memory/fact.md")).unwrap(),
            b"exact fact bytes\n"
        );
        assert_eq!(
            fs::read(config.join("sylphx-transcript.jsonl")).unwrap(),
            raw
        );
        let migration = HostMigration::open(dir.path()).unwrap();
        let transcript = migration
            .manifest
            .entries
            .iter()
            .find(|e| e.kind == MigrationKind::RawTranscript)
            .unwrap();
        assert_eq!(transcript.status, MigrationStatus::PendingRawHandoff);
        assert_eq!(
            transcript.source_fingerprint.as_ref().unwrap().sha256,
            sha256(raw)
        );
        assert!(!transcript.destination.as_ref().unwrap().exists());
        let state = migration
            .manifest
            .entries
            .iter()
            .find(|e| e.kind == MigrationKind::NativeState)
            .unwrap();
        assert_eq!(state.status, MigrationStatus::RetainedActiveWriter);
        assert_eq!(
            fs::read(state.destination.as_ref().unwrap()).unwrap(),
            fs::read(&state.source).unwrap()
        );
        fs::write(root.join("memory/MEMORY.md"), b"new host-owned fact").unwrap();
        before_launch_from(dir.path(), &agent.id, &dir.path().join("owners")).unwrap();
        assert_eq!(
            fs::read(root.join("memory/MEMORY.md")).unwrap(),
            b"new host-owned fact"
        );
        assert_eq!(
            fs::read(config.join("sylphx-transcript.jsonl")).unwrap(),
            raw
        );
    }
}
