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

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{
        NewAgent,
        host_migration::{MigrationKind, MigrationStatus},
        learned_memory::sha256,
    };
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
