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
    if !try_migration_lock(&lock)? {
        record_lock_deferral(data, "before_launch", Some(agent))?;
        return Ok(());
    }
    let mut migration = HostMigration::open(data)?;
    let stopped = HashSet::from([agent.to_string()]);
    migration.discover_selected(&owners, owners_pages, Some(&stopped))?;
    migration.stage_selected(&stopped, Some(&stopped))?;
    Ok(())
}

/// Deploy/startup migration inventories every registered identity, including
/// archived identities that will never launch. Process locks, not SQL state,
/// establish quiescence; live or busy identities are explicitly deferred.
pub(crate) fn on_start(store: &Store) {
    // Migration is maintenance, never a prerequisite for binding host sockets.
    // Failed integrity checks preserve their sources; keep a retryable receipt
    // and allow authorized non-migration operations to remain available.
    if let Err(error) = on_start_from(store, Path::new("/scratch/tmp/owners")) {
        let receipt = serde_json::json!({"version":1,"timestamp_ms":anyagents_core::now_ms(),
            "status":"deferred_error","retryable":true,"error":error.to_string()});
        let path = store
            .data_dir()
            .join("host-memory-migration/startup-error-v1.json");
        if let Err(write_error) = serde_json::to_vec_pretty(&receipt)
            .map_err(std::io::Error::other)
            .and_then(|bytes| anyagents_core::learned_memory::atomic_write(&path, &bytes))
        {
            eprintln!(
                "warning: host memory migration deferred: {error}; receipt write failed: {write_error}"
            );
        } else {
            eprintln!(
                "warning: host memory migration deferred: {error}; receipt: {}",
                path.display()
            );
        }
    }
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
    if !try_migration_lock(&lock)? {
        record_lock_deferral(store.data_dir(), "startup", None)?;
        return Ok(());
    }
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
    let mut failures = Vec::new();
    let mut selected: Vec<_> = registry.iter().map(|a| a.id.to_string()).collect();
    selected.sort();
    for id in selected {
        let selection = HashSet::from([id.clone()]);
        // Never traverse live directories. Even read_dir can fail or race with
        // native writers, and must not stop another owner's launch/startup.
        if stopped.contains(&id)
            && let Err(error) = migration.discover_selected(&owners, owners_pages, Some(&selection))
        {
            failures.push(serde_json::json!({"agent_id":id,"phase":"discovery",
                "retryable":true,"error":error.to_string()}));
            continue;
        }
        if let Err(error) = migration.stage_selected(&stopped, Some(&selection)) {
            failures.push(serde_json::json!({"agent_id":id,"phase":"staging",
                "retryable":true,"error":error.to_string()}));
        }
    }
    let mut statuses = std::collections::BTreeMap::<String, usize>::new();
    for entry in &migration.manifest.entries {
        *statuses.entry(format!("{:?}", entry.status)).or_default() += 1;
    }
    let mut stopped: Vec<_> = stopped.into_iter().collect();
    stopped.sort();
    deferred.sort();
    let receipt = serde_json::json!({"version": 1, "timestamp_ms": anyagents_core::now_ms(),
    "population_count": registry.len(), "stopped_agents": stopped, "deferred_failed_agents": failures,
    "deferred_active_or_busy_agents": deferred, "entry_counts": statuses,
    "manifest_sha256": match fs::read(directory.join("manifest-v1.json")) {
        Ok(bytes) => Some(anyagents_core::learned_memory::sha256(&bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    }});
    anyagents_core::learned_memory::atomic_write(
        &directory.join("population-v1.json"),
        &serde_json::to_vec_pretty(&receipt).map_err(std::io::Error::other)?,
    )?;
    Ok(())
}

/// No maintenance caller waits for another process's potentially long scan.
/// All actual moves still occur under the same exclusive cross-process lock.
fn try_migration_lock(file: &fs::File) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;
    loop {
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        match error.kind() {
            std::io::ErrorKind::WouldBlock => return Ok(false),
            std::io::ErrorKind::Interrupted => continue,
            _ => return Err(error),
        }
    }
}
fn record_lock_deferral(data: &Path, phase: &str, agent: Option<&AgentId>) -> Result<(), Error> {
    let receipt = serde_json::json!({"version":1,"timestamp_ms":anyagents_core::now_ms(),
        "status":"deferred_busy_lock","retryable":true,"phase":phase,"agent_id":agent});
    let name = agent.map_or_else(|| "startup".to_owned(), ToString::to_string);
    anyagents_core::learned_memory::atomic_write(
        &data
            .join("host-memory-migration")
            .join(format!("deferred-{name}-v1.json")),
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
    fn active_append_and_unreadable_bundle_do_not_block_startup_or_other_launches() {
        use std::os::unix::fs::PermissionsExt;
        use std::{
            io::Write,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
        };
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let active = store.create_agent(NewAgent::new("live", "sylphx")).unwrap();
        let selected = store
            .create_agent(NewAgent::new("selected", "sylphx"))
            .unwrap();
        let root = |id: &AgentId| dir.path().join("agents").join(id.as_str());
        let active_config = root(&active.id).join("config");
        let memory = active_config.join("projects/live/memory");
        fs::create_dir_all(&memory).unwrap();
        let unreadable = memory.join("MEMORY.md");
        fs::write(&unreadable, b"writer-owned facts").unwrap();
        // If preflight opens this unrelated live file, the real startup fails.
        let owners = dir.path().join("owners");
        let active_page = owners.join(&active.name);
        fs::create_dir_all(&active_page).unwrap();
        fs::write(active_page.join("facts.md"), b"live owner history").unwrap();
        let raw = active_config.join("sylphx-transcript.jsonl");
        let mut writer = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&raw)
            .unwrap();
        writer
            .write_all(b"{\"role\":\"user\",\"content\":\"before\"}\n")
            .unwrap();
        // Retain real prior discovery entries so the deferred-status assertions
        // below cannot pass vacuously when live discovery is correctly skipped.
        HostMigration::open(dir.path())
            .unwrap()
            .discover(
                &[AgentOwnership {
                    id: active.id.clone(),
                    aliases: vec![active.name.clone()],
                }],
                &owners,
            )
            .unwrap();
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000)).unwrap();
        fs::set_permissions(&memory, fs::Permissions::from_mode(0o000)).unwrap();
        fs::set_permissions(&active_page, fs::Permissions::from_mode(0o000)).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let done = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut n = 0;
            while !done.load(Ordering::Relaxed) {
                writer
                    .write_all(b"{\"role\":\"assistant\",\"content\":\"live append\"}\n")
                    .unwrap();
                n += 1;
                std::thread::yield_now();
            }
            n
        });
        let live_guard = store.try_lock_agent(&active.id).unwrap();
        let startup = on_start_from(&store, &dir.path().join("owners"));
        let launch = before_launch_from(dir.path(), &selected.id, &dir.path().join("owners"));
        stop.store(true, Ordering::Relaxed);
        let appends = thread.join().unwrap();
        fs::set_permissions(&memory, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&active_page, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o600)).unwrap();
        drop(live_guard);
        startup.unwrap();
        launch.unwrap();
        assert!(appends > 0);
        let migration = HostMigration::open(dir.path()).unwrap();
        let active_entries: Vec<_> = migration
            .manifest
            .entries
            .iter()
            .filter(|e| e.agent_id.as_deref() == Some(active.id.as_str()))
            .collect();
        assert!(active_entries.len() >= 3);
        for entry in active_entries {
            assert_eq!(entry.status, MigrationStatus::RetainedActiveWriter);
            assert!(entry.source_fingerprint.is_none());
            assert!(entry.reason.as_ref().unwrap().contains("deferred"));
        }
        assert!(!root(&active.id).join("memory/MEMORY.md").exists());
        assert!(fs::metadata(&raw).unwrap().len() > 0);
    }

    #[test]
    fn discovery_failure_is_deferred_per_stopped_owner_and_retried() {
        use std::os::unix::fs::PermissionsExt;
        for block_page in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path().join("state.db")).unwrap();
            let bad = store
                .create_agent(NewAgent::new("unreadable", "sylphx"))
                .unwrap();
            let good = store
                .create_agent(NewAgent::new("healthy", "sylphx"))
                .unwrap();
            let root = |id: &AgentId| dir.path().join("agents").join(id.as_str());
            for agent in [&bad, &good] {
                let memory = root(&agent.id).join("config/projects/known/memory");
                fs::create_dir_all(&memory).unwrap();
                fs::write(memory.join("MEMORY.md"), agent.name.as_bytes()).unwrap();
            }
            let owners = dir.path().join("owners");
            let page = owners.join(&bad.name);
            fs::create_dir_all(&page).unwrap();
            fs::write(page.join("history.md"), b"preserved owner history").unwrap();
            let blocked = if block_page {
                page
            } else {
                root(&bad.id).join("config/projects")
            };
            fs::set_permissions(&blocked, fs::Permissions::from_mode(0o000)).unwrap();
            // Permission refusal is meaningful only under an unprivileged UID.
            // The source fixture is still preserved and tested below as root.
            let permission_denied = fs::read_dir(&blocked)
                .is_err_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied);
            if unsafe { libc::geteuid() } != 0 {
                assert!(
                    permission_denied,
                    "fixture did not deny directory traversal"
                );
            }
            on_start_from(&store, &owners).unwrap();
            before_launch_from(dir.path(), &good.id, &owners).unwrap();
            fs::set_permissions(&blocked, fs::Permissions::from_mode(0o700)).unwrap();
            assert_eq!(
                fs::read(root(&good.id).join("memory/MEMORY.md")).unwrap(),
                b"healthy"
            );
            let receipt_path = dir.path().join("host-memory-migration/population-v1.json");
            if permission_denied {
                let receipt: serde_json::Value =
                    serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
                let failures = receipt["deferred_failed_agents"].as_array().unwrap();
                assert_eq!(failures.len(), 1, "{receipt}");
                assert_eq!(failures[0]["agent_id"], bad.id.as_str());
                assert_eq!(failures[0]["phase"], "discovery");
                assert_eq!(failures[0]["retryable"], true);
                assert_eq!(
                    fs::read(root(&bad.id).join("config/projects/known/memory/MEMORY.md")).unwrap(),
                    b"unreadable"
                );
                assert!(!root(&bad.id).join("memory/MEMORY.md").exists());
            }
            on_start_from(&store, &owners).unwrap();
            assert_eq!(
                fs::read(root(&bad.id).join("memory/MEMORY.md")).unwrap(),
                b"unreadable"
            );
            let receipt: serde_json::Value =
                serde_json::from_slice(&fs::read(receipt_path).unwrap()).unwrap();
            assert_eq!(receipt["deferred_failed_agents"], serde_json::json!([]));
        }
    }

    #[test]
    fn busy_global_lock_defers_launch_and_startup_without_moving_sources() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("selected", "sylphx"))
            .unwrap();
        let memory = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("config/projects/known/memory");
        fs::create_dir_all(&memory).unwrap();
        fs::write(memory.join("MEMORY.md"), b"serialized facts").unwrap();
        let directory = dir.path().join("host-memory-migration");
        fs::create_dir_all(&directory).unwrap();
        let held = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("migration.lock"))
            .unwrap();
        held.lock().unwrap();
        let path = dir.path().to_owned();
        let id = agent.id.clone();
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let launch = before_launch_from(&path, &id, &path.join("owners"));
            let store = Store::open(path.join("state.db")).unwrap();
            let startup = on_start_from(&store, &path.join("owners"));
            send.send((launch, startup)).unwrap();
        });
        let completed_while_held = receive.recv_timeout(std::time::Duration::from_secs(3));
        let unchanged_while_held =
            memory.join("MEMORY.md").exists() && !directory.join("manifest-v1.json").exists();
        drop(held); // Always release before assertion/join, including a regression.
        worker.join().unwrap();
        let (launch, startup) =
            completed_while_held.expect("maintenance waited for the held global lock");
        launch.unwrap();
        startup.unwrap();
        assert!(unchanged_while_held);
        for name in ["startup".to_owned(), agent.id.to_string()] {
            let receipt: serde_json::Value = serde_json::from_slice(
                &fs::read(directory.join(format!("deferred-{name}-v1.json"))).unwrap(),
            )
            .unwrap();
            assert_eq!(receipt["status"], "deferred_busy_lock");
            assert_eq!(receipt["retryable"], true);
        }
        before_launch_from(dir.path(), &agent.id, &dir.path().join("owners")).unwrap();
        assert_eq!(
            fs::read(
                dir.path()
                    .join("agents")
                    .join(agent.id.as_str())
                    .join("memory/MEMORY.md")
            )
            .unwrap(),
            b"serialized facts"
        );
        assert!(!memory.join("MEMORY.md").exists());
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
