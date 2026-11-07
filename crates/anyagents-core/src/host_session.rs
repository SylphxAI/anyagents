//! Host-owned logical sessions. Vendor IDs are aliases learned after dispatch,
//! not authority to refresh memory. Call lifecycle operations under the host's
//! per-agent lifecycle lock; no marker/mtime/compaction heuristic starts a session.
use crate::learned_memory::{MemoryPin, atomic_write, pin_session, read_pin};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, io, path::Path};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Session {
    generation: String,
    harness: String,
}
#[derive(Default, Serialize, Deserialize)]
struct Registry {
    active: Option<Session>,
    /// Key includes harness because vendor IDs are not globally unique.
    aliases: BTreeMap<String, Session>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostSession {
    pub generation: String,
    pub harness: String,
    pub memory: MemoryPin,
}

fn read(root: &Path) -> io::Result<Registry> {
    match fs::read(root.join("sessions/registry.json")) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Registry::default()),
        Err(e) => Err(e),
    }
}
fn write(root: &Path, registry: &Registry) -> io::Result<()> {
    atomic_write(
        &root.join("sessions/registry.json"),
        &serde_json::to_vec(registry).map_err(io::Error::other)?,
    )
}
fn materialize(root: &Path, session: &Session) -> io::Result<HostSession> {
    Ok(HostSession {
        generation: session.generation.clone(),
        harness: session.harness.clone(),
        memory: read_pin(root, &session.generation)?,
    })
}
fn alias(harness: &str, vendor_id: &str) -> io::Result<String> {
    if harness.is_empty() || vendor_id.trim().is_empty() {
        return Err(io::Error::other("empty vendor session identity"));
    }
    serde_json::to_string(&(harness, vendor_id)).map_err(io::Error::other)
}

/// Allocate before first dispatch only when there is no host session yet.
/// A different configured harness alone cannot silently discard the saved pin.
pub fn ensure_active(root: &Path, harness: &str) -> io::Result<HostSession> {
    let registry = read(root)?;
    match registry.active {
        Some(session) if session.harness == harness => materialize(root, &session),
        Some(_) => Err(io::Error::other(
            "host session requires explicit harness switch",
        )),
        None => new_session(root, harness),
    }
}

/// Only an explicit reset/new-session or deliberate harness-switch lifecycle
/// transition may call this. Ordinary config updates and compaction do not.
pub fn new_session(root: &Path, harness: &str) -> io::Result<HostSession> {
    if harness.trim().is_empty() {
        return Err(io::Error::other("empty session harness"));
    }
    let mut registry = read(root)?;
    let session = Session {
        generation: uuid::Uuid::now_v7().to_string(),
        harness: harness.to_owned(),
    };
    // Persist the complete pin before publishing its generation. A crash here
    // can leave an orphan pin but can never expose an unpinned active session.
    let memory = pin_session(root, &session.generation)?;
    let result = HostSession {
        generation: session.generation.clone(),
        harness: session.harness.clone(),
        memory,
    };
    registry.active = Some(session);
    write(root, &registry)?;
    Ok(result)
}

/// Bind a late vendor marker to the ALREADY pinned generation. A marker change
/// is only a new alias and does not start or refresh the host session.
pub fn bind_vendor(root: &Path, harness: &str, vendor_id: &str) -> io::Result<HostSession> {
    let mut registry = read(root)?;
    let session = registry
        .active
        .clone()
        .filter(|s| s.harness == harness)
        .ok_or_else(|| io::Error::other("no active host session for vendor binding"))?;
    let key = alias(harness, vendor_id)?;
    if let Some(previous) = registry.aliases.get(&key)
        && previous.generation != session.generation
    {
        return Err(io::Error::other(
            "vendor session is already bound to a different generation",
        ));
    }
    let result = materialize(root, &session)?;
    registry.aliases.insert(key, session);
    write(root, &registry)?;
    Ok(result)
}

/// An explicit resume reuses the original pin, including after a harness switch.
pub fn resume_vendor(root: &Path, harness: &str, vendor_id: &str) -> io::Result<HostSession> {
    let mut registry = read(root)?;
    let session = registry
        .aliases
        .get(&alias(harness, vendor_id)?)
        .cloned()
        .ok_or_else(|| io::Error::other("unknown vendor session; cannot refresh on resume"))?;
    let result = materialize(root, &session)?;
    registry.active = Some(session);
    write(root, &registry)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn memory(root: &Path, text: &str) {
        fs::create_dir_all(root.join("memory")).unwrap();
        fs::write(root.join("memory/MEMORY.md"), text).unwrap();
    }
    #[test]
    fn late_marker_restart_reset_switch_and_explicit_resume() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        memory(root, "initial fact");
        let initial = ensure_active(root, "claude-code").unwrap();
        memory(root, "changed after first dispatch");
        assert_eq!(
            bind_vendor(root, "claude-code", "late-id").unwrap(),
            initial
        );
        assert_eq!(
            bind_vendor(root, "claude-code", "changed-marker").unwrap(),
            initial
        );
        assert_eq!(ensure_active(root, "claude-code").unwrap(), initial); // restart/rerender
        assert!(ensure_active(root, "codex").is_err()); // config alone is not reset
        let reset = new_session(root, "claude-code").unwrap();
        assert_ne!(reset.generation, initial.generation);
        assert!(
            reset
                .memory
                .section
                .contains("changed after first dispatch")
        );
        memory(root, "codex-only-new-fact");
        let switched = new_session(root, "codex").unwrap();
        assert!(switched.memory.section.contains("codex-only-new-fact"));
        assert_eq!(
            resume_vendor(root, "claude-code", "late-id").unwrap(),
            initial
        );
        assert_eq!(ensure_active(root, "claude-code").unwrap(), initial);
        assert!(resume_vendor(root, "codex", "late-id").is_err());
    }
    #[test]
    fn corrupt_registry_and_foreign_alias_fail_without_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let initial = ensure_active(root, "sylphx").unwrap();
        bind_vendor(root, "sylphx", "id").unwrap();
        let reset = new_session(root, "sylphx").unwrap();
        assert_ne!(initial.generation, reset.generation);
        assert!(bind_vendor(root, "sylphx", "id").is_err());
        assert_eq!(ensure_active(root, "sylphx").unwrap(), reset);
        fs::write(root.join("sessions/registry.json"), b"broken").unwrap();
        assert!(ensure_active(root, "sylphx").is_err());
        assert_eq!(
            fs::read(root.join("sessions/registry.json")).unwrap(),
            b"broken"
        );
    }
    #[test]
    fn missing_known_pin_refuses_without_reading_changed_memory() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        memory(root, "old");
        let initial = ensure_active(root, "sylphx").unwrap();
        let path = root
            .join("sessions")
            .join(crate::learned_memory::sha256(initial.generation.as_bytes()))
            .join("memory-pin.json");
        fs::remove_file(&path).unwrap();
        memory(root, "new must not leak into old session");
        assert!(ensure_active(root, "sylphx").is_err());
        assert!(!path.exists());
    }
}
