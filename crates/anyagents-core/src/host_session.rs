//! Host-owned logical sessions. Vendor IDs are aliases learned after dispatch,
//! not authority to refresh memory. Call lifecycle operations under the host's
//! per-agent lifecycle lock; no marker/mtime/compaction heuristic starts a session.
use crate::learned_memory::{MemoryPin, atomic_write, pin_session_from_source, read_pin, sha256};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    path::Path,
};

// Serialize host transitions and late vendor aliases across daemon threads and
// restarts. This lock is host-only, outside the writable learned-memory tree.
fn lock(root: &Path) -> io::Result<fs::File> {
    let sessions = root.join("sessions");
    fs::create_dir_all(&sessions)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = fs::set_permissions(&sessions, fs::Permissions::from_mode(0o700));
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(sessions.join("registry.lock"))?;
    file.lock()?;
    Ok(file)
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_reset: Option<PendingReset>,
}
#[derive(Clone, Serialize, Deserialize)]
struct PendingReset {
    session: Session,
    marker: Option<(String, String)>,
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

/// Read-only renderer path. No session allocation, MEMORY.md read, or repin.
pub fn active(root: &Path) -> io::Result<Option<HostSession>> {
    let registry = read(root)?;
    if registry.pending_reset.is_some() {
        return Err(io::Error::other(
            "host session reset pending; retry lifecycle",
        ));
    }
    registry
        .active
        .as_ref()
        .map(|session| materialize(root, session))
        .transpose()
}

fn marker_bytes(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let file = match crate::raw_segment::open_readonly(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(io::Error::other("native resume pointer exceeds 4096 bytes"));
    }
    Ok(Some(bytes))
}

fn single_filename(name: &str) -> bool {
    !name.is_empty()
        && Path::new(name).components().count() == 1
        && matches!(
            Path::new(name).components().next(),
            Some(std::path::Component::Normal(_))
        )
}

fn complete_reset(root: &Path, registry: &mut Registry) -> io::Result<()> {
    let Some(pending) = registry.pending_reset.clone() else {
        return Ok(());
    };
    if uuid::Uuid::parse_str(&pending.session.generation).is_err()
        || pending.marker.as_ref().is_some_and(|(name, digest)| {
            !single_filename(name)
                || digest.len() != 64
                || !digest.bytes().all(|b| b.is_ascii_hexdigit())
        })
    {
        return Err(io::Error::other("invalid pending session reset identity"));
    }
    // The pin was durable before the intent. Recovery never rereads live memory.
    materialize(root, &pending.session)?;
    if let Some((name, expected)) = &pending.marker {
        let source = root.join("config").join(name);
        let retired_dir = root
            .join("sessions")
            .join(format!("retired-{}", pending.session.generation));
        let retired = retired_dir.join(name);
        let old = marker_bytes(&source)?;
        let saved = marker_bytes(&retired)?;
        match (old, saved) {
            (Some(bytes), None) if sha256(&bytes) == *expected => {
                fs::create_dir_all(&retired_dir)?;
                fs::File::open(root.join("sessions"))?.sync_all()?;
                fs::rename(&source, &retired)?;
            }
            (None, Some(bytes)) if sha256(&bytes) == *expected => (),
            _ => {
                return Err(io::Error::other(
                    "native resume pointer changed during session reset",
                ));
            }
        }
        if marker_bytes(&retired)?.is_none_or(|bytes| sha256(&bytes) != *expected) {
            return Err(io::Error::other(
                "retired resume pointer verification failed",
            ));
        }
        fs::File::open(&retired_dir)?.sync_all()?;
        fs::File::open(root.join("config"))?.sync_all()?;
    }
    registry.active = Some(pending.session);
    registry.pending_reset = None;
    write(root, registry)
}
fn read_ready(root: &Path) -> io::Result<Registry> {
    let mut registry = read(root)?;
    complete_reset(root, &mut registry)?;
    Ok(registry)
}
/// Resume only a previously durable reset intent, without allocating or repinning.
/// Caller holds the host lifecycle/process admission locks before launch.
pub fn recover_pending(root: &Path) -> io::Result<()> {
    let _lock = lock(root)?;
    read_ready(root).map(|_| ())
}

/// Explicit reset of a quiescent agent. Preserve old pins, vendor aliases and
/// raw/vendor state; retire only the small native resume pointer. The durable
/// intent makes either side of the pointer rename recoverable after a crash.
/// Caller must hold both lifecycle and vendor-process admission locks.
pub fn reset_session(root: &Path, harness: &str, marker: Option<&str>) -> io::Result<HostSession> {
    reset_session_with_source(root, harness, marker, Some(Path::new("memory/MEMORY.md")))
}

/// Explicit reset with caller-selected prompt-memory source; None disables it.
pub fn reset_session_with_source(
    root: &Path,
    harness: &str,
    marker: Option<&str>,
    source: Option<&Path>,
) -> io::Result<HostSession> {
    if harness.trim().is_empty() {
        return Err(io::Error::other("empty session harness"));
    }
    if marker.is_some_and(|name| !single_filename(name)) {
        return Err(io::Error::other("invalid native resume pointer filename"));
    }
    let _lock = lock(root)?;
    let mut registry = read(root)?;
    if registry.pending_reset.is_some() {
        complete_reset(root, &mut registry)?;
        return materialize(root, registry.active.as_ref().unwrap());
    }
    let old_marker = marker
        .map(|name| {
            marker_bytes(&root.join("config").join(name))
                .map(|bytes| bytes.map(|bytes| (name, bytes)))
        })
        .transpose()?
        .flatten();
    if let (Some(old), Some((_, bytes))) = (&registry.active, &old_marker) {
        if old.harness != harness {
            return Err(io::Error::other(
                "reset harness differs from active session",
            ));
        }
        materialize(root, old)?;
        let vendor_id = std::str::from_utf8(bytes).map_err(io::Error::other)?.trim();
        if !vendor_id.is_empty() {
            let key = alias(harness, vendor_id)?;
            if registry
                .aliases
                .get(&key)
                .is_some_and(|bound| bound.generation != old.generation)
            {
                return Err(io::Error::other(
                    "native resume pointer belongs to another generation",
                ));
            }
            registry.aliases.insert(key, old.clone());
        }
    }
    let session = Session {
        generation: uuid::Uuid::now_v7().to_string(),
        harness: harness.to_owned(),
    };
    pin_session_from_source(root, &session.generation, source)?;
    registry.pending_reset = Some(PendingReset {
        session,
        marker: old_marker.map(|(name, bytes)| (name.to_owned(), sha256(&bytes))),
    });
    write(root, &registry)?;
    complete_reset(root, &mut registry)?;
    materialize(root, registry.active.as_ref().unwrap())
}

/// Allocate before first dispatch only when there is no host session yet.
/// A different configured harness alone cannot silently discard the saved pin.
pub fn ensure_active(root: &Path, harness: &str) -> io::Result<HostSession> {
    ensure_active_with_source(root, harness, Some(Path::new("memory/MEMORY.md")))
}

/// Select the source only when allocating the first session, never on resume.
pub fn ensure_active_with_source(
    root: &Path,
    harness: &str,
    source: Option<&Path>,
) -> io::Result<HostSession> {
    let _lock = lock(root)?;
    let registry = read_ready(root)?;
    match registry.active {
        Some(session) if session.harness == harness => materialize(root, &session),
        Some(_) => Err(io::Error::other(
            "host session requires explicit harness switch",
        )),
        None => new_session_unlocked(root, harness, source),
    }
}

/// Only an explicit reset/new-session or deliberate harness-switch lifecycle
/// transition may call this. Ordinary config updates and compaction do not.
pub fn new_session(root: &Path, harness: &str) -> io::Result<HostSession> {
    new_session_with_source(root, harness, Some(Path::new("memory/MEMORY.md")))
}

/// Allocate explicitly from a caller-selected source; None disables pin input.
pub fn new_session_with_source(
    root: &Path,
    harness: &str,
    source: Option<&Path>,
) -> io::Result<HostSession> {
    let _lock = lock(root)?;
    new_session_unlocked(root, harness, source)
}

fn new_session_unlocked(
    root: &Path,
    harness: &str,
    source: Option<&Path>,
) -> io::Result<HostSession> {
    if harness.trim().is_empty() {
        return Err(io::Error::other("empty session harness"));
    }
    let mut registry = read_ready(root)?;
    let session = Session {
        generation: uuid::Uuid::now_v7().to_string(),
        harness: harness.to_owned(),
    };
    // Persist the complete pin before publishing its generation. A crash here
    // can leave an orphan pin but can never expose an unpinned active session.
    let memory = pin_session_from_source(root, &session.generation, source)?;
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
    let _lock = lock(root)?;
    let mut registry = read_ready(root)?;
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
    if registry
        .aliases
        .get(&key)
        .is_some_and(|s| s.generation == session.generation)
    {
        return Ok(result); // frequent health checks never rewrite the registry
    }
    registry.aliases.insert(key, session);
    write(root, &registry)?;
    Ok(result)
}

/// An explicit resume reuses the original pin, including after a harness switch.
pub fn resume_vendor(root: &Path, harness: &str, vendor_id: &str) -> io::Result<HostSession> {
    let _lock = lock(root)?;
    let mut registry = read_ready(root)?;
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
    use crate::learned_memory::pin_session;
    fn memory(root: &Path, text: &str) {
        fs::create_dir_all(root.join("memory")).unwrap();
        fs::write(root.join("memory/MEMORY.md"), text).unwrap();
    }
    #[test]
    fn explicit_reset_retires_only_pointer_and_resumes_prepared_intent_without_repin() {
        let dir = crate::test_tempdir();
        let root = dir.path();
        fs::create_dir_all(root.join("config")).unwrap();
        memory(root, "old pinned fact");
        let old = ensure_active(root, "opencode").unwrap();
        fs::write(root.join("config/opencode-session-id"), b"old-vendor\n").unwrap();
        fs::write(root.join("config/vendor.db"), b"unique native state").unwrap();
        fs::write(root.join("config/history.jsonl"), b"unique raw history").unwrap();
        memory(root, "new pinned fact");
        let reset = reset_session(root, "opencode", Some("opencode-session-id")).unwrap();
        assert_ne!(old.generation, reset.generation);
        assert!(reset.memory.section.contains("new pinned fact"));
        assert_eq!(
            fs::read(root.join("config/vendor.db")).unwrap(),
            b"unique native state"
        );
        assert_eq!(
            fs::read(root.join("config/history.jsonl")).unwrap(),
            b"unique raw history"
        );
        assert!(!root.join("config/opencode-session-id").exists());
        let retired = root
            .join("sessions")
            .join(format!("retired-{}", reset.generation))
            .join("opencode-session-id");
        assert_eq!(fs::read(retired).unwrap(), b"old-vendor\n");
        assert_eq!(resume_vendor(root, "opencode", "old-vendor").unwrap(), old);
        for after_rename in [false, true] {
            fs::write(root.join("config/opencode-session-id"), b"old-vendor\n").unwrap();
            let mut registry = read(root).unwrap();
            let session = Session {
                generation: uuid::Uuid::now_v7().to_string(),
                harness: "opencode".into(),
            };
            memory(root, "prepared fact");
            let expected = pin_session(root, &session.generation).unwrap();
            registry.pending_reset = Some(PendingReset {
                session: session.clone(),
                marker: Some(("opencode-session-id".into(), sha256(b"old-vendor\n"))),
            });
            write(root, &registry).unwrap();
            memory(root, "later edit must not be repinned");
            if after_rename {
                let retired_dir = root
                    .join("sessions")
                    .join(format!("retired-{}", session.generation));
                fs::create_dir_all(&retired_dir).unwrap();
                fs::rename(
                    root.join("config/opencode-session-id"),
                    retired_dir.join("opencode-session-id"),
                )
                .unwrap();
            } else {
                // A changed pointer must refuse without discarding either history.
                fs::write(
                    root.join("config/opencode-session-id"),
                    b"unexpected-vendor\n",
                )
                .unwrap();
                let failure = recover_pending(root).unwrap_err();
                assert!(failure.to_string().contains("resume pointer changed"));
                assert!(read(root).unwrap().pending_reset.is_some());
                fs::write(root.join("config/opencode-session-id"), b"old-vendor\n").unwrap();
            }
            assert!(
                active(root)
                    .unwrap_err()
                    .to_string()
                    .contains("reset pending")
            );
            recover_pending(root).unwrap();
            let actual = active(root).unwrap().unwrap();
            assert_eq!(actual.generation, session.generation);
            assert_eq!(actual.memory, expected);
            assert!(read(root).unwrap().pending_reset.is_none());
            assert_eq!(resume_vendor(root, "opencode", "old-vendor").unwrap(), old);
        }
        let before = fs::read(root.join("sessions/registry.json")).unwrap();
        let mut invalid = read(root).unwrap();
        invalid.pending_reset = Some(PendingReset {
            session: Session {
                generation: uuid::Uuid::now_v7().to_string(),
                harness: "opencode".into(),
            },
            marker: Some(("../foreign".into(), "0".repeat(64))),
        });
        write(root, &invalid).unwrap();
        assert!(
            recover_pending(root)
                .unwrap_err()
                .to_string()
                .contains("invalid pending session reset")
        );
        atomic_write(&root.join("sessions/registry.json"), &before).unwrap();
        assert!(reset_session(root, "opencode", Some("../foreign")).is_err());
        assert_eq!(
            fs::read(root.join("sessions/registry.json")).unwrap(),
            before
        );
    }

    #[test]
    fn late_marker_restart_reset_switch_and_explicit_resume() {
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
        let dir = crate::test_tempdir();
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
    #[test]
    fn caller_selected_source_flows_through_creation_resume_switch_and_reset() {
        let dir = crate::test_tempdir();
        let root = dir.path();
        fs::write(root.join("notes.md"), "selected snapshot").unwrap();
        let selected =
            ensure_active_with_source(root, "test", Some(Path::new("notes.md"))).unwrap();
        assert!(selected.memory.section.contains("selected snapshot"));
        fs::write(root.join("notes.md"), "later snapshot").unwrap();
        assert_eq!(
            ensure_active_with_source(root, "test", None).unwrap(),
            selected
        );
        let disabled = reset_session_with_source(root, "test", None, None).unwrap();
        assert_eq!(
            disabled.memory.section,
            crate::learned_memory::bounded_section("")
        );
        assert_ne!(selected.generation, disabled.generation);
        assert_eq!(
            read_pin(root, &selected.generation).unwrap(),
            selected.memory
        );
        let switched = new_session_with_source(root, "other", Some(Path::new("notes.md"))).unwrap();
        assert!(switched.memory.section.contains("later snapshot"));
        assert_eq!(active(root).unwrap().unwrap(), switched);
        assert!(!root.join("memory").exists());
        fs::write(root.join("bad.md"), [0xff]).unwrap();
        assert_eq!(
            new_session_with_source(root, "other", Some(Path::new("bad.md")))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(active(root).unwrap().unwrap(), switched);
    }
}
