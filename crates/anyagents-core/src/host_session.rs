//! Host-owned logical sessions. Vendor IDs are aliases learned after dispatch,
//! not authority to refresh memory. Call lifecycle operations under the host's
//! per-agent lifecycle lock; no marker/mtime/compaction heuristic starts a session.
use crate::agent_tree::AgentTree;
use crate::beneath::Dir;
use crate::learned_memory::{MemoryPin, pin_session_from_source, read_pin, sha256};
use anyagents_harness::normalize_harness_name;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, ffi::OsStr, fs, io, path::Path};

// `sessions/` is seat-admitted like `config/` (0o2770): the seat can put a
// link, a FIFO or a forged registry in it. Everything here works from the
// opened agent root and never follows a link below it (ADR 0010 D3), and the
// registry is untrusted input: only well-formed generations are accepted.
fn tree(root: &Path) -> io::Result<AgentTree> {
    // fs-ok: the agent root itself, which the server made and chose
    fs::create_dir_all(root)?;
    AgentTree::open(root)
}

fn sessions(root: &Path) -> io::Result<Dir> {
    let sessions = tree(root)?
        .root()
        .create_dir(OsStr::new("sessions"), 0o2770)?;
    // Keep 0o2770 with the existing seat group instead of clamping to a
    // private 0o700. The daemon stays owner and keeps full access through
    // owner bits, while the seat principal can enter the directory for
    // harness resume. Clamping to 0o700 here ran on every active-turn call
    // and caused seat-side resume to fail with Permission denied.
    let _ = sessions.set_mode(0o2770);
    Ok(sessions)
}

/// The agent's `config/`, when it is a plain directory.
fn config(root: &Path) -> io::Result<Option<Dir>> {
    match tree(root)?.dir(Path::new("config")) {
        Ok(dir) => Ok(Some(dir)),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::InvalidData
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

// Serialize host transitions and late vendor aliases across daemon threads and
// restarts. This lock is host-only, outside the writable learned-memory tree.
fn lock(root: &Path) -> io::Result<fs::File> {
    let file = sessions(root)?.open_or_create(OsStr::new("registry.lock"), 0o660)?;
    file.lock()?;
    Ok(file)
}

/// The largest registry the server parses.
const REGISTRY_LIMIT: u64 = 1024 * 1024;

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
    match sessions(root)?.read(OsStr::new("registry.json"), REGISTRY_LIMIT) {
        Ok(bytes) => {
            let mut registry: Registry =
                serde_json::from_slice(&bytes).map_err(io::Error::other)?;
            // Every generation names `sessions/<sha256>` and `retired-<gen>`:
            // only a UUID is one (a seat can write this file).
            if registry
                .active
                .iter()
                .chain(registry.aliases.values())
                .chain(
                    registry
                        .pending_reset
                        .iter()
                        .map(|pending| &pending.session),
                )
                .any(|session| uuid::Uuid::parse_str(&session.generation).is_err())
            {
                return Err(io::Error::other("invalid host session registry"));
            }
            // Renaming a selector is not a harness switch: retain the generation,
            // pinned memory and vendor aliases when an agent row is migrated.
            for session in registry
                .active
                .iter_mut()
                .chain(registry.aliases.values_mut())
                .chain(
                    registry
                        .pending_reset
                        .iter_mut()
                        .map(|pending| &mut pending.session),
                )
            {
                session.harness = normalize_harness_name(&session.harness).to_string();
            }
            let mut aliases: BTreeMap<String, Session> = BTreeMap::new();
            for (key, session) in registry.aliases {
                let (harness, vendor_id): (String, String) =
                    serde_json::from_str(&key).map_err(io::Error::other)?;
                let key = alias(&harness, &vendor_id)?;
                if aliases
                    .get(&key)
                    .is_some_and(|old| old.generation != session.generation)
                {
                    return Err(io::Error::other(
                        "conflicting legacy vendor session aliases",
                    ));
                }
                aliases.insert(key, session);
            }
            registry.aliases = aliases;
            Ok(registry)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Registry::default()),
        Err(e) => Err(e),
    }
}
fn write(root: &Path, registry: &Registry) -> io::Result<()> {
    sessions(root)?.write_atomic(
        OsStr::new("registry.json"),
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
    serde_json::to_string(&(normalize_harness_name(harness), vendor_id)).map_err(io::Error::other)
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

/// A native resume pointer `name` in `dir`, read without following a link;
/// `None` when absent.
fn marker_bytes(dir: Option<&Dir>, name: &str) -> io::Result<Option<Vec<u8>>> {
    let Some(dir) = dir else {
        return Ok(None);
    };
    match dir.read(OsStr::new(name), 4096) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => Err(io::Error::other(
            "native resume pointer is not a regular file of at most 4096 bytes",
        )),
        Err(error) => Err(error),
    }
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
        // Both ends are opened directories below the agent root, and the
        // move is a renameat between them: a link the seat put at
        // `retired-<gen>` or at the pointer is refused, never followed.
        let config = config(root)?;
        let sessions = sessions(root)?;
        let retired_name = format!("retired-{}", pending.session.generation);
        let existing_retired = match sessions.dir(Path::new(&retired_name)) {
            Ok(dir) => Some(dir),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let old = marker_bytes(config.as_ref(), name)?;
        let saved = marker_bytes(existing_retired.as_ref(), name)?;
        let retired = match (old, saved) {
            (Some(bytes), None) if sha256(&bytes) == *expected => {
                let retired = match existing_retired {
                    Some(dir) => dir,
                    None => sessions.create_dir(OsStr::new(&retired_name), 0o2770)?,
                };
                sessions.file().sync_all()?;
                let config = config
                    .as_ref()
                    .ok_or_else(|| io::Error::other("native resume pointer has no config"))?;
                config.rename_to(OsStr::new(name), &retired, OsStr::new(name))?;
                retired
            }
            (None, Some(bytes)) if sha256(&bytes) == *expected => existing_retired
                .ok_or_else(|| io::Error::other("retired resume pointer vanished"))?,
            _ => {
                return Err(io::Error::other(
                    "native resume pointer changed during session reset",
                ));
            }
        };
        if marker_bytes(Some(&retired), name)?.is_none_or(|bytes| sha256(&bytes) != *expected) {
            return Err(io::Error::other(
                "retired resume pointer verification failed",
            ));
        }
        retired.file().sync_all()?;
        if let Some(config) = &config {
            config.file().sync_all()?;
        }
    }
    retire_session_errors(root)?;
    registry.active = Some(pending.session);
    registry.pending_reset = None;
    write(root, registry)
}
fn retire_session_errors(root: &Path) -> io::Result<()> {
    // Never through a link: only a regular file in a plain config directory.
    match config(root)? {
        Some(config) => config
            .remove_file(OsStr::new(".anyagents-last-error.json"))
            .map(|_| ()),
        None => Ok(()),
    }
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

/// Prepare a configured harness before rendering or dispatch. A harness change
/// rotates the active generation and retires the destination's native resume
/// pointer, preserving all old pins, aliases and transcripts. First dispatch
/// still owns allocation when there is no active session.
pub fn prepare_harness_with_source(
    root: &Path,
    harness: &str,
    marker: Option<&str>,
    source: Option<&Path>,
) -> io::Result<Option<HostSession>> {
    let harness = normalize_harness_name(harness);
    if harness.trim().is_empty() || marker.is_some_and(|name| !single_filename(name)) {
        return Err(io::Error::other("invalid harness session identity"));
    }
    let _lock = lock(root)?;
    let mut registry = read_ready(root)?;
    let Some(old) = registry.active.as_ref() else {
        return Ok(None);
    };
    let previous = materialize(root, old)?;
    if old.harness == harness {
        return Ok(Some(previous));
    }
    // This marker belongs to the destination harness's historical session,
    // never to the outgoing generation. Keep its existing alias unchanged.
    let retired_marker = marker
        .map(|name| {
            marker_bytes(config(root)?.as_ref(), name)
                .map(|bytes| bytes.map(|bytes| (name.to_owned(), sha256(&bytes))))
        })
        .transpose()?
        .flatten();
    start_reset(root, &mut registry, harness, retired_marker, source).map(Some)
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
    let harness = normalize_harness_name(harness);
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
            marker_bytes(config(root)?.as_ref(), name).map(|bytes| bytes.map(|bytes| (name, bytes)))
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
    let retired_marker = old_marker.map(|(name, bytes)| (name.to_owned(), sha256(&bytes)));
    start_reset(root, &mut registry, harness, retired_marker, source)
}

fn start_reset(
    root: &Path,
    registry: &mut Registry,
    harness: &str,
    marker: Option<(String, String)>,
    source: Option<&Path>,
) -> io::Result<HostSession> {
    let session = Session {
        generation: uuid::Uuid::now_v7().to_string(),
        harness: harness.to_owned(),
    };
    pin_session_from_source(root, &session.generation, source)?;
    registry.pending_reset = Some(PendingReset { session, marker });
    write(root, registry)?;
    complete_reset(root, registry)?;
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
    let harness = normalize_harness_name(harness);
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
    let harness = normalize_harness_name(harness);
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
    let harness = normalize_harness_name(harness);
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

/// Read an already-bound vendor alias without changing the active generation.
/// Legacy-pointer adoption uses this positive evidence to avoid reviving a
/// historical session after an explicit reset.
pub fn bound_vendor(
    root: &Path,
    harness: &str,
    vendor_id: &str,
) -> io::Result<Option<HostSession>> {
    let registry = read(root)?;
    if registry.pending_reset.is_some() {
        return Err(io::Error::other(
            "host session reset pending; retry lifecycle",
        ));
    }
    registry
        .aliases
        .get(&alias(harness, vendor_id)?)
        .map(|session| materialize(root, session))
        .transpose()
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

    /// `sessions/` is the seat's to write. A forged pending reset whose
    /// `retired-<gen>` is a link to another directory never makes the server
    /// move the seat's file there; a linked lock or registry is never opened
    /// through; a generation that is not a UUID is refused.
    #[cfg(unix)]
    #[test]
    fn a_forged_registry_and_planted_links_move_nothing_outside() {
        use std::os::unix::fs::symlink;
        let dir = crate::test_tempdir();
        let root = dir.path().join("agent");
        let outside = dir.path().join("outside");
        fs::create_dir_all(root.join("config")).unwrap();
        fs::create_dir_all(&outside).unwrap();
        // The seat's file, which the forged reset would move outside.
        fs::write(root.join("config/.bashrc"), b"seat payload").unwrap();
        let generation = uuid::Uuid::now_v7().to_string();
        pin_session_from_source(&root, &generation, None).unwrap();
        let registry = serde_json::json!({
            "active": null,
            "aliases": {},
            "pending_reset": {
                "session": {"generation": generation, "harness": "codex"},
                "marker": [".bashrc", sha256(b"seat payload")],
            },
        });
        fs::write(root.join("sessions/registry.json"), registry.to_string()).unwrap();
        symlink(
            &outside,
            root.join(format!("sessions/retired-{generation}")),
        )
        .unwrap();
        assert!(recover_pending(&root).is_err());
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        assert_eq!(
            fs::read(root.join("config/.bashrc")).unwrap(),
            b"seat payload"
        );

        // A linked registry is never parsed from its target.
        fs::remove_file(root.join("sessions/registry.json")).unwrap();
        fs::write(outside.join("target.json"), registry.to_string()).unwrap();
        symlink(
            outside.join("target.json"),
            root.join("sessions/registry.json"),
        )
        .unwrap();
        assert!(active(&root).is_err());
        fs::remove_file(root.join("sessions/registry.json")).unwrap();

        // A linked lock is never created or opened through.
        fs::remove_file(root.join("sessions/registry.lock")).ok();
        symlink(
            outside.join("created-by-lock"),
            root.join("sessions/registry.lock"),
        )
        .unwrap();
        assert!(recover_pending(&root).is_err());
        assert!(!outside.join("created-by-lock").exists());
        fs::remove_file(root.join("sessions/registry.lock")).unwrap();

        // A generation that is not a UUID names nothing.
        fs::write(
            root.join("sessions/registry.json"),
            serde_json::json!({
                "active": {"generation": "../../outside", "harness": "codex"},
                "aliases": {},
            })
            .to_string(),
        )
        .unwrap();
        assert!(active(&root).is_err());
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 1);
    }
    #[cfg(unix)]
    #[test]
    fn active_turn_keeps_sessions_seat_admitted() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = crate::test_tempdir();
        let root = dir.path();
        let sessions = root.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        fs::set_permissions(&sessions, fs::Permissions::from_mode(0o2770)).unwrap();
        let _ = ensure_active(root, "spiron").unwrap();
        let mode = fs::metadata(&sessions).unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            mode, 0o2770,
            "active turn keeps sessions seat-admitted, never clamps to 0o700"
        );
        let dir2 = crate::test_tempdir();
        let root2 = dir2.path();
        let _ = ensure_active(root2, "spiron").unwrap();
        let mode2 = fs::metadata(root2.join("sessions"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(mode2, 0o2770, "fresh sessions dir is created seat-admitted");
    }

    #[test]
    fn legacy_registry_names_preserve_generation_memory_and_vendor_aliases() {
        for legacy in ["sylphx", "sylphx-agent", "sylphx_agent", "sylphx/default"] {
            let dir = crate::test_tempdir();
            let root = dir.path();
            memory(root, "original pin");
            let original = ensure_active(root, "spiron").unwrap();
            bind_vendor(root, "spiron", "legacy-vendor").unwrap();
            let mut registry = read(root).unwrap();
            registry.active.as_mut().unwrap().harness = legacy.into();
            let mut session = registry
                .aliases
                .remove(&alias("spiron", "legacy-vendor").unwrap())
                .unwrap();
            session.harness = legacy.into();
            registry.aliases.insert(
                serde_json::to_string(&(legacy, "legacy-vendor")).unwrap(),
                session,
            );
            write(root, &registry).unwrap();
            let bytes = fs::read(root.join("sessions/registry.json")).unwrap();
            memory(root, "must not repin");
            assert_eq!(active(root).unwrap().unwrap(), original);
            assert_eq!(ensure_active(root, legacy).unwrap(), original);
            assert_eq!(
                prepare_harness_with_source(
                    root,
                    "spiron",
                    None,
                    Some(Path::new("memory/MEMORY.md"))
                )
                .unwrap()
                .unwrap(),
                original
            );
            assert_eq!(
                bound_vendor(root, "spiron", "legacy-vendor")
                    .unwrap()
                    .unwrap(),
                original
            );
            assert_eq!(
                fs::read(root.join("sessions/registry.json")).unwrap(),
                bytes
            );
            assert_eq!(
                resume_vendor(root, legacy, "legacy-vendor").unwrap(),
                original
            );
        }
    }
    #[test]
    fn conflicting_legacy_vendor_aliases_fail_without_changing_the_registry() {
        let dir = crate::test_tempdir();
        let root = dir.path();
        ensure_active(root, "spiron").unwrap();
        bind_vendor(root, "spiron", "vendor-id").unwrap();
        let mut registry = read(root).unwrap();
        let mut conflicting = registry.active.clone().unwrap();
        conflicting.harness = "sylphx".into();
        conflicting.generation = uuid::Uuid::now_v7().to_string();
        registry.aliases.insert(
            serde_json::to_string(&("sylphx", "vendor-id")).unwrap(),
            conflicting,
        );
        write(root, &registry).unwrap();
        let before = fs::read(root.join("sessions/registry.json")).unwrap();
        assert!(active(root).is_err());
        assert!(ensure_active(root, "spiron").is_err());
        assert_eq!(
            fs::read(root.join("sessions/registry.json")).unwrap(),
            before
        );
    }

    #[test]
    fn preparing_harness_switches_once_and_preserves_history_and_aliases() {
        let dir = crate::test_tempdir();
        let root = dir.path();
        let source = Some(Path::new("memory/MEMORY.md"));
        assert!(
            prepare_harness_with_source(root, "opencode", None, source)
                .unwrap()
                .is_none()
        );
        fs::create_dir_all(root.join("config")).unwrap();
        memory(root, "original fact");
        let original = ensure_active(root, "opencode").unwrap();
        bind_vendor(root, "opencode", "old-opencode").unwrap();
        fs::write(root.join("config/opencode-session-id"), "old-opencode\n").unwrap();
        fs::write(root.join("config/history.jsonl"), "retained transcript").unwrap();
        memory(root, "spiron fact");
        let switched = prepare_harness_with_source(root, "spiron", None, source)
            .unwrap()
            .unwrap();
        assert_ne!(switched.generation, original.generation);
        assert_eq!(switched.harness, "spiron");
        assert!(switched.memory.section.contains("spiron fact"));
        memory(root, "roundtrip fact");
        let roundtrip =
            prepare_harness_with_source(root, "opencode", Some("opencode-session-id"), source)
                .unwrap()
                .unwrap();
        assert_ne!(roundtrip.generation, switched.generation);
        assert!(!root.join("config/opencode-session-id").exists());
        let retired = root.join(format!(
            "sessions/retired-{}/opencode-session-id",
            roundtrip.generation
        ));
        assert_eq!(fs::read_to_string(retired).unwrap(), "old-opencode\n");
        assert_eq!(
            fs::read_to_string(root.join("config/history.jsonl")).unwrap(),
            "retained transcript"
        );
        memory(root, "must not repin");
        assert_eq!(
            prepare_harness_with_source(root, "opencode", Some("opencode-session-id"), source)
                .unwrap()
                .unwrap(),
            roundtrip
        );
        assert_eq!(active(root).unwrap().unwrap(), roundtrip);
        assert_eq!(
            resume_vendor(root, "opencode", "old-opencode").unwrap(),
            original
        );
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
        crate::learned_memory::atomic_write(&root.join("sessions/registry.json"), &before).unwrap();
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
        let initial = ensure_active(root, "spiron").unwrap();
        bind_vendor(root, "spiron", "id").unwrap();
        let reset = new_session(root, "spiron").unwrap();
        assert_ne!(initial.generation, reset.generation);
        assert!(bind_vendor(root, "spiron", "id").is_err());
        assert_eq!(ensure_active(root, "spiron").unwrap(), reset);
        fs::write(root.join("sessions/registry.json"), b"broken").unwrap();
        assert!(ensure_active(root, "spiron").is_err());
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
        let initial = ensure_active(root, "spiron").unwrap();
        let path = root
            .join("sessions")
            .join(crate::learned_memory::sha256(initial.generation.as_bytes()))
            .join("memory-pin.json");
        fs::remove_file(&path).unwrap();
        memory(root, "new must not leak into old session");
        assert!(ensure_active(root, "spiron").is_err());
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
