//! Daemon-owned, resumable memory migration. No home-directory discovery, seat
//! ACL repair, Codex database access, symlinks, or raw-payload SQLite copies.
//! PR1 stages native state and indexes active JSONL in place. Raw rename is an
//! explicit PR2 operation requiring both writer-stop and host-port selection.
use crate::{
    AgentId,
    learned_memory::{atomic_write, sha256},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

const NATIVE_STATE: &[&str] = &[
    "sylphx-goal.json",
    "sylphx-session-id",
    "sylphx-cloud-session.json",
    "sylphx-cloud-session-fallback.json",
    "sylphx-workspace-placement.json",
    "sylphx-sandbox-lease.json",
    "sylphx-sandbox-fallback.json",
    "sylphx-usage.jsonl",
];

/// Supplied by the authoritative host registry. Aliases are reconciled current
/// and former names, not a guess from a working-directory basename.
#[derive(Clone, Debug)]
pub struct AgentOwnership {
    pub id: AgentId,
    pub aliases: Vec<String>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MigrationKind {
    LearnedMemory,
    History,
    NativeState,
    RawTranscript,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatus {
    Planned,
    Prepared,
    Complete,
    RetainedActiveWriter,
    PendingRawHandoff,
    Ambiguous,
    UnresolvedUntouched,
    Conflict,
    Changed,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fingerprint {
    pub bytes: u64,
    pub records: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationEntry {
    pub id: String,
    pub agent_id: Option<String>,
    pub source: PathBuf,
    pub destination: Option<PathBuf>,
    pub kind: MigrationKind,
    pub status: MigrationStatus,
    pub source_fingerprint: Option<Fingerprint>,
    pub readback: Option<Fingerprint>,
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationManifest {
    pub version: u32,
    pub entries: Vec<MigrationEntry>,
}

pub struct HostMigration {
    data_dir: PathBuf,
    manifest_path: PathBuf,
    pub manifest: MigrationManifest,
}
impl HostMigration {
    /// Call only inside the daemon's serialized migration/lifecycle boundary.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        let manifest_path = data_dir.join("host-memory-migration/manifest-v1.json");
        let manifest = match fs::read(&manifest_path) {
            Ok(bytes) => {
                serde_json::from_slice::<MigrationManifest>(&bytes).map_err(io::Error::other)?
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => MigrationManifest {
                version: 1,
                entries: Vec::new(),
            },
            Err(e) => return Err(e),
        };
        if manifest.version != 1 {
            return Err(io::Error::other("unknown host memory migration version"));
        }
        Ok(Self {
            data_dir: data_dir.to_owned(),
            manifest_path,
            manifest,
        })
    }
    fn save(&self) -> io::Result<()> {
        atomic_write(
            &self.manifest_path,
            &serde_json::to_vec_pretty(&self.manifest).map_err(io::Error::other)?,
        )
    }
    fn add(
        &mut self,
        owner: Option<&AgentId>,
        source: PathBuf,
        destination: Option<PathBuf>,
        kind: MigrationKind,
        reason: Option<&str>,
    ) {
        let id = sha256(source.as_os_str().as_encoded_bytes());
        if self.manifest.entries.iter().any(|e| e.id == id) {
            return;
        }
        self.manifest.entries.push(MigrationEntry {
            id,
            agent_id: owner.map(ToString::to_string),
            source,
            destination,
            kind,
            status: if reason.is_some() {
                MigrationStatus::Ambiguous
            } else {
                MigrationStatus::Planned
            },
            source_fingerprint: None,
            readback: None,
            reason: reason.map(str::to_owned),
        });
    }
    /// Enumerates only per-agent config roots plus the explicitly authorized
    /// owners-page directory. Unknown and multiply claimed pages are inventoried
    /// but never moved. Nested symlinks are inventoried, not followed.
    pub fn discover(&mut self, agents: &[AgentOwnership], owners_pages: &Path) -> io::Result<()> {
        for path in ["/home/kyle/.claude/projects", "/home/kyle/.codex/memories"] {
            self.add(
                None,
                PathBuf::from(path),
                None,
                MigrationKind::LearnedMemory,
                Some("UNRESOLVED/UNTOUCHED: personal/global namespace; contents not inspected"),
            );
            if let Some(entry) = self
                .manifest
                .entries
                .iter_mut()
                .find(|e| e.source == Path::new(path))
            {
                entry.status = MigrationStatus::UnresolvedUntouched;
            }
        }
        for agent in agents {
            let root = self.data_dir.join("agents").join(agent.id.to_string());
            let config = root.join("config");
            for projects in [config.join("projects"), config.join(".claude/projects")] {
                if has_symlink_component(&projects, &config)? {
                    self.add(
                        Some(&agent.id),
                        projects,
                        None,
                        MigrationKind::LearnedMemory,
                        Some("symlink project root retained without traversal"),
                    );
                    continue;
                }
                for project in children(&projects)? {
                    if !fs::symlink_metadata(&project)?.file_type().is_dir() {
                        continue;
                    }
                    let memory = project.join("memory");
                    for path in tree_files(&memory)? {
                        let relative = path.strip_prefix(&memory).map_err(io::Error::other)?;
                        let destination = root.join("memory").join(relative);
                        let reason = has_symlink_component(&path, &config)?
                            .then_some("symlink source retained");
                        self.add(
                            Some(&agent.id),
                            path,
                            Some(destination),
                            MigrationKind::LearnedMemory,
                            reason,
                        );
                    }
                    for path in children(&project)? {
                        if path.extension().is_some_and(|e| e == "jsonl") {
                            let dest = root.join("transcripts/retained").join(format!(
                                "{}.jsonl",
                                sha256(path.as_os_str().as_encoded_bytes())
                            ));
                            self.add(
                                Some(&agent.id),
                                path,
                                Some(dest),
                                MigrationKind::RawTranscript,
                                None,
                            );
                        }
                    }
                }
            }
            for name in NATIVE_STATE {
                let source = config.join(name);
                if source.try_exists()? {
                    self.add(
                        Some(&agent.id),
                        source,
                        Some(root.join("state-import").join(name)),
                        MigrationKind::NativeState,
                        None,
                    );
                }
            }
            let raw = config.join("sylphx-transcript.jsonl");
            if raw.try_exists()? {
                self.add(
                    Some(&agent.id),
                    raw,
                    Some(root.join("transcripts/retained/sylphx.jsonl")),
                    MigrationKind::RawTranscript,
                    None,
                );
            }
        }
        let mut aliases: BTreeMap<String, Vec<&AgentId>> = BTreeMap::new();
        for agent in agents {
            for alias in std::iter::once(agent.id.to_string()).chain(agent.aliases.iter().cloned())
            {
                aliases.entry(alias).or_default().push(&agent.id);
            }
        }
        for page in children(owners_pages)? {
            let name = if page.is_dir() {
                page.file_name()
            } else {
                page.file_stem()
            }
            .and_then(|s| s.to_str())
            .unwrap_or_default();
            let owners = aliases.get(name);
            let unique = owners.and_then(|owners| {
                let set: HashSet<_> = owners.iter().map(|id| id.to_string()).collect();
                (set.len() == 1).then_some(owners[0])
            });
            let files = if fs::symlink_metadata(&page)?.file_type().is_dir() {
                tree_files(&page)?
            } else {
                vec![page.clone()]
            };
            for file in files {
                let dest = unique.map(|id| {
                    self.data_dir
                        .join("agents")
                        .join(id.to_string())
                        .join("history/owners")
                        .join(sha256(file.as_os_str().as_encoded_bytes()))
                        .join(file.file_name().unwrap_or_default())
                });
                let reason = if unique.is_none() {
                    Some("unknown or ambiguous reconciled agent ownership")
                } else if has_symlink_component(&file, owners_pages)? {
                    Some("symlink source retained")
                } else {
                    None
                };
                self.add(unique, file, dest, MigrationKind::History, reason);
            }
        }
        self.save()
    }
    /// Stopped IDs come from the daemon lifecycle lock, never tool arguments.
    /// Raw sources remain in place regardless: PR1 cannot activate their handoff.
    pub fn stage(&mut self, stopped_agents: &HashSet<String>) -> io::Result<()> {
        self.stage_selected(stopped_agents, None)
    }
    /// A daemon pre-launch boundary stages only this agent; discovery still uses
    /// the complete registry so aliases shared by other agents stay ambiguous.
    pub fn stage_selected(
        &mut self,
        stopped_agents: &HashSet<String>,
        selected: Option<&HashSet<String>>,
    ) -> io::Result<()> {
        self.preflight_memory_bundles(stopped_agents, selected)?;
        for idx in 0..self.manifest.entries.len() {
            let entry = self.manifest.entries[idx].clone();
            if selected
                .is_some_and(|ids| !entry.agent_id.as_ref().is_some_and(|id| ids.contains(id)))
            {
                continue;
            }
            // Never fingerprint a live writer: even an ordinary append is not
            // stable input and must not prevent the daemon from binding sockets.
            if entry
                .agent_id
                .as_ref()
                .is_some_and(|id| !stopped_agents.contains(id))
            {
                if matches!(
                    entry.status,
                    MigrationStatus::Planned
                        | MigrationStatus::Prepared
                        | MigrationStatus::PendingRawHandoff
                        | MigrationStatus::RetainedActiveWriter
                ) {
                    self.manifest.entries[idx].status = MigrationStatus::RetainedActiveWriter;
                    self.manifest.entries[idx].source_fingerprint = None;
                    self.manifest.entries[idx].reason = Some(
                        "active/busy writer deferred; retry at a quiescent lifecycle boundary"
                            .into(),
                    );
                    self.save()?;
                }
                continue;
            }
            if matches!(
                entry.status,
                MigrationStatus::Ambiguous
                    | MigrationStatus::UnresolvedUntouched
                    | MigrationStatus::Conflict
                    | MigrationStatus::Changed
            ) {
                continue;
            }
            if entry.status == MigrationStatus::Complete {
                // The immutable receipt proves the handoff. The destination is
                // now host-owned mutable data, not a forever-frozen replica.
                continue;
            }
            let destination = entry
                .destination
                .as_ref()
                .ok_or_else(|| io::Error::other("migration destination missing"))?;
            // Recover the effect after interruption between rename and receipt.
            if !entry.source.try_exists()?
                && entry.status == MigrationStatus::Prepared
                && destination.try_exists()?
            {
                let readback = fingerprint(destination)?;
                if Some(&readback) != entry.source_fingerprint.as_ref() {
                    return Err(io::Error::other("interrupted migration readback mismatch"));
                }
                self.manifest.entries[idx].readback = Some(readback);
                self.manifest.entries[idx].status = MigrationStatus::Complete;
                self.save()?;
                continue;
            }
            if !entry.source.try_exists()? {
                self.manifest.entries[idx].status = MigrationStatus::Changed;
                self.manifest.entries[idx].reason =
                    Some("source missing; preserved manifest for reconciliation".into());
                self.save()?;
                continue;
            }
            if !fs::symlink_metadata(&entry.source)?.file_type().is_file()
                || has_symlink_component(&entry.source, &self.data_dir)?
            {
                self.manifest.entries[idx].status = MigrationStatus::Ambiguous;
                self.manifest.entries[idx].reason =
                    Some("non-regular or symlink source retained".into());
                self.save()?;
                continue;
            }
            let current = fingerprint(&entry.source)?;
            if entry.kind == MigrationKind::RawTranscript {
                self.manifest.entries[idx].source_fingerprint = Some(current);
                self.manifest.entries[idx].status = MigrationStatus::PendingRawHandoff;
                self.save()?;
                continue;
            }
            if entry.kind == MigrationKind::NativeState {
                // Content-addressed snapshots retain every staged version while
                // the native writer remains authoritative through PR2 catch-up.
                let owner = entry
                    .agent_id
                    .as_ref()
                    .ok_or_else(|| io::Error::other("state owner missing"))?;
                let root = self.data_dir.join("agents").join(owner);
                let relative = entry
                    .source
                    .strip_prefix(root.join("config"))
                    .map_err(io::Error::other)?;
                let dest = root
                    .join("state-import/snapshots")
                    .join(&current.sha256)
                    .join(relative);
                self.manifest.entries[idx].destination = Some(dest.clone());
                self.manifest.entries[idx].source_fingerprint = Some(current.clone());
                self.manifest.entries[idx].status = MigrationStatus::Prepared;
                self.save()?;
                snapshot(&entry.source, &dest, &current)?;
                self.manifest.entries[idx].readback = Some(fingerprint(&dest)?);
                // RetainedActiveWriter is deliberately retried on every pass.
                self.manifest.entries[idx].status = MigrationStatus::RetainedActiveWriter;
                self.save()?;
                continue;
            }
            if entry.status == MigrationStatus::Prepared
                && entry.source_fingerprint.as_ref() != Some(&current)
            {
                self.manifest.entries[idx].status = MigrationStatus::Changed;
                self.manifest.entries[idx].reason =
                    Some("prepared source changed; retained without moving".into());
                self.save()?;
                continue;
            }
            self.manifest.entries[idx].source_fingerprint = Some(current.clone());
            self.manifest.entries[idx].status = MigrationStatus::Prepared;
            self.save()?;
            if destination.try_exists()? {
                let existing = fingerprint(destination)?;
                if existing != current {
                    self.manifest.entries[idx].status = MigrationStatus::Conflict;
                    self.manifest.entries[idx].reason =
                        Some("destination differs; both copies retained".into());
                    self.save()?;
                    continue;
                }
                // Never delete even an identical predecessor just to tidy up.
            } else {
                rename_verified(&entry.source, destination, &current)?;
            }
            self.manifest.entries[idx].readback = Some(fingerprint(destination)?);
            self.manifest.entries[idx].status = MigrationStatus::Complete;
            self.save()?;
        }
        Ok(())
    }
    /// Refuse a whole Claude memory bundle before any per-file rename if its
    /// destinations collide. Retained relative index/fact references stay valid.
    fn preflight_memory_bundles(
        &mut self,
        stopped: &HashSet<String>,
        selected: Option<&HashSet<String>>,
    ) -> io::Result<()> {
        let mut destinations: BTreeMap<PathBuf, Vec<(usize, Fingerprint)>> = BTreeMap::new();
        let mut refused = HashSet::new();
        for (idx, entry) in self.manifest.entries.iter().enumerate() {
            if !entry.agent_id.as_ref().is_some_and(|id| {
                stopped.contains(id) && selected.is_none_or(|ids| ids.contains(id))
            }) {
                continue;
            }
            if entry.kind != MigrationKind::LearnedMemory
                || !matches!(
                    entry.status,
                    MigrationStatus::Planned
                        | MigrationStatus::Prepared
                        | MigrationStatus::RetainedActiveWriter
                )
            {
                continue;
            }
            let Some(bundle) = memory_bundle(&entry.source) else {
                continue;
            };
            if !entry.source.try_exists()? {
                continue;
            }
            if has_symlink_component(&entry.source, &self.data_dir)?
                || !fs::symlink_metadata(&entry.source)?.file_type().is_file()
            {
                refused.insert(bundle);
                continue;
            }
            let Some(destination) = &entry.destination else {
                continue;
            };
            let current = fingerprint(&entry.source)?;
            if destination.try_exists()? && fingerprint(destination)? != current {
                refused.insert(bundle);
            }
            destinations
                .entry(destination.clone())
                .or_default()
                .push((idx, current));
        }
        for entries in destinations.values() {
            if entries.iter().any(|(_, fp)| fp != &entries[0].1) {
                for (idx, _) in entries {
                    if let Some(bundle) = memory_bundle(&self.manifest.entries[*idx].source) {
                        refused.insert(bundle);
                    }
                }
            }
        }
        for entry in &mut self.manifest.entries {
            if entry.kind == MigrationKind::LearnedMemory
                && entry.status != MigrationStatus::Complete
                && memory_bundle(&entry.source).is_some_and(|b| refused.contains(&b))
            {
                entry.status = MigrationStatus::Conflict;
                entry.reason=Some("whole source memory bundle retained: unresolved destination collision or symlink".into());
            }
        }
        self.save()
    }
}
fn memory_bundle(path: &Path) -> Option<PathBuf> {
    path.ancestors()
        .find(|p| p.file_name().is_some_and(|n| n == "memory"))
        .map(Path::to_owned)
}

/// Explicit PR2 primitive, intentionally not called by PR1 staging. Both facts
/// must be established under the host lifecycle lock before moving an active
/// native path. Keeping either false leaves all bytes and the old path intact.
pub fn handoff_raw(
    source: &Path,
    destination: &Path,
    expected: &Fingerprint,
    native_writer_stopped: bool,
    host_port_selected: bool,
) -> io::Result<()> {
    if !native_writer_stopped || !host_port_selected {
        return Err(io::Error::other(
            "raw handoff requires stopped native writer and selected host port",
        ));
    }
    rename_verified(source, destination, expected)
}

pub fn fingerprint(path: &Path) -> io::Result<Fingerprint> {
    let mut file = File::open(path)?;
    let before = file.metadata()?;
    if !before.is_file() {
        return Err(io::Error::other("migration source is not a regular file"));
    }
    let mut hash = Sha256::new();
    let mut bytes = 0;
    let mut records = 0;
    let mut last = None;
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        bytes += n as u64;
        records += buffer[..n].iter().filter(|b| **b == b'\n').count() as u64;
        last = buffer.get(n - 1).copied();
    }
    let after = file.metadata()?;
    if before.len() != after.len()
        || before.modified()? != after.modified()?
        || bytes != before.len()
    {
        return Err(io::Error::other("migration source changed during hashing"));
    }
    if last.is_some_and(|b| b != b'\n') {
        records += 1;
    }
    Ok(Fingerprint {
        bytes,
        records,
        sha256: format!("{:x}", hash.finalize()),
    })
}
fn children(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = match fs::read_dir(root) {
        Ok(entries) => entries
            .map(|e| e.map(|e| e.path()))
            .collect::<io::Result<Vec<_>>>()?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    paths.sort();
    Ok(paths)
}
fn tree_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    if fs::symlink_metadata(root).is_ok_and(|m| m.file_type().is_symlink()) {
        return Ok(vec![root.to_owned()]);
    }
    let mut files = Vec::new();
    for path in children(root)? {
        if fs::symlink_metadata(&path)?.file_type().is_dir() {
            files.extend(tree_files(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
}
fn has_symlink_component(path: &Path, boundary: &Path) -> io::Result<bool> {
    for ancestor in path.ancestors() {
        if let Ok(meta) = fs::symlink_metadata(ancestor)
            && meta.file_type().is_symlink()
        {
            return Ok(true);
        }
        if ancestor == boundary {
            break;
        }
    }
    Ok(false)
}
fn snapshot(source: &Path, destination: &Path, expected: &Fingerprint) -> io::Result<()> {
    if destination.try_exists()? {
        if fingerprint(destination)? == *expected {
            return Ok(());
        }
        return Err(io::Error::other("state snapshot destination differs"));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("snapshot parent missing"))?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".stage-{}", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut input = File::open(source)?;
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        io::copy(&mut input, &mut out)?;
        out.flush()?;
        out.sync_all()?;
        if fingerprint(&temp)? != *expected || fingerprint(source)? != *expected {
            return Err(io::Error::other(
                "native state changed while staging; source retained",
            ));
        }
        rename_noreplace(&temp, destination)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}
fn rename_verified(source: &Path, destination: &Path, expected: &Fingerprint) -> io::Result<()> {
    if has_symlink_component(source, Path::new("/"))?
        || has_symlink_component(destination, Path::new("/"))?
    {
        return Err(io::Error::other("migration symlink path refused"));
    }
    if fingerprint(source)? != *expected {
        return Err(io::Error::other("migration source hash changed"));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("rename parent missing"))?;
    fs::create_dir_all(parent)?;
    rename_noreplace(source, destination)?;
    File::open(parent)?.sync_all()?;
    if let Some(parent) = source.parent() {
        File::open(parent)?.sync_all()?;
    }
    if fingerprint(destination)? != *expected {
        return Err(io::Error::other(
            "migration rename readback mismatch; destination preserved",
        ));
    }
    Ok(())
}
#[cfg(target_os = "linux")]
fn rename_noreplace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let source = CString::new(source.as_os_str().as_bytes()).map_err(io::Error::other)?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(io::Error::other)?;
    if unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(target_os = "macos")]
fn rename_noreplace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let source = CString::new(source.as_os_str().as_bytes()).map_err(io::Error::other)?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(io::Error::other)?;
    if unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn rename_noreplace(_source: &Path, _destination: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic no-replace same-filesystem rename unavailable",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn write(path: &Path, bytes: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    #[test]
    fn discovery_moves_exact_facts_stages_complete_state_and_keeps_active_raw() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentOwnership {
            id: AgentId::generate(),
            aliases: vec!["sage".into()],
        };
        let root = dir.path().join("agents").join(agent.id.to_string());
        let memory = root.join("config/projects/known/memory/MEMORY.md");
        let fact = root.join("config/projects/known/memory/fact.md");
        let bytes = b"---\nname: exact fact\ndescription: keep bytes\n---\n\x00\xff\n";
        write(&memory, b"index\n");
        write(&fact, bytes);
        for name in NATIVE_STATE {
            write(&root.join("config").join(name), b"{\"complete\":true}\n");
        }
        let raw = root.join("config/sylphx-transcript.jsonl");
        write(&raw, b"{\"event\":1}\n");
        let pages = dir.path().join("owners");
        write(&pages.join("sage.md"), b"progress never in prompt\n");
        write(&pages.join("unknown.md"), b"unknown stays\n");
        let mut migration = HostMigration::open(dir.path()).unwrap();
        migration
            .discover(std::slice::from_ref(&agent), &pages)
            .unwrap();
        migration.stage(&HashSet::new()).unwrap();
        assert!(memory.exists());
        let stopped = HashSet::from([agent.id.to_string()]);
        migration.stage(&stopped).unwrap();
        assert_eq!(fs::read(root.join("memory/fact.md")).unwrap(), bytes);
        assert!(!fact.exists());
        assert!(raw.exists());
        assert!(
            migration
                .manifest
                .entries
                .iter()
                .any(|e| e.status == MigrationStatus::Ambiguous)
        );
        assert_eq!(
            migration
                .manifest
                .entries
                .iter()
                .filter(
                    |e| e.kind == MigrationKind::NativeState && e.readback == e.source_fingerprint
                )
                .count(),
            NATIVE_STATE.len()
        );
        assert!(migration.manifest.entries.iter().all(|e| {
            e.kind != MigrationKind::History
                || e.destination
                    .as_ref()
                    .is_none_or(|p| !p.starts_with(root.join("memory")))
        }));
        assert!(
            migration
                .manifest
                .entries
                .iter()
                .filter(|e| e.status == MigrationStatus::Complete)
                .all(|e| e.source_fingerprint == e.readback)
        );
        let count = migration.manifest.entries.len();
        drop(migration);
        let mut migration = HostMigration::open(dir.path()).unwrap();
        migration.discover(&[agent], &pages).unwrap();
        migration.stage(&stopped).unwrap();
        assert_eq!(migration.manifest.entries.len(), count);
        assert!(pages.join("unknown.md").exists());
        assert!(raw.exists());
    }
    #[test]
    fn interrupted_rename_resumes_and_conflicting_destination_preserves_both() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentOwnership {
            id: AgentId::generate(),
            aliases: vec![],
        };
        let root = dir.path().join("agents").join(agent.id.to_string());
        let source = root.join("config/projects/known/memory/MEMORY.md");
        write(&source, b"exact\n");
        let destination = root.join("memory/MEMORY.md");
        let mut migration = HostMigration::open(dir.path()).unwrap();
        migration
            .discover(std::slice::from_ref(&agent), &dir.path().join("owners"))
            .unwrap();
        let fp = fingerprint(&source).unwrap();
        let idx = migration
            .manifest
            .entries
            .iter()
            .position(|e| e.source == source)
            .unwrap();
        migration.manifest.entries[idx].source_fingerprint = Some(fp.clone());
        migration.manifest.entries[idx].status = MigrationStatus::Prepared;
        migration.save().unwrap();
        rename_verified(&source, &destination, &fp).unwrap();
        drop(migration);
        let mut migration = HostMigration::open(dir.path()).unwrap();
        migration
            .stage(&HashSet::from([agent.id.to_string()]))
            .unwrap();
        assert_eq!(
            migration.manifest.entries[idx].status,
            MigrationStatus::Complete
        );
        assert_eq!(migration.manifest.entries[idx].readback, Some(fp));
        let other = root.join("config/projects/known/memory/other.md");
        write(&other, b"unique old");
        write(&root.join("memory/other.md"), b"unique new");
        migration
            .discover(std::slice::from_ref(&agent), &dir.path().join("owners"))
            .unwrap();
        migration
            .stage(&HashSet::from([agent.id.to_string()]))
            .unwrap();
        assert_eq!(fs::read(&other).unwrap(), b"unique old");
        assert_eq!(
            fs::read(root.join("memory/other.md")).unwrap(),
            b"unique new"
        );
        assert!(
            migration
                .manifest
                .entries
                .iter()
                .any(|e| e.status == MigrationStatus::Conflict)
        );
    }
    #[test]
    fn completed_receipt_allows_host_edits_and_collisions_retain_whole_project_bundles() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentOwnership {
            id: AgentId::generate(),
            aliases: vec![],
        };
        let root = dir.path().join("agents").join(agent.id.to_string());
        let source = root.join("config/projects/a/memory/MEMORY.md");
        write(&source, b"[fact](fact.md)\n");
        write(&source.with_file_name("fact.md"), b"original\n");
        let mut migration = HostMigration::open(dir.path()).unwrap();
        migration
            .discover(std::slice::from_ref(&agent), &dir.path().join("owners"))
            .unwrap();
        let stopped = HashSet::from([agent.id.to_string()]);
        migration.stage(&stopped).unwrap();
        let old = migration
            .manifest
            .entries
            .iter()
            .find(|e| e.source == source)
            .unwrap()
            .readback
            .clone();
        write(&root.join("memory/fact.md"), b"legitimate latest edit\n");
        migration.stage(&stopped).unwrap();
        assert_eq!(
            fs::read(root.join("memory/fact.md")).unwrap(),
            b"legitimate latest edit\n"
        );
        assert_eq!(
            migration
                .manifest
                .entries
                .iter()
                .find(|e| e.source == source)
                .unwrap()
                .readback,
            old
        );
        for project in ["b", "c"] {
            let memory = root.join(format!("config/projects/{project}/memory"));
            write(
                &memory.join("MEMORY.md"),
                format!("{project} [unique](unique.md)\n").as_bytes(),
            );
            write(&memory.join("unique.md"), project.as_bytes());
        }
        migration
            .discover(&[agent], &dir.path().join("owners"))
            .unwrap();
        migration.stage(&stopped).unwrap();
        for project in ["b", "c"] {
            let memory = root.join(format!("config/projects/{project}/memory"));
            assert!(
                fs::read_to_string(memory.join("MEMORY.md"))
                    .unwrap()
                    .contains("unique.md")
            );
            assert_eq!(
                fs::read_to_string(memory.join("unique.md")).unwrap(),
                project
            );
        }
        assert_eq!(
            migration
                .manifest
                .entries
                .iter()
                .filter(|e| e.status == MigrationStatus::UnresolvedUntouched)
                .count(),
            2
        );
        assert!(
            migration
                .manifest
                .entries
                .iter()
                .filter(|e| e.status == MigrationStatus::UnresolvedUntouched)
                .all(|e| e.source_fingerprint.is_none() && e.readback.is_none())
        );
    }
    #[test]
    fn raw_handoff_cannot_split_active_writer_and_preserves_same_file_history() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("native.jsonl");
        let dest = dir.path().join("host/retained.jsonl");
        let bytes = b"{\"old\":1}\n";
        write(&source, bytes);
        let fp = fingerprint(&source).unwrap();
        for (stopped, selected) in [(false, false), (false, true), (true, false)] {
            assert!(
                handoff_raw(&source, &dest, &fp, stopped, selected)
                    .unwrap_err()
                    .to_string()
                    .contains("requires stopped")
            );
            assert_eq!(fs::read(&source).unwrap(), bytes);
            assert!(!dest.exists());
        }
        handoff_raw(&source, &dest, &fp, true, true).unwrap();
        let segment = crate::raw_segment::RawSegment::open(&dest).unwrap();
        segment.append(b"{\"new\":2}").unwrap();
        assert!(!source.exists());
        assert_eq!(fs::read(&dest).unwrap(), b"{\"old\":1}\n{\"new\":2}\n");
        assert_eq!(segment.records().unwrap().len(), 2);
    }
}
