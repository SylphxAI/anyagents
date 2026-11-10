//! Host-only file retrieval. All paths are derived from authorized registry IDs;
//! active native JSONL is read in place and never handed off or rewritten here.
use crate::{
    Agent, HistorySearchQuery, Store,
    host_cursor::{FileStamp, SourceCursor},
    host_index::{HostIndex, IndexError, IndexHit, IndexMetadata, MemoryScope},
    host_native_metadata::{EventHeader, MetadataReader},
    learned_memory::sha256,
    raw_segment::{RawCoordinate, open_readonly, read_raw},
};
use std::{
    collections::HashSet,
    fs, io,
    path::{Path, PathBuf},
};

pub const INDEX_FILE: &str = "host-history-index-v1.db";

/// ID and expected digest come from a search result; offsets are event-relative.
pub struct HostRawRead<'a> {
    pub id: &'a str,
    pub sha256: &'a str,
    pub offset: u64,
    pub length: u64,
}

impl Store {
    pub fn is_host_path_redacted(&self, owner: &str, path: &Path) -> Result<bool, IndexError> {
        let root = self.data_dir().join("agents").join(owner);
        if !safe_components(self.data_dir(), &root)? {
            return Ok(true);
        }
        Ok(self
            .unresolved_host_paths(owner)?
            .iter()
            .any(|p| path.starts_with(p)))
    }

    fn unresolved_host_paths(&self, owner: &str) -> Result<Vec<PathBuf>, IndexError> {
        let paths = self
            .conn()
            .prepare("SELECT receipt FROM conversation_redactions WHERE agent_id=?1")?
            .query_map([owner], |r| r.get::<_, String>(0))?
            .map(|raw| -> Result<_, IndexError> {
                let receipt: crate::redaction::Receipt = serde_json::from_str(&raw?)?;
                Ok(receipt
                    .unscrubbed
                    .into_iter()
                    .map(|f| PathBuf::from(f.path))
                    .collect::<Vec<_>>())
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        Ok(paths)
    }

    /// Low-priority daemon maintenance: at most one due native source per call.
    /// Failures advance only attempt ordering, so one bad source cannot starve
    /// others; frontier/documents remain the previous atomic publication.
    pub fn verify_one_host_history_source(&self, now_ms: i64) -> Result<bool, IndexError> {
        let index_path = self.data_dir().join(INDEX_FILE);
        if !index_path.exists() {
            return Ok(false);
        }
        let candidate = HostIndex::open(&index_path)?.verification_candidate(now_ms)?;
        let Some((owner, path)) = candidate else {
            return Ok(false);
        };
        let agent = self
            .list_agents()
            .map_err(|e| io::Error::other(e.to_string()))?
            .into_iter()
            .find(|a| a.id.as_str() == owner);
        let Some(agent) = agent else {
            let index = HostIndex::open(&index_path)?;
            index.clear_source(&owner, &path)?;
            index.checkpoint()?;
            return Ok(true);
        };
        let _guard = self
            .lock_agent_lifecycle(&agent.id)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let mut index = HostIndex::open(&index_path)?;
        let Some(mut cursor) = index.source_cursor(&owner, &path)? else {
            return Ok(false);
        };
        cursor.verification_attempt_ms = now_ms;
        index.save_source(&owner, &path, &cursor)?;
        let root = self.data_dir().join("agents").join(agent.id.as_str());
        if !safe_components(self.data_dir(), &root)?
            || !native_transcripts(&root)?.contains(&path)
            || self
                .unresolved_host_paths(&owner)?
                .iter()
                .any(|p| path.starts_with(p))
        {
            index.clear_source(&owner, &path)?;
            index.checkpoint()?;
            return Ok(true);
        }
        refresh_native_at(&mut index, &agent, &path, true, now_ms)?;
        Ok(true)
    }

    /// Read a bounded range from an already-indexed document, never a supplied
    /// path/coordinate. Resolve ACL before file access, then repeat lookup under
    /// its owner lifecycle lock so redaction cannot race authorization/readback.
    pub fn read_host_file_range(
        &self,
        authorized: &[String],
        scope: MemoryScope,
        selected: Option<&str>,
        request: HostRawRead<'_>,
    ) -> Result<Option<(IndexHit, Vec<u8>)>, IndexError> {
        let selected = selected
            .map(str::trim)
            .filter(|s| *s != "*" && !s.eq_ignore_ascii_case("all"))
            .map(|s| self.agent_by_name(s).map(|a| a.id.to_string()))
            .transpose()
            .map_err(|e| io::Error::other(e.to_string()))?;
        let visible: Vec<String> = authorized
            .iter()
            .filter(|id| selected.as_ref().is_none_or(|s| s == *id))
            .cloned()
            .collect();
        if visible.is_empty() {
            return Ok(None);
        }
        let index_path = self.data_dir().join(INDEX_FILE);
        if !index_path.exists() {
            return Ok(None);
        }
        let Some(hit) = HostIndex::open(&index_path)?.document(&visible, scope, request.id)? else {
            return Ok(None);
        };
        let agent = self
            .agent_by_name(&hit.metadata.agent_id)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let _guard = self
            .lock_agent_lifecycle(&agent.id)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let Some(mut hit) = HostIndex::open(&index_path)?.document(
            std::slice::from_ref(&agent.id.to_string()),
            scope,
            request.id,
        )?
        else {
            return Ok(None);
        };
        if hit.raw.sha256 != request.sha256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw document changed since search",
            )
            .into());
        }
        let root = self.data_dir().join("agents").join(agent.id.as_str());
        if !safe_components(self.data_dir(), &root)?
            || self
                .unresolved_host_paths(agent.id.as_str())?
                .iter()
                .any(|p| hit.raw.segment.starts_with(p))
        {
            return Ok(None);
        }
        let paths = match hit.metadata.source {
            MemoryScope::Memory => regular_files(&root.join("memory"), true)?
                .into_iter()
                .filter(|p| !is_hidden_memory_file(p))
                .collect(),
            MemoryScope::Transcripts => {
                let mut paths = native_transcripts(&root)?;
                paths.extend(history_archives(&root)?);
                paths
            }
            MemoryScope::All => return Ok(None),
        };
        if !paths.contains(&hit.raw.segment) {
            return Ok(None);
        }
        let (window, bytes) =
            crate::raw_segment::read_raw_range(&hit.raw, request.offset, request.length)?;
        hit.window = window;
        Ok(Some((hit, bytes)))
    }

    /// Immediately synchronize an agent's `memory/` directory into `HostIndex`
    /// so newly upserted, read, or
    /// deleted topics are immediately readable via `raw_id` and `search_host_files`.
    pub fn sync_agent_memory_index(&self, agent: &Agent) -> Result<(), IndexError> {
        let root = self.data_dir().join("agents").join(agent.id.as_str());
        let mut index = HostIndex::open(&self.data_dir().join(INDEX_FILE))?;
        if !safe_components(self.data_dir(), &root)? {
            index.retain_agent_scope(agent.id.as_str(), MemoryScope::Memory, &HashSet::new())?;
            return Ok(());
        }
        let unresolved = self.unresolved_host_paths(agent.id.as_str())?;
        let mut ids = HashSet::new();
        for path in memory_indexable_files(&root.join("memory"))? {
            if unresolved.iter().any(|blocked| path.starts_with(blocked)) {
                continue;
            }
            let id = format!(
                "memory:{}:{}",
                agent.id,
                sha256(path.as_os_str().as_encoded_bytes())
            );
            let metadata = IndexMetadata {
                agent_id: agent.id.to_string(),
                source: MemoryScope::Memory,
                timestamp_ms: Some(modified_ms(&path)?),
                direction: "in".into(),
                sender: format!("agent:{}", agent.name),
                destination: format!("agent:{}", agent.name),
                tool_name: None,
                tool_names: Vec::new(),
                role: Some("memory".into()),
            };
            refresh_document(&mut index, &id, &metadata, &path)?;
            ids.insert(id);
        }
        index.retain_agent_scope(agent.id.as_str(), MemoryScope::Memory, &ids)?;
        Ok(())
    }

    /// The caller has already resolved the authorized immutable ID set. Locks
    /// serialize refresh/readback against redaction, rename and lifecycle changes.
    pub fn search_host_files(
        &self,
        authorized: &[String],
        scope: MemoryScope,
        query: &HistorySearchQuery,
    ) -> Result<Vec<(IndexHit, Vec<u8>)>, IndexError> {
        let selected = query
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| *s != "*" && !s.eq_ignore_ascii_case("all"))
            .map(|s| self.agent_by_name(s).map(|a| a.id))
            .transpose()
            .map_err(|e| io::Error::other(e.to_string()))?;
        let mut agents: Vec<Agent> = self
            .list_agents()
            .map_err(|e| io::Error::other(e.to_string()))?
            .into_iter()
            .filter(|a| authorized.contains(&a.id.to_string()))
            .filter(|a| selected.as_ref().is_none_or(|id| id == &a.id))
            .collect();
        agents.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        let mut index = HostIndex::open(&self.data_dir().join(INDEX_FILE))?;
        for agent in &agents {
            let _guard = self
                .lock_agent_lifecycle(&agent.id)
                .map_err(|e| io::Error::other(e.to_string()))?;
            let root = self.data_dir().join("agents").join(agent.id.as_str());
            if !safe_components(self.data_dir(), &root)? {
                index.retain_agent_scope(agent.id.as_str(), scope, &HashSet::new())?;
                continue;
            }
            let mut ids = HashSet::new();
            // The durable redactor is the only text mutation authority. A
            // pending/failed copy excludes precisely that path and descendants;
            // an unrelated unsupported store does not hide scrubbed local data.
            let unresolved = self.unresolved_host_paths(agent.id.as_str())?;
            if scope != MemoryScope::Transcripts {
                for path in memory_indexable_files(&root.join("memory"))? {
                    if unresolved.iter().any(|blocked| path.starts_with(blocked)) {
                        continue;
                    }
                    let id = format!(
                        "memory:{}:{}",
                        agent.id,
                        sha256(path.as_os_str().as_encoded_bytes())
                    );
                    let metadata = IndexMetadata {
                        agent_id: agent.id.to_string(),
                        source: MemoryScope::Memory,
                        timestamp_ms: Some(modified_ms(&path)?),
                        direction: "in".into(),
                        sender: format!("agent:{}", agent.name),
                        destination: format!("agent:{}", agent.name),
                        tool_name: None,
                        tool_names: Vec::new(),
                        role: Some("memory".into()),
                    };
                    refresh_document(&mut index, &id, &metadata, &path)?;
                    ids.insert(id);
                }
            }
            if scope != MemoryScope::Memory {
                for path in native_transcripts(&root)? {
                    if unresolved.iter().any(|blocked| path.starts_with(blocked)) {
                        continue;
                    }
                    ids.extend(refresh_native(&mut index, agent, &path)?.documents);
                }
                // Reconciled archive pages are lossless historical documents,
                // not necessarily JSONL. Their entire text remains searchable.
                for path in history_archives(&root)? {
                    if unresolved.iter().any(|blocked| path.starts_with(blocked)) {
                        continue;
                    }
                    let id = format!(
                        "history:{}:{}",
                        agent.id,
                        sha256(path.as_os_str().as_encoded_bytes())
                    );
                    let metadata = IndexMetadata {
                        agent_id: agent.id.to_string(),
                        source: MemoryScope::Transcripts,
                        timestamp_ms: None,
                        direction: "in".into(),
                        sender: format!("agent:{}", agent.name),
                        destination: format!("agent:{}", agent.name),
                        tool_name: None,
                        tool_names: Vec::new(),
                        role: Some("history".into()),
                    };
                    refresh_document(&mut index, &id, &metadata, &path)?;
                    ids.insert(id);
                }
            }
            index.retain_agent_scope(agent.id.as_str(), scope, &ids)?;
        }
        let visible: Vec<_> = agents.iter().map(|a| a.id.to_string()).collect();
        let mut request = query.clone();
        request.agent = None;
        let hits = index.search(&visible, scope, &request)?;
        let mut out = Vec::with_capacity(hits.len());
        for hit in hits {
            let owner_id = crate::AgentId::parse(&hit.metadata.agent_id)
                .map_err(|e| io::Error::other(e.to_string()))?;
            let _guard = self
                .lock_agent_lifecycle(&owner_id)
                .map_err(|e| io::Error::other(e.to_string()))?;
            let Some(current) =
                index.document(std::slice::from_ref(&hit.metadata.agent_id), scope, &hit.id)?
            else {
                continue;
            };
            if current.raw.sha256 != hit.raw.sha256
                || self.is_host_path_redacted(&hit.metadata.agent_id, &hit.window.segment)?
            {
                continue;
            }
            // Return the matching bounded window, not an unbounded tool payload.
            // The whole-event coordinate remains available for explicit readback.
            let bytes = read_raw(&hit.window)?;
            out.push((hit, bytes));
        }
        Ok(out)
    }
}

#[derive(Default)]
pub struct RefreshStats {
    pub documents: Vec<String>,
    pub prefix_verified_bytes: u64,
    pub boundary_capture_bytes: u64,
    pub suffix_scanned_bytes: u64,
    pub indexed_records: usize,
}
fn refresh_document(
    index: &mut HostIndex,
    id: &str,
    metadata: &IndexMetadata,
    path: &Path,
) -> Result<(), IndexError> {
    refresh_document_before_publish(index, id, metadata, path, || Ok(()))
}
fn refresh_document_before_publish(
    index: &mut HostIndex,
    id: &str,
    metadata: &IndexMetadata,
    path: &Path,
    before_publish: impl FnOnce() -> io::Result<()>,
) -> Result<(), IndexError> {
    let stamp = FileStamp::read(&open_readonly(path)?)?;
    let policy = serde_json::to_string(metadata)?;
    if index
        .source_cursor(&metadata.agent_id, path)?
        .is_some_and(|old| old.stamp == stamp && old.policy == policy)
    {
        return Ok(());
    }
    let raw = document_coordinate(path)?;
    index.refresh_source(|index| {
        index.index(id, metadata, &raw)?;
        before_publish()?;
        if FileStamp::read(&open_readonly(path)?)? == stamp {
            index.save_source(
                &metadata.agent_id,
                path,
                &SourceCursor {
                    stamp,
                    policy,
                    end: raw.length,
                    next_seq: 2,
                    boundary_sha256: String::new(),
                    full_verified_ms: 0,
                    verification_attempt_ms: 0,
                },
            )?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "memory source changed during refresh; retry",
            )
            .into());
        }
        Ok(())
    })
}
fn refresh_native(
    index: &mut HostIndex,
    agent: &Agent,
    path: &Path,
) -> Result<RefreshStats, IndexError> {
    refresh_native_at(index, agent, path, false, crate::now_ms())
}
fn refresh_native_at(
    index: &mut HostIndex,
    agent: &Agent,
    path: &Path,
    force_full: bool,
    now_ms: i64,
) -> Result<RefreshStats, IndexError> {
    let stamp = FileStamp::read(&open_readonly(path)?)?;
    let policy = format!("native-jsonl-v4:{}:{}", agent.id, agent.name);
    let old = index
        .source_cursor(agent.id.as_str(), path)?
        .filter(|old| old.policy == policy);
    if !force_full && old.as_ref().is_some_and(|old| old.stamp == stamp) {
        return Ok(RefreshStats {
            documents: index.source_documents(agent.id.as_str(), path)?,
            ..Default::default()
        });
    }
    let scan = crate::host_cursor::scan_jsonl(path, &stamp, old.as_ref(), force_full)?;
    index.refresh_source(|index| {
        if scan.replace {
            index.clear_source(agent.id.as_str(), path)?;
        }
        // There cannot be committed documents beyond the atomically published
        // frontier. An interrupted outer transaction is rolled back by SQLite.
        for raw in &scan.records {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = open_readonly(path)?;
            file.seek(SeekFrom::Start(raw.offset))?;
            let header: EventHeader =
                serde_json::from_reader(MetadataReader::new(file.take(raw.length)))?;
            let mut metadata = event_metadata(agent, &header);
            let records = header.tool_records();
            let mut names = Vec::new();
            if let Some(name) = &metadata.tool_name {
                names.push(name.clone());
            }
            for (call_id, name) in &records {
                let name = name.map(str::to_owned).or_else(|| {
                    call_id
                        .and_then(|id| {
                            records.iter().find_map(|(key, name)| {
                                (*key == Some(id)).then_some(*name).flatten()
                            })
                        })
                        .map(str::to_owned)
                });
                let name = match (name, call_id) {
                    (Some(name), _) => name,
                    (None, Some(id)) => index
                        .call_tool(agent.id.as_str(), path, id)?
                        .unwrap_or_else(|| "unknown".into()),
                    (None, None) => "unknown".into(),
                };
                if !names.contains(&name) {
                    names.push(name);
                }
            }
            metadata.tool_name = names.first().cloned();
            metadata.tool_names = names;
            let id = format!(
                "transcript:{}:{}:{}",
                agent.id,
                sha256(path.as_os_str().as_encoded_bytes()),
                raw.seq
            );
            index.index(&id, &metadata, raw)?;
            for (call_id, name) in records {
                if let (Some(call_id), Some(name)) = (call_id, name) {
                    index.save_call(agent.id.as_str(), path, call_id, name, &id)?;
                }
            }
        }
        // A live append racing this snapshot is not corruption. Do not advance
        // the frontier; the next refresh revalidates and retries that suffix.
        if FileStamp::read(&open_readonly(path)?)? == stamp {
            index.save_source(
                agent.id.as_str(),
                path,
                &SourceCursor {
                    stamp,
                    policy,
                    end: scan.end,
                    next_seq: scan.next_seq,
                    boundary_sha256: scan.boundary_sha256.clone(),
                    full_verified_ms: if scan.replace {
                        now_ms
                    } else {
                        old.as_ref().map_or(0, |c| c.full_verified_ms)
                    },
                    verification_attempt_ms: if scan.replace {
                        now_ms
                    } else {
                        old.as_ref().map_or(0, |c| c.verification_attempt_ms)
                    },
                },
            )?;
        } else {
            // Roll back the whole publication, leaving the prior committed
            // frontier and documents consistent for the next snapshot.
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "native source changed during refresh; retry",
            )
            .into());
        }
        Ok(RefreshStats {
            documents: index.source_documents(agent.id.as_str(), path)?,
            prefix_verified_bytes: scan.prefix_verified_bytes,
            boundary_capture_bytes: scan.boundary_capture_bytes,
            suffix_scanned_bytes: scan.suffix_scanned_bytes,
            indexed_records: scan.records.len(),
        })
    })
}

fn safe_components(root: &Path, target: &Path) -> io::Result<bool> {
    let relative = target.strip_prefix(root).map_err(io::Error::other)?;
    let mut path = root.to_owned();
    for component in relative.components() {
        path.push(component);
        match fs::symlink_metadata(&path) {
            Ok(m) if m.file_type().is_symlink() => return Ok(false),
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}
fn modified_ms(path: &Path) -> io::Result<i64> {
    Ok(fs::metadata(path)?
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_millis()
        .min(i64::MAX as u128) as i64)
}
fn document_coordinate(path: &Path) -> io::Result<RawCoordinate> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = crate::raw_segment::open_readonly(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    let mut length = 0;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        length += n as u64;
        hash.update(&buffer[..n]);
    }
    Ok(RawCoordinate {
        segment: path.into(),
        offset: 0,
        length,
        sha256: format!("{:x}", hash.finalize()),
        seq: 1,
    })
}
/// Read both the generic archive and the historical directory spelling so an
/// upgrade never makes already-migrated documents disappear. New migrations
/// only write the generic location.
fn history_archives(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = regular_files(&root.join("history/archive"), true)?;
    paths.extend(regular_files(&root.join("history").join("owners"), true)?);
    Ok(paths)
}
fn is_hidden_memory_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|s| s.to_str())
        .is_some_and(|name| name.starts_with('.'))
}

fn memory_indexable_files(mem_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let files: Vec<PathBuf> = regular_files(mem_dir, true)?
        .into_iter()
        .filter(|p| !is_hidden_memory_file(p))
        .collect();
    let has_topic_files = files.iter().any(|p| {
        p.file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|name| !name.eq_ignore_ascii_case("MEMORY.md"))
    });
    if !has_topic_files {
        return Ok(files);
    }
    Ok(files
        .into_iter()
        .filter(|p| {
            !p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case("MEMORY.md"))
        })
        .collect())
}

fn regular_files(root: &Path, recursive: bool) -> io::Result<Vec<PathBuf>> {
    let meta = match fs::symlink_metadata(root) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e),
    };
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Ok(vec![]);
    }
    let mut result = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_file() {
            result.push(entry.path());
        } else if recursive && kind.is_dir() {
            result.extend(regular_files(&entry.path(), true)?);
        }
    }
    result.sort();
    Ok(result)
}
fn native_transcripts(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut result = regular_files(&root.join("transcripts"), true)?;
    let config = root.join("config");
    if !safe_components(root, &config)? {
        return Ok(result);
    }
    let sylphx = config.join("sylphx-transcript.jsonl");
    if fs::symlink_metadata(&sylphx).is_ok_and(|m| m.is_file() && !m.file_type().is_symlink()) {
        result.push(sylphx);
    }
    for projects in [config.join("projects"), config.join(".claude/projects")] {
        // No guessed personal home or Codex SQLite discovery.
        if safe_components(root, &projects)?
            && fs::symlink_metadata(&projects)
                .is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink())
        {
            for entry in fs::read_dir(projects)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    result.extend(
                        regular_files(&entry.path(), false)?
                            .into_iter()
                            .filter(|p| p.extension().is_some_and(|e| e == "jsonl")),
                    );
                }
            }
        }
    }
    result.sort();
    result.dedup();
    Ok(result)
}
fn event_metadata(agent: &Agent, value: &EventHeader) -> IndexMetadata {
    let role = value
        .role
        .as_deref()
        .or_else(|| value.message.as_ref().and_then(|m| m.role.as_deref()))
        .unwrap_or("system");
    let tool = value
        .tool_name
        .clone()
        .or_else(|| value.tool.clone())
        .or_else(|| {
            (role == "tool").then(|| value.name.clone().unwrap_or_else(|| "unknown".into()))
        });
    let direction = value
        .direction
        .as_deref()
        .unwrap_or(if role == "user" { "in" } else { "out" });
    IndexMetadata {
        agent_id: agent.id.to_string(),
        source: MemoryScope::Transcripts,
        timestamp_ms: value.timestamp_ms.or_else(|| {
            value
                .timestamp
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|time| time.timestamp_millis())
        }),
        direction: direction.into(),
        sender: value.sender.clone().unwrap_or_else(|| role.into()),
        destination: value
            .destination
            .clone()
            .unwrap_or_else(|| format!("agent:{}", agent.name)),
        tool_names: tool.iter().cloned().collect(),
        tool_name: tool,
        role: Some(role.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;
    #[test]
    fn native_tool_correlation_survives_reopen_and_is_scoped_atomic_and_rebuildable() {
        use std::io::Write;
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let own = store
            .create_agent(NewAgent::new("tool-owner", "spiron"))
            .unwrap();
        let peer = store
            .create_agent(NewAgent::new("tool-peer", "spiron"))
            .unwrap();
        let path = |agent: &Agent| {
            dir.path()
                .join("agents")
                .join(agent.id.as_str())
                .join("config/sylphx-transcript.jsonl")
        };
        let native = path(&own);
        fs::create_dir_all(native.parent().unwrap()).unwrap();
        // Source-shaped synthetic Responses/Claude formats; not captured runtime data.
        fs::write(&native, concat!(
            "{\"type\":\"function_call\",\"call_id\":\"same\",\"name\":\"browser\",\"arguments\":\"{}\"}\n",
            "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"ordinary\"},{\"type\":\"tool_use\",\"id\":\"claude1\",\"name\":\"shell\",\"input\":{}},{\"type\":\"tool_use\",\"id\":\"claude2\",\"name\":\"files\",\"input\":{}}]}}\n"
        )).unwrap();
        let foreign = path(&peer);
        fs::create_dir_all(foreign.parent().unwrap()).unwrap();
        fs::write(
            &foreign,
            "{\"type\":\"function_call\",\"call_id\":\"same\",\"name\":\"foreign\"}\n",
        )
        .unwrap();
        let ids = [own.id.to_string(), peer.id.to_string()];
        store
            .search_host_files(
                &ids,
                MemoryScope::Transcripts,
                &HistorySearchQuery::default(),
            )
            .unwrap();
        drop(store);
        let store = Store::open(&db).unwrap();
        let huge = r#"escaped \"☃\\"#.repeat(20_000);
        let output = format!(
            "{{\"output\":{},\"type\":\"function_call_output\",\"call_id\":\"same\"}}\n",
            serde_json::to_string(&huge).unwrap()
        );
        let claude = "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"mixed\"},{\"type\":\"tool_result\",\"tool_use_id\":\"claude1\",\"content\":\"result\"}]}}\n";
        fs::OpenOptions::new()
            .append(true)
            .open(&native)
            .unwrap()
            .write_all(format!("{output}{claude}").as_bytes())
            .unwrap();
        let other = native
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("transcripts/other.jsonl");
        fs::create_dir_all(other.parent().unwrap()).unwrap();
        fs::write(
            &other,
            "{\"type\":\"function_call_output\",\"call_id\":\"same\",\"output\":\"unresolved\"}\n",
        )
        .unwrap();
        let search = |name: Option<&str>, include_tools, only_tools| {
            store
                .search_host_files(
                    &[own.id.to_string()],
                    MemoryScope::Transcripts,
                    &HistorySearchQuery {
                        tool_name: name.map(str::to_owned),
                        include_tools,
                        only_tools,
                        ..Default::default()
                    },
                )
                .unwrap()
        };
        let browser = search(Some("BROWSER"), false, false);
        assert_eq!(browser.len(), 2);
        let result = browser.iter().find(|(h, _)| h.raw.seq == 3).unwrap();
        assert_eq!(read_raw(&result.0.raw).unwrap(), output.as_bytes());
        assert_eq!(result.0.metadata.timestamp_ms, None);
        let shell = search(Some("shell"), true, false);
        assert_eq!(shell.len(), 2);
        let multi = shell.iter().find(|(h, _)| h.raw.seq == 2).unwrap();
        assert_eq!(multi.0.metadata.tool_names, vec!["files", "shell"]);
        assert_eq!(search(Some("files"), true, false).len(), 1);
        assert_eq!(search(Some("foreign"), true, false).len(), 0);
        assert_eq!(search(Some("unknown"), true, false).len(), 1);
        assert_eq!(search(None, true, true).len(), 5);
        assert!(search(None, false, false).is_empty());
        let mut index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let cursor = index.source_cursor(own.id.as_str(), &native).unwrap();
        let doc = browser[0].0.id.clone();
        let failure = index
            .refresh_source(|index| -> Result<(), IndexError> {
                index.save_call(own.id.as_str(), &native, "injected", "wrong", &doc)?;
                index.clear_source(own.id.as_str(), &native)?;
                Err(io::Error::other("injected metadata rollback").into())
            })
            .unwrap_err();
        assert!(failure.to_string().contains("injected metadata rollback"));
        assert!(
            index
                .call_tool(own.id.as_str(), &native, "injected")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            index
                .call_tool(own.id.as_str(), &native, "same")
                .unwrap()
                .as_deref(),
            Some("browser")
        );
        assert_eq!(
            serde_json::to_string(&cursor).unwrap(),
            serde_json::to_string(&index.source_cursor(own.id.as_str(), &native).unwrap()).unwrap()
        );
        assert_eq!(
            index
                .call_tool(peer.id.as_str(), &foreign, "same")
                .unwrap()
                .as_deref(),
            Some("foreign")
        );
        // A valid earlier append plus excessive later metadata must publish
        // neither the new correlation nor an advanced source cursor.
        let excessive = serde_json::json!({"content": (0..1025).map(|_| serde_json::json!({"type":"tool_use","id":"too-many","name":"wrong"})).collect::<Vec<_>>()});
        fs::OpenOptions::new().append(true).open(&native).unwrap().write_all(format!("{{\"type\":\"function_call\",\"call_id\":\"partial\",\"name\":\"not-published\"}}\n{excessive}\n").as_bytes()).unwrap();
        let failure = refresh_native(&mut index, &own, &native)
            .err()
            .expect("metadata cap must refuse refresh");
        assert!(
            failure
                .to_string()
                .contains("native event exceeds tool metadata limit")
        );
        assert!(
            index
                .call_tool(own.id.as_str(), &native, "partial")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            serde_json::to_string(&cursor).unwrap(),
            serde_json::to_string(&index.source_cursor(own.id.as_str(), &native).unwrap()).unwrap()
        );
        // Replacement must discard old call correlations, not attach stale names.
        fs::write(&native, &output).unwrap();
        refresh_native(&mut index, &own, &native).unwrap();
        assert!(
            index
                .call_tool(own.id.as_str(), &native, "same")
                .unwrap()
                .is_none()
        );
        assert_eq!(search(Some("unknown"), true, false).len(), 2);
        index
            .retain_agent_documents(own.id.as_str(), &HashSet::new())
            .unwrap();
        assert!(
            index
                .source_cursor(own.id.as_str(), &native)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            index
                .call_tool(peer.id.as_str(), &foreign, "same")
                .unwrap()
                .as_deref(),
            Some("foreign")
        );
    }

    #[test]
    fn native_times_preserve_explicit_values_and_leave_absent_or_invalid_unknown() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("native-time", "spiron"))
            .unwrap();
        let path = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("config/sylphx-transcript.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        // Source-shaped synthetic fixtures, not captured runtime transcripts.
        fs::write(&path, concat!(
            "{\"role\":\"user\",\"timestamp\":\"2026-11-07T04:00:00.123Z\",\"content\":\"timefixture iso\"}\n",
            "{\"role\":\"user\",\"timestamp_ms\":0,\"content\":\"timefixture epoch\"}\n",
            "{\"role\":\"user\",\"content\":\"timefixture absent\"}\n",
            "{\"role\":\"user\",\"timestamp\":\"invalid\",\"content\":\"timefixture invalid\"}\n"
        )).unwrap();
        let ids = [agent.id.to_string()];
        let query = HistorySearchQuery {
            query: Some("timefixture".into()),
            sort: crate::HistorySort::Oldest,
            ..Default::default()
        };
        let hits = store
            .search_host_files(&ids, MemoryScope::Transcripts, &query)
            .unwrap();
        assert_eq!(hits.len(), 4);
        for (needle, expected) in [
            ("iso", Some(1794024000123)),
            ("epoch", Some(0)),
            ("absent", None),
            ("invalid", None),
        ] {
            let hit = hits
                .iter()
                .find(|(_, b)| String::from_utf8_lossy(b).contains(needle))
                .unwrap();
            assert_eq!(hit.0.metadata.timestamp_ms, expected, "{needle}");
        }
        drop(store);
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let filtered = store
            .search_host_files(
                &ids,
                MemoryScope::Transcripts,
                &HistorySearchQuery {
                    since_ms: Some(-1),
                    until_ms: Some(1),
                    ..query
                },
            )
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0.metadata.timestamp_ms, Some(0));
        assert_eq!(
            serde_json::to_value(
                &hits
                    .iter()
                    .find(|(h, _)| h.metadata.timestamp_ms.is_none())
                    .unwrap()
                    .0
                    .metadata
            )
            .unwrap()["timestamp_ms"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn reconciled_non_jsonl_history_is_full_text_scoped_and_read_back_losslessly() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let own = store
            .create_agent(NewAgent::new("page-owner", "spiron"))
            .unwrap();
        let hidden = store
            .create_agent(NewAgent::new("page-hidden", "spiron"))
            .unwrap();
        let mut own_path = PathBuf::new();
        for agent in [&own, &hidden] {
            let root = dir
                .path()
                .join("agents")
                .join(agent.id.as_str())
                .join("history/archive/pagehash");
            fs::create_dir_all(&root).unwrap();
            let path = root.join("history.md");
            fs::write(
                &path,
                format!(
                    "# historical page\n{} pagetailneedle",
                    "padding ".repeat(2000)
                ),
            )
            .unwrap();
            if agent.id == own.id {
                own_path = path;
            }
        }
        let ids = [own.id.to_string()];
        let q = HistorySearchQuery {
            query: Some("pagetailneedle".into()),
            ..Default::default()
        };
        assert!(
            store
                .search_host_files(&ids, MemoryScope::Memory, &q)
                .unwrap()
                .is_empty()
        );
        let hits = store
            .search_host_files(&ids, MemoryScope::Transcripts, &q)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0.metadata.agent_id, own.id.as_str());
        assert_eq!(hits[0].0.metadata.role.as_deref(), Some("history"));
        assert_eq!(
            read_raw(&hits[0].0.raw).unwrap(),
            fs::read(&own_path).unwrap()
        );
        assert!(String::from_utf8_lossy(&hits[0].1).contains("pagetailneedle"));
        assert!(
            store
                .search_host_files(&[], MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn boundary_fast_path_reads_bounded_prefix_and_periodic_verifier_recovers_older_edits() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("boundary-owner", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        fs::create_dir_all(root.join("config")).unwrap();
        let path = root.join("config/sylphx-transcript.jsonl");
        let prefix = format!(
            "{{\"content\":\"oldneedle {}\"}}\n",
            "padding ".repeat(40_000)
        );
        fs::write(&path, &prefix).unwrap();
        let index_path = dir.path().join(INDEX_FILE);
        let mut index = HostIndex::open(&index_path).unwrap();
        let t0 = crate::now_ms();
        refresh_native_at(&mut index, &agent, &path, false, t0).unwrap();
        let suffix = b"{\"content\":\"suffixneedle\"}\n";
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(suffix)
            .unwrap();
        let append = refresh_native_at(&mut index, &agent, &path, false, t0 + 1).unwrap();
        assert_eq!(
            append.prefix_verified_bytes,
            crate::host_cursor::BOUNDARY_BYTES
        );
        assert_eq!(
            append.boundary_capture_bytes,
            crate::host_cursor::BOUNDARY_BYTES
        );
        assert_eq!(append.suffix_scanned_bytes, suffix.len() as u64);
        assert_eq!(append.indexed_records, 1);
        assert_eq!(append.documents.len(), 2);
        // Same-size edit inside the last boundary must trigger full replacement.
        let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(prefix.len() as u64 + 12))
            .unwrap();
        file.write_all(b"inside").unwrap();
        let inside = refresh_native_at(&mut index, &agent, &path, false, t0 + 2).unwrap();
        assert_eq!(inside.indexed_records, 2);
        assert_eq!(
            inside.suffix_scanned_bytes,
            (prefix.len() + suffix.len()) as u64
        );
        // An old edit outside the boundary is the documented delayed-detection
        // case, even when an append follows it. Per-hit SHA still refuses stale
        // returned bytes immediately, before the full verifier catches up.
        file.seek(SeekFrom::Start(12)).unwrap();
        file.write_all(b"newneedle").unwrap();
        let mut append_file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        append_file.write_all(suffix).unwrap();
        let outside = refresh_native_at(&mut index, &agent, &path, false, t0 + 3).unwrap();
        assert_eq!(outside.indexed_records, 1);
        assert_eq!(
            outside.prefix_verified_bytes,
            crate::host_cursor::BOUNDARY_BYTES
        );
        let query = |term: &str| HistorySearchQuery {
            query: Some(term.into()),
            ..Default::default()
        };
        let ids = [agent.id.to_string()];
        assert!(
            index
                .search(&ids, MemoryScope::Transcripts, &query("newneedle"))
                .unwrap()
                .is_empty()
        );
        let stale = index
            .search(&ids, MemoryScope::Transcripts, &query("oldneedle"))
            .unwrap();
        assert_eq!(stale.len(), 1);
        assert!(read_raw(&stale[0].window).is_err());
        drop(index);
        assert!(
            !store
                .verify_one_host_history_source(t0 + 3_599_000)
                .unwrap()
        );
        assert!(
            store
                .verify_one_host_history_source(t0 + 3_600_003)
                .unwrap()
        );
        let mut index = HostIndex::open(&index_path).unwrap();
        assert!(
            index
                .search(&ids, MemoryScope::Transcripts, &query("oldneedle"))
                .unwrap()
                .is_empty()
        );
        let fresh = index
            .search(&ids, MemoryScope::Transcripts, &query("newneedle"))
            .unwrap();
        assert_eq!(fresh.len(), 1);
        assert!(
            String::from_utf8_lossy(&read_raw(&fresh[0].window).unwrap()).contains("newneedle")
        );
        // Shrink and a new inode use the complete-source path, not suffix reuse.
        fs::write(&path, suffix).unwrap();
        let shrink = refresh_native_at(&mut index, &agent, &path, false, t0 + 3_600_004).unwrap();
        assert_eq!(
            (
                shrink.prefix_verified_bytes,
                shrink.suffix_scanned_bytes,
                shrink.indexed_records
            ),
            (0, suffix.len() as u64, 1)
        );
        let replacement = root.join("replacement");
        fs::write(&replacement, &prefix).unwrap();
        fs::rename(&replacement, &path).unwrap();
        let replaced = refresh_native_at(&mut index, &agent, &path, false, t0 + 3_600_005).unwrap();
        assert_eq!(
            (
                replaced.prefix_verified_bytes,
                replaced.suffix_scanned_bytes,
                replaced.indexed_records
            ),
            (0, prefix.len() as u64, 1)
        );
    }

    #[test]
    fn periodic_verifier_rotates_failed_sources_without_advancing_their_frontier() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("verifier-owner", "spiron"))
            .unwrap();
        let root = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("transcripts");
        fs::create_dir_all(&root).unwrap();
        let bad = root.join("a.jsonl");
        let good = root.join("b.jsonl");
        fs::write(&bad, "{\"content\":\"badneedle\"}\n").unwrap();
        fs::write(&good, "{\"content\":\"goodneedle\"}\n").unwrap();
        let mut index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let t0 = crate::now_ms();
        refresh_native_at(&mut index, &agent, &bad, false, t0).unwrap();
        refresh_native_at(&mut index, &agent, &good, false, t0).unwrap();
        let before = index
            .source_cursor(agent.id.as_str(), &bad)
            .unwrap()
            .unwrap();
        fs::write(&bad, "malformed complete line\n").unwrap();
        drop(index);
        let error = store
            .verify_one_host_history_source(t0 + 3_600_001)
            .unwrap_err();
        assert!(matches!(error, IndexError::Io(ref e) if e.kind() == io::ErrorKind::InvalidData));
        assert!(
            store
                .verify_one_host_history_source(t0 + 3_600_002)
                .unwrap()
        );
        let index = HostIndex::open(&dir.path().join(INDEX_FILE)).unwrap();
        let after = index
            .source_cursor(agent.id.as_str(), &bad)
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                after.end,
                after.next_seq,
                after.boundary_sha256,
                after.stamp,
                after.full_verified_ms
            ),
            (
                before.end,
                before.next_seq,
                before.boundary_sha256,
                before.stamp,
                before.full_verified_ms
            )
        );
        assert_eq!(after.verification_attempt_ms, t0 + 3_600_001);
        assert_eq!(
            index
                .source_cursor(agent.id.as_str(), &good)
                .unwrap()
                .unwrap()
                .full_verified_ms,
            t0 + 3_600_002
        );
    }

    #[test]
    fn memory_changed_during_publication_rolls_back_documents_and_cursor() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("MEMORY.md");
        let mut index = HostIndex::open(&dir.path().join("index.db")).unwrap();
        let metadata = IndexMetadata {
            agent_id: "owner".into(),
            source: MemoryScope::Memory,
            timestamp_ms: Some(0),
            direction: "in".into(),
            sender: "owner".into(),
            destination: "owner".into(),
            tool_name: None,
            tool_names: Vec::new(),
            role: Some("memory".into()),
        };
        fs::write(&path, "committedneedle").unwrap();
        refresh_document(&mut index, "memory:owner:one", &metadata, &path).unwrap();
        let before = serde_json::to_string(&index.source_cursor("owner", &path).unwrap()).unwrap();
        fs::write(&path, "uncommittedneedle").unwrap();
        let error = refresh_document_before_publish(
            &mut index,
            "memory:owner:one",
            &metadata,
            &path,
            || fs::write(&path, "racingneedle"),
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::Io(ref e) if e.kind() == io::ErrorKind::WouldBlock));
        assert_eq!(
            serde_json::to_string(&index.source_cursor("owner", &path).unwrap()).unwrap(),
            before
        );
        let query = |term: &str| HistorySearchQuery {
            query: Some(term.into()),
            ..Default::default()
        };
        assert_eq!(
            index
                .search(
                    &["owner".into()],
                    MemoryScope::Memory,
                    &query("committedneedle")
                )
                .unwrap()
                .len(),
            1
        );
        assert!(
            index
                .search(
                    &["owner".into()],
                    MemoryScope::Memory,
                    &query("uncommittedneedle")
                )
                .unwrap()
                .is_empty()
        );
        refresh_document(&mut index, "memory:owner:one", &metadata, &path).unwrap();
        assert_eq!(
            index
                .search(
                    &["owner".into()],
                    MemoryScope::Memory,
                    &query("racingneedle")
                )
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn scoped_refresh_ignores_unrelated_failures_and_preserves_other_class() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("scoped-owner", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        fs::create_dir_all(root.join("memory")).unwrap();
        fs::create_dir_all(root.join("config")).unwrap();
        let memory = root.join("memory/MEMORY.md");
        let native = root.join("config/sylphx-transcript.jsonl");
        fs::write(&memory, "sharedneedle").unwrap();
        fs::write(&native, "{\"content\":\"sharedneedle\"}\n").unwrap();
        let ids = [agent.id.to_string()];
        let q = HistorySearchQuery {
            query: Some("sharedneedle".into()),
            ..Default::default()
        };
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::All, &q)
                .unwrap()
                .len(),
            2
        );
        let cursor = |path: &Path| {
            serde_json::to_string(
                &HostIndex::open(&dir.path().join(INDEX_FILE))
                    .unwrap()
                    .source_cursor(agent.id.as_str(), path)
                    .unwrap(),
            )
            .unwrap()
        };
        let native_before = cursor(&native);
        fs::write(&native, "invalid complete JSON record\n").unwrap();
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::Memory, &q)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(cursor(&native), native_before);
        assert!(store.search_host_files(&ids, MemoryScope::All, &q).is_err());
        fs::write(&native, "{\"content\":\"sharedneedle\"}\n").unwrap();
        let memory_before = cursor(&memory);
        fs::hard_link(&memory, root.join("hardlink")).unwrap();
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(cursor(&memory), memory_before);
        assert!(store.search_host_files(&ids, MemoryScope::All, &q).is_err());
    }

    #[test]
    fn persistent_cursor_reuses_unchanged_and_authenticates_append_edit_replace_and_tail() {
        use std::io::Write;
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("cursor-owner", "spiron"))
            .unwrap();
        let path = dir.path().join("native.jsonl");
        let prefix = b"{\"role\":\"user\",\"content\":\"originalneedle\"}\n{\"role\":\"assistant\",\"content\":\"secondneedle\"}\n";
        let tail = b"{\"role\":\"tool\",\"content\":\"partial";
        fs::write(&path, [prefix.as_slice(), tail.as_slice()].concat()).unwrap();
        let index_path = dir.path().join("index.db");
        let mut index = HostIndex::open(&index_path).unwrap();
        let first = refresh_native(&mut index, &agent, &path).unwrap();
        assert_eq!(first.indexed_records, 2);
        assert_eq!(first.prefix_verified_bytes, 0);
        assert_eq!(
            first.suffix_scanned_bytes,
            (prefix.len() + tail.len()) as u64
        );
        assert_eq!(
            index
                .source_cursor(agent.id.as_str(), &path)
                .unwrap()
                .unwrap()
                .end,
            prefix.len() as u64
        );
        drop(index);
        let mut index = HostIndex::open(&index_path).unwrap();
        let warm = refresh_native(&mut index, &agent, &path).unwrap();
        assert_eq!(
            (
                warm.prefix_verified_bytes,
                warm.suffix_scanned_bytes,
                warm.indexed_records
            ),
            (0, 0, 0)
        );
        assert_eq!(warm.documents.len(), 2);
        let suffix = b"needle\"}\n{\"role\":\"user\",\"content\":\"newneedle\"}\n";
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(suffix)
            .unwrap();
        let appended = refresh_native(&mut index, &agent, &path).unwrap();
        assert_eq!(appended.prefix_verified_bytes, prefix.len() as u64);
        assert_eq!(
            appended.suffix_scanned_bytes,
            (tail.len() + suffix.len()) as u64
        );
        assert_eq!(appended.indexed_records, 2);
        assert_eq!(appended.documents.len(), 4);
        // A middle/prefix edit followed by append must not look append-only.
        let edited = String::from_utf8(fs::read(&path).unwrap())
            .unwrap()
            .replace("originalneedle", "rewrittenneedle")
            + "{\"content\":\"fifthneedle\"}\n";
        fs::write(&path, &edited).unwrap();
        let changed = refresh_native(&mut index, &agent, &path).unwrap();
        assert_eq!(changed.indexed_records, 5);
        assert_eq!(changed.suffix_scanned_bytes, edited.len() as u64);
        let search = |index: &HostIndex, term: &str| {
            index
                .search(
                    &[agent.id.to_string()],
                    MemoryScope::Transcripts,
                    &HistorySearchQuery {
                        query: Some(term.into()),
                        ..Default::default()
                    },
                )
                .unwrap()
        };
        assert!(search(&index, "originalneedle").is_empty());
        assert_eq!(search(&index, "rewrittenneedle").len(), 1);
        let replacement = dir.path().join("replacement");
        fs::write(&replacement, b"{\"content\":\"replacementneedle\"}\n").unwrap();
        fs::rename(replacement, &path).unwrap();
        let replaced = refresh_native(&mut index, &agent, &path).unwrap();
        assert_eq!(replaced.indexed_records, 1);
        assert_eq!(replaced.prefix_verified_bytes, 0);
        assert_eq!(replaced.documents.len(), 1);
        assert!(search(&index, "rewrittenneedle").is_empty());
        assert_eq!(
            read_raw(&search(&index, "replacementneedle")[0].raw).unwrap(),
            fs::read(&path).unwrap()
        );
        // A batch can write both documents and its cursor before an injected
        // interruption; the outer transaction must roll all of them back.
        let before =
            serde_json::to_string(&index.source_cursor(agent.id.as_str(), &path).unwrap()).unwrap();
        let failure = index
            .refresh_source(|index| -> Result<(), IndexError> {
                index.clear_source(agent.id.as_str(), &path)?;
                let raw = document_coordinate(&path)?;
                index.index(
                    "uncommitted-batch",
                    &event_metadata(&agent, &EventHeader::default()),
                    &raw,
                )?;
                let mut advanced: SourceCursor =
                    serde_json::from_str::<Option<SourceCursor>>(&before)?.unwrap();
                advanced.next_seq = 999;
                index.save_source(agent.id.as_str(), &path, &advanced)?;
                Err(io::Error::other("injected before source publication").into())
            })
            .unwrap_err();
        assert!(
            failure
                .to_string()
                .contains("injected before source publication")
        );
        assert_eq!(
            serde_json::to_string(&index.source_cursor(agent.id.as_str(), &path).unwrap()).unwrap(),
            before
        );
        assert_eq!(search(&index, "replacementneedle").len(), 1);
        index
            .retain_agent_documents(agent.id.as_str(), &HashSet::new())
            .unwrap();
        assert!(
            index
                .source_cursor(agent.id.as_str(), &path)
                .unwrap()
                .is_none()
        );
        assert!(search(&index, "replacementneedle").is_empty());
        refresh_native(&mut index, &agent, &path).unwrap();
        fs::write(&path, b"").unwrap();
        assert!(
            refresh_native(&mut index, &agent, &path)
                .unwrap()
                .documents
                .is_empty()
        );
    }

    #[test]
    fn indexed_native_history_cannot_bypass_redaction_or_symlink_privacy() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("redaction-owner", "spiron"))
            .unwrap();
        store.set_agent_state(&agent.id, "archived").unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        fs::create_dir_all(root.join("config")).unwrap();
        fs::write(
            root.join("config/sylphx-transcript.jsonl"),
            concat!(
                "{\"type\":\"function_call\",\"call_id\":\"redaction-call\",\"name\":\"browser\"}\n",
                "{\"type\":\"function_call_output\",\"call_id\":\"redaction-call\",\"output\":\"secrettoken remainingword\"}\n"
            ),
        )
        .unwrap();
        let ids = [agent.id.to_string()];
        let mut q = HistorySearchQuery {
            query: Some("secrettoken".into()),
            ..Default::default()
        };
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap()
                .len(),
            1
        );
        let native = root.join("config/sylphx-transcript.jsonl");
        assert!(
            HostIndex::open(&dir.path().join(INDEX_FILE))
                .unwrap()
                .source_cursor(agent.id.as_str(), &native)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            HostIndex::open(&dir.path().join(INDEX_FILE))
                .unwrap()
                .call_tool(agent.id.as_str(), &native, "redaction-call")
                .unwrap()
                .as_deref(),
            Some("browser")
        );
        // An unrelated unsupported binary copy produces a partial receipt but
        // must not hide the successfully scrubbed local transcript.
        fs::write(root.join("config/unrelated.sqlite"), b"unmodified binary").unwrap();
        let mut receipt = store
            .redact_conversation(
                crate::redaction::Actor::Operator,
                &agent.id,
                &crate::redaction::Request {
                    message_id: Some("transcript:1".into()),
                    seq: None,
                    span: Some(crate::redaction::Span {
                        unit: crate::redaction::SpanUnit::Bytes,
                        start: 0,
                        end: 11,
                    }),
                    reason: "privacy request".into(),
                },
            )
            .unwrap();
        assert!(!receipt.full_scrub, "{receipt:?}");
        assert!(
            HostIndex::open(&dir.path().join(INDEX_FILE))
                .unwrap()
                .source_cursor(agent.id.as_str(), &native)
                .unwrap()
                .is_none()
        );
        assert!(
            HostIndex::open(&dir.path().join(INDEX_FILE))
                .unwrap()
                .call_tool(agent.id.as_str(), &native, "redaction-call")
                .unwrap()
                .is_none()
        );
        assert!(
            receipt
                .unscrubbed
                .iter()
                .any(|f| f.path.ends_with("unrelated.sqlite"))
        );
        assert!(
            store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap()
                .is_empty()
        );
        q.query = Some("remainingword".into());
        let hits = store
            .search_host_files(&ids, MemoryScope::Transcripts, &q)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(String::from_utf8_lossy(&hits[0].1).contains("[redacted]"));
        let raw_id = hits[0].0.id.clone();
        let raw_sha = hits[0].0.raw.sha256.clone();
        assert!(
            self::Store::read_host_file_range(
                &store,
                &ids,
                MemoryScope::Transcripts,
                None,
                HostRawRead {
                    id: &raw_id,
                    sha256: &raw_sha,
                    offset: 0,
                    length: 65536
                }
            )
            .unwrap()
            .is_some()
        );
        // Persisted pending directory exclusions survive a Store reopen and
        // apply to descendants, not merely exact filenames.
        receipt.unscrubbed.push(crate::redaction::CopyFailure {
            path: root.join("config").display().to_string(),
            error: "scrub pending/interrupted".into(),
        });
        store
            .conn()
            .execute(
                "UPDATE conversation_redactions SET receipt=?1 WHERE id=?2",
                rusqlite::params![serde_json::to_string(&receipt).unwrap(), receipt.audit_id],
            )
            .unwrap();
        drop(store);
        let store = Store::open(dir.path().join("state.db")).unwrap();
        assert!(
            store
                .read_host_file_range(
                    &ids,
                    MemoryScope::Transcripts,
                    None,
                    HostRawRead {
                        id: &raw_id,
                        sha256: &raw_sha,
                        offset: 0,
                        length: 65536
                    }
                )
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .search_host_files(&ids, MemoryScope::Transcripts, &q)
                .unwrap()
                .is_empty()
        );
        #[cfg(unix)]
        {
            let foreign = dir.path().join("foreign");
            fs::create_dir_all(&foreign).unwrap();
            fs::write(foreign.join("MEMORY.md"), b"foreignsecret").unwrap();
            std::os::unix::fs::symlink(&foreign, root.join("memory")).unwrap();
            q.query = Some("foreignsecret".into());
            assert!(
                store
                    .search_host_files(&ids, MemoryScope::All, &q)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn host_selection_normalizes_wildcards_names_and_historical_aliases() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("old-name", "spiron"))
            .unwrap();
        store.rename_agent(&agent.id, "new-name").unwrap();
        let root = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("memory");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("MEMORY.md"), "positivehostneedle").unwrap();
        let ids = [agent.id.to_string()];
        for selector in [
            "*",
            "ALL",
            "all",
            "old-name",
            "new-name",
            "agent:old-name",
            agent.id.as_str(),
        ] {
            let q = HistorySearchQuery {
                query: Some("positivehostneedle".into()),
                agent: Some(selector.into()),
                ..Default::default()
            };
            assert_eq!(
                store
                    .search_host_files(&ids, MemoryScope::Memory, &q)
                    .unwrap()
                    .len(),
                1,
                "{selector}"
            );
            assert!(
                store
                    .search_host_files(&[], MemoryScope::Memory, &q)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn authorized_full_text_native_in_place_and_memory_share_filters() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let own = store.create_agent(NewAgent::new("own", "spiron")).unwrap();
        let hidden = store
            .create_agent(NewAgent::new("hidden", "spiron"))
            .unwrap();
        for agent in [&own, &hidden] {
            let root = dir.path().join("agents").join(agent.id.as_str());
            fs::create_dir_all(root.join("memory")).unwrap();
            fs::create_dir_all(root.join("config")).unwrap();
            fs::write(
                root.join("memory/MEMORY.md"),
                format!("{} lateuniqueneedle 繁體中文", "padding ".repeat(1700)),
            )
            .unwrap();
            fs::write(root.join("config/sylphx-transcript.jsonl"),serde_json::to_string(&serde_json::json!({"role":"tool","tool":"browser","timestamp_ms":123,"output":format!("{}\nlateuniqueneedle", "padding ".repeat(1700))})).unwrap()+"\n{\"partial\":").unwrap();
        }
        let source = dir
            .path()
            .join("agents")
            .join(own.id.as_str())
            .join("config/sylphx-transcript.jsonl");
        let before = fs::read(&source).unwrap();
        let mut q = HistorySearchQuery {
            query: Some("lateuniqueneedle".into()),
            ..Default::default()
        };
        let ids = [own.id.to_string()];
        let hits = store.search_host_files(&ids, MemoryScope::All, &q).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(
            hits.iter()
                .all(|(h, b)| h.metadata.agent_id == own.id.as_str()
                    && String::from_utf8_lossy(b).contains("lateuniqueneedle"))
        );
        assert_eq!(fs::read(&source).unwrap(), before);
        assert!(!source.with_extension("jsonl.ack").exists());
        q.only_tools = true;
        q.tool_name = Some("browser".into());
        assert_eq!(
            store
                .search_host_files(&ids, MemoryScope::All, &q)
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .search_host_files(&[], MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
        q.only_tools = false;
        q.tool_name = None;
        q.exclude = vec!["lateuniqueneedle".into()];
        assert!(
            store
                .search_host_files(&ids, MemoryScope::All, &q)
                .unwrap()
                .is_empty()
        );
        fs::remove_file(
            dir.path()
                .join("agents")
                .join(own.id.as_str())
                .join("memory/MEMORY.md"),
        )
        .unwrap();
        q.exclude.clear();
        assert!(
            store
                .search_host_files(&ids, MemoryScope::Memory, &q)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn generic_and_legacy_archives_remain_scoped_and_raw_readable_after_reopen() {
        let dir = crate::test_tempdir();
        let path = dir.path().join("state.db");
        let store = Store::open(&path).unwrap();
        let own = store
            .create_agent(NewAgent::new("archive-reader", "test"))
            .unwrap();
        let hidden = store
            .create_agent(NewAgent::new("archive-hidden", "test"))
            .unwrap();
        for agent in [&own, &hidden] {
            let root = dir.path().join("agents").join(agent.id.as_str());
            for folder in [
                root.join("history/archive"),
                root.join("history").join("owners"),
            ] {
                fs::create_dir_all(&folder).unwrap();
                fs::write(
                    folder.join("page.md"),
                    format!("archiveneedle {} {:?}", agent.name, folder),
                )
                .unwrap();
            }
        }
        let query = HistorySearchQuery {
            query: Some("archiveneedle".into()),
            ..Default::default()
        };
        let ids = [own.id.to_string()];
        for store in [store, Store::open(&path).unwrap()] {
            let hits = store
                .search_host_files(&ids, MemoryScope::Transcripts, &query)
                .unwrap();
            assert_eq!(hits.len(), 2);
            for (hit, bytes) in hits {
                assert_eq!(hit.metadata.agent_id, own.id.as_str());
                assert_eq!(read_raw(&hit.raw).unwrap(), bytes);
                assert!(String::from_utf8_lossy(&bytes).contains("archive-reader"));
            }
            assert!(
                store
                    .search_host_files(&[], MemoryScope::Transcripts, &query)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn memory_index_excludes_dotfiles_and_generated_index_when_topics_exist() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("topic-owner", "spiron"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        crate::learned_memory::write_sleep_state(
            &root,
            &crate::learned_memory::SleepState {
                last_consolidated_ms: 123,
                last_inbound_seq: 4,
                last_outbound_ms: 5,
                consolidated_topics: 1,
            },
        )
        .unwrap();
        crate::learned_memory::upsert_memory_topic(
            &root,
            &agent.name,
            None,
            "cache-architecture",
            Some("uniqueindexphrase for cache architecture"),
            "Detailed topic body with uniqueindexphrase",
            1700000000000,
        )
        .unwrap();
        fs::write(root.join("memory/.hidden-note.md"), "hiddensecretneedle").unwrap();
        assert!(root.join("memory/MEMORY.md").is_file());
        assert!(root.join("memory/.sleep-state.json").is_file());
        let ids = [agent.id.to_string()];
        let hits = store
            .search_host_files(
                &ids,
                MemoryScope::Memory,
                &HistorySearchQuery {
                    query: Some("uniqueindexphrase".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "MEMORY.md must not duplicate topic search hit"
        );
        assert!(
            hits[0]
                .0
                .raw
                .segment
                .ends_with("memory/cache-architecture.md")
        );
        let dot_hits = store
            .search_host_files(
                &ids,
                MemoryScope::Memory,
                &HistorySearchQuery {
                    query: Some("hiddensecretneedle".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            dot_hits.is_empty(),
            "dotfiles in memory/ must never be indexed"
        );
    }

    #[test]
    fn search_host_files_locks_one_agent_at_a_time_across_multi_agent_refresh_and_readback() {
        use std::time::{Duration, Instant};
        let dir = crate::test_tempdir();
        let db = dir.path().join("state.db");
        let store = Store::open(&db).unwrap();
        let a = store
            .create_agent(NewAgent::new("agent-a", "spiron"))
            .unwrap();
        let b = store
            .create_agent(NewAgent::new("agent-b", "spiron"))
            .unwrap();
        let (first, second) = if a.id.as_str() < b.id.as_str() {
            (a, b)
        } else {
            (b, a)
        };
        for agent in [&first, &second] {
            let mem_dir = dir
                .path()
                .join("agents")
                .join(agent.id.as_str())
                .join("memory");
            fs::create_dir_all(&mem_dir).unwrap();
            fs::write(
                mem_dir.join("MEMORY.md"),
                format!("multilockneedle {}", agent.name),
            )
            .unwrap();
        }
        let first_mem = dir
            .path()
            .join("agents")
            .join(first.id.as_str())
            .join("memory/MEMORY.md");
        let second_lock = store.lock_agent_lifecycle(&second.id).unwrap();
        let ids = vec![first.id.to_string(), second.id.to_string()];
        let search_db = db.clone();
        let index_path = dir.path().join(INDEX_FILE);
        let handle = std::thread::spawn(move || {
            let search_store = Store::open(&search_db).unwrap();
            search_store
                .search_host_files(
                    &ids,
                    MemoryScope::Memory,
                    &HistorySearchQuery {
                        query: Some("multilockneedle".into()),
                        ..Default::default()
                    },
                )
                .unwrap()
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut first_refreshed = false;
        let mut first_released = false;
        while Instant::now() < deadline {
            if !first_refreshed
                && index_path.exists()
                && HostIndex::open(&index_path)
                    .ok()
                    .and_then(|idx| idx.source_cursor(first.id.as_str(), &first_mem).ok())
                    .flatten()
                    .is_some()
            {
                first_refreshed = true;
            }
            if first_refreshed && let Ok(probe_first) = store.try_lock_agent_lifecycle(&first.id) {
                drop(probe_first);
                first_released = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            first_refreshed,
            "search_host_files did not refresh first agent before waiting on second"
        );
        // While search_host_files is waiting on `second.id`, `first.id`'s lifecycle lock
        // must already be released so at most one `.lifecycle` lock is held at a time.
        assert!(
            first_released,
            "first agent lifecycle lock remained held while acquiring second"
        );
        drop(second_lock);
        let hits = handle.join().unwrap();
        assert_eq!(hits.len(), 2);
    }
}
