//! Host-only file retrieval. All paths are derived from authorized registry IDs;
//! active native JSONL is read in place and never handed off or rewritten here.
use crate::{
    Agent, HistorySearchQuery, Store,
    host_index::{HostIndex, IndexError, IndexHit, IndexMetadata, MemoryScope},
    learned_memory::sha256,
    raw_segment::{RawCoordinate, read_raw, retained_jsonl_records},
};
use std::{
    collections::HashSet,
    fs, io,
    path::{Path, PathBuf},
};

pub const INDEX_FILE: &str = "host-history-index-v1.db";

impl Store {
    /// The caller has already resolved the authorized immutable ID set. Locks
    /// serialize refresh/readback against redaction, rename and lifecycle changes.
    pub fn search_host_files(
        &self,
        authorized: &[String],
        scope: MemoryScope,
        query: &HistorySearchQuery,
    ) -> Result<Vec<(IndexHit, Vec<u8>)>, IndexError> {
        let mut agents: Vec<Agent> = self
            .list_agents()
            .map_err(|e| io::Error::other(e.to_string()))?
            .into_iter()
            .filter(|a| authorized.contains(&a.id.to_string()))
            .filter(|a| {
                query
                    .agent
                    .as_deref()
                    .is_none_or(|id| id == "all" || id == a.id.as_str() || id == a.name)
            })
            .collect();
        agents.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        let mut guards = Vec::new();
        for a in &agents {
            guards.push(
                self.lock_agent_lifecycle(&a.id)
                    .map_err(|e| io::Error::other(e.to_string()))?,
            );
        }
        let mut index = HostIndex::open(&self.data_dir().join(INDEX_FILE))?;
        for agent in &agents {
            let root = self.data_dir().join("agents").join(agent.id.as_str());
            if !safe_components(self.data_dir(), &root)? {
                index.retain_agent_documents(agent.id.as_str(), &HashSet::new())?;
                continue;
            }
            let mut ids = HashSet::new();
            for path in regular_files(&root.join("memory"), true)? {
                let raw = document_coordinate(&path)?;
                let id = format!(
                    "memory:{}:{}",
                    agent.id,
                    sha256(path.as_os_str().as_encoded_bytes())
                );
                let metadata = IndexMetadata {
                    agent_id: agent.id.to_string(),
                    source: MemoryScope::Memory,
                    timestamp_ms: modified_ms(&path)?,
                    direction: "in".into(),
                    sender: format!("agent:{}", agent.name),
                    destination: format!("agent:{}", agent.name),
                    tool_name: None,
                    role: Some("memory".into()),
                };
                index.index(&id, &metadata, &raw)?;
                ids.insert(id);
            }
            // A failed/incomplete redaction must never expose a surviving local
            // copy. Successful scrubs are reindexed from current verified bytes.
            let blocked: bool = self.conn().query_row("SELECT EXISTS(SELECT 1 FROM conversation_redactions WHERE agent_id=?1 AND json_extract(receipt,'$.full_scrub') != 1)", [agent.id.as_str()], |r| r.get(0))?;
            if !blocked {
                for path in native_transcripts(&root)? {
                    for raw in retained_jsonl_records(&path)? {
                        use std::io::{Read, Seek, SeekFrom};
                        let mut file = crate::raw_segment::open_readonly(&path)?;
                        file.seek(SeekFrom::Start(raw.offset))?;
                        // Unknown content/output fields are skipped by the JSON
                        // visitor, not materialized as an unbounded Value.
                        let header: EventHeader = serde_json::from_reader(file.take(raw.length))?;
                        let metadata = event_metadata(agent, &header, modified_ms(&path)?);
                        let id = format!(
                            "transcript:{}:{}:{}",
                            agent.id,
                            sha256(path.as_os_str().as_encoded_bytes()),
                            raw.seq
                        );
                        index.index(&id, &metadata, &raw)?;
                        ids.insert(id);
                    }
                }
            }
            index.retain_agent_documents(agent.id.as_str(), &ids)?;
        }
        let visible: Vec<_> = agents.iter().map(|a| a.id.to_string()).collect();
        let mut request = query.clone();
        request.agent = None;
        let hits = index.search(&visible, scope, &request)?;
        hits.into_iter()
            .map(|hit| {
                // Return the matching bounded window, not an unbounded tool payload.
                // The whole-event coordinate remains available for explicit readback.
                let bytes = read_raw(&hit.window)?;
                Ok((hit, bytes))
            })
            .collect()
    }
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
#[derive(Default, serde::Deserialize)]
struct EventHeader {
    role: Option<String>,
    message: Option<EventRole>,
    tool_name: Option<String>,
    tool: Option<String>,
    name: Option<String>,
    direction: Option<String>,
    sender: Option<String>,
    destination: Option<String>,
    timestamp_ms: Option<i64>,
}
#[derive(Default, serde::Deserialize)]
struct EventRole {
    role: Option<String>,
}
fn event_metadata(agent: &Agent, value: &EventHeader, fallback_time: i64) -> IndexMetadata {
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
        timestamp_ms: value.timestamp_ms.unwrap_or(fallback_time),
        direction: direction.into(),
        sender: value.sender.clone().unwrap_or_else(|| role.into()),
        destination: value
            .destination
            .clone()
            .unwrap_or_else(|| format!("agent:{}", agent.name)),
        tool_name: tool,
        role: Some(role.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;
    #[test]
    fn indexed_native_history_cannot_bypass_redaction_or_symlink_privacy() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("redaction-owner", "sylphx"))
            .unwrap();
        store.set_agent_state(&agent.id, "archived").unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        fs::create_dir_all(root.join("config")).unwrap();
        fs::write(
            root.join("config/sylphx-transcript.jsonl"),
            b"{\"role\":\"user\",\"content\":\"secrettoken remainingword\"}\n",
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
        let receipt = store
            .redact_conversation(
                crate::redaction::Actor::Operator,
                &agent.id,
                &crate::redaction::Request {
                    message_id: Some("transcript:0".into()),
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
        assert!(receipt.full_scrub, "{receipt:?}");
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
    fn authorized_full_text_native_in_place_and_memory_share_filters() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let own = store.create_agent(NewAgent::new("own", "sylphx")).unwrap();
        let hidden = store
            .create_agent(NewAgent::new("hidden", "sylphx"))
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
}
