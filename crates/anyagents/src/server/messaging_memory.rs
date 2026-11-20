//! Learned-memory reads. Conversation/history visibility is intentionally separate.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::PathBuf;

use anyagents_core::learned_memory::{
    MemoryTopicEntry, SleepState, parse_topic_frontmatter_full, sanitize_topic_slug, topic_pin,
};
use anyagents_core::stateless::BlobEntry;
use anyagents_core::{Agent, Error, Store};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Json, Router, routing::get};
use serde::Deserialize;
use serde_json::{Value, json};

use super::super::types::{ApiError, AppState, spawn_store};
use crate::auth::Principal;

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/v1/agents/{name}/memory", get(memory_route))
}

#[derive(Default, Deserialize)]
struct MemoryQuery {
    topic: Option<String>,
    scope: Option<String>,
    query: Option<String>,
    prefix: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    #[serde(default)]
    status: bool,
}

const DEFAULT_LIMIT: usize = 25;
const MAX_LIMIT: usize = 50;
// The MCP envelope includes structured JSON and a JSON-encoded text copy.
// Leave room for the envelope itself and both array delimiters.
const LIST_WIRE_BUDGET: usize = 7500;

async fn memory_route(
    principal: Principal,
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<MemoryQuery>,
) -> Result<Json<Value>, ApiError> {
    spawn_store(move || read_memory(&state.store, &principal, &name, query).map(Json)).await
}

fn memory_forbidden() -> ApiError {
    ApiError::forbidden("cannot read another agent's private memory")
}

fn targets(
    store: &Store,
    principal: &Principal,
    name: &str,
    shared: bool,
    self_only: bool,
) -> Result<Vec<Agent>, ApiError> {
    let name = name.trim().strip_prefix("agent:").unwrap_or(name.trim());
    let self_name = name.eq_ignore_ascii_case("self") || name.eq_ignore_ascii_case("me");
    let caller = match principal {
        Principal::Agent { agent_id, .. } => {
            Some(store.agent(agent_id).map_err(ApiError::from_store)?)
        }
        Principal::Operator => {
            store
                .record_admin_audit("operator", "memory.read", name)
                .map_err(ApiError::from_store)?;
            None
        }
    };
    if shared {
        if self_only || caller.as_ref().is_some_and(Agent::is_isolated) {
            return Err(memory_forbidden());
        }
        if !self_name {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "shared memory does not accept a separate agent selector",
            ));
        }
        return Ok(store
            .list_agents()
            .map_err(ApiError::from_store)?
            .into_iter()
            .filter(|target| {
                target.scope() == "public"
                    && caller.as_ref().is_none_or(|caller| caller.id != target.id)
            })
            .collect());
    }
    if self_name || name.eq_ignore_ascii_case("all") || name == "*" {
        return caller.map(|a| vec![a]).ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "operator memory reads require an explicit agent name",
            )
        });
    }
    let target = store.agent_by_name(name).map_err(|err| match err {
        Error::AgentNotFound(_) if caller.is_some() => memory_forbidden(),
        other => ApiError::from_store(other),
    })?;
    if caller.as_ref().is_some_and(|caller| {
        caller.id != target.id && (self_only || caller.is_isolated() || target.scope() != "public")
    }) {
        return Err(memory_forbidden());
    }
    Ok(vec![target])
}

/// Read-only view over one authorized owner's learned memory. Never hydrate
/// blobs into a seat-writable path from this API. Local changes take precedence
/// over older snapshots, and redactions apply to both representations.
struct MemoryFiles<'a> {
    store: &'a Store,
    owner: &'a Agent,
    dir: PathBuf,
}

impl<'a> MemoryFiles<'a> {
    fn new(store: &'a Store, owner: &'a Agent) -> Self {
        let dir = store
            .data_dir()
            .join("agents")
            .join(owner.id.as_str())
            .join("memory");
        Self { store, owner, dir }
    }

    fn blocked(&self, filename: &str) -> bool {
        let owner_id = self.owner.id.as_str();
        let paths = [
            Some(self.dir.join(filename)),
            self.store
                .is_postgres()
                .then(|| anyagents_core::learned_memory::memory_blob_segment(owner_id, filename)),
        ];
        paths.into_iter().flatten().any(|path| {
            self.store
                .is_host_path_redacted(owner_id, &path)
                .unwrap_or(true)
        })
    }

    fn local(&self, filename: &str) -> Option<(Vec<u8>, i64)> {
        let path = self.dir.join(filename);
        if !std::fs::symlink_metadata(&path).ok()?.is_file() {
            return None;
        }
        let mut file = anyagents_core::raw_segment::open_readonly(&path).ok()?;
        let modified = file
            .metadata()
            .ok()?
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
            .unwrap_or(0);
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).ok()?;
        Some((bytes, modified))
    }

    fn load(&self, filename: &str, blob: Option<BlobEntry>) -> Option<(Vec<u8>, i64)> {
        if self.blocked(filename) || blob.as_ref().is_some_and(|b| b.encrypted) {
            return None;
        }
        let local = self.local(filename);
        match blob {
            Some(blob)
                if local
                    .as_ref()
                    .is_none_or(|(_, mtime)| *mtime < blob.updated_at_ms) =>
            {
                Some((blob.content, blob.updated_at_ms))
            }
            _ => local,
        }
    }

    fn file(&self, filename: &str) -> Result<Option<(Vec<u8>, i64)>, ApiError> {
        let blob = if self.store.is_postgres() {
            self.store
                .blob_store()
                .get_agent_file(&self.owner.id, "memory", filename)
                .map_err(ApiError::from_store)?
        } else {
            None
        };
        Ok(self.load(filename, blob))
    }

    fn topics(&self) -> Result<Vec<MemoryTopicEntry>, ApiError> {
        let mut files = BTreeMap::new();
        if std::fs::symlink_metadata(&self.dir).is_ok_and(|m| m.is_dir()) {
            let entries = std::fs::read_dir(&self.dir).map_err(|e| read_failed(&e))?;
            for entry in entries {
                if let Ok(filename) = entry
                    .map_err(|e| read_failed(&e))?
                    .file_name()
                    .into_string()
                    && topic_filename(&filename)
                {
                    files.insert(filename, None);
                }
            }
        }
        if self.store.is_postgres() {
            for blob in self
                .store
                .blob_store()
                .get_agent_files_all(&self.owner.id, "memory")
                .map_err(ApiError::from_store)?
            {
                if topic_filename(&blob.path) {
                    files.insert(blob.path.clone(), Some(blob));
                }
            }
        }
        Ok(files
            .into_iter()
            .filter_map(|(filename, blob)| {
                let (bytes, updated) = self.load(&filename, blob)?;
                Some(topic_entry(filename, &bytes, updated))
            })
            .collect())
    }
}

fn read_failed(err: &io::Error) -> ApiError {
    eprintln!("learned memory directory read failed: {err}");
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "could not list learned memory",
    )
}

fn topic_filename(filename: &str) -> bool {
    filename.len() <= 255
        && filename.ends_with(".md")
        && !filename.chars().any(char::is_control)
        && !filename.starts_with('.')
        && !filename.contains(['/', '\\'])
        && !filename.eq_ignore_ascii_case("MEMORY.md")
}

fn topic_entry(filename: String, bytes: &[u8], mtime: i64) -> MemoryTopicEntry {
    let slug = filename
        .strip_suffix(".md")
        .unwrap_or(&filename)
        .to_string();
    let raw = String::from_utf8_lossy(bytes);
    let (_, description, _, updated, _) = parse_topic_frontmatter_full(&slug, &raw);
    MemoryTopicEntry {
        slug,
        filename,
        description,
        updated_at_ms: updated.filter(|&time| time > 0).unwrap_or(mtime),
        bytes: bytes.len(),
        pinned: topic_pin(&raw).unwrap_or(false),
    }
}

fn read_memory(
    store: &Store,
    principal: &Principal,
    name: &str,
    query: MemoryQuery,
) -> Result<Value, ApiError> {
    let shared = match query.scope.as_deref() {
        None | Some("self") => false,
        Some("shared") => true,
        Some(_) => {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "memory scope must be self or shared",
            ));
        }
    };
    let owners = targets(store, principal, name, shared, query.status)?;
    if let Some(topic) = query.topic.as_deref() {
        let slug = sanitize_topic_slug(topic)
            .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
        let filename = format!("{slug}.md");
        let mut found = None;
        for owner in &owners {
            if let Some((bytes, modified)) = MemoryFiles::new(store, owner).file(&filename)? {
                if found.is_some() {
                    return Err(ApiError::conflict(
                        "multiple public agents have this topic; specify an agent from memory list",
                    ));
                }
                found = Some((owner, bytes, modified));
            }
        }
        let (owner, bytes, modified) =
            found.ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "memory topic not found"))?;
        let entry = topic_entry(filename, &bytes, modified);
        let content = String::from_utf8_lossy(&bytes);
        return Ok(json!({
            "action": "read", "agent": owner.name, "agent_id": owner.id.as_str(),
            "topic": entry.slug, "filename": entry.filename, "description": entry.description,
            "updated_at": entry.updated_at_ms, "pinned": entry.pinned, "content": content,
            "turns": [{
                "id": format!("memory:{}:{}", owner.id, entry.slug), "text": content,
                "source": "memory", "direction": "in", "from": format!("agent:{}", owner.name),
                "to": format!("agent:{}", owner.name), "reply_to": entry.filename,
                "timestamp_ms": entry.updated_at_ms,
            }],
        }));
    }
    if query.status {
        let owner = &owners[0];
        let files = MemoryFiles::new(store, owner);
        let count = files.topics()?.len();
        let index = files
            .file("MEMORY.md")?
            .map(|(bytes, _)| bytes)
            .unwrap_or_default();
        let state = files
            .file(".sleep-state.json")?
            .and_then(|(bytes, _)| serde_json::from_slice::<SleepState>(&bytes).ok())
            .unwrap_or_default();
        return Ok(json!({
            "action": "status", "agent": owner.name, "agent_id": owner.id.as_str(),
            "topic_count": count, "index_bytes": index.len(),
            "index_lines": String::from_utf8_lossy(&index).lines().count(),
            "last_consolidated_ms": state.last_consolidated_ms, "consolidated_topics": state.consolidated_topics,
        }));
    }
    let mut topics = Vec::new();
    for owner in &owners {
        topics.extend(
            MemoryFiles::new(store, owner)
                .topics()?
                .into_iter()
                .map(|topic| (owner, topic)),
        );
    }
    Ok(page(
        if shared { None } else { owners.first() },
        topics,
        &query,
    ))
}

fn page(
    owner: Option<&Agent>,
    mut topics: Vec<(&Agent, MemoryTopicEntry)>,
    args: &MemoryQuery,
) -> Value {
    let query = args
        .query
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    let prefix = args
        .prefix
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    topics.retain(|(_, t)| {
        let slug = t.slug.to_lowercase();
        slug.starts_with(&prefix)
            && (slug.contains(&query) || t.description.to_lowercase().contains(&query))
    });
    topics.sort_by(|(a_owner, a), (b_owner, b)| {
        b.pinned
            .cmp(&a.pinned)
            .then_with(|| b.updated_at_ms.cmp(&a.updated_at_ms))
            .then_with(|| a_owner.id.as_str().cmp(b_owner.id.as_str()))
            .then_with(|| a.slug.cmp(&b.slug))
    });
    let total_count = topics.len();
    let offset = args.offset.unwrap_or(0).min(total_count);
    let limit = args.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let mut output = json!({
        "action": "list", "agent": owner.map_or("shared", list_agent_label),
        "agent_id": owner.map(|a| a.id.as_str()), "total_count": total_count,
        "topic_count": total_count, "count": 0, "offset": offset,
        "has_more": false, "next_offset": null, "topics": [], "turns": [],
        "index": "Use topics below for this page; use memory read for topic content.",
    });
    // Reserve space for pagination counters and count each row in both forms
    // (including escaping in the text copy). Do not split or skip rows.
    let mut remaining = LIST_WIRE_BUDGET.saturating_sub(wire_size(&output) + 96);
    let mut rows = Vec::new();
    for (agent, t) in topics.into_iter().skip(offset).take(limit) {
        let description: String = t.description.chars().take(120).collect();
        let mut row = json!({
            "topic": t.slug, "slug": t.slug, "filename": t.filename, "description": description,
            "updated_at": t.updated_at_ms, "updated_at_ms": t.updated_at_ms, "pinned": t.pinned, "bytes": t.bytes,
        });
        if owner.is_none() {
            row["agent"] = json!(list_agent_label(agent));
            row["agent_id"] = json!(agent.id.as_str());
        }
        let cost = wire_size(&row) + 2;
        if cost > remaining {
            break;
        }
        remaining = remaining.saturating_sub(cost);
        rows.push(row);
    }
    let end = offset + rows.len();
    output["count"] = json!(rows.len());
    output["has_more"] = json!(end < total_count);
    output["next_offset"] = json!((end < total_count).then_some(end));
    output["topics"] = json!(rows);
    output
}

// Keep even an unusually long agent label from exhausting the list budget.
// Use its ID rather than truncating its name into an invalid selector.
fn list_agent_label(agent: &Agent) -> &str {
    if agent.name.len() <= 120 {
        &agent.name
    } else {
        agent.id.as_str()
    }
}

fn wire_size(value: &Value) -> usize {
    let json = value.to_string();
    json.len() + Value::String(json).to_string().len()
}
