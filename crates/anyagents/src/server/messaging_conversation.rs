//! Inbox, memory, conversation history, redaction, media serving, and operator inbox routes.

use std::path::Path;
use std::sync::Arc;

use anyagents_core::{Error, Store};
use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::api::{
    ConversationResponse, ConversationTurnView, InboxResponse, MessageView, OperatorInboxResponse,
};
use crate::auth::Principal;

use super::types::{
    ApiError, AppState, conversation_visibility, every_agent_id, is_self_or_ancestor,
    require_admin_or_operator, spawn_store,
};
use super::{hybrid, memory_backlog};

/// `GET /v1/operator/agents/{name}/inbox`: any agent's mailbox, for the
/// operator's or a root admin's reading. A different path from the agent's own
/// route, so a name can never be taken for an identity.
pub(super) async fn operator_inbox(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<Since>,
) -> Result<Json<InboxResponse>, ApiError> {
    spawn_store(move || {
        require_admin_or_operator(&state.store, &principal, "agents.operator_inbox", &name)?;
        let agent = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        inbox_of(
            &state.store,
            &agent.id,
            query.since,
            query.limit,
            query.unread.unwrap_or(false),
            query.sender.as_deref(),
        )
    })
    .await
}

/// `GET /v1/operator/inbox`: the reports agents sent to `operator`. Only the
/// operator reads them; every agent, a root admin included, may only write.
pub(super) async fn operator_reports(
    principal: Principal,
    State(state): State<AppState>,
    Query(query): Query<Since>,
) -> Result<Json<OperatorInboxResponse>, ApiError> {
    if !principal.is_operator() {
        return Err(ApiError::forbidden(
            "only the operator reads the operator inbox; an agent may only send to `operator`",
        ));
    }
    spawn_store(move || {
        let messages = state
            .store
            .operator_inbox(query.since, query.limit)
            .map_err(ApiError::from_store)?;
        Ok(Json(OperatorInboxResponse {
            messages: messages.into_iter().map(Into::into).collect(),
        }))
    })
    .await
}

pub(super) async fn media_file_route(
    _principal: Principal,
    State(state): State<AppState>,
    axum::extract::Path(file_name): axum::extract::Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let trimmed = file_name.trim();
    if trimmed.is_empty()
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains("..")
        || trimmed.contains('\0')
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid media filename",
        ));
    }
    let fname = trimmed.to_string();
    let store = Arc::clone(&state.store);
    let bytes = spawn_store(move || {
        if let Ok(Some(blob)) = store.blob_store().get_deployment_blob("media", &fname) {
            return Ok(Some(blob.content));
        }
        let local = store
            .data_dir()
            .join(anyagents_harness::media::MEDIA_DIR)
            .join(&fname);
        if local.is_file()
            && let Ok(raw) = std::fs::read(&local)
        {
            return Ok(Some(raw));
        }
        Ok(None)
    })
    .await?
    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "media file not found"))?;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        bytes,
    ))
}

#[derive(Deserialize)]
pub(super) struct Since {
    pub(super) since: Option<i64>,
    pub(super) limit: Option<usize>,
    pub(super) unread: Option<bool>,
    pub(super) sender: Option<String>,
}

#[derive(Default, Deserialize)]
pub(super) struct ConversationQuery {
    pub(super) all: Option<bool>,
    pub(super) raw_id: Option<String>,
    pub(super) raw_sha256: Option<String>,
    pub(super) raw_offset: Option<u64>,
    pub(super) raw_length: Option<u64>,
    #[serde(default)]
    pub(super) scope: anyagents_core::host_index::MemoryScope,
    #[serde(default, alias = "memory_topic", alias = "slug", alias = "file")]
    pub(super) topic: Option<String>,
    #[serde(default, alias = "summary", alias = "memory_description")]
    pub(super) description: Option<String>,
    #[serde(default, alias = "body", alias = "text", alias = "memory_content")]
    pub(super) content: Option<String>,
    #[serde(default, alias = "remove")]
    pub(super) delete: Option<bool>,
    pub(super) with: Option<String>,
    pub(super) since_ms: Option<i64>,
    pub(super) until_ms: Option<i64>,
    pub(super) from_seq: Option<i64>,
    pub(super) to_seq: Option<i64>,
    pub(super) direction: Option<String>,
    pub(super) sender: Option<String>,
    pub(super) include_tools: Option<bool>,
    pub(super) only_tools: Option<bool>,
    pub(super) tool_name: Option<String>,
    pub(super) sort: Option<String>,
    pub(super) radius: Option<usize>,
    pub(super) limit: Option<usize>,
    #[serde(default, alias = "q", alias = "search")]
    pub(super) query: Option<String>,
    #[serde(default)]
    pub(super) queries: Option<String>,
    #[serde(default)]
    pub(super) exclude: Option<String>,
}

pub(super) async fn inbox(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<Since>,
) -> Result<Json<InboxResponse>, ApiError> {
    // An agent's `inbox` is strictly its own incoming queue. To inspect another
    // agent's full two-way communication (`in` + `out` with users, groups, and
    // peers), agents use `/v1/agents/{name}/conversation`.
    spawn_store(move || {
        let agent_id = match &principal {
            Principal::Operator => {
                state
                    .store
                    .agent_by_name(&name)
                    .map_err(ApiError::from_store)?
                    .id
            }
            Principal::Agent {
                agent_id,
                name: own_name,
            } => {
                let matches_self = name == "self"
                    || name == agent_id.as_str()
                    || name == own_name.as_str()
                    || state
                        .store
                        .agent_by_name(&name)
                        .is_ok_and(|a| a.id == *agent_id);
                if !matches_self {
                    return Err(ApiError::forbidden(
                        "a session reads its own inbox, not another agent's; use conversation to read another agent's conversation",
                    ));
                }
                agent_id.clone()
            }
        };
        inbox_of(
            &state.store,
            &agent_id,
            query.since,
            query.limit,
            query.unread.unwrap_or(false),
            query.sender.as_deref(),
        )
    })
    .await
}

/// `conversation.redact`: authenticated ancestors and admins only. The store
/// repeats authorization inside the write transaction; peers and self are denied.
pub(super) async fn redact_conversation_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<anyagents_core::redaction::Request>,
) -> Result<Json<anyagents_core::redaction::Receipt>, ApiError> {
    spawn_store(move || {
        let target = state
            .store
            .agent_by_name(name.strip_prefix("agent:").unwrap_or(&name))
            .map_err(ApiError::from_store)?;
        let actor = match principal {
            Principal::Operator => anyagents_core::redaction::Actor::Operator,
            Principal::Agent { agent_id, .. } => anyagents_core::redaction::Actor::Agent(agent_id),
        };
        state
            .store
            .redact_conversation(actor, &target.id, &request)
            .map(Json)
            .map_err(ApiError::from_store)
    })
    .await
}

/// `GET /v1/agents/{name}/memory`: return the hydrated learned-memory topic list,
/// index markdown, and sleep-consolidation status for `name` (authorized with the
/// same visibility boundary as `conversation(scope="memory")`).
pub(super) async fn agent_memory_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    spawn_store(move || {
        let (resolved_name, visible_agent_ids) =
            conversation_visibility(&state.store, &principal, &name)?;
        let target = if resolved_name.eq_ignore_ascii_case("self")
            || resolved_name.eq_ignore_ascii_case("all")
            || resolved_name == "*"
        {
            match &principal {
                Principal::Agent { agent_id, .. } => {
                    state.store.agent(agent_id).map_err(ApiError::from_store)?
                }
                Principal::Operator => {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "operator memory listing requires an explicit agent name",
                    ));
                }
            }
        } else {
            state
                .store
                .agent_by_name(
                    resolved_name
                        .strip_prefix("agent:")
                        .unwrap_or(&resolved_name),
                )
                .map_err(ApiError::from_store)?
        };
        if let Some(visible) = visible_agent_ids.as_ref()
            && !visible
                .iter()
                .any(|v| v == target.id.as_str() || v == &target.name)
        {
            return Err(ApiError::forbidden(
                "cannot read private memory of another agent",
            ));
        }
        let agent_root = state
            .store
            .data_dir()
            .join("agents")
            .join(target.id.as_str());
        let _ = crate::sandbox_exec::hydrate_agent_memory_from_store(&state.store, &target.id);
        let mem_dir = agent_root.join("memory");
        let mem_dir_safe = std::fs::symlink_metadata(&mem_dir)
            .ok()
            .is_some_and(|m| m.file_type().is_dir());
        let read_no_follow = |path: &Path| -> Option<String> {
            use std::io::Read as _;
            if !mem_dir_safe
                || state
                    .store
                    .is_host_path_redacted(target.id.as_str(), path)
                    .unwrap_or(true)
            {
                return None;
            }
            let mut f = anyagents_core::raw_segment::open_readonly(path).ok()?;
            let mut bytes = Vec::new();
            f.read_to_end(&mut bytes).ok()?;
            Some(String::from_utf8_lossy(&bytes).into_owned())
        };
        let mut topics: Vec<anyagents_core::learned_memory::MemoryTopicEntry> = Vec::new();
        if mem_dir_safe && let Ok(entries) = std::fs::read_dir(&mem_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(fname) = path.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
                if fname.starts_with('.')
                    || fname.eq_ignore_ascii_case("MEMORY.md")
                    || !fname.ends_with(".md")
                {
                    continue;
                }
                let Ok(smeta) = std::fs::symlink_metadata(&path) else {
                    continue;
                };
                if !smeta.file_type().is_file() {
                    continue;
                }
                let Some(raw) = read_no_follow(&path) else {
                    continue;
                };
                let slug = fname.strip_suffix(".md").unwrap_or(fname).to_string();
                let (_, description, _, fm_updated_ms, _) =
                    anyagents_core::learned_memory::parse_topic_frontmatter_full(&slug, &raw);
                let fs_mtime_ms = smeta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
                    .unwrap_or(0);
                topics.push(anyagents_core::learned_memory::MemoryTopicEntry {
                    slug,
                    filename: fname.to_string(),
                    description,
                    updated_at_ms: fm_updated_ms.filter(|&ts| ts > 0).unwrap_or(fs_mtime_ms),
                    bytes: raw.len(),
                });
            }
            topics.sort_by(|a, b| {
                b.updated_at_ms
                    .cmp(&a.updated_at_ms)
                    .then_with(|| a.slug.cmp(&b.slug))
            });
        }
        let index_path = mem_dir.join("MEMORY.md");
        let index_is_symlink = std::fs::symlink_metadata(&index_path)
            .ok()
            .is_some_and(|m| m.file_type().is_symlink());
        let existing_index = if index_is_symlink {
            None
        } else {
            read_no_follow(&index_path)
        };
        let needs_index_rebuild = mem_dir_safe
            && !index_is_symlink
            && !topics.is_empty()
            && existing_index
                .as_deref()
                .is_none_or(|s| s.contains("No topic files recorded yet."));
        if needs_index_rebuild
            && let Ok(index) = anyagents_core::learned_memory::rebuild_memory_index(
                &agent_root,
                &target.name,
                target.role.as_deref(),
            )
        {
            crate::sandbox_exec::persist_agent_memory_file_to_store(
                &state.store,
                &target.id,
                "MEMORY.md",
                index.as_bytes(),
            );
        }
        let index_text = if index_is_symlink {
            String::new()
        } else {
            read_no_follow(&index_path).unwrap_or_else(|| {
                format!(
                    "# {} — Learned Memory Index\n- No topic files recorded yet.\n",
                    target.name
                )
            })
        };
        let sleep_path = mem_dir.join(".sleep-state.json");
        let sleep_state: anyagents_core::learned_memory::SleepState = read_no_follow(&sleep_path)
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        let topic_count = topics.len();
        Ok(Json(serde_json::json!({
            "agent": target.name,
            "agent_id": target.id.as_str(),
            "topics": topics,
            "topic_count": topic_count,
            "index": index_text,
            "index_bytes": index_text.len(),
            "index_lines": index_text.lines().count(),
            "last_consolidated_ms": sleep_state.last_consolidated_ms,
            "consolidated_topics": sleep_state.consolidated_topics,
        })))
    })
    .await
}

pub(super) async fn conversation_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(mut query): Query<ConversationQuery>,
) -> Result<Json<ConversationResponse>, ApiError> {
    if query.all == Some(true) {
        query.limit = None;
    }
    spawn_store(move || {
        if let Some(topic) = query
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let is_write = query.content.is_some() || query.delete == Some(true);
            if !is_write {
                let (resolved_name, visible_agent_ids) =
                    conversation_visibility(&state.store, &principal, &name)?;
                let target = if resolved_name.eq_ignore_ascii_case("self")
                    || resolved_name.eq_ignore_ascii_case("all")
                    || resolved_name == "*"
                {
                    match &principal {
                        Principal::Agent { agent_id, .. } => {
                            state.store.agent(agent_id).map_err(ApiError::from_store)?
                        }
                        Principal::Operator => {
                            return Err(ApiError::new(
                                StatusCode::BAD_REQUEST,
                                "operator memory topic read requires an explicit agent name",
                            ));
                        }
                    }
                } else {
                    state
                        .store
                        .agent_by_name(
                            resolved_name
                                .strip_prefix("agent:")
                                .unwrap_or(&resolved_name),
                        )
                        .map_err(ApiError::from_store)?
                };
                if let Some(visible) = visible_agent_ids.as_ref()
                    && !visible
                        .iter()
                        .any(|v| v == target.id.as_str() || v == &target.name)
                {
                    return Err(ApiError::forbidden(
                        "cannot read private memory of another agent",
                    ));
                }
                let agent_root = state
                    .store
                    .data_dir()
                    .join("agents")
                    .join(target.id.as_str());
                let _ = crate::sandbox_exec::hydrate_agent_memory_from_store(
                    &state.store,
                    &target.id,
                );
                let is_index_read = topic.eq_ignore_ascii_case("memory")
                    || topic.eq_ignore_ascii_case("memory.md")
                    || topic.eq_ignore_ascii_case("index");
                let (slug, filename) = if is_index_read {
                    ("MEMORY".to_string(), "MEMORY.md".to_string())
                } else {
                    let s = anyagents_core::learned_memory::sanitize_topic_slug(topic)
                        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                    let f = format!("{s}.md");
                    (s, f)
                };
                let path = agent_root.join("memory").join(&filename);
                let needs_index_rebuild = is_index_read
                    && (!path.is_file()
                        || std::fs::read_to_string(&path)
                            .ok()
                            .is_some_and(|s| s.contains("No topic files recorded yet.")));
                if needs_index_rebuild
                    && anyagents_core::learned_memory::list_memory_topics(&agent_root)
                        .is_ok_and(|topics| !topics.is_empty())
                    && let Ok(index) = anyagents_core::learned_memory::rebuild_memory_index(
                        &agent_root,
                        &target.name,
                        target.role.as_deref(),
                    )
                {
                    crate::sandbox_exec::persist_agent_memory_file_to_store(
                        &state.store,
                        &target.id,
                        "MEMORY.md",
                        index.as_bytes(),
                    );
                }
                if state
                    .store
                    .is_host_path_redacted(target.id.as_str(), &path)
                    .unwrap_or(true)
                {
                    return Err(ApiError::new(
                        StatusCode::NOT_FOUND,
                        format!("memory topic `{filename}` not found"),
                    ));
                }
                let mut bytes = Vec::new();
                let timestamp_ms = match anyagents_core::raw_segment::open_readonly(&path) {
                    Ok(mut file) => {
                        use std::io::Read as _;
                        file.read_to_end(&mut bytes)
                            .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                        let text_preview = String::from_utf8_lossy(&bytes);
                        let fm_updated_ms =
                            anyagents_core::learned_memory::parse_topic_frontmatter_full(
                                &slug,
                                &text_preview,
                            )
                            .3
                            .filter(|&ts| ts > 0);
                        fm_updated_ms.or_else(|| {
                            file.metadata()
                                .ok()
                                .and_then(|m| m.modified().ok())
                                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                .map(|d| d.as_millis() as i64)
                        })
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound && is_index_read => {
                        let rendered = format!(
                            "# {} — Learned Memory Index\n- No topic files recorded yet.\n",
                            target.name
                        );
                        bytes = rendered.into_bytes();
                        Some(anyagents_core::now_ms())
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Err(ApiError::new(
                            StatusCode::NOT_FOUND,
                            format!("memory topic `{filename}` not found"),
                        ));
                    }
                    Err(e) => {
                        return Err(ApiError::new(StatusCode::BAD_REQUEST, e.to_string()));
                    }
                };
                let text = String::from_utf8_lossy(&bytes).into_owned();
                let digest = anyagents_core::learned_memory::sha256(&bytes);
                let doc_id = format!(
                    "memory:{}:{}",
                    target.id,
                    anyagents_core::learned_memory::sha256(path.as_os_str().as_encoded_bytes())
                );
                let coord = anyagents_core::raw_segment::RawCoordinate {
                    segment: path,
                    offset: 0,
                    length: bytes.len() as u64,
                    sha256: digest,
                    seq: 1,
                };
                return Ok(Json(ConversationResponse {
                    retrieval: None,
                    agent: target.name.clone(),
                    agent_id: target.id.to_string(),
                    turns: vec![ConversationTurnView {
                        seq: 0,
                        id: doc_id,
                        text,
                        raw_bytes: None,
                        source: Some("memory".into()),
                        direction: "in".into(),
                        from: format!("agent:{}", target.name),
                        to: format!("agent:{}", target.name),
                        reply_to: Some(filename),
                        timestamp_ms,
                        raw: Some(coord.clone().into()),
                        window: Some(coord.into()),
                        tool_name: None,
                        tool_names: Vec::new(),
                        role: Some("memory".into()),
                    }],
                }));
            }
            let target = if name.eq_ignore_ascii_case("self")
                || name.eq_ignore_ascii_case("all")
                || name == "*"
            {
                match &principal {
                    Principal::Agent { agent_id, .. } => {
                        state.store.agent(agent_id).map_err(ApiError::from_store)?
                    }
                    Principal::Operator => {
                        return Err(ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "operator memory write requires an explicit agent name",
                        ));
                    }
                }
            } else {
                state
                    .store
                    .agent_by_name(name.strip_prefix("agent:").unwrap_or(&name))
                    .map_err(ApiError::from_store)?
            };
            let allowed = match &principal {
                Principal::Operator => true,
                Principal::Agent { agent_id, .. } => {
                    is_self_or_ancestor(&state.store, agent_id, &target)
                }
            };
            if !allowed {
                return Err(ApiError::forbidden(
                    "memory write requires self, ancestor, or operator authority",
                ));
            }
            let _guard = state
                .store
                .lock_agent_lifecycle(&target.id)
                .map_err(ApiError::from_store)?;
            memory_backlog::mark(state.store.data_dir(), &target.id);
            let agent_root = state
                .store
                .data_dir()
                .join("agents")
                .join(target.id.as_str());
            let _ = crate::sandbox_exec::hydrate_agent_memory_from_store(&state.store, &target.id);
            let now_ms = anyagents_core::now_ms();
            let turn = if query.delete == Some(true) {
                let slug = anyagents_core::learned_memory::sanitize_topic_slug(topic)
                    .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                let (removed, index) = anyagents_core::learned_memory::delete_memory_topic(
                    &agent_root,
                    &target.name,
                    target.role.as_deref(),
                    topic,
                )
                .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                crate::sandbox_exec::delete_agent_memory_file_from_store(
                    &state.store,
                    &target.id,
                    &format!("{slug}.md"),
                );
                crate::sandbox_exec::persist_agent_memory_file_to_store(
                    &state.store,
                    &target.id,
                    "MEMORY.md",
                    index.as_bytes(),
                );
                let _ = state.store.sync_agent_memory_index(&target);
                ConversationTurnView {
                    seq: 0,
                    id: format!("memory:{}:deleted:{}", target.id, topic),
                    text: format!("deleted={removed}\n{index}"),
                    raw_bytes: None,
                    source: Some("memory".into()),
                    direction: "in".into(),
                    from: format!("agent:{}", target.name),
                    to: format!("agent:{}", target.name),
                    reply_to: None,
                    timestamp_ms: Some(now_ms),
                    raw: None,
                    window: None,
                    tool_name: None,
                    tool_names: Vec::new(),
                    role: Some("memory".into()),
                }
            } else {
                let body_text = query.content.as_deref().unwrap_or_default();
                let (path, entry, index) = anyagents_core::learned_memory::upsert_memory_topic(
                    &agent_root,
                    &target.name,
                    target.role.as_deref(),
                    topic,
                    query.description.as_deref(),
                    body_text,
                    now_ms,
                )
                .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
                let bytes = std::fs::read(&path).unwrap_or_default();
                crate::sandbox_exec::persist_agent_memory_file_to_store(
                    &state.store,
                    &target.id,
                    &entry.filename,
                    &bytes,
                );
                crate::sandbox_exec::persist_agent_memory_file_to_store(
                    &state.store,
                    &target.id,
                    "MEMORY.md",
                    index.as_bytes(),
                );
                let _ = state.store.sync_agent_memory_index(&target);
                let digest = anyagents_core::learned_memory::sha256(&bytes);
                let doc_id = format!(
                    "memory:{}:{}",
                    target.id,
                    anyagents_core::learned_memory::sha256(path.as_os_str().as_encoded_bytes())
                );
                let coord = anyagents_core::raw_segment::RawCoordinate {
                    segment: path,
                    offset: 0,
                    length: bytes.len() as u64,
                    sha256: digest,
                    seq: 1,
                };
                ConversationTurnView {
                    seq: 0,
                    id: doc_id,
                    text: String::from_utf8_lossy(&bytes).into_owned(),
                    raw_bytes: None,
                    source: Some("memory".into()),
                    direction: "in".into(),
                    from: format!("agent:{}", target.name),
                    to: format!("agent:{}", target.name),
                    reply_to: Some(entry.filename),
                    timestamp_ms: Some(now_ms),
                    raw: Some(coord.clone().into()),
                    window: Some(coord.into()),
                    tool_name: None,
                    tool_names: Vec::new(),
                    role: Some("memory".into()),
                }
            };
            return Ok(Json(ConversationResponse {
                retrieval: None,
                agent: target.name,
                agent_id: target.id.to_string(),
                turns: vec![turn],
            }));
        }
        let (name, visible_agent_ids) = conversation_visibility(&state.store, &principal, &name)?;
        if query.raw_id.is_none()
            && (query.raw_sha256.is_some()
                || query.raw_offset.is_some()
                || query.raw_length.is_some())
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "raw_sha256/raw_offset/raw_length require raw_id",
            ));
        }
        if let Some(id) = query.raw_id.as_deref() {
            let digest = query
                .raw_sha256
                .as_deref()
                .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
                .ok_or_else(|| {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "raw_sha256 must be the 64-character digest from the search result",
                    )
                })?;
            let length = query
                .raw_length
                .unwrap_or(anyagents_core::raw_segment::MAX_RAW_RANGE_BYTES);
            if id.trim().is_empty()
                || length == 0
                || length > anyagents_core::raw_segment::MAX_RAW_RANGE_BYTES
            {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "raw_id must be nonempty; raw_length must be 1..=65536",
                ));
            }
            let authorized = match visible_agent_ids.as_ref() {
                Some(ids) => ids.clone(),
                None => every_agent_id(&state.store)?,
            };
            if state.store.is_postgres()
                && query.scope != anyagents_core::host_index::MemoryScope::Transcripts
                && let Some(owner_str) = id.strip_prefix("memory:").and_then(|s| s.split(':').next())
                && authorized.iter().any(|a| a == owner_str)
                && let Ok(aid) = anyagents_core::AgentId::parse(owner_str)
            {
                let _ = crate::sandbox_exec::hydrate_agent_memory_from_store(&state.store, &aid);
            }
            let (hit, bytes) = state
                .store
                .read_host_file_range(
                    &authorized,
                    query.scope,
                    Some(&name),
                    anyagents_core::host_retrieval::HostRawRead {
                        id,
                        sha256: digest,
                        offset: query.raw_offset.unwrap_or(0),
                        length,
                    },
                )
                .map_err(|e| {
                    ApiError::new(
                        match &e {
                            anyagents_core::host_index::IndexError::Io(e)
                                if matches!(
                                    e.kind(),
                                    std::io::ErrorKind::WouldBlock
                                        | std::io::ErrorKind::InvalidData
                                ) =>
                            {
                                StatusCode::CONFLICT
                            }
                            _ => StatusCode::BAD_REQUEST,
                        },
                        e.to_string(),
                    )
                })?
                .ok_or_else(|| {
                    ApiError::new(
                        StatusCode::NOT_FOUND,
                        "raw document not found in authorized scope",
                    )
                })?;
            let agent = state
                .store
                .agent_by_name(&hit.metadata.agent_id)
                .map_err(ApiError::from_store)?;
            return Ok(Json(ConversationResponse {
                retrieval: None,
                agent: agent.name,
                agent_id: agent.id.to_string(),
                turns: vec![ConversationTurnView {
                    seq: hit.raw.seq as i64,
                    id: hit.id,
                    text: String::from_utf8_lossy(&bytes).into_owned(),
                    raw_bytes: Some(bytes),
                    source: Some(
                        if hit.metadata.source == anyagents_core::host_index::MemoryScope::Memory {
                            "memory"
                        } else {
                            "transcripts"
                        }
                        .into(),
                    ),
                    direction: hit.metadata.direction,
                    from: hit.metadata.sender,
                    to: hit.metadata.destination,
                    reply_to: None,
                    timestamp_ms: hit.metadata.timestamp_ms,
                    raw: Some(hit.raw.into()),
                    window: Some(hit.window.into()),
                    tool_name: hit.metadata.tool_name,
                    tool_names: hit.metadata.tool_names,
                    role: hit.metadata.role,
                }],
            }));
        }
        let clean_q = query
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let extra_queries: Vec<String> = query
            .queries
            .as_deref()
            .unwrap_or("")
            .split('|')
            .flat_map(|s| s.split(','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let exclude_terms: Vec<String> = query
            .exclude
            .as_deref()
            .unwrap_or("")
            .split('|')
            .flat_map(|s| s.split(','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let has_structured_search = clean_q.is_some()
            || !extra_queries.is_empty()
            || !exclude_terms.is_empty()
            || query.until_ms.is_some()
            || query.from_seq.is_some()
            || query.to_seq.is_some()
            || query.direction.is_some()
            || query.sender.is_some()
            || query.include_tools == Some(false)
            || query.only_tools == Some(true)
            || query.tool_name.is_some()
            || query.sort.is_some()
            || query.radius.is_some_and(|r| r > 0);
        let is_all = name.eq_ignore_ascii_case("all") || name == "*";
        let file_request = anyagents_core::HistorySearchQuery {
            query: clean_q.map(str::to_string),
            queries: extra_queries.clone(),
            exclude: exclude_terms.clone(),
            agent: Some(name.clone()),
            with_peer: query.with.clone(),
            direction: query.direction.clone(),
            sender: query.sender.clone(),
            include_tools: query.include_tools.unwrap_or(true),
            only_tools: query.only_tools.unwrap_or(false),
            tool_name: query.tool_name.clone(),
            since_ms: query.since_ms,
            until_ms: query.until_ms,
            from_seq: query.from_seq,
            to_seq: query.to_seq,
            sort: anyagents_core::HistorySort::parse(query.sort.as_deref()),
            radius: query.radius.unwrap_or(0),
            limit: query
                .limit
                .unwrap_or(if has_structured_search { 20 } else { 50 })
                .max(1),
            visible_agent_ids: visible_agent_ids.clone(),
        };
        let authorized = match visible_agent_ids.as_ref() {
            Some(ids) => ids.clone(),
            None => every_agent_id(&state.store)?,
        };
        let should_search_host_files = has_structured_search
            || query.scope != anyagents_core::host_index::MemoryScope::All
            || std::env::var_os("ANYAGENTS_TESTS").is_some();
        if should_search_host_files
            && state.store.is_postgres()
            && query.scope != anyagents_core::host_index::MemoryScope::Transcripts
        {
            if !is_all {
                if let Ok(target) = state.store.agent_by_name(&name)
                    && authorized.iter().any(|id| id == target.id.as_str())
                {
                    let _ = crate::sandbox_exec::hydrate_agent_memory_from_store(
                        &state.store,
                        &target.id,
                    );
                }
            } else if authorized.len() <= 64 {
                for aid_str in &authorized {
                    if let Ok(aid) = anyagents_core::AgentId::parse(aid_str) {
                        let _ = crate::sandbox_exec::hydrate_agent_memory_from_store(
                            &state.store,
                            &aid,
                        );
                    }
                }
            }
        }
        // A search indexes only a bounded amount of new history before it
        // answers; the background indexer takes the rest, and the response
        // says the history it searched was partial.
        let (file_hits, pending) = if should_search_host_files {
            state
                .store
                .search_host_files_reporting(&authorized, query.scope, &file_request)
                .map_err(|e| ApiError::from_store(Error::Message(e.to_string())))?
        } else {
            (Vec::new(), Default::default())
        };
        // A relevance search for text also ranks by meaning: dense candidates
        // from the memory service join the lexical ones, and the fused list
        // is reranked below. Reads in time order rank nothing.
        let text_query = clean_q
            .map(str::to_string)
            .or_else(|| (!extra_queries.is_empty()).then(|| extra_queries.join(" ")));
        let ranks_by_relevance = should_search_host_files
            && file_request.sort == anyagents_core::HistorySort::Relevance
            && file_request.radius == 0;
        let mut retrieval = None;
        let file_hits = match text_query.as_deref().filter(|_| ranks_by_relevance) {
            Some(text) => {
                let owners: Vec<String> = if is_all {
                    authorized.clone()
                } else {
                    let selected = state
                        .store
                        .agent_by_name(&name)
                        .map_err(ApiError::from_store)?
                        .id
                        .to_string();
                    authorized
                        .iter()
                        .filter(|id| **id == selected)
                        .cloned()
                        .collect()
                };
                let hybrid = hybrid::search(
                    &state.store,
                    text,
                    &owners,
                    query.scope,
                    &file_request,
                    file_hits,
                );
                retrieval = Some(hybrid.retrieval);
                hybrid.hits
            }
            None => file_hits,
        };
        if pending.sources > 0 {
            let view = retrieval.get_or_insert_with(|| crate::api::RetrievalView {
                stages: vec!["lexical".into()],
                ..crate::api::RetrievalView::default()
            });
            view.degraded.push(format!(
                "partial: {} history source(s), about {} KiB, are still being indexed in the background; hits from them may be missing",
                pending.sources,
                pending.bytes.div_ceil(1024)
            ));
            view.pending = Some(crate::api::PendingHistoryView {
                sources: pending.sources,
                bytes: pending.bytes,
            });
        }

        let (agent_name, agent_id_str, raw_turns) =
            if query.scope == anyagents_core::host_index::MemoryScope::Memory {
                if is_all {
                    ("all".into(), "all".into(), Vec::new())
                } else {
                    let a = state
                        .store
                        .agent_by_name(&name)
                        .map_err(ApiError::from_store)?;
                    (a.name, a.id.to_string(), Vec::new())
                }
            } else if has_structured_search {
                let search_req = anyagents_core::HistorySearchQuery {
                    query: clean_q.map(str::to_string),
                    queries: extra_queries,
                    exclude: exclude_terms,
                    agent: Some(name.clone()),
                    with_peer: query.with.clone(),
                    direction: query.direction.clone(),
                    sender: query.sender.clone(),
                    include_tools: query.include_tools.unwrap_or(true),
                    only_tools: query.only_tools.unwrap_or(false),
                    tool_name: query.tool_name.clone(),
                    since_ms: query.since_ms,
                    until_ms: query.until_ms,
                    from_seq: query.from_seq,
                    to_seq: query.to_seq,
                    sort: anyagents_core::HistorySort::parse(query.sort.as_deref()),
                    radius: query.radius.unwrap_or(0),
                    limit: if query.all == Some(true) {
                        0
                    } else {
                        query.limit.unwrap_or(20)
                    },
                    visible_agent_ids: visible_agent_ids.clone(),
                };
                let turns = state
                    .store
                    .search_history(&search_req)
                    .map_err(ApiError::from_store)?;
                let (aname, aid) = if is_all {
                    ("all".to_string(), "all".to_string())
                } else if let Ok(a) = state.store.agent_by_name(&name) {
                    (a.name, a.id.to_string())
                } else {
                    (name.clone(), name.clone())
                };
                (aname, aid, turns)
            } else if is_all {
                let turns = state
                    .store
                    .conversation_all_scoped(
                        query.with.as_deref(),
                        query.since_ms,
                        query.limit,
                        visible_agent_ids.as_deref(),
                    )
                    .map_err(ApiError::from_store)?;
                ("all".to_string(), "all".to_string(), turns)
            } else {
                let agent = state
                    .store
                    .agent_by_name(&name)
                    .map_err(ApiError::from_store)?;
                let turns = state
                    .store
                    .conversation_scoped(
                        &agent.id,
                        query.with.as_deref(),
                        query.since_ms,
                        query.limit,
                        visible_agent_ids.as_deref(),
                    )
                    .map_err(ApiError::from_store)?;
                (agent.name, agent.id.to_string(), turns)
            };
        let turns: Vec<_> = raw_turns
            .into_iter()
            .map(|t| ConversationTurnView {
                seq: t.seq,
                raw_bytes: None,
                source: Some("transcripts".into()),
                raw: None,
                window: None,
                tool_name: None,
                tool_names: Vec::new(),
                role: None,
                id: t.id,
                direction: t.direction,
                from: t.from,
                to: t.to,
                reply_to: t.reply_to,
                text: t.text,
                timestamp_ms: Some(t.timestamp_ms),
            })
            .collect();
        let mut ranked: Vec<_> = turns
            .into_iter()
            .enumerate()
            .map(|(rank, t)| (1.0 / (30.0 + rank as f64), t))
            .collect();
        for (rank, (hit, bytes)) in file_hits.into_iter().enumerate() {
            let reply_to = (hit.metadata.source == anyagents_core::host_index::MemoryScope::Memory)
                .then(|| {
                    hit.raw
                        .segment
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_string)
                })
                .flatten();
            ranked.push((
                1.0 / (30.0 + rank as f64),
                ConversationTurnView {
                    seq: 0,
                    raw_bytes: None,
                    id: hit.id,
                    direction: hit.metadata.direction,
                    from: hit.metadata.sender,
                    to: hit.metadata.destination,
                    reply_to,
                    text: String::from_utf8_lossy(&bytes).into_owned(),
                    timestamp_ms: hit.metadata.timestamp_ms,
                    source: Some(
                        if hit.metadata.source == anyagents_core::host_index::MemoryScope::Memory {
                            "memory"
                        } else {
                            "transcripts"
                        }
                        .into(),
                    ),
                    raw: Some(hit.raw.into()),
                    window: Some(hit.window.into()),
                    tool_name: hit.metadata.tool_name,
                    tool_names: hit.metadata.tool_names,
                    role: hit.metadata.role,
                },
            ));
        }
        let lexical = clean_q.is_some() || !file_request.queries.is_empty();
        if lexical
            && file_request.sort == anyagents_core::HistorySort::Relevance
            && file_request.radius == 0
        {
            ranked.sort_by(|a, b| {
                b.0.total_cmp(&a.0)
                    .then_with(|| b.1.timestamp_ms.cmp(&a.1.timestamp_ms))
            });
            if let (Some(retrieval), Some(text)) = (retrieval.as_mut(), text_query.as_deref()) {
                hybrid::rerank(state.store.data_dir(), text, &mut ranked, retrieval);
            }
        } else {
            ranked.sort_by_key(|(_, t)| t.timestamp_ms);
            if file_request.sort == anyagents_core::HistorySort::Newest {
                ranked.reverse();
            }
        }
        if query.all != Some(true) && file_request.radius == 0 && ranked.len() > file_request.limit
        {
            if !lexical && file_request.sort == anyagents_core::HistorySort::Relevance {
                ranked.drain(..ranked.len() - file_request.limit);
            } else {
                ranked.truncate(file_request.limit);
            }
        }
        let turns = ranked.into_iter().map(|(_, t)| t).collect();
        Ok(Json(ConversationResponse {
            agent: agent_name,
            agent_id: agent_id_str,
            turns,
            retrieval,
        }))
    })
    .await
}

/// One agent's messages past `since`, in sequence order.
fn inbox_of(
    store: &Store,
    agent_id: &anyagents_core::AgentId,
    since: Option<i64>,
    limit: Option<usize>,
    unread: bool,
    sender: Option<&str>,
) -> Result<Json<InboxResponse>, ApiError> {
    let rows = store
        .inbox_query_filtered(agent_id, since, limit, unread, sender)
        .map_err(ApiError::from_store)?;
    let messages = rows
        .into_iter()
        .map(|message| MessageView {
            id: message.id.to_string(),
            seq: message.seq,
            text: message.text,
            sender: message.sender,
            reply_to: message.reply_to,
            source_time_ms: message.source_time_ms,
            received_at_ms: message.received_at_ms,
            delivered_at_ms: message.delivered_at_ms,
            ack: message.ack.to_string(),
        })
        .collect();
    Ok(Json(InboxResponse { messages }))
}
