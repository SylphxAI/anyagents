//! Learned-memory MCP arguments and operation dispatch.

use std::path::Path;

use rmcp::handler::server::wrapper::Parameters;
use schemars::JsonSchema;
use serde::Deserialize;

use super::{AgentMcp, ConversationArgs, sha256_hex, urlencoding_simple};

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct MemoryArgs {
    /// Learned-memory operation to perform:
    /// - `"list"` (default when no `topic`, `query`, `content`, or `delete` is passed): List a compact page of topic summaries.
    /// - `"read"` (inferred when `topic` is provided without `content` or `delete`): Read `<agent_root>/memory/<topic>.md` in O(1).
    /// - `"write"` / `"upsert"` (inferred when `content` is provided): Create or update `<agent_root>/memory/<topic>.md` and rebuild `MEMORY.md`.
    /// - `"delete"` (inferred when `delete: true` is provided): Delete `<agent_root>/memory/<topic>.md` and rebuild `MEMORY.md`.
    /// - `"search"` (inferred when `query` is provided): Search the FTS5 learned-memory index.
    #[serde(default)]
    pub(super) action: Option<String>,
    /// Learned-memory topic slug (`<agent_root>/memory/<topic>.md`, e.g. `"deployment-runbook"`).
    #[serde(default, alias = "slug", alias = "name")]
    pub(super) topic: Option<String>,
    /// 1-line trigger description indexed in `MEMORY.md` when `action = "write"` / `"upsert"`.
    #[serde(default, alias = "summary", alias = "trigger")]
    pub(super) description: Option<String>,
    /// Markdown body to write into `<agent_root>/memory/<topic>.md` when `action = "write"` / `"upsert"`.
    #[serde(default, alias = "body", alias = "text", alias = "markdown")]
    pub(super) content: Option<String>,
    /// Full-text search query, or a case-insensitive topic/description filter for explicit list.
    #[serde(default, alias = "q", alias = "search")]
    pub(super) query: Option<String>,
    /// When `true`, deletes `<agent_root>/memory/<topic>.md` and rebuilds `MEMORY.md`.
    #[serde(default)]
    pub(super) delete: Option<bool>,
    /// Target agent name or `agt_...` ID (defaults to this agent itself).
    #[serde(default, alias = "target")]
    pub(super) agent: Option<String>,
    /// Requested page size for list (default 25, maximum 50; the byte cap may return fewer).
    #[serde(default)]
    pub(super) limit: Option<usize>,
    /// List offset; use the returned next_offset to fetch the next page.
    pub(super) offset: Option<usize>,
    /// Case-insensitive topic slug prefix for list.
    pub(super) prefix: Option<String>,
    /// Use "shared" to list/read public peers only; omit agent when using this scope.
    pub(super) scope: Option<String>,
}

impl AgentMcp {
    pub(super) async fn execute_memory_tool(
        &self,
        args: MemoryArgs,
    ) -> Result<serde_json::Value, String> {
        let raw_topic = args
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let raw_query = args
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let is_index_topic = raw_topic.as_deref().is_some_and(|t| {
            t.eq_ignore_ascii_case("memory")
                || t.eq_ignore_ascii_case("memory.md")
                || t.eq_ignore_ascii_case("index")
        });

        let mut action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| {
                if args.delete == Some(true) {
                    "delete".to_string()
                } else if args.content.is_some() {
                    "write".to_string()
                } else if raw_query.is_some() {
                    "search".to_string()
                } else if raw_topic.is_some() && !is_index_topic {
                    "read".to_string()
                } else {
                    "list".to_string()
                }
            });
        if (action == "read" || action == "get") && (raw_topic.is_none() || is_index_topic) {
            action = "list".to_string();
        }

        let target_arg = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).trim())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let is_self_target = target_arg.as_deref().is_none_or(|target| {
            target.eq_ignore_ascii_case("self")
                || target.eq_ignore_ascii_case("me")
                || target == self.agent.name
                || target == self.agent.id.as_str()
        });
        let shared = match args.scope.as_deref().map(str::trim) {
            None | Some("self") => false,
            Some("shared") => true,
            _ => return Err("memory scope must be self or shared".into()),
        };
        let can_share = matches!(action.as_str(), "list" | "ls" | "index" | "read" | "get");
        let self_only = matches!(
            action.as_str(),
            "write"
                | "upsert"
                | "set"
                | "delete"
                | "remove"
                | "rm"
                | "status"
                | "consolidate"
                | "sleep"
        );
        if self_only && (shared || !is_self_target) {
            return Err("403 Forbidden: only list/read support public peer memory; other memory actions require self".into());
        }
        if shared && (!can_share || target_arg.is_some()) {
            return Err(
                "shared memory requires list/read without a separate agent selector".into(),
            );
        }
        let route_target = target_arg.as_deref().unwrap_or("self");
        let memory_path = format!("/v1/agents/{}/memory", urlencoding_simple(route_target));
        // Over HTTP this tool server runs as the server's own user, and the
        // agent's memory directory is writable by its seat: a planted link
        // would make an in-process read or write reach a server file. There,
        // every memory operation goes through the server's routes, which open
        // files without following links.
        let local_agent_root = if self.client.via_http {
            None
        } else {
            self.client.config.parent().map(Path::to_path_buf)
        };

        match action.as_str() {
            "list" | "ls" | "index" => {
                let mut params = Vec::new();
                if shared {
                    params.push("scope=shared".to_owned());
                }
                if let Some(query) = raw_query {
                    params.push(format!("query={}", urlencoding_simple(&query)));
                }
                if let Some(prefix) = args.prefix {
                    params.push(format!("prefix={}", urlencoding_simple(&prefix)));
                }
                if let Some(limit) = args.limit {
                    params.push(format!("limit={limit}"));
                }
                if let Some(offset) = args.offset {
                    params.push(format!("offset={offset}"));
                }
                self.client
                    .get(&format!("{memory_path}?{}", params.join("&")), None)
                    .await
            }
            "read" | "get" => {
                let topic = raw_topic.ok_or_else(|| "memory read requires topic".to_string())?;
                let scope = if shared { "&scope=shared" } else { "" };
                let path = format!("{memory_path}?topic={}{scope}", urlencoding_simple(&topic));
                self.client.get(&path, None).await
            }
            "write" | "upsert" | "set" => {
                let topic =
                    raw_topic.ok_or_else(|| "`memory` write requires `topic`".to_string())?;
                let slug = anyagents_core::learned_memory::sanitize_topic_slug(&topic)
                    .map_err(|e| e.to_string())?;
                let content = args.content.unwrap_or_default();

                match self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("memory".into()),
                        topic: Some(slug.clone()),
                        description: args.description.clone(),
                        content: Some(content.clone()),
                        agent: Some("self".into()),
                        ..ConversationArgs::default()
                    }))
                    .await
                {
                    Ok(resp) => {
                        let written_text = resp
                            .0
                            .turns
                            .first()
                            .map(|t| t.text.clone())
                            .unwrap_or_default();
                        let (_, description) =
                            anyagents_core::learned_memory::parse_topic_frontmatter(
                                &slug,
                                &written_text,
                            );
                        let sha256 = sha256_hex(written_text.as_bytes());
                        Ok(serde_json::json!({
                            "action": "write",
                            "agent": resp.0.agent,
                            "topic": slug,
                            "filename": format!("{slug}.md"),
                            "description": description,
                            "content": written_text,
                            "sha256": sha256,
                            "turns": resp.0.turns,
                        }))
                    }
                    Err(err) => {
                        if is_self_target && let Some(ref root) = local_agent_root {
                            let (path, entry, index_md) =
                                anyagents_core::learned_memory::upsert_memory_topic(
                                    root,
                                    &self.agent.name,
                                    None,
                                    &slug,
                                    args.description.as_deref(),
                                    &content,
                                    anyagents_core::now_ms(),
                                )
                                .map_err(|e| e.to_string())?;
                            let written_text = std::fs::read_to_string(&path).unwrap_or_default();
                            let sha256 = sha256_hex(written_text.as_bytes());
                            return Ok(serde_json::json!({
                                "action": "write",
                                "agent": self.agent.name,
                                "topic": entry.slug,
                                "filename": entry.filename,
                                "description": entry.description,
                                "content": written_text,
                                "index": index_md,
                                "sha256": sha256,
                                "turns": [{
                                    "id": format!("memory:{}:{}", self.agent.id, slug),
                                    "text": written_text,
                                    "source": "memory",
                                    "direction": "in",
                                    "from": format!("agent:{}", self.agent.name),
                                    "to": format!("agent:{}", self.agent.name),
                                    "reply_to": format!("{slug}.md"),
                                }],
                            }));
                        }
                        Err(err)
                    }
                }
            }
            "delete" | "remove" | "rm" => {
                let topic =
                    raw_topic.ok_or_else(|| "`memory` delete requires `topic`".to_string())?;
                let slug = anyagents_core::learned_memory::sanitize_topic_slug(&topic)
                    .map_err(|e| e.to_string())?;

                match self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("memory".into()),
                        topic: Some(slug.clone()),
                        delete: Some(true),
                        agent: Some("self".into()),
                        ..ConversationArgs::default()
                    }))
                    .await
                {
                    Ok(resp) => {
                        let first_text = resp
                            .0
                            .turns
                            .first()
                            .map(|t| t.text.clone())
                            .unwrap_or_default();
                        let deleted = first_text.starts_with("deleted=true");
                        let index = first_text
                            .split_once('\n')
                            .map(|(_, rest)| rest.to_string())
                            .unwrap_or_default();
                        Ok(serde_json::json!({
                            "action": "delete",
                            "agent": resp.0.agent,
                            "topic": slug,
                            "deleted": deleted,
                            "index": index,
                            "turns": resp.0.turns,
                        }))
                    }
                    Err(err) => {
                        if is_self_target && let Some(ref root) = local_agent_root {
                            let (removed, index) =
                                anyagents_core::learned_memory::delete_memory_topic(
                                    root,
                                    &self.agent.name,
                                    None,
                                    &slug,
                                )
                                .map_err(|e| e.to_string())?;
                            return Ok(serde_json::json!({
                                "action": "delete",
                                "agent": self.agent.name,
                                "topic": slug,
                                "deleted": removed,
                                "index": index,
                                "turns": [{
                                    "id": format!("memory:{}:deleted:{}", self.agent.id, slug),
                                    "text": format!("deleted={removed}\n{index}"),
                                    "source": "memory",
                                    "direction": "in",
                                    "from": format!("agent:{}", self.agent.name),
                                    "to": format!("agent:{}", self.agent.name),
                                }],
                            }));
                        }
                        Err(err)
                    }
                }
            }
            "search" | "query" | "find" => {
                let q = raw_query
                    .or(raw_topic)
                    .ok_or_else(|| "`memory` search requires `query`".to_string())?;
                match self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("memory".into()),
                        query: Some(q.clone()),
                        // Conversation search defaults to all; learned memory defaults to self.
                        agent: Some(
                            target_arg
                                .filter(|target| !target.trim().is_empty())
                                .unwrap_or_else(|| "self".into()),
                        ),
                        limit: args.limit,
                        ..ConversationArgs::default()
                    }))
                    .await
                {
                    Ok(resp) => {
                        let count = resp.0.turns.len();
                        Ok(serde_json::json!({
                            "action": "search",
                            "agent": resp.0.agent,
                            "query": q,
                            "matches": resp.0.turns,
                            "count": count,
                        }))
                    }
                    Err(err) => {
                        if is_self_target
                            && let Some(ref root) = local_agent_root
                            && let Ok(topics) =
                                anyagents_core::learned_memory::list_memory_topics(root)
                        {
                            let tokens: Vec<String> = q
                                .to_ascii_lowercase()
                                .split_whitespace()
                                .filter(|t| !t.is_empty())
                                .map(str::to_string)
                                .collect();
                            let limit = args.limit.unwrap_or(20).max(1);
                            let mut hits = Vec::new();
                            for entry in topics {
                                let path = root.join("memory").join(&entry.filename);
                                let text = std::fs::read_to_string(&path).unwrap_or_default();
                                let hay = format!("{} {} {}", entry.slug, entry.description, text)
                                    .to_ascii_lowercase();
                                if tokens.iter().all(|tok| hay.contains(tok)) {
                                    hits.push(serde_json::json!({
                                        "id": format!("memory:{}:{}", self.agent.id, entry.slug),
                                        "topic": entry.slug,
                                        "filename": entry.filename,
                                        "description": entry.description,
                                        "text": text,
                                        "source": "memory",
                                        "direction": "in",
                                        "from": format!("agent:{}", self.agent.name),
                                        "to": format!("agent:{}", self.agent.name),
                                        "reply_to": entry.filename,
                                        "timestamp_ms": entry.updated_at_ms,
                                    }));
                                    if hits.len() >= limit {
                                        break;
                                    }
                                }
                            }
                            let count = hits.len();
                            return Ok(serde_json::json!({
                                "action": "search",
                                "agent": self.agent.name,
                                "query": q,
                                "matches": hits,
                                "count": count,
                            }));
                        }
                        Err(err)
                    }
                }
            }
            "status" => {
                if let Some(ref root) = local_agent_root {
                    let topics = anyagents_core::learned_memory::list_memory_topics(root)
                        .unwrap_or_default();
                    let sleep_state = anyagents_core::learned_memory::read_sleep_state(root);
                    let index_text =
                        std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap_or_default();
                    return Ok(serde_json::json!({
                        "action": "status",
                        "agent": self.agent.name,
                        "topic_count": topics.len(),
                        "index_bytes": index_text.len(),
                        "index_lines": index_text.lines().count(),
                        "last_consolidated_ms": sleep_state.last_consolidated_ms,
                        "consolidated_topics": sleep_state.consolidated_topics,
                    }));
                }
                self.client
                    .get(&format!("{memory_path}?status=true"), None)
                    .await
            }
            "recall" | "history" => {
                let q = raw_query
                    .or(raw_topic)
                    .ok_or_else(|| "`memory` recall requires `query`".to_string())?;
                let resp = self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("all".into()),
                        query: Some(q.clone()),
                        agent: target_arg.clone(),
                        limit: args.limit,
                        ..ConversationArgs::default()
                    }))
                    .await?;
                let count = resp.0.turns.len();
                Ok(serde_json::json!({
                    "action": "recall",
                    "agent": resp.0.agent,
                    "query": q,
                    "matches": resp.0.turns,
                    "count": count,
                }))
            }
            "consolidate" | "sleep" => {
                if let Some(ref root) = local_agent_root {
                    let index_md = anyagents_core::learned_memory::rebuild_memory_index(
                        root,
                        &self.agent.name,
                        None,
                    )
                    .map_err(|e| e.to_string())?;
                    let topics = anyagents_core::learned_memory::list_memory_topics(root)
                        .unwrap_or_default();
                    return Ok(serde_json::json!({
                        "action": "consolidate",
                        "agent": self.agent.name,
                        "topic_count": topics.len(),
                        "index": index_md,
                    }));
                }
                Err("local memory root is not available for `memory.consolidate`".to_string())
            }
            other => Err(format!(
                "unknown `memory` action `{other}`; expected `list`, `read`, `write`, `delete`, `search`, `recall`, `status`, or `consolidate`"
            )),
        }
    }
}
