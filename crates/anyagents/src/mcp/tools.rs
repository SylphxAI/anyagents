//! MCP tool router (`#[tool_router]`), JSON-RPC dispatch, and tool handlers.

use super::*;

#[path = "knowledge_tool.rs"]
mod knowledge_tool;
#[path = "memory_tool.rs"]
mod memory_tool;
use memory_tool::MemoryArgs;

#[path = "ingress_tool.rs"]
mod ingress_tool;

#[path = "send_tool.rs"]
mod send_tool;

#[tool_router(vis = "pub(super)")]
impl AgentMcp {
    #[tool(
        description = "Manage this agent's external ingress rules. Use the channel and numeric IDs from sender_awareness. Block a sender or known group, remove an exact block, or list rules. Protected senders bypass. A permanent group block may leave the chat; unblocking does not rejoin it."
    )]
    pub(super) async fn ingress_guard(
        &self,
        Parameters(args): Parameters<ingress_tool::Args>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        ingress_tool::call(self, args)
            .await
            .map(rmcp::handler::server::wrapper::Json)
    }

    #[tool(
        description = "Unified outbound communication and message interaction tool: send a message, media file (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`), GPS `location`/venue, interactive `poll`, `checklist`, or Instant View `article` to one or multiple agents (`planner`, `agt_...`, `parent`, `children`), internal groups (`group:<name>`, `group:all`, `all`), or external channel targets (`telegram:chat:<id>[:<topic>]`, `telegram:user:<id>`); OR perform a message interaction (`action`: `\"react\"` | `\"vote\"` | `\"check\"` | `\"uncheck\"` | `\"edit\"` | `\"delete\"` | `\"pin\"` | `\"unpin\"` | `\"stop_poll\"`) on a target message (`target` or `reply_to`: `anyagents:reply:msg_...` or `telegram:chat:<chat_id>:<message_id>`). For Telegram chat administration, use `set_chat_title`, `set_chat_description`, or `set_chat_photo` with one `telegram:chat:<chat_id>` in `target` or `to`. The selected `bot` must have admin rights to change chat info. Supply `text` for title or description or `media: {kind: \"photo\", source: ...}` (local JPEG/PNG, direct public HTTP(S) image URL, or private `media://` reference) for photo. For a sandbox-local file, POST binary or multipart `file` to `$ANYAGENTS_API_URL/v1/media` with the remote MCP token as Bearer auth (read `$ANYAGENTS_MCP_TOKEN_FILE` if set), then use the returned `source` in `media.source`. Never pass a sandbox path to HTTP MCP."
    )]
    pub(super) async fn send(
        &self,
        Parameters(args): Parameters<SendArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        send_tool::send(self, args).await
    }

    #[tool(
        description = "ONE-WAY fire-and-forget telemetry for genuine AnyAgents, sandbox, or harness runtime bugs ONLY. NEVER call for task blockers, missing kubeconfig/secrets/permissions, CI/test failures, third-party API errors, or requests for an operator to run commands; send those to your parent or ops peer via send. Include the exact error with secrets removed. Returns an issue_id only; no human replies to unblock your task."
    )]
    pub(super) async fn report_bug(
        &self,
        Parameters(args): Parameters<crate::server::runtime_issues::ReportBug>,
    ) -> Result<
        rmcp::handler::server::wrapper::Json<crate::server::runtime_issues::ReportReceipt>,
        String,
    > {
        self.client
            .get(
                "/v1/runtime-issues",
                Some(serde_json::to_vec(&args).map_err(|err| err.to_string())?),
            )
            .await
            .map(rmcp::handler::server::wrapper::Json)
    }

    #[tool(
        description = "Unified two-way (`in` + `out`) conversation reader, host memory search (`scope`: all|memory|transcripts; default all), and zero-friction learned-memory topic reader/writer (`topic`, `description`, `content`, `delete`). Exact bounded raw readback uses `raw_id` and `raw_sha256` from a result plus optional `raw_offset`/`raw_length` (bytes, max65536); it returns exact `raw_bytes`, full raw coordinates and the requested window. Paths are never accepted. Native historical events without source time return timestamp_ms:null (CLI: unknown); missing time is never inferred from file mtime, and timestamp bounds exclude unknown times. Lexical + Structured History Search (`query`, `queries`, `exclude`, `include_tools`, `only_tools`, `tool_name`, `direction`, `sender`, `since_ms`, `until_ms`, `from_seq`, `to_seq`, `range`, `sort`, `radius`, `limit`). To read a learned-memory topic directly in O(1), pass `topic: \"<slug>\"` (without `content` or `delete`). To write or update a durable learned-memory topic without host file paths, pass `topic: \"<slug>\"`, `description: \"<1-line trigger>\"`, and `content: \"<markdown>\"` (or `delete: true` to remove a stale topic); AnyAgents writes `<agent_root>/memory/<topic>.md`, syncs the host index immediately, and rebuilds `MEMORY.md` automatically."
    )]
    pub(super) async fn conversation(
        &self,
        Parameters(args): Parameters<ConversationArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<ConversationResponse>, String> {
        let clean_q = args
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let (range_from, range_to) = parse_sequence_range(args.range.as_deref())?;
        let from_seq = args.from_seq.or(range_from);
        let to_seq = args.to_seq.or(range_to);
        let has_topic_mutation = args
            .topic
            .as_deref()
            .map(str::trim)
            .is_some_and(|s| !s.is_empty());
        let is_search_mode = !has_topic_mutation
            && (args.raw_id.is_some()
                || clean_q.is_some()
                || !args.queries.is_empty()
                || !args.exclude.is_empty()
                || args.only_tools == Some(true)
                || args.tool_name.is_some()
                || args.turn_id.is_some()
                || args.role.is_some());
        let default_agent = if is_search_mode {
            "all".to_string()
        } else {
            match crate::client::agent_identity() {
                Some(id) => id.to_string(),
                None => self.agent.id.to_string(),
            }
        };
        let target = match args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(raw) => {
                let resolved = self.resolve_agent_word(raw).await?;
                resolved
                    .strip_prefix("agent:")
                    .unwrap_or(&resolved)
                    .to_string()
            }
            None => default_agent,
        };
        let mut query = Vec::new();
        if let Some(id) = args.raw_id {
            query.push(format!("raw_id={}", urlencoding_simple(&id)));
        }
        if let Some(digest) = args.raw_sha256 {
            query.push(format!("raw_sha256={}", urlencoding_simple(&digest)));
        }
        if let Some(offset) = args.raw_offset {
            query.push(format!("raw_offset={offset}"));
        }
        if let Some(length) = args.raw_length {
            query.push(format!("raw_length={length}"));
        }
        if let Some(scope) = args.scope {
            if !["all", "memory", "transcripts"].contains(&scope.as_str()) {
                return Err("scope must be all, memory or transcripts".into());
            }
            query.push(format!("scope={scope}"));
        } else if has_topic_mutation {
            query.push("scope=memory".to_string());
        }
        if let Some(topic) = args
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("topic={}", urlencoding_simple(topic)));
        }
        if let Some(desc) = args
            .description
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("description={}", urlencoding_simple(desc)));
        }
        if let Some(content) = args.content.as_deref() {
            query.push(format!("content={}", urlencoding_simple(content)));
        }
        if let Some(del) = args.delete {
            query.push(format!("delete={del}"));
        }
        if let Some(q) = clean_q {
            query.push(format!("query={}", urlencoding_simple(q)));
        }
        if !args.queries.is_empty() {
            query.push(format!(
                "queries={}",
                urlencoding_simple(&args.queries.join("|"))
            ));
        }
        if !args.exclude.is_empty() {
            query.push(format!(
                "exclude={}",
                urlencoding_simple(&args.exclude.join("|"))
            ));
        }
        if let Some(with_peer) = args
            .with
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let with_peer = self.resolve_agent_word(with_peer).await?;
            query.push(format!("with={}", urlencoding_simple(&with_peer)));
        }
        if let Some(dir) = args
            .direction
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("direction={}", urlencoding_simple(dir)));
        }
        if let Some(sender) = args
            .sender
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("sender={}", urlencoding_simple(sender)));
        }
        if let Some(inc) = args.include_tools {
            query.push(format!("include_tools={inc}"));
        }
        if let Some(only) = args.only_tools {
            query.push(format!("only_tools={only}"));
        }
        if let Some(tname) = args
            .tool_name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("tool_name={}", urlencoding_simple(tname)));
        }
        if let Some(tid) = args
            .turn_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("turn_id={}", urlencoding_simple(tid)));
        }
        if let Some(role) = args
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("role={}", urlencoding_simple(role)));
        }
        if let Some(since_ms) = args.since_ms {
            query.push(format!("since_ms={since_ms}"));
        }
        if let Some(until_ms) = args.until_ms {
            query.push(format!("until_ms={until_ms}"));
        }
        if let Some(fs) = from_seq {
            query.push(format!("from_seq={fs}"));
        }
        if let Some(ts) = to_seq {
            query.push(format!("to_seq={ts}"));
        }
        if let Some(sort) = args
            .sort
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("sort={}", urlencoding_simple(sort)));
        }
        if let Some(rad) = args.radius {
            query.push(format!("radius={rad}"));
        }
        let limit = args.limit.unwrap_or(if is_search_mode { 20 } else { 50 });
        query.push(format!("limit={limit}"));
        let path = format!("/v1/agents/{target}/conversation?{}", query.join("&"));
        let response = self.client.get(&path, None).await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Zero-Embedding Lossless Lexical + Structured History Search across authorized conversations and tool history (`query`, `queries`, `exclude`, `agent`, `with`, `direction`, `sender`, `include_tools`, `only_tools`, `tool_name`, `since_ms`, `until_ms`, `from_seq`, `to_seq`, `range`, `sort`, `radius`, `limit`). Combines SQLite FTS5 BM25 (ASCII identifiers + CJK bigrams), exact phrase boost, and contiguous episode context expansion."
    )]
    pub(super) async fn search(
        &self,
        Parameters(args): Parameters<ConversationArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<ConversationResponse>, String> {
        self.conversation(Parameters(args)).await
    }

    #[tool(
        description = "Unified agent & cross-harness sub-agent manager (`action`: `list` | `inspect` | `spawn` | `create` | `update` | `secrets` | `describe` | `verdict` | `redact` | `steer` | `wake` | `pause` | `archive` | `delete` | `rename` | `role` | `harness` | `models` | `usage`, supporting multi-level sub-agent trees). Use `list` (default, with optional `parent`: `\"self\"` | `\"subagents\"` | `\"root\"` and `state`: `\"running\"` | `\"working\"` | `\"paused\"` | `\"archived\"` | `\"all\"`) for a compact roster with state counts and idle task-volume warnings (advisory only); set detail=full for all visible fields, and follow next_cursor for more rows. Use `inspect` (`name`: `\"<agent-or-id>\"`, `limit`: `10`) to view live status, schedules, and recent turns; use `spawn` (`role` defaulting to `\"general\"`, `name`, `repo`: `\"<org>/<repo>\"` | `\"inherit\"`, `branch`, `base`, `workdir`: path | `\"worktree\"` | `\"private\"` | `\"inherit\"`, `description`, `prompt`, `harness`, `model`, `profile`, `secrets`) to launch a sub-agent with an isolated managed Git repository; use `update` or `secrets` (`name`, `secrets`: `[...]`) to grant/revoke held deployment secrets on descendants (`child ⊆ parent`); use `describe` (`name`: `\"self\"` | `\"all\"` | `\"<agent>\"`, `description`: `\"...\"`) to read role descriptions or update a target's `description`/`instructions`/`role` (updating `description`, `instructions`, `role`, or `rename` requires the target's Parent/Ancestor agent or an Admin); use `verdict` (`name`, `verdict`: `accepted` | `rework` | `rejected` | `abandoned`, `reason`) to judge a descendant's work; use `steer`, `wake`, `pause`, `archive` (auto-reaps clean/pushed managed checkouts while preserving dirty ones), or `delete` to control lifecycle; use `redact` (`name`, `message_id` or `seq`, optional `span`: {`unit`: `bytes` | `chars`, `start`, `end`}, `reason`) for audited erasure with permission `conversation.redact` (ancestors/admin only, quiescent target); or use `usage`, `create`, `rename`, `role`, `harness`, `models`."
    )]
    pub(super) async fn agents(
        &self,
        Parameters(mut args): Parameters<AgentsArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let action = agent_secrets::resolve_agents_action(&args);
        // `name` names the agent acted on, except where it names a new one.
        if !matches!(action.as_str(), "list" | "spawn" | "create")
            && let Some(raw) = args.name.as_deref()
        {
            args.name = Some(self.resolve_agent_word(raw).await?);
        }
        match action.as_str() {
            "verdict" => {
                let response = agent_secrets::record_agent_verdict(
                    &self.client,
                    args.name.as_deref(),
                    args.verdict.as_deref(),
                    args.reason.as_deref(),
                )
                .await?;
                Ok(rmcp::handler::server::wrapper::Json(response))
            }
            "usage" => {
                let mut qs = String::new();
                let mut push = |k: &str, v: String| {
                    if !qs.is_empty() {
                        qs.push('&');
                    }
                    qs.push_str(k);
                    qs.push('=');
                    qs.push_str(&urlencoding_simple(&v));
                };
                if let Some(t) = args.name.as_deref().filter(|t| !t.trim().is_empty()) {
                    push("agent", t.to_string());
                }
                if let Some(v) = args.include_descendants {
                    push("include_descendants", v.to_string());
                }
                if let Some(v) = args.since_ms {
                    push("since_ms", v.to_string());
                }
                if let Some(v) = args.until_ms {
                    push("until_ms", v.to_string());
                }
                if let Some(v) = args.days {
                    push("days", v.to_string());
                }
                if let Some(v) = args.group_by.as_deref().filter(|v| !v.trim().is_empty()) {
                    push("group_by", v.to_string());
                }
                let path = if qs.is_empty() {
                    "/v1/usage".to_string()
                } else {
                    format!("/v1/usage?{qs}")
                };
                let response = self.client.get(&path, None).await?;
                Ok(rmcp::handler::server::wrapper::Json(response))
            }
            "redact" => {
                let target = args
                    .name
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| "redact requires a target agent".to_string())?;
                let request = anyagents_core::redaction::Request {
                    message_id: args.message_id,
                    seq: args.seq,
                    span: args
                        .span
                        .map(TryInto::try_into)
                        .transpose()
                        .map_err(|e: Error| e.to_string())?,
                    reason: args.reason.ok_or_else(|| {
                        "redact requires a reason (without sensitive text)".to_string()
                    })?,
                };
                let path = format!(
                    "/v1/agents/{}/conversation/redact",
                    urlencoding_simple(target.strip_prefix("agent:").unwrap_or(target))
                );
                let response = self
                    .client
                    .get(
                        &path,
                        Some(serde_json::to_vec(&request).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(response))
            }
            "list" | "ls" => {
                let state_trimmed = args
                    .state
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let parent = args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.strip_prefix("agent:").unwrap_or(s));
                // Explicit archive states keep their historical daemon population
                // (ordered and capped before MCP filters). Lifecycle/turn-state
                // filters, ranking and summary need the coordinator's live view.
                let live_parent = parent
                    .filter(|_| {
                        !state_trimmed.is_some_and(|s| {
                            s.eq_ignore_ascii_case("all")
                                || s.eq_ignore_ascii_case("archived")
                                // Archived ancestors have turn_state=idle.
                                || s.eq_ignore_ascii_case("idle")
                        })
                    })
                    .and_then(|p| {
                        if p.eq_ignore_ascii_case("root") || p.eq_ignore_ascii_case("none") {
                            Some("root")
                        } else if p.eq_ignore_ascii_case("subagents")
                            || p.eq_ignore_ascii_case("all_subagents")
                        {
                            Some("subagents")
                        } else if !matches!(
                            p.to_ascii_lowercase().as_str(),
                            "all" | "parent" | "sibling" | "siblings" | "peer" | "peers"
                        ) && (p.eq_ignore_ascii_case("self")
                            || p.eq_ignore_ascii_case("children")
                            || p.eq_ignore_ascii_case(&self.agent.name)
                            || p == self.agent.id.as_str())
                        {
                            Some(self.agent.id.as_str())
                        } else {
                            None
                        }
                    });
                let mut response = if let Some(parent) = live_parent {
                    self.client
                        .get_live_agents_for_parent(
                            parent,
                            args.include_descendants.unwrap_or(false),
                        )
                        .await?
                } else {
                    self.client.get_agents_for_state(state_trimmed).await?
                };
                if let Some(cleaned) = parent.filter(|_| live_parent.is_none()) {
                    if cleaned.eq_ignore_ascii_case("root") || cleaned.eq_ignore_ascii_case("none")
                    {
                        response.agents.retain(|a| a.parent.is_none());
                    } else if cleaned.eq_ignore_ascii_case("subagents")
                        || cleaned.eq_ignore_ascii_case("all_subagents")
                    {
                        response.agents.retain(|a| a.parent.is_some());
                    } else if cleaned.eq_ignore_ascii_case("siblings")
                        || cleaned.eq_ignore_ascii_case("sibling")
                        || cleaned.eq_ignore_ascii_case("peers")
                        || cleaned.eq_ignore_ascii_case("peer")
                    {
                        response.agents.retain(|a| {
                            matches!(a.relationship.as_deref(), Some("sibling" | "peer"))
                        });
                    } else if cleaned.eq_ignore_ascii_case("parent") {
                        response
                            .agents
                            .retain(|a| a.relationship.as_deref() == Some("parent"));
                    } else if !cleaned.eq_ignore_ascii_case("all") {
                        let wanted_id = if cleaned.eq_ignore_ascii_case("self")
                            || cleaned.eq_ignore_ascii_case("children")
                            || cleaned.eq_ignore_ascii_case(&self.agent.name)
                            || cleaned == self.agent.id.as_str()
                        {
                            Some(self.agent.id.to_string())
                        } else {
                            response
                                .agents
                                .iter()
                                .find(|a| {
                                    a.name.eq_ignore_ascii_case(cleaned)
                                        || a.id == cleaned
                                        || a.aliases
                                            .iter()
                                            .any(|al| al.eq_ignore_ascii_case(cleaned))
                                })
                                .map(|a| a.id.clone())
                                .or_else(|| Some(cleaned.to_string()))
                        };
                        if let Some(wid) = wanted_id {
                            let sub = args.include_descendants.unwrap_or(false);
                            retain_parent_or_descendants(&mut response.agents, &wid, sub);
                        }
                    }
                }
                match state_trimmed {
                    Some(state_filter) if !state_filter.eq_ignore_ascii_case("all") => {
                        response.agents.retain(|a| {
                            a.state.eq_ignore_ascii_case(state_filter)
                                || a.turn_state.eq_ignore_ascii_case(state_filter)
                        });
                    }
                    Some(_) => {}
                    None => {
                        response.agents.retain(|a| a.state != "archived");
                    }
                }
                let summary = filter_and_summarize_agent_list(
                    &mut response.agents,
                    args.query.as_deref(),
                    args.limit,
                    args.cursor,
                );
                let mut val = serde_json::to_value(&response).map_err(|e| e.to_string())?;
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("next_cursor".to_string(), summary["next_cursor"].clone());
                    obj.insert("summary".to_string(), summary);
                }
                Ok(rmcp::handler::server::wrapper::Json(bound_agent_list(
                    val,
                    "agents",
                    args.cursor,
                    args.detail.into(),
                )?))
            }
            "spawn" => {
                let spawned = self
                    .spawn(Parameters(agent_secrets::spawn_args_from_agents(args)))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(spawned).map_err(|e| e.to_string())?,
                ))
            }
            "create" => {
                let body = agent_secrets::create_request_from_agents(self.agent.id.as_str(), args)?;
                let created: AgentView = self
                    .client
                    .get(
                        "/v1/agents",
                        Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(created).map_err(|e| e.to_string())?,
                ))
            }
            "update" | "patch" => {
                let updated = agent_secrets::patch_agent(
                    &self.client,
                    args.name.as_deref(),
                    crate::api::PatchAgentRequest {
                        description: args.description,
                        instructions: args.instructions,
                        role: args.role,
                        scope: args.scope,
                        lifecycle: args.lifecycle,
                        public: args.public,
                        secrets: args.secrets,
                        browser_profiles: args.browser_profiles,
                    },
                )
                .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "secrets" => {
                let res =
                    agent_secrets::agent_secrets(&self.client, args.name.as_deref(), args.secrets)
                        .await?;
                Ok(rmcp::handler::server::wrapper::Json(res))
            }
            "browser_profiles" => {
                self.handle_agents_browser_profiles(args.name.as_deref(), args.browser_profiles)
                    .await
            }
            "describe" => {
                self.describe(Parameters(DescribeArgs {
                    agent: args.name,
                    description: args.description,
                    instructions: args.instructions,
                    role: args.role,
                    scope: args.scope,
                    lifecycle: args.lifecycle,
                    public: args.public,
                    clear: args.clear,
                    browser_profiles: args.browser_profiles,
                }))
                .await
            }
            "steer" | "send" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents steer` requires `name` (or `agent`)".to_string())?;
                let text = args
                    .prompt
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents steer` requires `prompt`".to_string())?;
                self.send(Parameters(SendArgs {
                    to: vec![target.to_string()],
                    text: Some(text.to_string()),
                    ..SendArgs::default()
                }))
                .await
            }
            "wake" | "resume" => {
                let target = args
                    .name
                    .ok_or_else(|| "`agents wake` requires `name` (or `agent`)".to_string())?;
                let woke = self
                    .wake(Parameters(WakeArgs {
                        agent: target,
                        prompt: args.prompt,
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(woke).map_err(|e| e.to_string())?,
                ))
            }
            "reset_session" | "new_session" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        "`agents reset_session` requires `name` (or `agent`)".to_string()
                    })?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let result: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/session/reset"),
                        Some(b"{}".to_vec()),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(result))
            }
            "pause" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents pause` requires `name` (or `agent`)".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let view: AgentView = self
                    .client
                    .get(&format!("/v1/agents/{clean}/pause"), Some(b"{}".to_vec()))
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(view).map_err(|e| e.to_string())?,
                ))
            }
            "archive" => {
                let view = self
                    .archive(Parameters(ArchiveArgs { agent: args.name }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(view).map_err(|e| e.to_string())?,
                ))
            }
            "delete" | "kill" | "remove" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents delete` requires `name` (or `agent`)".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let deleted: serde_json::Value = self
                    .client
                    .request_method("DELETE", &format!("/v1/agents/{clean}"), None)
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(deleted))
            }
            "rename" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents rename` requires `name`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let new_name = args
                    .new_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents rename` requires `new_name`".to_string())?;
                let clean_new = new_name.strip_prefix("agent:").unwrap_or(new_name);
                let body = serde_json::json!({
                    "name": clean_new,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/rename"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "role" | "set_role" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents role` requires `name`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let body = serde_json::json!({
                    "role": args.role,
                    "clear": args.clear.unwrap_or(false),
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/role"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "harness" | "set_harness" | "runtime" => {
                let updated = super::agent_secrets::switch_agent_harness_or_runtime(
                    &self.client,
                    "agents harness",
                    "name",
                    args.name.as_deref(),
                    args.harness.as_deref(),
                    args.runtime.as_deref(),
                    args.budget_cap_micros,
                    args.model,
                    args.profile,
                )
                .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "transfer" | "reparent" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents transfer` requires `name` (or `agent`)".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let new_parent = args
                    .parent
                    .as_deref()
                    .or(args.new_name.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let body = serde_json::json!({
                    "parent": new_parent,
                    "reason": args.prompt,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/transfer"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "inspect" | "status" | "get" | "info" | "show" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("self");
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let agent_path = format!("/v1/agents/{}", urlencoding_simple(clean));
                let view: AgentView = self.client.get(&agent_path, None).await?;
                let limit = args.limit.unwrap_or(10);
                let conv_path = format!("/v1/agents/{}/conversation?limit={limit}", view.id);
                let conv: ConversationResponse =
                    self.client
                        .get(&conv_path, None)
                        .await
                        .unwrap_or(ConversationResponse {
                            retrieval: None,
                            agent_id: view.id.clone(),
                            agent: view.name.clone(),
                            turns: Vec::new(),
                        });
                Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                    "agent": view,
                    "recent_turns": conv.turns,
                })))
            }
            "models" | "catalog" | "profiles" => {
                let filter = args
                    .profile
                    .as_deref()
                    .or(args.harness.as_deref())
                    .or(args.name.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let path = match filter {
                    Some(f) => format!("/v1/profiles/models?filter={}", urlencoding_simple(f)),
                    None => "/v1/profiles/models".to_string(),
                };
                let catalogs: serde_json::Value = self.client.get(&path, None).await?;
                Ok(rmcp::handler::server::wrapper::Json(catalogs))
            }
            "telegram_bot" | "bind_telegram" | "channel" | "request_bot" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        "`agents telegram_bot` requires `name` (target sub-agent name or ID)"
                            .to_string()
                    })?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let body = serde_json::json!({
                    "kind": "telegram",
                    "agent": clean,
                    "token": args.token,
                    "bot_id": args.bot_id,
                    "bot_username": args.bot_username,
                    "manager": args.parent,
                });
                let res: serde_json::Value = self
                    .client
                    .get(
                        "/v1/channels",
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(res))
            }
            "channels" | "list_channels" => {
                let res: serde_json::Value = self.client.get("/v1/channels", None).await?;
                Ok(rmcp::handler::server::wrapper::Json(res))
            }
            other => Err(format!(
                "unknown agents action `{other}`; expected `list`, `inspect`, `spawn`, `create`, `update`, `secrets`, `describe`, `verdict`, `redact`, `steer`, `wake`, `pause`, `archive`, `delete`, `rename`, `role`, `harness`, `transfer`, `telegram_bot`, `channels`, `usage`, or `models`"
            )),
        }
    }

    #[tool(
        description = "Spawn a new sub-agent with this agent as its `parent` (supports multi-level sub-agent trees). Pass `repo` (e.g. `\"example-org/workspace\"` or `\"inherit\"` to branch from your current repo, with optional `branch` and `base`) to automatically provision an isolated, lifecycle-managed Git repository of its own at `<data>/agents/<id>/work` (cleanly reaped on `archive` when pushed, or preserved if dirty). If both `repo` and `workdir` are omitted, defaults to its own isolated `<data>/agents/<id>/work`. Every sub-agent automatically receives the shared `AGENTS.md` and top-level `skills` in its config home."
    )]
    pub(super) async fn spawn(
        &self,
        Parameters(args): Parameters<SpawnArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<AgentView>, String> {
        let default_name = generate_subagent_name(&self.agent.name);
        let body =
            agent_secrets::create_request_from_spawn(self.agent.id.as_str(), default_name, args);
        let response = self
            .client
            .get(
                "/v1/agents",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Unified cross-harness sub-agent manager (`action`: `list` | `inspect` | `spawn` | `steer` | `wake` | `pause` | `archive` | `delete`, supporting multi-level sub-agent trees). Use `list` (default) to view your sub-agents and their live status (`running`, `paused`, `archived`, `turn_state`); use `inspect` with `agent` to view live status AND recent conversation turns; use `spawn` to launch a sub-agent on any harness (`opencode`, `codex`, `claude-code`, `agy`, `muse`, `omp`, `spiron`), model, profile, and workspace (`repo`: `\"<org>/<repo>\"` or `\"inherit\"` for an isolated Git repository at `<data>/agents/<id>/work`; `workdir`: path, `\"worktree\"`, `\"private\"`, or `\"inherit\"` for explicit parent sharing; omitting both `repo` and `workdir` gives a private directory); use `steer` to send follow-up instructions; use `pause`, `archive` (auto-reaps clean/pushed managed checkouts while preserving dirty ones), `wake`, or `delete` to control its lifecycle."
    )]
    pub(super) async fn subagents(
        &self,
        Parameters(args): Parameters<SubagentsArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("list")
            .to_ascii_lowercase();
        match action.as_str() {
            "list" | "ls" => {
                let parent_arg = args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("self");
                let mut resp = self
                    .client
                    .get_agents_for_state(args.state.as_deref())
                    .await?;
                if parent_arg.eq_ignore_ascii_case("all") {
                    resp.agents.retain(|a| a.parent.is_some());
                } else {
                    let cleaned = parent_arg.strip_prefix("agent:").unwrap_or(parent_arg);
                    let wanted_id = if cleaned.eq_ignore_ascii_case("self")
                        || cleaned.eq_ignore_ascii_case(&self.agent.name)
                        || cleaned == self.agent.id.as_str()
                    {
                        self.agent.id.to_string()
                    } else {
                        resp.agents
                            .iter()
                            .find(|a| {
                                a.name.eq_ignore_ascii_case(cleaned)
                                    || a.id == cleaned
                                    || a.aliases.iter().any(|al| al.eq_ignore_ascii_case(cleaned))
                            })
                            .map(|a| a.id.clone())
                            .unwrap_or_else(|| cleaned.to_string())
                    };
                    retain_parent_or_descendants(
                        &mut resp.agents,
                        &wanted_id,
                        args.include_descendants.unwrap_or(false),
                    );
                }
                if let Some(st) = args
                    .state
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
                {
                    resp.agents.retain(|a| {
                        a.state.eq_ignore_ascii_case(st) || a.turn_state.eq_ignore_ascii_case(st)
                    });
                }
                let summary = filter_and_summarize_agent_list(
                    &mut resp.agents,
                    args.query.as_deref(),
                    args.limit,
                    args.cursor,
                );
                let value = serde_json::json!({
                    "parent": self.agent.name,
                    "next_cursor": summary["next_cursor"],
                    "summary": summary,
                    "subagents": resp.agents,
                });
                Ok(rmcp::handler::server::wrapper::Json(bound_agent_list(
                    value,
                    "subagents",
                    args.cursor,
                    AgentListProjection::Legacy,
                )?))
            }
            "inspect" | "status" | "get" | "info" | "show" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("self");
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let agent_path = format!("/v1/agents/{}", urlencoding_simple(clean));
                let view: AgentView = self.client.get(&agent_path, None).await?;
                let limit = args.limit.unwrap_or(10);
                let conv_path = format!("/v1/agents/{}/conversation?limit={limit}", view.id);
                let conv: ConversationResponse =
                    self.client
                        .get(&conv_path, None)
                        .await
                        .unwrap_or(ConversationResponse {
                            retrieval: None,
                            agent_id: view.id.clone(),
                            agent: view.name.clone(),
                            turns: Vec::new(),
                        });
                Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                    "subagent": view,
                    "recent_turns": conv.turns,
                })))
            }
            "spawn" | "create" => {
                let spawned = self
                    .spawn(Parameters(agent_secrets::spawn_args_from_subagents(args)))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(spawned).map_err(|e| e.to_string())?,
                ))
            }
            "update" | "patch" => {
                let updated = agent_secrets::patch_agent(
                    &self.client,
                    args.agent.as_deref(),
                    crate::api::PatchAgentRequest {
                        description: args.description,
                        instructions: args.instructions,
                        role: args.role,
                        scope: args.scope,
                        lifecycle: args.lifecycle,
                        public: args.public,
                        secrets: args.secrets,
                        browser_profiles: args.browser_profiles,
                    },
                )
                .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "secrets" => {
                let res =
                    agent_secrets::agent_secrets(&self.client, args.agent.as_deref(), args.secrets)
                        .await?;
                Ok(rmcp::handler::server::wrapper::Json(res))
            }
            "browser_profiles" => {
                self.handle_agents_browser_profiles(args.agent.as_deref(), args.browser_profiles)
                    .await
            }
            "steer" | "send" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents steer` requires `agent`".to_string())?;
                let text = args
                    .prompt
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents steer` requires `prompt`".to_string())?;
                let sent = self
                    .send(Parameters(SendArgs {
                        to: vec![target.to_string()],
                        text: Some(text.to_string()),
                        ..SendArgs::default()
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(sent).map_err(|e| e.to_string())?,
                ))
            }
            "wake" | "resume" => {
                let target = args
                    .agent
                    .ok_or_else(|| "`subagents wake` requires `agent`".to_string())?;
                let woke = self
                    .wake(Parameters(WakeArgs {
                        agent: target,
                        prompt: args.prompt,
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(woke).map_err(|e| e.to_string())?,
                ))
            }
            "pause" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents pause` requires `agent`".to_string())?
                    .strip_prefix("agent:")
                    .unwrap_or_default()
                    .to_string();
                let clean = if target.is_empty() {
                    args.agent.unwrap()
                } else {
                    target
                };
                let view: AgentView = self
                    .client
                    .get(&format!("/v1/agents/{clean}/pause"), Some(b"{}".to_vec()))
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(view).map_err(|e| e.to_string())?,
                ))
            }
            "archive" => {
                let view = self
                    .archive(Parameters(ArchiveArgs { agent: args.agent }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(view).map_err(|e| e.to_string())?,
                ))
            }
            "harness" | "set_harness" | "runtime" => {
                let updated = super::agent_secrets::switch_agent_harness_or_runtime(
                    &self.client,
                    "subagents harness",
                    "agent",
                    args.agent.as_deref(),
                    args.harness.as_deref(),
                    args.runtime.as_deref(),
                    args.budget_cap_micros,
                    args.model,
                    args.profile,
                )
                .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "transfer" | "reparent" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents transfer` requires `agent`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let new_parent = args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let body = serde_json::json!({
                    "parent": new_parent,
                    "reason": args.prompt,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/transfer"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(updated))
            }
            "delete" | "kill" | "remove" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents delete` requires `agent`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let deleted: serde_json::Value = self
                    .client
                    .request_method("DELETE", &format!("/v1/agents/{clean}"), None)
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(deleted))
            }
            "telegram_bot" | "bind_telegram" | "channel" | "request_bot" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        "`subagents telegram_bot` requires `agent` (target sub-agent name or ID)"
                            .to_string()
                    })?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let body = serde_json::json!({
                    "kind": "telegram",
                    "agent": clean,
                    "token": args.token,
                    "bot_id": args.bot_id,
                    "bot_username": args.bot_username,
                    "manager": args.parent,
                });
                let res: serde_json::Value = self
                    .client
                    .get(
                        "/v1/channels",
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                    )
                    .await?;
                Ok(rmcp::handler::server::wrapper::Json(res))
            }
            other => Err(format!(
                "unknown subagents action `{other}`; expected `list`, `inspect`, `spawn`, `steer`, `wake`, `pause`, `archive`, `harness`, `transfer`, `telegram_bot`, or `delete`"
            )),
        }
    }

    #[tool(
        description = "Archive an agent (`state = archived`), stopping its OS process while preserving its permanent `agt_...` ID, mailbox, and session so an intentional direct `send` or `wake` call resumes it. For sub-agents with a managed Git checkout, automatically removes it if it is clean and has no unpushed commits, or preserves it on disk if uncommitted/unpushed work remains."
    )]
    pub(super) async fn archive(
        &self,
        Parameters(args): Parameters<ArchiveArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<crate::api::ArchiveResponse>, String> {
        let target = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("self"))
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
            .filter(|s| !s.eq_ignore_ascii_case("self"))
            .unwrap_or_else(|| self.agent.id.to_string());
        let response = self
            .client
            .get(
                &format!("/v1/agents/{target}/archive"),
                Some(b"{}".to_vec()),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Wake and resume an archived or paused agent by its `agt_...` ID or name, optionally delivering a follow-up `prompt`."
    )]
    pub(super) async fn wake(
        &self,
        Parameters(args): Parameters<WakeArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<AgentView>, String> {
        let target = args
            .agent
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(args.agent.trim())
            .to_string();
        let response: AgentView = self
            .client
            .get(&format!("/v1/agents/{target}/wake"), Some(b"{}".to_vec()))
            .await?;
        if let Some(prompt) = args
            .prompt
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let _ = self
                .send(Parameters(SendArgs {
                    to: vec![target],
                    text: Some(prompt.to_string()),
                    ..SendArgs::default()
                }))
                .await?;
        }
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Create, join, leave, or list internal agent groups (`action`: `list` | `members` | `join` | `leave`). Agents can form internal groups without any external channel or human."
    )]
    pub(super) async fn groups(
        &self,
        Parameters(args): Parameters<GroupsArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<GroupsResponse>, String> {
        let action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("list");
        if action == "list" {
            let path = if args.include_archived.unwrap_or(false) {
                "/v1/groups?include_archived=true"
            } else {
                "/v1/groups"
            };
            let response = self.client.get(path, None).await?;
            return Ok(rmcp::handler::server::wrapper::Json(response));
        }
        let group = args
            .group
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("groups {action} requires a `group` name"))?
            .to_string();
        let combined_agent = {
            let mut list = Vec::new();
            if let Some(a) = args
                .agent
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                list.push(a.to_string());
            }
            for item in args.agents {
                let trimmed = item.trim();
                if !trimmed.is_empty() {
                    list.push(trimmed.to_string());
                }
            }
            if list.is_empty() {
                None
            } else {
                Some(list.join(","))
            }
        };
        let body = GroupActionRequest {
            action: action.to_string(),
            group,
            agent: combined_agent,
            public: args.public,
        };
        let one: GroupView = self
            .client
            .get(
                "/v1/groups",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(GroupsResponse {
            groups: vec![one],
        }))
    }

    #[tool(
        description = "Perform a rich IM channel or message interaction (`action`: `react` | `vote` | `edit` | `delete` | `pin` | `unpin` | `stop_poll`) on a target message (`anyagents:reply:msg_...` from the message header or `telegram:chat:<chat_id>:<message_id>`)."
    )]
    pub(super) async fn interact(
        &self,
        Parameters(args): Parameters<InteractArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<InteractResponse>, String> {
        let body = InteractRequest {
            action: args.action,
            target: args.target,
            emoji: args.emoji,
            options: args.options,
            text: args.text,
            bot: args.bot,
            ..Default::default()
        };
        Ok(rmcp::handler::server::wrapper::Json(
            send_tool::post_interaction(self, body).await?,
        ))
    }

    #[tool(
        description = "Read or update an agent's role & responsibility description (`description` / `instructions`). Any agent can read its own or peers' descriptions (`agent: \"self\"` or `\"all\"`), while updating `description`, `instructions`, `role`, or `rename` requires the target's Parent/Ancestor agent or an Admin (`self` cannot modify its own `description` or `instructions` unless it is an Admin). Updating `description` automatically syncs the agent's private `SYSTEM_PROMPT.md` / `AGENTS.md` and makes the role visible to all peers in `agents` and `groups`."
    )]
    pub(super) async fn describe(
        &self,
        Parameters(args): Parameters<DescribeArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let target = args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
            .unwrap_or_else(|| self.agent.name.clone());
        let resolved_target = if target.eq_ignore_ascii_case("self") {
            self.agent.name.clone()
        } else {
            target
        };
        if resolved_target.eq_ignore_ascii_case("all")
            && args.description.is_none()
            && args.instructions.is_none()
            && args.role.is_none()
            && args.scope.is_none()
            && args.lifecycle.is_none()
            && args.public.is_none()
            && !args.clear.unwrap_or(false)
        {
            let resp: AgentsResponse = self.client.get_agents().await?;
            let roster: Vec<serde_json::Value> = resp
                .agents
                .into_iter()
                .filter(|a| a.state != "archived" || a.parent.is_none())
                .map(|a| {
                    serde_json::json!({
                        "id": a.id,
                        "name": a.name,
                        "role": a.role,
                        "aliases": a.aliases,
                        "parent": a.parent,
                        "scope": a.scope,
                        "lifecycle": a.lifecycle,
                        "relationship": a.relationship,
                        "can_message": a.can_message,
                        "harness": a.harness,
                        "runtime": a.runtime,
                        "model": a.model,
                        "state": a.state,
                        "description": a.description,
                    })
                })
                .collect();
            return Ok(rmcp::handler::server::wrapper::Json(serde_json::json!({
                "agents": roster,
            })));
        }
        if args.description.is_some()
            || args.instructions.is_some()
            || args.role.is_some()
            || args.scope.is_some()
            || args.lifecycle.is_some()
            || args.public.is_some()
            || args.clear.unwrap_or(false)
        {
            let body = serde_json::json!({
                "description": args.description,
                "instructions": args.instructions,
                "role": args.role,
                "scope": args.scope,
                "lifecycle": args.lifecycle,
                "public": args.public,
                "clear": args.clear.unwrap_or(false),
            });
            let mut updated: serde_json::Value = self
                .client
                .get(
                    &format!("/v1/agents/{resolved_target}/description"),
                    Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                )
                .await?;
            if let Some(profiles) = args.browser_profiles {
                let bp = self
                    .set_agent_browser_profiles(&resolved_target, profiles)
                    .await?;
                if let Some(obj) = updated.as_object_mut() {
                    obj.insert(
                        "browser_profiles".to_string(),
                        bp["browser_profiles"].clone(),
                    );
                }
            }
            return Ok(rmcp::handler::server::wrapper::Json(updated));
        }
        if let Some(profiles) = args.browser_profiles {
            let bp = self
                .set_agent_browser_profiles(&resolved_target, profiles)
                .await?;
            return Ok(rmcp::handler::server::wrapper::Json(bp));
        }
        let current: serde_json::Value = self
            .client
            .get(&format!("/v1/agents/{resolved_target}/description"), None)
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(current))
    }

    #[tool(
        description = SCHEDULES_TOOL_DESCRIPTION
    )]
    pub(super) async fn schedule(
        &self,
        Parameters(args): Parameters<ScheduleArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let target_opt = match args
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("self"))
        {
            Some(raw) => Some(self.resolve_agent_word(raw).await?),
            None => None,
        }
        .map(|s| s.strip_prefix("agent:").unwrap_or(&s).to_string())
        .filter(|s| s != &self.agent.name && s != self.agent.id.as_str());
        let path = match target_opt.as_deref() {
            Some(target) => format!("/v1/agents/{target}/schedules"),
            None => "/v1/schedules".to_string(),
        };
        let body = serde_json::json!({
            "action": args.action,
            "at": args.at,
            "every": args.every,
            "watch": args.watch,
            "persistent": args.persistent,
            "spec": args.spec,
            "prompt": args.prompt,
            "id": args.id,
            "agent": target_opt,
        });
        let response: serde_json::Value = self
            .client
            .get(
                &path,
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = "Read your own incoming mailbox queue (`since`, `limit`, `unread`, `sender`). Returns structured messages with delivery sequence numbers, sender identities, reply tokens, and attachments."
    )]
    pub(super) async fn inbox(
        &self,
        Parameters(args): Parameters<InboxArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<crate::api::InboxResponse>, String> {
        let mut query = Vec::new();
        if let Some(since) = args.since {
            query.push(format!("since={since}"));
        }
        if let Some(limit) = args.limit {
            query.push(format!("limit={limit}"));
        }
        if let Some(unread) = args.unread {
            query.push(format!("unread={unread}"));
        }
        if let Some(sender) = args
            .sender
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            query.push(format!("sender={}", urlencoding_simple(sender)));
        }
        let path = if query.is_empty() {
            format!("/v1/agents/{}/inbox", self.agent.id)
        } else {
            format!("/v1/agents/{}/inbox?{}", self.agent.id, query.join("&"))
        };
        let response = self.client.get(&path, None).await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
    }

    #[tool(
        description = SCHEDULES_TOOL_DESCRIPTION
    )]
    pub(super) async fn schedules(
        &self,
        Parameters(args): Parameters<ScheduleArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        self.schedule(Parameters(args)).await
    }

    #[tool(
        description = "Manage learned memory: list compact topic pages (limit, offset, query, prefix), read a topic, write/upsert, delete, or search. List/read accept a public peer name or ID in agent, or scope=shared to browse all public peers. Writes and deletes are self-only. For versioned knowledge use scope=org, domain:<slug>, agent, or all, or specify a namespace. Versioned writes require reason and use expected_version. With no action, delete=true deletes, content writes, query searches, topic reads, and no arguments lists."
    )]
    pub(super) async fn memory(
        &self,
        Parameters(args): Parameters<MemoryArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let val = self.execute_memory_tool(args).await?;
        Ok(rmcp::handler::server::wrapper::Json(val))
    }
}

impl AgentMcp {
    /// `self`, `me` and `parent` as the agent they name for this caller; any
    /// other name as written. Every tool that takes an agent name reads these
    /// words the way `send` reads them in `to`.
    pub(super) async fn resolve_agent_word(&self, raw: &str) -> Result<String, String> {
        let name = raw.trim();
        let word = name.strip_prefix("agent:").unwrap_or(name);
        if word.eq_ignore_ascii_case("self") || word.eq_ignore_ascii_case("me") {
            return Ok(self.agent.name.clone());
        }
        if !word.eq_ignore_ascii_case("parent") {
            return Ok(name.to_string());
        }
        let me: AgentView = self
            .client
            .get(&format!("/v1/agents/{}", self.agent.id), None)
            .await?;
        let parent_id = me.parent.ok_or_else(|| {
            format!(
                "`parent`: `{}` is a root agent and has no parent",
                self.agent.name
            )
        })?;
        // History and peer filters match names, so answer with the name.
        let parent: AgentView = self
            .client
            .get(&format!("/v1/agents/{parent_id}"), None)
            .await?;
        Ok(parent.name)
    }

    /// The universal surface every agent sees on every harness.
    pub(super) fn listed_tools(&self) -> Vec<rmcp::model::Tool> {
        let wd = self.resolve_workdir();
        tool_definitions_with_roles(Some(&wd), self.client.socket.parent())
    }

    pub(super) fn resolve_workdir(&self) -> PathBuf {
        if let Some(ref wd) = self.agent.workdir
            && !wd.as_os_str().is_empty()
        {
            return wd.clone();
        }
        if let Some(agent_root) = self.client.config.parent() {
            let sibling_work = agent_root.join("work");
            if sibling_work.is_dir() {
                return sibling_work;
            }
        }
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    }

    pub(crate) fn new_for_http(
        agent: AgentRef,
        socket: PathBuf,
        config: PathBuf,
        principal: crate::auth::Principal,
        router: axum::Router,
    ) -> Self {
        Self {
            agent,
            client: Client::new(socket, config, String::new()).over_http_router(router, principal),
        }
    }

    pub(crate) async fn handle_jsonrpc_request(
        &self,
        payload: serde_json::Value,
    ) -> Option<serde_json::Value> {
        if let serde_json::Value::Array(items) = payload {
            let mut responses = Vec::new();
            for item in items {
                if let Some(resp) = self.handle_single_jsonrpc(item).await {
                    responses.push(resp);
                }
            }
            if responses.is_empty() {
                None
            } else {
                Some(serde_json::Value::Array(responses))
            }
        } else {
            self.handle_single_jsonrpc(payload).await
        }
    }

    pub(super) async fn handle_single_jsonrpc(
        &self,
        req: serde_json::Value,
    ) -> Option<serde_json::Value> {
        let Some(obj) = req.as_object() else {
            return Some(serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {
                    "code": -32600,
                    "message": "Invalid Request: expected JSON object"
                }
            }));
        };
        let id = obj.get("id").cloned();
        let method = obj
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .trim();
        let params = obj
            .get("params")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        // Notifications (such as `notifications/initialized`) carry no `id` and expect no body.
        if id.is_none() || method.starts_with("notifications/") {
            return None;
        }
        let id_val = id.unwrap_or(serde_json::Value::Null);

        match method {
            "initialize" => {
                let requested_version = params
                    .get("protocolVersion")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("2025-03-26");
                Some(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id_val,
                    "result": {
                        "protocolVersion": requested_version,
                        "capabilities": {
                            "tools": {
                                "listChanged": false
                            }
                        },
                        "serverInfo": {
                            "name": "anyagents",
                            "version": env!("CARGO_PKG_VERSION")
                        }
                    }
                }))
            }
            "ping" => Some(serde_json::json!({
                "jsonrpc": "2.0",
                "id": id_val,
                "result": {}
            })),
            "tools/list" => {
                let tools = self.listed_tools();
                Some(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id_val,
                    "result": {
                        "tools": tools
                    }
                }))
            }
            "tools/call" => {
                let Some(tool_name) = params
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                else {
                    return Some(serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id_val,
                        "error": {
                            "code": -32602,
                            "message": "missing required `name` in `tools/call` params"
                        }
                    }));
                };
                if !profile::allows_tool(tool_name) {
                    return Some(serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id_val,
                        "error": {
                            "code": -32601,
                            "message": format!(
                                "tool `{tool_name}` is not available on the AnyAgents MCP surface"
                            )
                        }
                    }));
                }
                let args = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let call_result =
                    match self.dispatch_any_tool(tool_name, args).await {
                        Ok(val) => rmcp::model::CallToolResult::structured(val),
                        Err(err_msg) => rmcp::model::CallToolResult::error(vec![
                            rmcp::model::ContentBlock::text(err_msg),
                        ]),
                    };
                Some(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id_val,
                    "result": call_result
                }))
            }
            other => Some(serde_json::json!({
                "jsonrpc": "2.0",
                "id": id_val,
                "error": {
                    "code": -32601,
                    "message": format!("Method not found: `{other}`")
                }
            })),
        }
    }

    pub(super) async fn dispatch_any_tool(
        &self,
        tool: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let norm = tool
            .trim()
            .strip_prefix("mcp__anyagents__")
            .unwrap_or(tool.trim());
        if !profile::allows_tool(norm) {
            return Err(format!(
                "tool `{norm}` is not available on the AnyAgents MCP surface"
            ));
        }
        match norm {
            "ingress_guard" => {
                ingress_tool::call(
                    self,
                    serde_json::from_value(args).map_err(|e| e.to_string())?,
                )
                .await
            }
            "inbox" => {
                let parsed: InboxArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `inbox` arguments: {e}"))?;
                let res = self.inbox(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "memory" => {
                let parsed: MemoryArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `memory` arguments: {e}"))?;
                Ok(self.memory(Parameters(parsed)).await?.0)
            }
            "agents" => {
                let parsed: AgentsArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `agents` arguments: {e}"))?;
                Ok(self.agents(Parameters(parsed)).await?.0)
            }
            "send" => {
                let parsed: SendArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `send` arguments: {e}"))?;
                Ok(self.send(Parameters(parsed)).await?.0)
            }
            "report_bug" => {
                let parsed = serde_json::from_value(args)
                    .map_err(|e| format!("invalid report_bug arguments: {e}"))?;
                serde_json::to_value(self.report_bug(Parameters(parsed)).await?.0)
                    .map_err(|e| e.to_string())
            }
            "conversation" | "search" => {
                let parsed: ConversationArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `conversation` arguments: {e}"))?;
                let res = self.conversation(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "groups" => {
                let parsed: GroupsArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `groups` arguments: {e}"))?;
                let res = self.groups(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "schedule" | "schedules" => {
                let parsed: ScheduleArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `schedule` arguments: {e}"))?;
                Ok(self.schedule(Parameters(parsed)).await?.0)
            }
            "subagents" => {
                let parsed: SubagentsArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `subagents` arguments: {e}"))?;
                Ok(self.subagents(Parameters(parsed)).await?.0)
            }
            "spawn" => {
                let parsed: SpawnArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `spawn` arguments: {e}"))?;
                let res = self.spawn(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "describe" => {
                let parsed: DescribeArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `describe` arguments: {e}"))?;
                Ok(self.describe(Parameters(parsed)).await?.0)
            }
            "archive" => {
                let parsed: ArchiveArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `archive` arguments: {e}"))?;
                let res = self.archive(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "wake" => {
                let parsed: WakeArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `wake` arguments: {e}"))?;
                let res = self.wake(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            "interact" => {
                let parsed: InteractArgs = serde_json::from_value(args)
                    .map_err(|e| format!("invalid `interact` arguments: {e}"))?;
                let res = self.interact(Parameters(parsed)).await?.0;
                serde_json::to_value(res).map_err(|e| e.to_string())
            }
            other => Err(format!("unknown AnyAgents tool {other}")),
        }
    }
}
