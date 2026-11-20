//! MCP tool router (`#[tool_router]`), JSON-RPC dispatch, and tool handlers.

use super::*;

#[tool_router(vis = "pub(super)")]
impl AgentMcp {
    #[tool(
        description = "Unified outbound communication and message interaction tool: send a message, media file (`photo`, `video`, `audio`, `music`, `voice`, `document`, `animation`), GPS `location`/venue, interactive `poll`, `checklist`, or Instant View `article` to one or multiple agents (`planner`, `agt_...`, `parent`, `children`), internal groups (`group:<name>`, `group:all`, `all`), or external channel targets (`telegram:chat:<id>[:<topic>]`, `telegram:user:<id>`); OR perform a message interaction (`action`: `\"react\"` | `\"vote\"` | `\"check\"` | `\"uncheck\"` | `\"edit\"` | `\"delete\"` | `\"pin\"` | `\"unpin\"` | `\"stop_poll\"`) on a target message (`target` or `reply_to`: `anyagents:reply:msg_...` or `telegram:chat:<chat_id>:<message_id>`). Use `operator` to report a defect in AnyAgents, the harness, or the host environment that you cannot fix within your own work. Include the exact error and what you tried. Do not work around it silently."
    )]
    pub(super) async fn send(
        &self,
        Parameters(args): Parameters<SendArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let mut args = args;
        // Relation words (`parent`, `children`, ...) read the same in `to` and
        // in `reply_to`, whatever their case or `agent:` prefix.
        for target in &mut args.to {
            if let Some(word) = relation_word(target) {
                *target = word.to_string();
            }
        }
        if args.to.is_empty()
            && let Some(word) = args
                .reply_to
                .as_deref()
                .and_then(relation_word)
                .filter(|word| matches!(*word, "parent" | "children"))
        {
            args.to = vec![word.to_string()];
            args.reply_to = None;
        }
        let inferred_interact = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("send"))
            .map(str::to_ascii_lowercase)
            .or_else(|| {
                if args.emoji.is_some() && args.to.is_empty() && args.text.trim().is_empty() {
                    Some("react".to_string())
                } else {
                    None
                }
            });
        if let Some(interact_action) = inferred_interact {
            let target = args
                .target
                .or(args.reply_to)
                .or_else(|| args.to.into_iter().next())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    format!("`send` with `action = \"{interact_action}\"` requires `target` or `reply_to`")
                })?;
            let text_opt = if args.text.trim().is_empty() {
                None
            } else {
                Some(args.text)
            };
            let res = self
                .interact(Parameters(InteractArgs {
                    action: interact_action,
                    target,
                    emoji: args.emoji,
                    options: args.options,
                    text: text_opt,
                    bot: args.bot,
                }))
                .await?
                .0;
            return Ok(rmcp::handler::server::wrapper::Json(
                serde_json::to_value(res).map_err(|e| e.to_string())?,
            ));
        }

        let quote =
            args.quote
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|t| anyagents_core::Quote {
                    text: t.to_string(),
                    ..anyagents_core::Quote::default()
                });
        let body = SendRequest {
            // The server sends as this agent whatever this says, and refuses a
            // body that names anyone else.
            sender: Some(format!("agent:{}", self.agent.name)),
            to: args.to,
            text: args.text,
            reply_to: args.reply_to,
            quote,
            media: args.media,
            location: args.location,
            poll: args.poll,
            checklist: args.checklist,
            article: args.article,
            bot: args.bot,
            idempotency_key: args.idempotency_key,
        };
        let closing = body.is_turn_closing_sentinel();
        let response: SendResponse = self
            .client
            .get(
                "/v1/send",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(send_turn_result(
            response, closing,
        )?))
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
                || args.tool_name.is_some());
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
        description = "Unified agent & cross-harness sub-agent manager (`action`: `list` | `inspect` | `spawn` | `create` | `describe` | `redact` | `steer` | `wake` | `pause` | `archive` | `delete` | `rename` | `role` | `harness` | `models` | `usage`, supporting multi-level sub-agent trees). Use `list` (default, with optional `parent`: `\"self\"` | `\"subagents\"` | `\"root\"` and `state`: `\"running\"` | `\"working\"` | `\"paused\"` | `\"archived\"` | `\"all\"`) to inspect agents and sub-agents; use `inspect` (`name`: `\"<agent-or-id>\"`, `limit`: `10`) to view live status, schedules, and recent turns; use `spawn` (`role` defaulting to `\"general\"`, `name`, `repo`: `\"<org>/<repo>\"` | `\"inherit\"`, `branch`, `base`, `workdir`: path | `\"worktree\"` | `\"private\"` | `\"inherit\"`, `description`, `prompt`, `harness`, `model`, `profile`) to launch a sub-agent with an isolated managed Git repository; use `describe` (`name`: `\"self\"` | `\"all\"` | `\"<agent>\"`, `description`: `\"...\"`) to read role descriptions or update a target's `description`/`instructions`/`role` (updating `description`, `instructions`, `role`, or `rename` requires the target's Parent/Ancestor agent or an Admin); use `steer`, `wake`, `pause`, `archive` (auto-reaps clean/pushed managed checkouts while preserving dirty ones), or `delete` to control lifecycle; use `redact` (`name`, `message_id` or `seq`, optional `span`: {`unit`: `bytes` | `chars`, `start`, `end`}, `reason`) for audited erasure with permission `conversation.redact` (ancestors/admin only, quiescent target); or use `usage`, `create`, `rename`, `role`, `harness`, `models`."
    )]
    pub(super) async fn agents(
        &self,
        Parameters(mut args): Parameters<AgentsArgs>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| {
                if args.new_name.is_some() {
                    "rename".to_string()
                } else if args.instructions.is_some()
                    && args.prompt.is_none()
                    && args.harness.is_none()
                    && args.repo.is_none()
                {
                    "describe".to_string()
                } else if args.prompt.is_some()
                    && (args.repo.is_some() || args.workdir.is_some() || args.branch.is_some())
                {
                    "spawn".to_string()
                } else {
                    "list".to_string()
                }
            });
        // `name` names the agent acted on, except where it names a new one.
        if !matches!(action.as_str(), "list" | "spawn" | "create")
            && let Some(raw) = args.name.as_deref()
        {
            args.name = Some(self.resolve_agent_word(raw).await?);
        }
        match action.as_str() {
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
                let mut response = self.client.get_agents_for_state(state_trimmed).await?;
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
                if let Some(parent_filter) = args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    let cleaned = parent_filter
                        .strip_prefix("agent:")
                        .unwrap_or(parent_filter);
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
                            response
                                .agents
                                .retain(|a| a.parent.as_deref() == Some(wid.as_str()));
                        }
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
                )?))
            }
            "spawn" => {
                let spawned = self
                    .spawn(Parameters(SpawnArgs {
                        name: args.name,
                        role: args.role,
                        harness: args.harness,
                        model: args.model,
                        profile: args.profile,
                        repo: args.repo,
                        branch: args.branch,
                        base: args.base,
                        workdir: args.workdir,
                        description: args.description,
                        instructions: args.instructions,
                        scope: args.scope,
                        lifecycle: args.lifecycle,
                        public: args.public,
                        group: args.group,
                        prompt: args.prompt,
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(spawned).map_err(|e| e.to_string())?,
                ))
            }
            "create" => {
                let name = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents create` requires `name`".to_string())?
                    .to_string();
                let harness = args
                    .harness
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("")
                    .to_string();
                let parent_val = match args
                    .parent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    Some(p) if p.eq_ignore_ascii_case("self") => Some(self.agent.id.to_string()),
                    Some(p) => Some(p.to_string()),
                    None => Some("root".to_string()),
                };
                let body = CreateAgentRequest {
                    name,
                    role: args.role,
                    harness: Some(harness),
                    model: args.model,
                    profile: args.profile,
                    repo: args.repo,
                    branch: args.branch,
                    base: args.base,
                    workdir: args.workdir,
                    seat_user: None,
                    seat_uid: None,
                    description: args.description,
                    instructions: args.instructions,
                    parent: parent_val,
                    scope: args.scope,
                    lifecycle: args.lifecycle,
                    public: args.public,
                    group: args.group,
                    prompt: args.prompt,
                };
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
                    text: text.to_string(),
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
            "harness" | "set_harness" => {
                let target = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents harness` requires `name`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let harness = args
                    .harness
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`agents harness` requires `harness`".to_string())?;
                let body = serde_json::json!({
                    "harness": harness,
                    "model": args.model,
                    "profile": args.profile,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/harness"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
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
                let resp: AgentsResponse = self.client.get_agents().await?;
                let view = resp
                    .agents
                    .into_iter()
                    .find(|a| {
                        (clean.eq_ignore_ascii_case("self") && a.id == self.agent.id.as_str())
                            || a.name.eq_ignore_ascii_case(clean)
                            || a.id == clean
                            || a.aliases.iter().any(|al| al.eq_ignore_ascii_case(clean))
                    })
                    .ok_or_else(|| format!("agent `{clean}` not found"))?;
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
                "unknown agents action `{other}`; expected `list`, `inspect`, `spawn`, `create`, `describe`, `steer`, `wake`, `pause`, `archive`, `delete`, `rename`, `role`, `harness`, `transfer`, `telegram_bot`, `channels`, or `models`"
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
        let name = args
            .name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| generate_subagent_name(&self.agent.name));
        let harness = args
            .harness
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("")
            .to_string();
        let body = CreateAgentRequest {
            name,
            role: args.role,
            harness: Some(harness),
            model: args.model,
            profile: args.profile,
            repo: args.repo,
            branch: args.branch,
            base: args.base,
            workdir: args.workdir,
            seat_user: None,
            seat_uid: None,
            description: args.description,
            instructions: args.instructions,
            parent: Some(self.agent.id.to_string()),
            scope: args.scope,
            lifecycle: args.lifecycle,
            public: args.public,
            group: args.group,
            prompt: args.prompt,
        };
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
                    resp.agents
                        .retain(|a| a.parent.as_deref() == Some(wanted_id.as_str()));
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
                let resp: AgentsResponse = self.client.get_agents().await?;
                let view = resp
                    .agents
                    .into_iter()
                    .find(|a| {
                        (clean.eq_ignore_ascii_case("self") && a.id == self.agent.id.as_str())
                            || a.name.eq_ignore_ascii_case(clean)
                            || a.id == clean
                            || a.aliases.iter().any(|al| al.eq_ignore_ascii_case(clean))
                    })
                    .ok_or_else(|| format!("sub-agent `{clean}` not found"))?;
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
                    .spawn(Parameters(SpawnArgs {
                        name: args.agent,
                        role: args.role,
                        harness: args.harness,
                        model: args.model,
                        profile: args.profile,
                        repo: args.repo,
                        branch: args.branch,
                        base: args.base,
                        workdir: args.workdir,
                        description: args.description,
                        instructions: args.instructions,
                        scope: args.scope,
                        lifecycle: args.lifecycle,
                        public: args.public,
                        group: args.group,
                        prompt: args.prompt,
                    }))
                    .await?
                    .0;
                Ok(rmcp::handler::server::wrapper::Json(
                    serde_json::to_value(spawned).map_err(|e| e.to_string())?,
                ))
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
                        text: text.to_string(),
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
            "harness" | "set_harness" => {
                let target = args
                    .agent
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents harness` requires `agent`".to_string())?;
                let clean = target.strip_prefix("agent:").unwrap_or(target);
                let harness = args
                    .harness
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "`subagents harness` requires `harness`".to_string())?;
                let body = serde_json::json!({
                    "harness": harness,
                    "model": args.model,
                    "profile": args.profile,
                });
                let updated: serde_json::Value = self
                    .client
                    .get(
                        &format!("/v1/agents/{clean}/harness"),
                        Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
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
    ) -> Result<rmcp::handler::server::wrapper::Json<AgentView>, String> {
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
                    text: prompt.to_string(),
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
            let response = self.client.get("/v1/groups", None).await?;
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
            sender: Some(format!("agent:{}", self.agent.name)),
            action: args.action,
            target: args.target,
            emoji: args.emoji,
            options: args.options,
            text: args.text,
            bot: args.bot,
        };
        let response = self
            .client
            .get(
                "/v1/interact",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(response))
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
            let updated: serde_json::Value = self
                .client
                .get(
                    &format!("/v1/agents/{resolved_target}/description"),
                    Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
                )
                .await?;
            return Ok(rmcp::handler::server::wrapper::Json(updated));
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
        description = "First-class learned-memory manager (`action`: `\"list\"` | `\"read\"` | `\"write\"` | `\"upsert\"` | `\"delete\"` | `\"search\"`). Inferred automatically when `action` is omitted: `delete: true` -> `\"delete\"`, `content` -> `\"write\"`, `query` -> `\"search\"`, `topic` -> `\"read\"`, else `\"list\"`. Writes `<agent_root>/memory/<topic>.md`, syncs the FTS5 memory index, and rebuilds `MEMORY.md` (<150 lines / 4 KB)."
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

    /// The universal 7-tool surface every agent sees on every harness.
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
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string());
        let is_self_target = target_arg.as_deref().is_none_or(|t| {
            t.eq_ignore_ascii_case("self")
                || t.eq_ignore_ascii_case(&self.agent.name)
                || t == self.agent.id.as_str()
        });
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
                let mut index_text = String::new();
                let mut topics_val = Vec::new();
                let mut turns_val = Vec::new();

                if is_self_target && let Some(ref root) = local_agent_root {
                    if let Ok(topics) = anyagents_core::learned_memory::list_memory_topics(root) {
                        if let Some(lim) = args.limit {
                            topics_val = topics
                                .into_iter()
                                .take(lim.max(1))
                                .filter_map(|t| serde_json::to_value(t).ok())
                                .collect();
                        } else {
                            topics_val = topics
                                .into_iter()
                                .filter_map(|t| serde_json::to_value(t).ok())
                                .collect();
                        }
                    }
                    if let Ok(txt) = std::fs::read_to_string(root.join("memory/MEMORY.md")) {
                        index_text = txt;
                    }
                }

                if topics_val.is_empty() || !is_self_target {
                    let route_target = target_arg.as_deref().unwrap_or(self.agent.name.as_str());
                    let mem_path =
                        format!("/v1/agents/{}/memory", urlencoding_simple(route_target));
                    if let Ok(mem_doc) = self.client.get::<serde_json::Value>(&mem_path, None).await
                    {
                        if let Some(arr) = mem_doc.get("topics").and_then(|v| v.as_array()) {
                            topics_val = match args.limit {
                                Some(lim) => arr.iter().take(lim.max(1)).cloned().collect(),
                                None => arr.clone(),
                            };
                        }
                        if let Some(idx) = mem_doc.get("index").and_then(|v| v.as_str())
                            && !idx.is_empty()
                        {
                            index_text = idx.to_string();
                        }
                    }
                }
                if (index_text.is_empty() || !is_self_target)
                    && let Ok(resp) = self
                        .conversation(Parameters(ConversationArgs {
                            scope: Some("memory".into()),
                            topic: Some("MEMORY".into()),
                            agent: target_arg.clone(),
                            ..ConversationArgs::default()
                        }))
                        .await
                {
                    if let Some(first) = resp.0.turns.first()
                        && index_text.is_empty()
                    {
                        index_text = first.text.clone();
                    }
                    turns_val = resp
                        .0
                        .turns
                        .into_iter()
                        .filter_map(|t| serde_json::to_value(t).ok())
                        .collect();
                }
                if index_text.is_empty() {
                    index_text = format!(
                        "# {} — Learned Memory Index\n- No topic files recorded yet.\n",
                        target_arg.as_deref().unwrap_or(&self.agent.name)
                    );
                }
                let count = topics_val.len();
                Ok(serde_json::json!({
                    "action": "list",
                    "agent": target_arg.unwrap_or_else(|| self.agent.name.clone()),
                    "index": index_text,
                    "topics": topics_val,
                    "count": count,
                    "turns": turns_val,
                }))
            }
            "read" | "get" => {
                let topic =
                    raw_topic.ok_or_else(|| "`memory` read requires `topic`".to_string())?;
                let slug = anyagents_core::learned_memory::sanitize_topic_slug(&topic)
                    .map_err(|e| e.to_string())?;
                let filename = format!("{slug}.md");

                match self
                    .conversation(Parameters(ConversationArgs {
                        scope: Some("memory".into()),
                        topic: Some(slug.clone()),
                        agent: target_arg.clone(),
                        ..ConversationArgs::default()
                    }))
                    .await
                {
                    Ok(resp) => {
                        let content = resp
                            .0
                            .turns
                            .first()
                            .map(|t| t.text.clone())
                            .unwrap_or_default();
                        let (_, description) =
                            anyagents_core::learned_memory::parse_topic_frontmatter(
                                &slug, &content,
                            );
                        Ok(serde_json::json!({
                            "action": "read",
                            "agent": resp.0.agent,
                            "topic": slug,
                            "filename": filename,
                            "description": description,
                            "content": content,
                            "turns": resp.0.turns,
                        }))
                    }
                    Err(err) => {
                        if is_self_target && let Some(ref root) = local_agent_root {
                            let path = root.join("memory").join(&filename);
                            if let Ok(content) = std::fs::read_to_string(&path) {
                                let (_, description) =
                                    anyagents_core::learned_memory::parse_topic_frontmatter(
                                        &slug, &content,
                                    );
                                return Ok(serde_json::json!({
                                    "action": "read",
                                    "agent": self.agent.name,
                                    "topic": slug,
                                    "filename": filename,
                                    "description": description,
                                    "content": content,
                                    "turns": [{
                                        "id": format!("memory:{}:{}", self.agent.id, slug),
                                        "text": content,
                                        "source": "memory",
                                        "direction": "in",
                                        "from": format!("agent:{}", self.agent.name),
                                        "to": format!("agent:{}", self.agent.name),
                                        "reply_to": filename,
                                    }],
                                }));
                            }
                        }
                        Err(err)
                    }
                }
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
                        agent: target_arg.clone(),
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
                        agent: target_arg.clone(),
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
                            "turns": resp.0.turns,
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
                                "turns": hits,
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
                let route_target = target_arg.as_deref().unwrap_or(self.agent.name.as_str());
                let mem_path = format!("/v1/agents/{}/memory", urlencoding_simple(route_target));
                let mem_doc: serde_json::Value = self.client.get(&mem_path, None).await?;
                Ok(serde_json::json!({
                    "action": "status",
                    "agent": mem_doc.get("agent").and_then(|v| v.as_str()).unwrap_or(route_target),
                    "topic_count": mem_doc.get("topic_count").and_then(|v| v.as_u64()).unwrap_or(0),
                    "index_bytes": mem_doc.get("index_bytes").and_then(|v| v.as_u64()).unwrap_or(0),
                    "index_lines": mem_doc.get("index_lines").and_then(|v| v.as_u64()).unwrap_or(0),
                    "last_consolidated_ms": mem_doc.get("last_consolidated_ms").cloned().unwrap_or(serde_json::Value::Null),
                    "consolidated_topics": mem_doc.get("consolidated_topics").cloned().unwrap_or_else(|| serde_json::json!([])),
                }))
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
                    "turns": resp.0.turns,
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
            other => Err(format!(
                "unknown AnyAgents tool `{other}`; expected one of `agents`, `send`, `inbox`, `conversation`, `memory`, `schedules`, `groups`"
            )),
        }
    }
}
