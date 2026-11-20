//! Direct PostgreSQL conversation & full-text search queries.
//!
//! Queries `public.messages` and `public.outbound_messages` directly with
//! pushed-down timestamp, visibility, agent, direction, and GIN full-text
//! (`to_tsvector('simple', body) @@ plainto_tsquery('simple', ...)`) filters,
//! completely bypassing ephemeral local SQLite `rag_chunks` synchronization.

use std::collections::HashSet;

use super::Store;
use super::helpers::agent_by_id;
use super::history::{
    PeerTarget, build_peer_sql, matches_conversation_peer, resolve_inbound_dest,
    visible_agent_ids_sql,
};
use super::index_sync::{cjk_phrase_tokens, cjk_runs, is_cjk_char, tokenize_hybrid_search_tokens};
use super::types::{ConversationTurn, HistorySearchQuery, HistorySort};
use crate::db::{DbBackend, DbExec as _, DbExecExt as _, DbParam, ToDbParam};
use crate::error::Error;
use crate::id::AgentId;

/// Idempotent PostgreSQL GIN full-text & timestamp indexes for direct
/// `public.messages` / `public.outbound_messages` queries (version-neutral so
/// it applies cleanly to both fresh and existing PG deployments without
/// conflicting with pending numbered migrations).
pub(crate) const PG_CONVERSATION_INDEXES_SQL: &str = "\
CREATE INDEX IF NOT EXISTS idx_messages_body_fts \
    ON public.messages USING GIN (to_tsvector('simple', body));\n\
CREATE INDEX IF NOT EXISTS idx_outbound_messages_body_fts \
    ON public.outbound_messages USING GIN (to_tsvector('simple', body));\n\
CREATE INDEX IF NOT EXISTS idx_messages_received_at_desc \
    ON public.messages(received_at_ms DESC, id DESC);\n\
CREATE INDEX IF NOT EXISTS idx_messages_agent_received_desc \
    ON public.messages(agent_id, received_at_ms DESC, seq DESC);\n\
CREATE INDEX IF NOT EXISTS idx_outbound_messages_sent_at_desc \
    ON public.outbound_messages(sent_at_ms DESC, id DESC);\n\
CREATE INDEX IF NOT EXISTS idx_outbound_messages_agent_sent_desc \
    ON public.outbound_messages(agent_id, sent_at_ms DESC, id DESC);\n\
CREATE INDEX IF NOT EXISTS idx_messages_reply_to_pattern \
    ON public.messages(reply_to text_pattern_ops, received_at_ms DESC);\n\
CREATE INDEX IF NOT EXISTS idx_messages_sender_pattern \
    ON public.messages(sender text_pattern_ops, received_at_ms DESC);\n\
CREATE INDEX IF NOT EXISTS idx_outbound_messages_dest_pattern \
    ON public.outbound_messages(destination text_pattern_ops, sent_at_ms DESC);\n";

/// Ensure the PostgreSQL direct-conversation GIN FTS and timestamp indexes exist.
/// Uses a fast `pg_indexes` probe first so warm startups cost ~1ms and never
/// acquire the advisory lock once indexes are already present.
pub(crate) fn ensure_pg_conversation_indexes(
    pool: &sqlx::PgPool,
    direct_url: Option<&str>,
    lock_key: i64,
) -> Result<(), Error> {
    let has_fts: bool = crate::db::block_on_pg(async {
        sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM pg_indexes
                 WHERE schemaname = 'public'
                   AND indexname = 'idx_messages_body_fts'
             ) AND EXISTS(
                 SELECT 1 FROM pg_indexes
                 WHERE schemaname = 'public'
                   AND indexname = 'idx_outbound_messages_body_fts'
             ) AND EXISTS(
                 SELECT 1 FROM pg_indexes
                 WHERE schemaname = 'public'
                   AND indexname = 'idx_outbound_messages_dest_pattern'
             )",
        )
        .fetch_one(pool)
        .await
    })?;
    if has_fts {
        return Ok(());
    }
    let idx_pool = match direct_url {
        Some(url) => crate::db::build_pg_pool(url, Some(2))?,
        None => pool.clone(),
    };
    crate::db::block_on_pg(async move {
        let mut conn = idx_pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await?;
        let res = sqlx::raw_sql(PG_CONVERSATION_INDEXES_SQL)
            .execute(&mut *conn)
            .await;
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await;
        res?;
        Ok::<_, Error>(())
    })
}

/// Return `true` when `query` is an ASCII word/identifier query that can use
/// the PostgreSQL `'simple'` GIN full-text index (`to_tsvector('simple', body)
/// @@ plainto_tsquery('simple', ...)`). Non-ASCII/CJK text or punctuation-heavy
/// substring probes fall back to `ILIKE`.
pub(crate) fn query_uses_pg_fts(query: &str) -> bool {
    let trimmed = query.trim();
    !trimmed.is_empty()
        && trimmed.is_ascii()
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c.is_ascii_whitespace() || c == '_')
        && trimmed.chars().any(|c| c.is_ascii_alphanumeric())
}

fn like_pred(column: &str, op: &str, param: usize) -> String {
    format!("{column} {op} ?{param} ESCAPE '\\'")
}

fn escape_like_pattern(raw: &str) -> String {
    format!(
        "%{}%",
        raw.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}

struct SqlParams {
    params: Vec<DbParam>,
}

impl SqlParams {
    fn new() -> Self {
        Self { params: Vec::new() }
    }

    fn push<T: ToDbParam>(&mut self, val: T) -> usize {
        self.params.push(val.to_db_param());
        self.params.len()
    }
}

/// Build the direct `UNION ALL` SQL for recent conversation across authorized agents.
pub(crate) fn build_direct_conversation_all_sql(
    since: i64,
    fetch_limit: i64,
    ascending: bool,
    visible_agent_ids: Option<&[String]>,
    peer_forms: &[String],
    backend: DbBackend,
) -> (String, Vec<DbParam>) {
    let mut sp = SqlParams::new();
    let p_since = sp.push(since);
    let p_limit = sp.push(fetch_limit);
    let vis_first = sp.params.len() + 1;
    let vis_in = visible_agent_ids_sql("m.agent_id", visible_agent_ids, vis_first);
    let vis_out = visible_agent_ids_sql("o.agent_id", visible_agent_ids, vis_first);
    if let Some(ids) = visible_agent_ids {
        for id in ids {
            sp.push(id.as_str());
        }
    }
    let self_expr = Some("'agent:' || a.name");
    let peer_in = build_peer_sql(
        peer_forms,
        PeerTarget::Messages("m.", self_expr),
        backend,
        &mut sp.params,
    );
    let peer_out = build_peer_sql(
        peer_forms,
        PeerTarget::Outbound("o.", self_expr),
        backend,
        &mut sp.params,
    );
    let dir_kw = if ascending { "ASC" } else { "DESC" };
    let sql = format!(
        "SELECT id, direction, sender, destination, reply_to, body, timestamp_ms, seq, origin_raw
         FROM (
             SELECT * FROM (
                 SELECT m.id, 'in' AS direction, COALESCE(m.sender, 'unknown') AS sender,
                        CASE
                            WHEN m.reply_to LIKE 'telegram:chat:%' OR m.reply_to LIKE 'group:%'
                                THEN m.reply_to
                            ELSE 'agent:' || a.name
                        END AS destination,
                        m.reply_to, m.body, m.received_at_ms AS timestamp_ms, m.seq,
                        m.origin AS origin_raw
                 FROM messages m
                 JOIN agents a ON a.id = m.agent_id
                 WHERE m.received_at_ms > ?{p_since}{vis_in}{peer_in}
                 ORDER BY m.received_at_ms {dir_kw}, m.id {dir_kw}
                 LIMIT ?{p_limit}
             ) AS _in
             UNION ALL
             SELECT * FROM (
                 SELECT o.id, 'out' AS direction, 'agent:' || a.name AS sender,
                        o.destination, o.reply_to, o.body, o.sent_at_ms AS timestamp_ms, o.seq,
                        NULL AS origin_raw
                 FROM outbound_messages o
                 JOIN agents a ON a.id = o.agent_id
                 WHERE o.sent_at_ms > ?{p_since}{vis_out}{peer_out}
                 ORDER BY o.sent_at_ms {dir_kw}, o.id {dir_kw}
                 LIMIT ?{p_limit}
             ) AS _out
         ) AS combined
         ORDER BY timestamp_ms {dir_kw}, id {dir_kw}
         LIMIT ?{p_limit}"
    );
    (sql, sp.params)
}

/// Direct PostgreSQL implementation of [`Store::conversation_all_scoped`].
pub(super) fn pg_conversation_all_scoped(
    store: &Store,
    with_peer: Option<&str>,
    since_ms: Option<i64>,
    limit: Option<usize>,
    visible_agent_ids: Option<&[String]>,
) -> Result<Vec<ConversationTurn>, Error> {
    if visible_agent_ids.is_some_and(|ids| ids.is_empty()) {
        return Ok(Vec::new());
    }
    let conn = store.read_conn();
    let since = since_ms.unwrap_or(0);
    let lim = limit
        .map(|n| i64::try_from(n.max(1)).unwrap_or(i64::MAX))
        .unwrap_or(500);
    let peer_filter = with_peer.map(str::trim).filter(|s| !s.is_empty());
    let peer_forms: Vec<String> = peer_filter
        .map(|peer| store.conversation_peer_forms(peer))
        .unwrap_or_default();
    let fetch_limit = if !peer_forms.is_empty() {
        lim.saturating_mul(2).min(2_000)
    } else {
        lim
    };
    let (sql, params) = build_direct_conversation_all_sql(
        since,
        fetch_limit,
        since_ms.is_some(),
        visible_agent_ids,
        &peer_forms,
        conn.backend(),
    );
    let mut turns = conn.db_query_vec(&sql, &params, |row| {
        let direction: String = row.get(1)?;
        let origin_raw: Option<String> = row.get(8)?;
        let to = resolve_inbound_dest(&direction, row.get(3)?, origin_raw.as_deref());
        Ok(ConversationTurn {
            id: row.get(0)?,
            direction,
            from: row.get(2)?,
            to,
            reply_to: row.get(4)?,
            text: row.get(5)?,
            timestamp_ms: row.get(6)?,
            seq: row.get(7)?,
        })
    })?;
    if !peer_forms.is_empty() {
        turns.retain(|turn| {
            peer_forms
                .iter()
                .any(|p| matches_conversation_peer(turn, p))
        });
    }
    turns.sort_by(|a, b| {
        a.timestamp_ms
            .cmp(&b.timestamp_ms)
            .then_with(|| a.id.cmp(&b.id))
    });
    if let Some(limit) = limit {
        if since_ms.is_none() && turns.len() > limit {
            turns.drain(..turns.len() - limit);
        } else if turns.len() > limit {
            turns.truncate(limit);
        }
    }
    Ok(turns)
}

pub(crate) struct DirectSearchSpec<'a> {
    pub since: i64,
    pub until: i64,
    pub fetch_limit: i64,
    pub ascending: bool,
    pub include_in: bool,
    pub include_out: bool,
    pub include_tools: bool,
    pub tool_name: Option<&'a str>,
    pub agent_id: Option<&'a str>,
    pub agent_name: Option<&'a str>,
    pub sender_like: Option<&'a str>,
    pub from_seq: Option<i64>,
    pub to_seq: Option<i64>,
    pub visible_agent_ids: Option<&'a [String]>,
    pub peer_forms: &'a [String],
    pub queries: &'a [String],
    pub exclude: &'a [String],
    pub backend: DbBackend,
}

/// Search canonical mailbox rows and, when requested, standalone tool summaries.
pub(crate) fn build_direct_search_sql(spec: &DirectSearchSpec<'_>) -> (String, Vec<DbParam>) {
    let mut sp = SqlParams::new();
    let p_since = sp.push(spec.since);
    let p_until = sp.push(spec.until);
    let p_limit = sp.push(spec.fetch_limit);

    let p_agent_id = spec.agent_id.map(|id| sp.push(id));
    let p_agent = if p_agent_id.is_none() {
        spec.agent_name.map(|name| sp.push(name))
    } else {
        None
    };
    let p_sender = spec.sender_like.map(|s| sp.push(s));
    let p_from_seq = spec.from_seq.map(|s| sp.push(s));
    let p_to_seq = spec.to_seq.map(|s| sp.push(s));

    let vis_first = sp.params.len() + 1;
    let vis_in = visible_agent_ids_sql("m.agent_id", spec.visible_agent_ids, vis_first);
    let vis_out = visible_agent_ids_sql("o.agent_id", spec.visible_agent_ids, vis_first);
    if let Some(ids) = spec.visible_agent_ids {
        for id in ids {
            sp.push(id.as_str());
        }
    }

    let p_tool = spec.tool_name.map(|name| sp.push(name));
    let vis_tool = visible_agent_ids_sql("rag_chunks.agent_id", spec.visible_agent_ids, vis_first);
    let like_kw = spec.backend.sql_like_ci();
    let use_fts_engine = spec.backend == DbBackend::Postgres;
    let mut query_preds_in: Vec<String> = Vec::new();
    let mut query_preds_out: Vec<String> = Vec::new();
    let mut query_preds_tool = Vec::new();
    for q in spec.queries {
        let trimmed = q.trim();
        if trimmed.is_empty() {
            continue;
        }
        let runs = cjk_runs(trimmed).collect::<Vec<_>>();
        if use_fts_engine && !runs.is_empty() {
            // Canonical PostgreSQL mailbox rows do not carry CJK token vectors.
            // Search every contiguous run in the full body, even past the
            // projection cap or before a projection is caught up.
            let pats: Vec<_> = runs
                .iter()
                .map(|run| sp.push(escape_like_pattern(run)))
                .collect();
            let ascii = trimmed
                .split(is_cjk_char)
                .filter(|part| query_uses_pg_fts(part))
                .collect::<Vec<_>>()
                .join(" ");
            let ascii_q = (!ascii.trim().is_empty()).then(|| sp.push(ascii.trim()));
            let literal = |col: &str| {
                pats.iter()
                    .map(|p| like_pred(col, like_kw, *p))
                    .collect::<Vec<_>>()
            };
            for (col, preds) in [
                ("m.body", &mut query_preds_in),
                ("o.body", &mut query_preds_out),
            ] {
                let mut and = literal(col);
                if let Some(p) = ascii_q {
                    and.push(format!(
                        "to_tsvector('simple', {col}) @@ plainto_tsquery('simple', ?{p})"
                    ));
                }
                preds.push(format!("({})", and.join(" AND ")));
            }
            if spec.include_tools {
                let mut fallback = literal("rag_chunks.text");
                let mut phrases = Vec::new();
                for run in &runs {
                    let p = sp.push(cjk_phrase_tokens(run).join(" "));
                    phrases.push(format!("phraseto_tsquery('simple', ?{p})"));
                }
                if let Some(p) = ascii_q {
                    phrases.push(format!("plainto_tsquery('simple', ?{p})"));
                    fallback.push(format!(
                        "to_tsvector('simple', rag_chunks.text) @@ plainto_tsquery('simple', ?{p})"
                    ));
                }
                // Older vectors and out-of-prefix summaries can still match
                // against the full standalone body rather than losing recall.
                query_preds_tool.push(format!(
                    "(rag_chunks.search_tsv @@ ({}) OR ({}))",
                    phrases.join(" && "),
                    fallback.join(" AND ")
                ));
            }
        } else if use_fts_engine && query_uses_pg_fts(trimmed) {
            let p_q = sp.push(trimmed);
            query_preds_in.push(format!(
                "to_tsvector('simple', m.body) @@ plainto_tsquery('simple', ?{p_q})"
            ));
            query_preds_out.push(format!(
                "to_tsvector('simple', o.body) @@ plainto_tsquery('simple', ?{p_q})"
            ));
            if spec.include_tools {
                let p_pat = sp.push(escape_like_pattern(trimmed));
                query_preds_tool.push(format!(
                    "(rag_chunks.search_tsv @@ plainto_tsquery('simple', ?{p_q}) OR {})",
                    like_pred("rag_chunks.text", like_kw, p_pat)
                ));
            }
        } else {
            let p_pat = sp.push(escape_like_pattern(trimmed));
            query_preds_in.push(like_pred("m.body", like_kw, p_pat));
            query_preds_out.push(like_pred("o.body", like_kw, p_pat));
            if spec.include_tools {
                query_preds_tool.push(like_pred("rag_chunks.text", like_kw, p_pat));
            }
        }
    }

    let mut excl_preds_in: Vec<String> = Vec::new();
    let mut excl_preds_out: Vec<String> = Vec::new();
    let mut excl_preds_tool = Vec::new();
    for ex in spec.exclude {
        let trimmed = ex.trim();
        if trimmed.is_empty() {
            continue;
        }
        let p_ex = sp.push(escape_like_pattern(trimmed));
        excl_preds_in.push(format!("NOT (m.body {like_kw} ?{p_ex} ESCAPE '\\')"));
        excl_preds_out.push(format!("NOT (o.body {like_kw} ?{p_ex} ESCAPE '\\')"));
        if spec.include_tools {
            excl_preds_tool.push(format!(
                "NOT ({})",
                like_pred("rag_chunks.text", like_kw, p_ex)
            ));
        }
    }

    let dir_kw = if spec.ascending { "ASC" } else { "DESC" };
    let mut arms: Vec<String> = Vec::new();

    if spec.include_in {
        let mut conds = vec![
            format!("m.received_at_ms > ?{p_since}"),
            format!("m.received_at_ms <= ?{p_until}"),
        ];
        if let Some(p) = p_agent_id {
            conds.push(format!("m.agent_id = ?{p}"));
        } else if let Some(p) = p_agent {
            conds.push(format!("LOWER(a.name) = ?{p}"));
        }
        if let Some(p) = p_sender {
            conds.push(format!(
                "LOWER(COALESCE(m.sender, 'unknown')) LIKE ?{p} ESCAPE '\\'"
            ));
        }
        if let Some(p) = p_from_seq {
            conds.push(format!("m.seq >= ?{p}"));
        }
        if let Some(p) = p_to_seq {
            conds.push(format!("m.seq <= ?{p}"));
        }
        if !query_preds_in.is_empty() {
            conds.push(format!("({})", query_preds_in.join(" OR ")));
        }
        conds.extend(excl_preds_in);
        let peer_in = build_peer_sql(
            spec.peer_forms,
            PeerTarget::Messages("m.", Some("'agent:' || a.name")),
            spec.backend,
            &mut sp.params,
        );
        let where_clause = format!("{}{vis_in}{peer_in}", conds.join(" AND "));
        arms.push(format!(
            "SELECT * FROM (
                 SELECT m.id, 'in' AS direction, COALESCE(m.sender, 'unknown') AS sender,
                        CASE
                            WHEN m.reply_to LIKE 'telegram:chat:%' OR m.reply_to LIKE 'group:%'
                                THEN m.reply_to
                            ELSE 'agent:' || a.name
                        END AS destination,
                        m.reply_to, m.body, m.received_at_ms AS timestamp_ms, m.seq, a.name AS agent_name,
                        m.origin AS origin_raw
                 FROM messages m
                 JOIN agents a ON a.id = m.agent_id
                 WHERE {where_clause}
                 ORDER BY m.received_at_ms {dir_kw}, m.id {dir_kw}
                 LIMIT ?{p_limit}
             ) AS _in"
        ));
    }

    if spec.include_out {
        let mut conds = vec![
            format!("o.sent_at_ms > ?{p_since}"),
            format!("o.sent_at_ms <= ?{p_until}"),
        ];
        if let Some(p) = p_agent_id {
            conds.push(format!("o.agent_id = ?{p}"));
        } else if let Some(p) = p_agent {
            conds.push(format!("LOWER(a.name) = ?{p}"));
        }
        if let Some(p) = p_sender {
            conds.push(format!("LOWER('agent:' || a.name) LIKE ?{p} ESCAPE '\\'"));
        }
        if let Some(p) = p_from_seq {
            conds.push(format!("o.seq >= ?{p}"));
        }
        if let Some(p) = p_to_seq {
            conds.push(format!("o.seq <= ?{p}"));
        }
        if !query_preds_out.is_empty() {
            conds.push(format!("({})", query_preds_out.join(" OR ")));
        }
        conds.extend(excl_preds_out);
        let peer_out = build_peer_sql(
            spec.peer_forms,
            PeerTarget::Outbound("o.", Some("'agent:' || a.name")),
            spec.backend,
            &mut sp.params,
        );
        let where_clause = format!("{}{vis_out}{peer_out}", conds.join(" AND "));
        arms.push(format!(
            "SELECT * FROM (
                 SELECT o.id, 'out' AS direction, 'agent:' || a.name AS sender,
                        o.destination, o.reply_to, o.body, o.sent_at_ms AS timestamp_ms, o.seq, a.name AS agent_name,
                        NULL AS origin_raw
                 FROM outbound_messages o
                 JOIN agents a ON a.id = o.agent_id
                 WHERE {where_clause}
                 ORDER BY o.sent_at_ms {dir_kw}, o.id {dir_kw}
                 LIMIT ?{p_limit}
             ) AS _out"
        ));
    }

    if spec.include_tools {
        let mut conds = vec![
            format!("timestamp_ms > ?{p_since}"),
            format!("timestamp_ms <= ?{p_until}"),
            "left(chunk_id, 5) = 'tool_'".into(),
            "kind = 'tool'".into(),
        ];
        if let Some(p) = p_agent_id {
            conds.push(format!("agent_id = ?{p}"));
        } else if let Some(p) = p_agent {
            conds.push(format!("LOWER(agent_name) = ?{p}"));
        }
        if let Some(p) = p_tool {
            conds.push(format!("tool_name = ?{p}"));
        }
        if let Some(p) = p_sender {
            conds.push(like_pred("LOWER(sender)", like_kw, p));
        }
        if let Some(p) = p_from_seq {
            conds.push(format!("seq >= ?{p}"));
        }
        if let Some(p) = p_to_seq {
            conds.push(format!("seq <= ?{p}"));
        }
        if !query_preds_tool.is_empty() {
            conds.push(format!("({})", query_preds_tool.join(" OR ")));
        }
        conds.extend(excl_preds_tool);
        let peer = build_peer_sql(
            spec.peer_forms,
            PeerTarget::RagChunks,
            spec.backend,
            &mut sp.params,
        );
        let where_clause = format!("{}{vis_tool}{peer}", conds.join(" AND "));
        arms.push(format!(
            "SELECT * FROM (
                 SELECT chunk_id AS id, direction, sender, destination, NULL::text AS reply_to,
                        text AS body, timestamp_ms, seq, agent_name, NULL::text AS origin_raw
                 FROM memory_index.rag_chunks
                 WHERE {where_clause}
                 ORDER BY timestamp_ms {dir_kw}, chunk_id {dir_kw}
                 LIMIT ?{p_limit}
             ) AS _tool"
        ));
    }

    let union_body = arms.join("\nUNION ALL\n");
    let sql = format!(
        "SELECT id, direction, sender, destination, reply_to, body, timestamp_ms, seq, agent_name, origin_raw
         FROM ({union_body}) AS combined
         ORDER BY timestamp_ms {dir_kw}, id {dir_kw}
         LIMIT ?{p_limit}"
    );
    (sql, sp.params)
}

/// Direct PostgreSQL implementation of [`Store::search_history`].
pub(super) fn pg_search_history(
    store: &Store,
    req: &HistorySearchQuery,
) -> Result<Vec<ConversationTurn>, Error> {
    if req
        .visible_agent_ids
        .as_deref()
        .is_some_and(|ids| ids.is_empty())
    {
        return Ok(Vec::new());
    }
    let tool_filter = req
        .tool_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase);
    let only_tools = req.only_tools || tool_filter.is_some();
    let conn = store.read_conn();
    let limit = if req.limit == 0 { 500 } else { req.limit };
    let since = req.since_ms.unwrap_or(0);
    let until = req.until_ms.unwrap_or(i64::MAX);

    let (agent_id_filter, agent_name_filter) = match req
        .agent
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all") && *s != "*")
    {
        Some(raw) => {
            let stripped = raw.strip_prefix("agent:").unwrap_or(raw);
            if let Ok(aid) = AgentId::parse(stripped)
                && let Ok(found) = agent_by_id(&conn, &aid)
            {
                (Some(found.id.to_string()), None)
            } else if let Ok(found) = store.agent_by_name(stripped) {
                (Some(found.id.to_string()), None)
            } else {
                (None, Some(stripped.to_lowercase()))
            }
        }
        None => (None, None),
    };

    let dir_filter = req
        .direction
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
        .map(str::to_ascii_lowercase);
    let in_direction = dir_filter.as_deref().is_none_or(|d| d == "in");
    let out_direction = dir_filter.as_deref().is_none_or(|d| d == "out");
    let include_tools =
        conn.backend() == DbBackend::Postgres && out_direction && (req.include_tools || only_tools);
    let include_in = in_direction && !only_tools;
    let include_out = out_direction && !only_tools;
    if !include_in && !include_out && !include_tools {
        return Ok(Vec::new());
    }

    let sender_like = req
        .sender
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| escape_like_pattern(&s.to_lowercase()));

    let mut all_queries: Vec<String> = Vec::new();
    if let Some(q) = req
        .query
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        all_queries.push(q.to_string());
    }
    for q in &req.queries {
        let trimmed = q.trim();
        if !trimmed.is_empty() && !all_queries.iter().any(|existing| existing == trimmed) {
            all_queries.push(trimmed.to_string());
        }
    }

    let exclude: Vec<String> = req
        .exclude
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let peer_filter = req
        .with_peer
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let peer_forms: Vec<String> = peer_filter
        .map(|p| store.conversation_peer_forms(p))
        .unwrap_or_default();

    let fetch_limit = i64::try_from(limit.saturating_mul(2).clamp(40, 1_000)).unwrap_or(1_000);
    let ascending = req.sort == HistorySort::Oldest;

    let (sql, params) = build_direct_search_sql(&DirectSearchSpec {
        since,
        until,
        fetch_limit,
        ascending,
        include_in,
        include_out,
        include_tools,
        tool_name: tool_filter.as_deref(),
        agent_id: agent_id_filter.as_deref(),
        agent_name: agent_name_filter.as_deref(),
        sender_like: sender_like.as_deref(),
        from_seq: req.from_seq,
        to_seq: req.to_seq,
        visible_agent_ids: req.visible_agent_ids.as_deref(),
        peer_forms: &peer_forms,
        queries: &all_queries,
        exclude: &exclude,
        backend: conn.backend(),
    });

    let rows = conn.db_query_vec(&sql, &params, |row| {
        let direction: String = row.get(1)?;
        let origin_raw: Option<String> = row.get(9)?;
        let to = resolve_inbound_dest(&direction, row.get(3)?, origin_raw.as_deref());
        Ok((
            ConversationTurn {
                id: row.get(0)?,
                direction,
                from: row.get(2)?,
                to,
                reply_to: row.get(4)?,
                text: row.get(5)?,
                timestamp_ms: row.get(6)?,
                seq: row.get(7)?,
            },
            row.get::<String>(8)?,
        ))
    })?;

    let queries_lower: Vec<String> = all_queries.iter().map(|q| q.to_lowercase()).collect();
    let query_tokens: Vec<String> = {
        let mut toks = Vec::new();
        for q in &all_queries {
            for t in tokenize_hybrid_search_tokens(q) {
                if !toks.contains(&t) {
                    toks.push(t);
                }
            }
        }
        toks
    };

    let mut scored: Vec<(f64, ConversationTurn, String)> = Vec::new();
    for (rank, (turn, aname)) in rows.into_iter().enumerate() {
        if !peer_forms.is_empty()
            && !peer_forms
                .iter()
                .any(|p| matches_conversation_peer(&turn, p))
        {
            continue;
        }
        let score = if all_queries.is_empty() {
            1.0 / (1.0 + rank as f64)
        } else {
            let text_lower = turn.text.to_lowercase();
            let exact_hits = queries_lower
                .iter()
                .filter(|ql| !ql.is_empty() && text_lower.contains(ql.as_str()))
                .count() as f64;
            let token_hits = if query_tokens.is_empty() {
                0.0
            } else {
                let doc_tokens = tokenize_hybrid_search_tokens(&turn.text);
                query_tokens
                    .iter()
                    .filter(|qt| doc_tokens.contains(qt))
                    .count() as f64
                    / (query_tokens.len() as f64)
            };
            1.0 / (30.0 + rank as f64 + 1.0) + exact_hits * 1.5 + token_hits * 0.8
        };
        scored.push((score, turn, aname));
    }

    match req.sort {
        HistorySort::Relevance => {
            scored.sort_by(|a, b| {
                b.0.total_cmp(&a.0)
                    .then_with(|| b.1.timestamp_ms.cmp(&a.1.timestamp_ms))
                    .then_with(|| b.1.id.cmp(&a.1.id))
            });
        }
        HistorySort::Newest => {
            scored.sort_by(|a, b| {
                b.1.timestamp_ms
                    .cmp(&a.1.timestamp_ms)
                    .then_with(|| b.1.id.cmp(&a.1.id))
            });
        }
        HistorySort::Oldest => {
            scored.sort_by(|a, b| {
                a.1.timestamp_ms
                    .cmp(&b.1.timestamp_ms)
                    .then_with(|| a.1.id.cmp(&b.1.id))
            });
        }
    }

    let mut seen_text_sig: HashSet<String> = HashSet::new();
    let mut hits: Vec<ConversationTurn> = Vec::new();
    for (score, mut turn, aname) in scored {
        let sig = crate::learned_memory::sha256(turn.text.as_bytes());
        if !seen_text_sig.insert(sig) {
            continue;
        }
        let seq = turn.seq;
        let kind = if turn.id.starts_with("tool_") {
            "tool"
        } else {
            "message"
        };
        turn.reply_to = Some(format!(
            "score={score:.4} (pg_direct:agent={aname}:kind={kind}:seq={seq})"
        ));
        hits.push(turn);
        if hits.len() >= limit {
            break;
        }
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Incoming, NewAgent};

    #[test]
    fn query_uses_pg_fts_routes_ascii_words_to_gin_and_cjk_or_punctuation_to_ilike() {
        assert!(query_uses_pg_fts("HOLD"));
        assert!(query_uses_pg_fts("deploy failed 500"));
        assert!(query_uses_pg_fts("turn_complete"));
        assert!(!query_uses_pg_fts(""));
        assert!(!query_uses_pg_fts("   "));
        assert!(!query_uses_pg_fts("___"));
        assert!(!query_uses_pg_fts("测试查询"));
        assert!(!query_uses_pg_fts("crates/anyagents/src/lib.rs"));
        assert!(!query_uses_pg_fts("@alice"));
    }

    #[test]
    fn direct_search_sql_uses_pg_gin_tsquery_for_ascii_and_ilike_for_cjk() {
        let ascii_q = vec!["HOLD".to_string()];
        let (sql_pg, params_pg) = build_direct_search_sql(&DirectSearchSpec {
            since: 0,
            until: i64::MAX,
            fetch_limit: 40,
            ascending: false,
            include_in: true,
            include_out: true,
            include_tools: false,
            tool_name: None,
            agent_id: None,
            agent_name: None,
            sender_like: None,
            from_seq: None,
            to_seq: None,
            visible_agent_ids: None,
            peer_forms: &[],
            queries: &ascii_q,
            exclude: &[],
            backend: DbBackend::Postgres,
        });
        assert!(sql_pg.contains("to_tsvector('simple', m.body) @@ plainto_tsquery('simple',"));
        assert!(sql_pg.contains("to_tsvector('simple', o.body) @@ plainto_tsquery('simple',"));
        assert!(sql_pg.contains("ORDER BY timestamp_ms DESC, id DESC"));
        assert!(sql_pg.contains("LIMIT ?3"));
        assert_eq!(params_pg.len(), 4);

        let cjk_q = vec!["测试".to_string()];
        let (sql_cjk, _) = build_direct_search_sql(&DirectSearchSpec {
            since: 0,
            until: i64::MAX,
            fetch_limit: 40,
            ascending: false,
            include_in: true,
            include_out: false,
            include_tools: false,
            tool_name: None,
            agent_id: Some("agt_01test"),
            agent_name: Some("alpha"),
            sender_like: None,
            from_seq: None,
            to_seq: None,
            visible_agent_ids: None,
            peer_forms: &[],
            queries: &cjk_q,
            exclude: &["noise".to_string()],
            backend: DbBackend::Postgres,
        });
        assert!(sql_cjk.contains("m.agent_id = ?"));
        assert!(!sql_cjk.contains("LOWER(a.name)"));
        assert!(sql_cjk.contains("m.body ILIKE"));
        assert!(!sql_cjk.contains("plainto_tsquery"));
        assert!(!sql_cjk.contains("outbound_messages"));
        assert!(sql_cjk.contains("NOT (m.body ILIKE"));
    }

    #[test]
    fn direct_conversation_and_search_execute_cleanly_without_sync_memory_index() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let a = store.create_agent(NewAgent::new("alpha", "codex")).unwrap();
        let b = store.create_agent(NewAgent::new("beta", "codex")).unwrap();

        let mut m1 = Incoming::new("HOLD order pending review");
        m1.sender = Some("telegram:user:1".into());
        store.receive(&a.id, m1).unwrap();

        store
            .record_outbound(&a.id, "telegram:chat:99", None, "HOLD acknowledged")
            .unwrap();

        let mut m2 = Incoming::new("routine heartbeat ok");
        m2.sender = Some("telegram:user:2".into());
        store.receive(&b.id, m2).unwrap();

        // Direct recent read respects limit and visibility without calling sync_memory_index.
        let recent =
            pg_conversation_all_scoped(&store, None, None, Some(2), Some(&[a.id.to_string()]))
                .unwrap();
        assert_eq!(recent.len(), 2);
        assert!(recent.iter().all(|t| t.text.contains("HOLD")));

        // Direct search filters by query, direction, exclude, and visibility.
        let hits = pg_search_history(
            &store,
            &HistorySearchQuery {
                query: Some("HOLD".into()),
                exclude: vec!["acknowledged".into()],
                visible_agent_ids: Some(vec![a.id.to_string(), b.id.to_string()]),
                limit: 10,
                ..HistorySearchQuery::default()
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].text.contains("HOLD order pending review"));

        // Empty visibility fails closed immediately.
        let empty = pg_conversation_all_scoped(&store, None, None, Some(10), Some(&[])).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn conversation_with_telegram_chat_matches_inbound_origin_topic_and_outbound_under_busy_fleet()
    {
        let dir = crate::test_tempdir();
        let s = Store::open(dir.path().join("state.db")).unwrap();
        let a = s.create_agent(NewAgent::new("alpha", "codex")).unwrap();
        let b = s.create_agent(NewAgent::new("beta", "codex")).unwrap();
        let (chat, topic) = ("telegram:chat:-100123", "telegram:chat:-100123:42");
        let (other, t99) = ("telegram:chat:-1001234", "telegram:chat:-100123:99");
        for (text, rt, cid, tid, mid) in [
            ("tg root", Some(chat), -100123, None, 101),
            ("tg topic 42", Some(topic), -100123, Some(42), 102),
            ("tg origin 42", None, -100123, Some(42), 103),
            ("other -1001234", Some(other), -1001234, None, 201),
            ("topic 99", Some(t99), -100123, Some(99), 202),
        ] {
            let mut m = Incoming::new(text);
            m.sender = Some(format!("telegram:user:777:{mid}"));
            m.reply_to = rt.map(str::to_string);
            let orig = crate::Origin {
                chat_id: Some(cid),
                thread_id: tid,
                message_id: Some(mid),
            };
            m.origin = Some(orig);
            s.receive(&a.id, m).unwrap();
        }
        s.record_outbound(&a.id, chat, None, "tg out root").unwrap();
        let out_t = "telegram:chat:-100123:42:555";
        s.record_outbound(&a.id, out_t, None, "tg out 42").unwrap();
        for i in 0..60 {
            let mut n = Incoming::new(format!("fleet heartbeat {i}"));
            n.sender = Some("agent:beta".into());
            s.receive(&b.id, n).unwrap();
        }
        let tq = HistorySearchQuery {
            query: Some("42".into()),
            with_peer: Some(topic.into()),
            limit: 10,
            ..Default::default()
        };
        let chat_sets = [
            s.conversation(&a.id, Some(chat), None, Some(10)).unwrap(),
            s.conversation_all(Some(chat), None, Some(10)).unwrap(),
            pg_conversation_all_scoped(&s, Some(chat), None, Some(10), None).unwrap(),
        ];
        for turns in chat_sets {
            let txt: Vec<&str> = turns.iter().map(|t| t.text.as_str()).collect();
            assert_eq!(turns.len(), 6, "{txt:?}");
            assert!(!txt.iter().any(|t| t.contains("-1001234")));
        }
        let topic_sets = [
            s.conversation(&a.id, Some(topic), None, Some(10)).unwrap(),
            s.conversation_all(Some(topic), None, Some(10)).unwrap(),
            pg_conversation_all_scoped(&s, Some(topic), None, Some(10), None).unwrap(),
            s.search_history(&tq).unwrap(),
            pg_search_history(&s, &tq).unwrap(),
        ];
        for turns in topic_sets {
            let txt: Vec<&str> = turns.iter().map(|t| t.text.as_str()).collect();
            assert_eq!(turns.len(), 3, "{txt:?}");
            assert!(
                ["tg topic 42", "tg origin 42", "tg out 42"]
                    .iter()
                    .all(|w| txt.contains(w))
            );
        }
        let peers = vec![chat.to_string()];
        let (sql, _) =
            build_direct_conversation_all_sql(0, 40, false, None, &peers, DbBackend::Postgres);
        assert!(
            sql.contains("m.reply_to LIKE ?")
                && sql.contains("o.destination LIKE ?")
                && sql.contains("::jsonb->>'chat_id'")
        );
    }
}
