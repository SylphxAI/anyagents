//! Conversation history reading and scoped hybrid full-text search.

use super::helpers::{agent_by_id, message_from_row_with_cursor_db};
use super::index_sync::tokenize_hybrid_search_tokens;
use super::mailbox::cursor;
use super::types::{ConversationTurn, HistorySearchQuery, HistorySort};
use super::{MESSAGE_COLS, Store};
use crate::db::{DbBackend, DbExec, DbExecExt, DbParam, ToDbParam, build_pg_tsquery};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

impl Store {
    /// Read the complete two-way (`in` + `out`) conversation of `agent_id` across
    /// users, external channels, internal groups, and other agents, ordered
    /// chronologically. Optional `with_peer` filters to a specific user/chat/group/agent.
    pub fn conversation(
        &self,
        agent_id: &AgentId,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        self.conversation_scoped(agent_id, with_peer, since_ms, limit, None)
    }

    /// Read one agent's conversation after applying the caller's authorized
    /// agent set. The optional set is an authorization boundary, not a result
    /// filter: a target outside it is an empty internal result and is rejected
    /// by the API before this method is called.
    pub fn conversation_scoped(
        &self,
        agent_id: &AgentId,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
        visible_agent_ids: Option<&[String]>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        let conn = self.read_conn();
        let agent = agent_by_id(&conn, agent_id)?;
        if let Some(visible) = visible_agent_ids
            && !visible.iter().any(|name| name == agent.id.as_str())
        {
            return Ok(Vec::new());
        }
        let delivered = cursor(&conn, agent_id)?;
        let self_addr = format!("agent:{}", agent.name);
        let since = since_ms.unwrap_or(0);
        let peer_filter = with_peer.map(str::trim).filter(|s| !s.is_empty());
        let mut turns = Vec::new();

        // Fast SQL-pushed LIMIT when no post-query peer filter is active.
        let sql_limit = if peer_filter.is_none() {
            limit.map(|l| i64::try_from(l.max(1)).unwrap_or(i64::MAX))
        } else {
            None
        };

        let mut query_params = db_params![agent_id.as_str(), since];
        if let Some(lim) = sql_limit {
            query_params.push(lim.to_db_param());
        }

        // 1. Inbound messages received by this agent.
        let in_sql = match (sql_limit, since_ms.is_none()) {
            (Some(_), true) => format!(
                "SELECT {MESSAGE_COLS}
                 FROM messages WHERE agent_id = ?1 AND received_at_ms > ?2
                 ORDER BY received_at_ms DESC, seq DESC LIMIT ?3"
            ),
            (Some(_), false) => format!(
                "SELECT {MESSAGE_COLS}
                 FROM messages WHERE agent_id = ?1 AND received_at_ms > ?2
                 ORDER BY received_at_ms ASC, seq ASC LIMIT ?3"
            ),
            (None, _) => format!(
                "SELECT {MESSAGE_COLS}
                 FROM messages WHERE agent_id = ?1 AND received_at_ms > ?2
                 ORDER BY received_at_ms, seq"
            ),
        };
        let inbound_msgs = conn.db_query_vec(&in_sql, &query_params, |row| {
            message_from_row_with_cursor_db(row, delivered)
        })?;
        for msg in inbound_msgs {
            let from = msg.sender.unwrap_or_else(|| "unknown".to_string());
            let to = msg
                .reply_to
                .clone()
                .filter(|r| r.starts_with("telegram:chat:") || r.starts_with("group:"))
                .unwrap_or_else(|| self_addr.clone());
            turns.push(ConversationTurn {
                seq: msg.seq,
                id: msg.id.to_string(),
                direction: "in".to_string(),
                from,
                to,
                reply_to: msg.reply_to,
                text: msg.text,
                timestamp_ms: msg.received_at_ms,
            });
        }

        // 2. Outbound messages sent by this agent.
        let out_sql = match (sql_limit, since_ms.is_none()) {
            (Some(_), true) => {
                "SELECT id, destination, reply_to, body, sent_at_ms, seq
                 FROM outbound_messages WHERE agent_id = ?1 AND sent_at_ms > ?2
                 ORDER BY sent_at_ms DESC, id DESC LIMIT ?3"
            }
            (Some(_), false) => {
                "SELECT id, destination, reply_to, body, sent_at_ms, seq
                 FROM outbound_messages WHERE agent_id = ?1 AND sent_at_ms > ?2
                 ORDER BY sent_at_ms ASC, id ASC LIMIT ?3"
            }
            (None, _) => {
                "SELECT id, destination, reply_to, body, sent_at_ms, seq
                 FROM outbound_messages WHERE agent_id = ?1 AND sent_at_ms > ?2
                 ORDER BY sent_at_ms, id"
            }
        };
        let out_turns = conn.db_query_vec(out_sql, &query_params, |row| {
            Ok(ConversationTurn {
                seq: row.get(5)?,
                id: row.get(0)?,
                direction: "out".to_string(),
                from: self_addr.clone(),
                to: row.get(1)?,
                reply_to: row.get(2)?,
                text: row.get(3)?,
                timestamp_ms: row.get(4)?,
            })
        })?;
        turns.extend(out_turns);

        if let Some(peer) = peer_filter {
            let peers = self.conversation_peer_forms(peer);
            turns.retain(|turn| peers.iter().any(|p| matches_conversation_peer(turn, p)));
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

    /// Read the recent chronological conversation across all agents.
    pub fn conversation_all(
        &self,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        self.conversation_all_scoped(with_peer, since_ms, limit, None)
    }

    /// Read recent turns across an authorized set of agents. `visible_agent_ids`
    /// is pushed into SQLite so a limited caller never relies on a post-query
    /// truncation that could omit an authorized hit behind another agent's
    /// rows.
    pub fn conversation_all_scoped(
        &self,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
        visible_agent_ids: Option<&[String]>,
    ) -> Result<Vec<ConversationTurn>, Error> {
        if self.is_postgres() {
            return super::pg_conversation::pg_conversation_all_scoped(
                self,
                with_peer,
                since_ms,
                limit,
                visible_agent_ids,
            );
        }
        self.sync_memory_index()?;
        let conn = self.read_conn();
        let since = since_ms.unwrap_or(0);
        let lim = limit
            .map(|n| i64::try_from(n).unwrap_or(i64::MAX))
            .unwrap_or(i64::MAX / 8);
        let peer_filter = with_peer.map(str::trim).filter(|s| !s.is_empty());
        let fetch_limit = if peer_filter.is_some() {
            lim.saturating_mul(4)
        } else {
            lim
        };
        let visibility_sql = visible_agent_ids_sql("agent_id", visible_agent_ids, 3);
        let sql = if since_ms.is_some() {
            format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, seq
                 FROM rag_chunks
                 WHERE timestamp_ms > ?1 {visibility_sql}
                 ORDER BY timestamp_ms ASC, chunk_id ASC
                 LIMIT ?2"
            )
        } else {
            format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, seq
                 FROM rag_chunks
                 WHERE timestamp_ms > ?1 {visibility_sql}
                 ORDER BY timestamp_ms DESC, chunk_id DESC
                 LIMIT ?2"
            )
        };
        let all_params = scoped_params(db_params![since, fetch_limit], visible_agent_ids);
        let mut turns = conn.db_query_vec(&sql, &all_params, |row| {
            Ok(ConversationTurn {
                seq: row.get(6)?,
                id: row.get(0)?,
                direction: row.get(1)?,
                from: row.get(2)?,
                to: row.get(3)?,
                reply_to: None,
                text: row.get(4)?,
                timestamp_ms: row.get(5)?,
            })
        })?;
        if let Some(peer) = peer_filter {
            let peers = self.conversation_peer_forms(peer);
            turns.retain(|turn| peers.iter().any(|p| matches_conversation_peer(turn, p)));
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

    /// Zero-Embedding Lossless Lexical + Structured Agentic History Search (`Raw-RAG`).
    ///
    /// Searches `rag_chunks` and `rag_chunks_fts` across `req.agent` (`None` or `"all"` searches all agents)
    /// combining SQLite FTS5 BM25 (hybrid ASCII + CJK bigrams/unigrams), exact phrase/substring boost,
    /// multi-query coverage (`query` + `queries`), negative term filtering (`exclude`), tool history
    /// toggles (`include_tools`, `only_tools`, `tool_name`), range bounds (`since_ms`, `until_ms`,
    /// `from_seq`, `to_seq`), ordering (`Relevance`, `Newest`, `Oldest`), and contiguous episode
    /// context expansion (`radius`).
    pub fn search_history(&self, req: &HistorySearchQuery) -> Result<Vec<ConversationTurn>, Error> {
        if self.is_postgres() {
            return super::pg_conversation::pg_search_history(self, req);
        }
        self.sync_memory_index()?;
        let conn = self.read_conn();
        let limit = if req.limit == 0 {
            (i64::MAX / 16) as usize
        } else {
            req.limit
        };
        let radius = req.radius.min(10);
        let since = req.since_ms.unwrap_or(0);
        let until = req.until_ms.unwrap_or(i64::MAX);

        let agent_name_filter = req
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all") && *s != "*")
            .map(|s| {
                let stripped = s.strip_prefix("agent:").unwrap_or(s);
                if let Ok(aid) = AgentId::parse(stripped)
                    && let Ok(found) = agent_by_id(&conn, &aid)
                {
                    return found.name.to_lowercase();
                }
                stripped.to_lowercase()
            });
        let visible_agent_ids = req.visible_agent_ids.as_deref();
        let peer_filter = req
            .with_peer
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let dir_filter = req
            .direction
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
            .map(str::to_ascii_lowercase);
        let sender_filter = req
            .sender
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase);
        let tool_filter = req
            .tool_name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase);
        let only_tools = req.only_tools || tool_filter.is_some();
        let include_tools = req.include_tools || only_tools;
        let kind_sql_filter: Option<&str> = if only_tools {
            Some("tool")
        } else if !include_tools {
            Some("message")
        } else {
            None
        };

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
        let exclude_lower: Vec<String> = req
            .exclude
            .iter()
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

        let passes_filters =
            |turn: &ConversationTurn, aname: &str, kind: &str, tool_name: &str, seq: i64| -> bool {
                if let Some(ref wanted_agent) = agent_name_filter
                    && !aname.eq_ignore_ascii_case(wanted_agent)
                {
                    return false;
                }
                if only_tools && kind != "tool" {
                    return false;
                }
                if !include_tools && kind == "tool" {
                    return false;
                }
                if let Some(ref wanted_tool) = tool_filter
                    && !tool_name.eq_ignore_ascii_case(wanted_tool)
                    && !turn
                        .text
                        .to_lowercase()
                        .contains(&format!("[tool:{wanted_tool}]"))
                    && !turn
                        .text
                        .to_lowercase()
                        .contains(&format!("[tool_call:{wanted_tool}]"))
                {
                    return false;
                }
                if let Some(ref wanted_dir) = dir_filter
                    && !turn.direction.eq_ignore_ascii_case(wanted_dir)
                {
                    return false;
                }
                if let Some(ref wanted_sender) = sender_filter
                    && !turn.from.to_lowercase().contains(wanted_sender)
                {
                    return false;
                }
                if let Some(from_s) = req.from_seq
                    && seq < from_s
                {
                    return false;
                }
                if let Some(to_s) = req.to_seq
                    && seq > to_s
                {
                    return false;
                }
                if let Some(peer) = peer_filter
                    && !matches_conversation_peer(turn, peer)
                {
                    return false;
                }
                if !exclude_lower.is_empty() {
                    let text_l = turn.text.to_lowercase();
                    if exclude_lower.iter().any(|ex| text_l.contains(ex)) {
                        return false;
                    }
                }
                true
            };

        let mut scores: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        let mut chunk_meta: std::collections::HashMap<String, (ConversationTurn, String, i64)> =
            std::collections::HashMap::new();

        if !all_queries.is_empty() {
            // 1. Lexical FTS5 / PostgreSQL GIN tsvector leg with filter pushdown.
            let fts_limit = i64::try_from((limit * 8).max(120)).unwrap_or(120);
            type FtsChunkRow = (
                String,
                String,
                String,
                String,
                String,
                i64,
                String,
                String,
                String,
                i64,
                f64,
            );
            let fts_rows: Option<Vec<FtsChunkRow>> = match conn.backend() {
                DbBackend::Sqlite => {
                    let fts_query = build_fts5_query_with_filters(
                        &all_queries,
                        agent_name_filter.as_deref(),
                        kind_sql_filter,
                        tool_filter.as_deref(),
                    );
                    let visibility_sql = visible_agent_ids_sql("agent_id", visible_agent_ids, 5);
                    if fts_query.is_empty() {
                        None
                    } else {
                        let sql = format!(
                                "SELECT c.chunk_id, c.direction, c.sender, c.destination,
                                        COALESCE(
                                            (SELECT m.body FROM messages m WHERE m.agent_id = c.agent_id AND m.id = c.chunk_id),
                                            (SELECT o.body FROM outbound_messages o WHERE o.agent_id = c.agent_id AND o.id = c.chunk_id),
                                            c.text
                                        ),
                                        c.timestamp_ms, c.agent_name, c.kind, c.tool_name, c.seq, f.score
                                 FROM (
                                     SELECT chunk_id, bm25(rag_chunks_fts, 0.5, 1.0, 1.0, 1.0, 0.5, 2.0, 3.0, 2.5) as score
                                     FROM rag_chunks_fts
                                     WHERE rag_chunks_fts MATCH ?1
                                       AND chunk_id IN (
                                           SELECT chunk_id FROM rag_chunks WHERE 1=1{visibility_sql}
                                       )
                                     ORDER BY score ASC
                                     LIMIT ?4
                                 ) f
                                 JOIN rag_chunks c ON c.chunk_id = f.chunk_id
                                 WHERE c.timestamp_ms > ?2 AND c.timestamp_ms <= ?3
                                 ORDER BY f.score ASC"
                            );
                        let p = scoped_params(
                            db_params![fts_query, since, until, fts_limit],
                            visible_agent_ids,
                        );
                        conn.db_query_vec(&sql, &p, |r| {
                            Ok((
                                r.get(0)?,
                                r.get(1)?,
                                r.get(2)?,
                                r.get(3)?,
                                r.get(4)?,
                                r.get(5)?,
                                r.get(6)?,
                                r.get(7)?,
                                r.get(8)?,
                                r.get(9)?,
                                r.get::<Option<f64>>(10)?.unwrap_or(0.0),
                            ))
                        })
                        .ok()
                    }
                }
                DbBackend::Postgres => {
                    let pg_query = build_pg_tsquery(&all_queries);
                    let visibility_sql = visible_agent_ids_sql("c.agent_id", visible_agent_ids, 8);
                    pg_query.and_then(|fts_query| {
                            let sql = format!(
                                "SELECT c.chunk_id, c.direction, c.sender, c.destination,
                                        COALESCE(
                                            (SELECT m.body FROM messages m WHERE m.agent_id = c.agent_id AND m.id = c.chunk_id),
                                            (SELECT o.body FROM outbound_messages o WHERE o.agent_id = c.agent_id AND o.id = c.chunk_id),
                                            c.text
                                        ),
                                        c.timestamp_ms, c.agent_name, c.kind, c.tool_name, c.seq,
                                        -ts_rank_cd(c.search_tsv, to_tsquery('simple', ?1))::float8 AS score
                                 FROM memory_index.rag_chunks c
                                 WHERE c.search_tsv @@ to_tsquery('simple', ?1)
                                   AND c.timestamp_ms > ?2 AND c.timestamp_ms <= ?3
                                   AND (?5 IS NULL OR LOWER(c.agent_name) = ?5)
                                   AND (?6 IS NULL OR c.kind = ?6)
                                   AND (?7 IS NULL OR LOWER(c.tool_name) = ?7)
                                   {visibility_sql}
                                 ORDER BY score ASC
                                 LIMIT ?4"
                            );
                            let p = scoped_params(
                                db_params![
                                    fts_query,
                                    since,
                                    until,
                                    fts_limit,
                                    agent_name_filter.as_deref(),
                                    kind_sql_filter,
                                    tool_filter.as_deref(),
                                ],
                                visible_agent_ids,
                            );
                            conn.db_query_vec(&sql, &p, |r| {
                                Ok((
                                    r.get(0)?,
                                    r.get(1)?,
                                    r.get(2)?,
                                    r.get(3)?,
                                    r.get(4)?,
                                    r.get(5)?,
                                    r.get(6)?,
                                    r.get(7)?,
                                    r.get(8)?,
                                    r.get(9)?,
                                    r.get::<Option<f64>>(10)?.unwrap_or(0.0),
                                ))
                            })
                            .ok()
                        })
                }
            };
            if let Some(rows) = fts_rows {
                for (
                    rank,
                    (
                        cid,
                        direction,
                        from,
                        to,
                        text,
                        timestamp_ms,
                        aname,
                        kind,
                        tool_name,
                        seq,
                        bm25_raw,
                    ),
                ) in rows.into_iter().enumerate()
                {
                    let turn = ConversationTurn {
                        seq,
                        id: cid.clone(),
                        direction,
                        from,
                        to,
                        reply_to: Some(format!(
                            "bm25_rank={}:agent={aname}:kind={kind}:seq={seq}",
                            rank + 1
                        )),
                        text,
                        timestamp_ms,
                    };
                    if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                        continue;
                    }
                    let text_lower = turn.text.to_lowercase();
                    let exact_matches = queries_lower
                        .iter()
                        .filter(|ql| !ql.is_empty() && text_lower.contains(ql.as_str()))
                        .count();
                    let exact_boost = (exact_matches as f64) * 1.5;
                    let token_hits = if !query_tokens.is_empty() {
                        let doc_tokens = tokenize_hybrid_search_tokens(&turn.text);
                        query_tokens
                            .iter()
                            .filter(|qt| doc_tokens.contains(qt))
                            .count() as f64
                            / (query_tokens.len() as f64)
                    } else {
                        0.0
                    };
                    let rrf = 2.0 / (30.0 + (rank as f64) + 1.0)
                        + (-bm25_raw).clamp(0.0, 25.0) * 0.06
                        + exact_boost
                        + token_hits * 0.8;
                    *scores.entry(cid.clone()).or_insert(0.0) += rrf;
                    chunk_meta.entry(cid).or_insert((turn, aname, seq));
                }
            }

            // 2. Bounded index-pushed substring fallback (only when FTS5 returned fewer than `limit` hits
            //    or for symbol/punctuation queries that produce no FTS tokens).
            if scores.len() < limit {
                let like_kw = conn.backend().sql_like_ci();
                for q_raw in &all_queries {
                    let q_trim = q_raw.trim();
                    if q_trim.is_empty() {
                        continue;
                    }
                    let like_pat = format!("%{}%", q_trim.replace('%', "\\%").replace('_', "\\_"));
                    let sql = format!(
                        "SELECT chunk_id, direction, sender, destination, text, timestamp_ms,
                                agent_name, kind, tool_name, seq
                         FROM (
                             SELECT chunk_id, direction, sender, destination, text, timestamp_ms,
                                    agent_name, kind, tool_name, seq
                             FROM rag_chunks
                             WHERE timestamp_ms > ?1 AND timestamp_ms <= ?2
                               AND (?5 IS NULL OR LOWER(agent_name) = ?5)
                               AND (?6 IS NULL OR kind = ?6)
                               AND (?7 IS NULL OR seq >= ?7)
                               AND (?8 IS NULL OR seq <= ?8){}
                             ORDER BY timestamp_ms DESC
                             LIMIT 2000
                         ) AS _recent
                         WHERE text {like_kw} ?3 ESCAPE '\\'
                         ORDER BY timestamp_ms DESC
                         LIMIT ?4",
                        visible_agent_ids_sql("agent_id", visible_agent_ids, 9)
                    );
                    let p = scoped_params(
                        db_params![
                            since,
                            until,
                            like_pat,
                            i64::try_from((limit * 4).max(80)).unwrap_or(80),
                            agent_name_filter.as_deref(),
                            kind_sql_filter,
                            req.from_seq,
                            req.to_seq,
                        ],
                        visible_agent_ids,
                    );
                    let lrows = conn.db_query_vec(&sql, &p, |row| {
                        Ok((
                            row.get::<String>(0)?,
                            row.get::<String>(1)?,
                            row.get::<String>(2)?,
                            row.get::<String>(3)?,
                            row.get::<String>(4)?,
                            row.get::<i64>(5)?,
                            row.get::<String>(6)?,
                            row.get::<String>(7)?,
                            row.get::<String>(8)?,
                            row.get::<i64>(9)?,
                        ))
                    })?;
                    let mut rank = 0usize;
                    for (
                        cid,
                        direction,
                        from,
                        to,
                        text,
                        timestamp_ms,
                        aname,
                        kind,
                        tool_name,
                        seq,
                    ) in lrows
                    {
                        let turn = ConversationTurn {
                            seq,
                            id: cid.clone(),
                            direction,
                            from,
                            to,
                            reply_to: Some(format!("exact:agent={aname}:kind={kind}:seq={seq}")),
                            text,
                            timestamp_ms,
                        };
                        if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                            continue;
                        }
                        *scores.entry(cid.clone()).or_insert(0.0) +=
                            1.6 / (30.0 + (rank as f64) + 1.0);
                        chunk_meta.entry(cid).or_insert((turn, aname, seq));
                        rank += 1;
                    }
                }
            }
        } else {
            // Structured Filter / Range / Tool History Browse Mode (no keyword query required)
            // with full B-tree index pushdown on `agent_name`, `kind`, `tool_name`, `direction`, and `seq`.
            let order_sql = if req.sort == HistorySort::Oldest {
                "ORDER BY timestamp_ms ASC, seq ASC, chunk_id ASC"
            } else {
                "ORDER BY timestamp_ms DESC, seq DESC, chunk_id DESC"
            };
            let sql = format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms,
                        agent_name, kind, tool_name, seq
                 FROM rag_chunks
                 WHERE timestamp_ms > ?1 AND timestamp_ms <= ?2
                   AND (?4 IS NULL OR LOWER(agent_name) = ?4)
                   AND (?5 IS NULL OR kind = ?5)
                   AND (?6 IS NULL OR LOWER(direction) = ?6)
                   AND (?7 IS NULL OR seq >= ?7)
                   AND (?8 IS NULL OR seq <= ?8){}
                 {order_sql}
                 LIMIT ?3",
                visible_agent_ids_sql("agent_id", visible_agent_ids, 9)
            );
            let p = scoped_params(
                db_params![
                    since,
                    until,
                    i64::try_from((limit * 8).max(200)).unwrap_or(200),
                    agent_name_filter.as_deref(),
                    kind_sql_filter,
                    dir_filter.as_deref(),
                    req.from_seq,
                    req.to_seq,
                ],
                visible_agent_ids,
            );
            let brows = conn.db_query_vec(&sql, &p, |row| {
                Ok((
                    row.get::<String>(0)?,
                    row.get::<String>(1)?,
                    row.get::<String>(2)?,
                    row.get::<String>(3)?,
                    row.get::<String>(4)?,
                    row.get::<i64>(5)?,
                    row.get::<String>(6)?,
                    row.get::<String>(7)?,
                    row.get::<String>(8)?,
                    row.get::<i64>(9)?,
                ))
            })?;
            let mut rank = 0usize;
            for (cid, direction, from, to, text, timestamp_ms, aname, kind, tool_name, seq) in brows
            {
                let turn = ConversationTurn {
                    seq,
                    id: cid.clone(),
                    direction,
                    from,
                    to,
                    reply_to: Some(format!("filter:agent={aname}:kind={kind}:seq={seq}")),
                    text,
                    timestamp_ms,
                };
                if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                    continue;
                }
                scores.insert(cid.clone(), 1.0 / (1.0 + rank as f64));
                chunk_meta.insert(cid, (turn, aname, seq));
                rank += 1;
            }
        }

        let mut ranked: Vec<(String, f64)> = scores.into_iter().collect();
        match req.sort {
            HistorySort::Relevance => {
                ranked.sort_by(|a, b| {
                    b.1.partial_cmp(&a.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| {
                            let ts_a = chunk_meta.get(&a.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                            let ts_b = chunk_meta.get(&b.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                            ts_b.cmp(&ts_a)
                        })
                });
            }
            HistorySort::Newest => {
                ranked.sort_by(|a, b| {
                    let ts_a = chunk_meta.get(&a.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                    let ts_b = chunk_meta.get(&b.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                    ts_b.cmp(&ts_a).then_with(|| b.0.cmp(&a.0))
                });
            }
            HistorySort::Oldest => {
                ranked.sort_by(|a, b| {
                    let ts_a = chunk_meta.get(&a.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                    let ts_b = chunk_meta.get(&b.0).map(|m| m.0.timestamp_ms).unwrap_or(0);
                    ts_a.cmp(&ts_b).then_with(|| a.0.cmp(&b.0))
                });
            }
        }

        // Deduplicate identical message text across multiple recipient agents in room broadcasts.
        let mut seen_text_sig: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut matched_hits: Vec<(ConversationTurn, String, i64)> = Vec::new();
        for (cid, score) in ranked {
            if let Some((mut turn, aname, seq)) = chunk_meta.remove(&cid) {
                let sig = crate::learned_memory::sha256(turn.text.as_bytes());
                if !seen_text_sig.insert(sig) {
                    continue;
                }
                turn.reply_to = Some(format!(
                    "score={score:.4} ({})",
                    turn.reply_to.unwrap_or_default()
                ));
                matched_hits.push((turn, aname, seq));
                if matched_hits.len() >= limit {
                    break;
                }
            }
        }

        if radius == 0 || matched_hits.is_empty() {
            return Ok(matched_hits.into_iter().map(|(t, _, _)| t).collect());
        }

        // Contiguous episode context expansion (`radius > 0`): attach up to `radius` preceding
        // and `radius` following turns of the same agent around each matched turn.
        let mut out: Vec<ConversationTurn> = Vec::new();
        let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        let rad_i64 = i64::try_from(radius).unwrap_or(2);
        for (hit_turn, aname, _) in matched_hits {
            let hit_ts = hit_turn.timestamp_ms;
            let mut before: Vec<ConversationTurn> = Vec::new();
            let bsql = format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, kind, seq, tool_name
                 FROM rag_chunks
                 WHERE LOWER(agent_name) = LOWER(?1)
                   AND (timestamp_ms < ?2 OR (timestamp_ms = ?2 AND chunk_id != ?3))
                   AND timestamp_ms > ?5 AND timestamp_ms <= ?6{}
                 ORDER BY timestamp_ms DESC, seq DESC
                 LIMIT ?4",
                visible_agent_ids_sql("agent_id", visible_agent_ids, 7),
            );
            let bp = scoped_params(
                db_params![aname, hit_ts, hit_turn.id, rad_i64 * 2, since, until],
                visible_agent_ids,
            );
            if let Ok(brows) = conn.db_query_vec(&bsql, &bp, |r| {
                Ok((
                    r.get::<String>(0)?,
                    r.get::<String>(1)?,
                    r.get::<String>(2)?,
                    r.get::<String>(3)?,
                    r.get::<String>(4)?,
                    r.get::<i64>(5)?,
                    r.get::<String>(6)?,
                    r.get::<i64>(7)?,
                    r.get::<String>(8)?,
                ))
            }) {
                for (cid, direction, from, to, text, timestamp_ms, kind, seq, tool_name) in brows {
                    if only_tools && kind != "tool" {
                        continue;
                    }
                    if !include_tools && kind == "tool" {
                        continue;
                    }
                    let turn = ConversationTurn {
                        seq,
                        id: cid,
                        direction,
                        from,
                        to,
                        reply_to: Some(format!(
                            "context:before={}:agent={aname}:seq={seq}",
                            hit_turn.id
                        )),
                        text,
                        timestamp_ms,
                    };
                    if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                        continue;
                    }
                    before.push(turn);
                    if before.len() >= radius {
                        break;
                    }
                }
            }
            before.reverse();
            for b in before {
                if seen_ids.insert(b.id.clone()) {
                    out.push(b);
                }
            }
            let hit_id = hit_turn.id.clone();
            if seen_ids.insert(hit_id.clone()) {
                out.push(hit_turn);
            }
            let asql = format!(
                "SELECT chunk_id, direction, sender, destination, text, timestamp_ms, kind, seq, tool_name
                 FROM rag_chunks
                 WHERE LOWER(agent_name) = LOWER(?1)
                   AND timestamp_ms >= ?2
                   AND chunk_id != ?3
                   AND timestamp_ms > ?5 AND timestamp_ms <= ?6{}
                 ORDER BY timestamp_ms ASC, seq ASC
                 LIMIT ?4",
                visible_agent_ids_sql("agent_id", visible_agent_ids, 7),
            );
            let ap = scoped_params(
                db_params![aname, hit_ts, hit_id, rad_i64 * 2, since, until],
                visible_agent_ids,
            );
            if let Ok(arows) = conn.db_query_vec(&asql, &ap, |r| {
                Ok((
                    r.get::<String>(0)?,
                    r.get::<String>(1)?,
                    r.get::<String>(2)?,
                    r.get::<String>(3)?,
                    r.get::<String>(4)?,
                    r.get::<i64>(5)?,
                    r.get::<String>(6)?,
                    r.get::<i64>(7)?,
                    r.get::<String>(8)?,
                ))
            }) {
                let mut added_after = 0usize;
                for (cid, direction, from, to, text, timestamp_ms, kind, seq, tool_name) in arows {
                    if only_tools && kind != "tool" {
                        continue;
                    }
                    if !include_tools && kind == "tool" {
                        continue;
                    }
                    let turn = ConversationTurn {
                        seq,
                        id: cid,
                        direction,
                        from,
                        to,
                        reply_to: Some(format!("context:after={hit_id}:agent={aname}:seq={seq}")),
                        text,
                        timestamp_ms,
                    };
                    if !passes_filters(&turn, &aname, &kind, &tool_name, seq) {
                        continue;
                    }
                    if seen_ids.insert(turn.id.clone()) {
                        out.push(turn);
                        added_after += 1;
                        if added_after >= radius {
                            break;
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Lexical + structured search over `rag_chunks` across `agent_filter` (`None` or `"all"` searches all agents).
    pub fn search_conversation(
        &self,
        agent_filter: Option<&str>,
        with_peer: Option<&str>,
        since_ms: Option<i64>,
        limit: Option<usize>,
        query_text: &str,
    ) -> Result<Vec<ConversationTurn>, Error> {
        self.search_history(&HistorySearchQuery {
            query: Some(query_text.to_string()),
            agent: agent_filter.map(str::to_string),
            with_peer: with_peer.map(str::to_string),
            since_ms,
            limit: limit.unwrap_or(20),
            ..HistorySearchQuery::default()
        })
    }
}

#[allow(dead_code)]
fn build_fts5_query_with_filters(
    queries: &[String],
    agent_name: Option<&str>,
    kind: Option<&str>,
    tool_name: Option<&str>,
) -> String {
    let mut terms = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for raw in queries {
        for tok in tokenize_hybrid_search_tokens(raw) {
            let clean = tok.replace('"', "");
            if !clean.is_empty() && seen.insert(clean.clone()) {
                terms.push(format!("\"{clean}\""));
            }
        }
    }
    if terms.is_empty() {
        return String::new();
    }
    let text_expr = if terms.len() == 1 {
        terms.remove(0)
    } else {
        format!("({})", terms.join(" OR "))
    };
    let mut clauses = vec![text_expr];
    if let Some(a) = agent_name.map(str::trim).filter(|s| !s.is_empty()) {
        let clean = a.replace('"', "");
        if !clean.is_empty() {
            clauses.push(format!("agent_name : \"{clean}\""));
        }
    }
    if let Some(k) = kind.map(str::trim).filter(|s| !s.is_empty()) {
        let clean = k.replace('"', "");
        if !clean.is_empty() {
            clauses.push(format!("kind : \"{clean}\""));
        }
    }
    if let Some(t) = tool_name.map(str::trim).filter(|s| !s.is_empty()) {
        let clean = t.replace('"', "").to_ascii_lowercase();
        if !clean.is_empty() {
            clauses.push(format!("tool_name : \"{clean}\""));
        }
    }
    clauses.join(" AND ")
}

/// SQL fragment for the visibility predicate shared by recent and Raw-RAG
/// reads. Immutable IDs are exact matches; an empty set fails closed.
pub(super) fn visible_agent_ids_sql(
    column: &str,
    visible_agent_ids: Option<&[String]>,
    first_param: usize,
) -> String {
    let Some(agents) = visible_agent_ids else {
        return String::new();
    };
    if agents.is_empty() {
        return " AND 1 = 0".to_string();
    }
    let placeholders = (0..agents.len())
        .map(|offset| format!("?{}", first_param + offset))
        .collect::<Vec<_>>()
        .join(", ");
    format!(" AND {column} IN ({placeholders})")
}

fn scoped_params(mut bind: Vec<DbParam>, visible_agent_ids: Option<&[String]>) -> Vec<DbParam> {
    if let Some(agents) = visible_agent_ids {
        bind.extend(agents.iter().map(ToDbParam::to_db_param));
    }
    bind
}

impl Store {
    /// A peer as written, plus the name and id of the agent it names: a
    /// message records its sender by id and its recipient as addressed, so
    /// either form alone misses half an exchange.
    pub(super) fn conversation_peer_forms(&self, peer: &str) -> Vec<String> {
        let mut forms = vec![peer.to_string()];
        let bare = peer.strip_prefix("agent:").unwrap_or(peer);
        if !bare.contains(':')
            && let Ok(agent) = self.agent_by_name(bare)
        {
            for form in [agent.name, agent.id.to_string()] {
                if !forms.contains(&form) {
                    forms.push(form);
                }
            }
        }
        forms
    }
}

pub(super) fn matches_conversation_peer(turn: &ConversationTurn, peer: &str) -> bool {
    let bare = peer
        .strip_prefix("agent:")
        .or_else(|| peer.strip_prefix("group:"))
        .or_else(|| peer.strip_prefix("telegram:chat:"))
        .or_else(|| peer.strip_prefix("telegram:user:"))
        .unwrap_or(peer);
    let fields = [
        Some(turn.from.as_str()),
        Some(turn.to.as_str()),
        turn.reply_to.as_deref(),
    ];
    fields.into_iter().flatten().any(|field| {
        field == peer
            || field == bare
            || field.starts_with(&format!("{peer}:"))
            || field.ends_with(&format!(":{bare}"))
            || field.contains(&format!(":{bare}:"))
    })
}
