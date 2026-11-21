//! Live append-only raw turn transcript event log (`turn_transcript_events`).
//!
//! Stores unmasked, lossless step-by-step harness transcript events mid-turn
//! (thinking blocks, tool calls, tool results, assistant text, user inputs)
//! across both SQLite and PostgreSQL. Visibility is strictly enforced at query
//! boundaries (`self`, `descendants`, `Operator`); secret masking belongs only
//! at external egress boundaries, never destructively on raw transcript logs.

use serde::{Deserialize, Serialize};

use super::Store;
use super::helpers::agent_by_id;
use super::history::visible_agent_ids_sql;
use super::index_sync::{cjk_runs, is_cjk_char, tokenize_hybrid_search_tokens};
use super::pg_conversation::query_uses_pg_fts;
use super::types::{ConversationTurn, HistorySearchQuery, HistorySort};
use crate::db::{DbBackend, DbExec as _, DbExecExt as _, DbParam, ToDbParam};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;
use crate::stateless::sanitize_pg_text;

/// Canonical roles permitted in `turn_transcript_events.role`.
pub const VALID_TRANSCRIPT_EVENT_ROLES: &[&str] = &[
    "user",
    "assistant",
    "thinking",
    "tool_call",
    "tool_result",
    "system",
    "raw",
];

/// Normalize an incoming role label into one of the seven canonical roles.
pub fn normalize_transcript_event_role(raw: &str) -> &'static str {
    match raw.trim().to_ascii_lowercase().as_str() {
        "user" | "human" | "user_input" => "user",
        "assistant" | "model" | "agent" | "planner_response" => "assistant",
        "thinking" | "reasoning" | "thought" => "thinking",
        "tool_call" | "tool_use" | "function_call" | "custom_tool_call" | "tool" => "tool_call",
        "tool_result" | "function_call_output" | "custom_tool_call_output" => "tool_result",
        "system" | "developer" | "error" | "status" => "system",
        _ => "raw",
    }
}

/// One raw turn transcript event to append idempotently into `turn_transcript_events`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTurnTranscriptEvent {
    pub agent_id: AgentId,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub harness: String,
    pub source: String,
    pub seq: i64,
    pub timestamp_ms: i64,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// Unmasked searchable text (thinking, assistant text, tool call/result summary).
    pub text: String,
    /// Lossless unmasked raw entry JSON.
    pub raw_json: String,
}

/// One persisted row from `turn_transcript_events`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnTranscriptEvent {
    pub rowid: i64,
    pub agent_id: AgentId,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub harness: String,
    pub source: String,
    pub seq: i64,
    pub timestamp_ms: i64,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    pub text: String,
    pub raw_json: String,
}

/// Ranked search hit from `turn_transcript_events`, preserving full metadata for API callers.
#[derive(Clone, Debug, PartialEq)]
pub struct TurnTranscriptSearchHit {
    pub score: f64,
    pub agent_name: String,
    pub event: TurnTranscriptEvent,
}

impl TurnTranscriptSearchHit {
    /// Convert this hit into the canonical [`ConversationTurn`] projection.
    pub fn into_conversation_turn(self) -> ConversationTurn {
        let direction = if self.event.role == "user" {
            "in".to_string()
        } else {
            "out".to_string()
        };
        let from = if self.event.role == "user" {
            "user".to_string()
        } else {
            format!("agent:{}", self.agent_name)
        };
        let to = if self.event.role == "user" {
            format!("agent:{}", self.agent_name)
        } else {
            format!("turn:{}", self.event.turn_id)
        };
        let mut meta = format!(
            "score={:.4} (transcript_event:agent={}:turn={}:role={}:seq={}",
            self.score, self.agent_name, self.event.turn_id, self.event.role, self.event.seq
        );
        if let Some(ref tool) = self.event.tool_name {
            meta.push_str(":tool=");
            meta.push_str(tool);
        }
        meta.push(')');
        ConversationTurn {
            seq: self.event.seq,
            id: format!("tte_{}", self.event.rowid),
            direction,
            from,
            to,
            reply_to: Some(meta),
            text: self.event.text,
            timestamp_ms: self.event.timestamp_ms,
        }
    }
}

fn events_table(backend: DbBackend) -> &'static str {
    match backend {
        DbBackend::Sqlite => "main.turn_transcript_events",
        DbBackend::Postgres => "public.turn_transcript_events",
    }
}

fn clean_opt_meta(val: Option<&str>) -> Option<String> {
    val.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn escape_like_pattern(raw: &str) -> String {
    format!(
        "%{}%",
        raw.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}

fn like_pred(column: &str, op: &str, param: usize) -> String {
    format!("{column} {op} ?{param} ESCAPE '\\'")
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

pub(crate) struct TranscriptEventSearchSpec<'a> {
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub fetch_limit: i64,
    pub ascending: bool,
    pub agent_id: Option<&'a str>,
    pub agent_name: Option<&'a str>,
    pub turn_id: Option<&'a str>,
    pub role: Option<&'a str>,
    pub include_tools: bool,
    pub only_tools: bool,
    pub tool_name: Option<&'a str>,
    pub direction: Option<&'a str>,
    pub sender_like: Option<&'a str>,
    pub from_seq: Option<i64>,
    pub to_seq: Option<i64>,
    pub visible_agent_ids: Option<&'a [String]>,
    pub queries: &'a [String],
    pub exclude: &'a [String],
    pub backend: DbBackend,
}

/// Build dual-backend SQLite/PostgreSQL search query over `turn_transcript_events`.
pub(crate) fn build_transcript_event_search_sql(
    spec: &TranscriptEventSearchSpec<'_>,
) -> (String, Vec<DbParam>) {
    let mut sp = SqlParams::new();
    let p_limit = sp.push(spec.fetch_limit);

    let mut conds: Vec<String> = Vec::new();
    if let Some(since) = spec.since_ms {
        let p = sp.push(since);
        conds.push(format!("e.timestamp_ms > ?{p}"));
    }
    if let Some(until) = spec.until_ms {
        let p = sp.push(until);
        conds.push(format!("e.timestamp_ms <= ?{p}"));
    }
    if let Some(aid) = spec.agent_id {
        let p = sp.push(aid);
        conds.push(format!("e.agent_id = ?{p}"));
    } else if let Some(aname) = spec.agent_name {
        let p = sp.push(aname);
        conds.push(format!("LOWER(a.name) = LOWER(?{p})"));
    }
    if let Some(tid) = spec.turn_id {
        let p = sp.push(tid);
        conds.push(format!("e.turn_id = ?{p}"));
    }
    if let Some(role) = spec.role {
        let p = sp.push(role);
        conds.push(format!("e.role = ?{p}"));
    }
    if spec.only_tools {
        conds.push("e.role IN ('tool_call', 'tool_result')".to_string());
    } else if !spec.include_tools {
        conds.push("e.role NOT IN ('tool_call', 'tool_result')".to_string());
    }
    if let Some(tname) = spec.tool_name {
        let p = sp.push(tname);
        conds.push(format!("LOWER(COALESCE(e.tool_name, '')) = LOWER(?{p})"));
    }
    if let Some(dir) = spec.direction {
        match dir {
            "in" => conds.push("e.role = 'user'".to_string()),
            "out" => conds.push("e.role != 'user'".to_string()),
            _ => {}
        }
    }
    if let Some(sender) = spec.sender_like {
        let p = sp.push(sender);
        let like_kw = spec.backend.sql_like_ci();
        conds.push(format!(
            "(CASE WHEN e.role = 'user' THEN 'user' ELSE 'agent:' || a.name END) {like_kw} ?{p} ESCAPE '\\'"
        ));
    }
    if let Some(fs) = spec.from_seq {
        let p = sp.push(fs);
        conds.push(format!("e.seq >= ?{p}"));
    }
    if let Some(ts) = spec.to_seq {
        let p = sp.push(ts);
        conds.push(format!("e.seq <= ?{p}"));
    }

    let vis_first = sp.params.len() + 1;
    let vis_sql = visible_agent_ids_sql("e.agent_id", spec.visible_agent_ids, vis_first);
    if let Some(ids) = spec.visible_agent_ids {
        for id in ids {
            sp.push(id.as_str());
        }
    }

    let like_kw = spec.backend.sql_like_ci();
    let use_pg_fts = spec.backend == DbBackend::Postgres;
    let mut query_preds: Vec<String> = Vec::new();
    for q in spec.queries {
        let trimmed = q.trim();
        if trimmed.is_empty() {
            continue;
        }
        let runs = cjk_runs(trimmed).collect::<Vec<_>>();
        if use_pg_fts && !runs.is_empty() {
            let mut and_parts: Vec<String> = runs
                .iter()
                .map(|run| {
                    let p = sp.push(escape_like_pattern(run));
                    like_pred("e.text", like_kw, p)
                })
                .collect();
            let ascii = trimmed
                .split(is_cjk_char)
                .filter(|part| query_uses_pg_fts(part))
                .collect::<Vec<_>>()
                .join(" ");
            if !ascii.trim().is_empty() {
                let p = sp.push(ascii.trim());
                and_parts.push(format!(
                    "to_tsvector('simple', e.text) @@ plainto_tsquery('simple', ?{p})"
                ));
            }
            query_preds.push(format!("({})", and_parts.join(" AND ")));
        } else if use_pg_fts && query_uses_pg_fts(trimmed) {
            let p_q = sp.push(trimmed);
            query_preds.push(format!(
                "to_tsvector('simple', e.text) @@ plainto_tsquery('simple', ?{p_q})"
            ));
        } else {
            let p_pat = sp.push(escape_like_pattern(trimmed));
            query_preds.push(format!(
                "({} OR {})",
                like_pred("e.text", like_kw, p_pat),
                like_pred("COALESCE(e.tool_name, '')", like_kw, p_pat),
            ));
        }
    }
    if !query_preds.is_empty() {
        conds.push(format!("({})", query_preds.join(" OR ")));
    }

    for ex in spec.exclude {
        let trimmed = ex.trim();
        if trimmed.is_empty() {
            continue;
        }
        let p_ex = sp.push(escape_like_pattern(trimmed));
        conds.push(format!("NOT ({})", like_pred("e.text", like_kw, p_ex)));
    }

    let where_body = if conds.is_empty() {
        "1=1".to_string()
    } else {
        conds.join(" AND ")
    };
    let table = events_table(spec.backend);
    let dir_kw = if spec.ascending { "ASC" } else { "DESC" };
    let sql = format!(
        "SELECT e.rowid, e.agent_id, e.turn_id, e.session_id, e.harness, e.source, e.seq,
                e.timestamp_ms, e.role, e.tool_name, e.tool_call_id, e.text, e.raw_json,
                a.name AS agent_name
         FROM {table} e
         JOIN agents a ON a.id = e.agent_id
         WHERE {where_body}{vis_sql}
         ORDER BY e.timestamp_ms {dir_kw}, e.seq {dir_kw}, e.rowid {dir_kw}
         LIMIT ?{p_limit}"
    );
    (sql, sp.params)
}

impl Store {
    /// Idempotently append raw unmasked turn transcript events into
    /// `turn_transcript_events`. Duplicate `(agent_id, source, seq)` tuples are
    /// ignored without error. Returns the number of newly inserted rows.
    pub fn append_turn_transcript_events(
        &self,
        events: &[NewTurnTranscriptEvent],
    ) -> Result<usize, Error> {
        if events.is_empty() {
            return Ok(0);
        }
        for event in events {
            if event.turn_id.trim().is_empty() {
                return Err(Error::Message(
                    "turn transcript event requires non-empty turn_id".into(),
                ));
            }
            if event.harness.trim().is_empty() {
                return Err(Error::Message(
                    "turn transcript event requires non-empty harness".into(),
                ));
            }
            if event.source.trim().is_empty() {
                return Err(Error::Message(
                    "turn transcript event requires non-empty source".into(),
                ));
            }
        }
        self.write_tx(|tx| {
            let backend = tx.backend();
            let table = events_table(backend);
            let sql = format!(
                "INSERT INTO {table} (
                    agent_id, turn_id, session_id, harness, source, seq,
                    timestamp_ms, role, tool_name, tool_call_id, text, raw_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT (agent_id, source, seq) DO NOTHING"
            );
            let mut inserted = 0usize;
            for event in events {
                let role = normalize_transcript_event_role(&event.role);
                let session_id = clean_opt_meta(event.session_id.as_deref());
                let tool_name = clean_opt_meta(event.tool_name.as_deref());
                let tool_call_id = clean_opt_meta(event.tool_call_id.as_deref());
                let (text, raw_json) = if backend == DbBackend::Postgres {
                    (
                        sanitize_pg_text(&event.text),
                        sanitize_pg_text(&event.raw_json),
                    )
                } else {
                    (event.text.clone(), event.raw_json.clone())
                };
                let changed = tx.db_execute(
                    &sql,
                    &db_params![
                        &event.agent_id,
                        event.turn_id.trim(),
                        session_id.as_deref(),
                        event.harness.trim(),
                        event.source.trim(),
                        event.seq,
                        event.timestamp_ms,
                        role,
                        tool_name.as_deref(),
                        tool_call_id.as_deref(),
                        text.as_str(),
                        raw_json.as_str(),
                    ],
                )?;
                inserted = inserted.saturating_add(changed);
            }
            Ok(inserted)
        })
    }

    /// Fetch ordered raw transcript events for a single `(agent_id, turn_id)`
    /// (in-flight or completed), optionally resuming strictly after `after_seq`.
    pub fn list_turn_transcript_events(
        &self,
        agent_id: &AgentId,
        turn_id: &str,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<TurnTranscriptEvent>, Error> {
        let turn_id = turn_id.trim();
        if turn_id.is_empty() {
            return Ok(Vec::new());
        }
        let lim = i64::try_from(if limit == 0 { 500 } else { limit }).unwrap_or(500);
        let conn = self.read_conn();
        let table = events_table(conn.backend());
        let (sql, params) = match after_seq {
            Some(after) => (
                format!(
                    "SELECT rowid, agent_id, turn_id, session_id, harness, source, seq,
                            timestamp_ms, role, tool_name, tool_call_id, text, raw_json
                     FROM {table}
                     WHERE agent_id = ?1 AND turn_id = ?2 AND seq > ?3
                     ORDER BY seq ASC, rowid ASC
                     LIMIT ?4"
                ),
                db_params![agent_id, turn_id, after, lim],
            ),
            None => (
                format!(
                    "SELECT rowid, agent_id, turn_id, session_id, harness, source, seq,
                            timestamp_ms, role, tool_name, tool_call_id, text, raw_json
                     FROM {table}
                     WHERE agent_id = ?1 AND turn_id = ?2
                     ORDER BY seq ASC, rowid ASC
                     LIMIT ?3"
                ),
                db_params![agent_id, turn_id, lim],
            ),
        };
        conn.db_query_vec(&sql, &params, |row| {
            Ok(TurnTranscriptEvent {
                rowid: row.get(0)?,
                agent_id: row.get(1)?,
                turn_id: row.get(2)?,
                session_id: row.get(3)?,
                harness: row.get(4)?,
                source: row.get(5)?,
                seq: row.get(6)?,
                timestamp_ms: row.get(7)?,
                role: row.get(8)?,
                tool_name: row.get(9)?,
                tool_call_id: row.get(10)?,
                text: row.get(11)?,
                raw_json: row.get(12)?,
            })
        })
    }

    /// Search `turn_transcript_events` with strict `visible_agent_ids` isolation
    /// and return ranked hits preserving full role/tool/turn metadata.
    pub fn search_turn_transcript_event_hits(
        &self,
        req: &HistorySearchQuery,
    ) -> Result<Vec<TurnTranscriptSearchHit>, Error> {
        if req
            .visible_agent_ids
            .as_deref()
            .is_some_and(|ids| ids.is_empty())
        {
            return Ok(Vec::new());
        }
        // Raw turn transcript events are internal model/tool steps rather than
        // peer-addressed mailbox messages. Only match when no peer filter is set
        // (or when the caller explicitly scoped to a specific turn_id).
        if req
            .with_peer
            .as_deref()
            .map(str::trim)
            .is_some_and(|s| !s.is_empty())
            && req
                .turn_id
                .as_deref()
                .map(str::trim)
                .is_none_or(str::is_empty)
        {
            return Ok(Vec::new());
        }

        let conn = self.read_conn();
        let limit = if req.limit == 0 { 500 } else { req.limit };
        let fetch_limit = i64::try_from(limit.saturating_mul(2).clamp(40, 1_000)).unwrap_or(1_000);

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
                } else if let Ok(found) = self.agent_by_name(stripped) {
                    (Some(found.id.to_string()), None)
                } else {
                    (None, Some(stripped.to_lowercase()))
                }
            }
            None => (None, None),
        };

        let turn_id_filter = req
            .turn_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let role_filter = req
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(normalize_transcript_event_role);
        let tool_filter = req
            .tool_name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase);
        let only_tools = req.only_tools || tool_filter.is_some();
        let dir_filter = req
            .direction
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("all"))
            .map(str::to_ascii_lowercase);
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

        // When listing a specific turn without explicit sort/query, default to chronological.
        let ascending = req.sort == HistorySort::Oldest
            || (turn_id_filter.is_some()
                && all_queries.is_empty()
                && req.sort == HistorySort::Relevance);

        let (sql, params) = build_transcript_event_search_sql(&TranscriptEventSearchSpec {
            since_ms: req.since_ms,
            until_ms: req.until_ms,
            fetch_limit,
            ascending,
            agent_id: agent_id_filter.as_deref(),
            agent_name: agent_name_filter.as_deref(),
            turn_id: turn_id_filter,
            role: role_filter,
            include_tools: req.include_tools || only_tools,
            only_tools,
            tool_name: tool_filter.as_deref(),
            direction: dir_filter.as_deref(),
            sender_like: sender_like.as_deref(),
            from_seq: req.from_seq,
            to_seq: req.to_seq,
            visible_agent_ids: req.visible_agent_ids.as_deref(),
            queries: &all_queries,
            exclude: &exclude,
            backend: conn.backend(),
        });

        let rows = conn.db_query_vec(&sql, &params, |row| {
            Ok((
                TurnTranscriptEvent {
                    rowid: row.get(0)?,
                    agent_id: row.get(1)?,
                    turn_id: row.get(2)?,
                    session_id: row.get(3)?,
                    harness: row.get(4)?,
                    source: row.get(5)?,
                    seq: row.get(6)?,
                    timestamp_ms: row.get(7)?,
                    role: row.get(8)?,
                    tool_name: row.get(9)?,
                    tool_call_id: row.get(10)?,
                    text: row.get(11)?,
                    raw_json: row.get(12)?,
                },
                row.get::<String>(13)?,
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

        let mut hits: Vec<TurnTranscriptSearchHit> = rows
            .into_iter()
            .enumerate()
            .map(|(rank, (event, agent_name))| {
                let score = if all_queries.is_empty() {
                    1.0 / (1.0 + rank as f64)
                } else {
                    let text_lower = event.text.to_lowercase();
                    let exact_hits = queries_lower
                        .iter()
                        .filter(|ql| !ql.is_empty() && text_lower.contains(ql.as_str()))
                        .count() as f64;
                    let token_hits = if query_tokens.is_empty() {
                        0.0
                    } else {
                        let doc_tokens = tokenize_hybrid_search_tokens(&event.text);
                        query_tokens
                            .iter()
                            .filter(|qt| doc_tokens.contains(qt))
                            .count() as f64
                            / (query_tokens.len() as f64)
                    };
                    1.0 / (30.0 + rank as f64 + 1.0) + exact_hits * 1.5 + token_hits * 0.8
                };
                TurnTranscriptSearchHit {
                    score,
                    agent_name,
                    event,
                }
            })
            .collect();

        if !all_queries.is_empty() && req.sort == HistorySort::Relevance {
            hits.sort_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| b.event.timestamp_ms.cmp(&a.event.timestamp_ms))
                    .then_with(|| b.event.seq.cmp(&a.event.seq))
                    .then_with(|| b.event.rowid.cmp(&a.event.rowid))
            });
        } else if req.sort == HistorySort::Newest {
            hits.sort_by(|a, b| {
                b.event
                    .timestamp_ms
                    .cmp(&a.event.timestamp_ms)
                    .then_with(|| b.event.seq.cmp(&a.event.seq))
                    .then_with(|| b.event.rowid.cmp(&a.event.rowid))
            });
        } else if ascending {
            hits.sort_by(|a, b| {
                a.event
                    .timestamp_ms
                    .cmp(&b.event.timestamp_ms)
                    .then_with(|| a.event.seq.cmp(&b.event.seq))
                    .then_with(|| a.event.rowid.cmp(&b.event.rowid))
            });
        }

        hits.truncate(limit);
        Ok(hits)
    }

    /// Search `turn_transcript_events` and project hits into [`ConversationTurn`] rows.
    pub fn search_turn_transcript_events(
        &self,
        req: &HistorySearchQuery,
    ) -> Result<Vec<ConversationTurn>, Error> {
        Ok(self
            .search_turn_transcript_event_hits(req)?
            .into_iter()
            .map(TurnTranscriptSearchHit::into_conversation_turn)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;

    #[allow(clippy::too_many_arguments)]
    fn sample_event(
        agent_id: &AgentId,
        turn_id: &str,
        seq: i64,
        ts: i64,
        role: &str,
        tool_name: Option<&str>,
        text: &str,
        raw_json: &str,
    ) -> NewTurnTranscriptEvent {
        NewTurnTranscriptEvent {
            agent_id: agent_id.clone(),
            turn_id: turn_id.to_string(),
            session_id: Some("sess-live-1".to_string()),
            harness: "agy".to_string(),
            source: ".gemini/antigravity-cli/brain/s1/.system_generated/logs/transcript_full.jsonl"
                .to_string(),
            seq,
            timestamp_ms: ts,
            role: role.to_string(),
            tool_name: tool_name.map(str::to_string),
            tool_call_id: tool_name.map(|_| format!("call_{seq}")),
            text: text.to_string(),
            raw_json: raw_json.to_string(),
        }
    }

    #[test]
    fn append_is_idempotent_lossless_unmasked_and_searchable_by_all_facets() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let alpha = store.create_agent(NewAgent::new("alpha", "agy")).unwrap();
        let beta = store.create_agent(NewAgent::new("beta", "claude")).unwrap();

        let raw_secret = "sk-live-unmasked-transcript-canary-9876543210";
        let events = vec![
            sample_event(
                &alpha.id,
                "turn_1",
                10,
                1_000,
                "thinking",
                None,
                &format!("Planning deployment with token {raw_secret}"),
                &format!(
                    r#"{{"type":"PLANNER_RESPONSE","thinking":"Planning deployment with token {raw_secret}"}}"#
                ),
            ),
            sample_event(
                &alpha.id,
                "turn_1",
                20,
                2_000,
                "tool_call",
                Some("run_command"),
                &format!(
                    "[tool_call:run_command] curl -H 'Authorization: Bearer {raw_secret}' https://example.invalid"
                ),
                &format!(
                    r#"{{"type":"PLANNER_RESPONSE","tool_calls":[{{"name":"run_command","args":"{raw_secret}"}}]}}"#
                ),
            ),
            sample_event(
                &alpha.id,
                "turn_1",
                30,
                3_000,
                "tool_result",
                Some("run_command"),
                "HTTP 200 deployment succeeded",
                r#"{"type":"TOOL_RESULT","output":"HTTP 200 deployment succeeded"}"#,
            ),
            sample_event(
                &alpha.id,
                "turn_1",
                40,
                4_000,
                "assistant",
                None,
                "All checks passed for release v42",
                r#"{"type":"PLANNER_RESPONSE","content":"All checks passed for release v42"}"#,
            ),
            sample_event(
                &beta.id,
                "turn_beta",
                10,
                5_000,
                "thinking",
                None,
                "Beta private thought secret",
                r#"{"type":"thinking","thinking":"Beta private thought secret"}"#,
            ),
        ];

        // First append inserts all 5; duplicate replay inserts 0 (idempotent).
        assert_eq!(store.append_turn_transcript_events(&events).unwrap(), 5);
        assert_eq!(store.append_turn_transcript_events(&events).unwrap(), 0);

        // Ordered list for turn_1 with after_seq cursor and lossless unmasked text/raw_json.
        let listed = store
            .list_turn_transcript_events(&alpha.id, "turn_1", None, 100)
            .unwrap();
        assert_eq!(listed.len(), 4);
        assert_eq!(
            listed.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![10, 20, 30, 40]
        );
        assert!(listed[0].text.contains(raw_secret));
        assert!(listed[0].raw_json.contains(raw_secret));
        assert!(listed[1].text.contains(raw_secret));

        let resumed = store
            .list_turn_transcript_events(&alpha.id, "turn_1", Some(20), 100)
            .unwrap();
        assert_eq!(
            resumed.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![30, 40]
        );

        // Search thinking text including unmasked raw secret token.
        let secret_hits = store
            .search_turn_transcript_event_hits(&HistorySearchQuery {
                query: Some(raw_secret.to_string()),
                visible_agent_ids: Some(vec![alpha.id.to_string()]),
                limit: 20,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(secret_hits.len(), 2);
        assert!(secret_hits.iter().all(|h| h.event.agent_id == alpha.id));

        // Search by role=thinking.
        let thinking_hits = store
            .search_turn_transcript_event_hits(&HistorySearchQuery {
                role: Some("thinking".into()),
                visible_agent_ids: Some(vec![alpha.id.to_string()]),
                limit: 20,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(thinking_hits.len(), 1);
        assert_eq!(thinking_hits[0].event.seq, 10);

        // Search by tool_name and only_tools.
        let tool_hits = store
            .search_turn_transcript_event_hits(&HistorySearchQuery {
                tool_name: Some("run_command".into()),
                only_tools: true,
                visible_agent_ids: Some(vec![alpha.id.to_string()]),
                limit: 20,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(tool_hits.len(), 2);
        assert!(
            tool_hits
                .iter()
                .all(|h| h.event.tool_name.as_deref() == Some("run_command"))
        );

        // include_tools=false hides tool_call & tool_result.
        let non_tool_hits = store
            .search_turn_transcript_event_hits(&HistorySearchQuery {
                turn_id: Some("turn_1".into()),
                include_tools: false,
                visible_agent_ids: Some(vec![alpha.id.to_string()]),
                limit: 20,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(non_tool_hits.len(), 2);
        assert_eq!(
            non_tool_hits
                .iter()
                .map(|h| h.event.role.as_str())
                .collect::<Vec<_>>(),
            vec!["thinking", "assistant"]
        );

        // Visibility isolation: alpha cannot see beta's events.
        let cross_peer = store
            .search_turn_transcript_event_hits(&HistorySearchQuery {
                query: Some("Beta private".into()),
                visible_agent_ids: Some(vec![alpha.id.to_string()]),
                limit: 20,
                ..Default::default()
            })
            .unwrap();
        assert!(cross_peer.is_empty());

        // Empty visibility fails closed.
        let empty = store
            .search_turn_transcript_event_hits(&HistorySearchQuery {
                visible_agent_ids: Some(Vec::new()),
                limit: 20,
                ..Default::default()
            })
            .unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn postgres_search_sql_uses_gin_fts_and_migration_has_gin_index() {
        let pg_ddl = crate::db::PG_TURN_TRANSCRIPT_EVENTS_SQL;
        assert!(pg_ddl.contains("CREATE TABLE IF NOT EXISTS public.turn_transcript_events"));
        assert!(pg_ddl.contains("UNIQUE (agent_id, source, seq)"));
        assert!(pg_ddl.contains("USING GIN (to_tsvector('simple', text))"));
        assert!(pg_ddl.contains("idx_turn_transcript_events_turn_seq"));
        assert!(pg_ddl.contains("idx_turn_transcript_events_tool"));

        let q = vec!["deployment".to_string()];
        let (sql, _params) = build_transcript_event_search_sql(&TranscriptEventSearchSpec {
            since_ms: Some(100),
            until_ms: Some(900),
            fetch_limit: 40,
            ascending: false,
            agent_id: Some("agt_1"),
            agent_name: None,
            turn_id: Some("turn_abc"),
            role: Some("thinking"),
            include_tools: true,
            only_tools: false,
            tool_name: None,
            direction: None,
            sender_like: None,
            from_seq: None,
            to_seq: None,
            visible_agent_ids: Some(&["agt_1".to_string()]),
            queries: &q,
            exclude: &[],
            backend: DbBackend::Postgres,
        });
        assert!(sql.contains("public.turn_transcript_events"));
        assert!(sql.contains("to_tsvector('simple', e.text) @@ plainto_tsquery('simple',"));
        assert!(sql.contains("e.turn_id = ?"));
        assert!(sql.contains("e.role = ?"));

        // Non-ASCII / punctuation query falls back to ILIKE on Postgres.
        let sym_q = vec!["sk-live-secret".to_string()];
        let (sym_sql, _) = build_transcript_event_search_sql(&TranscriptEventSearchSpec {
            since_ms: None,
            until_ms: None,
            fetch_limit: 20,
            ascending: false,
            agent_id: None,
            agent_name: None,
            turn_id: None,
            role: None,
            include_tools: true,
            only_tools: true,
            tool_name: Some("bash"),
            direction: None,
            sender_like: None,
            from_seq: None,
            to_seq: None,
            visible_agent_ids: None,
            queries: &sym_q,
            exclude: &[],
            backend: DbBackend::Postgres,
        });
        assert!(sym_sql.contains("e.text ILIKE"));
        assert!(sym_sql.contains("e.role IN ('tool_call', 'tool_result')"));
        assert!(sym_sql.contains("LOWER(COALESCE(e.tool_name, '')) = LOWER("));
    }
}
