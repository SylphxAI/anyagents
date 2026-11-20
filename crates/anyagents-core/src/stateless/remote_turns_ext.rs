//! Bulk active-turn projection and mid-turn `inbox()` watermark helpers for [`RemoteTurnStore`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use sqlx::PgPool;

use crate::db::block_on_pg;
use crate::error::Error;
use crate::id::AgentId;
use crate::migrate::now_ms;

use super::remote_turns::{
    NewRemoteTurn, PendingRemoteTurn, RemoteTurnEventUpdate, RemoteTurnStore, SettleOutcome,
    SettleRemoteTurn, is_terminal_remote_turn_status, sanitize_pg_json, sanitize_pg_text,
    validate_status, validate_terminal_status, validate_turn_id, verify_callback_token_hash,
};

/// Default freshness window (3 minutes) for projecting an active remote turn
/// into Coordinator `AgentView` (`process="up", turn_state="in_turn"`).
pub const ACTIVE_REMOTE_TURN_FRESHNESS_MS: i64 = 180_000;

/// Metadata key storing the highest mailbox sequence read via `inbox()` during
/// an active serverless remote turn.
pub const MAX_INBOX_SEQ_META_KEY: &str = "max_inbox_seq";

/// Latest unrecovered terminal remote turn failure for an agent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatestRemoteTurnError {
    pub turn_id: String,
    pub error_text: String,
    pub settled_at_ms: i64,
}

fn terminal_turn_rank(turn: &PendingRemoteTurn) -> (i64, i64, bool, &str) {
    (
        turn.updated_at_ms,
        turn.created_at_ms,
        turn.status == "completed",
        turn.turn_id.as_str(),
    )
}

fn failed_turn_to_error(turn: &PendingRemoteTurn) -> Option<LatestRemoteTurnError> {
    if turn.status != "failed" {
        return None;
    }
    let error_text = turn
        .error_text
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    Some(LatestRemoteTurnError {
        turn_id: turn.turn_id.clone(),
        error_text: error_text.to_string(),
        settled_at_ms: turn.updated_at_ms,
    })
}

/// Return the latest unrecovered failed remote turn for `agent_id` in PostgreSQL.
/// Returns `Ok(None)` when the agent's latest terminal (`completed`/`failed`) turn
/// completed cleanly or has empty error text.
pub(super) fn pg_latest_failed_for_agent(
    pool: &PgPool,
    agent_id: &AgentId,
) -> Result<Option<LatestRemoteTurnError>, Error> {
    let pool = pool.clone();
    let aid = agent_id.as_str().to_string();
    block_on_pg(async move {
        use sqlx::Row as _;
        let row = sqlx::query(
            "SELECT turn_id, status, error_text, updated_at_ms
             FROM public.pending_remote_turns
             WHERE agent_id = $1
               AND status IN ('completed', 'failed')
             ORDER BY updated_at_ms DESC, created_at_ms DESC, (status = 'completed') DESC, turn_id DESC
             LIMIT 1",
        )
        .bind(&aid)
        .fetch_optional(&pool)
        .await
        .map_err(Error::from)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status: String = row.try_get("status").map_err(Error::from)?;
        if status != "failed" {
            return Ok(None);
        }
        let error_text: Option<String> = row.try_get("error_text").map_err(Error::from)?;
        let Some(trimmed) = error_text
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let turn_id: String = row.try_get("turn_id").map_err(Error::from)?;
        let settled_at_ms: i64 = row.try_get("updated_at_ms").map_err(Error::from)?;
        Ok(Some(LatestRemoteTurnError {
            turn_id,
            error_text: trimmed.to_string(),
            settled_at_ms,
        }))
    })
}

/// Single-query PostgreSQL lookup returning `agent_id -> LatestRemoteTurnError`
/// for all agents whose latest terminal (`completed`/`failed`) remote turn failed.
pub(super) fn pg_list_latest_failed_by_agent(
    pool: &PgPool,
) -> Result<HashMap<String, LatestRemoteTurnError>, Error> {
    let pool = pool.clone();
    block_on_pg(async move {
        use sqlx::Row as _;
        let rows = sqlx::query(
            "SELECT agent_id, turn_id, error_text, updated_at_ms
             FROM (
                 SELECT DISTINCT ON (agent_id)
                        agent_id, turn_id, status, error_text, updated_at_ms
                 FROM public.pending_remote_turns
                 WHERE status IN ('completed', 'failed')
                 ORDER BY agent_id, updated_at_ms DESC, created_at_ms DESC, (status = 'completed') DESC, turn_id DESC
             ) latest
             WHERE status = 'failed'
               AND error_text IS NOT NULL
               AND btrim(error_text) <> ''",
        )
        .fetch_all(&pool)
        .await
        .map_err(Error::from)?;
        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            let aid: String = row.try_get("agent_id").map_err(Error::from)?;
            let turn_id: String = row.try_get("turn_id").map_err(Error::from)?;
            let error_text: String = row.try_get("error_text").map_err(Error::from)?;
            let trimmed = error_text.trim();
            if trimmed.is_empty() {
                continue;
            }
            let settled_at_ms: i64 = row.try_get("updated_at_ms").map_err(Error::from)?;
            out.insert(
                aid,
                LatestRemoteTurnError {
                    turn_id,
                    error_text: trimmed.to_string(),
                    settled_at_ms,
                },
            );
        }
        Ok(out)
    })
}

/// Collect `agent_id -> LatestRemoteTurnError` from an in-memory/filesystem turn slice.
pub(super) fn collect_latest_failed_by_agent<'a>(
    turns: impl IntoIterator<Item = &'a PendingRemoteTurn>,
) -> HashMap<String, LatestRemoteTurnError> {
    let mut latest_by_agent: HashMap<&str, &'a PendingRemoteTurn> = HashMap::new();
    for turn in turns {
        if !matches!(turn.status.as_str(), "completed" | "failed") {
            continue;
        }
        latest_by_agent
            .entry(turn.agent_id.as_str())
            .and_modify(|existing| {
                if terminal_turn_rank(turn) > terminal_turn_rank(existing) {
                    *existing = turn;
                }
            })
            .or_insert(turn);
    }
    let mut out = HashMap::new();
    for (aid, turn) in latest_by_agent {
        if let Some(err) = failed_turn_to_error(turn) {
            out.insert(aid.to_string(), err);
        }
    }
    out
}

/// Find the latest unrecovered failed remote turn for `agent_id` from an iterator.
pub(super) fn latest_failed_for_agent_from_iter<'a>(
    turns: impl IntoIterator<Item = &'a PendingRemoteTurn>,
    agent_id: &AgentId,
) -> Option<LatestRemoteTurnError> {
    let winner = turns
        .into_iter()
        .filter(|t| &t.agent_id == agent_id && matches!(t.status.as_str(), "completed" | "failed"))
        .max_by(|a, b| terminal_turn_rank(a).cmp(&terminal_turn_rank(b)))?;
    failed_turn_to_error(winner)
}

/// Single-query PostgreSQL lookup returning `agent_id -> earliest created_at_ms`
/// for all agents with a fresh non-terminal remote turn.
pub(super) fn pg_list_active_fresh_by_agent(
    pool: &PgPool,
    fresh_since_ms: i64,
) -> Result<HashMap<String, i64>, Error> {
    let pool = pool.clone();
    let rows = block_on_pg(async move {
        use sqlx::Row as _;
        let rows = sqlx::query(
            "SELECT agent_id, MIN(created_at_ms) AS started_at_ms
             FROM public.pending_remote_turns
             WHERE status IN ('dispatched', 'running')
               AND last_event_at_ms >= $1
             GROUP BY agent_id",
        )
        .bind(fresh_since_ms)
        .fetch_all(&pool)
        .await
        .map_err(Error::from)?;
        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            let aid: String = row.try_get("agent_id").map_err(Error::from)?;
            let started: i64 = row.try_get("started_at_ms").map_err(Error::from)?;
            out.insert(aid, started);
        }
        Ok::<HashMap<String, i64>, Error>(out)
    })?;
    Ok(rows)
}

/// Collect `agent_id -> earliest created_at_ms` from an in-memory/filesystem turn slice.
pub(super) fn collect_active_fresh_by_agent<'a>(
    turns: impl IntoIterator<Item = &'a PendingRemoteTurn>,
    fresh_since_ms: i64,
) -> HashMap<String, i64> {
    let mut out: HashMap<String, i64> = HashMap::new();
    for turn in turns {
        if turn.is_terminal() || turn.last_event_at_ms < fresh_since_ms {
            continue;
        }
        out.entry(turn.agent_id.to_string())
            .and_modify(|existing| *existing = (*existing).min(turn.created_at_ms))
            .or_insert(turn.created_at_ms);
    }
    out
}

/// Atomically advance `metadata_json->>'max_inbox_seq'` on every active remote
/// turn belonging to `agent_id` in PostgreSQL.
pub(super) fn pg_record_agent_inbox_read_seq(
    pool: &PgPool,
    agent_id: &AgentId,
    max_read_seq: i64,
    now_ms: i64,
) -> Result<usize, Error> {
    if max_read_seq <= 0 {
        return Ok(0);
    }
    let pool = pool.clone();
    let aid = agent_id.as_str().to_string();
    let affected = block_on_pg(async move {
        sqlx::query(
            "UPDATE public.pending_remote_turns
             SET metadata_json = jsonb_set(
                     COALESCE(metadata_json, '{}'::jsonb),
                     '{max_inbox_seq}',
                     to_jsonb(GREATEST(
                         COALESCE((metadata_json->>'max_inbox_seq')::bigint, 0),
                         $2
                     )),
                     true
                 ),
                 updated_at_ms = GREATEST(updated_at_ms, $3)
             WHERE agent_id = $1
               AND status IN ('dispatched', 'running')",
        )
        .bind(&aid)
        .bind(max_read_seq)
        .bind(now_ms)
        .execute(&pool)
        .await
        .map(|r| r.rows_affected() as usize)
        .map_err(Error::from)
    })?;
    Ok(affected)
}

/// Advance `metadata_json.max_inbox_seq` monotonically in-place for local stores.
/// Returns `true` when the turn metadata was modified.
pub(super) fn apply_inbox_read_seq_to_turn(
    turn: &mut PendingRemoteTurn,
    max_read_seq: i64,
    now_ms: i64,
) -> bool {
    if turn.is_terminal() || max_read_seq <= 0 {
        return false;
    }
    let current = turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get(MAX_INBOX_SEQ_META_KEY).and_then(Value::as_i64))
        .unwrap_or(0);
    let next = current.max(max_read_seq);
    if next == current && turn.metadata_json.is_some() {
        return false;
    }
    match turn.metadata_json.as_mut() {
        Some(Value::Object(map)) => {
            map.insert(MAX_INBOX_SEQ_META_KEY.to_string(), json!(next));
        }
        _ => {
            turn.metadata_json = Some(json!({ MAX_INBOX_SEQ_META_KEY: next }));
        }
    }
    turn.updated_at_ms = turn.updated_at_ms.max(now_ms);
    true
}

/// Pure in-memory [`RemoteTurnStore`] implementation for isolated unit tests.
#[derive(Clone, Default)]
pub struct InMemoryRemoteTurnStore {
    rows: Arc<Mutex<HashMap<String, PendingRemoteTurn>>>,
}

impl InMemoryRemoteTurnStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RemoteTurnStore for InMemoryRemoteTurnStore {
    fn create_turn(&self, input: &NewRemoteTurn) -> Result<PendingRemoteTurn, Error> {
        let turn_id = validate_turn_id(&input.turn_id)?.to_string();
        let status = validate_status(&input.status)?.to_string();
        let mut map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        if map.contains_key(&turn_id) {
            return Err(Error::Message(format!(
                "remote turn `{turn_id}` already exists"
            )));
        }
        if !is_terminal_remote_turn_status(&status)
            && map
                .values()
                .any(|t| t.agent_id == input.agent_id && !t.is_terminal())
        {
            return Err(Error::TurnAlreadyActive(input.agent_id.to_string()));
        }
        let now = now_ms();
        let turn = PendingRemoteTurn {
            turn_id: turn_id.clone(),
            agent_id: input.agent_id.clone(),
            harness_kind: sanitize_pg_text(&input.harness_kind),
            provider_kind: input.provider_kind.as_deref().map(sanitize_pg_text),
            remote_run_id: input.remote_run_id.as_deref().map(sanitize_pg_text),
            callback_token_hash: input.callback_token_hash.clone(),
            cursor: input.cursor.as_deref().map(sanitize_pg_text),
            status,
            result_text: None,
            error_text: None,
            usage_json: None,
            metadata_json: input.metadata_json.as_ref().map(sanitize_pg_json),
            created_at_ms: now,
            updated_at_ms: now,
            last_event_at_ms: now,
            next_poll_at_ms: input.next_poll_at_ms,
        };
        map.insert(turn_id, turn.clone());
        Ok(turn)
    }

    fn get_turn(&self, turn_id: &str) -> Result<Option<PendingRemoteTurn>, Error> {
        let map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        Ok(map.get(turn_id).cloned())
    }

    fn find_by_remote_run_id(
        &self,
        harness_kind: Option<&str>,
        remote_run_id: &str,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let trimmed = remote_run_id.trim();
        let kind = harness_kind.map(str::trim).filter(|s| !s.is_empty());
        let map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let mut matches: Vec<PendingRemoteTurn> = map
            .values()
            .filter(|t| {
                t.remote_run_id.as_deref() == Some(trimmed)
                    && kind.is_none_or(|hk| t.harness_kind == hk)
            })
            .cloned()
            .collect();
        matches.sort_by_key(|t| std::cmp::Reverse(t.created_at_ms));
        Ok(matches.into_iter().next())
    }

    fn verify_callback_token(
        &self,
        turn_id: &str,
        bearer_token: &str,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let Some(turn) = self.get_turn(turn_id)? else {
            return Ok(None);
        };
        if verify_callback_token_hash(&turn.callback_token_hash, bearer_token) {
            Ok(Some(turn))
        } else {
            Ok(None)
        }
    }

    fn update_remote_run(
        &self,
        turn_id: &str,
        remote_run_id: Option<&str>,
        status: Option<&str>,
        next_poll_at_ms: Option<Option<i64>>,
        metadata_json: Option<&Value>,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let mut map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let Some(turn) = map.get_mut(turn_id) else {
            return Ok(None);
        };
        if let Some(rid) = remote_run_id {
            turn.remote_run_id = Some(sanitize_pg_text(rid));
        }
        if !turn.is_terminal() {
            if let Some(st) = status {
                turn.status = validate_status(st)?.to_string();
            }
            if let Some(np) = next_poll_at_ms {
                turn.next_poll_at_ms = np;
            }
        }
        if let Some(meta) = metadata_json {
            turn.metadata_json = Some(sanitize_pg_json(meta));
        }
        turn.updated_at_ms = turn.updated_at_ms.max(now_ms());
        Ok(Some(turn.clone()))
    }

    fn record_event(
        &self,
        turn_id: &str,
        update: &RemoteTurnEventUpdate,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let next_status = match update.status.as_deref() {
            Some(s) => {
                let valid = validate_status(s)?;
                if is_terminal_remote_turn_status(valid) {
                    return Err(Error::Message(
                        "record_event cannot write terminal status; use settle_turn".into(),
                    ));
                }
                valid
            }
            None => "running",
        };
        let mut map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let Some(turn) = map.get_mut(turn_id) else {
            return Ok(None);
        };
        let event_ts = if update.last_event_at_ms > 0 {
            update.last_event_at_ms
        } else {
            now_ms()
        };
        if !turn.is_terminal() {
            turn.status = next_status.to_string();
            if let Some(np) = update.next_poll_at_ms {
                turn.next_poll_at_ms = np;
            }
        }
        if let Some(ref cur) = update.cursor {
            turn.cursor = Some(sanitize_pg_text(cur));
        }
        if let Some(ref rid) = update.remote_run_id {
            turn.remote_run_id = Some(sanitize_pg_text(rid));
        }
        turn.last_event_at_ms = turn.last_event_at_ms.max(event_ts);
        turn.updated_at_ms = turn.updated_at_ms.max(now_ms().max(event_ts));
        Ok(Some(turn.clone()))
    }

    fn settle_turn(
        &self,
        turn_id: &str,
        settlement: &SettleRemoteTurn,
    ) -> Result<Option<SettleOutcome>, Error> {
        let terminal = validate_terminal_status(&settlement.status)?;
        let mut map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let Some(turn) = map.get_mut(turn_id) else {
            return Ok(None);
        };
        if turn.is_terminal() {
            return Ok(Some(SettleOutcome {
                turn: turn.clone(),
                transitioned: false,
            }));
        }
        let settled_at = if settlement.settled_at_ms > 0 {
            settlement.settled_at_ms
        } else {
            now_ms()
        };
        turn.status = terminal.to_string();
        turn.result_text = settlement.result_text.as_deref().map(sanitize_pg_text);
        turn.error_text = settlement.error_text.as_deref().map(sanitize_pg_text);
        turn.usage_json = settlement.usage_json.as_ref().map(sanitize_pg_json);
        if let Some(ref cur) = settlement.cursor {
            turn.cursor = Some(sanitize_pg_text(cur));
        }
        turn.last_event_at_ms = turn.last_event_at_ms.max(settled_at);
        turn.updated_at_ms = turn.updated_at_ms.max(now_ms().max(settled_at));
        turn.next_poll_at_ms = None;
        Ok(Some(SettleOutcome {
            turn: turn.clone(),
            transitioned: true,
        }))
    }

    fn list_due_polls(&self, now: i64, limit: usize) -> Result<Vec<PendingRemoteTurn>, Error> {
        let map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let mut due: Vec<PendingRemoteTurn> = map
            .values()
            .filter(|t| !t.is_terminal() && t.next_poll_at_ms.is_some_and(|at| at <= now))
            .cloned()
            .collect();
        due.sort_by_key(|t| (t.next_poll_at_ms.unwrap_or(i64::MAX), t.created_at_ms));
        due.truncate(limit.max(1));
        Ok(due)
    }

    fn list_active_for_agent(&self, agent_id: &AgentId) -> Result<Vec<PendingRemoteTurn>, Error> {
        let map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let mut active: Vec<PendingRemoteTurn> = map
            .values()
            .filter(|t| &t.agent_id == agent_id && !t.is_terminal())
            .cloned()
            .collect();
        active.sort_by_key(|t| t.created_at_ms);
        Ok(active)
    }

    fn list_stale_active(
        &self,
        harness_kind: Option<&str>,
        stale_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<PendingRemoteTurn>, Error> {
        let kind = harness_kind.map(str::trim).filter(|s| !s.is_empty());
        let map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let mut stale: Vec<PendingRemoteTurn> = map
            .values()
            .filter(|t| {
                !t.is_terminal()
                    && kind.is_none_or(|hk| t.harness_kind == hk)
                    && t.last_event_at_ms <= stale_before_ms
            })
            .cloned()
            .collect();
        stale.sort_by_key(|t| t.last_event_at_ms);
        stale.truncate(limit.max(1));
        Ok(stale)
    }

    fn schedule_next_poll(
        &self,
        turn_id: &str,
        next_poll_at_ms: Option<i64>,
        cursor: Option<&str>,
        now: i64,
    ) -> Result<bool, Error> {
        let mut map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let Some(turn) = map.get_mut(turn_id) else {
            return Ok(false);
        };
        if turn.is_terminal() {
            return Ok(false);
        }
        turn.next_poll_at_ms = next_poll_at_ms;
        if let Some(cur) = cursor {
            turn.cursor = Some(sanitize_pg_text(cur));
        }
        turn.last_event_at_ms = turn.last_event_at_ms.max(now);
        turn.updated_at_ms = turn.updated_at_ms.max(now);
        Ok(true)
    }

    fn delete_turn(&self, turn_id: &str) -> Result<bool, Error> {
        let mut map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        Ok(map.remove(turn_id).is_some())
    }

    fn list_active_fresh_by_agent(
        &self,
        fresh_since_ms: i64,
    ) -> Result<HashMap<String, i64>, Error> {
        let map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        Ok(collect_active_fresh_by_agent(map.values(), fresh_since_ms))
    }

    fn latest_failed_for_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<LatestRemoteTurnError>, Error> {
        let map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        Ok(latest_failed_for_agent_from_iter(map.values(), agent_id))
    }

    fn list_latest_failed_by_agent(&self) -> Result<HashMap<String, LatestRemoteTurnError>, Error> {
        let map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        Ok(collect_latest_failed_by_agent(map.values()))
    }

    fn record_agent_inbox_read_seq(
        &self,
        agent_id: &AgentId,
        max_read_seq: i64,
        now: i64,
    ) -> Result<usize, Error> {
        let mut map = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let mut updated = 0usize;
        for turn in map.values_mut() {
            if &turn.agent_id == agent_id && apply_inbox_read_seq_to_turn(turn, max_read_seq, now) {
                updated += 1;
            }
        }
        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stateless::{
        FsRemoteTurnStore, InMemoryRemoteTurnStore, NewRemoteTurn, RemoteTurnEventUpdate,
        RemoteTurnStore, SettleRemoteTurn, hash_callback_token,
    };

    fn sample_turn(turn_id: &str, agent_id: &AgentId, message_seq: i64) -> NewRemoteTurn {
        NewRemoteTurn {
            turn_id: turn_id.to_string(),
            agent_id: agent_id.clone(),
            harness_kind: "claude-code".to_string(),
            provider_kind: Some("sandbox".to_string()),
            remote_run_id: Some(format!("run-{turn_id}")),
            callback_token_hash: hash_callback_token("tok-secret"),
            cursor: None,
            status: "running".to_string(),
            metadata_json: Some(json!({ "message_seq": message_seq })),
            next_poll_at_ms: None,
        }
    }

    #[test]
    fn active_fresh_and_inbox_watermark_work_on_fs_and_memory_stores() {
        let tmp = tempfile::tempdir().unwrap();
        let fs_store = FsRemoteTurnStore::new(tmp.path());
        let mem_store = InMemoryRemoteTurnStore::new();
        let aid = AgentId::generate();
        let aid_stale = AgentId::generate();
        let aid_done = AgentId::generate();

        for store in [
            &fs_store as &dyn RemoteTurnStore,
            &mem_store as &dyn RemoteTurnStore,
        ] {
            let t_live = store
                .create_turn(&sample_turn("trn_live", &aid, 10))
                .unwrap();
            let _ = store
                .create_turn(&sample_turn("trn_stale", &aid_stale, 1))
                .unwrap();
            let _ = store
                .create_turn(&sample_turn("trn_done", &aid_done, 5))
                .unwrap();

            // Settle `trn_done` so it must NOT appear in active fresh projection.
            store
                .settle_turn(
                    "trn_done",
                    &SettleRemoteTurn {
                        status: "completed".into(),
                        result_text: Some("ok".into()),
                        error_text: None,
                        usage_json: None,
                        cursor: None,
                        settled_at_ms: t_live.created_at_ms + 100,
                    },
                )
                .unwrap();

            // Advance heartbeat of `trn_live` far ahead so `trn_stale` falls outside window.
            let fresh_now = t_live.created_at_ms + 500_000;
            store
                .record_event(
                    "trn_live",
                    &RemoteTurnEventUpdate {
                        status: Some("running".into()),
                        cursor: None,
                        remote_run_id: None,
                        last_event_at_ms: fresh_now,
                        next_poll_at_ms: None,
                    },
                )
                .unwrap();

            let active = store
                .list_active_fresh_by_agent(fresh_now - ACTIVE_REMOTE_TURN_FRESHNESS_MS)
                .unwrap();
            assert!(active.contains_key(aid.as_str()));
            assert!(!active.contains_key(aid_stale.as_str()));
            assert!(!active.contains_key(aid_done.as_str()));

            // Record mid-turn inbox read watermarks monotonically.
            assert_eq!(
                store
                    .record_agent_inbox_read_seq(&aid, 42, fresh_now + 10)
                    .unwrap(),
                1
            );
            assert_eq!(
                store
                    .record_agent_inbox_read_seq(&aid, 30, fresh_now + 20)
                    .unwrap(),
                0
            );
            assert_eq!(
                store
                    .record_agent_inbox_read_seq(&aid, 55, fresh_now + 30)
                    .unwrap(),
                1
            );
            let updated = store.get_turn("trn_live").unwrap().unwrap();
            assert_eq!(
                updated
                    .metadata_json
                    .as_ref()
                    .and_then(|m| m.get(MAX_INBOX_SEQ_META_KEY))
                    .and_then(Value::as_i64),
                Some(55)
            );
            assert_eq!(
                updated
                    .metadata_json
                    .as_ref()
                    .and_then(|m| m.get("message_seq"))
                    .and_then(Value::as_i64),
                Some(10)
            );
            // Terminal turns are never modified by inbox reads.
            assert_eq!(
                store
                    .record_agent_inbox_read_seq(&aid_done, 99, fresh_now + 40)
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn latest_failed_remote_turn_surfaces_and_clears_on_subsequent_completion() {
        let tmp = tempfile::tempdir().unwrap();
        let fs_store = FsRemoteTurnStore::new(tmp.path());
        let mem_store = InMemoryRemoteTurnStore::new();

        for store in [
            &fs_store as &dyn RemoteTurnStore,
            &mem_store as &dyn RemoteTurnStore,
        ] {
            let aid = AgentId::generate();
            let base = now_ms();
            store
                .create_turn(&sample_turn("trn_fail_1", &aid, 1))
                .unwrap();
            store
                .settle_turn(
                    "trn_fail_1",
                    &SettleRemoteTurn {
                        status: "failed".into(),
                        result_text: None,
                        error_text: Some("lease Pending Unschedulable".into()),
                        usage_json: None,
                        cursor: None,
                        settled_at_ms: base + 100,
                    },
                )
                .unwrap();

            let single = store.latest_failed_for_agent(&aid).unwrap().unwrap();
            assert_eq!(single.turn_id, "trn_fail_1");
            assert_eq!(single.error_text, "lease Pending Unschedulable");
            let bulk = store.list_latest_failed_by_agent().unwrap();
            assert_eq!(bulk.get(aid.as_str()), Some(&single));

            // A newer failure supersedes the older one.
            store
                .create_turn(&sample_turn("trn_fail_2", &aid, 2))
                .unwrap();
            store
                .settle_turn(
                    "trn_fail_2",
                    &SettleRemoteTurn {
                        status: "failed".into(),
                        result_text: None,
                        error_text: Some("sandbox API 503".into()),
                        usage_json: None,
                        cursor: None,
                        settled_at_ms: base + 200,
                    },
                )
                .unwrap();
            assert_eq!(
                store
                    .latest_failed_for_agent(&aid)
                    .unwrap()
                    .unwrap()
                    .error_text,
                "sandbox API 503"
            );

            // A subsequent completed turn clears the unrecovered failure cleanly.
            store
                .create_turn(&sample_turn("trn_ok_3", &aid, 3))
                .unwrap();
            store
                .settle_turn(
                    "trn_ok_3",
                    &SettleRemoteTurn {
                        status: "completed".into(),
                        result_text: Some("ok".into()),
                        error_text: None,
                        usage_json: None,
                        cursor: None,
                        settled_at_ms: base + 300,
                    },
                )
                .unwrap();
            assert!(store.latest_failed_for_agent(&aid).unwrap().is_none());
            assert!(
                !store
                    .list_latest_failed_by_agent()
                    .unwrap()
                    .contains_key(aid.as_str())
            );
        }
    }
}
