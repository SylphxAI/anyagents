//! Page selection and metadata for the coordinator roster.

use std::collections::{HashMap, HashSet};

use super::agents::{AGENT_COLS, AGENT_LIST_COLS, DEFAULT_ARCHIVE_ROSTER_LIMIT};
use super::helpers::agent_from_row_db;
use super::schedules::schedule_from_row_db;
use super::{Agent, AgentQueryMetadata, Store, TurnErrors};
use crate::db::{DbBackend, DbExec, DbExecExt};
use crate::error::Error;
use crate::stateless::LatestRemoteTurnError;
use crate::{AgentId, db_params, now_ms};

/// A single JSON parameter avoids SQLite's variable ceiling and keeps SQL text
/// stable across page sizes. Both forms return a one-column text relation.
fn ids_sql(backend: DbBackend, param: &str) -> String {
    match backend {
        DbBackend::Sqlite => format!("SELECT value FROM json_each({param})"),
        DbBackend::Postgres => format!("SELECT jsonb_array_elements_text(CAST({param} AS jsonb))"),
    }
}

fn ids_json<'a>(ids: impl IntoIterator<Item = &'a AgentId>) -> String {
    serde_json::to_string(&ids.into_iter().map(AgentId::as_str).collect::<Vec<_>>())
        .expect("string arrays are serializable")
}

#[derive(Default)]
pub struct RosterSelection<'a> {
    pub state: Option<&'a str>,
    pub parent: Option<&'a AgentId>,
    pub roots_only: bool,
    pub subagents_only: bool,
    pub name: Option<&'a AgentId>,
    pub descendants: bool,
    pub tree_context: bool,
    pub limit: Option<usize>,
    pub offset: usize,
    pub isolated_caller: Option<&'a Agent>,
    pub sharing: Option<&'a HashSet<AgentId>>,
}

impl Store {
    pub fn select_roster(&self, q: RosterSelection<'_>) -> Result<(Vec<Agent>, usize), Error> {
        let conn = self.read_conn();
        let archived: i64 = conn.db_query_one(
            "SELECT count(*) FROM agents WHERE state = 'archived'",
            &[],
            |r| r.get(0),
        )?;
        let archive_page = matches!(q.state, Some("all" | "archived"));
        let limit = q
            .limit
            .or(archive_page.then_some(DEFAULT_ARCHIVE_ROSTER_LIMIT));
        let limit = limit
            .map(|l| i64::try_from(l.max(1)).unwrap_or(i64::MAX))
            .unwrap_or(i64::MAX);
        let offset = i64::try_from(q.offset).unwrap_or(i64::MAX);
        let mut params = db_params![limit, offset];
        let mut bind = |value: Option<&str>| {
            params.extend(db_params![value]);
            format!("?{}", params.len())
        };
        let state = match q.state {
            Some("all") => "1 = 1".into(),
            Some("archived") => "state = 'archived'".into(),
            Some(s) => format!("state != 'archived' AND state = {}", bind(Some(s))),
            None => "state != 'archived'".into(),
        };
        let mut ctes = Vec::new();
        let visibility = if let Some(caller) = q.isolated_caller {
            let id = bind(Some(caller.id.as_str()));
            let parent = bind(caller.parent_id.as_ref().map(AgentId::as_str));
            let shared = ids_json(q.sharing.into_iter().flatten());
            let shared = ids_sql(conn.backend(), &bind(Some(&shared)));
            // Authorization walks at most seventeen parent edges.
            ctes.push(format!(
                "owned(id, depth) AS (
                SELECT id, 0 FROM agents WHERE id = {id}
                UNION SELECT a.id, o.depth + 1 FROM agents a JOIN owned o ON a.parent_id = o.id
                WHERE o.depth < 17
            )"
            ));
            format!("(id = {parent} OR id IN ({shared}) OR id IN (SELECT id FROM owned))")
        } else {
            "1 = 1".into()
        };
        let parent_clause = if let Some(parent) = q.parent {
            let parent = bind(Some(parent.as_str()));
            if q.descendants {
                // Existing unscoped archive pages traverse every lifecycle state.
                // Other roster queries traverse the visible state roster, including
                // the ancestor context used by the HTTP tree view.
                let traversable = if archive_page && q.isolated_caller.is_none() && q.name.is_none()
                {
                    "agents"
                } else {
                    if !archive_page {
                        ctes.push(format!(
                            "universe(id) AS (
                            SELECT id FROM agents WHERE {state}
                            UNION SELECT a.parent_id FROM agents a JOIN universe u ON a.id = u.id
                            WHERE a.parent_id IS NOT NULL
                        ), traversable AS (
                            SELECT id, parent_id FROM agents WHERE id IN (SELECT id FROM universe)
                            AND {visibility}
                        )"
                        ));
                    } else {
                        ctes.push(format!(
                            "traversable AS (
                            SELECT id, parent_id FROM agents WHERE {state} AND {visibility}
                        )"
                        ));
                    }
                    "traversable"
                };
                ctes.push(format!(
                    "below(id) AS (
                    SELECT id FROM {traversable} WHERE parent_id = {parent}
                    UNION SELECT a.id FROM {traversable} a JOIN below b ON a.parent_id = b.id
                )"
                ));
                "id IN (SELECT id FROM below)".into()
            } else {
                format!("parent_id = {parent}")
            }
        } else if q.roots_only {
            "parent_id IS NULL".into()
        } else if q.subagents_only {
            "parent_id IS NOT NULL".into()
        } else {
            "1 = 1".into()
        };
        let name = q.name.map_or_else(
            || "1 = 1".into(),
            |id| format!("id = {}", bind(Some(id.as_str()))),
        );
        let where_clause = format!("{state} AND {visibility} AND {parent_clause} AND {name}");
        let selected = if q.tree_context && !archive_page {
            // Include visible ancestors even when their lifecycle state differs.
            ctes.push(format!(
                "wanted(node, next_parent) AS (
                SELECT id, parent_id FROM agents WHERE {where_clause}
                UNION SELECT id, parent_id FROM agents JOIN wanted w ON id = w.next_parent
                WHERE {visibility}
            )"
            ));
            "id IN (SELECT node FROM wanted)".into()
        } else {
            where_clause
        };
        let with = if ctes.is_empty() {
            String::new()
        } else {
            format!("WITH RECURSIVE {}", ctes.join(", "))
        };
        let order = if archive_page {
            "(CASE WHEN state = 'archived' THEN 1 ELSE 0 END), created_at_ms DESC, name"
        } else {
            "name"
        };
        // Legacy non-fast-path rosters can derive the public description from
        // stored instructions on selected archive rows. Archive fast pages
        // already omit those instructions.
        let cols = if archive_page && q.isolated_caller.is_none() && q.name.is_none() {
            AGENT_LIST_COLS
        } else {
            AGENT_COLS
        };
        let agents = conn.db_query_vec(
            &format!(
                "{with} SELECT {cols} FROM agents WHERE {selected}
                      ORDER BY {order} LIMIT ?1 OFFSET ?2"
            ),
            &params,
            agent_from_row_db,
        )?;
        Ok((agents, archived as usize))
    }

    /// Lightweight ancestry for authorization and role resolution; never fetches
    /// ancestor instructions, and stops at the largest caller walk's depth.
    pub fn roster_ancestors(
        &self,
        ids: &[&AgentId],
    ) -> Result<HashMap<AgentId, RosterAncestor>, Error> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let conn = self.read_conn();
        let selected = ids_sql(conn.backend(), "?1");
        Ok(conn
            .db_query_vec(
                &format!(
                    "WITH RECURSIVE ancestors(id, depth) AS (
                SELECT id, 0 FROM agents WHERE id IN ({selected})
                UNION SELECT a.parent_id, w.depth + 1 FROM agents a JOIN ancestors w ON a.id = w.id
                WHERE w.depth < 64 AND a.parent_id IS NOT NULL
            ) SELECT id, parent_id, workdir FROM agents WHERE id IN (SELECT id FROM ancestors)"
                ),
                &db_params![ids_json(ids.iter().copied())],
                |r| {
                    Ok((
                        r.get(0)?,
                        RosterAncestor {
                            parent_id: r.get(1)?,
                            workdir: r.get(2)?,
                        },
                    ))
                },
            )?
            .into_iter()
            .collect())
    }

    /// Reply permissions for these senders only; absence or a read error grants no access.
    pub fn roster_reply_senders(&self, caller: &Agent, ids: &[&AgentId]) -> HashSet<AgentId> {
        if ids.is_empty() {
            return HashSet::new();
        }
        let conn = self.read_conn();
        let selected = ids_sql(conn.backend(), "?1");
        conn.db_query_vec(
            &format!("SELECT a.id FROM agents a WHERE a.id IN ({selected}) AND EXISTS (
                SELECT 1 FROM outbound_messages o WHERE o.agent_id = a.id AND o.destination IN (?2, ?3)
            )"),
            &db_params![ids_json(ids.iter().copied()), format!("agent:{}", caller.name), caller.id.as_str()],
            |r| r.get(0),
        ).unwrap_or_default().into_iter().collect()
    }

    pub fn agent_page_metadata(&self, page: &[&Agent]) -> Result<AgentQueryMetadata, Error> {
        let mut meta = AgentQueryMetadata::default();
        if page.is_empty() {
            return Ok(meta);
        }
        let ids = ids_json(page.iter().map(|a| &a.id));
        let conn = self.read_conn();
        let selected = ids_sql(conn.backend(), "?1");
        for (id, alias) in conn.db_query_vec(
            &format!("SELECT agent_id, alias FROM agent_aliases WHERE agent_id IN ({selected}) ORDER BY alias"),
            &db_params![ids], pair::<String, String>,
        )? {
            meta.aliases.entry(id).or_default().push(alias);
        }
        let live: Vec<_> = page
            .iter()
            .filter(|a| a.state != "archived")
            .map(|a| &a.id)
            .collect();
        if live.is_empty() {
            return Ok(meta);
        }
        let live_ids = ids_json(live.iter().copied());
        let params = db_params![live_ids];
        // Keep the roster's existing mixed-page behavior: archived rows have
        // secret names on a mixed page, but not on an archived-only page.
        for (id, secret) in conn
            .db_query_vec(
                &format!(
                    "SELECT agent_id, secret_name FROM agent_secret_grants
                      WHERE agent_id IN ({selected}) ORDER BY secret_name"
                ),
                &db_params![ids],
                pair::<String, String>,
            )
            .unwrap_or_default()
        {
            meta.secrets.entry(id).or_default().push(secret);
        }
        let not_passive = conn.backend().sql_not_passive_reaction("m.context");
        meta.oldest_undelivered = conn
            .db_query_vec(
                &format!(
                    "WITH selected(id) AS ({selected})
               SELECT a.id, (SELECT m.received_at_ms FROM messages m
                 WHERE m.agent_id = a.id AND m.seq > COALESCE(d.delivered_seq, 0) AND {not_passive}
                 ORDER BY m.seq LIMIT 1)
               FROM selected a LEFT JOIN mailbox_cursors d ON d.agent_id = a.id"
                ),
                &params,
                pair::<String, Option<i64>>,
            )?
            .into_iter()
            .filter_map(|(id, time)| time.map(|t| (id, t)))
            .collect();
        meta.open_turns = conn.db_query_vec(
            &format!("SELECT t.agent_id, COALESCE((SELECT m.received_at_ms FROM messages m
                WHERE m.agent_id = t.agent_id AND m.seq > t.completed_seq AND m.seq <= t.opened_seq
                ORDER BY m.seq LIMIT 1), t.opened_at_ms)
                FROM turn_recovery t WHERE t.agent_id IN ({selected}) AND t.opened_seq > t.completed_seq"),
            &params, pair::<String, i64>,
        )?.into_iter().collect();
        // Match list_schedules: repair legacy NULL timestamps once per
        // roster, not once per visible agent.
        if !super::holds_store_writer() {
            let _ = self.repair_null_schedule_next_fire(now_ms());
        }
        for schedule in conn.db_query_vec(
            &format!(
                "SELECT {} FROM agent_schedules WHERE agent_id IN ({selected}) AND state = 'active'
                      ORDER BY created_at_ms, id",
                schedule_cols!()
            ),
            &params,
            schedule_from_row_db,
        )? {
            meta.schedules
                .entry(schedule.agent_id.to_string())
                .or_default()
                .push(schedule);
        }
        meta.turn_errors = conn.db_query_vec(
            &format!("SELECT agent_id, streak, last_error, last_error_at_ms, faulted_at_ms FROM turn_errors
                       WHERE agent_id IN ({selected}) AND (streak > 0 OR faulted_at_ms IS NOT NULL)"),
            &params,
            |r| Ok((r.get(0)?, TurnErrors { streak: r.get(1)?, last_error: r.get(2)?,
                last_error_at_ms: r.get(3)?, faulted_at_ms: r.get(4)? })),
        )?.into_iter().collect();
        let fresh_since =
            now_ms().saturating_sub(crate::stateless::ACTIVE_REMOTE_TURN_FRESHNESS_MS);
        if conn.backend() == DbBackend::Postgres {
            meta.active_remote_turns = conn
                .db_query_vec(
                    &format!(
                        "SELECT agent_id, MIN(created_at_ms) FROM pending_remote_turns
                   WHERE agent_id IN ({selected}) AND status IN ('dispatched', 'running')
                   AND last_event_at_ms >= ?2 GROUP BY agent_id"
                    ),
                    &db_params![live_ids, fresh_since],
                    pair::<String, i64>,
                )
                .unwrap_or_default()
                .into_iter()
                .collect();
            meta.latest_remote_turn_errors = conn.db_query_vec(
                &format!("WITH selected(id) AS ({selected})
                   SELECT a.id, t.turn_id, t.error_text, t.updated_at_ms
                   FROM selected a CROSS JOIN LATERAL (
                       SELECT turn_id, error_text, updated_at_ms, status FROM pending_remote_turns
                       WHERE agent_id = a.id AND status IN ('completed', 'failed')
                       ORDER BY updated_at_ms DESC, created_at_ms DESC, (status = 'completed') DESC, turn_id DESC
                       LIMIT 1
                   ) t WHERE t.status = 'failed' AND t.error_text IS NOT NULL"),
                &params, |r| {
                    let error: String = r.get(2)?;
                    Ok((r.get(0)?, LatestRemoteTurnError {
                        turn_id: r.get(1)?, error_text: error.trim().to_string(), settled_at_ms: r.get(3)?,
                    }))
                },
            ).unwrap_or_default().into_iter().filter(|(_, e)| !e.error_text.is_empty()).collect();
            let locks = live
                .iter()
                .map(|id| format!("agent:proc:{id}"))
                .collect::<Vec<_>>();
            let locks = serde_json::to_string(&locks).expect("string arrays are serializable");
            meta.active_proc_locks = conn.db_query_vec(
                &format!("SELECT lease_key FROM coordinator_leases WHERE lease_key IN ({selected}) AND expires_at_ms > ?2"),
                &db_params![locks, now_ms()], |r| r.get::<String>(0),
            ).unwrap_or_default().into_iter().filter_map(|key| key.strip_prefix("agent:proc:").map(str::to_string)).collect();
        } else {
            let turns = self.remote_turn_store();
            meta.active_remote_turns = turns
                .list_active_fresh_by_agent(fresh_since)
                .unwrap_or_default();
            meta.latest_remote_turn_errors =
                turns.list_latest_failed_by_agent().unwrap_or_default();
        }
        Ok(meta)
    }
}

pub struct RosterAncestor {
    pub parent_id: Option<AgentId>,
    pub workdir: Option<String>,
}

fn pair<A: crate::db::FromDbValue, B: crate::db::FromDbValue>(
    r: &crate::db::DbRow<'_>,
) -> Result<(A, B), Error> {
    Ok((r.get(0)?, r.get(1)?))
}
