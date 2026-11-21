//! PostgreSQL page data in one statement, without joining one-to-many tables
//! to each other (which would multiply aliases, schedules and grants).

use super::{
    Agent, AgentQueryMetadata, RosterAncestor, RosterPageContext, Store, TurnErrors, ids_json,
};
use crate::db::DbExec;
use crate::error::Error;
use crate::stateless::{ACTIVE_REMOTE_TURN_FRESHNESS_MS, LatestRemoteTurnError};
use crate::store::AgentSchedule;
use crate::{AgentId, db_params, now_ms};
use serde::de::DeserializeOwned;

pub(super) fn load(
    store: &Store,
    page: &[&Agent],
    caller: Option<&Agent>,
    include_ancestry: bool,
) -> Result<RosterPageContext, Error> {
    let now = now_ms();
    let live: Vec<_> = page
        .iter()
        .filter(|a| a.state != "archived")
        .map(|a| &a.id)
        .collect();
    let seeds = include_ancestry.then(|| page.iter().map(|a| &a.id).chain(caller.map(|a| &a.id)));
    let caller = caller.filter(|a| !a.is_isolated());
    let mut context = RosterPageContext {
        ancestry: Default::default(),
        replies: Default::default(),
        metadata: Default::default(),
    };
    if page.is_empty() && seeds.as_ref().is_none_or(|s| s.clone().next().is_none()) {
        return Ok(context);
    }
    let conn = store.read_conn();
    let not_passive = conn.backend().sql_not_passive_reaction("m.context");
    let runtime = if live.is_empty() {
        String::new()
    } else {
        format!(
            r#"
        UNION ALL SELECT 'secret', agent_id, to_jsonb(secret_name), 0, secret_name
          FROM agent_secret_grants WHERE agent_id IN (SELECT id FROM page)
        UNION ALL SELECT 'undelivered', a.id, to_jsonb((SELECT m.received_at_ms FROM messages m
          WHERE m.agent_id = a.id AND m.seq > COALESCE(d.delivered_seq, 0) AND {not_passive}
          ORDER BY m.seq LIMIT 1)), 0, ''
          FROM live a LEFT JOIN mailbox_cursors d ON d.agent_id = a.id
        UNION ALL SELECT 'open', t.agent_id, to_jsonb(COALESCE((SELECT m.received_at_ms FROM messages m
          WHERE m.agent_id = t.agent_id AND m.seq > t.completed_seq AND m.seq <= t.opened_seq
          ORDER BY m.seq LIMIT 1), t.opened_at_ms)), 0, ''
          FROM turn_recovery t WHERE t.agent_id IN (SELECT id FROM live) AND t.opened_seq > t.completed_seq
        UNION ALL SELECT 'schedule', agent_id, to_jsonb(s), created_at_ms, id FROM (
          SELECT {} FROM agent_schedules WHERE agent_id IN (SELECT id FROM live) AND state = 'active'
        ) s
        UNION ALL SELECT 'error', agent_id,
          jsonb_build_array(streak, last_error, last_error_at_ms, faulted_at_ms), 0, ''
          FROM turn_errors WHERE agent_id IN (SELECT id FROM live) AND (streak > 0 OR faulted_at_ms IS NOT NULL)
        UNION ALL SELECT 'active', agent_id, to_jsonb(MIN(created_at_ms)), 0, ''
          FROM pending_remote_turns WHERE agent_id IN (SELECT id FROM live)
            AND status IN ('dispatched', 'running') AND last_event_at_ms >= (SELECT fresh FROM args)
          GROUP BY agent_id
        UNION ALL SELECT 'failed', a.id, jsonb_build_array(t.turn_id, t.error_text, t.updated_at_ms), 0, ''
          FROM live a CROSS JOIN LATERAL (
            SELECT turn_id, error_text, updated_at_ms, status FROM pending_remote_turns
            WHERE agent_id = a.id AND status IN ('completed', 'failed')
            ORDER BY updated_at_ms DESC, created_at_ms DESC, (status = 'completed') DESC, turn_id DESC
            LIMIT 1
          ) t WHERE t.status = 'failed' AND t.error_text IS NOT NULL
        UNION ALL SELECT 'lock', a.id, NULL, 0, ''
          FROM live a JOIN coordinator_leases l ON l.lease_key = 'agent:proc:' || a.id
          WHERE expires_at_ms > (SELECT now_ms FROM args)
        UNION ALL SELECT 'repair', '', NULL, 0, '' WHERE (SELECT can_repair FROM args) AND EXISTS (
          SELECT 1 FROM agent_schedules WHERE state = 'active' AND next_fire_at_ms IS NULL
            AND kind IN ('at', 'every')
        )
    "#,
            schedule_cols!()
        )
    };
    let sql = format!(
        r#"
        WITH RECURSIVE args AS (
          SELECT ?4::text AS destination, ?5::text AS caller_id,
            ?6::bigint AS fresh, ?7::bigint AS now_ms, (?8::bigint <> 0) AS can_repair
        ), page(id) AS (SELECT jsonb_array_elements_text(?1::jsonb)),
        live(id) AS (SELECT jsonb_array_elements_text(?2::jsonb)),
        ancestors(id, depth) AS (
          SELECT id, 0 FROM agents WHERE id IN (SELECT jsonb_array_elements_text(?3::jsonb))
          UNION SELECT a.parent_id, w.depth + 1 FROM agents a JOIN ancestors w ON a.id = w.id
            WHERE w.depth < 64 AND a.parent_id IS NOT NULL
        ), info(kind, id, payload, order_num, order_text) AS (
          SELECT 'ancestor', id, jsonb_build_array(parent_id, workdir), 0::bigint, ''::text
            FROM agents WHERE id IN (SELECT id FROM ancestors)
          UNION ALL SELECT 'reply', a.id, NULL, 0, '' FROM agents a CROSS JOIN args
            WHERE a.id IN (SELECT id FROM page) AND EXISTS (
              SELECT 1 FROM outbound_messages o WHERE o.agent_id = a.id
                AND o.destination IN (args.destination, args.caller_id)
            )
          UNION ALL SELECT 'alias', agent_id, to_jsonb(alias), 0, alias
            FROM agent_aliases WHERE agent_id IN (SELECT id FROM page)
          {runtime}
        ) SELECT kind, id, payload::text FROM info ORDER BY kind, order_num, order_text
    "#
    );
    let mut repair = false;
    conn.for_each_row_raw(
        &sql,
        &db_params![
            ids_json(page.iter().map(|a| &a.id)),
            ids_json(live.iter().copied()),
            ids_json(seeds.into_iter().flatten()),
            caller.map(|a| format!("agent:{}", a.name)),
            caller.map(|a| a.id.as_str()),
            now.saturating_sub(ACTIVE_REMOTE_TURN_FRESHNESS_MS),
            now,
            !crate::store::holds_store_writer()
        ],
        &mut |r| {
            let kind: String = r.get(0)?;
            let id: String = r.get(1)?;
            match kind.as_str() {
                "ancestor" => {
                    let (parent_id, workdir) = decode(r.get(2)?)?;
                    context
                        .ancestry
                        .insert(r.get::<AgentId>(1)?, RosterAncestor { parent_id, workdir });
                }
                "reply" => {
                    context.replies.insert(r.get::<AgentId>(1)?);
                }
                "repair" => repair = true,
                _ => populate(&mut context.metadata, &kind, id, r.get(2)?)?,
            }
            Ok(true)
        },
    )?;
    if repair && store.repair_null_schedule_next_fire(now).unwrap_or(0) > 0 {
        // Preserve the read-after-repair contract for legacy schedules. The
        // public fallback also handles repeated concurrent legacy inserts.
        context.metadata = store.agent_page_metadata(page)?;
    }
    Ok(context)
}

fn decode<T: DeserializeOwned>(payload: String) -> Result<T, Error> {
    serde_json::from_str(&payload)
        .map_err(|e| Error::Message(format!("invalid roster metadata: {e}")))
}

fn populate(
    meta: &mut AgentQueryMetadata,
    kind: &str,
    id: String,
    payload: Option<String>,
) -> Result<(), Error> {
    if kind == "lock" {
        meta.active_proc_locks.insert(id);
        return Ok(());
    }
    let Some(payload) = payload else {
        return if kind == "undelivered" {
            Ok(())
        } else {
            Err(Error::Message(format!("missing {kind} roster metadata")))
        };
    };
    match kind {
        "alias" => meta.aliases.entry(id).or_default().push(decode(payload)?),
        "secret" => meta.secrets.entry(id).or_default().push(decode(payload)?),
        "undelivered" => {
            meta.oldest_undelivered.insert(id, decode(payload)?);
        }
        "open" => {
            meta.open_turns.insert(id, decode(payload)?);
        }
        "schedule" => {
            let mut schedule: AgentSchedule = decode(payload)?;
            if schedule.kind == crate::store::schedules::WEBHOOK_SUBSCRIPTION {
                schedule.kind = "watch".into();
                schedule.persistent = true;
            }
            meta.schedules.entry(id).or_default().push(schedule);
        }
        "error" => {
            let (streak, last_error, last_error_at_ms, faulted_at_ms) = decode(payload)?;
            meta.turn_errors.insert(
                id,
                TurnErrors {
                    streak,
                    last_error,
                    last_error_at_ms,
                    faulted_at_ms,
                },
            );
        }
        "active" => {
            meta.active_remote_turns.insert(id, decode(payload)?);
        }
        "failed" => {
            let (turn_id, error_text, settled_at_ms): (String, String, i64) = decode(payload)?;
            if !error_text.trim().is_empty() {
                meta.latest_remote_turn_errors.insert(
                    id,
                    LatestRemoteTurnError {
                        turn_id,
                        error_text: error_text.trim().into(),
                        settled_at_ms,
                    },
                );
            }
        }
        _ => return Err(Error::Message(format!("unknown roster metadata: {kind}"))),
    }
    Ok(())
}
