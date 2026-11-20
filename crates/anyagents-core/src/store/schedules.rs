//! Agent schedules, due-tick claiming, and watch observation persistence.

use std::fs::File;
use std::io::Read;

use super::helpers::{agent_by_id, hex};
use super::mailbox::cursor;
use super::types::{AgentSchedule, Incoming, NewSchedule};
use super::{Store, now_ms};
use crate::db::{DbExec, DbExecExt, DbRow};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

/// Run in the owner's archive transaction, never in a second Store write.
/// Cancelled history and coalesced counts remain available for inspection.
pub(super) fn cancel_archived_schedules(
    conn: &(impl DbExec + ?Sized),
    id: &AgentId,
) -> Result<(), Error> {
    conn.db_execute(
        "UPDATE agent_schedules SET state = 'cancelled', next_fire_at_ms = NULL
         WHERE agent_id = ?1 AND state = 'active'",
        &db_params![id.as_str()],
    )?;
    conn.db_execute(
        "DELETE FROM task_turn_contexts WHERE child_id = ?1 AND closed_delivery_seq IS NULL",
        &db_params![id.as_str()],
    )?;
    Ok(())
}

impl Store {
    pub fn create_schedule(&self, input: NewSchedule) -> Result<AgentSchedule, Error> {
        let now = now_ms();
        let mut rand_bytes = [0u8; 8];
        if let Ok(mut f) = File::open("/dev/urandom") {
            let _ = f.read_exact(&mut rand_bytes);
        }
        let id = format!("sch_{:012x}{}", now, hex(&rand_bytes));
        self.write_tx(|tx| {
            let owner = agent_by_id(tx, &input.agent_id)?;
            if owner.state == "archived" {
                return Err(Error::Precondition {
                    machine: "schedule",
                    reason: format!("agent {} is archived", input.agent_id),
                });
            }
            // Deduplicate identical active recurring/watch schedules for the same agent.
            if (input.kind == "every" || input.kind == "watch")
                && let Some(existing_id) = tx.db_query_opt(
                    "SELECT id FROM agent_schedules
                     WHERE agent_id = ?1 AND kind = ?2 AND spec = ?3 AND prompt = ?4 AND state = 'active'
                     LIMIT 1",
                    &db_params![input.agent_id.as_str(), input.kind, input.spec, input.prompt],
                    |row| row.get::<String>(0),
                )?
            {
                tx.db_execute(
                    "UPDATE agent_schedules
                     SET interval_ms = ?2, next_fire_at_ms = ?3, watch_baseline = COALESCE(?4, watch_baseline)
                     WHERE id = ?1",
                    &db_params![
                        existing_id,
                        input.interval_ms,
                        input.next_fire_at_ms,
                        input.watch_baseline
                    ],
                )?;
                return schedule_by_id(tx, &existing_id);
            }
            tx.db_execute(
                "INSERT INTO agent_schedules (
                    id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms,
                    last_fired_at_ms, watch_baseline, last_observed, state, created_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?8, 'active', ?9)",
                &db_params![
                    id,
                    input.agent_id.as_str(),
                    input.kind,
                    input.spec,
                    input.prompt,
                    input.interval_ms,
                    input.next_fire_at_ms,
                    input.watch_baseline,
                    now,
                ],
            )?;
            schedule_by_id(tx, &id)
        })
    }

    pub fn list_schedules(
        &self,
        agent_id: Option<&AgentId>,
        active_only: bool,
    ) -> Result<Vec<AgentSchedule>, Error> {
        let conn = self.read_conn();
        let (sql, params) = match (agent_id, active_only) {
            (Some(aid), true) => (
                concat!(
                    "SELECT ",
                    schedule_cols!(),
                    " FROM agent_schedules WHERE agent_id = ?1 AND state = 'active'
                     ORDER BY created_at_ms ASC, id ASC"
                ),
                db_params![aid.as_str()],
            ),
            (Some(aid), false) => (
                concat!(
                    "SELECT ",
                    schedule_cols!(),
                    " FROM agent_schedules WHERE agent_id = ?1
                     ORDER BY created_at_ms ASC, id ASC"
                ),
                db_params![aid.as_str()],
            ),
            (None, true) => (
                concat!(
                    "SELECT ",
                    schedule_cols!(),
                    " FROM agent_schedules WHERE state = 'active'
                     ORDER BY created_at_ms ASC, id ASC"
                ),
                Vec::new(),
            ),
            (None, false) => (
                concat!(
                    "SELECT ",
                    schedule_cols!(),
                    " FROM agent_schedules ORDER BY created_at_ms ASC, id ASC"
                ),
                Vec::new(),
            ),
        };
        conn.db_query_vec(sql, &params, schedule_from_row_db)
    }

    pub fn get_schedule(&self, id: &str) -> Result<Option<AgentSchedule>, Error> {
        let conn = self.read_conn();
        conn.db_query_opt(
            concat!(
                "SELECT ",
                schedule_cols!(),
                " FROM agent_schedules WHERE id = ?1"
            ),
            &db_params![id.trim()],
            schedule_from_row_db,
        )
    }

    pub fn cancel_schedule(
        &self,
        agent_id: Option<&AgentId>,
        id_or_all: &str,
    ) -> Result<usize, Error> {
        let target = id_or_all.trim();
        self.write_tx(|tx| {
            let count = if target.eq_ignore_ascii_case("all") || target == "*" {
                match agent_id {
                    Some(aid) => tx.db_execute(
                        "UPDATE agent_schedules
                         SET state = 'cancelled', next_fire_at_ms = NULL
                         WHERE agent_id = ?1 AND state = 'active'",
                        &db_params![aid.as_str()],
                    )?,
                    None => tx.db_execute(
                        "UPDATE agent_schedules
                         SET state = 'cancelled', next_fire_at_ms = NULL
                         WHERE state = 'active'",
                        &[],
                    )?,
                }
            } else {
                match agent_id {
                    Some(aid) => tx.db_execute(
                        "UPDATE agent_schedules
                         SET state = 'cancelled', next_fire_at_ms = NULL
                         WHERE id = ?1 AND agent_id = ?2 AND state = 'active'",
                        &db_params![target, aid.as_str()],
                    )?,
                    None => tx.db_execute(
                        "UPDATE agent_schedules
                         SET state = 'cancelled', next_fire_at_ms = NULL
                         WHERE id = ?1 AND state = 'active'",
                        &db_params![target],
                    )?,
                }
            };
            Ok(count)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim_schedule_tick(
        &self,
        id: &str,
        expected_next_fire_at_ms: Option<i64>,
        new_state: &str,
        new_next_fire_at_ms: Option<i64>,
        fired_at_ms: Option<i64>,
        watch_baseline: Option<&str>,
        last_observed: Option<&str>,
    ) -> Result<bool, Error> {
        self.write_tx(|tx| {
            claim_schedule_tick_on(
                tx,
                id,
                expected_next_fire_at_ms,
                new_state,
                new_next_fire_at_ms,
                fired_at_ms,
                watch_baseline,
                last_observed,
            )
        })
    }

    /// Atomically claim a timer/watch transition and retain its mailbox wake.
    /// The wake enters the mailbox like any other message; a busy, paused or
    /// quota-blocked owner receives it through normal mailbox delivery. The
    /// caller holds the agent lifecycle lock and checks external retirement
    /// markers. A failed index/mail insert rolls back the claim for retry.
    #[allow(clippy::too_many_arguments)]
    pub fn claim_schedule_tick_with_wake(
        &self,
        id: &str,
        expected_next_fire_at_ms: Option<i64>,
        new_state: &str,
        new_next_fire_at_ms: Option<i64>,
        fired_at_ms: Option<i64>,
        watch_baseline: Option<&str>,
        last_observed: Option<&str>,
        agent_id: &AgentId,
        incoming: Incoming,
    ) -> Result<bool, Error> {
        let outcome = self.receive_with_admission(agent_id, incoming, true, |conn| {
            let eligible: bool = conn.db_query_one(
                "SELECT EXISTS(SELECT 1 FROM agent_schedules s JOIN agents a ON a.id=s.agent_id
                 WHERE s.id=?1 AND s.agent_id=?2 AND a.state <> 'archived')",
                &db_params![id, agent_id.as_str()],
                |r| r.get(0),
            )?;
            if !eligible {
                return Ok(false);
            }
            claim_schedule_tick_on(
                conn,
                id,
                expected_next_fire_at_ms,
                new_state,
                new_next_fire_at_ms,
                fired_at_ms,
                watch_baseline,
                last_observed,
            )
        })?;
        Ok(outcome.is_some())
    }

    /// Whether the agent's mailbox still holds a message from `sender` that the
    /// harness has not taken.
    pub fn has_undelivered_message_from(
        &self,
        agent_id: &AgentId,
        sender: &str,
    ) -> Result<bool, Error> {
        let conn = self.read_conn();
        let delivered = cursor(&conn, agent_id)?;
        conn.db_query_one(
            "SELECT EXISTS(
               SELECT 1 FROM messages
               WHERE agent_id = ?1 AND seq > ?2 AND sender = ?3
             )",
            &db_params![agent_id.as_str(), delivered, sender],
            |row| row.get(0),
        )
    }

    /// Reopen GitHub PR watches that an earlier release retired when checks
    /// completed, so they keep watching until the PR is merged or closed.
    pub fn restore_checks_completed_pr_watches(&self) -> Result<usize, Error> {
        self.write_tx(|tx| {
            let updated = tx.db_execute(
                "UPDATE agent_schedules
                 SET state = 'active',
                     watch_baseline = 'checks_completed'
                 WHERE kind = 'watch'
                   AND state = 'fired'
                   AND EXISTS (SELECT 1 FROM agents a
                               WHERE a.id = agent_schedules.agent_id
                                 AND a.state <> 'archived')
                   AND last_observed = 'checks_completed'
                   AND (
                       spec LIKE 'github:pr:%'
                       OR spec LIKE 'gh:pr:%'
                       OR spec LIKE 'pr:%'
                       OR spec LIKE 'https://github.com/%/pull/%'
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM agent_schedules active_s
                       WHERE active_s.agent_id = agent_schedules.agent_id
                         AND active_s.spec = agent_schedules.spec
                         AND active_s.state = 'active'
                   )",
                &[],
            )?;
            Ok(updated)
        })
    }

    pub fn update_schedule_watch_observation(
        &self,
        id: &str,
        watch_baseline: Option<&str>,
        last_observed: Option<&str>,
    ) -> Result<(), Error> {
        // Coalesce unchanged observations: a schedule pass would otherwise
        // write a WAL frame per watch even when both fields already hold
        // those values. The reader decides; only a change takes the writer.
        let cur: Option<(Option<String>, Option<String>)> = self.read_conn().db_query_opt(
            "SELECT watch_baseline, last_observed FROM agent_schedules WHERE id = ?1 AND state = 'active'",
            &db_params![id],
            |r| Ok((r.get::<Option<String>>(0)?, r.get::<Option<String>>(1)?)),
        )?;
        let (cur_baseline, cur_observed) = match cur {
            Some(v) => v,
            None => return Ok(()),
        };
        let new_baseline = match watch_baseline {
            Some(v) => Some(v.to_string()),
            None => cur_baseline.clone(),
        };
        let new_observed = match last_observed {
            Some(v) => Some(v.to_string()),
            None => cur_observed.clone(),
        };
        if new_baseline == cur_baseline && new_observed == cur_observed {
            return Ok(());
        }
        self.write_tx(|tx| {
            tx.db_execute(
                "UPDATE agent_schedules
                 SET watch_baseline = COALESCE(?2, watch_baseline),
                     last_observed = COALESCE(?3, last_observed)
                 WHERE id = ?1 AND state = 'active'",
                &db_params![id, watch_baseline, last_observed],
            )?;
            Ok(())
        })
    }
}

fn schedule_by_id(conn: &dyn DbExec, id: &str) -> Result<AgentSchedule, Error> {
    let found = conn.db_query_opt(
        concat!(
            "SELECT ",
            schedule_cols!(),
            " FROM agent_schedules WHERE id = ?1"
        ),
        &db_params![id],
        schedule_from_row_db,
    )?;
    found.ok_or_else(|| Error::Message(format!("schedule not found: {id}")))
}

fn schedule_from_row_db(row: &DbRow<'_>) -> Result<AgentSchedule, Error> {
    let agent_id = row.parse_str(1)?;
    Ok(AgentSchedule {
        id: row.get(0)?,
        agent_id,
        kind: row.get(2)?,
        spec: row.get(3)?,
        prompt: row.get(4)?,
        interval_ms: row.get(5)?,
        next_fire_at_ms: row.get(6)?,
        last_fired_at_ms: row.get(7)?,
        watch_baseline: row.get(8)?,
        last_observed: row.get(9)?,
        state: row.get(10)?,
        created_at_ms: row.get(11)?,
    })
}

#[allow(clippy::too_many_arguments)]
fn claim_schedule_tick_on(
    conn: &dyn DbExec,
    id: &str,
    expected_next_fire_at_ms: Option<i64>,
    new_state: &str,
    new_next_fire_at_ms: Option<i64>,
    fired_at_ms: Option<i64>,
    watch_baseline: Option<&str>,
    last_observed: Option<&str>,
) -> Result<bool, Error> {
    let updated = match expected_next_fire_at_ms {
        Some(expected) => conn.db_execute(
            "UPDATE agent_schedules
                 SET state = ?3,
                     next_fire_at_ms = ?4,
                     last_fired_at_ms = COALESCE(?5, last_fired_at_ms),
                     watch_baseline = COALESCE(?6, watch_baseline),
                     last_observed = COALESCE(?7, last_observed)
                 WHERE id = ?1 AND state = 'active' AND next_fire_at_ms = ?2",
            &db_params![
                id,
                expected,
                new_state,
                new_next_fire_at_ms,
                fired_at_ms,
                watch_baseline,
                last_observed
            ],
        )?,
        None => conn.db_execute(
            "UPDATE agent_schedules
                 SET state = ?2,
                     next_fire_at_ms = ?3,
                     last_fired_at_ms = COALESCE(?4, last_fired_at_ms),
                     watch_baseline = COALESCE(?5, watch_baseline),
                     last_observed = COALESCE(?6, last_observed)
                 WHERE id = ?1 AND state = 'active'",
            &db_params![
                id,
                new_state,
                new_next_fire_at_ms,
                fired_at_ms,
                watch_baseline,
                last_observed
            ],
        )?,
    };
    Ok(updated == 1)
}
