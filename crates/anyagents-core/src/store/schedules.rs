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
        "UPDATE agent_schedules SET state = 'cancelled', next_fire_at_ms = NULL, external_timer_id = NULL
         WHERE agent_id = ?1 AND state = 'active'",
        &db_params![id.as_str()],
    )?;
    conn.db_execute(
        "DELETE FROM task_turn_contexts WHERE child_id = ?1 AND closed_delivery_seq IS NULL",
        &db_params![id.as_str()],
    )?;
    Ok(())
}

const RECURRING_JITTER_MIN_INTERVAL_MS: i64 = 15 * 60_000;
const RECURRING_JITTER_MAX_MS: i64 = 180_000;
const PHASE_CLOCK_SKEW_TOLERANCE_MS: i64 = 5_000;

impl Store {
    /// Deterministic stagger offset in `[0, min(interval_ms / 10, 180_000)]`
    /// for recurring (`every`) schedules `>= 15m`, or `0` for shorter intervals.
    pub fn recurring_schedule_jitter_ms(
        schedule_id: &str,
        agent_id: &str,
        interval_ms: i64,
    ) -> i64 {
        if interval_ms < RECURRING_JITTER_MIN_INTERVAL_MS {
            return 0;
        }
        let max_jitter = (interval_ms / 10).min(RECURRING_JITTER_MAX_MS);
        if max_jitter <= 0 {
            return 0;
        }
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for part in [schedule_id.as_bytes(), b":", agent_id.as_bytes()] {
            for &b in part {
                hash ^= u64::from(b);
                hash = hash.wrapping_mul(0x0100_0000_01b3);
            }
        }
        (hash % (max_jitter as u64).saturating_add(1)) as i64
    }

    /// Next recurring fire timestamp strictly after `now_ms`, with one-time
    /// deterministic phase alignment for `interval_ms >= 15m` and zero drift.
    pub fn next_recurring_fire_ms(
        schedule_id: &str,
        agent_id: &str,
        created_at_ms: i64,
        due_ms: i64,
        interval_ms: i64,
        now_ms: i64,
    ) -> Option<i64> {
        let base = next_recurring_slot_raw(due_ms, interval_ms, now_ms)?;
        let jitter = Self::recurring_schedule_jitter_ms(schedule_id, agent_id, interval_ms);
        if jitter <= 0 || due_ms < created_at_ms.saturating_sub(PHASE_CLOCK_SKEW_TOLERANCE_MS) {
            return Some(base);
        }
        phase_align_with_jitter(base, created_at_ms, interval_ms, jitter)
    }

    pub fn create_schedule(&self, input: NewSchedule) -> Result<AgentSchedule, Error> {
        self.create_schedule_inner(input, false)
    }

    pub fn create_schedule_jittered(&self, input: NewSchedule) -> Result<AgentSchedule, Error> {
        self.create_schedule_inner(input, true)
    }

    fn create_schedule_inner(
        &self,
        input: NewSchedule,
        apply_jitter: bool,
    ) -> Result<AgentSchedule, Error> {
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
            let initial_next_fire = |sched_id: &str| {
                if apply_jitter
                    && input.kind == "every"
                    && let Some(interval) = input.interval_ms.filter(|&iv| iv > 0)
                {
                    let j = Self::recurring_schedule_jitter_ms(
                        sched_id,
                        input.agent_id.as_str(),
                        interval,
                    );
                    Some(now.saturating_add(interval).saturating_add(j))
                } else {
                    input.next_fire_at_ms
                }
            };
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
                let next_fire = initial_next_fire(&existing_id);
                tx.db_execute(
                    "UPDATE agent_schedules
                     SET interval_ms = ?2, next_fire_at_ms = ?3, watch_baseline = COALESCE(?4, watch_baseline)
                     WHERE id = ?1",
                    &db_params![
                        existing_id,
                        input.interval_ms,
                        next_fire,
                        input.watch_baseline
                    ],
                )?;
                return schedule_by_id(tx, &existing_id);
            }
            let next_fire = initial_next_fire(&id);
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
                    next_fire,
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
        if !super::holds_store_writer() {
            let _ = self.repair_null_schedule_next_fire(now_ms());
        }
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

    /// Self-heal active `at` / `every` schedules whose `next_fire_at_ms` is `NULL`:
    /// - For `kind = 'at'`: parse RFC3339 `spec` and restore `next_fire_at_ms` only
    ///   when strictly in the future (`target_ms > now_ms`), never retroactively
    ///   firing already-missed past one-shot rows.
    /// - For `kind = 'every'` with `interval_ms > 0`: advance from
    ///   `last_fired_at_ms.unwrap_or(created_at_ms)` by multiples of `interval_ms`
    ///   until strictly `> now_ms`.
    pub fn repair_null_schedule_next_fire(&self, now_ms: i64) -> Result<usize, Error> {
        let candidates = load_null_schedule_candidates(&self.read_conn())?;
        let any_repairable = candidates
            .iter()
            .any(|row| compute_repaired_next_fire(row, now_ms).is_some());
        if !any_repairable {
            return Ok(0);
        }
        self.write_tx(|tx| repair_null_schedule_next_fire(tx, now_ms))
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
        Ok(self.cancel_schedules_returning(agent_id, id_or_all)?.len())
    }

    /// Cancel matching active schedules and return their pre-cancellation snapshots
    /// (including `external_timer_id` and `next_fire_at_ms`) so callers can cancel
    /// pending triggers on the active [`TimerBackend`].
    pub fn cancel_schedules_returning(
        &self,
        agent_id: Option<&AgentId>,
        id_or_all: &str,
    ) -> Result<Vec<AgentSchedule>, Error> {
        let target = id_or_all.trim();
        self.write_tx(|tx| {
            let matched = if target.eq_ignore_ascii_case("all") || target == "*" {
                match agent_id {
                    Some(aid) => tx.db_query_vec(
                        concat!(
                            "SELECT ",
                            schedule_cols!(),
                            " FROM agent_schedules WHERE agent_id = ?1 AND state = 'active'"
                        ),
                        &db_params![aid.as_str()],
                        schedule_from_row_db,
                    )?,
                    None => tx.db_query_vec(
                        concat!(
                            "SELECT ",
                            schedule_cols!(),
                            " FROM agent_schedules WHERE state = 'active'"
                        ),
                        &[],
                        schedule_from_row_db,
                    )?,
                }
            } else {
                match agent_id {
                    Some(aid) => tx.db_query_vec(
                        concat!(
                            "SELECT ",
                            schedule_cols!(),
                            " FROM agent_schedules WHERE id = ?1 AND agent_id = ?2 AND state = 'active'"
                        ),
                        &db_params![target, aid.as_str()],
                        schedule_from_row_db,
                    )?,
                    None => tx.db_query_vec(
                        concat!(
                            "SELECT ",
                            schedule_cols!(),
                            " FROM agent_schedules WHERE id = ?1 AND state = 'active'"
                        ),
                        &db_params![target],
                        schedule_from_row_db,
                    )?,
                }
            };
            if matched.is_empty() {
                return Ok(matched);
            }
            if target.eq_ignore_ascii_case("all") || target == "*" {
                match agent_id {
                    Some(aid) => tx.db_execute(
                        "UPDATE agent_schedules
                         SET state = 'cancelled', next_fire_at_ms = NULL, external_timer_id = NULL
                         WHERE agent_id = ?1 AND state = 'active'",
                        &db_params![aid.as_str()],
                    )?,
                    None => tx.db_execute(
                        "UPDATE agent_schedules
                         SET state = 'cancelled', next_fire_at_ms = NULL, external_timer_id = NULL
                         WHERE state = 'active'",
                        &[],
                    )?,
                };
            } else {
                match agent_id {
                    Some(aid) => tx.db_execute(
                        "UPDATE agent_schedules
                         SET state = 'cancelled', next_fire_at_ms = NULL, external_timer_id = NULL
                         WHERE id = ?1 AND agent_id = ?2 AND state = 'active'",
                        &db_params![target, aid.as_str()],
                    )?,
                    None => tx.db_execute(
                        "UPDATE agent_schedules
                         SET state = 'cancelled', next_fire_at_ms = NULL, external_timer_id = NULL
                         WHERE id = ?1 AND state = 'active'",
                        &db_params![target],
                    )?,
                };
            }
            Ok(matched)
        })
    }

    /// Persist or clear the external timer/run identifier for a schedule.
    pub fn set_schedule_external_timer_id(
        &self,
        id: &str,
        external_timer_id: Option<&str>,
    ) -> Result<(), Error> {
        let cleaned = external_timer_id.map(str::trim).filter(|s| !s.is_empty());
        self.write_tx(|tx| {
            tx.db_execute(
                "UPDATE agent_schedules SET external_timer_id = ?2 WHERE id = ?1",
                &db_params![id.trim(), cleaned],
            )?;
            Ok(())
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
        external_timer_id: row.get(12)?,
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

fn next_recurring_slot_raw(due: i64, interval: i64, now: i64) -> Option<i64> {
    if interval <= 0 || due > now {
        return None;
    }
    let interval = i128::from(interval);
    let elapsed = i128::from(now) - i128::from(due);
    i64::try_from(i128::from(now) + interval - elapsed % interval).ok()
}

fn phase_align_with_jitter(base: i64, created: i64, interval: i64, jitter: i64) -> Option<i64> {
    if jitter <= 0 || interval <= 0 {
        return Some(base);
    }
    let (iv, b, c, j, tol) = (
        i128::from(interval),
        i128::from(base),
        i128::from(created),
        i128::from(jitter),
        i128::from(PHASE_CLOCK_SKEW_TOLERANCE_MS),
    );
    let off = (b - c).rem_euclid(iv);
    if off > tol && off < iv - tol {
        return Some(base);
    }
    let shift = (c + j - b).rem_euclid(iv);
    i64::try_from(b + if shift <= j + tol { shift } else { 0 }).ok()
}

struct NullScheduleCandidate {
    id: String,
    agent_id: String,
    kind: String,
    spec: String,
    interval_ms: Option<i64>,
    last_fired_at_ms: Option<i64>,
    created_at_ms: i64,
}

fn load_null_schedule_candidates(
    conn: &(impl DbExec + ?Sized),
) -> Result<Vec<NullScheduleCandidate>, Error> {
    conn.db_query_vec(
        "SELECT id, agent_id, kind, spec, interval_ms, last_fired_at_ms, created_at_ms
         FROM agent_schedules
         WHERE state = 'active'
           AND next_fire_at_ms IS NULL
           AND kind IN ('at', 'every')",
        &[],
        |row| {
            Ok(NullScheduleCandidate {
                id: row.get(0)?,
                agent_id: row.get(1)?,
                kind: row.get(2)?,
                spec: row.get(3)?,
                interval_ms: row.get(4)?,
                last_fired_at_ms: row.get(5)?,
                created_at_ms: row.get(6)?,
            })
        },
    )
}

fn compute_repaired_next_fire(row: &NullScheduleCandidate, now_ms: i64) -> Option<i64> {
    match row.kind.as_str() {
        "at" => {
            let target_ms = chrono::DateTime::parse_from_rfc3339(row.spec.trim())
                .ok()?
                .timestamp_millis();
            (target_ms > now_ms).then_some(target_ms)
        }
        "every" => {
            let interval = row.interval_ms.filter(|&iv| iv > 0)?;
            let jitter = Store::recurring_schedule_jitter_ms(&row.id, &row.agent_id, interval);
            let base = row
                .last_fired_at_ms
                .unwrap_or_else(|| row.created_at_ms.saturating_add(jitter));
            let next = if base > now_ms {
                base
            } else {
                let steps = (now_ms.saturating_sub(base) / interval).saturating_add(1);
                base.saturating_add(steps.saturating_mul(interval))
            };
            if row.last_fired_at_ms.is_some() && jitter > 0 {
                phase_align_with_jitter(next, row.created_at_ms, interval, jitter)
            } else {
                Some(next)
            }
        }
        _ => None,
    }
}

pub(crate) fn repair_null_schedule_next_fire(
    tx: &(impl DbExec + ?Sized),
    now_ms: i64,
) -> Result<usize, Error> {
    let candidates = load_null_schedule_candidates(tx)?;
    let mut repaired = 0usize;
    for row in candidates {
        if let Some(next_ms) = compute_repaired_next_fire(&row, now_ms) {
            repaired += tx.db_execute(
                "UPDATE agent_schedules
                 SET next_fire_at_ms = ?2
                 WHERE id = ?1 AND state = 'active' AND next_fire_at_ms IS NULL",
                &db_params![row.id, next_ms],
            )?;
        }
    }
    Ok(repaired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;

    #[test]
    fn repair_null_schedule_next_fire_heals_future_at_and_every_without_firing_past_at() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store.create_agent(NewAgent::new("sched", "codex")).unwrap();

        let now = chrono::DateTime::parse_from_rfc3339("2026-11-20T01:00:00Z")
            .unwrap()
            .timestamp_millis();
        let future_spec = "2026-11-20T02:30:00Z";
        let future_ms = chrono::DateTime::parse_from_rfc3339(future_spec)
            .unwrap()
            .timestamp_millis();
        let past_spec = "2026-11-19T20:00:00Z";

        let s_future_at = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: future_spec.into(),
                prompt: "future one-shot".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
        let s_past_at = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: past_spec.into(),
                prompt: "missed one-shot".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
        let s_every = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "every".into(),
                spec: "5m".into(),
                prompt: "recurring check".into(),
                interval_ms: Some(300_000),
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
        // Anchor the `every` schedule to a deterministic historical `created_at_ms`
        // 12 minutes before `now` so the repaired next fire lands 3 minutes after `now`.
        let anchor = now - 720_000;
        store
            .write_tx(|tx| {
                tx.db_execute(
                    "UPDATE agent_schedules SET created_at_ms = ?2, last_fired_at_ms = NULL WHERE id = ?1",
                    &db_params![s_every.id.as_str(), anchor],
                )?;
                Ok(())
            })
            .unwrap();

        let repaired = store.repair_null_schedule_next_fire(now).unwrap();
        assert_eq!(repaired, 2);

        let healed_future = store.get_schedule(&s_future_at.id).unwrap().unwrap();
        assert_eq!(healed_future.next_fire_at_ms, Some(future_ms));

        let unhealed_past = store.get_schedule(&s_past_at.id).unwrap().unwrap();
        assert_eq!(
            unhealed_past.next_fire_at_ms, None,
            "missed past `at` schedule must never be retroactively armed"
        );

        let healed_every = store.get_schedule(&s_every.id).unwrap().unwrap();
        assert_eq!(healed_every.next_fire_at_ms, Some(anchor + 3 * 300_000));

        // Second pass is an idempotent no-op.
        assert_eq!(store.repair_null_schedule_next_fire(now).unwrap(), 0);
    }

    #[test]
    fn recurring_schedule_jitter_is_zero_under_15m_and_bounded_deterministic_for_hourly() {
        let hour = 3_600_000i64;
        for short in [1_000, 60_000, 300_000, 15 * 60_000 - 1] {
            assert_eq!(
                Store::recurring_schedule_jitter_ms("sch_a", "agt_1", short),
                0
            );
        }
        assert!((0..=90_000).contains(&Store::recurring_schedule_jitter_ms(
            "sch_a",
            "agt_1",
            15 * 60_000
        )));
        let ja = Store::recurring_schedule_jitter_ms("sch_01aaaaaaaa", "agt_a", hour);
        let jb = Store::recurring_schedule_jitter_ms("sch_01bbbbbbbb", "agt_b", hour);
        assert!((0..=180_000).contains(&ja) && (0..=180_000).contains(&jb) && ja != jb);
        assert_eq!(
            ja,
            Store::recurring_schedule_jitter_ms("sch_01aaaaaaaa", "agt_a", hour)
        );

        let created = 1_795_000_000_000i64;
        let due1 = created + hour;
        let n1 =
            Store::next_recurring_fire_ms("sch_01aaaaaaaa", "agt_a", created, due1, hour, due1)
                .unwrap();
        assert_eq!(n1, due1 + hour + ja);
        let n2 = Store::next_recurring_fire_ms("sch_01aaaaaaaa", "agt_a", created, n1, hour, n1)
            .unwrap();
        assert_eq!(n2, n1 + hour);
    }
}
