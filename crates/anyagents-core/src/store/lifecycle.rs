//! Journal of lifecycle commands (ADR 0006): spawn, pause, resume, reset,
//! archive and delete. Each op moves forward through
//! `intent -> quiesce -> stop -> commit -> effects -> done`; before `commit` it
//! may instead end `rolled_back`. One unfinished op per agent, and an
//! unfinished spawn reserves its agent name. Also the schedule tick
//! coalescing counters, which only the owning agent's actor writes.

use std::fmt;
use std::str::FromStr;

use super::{AgentId, Error, Store, agent_exists, now_ms};
use crate::db::{DbExec, DbExecExt as _, DbRow, db_params};

macro_rules! ordered_enum {
    ($(#[$meta:meta])* $name:ident, $what:literal { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name { $($variant),+ }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text),+ }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = Error;
            fn from_str(text: &str) -> Result<Self, Error> {
                match text {
                    $($text => Ok($name::$variant),)+
                    other => Err(Error::Message(format!("unknown {} `{other}`", $what))),
                }
            }
        }
    };
}

ordered_enum!(
    LifecycleKind, "lifecycle kind" {
        Spawn => "spawn",
        Pause => "pause",
        Resume => "resume",
        Reset => "reset",
        Archive => "archive",
        Delete => "delete",
    }
);

ordered_enum!(
    /// Phases in their only order. `RolledBack` is terminal and reachable only
    /// from a phase before `Commit`.
    LifecyclePhase, "lifecycle phase" {
        Intent => "intent",
        Quiesce => "quiesce",
        Stop => "stop",
        Commit => "commit",
        Effects => "effects",
        Done => "done",
        RolledBack => "rolled_back",
    }
);

impl LifecyclePhase {
    pub fn is_finished(self) -> bool {
        matches!(self, LifecyclePhase::Done | LifecyclePhase::RolledBack)
    }

    pub fn allows(self, to: LifecyclePhase) -> bool {
        match to {
            LifecyclePhase::RolledBack => self < LifecyclePhase::Commit,
            _ => !self.is_finished() && to > self,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLifecycleOp {
    pub kind: LifecycleKind,
    pub agent_id: AgentId,
    pub agent_name: String,
    pub actor: String,
    pub reason: String,
    pub evidence: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LifecycleOp {
    pub op_id: String,
    pub agent_id: AgentId,
    pub agent_name: String,
    pub kind: LifecycleKind,
    pub phase: LifecyclePhase,
    pub actor: String,
    pub reason: String,
    /// One entry per phase change: `{phase, at_ms, evidence}`.
    pub evidence: Vec<serde_json::Value>,
    pub error: Option<String>,
    pub started_at_ms: i64,
    pub updated_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

/// Ticks of one schedule that came due while its agent could not take them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedulePending {
    pub since_ms: i64,
    pub count: i64,
}

const OP_COLS: &str = "op_id, agent_id, agent_name, kind, phase, actor, reason, evidence, error, \
     started_at_ms, updated_at_ms, finished_at_ms";

fn op_from_row(row: &DbRow<'_>) -> Result<LifecycleOp, Error> {
    Ok(LifecycleOp {
        op_id: row.get(0)?,
        agent_id: row.get(1)?,
        agent_name: row.get(2)?,
        kind: row.parse_str(3)?,
        phase: row.parse_str(4)?,
        actor: row.get(5)?,
        reason: row.get(6)?,
        evidence: row.json(7)?,
        error: row.get(8)?,
        started_at_ms: row.get(9)?,
        updated_at_ms: row.get(10)?,
        finished_at_ms: row.get(11)?,
    })
}

fn op_by_id(conn: &(impl DbExec + ?Sized), op_id: &str) -> Result<LifecycleOp, Error> {
    conn.db_query_opt(
        &format!("SELECT {OP_COLS} FROM lifecycle_ops WHERE op_id = ?1"),
        &db_params![op_id],
        op_from_row,
    )?
    .ok_or_else(|| Error::Message(format!("lifecycle op `{op_id}` not found")))
}

fn active_op_for(
    conn: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
) -> Result<Option<LifecycleOp>, Error> {
    conn.db_query_opt(
        &format!(
            "SELECT {OP_COLS} FROM lifecycle_ops
             WHERE agent_id = ?1 AND phase NOT IN ('done', 'rolled_back')"
        ),
        &db_params![agent_id],
        op_from_row,
    )
}

/// The op id of an unfinished spawn that reserves `name`, if any.
pub(super) fn spawn_reservation(
    conn: &(impl DbExec + ?Sized),
    name: &str,
) -> Result<Option<String>, Error> {
    conn.db_query_opt(
        "SELECT op_id FROM lifecycle_ops
         WHERE agent_name = ?1 AND kind = 'spawn' AND phase NOT IN ('done', 'rolled_back')",
        &db_params![name],
        |row| row.get(0),
    )
}

fn evidence_entry(
    phase: LifecyclePhase,
    at_ms: i64,
    evidence: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({ "phase": phase.as_str(), "at_ms": at_ms, "evidence": evidence })
}

impl Store {
    /// Record a lifecycle command's intent. A spawn reserves `agent_name`: it is
    /// refused when an agent or another unfinished spawn already has it. Any
    /// other kind needs the agent to exist. One unfinished op per agent.
    pub fn begin_lifecycle_op(&self, new: &NewLifecycleOp) -> Result<LifecycleOp, Error> {
        if new.actor.trim().is_empty()
            || new.reason.trim().is_empty()
            || new.agent_name.trim().is_empty()
        {
            return Err(Error::Message(
                "a lifecycle op needs an agent name, an actor and a reason".into(),
            ));
        }
        self.write_tx(|tx| {
            if new.kind == LifecycleKind::Spawn {
                let taken: bool = tx.db_query_one(
                    "SELECT EXISTS(SELECT 1 FROM agents WHERE name = ?1 OR id = ?2)",
                    &db_params![new.agent_name, new.agent_id],
                    |row| row.get(0),
                )?;
                if taken {
                    return Err(Error::AgentExists(new.agent_name.clone()));
                }
                if spawn_reservation(tx, &new.agent_name)?.is_some() {
                    return Err(Error::NameReserved(new.agent_name.clone()));
                }
            } else {
                agent_exists(tx, &new.agent_id)?;
            }
            if let Some(active) = active_op_for(tx, &new.agent_id)? {
                return Err(Error::LifecycleOpActive {
                    agent: new.agent_id.to_string(),
                    op_id: active.op_id,
                });
            }
            let op_id = format!("lop_{}", uuid::Uuid::now_v7().simple());
            let now = now_ms();
            let evidence = serde_json::Value::Array(vec![evidence_entry(
                LifecyclePhase::Intent,
                now,
                new.evidence.clone(),
            )]);
            tx.db_execute(
                &format!("INSERT INTO lifecycle_ops ({OP_COLS}) VALUES (?1, ?2, ?3, ?4, 'intent', ?5, ?6, ?7, NULL, ?8, ?8, NULL)"),
                &db_params![
                    op_id,
                    new.agent_id,
                    new.agent_name,
                    new.kind.as_str(),
                    new.actor,
                    new.reason,
                    evidence.to_string(),
                    now
                ],
            )?;
            op_by_id(tx, &op_id)
        })
    }

    /// Move an op from `from` to `to`, appending `evidence` for that phase.
    pub fn advance_lifecycle_op(
        &self,
        op_id: &str,
        from: LifecyclePhase,
        to: LifecyclePhase,
        evidence: serde_json::Value,
    ) -> Result<LifecycleOp, Error> {
        self.write_tx(|tx| {
            let op = op_by_id(tx, op_id)?;
            if op.phase != from {
                return Err(Error::StateConflict {
                    machine: "lifecycle",
                    expected: from.to_string(),
                    actual: op.phase.to_string(),
                });
            }
            if !from.allows(to) {
                return Err(Error::InvalidTransition {
                    machine: "lifecycle",
                    from: from.to_string(),
                    to: to.to_string(),
                });
            }
            let now = now_ms();
            let mut entries = op.evidence.clone();
            entries.push(evidence_entry(to, now, evidence.clone()));
            tx.db_execute(
                "UPDATE lifecycle_ops SET phase = ?2, evidence = ?3, updated_at_ms = ?4,
                     finished_at_ms = CASE WHEN ?2 IN ('done', 'rolled_back') THEN ?4 ELSE NULL END
                 WHERE op_id = ?1",
                &db_params![
                    op_id,
                    to.as_str(),
                    serde_json::Value::Array(entries).to_string(),
                    now
                ],
            )?;
            op_by_id(tx, op_id)
        })
    }

    /// Record the latest failure of an unfinished op without moving its phase.
    pub fn note_lifecycle_op_error(&self, op_id: &str, error: &str) -> Result<LifecycleOp, Error> {
        self.write_tx(|tx| {
            let op = op_by_id(tx, op_id)?;
            if op.phase.is_finished() {
                return Err(Error::InvalidTransition {
                    machine: "lifecycle",
                    from: op.phase.to_string(),
                    to: "error".into(),
                });
            }
            tx.db_execute(
                "UPDATE lifecycle_ops SET error = ?2, updated_at_ms = ?3 WHERE op_id = ?1",
                &db_params![op_id, error, now_ms()],
            )?;
            op_by_id(tx, op_id)
        })
    }

    pub fn lifecycle_op(&self, op_id: &str) -> Result<LifecycleOp, Error> {
        op_by_id(&self.read_conn(), op_id)
    }

    /// Ops that a crash left unfinished, oldest first, for startup resume.
    pub fn unfinished_lifecycle_ops(&self) -> Result<Vec<LifecycleOp>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec(
            &format!(
                "SELECT {OP_COLS} FROM lifecycle_ops WHERE phase NOT IN ('done', 'rolled_back')
                 ORDER BY started_at_ms, op_id"
            ),
            &[],
            op_from_row,
        )
    }

    /// An agent's ops, newest first.
    pub fn lifecycle_ops_for(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> Result<Vec<LifecycleOp>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec(
            &format!(
                "SELECT {OP_COLS} FROM lifecycle_ops WHERE agent_id = ?1
                 ORDER BY started_at_ms DESC, op_id DESC LIMIT ?2"
            ),
            &db_params![agent_id, i64::try_from(limit).unwrap_or(i64::MAX)],
            op_from_row,
        )
    }

    /// Count one tick of `schedule_id` that came due at `due_ms` while its agent
    /// was blocked, and move its next fire time to `next_fire_at_ms`, in one
    /// commit. Refused unless the schedule is still due at `due_ms`, so a tick
    /// is never counted twice.
    pub fn coalesce_schedule_tick(
        &self,
        schedule_id: &str,
        due_ms: i64,
        next_fire_at_ms: i64,
    ) -> Result<SchedulePending, Error> {
        self.write_tx(|tx| {
            let updated = tx.db_execute(
                "UPDATE agent_schedules
                 SET pending_since_ms = COALESCE(pending_since_ms, ?2),
                     pending_count = pending_count + 1,
                     next_fire_at_ms = ?3
                 WHERE id = ?1 AND next_fire_at_ms = ?2 AND state = 'active'",
                &db_params![schedule_id, due_ms, next_fire_at_ms],
            )?;
            if updated == 0 {
                let actual: Option<Option<i64>> = tx.db_query_opt(
                    "SELECT next_fire_at_ms FROM agent_schedules WHERE id = ?1",
                    &db_params![schedule_id],
                    |row| row.get(0),
                )?;
                return Err(match actual {
                    None => Error::Message(format!("schedule `{schedule_id}` not found")),
                    Some(next) => Error::StateConflict {
                        machine: "schedule",
                        expected: format!("due@{due_ms}"),
                        actual: next.map_or_else(|| "unscheduled".into(), |n| format!("due@{n}")),
                    },
                });
            }
            pending_of(tx, schedule_id)?
                .ok_or_else(|| Error::Message("schedule pending count vanished".into()))
        })
    }

    pub fn schedule_pending(&self, schedule_id: &str) -> Result<Option<SchedulePending>, Error> {
        pending_of(&self.read_conn(), schedule_id)
    }

    /// Take and clear the coalesced ticks of `schedule_id` (one delivery covers
    /// them all). `None` when nothing is pending or the schedule was cancelled.
    /// Cancelled ticks remain available through `schedule_pending` as history.
    pub fn take_schedule_pending(
        &self,
        schedule_id: &str,
    ) -> Result<Option<SchedulePending>, Error> {
        self.write_tx(|tx| {
            let cancelled: bool = tx.db_query_one(
                "SELECT EXISTS(SELECT 1 FROM agent_schedules WHERE id = ?1 AND state = 'cancelled')",
                &db_params![schedule_id],
                |row| row.get(0),
            )?;
            if cancelled {
                return Ok(None);
            }
            let pending = pending_of(tx, schedule_id)?;
            if pending.is_some() {
                tx.db_execute(
                    "UPDATE agent_schedules SET pending_since_ms = NULL, pending_count = 0 WHERE id = ?1",
                    &db_params![schedule_id],
                )?;
            }
            Ok(pending)
        })
    }
}

fn pending_of(
    conn: &(impl DbExec + ?Sized),
    schedule_id: &str,
) -> Result<Option<SchedulePending>, Error> {
    let row: Option<(Option<i64>, i64)> = conn.db_query_opt(
        "SELECT pending_since_ms, pending_count FROM agent_schedules WHERE id = ?1",
        &db_params![schedule_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(match row {
        Some((Some(since_ms), count)) if count > 0 => Some(SchedulePending { since_ms, count }),
        _ => None,
    })
}
