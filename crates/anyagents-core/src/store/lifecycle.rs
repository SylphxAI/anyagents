//! Journal of lifecycle commands (ADR 0006): spawn, pause, resume, reset,
//! archive and delete. Each op moves forward through
//! `intent -> quiesce -> stop -> commit -> effects -> done`; before `commit` it
//! may instead end `rolled_back`. One unfinished op per agent, and an
//! unfinished spawn reserves its agent name. Also the schedule tick
//! coalescing counters, which only the owning agent's actor writes.

use std::fmt;
use std::str::FromStr;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use super::{AgentId, Error, Store, agent_by_id, now_ms};

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

fn conversion(index: usize, err: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(err))
}

fn op_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LifecycleOp> {
    let agent: String = row.get(1)?;
    let kind: String = row.get(3)?;
    let phase: String = row.get(4)?;
    let evidence: String = row.get(7)?;
    Ok(LifecycleOp {
        op_id: row.get(0)?,
        agent_id: AgentId::parse(&agent).map_err(|err| conversion(1, err))?,
        agent_name: row.get(2)?,
        kind: kind.parse().map_err(|err| conversion(3, err))?,
        phase: phase.parse().map_err(|err| conversion(4, err))?,
        actor: row.get(5)?,
        reason: row.get(6)?,
        evidence: serde_json::from_str(&evidence).map_err(|err| conversion(7, err))?,
        error: row.get(8)?,
        started_at_ms: row.get(9)?,
        updated_at_ms: row.get(10)?,
        finished_at_ms: row.get(11)?,
    })
}

fn op_by_id(conn: &Connection, op_id: &str) -> Result<LifecycleOp, Error> {
    conn.query_row(
        &format!("SELECT {OP_COLS} FROM lifecycle_ops WHERE op_id = ?1"),
        [op_id],
        op_from_row,
    )
    .optional()?
    .ok_or_else(|| Error::Message(format!("lifecycle op `{op_id}` not found")))
}

fn active_op_for(conn: &Connection, agent_id: &AgentId) -> Result<Option<LifecycleOp>, Error> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {OP_COLS} FROM lifecycle_ops
                 WHERE agent_id = ?1 AND phase NOT IN ('done', 'rolled_back')"
            ),
            [agent_id.as_str()],
            op_from_row,
        )
        .optional()?)
}

/// The op id of an unfinished spawn that reserves `name`, if any.
pub(super) fn spawn_reservation(conn: &Connection, name: &str) -> Result<Option<String>, Error> {
    Ok(conn
        .query_row(
            "SELECT op_id FROM lifecycle_ops
             WHERE agent_name = ?1 AND kind = 'spawn' AND phase NOT IN ('done', 'rolled_back')",
            [name],
            |row| row.get(0),
        )
        .optional()?)
}

fn evidence_entry(phase: LifecyclePhase, at_ms: i64, evidence: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "phase": phase.as_str(), "at_ms": at_ms, "evidence": evidence })
}

impl Store {
    fn lifecycle_write<T>(
        &self,
        mut op: impl FnMut(&Transaction<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.write_with_retry(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let out = op(&tx)?;
            tx.commit()?;
            Ok(out)
        })
    }

    /// Record a lifecycle command's intent. A spawn reserves `agent_name`: it is
    /// refused when an agent or another unfinished spawn already has it. Any
    /// other kind needs the agent to exist. One unfinished op per agent.
    pub fn begin_lifecycle_op(&self, new: &NewLifecycleOp) -> Result<LifecycleOp, Error> {
        if new.actor.trim().is_empty() || new.reason.trim().is_empty() || new.agent_name.trim().is_empty() {
            return Err(Error::Message("a lifecycle op needs an agent name, an actor and a reason".into()));
        }
        self.lifecycle_write(|tx| {
            if new.kind == LifecycleKind::Spawn {
                let taken: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM agents WHERE name = ?1 OR id = ?2)",
                    params![new.agent_name, new.agent_id.as_str()],
                    |row| row.get(0),
                )?;
                if taken {
                    return Err(Error::AgentExists(new.agent_name.clone()));
                }
                if spawn_reservation(tx, &new.agent_name)?.is_some() {
                    return Err(Error::NameReserved(new.agent_name.clone()));
                }
            } else {
                agent_by_id(tx, &new.agent_id)?;
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
            tx.execute(
                &format!("INSERT INTO lifecycle_ops ({OP_COLS}) VALUES (?1, ?2, ?3, ?4, 'intent', ?5, ?6, ?7, NULL, ?8, ?8, NULL)"),
                params![
                    op_id,
                    new.agent_id.as_str(),
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
        self.lifecycle_write(|tx| {
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
            tx.execute(
                "UPDATE lifecycle_ops SET phase = ?2, evidence = ?3, updated_at_ms = ?4,
                     finished_at_ms = CASE WHEN ?2 IN ('done', 'rolled_back') THEN ?4 ELSE NULL END
                 WHERE op_id = ?1",
                params![op_id, to.as_str(), serde_json::Value::Array(entries).to_string(), now],
            )?;
            op_by_id(tx, op_id)
        })
    }

    /// Record the latest failure of an unfinished op without moving its phase.
    pub fn note_lifecycle_op_error(&self, op_id: &str, error: &str) -> Result<LifecycleOp, Error> {
        self.lifecycle_write(|tx| {
            let op = op_by_id(tx, op_id)?;
            if op.phase.is_finished() {
                return Err(Error::InvalidTransition {
                    machine: "lifecycle",
                    from: op.phase.to_string(),
                    to: "error".into(),
                });
            }
            tx.execute(
                "UPDATE lifecycle_ops SET error = ?2, updated_at_ms = ?3 WHERE op_id = ?1",
                params![op_id, error, now_ms()],
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
        let mut stmt = conn.prepare(&format!(
            "SELECT {OP_COLS} FROM lifecycle_ops WHERE phase NOT IN ('done', 'rolled_back')
             ORDER BY started_at_ms, op_id"
        ))?;
        let rows = stmt.query_map([], op_from_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// An agent's ops, newest first.
    pub fn lifecycle_ops_for(&self, agent_id: &AgentId, limit: usize) -> Result<Vec<LifecycleOp>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {OP_COLS} FROM lifecycle_ops WHERE agent_id = ?1
             ORDER BY started_at_ms DESC, op_id DESC LIMIT ?2"
        ))?;
        let rows = stmt.query_map(
            params![agent_id.as_str(), i64::try_from(limit).unwrap_or(i64::MAX)],
            op_from_row,
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
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
        self.lifecycle_write(|tx| {
            let updated = tx.execute(
                "UPDATE agent_schedules
                 SET pending_since_ms = COALESCE(pending_since_ms, ?2),
                     pending_count = pending_count + 1,
                     next_fire_at_ms = ?3
                 WHERE id = ?1 AND next_fire_at_ms = ?2",
                params![schedule_id, due_ms, next_fire_at_ms],
            )?;
            if updated == 0 {
                let actual: Option<Option<i64>> = tx
                    .query_row(
                        "SELECT next_fire_at_ms FROM agent_schedules WHERE id = ?1",
                        [schedule_id],
                        |row| row.get(0),
                    )
                    .optional()?;
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
    /// them all). `None` when nothing is pending.
    pub fn take_schedule_pending(&self, schedule_id: &str) -> Result<Option<SchedulePending>, Error> {
        self.lifecycle_write(|tx| {
            let pending = pending_of(tx, schedule_id)?;
            if pending.is_some() {
                tx.execute(
                    "UPDATE agent_schedules SET pending_since_ms = NULL, pending_count = 0 WHERE id = ?1",
                    [schedule_id],
                )?;
            }
            Ok(pending)
        })
    }
}

fn pending_of(conn: &Connection, schedule_id: &str) -> Result<Option<SchedulePending>, Error> {
    let row: Option<(Option<i64>, i64)> = conn
        .query_row(
            "SELECT pending_since_ms, pending_count FROM agent_schedules WHERE id = ?1",
            [schedule_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(match row {
        Some((Some(since_ms), count)) if count > 0 => Some(SchedulePending { since_ms, count }),
        _ => None,
    })
}
