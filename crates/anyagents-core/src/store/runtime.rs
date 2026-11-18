//! Explicit, persisted runtime state machines: the agent runtime, turns,
//! session leases and quota blocks.
//!
//! Every transition is a compare-and-set against the state the caller observed
//! and is checked against its transition table before anything is written. A
//! mismatch or a refused transition is a typed error and writes nothing. Legacy
//! readers stay correct: each runtime transition mirrors `agents.state`, and each
//! turn transition mirrors `turn_recovery`, in the same transaction.

use std::fmt;
use std::str::FromStr;

use super::{AgentId, Error, Store, agent_exists, cursor, now_ms};
use crate::db::{DbExec, DbExecExt as _, DbRow, db_params};

/// A process, identified so that a reused pid never inherits a lease: the pid
/// plus the kernel's start time for that pid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProcessIdentity {
    pub pid: i64,
    pub start_time: i64,
}

macro_rules! text_enum {
    ($(#[$meta:meta])* $name:ident, $machine:literal { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name { $($variant),+ }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

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
                    other => Err(Error::Message(format!("unknown {} state `{other}`", $machine))),
                }
            }
        }
    };
}

text_enum!(
    /// The process and session state of one agent.
    RuntimeState, "runtime" {
        Released => "released",
        Starting => "starting",
        Live => "live",
        Draining => "draining",
        Backoff => "backoff",
        QuotaBlocked => "quota_blocked",
        Paused => "paused",
        Faulted => "faulted",
        Archived => "archived",
    }
);

impl RuntimeState {
    /// The runtime transition table (ADR 0006).
    pub fn allows(self, to: RuntimeState) -> bool {
        use RuntimeState::*;
        match (self, to) {
            (Released, Starting) => true,
            (Starting, Live) => true,
            (Starting | Live, Backoff) => true,
            (Live, QuotaBlocked) => true,
            (Backoff | QuotaBlocked, Starting) => true,
            (Live, Released) => true,
            (Starting | Live | Backoff | QuotaBlocked, Draining) => true,
            (Draining, Paused | Archived | Released) => true,
            (Released | Faulted, Paused) => true,
            (Released | Paused | Faulted, Archived) => true,
            (from, Faulted) => from != Archived && from != Faulted,
            (Paused | Faulted | Archived, Released) => true,
            _ => false,
        }
    }

    /// Whether a vendor process may exist in this state.
    pub fn may_hold_process(self) -> bool {
        matches!(
            self,
            RuntimeState::Starting | RuntimeState::Live | RuntimeState::Draining
        )
    }

    /// The legacy `agents.state` value kept current for one release.
    fn legacy(self) -> &'static str {
        match self {
            RuntimeState::Released => "idle",
            RuntimeState::Paused => "paused",
            RuntimeState::Archived => "archived",
            _ => "running",
        }
    }
}

// Fault classes. Adapters classify; nothing matches error text.
text_enum!(
    FaultKind, "fault" {
        Auth => "auth",
        Transient => "transient",
        Fatal => "fatal",
        StorageRetry => "storage_retry",
        RecoveryExhausted => "recovery_exhausted",
        Legacy => "legacy",
    }
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fault {
    pub kind: FaultKind,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentRuntime {
    pub agent_id: AgentId,
    pub state: RuntimeState,
    pub generation: i64,
    pub attempt: i64,
    pub until_ms: Option<i64>,
    pub fault: Option<Fault>,
    pub vendor_session_id: Option<String>,
    pub updated_at_ms: i64,
}

/// One requested runtime transition.
///
/// Entering `starting` always opens the next generation. Entering `backoff`
/// counts one more attempt and requires `until_ms`; `released` resets the
/// attempt count. Entering `quota_blocked` requires a quota block row and copies
/// its deadline. Entering `faulted` requires `fault`; every other state clears
/// it. Leaving a process-holding state for one that cannot hold a process
/// interrupts the current generation's unfinished turn.
#[derive(Clone, Debug, Default)]
pub struct RuntimeChange {
    pub until_ms: Option<i64>,
    pub fault: Option<Fault>,
    pub vendor_session_id: Option<String>,
}

text_enum!(
    TurnState, "turn" {
        Open => "open",
        Interrupted => "interrupted",
        RecoveryOffered => "recovery_offered",
        Closed => "closed",
    }
);

text_enum!(
    TurnOutcome, "turn outcome" {
        Completed => "completed",
        Cancelled => "cancelled",
        Failed => "failed",
        Recovered => "recovered",
    }
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Turn {
    pub agent_id: AgentId,
    pub turn_id: String,
    pub generation: i64,
    pub state: TurnState,
    pub first_seq: i64,
    pub last_seq: i64,
    pub opened_at_ms: i64,
    pub closed_at_ms: Option<i64>,
    pub outcome: Option<TurnOutcome>,
    pub recovery_count: i64,
    pub recovers: Option<String>,
}

/// Result of asking to offer `[Recovery]` for an interrupted turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryOffer {
    Offered(Turn),
    /// The limit was reached: the turn stays interrupted and the runtime is
    /// faulted with `recovery_exhausted` in the same transaction.
    Exhausted(Turn),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionLease {
    pub agent_id: AgentId,
    pub vendor_session_id: Option<String>,
    pub generation: i64,
    pub holder: ProcessIdentity,
    pub pgid: i64,
    pub acquired_at_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaSource {
    Known,
    Estimated,
}

impl QuotaSource {
    fn as_str(self) -> &'static str {
        match self {
            QuotaSource::Known => "known",
            QuotaSource::Estimated => "estimated",
        }
    }
}

/// What the vendor said about when a quota resets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaReset {
    /// An absolute deadline from a typed field.
    Known { until_ms: i64 },
    /// No usable reset: the deadline is estimated on a bounded schedule.
    Unknown,
}

/// Estimated waits for an unknown reset: 1, 2, 4, 8, then 15 minutes.
pub const QUOTA_ESTIMATE_MS: [i64; 5] = [60_000, 120_000, 240_000, 480_000, 900_000];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuotaBlock {
    pub agent_id: AgentId,
    pub until_ms: i64,
    pub source: QuotaSource,
    pub attempt: i64,
    pub observed_at_ms: i64,
}

fn conflict(
    machine: &'static str,
    expected: impl fmt::Display,
    actual: impl fmt::Display,
) -> Error {
    Error::StateConflict {
        machine,
        expected: expected.to_string(),
        actual: actual.to_string(),
    }
}

fn refused(machine: &'static str, from: impl fmt::Display, to: impl fmt::Display) -> Error {
    Error::InvalidTransition {
        machine,
        from: from.to_string(),
        to: to.to_string(),
    }
}

const RUNTIME_COLS: &str = "agent_id, state, generation, attempt, until_ms, fault_kind, fault_detail, vendor_session_id, updated_at_ms";

fn runtime_from_row(row: &DbRow<'_>) -> Result<AgentRuntime, Error> {
    let fault_kind: Option<String> = row.get(5)?;
    let fault = match fault_kind {
        Some(kind) => Some(Fault {
            kind: kind.parse()?,
            detail: row.get::<Option<String>>(6)?.unwrap_or_default(),
        }),
        None => None,
    };
    Ok(AgentRuntime {
        agent_id: row.get(0)?,
        state: row.parse_str(1)?,
        generation: row.get(2)?,
        attempt: row.get(3)?,
        until_ms: row.get(4)?,
        fault,
        vendor_session_id: row.get(7)?,
        updated_at_ms: row.get(8)?,
    })
}

fn runtime_of(
    conn: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
) -> Result<Option<AgentRuntime>, Error> {
    conn.db_query_opt(
        &format!("SELECT {RUNTIME_COLS} FROM agent_runtime WHERE agent_id = ?1"),
        &db_params![agent_id],
        runtime_from_row,
    )
}

/// The runtime state an existing legacy lifecycle value maps to.
pub(super) fn runtime_state_for_legacy(state: &str) -> RuntimeState {
    match state {
        "paused" | "stopped" => RuntimeState::Paused,
        "archived" => RuntimeState::Archived,
        _ => RuntimeState::Released,
    }
}

/// Give a newly inserted agent its runtime row in the creating transaction.
pub(super) fn seed_runtime(tx: &(impl DbExec + ?Sized), agent_id: &AgentId) -> Result<(), Error> {
    tx.db_execute(
        "INSERT INTO agent_runtime (agent_id, state, updated_at_ms) VALUES (?1, 'released', ?2) ON CONFLICT DO NOTHING",
        &db_params![agent_id, now_ms()],
    )?;
    Ok(())
}

const TURN_COLS: &str = "agent_id, turn_id, generation, state, first_seq, last_seq, opened_at_ms, \
     closed_at_ms, outcome, recovery_count, recovers";

fn turn_from_row(row: &DbRow<'_>) -> Result<Turn, Error> {
    Ok(Turn {
        agent_id: row.get(0)?,
        turn_id: row.get(1)?,
        generation: row.get(2)?,
        state: row.parse_str(3)?,
        first_seq: row.get(4)?,
        last_seq: row.get(5)?,
        opened_at_ms: row.get(6)?,
        closed_at_ms: row.get(7)?,
        outcome: row.opt_parse_str(8)?,
        recovery_count: row.get(9)?,
        recovers: row.get(10)?,
    })
}

fn unfinished_turn(
    conn: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
) -> Result<Option<Turn>, Error> {
    conn.db_query_opt(
        &format!("SELECT {TURN_COLS} FROM turns WHERE agent_id = ?1 AND state != 'closed'"),
        &db_params![agent_id],
        turn_from_row,
    )
}

fn turn_by_id(
    conn: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
    turn_id: &str,
) -> Result<Turn, Error> {
    conn.db_query_opt(
        &format!("SELECT {TURN_COLS} FROM turns WHERE agent_id = ?1 AND turn_id = ?2"),
        &db_params![agent_id, turn_id],
        turn_from_row,
    )?
    .ok_or_else(|| Error::Message(format!("turn `{turn_id}` not found for {agent_id}")))
}

fn new_turn_id() -> String {
    format!("turn_{}", uuid::Uuid::now_v7().simple())
}

/// Keep the legacy `turn_recovery` row equal to what the turn table says.
fn mirror_turn_recovery(tx: &(impl DbExec + ?Sized), agent_id: &AgentId) -> Result<(), Error> {
    let (opened, completed, count, opened_at) = match unfinished_turn(tx, agent_id)? {
        Some(turn) => (
            turn.last_seq,
            turn.first_seq - 1,
            turn.recovery_count,
            turn.opened_at_ms,
        ),
        None => {
            let last: Option<(i64, i64)> = tx.db_query_opt(
                "SELECT last_seq, opened_at_ms FROM turns WHERE agent_id = ?1
                 ORDER BY last_seq DESC, opened_at_ms DESC LIMIT 1",
                &db_params![agent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let (seq, at) = last.unwrap_or((0, 0));
            (seq, seq, 0, at)
        }
    };
    tx.db_execute(
        "INSERT INTO turn_recovery (agent_id, opened_seq, completed_seq, recovery_count, opened_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(agent_id) DO UPDATE SET opened_seq = ?2, completed_seq = ?3,
             recovery_count = ?4, opened_at_ms = ?5",
        &db_params![agent_id, opened, completed, count, opened_at],
    )?;
    Ok(())
}

/// Precondition for `live -> released`: no unfinished turn, a drained
/// mailbox, and no child that may hold a process.
fn release_blocker(tx: &dyn DbExec, agent_id: &AgentId) -> Result<Option<String>, Error> {
    if let Some(turn) = unfinished_turn(tx, agent_id)? {
        return Ok(Some(format!("turn {} is {}", turn.turn_id, turn.state)));
    }
    let max_seq: i64 = tx.db_query_one(
        "SELECT COALESCE(MAX(seq), 0) FROM messages WHERE agent_id = ?1",
        &db_params![agent_id],
        |row| row.get(0),
    )?;
    let delivered = cursor(tx, agent_id)?;
    if delivered < max_seq {
        return Ok(Some(format!(
            "mailbox holds undelivered rows after {delivered} (last {max_seq})"
        )));
    }
    let busy_child: Option<String> = tx.db_query_opt(
        "SELECT a.id FROM agents a JOIN agent_runtime r ON r.agent_id = a.id
         WHERE a.parent_id = ?1 AND r.state IN ('starting', 'live', 'draining', 'backoff', 'quota_blocked')
         LIMIT 1",
        &db_params![agent_id],
        |row| row.get(0),
    )?;
    Ok(busy_child.map(|child| format!("child {child} is active")))
}

fn lease_from_row(row: &DbRow<'_>) -> Result<SessionLease, Error> {
    Ok(SessionLease {
        agent_id: row.get(0)?,
        vendor_session_id: row.get(1)?,
        generation: row.get(2)?,
        holder: ProcessIdentity {
            pid: row.get(3)?,
            start_time: row.get(4)?,
        },
        pgid: row.get(5)?,
        acquired_at_ms: row.get(6)?,
    })
}

const LEASE_COLS: &str =
    "agent_id, vendor_session_id, generation, holder_pid, holder_start_time, pgid, acquired_at_ms";

fn quota_from_row(row: &DbRow<'_>) -> Result<QuotaBlock, Error> {
    let source: String = row.get(2)?;
    Ok(QuotaBlock {
        agent_id: row.get(0)?,
        until_ms: row.get(1)?,
        source: if source == "known" {
            QuotaSource::Known
        } else {
            QuotaSource::Estimated
        },
        attempt: row.get(3)?,
        observed_at_ms: row.get(4)?,
    })
}

fn quota_of(
    conn: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
) -> Result<Option<QuotaBlock>, Error> {
    conn.db_query_opt(
        "SELECT agent_id, until_ms, source, attempt, observed_at_ms FROM quota_blocks WHERE agent_id = ?1",
        &db_params![agent_id],
        quota_from_row,
    )
}

impl Store {
    /// The agent's runtime row. Every agent has one after the runtime backfill.
    pub fn agent_runtime(&self, agent_id: &AgentId) -> Result<Option<AgentRuntime>, Error> {
        runtime_of(&self.read_conn(), agent_id)
    }

    /// Move the runtime from `from` to `to` (see [`RuntimeChange`]).
    pub fn transition_runtime(
        &self,
        agent_id: &AgentId,
        from: RuntimeState,
        to: RuntimeState,
        change: RuntimeChange,
    ) -> Result<AgentRuntime, Error> {
        self.write_tx(|tx| transition_runtime_tx(tx, agent_id, from, to, &change))
    }

    /// The agent's unfinished turn (`open`, `interrupted` or `recovery_offered`).
    pub fn unfinished_turn(&self, agent_id: &AgentId) -> Result<Option<Turn>, Error> {
        unfinished_turn(&self.read_conn(), agent_id)
    }

    /// The most recent turns, newest first.
    pub fn recent_turns(&self, agent_id: &AgentId, limit: usize) -> Result<Vec<Turn>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec(
            &format!(
                "SELECT {TURN_COLS} FROM turns WHERE agent_id = ?1
                 ORDER BY opened_at_ms DESC, last_seq DESC LIMIT ?2"
            ),
            &db_params![agent_id, i64::try_from(limit).unwrap_or(i64::MAX)],
            turn_from_row,
        )
    }

    /// Open a turn at `first_seq` for the live generation. Refused while another
    /// turn is unfinished or the runtime is not live in that generation.
    pub fn begin_turn(
        &self,
        agent_id: &AgentId,
        generation: i64,
        first_seq: i64,
    ) -> Result<Turn, Error> {
        self.write_tx(|tx| {
            require_live(tx, agent_id, generation)?;
            if let Some(turn) = unfinished_turn(tx, agent_id)? {
                return Err(refused("turn", turn.state, TurnState::Open));
            }
            let turn_id = new_turn_id();
            tx.db_execute(
                "INSERT INTO turns (agent_id, turn_id, generation, state, first_seq, last_seq, opened_at_ms)
                 VALUES (?1, ?2, ?3, 'open', ?4, ?4, ?5)",
                &db_params![agent_id, turn_id, generation, first_seq, now_ms()],
            )?;
            mirror_turn_recovery(tx, agent_id)?;
            turn_by_id(tx, agent_id, &turn_id)
        })
    }

    /// Record that the open turn took `seq` (a steer or a followup line).
    pub fn extend_turn(&self, agent_id: &AgentId, turn_id: &str, seq: i64) -> Result<Turn, Error> {
        self.write_tx(|tx| {
            let turn = turn_by_id(tx, agent_id, turn_id)?;
            if turn.state != TurnState::Open {
                return Err(conflict("turn", TurnState::Open, turn.state));
            }
            if seq < turn.last_seq {
                return Err(Error::Message(format!(
                    "turn {turn_id} cannot move back from {} to {seq}",
                    turn.last_seq
                )));
            }
            tx.db_execute(
                "UPDATE turns SET last_seq = ?3 WHERE agent_id = ?1 AND turn_id = ?2",
                &db_params![agent_id, turn_id, seq],
            )?;
            mirror_turn_recovery(tx, agent_id)?;
            turn_by_id(tx, agent_id, turn_id)
        })
    }

    /// Close the open turn on the harness's explicit terminal event.
    pub fn end_turn(
        &self,
        agent_id: &AgentId,
        turn_id: &str,
        outcome: TurnOutcome,
    ) -> Result<Turn, Error> {
        if outcome == TurnOutcome::Recovered {
            return Err(refused("turn", TurnState::Open, "closed(recovered)"));
        }
        self.write_tx(|tx| {
            let turn = turn_by_id(tx, agent_id, turn_id)?;
            if turn.state != TurnState::Open {
                return Err(conflict("turn", TurnState::Open, turn.state));
            }
            close_turn(tx, agent_id, turn_id, outcome)?;
            mirror_turn_recovery(tx, agent_id)?;
            turn_by_id(tx, agent_id, turn_id)
        })
    }

    /// Mark the open turn interrupted (process exit or generation change).
    pub fn interrupt_turn(&self, agent_id: &AgentId, turn_id: &str) -> Result<Turn, Error> {
        self.write_tx(|tx| {
            let turn = turn_by_id(tx, agent_id, turn_id)?;
            if turn.state != TurnState::Open {
                return Err(conflict("turn", TurnState::Open, turn.state));
            }
            set_turn_state(tx, agent_id, turn_id, TurnState::Interrupted)?;
            mirror_turn_recovery(tx, agent_id)?;
            turn_by_id(tx, agent_id, turn_id)
        })
    }

    /// Offer one `[Recovery]` for an interrupted turn, up to `limit` offers.
    pub fn offer_turn_recovery(
        &self,
        agent_id: &AgentId,
        turn_id: &str,
        limit: i64,
    ) -> Result<RecoveryOffer, Error> {
        self.write_tx(|tx| {
            let turn = turn_by_id(tx, agent_id, turn_id)?;
            if turn.state != TurnState::Interrupted {
                return Err(conflict("turn", TurnState::Interrupted, turn.state));
            }
            if turn.recovery_count >= limit {
                let runtime = runtime_of(tx, agent_id)?
                    .ok_or_else(|| Error::AgentNotFound(agent_id.to_string()))?;
                if runtime.state != RuntimeState::Faulted {
                    let change = RuntimeChange {
                        fault: Some(Fault {
                            kind: FaultKind::RecoveryExhausted,
                            detail: format!(
                                "turn {turn_id} recovered {} times",
                                turn.recovery_count
                            ),
                        }),
                        ..RuntimeChange::default()
                    };
                    transition_runtime_tx(
                        tx,
                        agent_id,
                        runtime.state,
                        RuntimeState::Faulted,
                        &change,
                    )?;
                }
                return Ok(RecoveryOffer::Exhausted(turn));
            }
            tx.db_execute(
                "UPDATE turns SET state = 'recovery_offered', recovery_count = recovery_count + 1
                 WHERE agent_id = ?1 AND turn_id = ?2",
                &db_params![agent_id, turn_id],
            )?;
            mirror_turn_recovery(tx, agent_id)?;
            Ok(RecoveryOffer::Offered(turn_by_id(tx, agent_id, turn_id)?))
        })
    }

    /// The harness took the `[Recovery]` line: the interrupted turn closes as
    /// `recovered` and a new open turn that names it begins, in one commit.
    pub fn accept_turn_recovery(
        &self,
        agent_id: &AgentId,
        turn_id: &str,
        generation: i64,
        seq: i64,
    ) -> Result<Turn, Error> {
        self.write_tx(|tx| {
            require_live(tx, agent_id, generation)?;
            let turn = turn_by_id(tx, agent_id, turn_id)?;
            if turn.state != TurnState::RecoveryOffered {
                return Err(conflict("turn", TurnState::RecoveryOffered, turn.state));
            }
            close_turn(tx, agent_id, turn_id, TurnOutcome::Recovered)?;
            let next = new_turn_id();
            tx.db_execute(
                "INSERT INTO turns (agent_id, turn_id, generation, state, first_seq, last_seq,
                                    opened_at_ms, recovery_count, recovers)
                 VALUES (?1, ?2, ?3, 'open', ?4, ?5, ?6, ?7, ?8)",
                &db_params![
                    agent_id,
                    next,
                    generation,
                    turn.first_seq,
                    seq.max(turn.last_seq),
                    now_ms(),
                    turn.recovery_count,
                    turn_id
                ],
            )?;
            mirror_turn_recovery(tx, agent_id)?;
            turn_by_id(tx, agent_id, &next)
        })
    }

    /// Acquire the vendor-session lease for `generation`. Refused while any
    /// lease row exists: the caller releases or reclaims it first.
    pub fn acquire_session_lease(
        &self,
        agent_id: &AgentId,
        vendor_session_id: Option<&str>,
        generation: i64,
        holder: ProcessIdentity,
        pgid: i64,
    ) -> Result<SessionLease, Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            if let Some(held) = session_lease_of(tx, agent_id)? {
                return Err(Error::LeaseHeld {
                    lease: "session",
                    holder_pid: held.holder.pid,
                });
            }
            tx.db_execute(
                &format!(
                    "INSERT INTO session_leases ({LEASE_COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
                ),
                &db_params![
                    agent_id,
                    vendor_session_id,
                    generation,
                    holder.pid,
                    holder.start_time,
                    pgid,
                    now_ms()
                ],
            )?;
            session_lease_of(tx, agent_id)?
                .ok_or_else(|| Error::Message("session lease vanished".into()))
        })
    }

    pub fn session_lease(&self, agent_id: &AgentId) -> Result<Option<SessionLease>, Error> {
        session_lease_of(&self.read_conn(), agent_id)
    }

    /// Release the lease only when it is the one `holder` took for `generation`.
    pub fn release_session_lease(
        &self,
        agent_id: &AgentId,
        generation: i64,
        holder: ProcessIdentity,
    ) -> Result<bool, Error> {
        self.write_tx(|tx| {
            Ok(tx.db_execute(
                "DELETE FROM session_leases WHERE agent_id = ?1 AND generation = ?2
                 AND holder_pid = ?3 AND holder_start_time = ?4",
                &db_params![agent_id, generation, holder.pid, holder.start_time],
            )? > 0)
        })
    }

    /// Drop every session lease whose holder `is_alive` reports gone, and
    /// return the dropped leases. A live orphaned holder must be stopped by the
    /// caller first; its lease stays. `is_alive` runs while this process's
    /// writer is held: it must not use the store.
    pub fn reclaim_session_leases(
        &self,
        is_alive: impl Fn(&ProcessIdentity) -> bool,
    ) -> Result<Vec<SessionLease>, Error> {
        self.write_tx(|tx| {
            let leases = tx.db_query_vec(
                &format!("SELECT {LEASE_COLS} FROM session_leases"),
                &[],
                lease_from_row,
            )?;
            let mut dropped = Vec::new();
            for lease in leases {
                if is_alive(&lease.holder) {
                    continue;
                }
                tx.db_execute(
                    "DELETE FROM session_leases WHERE agent_id = ?1 AND holder_pid = ?2 AND holder_start_time = ?3",
                    &db_params![lease.agent_id, lease.holder.pid, lease.holder.start_time],
                )?;
                dropped.push(lease);
            }
            Ok(dropped)
        })
    }

    /// Record a quota answer. A known reset is used as given; an unknown one is
    /// estimated from [`QUOTA_ESTIMATE_MS`] by consecutive unknown attempts.
    pub fn block_quota(
        &self,
        agent_id: &AgentId,
        reset: QuotaReset,
        now: i64,
    ) -> Result<QuotaBlock, Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            let previous = quota_of(tx, agent_id)?;
            let (until_ms, source, attempt) = match reset {
                QuotaReset::Known { until_ms } => (until_ms, QuotaSource::Known, 0),
                QuotaReset::Unknown => {
                    let attempt = previous
                        .as_ref()
                        .filter(|block| block.source == QuotaSource::Estimated)
                        .map_or(1, |block| block.attempt + 1);
                    let index = usize::try_from(attempt - 1)
                        .unwrap_or(0)
                        .min(QUOTA_ESTIMATE_MS.len() - 1);
                    (now.saturating_add(QUOTA_ESTIMATE_MS[index]), QuotaSource::Estimated, attempt)
                }
            };
            tx.db_execute(
                "INSERT INTO quota_blocks (agent_id, until_ms, source, attempt, observed_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(agent_id) DO UPDATE SET until_ms = ?2, source = ?3, attempt = ?4, observed_at_ms = ?5",
                &db_params![agent_id, until_ms, source.as_str(), attempt, now],
            )?;
            quota_of(tx, agent_id)?.ok_or_else(|| Error::Message("quota block vanished".into()))
        })
    }

    pub fn quota_block(&self, agent_id: &AgentId) -> Result<Option<QuotaBlock>, Error> {
        quota_of(&self.read_conn(), agent_id)
    }

    /// Clear the quota block (deadline passed, or an operator cleared it).
    pub fn clear_quota_block(&self, agent_id: &AgentId) -> Result<bool, Error> {
        self.write_tx(|tx| {
            Ok(tx.db_execute(
                "DELETE FROM quota_blocks WHERE agent_id = ?1",
                &db_params![agent_id],
            )? > 0)
        })
    }

    /// One-time derivation of runtime rows and turns from legacy state, for
    /// agents that have none. Idempotent. `last_error` reads an agent's legacy
    /// persisted fault, if any.
    pub fn backfill_runtime_state(
        &self,
        last_error: impl Fn(&AgentId) -> Option<String>,
    ) -> Result<BackfillReport, Error> {
        self.write_tx(|tx| backfill_tx(tx, &last_error))
    }
}

/// Counts from one runtime backfill pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackfillReport {
    pub runtimes: usize,
    pub faulted: usize,
    pub interrupted_turns: usize,
    pub closed_turns: usize,
}

pub(super) fn backfill_tx(
    tx: &(impl DbExec + ?Sized),
    last_error: &dyn Fn(&AgentId) -> Option<String>,
) -> Result<BackfillReport, Error> {
    let mut report = BackfillReport::default();
    let missing = tx.db_query_vec(
        "SELECT a.id, a.state FROM agents a
         WHERE NOT EXISTS (SELECT 1 FROM agent_runtime r WHERE r.agent_id = a.id)",
        &[],
        |row| Ok((row.get::<String>(0)?, row.get::<String>(1)?)),
    )?;
    let now = now_ms();
    for (id, legacy_state) in missing {
        let agent_id = AgentId::parse(&id)?;
        let mut state = runtime_state_for_legacy(&legacy_state);
        let fault = if state == RuntimeState::Archived {
            None
        } else {
            last_error(&agent_id).filter(|text| !text.trim().is_empty())
        };
        if fault.is_some() {
            state = RuntimeState::Faulted;
            report.faulted += 1;
        }
        tx.db_execute(
            "INSERT INTO agent_runtime (agent_id, state, fault_kind, fault_detail, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            &db_params![
                id,
                state.as_str(),
                fault.as_ref().map(|_| FaultKind::Legacy.as_str()),
                fault,
                now
            ],
        )?;
        report.runtimes += 1;
        let has_turns: bool = tx.db_query_one(
            "SELECT EXISTS(SELECT 1 FROM turns WHERE agent_id = ?1)",
            &db_params![&id],
            |row| row.get(0),
        )?;
        if has_turns {
            continue;
        }
        let legacy: Option<(i64, i64, i64, i64)> = tx.db_query_opt(
            "SELECT opened_seq, completed_seq, recovery_count, opened_at_ms FROM turn_recovery WHERE agent_id = ?1",
            &db_params![&id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        let Some((opened, completed, count, opened_at)) = legacy else {
            continue;
        };
        if opened > completed {
            tx.db_execute(
                "INSERT INTO turns (agent_id, turn_id, generation, state, first_seq, last_seq,
                                    opened_at_ms, recovery_count)
                 VALUES (?1, ?2, 0, 'interrupted', ?3, ?4, ?5, ?6)",
                &db_params![id, new_turn_id(), completed + 1, opened, opened_at, count],
            )?;
            report.interrupted_turns += 1;
        } else if opened > 0 {
            tx.db_execute(
                "INSERT INTO turns (agent_id, turn_id, generation, state, first_seq, last_seq,
                                    opened_at_ms, closed_at_ms, outcome)
                 VALUES (?1, ?2, 0, 'closed', ?3, ?3, ?4, ?4, 'completed')",
                &db_params![id, new_turn_id(), opened, opened_at],
            )?;
            report.closed_turns += 1;
        }
    }
    Ok(report)
}

fn session_lease_of(
    conn: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
) -> Result<Option<SessionLease>, Error> {
    conn.db_query_opt(
        &format!("SELECT {LEASE_COLS} FROM session_leases WHERE agent_id = ?1"),
        &db_params![agent_id],
        lease_from_row,
    )
}

fn require_live(
    tx: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
    generation: i64,
) -> Result<(), Error> {
    let runtime =
        runtime_of(tx, agent_id)?.ok_or_else(|| Error::AgentNotFound(agent_id.to_string()))?;
    if runtime.state != RuntimeState::Live || runtime.generation != generation {
        return Err(conflict(
            "runtime",
            format!("live@{generation}"),
            format!("{}@{}", runtime.state, runtime.generation),
        ));
    }
    Ok(())
}

fn set_turn_state(
    tx: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
    turn_id: &str,
    state: TurnState,
) -> Result<(), Error> {
    tx.db_execute(
        "UPDATE turns SET state = ?3 WHERE agent_id = ?1 AND turn_id = ?2",
        &db_params![agent_id, turn_id, state.as_str()],
    )?;
    Ok(())
}

fn close_turn(
    tx: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
    turn_id: &str,
    outcome: TurnOutcome,
) -> Result<(), Error> {
    tx.db_execute(
        "UPDATE turns SET state = 'closed', outcome = ?3, closed_at_ms = ?4 WHERE agent_id = ?1 AND turn_id = ?2",
        &db_params![agent_id, turn_id, outcome.as_str(), now_ms()],
    )?;
    Ok(())
}

fn transition_runtime_tx(
    tx: &dyn DbExec,
    agent_id: &AgentId,
    from: RuntimeState,
    to: RuntimeState,
    change: &RuntimeChange,
) -> Result<AgentRuntime, Error> {
    let current =
        runtime_of(tx, agent_id)?.ok_or_else(|| Error::AgentNotFound(agent_id.to_string()))?;
    if current.state != from {
        return Err(conflict("runtime", from, current.state));
    }
    if !from.allows(to) {
        return Err(refused("runtime", from, to));
    }
    let fault = match (to, &change.fault) {
        (RuntimeState::Faulted, Some(fault)) => Some(fault.clone()),
        (RuntimeState::Faulted, None) => {
            return Err(Error::Message("entering faulted requires a fault".into()));
        }
        (_, _) => None,
    };
    let mut until_ms = None;
    let mut attempt = current.attempt;
    let mut generation = current.generation;
    match to {
        RuntimeState::Starting => generation += 1,
        RuntimeState::Backoff => {
            until_ms = Some(
                change
                    .until_ms
                    .ok_or_else(|| Error::Message("entering backoff requires until_ms".into()))?,
            );
            attempt += 1;
        }
        RuntimeState::QuotaBlocked => {
            let block = quota_of(tx, agent_id)?.ok_or_else(|| {
                Error::Message("entering quota_blocked requires a quota block".into())
            })?;
            until_ms = Some(block.until_ms);
        }
        RuntimeState::Released => {
            attempt = 0;
            if from == RuntimeState::Live
                && let Some(reason) = release_blocker(tx, agent_id)?
            {
                return Err(Error::Precondition {
                    machine: "runtime",
                    reason,
                });
            }
        }
        _ => {}
    }
    if from.may_hold_process()
        && !to.may_hold_process()
        && let Some(turn) = unfinished_turn(tx, agent_id)?
        && turn.state == TurnState::Open
        && turn.generation == current.generation
    {
        set_turn_state(tx, agent_id, &turn.turn_id, TurnState::Interrupted)?;
        mirror_turn_recovery(tx, agent_id)?;
    }
    let vendor_session_id = change
        .vendor_session_id
        .clone()
        .or(current.vendor_session_id.clone());
    tx.db_execute(
        "UPDATE agent_runtime SET state = ?2, generation = ?3, attempt = ?4, until_ms = ?5,
             fault_kind = ?6, fault_detail = ?7, vendor_session_id = ?8, updated_at_ms = ?9
         WHERE agent_id = ?1",
        &db_params![
            agent_id,
            to.as_str(),
            generation,
            attempt,
            until_ms,
            fault.as_ref().map(|f| f.kind.as_str()),
            fault.as_ref().map(|f| f.detail.as_str()),
            vendor_session_id,
            now_ms()
        ],
    )?;
    tx.db_execute(
        "UPDATE agents SET state = ?2 WHERE id = ?1",
        &db_params![agent_id, to.legacy()],
    )?;
    if to == RuntimeState::Archived {
        super::cancel_archived_schedules(tx, agent_id)?;
    }
    runtime_of(tx, agent_id)?.ok_or_else(|| Error::AgentNotFound(agent_id.to_string()))
}
