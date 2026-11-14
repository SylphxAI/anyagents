//! Turns that end in an error (`docs/delivery.md`, "A turn that ends in an
//! error"). The harness reported the turn's end, and with it an error. The
//! turn is counted at once, and closed with that outcome as soon as the note
//! that carries its work is in the mailbox, so it never holds the mailbox; a
//! restart before then finds it open and recovers it as any interrupted turn.
//! At the limit the supervisor parks the agent instead; a resume clears the
//! count and the parking.

use std::collections::HashMap;

use rusqlite::{OptionalExtension, params};

use super::{OpenTurn, Store, Wake, agent_exists, now_ms};
use crate::error::Error;
use crate::id::AgentId;

/// Errored turns in a row that park an agent when the last error is one no
/// retry clears (a context the model cannot take, a request it refuses).
pub const MAX_ERRORED_TURNS: i64 = 3;

/// Errored turns in a row that park an agent while each error is transient
/// (an overloaded or unreachable provider). The supervisor backs off between
/// them, so this is about half an hour of failing turns.
pub const MAX_TRANSIENT_ERRORED_TURNS: i64 = 8;

/// An agent's run of errored turns, and its parking.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TurnErrors {
    /// Turns in a row that ended in an error.
    pub streak: i64,
    pub last_error: Option<String>,
    pub last_error_at_ms: Option<i64>,
    /// When the agent was parked because `streak` reached the limit.
    pub faulted_at_ms: Option<i64>,
}

impl TurnErrors {
    /// Worth showing: a run in progress, or a parking.
    pub fn is_current(&self) -> bool {
        self.streak > 0 || self.faulted_at_ms.is_some()
    }
}

/// One errored turn, as the supervisor follows it up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErroredTurn {
    /// Turns in a row that ended in an error, this one included.
    pub streak: i64,
    /// The turn the store holds open for it, when there is one: what a
    /// recovery note quotes.
    pub open: Option<OpenTurn>,
}

fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnErrors> {
    Ok(TurnErrors {
        streak: row.get(0)?,
        last_error: row.get(1)?,
        last_error_at_ms: row.get(2)?,
        faulted_at_ms: row.get(3)?,
    })
}

impl Store {
    /// Count a turn the harness ended with `error`, and return the run it
    /// makes. The turn stays open until [`Store::close_errored_turn`].
    pub fn note_turn_errored(&self, agent_id: &AgentId, error: &str) -> Result<i64, Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.execute(
                "INSERT INTO turn_errors (agent_id, streak, last_error, last_error_at_ms)
                 VALUES (?1, 1, ?2, ?3)
                 ON CONFLICT(agent_id) DO UPDATE SET
                     streak = turn_errors.streak + 1,
                     last_error = excluded.last_error,
                     last_error_at_ms = excluded.last_error_at_ms",
                params![agent_id.as_str(), error, now_ms()],
            )?;
            Ok(tx.query_row(
                "SELECT streak FROM turn_errors WHERE agent_id = ?1",
                [agent_id.as_str()],
                |row| row.get::<_, i64>(0),
            )?)
        })
    }

    /// Close the turn an errored turn left open, once the note that carries
    /// its work is in the mailbox: the one open through `opened_seq`, never
    /// one a later message opened. `true` when it was still open.
    pub fn close_errored_turn(&self, agent_id: &AgentId, opened_seq: i64) -> Result<bool, Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            Ok(tx.execute(
                "UPDATE turn_recovery
                 SET completed_seq = opened_seq,
                     recovery_count = 0
                 WHERE agent_id = ?1 AND opened_seq = ?2 AND completed_seq < opened_seq",
                params![agent_id.as_str(), opened_seq],
            )? > 0)
        })
    }

    /// Park an agent whose turns keep ending in an error: it is `paused`, so
    /// it runs nothing and its mail waits, and the parking is recorded with
    /// the error that caused it. `false` when the agent was not running or
    /// idle (an operator already paused or archived it).
    pub fn park_faulted(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let parked = self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            let parked = tx.execute(
                "UPDATE agents SET state = 'paused'
                 WHERE id = ?1 AND state IN ('running', 'idle')",
                [agent_id.as_str()],
            )? > 0;
            if parked {
                tx.execute(
                    "INSERT INTO turn_errors (agent_id, streak, faulted_at_ms)
                     VALUES (?1, 0, ?2)
                     ON CONFLICT(agent_id) DO UPDATE SET faulted_at_ms = excluded.faulted_at_ms",
                    params![agent_id.as_str(), now_ms()],
                )?;
            }
            Ok(parked)
        })?;
        if parked {
            self.notify(Wake::Agent {
                agent_id: agent_id.clone(),
            });
        }
        Ok(parked)
    }

    /// The agent's errored-turn record, when it has one.
    pub fn turn_errors(&self, agent_id: &AgentId) -> Result<Option<TurnErrors>, Error> {
        Ok(self
            .read_conn()
            .prepare_cached(
                "SELECT streak, last_error, last_error_at_ms, faulted_at_ms
                 FROM turn_errors WHERE agent_id = ?1",
            )?
            .query_row([agent_id.as_str()], row)
            .optional()?)
    }

    /// Every agent with a run of errored turns in progress, or parked for one.
    pub fn current_turn_errors(&self) -> Result<HashMap<AgentId, TurnErrors>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached(
            "SELECT agent_id, streak, last_error, last_error_at_ms, faulted_at_ms
             FROM turn_errors WHERE streak > 0 OR faulted_at_ms IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |r| {
            let id: String = r.get(0)?;
            Ok((
                id,
                TurnErrors {
                    streak: r.get(1)?,
                    last_error: r.get(2)?,
                    last_error_at_ms: r.get(3)?,
                    faulted_at_ms: r.get(4)?,
                },
            ))
        })?;
        let mut out = HashMap::new();
        for item in rows {
            let (id, errors) = item?;
            if let Ok(id) = AgentId::parse(&id) {
                out.insert(id, errors);
            }
        }
        Ok(out)
    }
}

/// A resume clears a parking and the run that caused it; any other start
/// leaves a run in progress counted. Called inside the transaction that sets
/// the agent running.
pub(super) fn clear_parking(
    tx: &rusqlite::Transaction<'_>,
    agent_id: &AgentId,
) -> Result<(), Error> {
    tx.execute(
        "UPDATE turn_errors SET streak = 0, faulted_at_ms = NULL
         WHERE agent_id = ?1 AND faulted_at_ms IS NOT NULL",
        [agent_id.as_str()],
    )?;
    Ok(())
}

/// A completed turn ends the run. Called inside the transaction that records
/// the completion.
pub(super) fn end_run(tx: &rusqlite::Transaction<'_>, agent_id: &AgentId) -> Result<(), Error> {
    tx.execute(
        "UPDATE turn_errors SET streak = 0 WHERE agent_id = ?1 AND streak != 0",
        [agent_id.as_str()],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::MAX_ERRORED_TURNS;
    use crate::{Incoming, NewAgent, ReceiveOutcome, Store};

    fn store() -> (tempfile::TempDir, Store) {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        (dir, store)
    }

    fn deliver(store: &Store, agent: &crate::AgentId, text: &str) -> i64 {
        let seq = match store.receive(agent, Incoming::new(text)).unwrap() {
            ReceiveOutcome::New(message) => message.seq,
            ReceiveOutcome::Duplicate(_) => panic!("duplicate"),
        };
        store.note_delivered(agent, seq).unwrap();
        seq
    }

    #[test]
    fn an_errored_turn_is_counted_and_closed_only_through_its_own_turn() {
        let (_dir, store) = store();
        let agent = store
            .create_agent(NewAgent::new("worker", "spiron"))
            .unwrap();
        deliver(&store, &agent.id, "first");
        deliver(&store, &agent.id, "second");
        assert_eq!(
            store
                .note_turn_errored(&agent.id, "context_length_exceeded")
                .unwrap(),
            1
        );
        let open = store.open_turn(&agent.id).unwrap().expect("still open");
        let texts: Vec<_> = open.messages.iter().map(|m| m.text.as_str()).collect();
        assert_eq!(texts, ["first", "second"]);
        let errors = store.turn_errors(&agent.id).unwrap().unwrap();
        assert_eq!(errors.streak, 1);
        assert_eq!(
            errors.last_error.as_deref(),
            Some("context_length_exceeded")
        );
        assert!(errors.faulted_at_ms.is_none());

        // A later message extends the turn: the stale close leaves it open.
        deliver(&store, &agent.id, "third");
        assert!(
            !store
                .close_errored_turn(&agent.id, open.opened_seq)
                .unwrap()
        );
        assert!(store.has_open_turn(&agent.id).unwrap());
        let open = store.open_turn(&agent.id).unwrap().unwrap();
        assert!(
            store
                .close_errored_turn(&agent.id, open.opened_seq)
                .unwrap()
        );
        assert!(!store.has_open_turn(&agent.id).unwrap());
        assert_eq!(store.note_turn_errored(&agent.id, "again").unwrap(), 2);

        // A completed turn ends the run.
        deliver(&store, &agent.id, "fourth");
        store.note_turn_completed(&agent.id).unwrap();
        assert_eq!(store.turn_errors(&agent.id).unwrap().unwrap().streak, 0);
        assert!(store.current_turn_errors().unwrap().is_empty());
    }

    #[test]
    fn parking_pauses_the_agent_and_a_resume_clears_it() {
        let (_dir, store) = store();
        let agent = store
            .create_agent(NewAgent::new("worker", "spiron"))
            .unwrap();
        store.set_agent_state(&agent.id, "running").unwrap();
        for _ in 0..MAX_ERRORED_TURNS {
            deliver(&store, &agent.id, "work");
            store.note_turn_errored(&agent.id, "boom").unwrap();
        }
        assert!(
            store.has_open_turn(&agent.id).unwrap(),
            "a parked turn stays open"
        );
        assert!(store.park_faulted(&agent.id).unwrap());
        assert_eq!(store.agent(&agent.id).unwrap().state, "paused");
        let errors = store.turn_errors(&agent.id).unwrap().unwrap();
        assert_eq!(errors.streak, MAX_ERRORED_TURNS);
        assert!(errors.faulted_at_ms.is_some());
        assert!(store.current_turn_errors().unwrap().contains_key(&agent.id));
        // Already paused: nothing more to park.
        assert!(!store.park_faulted(&agent.id).unwrap());

        store.set_agent_state(&agent.id, "running").unwrap();
        let errors = store.turn_errors(&agent.id).unwrap().unwrap();
        assert_eq!(errors.streak, 0, "a resume starts the count again");
        assert!(errors.faulted_at_ms.is_none());
        assert_eq!(errors.last_error.as_deref(), Some("boom"), "history stays");
    }

    #[test]
    fn an_operator_pause_is_never_turned_into_a_parking() {
        let (_dir, store) = store();
        let agent = store
            .create_agent(NewAgent::new("worker", "spiron"))
            .unwrap();
        store.set_agent_state(&agent.id, "archived").unwrap();
        assert!(!store.park_faulted(&agent.id).unwrap());
        assert_eq!(store.agent(&agent.id).unwrap().state, "archived");
        assert!(store.turn_errors(&agent.id).unwrap().is_none());
    }
}
