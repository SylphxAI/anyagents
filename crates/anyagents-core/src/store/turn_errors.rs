//! Turns that end in an error (`docs/delivery.md`, "A turn that ends in an
//! error"). The harness reported the turn's end, and with it an error. The
//! turn is closed with that outcome in the same transaction that stores the
//! note carrying its work, so it never holds the mailbox; a restart before
//! then finds it open and recovers it as any interrupted turn.
//!
//! Only an error of the agent's own session (its context is full, the
//! vendor refuses what the session holds) is a strike: at
//! [`MAX_ERRORED_TURNS`] strikes in a row the supervisor parks the agent. A
//! provider, model, auth, network, host or store error is the fleet's, not
//! the agent's: it is retried after a backoff and never parks anyone, so one
//! outage cannot pause every busy agent. A resume clears the strikes and the
//! parking.

use std::collections::HashMap;

use rusqlite::{OptionalExtension, params};

use super::{Incoming, OpenTurn, Store, Wake, agent_exists, now_ms};
use crate::error::Error;
use crate::id::AgentId;

/// Strikes (errors of the agent's own session) in a row that park it.
pub const MAX_ERRORED_TURNS: i64 = 3;

/// Whether `error` belongs to the agent's own session, so that retrying the
/// same session cannot clear it: its context is full, or the vendor refuses
/// the content the session holds. Anything else, and anything unknown, is
/// systemic: a provider, model, auth, network, host or store error that the
/// next success clears.
pub fn is_session_turn_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    // Named as the fleet's even when the wording also names the request.
    const SYSTEMIC: &[&str] = &[
        "model_not_found",
        "model not found",
        "auth",
        "credential",
        "unauthorized",
        "forbidden",
        "api key",
        "api_key",
        "rate limit",
        "rate_limit",
        "quota",
        "overloaded",
        "unavailable",
        "timeout",
        "timed out",
        "stalled",
        "database is locked",
        "already has an active turn",
    ];
    if SYSTEMIC.iter().any(|needle| lower.contains(needle)) {
        return false;
    }
    const SESSION: &[&str] = &[
        "context_length_exceeded",
        "context_exhausted",
        "context window",
        "context length",
        "maximum context",
        "prompt is too long",
        "tool_use",
        "tool_result",
        "roles must alternate",
    ];
    SESSION.iter().any(|needle| lower.contains(needle))
}

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
    /// Record a turn the harness ended with `error`, and return the strikes
    /// in a row: a `strike` (an error of the session's own) adds one, any
    /// other error leaves them as they are. The turn stays open until
    /// [`Store::post_errored_turn_note`] closes it.
    pub fn note_turn_errored(
        &self,
        agent_id: &AgentId,
        error: &str,
        strike: bool,
    ) -> Result<i64, Error> {
        let add = i64::from(strike);
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.execute(
                "INSERT INTO turn_errors (agent_id, streak, last_error, last_error_at_ms)
                 VALUES (?1, ?4, ?2, ?3)
                 ON CONFLICT(agent_id) DO UPDATE SET
                     streak = turn_errors.streak + ?4,
                     last_error = excluded.last_error,
                     last_error_at_ms = excluded.last_error_at_ms",
                params![agent_id.as_str(), error, now_ms(), add],
            )?;
            Ok(tx.query_row(
                "SELECT streak FROM turn_errors WHERE agent_id = ?1",
                [agent_id.as_str()],
                |row| row.get::<_, i64>(0),
            )?)
        })
    }

    /// Store the note that carries an errored turn's work in the agent's own
    /// mailbox and close that turn with its error, in one transaction: the
    /// turn open through `opened_seq` only, never one a later message
    /// opened. A note already stored under its source key closes it too.
    pub fn post_errored_turn_note(
        &self,
        agent_id: &AgentId,
        opened_seq: i64,
        note: Incoming,
    ) -> Result<(), Error> {
        let close = |conn: &rusqlite::Connection| -> Result<bool, Error> {
            conn.execute(
                "UPDATE turn_recovery
                 SET completed_seq = opened_seq,
                     recovery_count = 0
                 WHERE agent_id = ?1 AND opened_seq = ?2 AND completed_seq < opened_seq",
                params![agent_id.as_str(), opened_seq],
            )?;
            Ok(true)
        };
        self.receive_with_admission(agent_id, note, false, close)?;
        Ok(())
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

    fn note(text: &str, key: &str) -> Incoming {
        let mut note = Incoming::new(text);
        note.sender = Some(crate::SYSTEM_SENDER.into());
        note.source_key = Some(key.into());
        note
    }

    #[test]
    fn only_an_error_of_the_session_itself_is_a_strike() {
        for strike in [
            r#"Sylphx AI failure Some("context_length_exceeded"): The request exceeds the model's context window."#,
            "context_exhausted",
            "invalid_request_error: prompt is too long: 210000 tokens > 200000 maximum",
            "messages.12: `tool_use` ids were found without `tool_result` blocks",
        ] {
            assert!(super::is_session_turn_error(strike), "{strike}");
        }
        // What the live fleet logged that is the provider's, the host's or
        // unknown: never a strike.
        for systemic in [
            r#"Sylphx AI failure Some("model_not_found"): The model does not exist."#,
            "AUTH_REQUIRED: the provider rejected credentials",
            "Sylphx AI failure (upstream_stalled)",
            "stream_no_output: output timeout after 300s",
            "store: database is locked",
            "This Session already has an active turn",
            r#"Sylphx AI failure Some("service_unavailable"): Retry the request."#,
            "invalid_request_error: context window of model_not_found",
            "something nobody has seen before",
        ] {
            assert!(!super::is_session_turn_error(systemic), "{systemic}");
        }
    }

    #[test]
    fn strikes_count_and_the_note_closes_only_its_own_turn() {
        let (_dir, store) = store();
        let agent = store
            .create_agent(NewAgent::new("worker", "spiron"))
            .unwrap();
        deliver(&store, &agent.id, "first");
        deliver(&store, &agent.id, "second");
        assert_eq!(
            store
                .note_turn_errored(&agent.id, "context_length_exceeded", true)
                .unwrap(),
            1
        );
        // A systemic error is recorded but adds no strike.
        assert_eq!(
            store
                .note_turn_errored(&agent.id, "upstream_stalled", false)
                .unwrap(),
            1
        );
        let open = store.open_turn(&agent.id).unwrap().expect("still open");
        let texts: Vec<_> = open.messages.iter().map(|m| m.text.as_str()).collect();
        assert_eq!(texts, ["first", "second"]);
        let errors = store.turn_errors(&agent.id).unwrap().unwrap();
        assert_eq!(errors.streak, 1);
        assert_eq!(errors.last_error.as_deref(), Some("upstream_stalled"));
        assert!(errors.faulted_at_ms.is_none());

        // A later message extends the turn: a note for the stale one is
        // stored but closes nothing.
        deliver(&store, &agent.id, "third");
        store
            .post_errored_turn_note(&agent.id, open.opened_seq, note("stale", "k1"))
            .unwrap();
        assert!(store.has_open_turn(&agent.id).unwrap());
        // The note and the close of the turn it covers commit together.
        let open = store.open_turn(&agent.id).unwrap().unwrap();
        store
            .post_errored_turn_note(&agent.id, open.opened_seq, note("carries", "k2"))
            .unwrap();
        assert!(!store.has_open_turn(&agent.id).unwrap());
        let last = store.inbox(&agent.id, None).unwrap().pop().unwrap();
        assert_eq!(last.text, "carries");
        assert!(store.next_undelivered(&agent.id).unwrap().is_some());

        // A completed turn ends the run.
        assert_eq!(
            store
                .note_turn_errored(&agent.id, "context_exhausted", true)
                .unwrap(),
            2
        );
        let pending: Vec<i64> = store
            .inbox(&agent.id, None)
            .unwrap()
            .into_iter()
            .filter(|m| m.delivered_at_ms.is_none())
            .map(|m| m.seq)
            .collect();
        for seq in pending {
            store.note_delivered(&agent.id, seq).unwrap();
        }
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
            store
                .note_turn_errored(&agent.id, "context_length_exceeded", true)
                .unwrap();
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
        assert_eq!(
            errors.last_error.as_deref(),
            Some("context_length_exceeded"),
            "history stays"
        );
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
