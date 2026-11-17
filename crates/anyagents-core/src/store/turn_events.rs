use super::*;

/// One coherent observation for a causal terminal-state watch. The recovery
/// cursor can close implicitly; only terminal callbacks advance `turn_seq`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentTurnSnapshot {
    pub turn_seq: i64,
    pub terminal_delivered_seq: i64,
    pub outcome: Option<String>,
    pub delivered_seq: i64,
    pub pending_mail: bool,
    pub open_turn: bool,
}

pub(super) fn record_terminal(
    conn: &Connection,
    agent_id: &AgentId,
    outcome: &str,
) -> Result<(), Error> {
    let seq = conn.query_row(
        "SELECT COALESCE((SELECT delivered_seq FROM mailbox_cursors WHERE agent_id = ?1), 0)",
        [agent_id.as_str()],
        |row| row.get(0),
    )?;
    record_terminal_for(conn, agent_id, seq, outcome)?;
    Ok(())
}

/// Accept only the delivery owned by the caller, never infer callback custody
/// from the cursor that a newer delivery may already have advanced.
fn record_terminal_for(
    conn: &Connection,
    agent_id: &AgentId,
    delivered_seq: i64,
    outcome: &str,
) -> Result<bool, Error> {
    let changed = conn.execute(
        "INSERT INTO agent_turn_events(agent_id, turn_seq, delivered_seq, outcome)
         SELECT agent_id, 1, delivered_seq, ?3 FROM mailbox_cursors
         WHERE agent_id = ?1 AND delivered_seq = ?2 AND delivered_seq > 0
         ON CONFLICT(agent_id) DO UPDATE SET
           turn_seq = agent_turn_events.turn_seq + 1,
           delivered_seq = excluded.delivered_seq,
           outcome = excluded.outcome
         WHERE excluded.delivered_seq > agent_turn_events.delivered_seq
            OR (excluded.delivered_seq = agent_turn_events.delivered_seq
                AND agent_turn_events.outcome = 'errored' AND excluded.outcome = 'completed')",
        params![agent_id.as_str(), delivered_seq, outcome],
    )?;
    Ok(changed != 0)
}

impl Store {
    /// Complete callback-owned delivery custody exactly once. A stale or
    /// duplicate callback returns false without changing recovery or errors.
    pub fn note_turn_completed_for(
        &self,
        agent_id: &AgentId,
        delivered_seq: i64,
    ) -> Result<bool, Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            if !record_terminal_for(tx, agent_id, delivered_seq, "completed")? {
                return Ok(false);
            }
            tx.execute(
                "UPDATE turn_recovery SET completed_seq = opened_seq, recovery_count = 0
                 WHERE agent_id = ?1 AND completed_seq < opened_seq AND opened_seq <= ?2",
                params![agent_id.as_str(), delivered_seq],
            )?;
            super::turn_errors::end_run(tx, agent_id)?;
            Ok(true)
        })
    }

    /// Record an error only for this callback's exact delivery custody. False
    /// means no metadata, strike, recovery or terminal event was changed.
    pub fn note_turn_errored_for(
        &self,
        agent_id: &AgentId,
        delivered_seq: i64,
        error: &str,
        strike: bool,
    ) -> Result<bool, Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            if !record_terminal_for(tx, agent_id, delivered_seq, "errored")? {
                return Ok(false);
            }
            super::turn_errors::record_error(tx, agent_id, error, strike)?;
            Ok(true)
        })
    }

    pub fn agent_turn_snapshot(&self, agent_id: &AgentId) -> Result<AgentTurnSnapshot, Error> {
        let conn = self.read_conn();
        let tx = conn.unchecked_transaction()?;
        agent_exists(&tx, agent_id)?;
        let snapshot = tx.query_row(
            "SELECT COALESCE(e.turn_seq, 0), COALESCE(e.delivered_seq, 0), e.outcome,
                    COALESCE(c.delivered_seq, 0),
                    EXISTS(SELECT 1 FROM messages m WHERE m.agent_id = a.id
                           AND m.seq > COALESCE(c.delivered_seq, 0)
                           AND (CASE WHEN json_valid(m.context) THEN json_extract(m.context, '$.event') END)
                               IS NOT 'reaction_passive'),
                    COALESCE(r.opened_seq > r.completed_seq, 0)
             FROM agents a
             LEFT JOIN agent_turn_events e ON e.agent_id = a.id
             LEFT JOIN mailbox_cursors c ON c.agent_id = a.id
             LEFT JOIN turn_recovery r ON r.agent_id = a.id
             WHERE a.id = ?1",
            [agent_id.as_str()],
            |row| Ok(AgentTurnSnapshot {
                turn_seq: row.get(0)?, terminal_delivered_seq: row.get(1)?, outcome: row.get(2)?,
                delivered_seq: row.get(3)?, pending_mail: row.get(4)?, open_turn: row.get(5)?,
            }),
        )?;
        tx.commit()?;
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn terminal_ledger_is_causal_and_duplicate_safe() {
        let (_tmp, store, agent) = super::super::runtime_tests::setup();
        assert_eq!(store.agent_turn_snapshot(&agent.id).unwrap().turn_seq, 0);
        store.note_turn_completed(&agent.id).unwrap();
        assert_eq!(store.agent_turn_snapshot(&agent.id).unwrap().turn_seq, 0);
        let msg = super::super::runtime_tests::receive(&store, &agent, "first");
        assert!(store.agent_turn_snapshot(&agent.id).unwrap().pending_mail);
        store.note_delivered(&agent.id, msg.seq).unwrap();
        assert_eq!(store.agent_turn_snapshot(&agent.id).unwrap().turn_seq, 0);
        store.note_turn_errored(&agent.id, "retry", false).unwrap();
        let errored = store.agent_turn_snapshot(&agent.id).unwrap();
        assert_eq!(errored.turn_seq, 1);
        assert_eq!(errored.outcome.as_deref(), Some("errored"));
        store
            .note_turn_errored(&agent.id, "same retry", false)
            .unwrap();
        assert_eq!(store.agent_turn_snapshot(&agent.id).unwrap().turn_seq, 1);
        store.note_turn_completed(&agent.id).unwrap();
        let done = store.agent_turn_snapshot(&agent.id).unwrap();
        assert_eq!(done.turn_seq, 2);
        assert!(!done.open_turn);
        assert!(!done.pending_mail);
        store.note_turn_completed(&agent.id).unwrap();
        store.note_turn_errored(&agent.id, "stale", false).unwrap();
        assert_eq!(store.agent_turn_snapshot(&agent.id).unwrap(), done);
    }
    #[test]
    fn checked_terminal_callbacks_do_not_take_newer_custody_or_duplicate_effects() {
        let (_tmp, store, agent) = super::super::runtime_tests::setup();
        assert!(!store.note_turn_completed_for(&agent.id, 0).unwrap());
        assert!(
            !store
                .note_turn_errored_for(&agent.id, -1, "invalid", true)
                .unwrap()
        );
        assert!(store.turn_errors(&agent.id).unwrap().is_none());
        let first = super::super::runtime_tests::receive(&store, &agent, "first");
        store.note_delivered(&agent.id, first.seq).unwrap();
        assert!(
            store
                .note_turn_errored_for(&agent.id, first.seq, "session", true)
                .unwrap()
        );
        let error = store.turn_errors(&agent.id).unwrap();
        let failed = store.agent_turn_snapshot(&agent.id).unwrap();
        assert!(
            !store
                .note_turn_errored_for(&agent.id, first.seq, "duplicate", true)
                .unwrap()
        );
        assert_eq!(store.turn_errors(&agent.id).unwrap(), error);
        assert_eq!(store.agent_turn_snapshot(&agent.id).unwrap(), failed);
        assert!(store.note_turn_completed_for(&agent.id, first.seq).unwrap());
        assert_eq!(store.turn_errors(&agent.id).unwrap().unwrap().streak, 0);
        let done = store.agent_turn_snapshot(&agent.id).unwrap();
        assert!(!store.note_turn_completed_for(&agent.id, first.seq).unwrap());
        assert!(
            !store
                .note_turn_errored_for(&agent.id, first.seq, "late", true)
                .unwrap()
        );
        assert_eq!(store.agent_turn_snapshot(&agent.id).unwrap(), done);
        let second = super::super::runtime_tests::receive(&store, &agent, "second");
        store.note_delivered(&agent.id, second.seq).unwrap();
        assert!(store.agent_turn_snapshot(&agent.id).unwrap().open_turn);
        assert!(!store.note_turn_completed_for(&agent.id, first.seq).unwrap());
        assert!(
            !store
                .note_turn_errored_for(&agent.id, first.seq, "stale", true)
                .unwrap()
        );
        assert!(store.agent_turn_snapshot(&agent.id).unwrap().open_turn);
        assert_eq!(store.turn_errors(&agent.id).unwrap().unwrap().streak, 0);
        assert!(
            store
                .note_turn_completed_for(&agent.id, second.seq)
                .unwrap()
        );
        assert!(!store.agent_turn_snapshot(&agent.id).unwrap().open_turn);
    }

    #[test]
    fn checked_terminal_callbacks_serialize_concurrent_duplicates() {
        let (_tmp, store, agent) = super::super::runtime_tests::setup();
        let msg = super::super::runtime_tests::receive(&store, &agent, "first");
        store.note_delivered(&agent.id, msg.seq).unwrap();
        let accepted = std::thread::scope(|scope| {
            let calls: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        store
                            .note_turn_errored_for(&agent.id, msg.seq, "one strike", true)
                            .unwrap()
                    })
                })
                .collect();
            calls
                .into_iter()
                .map(|call| call.join().unwrap())
                .filter(|accepted| *accepted)
                .count()
        });
        assert_eq!(accepted, 1);
        assert_eq!(store.turn_errors(&agent.id).unwrap().unwrap().streak, 1);
        assert_eq!(store.agent_turn_snapshot(&agent.id).unwrap().turn_seq, 1);
    }
}
