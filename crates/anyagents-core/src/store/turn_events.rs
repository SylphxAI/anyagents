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
    if record_terminal_for(conn, agent_id, seq, outcome)? {
        revoke_task_contexts_through(conn, agent_id, seq)?;
    }
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

fn revoke_task_contexts_through(
    conn: &Connection,
    child_id: &AgentId,
    seq: i64,
) -> Result<(), Error> {
    conn.execute(
        "DELETE FROM task_turn_contexts WHERE child_id = ?1 AND opener_seq <= ?2 AND closed_delivery_seq IS NULL",
        params![child_id.as_str(), seq],
    )?;
    Ok(())
}

fn task_return_on(
    tx: &Connection,
    child_id: &AgentId,
    delivered_seq: i64,
    opener_id: &str,
    output: &str,
) -> Result<Option<ReceiveTx>, Error> {
    // Persist the authority of the exact result even when schedules or an
    // explicit send suppress the return; legacy idle notices must not run.
    let sent_to_parent = tx
        .query_row(
            "UPDATE task_turn_contexts SET closed_delivery_seq = ?4
         WHERE child_id = ?1 AND opener_id = ?2 AND opener_seq <= ?3
           AND closed_delivery_seq IS NULL RETURNING sent_to_parent_id",
            params![child_id.as_str(), opener_id, delivered_seq, delivered_seq],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    let Some(sent_to_parent) = sent_to_parent else {
        return Ok(None);
    };
    // Only the latest authoritative completion is needed by idle release;
    // a stale result cannot move the terminal event backwards.
    tx.execute(
        "DELETE FROM task_turn_contexts WHERE child_id = ?1 AND opener_id != ?2 AND opener_seq <= ?3",
        params![child_id.as_str(), opener_id, delivered_seq],
    )?;
    let child = agent_by_id(tx, child_id)?;
    if !child.is_task_subagent() || child.state == "archived" {
        return Ok(None);
    }
    let Some(parent_id) = child.parent_id else {
        return Ok(None);
    };
    let scheduled: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_schedules WHERE agent_id = ?1 AND state = 'active')",
        [child_id.as_str()],
        |row| row.get(0),
    )?;
    if scheduled || sent_to_parent.as_deref() == Some(parent_id.as_str()) {
        return Ok(None);
    }
    let mut incoming = Incoming::new(format!(
        "<task_return agent_id=\"{}\" delivered_seq=\"{delivered_seq}\">{}</task_return>",
        child_id,
        xml_escape_attr_local(output),
    ));
    incoming.sender = Some(format!("agent:{child_id}"));
    let key = format!("anyagents:task-return:{child_id}:{opener_id}");
    Store::receive_on(
        tx,
        &parent_id,
        incoming,
        Some(key),
        now_ms(),
        IngressHistory::default(),
        false,
        Some("task_return"),
        &mut Vec::new(),
    )
    .map(Some)
}

impl Store {
    pub fn task_result_inputs_at(
        &self,
        agent_id: &AgentId,
        seq: i64,
    ) -> Result<Vec<String>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached(
            "SELECT id FROM messages WHERE agent_id = ?1 AND seq = ?2
             UNION ALL SELECT opener_id FROM task_turn_contexts
             WHERE child_id = ?1 AND opener_seq = ?2 AND synthetic_recovery = 1
               AND closed_delivery_seq IS NULL",
        )?;
        Ok(stmt
            .query_map(params![agent_id.as_str(), seq], |row| row.get(0))?
            .collect::<Result<_, _>>()?)
    }

    /// Allocate transport-only context for a task mailbox opener before the
    /// first prompt byte can reach the engine. Retries of the same request
    /// keep the same token; a closed or non-task input cannot acquire one.
    pub fn prepare_task_turn_context(
        &self,
        child_id: &AgentId,
        opener_id: &str,
    ) -> Result<Option<String>, Error> {
        self.write_tx(|tx| {
            let child = agent_by_id(tx, child_id)?;
            if !child.is_task_subagent() || child.state != "running" {
                return Ok(None);
            }
            tx.execute(
                "INSERT INTO task_turn_contexts(child_id, opener_id, opener_seq, token)
                 SELECT agent_id, id, seq, ?3 FROM messages
                 WHERE agent_id = ?1 AND id = ?2 AND seq > COALESCE(
                     (SELECT delivered_seq FROM mailbox_cursors WHERE agent_id = ?1), 0)
                 ON CONFLICT(child_id, opener_id) DO NOTHING",
                params![child_id.as_str(), opener_id, uuid::Uuid::new_v4().simple().to_string()],
            )?;
            Ok(tx.query_row(
                "SELECT token FROM task_turn_contexts WHERE child_id = ?1 AND opener_id = ?2 AND closed_delivery_seq IS NULL",
                params![child_id.as_str(), opener_id],
                |row| row.get(0),
            ).optional()?)
        })
    }

    /// A crash-recovery note is a real new engine opener, but not an inbox
    /// row. Bind it to the exact durable open turn snapshot it describes.
    pub fn prepare_recovery_task_turn_context(
        &self,
        child_id: &AgentId,
        opener_id: &str,
        opened_seq: i64,
        recovery_count: i64,
    ) -> Result<Option<String>, Error> {
        self.write_tx(|tx| {
            let child = agent_by_id(tx, child_id)?;
            if !child.is_task_subagent() || child.state != "running" {
                return Ok(None);
            }
            tx.execute(
                "INSERT INTO task_turn_contexts(child_id, opener_id, opener_seq, token, synthetic_recovery)
                 SELECT agent_id, ?2, opened_seq, ?5, 1 FROM turn_recovery
                 WHERE agent_id = ?1 AND opened_seq = ?3 AND recovery_count = ?4 AND opened_seq > completed_seq
                 ON CONFLICT(child_id, opener_id) DO NOTHING",
                params![child_id.as_str(), opener_id, opened_seq, recovery_count, uuid::Uuid::new_v4().simple().to_string()],
            )?;
            Ok(tx.query_row(
                "SELECT token FROM task_turn_contexts WHERE child_id = ?1 AND opener_id = ?2
                 AND opener_seq = ?3 AND synthetic_recovery = 1 AND closed_delivery_seq IS NULL",
                params![child_id.as_str(), opener_id, opened_seq],
                |row| row.get(0),
            ).optional()?)
        })
    }

    /// Complete callback-owned delivery custody exactly once. A stale or
    /// duplicate callback returns false without changing recovery or errors.
    pub fn note_turn_completed_for(
        &self,
        agent_id: &AgentId,
        delivered_seq: i64,
    ) -> Result<bool, Error> {
        self.note_turn_completed_with_output_for(agent_id, delivered_seq, None)
    }

    /// Commit a terminal callback and its implicit task return together. The
    /// output must come from the harness for this exact delivery, not a recent
    /// transcript row or a user-supplied assertion of completion.
    pub fn note_turn_completed_with_output_for(
        &self,
        agent_id: &AgentId,
        delivered_seq: i64,
        exact_output: Option<(&str, &str)>,
    ) -> Result<bool, Error> {
        let exact_output = exact_output.map(|(opener, text)| {
            (
                opener,
                crate::masking::mask_known_secrets(text.trim())
                    .chars()
                    .take(16_384)
                    .collect::<String>(),
            )
        });
        let (accepted, notice) = self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            if !record_terminal_for(tx, agent_id, delivered_seq, "completed")? {
                return Ok((false, None));
            }
            tx.execute(
                "UPDATE turn_recovery SET completed_seq = opened_seq, recovery_count = 0
                 WHERE agent_id = ?1 AND completed_seq < opened_seq AND opened_seq <= ?2",
                params![agent_id.as_str(), delivered_seq],
            )?;
            super::turn_errors::end_run(tx, agent_id)?;
            let notice = exact_output
                .as_ref()
                .map(|(opener, text)| task_return_on(tx, agent_id, delivered_seq, opener, text))
                .transpose()?
                .flatten();
            revoke_task_contexts_through(tx, agent_id, delivered_seq)?;
            Ok((true, notice))
        })?;
        if let Some(notice) = notice {
            self.finish_receive(notice, &[]);
        }
        Ok(accepted)
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
            revoke_task_contexts_through(tx, agent_id, delivered_seq)?;
            super::turn_errors::record_error(tx, agent_id, error, strike)?;
            Ok(true)
        })
    }

    /// Record a turn that a transient provider capacity fault ended, for this
    /// callback's exact delivery custody, without a terminal outcome: its work
    /// is retried, so a watcher of the agent still sees the turn in progress.
    /// The error is kept for diagnosis and is never a strike. False means
    /// nothing was changed (stale, duplicate or unowned custody).
    pub fn note_turn_retrying_for(
        &self,
        agent_id: &AgentId,
        delivered_seq: i64,
        error: &str,
    ) -> Result<bool, Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            let owned: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM mailbox_cursors
                 WHERE agent_id = ?1 AND delivered_seq = ?2 AND delivered_seq > 0)",
                params![agent_id.as_str(), delivered_seq],
                |row| row.get(0),
            )?;
            if !owned {
                return Ok(false);
            }
            revoke_task_contexts_through(tx, agent_id, delivered_seq)?;
            super::turn_errors::record_error(tx, agent_id, error, false)?;
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
    use super::*;

    fn task_fixture() -> (tempfile::TempDir, Store, Agent, Agent) {
        let (dir, store, parent) = super::super::runtime_tests::setup();
        let mut child = NewAgent::new("child", "spiron");
        child.parent_id = Some(parent.id.clone());
        let child = store.create_agent(child).unwrap();
        assert!(child.is_task_subagent());
        let child = store.set_agent_state(&child.id, "running").unwrap();
        (dir, store, parent, child)
    }

    #[test]
    fn task_return_is_exactly_once_and_only_for_the_genuine_opener() {
        let (_dir, store, parent, child) = task_fixture();
        let first = super::super::runtime_tests::receive(&store, &child, "open turn");
        let token = store
            .prepare_task_turn_context(&child.id, first.id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .prepare_task_turn_context(&child.id, first.id.as_str())
                .unwrap()
                .as_deref(),
            Some(token.as_str())
        );
        store.note_delivered(&child.id, first.seq).unwrap();
        let steer = super::super::runtime_tests::receive(&store, &child, "follow-up");
        store.note_delivered(&child.id, steer.seq).unwrap();
        let result = (first.id.as_str(), "Finished <proof> & tests");
        assert!(
            !store
                .note_turn_completed_with_output_for(&child.id, first.seq, Some(result))
                .unwrap()
        );
        assert!(store.inbox(&parent.id, None).unwrap().is_empty());
        assert!(
            store
                .note_turn_completed_with_output_for(&child.id, steer.seq, Some(result))
                .unwrap()
        );
        assert!(
            !store
                .note_turn_completed_with_output_for(&child.id, steer.seq, Some(result))
                .unwrap()
        );
        let inbox = store.inbox(&parent.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].context.event.as_deref(), Some("task_return"));
        assert!(inbox[0].text.contains("Finished &lt;proof&gt; &amp; tests"));
    }

    #[test]
    fn only_a_new_parent_delivery_with_this_turns_token_suppresses_return() {
        let (_dir, store, parent, child) = task_fixture();
        let other = store.create_agent(NewAgent::new("other", "test")).unwrap();
        let mut send = Incoming::new("explicit message");
        send.source_key = Some("repeatable-key".into());
        send.sender = Some(format!("agent:{}", child.id));
        for seq in 1..=3 {
            let opener = super::super::runtime_tests::receive(&store, &child, "open turn");
            let token = store
                .prepare_task_turn_context(&child.id, opener.id.as_str())
                .unwrap()
                .unwrap();
            store.note_delivered(&child.id, opener.seq).unwrap();
            assert!(
                store
                    .receive_turn_send(&parent.id, send.clone(), false, &other.id, &token)
                    .is_err()
            );
            let recipient = if seq < 3 { &parent } else { &other };
            let result = store
                .receive_turn_send(&recipient.id, send.clone(), false, &child.id, &token)
                .unwrap();
            assert!(matches!(result, ReceiveOutcome::Duplicate(_)) == (seq == 2));
            assert!(
                store
                    .note_turn_completed_with_output_for(
                        &child.id,
                        opener.seq,
                        Some((opener.id.as_str(), "return text"))
                    )
                    .unwrap()
            );
            assert!(
                store
                    .receive_turn_send(
                        &parent.id,
                        Incoming::new("late send"),
                        false,
                        &child.id,
                        &token
                    )
                    .is_err()
            );
        }
        let inbox = store.inbox(&parent.id, None).unwrap();
        assert_eq!(inbox.len(), 3);
        assert_eq!(
            inbox
                .iter()
                .filter(|m| m.context.event.as_deref() == Some("task_return"))
                .count(),
            2
        );
    }

    #[test]
    fn failed_parent_mail_insert_rolls_back_task_completion() {
        let (_dir, store, parent, child) = task_fixture();
        let opener = super::super::runtime_tests::receive(&store, &child, "open turn");
        store
            .prepare_task_turn_context(&child.id, opener.id.as_str())
            .unwrap()
            .unwrap();
        store.note_delivered(&child.id, opener.seq).unwrap();
        store.conn().execute_batch("CREATE TRIGGER test_fail_task_return BEFORE INSERT ON messages
            WHEN NEW.source_key LIKE 'anyagents:task-return:%' BEGIN SELECT RAISE(ABORT, 'injected fault'); END;").unwrap();
        let result = Some((opener.id.as_str(), "return once"));
        assert!(
            store
                .note_turn_completed_with_output_for(&child.id, opener.seq, result)
                .is_err()
        );
        assert_eq!(store.agent_turn_snapshot(&child.id).unwrap().turn_seq, 0);
        assert!(store.inbox(&parent.id, None).unwrap().is_empty());
        store
            .conn()
            .execute_batch("DROP TRIGGER test_fail_task_return")
            .unwrap();
        assert!(
            store
                .note_turn_completed_with_output_for(&child.id, opener.seq, result)
                .unwrap()
        );
        assert_eq!(store.inbox(&parent.id, None).unwrap().len(), 1);
    }

    #[test]
    fn scheduled_archived_and_standing_children_do_not_return() {
        for case in ["scheduled", "archived", "standing"] {
            let (_dir, store, parent, child) = task_fixture();
            let opener = super::super::runtime_tests::receive(&store, &child, case);
            if case == "standing" {
                store.set_agent_lifecycle(&child.id, "standing").unwrap();
            }
            let token = store
                .prepare_task_turn_context(&child.id, opener.id.as_str())
                .unwrap();
            assert_eq!(token.is_some(), case != "standing");
            store.note_delivered(&child.id, opener.seq).unwrap();
            if case == "scheduled" {
                store
                    .create_schedule(NewSchedule {
                        agent_id: child.id.clone(),
                        kind: "at".into(),
                        spec: "future".into(),
                        prompt: "future work".into(),
                        interval_ms: None,
                        next_fire_at_ms: Some(now_ms() + 60_000),
                        watch_baseline: None,
                    })
                    .unwrap();
            } else if case == "archived" {
                store.set_agent_state(&child.id, "archived").unwrap();
                assert!(
                    store
                        .receive_turn_send(
                            &parent.id,
                            Incoming::new("revoked"),
                            false,
                            &child.id,
                            token.as_deref().unwrap()
                        )
                        .is_err()
                );
            }
            assert!(
                store
                    .note_turn_completed_with_output_for(
                        &child.id,
                        opener.seq,
                        Some((opener.id.as_str(), "must not return"))
                    )
                    .unwrap()
            );
            assert!(store.inbox(&parent.id, None).unwrap().is_empty(), "{case}");
        }
    }

    #[test]
    fn synthetic_recovery_is_bound_to_its_durable_span() {
        let (_dir, store, parent, child) = task_fixture();
        let opener = super::super::runtime_tests::receive(&store, &child, "interrupted work");
        store.note_delivered(&child.id, opener.seq).unwrap();
        let open = store.open_turn(&child.id).unwrap().unwrap();
        assert!(
            store
                .prepare_recovery_task_turn_context(
                    &child.id,
                    "recovery-stale",
                    open.opened_seq,
                    open.recovery_count + 1
                )
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .prepare_recovery_task_turn_context(
                    &child.id,
                    "recovery-exact",
                    open.opened_seq,
                    open.recovery_count
                )
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store.task_result_inputs_at(&child.id, opener.seq).unwrap(),
            [opener.id.to_string(), "recovery-exact".into()]
        );
        assert!(
            store
                .note_turn_completed_with_output_for(
                    &child.id,
                    opener.seq,
                    Some(("recovery-exact", "recovered answer"))
                )
                .unwrap()
        );
        let rows = store.inbox(&parent.id, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].text.contains(">recovered answer</task_return>"));
    }

    #[test]
    fn a_capacity_retry_keeps_the_turn_open_without_a_terminal_outcome() {
        let (_tmp, store, agent) = super::super::runtime_tests::setup();
        let msg = super::super::runtime_tests::receive(&store, &agent, "work");
        // Custody the cursor has not reached is not owned.
        assert!(
            !store
                .note_turn_retrying_for(&agent.id, msg.seq, "capacity")
                .unwrap()
        );
        assert!(store.turn_errors(&agent.id).unwrap().is_none());
        store.note_delivered(&agent.id, msg.seq).unwrap();
        let error = r#"Sylphx AI failure Some("included_capacity_unavailable"): Retry later."#;
        assert!(
            store
                .note_turn_retrying_for(&agent.id, msg.seq, error)
                .unwrap()
        );
        let snap = store.agent_turn_snapshot(&agent.id).unwrap();
        assert_eq!(snap.turn_seq, 0, "no terminal outcome for a retried turn");
        assert_eq!(snap.outcome, None);
        assert!(snap.open_turn, "the work stays open for its retry");
        let errors = store.turn_errors(&agent.id).unwrap().unwrap();
        assert_eq!(errors.streak, 0, "a capacity fault is never a strike");
        assert_eq!(errors.last_error.as_deref(), Some(error));
        // The retried work completes as any turn does.
        assert!(store.note_turn_completed_for(&agent.id, msg.seq).unwrap());
        let done = store.agent_turn_snapshot(&agent.id).unwrap();
        assert_eq!(
            (done.turn_seq, done.outcome.as_deref()),
            (1, Some("completed"))
        );
        assert!(!done.open_turn);
    }

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
