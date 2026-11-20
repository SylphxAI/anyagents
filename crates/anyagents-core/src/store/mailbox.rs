//! Mailbox inbox, delivery cursors, outbound recording, rejections, and open turns.

use super::helpers::{agent_by_id, agent_exists, message_from_row_with_cursor_db};
use super::types::{
    Incoming, Lag, Message, OpenTurn, OutboundEffect, ReceiveOutcome, RejectedMessage,
    TurnOwnershipSnapshot,
};
use super::{MESSAGE_COLS, Store, memory_index, now_ms, turn_errors, turn_events};
use crate::db::{DbBackend, DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;
use crate::id::{AgentId, MessageId};

impl Store {
    /// Messages with `seq > since`, in sequence order. `None` starts at the beginning.
    pub fn inbox(&self, agent_id: &AgentId, since: Option<i64>) -> Result<Vec<Message>, Error> {
        let conn = self.read_conn();
        agent_exists(&conn, agent_id)?;
        let delivered = cursor(&conn, agent_id)?;
        conn.db_query_vec(
            &format!(
                "SELECT {MESSAGE_COLS}
                 FROM messages WHERE agent_id = ?1 AND seq > ?2 ORDER BY seq"
            ),
            &db_params![agent_id.as_str(), since.unwrap_or(i64::MIN)],
            |row| message_from_row_with_cursor_db(row, delivered),
        )
    }

    /// Latest matching messages, limited in SQL and returned in sequence order.
    pub fn inbox_query(
        &self,
        agent_id: &AgentId,
        since: Option<i64>,
        limit: Option<usize>,
        unread_only: bool,
    ) -> Result<Vec<Message>, Error> {
        self.inbox_query_filtered(agent_id, since, limit, unread_only, None)
    }

    pub fn inbox_query_filtered(
        &self,
        agent_id: &AgentId,
        since: Option<i64>,
        limit: Option<usize>,
        unread_only: bool,
        sender: Option<&str>,
    ) -> Result<Vec<Message>, Error> {
        let conn = self.read_conn();
        agent_exists(&conn, agent_id)?;
        let delivered = cursor(&conn, agent_id)?;
        let lower = since
            .unwrap_or(i64::MIN)
            .max(if unread_only { delivered } else { i64::MIN });
        let mut messages = conn.db_query_vec(
            &format!(
                "SELECT {MESSAGE_COLS} FROM messages
                 WHERE agent_id = ?1 AND seq > ?2
                   AND (?3 IS NULL OR sender = ?3)
                 ORDER BY seq DESC LIMIT ?4"
            ),
            &db_params![
                agent_id.as_str(),
                lower,
                sender,
                limit
                    .map(|n| i64::try_from(n).unwrap_or(i64::MAX))
                    .unwrap_or(i64::MAX)
            ],
            |row| message_from_row_with_cursor_db(row, delivered),
        )?;
        messages.reverse();
        Ok(messages)
    }

    /// Record an outbound message sent by `agent_id` to `destination` (a user,
    /// a channel chat/topic, a group, or another agent) so the agent's full
    /// two-way conversation can be read by authorized supervising agents.
    pub fn record_outbound(
        &self,
        agent_id: &AgentId,
        destination: &str,
        reply_to: Option<&str>,
        body: &str,
    ) -> Result<String, Error> {
        self.record_outbound_id(
            format!("out_{}", MessageId::generate()),
            agent_id,
            destination,
            reply_to,
            body,
        )
    }

    /// Settle the history of an already-completed effect without repeating it.
    /// Keys are scoped to the immutable sender. Exact retries keep the original
    /// ID/time; changed destination, reply or body refuses without mutation.
    pub fn record_outbound_once(
        &self,
        effect_key: &str,
        agent_id: &AgentId,
        destination: &str,
        reply_to: Option<&str>,
        body: &str,
    ) -> Result<String, Error> {
        if effect_key.trim().is_empty() {
            return Err(Error::Message("empty outbound effect key".into()));
        }
        let id = format!(
            "out_{}",
            crate::learned_memory::sha256(format!("{agent_id}:{effect_key}").as_bytes())
        );
        self.record_outbound_id(id, agent_id, destination, reply_to, body)
    }

    fn record_outbound_id(
        &self,
        id: String,
        agent_id: &AgentId,
        destination: &str,
        reply_to: Option<&str>,
        body: &str,
    ) -> Result<String, Error> {
        let id = self.write_tx(|tx| {
            agent_by_id(tx, agent_id)?;
            let ts_ms = now_ms();
            // `seq` places the turn in the mailbox: after the last message
            // the agent had received when it sent.
            tx.db_execute(
                "INSERT INTO outbound_messages (id, agent_id, destination, reply_to, body, sent_at_ms, seq)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6,
                         (SELECT COALESCE(MAX(seq), 0) FROM messages WHERE agent_id = ?2))
                 ON CONFLICT(id) DO NOTHING",
                &db_params![id, agent_id.as_str(), destination, reply_to, body, ts_ms],
            )?;
            let is_eq = if tx.backend() == DbBackend::Postgres {
                "IS NOT DISTINCT FROM"
            } else {
                "IS"
            };
            let matches: bool = tx.db_query_one(
                &format!(
                    "SELECT agent_id=?2 AND destination=?3 AND reply_to {is_eq} ?4 AND body=?5
                     FROM outbound_messages WHERE id=?1"
                ),
                &db_params![id, agent_id.as_str(), destination, reply_to, body],
                |r| r.get(0),
            )?;
            if !matches {
                return Err(Error::Message(
                    "outbound effect key payload conflict".into(),
                ));
            }
            Ok(id)
        })?;
        self.project_mailbox();
        Ok(id)
    }

    /// Record a tool invocation or tool output in the history search index (`kind = 'tool'`)
    /// so agents can search with `include_tools` (`true` / `false`), `only_tools` (`true`),
    /// or `tool_name` (`"browser"`, `"agents"`, `"schedule"`, `"send"`, `"bash"`, etc.).
    pub fn record_tool_event(
        &self,
        agent_id: &AgentId,
        tool_name: &str,
        summary: &str,
    ) -> Result<String, Error> {
        // A standalone summary has no mailbox row: the index is where it is
        // kept, and no state transaction is involved.
        let (agent, cur_seq) = {
            let conn = self.read_conn();
            let agent = agent_by_id(&conn, agent_id)?;
            let seq: i64 = conn.db_query_one(
                "SELECT COALESCE(MAX(seq), 0) FROM messages WHERE agent_id = ?1",
                &db_params![agent_id.as_str()],
                |r| r.get(0),
            )?;
            (agent, seq)
        };
        let id = format!("tool_{}", MessageId::generate());
        let clean_tool = tool_name.trim().to_ascii_lowercase();
        let summary = crate::masking::mask_known_secrets(summary);
        let text = if summary.trim().starts_with("[tool") {
            summary.trim().to_string()
        } else {
            format!("[tool:{clean_tool}] {}", summary.trim())
        };
        self.memory_index()
            .record_standalone(&memory_index::Chunk {
                id: &id,
                agent_id: agent_id.as_str(),
                agent_name: &agent.name,
                direction: "out",
                sender: &format!("agent:{}", agent.name),
                destination: &format!("tool:{clean_tool}"),
                timestamp_ms: now_ms(),
                text: &text,
                kind: Some("tool"),
                tool: Some(&clean_tool),
                seq: cur_seq,
            })?;
        Ok(id)
    }

    pub fn turn_ownership_snapshot(
        &self,
        agent_id: &AgentId,
    ) -> Result<TurnOwnershipSnapshot, Error> {
        let conn = self.read_conn();
        let last_in: Option<(i64, i64, Option<String>, Option<String>)> = conn.db_query_opt(
            "SELECT seq, delivered_at_ms, sender, source_key
             FROM messages
             WHERE agent_id = ?1 AND delivered_at_ms IS NOT NULL
             ORDER BY seq DESC LIMIT 1",
            &db_params![agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        let last_outbound_at_ms: Option<i64> = conn.db_query_one(
            "SELECT MAX(sent_at_ms) FROM outbound_messages WHERE agent_id = ?1",
            &db_params![agent_id.as_str()],
            |row| row.get(0),
        )?;
        Ok(match last_in {
            Some((seq, delivered_at_ms, sender, source_key)) => TurnOwnershipSnapshot {
                last_delivered_seq: Some(seq),
                last_delivered_at_ms: Some(delivered_at_ms),
                last_delivered_sender: sender,
                last_delivered_source_key: source_key,
                last_outbound_at_ms,
            },
            None => TurnOwnershipSnapshot {
                last_delivered_seq: None,
                last_delivered_at_ms: None,
                last_delivered_sender: None,
                last_delivered_source_key: None,
                last_outbound_at_ms,
            },
        })
    }

    pub fn has_message_source_key(
        &self,
        agent_id: &AgentId,
        source_key: &str,
    ) -> Result<bool, Error> {
        let exists: bool = self.read_conn().db_query_one(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE agent_id = ?1 AND source_key = ?2)",
            &db_params![agent_id.as_str(), source_key],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    /// The next message the harness has not taken, or `None` when delivery has
    /// caught up.
    pub fn next_undelivered(&self, agent_id: &AgentId) -> Result<Option<Message>, Error> {
        let conn = self.read_conn();
        // One snapshot: a line committed between reading the cursor and the
        // last sequence must not look like a hole in the mailbox.
        let snapshot = conn.snapshot()?;
        agent_exists(&snapshot, agent_id)?;
        let delivered = cursor(&snapshot, agent_id)?;
        let next = delivered + 1;
        if let Some(message) = message_at_on(&snapshot, agent_id, next, delivered)? {
            return Ok(Some(message));
        }
        let received = last_seq(&snapshot, agent_id)?;
        if received > delivered {
            return Err(Error::Message(format!(
                "mailbox sequence has a hole at {next}"
            )));
        }
        Ok(None)
    }

    /// Return up to `limit` undelivered messages for `agent_id` in ascending `seq` order
    /// within a single read snapshot. Used by serverless dispatchers to batch queued mail
    /// instead of draining one old message per webhook.
    pub fn list_undelivered(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> Result<Vec<Message>, Error> {
        let cap = limit.clamp(1, 100) as i64;
        let conn = self.read_conn();
        let snapshot = conn.snapshot()?;
        agent_exists(&snapshot, agent_id)?;
        let delivered = cursor(&snapshot, agent_id)?;
        let rows = snapshot.db_query_vec(
            concat!(
                "SELECT ",
                message_cols!(),
                " FROM messages
                 WHERE agent_id = ?1 AND seq > ?2
                 ORDER BY seq ASC
                 LIMIT ?3"
            ),
            &db_params![agent_id.as_str(), delivered, cap],
            |r| message_from_row_with_cursor_db(r, delivered),
        )?;
        Ok(rows)
    }

    /// List up to `limit` live (`running`) agent IDs that currently have at least one
    /// undelivered message (`messages.seq > COALESCE(mailbox_cursors.delivered_seq, 0)`),
    /// excluding agents that already have an active remote turn or failed within the
    /// recent failure cooldown (`failure_cooldown_ms`), ordered oldest undelivered
    /// message first (FIFO) so backlog draining is starvation-free.
    pub fn list_running_agents_with_undelivered(
        &self,
        limit: usize,
    ) -> Result<Vec<AgentId>, Error> {
        self.list_running_agents_with_undelivered_backoff(limit, 120_000)
    }

    /// Variant of [`Self::list_running_agents_with_undelivered`] with an explicit
    /// cooldown window (in milliseconds) after a failed/timed-out remote turn.
    pub fn list_running_agents_with_undelivered_backoff(
        &self,
        limit: usize,
        failure_cooldown_ms: i64,
    ) -> Result<Vec<AgentId>, Error> {
        let cap = limit.clamp(1, 100) as i64;
        let cooldown_cutoff = now_ms().saturating_sub(failure_cooldown_ms.max(0));
        let conn = self.read_conn();
        let rows: Vec<String> = if conn.backend() == DbBackend::Postgres {
            conn.db_query_vec(
                "SELECT a.id
                 FROM agents a
                 JOIN messages m ON m.agent_id = a.id
                 LEFT JOIN mailbox_cursors c ON c.agent_id = a.id
                 WHERE a.state = 'running'
                   AND m.seq > COALESCE(c.delivered_seq, 0)
                   AND NOT EXISTS (
                       SELECT 1 FROM pending_remote_turns p
                       WHERE p.agent_id = a.id
                         AND p.status IN ('dispatched', 'running')
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM pending_remote_turns p
                       WHERE p.agent_id = a.id
                         AND p.status IN ('failed', 'timed_out', 'cancelled')
                         AND p.last_event_at_ms >= ?1
                         AND NOT EXISTS (
                             SELECT 1 FROM pending_remote_turns p2
                             WHERE p2.agent_id = a.id
                               AND p2.status = 'completed'
                               AND p2.last_event_at_ms > p.last_event_at_ms
                         )
                   )
                 GROUP BY a.id
                 ORDER BY MIN(m.received_at_ms) ASC
                 LIMIT ?2",
                &db_params![cooldown_cutoff, cap],
                |r| r.get::<String>(0),
            )?
        } else {
            conn.db_query_vec(
                "SELECT a.id
                 FROM agents a
                 JOIN messages m ON m.agent_id = a.id
                 LEFT JOIN mailbox_cursors c ON c.agent_id = a.id
                 WHERE a.state = 'running'
                   AND m.seq > COALESCE(c.delivered_seq, 0)
                 GROUP BY a.id
                 ORDER BY MIN(m.received_at_ms) ASC
                 LIMIT ?1",
                &db_params![cap],
                |r| r.get::<String>(0),
            )?
        };
        let mut out = Vec::with_capacity(rows.len());
        for raw in rows {
            if let Ok(id) = AgentId::parse(&raw) {
                if !self.is_postgres()
                    && self
                        .remote_turn_store()
                        .list_active_for_agent(&id)
                        .map(|v| !v.is_empty())
                        .unwrap_or(false)
                {
                    continue;
                }
                out.push(id);
            }
        }
        Ok(out)
    }

    /// Monotonically advance `mailbox_cursors.delivered_seq` up to `seq` and stamp
    /// `messages.delivered_at_ms` for any messages `<= seq` that have not been stamped yet.
    ///
    /// Used by stateless serverless turn settlement, where a single remote turn can
    /// process a batch of multiple undelivered messages in one shot.
    pub fn mark_delivered_up_to(&self, agent_id: &AgentId, seq: i64) -> Result<i64, Error> {
        if seq <= 0 {
            return self.delivered_cursor(agent_id);
        }
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            let now = now_ms();
            tx.db_execute(
                "INSERT INTO mailbox_cursors (agent_id, delivered_seq)
                 VALUES (?1, ?2)
                 ON CONFLICT(agent_id) DO UPDATE SET
                     delivered_seq = CASE
                         WHEN excluded.delivered_seq > mailbox_cursors.delivered_seq
                         THEN excluded.delivered_seq
                         ELSE mailbox_cursors.delivered_seq
                     END",
                &db_params![agent_id.as_str(), seq],
            )?;
            tx.db_execute(
                "UPDATE messages
                 SET delivered_at_ms = COALESCE(delivered_at_ms, ?1)
                 WHERE agent_id = ?2 AND seq <= ?3 AND delivered_at_ms IS NULL",
                &db_params![now, agent_id.as_str(), seq],
            )?;
            cursor(tx, agent_id)
        })
    }

    /// Close a stale `turn_recovery` row (`opened_seq > completed_seq`) when a serverless
    /// replica confirms no remote turn is currently running for `agent_id`. Returns `true`
    /// if a stale open-turn lock was cleared.
    pub fn reconcile_stale_open_turn(&self, agent_id: &AgentId) -> Result<bool, Error> {
        self.write_tx(|tx| {
            let changed = tx.db_execute(
                "UPDATE turn_recovery
                 SET completed_seq = opened_seq, recovery_count = 0
                 WHERE agent_id = ?1 AND opened_seq > completed_seq",
                &db_params![agent_id.as_str()],
            )?;
            Ok(changed > 0)
        })
    }

    /// Close stale `turn_recovery` rows (`opened_seq > completed_seq`) for all running
    /// agents that have no active `pending_remote_turns`. Returns the count cleared.
    pub fn reconcile_all_stale_serverless_open_turns(&self) -> Result<usize, Error> {
        self.write_tx(|tx| {
            let changed = tx.db_execute(
                "UPDATE turn_recovery
                 SET completed_seq = opened_seq, recovery_count = 0
                 WHERE opened_seq > completed_seq
                   AND agent_id IN (SELECT id FROM agents WHERE state = 'running')
                   AND NOT EXISTS (
                       SELECT 1 FROM pending_remote_turns p
                       WHERE p.agent_id = turn_recovery.agent_id
                         AND p.status IN ('dispatched', 'running')
                   )",
                &[],
            )?;
            Ok(changed)
        })
    }

    /// True if there is at least one undelivered message that should wake an idle
    /// agent (`event != "reaction_passive"`). Passive group reactions remain in
    /// the mailbox and are delivered on the agent's next active turn.
    pub fn has_actionable_undelivered(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let conn = self.read_conn();
        agent_exists(&conn, agent_id)?;
        let delivered = cursor(&conn, agent_id)?;
        // Server-side filter (same predicate as the roster query): the
        // per-row Rust JSON parse would scan and deserialize every
        // undelivered line on each idle poll, pinning one core per agent
        // (80 agents x 4 polls/s against a multi-GB mailbox).
        let not_passive = conn.backend().sql_not_passive_reaction("context");
        let found: bool = conn.db_query_one(
            &format!(
                "SELECT EXISTS(
                   SELECT 1 FROM messages
                   WHERE agent_id = ?1 AND seq > ?2
                     AND {not_passive}
                 )"
            ),
            &db_params![agent_id.as_str(), delivered],
            |row| row.get(0),
        )?;
        Ok(found)
    }

    pub fn received_cursor(&self, agent_id: &AgentId) -> Result<i64, Error> {
        let conn = self.read_conn();
        agent_exists(&conn, agent_id)?;
        last_seq(&conn, agent_id)
    }

    /// The one delivery cursor: the last sequence the harness took.
    pub fn delivered_cursor(&self, agent_id: &AgentId) -> Result<i64, Error> {
        let conn = self.read_conn();
        agent_exists(&conn, agent_id)?;
        cursor(&conn, agent_id)
    }

    /// Move the delivery cursor to `seq`. `seq` must be exactly one past the
    /// current cursor, so nothing is skipped.
    pub fn advance_delivered(&self, agent_id: &AgentId, seq: i64) -> Result<i64, Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            bump_delivered(tx, agent_id, seq)?;
            Ok(seq)
        })
    }

    /// Move the cursor back to `seq` for a test that simulates a crash between
    /// the harness taking a line and the commit recording it.
    #[doc(hidden)]
    pub fn rewind_for_test(&self, agent_id: &AgentId, seq: i64) {
        self.write_tx(|tx| {
            tx.db_execute(
                "UPDATE mailbox_cursors SET delivered_seq = ?1 WHERE agent_id = ?2",
                &db_params![seq, agent_id.as_str()],
            )?;
            Ok(())
        })
        .unwrap();
    }

    /// Record that the harness took `seq`, and when, in one commit.
    pub fn note_delivered(&self, agent_id: &AgentId, seq: i64) -> Result<(), Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            bump_delivered(tx, agent_id, seq)?;
            let now = now_ms();
            let prev_seq = (seq - 1).max(0);
            tx.db_execute(
                "UPDATE messages SET delivered_at_ms = ?1 WHERE agent_id = ?2 AND seq = ?3",
                &db_params![now, agent_id.as_str(), seq],
            )?;
            tx.db_execute(
                "INSERT INTO turn_recovery (agent_id, opened_seq, completed_seq, recovery_count, opened_at_ms)
                 VALUES (?1, ?2, ?3, 0, ?4)
                 ON CONFLICT(agent_id) DO UPDATE SET
                     completed_seq = CASE
                         WHEN turn_recovery.completed_seq >= turn_recovery.opened_seq THEN ?3
                         WHEN EXISTS (
                             SELECT 1 FROM outbound_messages
                             WHERE outbound_messages.agent_id = ?1
                               AND outbound_messages.sent_at_ms >= turn_recovery.opened_at_ms
                         ) THEN ?3
                         ELSE turn_recovery.completed_seq
                     END,
                     opened_at_ms = CASE
                         WHEN turn_recovery.completed_seq >= turn_recovery.opened_seq THEN excluded.opened_at_ms
                         WHEN EXISTS (
                             SELECT 1 FROM outbound_messages
                             WHERE outbound_messages.agent_id = ?1
                               AND outbound_messages.sent_at_ms >= turn_recovery.opened_at_ms
                         ) THEN excluded.opened_at_ms
                         ELSE turn_recovery.opened_at_ms
                     END,
                     opened_seq = excluded.opened_seq,
                     recovery_count = 0",
                &db_params![agent_id.as_str(), seq, prev_seq, now],
            )?;
            Ok(())
        })
    }

    /// Mark the agent's current open turn as completed (`completed_seq = opened_seq`).
    /// Transient lock errors are retried within the store's write budget.
    pub fn note_turn_completed(&self, agent_id: &AgentId) -> Result<(), Error> {
        self.write_tx(|tx| {
            // BEGIN IMMEDIATE: a SELECT-then-UPDATE under BEGIN DEFERRED raises
            // SQLITE_BUSY without honouring busy_timeout when a peer is writing.
            agent_exists(tx, agent_id)?;
            tx.db_execute(
                "UPDATE turn_recovery
                 SET completed_seq = opened_seq,
                     recovery_count = 0
                 WHERE agent_id = ?1 AND completed_seq < opened_seq",
                &db_params![agent_id.as_str()],
            )?;
            turn_events::record_terminal(tx, agent_id, "completed")?;
            turn_errors::end_run(tx, agent_id)?;
            Ok(())
        })
    }

    /// Record that one `[Recovery]` wake was delivered for `opened_seq`.
    /// Park the next undelivered message as refused for good by the harness:
    /// the cursor moves past it, no turn opens, and its reason is kept. An
    /// agent sender is told once, in its own mailbox.
    pub fn reject_message(
        &self,
        agent_id: &AgentId,
        seq: i64,
        reason: &str,
    ) -> Result<RejectedMessage, Error> {
        let rejected = self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            let (message_id, sender): (String, Option<String>) = tx.db_query_one(
                "SELECT id, sender FROM messages WHERE agent_id = ?1 AND seq = ?2",
                &db_params![agent_id.as_str(), seq],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            bump_delivered(tx, agent_id, seq)?;
            let now = now_ms();
            tx.db_execute(
                "INSERT INTO rejected_messages (message_id, agent_id, seq, reason, rejected_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(message_id) DO NOTHING",
                &db_params![message_id, agent_id.as_str(), seq, reason, now],
            )?;
            Ok(RejectedMessage {
                message_id,
                agent_id: agent_id.clone(),
                seq,
                sender,
                reason: reason.to_string(),
                rejected_at_ms: now,
                notified_at_ms: None,
            })
        })?;
        // The message is parked either way; a notice that fails is sent again
        // when the agent's delivery loop next starts.
        if let Err(err) = self.notify_rejection(&rejected) {
            eprintln!(
                "anyagents: notice for parked message {}: {err}",
                rejected.message_id
            );
        }
        Ok(rejected)
    }

    /// Tell the sender of a parked message, once: the notice is keyed by the
    /// message, and the parked row records that it was sent. A notice a crash
    /// cut off is sent by `send_pending_rejection_notices`.
    fn notify_rejection(&self, rejected: &RejectedMessage) -> Result<(), Error> {
        if let Some(name) = rejected
            .sender
            .as_deref()
            .and_then(|sender| sender.strip_prefix("agent:"))
            && let Ok(sender) = self.agent_by_name(name)
        {
            let recipient = self
                .agent(&rejected.agent_id)
                .map(|agent| agent.name)
                .unwrap_or_else(|_| rejected.agent_id.as_str().to_string());
            let mut note = Incoming::new(format!(
                "[Undeliverable] Your message anyagents-message:{} to {recipient} was refused by its harness and will not be delivered: {}",
                rejected.message_id, rejected.reason
            ));
            note.sender = Some("anyagents".to_string());
            note.source_key = Some(format!("anyagents:rejected:{}", rejected.message_id));
            self.receive(&sender.id, note)?;
        }
        self.write_tx(|tx| {
            tx.db_execute(
                "UPDATE rejected_messages SET notified_at_ms = ?1 WHERE message_id = ?2",
                &db_params![now_ms(), rejected.message_id],
            )?;
            Ok(())
        })
    }

    /// Send every notice for this agent's parked messages that a crash cut
    /// off.
    pub fn send_pending_rejection_notices(&self, agent_id: &AgentId) -> Result<(), Error> {
        let pending: Vec<RejectedMessage> = self
            .rejected_messages(agent_id)?
            .into_iter()
            .filter(|rejected| rejected.notified_at_ms.is_none())
            .collect();
        for rejected in pending {
            self.notify_rejection(&rejected)?;
        }
        Ok(())
    }

    /// Put a parked message back in the mailbox: the same body, author and
    /// origin, as a new message at the end, and the parked record removed.
    /// Re-queueing the same parking twice adds it once.
    pub fn requeue_rejected(&self, agent_id: &AgentId, message_id: &str) -> Result<Message, Error> {
        let rejected_at_ms: Option<i64> = self.read_conn().db_query_opt(
            "SELECT rejected_at_ms FROM rejected_messages
             WHERE agent_id = ?1 AND message_id = ?2",
            &db_params![agent_id.as_str(), message_id],
            |row| row.get(0),
        )?;
        let Some(rejected_at_ms) = rejected_at_ms else {
            return Err(Error::Message(format!(
                "{message_id} is not a parked message of this agent"
            )));
        };
        let Some(original) = self.message(agent_id, message_id)? else {
            return Err(Error::Message(format!("{message_id} is no longer stored")));
        };
        let incoming = Incoming {
            text: original.text.clone(),
            sender: original.sender.clone(),
            source_time_ms: original.source_time_ms,
            source_key: Some(format!("anyagents:requeue:{message_id}:{rejected_at_ms}")),
            reply_to: original.reply_to.clone(),
            quote: None,
            origin: original.origin,
            received_at_ms: None,
        };
        let message = match self.receive(agent_id, incoming)? {
            ReceiveOutcome::New(message) | ReceiveOutcome::Duplicate(message) => message,
        };
        self.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM rejected_messages WHERE agent_id = ?1 AND message_id = ?2",
                &db_params![agent_id.as_str(), message_id],
            )?;
            Ok(())
        })?;
        Ok(message)
    }

    /// Messages the harness refused for good, oldest first.
    pub fn rejected_messages(&self, agent_id: &AgentId) -> Result<Vec<RejectedMessage>, Error> {
        self.read_conn().db_query_vec(
            "SELECT r.message_id, r.seq, m.sender, r.reason, r.rejected_at_ms, r.notified_at_ms
             FROM rejected_messages r LEFT JOIN messages m ON m.id = r.message_id
             WHERE r.agent_id = ?1 ORDER BY r.seq",
            &db_params![agent_id.as_str()],
            |row| {
                Ok(RejectedMessage {
                    message_id: row.get(0)?,
                    agent_id: agent_id.clone(),
                    seq: row.get(1)?,
                    sender: row.get(2)?,
                    reason: row.get(3)?,
                    rejected_at_ms: row.get(4)?,
                    notified_at_ms: row.get(5)?,
                })
            },
        )
    }

    pub fn note_turn_recovered(&self, agent_id: &AgentId, opened_seq: i64) -> Result<(), Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.db_execute(
                "UPDATE turn_recovery
                 SET recovery_count = recovery_count + 1
                 WHERE agent_id = ?1 AND opened_seq = ?2",
                &db_params![agent_id.as_str(), opened_seq],
            )?;
            Ok(())
        })
    }

    /// Return `true` when the agent currently has an open turn (`opened_seq > completed_seq`).
    pub fn has_open_turn(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let conn = self.read_conn();
        let row: Option<(i64, i64)> = conn.db_query_opt(
            "SELECT opened_seq, completed_seq FROM turn_recovery WHERE agent_id = ?1",
            &db_params![agent_id.as_str()],
            |r| Ok((r.get::<i64>(0)?, r.get::<i64>(1)?)),
        )?;
        Ok(row.is_some_and(|(opened_seq, completed_seq)| opened_seq > completed_seq))
    }

    /// Return the agent's open turn (`opened_seq > completed_seq`) when it is
    /// eligible for automatic mid-turn recovery.
    pub fn open_turn(&self, agent_id: &AgentId) -> Result<Option<OpenTurn>, Error> {
        let conn = self.read_conn();
        agent_exists(&conn, agent_id)?;
        let row: Option<(i64, i64, i64, i64)> = conn.db_query_opt(
            "SELECT opened_seq, completed_seq, recovery_count, opened_at_ms
             FROM turn_recovery WHERE agent_id = ?1",
            &db_params![agent_id.as_str()],
            |r| {
                Ok((
                    r.get::<i64>(0)?,
                    r.get::<i64>(1)?,
                    r.get::<i64>(2)?,
                    r.get::<i64>(3)?,
                ))
            },
        )?;
        let Some((opened_seq, completed_seq, recovery_count, opened_at_ms)) = row else {
            return Ok(None);
        };
        if opened_seq <= completed_seq {
            return Ok(None);
        }
        let delivered = cursor(&conn, agent_id)?;
        // A message the harness refused for good was never in any turn.
        let messages = conn.db_query_vec(
            concat!(
                "SELECT ",
                message_cols!(),
                " FROM messages
                 WHERE agent_id = ?1 AND seq > ?2 AND seq <= ?3
                   AND id NOT IN (SELECT message_id FROM rejected_messages WHERE agent_id = ?1)
                 ORDER BY seq ASC"
            ),
            &db_params![agent_id.as_str(), completed_seq, opened_seq],
            |r| message_from_row_with_cursor_db(r, delivered),
        )?;
        if messages.is_empty() {
            return Ok(None);
        }
        let completed_outbounds = conn.db_query_vec(
            "SELECT id, destination, reply_to, body, sent_at_ms
             FROM outbound_messages
             WHERE agent_id = ?1 AND sent_at_ms >= ?2
             ORDER BY sent_at_ms ASC, id ASC",
            &db_params![agent_id.as_str(), opened_at_ms],
            |r| {
                Ok(OutboundEffect {
                    id: r.get(0)?,
                    destination: r.get(1)?,
                    reply_to: r.get(2)?,
                    body: r.get(3)?,
                    sent_at_ms: r.get(4)?,
                })
            },
        )?;
        Ok(Some(OpenTurn {
            agent_id: agent_id.clone(),
            opened_seq,
            completed_seq,
            recovery_count,
            opened_at_ms,
            messages,
            completed_outbounds,
        }))
    }

    pub fn message(&self, agent_id: &AgentId, id: &str) -> Result<Option<Message>, Error> {
        let conn = self.read_conn();
        agent_exists(&conn, agent_id)?;
        let delivered = cursor(&conn, agent_id)?;
        conn.db_query_opt(
            concat!(
                "SELECT ",
                message_cols!(),
                " FROM messages WHERE agent_id = ?1 AND id = ?2"
            ),
            &db_params![agent_id.as_str(), id],
            |row| message_from_row_with_cursor_db(row, delivered),
        )
    }

    /// Age of the oldest message the harness has not taken.
    pub fn lag(&self, agent_id: &AgentId, now_ms: i64) -> Result<Lag, Error> {
        let delivered = self.delivered_cursor(agent_id)?;
        Ok(Lag {
            oldest_undelivered_age_ms: self.age_at(agent_id, delivered + 1, now_ms)?,
        })
    }

    fn age_at(&self, agent_id: &AgentId, seq: i64, now_ms: i64) -> Result<Option<i64>, Error> {
        let conn = self.read_conn();
        // Server-side filter: per-row JSON parse here pins a core per agent
        // on every health publish; see `has_actionable_undelivered`.
        let not_passive = conn.backend().sql_not_passive_reaction("context");
        let sql = format!(
            "SELECT received_at_ms FROM messages
             WHERE agent_id = ?1 AND seq >= ?2 AND {not_passive}
             ORDER BY seq ASC LIMIT 1"
        );
        let received_at: Option<i64> =
            conn.db_query_opt(&sql, &db_params![agent_id.as_str(), seq], |row| row.get(0))?;
        Ok(received_at.map(|at| (now_ms - at).max(0)))
    }
}

pub(super) fn next_seq(conn: &dyn DbExec, agent_id: &AgentId) -> Result<i64, Error> {
    conn.lock_agent_for_seq(agent_id.as_str())?;
    let greatest = conn.backend().sql_greatest("0", "COALESCE(MAX(seq), 0)");
    let sql = format!("SELECT {greatest} + 1 FROM messages WHERE agent_id = ?1");
    conn.db_query_one(&sql, &db_params![agent_id.as_str()], |row| row.get(0))
}

/// The highest sequence in the agent's mailbox, or 0 when it is empty.
pub(super) fn last_seq(conn: &dyn DbExec, agent_id: &AgentId) -> Result<i64, Error> {
    let greatest = conn.backend().sql_greatest("0", "COALESCE(MAX(seq), 0)");
    let sql = format!("SELECT {greatest} FROM messages WHERE agent_id = ?1");
    conn.db_query_one(&sql, &db_params![agent_id.as_str()], |row| row.get(0))
}

/// The mailbox line at `seq`, acknowledged against the `delivered` cursor.
pub(super) fn message_at_on(
    conn: &dyn DbExec,
    agent_id: &AgentId,
    seq: i64,
    delivered: i64,
) -> Result<Option<Message>, Error> {
    conn.db_query_opt(
        concat!(
            "SELECT ",
            message_cols!(),
            " FROM messages WHERE agent_id = ?1 AND seq = ?2"
        ),
        &db_params![agent_id.as_str(), seq],
        |row| message_from_row_with_cursor_db(row, delivered),
    )
}

pub(super) fn cursor(conn: &dyn DbExec, agent_id: &AgentId) -> Result<i64, Error> {
    let found = conn.db_query_opt(
        "SELECT delivered_seq FROM mailbox_cursors WHERE agent_id = ?1",
        &db_params![agent_id.as_str()],
        |row| row.get(0),
    )?;
    found.ok_or_else(|| Error::Message(format!("mailbox cursor missing for {agent_id}")))
}

fn bump_delivered(conn: &dyn DbExec, agent_id: &AgentId, seq: i64) -> Result<(), Error> {
    let delivered = cursor(conn, agent_id)?;
    if seq != delivered + 1 || !message_exists(conn, agent_id, seq)? {
        return Err(Error::CursorGap {
            cursor: "delivered",
            at: delivered,
            attempted: seq,
        });
    }
    conn.db_execute(
        "UPDATE mailbox_cursors SET delivered_seq = ?1 WHERE agent_id = ?2",
        &db_params![seq, agent_id.as_str()],
    )?;
    Ok(())
}

fn message_exists(conn: &dyn DbExec, agent_id: &AgentId, seq: i64) -> Result<bool, Error> {
    let count: i64 = conn.db_query_one(
        "SELECT COUNT(*) FROM messages WHERE agent_id = ?1 AND seq = ?2",
        &db_params![agent_id.as_str(), seq],
        |row| row.get(0),
    )?;
    Ok(count == 1)
}
