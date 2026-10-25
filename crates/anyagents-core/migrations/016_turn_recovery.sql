-- Durable open-turn state per agent.
--
-- `opened_seq` moves when a message is accepted by the harness (`note_delivered`).
-- `completed_seq` catches up to `opened_seq` when the turn finishes (`note_turn_completed`).
-- When a harness process restarts while `opened_seq > completed_seq` and no newer
-- undelivered message is waiting in the mailbox, the supervisor injects a bounded
-- `[Recovery]` prompt (`recovery_count < MAX_RECOVERY_ATTEMPTS`) so the resumed
-- CLI session immediately continues its interrupted turn.
CREATE TABLE IF NOT EXISTS turn_recovery (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    opened_seq INTEGER NOT NULL DEFAULT 0,
    completed_seq INTEGER NOT NULL DEFAULT 0,
    recovery_count INTEGER NOT NULL DEFAULT 0,
    opened_at_ms INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO turn_recovery (agent_id, opened_seq, completed_seq, recovery_count, opened_at_ms)
SELECT agent_id, delivered_seq, delivered_seq, 0, 0 FROM mailbox_cursors;
