-- The mailbox is one log with one delivery cursor: the last sequence the
-- harness's own input or steer API took. The old pair collapses to
-- delivered = the higher of the two, so nothing a harness already took is
-- handed over again after the upgrade.
--
-- SQLite renames the columns an index depends on when the table is renamed, so
-- the one index over this table is dropped and made again around the swap.
DROP INDEX IF EXISTS messages_source_key;

CREATE TABLE mailbox_cursors_one (
    agent_id TEXT PRIMARY KEY REFERENCES agents (id),
    delivered_seq INTEGER NOT NULL
);

INSERT INTO mailbox_cursors_one (agent_id, delivered_seq)
    SELECT agent_id, MAX(accepted_seq, processed_seq) FROM mailbox_cursors;

DROP TABLE mailbox_cursors;

ALTER TABLE mailbox_cursors_one RENAME TO mailbox_cursors;

CREATE INDEX messages_source_key ON messages (agent_id, source_key)
    WHERE source_key IS NOT NULL;

-- Message rows carry no delivery or turn state of their own: the one cursor
-- is the whole record.
ALTER TABLE messages DROP COLUMN processed_at_delivery;

-- The agent's own replies were written back into its inbox with an
-- `answer:<id>` source key and handed to the harness again. An agent now
-- replies only through its MCP `send`, so those rows are stale: they are
-- removed, and every cursor that pointed past one moves back to the last row
-- that remains.
DELETE FROM messages WHERE source_key LIKE 'answer:%';

UPDATE mailbox_cursors SET delivered_seq = COALESCE(
    (SELECT MAX(seq) FROM messages WHERE agent_id = mailbox_cursors.agent_id), 0)
WHERE delivered_seq > COALESCE(
    (SELECT MAX(seq) FROM messages WHERE agent_id = mailbox_cursors.agent_id), 0);

-- `failed_turns` recorded a vendor turn that ended in an error, which the
-- switchboard no longer observes.
DROP TABLE failed_turns;
