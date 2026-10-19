-- One row per mailbox message whose turn ended in an error. The message keeps
-- the `accepted` ack and the processed cursor does not move past it. A later
-- successful turn on the same message clears the row.
CREATE TABLE failed_turns (
    message_id TEXT PRIMARY KEY REFERENCES messages (id),
    agent_id TEXT NOT NULL REFERENCES agents (id),
    seq INTEGER NOT NULL,
    reason TEXT NOT NULL,
    failed_at_ms INTEGER NOT NULL,
    UNIQUE (agent_id, seq)
);

CREATE INDEX failed_turns_agent ON failed_turns (agent_id, seq);

-- The last time each message was handed to a vendor process, and whether that
-- hand-over already found its turn finished before the host recorded it.
ALTER TABLE messages ADD COLUMN delivered_at_ms INTEGER;
ALTER TABLE messages ADD COLUMN processed_at_delivery INTEGER NOT NULL DEFAULT 0;
