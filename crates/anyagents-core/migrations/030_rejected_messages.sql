-- A mailbox message the harness refused for good (docs/delivery.md,
-- `Rejected`): parked past the delivery cursor with the vendor's reason, and
-- its sender told. No foreign keys: a binary from before this table deletes
-- agents and messages without knowing it, so nothing here may block that.
CREATE TABLE rejected_messages (
    message_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    reason TEXT NOT NULL,
    rejected_at_ms INTEGER NOT NULL,
    -- When its sender was told; NULL until then, and sent again at the next
    -- delivery loop's start.
    notified_at_ms INTEGER
);

CREATE INDEX rejected_messages_agent ON rejected_messages (agent_id, seq);
