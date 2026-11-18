-- An opener is one durable mailbox message, not the cursor (steers can advance it).
CREATE TABLE task_turn_contexts (
    child_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    opener_id TEXT NOT NULL,
    opener_seq INTEGER NOT NULL,
    token TEXT NOT NULL UNIQUE,
    synthetic_recovery INTEGER NOT NULL DEFAULT 0,
    sent_to_parent_id TEXT REFERENCES agents(id) ON DELETE SET NULL,
    closed_delivery_seq INTEGER,
    PRIMARY KEY (child_id, opener_id)
) WITHOUT ROWID;
