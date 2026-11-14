-- The operator's inbox: reports an agent sends to whoever runs this
-- deployment, through `send` to `operator`. Agents write; only the operator
-- reads.
--
-- A sibling of `messages`, not rows in it: every mailbox row belongs to an
-- agent (`agent_id` references `agents`, with foreign keys on), carries a
-- delivery cursor a harness advances, and is indexed into that agent's history.
-- The operator is a principal, not an agent row, and no harness takes this
-- mail, so there is no cursor; the reader keeps its own `since`.
-- AUTOINCREMENT keeps a pruned `seq` from being reused under that cursor.
--
-- `sender` is the authenticated author, `agent:<agent id>` or `operator`, and
-- keys the per-sender rate limit, so a rename does not reset it.
-- `sender_name` is the display name it had when it wrote. There is no foreign
-- key: a report outlives its sender, like mail an agent sent to a peer.
-- The same body from any sender within an hour of a report is that report:
-- `repeats` counts it and `last_repeated_at_ms` dates the latest one.
CREATE TABLE IF NOT EXISTS operator_messages (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    sender TEXT NOT NULL,
    sender_name TEXT,
    body TEXT NOT NULL,
    body_sha256 TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    repeats INTEGER NOT NULL DEFAULT 0,
    last_repeated_at_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_operator_messages_sender
    ON operator_messages (sender, received_at_ms);
CREATE INDEX IF NOT EXISTS idx_operator_messages_body
    ON operator_messages (body_sha256, received_at_ms);
CREATE INDEX IF NOT EXISTS idx_operator_messages_received
    ON operator_messages (received_at_ms);
