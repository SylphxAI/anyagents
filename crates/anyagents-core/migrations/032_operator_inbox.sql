-- The operator's inbox: reports an agent sends to whoever runs this
-- deployment, through `send` to `operator`. Agents write; only the operator
-- reads.
--
-- A sibling of `messages`, not rows in it: every mailbox row belongs to an
-- agent (`agent_id` references `agents`, with foreign keys on), carries a
-- delivery cursor a harness advances, and is indexed into that agent's history.
-- The operator is a principal, not an agent row, and no harness takes this
-- mail, so there is no cursor; the reader keeps its own `since`.
--
-- `sender` is the authenticated author, `agent:<agent id>` or `operator`, and
-- keys the per-sender rate limit and deduplication, so a rename does not reset
-- them. `sender_name` is the display name it had when it wrote. There is no
-- foreign key: a report outlives its sender, like mail an agent sent to a peer.
CREATE TABLE IF NOT EXISTS operator_messages (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    sender TEXT NOT NULL,
    sender_name TEXT,
    body TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operator_messages_sender
    ON operator_messages (sender, received_at_ms);
