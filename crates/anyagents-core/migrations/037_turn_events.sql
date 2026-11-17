-- Only actual terminal callbacks populate this ledger. Recovery high-watermarks
-- and upgrade-time mailbox cursors are not evidence of a finished turn.
CREATE TABLE agent_turn_events (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    turn_seq INTEGER NOT NULL CHECK (turn_seq > 0),
    delivered_seq INTEGER NOT NULL CHECK (delivered_seq > 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('completed', 'errored'))
);
