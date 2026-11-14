-- A turn whose harness reported its end with an error is closed with that
-- outcome, and the agent's run of such turns is counted here
-- (docs/delivery.md, "A turn that ends in an error"). At the limit the
-- supervisor parks the agent (`paused`) and records when; a resume clears
-- both. No foreign key, as for `rejected_messages`: a binary from before this
-- table deletes agents without knowing it.
CREATE TABLE turn_errors (
    agent_id TEXT PRIMARY KEY,
    -- Turns in a row that ended in an error. A completed turn resets it.
    streak INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_error_at_ms INTEGER,
    -- When the agent was parked because `streak` reached the limit.
    faulted_at_ms INTEGER
);
