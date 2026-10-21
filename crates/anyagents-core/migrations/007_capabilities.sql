-- A mailbox capability per agent instance. The token stays in the agent's own
-- config directory; this table holds only its hash, the agent's immutable id
-- and the session generation it was minted for. A new instance replaces the
-- last one, and every older token stops authenticating at once.
CREATE TABLE agent_capabilities (
    token_hash TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents (id),
    generation INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER
);

CREATE INDEX agent_capabilities_by_agent ON agent_capabilities (agent_id, generation);
