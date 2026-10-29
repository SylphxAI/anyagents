-- Preserve former agent names on rename and attach a short role label to each agent.
ALTER TABLE agents ADD COLUMN role TEXT;

CREATE TABLE IF NOT EXISTS agent_aliases (
    alias TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_aliases_agent_id ON agent_aliases(agent_id);
