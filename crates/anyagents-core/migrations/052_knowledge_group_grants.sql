-- Grants belong to a group, not to a materialized list of its members.
CREATE TABLE knowledge_group_grants (
    id TEXT PRIMARY KEY NOT NULL,
    group_name TEXT NOT NULL REFERENCES groups(name) ON DELETE CASCADE,
    namespace_pattern TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('read', 'write', 'propose')),
    granted_by TEXT NOT NULL,
    granted_at_ms BIGINT NOT NULL,
    UNIQUE (group_name, mode, namespace_pattern)
);

CREATE INDEX idx_group_agents_by_agent ON group_agents(agent_id, group_name);
