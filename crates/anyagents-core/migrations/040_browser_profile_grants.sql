-- Persistent browser profile grants scoped per agent (SQLite).
CREATE TABLE IF NOT EXISTS agent_browser_profile_grants (
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    profile_name TEXT NOT NULL,
    granted_by TEXT,
    granted_at_ms BIGINT NOT NULL,
    PRIMARY KEY (agent_id, profile_name)
);

CREATE INDEX IF NOT EXISTS idx_agent_browser_profile_grants_profile
    ON agent_browser_profile_grants(profile_name);
