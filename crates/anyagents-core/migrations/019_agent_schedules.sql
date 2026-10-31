-- Per-agent self-scheduled wakes (at/every) and event watches (github/spiron/agent:idle).
CREATE TABLE IF NOT EXISTS agent_schedules (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    spec TEXT NOT NULL,
    prompt TEXT NOT NULL,
    interval_ms INTEGER,
    next_fire_at_ms INTEGER,
    last_fired_at_ms INTEGER,
    watch_baseline TEXT,
    last_observed TEXT,
    state TEXT NOT NULL DEFAULT 'active',
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_schedules_agent_state ON agent_schedules(agent_id, state);
