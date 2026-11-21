-- Sender and chat blocks are namespaced by receiving agent and channel/account.
CREATE TABLE IF NOT EXISTS ingress_blocks (
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('sender', 'chat')),
    target_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    reason TEXT,
    blocked_by TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER,
    PRIMARY KEY (agent_id, channel, scope, target_id)
);
