-- Deliberately no original text or digest: even a hash can disclose a short secret.
CREATE TABLE conversation_redactions (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    actor TEXT NOT NULL,
    permission TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    span_unit TEXT NOT NULL,
    span_start INTEGER NOT NULL,
    span_end INTEGER NOT NULL,
    removed_bytes INTEGER NOT NULL,
    removed_chars INTEGER NOT NULL,
    receipt TEXT NOT NULL,
    UNIQUE(agent_id, target_id, request_key)
);
