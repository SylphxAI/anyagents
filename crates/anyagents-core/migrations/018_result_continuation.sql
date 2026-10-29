CREATE TABLE IF NOT EXISTS result_subscriptions (
    id TEXT PRIMARY KEY,
    recipient_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    subject TEXT,
    org_id TEXT,
    event_types TEXT NOT NULL DEFAULT '[]',
    secret TEXT NOT NULL,
    previous_secret TEXT,
    created_by TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    after_seq INTEGER NOT NULL DEFAULT 0,
    last_seq INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_result_subscriptions_recipient
    ON result_subscriptions (recipient_id, enabled);

CREATE INDEX IF NOT EXISTS idx_result_subscriptions_source_subject
    ON result_subscriptions (source, subject, enabled);

CREATE TABLE IF NOT EXISTS result_deliveries (
    id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL REFERENCES result_subscriptions(id) ON DELETE CASCADE,
    recipient_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    webhook_id TEXT NOT NULL,
    webhook_timestamp INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    subject TEXT,
    org_id TEXT,
    seq INTEGER,
    message_id TEXT NOT NULL,
    message_seq INTEGER NOT NULL,
    woke_from_state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    UNIQUE (subscription_id, webhook_id),
    UNIQUE (subscription_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_result_deliveries_recipient
    ON result_deliveries (recipient_id, received_at_ms DESC);
