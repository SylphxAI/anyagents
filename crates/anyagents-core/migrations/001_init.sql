CREATE TABLE agents (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    harness TEXT NOT NULL,
    model TEXT,
    workdir TEXT,
    instructions TEXT,
    state TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE channels (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    identity TEXT,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE channel_agents (
    channel_id TEXT NOT NULL REFERENCES channels (id),
    agent_id TEXT NOT NULL REFERENCES agents (id),
    PRIMARY KEY (channel_id, agent_id)
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents (id),
    seq INTEGER NOT NULL,
    source_time_ms INTEGER,
    source_key TEXT,
    sender TEXT,
    body TEXT NOT NULL,
    reply_to TEXT,
    received_at_ms INTEGER NOT NULL,
    UNIQUE (agent_id, seq)
);

-- NULL source keys are not keys: several messages may omit one.
CREATE UNIQUE INDEX messages_source_key ON messages (agent_id, source_key)
    WHERE source_key IS NOT NULL;

CREATE TABLE mailbox_cursors (
    agent_id TEXT PRIMARY KEY REFERENCES agents (id),
    accepted_seq INTEGER NOT NULL,
    processed_seq INTEGER NOT NULL
);

CREATE TABLE usage_records (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents (id),
    harness TEXT NOT NULL,
    model TEXT,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cache_read_tokens INTEGER NOT NULL,
    cache_write_tokens INTEGER NOT NULL,
    reported_cost_micros INTEGER,
    estimated_cost_micros INTEGER,
    billing_mode TEXT NOT NULL,
    quota_state TEXT,
    day TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL
);

CREATE INDEX usage_records_agent ON usage_records (agent_id, recorded_at_ms);
