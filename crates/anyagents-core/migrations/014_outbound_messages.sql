CREATE TABLE IF NOT EXISTS outbound_messages (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents(id),
    destination TEXT NOT NULL,
    reply_to TEXT,
    body TEXT NOT NULL,
    sent_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbound_messages_agent_time
    ON outbound_messages(agent_id, sent_at_ms, id);

INSERT OR IGNORE INTO outbound_messages (id, agent_id, destination, reply_to, body, sent_at_ms)
SELECT
    'out_' || MIN(m.id),
    a.id,
    COALESCE(m.reply_to, 'agent:' || r.name),
    m.reply_to,
    m.body,
    MIN(m.received_at_ms)
FROM messages m
JOIN agents a ON m.sender = 'agent:' || a.name
JOIN agents r ON r.id = m.agent_id
GROUP BY a.id, m.body, COALESCE(m.reply_to, 'agent:' || r.name), (m.received_at_ms / 2000);
