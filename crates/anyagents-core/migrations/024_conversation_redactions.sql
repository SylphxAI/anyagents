-- Deliberately no original text or digest: even a hash can disclose a short secret.
CREATE TABLE conversation_redactions (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    -- Provider identity is independent of body/source keys and bounded history
    -- scans, so a later retry or edit cannot resurrect this captured content.
    origin_chat_id INTEGER,
    origin_message_id INTEGER,
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

CREATE INDEX conversation_redactions_origin
    ON conversation_redactions(agent_id, origin_chat_id, origin_message_id);

-- A canonical mailbox row can coalesce several provider/local source ids.
-- Preserve them before promotion changes messages.origin/source_key. These
-- tables contain routing identities only, never message content or its hash.
-- A late quote can intentionally receive a second mailbox seq. Both rows
-- still represent the same provider identity and share the complete alias set.
CREATE TABLE message_identities (
    agent_id TEXT NOT NULL,
    target_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    identity_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    PRIMARY KEY (agent_id, target_id)
);
CREATE INDEX message_identities_lookup ON message_identities(agent_id, identity_id);

CREATE TABLE message_origin_aliases (
    agent_id TEXT NOT NULL,
    identity_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (agent_id, identity_id, chat_id, message_id)
);
CREATE INDEX message_origin_aliases_lookup
    ON message_origin_aliases(agent_id, chat_id, message_id);

CREATE TABLE message_source_aliases (
    agent_id TEXT NOT NULL,
    identity_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    source_key TEXT NOT NULL,
    PRIMARY KEY (agent_id, identity_id, source_key)
);
CREATE INDEX message_source_aliases_lookup
    ON message_source_aliases(agent_id, source_key);
