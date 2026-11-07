-- Deliberately no original text or digest: even a hash can disclose a short secret.
CREATE TABLE conversation_redactions (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    -- Visible provider coordinates are audit metadata only. Origin lookup
    -- requires the namespaced aliases below, never these unqualified numbers.
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
    namespace TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (agent_id, identity_id, namespace, chat_id, message_id)
);
CREATE INDEX message_origin_aliases_lookup
    ON message_origin_aliases(agent_id, namespace, chat_id, message_id);

CREATE TABLE message_source_aliases (
    agent_id TEXT NOT NULL,
    identity_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    source_key TEXT NOT NULL,
    PRIMARY KEY (agent_id, identity_id, source_key)
);
CREATE INDEX message_source_aliases_lookup
    ON message_source_aliases(agent_id, source_key);

-- The pre-migration bootstrap table may not exist on an empty database.
CREATE TABLE IF NOT EXISTS message_versions (
    agent_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    body TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (agent_id, chat_id, message_id)
);
-- NULL means the legacy row's account cannot be proven: preserve/report it,
-- rather than erasing a potentially independent bot's baseline by numeric id.
ALTER TABLE message_versions ADD COLUMN origin_namespace TEXT;
