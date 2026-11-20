CREATE TABLE knowledge_docs (
    id TEXT PRIMARY KEY NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    body TEXT NOT NULL,
    pinned BIGINT NOT NULL CHECK (pinned IN (0, 1)),
    version BIGINT NOT NULL CHECK (version >= 1),
    updated_by TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    deleted_at_ms BIGINT,
    content_hash TEXT NOT NULL,
    UNIQUE (namespace, topic)
);

CREATE TABLE knowledge_revisions (
    id TEXT PRIMARY KEY NOT NULL,
    doc_id TEXT NOT NULL REFERENCES knowledge_docs(id),
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 1),
    actor_id TEXT NOT NULL,
    body TEXT NOT NULL,
    prior_body TEXT,
    reason TEXT NOT NULL,
    sim_problem TEXT,
    sim_expected TEXT,
    sim_observed TEXT,
    sim_context TEXT,
    created_at_ms BIGINT NOT NULL,
    deleted_at_ms BIGINT,
    UNIQUE (doc_id, version)
);

CREATE TABLE knowledge_grants (
    id TEXT PRIMARY KEY NOT NULL,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    namespace_pattern TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('read', 'write', 'propose')),
    granted_by TEXT NOT NULL,
    granted_at_ms BIGINT NOT NULL,
    UNIQUE (agent_id, mode, namespace_pattern)
);

CREATE TABLE knowledge_proposals (
    id TEXT PRIMARY KEY NOT NULL,
    namespace TEXT NOT NULL,
    topic TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    expected_version BIGINT NOT NULL CHECK (expected_version >= 0),
    edit_json TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'promoted', 'rejected')),
    reviewed_by TEXT,
    reviewed_at_ms BIGINT
);
CREATE INDEX idx_knowledge_proposals_scope ON knowledge_proposals(namespace, status, id);

CREATE TRIGGER knowledge_revisions_no_update BEFORE UPDATE ON knowledge_revisions
BEGIN SELECT RAISE(ABORT, 'knowledge revisions are append-only'); END;
CREATE TRIGGER knowledge_revisions_no_delete BEFORE DELETE ON knowledge_revisions
BEGIN SELECT RAISE(ABORT, 'knowledge revisions are append-only'); END;

-- A separate RAG partition: old mailbox readers must not see protected chunks
-- during rolling upgrades. State and its index commit together.
CREATE TABLE knowledge_rag_chunks (
    chunk_id TEXT PRIMARY KEY NOT NULL REFERENCES knowledge_docs(id),
    content_hash TEXT NOT NULL
);
CREATE VIRTUAL TABLE knowledge_rag_chunks_fts USING fts5(
    chunk_id UNINDEXED, search_tokens, tokenize='unicode61'
);
