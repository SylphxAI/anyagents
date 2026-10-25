CREATE INDEX IF NOT EXISTS idx_messages_agent_time
    ON messages(agent_id, received_at_ms, seq);

CREATE TABLE IF NOT EXISTS rag_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO rag_meta (key, value) VALUES ('embedding_model', 'voyageai/voyage-4-large');
INSERT OR IGNORE INTO rag_meta (key, value) VALUES ('embedding_dimensions', '1024');

CREATE TABLE IF NOT EXISTS rag_chunks (
    chunk_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    direction TEXT NOT NULL,
    sender TEXT NOT NULL,
    destination TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    text TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_time
    ON rag_chunks(agent_name, timestamp_ms DESC);

CREATE INDEX IF NOT EXISTS idx_rag_chunks_time
    ON rag_chunks(timestamp_ms DESC);

CREATE INDEX IF NOT EXISTS idx_rag_chunks_hash
    ON rag_chunks(content_hash);

CREATE VIRTUAL TABLE IF NOT EXISTS rag_chunks_fts USING fts5(
    chunk_id UNINDEXED,
    agent_name,
    sender,
    destination,
    text,
    tokenize='trigram'
);

CREATE TABLE IF NOT EXISTS rag_embeddings (
    content_hash TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    vector_i8 BLOB NOT NULL,
    created_at_ms INTEGER NOT NULL
);
