-- Rebuildable index policy marker; raw mailbox bodies remain authoritative.
-- Runtime backfill performs the state changes, never the schema migration.
ALTER TABLE rag_chunks ADD COLUMN text_format INTEGER NOT NULL DEFAULT 1;
CREATE INDEX idx_rag_chunks_format ON rag_chunks(text_format, agent_id);
