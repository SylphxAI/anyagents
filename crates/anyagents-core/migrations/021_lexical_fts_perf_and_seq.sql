-- Backfill monotonic `seq` on outbound and legacy chunks where `seq = 0`.
UPDATE rag_chunks
SET seq = rowid
WHERE seq = 0;

-- Composite indexes for 100x-scale agent-scoped and kind-scoped structured history queries.
CREATE INDEX IF NOT EXISTS idx_rag_chunks_time_seq
    ON rag_chunks (timestamp_ms DESC, seq DESC);

CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_kind_time
    ON rag_chunks (LOWER(agent_name), kind, timestamp_ms DESC);
