-- Zero-Embedding Lossless Lexical + Structured Agentic History Search (`Raw-RAG`).
-- Drops dense vector embedding tables (`rag_embeddings`, `rag_meta`) and upgrades
-- `rag_chunks` + `rag_chunks_fts` with structured tool/message filtering, sequence
-- range coordinates, and hybrid ASCII + CJK bigram FTS5 indexing.

DROP TABLE IF EXISTS rag_embeddings;
DROP TABLE IF EXISTS rag_meta;

ALTER TABLE rag_chunks ADD COLUMN kind TEXT NOT NULL DEFAULT 'message';
ALTER TABLE rag_chunks ADD COLUMN tool_name TEXT NOT NULL DEFAULT '';
ALTER TABLE rag_chunks ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_rag_chunks_agent_seq
    ON rag_chunks(agent_name, seq, timestamp_ms);

CREATE INDEX IF NOT EXISTS idx_rag_chunks_kind_tool
    ON rag_chunks(kind, tool_name, timestamp_ms DESC);

-- Backfill per-agent mailbox sequence on existing inbound chunks.
UPDATE rag_chunks
SET seq = COALESCE((SELECT m.seq FROM messages m WHERE m.id = rag_chunks.chunk_id), 0)
WHERE direction = 'in' AND seq = 0;

-- Classify existing tool/system notification chunks.
UPDATE rag_chunks
SET kind = 'tool',
    tool_name = CASE
        WHEN text LIKE '[Sub-Agent Completed]%' THEN 'subagents'
        WHEN text LIKE '[Schedule%' OR text LIKE '[Watch%' THEN 'schedule'
        WHEN sender LIKE 'tool:%' THEN SUBSTR(sender, 6)
        ELSE 'system'
    END
WHERE kind = 'message'
  AND (
      text LIKE '[Sub-Agent Completed]%'
      OR text LIKE '[Schedule%'
      OR text LIKE '[Watch%'
      OR text LIKE '[tool_call:%'
      OR text LIKE '[tool_output:%'
      OR text LIKE '[Tool:%'
      OR sender LIKE 'tool:%'
  );

-- Backfill any inbound messages not yet present in rag_chunks.
INSERT OR IGNORE INTO rag_chunks (
    chunk_id, agent_id, agent_name, direction, sender, destination,
    timestamp_ms, content_hash, text, kind, tool_name, seq
)
SELECT
    m.id,
    m.agent_id,
    a.name,
    'in',
    COALESCE(m.sender, 'unknown'),
    COALESCE(m.reply_to, 'agent:' || a.name),
    m.received_at_ms,
    m.id,
    SUBSTR(TRIM(m.body), 1, 2400),
    CASE
        WHEN m.body LIKE '[Sub-Agent Completed]%' THEN 'tool'
        WHEN m.body LIKE '[Schedule%' OR m.body LIKE '[Watch%' THEN 'tool'
        WHEN m.body LIKE '[tool_call:%' OR m.body LIKE '[tool_output:%' OR m.body LIKE '[Tool:%' THEN 'tool'
        ELSE 'message'
    END,
    CASE
        WHEN m.body LIKE '[Sub-Agent Completed]%' THEN 'subagents'
        WHEN m.body LIKE '[Schedule%' OR m.body LIKE '[Watch%' THEN 'schedule'
        ELSE ''
    END,
    m.seq
FROM messages m
JOIN agents a ON a.id = m.agent_id
WHERE LENGTH(TRIM(m.body)) >= 2;

-- Backfill any outbound messages not yet present in rag_chunks.
INSERT OR IGNORE INTO rag_chunks (
    chunk_id, agent_id, agent_name, direction, sender, destination,
    timestamp_ms, content_hash, text, kind, tool_name, seq
)
SELECT
    om.id,
    om.agent_id,
    a.name,
    'out',
    'agent:' || a.name,
    om.destination,
    om.sent_at_ms,
    om.id,
    SUBSTR(TRIM(om.body), 1, 2400),
    'message',
    '',
    0
FROM outbound_messages om
JOIN agents a ON a.id = om.agent_id
WHERE LENGTH(TRIM(om.body)) >= 2;

DROP TABLE IF EXISTS rag_chunks_fts;

CREATE VIRTUAL TABLE rag_chunks_fts USING fts5(
    chunk_id,
    agent_name,
    sender,
    destination,
    kind,
    tool_name,
    text,
    search_tokens,
    tokenize='unicode61'
);
