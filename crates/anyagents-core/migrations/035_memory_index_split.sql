-- The mailbox search index leaves the state database for its own file,
-- memory-index.db (store/memory_index.rs), rebuilt there from `messages` and
-- `outbound_messages`. Before this, `rag_chunks` and `rag_chunks_fts` held
-- every mailbox body two more times, and maintaining them held the state
-- writer.
--
-- An outbound turn's mailbox position was kept only in its chunk; it moves to
-- the outbound row it describes. The standalone tool summaries only these
-- tables held are copied to the index before this runs.
ALTER TABLE outbound_messages ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;
UPDATE outbound_messages
SET seq = (SELECT c.seq FROM rag_chunks c WHERE c.chunk_id = outbound_messages.id)
WHERE EXISTS (SELECT 1 FROM rag_chunks c WHERE c.chunk_id = outbound_messages.id AND c.seq != 0);
DROP TABLE IF EXISTS rag_chunks_fts;
DROP TABLE IF EXISTS rag_chunks;
