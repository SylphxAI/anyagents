-- The mailbox search index leaves the state database for its own file,
-- memory-index.db (store/memory_index.rs), rebuilt there from `messages` and
-- `outbound_messages`. Before this, `rag_chunks` and `rag_chunks_fts` held
-- every mailbox body two more times, and maintaining them held the state
-- writer.
--
-- This runs before the server answers, so it changes only the schema: no
-- statement here reads or frees the old tables' pages, which on a large
-- store would take longer than any readiness window. The old tables are
-- renamed out of the way (search SQL then resolves `rag_chunks` to the
-- attached index) and dismantled after startup in short transactions
-- (store/retired_search.rs). Until then they still hold each outbound turn's
-- mailbox position, which moves to the outbound row it describes; the
-- standalone summaries only these tables held were copied to the index
-- before this runs.
ALTER TABLE outbound_messages ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE rag_chunks RENAME TO retired_rag_chunks;
ALTER TABLE rag_chunks_fts RENAME TO retired_rag_chunks_fts;
