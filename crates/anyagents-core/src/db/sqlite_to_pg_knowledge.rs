//! Knowledge table specifications in dependency order.
use super::{ColSpec, TableSpec};

// Run after agents (grants) and before the optional projection (documents).
pub(super) const KNOWLEDGE_TABLES: &[TableSpec] = &[
    TableSpec::simple(
        "knowledge_docs",
        "public.knowledge_docs",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("namespace"),
            ColSpec::txt("topic"),
            ColSpec::txt("title"),
            ColSpec::txt("description"),
            ColSpec::txt("body"),
            ColSpec::int("pinned"),
            ColSpec::int("version"),
            ColSpec::txt("updated_by"),
            ColSpec::int("created_at_ms"),
            ColSpec::int("updated_at_ms"),
            ColSpec::opt_int("deleted_at_ms"),
            ColSpec::txt("content_hash"),
        ],
    ),
    // Keep INSERT ... ON CONFLICT DO NOTHING; even a no-op UPDATE fires the
    // PostgreSQL statement-level append-only trigger.
    TableSpec::simple(
        "knowledge_revisions",
        "public.knowledge_revisions",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("doc_id"),
            ColSpec::txt("namespace"),
            ColSpec::txt("topic"),
            ColSpec::int("version"),
            ColSpec::txt("actor_id"),
            ColSpec::txt("body"),
            ColSpec::opt_txt("prior_body"),
            ColSpec::txt("reason"),
            ColSpec::opt_txt("sim_problem"),
            ColSpec::opt_txt("sim_expected"),
            ColSpec::opt_txt("sim_observed"),
            ColSpec::opt_txt("sim_context"),
            ColSpec::int("created_at_ms"),
            ColSpec::opt_int("deleted_at_ms"),
        ],
    ),
    TableSpec::simple(
        "knowledge_grants",
        "public.knowledge_grants",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("namespace_pattern"),
            ColSpec::txt("mode"),
            ColSpec::txt("granted_by"),
            ColSpec::int("granted_at_ms"),
        ],
    ),
    TableSpec::simple(
        "knowledge_proposals",
        "public.knowledge_proposals",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("namespace"),
            ColSpec::txt("topic"),
            ColSpec::txt("actor_id"),
            ColSpec::int("expected_version"),
            ColSpec::txt("edit_json"),
            ColSpec::int("created_at_ms"),
            ColSpec::txt("status"),
            ColSpec::opt_txt("reviewed_by"),
            ColSpec::opt_int("reviewed_at_ms"),
        ],
    ),
];

pub(super) const KNOWLEDGE_INDEX: TableSpec = TableSpec {
    sqlite_table: "knowledge_rag_chunks",
    sqlite_from: "knowledge_rag_chunks r JOIN knowledge_rag_chunks_fts f ON f.chunk_id = r.chunk_id",
    pg_table: "knowledge_index.rag_chunks",
    cols: &[
        ColSpec::expr_txt("r.chunk_id", "chunk_id"),
        ColSpec::expr_txt("r.content_hash", "content_hash"),
        ColSpec::expr_txt("f.search_tokens", "search_tsv"),
    ],
    extra_pg_cols: None,
    // SQLite stores the same hybrid/CJK tokens the PostgreSQL writer uses.
    pg_select: Some("t.chunk_id, t.content_hash, to_tsvector('simple', t.search_tsv)"),
};
