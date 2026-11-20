//! Durable knowledge table specifications and the optional search rebuild.

use rusqlite::Row;

use super::{ColBuf, ColSpec, SqliteProjection, TableSpec, sqlite_cell_to_string};
use crate::error::Error;
use crate::store::knowledge::projection_fields;

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
    sqlite_table: "knowledge_docs",
    sqlite_from: "knowledge_docs WHERE deleted_at_ms IS NULL",
    pg_table: "knowledge_index.rag_chunks",
    cols: &[
        ColSpec::txt("chunk_id"),
        ColSpec::txt("content_hash"),
        ColSpec::txt("search_tsv"),
    ],
    extra_pg_cols: None,
    pg_select: Some("t.chunk_id, t.content_hash, to_tsvector('simple', t.search_tsv)"),
    sqlite_projection: Some(SqliteProjection {
        select: "id, topic, title, description, body",
        push: push_projection,
    }),
};

fn push_projection(row: &Row<'_>, bufs: &mut [ColBuf]) -> Result<(), Error> {
    let [
        ColBuf::Text(ids),
        ColBuf::Text(hashes),
        ColBuf::Text(tokens),
    ] = bufs
    else {
        unreachable!("knowledge projection requires three text buffers");
    };
    // Use the same text sanitization as the durable document copy.
    let text =
        |idx: usize| Ok::<_, Error>(sqlite_cell_to_string(row.get_ref(idx)?)?.unwrap_or_default());
    let (id, topic, title, description, body) = (text(0)?, text(1)?, text(2)?, text(3)?, text(4)?);
    let (hash, search_tokens) = projection_fields(&topic, &title, &description, &body);
    ids.push(id);
    hashes.push(hash);
    tokens.push(search_tokens);
    Ok(())
}
