//! Restore generated IDs beyond the copied high-water marks.
use super::{Error, PgPool};

pub(super) async fn reset_identity_sequences(pool: &PgPool) -> Result<(), Error> {
    const SEQUENCES: &[(&str, &str)] = &[
        ("public.admin_audit", "id"),
        ("public.credential_binding_audit", "id"),
        ("public.messages", "rowid"),
        ("public.operator_messages", "seq"),
        ("public.outbound_messages", "rowid"),
        ("public.skill_reads", "id"),
        ("memory_index.rag_chunks", "rowid"),
        ("host_index.host_documents", "rowid"),
        ("host_index.host_windows", "id"),
    ];
    for &(table, col) in SEQUENCES {
        let sql = format!(
            "SELECT setval(pg_get_serial_sequence('{table}', '{col}'), \
             COALESCE((SELECT MAX(\"{col}\") FROM {table}), 1), \
             EXISTS (SELECT 1 FROM {table}))"
        );
        sqlx::query(&sql).execute(pool).await.map_err(|e| {
            Error::Message(format!(
                "failed to reset identity sequence on {table}.{col}: {e}"
            ))
        })?;
    }
    Ok(())
}
