//! Batched SQLite -> PostgreSQL migration and stale-runtime reconciliation.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Instant;

use rusqlite::{Connection, OpenFlags, types::ValueRef};
use serde::Serialize;
use sqlx::PgPool;

use super::{
    block_on_pg, build_pg_pool, configured_direct_database_url, is_postgres_url, migrate_postgres,
};
use crate::error::Error;
use crate::migrate::now_ms;

/// Options controlling `migrate_sqlite_dir_to_postgres`.
#[derive(Clone, Debug)]
pub struct MigrateToPostgresOptions {
    pub source_dir: PathBuf,
    pub target_url: String,
    pub skip_index: bool,
    pub batch_size: usize,
    pub reconcile_stale_runtime: bool,
    /// When true, tables whose PostgreSQL row count already equals or exceeds
    /// the SQLite source row count are verified and skipped without re-streaming.
    pub resume: bool,
    /// Optional AEAD master key for encrypting migrated secret files, gateway
    /// routes, and sandbox transport tokens. Defaults to `MasterKey::from_env()`.
    pub master_key: Option<crate::stateless::MasterKey>,
}

impl MigrateToPostgresOptions {
    pub fn new(source_dir: impl AsRef<Path>, target_url: impl Into<String>) -> Self {
        Self {
            source_dir: source_dir.as_ref().to_path_buf(),
            target_url: target_url.into(),
            skip_index: false,
            batch_size: 2500,
            reconcile_stale_runtime: true,
            resume: false,
            master_key: crate::stateless::MasterKey::from_env().ok().flatten(),
        }
    }
}

/// Per-table migration verification metrics.
#[derive(Clone, Debug, Serialize)]
pub struct TableMigrationStat {
    pub pg_table: String,
    pub sqlite_rows: i64,
    pub pg_rows_before: i64,
    pub pg_rows_after: i64,
}

/// Summary report returned after SQLite -> PostgreSQL migration completes.
#[derive(Clone, Debug, Serialize)]
pub struct MigrateToPostgresReport {
    pub tables: Vec<TableMigrationStat>,
    pub agent_files_migrated: u64,
    pub deployment_blobs_migrated: u64,
    pub gateway_routes_migrated: u64,
    pub sandbox_bindings_migrated: u64,
    pub skipped_secret_files: u64,
    pub reconciled_archived_runtime: u64,
    pub reconciled_legacy_faults: u64,
    pub reconciled_stale_turns: u64,
    pub elapsed_ms: u64,
}

#[derive(Clone, Copy)]
enum ColKind {
    Text { default: &'static str },
    OptText,
    Int { default: i64 },
    OptInt,
}

#[derive(Clone, Copy)]
struct ColSpec {
    sqlite_expr: &'static str,
    pg_col: &'static str,
    kind: ColKind,
}

impl ColSpec {
    const fn txt(col: &'static str) -> Self {
        Self {
            sqlite_expr: col,
            pg_col: col,
            kind: ColKind::Text { default: "" },
        }
    }
    const fn txt_def(col: &'static str, default: &'static str) -> Self {
        Self {
            sqlite_expr: col,
            pg_col: col,
            kind: ColKind::Text { default },
        }
    }
    const fn opt_txt(col: &'static str) -> Self {
        Self {
            sqlite_expr: col,
            pg_col: col,
            kind: ColKind::OptText,
        }
    }
    const fn int(col: &'static str) -> Self {
        Self {
            sqlite_expr: col,
            pg_col: col,
            kind: ColKind::Int { default: 0 },
        }
    }
    const fn opt_int(col: &'static str) -> Self {
        Self {
            sqlite_expr: col,
            pg_col: col,
            kind: ColKind::OptInt,
        }
    }
    const fn expr_txt(sqlite_expr: &'static str, pg_col: &'static str) -> Self {
        Self {
            sqlite_expr,
            pg_col,
            kind: ColKind::Text { default: "" },
        }
    }
}

struct TableSpec {
    sqlite_table: &'static str,
    sqlite_from: &'static str,
    pg_table: &'static str,
    cols: &'static [ColSpec],
    extra_pg_cols: Option<&'static str>,
    extra_pg_expr: Option<&'static str>,
}

impl TableSpec {
    const fn simple(
        sqlite_table: &'static str,
        pg_table: &'static str,
        cols: &'static [ColSpec],
    ) -> Self {
        Self {
            sqlite_table,
            sqlite_from: sqlite_table,
            pg_table,
            cols,
            extra_pg_cols: None,
            extra_pg_expr: None,
        }
    }
}

enum ColBuf {
    Text(Vec<String>),
    OptText(Vec<Option<String>>),
    Int(Vec<i64>),
    OptInt(Vec<Option<i64>>),
}

impl ColBuf {
    fn from_kind(kind: ColKind, cap: usize) -> Self {
        match kind {
            ColKind::Text { .. } => Self::Text(Vec::with_capacity(cap)),
            ColKind::OptText => Self::OptText(Vec::with_capacity(cap)),
            ColKind::Int { .. } => Self::Int(Vec::with_capacity(cap)),
            ColKind::OptInt => Self::OptInt(Vec::with_capacity(cap)),
        }
    }

    fn pg_array_type(&self) -> &'static str {
        match self {
            Self::Text(_) | Self::OptText(_) => "text[]",
            Self::Int(_) | Self::OptInt(_) => "bigint[]",
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Text(v) => v.len(),
            Self::OptText(v) => v.len(),
            Self::Int(v) => v.len(),
            Self::OptInt(v) => v.len(),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Text(v) => v.clear(),
            Self::OptText(v) => v.clear(),
            Self::Int(v) => v.clear(),
            Self::OptInt(v) => v.clear(),
        }
    }

    fn push_cell(&mut self, cell: ValueRef<'_>, kind: ColKind) -> Result<(), Error> {
        match (self, kind) {
            (Self::Text(v), ColKind::Text { default }) => {
                v.push(sqlite_cell_to_string(cell)?.unwrap_or_else(|| default.to_owned()));
            }
            (Self::OptText(v), ColKind::OptText) => {
                v.push(sqlite_cell_to_string(cell)?);
            }
            (Self::Int(v), ColKind::Int { default }) => {
                v.push(sqlite_cell_to_i64(cell)?.unwrap_or(default));
            }
            (Self::OptInt(v), ColKind::OptInt) => {
                v.push(sqlite_cell_to_i64(cell)?);
            }
            _ => unreachable!("ColBuf must match ColKind"),
        }
        Ok(())
    }
}

fn strip_nul(s: String) -> String {
    if s.as_bytes().contains(&0) {
        s.replace('\0', "")
    } else {
        s
    }
}

fn sqlite_cell_to_string(cell: ValueRef<'_>) -> Result<Option<String>, Error> {
    match cell {
        ValueRef::Null => Ok(None),
        ValueRef::Text(b) => Ok(Some(strip_nul(String::from_utf8_lossy(b).into_owned()))),
        ValueRef::Integer(i) => Ok(Some(i.to_string())),
        ValueRef::Real(f) => Ok(Some(f.to_string())),
        ValueRef::Blob(b) => Ok(Some(strip_nul(String::from_utf8_lossy(b).into_owned()))),
    }
}

fn sqlite_cell_to_i64(cell: ValueRef<'_>) -> Result<Option<i64>, Error> {
    match cell {
        ValueRef::Null => Ok(None),
        ValueRef::Integer(i) => Ok(Some(i)),
        ValueRef::Real(f) => Ok(Some(f as i64)),
        ValueRef::Text(b) => {
            let s = String::from_utf8_lossy(b);
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                trimmed
                    .parse::<i64>()
                    .map(Some)
                    .map_err(|e| Error::Message(format!("invalid integer `{trimmed}`: {e}")))
            }
        }
        ValueRef::Blob(b) => Err(Error::Message(format!(
            "unexpected SQLite blob ({} bytes) in BIGINT column",
            b.len()
        ))),
    }
}

fn open_sqlite_readonly(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    Ok(conn)
}

fn sqlite_table_exists(conn: &Connection, table: &str) -> Result<bool, Error> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        [table],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn sqlite_count(conn: &Connection, table: &str) -> Result<i64, Error> {
    if !sqlite_table_exists(conn, table)? {
        return Ok(0);
    }
    let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |r| {
        r.get(0)
    })?;
    Ok(count)
}

async fn pg_count(pool: &PgPool, pg_table: &str) -> Result<i64, Error> {
    let sql = format!("SELECT COUNT(*) FROM {pg_table}");
    let (count,): (i64,) = sqlx::query_as(&sql)
        .fetch_one(pool)
        .await
        .map_err(|e| Error::Message(format!("postgres count failed on {pg_table}: {e}")))?;
    Ok(count)
}

fn build_unnest_insert_sql(spec: &TableSpec, bufs: &[ColBuf]) -> String {
    let col_list = spec
        .cols
        .iter()
        .map(|c| format!("\"{}\"", c.pg_col))
        .collect::<Vec<_>>()
        .join(", ");
    let pg_cols = match spec.extra_pg_cols {
        Some(extra) => format!("{col_list}, {extra}"),
        None => col_list.clone(),
    };
    let select_expr = match spec.extra_pg_expr {
        Some(expr) => format!("t.*, {expr}"),
        None => "t.*".to_owned(),
    };
    let unnest_args = bufs
        .iter()
        .enumerate()
        .map(|(i, b)| format!("${}::{}", i + 1, b.pg_array_type()))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({pg_cols}) SELECT {select_expr} FROM UNNEST({unnest_args}) AS t({col_list}) ON CONFLICT DO NOTHING",
        spec.pg_table
    )
}

async fn flush_batch(pool: &PgPool, insert_sql: &str, bufs: &mut [ColBuf]) -> Result<(), Error> {
    if bufs.first().is_none_or(|b| b.len() == 0) {
        return Ok(());
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| Error::Message(format!("postgres begin batch tx failed: {e}")))?;
    let mut query = sqlx::query(insert_sql);
    for buf in bufs.iter() {
        query = match buf {
            ColBuf::Text(v) => query.bind(v.as_slice()),
            ColBuf::OptText(v) => query.bind(v.as_slice()),
            ColBuf::Int(v) => query.bind(v.as_slice()),
            ColBuf::OptInt(v) => query.bind(v.as_slice()),
        };
    }
    query
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Message(format!("postgres batch insert failed: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| Error::Message(format!("postgres commit batch tx failed: {e}")))?;
    for buf in bufs.iter_mut() {
        buf.clear();
    }
    Ok(())
}

fn copy_table(
    sqlite: &Connection,
    pool: &PgPool,
    spec: &TableSpec,
    batch_size: usize,
    resume: bool,
) -> Result<TableMigrationStat, Error> {
    let pg_before = block_on_pg(pg_count(pool, spec.pg_table))?;
    if !sqlite_table_exists(sqlite, spec.sqlite_table)? {
        return Ok(TableMigrationStat {
            pg_table: spec.pg_table.to_owned(),
            sqlite_rows: 0,
            pg_rows_before: pg_before,
            pg_rows_after: pg_before,
        });
    }
    let expected = sqlite_count(sqlite, spec.sqlite_table)?;
    if expected == 0 {
        return Ok(TableMigrationStat {
            pg_table: spec.pg_table.to_owned(),
            sqlite_rows: 0,
            pg_rows_before: pg_before,
            pg_rows_after: pg_before,
        });
    }
    if resume && pg_before >= expected {
        eprintln!(
            "  [migrate] {}: {} rows OK (resumed: pg={} >= sqlite={})",
            spec.pg_table, pg_before, pg_before, expected
        );
        return Ok(TableMigrationStat {
            pg_table: spec.pg_table.to_owned(),
            sqlite_rows: expected,
            pg_rows_before: pg_before,
            pg_rows_after: pg_before,
        });
    }

    let select_cols = spec
        .cols
        .iter()
        .map(|c| c.sqlite_expr)
        .collect::<Vec<_>>()
        .join(", ");
    let select_sql = format!("SELECT {select_cols} FROM {}", spec.sqlite_from);
    let mut stmt = sqlite.prepare(&select_sql)?;
    let mut rows = stmt.query([])?;

    let mut bufs: Vec<ColBuf> = spec
        .cols
        .iter()
        .map(|c| ColBuf::from_kind(c.kind, batch_size))
        .collect();
    let insert_sql = build_unnest_insert_sql(spec, &bufs);

    let started = Instant::now();
    let progress_step = (batch_size as i64 * 10).max(50_000);
    let mut next_progress = progress_step;
    let mut copied = 0_i64;
    while let Some(row) = rows.next()? {
        for (idx, col) in spec.cols.iter().enumerate() {
            bufs[idx].push_cell(row.get_ref(idx)?, col.kind)?;
        }
        copied += 1;
        if bufs[0].len() >= batch_size {
            block_on_pg(flush_batch(pool, &insert_sql, &mut bufs))?;
            if expected >= 10_000 && copied >= next_progress {
                eprintln!(
                    "  [migrate] {}: {}/{} ({:.1}s)",
                    spec.pg_table,
                    copied,
                    expected,
                    started.elapsed().as_secs_f64()
                );
                next_progress = copied + progress_step;
            }
        }
    }
    block_on_pg(flush_batch(pool, &insert_sql, &mut bufs))?;

    let pg_after = block_on_pg(pg_count(pool, spec.pg_table))?;
    if pg_after < expected {
        return Err(Error::Message(format!(
            "row count mismatch on {}: sqlite={expected}, postgres={pg_after}",
            spec.pg_table
        )));
    }
    eprintln!(
        "  [migrate] {}: {} rows OK ({:.2}s)",
        spec.pg_table,
        pg_after,
        started.elapsed().as_secs_f64()
    );
    Ok(TableMigrationStat {
        pg_table: spec.pg_table.to_owned(),
        sqlite_rows: expected,
        pg_rows_before: pg_before,
        pg_rows_after: pg_after,
    })
}

/// Topologically orders `agents` so parents always insert before children, and
/// strips dangling `parent_id` references so self-referential FK constraints succeed.
fn copy_agents_topological(
    sqlite: &Connection,
    pool: &PgPool,
    batch_size: usize,
    resume: bool,
) -> Result<(TableMigrationStat, HashSet<String>), Error> {
    let pg_before = block_on_pg(pg_count(pool, "public.agents"))?;
    let expected = sqlite_count(sqlite, "agents")?;
    if resume && expected > 0 && pg_before >= expected {
        let mut ids = HashSet::with_capacity(expected as usize);
        let mut stmt = sqlite.prepare("SELECT id FROM agents")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            if let Some(id) = sqlite_cell_to_string(row.get_ref(0)?)? {
                ids.insert(id);
            }
        }
        eprintln!(
            "  [migrate] public.agents: {pg_before} rows OK (resumed: pg={pg_before} >= sqlite={expected})"
        );
        return Ok((
            TableMigrationStat {
                pg_table: "public.agents".to_owned(),
                sqlite_rows: expected,
                pg_rows_before: pg_before,
                pg_rows_after: pg_before,
            },
            ids,
        ));
    }

    #[derive(Clone)]
    struct AgentRow {
        id: String,
        name: String,
        harness: String,
        model: Option<String>,
        workdir: Option<String>,
        instructions: Option<String>,
        state: String,
        created_at_ms: i64,
        launch_env: Option<String>,
        parent_id: Option<String>,
        is_admin: i64,
        role: Option<String>,
        description: Option<String>,
    }

    let mut stmt = sqlite.prepare(
        "SELECT id, name, harness, model, workdir, instructions, state, created_at_ms,
                launch_env, parent_id, is_admin, role, description
         FROM agents",
    )?;
    let mut rows = stmt.query([])?;
    let mut all = Vec::with_capacity(expected.max(0) as usize);
    let mut ids = HashSet::with_capacity(expected.max(0) as usize);
    while let Some(row) = rows.next()? {
        let id = sqlite_cell_to_string(row.get_ref(0)?)?.unwrap_or_default();
        ids.insert(id.clone());
        all.push(AgentRow {
            id,
            name: sqlite_cell_to_string(row.get_ref(1)?)?.unwrap_or_default(),
            harness: sqlite_cell_to_string(row.get_ref(2)?)?.unwrap_or_default(),
            model: sqlite_cell_to_string(row.get_ref(3)?)?,
            workdir: sqlite_cell_to_string(row.get_ref(4)?)?,
            instructions: sqlite_cell_to_string(row.get_ref(5)?)?,
            state: sqlite_cell_to_string(row.get_ref(6)?)?.unwrap_or_default(),
            created_at_ms: sqlite_cell_to_i64(row.get_ref(7)?)?.unwrap_or(0),
            launch_env: sqlite_cell_to_string(row.get_ref(8)?)?,
            parent_id: sqlite_cell_to_string(row.get_ref(9)?)?,
            is_admin: sqlite_cell_to_i64(row.get_ref(10)?)?.unwrap_or(0),
            role: sqlite_cell_to_string(row.get_ref(11)?)?,
            description: sqlite_cell_to_string(row.get_ref(12)?)?,
        });
    }

    let mut children: HashMap<Option<String>, Vec<usize>> = HashMap::new();
    for (idx, a) in all.iter_mut().enumerate() {
        if let Some(pid) = &a.parent_id
            && !ids.contains(pid)
        {
            a.parent_id = None;
        }
        children.entry(a.parent_id.clone()).or_default().push(idx);
    }

    let mut ordered = Vec::with_capacity(all.len());
    let mut seen = vec![false; all.len()];
    let mut queue = VecDeque::new();
    if let Some(roots) = children.get(&None) {
        for &idx in roots {
            queue.push_back(idx);
        }
    }
    while let Some(idx) = queue.pop_front() {
        if seen[idx] {
            continue;
        }
        seen[idx] = true;
        ordered.push(idx);
        if let Some(kids) = children.get(&Some(all[idx].id.clone())) {
            for &kid in kids {
                if !seen[kid] {
                    queue.push_back(kid);
                }
            }
        }
    }
    // Any remaining node belongs to a parent cycle; null its parent_id to preserve the agent row.
    for idx in 0..all.len() {
        if !seen[idx] {
            all[idx].parent_id = None;
            ordered.push(idx);
        }
    }

    let sql = "INSERT INTO public.agents (\"id\", \"name\", \"harness\", \"model\", \"workdir\", \
               \"instructions\", \"state\", \"created_at_ms\", \"launch_env\", \"parent_id\", \
               \"is_admin\", \"role\", \"description\") \
               SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], \
               $6::text[], $7::text[], $8::bigint[], $9::text[], $10::text[], $11::bigint[], \
               $12::text[], $13::text[]) ON CONFLICT DO NOTHING";

    for chunk in ordered.chunks(batch_size) {
        let n = chunk.len();
        let (mut c0, mut c1, mut c2, mut c3, mut c4, mut c5, mut c6) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let (mut c7, mut c8, mut c9, mut c10, mut c11, mut c12) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        for &idx in chunk {
            let a = &all[idx];
            c0.push(a.id.as_str());
            c1.push(a.name.as_str());
            c2.push(a.harness.as_str());
            c3.push(a.model.as_deref());
            c4.push(a.workdir.as_deref());
            c5.push(a.instructions.as_deref());
            c6.push(a.state.as_str());
            c7.push(a.created_at_ms);
            c8.push(a.launch_env.as_deref());
            c9.push(a.parent_id.as_deref());
            c10.push(a.is_admin);
            c11.push(a.role.as_deref());
            c12.push(a.description.as_deref());
        }
        block_on_pg(async {
            sqlx::query(sql)
                .bind(&c0)
                .bind(&c1)
                .bind(&c2)
                .bind(&c3)
                .bind(&c4)
                .bind(&c5)
                .bind(&c6)
                .bind(&c7)
                .bind(&c8)
                .bind(&c9)
                .bind(&c10)
                .bind(&c11)
                .bind(&c12)
                .execute(pool)
                .await
                .map_err(|e| Error::Message(format!("postgres agents batch failed: {e}")))
        })?;
    }

    let pg_after = block_on_pg(pg_count(pool, "public.agents"))?;
    if pg_after < expected {
        return Err(Error::Message(format!(
            "row count mismatch on public.agents: sqlite={expected}, postgres={pg_after}"
        )));
    }
    eprintln!("  [migrate] public.agents: {pg_after} rows OK");
    Ok((
        TableMigrationStat {
            pg_table: "public.agents".to_owned(),
            sqlite_rows: expected,
            pg_rows_before: pg_before,
            pg_rows_after: pg_after,
        },
        ids,
    ))
}

async fn reset_identity_sequences(pool: &PgPool) -> Result<(), Error> {
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

async fn run_reconcile_stale_runtime(pool: &PgPool) -> Result<(u64, u64, u64), Error> {
    let now = now_ms();
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| Error::Message(format!("reconcile begin failed: {e}")))?;
    let archived = sqlx::query(
        "UPDATE public.agent_runtime ar
         SET state = 'archived',
             fault_kind = NULL,
             fault_detail = NULL,
             until_ms = NULL,
             updated_at_ms = $1
         FROM public.agents a
         WHERE a.id = ar.agent_id AND a.state = 'archived' AND ar.state <> 'archived'",
    )
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(|e| Error::Message(format!("reconcile archived runtime failed: {e}")))?
    .rows_affected();

    let legacy = sqlx::query(
        "UPDATE public.agent_runtime
         SET state = 'released',
             fault_kind = NULL,
             fault_detail = NULL,
             until_ms = NULL,
             attempt = 0,
             updated_at_ms = $1
         WHERE fault_kind = 'legacy'",
    )
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(|e| Error::Message(format!("reconcile legacy faults failed: {e}")))?
    .rows_affected();

    let turns = sqlx::query(
        "UPDATE public.turns
         SET state = 'closed',
             closed_at_ms = COALESCE(closed_at_ms, $1),
             outcome = COALESCE(outcome, 'cancelled')
         WHERE state <> 'closed'",
    )
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(|e| Error::Message(format!("reconcile stale turns failed: {e}")))?
    .rows_affected();

    tx.commit()
        .await
        .map_err(|e| Error::Message(format!("reconcile commit failed: {e}")))?;
    Ok((archived, legacy, turns))
}

const STATE_WAVE_BEFORE_AGENTS: &[TableSpec] = &[
    TableSpec::simple(
        "schema_migrations",
        "public.schema_migrations",
        &[ColSpec::int("version"), ColSpec::int("applied_at_ms")],
    ),
    TableSpec::simple(
        "channels",
        "public.channels",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("kind"),
            ColSpec::opt_txt("identity"),
            ColSpec::int("created_at_ms"),
            ColSpec::txt_def("config", "{}"),
            ColSpec::opt_txt("credential_ref"),
            ColSpec::txt_def("state", "{}"),
        ],
    ),
    TableSpec::simple(
        "admin_audit",
        "public.admin_audit",
        &[
            ColSpec::int("id"),
            ColSpec::int("created_at"),
            ColSpec::txt("actor"),
            ColSpec::txt("action"),
            ColSpec::txt("target"),
            ColSpec::opt_txt("before_scope"),
            ColSpec::opt_txt("after_scope"),
            ColSpec::opt_txt("source"),
        ],
    ),
    TableSpec::simple(
        "agent_topology_state",
        "public.agent_topology_state",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("state_json"),
            ColSpec::int("updated_at_ms"),
        ],
    ),
    TableSpec::simple(
        "conversation_redactions",
        "public.conversation_redactions",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("target_id"),
            ColSpec::opt_int("origin_chat_id"),
            ColSpec::opt_int("origin_message_id"),
            ColSpec::txt("request_key"),
            ColSpec::txt("actor"),
            ColSpec::txt("permission"),
            ColSpec::txt("reason"),
            ColSpec::int("created_at_ms"),
            ColSpec::txt("span_unit"),
            ColSpec::int("span_start"),
            ColSpec::int("span_end"),
            ColSpec::int("removed_bytes"),
            ColSpec::int("removed_chars"),
            ColSpec::txt("receipt"),
        ],
    ),
    TableSpec::simple(
        "credential_binding_audit",
        "public.credential_binding_audit",
        &[
            ColSpec::int("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("slot"),
            ColSpec::opt_txt("before_target"),
            ColSpec::opt_txt("after_target"),
            ColSpec::txt("actor"),
            ColSpec::txt("reason"),
            ColSpec::int("at_ms"),
        ],
    ),
    TableSpec::simple(
        "credential_bindings",
        "public.credential_bindings",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("slot"),
            ColSpec::txt("target"),
            ColSpec::txt("set_by"),
            ColSpec::int("set_at_ms"),
            ColSpec::txt("reason"),
        ],
    ),
    TableSpec::simple(
        "credential_observations",
        "public.credential_observations",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("slot"),
            ColSpec::txt("record"),
            ColSpec::int("generation"),
            ColSpec::txt("reason"),
            ColSpec::int("at_ms"),
        ],
    ),
    TableSpec::simple(
        "lifecycle_ops",
        "public.lifecycle_ops",
        &[
            ColSpec::txt("op_id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("agent_name"),
            ColSpec::txt("kind"),
            ColSpec::txt("phase"),
            ColSpec::txt("actor"),
            ColSpec::txt("reason"),
            ColSpec::txt_def("evidence", "[]"),
            ColSpec::opt_txt("error"),
            ColSpec::int("started_at_ms"),
            ColSpec::int("updated_at_ms"),
            ColSpec::opt_int("finished_at_ms"),
        ],
    ),
    TableSpec::simple(
        "operator_messages",
        "public.operator_messages",
        &[
            ColSpec::int("seq"),
            ColSpec::txt("id"),
            ColSpec::txt("sender"),
            ColSpec::opt_txt("sender_name"),
            ColSpec::txt("body"),
            ColSpec::txt("body_sha256"),
            ColSpec::int("received_at_ms"),
            ColSpec::int("repeats"),
            ColSpec::opt_int("last_repeated_at_ms"),
        ],
    ),
    TableSpec::simple(
        "rejected_messages",
        "public.rejected_messages",
        &[
            ColSpec::txt("message_id"),
            ColSpec::txt("agent_id"),
            ColSpec::int("seq"),
            ColSpec::txt("reason"),
            ColSpec::int("rejected_at_ms"),
            ColSpec::opt_int("notified_at_ms"),
        ],
    ),
    TableSpec::simple(
        "sends",
        "public.sends",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("receipt"),
            ColSpec::int("created_at_ms"),
        ],
    ),
    TableSpec::simple(
        "turn_errors",
        "public.turn_errors",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::int("streak"),
            ColSpec::opt_txt("last_error"),
            ColSpec::opt_int("last_error_at_ms"),
            ColSpec::opt_int("faulted_at_ms"),
        ],
    ),
];

const STATE_WAVE_AFTER_AGENTS: &[TableSpec] = &[
    TableSpec::simple(
        "groups",
        "public.groups",
        &[
            ColSpec::txt("name"),
            ColSpec::int("created_at_ms"),
            ColSpec::int("public"),
            ColSpec {
                sqlite_expr: "CASE WHEN EXISTS(SELECT 1 FROM agents a WHERE a.id = groups.owner_agent_id) THEN groups.owner_agent_id ELSE NULL END",
                pg_col: "owner_agent_id",
                kind: ColKind::OptText,
            },
        ],
    ),
    TableSpec::simple(
        "channel_agents",
        "public.channel_agents",
        &[ColSpec::txt("channel_id"), ColSpec::txt("agent_id")],
    ),
    TableSpec::simple(
        "group_agents",
        "public.group_agents",
        &[ColSpec::txt("group_name"), ColSpec::txt("agent_id")],
    ),
    TableSpec::simple(
        "agent_aliases",
        "public.agent_aliases",
        &[
            ColSpec::txt("alias"),
            ColSpec::txt("agent_id"),
            ColSpec::int("created_at_ms"),
        ],
    ),
    TableSpec::simple(
        "agent_capabilities",
        "public.agent_capabilities",
        &[
            ColSpec::txt("token_hash"),
            ColSpec::txt("agent_id"),
            ColSpec::int("generation"),
            ColSpec::int("created_at_ms"),
            ColSpec::opt_int("revoked_at_ms"),
        ],
    ),
    TableSpec::simple(
        "agent_runtime",
        "public.agent_runtime",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("state"),
            ColSpec::int("generation"),
            ColSpec::int("attempt"),
            ColSpec::opt_int("until_ms"),
            ColSpec::opt_txt("fault_kind"),
            ColSpec::opt_txt("fault_detail"),
            ColSpec::opt_txt("vendor_session_id"),
            ColSpec::int("updated_at_ms"),
        ],
    ),
    TableSpec::simple(
        "agent_schedules",
        "public.agent_schedules",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("kind"),
            ColSpec::txt("spec"),
            ColSpec::txt("prompt"),
            ColSpec::opt_int("interval_ms"),
            ColSpec::opt_int("next_fire_at_ms"),
            ColSpec::opt_int("last_fired_at_ms"),
            ColSpec::opt_txt("watch_baseline"),
            ColSpec::opt_txt("last_observed"),
            ColSpec::txt_def("state", "active"),
            ColSpec::int("created_at_ms"),
            ColSpec::opt_int("pending_since_ms"),
            ColSpec::int("pending_count"),
        ],
    ),
    TableSpec::simple(
        "agent_turn_events",
        "public.agent_turn_events",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::int("turn_seq"),
            ColSpec::int("delivered_seq"),
            ColSpec::txt("outcome"),
        ],
    ),
    TableSpec::simple(
        "harness_leases",
        "public.harness_leases",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("harness"),
            ColSpec::txt("version"),
            ColSpec::int("holder_pid"),
            ColSpec::int("holder_start_time"),
            ColSpec::int("acquired_at_ms"),
        ],
    ),
    TableSpec::simple(
        "mailbox_cursors",
        "public.mailbox_cursors",
        &[ColSpec::txt("agent_id"), ColSpec::int("delivered_seq")],
    ),
    TableSpec::simple(
        "messages",
        "public.messages",
        &[
            ColSpec::int("rowid"),
            ColSpec::txt("id"),
            ColSpec::txt("agent_id"),
            ColSpec::int("seq"),
            ColSpec::opt_int("source_time_ms"),
            ColSpec::opt_txt("source_key"),
            ColSpec::opt_txt("sender"),
            ColSpec::txt("body"),
            ColSpec::opt_txt("reply_to"),
            ColSpec::int("received_at_ms"),
            ColSpec::opt_int("delivered_at_ms"),
            ColSpec::opt_txt("origin"),
            ColSpec::opt_txt("context"),
        ],
    ),
    TableSpec::simple(
        "message_identities",
        "public.message_identities",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("target_id"),
            ColSpec::txt("identity_id"),
        ],
    ),
    TableSpec::simple(
        "message_origin_aliases",
        "public.message_origin_aliases",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("identity_id"),
            ColSpec::txt("namespace"),
            ColSpec::int("chat_id"),
            ColSpec::int("message_id"),
        ],
    ),
    TableSpec::simple(
        "message_source_aliases",
        "public.message_source_aliases",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("identity_id"),
            ColSpec::txt("source_key"),
        ],
    ),
    TableSpec::simple(
        "message_versions",
        "public.message_versions",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::int("chat_id"),
            ColSpec::int("message_id"),
            ColSpec::txt("body"),
            ColSpec::int("updated_at_ms"),
            ColSpec::opt_txt("origin_namespace"),
        ],
    ),
    TableSpec::simple(
        "outbound_messages",
        "public.outbound_messages",
        &[
            ColSpec::int("rowid"),
            ColSpec::txt("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("destination"),
            ColSpec::opt_txt("reply_to"),
            ColSpec::txt("body"),
            ColSpec::int("sent_at_ms"),
            ColSpec::int("seq"),
        ],
    ),
    TableSpec::simple(
        "outcomes",
        "public.outcomes",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("kind"),
            ColSpec::txt("source"),
            ColSpec::txt("subject"),
            ColSpec::txt("dedupe_key"),
            ColSpec::txt("actor"),
            ColSpec::txt("harness"),
            ColSpec::opt_txt("model"),
            ColSpec::opt_txt("role"),
            ColSpec::opt_txt("profile"),
            ColSpec::opt_txt("note"),
            ColSpec::int("claimed_at_ms"),
            ColSpec::int("observed_at_ms"),
        ],
    ),
    TableSpec::simple(
        "quota_blocks",
        "public.quota_blocks",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::int("until_ms"),
            ColSpec::txt("source"),
            ColSpec::int("attempt"),
            ColSpec::int("observed_at_ms"),
        ],
    ),
    TableSpec::simple(
        "remote_session_tokens",
        "public.remote_session_tokens",
        &[
            ColSpec::txt("token_hash"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("session_id"),
            ColSpec::int("created_at_ms"),
            ColSpec::int("expires_at_ms"),
            ColSpec::opt_int("revoked_at_ms"),
        ],
    ),
    TableSpec::simple(
        "session_leases",
        "public.session_leases",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::opt_txt("vendor_session_id"),
            ColSpec::int("generation"),
            ColSpec::int("holder_pid"),
            ColSpec::int("holder_start_time"),
            ColSpec::int("pgid"),
            ColSpec::int("acquired_at_ms"),
        ],
    ),
    TableSpec::simple(
        "skill_reads",
        "public.skill_reads",
        &[
            ColSpec::int("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("skill"),
            ColSpec::txt("file"),
            ColSpec::txt("sha256"),
            ColSpec::int("read_at_ms"),
        ],
    ),
    TableSpec::simple(
        "steer_holds",
        "public.steer_holds",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("message_id"),
            ColSpec::int("generation"),
            ColSpec::opt_txt("turn_id"),
            ColSpec::txt("state"),
            ColSpec::int("redelivered"),
            ColSpec::int("updated_at_ms"),
        ],
    ),
    TableSpec::simple(
        "task_turn_contexts",
        "public.task_turn_contexts",
        &[
            ColSpec::txt("child_id"),
            ColSpec::txt("opener_id"),
            ColSpec::int("opener_seq"),
            ColSpec::txt("token"),
            ColSpec::int("synthetic_recovery"),
            ColSpec {
                sqlite_expr: "CASE WHEN EXISTS(SELECT 1 FROM agents a WHERE a.id = task_turn_contexts.sent_to_parent_id) THEN task_turn_contexts.sent_to_parent_id ELSE NULL END",
                pg_col: "sent_to_parent_id",
                kind: ColKind::OptText,
            },
            ColSpec::opt_int("closed_delivery_seq"),
        ],
    ),
    TableSpec::simple(
        "turn_recovery",
        "public.turn_recovery",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::int("opened_seq"),
            ColSpec::int("completed_seq"),
            ColSpec::int("recovery_count"),
            ColSpec::int("opened_at_ms"),
        ],
    ),
    TableSpec::simple(
        "turns",
        "public.turns",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("turn_id"),
            ColSpec::int("generation"),
            ColSpec::txt("state"),
            ColSpec::int("first_seq"),
            ColSpec::int("last_seq"),
            ColSpec::int("opened_at_ms"),
            ColSpec::opt_int("closed_at_ms"),
            ColSpec::opt_txt("outcome"),
            ColSpec::int("recovery_count"),
            ColSpec::opt_txt("recovers"),
        ],
    ),
    TableSpec::simple(
        "usage_records",
        "public.usage_records",
        &[
            ColSpec::txt("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("harness"),
            ColSpec::opt_txt("model"),
            ColSpec::int("input_tokens"),
            ColSpec::int("output_tokens"),
            ColSpec::int("cache_read_tokens"),
            ColSpec::int("cache_write_tokens"),
            ColSpec::opt_int("reported_cost_micros"),
            ColSpec::opt_int("estimated_cost_micros"),
            ColSpec::txt("billing_mode"),
            ColSpec::opt_txt("quota_state"),
            ColSpec::txt("day"),
            ColSpec::int("recorded_at_ms"),
        ],
    ),
];

const MEMORY_INDEX_TABLES: &[TableSpec] = &[
    TableSpec::simple(
        "index_meta",
        "memory_index.index_meta",
        &[ColSpec::txt("key"), ColSpec::int("value")],
    ),
    TableSpec::simple(
        "index_pending",
        "memory_index.index_pending",
        &[
            ColSpec::txt("kind"),
            ColSpec::txt("id"),
            ColSpec::opt_int("cap_messages"),
            ColSpec::opt_int("cap_outbound"),
        ],
    ),
    TableSpec::simple(
        "index_redactions",
        "memory_index.index_redactions",
        &[
            ColSpec::txt("chunk_id"),
            ColSpec::txt("request_key"),
            ColSpec::txt("receipt"),
        ],
    ),
    TableSpec {
        sqlite_table: "rag_chunks",
        sqlite_from: "rag_chunks r LEFT JOIN rag_chunks_fts f ON f.rowid = r.rowid",
        pg_table: "memory_index.rag_chunks",
        cols: &[
            ColSpec {
                sqlite_expr: "r.rowid",
                pg_col: "rowid",
                kind: ColKind::Int { default: 0 },
            },
            ColSpec {
                sqlite_expr: "r.chunk_id",
                pg_col: "chunk_id",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.agent_id",
                pg_col: "agent_id",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.agent_name",
                pg_col: "agent_name",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.direction",
                pg_col: "direction",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.sender",
                pg_col: "sender",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.destination",
                pg_col: "destination",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.timestamp_ms",
                pg_col: "timestamp_ms",
                kind: ColKind::Int { default: 0 },
            },
            ColSpec {
                sqlite_expr: "r.content_hash",
                pg_col: "content_hash",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.text",
                pg_col: "text",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.kind",
                pg_col: "kind",
                kind: ColKind::Text { default: "message" },
            },
            ColSpec {
                sqlite_expr: "r.tool_name",
                pg_col: "tool_name",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "r.seq",
                pg_col: "seq",
                kind: ColKind::Int { default: 0 },
            },
            ColSpec::expr_txt("COALESCE(f.search_tokens, '')", "search_tokens"),
        ],
        extra_pg_cols: Some("\"search_tsv\""),
        extra_pg_expr: Some(
            "to_tsvector('simple', coalesce(t.\"search_tokens\", '') || ' ' || coalesce(t.\"text\", ''))",
        ),
    },
];

const HOST_INDEX_TABLES: &[TableSpec] = &[
    TableSpec::simple(
        "host_generation",
        "host_index.host_generation",
        &[ColSpec::int("id"), ColSpec::txt("generation")],
    ),
    TableSpec::simple(
        "host_sources",
        "host_index.host_sources",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("segment"),
            ColSpec::txt("cursor"),
        ],
    ),
    TableSpec::simple(
        "host_agent_trees",
        "host_index.host_agent_trees",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("class"),
            ColSpec::txt("policy"),
            ColSpec::txt("tree"),
        ],
    ),
    TableSpec::simple(
        "host_documents",
        "host_index.host_documents",
        &[
            ColSpec::int("rowid"),
            ColSpec::txt("id"),
            ColSpec::txt("agent_id"),
            ColSpec::txt("source"),
            ColSpec::opt_int("timestamp_ms"),
            ColSpec::txt("direction"),
            ColSpec::txt("sender"),
            ColSpec::txt("destination"),
            ColSpec::opt_txt("tool_name"),
            ColSpec::opt_txt("role"),
            ColSpec::txt("segment"),
            ColSpec {
                sqlite_expr: "\"offset\"",
                pg_col: "offset",
                kind: ColKind::Int { default: 0 },
            },
            ColSpec::int("length"),
            ColSpec::txt("sha256"),
            ColSpec::int("seq"),
        ],
    ),
    TableSpec::simple(
        "host_document_tools",
        "host_index.host_document_tools",
        &[ColSpec::txt("document_id"), ColSpec::txt("name")],
    ),
    TableSpec::simple(
        "host_calls",
        "host_index.host_calls",
        &[
            ColSpec::txt("agent_id"),
            ColSpec::txt("segment"),
            ColSpec::txt("call_id"),
            ColSpec::txt("tool_name"),
            ColSpec::txt("document_id"),
        ],
    ),
    TableSpec {
        sqlite_table: "host_windows",
        // `host_fts` is a contentless FTS5 virtual table (`content=''`), so reading
        // `f.tokens` via `LEFT JOIN host_fts` always yields NULL while triggering a
        // random per-row virtual-table probe across millions of rows.
        sqlite_from: "host_windows w",
        pg_table: "host_index.host_windows",
        cols: &[
            ColSpec {
                sqlite_expr: "w.id",
                pg_col: "id",
                kind: ColKind::Int { default: 0 },
            },
            ColSpec {
                sqlite_expr: "w.document_id",
                pg_col: "document_id",
                kind: ColKind::Text { default: "" },
            },
            ColSpec {
                sqlite_expr: "w.\"offset\"",
                pg_col: "offset",
                kind: ColKind::Int { default: 0 },
            },
            ColSpec {
                sqlite_expr: "w.length",
                pg_col: "length",
                kind: ColKind::Int { default: 0 },
            },
            ColSpec {
                sqlite_expr: "w.sha256",
                pg_col: "sha256",
                kind: ColKind::Text { default: "" },
            },
            ColSpec::expr_txt("''", "tokens"),
        ],
        extra_pg_cols: Some("\"tokens_tsv\""),
        extra_pg_expr: Some("to_tsvector('simple', coalesce(t.\"tokens\", ''))"),
    },
];

const MAX_MIGRATED_BLOB_BYTES: u64 = 16 * 1024 * 1024;
const BLOB_BATCH_MAX_ROWS: usize = 250;
const BLOB_BATCH_MAX_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct BlobBackfillCounts {
    agent_files_migrated: u64,
    deployment_blobs_migrated: u64,
    gateway_routes_migrated: u64,
    sandbox_bindings_migrated: u64,
    skipped_secret_files: u64,
}

struct AgentFileBatch {
    agent_ids: Vec<String>,
    namespaces: Vec<String>,
    paths: Vec<String>,
    contents: Vec<Vec<u8>>,
    sha256s: Vec<String>,
    encrypted: Vec<bool>,
    updated_at_ms: Vec<i64>,
    total_bytes: usize,
}

impl AgentFileBatch {
    fn new() -> Self {
        Self {
            agent_ids: Vec::new(),
            namespaces: Vec::new(),
            paths: Vec::new(),
            contents: Vec::new(),
            sha256s: Vec::new(),
            encrypted: Vec::new(),
            updated_at_ms: Vec::new(),
            total_bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.agent_ids.is_empty()
    }

    fn should_flush(&self) -> bool {
        self.agent_ids.len() >= BLOB_BATCH_MAX_ROWS || self.total_bytes >= BLOB_BATCH_MAX_BYTES
    }

    fn push(
        &mut self,
        agent_id: String,
        namespace: &str,
        path: String,
        content: Vec<u8>,
        sha256: String,
        encrypted: bool,
        updated_at_ms: i64,
    ) {
        self.total_bytes += content.len();
        self.agent_ids.push(agent_id);
        self.namespaces.push(namespace.to_string());
        self.paths.push(path);
        self.contents.push(content);
        self.sha256s.push(sha256);
        self.encrypted.push(encrypted);
        self.updated_at_ms.push(updated_at_ms);
    }

    fn flush(&mut self, pool: &PgPool) -> Result<u64, Error> {
        if self.is_empty() {
            return Ok(0);
        }
        let count = self.agent_ids.len() as u64;
        let agent_ids = std::mem::take(&mut self.agent_ids);
        let namespaces = std::mem::take(&mut self.namespaces);
        let paths = std::mem::take(&mut self.paths);
        let contents = std::mem::take(&mut self.contents);
        let sha256s = std::mem::take(&mut self.sha256s);
        let encrypted = std::mem::take(&mut self.encrypted);
        let updated_at_ms = std::mem::take(&mut self.updated_at_ms);
        self.total_bytes = 0;

        block_on_pg(async move {
            sqlx::query(
                "INSERT INTO public.agent_files
                    (agent_id, namespace, path, content, sha256, encrypted, version, updated_at_ms)
                 SELECT t.agent_id, t.namespace, t.path, t.content, t.sha256, t.encrypted, 1, t.updated_at_ms
                 FROM UNNEST(
                     $1::text[], $2::text[], $3::text[], $4::bytea[],
                     $5::text[], $6::boolean[], $7::bigint[]
                 ) AS t(agent_id, namespace, path, content, sha256, encrypted, updated_at_ms)
                 ON CONFLICT (agent_id, namespace, path) DO UPDATE
                 SET content       = EXCLUDED.content,
                     sha256        = EXCLUDED.sha256,
                     encrypted     = EXCLUDED.encrypted,
                     version       = CASE
                                       WHEN public.agent_files.sha256 IS DISTINCT FROM EXCLUDED.sha256
                                         OR public.agent_files.encrypted IS DISTINCT FROM EXCLUDED.encrypted
                                       THEN public.agent_files.version + 1
                                       ELSE public.agent_files.version
                                     END,
                     updated_at_ms = EXCLUDED.updated_at_ms",
            )
            .bind(&agent_ids)
            .bind(&namespaces)
            .bind(&paths)
            .bind(&contents)
            .bind(&sha256s)
            .bind(&encrypted)
            .bind(&updated_at_ms)
            .execute(pool)
            .await
            .map_err(|e| Error::Message(format!("postgres agent_files backfill failed: {e}")))
        })?;
        Ok(count)
    }
}

struct DeploymentBlobBatch {
    namespaces: Vec<String>,
    paths: Vec<String>,
    contents: Vec<Vec<u8>>,
    sha256s: Vec<String>,
    encrypted: Vec<bool>,
    updated_at_ms: Vec<i64>,
    total_bytes: usize,
}

impl DeploymentBlobBatch {
    fn new() -> Self {
        Self {
            namespaces: Vec::new(),
            paths: Vec::new(),
            contents: Vec::new(),
            sha256s: Vec::new(),
            encrypted: Vec::new(),
            updated_at_ms: Vec::new(),
            total_bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.namespaces.is_empty()
    }

    fn should_flush(&self) -> bool {
        self.namespaces.len() >= BLOB_BATCH_MAX_ROWS || self.total_bytes >= BLOB_BATCH_MAX_BYTES
    }

    fn push(
        &mut self,
        namespace: &str,
        path: String,
        content: Vec<u8>,
        sha256: String,
        encrypted: bool,
        updated_at_ms: i64,
    ) {
        self.total_bytes += content.len();
        self.namespaces.push(namespace.to_string());
        self.paths.push(path);
        self.contents.push(content);
        self.sha256s.push(sha256);
        self.encrypted.push(encrypted);
        self.updated_at_ms.push(updated_at_ms);
    }

    fn flush(&mut self, pool: &PgPool) -> Result<u64, Error> {
        if self.is_empty() {
            return Ok(0);
        }
        let count = self.namespaces.len() as u64;
        let namespaces = std::mem::take(&mut self.namespaces);
        let paths = std::mem::take(&mut self.paths);
        let contents = std::mem::take(&mut self.contents);
        let sha256s = std::mem::take(&mut self.sha256s);
        let encrypted = std::mem::take(&mut self.encrypted);
        let updated_at_ms = std::mem::take(&mut self.updated_at_ms);
        self.total_bytes = 0;

        block_on_pg(async move {
            sqlx::query(
                "INSERT INTO public.deployment_blobs
                    (namespace, path, content, sha256, encrypted, version, updated_at_ms)
                 SELECT t.namespace, t.path, t.content, t.sha256, t.encrypted, 1, t.updated_at_ms
                 FROM UNNEST(
                     $1::text[], $2::text[], $3::bytea[], $4::text[], $5::boolean[], $6::bigint[]
                 ) AS t(namespace, path, content, sha256, encrypted, updated_at_ms)
                 ON CONFLICT (namespace, path) DO UPDATE
                 SET content       = EXCLUDED.content,
                     sha256        = EXCLUDED.sha256,
                     encrypted     = EXCLUDED.encrypted,
                     version       = CASE
                                       WHEN public.deployment_blobs.sha256 IS DISTINCT FROM EXCLUDED.sha256
                                         OR public.deployment_blobs.encrypted IS DISTINCT FROM EXCLUDED.encrypted
                                       THEN public.deployment_blobs.version + 1
                                       ELSE public.deployment_blobs.version
                                     END,
                     updated_at_ms = EXCLUDED.updated_at_ms",
            )
            .bind(&namespaces)
            .bind(&paths)
            .bind(&contents)
            .bind(&sha256s)
            .bind(&encrypted)
            .bind(&updated_at_ms)
            .execute(pool)
            .await
            .map_err(|e| {
                Error::Message(format!("postgres deployment_blobs backfill failed: {e}"))
            })
        })?;
        Ok(count)
    }
}

fn file_mtime_ms(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or_else(now_ms)
}

fn walk_regular_files_bounded(
    base: &Path,
    dir: &Path,
    skip_dirs: &[&str],
    skip_unreadable: bool,
    skipped_unreadable_dirs: &mut u64,
    out: &mut Vec<(String, PathBuf, i64)>,
) -> Result<(), Error> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) if skip_unreadable && e.kind() == std::io::ErrorKind::PermissionDenied => {
            *skipped_unreadable_dirs += 1;
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if meta.is_dir() {
            if skip_dirs.iter().any(|skip| *skip == name_str.as_ref()) {
                continue;
            }
            walk_regular_files_bounded(
                base,
                &path,
                skip_dirs,
                skip_unreadable,
                skipped_unreadable_dirs,
                out,
            )?;
        } else if meta.is_file() {
            if meta.len() > MAX_MIGRATED_BLOB_BYTES {
                continue;
            }
            let Ok(rel) = path.strip_prefix(base) else {
                continue;
            };
            let rel_str = rel.to_string_lossy().replace('\\', "/");
            if crate::stateless::validate_blob_path(&rel_str).is_err() {
                continue;
            }
            out.push((rel_str, path, file_mtime_ms(&meta)));
        }
    }
    Ok(())
}

fn is_agent_config_transient(rel: &str) -> bool {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    name.ends_with(".lock")
        || matches!(
            name,
            "opencode-service-pid"
                | "opencode-service-port"
                | "spiron.sock"
                | "spiron.log"
                | "sandbox.json"
                | "sandbox-sylphx.json"
        )
}

fn is_agent_config_secret(rel: &str) -> bool {
    let lower = rel.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    matches!(
        name,
        "mcp-capability"
            | "remote-session-token"
            | ".credentials.json"
            | "credentials.json"
            | "auth.json"
            | "antigravity-oauth-token"
    ) || name.ends_with(".key")
}

fn is_profile_secret(rel: &str) -> bool {
    let lower = rel.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    matches!(
        name,
        ".credentials.json" | "credentials.json" | "auth.json" | "key"
    ) || name.contains("token")
        || name.ends_with(".key")
}

fn classify_agent_config(rel: &str) -> (&'static str, bool) {
    if is_agent_config_secret(rel) {
        return ("secrets", true);
    }
    if rel.starts_with("spiron-cloud-") || rel.starts_with("spiron-cloud-inputs/") {
        return ("spiron_cloud", false);
    }
    if rel.ends_with("-acp.json") {
        return ("acp", false);
    }
    if rel.starts_with(".anyagents-") {
        return ("markers", false);
    }
    ("config", false)
}

fn parse_sandbox_binding_from_config(
    agent_id: &crate::AgentId,
    config_dir: &Path,
) -> Option<crate::stateless::SandboxBinding> {
    let sylphx_path = config_dir.join("sandbox-sylphx.json");
    if let Ok(raw) = std::fs::read(&sylphx_path)
        && let Ok(val) = serde_json::from_slice::<serde_json::Value>(&raw)
    {
        let mut binding = crate::stateless::SandboxBinding::new(agent_id.clone(), "sylphx");
        binding.template = val
            .get("template")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        binding.image = val
            .get("image")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if let Some(wd) = val
            .get("workdir")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty() && !s.ends_with("/launcher"))
        {
            binding.remote_workdir = wd.to_string();
        }
        binding.volume_id = val
            .get("volume_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        binding.volume_resource = val
            .get("volume_resource")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        binding.volume_size_gib = val.get("volume_size_gib").and_then(|v| v.as_i64());
        binding.lease_id = val
            .get("lease_id")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        binding.lease_resource = val
            .get("lease_resource")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        binding.data_plane_resource = val
            .get("data_plane_resource")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        binding.transport_url = val
            .get("envd_url")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        binding.guest_endpoint = binding.transport_url.clone();
        binding.transport_token = val
            .get("envd_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        binding.lease_state = if binding.lease_id.is_some() {
            "ready".to_string()
        } else {
            "detached".to_string()
        };
        let spec = serde_json::json!({
            "endpoint": val.get("endpoint").and_then(|v| v.as_str()).unwrap_or_default(),
            "parent": val.get("parent").and_then(|v| v.as_str()).unwrap_or_default(),
            "shape": val.get("shape").and_then(|v| v.as_str()).unwrap_or_default(),
            "mount_path": val.get("mount_path").and_then(|v| v.as_str()).unwrap_or("/home/user/agent-data"),
        });
        binding.spec_json = spec.to_string();
        return Some(binding);
    }

    let legacy_path = config_dir.join("sandbox.json");
    if let Ok(raw) = std::fs::read(&legacy_path)
        && let Ok(val) = serde_json::from_slice::<serde_json::Value>(&raw)
        && val
            .get("remote_spawn")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    {
        let mut binding = crate::stateless::SandboxBinding::new(agent_id.clone(), "legacy_http");
        if let Some(wd) = val
            .get("workdir")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty() && !s.ends_with("/launcher"))
        {
            binding.remote_workdir = wd.to_string();
        }
        binding.lease_id = val
            .get("sandbox_id")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        binding.transport_url = val
            .get("api_url")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        binding.guest_endpoint = binding.transport_url.clone();
        binding.transport_token = val
            .get("token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        binding.lease_state = if binding.lease_id.is_some() {
            "ready".to_string()
        } else {
            "detached".to_string()
        };
        return Some(binding);
    }
    None
}

fn backfill_on_disk_blobs(
    source_dir: &Path,
    valid_agents: &HashSet<String>,
    agent_names: &HashMap<String, String>,
    master_key: Option<&crate::stateless::MasterKey>,
    pool: &PgPool,
) -> Result<(BlobBackfillCounts, Vec<TableMigrationStat>), Error> {
    let af_before = block_on_pg(pg_count(pool, "public.agent_files"))?;
    let db_before = block_on_pg(pg_count(pool, "public.deployment_blobs"))?;

    let mut counts = BlobBackfillCounts::default();
    let mut af_batch = AgentFileBatch::new();
    let mut db_batch = DeploymentBlobBatch::new();

    let binding_store =
        crate::stateless::PgSandboxBindingStore::new(pool.clone(), master_key.cloned());
    let route_store = crate::stateless::PgGatewayRouteStore::new(pool.clone(), master_key.cloned());

    // 1. Per-agent files under `<source_dir>/agents/<id>/{memory,history/archive/memory,skills,sessions,config}`
    let agents_root = source_dir.join("agents");
    let mut sorted_agents: Vec<&String> = valid_agents.iter().collect();
    sorted_agents.sort();

    let mut skipped_unreadable_dirs = 0_u64;
    let total_agents = sorted_agents.len();
    for (agent_idx, aid_str) in sorted_agents.into_iter().enumerate() {
        let agent_dir = agents_root.join(aid_str);
        if !agent_dir.is_dir() {
            continue;
        }
        let Ok(agent_id) = crate::AgentId::parse(aid_str) else {
            continue;
        };

        // Direct 1:1 namespace subtrees (fail-closed on unreadable dirs)
        for (subdir, namespace) in [
            (agent_dir.join("memory"), "memory"),
            (
                agent_dir.join("history").join("archive").join("memory"),
                "memory_archive",
            ),
            (agent_dir.join("skills"), "skills"),
            (agent_dir.join("sessions"), "sessions"),
        ] {
            let mut files = Vec::new();
            walk_regular_files_bounded(
                &subdir,
                &subdir,
                &[],
                false,
                &mut skipped_unreadable_dirs,
                &mut files,
            )?;
            for (rel, full_path, mtime_ms) in files {
                if namespace == "sessions" && rel.ends_with(".lock") {
                    continue;
                }
                let Ok(bytes) = std::fs::read(&full_path) else {
                    continue;
                };
                let digest = crate::learned_memory::sha256(&bytes);
                af_batch.push(
                    aid_str.clone(),
                    namespace,
                    rel,
                    bytes,
                    digest,
                    false,
                    mtime_ms,
                );
                if af_batch.should_flush() {
                    counts.agent_files_migrated += af_batch.flush(pool)?;
                }
            }
        }

        // `agents/<id>/config/**`
        let config_dir = agent_dir.join("config");
        if config_dir.is_dir() {
            // Migrate sandbox binding into `agent_sandbox_bindings`
            if let Some(mut binding) = parse_sandbox_binding_from_config(&agent_id, &config_dir) {
                if binding.transport_token.is_some() && master_key.is_none() {
                    // Preserve non-secret binding metadata while never writing plaintext token.
                    binding.transport_token = None;
                    counts.skipped_secret_files += 1;
                }
                use crate::stateless::SandboxBindingStore as _;
                binding_store.upsert(&binding)?;
                counts.sandbox_bindings_migrated += 1;
            }

            let mut files = Vec::new();
            const SKIP_CONFIG_DIRS: &[&str] = &[
                "cache-home",
                "data-home",
                "state-home",
                ".cache",
                "tmp",
                "projects",
                "shell-snapshots",
                "plugins",
                "session-env",
                "backups",
                "node_modules",
                ".git",
                "brain",
                "recordings",
                "logs",
                ".npm",
                ".bun",
                ".cargo",
                "sessions",
                ".local",
                "chromium",
            ];
            walk_regular_files_bounded(
                &config_dir,
                &config_dir,
                SKIP_CONFIG_DIRS,
                true,
                &mut skipped_unreadable_dirs,
                &mut files,
            )?;
            for (rel, full_path, mtime_ms) in files {
                if is_agent_config_transient(&rel) {
                    continue;
                }
                let (ns, is_secret) = classify_agent_config(&rel);
                let Ok(bytes) = std::fs::read(&full_path) else {
                    continue;
                };
                let digest = crate::learned_memory::sha256(&bytes);
                let (stored, encrypted) = if is_secret {
                    let Some(key) = master_key else {
                        counts.skipped_secret_files += 1;
                        continue;
                    };
                    let aad = format!("agent_files:{aid_str}:{ns}:{rel}");
                    (key.encrypt(aad.as_bytes(), &bytes)?, true)
                } else {
                    (bytes, false)
                };
                af_batch.push(
                    aid_str.clone(),
                    ns,
                    rel,
                    stored,
                    digest,
                    encrypted,
                    mtime_ms,
                );
                if af_batch.should_flush() {
                    counts.agent_files_migrated += af_batch.flush(pool)?;
                }
            }
        }

        // `agents/<id>/provider.json` + `gateway-token.txt` -> `public.gateway_routes`
        let provider_path = agent_dir.join("provider.json");
        let token_path = agent_dir.join("gateway-token.txt");
        if provider_path.is_file() && token_path.is_file() {
            if master_key.is_some()
                && let Ok(raw) = std::fs::read(&provider_path)
                && let Ok(val) = serde_json::from_slice::<serde_json::Value>(&raw)
                && let Ok(tok_raw) = std::fs::read_to_string(&token_path)
            {
                let token = tok_raw.trim().to_string();
                let str_field = |k: &str| {
                    val.get(k)
                        .and_then(|v| v.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                };
                if let (Some(harness), Some(model), Some(base_url), Some(api_key)) = (
                    str_field("harness"),
                    str_field("model"),
                    str_field("baseUrl"),
                    str_field("apiKey"),
                ) && !token.is_empty()
                {
                    let protocol = match harness.as_str() {
                        "codex" => "openai_responses",
                        "opencode" => "openai_chat",
                        _ => "anthropic_messages",
                    }
                    .to_string();
                    let agent_name = agent_names
                        .get(aid_str)
                        .cloned()
                        .unwrap_or_else(|| aid_str.clone());
                    route_store.upsert(&crate::stateless::UpsertGatewayRoute {
                        agent_id,
                        agent_name,
                        harness,
                        protocol,
                        model,
                        base_url,
                        api_key,
                        token,
                    })?;
                    counts.gateway_routes_migrated += 1;
                }
            } else {
                counts.skipped_secret_files += 1;
            }
        }
        if (agent_idx + 1) % 500 == 0 || agent_idx + 1 == total_agents {
            eprintln!(
                "  [migrate] agent blobs: {}/{} agents ({} files, {} gateway_routes, {} skipped_unreadable_config_dirs)",
                agent_idx + 1,
                total_agents,
                counts.agent_files_migrated,
                counts.gateway_routes_migrated,
                skipped_unreadable_dirs
            );
        }
    }
    counts.agent_files_migrated += af_batch.flush(pool)?;

    // 2. Deployment-level config files -> `deployment_blobs(namespace = 'config')`
    for filename in [
        "anyagents.toml",
        "subagent-defaults.json",
        "sandbox-provider.json",
    ] {
        let path = source_dir.join(filename);
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_file() || meta.len() > MAX_MIGRATED_BLOB_BYTES {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let contains_secret = match filename {
            "anyagents.toml" => String::from_utf8_lossy(&bytes).contains("github_webhook_secret"),
            "sandbox-provider.json" => serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|v| {
                    v.get("token")
                        .and_then(|t| t.as_str())
                        .map(|t| !t.trim().is_empty())
                })
                .unwrap_or(false),
            _ => false,
        };
        let digest = crate::learned_memory::sha256(&bytes);
        let (stored, encrypted) = if contains_secret {
            let Some(key) = master_key else {
                counts.skipped_secret_files += 1;
                continue;
            };
            let aad = format!("deployment_blobs:config:{filename}");
            (key.encrypt(aad.as_bytes(), &bytes)?, true)
        } else {
            (bytes, false)
        };
        db_batch.push(
            "config",
            filename.to_string(),
            stored,
            digest,
            encrypted,
            file_mtime_ms(&meta),
        );
        if db_batch.should_flush() {
            counts.deployment_blobs_migrated += db_batch.flush(pool)?;
        }
    }

    // 3. Deployment-level directories: `profiles/**`, `skills/**`, `secrets/**`, `media/**`
    for (dir_name, namespace) in [
        ("profiles", "profiles"),
        ("skills", "skills"),
        ("secrets", "secrets"),
        ("media", "media"),
    ] {
        let root = source_dir.join(dir_name);
        let mut files = Vec::new();
        walk_regular_files_bounded(
            &root,
            &root,
            &[],
            false,
            &mut skipped_unreadable_dirs,
            &mut files,
        )?;
        for (rel, full_path, mtime_ms) in files {
            if rel.ends_with(".lock") {
                continue;
            }
            let is_secret =
                namespace == "secrets" || (namespace == "profiles" && is_profile_secret(&rel));
            let Ok(bytes) = std::fs::read(&full_path) else {
                continue;
            };
            let digest = crate::learned_memory::sha256(&bytes);
            let (stored, encrypted) = if is_secret {
                let Some(key) = master_key else {
                    counts.skipped_secret_files += 1;
                    continue;
                };
                let aad = format!("deployment_blobs:{namespace}:{rel}");
                (key.encrypt(aad.as_bytes(), &bytes)?, true)
            } else {
                (bytes, false)
            };
            db_batch.push(namespace, rel, stored, digest, encrypted, mtime_ms);
            if db_batch.should_flush() {
                counts.deployment_blobs_migrated += db_batch.flush(pool)?;
            }
        }
    }
    counts.deployment_blobs_migrated += db_batch.flush(pool)?;

    let af_after = block_on_pg(pg_count(pool, "public.agent_files"))?;
    let db_after = block_on_pg(pg_count(pool, "public.deployment_blobs"))?;
    let extra_stats = vec![
        TableMigrationStat {
            pg_table: "public.agent_files".to_owned(),
            sqlite_rows: counts.agent_files_migrated as i64,
            pg_rows_before: af_before,
            pg_rows_after: af_after,
        },
        TableMigrationStat {
            pg_table: "public.deployment_blobs".to_owned(),
            sqlite_rows: counts.deployment_blobs_migrated as i64,
            pg_rows_before: db_before,
            pg_rows_after: db_after,
        },
    ];
    Ok((counts, extra_stats))
}

/// Migrates `state.db`, `memory-index.db`, `host-history-index-v1.db`, and
/// on-disk agent/deployment files from `opts.source_dir` into the target
/// PostgreSQL database with batched `UNNEST` inserts, `\0` byte sanitization,
/// topological `agents` ordering, identity sequence reset, optional stale
/// runtime reconciliation, and row-count checks.
pub fn migrate_sqlite_dir_to_postgres(
    opts: &MigrateToPostgresOptions,
) -> Result<MigrateToPostgresReport, Error> {
    if !is_postgres_url(&opts.target_url) {
        return Err(Error::Message(
            "target URL must start with postgres:// or postgresql://".into(),
        ));
    }
    let state_path = opts.source_dir.join("state.db");
    if !state_path.is_file() {
        return Err(Error::Message(format!(
            "source state.db not found at {}",
            state_path.display()
        )));
    }
    let batch_size = opts.batch_size.clamp(100, 50_000);
    let started = Instant::now();
    let direct_url = configured_direct_database_url(&opts.target_url);
    migrate_postgres(&direct_url)?;
    let pool = build_pg_pool(&direct_url, Some(4))?;

    let state_sqlite = open_sqlite_readonly(&state_path)?;
    let schema_ver = crate::migrate::current_version(&state_sqlite)?;
    let latest_sqlite = crate::migrate::MIGRATIONS
        .last()
        .map(|(v, _)| *v)
        .unwrap_or(0);
    if schema_ver < latest_sqlite {
        return Err(Error::Message(format!(
            "source state.db schema version {schema_ver} is behind expected v{latest_sqlite}; open/upgrade a copy before migrating"
        )));
    }

    let mut stats = Vec::new();
    for spec in STATE_WAVE_BEFORE_AGENTS {
        stats.push(copy_table(
            &state_sqlite,
            &pool,
            spec,
            batch_size,
            opts.resume,
        )?);
    }
    let (agents_stat, valid_agents) =
        copy_agents_topological(&state_sqlite, &pool, batch_size, opts.resume)?;
    stats.push(agents_stat);
    for spec in STATE_WAVE_AFTER_AGENTS {
        stats.push(copy_table(
            &state_sqlite,
            &pool,
            spec,
            batch_size,
            opts.resume,
        )?);
    }

    let mut agent_names = HashMap::new();
    {
        let mut stmt = state_sqlite.prepare("SELECT id, name FROM agents")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            if let (Some(id), Some(name)) = (
                sqlite_cell_to_string(row.get_ref(0)?)?,
                sqlite_cell_to_string(row.get_ref(1)?)?,
            ) {
                agent_names.insert(id, name);
            }
        }
    }

    if !opts.skip_index {
        let mem_path = opts.source_dir.join("memory-index.db");
        if mem_path.is_file() {
            let mem_sqlite = open_sqlite_readonly(&mem_path)?;
            for spec in MEMORY_INDEX_TABLES {
                stats.push(copy_table(
                    &mem_sqlite,
                    &pool,
                    spec,
                    batch_size,
                    opts.resume,
                )?);
            }
        }
        let host_path = opts.source_dir.join(crate::host_retrieval::INDEX_FILE);
        if host_path.is_file() {
            let host_sqlite = open_sqlite_readonly(&host_path)?;
            for spec in HOST_INDEX_TABLES {
                stats.push(copy_table(
                    &host_sqlite,
                    &pool,
                    spec,
                    batch_size,
                    opts.resume,
                )?);
            }
        }
    }

    let (blob_counts, blob_stats) = backfill_on_disk_blobs(
        &opts.source_dir,
        &valid_agents,
        &agent_names,
        opts.master_key.as_ref(),
        &pool,
    )?;
    stats.extend(blob_stats);

    block_on_pg(reset_identity_sequences(&pool))?;
    let (reconciled_archived_runtime, reconciled_legacy_faults, reconciled_stale_turns) =
        if opts.reconcile_stale_runtime {
            block_on_pg(run_reconcile_stale_runtime(&pool))?
        } else {
            (0, 0, 0)
        };

    Ok(MigrateToPostgresReport {
        tables: stats,
        agent_files_migrated: blob_counts.agent_files_migrated,
        deployment_blobs_migrated: blob_counts.deployment_blobs_migrated,
        gateway_routes_migrated: blob_counts.gateway_routes_migrated,
        sandbox_bindings_migrated: blob_counts.sandbox_bindings_migrated,
        skipped_secret_files: blob_counts.skipped_secret_files,
        reconciled_archived_runtime,
        reconciled_legacy_faults,
        reconciled_stale_turns,
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}
