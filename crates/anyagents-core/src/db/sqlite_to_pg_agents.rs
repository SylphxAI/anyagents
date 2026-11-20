//! Topological `agents` table migration from SQLite to PostgreSQL, including
//! `runtime` and `remote_budget_micros` parity and pre-041 SQLite compatibility.

use std::collections::{HashMap, HashSet, VecDeque};

use anyagents_harness::{ExecutionRuntime, resolve_stored_runtime};
use rusqlite::Connection;
use sqlx::PgPool;

use super::{
    TableMigrationStat, block_on_pg, pg_count, sqlite_cell_to_i64, sqlite_cell_to_string,
    sqlite_count,
};
use crate::error::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
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
    runtime: String,
    remote_budget_micros: Option<i64>,
}

fn sqlite_table_columns(conn: &Connection, table: &str) -> Result<HashSet<String>, Error> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{table}\")"))?;
    let mut rows = stmt.query([])?;
    let mut cols = HashSet::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        cols.insert(name);
    }
    Ok(cols)
}

fn read_sqlite_agents(
    sqlite: &Connection,
    expected: i64,
) -> Result<(Vec<AgentRow>, HashSet<String>), Error> {
    let cols = sqlite_table_columns(sqlite, "agents")?;
    let runtime_col = if cols.contains("runtime") {
        "runtime"
    } else {
        "NULL"
    };
    let budget_col = if cols.contains("remote_budget_micros") {
        "remote_budget_micros"
    } else {
        "NULL"
    };
    let select_sql = format!(
        "SELECT id, name, harness, model, workdir, instructions, state, created_at_ms,
                launch_env, parent_id, is_admin, role, description, {runtime_col}, {budget_col}
         FROM agents"
    );
    let mut stmt = sqlite.prepare(&select_sql)?;
    let mut rows = stmt.query([])?;
    let mut all = Vec::with_capacity(expected.max(0) as usize);
    let mut ids = HashSet::with_capacity(expected.max(0) as usize);
    while let Some(row) = rows.next()? {
        let id = sqlite_cell_to_string(row.get_ref(0)?)?.unwrap_or_default();
        let harness = sqlite_cell_to_string(row.get_ref(2)?)?.unwrap_or_default();
        let runtime = sqlite_cell_to_string(row.get_ref(13)?)?
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .and_then(|raw| ExecutionRuntime::parse(raw).ok())
            .unwrap_or_else(|| resolve_stored_runtime(&harness, &[]))
            .as_canonical_string();
        let remote_budget_micros = sqlite_cell_to_i64(row.get_ref(14)?)?.filter(|&v| v > 0);
        ids.insert(id.clone());
        all.push(AgentRow {
            id,
            name: sqlite_cell_to_string(row.get_ref(1)?)?.unwrap_or_default(),
            harness,
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
            runtime,
            remote_budget_micros,
        });
    }
    Ok((all, ids))
}

/// Topologically orders `agents` so parents always insert before children, and
/// strips dangling `parent_id` references so self-referential FK constraints succeed.
pub(super) fn copy_agents_topological(
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

    let (mut all, ids) = read_sqlite_agents(sqlite, expected)?;

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
               \"is_admin\", \"role\", \"description\", \"runtime\", \"remote_budget_micros\") \
               SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], \
               $6::text[], $7::text[], $8::bigint[], $9::text[], $10::text[], $11::bigint[], \
               $12::text[], $13::text[], $14::text[], $15::bigint[]) ON CONFLICT DO NOTHING";

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
        let (mut c7, mut c8, mut c9, mut c10, mut c11, mut c12, mut c13, mut c14) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
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
            c13.push(a.runtime.as_str());
            c14.push(a.remote_budget_micros);
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
                .bind(&c13)
                .bind(&c14)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::MIGRATIONS;

    #[test]
    fn reads_runtime_and_budget_from_modern_and_legacy_sqlite_schemas() {
        // 1. Legacy v39 schema (no `runtime` or `remote_budget_micros` columns).
        let legacy = Connection::open_in_memory().unwrap();
        legacy
            .execute_batch(
                "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
            )
            .unwrap();
        for (version, sql) in MIGRATIONS.iter().filter(|(version, _)| *version <= 39) {
            legacy.execute_batch(sql).unwrap();
            legacy
                .execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
                .unwrap();
        }
        legacy
            .execute(
                "INSERT INTO agents (id, name, harness, state, created_at_ms)
                 VALUES ('agt_1', 'local-a', 'spiron', 'idle', 10),
                        ('agt_2', 'cloud-a', 'spiron-cloud', 'idle', 20)",
                [],
            )
            .unwrap();
        let (rows, ids) = read_sqlite_agents(&legacy, 2).unwrap();
        assert_eq!(ids.len(), 2);
        let by_id: HashMap<&str, &AgentRow> = rows.iter().map(|r| (r.id.as_str(), r)).collect();
        assert_eq!(by_id["agt_1"].runtime, "local");
        assert_eq!(by_id["agt_1"].remote_budget_micros, None);
        assert_eq!(by_id["agt_2"].runtime, "cloud");
        assert_eq!(by_id["agt_2"].remote_budget_micros, None);

        // 2. Modern schema (with `runtime` and `remote_budget_micros`).
        let mut modern = Connection::open_in_memory().unwrap();
        crate::migrate::migrate(&mut modern).unwrap();
        modern
            .execute(
                "INSERT INTO agents (id, name, harness, state, created_at_ms, runtime, remote_budget_micros)
                 VALUES ('agt_3', 'sbx-a', 'claude-code', 'idle', 30, 'sandbox:sylphx', 2500000),
                        ('agt_4', 'cld-b', 'spiron', 'idle', 40, 'cloud', NULL)",
                [],
            )
            .unwrap();
        let (rows, _) = read_sqlite_agents(&modern, 2).unwrap();
        let by_id: HashMap<&str, &AgentRow> = rows.iter().map(|r| (r.id.as_str(), r)).collect();
        assert_eq!(by_id["agt_3"].runtime, "sandbox:sylphx");
        assert_eq!(by_id["agt_3"].remote_budget_micros, Some(2_500_000));
        assert_eq!(by_id["agt_4"].runtime, "cloud");
        assert_eq!(by_id["agt_4"].remote_budget_micros, None);
    }
}
